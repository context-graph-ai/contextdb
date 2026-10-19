use super::*;
use std::collections::HashMap;

fn fixture() -> (
    tempfile::TempDir,
    RedbPersistence,
    VectorPartitionRef,
    PagedDeclaration,
) {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("namespace.db");
    let db = crate::Database::open(&path).unwrap();
    db.execute("CREATE TABLE points (id INT PRIMARY KEY, bucket INT NOT NULL, embedding VECTOR(3) PARTITION_KEY (bucket) MAX_PARTITIONS 2)", &HashMap::new()).unwrap();
    db.close().unwrap();
    let persistence = RedbPersistence::open(&path).unwrap();
    let partition = VectorPartitionRef::new(
        VectorIndexRef::new("points", "embedding"),
        VectorPartitionKey::from_values(&[contextdb_core::Value::Int64(1)]).unwrap(),
    );
    let declaration = PagedDeclaration {
        dimension: 3,
        quantization: VectorQuantization::F32,
        hnsw_m: 8,
        hnsw_ef_construction: 32,
        hnsw_ef_search: 64,
        policy_revision: 1,
    };
    (root, persistence, partition, declaration)
}
fn set_next(persistence: &RedbPersistence, next: u64) {
    persistence
        .with_db(|db| {
            let write = db.begin_write().map_err(RedbPersistence::storage_error)?;
            write
                .open_table(COUNTERS)
                .map_err(RedbPersistence::storage_error)?
                .insert(NEXT_STORAGE, next)
                .map_err(RedbPersistence::storage_error)?;
            write.commit().map_err(RedbPersistence::storage_error)
        })
        .unwrap();
}
fn next(persistence: &RedbPersistence) -> u64 {
    persistence
        .with_db(|db| {
            let read = db.begin_read().map_err(RedbPersistence::storage_error)?;
            let counters = read
                .open_table(COUNTERS)
                .map_err(RedbPersistence::storage_error)?;
            Ok(counters
                .get(NEXT_STORAGE)
                .map_err(RedbPersistence::storage_error)?
                .unwrap()
                .value())
        })
        .unwrap()
}
#[test]
fn native_and_legacy_allocations_are_disjoint_and_sql_integer_bounded() {
    let _clock = contextdb_core::Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let (_root, persistence, partition, declaration) = fixture();
    let binding = persistence.vector_schema_binding(&partition.index).unwrap();
    let legacy = persistence
        .begin_paged_build(
            &partition,
            declaration.clone(),
            PagedBuildReason::InitialBuild,
            1,
            0,
            TxId(1),
            Lsn(1),
            1,
            binding,
            false,
        )
        .unwrap();
    assert!(legacy.storage_id < MUTABLE_STORAGE);
    persistence.discard_paged_build(&partition).unwrap();
    let native = persistence.allocate_native_tail_storage().unwrap();
    let candidate = persistence
        .begin_paged_build(
            &partition,
            declaration.clone(),
            PagedBuildReason::NewChanges,
            native + 1,
            native,
            TxId(1),
            Lsn(1),
            1,
            binding,
            false,
        )
        .unwrap();
    assert!(candidate.storage_id > native);
    assert_eq!(
        candidate.progress().unwrap().generation_id,
        candidate.storage_id
    );
    assert!(i64::try_from(candidate.storage_id).is_ok());
    assert_ne!(candidate.storage_id & MUTABLE_STORAGE, 0);
    // Physical adoption cannot authorize any logical or semantic replacement.
    let mut progress = legacy.progress().unwrap();
    progress.phase = PagedVectorPhase::Complete;
    let original = PagedGenerationMeta::decode(
        &RedbPersistence::encode_paged_generation_meta(
            legacy.storage_id,
            &declaration,
            &progress,
            binding,
            PageSetRoot {
                pages: 0,
                bytes: 0,
                digest: [0; 32],
            },
        )
        .unwrap(),
    )
    .unwrap();
    let mut physical = original.clone();
    physical.storage_id = native;
    physical.page_set = PageSetRoot {
        pages: MUTABLE_PAGE_SET,
        bytes: 123,
        digest: [1; 32],
    };
    let frame = |physical: &PagedGenerationMeta| {
        let payload = RedbPersistence::encode(&(
            RedbPersistence::encode(&original).unwrap(),
            RedbPersistence::encode(physical).unwrap(),
        ))
        .unwrap();
        seal_value(&original.storage_id.to_be_bytes(), &payload)
    };
    assert_eq!(
        import_tail::decode_native_change_backing(original.storage_id, &frame(&physical))
            .unwrap()
            .physical
            .storage_id,
        native
    );
    for row in 0..6 {
        let mut changed = physical.clone();
        match row {
            0 => changed.progress.generation_id += 1,
            1 => changed.progress.frontier_lsn += 1,
            2 => changed.declaration.hnsw_m += 1,
            3 => changed.progress.graph.points += 1,
            4 => changed.storage_id = original.storage_id,
            _ => changed.storage_id |= 1 << 63,
        }
        assert!(
            matches!(
                import_tail::decode_native_change_backing(original.storage_id, &frame(&changed)),
                Err(Error::StoreCorrupted { .. })
            ),
            "semantic replacement row {row} must refuse"
        );
    }
    let mut damaged = frame(&physical);
    *damaged.last_mut().unwrap() ^= 1;
    assert!(matches!(
        import_tail::decode_native_change_backing(original.storage_id, &damaged),
        Err(Error::StoreCorrupted { .. })
    ));
    set_next(&persistence, MUTABLE_STORAGE - 1);
    let last = persistence.allocate_native_tail_storage().unwrap();
    assert_eq!(i64::try_from(last).unwrap(), i64::MAX);
    assert!(persistence.allocate_native_tail_storage().is_err());
    assert_eq!(next(&persistence), MUTABLE_STORAGE);
}
#[test]
fn resident_export_cannot_cross_native_namespace_or_sql_integer_range() {
    let _clock = contextdb_core::Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let (_root, persistence, partition, declaration) = fixture();
    let binding = persistence.vector_schema_binding(&partition.index).unwrap();
    set_next(&persistence, 3);
    assert!(
        persistence
            .begin_resident_paged_export(
                &partition,
                declaration.clone(),
                PagedBuildReason::InitialBuild,
                MUTABLE_STORAGE - 1,
                0,
                TxId(1),
                Lsn(1),
                1,
                binding,
                false,
            )
            .is_err()
    );
    assert_eq!(next(&persistence), 3, "refusal rolls back the allocator");
    assert!(
        persistence
            .begin_paged_build(
                &partition,
                declaration,
                PagedBuildReason::NewChanges,
                1 << 63,
                MUTABLE_STORAGE + 1,
                TxId(1),
                Lsn(1),
                1,
                binding,
                false,
            )
            .is_err()
    );
    assert_eq!(next(&persistence), 3);
}

fn corrupt_catalog(persistence: &RedbPersistence, partition: &VectorPartitionRef) {
    let key = RedbPersistence::vector_partition_catalog_key_from_parts(
        &partition.index,
        &partition.partition_key,
    );
    persistence
        .with_db(|db| {
            let write = db.begin_write().map_err(RedbPersistence::storage_error)?;
            write
                .open_table(VECTOR_PARTITION_GENERATION_CATALOG_TABLE)
                .map_err(RedbPersistence::storage_error)?
                .insert(key.as_slice(), b"not a catalog record".as_slice())
                .map_err(RedbPersistence::storage_error)?;
            write.commit().map_err(RedbPersistence::storage_error)
        })
        .unwrap();
}

/// A build that does not replace a quarantined route still refuses a
/// damaged catalog as corruption; only a quarantined route's repair treats
/// that catalog as the artifact it replaces.
#[test]
fn a_damaged_catalog_is_replaced_only_by_its_quarantined_routes_repair() {
    let (_root, persistence, partition, declaration) = fixture();
    let binding = persistence.vector_schema_binding(&partition.index).unwrap();
    corrupt_catalog(&persistence, &partition);
    let ordinary = persistence.begin_paged_build(
        &partition,
        declaration.clone(),
        PagedBuildReason::NewChanges,
        2,
        1,
        TxId(1),
        Lsn(1),
        1,
        binding,
        false,
    );
    assert!(
        matches!(ordinary, Err(Error::StoreCorrupted { .. })),
        "a damaged catalog outside a repair is reported as corruption: {ordinary:?}"
    );
    persistence
        .begin_paged_build(
            &partition,
            declaration,
            PagedBuildReason::InitialBuild,
            2,
            0,
            TxId(1),
            Lsn(1),
            1,
            binding,
            true,
        )
        .expect("a quarantined route's repair replaces its damaged catalog");
}
