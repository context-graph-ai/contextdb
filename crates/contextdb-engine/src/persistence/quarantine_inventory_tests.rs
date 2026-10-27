use super::*;

fn writer() -> (tempfile::TempDir, RedbPersistence) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("quarantine-inventory.db");
    let database = crate::Database::open(&path).unwrap();
    database.close().unwrap();
    (directory, RedbPersistence::open(&path).unwrap())
}

fn base(generation_id: u64, graph_len: usize) -> VectorPartitionBaseGenerationRecord {
    VectorPartitionBaseGenerationRecord {
        index: VectorIndexRef::new("items", "embedding"),
        partition_key: VectorPartitionKey::from_components(vec![
            contextdb_core::VectorPartitionComponent::Text("tenant".into()),
        ]),
        generation_id,
        base_tx: TxId(4),
        base_lsn: Lsn(5),
        highest_included_tx: TxId(6),
        highest_included_lsn: Lsn(8),
        dimension: 768,
        quantization: VectorQuantization::F32,
        algorithm_version: 1,
        graph_format_version: 1,
        graph: VectorPartitionGraphBytes::verified(vec![13; graph_len]),
        state_digest: [17; 32],
    }
}

#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
fn put(
    persistence: &RedbPersistence,
    definition: TableDefinition<&'static [u8], &'static [u8]>,
    key: &[u8],
    encoded: &[u8],
) {
    persistence
        .with_db(|db| {
            let write = db.begin_write().map_err(RedbPersistence::storage_error)?;
            {
                let mut table = write
                    .open_table(definition)
                    .map_err(RedbPersistence::storage_error)?;
                table
                    .insert(key, encoded)
                    .map_err(RedbPersistence::storage_error)?;
            }
            write.commit().map_err(RedbPersistence::storage_error)
        })
        .unwrap();
}

#[test]
#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
fn quarantined_inventory_borrows_retained_graphs_with_graph_size_independent_admission() {
    let (_directory, persistence) = writer();
    let route = base(1, 0);
    let inspect = || {
        let mut charges = Vec::new();
        let highest = persistence
            .highest_vector_partition_generation_id(&route.index, &route.partition_key, |bytes| {
                assert!(
                    bytes < 4096,
                    "graph ownership cannot fit this metadata admission: {bytes}"
                );
                charges.push(bytes);
                Ok(())
            })
            .unwrap();
        (highest, charges)
    };
    let install = |record: &VectorPartitionBaseGenerationRecord| {
        put(
            &persistence,
            VECTOR_PARTITION_BASE_GENERATION_TABLE,
            &RedbPersistence::vector_partition_base_generation_key(record),
            &RedbPersistence::encode_vector_partition_record(
                VectorPartitionRecordKind::BaseGeneration,
                record,
            )
            .unwrap(),
        );
    };
    install(&base(1, 7));
    let (_, small_charges) = inspect();
    install(&base(1, 2 * 1024 * 1024));
    assert_eq!(inspect().1, small_charges);
    for generation_id in 2..=4 {
        install(&base(generation_id, 2 * 1024 * 1024));
    }
    let change = VectorPartitionChangeGenerationRecord {
        index: route.index.clone(),
        partition_key: route.partition_key.clone(),
        base_generation_id: 4,
        change_generation_id: 9,
        covered_through_tx: TxId(6),
        covered_through_lsn: Lsn(8),
        fresh_tail_from_tx: TxId(7),
        fresh_tail_from_lsn: Lsn(9),
        dimension: 768,
        quantization: VectorQuantization::F32,
        algorithm_version: 1,
        graph_format_version: 1,
        graph: VectorPartitionGraphBytes::verified(vec![19; 2 * 1024 * 1024]),
    };
    put(
        &persistence,
        VECTOR_PARTITION_CHANGE_GENERATION_TABLE,
        &RedbPersistence::vector_partition_change_generation_key(&change),
        &RedbPersistence::encode_vector_partition_record(
            VectorPartitionRecordKind::ChangeGeneration,
            &change,
        )
        .unwrap(),
    );
    let (highest, charges) = inspect();
    assert_eq!(highest, 9);
    assert_eq!(
        charges.len(),
        6,
        "one initial admission and five retained records"
    );
    assert!(charges.iter().all(|charge| *charge == small_charges[0]));
}

#[test]
#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
fn quarantined_inventory_rejects_authenticated_unbounded_route_and_invalid_complete_records() {
    let (_directory, persistence) = writer();
    let original = base(7, 17);
    let key = RedbPersistence::vector_partition_base_generation_key(&original);
    let encode = |record: &VectorPartitionBaseGenerationRecord| {
        RedbPersistence::encode_vector_partition_record(
            VectorPartitionRecordKind::BaseGeneration,
            record,
        )
        .unwrap()
    };
    let assert_corruption = |encoded: &[u8], expected: &str| {
        put(
            &persistence,
            VECTOR_PARTITION_BASE_GENERATION_TABLE,
            &key,
            encoded,
        );
        let error = persistence
            .highest_vector_partition_generation_id(
                &original.index,
                &original.partition_key,
                |bytes| {
                    assert!(bytes < 4096);
                    Ok(())
                },
            )
            .unwrap_err();
        assert!(
            matches!(&error, Error::StoreCorrupted { reason, .. } if reason.contains(expected)),
            "expected {expected}: {error:?}"
        );
    };
    let mut hostile_route = original.clone();
    hostile_route.index.table = "x".repeat(1024 * 1024);
    assert_corruption(&encode(&hostile_route), "route prefix");
    let mut wrong_key = original.clone();
    wrong_key.generation_id += 1;
    assert_corruption(&encode(&wrong_key), "table key");
    let mut corrupt_checksum = encode(&original);
    corrupt_checksum[0] ^= 1;
    assert_corruption(&corrupt_checksum, "checksum mismatch");
    let mut trailing = encode(&original);
    trailing.truncate(trailing.len() - 32);
    trailing.push(0);
    let length_offset = VECTOR_PARTITION_RECORD_MAGIC.len() + 2 + 1;
    let payload_length = trailing.len() - length_offset - 8;
    trailing[length_offset..length_offset + 8]
        .copy_from_slice(&(payload_length as u64).to_be_bytes());
    let checksum = *blake3::hash(&trailing).as_bytes();
    trailing.extend_from_slice(&checksum);
    assert_corruption(&trailing, "exact decode left trailing bytes");
}
