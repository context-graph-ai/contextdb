use super::*;

fn fixture() -> (tempfile::TempDir, RedbPersistence) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("placements.db");
    let db = crate::Database::open(&path).unwrap();
    db.execute(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, scope TEXT NOT NULL, embedding VECTOR(2) PARTITION_KEY(scope) MAX_PARTITIONS 2)",
        &HashMap::new(),
    ).expect("declare the physical vector fixture table and its maintained totals");
    db.close().unwrap();
    let fixture_open_accountant_12 = std::sync::Arc::new(crate::memory_accounting::MemoryAccountant::no_limit());
    let _fixture_decode_scope_12 = crate::read_image_memory::ImageMemory::enter_writer_operation(std::sync::Arc::clone(&fixture_open_accountant_12).into()).unwrap();
    let persistence = RedbPersistence::open(&path).unwrap();
    persistence.attach_memory_accountant(std::sync::Arc::clone(&fixture_open_accountant_12).into());
    persistence
        .append_vector_entries_batch(
            &[VectorEntry {
                index: VectorIndexRef::new("items", "embedding"),
                row_id: RowId(1),
                vector: vec![1.0, 0.0],
                created_tx: TxId(1),
                deleted_tx: None,
                lsn: Lsn(1),
            }],
            &HashMap::new(),
        )
        .unwrap();
    (dir, persistence)
}

fn placement(route: &str, start: u64, end: Option<u64>) -> VectorPartitionMembershipRecord {
    VectorPartitionMembershipRecord {
        index: VectorIndexRef::new("items", "embedding"),
        partition_key: VectorPartitionKey::from_components(vec![
            contextdb_core::VectorPartitionComponent::Text(route.into()),
        ]),
        row_id: RowId(1),
        row_created_tx: TxId(start),
        row_lsn: Lsn(start),
        vector_created_tx: TxId(1),
        vector_lsn: Lsn(1),
        visible_from: TxId(start),
        deleted_tx: end.map(TxId),
    }
}

fn decode_catalog(persistence: &RedbPersistence) -> std::result::Result<bool, LoadReadImageError> {
    persistence
        .with_db(|db| {
            let read = db.begin_read().map_err(RedbPersistence::storage_error)?;
            let (_, raw) = decode_read_vector_generation_catalog(
                &read,
                &crate::read_image_memory::ImageMemory::new(Some(1024 * 1024)),
            )
            .map_err(|error| Error::Other(format!("{error:?}")))?;
            Ok(raw.ready)
        })
        .map_err(read_image_error)
}

/// Adds a raw body for a row no placement covers and raises the copied
/// referenced-body count to match, leaving the canonical projection unchanged.
fn insert_orphan_body_and_compensate_count(write: &redb::WriteTransaction) {
    let mut bodies = write.open_table(VECTORS_TABLE).unwrap();
    let key = RedbPersistence::vector_identity_key(
        &VectorIndexRef::new("items", "embedding"),
        RowId(2),
        TxId(1),
        Lsn(1),
    );
    bodies.insert(key.as_slice(), &[0_u8; 8][..]).unwrap();
    write
        .open_table(CONTENT_IDENTITY_METADATA_TABLE)
        .unwrap()
        .insert(
            VECTOR_REFERENCED_BODY_COUNT_KEY,
            2_u64.to_be_bytes().as_slice(),
        )
        .unwrap();
}

#[test]
fn disjoint_shared_body_placements_publish_and_open_across_partitions() {
    let (_dir, persistence) = fixture();
    persistence
        .append_vector_partition_memberships_batch(&[
            placement("first", 1, Some(5)),
            placement("second", 5, None),
        ])
        .unwrap();
    persistence
        .with_db(|db| {
            let read = db.begin_read().map_err(RedbPersistence::storage_error)?;
            assert_eq!(read.open_table(VECTORS_TABLE).unwrap().len().unwrap(), 1);
            assert_eq!(
                read.open_table(VECTOR_PARTITION_MEMBERSHIP_TABLE)
                    .unwrap()
                    .len()
                    .unwrap(),
                2
            );
            assert_eq!(
                vector_membership_pages::referenced_body_count_in_read(&read).unwrap(),
                1
            );
            assert!(vector_body_root_matches(&read).unwrap());
            Ok(())
        })
        .unwrap();
    let validation_calls = vector_membership_pages::body_placement_validation_calls();
    assert!(
        decode_catalog(&persistence).unwrap(),
        "healthy open accepts the authenticated coverage relation"
    );
    assert_eq!(
        vector_membership_pages::body_placement_validation_calls(),
        validation_calls,
        "healthy open trusts committed producer metadata without semantic history validation"
    );
}

#[test]
fn shared_body_overlap_is_refused_within_and_across_partitions_atomically() {
    for route in ["first", "second"] {
        let (_dir, persistence) = fixture();
        persistence
            .append_vector_partition_memberships_batch(&[placement("first", 1, Some(5))])
            .unwrap();
        let error = persistence
            .append_vector_partition_memberships_batch(&[placement(route, 4, Some(6))])
            .unwrap_err();
        assert!(
            matches!(error, Error::StoreCorrupted { ref reason, .. }
            if reason.contains("overlapping placement lifetimes")),
            "{error:?}"
        );
        persistence
            .with_db(|db| {
                let read = db.begin_read().map_err(RedbPersistence::storage_error)?;
                assert_eq!(
                    read.open_table(VECTOR_PARTITION_MEMBERSHIP_TABLE)
                        .unwrap()
                        .len()
                        .unwrap(),
                    1
                );
                assert_eq!(
                    vector_membership_pages::referenced_body_count_in_read(&read).unwrap(),
                    1
                );
                Ok(())
            })
            .unwrap();
        assert!(decode_catalog(&persistence).unwrap());
    }
}

#[test]
fn shared_body_coverage_count_is_bound_to_the_committed_vector_projection() {
    let (_dir, persistence) = fixture();
    persistence
        .append_vector_partition_memberships_batch(&[placement("first", 1, None)])
        .unwrap();
    // Add a raw orphan and compensate the copied count without changing the
    // canonical projection. Cardinality alone would now look consistent.
    persistence
        .with_db(|db| {
            let write = db.begin_write().map_err(RedbPersistence::storage_error)?;
            insert_orphan_body_and_compensate_count(&write);
            write.commit().map_err(RedbPersistence::storage_error)
        })
        .unwrap();
    // Open no longer re-proves every membership (lazy open); the compensated
    // count still cannot hide the orphan, which open refuses by its body key.
    let error = decode_catalog(&persistence).unwrap_err();
    assert!(
        format!("{error:?}").contains("a vector body key is absent from the committed directory"),
        "{error:?}"
    );
}

#[test]
fn damaged_body_root_retains_the_physical_orphan_key_check() {
    let (_dir, persistence) = fixture();
    persistence
        .append_vector_partition_memberships_batch(&[placement("first", 1, None)])
        .unwrap();
    persistence
        .with_db(|db| {
            let write = db.begin_write().map_err(RedbPersistence::storage_error)?;
            insert_orphan_body_and_compensate_count(&write);
            // Rebind only the copied completeness count in this adversarial
            // fixture. The old physical body fingerprint remains mismatched.
            RedbPersistence::apply_content_identity_mutations_in_write(
                &write,
                &[],
                &mut persistence.proof_admission(),
            )?;
            write.commit().map_err(RedbPersistence::storage_error)
        })
        .unwrap();
    let error = decode_catalog(&persistence).unwrap_err();
    assert!(
        format!("{error:?}").contains("a vector body key is absent from the committed directory"),
        "{error:?}"
    );
}
