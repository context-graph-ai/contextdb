use super::*;
use content_identity::proof_allocation_oracle_tests::measured;
use serial_test::serial;

fn writer() -> (tempfile::TempDir, RedbPersistence) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("metadata-decode.db");
    let database = crate::Database::open(&path).unwrap();
    database.close().unwrap();
    (directory, RedbPersistence::open(&path).unwrap())
}

fn envelope(kind: VectorPartitionRecordKind, payload: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(VECTOR_PARTITION_RECORD_MAGIC);
    bytes.extend_from_slice(&VECTOR_PARTITION_RECORD_VERSION.to_be_bytes());
    bytes.push(kind as u8);
    bytes.extend_from_slice(&(payload.len() as u64).to_be_bytes());
    bytes.extend_from_slice(payload);
    bytes.extend_from_slice(blake3::hash(&bytes).as_bytes());
    bytes
}

fn hostile_route(index: &VectorIndexRef) -> Vec<u8> {
    // A tiny authenticated record claims a huge component Vec. Serde's normal
    // Vec visitor reserves its capped initial allocation before noticing EOF.
    RedbPersistence::encode(&(index, u64::MAX)).unwrap()
}

fn descriptor() -> VectorPartitionGenerationDescriptor {
    VectorPartitionGenerationDescriptor {
        generation_id: 1,
        covered_tx: TxId(2),
        covered_lsn: Lsn(3),
        resident_bytes: 4,
        durable_bytes: 5,
        policy_revision: 6,
        hnsw_m: 24,
        hnsw_ef_construction: 64,
    }
}

fn catalog(
    index: VectorIndexRef,
    key: VectorPartitionKey,
) -> VectorPartitionGenerationCatalogRecord {
    VectorPartitionGenerationCatalogRecord {
        index,
        partition_key: key,
        base: descriptor(),
        change: Some(descriptor()),
        published_tx: TxId(2),
        published_lsn: Lsn(3),
    }
}

fn membership(index: VectorIndexRef, key: VectorPartitionKey) -> VectorPartitionMembershipRecord {
    VectorPartitionMembershipRecord {
        index,
        partition_key: key,
        row_id: RowId(1),
        row_created_tx: TxId(2),
        row_lsn: Lsn(3),
        vector_created_tx: TxId(2),
        vector_lsn: Lsn(3),
        visible_from: TxId(2),
        deleted_tx: None,
    }
}

#[test]
#[serial]
fn short_authenticated_metadata_hints_are_rejected_before_the_serde_vec_reservation() {
    let (_directory, persistence) = writer();
    let index = VectorIndexRef::new("items", "v");
    let partition_key = VectorPartitionKey::unpartitioned();
    for kind in [
        VectorPartitionRecordKind::Membership,
        VectorPartitionRecordKind::GenerationCatalog,
    ] {
        let bytes = envelope(kind, &hostile_route(&index));
        let (unrestricted, old_peak, _) = measured(|| {
            persistence
                .decode_vector_partition_record::<VectorPartitionMembershipRecord>(kind, &bytes)
        });
        assert!(unrestricted.is_err());
        assert!(
            old_peak < 4096,
            "guarded generic decode must reject before a large reservation: {old_peak}"
        );
        drop(unrestricted);
        let (result, guarded_peak, _) = measured(|| {
            persistence
                .decode_expected_vector_partition_metadata::<VectorPartitionMembershipRecord>(
                    kind,
                    &bytes,
                    &index,
                    &partition_key,
                )
        });
        assert!(matches!(result, Err(Error::StoreCorrupted { .. })));
        assert!(guarded_peak < 4096, "guard allocated {guarded_peak} bytes");
        drop(result);
        let (result, inspected_peak, _) = measured(|| {
            persistence
                .decode_vector_partition_record::<VectorPartitionMembershipRecord>(kind, &bytes)
        });
        assert!(matches!(result, Err(Error::StoreCorrupted { .. })));
        assert!(
            inspected_peak < 4096,
            "arbitrary-route inspection allocated {inspected_peak} bytes"
        );
        drop(result);
    }
}

#[test]
#[serial]
fn journal_inspection_covers_the_repeated_route_in_both_changes() {
    let (_directory, persistence) = writer();
    let index = VectorIndexRef::new("items", "v");
    let key = VectorPartitionKey::unpartitioned();
    for variant in [0u32, 1] {
        let mut payload = RedbPersistence::encode(&(&index, &key, Lsn(3), 0u64, variant)).unwrap();
        payload.extend_from_slice(&hostile_route(&index));
        let bytes = envelope(VectorPartitionRecordKind::Journal, &payload);
        let (result, peak, _) = measured(|| {
            persistence.decode_expected_vector_partition_metadata::<VectorPartitionJournalRecord>(
                VectorPartitionRecordKind::Journal,
                &bytes,
                &index,
                &key,
            )
        });
        assert!(matches!(result, Err(Error::StoreCorrupted { .. })));
        assert!(peak < 4096, "nested route allocated {peak} bytes");
        drop(result);
        let (result, peak, _) = measured(|| {
            persistence.decode_vector_partition_record::<VectorPartitionJournalRecord>(
                VectorPartitionRecordKind::Journal,
                &bytes,
            )
        });
        assert!(matches!(result, Err(Error::StoreCorrupted { .. })));
        assert!(peak < 4096, "nested arbitrary route allocated {peak} bytes");
        drop(result);
    }
}

#[test]
#[serial]
fn valid_catalog_attribution_borrows_all_route_types_and_rejects_invalid_suffixes() {
    let index = VectorIndexRef::new("unknown-table", "unknown-column");
    let key = VectorPartitionKey::from_components(vec![
        VectorPartitionComponent::Uuid(uuid::Uuid::nil()),
        VectorPartitionComponent::Text("x".repeat(65_000)),
        VectorPartitionComponent::Integer(-7),
        VectorPartitionComponent::Boolean(true),
        VectorPartitionComponent::Timestamp(-8),
        VectorPartitionComponent::TxId(TxId(9)),
    ]);
    let record = catalog(index.clone(), key.clone());
    let bytes = RedbPersistence::encode_vector_partition_record(
        VectorPartitionRecordKind::GenerationCatalog,
        &record,
    )
    .unwrap();
    let (attributable, peak, _) =
        measured(|| vector_partition_catalog_has_expected_route(&bytes, &index, &key));
    assert!(attributable);
    assert_eq!(peak, 0, "valid attribution owns no metadata");
    let payload =
        vector_partition_record_payload(VectorPartitionRecordKind::GenerationCatalog, &bytes)
            .unwrap();
    let (inspection, peak, _) = measured(|| {
        prevalidate_vector_partition_metadata_routes(
            VectorPartitionRecordKind::GenerationCatalog,
            payload,
        )
    });
    assert!(inspection.is_ok());
    assert_eq!(peak, 0);
    for invalid_payload in [
        payload[..payload.len() - 1].to_vec(),
        [payload, &[0]].concat(),
        hostile_route(&index),
    ] {
        let invalid = envelope(
            VectorPartitionRecordKind::GenerationCatalog,
            &invalid_payload,
        );
        assert!(!vector_partition_catalog_has_expected_route(
            &invalid, &index, &key
        ));
    }
    assert!(!vector_partition_catalog_has_expected_route(
        &bytes,
        &VectorIndexRef::new("other", "v"),
        &key
    ));
}

#[test]
fn valid_metadata_keeps_both_journal_variants_and_unknown_catalog_inventory() {
    let (_directory, persistence) = writer();
    let index = VectorIndexRef::new("unknown-table", "v");
    let key = VectorPartitionKey::from_components(vec![VectorPartitionComponent::Text(
        "partition".into(),
    )]);
    let entry = membership(index.clone(), key.clone());
    for change in [
        VectorPartitionJournalChange::Upsert(entry.clone()),
        VectorPartitionJournalChange::Tombstone(VectorPartitionTombstoneRecord {
            membership: entry.clone(),
            deleted_tx: TxId(4),
            lsn: Lsn(5),
            cause: VectorPartitionTombstoneCause::Delete,
        }),
    ] {
        let journal = VectorPartitionJournalRecord {
            index: index.clone(),
            partition_key: key.clone(),
            lsn: Lsn(5),
            ordinal: 0,
            change,
        };
        let bytes = RedbPersistence::encode_vector_partition_record(
            VectorPartitionRecordKind::Journal,
            &journal,
        )
        .unwrap();
        let decoded: VectorPartitionJournalRecord = persistence
            .decode_expected_vector_partition_metadata(
                VectorPartitionRecordKind::Journal,
                &bytes,
                &index,
                &key,
            )
            .unwrap();
        assert_eq!(decoded, journal);
        assert!(
            prevalidate_vector_partition_metadata_routes(
                VectorPartitionRecordKind::Journal,
                vector_partition_record_payload(VectorPartitionRecordKind::Journal, &bytes)
                    .unwrap()
            )
            .is_ok()
        );
    }
    let record = catalog(index.clone(), key.clone());
    let bytes = RedbPersistence::encode_vector_partition_record(
        VectorPartitionRecordKind::GenerationCatalog,
        &record,
    )
    .unwrap();
    let physical_key = RedbPersistence::vector_partition_catalog_key(&record);
    persistence
        .with_db(|db| {
            let write = db.begin_write().unwrap();
            write
                .open_table(VECTOR_PARTITION_GENERATION_CATALOG_TABLE)
                .unwrap()
                .insert(physical_key.as_slice(), bytes.as_slice())
                .unwrap();
            write.commit().unwrap();
            Ok(())
        })
        .unwrap();
    let inventory = persistence
        .inspect_vector_partition_generation_catalog(&[])
        .unwrap();
    assert!(inventory.quarantines.is_empty());
    assert_eq!(inventory.catalogs, vec![record.clone()]);
    assert_eq!(
        persistence
            .load_vector_partition_generation_catalog_for(&index, &key)
            .unwrap(),
        Some(record)
    );
}

#[test]
fn malformed_and_misplaced_catalogs_keep_existing_inventory_attribution() {
    let (_directory, persistence) = writer();
    let index = VectorIndexRef::new("items", "v");
    let key = VectorPartitionKey::unpartitioned();
    let route = VectorPartitionRef::new(index.clone(), key.clone());
    let physical_key = RedbPersistence::vector_partition_catalog_key_from_parts(&index, &key);
    let bytes = envelope(
        VectorPartitionRecordKind::GenerationCatalog,
        &hostile_route(&index),
    );
    persistence
        .with_db(|db| {
            let write = db.begin_write().unwrap();
            {
                let mut table = write
                    .open_table(VECTOR_PARTITION_GENERATION_CATALOG_TABLE)
                    .unwrap();
                table
                    .insert(physical_key.as_slice(), bytes.as_slice())
                    .unwrap();
                table
                    .insert(b"unattributed-malformed".as_slice(), bytes.as_slice())
                    .unwrap();
            }
            write.commit().unwrap();
            Ok(())
        })
        .unwrap();
    let inventory = persistence
        .inspect_vector_partition_generation_catalog(std::slice::from_ref(&route))
        .unwrap();
    assert!(inventory.catalogs.is_empty());
    assert_eq!(
        inventory.quarantines,
        vec![(route.clone(), VectorRouteQuarantineReason::CorruptBase)]
    );
    assert!(matches!(
        persistence.load_vector_partition_generation_catalog_for(&index, &key),
        Err(Error::StoreCorrupted { .. })
    ));
    let valid = RedbPersistence::encode_vector_partition_record(
        VectorPartitionRecordKind::GenerationCatalog,
        &catalog(index, key),
    )
    .unwrap();
    persistence
        .with_db(|db| {
            let write = db.begin_write().unwrap();
            {
                let mut table = write
                    .open_table(VECTOR_PARTITION_GENERATION_CATALOG_TABLE)
                    .unwrap();
                table.remove(physical_key.as_slice()).unwrap();
                table
                    .insert(b"misplaced-valid".as_slice(), valid.as_slice())
                    .unwrap();
            }
            write.commit().unwrap();
            Ok(())
        })
        .unwrap();
    let inventory = persistence
        .inspect_vector_partition_generation_catalog(&[])
        .unwrap();
    assert!(inventory.catalogs.is_empty());
    assert_eq!(
        inventory.quarantines,
        vec![(route, VectorRouteQuarantineReason::CorruptBase)]
    );
}
