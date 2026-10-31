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
#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
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
#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
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
