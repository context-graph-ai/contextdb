use super::*;

fn membership() -> VectorPartitionMembershipRecord {
    VectorPartitionMembershipRecord {
        index: VectorIndexRef::new("items", "embedding"),
        partition_key: VectorPartitionKey::unpartitioned(),
        row_id: RowId(1),
        row_created_tx: TxId(1),
        row_lsn: Lsn(1),
        vector_created_tx: TxId(1),
        vector_lsn: Lsn(1),
        visible_from: TxId(1),
        deleted_tx: None,
    }
}

fn corruption(reason: contextdb_core::error::DiagnosticText) -> Error {
    Error::StoreCorrupted {
        path: "retained-test.db".into(),
        reason,
    }
}

#[test]
fn retained_raw_body_guard_accepts_each_quantization_and_preserves_identity() {
    // This raw codec fixture declares its own writer operation; it has no
    // database request whose ambient owner it may borrow.
    let _fixture_decode_scope = crate::read_image_memory::ImageMemory::enter_writer_operation(
        crate::memory_accounting::AccountantHandle::standalone(None, true).unwrap(),
    )
    .unwrap();

    let membership = membership();
    for quantization in [
        VectorQuantization::F32,
        VectorQuantization::SQ8,
        VectorQuantization::SQ4,
    ] {
        let entry = PersistedVectorEntry {
            index: membership.index.clone(),
            row_id: membership.row_id,
            vector: PersistedVector::from_f32(&[0.25], quantization),
            created_tx: TxId(1),
            deleted_tx: None,
            lsn: Lsn(1),
        };
        let encoded = RedbPersistence::encode(&entry).unwrap();
        let decoded =
            decode_retained_raw_vector_entry(&encoded, &membership, 1, corruption).unwrap();
        assert_eq!(decoded.row_id, membership.row_id);
        assert_eq!(decoded.vector, vec![0.25]);
    }
}

#[test]
#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
fn retained_corrupt_sq4_refuses_before_conversion_owner() {
    let membership = membership();
    let corrupt = PersistedVectorEntry {
        index: membership.index.clone(),
        row_id: membership.row_id,
        vector: PersistedVector::SQ4 {
            min: 0.0,
            max: 1.0,
            len: u32::MAX,
            payload: vec![0x12; 4],
        },
        created_tx: TxId(1),
        deleted_tx: None,
        lsn: Lsn(1),
    };
    let encoded = RedbPersistence::encode(&corrupt).unwrap();
    validate_raw_vector_entry_encoded_width(&encoded, &membership.index, 1, corruption).unwrap();
    let calibration = crate::read_image_memory::ImageMemory::new(None)
        .expect("unlimited raw decode calibration image owner");
    calibration
        .scope(|| RedbPersistence::decode::<PersistedVectorEntry>(&encoded))
        .unwrap();
    let decode_charge = calibration.held();
    // The guarded decoder admits its fixed control transient before parsing.
    // That transient is refunded; the body still owns exactly decode_charge.
    let decode_control = std::mem::size_of::<PersistedVectorEntry>() * 4 + 128;
    let memory = crate::read_image_memory::ImageMemory::new(Some(decode_charge + decode_control))
        .expect("original raw decode limit must reach the intended corruption check");
    let error = memory
        .scope(|| decode_retained_raw_vector_entry(&encoded, &membership, 1, corruption))
        .unwrap_err();
    assert!(
        matches!(&error, Error::StoreCorrupted { reason, .. } if reason.contains("body dimension 8 does not match declared dimension 1")),
        "{error:?}"
    );
    assert_eq!(memory.held(), decode_charge);
}

#[test]
#[cfg_attr(
    any(test, feature = "test-seams"),
    expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )
)]
fn retained_oversized_body_is_refused_before_persisted_decode() {
    let membership = membership();
    let oversized = PersistedVectorEntry {
        index: membership.index.clone(),
        row_id: membership.row_id,
        vector: PersistedVector::F32(vec![1.0; 4096]),
        created_tx: TxId(1),
        deleted_tx: None,
        lsn: Lsn(1),
    };
    let encoded = RedbPersistence::encode(&oversized).unwrap();
    let memory = crate::read_image_memory::ImageMemory::new(Some(0))
        .expect("BLOCKED: zero image total must reach the encoded-width corruption check");
    let error = memory
        .scope(|| decode_retained_raw_vector_entry(&encoded, &membership, 1, corruption))
        .unwrap_err();
    assert!(
        matches!(error, Error::StoreCorrupted { reason, .. } if reason.contains("encoded body width"))
    );
    assert_eq!(memory.held(), 0);
}

#[test]
fn raw_vector_encoded_width_accepts_all_quantizations_and_extreme_scalar_framing() {
    // This raw codec fixture declares its own writer operation; it has no
    // database request whose ambient owner it may borrow.
    let _fixture_decode_scope = crate::read_image_memory::ImageMemory::enter_writer_operation(
        crate::memory_accounting::AccountantHandle::standalone(None, true).unwrap(),
    )
    .unwrap();

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("raw-width.db");
    let database = crate::Database::open(&path).unwrap();
    database.close().unwrap();
    let persistence = RedbPersistence::open(&path).unwrap();
    for dimension in [1, 2, 251, 768, 65_536] {
        for quantization in [
            VectorQuantization::F32,
            VectorQuantization::SQ8,
            VectorQuantization::SQ4,
        ] {
            let mut entry = PersistedVectorEntry {
                // UTF-8 lengths are bytes, as in the serialized route strings.
                index: VectorIndexRef::new("資料".repeat(43), "embedding".repeat(31)),
                row_id: RowId(u64::MAX),
                vector: PersistedVector::from_f32(&vec![0.0; dimension], quantization),
                created_tx: TxId(u64::MAX),
                deleted_tx: Some(TxId(u64::MAX)),
                lsn: Lsn(u64::MAX),
            };
            if let PersistedVector::SQ8 { min, max, .. } | PersistedVector::SQ4 { min, max, .. } =
                &mut entry.vector
            {
                *min = f32::MIN;
                *max = f32::MAX;
            }
            let encoded = RedbPersistence::encode(&entry).unwrap();
            persistence
                .validate_raw_vector_entry_encoded_width(&encoded, &entry.index, dimension)
                .unwrap();
            let decoded = RedbPersistence::decode_vector_entry(&encoded).unwrap();
            assert_eq!(decoded.vector.len(), dimension);
            assert_eq!(decoded.index, entry.index);
            assert_eq!(decoded.row_id, entry.row_id);
            assert_eq!(decoded.created_tx, entry.created_tx);
            assert_eq!(decoded.deleted_tx, entry.deleted_tx);
            assert_eq!(decoded.lsn, entry.lsn);
        }
    }
}
