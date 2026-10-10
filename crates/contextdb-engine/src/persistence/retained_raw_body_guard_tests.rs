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

fn corruption(reason: String) -> Error {
    Error::StoreCorrupted {
        path: "retained-test.db".into(),
        reason,
    }
}

#[test]
fn retained_raw_body_guard_accepts_each_quantization_and_preserves_identity() {
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
    let calibration = crate::read_image_memory::ImageMemory::new(None);
    calibration
        .scope(|| RedbPersistence::decode::<PersistedVectorEntry>(&encoded))
        .unwrap();
    let decode_charge = calibration.held();
    // The guarded decoder admits its fixed control transient before parsing.
    // That transient is refunded; the body still owns exactly decode_charge.
    let decode_control = std::mem::size_of::<PersistedVectorEntry>() * 4 + 128;
    let memory = crate::read_image_memory::ImageMemory::new(Some(decode_charge + decode_control));
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
    let memory = crate::read_image_memory::ImageMemory::new(Some(0));
    let error = memory
        .scope(|| decode_retained_raw_vector_entry(&encoded, &membership, 1, corruption))
        .unwrap_err();
    assert!(
        matches!(error, Error::StoreCorrupted { reason, .. } if reason.contains("encoded body width"))
    );
    assert_eq!(memory.held(), 0);
}
