use super::*;
use content_identity::proof_allocation_oracle_tests::measured;
use serial_test::serial;

fn envelope(kind: VectorPartitionRecordKind, payload: &[u8]) -> Vec<u8> {
    let mut bytes = VECTOR_PARTITION_RECORD_MAGIC.to_vec();
    bytes.extend_from_slice(&VECTOR_PARTITION_RECORD_VERSION.to_be_bytes());
    bytes.push(kind as u8);
    bytes.extend_from_slice(&(payload.len() as u64).to_be_bytes());
    bytes.extend_from_slice(payload);
    bytes.extend_from_slice(blake3::hash(&bytes).as_bytes());
    bytes
}

fn records(
    quantization: VectorQuantization,
) -> (
    VectorPartitionBaseGenerationRecord,
    VectorPartitionChangeGenerationRecord,
) {
    let base = VectorPartitionBaseGenerationRecord {
        index: VectorIndexRef::new("items", "v"),
        partition_key: VectorPartitionKey::from_components(
            vec![contextdb_core::VectorPartitionComponent::Boolean(true); 4096],
        ),
        generation_id: 1,
        base_tx: TxId(1),
        base_lsn: Lsn(1),
        highest_included_tx: TxId(2),
        highest_included_lsn: Lsn(2),
        dimension: 768,
        quantization,
        algorithm_version: 1,
        graph_format_version: 1,
        graph: VectorPartitionGraphBytes::verified(vec![3; 1024 * 1024 + 13]),
        state_digest: [0; 32],
    };
    let change = VectorPartitionChangeGenerationRecord {
        index: base.index.clone(),
        partition_key: base.partition_key.clone(),
        base_generation_id: 1,
        change_generation_id: 2,
        covered_through_tx: TxId(3),
        covered_through_lsn: Lsn(3),
        fresh_tail_from_tx: TxId(3),
        fresh_tail_from_lsn: Lsn(3),
        dimension: base.dimension,
        quantization,
        algorithm_version: 1,
        graph_format_version: 1,
        graph: VectorPartitionGraphBytes::verified(vec![5; 1024 * 1024 + 17]),
    };
    (base, change)
}

fn corruption(reason: String) -> Error {
    Error::StoreCorrupted {
        path: "test".into(),
        reason,
    }
}

#[test]
#[serial]
fn change_reload_borrows_prior_graph_and_copies_change_at_exact_capacity() {
    for quantization in [
        VectorQuantization::F32,
        VectorQuantization::SQ8,
        VectorQuantization::SQ4,
    ] {
        let (base, change) = records(quantization);
        let base_bytes = RedbPersistence::encode_vector_partition_record(
            VectorPartitionRecordKind::BaseGeneration,
            &base,
        )
        .unwrap();
        let change_bytes = RedbPersistence::encode_vector_partition_record(
            VectorPartitionRecordKind::ChangeGeneration,
            &change,
        )
        .unwrap();
        let (loaded, peak, _) = measured(|| {
            let prior = borrow_vector_partition_base(
                &base_bytes,
                &base.index,
                &base.partition_key,
                1,
                corruption,
            )
            .unwrap();
            assert!(prior.graph.bytes.as_ptr() >= base_bytes.as_ptr());
            borrow_vector_partition_change(
                &change_bytes,
                &base.index,
                &base.partition_key,
                &prior,
                2,
                corruption,
            )
            .unwrap()
            .into_owned()
        });
        assert_eq!(loaded, change);
        assert_eq!(loaded.graph.bytes.capacity(), loaded.graph.bytes.len());
        let route = base.partition_key.estimated_bytes()
            + base.index.table.len()
            + base.index.column.len()
            + 128;
        assert!(
            peak <= change.graph.bytes.len() + route * 8 + 1024,
            "simultaneous owner peak {peak}"
        );
    }
}

#[test]
#[serial]
fn short_authenticated_change_route_and_graph_hints_do_not_allocate_serde_cap() {
    let (base, change) = records(VectorQuantization::F32);
    let base_bytes = RedbPersistence::encode_vector_partition_record(
        VectorPartitionRecordKind::BaseGeneration,
        &base,
    )
    .unwrap();
    let prior =
        borrow_vector_partition_base(&base_bytes, &base.index, &base.partition_key, 1, corruption)
            .unwrap();
    // Persisted route claims a component Vec whose elements are absent.
    let route_hint = RedbPersistence::encode(&(&base.index, u64::MAX)).unwrap();
    // The legitimate route precedes a graph Vec whose huge declared body is absent.
    let graph_hint = RedbPersistence::encode(&(
        &change.index,
        &change.partition_key,
        change.base_generation_id,
        change.change_generation_id,
        change.covered_through_tx,
        change.covered_through_lsn,
        change.fresh_tail_from_tx,
        change.fresh_tail_from_lsn,
        change.dimension,
        change.quantization,
        change.algorithm_version,
        change.graph_format_version,
        u64::MAX,
    ))
    .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("change-decode.db");
    let database = crate::Database::open(&path).unwrap();
    database.close().unwrap();
    let persistence = RedbPersistence::open(&path).unwrap();
    for payload in [&route_hint, &graph_hint] {
        let bytes = envelope(VectorPartitionRecordKind::ChangeGeneration, payload);
        let (old, old_peak, _) = measured(|| {
            persistence.decode_vector_partition_record::<VectorPartitionChangeGenerationRecord>(
                VectorPartitionRecordKind::ChangeGeneration,
                &bytes,
            )
        });
        assert!(old.is_err());
        assert!(
            old_peak < 4096,
            "guarded generic decode must reject before a large reservation: {old_peak}"
        );
        drop(old);
        let (result, peak, _) = measured(|| {
            borrow_vector_partition_change(
                &bytes,
                &base.index,
                &base.partition_key,
                &prior,
                2,
                corruption,
            )
        });
        assert!(matches!(result, Err(Error::StoreCorrupted { .. })));
        assert!(
            peak < base.partition_key.estimated_bytes() * 3 + 4096,
            "short corrupt hint allocated {peak}"
        );
    }
    let mut damaged = change.clone();
    damaged.graph.checksum[0] ^= 1;
    let bytes = RedbPersistence::encode_vector_partition_record(
        VectorPartitionRecordKind::ChangeGeneration,
        &damaged,
    )
    .unwrap();
    let (result, peak, _) = measured(|| {
        borrow_vector_partition_change(
            &bytes,
            &base.index,
            &base.partition_key,
            &prior,
            2,
            corruption,
        )
    });
    assert!(
        matches!(result, Err(Error::StoreCorrupted { reason, .. }) if reason.contains("immutable graph byte checksum mismatch"))
    );
    assert!(
        peak < change.graph.bytes.len(),
        "bad checksum copied graph before validation"
    );
}
