use super::*;
use crate::persistence::{
    RedbPersistence, VectorPartitionBaseGenerationRecord, VectorPartitionGraphBytes,
};
use contextdb_core::{Lsn, TxId, VectorIndexRef, VectorPartitionKey, VectorQuantization};

fn prior_base_fixture() -> (
    tempfile::TempDir,
    RedbPersistence,
    VectorPartitionBaseGenerationRecord,
) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("prior-base-cold-root.db");
    let database = crate::Database::open(&path).unwrap();
    database.close().unwrap();
    drop(database);
    let record = VectorPartitionBaseGenerationRecord {
        index: VectorIndexRef::new("items", "embedding"),
        partition_key: VectorPartitionKey::unpartitioned(),
        generation_id: 7,
        base_tx: TxId(4),
        base_lsn: Lsn(5),
        highest_included_tx: TxId(6),
        highest_included_lsn: Lsn(8),
        dimension: 768,
        quantization: VectorQuantization::F32,
        algorithm_version: 1,
        graph_format_version: 1,
        graph: VectorPartitionGraphBytes::verified(vec![13; GRAPH_BYTES]),
        state_digest: [17; 32],
    };
    // Install a valid immutable base directly: one oversized leaf is
    // the root that open_table must read before the loader can call get.
    let mut key = Vec::new();
    for component in [
        record.index.table.as_bytes(),
        record.index.column.as_bytes(),
        record.partition_key.canonical_bytes().as_slice(),
    ] {
        key.extend_from_slice(&(component.len() as u64).to_be_bytes());
        key.extend_from_slice(component);
    }
    key.extend_from_slice(&record.generation_id.to_be_bytes());
    let payload = bincode::serde::encode_to_vec(&record, bincode::config::standard()).unwrap();
    let mut encoded = b"contextdb-vector-partition".to_vec();
    encoded.extend_from_slice(&1u16.to_be_bytes());
    encoded.push(4); // Existing immutable base-generation record kind.
    encoded.extend_from_slice(&(payload.len() as u64).to_be_bytes());
    encoded.extend_from_slice(&payload);
    let checksum = blake3::hash(&encoded);
    encoded.extend_from_slice(checksum.as_bytes());
    {
        let db = redb::Database::open(&path).unwrap();
        let write = db.begin_write().unwrap();
        {
            let mut table = write.open_table(GENERATIONS).unwrap();
            table.insert(key.as_slice(), encoded.as_slice()).unwrap();
        }
        write.commit().unwrap();
    }
    let persistence = RedbPersistence::open_with_cache(&path, CACHE_BYTES).unwrap();
    (directory, persistence, record)
}

#[test]
fn prior_base_loader_cold_root_preserves_source_refusal_and_releases_charge() {
    let (_directory, persistence, record) = prior_base_fixture();
    let accountant = Arc::new(MemoryAccountant::with_budget(128 * 1024 * 1024));
    accountant.set_budget(Some(CACHE_BYTES + 1024)).unwrap();
    accountant.try_allocate(CACHE_BYTES).unwrap();
    let largest_read = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&largest_read);
    let observation = redb::observe_backend_reads(Arc::new(move |bytes| {
        observed.fetch_max(bytes, Ordering::SeqCst);
    }));
    let error = with_source_page_admission(accountant.clone().into(), || {
        persistence.load_vector_partition_base_generation_with_expected_graph_bytes(
            &record.index,
            &record.partition_key,
            record.generation_id,
            GRAPH_BYTES as u64,
        )
    })
    .unwrap_err();
    match error {
        Error::MemoryBudgetExceeded {
            subsystem,
            operation,
            requested_bytes,
            available_bytes,
            budget_limit_bytes,
            hint,
        } => {
            assert_eq!(subsystem, "vector_maintenance");
            assert_eq!(operation, "read_vector_generation_source_page");
            assert!(requested_bytes > GRAPH_BYTES);
            assert_eq!(available_bytes, 1024);
            assert_eq!(budget_limit_bytes, CACHE_BYTES + 1024);
            assert_eq!(
                hint,
                "Raise MEMORY_LIMIT before reading this maintained generation's source page."
            );
        }
        other => panic!("cold root refusal was reclassified as corruption: {other:?}"),
    }
    assert!(largest_read.load(Ordering::SeqCst) < GRAPH_BYTES);
    assert_eq!(accountant.usage().used, CACHE_BYTES);
    drop(observation);
    accountant.set_budget(Some(128 * 1024 * 1024)).unwrap();
    let loaded = with_source_page_admission(accountant.clone().into(), || {
        persistence.load_vector_partition_base_generation_with_expected_graph_bytes(
            &record.index,
            &record.partition_key,
            record.generation_id,
            GRAPH_BYTES as u64,
        )
    })
    .unwrap();
    assert_eq!(
        loaded, record,
        "a refused source leaves the prior base intact"
    );
    // The returned graph owns its exact decoded bytes, so no redb page handle
    // survives the loader. All scoped source charges have released already.
    assert_eq!(accountant.usage().used, CACHE_BYTES);
    accountant.release(CACHE_BYTES);
    assert_eq!(accountant.usage().used, 0);
    assert_eq!(accountant.underflow_count_for_test(), 0);
    persistence.close();
}
