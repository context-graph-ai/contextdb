use super::*;
use crate::persistence::{
    RedbPersistence, VectorPartitionBaseGenerationRecord, VectorPartitionChangeGenerationRecord,
    VectorPartitionGraphBytes,
};
use contextdb_core::{Lsn, TxId, VectorIndexRef, VectorPartitionKey, VectorQuantization};

const CHANGES: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("vector_partition_change_generations");
const DECODE_BYTES: usize = 1024 * 1024;
const SOURCE_HEADROOM: usize = 1024 * 1024;

fn encoded_record(record: &impl serde::Serialize, kind: u8) -> Vec<u8> {
    let payload = bincode::serde::encode_to_vec(record, bincode::config::standard()).unwrap();
    let mut encoded = b"contextdb-vector-partition".to_vec();
    encoded.extend_from_slice(&1u16.to_be_bytes());
    encoded.push(kind);
    encoded.extend_from_slice(&(payload.len() as u64).to_be_bytes());
    encoded.extend_from_slice(&payload);
    let checksum = blake3::hash(&encoded);
    encoded.extend_from_slice(checksum.as_bytes());
    encoded
}

fn change_fixture(
    oversized_base: bool,
) -> (
    tempfile::TempDir,
    RedbPersistence,
    VectorPartitionChangeGenerationRecord,
) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("change-cold-root.db");
    let database = crate::Database::open(&path).unwrap();
    database.close().unwrap();
    drop(database);
    let base = VectorPartitionBaseGenerationRecord {
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
        graph: VectorPartitionGraphBytes::verified(vec![
            13;
            if oversized_base {
                GRAPH_BYTES
            } else {
                32
            }
        ]),
        state_digest: [17; 32],
    };
    let change = VectorPartitionChangeGenerationRecord {
        index: base.index.clone(),
        partition_key: base.partition_key.clone(),
        base_generation_id: base.generation_id,
        change_generation_id: 9,
        covered_through_tx: TxId(10),
        covered_through_lsn: Lsn(11),
        fresh_tail_from_tx: TxId(10),
        fresh_tail_from_lsn: Lsn(11),
        dimension: base.dimension,
        quantization: base.quantization,
        algorithm_version: base.algorithm_version,
        graph_format_version: base.graph_format_version,
        graph: VectorPartitionGraphBytes::verified(vec![
            19;
            if oversized_base {
                32
            } else {
                GRAPH_BYTES
            }
        ]),
    };
    let mut route = Vec::new();
    for component in [
        base.index.table.as_bytes(),
        base.index.column.as_bytes(),
        base.partition_key.canonical_bytes().as_slice(),
    ] {
        route.extend_from_slice(&(component.len() as u64).to_be_bytes());
        route.extend_from_slice(component);
    }
    let mut base_key = route;
    base_key.extend_from_slice(&base.generation_id.to_be_bytes());
    let mut change_key = base_key.clone();
    change_key.extend_from_slice(&change.change_generation_id.to_be_bytes());
    {
        let db = redb::Database::open(&path).unwrap();
        let write = db.begin_write().unwrap();
        {
            let mut table = write.open_table(GENERATIONS).unwrap();
            let encoded = encoded_record(&base, 4);
            table
                .insert(base_key.as_slice(), encoded.as_slice())
                .unwrap();
        }
        {
            let mut table = write.open_table(CHANGES).unwrap();
            let encoded = encoded_record(&change, 5);
            table
                .insert(change_key.as_slice(), encoded.as_slice())
                .unwrap();
        }
        write.commit().unwrap();
    }
    let persistence = RedbPersistence::open_with_cache(&path, CACHE_BYTES).unwrap();
    (directory, persistence, change)
}

fn prove_change_loader_cold_root_refusal(oversized_base: bool) {
    let (_directory, persistence, record) = change_fixture(oversized_base);
    let accountant = Arc::new(MemoryAccountant::with_budget(128 * 1024 * 1024));
    accountant.try_allocate(CACHE_BYTES).unwrap();
    let load = || {
        persistence.load_vector_partition_change_generation(
            &record.index,
            &record.partition_key,
            record.base_generation_id,
            record.change_generation_id,
        )
    };
    let probe_read = Arc::new(AtomicUsize::new(0));
    let observed_probe = Arc::clone(&probe_read);
    let probe_observation = redb::observe_backend_reads(Arc::new(move |bytes| {
        observed_probe.fetch_max(bytes, Ordering::SeqCst);
    }));
    // The source probe succeeds, but its oversized uncached root drops before
    // the actual load. Decode ownership admitted between the two reads cannot
    // make a source refusal at the second open_table into corruption.
    assert_eq!(
        with_source_page_admission(accountant.clone().into(), load).unwrap(),
        record
    );
    drop(probe_observation);
    let root_bytes = probe_read.load(Ordering::SeqCst);
    assert!(
        root_bytes > GRAPH_BYTES,
        "the probe read the oversized cold root"
    );
    assert_eq!(accountant.usage().used, CACHE_BYTES);
    accountant.try_allocate(DECODE_BYTES).unwrap();
    let held = CACHE_BYTES + DECODE_BYTES;
    let limit = held + SOURCE_HEADROOM;
    accountant.set_budget(Some(limit)).unwrap();
    let largest_read = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&largest_read);
    let observation = redb::observe_backend_reads(Arc::new(move |bytes| {
        observed.fetch_max(bytes, Ordering::SeqCst);
    }));
    let error = with_source_page_admission(accountant.clone().into(), load).unwrap_err();
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
            assert!(
                requested_bytes > GRAPH_BYTES,
                "source request {requested_bytes}, available {available_bytes}"
            );
            assert!(available_bytes > 0 && available_bytes <= SOURCE_HEADROOM);
            assert_eq!(budget_limit_bytes, limit);
            assert_eq!(
                hint,
                "Raise MEMORY_LIMIT before reading this maintained generation's source page."
            );
        }
        other => panic!("actual change loader reclassified cold source refusal: {other:?}"),
    }
    assert!(largest_read.load(Ordering::SeqCst) < GRAPH_BYTES);
    assert_eq!(accountant.usage().used, held);
    drop(observation);
    accountant.set_budget(Some(128 * 1024 * 1024)).unwrap();
    assert_eq!(
        with_source_page_admission(accountant.clone().into(), load).unwrap(),
        record
    );
    assert_eq!(
        accountant.usage().used,
        held,
        "all scoped source owners released"
    );
    accountant.release(DECODE_BYTES);
    accountant.release(CACHE_BYTES);
    assert_eq!(accountant.usage().used, 0);
    assert_eq!(accountant.underflow_count_for_test(), 0);
    persistence.close();
}

#[test]
fn change_loader_cold_base_root_preserves_typed_source_refusal_and_retry() {
    prove_change_loader_cold_root_refusal(true);
}

#[test]
fn change_loader_cold_change_root_preserves_typed_source_refusal_and_retry() {
    prove_change_loader_cold_root_refusal(false);
}
