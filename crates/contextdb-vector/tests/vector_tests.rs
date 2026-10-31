#[path = "fixtures/read_owner_test_support.rs"]
mod read_owner_test_support;

use contextdb_core::{
    Error, Lsn, RowId, SnapshotId, TxId, Value, VectorEntry, VectorExecutor, VectorIndexRef,
    VectorQuantization,
};
use contextdb_tx::{TransactionManager, WriteSet, WriteSetApplicator};
use contextdb_vector::{
    MemVectorExecutor, MemoryBudget, VectorCopyAdmission, VectorStore, cosine_similarity,
};
use roaring::RoaringTreemap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Barrier;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
#[cfg(feature = "test-seams")]
use std::sync::mpsc::TryRecvError;
use std::thread;
use std::time::Duration;

const HNSW_THRESHOLD_FOR_TEST: usize = 1000;
const HNSW_ROWS_FOR_TEST: usize = 1024;
const MIN_EXACT_COSINE: f32 = 0.99999;
#[cfg(feature = "test-seams")]
const TEST_TIMEOUT: Duration = Duration::from_secs(5);
#[cfg(feature = "test-seams")]
const NEGATIVE_WINDOW: Duration = Duration::from_millis(250);

#[derive(Debug)]
struct MemoryAccountant {
    limit: usize,
    used: AtomicUsize,
}

impl MemoryAccountant {
    fn no_limit() -> Self {
        Self {
            limit: 0,
            used: AtomicUsize::new(0),
        }
    }
}

impl MemoryBudget for MemoryAccountant {
    fn try_allocate_for(
        &self,
        bytes: usize,
        subsystem: &str,
        operation: &str,
        hint: &str,
    ) -> contextdb_core::Result<()> {
        loop {
            let used = self.used.load(Ordering::SeqCst);
            let available = self.limit.saturating_sub(used);
            if self.limit != 0 && bytes > available {
                return Err(Error::MemoryBudgetExceeded {
                    subsystem: subsystem.to_string().into(),
                    operation: operation.to_string().into(),
                    requested_bytes: bytes,
                    available_bytes: available,
                    budget_limit_bytes: self.limit,
                    hint: hint.to_string().into(),
                });
            }
            if self
                .used
                .compare_exchange(
                    used,
                    used.saturating_add(bytes),
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                )
                .is_ok()
            {
                return Ok(());
            }
        }
    }

    fn release(&self, bytes: usize) {
        let _ = self
            .used
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |used| {
                Some(used.saturating_sub(bytes))
            });
    }
}

impl contextdb_core::read_memory::ReadMemoryBudget for MemoryAccountant {
    fn try_reserve(
        &self,
        bytes: usize,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> contextdb_core::Result<()> {
        MemoryBudget::try_allocate_for(self, bytes, subsystem, operation, hint)
    }
    fn release(&self, bytes: usize) {
        MemoryBudget::release(self, bytes);
    }
}

struct TestStore {
    vector: Arc<VectorStore>,
}

impl WriteSetApplicator for TestStore {
    fn apply(
        &self,
        ws: &WriteSet,
        read_memory: contextdb_core::read_memory::ReadMemoryOwner,
    ) -> contextdb_core::Result<()> {
        let executing_owner = read_owner_test_support::budget(read_memory);
        // Capture the exact committed body before this fixture publishes inserts.
        let mut before_owner = VectorCopyAdmission::new(executing_owner.clone());
        before_owner.reserve(
            ws.vector_deletes
                .len()
                .saturating_mul(std::mem::size_of::<(VectorIndexRef, RowId, TxId, TxId, Lsn)>())
                .saturating_add(
                    ws.vector_deletes
                        .iter()
                        .map(|(index, _, _)| index.table.len().saturating_add(index.column.len()))
                        .sum::<usize>(),
                ),
        )?;
        let mut deletes = Vec::with_capacity(ws.vector_deletes.len());
        for (index, row, tx) in &ws.vector_deletes {
            let before = self
                .vector
                .live_entry_for_row(
                    index,
                    *row,
                    contextdb_core::SnapshotId(tx.0),
                    &mut before_owner,
                )?
                .expect("the fixture deletes an existing committed body");
            deletes.push((index.clone(), *row, *tx, before.created_tx, before.lsn));
        }

        insert_fixture_vectors(
            &self.vector,
            ws.vector_inserts.clone(),
            None,
            VectorCopyAdmission::new(executing_owner.clone()),
        )?;
        self.vector
            .apply_deletes(deletes, VectorCopyAdmission::new(executing_owner.clone()))?;
        Ok(())
    }

    fn new_row_id(&self) -> RowId {
        RowId(1)
    }
}

fn setup(
    accountant: Arc<MemoryAccountant>,
) -> (
    Arc<TransactionManager<TestStore>>,
    MemVectorExecutor<TestStore>,
) {
    let vector = Arc::new(VectorStore::new());
    vector
        .register_index(
            index(),
            2,
            VectorQuantization::F32,
            Arc::new(MemoryAccountant::no_limit()),
        )
        .unwrap();
    let tx_mgr = Arc::new(TransactionManager::new(TestStore {
        vector: vector.clone(),
    }));
    let exec = MemVectorExecutor::new(vector, tx_mgr.clone(), accountant.clone());
    (tx_mgr, exec)
}

fn index() -> VectorIndexRef {
    VectorIndexRef::new("vectors", "embedding")
}

fn ref_named(table: &str, column: &str) -> VectorIndexRef {
    VectorIndexRef::new(table, column)
}

fn setup_refs(
    refs: &[(VectorIndexRef, usize)],
    accountant: Arc<MemoryAccountant>,
) -> (
    Arc<VectorStore>,
    Arc<TransactionManager<TestStore>>,
    Arc<MemVectorExecutor<TestStore>>,
) {
    let vector = Arc::new(VectorStore::new());
    for (index, dim) in refs {
        vector
            .register_index(
                index.clone(),
                *dim,
                VectorQuantization::F32,
                Arc::new(MemoryAccountant::no_limit()),
            )
            .unwrap();
    }
    let tx_mgr = Arc::new(TransactionManager::new(TestStore {
        vector: vector.clone(),
    }));
    let exec = Arc::new(MemVectorExecutor::new_with_accountant(
        vector.clone(),
        tx_mgr.clone(),
        accountant,
    ));
    (vector, tx_mgr, exec)
}

fn test_entry(index: &VectorIndexRef, row_id: u64, vector: Vec<f32>, tx: u64) -> VectorEntry {
    VectorEntry {
        index: index.clone(),
        row_id: RowId(row_id),
        vector,
        created_tx: TxId(tx),
        deleted_tx: None,
        lsn: Lsn(tx),
    }
}

fn ranked_vector(rank: usize, dim: usize) -> Vec<f32> {
    let score = (1.0 - (rank as f32 * 0.0005)).clamp(0.05, 1.0);
    let side = (1.0 - score * score).max(0.0).sqrt();
    let mut vector = vec![0.0; dim];
    vector[0] = score;
    if dim > 1 {
        vector[1] = side;
    }
    if dim > 2 {
        vector[2] = (rank % 7) as f32 * 0.00001;
    }
    vector
}

fn exact_vector(dim: usize, axis: usize) -> Vec<f32> {
    let mut vector = vec![0.0; dim];
    vector[axis.min(dim - 1)] = 1.0;
    vector
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn insert_fixture_vectors(
    store: &VectorStore,
    entries: Vec<VectorEntry>,
    graph_owner: Option<&dyn MemoryBudget>,
    executing_owner: VectorCopyAdmission,
) -> contextdb_core::Result<()> {
    let mut bodies = Vec::new();
    for entry in &entries {
        let mut retained = store.retained_column_copy_owner(&entry.index, &executing_owner)?;
        let bytes = entry.vector.len() * std::mem::size_of::<f32>();
        retained.reserve_for(bytes, "test", "caller_vector_body", "test")?;
        bodies.push(retained);
    }
    let result = store.apply_inserts_with_accountant(entries, graph_owner, executing_owner);
    if result.is_ok() {
        for owner in bodies {
            owner.transfer();
        }
    }
    result
}

fn seed_ranked(
    store: &VectorStore,
    index: &VectorIndexRef,
    start_row: u64,
    count: usize,
    dim: usize,
    tx_base: u64,
    accountant: &MemoryAccountant,
) {
    let entries = (0..count)
        .map(|i| {
            test_entry(
                index,
                start_row + i as u64,
                ranked_vector(i, dim),
                tx_base + i as u64,
            )
        })
        .collect::<Vec<_>>();
    insert_fixture_vectors(
        store,
        entries,
        Some(accountant),
        VectorCopyAdmission::new(Arc::new(MemoryAccountant::no_limit())),
    )
    .unwrap();
}

fn vector_count_for(
    store: &VectorStore,
    index: &VectorIndexRef,
    _accountant: &Arc<MemoryAccountant>,
) -> usize {
    store
        .entries_for_index(
            index,
            &mut VectorCopyAdmission::new(Arc::new(MemoryAccountant::no_limit())),
        )
        .unwrap()
        .into_iter()
        .filter(|entry| entry.deleted_tx.is_none())
        .count()
}

fn assert_top_row(result: &[(RowId, f32)], row_id: RowId) {
    assert_eq!(result.first().map(|(row, _)| *row), Some(row_id));
    assert!(
        result.first().map(|(_, cos)| *cos).unwrap_or_default() >= MIN_EXACT_COSINE,
        "top row cosine too low: {result:?}"
    );
}

#[cfg(feature = "test-seams")]
fn default_bulk_ref() -> VectorIndexRef {
    VectorIndexRef::default()
}

#[test]
fn cosine_similarity_known_vectors() {
    assert!((cosine_similarity(&[1.0, 0.0], &[1.0, 0.0]) - 1.0).abs() < 1e-6);
    assert!((cosine_similarity(&[1.0, 0.0], &[0.0, 1.0]) - 0.0).abs() < 1e-6);
}

#[test]
fn search_returns_top_k_descending() {
    let accountant = Arc::new(MemoryAccountant::no_limit());
    let (tx_mgr, exec) = setup(accountant.clone());
    let tx = tx_mgr.begin();
    exec.insert_vector(
        tx,
        index(),
        RowId(1),
        vec![1.0, 0.0],
        Arc::new(MemoryAccountant::no_limit()).into(),
    )
    .unwrap();
    exec.insert_vector(
        tx,
        index(),
        RowId(2),
        vec![0.9, 0.1],
        Arc::new(MemoryAccountant::no_limit()).into(),
    )
    .unwrap();
    exec.insert_vector(
        tx,
        index(),
        RowId(3),
        vec![0.0, 1.0],
        Arc::new(MemoryAccountant::no_limit()).into(),
    )
    .unwrap();
    tx_mgr
        .commit(tx, Arc::new(MemoryAccountant::no_limit()))
        .unwrap();

    let r = exec
        .search(
            index(),
            &[1.0, 0.0],
            2,
            None,
            tx_mgr.snapshot(),
            accountant.clone().into(),
        )
        .unwrap();
    assert_eq!(r.len(), 2);
    assert_eq!(r[0].0, RowId(1));
    assert_eq!(r[1].0, RowId(2));
}

#[test]
fn public_search_ties_order_by_row_id_over_a_large_population() {
    let r = ref_named("tie_vectors", "embedding");
    let accountant = Arc::new(MemoryAccountant::no_limit());
    let (store, _tx_mgr, exec) = setup_refs(&[(r.clone(), 2)], accountant.clone());

    insert_fixture_vectors(
        &store,
        vec![
            test_entry(&r, 30, exact_vector(2, 0), 30),
            test_entry(&r, 10, exact_vector(2, 0), 10),
            test_entry(&r, 20, exact_vector(2, 0), 20),
        ],
        Some(accountant.as_ref()),
        VectorCopyAdmission::new(Arc::new(MemoryAccountant::no_limit())),
    )
    .unwrap();
    let brute_force = exec
        .search(
            r.clone(),
            &exact_vector(2, 0),
            3,
            None,
            SnapshotId(10_000),
            accountant.clone().into(),
        )
        .unwrap();
    assert_eq!(
        brute_force
            .iter()
            .map(|(row_id, _)| *row_id)
            .collect::<Vec<_>>(),
        vec![RowId(10), RowId(20), RowId(30)]
    );

    let hnsw_entries = (0..HNSW_ROWS_FOR_TEST)
        .rev()
        .map(|i| test_entry(&r, 1_000 + i as u64, exact_vector(2, 0), 20_000 + i as u64))
        .collect::<Vec<_>>();
    insert_fixture_vectors(
        &store,
        hnsw_entries,
        Some(accountant.as_ref()),
        VectorCopyAdmission::new(Arc::new(MemoryAccountant::no_limit())),
    )
    .unwrap();
    let hnsw = exec
        .search(
            r.clone(),
            &exact_vector(2, 0),
            8,
            None,
            SnapshotId(50_000),
            accountant.clone().into(),
        )
        .unwrap();
    assert_eq!(
        hnsw.iter().map(|(row_id, _)| *row_id).collect::<Vec<_>>(),
        vec![
            RowId(10),
            RowId(20),
            RowId(30),
            RowId(1_000),
            RowId(1_001),
            RowId(1_002),
            RowId(1_003),
            RowId(1_004),
        ]
    );
    assert!(hnsw.iter().all(|(_, score)| (*score - 1.0).abs() < 1e-6));
}

#[test]
fn zero_norm_query_uses_public_row_id_tie_order() {
    let r = ref_named("zero_query_vectors", "embedding");
    let accountant = Arc::new(MemoryAccountant::no_limit());
    let (store, _tx_mgr, exec) = setup_refs(&[(r.clone(), 2)], accountant.clone());

    let mut entries = (1..=500)
        .map(|row| test_entry(&r, row, exact_vector(2, 1), row))
        .collect::<Vec<_>>();
    entries.extend((0..5_500).map(|i| test_entry(&r, 10_000 + i, vec![0.0, 0.0], 20_000 + i)));
    insert_fixture_vectors(
        &store,
        entries,
        Some(accountant.as_ref()),
        VectorCopyAdmission::new(Arc::new(MemoryAccountant::no_limit())),
    )
    .unwrap();
    let rows = exec
        .search(
            r.clone(),
            &[0.0, 0.0],
            8,
            None,
            SnapshotId(30_000),
            accountant.clone().into(),
        )
        .unwrap();

    assert_eq!(
        rows.iter().map(|(row_id, _)| *row_id).collect::<Vec<_>>(),
        (1..=8).map(RowId).collect::<Vec<_>>()
    );
    assert!(rows.iter().all(|(_, score)| *score == 0.0));
}

#[test]
fn candidate_prefilter_is_applied() {
    let accountant = Arc::new(MemoryAccountant::no_limit());
    let (tx_mgr, exec) = setup(accountant.clone());
    let tx = tx_mgr.begin();
    exec.insert_vector(
        tx,
        index(),
        RowId(1),
        vec![1.0, 0.0],
        Arc::new(MemoryAccountant::no_limit()).into(),
    )
    .unwrap();
    exec.insert_vector(
        tx,
        index(),
        RowId(2),
        vec![0.9, 0.1],
        Arc::new(MemoryAccountant::no_limit()).into(),
    )
    .unwrap();
    tx_mgr
        .commit(tx, Arc::new(MemoryAccountant::no_limit()))
        .unwrap();

    let mut cands = RoaringTreemap::new();
    cands.insert(2);

    let r = exec
        .search(
            index(),
            &[1.0, 0.0],
            5,
            Some(&cands),
            tx_mgr.snapshot(),
            accountant.clone().into(),
        )
        .unwrap();
    assert_eq!(r.len(), 1);
    assert_eq!(r[0].0, RowId(2));
}

#[test]
fn mvcc_snapshot_isolation() {
    let accountant = Arc::new(MemoryAccountant::no_limit());
    let (tx_mgr, exec) = setup(accountant.clone());

    let tx1 = tx_mgr.begin();
    exec.insert_vector(
        tx1,
        index(),
        RowId(1),
        vec![1.0, 0.0],
        Arc::new(MemoryAccountant::no_limit()).into(),
    )
    .unwrap();
    tx_mgr
        .commit(tx1, Arc::new(MemoryAccountant::no_limit()))
        .unwrap();

    let snap1 = tx_mgr.snapshot();

    let tx2 = tx_mgr.begin();
    exec.insert_vector(
        tx2,
        index(),
        RowId(2),
        vec![0.9, 0.1],
        Arc::new(MemoryAccountant::no_limit()).into(),
    )
    .unwrap();
    tx_mgr
        .commit(tx2, Arc::new(MemoryAccountant::no_limit()))
        .unwrap();

    let old = exec
        .search(
            index(),
            &[1.0, 0.0],
            10,
            None,
            snap1,
            accountant.clone().into(),
        )
        .unwrap();
    assert_eq!(old.len(), 1);

    let new = exec
        .search(
            index(),
            &[1.0, 0.0],
            10,
            None,
            tx_mgr.snapshot(),
            accountant.clone().into(),
        )
        .unwrap();
    assert_eq!(new.len(), 2);
}

#[test]
fn dimension_mismatch_errors() {
    let accountant = Arc::new(MemoryAccountant::no_limit());
    let (tx_mgr, exec) = setup(accountant.clone());
    let tx1 = tx_mgr.begin();
    exec.insert_vector(
        tx1,
        index(),
        RowId(1),
        vec![1.0, 0.0],
        Arc::new(MemoryAccountant::no_limit()).into(),
    )
    .unwrap();
    tx_mgr
        .commit(tx1, Arc::new(MemoryAccountant::no_limit()))
        .unwrap();

    let tx2 = tx_mgr.begin();
    let err = exec
        .insert_vector(
            tx2,
            index(),
            RowId(2),
            vec![1.0, 0.0, 0.0],
            Arc::new(MemoryAccountant::no_limit()).into(),
        )
        .unwrap_err();
    assert!(matches!(
        err,
        Error::VectorIndexDimensionMismatch {
            index,
            expected: 2,
            actual: 3
        } if index == VectorIndexRef::new("vectors", "embedding")
    ));
}

#[test]
fn k_zero_returns_empty_and_empty_store_returns_empty() {
    let accountant = Arc::new(MemoryAccountant::no_limit());
    let (tx_mgr, exec) = setup(accountant.clone());
    assert!(
        exec.search(
            index(),
            &[1.0],
            0,
            None,
            tx_mgr.snapshot(),
            accountant.clone().into()
        )
        .unwrap()
        .is_empty()
    );
    assert!(
        exec.search(
            index(),
            &[1.0, 0.0],
            10,
            None,
            tx_mgr.snapshot(),
            accountant.clone().into()
        )
        .unwrap()
        .is_empty()
    );
    let err = exec
        .search(
            index(),
            &[1.0],
            10,
            None,
            tx_mgr.snapshot(),
            accountant.clone().into(),
        )
        .unwrap_err();
    assert!(matches!(
        err,
        Error::VectorIndexDimensionMismatch {
            index,
            expected: 2,
            actual: 1
        } if index == VectorIndexRef::new("vectors", "embedding")
    ));
}

#[test]
fn float32_precision_is_preserved() {
    let accountant = Arc::new(MemoryAccountant::no_limit());
    let (tx_mgr, exec) = setup(accountant.clone());
    let tx = tx_mgr.begin();
    exec.insert_vector(
        tx,
        index(),
        RowId(1),
        vec![0.12345679, 0.9876543],
        Arc::new(MemoryAccountant::no_limit()).into(),
    )
    .unwrap();
    tx_mgr
        .commit(tx, Arc::new(MemoryAccountant::no_limit()))
        .unwrap();

    let r = exec
        .search(
            index(),
            &[0.12345679, 0.9876543],
            1,
            None,
            tx_mgr.snapshot(),
            accountant.clone().into(),
        )
        .unwrap();
    assert_eq!(r.len(), 1);

    let _ = Value::Null;
}

#[test]
fn concurrent_same_ref_updates_leave_single_live_vector_after_commit() {
    for first_tx in [30_001, 30_002] {
        concurrent_same_ref_updates_in_order(first_tx);
    }
}

fn concurrent_same_ref_updates_in_order(first_tx: u64) {
    let r = index();
    let accountant = Arc::new(MemoryAccountant::no_limit());
    let (store, _tx_mgr, exec) = setup_refs(&[(r.clone(), 4)], accountant.clone());
    seed_ranked(&store, &r, 1, HNSW_ROWS_FOR_TEST, 4, 10, &accountant);
    exec.search(
        r.clone(),
        &ranked_vector(0, 4),
        1,
        None,
        SnapshotId(20_000),
        accountant.clone().into(),
    )
    .unwrap();

    let publication_gate = Arc::new(std::sync::Mutex::new(()));
    let before_identities = Arc::new(std::sync::Mutex::new(Vec::new()));
    let (first_done, second_ready) = mpsc::channel();
    let second_ready = Arc::new(std::sync::Mutex::new(second_ready));
    let barrier = Arc::new(Barrier::new(3));
    let mut workers = Vec::new();
    for (tx, vector) in [
        (30_001, exact_vector(4, 2)),
        (30_002, vec![0.0, 0.0, 0.8, 0.6]),
    ] {
        let store = store.clone();
        let accountant = accountant.clone();
        let r = r.clone();
        let barrier = barrier.clone();
        let publication_gate = publication_gate.clone();
        let before_identities = before_identities.clone();
        let first_done = first_done.clone();
        let second_ready = second_ready.clone();
        workers.push(thread::spawn(move || {
            barrier.wait();
            if tx != first_tx {
                second_ready.lock().unwrap().recv().unwrap();
            }
            // Raw producers capture the actual before version and publish under one gate.
            let _publication = publication_gate.lock().unwrap();
            let mut before_owner = VectorCopyAdmission::new(Arc::new(MemoryAccountant::no_limit()));
            let before = store
                .live_entry_for_row_in_partition(
                    &r,
                    &contextdb_core::VectorPartitionKey::unpartitioned(),
                    RowId(7),
                    SnapshotId(tx),
                    &mut before_owner,
                )
                .unwrap()
                .expect("publication snapshot has its actual before body");
            before_identities
                .lock()
                .unwrap()
                .push((tx, before.created_tx, before.lsn));
            store
                .apply_changes_with_accountant(
                    vec![(r.clone(), RowId(7), TxId(tx), before.created_tx, before.lsn)],
                    vec![test_entry(&r, 7, vector, tx)],
                    Vec::new(),
                    Lsn(tx),
                    Some(accountant.as_ref()),
                    VectorCopyAdmission::new(Arc::new(MemoryAccountant::no_limit())),
                )
                .unwrap();
            if tx == first_tx {
                first_done.send(()).unwrap();
            }
        }));
    }
    barrier.wait();
    for worker in workers {
        worker.join().unwrap();
    }
    let expected_before = if first_tx == 30_001 {
        vec![
            (30_001, TxId(16), Lsn(16)),
            (30_002, TxId(30_001), Lsn(30_001)),
        ]
    } else {
        vec![(30_002, TxId(16), Lsn(16)), (30_001, TxId(16), Lsn(16))]
    };
    assert_eq!(
        *before_identities.lock().unwrap(),
        expected_before,
        "each raw publication binds its exact snapshot-visible source under the same gate"
    );

    let live_entries = store
        .entries_for_index(
            &r,
            &mut VectorCopyAdmission::new(Arc::new(MemoryAccountant::no_limit())),
        )
        .unwrap()
        .into_iter()
        .filter(|entry| entry.row_id == RowId(7) && entry.deleted_tx.is_none())
        .collect::<Vec<_>>();
    assert_eq!(live_entries.len(), 1);
    let live_vector = live_entries[0].vector.clone();
    let result = exec
        .search(
            r.clone(),
            &live_vector,
            1,
            None,
            SnapshotId(40_000),
            accountant.clone().into(),
        )
        .unwrap();
    assert_top_row(&result, RowId(7));
}

#[test]
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn same_row_updates_published_in_reverse_transaction_order_remain_searchable() {
    let r = index();
    let accountant = Arc::new(MemoryAccountant::no_limit());
    let (store, _tx_mgr, exec) = setup_refs(&[(r.clone(), 4)], accountant.clone());
    seed_ranked(&store, &r, 1, HNSW_ROWS_FOR_TEST, 4, 10, &accountant);
    // Exercise raw-store publication directly. Database commits restamp a
    // late lower transaction above the watermark before applying vectors.
    for (tx, vector) in [
        (30_002, vec![0.0, 0.0, 0.8, 0.6]),
        (30_001, exact_vector(4, 2)),
    ] {
        store
            .apply_changes_with_accountant(
                vec![(r.clone(), RowId(7), TxId(tx), TxId(16), Lsn(16))],
                vec![test_entry(&r, 7, vector, tx)],
                Vec::new(),
                Lsn(tx),
                Some(accountant.as_ref()),
                VectorCopyAdmission::new(Arc::new(MemoryAccountant::no_limit())),
            )
            .unwrap();
    }
    {
        let mode = contextdb_core::VectorSearchMode::Exact;
        let (rows, _) = exec
            .search_with_mode(
                r.clone(),
                &exact_vector(4, 2),
                1,
                None,
                SnapshotId(40_000),
                mode,
                accountant.clone(),
            )
            .unwrap();
        assert_top_row(&rows, RowId(7));
    }
    for snapshot in [30_001, 30_000, 30_002, 40_000] {
        let snapshot = SnapshotId(snapshot);
        let entries = store
            .entries_for_index(
                &r,
                &mut VectorCopyAdmission::new(Arc::new(MemoryAccountant::no_limit())),
            )
            .unwrap();
        let visible = entries
            .iter()
            .filter(|entry| entry.visible_at(snapshot))
            .collect::<Vec<_>>();
        assert_eq!(
            visible
                .iter()
                .filter(|entry| entry.row_id == RowId(7))
                .count(),
            1,
            "each snapshot must expose exactly one physical version of logical row 7 at {snapshot:?}"
        );
        assert_eq!(visible.len(), HNSW_ROWS_FOR_TEST);
        {
            let mode = contextdb_core::VectorSearchMode::Exact;
            let (rows, _) = exec
                .search_with_mode(
                    r.clone(),
                    &exact_vector(4, 2),
                    HNSW_ROWS_FOR_TEST + 2,
                    None,
                    snapshot,
                    mode,
                    accountant.clone(),
                )
                .unwrap();
            assert_eq!(
                rows.len(),
                HNSW_ROWS_FOR_TEST,
                "complete unfiltered {mode:?} result at {snapshot:?}"
            );
            assert_eq!(
                rows.iter().map(|(id, _)| *id).collect::<HashSet<_>>().len(),
                rows.len()
            );
            let point = exec
                .search_with_mode(
                    r.clone(),
                    &exact_vector(4, 2),
                    1,
                    Some(&[7].into_iter().collect()),
                    snapshot,
                    mode,
                    accountant.clone(),
                )
                .unwrap()
                .0;
            assert_eq!(point.len(), 1);
            assert_eq!(rows.iter().find(|(id, _)| *id == RowId(7)), point.first());
        }
        let mut cursor = exec
            .bounded_brute_force_cursor::<Error>(
                r.clone(),
                &exact_vector(4, 2),
                HNSW_ROWS_FOR_TEST + 2,
                None,
                snapshot,
                || Ok(()),
                |_| Ok(()),
                |_| {},
                &mut VectorCopyAdmission::new(Arc::new(MemoryAccountant::no_limit())),
            )
            .unwrap();
        let mut bounded_rows = Vec::new();
        loop {
            match exec
                .bounded_brute_force_step::<Error>(
                    &mut cursor,
                    || Ok(()),
                    || Ok(()),
                    |_| Ok(()),
                    |_| {},
                    &mut VectorCopyAdmission::new(Arc::new(MemoryAccountant::no_limit())),
                )
                .unwrap()
            {
                contextdb_vector::mem::BoundedVectorStep::Pending => {}
                contextdb_vector::mem::BoundedVectorStep::Row(id, score) => {
                    bounded_rows.push((id, score))
                }
                contextdb_vector::mem::BoundedVectorStep::Exhausted => break,
            }
        }
        let (exact, _) = exec
            .search_with_mode(
                r.clone(),
                &exact_vector(4, 2),
                HNSW_ROWS_FOR_TEST + 2,
                None,
                snapshot,
                contextdb_core::VectorSearchMode::Exact,
                accountant.clone(),
            )
            .unwrap();
        assert_eq!(bounded_rows.as_slice(), &*exact);
        let count = exec
            .bounded_authorized_visible_count::<Error>(
                &r,
                None,
                snapshot,
                || Ok(()),
                |_| Ok(()),
                |_| {},
                &mut VectorCopyAdmission::new(Arc::new(MemoryAccountant::no_limit())),
            )
            .unwrap();
        assert_eq!(count, HNSW_ROWS_FOR_TEST);
    }
}

#[test]
fn same_ref_concurrent_writes_brute_force_and_hnsw_agree() {
    let r = index();
    let accountant = Arc::new(MemoryAccountant::no_limit());
    let (store, _tx_mgr, exec) = setup_refs(&[(r.clone(), 4)], accountant.clone());
    let entries = (0..HNSW_THRESHOLD_FOR_TEST)
        .map(|i| {
            test_entry(
                &r,
                1 + i as u64,
                if i < 50 {
                    exact_vector(4, 0)
                } else {
                    ranked_vector(200 + i, 4)
                },
                10 + i as u64,
            )
        })
        .collect::<Vec<_>>();
    insert_fixture_vectors(
        &store,
        entries,
        Some(accountant.as_ref()),
        VectorCopyAdmission::new(Arc::new(MemoryAccountant::no_limit())),
    )
    .unwrap();
    exec.search(
        r.clone(),
        &exact_vector(4, 0),
        1,
        None,
        SnapshotId(20_000),
        accountant.clone().into(),
    )
    .unwrap();

    let barrier = Arc::new(Barrier::new(51));
    let mut workers = Vec::new();
    for i in 0..50 {
        let store = store.clone();
        let accountant = accountant.clone();
        let r = r.clone();
        let barrier = barrier.clone();
        workers.push(thread::spawn(move || {
            barrier.wait();
            insert_fixture_vectors(
                &store,
                vec![test_entry(
                    &r,
                    10_000 + i as u64,
                    ranked_vector(200 + i, 4),
                    30_000 + i as u64,
                )],
                Some(accountant.as_ref()),
                VectorCopyAdmission::new(Arc::new(MemoryAccountant::no_limit())),
            )
            .unwrap();
        }));
    }
    barrier.wait();
    for worker in workers {
        worker.join().unwrap();
    }

    let snapshot = SnapshotId(40_000);
    let hnsw = exec
        .search(
            r.clone(),
            &exact_vector(4, 0),
            50,
            None,
            snapshot,
            accountant.clone().into(),
        )
        .unwrap();
    insert_fixture_vectors(
        &store,
        vec![test_entry(&r, 999_999, exact_vector(4, 3), 50_000)],
        Some(accountant.as_ref()),
        VectorCopyAdmission::new(Arc::new(MemoryAccountant::no_limit())),
    )
    .unwrap();
    let brute = exec
        .search(
            r.clone(),
            &exact_vector(4, 0),
            50,
            None,
            snapshot,
            accountant.clone().into(),
        )
        .unwrap();
    assert_eq!(hnsw.len(), brute.len());
    let hnsw_rows = hnsw.iter().map(|(row, _)| *row).collect::<HashSet<_>>();
    let brute_rows = brute.iter().map(|(row, _)| *row).collect::<HashSet<_>>();
    let expected_rows = (1..=50u64).map(RowId).collect::<HashSet<_>>();
    assert_eq!(hnsw_rows, expected_rows);
    assert_eq!(brute_rows, expected_rows);
    assert!(hnsw.iter().all(|(_, cos)| (*cos - 1.0).abs() < 1e-6));
    assert!(brute.iter().all(|(_, cos)| (*cos - 1.0).abs() < 1e-6));
}

#[test]
fn same_ref_concurrent_deletes_leave_no_deleted_rows_in_results() {
    let r = index();
    let accountant = Arc::new(MemoryAccountant::no_limit());
    let (store, _tx_mgr, exec) = setup_refs(&[(r.clone(), 4)], accountant.clone());
    seed_ranked(&store, &r, 1, HNSW_ROWS_FOR_TEST, 4, 10, &accountant);
    exec.search(
        r.clone(),
        &exact_vector(4, 0),
        1,
        None,
        SnapshotId(20_000),
        accountant.clone().into(),
    )
    .unwrap();

    let barrier = Arc::new(Barrier::new(11));
    let mut workers = Vec::new();
    for row in 1..=10u64 {
        let store = store.clone();
        let accountant = accountant.clone();
        let r = r.clone();
        let barrier = barrier.clone();
        workers.push(thread::spawn(move || {
            barrier.wait();
            store
                .apply_deletes_with_accountant(
                    vec![(
                        r,
                        RowId(row),
                        TxId(30_000 + row),
                        TxId(9 + row),
                        Lsn(9 + row),
                    )],
                    Some(accountant.as_ref()),
                    VectorCopyAdmission::new(Arc::new(MemoryAccountant::no_limit())),
                )
                .unwrap();
        }));
    }
    barrier.wait();
    for worker in workers {
        worker.join().unwrap();
    }
    let result = exec
        .search(
            r.clone(),
            &ranked_vector(20, 4),
            20,
            None,
            SnapshotId(40_000),
            accountant.clone().into(),
        )
        .unwrap();
    assert!(result.iter().all(|(row, _)| row.0 > 10));
}

#[test]
fn multi_ref_apply_batches_with_overlapping_ref_sets_do_not_deadlock() {
    let a = ref_named("a", "embedding");
    let b = ref_named("b", "embedding");
    let accountant = Arc::new(MemoryAccountant::no_limit());
    let (store, _tx_mgr, _exec) = setup_refs(&[(a.clone(), 3), (b.clone(), 3)], accountant.clone());

    for i in 0..100u64 {
        let barrier = Arc::new(Barrier::new(2));
        let (done_tx, done_rx) = mpsc::channel();
        for order in 0..2u64 {
            let store = store.clone();
            let accountant = accountant.clone();
            let a = a.clone();
            let b = b.clone();
            let barrier = barrier.clone();
            let done_tx = done_tx.clone();
            thread::spawn(move || {
                let tx = 10_000 + i * 10 + order;
                let entries = if order == 0 {
                    vec![
                        test_entry(&a, 100_000 + i * 2, exact_vector(3, 0), tx),
                        test_entry(&b, 200_000 + i * 2, exact_vector(3, 1), tx),
                    ]
                } else {
                    vec![
                        test_entry(&b, 200_001 + i * 2, exact_vector(3, 1), tx),
                        test_entry(&a, 100_001 + i * 2, exact_vector(3, 0), tx),
                    ]
                };
                barrier.wait();
                store
                    .apply_changes_with_accountant(
                        Vec::new(),
                        entries,
                        Vec::new(),
                        Lsn(tx),
                        Some(accountant.as_ref()),
                        VectorCopyAdmission::new(Arc::new(MemoryAccountant::no_limit())),
                    )
                    .unwrap();
                done_tx.send(()).unwrap();
            });
        }
        assert!(done_rx.recv_timeout(Duration::from_secs(2)).is_ok());
        assert!(done_rx.recv_timeout(Duration::from_secs(2)).is_ok());
    }
    assert_eq!(vector_count_for(&store, &a, &accountant), 200);
    assert_eq!(vector_count_for(&store, &b, &accountant), 200);
}

#[test]
fn insert_vector_at_wrong_dimension_returns_typed_error_for_target_ref() {
    let a = ref_named("vectors", "embedding");
    let b = ref_named("vectors", "vision");
    let (_store, tx_mgr, exec) = setup_refs(
        &[(a.clone(), 3), (b, 4)],
        Arc::new(MemoryAccountant::no_limit()),
    );
    let tx = tx_mgr.begin();
    let err = exec
        .insert_vector(
            tx,
            a.clone(),
            RowId(1),
            vec![1.0, 0.0, 0.0, 0.0],
            Arc::new(MemoryAccountant::no_limit()).into(),
        )
        .unwrap_err();
    assert!(matches!(
        err,
        Error::VectorIndexDimensionMismatch {
            index,
            expected: 3,
            actual: 4
        } if index == a
    ));
}

#[cfg(feature = "test-seams")]
#[test]
fn concurrent_distinct_ref_applies_both_enter_maintenance_window() {
    use contextdb_vector::test_seam::PauseWindow;

    let a = ref_named("a", "embedding");
    let b = ref_named("b", "embedding");
    let accountant = Arc::new(MemoryAccountant::no_limit());
    let (store, _tx_mgr, _exec) = setup_refs(&[(a.clone(), 3), (b.clone(), 3)], accountant.clone());
    let pause_a = store.arm_maintenance_pause_for_test(&a, PauseWindow::Apply);
    let pause_b = store.arm_maintenance_pause_for_test(&b, PauseWindow::Apply);

    let (done_a_tx, done_a_rx) = mpsc::channel();
    let store_a = store.clone();
    let accountant_a = accountant.clone();
    let a_worker = a.clone();
    thread::spawn(move || {
        insert_fixture_vectors(
            &store_a,
            vec![test_entry(&a_worker, 1, exact_vector(3, 0), 10)],
            Some(accountant_a.as_ref()),
            VectorCopyAdmission::new(Arc::new(MemoryAccountant::no_limit())),
        )
        .unwrap();
        done_a_tx.send(()).unwrap();
    });
    assert!(pause_a.wait_until_reached(TEST_TIMEOUT));

    let (done_b_tx, done_b_rx) = mpsc::channel();
    let store_b = store.clone();
    let accountant_b = accountant.clone();
    let b_worker = b.clone();
    thread::spawn(move || {
        insert_fixture_vectors(
            &store_b,
            vec![test_entry(&b_worker, 2, exact_vector(3, 1), 20)],
            Some(accountant_b.as_ref()),
            VectorCopyAdmission::new(Arc::new(MemoryAccountant::no_limit())),
        )
        .unwrap();
        done_b_tx.send(()).unwrap();
    });
    let b_reached_before_release = pause_b.wait_until_reached(TEST_TIMEOUT);
    let a_done_before_release = done_a_rx.try_recv();
    let b_done_before_release = done_b_rx.try_recv();
    if !b_reached_before_release {
        pause_a.release();
        let _ = pause_b.wait_until_reached(TEST_TIMEOUT);
    }
    pause_b.release();
    let _ = done_b_rx.recv_timeout(TEST_TIMEOUT);
    pause_a.release();
    let _ = done_a_rx.recv_timeout(TEST_TIMEOUT);

    assert!(b_reached_before_release);
    assert!(matches!(a_done_before_release, Err(TryRecvError::Empty)));
    assert!(matches!(b_done_before_release, Err(TryRecvError::Empty)));
    assert_eq!(vector_count_for(&store, &a, &accountant), 1);
    assert_eq!(vector_count_for(&store, &b, &accountant), 1);
}

#[test]
fn search_on_ref_a_returns_only_ref_a_vectors_under_10000_iteration_apply_storm() {
    let a = ref_named("a", "embedding");
    let b = ref_named("b", "embedding");
    let accountant = Arc::new(MemoryAccountant::no_limit());
    let (store, _tx_mgr, exec) = setup_refs(&[(a.clone(), 3), (b.clone(), 3)], accountant.clone());
    seed_ranked(&store, &a, 1, 100, 3, 10, &accountant);
    seed_ranked(&store, &b, 1_000_000, 100, 3, 1_000, &accountant);

    let iterations_per_worker = if cfg!(debug_assertions) { 250 } else { 2500 };
    let search_iterations = if cfg!(debug_assertions) { 1000 } else { 10_000 };
    let mut workers = Vec::new();
    for worker in 0..4u64 {
        let store = store.clone();
        let accountant = accountant.clone();
        let a = a.clone();
        let b = b.clone();
        workers.push(thread::spawn(move || {
            for i in 0..iterations_per_worker {
                let is_a = i % 2 == 0;
                let index = if is_a { &a } else { &b };
                let row = if is_a {
                    10_000 + worker * 10_000 + i
                } else {
                    1_010_000 + worker * 10_000 + i
                };
                insert_fixture_vectors(
                    &store,
                    vec![test_entry(
                        index,
                        row,
                        if is_a {
                            exact_vector(3, 2)
                        } else {
                            exact_vector(3, 0)
                        },
                        20_000 + worker * 3_000 + i,
                    )],
                    Some(accountant.as_ref()),
                    VectorCopyAdmission::new(Arc::new(MemoryAccountant::no_limit())),
                )
                .unwrap();
            }
        }));
    }

    let a_rows = (1..=100u64)
        .chain((0..4u64).flat_map(|worker| {
            (0..iterations_per_worker)
                .filter(|i| i % 2 == 0)
                .map(move |i| 10_000 + worker * 10_000 + i)
        }))
        .map(RowId)
        .collect::<HashSet<_>>();
    for _ in 0..search_iterations {
        let result = exec
            .search(
                a.clone(),
                &exact_vector(3, 0),
                5,
                None,
                SnapshotId(100_000),
                accountant.clone().into(),
            )
            .unwrap();
        assert!(result.iter().all(|(row, _)| a_rows.contains(row)));
    }
    for worker in workers {
        worker.join().unwrap();
    }
}

#[test]
fn dimension_mismatch_typed_error_with_positive_control() {
    let a = ref_named("a", "embedding");
    let b = ref_named("b", "embedding");
    let accountant = Arc::new(MemoryAccountant::no_limit());
    let (store, _tx_mgr, exec) = setup_refs(&[(a.clone(), 3), (b.clone(), 4)], accountant.clone());
    insert_fixture_vectors(
        &store,
        vec![
            test_entry(&a, 1, exact_vector(3, 0), 10),
            test_entry(&b, 2, exact_vector(4, 1), 11),
        ],
        Some(accountant.as_ref()),
        VectorCopyAdmission::new(Arc::new(MemoryAccountant::no_limit())),
    )
    .unwrap();

    let err_a = exec
        .search(
            a.clone(),
            &exact_vector(4, 0),
            1,
            None,
            SnapshotId(20),
            accountant.clone().into(),
        )
        .unwrap_err();
    assert!(matches!(
        err_a,
        Error::VectorIndexDimensionMismatch {
            index,
            expected: 3,
            actual: 4
        } if index == a
    ));
    assert_top_row(
        &exec
            .search(
                a.clone(),
                &exact_vector(3, 0),
                1,
                None,
                SnapshotId(20),
                accountant.clone().into(),
            )
            .unwrap(),
        RowId(1),
    );

    let err_b = exec
        .search(
            b.clone(),
            &exact_vector(3, 0),
            1,
            None,
            SnapshotId(20),
            accountant.clone().into(),
        )
        .unwrap_err();
    assert!(matches!(
        err_b,
        Error::VectorIndexDimensionMismatch {
            index,
            expected: 4,
            actual: 3
        } if index == b
    ));
    assert_top_row(
        &exec
            .search(
                b.clone(),
                &exact_vector(4, 1),
                1,
                None,
                SnapshotId(20),
                accountant.clone().into(),
            )
            .unwrap(),
        RowId(2),
    );
}

#[cfg(feature = "test-seams")]
#[test]
fn prune_row_ids_deterministically_blocks_concurrent_per_ref_insert_then_completes_correctly() {
    use contextdb_vector::test_seam::PauseWindow;

    let a = ref_named("a", "embedding");
    let b = ref_named("b", "embedding");
    let accountant = Arc::new(MemoryAccountant::no_limit());
    let (store, _tx_mgr, _exec) = setup_refs(&[(a.clone(), 3), (b.clone(), 3)], accountant.clone());
    seed_ranked(&store, &a, 1, 100, 3, 10, &accountant);
    seed_ranked(&store, &b, 1_000, 100, 3, 1_000, &accountant);

    let bulk_pause = store.arm_maintenance_pause_for_test(&default_bulk_ref(), PauseWindow::Bulk);
    let apply_pause = store.arm_maintenance_pause_for_test(&b, PauseWindow::Apply);
    let prune_rows = (1..=10u64).map(RowId).collect::<HashSet<_>>();
    let (done_prune_tx, done_prune_rx) = mpsc::channel();
    let store_prune = store.clone();
    let accountant_prune = accountant.clone();
    thread::spawn(move || {
        let pruned = store_prune
            .prune_row_ids(
                &prune_rows,
                &mut VectorCopyAdmission::new(accountant_prune.clone()),
            )
            .unwrap();
        done_prune_tx.send(pruned).unwrap();
    });
    assert!(bulk_pause.wait_until_reached(TEST_TIMEOUT));

    let (started_insert_tx, started_insert_rx) = mpsc::channel();
    let (done_insert_tx, done_insert_rx) = mpsc::channel();
    let store_insert = store.clone();
    let accountant_insert = accountant.clone();
    let b_insert = b.clone();
    thread::spawn(move || {
        started_insert_tx.send(()).unwrap();
        insert_fixture_vectors(
            &store_insert,
            vec![test_entry(&b_insert, 2_000, exact_vector(3, 1), 9_000)],
            Some(accountant_insert.as_ref()),
            VectorCopyAdmission::new(Arc::new(MemoryAccountant::no_limit())),
        )
        .unwrap();
        done_insert_tx.send(()).unwrap();
    });
    assert!(started_insert_rx.recv_timeout(TEST_TIMEOUT).is_ok());
    assert!(!apply_pause.wait_until_reached(NEGATIVE_WINDOW));
    assert!(matches!(
        done_insert_rx.try_recv(),
        Err(TryRecvError::Empty)
    ));

    let ownership_before = store.memory_ownership_snapshot_for_test();
    bulk_pause.release();
    assert_eq!(done_prune_rx.recv_timeout(TEST_TIMEOUT).unwrap(), 0);
    let ownership_after = store.memory_ownership_snapshot_for_test();
    assert_eq!(
        ownership_before.raw_vector_bodies - ownership_after.raw_vector_bodies,
        10 * 3 * std::mem::size_of::<f32>()
    );
    assert_eq!(vector_count_for(&store, &a, &accountant), 90);
    assert!(apply_pause.wait_until_reached(TEST_TIMEOUT));
    apply_pause.release();
    done_insert_rx.recv_timeout(TEST_TIMEOUT).unwrap();
    assert_eq!(vector_count_for(&store, &b, &accountant), 101);
}

#[test]
fn replace_loaded_vectors_post_state_is_correct() {
    let a = ref_named("a", "embedding");
    let b = ref_named("b", "embedding");
    let accountant = Arc::new(MemoryAccountant::no_limit());
    let (store, _tx_mgr, exec) = setup_refs(&[(a.clone(), 3), (b.clone(), 3)], accountant.clone());
    seed_ranked(&store, &a, 1, HNSW_ROWS_FOR_TEST, 3, 10, &accountant);
    seed_ranked(
        &store,
        &b,
        100_000,
        HNSW_ROWS_FOR_TEST,
        3,
        20_000,
        &accountant,
    );
    store
        .replace_loaded_vectors(
            vec![
                test_entry(&a, 7, exact_vector(3, 0), 60_000),
                test_entry(&b, 8, exact_vector(3, 1), 60_001),
            ],
            accountant.clone(),
        )
        .unwrap();
    assert_eq!(vector_count_for(&store, &a, &accountant), 1);
    assert_eq!(vector_count_for(&store, &b, &accountant), 1);
    assert_top_row(
        &exec
            .search(
                a.clone(),
                &exact_vector(3, 0),
                1,
                None,
                SnapshotId(70_000),
                accountant.clone().into(),
            )
            .unwrap(),
        RowId(7),
    );
}

#[cfg(feature = "test-seams")]
#[test]
fn replace_loaded_vectors_blocks_per_ref_apply() {
    use contextdb_vector::test_seam::PauseWindow;

    let a = ref_named("a", "embedding");
    let b = ref_named("b", "embedding");
    let accountant = Arc::new(MemoryAccountant::no_limit());
    let (store, _tx_mgr, _exec) = setup_refs(&[(a.clone(), 3), (b.clone(), 3)], accountant.clone());
    seed_ranked(&store, &a, 1, HNSW_ROWS_FOR_TEST, 3, 10, &accountant);
    seed_ranked(
        &store,
        &b,
        100_000,
        HNSW_ROWS_FOR_TEST,
        3,
        20_000,
        &accountant,
    );
    let bulk_pause = store.arm_maintenance_pause_for_test(&default_bulk_ref(), PauseWindow::Bulk);
    let replacements = (0..HNSW_ROWS_FOR_TEST)
        .flat_map(|i| {
            [
                test_entry(
                    &a,
                    10_000 + i as u64,
                    ranked_vector(i, 3),
                    60_000 + i as u64,
                ),
                test_entry(
                    &b,
                    20_000 + i as u64,
                    ranked_vector(i, 3),
                    70_000 + i as u64,
                ),
            ]
        })
        .collect::<Vec<_>>();
    let (done_replace_tx, done_replace_rx) = mpsc::channel();
    let store_replace = store.clone();
    let accountant_replace = accountant.clone();
    thread::spawn(move || {
        store_replace
            .replace_loaded_vectors(replacements, accountant_replace)
            .unwrap();
        done_replace_tx.send(()).unwrap();
    });
    assert!(bulk_pause.wait_until_reached(TEST_TIMEOUT));

    let apply_pause = store.arm_maintenance_pause_for_test(&b, PauseWindow::Apply);
    let (started_apply_tx, started_apply_rx) = mpsc::channel();
    let (done_apply_tx, done_apply_rx) = mpsc::channel();
    let store_apply = store.clone();
    let accountant_apply = accountant.clone();
    let b_apply = b.clone();
    thread::spawn(move || {
        started_apply_tx.send(()).unwrap();
        insert_fixture_vectors(
            &store_apply,
            vec![test_entry(&b_apply, 99_999, exact_vector(3, 2), 90_000)],
            Some(accountant_apply.as_ref()),
            VectorCopyAdmission::new(Arc::new(MemoryAccountant::no_limit())),
        )
        .unwrap();
        done_apply_tx.send(()).unwrap();
    });
    assert!(started_apply_rx.recv_timeout(TEST_TIMEOUT).is_ok());
    assert!(!apply_pause.wait_until_reached(NEGATIVE_WINDOW));
    assert!(matches!(done_apply_rx.try_recv(), Err(TryRecvError::Empty)));

    bulk_pause.release();
    done_replace_rx.recv_timeout(TEST_TIMEOUT).unwrap();
    apply_pause.release();
    done_apply_rx.recv_timeout(TEST_TIMEOUT).unwrap();
    assert_eq!(
        vector_count_for(&store, &a, &accountant),
        HNSW_ROWS_FOR_TEST
    );
    assert_eq!(
        vector_count_for(&store, &b, &accountant),
        HNSW_ROWS_FOR_TEST + 1
    );
}

#[test]
fn unknown_vector_index_error_under_concurrent_multi_ref_apply_with_positive_control() {
    let a = ref_named("a", "embedding");
    let b = ref_named("b", "embedding");
    let c = ref_named("missing", "embedding");
    let accountant = Arc::new(MemoryAccountant::no_limit());
    let (store, _tx_mgr, exec) = setup_refs(&[(a.clone(), 3), (b.clone(), 3)], accountant.clone());
    seed_ranked(&store, &a, 1, 10, 3, 10, &accountant);
    seed_ranked(&store, &b, 100, 10, 3, 100, &accountant);

    let storm_store = store.clone();
    let storm_accountant = accountant.clone();
    let a_storm = a.clone();
    let b_storm = b.clone();
    let storm = thread::spawn(move || {
        for i in 0..500u64 {
            let index = if i % 2 == 0 { &a_storm } else { &b_storm };
            insert_fixture_vectors(
                &storm_store,
                vec![test_entry(index, 10_000 + i, exact_vector(3, 0), 1_000 + i)],
                Some(storm_accountant.as_ref()),
                VectorCopyAdmission::new(Arc::new(MemoryAccountant::no_limit())),
            )
            .unwrap();
        }
    });

    for _ in 0..500 {
        let err = exec
            .search(
                c.clone(),
                &exact_vector(3, 0),
                1,
                None,
                SnapshotId(10_000),
                accountant.clone().into(),
            )
            .unwrap_err();
        assert!(matches!(err, Error::UnknownVectorIndex { index } if index == c));
        let ok = exec
            .search(
                a.clone(),
                &exact_vector(3, 0),
                1,
                None,
                SnapshotId(10_000),
                accountant.clone().into(),
            )
            .unwrap();
        assert!(!ok.is_empty());
    }
    storm.join().unwrap();
}

#[cfg(feature = "test-seams")]
#[test]
fn selected_index_row_pruning_preserves_unrelated_index_rows_and_answers() {
    use contextdb_core::VectorSearchMode;

    let a = ref_named("items", "embedding_a");
    let b = ref_named("items", "embedding_b");
    let accountant = Arc::new(MemoryAccountant::no_limit());
    let (store, _tx_mgr, exec) = setup_refs(&[(a.clone(), 3), (b.clone(), 3)], accountant.clone());
    for index in [&a, &b] {
        let entries = (1..=4)
            .map(|row| test_entry(index, row, ranked_vector(row as usize, 3), 1))
            .collect::<Vec<_>>();
        insert_fixture_vectors(
            &store,
            entries,
            Some(accountant.as_ref()),
            VectorCopyAdmission::new(Arc::new(MemoryAccountant::no_limit())),
        )
        .unwrap();
    }
    let query = exact_vector(3, 0);
    let search_b = || {
        exec.search_with_mode(
            b.clone(),
            &query,
            4,
            None,
            SnapshotId(1),
            VectorSearchMode::Exact,
            accountant.clone(),
        )
        .unwrap()
        .0
    };
    let answers_before = search_b();
    assert_eq!(answers_before.len(), 4);
    assert_eq!(answers_before[0].0, RowId(1));

    let released = store
        .prune_row_ids_in_indexes(
            &[RowId(1)].into_iter().collect(),
            std::slice::from_ref(&a),
            &mut VectorCopyAdmission::new(Arc::new(MemoryAccountant::no_limit())),
        )
        .unwrap();
    accountant.release(released);

    assert_eq!(vector_count_for(&store, &a, &accountant), 3);
    assert!(
        store
            .entries_for_index(
                &a,
                &mut VectorCopyAdmission::new(Arc::new(MemoryAccountant::no_limit()))
            )
            .unwrap()
            .iter()
            .all(|entry| entry.row_id != RowId(1))
    );
    assert_eq!(vector_count_for(&store, &b, &accountant), 4);
    assert_eq!(&*search_b(), &*answers_before);
}

#[cfg(feature = "test-seams")]
#[test]
fn two_independent_vector_stores_do_not_share_maintenance_locks() {
    use contextdb_vector::test_seam::PauseWindow;

    let r = index();
    let accountant_1 = Arc::new(MemoryAccountant::no_limit());
    let accountant_2 = Arc::new(MemoryAccountant::no_limit());
    let (store_1, _tx_mgr_1, exec_1) = setup_refs(&[(r.clone(), 3)], accountant_1.clone());
    let (store_2, _tx_mgr_2, exec_2) = setup_refs(&[(r.clone(), 3)], accountant_2.clone());
    seed_ranked(&store_1, &r, 1, 100, 3, 10, &accountant_1);
    seed_ranked(&store_2, &r, 1, 100, 3, 10, &accountant_2);

    // Park an insert inside store 1's maintenance window for this index.
    let pause_1 = store_1.arm_maintenance_pause_for_test(&r, PauseWindow::Apply);
    let (done_1_tx, done_1_rx) = mpsc::channel();
    let store_1_worker = store_1.clone();
    let accountant_1_worker = accountant_1.clone();
    let r_1 = r.clone();
    thread::spawn(move || {
        insert_fixture_vectors(
            &store_1_worker,
            vec![test_entry(&r_1, 50_000, exact_vector(3, 1), 60_000)],
            Some(accountant_1_worker.as_ref()),
            VectorCopyAdmission::new(Arc::new(MemoryAccountant::no_limit())),
        )
        .unwrap();
        done_1_tx.send(()).unwrap();
    });
    assert!(pause_1.wait_until_reached(TEST_TIMEOUT));

    // The other store takes the same index's insert and answers a search.
    insert_fixture_vectors(
        &store_2,
        vec![test_entry(&r, 99_999, exact_vector(3, 2), 60_000)],
        Some(accountant_2.as_ref()),
        VectorCopyAdmission::new(Arc::new(MemoryAccountant::no_limit())),
    )
    .unwrap();
    let result_2 = exec_2
        .search(
            r.clone(),
            &exact_vector(3, 2),
            1,
            None,
            SnapshotId(70_000),
            accountant_2.clone().into(),
        )
        .unwrap();
    let one_still_paused = done_1_rx.try_recv();
    pause_1.release();
    done_1_rx.recv_timeout(TEST_TIMEOUT).unwrap();
    let result_1 = exec_1
        .search(
            r.clone(),
            &ranked_vector(0, 3),
            1,
            None,
            SnapshotId(70_000),
            accountant_1.clone().into(),
        )
        .unwrap();
    assert_top_row(&result_2, RowId(99_999));
    assert!(matches!(one_still_paused, Err(TryRecvError::Empty)));
    assert_top_row(&result_1, RowId(1));
}
