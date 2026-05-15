use contextdb_core::{
    Error, Lsn, MemoryAccountant, RowId, SnapshotId, TxId, Value, VectorEntry, VectorExecutor,
    VectorIndexRef, VectorQuantization,
};
use contextdb_tx::{TxManager, WriteSet, WriteSetApplicator};
use contextdb_vector::{MemVectorExecutor, VectorStore, cosine_similarity};
use roaring::RoaringTreemap;
use std::collections::HashSet;
use std::sync::Barrier;
use std::sync::mpsc;
#[cfg(feature = "test-seams")]
use std::sync::mpsc::TryRecvError;
use std::sync::{Arc, OnceLock};
use std::thread;
use std::time::Duration;

const HNSW_THRESHOLD_FOR_TEST: usize = 1000;
const HNSW_ROWS_FOR_TEST: usize = 1024;
#[cfg(feature = "test-seams")]
const TEST_TIMEOUT: Duration = Duration::from_secs(5);
#[cfg(feature = "test-seams")]
const NEGATIVE_WINDOW: Duration = Duration::from_millis(250);

struct TestStore {
    vector: Arc<VectorStore>,
}

impl WriteSetApplicator for TestStore {
    fn apply(&self, ws: WriteSet) -> contextdb_core::Result<()> {
        self.vector.apply_inserts(ws.vector_inserts);
        self.vector.apply_deletes(ws.vector_deletes);
        Ok(())
    }

    fn new_row_id(&self) -> RowId {
        RowId(1)
    }
}

fn setup() -> (Arc<TxManager<TestStore>>, MemVectorExecutor<TestStore>) {
    let hnsw = Arc::new(OnceLock::new());
    let vector = Arc::new(VectorStore::new(hnsw.clone()));
    vector.register_index(index(), 2, VectorQuantization::F32);
    let tx_mgr = Arc::new(TxManager::new(TestStore {
        vector: vector.clone(),
    }));
    let exec = MemVectorExecutor::new(vector, tx_mgr.clone(), hnsw);
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
    Arc<TxManager<TestStore>>,
    Arc<MemVectorExecutor<TestStore>>,
) {
    let hnsw = Arc::new(OnceLock::new());
    let vector = Arc::new(VectorStore::new(hnsw.clone()));
    for (index, dim) in refs {
        vector.register_index(index.clone(), *dim, VectorQuantization::F32);
    }
    let tx_mgr = Arc::new(TxManager::new(TestStore {
        vector: vector.clone(),
    }));
    let exec = Arc::new(MemVectorExecutor::new_with_accountant(
        vector.clone(),
        tx_mgr.clone(),
        hnsw,
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
    store.apply_inserts_with_accountant(entries, Some(accountant));
}

fn hnsw_final_estimate(entry_count: usize, dimension: usize) -> usize {
    let entry_bytes = VectorQuantization::F32.storage_bytes(dimension);
    let exact_key_bytes = entry_bytes
        .saturating_add(std::mem::size_of::<RowId>())
        .saturating_add(64);
    entry_count.saturating_mul(
        entry_bytes
            .saturating_mul(3)
            .saturating_add(exact_key_bytes),
    )
}

fn hnsw_build_reservation_estimate(entry_count: usize, dimension: usize) -> usize {
    let final_bytes = hnsw_final_estimate(entry_count, dimension);
    let m = if entry_count <= 5000 {
        16usize
    } else {
        24usize
    };
    let ef_construction = if entry_count <= 5000 {
        200usize
    } else {
        400usize
    };
    let max_level_bound = 16usize;
    let stored_vector_bytes = VectorQuantization::F32.storage_bytes(dimension);
    let word = std::mem::size_of::<usize>();
    let sorted_entry_refs = entry_count.saturating_mul(word);
    let cloned_vectors_and_refs = entry_count.saturating_mul(
        stored_vector_bytes
            .saturating_add(word.saturating_mul(5))
            .saturating_add(std::mem::size_of::<RowId>()),
    );
    let map_and_exact_key_overhead = entry_count.saturating_mul(
        std::mem::size_of::<RowId>()
            .saturating_add(word.saturating_mul(3))
            .saturating_add(64),
    );
    let graph_link_upper_bound = entry_count
        .saturating_mul(m)
        .saturating_mul(max_level_bound)
        .saturating_mul(word.saturating_add(std::mem::size_of::<f32>()));
    let construction_scratch = entry_count.min(ef_construction).saturating_mul(
        word.saturating_mul(6)
            .saturating_add(std::mem::size_of::<f32>().saturating_mul(2)),
    );
    final_bytes
        .saturating_add(sorted_entry_refs)
        .saturating_add(cloned_vectors_and_refs)
        .saturating_add(map_and_exact_key_overhead)
        .saturating_add(graph_link_upper_bound)
        .saturating_add(construction_scratch)
}

fn vector_count_for(store: &VectorStore, index: &VectorIndexRef) -> usize {
    store
        .entries_for_index(index)
        .unwrap()
        .into_iter()
        .filter(|entry| entry.deleted_tx.is_none())
        .count()
}

fn assert_top_row(result: &[(RowId, f32)], row_id: RowId) {
    assert_eq!(result.first().map(|(row, _)| *row), Some(row_id));
    assert!(
        result.first().map(|(_, cos)| *cos).unwrap_or_default() >= 0.999,
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
    let (tx_mgr, exec) = setup();
    let tx = tx_mgr.begin();
    exec.insert_vector(tx, index(), RowId(1), vec![1.0, 0.0])
        .unwrap();
    exec.insert_vector(tx, index(), RowId(2), vec![0.9, 0.1])
        .unwrap();
    exec.insert_vector(tx, index(), RowId(3), vec![0.0, 1.0])
        .unwrap();
    tx_mgr.commit(tx).unwrap();

    let r = exec
        .search(index(), &[1.0, 0.0], 2, None, tx_mgr.snapshot())
        .unwrap();
    assert_eq!(r.len(), 2);
    assert_eq!(r[0].0, RowId(1));
    assert_eq!(r[1].0, RowId(2));
}

#[test]
fn candidate_prefilter_is_applied() {
    let (tx_mgr, exec) = setup();
    let tx = tx_mgr.begin();
    exec.insert_vector(tx, index(), RowId(1), vec![1.0, 0.0])
        .unwrap();
    exec.insert_vector(tx, index(), RowId(2), vec![0.9, 0.1])
        .unwrap();
    tx_mgr.commit(tx).unwrap();

    let mut cands = RoaringTreemap::new();
    cands.insert(2);

    let r = exec
        .search(index(), &[1.0, 0.0], 5, Some(&cands), tx_mgr.snapshot())
        .unwrap();
    assert_eq!(r.len(), 1);
    assert_eq!(r[0].0, RowId(2));
}

#[test]
fn mvcc_snapshot_isolation() {
    let (tx_mgr, exec) = setup();

    let tx1 = tx_mgr.begin();
    exec.insert_vector(tx1, index(), RowId(1), vec![1.0, 0.0])
        .unwrap();
    tx_mgr.commit(tx1).unwrap();

    let snap1 = tx_mgr.snapshot();

    let tx2 = tx_mgr.begin();
    exec.insert_vector(tx2, index(), RowId(2), vec![0.9, 0.1])
        .unwrap();
    tx_mgr.commit(tx2).unwrap();

    let old = exec.search(index(), &[1.0, 0.0], 10, None, snap1).unwrap();
    assert_eq!(old.len(), 1);

    let new = exec
        .search(index(), &[1.0, 0.0], 10, None, tx_mgr.snapshot())
        .unwrap();
    assert_eq!(new.len(), 2);
}

#[test]
fn dimension_mismatch_errors() {
    let (tx_mgr, exec) = setup();
    let tx1 = tx_mgr.begin();
    exec.insert_vector(tx1, index(), RowId(1), vec![1.0, 0.0])
        .unwrap();
    tx_mgr.commit(tx1).unwrap();

    let tx2 = tx_mgr.begin();
    let err = exec
        .insert_vector(tx2, index(), RowId(2), vec![1.0, 0.0, 0.0])
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
    let (tx_mgr, exec) = setup();
    assert!(
        exec.search(index(), &[1.0], 0, None, tx_mgr.snapshot())
            .unwrap()
            .is_empty()
    );
    assert!(
        exec.search(index(), &[1.0, 0.0], 10, None, tx_mgr.snapshot())
            .unwrap()
            .is_empty()
    );
    let err = exec
        .search(index(), &[1.0], 10, None, tx_mgr.snapshot())
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
    let (tx_mgr, exec) = setup();
    let tx = tx_mgr.begin();
    exec.insert_vector(tx, index(), RowId(1), vec![0.12345679, 0.9876543])
        .unwrap();
    tx_mgr.commit(tx).unwrap();

    let r = exec
        .search(
            index(),
            &[0.12345679, 0.9876543],
            1,
            None,
            tx_mgr.snapshot(),
        )
        .unwrap();
    assert_eq!(r.len(), 1);

    let _ = Value::Null;
}

#[test]
fn concurrent_same_ref_updates_leave_single_live_vector_after_commit() {
    let r = index();
    let accountant = Arc::new(MemoryAccountant::no_limit());
    let (store, _tx_mgr, exec) = setup_refs(&[(r.clone(), 4)], accountant.clone());
    seed_ranked(&store, &r, 1, HNSW_ROWS_FOR_TEST, 4, 10, &accountant);
    exec.search(r.clone(), &ranked_vector(0, 4), 1, None, SnapshotId(20_000))
        .unwrap();

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
        workers.push(thread::spawn(move || {
            barrier.wait();
            store.apply_changes_with_accountant(
                vec![(r.clone(), RowId(7), TxId(tx))],
                vec![test_entry(&r, 7, vector, tx)],
                Vec::new(),
                Lsn(tx),
                Some(accountant.as_ref()),
            );
        }));
    }
    barrier.wait();
    for worker in workers {
        worker.join().unwrap();
    }

    let live_entries = store
        .entries_for_index(&r)
        .unwrap()
        .into_iter()
        .filter(|entry| entry.row_id == RowId(7) && entry.deleted_tx.is_none())
        .collect::<Vec<_>>();
    assert_eq!(live_entries.len(), 1);
    let live_vector = live_entries[0].vector.clone();
    let result = exec
        .search(r.clone(), &live_vector, 1, None, SnapshotId(40_000))
        .unwrap();
    assert_top_row(&result, RowId(7));
    assert_eq!(store.raw_hnsw_entry_count_for_row(&r, RowId(7)), Some(1));
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
    store.apply_inserts_with_accountant(entries, Some(accountant.as_ref()));
    exec.search(r.clone(), &exact_vector(4, 0), 1, None, SnapshotId(20_000))
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
            store.apply_inserts_with_accountant(
                vec![test_entry(
                    &r,
                    10_000 + i as u64,
                    ranked_vector(200 + i, 4),
                    30_000 + i as u64,
                )],
                Some(accountant.as_ref()),
            );
        }));
    }
    barrier.wait();
    for worker in workers {
        worker.join().unwrap();
    }

    let snapshot = SnapshotId(40_000);
    let hnsw = exec
        .search(r.clone(), &exact_vector(4, 0), 50, None, snapshot)
        .unwrap();
    store.apply_inserts_with_accountant(
        vec![test_entry(&r, 999_999, exact_vector(4, 3), 50_000)],
        Some(accountant.as_ref()),
    );
    let brute = exec
        .search(r.clone(), &exact_vector(4, 0), 50, None, snapshot)
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
fn same_ref_concurrent_deletes_leave_no_zombie_hnsw_entries() {
    let r = index();
    let accountant = Arc::new(MemoryAccountant::no_limit());
    let (store, _tx_mgr, exec) = setup_refs(&[(r.clone(), 4)], accountant.clone());
    seed_ranked(&store, &r, 1, HNSW_ROWS_FOR_TEST, 4, 10, &accountant);
    exec.search(r.clone(), &exact_vector(4, 0), 1, None, SnapshotId(20_000))
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
            store.apply_deletes_with_accountant(
                vec![(r, RowId(row), TxId(30_000 + row))],
                Some(accountant.as_ref()),
            );
        }));
    }
    barrier.wait();
    for worker in workers {
        worker.join().unwrap();
    }
    exec.search(
        r.clone(),
        &ranked_vector(20, 4),
        1,
        None,
        SnapshotId(40_000),
    )
    .unwrap();
    for row in 1..=10u64 {
        assert_eq!(store.raw_hnsw_entry_count_for_row(&r, RowId(row)), Some(0));
    }
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
                store.apply_changes_with_accountant(
                    Vec::new(),
                    entries,
                    Vec::new(),
                    Lsn(tx),
                    Some(accountant.as_ref()),
                );
                done_tx.send(()).unwrap();
            });
        }
        assert!(done_rx.recv_timeout(Duration::from_secs(2)).is_ok());
        assert!(done_rx.recv_timeout(Duration::from_secs(2)).is_ok());
    }
    assert_eq!(vector_count_for(&store, &a), 200);
    assert_eq!(vector_count_for(&store, &b), 200);
}

#[cfg(feature = "test-seams")]
#[test]
fn same_ref_apply_waits_while_same_ref_hnsw_build_paused() {
    use contextdb_vector::test_seam::PauseWindow;

    let r = index();
    let accountant = Arc::new(MemoryAccountant::no_limit());
    let (store, _tx_mgr, exec) = setup_refs(&[(r.clone(), 3)], accountant.clone());
    seed_ranked(&store, &r, 1, HNSW_ROWS_FOR_TEST, 3, 10, &accountant);

    let build_pause = store.arm_maintenance_pause_for_test(&r, PauseWindow::Build);
    let (done_build_tx, done_build_rx) = mpsc::channel();
    let exec_build = exec.clone();
    let r_build = r.clone();
    thread::spawn(move || {
        let result = exec_build.search(r_build, &exact_vector(3, 0), 1, None, SnapshotId(20_000));
        done_build_tx.send(result).unwrap();
    });
    assert!(build_pause.wait_until_reached(TEST_TIMEOUT));

    let apply_pause = store.arm_maintenance_pause_for_test(&r, PauseWindow::Apply);
    let (started_apply_tx, started_apply_rx) = mpsc::channel();
    let (done_apply_tx, done_apply_rx) = mpsc::channel();
    let store_apply = store.clone();
    let accountant_apply = accountant.clone();
    let r_apply = r.clone();
    thread::spawn(move || {
        started_apply_tx.send(()).unwrap();
        store_apply.apply_changes_with_accountant(
            vec![(r_apply.clone(), RowId(7), TxId(30_000))],
            vec![test_entry(&r_apply, 7, exact_vector(3, 2), 30_000)],
            Vec::new(),
            Lsn(30_000),
            Some(accountant_apply.as_ref()),
        );
        done_apply_tx.send(()).unwrap();
    });
    assert!(started_apply_rx.recv_timeout(TEST_TIMEOUT).is_ok());
    assert!(!apply_pause.wait_until_reached(NEGATIVE_WINDOW));
    assert!(matches!(done_apply_rx.try_recv(), Err(TryRecvError::Empty)));

    build_pause.release();
    done_build_rx.recv_timeout(TEST_TIMEOUT).unwrap().unwrap();
    assert!(apply_pause.wait_until_reached(TEST_TIMEOUT));
    apply_pause.release();
    done_apply_rx.recv_timeout(TEST_TIMEOUT).unwrap();
    let result = exec
        .search(r.clone(), &exact_vector(3, 2), 1, None, SnapshotId(40_000))
        .unwrap();
    assert_top_row(&result, RowId(7));
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
        .insert_vector(tx, a.clone(), RowId(1), vec![1.0, 0.0, 0.0, 0.0])
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
fn run_distinct_ref_build_overlap_case(test_first_build: bool) {
    use contextdb_vector::test_seam::PauseWindow;

    let a = ref_named("evidence", "vector_text");
    let b = ref_named("digests", "vector_text");
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
    if !test_first_build {
        assert!(!store.has_hnsw_index_for(&a));
        assert!(!store.has_hnsw_index_for(&b));
    }

    let pause_a = store.arm_maintenance_pause_for_test(&a, PauseWindow::Build);
    let (done_a_tx, done_a_rx) = mpsc::channel();
    let exec_a = exec.clone();
    let a_search = a.clone();
    thread::spawn(move || {
        done_a_tx
            .send(exec_a.search(a_search, &exact_vector(3, 0), 1, None, SnapshotId(50_000)))
            .unwrap();
    });
    assert!(pause_a.wait_until_reached(TEST_TIMEOUT));

    let (done_b_tx, done_b_rx) = mpsc::channel();
    let exec_b = exec.clone();
    let b_search = b.clone();
    thread::spawn(move || {
        done_b_tx
            .send(exec_b.search(b_search, &ranked_vector(0, 3), 1, None, SnapshotId(50_000)))
            .unwrap();
    });

    let b_first = done_b_rx.recv_timeout(TEST_TIMEOUT);
    let b_hnsw_before_release = store.has_hnsw_index_for(&b);
    let b_raw_before_release = store.raw_hnsw_entry_count_for_row(&b, RowId(100_000));
    let a_still_paused = done_a_rx.try_recv();
    pause_a.release();
    let a_result = done_a_rx.recv_timeout(TEST_TIMEOUT).unwrap().unwrap();
    let b_result = match b_first {
        Ok(result) => result.unwrap(),
        Err(_) => done_b_rx.recv_timeout(TEST_TIMEOUT).unwrap().unwrap(),
    };

    assert_top_row(&b_result, RowId(100_000));
    assert!(
        b_hnsw_before_release,
        "ref B must install HNSW while ref A is paused"
    );
    assert_eq!(b_raw_before_release, Some(1));
    assert!(matches!(a_still_paused, Err(TryRecvError::Empty)));
    assert_top_row(&a_result, RowId(1));
}

#[cfg(feature = "test-seams")]
#[test]
fn index_b_search_completes_while_index_a_hnsw_build_paused() {
    run_distinct_ref_build_overlap_case(false);
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
        store_a.apply_inserts_with_accountant(
            vec![test_entry(&a_worker, 1, exact_vector(3, 0), 10)],
            Some(accountant_a.as_ref()),
        );
        done_a_tx.send(()).unwrap();
    });
    assert!(pause_a.wait_until_reached(TEST_TIMEOUT));

    let (done_b_tx, done_b_rx) = mpsc::channel();
    let store_b = store.clone();
    let accountant_b = accountant.clone();
    let b_worker = b.clone();
    thread::spawn(move || {
        store_b.apply_inserts_with_accountant(
            vec![test_entry(&b_worker, 2, exact_vector(3, 1), 20)],
            Some(accountant_b.as_ref()),
        );
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
    assert_eq!(vector_count_for(&store, &a), 1);
    assert_eq!(vector_count_for(&store, &b), 1);
}

#[cfg(feature = "test-seams")]
#[test]
fn reindex_on_index_a_does_not_block_ingest_on_index_b() {
    use contextdb_vector::test_seam::PauseWindow;

    let a = ref_named("reindex", "vector_text");
    let b = ref_named("traffic", "vector_text");
    let accountant = Arc::new(MemoryAccountant::no_limit());
    let (store, _tx_mgr, exec) = setup_refs(&[(a.clone(), 3), (b.clone(), 3)], accountant.clone());
    seed_ranked(&store, &a, 1, HNSW_ROWS_FOR_TEST, 3, 10, &accountant);
    let pause_a = store.arm_maintenance_pause_for_test(&a, PauseWindow::Build);

    let (done_a_tx, done_a_rx) = mpsc::channel();
    let store_a = store.clone();
    let accountant_a = accountant.clone();
    let exec_a = exec.clone();
    let a_worker = a.clone();
    thread::spawn(move || {
        let deletes = (0..HNSW_ROWS_FOR_TEST)
            .map(|i| (a_worker.clone(), RowId(1 + i as u64), TxId(30_000)))
            .collect::<Vec<_>>();
        let inserts = (0..HNSW_ROWS_FOR_TEST)
            .map(|i| {
                test_entry(
                    &a_worker,
                    1 + i as u64,
                    ranked_vector(HNSW_ROWS_FOR_TEST - i, 3),
                    30_001 + i as u64,
                )
            })
            .collect::<Vec<_>>();
        store_a.apply_changes_with_accountant(
            deletes,
            inserts,
            Vec::new(),
            Lsn(40_000),
            Some(accountant_a.as_ref()),
        );
        let result = exec_a.search(
            a_worker,
            &ranked_vector(HNSW_ROWS_FOR_TEST, 3),
            1,
            None,
            SnapshotId(60_000),
        );
        done_a_tx.send(result).unwrap();
    });
    assert!(pause_a.wait_until_reached(TEST_TIMEOUT));

    let (started_b_tx, started_b_rx) = mpsc::channel();
    let (done_b_tx, done_b_rx) = mpsc::channel();
    let store_b = store.clone();
    let accountant_b = accountant.clone();
    let exec_b = exec.clone();
    let b_worker = b.clone();
    thread::spawn(move || {
        started_b_tx.send(()).unwrap();
        let mut rows = Vec::new();
        for i in 0..16u64 {
            let row = 500_000 + i;
            store_b.apply_inserts_with_accountant(
                vec![test_entry(&b_worker, row, exact_vector(3, 1), 70_000 + i)],
                Some(accountant_b.as_ref()),
            );
            let result = exec_b
                .search(
                    b_worker.clone(),
                    &exact_vector(3, 1),
                    1,
                    None,
                    SnapshotId(80_000 + i),
                )
                .unwrap();
            rows.push(result[0]);
        }
        done_b_tx.send(rows).unwrap();
    });
    assert!(started_b_rx.recv_timeout(TEST_TIMEOUT).is_ok());
    let b_first = done_b_rx.recv_timeout(Duration::from_secs(10));
    let b_completed_before_release = b_first.is_ok();
    let a_done_before_release = done_a_rx.try_recv();
    pause_a.release();
    let _ = done_a_rx.recv_timeout(TEST_TIMEOUT).unwrap().unwrap();
    let b_rows = match b_first {
        Ok(rows) => rows,
        Err(_) => done_b_rx.recv_timeout(TEST_TIMEOUT).unwrap(),
    };

    assert!(b_completed_before_release);
    assert!(matches!(a_done_before_release, Err(TryRecvError::Empty)));
    assert_eq!(b_rows.len(), 16);
    assert!(
        b_rows
            .iter()
            .all(|(row, cos)| row.0 >= 500_000 && *cos >= 0.999)
    );
    assert_eq!(vector_count_for(&store, &a), HNSW_ROWS_FOR_TEST);
}

#[cfg(feature = "test-seams")]
#[test]
fn index_b_first_build_completes_while_index_a_first_build_paused() {
    run_distinct_ref_build_overlap_case(true);
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
                store.apply_inserts_with_accountant(
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
                );
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
            .search(a.clone(), &exact_vector(3, 0), 5, None, SnapshotId(100_000))
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
    store.apply_inserts_with_accountant(
        vec![
            test_entry(&a, 1, exact_vector(3, 0), 10),
            test_entry(&b, 2, exact_vector(4, 1), 11),
        ],
        Some(accountant.as_ref()),
    );

    let err_a = exec
        .search(a.clone(), &exact_vector(4, 0), 1, None, SnapshotId(20))
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
            .search(a.clone(), &exact_vector(3, 0), 1, None, SnapshotId(20))
            .unwrap(),
        RowId(1),
    );

    let err_b = exec
        .search(b.clone(), &exact_vector(3, 0), 1, None, SnapshotId(20))
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
            .search(b.clone(), &exact_vector(4, 1), 1, None, SnapshotId(20))
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
        let pruned = store_prune.prune_row_ids(&prune_rows, accountant_prune.as_ref());
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
        store_insert.apply_inserts_with_accountant(
            vec![test_entry(&b_insert, 2_000, exact_vector(3, 1), 9_000)],
            Some(accountant_insert.as_ref()),
        );
        done_insert_tx.send(()).unwrap();
    });
    assert!(started_insert_rx.recv_timeout(TEST_TIMEOUT).is_ok());
    assert!(!apply_pause.wait_until_reached(NEGATIVE_WINDOW));
    assert!(matches!(
        done_insert_rx.try_recv(),
        Err(TryRecvError::Empty)
    ));

    bulk_pause.release();
    assert!(done_prune_rx.recv_timeout(TEST_TIMEOUT).unwrap() > 0);
    assert_eq!(vector_count_for(&store, &a), 90);
    assert!(!store.has_hnsw_index_for(&a));
    assert!(apply_pause.wait_until_reached(TEST_TIMEOUT));
    apply_pause.release();
    done_insert_rx.recv_timeout(TEST_TIMEOUT).unwrap();
    assert_eq!(vector_count_for(&store, &b), 101);
}

#[test]
fn replace_loaded_vectors_and_clear_hnsw_post_state_correct_with_balanced_accountant() {
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
    exec.search(a.clone(), &exact_vector(3, 0), 1, None, SnapshotId(50_000))
        .unwrap();
    exec.search(b.clone(), &exact_vector(3, 0), 1, None, SnapshotId(50_000))
        .unwrap();
    assert!(accountant.usage().used > 0);

    store.clear_hnsw(accountant.as_ref());
    assert_eq!(accountant.usage().used, 0);
    assert!(!store.has_hnsw_index_for(&a));
    assert!(!store.has_hnsw_index_for(&b));

    store.replace_loaded_vectors(vec![
        test_entry(&a, 7, exact_vector(3, 0), 60_000),
        test_entry(&b, 8, exact_vector(3, 1), 60_001),
    ]);
    assert_eq!(vector_count_for(&store, &a), 1);
    assert_eq!(vector_count_for(&store, &b), 1);
    assert_top_row(
        &exec
            .search(a.clone(), &exact_vector(3, 0), 1, None, SnapshotId(70_000))
            .unwrap(),
        RowId(7),
    );
    store.clear_hnsw(accountant.as_ref());
    assert_eq!(accountant.usage().used, 0);
}

#[cfg(feature = "test-seams")]
#[test]
fn bulk_pause_blocks_unrelated_lazy_hnsw_build_then_build_completes() {
    use contextdb_vector::test_seam::PauseWindow;

    let a = ref_named("a", "embedding");
    let b = ref_named("b", "embedding");
    let accountant = Arc::new(MemoryAccountant::no_limit());
    let (store, _tx_mgr, exec) = setup_refs(&[(a.clone(), 3), (b.clone(), 3)], accountant.clone());
    seed_ranked(&store, &a, 1, 100, 3, 10, &accountant);
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
    let prune_rows = (1..=10u64).map(RowId).collect::<HashSet<_>>();
    let (done_bulk_tx, done_bulk_rx) = mpsc::channel();
    let store_bulk = store.clone();
    let accountant_bulk = accountant.clone();
    thread::spawn(move || {
        store_bulk.prune_row_ids(&prune_rows, accountant_bulk.as_ref());
        done_bulk_tx.send(()).unwrap();
    });
    assert!(bulk_pause.wait_until_reached(TEST_TIMEOUT));

    let build_pause = store.arm_maintenance_pause_for_test(&b, PauseWindow::Build);
    let (started_build_tx, started_build_rx) = mpsc::channel();
    let (done_build_tx, done_build_rx) = mpsc::channel();
    let exec_build = exec.clone();
    let b_build = b.clone();
    thread::spawn(move || {
        started_build_tx.send(()).unwrap();
        let result = exec_build.search(b_build, &ranked_vector(0, 3), 1, None, SnapshotId(50_000));
        done_build_tx.send(result).unwrap();
    });
    assert!(started_build_rx.recv_timeout(TEST_TIMEOUT).is_ok());
    assert!(!build_pause.wait_until_reached(NEGATIVE_WINDOW));
    assert!(matches!(done_build_rx.try_recv(), Err(TryRecvError::Empty)));

    bulk_pause.release();
    done_bulk_rx.recv_timeout(TEST_TIMEOUT).unwrap();
    assert!(build_pause.wait_until_reached(TEST_TIMEOUT));
    build_pause.release();
    let result = done_build_rx.recv_timeout(TEST_TIMEOUT).unwrap().unwrap();
    assert_top_row(&result, RowId(100_000));
    assert!(store.has_hnsw_index_for(&b));
}

#[cfg(feature = "test-seams")]
#[test]
fn clear_hnsw_waits_for_inflight_lazy_build_then_releases_accountant() {
    use contextdb_vector::test_seam::PauseWindow;

    let a = ref_named("a", "embedding");
    let accountant = Arc::new(MemoryAccountant::no_limit());
    let (store, _tx_mgr, exec) = setup_refs(&[(a.clone(), 3)], accountant.clone());
    seed_ranked(&store, &a, 1, HNSW_ROWS_FOR_TEST, 3, 10, &accountant);

    let build_pause = store.arm_maintenance_pause_for_test(&a, PauseWindow::Build);
    let (done_build_tx, done_build_rx) = mpsc::channel();
    let exec_build = exec.clone();
    let a_build = a.clone();
    thread::spawn(move || {
        done_build_tx
            .send(exec_build.search(a_build, &ranked_vector(0, 3), 1, None, SnapshotId(30_000)))
            .unwrap();
    });
    assert!(build_pause.wait_until_reached(TEST_TIMEOUT));

    let (started_clear_tx, started_clear_rx) = mpsc::channel();
    let (done_clear_tx, done_clear_rx) = mpsc::channel();
    let store_clear = store.clone();
    let accountant_clear = accountant.clone();
    thread::spawn(move || {
        started_clear_tx.send(()).unwrap();
        store_clear.clear_hnsw(accountant_clear.as_ref());
        done_clear_tx.send(()).unwrap();
    });
    assert!(started_clear_rx.recv_timeout(TEST_TIMEOUT).is_ok());
    assert!(matches!(done_clear_rx.try_recv(), Err(TryRecvError::Empty)));

    build_pause.release();
    let result = done_build_rx.recv_timeout(TEST_TIMEOUT).unwrap().unwrap();
    assert_top_row(&result, RowId(1));
    done_clear_rx.recv_timeout(TEST_TIMEOUT).unwrap();
    assert!(!store.has_hnsw_index_for(&a));
    assert_eq!(accountant.usage().used, 0);
}

#[cfg(feature = "test-seams")]
#[test]
fn replace_loaded_vectors_blocks_per_ref_work_and_releases_existing_hnsw_accounting() {
    use contextdb_vector::test_seam::PauseWindow;

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
    exec.search(a.clone(), &ranked_vector(0, 3), 1, None, SnapshotId(50_000))
        .unwrap();
    let old_charge = accountant.usage().used;
    assert!(old_charge > 0);

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
    thread::spawn(move || {
        store_replace.replace_loaded_vectors(replacements);
        done_replace_tx.send(()).unwrap();
    });
    assert!(bulk_pause.wait_until_reached(TEST_TIMEOUT));

    let apply_pause = store.arm_maintenance_pause_for_test(&b, PauseWindow::Apply);
    let build_pause = store.arm_maintenance_pause_for_test(&b, PauseWindow::Build);
    let (started_apply_tx, started_apply_rx) = mpsc::channel();
    let (done_apply_tx, done_apply_rx) = mpsc::channel();
    let store_apply = store.clone();
    let accountant_apply = accountant.clone();
    let b_apply = b.clone();
    thread::spawn(move || {
        started_apply_tx.send(()).unwrap();
        store_apply.apply_inserts_with_accountant(
            vec![test_entry(&b_apply, 99_999, exact_vector(3, 2), 90_000)],
            Some(accountant_apply.as_ref()),
        );
        done_apply_tx.send(()).unwrap();
    });
    let (started_build_tx, started_build_rx) = mpsc::channel();
    let (done_build_tx, done_build_rx) = mpsc::channel();
    let exec_build = exec.clone();
    let b_build = b.clone();
    thread::spawn(move || {
        started_build_tx.send(()).unwrap();
        let result = exec_build.search(b_build, &ranked_vector(0, 3), 1, None, SnapshotId(100_000));
        done_build_tx.send(result).unwrap();
    });
    assert!(started_apply_rx.recv_timeout(TEST_TIMEOUT).is_ok());
    assert!(started_build_rx.recv_timeout(TEST_TIMEOUT).is_ok());
    assert!(!apply_pause.wait_until_reached(NEGATIVE_WINDOW));
    assert!(!build_pause.wait_until_reached(NEGATIVE_WINDOW));
    assert!(matches!(done_apply_rx.try_recv(), Err(TryRecvError::Empty)));
    assert!(matches!(done_build_rx.try_recv(), Err(TryRecvError::Empty)));

    bulk_pause.release();
    done_replace_rx.recv_timeout(TEST_TIMEOUT).unwrap();
    apply_pause.release();
    build_pause.release();
    done_apply_rx.recv_timeout(TEST_TIMEOUT).unwrap();
    done_build_rx.recv_timeout(TEST_TIMEOUT).unwrap().unwrap();
    assert_eq!(vector_count_for(&store, &a), HNSW_ROWS_FOR_TEST);
    assert_eq!(vector_count_for(&store, &b), HNSW_ROWS_FOR_TEST + 1);
    store.clear_hnsw(accountant.as_ref());
    assert_eq!(
        accountant.usage().used,
        0,
        "replace_loaded_vectors must not leak the old HNSW charge"
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
            storm_store.apply_inserts_with_accountant(
                vec![test_entry(index, 10_000 + i, exact_vector(3, 0), 1_000 + i)],
                Some(storm_accountant.as_ref()),
            );
        }
    });

    for _ in 0..500 {
        let err = exec
            .search(c.clone(), &exact_vector(3, 0), 1, None, SnapshotId(10_000))
            .unwrap_err();
        assert!(matches!(err, Error::UnknownVectorIndex { index } if index == c));
        let ok = exec
            .search(a.clone(), &exact_vector(3, 0), 1, None, SnapshotId(10_000))
            .unwrap();
        assert!(!ok.is_empty());
    }
    storm.join().unwrap();
}

#[test]
fn memory_accountant_rejection_on_a_leaves_b_accountant_byte_balance_correct() {
    let a = ref_named("large", "embedding");
    let b = ref_named("small", "embedding");
    let final_b = hnsw_final_estimate(HNSW_ROWS_FOR_TEST, 3);
    let reservation_b = hnsw_build_reservation_estimate(HNSW_ROWS_FOR_TEST, 3);
    let reservation_a = hnsw_build_reservation_estimate(HNSW_ROWS_FOR_TEST, 256);
    assert!(reservation_a > reservation_b);
    let accountant = Arc::new(MemoryAccountant::with_budget(reservation_b + 1024));
    let (store, _tx_mgr, exec) =
        setup_refs(&[(a.clone(), 256), (b.clone(), 3)], accountant.clone());
    seed_ranked(&store, &a, 1, HNSW_ROWS_FOR_TEST, 256, 10, &accountant);
    seed_ranked(
        &store,
        &b,
        100_000,
        HNSW_ROWS_FOR_TEST,
        3,
        20_000,
        &accountant,
    );
    assert_eq!(accountant.usage().used, 0);

    let a_result = exec
        .search(
            a.clone(),
            &exact_vector(256, 0),
            1,
            None,
            SnapshotId(50_000),
        )
        .unwrap();
    assert_top_row(&a_result, RowId(1));
    assert_eq!(accountant.usage().used, 0);
    assert!(!store.has_hnsw_index_for(&a));

    let b_result = exec
        .search(b.clone(), &ranked_vector(0, 3), 1, None, SnapshotId(50_000))
        .unwrap();
    assert_top_row(&b_result, RowId(100_000));
    assert!(store.has_hnsw_index_for(&b));
    assert_eq!(accountant.usage().used, final_b);
}

#[cfg(feature = "test-seams")]
#[test]
fn concurrent_distinct_hnsw_builds_do_not_oversubscribe_transient_budget() {
    use contextdb_vector::test_seam::{
        PauseWindow, estimate_hnsw_build_reservation_for_test, estimate_hnsw_final_bytes_for_test,
    };

    let a = ref_named("a", "embedding");
    let b = ref_named("b", "embedding");
    let dim = 3;
    let final_bytes =
        estimate_hnsw_final_bytes_for_test(HNSW_ROWS_FOR_TEST, dim, VectorQuantization::F32);
    let reservation =
        estimate_hnsw_build_reservation_for_test(HNSW_ROWS_FOR_TEST, dim, VectorQuantization::F32);
    assert!(reservation > final_bytes);
    let accountant = Arc::new(MemoryAccountant::with_budget(reservation + final_bytes - 1));
    let (store, _tx_mgr, exec) =
        setup_refs(&[(a.clone(), dim), (b.clone(), dim)], accountant.clone());
    seed_ranked(&store, &a, 1, HNSW_ROWS_FOR_TEST, dim, 10, &accountant);
    seed_ranked(
        &store,
        &b,
        100_000,
        HNSW_ROWS_FOR_TEST,
        dim,
        20_000,
        &accountant,
    );

    let pause_a = store.arm_maintenance_pause_for_test(&a, PauseWindow::Build);
    let (done_a_tx, done_a_rx) = mpsc::channel();
    let exec_a = exec.clone();
    let a_search = a.clone();
    thread::spawn(move || {
        done_a_tx
            .send(exec_a.search(
                a_search,
                &ranked_vector(0, dim),
                1,
                None,
                SnapshotId(50_000),
            ))
            .unwrap();
    });
    assert!(pause_a.wait_until_reached(TEST_TIMEOUT));
    let used_while_paused = accountant.usage().used;

    let (done_b_tx, done_b_rx) = mpsc::channel();
    let exec_b = exec.clone();
    let b_search = b.clone();
    thread::spawn(move || {
        done_b_tx
            .send(exec_b.search(
                b_search,
                &ranked_vector(0, dim),
                1,
                None,
                SnapshotId(50_000),
            ))
            .unwrap();
    });
    let b_first = done_b_rx.recv_timeout(TEST_TIMEOUT);
    let b_hnsw_while_a_paused = store.has_hnsw_index_for(&b);
    pause_a.release();
    let _ = done_a_rx.recv_timeout(TEST_TIMEOUT).unwrap().unwrap();
    let b_result = match b_first {
        Ok(result) => result.unwrap(),
        Err(_) => done_b_rx.recv_timeout(TEST_TIMEOUT).unwrap().unwrap(),
    };
    assert_top_row(&b_result, RowId(100_000));
    assert_eq!(used_while_paused, reservation);
    assert!(
        !b_hnsw_while_a_paused,
        "B must fall back instead of installing HNSW while A holds the transient reservation"
    );
    assert_eq!(accountant.usage().used, final_bytes);
    store.clear_hnsw(accountant.as_ref());
    assert_eq!(accountant.usage().used, 0);
}

#[cfg(feature = "test-seams")]
#[test]
fn same_ref_concurrent_hnsw_builders_reserve_and_install_once() {
    use contextdb_vector::test_seam::{
        PauseWindow, estimate_hnsw_build_reservation_for_test, estimate_hnsw_final_bytes_for_test,
    };

    let r = ref_named("vectors", "embedding");
    let dim = 3;
    let final_bytes =
        estimate_hnsw_final_bytes_for_test(HNSW_ROWS_FOR_TEST, dim, VectorQuantization::F32);
    let reservation =
        estimate_hnsw_build_reservation_for_test(HNSW_ROWS_FOR_TEST, dim, VectorQuantization::F32);
    let accountant = Arc::new(MemoryAccountant::with_budget(reservation * 2));
    let (store, _tx_mgr, exec) = setup_refs(&[(r.clone(), dim)], accountant.clone());
    seed_ranked(&store, &r, 1, HNSW_ROWS_FOR_TEST, dim, 10, &accountant);

    let pause = store.arm_maintenance_pause_for_test(&r, PauseWindow::Build);
    let (done_1_tx, done_1_rx) = mpsc::channel();
    let exec_1 = exec.clone();
    let r_1 = r.clone();
    thread::spawn(move || {
        done_1_tx
            .send(exec_1.search(r_1, &ranked_vector(0, dim), 1, None, SnapshotId(50_000)))
            .unwrap();
    });
    assert!(pause.wait_until_reached(TEST_TIMEOUT));
    let used_while_paused = accountant.usage().used;

    let (done_2_tx, done_2_rx) = mpsc::channel();
    let exec_2 = exec.clone();
    let r_2 = r.clone();
    thread::spawn(move || {
        done_2_tx
            .send(exec_2.search(r_2, &ranked_vector(0, dim), 1, None, SnapshotId(50_000)))
            .unwrap();
    });
    assert_eq!(
        done_2_rx.recv_timeout(NEGATIVE_WINDOW).unwrap_err(),
        mpsc::RecvTimeoutError::Timeout
    );
    assert_eq!(accountant.usage().used, used_while_paused);

    pause.release();
    let r1 = done_1_rx.recv_timeout(TEST_TIMEOUT).unwrap().unwrap();
    let r2 = done_2_rx.recv_timeout(TEST_TIMEOUT).unwrap().unwrap();
    assert_top_row(&r1, RowId(1));
    assert_top_row(&r2, RowId(1));
    assert!(store.has_hnsw_index_for(&r));
    assert_eq!(store.raw_hnsw_entry_count_for_row(&r, RowId(1)), Some(1));
    assert_eq!(used_while_paused, reservation);
    assert_eq!(accountant.usage().used, final_bytes);
    store.clear_hnsw(accountant.as_ref());
    assert_eq!(accountant.usage().used, 0);
}

#[cfg(feature = "test-seams")]
#[test]
fn clear_hnsw_keeps_accounting_charged_while_search_holds_graph() {
    use contextdb_vector::test_seam::{PauseWindow, estimate_hnsw_final_bytes_for_test};

    let a = ref_named("a", "embedding");
    let dim = 3;
    let final_bytes =
        estimate_hnsw_final_bytes_for_test(HNSW_ROWS_FOR_TEST, dim, VectorQuantization::F32);
    let accountant = Arc::new(MemoryAccountant::no_limit());
    let (store, _tx_mgr, exec) = setup_refs(&[(a.clone(), dim)], accountant.clone());
    seed_ranked(&store, &a, 1, HNSW_ROWS_FOR_TEST, dim, 10, &accountant);
    exec.search(
        a.clone(),
        &ranked_vector(0, dim),
        1,
        None,
        SnapshotId(50_000),
    )
    .unwrap();
    assert_eq!(accountant.usage().used, final_bytes);

    let search_pause = store.arm_maintenance_pause_for_test(&a, PauseWindow::Search);
    let (done_search_tx, done_search_rx) = mpsc::channel();
    let exec_search = exec.clone();
    let a_search = a.clone();
    thread::spawn(move || {
        done_search_tx
            .send(exec_search.search(
                a_search,
                &ranked_vector(0, dim),
                1,
                None,
                SnapshotId(50_000),
            ))
            .unwrap();
    });
    assert!(search_pause.wait_until_reached(TEST_TIMEOUT));

    let (started_clear_tx, started_clear_rx) = mpsc::channel();
    let (done_clear_tx, done_clear_rx) = mpsc::channel();
    let store_clear = store.clone();
    let accountant_clear = accountant.clone();
    thread::spawn(move || {
        started_clear_tx.send(()).unwrap();
        store_clear.clear_hnsw(accountant_clear.as_ref());
        done_clear_tx.send(()).unwrap();
    });
    assert!(started_clear_rx.recv_timeout(TEST_TIMEOUT).is_ok());
    let clear_done_while_search_paused = done_clear_rx.try_recv();
    let used_while_search_paused = accountant.usage().used;
    search_pause.release();
    let result = done_search_rx.recv_timeout(TEST_TIMEOUT).unwrap().unwrap();
    assert_top_row(&result, RowId(1));
    done_clear_rx.recv_timeout(TEST_TIMEOUT).unwrap();
    assert!(matches!(
        clear_done_while_search_paused,
        Err(TryRecvError::Empty)
    ));
    assert_eq!(used_while_search_paused, final_bytes);
    assert!(!store.has_hnsw_index_for(&a));
    assert_eq!(accountant.usage().used, 0);
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
    seed_ranked(&store_1, &r, 1, HNSW_ROWS_FOR_TEST, 3, 10, &accountant_1);
    seed_ranked(&store_2, &r, 1, HNSW_ROWS_FOR_TEST, 3, 10, &accountant_2);

    let pause_1 = store_1.arm_maintenance_pause_for_test(&r, PauseWindow::Build);
    let (done_1_tx, done_1_rx) = mpsc::channel();
    let exec_1_worker = exec_1.clone();
    let r_1 = r.clone();
    thread::spawn(move || {
        done_1_tx
            .send(exec_1_worker.search(r_1, &ranked_vector(0, 3), 1, None, SnapshotId(50_000)))
            .unwrap();
    });
    assert!(pause_1.wait_until_reached(TEST_TIMEOUT));

    store_2.apply_inserts_with_accountant(
        vec![test_entry(&r, 99_999, exact_vector(3, 2), 60_000)],
        Some(accountant_2.as_ref()),
    );
    let result_2 = exec_2
        .search(r.clone(), &exact_vector(3, 2), 1, None, SnapshotId(70_000))
        .unwrap();
    let one_still_paused = done_1_rx.try_recv();
    pause_1.release();
    let result_1 = done_1_rx.recv_timeout(TEST_TIMEOUT).unwrap().unwrap();
    assert_top_row(&result_2, RowId(99_999));
    assert!(matches!(one_still_paused, Err(TryRecvError::Empty)));
    assert_top_row(&result_1, RowId(1));
}
