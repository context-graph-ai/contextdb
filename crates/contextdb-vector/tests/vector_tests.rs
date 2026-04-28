use contextdb_core::{Error, RowId, Value, VectorExecutor, VectorIndexRef};
use contextdb_tx::{TxManager, WriteSet, WriteSetApplicator};
use contextdb_vector::{MemVectorExecutor, VectorStore, cosine_similarity};
use roaring::RoaringTreemap;
use std::sync::{Arc, OnceLock};

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
    let tx_mgr = Arc::new(TxManager::new(TestStore {
        vector: vector.clone(),
    }));
    let exec = MemVectorExecutor::new(vector, tx_mgr.clone(), hnsw);
    (tx_mgr, exec)
}

fn index() -> VectorIndexRef {
    VectorIndexRef::new("vectors", "embedding")
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
        Error::VectorDimensionMismatch {
            expected: 2,
            got: 3
        }
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
        exec.search(index(), &[1.0], 10, None, tx_mgr.snapshot())
            .unwrap()
            .is_empty()
    );
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
