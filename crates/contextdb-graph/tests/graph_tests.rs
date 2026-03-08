use contextdb_core::{Direction, EdgeType, Error, GraphExecutor, RowId, Value};
use contextdb_graph::{GraphStore, MemGraphExecutor};
use contextdb_tx::{TxManager, WriteSet, WriteSetApplicator};
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

struct TestStore {
    graph: Arc<GraphStore>,
}

impl WriteSetApplicator for TestStore {
    fn apply(&self, ws: WriteSet) -> contextdb_core::Result<()> {
        self.graph.apply_inserts(ws.adj_inserts);
        self.graph.apply_deletes(ws.adj_deletes);
        Ok(())
    }

    fn new_row_id(&self) -> RowId {
        1
    }
}

fn setup() -> (Arc<TxManager<TestStore>>, MemGraphExecutor<TestStore>) {
    let graph = Arc::new(GraphStore::new());
    let tx_mgr = Arc::new(TxManager::new(TestStore {
        graph: graph.clone(),
    }));
    let exec = MemGraphExecutor::new(graph, tx_mgr.clone());
    (tx_mgr, exec)
}

fn connect(
    tx_mgr: &TxManager<TestStore>,
    exec: &MemGraphExecutor<TestStore>,
    source: Uuid,
    target: Uuid,
    edge_type: &str,
) {
    let tx = tx_mgr.begin();
    exec.insert_edge(tx, source, target, edge_type.to_string(), HashMap::new())
        .unwrap();
    tx_mgr.commit(tx).unwrap();
}

#[test]
fn bfs_depth_bound_on_chain() {
    let (tx_mgr, exec) = setup();
    let nodes: Vec<Uuid> = (0..6).map(|_| Uuid::new_v4()).collect();

    for i in 0..5 {
        connect(&tx_mgr, &exec, nodes[i], nodes[i + 1], "BASED_ON");
    }

    let r = exec
        .bfs(nodes[0], None, Direction::Outgoing, 1, 3, tx_mgr.snapshot())
        .unwrap();
    assert_eq!(r.nodes.len(), 3);
}

#[test]
fn bfs_cycle_no_duplicates() {
    let (tx_mgr, exec) = setup();
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    let c = Uuid::new_v4();

    connect(&tx_mgr, &exec, a, b, "R");
    connect(&tx_mgr, &exec, b, c, "R");
    connect(&tx_mgr, &exec, c, a, "R");

    let r = exec
        .bfs(a, None, Direction::Outgoing, 1, 5, tx_mgr.snapshot())
        .unwrap();
    assert_eq!(r.nodes.len(), 2);
}

#[test]
fn bfs_respects_mvcc_snapshot() {
    let (tx_mgr, exec) = setup();
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    let c = Uuid::new_v4();

    connect(&tx_mgr, &exec, a, b, "R");
    let snap1 = tx_mgr.snapshot();
    connect(&tx_mgr, &exec, b, c, "R");

    let old = exec.bfs(a, None, Direction::Outgoing, 1, 5, snap1).unwrap();
    assert_eq!(old.nodes.len(), 1);

    let new = exec
        .bfs(a, None, Direction::Outgoing, 1, 5, tx_mgr.snapshot())
        .unwrap();
    assert_eq!(new.nodes.len(), 2);
}

#[test]
fn accepts_max_depth_one() {
    let (tx_mgr, exec) = setup();
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    let c = Uuid::new_v4();

    connect(&tx_mgr, &exec, a, b, "R");
    connect(&tx_mgr, &exec, b, c, "R");

    let r = exec
        .bfs(a, None, Direction::Outgoing, 1, 1, tx_mgr.snapshot())
        .unwrap();
    assert_eq!(r.nodes.len(), 1);
}

#[test]
fn bfs_visited_limit_error() {
    let (tx_mgr, exec) = setup();
    let root = Uuid::new_v4();

    let tx = tx_mgr.begin();
    for _ in 0..100_001 {
        exec.insert_edge(tx, root, Uuid::new_v4(), "R".to_string(), HashMap::new())
            .unwrap();
    }
    tx_mgr.commit(tx).unwrap();

    let err = exec
        .bfs(root, None, Direction::Outgoing, 1, 1, tx_mgr.snapshot())
        .unwrap_err();
    assert!(matches!(err, Error::BfsVisitedExceeded(100_000)));
}

#[test]
fn direction_incoming_and_both() {
    let (tx_mgr, exec) = setup();
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    let c = Uuid::new_v4();

    connect(&tx_mgr, &exec, a, b, "R");
    connect(&tx_mgr, &exec, c, b, "R");

    let incoming = exec
        .neighbors(b, None, Direction::Incoming, tx_mgr.snapshot())
        .unwrap();
    assert_eq!(incoming.len(), 2);

    let both = exec
        .neighbors(b, None, Direction::Both, tx_mgr.snapshot())
        .unwrap();
    assert_eq!(both.len(), 2);
}

#[test]
fn edge_type_filtering_and_empty_graph() {
    let (tx_mgr, exec) = setup();
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();

    let empty = exec
        .bfs(a, None, Direction::Outgoing, 1, 1, tx_mgr.snapshot())
        .unwrap();
    assert!(empty.nodes.is_empty());

    connect(&tx_mgr, &exec, a, b, "BASED_ON");

    let filter: Vec<EdgeType> = vec!["CITES".to_string()];
    let r = exec
        .bfs(
            a,
            Some(&filter),
            Direction::Outgoing,
            1,
            1,
            tx_mgr.snapshot(),
        )
        .unwrap();
    assert!(r.nodes.is_empty());

    let _ = Value::Null;
}
