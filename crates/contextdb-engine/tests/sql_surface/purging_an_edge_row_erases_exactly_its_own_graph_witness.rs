//! Purging a graph-edge row erases exactly that row's graph artifacts and
//! nothing a surviving sibling row owns.
//!
//! Promise (contextdb-intent, Policy 12 and Policy 32): an authoritative purge
//! erases every copy of exactly the selected rows' lineages, atomically. Two
//! edge rows between the same nodes with the same type that differ by a
//! declared key column are two rows; each owns its own adjacency in both
//! directions. Purging one removes that row and the adjacency it owns; the
//! other row stays readable, stays reachable by graph traversal, is still
//! carried by the change log and exported; purging the last owner leaves no
//! adjacency behind; and a fresh row with the same key afterwards is a new
//! row.
//!
//! Every journey runs on a task-scoped file-backed store, because the
//! authoritative purge kernel requires one. Counts are exact states: rows,
//! traversal targets, change-log occurrences and exported rows.
//!
//! Reason (2026-10-16): purging one of two sibling edge rows consumed the one
//! adjacency the graph keeps per source, target and type, so it either failed
//! to find the purged row's witness or removed the survivor's edge. No other
//! test purges an edge row at all. The rows differ in which sibling is purged,
//! whether both are, how they were created and whether the store is reopened;
//! they share the harness.

use contextdb_core::{Lsn, Value};
use contextdb_engine::Database;
use std::collections::HashMap;
use std::path::PathBuf;
use uuid::Uuid;

const DDL: &str = "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID NOT NULL, target_id UUID NOT NULL, edge_type TEXT NOT NULL, context_id UUID CONTEXT_ID NOT NULL REFERENCES contexts(id), valid_from TXID NOT NULL, valid_to TXID, properties JSON, subtype_key TEXT NOT NULL DEFAULT '', UNIQUE (source_id, target_id, edge_type, subtype_key)) DAG('CITES') SYNC TWO WAY SYNC CONFLICT KEEP FIRST";
const INSERT: &str = "INSERT INTO edges (id, source_id, target_id, edge_type, context_id, valid_from, subtype_key) VALUES ($id, $source_id, $target_id, $edge_type, $context_id, $valid_from, $subtype_key)";
const PURGE_ONE: &str = "PURGE FROM edges WHERE source_id = $source_id AND target_id = $target_id AND edge_type = $edge_type AND subtype_key = $subtype_key";
const PURGE_BOTH: &str = "PURGE FROM edges WHERE source_id = $source_id AND target_id = $target_id AND edge_type = $edge_type";
const TRAVERSE: &str = "SELECT t FROM GRAPH_TABLE(edges MATCH (a)-[:RELATES_TO]->(b) WHERE a.id = $source_id COLUMNS (b.id AS t))";
const FORWARD_SELECT: &str = "SELECT id FROM edges WHERE source_id = $source_id AND target_id = $target_id AND edge_type = $edge_type AND subtype_key = $subtype_key";

struct Store {
    directory: tempfile::TempDir,
    database: Database,
    context: Uuid,
    source: Uuid,
    target: Uuid,
}

#[derive(Clone)]
struct Edge {
    id: Uuid,
    subtype: &'static str,
}

fn path(directory: &tempfile::TempDir) -> PathBuf {
    directory.path().join("edges.db")
}

fn store() -> Store {
    let directory = tempfile::tempdir().expect("task-scoped directory");
    let database = Database::open(path(&directory)).expect("open the file-backed store");
    database
        .execute(
            "CREATE TABLE contexts (id UUID PRIMARY KEY)",
            &HashMap::new(),
        )
        .expect("declare the contexts");
    database
        .execute(DDL, &HashMap::new())
        .expect("declare the edge table");
    let context = Uuid::new_v4();
    database
        .execute(
            "INSERT INTO contexts (id) VALUES ($id)",
            &HashMap::from([("id".into(), Value::Uuid(context))]),
        )
        .expect("declare the context");
    Store {
        directory,
        database,
        context,
        source: Uuid::new_v4(),
        target: Uuid::new_v4(),
    }
}

impl Store {
    fn key(&self, subtype: &str) -> HashMap<String, Value> {
        HashMap::from([
            ("source_id".into(), Value::Uuid(self.source)),
            ("target_id".into(), Value::Uuid(self.target)),
            ("edge_type".into(), Value::Text("RELATES_TO".into())),
            ("subtype_key".into(), Value::Text(subtype.into())),
        ])
    }

    fn row(&self, edge: &Edge, valid_from: Value) -> HashMap<String, Value> {
        let mut row = self.key(edge.subtype);
        row.insert("id".into(), Value::Uuid(edge.id));
        row.insert("context_id".into(), Value::Uuid(self.context));
        row.insert("valid_from".into(), valid_from);
        row
    }

    fn new_edge(&self, subtype: &'static str) -> Edge {
        Edge {
            id: Uuid::new_v4(),
            subtype,
        }
    }

    /// One edge inserted by its own autocommit; the insertion itself is
    /// required to succeed.
    fn insert(&self, edge: &Edge) {
        let inserted = self
            .database
            .execute(
                INSERT,
                &self.row(edge, Value::TxId(contextdb_core::TxId(1))),
            )
            .expect("insert an edge");
        assert_eq!(
            inserted.rows_affected, 1,
            "{} is inserted as a row of its own",
            edge.subtype
        );
    }

    fn purge_one(&self, edge: &Edge) {
        self.database
            .execute(PURGE_ONE, &self.key(edge.subtype))
            .unwrap_or_else(|error| {
                panic!("purging {} by its declared key: {error:?}", edge.subtype)
            });
    }

    fn row_ids(&self, database: &Database, edge: &Edge) -> Vec<Value> {
        database
            .execute(FORWARD_SELECT, &self.key(edge.subtype))
            .expect("read an edge by its declared key")
            .rows
            .into_iter()
            .map(|row| row.into_iter().next().expect("an id column"))
            .collect()
    }

    fn row_count(&self, database: &Database) -> usize {
        database
            .execute("SELECT id FROM edges", &HashMap::new())
            .expect("scan the edges")
            .rows
            .len()
    }

    /// How many targets a traversal from the source reaches.
    fn reachable(&self, database: &Database) -> Vec<Value> {
        database
            .execute(
                TRAVERSE,
                &HashMap::from([("source_id".into(), Value::Uuid(self.source))]),
            )
            .expect("traverse from the source")
            .rows
            .into_iter()
            .map(|row| row.into_iter().next().expect("a target column"))
            .collect()
    }

    fn carried(&self, database: &Database) -> Vec<Value> {
        database
            .changes_since(Lsn(0))
            .expect("the change log")
            .rows
            .iter()
            .filter(|row| row.table == "edges" && !row.deleted)
            .filter_map(|row| row.values.get("id").cloned())
            .collect()
    }

    fn assert_only_survivor_remains(&self, survivor: &Edge, purged: &Edge, database: &Database) {
        assert!(
            self.row_ids(database, purged).is_empty(),
            "the purged row is gone"
        );
        assert_eq!(
            self.row_ids(database, survivor),
            vec![Value::Uuid(survivor.id)],
            "the surviving row is untouched"
        );
        assert_eq!(self.row_count(database), 1);
        assert_eq!(
            self.reachable(database),
            vec![Value::Uuid(self.target)],
            "the surviving row's edge is still reachable by traversal"
        );
    }
}

/// Purge the second of two sibling rows: the first row and its edge survive.
#[test]
fn purging_the_second_sibling_leaves_the_first_row_and_its_edge() {
    let store = store();
    let (first, second) = (store.new_edge("depends_on"), store.new_edge("posts_to"));
    store.insert(&first);
    store.insert(&second);
    store.purge_one(&second);
    store.assert_only_survivor_remains(&first, &second, &store.database);
    let carried = store.carried(&store.database);
    assert!(
        carried.contains(&Value::Uuid(first.id)),
        "the survivor still syncs"
    );
    assert!(
        !carried.contains(&Value::Uuid(second.id)),
        "the purged row is carried by no change"
    );
}

/// Purge the first of two sibling rows: the second row and its edge survive,
/// though the graph kept its one adjacency under the first row's creation.
#[test]
fn purging_the_first_sibling_leaves_the_second_row_and_its_edge() {
    let store = store();
    let (first, second) = (store.new_edge("depends_on"), store.new_edge("posts_to"));
    store.insert(&first);
    store.insert(&second);
    store.purge_one(&first);
    store.assert_only_survivor_remains(&second, &first, &store.database);
}

/// Purge both siblings with one statement: no row and no edge remain.
#[test]
fn purging_both_siblings_at_once_erases_the_rows_and_the_edge() {
    let store = store();
    let (first, second) = (store.new_edge("depends_on"), store.new_edge("posts_to"));
    store.insert(&first);
    store.insert(&second);
    store
        .database
        .execute(
            PURGE_BOTH,
            &HashMap::from([
                ("source_id".into(), Value::Uuid(store.source)),
                ("target_id".into(), Value::Uuid(store.target)),
                ("edge_type".into(), Value::Text("RELATES_TO".into())),
            ]),
        )
        .expect("purge both rows with one statement");
    assert_eq!(store.row_count(&store.database), 0);
    assert!(
        store.reachable(&store.database).is_empty(),
        "no adjacency outlives its last owner"
    );
    assert!(store.carried(&store.database).is_empty());
}

/// Purge the siblings one after the other: the last purge removes the last
/// owner and with it the edge.
#[test]
fn purging_the_siblings_in_turn_removes_the_edge_with_its_last_owner() {
    let store = store();
    let (first, second) = (store.new_edge("depends_on"), store.new_edge("posts_to"));
    store.insert(&first);
    store.insert(&second);
    store.purge_one(&first);
    assert_eq!(
        store.reachable(&store.database),
        vec![Value::Uuid(store.target)]
    );
    store.purge_one(&second);
    assert_eq!(store.row_count(&store.database), 0);
    assert!(
        store.reachable(&store.database).is_empty(),
        "the last owner's purge removes the edge"
    );
}

/// Two siblings created in ONE transaction: purging one still leaves the
/// other row and its edge.
#[test]
fn purging_one_of_two_siblings_created_in_one_transaction_leaves_the_other() {
    let store = store();
    let (first, second) = (store.new_edge("depends_on"), store.new_edge("posts_to"));
    let tx = store.database.begin().expect("begin");
    for edge in [&first, &second] {
        let inserted = store
            .database
            .execute_in_tx(tx, INSERT, &store.row(edge, Value::TxId(tx)))
            .expect("insert in the transaction");
        assert_eq!(inserted.rows_affected, 1, "{} is inserted", edge.subtype);
    }
    store.database.commit(tx).expect("commit");
    store.purge_one(&first);
    store.assert_only_survivor_remains(&second, &first, &store.database);
}

/// After a close and reopen, the survivor row, its edge and its change-log
/// occurrence are still there and the purged row is still gone.
#[test]
fn the_survivor_and_the_erasure_both_persist_across_a_reopen() {
    let store = store();
    let (first, second) = (store.new_edge("depends_on"), store.new_edge("posts_to"));
    store.insert(&first);
    store.insert(&second);
    store.purge_one(&second);
    let Store {
        directory,
        database,
        context,
        source,
        target,
    } = store;
    database.close().expect("close");
    let reopened = Database::open(path(&directory)).expect("reopen");
    let store = Store {
        directory,
        database: reopened,
        context,
        source,
        target,
    };
    store.assert_only_survivor_remains(&first, &second, &store.database);
    assert!(
        !store
            .carried(&store.database)
            .contains(&Value::Uuid(second.id))
    );
}

/// A fresh row with the purged row's key, after the purge, is a new row that
/// is inserted, readable and reachable; the survivor is unaffected.
#[test]
fn a_fresh_row_with_a_purged_rows_key_is_a_new_row() {
    let store = store();
    let (first, second) = (store.new_edge("depends_on"), store.new_edge("posts_to"));
    store.insert(&first);
    store.insert(&second);
    store.purge_one(&second);
    let again = store.new_edge("posts_to");
    store.insert(&again);
    assert_eq!(
        store.row_ids(&store.database, &again),
        vec![Value::Uuid(again.id)]
    );
    assert_eq!(store.row_count(&store.database), 2);
    assert_eq!(
        store.reachable(&store.database),
        vec![Value::Uuid(store.target), Value::Uuid(store.target)],
        "a single-hop pattern returns one row per matching edge binding: two rows own the edge"
    );
}

/// An exported copy taken after the purge holds exactly the survivor row and
/// its edge.
#[test]
fn an_export_after_the_purge_holds_exactly_the_surviving_row_and_its_edge() {
    let store = store();
    let (first, second) = (store.new_edge("depends_on"), store.new_edge("posts_to"));
    store.insert(&first);
    store.insert(&second);
    store.purge_one(&first);
    let exported = store.directory.path().join("exported.db");
    store.database.export_snapshot(&exported).expect("export");
    let restored = Database::open(&exported).expect("open the exported copy");
    store.assert_only_survivor_remains(&second, &first, &restored);
}

// These rows use the same file-backed ownership harness. They cover the
// lifecycle and independent ownership cases required by the erasure contract;
// the sibling-purge rows alone cannot detect an orphan after DELETE/UPDATE,
// a transported derived edge, or an unrelated standalone/table owner.
#[test]
fn a_single_hop_binds_each_declared_sibling_row() {
    let store = store();
    for subtype in ["depends_on", "posts_to"] {
        store.insert(&store.new_edge(subtype));
    }
    assert_eq!(
        store.reachable(&store.database),
        vec![Value::Uuid(store.target); 2]
    );
    assert!(
        store
            .database
            .changes_since(Lsn(0))
            .unwrap()
            .edges
            .is_empty(),
        "row-derived adjacency is carried by rows"
    );
}

#[test]
fn deleting_one_sibling_leaves_exactly_the_other_binding() {
    let store = store();
    let (first, second) = (store.new_edge("depends_on"), store.new_edge("posts_to"));
    store.insert(&first);
    store.insert(&second);
    let result = store
        .database
        .execute(
            "DELETE FROM edges WHERE id = $id",
            &HashMap::from([("id".into(), Value::Uuid(first.id))]),
        )
        .unwrap();
    assert_eq!(result.rows_affected, 1);
    store.assert_only_survivor_remains(&second, &first, &store.database);
    assert!(
        store
            .database
            .changes_since(Lsn(0))
            .unwrap()
            .edges
            .is_empty()
    );
}

fn update_first_owner(changes_target: bool) {
    let store = store();
    let (first, second) = (store.new_edge("depends_on"), store.new_edge("posts_to"));
    store.insert(&first);
    store.insert(&second);
    let replacement_target = Uuid::new_v4();
    let (statement, parameter) = if changes_target {
        (
            "UPDATE edges SET target_id = $value WHERE id = $id",
            Value::Uuid(replacement_target),
        )
    } else {
        (
            "UPDATE edges SET subtype_key = $value WHERE id = $id",
            Value::Text("updated".into()),
        )
    };
    let result = store
        .database
        .execute(
            statement,
            &HashMap::from([
                ("id".into(), Value::Uuid(first.id)),
                ("value".into(), parameter),
            ]),
        )
        .unwrap();
    assert_eq!(result.rows_affected, 1);
    assert_eq!(store.row_count(&store.database), 2);
    let mut actual = store.reachable(&store.database);
    let mut expected = vec![
        Value::Uuid(store.target),
        Value::Uuid(if changes_target {
            replacement_target
        } else {
            store.target
        }),
    ];
    actual.sort_by_key(|v| v.as_uuid().copied());
    expected.sort_by_key(|v| v.as_uuid().copied());
    assert_eq!(
        actual, expected,
        "the replacement owns one binding and the prior version owns none"
    );
    assert!(
        store
            .database
            .changes_since(Lsn(0))
            .unwrap()
            .edges
            .is_empty()
    );
}

#[test]
fn updating_a_sibling_without_changing_its_triad_replaces_its_own_interval() {
    update_first_owner(false);
}

#[test]
fn updating_a_siblings_triad_leaves_no_old_binding() {
    update_first_owner(true);
}

#[test]
fn rolling_back_a_siblings_update_restores_both_original_bindings() {
    let store = store();
    let (first, second) = (store.new_edge("depends_on"), store.new_edge("posts_to"));
    store.insert(&first);
    store.insert(&second);
    let tx = store.database.begin().unwrap();
    let replacement = Uuid::new_v4();
    store
        .database
        .execute_in_tx(
            tx,
            "UPDATE edges SET target_id = $target WHERE id = $id",
            &HashMap::from([
                ("id".into(), Value::Uuid(first.id)),
                ("target".into(), Value::Uuid(replacement)),
            ]),
        )
        .unwrap();
    let inside = store
        .database
        .execute_in_tx(
            tx,
            TRAVERSE,
            &HashMap::from([("source_id".into(), Value::Uuid(store.source))]),
        )
        .unwrap();
    assert_eq!(inside.rows.len(), 2);
    assert_eq!(
        inside
            .rows
            .iter()
            .filter(|r| r[0] == Value::Uuid(replacement))
            .count(),
        1
    );
    store.database.rollback(tx).unwrap();
    assert_eq!(
        store.reachable(&store.database),
        vec![Value::Uuid(store.target); 2]
    );
    assert_eq!(
        store.row_ids(&store.database, &first),
        vec![Value::Uuid(first.id)]
    );
}

#[test]
fn deleting_an_uncommitted_edge_row_leaves_no_graph_witness() {
    let store = store();
    let first = store.new_edge("depends_on");
    let tx = store.database.begin().unwrap();
    store
        .database
        .execute_in_tx(tx, INSERT, &store.row(&first, Value::TxId(tx)))
        .unwrap();
    store
        .database
        .execute_in_tx(
            tx,
            "DELETE FROM edges WHERE id = $id",
            &HashMap::from([("id".into(), Value::Uuid(first.id))]),
        )
        .unwrap();
    let inside = store
        .database
        .execute_in_tx(
            tx,
            TRAVERSE,
            &HashMap::from([("source_id".into(), Value::Uuid(store.source))]),
        )
        .unwrap();
    assert!(inside.rows.is_empty());
    store.database.commit(tx).unwrap();
    assert_eq!(store.row_count(&store.database), 0);
    assert!(store.reachable(&store.database).is_empty());
    assert!(
        store
            .database
            .changes_since(Lsn(0))
            .unwrap()
            .edges
            .is_empty()
    );
}

#[test]
fn purging_an_edge_row_leaves_a_standalone_edge_with_the_same_triad() {
    let store = store();
    let tx = store.database.begin().unwrap();
    assert!(
        store
            .database
            .insert_edge(
                tx,
                store.source,
                store.target,
                "RELATES_TO".into(),
                HashMap::new()
            )
            .unwrap()
    );
    store.database.commit(tx).unwrap();
    let first = store.new_edge("depends_on");
    store.insert(&first);
    assert_eq!(
        store.reachable(&store.database),
        vec![Value::Uuid(store.target); 2]
    );
    store.purge_one(&first);
    assert_eq!(store.row_count(&store.database), 0);
    assert_eq!(
        store.reachable(&store.database),
        vec![Value::Uuid(store.target)]
    );
    let changes = store.database.changes_since(Lsn(0)).unwrap();
    assert_eq!(
        changes.edges.len(),
        1,
        "the independently inserted edge keeps its transport lane"
    );
    assert!(!changes.rows.iter().any(|r| r.table == "edges"));
    let tx = store.database.begin().unwrap();
    store
        .database
        .delete_edge(tx, store.source, store.target, "RELATES_TO")
        .unwrap();
    store.database.commit(tx).unwrap();
    assert!(store.reachable(&store.database).is_empty());
}

#[test]
fn purging_one_tables_edge_preserves_another_tables_same_triad() {
    let store = store();
    store
        .database
        .execute(
            &DDL.replacen("CREATE TABLE edges", "CREATE TABLE other_edges", 1),
            &HashMap::new(),
        )
        .unwrap();
    let (first, other) = (store.new_edge("depends_on"), store.new_edge("posts_to"));
    store.insert(&first);
    assert_eq!(
        store
            .database
            .execute(
                &INSERT.replacen("INTO edges", "INTO other_edges", 1),
                &store.row(&other, Value::TxId(contextdb_core::TxId(1)))
            )
            .unwrap()
            .rows_affected,
        1
    );
    assert_eq!(
        store.reachable(&store.database),
        vec![Value::Uuid(store.target); 2]
    );
    store.purge_one(&first);
    assert_eq!(
        store.reachable(&store.database),
        vec![Value::Uuid(store.target)]
    );
    assert_eq!(
        store
            .database
            .execute("SELECT id FROM other_edges", &HashMap::new())
            .unwrap()
            .rows,
        vec![vec![Value::Uuid(other.id)]]
    );
}

#[test]
fn dropping_one_edge_table_preserves_another_tables_same_triad() {
    let store = store();
    store
        .database
        .execute(
            &DDL.replacen("CREATE TABLE edges", "CREATE TABLE other_edges", 1),
            &HashMap::new(),
        )
        .unwrap();
    let (first, other) = (store.new_edge("depends_on"), store.new_edge("posts_to"));
    store.insert(&first);
    store
        .database
        .execute(
            &INSERT.replacen("INTO edges", "INTO other_edges", 1),
            &store.row(&other, Value::TxId(contextdb_core::TxId(1))),
        )
        .unwrap();
    store
        .database
        .execute("DROP TABLE edges", &HashMap::new())
        .unwrap();
    assert_eq!(
        store
            .database
            .execute(
                &TRAVERSE.replacen("GRAPH_TABLE(edges", "GRAPH_TABLE(other_edges", 1),
                &HashMap::from([("source_id".into(), Value::Uuid(store.source))])
            )
            .unwrap()
            .rows,
        vec![vec![Value::Uuid(store.target)]]
    );
    assert_eq!(
        store
            .database
            .execute("SELECT id FROM other_edges", &HashMap::new())
            .unwrap()
            .rows,
        vec![vec![Value::Uuid(other.id)]]
    );
}
#[test]
fn a_failed_purge_keeps_both_sibling_witnesses_across_reopen() {
    let store = store();
    let (first, second) = (store.new_edge("depends_on"), store.new_edge("posts_to"));
    store.insert(&first);
    store.insert(&second);
    let carried = store.carried(&store.database);
    let rows = store
        .database
        .execute("SELECT id FROM edges ORDER BY id", &HashMap::new())
        .expect("capture both live owners")
        .rows;
    store
        .database
        .arm_authoritative_purge_point_remove_persistence_failure_for_test();
    let failure = store
        .database
        .execute(PURGE_ONE, &store.key(second.subtype))
        .expect_err("the armed persistence fault aborts this purge");
    assert!(
        matches!(&failure, contextdb_core::Error::Other(message)
        if message == "authoritative purge point-remove persistence failure injected"),
        "the failure must come from the injected durable boundary: {failure:?}"
    );
    assert_eq!(store.carried(&store.database), carried);
    assert_eq!(
        store
            .database
            .execute("SELECT id FROM edges ORDER BY id", &HashMap::new())
            .expect("both rows survive the failed write")
            .rows,
        rows
    );
    let assert_bindings = |db: &Database| {
        assert_eq!(
            db.execute(TRAVERSE, &store.key(first.subtype))
                .expect("both forward witnesses survive")
                .rows
                .len(),
            2
        );
        assert_eq!(db.execute(
            "SELECT t FROM GRAPH_TABLE(edges MATCH (a)-[:RELATES_TO]->(b) WHERE b.id = $target_id COLUMNS (a.id AS t))",
            &store.key(first.subtype),
        ).expect("both reverse witnesses survive").rows.len(), 2);
    };
    assert_bindings(&store.database);
    store.database.close().expect("close after failed purge");
    let reopened = Database::open(path(&store.directory)).expect("reopen after failed purge");
    assert_eq!(store.carried(&reopened), carried);
    assert_eq!(
        reopened
            .execute("SELECT id FROM edges ORDER BY id", &HashMap::new())
            .expect("both rows survived reopen")
            .rows,
        rows
    );
    assert_bindings(&reopened);
}
// The late-child row isolates commit-frontier propagation; the eager row
// reaches successor-witness and erasure assertions without that precondition.
fn propagated_edge_successor_witness(children_after_transition: bool) {
    let directory = tempfile::tempdir().unwrap();
    let database = Database::open(path(&directory)).unwrap();
    let params = HashMap::from([
        ("parent".into(), Value::Uuid(Uuid::from_u128(31))),
        ("first".into(), Value::Uuid(Uuid::from_u128(41))),
        ("second".into(), Value::Uuid(Uuid::from_u128(42))),
        ("source".into(), Value::Uuid(Uuid::from_u128(51))),
        ("target".into(), Value::Uuid(Uuid::from_u128(52))),
        ("active".into(), Value::Text("active".into())),
        ("archived".into(), Value::Text("archived".into())),
        ("kind".into(), Value::Text("LINKS".into())),
        ("one".into(), Value::Text("one".into())),
        ("two".into(), Value::Text("two".into())),
    ]);
    database.execute("CREATE TABLE parents (id UUID PRIMARY KEY, status TEXT) STATE MACHINE (status: active -> [archived])", &params).unwrap();
    database.execute("CREATE TABLE propagated_edges (id UUID PRIMARY KEY, parent_id UUID REFERENCES parents(id) ON STATE archived PROPAGATE SET invalidated, status TEXT, source_id UUID, target_id UUID, edge_type TEXT, subtype TEXT, UNIQUE(source_id, target_id, edge_type, subtype)) STATE MACHINE (status: active -> [invalidated])", &params).unwrap();
    database
        .execute("INSERT INTO parents VALUES ($parent, $active)", &params)
        .unwrap();
    let insert_children = || {
        database.execute("INSERT INTO propagated_edges VALUES ($first, $parent, $active, $source, $target, $kind, $one)", &params).unwrap();
        database.execute("INSERT INTO propagated_edges VALUES ($second, $parent, $active, $source, $target, $kind, $two)", &params).unwrap();
    };
    if !children_after_transition {
        insert_children();
    }
    let archiver = database.begin().unwrap();
    database
        .upsert_row(
            archiver,
            "parents",
            "id",
            HashMap::from([
                ("id".into(), params["parent"].clone()),
                ("status".into(), params["archived"].clone()),
            ]),
        )
        .unwrap();
    if children_after_transition {
        insert_children();
    }
    assert_eq!(
        database
            .execute("SELECT status FROM propagated_edges", &params)
            .unwrap()
            .rows,
        vec![vec![Value::Text("active".into())]; 2],
        "uncommitted parent transition cannot leak to committed children"
    );
    database.commit(archiver).expect(
        "the commit backstop must propagate to children committed after staging the parent",
    );
    let rows = database
        .execute("SELECT status FROM propagated_edges", &params)
        .unwrap()
        .rows;
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows,
        vec![vec![Value::Text("invalidated".into())]; 2],
        "every accepted child at the commit frontier must transition"
    );
    let forward = "SELECT t FROM GRAPH_TABLE(propagated_edges MATCH (a)-[:LINKS]->(b) WHERE a.id = $source COLUMNS (b.id AS t))";
    let reverse = "SELECT t FROM GRAPH_TABLE(propagated_edges MATCH (a)<-[:LINKS]-(b) WHERE a.id = $target COLUMNS (b.id AS t))";
    for query in [forward, reverse] {
        let endpoint = if query == forward {
            params["target"].clone()
        } else {
            params["source"].clone()
        };
        assert_eq!(
            database.execute(query, &params).unwrap().rows,
            vec![vec![endpoint]; 2],
            "each propagated sibling retains its own physical binding"
        );
    }
    database
        .execute("PURGE FROM propagated_edges WHERE id = $second", &params)
        .expect("the propagated successor must own an exact purge witness");
    assert_eq!(
        database
            .execute("SELECT id FROM propagated_edges", &params)
            .unwrap()
            .rows
            .len(),
        1
    );
    assert_eq!(database.execute("SELECT t FROM GRAPH_TABLE(propagated_edges MATCH (a)-[:LINKS]->(b) WHERE a.id = $source COLUMNS (b.id AS t))", &params).unwrap().rows.len(), 1);
    drop(database);
    let reopened = Database::open(path(&directory)).unwrap();
    assert_eq!(
        reopened
            .execute("SELECT id FROM propagated_edges", &params)
            .unwrap()
            .rows,
        vec![vec![params["first"].clone()]],
        "purge selects exactly the second owner"
    );
    assert_eq!(
        reopened.execute(reverse, &params).unwrap().rows,
        vec![vec![params["source"].clone()]]
    );
    assert_eq!(
        reopened.execute(forward, &params).unwrap().rows,
        vec![vec![params["target"].clone()]]
    );
    reopened
        .execute("PURGE FROM propagated_edges WHERE id = $first", &params)
        .expect("the final successor owns its own purge witness");
    assert!(reopened.execute(forward, &params).unwrap().rows.is_empty());
    assert!(reopened.execute(reverse, &params).unwrap().rows.is_empty());
}
#[test]
fn a_propagated_edge_row_keeps_an_exact_successor_witness() {
    propagated_edge_successor_witness(true);
}
#[test]
fn an_eagerly_propagated_edge_row_keeps_an_exact_successor_witness() {
    propagated_edge_successor_witness(false);
}

// Public cursor lifecycle on the existing real SQL graph fixture. These rows
// prove committed graph controls; the public door rejects active transactions.
fn committed_graph_cursor_lifecycle(action: &str) {
    use contextdb_core::read_contract::{OwnerReadCancellation, ReadFailureKind, ReadLimits};
    use contextdb_engine::local_transport::ManualDeadlineClock;
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    let s = store();
    let edges = [Edge { id: Uuid::new_v4(), subtype: "one" }, Edge { id: Uuid::new_v4(), subtype: "two" }, Edge { id: Uuid::new_v4(), subtype: "three" }];
    for edge in &edges { s.insert(edge); }
    let limits = ReadLimits { cursor_page_rows: 1, cursor_idle_ms: 10, cursor_lifetime_ms: 100, ..ReadLimits::default() };
    let clock = Arc::new(ManualDeadlineClock::at(1000));
    let session = s.database.read_session_with_clock_for_test(limits, clock.clone()).unwrap();
    let mut cursor = session.open_cursor(TRAVERSE, &s.key("one")).unwrap();
    assert_eq!(cursor.first_page().rows, vec![vec![Value::Uuid(s.target)]]);
    assert!(cursor.first_page().has_more); assert!(cursor.is_live());
    match action {
        "purge" => {
            s.purge_one(&edges[1]);
            let error = cursor.fetch(NonZeroUsize::new(1)).unwrap_err();
            assert!(matches!(&error, contextdb_core::Error::ReadFailure(f) if f.kind() == ReadFailureKind::AuthoritativeErasure), "held graph fetch must report typed authoritative erasure; observed {error:?}");
            assert!(!cursor.is_live(), "purge destroys the held graph kernel before a later fetch can emit");
            assert_eq!(s.row_count(&s.database), 2); assert_eq!(s.reachable(&s.database), vec![Value::Uuid(s.target), Value::Uuid(s.target)]);
        }
        "cancel" => {
            let cancelled = OwnerReadCancellation::new(); cancelled.cancel();
            assert!(cursor.fetch_with_cancellation(NonZeroUsize::new(1), &cancelled).is_err());
            assert!(cursor.is_live(), "fetch cancellation preserves the existing resumable cursor contract");
            let page = cursor.fetch(NonZeroUsize::new(1)).unwrap();
            assert_eq!(page.rows, vec![vec![Value::Uuid(s.target)]]); assert!(page.has_more);
            let last = cursor.fetch(NonZeroUsize::new(1)).unwrap();
            assert_eq!(last.rows, vec![vec![Value::Uuid(s.target)]]); assert!(!last.has_more);
        }
        "expiry" => {
            clock.advance_to(1011);
            let error = cursor.fetch(NonZeroUsize::new(1)).unwrap_err();
            assert!(matches!(error, contextdb_core::Error::ReadFailure(f) if f.kind() == ReadFailureKind::CursorExpired));
            assert!(!cursor.is_live());
        }
        _ => unreachable!(),
    }
    drop(cursor); drop(session);
}
#[test]
fn committed_graph_cursor_purge_fences_held_results() { committed_graph_cursor_lifecycle("purge"); }
#[test]
fn committed_graph_cursor_cancellation_preserves_continuation() { committed_graph_cursor_lifecycle("cancel"); }
#[test]
fn committed_graph_cursor_expiry_is_terminal() { committed_graph_cursor_lifecycle("expiry"); }
