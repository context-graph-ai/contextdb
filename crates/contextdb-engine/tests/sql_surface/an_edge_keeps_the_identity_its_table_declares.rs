//! A graph edge's identity is the unique key its table declares, never a fixed
//! triple of source, target and type.
//!
//! An INSERT into a table the engine routes as graph edges is an ordinary SQL
//! write. A second edge between the same two nodes with the same type that
//! differs by a further column of the declared UNIQUE key is a different row: it
//! is inserted (`rows_affected` 1) and read back. A true duplicate under the
//! declared key is refused with the unique violation, as before, and the engine's
//! automatic de-duplication of source, target and type applies only where the
//! declared key is exactly those three columns or none is declared.
//!
//! Before the repair, an edge that differed only by an extra
//! declared key column was silently dropped -- `rows_affected` 0 and absent on
//! readback -- because the graph index, which holds one edge per triple, vetoed a
//! row the declared key allows. No other test declares a UNIQUE key wider than the
//! triple. The
//! insert variants are separate functions where the store, the transaction path or
//! the table decoration differ; the duplicate and exact-triple cases pin behaviour
//! that must not change.

use contextdb_core::Value;
use contextdb_engine::Database;
use std::collections::HashMap;
use uuid::Uuid;

const WIDE_KEY_DDL: &str = "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID NOT NULL, target_id UUID NOT NULL, edge_type TEXT NOT NULL, context_id UUID NOT NULL, valid_from TXID NOT NULL, valid_to TXID, properties JSON, subtype_key TEXT NOT NULL DEFAULT '', UNIQUE (source_id, target_id, edge_type, subtype_key))";
const TRIPLE_KEY_DDL: &str = "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID NOT NULL, target_id UUID NOT NULL, edge_type TEXT NOT NULL, context_id UUID NOT NULL, valid_from TXID NOT NULL, valid_to TXID, properties JSON, subtype_key TEXT NOT NULL DEFAULT '', UNIQUE (source_id, target_id, edge_type))";
const NO_KEY_DDL: &str = "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID NOT NULL, target_id UUID NOT NULL, edge_type TEXT NOT NULL, context_id UUID NOT NULL, valid_from TXID NOT NULL, valid_to TXID, properties JSON, subtype_key TEXT NOT NULL DEFAULT '')";
const INSERT: &str = "INSERT INTO edges (id, source_id, target_id, edge_type, context_id, valid_from, subtype_key) VALUES ($id, $source_id, $target_id, $edge_type, $context_id, $valid_from, $subtype_key)";
const EXACT: &str = "SELECT id FROM edges WHERE source_id = $source_id AND target_id = $target_id AND edge_type = $edge_type AND subtype_key = $subtype_key";

struct Edge {
    id: Uuid,
    source: Uuid,
    target: Uuid,
    subtype: &'static str,
    context: Uuid,
}

impl Edge {
    fn new(source: Uuid, target: Uuid, subtype: &'static str, context: Uuid) -> Self {
        Self {
            id: Uuid::new_v4(),
            source,
            target,
            subtype,
            context,
        }
    }

    fn key(&self) -> HashMap<String, Value> {
        HashMap::from([
            ("source_id".into(), Value::Uuid(self.source)),
            ("target_id".into(), Value::Uuid(self.target)),
            ("edge_type".into(), Value::Text("RELATES_TO".into())),
            ("subtype_key".into(), Value::Text(self.subtype.into())),
        ])
    }

    fn row(&self, valid_from: Value) -> HashMap<String, Value> {
        let mut row = self.key();
        row.insert("id".into(), Value::Uuid(self.id));
        row.insert("context_id".into(), Value::Uuid(self.context));
        row.insert("valid_from".into(), valid_from);
        row
    }
}

fn open(on_disk: bool, ddl: &str) -> (Database, Option<tempfile::TempDir>) {
    let directory = on_disk.then(|| tempfile::tempdir().expect("task-scoped directory"));
    let database = match &directory {
        Some(directory) => Database::open(directory.path().join("edges.db")).expect("open"),
        None => Database::open_memory(),
    };
    database
        .execute(ddl, &HashMap::new())
        .expect("declare the edge table");
    (database, directory)
}

fn insert_autocommit(database: &Database, edge: &Edge) -> u64 {
    database
        .execute(INSERT, &edge.row(Value::TxId(contextdb_core::TxId(1))))
        .expect("insert an edge")
        .rows_affected
}

fn ids_of(database: &Database, edge: &Edge) -> Vec<Value> {
    database
        .execute(EXACT, &edge.key())
        .expect("read an edge by its declared key")
        .rows
        .into_iter()
        .map(|row| row.into_iter().next().expect("an id column"))
        .collect()
}

fn insert_refused_as_a_unique_violation(database: &Database, edge: &Edge) {
    let refusal = database
        .execute(INSERT, &edge.row(Value::TxId(contextdb_core::TxId(1))))
        .expect_err("a second row under the declared key is refused");
    assert!(
        matches!(refusal, contextdb_core::Error::UniqueViolation { .. }),
        "the refusal is the unique violation the table's key raises, got {refusal:?}"
    );
}

fn count(database: &Database) -> usize {
    database
        .execute("SELECT id FROM edges", &HashMap::new())
        .expect("scan the edges")
        .rows
        .len()
}

fn assert_both_subtypes_are_inserted_and_readable(database: &Database) {
    let (source, target, context) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    let first = Edge::new(source, target, "depends_on", context);
    let second = Edge::new(source, target, "posts_to", context);
    assert_eq!(insert_autocommit(database, &first), 1);
    assert_eq!(
        insert_autocommit(database, &second),
        1,
        "an edge that differs only by an extra declared key column is a new row"
    );
    assert_eq!(ids_of(database, &first), vec![Value::Uuid(first.id)]);
    assert_eq!(
        ids_of(database, &second),
        vec![Value::Uuid(second.id)],
        "the second subtype is readable by its declared key"
    );
    assert_eq!(count(database), 2, "both edges are in the table");
}

/// The defect itself, in memory.
#[test]
fn an_edge_differing_only_by_an_extra_declared_key_column_is_inserted() {
    let (database, _directory) = open(false, WIDE_KEY_DDL);
    assert_both_subtypes_are_inserted_and_readable(&database);
}

/// The same on a disk-backed store, which is then closed and reopened: both
/// edges are still there.
#[test]
fn an_edge_differing_only_by_an_extra_declared_key_column_is_inserted_on_disk() {
    let (database, directory) = open(true, WIDE_KEY_DDL);
    assert_both_subtypes_are_inserted_and_readable(&database);
    database.close().expect("close");
    let directory = directory.expect("a disk-backed store");
    let reopened = Database::open(directory.path().join("edges.db")).expect("reopen");
    assert_eq!(count(&reopened), 2, "both edges survive a close and reopen");
}

/// The same through an explicit transaction,
/// read inside it and after its commit, the path the consumer's edge writer uses.
#[test]
fn an_edge_differing_only_by_an_extra_declared_key_column_is_inserted_in_a_transaction() {
    let (database, _directory) = open(false, WIDE_KEY_DDL);
    let (source, target, context) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    for subtype in ["depends_on", "posts_to"] {
        let edge = Edge::new(source, target, subtype, context);
        let tx = database.begin().expect("begin");
        let inserted = database
            .execute_in_tx(tx, INSERT, &edge.row(Value::TxId(tx)))
            .expect("insert in the transaction");
        assert_eq!(inserted.rows_affected, 1, "{subtype} is inserted");
        assert_eq!(
            database
                .execute_in_tx(tx, EXACT, &edge.key())
                .expect("read inside the transaction")
                .rows
                .len(),
            1,
            "{subtype} is readable inside its transaction"
        );
        database.commit(tx).expect("commit");
        assert_eq!(ids_of(&database, &edge), vec![Value::Uuid(edge.id)]);
    }
    assert_eq!(count(&database), 2);
}

/// The table the consumer actually declares is
/// decorated (context column, DAG edge type, sync clauses); the decoration must
/// not change the answer.
#[test]
fn a_decorated_edge_table_keeps_both_subtypes() {
    let database = Database::open_memory();
    database
        .execute(
            "CREATE TABLE contexts (id UUID PRIMARY KEY)",
            &HashMap::new(),
        )
        .expect("declare the contexts");
    let ddl = WIDE_KEY_DDL.replace(
        "context_id UUID NOT NULL",
        "context_id UUID CONTEXT_ID NOT NULL REFERENCES contexts(id)",
    ) + " DAG('CITES') SYNC TWO WAY SYNC CONFLICT KEEP FIRST";
    database
        .execute(&ddl, &HashMap::new())
        .expect("declare the decorated edge table");
    let context = Uuid::new_v4();
    database
        .execute(
            "INSERT INTO contexts (id) VALUES ($id)",
            &HashMap::from([("id".into(), Value::Uuid(context))]),
        )
        .expect("declare the context");
    let (source, target) = (Uuid::new_v4(), Uuid::new_v4());
    let first = Edge::new(source, target, "depends_on", context);
    let second = Edge::new(source, target, "posts_to", context);
    assert_eq!(insert_autocommit(&database, &first), 1);
    assert_eq!(insert_autocommit(&database, &second), 1);
    assert_eq!(ids_of(&database, &second), vec![Value::Uuid(second.id)]);
    assert_eq!(count(&database), 2);
}

/// Guard: a true duplicate under the declared
/// key is refused with the unique violation the key raises, exactly as today.
#[test]
fn a_true_duplicate_under_the_declared_key_is_still_not_inserted_twice() {
    let (database, _directory) = open(false, WIDE_KEY_DDL);
    let (source, target, context) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    let first = Edge::new(source, target, "depends_on", context);
    let duplicate = Edge::new(source, target, "depends_on", context);
    assert_eq!(insert_autocommit(&database, &first), 1);
    insert_refused_as_a_unique_violation(&database, &duplicate);
    assert_eq!(ids_of(&database, &first), vec![Value::Uuid(first.id)]);
    assert_eq!(count(&database), 1);
}

/// Guard: where the declared key is exactly
/// (source, target, type), a second edge with the same triple is deduplicated,
/// whatever its other columns say.
#[test]
fn a_declared_key_that_is_exactly_the_triple_still_deduplicates_it() {
    let (database, _directory) = open(false, TRIPLE_KEY_DDL);
    let (source, target, context) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    let first = Edge::new(source, target, "depends_on", context);
    let second = Edge::new(source, target, "posts_to", context);
    assert_eq!(insert_autocommit(&database, &first), 1);
    // The declared key is the triple, so a second edge on it is a duplicate.
    insert_refused_as_a_unique_violation(&database, &second);
    assert_eq!(count(&database), 1);
}

/// Guard: where no key is declared the
/// engine's automatic triple de-duplication still applies.
#[test]
fn with_no_declared_key_the_triple_is_still_deduplicated() {
    let (database, _directory) = open(false, NO_KEY_DDL);
    let (source, target, context) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    let first = Edge::new(source, target, "depends_on", context);
    let second = Edge::new(source, target, "posts_to", context);
    assert_eq!(insert_autocommit(&database, &first), 1);
    assert_eq!(
        insert_autocommit(&database, &second),
        0,
        "no declared key: the same triple is one edge"
    );
    assert_eq!(count(&database), 1);
}
