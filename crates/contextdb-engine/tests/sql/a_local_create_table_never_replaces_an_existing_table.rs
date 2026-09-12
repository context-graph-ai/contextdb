//! A consumer who types `CREATE TABLE` against a name their store already
//! carries is entitled to be told so — and to still have every row and column
//! that was there a moment ago. Today they are told `ok (rows_affected=0)`,
//! the table is silently redefined to the new column list, and the values in
//! every dropped column are gone: the store cannot even be opened for reading
//! again until a writer sanitizes it, and once one does, the old values are
//! unrecoverable.
//!
//! Redefining an existing table is a legitimate SYNC operation — the
//! `CreateTable`-adopt arm exists so a definition arriving from a peer can
//! replace the local one, and both
//! `crates/contextdb-engine/src/executor.rs:11163` and
//! `crates/contextdb-engine/src/database.rs:45203` describe it as overwriting
//! the same fields an `AlterTable` does. That door is right. What is wrong is
//! that a plain local statement a person typed is allowed to redefine a table
//! at all: the local `PhysicalPlan::CreateTable` arm
//! (`crates/contextdb-engine/src/executor.rs:1978`) has no collision check, so
//! it replaces the column list wholesale and the values in every dropped
//! column go with it. `CREATE TABLE IF NOT EXISTS` already parses
//! (`crates/contextdb-parser/src/parser.rs:1096`), so the idempotent spelling
//! is available and the bare form has no reason to be lenient — and today the
//! `if_not_exists` flag the parser sets is never read by the executor, so the
//! lenient spelling clobbers exactly as hard as the bare one and has no no-op
//! contract at all.
//!
//! What these tests pin:
//! - A bare local `CREATE TABLE` on an existing name is REFUSED, with a typed
//!   `Error::SchemaInvalid` whose reason names the table.
//! - A refused re-create destroys nothing: the prior columns and their values
//!   still answer afterwards.
//! - `CREATE TABLE IF NOT EXISTS` on an existing name is a genuine no-op —
//!   no error, and the existing shape and rows are left exactly as they were.
//! - CONTROL, and it must stay green: an arriving peer `CreateTable` for a
//!   table that already exists locally still ADOPTS the arriving declaration.
//!   The local door is what changes; the sync door is untouched.
//! - CONTROL: a first `CREATE TABLE` on an unused name still works, so the
//!   refusal is scoped to the collision and nothing else.

use contextdb_core::{ConflictPolicy as DeclaredConflictPolicy, Error, Lsn, Value};
use contextdb_engine::Database;
use contextdb_engine::sync_types::{ChangeSet, ConflictPolicies, ConflictPolicy, DdlChange};
use std::collections::HashMap;

const ROW_ID: &str = "11111111-1111-1111-1111-111111111111";

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

/// A store carrying `foo (id UUID PRIMARY KEY, a TEXT)` with one row whose
/// `a` is `'x'` — the shape every probe below starts from.
fn store_with_one_row() -> Database {
    let db = Database::open_memory();
    db.execute("CREATE TABLE foo (id UUID PRIMARY KEY, a TEXT)", &empty())
        .expect("declare foo");
    db.execute(
        "INSERT INTO foo (id, a) VALUES ($id, $a)",
        &[
            (
                "id".to_string(),
                Value::Uuid(ROW_ID.parse().expect("row uuid")),
            ),
            ("a".to_string(), Value::Text("x".to_string())),
        ]
        .into_iter()
        .collect::<HashMap<_, _>>(),
    )
    .expect("insert the row that must survive");
    db
}

fn column_names(db: &Database, table: &str) -> Vec<String> {
    db.table_meta(table)
        .unwrap_or_else(|| panic!("{table} exists"))
        .columns
        .iter()
        .map(|c| c.name.clone())
        .collect()
}

fn apply_single_ddl(db: &Database, ddl: DdlChange) -> contextdb_core::Result<()> {
    let changes = ChangeSet {
        rows: Vec::new(),
        edges: Vec::new(),
        vectors: Vec::new(),
        ddl: vec![ddl],
        ddl_lsn: vec![Lsn(db.current_lsn().0 + 1)],
    };
    db.apply_changes(
        changes,
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    )
    .map(|_| ())
}

/// The refusal itself: a second bare `CREATE TABLE foo` must not be accepted.
#[test]
fn a_bare_local_create_table_on_an_existing_name_is_refused_and_names_the_table() {
    let db = store_with_one_row();

    let err = db
        .execute("CREATE TABLE foo (id UUID PRIMARY KEY, b TEXT)", &empty())
        .expect_err(
            "a second bare CREATE TABLE on an existing name must refuse, not silently \
             redefine the table",
        );

    let reason = match err {
        Error::SchemaInvalid { reason } => reason,
        other => panic!("expected Error::SchemaInvalid naming the table, got {other:?}"),
    };
    assert!(
        reason.contains("foo"),
        "the refusal must name the table the consumer collided with; got: {reason}"
    );
    assert!(
        reason.contains("already exists"),
        "the refusal must say the table already exists, so the reader knows this is a \
         collision and not a malformed declaration; got: {reason}"
    );
}

/// The consequence that actually bites: the data. A refused re-create must
/// leave the store exactly as it found it.
#[test]
fn a_refused_recreate_leaves_the_existing_columns_and_rows_untouched() {
    let db = store_with_one_row();

    let _ = db.execute("CREATE TABLE foo (id UUID PRIMARY KEY, b TEXT)", &empty());

    assert_eq!(
        column_names(&db, "foo"),
        vec!["id".to_string(), "a".to_string()],
        "the refused declaration must not have replaced foo's column list"
    );

    let result = db
        .execute("SELECT a FROM foo", &empty())
        .expect("foo must still be readable after a refused re-create");
    assert_eq!(
        result.rows.len(),
        1,
        "the row declared before the refused re-create must still be there"
    );
    let a_idx = result
        .columns
        .iter()
        .position(|c| c == "a")
        .expect("column `a` must still be projectable");
    assert_eq!(
        result.rows[0][a_idx],
        Value::Text("x".to_string()),
        "the value in the column the refused declaration would have dropped must survive"
    );
}

/// `IF NOT EXISTS` is the spelling that IS allowed to be idempotent — and
/// idempotent means it changes nothing at all, not that it clobbers quietly.
#[test]
fn create_table_if_not_exists_on_an_existing_name_changes_nothing() {
    let db = store_with_one_row();

    db.execute(
        "CREATE TABLE IF NOT EXISTS foo (id UUID PRIMARY KEY, b TEXT)",
        &empty(),
    )
    .expect("IF NOT EXISTS must succeed as a no-op, not refuse");

    assert_eq!(
        column_names(&db, "foo"),
        vec!["id".to_string(), "a".to_string()],
        "IF NOT EXISTS must leave the existing table's column list alone"
    );

    let result = db
        .execute("SELECT a FROM foo", &empty())
        .expect("foo must still be readable after an IF NOT EXISTS no-op");
    assert_eq!(result.rows.len(), 1, "the existing row must still be there");
    let a_idx = result
        .columns
        .iter()
        .position(|c| c == "a")
        .expect("column `a` must still be projectable");
    assert_eq!(
        result.rows[0][a_idx],
        Value::Text("x".to_string()),
        "IF NOT EXISTS must not drop the column the second declaration omitted"
    );
}

/// CONTROL — the sync door is not what changes. A `CreateTable` ARRIVING from
/// a peer, naming a table that already exists locally, is accepted and its
/// declaration is adopted: this is the same adopt path the local statement
/// reaches today, and it must keep working after the local door is closed. If
/// a fix to the local door reaches this test, the fix went in the wrong place.
#[test]
fn an_arriving_peer_create_table_still_adopts_an_existing_tables_declaration() {
    let db = store_with_one_row();
    assert_eq!(
        db.table_meta("foo").expect("foo exists").conflict_policy,
        None,
        "premise: the local declaration named no conflict policy"
    );

    let peer_create = DdlChange::CreateTable {
        name: "foo".to_string(),
        columns: vec![
            ("id".to_string(), "UUID PRIMARY KEY".to_string()),
            ("a".to_string(), "TEXT".to_string()),
        ],
        constraints: vec!["SYNC CONFLICT KEEP LATEST".to_string()],
        foreign_keys: Vec::new(),
        composite_foreign_keys: Vec::new(),
        composite_unique: Vec::new(),
    };
    apply_single_ddl(&db, peer_create)
        .expect("arriving peer DDL for an existing table must still adopt, not refuse");

    assert_eq!(
        db.table_meta("foo").expect("foo exists").conflict_policy,
        Some(DeclaredConflictPolicy::KEEP_LATEST),
        "the adopt branch must still take the arriving peer's declaration"
    );
    assert_eq!(
        column_names(&db, "foo"),
        vec!["id".to_string(), "a".to_string()],
        "and it must not have disturbed the rows' own columns"
    );
}

/// CONTROL — the refusal is scoped to the collision. A first `CREATE TABLE`
/// on a name the store does not carry is untouched.
#[test]
fn a_first_create_table_on_an_unused_name_still_works() {
    let db = store_with_one_row();

    db.execute("CREATE TABLE bar (id UUID PRIMARY KEY, b TEXT)", &empty())
        .expect("a first CREATE TABLE on an unused name must still work");

    assert_eq!(
        column_names(&db, "bar"),
        vec!["id".to_string(), "b".to_string()]
    );
}
