//! `SHOW SYNC_CONFLICT_POLICY` must reflect the policy a table DECLARED,
//! which is what the sync apply resolves against.
//!
//! The conflict policy a `CREATE ... SYNC CONFLICT ...` declares is stored on the
//! table meta; `sync_conflict_policy_for_table` resolves against that meta. But
//! `SHOW SYNC_CONFLICT_POLICY` read a separate in-memory runtime layer that no
//! product surface writes (its only setters were dead code), so a declared policy
//! never appeared. SHOW must read the declared meta so what it reports is what the
//! sync path actually uses.

use contextdb_core::Value;
use contextdb_engine::Database;
use std::collections::HashMap;

fn policy_rows(db: &Database) -> Vec<String> {
    let result = db
        .execute("SHOW SYNC_CONFLICT_POLICY", &HashMap::new())
        .expect("show");
    result
        .rows
        .iter()
        .map(|row| match &row[0] {
            Value::Text(t) => t.clone(),
            other => panic!("policy column must be text, got {other:?}"),
        })
        .collect()
}

#[test]
fn show_reflects_a_create_declared_conflict_policy() {
    let db = Database::open_memory();
    let empty = HashMap::new();
    db.execute(
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT) SYNC CONFLICT KEEP LATEST",
        &empty,
    )
    .expect("declare a table with a conflict policy");

    let rows = policy_rows(&db);
    assert!(
        rows.iter().any(|row| row == "notes=latest_wins"),
        "SHOW SYNC_CONFLICT_POLICY must reflect the CREATE-declared policy the \
         sync path resolves against; got {rows:?}"
    );

    // A table with no declared policy contributes no per-table row.
    db.execute(
        "CREATE TABLE plain (id INTEGER PRIMARY KEY, body TEXT)",
        &empty,
    )
    .expect("declare a plain table");
    let rows = policy_rows(&db);
    assert!(
        !rows.iter().any(|row| row.starts_with("plain=")),
        "an undeclared table must not appear as a per-table policy row; got {rows:?}"
    );
    assert!(
        rows.iter().any(|row| row == "notes=latest_wins"),
        "the declared table still appears; got {rows:?}"
    );
}
