//! The sync-apply MEMORY preflight must project a pre-existing table's
//! policy the SAME way the real apply mutates it.
//!
//! `preflight_sync_apply_memory` builds a projected table-meta from the current
//! meta plus the incoming DDL. For a `CreateTable` naming a table that already
//! exists it used `entry(name).or_insert_with(...)`, KEEPING the old policy — but
//! the real apply now OVERWRITES it with the arriving declaration (the pre-existing
//! table adoption). The preflight therefore charges memory against the WRONG
//! policy. Here the hub holds the table as keep-latest and the arriving
//! declaration is keep-first: the preflight charges the incoming update (keep-latest
//! overwrites an existing row) while the real apply skips it (keep-first leaves the
//! existing row), so a batch the apply would admit is REJECTED at preflight with a
//! spurious `MemoryBudgetExceeded`. The projection must overwrite, mirroring apply.
//!
//! Discipline: no sleeps, no elapsed-time assertions, no raw clock reads.
//! Deterministic apply, asserted by outcome and by value.

use contextdb_core::{Lsn, Value};
use contextdb_engine::Database;
use contextdb_engine::sync_types::{ConflictPolicies, ConflictPolicy};
use std::collections::HashMap;
use std::sync::Arc;

fn insert_note(db: &Database, id: i64, body: &str) {
    let mut row = HashMap::new();
    row.insert("id".to_string(), Value::Int64(id));
    row.insert("body".to_string(), Value::Text(body.to_string()));
    db.execute("INSERT INTO notes (id, body) VALUES ($id, $body)", &row)
        .expect("insert note");
}

fn body_of(db: &Database, id: i64) -> Option<String> {
    let mut params = HashMap::new();
    params.insert("id".to_string(), Value::Int64(id));
    let result = db
        .execute("SELECT body FROM notes WHERE id = $id", &params)
        .expect("scan");
    result.rows.first().map(|row| match &row[0] {
        Value::Text(t) => t.clone(),
        other => panic!("body must be text, got {other:?}"),
    })
}

#[test]
fn fixd_preflight_projects_the_adopted_policy_so_it_does_not_over_charge() {
    // The hub holds the table declaring KEEP LATEST, with one small row, under a
    // tight memory limit.
    let hub = Arc::new(Database::open_memory());
    let empty = HashMap::new();
    hub.execute(
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT) SYNC CONFLICT KEEP LATEST",
        &empty,
    )
    .expect("hub table");
    insert_note(&hub, 1, "small");
    hub.execute("SET MEMORY_LIMIT '1M'", &empty)
        .expect("set a tight memory limit");

    // A source edge declares the SAME table as KEEP FIRST and carries a HUGE
    // update to the existing row. Its outgoing delta is the CreateTable declaring
    // keep-first plus that row — exactly what crosses the wire.
    let source = Arc::new(Database::open_memory());
    source
        .execute(
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT) SYNC CONFLICT KEEP FIRST",
            &empty,
        )
        .expect("source table");
    let huge = "x".repeat(4 * 1024 * 1024);
    insert_note(&source, 1, &huge);
    let changeset = source.changes_since(Lsn(0));

    // Under the adopted keep-first policy the existing row is left untouched, so
    // the huge update is never applied and never charged — the batch fits. A
    // preflight that projects the hub's OLD keep-latest policy charges the whole
    // 4 MiB against the 1 MiB budget and rejects it.
    let result = hub.apply_changes(
        changeset,
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    );
    assert!(
        result.is_ok(),
        "the apply adopts keep-first and skips the existing row, so nothing near \
         the budget is stored; the preflight must project that same policy and admit \
         the batch, not reject it with a spurious MemoryBudgetExceeded. Got {result:?}"
    );
    assert_eq!(
        body_of(&hub, 1),
        Some("small".to_string()),
        "under the adopted keep-first policy the existing value is left in place"
    );
}
