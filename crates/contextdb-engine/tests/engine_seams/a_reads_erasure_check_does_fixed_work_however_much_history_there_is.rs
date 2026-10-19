//! The check that keeps an erased result from being served does a fixed amount
//! of work however much lineage and purge history the store holds.
//!
//! Promise (contextdb-intent, Policy 22 and the bounded-work contract): a
//! one-row read of an unrelated table is answered with work bounded by the
//! read, not by the store's history. Authorizing its publication against
//! authoritative erasure looks up the erasure state of the tables the read
//! touches; it does not enumerate and decode every lineage record and every
//! displaced purged life the store has ever written.
//!
//! The observation is the engine's own count of configuration entries
//! enumerated and decoded on the reading thread. The same read is measured
//! with 10 and with 100 purged tables behind it, on the open store and again
//! after the store is closed and reopened; the counts must be equal.
//!
//! Reason (2026-10-16): final authorization of every ordinary read collected
//! all current lineage records and all displaced purged history before it
//! looked for one that invalidated the read, so a one-row read paid for the
//! whole history. The counts are relational (equal at 10 and 100), never a
//! literal.

use contextdb_core::Value;
use contextdb_core::read_contract::ReadLimits;
use contextdb_engine::writer_query_test_seams::observe;
use contextdb_engine::{Database, DatabaseOpenOptions, MaintenancePolicy};
use std::collections::HashMap;
use std::path::Path;

fn no_params() -> HashMap<String, Value> {
    HashMap::new()
}

fn open(path: &Path) -> Database {
    Database::open_with_options(
        path,
        DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            ..DatabaseOpenOptions::default()
        },
    )
    .expect("open the file-backed store")
}

/// A store with an unrelated table to read and `purged` tables each purged
/// once, so the store holds that many lineage records of erased tables.
fn store_with_history(path: &Path, purged: usize) {
    let database = open(path);
    database
        .execute(
            "CREATE TABLE unrelated (id INT PRIMARY KEY, body TEXT)",
            &no_params(),
        )
        .expect("declare the read table");
    database
        .execute(
            "INSERT INTO unrelated (id, body) VALUES (1, 'kept')",
            &no_params(),
        )
        .expect("seed the read row");
    for table in 0..purged {
        database
            .execute(
                &format!("CREATE TABLE erased_{table} (id INT PRIMARY KEY, body TEXT)"),
                &no_params(),
            )
            .expect("declare an erased table");
        database
            .execute(
                &format!("INSERT INTO erased_{table} (id, body) VALUES (1, 'secret')"),
                &no_params(),
            )
            .expect("seed an erased row");
        database
            .execute(
                &format!("PURGE FROM erased_{table} WHERE id = 1"),
                &no_params(),
            )
            .expect("purge the table's row");
    }
    database.close().expect("close the seeded store");
}

/// Entries enumerated and decoded on this thread by one one-row read of the
/// unrelated table, and the row it returned.
fn work_of_one_read(database: &Database) -> (u64, u64, Vec<Vec<Value>>) {
    let session = database
        .read_session(ReadLimits::default())
        .expect("a read session");
    let (answer, work) =
        observe(|| session.execute("SELECT body FROM unrelated WHERE id = 1", &no_params()));
    let entries = work.history.sidecar_entries.iter().sum::<u64>();
    let decodes = work.history.sidecar_decodes.iter().sum::<u64>();
    (entries, decodes, answer.expect("the read is answered").rows)
}

fn measured(purged: usize, reopen: bool) -> (u64, u64) {
    let directory = tempfile::tempdir().expect("task-scoped directory");
    let path = directory.path().join(format!("history-{purged}.db"));
    store_with_history(&path, purged);
    let database = open(&path);
    let (entries, decodes, rows) = work_of_one_read(&database);
    assert_eq!(rows, vec![vec![Value::Text("kept".into())]]);
    if !reopen {
        return (entries, decodes);
    }
    database.close().expect("close before reopening");
    let reopened = open(&path);
    let (entries, decodes, rows) = work_of_one_read(&reopened);
    assert_eq!(rows, vec![vec![Value::Text("kept".into())]]);
    (entries, decodes)
}

/// The read's configuration work is the same with 10 and with 100 purged
/// tables in the store.
#[test]
fn a_reads_erasure_check_does_the_same_work_with_ten_and_with_a_hundred_purged_tables() {
    let small = measured(10, false);
    let large = measured(100, false);
    assert_eq!(
        large, small,
        "(entries enumerated, entries decoded) must not grow with the store's lineage history"
    );
}

/// The same after the store is closed and reopened, when the erasure state has
/// to come from durable records.
#[test]
fn a_reads_erasure_check_does_the_same_work_after_a_reopen() {
    let small = measured(10, true);
    let large = measured(100, true);
    assert_eq!(
        large, small,
        "after a reopen the check still does not grow with the store's lineage history"
    );
}
