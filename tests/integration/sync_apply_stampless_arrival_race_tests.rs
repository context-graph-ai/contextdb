//! Concurrent sync applies of different STAMPLESS rows (rows whose sender
//! never had an established arrival) must never freeze the same served
//! arrival position onto two rows that actually land at different commit
//! positions -- see `database.rs`'s `SYNC_SOURCE_LSN_OWN_COMMIT` sentinel and
//! `resolve_sync_source_lsn`.
//!
//! Before the fix: a stampless row's provenance sidecar was stamped with a
//! `current_lsn()` SAMPLE taken well before the row's own commit. Two
//! concurrent applies sampling at the same pre-commit instant, then
//! committing in some order through the transaction manager's single commit
//! lock, freeze the IDENTICAL sampled value into two rows that land at two
//! DIFFERENT committed positions -- diverging the served arrivals a peer
//! would arbitrate on (a peer holding the first value refuses the true later
//! one forever, contradicting the accepting-node-order contract).
//!
//! Deterministic, no sleeps: `Database::pause_before_sync_apply_commit_for_test`
//! arms a checkpoint immediately before a sync apply's terminal commit (after
//! every row is already staged), and `Database::mark_this_thread_for_sync_
//! apply_pre_commit_pause_for_test` selects exactly ONE thread to actually
//! stop there, so a second, unmarked apply on another thread runs an entire
//! independent commit while the first waits.

use contextdb_core::{Lsn, Value};
use contextdb_engine::Database;
use contextdb_engine::sync_types::{
    ChangeSet, ConflictPolicies, ConflictPolicy, NaturalKey, RowChange, SyncAdoption,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

const DDL: &str = "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)";

fn stampless_changeset(
    id: i64,
    body: &str,
    sender_lsn: Lsn,
) -> (ChangeSet, HashMap<Lsn, Option<Lsn>>) {
    let mut values = HashMap::new();
    values.insert("id".to_string(), Value::Int64(id));
    values.insert("body".to_string(), Value::Text(body.to_string()));
    let row = RowChange {
        table: "notes".to_string(),
        natural_key: NaturalKey::single("id".to_string(), Value::Int64(id)),
        values,
        deleted: false,
        lsn: sender_lsn,
        created_at: None,
    };
    let changes = ChangeSet {
        rows: vec![row],
        edges: Vec::new(),
        vectors: Vec::new(),
        ddl: Vec::new(),
        ddl_lsn: Vec::new(),
    };
    let mut arrivals = HashMap::new();
    // Stampless: the sender never had an established arrival for this row.
    arrivals.insert(sender_lsn, None);
    (changes, arrivals)
}

/// The row's own committed LSN and its resolved served arrival, read back
/// via the same path a sync peer would use (`changes_since_with_arrivals`).
fn committed_lsn_and_served_arrival(db: &Database, id: i64) -> (Lsn, Option<Lsn>) {
    let (changes, arrivals) = db.changes_since_with_arrivals(Lsn(0));
    let target = Value::Int64(id);
    let row = changes
        .rows
        .into_iter()
        .find(|r| r.natural_key.value == target)
        .expect("row must be present in the change log");
    let arrival = arrivals.get(&row.lsn).copied().flatten();
    (row.lsn, arrival)
}

/// Two stampless rows applied by two CONCURRENT, independent sync applies
/// must never end up carrying the same served arrival once both are
/// committed, even though the buggy pre-fix code samples the identical
/// `current_lsn()` for both while neither has committed yet.
#[test]
fn concurrent_stampless_applies_never_serve_equal_arrivals_for_different_commits() {
    let db = Arc::new(Database::open_memory());
    db.execute(DDL, &HashMap::new()).expect("ddl");
    let policies = ConflictPolicies::uniform(ConflictPolicy::LatestWins);

    let pause = db.pause_before_sync_apply_commit_for_test();

    let db_a = db.clone();
    let policies_a = policies.clone();
    let handle_a = thread::spawn(move || {
        db_a.mark_this_thread_for_sync_apply_pre_commit_pause_for_test();
        let (changes, arrivals) = stampless_changeset(1, "from-a", Lsn(9001));
        db_a.apply_synced_changes(changes, &policies_a, &arrivals, SyncAdoption::Continuing)
            .expect("apply A must succeed")
    });

    assert!(
        pause.wait_until_reached(Duration::from_secs(5)),
        "apply A must reach the pre-commit checkpoint deterministically"
    );

    // Apply B is UNMARKED: it passes the same (globally armed) checkpoint
    // unconditionally and runs a full, independent commit while A waits.
    let db_b = db.clone();
    let policies_b = policies.clone();
    let handle_b = thread::spawn(move || {
        let (changes, arrivals) = stampless_changeset(2, "from-b", Lsn(9002));
        db_b.apply_synced_changes(changes, &policies_b, &arrivals, SyncAdoption::Continuing)
            .expect("apply B must succeed")
    });
    let result_b = handle_b.join().expect("apply B thread must not panic");
    assert_eq!(
        result_b.applied_rows, 1,
        "apply B must have applied its row: {result_b:?}"
    );

    // Let A proceed to its own commit now that B has fully landed.
    pause.release();
    let result_a = handle_a.join().expect("apply A thread must not panic");
    assert_eq!(
        result_a.applied_rows, 1,
        "apply A must have applied its row: {result_a:?}"
    );

    let (lsn_a, arrival_a) = committed_lsn_and_served_arrival(&db, 1);
    let (lsn_b, arrival_b) = committed_lsn_and_served_arrival(&db, 2);

    assert_ne!(
        lsn_a, lsn_b,
        "the two applies must have landed at two DIFFERENT committed positions"
    );
    assert_ne!(
        arrival_a, arrival_b,
        "two rows committed at different positions must never serve the SAME arrival -- \
         got row 1 lsn={lsn_a:?} arrival={arrival_a:?}, row 2 lsn={lsn_b:?} arrival={arrival_b:?}"
    );
    assert_eq!(
        arrival_a,
        Some(lsn_a),
        "a stampless row's served arrival must equal its OWN committed lsn"
    );
    assert_eq!(
        arrival_b,
        Some(lsn_b),
        "a stampless row's served arrival must equal its OWN committed lsn"
    );
}
