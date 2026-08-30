//! Memory accounting across the whole lifetime of a row version and of the
//! vector and edge bytes attached to it.
//!
//! `MEMORY_LIMIT` is enforced against the accountant's `used` counter, so that
//! counter is the operator's promise: it tracks bytes the process is actually
//! holding. Two rules keep that promise, and each test below pins one of them
//! against a reclaim pass.
//!
//! Returned exactly ONCE. A version whose charge was already handed back when
//! it was superseded or deleted must not have its vector or edge bytes handed
//! back a second time by a later reclaim pass. A second return lowers `used`
//! below the resident total, so writes that should be refused are admitted and
//! the effective ceiling drifts above the configured one.
//!
//! Returned AT ALL. Bytes charged when a store is reopened from disk must be
//! reclaimable by the same maintenance that reclaims bytes charged by a live
//! write. Otherwise a reopened store holds charge against memory it is not
//! using, and writes that should be admitted are refused.
//!
//! Discipline: no sleeps, no elapsed-time assertions, expiry driven only
//! through the seamed wall clock, and no fixture value that restates the
//! number an assertion checks.

#![cfg(feature = "test-seams")]

use contextdb_core::{Value, Wallclock};
use contextdb_engine::Database;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tempfile::TempDir;
use uuid::Uuid;

const BUDGET_BYTES: usize = 64 * 1024 * 1024;
const CHURNED_ROWS: usize = 40;

fn p() -> HashMap<String, Value> {
    HashMap::new()
}

/// The counter `MEMORY_LIMIT` is enforced against.
fn charged(db: &Database) -> usize {
    db.accountant().usage().used
}

/// Row payload derived from a seed, so no assertion can be satisfied by a
/// value the fixture chose to match it.
fn body(seed: usize) -> String {
    format!("{seed:-<96}")
}

// ---------------------------------------------------------------------------
// Charge taken at open must be reclaimable by the same pass that reclaims a
// charge taken by a live write.
// ---------------------------------------------------------------------------

/// A store that churns rows, closes, and reopens carries the superseded
/// versions on disk. Reclaiming them must settle the accountant at the same
/// place as reopening the already-reclaimed store: one charge per surviving
/// version and nothing else. Any charge the reclaim pass cannot return is
/// stranded for the lifetime of the process, and every reopen of a churned
/// store strands more, until writes are refused against memory nobody holds.
#[test]
fn reclaiming_superseded_versions_after_reopen_settles_where_a_reclaimed_store_reopens() {
    let dir = TempDir::new().expect("temporary store directory");
    let path = dir.path().join("store.db");

    let db = Database::open(&path).expect("open a fresh store");
    db.execute(
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT) \
         HISTORY CURRENT ONLY SYNC CONFLICT KEEP LATEST",
        &p(),
    )
    .expect("declare a table that keeps only current values");
    db.set_memory_limit(Some(BUDGET_BYTES))
        .expect("declare a durable memory ceiling");
    for row in 0..CHURNED_ROWS {
        let mut params = HashMap::new();
        params.insert("id".to_string(), Value::Int64(row as i64));
        params.insert("b".to_string(), Value::Text(body(row)));
        db.execute("INSERT INTO notes (id, body) VALUES ($id, $b)", &params)
            .expect("insert");
    }
    let charged_after_insert = charged(&db);
    for row in 0..CHURNED_ROWS {
        let mut params = HashMap::new();
        params.insert("id".to_string(), Value::Int64(row as i64));
        params.insert("b".to_string(), Value::Text(body(row + CHURNED_ROWS)));
        db.execute("UPDATE notes SET body = $b WHERE id = $id", &params)
            .expect("supersede every row once");
    }
    assert_eq!(
        charged(&db),
        charged_after_insert,
        "superseding a row hands back the old version's charge at commit, so \
         a churn that replaces every row leaves the same total charged"
    );
    db.close()
        .expect("close with the superseded versions on disk");

    let db = Database::open(&path).expect("reopen the churned store");
    assert_eq!(
        db.memory_limit(),
        Some(BUDGET_BYTES),
        "a durable memory ceiling is enforced again after reopen"
    );
    let first = db
        .compact_currency_versions()
        .expect("reclaim the superseded versions carried in from disk");
    assert!(
        first.pruned_versions > 0,
        "the reopened store must still carry superseded versions to reclaim: \
         {first:?}"
    );
    let charged_after_first_reclaim = charged(&db);
    db.close().expect("close the reclaimed store");

    let db = Database::open(&path).expect("reopen the reclaimed store");
    let second = db
        .compact_currency_versions()
        .expect("reclaim again on a store with nothing left to reclaim");
    assert_eq!(
        second.pruned_versions, 0,
        "the earlier reclaim is durable, so nothing is left to reclaim: \
         {second:?}"
    );
    let charged_after_second_reclaim = charged(&db);

    assert_eq!(
        charged_after_first_reclaim, charged_after_second_reclaim,
        "reclaiming superseded versions carried in from disk must return their \
         charge, leaving the accountant exactly where reopening the already- \
         reclaimed store leaves it; a higher first figure is charge the reopen \
         took and no pass can give back"
    );
}

// ---------------------------------------------------------------------------
// A charge already returned at commit must not be returned again by reclaim.
// ---------------------------------------------------------------------------

/// Superseding a vector hands its bytes back at commit. When the reclaim pass
/// later removes that superseded copy it must not hand the same bytes back a
/// second time: a store that churned one row down to its current value must
/// end up charged exactly what a store holding only that current value is
/// charged, never less.
#[test]
fn reclaiming_a_superseded_vector_returns_its_bytes_only_once() {
    let current_body = Value::Text(body(2));
    let current_vector = Value::Vector(vec![0.5, 0.25, 0.125, 0.0625, 0.5, 0.25, 0.125, 0.0625]);

    let settled = Database::open_memory();
    settled
        .execute(
            "CREATE TABLE embeds (id INTEGER PRIMARY KEY, body TEXT, v VECTOR(8)) \
             HISTORY CURRENT ONLY SYNC CONFLICT KEEP LATEST",
            &p(),
        )
        .expect("declare a current-only vector table");
    let mut current = HashMap::new();
    current.insert("b".to_string(), current_body.clone());
    current.insert("v".to_string(), current_vector.clone());
    settled
        .execute(
            "INSERT INTO embeds (id, body, v) VALUES (1, $b, $v)",
            &current,
        )
        .expect("write the current value straight in");
    let charged_for_current_value_only = charged(&settled);

    let churned = Database::open_memory();
    churned
        .execute(
            "CREATE TABLE embeds (id INTEGER PRIMARY KEY, body TEXT, v VECTOR(8)) \
             HISTORY CURRENT ONLY SYNC CONFLICT KEEP LATEST",
            &p(),
        )
        .expect("declare a current-only vector table");
    let mut superseded = HashMap::new();
    superseded.insert("b".to_string(), Value::Text(body(1)));
    superseded.insert(
        "v".to_string(),
        Value::Vector(vec![0.0625, 0.125, 0.25, 0.5, 0.0625, 0.125, 0.25, 0.5]),
    );
    churned
        .execute(
            "INSERT INTO embeds (id, body, v) VALUES (1, $b, $v)",
            &superseded,
        )
        .expect("write a value that will be superseded");
    churned
        .execute("UPDATE embeds SET body = $b, v = $v WHERE id = 1", &current)
        .expect("supersede it with the current value");
    assert_eq!(
        charged(&churned),
        charged_for_current_value_only,
        "superseding hands the old row and vector back at commit, so before \
         any reclaim the churned store is charged for the current value alone"
    );

    let report = churned
        .compact_currency_versions()
        .expect("reclaim the superseded version");
    assert_eq!(
        report.pruned_versions, 1,
        "exactly the one superseded version is reclaimed: {report:?}"
    );

    assert_eq!(
        charged(&churned),
        charged_for_current_value_only,
        "the superseded vector's bytes were already handed back at commit, so \
         removing that copy must not hand them back again; a lower figure is \
         the same bytes returned twice and lifts the effective ceiling above \
         the configured one"
    );
}

/// Deleting an edge only tombstones it; its bytes come back when the pass that
/// drops the entry runs. Expiring the row it was attached to is that pass, and
/// it must hand those bytes back once rather than twice.
#[test]
fn expiring_a_row_whose_edge_was_deleted_returns_the_edge_bytes_only_once() {
    let charged_after_expiry = run_edge_expiry(EdgeFate::DeletedBeforeExpiry);
    assert_eq!(
        charged_after_expiry.after_expiry, charged_after_expiry.before_data,
        "the expiry pass drops the deleted edge's tombstone and the expired rows \
         together, handing each byte back once, so the accountant lands exactly \
         where it stood before any of this data existed; a lower figure is the \
         edge's bytes returned twice"
    );
}

/// The companion arm: with the edge still live at expiry, its bytes are handed
/// back for the first and only time by the expiry pass. This is what keeps the
/// test above honest — the invariant is "returned once", not "never returned".
#[test]
fn expiring_a_row_whose_edge_is_live_returns_the_edge_bytes_exactly_once() {
    let charged_after_expiry = run_edge_expiry(EdgeFate::LiveAtExpiry);
    assert_eq!(
        charged_after_expiry.after_expiry, charged_after_expiry.before_data,
        "expiring the rows an edge connects reclaims the edge too, so the \
         accountant lands exactly where it stood before any of this data \
         existed"
    );
}

enum EdgeFate {
    DeletedBeforeExpiry,
    LiveAtExpiry,
}

struct ChargeAroundExpiry {
    before_data: usize,
    after_expiry: usize,
}

/// Two connected rows in an expiring table plus one edge between them, taken
/// through expiry. The only difference between the arms is whether the edge is
/// deleted first; both arms must land on the same figure they started from.
fn run_edge_expiry(fate: EdgeFate) -> ChargeAroundExpiry {
    let now = Arc::new(AtomicU64::new(1_000_000));
    let _clock = {
        let now = Arc::clone(&now);
        Wallclock::test_clock_guard(move || now.load(Ordering::SeqCst))
    };

    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE nodes (id UUID PRIMARY KEY, body TEXT) RETAIN 1 SECONDS",
        &p(),
    )
    .expect("declare an expiring table");
    let before_data = charged(&db);

    let source = Uuid::new_v4();
    let target = Uuid::new_v4();
    for (id, seed) in [(source, 7usize), (target, 8usize)] {
        let mut params = HashMap::new();
        params.insert("id".to_string(), Value::Uuid(id));
        params.insert("b".to_string(), Value::Text(body(seed)));
        db.execute("INSERT INTO nodes (id, body) VALUES ($id, $b)", &params)
            .expect("insert an expiring row");
    }

    let tx = db.begin_or_panic();
    db.insert_edge(tx, source, target, "LINKS".to_string(), HashMap::new())
        .expect("connect the two rows");
    db.commit(tx).expect("commit the edge");
    let charged_with_edge = charged(&db);

    if matches!(fate, EdgeFate::DeletedBeforeExpiry) {
        let tx = db.begin_or_panic();
        db.delete_edge(tx, source, target, "LINKS")
            .expect("delete the edge");
        db.commit(tx).expect("commit the deletion");
        assert_eq!(
            charged(&db),
            charged_with_edge,
            "deleting an edge tombstones it: the entry stays whole in both adjacency \
             maps, so the store is still charged for what it is still holding"
        );
    }

    // Past the declared window; the expiry read runs on this thread, under the
    // seamed clock this guard installs.
    now.fetch_add(5_000, Ordering::SeqCst);
    let report = db
        .run_pruning_cycle_checked()
        .expect("run the expiry pass once");
    assert_eq!(
        report.pruned_rows, 2,
        "both connected rows are past their window: {report:?}"
    );

    ChargeAroundExpiry {
        before_data,
        after_expiry: charged(&db),
    }
}
