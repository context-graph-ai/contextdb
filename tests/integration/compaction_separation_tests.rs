//! Compaction separation: a scoped, O(pruned) cleanup pass (retention or
//! currency version cleanup) and a full-file, O(whole-file) redb `compact()`
//! are different-shaped costs, reported through different receipts.
//! Currency version cleanup (`compact_currency_versions`) NEVER compacts on
//! its own -- compaction for a currency table is either the explicit
//! operator action (`Database::compact_now`, `.maintenance compact`) or the
//! much rarer, interval-gated automatic path the engine-owned tick can take
//! (`Database::run_maintenance_cycle`'s own `MaintenanceReport.compaction`).
//! See `Database::compact_now`'s and `AUTO_COMPACT_MIN_INTERVAL`'s own doc
//! comments in `contextdb_engine::database` for the full rationale.
//!
//! Discipline: no sleeps, no elapsed-time assertions, no raw clock reads --
//! the interval gate is proven via `__set_auto_compact_min_interval_for_test`
//! (a huge interval to prove suppression, a zero interval to prove the gate
//! fires again the moment it is next due), never real time.

use contextdb_core::Value;
use contextdb_engine::work_ledger::install_work_ledger_schema;
use contextdb_engine::{Database, MaintenancePolicy};
use std::collections::HashMap;
use std::time::Duration;

fn advertise(db: &Database, key: &str, capability_id: &str, advertised_at: i64) {
    let mut params = HashMap::new();
    params.insert("k".to_string(), Value::Text(key.to_string()));
    params.insert("node_id".to_string(), Value::Text(key.to_string()));
    params.insert(
        "capability_id".to_string(),
        Value::Text(capability_id.to_string()),
    );
    params.insert("tags".to_string(), Value::Json(serde_json::json!([])));
    params.insert("advertised_at".to_string(), Value::Timestamp(advertised_at));
    db.execute(
        "INSERT INTO work_capabilities (capability_key, node_id, capability_id, tags, detail, \
         advertised_at) VALUES ($k, $node_id, $capability_id, $tags, NULL, $advertised_at) \
         ON CONFLICT (capability_key) DO UPDATE SET \
         advertised_at = $advertised_at, node_id = $node_id, capability_id = $capability_id",
        &params,
    )
    .expect("advertise/re-advertise a capability");
}

/// Enough churn on one logical row, on a file-backed database, to leave the
/// file dead-space dominated -- the same shape the version-cleanup scaling
/// bench's steady-state arms use, which measured this reliably crossing
/// `REDB_COMPACT_FRAGMENTATION_THRESHOLD` on real redb files.
fn churn_one_row(db: &Database, writes: u64) {
    for v in 0..writes {
        advertise(
            db,
            "node-a",
            &format!("cap-{v}"),
            1_700_000_000_000 + v as i64,
        );
    }
}

fn open_caller_driven(path: &std::path::Path) -> Database {
    let db = Database::open(path).expect("open a file-backed database");
    install_work_ledger_schema(&db).expect("install work ledger schema");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db
}

/// The headline separation: a scoped currency cleanup pass that DOES prune
/// (and, per the churn shape above, crosses the shared fragmentation
/// threshold) must still report `redb_compacted == false` -- it never calls
/// `persistence.compact()` itself, however dead-space-dominated the file is.
#[test]
fn currency_cleanup_never_compacts_on_its_own() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open_caller_driven(&dir.path().join("never_auto.db"));
    churn_one_row(&db, 2_000);

    let report = db
        .compact_currency_versions()
        .expect("compaction must succeed");
    assert!(
        report.pruned_versions > 0,
        "setup: churn must leave prunable versions: {report:?}"
    );
    assert!(
        !report.redb_compacted,
        "currency version cleanup must never trigger a full-file redb compaction on its \
         own, however dead-space-dominated the file is: {report:?}"
    );
    assert_eq!(
        report.redb_compact_micros, 0,
        "no compaction ran, so its time must read zero: {report:?}"
    );
}

/// The explicit operator action: unconditional, no threshold, no interval
/// gate -- calling it twice in a row both times actually compacts (unlike
/// the automatic path, which the next test proves throttles the SECOND
/// call).
#[test]
fn compact_now_is_unconditional_and_reports_a_real_receipt() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open_caller_driven(&dir.path().join("compact_now.db"));
    churn_one_row(&db, 2_000);
    db.compact_currency_versions()
        .expect("compaction must succeed");

    let first = db.compact_now().expect("explicit compaction must succeed");
    assert!(
        first.ran,
        "an explicit, on-demand compaction must always run on a file-backed database: \
         {first:?}"
    );
    let before = first
        .bytes_before
        .expect("a file-backed database reports its size before compaction");
    let after = first
        .bytes_after
        .expect("a file-backed database reports its size after compaction");
    assert!(
        after <= before,
        "a compaction over a dead-space-dominated file must not leave it larger \
         (before={before}, after={after})"
    );

    // Calling it again immediately still runs -- the explicit verb has no
    // interval gate at all, unlike the automatic path.
    let second = db
        .compact_now()
        .expect("a second explicit compaction must succeed");
    assert!(
        second.ran,
        "compact_now has no interval gate -- a second immediate call must still run: \
         {second:?}"
    );
}

/// An in-memory database has no file to compact -- both the explicit verb
/// and the automatic path must report an honest no-op, never an error.
#[test]
fn compact_now_is_a_no_op_on_an_in_memory_database() {
    let db = Database::open_memory();
    let report = db
        .compact_now()
        .expect("must not error on an in-memory database");
    assert_eq!(
        report,
        contextdb_engine::CompactionReport::default(),
        "an in-memory database has nothing to compact: {report:?}"
    );
}

/// The rare automatic path, proven deterministically (no sleeps): with the
/// shipped default interval, a SECOND qualifying cycle run immediately after
/// the first must NOT recompact -- this is the exact 20/20-cycles-fire
/// defect the bench caught, now closed. Then, with the interval overridden
/// to zero, the NEXT cycle proves the gate still fires the moment it is
/// due, not merely "disabled."
#[test]
fn the_automatic_compaction_path_is_interval_gated_not_per_cycle() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open_caller_driven(&dir.path().join("interval_gated.db"));
    churn_one_row(&db, 2_000);

    // First qualifying cycle: nothing has EVER auto-compacted on this
    // handle, so the interval is trivially satisfied; if fragmentation is at
    // or above the shared threshold, this cycle compacts.
    let first = db
        .run_maintenance_cycle()
        .expect("first maintenance cycle must succeed");
    assert!(
        first.compaction.ran,
        "setup: the churned file must be dead-space-dominated enough to cross the shared \
         threshold on the very first check: {:?}",
        first.compaction
    );

    // More churn, still well past the threshold -- but the default interval
    // (one hour) has not elapsed since the compaction above, so a SECOND
    // cycle right after must NOT recompact. This is the defect: before the
    // separation, currency cleanup's OWN per-cycle check fired on 20/20
    // steady-state cycles at bench scale.
    churn_one_row(&db, 2_000);
    let second = db
        .run_maintenance_cycle()
        .expect("second maintenance cycle must succeed");
    assert!(
        !second.compaction.ran,
        "a second qualifying cycle immediately after the first must not recompact -- the \
         minimum interval, not the threshold alone, gates the automatic path: {:?}",
        second.compaction
    );

    // Shrink the interval to zero (test-only override): the very NEXT cycle
    // must compact again, proving the gate is a THROTTLE, not a permanent
    // disable once tripped once.
    db.__set_auto_compact_min_interval_for_test(Duration::ZERO);
    churn_one_row(&db, 2_000);
    let third = db
        .run_maintenance_cycle()
        .expect("third maintenance cycle must succeed");
    assert!(
        third.compaction.ran,
        "once the interval is satisfied again, the next qualifying cycle must compact: \
         {:?}",
        third.compaction
    );
}

/// `compact_now` (an explicit, unconditional compaction) also resets the
/// automatic path's own interval clock -- an operator who just compacted by
/// hand should not have the tick immediately redo the same work.
#[test]
fn compact_now_resets_the_automatic_paths_interval_clock() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open_caller_driven(&dir.path().join("resets_clock.db"));
    churn_one_row(&db, 2_000);

    let explicit = db.compact_now().expect("explicit compaction must succeed");
    assert!(
        explicit.ran,
        "setup: the explicit call must actually compact"
    );

    churn_one_row(&db, 2_000);
    let cycle = db
        .run_maintenance_cycle()
        .expect("maintenance cycle must succeed");
    assert!(
        !cycle.compaction.ran,
        "the explicit compact_now above must count toward the automatic path's own \
         interval, so an immediately-following cycle must not recompact: {:?}",
        cycle.compaction
    );
}

fn col_idx(result: &contextdb_engine::QueryResult, name: &str) -> usize {
    result
        .columns
        .iter()
        .position(|c| c == name)
        .unwrap_or_else(|| panic!("column '{name}' not found in {:?}", result.columns))
}

/// A compaction restores steady-state write cost, not just file size, by
/// closing and reopening the store's redb handle after the file-level
/// compaction finishes -- reported honestly on the receipt, and the store
/// stays fully usable across the swap: a value written before compaction is
/// still readable immediately after, through the SAME `Database` handle, no
/// reopen required by the caller.
#[test]
fn compact_now_recycles_the_handle_and_reports_it_honestly() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = open_caller_driven(&dir.path().join("recycles_handle.db"));
    churn_one_row(&db, 2_000);
    advertise(&db, "node-marker", "cap-marker", 1_750_000_000_000);

    let report = db.compact_now().expect("explicit compaction must succeed");
    assert!(report.ran, "setup: the explicit call must actually compact");
    assert!(
        report.handle_recycled,
        "an explicit compaction on a file-backed database must recycle the handle, so a \
         long-running consumer that cannot process-restart still gets its steady-state cost \
         restored: {report:?}"
    );

    // The report's own recycle timing is a real, separately-measured
    // duration, never folded silently into the compaction total with no
    // trace of its own cost.
    assert!(
        report.handle_recycle_micros <= report.duration_micros,
        "the recycle is one part of the whole compaction call, so its own duration cannot \
         exceed the call's total duration: {report:?}"
    );

    // The handle swap is transparent: the SAME `Database` value, used
    // immediately after `compact_now` returns, still reads back the value
    // written before compaction ran.
    let result = db
        .execute("SELECT * FROM work_capabilities", &HashMap::new())
        .expect("a query right after compact_now must succeed through the recycled handle");
    let idx = col_idx(&result, "capability_id");
    let node_idx = col_idx(&result, "node_id");
    let found = result.rows.iter().any(|row| {
        row[node_idx] == Value::Text("node-marker".to_string())
            && row[idx] == Value::Text("cap-marker".to_string())
    });
    assert!(
        found,
        "the row written before compaction must still be readable through the same handle \
         right after the recycle: {:?}",
        result.rows
    );
}

/// A reopen failure during the handle recycle must never lose data. The
/// file-level compaction has already finished (and already succeeded)
/// before the recycle step runs, so the on-disk file is untouched by a
/// reopen failure; the failure surfaces as the typed
/// `StoreHandleRecycleFailed` error (never a generic string error, never a
/// panic, never a silent success), this `Database` is left closed, and
/// closing it and opening a fresh `Database` on the SAME path recovers
/// every row -- including whatever the compaction itself had already
/// reclaimed.
#[test]
fn a_failed_handle_recycle_leaves_the_file_intact_and_a_fresh_open_recovers() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("recycle_fault.db");
    let db = open_caller_driven(&path);
    churn_one_row(&db, 2_000);
    advertise(&db, "node-marker", "cap-marker", 1_750_000_000_000);

    db.__arm_handle_recycle_reopen_fault_for_test();
    let err = db
        .compact_now()
        .expect_err("an injected reopen failure must surface as an error, not succeed silently");
    assert!(
        matches!(err, contextdb_core::Error::StoreHandleRecycleFailed { .. }),
        "a reopen failure must surface as the typed StoreHandleRecycleFailed variant, not a \
         generic error: {err:?}"
    );

    // This instance's redb handle was already dropped as part of the failed
    // recycle attempt; release its pid lock so a fresh instance can open the
    // same path.
    db.close()
        .expect("closing an already-recycle-failed database must still succeed");

    let reopened = Database::open(&path).expect(
        "the on-disk file must be untouched by a failed handle recycle -- a fresh open on the \
         same path must succeed",
    );
    let result = reopened
        .execute("SELECT * FROM work_capabilities", &HashMap::new())
        .expect("a fresh open must read back every row, including what compaction reclaimed");
    let idx = col_idx(&result, "capability_id");
    let node_idx = col_idx(&result, "node_id");
    let found = result.rows.iter().any(|row| {
        row[node_idx] == Value::Text("node-marker".to_string())
            && row[idx] == Value::Text("cap-marker".to_string())
    });
    assert!(
        found,
        "the marker row must survive a failed handle recycle followed by a fresh open: {:?}",
        result.rows
    );
}
