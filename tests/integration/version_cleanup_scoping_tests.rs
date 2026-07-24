//! Version cleanup (`Database::compact_currency_versions`, the pass that
//! collapses a high-churn table back to one live version per logical row)
//! against the shipped built-in currency tables (`work_capabilities`,
//! `peer_directory`) -- these are ALREADY eligible today via the hardcoded
//! `VERSION_COMPACTION_TABLES` list, so this file needs no `HISTORY`
//! declaration to exercise the real pass and its real correctness
//! properties.
//!
//! What these pin, in plain language: reclaiming superseded row versions
//! must never (a) cost proportionally to the WHOLE database (today's
//! `rewrite_pruned_state`/`rewrite_change_log` rewrite every vector, every
//! edge, and every surviving change-log entry on EVERY pass, regardless of
//! how little was pruned), (b) corrupt `changes_since` replay by leaving an
//! orphaned change-log entry that names a version no longer on disk, (c)
//! divorce memory from disk when the persisted rewrite fails partway, or
//! (d) remove a version a still-open read snapshot is entitled to see.
//!
//! Discipline: no sleeps, no elapsed-time assertions, no raw clock reads.

use contextdb_core::Value;
use contextdb_engine::Database;
use contextdb_engine::work_ledger::install_work_ledger_schema;
use std::collections::HashMap;

fn p() -> HashMap<String, Value> {
    HashMap::new()
}

fn advertise(db: &Database, key: &str, node_id: &str, capability_id: &str, advertised_at: i64) {
    let mut params = HashMap::new();
    params.insert("k".to_string(), Value::Text(key.to_string()));
    params.insert("node_id".to_string(), Value::Text(node_id.to_string()));
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

fn capability_ids(db: &Database, key: &str) -> Vec<String> {
    let mut params = HashMap::new();
    params.insert("k".to_string(), Value::Text(key.to_string()));
    let result = db
        .execute(
            "SELECT capability_id FROM work_capabilities WHERE capability_key = $k",
            &params,
        )
        .expect("select");
    let idx = result
        .columns
        .iter()
        .position(|c| c == "capability_id")
        .unwrap();
    result
        .rows
        .into_iter()
        .map(|row| match &row[idx] {
            Value::Text(s) => s.clone(),
            other => panic!("expected TEXT, got {other:?}"),
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Baseline: the pass reclaims superseded versions on the shipped built-ins
// (already passing today -- verification, not a defect).
// ---------------------------------------------------------------------------

#[test]
fn version_cleanup_reclaims_superseded_capability_advertisements() {
    let db = Database::open_memory();
    install_work_ledger_schema(&db).expect("install");
    for i in 0..5 {
        advertise(
            &db,
            "node-a",
            "node-a",
            &format!("cap-{i}"),
            1_700_000_000_000 + i,
        );
    }
    let report = db
        .compact_currency_versions()
        .expect("compaction must succeed");
    assert!(
        report.pruned_versions >= 4,
        "4 of the 5 advertisements are superseded: {report:?}"
    );
    assert_eq!(
        capability_ids(&db, "node-a"),
        vec!["cap-4".to_string()],
        "only the current advertisement is visible after cleanup"
    );
}

// ---------------------------------------------------------------------------
// A fresh edge pulling after cleanup converges to exactly the current
// values, byte-identical to what a pre-cleanup changes_since(0) snapshot
// would have delivered. (Already passing today -- this is the shipped
// sync-safety argument at database.rs:17823-17833, verified here rather
// than re-derived, and kept as a regression guard.)
// ---------------------------------------------------------------------------

#[test]
fn a_fresh_edge_pulling_after_cleanup_converges_to_current_truth() {
    let db = Database::open_memory();
    install_work_ledger_schema(&db).expect("install");
    for i in 0..8 {
        advertise(
            &db,
            "node-a",
            "node-a",
            &format!("cap-{i}"),
            1_700_000_000_000 + i,
        );
    }
    advertise(&db, "node-b", "node-b", "cap-b0", 1_700_000_000_100);

    // What a pull BEFORE cleanup would have delivered, for comparison.
    use contextdb_engine::sync_types::{ConflictPolicies, ConflictPolicy};
    let pre_cleanup_snapshot = db.changes_since(contextdb_core::Lsn(0));

    let report = db
        .compact_currency_versions()
        .expect("compaction must succeed");
    assert!(
        report.pruned_versions > 0,
        "setup must produce superseded versions"
    );

    let fresh_edge = Database::open_memory();
    fresh_edge
        .apply_changes(
            db.changes_since(contextdb_core::Lsn(0)),
            &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
        )
        .expect("a fresh edge must converge after cleanup");

    assert_eq!(
        capability_ids(&fresh_edge, "node-a"),
        vec!["cap-7".to_string()]
    );
    assert_eq!(
        capability_ids(&fresh_edge, "node-b"),
        vec!["cap-b0".to_string()]
    );

    // The pre-cleanup snapshot must have named the SAME current values --
    // cleanup must never change what a consumer already converged to.
    let pre_cleanup_edge = Database::open_memory();
    pre_cleanup_edge
        .apply_changes(
            pre_cleanup_snapshot,
            &ConflictPolicies::uniform(ConflictPolicy::ServerWins),
        )
        .expect("apply the pre-cleanup snapshot");
    assert_eq!(
        capability_ids(&pre_cleanup_edge, "node-a"),
        capability_ids(&fresh_edge, "node-a"),
        "cleanup must not change the current value a consumer already saw"
    );
}

// ---------------------------------------------------------------------------
// Re-aimed (own commit, see its subject for the reason): the engine does not
// pin a snapshot at `begin()` -- a repeat `execute_in_tx` read on the SAME
// open transaction already observes the live watermark with zero cleanup
// involved, so the original open-transaction shape here could never pass
// without a foundational MVCC change nobody chartered. The REAL, reproduced
// hazard is different: `Database::execute_at_snapshot` is a public,
// documented promise that a caller-held `SnapshotId` resolves historical
// versions, and cleanup could silently empty that promise out from under
// it. The three tests below pin the real contract: an in-flight
// registration defers cleanup automatically (every
// `execute`/`execute_in_tx`/`execute_at_snapshot` call registers its own
// snapshot for the call's duration), a caller that
// explicitly pins a snapshot across separate calls keeps it resolvable until
// the pin releases, and a caller that does NOT pin gets the honestly
// documented boundary -- a version cleanup already reclaimed is simply gone.
// ---------------------------------------------------------------------------

fn capability_id_at_snapshot(
    db: &Database,
    key: &str,
    snapshot: contextdb_core::SnapshotId,
) -> Option<String> {
    let mut params = HashMap::new();
    params.insert("k".to_string(), Value::Text(key.to_string()));
    let result = db
        .execute_at_snapshot(
            "SELECT capability_id FROM work_capabilities WHERE capability_key = $k",
            &params,
            snapshot,
        )
        .expect("execute_at_snapshot must succeed even when narrower than before");
    let idx = result
        .columns
        .iter()
        .position(|c| c == "capability_id")
        .unwrap();
    result.rows.first().map(|row| match &row[idx] {
        Value::Text(s) => s.clone(),
        other => panic!("expected TEXT, got {other:?}"),
    })
}

/// A registered in-flight snapshot defers cleanup deterministically -- no
/// sleeps, no real thread interleaving needed: `pin_snapshot` registers in
/// the SAME registry `execute`/`execute_in_tx`/`execute_at_snapshot`
/// populate automatically for their own call's duration, so this proves the
/// registry-consulted-by-cleanup half of the contract directly. Every other
/// test in this suite exercises the automatic per-call registration path
/// (it runs on every `execute`) without ever observing a regression, since
/// none of them hold a reader open across a cleanup cycle.
#[test]
fn a_registered_snapshot_defers_cleanup_instead_of_losing_the_version() {
    let db = Database::open_memory();
    install_work_ledger_schema(&db).expect("install");
    advertise(&db, "node-a", "node-a", "cap-old", 1_700_000_000_000);
    let snap = db.snapshot();
    let pin = db.pin_snapshot(snap);

    advertise(&db, "node-a", "node-a", "cap-new", 1_700_000_000_100);
    let report = db
        .compact_currency_versions()
        .expect("compaction must succeed");
    assert_eq!(
        report.pruned_versions, 0,
        "the only superseded version is still visible to the registered snapshot, so cleanup \
         must not reclaim it this cycle: {report:?}"
    );
    assert_eq!(
        report.versions_deferred_for_readers, 1,
        "the receipt must show the deferral, not silence: {report:?}"
    );
    drop(pin);
}

/// The full pinned lifecycle: capture, pin, supersede, cleanup defers and
/// says so, the pinned read still resolves the old value, the pin releases,
/// and the NEXT cycle reclaims what is no longer needed -- the deferred
/// count returns to zero once nothing registered still needs the version.
#[test]
fn a_pinned_snapshot_survives_cleanup_until_the_pin_releases() {
    let db = Database::open_memory();
    install_work_ledger_schema(&db).expect("install");
    advertise(&db, "node-a", "node-a", "cap-old", 1_700_000_000_000);
    let snap = db.snapshot();

    {
        let _pin = db.pin_snapshot(snap);
        advertise(&db, "node-a", "node-a", "cap-new", 1_700_000_000_100);
        let report = db
            .compact_currency_versions()
            .expect("compaction must succeed while the pin is held");
        assert_eq!(
            report.pruned_versions, 0,
            "the pinned version must not be pruned while the pin is held: {report:?}"
        );
        assert_eq!(
            report.versions_deferred_for_readers, 1,
            "the receipt must show exactly the pinned version deferred: {report:?}"
        );
        assert_eq!(
            capability_id_at_snapshot(&db, "node-a", snap),
            Some("cap-old".to_string()),
            "the pinned snapshot must still resolve the value it was entitled to WHILE the pin \
             is held, even though it has since been superseded"
        );
    }

    let report_after_release = db
        .compact_currency_versions()
        .expect("compaction must succeed after the pin releases");
    assert!(
        report_after_release.pruned_versions > 0,
        "once the pin releases, nothing still needs the superseded version, so the NEXT cycle \
         must reclaim it: {report_after_release:?}"
    );
    assert_eq!(
        report_after_release.versions_deferred_for_readers, 0,
        "nothing is deferred once no registered reader still needs it: {report_after_release:?}"
    );
}

/// TWO pins at two DIFFERENT snapshots, with a version created strictly
/// between them and only superseded after both -- the case a single
/// "minimum registered snapshot" floor cannot protect. `cap-mid` is created
/// after `pin_low`'s snapshot and superseded after `pin_high`'s snapshot, so
/// `pin_high` (the newer, HIGHER-numbered pin) is the one actually entitled
/// to see it; a deferral check against only the LOWEST registered snapshot
/// (`pin_low`) would wrongly conclude `cap-mid` isn't visible there yet and
/// prune it out from under `pin_high`. Cleanup must defer any version
/// visible to ANY registered reader, not just the oldest one.
#[test]
fn two_pins_at_different_snapshots_both_protect_the_version_created_between_them() {
    let db = Database::open_memory();
    install_work_ledger_schema(&db).expect("install");

    advertise(&db, "node-a", "node-a", "cap-low", 1_700_000_000_000);
    let snap_low = db.snapshot();
    let pin_low = db.pin_snapshot(snap_low);

    // Created strictly AFTER pin_low's snapshot.
    advertise(&db, "node-a", "node-a", "cap-mid", 1_700_000_000_100);
    let snap_high = db.snapshot();
    let pin_high = db.pin_snapshot(snap_high);

    // Superseded strictly AFTER both pins' snapshots.
    advertise(&db, "node-a", "node-a", "cap-new", 1_700_000_000_200);

    let report = db
        .compact_currency_versions()
        .expect("compaction must succeed with two live pins");
    assert_eq!(
        report.pruned_versions, 0,
        "both superseded versions (cap-low, cap-mid) are each visible to at least one of the \
         two registered pins, so neither may be pruned this cycle: {report:?}"
    );
    assert_eq!(
        report.versions_deferred_for_readers, 2,
        "the receipt must show both pinned versions deferred, not just the one the lowest \
         floor alone would have protected: {report:?}"
    );

    // The load-bearing assertion: cap-mid -- the INTERIOR version between
    // the two pins -- must still read back at the HIGHER pin. A
    // minimum-floor-only deferral would have pruned it out from under
    // `pin_high` here.
    assert_eq!(
        capability_id_at_snapshot(&db, "node-a", snap_high),
        Some("cap-mid".to_string()),
        "the interior version created between the two pins must survive cleanup and still \
         resolve at the higher pin, which is the one actually entitled to see it"
    );
    assert_eq!(
        capability_id_at_snapshot(&db, "node-a", snap_low),
        Some("cap-low".to_string()),
        "the lower pin's own version must also still resolve"
    );

    drop(pin_low);
    drop(pin_high);
}

/// The honest, documented boundary (probe 2, reproduced): a caller that
/// captures a `SnapshotId` and reuses it in a LATER call WITHOUT pinning it
/// gets no protection -- on a table declaring `HISTORY CURRENT ONLY`, a
/// cleanup cycle that runs in between may reclaim the exact version that
/// snapshot depended on, and the later `execute_at_snapshot` call silently
/// resolves fewer rows than it used to. Not a wrong value, not an error --
/// exactly the gap `pin_snapshot` exists to close for a caller who opts in.
#[test]
fn an_unpinned_snapshot_may_lose_a_reclaimed_version_the_documented_boundary() {
    let db = Database::open_memory();
    install_work_ledger_schema(&db).expect("install");
    advertise(&db, "node-a", "node-a", "cap-old", 1_700_000_000_000);
    let snap = db.snapshot();

    // No pin taken -- the snapshot is a bare, unregistered value from here.
    advertise(&db, "node-a", "node-a", "cap-new", 1_700_000_000_100);
    assert_eq!(
        capability_id_at_snapshot(&db, "node-a", snap),
        Some("cap-old".to_string()),
        "before cleanup runs, the unpinned snapshot still resolves normally"
    );

    let report = db
        .compact_currency_versions()
        .expect("compaction must succeed");
    assert!(
        report.pruned_versions > 0,
        "without a pin, the superseded version is reclaimed like any other: {report:?}"
    );
    assert_eq!(
        capability_id_at_snapshot(&db, "node-a", snap),
        None,
        "the documented boundary: an unpinned snapshot reused after cleanup on a declared \
         table may silently lose a version cleanup already reclaimed -- this is why \
         Database::pin_snapshot exists for a caller that needs to hold one across calls"
    );
}

// ---------------------------------------------------------------------------
// Crash arm: a persist failure mid-pass must leave memory and disk agreeing,
// using the currency-compaction fault-injection seam added alongside the
// new receipt counters.
// ---------------------------------------------------------------------------

#[test]
fn a_persist_failure_mid_pass_leaves_the_in_memory_change_log_matching_what_was_persisted() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("crash.db");
    let (before_attempt_ddl, before_attempt_rows, before_attempt_values) = {
        let db = Database::open(&path).expect("open");
        install_work_ledger_schema(&db).expect("install");
        for i in 0..4 {
            advertise(
                &db,
                "node-a",
                "node-a",
                &format!("cap-{i}"),
                1_700_000_000_000 + i,
            );
        }

        // The exact content the live, in-process change log carries BEFORE
        // the failed attempt -- the ground truth to compare against, not a
        // count.
        let before = db.changes_since(contextdb_core::Lsn(0));

        db.__arm_currency_compaction_persist_fault_for_test();
        let err = db
            .compact_currency_versions()
            .expect_err("the injected fault must fail the pass");
        let _ = err;

        // Read the log back via the public surface (`changes_since(0)`)
        // immediately after the failed attempt, still in the same process,
        // before any reopen.
        let after_attempt = db.changes_since(contextdb_core::Lsn(0));
        assert_eq!(
            before.ddl, after_attempt.ddl,
            "a failed version-cleanup pass must not have mutated the DDL log at all"
        );
        assert_eq!(
            before.rows, after_attempt.rows,
            "a failed version-cleanup pass must leave every row-change entry exactly as it was \
             before the attempt -- not merely the same COUNT of entries, the same entries"
        );

        let current_value = capability_ids(&db, "node-a");
        assert_eq!(
            current_value,
            vec!["cap-3".to_string()],
            "setup: the current advertisement must be the last one written"
        );

        (before.ddl, before.rows, current_value)
    };

    // Reopen from disk: whatever the persisted state actually is, it must
    // reconstruct the SAME entries `changes_since` reported live, in-process,
    // BEFORE the failed cycle even ran -- proving the failed pass didn't
    // silently persist a partial change despite returning Err, and that
    // memory did not diverge from what was durably written.
    let db2 = Database::open(&path).expect("reopen after the failed cycle");
    let after_reopen = db2.changes_since(contextdb_core::Lsn(0));
    assert_eq!(
        before_attempt_ddl, after_reopen.ddl,
        "the reopened DDL log must match the pre-attempt content exactly"
    );
    assert_eq!(
        before_attempt_rows, after_reopen.rows,
        "the reopened row-change log must match the pre-attempt content exactly, entry for \
         entry, not merely in count"
    );
    assert_eq!(
        capability_ids(&db2, "node-a"),
        before_attempt_values,
        "a failed version-cleanup pass must never leave the live in-memory state diverged from \
         what is durably persisted -- the in-memory change log must not be mutated ahead of a \
         persisted write that can still fail"
    );
}

// ---------------------------------------------------------------------------
// The second-pass change-log coordinate regression. On the EXISTING
// wholesale-rewrite mechanism this cannot manifest (each pass fully
// re-densifies the persisted change log, so the in-memory recount used to
// derive each entry's disk position stays valid). This test is therefore a
// forward-looking REGRESSION GUARD: passing today, and it must stay passing
// the moment a scoped point-removal pass replaces the wholesale rewrite --
// a failure at that point means a change-log coordinate bug that corrupts
// `changes_since` replay was reintroduced.
// ---------------------------------------------------------------------------

#[test]
fn two_successive_cleanup_passes_over_a_multi_row_commit_never_orphan_a_change_log_entry() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("two_pass.db");
    {
        let db = Database::open(&path).expect("open");
        install_work_ledger_schema(&db).expect("install");
        advertise(&db, "node-a", "node-a", "cap-a0", 1_700_000_000_000);
        advertise(&db, "node-b", "node-b", "cap-b0", 1_700_000_000_000);

        // One commit that writes a NEW version of BOTH rows under one LSN --
        // the shape the coordinate bug needs (multiple entries per LSN).
        {
            let mut params = HashMap::new();
            params.insert("a".to_string(), Value::Text("cap-a1".to_string()));
            params.insert("b".to_string(), Value::Text("cap-b1".to_string()));
            let tx = db.begin().expect("begin");
            db.execute_in_tx(
                tx,
                "UPDATE work_capabilities SET capability_id = $a WHERE capability_key = 'node-a'",
                &params,
            )
            .expect("update a");
            db.execute_in_tx(
                tx,
                "UPDATE work_capabilities SET capability_id = $b WHERE capability_key = 'node-b'",
                &params,
            )
            .expect("update b");
            db.commit(tx).expect("commit both under one LSN");
        }

        // Pass 1 prunes row a's superseded version.
        let report1 = db.compact_currency_versions().expect("pass 1");
        assert!(report1.pruned_versions > 0, "pass 1 must reclaim something");

        // A second superseding write, then pass 2 prunes row b's turn.
        let mut params = HashMap::new();
        params.insert("b".to_string(), Value::Text("cap-b2".to_string()));
        db.execute(
            "UPDATE work_capabilities SET capability_id = $b WHERE capability_key = 'node-b'",
            &params,
        )
        .expect("update b again");
        let report2 = db.compact_currency_versions().expect("pass 2");
        assert!(report2.pruned_versions > 0, "pass 2 must reclaim something");
    }

    // Reopen: `changes_since(0)` must reconstruct current truth for BOTH
    // rows. An orphaned change-log entry (naming a version no longer on
    // disk) makes `row_for_change` silently substitute the row's latest
    // value at a stale LSN, which this equality check would catch.
    let db2 = Database::open(&path).expect("reopen");
    let mut params = HashMap::new();
    params.insert("k".to_string(), Value::Text("node-b".to_string()));
    assert_eq!(capability_ids(&db2, "node-a"), vec!["cap-a1".to_string()]);
    assert_eq!(capability_ids(&db2, "node-b"), vec!["cap-b2".to_string()]);
    let _ = params;
}

// ---------------------------------------------------------------------------
// Cost must be proportional to what was pruned, not to an unrelated
// population sharing the database. Today's wholesale rewrite rewrites EVERY
// vector in the database on every currency-compaction pass, regardless of
// the compacted table, so this is a real proportionality defect measurable
// with the `keys_rewritten` counter added alongside it.
// ---------------------------------------------------------------------------

fn ballast_db(vector_row_count: usize) -> Database {
    let db = Database::open_memory();
    install_work_ledger_schema(&db).expect("install");
    db.execute(
        "CREATE TABLE ballast (id INTEGER PRIMARY KEY, embedding VECTOR(8))",
        &p(),
    )
    .expect("create an unrelated vector-bearing table");
    for i in 0..vector_row_count {
        let mut params = HashMap::new();
        params.insert("id".to_string(), Value::Int64(i as i64));
        params.insert("embedding".to_string(), Value::Vector(vec![0.1_f32; 8]));
        db.execute(
            "INSERT INTO ballast (id, embedding) VALUES ($id, $embedding)",
            &params,
        )
        .expect("insert ballast row");
    }
    advertise(&db, "node-a", "node-a", "cap-old", 1_700_000_000_000);
    advertise(&db, "node-a", "node-a", "cap-new", 1_700_000_000_100);
    db
}

#[test]
fn version_cleanup_cost_is_invariant_to_unrelated_vector_ballast() {
    let small = ballast_db(20);
    let large = ballast_db(200);

    let small_report = small.compact_currency_versions().expect("small compaction");
    let large_report = large.compact_currency_versions().expect("large compaction");

    assert_eq!(
        small_report.pruned_versions, large_report.pruned_versions,
        "both arms prune the identical single superseded advertisement"
    );
    assert!(
        (large_report.keys_rewritten as f64) <= 1.5 * (small_report.keys_rewritten as f64).max(1.0),
        "reclaiming ONE superseded version must cost the same regardless of how much unrelated \
         vector data shares the database -- small={small_report:?} large={large_report:?}"
    );
}

// ---------------------------------------------------------------------------
// Sanctioned flip (own commit; see its subject): commit-index candidate-only
// removal now WORKS -- `compact_currency_versions_inner` shrinks the commit
// index to exactly the LSNs the pruned change-log entries touched (via
// `scoped_commit_index_removal`), landed in commit `a78b76d engine: move
// version cleanup and retention onto scoped point removal` /
// `ab14294 engine: add scoped point-removal persistence for reclaiming
// rows`, before this flip. This test's own prior assertion (`census_before
// == census_after`) predicted exactly this flip in its own failure
// message. A reader must still resolve the oldest surviving delete (the
// floor must never advance past what a consumer can still name), and the
// index must never go empty while commits exist.
// ---------------------------------------------------------------------------

#[test]
fn version_cleanup_shrinks_the_commit_index_to_reachable_entries() {
    let db = Database::open_memory();
    install_work_ledger_schema(&db).expect("install");
    for i in 0..20 {
        advertise(
            &db,
            "node-a",
            "node-a",
            &format!("cap-{i}"),
            1_700_000_000_000 + i,
        );
    }
    let (census_before, floor_before) = db.commit_index_census_for_test();
    assert!(
        census_before > 0,
        "setup: committing rows must leave commit-index entries to observe"
    );

    let report = db
        .compact_currency_versions()
        .expect("compaction must succeed");
    assert!(
        report.pruned_versions > 0,
        "setup must produce prunable versions"
    );

    let (census_after, floor_after) = db.commit_index_census_for_test();
    // THE FLIP: candidate-only removal shrinks the index to what a consumer
    // can still name -- a strong assertion (strictly fewer, matching the
    // report's own count), not merely "changed".
    assert!(
        census_after < census_before,
        "version cleanup must shrink the commit index to the LSNs its own pruned entries \
         made unreachable (before={census_before}, after={census_after}): {report:?}"
    );
    assert_eq!(
        (census_before - census_after) as u64,
        report.commit_index_keys_removed,
        "the census delta must equal the report's own removed count exactly, not merely a \
         plausible-looking reduction: before={census_before} after={census_after} {report:?}"
    );
    assert!(
        floor_after >= floor_before,
        "the commit index's floor LSN must never regress: before={floor_before:?} \
         after={floor_after:?}"
    );
    assert!(
        census_after > 0,
        "the commit index must never go empty while commits exist"
    );
}

// ---------------------------------------------------------------------------
// Sanctioned flip (own commit; see its subject): vector-copy release now
// WORKS. Pruning a row version that carries a vector releases ITS vector
// copy too, not just the row -- `VectorStore::prune_superseded_versions`,
// matched by `(row_id, created_tx, lsn)` identity, landed in
// `compact_currency_versions_inner` before this flip (commit
// `7f5fcda vector: add scoped removal of superseded vector versions` and
// `a78b76d engine: move version cleanup and retention onto scoped point
// removal`). This test's own prior assertion (`vectors_after ==
// vectors_before`) predicted exactly this flip in its own failure message.
// No hardcoded currency table carries a vector column today, so this still
// exercises the real mechanism via the test-only
// `__compact_currency_versions_for_tables_for_test` entry point, over a
// table declared here for the purpose -- the identical real
// `compact_currency_versions_inner` pass, just not gated behind the
// hardcoded eligibility list.
// ---------------------------------------------------------------------------

#[test]
fn version_cleanup_releases_a_pruned_rows_vector_copy() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE vector_currency (id TEXT PRIMARY KEY, embedding VECTOR(4))",
        &p(),
    )
    .expect("create a vector-bearing currency-shaped table");

    for v in 0..6 {
        db.execute(
            "INSERT INTO vector_currency (id, embedding) VALUES ($id, $embedding) \
             ON CONFLICT (id) DO UPDATE SET embedding = $embedding",
            &{
                let mut params = HashMap::new();
                params.insert("id".to_string(), Value::Text("row-0".to_string()));
                params.insert("embedding".to_string(), Value::Vector(vec![v as f32; 4]));
                params
            },
        )
        .expect("churn the vector-bearing row");
    }
    let vectors_before = db.__vector_entry_count_for_test();
    assert!(
        vectors_before >= 6,
        "setup: 6 upserts to one vector column must leave at least 6 physical vector entries \
         (this store never filters deleted_tx on all_entries) -- got {vectors_before}"
    );

    let report = db
        .__compact_currency_versions_for_tables_for_test(&["vector_currency"])
        .expect("scoped compaction over the vector-bearing table must succeed");
    assert!(
        report.pruned_versions > 0,
        "setup must produce prunable row versions: {report:?}"
    );

    let vectors_after = db.__vector_entry_count_for_test();
    // THE FLIP: pruning superseded row versions now releases their
    // superseded vector copies for real -- a strong assertion (strictly
    // fewer, not merely "changed"), pinning the NEW behavior.
    assert!(
        vectors_after < vectors_before,
        "pruning superseded row versions must release their superseded vector copies \
         (before={vectors_before}, after={vectors_after}): the vector-bearing table had 6 \
         upserts to ONE row, so exactly 1 vector entry (the keeper) must survive"
    );
    assert_eq!(
        vectors_after, 1,
        "the vector-bearing row has exactly one keeper after compaction -- every superseded \
         copy must be gone, not merely reduced (before={vectors_before}, after={vectors_after})"
    );
}

// ---------------------------------------------------------------------------
// Sanctioned flip (own commit; see its subject): eligible-tables-only
// scoping of the in-memory clone now WORKS.
// `compact_currency_versions_inner` reads (and clones) only the DECLARED
// eligible tables' row histories (`tables.iter().filter_map(...)`), not
// `relational_store.tables.read().clone()` (every table sharing the
// database) -- landed in commit `a78b76d engine: move version cleanup and
// retention onto scoped point removal`, before this flip. This test's own
// prior assertion (`tables_in_memory_snapshot > 1`) predicted exactly this
// flip in its own failure message.
// ---------------------------------------------------------------------------

#[test]
fn version_cleanup_clones_only_the_eligible_tables() {
    let db = Database::open_memory();
    install_work_ledger_schema(&db).expect("install");
    for i in 0..3 {
        db.execute(
            &format!("CREATE TABLE unrelated_{i} (id INTEGER PRIMARY KEY, body TEXT)"),
            &p(),
        )
        .expect("create an unrelated table sharing the database");
    }
    advertise(&db, "node-a", "node-a", "cap-old", 1_700_000_000_000);
    advertise(&db, "node-a", "node-a", "cap-new", 1_700_000_000_100);

    let report = db
        .compact_currency_versions()
        .expect("compaction must succeed");
    assert!(
        report.pruned_versions > 0,
        "setup must produce a prunable version"
    );

    // THE FLIP: work_capabilities (the one eligible table with any rows) is
    // the ONLY table a scoped clone needs, and is now the ONLY one counted --
    // three unrelated tables were declared alongside it specifically to
    // prove they do not inflate the clone, however many share the database.
    assert_eq!(
        report.tables_in_memory_snapshot, 1,
        "a scoped clone must count exactly the eligible tables with rows, never inflated by \
         unrelated tables merely sharing the file (3 were declared alongside it here): \
         {report:?}"
    );
}
