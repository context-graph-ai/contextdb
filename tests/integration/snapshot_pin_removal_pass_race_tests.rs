//! A `SnapshotPin` must never return while an in-flight version-cleanup
//! removal pass still prunes under it -- see `database.rs`'s
//! `SnapshotFloorRegistry::begin_removal_pass` and `pin_snapshot`'s doc
//! comment for the exact contract.
//!
//! Before the fix: version cleanup sampled the snapshot floor ONCE at the
//! start of a pass, then pruned for the rest of the pass with no further
//! coordination. `Database::pin_snapshot` registered an arbitrary
//! caller-held snapshot and returned immediately, regardless of whether a
//! pass had already sampled its floor without accounting for it -- so a pin
//! could return while the SAME pass went on to remove versions the pin
//! believed itself entitled to.
//!
//! Deterministic, no sleeps: `Database::pause_after_currency_floor_sample_for_test`
//! pauses a removal pass immediately after it samples its floor, before any
//! persisted removal runs, so a concurrent `pin_snapshot` call can be proven
//! to block for the pass's real duration -- not a timing assumption.

use contextdb_core::Value;
use contextdb_engine::Database;
use contextdb_engine::work_ledger::install_work_ledger_schema;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

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

/// A `pin_snapshot` call racing against an in-flight removal pass that
/// sampled no reader floor at all (nothing registered yet, so the pass
/// protects nothing) must BLOCK until that pass fully finishes, rather than
/// returning immediately while the pass is still free to prune anything.
/// Once resolved, the pin -- now safely registered -- must protect a LATER
/// supersede exactly like an ordinary, non-racing pin does.
#[test]
fn pin_snapshot_blocks_for_a_racing_removal_pass_and_then_protects_normally() {
    let db = Arc::new(Database::open_memory());
    install_work_ledger_schema(&db).expect("install");

    // A version this in-flight pass WILL remove (nothing protects it when
    // the pass samples its floor): superseded before anything is paused.
    advertise(&db, "node-a", "node-a", "cap-old", 1_700_000_000_000);
    advertise(&db, "node-a", "node-a", "cap-mid", 1_700_000_000_100);

    // The point the pin will protect: `cap-mid` is CURRENT here.
    let snap = db.snapshot();

    let floor_pause = db.pause_after_currency_floor_sample_for_test();

    let db_pass = db.clone();
    let pass_handle = thread::spawn(move || {
        db_pass
            .compact_currency_versions()
            .expect("first removal pass must succeed")
    });

    assert!(
        floor_pause.wait_until_reached(Duration::from_secs(5)),
        "the removal pass must reach the floor-sample checkpoint deterministically"
    );

    // `pin_snapshot` races in HERE: the pass has already sampled its floor
    // (as `None` -- nothing was registered yet) and is paused before its
    // persisted removal, so nothing protects `snap` from THIS pass's
    // decision. The pin must not return yet.
    let (registered_tx, registered_rx) = mpsc::channel::<()>();
    let (release_pin_tx, release_pin_rx) = mpsc::channel::<()>();
    let db_pin = db.clone();
    let pin_handle = thread::spawn(move || {
        let _pin = db_pin.pin_snapshot(snap);
        registered_tx.send(()).expect("send pin-registered signal");
        // Hold the pin alive until the main thread says to let go, so the
        // main thread can exercise a SECOND cleanup pass while this pin is
        // still registered.
        let _ = release_pin_rx.recv();
    });

    assert!(
        registered_rx
            .recv_timeout(Duration::from_millis(300))
            .is_err(),
        "pin_snapshot must BLOCK while a removal pass with no established floor is in flight -- \
         a pin that returns here would falsely promise protection the in-flight pass can still \
         violate"
    );

    // Let the first pass finish: it removes `cap-old` (nothing protected it
    // when it sampled), then clears its marker and wakes the blocked pin.
    floor_pause.release();
    let first_pass_report = pass_handle.join().expect("pass thread must not panic");
    assert!(
        first_pass_report.pruned_versions > 0,
        "the racing pass must have reclaimed the superseded version nothing protected: \
         {first_pass_report:?}"
    );
    assert_eq!(
        capability_ids(&db, "node-a"),
        vec!["cap-mid".to_string()],
        "only the superseded cap-old version is gone; cap-mid (current) survives"
    );

    registered_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("pin_snapshot must eventually return once the racing pass finishes");

    // The pin is NOW safely registered (after the race resolved). Supersede
    // the version it protects and prove a SECOND, ordinary cleanup pass
    // defers it exactly like `a_pinned_snapshot_survives_cleanup_until_the_
    // pin_releases` -- the pre-existing, already-proven contract, now reached
    // via a pin that had to survive a race first.
    advertise(&db, "node-a", "node-a", "cap-new", 1_700_000_000_200);
    let second_pass_report = db
        .compact_currency_versions()
        .expect("second removal pass must succeed");
    assert_eq!(
        second_pass_report.pruned_versions, 0,
        "the pin, now registered, must defer the version it protects: {second_pass_report:?}"
    );
    assert_eq!(
        second_pass_report.versions_deferred_for_readers, 1,
        "the receipt must show exactly the pinned version deferred: {second_pass_report:?}"
    );
    assert_eq!(
        capability_id_at_snapshot(&db, "node-a", snap),
        Some("cap-mid".to_string()),
        "the pinned snapshot must still resolve the value it protects, even though it has since \
         been superseded"
    );

    // Release the pin and prove the deferred version is reclaimed once
    // nothing protects it anymore -- mirrors the existing pin lifecycle
    // contract exactly.
    release_pin_tx.send(()).expect("release the held pin");
    pin_handle.join().expect("pin thread must not panic");

    let third_pass_report = db
        .compact_currency_versions()
        .expect("third removal pass must succeed");
    assert!(
        third_pass_report.pruned_versions > 0,
        "once the pin releases, the next pass must reclaim what it no longer protects: \
         {third_pass_report:?}"
    );
}
