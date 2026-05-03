//! tests/acceptance/txid_monotonic_concurrent_commits.rs
//!
//! §18 — TxIdColumnMonotonicUnderConcurrentCommits.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use contextdb_core::{Error, TxId, Value};
use contextdb_engine::Database;
use tempfile::tempdir;
use uuid::Uuid;

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn open_db() -> (tempfile::TempDir, Database) {
    let dir = tempdir().expect("tempdir");
    let db = Database::open(dir.path().join("t18.db")).expect("open db");
    (dir, db)
}

#[test]
fn t18_01_concurrent_committers_stamp_distinct_txids() {
    let (_dir, db) = open_db();
    let db = &db;
    db.execute(
        "CREATE TABLE evidence_log (id UUID PRIMARY KEY, recorded_at TXID NOT NULL)",
        &empty(),
    )
    .unwrap();
    let receiver = db.subscribe();
    let txs: Vec<_> = (0..3).map(|_| db.begin()).collect();
    for tx in &txs {
        db.execute_in_tx(
            *tx,
            "INSERT INTO evidence_log (id, recorded_at) VALUES ($id, $recorded_at)",
            &[
                ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                ("recorded_at".to_string(), Value::TxId(*tx)),
            ]
            .into_iter()
            .collect(),
        )
        .unwrap();
    }
    for tx in txs.iter().rev() {
        db.commit(*tx).unwrap();
    }

    let mut events = Vec::new();
    for _ in 0..3 {
        events.push(
            receiver
                .recv_timeout(Duration::from_secs(2))
                .expect("commit event"),
        );
    }
    events.sort_by_key(|event| event.lsn.0);
    let lsn_ordered_txids: Vec<TxId> = events
        .iter()
        .map(|event| TxId(db.snapshot_at(event.lsn).0))
        .collect();
    let mut stamped: Vec<TxId> = db
        .execute("SELECT recorded_at FROM evidence_log", &empty())
        .unwrap()
        .rows
        .into_iter()
        .filter_map(|row| match &row[0] {
            Value::TxId(t) => Some(*t),
            _ => None,
        })
        .collect();
    stamped.sort_by_key(|tx| tx.0);
    assert_eq!(
        stamped, lsn_ordered_txids,
        "caller-supplied active TxIds must be rewritten to commit-order TxIds"
    );
}

#[test]
fn t18_02_pagination_cursor_after_concurrent_commits_misses_no_rows() {
    let (_dir, db) = open_db();
    let db = &db;
    db.execute(
        "CREATE TABLE audit_log (id UUID PRIMARY KEY, recorded_at TXID NOT NULL)",
        &empty(),
    )
    .unwrap();
    let receiver = db.subscribe();

    // Deterministic interleaving: tx_a begins first but commits after tx_b.
    // Cursor pagination from tx_b's commit must still see tx_a's later commit.
    let tx_a = db.begin();
    let tx_b = db.begin();
    let row_a = Uuid::new_v4();
    let row_b = Uuid::new_v4();
    for (tx, row_id) in [(tx_a, row_a), (tx_b, row_b)] {
        db.execute_in_tx(
            tx,
            "INSERT INTO audit_log (id) VALUES ($id)",
            &[("id".to_string(), Value::Uuid(row_id))]
                .into_iter()
                .collect(),
        )
        .unwrap();
    }

    db.commit(tx_b).unwrap();
    let event_b = receiver
        .recv_timeout(Duration::from_secs(2))
        .expect("commit event for tx_b");
    let cursor = TxId(db.snapshot_at(event_b.lsn).0);
    db.commit(tx_a).unwrap();

    let after_cursor: Vec<Uuid> = db
        .execute(
            "SELECT id FROM audit_log WHERE recorded_at > $cursor",
            &[("cursor".to_string(), Value::TxId(cursor))]
                .into_iter()
                .collect(),
        )
        .unwrap()
        .rows
        .into_iter()
        .filter_map(|row| match &row[0] {
            Value::Uuid(u) => Some(*u),
            _ => None,
        })
        .collect();

    let returned: std::collections::HashSet<Uuid> = after_cursor.into_iter().collect();
    assert_eq!(
        returned,
        std::collections::HashSet::from([row_a]),
        "cursor after tx_b must surface tx_a, the later commit"
    );
}

#[test]
fn t18_03_auto_stamped_txid_order_matches_commit_lsn_order() {
    let (_dir, db) = open_db();
    let db = &db;
    db.execute(
        "CREATE TABLE audit_log (id UUID PRIMARY KEY, recorded_at TXID NOT NULL)",
        &empty(),
    )
    .unwrap();
    let receiver = db.subscribe();

    let txs: Vec<_> = (0..6).map(|_| db.begin()).collect();
    for tx in &txs {
        db.execute_in_tx(
            *tx,
            "INSERT INTO audit_log (id) VALUES ($id)",
            &[("id".to_string(), Value::Uuid(Uuid::new_v4()))]
                .into_iter()
                .collect(),
        )
        .unwrap();
    }
    for tx in txs.iter().rev() {
        db.commit(*tx).unwrap();
    }

    // Drain the subscription stream — six events, one per commit, each with an LSN.
    let mut events = Vec::new();
    for _ in 0..6 {
        events.push(
            receiver
                .recv_timeout(Duration::from_secs(2))
                .expect("commit event"),
        );
    }
    events.sort_by_key(|e| e.lsn.0);

    // Snapshot resolution: snapshot_at(event.lsn) yields a SnapshotId whose
    // raw inner u64 is the canonical TxId for that commit (Tier-1-§21
    // `SnapshotId::from_tx`).
    let lsn_ordered_txids: Vec<TxId> = events
        .iter()
        .map(|e| TxId(db.snapshot_at(e.lsn).0))
        .collect();

    // Auto-stamped TXIDs sorted ascending must equal the LSN-ordered list.
    let mut stamped: Vec<TxId> = db
        .execute("SELECT recorded_at FROM audit_log", &empty())
        .unwrap()
        .rows
        .into_iter()
        .filter_map(|row| match &row[0] {
            Value::TxId(t) => Some(*t),
            _ => None,
        })
        .collect();
    stamped.sort_by_key(|t| t.0);
    assert_eq!(
        stamped, lsn_ordered_txids,
        "auto-stamped TXIDs must sort identically to commit-LSN-ordered TxIds"
    );
}

#[test]
fn t18_04_caller_supplied_txid_above_watermark_rejected() {
    // REGRESSION GUARD: v0.3.4 ceiling check stays.
    let (_dir, db) = open_db();
    let db = &db;
    db.execute(
        "CREATE TABLE audit_log (id UUID PRIMARY KEY, recorded_at TXID NOT NULL)",
        &empty(),
    )
    .unwrap();
    let max = db.committed_watermark().0;
    let bad = TxId(max + 100);
    let err = db
        .execute(
            "INSERT INTO audit_log (id, recorded_at) VALUES ($id, $recorded_at)",
            &[
                ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                ("recorded_at".to_string(), Value::TxId(bad)),
            ]
            .into_iter()
            .collect(),
        )
        .unwrap_err();
    assert!(
        matches!(
            err,
            Error::TxIdOutOfRange {
                ref table,
                ref column,
                value,
                max: reported_max,
            } if table == "audit_log"
                && column == "recorded_at"
                && value == bad.0
                && reported_max == max
        ),
        "TXID ceiling violation must identify audit_log.recorded_at value {bad:?} max {max}, got {err:?}"
    );
}

#[test]
fn t18_05_cron_callback_stamped_txid_matches_commit_event_lsn_resolution() {
    use std::sync::Mutex;
    let (_dir, db) = open_db();
    let db = &db;
    let _pause = db.pause_cron_tickler_for_test();
    db.execute(
        "CREATE TABLE cron_audit (id UUID PRIMARY KEY, recorded_at TXID NOT NULL)",
        &empty(),
    )
    .unwrap();
    let observed_lsns = Arc::new(Mutex::new(Vec::new()));
    {
        let observed = Arc::clone(&observed_lsns);
        db.register_cron_callback("tier2_cron_audit_writer", move |db: &Database| {
            db.execute(
                "INSERT INTO cron_audit (id) VALUES ($id)",
                &[("id".to_string(), Value::Uuid(Uuid::new_v4()))]
                    .into_iter()
                    .collect(),
            )?;
            observed.lock().unwrap().push(db.current_lsn());
            Ok(())
        })
        .unwrap();
    }
    db.execute(
        "CREATE SCHEDULE tier2_cron_audit_writer EVERY '500 MILLISECONDS' TX (tier2_cron_audit_writer)",
        &empty(),
    )
    .unwrap();
    let receiver = db.subscribe();
    std::thread::sleep(Duration::from_millis(600));
    let fires = db.cron_run_due_now_for_test().unwrap();
    assert_eq!(fires, 1);
    let event = receiver
        .recv_timeout(Duration::from_secs(2))
        .expect("cron commit event");
    assert!(
        event.tables_changed.contains(&"cron_audit".to_string()),
        "cron commit event must include cron_audit, got {:?}",
        event.tables_changed
    );

    let stamped: Vec<TxId> = db
        .execute("SELECT recorded_at FROM cron_audit", &empty())
        .unwrap()
        .rows
        .into_iter()
        .filter_map(|row| match &row[0] {
            Value::TxId(t) => Some(*t),
            _ => None,
        })
        .collect();
    let observed = observed_lsns.lock().unwrap();
    assert_eq!(observed.len(), 1, "callback must run exactly once");
    assert_eq!(stamped.len(), 1, "cron callback must write exactly one row");
    assert_eq!(
        observed[0], event.lsn,
        "callback's captured LSN must equal the commit event LSN"
    );
    assert_eq!(TxId(db.snapshot_at(event.lsn).0), stamped[0]);
}

#[test]
fn t18_06_commit_time_upsert_rewrites_txid_post_image_to_canonical() {
    // REGRESSION GUARD. A deferred UPSERT may evaluate DO UPDATE expressions
    // after the initial active-TxId rewrite. TXID values introduced by that
    // rewrite must still be canonicalized to the commit-order TxId.
    let (_dir, db) = open_db();
    let db = &db;
    db.execute(
        "CREATE TABLE audit_log (id UUID PRIMARY KEY, natural_key TEXT NOT NULL UNIQUE, recorded_at TXID NOT NULL)",
        &empty(),
    )
    .unwrap();
    let receiver = db.subscribe();

    let deferred_update_tx = db.begin();
    let first_commit_tx = db.begin();
    for tx in [deferred_update_tx, first_commit_tx] {
        db.execute_in_tx(
            tx,
            "INSERT INTO audit_log (id, natural_key, recorded_at) VALUES ($id, 'same-key', $recorded_at) ON CONFLICT (natural_key) DO UPDATE SET recorded_at = $recorded_at",
            &[
                ("id".to_string(), Value::Uuid(Uuid::new_v4())),
                ("recorded_at".to_string(), Value::TxId(tx)),
            ]
            .into_iter()
            .collect(),
        )
        .unwrap();
    }

    db.commit(first_commit_tx).unwrap();
    let _first_event = receiver
        .recv_timeout(Duration::from_secs(2))
        .expect("first commit event");
    db.commit(deferred_update_tx).unwrap();
    let update_event = receiver
        .recv_timeout(Duration::from_secs(2))
        .expect("deferred UPSERT commit event");
    let expected = TxId(db.snapshot_at(update_event.lsn).0);

    let rows = db
        .execute("SELECT recorded_at FROM audit_log", &empty())
        .unwrap()
        .rows;
    assert_eq!(rows, vec![vec![Value::TxId(expected)]]);
    assert_ne!(
        expected, deferred_update_tx,
        "test setup must begin and commit in different orders"
    );
}
