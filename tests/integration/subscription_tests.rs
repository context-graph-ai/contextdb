use contextdb_core::Lsn;
use contextdb_core::Value;
use contextdb_engine::Database;
use contextdb_engine::sync_types::{
    ChangeSet, ConflictPolicies, ConflictPolicy, NaturalKey, RowChange,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

#[test]
fn s01_subscribe_fires_on_insert() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, name TEXT)",
        &HashMap::new(),
    )
    .unwrap();

    let rx = db.subscribe();

    db.execute(
        "INSERT INTO t (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'alice')",
        &HashMap::new(),
    )
    .unwrap();

    let event = rx
        .recv_timeout(Duration::from_secs(2))
        .expect("should receive CommitEvent after insert");
    assert!(event.lsn > Lsn(0), "LSN must be positive");
    assert!(event.row_count > 0, "row_count must be positive");
    assert_eq!(
        event.source,
        contextdb_engine::plugin::CommitSource::AutoCommit,
        "single INSERT must report AutoCommit source"
    );
    assert!(
        event.tables_changed.contains(&"t".to_string()),
        "tables_changed must include 't'"
    );
}

#[test]
fn s02_autocommit_source_is_autocommit() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, name TEXT)",
        &HashMap::new(),
    )
    .unwrap();

    let rx = db.subscribe();

    // Autocommit INSERT (no BEGIN/COMMIT wrapper)
    db.execute(
        "INSERT INTO t (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'bob')",
        &HashMap::new(),
    )
    .unwrap();

    let event = rx
        .recv_timeout(Duration::from_secs(2))
        .expect("should receive event");
    assert_eq!(
        event.source,
        contextdb_engine::plugin::CommitSource::AutoCommit,
        "autocommit INSERT must report AutoCommit source"
    );
}

#[test]
fn s03_explicit_commit_source_is_user() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, name TEXT)",
        &HashMap::new(),
    )
    .unwrap();

    let rx = db.subscribe();

    db.execute("BEGIN", &HashMap::new()).unwrap();
    db.execute(
        "INSERT INTO t (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'carol')",
        &HashMap::new(),
    )
    .unwrap();
    db.execute("COMMIT", &HashMap::new()).unwrap();

    let event = rx
        .recv_timeout(Duration::from_secs(2))
        .expect("should receive event for explicit commit");
    assert_eq!(
        event.source,
        contextdb_engine::plugin::CommitSource::User,
        "explicit BEGIN/COMMIT must report User source"
    );
}

#[test]
fn s04_apply_changes_source_is_sync_pull() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, name TEXT)",
        &HashMap::new(),
    )
    .unwrap();

    let rx = db.subscribe();

    let row_id = Uuid::new_v4();
    let changes = ChangeSet {
        rows: vec![RowChange {
            table: "t".to_string(),
            natural_key: NaturalKey {
                column: "id".to_string(),
                value: Value::Uuid(row_id),
            },
            values: HashMap::from([
                ("id".to_string(), Value::Uuid(row_id)),
                ("name".to_string(), Value::Text("synced".to_string())),
            ]),
            deleted: false,
            lsn: Lsn(1),
        }],
        edges: vec![],
        vectors: vec![],
        ddl: vec![],

        ddl_lsn: Vec::new(),
    };

    let policies = ConflictPolicies::uniform(ConflictPolicy::ServerWins);
    db.apply_changes(changes, &policies).unwrap();

    let event = rx
        .recv_timeout(Duration::from_secs(2))
        .expect("should receive event for apply_changes");
    assert_eq!(
        event.source,
        contextdb_engine::plugin::CommitSource::SyncPull,
        "apply_changes must report SyncPull source"
    );
}

#[test]
fn s05_tables_changed_lists_affected_tables() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t1 (id UUID PRIMARY KEY, name TEXT)",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE t2 (id UUID PRIMARY KEY, label TEXT)",
        &HashMap::new(),
    )
    .unwrap();

    let rx = db.subscribe();

    db.execute("BEGIN", &HashMap::new()).unwrap();
    db.execute(
        "INSERT INTO t1 (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'a')",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(
        "INSERT INTO t2 (id, label) VALUES ('00000000-0000-0000-0000-000000000002', 'b')",
        &HashMap::new(),
    )
    .unwrap();
    db.execute("COMMIT", &HashMap::new()).unwrap();

    let event = rx
        .recv_timeout(Duration::from_secs(2))
        .expect("should receive event");

    let mut tables = event.tables_changed.clone();
    tables.sort();
    assert_eq!(tables, vec!["t1".to_string(), "t2".to_string()]);
}

#[test]
fn s06_graph_only_commit_empty_tables_changed() {
    let db = Database::open_memory();
    let rx = db.subscribe();

    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    let tx = db.begin();
    db.insert_edge(tx, a, b, "RELATES_TO".to_string(), HashMap::new())
        .unwrap();
    db.commit(tx).unwrap();

    let event = rx
        .recv_timeout(Duration::from_secs(2))
        .expect("should receive event for graph-only commit");
    assert!(
        event.tables_changed.is_empty(),
        "graph-only commit must have empty tables_changed, got: {:?}",
        event.tables_changed
    );
    assert!(event.row_count > 0, "row_count should count graph edges");
    assert!(
        event.lsn > Lsn(0),
        "LSN must be positive for graph-only commit"
    );
    assert_eq!(
        event.source,
        contextdb_engine::plugin::CommitSource::User,
        "explicit begin/commit must report User source"
    );
}

#[test]
fn s07_row_count_matches_inserted_rows() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, name TEXT)",
        &HashMap::new(),
    )
    .unwrap();

    let rx = db.subscribe();

    db.execute("BEGIN", &HashMap::new()).unwrap();
    for i in 1..=5u128 {
        db.execute(
            &format!(
                "INSERT INTO t (id, name) VALUES ('00000000-0000-0000-0000-{:012}', 'row{}')",
                i, i
            ),
            &HashMap::new(),
        )
        .unwrap();
    }
    db.execute("COMMIT", &HashMap::new()).unwrap();

    let event = rx
        .recv_timeout(Duration::from_secs(2))
        .expect("should receive event");
    assert_eq!(event.row_count, 5, "row_count must match 5 inserted rows");
}

#[test]
fn s08_fanout_multiple_subscribers() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, name TEXT)",
        &HashMap::new(),
    )
    .unwrap();

    let rx1 = db.subscribe();
    let rx2 = db.subscribe();

    db.execute(
        "INSERT INTO t (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'multi')",
        &HashMap::new(),
    )
    .unwrap();

    let event1 = rx1
        .recv_timeout(Duration::from_secs(2))
        .expect("subscriber 1 should receive event");
    let event2 = rx2
        .recv_timeout(Duration::from_secs(2))
        .expect("subscriber 2 should receive event");

    assert_eq!(
        event1, event2,
        "both subscribers must receive identical CommitEvent"
    );
}

#[test]
fn s09_dead_channel_cleanup_no_panic() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, name TEXT)",
        &HashMap::new(),
    )
    .unwrap();

    let rx = db.subscribe();
    drop(rx); // receiver dropped before commit

    // This must not panic or return error
    db.execute(
        "INSERT INTO t (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'ghost')",
        &HashMap::new(),
    )
    .unwrap();

    // Subscribe again and verify the database still works
    let rx2 = db.subscribe();
    db.execute(
        "INSERT INTO t (id, name) VALUES ('00000000-0000-0000-0000-000000000002', 'alive')",
        &HashMap::new(),
    )
    .unwrap();

    let event = rx2
        .recv_timeout(Duration::from_secs(2))
        .expect("new subscriber should receive events after dead channel cleanup");
    assert!(event.row_count > 0);
}

#[test]
fn s10_bounded_channel_does_not_block_commits() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, name TEXT)",
        &HashMap::new(),
    )
    .unwrap();

    // Default capacity is 64. We subscribe but never drain.
    let _rx = db.subscribe();

    // Insert 100 rows (each autocommit = 1 event). Must not hang.
    for i in 1..=100u128 {
        db.execute(
            &format!(
                "INSERT INTO t (id, name) VALUES ('00000000-0000-0000-0000-{:012}', 'row')",
                i
            ),
            &HashMap::new(),
        )
        .unwrap();
    }

    // If we got here, commits did not block. Verify at least some data exists.
    let result = db
        .execute("SELECT COUNT(*) FROM t", &HashMap::new())
        .unwrap();
    let count = &result.rows[0][0];
    match count {
        Value::Int64(n) => assert_eq!(*n, 100),
        _ => panic!("expected integer count"),
    }
}

#[test]
fn s11_subscription_health_reports_active_channels() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, name TEXT)",
        &HashMap::new(),
    )
    .unwrap();

    let health_before = db.subscription_health();
    assert_eq!(health_before.active_channels, 0);

    let _rx1 = db.subscribe();
    let _rx2 = db.subscribe();

    let health_after = db.subscription_health();
    assert_eq!(
        health_after.active_channels, 2,
        "two active subscribers must be reported"
    );

    // Trigger an event
    db.execute(
        "INSERT INTO t (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'x')",
        &HashMap::new(),
    )
    .unwrap();

    let health_sent = db.subscription_health();
    assert!(
        health_sent.events_sent > 0,
        "events_sent must be positive after a commit"
    );
}

#[test]
fn s12_dead_channel_reflected_in_metrics() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, name TEXT)",
        &HashMap::new(),
    )
    .unwrap();

    let rx1 = db.subscribe();
    let _rx2 = db.subscribe();

    // Drop one subscriber
    drop(rx1);

    // Trigger a commit to force lazy cleanup
    db.execute(
        "INSERT INTO t (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'x')",
        &HashMap::new(),
    )
    .unwrap();

    let health = db.subscription_health();
    assert_eq!(
        health.active_channels, 1,
        "after dropping one of two subscribers, active_channels must be 1"
    );
}

#[test]
fn s13_subscribe_with_capacity_honors_limit() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, name TEXT)",
        &HashMap::new(),
    )
    .unwrap();

    // Tiny capacity: 2
    let rx = db.subscribe_with_capacity(2);

    // Send 5 events without draining
    for i in 1..=5u128 {
        db.execute(
            &format!(
                "INSERT INTO t (id, name) VALUES ('00000000-0000-0000-0000-{:012}', 'x')",
                i
            ),
            &HashMap::new(),
        )
        .unwrap();
    }

    // Drain what's available. At most 2 should be in the channel.
    let mut received = 0;
    while rx.try_recv().is_ok() {
        received += 1;
    }

    // With capacity 2, at most 2 events should be buffered (3 dropped).
    assert!(
        received <= 2,
        "with capacity 2, at most 2 events should be buffered, got {}",
        received
    );
    assert!(
        received > 0,
        "at least 1 event must be delivered to prove the channel works"
    );

    let health = db.subscription_health();
    assert!(
        health.events_dropped >= 3,
        "at least 3 events must be dropped with capacity 2 and 5 commits"
    );
}

#[test]
fn s14_shutdown_disconnects_subscribers() {
    let rx = {
        let db = Database::open_memory();
        db.execute(
            "CREATE TABLE t (id UUID PRIMARY KEY, name TEXT)",
            &HashMap::new(),
        )
        .unwrap();
        let rx = db.subscribe();

        // Insert to verify subscriber is alive
        db.execute(
            "INSERT INTO t (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'pre')",
            &HashMap::new(),
        )
        .unwrap();

        // Drain the event to confirm it works
        let _ = rx.recv_timeout(Duration::from_secs(2));

        rx
        // db dropped here
    };

    // After Database drop, recv must return error (disconnected)
    let result = rx.recv_timeout(Duration::from_secs(1));
    assert!(
        result.is_err(),
        "after Database drop, subscriber must receive disconnect"
    );
}

#[test]
fn s15_commit_event_lsn_matches_current_lsn() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, name TEXT)",
        &HashMap::new(),
    )
    .unwrap();

    let rx = db.subscribe();

    db.execute(
        "INSERT INTO t (id, name) VALUES ('00000000-0000-0000-0000-000000000001', 'lsn-test')",
        &HashMap::new(),
    )
    .unwrap();

    let event = rx
        .recv_timeout(Duration::from_secs(2))
        .expect("should receive event");
    let db_lsn = db.current_lsn();

    assert_eq!(
        event.lsn, db_lsn,
        "CommitEvent LSN must match database current_lsn"
    );
}

#[test]
fn s16_row_count_includes_relational_and_graph() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, name TEXT)",
        &HashMap::new(),
    )
    .unwrap();

    let rx = db.subscribe();

    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    let tx = db.begin();

    // 2 relational inserts
    db.insert_row(
        tx,
        "t",
        HashMap::from([
            ("id".to_string(), Value::Uuid(a)),
            ("name".to_string(), Value::Text("a".to_string())),
        ]),
    )
    .unwrap();
    db.insert_row(
        tx,
        "t",
        HashMap::from([
            ("id".to_string(), Value::Uuid(b)),
            ("name".to_string(), Value::Text("b".to_string())),
        ]),
    )
    .unwrap();

    // 1 graph edge
    db.insert_edge(tx, a, b, "LINKS".to_string(), HashMap::new())
        .unwrap();

    db.commit(tx).unwrap();

    let event = rx
        .recv_timeout(Duration::from_secs(2))
        .expect("should receive event");
    assert!(
        event.row_count >= 3,
        "row_count must include 2 relational + 1 graph = at least 3, got {}",
        event.row_count
    );
}

#[test]
fn s17_events_dropped_counted_in_metrics() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, name TEXT)",
        &HashMap::new(),
    )
    .unwrap();

    // Subscribe with tiny capacity to force drops
    let _rx = db.subscribe_with_capacity(2);

    // Generate 10 events without draining
    for i in 1..=10u128 {
        db.execute(
            &format!(
                "INSERT INTO t (id, name) VALUES ('00000000-0000-0000-0000-{:012}', 'x')",
                i
            ),
            &HashMap::new(),
        )
        .unwrap();
    }

    let health = db.subscription_health();
    assert!(
        health.events_dropped >= 8,
        "with capacity 2 and 10 commits, at least 8 events should be dropped, got {}",
        health.events_dropped
    );
}

#[test]
fn s18_concurrent_commits_with_subscriber() {
    use std::thread;

    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, name TEXT)",
        &HashMap::new(),
    )
    .unwrap();

    let db = Arc::new(db);
    let rx = db.subscribe();

    // Spawn 2 threads, each inserting 10 rows
    let db1 = Arc::clone(&db);
    let db2 = Arc::clone(&db);

    let h1 = thread::spawn(move || {
        for i in 0..10u128 {
            db1.execute(
                &format!(
                    "INSERT INTO t (id, name) VALUES ('00000000-0000-0000-0001-{:012}', 'a{}')",
                    i, i
                ),
                &HashMap::new(),
            )
            .unwrap();
        }
    });

    let h2 = thread::spawn(move || {
        for i in 0..10u128 {
            db2.execute(
                &format!(
                    "INSERT INTO t (id, name) VALUES ('00000000-0000-0000-0002-{:012}', 'b{}')",
                    i, i
                ),
                &HashMap::new(),
            )
            .unwrap();
        }
    });

    h1.join().unwrap();
    h2.join().unwrap();

    // Drain all events from subscriber
    let mut events = Vec::new();
    while let Ok(event) = rx.recv_timeout(Duration::from_secs(2)) {
        events.push(event);
    }

    // Must have received at least 1 event (20 autocommits may coalesce)
    assert!(
        !events.is_empty(),
        "subscriber must receive events from concurrent commits"
    );

    // Total row_count across all events must equal 20
    let total_rows: usize = events.iter().map(|e| e.row_count).sum();
    assert_eq!(
        total_rows, 20,
        "total row_count across events must equal 20 inserts"
    );

    // Every event must have positive LSN
    for event in &events {
        assert!(event.lsn > Lsn(0), "every event must have positive LSN");
    }
}

#[test]
fn s19_ddl_only_commit_create_table() {
    let db = Database::open_memory();
    let rx = db.subscribe();

    db.execute(
        "CREATE TABLE new_table (id UUID PRIMARY KEY, val TEXT)",
        &HashMap::new(),
    )
    .unwrap();

    // DDL-only commits may or may not fire CommitEvent. This test documents the behavior.
    // If the implementation fires events for DDL, the event must have lsn > 0 and row_count == 0.
    // If not, recv_timeout returns Err — which we also accept.
    match rx.recv_timeout(Duration::from_secs(2)) {
        Ok(event) => {
            assert!(event.lsn > Lsn(0), "DDL event must have positive LSN");
            assert_eq!(event.row_count, 0, "DDL-only commit has no row changes");
        }
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            // DDL does not fire CommitEvent — acceptable behavior, documented here
        }
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            panic!("channel disconnected — subscribe() stub not wired");
        }
    }
}

#[test]
fn s20_auto_sync_fires_commit_event() {
    // This test covers the `.sync auto` path from the intent.
    // It requires a running NATS server. When auto-sync pulls changes from a
    // remote, the local apply must fire a CommitEvent with source == SyncPull.
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE t (id UUID PRIMARY KEY, name TEXT)",
        &HashMap::new(),
    )
    .unwrap();

    let rx = db.subscribe();

    // Trigger an auto-sync pull (implementation will connect to NATS and pull changes).
    // For now, simulate by calling apply_changes directly.
    let row_id = Uuid::new_v4();
    let changes = ChangeSet {
        rows: vec![RowChange {
            table: "t".to_string(),
            natural_key: NaturalKey {
                column: "id".to_string(),
                value: Value::Uuid(row_id),
            },
            values: HashMap::from([
                ("id".to_string(), Value::Uuid(row_id)),
                ("name".to_string(), Value::Text("auto-synced".to_string())),
            ]),
            deleted: false,
            lsn: Lsn(1),
        }],
        edges: vec![],
        vectors: vec![],
        ddl: vec![],

        ddl_lsn: Vec::new(),
    };
    let policies = ConflictPolicies::uniform(ConflictPolicy::ServerWins);
    db.apply_changes(changes, &policies).unwrap();

    let event = rx
        .recv_timeout(Duration::from_secs(5))
        .expect("auto-sync pull must fire CommitEvent");
    assert_eq!(
        event.source,
        contextdb_engine::plugin::CommitSource::SyncPull,
        "auto-sync must report SyncPull source"
    );
    assert!(event.lsn > Lsn(0));
    assert!(event.row_count > 0);
    assert!(event.tables_changed.contains(&"t".to_string()));
}
