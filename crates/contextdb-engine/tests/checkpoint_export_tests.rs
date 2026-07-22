//! Checkpoint export behavior suite (CE01–CE22).
//!
//! Contract surface: `Database::export_snapshot(dest) -> Result<ExportReport>`.
//! All 22 tests are RED at stub time and fail on assertions (see the
//! `assert_has_tables` gate), never on panics from missing tables.

use contextdb_core::{Direction, Error, Lsn, Value};
use contextdb_engine::{Database, ExportReport, SinkEvent};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tempfile::TempDir;
use uuid::Uuid;

fn p() -> HashMap<String, Value> {
    HashMap::new()
}

fn params(pairs: Vec<(&str, Value)>) -> HashMap<String, Value> {
    pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
}

/// Leading artifact gate. The stub artifact is an empty-but-openable database,
/// so this fails on an assertion (not an unwrap panic) for every
/// artifact-content test at stub time.
fn assert_has_tables(db: &Database, tables: &[&str], what: &str) {
    let names = db.table_names();
    for t in tables {
        assert!(
            names.iter().any(|n| n == t),
            "{what}: table `{t}` missing from artifact; artifact tables: {names:?}"
        );
    }
}

/// Only called after `assert_has_tables` has passed for the queried tables.
fn rows(db: &Database, sql: &str) -> Vec<Vec<Value>> {
    db.execute(sql, &p())
        .unwrap_or_else(|e| panic!("query `{sql}` failed: {e}"))
        .rows
}

fn dir_names(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("read_dir {dir:?} failed: {e}"))
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    names
}

/// The canonical on-disk export fixture: a fresh temp dir holding an opened
/// `src.db` and the `artifact.db` destination path. Tests with divergent
/// layouts (multiple artifacts, re-export, separate temp roots) keep their own
/// bring-up.
fn new_export_fixture() -> (TempDir, Database, PathBuf) {
    let tmp = TempDir::new().unwrap();
    let src = Database::open(tmp.path().join("src.db")).unwrap();
    let dest = tmp.path().join("artifact.db");
    (tmp, src, dest)
}

// ---------------------------------------------------------------------------
// CE01 — relational round-trip with UPDATE + DELETE visibility. RED.
// ---------------------------------------------------------------------------
#[test]
fn ce01_relational_rows_round_trip_with_updates_and_deletes() {
    let (_tmp, src, dest) = new_export_fixture();

    src.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)", &p())
        .unwrap();
    src.execute("CREATE TABLE u (id INTEGER PRIMARY KEY, w TEXT)", &p())
        .unwrap();
    src.execute("INSERT INTO t (id, v) VALUES (1, 'alpha')", &p())
        .unwrap();
    src.execute("INSERT INTO t (id, v) VALUES (2, 'beta')", &p())
        .unwrap();
    src.execute("INSERT INTO t (id, v) VALUES (3, 'gamma')", &p())
        .unwrap();
    src.execute("UPDATE t SET v = 'alpha-updated' WHERE id = 1", &p())
        .unwrap();
    src.execute("DELETE FROM t WHERE id = 2", &p()).unwrap();
    src.execute("INSERT INTO u (id, w) VALUES (10, 'keep')", &p())
        .unwrap();

    src.export_snapshot(&dest).unwrap();

    let artifact = Database::open(&dest).unwrap();
    assert_has_tables(&artifact, &["t", "u"], "CE01");
    assert_eq!(
        rows(&artifact, "SELECT id, v FROM t ORDER BY id"),
        vec![
            vec![Value::Int64(1), Value::Text("alpha-updated".into())],
            vec![Value::Int64(3), Value::Text("gamma".into())],
        ],
        "artifact must hold the exact post-UPDATE/post-DELETE row set"
    );
    assert_eq!(
        rows(&artifact, "SELECT id, w FROM u ORDER BY id"),
        vec![vec![Value::Int64(10), Value::Text("keep".into())]],
        "second table must round-trip exactly"
    );
}

// ---------------------------------------------------------------------------
// CE02 — DDL round-trip: table set, IMMUTABLE, STATE MACHINE. RED.
// ---------------------------------------------------------------------------
#[test]
fn ce02_ddl_round_trip_enforces_schema_constraints_on_artifact() {
    let (_tmp, src, dest) = new_export_fixture();

    src.execute(
        "CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT) IMMUTABLE",
        &p(),
    )
    .unwrap();
    src.execute(
        "CREATE TABLE flows (id UUID PRIMARY KEY, status TEXT) STATE MACHINE (status: pending -> [acknowledged, dismissed], acknowledged -> [resolved])",
        &p(),
    )
    .unwrap();
    let note_id = Uuid::from_u128(0xCE02_0001);
    let flow_id = Uuid::from_u128(0xCE02_0002);
    src.execute(
        "INSERT INTO notes (id, body) VALUES ($id, $b)",
        &params(vec![
            ("id", Value::Uuid(note_id)),
            ("b", Value::Text("pinned".into())),
        ]),
    )
    .unwrap();
    src.execute(
        "INSERT INTO flows (id, status) VALUES ($id, $s)",
        &params(vec![
            ("id", Value::Uuid(flow_id)),
            ("s", Value::Text("pending".into())),
        ]),
    )
    .unwrap();

    src.export_snapshot(&dest).unwrap();
    let artifact = Database::open(&dest).unwrap();
    assert_has_tables(&artifact, &["notes", "flows"], "CE02");

    let src_tables: BTreeSet<String> = src.table_names().into_iter().collect();
    let artifact_tables: BTreeSet<String> = artifact.table_names().into_iter().collect();
    assert_eq!(
        artifact_tables, src_tables,
        "artifact table set must equal the source table set"
    );

    let del = artifact.execute(
        "DELETE FROM notes WHERE id = $id",
        &params(vec![("id", Value::Uuid(note_id))]),
    );
    assert!(
        matches!(del, Err(Error::ImmutableTable(_))),
        "IMMUTABLE must survive export; got {del:?}"
    );

    artifact
        .execute(
            "INSERT INTO flows (id, status) VALUES ($id, $s) ON CONFLICT (id) DO UPDATE SET status=$s",
            &params(vec![
                ("id", Value::Uuid(flow_id)),
                ("s", Value::Text("acknowledged".into())),
            ]),
        )
        .unwrap_or_else(|e| panic!("legal state transition must work on the artifact: {e}"));
    let illegal = artifact.execute(
        "INSERT INTO flows (id, status) VALUES ($id, $s) ON CONFLICT (id) DO UPDATE SET status=$s",
        &params(vec![
            ("id", Value::Uuid(flow_id)),
            ("s", Value::Text("pending".into())),
        ]),
    );
    assert!(
        matches!(illegal, Err(Error::InvalidStateTransition(_))),
        "STATE MACHINE must survive export; got {illegal:?}"
    );
}

// ---------------------------------------------------------------------------
// CE03 — graph edges round-trip: traversals identical. RED.
// ---------------------------------------------------------------------------
#[test]
fn ce03_graph_edges_round_trip_traversals_equal() {
    let (_tmp, src, dest) = new_export_fixture();

    let a = Uuid::from_u128(0xCE03_000A);
    let b = Uuid::from_u128(0xCE03_000B);
    let c = Uuid::from_u128(0xCE03_000C);
    let d = Uuid::from_u128(0xCE03_000D);
    let tx = src.begin_or_panic();
    src.insert_edge(tx, a, b, "CITES".into(), HashMap::new())
        .unwrap();
    src.insert_edge(tx, b, c, "CITES".into(), HashMap::new())
        .unwrap();
    src.insert_edge(tx, a, d, "SERVES".into(), HashMap::new())
        .unwrap();
    src.commit(tx).unwrap();

    src.export_snapshot(&dest).unwrap();
    let artifact = Database::open(&dest).unwrap();

    let bfs_ids = |db: &Database, start: Uuid, dir: Direction| -> BTreeSet<Uuid> {
        db.query_bfs(start, None, dir, 3, db.snapshot())
            .unwrap_or_else(|e| panic!("query_bfs failed: {e}"))
            .nodes
            .iter()
            .map(|n| n.id)
            .collect()
    };

    assert_eq!(
        bfs_ids(&artifact, a, Direction::Outgoing),
        BTreeSet::from([b, c, d]),
        "artifact outgoing traversal from a must equal the source graph"
    );
    assert_eq!(
        bfs_ids(&artifact, a, Direction::Outgoing),
        bfs_ids(&src, a, Direction::Outgoing),
        "artifact and source traversals must be identical"
    );
    assert_eq!(
        bfs_ids(&artifact, c, Direction::Incoming),
        BTreeSet::from([a, b]),
        "incoming traversal must survive export"
    );
}

// ---------------------------------------------------------------------------
// CE04 — vectors round-trip: search results identical, ids + order. RED.
// ---------------------------------------------------------------------------
#[test]
fn ce04_vectors_round_trip_search_results_equal() {
    let (_tmp, src, dest) = new_export_fixture();

    src.execute(
        "CREATE TABLE docs (id UUID PRIMARY KEY, body TEXT, embedding VECTOR(3))",
        &p(),
    )
    .unwrap();
    let id1 = Uuid::from_u128(0xCE04_0001); // closest to the query
    let id2 = Uuid::from_u128(0xCE04_0002); // second
    let id3 = Uuid::from_u128(0xCE04_0003); // farthest
    for (id, emb) in [
        (id1, vec![1.0_f32, 0.0, 0.0]),
        (id2, vec![0.8_f32, 0.2, 0.0]),
        (id3, vec![0.0_f32, 0.0, 1.0]),
    ] {
        src.execute(
            "INSERT INTO docs (id, body, embedding) VALUES ($id, 'd', $e)",
            &params(vec![("id", Value::Uuid(id)), ("e", Value::Vector(emb))]),
        )
        .unwrap();
    }

    src.export_snapshot(&dest).unwrap();
    let artifact = Database::open(&dest).unwrap();
    assert_has_tables(&artifact, &["docs"], "CE04");

    let search = |db: &Database| -> Vec<Value> {
        db.execute(
            "SELECT id FROM docs ORDER BY embedding <=> $q LIMIT 3",
            &params(vec![("q", Value::Vector(vec![1.0_f32, 0.0, 0.0]))]),
        )
        .unwrap_or_else(|e| panic!("vector search failed: {e}"))
        .rows
        .into_iter()
        .map(|mut r| r.remove(0))
        .collect()
    };
    assert_eq!(
        search(&artifact),
        vec![Value::Uuid(id1), Value::Uuid(id2), Value::Uuid(id3)],
        "artifact vector search must return the exact ids in the exact order"
    );
    assert_eq!(
        search(&artifact),
        search(&src),
        "artifact and source search results must be identical"
    );

    let show = |db: &Database| -> BTreeSet<String> {
        db.execute("SHOW VECTOR_INDEXES", &p())
            .unwrap_or_else(|e| panic!("SHOW VECTOR_INDEXES failed: {e}"))
            .rows
            .iter()
            .map(|r| format!("{r:?}"))
            .collect()
    };
    assert_eq!(
        show(&artifact),
        show(&src),
        "vector index catalog must survive export"
    );
}

// ---------------------------------------------------------------------------
// CE05 — per-tenant sync push/pull watermarks round-trip. RED.
// ---------------------------------------------------------------------------
#[test]
fn ce05_sync_watermarks_round_trip_per_tenant() {
    let (_tmp, src, dest) = new_export_fixture();

    src.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)", &p())
        .unwrap();
    src.persist_sync_push_watermark(&contextdb_core::TenantId::from("tenant-a"), Lsn(7))
        .unwrap();
    src.persist_sync_pull_watermark(&contextdb_core::TenantId::from("tenant-a"), Lsn(4))
        .unwrap();
    src.persist_sync_push_watermark(&contextdb_core::TenantId::from("tenant-b"), Lsn(9))
        .unwrap();
    src.persist_sync_pull_watermark(&contextdb_core::TenantId::from("tenant-b"), Lsn(2))
        .unwrap();

    src.export_snapshot(&dest).unwrap();
    let artifact = Database::open(&dest).unwrap();

    assert_eq!(
        artifact
            .persisted_sync_watermarks(&contextdb_core::TenantId::from("tenant-a"))
            .unwrap(),
        (Lsn(7), Lsn(4)),
        "tenant-a push/pull watermarks must survive export"
    );
    assert_eq!(
        artifact
            .persisted_sync_watermarks(&contextdb_core::TenantId::from("tenant-b"))
            .unwrap(),
        (Lsn(9), Lsn(2)),
        "tenant-b push/pull watermarks must survive export"
    );
}

// ---------------------------------------------------------------------------
// CE06 — persisted config (DISK_LIMIT) round-trip. RED.
// ---------------------------------------------------------------------------
#[test]
fn ce06_persisted_config_round_trip_disk_limit() {
    let (_tmp, src, dest) = new_export_fixture();

    src.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)", &p())
        .unwrap();
    src.execute("SET DISK_LIMIT '1048576K'", &p()).unwrap();
    let expected_bytes = Value::Int64(1024 * 1024 * 1024);
    let before = src.execute("SHOW DISK_LIMIT", &p()).unwrap();
    assert_eq!(
        before.rows[0][0], expected_bytes,
        "precondition: source SHOW DISK_LIMIT must reflect the set limit at the `limit` cell: {:?}",
        before.rows
    );

    src.export_snapshot(&dest).unwrap();
    let artifact = Database::open(&dest).unwrap();
    let after = artifact.execute("SHOW DISK_LIMIT", &p()).unwrap();
    assert_eq!(
        after.columns,
        vec!["limit", "used", "available", "startup_ceiling"],
        "SHOW DISK_LIMIT column shape on the artifact"
    );
    assert_eq!(
        after.rows.len(),
        1,
        "SHOW DISK_LIMIT returns exactly one row"
    );
    assert_eq!(
        after.rows[0][0], expected_bytes,
        "persisted DISK_LIMIT must survive export at the exact `limit` cell; artifact row: {:?}",
        after.rows
    );
}

// ---------------------------------------------------------------------------
// CE07 — cron schedule round-trip: fires on the artifact. RED.
// ---------------------------------------------------------------------------
#[test]
fn ce07_cron_schedule_round_trip_fires_on_artifact() {
    let (_tmp, src, dest) = new_export_fixture();

    src.execute("CREATE TABLE cron_log (id UUID PRIMARY KEY)", &p())
        .unwrap();
    src.execute(
        "CREATE SCHEDULE heartbeat EVERY '200 MILLISECONDS' TX (heartbeat_cb)",
        &p(),
    )
    .unwrap();
    src.register_cron_callback("heartbeat_cb", |handle| {
        let mut params = HashMap::new();
        params.insert("id".to_string(), Value::Uuid(Uuid::new_v4()));
        handle.execute("INSERT INTO cron_log (id) VALUES ($id)", &params)?;
        Ok(())
    })
    .unwrap();

    src.export_snapshot(&dest).unwrap();

    let artifact = Database::open(&dest).unwrap();
    assert_has_tables(&artifact, &["cron_log"], "CE07");
    // Rows exported from source fires are the baseline; the schedule must
    // produce NEW rows on the artifact, so exported rows alone cannot pass.
    let baseline = artifact
        .scan("cron_log", artifact.snapshot())
        .map(|r| r.len())
        .unwrap_or(0);
    artifact
        .register_cron_callback("heartbeat_cb", |handle| {
            let mut params = HashMap::new();
            params.insert("id".to_string(), Value::Uuid(Uuid::new_v4()));
            handle.execute("INSERT INTO cron_log (id) VALUES ($id)", &params)?;
            Ok(())
        })
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(5);
    let mut count = baseline;
    while Instant::now() < deadline {
        count = artifact
            .scan("cron_log", artifact.snapshot())
            .map(|r| r.len())
            .unwrap_or(0);
        if count > baseline {
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(
        count > baseline,
        "exported CREATE SCHEDULE must fire on the reopened artifact; baseline {baseline}, count {count}"
    );
}

// ---------------------------------------------------------------------------
// CE08 — trigger declaration round-trip: registers and fires on artifact. RED.
// ---------------------------------------------------------------------------
#[test]
fn ce08_trigger_declaration_round_trip_fires_on_artifact() {
    let (_tmp, src, dest) = new_export_fixture();

    src.execute("CREATE TABLE obs (id UUID PRIMARY KEY, kind TEXT)", &p())
        .unwrap();
    src.execute("CREATE TABLE audit (id UUID PRIMARY KEY, kind TEXT)", &p())
        .unwrap();
    src.execute("CREATE TRIGGER obs_tr ON obs WHEN INSERT", &p())
        .unwrap();

    src.export_snapshot(&dest).unwrap();
    let artifact = Database::open(&dest).unwrap();
    assert_has_tables(&artifact, &["obs", "audit"], "CE08");

    let reg = artifact.register_trigger_callback("obs_tr", |handle, ctx| {
        let id = ctx.row_values.get("id").cloned().unwrap_or(Value::Null);
        let kind = ctx.row_values.get("kind").cloned().unwrap_or(Value::Null);
        handle.insert_row(
            ctx.tx,
            "audit",
            HashMap::from([("id".to_string(), id), ("kind".to_string(), kind)]),
        )?;
        Ok(())
    });
    assert!(
        reg.is_ok(),
        "trigger declaration must survive export; registration failed: {reg:?}"
    );

    let row_id = Uuid::from_u128(0xCE08_0001);
    artifact
        .execute(
            "INSERT INTO obs (id, kind) VALUES ($id, 'measurement')",
            &params(vec![("id", Value::Uuid(row_id))]),
        )
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(5);
    let mut audit_rows = Vec::new();
    while Instant::now() < deadline {
        audit_rows = rows(&artifact, "SELECT id, kind FROM audit");
        if !audit_rows.is_empty() {
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    assert_eq!(
        audit_rows,
        vec![vec![Value::Uuid(row_id), Value::Text("measurement".into())]],
        "exported trigger declaration must fire on artifact inserts"
    );
}

// ---------------------------------------------------------------------------
// CE09 — event-bus definitions + durable sink queue round-trip. RED.
// ---------------------------------------------------------------------------
#[test]
fn ce09_event_bus_definitions_and_durable_queue_round_trip() {
    let (_tmp, src, dest) = new_export_fixture();

    src.execute(
        "CREATE TABLE invalidations (id UUID PRIMARY KEY, severity TEXT, reason TEXT)",
        &p(),
    )
    .unwrap();
    src.execute(
        "CREATE EVENT TYPE inv_match WHEN INSERT ON invalidations",
        &p(),
    )
    .unwrap();
    src.execute("CREATE SINK slack TYPE callback", &p())
        .unwrap();
    src.execute("CREATE ROUTE inv_to_slack EVENT inv_match TO slack", &p())
        .unwrap();
    // No callback registered on the source: the insert event is queued
    // durably for `slack`. The row is then deleted, so the only way the
    // artifact can deliver it is from the exported durable queue.
    src.execute(
        "INSERT INTO invalidations (id, severity, reason) VALUES ('00000000-0000-0000-0000-0000000000c9', 'warning', 'queue-me')",
        &p(),
    )
    .unwrap();
    src.execute(
        "DELETE FROM invalidations WHERE id = '00000000-0000-0000-0000-0000000000c9'",
        &p(),
    )
    .unwrap();

    src.export_snapshot(&dest).unwrap();
    let artifact = Database::open(&dest).unwrap();
    assert_has_tables(&artifact, &["invalidations"], "CE09");
    assert_eq!(
        rows(&artifact, "SELECT id FROM invalidations").len(),
        0,
        "source row was deleted before export; replay must come from the durable queue, not a table scan"
    );

    let received: Arc<Mutex<Vec<SinkEvent>>> = Arc::new(Mutex::new(Vec::new()));
    let received_cb = received.clone();
    let reg = artifact.register_sink("slack", None, move |event| {
        received_cb.lock().unwrap().push(event.clone());
        Ok(())
    });
    assert!(
        reg.is_ok(),
        "sink definition must survive export; registration failed: {reg:?}"
    );

    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && received.lock().unwrap().is_empty() {
        std::thread::sleep(Duration::from_millis(50));
    }
    {
        let events = received.lock().unwrap();
        assert_eq!(
            events.len(),
            1,
            "queued event must survive export and deliver exactly once"
        );
        assert_eq!(events[0].event_type, "inv_match");
        assert_eq!(events[0].table, "invalidations");
        assert_eq!(
            events[0].row_values.get("reason"),
            Some(&Value::Text("queue-me".into())),
            "durable queue must preserve the row payload"
        );
    }

    // Event-type + route definitions must stay live for new artifact writes.
    artifact
        .execute(
            "INSERT INTO invalidations (id, severity, reason) VALUES ('00000000-0000-0000-0000-0000000000ca', 'warning', 'post-export')",
            &p(),
        )
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && received.lock().unwrap().len() < 2 {
        std::thread::sleep(Duration::from_millis(50));
    }
    let events = received.lock().unwrap();
    assert_eq!(
        events.len(),
        2,
        "event-bus definitions must stay live on the artifact"
    );
    assert_eq!(
        events[1].row_values.get("reason"),
        Some(&Value::Text("post-export".into()))
    );
}

// ---------------------------------------------------------------------------
// CE10 — RETAIN metadata round-trip: artifact prunes on its own clock. RED.
// ---------------------------------------------------------------------------
#[test]
fn ce10_retention_metadata_round_trip_artifact_prunes_after_ttl() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

    use contextdb_core::Wallclock;

    // Mock clock (thread-local): insert stamp, export, and the artifact's
    // pruning read all happen on this thread, so advancing the mock ages the
    // row past its TTL with no real sleep.
    let mock_now = Arc::new(AtomicU64::new(1_000_000));
    let _clock = {
        let mock_now = Arc::clone(&mock_now);
        Wallclock::test_clock_guard(move || mock_now.load(AtomicOrdering::SeqCst))
    };

    let (_tmp, src, dest) = new_export_fixture();

    src.execute(
        "CREATE TABLE obs (id INTEGER PRIMARY KEY, data TEXT) RETAIN 2 SECONDS",
        &p(),
    )
    .unwrap();
    src.execute("INSERT INTO obs (id, data) VALUES (1, 'fresh')", &p())
        .unwrap();

    src.export_snapshot(&dest).unwrap();

    let artifact = Database::open(&dest).unwrap();
    assert_has_tables(&artifact, &["obs"], "CE10");
    assert_eq!(
        rows(&artifact, "SELECT id, data FROM obs"),
        vec![vec![Value::Int64(1), Value::Text("fresh".into())]],
        "row alive at the snapshot point must be in the artifact"
    );

    // Advance 3 s past the row's stamped time — beyond the 2 s TTL.
    mock_now.fetch_add(3_000, AtomicOrdering::SeqCst);
    let pruned = artifact.run_pruning_cycle();
    assert!(
        pruned > 0,
        "RETAIN metadata must survive export: artifact pruning must delete the expired row, pruned {pruned}"
    );
    assert_eq!(
        rows(&artifact, "SELECT id FROM obs").len(),
        0,
        "expired row must be gone after artifact pruning"
    );
}

// ---------------------------------------------------------------------------
// CE11 — expired-but-unpruned row is exported; source pruning resumes. RED.
// ---------------------------------------------------------------------------
#[test]
fn ce11_expired_unpruned_row_exported_and_source_pruning_resumes() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

    use contextdb_core::Wallclock;

    let mock_now = Arc::new(AtomicU64::new(1_000_000));
    let _clock = {
        let mock_now = Arc::clone(&mock_now);
        Wallclock::test_clock_guard(move || mock_now.load(AtomicOrdering::SeqCst))
    };

    let (_tmp, src, dest) = new_export_fixture();

    src.execute(
        "CREATE TABLE obs (id INTEGER PRIMARY KEY, data TEXT) RETAIN 2 SECONDS",
        &p(),
    )
    .unwrap();
    src.execute(
        "INSERT INTO obs (id, data) VALUES (1, 'expired-but-unpruned')",
        &p(),
    )
    .unwrap();
    // Advance 3 s past the stamp — beyond the 2 s TTL, no real sleep.
    mock_now.fetch_add(3_000, AtomicOrdering::SeqCst);
    // TTL has lapsed but no pruning cycle has run: still observable on source.
    assert_eq!(
        rows(&src, "SELECT id FROM obs").len(),
        1,
        "precondition: expired row still unpruned and visible on the source"
    );

    src.export_snapshot(&dest).unwrap();

    // Positive arm: retention on the source works right after export.
    let pruned = src.run_pruning_cycle();
    assert!(
        pruned > 0,
        "source pruning must resume after export, pruned {pruned}"
    );
    assert_eq!(rows(&src, "SELECT id FROM obs").len(), 0);

    // The artifact still holds the row that was alive at the snapshot point.
    let artifact = Database::open(&dest).unwrap();
    assert_has_tables(&artifact, &["obs"], "CE11");
    assert_eq!(
        rows(&artifact, "SELECT id, data FROM obs"),
        vec![vec![
            Value::Int64(1),
            Value::Text("expired-but-unpruned".into())
        ]],
        "a row alive (unpruned) at the snapshot point must appear in the artifact even though its TTL had lapsed"
    );
}

// ---------------------------------------------------------------------------
// CE12 — concurrent relational writers (forced overlap): single commit
// boundary, no torn insert/update pairs, writer never blocked. RED.
// ---------------------------------------------------------------------------
#[test]
fn ce12_export_under_concurrent_writers_lands_on_single_commit_boundary() {
    let (_tmp, src, dest) = new_export_fixture();

    src.execute("CREATE TABLE pair_a (id INTEGER PRIMARY KEY, v TEXT)", &p())
        .unwrap();
    src.execute("CREATE TABLE pair_b (id INTEGER PRIMARY KEY, v TEXT)", &p())
        .unwrap();
    // Seed pair 0 so the snapshot is non-empty even if it lands before the
    // writer's first commit.
    let tx = src.begin_or_panic();
    src.execute_in_tx(tx, "INSERT INTO pair_a (id, v) VALUES (0, 'orig')", &p())
        .unwrap();
    src.execute_in_tx(tx, "INSERT INTO pair_b (id, v) VALUES (0, 'orig')", &p())
        .unwrap();
    src.commit(tx).unwrap();

    let lsn_pre = src.current_lsn();
    let stop = AtomicBool::new(false);
    let committed = AtomicU64::new(0);
    let report = std::thread::scope(|scope| {
        let writer = scope.spawn(|| {
            // Each commit inserts pair i ('orig') AND seals pair i-1, all in
            // one transaction. Committed state after commit i is therefore
            // exactly: ids 0..=i, v == 'sealed' for id < i, 'orig' for id == i.
            let mut i: i64 = 0;
            while !stop.load(Ordering::SeqCst) {
                i += 1;
                let tx = src.begin_or_panic();
                src.execute_in_tx(
                    tx,
                    &format!("INSERT INTO pair_a (id, v) VALUES ({i}, 'orig')"),
                    &p(),
                )
                .expect("writer insert into pair_a");
                src.execute_in_tx(
                    tx,
                    &format!("INSERT INTO pair_b (id, v) VALUES ({i}, 'orig')"),
                    &p(),
                )
                .expect("writer insert into pair_b");
                src.execute_in_tx(
                    tx,
                    &format!("UPDATE pair_a SET v = 'sealed' WHERE id = {}", i - 1),
                    &p(),
                )
                .expect("writer update in pair_a");
                src.execute_in_tx(
                    tx,
                    &format!("UPDATE pair_b SET v = 'sealed' WHERE id = {}", i - 1),
                    &p(),
                )
                .expect("writer update in pair_b");
                src.commit(tx)
                    .expect("writer commits must not be blocked or fail during export");
                committed.fetch_add(1, Ordering::SeqCst);
            }
        });
        // Forced overlap: wait until the writer is demonstrably committing,
        // export while it is still running, stop it only AFTER export returns.
        while committed.load(Ordering::SeqCst) < 5 {
            std::thread::sleep(Duration::from_millis(1));
        }
        let committed_at_export_start = committed.load(Ordering::SeqCst);
        let report = src
            .export_snapshot(&dest)
            .expect("export under concurrent writers must succeed");
        let committed_at_export_end = committed.load(Ordering::SeqCst);
        stop.store(true, Ordering::SeqCst);
        assert!(
            committed_at_export_end >= committed_at_export_start + 2,
            "commits must keep landing WHILE export runs (got {} -> {}); \
             an export that stalls concurrent writers for its duration is a quiesce, not a hot snapshot",
            committed_at_export_start,
            committed_at_export_end
        );
        writer.join().expect("writer thread must complete cleanly");
        report
    });
    let lsn_post = src.current_lsn();

    let artifact = Database::open(&dest).unwrap();
    assert_has_tables(&artifact, &["pair_a", "pair_b"], "CE12");
    let table_map = |table: &str| -> BTreeMap<i64, String> {
        rows(&artifact, &format!("SELECT id, v FROM {table} ORDER BY id"))
            .into_iter()
            .map(|r| match (&r[0], &r[1]) {
                (Value::Int64(id), Value::Text(v)) => (*id, v.clone()),
                other => panic!("unexpected row shape {other:?}"),
            })
            .collect()
    };
    let a_map = table_map("pair_a");
    let b_map = table_map("pair_b");
    assert_eq!(
        a_map, b_map,
        "a torn snapshot shows a half-pair or half-update: both tables must hold identical id->v maps"
    );
    let max = *a_map.keys().max().expect("seed pair guarantees id 0");
    let expected: BTreeMap<i64, String> = (0..=max)
        .map(|id| {
            let v = if id < max { "sealed" } else { "orig" };
            (id, v.to_string())
        })
        .collect();
    assert_eq!(
        a_map, expected,
        "the artifact must sit on a single commit boundary: ids are exactly the commit prefix 0..={max} with every pair but the newest sealed"
    );

    assert!(
        report.snapshot_lsn >= lsn_pre && report.snapshot_lsn <= lsn_post,
        "snapshot LSN {:?} must lie within the export window [{lsn_pre:?}, {lsn_post:?}]",
        report.snapshot_lsn
    );
    assert_eq!(
        artifact.current_lsn(),
        report.snapshot_lsn,
        "the reopened artifact's own LSN must equal the reported snapshot LSN"
    );
}

// ---------------------------------------------------------------------------
// CE13 — concurrent cross-category writers (forced overlap): row+edge
// committed in one tx are never torn across relational and graph stores. RED.
// ---------------------------------------------------------------------------
#[test]
fn ce13_export_under_concurrent_writers_never_tears_row_edge_commits() {
    let (_tmp, src, dest) = new_export_fixture();

    src.execute("CREATE TABLE nodes (id UUID PRIMARY KEY)", &p())
        .unwrap();
    let hub = Uuid::from_u128(0xCE13_0000);
    let seed = Uuid::from_u128(0xCE13_0FFF);
    let tx = src.begin_or_panic();
    src.insert_row(
        tx,
        "nodes",
        HashMap::from([("id".to_string(), Value::Uuid(seed))]),
    )
    .unwrap();
    src.insert_edge(tx, hub, seed, "LINKS".into(), HashMap::new())
        .unwrap();
    src.commit(tx).unwrap();

    let stop = AtomicBool::new(false);
    let committed = AtomicU64::new(0);
    std::thread::scope(|scope| {
        let writer = scope.spawn(|| {
            let mut i: u128 = 0;
            while !stop.load(Ordering::SeqCst) {
                let node = Uuid::from_u128(0xCE13_1000 + i);
                i += 1;
                let tx = src.begin_or_panic();
                src.insert_row(
                    tx,
                    "nodes",
                    HashMap::from([("id".to_string(), Value::Uuid(node))]),
                )
                .expect("writer insert node row");
                src.insert_edge(tx, hub, node, "LINKS".into(), HashMap::new())
                    .expect("writer insert edge");
                src.commit(tx)
                    .expect("writer commits must not be blocked or fail during export");
                committed.fetch_add(1, Ordering::SeqCst);
            }
        });
        // Forced overlap: export only once the writer is demonstrably
        // committing; stop it only AFTER export returns.
        while committed.load(Ordering::SeqCst) < 5 {
            std::thread::sleep(Duration::from_millis(1));
        }
        let committed_at_export_start = committed.load(Ordering::SeqCst);
        src.export_snapshot(&dest)
            .expect("export under concurrent writers must succeed");
        let committed_at_export_end = committed.load(Ordering::SeqCst);
        stop.store(true, Ordering::SeqCst);
        assert!(
            committed_at_export_end >= committed_at_export_start + 2,
            "row+edge commits must keep landing WHILE export runs (got {} -> {})",
            committed_at_export_start,
            committed_at_export_end
        );
        writer.join().expect("writer thread must complete cleanly");
    });

    let artifact = Database::open(&dest).unwrap();
    assert_has_tables(&artifact, &["nodes"], "CE13");
    let row_ids: BTreeSet<Uuid> = rows(&artifact, "SELECT id FROM nodes")
        .into_iter()
        .map(|r| match &r[0] {
            Value::Uuid(u) => *u,
            other => panic!("unexpected id value {other:?}"),
        })
        .collect();
    let edge_targets: BTreeSet<Uuid> = artifact
        .query_bfs(hub, None, Direction::Outgoing, 1, artifact.snapshot())
        .unwrap_or_else(|e| panic!("query_bfs failed: {e}"))
        .nodes
        .iter()
        .map(|n| n.id)
        .collect();
    assert!(
        row_ids.contains(&seed),
        "the pre-export seeded pair must be present (kills the both-empty vacuous pass)"
    );
    assert_eq!(
        row_ids, edge_targets,
        "every committed row+edge pair must appear in both stores or neither — a cross-category tear shows a set difference"
    );
}

// ---------------------------------------------------------------------------
// CE14 — in-memory source round-trip: rows, vectors, edges. RED.
// ---------------------------------------------------------------------------
#[test]
fn ce14_in_memory_source_round_trip() {
    let tmp = TempDir::new().unwrap();
    let dest = tmp.path().join("artifact.db");
    let src = Database::open_memory();

    src.execute(
        "CREATE TABLE mem (id INTEGER PRIMARY KEY, v TEXT, embedding VECTOR(3))",
        &p(),
    )
    .unwrap();
    src.execute(
        "INSERT INTO mem (id, v, embedding) VALUES (1, 'one', $e)",
        &params(vec![("e", Value::Vector(vec![1.0_f32, 0.0, 0.0]))]),
    )
    .unwrap();
    src.execute(
        "INSERT INTO mem (id, v, embedding) VALUES (2, 'two', $e)",
        &params(vec![("e", Value::Vector(vec![0.0_f32, 1.0, 0.0]))]),
    )
    .unwrap();
    let a = Uuid::from_u128(0xCE14_000A);
    let b = Uuid::from_u128(0xCE14_000B);
    let tx = src.begin_or_panic();
    src.insert_edge(tx, a, b, "LINKS".into(), HashMap::new())
        .unwrap();
    src.commit(tx).unwrap();
    src.execute(
        "CREATE SCHEDULE mem_beat EVERY '200 MILLISECONDS' TX (mem_beat_cb)",
        &p(),
    )
    .unwrap();

    src.export_snapshot(&dest).unwrap();

    let artifact = Database::open(&dest).unwrap();
    assert_has_tables(&artifact, &["mem"], "CE14");
    let reg = artifact.register_cron_callback("mem_beat_cb", |_handle| Ok(()));
    assert!(
        reg.is_ok(),
        "a schedule declared on an in-memory source is database state and must survive \
         export into the artifact; registration failed: {reg:?}"
    );
    assert_eq!(
        rows(&artifact, "SELECT id, v FROM mem ORDER BY id"),
        vec![
            vec![Value::Int64(1), Value::Text("one".into())],
            vec![Value::Int64(2), Value::Text("two".into())],
        ],
        "in-memory source rows must round-trip into a file artifact"
    );
    let near = artifact
        .execute(
            "SELECT id FROM mem ORDER BY embedding <=> $q LIMIT 1",
            &params(vec![("q", Value::Vector(vec![0.0_f32, 1.0, 0.0]))]),
        )
        .unwrap_or_else(|e| panic!("artifact vector search failed: {e}"));
    assert_eq!(
        near.rows,
        vec![vec![Value::Int64(2)]],
        "vector search on the artifact must find the in-memory source's nearest row"
    );
    let neighbors: BTreeSet<Uuid> = artifact
        .query_bfs(a, None, Direction::Outgoing, 1, artifact.snapshot())
        .unwrap_or_else(|e| panic!("query_bfs failed: {e}"))
        .nodes
        .iter()
        .map(|n| n.id)
        .collect();
    assert_eq!(
        neighbors,
        BTreeSet::from([b]),
        "graph edges from an in-memory source must survive export"
    );
}

// ---------------------------------------------------------------------------
// CE15 — source untouched; later export reflects post-snapshot commits. RED.
// ---------------------------------------------------------------------------
#[test]
fn ce15_source_unaffected_and_second_export_reflects_new_commits() {
    let tmp = TempDir::new().unwrap();
    let src_path = tmp.path().join("src.db");
    let src = Database::open(&src_path).unwrap();
    let dest1 = tmp.path().join("artifact1.db");
    let dest2 = tmp.path().join("artifact2.db");

    src.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)", &p())
        .unwrap();
    src.execute("INSERT INTO t (id, v) VALUES (1, 'pre')", &p())
        .unwrap();
    let lsn_before = src.current_lsn();
    let rows_before = rows(&src, "SELECT id, v FROM t ORDER BY id");
    // Subscribe BEFORE the export: a source that was internally closed and
    // reopened would have its subscriber channels cleared.
    let rx = src.subscribe();

    src.export_snapshot(&dest1).unwrap();

    assert_eq!(
        src.current_lsn(),
        lsn_before,
        "export must not advance the source LSN"
    );
    assert_eq!(
        rows(&src, "SELECT id, v FROM t ORDER BY id"),
        rows_before,
        "export must not change source rows"
    );
    assert!(
        src_path.with_extension("lock").exists(),
        "source advisory lock must remain in place after export"
    );

    src.execute("INSERT INTO t (id, v) VALUES (2, 'post')", &p())
        .expect("source must accept commits immediately after export");
    let event = rx.recv_timeout(Duration::from_secs(2));
    assert!(
        event
            .as_ref()
            .is_ok_and(|e| e.tables_changed.contains(&"t".to_string())),
        "a subscriber opened before the export must receive the post-export commit event for `t`; got {event:?}"
    );

    src.export_snapshot(&dest2).unwrap();
    let artifact2 = Database::open(&dest2).unwrap();
    assert_has_tables(&artifact2, &["t"], "CE15/artifact2");
    assert_eq!(
        rows(&artifact2, "SELECT id, v FROM t ORDER BY id"),
        vec![
            vec![Value::Int64(1), Value::Text("pre".into())],
            vec![Value::Int64(2), Value::Text("post".into())],
        ],
        "a later export must reflect post-snapshot commits"
    );

    let artifact1 = Database::open(&dest1).unwrap();
    assert_has_tables(&artifact1, &["t"], "CE15/artifact1");
    assert_eq!(
        rows(&artifact1, "SELECT id, v FROM t ORDER BY id"),
        vec![vec![Value::Int64(1), Value::Text("pre".into())]],
        "the first artifact must stay at its own snapshot point"
    );
}

// ---------------------------------------------------------------------------
// CE16 — destination exists: typed error, dest unmodified, no stray files;
// paired positive export in the same directory. RED.
// ---------------------------------------------------------------------------
#[test]
fn ce16_destination_exists_typed_error_and_dest_unmodified() {
    let tmp = TempDir::new().unwrap();
    let src = Database::open(tmp.path().join("src.db")).unwrap();
    src.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)", &p())
        .unwrap();
    src.execute("INSERT INTO t (id) VALUES (1)", &p()).unwrap();

    let export_dir = tmp.path().join("exports");
    std::fs::create_dir(&export_dir).unwrap();
    let dest = export_dir.join("artifact.db");
    let pre_existing: &[u8] = b"pre-existing destination bytes - not a contextdb artifact";
    std::fs::write(&dest, pre_existing).unwrap();
    // Unrelated bystander in the same directory: export must not delete or
    // touch files it does not own.
    let bystander = export_dir.join("unrelated.txt");
    let bystander_bytes: &[u8] = b"bystander file - export must not touch this";
    std::fs::write(&bystander, bystander_bytes).unwrap();

    let result = src.export_snapshot(&dest);
    assert!(
        matches!(&result, Err(Error::ExportDestinationExists { path }) if path == &dest),
        "existing destination must yield the typed destination-exists error; got {result:?}"
    );
    assert_eq!(
        std::fs::read(&dest).unwrap(),
        pre_existing.to_vec(),
        "the existing destination must be byte-for-byte unmodified"
    );
    assert_eq!(
        std::fs::read(&bystander).unwrap(),
        bystander_bytes.to_vec(),
        "an unrelated file in the destination directory must survive untouched"
    );
    assert_eq!(
        dir_names(&export_dir),
        vec!["artifact.db".to_string(), "unrelated.txt".to_string()],
        "a destination-exists failure must leave no stray files and delete nothing in the destination directory"
    );

    // Positive pair: a fresh path in the same directory exports fine.
    let dest_ok = export_dir.join("artifact-ok.db");
    src.export_snapshot(&dest_ok)
        .expect("export to a fresh destination must succeed after a destination-exists failure");
    let artifact = Database::open(&dest_ok).unwrap();
    assert_has_tables(&artifact, &["t"], "CE16/positive");
    assert_eq!(
        rows(&artifact, "SELECT id FROM t"),
        vec![vec![Value::Int64(1)]]
    );
}

// ---------------------------------------------------------------------------
// CE17 — missing destination directory: typed I/O error, zero debris;
// paired positive export after mkdir. RED.
// ---------------------------------------------------------------------------
#[test]
fn ce17_missing_destination_directory_typed_error_no_partial_artifact() {
    let tmp = TempDir::new().unwrap();
    let src = Database::open(tmp.path().join("src.db")).unwrap();
    src.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)", &p())
        .unwrap();
    src.execute("INSERT INTO t (id) VALUES (7)", &p()).unwrap();
    // Unrelated bystander in the parent directory: export must not delete or
    // touch files it does not own.
    let bystander = tmp.path().join("unrelated.txt");
    let bystander_bytes: &[u8] = b"bystander file - export must not touch this";
    std::fs::write(&bystander, bystander_bytes).unwrap();
    let before = dir_names(tmp.path());

    let missing_dir = tmp.path().join("no_such_dir");
    let dest = missing_dir.join("artifact.db");
    let result = src.export_snapshot(&dest);
    assert!(
        matches!(&result, Err(Error::ExportIo { .. })),
        "missing destination directory must yield the typed export-I/O error; got {result:?}"
    );
    assert!(
        !missing_dir.exists(),
        "export must not create the missing destination directory"
    );
    assert!(!dest.exists(), "no partial artifact may remain at dest");
    assert_eq!(
        dir_names(tmp.path()),
        before,
        "a failed export must leave no stray files and delete nothing in the parent directory"
    );
    assert_eq!(
        std::fs::read(&bystander).unwrap(),
        bystander_bytes.to_vec(),
        "an unrelated file in the parent directory must survive untouched"
    );

    // Positive pair: once the directory exists, the same dest succeeds.
    std::fs::create_dir(&missing_dir).unwrap();
    src.export_snapshot(&dest)
        .expect("export must succeed once the destination directory exists");
    let artifact = Database::open(&dest).unwrap();
    assert_has_tables(&artifact, &["t"], "CE17/positive");
    assert_eq!(
        rows(&artifact, "SELECT id FROM t"),
        vec![vec![Value::Int64(7)]]
    );
}

// ---------------------------------------------------------------------------
// CE18 — ExportReport honesty against a known fixture. RED.
// ---------------------------------------------------------------------------
#[test]
fn ce18_export_report_counts_are_honest() {
    let tmp = TempDir::new().unwrap();
    let src = Database::open(tmp.path().join("src.db")).unwrap();
    let dest1 = tmp.path().join("artifact1.db");
    let dest2 = tmp.path().join("artifact2.db");

    src.execute(
        "CREATE TABLE docs (id UUID PRIMARY KEY, body TEXT, embedding VECTOR(3))",
        &p(),
    )
    .unwrap();
    src.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)", &p())
        .unwrap();
    // 2 live docs rows, both with vectors.
    let d1 = Uuid::from_u128(0xCE18_00D1);
    let d2 = Uuid::from_u128(0xCE18_00D2);
    src.execute(
        "INSERT INTO docs (id, body, embedding) VALUES ($id, 'doc', $e)",
        &params(vec![
            ("id", Value::Uuid(d1)),
            ("e", Value::Vector(vec![1.0_f32, 0.0, 0.0])),
        ]),
    )
    .unwrap();
    src.execute(
        "INSERT INTO docs (id, body, embedding) VALUES ($id, 'doc', $e)",
        &params(vec![
            ("id", Value::Uuid(d2)),
            ("e", Value::Vector(vec![0.0_f32, 1.0, 0.0])),
        ]),
    )
    .unwrap();
    // 4 t rows; 1 updated, 1 deleted → 3 live.
    for (id, v) in [(1, "v1"), (2, "v2"), (3, "v3"), (4, "v4")] {
        src.execute(&format!("INSERT INTO t (id, v) VALUES ({id}, '{v}')"), &p())
            .unwrap();
    }
    src.execute("UPDATE t SET v = 'v1-updated' WHERE id = 1", &p())
        .unwrap();
    src.execute("DELETE FROM t WHERE id = 4", &p()).unwrap();
    // 2 edges.
    let n1 = Uuid::from_u128(0xCE18_00E1);
    let n2 = Uuid::from_u128(0xCE18_00E2);
    let n3 = Uuid::from_u128(0xCE18_00E3);
    let tx = src.begin_or_panic();
    src.insert_edge(tx, n1, n2, "LINKS".into(), HashMap::new())
        .unwrap();
    src.insert_edge(tx, n1, n3, "LINKS".into(), HashMap::new())
        .unwrap();
    src.commit(tx).unwrap();

    let lsn = src.current_lsn();
    let report: ExportReport = src.export_snapshot(&dest1).unwrap();
    assert_eq!(
        report.snapshot_lsn, lsn,
        "a quiescent export must snapshot at the current commit boundary"
    );
    assert_eq!(
        report.rows, 5,
        "live rows at the snapshot point: 2 docs + 3 t (deleted row is not an exported row)"
    );
    assert_eq!(report.edges, 2, "exactly the two committed edges");
    assert_eq!(report.vectors, 2, "exactly the two stored vectors");
    assert!(
        report.bytes_written > 0,
        "bytes_written must be honest, got {}",
        report.bytes_written
    );
    assert_eq!(
        report.bytes_written,
        std::fs::metadata(&dest1).unwrap().len(),
        "bytes_written must equal the artifact's on-disk file size"
    );
    let reopened1 = Database::open(&dest1).unwrap();
    assert_eq!(
        reopened1.current_lsn(),
        report.snapshot_lsn,
        "the reopened artifact's own LSN must equal the reported snapshot LSN"
    );

    // Strictly larger database → strictly more bytes written.
    for i in 0..500_i64 {
        src.execute(
            &format!(
                "INSERT INTO t (id, v) VALUES ({}, '{}')",
                100 + i,
                "x".repeat(200)
            ),
            &p(),
        )
        .unwrap();
    }
    let report2: ExportReport = src.export_snapshot(&dest2).unwrap();
    assert_eq!(
        report2.rows, 505,
        "2 docs + 3 original t + 500 padded t rows"
    );
    assert_eq!(report2.edges, 2);
    assert_eq!(report2.vectors, 2);
    assert_eq!(report2.snapshot_lsn, src.current_lsn());
    assert_eq!(
        report2.bytes_written,
        std::fs::metadata(&dest2).unwrap().len(),
        "second report's bytes_written must equal its artifact's on-disk file size"
    );
    assert!(
        report2.bytes_written > report.bytes_written,
        "a strictly larger database must report strictly more bytes written ({} vs {})",
        report2.bytes_written,
        report.bytes_written
    );
    let reopened2 = Database::open(&dest2).unwrap();
    assert_eq!(
        reopened2.current_lsn(),
        report2.snapshot_lsn,
        "the second artifact's own LSN must equal its reported snapshot LSN"
    );
}

// ---------------------------------------------------------------------------
// CE19 — artifact independence: simultaneous handles, no cross-leakage,
// re-exportable. RED.
// ---------------------------------------------------------------------------
#[test]
fn ce19_artifact_is_independent_of_source() {
    let tmp_src = TempDir::new().unwrap();
    let tmp_art = TempDir::new().unwrap();
    let src_path = tmp_src.path().join("src.db");
    let src = Database::open(&src_path).unwrap();
    src.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)", &p())
        .unwrap();
    src.execute("INSERT INTO t (id, v) VALUES (1, 'origin')", &p())
        .unwrap();

    let artifact_path = tmp_art.path().join("artifact.db");
    src.export_snapshot(&artifact_path).unwrap();

    // Open the artifact at a different path while the source is open in the
    // same process.
    let artifact = Database::open(&artifact_path)
        .expect("artifact must open while the source is open in the same process");
    assert_has_tables(&artifact, &["t"], "CE19");
    assert!(
        src_path.with_extension("lock").exists(),
        "source lock must be unaffected by opening the artifact"
    );

    // Writes to the artifact must not leak into the source, and vice versa.
    artifact
        .execute("INSERT INTO t (id, v) VALUES (2, 'artifact-only')", &p())
        .expect("artifact must accept writes");
    assert_eq!(
        rows(&src, "SELECT id, v FROM t ORDER BY id"),
        vec![vec![Value::Int64(1), Value::Text("origin".into())]],
        "artifact writes must never appear in the source"
    );
    src.execute("INSERT INTO t (id, v) VALUES (3, 'source-only')", &p())
        .expect("source must keep serving writes");
    assert_eq!(
        rows(&artifact, "SELECT id, v FROM t ORDER BY id"),
        vec![
            vec![Value::Int64(1), Value::Text("origin".into())],
            vec![Value::Int64(2), Value::Text("artifact-only".into())],
        ],
        "source writes must never appear in the artifact"
    );

    // Re-export from the artifact itself.
    let second_gen = tmp_art.path().join("second-gen.db");
    artifact
        .export_snapshot(&second_gen)
        .expect("re-export from an artifact must work");
    let grandchild = Database::open(&second_gen).unwrap();
    assert_has_tables(&grandchild, &["t"], "CE19/second-gen");
    assert_eq!(
        rows(&grandchild, "SELECT id, v FROM t ORDER BY id"),
        vec![
            vec![Value::Int64(1), Value::Text("origin".into())],
            vec![Value::Int64(2), Value::Text("artifact-only".into())],
        ],
        "the second-generation artifact must hold the first artifact's exact rows"
    );
}

// ---------------------------------------------------------------------------
// CE20 — concurrent exports to distinct destinations: both complete and
// consistent. RED.
// ---------------------------------------------------------------------------
#[test]
fn ce20_concurrent_exports_to_distinct_destinations_both_complete() {
    let tmp = TempDir::new().unwrap();
    let src = Database::open(tmp.path().join("src.db")).unwrap();
    let dest_a = tmp.path().join("artifact-a.db");
    let dest_b = tmp.path().join("artifact-b.db");

    src.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)", &p())
        .unwrap();
    src.execute("INSERT INTO t (id, v) VALUES (1, 'one')", &p())
        .unwrap();
    src.execute("INSERT INTO t (id, v) VALUES (2, 'two')", &p())
        .unwrap();
    let g1 = Uuid::from_u128(0xCE20_0001);
    let g2 = Uuid::from_u128(0xCE20_0002);
    let g3 = Uuid::from_u128(0xCE20_0003);
    let tx = src.begin_or_panic();
    src.insert_edge(tx, g1, g2, "LINKS".into(), HashMap::new())
        .unwrap();
    src.insert_edge(tx, g1, g3, "LINKS".into(), HashMap::new())
        .unwrap();
    src.commit(tx).unwrap();
    let lsn = src.current_lsn();

    let (report_a, report_b) = std::thread::scope(|scope| {
        let ta = scope.spawn(|| {
            src.export_snapshot(&dest_a)
                .expect("concurrent export to dest_a must succeed")
        });
        let tb = scope.spawn(|| {
            src.export_snapshot(&dest_b)
                .expect("concurrent export to dest_b must succeed")
        });
        (
            ta.join().expect("export thread a must complete cleanly"),
            tb.join().expect("export thread b must complete cleanly"),
        )
    });

    for (report, dest, what) in [
        (&report_a, &dest_a, "CE20/report-a"),
        (&report_b, &dest_b, "CE20/report-b"),
    ] {
        assert_eq!(report.rows, 2, "{what}: full fixture row count");
        assert_eq!(report.edges, 2, "{what}: full fixture edge count");
        assert_eq!(report.vectors, 0, "{what}: fixture has no vectors");
        assert_eq!(
            report.snapshot_lsn, lsn,
            "{what}: quiescent source — both snapshots sit on the same boundary"
        );
        assert_eq!(
            report.bytes_written,
            std::fs::metadata(dest).unwrap().len(),
            "{what}: bytes_written must equal the artifact's on-disk file size"
        );
    }

    let expected_rows = vec![
        vec![Value::Int64(1), Value::Text("one".into())],
        vec![Value::Int64(2), Value::Text("two".into())],
    ];
    for (dest, what) in [(&dest_a, "CE20/a"), (&dest_b, "CE20/b")] {
        let artifact = Database::open(dest).unwrap();
        assert_has_tables(&artifact, &["t"], what);
        assert_eq!(
            rows(&artifact, "SELECT id, v FROM t ORDER BY id"),
            expected_rows,
            "{what}: exact fixture rows"
        );
        let targets: BTreeSet<Uuid> = artifact
            .query_bfs(g1, None, Direction::Outgoing, 1, artifact.snapshot())
            .unwrap_or_else(|e| panic!("{what}: query_bfs failed: {e}"))
            .nodes
            .iter()
            .map(|n| n.id)
            .collect();
        assert_eq!(
            targets,
            BTreeSet::from([g2, g3]),
            "{what}: exact fixture edges"
        );
    }
}

// ---------------------------------------------------------------------------
// CE21 — export during an open explicit transaction: returns, excludes
// uncommitted state, commit still works afterward. RED.
// ---------------------------------------------------------------------------
#[test]
fn ce21_export_during_open_transaction_excludes_uncommitted_state() {
    let tmp = TempDir::new().unwrap();
    let src = Database::open(tmp.path().join("src.db")).unwrap();
    let dest1 = tmp.path().join("artifact1.db");
    let dest2 = tmp.path().join("artifact2.db");

    src.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)", &p())
        .unwrap();
    src.execute("INSERT INTO t (id, v) VALUES (1, 'committed')", &p())
        .unwrap();

    let tx = src.begin_or_panic();
    src.insert_row(
        tx,
        "t",
        HashMap::from([
            ("id".to_string(), Value::Int64(2)),
            ("v".to_string(), Value::Text("uncommitted".into())),
        ]),
    )
    .unwrap();

    // Export with the transaction still open: must return, not deadlock.
    src.export_snapshot(&dest1)
        .expect("export must not block or fail against an open transaction on the same handle");

    let artifact1 = Database::open(&dest1).unwrap();
    assert_has_tables(&artifact1, &["t"], "CE21");
    // One exact assertion covers both arms: committed seed row present
    // (positive — fails under a copy-nothing no-op) AND uncommitted row
    // absent (negative — cannot pass vacuously, the same assertion demands
    // the committed row).
    assert_eq!(
        rows(&artifact1, "SELECT id, v FROM t ORDER BY id"),
        vec![vec![Value::Int64(1), Value::Text("committed".into())]],
        "the artifact must hold exactly the committed state: seed row present, uncommitted row absent"
    );

    src.commit(tx)
        .expect("the open transaction must commit normally after the export");
    assert_eq!(
        rows(&src, "SELECT id, v FROM t ORDER BY id"),
        vec![
            vec![Value::Int64(1), Value::Text("committed".into())],
            vec![Value::Int64(2), Value::Text("uncommitted".into())],
        ],
        "source must hold both rows after the commit"
    );

    src.export_snapshot(&dest2).unwrap();
    let artifact2 = Database::open(&dest2).unwrap();
    assert_has_tables(&artifact2, &["t"], "CE21/second");
    assert_eq!(
        rows(&artifact2, "SELECT id, v FROM t ORDER BY id"),
        vec![
            vec![Value::Int64(1), Value::Text("committed".into())],
            vec![Value::Int64(2), Value::Text("uncommitted".into())],
        ],
        "a later export must include the now-committed row"
    );
}

// ---------------------------------------------------------------------------
// CE22 — vector completeness at scale: 999 vectors round-trip with exact
// searchable id set and pinned beacon neighbors. RED.
// (999, not 1000: HNSW_THRESHOLD is 1000, so 999 keeps both sides on the
// brute-force-exact search path and the full-recall assertion deterministic.)
// ---------------------------------------------------------------------------
#[test]
fn ce22_vector_index_completeness_at_scale() {
    let (_tmp, src, dest) = new_export_fixture();

    src.execute(
        "CREATE TABLE corpus (id UUID PRIMARY KEY, embedding VECTOR(8))",
        &p(),
    )
    .unwrap();

    // 999 vectors in one transaction. Beacons at insertion indexes 7, 499,
    // 991 sit alone on axes 5/6/7; every other vector lives in the span of
    // axes 0..=3, so each beacon is the unique zero-distance answer to its
    // axis probe.
    let uuid_for = |i: u128| Uuid::from_u128(0xCE22_0000 + i);
    let beacon = |axis: usize| -> Vec<f32> {
        let mut v = vec![0.0_f32; 8];
        v[axis] = 1.0;
        v
    };
    let tx = src.begin_or_panic();
    for i in 0..999_u128 {
        let emb: Vec<f32> = match i {
            7 => beacon(5),
            499 => beacon(6),
            991 => beacon(7),
            _ => {
                let a = (i % 17) as f32 * 0.01;
                let b = (i % 29) as f32 * 0.01;
                vec![1.0 + a, 1.0 + b, 1.0 - a, 1.0 - b, 0.0, 0.0, 0.0, 0.0]
            }
        };
        src.execute_in_tx(
            tx,
            "INSERT INTO corpus (id, embedding) VALUES ($id, $e)",
            &params(vec![
                ("id", Value::Uuid(uuid_for(i))),
                ("e", Value::Vector(emb)),
            ]),
        )
        .unwrap();
    }
    src.commit(tx).unwrap();

    let all_ids = |db: &Database, what: &str| -> BTreeSet<Uuid> {
        db.execute(
            "SELECT id FROM corpus ORDER BY embedding <=> $q LIMIT 999",
            &params(vec![(
                "q",
                Value::Vector(vec![1.0_f32, 1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0]),
            )]),
        )
        .unwrap_or_else(|e| panic!("{what}: wide vector query failed: {e}"))
        .rows
        .into_iter()
        .map(|r| match &r[0] {
            Value::Uuid(u) => *u,
            other => panic!("{what}: unexpected id value {other:?}"),
        })
        .collect()
    };
    let top1 = |db: &Database, axis: usize, what: &str| -> Value {
        let mut result = db
            .execute(
                "SELECT id FROM corpus ORDER BY embedding <=> $q LIMIT 1",
                &params(vec![("q", Value::Vector(beacon(axis)))]),
            )
            .unwrap_or_else(|e| panic!("{what}: beacon query failed: {e}"));
        assert_eq!(
            result.rows.len(),
            1,
            "{what}: beacon query must return exactly one row"
        );
        result.rows.remove(0).remove(0)
    };
    let expected_ids: BTreeSet<Uuid> = (0..999_u128).map(uuid_for).collect();

    // Fixture gate on the source — validates the fixture independently of
    // export, so an artifact failure below is attributable to export alone.
    assert_eq!(
        all_ids(&src, "CE22/source"),
        expected_ids,
        "precondition: all 999 vectors searchable on the source"
    );
    assert_eq!(top1(&src, 5, "CE22/source"), Value::Uuid(uuid_for(7)));

    src.export_snapshot(&dest).unwrap();
    let artifact = Database::open(&dest).unwrap();
    assert_has_tables(&artifact, &["corpus"], "CE22");

    assert_eq!(
        all_ids(&artifact, "CE22/artifact"),
        expected_ids,
        "all 999 vectors must be searchable in the artifact — exact id set, no silently truncated tail"
    );
    assert_eq!(
        top1(&artifact, 5, "CE22/artifact"),
        Value::Uuid(uuid_for(7)),
        "beacon at insertion index 7 must be the exact nearest neighbor on its axis"
    );
    assert_eq!(
        top1(&artifact, 6, "CE22/artifact"),
        Value::Uuid(uuid_for(499)),
        "beacon at insertion index 499 must be the exact nearest neighbor on its axis"
    );
    assert_eq!(
        top1(&artifact, 7, "CE22/artifact"),
        Value::Uuid(uuid_for(991)),
        "beacon at insertion index 991 must be the exact nearest neighbor on its axis"
    );
}
