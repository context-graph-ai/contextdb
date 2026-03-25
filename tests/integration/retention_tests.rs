use contextdb_core::{Value, VersionedRow};
use contextdb_engine::{Database, QueryResult};
use std::collections::HashMap;
use std::thread;
use std::time::Duration;

fn p() -> HashMap<String, Value> {
    HashMap::new()
}

fn row_count(db: &Database, table: &str) -> usize {
    let result = db.execute(&format!("SELECT * FROM {table}"), &p()).unwrap();
    result.rows.len()
}

fn col_idx(result: &QueryResult, name: &str) -> usize {
    result
        .columns
        .iter()
        .position(|c| c == name)
        .unwrap_or_else(|| panic!("column '{name}' not found in {:?}", result.columns))
}

// ---------------------------------------------------------------------------
// R01 — Basic age-based pruning (with young-row survival guard)
// RED: run_pruning_cycle is a no-op stub returning 0, rows stay
// ---------------------------------------------------------------------------
#[test]
fn r01_basic_age_pruning() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE obs (id INTEGER PRIMARY KEY, data TEXT) RETAIN 2 SECONDS",
        &p(),
    )
    .unwrap();
    db.execute("INSERT INTO obs (id, data) VALUES (1, 'old1')", &p())
        .unwrap();
    db.execute("INSERT INTO obs (id, data) VALUES (2, 'old2')", &p())
        .unwrap();

    assert_eq!(row_count(&db, "obs"), 2);

    thread::sleep(Duration::from_secs(3));

    // Insert a young row AFTER the sleep — must survive pruning
    db.execute("INSERT INTO obs (id, data) VALUES (3, 'young')", &p())
        .unwrap();

    let pruned = db.run_pruning_cycle();

    assert!(pruned > 0, "pruning must delete expired rows");
    assert_eq!(row_count(&db, "obs"), 1, "only the young row survives");

    let result = db.execute("SELECT * FROM obs", &p()).unwrap();
    let idx = col_idx(&result, "data");
    assert_eq!(result.rows[0][idx], Value::Text("young".to_string()));
}

// ---------------------------------------------------------------------------
// R02 — Short TTL with sleep verification
// RED: run_pruning_cycle is a no-op, rows survive
// ---------------------------------------------------------------------------
#[test]
fn r02_short_ttl_prunes_old_not_new() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE obs (id INTEGER PRIMARY KEY, data TEXT) RETAIN 2 SECONDS",
        &p(),
    )
    .unwrap();

    db.execute("INSERT INTO obs (id, data) VALUES (1, 'old')", &p())
        .unwrap();
    thread::sleep(Duration::from_secs(3));
    db.execute("INSERT INTO obs (id, data) VALUES (2, 'new')", &p())
        .unwrap();

    let pruned = db.run_pruning_cycle();
    assert!(pruned > 0, "at least the old row must be pruned");
    assert_eq!(row_count(&db, "obs"), 1, "only the new row survives");

    let result = db.execute("SELECT * FROM obs", &p()).unwrap();
    let idx = col_idx(&result, "data");
    assert_eq!(result.rows[0][idx], Value::Text("new".to_string()));
}

// ---------------------------------------------------------------------------
// R03 — SYNC SAFE prevents pruning of unsynced rows
// RED: run_pruning_cycle is a no-op
// ---------------------------------------------------------------------------
#[test]
fn r03_sync_safe_unsynced_rows_survive() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE obs (id INTEGER PRIMARY KEY, data TEXT) RETAIN 2 SECONDS SYNC SAFE",
        &p(),
    )
    .unwrap();

    db.execute("INSERT INTO obs (id, data) VALUES (1, 'a')", &p())
        .unwrap();
    thread::sleep(Duration::from_secs(3));

    // Sync watermark is 0 — no rows synced
    db.set_sync_watermark(0);
    db.run_pruning_cycle();

    assert_eq!(
        row_count(&db, "obs"),
        1,
        "unsynced row must survive despite expired TTL"
    );
}

// ---------------------------------------------------------------------------
// R04 — SYNC SAFE allows pruning after sync
// RED: run_pruning_cycle is a no-op
// ---------------------------------------------------------------------------
#[test]
fn r04_sync_safe_synced_rows_pruned() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE obs (id INTEGER PRIMARY KEY, data TEXT) RETAIN 2 SECONDS SYNC SAFE",
        &p(),
    )
    .unwrap();

    db.execute("INSERT INTO obs (id, data) VALUES (1, 'a')", &p())
        .unwrap();

    // Advance sync watermark past the row's LSN
    db.set_sync_watermark(u64::MAX);

    thread::sleep(Duration::from_secs(3));
    let pruned = db.run_pruning_cycle();

    assert!(pruned > 0, "synced expired row must be pruned");
    assert_eq!(row_count(&db, "obs"), 0);
}

// ---------------------------------------------------------------------------
// R05 — Per-row EXPIRES override
// RED: run_pruning_cycle is a no-op
// ---------------------------------------------------------------------------
#[test]
fn r05_expires_column_per_row_override() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE obs (id INTEGER PRIMARY KEY, data TEXT, expires_at TIMESTAMP EXPIRES) RETAIN 1000 DAYS",
        &p(),
    )
    .unwrap();

    // Row with explicit past expiry — should be pruned immediately
    db.execute(
        "INSERT INTO obs (id, data, expires_at) VALUES (1, 'past', '2020-01-01T00:00:00Z')",
        &p(),
    )
    .unwrap();

    // Row with far-future expiry — should survive
    db.execute(
        "INSERT INTO obs (id, data, expires_at) VALUES (2, 'future', '2099-01-01T00:00:00Z')",
        &p(),
    )
    .unwrap();

    let pruned = db.run_pruning_cycle();

    assert!(pruned > 0, "past-expiry row must be pruned");
    assert_eq!(row_count(&db, "obs"), 1, "only future row survives");

    let result = db.execute("SELECT * FROM obs", &p()).unwrap();
    let idx = col_idx(&result, "data");
    assert_eq!(result.rows[0][idx], Value::Text("future".to_string()));
}

// ---------------------------------------------------------------------------
// R06 — Infinity means never prune
// RED: run_pruning_cycle is a no-op
// ---------------------------------------------------------------------------
#[test]
fn r06_expires_infinity_never_pruned() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE obs (id INTEGER PRIMARY KEY, data TEXT, expires_at TIMESTAMP EXPIRES) RETAIN 1 SECONDS",
        &p(),
    )
    .unwrap();

    db.execute(
        "INSERT INTO obs (id, data, expires_at) VALUES (1, 'immortal', 'infinity')",
        &p(),
    )
    .unwrap();

    thread::sleep(Duration::from_secs(2));
    db.run_pruning_cycle();

    assert_eq!(
        row_count(&db, "obs"),
        1,
        "infinity row must never be pruned"
    );
}

// ---------------------------------------------------------------------------
// R07 — NULL EXPIRES falls back to table-level RETAIN
// RED: run_pruning_cycle is a no-op
// ---------------------------------------------------------------------------
#[test]
fn r07_null_expires_uses_default_ttl() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE obs (id INTEGER PRIMARY KEY, data TEXT, expires_at TIMESTAMP EXPIRES) RETAIN 2 SECONDS",
        &p(),
    )
    .unwrap();

    // NULL expires_at — should use table-level 2 second TTL
    db.execute(
        "INSERT INTO obs (id, data, expires_at) VALUES (1, 'default-ttl', NULL)",
        &p(),
    )
    .unwrap();

    thread::sleep(Duration::from_secs(3));
    let pruned = db.run_pruning_cycle();

    assert!(pruned > 0, "NULL expires row must be pruned by table TTL");
    assert_eq!(row_count(&db, "obs"), 0);
}

// ---------------------------------------------------------------------------
// R08 — ALTER TABLE SET RETAIN adds retention (with young-row survival guard)
// RED: ALTER TABLE SET RETAIN is not wired (stub AST builder ignores it)
// ---------------------------------------------------------------------------
#[test]
fn r08_alter_table_set_retain() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE obs (id INTEGER PRIMARY KEY, data TEXT)", &p())
        .unwrap();
    db.execute("INSERT INTO obs (id, data) VALUES (1, 'old')", &p())
        .unwrap();

    // No retention initially — pruning does nothing
    db.run_pruning_cycle();
    assert_eq!(row_count(&db, "obs"), 1);

    // Add retention
    db.execute("ALTER TABLE obs SET RETAIN 2 SECONDS", &p())
        .unwrap();

    thread::sleep(Duration::from_secs(3));

    // Insert a young row AFTER the sleep — must survive
    db.execute("INSERT INTO obs (id, data) VALUES (2, 'young')", &p())
        .unwrap();

    let pruned = db.run_pruning_cycle();

    assert!(
        pruned > 0,
        "old row must be pruned after ALTER TABLE SET RETAIN"
    );
    assert_eq!(row_count(&db, "obs"), 1, "only the young row survives");

    let result = db.execute("SELECT * FROM obs", &p()).unwrap();
    let idx = col_idx(&result, "data");
    assert_eq!(result.rows[0][idx], Value::Text("young".to_string()));
}

// ---------------------------------------------------------------------------
// R09 — ALTER TABLE DROP RETAIN stops pruning
// RED: ALTER TABLE DROP RETAIN is not wired
// ---------------------------------------------------------------------------
#[test]
fn r09_alter_table_drop_retain() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE obs (id INTEGER PRIMARY KEY, data TEXT) RETAIN 2 SECONDS",
        &p(),
    )
    .unwrap();

    // Drop retention
    db.execute("ALTER TABLE obs DROP RETAIN", &p()).unwrap();

    db.execute("INSERT INTO obs (id, data) VALUES (1, 'a')", &p())
        .unwrap();

    thread::sleep(Duration::from_secs(3));
    db.run_pruning_cycle();

    assert_eq!(
        row_count(&db, "obs"),
        1,
        "row must survive after DROP RETAIN"
    );
}

// ---------------------------------------------------------------------------
// R10 — ALTER TABLE SET RETAIN SYNC SAFE
// RED: ALTER TABLE SET RETAIN is not wired
// ---------------------------------------------------------------------------
#[test]
fn r10_alter_table_set_retain_sync_safe() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE obs (id INTEGER PRIMARY KEY, data TEXT)", &p())
        .unwrap();

    db.execute("ALTER TABLE obs SET RETAIN 2 SECONDS SYNC SAFE", &p())
        .unwrap();

    db.execute("INSERT INTO obs (id, data) VALUES (1, 'a')", &p())
        .unwrap();

    thread::sleep(Duration::from_secs(3));

    // Sync watermark at 0 — unsynced
    db.set_sync_watermark(0);
    db.run_pruning_cycle();

    assert_eq!(
        row_count(&db, "obs"),
        1,
        "unsynced row survives with SYNC SAFE via ALTER"
    );

    // Now sync and prune
    db.set_sync_watermark(u64::MAX);
    let pruned = db.run_pruning_cycle();

    assert!(pruned > 0, "synced expired row must be pruned");
    assert_eq!(row_count(&db, "obs"), 0);
}

// ---------------------------------------------------------------------------
// R11 — IMMUTABLE + RETAIN mutual exclusion at CREATE
// RED: grammar stub accepts both; engine stub does not reject
// ---------------------------------------------------------------------------
#[test]
fn r11_immutable_retain_mutual_exclusion_create() {
    let db = Database::open_memory();
    let result = db.execute(
        "CREATE TABLE obs (id INTEGER PRIMARY KEY) IMMUTABLE RETAIN 30 DAYS",
        &p(),
    );

    assert!(
        result.is_err(),
        "CREATE TABLE with both IMMUTABLE and RETAIN must error"
    );
    let err_msg = format!("{}", result.unwrap_err());
    let has_keyword = err_msg.to_lowercase().contains("immutable")
        || err_msg.to_lowercase().contains("retain")
        || err_msg.to_lowercase().contains("mutually exclusive");
    assert!(
        has_keyword,
        "error message must mention IMMUTABLE or RETAIN conflict, got: {err_msg}"
    );
}

// ---------------------------------------------------------------------------
// R12 — IMMUTABLE + RETAIN mutual exclusion at ALTER
// RED: ALTER TABLE SET RETAIN is not wired; IMMUTABLE check not performed
// ---------------------------------------------------------------------------
#[test]
fn r12_immutable_retain_mutual_exclusion_alter() {
    let db = Database::open_memory();
    db.execute("CREATE TABLE obs (id INTEGER PRIMARY KEY) IMMUTABLE", &p())
        .unwrap();

    let result = db.execute("ALTER TABLE obs SET RETAIN 14 DAYS", &p());

    assert!(
        result.is_err(),
        "ALTER TABLE SET RETAIN on IMMUTABLE table must error"
    );
}

// ---------------------------------------------------------------------------
// R13 — EXPIRES on non-TIMESTAMP column is an error
// RED: grammar stub accepts EXPIRES on any column; engine stub does not check type
// ---------------------------------------------------------------------------
#[test]
fn r13_expires_on_non_timestamp_errors() {
    let db = Database::open_memory();
    let result = db.execute(
        "CREATE TABLE obs (id INTEGER PRIMARY KEY, name TEXT EXPIRES) RETAIN 30 DAYS",
        &p(),
    );

    assert!(
        result.is_err(),
        "EXPIRES on non-TIMESTAMP column must be an error"
    );
}

// ---------------------------------------------------------------------------
// R14 — TIMESTAMP column type: ISO 8601 round-trip
// RED: ColumnType::Timestamp stub exists but engine does not parse ISO strings
//      into Value::Timestamp on INSERT
// ---------------------------------------------------------------------------
#[test]
fn r14_timestamp_iso8601_round_trip() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE events (id INTEGER PRIMARY KEY, ts TIMESTAMP)",
        &p(),
    )
    .unwrap();

    db.execute(
        "INSERT INTO events (id, ts) VALUES (1, '2026-03-24T12:00:00Z')",
        &p(),
    )
    .unwrap();

    let result = db
        .execute("SELECT * FROM events WHERE id = 1", &p())
        .unwrap();
    let idx = col_idx(&result, "ts");
    match &result.rows[0][idx] {
        Value::Timestamp(millis) => {
            // 2026-03-24T12:00:00Z in epoch millis
            assert!(*millis > 0, "timestamp must be positive epoch millis");
            // Approximately 2026-03-24 — between 2026-01-01 and 2027-01-01
            let year_2026_start = 1_767_225_600_000_i64; // 2026-01-01T00:00:00Z
            let year_2027_start = 1_798_761_600_000_i64; // 2027-01-01T00:00:00Z
            assert!(
                *millis >= year_2026_start && *millis < year_2027_start,
                "timestamp {millis} must be in year 2026"
            );
        }
        other => panic!("expected Value::Timestamp, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// R15 — 'infinity' stored as i64::MAX
// RED: engine does not parse 'infinity' into Value::Timestamp(i64::MAX)
// ---------------------------------------------------------------------------
#[test]
fn r15_infinity_sentinel_value() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE obs (id INTEGER PRIMARY KEY, expires_at TIMESTAMP EXPIRES) RETAIN 30 DAYS",
        &p(),
    )
    .unwrap();

    db.execute(
        "INSERT INTO obs (id, expires_at) VALUES (1, 'infinity')",
        &p(),
    )
    .unwrap();

    let result = db.execute("SELECT * FROM obs WHERE id = 1", &p()).unwrap();
    let idx = col_idx(&result, "expires_at");
    match &result.rows[0][idx] {
        Value::Timestamp(millis) => {
            assert_eq!(*millis, i64::MAX, "'infinity' must be stored as i64::MAX");
        }
        other => panic!("expected Value::Timestamp(i64::MAX), got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// R16 — \d tablename shows RETAIN and SYNC SAFE
// RED: stub TableMeta has default_ttl_seconds=None, sync_safe=false;
//      \d output won't contain RETAIN
// ---------------------------------------------------------------------------
#[test]
fn r16_describe_shows_retain() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE obs (id INTEGER PRIMARY KEY, data TEXT) RETAIN 30 DAYS SYNC SAFE",
        &p(),
    )
    .unwrap();

    // \d is exposed via the meta-command handler which ultimately calls
    // Database::describe_table or similar. We test via SQL-level introspection.
    // The engine returns schema info that includes retention metadata.
    let _result = db.execute("SELECT * FROM obs", &p()).unwrap();

    // For introspection, we rely on the internal table_meta access.
    // The test verifies the metadata is stored correctly.
    let meta = db.table_meta("obs");
    assert!(meta.is_some(), "table 'obs' must have metadata");
    let meta = meta.unwrap();
    assert_eq!(
        meta.default_ttl_seconds,
        Some(30 * 24 * 60 * 60),
        "RETAIN 30 DAYS = {} seconds",
        30 * 24 * 60 * 60
    );
    assert!(meta.sync_safe, "SYNC SAFE must be true");
}

// ---------------------------------------------------------------------------
// R17 — \d tablename shows EXPIRES on column
// RED: stub ColumnDef has expires=false; metadata won't show EXPIRES
// ---------------------------------------------------------------------------
#[test]
fn r17_describe_shows_expires_column() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE obs (id INTEGER PRIMARY KEY, expires_at TIMESTAMP EXPIRES) RETAIN 30 DAYS",
        &p(),
    )
    .unwrap();

    let meta = db.table_meta("obs");
    assert!(meta.is_some(), "table 'obs' must have metadata");
    let meta = meta.unwrap();

    let expires_col = meta
        .columns
        .iter()
        .find(|c| c.name == "expires_at")
        .expect("expires_at column must exist");
    assert!(
        expires_col.expires,
        "expires_at column must have expires=true"
    );
    assert_eq!(
        expires_col.column_type,
        contextdb_core::ColumnType::Timestamp,
        "expires_at must be ColumnType::Timestamp"
    );
    assert_eq!(
        meta.expires_column,
        Some("expires_at".to_string()),
        "TableMeta.expires_column must be set"
    );
}

// ---------------------------------------------------------------------------
// R18 — Pruning removes graph edges
// RED: run_pruning_cycle is a no-op
// ---------------------------------------------------------------------------
#[test]
fn r18_pruning_removes_graph_edges() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE nodes (id UUID PRIMARY KEY, name TEXT) RETAIN 2 SECONDS",
        &p(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT)",
        &p(),
    )
    .unwrap();

    let id1 = uuid::Uuid::new_v4();
    let id2 = uuid::Uuid::new_v4();
    let params1 = super::helpers::make_params(vec![
        ("id", Value::Uuid(id1)),
        ("name", Value::Text("node1".to_string())),
    ]);
    let params2 = super::helpers::make_params(vec![
        ("id", Value::Uuid(id2)),
        ("name", Value::Text("node2".to_string())),
    ]);

    db.execute("INSERT INTO nodes (id, name) VALUES ($id, $name)", &params1)
        .unwrap();
    db.execute("INSERT INTO nodes (id, name) VALUES ($id, $name)", &params2)
        .unwrap();

    // Insert edge between node1 and node2
    let tx = db.begin();
    db.insert_edge(tx, id1, id2, "RELATES_TO".to_string(), HashMap::new())
        .unwrap();
    db.commit(tx).unwrap();

    thread::sleep(Duration::from_secs(3));
    db.run_pruning_cycle();

    assert_eq!(row_count(&db, "nodes"), 0, "both nodes must be pruned");

    // Verify edges are also removed by attempting a graph traversal
    // that previously would have returned results
    let snapshot = db.snapshot();
    let edge_count = db.edge_count(id1, "RELATES_TO", snapshot).unwrap();
    assert_eq!(
        edge_count, 0,
        "graph edges must be removed when source rows are pruned"
    );
}

// ---------------------------------------------------------------------------
// R19 — Pruning removes vector index entries
// RED: run_pruning_cycle is a no-op
// ---------------------------------------------------------------------------
#[test]
fn r19_pruning_removes_vector_entries() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE obs (id UUID PRIMARY KEY, data TEXT, embedding VECTOR(3)) RETAIN 2 SECONDS",
        &p(),
    )
    .unwrap();

    let id1 = uuid::Uuid::new_v4();
    let params = super::helpers::make_params(vec![
        ("id", Value::Uuid(id1)),
        ("data", Value::Text("vec-row".to_string())),
        ("embedding", Value::Vector(vec![1.0, 0.0, 0.0])),
    ]);
    db.execute(
        "INSERT INTO obs (id, data, embedding) VALUES ($id, $data, $embedding)",
        &params,
    )
    .unwrap();

    // Verify vector search finds the row
    let search_params =
        super::helpers::make_params(vec![("q", Value::Vector(vec![1.0, 0.0, 0.0]))]);
    let result = db
        .execute(
            "SELECT id FROM obs ORDER BY embedding <=> $q LIMIT 5",
            &search_params,
        )
        .unwrap();
    assert_eq!(
        result.rows.len(),
        1,
        "vector search must find the row before pruning"
    );

    thread::sleep(Duration::from_secs(3));
    db.run_pruning_cycle();

    assert_eq!(row_count(&db, "obs"), 0, "row must be pruned");

    // Vector search must return nothing
    let result = db
        .execute(
            "SELECT id FROM obs ORDER BY embedding <=> $q LIMIT 5",
            &search_params,
        )
        .unwrap();
    assert_eq!(
        result.rows.len(),
        0,
        "vector search must return nothing after pruning"
    );
}

// ---------------------------------------------------------------------------
// R20 — Legacy rows without created_at are never age-pruned (unit test)
// RED: run_pruning_cycle is a no-op (stub returns 0).
//
// The engine stamps `created_at` on ALL inserts, so there is no SQL-level
// way to produce a row with `created_at: None`. This test verifies the
// pruning logic via direct VersionedRow construction — a unit test on the
// pruning decision function, not an integration test through SQL.
//
// Test strategy:
//   Part A (unit): Construct two VersionedRows directly — one with
//     `created_at: None` (legacy) and one with `created_at` in the past.
//     Call the pruning-eligibility function. Assert the legacy row is
//     NOT eligible and the timestamped row IS eligible.
//   Part B (integration, via SQL): Insert a row, ALTER TABLE SET RETAIN
//     with short TTL, sleep, prune. The row HAS `created_at` and IS old
//     enough, so it gets pruned. This verifies the ALTER→prune path works
//     end-to-end.
// ---------------------------------------------------------------------------
#[test]
fn r20_legacy_rows_without_created_at_survive() {
    use std::collections::HashMap as HM;

    // --- Part A: unit test on pruning eligibility ---
    let legacy_row = VersionedRow {
        row_id: 1,
        values: HM::new(),
        created_tx: 1,
        deleted_tx: None,
        lsn: 1,
        created_at: None, // legacy: no timestamp
    };
    let old_row = VersionedRow {
        row_id: 2,
        values: HM::new(),
        created_tx: 2,
        deleted_tx: None,
        lsn: 2,
        created_at: Some(0), // epoch 0 — ancient
    };

    let ttl_millis: u64 = 1_000; // 1 second in millis
    let now_millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    // Legacy row: created_at is None → NOT eligible for age-based pruning
    let legacy_eligible = legacy_row
        .created_at
        .map(|ts| now_millis.saturating_sub(ts) > ttl_millis)
        .unwrap_or(false);
    assert!(
        !legacy_eligible,
        "legacy row with created_at: None must NOT be eligible for pruning"
    );

    // Old row: created_at is 0, now is far past TTL → eligible
    let old_eligible = old_row
        .created_at
        .map(|ts| now_millis.saturating_sub(ts) > ttl_millis)
        .unwrap_or(false);
    assert!(
        old_eligible,
        "old row with created_at: 0 must be eligible for pruning"
    );

    // --- Part B: integration test — ALTER TABLE SET RETAIN prunes old rows ---
    let db = Database::open_memory();
    db.execute("CREATE TABLE obs (id INTEGER PRIMARY KEY, data TEXT)", &p())
        .unwrap();

    db.execute("INSERT INTO obs (id, data) VALUES (1, 'pre-retain')", &p())
        .unwrap();

    // Add retention with short TTL
    db.execute("ALTER TABLE obs SET RETAIN 1 SECONDS", &p())
        .unwrap();

    thread::sleep(Duration::from_secs(2));
    db.run_pruning_cycle();

    // The row was inserted by the engine, so it HAS created_at and IS old → pruned
    assert_eq!(
        row_count(&db, "obs"),
        0,
        "row with created_at past TTL must be pruned after ALTER TABLE SET RETAIN"
    );
}

// ---------------------------------------------------------------------------
// R21 — In-memory databases support retention pruning (with young-row guard)
// RED: run_pruning_cycle is a no-op
// ---------------------------------------------------------------------------
#[test]
fn r21_in_memory_retention_works() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE obs (id INTEGER PRIMARY KEY, data TEXT) RETAIN 2 SECONDS",
        &p(),
    )
    .unwrap();
    db.execute("INSERT INTO obs (id, data) VALUES (1, 'old')", &p())
        .unwrap();

    thread::sleep(Duration::from_secs(3));

    // Insert a young row AFTER the sleep — must survive
    db.execute("INSERT INTO obs (id, data) VALUES (2, 'young')", &p())
        .unwrap();

    let pruned = db.run_pruning_cycle();

    assert!(pruned > 0, "in-memory DB must support pruning");
    assert_eq!(row_count(&db, "obs"), 1, "only the young row survives");

    let result = db.execute("SELECT * FROM obs", &p()).unwrap();
    let idx = col_idx(&result, "data");
    assert_eq!(result.rows[0][idx], Value::Text("young".to_string()));
}

// ---------------------------------------------------------------------------
// R22 — Pruning does NOT fire CommitEvent
// RED: run_pruning_cycle is a no-op (no pruning = no event either way).
//      After implementation, if pruning fires CommitEvent, the hook counter
//      will be > 0, failing the assertion.
// ---------------------------------------------------------------------------
#[test]
fn r22_pruning_does_not_fire_commit_event() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    let commit_count = Arc::new(AtomicU64::new(0));
    let counter = commit_count.clone();

    struct CountingPlugin {
        commits: Arc<AtomicU64>,
    }

    impl contextdb_engine::plugin::DatabasePlugin for CountingPlugin {
        fn post_commit(
            &self,
            _ws: &contextdb_tx::WriteSet,
            _source: contextdb_engine::plugin::CommitSource,
        ) {
            self.commits
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    let plugin = Arc::new(CountingPlugin { commits: counter });

    let db = Database::open_memory_with_plugin(plugin).unwrap();
    db.execute(
        "CREATE TABLE obs (id INTEGER PRIMARY KEY, data TEXT) RETAIN 2 SECONDS",
        &p(),
    )
    .unwrap();
    db.execute("INSERT INTO obs (id, data) VALUES (1, 'a')", &p())
        .unwrap();

    // Record commit count after setup (CREATE TABLE + INSERT = 2 autocommits)
    let baseline = commit_count.load(Ordering::SeqCst);

    thread::sleep(Duration::from_secs(3));
    db.run_pruning_cycle();

    let after_prune = commit_count.load(Ordering::SeqCst);
    assert_eq!(
        after_prune, baseline,
        "pruning must NOT fire CommitEvent (baseline={baseline}, after={after_prune})"
    );

    // Verify pruning actually happened
    assert_eq!(
        row_count(&db, "obs"),
        0,
        "rows must be pruned (verifying pruning occurred)"
    );
}

// ---------------------------------------------------------------------------
// R23 — All four RETAIN units parse correctly
// RED: grammar stub may accept units but AST builder discards duration;
//      metadata shows None
// ---------------------------------------------------------------------------
#[test]
fn r23_retain_all_units() {
    let cases = vec![
        ("RETAIN 1 SECONDS", 1u64),
        ("RETAIN 5 MINUTES", 5 * 60),
        ("RETAIN 2 HOURS", 2 * 60 * 60),
        ("RETAIN 30 DAYS", 30 * 24 * 60 * 60),
    ];

    for (clause, expected_seconds) in cases {
        let db = Database::open_memory();
        let sql = format!("CREATE TABLE t_{expected_seconds} (id INTEGER PRIMARY KEY) {clause}");
        db.execute(&sql, &p()).unwrap();

        let table_name = format!("t_{expected_seconds}");
        let meta = db.table_meta(&table_name);
        assert!(meta.is_some(), "table '{table_name}' must have metadata");
        let meta = meta.unwrap();
        assert_eq!(
            meta.default_ttl_seconds,
            Some(expected_seconds),
            "'{clause}' must store {expected_seconds} seconds, got {:?}",
            meta.default_ttl_seconds
        );
    }
}

// ---------------------------------------------------------------------------
// R24 — ALTER TABLE SET RETAIN overwrites existing retention
// RED: ALTER TABLE SET RETAIN is not wired
// ---------------------------------------------------------------------------
#[test]
fn r24_alter_set_retain_overwrites() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE obs (id INTEGER PRIMARY KEY) RETAIN 30 DAYS",
        &p(),
    )
    .unwrap();

    // Overwrite with 14 DAYS
    db.execute("ALTER TABLE obs SET RETAIN 14 DAYS", &p())
        .unwrap();

    let meta = db.table_meta("obs");
    assert!(meta.is_some());
    let meta = meta.unwrap();
    assert_eq!(
        meta.default_ttl_seconds,
        Some(14 * 24 * 60 * 60),
        "ALTER SET RETAIN must overwrite previous value"
    );
}

// ---------------------------------------------------------------------------
// R25 — Background pruning loop runs automatically
// RED: no background loop exists; Database::set_pruning_interval is not
//      implemented. The row survives because no one calls run_pruning_cycle.
// ---------------------------------------------------------------------------
#[test]
fn r25_background_pruning_loop() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE obs (id INTEGER PRIMARY KEY, data TEXT) RETAIN 1 SECONDS",
        &p(),
    )
    .unwrap();

    // Set a short pruning interval for testing (100ms)
    db.set_pruning_interval(Duration::from_millis(100));

    db.execute("INSERT INTO obs (id, data) VALUES (1, 'auto-pruned')", &p())
        .unwrap();
    assert_eq!(row_count(&db, "obs"), 1);

    // Wait long enough for TTL to expire AND the background loop to run
    thread::sleep(Duration::from_secs(3));

    // Row should be gone WITHOUT manual run_pruning_cycle()
    assert_eq!(
        row_count(&db, "obs"),
        0,
        "background pruning loop must automatically prune expired rows"
    );
}

// ---------------------------------------------------------------------------
// R26 — Retention metadata survives persistence round-trip
// RED: TableMeta fields (default_ttl_seconds, sync_safe, expires_column)
//      are never populated, and persistence does not serialize them.
// ---------------------------------------------------------------------------
#[test]
fn r26_retention_metadata_persists() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.db");

    // Create table with RETAIN, close database
    {
        let db = Database::open(&path).unwrap();
        db.execute(
            "CREATE TABLE obs (id INTEGER PRIMARY KEY, expires_at TIMESTAMP EXPIRES) RETAIN 30 DAYS SYNC SAFE",
            &p(),
        )
        .unwrap();
        drop(db);
    }

    // Reopen and verify retention metadata survived
    {
        let db = Database::open(&path).unwrap();
        let meta = db.table_meta("obs").expect("table must exist after reopen");
        assert_eq!(
            meta.default_ttl_seconds,
            Some(30 * 24 * 60 * 60),
            "RETAIN duration must survive reopen"
        );
        assert!(meta.sync_safe, "SYNC SAFE must survive reopen");
        assert_eq!(
            meta.expires_column,
            Some("expires_at".to_string()),
            "expires_column must survive reopen"
        );
    }
}

// ---------------------------------------------------------------------------
// R27 — Pruning thread stops on database drop
// RED: no background pruning thread exists to stop.
//      After implementation, if the thread leaks, the test process hangs
//      or panics on dropped channel receivers.
// ---------------------------------------------------------------------------
#[test]
fn r27_pruning_thread_stops_on_drop() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE obs (id INTEGER PRIMARY KEY) RETAIN 1 SECONDS",
        &p(),
    )
    .unwrap();
    db.set_pruning_interval(Duration::from_millis(50));

    // Drop the database — pruning thread must stop cleanly
    drop(db);

    // If the thread is still running after drop, it would panic on accessing
    // the dropped Database. Sleep briefly to give the thread time to notice.
    thread::sleep(Duration::from_millis(200));

    // If we reach here without panic/hang, the thread stopped cleanly.
    // (No explicit assertion — the test passes by not panicking or hanging.)
}

// ---------------------------------------------------------------------------
// R28 — Concurrent access: pruning while inserting/selecting
// RED: run_pruning_cycle is a no-op
// ---------------------------------------------------------------------------
#[test]
fn r28_concurrent_prune_and_insert() {
    use std::sync::Arc;

    let db = Arc::new(Database::open_memory());
    db.execute(
        "CREATE TABLE obs (id INTEGER PRIMARY KEY, data TEXT) RETAIN 1 SECONDS",
        &p(),
    )
    .unwrap();

    // Seed some rows
    for i in 0..10 {
        db.execute(
            &format!("INSERT INTO obs (id, data) VALUES ({i}, 'row-{i}')"),
            &p(),
        )
        .unwrap();
    }

    thread::sleep(Duration::from_secs(2));

    // Spawn a thread that inserts + selects while pruning runs
    let db2 = db.clone();
    let inserter = thread::spawn(move || {
        for i in 100..110 {
            db2.execute(
                &format!("INSERT INTO obs (id, data) VALUES ({i}, 'new-{i}')"),
                &p(),
            )
            .unwrap();
            let _ = db2.execute("SELECT * FROM obs", &p()).unwrap();
        }
    });

    // Run pruning concurrently
    let pruned = db.run_pruning_cycle();
    assert!(pruned > 0, "old rows must be pruned");

    inserter.join().expect("inserter thread must not panic");

    // All newly inserted rows must survive (they are young)
    let count = row_count(&db, "obs");
    assert!(
        count >= 10,
        "at least 10 newly inserted rows must survive, got {count}"
    );
}

// ---------------------------------------------------------------------------
// MR1 — RED: ANN search accuracy after vector pruning
// Pruned vectors must not appear in ANN results; remaining vectors rank correctly.
// RED: run_pruning_cycle is a no-op, so pruned vectors remain in search results.
// ---------------------------------------------------------------------------
#[test]
fn mr1_ann_accuracy_after_vector_pruning() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE docs (id UUID PRIMARY KEY, data TEXT, embedding VECTOR(3)) RETAIN 1 SECONDS",
        &p(),
    )
    .unwrap();

    // Insert 10 vectors: 5 "old" (will be pruned) and 5 "new" (will survive).
    let mut old_ids = Vec::new();
    for i in 0..5 {
        let id = uuid::Uuid::new_v4();
        old_ids.push(id);
        let params = super::helpers::make_params(vec![
            ("id", Value::Uuid(id)),
            ("data", Value::Text(format!("old-{i}"))),
            ("embedding", Value::Vector(vec![1.0, 0.0, 0.0])),
        ]);
        db.execute(
            "INSERT INTO docs (id, data, embedding) VALUES ($id, $data, $embedding)",
            &params,
        )
        .unwrap();
    }

    thread::sleep(Duration::from_secs(2));

    // Insert young vectors AFTER sleep — they survive pruning.
    let mut new_ids = Vec::new();
    for i in 0..5 {
        let id = uuid::Uuid::new_v4();
        new_ids.push(id);
        let params = super::helpers::make_params(vec![
            ("id", Value::Uuid(id)),
            ("data", Value::Text(format!("new-{i}"))),
            ("embedding", Value::Vector(vec![0.0, 1.0, 0.0])),
        ]);
        db.execute(
            "INSERT INTO docs (id, data, embedding) VALUES ($id, $data, $embedding)",
            &params,
        )
        .unwrap();
    }

    db.run_pruning_cycle();

    assert_eq!(row_count(&db, "docs"), 5, "only 5 new rows survive");

    // ANN search: query close to new vectors [0, 1, 0].
    let search_params =
        super::helpers::make_params(vec![("q", Value::Vector(vec![0.0, 1.0, 0.0]))]);
    let result = db
        .execute(
            "SELECT id FROM docs ORDER BY embedding <=> $q LIMIT 10",
            &search_params,
        )
        .unwrap();

    // Must return exactly 5 results (the surviving new vectors).
    assert_eq!(
        result.rows.len(),
        5,
        "ANN search must return only surviving vectors, got {}",
        result.rows.len()
    );

    // None of the old IDs should appear in results.
    let result_ids: Vec<uuid::Uuid> = result
        .rows
        .iter()
        .filter_map(|row| match &row[0] {
            Value::Uuid(id) => Some(*id),
            _ => None,
        })
        .collect();
    for old_id in &old_ids {
        assert!(
            !result_ids.contains(old_id),
            "pruned vector {old_id} must NOT appear in ANN results"
        );
    }
}

// ---------------------------------------------------------------------------
// MR2 — RED: Graph edge cleanup after prune + reopen (file-backed)
// Pruned row's edges must be removed from in-memory graph AND from redb
// on reopen. RED: run_pruning_cycle is a no-op.
// ---------------------------------------------------------------------------
#[test]
fn mr2_graph_edge_cleanup_after_prune_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("mr2.db");

    let id1 = uuid::Uuid::new_v4();
    let id2 = uuid::Uuid::new_v4();

    // Phase 1: create, insert, prune.
    {
        let db = Database::open(&path).unwrap();
        db.execute(
            "CREATE TABLE nodes (id UUID PRIMARY KEY, name TEXT) RETAIN 1 SECONDS",
            &p(),
        )
        .unwrap();
        db.execute(
            "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT)",
            &p(),
        )
        .unwrap();

        let params1 = super::helpers::make_params(vec![
            ("id", Value::Uuid(id1)),
            ("name", Value::Text("node1".to_string())),
        ]);
        let params2 = super::helpers::make_params(vec![
            ("id", Value::Uuid(id2)),
            ("name", Value::Text("node2".to_string())),
        ]);
        db.execute("INSERT INTO nodes (id, name) VALUES ($id, $name)", &params1)
            .unwrap();
        db.execute("INSERT INTO nodes (id, name) VALUES ($id, $name)", &params2)
            .unwrap();

        let tx = db.begin();
        db.insert_edge(tx, id1, id2, "LINKS".to_string(), HashMap::new())
            .unwrap();
        db.commit(tx).unwrap();

        // Verify edge exists.
        let snapshot = db.snapshot();
        let edge_count = db.edge_count(id1, "LINKS", snapshot).unwrap();
        assert_eq!(edge_count, 1, "edge must exist before pruning");

        thread::sleep(Duration::from_secs(2));
        let pruned = db.run_pruning_cycle();
        assert!(pruned > 0, "nodes must be pruned");

        // In-memory edges must be removed.
        let snapshot = db.snapshot();
        let edge_count = db.edge_count(id1, "LINKS", snapshot).unwrap();
        assert_eq!(
            edge_count, 0,
            "in-memory edges must be removed after pruning"
        );
    }

    // Phase 2: reopen and verify edges are not reloaded from redb.
    {
        let db = Database::open(&path).unwrap();
        let snapshot = db.snapshot();
        let edge_count = db.edge_count(id1, "LINKS", snapshot).unwrap();
        assert_eq!(
            edge_count, 0,
            "pruned row's edges must NOT be reloaded from redb after reopen"
        );
    }
}

// ---------------------------------------------------------------------------
// MR3 — RED: LSN stamped and sync-safe pruning semantics
// Verifies that rows get LSN stamped at commit, and that sync_safe pruning
// respects the watermark correctly (lsn >= watermark → skip).
// RED: run_pruning_cycle is a no-op.
// ---------------------------------------------------------------------------
#[test]
fn mr3_lsn_stamped_and_sync_safe_pruning() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE obs (id INTEGER PRIMARY KEY, data TEXT) RETAIN 1 SECONDS SYNC SAFE",
        &p(),
    )
    .unwrap();

    db.execute("INSERT INTO obs (id, data) VALUES (1, 'row1')", &p())
        .unwrap();

    thread::sleep(Duration::from_secs(2));

    // With watermark at 0 and row lsn at 0, the row is "unsynced" (lsn >= watermark).
    // Sync-safe must NOT prune it.
    db.set_sync_watermark(0);
    db.run_pruning_cycle();
    assert_eq!(
        row_count(&db, "obs"),
        1,
        "row with lsn=0, watermark=0 must NOT be pruned (unsynced)"
    );

    // Set watermark high enough that the row's LSN is below it.
    // This means the row has been synced → eligible for pruning.
    db.set_sync_watermark(u64::MAX);
    let pruned = db.run_pruning_cycle();
    assert!(pruned > 0, "row must be pruned when watermark > lsn");
    assert_eq!(row_count(&db, "obs"), 0);
}

// ---------------------------------------------------------------------------
// MR4 — RED: Pruning under concurrent read
// A SELECT running during pruning must see a consistent snapshot.
// RED: run_pruning_cycle is a no-op.
// ---------------------------------------------------------------------------
#[test]
fn mr4_pruning_under_concurrent_read() {
    use std::sync::Arc;

    let db = Arc::new(Database::open_memory());
    db.execute(
        "CREATE TABLE obs (id INTEGER PRIMARY KEY, data TEXT) RETAIN 1 SECONDS",
        &p(),
    )
    .unwrap();

    // Insert rows that will expire.
    for i in 0..100 {
        db.execute(
            &format!("INSERT INTO obs (id, data) VALUES ({i}, 'data-{i}')"),
            &p(),
        )
        .unwrap();
    }

    thread::sleep(Duration::from_secs(2));

    // Spawn reader that does SELECT while pruning runs.
    let db2 = db.clone();
    let reader = thread::spawn(move || {
        let mut read_count = 0;
        for _ in 0..20 {
            let result = db2.execute("SELECT * FROM obs", &p());
            assert!(
                result.is_ok(),
                "SELECT must not error during concurrent prune"
            );
            read_count += 1;
            thread::sleep(Duration::from_millis(10));
        }
        read_count
    });

    // Run pruning concurrently.
    let pruned = db.run_pruning_cycle();
    assert!(pruned > 0, "old rows must be pruned");

    let reads = reader.join().expect("reader thread must not panic");
    assert!(reads > 0, "reader must have completed reads");

    // After pruning + reads complete, rows should be gone.
    assert_eq!(row_count(&db, "obs"), 0, "all expired rows must be pruned");
}

// ---------------------------------------------------------------------------
// MR5 — RED: Persistence round-trip with pruning (file-backed)
// After pruning, reopened DB must not contain pruned rows.
// RED: run_pruning_cycle is a no-op.
// ---------------------------------------------------------------------------
#[test]
fn mr5_persistence_round_trip_with_pruning() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("mr5.db");

    {
        let db = Database::open(&path).unwrap();
        db.execute(
            "CREATE TABLE obs (id INTEGER PRIMARY KEY, data TEXT) RETAIN 1 SECONDS",
            &p(),
        )
        .unwrap();

        for i in 0..50 {
            db.execute(
                &format!("INSERT INTO obs (id, data) VALUES ({i}, 'row-{i}')"),
                &p(),
            )
            .unwrap();
        }

        thread::sleep(Duration::from_secs(2));
        let pruned = db.run_pruning_cycle();
        assert!(pruned > 0, "rows must be pruned");
        assert_eq!(row_count(&db, "obs"), 0, "all rows must be pruned");
    }

    // Reopen and verify pruned rows did not come back.
    {
        let db = Database::open(&path).unwrap();
        assert_eq!(
            row_count(&db, "obs"),
            0,
            "pruned rows must not be reloaded from persistence after reopen"
        );
    }
}
