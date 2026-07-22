use contextdb_core::Value;
use contextdb_engine::Database;
use std::collections::HashMap;

fn p() -> HashMap<String, Value> {
    HashMap::new()
}

// ---------------------------------------------------------------------------
// A-RT1 — CREATE TABLE with RETAIN parses and table is queryable
// ---------------------------------------------------------------------------
#[test]
fn a_rt1_create_table_retain_parses() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE logs (id INTEGER PRIMARY KEY, msg TEXT) RETAIN 7 DAYS",
        &p(),
    )
    .unwrap();
    db.execute("INSERT INTO logs (id, msg) VALUES (1, 'hello')", &p())
        .unwrap();
    let result = db.execute("SELECT * FROM logs", &p()).unwrap();
    assert_eq!(result.rows.len(), 1);
}

// ---------------------------------------------------------------------------
// A-RT2 — Rows disappear after their TTL expires
// ---------------------------------------------------------------------------
#[test]
fn a_rt2_rows_disappear_after_ttl() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

    use contextdb_core::Wallclock;

    let mock_now = Arc::new(AtomicU64::new(1_000_000));
    let _clock = {
        let mock_now = Arc::clone(&mock_now);
        Wallclock::test_clock_guard(move || mock_now.load(AtomicOrdering::SeqCst))
    };

    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE logs (id INTEGER PRIMARY KEY, msg TEXT) RETAIN 1 SECONDS",
        &p(),
    )
    .unwrap();
    db.execute("INSERT INTO logs (id, msg) VALUES (1, 'old')", &p())
        .unwrap();
    // Advance 2 s past the old row's stamped time — beyond the 1 s TTL.
    mock_now.fetch_add(2_000, AtomicOrdering::SeqCst);
    db.execute("INSERT INTO logs (id, msg) VALUES (2, 'new')", &p())
        .unwrap();
    db.run_pruning_cycle();
    let result = db.execute("SELECT * FROM logs", &p()).unwrap();
    assert_eq!(result.rows.len(), 1, "only the new row survives");
}

// ---------------------------------------------------------------------------
// A-RT3 — ALTER TABLE SET RETAIN adds retention to existing table
// ---------------------------------------------------------------------------
#[test]
fn a_rt3_alter_table_set_retain() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

    use contextdb_core::Wallclock;

    let mock_now = Arc::new(AtomicU64::new(1_000_000));
    let _clock = {
        let mock_now = Arc::clone(&mock_now);
        Wallclock::test_clock_guard(move || mock_now.load(AtomicOrdering::SeqCst))
    };

    let db = Database::open_memory();
    db.execute("CREATE TABLE logs (id INTEGER PRIMARY KEY, msg TEXT)", &p())
        .unwrap();
    db.execute("INSERT INTO logs (id, msg) VALUES (1, 'a')", &p())
        .unwrap();
    db.execute("ALTER TABLE logs SET RETAIN 1 SECONDS", &p())
        .unwrap();
    // Advance 2 s past the row's stamped time — beyond the 1 s TTL.
    mock_now.fetch_add(2_000, AtomicOrdering::SeqCst);
    db.run_pruning_cycle();
    assert_eq!(
        db.execute("SELECT * FROM logs", &p()).unwrap().rows.len(),
        0,
    );
}

// ---------------------------------------------------------------------------
// A-RT4 — ALTER TABLE DROP RETAIN stops pruning
// ---------------------------------------------------------------------------
#[test]
fn a_rt4_alter_table_drop_retain() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

    use contextdb_core::Wallclock;

    let mock_now = Arc::new(AtomicU64::new(1_000_000));
    let _clock = {
        let mock_now = Arc::clone(&mock_now);
        Wallclock::test_clock_guard(move || mock_now.load(AtomicOrdering::SeqCst))
    };

    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE logs (id INTEGER PRIMARY KEY, msg TEXT) RETAIN 1 SECONDS",
        &p(),
    )
    .unwrap();
    db.execute("ALTER TABLE logs DROP RETAIN", &p()).unwrap();
    db.execute("INSERT INTO logs (id, msg) VALUES (1, 'a')", &p())
        .unwrap();
    // Advance 2 s past the row's stamped time — the DROP means it must survive anyway.
    mock_now.fetch_add(2_000, AtomicOrdering::SeqCst);
    db.run_pruning_cycle();
    assert_eq!(
        db.execute("SELECT * FROM logs", &p()).unwrap().rows.len(),
        1,
        "row must survive after DROP RETAIN"
    );
}

// ---------------------------------------------------------------------------
// A-RT5 — EXPIRES column overrides table TTL
// ---------------------------------------------------------------------------
#[test]
fn a_rt5_expires_column_override() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE logs (id INTEGER PRIMARY KEY, msg TEXT, exp TIMESTAMP EXPIRES) RETAIN 1000 DAYS",
        &p(),
    )
    .unwrap();
    db.execute(
        "INSERT INTO logs (id, msg, exp) VALUES (1, 'past', '2020-01-01T00:00:00Z')",
        &p(),
    )
    .unwrap();
    db.execute(
        "INSERT INTO logs (id, msg, exp) VALUES (2, 'future', '2099-01-01T00:00:00Z')",
        &p(),
    )
    .unwrap();
    db.run_pruning_cycle();
    let result = db.execute("SELECT * FROM logs", &p()).unwrap();
    assert_eq!(result.rows.len(), 1, "only the future row survives");
}

// ---------------------------------------------------------------------------
// A-RT6 — IMMUTABLE and RETAIN are mutually exclusive
// ---------------------------------------------------------------------------
#[test]
fn a_rt6_immutable_retain_mutual_exclusion() {
    let db = Database::open_memory();
    let result = db.execute(
        "CREATE TABLE logs (id INTEGER PRIMARY KEY) IMMUTABLE RETAIN 7 DAYS",
        &p(),
    );
    assert!(result.is_err(), "IMMUTABLE + RETAIN must error");
}

// ---------------------------------------------------------------------------
// A-RT7 — Retention metadata survives database restart
// ---------------------------------------------------------------------------
#[test]
fn a_rt7_retention_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.db");

    {
        let db = Database::open(&path).unwrap();
        db.execute(
            "CREATE TABLE logs (id INTEGER PRIMARY KEY) RETAIN 14 DAYS",
            &p(),
        )
        .unwrap();
        drop(db);
    }

    let db = Database::open(&path).unwrap();
    let meta = db.table_meta("logs").expect("table must exist");
    assert_eq!(meta.default_ttl_seconds, Some(14 * 24 * 60 * 60));
}
