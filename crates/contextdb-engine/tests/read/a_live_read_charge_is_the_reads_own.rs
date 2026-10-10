//! A live session's charge is its reads' own (CDB-144). The database's shared
//! accountant also carries writer state and background-maintenance
//! reservations whose size depends on timing; a read never reports those.

use contextdb_core::Value;
use contextdb_core::read_contract::ReadLimits;
use contextdb_engine::Database;
use std::collections::HashMap;

const QUERY: &str = "SELECT id, payload FROM charged_rows";

fn insert(db: &Database, id: i64) {
    db.execute(
        "INSERT INTO charged_rows VALUES ($id, $payload)",
        &HashMap::from([
            ("id".into(), Value::Int64(id)),
            ("payload".into(), Value::Text("x".repeat(16 * 1024))),
        ]),
    )
    .expect("insert row");
}

fn limits() -> ReadLimits {
    ReadLimits {
        cursor_page_rows: 1,
        ..ReadLimits::default()
    }
}

/// (charge before, charge with one cursor open, charge after close).
fn one_cursor_charge(db: &Database) -> (u64, u64, u64) {
    let reader = db.read_session(limits()).expect("live read session");
    let baseline = reader.charged_bytes().expect("baseline");
    let cursor = reader.open_cursor(QUERY, &HashMap::new()).expect("cursor");
    assert!(cursor.first_page().has_more, "three rows exceed one page");
    let held = reader.charged_bytes().expect("held");
    cursor.close().expect("close cursor");
    let after = reader.charged_bytes().expect("after");
    (baseline, held, after)
}

#[test]
fn a_live_read_reports_its_own_charge_unmoved_by_writes_and_identical_across_reopen() {
    let root = tempfile::tempdir().expect("task-scoped store directory");
    let path = root.path().join("live-read-charge.db");
    let first = {
        let db = Database::open(&path).expect("open writer");
        db.execute(
            "CREATE TABLE charged_rows (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)",
            &HashMap::new(),
        )
        .expect("create rows");
        for id in 1..=3 {
            insert(&db, id);
        }
        let (baseline, held, after) = one_cursor_charge(&db);
        assert!(held > baseline, "hydrated rows charge the read that holds them");
        assert_eq!(after, baseline, "closing the cursor returns its charge");

        // Writer state grows the shared accountant; the read's charge does not move.
        let reader = db.read_session(limits()).expect("live read session");
        let cursor = reader.open_cursor(QUERY, &HashMap::new()).expect("cursor");
        let before_write = reader.charged_bytes().expect("before write");
        insert(&db, 4);
        assert_eq!(
            reader.charged_bytes().expect("after write"),
            before_write,
            "a writer commit is not the read's charge"
        );
        cursor.close().expect("close cursor");
        db.close().expect("close writer");
        held - baseline
    };
    let db = Database::open(&path).expect("reopen writer");
    db.execute("DELETE FROM charged_rows WHERE id = 4", &HashMap::new())
        .expect("restore the three-row table");
    let (baseline, held, after) = one_cursor_charge(&db);
    assert_eq!(held - baseline, first, "the same read reports the same charge after reopen");
    assert_eq!(after, baseline);
}
