//! The maintenance cycle's reclaim stage is bounded per cycle, and a backlog bigger than one cycle's
//! bound drains across cycles. The seam caps the stage's steps on the calling thread so the bound
//! is reachable without a gigabyte of freed pages.

use contextdb_core::Value;
use contextdb_core::read_contract::ReadLimits;
use contextdb_engine::{
    Database, MaintenancePolicy, reclaim_steps_last_cycle_for_test, set_reclaim_steps_per_cycle_for_test,
};
use std::collections::HashMap;

fn p() -> HashMap<String, Value> {
    HashMap::new()
}

fn backlog(db: &Database) -> (usize, usize) {
    let counts = db.__deferred_free_backlog_for_test().unwrap();
    (counts[0].0 + counts[1].0, counts[0].1 + counts[1].1)
}

/// Restores the uncapped cycle even when an assertion fails.
struct Cap;

impl Cap {
    fn set(steps: usize) -> Cap {
        set_reclaim_steps_per_cycle_for_test(Some(steps));
        Cap
    }
}

impl Drop for Cap {
    fn drop(&mut self) {
        set_reclaim_steps_per_cycle_for_test(None);
    }
}

/// A store whose released snapshot left more than two full 4,096-extent steps of backlog.
fn parked_backlog() -> (tempfile::TempDir, Database) {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open(dir.path().join("cycle-bound.db")).unwrap();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute("CREATE TABLE churn (id INTEGER PRIMARY KEY, payload TEXT)", &p()).unwrap();
    db.execute("CREATE TABLE bulk (id INTEGER PRIMARY KEY, payload TEXT)", &p()).unwrap();
    db.execute("INSERT INTO churn (id, payload) VALUES (0, 'seed')", &p()).unwrap();
    let mut sql = String::from("INSERT INTO bulk (id, payload) VALUES ");
    for i in 0..200 {
        if i > 0 {
            sql.push(',');
        }
        sql.push_str(&format!("({i}, '{}')", "x".repeat(200)));
    }
    db.execute(&sql, &p()).unwrap();
    let mut sql = String::from("INSERT INTO bulk (id, payload) VALUES ");
    for i in 200..400 {
        if i > 200 {
            sql.push(',');
        }
        sql.push_str(&format!("({i}, '{}')", "x".repeat(200)));
    }
    db.execute(&sql, &p()).unwrap();
    let session = db.read_session(ReadLimits::default()).unwrap();
    let cursor = session.open_cursor("SELECT id FROM bulk ORDER BY id", &p()).unwrap();
    assert!(cursor.first_page().has_more, "the cursor must keep a live snapshot");
    for generation in 0..300 {
        db.execute(
            &format!("UPDATE churn SET payload = 'g{generation}{}' WHERE id = 0", "y".repeat(8192)),
            &p(),
        )
        .unwrap();
    }
    drop(cursor);
    drop(session);
    (dir, db)
}

#[test]
fn each_cycle_reports_at_most_its_cap_and_a_larger_backlog_drains_across_cycles() {
    let (_dir, db) = parked_backlog();
    let start = backlog(&db);
    assert!(start.1 > 2 * 4096 + 1_000, "the backlog must need more than two steps: {start:?}");
    let _cap = Cap::set(2);
    let mut previous = start.1;
    let mut cycles_that_hit_the_cap = 0;
    let mut cycles = 0;
    while backlog(&db).1 > 128 {
        cycles += 1;
        assert!(cycles <= 60, "the backlog must drain across capped cycles: {:?} after {cycles} cycles", backlog(&db));
        db.run_maintenance_cycle().unwrap();
        let steps = reclaim_steps_last_cycle_for_test();
        assert!(steps <= 2, "cycle {cycles} reported {steps} reclaim steps against a cap of 2");
        if steps == 2 {
            cycles_that_hit_the_cap += 1;
        }
        let now = backlog(&db).1;
        assert!(now < previous, "cycle {cycles} must make progress: {previous} -> {now}");
        previous = now;
    }
    assert!(cycles >= 2, "a backlog larger than one cycle's bound takes more than one cycle");
    assert!(cycles_that_hit_the_cap >= 1, "the first cycle runs to its cap");
    assert_eq!(
        db.execute("SELECT COUNT(*) FROM bulk", &p()).unwrap().rows,
        vec![vec![Value::Int64(400)]]
    );
}

#[test]
fn without_a_cap_one_cycle_finishes_a_backlog_that_the_cap_split() {
    let (_dir, db) = parked_backlog();
    set_reclaim_steps_per_cycle_for_test(None);
    db.run_maintenance_cycle().unwrap();
    let steps = reclaim_steps_last_cycle_for_test();
    assert!(steps >= 3 && steps <= 64, "an uncapped cycle takes the steps it needs, at most 64: {steps}");
    assert!(backlog(&db).1 <= 128, "{:?}", backlog(&db));
}
