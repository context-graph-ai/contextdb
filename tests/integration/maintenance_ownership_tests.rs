//! Maintenance ownership: `MaintenancePolicy::EngineOwned` (the default) is
//! what every database gets until a caller asks otherwise -- the engine
//! spawns its own background thread the moment anything is declared.
//! `MaintenancePolicy::CallerDriven` hands the schedule to the host: the
//! engine spawns ZERO threads for this database, however much is declared,
//! and the caller is expected to drive `run_maintenance_cycle` itself.
//!
//! Discipline: no sleeps, no elapsed-time assertions, no raw clock reads.

use contextdb_core::Value;
use contextdb_engine::{Database, MaintenancePolicy};
use std::collections::HashMap;

fn p() -> HashMap<String, Value> {
    HashMap::new()
}

fn create_retained_table(db: &Database, name: &str) {
    db.execute(
        &format!("CREATE TABLE {name} (id INTEGER PRIMARY KEY) RETAIN 1 HOURS"),
        &p(),
    )
    .unwrap_or_else(|err| panic!("retained table {name} must create: {err}"));
}

/// A fresh database is `EngineOwned` -- the default every consumer gets
/// without asking.
#[test]
fn a_fresh_database_defaults_to_engine_owned_maintenance() {
    let db = Database::open_memory();
    assert_eq!(db.maintenance_policy(), MaintenancePolicy::EngineOwned);
    assert_eq!(
        db.maintenance_status().policy,
        MaintenancePolicy::EngineOwned
    );
}

/// `CallerDriven` spawns zero threads, however much is declared -- declaring
/// a retained table AFTER switching to `CallerDriven` must not start one.
#[test]
fn caller_driven_spawns_no_thread_however_much_is_declared() {
    let db = Database::open_memory();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    assert_eq!(db.maintenance_policy(), MaintenancePolicy::CallerDriven);

    create_retained_table(&db, "windows");
    let status = db.maintenance_status();
    assert!(
        status.retention_enabled,
        "the table is still declared retention-eligible: {status:?}"
    );
    assert!(
        !status.running,
        "CallerDriven must spawn zero threads however much is declared: {status:?}"
    );
    assert_eq!(status.active_maintenance_loops, 0);

    // The caller can still drive a cycle itself.
    db.run_maintenance_cycle()
        .expect("a caller-driven database must still accept an explicit cycle call");
}

/// Switching an already-running `EngineOwned` database to `CallerDriven`
/// stops its thread; switching back starts it again if still eligible.
#[test]
fn switching_maintenance_policy_stops_and_restarts_the_thread() {
    let db = Database::open_memory();
    create_retained_table(&db, "windows");
    assert!(
        db.maintenance_status().running,
        "EngineOwned starts a thread the moment something is declared"
    );

    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    assert!(
        !db.maintenance_status().running,
        "switching to CallerDriven must stop the running thread"
    );

    db.set_maintenance_policy(MaintenancePolicy::EngineOwned);
    assert!(
        db.maintenance_status().running,
        "switching back to EngineOwned must restart the thread since the table is still \
         declared"
    );
}

/// A database with nothing declared stays thread-free under either policy.
#[test]
fn caller_driven_on_a_database_with_nothing_declared_stays_thread_free() {
    let db = Database::open_memory();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    assert!(!db.maintenance_status().running);
    db.set_maintenance_policy(MaintenancePolicy::EngineOwned);
    assert!(
        !db.maintenance_status().running,
        "nothing declared means no thread under EngineOwned either"
    );
}
