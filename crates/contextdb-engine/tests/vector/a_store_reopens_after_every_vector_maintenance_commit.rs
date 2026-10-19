//! A durable store that maintenance has committed to must reopen for writing,
//! whatever the last committed maintenance step was. Every commit that moves a
//! table authenticated by the committed read bounds must refresh that binding,
//! or the next open refuses the store as corrupt.

use contextdb_core::{Value, VectorIndexRef, VectorPartitionKey};
use contextdb_engine::{
    Database, MaintenancePolicy, ReadSession, VectorMaintenancePreparationPhaseForTest,
};
use contextdb_vector::VectorPartitionRef;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use uuid::Uuid;

const ROWS_PER_ROUTE: u128 = 32;
const MAX_CYCLES: usize = 32;

fn params(pairs: impl IntoIterator<Item = (&'static str, Value)>) -> HashMap<String, Value> {
    pairs
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

fn ranked(axis: usize, ordinal: u128) -> Vec<f32> {
    let score = 1.0 - ordinal as f32 * 0.01;
    let remainder = (1.0 - score * score).max(0.0).sqrt();
    let mut vector = vec![0.0; 3];
    vector[axis] = score;
    vector[(axis + 1) % 3] = remainder;
    vector
}

fn open_writable(path: &std::path::Path, step: &str) -> Database {
    let db = Database::open(path)
        .unwrap_or_else(|error| panic!("the store reopens for writing after {step}: {error:?}"));
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db
}

fn insert_route(db: &Database, scope: Uuid, first_id: u128, axis: usize) {
    let tx = db.begin_or_panic();
    for ordinal in 0..ROWS_PER_ROUTE {
        db.insert_row(
            tx,
            "reopen_items",
            params([
                ("id", Value::Uuid(Uuid::from_u128(first_id + ordinal))),
                ("scope_id", Value::Uuid(scope)),
                ("embedding", Value::Vector(ranked(axis, ordinal))),
            ]),
        )
        .expect("stage one route vector");
    }
    db.commit(tx).expect("commit one vector route");
}

fn declare(db: &Database) {
    db.execute(
        "CREATE TABLE reopen_items (\
         id UUID PRIMARY KEY, \
         scope_id UUID NOT NULL, \
         embedding VECTOR(3) PARTITION_KEY (scope_id) MAX_PARTITIONS 8 SEARCH_MODE INDEXED \
             CONSOLIDATION (CHANGE_PERCENT = 1, TOMBSTONE_PERCENT = 1)\
         )",
        &HashMap::new(),
    )
    .expect("declare the maintained partitioned vector table");
}

const CRASH_CHILD_PATH: &str = "CONTEXTDB_REOPEN_CRASH_CHILD_PATH";
const CRASH_CHILD_PHASE: &str = "CONTEXTDB_REOPEN_CRASH_CHILD_PHASE";
const CRASH_MARKER: &str = "reopen-crash-child: phase reached";

const BUILD_PHASES: [VectorMaintenancePreparationPhaseForTest; 9] = [
    VectorMaintenancePreparationPhaseForTest::LoadAuthoritativeRawVectors,
    VectorMaintenancePreparationPhaseForTest::AdmitEncoding,
    VectorMaintenancePreparationPhaseForTest::AdmitCandidate,
    VectorMaintenancePreparationPhaseForTest::CandidateAdmitted,
    VectorMaintenancePreparationPhaseForTest::AdmitPersistence,
    VectorMaintenancePreparationPhaseForTest::PersistenceAdmitted,
    VectorMaintenancePreparationPhaseForTest::EncodeGraph,
    VectorMaintenancePreparationPhaseForTest::HashStateDigest,
    VectorMaintenancePreparationPhaseForTest::PrepareDurableGeneration,
];

fn alpha() -> Uuid {
    Uuid::from_u128(0xA1)
}

fn alpha_route() -> VectorPartitionRef {
    VectorPartitionRef::new(
        VectorIndexRef::new("reopen_items", "embedding"),
        VectorPartitionKey::from_values(&[Value::Uuid(alpha())]).expect("one UUID key"),
    )
}

fn child_test_filter(module_path: &str, fn_name: &str) -> String {
    match module_path.split_once("::") {
        Some((_, rest)) => format!("{rest}::{fn_name}"),
        None => fn_name.to_string(),
    }
}

/// Child half of the crash test: run maintenance to one preparation phase and
/// abort the process there, leaving exactly what the build had committed.
#[test]
fn reopen_crash_child() {
    let (Ok(path), Ok(phase)) = (
        std::env::var(CRASH_CHILD_PATH),
        std::env::var(CRASH_CHILD_PHASE),
    ) else {
        return;
    };
    let phase = BUILD_PHASES[phase.parse::<usize>().expect("phase ordinal")];
    let db = Arc::new(open_writable(std::path::Path::new(&path), "child creation"));
    declare(&db);
    insert_route(&db, alpha(), 0xA000, 0);
    let pause =
        db.__arm_one_shot_vector_maintenance_preparation_pause_for_test(&alpha_route(), phase);
    let worker_db = Arc::clone(&db);
    let worker = std::thread::spawn(move || worker_db.run_maintenance_cycle());
    if pause
        .wait_until_reached_timeout(Duration::from_secs(120))
        .is_ok()
    {
        println!("{CRASH_MARKER}");
        std::process::abort();
    }
    pause.release();
    worker
        .join()
        .expect("maintenance worker joins")
        .expect("maintenance runs when the phase is not on this build's path");
}

/// A crash may land between any two maintenance commits. The store a crash
/// leaves must reopen for writing and for reading.
#[test]
fn a_crash_inside_a_maintenance_build_leaves_a_store_that_reopens_for_writing() {
    let mut crashes = 0;
    for (ordinal, phase) in BUILD_PHASES.iter().enumerate() {
        let root = TempDir::new().expect("temporary durable store directory");
        let path = root.path().join("live.redb");
        let output = std::process::Command::new(
            std::env::current_exe().expect("current integration-test binary"),
        )
        .arg("--exact")
        .arg(child_test_filter(module_path!(), "reopen_crash_child"))
        .arg("--nocapture")
        .env(CRASH_CHILD_PATH, &path)
        .env(CRASH_CHILD_PHASE, ordinal.to_string())
        .output()
        .expect("spawn the crashing maintenance child");
        let stdout = String::from_utf8_lossy(&output.stdout);
        if !stdout.contains(CRASH_MARKER) {
            assert!(
                output.status.success(),
                "a child that never reached {phase:?} completes cleanly:\nstdout:\n{stdout}\nstderr:\n{}",
                String::from_utf8_lossy(&output.stderr)
            );
            continue;
        }
        assert!(!output.status.success(), "the child aborted at {phase:?}");
        crashes += 1;
        let reopened = Database::open(&path).unwrap_or_else(|error| {
            panic!("a store crashed at {phase:?} reopens for writing: {error:?}")
        });
        reopened.close().expect("close the reopened store");
        ReadSession::open(&path).unwrap_or_else(|error| {
            panic!("a read session opens a store crashed at {phase:?}: {error:?}")
        });
    }
    assert!(
        crashes > 0,
        "at least one build phase after the construction floor is reached"
    );
}

#[test]
fn a_store_reopens_for_writing_after_every_vector_maintenance_cycle() {
    let root = TempDir::new().expect("temporary durable store directory");
    let path = root.path().join("reopen-after-maintenance.redb");
    let alpha = Uuid::from_u128(0xA1);
    let bravo = Uuid::from_u128(0xB1);

    let db = open_writable(&path, "creation");
    declare(&db);
    insert_route(&db, alpha, 0xA000, 0);
    insert_route(&db, bravo, 0xB000, 1);
    db.close().expect("close the seeded store");

    // Base builds, then a change round that publishes over the saved base.
    for round in 0..2 {
        for cycle in 0..MAX_CYCLES {
            let step = format!("maintenance round {round} cycle {cycle}");
            let db = open_writable(&path, &step);
            db.run_maintenance_cycle()
                .unwrap_or_else(|error| panic!("{step} runs: {error:?}"));
            db.close()
                .unwrap_or_else(|error| panic!("{step} closes: {error:?}"));
            ReadSession::open(&path).unwrap_or_else(|error| {
                panic!("a read session opens the store after {step}: {error:?}")
            });
        }
        let db = open_writable(&path, &format!("round {round}"));
        db.execute(
            "UPDATE reopen_items SET embedding = $embedding WHERE id = $id",
            &params([
                ("id", Value::Uuid(Uuid::from_u128(0xA000))),
                ("embedding", Value::Vector(ranked(2, 0))),
            ]),
        )
        .expect("commit a change the next round consolidates");
        db.close().expect("close after the change");
    }

    let db = open_writable(&path, "both rounds");
    let rows = db
        .execute(
            "SELECT id FROM reopen_items WHERE scope_id = $scope \
             ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 4",
            &params([
                ("scope", Value::Uuid(bravo)),
                ("query", Value::Vector(ranked(1, 0))),
            ]),
        )
        .expect("the maintained route answers after every reopen")
        .rows;
    assert_eq!(
        rows.first(),
        Some(&vec![Value::Uuid(Uuid::from_u128(0xB000))]),
        "the nearest bravo row leads"
    );
}
