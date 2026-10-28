//! A reader pinned before writes keeps its answer while maintenance publishes;
//! a second maintenance caller contends on the actual publication exclusion.
use super::support::{
    PublicationCut, drive_until_serving, empty, int_ids, ivf, observe_publication_cuts,
    open_caller_driven, params, serving, unit_vector,
};
use contextdb_core::{SnapshotId, Value};
use contextdb_engine::{Database, database::MaintenancePruningMutexEventForTest};
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;

const DIM: usize = 8;
const WATCHDOG: Duration = Duration::from_secs(60);

fn insert(db: &Database, id: i64, vector_id: u64) {
    db.execute(
        "INSERT INTO items VALUES ($id, $v)",
        &params([
            ("id", Value::Int64(id)),
            ("v", Value::Vector(unit_vector(vector_id, DIM))),
        ]),
    )
    .unwrap();
}
fn ids(db: &Database, snapshot: SnapshotId) -> Vec<i64> {
    let result = db
        .execute_at_snapshot(
            "SELECT id FROM items ORDER BY embedding <=> $v USE VECTOR INDEXED LIMIT 1",
            &params([("v", Value::Vector(unit_vector(7, DIM)))]),
            snapshot,
        )
        .unwrap();
    int_ids(&result)
}

fn complete_rows(db: &Database, snapshot: SnapshotId) -> Result<Vec<i64>, String> {
    let result = db
        .execute_at_snapshot(
            "SELECT id FROM items ORDER BY embedding <=> $v USE VECTOR INDEXED LIMIT 64",
            &params([("v", Value::Vector(unit_vector(7, DIM)))]),
            snapshot,
        )
        .map_err(|error| format!("{error:?}"))?;
    let mut rows = int_ids(&result);
    rows.sort_unstable();
    Ok(rows)
}

#[test]
fn concurrent_maintenance_publishes_one_complete_generation_for_pinned_readers() {
    let root = tempfile::tempdir().unwrap();
    let db = Arc::new(open_caller_driven(&root.path().join("publication.db")));
    db.execute(
        &format!("CREATE TABLE items (id INTEGER PRIMARY KEY, embedding VECTOR({DIM}) SEARCH_MODE INDEXED {} CONSOLIDATION (CHANGE_PERCENT = 10))", ivf(4, 4, 64)),
        &empty(),
    ).unwrap();
    for id in 0..40 {
        insert(&db, id, id as u64);
    }
    drive_until_serving(&db, 40, 1, 10);
    let first = serving(&db, "items", "embedding", &[]).unwrap().generation;
    let old = db.snapshot();
    let pin = db.pin_snapshot(old).unwrap();
    assert_eq!(ids(&db, old), vec![7]);
    db.execute("DELETE FROM items WHERE id = 7", &empty())
        .unwrap();
    for id in 40..48 {
        insert(&db, id, id as u64);
    }
    insert(&db, 1000, 7);
    let newer = db.snapshot();
    assert_eq!(ids(&db, old), vec![7]);
    assert_eq!(ids(&db, newer), vec![1000]);
    let old_expected: Vec<i64> = (0..40).collect();
    let new_expected: Vec<i64> = (0..48).filter(|id| *id != 7).chain([1000]).collect();
    assert_eq!(complete_rows(&db, old).unwrap(), old_expected);
    assert_eq!(complete_rows(&db, newer).unwrap(), new_expected);

    let (reached_tx, reached_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Arc::new(Mutex::new(release_rx));
    let worker_db = db.clone();
    let first_worker = std::thread::spawn(move || {
        observe_publication_cuts(
            Arc::new(move |cut| {
                if cut == PublicationCut::BeforeGenerationPublication {
                    reached_tx.send(()).unwrap();
                    release_rx
                        .lock()
                        .unwrap()
                        .recv_timeout(WATCHDOG)
                        .expect("publication release event was never delivered");
                }
            }),
            || drive_until_serving(&worker_db, 48, 1, 10),
        );
    });
    reached_rx
        .recv_timeout(WATCHDOG)
        .expect("native pre-publication event was never delivered");
    let (attempt_tx, attempt_rx) = mpsc::channel();
    let second_db = db.clone();
    let second_worker = std::thread::spawn(move || {
        Database::observe_maintenance_pruning_mutex_for_test(
            Arc::new(move |event| {
                attempt_tx.send(event).unwrap();
            }),
            || second_db.run_maintenance_cycle(),
        )
    });
    let event = attempt_rx.recv_timeout(WATCHDOG);
    let (answers_tx, answers_rx) = mpsc::channel();
    let reader_db = db.clone();
    let reader = std::thread::spawn(move || {
        answers_tx
            .send((
                complete_rows(&reader_db, old),
                complete_rows(&reader_db, newer),
            ))
            .unwrap();
    });
    let during = answers_rx.recv_timeout(WATCHDOG);
    release_tx.send(()).unwrap();
    first_worker.join().unwrap();
    let second_result = second_worker.join().unwrap().unwrap();
    reader.join().unwrap();
    let (during_old, during_new) =
        during.expect("complete reader answers during publication were never delivered");
    assert_eq!(
        during_old,
        Ok(old_expected.clone()),
        "the old snapshot must return every old row exactly once during competing maintenance"
    );
    assert_eq!(
        during_new,
        Ok(new_expected.clone()),
        "the new snapshot must return every new row exactly once during competing maintenance"
    );
    assert_eq!(
        complete_rows(&db, old),
        Ok(old_expected),
        "the old snapshot remains complete after both maintenance calls"
    );
    assert_eq!(
        complete_rows(&db, newer),
        Ok(new_expected),
        "the new snapshot remains complete after both maintenance calls"
    );
    println!(
        "PUBLICATION_ANSWER_PROMISE_PASS: complete old and new snapshots during and after competing maintenance"
    );
    assert!(
        matches!(
            event.expect("second maintenance acquisition event was never delivered"),
            MaintenancePruningMutexEventForTest::Contended
        ),
        "the second maintenance call must reach the held, real pruning mutex"
    );
    assert!(
        second_result.vector.first_failure.is_none(),
        "second maintenance refused: {second_result:?}"
    );
    let replacement = serving(&db, "items", "embedding", &[]).unwrap().generation;
    assert_ne!(replacement, first);
    assert_eq!(ids(&db, old), vec![7]);
    assert_eq!(ids(&db, newer), vec![1000]);
    assert_eq!(ids(&db, db.snapshot()), vec![1000]);
    drop(pin);
}

#[test]
fn a_paused_native_reader_keeps_its_answer_while_vector_writes_commit() {
    use contextdb_engine::vector_ivf_observations::{
        NativeIvfWorkPoint, with_native_ivf_work_observer_for_test,
    };
    use std::sync::atomic::{AtomicBool, Ordering};
    let root = tempfile::tempdir().unwrap();
    let db = Arc::new(open_caller_driven(&root.path().join("read-write.db")));
    db.execute(
        &format!("CREATE TABLE items (id INTEGER PRIMARY KEY, embedding VECTOR({DIM}) SEARCH_MODE INDEXED {})", ivf(4, 4, 64)),
        &empty(),
    ).unwrap();
    for id in 0..40 {
        insert(&db, id, id as u64);
    }
    drive_until_serving(&db, 40, 1, 10);
    let old = db.snapshot();
    let pin = db.pin_snapshot(old).unwrap();
    let expected = ids(&db, old);
    assert_eq!(expected, vec![7]);
    let (reached_tx, reached_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Arc::new(Mutex::new(release_rx));
    let reader = db.clone();
    let worker = std::thread::spawn(move || {
        let once = AtomicBool::new(false);
        with_native_ivf_work_observer_for_test(
            Arc::new(move |point| {
                if point == NativeIvfWorkPoint::ListPageRead && !once.swap(true, Ordering::SeqCst) {
                    reached_tx.send(()).unwrap();
                    release_rx
                        .lock()
                        .unwrap()
                        .recv_timeout(WATCHDOG)
                        .expect("reader release event was never delivered");
                }
            }),
            || ids(&reader, old),
        )
    });
    reached_rx
        .recv_timeout(WATCHDOG)
        .expect("native list-read event was never delivered");
    db.execute("DELETE FROM items WHERE id = 7", &empty())
        .unwrap();
    insert(&db, 1000, 7);
    release_tx.send(()).unwrap();
    assert_eq!(worker.join().unwrap(), expected);
    assert_eq!(ids(&db, old), expected);
    assert_eq!(ids(&db, db.snapshot()), vec![1000]);
    drop(pin);
}
