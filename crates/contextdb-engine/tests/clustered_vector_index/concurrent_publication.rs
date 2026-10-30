//! A reader pinned before writes keeps its answer while maintenance publishes;
//! a second maintenance caller contends on the actual publication exclusion.
use super::support::{
    PublicationCut, drive_until_serving, empty, int_ids, ivf, observe_publication_cuts,
    open_caller_driven, params, retained_generations, serving, unit_vector,
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
    let result = db.execute_at_snapshot(
        "SELECT id FROM items ORDER BY embedding <=> $v USE VECTOR INDEXED LIMIT 1",
        &params([("v", Value::Vector(unit_vector(7, DIM)))]),
        snapshot,
    );
    assert!(
        result.is_ok(),
        "a pinned or current snapshot must remain searchable on the indexed route: {:?}",
        result.as_ref().err()
    );
    int_ids(&result.unwrap())
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

    let publications = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let first_publications = publications.clone();
    let (reached_tx, reached_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Arc::new(Mutex::new(release_rx));
    let worker_db = db.clone();
    let first_worker = std::thread::spawn(move || {
        observe_publication_cuts(
            Arc::new(move |cut| {
                if cut == PublicationCut::AfterGenerationPublication {
                    first_publications.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
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
    let second_publications = publications.clone();
    let (done_tx, done_rx) = mpsc::channel();
    let second_worker = std::thread::spawn(move || {
        let result = Database::observe_maintenance_pruning_mutex_for_test(
            Arc::new(move |event| {
                attempt_tx.send(event).unwrap();
            }),
            || {
                observe_publication_cuts(
                    Arc::new(move |cut| {
                        if cut == PublicationCut::AfterGenerationPublication {
                            second_publications.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        }
                    }),
                    || second_db.run_maintenance_cycle(),
                )
            },
        );
        done_tx.send(result).unwrap();
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
    let second_pending = matches!(done_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
    release_tx.send(()).unwrap();
    first_worker.join().unwrap();
    second_worker.join().unwrap();
    let second_result = done_rx
        .recv_timeout(WATCHDOG)
        .expect("second maintenance result was never delivered")
        .unwrap();
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
        second_pending,
        "second maintenance must remain incomplete until the first publication is released"
    );
    assert_eq!(
        publications.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "exactly one new generation publication is observed across the two maintenance calls"
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
    repeated_replacements_return_aggregate_charges_and_bound_pages(&db, first);
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
    println!("READER_STAGE: before writer commit");
    let expected = ids(&db, old);
    assert_eq!(expected, vec![7]);
    let old_expected: Vec<i64> = (0..40).collect();
    let current_expected: Vec<i64> = (0..40).filter(|id| *id != 7).chain([1000]).collect();
    assert_eq!(complete_rows(&db, old), Ok(old_expected.clone()));
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
    println!("READER_STAGE: pinned answer after writer commit");
    assert_eq!(ids(&db, old), expected);
    assert_eq!(
        complete_rows(&db, old),
        Ok(old_expected),
        "the pinned reader sees every original row exactly once"
    );
    println!("READER_STAGE: current answer after writer commit");
    let current = db.snapshot();
    assert_eq!(ids(&db, current), vec![1000]);
    assert_eq!(
        complete_rows(&db, current),
        Ok(current_expected),
        "the current reader sees every committed row exactly once"
    );
    drop(pin);
}

/// Current native RowCopies table entries, counted by actual storage and page key.
/// This does not enumerate retained COW roots or prove physical disk-block release.
fn native_row_copy_pages(db: &Database) -> usize {
    let mut pages = std::collections::BTreeSet::new();
    db.visit_native_ivf_physical_pages_for_test(
        &contextdb_core::VectorIndexRef::new("items", "embedding"),
        &mut |page| {
            if page.key.kind == contextdb_vector::ivf::storage::PageKind::RowCopies {
                pages.insert((page.storage, page.key));
            }
        },
    )
    .expect("inspect actual native row-copy pages");
    pages.len()
}

/// Aggregate accounting and native index entries across equal-size replacement cycles.
/// These measurements do not identify individual disk blocks or invoice frees.
fn repeated_replacements_return_aggregate_charges_and_bound_pages(db: &Database, initial_old: u64) {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let settle = |old: u64| loop {
        let report = db.run_maintenance_cycle().expect("one cleanup cycle");
        assert!(
            report.vector.first_failure.is_none(),
            "cleanup refused: {report:?}"
        );
        if !retained_generations(db, "items", "embedding", &[]).contains(&old)
            && report.vector.remaining_partitions == 0
        {
            break;
        }
    };
    settle(initial_old);
    let baseline_pages = db.__durable_page_population_for_test().unwrap().1;
    let baseline_copies = native_row_copy_pages(db);
    assert!(
        baseline_copies > 0,
        "the current native index must expose actual RowCopies entries before measuring their settlement"
    );
    let mut one_cycle_pages = None;
    let mut settled_pages = Vec::new();
    for cycle in 0..8_u64 {
        let old = serving(db, "items", "embedding", &[]).unwrap().generation;
        let copies_before = native_row_copy_pages(db);
        let writer_before = db.writer_memory_usage_for_test().used;
        let (_, reader_before) = db.with_reader_memory_high_water_for_test(|| ());
        let pages_before = db.__durable_page_population_for_test().unwrap().1;
        let pin = db
            .pin_snapshot(db.snapshot())
            .expect("pin pre-replacement reader");
        for id in 0..8_i64 {
            if id == 7 {
                continue;
            }
            db.execute(
                "UPDATE items SET embedding = $v WHERE id = $id",
                &params([
                    ("id", Value::Int64(id)),
                    (
                        "v",
                        Value::Vector(unit_vector((cycle + 1) * 1000 + id as u64, DIM)),
                    ),
                ]),
            )
            .expect("commit an equal-cardinality vector replacement");
        }
        let publications = Arc::new(AtomicUsize::new(0));
        let observed = publications.clone();
        observe_publication_cuts(
            Arc::new(move |cut| {
                if cut == PublicationCut::AfterGenerationPublication {
                    observed.fetch_add(1, Ordering::SeqCst);
                }
            }),
            || drive_until_serving(db, 48, 1, 10),
        );
        let new = serving(db, "items", "embedding", &[]).unwrap().generation;
        assert_ne!(new, old, "each cycle publishes a replacement");
        assert_eq!(
            publications.load(Ordering::SeqCst),
            1,
            "exactly one publication is observed during this replacement"
        );
        let pages_pinned = db.__durable_page_population_for_test().unwrap().1;
        drop(pin);
        settle(old);
        let pages_after = db.__durable_page_population_for_test().unwrap().1;
        let copies_after = native_row_copy_pages(db);
        let writer_after = db.writer_memory_usage_for_test().used;
        let (_, reader_after) = db.with_reader_memory_high_water_for_test(|| ());
        let observed_growth = pages_pinned.saturating_sub(pages_before);
        let bound = *one_cycle_pages.get_or_insert(observed_growth);
        println!(
            "REPLACEMENT_ACCOUNT cycle={cycle} writer_before={writer_before} writer_after={writer_after} reader_before={} reader_after={} pages_before={pages_before} pages_pinned={pages_pinned} pages_after={pages_after} first_cycle_growth_bound={bound}",
            reader_before.before, reader_after.after
        );
        assert_eq!(
            writer_after, writer_before,
            "the original writer payer returns to its pre-cycle debit"
        );
        assert_eq!(
            reader_after.after, reader_before.before,
            "the declared reader payer returns to its pre-cycle debit"
        );
        settled_pages.push(copies_after);
        println!(
            "NATIVE_ROW_COPY_SAMPLE cycle={cycle} baseline={baseline_copies} before={copies_before} settled={copies_after}"
        );
        println!(
            "EIGHT_CYCLE_PAGE_SAMPLE cycle={cycle} baseline_pages={baseline_pages} settled={pages_after} first_peak_ceiling={}",
            baseline_pages.saturating_add(bound)
        );
    }
    let plateau_start = settled_pages
        .windows(3)
        .position(|window| window[0] == window[1] && window[1] == window[2]);
    assert!(
        plateau_start.is_some(),
        "settled native row-copy pages must reach a three-observation plateau within eight replacement cycles: {settled_pages:?}"
    );
    let plateau_start = plateau_start.unwrap();
    let ceiling = settled_pages[plateau_start];
    assert!(
        settled_pages[plateau_start..]
            .iter()
            .all(|pages| *pages <= ceiling),
        "settled native row-copy pages must not exceed the observed plateau ceiling {ceiling}: {settled_pages:?}"
    );
}
