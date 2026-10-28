//! Cancelling work inside the clustered index is an ordinary, typed outcome.
//! A search cancelled while it pages through lists or while it scores and
//! re-ranks candidates, and a maintenance cycle cancelled inside a build batch
//! or inside a reclamation batch, each return the existing typed cancellation
//! error: never a panic, never a corrupt-store error, never a partial answer.
//! The cancelled operation releases every index reservation it held, the
//! reader returns to the level it held before, nothing is half-published,
//! and the store keeps answering, finishes the interrupted work later, and
//! reopens cleanly.

use super::support::{
    IndexCancelPoint, count, drive_until_serving, empty, int_ids, ivf, maintenance_cycles,
    observe_index_cancel_points, observe_search, open_caller_driven, open_with_build_batch_rows,
    outstanding_index_reservations, params, retained_generations,
    run_maintenance_cycle_cancellable, serving, unit_vector,
};
use contextdb_core::read_contract::{OwnerReadCancellation, ReadLimits};
use contextdb_core::{Error, Value};
use contextdb_engine::Database;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

const DIMENSIONS: usize = 16;
const FIRST_ROWS: i64 = 120;
const SECOND_ROWS: std::ops::Range<i64> = 1_000..1_120;
const SQL: &str =
    "SELECT id FROM cancel_items ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 5";

fn insert_rows(db: &Database, ids: impl IntoIterator<Item = i64>) {
    let tx = db.begin_or_panic();
    for id in ids {
        db.execute_in_tx(
            tx,
            "INSERT INTO cancel_items (id, embedding) VALUES ($id, $embedding)",
            &params([
                ("id", Value::Int64(id)),
                (
                    "embedding",
                    Value::Vector(unit_vector(id as u64, DIMENSIONS)),
                ),
            ]),
        )
        .expect("stage one row");
    }
    db.commit(tx).expect("commit the rows");
}

fn create(db: &Database) {
    db.execute(
        &format!(
            "CREATE TABLE cancel_items (id INTEGER PRIMARY KEY, embedding VECTOR({DIMENSIONS}) SEARCH_MODE INDEXED {} CONSOLIDATION (CHANGE_PERCENT = 10))",
            ivf(4, 4, 64)
        ),
        &empty(),
    )
    .expect("declare the clustered index");
}

fn query() -> std::collections::HashMap<String, Value> {
    params([("query", Value::Vector(unit_vector(17, DIMENSIONS)))])
}

/// An observer that cancels `cancellation` the first time the operation
/// reaches `target`, and counts every time it does.
fn cancel_at(
    target: IndexCancelPoint,
    cancellation: &OwnerReadCancellation,
) -> (Arc<dyn Fn(IndexCancelPoint) + Send + Sync>, Arc<AtomicU64>) {
    let hits = Arc::new(AtomicU64::new(0));
    let (seen, token) = (Arc::clone(&hits), cancellation.clone());
    (
        Arc::new(move |point| {
            if point == target && seen.fetch_add(1, Ordering::SeqCst) == 0 {
                token.cancel();
            }
        }),
        hits,
    )
}

fn assert_typed_cancellation<T: std::fmt::Debug>(result: contextdb_core::Result<T>, path: &str) {
    match result {
        Err(Error::ReadCancelled) => {}
        Ok(value) => panic!(
            "{path}: a cancelled operation must not complete or return a partial answer: {value:?}"
        ),
        Err(error @ Error::StoreCorrupted { .. }) => {
            panic!("{path}: a cancellation is never a corrupt-store error: {error:?}")
        }
        Err(other) => {
            panic!("{path}: a cancellation returns the typed cancellation error: {other:?}")
        }
    }
}

/// Every committed row is found by an indexed search for its own vector on
/// the clustered route.
fn assert_every_row_is_served(db: &Database, ids: &[i64], stage: &str) {
    assert_eq!(
        count(db, "cancel_items"),
        ids.len() as i64,
        "{stage}: every row survives"
    );
    for id in ids {
        let (result, work) = observe_search(db, || {
            db.execute(
                "SELECT id FROM cancel_items ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 1",
                &params([("query", Value::Vector(unit_vector(*id as u64, DIMENSIONS)))]),
            )
            .expect("indexed search")
        });
        assert!(
            work.clustered_route,
            "{stage}: row {id} was searched on the clustered index"
        );
        assert_eq!(int_ids(&result), vec![*id], "{stage}: row {id} is served");
    }
}

fn assert_reopens_usable(path: &Path, ids: &[i64], stage: &str) {
    let db = open_caller_driven(path);
    assert_every_row_is_served(&db, ids, &format!("{stage}, after reopen"));
    db.close().expect("close the reopened store");
}

fn search_cancelled_at(target: IndexCancelPoint) {
    let root = tempfile::tempdir().expect("temporary store directory");
    let path = root.path().join("cancelled-search.db");
    let db = open_caller_driven(&path);
    create(&db);
    insert_rows(&db, 0..FIRST_ROWS);
    drive_until_serving(&db, 120, 1, 10);
    let ids = (0..FIRST_ROWS).collect::<Vec<_>>();

    let session = db
        .read_session(ReadLimits::default())
        .expect("a reader session");
    // A completed run first, so the pages the cancelled run touches are
    // already the cache's and the reader's settled level is known.
    let reference = int_ids(&session.execute(SQL, &query()).expect("uncancelled search"));
    assert_eq!(reference.len(), 5);
    // A subsequent search clears the previous trace, so settle that refund
    // before measuring the reader level for the cancellation.
    db.__reset_last_query_vector_trace_for_test();
    let settled = db.declared_read_memory_for_test().retained_bytes_for_test();

    let cancellation = OwnerReadCancellation::new();
    let (observer, hits) = cancel_at(target, &cancellation);
    let result = observe_index_cancel_points(observer, || {
        session.execute_with_cancellation(SQL, &query(), &cancellation)
    });
    assert!(
        hits.load(Ordering::SeqCst) >= 1,
        "the search reached the {target:?} point before it was cancelled"
    );
    assert_typed_cancellation(
        result.map(|answer| int_ids(&answer)),
        &format!("{target:?}"),
    );
    assert_eq!(
        outstanding_index_reservations(&db),
        0,
        "{target:?}: the cancelled search released every index reservation"
    );
    assert_eq!(
        db.declared_read_memory_for_test().retained_bytes_for_test(),
        settled,
        "{target:?}: the reader is back at its settled level"
    );

    assert_eq!(
        int_ids(
            &session
                .execute(SQL, &query())
                .expect("search after the cancellation")
        ),
        reference,
        "{target:?}: the same session answers the same search afterwards"
    );
    assert_every_row_is_served(&db, &ids, &format!("{target:?}"));
    drop(session);
    db.close().expect("close the store");
    assert_reopens_usable(&path, &ids, &format!("{target:?}"));
}

#[test]
fn a_search_cancelled_while_paging_lists_returns_the_typed_cancellation() {
    search_cancelled_at(IndexCancelPoint::ListPageRead);
}

#[test]
fn a_search_cancelled_while_scoring_candidates_returns_the_typed_cancellation() {
    search_cancelled_at(IndexCancelPoint::CandidateScoring);
}

/// Runs cancellable maintenance cycles until one reaches `target`, where it
/// is cancelled; every earlier cycle completes normally.
fn cancel_maintenance_at(db: &Database, target: IndexCancelPoint, batch: usize) {
    for _ in 0..maintenance_cycles((FIRST_ROWS * 2) as usize, batch, 10) {
        let cancellation = OwnerReadCancellation::new();
        let (observer, hits) = cancel_at(target, &cancellation);
        let result = observe_index_cancel_points(observer, || {
            run_maintenance_cycle_cancellable(db, &cancellation)
        });
        if hits.load(Ordering::SeqCst) == 0 {
            result.unwrap_or_else(|error| {
                panic!("a maintenance cycle that was not cancelled completes: {error:?}")
            });
            continue;
        }
        assert_typed_cancellation(result, &format!("{target:?}"));
        assert_eq!(
            outstanding_index_reservations(db),
            0,
            "{target:?}: the cancelled cycle released every index reservation"
        );
        return;
    }
    panic!(
        "maintenance never reached the {target:?} point in {} cycles",
        maintenance_cycles((FIRST_ROWS * 2) as usize, batch, 10)
    );
}

#[test]
fn a_maintenance_cycle_cancelled_inside_a_build_batch_returns_the_typed_cancellation() {
    let root = tempfile::tempdir().expect("temporary store directory");
    let path = root.path().join("cancelled-build.db");
    let db = open_with_build_batch_rows(&path, 16);
    create(&db);
    insert_rows(&db, 0..FIRST_ROWS);
    drive_until_serving(&db, 120, 16, 10);
    let first = serving(&db, "cancel_items", "embedding", &[])
        .expect("the first generation is serving")
        .generation;
    // Doubling the rows schedules a replacement that spans several batches.
    insert_rows(&db, SECOND_ROWS);
    let ids = (0..FIRST_ROWS).chain(SECOND_ROWS).collect::<Vec<_>>();

    cancel_maintenance_at(&db, IndexCancelPoint::BuildBatch, 16);
    assert_eq!(
        serving(&db, "cancel_items", "embedding", &[]).map(|serving| serving.generation),
        Some(first),
        "the cancelled build published nothing; the old generation still serves"
    );
    assert_every_row_is_served(&db, &ids, "after the cancelled build batch");

    drive_until_serving(&db, 240, 16, 10);
    assert_ne!(
        serving(&db, "cancel_items", "embedding", &[])
            .expect("a generation is serving")
            .generation,
        first,
        "later maintenance finishes the interrupted replacement"
    );
    assert_every_row_is_served(&db, &ids, "after the replacement finished");
    db.close().expect("close the store");
    assert_reopens_usable(&path, &ids, "build batch");
}

#[test]
fn a_maintenance_cycle_cancelled_inside_a_reclaim_batch_returns_the_typed_cancellation() {
    let root = tempfile::tempdir().expect("temporary store directory");
    let path = root.path().join("cancelled-reclaim.db");
    let db = open_caller_driven(&path);
    create(&db);
    insert_rows(&db, 0..FIRST_ROWS);
    drive_until_serving(&db, 120, 1, 10);
    let first = serving(&db, "cancel_items", "embedding", &[])
        .expect("the first generation is serving")
        .generation;

    // A pinned reader keeps the first generation while its replacement
    // publishes; releasing it leaves the first generation to reclaim.
    let pin = db.pin_snapshot(db.snapshot()).expect("pin a reader");
    insert_rows(&db, SECOND_ROWS);
    drive_until_serving(&db, 240, 1, 10);
    let replacement = serving(&db, "cancel_items", "embedding", &[])
        .expect("a generation is serving")
        .generation;
    assert_ne!(replacement, first, "the replacement published");
    assert!(
        retained_generations(&db, "cancel_items", "embedding", &[]).contains(&first),
        "the pinned reader keeps the first generation"
    );
    drop(pin);
    let ids = (0..FIRST_ROWS).chain(SECOND_ROWS).collect::<Vec<_>>();

    cancel_maintenance_at(&db, IndexCancelPoint::ReclaimBatch, 1);
    assert_eq!(
        serving(&db, "cancel_items", "embedding", &[]).map(|serving| serving.generation),
        Some(replacement),
        "a cancelled reclamation changes nothing that serves"
    );
    assert_every_row_is_served(&db, &ids, "after the cancelled reclaim batch");

    let mut reclaimed = false;
    for _ in 0..maintenance_cycles((FIRST_ROWS * 2) as usize, 1, 10) {
        db.run_maintenance_cycle().expect("one maintenance cycle");
        if retained_generations(&db, "cancel_items", "embedding", &[]) == vec![replacement] {
            reclaimed = true;
            break;
        }
    }
    assert!(
        reclaimed,
        "later maintenance finishes the interrupted reclamation"
    );
    assert_every_row_is_served(&db, &ids, "after reclamation finished");
    db.close().expect("close the store");
    assert_reopens_usable(&path, &ids, "reclaim batch");
}
