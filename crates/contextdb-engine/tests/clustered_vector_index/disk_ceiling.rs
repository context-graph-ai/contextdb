//! Building or replacing a clustered generation respects the store's
//! declared disk ceiling. When the build's training reservoir, accumulators
//! or temporary list runs would take the store past the ceiling, the build is
//! refused with the existing typed disk-budget error before anything is
//! written: the store file does not grow, no build state is added, every
//! index reservation is returned, the old generation keeps serving every
//! committed row, and once the ceiling is lifted the build finishes and the
//! store reopens cleanly.

use super::support::{
    MAX_MAINTENANCE_CYCLES, PhysicalPage, build_progress, count, drive_until_serving, empty,
    int_ids, ivf, last_build_refusal, observe_search, open_caller_driven,
    open_with_build_batch_rows, outstanding_index_reservations, params, physical_pages, serving,
    unit_vector,
};
use contextdb_core::{Error, Value};
use contextdb_engine::Database;
use std::path::Path;

const DIMENSIONS: usize = 64;
const FIRST_ROWS: std::ops::Range<i64> = 0..120;
const SECOND_ROWS: std::ops::Range<i64> = 1_000..1_240;
/// A ceiling below what any further write needs, the same way the existing
/// vector-generation disk refusal is driven.
const CEILING_BYTES: u64 = 1;
const REFUSED_CYCLES: usize = 3;

fn insert_rows(db: &Database, ids: std::ops::Range<i64>) {
    let tx = db.begin_or_panic();
    for id in ids {
        db.execute_in_tx(
            tx,
            "INSERT INTO ceiling_items (id, embedding) VALUES ($id, $embedding)",
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

/// A store serving its first generation, with a doubling of rows committed
/// after it so a multi-batch replacement is due. Returns the first
/// generation.
fn store_with_due_replacement(db: &Database) -> u64 {
    db.execute(
        &format!(
            "CREATE TABLE ceiling_items (id INTEGER PRIMARY KEY, embedding VECTOR({DIMENSIONS}) SEARCH_MODE INDEXED {} CONSOLIDATION (CHANGE_PERCENT = 10))",
            ivf(4, 4, 64)
        ),
        &empty(),
    )
    .expect("declare the clustered index");
    insert_rows(db, FIRST_ROWS);
    drive_until_serving(db);
    let first = serving(db, "ceiling_items", "embedding", &[])
        .expect("the first generation is serving")
        .generation;
    insert_rows(db, SECOND_ROWS);
    first
}

fn ids() -> Vec<i64> {
    FIRST_ROWS.chain(SECOND_ROWS).collect()
}

fn build_state(db: &Database) -> Vec<(String, Vec<u8>)> {
    let mut pages = physical_pages(db, "ceiling_items", "embedding")
        .into_iter()
        .filter(|page: &PhysicalPage| page.place.belongs_to_an_unpublished_build())
        .map(|page| (format!("{:?}", page.place), page.bytes))
        .collect::<Vec<_>>();
    pages.sort();
    pages
}

fn assert_every_row_is_served(db: &Database, stage: &str) {
    let ids = ids();
    assert_eq!(
        count(db, "ceiling_items"),
        ids.len() as i64,
        "{stage}: every row survives"
    );
    for id in ids {
        let (result, work) = observe_search(db, || {
            db.execute(
                "SELECT id FROM ceiling_items ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 1",
                &params([("query", Value::Vector(unit_vector(id as u64, DIMENSIONS)))]),
            )
            .expect("indexed search")
        });
        assert!(
            work.clustered_route,
            "{stage}: row {id} was searched on the clustered index"
        );
        assert_eq!(int_ids(&result), vec![id], "{stage}: row {id} is served");
    }
}

fn assert_disk_refusal(error: &Error, stage: &str) {
    match error {
        Error::DiskBudgetExceeded {
            budget_limit_bytes, ..
        } => assert_eq!(
            *budget_limit_bytes, CEILING_BYTES,
            "{stage}: the refusal names the declared ceiling"
        ),
        other => {
            panic!("{stage}: the build is refused with the typed disk-budget error: {other:?}")
        }
    }
}

/// Under the ceiling, each maintenance cycle surfaces the typed disk refusal
/// (as its result, or as the Context's recorded build refusal) and writes
/// nothing: the file and the build state are byte-for-byte where they were.
fn assert_cycles_refused_at_the_ceiling(db: &Database, first: u64, stage: &str) {
    db.set_disk_limit(Some(CEILING_BYTES))
        .expect("declare a disk ceiling the build cannot fit under");
    let file_before = db.disk_file_size().expect("a file-backed store has a size");
    let state_before = build_state(db);
    for cycle in 0..REFUSED_CYCLES {
        let stage = format!("{stage}, refused cycle {cycle}");
        match db.run_maintenance_cycle() {
            Err(error) => assert_disk_refusal(&error, &stage),
            Ok(_) => assert_disk_refusal(
                &last_build_refusal(db, "ceiling_items", "embedding", &[])
                    .unwrap_or_else(|| panic!("{stage}: the refused build records why it stopped")),
                &stage,
            ),
        }
        assert_eq!(
            db.disk_file_size(),
            Some(file_before),
            "{stage}: nothing was written past the ceiling"
        );
        assert_eq!(
            build_state(db),
            state_before,
            "{stage}: no training reservoir, accumulator or unpublished run was added"
        );
        assert_eq!(
            outstanding_index_reservations(db),
            0,
            "{stage}: the refused cycle returned every index reservation"
        );
        assert_eq!(
            serving(db, "ceiling_items", "embedding", &[]).map(|serving| serving.generation),
            Some(first),
            "{stage}: the old generation keeps serving"
        );
    }
    assert_every_row_is_served(db, stage);
}

fn assert_finishes_once_lifted(db: Database, path: &Path, first: u64, stage: &str) {
    db.set_disk_limit(None).expect("lift the disk ceiling");
    drive_until_serving(&db);
    assert_ne!(
        serving(&db, "ceiling_items", "embedding", &[])
            .expect("a generation is serving")
            .generation,
        first,
        "{stage}: with room again the replacement publishes"
    );
    assert_every_row_is_served(&db, &format!("{stage}, after the replacement"));
    db.close().expect("close the store");
    let db = open_caller_driven(path);
    assert_every_row_is_served(&db, &format!("{stage}, after reopen"));
    db.close().expect("close the reopened store");
}

#[test]
fn a_replacement_build_is_refused_at_the_disk_ceiling_before_it_writes() {
    let root = tempfile::tempdir().expect("temporary store directory");
    let path = root.path().join("ceiling-start.db");
    let db = open_with_build_batch_rows(&path, 16);
    let first = store_with_due_replacement(&db);
    assert_cycles_refused_at_the_ceiling(&db, first, "build start");
    assert_finishes_once_lifted(db, &path, first, "build start");
}

#[test]
fn a_build_in_flight_is_refused_at_the_disk_ceiling_without_growing_its_reservoir_or_runs() {
    let root = tempfile::tempdir().expect("temporary store directory");
    let path = root.path().join("ceiling-mid.db");
    let db = open_with_build_batch_rows(&path, 16);
    let first = store_with_due_replacement(&db);
    // Let the replacement persist some training and run state first.
    let mut in_flight = false;
    for _ in 0..MAX_MAINTENANCE_CYCLES {
        db.run_maintenance_cycle().expect("one bounded build cycle");
        if build_progress(&db, "ceiling_items", "embedding", &[]).in_flight
            && !build_state(&db).is_empty()
        {
            in_flight = true;
            break;
        }
    }
    assert!(
        in_flight,
        "the replacement is in flight with persisted reservoir or run state"
    );
    assert_cycles_refused_at_the_ceiling(&db, first, "build in flight");
    assert_finishes_once_lifted(db, &path, first, "build in flight");
}
