#![cfg(feature = "test-seams")]
//! A cg file is one committed LSN. When that one commit's new vectors exceed
//! the share of the writer limit an import cohort may retain, making them
//! searchable must still complete under the unchanged limit, with every
//! vector answered by the clustered index and no ownerless decode.
use contextdb_core::{Value, Wallclock};
use contextdb_engine::database::resident_history_test_seams;
use contextdb_engine::vector_read_memory::unowned_decodes_for_test;
use contextdb_engine::{Database, DatabaseOpenOptions, MaintenancePolicy};
use std::collections::HashMap;
const DIMENSION: usize = 384;
const BASE_ROWS: i64 = 16;
const ONE_COMMIT: usize = 640;
const ROOMY: usize = 512 * 1024 * 1024;
const TIGHT: usize = 16 * 1024 * 1024;

fn options(writer: usize) -> DatabaseOpenOptions {
    let mut options = DatabaseOpenOptions {
        maintenance_policy: MaintenancePolicy::CallerDriven,
        memory_limit: Some(writer),
        ..DatabaseOpenOptions::default()
    };
    options.owner_reads.limits.limits.memory = 1 << 30;
    options
}

/// A unique direction per row: no two rows share a nearest neighbour.
fn vector_for(id: i64) -> Vec<f32> {
    let mut values: Vec<f32> = (0..DIMENSION)
        .map(|lane| ((id as f32) * 0.618 + lane as f32 * 0.37).sin() * 0.25)
        .collect();
    values[(id as usize) % DIMENSION] += 4.0;
    values[(id as usize * 7 + 3) % DIMENSION] += 2.0;
    values
}

fn insert_one(db: &Database, id: i64) {
    db.execute(
        "INSERT INTO points VALUES ($id, 1, $vector)",
        &HashMap::from([
            ("id".into(), Value::Int64(id)),
            ("vector".into(), Value::Vector(vector_for(id))),
        ]),
    )
    .unwrap();
}

/// One INSERT statement, one committed LSN, `rows` new vectors from `first`.
fn one_commit(db: &Database, first: i64, rows: usize) {
    let mut sql = String::from("INSERT INTO points VALUES ");
    let mut params = HashMap::new();
    for row in 0..rows {
        if row > 0 {
            sql.push_str(", ");
        }
        sql.push_str(&format!("($i{row}, 1, $v{row})"));
        let id = first + row as i64;
        params.insert(format!("i{row}"), Value::Int64(id));
        params.insert(format!("v{row}"), Value::Vector(vector_for(id)));
    }
    db.execute(&sql, &params).unwrap();
}

/// A built 16-row route, then one commit of 640 vectors, closed.
fn store_with_one_large_commit(path: &std::path::Path) {
    store_with_one_commit_of(path, ONE_COMMIT);
}

/// A built 16-row route, then one commit of `rows` vectors, closed.
fn store_with_one_commit_of(path: &std::path::Path, rows: usize) {
    store_with_base_and_one_commit(path, 0, rows);
}

/// A built route of 16 + `extra_base` rows, then one commit of `rows`
/// vectors, closed.
fn store_with_base_and_one_commit(path: &std::path::Path, extra_base: usize, rows: usize) {
    let db = Database::open_with_options(path, options(ROOMY)).unwrap();
    db.execute(&format!("CREATE TABLE points (id INT PRIMARY KEY, bucket INT NOT NULL, embedding VECTOR({DIMENSION}) PARTITION_KEY (bucket) MAX_PARTITIONS 2 SEARCH_MODE INDEXED AUTO_INDEX_AT 1 CONSOLIDATION NONE)"), &HashMap::new()).unwrap();
    for id in 0..BASE_ROWS {
        insert_one(&db, id);
    }
    for first in (0..extra_base).step_by(256) {
        one_commit(&db, 500_000 + first as i64, 256.min(extra_base - first));
    }
    while db
        .run_maintenance_cycle()
        .unwrap()
        .vector
        .remaining_partitions
        != 0
    {}
    one_commit(&db, 1_000_000, rows);
    db.close().unwrap();
}

fn nearest(db: &Database, id: i64) -> Vec<Vec<Value>> {
    db.execute(
        "SELECT id FROM points WHERE bucket = 1 ORDER BY embedding <=> $q USE VECTOR INDEXED LIMIT 1",
        &HashMap::from([("q".into(), Value::Vector(vector_for(id)))]),
    )
    .unwrap()
    .rows().to_vec()
}

fn count(db: &Database) -> i64 {
    match db
        .execute(
            "SELECT COUNT(*) FROM points WHERE bucket = 1",
            &HashMap::new(),
        )
        .unwrap()
        .rows()[0][0]
    {
        Value::Int64(count) => count,
        ref other => panic!("{other:?}"),
    }
}

#[test]
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn one_committed_lsn_larger_than_the_cohort_share_is_absorbed_without_raising_the_limit() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("one-large-commit.db");
    store_with_one_large_commit(&path);
    let db = Database::open_with_options(&path, options(TIGHT)).unwrap();
    let unowned = unowned_decodes_for_test();
    let (completed, _work) =
        resident_history_test_seams::observe(|| db.persist_import_vector_tails());
    assert_eq!(completed.as_ref().ok(), Some(&true), "{completed:?}");
    assert_eq!(db.memory_limit(), Some(TIGHT), "the limit is never raised");
    assert!(db.write_memory_high_water_for_test() <= TIGHT);
    assert_eq!(unowned_decodes_for_test(), unowned, "no ownerless decode");
    assert_eq!(count(&db), BASE_ROWS + ONE_COMMIT as i64);
    for id in [1_000_000, 1_000_319, 1_000_639, 3] {
        assert_eq!(nearest(&db, id), vec![vec![Value::Int64(id)]]);
    }
}

#[test]
fn a_pinned_reader_keeps_its_answer_while_the_large_commit_is_rebuilt_and_published() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("pinned-reader.db");
    store_with_one_large_commit(&path);
    let db = Database::open_with_options(&path, options(TIGHT)).unwrap();
    let query = "SELECT id FROM points WHERE bucket = 1 ORDER BY embedding <=> $q USE VECTOR INDEXED LIMIT 1";
    let params = HashMap::from([("q".into(), Value::Vector(vector_for(1_000_123)))]);
    let pinned = db.begin().unwrap();
    let before = {
        let fixture_answer_45 = db.execute_in_tx(pinned, query, &params).unwrap();
        fixture_answer_45.rows().to_vec()
    };
    assert_eq!(before, vec![vec![Value::Int64(1_000_123)]]);
    assert!(db.persist_import_vector_tails().unwrap());
    db.execute(
        "DELETE FROM points WHERE id = $id",
        &HashMap::from([("id".into(), Value::Int64(1_000_123))]),
    )
    .unwrap();
    insert_one(&db, 3_000_000);
    let cycle = db.run_maintenance_cycle().unwrap().vector;
    assert!(cycle.first_failure.is_none(), "{:?}", cycle.first_failure);
    assert_eq!(
        db.execute_in_tx(pinned, query, &params).unwrap().rows(),
        before,
        "the pinned reader keeps its original answer"
    );
    let fresh = {
        let fixture_answer_46 = db.execute(query, &params).unwrap();
        fixture_answer_46.rows().to_vec()
    };
    assert_ne!(fresh, before, "a fresh reader sees the delete");
    assert_eq!(nearest(&db, 3_000_000), vec![vec![Value::Int64(3_000_000)]]);
    db.rollback(pinned).unwrap();
}
