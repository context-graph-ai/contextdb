//! A reader keeps the committed view it opened with. A search at a pinned
//! snapshot returns the same answer after a later commit, after maintenance
//! builds and publishes a replacement generation, and after reclamation runs;
//! a reader opened after the commit sees the new rows and not the deleted
//! one. Once the old reader is gone, reclamation releases the old generation.

use super::support::{
    MAX_MAINTENANCE_CYCLES, drive_until_serving, empty, int_ids, ivf, observe_search,
    open_caller_driven, params, retained_generations, serving, unit_vector,
};
use contextdb_core::Value;
use contextdb_engine::{Database, QueryResult};

const DIMENSIONS: usize = 8;
const ROWS: i64 = 40;
const VICTIM: i64 = 7;
const NEWCOMER: i64 = 100;
const SQL: &str =
    "SELECT id FROM isolated_items ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 5";

fn insert(db: &Database, id: i64, vector: Vec<f32>) {
    db.execute(
        "INSERT INTO isolated_items (id, embedding) VALUES ($id, $embedding)",
        &params([
            ("id", Value::Int64(id)),
            ("embedding", Value::Vector(vector)),
        ]),
    )
    .expect("commit one vector");
}

fn query() -> std::collections::HashMap<String, Value> {
    params([(
        "query",
        Value::Vector(unit_vector(VICTIM as u64, DIMENSIONS)),
    )])
}

fn search_now(db: &Database) -> QueryResult {
    let (result, work) = observe_search(db, || db.execute(SQL, &query()).expect("search now"));
    assert!(
        work.clustered_route,
        "the current search ran on the clustered index"
    );
    result
}

#[test]
fn a_pinned_reader_keeps_its_view_through_commit_replacement_and_reclamation() {
    let root = tempfile::tempdir().expect("temporary store directory");
    let db = open_caller_driven(&root.path().join("isolated.db"));
    db.execute(
        &format!(
            "CREATE TABLE isolated_items (id INTEGER PRIMARY KEY, embedding VECTOR({DIMENSIONS}) SEARCH_MODE INDEXED {} CONSOLIDATION (CHANGE_PERCENT = 10))",
            ivf(4, 4, 64)
        ),
        &empty(),
    )
    .expect("declare the clustered index");
    for id in 0..ROWS {
        insert(&db, id, unit_vector(id as u64, DIMENSIONS));
    }
    drive_until_serving(&db);
    let first = serving(&db, "isolated_items", "embedding", &[])
        .expect("the first generation is serving")
        .generation;

    let before = db.snapshot();
    let pin = db.pin_snapshot(before).expect("pin the reader's snapshot");
    let at_before = || {
        let (result, work) = observe_search(&db, || {
            db.execute_at_snapshot(SQL, &query(), before)
                .expect("search at the pinned snapshot")
        });
        assert!(
            work.clustered_route,
            "the pinned search ran on the clustered index"
        );
        int_ids(&result)
    };
    let view = at_before();
    assert_eq!(
        view[0], VICTIM,
        "the victim is its own nearest row before the commit"
    );

    // One commit: a new exact match for the query, the victim's removal, and
    // enough further inserts for the declared consolidation to replace.
    let tx = db.begin_or_panic();
    db.execute_in_tx(
        tx,
        "INSERT INTO isolated_items (id, embedding) VALUES ($id, $embedding)",
        &params([
            ("id", Value::Int64(NEWCOMER)),
            (
                "embedding",
                Value::Vector(unit_vector(VICTIM as u64, DIMENSIONS)),
            ),
        ]),
    )
    .expect("stage the newcomer");
    db.execute_in_tx(
        tx,
        "DELETE FROM isolated_items WHERE id = $id",
        &params([("id", Value::Int64(VICTIM))]),
    )
    .expect("stage the victim's deletion");
    for id in NEWCOMER + 1..NEWCOMER + 8 {
        db.execute_in_tx(
            tx,
            "INSERT INTO isolated_items (id, embedding) VALUES ($id, $embedding)",
            &params([
                ("id", Value::Int64(id)),
                (
                    "embedding",
                    Value::Vector(unit_vector(id as u64, DIMENSIONS)),
                ),
            ]),
        )
        .expect("stage a further insert");
    }
    db.commit(tx).expect("commit the change");
    assert_eq!(
        at_before(),
        view,
        "the pinned reader keeps its view through the commit"
    );

    let mut replaced = false;
    for _ in 0..MAX_MAINTENANCE_CYCLES {
        db.run_maintenance_cycle().expect("one maintenance cycle");
        assert_eq!(
            at_before(),
            view,
            "the pinned reader keeps its view through every maintenance step"
        );
        if matches!(
            serving(&db, "isolated_items", "embedding", &[]),
            Some(serving) if serving.generation != first
        ) {
            replaced = true;
        }
    }
    assert!(replaced, "maintenance published a replacement generation");

    let after = int_ids(&search_now(&db));
    assert_eq!(
        after[0], NEWCOMER,
        "a new reader sees the committed newcomer first"
    );
    assert!(
        !after.contains(&VICTIM),
        "a new reader never sees the deleted victim"
    );

    drop(pin);
    let serving_now = serving(&db, "isolated_items", "embedding", &[])
        .expect("a generation is serving")
        .generation;
    let mut released = false;
    for _ in 0..MAX_MAINTENANCE_CYCLES {
        db.run_maintenance_cycle().expect("one maintenance cycle");
        if retained_generations(&db, "isolated_items", "embedding", &[]) == vec![serving_now] {
            released = true;
            break;
        }
    }
    assert!(
        released,
        "once the old reader is gone, reclamation releases every generation but the serving one"
    );
    assert_eq!(
        int_ids(&search_now(&db)),
        after,
        "reclamation changes no answer"
    );
}
