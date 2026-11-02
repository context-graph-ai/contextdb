//! A declared CALIBRATION_QUERIES source is read when calibration runs, and
//! its identity and frontier are pinned in the qualification receipt. A later
//! change to the source never flips the served generation's basis or route:
//! it marks the Context as pending requalification, and a qualified indexed
//! route (AUTO on the index, INDEXED meeting its objective) and its receipt
//! stay exactly as qualified until requalification replaces the receipt with
//! one pinned at the new frontier.

use super::support::{
    CalibrationBasis, ObjectiveStatus, SearchRoute, calibration_receipt,
    declare_calibration_queries, drive_until_serving, empty, maintenance_cycles, observe_search,
    open_caller_driven, params, plant_qualification, requalification_pending, serving, unit_vector,
};
use contextdb_core::Value;
use contextdb_engine::Database;

const DIMENSIONS: usize = 16;

fn insert(db: &Database, table: &str, ids: std::ops::Range<i64>, seed_base: u64) {
    let tx = db.begin_or_panic();
    for id in ids {
        db.execute_in_tx(
            tx,
            &format!("INSERT INTO {table} (id, embedding) VALUES ($id, $embedding)"),
            &params([
                ("id", Value::Int64(id)),
                (
                    "embedding",
                    Value::Vector(unit_vector(seed_base + id as u64, DIMENSIONS)),
                ),
            ]),
        )
        .expect("stage one row");
    }
    db.commit(tx).expect("commit the rows");
}

fn routes(
    db: &Database,
) -> (
    Option<super::support::SearchRoute>,
    Option<super::support::ObjectiveStatus>,
) {
    let query = params([("query", Value::Vector(unit_vector(424_242, DIMENSIONS)))]);
    let (_, auto) = observe_search(db, || {
        db.execute(
            "SELECT id FROM sourced_items ORDER BY embedding <=> $query USE VECTOR AUTO LIMIT 10",
            &query,
        )
        .unwrap()
    });
    let (_, indexed) = observe_search(db, || {
        db.execute(
            "SELECT id FROM sourced_items ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 10",
            &query,
        )
        .unwrap()
    });
    (auto.route, indexed.objective_status)
}

#[test]
#[ignore = "index deferred to the follow-on plan"]
fn a_calibration_source_change_marks_requalification_pending_without_flipping_the_route() {
    let root = tempfile::tempdir().expect("temporary store directory");
    let db = open_caller_driven(&root.path().join("sourced.db"));
    for table in ["sourced_items", "sourced_questions"] {
        db.execute(
            &format!(
                "CREATE TABLE {table} (id INTEGER PRIMARY KEY, embedding VECTOR({DIMENSIONS}))"
            ),
            &empty(),
        )
        .expect("declare the table");
    }
    insert(&db, "sourced_items", 0..2_000, 0);
    insert(&db, "sourced_questions", 0..50, 900_000);
    db.execute(
        "ALTER TABLE sourced_items ALTER COLUMN embedding SET WITH (calibration_samples = 50)",
        &empty(),
    )
    .expect("declare the fifty-question calibration threshold");
    declare_calibration_queries(
        &db,
        "sourced_items",
        "embedding",
        "sourced_questions",
        "embedding",
    );
    drive_until_serving(&db, 2050, 1, 10);
    // A deterministic qualified baseline: depth 10 qualified on the
    // source-calibrated generation at a twentieth of EXACT's work.
    plant_qualification(&db, "sourced_items", "embedding", &[], &[10], 100, 2_000);

    let qualified = calibration_receipt(&db, "sourced_items", "embedding", &[])
        .expect("the serving generation has a receipt");
    let (identity, frontier) = match qualified.basis {
        CalibrationBasis::Queries {
            source_identity,
            source_frontier,
        } => (source_identity, source_frontier),
        other => panic!("calibration ran on the declared source: {other:?}"),
    };
    assert!(!requalification_pending(
        &db,
        "sourced_items",
        "embedding",
        &[]
    ));
    let generation = serving(&db, "sourced_items", "embedding", &[])
        .unwrap()
        .generation;
    assert_eq!(
        routes(&db),
        (Some(SearchRoute::Indexed), Some(ObjectiveStatus::Met)),
        "before the source changes, AUTO takes the index and INDEXED meets its objective"
    );

    insert(&db, "sourced_questions", 50..70, 900_000);
    assert!(
        requalification_pending(&db, "sourced_items", "embedding", &[]),
        "a change to the calibration source marks requalification pending"
    );
    let mut requalified = false;
    for _ in 0..maintenance_cycles(2070, 1, 10) {
        if !requalification_pending(&db, "sourced_items", "embedding", &[]) {
            requalified = true;
            break;
        }
        assert_eq!(
            calibration_receipt(&db, "sourced_items", "embedding", &[]).as_ref(),
            Some(&qualified),
            "until requalification lands, the served receipt and its basis do not change"
        );
        assert_eq!(
            serving(&db, "sourced_items", "embedding", &[])
                .unwrap()
                .generation,
            generation,
            "requalification is pending on the same served generation"
        );
        assert_eq!(
            routes(&db),
            (Some(SearchRoute::Indexed), Some(ObjectiveStatus::Met)),
            "while requalification is pending, AUTO keeps the index and INDEXED keeps Met"
        );
        db.run_maintenance_cycle().expect("one maintenance cycle");
    }
    assert!(
        requalified,
        "maintenance requalifies against the changed source"
    );
    match calibration_receipt(&db, "sourced_items", "embedding", &[])
        .expect("the requalified generation has a receipt")
        .basis
    {
        CalibrationBasis::Queries {
            source_identity,
            source_frontier,
        } => {
            assert_eq!(source_identity, identity, "the same declared source");
            assert!(
                source_frontier > frontier,
                "the new receipt is pinned at the changed source's frontier"
            );
        }
        other => panic!("requalification stays on the declared source: {other:?}"),
    }
}

#[test]
#[ignore = "index deferred to the follow-on plan"]
fn fifty_source_questions_use_stored_vectors_under_the_default_two_hundred_sample_threshold() {
    let root = tempfile::tempdir().expect("temporary store directory");
    let db = open_caller_driven(&root.path().join("sourced-default.db"));
    for table in ["sourced_items", "sourced_questions"] {
        db.execute(
            &format!(
                "CREATE TABLE {table} (id INTEGER PRIMARY KEY, embedding VECTOR({DIMENSIONS}))"
            ),
            &empty(),
        )
        .expect("declare the table");
    }
    insert(&db, "sourced_items", 0..2_000, 0);
    insert(&db, "sourced_questions", 0..50, 900_000);
    declare_calibration_queries(
        &db,
        "sourced_items",
        "embedding",
        "sourced_questions",
        "embedding",
    );
    drive_until_serving(&db, 2050, 1, 10);

    assert_eq!(
        calibration_receipt(&db, "sourced_items", "embedding", &[])
            .expect("the serving generation has a receipt")
            .basis,
        CalibrationBasis::StoredVectors,
        "fifty source questions do not meet the default two-hundred-sample threshold"
    );
}
