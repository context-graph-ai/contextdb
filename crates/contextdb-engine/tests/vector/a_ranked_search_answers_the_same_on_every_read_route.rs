//! A ranked vector search gives one answer whichever route reads the store:
//! the writer, a read session served by the live owner, and a read session
//! that reads the closed store's file directly. The rank policy's joined
//! weights decide the order on every one of them.

use contextdb_core::Value;
use contextdb_engine::{Database, MaintenancePolicy};
use std::collections::HashMap;
use uuid::Uuid;

fn id(n: u128) -> Value {
    Value::Uuid(Uuid::from_u128(n))
}

fn open(path: &std::path::Path) -> Database {
    let db = Database::open(path).expect("open the ranked fixture");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db
}

const RANKED: &str =
    "SELECT id FROM decisions ORDER BY embedding <=> $q USE RANK effective LIMIT 3";

#[test]
fn a_ranked_vector_search_answers_the_same_on_the_writer_owner_and_file_routes() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("ranked-routes.redb");
    let db = open(&path);
    for statement in [
        "CREATE TABLE outcomes (id UUID PRIMARY KEY, bucket INTEGER, weight REAL) SYNC OFF",
        "CREATE INDEX outcomes_bucket_idx ON outcomes (bucket)",
        "CREATE TABLE decisions (
            id UUID PRIMARY KEY,
            bucket INTEGER,
            embedding VECTOR(2) RANK_POLICY (
                JOIN outcomes ON bucket,
                FORMULA 'coalesce({weight}, 0.0)',
                SORT_KEY effective
            )
        ) SYNC OFF",
    ] {
        db.execute(statement, &HashMap::new()).unwrap();
    }
    // Similarity alone orders the decisions 2, 1, 3; the joined weights
    // 20, 10, 5 order them 3, 1, 2.
    for (n, bucket, embedding) in [
        (1, 1, vec![0.6_f32, 0.8]),
        (2, 2, vec![0.8, 0.6]),
        (3, 3, vec![0.5, 0.866_025_4]),
    ] {
        db.execute(
            "INSERT INTO decisions (id, bucket, embedding) VALUES ($id, $bucket, $embedding)",
            &HashMap::from([
                ("id".into(), id(n)),
                ("bucket".into(), Value::Int64(bucket)),
                ("embedding".into(), Value::Vector(embedding)),
            ]),
        )
        .unwrap();
    }
    for (n, bucket, weight) in [(11, 1, 10.0), (12, 2, 5.0), (13, 3, 20.0)] {
        db.execute(
            "INSERT INTO outcomes (id, bucket, weight) VALUES ($id, $bucket, $weight)",
            &HashMap::from([
                ("id".into(), id(n)),
                ("bucket".into(), Value::Int64(bucket)),
                ("weight".into(), Value::Float64(weight)),
            ]),
        )
        .unwrap();
    }
    let query = HashMap::from([("q".to_string(), Value::Vector(vec![1.0, 0.0]))]);
    let ranked = vec![vec![id(3)], vec![id(1)], vec![id(2)]];

    let writer = db.execute(RANKED, &query).unwrap().rows;
    let owner = {
        let reader = contextdb_engine::ReadSession::open(&path).expect("read session on the owner");
        reader.execute(RANKED, &query).unwrap().rows
    };
    db.close().unwrap();
    drop(db);
    let file = {
        let reader = contextdb_engine::ReadSession::open(&path).expect("read session on the file");
        reader.execute(RANKED, &query).unwrap().rows
    };
    eprintln!("ranked order: writer {writer:?}, owner {owner:?}, file {file:?}");
    for (route, rows) in [
        ("writer", writer),
        ("owner route", owner),
        ("file route", file),
    ] {
        assert_eq!(
            rows, ranked,
            "{route}: the rank policy's joined weights order the answer"
        );
    }
}

/// Equal scores across partitions are cut by the canonical row-id order on
/// every route: twelve rows with one embedding, inserted round-robin over four
/// partitions, so a LIMIT of five cuts inside the tie and must keep the five
/// rows stored first, whichever partitions hold them.
#[test]
fn an_equal_score_cutoff_across_partitions_keeps_row_id_order_on_every_route() {
    const CUTOFF: &str = "SELECT id FROM docs ORDER BY embedding <=> $q LIMIT 5";
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("tied-routes.redb");
    let db = open(&path);
    db.execute(
        "CREATE TABLE docs (id UUID PRIMARY KEY, scope INTEGER NOT NULL, \
         embedding VECTOR(2) PARTITION_KEY (scope) MAX_PARTITIONS 16) SYNC OFF",
        &HashMap::new(),
    )
    .unwrap();
    for n in 1..=12u128 {
        db.execute(
            "INSERT INTO docs (id, scope, embedding) VALUES ($id, $scope, $embedding)",
            &HashMap::from([
                ("id".into(), id(n)),
                ("scope".into(), Value::Int64((n % 4) as i64)),
                ("embedding".into(), Value::Vector(vec![0.6, 0.8])),
            ]),
        )
        .unwrap();
    }
    let query = HashMap::from([("q".to_string(), Value::Vector(vec![1.0, 0.0]))]);
    let first_five = (1..=5).map(|n| vec![id(n)]).collect::<Vec<_>>();

    let writer = db.execute(CUTOFF, &query).unwrap().rows;
    let owner = {
        let reader = contextdb_engine::ReadSession::open(&path).expect("read session on the owner");
        reader.execute(CUTOFF, &query).unwrap().rows
    };
    db.close().unwrap();
    drop(db);
    let file = {
        let reader = contextdb_engine::ReadSession::open(&path).expect("read session on the file");
        reader.execute(CUTOFF, &query).unwrap().rows
    };
    for (route, rows) in [
        ("writer", writer),
        ("owner route", owner),
        ("file route", file),
    ] {
        assert_eq!(
            rows, first_five,
            "{route}: equal scores keep the rows stored first"
        );
    }
}
