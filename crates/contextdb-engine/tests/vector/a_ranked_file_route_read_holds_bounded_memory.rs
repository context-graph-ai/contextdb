//! A ranked search on the read-only file route holds the same transient
//! memory however much its rank policy's join visits: one visited index
//! identity at a time however long a run of matching joined rows is, and one
//! joined body at a time however many candidates are ranked. Each case also
//! answers the same on the writer, the owner route and the file route.

use contextdb_core::Value;
use contextdb_core::read_contract::ReadLimits;
use contextdb_engine::vector_read_memory::{
    take_reader_budget_ledger_for_test, watch_reader_budget_ledger_for_test,
};
use contextdb_engine::{
    Database, DatabaseOpenOptions, MaintenancePolicy, ReadSession, ReadSessionOptions,
};
use std::collections::HashMap;
use uuid::Uuid;

fn id(n: u128) -> Value {
    Value::Uuid(Uuid::from_u128(n))
}

/// Room for a long joined run, so the reads below are bounded by what they
/// hold rather than refused for the work of walking the run.
fn limits() -> ReadLimits {
    ReadLimits {
        work: 5_000_000,
        memory: 512 * 1024 * 1024,
        ..ReadLimits::default()
    }
}

fn open(path: &std::path::Path) -> Database {
    let mut options = DatabaseOpenOptions {
        maintenance_policy: MaintenancePolicy::CallerDriven,
        ..DatabaseOpenOptions::default()
    };
    options.owner_reads.limits.limits = limits();
    Database::open_with_options(path, options).expect("open the ranked fixture")
}

fn read_session(path: &std::path::Path) -> ReadSession {
    ReadSession::open_with_options(
        path,
        ReadSessionOptions {
            limits: limits(),
            ..ReadSessionOptions::default()
        },
    )
    .expect("read session")
}

fn run(db: &Database, sql: &str, params: HashMap<String, Value>) {
    db.execute(sql, &params).unwrap();
}

const RANKED: &str =
    "SELECT id FROM decisions ORDER BY embedding <=> $q USE RANK effective LIMIT 3";

fn query() -> HashMap<String, Value> {
    HashMap::from([("q".to_string(), Value::Vector(vec![1.0, 0.0]))])
}

/// The ranked answer on the writer, the owner route and the file route, and
/// the most the file route's reader held above where it started while it
/// answered.
fn answer_on_every_route(db: Database, path: &std::path::Path) -> (Vec<Vec<Value>>, usize) {
    let writer = db.execute(RANKED, &query()).unwrap().rows;
    let owner = {
        let reader = read_session(path);
        reader.execute(RANKED, &query()).unwrap().rows
    };
    db.close().unwrap();
    drop(db);
    let reader = read_session(path);
    watch_reader_budget_ledger_for_test();
    let file = reader.execute(RANKED, &query()).unwrap().rows;
    let ledger = take_reader_budget_ledger_for_test();
    let start = ledger
        .first()
        .map_or(0, |entry| entry.used_after.saturating_sub(entry.bytes));
    let peak = ledger
        .iter()
        .map(|entry| entry.used_after)
        .max()
        .unwrap_or(0);
    assert_eq!(owner, writer, "the owner route answers as the writer does");
    assert_eq!(file, writer, "the file route answers as the writer does");
    (writer, peak.saturating_sub(start))
}

/// One decision whose rank weight joins on `bucket`, against a run of `run`
/// outcome rows that all share that bucket. The newest of them carries the
/// weight that decides the order.
fn duplicate_run(composite: bool, run_length: u128) -> (Vec<Vec<Value>>, usize) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("ranked-run.redb");
    let db = open(&path);
    run(
        &db,
        "CREATE TABLE outcomes (id UUID PRIMARY KEY, bucket INTEGER, seq INTEGER, weight REAL) SYNC OFF",
        HashMap::new(),
    );
    run(
        &db,
        if composite {
            "CREATE INDEX outcomes_bucket_idx ON outcomes (bucket, seq)"
        } else {
            "CREATE INDEX outcomes_bucket_idx ON outcomes (bucket)"
        },
        HashMap::new(),
    );
    run(
        &db,
        "CREATE TABLE decisions (
            id UUID PRIMARY KEY,
            bucket INTEGER,
            embedding VECTOR(2) RANK_POLICY (
                JOIN outcomes ON bucket,
                FORMULA 'coalesce({weight}, 0.0)',
                SORT_KEY effective
            )
        ) SYNC OFF",
        HashMap::new(),
    );
    // Similarity alone orders 2, 1; the weight on bucket 1 lifts 1 first.
    for (n, bucket, embedding) in [(1, 1, vec![0.6_f32, 0.8]), (2, 2, vec![0.8, 0.6])] {
        run(
            &db,
            "INSERT INTO decisions (id, bucket, embedding) VALUES ($id, $bucket, $embedding)",
            HashMap::from([
                ("id".into(), id(n)),
                ("bucket".into(), Value::Int64(bucket)),
                ("embedding".into(), Value::Vector(embedding)),
            ]),
        );
    }
    // Inserted 200 rows to a statement.
    let mut seq = 0;
    while seq < run_length {
        let batch = (run_length - seq).min(200);
        let mut sql = String::from("INSERT INTO outcomes (id, bucket, seq, weight) VALUES ");
        let mut params = HashMap::new();
        for offset in 0..batch {
            let row = seq + offset;
            if offset > 0 {
                sql.push_str(", ");
            }
            sql.push_str(&format!("($i{row}, 1, $s{row}, 50.0)"));
            params.insert(format!("i{row}"), id(1_000_000 + row));
            params.insert(format!("s{row}"), Value::Int64(row as i64));
        }
        run(&db, &sql, params);
        seq += batch;
    }
    answer_on_every_route(db, &path)
}

#[test]
fn a_ranked_file_route_join_holds_one_visited_identity_at_a_time() {
    // A held identity costs 64 B, so a join that kept every visited identity
    // would grow by at least that per extra row. Both index shapes hold one
    // visited row at a time, so the peak is the same however long the run of
    // matches: a single-column run of 4 or 400, a composite run of 400 or
    // 32,000 (S9 measured the plain ordered walk flat to 32k postings).
    for (composite, short, long, per_extra_row) in [(false, 4, 400, 0), (true, 400, 32_000, 0)] {
        let (short_answer, short_peak) = duplicate_run(composite, short);
        let (long_answer, long_peak) = duplicate_run(composite, long);
        eprintln!(
            "composite {composite}: transient peak {short_peak} B over a {short}-row run, {long_peak} B over a {long}-row run"
        );
        assert_eq!(short_answer, vec![vec![id(1)], vec![id(2)]]);
        assert_eq!(long_answer, short_answer);
        let allowance = 4096 + (long - short) as usize * per_extra_row;
        assert!(
            long_peak <= short_peak + allowance,
            "composite {composite}: a longer run of matches must not hold more identities \
             ({short_peak} B over {short} rows, {long_peak} B over {long})"
        );
    }
}

/// `candidates` decisions ranked through joined outcome rows with 32 KiB
/// bodies; half of the decisions share one joined row.
fn many_candidates(candidates: u128) -> (Vec<Vec<Value>>, usize) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("ranked-bodies.redb");
    let db = open(&path);
    for statement in [
        "CREATE TABLE outcomes (id UUID PRIMARY KEY, bucket INTEGER, weight REAL, note TEXT) SYNC OFF",
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
        run(&db, statement, HashMap::new());
    }
    let note = "n".repeat(32 * 1024);
    for n in 0..candidates {
        // Even decisions share bucket 0; odd ones each have their own.
        let bucket = if n % 2 == 0 { 0 } else { n as i64 };
        let angle = 0.1 + (n as f32) * 0.01;
        run(
            &db,
            "INSERT INTO decisions (id, bucket, embedding) VALUES ($id, $bucket, $embedding)",
            HashMap::from([
                ("id".into(), id(n + 1)),
                ("bucket".into(), Value::Int64(bucket)),
                (
                    "embedding".into(),
                    Value::Vector(vec![angle.cos(), angle.sin()]),
                ),
            ]),
        );
        if n % 2 == 1 || n == 0 {
            // The weight grows with n.
            run(
                &db,
                "INSERT INTO outcomes (id, bucket, weight, note) VALUES ($id, $bucket, $weight, $note)",
                HashMap::from([
                    ("id".into(), id(10_000 + n)),
                    ("bucket".into(), Value::Int64(bucket)),
                    (
                        "weight".into(),
                        Value::Float64(if n == 0 { 0.0 } else { n as f64 }),
                    ),
                    ("note".into(), Value::Text(note.clone())),
                ]),
            );
        }
    }
    answer_on_every_route(db, &path)
}

#[test]
fn a_ranked_file_route_returns_each_joined_body_after_its_evaluation() {
    let (few_answer, few_peak) = many_candidates(8);
    let (many_answer, many_peak) = many_candidates(48);
    eprintln!("transient peak {few_peak} B over 8 candidates, {many_peak} B over 48");
    assert_eq!(few_answer.len(), 3);
    assert_eq!(many_answer.len(), 3);
    // Forty more evaluations each keeping a 32 KiB body would be over 1 MiB more.
    assert!(
        many_peak <= few_peak + 16 * 1024,
        "ranking more candidates must not keep their joined bodies \
         ({few_peak} B over 8 candidates, {many_peak} B over 48)"
    );
}
