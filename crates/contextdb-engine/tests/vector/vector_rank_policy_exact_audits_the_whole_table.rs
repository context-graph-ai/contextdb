//! `USE VECTOR EXACT` on a ranked read promises to evaluate the rank formula
//! over every allowed stored vector before the final `LIMIT` -- not over a
//! candidate pool trimmed by cosine similarity first.
//!
//! The table below declares EF_SEARCH = 1,500 and holds more rows than that
//! bounded candidate pool. One row -- the "needle" -- is built so it is the least
//! cosine-similar row in the whole table: by construction it can never be
//! among the nearest 1,500 by cosine, so a candidate-pool cap of 1,500 always
//! excludes it before any rank formula runs. The needle also carries, by far,
//! the highest rank-formula weight of any row in the table.
//!
//! `USE VECTOR EXACT` must return the needle: the reference promises the
//! formula is evaluated over every allowed stored vector, and the needle's
//! weight makes it the best-scoring row in the table. `INDEXED` is not
//! required to return it -- ranking only the bounded candidate set is that
//! route's contract, and it must stay intact. Both halves are pinned here
//! together so neither can be "fixed" by breaking the other.
//!
//! Every read below goes through a real production entrance: ordinary SQL,
//! the Rust `SemanticQuery` API, and the memory-bounded read session.

use contextdb_core::read_contract::ReadLimits;
use contextdb_core::{Value, VectorSearchMode};
use contextdb_engine::{Database, MaintenancePolicy, SemanticQuery};
use std::collections::HashMap;
use uuid::Uuid;

/// Rows that exist only to fill the table past the internal candidate cap
/// and to crowd the cosine-similarity ranking above the needle. None of them
/// carries any rank-formula weight.
const HAYSTACK_ROWS: u32 = 1_999;
/// The answer size every read below asks for.
const LIMIT: usize = 10;
/// The rank-formula weight every haystack row's joined outcome carries.
const HAYSTACK_WEIGHT: f64 = 0.0;
/// The rank-formula weight the needle's joined outcome carries: far above
/// anything a haystack row could ever contribute.
const NEEDLE_WEIGHT: f64 = 1_000_000.0;

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn params(pairs: impl IntoIterator<Item = (&'static str, Value)>) -> HashMap<String, Value> {
    pairs
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

/// A 2-D unit vector whose cosine similarity to `query_vector()` is exactly
/// `score`. Constructing similarity directly, rather than trusting a proxy
/// like Euclidean distance, is what makes the needle's exclusion from the
/// nearest-1,500 pool a property of the fixture rather than an accident of
/// one similarity metric.
fn vector_for_cosine(score: f32) -> Vec<f32> {
    vec![score, (1.0 - score * score).max(0.0).sqrt()]
}

fn query_vector() -> Value {
    Value::Vector(vec![1.0, 0.0])
}

fn passage_id(ordinal: u32) -> Uuid {
    Uuid::from_u128(0x9A55_A6E5_0000_0000_0000_0000_0000_0000 + ordinal as u128)
}

fn needle_id() -> Uuid {
    passage_id(HAYSTACK_ROWS)
}

fn roomy_limits() -> ReadLimits {
    ReadLimits {
        result_rows: 1_000,
        result_bytes: 16 * 1024 * 1024,
        work: 100_000_000,
        active_ms: 1_000_000,
        memory: 64 * 1024 * 1024,
        cursor_page_rows: 128,
        cursor_page_bytes: 4 * 1024 * 1024,
        cursor_idle_ms: 10_000,
        cursor_lifetime_ms: 100_000,
    }
}

fn insert_passage(db: &Database, id: Uuid, vector: Vec<f32>) {
    db.execute(
        "INSERT INTO passages (id, embedding) VALUES ($id, $embedding)",
        &params([
            ("id", Value::Uuid(id)),
            ("embedding", Value::Vector(vector)),
        ]),
    )
    .expect("store a passage row");
}

fn insert_outcome(db: &Database, outcome_id: Uuid, passage: Uuid, weight: f64) {
    db.execute(
        "INSERT INTO passage_outcomes (id, passage_id, weight) VALUES ($id, $passage, $weight)",
        &params([
            ("id", Value::Uuid(outcome_id)),
            ("passage", Value::Uuid(passage)),
            ("weight", Value::Float64(weight)),
        ]),
    )
    .expect("store a passage's rank-formula outcome row");
}

fn drive_maintenance(db: &Database) {
    // A fixed number of finite calls is the public caller-driven operation;
    // this is not a timing wait.
    for _ in 0..32 {
        db.run_maintenance_cycle()
            .expect("one caller-driven maintenance batch succeeds");
    }
}

/// A ranked passage table holding more rows than its declared candidate breadth,
/// with the rank policy's `weight` column supplied by a joined outcomes
/// table, exactly as production rank policies are declared.
///
/// The needle sits deterministically outside the nearest-1,500-by-cosine
/// pool: every haystack row is built with a cosine similarity strictly
/// between 0.5 and 1.0, while the needle's similarity is -0.9. With 1,999
/// haystack rows all more similar than the needle, the needle is the least
/// similar of all 2,000 rows -- outside the top 1,500 by a margin of 500,
/// not by chance.
fn passage_table_with_a_buried_best_scoring_row() -> Database {
    let db = Database::open_memory();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE passage_outcomes (id UUID PRIMARY KEY, passage_id UUID, weight REAL)",
        &empty(),
    )
    .expect("create the rank-formula outcome table");
    db.execute(
        "CREATE INDEX passage_outcomes_passage_idx ON passage_outcomes(passage_id)",
        &empty(),
    )
    .expect("declare the joined index the rank policy relies on");
    db.execute(
        "CREATE TABLE passages (
            id UUID PRIMARY KEY,
            embedding VECTOR(2) SEARCH_MODE INDEXED HNSW (EF_SEARCH = 1500) RANK_POLICY (
                JOIN passage_outcomes ON passage_id,
                FORMULA 'coalesce({weight}, 0.0)',
                SORT_KEY effective_confidence
            )
        )",
        &empty(),
    )
    .expect("create the ranked passage table");

    for ordinal in 0..HAYSTACK_ROWS {
        // Spread strictly across (0.5, 1.0) so every haystack row outranks
        // the needle on cosine similarity, none of them tie each other
        // exactly, and none collides with the needle's own vector.
        let score = 0.5 + 0.499 * (ordinal as f32 / (HAYSTACK_ROWS - 1) as f32);
        let id = passage_id(ordinal);
        insert_passage(&db, id, vector_for_cosine(score));
        insert_outcome(
            &db,
            Uuid::from_u128(0xEEEE_0000_0000_0000_0000_0000_0000_0000 + ordinal as u128),
            id,
            HAYSTACK_WEIGHT,
        );
    }

    insert_passage(&db, needle_id(), vector_for_cosine(-0.9));
    insert_outcome(
        &db,
        Uuid::from_u128(0xFFFF_0000_0000_0000_0000_0000_0000_0001),
        needle_id(),
        NEEDLE_WEIGHT,
    );

    drive_maintenance(&db);
    db
}

const EXACT_RANKED_SQL: &str = "SELECT id FROM passages ORDER BY embedding <=> $query \
                                 USE VECTOR EXACT USE RANK effective_confidence LIMIT 10";
const INDEXED_RANKED_SQL: &str = "SELECT id FROM passages ORDER BY embedding <=> $query \
                                   USE RANK effective_confidence LIMIT 10";

fn ranked_query_params() -> HashMap<String, Value> {
    params([("query", query_vector())])
}

fn result_ids(result: &contextdb_engine::QueryResult) -> Vec<Uuid> {
    let id_idx = result
        .columns
        .iter()
        .position(|name| name == "id" || name.rsplit('.').next() == Some("id"))
        .expect("the projection contains id");
    result
        .rows
        .iter()
        .map(|row| match row.get(id_idx) {
            Some(Value::Uuid(id)) => *id,
            other => panic!("expected an id-leading ranked row, got {other:?}"),
        })
        .collect()
}

/// `USE VECTOR EXACT` through ordinary SQL must evaluate the rank formula
/// over every allowed stored vector, including the needle -- not only over a
/// candidate pool trimmed to the nearest 1,500 by cosine.
#[test]
fn exact_search_mode_reaches_the_best_scoring_row_through_sql() {
    let db = passage_table_with_a_buried_best_scoring_row();
    let result = db
        .execute(EXACT_RANKED_SQL, &ranked_query_params())
        .expect("USE VECTOR EXACT must be served");
    let ids = result_ids(&result);
    assert_eq!(ids.len(), LIMIT);
    assert!(
        ids.contains(&needle_id()),
        "USE VECTOR EXACT promises to evaluate the rank formula over every allowed stored \
         vector; the needle carries by far the highest weight ({NEEDLE_WEIGHT}) of any row in \
         the table but sits outside the nearest 1,500 rows by cosine similarity, and a \
         candidate pool capped at 1,500 excludes it before the formula ever runs. Got {ids:?}"
    );
}

/// The same promise through the Rust `SemanticQuery` API with
/// `search_mode: Some(Exact)` and a rank-policy sort key.
#[test]
fn exact_search_mode_reaches_the_best_scoring_row_through_the_rust_semantic_query() {
    let db = passage_table_with_a_buried_best_scoring_row();
    let mut query = SemanticQuery::new("passages", "embedding", vec![1.0, 0.0], LIMIT);
    query.sort_key = Some("effective_confidence".to_owned());
    query.search_mode = Some(VectorSearchMode::Exact);
    let results = db
        .semantic_search(query)
        .expect("the Rust EXACT override with a rank-policy sort key must be served");
    assert_eq!(results.len(), LIMIT);
    let ids: Vec<Uuid> = results
        .iter()
        .map(|r| match r.values.get("id") {
            Some(Value::Uuid(id)) => *id,
            other => panic!("expected a ranked result carrying a UUID id, got {other:?}"),
        })
        .collect();
    assert!(
        ids.contains(&needle_id()),
        "the Rust SemanticQuery EXACT override promises to evaluate the rank formula over every \
         allowed stored vector; the needle should be the top result by weight, but a candidate \
         pool capped at 1,500 by cosine excludes it before the formula ever runs. Got {ids:?}"
    );
}

/// The same promise through the memory-bounded read session: the ordinary
/// and bounded readers must agree, so the ordinary door alone is not enough.
#[test]
fn exact_search_mode_reaches_the_best_scoring_row_through_the_bounded_read_session() {
    let db = passage_table_with_a_buried_best_scoring_row();
    let session = db
        .read_session(roomy_limits())
        .expect("open a bounded read view");
    let result = session
        .execute(EXACT_RANKED_SQL, &ranked_query_params())
        .expect("the bounded reader must serve USE VECTOR EXACT");
    let ids = result_ids(&result);
    assert_eq!(ids.len(), LIMIT);
    assert!(
        ids.contains(&needle_id()),
        "the bounded read session must agree with the ordinary door: USE VECTOR EXACT \
         evaluates the rank formula over every allowed stored vector, but a candidate pool \
         capped at 1,500 by cosine excludes the needle before the formula ever runs. Got {ids:?}"
    );
}

/// `INDEXED` (the column's declared default here) ranks only the bounded
/// candidate set -- it is not required to reach a best-scoring row that sits
/// outside that set. This is the contract EXACT must not disturb; both
/// readers are pinned here in the same place as the EXACT proofs above so
/// neither can be "fixed" by breaking the other.
#[test]
fn indexed_search_mode_is_not_required_to_reach_past_its_bounded_candidate_set() {
    let db = passage_table_with_a_buried_best_scoring_row();

    let ordinary = db
        .execute(INDEXED_RANKED_SQL, &ranked_query_params())
        .expect("the declared INDEXED route must be served once the graph is built");
    assert_eq!(result_ids(&ordinary).len(), LIMIT);
    assert!(
        !result_ids(&ordinary).contains(&needle_id()),
        "INDEXED ranks only the bounded candidate set; it must not reach past it to the \
         needle even though the needle carries the highest weight in the table"
    );

    let session = db
        .read_session(roomy_limits())
        .expect("open a bounded read view");
    let bounded = session
        .execute(INDEXED_RANKED_SQL, &ranked_query_params())
        .expect("the bounded reader must serve the declared INDEXED route");
    assert!(
        !result_ids(&bounded).contains(&needle_id()),
        "the bounded reader must agree with the ordinary door: INDEXED ranks only the bounded \
         candidate set, so it must not reach the needle either"
    );
}
