use contextdb_core::Value;
use contextdb_engine::{Database, SearchResult, SemanticQuery};
use criterion::{Criterion, criterion_group, criterion_main};
use std::collections::HashMap;
use std::hint::black_box;
use uuid::Uuid;

const DIMS: usize = 768;
const ROWS: usize = 2_000;
const LIMIT: usize = 50;
const TOP_K: usize = 10;
const BOOST_START: usize = 1_000;
const BOOST_COUNT: usize = LIMIT;

fn params(pairs: Vec<(&str, Value)>) -> HashMap<String, Value> {
    pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
}

fn uuid(n: usize) -> Uuid {
    Uuid::from_u128(900_000 + n as u128)
}

fn vector_for_cosine(score: f32) -> Vec<f32> {
    let mut vector = vec![0.0; DIMS];
    vector[0] = score;
    vector[1] = (1.0 - score * score).max(0.0).sqrt();
    vector
}

fn query_vec() -> Vec<f32> {
    let mut query = vec![0.0; DIMS];
    query[0] = 1.0;
    query
}

fn create_schema(db: &Database, table: &str, quantization_clause: &str) {
    db.execute(
        "CREATE TABLE observations (id UUID PRIMARY KEY, confidence REAL)",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(
        "CREATE INDEX observations_id_idx ON observations(id)",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(
        &format!(
            "CREATE TABLE {table} (
                id UUID PRIMARY KEY,
                observation_id UUID,
                vector_text VECTOR({DIMS}) {quantization_clause} RANK_POLICY (
                    JOIN observations ON id,
                    FORMULA '{{vector_score}} * coalesce({{confidence}}, 1.0)',
                    SORT_KEY weighted
                )
            )"
        ),
        &HashMap::new(),
    )
    .unwrap();
}

fn seed(db: &Database, table: &str) {
    for n in 0..ROWS {
        let id = uuid(n);
        let score = 1.0 - (n as f32 * 0.0002);
        let confidence = if (BOOST_START..BOOST_START + BOOST_COUNT).contains(&n) {
            1.0
        } else {
            0.1
        };
        db.execute(
            "INSERT INTO observations (id, confidence) VALUES ($id, $confidence)",
            &params(vec![
                ("id", Value::Uuid(id)),
                ("confidence", Value::Float64(confidence)),
            ]),
        )
        .unwrap();
        db.execute(
            &format!(
                "INSERT INTO {table} (id, observation_id, vector_text) VALUES ($id, $obs, $vec)"
            ),
            &params(vec![
                ("id", Value::Uuid(id)),
                ("obs", Value::Uuid(id)),
                ("vec", Value::Vector(vector_for_cosine(score))),
            ]),
        )
        .unwrap();
    }
}

fn search_results(
    db: &Database,
    table: &str,
    sort_key: Option<&str>,
    limit: usize,
) -> Vec<SearchResult> {
    let mut query = SemanticQuery::new(table, "vector_text", query_vec(), limit);
    query.sort_key = sort_key.map(str::to_string);
    db.semantic_search(query).unwrap()
}

fn result_ids(results: &[SearchResult]) -> Vec<Uuid> {
    results
        .iter()
        .map(|result| match result.values.get("id") {
            Some(Value::Uuid(id)) => *id,
            other => panic!("result missing UUID id: {other:?}"),
        })
        .collect()
}

fn expected_policy_ids() -> Vec<Uuid> {
    (BOOST_START..BOOST_START + LIMIT).map(uuid).collect()
}

fn recall(actual: &[Uuid], expected: &[Uuid]) -> f32 {
    let hits = actual.iter().filter(|id| expected.contains(id)).count();
    hits as f32 / expected.len() as f32
}

fn assert_finite_rank(results: &[SearchResult]) {
    assert!(
        results.iter().all(|result| result.rank.is_finite()),
        "RED: every rank-policy result must carry a finite rank"
    );
}

fn assert_rank_desc(results: &[SearchResult]) {
    assert!(
        results.windows(2).all(|pair| pair[0].rank >= pair[1].rank),
        "RED: rank-policy search must return rank-desc ordered results"
    );
}

fn mv11_recall_bench(c: &mut Criterion) {
    let f32_db = Database::open_memory();
    create_schema(&f32_db, "evidence_f32", "");
    seed(&f32_db, "evidence_f32");

    let sq8_db = Database::open_memory();
    create_schema(&sq8_db, "evidence_sq8", "WITH (quantization = 'SQ8')");
    seed(&sq8_db, "evidence_sq8");

    let expected = expected_policy_ids();
    let cosine_f32 = search_results(&f32_db, "evidence_f32", None, LIMIT);
    let cosine_sq8 = search_results(&sq8_db, "evidence_sq8", None, LIMIT);
    f32_db.__reset_rank_policy_eval_count();
    let policy_f32_a = search_results(&f32_db, "evidence_f32", Some("weighted"), LIMIT);
    let f32_eval_count = f32_db.__rank_policy_eval_count();
    f32_db.__reset_rank_policy_eval_count();
    let policy_f32_b = search_results(&f32_db, "evidence_f32", Some("weighted"), LIMIT);
    sq8_db.__reset_rank_policy_eval_count();
    let policy_sq8_a = search_results(&sq8_db, "evidence_sq8", Some("weighted"), LIMIT);
    let sq8_eval_count = sq8_db.__rank_policy_eval_count();
    sq8_db.__reset_rank_policy_eval_count();
    let policy_sq8_b = search_results(&sq8_db, "evidence_sq8", Some("weighted"), LIMIT);

    assert_eq!(
        policy_f32_a, policy_f32_b,
        "RED: F32 rank-policy search must be deterministic across consecutive runs"
    );
    assert_eq!(
        policy_sq8_a, policy_sq8_b,
        "RED: SQ8 rank-policy search must be deterministic across consecutive runs"
    );
    assert_finite_rank(&policy_f32_a);
    assert_finite_rank(&policy_sq8_a);
    assert_rank_desc(&policy_f32_a);
    assert_rank_desc(&policy_sq8_a);
    assert!(
        f32_eval_count > 0 && f32_eval_count < ROWS as u64,
        "RED: F32 rank-policy search must evaluate the ANN candidate set, not full-scan every row"
    );
    assert!(
        sq8_eval_count > 0 && sq8_eval_count < ROWS as u64,
        "RED: SQ8 rank-policy search must evaluate the ANN candidate set, not full-scan every row"
    );
    assert_eq!(
        result_ids(&policy_f32_a),
        expected,
        "RED: F32 rank-policy search must choose the boosted policy top-50 before recall is measured"
    );

    let cosine_recall = recall(&result_ids(&cosine_sq8), &result_ids(&cosine_f32));
    let policy_f32_ids = result_ids(&policy_f32_a);

    c.bench_function("mv11_recall_bench", |b| {
        b.iter(|| {
            sq8_db.__reset_rank_policy_eval_count();
            let policy_sq8 = search_results(
                black_box(&sq8_db),
                black_box("evidence_sq8"),
                Some("weighted"),
                LIMIT,
            );
            let eval_count = sq8_db.__rank_policy_eval_count();
            assert!(
                eval_count > 0 && eval_count < ROWS as u64,
                "RED: SQ8 rank-policy search must remain bounded to ANN candidates during the bench"
            );
            assert_finite_rank(&policy_sq8);
            assert_rank_desc(&policy_sq8);
            let policy_sq8_ids = result_ids(&policy_sq8);
            let policy_recall = recall(&policy_sq8_ids, &policy_f32_ids);
            assert!(
                policy_recall >= cosine_recall - 0.05,
                "RED: rank-policy SQ8 recall must compose with the raw-cosine recall floor"
            );
            let top10_hits = policy_sq8_ids
                .iter()
                .take(TOP_K)
                .filter(|id| policy_f32_ids[..TOP_K].contains(id))
                .count();
            assert!(
                top10_hits >= 9,
                "RED: rank-policy SQ8 top-10 recall must keep at least 9 of 10 F32 policy results"
            );
        })
    });
}

criterion_group!(benches, mv11_recall_bench);
criterion_main!(benches);
