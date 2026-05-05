use contextdb_core::{Error, Lsn, Value, VectorIndexRef};
use contextdb_engine::sync_types::{ChangeSet, ConflictPolicies, ConflictPolicy, DdlChange};
use contextdb_engine::{Database, SearchResult, SemanticQuery};
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::thread;
use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::{Context, SubscriberExt};
use uuid::Uuid;

fn params(pairs: Vec<(&str, Value)>) -> HashMap<String, Value> {
    pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
}

fn empty_params() -> HashMap<String, Value> {
    HashMap::new()
}

fn uuid(n: u128) -> Uuid {
    Uuid::from_u128(n)
}

fn vector_for_cosine(score: f32) -> Vec<f32> {
    vector_for_cosine_dim(score, 2)
}

fn vector_for_cosine_dim(score: f32, dims: usize) -> Vec<f32> {
    let mut vector = vec![0.0; dims];
    vector[0] = score;
    vector[1] = (1.0 - score * score).max(0.0).sqrt();
    vector
}

fn query_vec() -> Vec<f32> {
    query_vec_dim(2)
}

fn query_vec_dim(dims: usize) -> Vec<f32> {
    let mut query = vec![0.0; dims];
    query[0] = 1.0;
    query
}

fn create_policy_schema(db: &Database, formula: &str) {
    db.execute(
        "CREATE TABLE outcomes (
            id UUID PRIMARY KEY,
            decision_id UUID,
            success BOOL,
            success_rate REAL,
            producer_quality REAL,
            confidence REAL,
            created_at INTEGER
        )",
        &empty_params(),
    )
    .expect("create outcomes");
    db.execute(
        "CREATE INDEX outcomes_decision_id_idx ON outcomes(decision_id)",
        &empty_params(),
    )
    .expect("index outcomes decision_id");
    let ddl = format!(
        "CREATE TABLE decisions (
            id UUID PRIMARY KEY,
            decision_type TEXT,
            description TEXT,
            confidence REAL,
            created_at INTEGER,
            embedding VECTOR(2) RANK_POLICY (
                JOIN outcomes ON decision_id,
                FORMULA '{}',
                SORT_KEY effective_confidence
            )
        )",
        formula.replace('\'', "''")
    );
    db.execute(&ddl, &empty_params()).expect("create decisions");
}

fn create_standard_policy_schema(db: &Database) {
    create_policy_schema(db, "coalesce({confidence}, 1.0) * coalesce({success}, 1.0)");
}

fn create_plain_policy_shape_schema(db: &Database) {
    db.execute(
        "CREATE TABLE outcomes (
            id UUID PRIMARY KEY,
            decision_id UUID,
            success BOOL,
            success_rate REAL,
            producer_quality REAL,
            confidence REAL,
            created_at INTEGER
        )",
        &empty_params(),
    )
    .expect("create plain outcomes");
    db.execute(
        "CREATE INDEX outcomes_decision_id_idx ON outcomes(decision_id)",
        &empty_params(),
    )
    .expect("index plain outcomes decision_id");
    db.execute(
        "CREATE TABLE decisions (
            id UUID PRIMARY KEY,
            decision_type TEXT,
            description TEXT,
            confidence REAL,
            created_at INTEGER,
            embedding VECTOR(2)
        )",
        &empty_params(),
    )
    .expect("create plain decisions");
}

fn insert_decision(
    db: &Database,
    id: Uuid,
    decision_type: &str,
    description: &str,
    confidence: Option<f64>,
    created_at: i64,
    vector: Vec<f32>,
) {
    db.execute(
        "INSERT INTO decisions (id, decision_type, description, confidence, created_at, embedding)
         VALUES ($id, $decision_type, $description, $confidence, $created_at, $embedding)",
        &params(vec![
            ("id", Value::Uuid(id)),
            ("decision_type", Value::Text(decision_type.to_string())),
            ("description", Value::Text(description.to_string())),
            (
                "confidence",
                confidence.map(Value::Float64).unwrap_or(Value::Null),
            ),
            ("created_at", Value::Int64(created_at)),
            ("embedding", Value::Vector(vector)),
        ]),
    )
    .expect("insert decision");
}

fn insert_outcome(
    db: &Database,
    id: Uuid,
    decision_id: Uuid,
    success: Option<bool>,
    success_rate: Option<f64>,
    producer_quality: Option<f64>,
) {
    db.execute(
        "INSERT INTO outcomes (id, decision_id, success, success_rate, producer_quality, confidence, created_at)
         VALUES ($id, $decision_id, $success, $success_rate, $producer_quality, $confidence, $created_at)",
        &params(vec![
            ("id", Value::Uuid(id)),
            ("decision_id", Value::Uuid(decision_id)),
            ("success", success.map(Value::Bool).unwrap_or(Value::Null)),
            (
                "success_rate",
                success_rate.map(Value::Float64).unwrap_or(Value::Null),
            ),
            (
                "producer_quality",
                producer_quality.map(Value::Float64).unwrap_or(Value::Null),
            ),
            ("confidence", Value::Float64(0.5)),
            ("created_at", Value::Int64(1)),
        ]),
    )
    .expect("insert outcome");
}

fn semantic_results(db: &Database, sort_key: Option<&str>, limit: usize) -> Vec<SearchResult> {
    let mut query = SemanticQuery::new("decisions", "embedding", query_vec(), limit);
    query.sort_key = sort_key.map(str::to_string);
    db.semantic_search(query).expect("semantic search")
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

fn result_ids_from_query(result: &contextdb_engine::QueryResult) -> Vec<Uuid> {
    let id_col = result.columns.iter().position(|c| c == "id").unwrap();
    result
        .rows
        .iter()
        .map(|row| match row[id_col] {
            Value::Uuid(id) => id,
            ref other => panic!("expected UUID id, got {other:?}"),
        })
        .collect()
}

fn sql_vector_search_ids_with_query(
    db: &Database,
    table: &str,
    column: &str,
    sort_key: Option<&str>,
    query: Vec<f32>,
    limit: usize,
) -> Vec<Uuid> {
    let sql = match sort_key {
        Some(sort_key) => {
            format!(
                "SELECT id FROM {table} ORDER BY {column} <=> $q USE RANK {sort_key} LIMIT {limit}"
            )
        }
        None => format!("SELECT id FROM {table} ORDER BY {column} <=> $q LIMIT {limit}"),
    };
    let result = db
        .execute(&sql, &params(vec![("q", Value::Vector(query))]))
        .unwrap();
    result_ids_from_query(&result)
}

fn seed_effective_confidence_fixture(db: &Database) -> [Uuid; 4] {
    let d1 = uuid(1);
    let d2 = uuid(2);
    let d3 = uuid(3);
    let d4 = uuid(4);
    insert_decision(
        db,
        d1,
        "framework",
        "d1",
        Some(0.9),
        10,
        vector_for_cosine(1.0),
    );
    insert_decision(
        db,
        d2,
        "framework",
        "d2",
        Some(0.8),
        20,
        vector_for_cosine(0.99),
    );
    insert_decision(
        db,
        d3,
        "runtime",
        "d3",
        Some(0.7),
        30,
        vector_for_cosine(0.80),
    );
    insert_decision(
        db,
        d4,
        "runtime",
        "d4",
        Some(0.6),
        40,
        vector_for_cosine(0.70),
    );
    insert_outcome(db, uuid(101), d1, Some(true), Some(1.0), Some(0.9));
    insert_outcome(db, uuid(102), d2, Some(false), Some(0.0), Some(0.2));
    [d1, d2, d3, d4]
}

fn seed_ten_candidate_policy_fixture(db: &Database) -> [Uuid; 10] {
    let mut ids = [Uuid::nil(); 10];
    let scores = [0.95, 0.90, 0.85, 0.80, 0.70, 0.60, 0.40, 0.30, 0.20, 0.10];
    for (i, id) in ids.iter_mut().enumerate() {
        *id = uuid(300 + i as u128);
        let decision_type = if matches!(i, 0 | 2 | 3 | 5 | 7 | 9) {
            "caching"
        } else {
            "routing"
        };
        insert_decision(
            db,
            *id,
            decision_type,
            "ten candidate",
            Some(0.1 + i as f64 * 0.1),
            20250100 + i as i64,
            vector_for_cosine(scores[i]),
        );
        insert_outcome(
            db,
            uuid(400 + i as u128),
            *id,
            Some(true),
            Some(1.0),
            Some(0.5),
        );
    }
    ids
}

fn expect_policy_order(db: &Database, expected: &[Uuid]) {
    db.__reset_rank_policy_eval_count();
    let results = semantic_results(db, Some("effective_confidence"), expected.len());
    assert_eq!(result_ids(&results), expected);
    assert_eq!(db.__rank_policy_eval_count(), expected.len() as u64);
}

fn expect_rank_policy_not_found(
    result: contextdb_core::Result<contextdb_engine::QueryResult>,
    expected_index: &str,
    expected_sort_key: &str,
) {
    match result {
        Err(Error::RankPolicyNotFound { index, sort_key }) => {
            assert_eq!(index, expected_index);
            assert_eq!(sort_key, expected_sort_key);
        }
        other => panic!("expected RankPolicyNotFound, got {other:?}"),
    }
}

fn expect_search_rank_policy_not_found(
    result: contextdb_core::Result<Vec<SearchResult>>,
    expected_index: &str,
    expected_sort_key: &str,
) {
    match result {
        Err(Error::RankPolicyNotFound { index, sort_key }) => {
            assert_eq!(index, expected_index);
            assert_eq!(sort_key, expected_sort_key);
        }
        other => panic!("expected RankPolicyNotFound, got {other:?}"),
    }
}

fn expect_join_table_unknown(
    result: contextdb_core::Result<contextdb_engine::QueryResult>,
    expected_index: &str,
    expected_table: &str,
) {
    match result {
        Err(Error::RankPolicyJoinTableUnknown { index, table }) => {
            assert_eq!(index, expected_index);
            assert_eq!(table, expected_table);
        }
        other => panic!("expected RankPolicyJoinTableUnknown, got {other:?}"),
    }
}

fn expect_use_rank_requires_limit(result: contextdb_core::Result<contextdb_engine::QueryResult>) {
    assert!(
        matches!(result, Err(Error::UseRankRequiresLimit)),
        "expected UseRankRequiresLimit, got {result:?}"
    );
}

fn expect_use_rank_requires_vector_order(
    result: contextdb_core::Result<contextdb_engine::QueryResult>,
) {
    assert!(
        matches!(result, Err(Error::UseRankRequiresVectorOrder)),
        "expected UseRankRequiresVectorOrder, got {result:?}"
    );
}

fn create_rank_policy_on_text_anchor(
    db: &Database,
) -> contextdb_core::Result<contextdb_engine::QueryResult> {
    db.execute(
        "CREATE TABLE outcomes (id UUID PRIMARY KEY, decision_id UUID, success BOOL)",
        &empty_params(),
    )
    .unwrap();
    db.execute(
        "CREATE INDEX outcomes_decision_id_idx ON outcomes(decision_id)",
        &empty_params(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE decisions (
            id UUID PRIMARY KEY,
            confidence REAL,
            description TEXT RANK_POLICY (
                JOIN outcomes ON decision_id,
                FORMULA '1.0',
                SORT_KEY effective_confidence
            )
        )",
        &empty_params(),
    )
}

fn expect_precedence_error(
    result: contextdb_core::Result<contextdb_engine::QueryResult>,
    expected: &str,
) {
    match (result, expected) {
        (
            Err(Error::RankPolicyColumnType {
                index,
                column,
                expected,
                actual,
            }),
            "anchor-type",
        ) => {
            assert_eq!(index, "decisions.description");
            assert_eq!(column, "description");
            assert_eq!(expected, "VECTOR(N)");
            assert_eq!(actual, "TEXT");
        }
        (Err(Error::RankPolicyJoinTableUnknown { index, table }), "join-table") => {
            assert_eq!(index, "decisions.embedding");
            assert_eq!(table, "missing_outcomes");
        }
        (
            Err(Error::RankPolicyJoinColumnUnknown {
                index,
                table,
                column,
            }),
            "join-column",
        ) => {
            assert_eq!(index, "decisions.embedding");
            assert_eq!(table, "outcomes");
            assert_eq!(column, "missing_decision_id");
        }
        (
            Err(Error::RankPolicyJoinColumnUnindexed {
                index,
                joined_table,
                column,
            }),
            "join-unindexed",
        ) => {
            assert_eq!(index, "decisions.embedding");
            assert_eq!(joined_table, "outcomes");
            assert_eq!(column, "decision_id");
        }
        (
            Err(Error::RankPolicyFormulaParse {
                index,
                position,
                reason,
            }),
            "formula-parse",
        ) => {
            assert_eq!(index, "decisions.embedding");
            assert_eq!(position, 11);
            assert!(
                reason.contains("coalesce"),
                "formula parse reason must mention coalesce, got {reason}"
            );
        }
        (Err(Error::RankPolicyColumnUnknown { index, column }), "formula-unknown") => {
            assert_eq!(index, "decisions.embedding");
            assert_eq!(column, "bogus");
        }
        (Err(Error::RankPolicyColumnAmbiguous { index, column }), "formula-ambiguous") => {
            assert_eq!(index, "decisions.embedding");
            assert_eq!(column, "id");
        }
        (
            Err(Error::RankPolicyColumnType {
                index,
                column,
                expected,
                actual,
            }),
            "formula-type",
        ) => {
            assert_eq!(index, "decisions.embedding");
            assert_eq!(column, "description");
            assert_eq!(expected, "number-or-bool");
            assert_eq!(actual, "TEXT");
        }
        (other, _) => panic!("expected precedence error {expected}, got {other:?}"),
    }
}

fn expect_formula_parse_error(
    result: contextdb_core::Result<contextdb_engine::QueryResult>,
    expected_index: &str,
    expected_position: usize,
    reason_contains: &str,
) {
    match result {
        Err(Error::RankPolicyFormulaParse {
            index,
            position,
            reason,
        }) => {
            assert_eq!(index, expected_index);
            assert_eq!(position, expected_position);
            assert!(
                reason.contains(reason_contains),
                "formula parse reason `{reason}` must mention `{reason_contains}`"
            );
        }
        other => panic!("expected RankPolicyFormulaParse, got {other:?}"),
    }
}

fn expect_drop_blocked(
    result: contextdb_core::Result<contextdb_engine::QueryResult>,
    expected_table: &str,
    expected_column: Option<&str>,
    expected_dropped_index: Option<&str>,
    expected_policy_table: &str,
    expected_policy_column: &str,
    expected_sort_key: &str,
) {
    match result {
        Err(Error::DropBlockedByRankPolicy {
            table,
            column,
            dropped_index,
            policy_table,
            policy_column,
            sort_key,
        }) => {
            assert_eq!(table.as_ref(), expected_table);
            assert_eq!(column.as_deref(), expected_column);
            assert_eq!(dropped_index.as_deref(), expected_dropped_index);
            assert_eq!(policy_table.as_ref(), expected_policy_table);
            assert_eq!(policy_column.as_ref(), expected_policy_column);
            assert_eq!(sort_key.as_ref(), expected_sort_key);
        }
        other => panic!("expected DropBlockedByRankPolicy, got {other:?}"),
    }
}

fn create_bad_policy_table(
    db: &Database,
    column_def: &str,
    formula: &str,
) -> contextdb_core::Result<contextdb_engine::QueryResult> {
    db.execute(
        "CREATE TABLE outcomes (id UUID PRIMARY KEY, decision_id UUID, success BOOL)",
        &empty_params(),
    )
    .ok();
    db.execute(
        "CREATE INDEX outcomes_decision_id_idx ON outcomes(decision_id)",
        &empty_params(),
    )
    .ok();
    let ddl = format!(
        "CREATE TABLE decisions (
            id UUID PRIMARY KEY,
            description TEXT,
            payload JSON,
            confidence REAL,
            {column_def} RANK_POLICY (JOIN outcomes ON decision_id, FORMULA '{}', SORT_KEY effective_confidence)
        )",
        formula.replace('\'', "''")
    );
    db.execute(&ddl, &empty_params())
}

fn rank_policy_meta(db: &Database, table: &str, column: &str) -> contextdb_core::RankPolicy {
    db.table_meta(table)
        .expect("table meta")
        .columns
        .into_iter()
        .find(|col| col.name == column)
        .and_then(|col| col.rank_policy)
        .expect("rank policy")
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CapturedRankPolicyWarn {
    target: String,
    level: String,
    name: String,
    fields: HashMap<String, String>,
}

#[derive(Default)]
struct FieldRecorder {
    fields: HashMap<String, String>,
}

impl Visit for FieldRecorder {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.fields
            .insert(field.name().to_string(), format!("{value:?}"));
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.fields
            .insert(field.name().to_string(), value.to_string());
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.fields
            .insert(field.name().to_string(), value.to_string());
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.fields
            .insert(field.name().to_string(), value.to_string());
    }
}

#[derive(Clone)]
struct CaptureLayer {
    events: Arc<Mutex<Vec<CapturedRankPolicyWarn>>>,
}

impl<S> Layer<S> for CaptureLayer
where
    S: Subscriber,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        if event.metadata().target() == "rank_policy_eval_error" {
            let mut recorder = FieldRecorder::default();
            event.record(&mut recorder);
            self.events
                .lock()
                .expect("events lock")
                .push(CapturedRankPolicyWarn {
                    target: event.metadata().target().to_string(),
                    level: event.metadata().level().to_string(),
                    name: event.metadata().name().to_string(),
                    fields: recorder.fields,
                });
        }
    }
}

fn capture_rank_policy_warns<F: FnOnce()>(f: F) -> Vec<CapturedRankPolicyWarn> {
    let events = Arc::new(Mutex::new(Vec::new()));
    let layer = CaptureLayer {
        events: events.clone(),
    };
    let subscriber = tracing_subscriber::registry().with(layer);
    tracing::subscriber::with_default(subscriber, f);
    events.lock().expect("events lock").clone()
}

fn unquoted_field(value: Option<&String>) -> Option<&str> {
    value.map(|value| value.trim_matches('"'))
}

/// RED: happy-path DDL must populate the protected join index, not just parse text.
#[test]
fn dp01_happy_path_populates_rank_policy() {
    let db = Database::open_memory();
    create_standard_policy_schema(&db);
    let policy = rank_policy_meta(&db, "decisions", "embedding");
    assert_eq!(policy.joined_table, "outcomes");
    assert_eq!(policy.joined_column, "decision_id");
    assert_eq!(policy.sort_key, "effective_confidence");
    assert_eq!(policy.protected_index, "outcomes_decision_id_idx");
}

/// REGRESSION GUARD: a plain vector column must not acquire a rank policy.
#[test]
fn dp02_no_clause_yields_none() {
    let stmt = contextdb_parser::parse(
        "CREATE TABLE decisions (id UUID PRIMARY KEY, embedding VECTOR(2))",
    )
    .expect("parse");
    let contextdb_parser::Statement::CreateTable(table) = stmt else {
        panic!("expected create table");
    };
    let embedding = table
        .columns
        .iter()
        .find(|col| col.name == "embedding")
        .unwrap();
    assert!(embedding.rank_policy.is_none());
}

/// RED: RANK_POLICY must behave identically in every column-constraint position.
#[test]
fn dp03_any_constraint_position() {
    for ddl in [
        "CREATE TABLE decisions (id UUID PRIMARY KEY, embedding VECTOR(2) RANK_POLICY (JOIN outcomes ON decision_id, FORMULA '1.0', SORT_KEY effective_confidence) NOT NULL)",
        "CREATE TABLE decisions (id UUID PRIMARY KEY, embedding VECTOR(2) NOT NULL RANK_POLICY (JOIN outcomes ON decision_id, FORMULA '1.0', SORT_KEY effective_confidence))",
        "CREATE TABLE decisions (id UUID PRIMARY KEY, embedding VECTOR(2) DEFAULT '[1,0]' RANK_POLICY (JOIN outcomes ON decision_id, FORMULA '1.0', SORT_KEY effective_confidence))",
    ] {
        let db = Database::open_memory();
        db.execute(
            "CREATE TABLE outcomes (id UUID PRIMARY KEY, decision_id UUID)",
            &empty_params(),
        )
        .unwrap();
        db.execute(
            "CREATE INDEX outcomes_decision_id_idx ON outcomes(decision_id)",
            &empty_params(),
        )
        .unwrap();
        db.execute(ddl, &empty_params()).unwrap();
        assert_eq!(
            rank_policy_meta(&db, "decisions", "embedding").protected_index,
            "outcomes_decision_id_idx"
        );
    }
}

/// RED: duplicate RANK_POLICY clauses must be rejected with a column-specific parse error.
#[test]
fn dp04_duplicate_clauses_rejected() {
    let result = contextdb_parser::parse(
        "CREATE TABLE decisions (id UUID PRIMARY KEY, embedding VECTOR(2) RANK_POLICY (JOIN outcomes ON decision_id, FORMULA '1.0', SORT_KEY effective_confidence) RANK_POLICY (JOIN outcomes ON decision_id, FORMULA '1.0', SORT_KEY other_key))",
    );
    assert!(
        matches!(result, Err(Error::ParseError(ref msg)) if msg.contains("embedding")),
        "{result:?}"
    );
}

/// RED: a policy attached to a non-vector anchor column is rejected at DDL time.
#[test]
fn dp05_non_vector_anchor_rejected() {
    let db = Database::open_memory();
    let result = create_rank_policy_on_text_anchor(&db);
    assert!(
        matches!(result, Err(Error::RankPolicyColumnType { ref index, ref column, ref expected, ref actual })
            if index == "decisions.description"
                && column == "description"
                && expected == "VECTOR(N)"
                && actual == "TEXT"),
        "expected vector-column type error for decisions.description, got {result:?}"
    );
}

/// RED: unknown anchor formula references are rejected at DDL time.
#[test]
fn de01_unknown_anchor_column() {
    let db = Database::open_memory();
    let result = create_bad_policy_table(&db, "embedding VECTOR(2)", "coalesce({bogus}, 1.0)");
    assert!(
        matches!(result, Err(Error::RankPolicyColumnUnknown { ref index, ref column })
            if index == "decisions.embedding" && column == "bogus"),
        "expected unknown formula column `bogus`, got {result:?}"
    );
}

/// RED: ambiguous anchor/joined column references are rejected instead of resolved silently.
#[test]
fn de02_ambiguous_column() {
    let db = Database::open_memory();
    let result = create_bad_policy_table(&db, "embedding VECTOR(2)", "{id}");
    assert!(
        matches!(result, Err(Error::RankPolicyColumnAmbiguous { ref index, ref column })
            if index == "decisions.embedding" && column == "id"),
        "expected ambiguous formula column `id`, got {result:?}"
    );
}

/// RED: TEXT and JSON formula operands are rejected with actionable type errors.
#[test]
fn de03_unsupported_column_type() {
    for (formula, column, actual) in [
        ("{description}", "description", "TEXT"),
        ("{payload}", "payload", "JSON"),
    ] {
        let db = Database::open_memory();
        let result = create_bad_policy_table(&db, "embedding VECTOR(2)", formula);
        assert!(
            matches!(
                result,
                Err(Error::RankPolicyColumnType {
                    ref index,
                    column: ref got_column,
                    ref expected,
                    actual: ref got_actual,
                }) if index == "decisions.embedding"
                    && got_column == column
                    && expected == "number-or-bool"
                    && got_actual == actual
            ),
            "expected RankPolicyColumnType for {column} actual {actual}, got {result:?}"
        );
    }
}

/// RED: unknown join tables fail CREATE TABLE before search.
#[test]
fn de04_unknown_join_table() {
    let db = Database::open_memory();
    let result = db.execute(
        "CREATE TABLE decisions (id UUID PRIMARY KEY, confidence REAL, embedding VECTOR(2) RANK_POLICY (JOIN bogus_table ON decision_id, FORMULA '1.0', SORT_KEY effective_confidence))",
        &empty_params(),
    );
    assert!(
        matches!(result, Err(Error::RankPolicyJoinTableUnknown { ref index, ref table })
            if index == "decisions.embedding" && table == "bogus_table"),
        "expected unknown join table `bogus_table`, got {result:?}"
    );
}

/// RED: unknown join columns fail CREATE TABLE before search.
#[test]
fn de05_unknown_join_column() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE outcomes (id UUID PRIMARY KEY, decision_id UUID)",
        &empty_params(),
    )
    .unwrap();
    let result = db.execute(
        "CREATE TABLE decisions (id UUID PRIMARY KEY, confidence REAL, embedding VECTOR(2) RANK_POLICY (JOIN outcomes ON bogus_column, FORMULA '1.0', SORT_KEY effective_confidence))",
        &empty_params(),
    );
    assert!(
        matches!(result, Err(Error::RankPolicyJoinColumnUnknown { ref index, ref table, ref column })
            if index == "decisions.embedding" && table == "outcomes" && column == "bogus_column"),
        "expected unknown join column `outcomes.bogus_column`, got {result:?}"
    );
}

/// RED: join columns must be indexed and the error must include the suggested index DDL.
#[test]
fn de06_unindexed_join_column() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE outcomes (id UUID PRIMARY KEY, decision_id UUID)",
        &empty_params(),
    )
    .unwrap();
    let result = db.execute(
        "CREATE TABLE decisions (id UUID PRIMARY KEY, confidence REAL, embedding VECTOR(2) RANK_POLICY (JOIN outcomes ON decision_id, FORMULA '1.0', SORT_KEY effective_confidence))",
        &empty_params(),
    );
    assert!(
        matches!(result, Err(Error::RankPolicyJoinColumnUnindexed { ref joined_table, ref column, .. }) if joined_table == "outcomes" && column == "decision_id"),
        "expected unindexed join column, got {result:?}"
    );
    if let Err(err) = result {
        assert!(
            err.to_string()
                .contains("CREATE INDEX outcomes_decision_id_idx ON outcomes(decision_id);"),
            "message must include actionable index DDL, got {err}"
        );
    }
}

/// RED: malformed formulas are parsed eagerly at DDL time.
#[test]
fn de07_malformed_formula_at_ddl() {
    let db = Database::open_memory();
    expect_formula_parse_error(
        create_bad_policy_table(&db, "embedding VECTOR(2)", "coalesce("),
        "decisions.embedding",
        11,
        "coalesce",
    );
}

/// RED: DDL validation reports the deterministic first error as each prior defect is fixed.
#[test]
fn de08_error_precedence_walkthrough() {
    fn run_case(
        setup_outcomes: bool,
        index_join_column: bool,
        anchor_column: &str,
        join_table: &str,
        join_column: &str,
        formula: &str,
        expected: &str,
    ) {
        let db = Database::open_memory();
        if setup_outcomes {
            db.execute(
                "CREATE TABLE outcomes (id UUID PRIMARY KEY, decision_id UUID, success BOOL)",
                &empty_params(),
            )
            .unwrap();
            if index_join_column {
                db.execute(
                    "CREATE INDEX outcomes_decision_id_idx ON outcomes(decision_id)",
                    &empty_params(),
                )
                .unwrap();
            }
        }
        let description_column = if anchor_column.starts_with("description ") {
            ""
        } else {
            "description TEXT,"
        };
        let ddl = format!(
            "CREATE TABLE decisions (
                id UUID PRIMARY KEY,
                {description_column}
                payload JSON,
                confidence REAL,
                {anchor_column} RANK_POLICY (
                    JOIN {join_table} ON {join_column},
                    FORMULA '{}',
                    SORT_KEY effective_confidence
                )
            )",
            formula.replace('\'', "''")
        );
        expect_precedence_error(db.execute(&ddl, &empty_params()), expected);
    }

    run_case(
        false,
        false,
        "description TEXT",
        "missing_outcomes",
        "missing_decision_id",
        "coalesce(",
        "anchor-type",
    );
    run_case(
        false,
        false,
        "embedding VECTOR(2)",
        "missing_outcomes",
        "missing_decision_id",
        "coalesce(",
        "join-table",
    );
    run_case(
        true,
        false,
        "embedding VECTOR(2)",
        "outcomes",
        "missing_decision_id",
        "coalesce(",
        "join-column",
    );
    run_case(
        true,
        false,
        "embedding VECTOR(2)",
        "outcomes",
        "decision_id",
        "coalesce(",
        "join-unindexed",
    );
    run_case(
        true,
        true,
        "embedding VECTOR(2)",
        "outcomes",
        "decision_id",
        "coalesce(",
        "formula-parse",
    );
    run_case(
        true,
        true,
        "embedding VECTOR(2)",
        "outcomes",
        "decision_id",
        "{bogus}",
        "formula-unknown",
    );
    run_case(
        true,
        true,
        "embedding VECTOR(2)",
        "outcomes",
        "decision_id",
        "{id}",
        "formula-ambiguous",
    );
    run_case(
        true,
        true,
        "embedding VECTOR(2)",
        "outcomes",
        "decision_id",
        "{description}",
        "formula-type",
    );
}

/// REGRESSION GUARD: a clean CREATE TABLE stores the declared rank-policy metadata.
#[test]
fn de09_clean_create_table_succeeds() {
    let db = Database::open_memory();
    create_standard_policy_schema(&db);
    let policy = rank_policy_meta(&db, "decisions", "embedding");
    assert_eq!(policy.joined_table, "outcomes");
    assert_eq!(policy.joined_column, "decision_id");
    assert_eq!(policy.sort_key, "effective_confidence");
    assert_eq!(
        policy.formula,
        "coalesce({confidence}, 1.0) * coalesce({success}, 1.0)"
    );
}

/// RED: literal-only formulas produce a constant rank independent of cosine.
#[test]
fn fg01_literal_only() {
    let db = Database::open_memory();
    create_policy_schema(&db, "1.0");
    let ids = seed_effective_confidence_fixture(&db);
    let results = semantic_results(&db, Some("effective_confidence"), 4);
    assert_eq!(result_ids(&results), vec![ids[0], ids[1], ids[2], ids[3]]);
    assert!(results.iter().all(|result| result.rank == 1.0));
}

/// RED: BOOL formula operands coerce locally to 1.0 and 0.0.
#[test]
fn fg02_bool_coercion() {
    let db = Database::open_memory();
    create_policy_schema(&db, "coalesce({success}, 0.0)");
    let [d1, d2, _, _] = seed_effective_confidence_fixture(&db);
    let results = semantic_results(&db, Some("effective_confidence"), 2);
    assert_eq!(result_ids(&results), vec![d1, d2]);
    assert_eq!(results[0].rank, 1.0);
    assert_eq!(results[1].rank, 0.0);
}

/// RED: coalesce(expr, literal) substitutes only NULL operands.
#[test]
fn fg03_coalesce() {
    let db = Database::open_memory();
    create_policy_schema(&db, "coalesce({confidence}, 1.0)");
    let null_id = uuid(11);
    let conf_id = uuid(12);
    insert_decision(
        &db,
        null_id,
        "framework",
        "null",
        None,
        1,
        vector_for_cosine(0.7),
    );
    insert_decision(
        &db,
        conf_id,
        "framework",
        "conf",
        Some(0.3),
        2,
        vector_for_cosine(1.0),
    );
    let results = semantic_results(&db, Some("effective_confidence"), 2);
    assert_eq!(result_ids(&results), vec![null_id, conf_id]);
    assert_eq!(
        results.iter().map(|r| r.rank).collect::<Vec<_>>(),
        vec![1.0, 0.3]
    );
}

/// RED: multiplication binds tighter than addition in formula evaluation.
#[test]
fn fg04_mul_binds_tighter_than_add() {
    let db = Database::open_memory();
    create_policy_schema(&db, "1 + 2 * 3");
    seed_effective_confidence_fixture(&db);
    let results = semantic_results(&db, Some("effective_confidence"), 1);
    assert_eq!(results[0].rank, 7.0);
}

/// RED: parentheses override default precedence in formula evaluation.
#[test]
fn fg05_parens_override_precedence() {
    let db = Database::open_memory();
    create_policy_schema(&db, "(1 + 2) * 3");
    seed_effective_confidence_fixture(&db);
    let results = semantic_results(&db, Some("effective_confidence"), 1);
    assert_eq!(results[0].rank, 9.0);
}

/// RED: {vector_score} participates in formulas and cannot be dropped by the compiler.
#[test]
fn fg07_cosine_times_confidence() {
    let db = Database::open_memory();
    create_policy_schema(&db, "{vector_score} * coalesce({confidence}, 1.0)");
    let a = uuid(21);
    let b = uuid(22);
    insert_decision(
        &db,
        a,
        "framework",
        "a",
        Some(0.5),
        1,
        vector_for_cosine(0.9),
    );
    insert_decision(&db, b, "framework", "b", None, 2, vector_for_cosine(0.7));
    expect_policy_order(&db, &[b, a]);
}

/// RED: a real column named vector_score is rejected because the name is reserved.
#[test]
fn fg08_vector_score_reserved() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE outcomes (id UUID PRIMARY KEY, decision_id UUID)",
        &empty_params(),
    )
    .unwrap();
    db.execute(
        "CREATE INDEX outcomes_decision_id_idx ON outcomes(decision_id)",
        &empty_params(),
    )
    .unwrap();
    let result = db.execute(
        "CREATE TABLE decisions (id UUID PRIMARY KEY, vector_score REAL, embedding VECTOR(2) RANK_POLICY (JOIN outcomes ON decision_id, FORMULA '{vector_score}', SORT_KEY effective_confidence))",
        &empty_params(),
    );
    assert!(
        matches!(result, Err(Error::RankPolicyColumnAmbiguous { ref index, ref column })
            if index == "decisions.embedding" && column == "vector_score"),
        "expected reserved vector_score ambiguity, got {result:?}"
    );
}

/// RED: CASE expressions remain outside the formula grammar.
#[test]
fn fg09_case_rejected() {
    let db = Database::open_memory();
    expect_formula_parse_error(
        create_bad_policy_table(
            &db,
            "embedding VECTOR(2)",
            "CASE WHEN {confidence} THEN 1 END",
        ),
        "decisions.embedding",
        1,
        "CASE",
    );
}

/// RED: subqueries remain outside the formula grammar.
#[test]
fn fg10_subquery_rejected() {
    let db = Database::open_memory();
    expect_formula_parse_error(
        create_bad_policy_table(&db, "embedding VECTOR(2)", "(SELECT 1)"),
        "decisions.embedding",
        2,
        "SELECT",
    );
}

/// RED: dotted refs are rejected because table-qualified formula refs are unsupported.
#[test]
fn fg11_dotted_column_rejected() {
    let db = Database::open_memory();
    expect_formula_parse_error(
        create_bad_policy_table(&db, "embedding VECTOR(2)", "{outcomes.success}"),
        "decisions.embedding",
        10,
        "qualified",
    );
}

/// RED: division stays banned from the rank formula grammar.
#[test]
fn fg12_division_rejected() {
    let db = Database::open_memory();
    expect_formula_parse_error(
        create_bad_policy_table(&db, "embedding VECTOR(2)", "{confidence} / 2"),
        "decisions.embedding",
        14,
        "/",
    );
}

/// RED: arbitrary function calls stay banned from the rank formula grammar.
#[test]
fn fg13_function_call_rejected() {
    let db = Database::open_memory();
    expect_formula_parse_error(
        create_bad_policy_table(&db, "embedding VECTOR(2)", "sum({confidence})"),
        "decisions.embedding",
        1,
        "function",
    );
}

/// RED: subtraction stays banned from the rank formula grammar.
#[test]
fn fg14_subtraction_rejected() {
    let db = Database::open_memory();
    expect_formula_parse_error(
        create_bad_policy_table(&db, "embedding VECTOR(2)", "{confidence} - 1"),
        "decisions.embedding",
        14,
        "-",
    );
}

/// RED: every banned formula construct is rejected by the same compile-time gate.
#[test]
fn bf01_banned_features_table() {
    for (formula, position, reason) in [
        ("CASE WHEN {confidence} THEN 1 END", 1, "CASE"),
        ("(SELECT 1)", 2, "SELECT"),
        ("{outcomes.success}", 10, "qualified"),
        ("{confidence} / 2", 14, "/"),
        ("sum({confidence})", 1, "function"),
        ("{confidence} - 1", 14, "-"),
    ] {
        let db = Database::open_memory();
        expect_formula_parse_error(
            create_bad_policy_table(&db, "embedding VECTOR(2)", formula),
            "decisions.embedding",
            position,
            reason,
        );
    }
}

/// RED: cg effective-confidence fixture ranks by confidence times outcome.
#[test]
fn fe01_base_arithmetic() {
    let db = Database::open_memory();
    create_standard_policy_schema(&db);
    let [d1, d2, d3, d4] = seed_effective_confidence_fixture(&db);
    expect_policy_order(&db, &[d1, d3, d4, d2]);
}

/// RED: NULL confidence defaults to the joined success value.
#[test]
fn fe02_null_confidence() {
    let db = Database::open_memory();
    create_standard_policy_schema(&db);
    let d = uuid(31);
    insert_decision(
        &db,
        d,
        "framework",
        "null confidence",
        None,
        1,
        vector_for_cosine(0.4),
    );
    insert_outcome(&db, uuid(131), d, Some(true), Some(1.0), Some(0.5));
    let results = semantic_results(&db, Some("effective_confidence"), 1);
    assert_eq!(result_ids(&results), vec![d]);
    assert_eq!(results[0].rank, 1.0);
}

/// RED: missing outcome uses LEFT OUTER null semantics with the coalesce fallback.
#[test]
fn fe03_no_outcome() {
    let db = Database::open_memory();
    create_standard_policy_schema(&db);
    let d = uuid(32);
    insert_decision(
        &db,
        d,
        "framework",
        "no outcome",
        Some(0.7),
        1,
        vector_for_cosine(0.4),
    );
    let results = semantic_results(&db, Some("effective_confidence"), 1);
    assert_eq!(results[0].rank, 0.7);
}

/// RED: failed outcomes force rank to zero.
#[test]
fn fe04_failed_outcome() {
    let db = Database::open_memory();
    create_standard_policy_schema(&db);
    let d = uuid(33);
    insert_decision(
        &db,
        d,
        "framework",
        "failed",
        Some(0.8),
        1,
        vector_for_cosine(0.9),
    );
    insert_outcome(&db, uuid(133), d, Some(false), Some(0.0), Some(0.2));
    let results = semantic_results(&db, Some("effective_confidence"), 1);
    assert_eq!(results[0].rank, 0.0);
}

/// RED: simultaneous null-confidence and no-outcome fallbacks multiply to one.
#[test]
fn fe05_both_fallbacks() {
    let db = Database::open_memory();
    create_standard_policy_schema(&db);
    let d = uuid(34);
    insert_decision(&db, d, "framework", "both", None, 1, vector_for_cosine(0.6));
    let results = semantic_results(&db, Some("effective_confidence"), 1);
    assert_eq!(results[0].rank, 1.0);
}

/// RED: NULL confidence with failed outcome still ranks zero.
#[test]
fn fe06_null_conf_failed_outcome() {
    let db = Database::open_memory();
    create_standard_policy_schema(&db);
    let d = uuid(35);
    insert_decision(
        &db,
        d,
        "framework",
        "null failed",
        None,
        1,
        vector_for_cosine(0.9),
    );
    insert_outcome(&db, uuid(135), d, Some(false), Some(0.0), Some(0.2));
    let results = semantic_results(&db, Some("effective_confidence"), 1);
    assert_eq!(results[0].rank, 0.0);
}

/// RED: policy top-k must differ from raw cosine when the formula says so.
#[test]
fn top01_policy_topk_ne_cosine_topk() {
    let db = Database::open_memory();
    create_standard_policy_schema(&db);
    let ids = seed_ten_candidate_policy_fixture(&db);
    db.__reset_rank_policy_eval_count();
    let results = semantic_results(&db, Some("effective_confidence"), 3);
    assert_eq!(result_ids(&results), vec![ids[9], ids[8], ids[7]]);
    assert_eq!(db.__rank_policy_eval_count(), 10);
    let raw = semantic_results(&db, None, 3);
    assert_eq!(result_ids(&raw), vec![ids[0], ids[1], ids[2]]);
}

/// RED: implementations may not cosine-top-k first and rerank only those rows.
#[test]
fn top03_no_posthoc_rerank() {
    let db = Database::open_memory();
    create_standard_policy_schema(&db);
    let ids = seed_ten_candidate_policy_fixture(&db);
    db.__reset_rank_policy_eval_count();
    let results = semantic_results(&db, Some("effective_confidence"), 3);
    assert_eq!(result_ids(&results), vec![ids[9], ids[8], ids[7]]);
    assert_eq!(db.__rank_policy_eval_count(), 10);
}

/// RED: equal computed rank ties are broken by raw cosine descending.
#[test]
fn tie01_cosine_on_equal_rank() {
    let db = Database::open_memory();
    create_policy_schema(&db, "1.0");
    let high = uuid(41);
    let low = uuid(42);
    insert_decision(
        &db,
        high,
        "framework",
        "high",
        Some(0.1),
        1,
        vector_for_cosine(0.9),
    );
    insert_decision(
        &db,
        low,
        "framework",
        "low",
        Some(0.9),
        2,
        vector_for_cosine(0.7),
    );
    let results = semantic_results(&db, Some("effective_confidence"), 2);
    assert_eq!(result_ids(&results), vec![high, low]);
    assert_eq!(
        results.iter().map(|r| r.rank).collect::<Vec<_>>(),
        vec![1.0, 1.0]
    );
}

/// RED: equal rank and cosine ties are broken by internal RowId descending.
#[test]
fn tie02_rowid_on_equal_rank_and_cosine() {
    let db = Database::open_memory();
    create_policy_schema(&db, "1.0");
    let first = uuid(43);
    let second = uuid(44);
    insert_decision(
        &db,
        first,
        "framework",
        "first",
        Some(0.5),
        1,
        vector_for_cosine(0.8),
    );
    insert_decision(
        &db,
        second,
        "framework",
        "second",
        Some(0.5),
        2,
        vector_for_cosine(0.8),
    );
    assert_eq!(
        result_ids(&semantic_results(&db, Some("effective_confidence"), 2)),
        vec![second, first]
    );
}

/// RED: NaN ranks sort last under deterministic total-order semantics.
#[test]
fn tie03_totalorder_on_nan() {
    let db = Database::open_memory();
    create_policy_schema(&db, "{confidence}");
    let nan = uuid(45);
    let one = uuid(46);
    insert_decision(
        &db,
        nan,
        "framework",
        "nan",
        Some(f64::NAN),
        1,
        vector_for_cosine(1.0),
    );
    insert_decision(
        &db,
        one,
        "framework",
        "one",
        Some(1.0),
        2,
        vector_for_cosine(0.9),
    );
    assert_eq!(
        result_ids(&semantic_results(&db, Some("effective_confidence"), 2)),
        vec![one, nan]
    );
}

/// RED: formula-generated NaN also sorts last and does not break deterministic ties.
#[test]
fn tie04_nan_from_formula() {
    let db = Database::open_memory();
    create_policy_schema(&db, "{success_rate}");
    let nan_a = uuid(47);
    let nan_b = uuid(48);
    let good = uuid(49);
    insert_decision(
        &db,
        nan_a,
        "framework",
        "nan a",
        Some(0.5),
        1,
        vector_for_cosine(1.0),
    );
    insert_decision(
        &db,
        nan_b,
        "framework",
        "nan b",
        Some(0.5),
        2,
        vector_for_cosine(0.9),
    );
    insert_decision(
        &db,
        good,
        "framework",
        "good",
        Some(0.5),
        3,
        vector_for_cosine(0.8),
    );
    insert_outcome(&db, uuid(147), nan_a, Some(true), Some(f64::NAN), Some(0.5));
    insert_outcome(&db, uuid(148), nan_b, Some(true), Some(f64::NAN), Some(0.5));
    insert_outcome(&db, uuid(149), good, Some(true), Some(1.0), Some(0.5));
    let results = semantic_results(&db, Some("effective_confidence"), 3);
    assert_eq!(result_ids(&results), vec![good, nan_a, nan_b]);
    assert_eq!(results[0].rank, 1.0);
    assert!(results[1].rank.is_nan());
    assert!(results[2].rank.is_nan());
}

/// RED: an orphan anchor remains visible under LEFT OUTER semantics.
#[test]
fn jn01_left_outer_orphan_present() {
    let db = Database::open_memory();
    create_standard_policy_schema(&db);
    let d = uuid(51);
    insert_decision(
        &db,
        d,
        "framework",
        "orphan",
        Some(0.7),
        1,
        vector_for_cosine(0.4),
    );
    let results = semantic_results(&db, Some("effective_confidence"), 1);
    assert_eq!(result_ids(&results), vec![d]);
    assert_eq!(results[0].rank, 0.7);
}

/// RED: a single joined row can match multiple anchors without cross-product duplication.
#[test]
fn jn02_multi_anchor_single_joined_row() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE observations (id UUID PRIMARY KEY, confidence REAL)",
        &empty_params(),
    )
    .unwrap();
    db.execute(
        "CREATE INDEX observations_id_idx ON observations(id)",
        &empty_params(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE evidence (
            id UUID PRIMARY KEY,
            observation_id UUID,
            vector_text VECTOR(2) RANK_POLICY (
                JOIN observations ON id,
                FORMULA 'coalesce({confidence}, 1.0)',
                SORT_KEY weighted
            )
        )",
        &empty_params(),
    )
    .unwrap();
    let shared_observation = uuid(152);
    db.execute(
        "INSERT INTO observations (id, confidence) VALUES ($id, $confidence)",
        &params(vec![
            ("id", Value::Uuid(shared_observation)),
            ("confidence", Value::Float64(0.9)),
        ]),
    )
    .unwrap();
    let a = uuid(52);
    let b = uuid(53);
    let c = uuid(54);
    for (id, score) in [(a, 0.5), (b, 0.4), (c, 0.3)] {
        db.execute(
            "INSERT INTO evidence (id, observation_id, vector_text) VALUES ($id, $obs, $vec)",
            &params(vec![
                ("id", Value::Uuid(id)),
                ("obs", Value::Uuid(shared_observation)),
                ("vec", Value::Vector(vector_for_cosine(score))),
            ]),
        )
        .unwrap();
    }
    let mut query = SemanticQuery::new("evidence", "vector_text", query_vec(), 3);
    query.sort_key = Some("weighted".into());
    let results = db.semantic_search(query).unwrap();
    assert_eq!(result_ids(&results), vec![a, b, c]);
    assert_eq!(
        results.iter().map(|r| r.rank).collect::<Vec<_>>(),
        vec![0.9, 0.9, 0.9]
    );
    assert_eq!(db.__rank_policy_eval_count(), 3);
}

/// RED: NULL join keys evaluate joined columns as NULL and keep the anchor candidate.
#[test]
fn jn03_null_join_key() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE outcomes (
            id UUID PRIMARY KEY,
            decision_id UUID,
            success BOOL
        )",
        &empty_params(),
    )
    .unwrap();
    db.execute(
        "CREATE INDEX outcomes_decision_id_idx ON outcomes(decision_id)",
        &empty_params(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE decisions (
            id UUID PRIMARY KEY,
            decision_id UUID,
            embedding VECTOR(2) RANK_POLICY (
                JOIN outcomes ON decision_id,
                FORMULA '0.6 * coalesce({success}, 1.0)',
                SORT_KEY effective_confidence
            )
        )",
        &empty_params(),
    )
    .unwrap();
    let d = uuid(55);
    db.execute(
        "INSERT INTO decisions (id, decision_id, embedding) VALUES ($id, $decision_id, $embedding)",
        &params(vec![
            ("id", Value::Uuid(d)),
            ("decision_id", Value::Null),
            ("embedding", Value::Vector(vector_for_cosine(0.5))),
        ]),
    )
    .unwrap();
    db.execute(
        "INSERT INTO outcomes (id, decision_id, success) VALUES ($id, $decision_id, $success)",
        &params(vec![
            ("id", Value::Uuid(uuid(155))),
            ("decision_id", Value::Null),
            ("success", Value::Bool(false)),
        ]),
    )
    .unwrap();
    let results = semantic_results(&db, Some("effective_confidence"), 1);
    assert_eq!(result_ids(&results), vec![d]);
    assert_eq!(results[0].rank, 0.6);
}

/// RED: min_similarity filters are applied before policy evaluation.
#[test]
fn fil01_min_similarity_pre_eval() {
    let db = Database::open_memory();
    create_standard_policy_schema(&db);
    let ids = seed_ten_candidate_policy_fixture(&db);
    db.__reset_rank_policy_eval_count();
    let mut query = SemanticQuery::new("decisions", "embedding", query_vec(), 10);
    query.sort_key = Some("effective_confidence".to_string());
    query.min_similarity = Some(0.5);
    let results = db.semantic_search(query).unwrap();
    assert_eq!(
        result_ids(&results),
        vec![ids[5], ids[4], ids[3], ids[2], ids[1], ids[0]]
    );
    assert_eq!(db.__rank_policy_eval_count(), 6);
}

/// RED: time-range WHERE filters are applied before policy evaluation.
#[test]
fn fil02_where_time_range() {
    let db = Database::open_memory();
    create_standard_policy_schema(&db);
    let ids = seed_ten_candidate_policy_fixture(&db);
    db.__reset_rank_policy_eval_count();
    let mut query = SemanticQuery::new("decisions", "embedding", query_vec(), 10);
    query.sort_key = Some("effective_confidence".to_string());
    query.where_clause = Some("created_at > 20250102".to_string());
    let results = db.semantic_search(query).unwrap();
    assert_eq!(
        result_ids(&results),
        vec![ids[9], ids[8], ids[7], ids[6], ids[5], ids[4], ids[3]]
    );
    assert_eq!(db.__rank_policy_eval_count(), 7);
}

/// RED: equality WHERE filters are applied before policy evaluation.
#[test]
fn fil03_where_column_equality() {
    let db = Database::open_memory();
    create_standard_policy_schema(&db);
    let ids = seed_ten_candidate_policy_fixture(&db);
    db.__reset_rank_policy_eval_count();
    let mut query = SemanticQuery::new("decisions", "embedding", query_vec(), 10);
    query.sort_key = Some("effective_confidence".to_string());
    query.where_clause = Some("decision_type = 'caching'".to_string());
    let results = db.semantic_search(query).unwrap();
    assert_eq!(
        result_ids(&results),
        vec![ids[9], ids[7], ids[5], ids[3], ids[2], ids[0]]
    );
    assert_eq!(db.__rank_policy_eval_count(), 6);
}

/// RED: min_similarity and WHERE filters compose before policy evaluation.
#[test]
fn fil04_filter_composition() {
    let db = Database::open_memory();
    create_standard_policy_schema(&db);
    let ids = seed_ten_candidate_policy_fixture(&db);
    db.__reset_rank_policy_eval_count();
    let mut query = SemanticQuery::new("decisions", "embedding", query_vec(), 10);
    query.sort_key = Some("effective_confidence".to_string());
    query.min_similarity = Some(0.5);
    query.where_clause = Some("created_at > 20250102 AND decision_type = 'caching'".to_string());
    let results = db.semantic_search(query).unwrap();
    assert_eq!(result_ids(&results), vec![ids[5], ids[3]]);
    assert_eq!(db.__rank_policy_eval_count(), 2);
}

fn seed_many(db: &Database, count: usize) {
    for n in 0..count {
        let id = uuid(10_000 + n as u128);
        let score = 1.0 - (n as f32 / (count as f32 + 1.0)) * 0.4;
        insert_decision(
            db,
            id,
            "framework",
            "many",
            Some(0.9),
            n as i64,
            vector_for_cosine(score),
        );
    }
}

fn seed_many_with_policy_boost(db: &Database, count: usize, boost_start: usize) -> Vec<Uuid> {
    for n in 0..count {
        let id = uuid(10_000 + n as u128);
        let score = 1.0 - (n as f32 / (count as f32 + 1.0)) * 0.4;
        let confidence = if (boost_start..boost_start + 10).contains(&n) {
            1.0
        } else {
            0.1
        };
        insert_decision(
            db,
            id,
            "framework",
            "many",
            Some(confidence),
            n as i64,
            vector_for_cosine(score),
        );
    }
    (boost_start..boost_start + 10)
        .map(|n| uuid(10_000 + n as u128))
        .collect()
}

fn seed_many_hnsw_frontier_policy(db: &Database, count: usize) -> Vec<Uuid> {
    for n in 0..count {
        let id = uuid(10_000 + n as u128);
        let score = 1.0 - (n as f32 / (count as f32 + 1.0)) * 0.4;
        let confidence = if n < 10 { 1.0 + n as f64 * 0.01 } else { 0.1 };
        insert_decision(
            db,
            id,
            "framework",
            "many",
            Some(confidence),
            n as i64,
            vector_for_cosine(score),
        );
    }
    (0..10).rev().map(|n| uuid(10_000 + n)).collect()
}

/// RED: brute-force boundary evaluates policy over every survivor.
#[test]
fn hnsw_threshold_999_brute_force_boundary() {
    let db = Database::open_memory();
    create_policy_schema(&db, "coalesce({confidence}, 0.0)");
    let expected = seed_many_with_policy_boost(&db, 999, 500);
    let results = semantic_results(&db, Some("effective_confidence"), 10);
    assert_eq!(results.len(), 10);
    assert_eq!(result_ids(&results), expected);
    assert!(results.iter().all(|r| r.rank == 1.0));
    assert_eq!(db.__rank_policy_eval_count(), 999);
}

/// RED: HNSW boundary still evaluates the surfaced policy candidates.
#[test]
fn hnsw_threshold_1001_hnsw_boundary() {
    let db = Database::open_memory();
    create_policy_schema(&db, "coalesce({confidence}, 0.0)");
    let expected = seed_many_hnsw_frontier_policy(&db, 1001);
    let results = semantic_results(&db, Some("effective_confidence"), 10);
    assert_eq!(results.len(), 10);
    assert_eq!(result_ids(&results), expected);
    assert_eq!(
        results.iter().map(|r| r.rank).collect::<Vec<_>>(),
        (0..10)
            .rev()
            .map(|n| 1.0 + n as f32 * 0.01)
            .collect::<Vec<_>>()
    );
    assert!(db.__rank_policy_eval_count() > 0 && db.__rank_policy_eval_count() < 1001);
}

/// REGRESSION GUARD: HNSW with no sort_key remains byte-for-byte raw cosine search.
#[test]
fn hnsw03_sort_key_none_unchanged() {
    let db = Database::open_memory();
    create_policy_schema(&db, "0.9");
    seed_many(&db, 1001);
    let semantic = semantic_results(&db, None, 10);
    let raw = db
        .query_vector(
            VectorIndexRef::new("decisions", "embedding"),
            &query_vec(),
            10,
            None,
            db.snapshot(),
        )
        .unwrap();
    assert_eq!(
        semantic
            .iter()
            .map(|r| (r.row_id, r.vector_score))
            .collect::<Vec<_>>(),
        raw
    );
}

/// RED: formulas are parsed once at DDL time and never in the search hot path.
#[test]
fn formula_cache_hit() {
    let db = Database::open_memory();
    assert_eq!(db.__rank_policy_formula_parse_count(), 0);
    create_standard_policy_schema(&db);
    assert_eq!(db.__rank_policy_formula_parse_count(), 1);
    seed_effective_confidence_fixture(&db);
    semantic_results(&db, Some("effective_confidence"), 2);
    semantic_results(&db, Some("effective_confidence"), 2);
    assert_eq!(db.__rank_policy_formula_parse_count(), 1);
}

/// RED: dropping a policy-bearing table clears the formula cache atomically.
#[test]
fn drop_clears_cache() {
    let db = Database::open_memory();
    create_policy_schema(&db, "1.0");
    let old_id = uuid(141);
    insert_decision(
        &db,
        old_id,
        "framework",
        "old formula",
        Some(0.5),
        1,
        vector_for_cosine(1.0),
    );
    let old_results = semantic_results(&db, Some("effective_confidence"), 1);
    assert_eq!(old_results[0].rank, 1.0);
    assert_eq!(db.__rank_policy_formula_parse_count(), 1);
    db.execute("DROP TABLE decisions", &empty_params()).unwrap();
    assert_eq!(db.__rank_policy_formula_parse_count(), 0);
    db.execute(
        "CREATE TABLE decisions (
            id UUID PRIMARY KEY,
            decision_type TEXT,
            description TEXT,
            confidence REAL,
            created_at INTEGER,
            embedding VECTOR(2) RANK_POLICY (
                JOIN outcomes ON decision_id,
                FORMULA '0.25',
                SORT_KEY effective_confidence
            )
        )",
        &empty_params(),
    )
    .unwrap();
    assert_eq!(db.__rank_policy_formula_parse_count(), 1);
    let new_id = uuid(142);
    insert_decision(
        &db,
        new_id,
        "framework",
        "new formula",
        Some(0.5),
        1,
        vector_for_cosine(1.0),
    );
    let new_results = semantic_results(&db, Some("effective_confidence"), 1);
    assert_eq!(result_ids(&new_results), vec![new_id]);
    assert_eq!(new_results[0].rank, 0.25);
}

/// RED: an empty corpus returns no rows but still proves eager policy registration.
#[test]
fn empty_corpus_with_policy() {
    let db = Database::open_memory();
    create_standard_policy_schema(&db);
    assert_eq!(db.__rank_policy_formula_parse_count(), 1);
    let warns = capture_rank_policy_warns(|| {
        assert!(semantic_results(&db, Some("effective_confidence"), 10).is_empty());
    });
    assert!(warns.is_empty());
    assert_eq!(db.__rank_policy_eval_count(), 0);
}

/// RED: reopen preserves rank-policy ordering, not just raw vector storage.
#[test]
fn ps01_reopen_preserves_ordering() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rank.db");
    let ids;
    {
        let db = Database::open(&path).unwrap();
        create_standard_policy_schema(&db);
        ids = seed_effective_confidence_fixture(&db);
        expect_policy_order(&db, &[ids[0], ids[2], ids[3], ids[1]]);
    }
    let reopened = Database::open(&path).unwrap();
    expect_policy_order(&reopened, &[ids[0], ids[2], ids[3], ids[1]]);
}

/// RED: reopen preserves the full RankPolicy struct, including protected_index.
#[test]
fn ps02_reopen_preserves_rank_policy_struct() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rank.db");
    {
        let db = Database::open(&path).unwrap();
        create_standard_policy_schema(&db);
    }
    let reopened = Database::open(&path).unwrap();
    let policy = rank_policy_meta(&reopened, "decisions", "embedding");
    assert_eq!(policy.joined_table, "outcomes");
    assert_eq!(policy.joined_column, "decision_id");
    assert_eq!(policy.sort_key, "effective_confidence");
    assert_eq!(
        policy.formula,
        "coalesce({confidence}, 1.0) * coalesce({success}, 1.0)"
    );
    assert_eq!(policy.protected_index, "outcomes_decision_id_idx");
}

/// RED: DdlChange round-trips policy metadata through sync apply.
#[test]
fn sy01_ddl_change_round_trip() {
    let origin = Database::open_memory();
    create_standard_policy_schema(&origin);
    let peer = Database::open_memory();
    let changes = origin.changes_since(Lsn(0));
    peer.apply_changes(
        changes,
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    )
    .unwrap();
    assert_eq!(
        rank_policy_meta(&peer, "decisions", "embedding"),
        rank_policy_meta(&origin, "decisions", "embedding")
    );
}

/// RED: peer-side searches are byte-identical to origin rank-policy searches.
#[test]
fn sy02_peer_search_byte_identical() {
    let origin = Database::open_memory();
    create_standard_policy_schema(&origin);
    let ids = seed_effective_confidence_fixture(&origin);
    let peer = Database::open_memory();
    peer.apply_changes(
        origin.changes_since(Lsn(0)),
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    )
    .unwrap();
    expect_policy_order(&origin, &[ids[0], ids[2], ids[3], ids[1]]);
    expect_policy_order(&peer, &[ids[0], ids[2], ids[3], ids[1]]);
}

/// RED: persisted full snapshots emit joined-table dependencies before rank-policy anchor tables.
#[test]
fn sy03_persisted_full_snapshot_orders_rank_policy_dependencies() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rank-sync.db");
    {
        let origin = Database::open(&path).unwrap();
        create_standard_policy_schema(&origin);
    }
    let reopened = Database::open(&path).unwrap();
    let changes = reopened.changes_since(Lsn(0));
    let outcomes_table = changes
        .ddl
        .iter()
        .position(
            |change| matches!(change, DdlChange::CreateTable { name, .. } if name == "outcomes"),
        )
        .expect("outcomes CreateTable");
    let outcomes_index = changes
        .ddl
        .iter()
        .position(|change| matches!(change, DdlChange::CreateIndex { table, name, .. } if table == "outcomes" && name == "outcomes_decision_id_idx"))
        .expect("outcomes CreateIndex");
    let decisions_table = changes
        .ddl
        .iter()
        .position(
            |change| matches!(change, DdlChange::CreateTable { name, .. } if name == "decisions"),
        )
        .expect("decisions CreateTable");
    assert!(
        outcomes_table < outcomes_index && outcomes_index < decisions_table,
        "rank-policy full snapshot DDL must create joined table + protected index before anchor table: {:?}",
        changes.ddl
    );

    let peer = Database::open_memory();
    peer.apply_changes(
        changes,
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    )
    .unwrap();
    assert_eq!(
        rank_policy_meta(&peer, "decisions", "embedding"),
        rank_policy_meta(&reopened, "decisions", "embedding")
    );
}

/// RED: sync-applied DropTable respects rank-policy dependent-DDL RESTRICT.
#[test]
fn sy04_sync_drop_table_respects_rank_policy_restrict() {
    let db = Database::open_memory();
    create_standard_policy_schema(&db);
    let result = db.apply_changes(
        ChangeSet {
            ddl: vec![DdlChange::DropTable {
                name: "outcomes".into(),
            }],
            ddl_lsn: vec![Lsn(1)],
            ..Default::default()
        },
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    );
    match result {
        Err(Error::DropBlockedByRankPolicy {
            table,
            policy_table,
            policy_column,
            sort_key,
            ..
        }) => {
            assert_eq!(table.as_ref(), "outcomes");
            assert_eq!(policy_table.as_ref(), "decisions");
            assert_eq!(policy_column.as_ref(), "embedding");
            assert_eq!(sort_key.as_ref(), "effective_confidence");
        }
        other => panic!("expected sync DropTable to be rank-policy blocked, got {other:?}"),
    }
}

/// RED: sync-applied DropIndex respects rank-policy dependent-DDL RESTRICT.
#[test]
fn sy05_sync_drop_index_respects_rank_policy_restrict() {
    let db = Database::open_memory();
    create_standard_policy_schema(&db);
    let result = db.apply_changes(
        ChangeSet {
            ddl: vec![DdlChange::DropIndex {
                table: "outcomes".into(),
                name: "outcomes_decision_id_idx".into(),
            }],
            ddl_lsn: vec![Lsn(1)],
            ..Default::default()
        },
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    );
    match result {
        Err(Error::DropBlockedByRankPolicy {
            table,
            dropped_index,
            policy_table,
            policy_column,
            sort_key,
            ..
        }) => {
            assert_eq!(table.as_ref(), "outcomes");
            assert_eq!(dropped_index.as_deref(), Some("outcomes_decision_id_idx"));
            assert_eq!(policy_table.as_ref(), "decisions");
            assert_eq!(policy_column.as_ref(), "embedding");
            assert_eq!(sort_key.as_ref(), "effective_confidence");
        }
        other => panic!("expected sync DropIndex to be rank-policy blocked, got {other:?}"),
    }
}

/// RED: full snapshots can apply a same-table rank policy after its protected index exists.
#[test]
fn sy06_persisted_full_snapshot_orders_same_table_rank_policy_dependencies() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rank-self-sync.db");
    {
        let origin = Database::open(&path).unwrap();
        origin
            .execute(
                "CREATE TABLE memories (id UUID PRIMARY KEY, confidence REAL)",
                &empty_params(),
            )
            .unwrap();
        origin
            .execute(
                "CREATE INDEX memories_id_idx ON memories(id)",
                &empty_params(),
            )
            .unwrap();
        origin
            .execute(
                "ALTER TABLE memories ADD COLUMN embedding VECTOR(2) RANK_POLICY (
                    JOIN memories ON id,
                    FORMULA '{vector_score} * coalesce({confidence}, 1.0)',
                    SORT_KEY self_weighted
                )",
                &empty_params(),
            )
            .unwrap();
    }
    let reopened = Database::open(&path).unwrap();
    let changes = reopened.changes_since(Lsn(0));
    let memories_table = changes
        .ddl
        .iter()
        .position(
            |change| matches!(change, DdlChange::CreateTable { name, .. } if name == "memories"),
        )
        .expect("memories CreateTable");
    let memories_index = changes
        .ddl
        .iter()
        .position(|change| matches!(change, DdlChange::CreateIndex { table, name, .. } if table == "memories" && name == "memories_id_idx"))
        .expect("memories CreateIndex");
    let memories_alter = changes
        .ddl
        .iter()
        .position(
            |change| matches!(change, DdlChange::AlterTable { name, .. } if name == "memories"),
        )
        .expect("memories AlterTable");
    assert!(
        memories_table < memories_index && memories_index < memories_alter,
        "same-table rank-policy snapshot must create table, then protected index, then add policy column: {:?}",
        changes.ddl
    );

    let peer = Database::open_memory();
    peer.apply_changes(
        changes,
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    )
    .unwrap();
    assert_eq!(
        rank_policy_meta(&peer, "memories", "embedding"),
        rank_policy_meta(&reopened, "memories", "embedding")
    );
}

/// RED: sort_key Some populates formula rank and raw vector_score distinctly.
#[test]
fn an01_sort_key_some_populates_fields() {
    let db = Database::open_memory();
    create_policy_schema(&db, "1.0");
    seed_effective_confidence_fixture(&db);
    let results = semantic_results(&db, Some("effective_confidence"), 4);
    assert!(results.iter().all(|r| r.vector_score.is_finite()));
    assert!(results.iter().all(|r| r.rank == 1.0));
}

/// REGRESSION GUARD: sort_key None returns raw cosine and rank equals vector_score.
#[test]
fn an02_sort_key_none_rank_equals_vector_score() {
    let db = Database::open_memory();
    create_policy_schema(&db, "1.0");
    seed_effective_confidence_fixture(&db);
    let results = semantic_results(&db, None, 4);
    assert!(results.iter().all(|r| r.rank == r.vector_score));
}

/// RED: SQL USE RANK orders identically to the library semantic-search API.
#[test]
fn api01_use_rank_parses_and_orders() {
    let db = Database::open_memory();
    create_standard_policy_schema(&db);
    let [d1, _, d3, d4] = seed_effective_confidence_fixture(&db);
    let result = db.execute(
        "SELECT id FROM decisions ORDER BY embedding <=> $q USE RANK effective_confidence LIMIT 3",
        &params(vec![("q", Value::Vector(query_vec()))]),
    ).unwrap();
    let col = result.columns.iter().position(|c| c == "id").unwrap();
    let ids = result
        .rows
        .iter()
        .map(|row| match row[col] {
            Value::Uuid(id) => id,
            _ => panic!("id"),
        })
        .collect::<Vec<_>>();
    assert_eq!(ids, vec![d1, d3, d4]);
}

/// RED: USE RANK without LIMIT is rejected loudly.
#[test]
fn api02_use_rank_without_limit() {
    let db = Database::open_memory();
    create_standard_policy_schema(&db);
    expect_use_rank_requires_limit(db.execute(
        "SELECT * FROM decisions ORDER BY embedding <=> $q USE RANK effective_confidence",
        &params(vec![("q", Value::Vector(query_vec()))]),
    ));
}

/// RED: USE RANK without vector ORDER BY is rejected loudly.
#[test]
fn api03_use_rank_without_vector_order() {
    let db = Database::open_memory();
    create_standard_policy_schema(&db);
    expect_use_rank_requires_vector_order(db.execute(
        "SELECT * FROM decisions USE RANK effective_confidence LIMIT 10",
        &empty_params(),
    ));
}

/// RED: SQL USE RANK with an unknown sort key returns RankPolicyNotFound.
#[test]
fn api04_use_rank_unknown_sort_key_sql() {
    let db = Database::open_memory();
    create_standard_policy_schema(&db);
    expect_rank_policy_not_found(
        db.execute(
            "SELECT * FROM decisions ORDER BY embedding <=> $q USE RANK bogus_key LIMIT 10",
            &params(vec![("q", Value::Vector(query_vec()))]),
        ),
        "decisions.embedding",
        "bogus_key",
    );
}

/// RED: library semantic_search with an unknown sort key returns RankPolicyNotFound.
#[test]
fn api05_unknown_sort_key_library() {
    let db = Database::open_memory();
    create_standard_policy_schema(&db);
    seed_effective_confidence_fixture(&db);
    let mut query = SemanticQuery::new("decisions", "embedding", query_vec(), 3);
    query.sort_key = Some("bogus_key".to_string());
    expect_search_rank_policy_not_found(
        db.semantic_search(query),
        "decisions.embedding",
        "bogus_key",
    );
}

/// REGRESSION GUARD: SELECT without USE RANK keeps raw cosine ordering.
#[test]
fn api06_select_without_use_rank() {
    let db = Database::open_memory();
    create_standard_policy_schema(&db);
    let [d1, d2, d3, _] = seed_effective_confidence_fixture(&db);
    let result = db
        .execute(
            "SELECT id FROM decisions ORDER BY embedding <=> $q LIMIT 3",
            &params(vec![("q", Value::Vector(query_vec()))]),
        )
        .unwrap();
    let col = result.columns.iter().position(|c| c == "id").unwrap();
    let ids = result
        .rows
        .iter()
        .map(|row| match row[col] {
            Value::Uuid(id) => id,
            _ => panic!("id"),
        })
        .collect::<Vec<_>>();
    assert_eq!(ids, vec![d1, d2, d3]);
}

/// RED: dropping a joined table used by a rank policy is refused.
#[test]
fn ddl_rj01_drop_table_joined() {
    let db = Database::open_memory();
    create_standard_policy_schema(&db);
    expect_drop_blocked(
        db.execute("DROP TABLE outcomes", &empty_params()),
        "outcomes",
        None,
        None,
        "decisions",
        "embedding",
        "effective_confidence",
    );
}

/// RED: dropping the joined key column used by a rank policy is refused.
#[test]
fn ddl_rj02_drop_column_join_key() {
    let db = Database::open_memory();
    create_standard_policy_schema(&db);
    expect_drop_blocked(
        db.execute(
            "ALTER TABLE outcomes DROP COLUMN decision_id",
            &empty_params(),
        ),
        "outcomes",
        Some("decision_id"),
        None,
        "decisions",
        "embedding",
        "effective_confidence",
    );
}

/// RED: renaming the joined key column used by a rank policy is refused.
#[test]
fn ddl_rj03_rename_column_join_key() {
    let db = Database::open_memory();
    create_standard_policy_schema(&db);
    expect_drop_blocked(
        db.execute(
            "ALTER TABLE outcomes RENAME COLUMN decision_id TO old_decision_id",
            &empty_params(),
        ),
        "outcomes",
        Some("decision_id"),
        None,
        "decisions",
        "embedding",
        "effective_confidence",
    );
}

/// RED: dropping the protected join index used by a rank policy is refused.
#[test]
fn ddl_rj04_drop_index_protected() {
    let db = Database::open_memory();
    create_standard_policy_schema(&db);
    expect_drop_blocked(
        db.execute(
            "DROP INDEX outcomes_decision_id_idx ON outcomes",
            &empty_params(),
        ),
        "outcomes",
        None,
        Some("outcomes_decision_id_idx"),
        "decisions",
        "embedding",
        "effective_confidence",
    );
}

/// REGRESSION GUARD: unrelated destructive DDL still succeeds.
#[test]
fn ddl_rj05_unrelated_ddl_succeeds() {
    let db = Database::open_memory();
    create_standard_policy_schema(&db);
    db.execute(
        "CREATE TABLE scratch (id UUID PRIMARY KEY, note TEXT)",
        &empty_params(),
    )
    .unwrap();
    db.execute(
        "CREATE INDEX scratch_note_idx ON scratch(note)",
        &empty_params(),
    )
    .unwrap();
    db.execute("DROP INDEX scratch_note_idx ON scratch", &empty_params())
        .unwrap();
    db.execute("ALTER TABLE scratch DROP COLUMN note", &empty_params())
        .unwrap();
    db.execute("DROP TABLE scratch", &empty_params()).unwrap();
}

/// RED: dropping the anchor vector column is refused with the policy identity in the error.
#[test]
fn ddl_rj06_drop_column_anchor() {
    let db = Database::open_memory();
    create_standard_policy_schema(&db);
    expect_drop_blocked(
        db.execute(
            "ALTER TABLE decisions DROP COLUMN embedding",
            &empty_params(),
        ),
        "decisions",
        Some("embedding"),
        None,
        "decisions",
        "embedding",
        "effective_confidence",
    );
}

/// RED: corrupt joined-row values warn once, skip the corrupt candidate, and return remaining rows.
#[test]
fn wn01_corrupt_joined_row_warns_and_skips() {
    let db = Database::open_memory();
    create_standard_policy_schema(&db);
    let ids = seed_effective_confidence_fixture(&db);
    let corrupt_row_id = db
        .point_lookup("outcomes", "id", &Value::Uuid(uuid(102)), db.snapshot())
        .unwrap()
        .expect("corrupt target outcome")
        .row_id;
    db.__inject_raw_joined_row_value_for_test(
        "outcomes",
        corrupt_row_id,
        "success",
        vec![0, 159, 146],
    )
    .unwrap();
    db.__reset_rank_policy_eval_count();
    let warns = capture_rank_policy_warns(|| {
        let results = semantic_results(&db, Some("effective_confidence"), 4);
        assert_eq!(result_ids(&results), vec![ids[0], ids[2], ids[3]]);
    });
    assert_eq!(db.__rank_policy_eval_count(), 4);
    assert_eq!(warns.len(), 1);
    let warn = &warns[0];
    assert_eq!(warn.target, "rank_policy_eval_error");
    assert_eq!(warn.level, "WARN");
    assert!(
        warn.name.contains("rank_policy"),
        "warn event name should identify rank-policy evaluation, got {}",
        warn.name
    );
    assert_eq!(
        unquoted_field(warn.fields.get("index")),
        Some("decisions.embedding"),
        "warn must carry exact index field, got {warn:?}"
    );
    assert!(
        matches!(
            unquoted_field(warn.fields.get("row_id")),
            Some(value)
                if value == corrupt_row_id.0.to_string()
                    || value == format!("RowId({})", corrupt_row_id.0)
        ),
        "warn must carry exact corrupt row_id field, got {warn:?}"
    );
    assert_eq!(
        unquoted_field(warn.fields.get("reason")),
        Some("failed to decode joined column `success`"),
        "warn must carry exact actionable reason field, got {warn:?}"
    );
}

/// REGRESSION GUARD: a clean corpus emits no rank-policy evaluation warnings.
#[test]
fn wn02_clean_corpus_no_warn() {
    let db = Database::open_memory();
    create_standard_policy_schema(&db);
    seed_effective_confidence_fixture(&db);
    let warns = capture_rank_policy_warns(|| {
        semantic_results(&db, Some("effective_confidence"), 4);
    });
    assert!(warns.is_empty());
}

/// REGRESSION GUARD: non-vector reads are unchanged by rank-policy metadata.
#[test]
fn rd01_non_vector_reads_unchanged() {
    let with_policy = Database::open_memory();
    create_standard_policy_schema(&with_policy);
    let without_policy = Database::open_memory();
    create_plain_policy_shape_schema(&without_policy);
    for db in [&with_policy, &without_policy] {
        insert_decision(
            db,
            uuid(61),
            "framework",
            "same",
            Some(0.5),
            1,
            vector_for_cosine(0.5),
        );
        insert_outcome(db, uuid(161), uuid(61), Some(true), Some(1.0), Some(0.5));
    }
    let a = with_policy
        .execute("SELECT * FROM decisions", &empty_params())
        .unwrap();
    let b = without_policy
        .execute("SELECT * FROM decisions", &empty_params())
        .unwrap();
    assert_eq!(a.columns, b.columns);
    assert_eq!(a.rows, b.rows);
    let scan_with_policy = with_policy
        .scan_filter("decisions", with_policy.snapshot(), &|_| true)
        .unwrap();
    let scan_without_policy = without_policy
        .scan_filter("decisions", without_policy.snapshot(), &|_| true)
        .unwrap();
    let row_signature = |rows: Vec<contextdb_core::VersionedRow>| {
        rows.into_iter()
            .map(|row| {
                (
                    row.row_id,
                    row.values,
                    row.created_tx,
                    row.deleted_tx,
                    row.lsn,
                )
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(
        row_signature(scan_with_policy),
        row_signature(scan_without_policy)
    );
    let join_sql = "SELECT decisions.id, outcomes.success FROM decisions INNER JOIN outcomes ON decisions.id = outcomes.decision_id";
    let joined_with_policy = with_policy.execute(join_sql, &empty_params()).unwrap();
    let joined_without_policy = without_policy.execute(join_sql, &empty_params()).unwrap();
    assert_eq!(joined_with_policy.columns, joined_without_policy.columns);
    assert_eq!(joined_with_policy.rows, joined_without_policy.rows);
}

/// REGRESSION GUARD: BOOL values outside rank formulas remain bools.
#[test]
fn rd02_bool_outside_rank_formula_unchanged() {
    let db = Database::open_memory();
    create_standard_policy_schema(&db);
    let id = uuid(62);
    insert_decision(
        &db,
        id,
        "framework",
        "bool",
        Some(0.5),
        1,
        vector_for_cosine(0.5),
    );
    insert_outcome(&db, uuid(162), id, Some(true), Some(1.0), Some(0.5));
    let false_id = uuid(63);
    insert_decision(
        &db,
        false_id,
        "framework",
        "bool false",
        Some(0.5),
        2,
        vector_for_cosine(0.4),
    );
    insert_outcome(&db, uuid(163), false_id, Some(false), Some(0.0), Some(0.5));
    let result = db
        .execute(
            "SELECT success FROM outcomes WHERE decision_id = $id AND success = TRUE",
            &params(vec![("id", Value::Uuid(id))]),
        )
        .unwrap();
    assert_eq!(result.rows[0][0], Value::Bool(true));
    let result = db
        .execute(
            "SELECT success FROM outcomes WHERE decision_id = $id AND success = FALSE",
            &params(vec![("id", Value::Uuid(false_id))]),
        )
        .unwrap();
    assert_eq!(result.rows[0][0], Value::Bool(false));
    let joined = db
        .execute(
            "SELECT decisions.id, outcomes.success FROM decisions INNER JOIN outcomes ON decisions.id = outcomes.decision_id AND outcomes.success = TRUE",
            &empty_params(),
        )
        .unwrap();
    assert_eq!(joined.rows.len(), 1);
    assert_eq!(joined.rows[0][0], Value::Uuid(id));
    assert_eq!(joined.rows[0][1], Value::Bool(true));
}

/// RED: every new Error variant exposes the pinned user-facing Display text.
#[test]
fn err_disp_every_new_variant() {
    let cases = vec![
        (
            Error::RankPolicyColumnUnknown {
                index: "decisions.embedding".into(),
                column: "bogus".into(),
            },
            "rank policy on index `decisions.embedding` references unknown column `bogus`",
        ),
        (
            Error::RankPolicyColumnAmbiguous {
                index: "decisions.embedding".into(),
                column: "id".into(),
            },
            "rank policy on index `decisions.embedding` references ambiguous column `id` (present on both anchor and joined table); rename one of the columns -- the rank-formula grammar does not support table-qualified column references",
        ),
        (
            Error::RankPolicyColumnType {
                index: "decisions.embedding".into(),
                column: "description".into(),
                expected: "number-or-bool".into(),
                actual: "TEXT".into(),
            },
            "rank policy on index `decisions.embedding` references column `description` of type TEXT; expected number-or-bool",
        ),
        (
            Error::RankPolicyJoinTableUnknown {
                index: "decisions.embedding".into(),
                table: "missing".into(),
            },
            "rank policy on index `decisions.embedding` joins unknown table `missing`",
        ),
        (
            Error::RankPolicyJoinColumnUnknown {
                index: "decisions.embedding".into(),
                table: "outcomes".into(),
                column: "missing".into(),
            },
            "rank policy on index `decisions.embedding` joins column `missing` not present on table `outcomes`",
        ),
        (
            Error::RankPolicyJoinColumnUnindexed {
                index: "decisions.embedding".into(),
                joined_table: "outcomes".into(),
                column: "decision_id".into(),
            },
            "rank policy on index `decisions.embedding` joins column `outcomes.decision_id` which has no index; add: CREATE INDEX outcomes_decision_id_idx ON outcomes(decision_id);",
        ),
        (
            Error::RankPolicyNotFound {
                index: "decisions.embedding".into(),
                sort_key: "bogus".into(),
            },
            "rank policy sort key `bogus` not found on index `decisions.embedding`",
        ),
        (
            Error::RankPolicyFormulaParse {
                index: "decisions.embedding".into(),
                position: 11,
                reason: "bad".into(),
            },
            "rank policy formula on index `decisions.embedding` failed to parse at position 11: bad",
        ),
        (
            Error::UseRankRequiresVectorOrder,
            "USE RANK requires ORDER BY embedding <=> $param in the same query",
        ),
        (
            Error::UseRankRequiresLimit,
            "USE RANK requires LIMIT in the same query",
        ),
        (
            Error::DropBlockedByRankPolicy {
                table: "outcomes".into(),
                column: None,
                dropped_index: None,
                policy_table: "decisions".into(),
                policy_column: "embedding".into(),
                sort_key: "effective_confidence".into(),
            },
            "cannot drop table `outcomes`: rank policy `effective_confidence` on `decisions.embedding` depends on it",
        ),
        (
            Error::DropBlockedByRankPolicy {
                table: "outcomes".into(),
                column: Some("decision_id".into()),
                dropped_index: None,
                policy_table: "decisions".into(),
                policy_column: "embedding".into(),
                sort_key: "effective_confidence".into(),
            },
            "cannot drop or rename column `outcomes.decision_id`: rank policy `effective_confidence` on `decisions.embedding` depends on it",
        ),
        (
            Error::DropBlockedByRankPolicy {
                table: "outcomes".into(),
                column: None,
                dropped_index: Some("outcomes_decision_id_idx".into()),
                policy_table: "decisions".into(),
                policy_column: "embedding".into(),
                sort_key: "effective_confidence".into(),
            },
            "cannot drop index `outcomes_decision_id_idx` on `outcomes`: rank policy `effective_confidence` on `decisions.embedding` depends on it",
        ),
    ];
    for (err, expected) in cases {
        assert_eq!(err.to_string(), expected);
    }
}

/// RED: independent databases with identical data produce byte-identical policy ordering.
#[test]
fn prod01_byte_identical_across_independent_dbs() {
    let a = Database::open_memory();
    let b = Database::open_memory();
    for db in [&a, &b] {
        create_standard_policy_schema(db);
        let ids = seed_effective_confidence_fixture(db);
        expect_policy_order(db, &[ids[0], ids[2], ids[3], ids[1]]);
    }
    assert_eq!(
        semantic_results(&a, Some("effective_confidence"), 4),
        semantic_results(&b, Some("effective_confidence"), 4)
    );
}

/// RED: concurrent policy reads on the same DB produce identical orderings.
#[test]
fn conc01_concurrent_reads_deterministic() {
    let db = Arc::new(Database::open_memory());
    create_standard_policy_schema(&db);
    let ids = seed_effective_confidence_fixture(&db);
    let a = {
        let db = db.clone();
        thread::spawn(move || result_ids(&semantic_results(&db, Some("effective_confidence"), 4)))
    };
    let b = {
        let db = db.clone();
        thread::spawn(move || result_ids(&semantic_results(&db, Some("effective_confidence"), 4)))
    };
    assert_eq!(a.join().unwrap(), vec![ids[0], ids[2], ids[3], ids[1]]);
    assert_eq!(b.join().unwrap(), vec![ids[0], ids[2], ids[3], ids[1]]);
}

/// RED: reader-at-snapshot rank search ignores a concurrent joined-table insert.
#[test]
fn write01_reader_at_snapshot_unaffected() {
    let db = Database::open_memory();
    create_standard_policy_schema(&db);
    let d1 = uuid(71);
    let d2 = uuid(72);
    insert_decision(
        &db,
        d1,
        "framework",
        "raw-near-low-rank",
        Some(0.2),
        1,
        vector_for_cosine(1.0),
    );
    insert_decision(
        &db,
        d2,
        "framework",
        "raw-far-high-rank",
        Some(0.9),
        2,
        vector_for_cosine(0.8),
    );
    let snap = db.snapshot();
    insert_outcome(&db, uuid(171), d2, Some(false), Some(0.0), Some(0.1));
    let result = db
        .execute_at_snapshot(
            "SELECT id FROM decisions ORDER BY embedding <=> $q USE RANK effective_confidence LIMIT 1",
            &params(vec![("q", Value::Vector(query_vec()))]),
            snap,
        )
        .unwrap();
    assert_eq!(result.rows, vec![vec![Value::Uuid(d2)]]);
    expect_policy_order(&db, &[d1, d2]);
}

/// RED: new joined outcomes surface in a fresh policy search.
#[test]
fn write02_new_outcome_surfaces_new_rank() {
    let db = Database::open_memory();
    create_standard_policy_schema(&db);
    let d = uuid(72);
    insert_decision(
        &db,
        d,
        "framework",
        "new outcome",
        Some(0.8),
        1,
        vector_for_cosine(0.8),
    );
    let before = semantic_results(&db, Some("effective_confidence"), 1);
    assert_eq!(result_ids(&before), vec![d]);
    assert_eq!(before[0].rank, 0.8);
    insert_outcome(&db, uuid(172), d, Some(false), Some(0.0), Some(0.1));
    let after = semantic_results(&db, Some("effective_confidence"), 1);
    assert_eq!(result_ids(&after), vec![d]);
    assert_eq!(after[0].rank, 0.0);
}

/// RED: first-run walkthrough matches the cg search-ranking fixture ordering.
#[test]
fn walk01_first_run_walkthrough() {
    let db = Database::open_memory();
    create_standard_policy_schema(&db);
    let [d1, d2, d3, d4] = seed_effective_confidence_fixture(&db);
    expect_policy_order(&db, &[d1, d3, d4, d2]);
}

/// RED: agent-memory persona ranks useful precedent over a failed high-cosine decision.
#[test]
fn persona01_agent_memory_end_to_end() {
    let db = Database::open_memory();
    create_standard_policy_schema(&db);
    let ids = seed_effective_confidence_fixture(&db);
    expect_policy_order(&db, &[ids[0], ids[2], ids[3], ids[1]]);
}

/// RED: hook-injection bridge returns framework context with finite formula rank.
#[test]
fn cgs01_hook_injection_bridge() {
    let db = Database::open_memory();
    create_standard_policy_schema(&db);
    let [d1, _, _, _] = seed_effective_confidence_fixture(&db);
    let results = semantic_results(&db, Some("effective_confidence"), 5);
    assert_eq!(result_ids(&results)[0], d1);
    assert_eq!(
        results[0].values.get("decision_type"),
        Some(&Value::Text("framework".into()))
    );
    assert_eq!(results[0].rank, 0.9);
    assert!(results[0].vector_score.is_finite());
}

/// RED: dedup bridge is deterministic and prefers the successful near-duplicate.
#[test]
fn cgs02_dedup_bridge() {
    fn seed(db: &Database) -> (Uuid, Uuid) {
        create_standard_policy_schema(db);
        let a = uuid(81);
        let b = uuid(82);
        insert_decision(
            db,
            a,
            "framework",
            "same prefix a",
            Some(0.9),
            1,
            vector_for_cosine(0.999999),
        );
        insert_decision(
            db,
            b,
            "framework",
            "same prefix b",
            Some(0.9),
            2,
            vector_for_cosine(1.0),
        );
        insert_outcome(db, uuid(181), a, Some(true), Some(1.0), Some(0.9));
        insert_outcome(db, uuid(182), b, Some(false), Some(0.0), Some(0.1));
        (a, b)
    }

    let first_db = Database::open_memory();
    let (a, b) = seed(&first_db);
    let second_db = Database::open_memory();
    assert_eq!(seed(&second_db), (a, b));

    let first = semantic_results(&first_db, Some("effective_confidence"), 2);
    let second = semantic_results(&second_db, Some("effective_confidence"), 2);
    assert_eq!(result_ids(&first), vec![a, b]);
    assert_eq!(result_ids(&second), vec![a, b]);
    assert_eq!(first, second);
    assert_eq!(first[0].rank, 0.9);
    assert_eq!(first[1].rank, 0.0);
}

fn create_evidence_schema(db: &Database, vision_policy: bool, same_sort_key: bool) {
    db.execute(
        "CREATE TABLE observations (id UUID PRIMARY KEY, confidence REAL, producer_quality REAL)",
        &empty_params(),
    )
    .unwrap();
    db.execute(
        "CREATE INDEX observations_id_idx ON observations(id)",
        &empty_params(),
    )
    .unwrap();
    let vision = if vision_policy {
        if same_sort_key {
            "vector_vision VECTOR(2) RANK_POLICY (JOIN observations ON id, FORMULA '{vector_score} * coalesce({producer_quality}, 1.0)', SORT_KEY weighted)"
        } else {
            "vector_vision VECTOR(2) RANK_POLICY (JOIN observations ON id, FORMULA '{vector_score} * coalesce({producer_quality}, 1.0)', SORT_KEY weighted_vision)"
        }
    } else {
        "vector_vision VECTOR(2)"
    };
    let text_key = if same_sort_key {
        "weighted"
    } else {
        "weighted_text"
    };
    let ddl = format!(
        "CREATE TABLE evidence (
            id UUID PRIMARY KEY,
            observation_id UUID,
            vector_text VECTOR(2) RANK_POLICY (JOIN observations ON id, FORMULA '{{vector_score}} * coalesce({{confidence}}, 1.0)', SORT_KEY {text_key}),
            {vision}
        )"
    );
    db.execute(&ddl, &empty_params()).unwrap();
}

fn seed_evidence(db: &Database) -> [Uuid; 3] {
    let ids = [uuid(91), uuid(92), uuid(93)];
    for (i, id) in ids.iter().enumerate() {
        db.execute(
            "INSERT INTO observations (id, confidence, producer_quality) VALUES ($id, $confidence, $quality)",
            &params(vec![
                ("id", Value::Uuid(*id)),
                ("confidence", Value::Float64([0.3, 0.9, 0.5][i])),
                ("quality", Value::Float64([0.9, 0.2, 0.7][i])),
            ]),
        ).unwrap();
        db.execute(
            "INSERT INTO evidence (id, observation_id, vector_text, vector_vision) VALUES ($id, $obs, $text, $vision)",
            &params(vec![
                ("id", Value::Uuid(*id)),
                ("obs", Value::Uuid(*id)),
                ("text", Value::Vector(vector_for_cosine([1.0, 0.8, 0.7][i]))),
                ("vision", Value::Vector(vector_for_cosine([0.5, 1.0, 0.6][i]))),
            ]),
        ).unwrap();
    }
    ids
}

fn evidence_search(db: &Database, column: &str, sort_key: Option<&str>) -> Vec<Uuid> {
    sql_vector_search_ids_with_query(db, "evidence", column, sort_key, query_vec(), 3)
}

/// RED: one policy vector and one raw vector column coexist independently.
#[test]
fn mv01_two_columns_one_with_policy() {
    let db = Database::open_memory();
    create_evidence_schema(&db, false, false);
    let ids = seed_evidence(&db);
    assert_eq!(
        evidence_search(&db, "vector_text", Some("weighted_text")),
        vec![ids[1], ids[2], ids[0]]
    );
    assert_eq!(
        evidence_search(&db, "vector_vision", None),
        vec![ids[1], ids[2], ids[0]]
    );
    assert_eq!(db.__rank_policy_eval_count(), 3);
}

/// RED: two vector columns may carry distinct policies and distinct sort keys.
#[test]
fn mv02_two_columns_distinct_policies() {
    let db = Database::open_memory();
    create_evidence_schema(&db, true, false);
    let ids = seed_evidence(&db);
    assert_eq!(
        evidence_search(&db, "vector_text", Some("weighted_text")),
        vec![ids[1], ids[2], ids[0]]
    );
    assert_eq!(
        evidence_search(&db, "vector_vision", Some("weighted_vision")),
        vec![ids[0], ids[2], ids[1]]
    );
}

/// RED: USE RANK binds to the vector column named in the same ORDER BY.
#[test]
fn mv03_use_rank_binds_to_order_by_column() {
    let db = Database::open_memory();
    create_evidence_schema(&db, true, false);
    seed_evidence(&db);
    expect_rank_policy_not_found(
        db.execute(
            "SELECT id FROM evidence ORDER BY vector_vision <=> $q USE RANK weighted_text LIMIT 3",
            &params(vec![("q", Value::Vector(query_vec()))]),
        ),
        "evidence.vector_vision",
        "weighted_text",
    );
}

/// RED: the same sort-key name may exist independently on two vector columns.
#[test]
fn mv04_same_sort_key_different_columns() {
    let db = Database::open_memory();
    create_evidence_schema(&db, true, true);
    let ids = seed_evidence(&db);
    assert_eq!(
        evidence_search(&db, "vector_text", Some("weighted")),
        vec![ids[1], ids[2], ids[0]]
    );
    assert_eq!(
        evidence_search(&db, "vector_vision", Some("weighted")),
        vec![ids[0], ids[2], ids[1]]
    );
}

/// RED: writes to one vector index do not perturb another vector index's policy order.
#[test]
fn mv05_independent_index_writes() {
    let db = Database::open_memory();
    create_evidence_schema(&db, true, false);
    let ids = seed_evidence(&db);
    assert_eq!(
        evidence_search(&db, "vector_text", Some("weighted_text")),
        vec![ids[1], ids[2], ids[0]]
    );
    db.execute(
        "UPDATE evidence SET vector_vision = $v WHERE id = $id",
        &params(vec![
            ("v", Value::Vector(vector_for_cosine(1.0))),
            ("id", Value::Uuid(ids[0])),
        ]),
    )
    .unwrap();
    assert_eq!(
        evidence_search(&db, "vector_text", Some("weighted_text")),
        vec![ids[1], ids[2], ids[0]]
    );
    db.execute(
        "UPDATE evidence SET vector_text = $v WHERE id = $id",
        &params(vec![
            ("v", Value::Vector(vector_for_cosine(0.1))),
            ("id", Value::Uuid(ids[1])),
        ]),
    )
    .unwrap();
    assert_eq!(
        evidence_search(&db, "vector_text", Some("weighted_text")),
        vec![ids[2], ids[0], ids[1]]
    );
}

/// RED: dropping one vector column leaves the other policy usable.
#[test]
fn mv06_drop_column_one_policy_isolated() {
    let db = Database::open_memory();
    create_evidence_schema(&db, false, false);
    let ids = seed_evidence(&db);
    db.execute(
        "ALTER TABLE evidence DROP COLUMN vector_vision",
        &empty_params(),
    )
    .unwrap();
    let missing_vision = SemanticQuery::new("evidence", "vector_vision", query_vec(), 1);
    assert!(
        matches!(
            db.semantic_search(missing_vision),
            Err(Error::UnknownVectorIndex { ref index })
                if index.table == "evidence" && index.column == "vector_vision"
        ),
        "dropped vector_vision index must not remain queryable"
    );
    assert_eq!(
        evidence_search(&db, "vector_text", Some("weighted_text")),
        vec![ids[1], ids[2], ids[0]]
    );
}

/// RED: reopen preserves both policies on a two-vector table.
#[test]
fn mv07_reopen_preserves_both_policies() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("mv.db");
    {
        let db = Database::open(&path).unwrap();
        create_evidence_schema(&db, true, false);
    }
    let reopened = Database::open(&path).unwrap();
    let text_policy = rank_policy_meta(&reopened, "evidence", "vector_text");
    let vision_policy = rank_policy_meta(&reopened, "evidence", "vector_vision");
    assert!(text_policy.formula.contains("confidence"));
    assert!(vision_policy.formula.contains("producer_quality"));
    assert_eq!(text_policy.protected_index, "observations_id_idx");
    assert_eq!(vision_policy.protected_index, "observations_id_idx");
}

/// RED: sync round-trips both policies on a two-vector table.
#[test]
fn mv08_sync_round_trip_both_policies() {
    let origin = Database::open_memory();
    create_evidence_schema(&origin, true, false);
    let peer = Database::open_memory();
    peer.apply_changes(
        origin.changes_since(Lsn(0)),
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    )
    .unwrap();
    assert_eq!(
        rank_policy_meta(&peer, "evidence", "vector_text"),
        rank_policy_meta(&origin, "evidence", "vector_text")
    );
    assert_eq!(
        rank_policy_meta(&peer, "evidence", "vector_vision"),
        rank_policy_meta(&origin, "evidence", "vector_vision")
    );
}

/// REGRESSION GUARD: single-vector rank-policy tables retain the one-column metadata shape.
#[test]
fn mv09_single_vector_regression_guard() {
    let db = Database::open_memory();
    create_standard_policy_schema(&db);
    assert!(
        rank_policy_meta(&db, "decisions", "embedding")
            .formula
            .contains("confidence")
    );
}

/// RED: same vector column names across tables route by full (table, column).
#[test]
fn mv10_cross_table_same_column_routing() {
    let db = Database::open_memory();
    create_evidence_schema(&db, false, true);
    let eids = seed_evidence(&db);
    db.execute(
        "CREATE TABLE digest_observations (id UUID PRIMARY KEY, digest_quality REAL)",
        &empty_params(),
    )
    .unwrap();
    db.execute(
        "CREATE INDEX digest_observations_id_idx ON digest_observations(id)",
        &empty_params(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE digests (
            id UUID PRIMARY KEY,
            observation_id UUID,
            vector_text VECTOR(2) RANK_POLICY (
                JOIN digest_observations ON id,
                FORMULA '{vector_score} * coalesce({digest_quality}, 1.0)',
                SORT_KEY weighted
            )
        )",
        &empty_params(),
    )
    .unwrap();
    let dids = [uuid(191), uuid(192), uuid(193)];
    for (i, id) in dids.iter().enumerate() {
        db.execute(
            "INSERT INTO digest_observations (id, digest_quality) VALUES ($id, $quality)",
            &params(vec![
                ("id", Value::Uuid(*id)),
                ("quality", Value::Float64([0.95, 0.1, 0.7][i])),
            ]),
        )
        .unwrap();
        db.execute(
            "INSERT INTO digests (id, observation_id, vector_text) VALUES ($id, $obs, $vec)",
            &params(vec![
                ("id", Value::Uuid(*id)),
                ("obs", Value::Uuid(*id)),
                ("vec", Value::Vector(vector_for_cosine([0.6, 1.0, 0.8][i]))),
            ]),
        )
        .unwrap();
    }
    assert_eq!(
        evidence_search(&db, "vector_text", Some("weighted")),
        vec![eids[1], eids[2], eids[0]]
    );
    assert_eq!(
        sql_vector_search_ids_with_query(
            &db,
            "digests",
            "vector_text",
            Some("weighted"),
            query_vec(),
            3
        ),
        vec![dids[0], dids[2], dids[1]]
    );
}

/// RED: quantization and RANK_POLICY compose through parse, persistence, and sync.
#[test]
fn mv11_quantization_composes_with_rank_policy() {
    const MV11_DIMS: usize = 768;

    fn create_sq8_schema(db: &Database) {
        db.execute(
            "CREATE TABLE observations (id UUID PRIMARY KEY, confidence REAL)",
            &empty_params(),
        )
        .unwrap();
        db.execute(
            "CREATE INDEX observations_id_idx ON observations(id)",
            &empty_params(),
        )
        .unwrap();
        db.execute(
            &format!("CREATE TABLE evidence_sq8 (id UUID PRIMARY KEY, observation_id UUID, vector_text VECTOR({MV11_DIMS}) WITH (quantization = 'SQ8') RANK_POLICY (JOIN observations ON id, FORMULA '{{vector_score}} * coalesce({{confidence}}, 1.0)', SORT_KEY weighted))"),
            &empty_params(),
        )
        .unwrap();
    }

    fn seed_sq8(db: &Database) -> [Uuid; 10] {
        let ids = [
            uuid(201),
            uuid(202),
            uuid(203),
            uuid(204),
            uuid(205),
            uuid(206),
            uuid(207),
            uuid(208),
            uuid(209),
            uuid(210),
        ];
        for (i, id) in ids.iter().enumerate() {
            let confidence = if i >= 5 { 1.0 } else { 0.1 };
            let score = 1.0 - (i as f32 * 0.02);
            db.execute(
                "INSERT INTO observations (id, confidence) VALUES ($id, $confidence)",
                &params(vec![
                    ("id", Value::Uuid(*id)),
                    ("confidence", Value::Float64(confidence)),
                ]),
            )
            .unwrap();
            db.execute(
                "INSERT INTO evidence_sq8 (id, observation_id, vector_text) VALUES ($id, $obs, $vec)",
                &params(vec![
                    ("id", Value::Uuid(*id)),
                    ("obs", Value::Uuid(*id)),
                    (
                        "vec",
                        Value::Vector(vector_for_cosine_dim(score, MV11_DIMS)),
                    ),
                ]),
            )
            .unwrap();
        }
        ids
    }

    fn sq8_search(db: &Database) -> Vec<SearchResult> {
        let mut query =
            SemanticQuery::new("evidence_sq8", "vector_text", query_vec_dim(MV11_DIMS), 5);
        query.sort_key = Some("weighted".into());
        db.semantic_search(query).unwrap()
    }

    fn assert_sq8_policy_meta(db: &Database) {
        let meta = db.table_meta("evidence_sq8").unwrap();
        let col = meta
            .columns
            .iter()
            .find(|c| c.name == "vector_text")
            .unwrap();
        assert_eq!(col.quantization, contextdb_core::VectorQuantization::SQ8);
        let policy = col.rank_policy.as_ref().expect("rank policy");
        assert_eq!(policy.joined_table, "observations");
        assert_eq!(policy.joined_column, "id");
        assert_eq!(policy.sort_key, "weighted");
        assert!(policy.formula.contains("vector_score"));
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("mv11.db");
    let expected;
    let first;
    {
        let db = Database::open(&path).unwrap();
        create_sq8_schema(&db);
        let ids = seed_sq8(&db);
        expected = vec![ids[5], ids[6], ids[7], ids[8], ids[9]];
        db.__reset_rank_policy_eval_count();
        first = sq8_search(&db);
        assert_eq!(result_ids(&first), expected);
        assert!(first.iter().all(|result| result.rank.is_finite()));
        assert_eq!(db.__rank_policy_eval_count(), 10);
        db.__reset_rank_policy_eval_count();
        let second = sq8_search(&db);
        assert_eq!(first, second);
        assert_eq!(db.__rank_policy_eval_count(), 10);
        assert_sq8_policy_meta(&db);
    }

    let reopened = Database::open(&path).unwrap();
    assert_eq!(sq8_search(&reopened), first);
    assert_sq8_policy_meta(&reopened);

    let origin = Database::open_memory();
    create_sq8_schema(&origin);
    seed_sq8(&origin);
    assert_sq8_policy_meta(&origin);
    let peer = Database::open_memory();
    peer.apply_changes(
        origin.changes_since(Lsn(0)),
        &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    )
    .unwrap();
    assert_sq8_policy_meta(&peer);
    assert_eq!(
        rank_policy_meta(&peer, "evidence_sq8", "vector_text"),
        rank_policy_meta(&origin, "evidence_sq8", "vector_text")
    );
    assert_eq!(result_ids(&sq8_search(&peer)), expected);
}

/// RED: ALTER TABLE ADD COLUMN can register a new rank-policy vector column.
#[test]
fn mv12_alter_table_add_column_with_rank_policy() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE observations (id UUID PRIMARY KEY, producer_quality REAL)",
        &empty_params(),
    )
    .unwrap();
    db.execute(
        "CREATE INDEX observations_id_idx ON observations(id)",
        &empty_params(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE evidence (id UUID PRIMARY KEY, observation_id UUID, vector_text VECTOR(2))",
        &empty_params(),
    )
    .unwrap();
    db.execute("ALTER TABLE evidence ADD COLUMN vector_vision VECTOR(2) RANK_POLICY (JOIN observations ON id, FORMULA '{vector_score} * coalesce({producer_quality}, 1.0)', SORT_KEY weighted_vision)", &empty_params()).unwrap();
    let ids = [uuid(211), uuid(212), uuid(213)];
    for (i, id) in ids.iter().enumerate() {
        db.execute(
            "INSERT INTO observations (id, producer_quality) VALUES ($id, $quality)",
            &params(vec![
                ("id", Value::Uuid(*id)),
                ("quality", Value::Float64([0.1, 0.9, 0.5][i])),
            ]),
        )
        .unwrap();
        db.execute(
            "INSERT INTO evidence (id, observation_id, vector_text, vector_vision) VALUES ($id, $obs, $text, $vision)",
            &params(vec![
                ("id", Value::Uuid(*id)),
                ("obs", Value::Uuid(*id)),
                ("text", Value::Vector(vector_for_cosine([0.5, 0.6, 0.7][i]))),
                ("vision", Value::Vector(vector_for_cosine([1.0, 0.8, 0.7][i]))),
            ]),
        )
        .unwrap();
    }
    assert_eq!(
        sql_vector_search_ids_with_query(
            &db,
            "evidence",
            "vector_vision",
            Some("weighted_vision"),
            query_vec(),
            3
        ),
        vec![ids[1], ids[2], ids[0]]
    );
    assert_eq!(db.__rank_policy_formula_parse_count(), 1);
    assert!(
        rank_policy_meta(&db, "evidence", "vector_vision")
            .formula
            .contains("producer_quality")
    );
    assert!(
        db.table_meta("evidence")
            .unwrap()
            .columns
            .iter()
            .find(|c| c.name == "vector_text")
            .unwrap()
            .rank_policy
            .is_none()
    );
    expect_join_table_unknown(
        db.execute("ALTER TABLE evidence ADD COLUMN bogus VECTOR(2) RANK_POLICY (JOIN missing_table ON id, FORMULA '1.0', SORT_KEY weighted_bogus)", &empty_params()),
        "evidence.bogus",
        "missing_table",
    );
}
