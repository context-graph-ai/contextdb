#![cfg(feature = "test-seams")]
//! Partition scope resolves after parameters bind. Ordinary SQL and the
//! SemanticQuery WHERE surface reject missing parameters; passive EXPLAIN leaves
//! an unbound AUTO route and count unresolved instead of describing an unnamed
//! scope. Each surface also retains its bound or parameter-free control.

use contextdb_core::{Value, VectorSearchMode};
use contextdb_engine::{Database, MaintenancePolicy, QueryResult, SemanticQuery};
use std::collections::HashMap;
use uuid::Uuid;

const SCOPE_ALPHA: &str = "alpha-scope";
const SCOPE_BRAVO: &str = "bravo-scope";
const MAX_FINITE_CYCLES: usize = 32;

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn params(pairs: impl IntoIterator<Item = (&'static str, Value)>) -> HashMap<String, Value> {
    pairs
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

fn column(result: &QueryResult, name: &str) -> usize {
    result
        .columns
        .iter()
        .position(|candidate| candidate == name)
        .unwrap_or_else(|| panic!("inspection contains {name}: {:?}", result.columns))
}

fn text<'a>(result: &'a QueryResult, row: usize, name: &str) -> &'a str {
    match result.rows[row].get(column(result, name)) {
        Some(Value::Text(value)) => value,
        other => panic!("inspection {name} is text, got {other:?}"),
    }
}

/// Seeds one AUTO-mode partitioned vector column with two authorized partitions
/// (`SCOPE_ALPHA`, `SCOPE_BRAVO`), each with real, distinct rows -- so an
/// unbound-key query has a genuine unauthorized-scope aggregate to withhold, not an
/// empty column that would make every proof accidentally trivial.
fn seed(db: &Database) {
    db.execute(
        "CREATE TABLE docs (\
            id UUID PRIMARY KEY, \
            scope_id TEXT NOT NULL, \
            embedding VECTOR(3) PARTITION_KEY (scope_id) \
                MAX_PARTITIONS 8 SEARCH_MODE AUTO\
        )",
        &empty(),
    )
    .expect("declare a partitioned AUTO-mode vector column");

    for (offset, scope, vector) in [
        (0_u128, SCOPE_ALPHA, vec![1.0, 0.0, 0.0]),
        (1_u128, SCOPE_ALPHA, vec![0.9, 0.1, 0.0]),
        (2_u128, SCOPE_BRAVO, vec![0.0, 1.0, 0.0]),
        (3_u128, SCOPE_BRAVO, vec![0.0, 0.9, 0.1]),
    ] {
        db.execute(
            "INSERT INTO docs (id, scope_id, embedding) VALUES ($id, $scope, $embedding)",
            &params([
                (
                    "id",
                    Value::Uuid(Uuid::from_u128(
                        0xB0B0_0000_0000_0000_0000_0000_0000_0000 + offset,
                    )),
                ),
                ("scope", Value::Text(scope.to_owned())),
                ("embedding", Value::Vector(vector)),
            ]),
        )
        .expect("commit one fixture row");
    }
}

fn fixture() -> Database {
    let db = Database::open_memory();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    seed(&db);
    db
}

fn show_partitions(db: &Database) -> QueryResult {
    db.execute("SHOW VECTOR_PARTITIONS FOR docs.embedding", &empty())
        .expect("SHOW VECTOR_PARTITIONS is an admin inspection surface")
}

fn drive_to_ready(db: &Database) {
    for _ in 0..MAX_FINITE_CYCLES {
        let partitions = show_partitions(db);
        if (0..partitions.rows.len()).all(|row| text(&partitions, row, "query_state") == "ready") {
            return;
        }
        db.run_maintenance_cycle()
            .expect("one caller-driven maintenance batch returns");
    }
    panic!("finite caller-driven maintenance did not publish every partition route");
}

/// Surface 1 -- SQL execution: every executing reader binds every placeholder in the
/// query expression and candidate predicate before any partition scope or route
/// availability is judged (`bind_vector_search_parameters`, executor.rs).
#[test]
fn sql_execution_of_an_unbound_partition_key_is_the_ordinary_missing_parameter_refusal() {
    let db = fixture();
    drive_to_ready(&db);

    let sql = "SELECT id FROM docs WHERE scope_id = $scope \
               ORDER BY embedding <=> [1.0,0.0,0.0] LIMIT 2";
    let error = db
        .execute(sql, &empty())
        .expect_err("an unbound named placeholder in a partition predicate must fail to execute");
    assert!(
        error.to_string().contains("missing parameter: scope"),
        "the planner resolves partition scope only after parameters are bound, so a forgotten \
         binding is the ordinary missing-parameter refusal, never a route refusal that sends the \
         caller to add a relational index or switch modes: {error}"
    );
}

/// Surface 2 -- the semantic-search WHERE door. `Database::semantic_search`
/// accepts a raw `where_clause` string and `SemanticQuery` carries no parameter
/// map, so a placeholder in that clause can never be bound. It must be the
/// ordinary missing-parameter refusal, never "no bounded relational candidate
/// route; add a supporting relational index or use EXACT".
#[test]
fn semantic_search_where_door_unbound_placeholder_is_the_ordinary_missing_parameter_refusal() {
    let db = fixture();
    drive_to_ready(&db);

    let query = SemanticQuery {
        where_clause: Some("scope_id = $scope".to_owned()),
        search_mode: Some(VectorSearchMode::Indexed),
        ..SemanticQuery::new("docs", "embedding", vec![1.0, 0.0, 0.0], 2)
    };
    let error = db.semantic_search(query).expect_err(
        "an unbound named placeholder in semantic_search's where_clause must fail to execute, \
         exactly like the same placeholder would in ordinary SQL",
    );
    assert!(
        error.to_string().contains("missing parameter"),
        "the same bind-before-route rule that governs SQL execution must govern the \
         semantic-search WHERE door: an unbound placeholder is the caller's forgotten binding, \
         never a missing relational index or a reason to switch to EXACT: {error}"
    );
}

/// Surface 3 -- `.explain` of an unbound key. Falling back to the whole authorized
/// column's aggregate count, and deriving `resolved_mode` from it, would misreport a
/// scope the caller never named and let a restricted handle learn that scope's
/// count. For an unbound key, count and AUTO route are reported as
/// unresolved-until-bind with a typed reason, and the partition shape as one state
/// with the key redacted, while explicit modes report normally.
#[test]
fn explain_reports_no_count_or_route_for_an_unbound_key() {
    let db = fixture();
    drive_to_ready(&db);

    let no_predicate_sql =
        "SELECT id FROM docs ORDER BY embedding <=> [1.0,0.0,0.0] USE VECTOR AUTO LIMIT 2";
    let whole_column_count = db
        .explain_output(no_predicate_sql)
        .unwrap_or_else(|error| panic!("explain_output must bind nothing: {error}"))
        .vector_search
        .expect("a vector-similarity SELECT reports a vector disclosure")
        .aggregate_allowed_vectors;

    let unbound_eq_sql = "SELECT id FROM docs WHERE scope_id = $scope \
                           ORDER BY embedding <=> [1.0,0.0,0.0] USE VECTOR AUTO LIMIT 2";
    let unbound_in_sql = "SELECT id FROM docs WHERE scope_id IN ($a, $b) \
                           ORDER BY embedding <=> [1.0,0.0,0.0] USE VECTOR AUTO LIMIT 2";

    let eq_disclosure = db
        .explain_output(unbound_eq_sql)
        .unwrap_or_else(|error| panic!("explain_output must bind nothing: {error}"))
        .vector_search
        .expect("a vector-similarity SELECT reports a vector disclosure");
    let in_disclosure = db
        .explain_output(unbound_in_sql)
        .unwrap_or_else(|error| panic!("explain_output must bind nothing: {error}"))
        .vector_search
        .expect("a vector-similarity SELECT reports a vector disclosure");

    // Route: an unbound key leaves AUTO unresolved. It must never be derived from a
    // count computed over a scope the caller never named.
    assert_eq!(
        eq_disclosure.resolved_mode,
        VectorSearchMode::Auto,
        "an unbound partition key must leave the AUTO route unresolved-until-bind rather than \
         resolving it to EXACT or INDEXED from a count taken over an unnamed scope: {eq_disclosure:?}"
    );

    // Typed reason: the disclosure's existing typed-reason channels (`fallback`,
    // `refusal`) must name the unresolved state, not stay silent about it.
    let reason = eq_disclosure
        .fallback
        .as_deref()
        .or(eq_disclosure.refusal.as_deref())
        .unwrap_or("");
    assert!(
        ["unresolved", "unbound", "unbind", "bind"]
            .iter()
            .any(|needle| reason.to_lowercase().contains(needle)),
        "an unresolved AUTO route for an unbound partition key must carry a typed reason \
         explaining why: fallback={:?} refusal={:?}",
        eq_disclosure.fallback,
        eq_disclosure.refusal
    );

    // Count: never the whole authorized column's aggregate for a scope nobody named.
    assert_ne!(
        eq_disclosure.aggregate_allowed_vectors, whole_column_count,
        "an unbound partition key must not disclose the whole authorized column's aggregate \
         count -- that count belongs to a scope the caller never named: {eq_disclosure:?}"
    );

    // Partition shape: one redacted state, not a fact that leaks how many unbound
    // branches the predicate happens to name (a single `=` versus a two-value `IN`
    // must not report different shapes when neither branch is bound to anything).
    assert_eq!(
        eq_disclosure.scope, in_disclosure.scope,
        "the partition shape for an unbound key must be one redacted state regardless of how \
         many unbound branches the predicate names: eq={:?} in={:?}",
        eq_disclosure.scope, in_disclosure.scope
    );

    // Partition-detail list: the same "one redacted state" contract that governs
    // `scope` above also governs the sibling per-partition HNSW disclosure list.
    // An unbound key must not enumerate one entry per authorized partition in the
    // whole column -- that is the same count leak as `aggregate_allowed_vectors`
    // above, through a different field, and it slips past a check that only looks
    // at `scope`.
    assert_eq!(
        eq_disclosure.partition_hnsw.len(),
        1,
        "an unbound partition key must report the partition-detail list as one redacted state, \
         not one entry per authorized partition in the whole column -- that is the same leak as \
         the aggregate count, through a different field: {:?}",
        eq_disclosure.partition_hnsw
    );
    assert_eq!(
        eq_disclosure.partition_hnsw.len(),
        in_disclosure.partition_hnsw.len(),
        "the partition-detail list for an unbound key must be one redacted state regardless of \
         how many unbound branches the predicate names: eq={:?} in={:?}",
        eq_disclosure.partition_hnsw,
        in_disclosure.partition_hnsw
    );
}
