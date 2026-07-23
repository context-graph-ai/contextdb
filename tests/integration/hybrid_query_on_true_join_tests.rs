//! The flagship hybrid query (docs/usage-scenarios.md Scenario 4,
//! "Has Anyone Solved This Before?") must return rows once real data exists.
//!
//! The documented pattern LEFT JOINs vector-ranked rows to a graph-traversal
//! CTE with a standard `ON TRUE` join condition. A boolean-literal join/
//! filter predicate (`TRUE`/`FALSE`) must be a legal, generally-supported
//! predicate — including once the join's right side actually produces a row
//! to evaluate the condition against, not merely while the tables are empty.
//!
//! Schema mirrors docs/usage-scenarios.md Scenario 3/4 exactly: `decisions`
//! (id, description, status, confidence, embedding), `entities` (id, name,
//! entity_type, properties), `edges` (id, source_id, target_id, edge_type)
//! DAG('DEPENDS_ON', 'BASED_ON').

use super::helpers::embedding384;
use contextdb_core::Value;
use contextdb_engine::Database;
use std::collections::HashMap;
use uuid::Uuid;

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn params(pairs: Vec<(&str, Value)>) -> HashMap<String, Value> {
    pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
}

fn setup_scenario4_db() -> Database {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE decisions (id UUID PRIMARY KEY, description TEXT NOT NULL, status TEXT NOT NULL, confidence REAL, embedding VECTOR(384))",
        &empty(),
    )
    .expect("CREATE TABLE decisions");
    db.execute(
        "CREATE TABLE entities (id UUID PRIMARY KEY, name TEXT NOT NULL, entity_type TEXT NOT NULL, properties JSON)",
        &empty(),
    )
    .expect("CREATE TABLE entities");
    db.execute(
        "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID NOT NULL, target_id UUID NOT NULL, edge_type TEXT NOT NULL) DAG('DEPENDS_ON', 'BASED_ON')",
        &empty(),
    )
    .expect("CREATE TABLE edges");
    db
}

// The exact documented query, verbatim in shape, from
// docs/usage-scenarios.md Scenario 4.
const SCENARIO4_QUERY: &str = "\
WITH similar_decisions AS (\
  SELECT id, description, confidence \
  FROM decisions \
  WHERE status = 'active' \
  ORDER BY embedding <=> $task_embedding \
  LIMIT 10\
), \
basis_entities AS (\
  SELECT b_id FROM GRAPH_TABLE(\
    edges \
    MATCH (d)-[:BASED_ON]->(b) \
    WHERE d.id IN (SELECT id FROM similar_decisions) \
    COLUMNS (b.id AS b_id)\
  )\
) \
SELECT sd.id, sd.description, sd.confidence, e.name, e.properties \
FROM similar_decisions sd \
LEFT JOIN basis_entities be ON TRUE \
LEFT JOIN entities e ON e.id = be.b_id";

// ============================================================================
// With real data present (an active decision, a basis entity, and a
// BASED_ON edge between them producing at least one graph-traversal row),
// the documented Scenario 4 query must return Ok with the expected joined
// row, not a plan error.
// ============================================================================
#[test]
fn scenario4_hybrid_query_returns_rows_once_traversal_has_data() {
    let db = setup_scenario4_db();

    let decision_id = Uuid::new_v4();
    let entity_id = Uuid::new_v4();
    let edge_id = Uuid::new_v4();
    let query_embedding = embedding384(&[1.0, 0.0]);

    db.execute(
        "INSERT INTO decisions (id, description, status, confidence, embedding) VALUES ($id, $description, $status, $confidence, $embedding)",
        &params(vec![
            ("id", Value::Uuid(decision_id)),
            ("description", Value::Text("use managed RDS for the primary datastore".to_string())),
            ("status", Value::Text("active".to_string())),
            ("confidence", Value::Float64(0.9)),
            ("embedding", Value::Vector(embedding384(&[1.0, 0.0]))),
        ]),
    )
    .expect("INSERT decisions");

    db.execute(
        "INSERT INTO entities (id, name, entity_type, properties) VALUES ($id, $name, $entity_type, $properties)",
        &params(vec![
            ("id", Value::Uuid(entity_id)),
            ("name", Value::Text("RDS".to_string())),
            ("entity_type", Value::Text("SERVICE".to_string())),
            ("properties", Value::Json(serde_json::json!({"region": "us-east-1"}))),
        ]),
    )
    .expect("INSERT entities");

    db.execute(
        "INSERT INTO edges (id, source_id, target_id, edge_type) VALUES ($id, $source_id, $target_id, $edge_type)",
        &params(vec![
            ("id", Value::Uuid(edge_id)),
            ("source_id", Value::Uuid(decision_id)),
            ("target_id", Value::Uuid(entity_id)),
            ("edge_type", Value::Text("BASED_ON".to_string())),
        ]),
    )
    .expect("INSERT edges");

    let result = db.execute(
        SCENARIO4_QUERY,
        &params(vec![("task_embedding", Value::Vector(query_embedding))]),
    );

    let r = result.expect(
        "the documented Scenario 4 hybrid query must succeed once the graph \
         traversal produces a real row, not fail with \
         `unsupported WHERE expression: Literal(Bool(true))`",
    );

    assert_eq!(
        r.rows.len(),
        1,
        "expected exactly one joined row (the decision, its basis entity), got {:?}",
        r.rows
    );
    let row = &r.rows[0];
    assert_eq!(row[0], Value::Uuid(decision_id), "sd.id");
    assert_eq!(
        row[1],
        Value::Text("use managed RDS for the primary datastore".to_string()),
        "sd.description"
    );
    assert_eq!(row[2], Value::Float64(0.9), "sd.confidence");
    assert_eq!(row[3], Value::Text("RDS".to_string()), "e.name");
    assert_eq!(
        row[4],
        Value::Json(serde_json::json!({"region": "us-east-1"})),
        "e.properties"
    );
}

// ============================================================================
// Sanity companion: the same query against empty tables must keep
// returning Ok with zero rows.
// ============================================================================
#[test]
fn scenario4_hybrid_query_returns_empty_on_empty_tables() {
    let db = setup_scenario4_db();
    let query_embedding = embedding384(&[1.0, 0.0]);

    let result = db.execute(
        SCENARIO4_QUERY,
        &params(vec![("task_embedding", Value::Vector(query_embedding))]),
    );

    let r = result.expect("Scenario 4 query against empty tables must succeed with zero rows");
    assert_eq!(r.rows.len(), 0);
}

// ============================================================================
// Generic (non-Scenario-4) tables used by the additions below, so none of
// these tests can pass via a fix that merely recognizes the documented
// query's specific schema/CTE/alias shape rather than implementing general
// boolean-literal-predicate semantics.
// ============================================================================

fn seed_generic_table(db: &Database, values: &[i64]) {
    db.execute("CREATE TABLE g (id UUID PRIMARY KEY, v INTEGER)", &empty())
        .expect("CREATE TABLE g");
    for v in values {
        db.execute(
            "INSERT INTO g (id, v) VALUES ($id, $v)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("v", Value::Int64(*v)),
            ]),
        )
        .expect("INSERT g");
    }
}

fn int_col(value: &Value) -> i64 {
    match value {
        Value::Int64(v) => *v,
        other => panic!("expected Value::Int64, got {other:?}"),
    }
}

// ============================================================================
// A bare `WHERE TRUE` on a generic table with multiple real rows must
// return every row. This is the general base-row WHERE-evaluator contract
// (`eval_bool_expr`), independent of the join-row evaluator
// (`eval_query_result_bool_expr`) the Scenario 4 tests above exercise — the
// two evaluators are separate functions, so both must independently honor a
// boolean-literal predicate.
// ============================================================================
#[test]
fn select_where_bare_true_returns_all_rows() {
    let db = Database::open_memory();
    seed_generic_table(&db, &[10, 20, 30]);

    let r = db
        .execute("SELECT v FROM g WHERE TRUE", &empty())
        .expect("WHERE TRUE must succeed and return every row");
    let mut got: Vec<i64> = r.rows.iter().map(|row| int_col(&row[0])).collect();
    got.sort();
    assert_eq!(got, vec![10, 20, 30]);
}

// ============================================================================
// A bare `WHERE FALSE` on the same non-empty table must return zero
// rows. Recorded honestly: on its own this assertion does NOT discriminate a
// genuine `Literal(Bool(false))` evaluation from a broken evaluator,
// because every row-filter call site in the executor swallows a predicate
// evaluation error into "row excluded" (`row_matches(..).unwrap_or(false)`)
// rather than propagating it — an unhandled-literal `PlanError` would be
// silently absorbed per row, which happens to coincide with the correct
// answer for FALSE specifically. The `WHERE TRUE` test above (which needs a
// NON-empty correct answer) is the test that actually discriminates; this
// one is kept as a companion regression lock, not as independent proof.
// ============================================================================
#[test]
fn select_where_bare_false_returns_no_rows() {
    let db = Database::open_memory();
    seed_generic_table(&db, &[10, 20, 30]);

    let r = db
        .execute("SELECT v FROM g WHERE FALSE", &empty())
        .expect("WHERE FALSE must succeed");
    assert_eq!(r.rows.len(), 0, "got {:?}", r.rows);
}

fn seed_generic_join_tables(db: &Database, left: &[i64], right: &[i64]) {
    db.execute("CREATE TABLE gl (id UUID PRIMARY KEY, v INTEGER)", &empty())
        .expect("CREATE TABLE gl");
    db.execute("CREATE TABLE gr (id UUID PRIMARY KEY, w INTEGER)", &empty())
        .expect("CREATE TABLE gr");
    for v in left {
        db.execute(
            "INSERT INTO gl (id, v) VALUES ($id, $v)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("v", Value::Int64(*v)),
            ]),
        )
        .expect("INSERT gl");
    }
    for w in right {
        db.execute(
            "INSERT INTO gr (id, w) VALUES ($id, $w)",
            &params(vec![
                ("id", Value::Uuid(Uuid::new_v4())),
                ("w", Value::Int64(*w)),
            ]),
        )
        .expect("INSERT gr");
    }
}

// ============================================================================
// Multi-row `INNER JOIN ... ON TRUE` Cartesian cardinality: 2 left
// rows x 3 right rows must produce exactly 6 rows, one per (left, right)
// pair. The single-row Scenario 4 fixture above cannot distinguish a correct
// general evaluator from special-casing that returns "the one compatible
// row" — this fixture has no unique compatible row, only the full cross
// product.
// ============================================================================
#[test]
fn inner_join_on_true_produces_full_cartesian_product() {
    let db = Database::open_memory();
    seed_generic_join_tables(&db, &[0, 1], &[100, 101, 102]);

    let r = db
        .execute("SELECT gl.v, gr.w FROM gl INNER JOIN gr ON TRUE", &empty())
        .expect("INNER JOIN ... ON TRUE must succeed");
    assert_eq!(
        r.rows.len(),
        6,
        "2 left rows x 3 right rows must Cartesian-product to 6 rows, got {:?}",
        r.rows
    );
    let mut got: Vec<(i64, i64)> = r
        .rows
        .iter()
        .map(|row| (int_col(&row[0]), int_col(&row[1])))
        .collect();
    got.sort();
    let mut want: Vec<(i64, i64)> = [0i64, 1]
        .into_iter()
        .flat_map(|v| [100i64, 101, 102].into_iter().map(move |w| (v, w)))
        .collect();
    want.sort();
    assert_eq!(got, want);
}

// ============================================================================
// `LEFT JOIN ... ON FALSE` must produce exactly one null-extended row
// PER LEFT ROW (the join condition never matches, so every left row falls
// through to the LEFT JOIN's null-extension path) — never zero rows, never
// a Cartesian product.
// ============================================================================
#[test]
fn left_join_on_false_produces_one_null_extended_row_per_left_row() {
    let db = Database::open_memory();
    seed_generic_join_tables(&db, &[0, 1], &[100, 101, 102]);

    let r = db
        .execute("SELECT gl.v, gr.w FROM gl LEFT JOIN gr ON FALSE", &empty())
        .expect("LEFT JOIN ... ON FALSE must succeed");
    assert_eq!(
        r.rows.len(),
        2,
        "must produce exactly one null-extended row per left row (2), got {:?}",
        r.rows
    );
    let mut got_v: Vec<i64> = r.rows.iter().map(|row| int_col(&row[0])).collect();
    got_v.sort();
    assert_eq!(got_v, vec![0, 1]);
    for row in &r.rows {
        assert_eq!(
            row[1],
            Value::Null,
            "gr.w must be null-extended, got {row:?}"
        );
    }
}

// ============================================================================
// `INNER JOIN ... ON FALSE` must produce zero rows (INNER never
// null-extends, unlike LEFT above).
// ============================================================================
#[test]
fn inner_join_on_false_produces_no_rows() {
    let db = Database::open_memory();
    seed_generic_join_tables(&db, &[0, 1], &[100, 101, 102]);

    let r = db
        .execute("SELECT gl.v, gr.w FROM gl INNER JOIN gr ON FALSE", &empty())
        .expect("INNER JOIN ... ON FALSE must succeed");
    assert_eq!(r.rows.len(), 0, "got {:?}", r.rows);
}
