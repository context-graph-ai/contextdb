//! Declared vector-policy resolution through SQL, the bounded kernel, and
//! SemanticQuery. The fixtures declare crossovers, verify the effective
//! settings disclosed by inspection and explain, and confirm that AUTO
//! resolves to EXACT while an INDEXED request is refused with its typed error.

use contextdb_core::read_contract::{DeadlineClock, DeadlineWait, ReadLimits};
use contextdb_core::{Error, Value, VectorIndexRef, VectorSearchMode};
use contextdb_engine::executor::bounded_read_test_support as bounded;
use contextdb_engine::{Database, MaintenancePolicy, QueryResult, SemanticQuery};
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

const CONTROL_SQL: &str = "SELECT id FROM resolver_control \
    ORDER BY embedding <=> $query USE VECTOR EXACT LIMIT 3";

#[derive(Clone, Copy)]
struct FrozenClock;

impl DeadlineClock for FrozenClock {
    fn now_ms(&self) -> u64 {
        0
    }

    fn wait_until(&self, _deadline_ms: u64) -> DeadlineWait<'_> {
        Box::pin(async {})
    }
}

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn params(pairs: impl IntoIterator<Item = (&'static str, Value)>) -> HashMap<String, Value> {
    pairs
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

fn roomy_limits() -> ReadLimits {
    ReadLimits {
        result_rows: 1_000,
        result_bytes: 16 * 1024 * 1024,
        work: 10_000_000,
        active_ms: 1_000_000,
        memory: 64 * 1024 * 1024,
        cursor_page_rows: 128,
        cursor_page_bytes: 4 * 1024 * 1024,
        cursor_idle_ms: 10_000,
        cursor_lifetime_ms: 100_000,
    }
}

fn bounded_request(sql: &str, bound: HashMap<String, Value>) -> bounded::BoundedReadRequest {
    bounded::BoundedReadRequest::new(sql, bound, roomy_limits(), Arc::new(FrozenClock))
}

fn ids(result: &QueryResult) -> Vec<Uuid> {
    let id_column = result
        .columns()
        .iter()
        .position(|name| name == "id" || name.rsplit('.').next() == Some("id"))
        .expect("the vector projection has id");
    result
        .rows()
        .iter()
        .map(|row| match row.get(id_column) {
            Some(Value::Uuid(id)) => *id,
            value => panic!("vector query returned a non-UUID id: {value:?}"),
        })
        .collect()
}

fn column(result: &QueryResult, name: &str) -> usize {
    result
        .columns()
        .iter()
        .position(|column| column == name)
        .unwrap_or_else(|| panic!("inspection omits {name}: {:?}", result.columns()))
}

fn value<'a>(result: &'a QueryResult, row: usize, name: &str) -> &'a Value {
    result
        .rows()
        .get(row)
        .and_then(|row| row.get(column(result, name)))
        .unwrap_or_else(|| panic!("inspection omits row {row} field {name}: {result:?}"))
}

fn assert_explain_fact(explain: &str, name: &str, expected: &str) {
    let fact = format!("{name}: {expected}");
    assert!(
        explain.contains(&fact),
        "explain must name {fact:?}, got: {explain}"
    );
}

fn query_vector() -> Value {
    Value::Vector(vec![1.0, 0.0, 0.0])
}

fn control_db() -> Database {
    let db = Database::open_memory();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE resolver_control (id UUID PRIMARY KEY, embedding VECTOR(3) SEARCH_MODE EXACT)",
        &empty(),
    )
    .expect("create the exact-route control");
    for (id, embedding) in [
        (Uuid::from_u128(0x5101), vec![1.0, 0.0, 0.0]),
        (Uuid::from_u128(0x5102), vec![0.9, 0.1, 0.0]),
        (Uuid::from_u128(0x5103), vec![0.0, 1.0, 0.0]),
    ] {
        db.execute(
            "INSERT INTO resolver_control (id, embedding) VALUES ($id, $embedding)",
            &params([
                ("id", Value::Uuid(id)),
                ("embedding", Value::Vector(embedding)),
            ]),
        )
        .expect("seed the exact-route control");
    }
    db
}

fn assert_exact_control(db: &Database) {
    let bound = params([("query", query_vector())]);
    let ordinary = db
        .execute(CONTROL_SQL, &bound)
        .expect("current ordinary SQL reaches the EXACT route");
    assert_eq!(
        ids(&ordinary),
        vec![
            Uuid::from_u128(0x5101),
            Uuid::from_u128(0x5102),
            Uuid::from_u128(0x5103),
        ]
    );
    let bounded = bounded::execute(db, &bounded_request(CONTROL_SQL, bound))
        .expect("current bounded reader reaches the same EXACT route");
    assert_eq!(bounded.result.rows(), ordinary.rows());

    let mut rust = SemanticQuery::new("resolver_control", "embedding", vec![1.0, 0.0, 0.0], 3);
    rust.search_mode = Some(VectorSearchMode::Exact);
    let rust_ids = db
        .semantic_search(rust)
        .expect("current Rust SemanticQuery reaches the EXACT route")
        .into_iter()
        .map(|result| match result.values.get("id") {
            Some(Value::Uuid(id)) => *id,
            value => panic!("Rust result returned a non-UUID id: {value:?}"),
        })
        .collect::<Vec<_>>();
    assert_eq!(rust_ids, ids(&ordinary));

    // An INDEXED request is refused with its typed error on every door.
    let indexed_sql = CONTROL_SQL.replace("USE VECTOR EXACT", "USE VECTOR INDEXED");
    let bound = params([("query", query_vector())]);
    assert!(
        matches!(
            db.execute(&indexed_sql, &bound),
            Err(Error::VectorIndexedRouteUnavailable { .. })
        ),
        "SQL refuses an INDEXED request"
    );
    // The bounded reader reports an engine error as its text; it is the text
    // of the same typed refusal.
    let refusal = Error::VectorIndexedRouteUnavailable {
        index: VectorIndexRef::new("resolver_control", "embedding"),
    }
    .to_string();
    assert!(
        matches!(
            bounded::execute(db, &bounded_request(&indexed_sql, bound)),
            Err(bounded::TestError::Engine(message)) if message == refusal
        ),
        "the bounded reader refuses an INDEXED request with the typed refusal"
    );
    let mut indexed = SemanticQuery::new("resolver_control", "embedding", vec![1.0, 0.0, 0.0], 3);
    indexed.search_mode = Some(VectorSearchMode::Indexed);
    assert!(
        matches!(
            db.semantic_search(indexed),
            Err(Error::VectorIndexedRouteUnavailable { .. })
        ),
        "Rust SemanticQuery refuses an INDEXED request"
    );
}

fn declared_fixture(db: &Database) {
    db.execute(
        "CREATE TABLE resolver_policy_docs (\
         id UUID PRIMARY KEY, \
         scope INTEGER NOT NULL, \
         f32 VECTOR(3) PARTITION_KEY (scope) AUTO_INDEX_AT 4 \
             IVF (PROBES = 17), \
         sq8 VECTOR(3) WITH (quantization = 'SQ8') PARTITION_KEY (scope) AUTO_INDEX_AT 3, \
         sq4 VECTOR(3) WITH (quantization = 'SQ4') PARTITION_KEY (scope) AUTO_INDEX_AT 5\
         )",
        &empty(),
    )
    .expect(
        "vector declarations accept independently declared aggregate AUTO_INDEX_AT and index policy",
    );
}

#[test]
fn declared_vector_policy_resolves_consistently_at_default_and_declared_boundaries() {
    let db = control_db();
    assert_exact_control(&db);

    // The control above already ran the EXACT search routes before the
    // declared policy arrives.
    declared_fixture(&db);

    let indexes = db
        .execute("SHOW VECTOR_INDEXES", &empty())
        .expect("inspect the resolver's declared and effective summary");
    let f32 = indexes
        .rows()
        .iter()
        .position(|row| {
            row[column(&indexes, "table")] == Value::Text("resolver_policy_docs".into())
                && row[column(&indexes, "column")] == Value::Text("f32".into())
        })
        .expect("one F32 index summary row");
    let sq8 = indexes
        .rows()
        .iter()
        .position(|row| row[column(&indexes, "column")] == Value::Text("sq8".into()))
        .expect("one SQ8 index summary row");
    let sq4 = indexes
        .rows()
        .iter()
        .position(|row| row[column(&indexes, "column")] == Value::Text("sq4".into()))
        .expect("one SQ4 index summary row");
    assert_eq!(
        value(&indexes, f32, "declared_auto_index_at"),
        &Value::Int64(4)
    );
    assert_eq!(
        value(&indexes, f32, "effective_auto_index_at"),
        &Value::Int64(4)
    );
    assert_eq!(value(&indexes, f32, "declared_probes"), &Value::Int64(17));
    assert_eq!(
        value(&indexes, sq8, "declared_auto_index_at"),
        &Value::Int64(3)
    );
    assert_eq!(
        value(&indexes, sq4, "declared_auto_index_at"),
        &Value::Int64(5)
    );

    // The default rows pin the silent compatibility ladder without allocating
    // a 5,001- or 50,001-vector normal-test fixture. The resolver observation
    // itself must later supply the build/admission facts at 5,000/5,001 and
    // 50,000/50,001; no test-only count/profile substitute exists today.
    db.execute(
        "CREATE TABLE resolver_defaults (\
         id UUID PRIMARY KEY, \
         f32 VECTOR(3), \
         sq8 VECTOR(3) WITH (quantization = 'SQ8'), \
         sq4 VECTOR(3) WITH (quantization = 'SQ4')\
         )",
        &empty(),
    )
    .expect("create silent compatibility-default columns");
    let defaults = db
        .execute("SHOW VECTOR_INDEXES", &empty())
        .expect("inspect silent compatibility defaults");
    for (name, threshold) in [("f32", 1_000_i64), ("sq8", 5_001), ("sq4", 5_001)] {
        let row = defaults
            .rows()
            .iter()
            .position(|row| {
                row[column(&defaults, "table")] == Value::Text("resolver_defaults".into())
                    && row[column(&defaults, "column")] == Value::Text(name.into())
            })
            .unwrap_or_else(|| panic!("silent default summary has resolver_defaults.{name}"));
        assert_eq!(
            value(&defaults, row, "declared_auto_index_at"),
            &Value::Null,
            "silence remains a declaration absence for {name}"
        );
        assert_eq!(
            value(&defaults, row, "effective_auto_index_at"),
            &Value::Int64(threshold),
            "effective crossover for {name}"
        );
    }
    for (id, scope, vector) in [
        (Uuid::from_u128(0x5201), 1_i64, vec![1.0, 0.0, 0.0]),
        (Uuid::from_u128(0x5202), 1, vec![0.9, 0.1, 0.0]),
        (Uuid::from_u128(0x5203), 2, vec![0.8, 0.2, 0.0]),
        (Uuid::from_u128(0x5204), 2, vec![0.0, 1.0, 0.0]),
    ] {
        db.execute(
            "INSERT INTO resolver_policy_docs (id, scope, f32) VALUES ($id, $scope, $vector)",
            &params([
                ("id", Value::Uuid(id)),
                ("scope", Value::Int64(scope)),
                ("vector", Value::Vector(vector)),
            ]),
        )
        .expect("seed the declared aggregate F32 fixture");
    }
    let sql = "SELECT id FROM resolver_policy_docs WHERE scope IN (1, 2) \
               ORDER BY f32 <=> $query LIMIT 3";
    let bound = params([("query", query_vector())]);
    let ordinary = db
        .execute(sql, &bound)
        .expect("AUTO serves declared F32 threshold");
    assert_eq!(
        ids(&ordinary),
        vec![
            Uuid::from_u128(0x5201),
            Uuid::from_u128(0x5202),
            Uuid::from_u128(0x5203),
        ],
        "the declared aggregate F32 route has stable nearest ids"
    );
    assert!(
        !db.__debug_last_query_vector_used_indexed_route_for_test(),
        "no index ships: the statement compares its four rows exactly"
    );
    let bounded = bounded::execute(&db, &bounded_request(sql, bound.clone()))
        .expect("bounded reader honors the declared F32 threshold");
    assert_eq!(bounded.result.rows(), ordinary.rows());
    assert!(!db.__debug_last_query_vector_used_indexed_route_for_test());
    let rust = db
        .semantic_search(SemanticQuery::new(
            "resolver_policy_docs",
            "f32",
            vec![1.0, 0.0, 0.0],
            3,
        ))
        .expect("Rust AUTO honors the same declared F32 threshold");
    let rust_ids = rust
        .into_iter()
        .map(|result| match result.values.get("id") {
            Some(Value::Uuid(id)) => *id,
            value => panic!("Rust AUTO returned a non-UUID id: {value:?}"),
        })
        .collect::<Vec<_>>();
    assert_eq!(rust_ids, ids(&ordinary));
    assert!(!db.__debug_last_query_vector_used_indexed_route_for_test());
    let explain = db.explain(sql).expect("explain the declared AUTO route");
    assert_explain_fact(&explain, "aggregate_allowed_vectors", "4");
    assert_explain_fact(&explain, "effective_auto_index_at", "4");
    assert_explain_fact(&explain, "auto_index_at_source", "declared");
    assert_explain_fact(&explain, "resolved_mode", "EXACT");
    assert!(explain.contains("route=exact"));
    assert!(explain.contains("fallback=no vector index ships in this release"));
    let disclosed = db
        .explain_output(sql)
        .expect("passive explain")
        .vector_search
        .expect("explain discloses the vector route");
    assert!(
        disclosed
            .partitions
            .iter()
            .all(|partition| partition.probes == Some(17)
                && partition.probes_source.as_deref() == Some("declared")),
        "explain names the declared breadth: {disclosed:?}"
    );
}
