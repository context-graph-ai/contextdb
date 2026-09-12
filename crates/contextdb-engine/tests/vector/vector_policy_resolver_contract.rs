//! Declared vector-policy resolution through SQL, the bounded kernel, and
//! SemanticQuery. The fixtures execute default and declared crossovers and
//! verify the effective graph settings disclosed by the maintained route.

use contextdb_core::read_contract::{DeadlineClock, DeadlineWait, ReadLimits};
use contextdb_core::{Value, VectorSearchMode};
use contextdb_engine::executor::bounded_read_test_support as bounded;
use contextdb_engine::{Database, MaintenancePolicy, QueryResult, SemanticQuery};
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

const CONTROL_SQL: &str = "SELECT id FROM resolver_control \
    ORDER BY embedding <=> $query USE VECTOR INDEXED LIMIT 3";

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
        .columns
        .iter()
        .position(|name| name == "id" || name.rsplit('.').next() == Some("id"))
        .expect("the vector projection has id");
    result
        .rows
        .iter()
        .map(|row| match row.get(id_column) {
            Some(Value::Uuid(id)) => *id,
            value => panic!("vector query returned a non-UUID id: {value:?}"),
        })
        .collect()
}

fn column(result: &QueryResult, name: &str) -> usize {
    result
        .columns
        .iter()
        .position(|column| column == name)
        .unwrap_or_else(|| panic!("inspection omits {name}: {:?}", result.columns))
}

fn value<'a>(result: &'a QueryResult, row: usize, name: &str) -> &'a Value {
    result
        .rows
        .get(row)
        .and_then(|row| row.get(column(result, name)))
        .unwrap_or_else(|| panic!("inspection omits row {row} field {name}: {result:?}"))
}

fn assert_explain_fact(explain: &str, name: &str, expected: &str) {
    let fact = format!("{name}: {expected}");
    let partition_fact = match name {
        "effective_ef_search" => Some(format!("hnsw_ef_search={expected}")),
        "ef_search_source" => Some(format!("ef_search_source={expected}")),
        _ => None,
    };
    assert!(
        explain.contains(&fact)
            || partition_fact
                .as_ref()
                .is_some_and(|fact| explain.contains(fact)),
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
        "CREATE TABLE resolver_control (id UUID PRIMARY KEY, embedding VECTOR(3) SEARCH_MODE INDEXED)",
        &empty(),
    )
    .expect("create current maintained-route control");
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
        .expect("seed current maintained-route control");
    }
    for _ in 0..32 {
        db.run_maintenance_cycle()
            .expect("one finite caller-driven maintenance cycle returns");
    }
    db
}

fn assert_current_indexed_control(db: &Database) {
    let bound = params([("query", query_vector())]);
    let ordinary = db
        .execute(CONTROL_SQL, &bound)
        .expect("current ordinary SQL reaches the maintained INDEXED route");
    assert_eq!(
        ids(&ordinary),
        vec![
            Uuid::from_u128(0x5101),
            Uuid::from_u128(0x5102),
            Uuid::from_u128(0x5103),
        ]
    );
    let bounded = bounded::execute(db, &bounded_request(CONTROL_SQL, bound))
        .expect("current bounded reader reaches the same maintained INDEXED route");
    assert_eq!(bounded.result.rows, ordinary.rows);

    let mut rust = SemanticQuery::new("resolver_control", "embedding", vec![1.0, 0.0, 0.0], 3);
    rust.search_mode = Some(VectorSearchMode::Indexed);
    let rust_ids = db
        .semantic_search(rust)
        .expect("current Rust SemanticQuery reaches the maintained INDEXED route")
        .into_iter()
        .map(|result| match result.values.get("id") {
            Some(Value::Uuid(id)) => *id,
            value => panic!("Rust result returned a non-UUID id: {value:?}"),
        })
        .collect::<Vec<_>>();
    assert_eq!(rust_ids, ids(&ordinary));
}

fn declared_fixture(db: &Database) {
    db.execute(
        "CREATE TABLE resolver_policy_docs (\
         id UUID PRIMARY KEY, \
         scope INTEGER NOT NULL, \
         f32 VECTOR(3) PARTITION_KEY (scope) AUTO_INDEX_AT 4 \
             HNSW (M = 12, EF_CONSTRUCTION = 64, EF_SEARCH = 17), \
         sq8 VECTOR(3) WITH (quantization = 'SQ8') PARTITION_KEY (scope) AUTO_INDEX_AT 3, \
         sq4 VECTOR(3) WITH (quantization = 'SQ4') PARTITION_KEY (scope) AUTO_INDEX_AT 5\
         )",
        &empty(),
    )
    .expect(
        "vector declarations accept independently declared aggregate AUTO_INDEX_AT and HNSW policy",
    );
}

#[test]
fn declared_vector_policy_resolves_consistently_at_default_and_declared_boundaries() {
    let db = control_db();
    assert_current_indexed_control(&db);

    // The control above already ran the nearest production maintenance and
    // INDEXED search routes before the declared policy arrives.
    declared_fixture(&db);

    let indexes = db
        .execute("SHOW VECTOR_INDEXES", &empty())
        .expect("inspect the resolver's declared and effective summary");
    let f32 = indexes
        .rows
        .iter()
        .position(|row| {
            row[column(&indexes, "table")] == Value::Text("resolver_policy_docs".into())
                && row[column(&indexes, "column")] == Value::Text("f32".into())
        })
        .expect("one F32 index summary row");
    let sq8 = indexes
        .rows
        .iter()
        .position(|row| row[column(&indexes, "column")] == Value::Text("sq8".into()))
        .expect("one SQ8 index summary row");
    let sq4 = indexes
        .rows
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
    assert_eq!(value(&indexes, f32, "declared_hnsw_m"), &Value::Int64(12));
    assert_eq!(
        value(&indexes, f32, "declared_hnsw_ef_construction"),
        &Value::Int64(64)
    );
    assert_eq!(
        value(&indexes, f32, "declared_hnsw_ef_search"),
        &Value::Int64(17)
    );
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
            .rows
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
    db.execute(
        "INSERT INTO resolver_defaults (id, f32) VALUES ($id, $vector)",
        &params([
            ("id", Value::Uuid(Uuid::from_u128(0x51FF))),
            ("vector", query_vector()),
        ]),
    )
    .expect("give the silent F32 column one real partition policy to disclose");
    let omitted_ef = db
        .explain("SELECT id FROM resolver_defaults ORDER BY f32 <=> $query LIMIT 3")
        .expect("explain silent F32 EF_SEARCH");
    assert_explain_fact(&omitted_ef, "effective_ef_search", "200");
    assert_explain_fact(&omitted_ef, "ef_search_source", "compatibility_profile");

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
    for _ in 0..32 {
        db.run_maintenance_cycle()
            .expect("finite caller-driven maintenance advances declared routes");
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
        db.__debug_last_query_vector_used_hnsw_for_test(),
        "four selected F32 vectors at declared AUTO_INDEX_AT 4 take INDEXED"
    );
    let bounded = bounded::execute(&db, &bounded_request(sql, bound.clone()))
        .expect("bounded reader honors the declared F32 threshold");
    assert_eq!(bounded.result.rows, ordinary.rows);
    assert!(
        db.__debug_last_query_vector_used_hnsw_for_test(),
        "bounded reader keeps the declared aggregate INDEXED route"
    );
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
    assert!(
        db.__debug_last_query_vector_used_hnsw_for_test(),
        "Rust AUTO keeps the declared aggregate INDEXED route"
    );
    let explain = db.explain(sql).expect("explain the declared AUTO route");
    assert_explain_fact(&explain, "aggregate_allowed_vectors", "4");
    assert_explain_fact(&explain, "effective_auto_index_at", "4");
    assert_explain_fact(&explain, "auto_index_at_source", "declared");
    assert_explain_fact(&explain, "resolved_mode", "INDEXED");
    assert_explain_fact(&explain, "effective_ef_search", "17");
    assert_explain_fact(&explain, "ef_search_source", "declared");

    let wide = db
        .explain("SELECT id FROM resolver_policy_docs WHERE scope IN (1, 2) ORDER BY f32 <=> $query LIMIT 20")
        .expect("explain explicit EF_SEARCH breadth");
    assert_explain_fact(&wide, "effective_ef_search", "20");
    assert_explain_fact(&wide, "ef_search_source", "declared_raised_to_k");

    // No current production observation exposes a resolver against a virtual
    // 5,000/5,001 or 50,000/50,001 count without constructing that dataset.
    // Once the declaration above is accepted, this same production .explain
    // and SHOW path must report one resolver's F32 build/admission/search
    // values: (16, 200, max(200, n)) at 5,000; (24, 400, 400) at 5,001 through
    // 50,000; and (16, 200, 200) at 50,001. The held-out recall matrix owns
    // the large datasets; this normal contract deliberately has no fake count
    // override or copied private profile table.
}

#[test]
fn declared_ef_search_reaches_graph_work_without_the_silent_compatibility_floor() {
    let db = Database::open_memory();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE breadth_docs (\
         id UUID PRIMARY KEY, \
         declared VECTOR(3) SEARCH_MODE INDEXED HNSW (EF_SEARCH = 32), \
         silent VECTOR(3) SEARCH_MODE INDEXED\
         )",
        &empty(),
    )
    .expect("declare one explicit and one compatibility-profile search breadth");
    for offset in 0_u128..120 {
        let x = offset as f32 + 1.0;
        db.execute(
            "INSERT INTO breadth_docs (id, declared, silent) \
             VALUES ($id, $vector, $vector)",
            &params([
                ("id", Value::Uuid(Uuid::from_u128(0x5300 + offset))),
                ("vector", Value::Vector(vec![x, 121.0 - x, 1.0])),
            ]),
        )
        .expect("seed one graph candidate");
    }
    for _ in 0..4 {
        db.run_maintenance_cycle()
            .expect("publish both maintained routes");
    }

    let query = params([("query", Value::Vector(vec![1.0, 120.0, 1.0]))]);
    db.execute(
        "SELECT id FROM breadth_docs ORDER BY declared <=> $query LIMIT 10",
        &query,
    )
    .expect("search with the declared breadth");
    let declared = db
        .__debug_last_query_vector_trace_for_test()
        .expect("declared route publishes candidate accounting");
    assert!(declared.used_hnsw);
    assert_eq!(
        declared.hnsw_candidate_count, 32,
        "a hidden 10*k floor would widen this declared graph result to at least 100 candidates"
    );

    db.execute(
        "SELECT id FROM breadth_docs ORDER BY silent <=> $query LIMIT 10",
        &query,
    )
    .expect("search with the silent compatibility breadth");
    let silent = db
        .__debug_last_query_vector_trace_for_test()
        .expect("silent route publishes candidate accounting");
    assert!(silent.used_hnsw);
    assert!(
        silent.hnsw_candidate_count >= 100,
        "the silent column retains the compatibility breadth and 10*k floor: {silent:?}"
    );

    let declared_explain = db
        .explain("SELECT id FROM breadth_docs ORDER BY declared <=> $query LIMIT 10")
        .expect("explain declared breadth");
    assert_explain_fact(&declared_explain, "effective_ef_search", "32");
    assert_explain_fact(&declared_explain, "ef_search_source", "declared");
    let silent_explain = db
        .explain("SELECT id FROM breadth_docs ORDER BY silent <=> $query LIMIT 10")
        .expect("explain silent breadth");
    assert_explain_fact(&silent_explain, "effective_ef_search", "200");
    assert_explain_fact(&silent_explain, "ef_search_source", "compatibility_profile");
}

fn assert_executed_default_boundary(
    db: &Database,
    column_name: &str,
    count: usize,
    threshold: usize,
    expected_id: u128,
) {
    use contextdb_core::VectorIndexRef;

    let index = VectorIndexRef::new("boundary_defaults", column_name);
    let serial = db
        .__debug_vector_hnsw_build_serial_for_test(index.clone())
        .expect("a maintained graph exists below the automatic crossover");
    for override_mode in [None, Some(VectorSearchMode::Indexed)] {
        let indexed = override_mode.is_some() || count >= threshold;
        let mode_clause = if override_mode.is_some() {
            " USE VECTOR INDEXED"
        } else {
            ""
        };
        let sql = format!(
            "SELECT id FROM boundary_defaults ORDER BY {column_name} <=> $query{mode_clause} LIMIT 1"
        );
        let bound = params([("query", Value::Vector(vec![0.0, 0.0, 1.0]))]);
        db.__reset_last_query_vector_trace_for_test();
        let ordinary = db
            .execute(&sql, &bound)
            .expect("SQL executes the boundary search");
        assert_eq!(ids(&ordinary), vec![Uuid::from_u128(expected_id)]);
        assert_eq!(
            db.__debug_last_query_vector_used_hnsw_for_test(),
            indexed,
            "SQL {column_name} at {count}"
        );
        db.__reset_last_query_vector_trace_for_test();
        let bounded = bounded::execute(db, &bounded_request(&sql, bound.clone()))
            .expect("bounded reader executes the same boundary search");
        assert_eq!(bounded.result.rows, ordinary.rows);
        assert_eq!(
            db.__debug_last_query_vector_used_hnsw_for_test(),
            indexed,
            "bounded {column_name} at {count}"
        );
        let mut query =
            SemanticQuery::new("boundary_defaults", column_name, vec![0.0, 0.0, 1.0], 1);
        query.search_mode = override_mode;
        db.__reset_last_query_vector_trace_for_test();
        let rust = db
            .semantic_search(query)
            .expect("Rust executes the same boundary search");
        assert_eq!(rust.len(), 1);
        assert_eq!(
            rust[0].values.get("id"),
            Some(&Value::Uuid(Uuid::from_u128(expected_id)))
        );
        assert_eq!(
            db.__debug_last_query_vector_used_hnsw_for_test(),
            indexed,
            "Rust {column_name} at {count}"
        );
        let explained = db
            .explain_output(&sql.replace("$query", "[0.0, 0.0, 1.0]"))
            .unwrap();
        assert_eq!(
            contextdb_engine::cli_render::render_explain(db, &sql, &bound).unwrap(),
            contextdb_engine::cli_render::render_explain_output(&explained),
            "parameter binding and the equivalent literal disclose the same route"
        );
        let explain = explained.vector_search.unwrap();
        assert_eq!(explain.aggregate_allowed_vectors, Some(count));
        assert_eq!(explain.effective_auto_index_at, threshold);
        assert_eq!(explain.auto_index_at_source, "compatibility_profile");
        assert_eq!(
            explain.resolved_mode,
            if indexed {
                VectorSearchMode::Indexed
            } else {
                VectorSearchMode::Exact
            }
        );
        assert_eq!(
            explain.route.unwrap().as_str(),
            if indexed { "indexed" } else { "exact" }
        );
        assert_eq!(
            explain.fallback.as_deref(),
            if indexed {
                None
            } else {
                Some("aggregate_below_auto_index_at")
            }
        );
        assert_eq!(explain.refusal, None);
        println!(
            "column={column_name} count={count} threshold={threshold} indexed={indexed} id={expected_id}"
        );
        assert_eq!(
            db.__debug_vector_hnsw_build_serial_for_test(index.clone()),
            Some(serial),
            "queries and explanation never build at a crossover"
        );
    }
}

#[test]
fn default_crossovers_execute_both_sides_without_a_normal_write_availability_gap() {
    use contextdb_core::VectorIndexRef;

    let db = Database::open_memory();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE boundary_defaults (id UUID PRIMARY KEY, f32 VECTOR(3), sq8 VECTOR(3) WITH (quantization = 'SQ8'), sq4 VECTOR(3) WITH (quantization = 'SQ4'))",
        &empty(),
    ).unwrap();
    let tx = db.begin_or_panic();
    for ordinal in 1..=999_u128 {
        let vector = if ordinal == 1 {
            vec![0.0, 1.0, 1.0]
        } else {
            vec![1.0, ordinal as f32 / 5_001.0, 0.0]
        };
        db.insert_row(
            tx,
            "boundary_defaults",
            params([
                ("id", Value::Uuid(Uuid::from_u128(ordinal))),
                ("f32", Value::Vector(vector.clone())),
                ("sq8", Value::Vector(vector.clone())),
                ("sq4", Value::Vector(vector)),
            ]),
        )
        .unwrap();
    }
    db.commit(tx).unwrap();
    for _ in 0..8 {
        db.run_maintenance_cycle().unwrap();
    }
    assert_executed_default_boundary(&db, "f32", 999, 1_000, 1);
    let f32_serial = db
        .__debug_vector_hnsw_build_serial_for_test(VectorIndexRef::new("boundary_defaults", "f32"));
    db.execute(
        "INSERT INTO boundary_defaults (id, f32, sq8, sq4) VALUES ($id, $nearest, $other, $other)",
        &params([
            ("id", Value::Uuid(Uuid::from_u128(1_000))),
            ("nearest", Value::Vector(vec![0.0, 0.0, 1.0])),
            ("other", Value::Vector(vec![1.0, 0.2, 0.0])),
        ]),
    )
    .unwrap();
    assert_eq!(
        db.__debug_vector_hnsw_build_serial_for_test(VectorIndexRef::new(
            "boundary_defaults",
            "f32"
        )),
        f32_serial
    );
    assert_executed_default_boundary(&db, "f32", 1_000, 1_000, 1_000);

    let tx = db.begin_or_panic();
    for ordinal in 1_001..=5_000_u128 {
        let vector = Value::Vector(vec![1.0, ordinal as f32 / 5_001.0, 0.0]);
        db.insert_row(
            tx,
            "boundary_defaults",
            params([
                ("id", Value::Uuid(Uuid::from_u128(ordinal))),
                ("f32", Value::Null),
                ("sq8", vector.clone()),
                ("sq4", vector),
            ]),
        )
        .unwrap();
    }
    db.commit(tx).unwrap();
    for _ in 0..8 {
        db.run_maintenance_cycle().unwrap();
    }
    for column_name in ["sq8", "sq4"] {
        assert_executed_default_boundary(&db, column_name, 5_000, 5_001, 1);
    }
    let serials = ["sq8", "sq4"].map(|column_name| {
        db.__debug_vector_hnsw_build_serial_for_test(VectorIndexRef::new(
            "boundary_defaults",
            column_name,
        ))
    });
    db.execute(
        "INSERT INTO boundary_defaults (id, sq8, sq4) VALUES ($id, $nearest, $nearest)",
        &params([
            ("id", Value::Uuid(Uuid::from_u128(5_001))),
            ("nearest", Value::Vector(vec![0.0, 0.0, 1.0])),
        ]),
    )
    .unwrap();
    for (column_name, serial) in ["sq8", "sq4"].into_iter().zip(serials) {
        assert_eq!(
            db.__debug_vector_hnsw_build_serial_for_test(VectorIndexRef::new(
                "boundary_defaults",
                column_name
            )),
            serial
        );
        assert_executed_default_boundary(&db, column_name, 5_001, 5_001, 5_001);
    }
}
