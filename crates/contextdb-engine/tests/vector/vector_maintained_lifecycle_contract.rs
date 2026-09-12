use contextdb_core::{Value, VectorIndexRef, VectorPartitionKey};
use contextdb_engine::{Database, MaintenancePolicy};
use contextdb_vector::VectorPartitionRef;
use std::collections::HashMap;
use tempfile::TempDir;
use uuid::Uuid;

const INDEXED_ROWS: usize = 1_000;
const MAX_BUILD_MAINTENANCE_CYCLES: usize = 64;
const SENTINEL_ID: u128 = 900_000;

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn params(pairs: Vec<(&str, Value)>) -> HashMap<String, Value> {
    pairs
        .into_iter()
        .map(|(name, value)| (name.to_string(), value))
        .collect()
}

fn index(column: &str) -> VectorIndexRef {
    VectorIndexRef::new("lifecycle_items", column)
}

fn ranked_vector(rank: usize, leading_axis: usize) -> Vec<f32> {
    let score = (1.0 - rank as f32 * 0.0005).clamp(0.05, 1.0);
    let remainder = (1.0 - score * score).max(0.0).sqrt();
    match leading_axis {
        0 => vec![score, remainder, 0.0],
        1 => vec![0.0, score, remainder],
        _ => panic!("the fixture defines only its first two axes"),
    }
}

fn axis(axis: usize) -> Vec<f32> {
    let mut vector = vec![0.0; 3];
    vector[axis] = 1.0;
    vector
}

fn seed_fixture() -> Database {
    let db = Database::open_memory();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);

    db.execute(
        "CREATE TABLE control_items (id UUID PRIMARY KEY, embedding VECTOR(3))",
        &empty(),
    )
    .expect("create the small vector-search control table");
    for (ordinal, vector) in [axis(0), axis(1), axis(2)].into_iter().enumerate() {
        db.execute(
            "INSERT INTO control_items (id, embedding) VALUES ($id, $embedding)",
            &params(vec![
                ("id", Value::Uuid(Uuid::from_u128(ordinal as u128 + 1))),
                ("embedding", Value::Vector(vector)),
            ]),
        )
        .expect("seed the small vector-search control table");
    }

    db.execute(
        "CREATE TABLE lifecycle_items (\
            id UUID PRIMARY KEY, \
            embedding_primary VECTOR(3), \
            embedding_untouched VECTOR(3)\
        )",
        &empty(),
    )
    .expect("create the maintained-index fixture table");

    let tx = db.begin_or_panic();
    for ordinal in 0..INDEXED_ROWS {
        db.insert_row(
            tx,
            "lifecycle_items",
            params(vec![
                ("id", Value::Uuid(Uuid::from_u128(10_000 + ordinal as u128))),
                (
                    "embedding_primary",
                    Value::Vector(ranked_vector(ordinal, 0)),
                ),
                (
                    "embedding_untouched",
                    Value::Vector(ranked_vector(ordinal, 1)),
                ),
            ]),
        )
        .expect("stage one deterministic vector row");
    }
    db.commit(tx)
        .expect("commit the shared maintained-index fixture");

    db
}

fn seed_file_fixture(path: &std::path::Path) -> Database {
    let db = Database::open(path).expect("open the durable maintained-index fixture");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE lifecycle_items (id UUID PRIMARY KEY, embedding_primary VECTOR(3) SEARCH_MODE INDEXED)",
        &empty(),
    )
    .expect("create the durable maintained-index fixture table");
    let tx = db.begin_or_panic();
    for ordinal in 0..INDEXED_ROWS {
        db.insert_row(
            tx,
            "lifecycle_items",
            params(vec![
                ("id", Value::Uuid(Uuid::from_u128(30_000 + ordinal as u128))),
                (
                    "embedding_primary",
                    Value::Vector(ranked_vector(ordinal, 0)),
                ),
            ]),
        )
        .expect("stage one durable vector row");
    }
    db.commit(tx)
        .expect("commit the durable maintained-index fixture");
    db
}

fn nearest_ids(
    db: &Database,
    table: &str,
    column: &str,
    query: Vec<f32>,
    limit: usize,
) -> Vec<Uuid> {
    let result = db
        .execute(
            &format!("SELECT id FROM {table} ORDER BY {column} <=> $query LIMIT {limit}"),
            &params(vec![("query", Value::Vector(query))]),
        )
        .unwrap_or_else(|error| panic!("search {table}.{column}: {error}"));
    let id_column = result
        .columns
        .iter()
        .position(|name| name == "id")
        .expect("the search projection contains id");
    result
        .rows
        .into_iter()
        .map(|row| match row.get(id_column) {
            Some(Value::Uuid(id)) => *id,
            other => panic!("the search returned a non-UUID id: {other:?}"),
        })
        .collect()
}

fn graph_serial(db: &Database, vector_index: &VectorIndexRef) -> Option<u64> {
    db.__debug_vector_hnsw_build_serial_for_test(vector_index.clone())
}

fn graph_generation_status(
    db: &Database,
    vector_index: &VectorIndexRef,
) -> contextdb_vector::store::VectorGraphGenerationStatus {
    db.vector_store_for_test()
        .partition_graph_generation_status(&VectorPartitionRef::new(
            vector_index.clone(),
            VectorPartitionKey::unpartitioned(),
        ))
        .expect("the declared unpartitioned vector column has one graph state")
}

fn inspected_query_state(db: &Database, vector_index: &VectorIndexRef) -> String {
    let result = db
        .execute("SHOW VECTOR_INDEXES", &empty())
        .expect("inspect vector route state without advancing it");
    let table = result
        .columns
        .iter()
        .position(|name| name == "table")
        .unwrap();
    let column = result
        .columns
        .iter()
        .position(|name| name == "column")
        .unwrap();
    let query_state = result
        .columns
        .iter()
        .position(|name| name == "query_state")
        .unwrap();
    result
        .rows
        .iter()
        .find_map(|row| match (&row[table], &row[column], &row[query_state]) {
            (Value::Text(table), Value::Text(column), Value::Text(state))
                if table == &vector_index.table && column == &vector_index.column =>
            {
                Some(state.clone())
            }
            _ => None,
        })
        .expect("inspection includes the declared vector column")
}

fn record_contract(failures: &mut Vec<String>, holds: bool, message: impl Into<String>) {
    if !holds {
        failures.push(message.into());
    }
}

#[test]
fn engine_owned_file_maintenance_publishes_a_durable_indexed_route_for_a_reopened_reader() {
    let root = TempDir::new().expect("durable vector fixture directory");
    let path = root.path().join("maintained-vectors.redb");
    let db = seed_file_fixture(&path);
    let primary = index("embedding_primary");
    let indexed_sql = "SELECT id FROM lifecycle_items \
                       ORDER BY embedding_primary <=> $query USE VECTOR INDEXED LIMIT 10";
    let query = params(vec![("query", Value::Vector(axis(0)))]);

    assert_eq!(
        graph_serial(&db, &primary),
        None,
        "seeding and opening the file must not eagerly build the route"
    );
    assert!(
        db.execute(indexed_sql, &query).is_err(),
        "before publication INDEXED must refuse rather than synchronously build"
    );

    db.set_maintenance_policy(MaintenancePolicy::EngineOwned);
    let worker_clock = db
        .__hold_engine_owned_vector_maintenance_clock_for_test()
        .expect("the declared vector index starts the real engine-owned worker");
    assert_eq!(
        graph_serial(&db, &primary),
        None,
        "holding the real worker proves publication has not been smuggled into policy selection"
    );
    let caller = std::thread::current().id();
    let mut worker_cycles = 0;
    while graph_serial(&db, &primary).is_none() && worker_cycles < MAX_BUILD_MAINTENANCE_CYCLES {
        let completed = worker_clock
            .wake_once()
            .expect("one deterministic engine-owned worker wake completes");
        assert_ne!(
            completed.worker_thread_id, caller,
            "the maintenance publication must run on the engine-owned worker"
        );
        worker_cycles += 1;
    }
    let published_generation = graph_serial(&db, &primary).unwrap_or_else(|| {
        panic!(
            "engine-owned maintenance did not publish within {MAX_BUILD_MAINTENANCE_CYCLES} finite wakes"
        )
    });
    assert!(
        published_generation > 0,
        "the engine-owned worker publishes a durable generation identity"
    );
    db.__reset_last_query_vector_trace_for_test();
    let writer_result = db
        .execute(indexed_sql, &query)
        .expect("the publisher serves its completed indexed route");
    assert!(
        db.__take_last_query_vector_trace_for_test()
            .is_some_and(|trace| trace.used_hnsw),
        "the publisher's successful query uses the maintained route"
    );
    drop(worker_clock);
    db.close().expect("close the publishing database");

    let reader = Database::open(&path).expect("a separate reader reopens the published file");
    let reader_clock = reader
        .__hold_engine_owned_vector_maintenance_clock_for_test()
        .expect("the reopened declaration starts its real engine-owned worker");
    assert_eq!(reader.maintenance_policy(), MaintenancePolicy::EngineOwned);
    let before_read = graph_generation_status(&reader, &primary);
    assert_eq!(
        before_read
            .dormant_base
            .map(|generation| generation.generation_id),
        Some(published_generation),
        "the separately reopened reader receives the exact durable generation before any query"
    );
    assert!(
        !before_read.base_resident,
        "opening the reader must not eagerly decode or rebuild the published graph"
    );
    reader.__reset_last_query_vector_trace_for_test();
    let reopened_result = reader
        .execute(indexed_sql, &query)
        .expect("the reopened reader serves the durable indexed route");
    assert_eq!(reopened_result.rows, writer_result.rows);
    assert!(
        reader
            .__take_last_query_vector_trace_for_test()
            .is_some_and(|trace| trace.used_hnsw),
        "the reopened reader uses the durable indexed route rather than row presence alone"
    );
    let after_read = graph_generation_status(&reader, &primary);
    assert_eq!(
        after_read.base.map(|generation| generation.generation_id),
        Some(published_generation),
        "the indexed reader loaded the generation the publisher actually committed"
    );
    drop(reader_clock);
    reader.close().expect("close the reopened reader");
}

#[test]
fn caller_driven_vector_indexes_are_built_and_maintained_only_by_maintenance() {
    let db = seed_fixture();
    let primary = index("embedding_primary");
    let untouched = index("embedding_untouched");

    let maintenance = db.maintenance_status();
    assert_eq!(maintenance.policy, MaintenancePolicy::CallerDriven);
    assert_eq!(
        maintenance.active_maintenance_loops, 0,
        "caller-driven maintenance must not leave a hidden worker running"
    );

    let control = nearest_ids(&db, "control_items", "embedding", axis(0), 1);
    assert_eq!(
        control,
        vec![Uuid::from_u128(1)],
        "the small unpartitioned control must seed and search normally"
    );
    assert_eq!(
        graph_serial(&db, &VectorIndexRef::new("control_items", "embedding")),
        None,
        "the small exact-search control must not need an HNSW graph"
    );

    let mut failures = Vec::new();

    assert_eq!(graph_serial(&db, &primary), None);
    assert_eq!(inspected_query_state(&db, &primary), "unavailable");
    let auto_sql = "SELECT id FROM lifecycle_items ORDER BY embedding_primary <=> $query LIMIT 10";
    let indexed_sql = "SELECT id FROM lifecycle_items ORDER BY embedding_primary <=> $query \
                       USE VECTOR INDEXED LIMIT 10";
    let before_explain = db
        .explain(auto_sql)
        .expect("explain the initial AUTO route");
    assert!(
        !before_explain.contains("HNSWSearch"),
        "explain cannot promise an unpublished route: {before_explain}"
    );
    let unavailable = db
        .execute(
            indexed_sql,
            &params(vec![("query", Value::Vector(axis(0)))]),
        )
        .expect_err("INDEXED execution refuses before maintenance publication");
    let unavailable = unavailable.to_string();
    assert!(unavailable.contains("lifecycle_items.embedding_primary"));
    assert!(!unavailable.contains("VectorIndexRef"));
    db.__reset_last_query_vector_trace_for_test();
    let primary_control = db
        .execute(auto_sql, &params(vec![("query", Value::Vector(axis(0)))]))
        .expect("AUTO executes by exact fallback before maintenance");
    assert_eq!(primary_control.rows.len(), 10);
    assert_eq!(
        primary_control.rows[0][0],
        Value::Uuid(Uuid::from_u128(10_000))
    );
    record_contract(
        &mut failures,
        graph_serial(&db, &primary).is_none(),
        "an ordinary query secretly built the primary HNSW graph under CallerDriven maintenance",
    );

    assert_eq!(graph_serial(&db, &untouched), None);
    let mut maintenance_cycles = 0;
    while maintenance_cycles < MAX_BUILD_MAINTENANCE_CYCLES
        && (graph_serial(&db, &primary).is_none() || graph_serial(&db, &untouched).is_none())
    {
        db.run_maintenance_cycle()
            .expect("one finite caller-driven maintenance cycle completes");
        maintenance_cycles += 1;
    }
    let primary_after_maintenance = graph_serial(&db, &primary);
    let untouched_after_maintenance = graph_serial(&db, &untouched);
    record_contract(
        &mut failures,
        primary_after_maintenance.is_some() && untouched_after_maintenance.is_some(),
        format!(
            "caller-driven maintenance did not build both vector indexes within \
             {MAX_BUILD_MAINTENANCE_CYCLES} finite cycles: ran {maintenance_cycles}, primary \
             {primary_after_maintenance:?}, untouched {untouched_after_maintenance:?}"
        ),
    );
    assert_eq!(inspected_query_state(&db, &primary), "ready");
    let ready_explain = db
        .explain(auto_sql)
        .expect("explain the published AUTO route");
    assert!(
        ready_explain.contains("HNSWSearch"),
        "explain must report the same published route execution can use: {ready_explain}"
    );
    db.__reset_last_query_vector_trace_for_test();
    let indexed_ready = db
        .execute(
            indexed_sql,
            &params(vec![("query", Value::Vector(axis(0)))]),
        )
        .expect("INDEXED execution uses the maintenance-published route");
    assert_eq!(indexed_ready.rows.len(), 10);
    assert!(
        db.__debug_last_query_vector_trace_for_test()
            .is_some_and(|trace| trace.used_hnsw),
        "INDEXED execution must use the published route"
    );

    let primary_ready = nearest_ids(&db, "lifecycle_items", "embedding_primary", axis(0), 10);
    assert_eq!(primary_ready.len(), 10);
    assert_eq!(primary_ready[0], Uuid::from_u128(10_000));
    let primary_after_ready_query = graph_serial(&db, &primary);
    record_contract(
        &mut failures,
        primary_after_ready_query == primary_after_maintenance,
        format!(
            "the first primary query after maintenance built the graph: maintenance serial \
             {primary_after_maintenance:?}, query serial {primary_after_ready_query:?}"
        ),
    );

    let untouched_control = nearest_ids(&db, "lifecycle_items", "embedding_untouched", axis(1), 10);
    assert_eq!(untouched_control.len(), 10);
    assert_eq!(untouched_control[0], Uuid::from_u128(10_000));
    let untouched_after_query = graph_serial(&db, &untouched);
    record_contract(
        &mut failures,
        untouched_after_query == untouched_after_maintenance,
        format!(
            "the first query after maintenance built the untouched graph: maintenance serial \
             {untouched_after_maintenance:?}, query serial {untouched_after_query:?}"
        ),
    );
    let untouched_serial = untouched_after_query.expect(
        "the downstream checks require the healthy graph that the query currently supplies",
    );
    let mut primary_serial =
        primary_after_ready_query.expect("the downstream checks require the healthy primary graph");

    db.execute(
        "INSERT INTO lifecycle_items (id, embedding_primary) VALUES ($id, $primary)",
        &params(vec![
            ("id", Value::Uuid(Uuid::from_u128(SENTINEL_ID))),
            ("primary", Value::Vector(axis(1))),
        ]),
    )
    .expect("commit a fresh vector row");
    record_contract(
        &mut failures,
        graph_serial(&db, &primary) == Some(primary_serial),
        "a committed insert cleared or replaced the whole primary graph",
    );
    let inserted = nearest_ids(&db, "lifecycle_items", "embedding_primary", axis(1), 1);
    assert_eq!(inserted, vec![Uuid::from_u128(SENTINEL_ID)]);
    let after_insert_search = graph_serial(&db, &primary)
        .expect("the inserted vector remains searchable through an HNSW route");
    record_contract(
        &mut failures,
        after_insert_search == primary_serial,
        format!(
            "the first query after insert rebuilt every primary vector: before {primary_serial}, \
             after {after_insert_search}"
        ),
    );
    primary_serial = after_insert_search;

    db.execute(
        "UPDATE lifecycle_items SET embedding_primary = $embedding WHERE id = $id",
        &params(vec![
            ("id", Value::Uuid(Uuid::from_u128(SENTINEL_ID))),
            ("embedding", Value::Vector(axis(2))),
        ]),
    )
    .expect("commit a replacement vector");
    record_contract(
        &mut failures,
        graph_serial(&db, &primary) == Some(primary_serial),
        "a committed replacement cleared or replaced the whole primary graph",
    );
    let replaced = nearest_ids(&db, "lifecycle_items", "embedding_primary", axis(2), 1);
    assert_eq!(replaced, vec![Uuid::from_u128(SENTINEL_ID)]);
    let after_replacement_search = graph_serial(&db, &primary)
        .expect("the replacement remains searchable through an HNSW route");
    record_contract(
        &mut failures,
        after_replacement_search == primary_serial,
        format!(
            "the first query after replacement rebuilt every primary vector: before \
             {primary_serial}, after {after_replacement_search}"
        ),
    );
    primary_serial = after_replacement_search;

    db.execute(
        "DELETE FROM lifecycle_items WHERE id = $id",
        &params(vec![("id", Value::Uuid(Uuid::from_u128(SENTINEL_ID)))]),
    )
    .expect("commit a vector-row deletion");
    record_contract(
        &mut failures,
        graph_serial(&db, &primary) == Some(primary_serial),
        "a committed delete cleared or replaced the whole primary graph",
    );
    let after_delete = nearest_ids(&db, "lifecycle_items", "embedding_primary", axis(2), 10);
    assert!(!after_delete.contains(&Uuid::from_u128(SENTINEL_ID)));
    let after_delete_search = graph_serial(&db, &primary)
        .expect("the surviving vectors remain searchable through an HNSW route");
    record_contract(
        &mut failures,
        after_delete_search == primary_serial,
        format!(
            "the first query after delete rebuilt every primary vector: before {primary_serial}, \
             after {after_delete_search}"
        ),
    );

    record_contract(
        &mut failures,
        graph_serial(&db, &untouched) == Some(untouched_serial),
        format!(
            "writes and searches in the primary index disturbed the untouched index: before \
             {untouched_serial}, after {:?}",
            graph_serial(&db, &untouched)
        ),
    );

    assert!(
        failures.is_empty(),
        "maintained vector-index lifecycle violations:\n- {}",
        failures.join("\n- ")
    );
}
