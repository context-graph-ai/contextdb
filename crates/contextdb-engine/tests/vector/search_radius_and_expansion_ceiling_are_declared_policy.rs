//! Two numbers the vector route used to keep as hidden constants are declared
//! policy: HNSW `SEARCH_RADIUS`, how far past its beam's edge a walk keeps
//! expanding (default 0.02), declared beside `EF_SEARCH`; and
//! `AUTO_EXPANSION_CEILING`, the most of its walk budget in percent AUTO
//! expects one partition's graph search to spend (default 25), declared beside
//! `AUTO_INDEX_AT`. Each is persisted, rendered, replayed, disclosed with its
//! source, and honoured by the search; omitted, the search behaves as before.

use contextdb_core::{Error, Value, VectorPolicyDeclarationIssue as Issue};
use contextdb_engine::{Database, MaintenancePolicy, QueryResult, VectorSearchRoute};
use contextdb_vector::hnsw::{
    HnswCandidateDistanceEvent, HnswCandidateObserver, with_hnsw_candidate_observer,
};
use contextdb_vector::mem::AUTO_STATEMENT_EXHAUSTIVE_REASON;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tempfile::TempDir;
use uuid::Uuid;

const DIMENSION: usize = 8;

struct Counter(AtomicU64);

impl HnswCandidateObserver for Counter {
    fn before_candidate_distance(&self, _: HnswCandidateDistanceEvent) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn vector(seed: usize) -> Vec<f32> {
    (0..DIMENSION)
        .map(|axis| {
            let mixed = (seed as u64 + 11)
                .wrapping_mul(0x9e37_79b9_7f4a_7c15)
                .wrapping_add((axis as u64 + 5).wrapping_mul(0xbf58_476d_1ce4_e5b9));
            ((mixed ^ (mixed >> 29)) % 10_007) as f32 / 10_007.0 - 0.5
        })
        .collect()
}

/// One unpartitioned table with `clauses` after the vector type, `rows`
/// deterministic rows and its graph maintained.
fn fixture(db: &Database, clauses: &str, rows: usize) {
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        &format!(
            "CREATE TABLE knobs (id UUID PRIMARY KEY, \
             embedding VECTOR({DIMENSION}) AUTO_INDEX_AT 1{clauses})"
        ),
        &empty(),
    )
    .expect("declare the vector table");
    for seed in 1..=rows {
        db.execute(
            "INSERT INTO knobs (id, embedding) VALUES ($id, $embedding)",
            &HashMap::from([
                ("id".to_owned(), Value::Uuid(Uuid::from_u128(seed as u128))),
                ("embedding".to_owned(), Value::Vector(vector(seed))),
            ]),
        )
        .expect("insert one deterministic row");
    }
    for _ in 0..64 {
        db.run_maintenance_cycle()
            .expect("one caller-driven maintenance cycle");
        let status = db.vector_store_for_test().hnsw_maintenance_status();
        if status.ready_partitions == 1 && status.remaining_partitions == 0 {
            return;
        }
    }
    panic!("maintenance did not publish the graph");
}

fn column(db: &Database) -> contextdb_core::ColumnDef {
    db.table_meta("knobs")
        .expect("the table exists")
        .columns
        .into_iter()
        .find(|column| column.name == "embedding")
        .expect("the vector column exists")
}

fn search(db: &Database, mode: &str) -> (QueryResult, u64) {
    let graph = Arc::new(Counter(AtomicU64::new(0)));
    let result = with_hnsw_candidate_observer(graph.clone(), || {
        db.execute(
            &format!(
                "SELECT id FROM knobs ORDER BY embedding <=> $query USE VECTOR {mode} LIMIT 10"
            ),
            &HashMap::from([("query".to_owned(), Value::Vector(vector(1_000_000)))]),
        )
        .expect("the vector search executes")
    });
    let scores = graph.0.load(Ordering::SeqCst);
    (result, scores)
}

/// The disclosed (radius millionths, source, ceiling, source).
fn disclosed(result: &QueryResult) -> (usize, String, usize, String) {
    let disclosure = result
        .trace
        .vector_search
        .as_ref()
        .expect("the executed search discloses its route");
    (
        disclosure.effective_search_radius_millionths,
        disclosure.search_radius_source.clone(),
        disclosure.effective_auto_expansion_ceiling,
        disclosure.auto_expansion_ceiling_source.clone(),
    )
}

fn routes(result: &QueryResult) -> Vec<(VectorSearchRoute, Option<String>)> {
    result
        .trace
        .vector_search
        .as_ref()
        .expect("the executed search discloses its route")
        .partition_hnsw
        .iter()
        .map(|partition| {
            (
                partition.route.expect("an executed partition has a route"),
                partition.reason.clone(),
            )
        })
        .collect()
}

fn ids(result: &QueryResult) -> Vec<Value> {
    result.rows.iter().map(|row| row[0].clone()).collect()
}

fn refused_issue(db: &Database, sql: &str) -> Issue {
    match db.execute(sql, &empty()) {
        Err(Error::InvalidVectorPolicyDeclaration { issue, .. }) => issue,
        other => panic!("{sql} is refused as a vector policy declaration, got {other:?}"),
    }
}

#[test]
fn both_numbers_are_declared_persisted_replayed_and_disclosed() {
    let root = TempDir::new().expect("create the durable fixture directory");
    let path = root.path().join("knobs.redb");
    let source = Database::open(&path).expect("open the durable fixture");
    fixture(
        &source,
        " AUTO_EXPANSION_CEILING 60 HNSW (EF_SEARCH = 40, SEARCH_RADIUS = 0.125)",
        200,
    );
    let declared = column(&source);
    let rendered = contextdb_engine::cli_render::render_table_meta(
        "knobs",
        &source.table_meta("knobs").expect("the table exists"),
    );
    assert!(
        rendered.contains("SEARCH_RADIUS = 0.125")
            && rendered.contains("AUTO_EXPANSION_CEILING 60"),
        "declared numbers render in the schema: {rendered}"
    );
    assert_eq!(declared.auto_expansion_ceiling, Some(60));
    assert_eq!(declared.hnsw_search_radius_millionths, Some(125_000));
    let (result, _) = search(&source, "INDEXED");
    assert_eq!(
        disclosed(&result),
        (125_000, "declared".into(), 60, "declared".into())
    );
    let explain = source
        .explain_output(&format!(
            "SELECT id FROM knobs ORDER BY embedding <=> [{}] USE VECTOR INDEXED LIMIT 10",
            vector(1_000_000)
                .iter()
                .map(f32::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        ))
        .expect("explain the search")
        .vector_search
        .expect("the explain discloses the vector policy");
    assert_eq!(
        (
            explain.effective_search_radius_millionths,
            explain.search_radius_source.as_str(),
            explain.effective_auto_expansion_ceiling,
            explain.auto_expansion_ceiling_source.as_str(),
        ),
        (125_000, "declared", 60, "declared"),
        "EXPLAIN shows both numbers and their source"
    );

    // Replayed: the receiver declares and honours the same numbers.
    let receiver = Database::open_memory();
    receiver.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    receiver
        .apply_changes(
            source
                .changes_since(contextdb_core::Lsn(0))
                .expect("read the source's changes"),
            &contextdb_engine::sync_types::ConflictPolicies::uniform(
                contextdb_engine::sync_types::ConflictPolicy::LatestWins,
            ),
        )
        .expect("the receiver applies the source's changes");
    let replayed = column(&receiver);
    assert_eq!(
        (
            replayed.auto_expansion_ceiling,
            replayed.hnsw_search_radius_millionths
        ),
        (Some(60), Some(125_000)),
        "both numbers travel with the schema"
    );

    // Reset by DEFAULT: the compatibility profile and its source return,
    // and neither change revises the graph topology.
    let revision = declared.vector_policy_revision;
    source
        .execute(
            "ALTER TABLE knobs ALTER COLUMN embedding SET HNSW (SEARCH_RADIUS = DEFAULT)",
            &empty(),
        )
        .expect("reset the search radius");
    source
        .execute(
            "ALTER TABLE knobs ALTER COLUMN embedding SET AUTO_EXPANSION_CEILING DEFAULT",
            &empty(),
        )
        .expect("reset the expansion ceiling");
    let reset = column(&source);
    let rendered = contextdb_engine::cli_render::render_table_meta(
        "knobs",
        &source.table_meta("knobs").expect("the table exists"),
    );
    assert!(
        !rendered.contains("SEARCH_RADIUS") && !rendered.contains("AUTO_EXPANSION_CEILING"),
        "defaulted numbers are silent in the rendered schema: {rendered}"
    );
    assert_eq!(
        (
            reset.auto_expansion_ceiling,
            reset.hnsw_search_radius_millionths,
            reset.hnsw_ef_search,
            reset.vector_policy_revision
        ),
        (None, None, Some(40), revision),
        "DEFAULT clears only its own member and leaves the topology alone"
    );
    let (result, _) = search(&source, "INDEXED");
    assert_eq!(
        disclosed(&result),
        (
            20_000,
            "compatibility_profile".into(),
            25,
            "compatibility_profile".into()
        )
    );

    // Declared again, they survive a reopen.
    source
        .execute(
            "ALTER TABLE knobs ALTER COLUMN embedding SET HNSW (SEARCH_RADIUS = 1.5)",
            &empty(),
        )
        .expect("declare the search radius");
    source
        .execute(
            "ALTER TABLE knobs ALTER COLUMN embedding SET AUTO_EXPANSION_CEILING 5",
            &empty(),
        )
        .expect("declare the expansion ceiling");
    source.close().expect("close the fixture");
    let reopened = Database::open(&path).expect("reopen the fixture");
    let kept = column(&reopened);
    assert_eq!(
        (
            kept.auto_expansion_ceiling,
            kept.hnsw_search_radius_millionths
        ),
        (Some(5), Some(1_500_000))
    );

    // Values outside their ranges are refused by name.
    for (sql, issue) in [
        (
            "ALTER TABLE knobs ALTER COLUMN embedding SET HNSW (SEARCH_RADIUS = 2.5)",
            Issue::SearchRadiusOutOfRange,
        ),
        (
            "ALTER TABLE knobs ALTER COLUMN embedding SET HNSW (SEARCH_RADIUS = 0.1234567)",
            Issue::SearchRadiusOutOfRange,
        ),
        (
            "ALTER TABLE knobs ALTER COLUMN embedding SET AUTO_EXPANSION_CEILING 0",
            Issue::AutoExpansionCeilingOutOfRange,
        ),
        (
            "ALTER TABLE knobs ALTER COLUMN embedding SET AUTO_EXPANSION_CEILING 101",
            Issue::AutoExpansionCeilingOutOfRange,
        ),
    ] {
        assert_eq!(refused_issue(&reopened, sql), issue, "{sql}");
    }
    assert!(
        reopened
            .execute(
                "ALTER TABLE knobs ALTER COLUMN embedding SET HNSW (M = 1.5)",
                &empty()
            )
            .is_err(),
        "an integer member refuses a decimal"
    );
}

#[test]
fn a_declared_search_radius_changes_the_walk_and_the_default_is_unchanged() {
    let walk = |clauses: &str| {
        let db = Database::open_memory();
        fixture(&db, clauses, 2_000);
        let (result, scores) = search(&db, "INDEXED");
        assert_ne!(
            routes(&result)[0].0,
            VectorSearchRoute::Exact,
            "{clauses}: INDEXED walks the graph"
        );
        (ids(&result), scores)
    };
    let default = walk("");
    let explicit_default = walk(" HNSW (SEARCH_RADIUS = 0.02)");
    let none = walk(" HNSW (SEARCH_RADIUS = 0)");
    let wide = walk(" HNSW (SEARCH_RADIUS = 1)");
    assert_eq!(
        explicit_default, default,
        "declaring the default radius walks exactly as omitting it"
    );
    assert!(
        none.1 < wide.1,
        "a wider declared radius keeps the walk expanding: {} evaluations at 0, {} at 1",
        none.1,
        wide.1
    );
}

#[test]
fn a_declared_expansion_ceiling_changes_the_auto_route_and_the_default_is_unchanged() {
    // EF_SEARCH 1000 makes the walk budget 32,000 evaluations on a 2,000-row
    // graph: at a 1% ceiling AUTO expects the walk to spend 320 of them and
    // walks; at 100% it expects 3,200 and compares every row instead.
    let route = |ceiling: &str| {
        let db = Database::open_memory();
        fixture(&db, &format!("{ceiling} HNSW (EF_SEARCH = 1000)"), 2_000);
        let (result, _) = search(&db, "AUTO");
        routes(&result)
    };
    let exhaustive = vec![(
        VectorSearchRoute::Exact,
        Some(AUTO_STATEMENT_EXHAUSTIVE_REASON.to_owned()),
    )];
    assert_ne!(
        route(" AUTO_EXPANSION_CEILING 1"),
        exhaustive,
        "a 1% ceiling makes the walk the cheaper route"
    );
    assert_eq!(
        route(" AUTO_EXPANSION_CEILING 100"),
        exhaustive,
        "a 100% ceiling makes comparing every row the cheaper route"
    );
    assert_eq!(
        route(" AUTO_EXPANSION_CEILING 25"),
        route(""),
        "declaring the default ceiling routes exactly as omitting it"
    );
}
