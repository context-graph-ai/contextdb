use contextdb_core::{ContextId, Error, Value};
use contextdb_engine::Database;
use std::collections::{BTreeSet, HashMap};
use std::path::Path;
use tempfile::TempDir;
use uuid::Uuid;

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn params(pairs: Vec<(&str, Value)>) -> HashMap<String, Value> {
    pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
}

fn ga_uuid(n: u128) -> Uuid {
    Uuid::from_u128(0xB000_0000_0000_0000_0000_0000_0000_0000 + n)
}

fn create_graph_tables(db: &Database) {
    db.execute(
        "CREATE TABLE nodes (id UUID PRIMARY KEY, label TEXT, context_id UUID CONTEXT_ID)",
        &empty(),
    )
    .unwrap();
    db.execute(
        "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT)",
        &empty(),
    )
    .unwrap();
}

fn insert_node(db: &Database, id: Uuid, label: &str, ctx: Uuid) {
    db.execute(
        "INSERT INTO nodes (id, label, context_id) VALUES ($id, $label, $ctx)",
        &params(vec![
            ("id", Value::Uuid(id)),
            ("label", Value::Text(label.to_string())),
            ("ctx", Value::Uuid(ctx)),
        ]),
    )
    .unwrap();
}

fn insert_edge(db: &Database, source: Uuid, target: Uuid, edge_type: &str) {
    db.execute(
        "INSERT INTO edges (id, source_id, target_id, edge_type) VALUES ($id, $source, $target, $edge_type)",
        &params(vec![
            ("id", Value::Uuid(Uuid::new_v4())),
            ("source", Value::Uuid(source)),
            ("target", Value::Uuid(target)),
            ("edge_type", Value::Text(edge_type.to_string())),
        ]),
    )
    .unwrap();
}

fn uuid_column_set(result: &contextdb_engine::QueryResult, column: &str) -> BTreeSet<Uuid> {
    let idx = result
        .columns
        .iter()
        .position(|c| c == column || c.rsplit('.').next() == Some(column))
        .unwrap_or_else(|| panic!("column {column} not found in {:?}", result.columns));
    result
        .rows
        .iter()
        .map(|row| match &row[idx] {
            Value::Uuid(id) => *id,
            other => panic!("expected UUID in column {column}, got {other:?}"),
        })
        .collect()
}

fn forward_probe(db: &Database, x: Uuid) -> contextdb_engine::QueryResult {
    db.execute(
        "SELECT t FROM GRAPH_TABLE(edges MATCH (a)-[:LINKS]->(b) WHERE a.id = $x COLUMNS (b.id AS t))",
        &params(vec![("x", Value::Uuid(x))]),
    )
    .unwrap()
}

fn seed_reopen_fixture(path: &Path) -> (Uuid, BTreeSet<Uuid>) {
    let db = Database::open(path).unwrap();
    create_graph_tables(&db);
    let ctx = ga_uuid(0xA);
    let x = ga_uuid(2_000);
    let targets: BTreeSet<_> = (0..5).map(|i| ga_uuid(2_010 + i)).collect();
    insert_node(&db, x, "start", ctx);
    for target in &targets {
        insert_node(&db, *target, "target", ctx);
        insert_edge(&db, x, *target, "LINKS");
    }
    for i in 0..2_000 {
        let source = ga_uuid(20_000 + i * 2);
        let target = ga_uuid(20_000 + i * 2 + 1);
        insert_node(&db, source, "noise-source", ctx);
        insert_node(&db, target, "noise-target", ctx);
        insert_edge(&db, source, target, "LINKS");
    }
    db.close().unwrap();
    (x, targets)
}

#[test]
fn ga15_context_scoped_forward_probe_reports_visible_degree_only() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("ga15.redb");
    let ctx_a = ga_uuid(0xA);
    let ctx_b = ga_uuid(0xB);
    let x = ga_uuid(1);
    let hidden_start = ga_uuid(2);
    let hidden_start_visible_target = ga_uuid(3);
    let visible: BTreeSet<_> = (0..4).map(|i| ga_uuid(10 + i)).collect();
    let hidden: BTreeSet<_> = (0..3).map(|i| ga_uuid(20 + i)).collect();

    {
        let admin = Database::open(&path).unwrap();
        create_graph_tables(&admin);
        insert_node(&admin, x, "start-a", ctx_a);
        insert_node(&admin, hidden_start, "start-b", ctx_b);
        insert_node(
            &admin,
            hidden_start_visible_target,
            "visible-target-from-hidden-start",
            ctx_a,
        );
        insert_edge(&admin, hidden_start, hidden_start_visible_target, "LINKS");
        for target in &visible {
            insert_node(&admin, *target, "target-a", ctx_a);
            insert_edge(&admin, x, *target, "LINKS");
        }
        for target in &hidden {
            insert_node(&admin, *target, "target-b", ctx_b);
            insert_edge(&admin, x, *target, "LINKS");
        }
        for i in 0..2_000 {
            let source = ga_uuid(10_000 + i * 2);
            let target = ga_uuid(10_000 + i * 2 + 1);
            insert_node(&admin, source, "noise-a", ctx_a);
            insert_node(&admin, target, "noise-a", ctx_a);
            insert_edge(&admin, source, target, "LINKS");
        }
        admin.close().unwrap();
    }

    let scoped =
        Database::open_with_contexts(&path, BTreeSet::from([ContextId::new(ctx_a)])).unwrap();
    let result = forward_probe(&scoped, x);
    let visible_examined = scoped.__rows_examined();
    let hidden_start_result = scoped.execute(
        "SELECT t FROM GRAPH_TABLE(edges MATCH (a)-[:LINKS]->(b) WHERE a.id = $x COLUMNS (b.id AS t))",
        &params(vec![("x", Value::Uuid(hidden_start))]),
    );

    let actual = uuid_column_set(&result, "t");
    assert_eq!(actual, visible);
    for leaked in hidden {
        assert!(
            !actual.contains(&leaked),
            "ctx-b target {leaked} leaked into ctx-a-scoped graph probe"
        );
    }
    assert_eq!(result.trace.physical_plan, "AdjacencyProbe");
    assert_eq!(result.trace.index_used.as_deref(), Some("forward_adj"));
    assert_eq!(visible_examined, 4);
    match hidden_start_result {
        Err(Error::ContextScopeViolation { requested, allowed }) => {
            assert_eq!(requested, ContextId::new(ctx_b));
            assert_eq!(allowed, BTreeSet::from([ContextId::new(ctx_a)]));
        }
        Ok(_) => panic!("expected hidden pinned start to be denied"),
        Err(other) => {
            panic!("expected ContextScopeViolation for hidden pinned start, got {other:?}")
        }
    }
}

#[test]
fn ga20_file_backed_reopen_preserves_forward_probe_trace_and_accounting() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("ga20.redb");
    let (x, targets) = seed_reopen_fixture(&path);

    let first = Database::open(&path).unwrap();
    let before = forward_probe(&first, x);
    let before_examined = first.__rows_examined();
    first.close().unwrap();

    let reopened = Database::open(&path).unwrap();
    let after = forward_probe(&reopened, x);
    let after_examined = reopened.__rows_examined();

    assert_eq!(uuid_column_set(&before, "t"), targets);
    assert_eq!(uuid_column_set(&after, "t"), targets);
    assert_eq!(before.trace.physical_plan, "AdjacencyProbe");
    assert_eq!(after.trace.physical_plan, "AdjacencyProbe");
    assert_eq!(before.trace.index_used.as_deref(), Some("forward_adj"));
    assert_eq!(after.trace.index_used.as_deref(), Some("forward_adj"));
    assert_eq!(before_examined, 5);
    assert_eq!(after_examined, 5);
}
