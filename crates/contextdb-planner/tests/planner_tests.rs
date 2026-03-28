use contextdb_core::Error;
use contextdb_parser::parse;
use contextdb_planner::{PhysicalPlan, plan};

#[test]
fn match_cte_routes_to_graph_bfs() {
    let stmt = parse(
        "WITH n AS (SELECT b_id FROM GRAPH_TABLE(edges MATCH (a)-[:BASED_ON]->{1,3}(b) COLUMNS(b.id AS b_id))) SELECT * FROM n",
    )
    .unwrap();
    let p = plan(&stmt).unwrap();
    let text = p.explain();
    assert!(text.contains("GraphBfs"));
}

#[test]
fn cosine_order_routes_to_vector_search() {
    let stmt = parse("SELECT * FROM observations ORDER BY embedding <=> $vec LIMIT 10").unwrap();
    let p = plan(&stmt).unwrap();
    let text = p.explain();
    assert!(text.contains("VectorSearch"));
}

#[test]
fn standard_select_routes_to_scan() {
    let stmt = parse("SELECT * FROM entities WHERE id = $id").unwrap();
    let p = plan(&stmt).unwrap();
    assert!(matches!(p, PhysicalPlan::Scan { .. }));
    assert!(p.explain().contains("Scan"));
}

#[test]
fn unused_graph_cte_is_not_included_in_explain() {
    let stmt = parse(
        "WITH n AS (SELECT b_id FROM GRAPH_TABLE(edges MATCH (a)-[:BASED_ON]->{1,3}(b) COLUMNS(b.id AS b_id))) SELECT * FROM observations ORDER BY embedding <=> $vec LIMIT 5",
    )
    .unwrap();
    let p = plan(&stmt).unwrap();
    let e = p.explain();
    assert!(!e.contains("GraphBfs"));
    assert!(e.contains("VectorSearch"));
}

#[test]
fn immutability_checked_at_runtime_not_plan_time() {
    let update = parse("UPDATE observations SET data = 'x'").unwrap();
    assert!(matches!(plan(&update).unwrap(), PhysicalPlan::Update(_)));

    let delete = parse("DELETE FROM observations WHERE id = $id").unwrap();
    assert!(matches!(plan(&delete).unwrap(), PhysicalPlan::Delete(_)));
}

#[test]
fn depth_over_cap_rejected() {
    let parsed = parse(
        "WITH n AS (SELECT b_id FROM GRAPH_TABLE(edges MATCH (a)-[:BASED_ON]->{1,11}(b) COLUMNS(b.id AS b_id))) SELECT * FROM n",
    );

    match parsed {
        Err(Error::BfsDepthExceeded(11)) => {}
        Ok(stmt) => {
            let err = plan(&stmt).unwrap_err();
            assert!(matches!(err, Error::BfsDepthExceeded(11)));
        }
        Err(other) => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn explain_contains_operator_names() {
    let stmt = parse("SELECT * FROM observations ORDER BY embedding <=> $vec LIMIT 10").unwrap();
    let p = plan(&stmt).unwrap();
    let txt = p.explain();
    assert!(txt.contains("VectorSearch"));
}
