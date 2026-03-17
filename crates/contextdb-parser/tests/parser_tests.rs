use contextdb_core::Error;
use contextdb_parser::ast::{
    AstPropagationRule, BinOp, Cte, EdgeDirection, Expr, ForeignKey, FromItem, Literal, Statement,
};
use contextdb_parser::parse;

#[test]
fn parse_valid_sql_subset() {
    assert!(matches!(
        parse("SELECT * FROM entities WHERE id = $id"),
        Ok(Statement::Select(_))
    ));
    assert!(matches!(
        parse("INSERT INTO entities (id, name) VALUES ($id, $name)"),
        Ok(Statement::Insert(_))
    ));
    assert!(matches!(
        parse(
            "INSERT INTO entities (id, name) VALUES ($id, $name) ON CONFLICT (id) DO UPDATE SET name=$name"
        ),
        Ok(Statement::Insert(_))
    ));
    assert!(matches!(
        parse("DELETE FROM edges WHERE id = $id"),
        Ok(Statement::Delete(_))
    ));
    assert!(matches!(
        parse("CREATE TABLE foo (id UUID PRIMARY KEY, name TEXT NOT NULL)"),
        Ok(Statement::CreateTable(_))
    ));
    assert!(matches!(
        parse("DROP TABLE foo"),
        Ok(Statement::DropTable(_))
    ));
    assert!(matches!(
        parse("SELECT * FROM observations ORDER BY embedding <=> $vec LIMIT 10"),
        Ok(Statement::Select(_))
    ));
    let (gn, mc, cols) = expect_graph_table(parse(
        "SELECT b_id FROM GRAPH_TABLE (edges MATCH (a:Entity)-[:BASED_ON]->{1,3}(b) WHERE a.id = $id COLUMNS (b.id AS b_id))",
    ));
    assert_eq!(gn, "edges");
    // CRITICAL-1: graph_name cross-check
    assert_eq!(mc.graph_name, Some("edges".to_string()));
    assert_eq!(mc.pattern.start.label.as_deref(), Some("Entity"));
    assert_eq!(mc.pattern.edges[0].min_hops, 1);
    assert_eq!(mc.pattern.edges[0].max_hops, 3);
    assert!(mc.where_clause.is_some());
    assert_eq!(cols.len(), 1);
    // CRITICAL-2: return_cols mirrors columns
    assert_eq!(mc.return_cols.len(), cols.len());
    assert_eq!(mc.return_cols[0].alias.as_deref(), Some(&cols[0].alias[..]));
}

#[test]
fn anti_tests_rejected_constructs() {
    assert!(matches!(
        parse("WITH RECURSIVE t AS (SELECT 1) SELECT * FROM t"),
        Err(Error::RecursiveCteNotSupported)
    ));
    assert!(matches!(
        parse("SELECT ROW_NUMBER() OVER (PARTITION BY x) FROM t"),
        Err(Error::WindowFunctionNotSupported)
    ));
    assert!(matches!(
        parse("CREATE PROCEDURE p AS SELECT 1"),
        Err(Error::StoredProcNotSupported)
    ));
    assert!(matches!(
        parse(
            "SELECT b_id FROM GRAPH_TABLE (edges MATCH (a)-[:EDGE]->+(b) COLUMNS (b.id AS b_id))"
        ),
        Err(Error::UnboundedTraversal)
    ));
    assert!(matches!(
        parse("SELECT * FROM obs ORDER BY embedding <=> $q"),
        Err(Error::UnboundedVectorSearch)
    ));
    assert!(matches!(
        parse("SELECT * FROM d WHERE id IN (SELECT id FROM edges WHERE x = 1)"),
        Err(Error::SubqueryNotSupported)
    ));
}

#[test]
fn cte_ref_subquery_is_allowed() {
    assert!(matches!(
        parse(
            "WITH my_cte AS (SELECT id FROM decisions) SELECT * FROM d WHERE id IN (SELECT id FROM my_cte)"
        ),
        Ok(Statement::Select(_))
    ));
}

#[test]
fn parse_create_table_constraints() {
    assert!(matches!(
        parse("CREATE TABLE obs (id UUID PRIMARY KEY) IMMUTABLE"),
        Ok(Statement::CreateTable(_))
    ));
    assert!(matches!(
        parse(
            "CREATE TABLE inv (id UUID PRIMARY KEY, status TEXT) STATE MACHINE (status: pending -> [acknowledged], pending -> [dismissed])"
        ),
        Ok(Statement::CreateTable(_))
    ));
    assert!(matches!(
        parse(
            "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT) DAG('CITES', 'BASED_ON')"
        ),
        Ok(Statement::CreateTable(_))
    ));
    assert!(matches!(
        parse("CREATE TABLE bad (id UUID PRIMARY KEY) IMMUTABLE STATE MACHINE (status: a -> [b])"),
        Err(Error::ParseError(_))
    ));
    assert!(matches!(
        parse("CREATE TABLE bad2 (id UUID PRIMARY KEY) IMMUTABLE DAG('CITES')"),
        Err(Error::ParseError(_))
    ));
    assert!(matches!(
        parse(
            "CREATE TABLE bad3 (id UUID PRIMARY KEY) STATE MACHINE (status: a -> [b]) DAG('CITES')"
        ),
        Err(Error::ParseError(_))
    ));
}

#[test]
fn sql_edge_case_keywords_inside_string_literals() {
    assert!(matches!(
        parse("INSERT INTO entities (id, name) VALUES ($id, 'SELECT FROM table WHERE x = 1')"),
        Ok(Statement::Insert(_))
    ));
    assert!(matches!(
        parse("INSERT INTO entities (id, name) VALUES ($id, 'MATCH (a)-[:EDGE]->(b)')"),
        Ok(Statement::Insert(_))
    ));
    assert!(matches!(
        parse("INSERT INTO entities (id, name) VALUES ($id, 'it''s a WHERE AND OR test')"),
        Ok(Statement::Insert(_))
    ));
}

#[test]
fn sql_edge_case_nested_boolean_expressions_with_parentheses() {
    assert!(matches!(
        parse(
            "SELECT * FROM decisions WHERE (status = 'active' AND confidence > 0.8) OR status = 'confirmed'"
        ),
        Ok(Statement::Select(_))
    ));
    assert!(matches!(
        parse("SELECT * FROM entities WHERE ((a = 1 AND b = 2) OR (c = 3 AND d = 4)) AND e = 5"),
        Ok(Statement::Select(_))
    ));
}

#[test]
fn sql_edge_case_quoted_identifiers() {
    assert!(matches!(
        parse("SELECT \"column name\" FROM \"table name\" WHERE \"odd column\" = $val"),
        Ok(Statement::Select(_))
    ));
    assert!(matches!(
        parse("SELECT \"SELECT\" FROM \"FROM\" WHERE \"WHERE\" = 'AND'"),
        Ok(Statement::Select(_))
    ));
}

#[test]
fn sql_edge_case_multi_cte_queries() {
    assert!(matches!(
        parse(
            "WITH active AS (SELECT id FROM decisions WHERE status = 'active'), recent AS (SELECT id FROM observations WHERE created_at > $since) SELECT * FROM active"
        ),
        Ok(Statement::Select(_))
    ));
    {
        let stmt = parse(
            "WITH graph_results AS (SELECT b_id FROM GRAPH_TABLE (edges MATCH (a)-[:BASED_ON]->{1,3}(b) COLUMNS (b.id AS b_id))), filtered AS (SELECT id FROM decisions WHERE status = 'active') SELECT * FROM graph_results WHERE b_id IN (SELECT id FROM filtered)",
        ).expect("parse should succeed");
        let Statement::Select(sel) = stmt else {
            panic!("expected Select");
        };
        assert_eq!(sel.ctes.len(), 2);
        // First CTE should contain a GRAPH_TABLE FROM item
        let Cte::SqlCte { name, query } = &sel.ctes[0] else {
            panic!("expected SqlCte");
        };
        assert_eq!(name, "graph_results");
        let gt = query
            .from
            .iter()
            .find(|f| matches!(f, FromItem::GraphTable { .. }));
        assert!(
            gt.is_some(),
            "expected FromItem::GraphTable in CTE body, got {:?}",
            query.from
        );
    }
}

#[test]
fn sql_edge_case_parameter_binding() {
    assert!(matches!(
        parse("SELECT * FROM entities WHERE name = $name AND id = $entity_id"),
        Ok(Statement::Select(_))
    ));
    assert!(matches!(
        parse(
            "INSERT INTO edges (id, source_id, target_id, edge_type) VALUES ($id, $source_id, $target_id, $edge_type)"
        ),
        Ok(Statement::Insert(_))
    ));
}

#[test]
fn sql_edge_case_null_handling_variations() {
    assert!(matches!(
        parse("SELECT * FROM entities WHERE name IS NULL"),
        Ok(Statement::Select(_))
    ));
    assert!(matches!(
        parse("SELECT * FROM entities WHERE name IS NOT NULL AND status = 'active'"),
        Ok(Statement::Select(_))
    ));
    assert!(matches!(
        parse("INSERT INTO entities (id, name) VALUES ($id, NULL)"),
        Ok(Statement::Insert(_))
    ));
}

/// Extract the first `FromItem::GraphTable` from a parsed SELECT, panicking with
/// diagnostics if the statement is not a SELECT or has no GraphTable in FROM.
fn expect_graph_table(
    stmt: Result<Statement, contextdb_core::Error>,
) -> (
    String,
    contextdb_parser::ast::MatchClause,
    Vec<contextdb_parser::ast::GraphTableColumn>,
) {
    let stmt = stmt.expect("parse should succeed");
    let Statement::Select(sel) = stmt else {
        panic!("expected Statement::Select, got {:?}", stmt);
    };
    for item in &sel.body.from {
        if let FromItem::GraphTable {
            graph_name,
            match_clause,
            columns,
        } = item
        {
            return (graph_name.clone(), match_clause.clone(), columns.clone());
        }
    }
    panic!(
        "expected FromItem::GraphTable in FROM clause, got {:?}",
        sel.body.from
    );
}

#[test]
fn sql_pgq_basic_graph_table_single_edge_step() {
    let (graph_name, mc, cols) = expect_graph_table(parse(
        "SELECT b_id FROM GRAPH_TABLE (edges MATCH (a)-[:BASED_ON]->(b) COLUMNS (b.id AS b_id))",
    ));
    assert_eq!(graph_name, "edges");
    // CRITICAL-1: graph_name cross-check
    assert_eq!(mc.graph_name, Some("edges".to_string()));
    assert_eq!(mc.pattern.start.alias, "a");
    assert_eq!(mc.pattern.edges.len(), 1);
    let e = &mc.pattern.edges[0];
    assert_eq!(e.edge_type.as_deref(), Some("BASED_ON"));
    assert_eq!(e.direction, EdgeDirection::Outgoing);
    assert_eq!(e.min_hops, 1);
    assert_eq!(e.max_hops, 1);
    assert_eq!(e.target.alias, "b");
    // MEDIUM-5: edge alias None when absent
    assert!(
        e.alias.is_none(),
        "edge without alias syntax should have alias=None"
    );
    assert_eq!(cols.len(), 1);
    assert_eq!(cols[0].alias, "b_id");
    // CRITICAL-2: return_cols mirrors columns
    assert_eq!(mc.return_cols.len(), cols.len());
    assert_eq!(mc.return_cols[0].alias.as_deref(), Some(&cols[0].alias[..]));
    // LOW-1: node properties empty when absent
    assert!(mc.pattern.start.properties.is_empty());
    assert!(mc.pattern.edges[0].target.properties.is_empty());
}

#[test]
fn sql_pgq_multi_step_patterns() {
    // Two-step pattern: (a)-[:SERVES]->(b)-[:BASED_ON]->(c)
    // Chain connectivity is implicit via sequential edge ordering:
    // start("a") -> edges[0].target("b") -> edges[1].target("c")
    // Each edge's source is the previous edge's target (or start for edges[0]).
    // EdgeStep has no `source` field; adjacency is positional.
    let (gn, mc, cols) = expect_graph_table(parse(
        "SELECT c_id FROM GRAPH_TABLE (edges MATCH (a)-[:SERVES]->(b)-[:BASED_ON]->(c) COLUMNS (c.id AS c_id))",
    ));
    assert_eq!(gn, "edges");
    // CRITICAL-1: graph_name cross-check
    assert_eq!(mc.graph_name, Some("edges".to_string()));
    assert_eq!(mc.pattern.start.alias, "a");
    assert_eq!(mc.pattern.edges.len(), 2);
    assert_eq!(mc.pattern.edges[0].edge_type.as_deref(), Some("SERVES"));
    // HIGH-4: direction assertions for ALL edges
    assert_eq!(mc.pattern.edges[0].direction, EdgeDirection::Outgoing);
    // b is target of step 0 and implicit source of step 1
    assert_eq!(mc.pattern.edges[0].target.alias, "b");
    assert_eq!(mc.pattern.edges[1].edge_type.as_deref(), Some("BASED_ON"));
    assert_eq!(mc.pattern.edges[1].direction, EdgeDirection::Outgoing);
    assert_eq!(mc.pattern.edges[1].target.alias, "c");
    assert_eq!(cols.len(), 1);
    assert_eq!(cols[0].alias, "c_id");
    // CRITICAL-2: return_cols mirrors columns
    assert_eq!(mc.return_cols.len(), cols.len());
    assert_eq!(mc.return_cols[0].alias.as_deref(), Some(&cols[0].alias[..]));
    // MEDIUM-4: node labels None when absent
    assert!(mc.pattern.start.label.is_none());
    assert!(mc.pattern.edges[0].target.label.is_none());
    assert!(mc.pattern.edges[1].target.label.is_none());
    // MEDIUM-5: edge aliases None when absent
    assert!(mc.pattern.edges[0].alias.is_none());
    assert!(mc.pattern.edges[1].alias.is_none());
    // LOW-1: node properties empty when absent
    assert!(mc.pattern.start.properties.is_empty());
    assert!(mc.pattern.edges[0].target.properties.is_empty());
    assert!(mc.pattern.edges[1].target.properties.is_empty());

    // Three-step pattern
    let (gn, mc, cols) = expect_graph_table(parse(
        "SELECT d_desc FROM GRAPH_TABLE (edges MATCH (a)-[:CITES]->(b)-[:BASED_ON]->(c)-[:SERVES]->(d) COLUMNS (d.description AS d_desc))",
    ));
    assert_eq!(gn, "edges");
    // CRITICAL-1: graph_name cross-check
    assert_eq!(mc.graph_name, Some("edges".to_string()));
    assert_eq!(mc.pattern.edges.len(), 3);
    assert_eq!(mc.pattern.edges[0].edge_type.as_deref(), Some("CITES"));
    assert_eq!(mc.pattern.edges[0].target.alias, "b");
    assert_eq!(mc.pattern.edges[1].edge_type.as_deref(), Some("BASED_ON"));
    assert_eq!(mc.pattern.edges[1].target.alias, "c");
    assert_eq!(mc.pattern.edges[2].edge_type.as_deref(), Some("SERVES"));
    assert_eq!(mc.pattern.edges[2].target.alias, "d");
    // HIGH-4: direction assertions for ALL edges in 3-step
    assert_eq!(mc.pattern.edges[0].direction, EdgeDirection::Outgoing);
    assert_eq!(mc.pattern.edges[1].direction, EdgeDirection::Outgoing);
    assert_eq!(mc.pattern.edges[2].direction, EdgeDirection::Outgoing);
    assert_eq!(cols.len(), 1);
    assert_eq!(cols[0].alias, "d_desc");
    // CRITICAL-2: return_cols mirrors columns
    assert_eq!(mc.return_cols.len(), cols.len());
    assert_eq!(mc.return_cols[0].alias.as_deref(), Some(&cols[0].alias[..]));
}

#[test]
fn sql_pgq_meaningful_node_aliases() {
    let (gn, mc, cols) = expect_graph_table(parse(
        "SELECT decision_desc FROM GRAPH_TABLE (edges MATCH (entity)-[:BASED_ON]->(decision) COLUMNS (decision.description AS decision_desc))",
    ));
    assert_eq!(gn, "edges");
    // CRITICAL-1: graph_name cross-check
    assert_eq!(mc.graph_name, Some("edges".to_string()));
    assert_eq!(mc.pattern.start.alias, "entity");
    assert_eq!(mc.pattern.edges.len(), 1);
    assert_eq!(mc.pattern.edges[0].edge_type.as_deref(), Some("BASED_ON"));
    assert_eq!(mc.pattern.edges[0].target.alias, "decision");
    // MEDIUM-5: edge alias None when absent
    assert!(mc.pattern.edges[0].alias.is_none());
    assert_eq!(cols.len(), 1);
    assert_eq!(cols[0].alias, "decision_desc");
    // CRITICAL-2: return_cols mirrors columns
    assert_eq!(mc.return_cols.len(), cols.len());
    assert_eq!(mc.return_cols[0].alias.as_deref(), Some(&cols[0].alias[..]));
}

#[test]
fn sql_pgq_edge_type_filtering() {
    // Specific edge type
    let (gn, mc, _) = expect_graph_table(parse(
        "SELECT b_id FROM GRAPH_TABLE (edges MATCH (a)-[:CITES]->(b) COLUMNS (b.id AS b_id))",
    ));
    // CRITICAL-1: graph_name cross-check
    assert_eq!(mc.graph_name, Some(gn.clone()));
    assert_eq!(mc.pattern.edges[0].edge_type.as_deref(), Some("CITES"));
    // MEDIUM-5: edge alias None when absent
    assert!(mc.pattern.edges[0].alias.is_none());

    // No edge type (empty brackets)
    let (gn, mc, _) = expect_graph_table(parse(
        "SELECT b_id FROM GRAPH_TABLE (edges MATCH (a)-[]->(b) COLUMNS (b.id AS b_id))",
    ));
    // CRITICAL-1: graph_name cross-check
    assert_eq!(mc.graph_name, Some(gn.clone()));
    assert_eq!(mc.pattern.edges[0].edge_type, None);
    // MEDIUM-5: edge alias None when absent
    assert!(mc.pattern.edges[0].alias.is_none());
}

#[test]
fn sql_pgq_quantified_paths_depth_bounds() {
    // {1,5}
    let (gn, mc, _) = expect_graph_table(parse(
        "SELECT b_id FROM GRAPH_TABLE (edges MATCH (a)-[:BASED_ON]->{1,5}(b) COLUMNS (b.id AS b_id))",
    ));
    // CRITICAL-1: graph_name cross-check
    assert_eq!(mc.graph_name, Some(gn.clone()));
    assert_eq!(mc.pattern.edges[0].edge_type.as_deref(), Some("BASED_ON"));
    assert_eq!(mc.pattern.edges[0].min_hops, 1);
    assert_eq!(mc.pattern.edges[0].max_hops, 5);
    // MEDIUM-5: edge alias None when absent
    assert!(mc.pattern.edges[0].alias.is_none());

    // {2,10}
    let (gn, mc, _) = expect_graph_table(parse(
        "SELECT b_id FROM GRAPH_TABLE (edges MATCH (a)-[:CITES]->{2,10}(b) COLUMNS (b.id AS b_id))",
    ));
    // CRITICAL-1: graph_name cross-check
    assert_eq!(mc.graph_name, Some(gn.clone()));
    assert_eq!(mc.pattern.edges[0].edge_type.as_deref(), Some("CITES"));
    assert_eq!(mc.pattern.edges[0].min_hops, 2);
    assert_eq!(mc.pattern.edges[0].max_hops, 10);

    // {1,1} - single hop
    let (_, mc, _) = expect_graph_table(parse(
        "SELECT b_id FROM GRAPH_TABLE (edges MATCH (a)-[:CITES]->{1,1}(b) COLUMNS (b.id AS b_id))",
    ));
    assert_eq!(mc.pattern.edges[0].min_hops, 1);
    assert_eq!(mc.pattern.edges[0].max_hops, 1);
}

#[test]
fn sql_pgq_where_inside_match() {
    // Simple WHERE inside MATCH
    let (gn, mc, _) = expect_graph_table(parse(
        "SELECT b_id FROM GRAPH_TABLE (edges MATCH (a)-[:CITES]->(b) WHERE b.status = 'active' COLUMNS (b.id AS b_id))",
    ));
    // CRITICAL-1: graph_name cross-check
    assert_eq!(mc.graph_name, Some(gn.clone()));
    assert_eq!(mc.pattern.edges[0].edge_type.as_deref(), Some("CITES"));
    assert!(mc.where_clause.is_some(), "expected WHERE clause in MATCH");
    // MEDIUM-1: verify WHERE is a real expression, not a dummy literal
    let wc = mc.where_clause.as_ref().unwrap();
    assert!(
        matches!(wc, Expr::BinaryOp { .. }),
        "expected BinaryOp WHERE clause, got {:?}",
        wc
    );

    // Compound WHERE inside MATCH with quantified path
    let (gn, mc, _) = expect_graph_table(parse(
        "SELECT b_id FROM GRAPH_TABLE (edges MATCH (a)-[:BASED_ON]->{1,3}(b) WHERE b.confidence > 0.5 AND a.status = 'active' COLUMNS (b.id AS b_id))",
    ));
    // CRITICAL-1: graph_name cross-check
    assert_eq!(mc.graph_name, Some(gn.clone()));
    assert_eq!(mc.pattern.edges[0].min_hops, 1);
    assert_eq!(mc.pattern.edges[0].max_hops, 3);
    assert!(mc.where_clause.is_some(), "expected WHERE clause in MATCH");
    // MEDIUM-1: verify compound WHERE is a real expression
    let wc = mc.where_clause.as_ref().unwrap();
    assert!(
        matches!(wc, Expr::BinaryOp { .. }),
        "expected BinaryOp WHERE clause, got {:?}",
        wc
    );
}

#[test]
fn sql_pgq_bidirectional_edges() {
    // Incoming edge
    let (gn, mc, cols) = expect_graph_table(parse(
        "SELECT a_id FROM GRAPH_TABLE (edges MATCH (a)<-[:BASED_ON]-(b) COLUMNS (a.id AS a_id))",
    ));
    // CRITICAL-1: graph_name cross-check
    assert_eq!(mc.graph_name, Some(gn.clone()));
    assert_eq!(mc.pattern.edges[0].direction, EdgeDirection::Incoming);
    assert_eq!(mc.pattern.edges[0].edge_type.as_deref(), Some("BASED_ON"));
    // MEDIUM-5: edge alias None when absent
    assert!(mc.pattern.edges[0].alias.is_none());
    assert_eq!(cols[0].alias, "a_id");

    // Undirected (both) edge
    let (gn, mc, cols) = expect_graph_table(parse(
        "SELECT b_id FROM GRAPH_TABLE (edges MATCH (a)-[:RELATES_TO]-(b) COLUMNS (b.id AS b_id))",
    ));
    // CRITICAL-1: graph_name cross-check
    assert_eq!(mc.graph_name, Some(gn.clone()));
    assert_eq!(mc.pattern.edges[0].direction, EdgeDirection::Both);
    assert_eq!(mc.pattern.edges[0].edge_type.as_deref(), Some("RELATES_TO"));
    // MEDIUM-5: edge alias None when absent
    assert!(mc.pattern.edges[0].alias.is_none());
    assert_eq!(cols[0].alias, "b_id");
}

#[test]
fn sql_pgq_columns_clause_variations() {
    // Multiple columns with edge alias
    let (gn, mc, cols) = expect_graph_table(parse(
        "SELECT src, tgt, etype FROM GRAPH_TABLE (edges MATCH (a)-[e:CITES]->(b) COLUMNS (a.id AS src, b.id AS tgt, e.edge_type AS etype))",
    ));
    // CRITICAL-1: graph_name cross-check
    assert_eq!(mc.graph_name, Some(gn.clone()));
    assert_eq!(mc.pattern.edges[0].alias.as_deref(), Some("e"));
    assert_eq!(mc.pattern.edges[0].edge_type.as_deref(), Some("CITES"));
    assert_eq!(cols.len(), 3);
    assert_eq!(cols[0].alias, "src");
    assert_eq!(cols[1].alias, "tgt");
    assert_eq!(cols[2].alias, "etype");
    // CRITICAL-2: return_cols mirrors columns
    assert_eq!(mc.return_cols.len(), cols.len());
    assert_eq!(mc.return_cols[0].alias.as_deref(), Some(&cols[0].alias[..]));
    assert_eq!(mc.return_cols[1].alias.as_deref(), Some(&cols[1].alias[..]));
    assert_eq!(mc.return_cols[2].alias.as_deref(), Some(&cols[2].alias[..]));

    // Two columns with quantified path
    let (gn, mc, cols) = expect_graph_table(parse(
        "SELECT b_id, b_name FROM GRAPH_TABLE (edges MATCH (a)-[:BASED_ON]->{1,2}(b) COLUMNS (b.id AS b_id, b.name AS b_name))",
    ));
    // CRITICAL-1: graph_name cross-check
    assert_eq!(mc.graph_name, Some(gn.clone()));
    assert_eq!(mc.pattern.edges[0].min_hops, 1);
    assert_eq!(mc.pattern.edges[0].max_hops, 2);
    assert_eq!(cols.len(), 2);
    assert_eq!(cols[0].alias, "b_id");
    assert_eq!(cols[1].alias, "b_name");
    // CRITICAL-2: return_cols mirrors columns
    assert_eq!(mc.return_cols.len(), cols.len());
    assert_eq!(mc.return_cols[0].alias.as_deref(), Some(&cols[0].alias[..]));
    assert_eq!(mc.return_cols[1].alias.as_deref(), Some(&cols[1].alias[..]));
}

#[test]
fn sql_pgq_graph_table_as_cte_source() {
    let stmt = parse(
        "WITH reachable AS (SELECT b_id FROM GRAPH_TABLE (edges MATCH (a)-[:BASED_ON]->{1,3}(b) COLUMNS (b.id AS b_id))) SELECT * FROM reachable WHERE b_id = $target",
    ).expect("parse should succeed");
    let Statement::Select(sel) = stmt else {
        panic!("expected Select");
    };
    // The CTE's inner SELECT should contain a GraphTable FROM item
    assert_eq!(sel.ctes.len(), 1);
    let Cte::SqlCte { name, query } = &sel.ctes[0] else {
        panic!("expected SqlCte, got {:?}", sel.ctes[0]);
    };
    assert_eq!(name, "reachable");
    let gt = query
        .from
        .iter()
        .find(|f| matches!(f, FromItem::GraphTable { .. }));
    assert!(
        gt.is_some(),
        "expected FromItem::GraphTable in CTE body, got {:?}",
        query.from
    );
    if let Some(FromItem::GraphTable {
        graph_name,
        match_clause,
        columns,
    }) = gt
    {
        assert_eq!(graph_name, "edges");
        assert_eq!(
            match_clause.pattern.edges[0].edge_type.as_deref(),
            Some("BASED_ON")
        );
        assert_eq!(match_clause.pattern.edges[0].min_hops, 1);
        assert_eq!(match_clause.pattern.edges[0].max_hops, 3);
        assert_eq!(columns.len(), 1);
        assert_eq!(columns[0].alias, "b_id");
    }
}

#[test]
fn sql_pgq_node_labels() {
    let (gn, mc, _) = expect_graph_table(parse(
        "SELECT b_id FROM GRAPH_TABLE (edges MATCH (a:Entity)-[:BASED_ON]->(b:Decision) COLUMNS (b.id AS b_id))",
    ));
    // CRITICAL-1: graph_name cross-check
    assert_eq!(mc.graph_name, Some(gn.clone()));
    assert_eq!(mc.pattern.start.alias, "a");
    assert_eq!(mc.pattern.start.label.as_deref(), Some("Entity"));
    assert_eq!(mc.pattern.edges[0].target.alias, "b");
    assert_eq!(
        mc.pattern.edges[0].target.label.as_deref(),
        Some("Decision")
    );
}

#[test]
fn ddl_references_table_column() {
    let stmt = parse(
        "CREATE TABLE decisions (id UUID PRIMARY KEY, status TEXT, intention_id UUID REFERENCES intentions(id)) STATE MACHINE (status: active -> [invalidated, superseded])",
    );
    assert!(matches!(stmt, Ok(Statement::CreateTable(_))));

    if let Ok(Statement::CreateTable(ct)) = stmt {
        let fk = ct
            .columns
            .iter()
            .find(|c| c.name == "intention_id")
            .and_then(|c| c.references.clone());
        assert!(matches!(
            fk,
            Some(ForeignKey { table, column, .. }) if table == "intentions" && column == "id"
        ));
    }

    assert!(matches!(
        parse(
            "CREATE TABLE outcomes (id UUID PRIMARY KEY, decision_id UUID REFERENCES decisions(id), success BOOLEAN)"
        ),
        Ok(Statement::CreateTable(_))
    ));
}

#[test]
fn ddl_state_propagation_fk_rules_parse() {
    let stmt = parse(
        "CREATE TABLE decisions (id UUID PRIMARY KEY, status TEXT, intention_id UUID REFERENCES intentions(id) ON STATE archived PROPAGATE SET invalidated MAX DEPTH 3 ABORT ON FAILURE ON STATE superseded PROPAGATE SET invalidated) STATE MACHINE (status: active -> [invalidated, superseded])",
    );
    assert!(matches!(stmt, Ok(Statement::CreateTable(_))));
    let Statement::CreateTable(ct) = stmt.expect("create table") else {
        unreachable!();
    };
    let fk_rules = &ct
        .columns
        .iter()
        .find(|c| c.name == "intention_id")
        .expect("intention_id column")
        .references
        .as_ref()
        .expect("fk references")
        .propagation_rules;
    assert_eq!(fk_rules.len(), 2);
    assert!(matches!(
        fk_rules[0],
        AstPropagationRule::FkState {
            ref trigger_state,
            ref target_state,
            max_depth: Some(3),
            abort_on_failure: true
        } if trigger_state == "archived" && target_state == "invalidated"
    ));
    assert!(matches!(
        fk_rules[1],
        AstPropagationRule::FkState {
            ref trigger_state,
            ref target_state,
            max_depth: None,
            abort_on_failure: false
        } if trigger_state == "superseded" && target_state == "invalidated"
    ));
}

#[test]
fn ddl_state_propagation_edge_rules_parse() {
    let stmt = parse(
        "CREATE TABLE decisions (id UUID PRIMARY KEY, status TEXT) STATE MACHINE (status: active -> [invalidated, superseded]) PROPAGATE ON EDGE CITES INCOMING STATE invalidated SET invalidated MAX DEPTH 5 ABORT ON FAILURE PROPAGATE ON EDGE SERVES BOTH STATE invalidated SET flagged",
    );
    assert!(matches!(stmt, Ok(Statement::CreateTable(_))));
    let Statement::CreateTable(ct) = stmt.expect("create table") else {
        unreachable!();
    };
    assert_eq!(ct.propagation_rules.len(), 2);
    assert!(matches!(
        ct.propagation_rules[0],
        AstPropagationRule::EdgeState {
            ref edge_type,
            ref direction,
            ref trigger_state,
            ref target_state,
            max_depth: Some(5),
            abort_on_failure: true
        } if edge_type == "CITES"
            && direction == "INCOMING"
            && trigger_state == "invalidated"
            && target_state == "invalidated"
    ));
    assert!(matches!(
        ct.propagation_rules[1],
        AstPropagationRule::EdgeState {
            ref edge_type,
            ref direction,
            ref trigger_state,
            ref target_state,
            max_depth: None,
            abort_on_failure: false
        } if edge_type == "SERVES"
            && direction == "BOTH"
            && trigger_state == "invalidated"
            && target_state == "flagged"
    ));
}

#[test]
fn ddl_state_propagation_vector_exclusions_parse() {
    let stmt = parse(
        "CREATE TABLE decisions (id UUID PRIMARY KEY, status TEXT, embedding VECTOR(384)) STATE MACHINE (status: active -> [invalidated, superseded]) PROPAGATE ON STATE invalidated EXCLUDE VECTOR PROPAGATE ON STATE superseded EXCLUDE VECTOR",
    );
    assert!(matches!(stmt, Ok(Statement::CreateTable(_))));
    let Statement::CreateTable(ct) = stmt.expect("create table") else {
        unreachable!();
    };
    assert_eq!(ct.propagation_rules.len(), 2);
    assert!(matches!(
        ct.propagation_rules[0],
        AstPropagationRule::VectorExclusion { ref trigger_state }
            if trigger_state == "invalidated"
    ));
    assert!(matches!(
        ct.propagation_rules[1],
        AstPropagationRule::VectorExclusion { ref trigger_state }
            if trigger_state == "superseded"
    ));
}

#[test]
fn ddl_state_propagation_max_depth_parse() {
    assert!(matches!(
        parse(
            "CREATE TABLE decisions (id UUID PRIMARY KEY, status TEXT) STATE MACHINE (status: active -> [invalidated, superseded]) PROPAGATE ON EDGE CITES INCOMING STATE invalidated SET invalidated MAX DEPTH 5"
        ),
        Ok(Statement::CreateTable(_))
    ));
}

#[test]
fn ddl_combined_state_machine_and_propagation_parse() {
    assert!(matches!(
        parse(
            "CREATE TABLE decisions (id UUID PRIMARY KEY, description TEXT, status TEXT, confidence REAL, reasoning TEXT, agent_id TEXT, embedding VECTOR(384), intention_id UUID REFERENCES intentions(id) ON STATE archived PROPAGATE SET invalidated) STATE MACHINE (status: active -> [invalidated, superseded]) PROPAGATE ON EDGE CITES INCOMING STATE invalidated SET invalidated PROPAGATE ON STATE invalidated EXCLUDE VECTOR PROPAGATE ON STATE superseded EXCLUDE VECTOR"
        ),
        Ok(Statement::CreateTable(_))
    ));
}

#[test]
fn ddl_mutual_exclusion_enforcement() {
    assert!(matches!(
        parse("CREATE TABLE bad (id UUID PRIMARY KEY) IMMUTABLE DAG('CITES')"),
        Err(Error::ParseError(_))
    ));
    assert!(matches!(
        parse("CREATE TABLE bad (id UUID PRIMARY KEY) IMMUTABLE STATE MACHINE (status: a -> [b])"),
        Err(Error::ParseError(_))
    ));
    assert!(matches!(
        parse(
            "CREATE TABLE bad (id UUID PRIMARY KEY) STATE MACHINE (status: a -> [b]) DAG('CITES')"
        ),
        Err(Error::ParseError(_))
    ));
    assert!(matches!(
        parse(
            "CREATE TABLE bad (id UUID PRIMARY KEY) IMMUTABLE PROPAGATE ON EDGE CITES INCOMING STATE x SET y"
        ),
        Err(Error::ParseError(_))
    ));
    assert!(matches!(
        parse(
            "CREATE TABLE bad (id UUID PRIMARY KEY) DAG('CITES') PROPAGATE ON EDGE CITES INCOMING STATE x SET y"
        ),
        Err(Error::ParseError(_))
    ));
}

#[test]
fn rejection_recursive_cte() {
    assert!(matches!(
        parse("WITH RECURSIVE t AS (SELECT 1) SELECT * FROM t"),
        Err(Error::RecursiveCteNotSupported)
    ));
    assert!(matches!(
        parse(
            "WITH RECURSIVE tree(id, parent) AS (SELECT id, parent_id FROM nodes WHERE id = $root UNION ALL SELECT n.id, n.parent_id FROM nodes n JOIN tree t ON n.parent_id = t.id) SELECT * FROM tree"
        ),
        Err(Error::RecursiveCteNotSupported)
    ));
}

#[test]
fn rejection_window_functions() {
    assert!(matches!(
        parse("SELECT ROW_NUMBER() OVER (PARTITION BY x) FROM t"),
        Err(Error::WindowFunctionNotSupported)
    ));
    assert!(matches!(
        parse("SELECT id, status, RANK() OVER (ORDER BY confidence DESC) FROM decisions"),
        Err(Error::WindowFunctionNotSupported)
    ));
    assert!(matches!(
        parse(
            "SELECT id, SUM(count) OVER (PARTITION BY entity_id ORDER BY created_at) FROM outcomes"
        ),
        Err(Error::WindowFunctionNotSupported)
    ));
}

#[test]
fn rejection_stored_procs() {
    assert!(matches!(
        parse("CREATE PROCEDURE p AS SELECT 1"),
        Err(Error::StoredProcNotSupported)
    ));
    assert!(matches!(
        parse("CREATE FUNCTION calc(x INTEGER) RETURNS INTEGER AS SELECT x * 2"),
        Err(Error::StoredProcNotSupported)
    ));
}

#[test]
fn rejection_full_text_match_operator() {
    assert!(matches!(
        parse("SELECT * FROM observations WHERE text MATCH 'pattern'"),
        Err(Error::FullTextSearchNotSupported)
    ));
    assert!(matches!(
        parse("SELECT * FROM entities WHERE name MATCH 'search term' AND status = 'active'"),
        Err(Error::FullTextSearchNotSupported)
    ));
}

#[test]
fn rejection_unbounded_vector_search() {
    assert!(matches!(
        parse("SELECT * FROM observations ORDER BY embedding <=> $q"),
        Err(Error::UnboundedVectorSearch)
    ));
}

#[test]
fn rejection_unbounded_graph_traversal() {
    assert!(matches!(
        parse(
            "SELECT b_id FROM GRAPH_TABLE (edges MATCH (a)-[:EDGE]->+(b) COLUMNS (b.id AS b_id))"
        ),
        Err(Error::UnboundedTraversal)
    ));
    assert!(matches!(
        parse(
            "SELECT b_id FROM GRAPH_TABLE (edges MATCH (a)-[:EDGE]->*(b) COLUMNS (b.id AS b_id))"
        ),
        Err(Error::UnboundedTraversal)
    ));
    assert!(matches!(
        parse("SELECT * FROM GRAPH_TABLE(edges MATCH (a)-[:EDGE]->{1,}(b) COLUMNS(b.id))"),
        Err(Error::UnboundedTraversal)
    ));
    assert!(matches!(
        parse("SELECT b_id FROM GRAPH_TABLE (edges MATCH (a)-[:EDGE]->(b) COLUMNS (b.id AS b_id))"),
        Ok(Statement::Select(_))
    ));
}

#[test]
fn rejection_subquery_support_boundary() {
    assert!(matches!(
        parse(
            "SELECT * FROM decisions WHERE id IN (SELECT id FROM edges WHERE edge_type = 'CITES')"
        ),
        Err(Error::SubqueryNotSupported)
    ));
    assert!(matches!(
        parse(
            "WITH my_cte AS (SELECT id FROM decisions) SELECT * FROM d WHERE id IN (SELECT id FROM my_cte)"
        ),
        Ok(Statement::Select(_))
    ));
}

#[test]
fn rejection_bfs_depth_exceeded() {
    assert!(matches!(
        parse(
            "SELECT b_id FROM GRAPH_TABLE (edges MATCH (a)-[:RELATES_TO]->{1,11}(b) COLUMNS (b.id AS b_id))"
        ),
        Err(Error::BfsDepthExceeded(11))
    ));
    assert!(matches!(
        parse(
            "SELECT b_id FROM GRAPH_TABLE (edges MATCH (a)-[:RELATES_TO]->{1,10}(b) COLUMNS (b.id AS b_id))"
        ),
        Ok(Statement::Select(_))
    ));
}

#[test]
fn common_mistake_graph_table_without_columns_clause() {
    let (gn, mc, cols) = expect_graph_table(parse(
        "SELECT * FROM GRAPH_TABLE (edges MATCH (a)-[:BASED_ON]->(b))",
    ));
    assert_eq!(gn, "edges");
    // CRITICAL-1: graph_name cross-check
    assert_eq!(mc.graph_name, Some("edges".to_string()));
    assert_eq!(mc.pattern.edges[0].edge_type.as_deref(), Some("BASED_ON"));
    assert_eq!(mc.pattern.edges[0].direction, EdgeDirection::Outgoing);
    // No COLUMNS clause means empty columns vec
    assert!(
        cols.is_empty(),
        "expected empty columns for GRAPH_TABLE without COLUMNS clause, got {:?}",
        cols
    );
    // CRITICAL-2: return_cols should also be empty when no COLUMNS clause
    assert!(mc.return_cols.is_empty());
}

#[test]
fn common_mistake_wrong_direction_arrow_syntax() {
    assert!(parse("SELECT b_id FROM GRAPH_TABLE (edges MATCH (a)<-[:BASED_ON]->(b) COLUMNS (b.id AS b_id))").is_err());
}

#[test]
fn common_mistake_quantifier_inside_edge_brackets() {
    assert!(parse("SELECT b_id FROM GRAPH_TABLE (edges MATCH (a)-[:BASED_ON*1..3]->(b) COLUMNS (b.id AS b_id))").is_err());
}

#[test]
fn common_mistake_missing_graph_name_in_graph_table() {
    assert!(
        parse("SELECT b_id FROM GRAPH_TABLE (MATCH (a)-[:BASED_ON]->(b) COLUMNS (b.id AS b_id))")
            .is_err()
    );
}

#[test]
fn common_mistake_old_cypher_syntax_rejected() {
    assert!(
        parse("WITH n AS (MATCH (a)-[:BASED_ON*1..2]->(b) RETURN b.id) SELECT * FROM n").is_err()
    );
}

#[test]
fn sql_basics_in_with_literal_list() {
    assert!(matches!(
        parse("SELECT * FROM decisions WHERE status IN ('active', 'pending', 'review')"),
        Ok(Statement::Select(_))
    ));
}

#[test]
fn sql_basics_not_in_with_literal_list() {
    assert!(matches!(
        parse("SELECT * FROM decisions WHERE status NOT IN ('archived', 'deleted')"),
        Ok(Statement::Select(_))
    ));
}

// HIGH-3: LIKE negated=false explicit assertion via if-let (not matches!)
#[test]
fn sql_basics_like_pattern_matching() {
    let stmt = parse("SELECT * FROM entities WHERE name LIKE 'rate%'").unwrap();
    let Statement::Select(sel) = stmt else {
        panic!("expected Select");
    };
    if let Some(Expr::Like { negated, .. }) = &sel.body.where_clause {
        assert!(!negated, "LIKE without NOT should have negated=false");
    } else {
        panic!(
            "expected Expr::Like where clause, got {:?}",
            sel.body.where_clause
        );
    }
}

#[test]
fn sql_basics_not_like_pattern_matching() {
    let stmt = parse("SELECT * FROM entities WHERE name NOT LIKE '%test%'");
    assert!(matches!(stmt, Ok(Statement::Select(_))));
    if let Ok(Statement::Select(sel)) = stmt {
        assert!(matches!(
            sel.body.where_clause,
            Some(Expr::Like { negated: true, .. })
        ));
    }
}

#[test]
fn sql_basics_between_sugar() {
    let stmt = parse("SELECT * FROM decisions WHERE confidence BETWEEN 0.5 AND 0.9");
    assert!(matches!(stmt, Ok(Statement::Select(_))));
}

#[test]
fn sql_basics_select_with_column_aliases() {
    assert!(matches!(
        parse("SELECT id AS decision_id, status AS state FROM decisions"),
        Ok(Statement::Select(_))
    ));
}

#[test]
fn sql_basics_multi_column_order_by() {
    assert!(matches!(
        parse("SELECT * FROM decisions ORDER BY status ASC, confidence DESC"),
        Ok(Statement::Select(_))
    ));
}

#[test]
fn sql_basics_distinct() {
    let stmt = parse("SELECT DISTINCT entity_type FROM entities");
    assert!(matches!(stmt, Ok(Statement::Select(_))));
    if let Ok(Statement::Select(sel)) = stmt {
        assert!(sel.body.distinct);
    }
}

#[test]
fn ast_scaffolding_fields_present_and_compilable() {
    let stmt = parse("WITH x AS (SELECT id FROM entities) SELECT * FROM x").unwrap();
    let Statement::Select(sel) = stmt else {
        panic!("expected select");
    };

    for cte in sel.ctes {
        match cte {
            Cte::SqlCte { .. } => {}
            Cte::MatchCte { match_clause, .. } => {
                assert!(match_clause.graph_name.is_none());
            }
        }
    }

    let like = Expr::Like {
        expr: Box::new(Expr::Literal(Literal::Text("name".to_string()))),
        pattern: Box::new(Expr::Literal(Literal::Text("rate%".to_string()))),
        negated: false,
    };
    assert!(matches!(like, Expr::Like { negated: false, .. }));

    let and_expr = Expr::BinaryOp {
        left: Box::new(Expr::Literal(Literal::Bool(true))),
        op: BinOp::And,
        right: Box::new(Expr::Literal(Literal::Bool(false))),
    };
    assert!(matches!(and_expr, Expr::BinaryOp { op: BinOp::And, .. }));
}

// HIGH-1: Invalid quantifier rejection tests
#[test]
fn rejection_invalid_quantifier_min_greater_than_max() {
    assert!(
        parse(
            "SELECT b_id FROM GRAPH_TABLE (edges MATCH (a)-[:EDGE]->{3,1}(b) COLUMNS (b.id AS b_id))"
        )
        .is_err()
    );
}

#[test]
fn rejection_invalid_quantifier_zero_min() {
    assert!(
        parse(
            "SELECT b_id FROM GRAPH_TABLE (edges MATCH (a)-[:EDGE]->{0,5}(b) COLUMNS (b.id AS b_id))"
        )
        .is_err()
    );
}

// HIGH-2: DISTINCT=false negative test
#[test]
fn sql_basics_select_without_distinct_is_false() {
    let stmt = parse("SELECT * FROM entities").unwrap();
    if let Statement::Select(sel) = stmt {
        assert!(
            !sel.body.distinct,
            "SELECT without DISTINCT should have distinct=false"
        );
    } else {
        panic!("expected Select");
    }
}

// LOW-2: Rejection test for missing start node alias
#[test]
fn rejection_graph_pattern_without_start_node_alias() {
    assert!(
        parse("SELECT b_id FROM GRAPH_TABLE (edges MATCH ()-[:EDGE]->(b) COLUMNS (b.id AS b_id))")
            .is_err()
    );
}

// LOW-3: Unbounded {0,} rejection test
#[test]
fn rejection_unbounded_traversal_zero_minimum() {
    assert!(matches!(
        parse(
            "SELECT b_id FROM GRAPH_TABLE (edges MATCH (a)-[:EDGE]->{0,}(b) COLUMNS (b.id AS b_id))"
        ),
        Err(Error::UnboundedTraversal)
    ));
}

#[test]
fn sql_pgq_graph_table_keyword_case_insensitive() {
    // SQL keywords are case-insensitive. GRAPH_TABLE must work in any case.
    let lower =
        parse("SELECT b_id FROM graph_table (edges MATCH (a)-[:EDGE]->(b) COLUMNS (b.id AS b_id))");
    let mixed =
        parse("SELECT b_id FROM Graph_Table (edges MATCH (a)-[:EDGE]->(b) COLUMNS (b.id AS b_id))");
    let upper =
        parse("SELECT b_id FROM GRAPH_TABLE (edges MATCH (a)-[:EDGE]->(b) COLUMNS (b.id AS b_id))");
    // All three must parse successfully and produce FromItem::GraphTable
    for (label, result) in [("lowercase", lower), ("mixed", mixed), ("uppercase", upper)] {
        let stmt = result.unwrap_or_else(|e| panic!("{label} GRAPH_TABLE should parse, got: {e}"));
        match stmt {
            Statement::Select(sel) => {
                assert!(
                    sel.body
                        .from
                        .iter()
                        .any(|f| matches!(f, FromItem::GraphTable { .. })),
                    "{label} GRAPH_TABLE should produce FromItem::GraphTable, got {:?}",
                    sel.body.from
                );
            }
            _ => panic!("{label} GRAPH_TABLE should produce Statement::Select"),
        }
    }
}

#[test]
fn anti_string_scan_nested_parens_in_where() {
    // Deeply nested boolean with parens — string scanner can't track paren depth
    let result = parse(
        "SELECT * FROM entities WHERE ((a = 1 AND (b = 2 OR c = 3)) OR (d = 4 AND e = 5)) AND f = 6",
    );
    assert!(result.is_ok());
    if let Ok(Statement::Select(sel)) = result {
        // The outermost operator must be AND (... AND f = 6)
        assert!(
            matches!(
                &sel.body.where_clause,
                Some(Expr::BinaryOp { op: BinOp::And, .. })
            ),
            "top-level WHERE should be AND, got {:?}",
            sel.body.where_clause
        );
        // The left side of that AND must be an OR (the two paren groups)
        if let Some(Expr::BinaryOp { left, op, .. }) = &sel.body.where_clause {
            assert_eq!(*op, BinOp::And);
            assert!(
                matches!(left.as_ref(), Expr::BinaryOp { op: BinOp::Or, .. }),
                "left of top AND should be OR, got {:?}",
                left
            );
        }
    }
}

#[test]
fn anti_string_scan_keyword_in_column_name() {
    // Column named "select_count", "from_date", alias "as_val" — scanner confuses with keywords
    let result =
        parse("SELECT select_count, from_date AS as_val FROM table_from WHERE where_clause = $val");
    assert!(result.is_ok());
    if let Ok(Statement::Select(sel)) = result {
        // Must have exactly 2 columns
        assert_eq!(
            sel.body.columns.len(),
            2,
            "expected 2 columns, got {:?}",
            sel.body.columns
        );
        // First column must be "select_count"
        assert!(
            matches!(&sel.body.columns[0].expr, Expr::Column(cr) if cr.column == "select_count"),
            "first column should be 'select_count', got {:?}",
            sel.body.columns[0]
        );
        // Second column must be "from_date" with alias "as_val"
        assert!(
            matches!(&sel.body.columns[1].expr, Expr::Column(cr) if cr.column == "from_date"),
            "second column should be 'from_date', got {:?}",
            sel.body.columns[1]
        );
        assert_eq!(
            sel.body.columns[1].alias.as_deref(),
            Some("as_val"),
            "second column alias should be 'as_val', got {:?}",
            sel.body.columns[1].alias
        );
    }
}

#[test]
fn anti_string_scan_string_literal_with_sql() {
    // String literal contains complete SQL statement — scanner must not parse it
    let result = parse(
        "INSERT INTO logs (id, query) VALUES ($id, 'SELECT * FROM GRAPH_TABLE (edges MATCH (a)-[:EDGE]->(b) COLUMNS (b.id AS b_id))')",
    );
    assert!(matches!(result, Ok(Statement::Insert(_))));
    if let Ok(Statement::Insert(ins)) = result {
        assert_eq!(ins.columns.len(), 2, "expected 2 columns");
        assert_eq!(ins.columns[0], "id");
        assert_eq!(ins.columns[1], "query");
        assert_eq!(ins.values.len(), 1, "expected 1 row");
        assert_eq!(ins.values[0].len(), 2, "expected 2 values in row");
        // The second value must be a Text literal containing the SQL string, not parsed as SQL
        assert!(
            matches!(&ins.values[0][1], Expr::Literal(Literal::Text(t)) if t.contains("GRAPH_TABLE")),
            "second value should be a string literal containing GRAPH_TABLE, got {:?}",
            ins.values[0][1]
        );
    }
}

#[test]
fn anti_string_scan_escaped_quotes_in_string() {
    // Escaped quotes inside string literal
    let result = parse(
        "INSERT INTO entities (id, name) VALUES ($id, 'it''s a ''quoted'' value WITH ''MATCH'' inside')",
    );
    assert!(matches!(result, Ok(Statement::Insert(_))));
    if let Ok(Statement::Insert(ins)) = result {
        assert_eq!(ins.values[0].len(), 2, "expected 2 values");
        // The second value must be a single Text literal with escaped quotes resolved
        assert!(
            matches!(
                &ins.values[0][1],
                Expr::Literal(Literal::Text(t)) if t.contains("it's") && t.contains("MATCH")
            ),
            "second value should be text with escaped quotes resolved, got {:?}",
            ins.values[0][1]
        );
    }
}

#[test]
fn anti_string_scan_graph_table_in_string_literal() {
    // GRAPH_TABLE keyword inside a string literal with commas — scanner splits on commas in string
    let result = parse(
        "INSERT INTO logs (id, sql_text) VALUES ($id, 'SELECT a, b FROM GRAPH_TABLE (edges MATCH (a)->(b) COLUMNS (a.id AS a_id, b.id AS b_id))')",
    );
    assert!(matches!(result, Ok(Statement::Insert(_))));
    if let Ok(Statement::Insert(ins)) = result {
        assert_eq!(ins.table, "logs");
        assert_eq!(ins.columns.len(), 2);
        // Must be exactly 2 values — scanner that splits on commas inside string literal will see more
        assert_eq!(
            ins.values[0].len(),
            2,
            "expected 2 values in row, got {:?}",
            ins.values[0]
        );
        // The second value must be a single Text literal containing the entire SQL string
        assert!(
            matches!(&ins.values[0][1], Expr::Literal(Literal::Text(t)) if t.contains("GRAPH_TABLE") && t.contains("COLUMNS")),
            "second value should be a text literal with GRAPH_TABLE, got {:?}",
            ins.values[0][1]
        );
    }
}

#[test]
fn anti_string_scan_multiple_graph_tables_in_from() {
    // Two GRAPH_TABLEs in FROM — string scanner would struggle with boundaries
    let result = parse(
        "SELECT a_id, b_id FROM GRAPH_TABLE (edges MATCH (a)-[:SERVES]->(b) COLUMNS (a.id AS a_id)), GRAPH_TABLE (edges MATCH (c)-[:CITES]->(d) COLUMNS (d.id AS b_id))",
    );
    assert!(result.is_ok());
    if let Ok(Statement::Select(sel)) = result {
        let gt_count = sel
            .body
            .from
            .iter()
            .filter(|f| matches!(f, FromItem::GraphTable { .. }))
            .count();
        assert_eq!(gt_count, 2, "expected two GRAPH_TABLE items in FROM");
    }
}

#[test]
fn anti_string_scan_where_with_subexpression_containing_keywords() {
    // WHERE clause contains identifiers that look like keywords
    let result = parse(
        "SELECT * FROM entities WHERE from_date > $start AND to_date < $end AND select_mode = 'auto'",
    );
    assert!(result.is_ok());
    if let Ok(Statement::Select(sel)) = result {
        // The WHERE clause must be a compound AND expression with three conditions
        // A string scanner that splits on "AND" without respecting structure will fail
        let wc = sel.body.where_clause.as_ref().expect("WHERE should exist");
        // Outermost must be AND
        assert!(
            matches!(wc, Expr::BinaryOp { op: BinOp::And, .. }),
            "top-level WHERE should be AND, got {:?}",
            wc
        );
        // The from_date condition must use Gt (>), not Eq
        fn contains_gt(e: &Expr) -> bool {
            match e {
                Expr::BinaryOp {
                    left,
                    op: BinOp::Gt,
                    ..
                } => matches!(left.as_ref(), Expr::Column(cr) if cr.column == "from_date"),
                Expr::BinaryOp { left, right, .. } => contains_gt(left) || contains_gt(right),
                _ => false,
            }
        }
        assert!(
            contains_gt(wc),
            "WHERE should contain from_date > $start, got {:?}",
            wc
        );
    }
}

#[test]
fn anti_string_scan_graph_table_with_complex_where() {
    // GRAPH_TABLE with multi-condition WHERE inside MATCH + conditions outside
    let result = parse(
        "SELECT b_id FROM GRAPH_TABLE (edges MATCH (a)-[:BASED_ON]->(b) WHERE a.status = 'active' AND b.confidence > 0.5 COLUMNS (b.id AS b_id)) WHERE b_id IS NOT NULL",
    );
    assert!(result.is_ok());
    if let Ok(Statement::Select(sel)) = result {
        // Outer WHERE exists
        assert!(sel.body.where_clause.is_some(), "outer WHERE should exist");
        // Inner MATCH WHERE exists
        let gt = sel
            .body
            .from
            .iter()
            .find(|f| matches!(f, FromItem::GraphTable { .. }));
        assert!(gt.is_some(), "expected GRAPH_TABLE");
        if let Some(FromItem::GraphTable { match_clause, .. }) = gt {
            assert!(
                match_clause.where_clause.is_some(),
                "inner MATCH WHERE should exist"
            );
        }
    }
}

#[test]
fn anti_string_scan_whitespace_variations() {
    // Unusual whitespace — tabs, multiple spaces, newlines
    let result = parse(
        "SELECT\n  b_id\nFROM\n  GRAPH_TABLE (\n    edges\n    MATCH  (a)-[:EDGE]->(b)\n    COLUMNS  (b.id  AS  b_id)\n  )",
    );
    assert!(result.is_ok());
    if let Ok(Statement::Select(sel)) = result {
        assert!(
            sel.body
                .from
                .iter()
                .any(|f| matches!(f, FromItem::GraphTable { .. }))
        );
    }
}
