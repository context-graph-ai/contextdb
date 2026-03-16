use contextdb_core::Error;
use contextdb_parser::ast::{BinOp, Cte, Expr, ForeignKey, FromItem, Literal, Statement};
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
    assert!(matches!(
        parse(
            "SELECT b_id FROM GRAPH_TABLE (edges MATCH (a:Entity)-[:BASED_ON]->{1,3}(b) WHERE a.id = $id COLUMNS (b.id AS b_id))"
        ),
        Ok(Statement::Select(_))
    ));
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
    assert!(matches!(
        parse(
            "WITH graph_results AS (SELECT b_id FROM GRAPH_TABLE (edges MATCH (a)-[:BASED_ON]->{1,3}(b) COLUMNS (b.id AS b_id))), filtered AS (SELECT id FROM decisions WHERE status = 'active') SELECT * FROM graph_results WHERE b_id IN (SELECT id FROM filtered)"
        ),
        Ok(Statement::Select(_))
    ));
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

#[test]
fn sql_pgq_basic_graph_table_single_edge_step() {
    let stmt = parse(
        "SELECT b_id FROM GRAPH_TABLE (edges MATCH (a)-[:BASED_ON]->(b) COLUMNS (b.id AS b_id))",
    );
    assert!(matches!(stmt, Ok(Statement::Select(_))));
}

#[test]
fn sql_pgq_multi_step_patterns() {
    assert!(matches!(
        parse(
            "SELECT c_id FROM GRAPH_TABLE (edges MATCH (a)-[:SERVES]->(b)-[:BASED_ON]->(c) COLUMNS (c.id AS c_id))"
        ),
        Ok(Statement::Select(_))
    ));
    assert!(matches!(
        parse(
            "SELECT d_desc FROM GRAPH_TABLE (edges MATCH (a)-[:CITES]->(b)-[:BASED_ON]->(c)-[:SERVES]->(d) COLUMNS (d.description AS d_desc))"
        ),
        Ok(Statement::Select(_))
    ));
}

#[test]
fn sql_pgq_meaningful_node_aliases() {
    assert!(matches!(
        parse(
            "SELECT decision_desc FROM GRAPH_TABLE (edges MATCH (entity)-[:BASED_ON]->(decision) COLUMNS (decision.description AS decision_desc))"
        ),
        Ok(Statement::Select(_))
    ));
}

#[test]
fn sql_pgq_edge_type_filtering() {
    assert!(matches!(
        parse(
            "SELECT b_id FROM GRAPH_TABLE (edges MATCH (a)-[:CITES]->(b) COLUMNS (b.id AS b_id))"
        ),
        Ok(Statement::Select(_))
    ));
    assert!(matches!(
        parse("SELECT b_id FROM GRAPH_TABLE (edges MATCH (a)-[]->(b) COLUMNS (b.id AS b_id))"),
        Ok(Statement::Select(_))
    ));
}

#[test]
fn sql_pgq_quantified_paths_depth_bounds() {
    assert!(matches!(
        parse(
            "SELECT b_id FROM GRAPH_TABLE (edges MATCH (a)-[:BASED_ON]->{1,5}(b) COLUMNS (b.id AS b_id))"
        ),
        Ok(Statement::Select(_))
    ));
    assert!(matches!(
        parse(
            "SELECT b_id FROM GRAPH_TABLE (edges MATCH (a)-[:CITES]->{2,10}(b) COLUMNS (b.id AS b_id))"
        ),
        Ok(Statement::Select(_))
    ));
    assert!(matches!(
        parse(
            "SELECT b_id FROM GRAPH_TABLE (edges MATCH (a)-[:CITES]->{1,1}(b) COLUMNS (b.id AS b_id))"
        ),
        Ok(Statement::Select(_))
    ));
}

#[test]
fn sql_pgq_where_inside_match() {
    assert!(matches!(
        parse(
            "SELECT b_id FROM GRAPH_TABLE (edges MATCH (a)-[:CITES]->(b) WHERE b.status = 'active' COLUMNS (b.id AS b_id))"
        ),
        Ok(Statement::Select(_))
    ));
    assert!(matches!(
        parse(
            "SELECT b_id FROM GRAPH_TABLE (edges MATCH (a)-[:BASED_ON]->{1,3}(b) WHERE b.confidence > 0.5 AND a.status = 'active' COLUMNS (b.id AS b_id))"
        ),
        Ok(Statement::Select(_))
    ));
}

#[test]
fn sql_pgq_bidirectional_edges() {
    assert!(matches!(
        parse(
            "SELECT a_id FROM GRAPH_TABLE (edges MATCH (a)<-[:BASED_ON]-(b) COLUMNS (a.id AS a_id))"
        ),
        Ok(Statement::Select(_))
    ));
    assert!(matches!(
        parse(
            "SELECT b_id FROM GRAPH_TABLE (edges MATCH (a)-[:RELATES_TO]-(b) COLUMNS (b.id AS b_id))"
        ),
        Ok(Statement::Select(_))
    ));
}

#[test]
fn sql_pgq_columns_clause_variations() {
    assert!(matches!(
        parse(
            "SELECT src, tgt, etype FROM GRAPH_TABLE (edges MATCH (a)-[e:CITES]->(b) COLUMNS (a.id AS src, b.id AS tgt, e.edge_type AS etype))"
        ),
        Ok(Statement::Select(_))
    ));
    assert!(matches!(
        parse(
            "SELECT b_id, b_name FROM GRAPH_TABLE (edges MATCH (a)-[:BASED_ON]->{1,2}(b) COLUMNS (b.id AS b_id, b.name AS b_name))"
        ),
        Ok(Statement::Select(_))
    ));
}

#[test]
fn sql_pgq_graph_table_as_cte_source() {
    assert!(matches!(
        parse(
            "WITH reachable AS (SELECT b_id FROM GRAPH_TABLE (edges MATCH (a)-[:BASED_ON]->{1,3}(b) COLUMNS (b.id AS b_id))) SELECT * FROM reachable WHERE b_id = $target"
        ),
        Ok(Statement::Select(_))
    ));
}

#[test]
fn sql_pgq_node_labels() {
    assert!(matches!(
        parse(
            "SELECT b_id FROM GRAPH_TABLE (edges MATCH (a:Entity)-[:BASED_ON]->(b:Decision) COLUMNS (b.id AS b_id))"
        ),
        Ok(Statement::Select(_))
    ));
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
            Some(ForeignKey { table, column }) if table == "intentions" && column == "id"
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
    assert!(matches!(
        parse(
            "CREATE TABLE decisions (id UUID PRIMARY KEY, status TEXT, intention_id UUID REFERENCES intentions(id) ON STATE archived PROPAGATE SET invalidated) STATE MACHINE (status: active -> [invalidated, superseded])"
        ),
        Ok(Statement::CreateTable(_))
    ));
    assert!(matches!(
        parse(
            "CREATE TABLE decisions (id UUID PRIMARY KEY, status TEXT, intention_id UUID REFERENCES intentions(id) ON STATE archived PROPAGATE SET invalidated ON STATE superseded PROPAGATE SET invalidated) STATE MACHINE (status: active -> [invalidated, superseded])"
        ),
        Ok(Statement::CreateTable(_))
    ));
}

#[test]
fn ddl_state_propagation_edge_rules_parse() {
    assert!(matches!(
        parse(
            "CREATE TABLE decisions (id UUID PRIMARY KEY, status TEXT) STATE MACHINE (status: active -> [invalidated, superseded]) PROPAGATE ON EDGE CITES INCOMING STATE invalidated SET invalidated"
        ),
        Ok(Statement::CreateTable(_))
    ));
    assert!(matches!(
        parse(
            "CREATE TABLE decisions (id UUID PRIMARY KEY, status TEXT) STATE MACHINE (status: active -> [invalidated, superseded]) PROPAGATE ON EDGE CITES INCOMING STATE invalidated SET invalidated PROPAGATE ON EDGE SERVES OUTGOING STATE invalidated SET flagged"
        ),
        Ok(Statement::CreateTable(_))
    ));
}

#[test]
fn ddl_state_propagation_vector_exclusions_parse() {
    assert!(matches!(
        parse(
            "CREATE TABLE decisions (id UUID PRIMARY KEY, status TEXT, embedding VECTOR(384)) STATE MACHINE (status: active -> [invalidated, superseded]) PROPAGATE ON STATE invalidated EXCLUDE VECTOR"
        ),
        Ok(Statement::CreateTable(_))
    ));
    assert!(matches!(
        parse(
            "CREATE TABLE decisions (id UUID PRIMARY KEY, status TEXT, embedding VECTOR(384)) STATE MACHINE (status: active -> [invalidated, superseded]) PROPAGATE ON STATE invalidated EXCLUDE VECTOR PROPAGATE ON STATE superseded EXCLUDE VECTOR"
        ),
        Ok(Statement::CreateTable(_))
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
    let stmt = parse("SELECT * FROM GRAPH_TABLE (edges MATCH (a)-[:BASED_ON]->(b))");
    assert!(matches!(stmt, Ok(Statement::Select(_))));
    if let Ok(Statement::Select(sel)) = stmt
        && let Some(FromItem::GraphTable { match_clause, .. }) = sel.body.from.first()
    {
        assert!(match_clause.return_cols.is_empty());
    }
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

#[test]
fn sql_basics_like_pattern_matching() {
    let stmt = parse("SELECT * FROM entities WHERE name LIKE 'rate%'");
    assert!(matches!(stmt, Ok(Statement::Select(_))));
    if let Ok(Statement::Select(sel)) = stmt {
        assert!(matches!(
            sel.body.where_clause,
            Some(Expr::Like { negated: false, .. })
        ));
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
