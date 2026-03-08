use contextdb_core::Error;
use contextdb_parser::{Statement, parse};

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
            "WITH n AS (MATCH (a:Entity {id: $id})-[:BASED_ON*1..3]->(b) RETURN b.id) SELECT * FROM n"
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
        parse("WITH n AS (MATCH (a)-[:EDGE*]->(b) RETURN b) SELECT * FROM n"),
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
        parse("CREATE TABLE bad (id UUID PRIMARY KEY) IMMUTABLE STATE MACHINE (status: a -> [b])"),
        Err(Error::ParseError(_))
    ));
}
