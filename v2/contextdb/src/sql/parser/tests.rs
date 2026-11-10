use super::parse;
use crate::sql::ast::{DataType, Expr, Literal, Statement};
use crate::sql::plan;
use crate::sql::plan::{Effect, PhysicalPlan};
use crate::types::Error;

fn refusal(sql: &str) -> String {
    match parse(sql) {
        Err(Error::ParseError(text)) => text,
        other => panic!("{sql}\nwas not refused as a parse error: {other:?}"),
    }
}

fn planned(sql: &str) -> PhysicalPlan {
    let statement = parse(sql).unwrap_or_else(|error| panic!("{sql}\nwas refused: {error:?}"));
    plan(&statement).unwrap_or_else(|error| panic!("{sql}\nwas not planned: {error:?}"))
}

#[test]
fn a_refusal_names_the_word_the_grammar_stopped_at() {
    assert_eq!(
        refusal("SELECT id FRUM t"),
        "syntax error at line 1, column 11: unexpected 'FRUM'"
    );
    assert!(refusal("SELEKT id FROM t").contains("'SELEKT'"));
    assert!(refusal("SELECT id\nFROM").contains("line 2, column 5: the statement ends too soon"));
}

#[test]
fn nesting_is_read_to_the_limit_and_refused_past_it() {
    // Each way an expression nests, on the stack a test thread has.
    for (open, close) in [("(", ")"), ("COALESCE(", ", 1)"), ("NOT ", "")] {
        let nested = |levels: usize| {
            format!(
                "SELECT id FROM t WHERE {}id{}",
                open.repeat(levels),
                close.repeat(levels)
            )
        };
        if let Err(error) = parse(&nested(128)) {
            panic!("128 levels of `{open}` were refused: {error:?}");
        }
        assert!(refusal(&nested(129)).contains("deeper than the limit of 128"));
    }
    // A parenthesis inside quoted text or a comment is not a level.
    let quoted = format!(
        "SELECT '{}' /* {} */ FROM t",
        "(".repeat(200),
        "(".repeat(200)
    );
    parse(&quoted).expect("quoted parentheses do not nest");
    // A chain of operators nests in the tree it builds, not in the text.
    let chain = format!("SELECT id FROM t WHERE id = 1{}", " + 1".repeat(200));
    assert!(refusal(&chain).contains("deeper than the limit of 128"));
    // A run of ANDs is joined into a shallow tree and is not a chain.
    let run = format!(
        "SELECT id FROM t WHERE id = 1{}",
        " AND id = 1".repeat(2000)
    );
    parse(&run).expect("a long run of ANDs is read");
}

#[test]
fn operators_bind_as_in_standard_sql() {
    let filter = |sql: &str| {
        let Statement::Select(select) = parse(&format!("SELECT id FROM t WHERE {sql}")).unwrap()
        else {
            panic!("not a SELECT");
        };
        format!("{:?}", select.where_clause.unwrap())
    };
    assert_eq!(
        filter("a = 1 + 2 * 3 - 4"),
        filter("a = ((1 + (2 * 3)) - 4)")
    );
    assert_eq!(
        filter("a = 8 / 4 / 2 * -b"),
        filter("a = (((8 / 4) / 2) * (-b))")
    );
    assert_eq!(
        filter("NOT a = 1 AND b IS NOT NULL OR c <> 3"),
        filter("((NOT (a = 1)) AND (b IS NOT NULL)) OR (c != 3)")
    );
}

#[test]
fn a_table_is_read_with_its_columns_and_rules() {
    let Statement::CreateTable(table) = parse(
        "CREATE TABLE IF NOT EXISTS \"my codes\" (id INTEGER PRIMARY KEY, code TEXT NOT NULL \
         DEFAULT 'a''b', v VECTOR(3), at TIMESTAMP DEFAULT NOW(), UNIQUE (code))",
    )
    .unwrap() else {
        panic!("not a CREATE TABLE");
    };
    assert_eq!(table.name, "my codes");
    assert!(table.if_not_exists);
    // A table-level UNIQUE may name one column.
    assert_eq!(table.unique_constraints, [["code"]]);
    let types: Vec<&DataType> = table
        .columns
        .iter()
        .map(|column| &column.data_type)
        .collect();
    assert_eq!(
        types,
        [
            &DataType::Integer,
            &DataType::Text,
            &DataType::Vector(3),
            &DataType::Timestamp
        ]
    );
    assert!(table.columns[0].primary_key && !table.columns[1].nullable);
    assert!(matches!(
        &table.columns[1].default,
        Some(Expr::Literal(Literal::Text(text))) if text == "a'b"
    ));
    assert!(refusal("CREATE TABLE t (a TEXT, UNIQUE (a, a))").contains("twice"));
    assert!(refusal("CREATE TABLE t (a TEXT, UNIQUE (b))").contains("unknown column 'b'"));
    assert!(refusal("CREATE TABLE t (a TEXT NOT NULL NOT NULL)").contains("duplicate NOT NULL"));
}

#[test]
fn every_statement_is_read_planned_and_told_apart() {
    for (sql, effect) in [
        (
            "CREATE TABLE t (id UUID PRIMARY KEY, n INT NULL, ok BOOL, r FLOAT, doc JSON)",
            Effect::Declares,
        ),
        ("CREATE INDEX by_n ON t (n DESC, id)", Effect::Declares),
        (
            "INSERT INTO t VALUES (1, 'a', NULL, TRUE, -2.5, [0.1, -0.2], $p)",
            Effect::WritesRows,
        ),
        (
            "INSERT INTO t (a, b) VALUES (1, 2), (3, 4);",
            Effect::WritesRows,
        ),
        (
            "UPDATE t SET a = 1, b = b * 2 WHERE a IS NOT NULL",
            Effect::WritesRows,
        ),
        ("DELETE FROM t WHERE a >= 1 OR NOT ok", Effect::WritesRows),
        ("DELETE FROM t", Effect::WritesRows),
        ("SELECT * FROM t", Effect::Reads),
        (
            "SELECT COUNT(*), MIN(n), MAX(n), SUM(n) AS total FROM t WHERE t.n < 5",
            Effect::Reads,
        ),
        (
            "SELECT id, n + 1 AS next FROM t ORDER BY next DESC, COALESCE(n, 0), id LIMIT 3",
            Effect::Reads,
        ),
        ("SET MEMORY_LIMIT '64M'", Effect::Sets(Some(64 << 20))),
        ("SET MEMORY_LIMIT = none", Effect::Sets(None)),
        ("SHOW MEMORY_LIMIT", Effect::Reads),
    ] {
        assert_eq!(planned(sql).effect(), effect, "{sql}");
    }
    assert!(matches!(
        planned("SELECT * FROM t LIMIT 1"),
        PhysicalPlan::Limit { .. }
    ));
    assert!(matches!(
        planned("SELECT a FROM t ORDER BY a"),
        PhysicalPlan::Project { .. }
    ));
    assert!(refusal("SET MEMORY_LIMIT 99999999999999999999G").contains("out of range"));
}

#[test]
fn a_sort_key_the_engine_cannot_work_out_is_refused_when_planned() {
    for key in ["UPPER(tag)", "tag = 'a'", "1"] {
        let statement = parse(&format!("SELECT tag FROM notes ORDER BY {key}")).unwrap();
        assert!(
            matches!(plan(&statement), Err(Error::OrderByExpressionNotSupported)),
            "{key}"
        );
    }
}

#[test]
fn comments_are_skipped_and_kept_inside_quoted_text() {
    let Statement::Insert(insert) = parse(
        "/* before */ INSERT INTO notes (id, body) -- the columns\n VALUES (1, '-- kept /* too */');",
    )
    .unwrap() else {
        panic!("not an INSERT");
    };
    assert!(matches!(
        &insert.values[0][1],
        Expr::Literal(Literal::Text(text)) if text == "-- kept /* too */"
    ));
}

#[test]
fn what_a_later_version_reads_is_refused_by_its_name() {
    for (sql, name) in [
        ("ALTER TABLE t ADD COLUMN c TEXT", "ALTER"),
        ("drop table t", "DROP"),
        ("BEGIN", "BEGIN"),
        (
            "WITH recent AS (SELECT a FROM t) SELECT a FROM recent",
            "WITH",
        ),
        ("SELECT DISTINCT a FROM t", "DISTINCT"),
        (
            "SELECT a FROM t LEFT  JOIN u ON t.a = u.a WHERE a = 1",
            "LEFT JOIN",
        ),
        ("SELECT a FROM t JOIN u ON t.a = u.a", "JOIN"),
        (
            "SELECT a, COUNT(*) FROM t WHERE a = 1 GROUP BY a",
            "GROUP BY",
        ),
        (
            "SELECT a FROM GRAPH_TABLE(g MATCH (a)-[e]->(b))",
            "GRAPH_TABLE",
        ),
        ("SELECT id FROM t ORDER BY v <=> $q LIMIT 3", "<=>"),
        ("SELECT id FROM t WHERE a NOT IN (1, 2)", "NOT IN"),
        ("SELECT id FROM t WHERE a LIKE 'x%'", "LIKE"),
        ("UPDATE t SET a = 1 WHERE b BETWEEN 1 AND 2", "BETWEEN"),
        (
            "INSERT INTO t (a) VALUES (1) ON CONFLICT (a) DO UPDATE SET a = 2",
            "ON CONFLICT",
        ),
        (
            "CREATE TABLE t (id UUID PRIMARY KEY) RETAIN 7 DAYS",
            "RETAIN",
        ),
        (
            "CREATE TABLE t (id UUID PRIMARY KEY) SYNC PUSH ONLY",
            "SYNC",
        ),
        (
            "CREATE TABLE t (id UUID PRIMARY KEY, o UUID REFERENCES p(id))",
            "REFERENCES",
        ),
        (
            "CREATE TABLE t (id UUID PRIMARY KEY, v VECTOR(3) WITH (quantization = 'SQ8'))",
            "WITH",
        ),
        (
            "CREATE TABLE t (a INT, b INT, FOREIGN KEY (a, b) REFERENCES o (a, b))",
            "FOREIGN KEY",
        ),
    ] {
        match parse(sql) {
            Err(Error::PlanError(text)) => assert_eq!(
                text,
                format!("{name} is not supported in this version of the library"),
                "{sql}"
            ),
            other => panic!("{sql}\nwas not refused by name: {other:?}"),
        }
    }
    // A column may still carry one of those names.
    planned("CREATE TABLE t (id INT PRIMARY KEY, state TEXT, immutable BOOL, history TEXT)");
    planned("SELECT state, history FROM t WHERE immutable ORDER BY state");
    // What is neither read nor named is a syntax error.
    assert!(refusal("SELECT a FROM t x").contains("unexpected 'x'"));
    assert!(refusal("SELECT (a, b) FROM t").contains("unexpected ','"));
}
