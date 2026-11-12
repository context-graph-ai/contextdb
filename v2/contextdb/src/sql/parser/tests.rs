use super::parse;
use crate::sql::ast::{
    BinOp, ColumnRef, ConsolidationClause, DataType, Expr, Literal, Select, Statement,
    VectorClauses,
};
use crate::sql::plan;
use crate::sql::plan::{Effect, PhysicalPlan};
use crate::types::{Error, SortDirection, VectorSearchMode};

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
        ("BEGIN", "BEGIN"),
        ("purge FROM t WHERE id = 1", "PURGE"),
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
        ("SELECT id FROM t WHERE a LIKE 'x%'", "LIKE"),
        ("UPDATE t SET a = 1 WHERE b BETWEEN 1 AND 2", "BETWEEN"),
        (
            "CREATE TABLE t (id UUID PRIMARY KEY, until TIMESTAMP EXPIRES)",
            "EXPIRES",
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
    // A row value is read wherever an expression is. In a select list it is refused by name
    // when the statement is planned.
    let read = parse("SELECT (a, b) FROM t")
        .unwrap_or_else(|error| panic!("a row value in a select list was not read: {error:?}"));
    match plan(&read) {
        Err(Error::PlanError(text)) => {
            assert!(text.to_lowercase().contains("row value"), "{text}");
        }
        other => panic!("a row value in a select list was planned as {other:?}"),
    }
}

fn select(sql: &str) -> Select {
    match parse(sql) {
        Ok(Statement::Select(select)) => select,
        other => panic!("{sql}\nwas not read as a SELECT: {other:?}"),
    }
}

/// True for `<column> <=> $<parameter>`.
fn is_distance(expr: &Expr, column: &str, parameter: &str) -> bool {
    let Expr::Distance { left, right } = expr else {
        return false;
    };
    let of_column = matches!(
        &**left,
        Expr::Column(ColumnRef { table: None, column: name }) if name == column
    );
    of_column && matches!(&**right, Expr::Parameter(name) if name == parameter)
}

#[test]
fn a_distance_is_read_as_an_expression() {
    // In a select item under a name, with the search ordered by that name.
    let read = select("SELECT id, v <=> $q AS distance FROM t ORDER BY distance LIMIT 3");
    assert!(is_distance(&read.columns[1].expr, "v", "q"));
    assert_eq!(read.columns[1].alias.as_deref(), Some("distance"));
    assert!(matches!(
        &read.order_by[..],
        [item] if item.direction == SortDirection::Asc
            && matches!(
                &item.expr,
                Expr::Column(ColumnRef { table: None, column }) if column == "distance"
            )
    ));
    assert_eq!((read.limit, read.use_vector), (Some(3), None));

    // Under no name, and as the ORDER BY item itself, with ASC written or not.
    for order in ["v <=> $q", "v <=> $q ASC", "v<=>$q asc"] {
        let read = select(&format!(
            "SELECT id, v <=> $q FROM t ORDER BY {order} LIMIT 3"
        ));
        assert!(is_distance(&read.columns[1].expr, "v", "q"), "{order}");
        assert_eq!(read.columns[1].alias, None, "{order}");
        assert!(
            matches!(
                &read.order_by[..],
                [item] if item.direction == SortDirection::Asc && is_distance(&item.expr, "v", "q")
            ),
            "{order}"
        );
    }

    // Inside another expression: one minus the distance.
    let read = select("SELECT id, 1 - (v <=> $q) AS similarity FROM t ORDER BY v <=> $q LIMIT 3");
    assert!(matches!(
        &read.columns[1].expr,
        Expr::FunctionCall { name, args }
            if name == "__sub"
                && matches!(&args[..], [Expr::Literal(Literal::Integer(1)), distance]
                    if is_distance(distance, "v", "q"))
    ));
    assert_eq!(read.columns[1].alias.as_deref(), Some("similarity"));

    // The vector on the right is a literal, its quoted text or a bound name.
    let read = select("SELECT id FROM t ORDER BY v <=> [1.0, 0.0] LIMIT 3");
    assert!(matches!(
        &read.order_by[0].expr,
        Expr::Distance { right, .. }
            if matches!(&**right, Expr::Literal(Literal::Vector(vector)) if vector == &[1.0, 0.0])
    ));
    let read = select("SELECT id FROM t ORDER BY v <=> '[1,0]' LIMIT 3");
    assert!(matches!(
        &read.order_by[0].expr,
        Expr::Distance { right, .. }
            if matches!(&**right, Expr::Literal(Literal::Text(text)) if text == "[1,0]")
    ));
}

#[test]
fn a_distance_binds_tighter_than_a_comparison_and_looser_than_a_sum() {
    let item = |sql: &str| {
        let read = select(&format!("SELECT {sql} FROM t"));
        format!("{:?}", read.columns[0].expr)
    };
    assert_eq!(item("v <=> $q < 0.5"), item("(v <=> $q) < 0.5"));
    assert_eq!(item("0.5 >= v <=> $q"), item("0.5 >= (v <=> $q)"));
    assert_eq!(item("v <=> $q + $r"), item("v <=> ($q + $r)"));
    assert_eq!(item("a + b <=> $q - $r"), item("(a + b) <=> ($q - $r)"));
    assert_eq!(item("1 - v <=> $q"), item("(1 - v) <=> $q"));
    assert_eq!(
        item("v <=> $q < w <=> $q AND NOT v <=> $q = 1"),
        item("((v <=> $q) < (w <=> $q)) AND (NOT ((v <=> $q) = 1))")
    );
    // The first of them is a comparison of a distance, and the third a distance to a sum.
    let read = select("SELECT v <=> $q < 0.5, v <=> $q + $r FROM t");
    assert!(matches!(
        &read.columns[0].expr,
        Expr::BinaryOp { left, op: BinOp::Lt, .. } if is_distance(left, "v", "q")
    ));
    assert!(matches!(
        &read.columns[1].expr,
        Expr::Distance { right, .. }
            if matches!(&**right, Expr::FunctionCall { name, .. } if name == "__add")
    ));
}

#[test]
fn a_search_is_planned_as_one_step_with_its_limit() {
    // Under a projection when the statement names its columns: the search holds the column,
    // the vector, the limit, the filter and the mode, and no limit wraps it.
    let planned_search = planned(
        "SELECT id, v <=> $q AS distance FROM t WHERE a = 1 ORDER BY distance USE VECTOR EXACT \
         LIMIT 3",
    );
    assert_eq!(planned_search.effect(), Effect::Reads);
    let (input, columns) = match planned_search {
        PhysicalPlan::Project { input, columns } => (input, columns),
        other => panic!("a search with a select list is not under a projection: {other:?}"),
    };
    assert!(is_distance(&columns[1].expr, "v", "q"));
    assert_eq!(columns[1].alias.as_deref(), Some("distance"));
    assert!(matches!(
        &*input,
        PhysicalPlan::VectorSearch {
            table,
            column,
            query: Expr::Parameter(query),
            k: 3,
            filter: Some(_),
            mode: Some(VectorSearchMode::Exact),
        } if table == "t" && column == "v" && query == "q"
    ));
    // `SELECT *` is the search itself, and with no USE VECTOR the mode is left to the column.
    for order in ["v <=> $q", "v <=> $q ASC"] {
        let sql = format!("SELECT * FROM t ORDER BY {order} LIMIT 10");
        assert!(
            matches!(
                planned(&sql),
                PhysicalPlan::VectorSearch {
                    k: 10,
                    filter: None,
                    mode: None,
                    ..
                }
            ),
            "{sql}"
        );
    }
    for (mode, read) in [
        ("AUTO", VectorSearchMode::Auto),
        ("exact", VectorSearchMode::Exact),
        ("Indexed", VectorSearchMode::Indexed),
    ] {
        let sql = format!("SELECT * FROM t ORDER BY v <=> $q USE VECTOR {mode} LIMIT 1");
        assert_eq!(select(&sql).use_vector, Some(read), "{sql}");
        assert!(
            matches!(
                planned(&sql),
                PhysicalPlan::VectorSearch { mode: Some(found), .. } if found == read
            ),
            "{sql}"
        );
    }
    // What is refused without looking at any table, when the statement is read or planned.
    let refused = |sql: &str| match parse(sql) {
        Ok(statement) => plan(&statement).expect_err(sql),
        Err(error) => error,
    };
    // A search with no LIMIT has no kind of its own: it is refused as every statement of a
    // later version is.
    match refused("SELECT id FROM t ORDER BY v <=> $q") {
        Error::PlanError(text) => assert_eq!(
            text,
            "a vector search without LIMIT is not supported in this version of the library"
        ),
        other => panic!("a search with no LIMIT was refused as {other:?}"),
    }
    assert!(matches!(
        refused("SELECT id FROM t ORDER BY id USE VECTOR EXACT LIMIT 3"),
        Error::UseVectorRequiresVectorOrder
    ));
    for sql in [
        "SELECT id FROM t ORDER BY v <=> $q DESC LIMIT 3",
        "SELECT id FROM t ORDER BY v <=> $q, id LIMIT 3",
        "SELECT id FROM t ORDER BY $q <=> v LIMIT 3",
        "SELECT id FROM t WHERE v <=> $q < 0.5",
        "SELECT id, v <=> $q AS distance FROM t",
        "SELECT id, v <=> $q AS distance FROM t ORDER BY id LIMIT 3",
    ] {
        match refused(sql) {
            Error::PlanError(text) => assert!(text.contains("<=>"), "{sql}: {text}"),
            other => panic!("{sql}\nwas refused as {other:?}"),
        }
    }
}

#[test]
fn a_row_value_and_any_right_side_of_a_distance_are_read_and_refused_when_planned() {
    // Each is read by the parser, and refused by the planner in a sentence that names it.
    // The refusal of ROW_VECTOR(...) names the function and the `<=>` it stands beside.
    let names: [(&str, &[&str]); 4] = [
        ("SELECT id FROM t WHERE (a, b) = (1, 2)", &["row value"]),
        (
            "SELECT id FROM t WHERE (a, b) IN ((1, 2, 3))",
            &["row value"],
        ),
        ("SELECT id FROM t ORDER BY v <=> w LIMIT 3", &["<=>"]),
        (
            "SELECT id FROM t ORDER BY v <=> ROW_VECTOR('t', 'v', 1) LIMIT 3",
            &["<=>", "row_vector"],
        ),
    ];
    for (sql, names) in names {
        let read = parse(sql).unwrap_or_else(|error| panic!("{sql}\nwas not read: {error:?}"));
        match plan(&read) {
            Err(Error::PlanError(text)) => {
                for name in names {
                    assert!(text.to_lowercase().contains(name), "{sql}: {text}");
                }
            }
            other => panic!("{sql}\nwas planned as {other:?}"),
        }
    }
}

#[test]
fn the_clauses_of_a_vector_column_and_in_lists_are_read() {
    let Statement::CreateTable(table) = parse(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, ctx TEXT NOT NULL, kind INT NOT NULL, \
         plain VECTOR(3), v VECTOR(4) PARTITION_KEY (ctx, kind) MAX_PARTITIONS 8 \
         SEARCH_MODE EXACT NOT NULL, w VECTOR(2) partition_key (ctx) max_partitions -1, \
         x VECTOR(2) SEARCH_MODE indexed)",
    )
    .unwrap() else {
        panic!("not a CREATE TABLE");
    };
    let clauses: Vec<&VectorClauses> = table.columns.iter().map(|column| &column.vector).collect();
    let keyed = |key: &[&str], max: Option<&str>, mode: Option<VectorSearchMode>| VectorClauses {
        partition_key: key.iter().map(|column| (*column).to_owned()).collect(),
        max_partitions: max.map(str::to_owned),
        search_mode: mode,
        ..VectorClauses::default()
    };
    let none = VectorClauses::default();
    assert_eq!(
        clauses,
        [
            &none,
            &none,
            &none,
            &none,
            &keyed(&["ctx", "kind"], Some("8"), Some(VectorSearchMode::Exact)),
            // The number is kept as it was written: it is `schema` that judges it.
            &keyed(&["ctx"], Some("-1"), None),
            &keyed(&[], None, Some(VectorSearchMode::Indexed)),
        ]
    );
    assert_eq!(table.columns[4].data_type, DataType::Vector(4));
    assert!(!table.columns[4].nullable && table.columns[3].nullable);

    // IN and NOT IN over a list, and over a list of row values.
    let filter = |sql: &str| {
        let read = select(&format!("SELECT id FROM t WHERE {sql}"));
        read.where_clause.expect("a WHERE")
    };
    assert!(matches!(
        filter("a IN (1, 2, $p)"),
        Expr::InList { expr, list, negated: false }
            if matches!(&*expr, Expr::Column(_)) && list.len() == 3
    ));
    assert!(matches!(
        filter("a not in ('x')"),
        Expr::InList { list, negated: true, .. } if list.len() == 1
    ));
    assert!(matches!(
        filter("(a, b) IN ((1, 'x'), (2, 'y'))"),
        Expr::InList { expr, list, negated: false }
            if matches!(&*expr, Expr::Row(row) if row.len() == 2)
                && list.len() == 2
                && list.iter().all(|item| matches!(item, Expr::Row(row) if row.len() == 2))
    ));
    // A list is an operand of AND and OR like any comparison.
    assert!(matches!(
        filter("a IN (1, 2) AND NOT b IN (3) OR c = 4"),
        Expr::BinaryOp { op: BinOp::Or, .. }
    ));
}

#[test]
fn ten_thousand_operands_of_and_and_or_are_read_in_the_order_written() {
    /// The numbers the comparisons of a tree of ANDs and ORs hold, from left to right.
    fn numbers(expr: &Expr, found: &mut Vec<i64>) {
        match expr {
            Expr::BinaryOp {
                left,
                op: BinOp::And | BinOp::Or,
                right,
            } => {
                numbers(left, found);
                numbers(right, found);
            }
            Expr::BinaryOp { right, .. } => match &**right {
                Expr::Literal(Literal::Integer(number)) => found.push(*number),
                other => panic!("a comparison with {other:?}"),
            },
            other => panic!("an operand that is no comparison: {other:?}"),
        }
    }
    for operator in ["AND", "OR"] {
        let operands: Vec<String> = (0..10_000).map(|n| format!("a = {n}")).collect();
        let sql = format!(
            "SELECT id FROM t WHERE {}",
            operands.join(&format!(" {operator} "))
        );
        let filter = select(&sql).where_clause.expect("a WHERE");
        let mut found = Vec::new();
        numbers(&filter, &mut found);
        assert!(found.iter().copied().eq(0..10_000), "{operator}");
    }
}

#[test]
fn the_index_clauses_of_a_vector_column_are_read_as_written() {
    let Statement::CreateTable(table) = parse(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, \
         a VECTOR(4) WITH (quantization = 'sq8') PARTITION_KEY (id) MAX_PARTITIONS 2 \
         SEARCH_MODE AUTO AUTO_INDEX_AT -5 \
         CONSOLIDATION (TOMBSTONE_PERCENT = 0, CHANGE_PERCENT = 101), \
         b VECTOR(4) auto_index_at 18446744073709551616 consolidation none, \
         c VECTOR(4) CONSOLIDATION (CHANGE_PERCENT = 7))",
    )
    .unwrap() else {
        panic!("not a CREATE TABLE");
    };
    // Every number and the quantization's text are kept as written: `schema` judges them.
    let written = |text: &str| Some(text.to_owned());
    let wanted = [
        VectorClauses::default(),
        VectorClauses {
            partition_key: vec!["id".to_owned()],
            max_partitions: written("2"),
            search_mode: Some(VectorSearchMode::Auto),
            auto_index_at: written("-5"),
            consolidation: Some(ConsolidationClause::Percent {
                change: written("101"),
                tombstone: written("0"),
            }),
            quantization: written("sq8"),
        },
        VectorClauses {
            auto_index_at: written("18446744073709551616"),
            consolidation: Some(ConsolidationClause::None),
            ..VectorClauses::default()
        },
        VectorClauses {
            consolidation: Some(ConsolidationClause::Percent {
                change: written("7"),
                tombstone: None,
            }),
            ..VectorClauses::default()
        },
    ];
    let read: Vec<VectorClauses> = table.columns.into_iter().map(|c| c.vector).collect();
    assert_eq!(read, wanted);

    // The clauses stand in one order, and a CONSOLIDATION list names one or both of its two
    // members, each once.
    for clauses in [
        "AUTO_INDEX_AT 5 SEARCH_MODE AUTO",
        "CONSOLIDATION NONE AUTO_INDEX_AT 5",
        "SEARCH_MODE AUTO WITH (quantization = 'SQ8')",
        "CONSOLIDATION ()",
        "CONSOLIDATION (CHANGE_PERCENT = 1, CHANGE_PERCENT = 2)",
        "CONSOLIDATION (PROBES = 1)",
        "AUTO_INDEX_AT",
    ] {
        refusal(&format!(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v VECTOR(4) {clauses})"
        ));
    }
}

#[test]
fn the_inspection_statements_are_planned_as_reads() {
    assert!(matches!(
        planned("show vector_indexes"),
        PhysicalPlan::ShowVectorIndexes
    ));
    let partitions = |sql: &str| match planned(sql) {
        PhysicalPlan::ShowVectorPartitions {
            column,
            limit,
            offset,
        } => (column, limit, offset),
        other => panic!("{sql}\nwas planned as {other:?}"),
    };
    let docs_v = Some(("docs".to_owned(), "v".to_owned()));
    assert_eq!(partitions("SHOW VECTOR_PARTITIONS"), (None, None, 0));
    assert_eq!(
        partitions("show vector_partitions for docs.v"),
        (docs_v.clone(), None, 0)
    );
    assert_eq!(
        partitions("SHOW VECTOR_PARTITIONS LIMIT 3"),
        (None, Some(3), 0)
    );
    assert_eq!(
        partitions("SHOW VECTOR_PARTITIONS FOR docs.v LIMIT 5 OFFSET 2"),
        (docs_v, Some(5), 2)
    );
    for sql in ["SHOW VECTOR_INDEXES", "SHOW VECTOR_PARTITIONS LIMIT 1"] {
        assert_eq!(planned(sql).effect(), Effect::Reads, "{sql}");
    }
    // OFFSET stands only after LIMIT, and FOR names a table and a column.
    refusal("SHOW VECTOR_PARTITIONS OFFSET 2");
    refusal("SHOW VECTOR_PARTITIONS FOR docs");
    refusal("SHOW VECTOR_INDEXES FOR docs.v");
}
