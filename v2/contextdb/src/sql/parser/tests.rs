use super::parse;
use crate::sql::ast::{BinOp, ColumnRef, Expr, Literal, Select, Statement};
use crate::sql::plan;
use crate::types::Error;

fn refusal(sql: &str) -> String {
    match parse(sql) {
        Err(Error::ParseError(text)) => text,
        other => panic!("{sql}\nwas not refused as a parse error: {other:?}"),
    }
}

fn planned(sql: &str) -> Statement {
    let statement = parse(sql).unwrap_or_else(|error| panic!("{sql}\nwas refused: {error:?}"));
    plan(statement).unwrap_or_else(|error| panic!("{sql}\nwas not planned: {error:?}"))
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
fn what_a_later_version_reads_is_refused_by_its_name() {
    for (sql, name) in [
        ("BEGIN", "BEGIN"),
        ("purge FROM t WHERE id = 1", "PURGE"),
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
    assert!(refusal("SELECT a FROM t x y").contains("unexpected 'y'"));
    // A row value is read wherever an expression is. In a select list it is refused by name
    // when the statement is planned.
    let read = parse("SELECT (a, b) FROM t")
        .unwrap_or_else(|error| panic!("a row value in a select list was not read: {error:?}"));
    match plan(read) {
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
        let filter = select(&sql).filter.expect("a WHERE");
        let mut found = Vec::new();
        numbers(&filter, &mut found);
        assert!(found.iter().copied().eq(0..10_000), "{operator}");
    }
}
