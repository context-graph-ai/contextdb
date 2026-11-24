use crate::memory::{Budget, Owned, Reservation};
use crate::sql::ast::{BinOp, ColumnRef, Expr, Select, Statement};
use crate::sql::plan;
use crate::types::{Error, Result, Value};

fn parse(sql: &str) -> Result<(Statement, Reservation)> {
    crate::sql::parse(sql, &Budget::new(None).expect("a parser fixture budget"))
        .map(|(statement, held, _)| (statement, held))
}

fn refusal(sql: &str) -> String {
    match parse(sql).map(|(statement, held)| Owned::charged(statement, held)) {
        Err(Error::ParseError(text)) => text,
        other => panic!("{sql}\nwas not refused as a parse error: {other:?}"),
    }
}

fn planned(sql: &str) -> Owned<Statement> {
    let (statement, held) =
        parse(sql).unwrap_or_else(|error| panic!("{sql}\nwas refused: {error:?}"));
    let statement =
        plan(statement).unwrap_or_else(|error| panic!("{sql}\nwas not planned: {error:?}"));
    Owned::charged(statement, held)
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
    for (sql, name) in [("BEGIN", "BEGIN")] {
        match parse(sql).map(|(statement, held)| Owned::charged(statement, held)) {
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
    let (read, held) = parse("SELECT (a, b) FROM t")
        .unwrap_or_else(|error| panic!("a row value in a select list was not read: {error:?}"));
    match plan(read).map(|statement| Owned::charged(statement, held)) {
        Err(Error::PlanError(text)) => {
            assert!(text.to_lowercase().contains("row value"), "{text}");
        }
        other => panic!("a row value in a select list was planned as {other:?}"),
    }
}

fn select(sql: &str) -> Owned<Select> {
    let (statement, held) =
        parse(sql).unwrap_or_else(|error| panic!("{sql}\nwas refused: {error:?}"));
    match statement {
        Statement::Select(select) => Owned::charged(select, held),
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
                Expr::Literal(Value::Int64(number)) => found.push(*number),
                other => panic!("a comparison with {other:?}"),
            },
            other => panic!("an operand that is no comparison: {other:?}"),
        }
    }
    fn depth(expr: &Expr) -> usize {
        match expr {
            Expr::BinaryOp {
                left,
                op: BinOp::And | BinOp::Or,
                right,
            } => 1 + depth(left).max(depth(right)),
            _ => 1,
        }
    }
    for operator in ["AND", "OR"] {
        let operands: Vec<String> = (0..10_000).map(|n| format!("a = {n}")).collect();
        let sql = format!(
            "SELECT id FROM t WHERE {}",
            operands.join(&format!(" {operator} "))
        );
        let read = select(&sql);
        let filter = read.filter.as_ref().expect("a WHERE");
        let mut found = Vec::new();
        numbers(filter, &mut found);
        assert!(found.iter().copied().eq(0..10_000), "{operator}");
        assert!(depth(filter) <= 16, "{operator} is a balanced tree");
    }
}

#[test]
fn a_vector_component_with_an_exponent_is_charged_as_a_number_and_half_a_number_is_refused() {
    let vector = |number: &str| format!("SELECT [{}1]", format!("{number}, ").repeat(767));
    let charge = |number| super::charge(&vector(number));
    assert_eq!(charge("1e-05"), charge("0.001"), "one number per component");
    for part in ["1e", "1e+", ".", "1,,2"] {
        let text = refusal(&format!("SELECT [{part}]"));
        assert!(text.contains("syntax error"), "[{part}]: {text}");
    }
}

struct Counting;
thread_local! { static PARSED: std::cell::Cell<(isize, isize)> = const { std::cell::Cell::new((0, 0)) }; }
fn count(delta: isize) {
    let _ = PARSED.try_with(|v| v.set((v.get().0 + delta, v.get().1.max(v.get().0 + delta))));
}
#[global_allocator]
static COUNTING: Counting = Counting;
// SAFETY: each allocation and layout is forwarded unchanged to the system allocator.
unsafe impl std::alloc::GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
        count(layout.size() as isize);
        unsafe { std::alloc::System.alloc(layout) }
    }
    unsafe fn dealloc(&self, p: *mut u8, layout: std::alloc::Layout) {
        count(-(layout.size() as isize));
        unsafe { std::alloc::System.dealloc(p, layout) }
    }
    unsafe fn realloc(&self, p: *mut u8, layout: std::alloc::Layout, size: usize) -> *mut u8 {
        count(size as isize - layout.size() as isize);
        unsafe { std::alloc::System.realloc(p, layout, size) }
    }
}

// M5: a statement is charged what parsing it holds, never less than the most its parse has
// allocated at once and never more than twice that.
#[test]
fn a_statement_is_charged_at_least_what_parsing_it_allocates_and_at_most_twice() {
    let vector = format!("[{}]", ["0.5"; 768].join(", "));
    let rows: Vec<String> = (0..1000).map(|n| format!("({n}, 'row {n}')")).collect();
    for sql in [
        format!("SELECT id FROM t ORDER BY v <=> {vector} LIMIT 10"),
        format!("INSERT INTO t (id, body) VALUES {}", rows.join(", ")),
        format!(
            "INSERT INTO t (id, body) VALUES (1, '{}')",
            "x".repeat(30_000_000)
        ),
    ] {
        let budget = Budget::new(None).expect("a budget with no ceiling");
        let start = PARSED.with(|v| v.get().0);
        PARSED.with(|v| v.set((start, start)));
        let parsed = crate::sql::parse(&sql, &budget).expect("the statement parses");
        let peak = PARSED.with(|v| v.get().1 - start) as u64;
        let charged = parsed.1.bytes();
        let what = &sql[..40];
        assert!(
            peak <= charged,
            "{what}: charged {charged}, below the {peak} bytes it took"
        );
        assert!(
            charged <= 2 * peak,
            "{what}: charged {charged} for {peak} bytes"
        );
    }
}
