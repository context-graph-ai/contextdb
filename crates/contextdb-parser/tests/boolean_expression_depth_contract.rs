use contextdb_core::Error;
use contextdb_parser::{BinOp, Expr, Literal, Statement, parse};

// Acceptance: a large boolean list keeps every ordered operand and can be destroyed
// on the standard test-thread stack. The pre-fix left-fold tree aborts on drop.
#[test]
fn ten_thousand_boolean_operands_parse_in_order_and_drop_on_two_mib_stack() {
    std::thread::Builder::new()
        .stack_size(2 * 1024 * 1024)
        .spawn(|| {
            for (sql_op, expected_op) in [("AND", BinOp::And), ("OR", BinOp::Or)] {
                let chain = (0..10_000)
                    .map(|n| format!("value = {n}"))
                    .collect::<Vec<_>>()
                    .join(&format!(" {sql_op} "));
                // Source nesting and balanced AST depth have independent caps:
                // 120 parentheses plus a depth-14 boolean chain must still pass.
                for source_parentheses in [0, 120] {
                    let sql = format!(
                        "SELECT * FROM items WHERE {}{}{}",
                        "(".repeat(source_parentheses),
                        chain,
                        ")".repeat(source_parentheses)
                    );
                    let statement = parse(&sql).expect("large boolean chain");
                    let Statement::Select(select) = &statement else {
                        panic!("SELECT");
                    };
                    let mut stack = vec![(select.body.where_clause.as_ref().unwrap(), 0)];
                    let mut leaves = 0;
                    let mut nodes = 0;
                    let mut max_depth = 0;
                    while let Some((expr, depth)) = stack.pop() {
                        max_depth = max_depth.max(depth);
                        match expr {
                            Expr::BinaryOp { left, op, right } if *op == expected_op => {
                                nodes += 1;
                                stack.push((right, depth + 1));
                                stack.push((left, depth + 1));
                            }
                            Expr::BinaryOp {
                                left,
                                op: BinOp::Eq,
                                right,
                            } => {
                                assert!(matches!(&**left,
                                    Expr::Column(column) if column.column == "value"));
                                assert!(matches!(&**right,
                                    Expr::Literal(Literal::Integer(n)) if *n == leaves));
                                leaves += 1;
                            }
                            _ => panic!("unexpected boolean operand"),
                        }
                    }
                    assert_eq!(leaves, 10_000);
                    assert_eq!(nodes, 9_999);
                    assert_eq!(max_depth, 14);
                    drop(statement);
                }
            }
        })
        .unwrap()
        .join()
        .unwrap();
}

// Acceptance: unsafe recursive input is refused before pest with depth and limit;
// SQL data and comments cannot manufacture nesting. No existing parser test covers it.
#[test]
fn expression_depth_refusals_are_typed_and_ignore_sql_data() {
    std::thread::Builder::new()
        .stack_size(4 * 1024 * 1024)
        .spawn(|| {
            for expression in [
                format!("{}true{}", "(".repeat(129), ")".repeat(129)),
                format!("{}true", "NOT ".repeat(129)),
                format!("{}1", "- ".repeat(129)),
                (0..130).map(|_| "1").collect::<Vec<_>>().join(" + "),
                format!("{}NOT true{}", "(".repeat(128), ")".repeat(128)),
            ] {
                assert!(matches!(
                    parse(&format!("SELECT {expression} FROM items")),
                    Err(Error::ExpressionDepthExceeded {
                        depth: 129,
                        limit: 128
                    })
                ));
            }
            for expression in [
                format!("'{}'", "(NOT - ".repeat(200)),
                format!("\"{}\"", "(NOT - ".repeat(200)),
                format!("/* {} */ true", "(NOT - ".repeat(200)),
                format!("-- {}\n true", "(NOT - ".repeat(200)),
                format!("{}true{}", "(".repeat(128), ")".repeat(128)),
                format!("{}NOT true{}", "(".repeat(127), ")".repeat(127)),
                format!("{}true", "NOT ".repeat(128)),
                (0..129).map(|_| "1").collect::<Vec<_>>().join(" + "),
            ] {
                parse(&format!("SELECT {expression} FROM items")).expect("within nesting limit");
            }
        })
        .unwrap()
        .join()
        .unwrap();
}
