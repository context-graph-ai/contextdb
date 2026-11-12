//! The evaluator itself, over expressions that read no column.

use std::collections::HashMap;

use super::eval;
use crate::query::{EvalContext, Scope};
use crate::sql::Expr;
use crate::sql::ast::{BinOp, Literal, UnaryOp};
use crate::types::Value;

/// An expression that reads no column, worked out.
fn worked_out(expr: &Expr) -> Value {
    let bound = HashMap::new();
    eval(expr, &Scope::default(), &[], &EvalContext::new(&bound, 0))
        .expect("an expression of literals")
}

fn joined(left: Literal, op: BinOp, right: Literal) -> Expr {
    Expr::BinaryOp {
        left: Box::new(Expr::Literal(left)),
        op,
        right: Box::new(Expr::Literal(right)),
    }
}

#[test]
fn null_is_unknown_unless_the_other_side_settles_the_answer() {
    let (null, yes, no) = (Literal::Null, Literal::Bool(true), Literal::Bool(false));
    let and = |left: &Literal, right: &Literal| {
        worked_out(&joined(left.clone(), BinOp::And, right.clone()))
    };
    let or = |left: &Literal, right: &Literal| {
        worked_out(&joined(left.clone(), BinOp::Or, right.clone()))
    };
    assert_eq!(and(&null, &no), Value::Bool(false));
    assert_eq!(and(&no, &null), Value::Bool(false));
    assert_eq!(and(&null, &yes), Value::Null);
    assert_eq!(and(&yes, &yes), Value::Bool(true));
    assert_eq!(or(&null, &yes), Value::Bool(true));
    assert_eq!(or(&yes, &null), Value::Bool(true));
    assert_eq!(or(&null, &no), Value::Null);
    assert_eq!(or(&no, &no), Value::Bool(false));

    let not_null = Expr::UnaryOp {
        op: UnaryOp::Not,
        operand: Box::new(Expr::Literal(Literal::Null)),
    };
    assert_eq!(worked_out(&not_null), Value::Null);
    let null_equals_null = joined(Literal::Null, BinOp::Eq, Literal::Null);
    assert_eq!(worked_out(&null_equals_null), Value::Null);
}
