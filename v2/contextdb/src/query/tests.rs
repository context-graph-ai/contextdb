use std::cmp::Ordering;
use std::collections::HashMap;

use super::expression::{Arithmetic, arithmetic, compare, eval, sort_order};
use crate::schema::Table;
use crate::sql::Expr;
use crate::sql::ast::{BinOp, Literal, UnaryOp};
use crate::store::TableId;
use crate::types::{Error, TableMeta, Uuid, Value};

/// An expression that reads no column, worked out.
fn worked_out(expr: &Expr) -> Value {
    let table = Table {
        id: TableId(1),
        name: "t".to_owned(),
        meta: TableMeta {
            columns: Vec::new(),
            unique_constraints: Vec::new(),
            indexes: Vec::new(),
        },
        index_ids: Vec::new(),
    };
    eval(expr, &table, &[], &HashMap::new(), 0).expect("an expression of literals")
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

#[test]
fn arithmetic_keeps_whole_numbers_whole_and_refuses_what_does_not_fit() {
    let (int, real) = (Value::Int64, Value::Float64);
    let ok = |op, left: &Value, right: &Value| arithmetic(op, left, right).expect("arithmetic");
    assert_eq!(ok(Arithmetic::Div, &int(7), &int(2)), int(3));
    assert_eq!(ok(Arithmetic::Mul, &int(7), &real(0.5)), real(3.5));
    assert_eq!(ok(Arithmetic::Sub, &real(1.5), &int(1)), real(0.5));
    assert_eq!(ok(Arithmetic::Add, &Value::Null, &int(1)), Value::Null);
    for (op, left, right) in [
        (Arithmetic::Add, int(i64::MAX), int(1)),
        (Arithmetic::Mul, int(i64::MIN), int(-1)),
        (Arithmetic::Div, int(7), int(0)),
        (Arithmetic::Div, real(7.0), real(0.0)),
        (Arithmetic::Add, int(7), Value::Text("x".to_owned())),
    ] {
        let refused = arithmetic(op, &left, &right);
        assert!(
            matches!(refused, Err(Error::PlanError(_))),
            "{left:?} and {right:?} gave {refused:?}"
        );
    }
}

#[test]
fn values_sort_with_nan_above_every_number_and_null_after_everything() {
    let mut values = [
        Value::Null,
        Value::Float64(f64::NAN),
        Value::Float64(f64::MAX),
        Value::Int64(3),
        Value::Float64(-1.5),
    ];
    values.sort_by(sort_order);
    assert_eq!(
        values[..3],
        [
            Value::Float64(-1.5),
            Value::Int64(3),
            Value::Float64(f64::MAX)
        ]
    );
    assert!(matches!(values[3], Value::Float64(nan) if nan.is_nan()));
    assert_eq!(values[4], Value::Null);
}

#[test]
fn a_uuid_compares_with_the_text_that_spells_it() {
    let uuid = Value::Uuid(Uuid::from_u128(0x550e_8400_e29b_41d4_a716_4466_5544_0000));
    let spelled = |text: &str| Value::Text(text.to_owned());
    assert_eq!(
        compare(&uuid, &spelled("550E8400-E29B-41D4-A716-446655440000")),
        Some(Ordering::Equal)
    );
    assert_eq!(compare(&spelled("not a uuid"), &uuid), None);
    assert_eq!(compare(&uuid, &Value::Int64(1)), None);
}

#[test]
fn a_real_compares_as_a_key_holds_it() {
    let real = Value::Float64;
    assert_eq!(compare(&real(-0.0), &real(0.0)), Some(Ordering::Equal));
    assert_eq!(
        compare(&real(f64::NAN), &real(-f64::NAN)),
        Some(Ordering::Equal)
    );
    assert_eq!(
        compare(&real(-f64::NAN), &real(f64::INFINITY)),
        Some(Ordering::Greater)
    );
    assert_eq!(compare(&Value::Int64(2), &real(2.5)), Some(Ordering::Less));
}
