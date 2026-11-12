//! How two values compare and sort, and arithmetic: the evaluator's pure functions.

use std::cmp::Ordering;

use super::expression::{Arithmetic, arithmetic, compare, sort_order};
use crate::types::{Error, Uuid, Value};

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
