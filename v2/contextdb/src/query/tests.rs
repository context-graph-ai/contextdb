//! How two reals compare: the evaluator's pure function.

use std::cmp::Ordering;

use super::expression::compare;
use crate::types::Value;

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
