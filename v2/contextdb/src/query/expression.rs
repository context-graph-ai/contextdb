//! The expression evaluator, and how two values compare and sort.

use std::cmp::Ordering;
use std::collections::HashMap;

use super::heap_bytes;
use crate::schema::Table;
use crate::sql::Expr;
use crate::sql::ast::{BinOp, ColumnRef, Literal, UnaryOp};
use crate::types::{Error, Result, Uuid, Value};

/// Works out one expression against one row. The only expression evaluator in the library.
///
/// `row` holds one value per column of `table`, in declared order, and `now` is the statement's
/// one time in milliseconds, which `NOW()` answers with. A comparison with NULL, and one between
/// values of types that do not compare, is unknown: it gives NULL, and a filter keeps a row
/// only for TRUE.
pub(crate) fn eval(
    expr: &Expr,
    table: &Table,
    row: &[Value],
    params: &HashMap<String, Value>,
    now: i64,
) -> Result<Value> {
    let value = |expr: &Expr| eval(expr, table, row, params, now);
    Ok(match expr {
        Expr::Column(column) => {
            let at = column_index(table, column)?;
            row.get(at).cloned().ok_or_else(|| missing(table, column))?
        }
        Expr::Literal(literal) => match literal {
            Literal::Null => Value::Null,
            Literal::Bool(value) => Value::Bool(*value),
            Literal::Integer(value) => Value::Int64(*value),
            Literal::Real(value) => Value::Float64(*value),
            Literal::Text(value) => Value::Text(value.clone()),
            Literal::Vector(value) => Value::Vector(value.clone()),
        },
        Expr::Parameter(name) => params.get(name).cloned().ok_or_else(|| unbound(name))?,
        Expr::BinaryOp { left, op, right } => {
            let holds: fn(Ordering) -> bool = match op {
                BinOp::And | BinOp::Or => {
                    // The value that decides the answer alone: FALSE for AND, TRUE for OR.
                    let settles = *op == BinOp::Or;
                    let left = truth(&value(left)?);
                    if left == Some(settles) {
                        return Ok(Value::Bool(settles));
                    }
                    return Ok(match (left, truth(&value(right)?)) {
                        (_, Some(right)) if right == settles => Value::Bool(settles),
                        (Some(_), Some(_)) => Value::Bool(!settles),
                        _ => Value::Null,
                    });
                }
                BinOp::Eq => Ordering::is_eq,
                BinOp::Neq => Ordering::is_ne,
                BinOp::Lt => Ordering::is_lt,
                BinOp::Lte => Ordering::is_le,
                BinOp::Gt => Ordering::is_gt,
                BinOp::Gte => Ordering::is_ge,
            };
            compare(&value(left)?, &value(right)?)
                .map_or(Value::Null, |order| Value::Bool(holds(order)))
        }
        Expr::UnaryOp { op, operand } => match (op, value(operand)?) {
            (UnaryOp::Not, value) => truth(&value).map_or(Value::Null, |truth| Value::Bool(!truth)),
            (UnaryOp::Neg, Value::Null) => Value::Null,
            (UnaryOp::Neg, Value::Int64(value)) => {
                Value::Int64(value.checked_neg().ok_or_else(out_of_range)?)
            }
            (UnaryOp::Neg, Value::Float64(value)) => Value::Float64(-value),
            (UnaryOp::Neg, _) => {
                return Err(Error::PlanError(
                    "cannot negate a value that is not a number".to_owned(),
                ));
            }
        },
        Expr::IsNull { expr, negated } => Value::Bool((value(expr)? == Value::Null) != *negated),
        Expr::FunctionCall { name, args } => match (scalar(name), args.as_slice()) {
            (Some(Scalar::Arithmetic(op)), [left, right]) => {
                arithmetic(op, &value(left)?, &value(right)?)?
            }
            (Some(Scalar::Now), []) => Value::Timestamp(now),
            (Some(Scalar::Coalesce), args) => {
                for arg in args {
                    let first = value(arg)?;
                    if first != Value::Null {
                        return Ok(first);
                    }
                }
                Value::Null
            }
            _ => return Err(refused(name)),
        },
    })
}

/// The most memory the value of an expression can take outside its own cell. It is known
/// before the expression is worked out, so the memory is asked for before the value is made.
pub(super) fn most_bytes(
    expr: &Expr,
    table: &Table,
    row: &[Value],
    params: &HashMap<String, Value>,
) -> u64 {
    match expr {
        Expr::Column(column) => {
            let at = column_index(table, column).ok();
            at.and_then(|at| row.get(at)).map_or(0, heap_bytes)
        }
        Expr::Literal(Literal::Text(text)) => text.len() as u64,
        Expr::Literal(Literal::Vector(vector)) => size_of_val(vector.as_slice()) as u64,
        Expr::Parameter(name) => params.get(name).map_or(0, heap_bytes),
        Expr::FunctionCall { name, args } if matches!(scalar(name), Some(Scalar::Coalesce)) => {
            let each = args.iter().map(|arg| most_bytes(arg, table, row, params));
            each.max().unwrap_or(0)
        }
        // Everything else gives a number, a truth, a time or NULL.
        _ => 0,
    }
}

/// Marks in `read` each column the expression reads.
///
/// Refuses, before any row is read, what [`eval`] would refuse at the first row: a column the
/// table does not have, a `$name` nothing is bound to, and a form this version does not work
/// out. A statement is therefore refused or not whatever rows its table holds.
pub(super) fn reads(
    expr: &Expr,
    table: &Table,
    params: &HashMap<String, Value>,
    read: &mut [bool],
) -> Result<()> {
    match expr {
        Expr::Column(column) => read[column_index(table, column)?] = true,
        Expr::Literal(_) => {}
        Expr::Parameter(name) => {
            if !params.contains_key(name) {
                return Err(unbound(name));
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            reads(left, table, params, read)?;
            reads(right, table, params, read)?;
        }
        Expr::UnaryOp { operand, .. } | Expr::IsNull { expr: operand, .. } => {
            reads(operand, table, params, read)?;
        }
        Expr::FunctionCall { name, args } => {
            if scalar(name).is_none() {
                return Err(refused(name));
            }
            for arg in args {
                reads(arg, table, params, read)?;
            }
        }
    }
    Ok(())
}

/// Where a column sits in a row of its table. A name the table does not have, and a name
/// qualified with anything but the table's own name, is `ColumnNotFound`.
pub(super) fn column_index(table: &Table, column: &ColumnRef) -> Result<usize> {
    let in_scope = column.table.as_ref().is_none_or(|name| *name == table.name);
    table
        .meta
        .columns
        .iter()
        .position(|declared| in_scope && declared.name == column.column)
        .ok_or_else(|| missing(table, column))
}

fn missing(table: &Table, column: &ColumnRef) -> Error {
    Error::ColumnNotFound {
        table: table.name.clone(),
        column: column.column.clone(),
    }
}

fn unbound(name: &str) -> Error {
    Error::PlanError(format!("no value is bound to ${name}"))
}

fn out_of_range() -> Error {
    Error::PlanError("integer out of range".to_owned())
}

/// The refusal of something this version of the library does not do, by its name.
pub(super) fn unsupported(what: &str) -> Error {
    Error::PlanError(format!(
        "{what} is not supported in this version of the library"
    ))
}

/// The refusal of a call [`eval`] does not work out.
fn refused(name: &str) -> Error {
    unsupported(&format!("the function {name}"))
}

/// A function that gives one value for one row.
#[derive(Clone, Copy)]
pub(super) enum Scalar {
    Arithmetic(Arithmetic),
    Now,
    /// The first of its values that is not NULL.
    Coalesce,
}

/// The four operators as the parser names them: `a + b` arrives as the call `__add(a, b)`.
#[derive(Clone, Copy)]
pub(super) enum Arithmetic {
    Add,
    Sub,
    Mul,
    Div,
}

/// The scalar function of this name, if it is one.
pub(super) fn scalar(name: &str) -> Option<Scalar> {
    [
        ("__add", Scalar::Arithmetic(Arithmetic::Add)),
        ("__sub", Scalar::Arithmetic(Arithmetic::Sub)),
        ("__mul", Scalar::Arithmetic(Arithmetic::Mul)),
        ("__div", Scalar::Arithmetic(Arithmetic::Div)),
        ("now", Scalar::Now),
        ("coalesce", Scalar::Coalesce),
    ]
    .into_iter()
    .find(|(known, _)| name.eq_ignore_ascii_case(known))
    .map(|(_, scalar)| scalar)
}

/// Arithmetic as standard SQL does it: NULL in gives NULL out, two whole numbers give a whole
/// number or are refused when it would not fit, and a REAL on either side gives a REAL.
/// Dividing by zero is refused.
pub(super) fn arithmetic(op: Arithmetic, left: &Value, right: &Value) -> Result<Value> {
    let by_zero = || Error::PlanError("division by zero".to_owned());
    Ok(match (left, right) {
        (Value::Null, _) | (_, Value::Null) => Value::Null,
        (Value::Int64(left), Value::Int64(right)) => Value::Int64(
            match op {
                Arithmetic::Add => left.checked_add(*right),
                Arithmetic::Sub => left.checked_sub(*right),
                Arithmetic::Mul => left.checked_mul(*right),
                Arithmetic::Div if *right == 0 => return Err(by_zero()),
                Arithmetic::Div => left.checked_div(*right),
            }
            .ok_or_else(out_of_range)?,
        ),
        _ => {
            let (Some(left), Some(right)) = (real(left), real(right)) else {
                return Err(Error::PlanError(
                    "arithmetic needs numbers on both sides".to_owned(),
                ));
            };
            Value::Float64(match op {
                Arithmetic::Add => left + right,
                Arithmetic::Sub => left - right,
                Arithmetic::Mul => left * right,
                Arithmetic::Div if right == 0.0 => return Err(by_zero()),
                Arithmetic::Div => left / right,
            })
        }
    })
}

fn real(value: &Value) -> Option<f64> {
    match value {
        Value::Int64(value) => Some(*value as f64),
        Value::Float64(value) => Some(*value),
        _ => None,
    }
}

/// TRUE or FALSE, or `None` for NULL and for anything that is not a boolean.
fn truth(value: &Value) -> Option<bool> {
    match value {
        Value::Bool(value) => Some(*value),
        _ => None,
    }
}

/// How two values compare, or `None` when either is NULL or their types do not compare.
///
/// A REAL has one rule for `=`, ORDER BY and the bytes of a key: the two zeros are one value,
/// and every NaN is one value, equal to itself and above positive infinity. A whole number
/// compares with a REAL as a REAL, and a UUID with the text that spells one.
pub(super) fn compare(left: &Value, right: &Value) -> Option<Ordering> {
    let reals = |left: f64, right: f64| {
        let unordered = || left.is_nan().cmp(&right.is_nan());
        left.partial_cmp(&right).unwrap_or_else(unordered)
    };
    Some(match (left, right) {
        (Value::Int64(left), Value::Int64(right)) => left.cmp(right),
        (Value::Float64(left), Value::Float64(right)) => reals(*left, *right),
        (Value::Int64(left), Value::Float64(right)) => reals(*left as f64, *right),
        (Value::Float64(left), Value::Int64(right)) => reals(*left, *right as f64),
        (Value::Text(left), Value::Text(right)) => left.cmp(right),
        (Value::Bool(left), Value::Bool(right)) => left.cmp(right),
        (Value::Timestamp(left), Value::Timestamp(right)) => left.cmp(right),
        (Value::TxId(left), Value::TxId(right)) => left.cmp(right),
        (Value::Uuid(left), Value::Uuid(right)) => left.cmp(right),
        (Value::Uuid(left), Value::Text(right)) => left.cmp(&right.parse().ok()?),
        (Value::Text(left), Value::Uuid(right)) => left.parse::<Uuid>().ok()?.cmp(right),
        _ => return None,
    })
}

/// How two values sort going up: as they compare, with NULL after every value. Going down is
/// the reverse, which puts NULL first.
pub(super) fn sort_order(left: &Value, right: &Value) -> Ordering {
    match (left, right) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Greater,
        (_, Value::Null) => Ordering::Less,
        _ => compare(left, right).unwrap_or(Ordering::Equal),
    }
}
