//! The expression evaluator, and how two values compare and sort.

use std::cmp::Ordering;
use std::collections::HashMap;

use super::bind::bind;
use crate::schema::Table;
use crate::sql::Expr;
use crate::sql::ast::{BinOp, ColumnRef, Literal, UnaryOp};
use crate::types::{
    ColumnDef, ColumnType, Error, Result, Uuid, Value, heap_bytes, timestamp_millis,
};

#[cfg(test)]
mod tests;

/// The columns an expression has in reach: for each table of the statement, in the order their
/// values stand in a row, the name that qualifies its columns and the columns themselves.
#[derive(Default)]
pub(crate) struct Scope<'a> {
    relations: Vec<(&'a str, &'a [ColumnDef])>,
}

impl<'a> Scope<'a> {
    /// The scope of one table's columns.
    pub(crate) fn of(qualifier: &'a str, columns: &'a [ColumnDef]) -> Self {
        Scope {
            relations: vec![(qualifier, columns)],
        }
    }

    /// Where a column sits in a row, and its type. A name no table in reach has is
    /// `ColumnNotFound`, and so is a name qualified with anything but one of the tables: that
    /// refusal names the qualifier as it was written.
    pub(super) fn find(&self, column: &ColumnRef) -> Result<(usize, ColumnType)> {
        let mut before = 0;
        for (qualifier, columns) in &self.relations {
            let named = |declared: &ColumnDef| declared.name == column.column;
            if column
                .table
                .as_deref()
                .is_none_or(|table| table == *qualifier)
                && let Some(at) = columns.iter().position(named)
            {
                return Ok((before + at, columns[at].column_type));
            }
            before += columns.len();
        }
        Err(self.missing(column))
    }

    /// The refusal of a column that is not in reach, under the qualifier it was written with or
    /// else the first table.
    fn missing(&self, column: &ColumnRef) -> Error {
        let first = self.relations.first().map(|(qualifier, _)| *qualifier);
        Error::ColumnNotFound {
            table: column
                .table
                .as_deref()
                .or(first)
                .unwrap_or_default()
                .to_owned(),
            column: column.column.clone(),
        }
    }
}

/// What an expression is worked out with beside its row: the values the statement bound, the
/// statement's one time in milliseconds, which `NOW()` answers with, and the row's distance
/// when a similarity search found it.
pub(crate) struct EvalContext<'a> {
    params: &'a HashMap<String, Value>,
    now: i64,
    distance: Option<f64>,
}

impl<'a> EvalContext<'a> {
    pub(crate) fn new(params: &'a HashMap<String, Value>, now: i64) -> Self {
        EvalContext {
            params,
            now,
            distance: None,
        }
    }

    /// The context for a row a similarity search found at this distance: every `<=>` in an
    /// expression is that number as it is.
    pub(super) fn found(self, distance: Option<f64>) -> Self {
        EvalContext { distance, ..self }
    }
}

/// Works out one expression against one row. The only expression evaluator in the library.
///
/// `row` holds one value per column of `scope`, in its order. A comparison with NULL, and one
/// between values that do not compare, is unknown: it gives NULL, and a filter keeps a row
/// only for TRUE.
///
/// A distance, `column <=> vector`, has no value of its own here: only a similarity search
/// gives a row one, through the context, so without one it is refused by name.
pub(crate) fn eval(expr: &Expr, scope: &Scope, row: &[Value], ctx: &EvalContext) -> Result<Value> {
    let value = |expr: &Expr| eval(expr, scope, row, ctx);
    Ok(match expr {
        Expr::Column(column) => {
            let (at, _) = scope.find(column)?;
            let value = row.get(at).cloned();
            value.ok_or_else(|| scope.missing(column))?
        }
        Expr::Literal(literal) => match literal {
            Literal::Null => Value::Null,
            Literal::Bool(value) => Value::Bool(*value),
            Literal::Integer(value) => Value::Int64(*value),
            Literal::Real(value) => Value::Float64(*value),
            Literal::Text(value) => Value::Text(value.clone()),
            Literal::Vector(value) => Value::Vector(value.clone()),
        },
        Expr::Parameter(name) => ctx.params.get(name).cloned().ok_or_else(|| unbound(name))?,
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
            (Some(Scalar::Now), []) => Value::Timestamp(ctx.now),
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
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            // TRUE when an item equals the left side, else unknown when any equality was.
            let left = side(expr, value)?;
            let mut found = Some(false);
            for item in list {
                let item = side(item, value)?;
                if item.len() != left.len() {
                    return Err(Error::PlanError(
                        "a row value beside IN needs as many values as its partner".to_owned(),
                    ));
                }
                match rows_equal(&left, &item) {
                    Some(true) => {
                        found = Some(true);
                        break;
                    }
                    Some(false) => {}
                    None => found = None,
                }
            }
            found.map_or(Value::Null, |found| Value::Bool(found != *negated))
        }
        Expr::Row(_) => {
            return Err(Error::PlanError(
                "a row value, (a, b), stands only on either side of IN".to_owned(),
            ));
        }
        Expr::Distance { .. } => Value::Float64(ctx.distance.ok_or_else(|| {
            Error::PlanError(
                "<=> has a value only for the rows of a similarity search, SELECT ... ORDER BY \
                 column <=> vector LIMIT k, and in that statement's select list"
                    .to_owned(),
            )
        })?),
    })
}

/// The values of one side of IN: those of a row value, or the one value of anything else.
fn side(expr: &Expr, value: impl Fn(&Expr) -> Result<Value>) -> Result<Vec<Value>> {
    match expr {
        Expr::Row(items) => items.iter().map(value).collect(),
        one => Ok(vec![value(one)?]),
    }
}

/// Whether two row values of one length are equal, as standard SQL has it: FALSE as soon as one
/// pair of columns is definitely different, whatever a NULL beside it holds; TRUE when every
/// pair is equal; otherwise unknown. One value is a row of one column.
fn rows_equal(left: &[Value], right: &[Value]) -> Option<bool> {
    let mut equal = Some(true);
    for (left, right) in left.iter().zip(right) {
        match compare(left, right) {
            Some(Ordering::Equal) => {}
            Some(_) => return Some(false),
            None => equal = None,
        }
    }
    equal
}

/// The most memory the value of an expression can take outside its own cell. It is known
/// before the expression is worked out, so the memory is asked for before the value is made.
pub(super) fn most_bytes(
    expr: &Expr,
    scope: &Scope,
    row: &[Value],
    params: &HashMap<String, Value>,
) -> u64 {
    match expr {
        Expr::Column(column) => {
            let at = scope.find(column).ok();
            at.and_then(|(at, _)| row.get(at)).map_or(0, heap_bytes)
        }
        Expr::Literal(Literal::Text(text)) => text.len() as u64,
        Expr::Literal(Literal::Vector(vector)) => size_of_val(vector.as_slice()) as u64,
        Expr::Parameter(name) => params.get(name).map_or(0, heap_bytes),
        Expr::FunctionCall { name, args } if matches!(scalar(name), Some(Scalar::Coalesce)) => {
            let each = args.iter().map(|arg| most_bytes(arg, scope, row, params));
            each.max().unwrap_or(0)
        }
        // Everything else gives a number, a truth, a time or NULL.
        _ => 0,
    }
}

/// Binds the expression against the table, then marks in `read` each column it reads. The
/// column on the left of a distance is not marked: the search gives a row's distance.
pub(crate) fn reads(
    expr: &Expr,
    table: &Table,
    params: &HashMap<String, Value>,
    read: &mut [bool],
) -> Result<()> {
    let scope = table.scope();
    bind(expr, &scope, params)?;
    mark(expr, &scope, read);
    Ok(())
}

fn mark(expr: &Expr, scope: &Scope, read: &mut [bool]) {
    let mut all = |items: &[Expr]| items.iter().for_each(|item| mark(item, scope, read));
    match expr {
        Expr::Column(column) => {
            if let Ok((at, _)) = scope.find(column) {
                read[at] = true;
            }
        }
        Expr::Literal(_) | Expr::Parameter(_) => {}
        Expr::BinaryOp { left, right, .. } => {
            mark(left, scope, read);
            mark(right, scope, read);
        }
        Expr::UnaryOp { operand, .. }
        | Expr::IsNull { expr: operand, .. }
        | Expr::Distance { right: operand, .. } => mark(operand, scope, read),
        Expr::FunctionCall { args: items, .. } | Expr::Row(items) => all(items),
        Expr::InList { expr, list, .. } => {
            all(list);
            mark(expr, scope, read);
        }
    }
}

/// Where a column sits in a row of its table.
pub(super) fn column_index(table: &Table, column: &ColumnRef) -> Result<usize> {
    Ok(table.scope().find(column)?.0)
}

pub(super) fn unbound(name: &str) -> Error {
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
pub(super) fn refused(name: &str) -> Error {
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
/// compares with a REAL as a REAL, and a UUID or a TIMESTAMP with the text that spells one.
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
        (Value::Timestamp(left), Value::Text(right)) => left.cmp(&timestamp_millis(right)?),
        (Value::Text(left), Value::Timestamp(right)) => timestamp_millis(left)?.cmp(right),
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
