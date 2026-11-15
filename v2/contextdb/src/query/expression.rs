//! The expression evaluator, and how two values compare and sort.

use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};

use serde_json::{Number, Value as Json};

use super::bind::bind;
use crate::schema::Table;
use crate::sql::ast::{BinOp, ColumnRef, Literal, Select, UnaryOp};
use crate::sql::{Expr, Plan, not_supported};
use crate::types::{
    ColumnDef, ColumnType, Error, Result, Value, heap_bytes, timestamp_millis, vector_from_text,
};

/// The columns an expression has in reach: for each table of the statement, in the order their
/// values stand in a row, the name that qualifies its columns and the columns themselves.
#[derive(Default, Clone)]
pub(crate) struct Scope<'a> {
    relations: Vec<(&'a str, &'a [ColumnDef])>,
    /// Whether the statement's own names meet here, so a name two columns answer to is refused.
    pub(super) checked: bool,
    /// The `$name`s an explained search binds nothing to, each of no known type.
    pub(super) open: Vec<String>,
}

impl<'a> Scope<'a> {
    /// The scope of one table's columns.
    pub(crate) fn of(qualifier: &'a str, columns: &'a [ColumnDef]) -> Self {
        Scope::default().and(qualifier, columns)
    }

    /// This scope with one more relation after it: the columns of a row of both, those of
    /// the new relation last.
    pub(crate) fn and(mut self, qualifier: &'a str, columns: &'a [ColumnDef]) -> Self {
        self.relations.push((qualifier, columns));
        self
    }

    /// Every column in reach, in the order their values stand in a row.
    pub(super) fn columns(&self) -> impl Iterator<Item = &'a ColumnDef> {
        self.relations.iter().flat_map(|(_, columns)| *columns)
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

    /// As `find`, for binding: where the statement's own names can clash, a name that more than
    /// one column in reach answers to is refused, as Postgres refuses it.
    pub(super) fn one(&self, column: &ColumnRef) -> Result<(usize, ColumnType)> {
        let (table, name) = (column.table.as_deref(), &column.column);
        let reached = self.relations.iter();
        let reached = reached.filter(|(at, _)| table.is_none_or(|table| table == *at));
        let named = reached.flat_map(|(_, columns)| columns.iter());
        if self.checked && named.filter(|declared| declared.name == *name).count() > 1 {
            let refused = format!("column reference \"{name}\" is ambiguous");
            return Err(Error::PlanError(refused));
        }
        self.find(column)
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
/// statement's one time in milliseconds, which `NOW()` answers with, and the values of the
/// statement's subqueries, each worked out once before any row; and, when the statement is
/// explained and not run, the list each choice it makes is said into and the statement.
#[derive(Clone, Copy)]
pub(crate) struct EvalContext<'a> {
    pub(super) params: &'a HashMap<String, Value>,
    now: i64,
    subqueries: &'a [(usize, Vec<Value>)],
    pub(super) said: Option<&'a RefCell<Vec<String>>>,
    pub(super) explained: Option<&'a Plan>,
}

impl<'a> EvalContext<'a> {
    pub(crate) fn new(params: &'a HashMap<String, Value>, now: i64) -> Self {
        EvalContext {
            params,
            now,
            subqueries: &[],
            said: None,
            explained: None,
        }
    }

    /// The statement's one time in milliseconds.
    pub(crate) fn now(&self) -> i64 {
        self.now
    }

    /// The context of a statement whose subqueries gave these values, each under the place its
    /// text begins in the statement: one value for each row of each.
    pub(crate) fn over(self, subqueries: &'a [(usize, Vec<Value>)]) -> Self {
        EvalContext { subqueries, ..self }
    }

    /// The values of one subquery of the statement, found by its place, which every copy of
    /// it keeps.
    pub(super) fn subquery(&self, query: &Select) -> Result<&'a [Value]> {
        let found = self.subqueries.iter().find(|(at, _)| *at == query.at.0);
        found.map(|(_, values)| values.as_slice()).ok_or_else(|| {
            Error::PlanError("a subquery has values only where its statement runs it".to_owned())
        })
    }
}

/// Works out one expression against one row. The only expression evaluator in the library.
///
/// `row` holds one value per column of `scope`, in its order. A comparison with NULL, and one
/// between values that do not compare, is unknown: it gives NULL, and a filter keeps a row
/// only for TRUE.
///
/// A distance, `left <=> right`, is worked out from the two vectors as the vector part works it
/// out, so a row's distance is the same number wherever it is written; NULL when either side is.
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
                BinOp::Eq | BinOp::Neq
                    if matches!((&**left, &**right), (Expr::Row(_), _) | (_, Expr::Row(_))) =>
                {
                    let equal = rows_equal(&side(left, value)?, &side(right, value)?);
                    return Ok(equal.map_or(Value::Null, |equal| {
                        Value::Bool(equal == (*op == BinOp::Eq))
                    }));
                }
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
            let items = list.iter().map(|item| side(item, value));
            among(&side(expr, value)?, items, *negated)?
        }
        Expr::InSubquery {
            expr,
            query,
            negated,
        } => {
            let items = ctx.subquery(query)?.iter().map(std::slice::from_ref);
            among(&[value(expr)?], items.map(Ok), *negated)?
        }
        // A subquery that found no row is NULL.
        Expr::Subquery(query) => ctx.subquery(query)?.first().cloned().unwrap_or(Value::Null),
        Expr::Row(_) => {
            return Err(Error::PlanError(
                "a row value, (a, b), stands only on either side of =, <> or IN".to_owned(),
            ));
        }
        Expr::Distance { left, right } => match (numbers(value(left)?)?, numbers(value(right)?)?) {
            (Some(left), Some(right)) if left.len() == right.len() => {
                Value::Float64(crate::vector::distance(&left, &right))
            }
            (Some(_), Some(_)) => {
                return Err(Error::PlanError(
                    "<=> needs two vectors of one length".to_owned(),
                ));
            }
            _ => Value::Null,
        },
        Expr::Like {
            expr,
            pattern,
            negated,
        } => match (value(expr)?, value(pattern)?) {
            (Value::Text(text), Value::Text(pattern)) => {
                Value::Bool(like(&text, &pattern) != *negated)
            }
            _ => Value::Null,
        },
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let middle = value(expr)?;
            let above = compare(&middle, &value(low)?).map(Ordering::is_ge);
            let below = compare(&middle, &value(high)?).map(Ordering::is_le);
            match (above, below) {
                (Some(false), _) | (_, Some(false)) => Value::Bool(*negated),
                (Some(true), Some(true)) => Value::Bool(!*negated),
                _ => Value::Null,
            }
        }
        Expr::Field { json, key, as_text } => {
            let field = match (value(json)?, value(key)?) {
                (Value::Json(json), Value::Text(key)) => json.get(key.as_str()).cloned(),
                (Value::Json(json), Value::Int64(place)) => {
                    let place = usize::try_from(place).ok();
                    place.and_then(|place| json.get(place).cloned())
                }
                _ => None,
            };
            match field {
                None => Value::Null,
                Some(field) if !*as_text => Value::Json(field),
                Some(serde_json::Value::Null) => Value::Null,
                Some(serde_json::Value::String(text)) => Value::Text(text),
                Some(field) => Value::Text(field.to_string()),
            }
        }
        Expr::RowNumber { .. } => {
            return Err(Error::PlanError(
                "ROW_NUMBER() has a value only for the rows its window numbered".to_owned(),
            ));
        }
    })
}

/// Whether `left` is among `items`: TRUE when one of them equals it, else unknown when any
/// equality was.
fn among<T: AsRef<[Value]>>(
    left: &[Value],
    items: impl Iterator<Item = Result<T>>,
    negated: bool,
) -> Result<Value> {
    let mut found = Some(false);
    for item in items {
        let item = item?;
        if item.as_ref().len() != left.len() {
            return Err(Error::PlanError(
                "a row value beside IN needs as many values as its partner".to_owned(),
            ));
        }
        match rows_equal(left, item.as_ref()) {
            Some(true) => return Ok(Value::Bool(!negated)),
            Some(false) => {}
            None => found = None,
        }
    }
    Ok(found.map_or(Value::Null, |found| Value::Bool(found != negated)))
}

/// Whether a text matches a LIKE pattern: `%` stands for any run of characters, `_` for any
/// one, and the character after a backslash for itself.
fn like(text: &str, pattern: &str) -> bool {
    let (mut text, mut pattern) = (text.chars(), pattern.chars());
    // The pattern after its last `%` so far, and the text that `%` has not yet taken.
    let mut retry: Option<(std::str::Chars, std::str::Chars)> = None;
    loop {
        let (mut after, mut rest) = (pattern.clone(), text.clone());
        let matched = match after.next() {
            Some('%') => {
                retry = Some((after.clone(), text.clone()));
                pattern = after;
                continue;
            }
            None if rest.next().is_none() => return true,
            None => false,
            Some('_') => rest.next().is_some(),
            Some(written) => {
                let exact = if written == '\\' { after.next() } else { None };
                rest.next() == Some(exact.unwrap_or(written))
            }
        };
        if matched {
            (pattern, text) = (after, rest);
        } else {
            // The last `%` takes one more character, if the text has one.
            let Some((after, from)) = &mut retry else {
                return false;
            };
            if from.next().is_none() {
                return false;
            }
            (pattern, text) = (after.clone(), from.clone());
        }
    }
}

/// Whether `expr`, or an expression inside it, is one `is` answers true for.
pub(super) fn holds(expr: &Expr, is: fn(&Expr) -> bool) -> bool {
    let inner = crate::sql::inside(expr);
    is(expr) || inner.into_iter().any(|inner| holds(inner, is))
}

/// The numbers of a side of `<=>`: a vector, or a text that spells one, such as '[1, 0]'; `None`
/// for NULL. Anything else is refused.
pub(super) fn numbers(value: Value) -> Result<Option<Vec<f32>>> {
    let vector = match value {
        Value::Null => return Ok(None),
        Value::Vector(vector) => Some(vector),
        Value::Text(text) => vector_from_text(&text),
        _ => None,
    };
    let refused = "<=> needs a vector on both sides: write [1.0, 0.0], its text '[1, 0]', or a \
                   $name bound to a vector";
    vector
        .map(Some)
        .ok_or_else(|| Error::PlanError(refused.to_owned()))
}

/// The values of one side of IN: those of a row value, or the one value of anything else.
fn side(expr: &Expr, value: impl Fn(&Expr) -> Result<Value>) -> Result<Vec<Value>> {
    expr.sides().iter().map(value).collect()
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
pub(super) fn most_bytes(expr: &Expr, scope: &Scope, row: &[Value], ctx: &EvalContext) -> u64 {
    match expr {
        Expr::Column(column) => {
            let at = scope.find(column).ok();
            at.and_then(|(at, _)| row.get(at)).map_or(0, heap_bytes)
        }
        Expr::Literal(Literal::Text(text)) => text.len() as u64,
        Expr::Literal(Literal::Vector(vector)) => size_of_val(vector.as_slice()) as u64,
        Expr::Parameter(name) => ctx.params.get(name).map_or(0, heap_bytes),
        Expr::FunctionCall { name, args } if matches!(scalar(name), Some(Scalar::Coalesce)) => {
            let each = args.iter().map(|arg| most_bytes(arg, scope, row, ctx));
            each.max().unwrap_or(0)
        }
        Expr::Subquery(query) => {
            let first = ctx.subquery(query).ok().and_then(|found| found.first());
            first.map_or(0, heap_bytes)
        }
        // A field is no larger than the JSON value it is taken from.
        Expr::Field { json, .. } => most_bytes(json, scope, row, ctx),
        // Everything else gives a number, a truth, a time or NULL.
        _ => 0,
    }
}

/// Binds the expression against the scope, then marks in `read`, which has one place for each
/// column of the scope, each column it reads.
pub(crate) fn reads(
    expr: &Expr,
    scope: &Scope,
    params: &HashMap<String, Value>,
    read: &mut [bool],
) -> Result<()> {
    bind(expr, scope, params)?;
    mark(expr, scope, read);
    Ok(())
}

pub(super) fn mark(expr: &Expr, scope: &Scope, read: &mut [bool]) {
    if let Expr::Column(column) = expr
        && let Ok((at, _)) = scope.find(column)
    {
        read[at] = true;
    }
    let inner = crate::sql::inside(expr).into_iter();
    inner.for_each(|inner| mark(inner, scope, read));
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

/// The refusal of a call [`eval`] does not work out.
pub(super) fn refused(name: &str) -> Error {
    not_supported(&format!("the function {name}"))
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
/// compares with a REAL by exact value, and a UUID, a TIMESTAMP or a JSON value with the text that
/// spells one; two JSON values compare as [`json`] says. Two vectors compare number by number,
/// the shorter first when it is the other's beginning, as pgvector compares them; a vector
/// compares with the text that spells one.
pub(super) fn compare(left: &Value, right: &Value) -> Option<Ordering> {
    // Each pair of two types is written one way round; the other way round is its reverse.
    ordered(left, right).or_else(|| Some(ordered(right, left)?.reverse()))
}

fn ordered(left: &Value, right: &Value) -> Option<Ordering> {
    let reals = |left: f64, right: f64| {
        let unordered = || left.is_nan().cmp(&right.is_nan());
        left.partial_cmp(&right).unwrap_or_else(unordered)
    };
    // NaN above all, then the whole parts as 128-bit numbers (the cast saturates), then the rest.
    let mixed = |whole: i64, real: f64| {
        let parts = (real.is_nan(), real.trunc() as i128);
        let rest = || reals(0.0, real.fract());
        (false, i128::from(whole)).cmp(&parts).then_with(rest)
    };
    Some(match (left, right) {
        (Value::Int64(left), Value::Int64(right)) => left.cmp(right),
        (Value::Float64(left), Value::Float64(right)) => reals(*left, *right),
        (Value::Int64(left), Value::Float64(right)) => mixed(*left, *right),
        (Value::Text(left), Value::Text(right)) => left.cmp(right),
        (Value::Bool(left), Value::Bool(right)) => left.cmp(right),
        (Value::Timestamp(left), Value::Timestamp(right)) => left.cmp(right),
        (Value::TxId(left), Value::TxId(right)) => left.cmp(right),
        (Value::Uuid(left), Value::Uuid(right)) => left.cmp(right),
        (Value::Uuid(left), Value::Text(right)) => left.cmp(&right.parse().ok()?),
        (Value::Timestamp(left), Value::Text(right)) => left.cmp(&timestamp_millis(right)?),
        (Value::Json(left), Value::Json(right)) => json(left, right),
        (Value::Json(left), Value::Text(right)) => json(left, &serde_json::from_str(right).ok()?),
        (Value::Vector(left), Value::Vector(right)) => left.partial_cmp(right)?,
        (Value::Vector(left), Value::Text(right)) => left.partial_cmp(&vector_from_text(right)?)?,
        _ => return None,
    })
}

/// How two JSON values compare, as Postgres compares `jsonb`: null, then a string, a number, a
/// boolean, an array and an object; arrays and objects by their size, then item by item or pair
/// by pair, a key by its bytes; numbers by exact value however written, so `5` is `5.0`.
fn json(left: &Json, right: &Json) -> Ordering {
    let number = |n: &Number| n.as_i64().map(Value::Int64);
    let number = |n: &Number| number(n).unwrap_or(Value::Float64(n.as_f64().unwrap_or_default()));
    // What tells apart whole numbers that are one 64-bit real: their exact value.
    let exact = |n: &Number| n.as_i128().or(Some(n.as_f64()? as i128));
    // A kind's rank, going down.
    let kind = |j: &Json| (j.is_null(), j.is_string(), j.is_number(), j.is_boolean());
    let kind = |j: &Json| (kind(j), j.is_array());
    match (left, right) {
        (Json::Number(l), Json::Number(r)) => {
            sort_order(&number(l), &number(r)).then(exact(l).cmp(&exact(r)))
        }
        (Json::String(l), Json::String(r)) => l.cmp(r),
        (Json::Bool(l), Json::Bool(r)) => l.cmp(r),
        (Json::Array(l), Json::Array(r)) => l.len().cmp(&r.len()).then_with(|| {
            let mut items = l.iter().zip(r).map(|(l, r)| json(l, r));
            items.find(|order| order.is_ne()).unwrap_or(Ordering::Equal)
        }),
        (Json::Object(l), Json::Object(r)) => l.len().cmp(&r.len()).then_with(|| {
            // Pairs in the order Postgres stores them, a shorter key first.
            let [l, r] = [l, r].map(|pairs| {
                let pairs = pairs.iter().map(|(key, value)| ((key.len(), key), value));
                pairs.collect::<BTreeMap<_, _>>()
            });
            let pairs = l.iter().zip(&r);
            let mut pairs = pairs.map(|(l, r)| l.0.1.cmp(r.0.1).then_with(|| json(l.1, r.1)));
            pairs.find(|order| order.is_ne()).unwrap_or(Ordering::Equal)
        }),
        _ => kind(right).cmp(&kind(left)),
    }
}

/// How two values sort going up: as they compare, with NULL after every value. Going down is
/// the reverse, which puts NULL first. Two values that do not compare are the same only when
/// they are equal; any others are told apart by how each is written, so two different values
/// never sort as one.
pub(super) fn sort_order(left: &Value, right: &Value) -> Ordering {
    match (left, right) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Greater,
        (_, Value::Null) => Ordering::Less,
        _ => compare(left, right).unwrap_or_else(|| match left == right {
            true => Ordering::Equal,
            false => format!("{left:?}").cmp(&format!("{right:?}")),
        }),
    }
}
