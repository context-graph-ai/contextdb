//! The expression evaluator, and how two values compare and sort.

use super::sets::List;
use std::borrow::Cow;
use std::cell::{Cell, RefCell};
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::io::{Cursor, Write};

use serde_json::{Number, Value as Json};

use super::bind::bind;
use crate::memory::{Passing, Reservation, node};
use crate::schema::Table;
use crate::sql::ast::{BinOp, ColumnRef, Select, UnaryOp};
use crate::sql::{Expr, Plan, not_supported};
use crate::types::{
    ColumnDef, ColumnType, Error, Result, Value, heap_bytes, json_bytes, timestamp_millis,
    vector_numbers,
};

#[cfg(test)]
#[path = "tests.rs"]
mod tests;

/// A table's columns as an expression reaches them: under the table's name, in row order.
pub(crate) fn scope(table: &Table) -> Scope<'_> {
    Scope::of(&table.name, &table.meta.columns)
}

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
        let bare = self
            .relations
            .last()
            .filter(|(q, _)| q.is_empty() && column.table.is_none());
        if let Some((_, own)) = bare
            && let Some(at) = own.iter().position(|c| c.name == column.column)
        {
            return Ok((self.columns().count() - own.len() + at, own[at].column_type));
        }
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

/// The statement's parsed comparison operands and simultaneously live computed values.
pub(crate) struct Kept {
    values: RefCell<Vec<(String, Json)>>,
    bytes: Cell<u64>,
    live: Cell<u64>,
    cover: RefCell<Passing>,
}
impl Kept {
    pub(crate) fn new(cover: Passing) -> Self {
        Self {
            values: RefCell::default(),
            bytes: Cell::new(0),
            live: Cell::new(0),
            cover: RefCell::new(cover),
        }
    }
    fn cover(&self, bytes: u64) -> Result<()> {
        self.cover
            .borrow_mut()
            .cover(self.bytes.get() + self.live.get() + bytes)
    }
    pub(super) fn keep(&self, bytes: u64) -> Result<()> {
        self.cover(bytes)?;
        self.bytes.set(self.bytes.get() + bytes);
        Ok(())
    }
    pub(super) fn with_json<T>(
        &self,
        expr: Option<&Expr>,
        text: &str,
        read: impl FnOnce(&Json) -> T,
    ) -> Result<T> {
        let Some(Expr::Literal(_) | Expr::Parameter(_)) = expr else {
            return Err(Error::PlanError(
                "cannot compare TEXT with JSON; cast the TEXT to JSON explicitly".to_owned(),
            ));
        };
        let mut entries = self.values.borrow_mut();
        let at = match entries.iter().position(|entry| entry.0 == text) {
            Some(at) => at,
            None => {
                let bytes =
                    json_bytes(text.as_bytes()) + text.len() as u64 + node::<(String, Json)>();
                self.keep(bytes)?;
                let value = serde_json::from_str(text).map_err(|_| {
                    Error::PlanError(
                        "TEXT beside JSON must contain valid JSON; use an explicit cast".to_owned(),
                    )
                })?;
                let at = entries.len();
                entries.push((text.to_owned(), value));
                at
            }
        };
        Ok(read(&entries[at].1))
    }
}

/// What an expression is worked out with beside its row: the values the statement bound, the
/// statement's one time in milliseconds, which `NOW()` answers with, and the values of the
/// statement's subqueries, each worked out once before any row; and, when the statement is
/// explained and not run, the list each choice it makes is said into and the statement.
#[derive(Clone, Copy)]
pub(crate) struct EvalContext<'a> {
    pub(super) params: &'a HashMap<String, Value>,
    pub(super) kept: &'a Kept,
    now: i64,
    subqueries: &'a [(usize, List)],
    pub(super) said: Option<&'a RefCell<Vec<String>>>,
    pub(super) held: Option<&'a RefCell<Reservation>>,
    pub(super) explained: Option<&'a Plan>,
    pub(super) sets: Option<super::SetContext<'a>>,
    pub(super) membership: Option<usize>,
}

impl<'a> EvalContext<'a> {
    pub(crate) fn new(params: &'a HashMap<String, Value>, now: i64, kept: &'a Kept) -> Self {
        EvalContext {
            params,
            kept,
            now,
            subqueries: &[],
            said: None,
            held: None,
            explained: None,
            sets: None,
            membership: None,
        }
    }

    pub(crate) fn with_sets(self, sets: Option<super::SetContext<'a>>) -> Self {
        Self { sets, ..self }
    }

    /// The context of a statement whose subqueries gave these values, each under the place its
    /// text begins in the statement: one value for each row of each.
    pub(crate) fn over(self, subqueries: &'a [(usize, List)]) -> Self {
        EvalContext { subqueries, ..self }
    }

    /// The values of one subquery of the statement, found by its place, which every copy of
    /// it keeps.
    pub(super) fn subquery(&self, query: &Select) -> Result<&'a List> {
        let found = self.subqueries.iter().find(|(at, _)| *at == query.at.0);
        found.map(|(_, values)| values).ok_or_else(|| {
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
pub(crate) fn eval<'a>(
    expr: &'a Expr,
    scope: &Scope,
    row: &'a [Value],
    ctx: &EvalContext<'a>,
) -> Result<Cow<'a, Value>> {
    let live = ctx.kept.live.get();
    let result = worked(expr, scope, row, ctx, false);
    ctx.kept.live.set(live);
    result
}

fn worked<'a>(
    expr: &'a Expr,
    scope: &Scope,
    row: &'a [Value],
    ctx: &EvalContext<'a>,
    intermediate: bool,
) -> Result<Cow<'a, Value>> {
    let value = |expr: &'a Expr| {
        let live = ctx.kept.live.get();
        let value = worked(expr, scope, row, ctx, true);
        ctx.kept.live.set(live);
        let value = value?;
        if let Cow::Owned(value) = &value {
            ctx.kept.live.set(ctx.kept.live.get() + heap_bytes(value));
        }
        Ok(value)
    };
    Ok(Cow::Owned(match expr {
        Expr::Column(column) => {
            let (at, _) = scope.find(column)?;
            return row
                .get(at)
                .map(Cow::Borrowed)
                .ok_or_else(|| scope.missing(column));
        }
        Expr::Literal(value) => return Ok(Cow::Borrowed(value)),
        Expr::Parameter(name) => {
            return ctx
                .params
                .get(name)
                .map(Cow::Borrowed)
                .ok_or_else(|| unbound(name));
        }
        Expr::BinaryOp { left, op, right } => {
            let holds: fn(Ordering) -> bool = match op {
                BinOp::Eq | BinOp::Neq
                    if matches!((&**left, &**right), (Expr::Row(_), _) | (_, Expr::Row(_))) =>
                {
                    let equal = rows_equal(
                        &side(left, value)?,
                        &side(right, value)?,
                        (Some(left.sides()), Some(right.sides())),
                        ctx,
                    )?;
                    return Ok(Cow::Owned(equal.map_or(Value::Null, |equal| {
                        Value::Bool(equal == (*op == BinOp::Eq))
                    })));
                }
                BinOp::And | BinOp::Or => {
                    // The value that decides the answer alone: FALSE for AND, TRUE for OR.
                    let settles = *op == BinOp::Or;
                    let left = truth(&*value(left)?);
                    if left == Some(settles) {
                        return Ok(Cow::Owned(Value::Bool(settles)));
                    }
                    return Ok(Cow::Owned(match (left, truth(&*value(right)?)) {
                        (_, Some(right)) if right == settles => Value::Bool(settles),
                        (Some(_), Some(_)) => Value::Bool(!settles),
                        _ => Value::Null,
                    }));
                }
                BinOp::Eq => Ordering::is_eq,
                BinOp::Neq => Ordering::is_ne,
                BinOp::Lt => Ordering::is_lt,
                BinOp::Lte => Ordering::is_le,
                BinOp::Gt => Ordering::is_gt,
                BinOp::Gte => Ordering::is_ge,
            };
            compared(
                ctx,
                (Some(left), &*value(left)?),
                (Some(right), &*value(right)?),
            )?
            .map_or(Value::Null, |order| Value::Bool(holds(order)))
        }
        Expr::UnaryOp { op, operand } => match (op, value(operand)?.as_ref()) {
            (UnaryOp::Not, value) => truth(value).map_or(Value::Null, |truth| Value::Bool(!truth)),
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
        Expr::IsNull { expr, negated } => Value::Bool((*value(expr)? == Value::Null) != *negated),
        Expr::FunctionCall { name, args } => match (scalar(name), args.as_slice()) {
            (Some(Scalar::Arithmetic(op)), [left, right]) => {
                arithmetic(op, &*value(left)?, &*value(right)?)?
            }
            (Some(Scalar::Now), []) => Value::Timestamp(ctx.now),
            (Some(Scalar::Coalesce), args) => {
                for arg in args {
                    let first = value(arg)?;
                    if let Value::Int64(whole) = *first
                        && super::bind::kind(expr, scope, ctx.params)? == Some(ColumnType::Real)
                    {
                        return Ok(Cow::Owned(Value::Float64(whole as f64)));
                    }
                    if *first != Value::Null {
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
            let items = list
                .iter()
                .map(|item| Ok((Some(item.sides()), side(item, value)?)));
            among(&side(expr, value)?, items, *negated, expr.sides(), ctx)?
        }
        Expr::InSubquery {
            expr,
            query,
            negated,
        } => match ctx.subquery(query)? {
            List::Stored(s) => s.member(value(expr)?, *negated, expr.sides(), ctx)?,
            List::Memory(items, _) => {
                let items = items.iter().map(|v| Ok((None, [Cow::Borrowed(v)])));
                among(&[value(expr)?], items, *negated, expr.sides(), ctx)?
            }
        },
        // A subquery that found no row is NULL.
        Expr::Subquery(query) => {
            return Ok(Cow::Borrowed(
                ctx.subquery(query)?
                    .small()?
                    .first()
                    .unwrap_or(&Value::Null),
            ));
        }
        Expr::Row(_) => {
            return Err(Error::PlanError(
                "a row value, (a, b), stands only on either side of =, <> or IN".to_owned(),
            ));
        }
        Expr::Distance { left, right } => {
            match (numbers(&*value(left)?)?, numbers(&*value(right)?)?) {
                (Some(left), Some(right)) if left.clone().count() == right.clone().count() => {
                    Value::Float64(crate::vector::distance(left, right))
                }
                (Some(_), Some(_)) => {
                    return Err(Error::PlanError(
                        "<=> needs two vectors of one length".to_owned(),
                    ));
                }
                _ => Value::Null,
            }
        }
        Expr::Like {
            expr,
            pattern,
            negated,
        } => match (value(expr)?.as_ref(), value(pattern)?.as_ref()) {
            (Value::Text(text), Value::Text(pattern)) => {
                Value::Bool(like(text, pattern) != *negated)
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
            let above = compared(ctx, (Some(expr), &middle), (Some(low), &*value(low)?))?
                .map(Ordering::is_ge);
            let below = compared(ctx, (Some(expr), &middle), (Some(high), &*value(high)?))?
                .map(Ordering::is_le);
            match (above, below) {
                (Some(false), _) | (_, Some(false)) => Value::Bool(*negated),
                (Some(true), Some(true)) => Value::Bool(!*negated),
                _ => Value::Null,
            }
        }
        Expr::Field { json, key, as_text } => {
            let (json, key) = (value(json)?, value(key)?);
            let field = match (json.as_ref(), key.as_ref()) {
                (Value::Json(json), Value::Text(key)) => json.get(key.as_str()),
                (Value::Json(json), Value::Int64(at)) => {
                    usize::try_from(*at).ok().and_then(|at| json.get(at))
                }
                _ => None,
            };
            match field {
                None => Value::Null,
                Some(Json::Null) if *as_text => Value::Null,
                Some(field) => {
                    if intermediate {
                        ctx.kept
                            .cover(heap_bytes(&json) * if *as_text { 18 } else { 1 })?;
                    }
                    match (field, as_text) {
                        (Json::String(text), true) => Value::Text(text.clone()),
                        (_, true) => Value::Text(field.to_string()),
                        _ => Value::Json(field.clone()),
                    }
                }
            }
        }
        Expr::RowNumber { .. } => {
            return Err(Error::PlanError(
                "ROW_NUMBER() has a value only for the rows its window numbered".to_owned(),
            ));
        }
    }))
}

/// Whether `left` is among `items`: TRUE when one of them equals it, else unknown when any
/// equality was.
pub(super) fn among<'a, 'e, T: AsRef<[Cow<'a, Value>]>>(
    left: &[Cow<'a, Value>],
    items: impl Iterator<Item = Result<(Option<&'e [Expr]>, T)>>,
    negated: bool,
    expr: &[Expr],
    ctx: &EvalContext,
) -> Result<Value> {
    let mut found = Some(false);
    for item in items {
        let (other, item) = item?;
        if item.as_ref().len() != left.len() {
            return Err(Error::PlanError(
                "a row value beside IN needs as many values as its partner".to_owned(),
            ));
        }
        match rows_equal(left, item.as_ref(), (Some(expr), other), ctx)? {
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
pub(super) fn numbers(value: &Value) -> Result<Option<impl Iterator<Item = f32> + Clone + '_>> {
    let (vector, text) = match value {
        Value::Null => return Ok(None),
        Value::Vector(vector) => (Some(vector.as_slice()), None),
        Value::Text(text) => (None, vector_numbers(text)),
        _ => (None, None),
    };
    if vector.is_none()
        && text
            .as_ref()
            .is_none_or(|it| it.clone().any(|n| n.is_none()))
    {
        return Err(Error::PlanError("<=> needs a vector on both sides: write [1.0, 0.0], its text '[1, 0]', or a $name bound to a vector".to_owned()));
    }
    Ok(Some(vector.into_iter().flatten().copied().chain(
        text.into_iter().flatten().map(|n| n.unwrap_or_default()),
    )))
}

/// The values of one side of IN: those of a row value, or the one value of anything else.
fn side<'a>(
    expr: &'a Expr,
    value: impl Fn(&'a Expr) -> Result<Cow<'a, Value>>,
) -> Result<Vec<Cow<'a, Value>>> {
    expr.sides().iter().map(value).collect()
}

/// Whether two row values of one length are equal, as standard SQL has it: FALSE as soon as one
/// pair of columns is definitely different, whatever a NULL beside it holds; TRUE when every
/// pair is equal; otherwise unknown. One value is a row of one column.
fn rows_equal(
    left: &[Cow<'_, Value>],
    right: &[Cow<'_, Value>],
    expr: (Option<&[Expr]>, Option<&[Expr]>),
    ctx: &EvalContext,
) -> Result<Option<bool>> {
    let mut equal = Some(true);
    for (at, (left, right)) in left.iter().zip(right).enumerate() {
        match compared(
            ctx,
            (expr.0.and_then(|items| items.get(at)), left),
            (expr.1.and_then(|items| items.get(at)), right),
        )? {
            Some(Ordering::Equal) => {}
            Some(_) => return Ok(Some(false)),
            None => equal = None,
        }
    }
    Ok(equal)
}

fn compared(
    ctx: &EvalContext,
    left: (Option<&Expr>, &Value),
    right: (Option<&Expr>, &Value),
) -> Result<Option<Ordering>> {
    let (tree, expr, text, reverse) = match (left, right) {
        ((_, Value::Json(tree)), (expr, Value::Text(text))) => (tree, expr, text, false),
        ((expr, Value::Text(text)), (_, Value::Json(tree))) => (tree, expr, text, true),
        _ => return Ok(compare(left.1, right.1)),
    };
    ctx.kept.with_json(expr, text, |cached| {
        Some(if reverse {
            json(cached, tree)
        } else {
            json(tree, cached)
        })
    })
}

/// The most memory the value of an expression can take outside its own cell. It is known
/// before the expression is worked out, so the memory is asked for before the value is made.
pub(crate) fn most_bytes(expr: &Expr, scope: &Scope, row: &[Value], ctx: &EvalContext) -> u64 {
    match expr {
        Expr::Column(column) => {
            let at = scope.find(column).ok();
            at.and_then(|(at, _)| row.get(at)).map_or(0, heap_bytes)
        }
        Expr::Literal(value) => heap_bytes(value),
        Expr::Parameter(name) => ctx.params.get(name).map_or(0, heap_bytes),
        Expr::FunctionCall { name, args } if matches!(scalar(name), Some(Scalar::Coalesce)) => {
            let each = args.iter().map(|arg| most_bytes(arg, scope, row, ctx));
            each.max().unwrap_or(0)
        }
        Expr::Subquery(query) => {
            let first = ctx
                .subquery(query)
                .ok()
                .and_then(|found| found.small().ok()?.first());
            first.map_or(0, heap_bytes)
        }
        // A field is no larger than the JSON value it is taken from.
        Expr::Field { json, as_text, .. } => {
            most_bytes(json, scope, row, ctx).saturating_mul(if *as_text { 18 } else { 1 })
        }
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
    let inner = crate::sql::inside(expr);
    inner.for_each(|inner| mark(inner, scope, read));
}

/// Where a column sits in a row of its table.
pub(super) fn column_index(table: &Table, column: &ColumnRef) -> Result<usize> {
    Ok(crate::query::scope(table).find(column)?.0)
}

pub(super) fn unbound(name: &str) -> Error {
    Error::PlanError(format!(
        "no value is bound to ${}",
        crate::types::excerpt(name)
    ))
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

/// The five operators as the parser names them: `a + b` arrives as the call `__add(a, b)`.
#[derive(Clone, Copy)]
pub(super) enum Arithmetic {
    Add,
    Sub,
    Mul,
    Div,
    /// The remainder of whole numbers, with the sign of the dividend, as Postgres `%` gives it.
    Rem,
}

/// The scalar function of this name, if it is one.
pub(super) fn scalar(name: &str) -> Option<Scalar> {
    [
        ("__add", Scalar::Arithmetic(Arithmetic::Add)),
        ("__sub", Scalar::Arithmetic(Arithmetic::Sub)),
        ("__mul", Scalar::Arithmetic(Arithmetic::Mul)),
        ("__div", Scalar::Arithmetic(Arithmetic::Div)),
        ("__mod", Scalar::Arithmetic(Arithmetic::Rem)),
        ("now", Scalar::Now),
        ("coalesce", Scalar::Coalesce),
    ]
    .into_iter()
    .find(|(known, _)| name.eq_ignore_ascii_case(known))
    .map(|(_, scalar)| scalar)
}

/// Arithmetic as standard SQL does it: NULL in gives NULL out, two whole numbers give a whole
/// number or are refused when it would not fit, and a REAL on either side gives a REAL.
/// Dividing by zero is refused. `%` takes whole numbers only, as Postgres has no `%` of a
/// double precision; `MIN % -1` is 0, as there.
pub(super) fn arithmetic(op: Arithmetic, left: &Value, right: &Value) -> Result<Value> {
    let by_zero = || Error::PlanError("division by zero".to_owned());
    Ok(match (left, right) {
        (Value::Null, _) | (_, Value::Null) => Value::Null,
        (Value::Int64(left), Value::Int64(right)) => Value::Int64(
            match op {
                Arithmetic::Add => left.checked_add(*right),
                Arithmetic::Sub => left.checked_sub(*right),
                Arithmetic::Mul => left.checked_mul(*right),
                Arithmetic::Div | Arithmetic::Rem if *right == 0 => return Err(by_zero()),
                Arithmetic::Div => left.checked_div(*right),
                Arithmetic::Rem => Some(left.wrapping_rem(*right)),
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
                Arithmetic::Rem => {
                    return Err(Error::PlanError(
                        "the remainder % takes two whole numbers".to_owned(),
                    ));
                }
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
        (Value::Vector(left), Value::Vector(right)) => left.partial_cmp(right)?,
        (Value::Vector(left), Value::Text(right)) => {
            let right = vector_numbers(right)?;
            if right.clone().any(|n| n.is_none()) {
                return None;
            }
            left.iter()
                .copied()
                .partial_cmp(right.map(|n| n.unwrap_or_default()))?
        }
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
pub(crate) fn sort_order(left: &Value, right: &Value) -> Ordering {
    match (left, right) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Greater,
        (_, Value::Null) => Ordering::Less,
        _ => compare(left, right).unwrap_or_else(|| match left == right {
            true => Ordering::Equal,
            false => spelling(left).cmp(spelling(right)),
        }),
    }
}

/// Full Debug spelling for vectors; other kinds differ within the bounded prefix.
fn spelling(value: &Value) -> impl Iterator<Item = u8> + '_ {
    let mut prefix = Cursor::new([0; 16]);
    let _ = write!(prefix, "{value:?}");
    let vector = match value {
        Value::Vector(vector) => Some(vector),
        _ => None,
    };
    let len = vector.map_or(prefix.position() as usize, |_| 8);
    let tail = vector.into_iter().flat_map(|vector| {
        vector
            .iter()
            .enumerate()
            .flat_map(|(at, n)| {
                let mut text = Cursor::new([0; 64]);
                let _ = write!(text, "{}{n:?}", if at == 0 { "" } else { ", " });
                let len = text.position() as usize;
                text.into_inner().into_iter().take(len)
            })
            .chain([b']', b')'])
    });
    prefix.into_inner().into_iter().take(len).chain(tail)
}
