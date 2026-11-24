//! Refuses what no table could make right; a statement that passes is its own plan.
//!
//! The planner is a set of fixed rules over the statement alone. It knows no table, so it
//! chooses no index, no join order and no search: a SELECT's plan says what was written, and
//! `query` decides how to answer it.

use super::ast::{BinOp, Expr, OrderByItem, Select, SortDirection::Asc, Statement};
use super::{not_carried, not_supported};
use crate::types::{Error, Result, Value};

/// The functions that work one value out of many rows, in the order `query` names them.
pub(crate) const AGGREGATES: [&str; 5] = ["count", "sum", "min", "max", "avg"];

/// Refuses a form of `statement`, or of a statement inside it, that is refused whatever the
/// tables hold.
pub(super) fn stands(statement: &Statement) -> Result<()> {
    match statement {
        Statement::Insert(insert) => {
            insert.values.iter().flatten().try_for_each(forms_stand)?;
            let conflict = insert.on_conflict.iter();
            let mut set = conflict.flat_map(|conflict| conflict.assignments.iter());
            set.try_for_each(|(_, value)| in_a_write(value))
        }
        Statement::Delete(_) | Statement::Update(_) => {
            let (_, written) = statement.written().expect("an UPDATE or DELETE");
            written.into_iter().try_for_each(in_a_write)
        }
        Statement::Purge(selections) => selections
            .iter()
            .flat_map(|s| &s.where_clause)
            .try_for_each(in_a_write),
        Statement::Select(select) => select_stands(select),
        Statement::Explain(statement) => stands(statement),
        _ => Ok(()),
    }
}

/// Refuses a form of the SELECT, or of a SELECT inside it, that is refused whatever the tables
/// hold.
fn select_stands(select: &Select) -> Result<()> {
    let on = select.joins.iter().filter_map(|join| join.on.as_ref());
    let before_groups = on.chain(&select.filter).chain(&select.group_by);
    let after_groups = select.columns.iter().map(|column| &column.expr);
    let after_groups = after_groups.chain(select.order_by.iter().map(|item| &item.expr));
    for expr in before_groups.clone() {
        calls_stand(expr, false)?;
    }
    let mut having = select.having.iter();
    having.try_for_each(|expr| calls_stand(expr, true))?;
    let mut written = before_groups.chain(&select.having).chain(after_groups);
    written.try_for_each(forms_stand)?;
    let order_by = select.order_by.as_slice();
    let distance = |key: &OrderByItem| matches!(key.expr, Expr::Distance { .. });
    if select.use_vector.is_some() && !order_by.iter().any(distance) {
        return Err(Error::UseVectorRequiresVectorOrder);
    }
    // A rank orders the rows of one search: its one sort key is the distance, nearest first.
    if select.use_rank.is_some() {
        if !matches!(order_by, [key] if distance(key) && key.direction == Asc) {
            return Err(Error::UseRankRequiresVectorOrder);
        }
        if select.limit.is_none() && select.limit_name.is_none() {
            return Err(Error::UseRankRequiresLimit);
        }
    }
    let named = select.with.iter().map(|(_, select)| select);
    let mut inner = named.chain(select.union.iter().map(|(_, select)| select));
    inner.try_for_each(select_stands)
}

/// Refuses a window, and an aggregate unless `grouped`, in a clause that is worked out before
/// the rows are grouped (JOIN ... ON, WHERE, GROUP BY) or, for a window, before they are numbered
/// (HAVING).
fn calls_stand(expr: &Expr, grouped: bool) -> Result<()> {
    let refused = match expr {
        Expr::RowNumber { .. } => Some("ROW_NUMBER() OVER"),
        Expr::FunctionCall { name, .. } if !grouped => AGGREGATES
            .into_iter()
            .find(|known| name.eq_ignore_ascii_case(known)),
        _ => None,
    };
    if let Some(name) = refused {
        return Err(Error::PlanError(format!(
            "{name} stands only in a select list, in ORDER BY or, for an aggregate, in HAVING"
        )));
    }
    let mut inner = inside(expr);
    inner.try_for_each(|inner| calls_stand(inner, grouped))
}

/// Writes `$name`, which gives the expressions directly inside an expression, as `&` or, with
/// `mut`, as `&mut` references. A SELECT inside an expression is a statement of its own.
macro_rules! inside {
    ($name:ident, $iter:ident $(, $mut:tt)?) => {
        pub(crate) fn $name(expr: &$($mut)? Expr) -> impl Iterator<Item = &$($mut)? Expr> {
            let (one, many, order) = match expr {
                Expr::BinaryOp { left, right, .. } | Expr::Distance { left, right }
                | Expr::Like { expr: left, pattern: right, .. }
                | Expr::Field { json: left, key: right, .. } =>
                    ([Some(& $($mut)? **left), Some(& $($mut)? **right), None], & $($mut)? [][..], & $($mut)? [][..]),
                Expr::UnaryOp { operand, .. } | Expr::IsNull { expr: operand, .. }
                | Expr::InSubquery { expr: operand, .. } =>
                    ([Some(& $($mut)? **operand), None, None], & $($mut)? [][..], & $($mut)? [][..]),
                Expr::FunctionCall { args, .. } | Expr::Row(args) =>
                    ([None, None, None], & $($mut)? args[..], & $($mut)? [][..]),
                Expr::InList { expr, list, .. } =>
                    ([Some(& $($mut)? **expr), None, None], & $($mut)? list[..], & $($mut)? [][..]),
                Expr::Between { expr, low, high, .. } =>
                    ([Some(& $($mut)? **expr), Some(& $($mut)? **low), Some(& $($mut)? **high)], & $($mut)? [][..], & $($mut)? [][..]),
                Expr::RowNumber { partition_by, order_by } =>
                    ([None, None, None], & $($mut)? partition_by[..], & $($mut)? order_by[..]),
                Expr::Column(_) | Expr::Literal(_) | Expr::Parameter(_) | Expr::Subquery(_) =>
                    ([None, None, None], & $($mut)? [][..], & $($mut)? [][..]),
            };
            one.into_iter().flatten().chain(many.$iter()).chain(order.$iter().map(|key| & $($mut)? key.expr))
        }
    };
}
inside!(inside, iter);
inside!(inside_mut, iter_mut, mut);

/// What an expression of an UPDATE or DELETE must pass: [`forms_stand`], and no distance, which
/// a write does not carry. Refused here, the statement is refused whatever its table holds.
fn in_a_write(expr: &Expr) -> Result<()> {
    fn distance(expr: &Expr) -> bool {
        matches!(expr, Expr::Distance { .. }) || inside(expr).any(distance)
    }
    forms_stand(expr)?;
    match distance(expr) {
        true => Err(not_supported("<=> in an UPDATE or DELETE")),
        false => Ok(()),
    }
}

/// Refuses what is refused whatever the tables hold: a row value that stands anywhere but on
/// either side of `=`, `<>` and `IN`, one there that has another number of values than its
/// partner, a quoted text there that is no JSON beside a JSON field, which reads it as JSON,
/// and ROW_VECTOR, which no version has. A SELECT inside the expression is checked as the
/// statement it is.
fn forms_stand(expr: &Expr) -> Result<()> {
    let (left, list) = match expr {
        Expr::InList { expr, list, .. } => (&**expr, list.as_slice()),
        Expr::BinaryOp {
            left,
            op: BinOp::Eq | BinOp::Neq,
            right,
        } => (&**left, std::slice::from_ref(&**right)),
        Expr::Row(_) => {
            return Err(not_supported(
                "a row value anywhere but beside =, <> and IN",
            ));
        }
        Expr::InSubquery { expr, query, .. } => {
            select_stands(query)?;
            return forms_stand(expr);
        }
        Expr::Subquery(query) => return select_stands(query),
        Expr::FunctionCall { name, .. } if name.eq_ignore_ascii_case("row_vector") => {
            let instead = "a scalar subquery supplies another row's vector";
            return Err(not_carried(&format!("{name}(...) beside <=>"), instead));
        }
        _ => return inside(expr).try_for_each(forms_stand),
    };
    // A quoted text that is no JSON, compared with a JSON field.
    let word = |one: &Expr, other: &Expr| match (one, other) {
        (Expr::Field { as_text: false, .. }, Expr::Literal(Value::Text(text))) => {
            let json = serde_json::from_str::<serde_json::Value>(text);
            json.is_err().then(|| text.clone())
        }
        _ => None,
    };
    for side in std::iter::once(left).chain(list) {
        if let Some(text) = word(left, side).or_else(|| word(side, left)) {
            let string = serde_json::Value::from(text.as_str());
            return Err(Error::PlanError(format!(
                "'{text}' is compared with a JSON field, which reads it as JSON, and it is no \
                 JSON: write '{string}' for the string"
            )));
        }
        if side.sides().len() != left.sides().len() {
            return Err(Error::PlanError(
                "a row value must have as many values as its partner".to_owned(),
            ));
        }
        // What a side holds is checked as any expression is: a row value holds no row value.
        match side {
            Expr::Row(values) => values.iter().try_for_each(forms_stand)?,
            _ => forms_stand(side)?,
        }
    }
    Ok(())
}
