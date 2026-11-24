//! UNION ALL and UNION: the rows of one SELECT after another's, each SELECT read in turn. UNION
//! ALL hands each row on and holds nothing; the SELECTs up to the last UNION pass through one
//! table that holds one entry for each different row, as DISTINCT does.

use super::expression::{Scope, reads};
use super::join::Parts;
use super::plan::{Cut, Distinct, Each, Given, Source, limits};
use super::sort::Sort;
use super::subquery::{Pass, Then, Whole};
use crate::memory::Reservation;
use crate::sql::Expr;
use crate::sql::plan::{ProjectColumn, SelectPlan};
use crate::types::ColumnType::{self, Integer, Real};
use crate::types::Value;
use crate::types::{Error, Result};

/// Puts the SELECTs of the UNION of `select` one after another, each read `depth` statements
/// deep, under the ORDER BY, LIMIT and OFFSET of `select`, which are over the whole and name its
/// columns, and hands the whole to `then`. Each SELECT gives as many columns as the first, of its
/// types (a whole number and a REAL are one, as are two VECTOR lengths), or a NULL that takes the
/// other's type: refused when the statement is bound, whatever the tables hold.
pub(super) fn after<R>(
    select: &SelectPlan,
    depth: usize,
    given: &Given<'_>,
    pass: &mut Pass<'_>,
    then: &mut Then<'_, R>,
) -> Result<R> {
    let mismatch = || {
        let why = "the SELECTs of a UNION give the same number of columns, of types that match";
        Error::PlanError(why.to_owned())
    };
    // A column of NULL has no type of its own: the first SELECT that gives another types it.
    let null = |list: &[ProjectColumn], at: usize| {
        let expr = list.get(at).map(|column| &column.expr);
        matches!(expr, Some(Expr::Literal(Value::Null)))
    };
    let mut first = select.clone();
    (first.with, first.union, first.order_by) = (Vec::new(), Vec::new(), Vec::new());
    (first.limit, first.offset, first.limit_name) = (None, None, None);
    first.offset_name = None;
    let (mut columns, mut typed, mut sources) = (Vec::new(), Vec::new(), Vec::new());
    let arms = select.union.iter().map(|(_, arm)| arm);
    for arm in std::iter::once(&first).chain(arms) {
        let arm = Whole {
            select: arm,
            given,
            depth,
        };
        let theirs = arm.bound()?;
        if sources.is_empty() {
            (columns, typed) = (theirs.clone(), vec![false; theirs.len()]);
        }
        if theirs.len() != columns.len() {
            return Err(mismatch());
        }
        for (at, (column, next)) in columns.iter_mut().zip(&theirs).enumerate() {
            if null(&arm.select.columns, at) {
            } else if !typed[at] {
                (column.column_type, typed[at]) = (next.column_type, true);
            } else {
                column.column_type =
                    meet(column.column_type, next.column_type).ok_or_else(mismatch)?;
            }
        }
        sources.push(Box::new(arm) as Box<dyn Source>);
    }
    if let Some(last) = select.union.iter().rposition(|(all, _)| !all) {
        let rest = sources.split_off(last + 2);
        let input = Box::new(Parts(std::mem::replace(&mut sources, rest), &columns));
        sources.insert(0, Box::new(Distinct { input }));
    }
    let mut scope = Scope::of("", &columns);
    scope.checked = true;
    let read = &mut vec![false; columns.len()];
    for key in &select.order_by {
        let Expr::Column(_) = key.expr else {
            let why = "the ORDER BY of a UNION names columns of its result, not expressions";
            return Err(Error::PlanError(why.to_owned()));
        };
        reads(&key.expr, &scope, given.params, read)?;
    }
    let (limit, offset) = limits(select, given.params)?;
    let mut input: Box<dyn Source + '_> = Box::new(Parts(sources, &columns));
    if !select.order_by.is_empty() {
        input = Box::new(Sort {
            input,
            scope,
            order: &select.order_by,
            ctx: &given.ctx,
            most: limit.saturating_add(offset),
        });
    }
    let cut = Cut {
        input,
        offset,
        limit,
    };
    then(&columns, &cut, pass)
}

/// The type values of two types share, as in Postgres: a REAL for a whole number and a REAL.
pub(super) fn meet(left: ColumnType, right: ColumnType) -> Option<ColumnType> {
    let real = matches!((left, right), (Integer, Real) | (Real, Integer));
    let same = std::mem::discriminant(&left) == std::mem::discriminant(&right);
    (same || real).then_some(if real { Real } else { left })
}

impl<S: Source + ?Sized> Source for &S {
    fn rows(&self, reservation: &mut Reservation, each: &mut Each<'_>) -> Result<()> {
        (**self).rows(reservation, each)
    }
}
