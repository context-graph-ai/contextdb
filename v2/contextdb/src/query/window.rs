//! `ROW_NUMBER() OVER ([PARTITION BY ...] ORDER BY ...)`: the rows of each partition numbered
//! 1, 2, 3 in the window's order.

use std::cmp::Ordering;

use super::aggregate::made;
use super::expression::{EvalContext, Scope, eval, most_bytes, sort_order};
use super::plan::{Each, Source, listed};
use super::sort::Sort;
use crate::memory::Reservation;
use crate::sql::plan::{ProjectColumn, SelectPlan, SortKey};
use crate::sql::{Expr, inside_mut};
use crate::types::{ColumnDef, Error, Result, SortDirection, Value};

/// What the column of a row's number is called and qualified with: no table has this name.
const NUMBER: &str = "ROW_NUMBER() OVER";

/// The one window of a SELECT, and the statement's ORDER BY and select list written over a
/// numbered row: a row of the input and then its number.
pub(super) struct Window {
    /// What the rows are numbered in the order of: the partition's columns, then the window's
    /// ORDER BY.
    keys: Vec<SortKey>,
    /// How many of `keys` name the partition.
    partition: usize,
    number: [ColumnDef; 1],
    order: Vec<SortKey>,
    list: Vec<ProjectColumn>,
}

impl Window {
    /// The window of a SELECT that writes `ROW_NUMBER() OVER`; `None` for any other. Two
    /// different windows in one SELECT, and a partition or an order that is not a column, are
    /// refused by name.
    pub(super) fn of(select: &SelectPlan) -> Result<Option<Window>> {
        let (mut order, mut list) = (select.order_by.clone(), select.columns.clone());
        let mut found = None;
        let written = order.iter_mut().map(|key| &mut key.expr);
        let mut written = written.chain(list.iter_mut().map(|column| &mut column.expr));
        written.try_for_each(|expr| numbered(expr, &mut found))?;
        let Some(Expr::RowNumber {
            partition_by,
            order_by,
        }) = found
        else {
            return Ok(None);
        };
        let partition = partition_by.len();
        let partition_by = partition_by.into_iter().map(|expr| SortKey {
            expr,
            direction: SortDirection::Asc,
        });
        Ok(Some(Window {
            keys: partition_by.chain(order_by).collect(),
            partition,
            number: [made(Some(NUMBER), NUMBER).0],
            order,
            list,
        }))
    }

    /// The order the rows are numbered in. An input that arrives in it needs no sort.
    pub(super) fn keys(&self) -> &[SortKey] {
        &self.keys
    }
}

/// Puts the column of the row's number wherever `expr` writes a window, which becomes `found`.
fn numbered(expr: &mut Expr, found: &mut Option<Expr>) -> Result<()> {
    let Expr::RowNumber {
        partition_by,
        order_by,
    } = expr
    else {
        let mut inner = inside_mut(expr);
        return inner.try_for_each(|inner| numbered(inner, found));
    };
    let refused = |form: &str| {
        let form = form.to_owned();
        Err(Error::WindowFunctionNotSupported { form })
    };
    let mut named = partition_by
        .iter()
        .chain(order_by.iter().map(|key| &key.expr));
    if !named.all(|expr| matches!(expr, Expr::Column(_))) {
        return refused("a computed partition or order expression");
    }
    let window = std::mem::replace(expr, made(Some(NUMBER), NUMBER).1);
    match found {
        Some(first) if format!("{first:?}") != format!("{window:?}") => {
            refused("several different windows in one SELECT")
        }
        _ => {
            *found = Some(window);
            Ok(())
        }
    }
}

/// The rows of a statement with a window: its input, whose columns are `scope`, numbered, then
/// the statement's ORDER BY and its select list over the numbered rows. An input that is not
/// `in_order`, the order of [`Window::keys`], is sorted first, which holds every row of it.
pub(super) struct Numbered<'a> {
    pub(super) input: Box<dyn Source + 'a>,
    pub(super) scope: Scope<'a>,
    pub(super) window: Window,
    pub(super) ctx: &'a EvalContext<'a>,
    pub(super) in_order: bool,
    /// How many rows can leave, as [`Sort`] has it.
    pub(super) most: usize,
}

impl Source for Numbered<'_> {
    fn rows(&self, reservation: &mut Reservation, each: &mut Each<'_>) -> Result<()> {
        let (window, ctx) = (&self.window, self.ctx);
        let scope = self.scope.clone().and(NUMBER, &window.number);
        let rows = Box::new(Numbers(self));
        let written = (window.order.as_slice(), window.list.as_slice());
        listed(rows, scope, written, ctx, self.most).rows(reservation, each)
    }
}

/// The rows of a window's input in the window's order, each with its number after its values.
/// Holds the partition values of the row before, to tell where the next partition begins.
struct Numbers<'s, 'a>(&'s Numbered<'a>);

impl Source for Numbers<'_, '_> {
    fn rows(&self, reservation: &mut Reservation, each: &mut Each<'_>) -> Result<()> {
        let Numbered {
            input,
            scope,
            window,
            ctx,
            in_order,
            ..
        } = self.0;
        let sorted = Sort {
            input: Box::new(&**input),
            scope: scope.clone(),
            order: &window.keys,
            ctx,
            most: usize::MAX,
        };
        let input: &dyn Source = if *in_order { &**input } else { &sorted };
        let partition = &window.keys[..window.partition];
        let cell = size_of::<Value>() as u64;
        let (mut before, mut held, mut number) = (Vec::new(), 0, 0);
        input.rows(reservation, &mut |mut row, bytes, reservation| {
            let most = partition.iter();
            let most = most.map(|key| most_bytes(&key.expr, scope, &row, ctx));
            let most = most.sum::<u64>();
            reservation.grow(most)?;
            reservation.room(&mut row, usize::MAX)?;
            let values = partition
                .iter()
                .map(|key| eval(&key.expr, scope, &row, ctx).map(std::borrow::Cow::into_owned))
                .collect::<Result<Vec<_>>>()?;
            let same = |(left, right)| sort_order(left, right) == Ordering::Equal;
            if number > 0 && values.iter().zip(&before).all(same) {
                reservation.shrink(most);
                number += 1;
            } else {
                reservation.shrink(held);
                (before, held, number) = (values, most, 1);
            }
            row.push(Value::Int64(number));
            each(row, bytes + cell, reservation)
        })?;
        reservation.shrink(held);
        Ok(())
    }
}
