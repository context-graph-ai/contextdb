//! ORDER BY for rows that do not arrive in the order asked for: every row of the input is read,
//! and only the rows that can still leave are kept.

use std::cmp::Ordering;
use std::collections::BinaryHeap;

use super::expression::{EvalContext, Scope, eval, holds, most_bytes, sort_order};
use super::plan::{Each, Source, hand_on};
use super::row_overhead;
use crate::memory::Reservation;
use crate::sql::Expr;
use crate::sql::ast::SortDirection;
use crate::sql::plan::SortKey;
use crate::types::{Result, Value};

/// The rows of its input, whose columns are `scope`, in the order of the sort keys; rows the
/// keys do not tell apart stay in the order they arrived in. The keys are worked out with the
/// caller's `ctx`. Holds each kept row and the values it sorts by, and never more than `most`
/// rows and the one that has just arrived: with a LIMIT that is the rows LIMIT and OFFSET let
/// leave, the best so far.
pub(super) struct Sort<'s> {
    pub(super) input: Box<dyn Source + 's>,
    pub(super) scope: Scope<'s>,
    pub(super) order: &'s [SortKey],
    pub(super) ctx: &'s EvalContext<'s>,
    pub(super) most: usize,
}

impl Source for Sort<'_> {
    fn rows(&self, reservation: &mut Reservation, each: &mut Each<'_>) -> Result<u64> {
        let (scope, ctx) = (&self.scope, self.ctx);
        // A row whose distance in a sort key is NULL is in no answer.
        let distance = |expr: &Expr| matches!(expr, Expr::Distance { .. });
        // The order of arrival is the last key, so rows the keys do not tell apart keep it.
        let order = self.order.iter();
        let descending = order.map(|key| key.direction == SortDirection::Desc);
        let descending: Vec<bool> = descending.chain([false]).collect();
        // The greatest of a heap is the row that sorts last: the one to drop for a better one.
        let mut best = BinaryHeap::new();
        let mut arrived = 0_i64;
        let examined = self
            .input
            .rows(reservation, &mut |row, bytes, reservation| {
                let order = self.order.iter();
                let most = order.map(|key| most_bytes(&key.expr, scope, &row, ctx));
                let beside = row_overhead(descending.len())
                    + size_of::<Key<'_, (Vec<Value>, u64, u64)>>() as u64
                    + most.sum::<u64>();
                reservation.grow(beside)?;
                let order = self.order.iter();
                let keys = order.map(|key| eval(&key.expr, scope, &row, ctx));
                let keys: Vec<Value> = keys
                    .chain([Ok(Value::Int64(arrived))])
                    .collect::<Result<_>>()?;
                let mut keyed = self.order.iter().zip(&keys);
                if keyed.any(|(key, value)| *value == Value::Null && holds(&key.expr, distance)) {
                    reservation.shrink(bytes + beside);
                    return Ok(true);
                }
                best.push(Key(keys, &descending, (row, bytes, beside)));
                arrived += 1;
                if best.len() > self.most
                    && let Some(Key(_, _, (_, bytes, beside))) = best.pop()
                {
                    reservation.shrink(bytes + beside);
                }
                Ok(true)
            })?;
        let kept = best
            .into_sorted_vec()
            .into_iter()
            .map(|Key(_, _, kept)| kept);
        hand_on(kept, reservation, each)?;
        Ok(examined)
    }
}

/// Values in the order of the keys they were worked out for, each greatest first where the flag
/// beside it says so (going up where no flag is), and what rides with them, which takes no part
/// in the order. Values that no key tells apart are equal, as two NULLs are.
pub(super) struct Key<'s, T>(pub(super) Vec<Value>, pub(super) &'s [bool], pub(super) T);

impl<T> Ord for Key<'_, T> {
    fn cmp(&self, other: &Self) -> Ordering {
        let flags = self.1.iter().chain(std::iter::repeat(&false));
        let values = self.0.iter().zip(&other.0).zip(flags);
        let mut orders = values.map(|((left, right), descending)| match descending {
            true => sort_order(left, right).reverse(),
            false => sort_order(left, right),
        });
        orders
            .find(|order| order.is_ne())
            .unwrap_or(Ordering::Equal)
    }
}

impl<T> PartialOrd for Key<'_, T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> PartialEq for Key<'_, T> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl<T> Eq for Key<'_, T> {}
