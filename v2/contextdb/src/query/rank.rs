//! `USE RANK`: the order a VECTOR column's rank policy gives the rows a similarity search
//! compares. A row's joined row is found by one lookup, and the policy's formula is worked out
//! by the one evaluator over the row, the joined row and `vector_score`, which is one minus the
//! row's distance.

use std::cmp::Ordering;

use super::expression::{Scope, column_index, compare, eval, reads};
use super::plan::Given;
use super::table::Source;
use super::visible::Visible;
use crate::memory::Reservation;
use crate::schema::Table;
use crate::sql::Expr;
use crate::sql::ast::ColumnRef;
use crate::types::{ColumnDef, Error, Result, Value};

/// A rank policy ready to rank rows.
pub(super) struct Rank<'a> {
    formula: Expr,
    /// The columns of the table, then those of the joined table, then `vector_score`.
    scope: Scope<'a>,
    /// The joined table's rows the reader may see, read for what the formula reads of them.
    lookup: Source<'a>,
    width: usize,
    /// The column of the joined table its row is found by, and the column of the table whose
    /// value finds it.
    on: usize,
    anchor: usize,
}

impl<'a> Rank<'a> {
    /// The policy at `policy` among those of `table`; `score` is the column `vector_score`, and
    /// the joined rows are read through `seeing`. Marks in `read`, which has one place for each
    /// column of the table, the columns of it the formula reads and the one the joined row is
    /// found by.
    pub(super) fn of(
        table: &'a Table,
        policy: usize,
        score: &'a [ColumnDef],
        given: &'a Given<'a>,
        seeing: &'a mut Reservation,
        read: &mut [bool],
    ) -> Result<Rank<'a>> {
        let (ranked, catalog) = (&table.rules.rank_policies[policy], given.catalog);
        let policy = &ranked.policy;
        let joined = catalog.table(&policy.table);
        let joined = joined.ok_or_else(|| Error::TableNotFound(policy.table.clone()))?;
        let on = ColumnRef {
            table: None,
            column: policy.column.clone(),
        };
        let scope = Scope::of(&table.name, &table.meta.columns);
        let scope = scope.and(&joined.name, &joined.meta.columns).and("", score);
        let formula = crate::sql::formula(&policy.formula)?;
        let mut every = vec![false; scope.columns().count()];
        reads(&formula, &scope, given.params, &mut every)?;
        // The joined column is read: a row found through a digest is checked against it.
        let on = column_index(joined, &on)?;
        every[read.len() + on] = true;
        let (own, rest) = every.split_at(read.len());
        read.iter_mut().zip(own).for_each(|(read, by)| *read |= by);
        read[ranked.anchor] = true;
        let width = joined.meta.columns.len();
        let visible = Visible::new(joined, catalog, given.reader, seeing)?;
        let lookup = Source::new(joined, None, &given.ctx, given.read, visible);
        let lookup = lookup.reading(vec![false; width], rest);
        Ok(Rank {
            formula,
            scope,
            lookup,
            width,
            on,
            anchor: ranked.anchor,
        })
    }

    /// The rank of a row at `distance` as an answer is put in order going up: its negation, so
    /// that highest rank comes first and a NULL rank after every other; and how many stored
    /// rows the lookup of the joined row read. That row is the one of lowest key among those
    /// the reader may see; with none, its columns are NULL. A BOOLEAN is 1 or 0.
    pub(super) fn order(
        &self,
        row: &[Value],
        distance: f64,
        given: &Given,
        reservation: &mut Reservation,
    ) -> Result<(Value, u64)> {
        let (mut lowest, mut joined) = (None, vec![Value::Null; self.width]);
        let path = (self.lookup).given(&[(self.on, Some(&row[self.anchor]))], &[], &[])?;
        // A value that is no key of the joined column, NULL among them, finds no row.
        let read = match path.pinned {
            0 => 0,
            _ => self
                .lookup
                .walk(&path, reservation, |key, values, bytes, reservation| {
                    reservation.shrink(bytes);
                    // An index entry of a long text stands for every text with its digest.
                    let equal =
                        compare(&values[self.on], &row[self.anchor]) == Some(Ordering::Equal);
                    if equal && lowest.as_ref().is_none_or(|lowest| key < *lowest) {
                        (lowest, joined) = (Some(key), values);
                    }
                    Ok(true)
                })?,
        };
        let values = row.iter().cloned().chain(joined);
        let values = values.chain([Value::Float64(1.0 - distance)]);
        let values: Vec<Value> = values
            .map(|value| match value {
                Value::Bool(yes) => Value::Int64(i64::from(yes)),
                value => value,
            })
            .collect();
        let order = match eval(&self.formula, &self.scope, &values, &given.ctx)? {
            Value::Int64(rank) => Value::Float64(-(rank as f64)),
            Value::Float64(rank) => Value::Float64(-rank),
            _ => Value::Null,
        };
        Ok((order, read))
    }
}
