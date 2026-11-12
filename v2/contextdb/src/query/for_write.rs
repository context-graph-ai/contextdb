//! The rows an UPDATE or DELETE acts on, handed over a batch at a time.

use std::collections::HashMap;

use super::expression::reads;
use super::table::Source;
use super::visible::{Reader, Visible};
use crate::memory::{Budget, Reservation};
use crate::schema::{Catalog, Table};
use crate::sql::Expr;
use crate::store::{Cell, Key, Read};
use crate::types::heap_bytes;
use crate::types::{Result, Value};

/// What an UPDATE or DELETE asks of its table, the same for every batch.
pub(crate) struct Asked<'a> {
    pub table: &'a Table,
    pub filter: Option<&'a Expr>,
    /// The columns the statement assigns. No index over one of them is walked, so a row the
    /// statement has changed is never met again.
    pub assigned: &'a [usize],
    /// The columns the caller reads. A large value in any other column is handed over as the
    /// cell it is stored as and never loaded.
    pub needed: &'a [bool],
    pub reader: &'a Reader,
    pub params: &'a HashMap<String, Value>,
    pub now: i64,
}

/// The most rows one batch holds.
const BATCH: usize = 1024;

/// Rows with their keys, each row one cell for each column of its table.
pub(crate) type Batch = Vec<(Key, Vec<Cell>)>;

/// The next rows an UPDATE or DELETE acts on: those after the row with the key `after` in the
/// order the table source walks, which the statement cannot disturb. `None` when there are no
/// more.
///
/// The rows are read through the one table source, with the index choice and the evaluator of a
/// SELECT, so a row the reader may not see is never handed over: named by its key, it is no
/// row. The rows stay counted against the memory limit for as long as the caller holds the
/// reservation that comes with them.
///
/// Read through a change set, a batch may be staged before the next is asked for. A walk by
/// an index then passes the entries before its place again, reading no row for them.
pub(crate) fn rows_for_write(
    asked: &Asked,
    after: Option<&Key>,
    catalog: &Catalog,
    read: &dyn Read,
    budget: &Budget,
) -> Result<Option<(Batch, Reservation)>> {
    let Asked { table, filter, .. } = *asked;
    let mut filtered = vec![false; table.meta.columns.len()];
    if let Some(filter) = filter {
        reads(filter, table, asked.params, &mut filtered)?;
    }
    // What the reader's filter holds is given back when the batch is found.
    let mut seeing = budget.reserve(0, "query")?;
    let visible = Visible::new(table, catalog, asked.reader, &mut seeing)?;
    let source = Source::new(table, filter, asked.params, asked.now, read, visible)
        .reading(filtered, asked.needed);
    let mut path = source.choose(&[], asked.assigned)?;
    path.one = false;
    let mut reservation = budget.reserve(0, "query")?;
    let mut batch = Vec::new();
    let mut keep = |(key, values, left, _): (Key, Vec<Value>, Vec<Cell>, u64),
                    reservation: &mut Reservation| {
        // Beside the values that were asked for: the key, and each small value that was not.
        let small = left.iter().map(|cell| match cell {
            Cell::Here(value) => heap_bytes(value),
            Cell::Beside(_) => 0,
        });
        reservation.grow(key.as_bytes().len() as u64 + small.sum::<u64>())?;
        // A cell that was taken is left as NULL, and its value is the cell.
        let cells = values.into_iter().zip(left).map(|pair| match pair {
            (value, Cell::Here(Value::Null)) => Cell::Here(value),
            (_, left) => left,
        });
        batch.push((key, cells.collect()));
        Ok(batch.len() < BATCH)
    };
    source.cells(&path, after, &mut reservation, &mut keep)?;
    Ok((!batch.is_empty()).then_some((batch, reservation)))
}
