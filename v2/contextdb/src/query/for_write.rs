//! The rows an UPDATE or DELETE acts on, handed over a batch at a time.

use super::expression::{EvalContext, reads};
use super::table::{Place, Reach, Source};
use super::visible::{Reader, Visible};
use crate::memory::{Budget, Reservation};
use crate::schema::{Catalog, Table};
use crate::sql::Expr;
use crate::store::{Cell, Key, Read};
use crate::types::{Result, Value};

/// What an UPDATE or DELETE asks of its table, the same for every batch.
pub(crate) struct Asked<'a> {
    pub table: &'a Table,
    pub filter: Option<&'a Expr>,
    pub keys: Option<&'a [Key]>,
    /// The columns the statement assigns. No index over one of them is walked, so a row the
    /// statement has changed is never met again.
    pub assigned: &'a [usize],
    /// The columns the caller reads. A large value in any other column is handed over as the
    /// cell it is stored as and never loaded.
    pub needed: &'a [bool],
    pub reader: &'a Reader,
    pub examined: super::Counted<'a>,
    /// What the filter is worked out with: the statement's time and the values of its SELECTs.
    pub ctx: EvalContext<'a>,
}

/// The most rows one batch holds.
const BATCH: usize = 1024;

/// Rows with their keys, each row one cell for each column of its table.
pub(crate) type Batch = Vec<(Key, Vec<Cell>)>;

/// The next batch in the source's undisturbed order. An optional place retains its charge
/// after the batch is dropped; exhaustion clears it. An absent batch means no rows remain.
///
/// The rows are read through the one table source, with the index choice and the evaluator of a
/// SELECT, so a row the reader may not see is never handed over: named by its key, it is no
/// row. The rows stay counted against the memory limit for as long as the caller holds the
/// reservation that comes with them.
///
/// A batch may be staged before the next is asked for. The continuation seeks past its last
/// index entry and row key even when that row has since been deleted.
pub(crate) fn rows_for_write(
    asked: &Asked,
    place: Option<&mut Option<Place>>,
    catalog: &Catalog,
    read: &dyn Read,
    budget: &Budget,
) -> Result<Option<(Batch, Reservation)>> {
    let mut reservation = budget.reserve(0, "query_decode")?;
    let mut batch = Vec::new();
    let mut keep = |(key, cells, _), _: &mut Reservation| {
        batch.push((key, cells));
        Ok(batch.len() < BATCH)
    };
    walk_for_write(asked, place, catalog, read, &mut reservation, &mut keep)?;
    Ok((!batch.is_empty()).then_some((batch, reservation)))
}

/// Walks one table source without releasing or choosing its path between rows.
pub(crate) fn walk_for_write(
    asked: &Asked,
    place: Option<&mut Option<Place>>,
    catalog: &Catalog,
    read: &dyn Read,
    reservation: &mut Reservation,
    each: &mut impl FnMut((Key, Vec<Cell>, u64), &mut Reservation) -> Result<bool>,
) -> Result<()> {
    let Asked { table, filter, .. } = *asked;
    let mut filtered = vec![false; table.meta.columns.len()];
    if let Some(filter) = filter {
        reads(
            filter,
            &crate::query::scope(table),
            asked.ctx.params,
            &mut filtered,
        )?;
    }
    // What the reader's filter holds is given back when the batch is found.
    let mut seeing = reservation.split(0);
    let visible = Visible::new(table, catalog, asked.reader, &mut seeing)?;
    let source = Source::new(table, filter, &asked.ctx, read, visible)
        .counted(asked.examined)
        .reading(filtered, asked.needed);
    let mut path = source.choose(&[], asked.assigned)?;
    path.one = false;
    if let Some(keys) = asked.keys {
        reservation.grow(keys.iter().map(|k| k.as_bytes().len() as u64 + 24).sum())?;
        path.reach = Reach::Keys(keys.to_vec());
        path.checks = None;
    }
    let mut keep = |(key, values, left, bytes): (Key, Vec<Value>, Vec<Cell>, u64),
                    reservation: &mut Reservation| {
        // Source keeps every decoded cell charged, including unasked cells.
        let extra = key.as_bytes().len() as u64;
        reservation.grow(extra)?;
        // A cell that was taken is left as NULL, and its value is the cell.
        let cells = values.into_iter().zip(left).map(|pair| match pair {
            (value, Cell::Here(Value::Null)) => Cell::Here(value),
            (_, left) => left,
        });
        each((key, cells.collect(), bytes + extra), reservation)
    };
    source.cells(&path, place, reservation, &mut keep).map(drop)
}

/// The keys the filter's path reaches, in key order, when it reaches only named keys; the
/// filter is still to be tested on what each holds.
pub(crate) fn named_keys(
    asked: &Asked,
    catalog: &Catalog,
    read: &dyn Read,
    reservation: &mut Reservation,
) -> Result<Option<Vec<Key>>> {
    let visible = Visible::new(asked.table, catalog, asked.reader, reservation)?;
    let source = Source::new(asked.table, asked.filter, &asked.ctx, read, visible);
    let path = source.choose(&[], &[])?;
    Ok(match path.reach {
        Reach::Keys(keys) => Some(keys),
        _ => None,
    })
}
