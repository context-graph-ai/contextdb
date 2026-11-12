//! Dropping: what a transaction stages to remove whole, and how a commit removes it. A table's
//! rows go with their large values and an index's entries go, a storage page at a time with
//! no row held; one cell goes from every row of a table, one row in memory at a time; every
//! record and number of a VECTOR column goes, a storage page at a time.

use redb::{ReadableTable, Table};

use super::disk::{self, Stored};
use super::keys::{self, stored};
use super::record;
use super::{ChangeSet, CountId, IndexId, STAGED_BYTES, TableId, Undo, VectorId, VectorNumber};
use crate::types::Result;

/// One thing a transaction has staged to drop.
pub(crate) enum Dropped {
    /// Every row of a table, with its large values, its row count and its next row number.
    Rows(TableId),
    /// Every entry of an index.
    Index(IndexId),
    /// Every record of a kept count.
    Kept(CountId),
    /// The cell at this position of every row of a table, with the large value it keeps. A row
    /// that has no cell there stays as it is.
    Cell(TableId, usize),
    /// Every record and number of a VECTOR column, and its rows' place among those waiting.
    Vectors(VectorId),
}

impl ChangeSet {
    /// Stages the removal of one thing whole.
    pub(crate) fn stage_drop(&mut self, dropped: Dropped) -> Result<()> {
        self.reserved.grow(STAGED_BYTES)?;
        self.dropped.push(dropped);
        self.calls.push((Undo::Dropped, STAGED_BYTES));
        Ok(())
    }
}

/// Removes what `changes` drops, in the order it was staged; it counts a row's packing too.
pub(super) fn write(
    changes: &mut ChangeSet,
    rows: &mut Table<'_, &'static [u8], &'static [u8]>,
    beside: &mut Table<'_, u64, &'static [u8]>,
    index: &mut Table<'_, &'static [u8], &'static [u8]>,
    counts: &mut Table<'_, &'static [u8], &'static [u8]>,
    numbers: &mut Table<'_, u64, u64>,
    [blocks, vectors]: [&mut Table<'_, &'static [u8], &'static [u8]>; 2],
) -> Stored<()> {
    for dropped in &changes.dropped {
        match *dropped {
            Dropped::Rows(table) => {
                let (from, end) = all(table.0);
                let all = from.as_slice()..end.as_slice();
                for row in rows.extract_from_if::<&[u8], _>(all, |_, _| true)? {
                    for kept in record::besides(&record::plain(row?.1.value())?) {
                        disk::remove_slices(beside, &kept)?;
                    }
                }
                numbers.remove(disk::count_of(table.0))?;
            }
            Dropped::Index(id) => {
                let (from, end) = all(id.0);
                index.retain_in::<&[u8], _>(from.as_slice()..end.as_slice(), |_, _| false)?;
            }
            Dropped::Kept(id) => {
                let (from, end) = all(id.0);
                counts.retain_in::<&[u8], _>(from.as_slice()..end.as_slice(), |_, _| false)?;
            }
            Dropped::Cell(table, slot) => {
                let (mut from, end) = all(table.0);
                loop {
                    let mut after = rows.range::<&[u8]>(from.as_slice()..end.as_slice())?;
                    let Some(next) = after.next() else { break };
                    // The row is copied out, so that no page is lent while the table changes.
                    let (key, without) = {
                        let (key, row) = next?;
                        (
                            key.value().to_vec(),
                            record::without_cell(&record::plain(row.value())?, slot),
                        )
                    };
                    drop(after);
                    if let Some((mut without, kept)) = without {
                        let room = record::pack(&mut without, &mut changes.reserved);
                        rows.insert(key.as_slice(), without.as_slice())?;
                        changes.reserved.shrink(room);
                        if let Some(kept) = kept {
                            disk::remove_slices(beside, &kept)?;
                        }
                    }
                    from = key;
                    from.push(0);
                }
            }
            Dropped::Vectors(column) => {
                let mut pending = 0;
                for kind in 0..=keys::LEFT {
                    let table = if keys::with_blocks(kind) {
                        &mut *blocks
                    } else {
                        &mut *vectors
                    };
                    let [from, end] = [column.0, column.0 + 1]
                        .map(|at| [&[kind][..], &at.to_be_bytes()].concat());
                    table.retain_in::<&[u8], _>(from.as_slice()..end.as_slice(), |_, _| {
                        pending += u64::from(kind == keys::PENDING);
                        false
                    })?;
                }
                let waiting = disk::vector_number_at(VectorNumber::Pending);
                let left = numbers
                    .get(waiting)?
                    .map_or(0, |n| n.value())
                    .saturating_sub(pending);
                numbers.insert(waiting, left)?;
                numbers.remove(disk::vector_number_at(VectorNumber::Partitions(column)))?;
                numbers.remove(disk::vector_number_at(VectorNumber::NextPartition(column)))?;
            }
        }
    }
    Ok(())
}

/// The first stored key of a table or an index and the end of its keys: after the number, no
/// key begins with 0xFF.
fn all(id: u32) -> (Vec<u8>, Vec<u8>) {
    (stored(id, &[]), stored(id, &[0xFF]))
}
