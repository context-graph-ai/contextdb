//! Dropping: what a transaction stages to remove whole, and how a commit removes it. A table's
//! rows go with their large values and an index's entries go, a storage page at a time with
//! no row held; one cell goes from every row of a table, one row in memory at a time; every
//! record and number of a VECTOR column goes, a storage page at a time. A table's versions go
//! with its rows, and lose a dropped cell as its rows do.

use redb::{ReadableTable, Table};

use super::disk::{self, ByteTable, Stored};
use super::keys::{self, stored};
use super::record;
use super::{Beside, Passing, Row};
use super::{ChangeSet, CountId, IndexId, STAGED_BYTES, TableId, Undo, VectorId, VectorNumber};
use crate::memory::Reservation;
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
    /// Every record and number of a VECTOR column, and its rows' place among those unindexed.
    Vectors(VectorId),
    /// The versions of a table's rows, of at most [`VERSION_KEYS`] keys; the rows stay.
    Versions(TableId),
}

/// The keys whose versions one maintenance step removes.
pub(crate) const VERSION_KEYS: usize = 1024;

impl ChangeSet {
    /// Stages the removal of one thing whole.
    pub(crate) fn stage_drop(&mut self, dropped: Dropped) -> Result<()> {
        self.write_room(STAGED_BYTES)?;
        self.undo.room(&mut self.dropped, usize::MAX)?;
        self.dropped.push(dropped);
        self.calls.push((Undo::Dropped, STAGED_BYTES));
        Ok(())
    }
}

/// Removes what `changes` drops, in the order it was staged; it counts a row's packing too.
pub(super) fn write(
    changes: &mut ChangeSet,
    (rows, versions, beside): (
        &mut ByteTable<'_>,
        &mut ByteTable<'_>,
        &mut Table<'_, u64, &'static [u8]>,
    ),
    index: &mut Table<'_, &'static [u8], &'static [u8]>,
    counts: &mut Table<'_, &'static [u8], &'static [u8]>,
    numbers: &mut Table<'_, u64, u64>,
    [blocks, vectors]: [&mut Table<'_, &'static [u8], &'static [u8]>; 2],
    write: &redb::WriteTransaction,
) -> Stored<()> {
    let mut cover = changes.reserved.passing();
    for dropped in &changes.dropped {
        match *dropped {
            Dropped::Rows(table) | Dropped::Versions(table) => {
                let (from, end) = all(table.0);
                let rows_too = matches!(dropped, Dropped::Rows(_));
                for _ in 0..if rows_too { usize::MAX } else { VERSION_KEYS } {
                    // The range holds a page of this transaction. Close it before release,
                    // which frees a page the first pass allocated.
                    let key = {
                        let mut first = versions.range::<&[u8]>(from.as_slice()..end.as_slice())?;
                        let Some(first) = first.next().transpose()?.map(|k| k.0.value().to_vec())
                        else {
                            break;
                        };
                        first[..first.len().saturating_sub(8)].to_vec()
                    };
                    let row = rows
                        .get(key.as_slice())?
                        .map(|row| record::decode_record(row.value(), &mut cover));
                    let after: Vec<_> = row
                        .transpose()?
                        .into_iter()
                        .flat_map(Row::besides)
                        .collect();
                    disk::release(
                        (versions, beside),
                        key.as_slice(),
                        None,
                        &after,
                        &mut cover,
                        write,
                    )?;
                }
                if rows_too {
                    let all = from.as_slice()..end.as_slice();
                    for row in rows.extract_from_if::<&[u8], _>(all, |_, _| true)? {
                        let row = record::decode_record(row?.1.value(), &mut cover)?;
                        for kept in row.besides() {
                            disk::remove_slices(beside, &kept, write)?;
                        }
                        disk::checkpoint(write)?;
                    }
                    numbers.remove(disk::count_of(table.0))?;
                }
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
                // The versions first: a version's cell goes free at the last holder of it in
                // commit order, unless the row holds it, whose own cell goes after.
                let mut second = changes.reserved.passing();
                let mut cell = |key: &[u8]| -> Stored<Option<Beside>> {
                    let Some(row) = rows.get(key)? else {
                        return Ok(None);
                    };
                    let plain = record::plain(row.value(), &mut second)?;
                    Ok(record::without_cell(&plain, slot)?.and_then(|cell| cell.1))
                };
                let mut last: Option<(Vec<u8>, Option<Beside>)> = None;
                let mut held = |key: Option<&[u8]>, kept: Option<Beside>| -> Stored<()> {
                    let key = key.map(|key| &key[..key.len().saturating_sub(8)]);
                    if let Some((of, Some(held))) = last.take() {
                        let next = if key == Some(&of[..]) {
                            kept.clone()
                        } else {
                            cell(&of)?
                        };
                        if next.as_ref() != Some(&held) {
                            disk::remove_slices(beside, &held, write)?;
                        }
                    }
                    last = key.map(|key| (key.to_vec(), kept));
                    Ok(())
                };
                let reserved = (&mut changes.reserved, &mut cover);
                rewrite(versions, table, slot, reserved, write, |k: &[u8], v| {
                    held(Some(k), v)
                })?;
                held(None, None)?;
                let reserved = (&mut changes.reserved, &mut cover);
                rewrite(rows, table, slot, reserved, write, |_, kept| {
                    kept.map_or(Ok(()), |kept| disk::remove_slices(beside, &kept, write))
                })?;
            }
            Dropped::Vectors(column) => {
                let mut pending = 0;
                for kind in (0..=keys::LEFT).chain([keys::SLOT]) {
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
                let unindexed = disk::vector_number_at(VectorNumber::Pending);
                let left = numbers
                    .get(unindexed)?
                    .map_or(0, |n| n.value())
                    .saturating_sub(pending);
                numbers.insert_discard(unindexed, left)?;
                numbers.remove(disk::vector_number_at(VectorNumber::Partitions(column)))?;
                numbers.remove(disk::vector_number_at(VectorNumber::NextPartition(column)))?;
            }
        }
        disk::checkpoint(write)?;
    }
    Ok(())
}

/// Writes each record of a table's run in `records` without the cell at `slot`, one at a time
/// in key order, and hands `kept` its key and the large value the cell kept.
fn rewrite(
    records: &mut ByteTable<'_>,
    table: TableId,
    slot: usize,
    (reserved, cover): (&mut Reservation, &mut Passing),
    write: &redb::WriteTransaction,
    mut kept: impl FnMut(&[u8], Option<Beside>) -> Stored<()>,
) -> Stored<()> {
    let (mut from, end) = all(table.0);
    loop {
        let mut after = records.range::<&[u8]>(from.as_slice()..end.as_slice())?;
        let Some(next) = after.next() else { break };
        // The record is copied out, so that no page is lent while the table changes.
        let (key, without) = {
            let (key, row) = next?;
            let plain = record::plain(row.value(), cover)?;
            cover.cover(2 * plain.len() as u64)?;
            (key.value().to_vec(), record::without_cell(&plain, slot)?)
        };
        drop(after);
        let mut held = None;
        if let Some((mut without, cell)) = without {
            let room = record::pack(&mut without, reserved);
            records.insert_discard(key.as_slice(), without.as_slice())?;
            reserved.shrink(room);
            held = cell;
        }
        kept(&key, held)?;
        disk::checkpoint(write)?;
        from = key;
        from.push(0);
    }
    Ok(())
}

/// The first stored key of a table or an index and the end of its keys: after the number, no
/// key begins with 0xFF.
fn all(id: u32) -> (Vec<u8>, Vec<u8>) {
    (stored(id, &[]), stored(id, &[0xFF]))
}
