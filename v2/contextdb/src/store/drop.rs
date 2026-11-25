//! Dropping: what a transaction stages to remove whole, and how what nothing can read any more
//! leaves. A dropped table, index, kept count or VECTOR column, the versions a table no longer
//! keeps and the rest of a large value a batch had no room for are owed ranges: a mark in the
//! vector part's table names each, and a batch of the removal loop takes their records, the keys
//! first and then the deletes. A drop that fits one batch leaves nothing behind. One cell goes
//! from every row of a table, one row in memory at a time.

use std::ops::Bound::{Excluded, Included};
use std::ops::Range;

use redb::{ReadableTable, Table};

use super::disk::{self, ByteTable, OWED, Stored};
use super::keys::{self, stored};
use super::record;
use super::{Beside, Passing};
use super::{ChangeSet, CountId, IndexId, STAGED_BYTES, TableId, Undo, VectorId, VectorNumber};
use crate::memory::Reservation;
use crate::types::Result;

/// One thing a transaction has staged to drop.
pub(crate) enum Dropped {
    /// Every row of a table, with its versions, its large values and its row count.
    Rows(TableId),
    /// Every entry of an index.
    Index(IndexId),
    /// Every record of a kept count.
    Kept(CountId),
    /// The cell at this position of every row of a table, with the large value it keeps. A row
    /// that has no cell there stays as it is.
    Cell(TableId, usize),
    /// Every record and number of a VECTOR column, and its rows' place among those unindexed:
    /// the column's pending total.
    Vectors(VectorId, u64),
    /// The versions a table's rows have as of the commit; the rows stay.
    Versions(TableId),
}

/// What an owed range holds, the byte after [`OWED`] in its mark, before the number it names: a
/// VECTOR column's records, an index's entries, a kept count's records, a table's versions and
/// then its rows, versions up to the commit the mark holds (of a table, or of one row's stored
/// key), and the slices of a large value from a place to the end the mark holds.
pub(crate) const VECTOR: u8 = 0;
pub(crate) const INDEX: u8 = 1;
pub(crate) const KEPT: u8 = 2;
pub(crate) const ROWS: u8 = 3;
pub(crate) const VERSIONS: u8 = 4;
pub(crate) const SLICES: u8 = 5;

/// The records one batch of the removal loop removes at most: rows, versions, index entries,
/// vector records and slices of large values.
pub(crate) const BATCH: u64 = 1024;

/// The key of the mark of an owed range.
pub(super) fn mark(source: u8, named: &[u8]) -> Vec<u8> {
    [&[OWED, source][..], named].concat()
}

/// What one batch of the removal loop did: the records it removed, how many of those were
/// versions, and whether it moved on a range at all, if only its stored place.
#[derive(Debug, Default)]
pub(crate) struct Reclaim {
    pub(crate) removed: u64,
    pub(crate) versions: u64,
    pub(crate) advanced: bool,
}

/// The six byte tables in the order the record walk takes them: rows, index entries, kept
/// counts, blocks, the vector part's other records and versions.
pub(super) type Tables<'a, 't> = [&'a mut ByteTable<'t>; 6];
type Slices<'t> = Table<'t, u64, &'static [u8]>;

/// One batch: what it may still examine, what it did, and the marks it wrote less those it
/// removed.
struct Batch<'a> {
    left: u64,
    done: Reclaim,
    held: &'a mut Reservation,
}

impl Batch<'_> {
    /// Frees the slices `all` while the allowance lasts, and owes the rest.
    fn free(
        &mut self,
        t: &mut Tables,
        slices: &mut Slices,
        all: Range<u64>,
        write: &Write,
    ) -> Stored<()> {
        let end = all.end.min(all.start.saturating_add(self.left));
        if end < all.end {
            let rest = mark(SLICES, &end.to_be_bytes());
            t[4].insert_discard(rest.as_slice(), &all.end.to_le_bytes()[..])?;
        }
        for place in all.start..end {
            slices.remove(place)?;
            disk::checkpoint(write)?;
        }
        self.left -= end - all.start;
        self.done.removed += end - all.start;
        Ok(())
    }

    /// Reads the keys of `range` in `table` while the allowance lasts, closing the range before
    /// any is deleted, and whether the range held no more.
    fn keys(
        &mut self,
        table: &ByteTable,
        (from, end): (Vec<u8>, Vec<u8>),
    ) -> Stored<(Vec<Vec<u8>>, bool)> {
        let mut found = Vec::new();
        for pair in table.range::<&[u8]>(from.as_slice()..end.as_slice())? {
            if self.left == 0 {
                return Ok((found, false));
            }
            let key = pair?.0.value().to_vec();
            self.held.grow(64 + key.len() as u64)?;
            found.push(key);
            self.left -= 1;
        }
        Ok((found, true))
    }

    /// Deletes `found` from `table` with the library's ordinary delete.
    fn delete(&mut self, table: &mut ByteTable, found: Vec<Vec<u8>>, write: &Write) -> Stored<()> {
        for key in found {
            drop(table.remove(key.as_slice())?);
            disk::checkpoint(write)?;
            self.held.shrink(64 + key.len() as u64);
            self.done.removed += 1;
        }
        Ok(())
    }
}

type Write = redb::WriteTransaction;

/// Removes what one owed range gives up within the batch's allowance: `None` when that is all
/// it held, or else the mark's value to go on from.
fn take(
    (t, slices): (&mut Tables, &mut Slices),
    (key, value): (&[u8], &[u8]),
    b: &mut Batch,
    cover: &mut Passing,
    write: &Write,
) -> Stored<Option<Vec<u8>>> {
    let (source, named) = (key[1], &key[2..]);
    let bound = value.first_chunk().map(|v| u64::from_le_bytes(*v));
    let bound = bound.ok_or_else(disk::unsound)?;
    if source == SLICES {
        // With no allowance left the mark stays as it is, for the next batch.
        if b.left == 0 {
            return Ok(Some(value.to_vec()));
        }
        let from = named.try_into().map_err(|_| disk::unsound())?;
        b.free(t, slices, u64::from_be_bytes(from)..bound, write)?;
        return Ok(None);
    }
    let id = u32::from_be_bytes(*named.first_chunk().ok_or_else(disk::unsound)?);
    let besides = |bytes: &[u8], cover: &mut Passing| -> Stored<Vec<Beside>> {
        Ok(record::decode_record(bytes, cover)?.besides().collect())
    };
    let mut done = true;
    match source {
        VECTOR => {
            for kind in (0..=keys::LEFT).chain([keys::SLOT]) {
                let range = [id, id + 1].map(|at| [&[kind][..], &at.to_be_bytes()].concat());
                let table = if keys::with_blocks(kind) { 3 } else { 4 };
                let (found, whole) = b.keys(t[table], range.into())?;
                done &= whole;
                b.delete(t[table], found, write)?;
            }
        }
        INDEX | KEPT => {
            let table = usize::from(source);
            let found;
            (found, done) = b.keys(t[table], all(id))?;
            b.delete(t[table], found, write)?;
        }
        _ => {
            let (from, end) = match named.len() {
                4 => all(id),
                _ => ([named, &[0; 8]].concat(), [named, &[0xFF; 9]].concat()),
            };
            let from = from.max([&value[8..], &[0]].concat());
            let (mut taken, mut last) = (Vec::<(Vec<u8>, Vec<Beside>)>::new(), None);
            for pair in t[5].range::<&[u8]>(from.as_slice()..end.as_slice())? {
                if b.left == 0 {
                    done = false;
                    break;
                }
                let (key, version) = pair?;
                let key = key.value().to_vec();
                let commit = key.last_chunk().map(|c| u64::from_be_bytes(*c));
                if commit.ok_or_else(disk::unsound)? <= bound {
                    b.held.grow(64 + key.len() as u64)?;
                    taken.push((key.clone(), besides(version.value(), cover)?));
                }
                (b.left, last) = (b.left - 1, Some(key));
            }
            // Each version is compared with its next holder: the next stored version of its
            // row, across the batch's end too, or else the row.
            let mut gone = Vec::new();
            for (at, (key, held)) in taken.iter().enumerate() {
                let row = &key[..key.len() - 8];
                let next = match taken.get(at + 1).filter(|n| n.0.len() == key.len()) {
                    Some(next) if next.0.starts_with(row) => next.1.clone(),
                    _ => {
                        let end = [row, &[0xFF; 8]].concat();
                        let after = (Excluded(key.as_slice()), Included(end.as_slice()));
                        match t[5].range::<&[u8]>(after)?.next().transpose()? {
                            Some((_, next)) => besides(next.value(), cover)?,
                            None => match t[0].get(row)? {
                                Some(next) => besides(next.value(), cover)?,
                                None => Vec::new(),
                            },
                        }
                    }
                };
                gone.extend(disk::unheld(held, &next).cloned());
            }
            b.done.versions += taken.len() as u64;
            b.delete(t[5], taken.into_iter().map(|(key, _)| key).collect(), write)?;
            if source == ROWS && done {
                let mut rows = Vec::new();
                for pair in t[0].range::<&[u8]>(all(id).0.as_slice()..all(id).1.as_slice())? {
                    if b.left == 0 {
                        done = false;
                        break;
                    }
                    let (key, row) = pair?;
                    rows.push(key.value().to_vec());
                    b.held.grow(64 + rows[rows.len() - 1].len() as u64)?;
                    gone.extend(besides(row.value(), cover)?);
                    b.left -= 1;
                }
                b.delete(t[0], rows, write)?;
            }
            for kept in gone {
                b.free(t, slices, disk::places(&kept)?, write)?;
            }
            if source == VERSIONS && !done {
                return Ok(Some([&value[..8], &last.unwrap_or_default()].concat()));
            }
        }
    }
    Ok((!done).then(|| value.to_vec()))
}

/// Removes one batch of at most [`BATCH`] records and marks examined: from the ranges `given`,
/// or else from the first mark on, going on to the next mark while the allowance lasts. A range
/// not finished keeps its place in its mark; a finished one loses its mark.
pub(super) fn reclaim(
    given: Option<Vec<(Vec<u8>, Vec<u8>)>>,
    (t, slices): (&mut Tables, &mut Slices),
    (held, cover): (&mut Reservation, &mut Passing),
    write: &Write,
) -> Stored<Reclaim> {
    let mut b = Batch {
        left: BATCH,
        done: Reclaim::default(),
        held,
    };
    let mut given = given.map(Vec::into_iter);
    let mut at = vec![OWED];
    while b.left > 0 {
        let next = match &mut given {
            Some(given) => given.next(),
            None => t[4]
                .range::<&[u8]>(at.as_slice()..&[OWED + 1][..])?
                .next()
                .transpose()?
                .map(|(key, value)| (key.value().to_vec(), value.value().to_vec())),
        };
        let Some((key, value)) = next else { break };
        b.left -= 1;
        match take(
            (&mut *t, &mut *slices),
            (&key, &value),
            &mut b,
            cover,
            write,
        )? {
            Some(more) if more == value => {}
            Some(more) => {
                t[4].insert_discard(key.as_slice(), more.as_slice())?;
                b.done.advanced = true;
            }
            None => {
                drop(t[4].remove(key.as_slice())?);
                b.done.advanced = true;
            }
        }
        at = [&key[..], &[0]].concat();
    }
    Ok(b.done)
}

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

/// Marks what `changes` drops as owed, in the order it was staged, and gives those ranges one
/// batch; a dropped object's row count and vector numbers go now. Without a drop it runs the
/// batch the change set asks for. A dropped cell's rewrite counts a row's packing too.
pub(super) fn write(
    changes: &mut ChangeSet,
    t: &mut Tables,
    slices: &mut Slices,
    numbers: &mut Table<'_, u64, u64>,
    write: &redb::WriteTransaction,
) -> Stored<()> {
    let mut cover = changes.reserved.passing();
    let commit = changes.commit_number().0.to_le_bytes();
    let mut marks = Vec::new();
    for dropped in &changes.dropped {
        let (source, id) = match *dropped {
            Dropped::Rows(table) => {
                numbers.remove(disk::count_of(table.0))?;
                (ROWS, table.0)
            }
            Dropped::Index(id) => (INDEX, id.0),
            Dropped::Kept(id) => (KEPT, id.0),
            Dropped::Versions(table) => (VERSIONS, table.0),
            Dropped::Vectors(column, pending) => {
                let unindexed = disk::vector_number_at(VectorNumber::Pending);
                let left = numbers
                    .get(unindexed)?
                    .map_or(0, |n| n.value())
                    .saturating_sub(pending);
                numbers.insert_discard(unindexed, left)?;
                numbers.remove(disk::vector_number_at(VectorNumber::Partitions(column)))?;
                numbers.remove(disk::vector_number_at(VectorNumber::NextPartition(column)))?;
                (VECTOR, column.0)
            }
            Dropped::Cell(table, slot) => {
                // The versions first: a version's cell goes free at the last holder of it in
                // commit order, unless the row holds it, whose own cell goes after.
                let [rows, .., versions] = &mut *t;
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
                            disk::remove_slices(slices, &held, write)?;
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
                    kept.map_or(Ok(()), |kept| disk::remove_slices(slices, &kept, write))
                })?;
                continue;
            }
        };
        let key = mark(source, &id.to_be_bytes());
        t[4].insert_discard(key.as_slice(), &commit[..])?;
        // The versions a table no longer keeps are the loop's; a drop's batch is its own.
        marks.extend((source != VERSIONS).then(|| (key, commit.to_vec())));
        disk::checkpoint(write)?;
    }
    let asked = changes.reclaim.clone();
    if !marks.is_empty() || asked.is_some() {
        let given = (!marks.is_empty()).then_some(marks);
        let reserved = (&mut changes.reserved, &mut cover);
        let done = reclaim(given, (t, slices), reserved, write)?;
        if let Some(asked) = asked {
            *super::locked(&asked) = done;
        }
    }
    // A PURGE leaves no copy of a value in what is still owed: every owed range goes in its
    // commit, batch after batch.
    let owed = [&[OWED][..], &[OWED + 1]];
    while changes.purging && t[4].range::<&[u8]>(owed[0]..owed[1])?.next().is_some() {
        let reserved = (&mut changes.reserved, &mut cover);
        reclaim(None, (t, slices), reserved, write)?;
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
