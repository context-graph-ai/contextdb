//! Sealing: moving the vectors on pending lists into blocks.

use super::block::{BLOCK_BYTES, BROKEN, BlockReader, BlockWriter};
use super::directory;
use super::grouping;
use super::sketch::{sketch_len, sketches_of};
use super::tally::{position, tuples};
use crate::memory::{Budget, Reservation};
use crate::schema::Catalog;
use crate::store::{Cell, ChangeSet, Key, Read, Row, VectorId, VectorNumber, VectorRecord};
use crate::types::{ColumnType, Error, Result, Value};

/// What one call of [`seal`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Sealed {
    /// How many vectors it moved into blocks.
    pub done: u64,
    /// How many are still pending, over every column.
    pub remaining: u64,
}

/// What a store is told when a pending record is not of a row that has a vector in a
/// partition of a VECTOR column, which [`super::stage`] keeps true of every one.
const WAITING: &str = "a pending record of the vector part is of no row with a vector";

/// Stages the sealing of the first `most` pending vectors, in the order of column number,
/// partition number and row key.
///
/// The vectors of a partition are added to its last tail block while that block has room and
/// is not closed, and to new blocks after it, so every block from the first that is not closed
/// on but the last is full. Each is read from its row at full precision, one at a time, and
/// its pending record is removed. In a partition that has a mean every block that is staged
/// comes with the record of its sketches, each taken from the mean. Nothing is staged when
/// nothing is pending.
///
/// Every byte is reserved before it is taken. The blocks, the removals and the partition
/// records it stages are charged to the change set's reservation as they are staged. What it
/// works with is held by one reservation it takes from `budget`, named `maintenance`: the keys
/// of the pending records it collected, one block being put together (`BLOCK_BYTES`, taken once
/// and kept for the call), the partition record it is adding to, the partition's mean, the
/// record of one block's sketches until it is staged, and one row with its vector, given back
/// before the next is read.
///
/// A pending record, a partition record, a block or a count that does not agree with the rows
/// is `StoreCorrupted`, and nothing is staged.
///
/// It works a partition at a time. When the memory limit has no room for a further partition,
/// that partition is left as it was and the call ends with those before it staged. It is
/// refused only when there is no room for the first partition, and then nothing is staged.
pub(crate) fn seal(
    changes: &mut ChangeSet,
    catalog: &Catalog,
    budget: &Budget,
    most: usize,
) -> Result<Sealed> {
    let mut sealed = Sealed {
        done: 0,
        remaining: changes.vector_number(VectorNumber::Pending)?,
    };
    if sealed.remaining == 0 {
        return Ok(sealed);
    }
    // One block is put together at a time, so its bytes are taken once.
    let mut held = budget.reserve(BLOCK_BYTES as u64, "maintenance")?;
    let mut next = Some((VectorId(0), 0));
    while let Some(from) = next
        && (sealed.done as usize) < most
    {
        let mark = changes.mark();
        let room = most - sealed.done as usize;
        match seal_partition(changes, catalog, &mut held, from, room, sealed.remaining) {
            Ok((done, following)) => {
                sealed.done += done;
                sealed.remaining -= done;
                next = following;
            }
            Err(refused) => {
                changes.rewind(mark);
                let no_room = matches!(refused, Error::MemoryBudgetExceeded { .. });
                if sealed.done == 0 || !no_room {
                    return Err(refused);
                }
                break;
            }
        }
        held.shrink(held.bytes() - BLOCK_BYTES as u64);
    }
    Ok(sealed)
}

/// Stages the sealing of one partition: the first that has a pending record at `from`, a
/// column and a partition number, or after it. Seals at most `most` of its vectors, of the
/// `remaining` that are pending over every column. Answers how many it sealed and the next
/// column and partition number with a pending record, `None` when it met none.
fn seal_partition(
    changes: &mut ChangeSet,
    catalog: &Catalog,
    held: &mut Reservation,
    from: (VectorId, u32),
    most: usize,
    remaining: u64,
) -> Result<(u64, Option<(VectorId, u32)>)> {
    let first = Key::from_values(&[], &[])?;
    let (mut found, mut following) = (None, None);
    let mut keys: Vec<Key> = Vec::new();
    changes.vectors(
        VectorRecord::Pending(from.0, from.1, &first),
        &mut |record, _| {
            let VectorRecord::Pending(vector, number, key) = record else {
                return Ok(false);
            };
            if *found.get_or_insert((vector, number)) != (vector, number) {
                following = Some((vector, number));
                return Ok(false);
            }
            room(&mut keys, most, held)?;
            held.grow(key.as_bytes().len() as u64)?;
            keys.push(key.clone());
            Ok(keys.len() < most)
        },
    )?;
    let Some((vector, number)) = found else {
        return Ok((0, None));
    };
    let waiting = catalog.vector(vector);
    let (table, column) = waiting.ok_or_else(|| changes.damaged(WAITING))?;
    let at = position(table, &column.column);
    let ColumnType::Vector(dimension) = table.meta.columns[at].column_type else {
        unreachable!("a VECTOR column is of the type VECTOR")
    };

    // The partition is found through the key tuple of its first pending row.
    let row = changes.row(table.id, &keys[0])?;
    let (tuple, _) = tuples(table, column, row.as_ref(), None, &*changes, held)?;
    let tuple = tuple.ok_or_else(|| changes.damaged(WAITING))?;
    held.grow(tuple.as_bytes().len() as u64 + 64)?;
    let partition = directory::partition(&*changes, vector, &tuple)?;
    let mut partition = partition.ok_or_else(|| changes.damaged(WAITING))?;

    // The mean is read once, and held until the partition is done.
    let mean = partition
        .mean
        .then(|| grouping::mean(&*changes, vector, number, dimension, held))
        .transpose()?;
    let mean = mean.as_deref();

    // Its last block is added to while it has room and is not closed. A writer is made, after
    // the block before it is staged, with room for exactly the slots still to come, which
    // `left` counts.
    let open = partition.blocks > partition.closed;
    let mut block = partition.blocks - u32::from(open);
    let begun = |last: &[u8]| {
        let whole = BlockReader::new(last, dimension).is_whole();
        whole.then(|| BlockWriter::new(dimension, Some(last), &keys))
    };
    let last = if open {
        directory::kept(&*changes, VectorRecord::Block(vector, number, block), begun)?
    } else {
        None
    };
    let (mut writer, mut left) = match last {
        Some(begun) => begun.ok_or_else(|| changes.damaged(BROKEN))?,
        None => BlockWriter::new(dimension, None, &keys),
    };
    // A row is its cells and, when the row itself holds it, its vector; a vector kept beside
    // its row is counted as it is loaded.
    let width = table.meta.columns.len();
    let row_bytes = size_of::<Row>() + width * size_of::<Cell>() + 4 * dimension;
    for (done, key) in keys.iter().enumerate() {
        let taken = held.bytes();
        held.grow(row_bytes as u64)?;
        let row = changes.row(table.id, key)?;
        let mut row = row.ok_or_else(|| changes.damaged(WAITING))?;
        let numbers = match row.cells.swap_remove(at) {
            Cell::Here(numbers) => numbers,
            Cell::Beside(beside) => changes.load(&beside, held)?,
        };
        let Value::Vector(numbers) = numbers else {
            return Err(changes.damaged(WAITING));
        };
        if left == 0 {
            // A block that was full before this call is left as it is.
            if !writer.is_empty() {
                put_block(changes, held, (vector, number, block), writer, mean)?;
            }
            block += 1;
            (writer, left) = BlockWriter::new(dimension, None, &keys[done..]);
        }
        writer.push(key, &numbers);
        left -= 1;
        drop((row, numbers));
        held.shrink(held.bytes() - taken);
        changes.delete_vector(VectorRecord::Pending(vector, number, key))?;
    }
    put_block(changes, held, (vector, number, block), writer, mean)?;
    partition.blocks = block + 1;
    let sealed = keys.len() as u64;
    partition.slots += sealed;
    partition.sealed += sealed;
    changes.put_vector(VectorRecord::Partition(vector, &tuple), partition.encode())?;
    let left = remaining.checked_sub(sealed);
    let left = left.ok_or_else(|| changes.damaged(directory::SHORT))?;
    changes.put_vector_number(VectorNumber::Pending, left)?;
    Ok((sealed, following))
}

/// Stages a block of `bytes` bytes under its column, the number its blocks are kept under and
/// its own. `finish` makes those bytes while the writer still holds its own, so they are asked
/// for before it. When it returns the writer's own are gone, and the finished block is the one
/// block the call keeps `BLOCK_BYTES` for: what was asked is given back, and the change set
/// counts the block once, as the record it stages.
///
/// With the `mean` of a partition that has one, the record of the block's sketches is staged
/// under the same numbers, each sketch taken from the mean: its bytes are asked for before
/// they are made and given back when the change set counts them.
fn put_block(
    changes: &mut ChangeSet,
    held: &mut Reservation,
    (vector, under, number): (VectorId, u32, u32),
    writer: BlockWriter,
    mean: Option<&[f32]>,
) -> Result<()> {
    let bytes = writer.bytes();
    held.grow(bytes as u64)?;
    let block = writer.finish();
    held.shrink(bytes as u64);
    if let Some(mean) = mean {
        let slots = BlockReader::new(&block, mean.len()).len();
        let bytes = (4 + slots * sketch_len(mean.len())) as u64;
        held.grow(bytes)?;
        let sketches = sketches_of(&block, mean.len(), &mut |_| mean);
        held.shrink(bytes);
        changes.put_vector(VectorRecord::Sketch(vector, under, number), sketches)?;
    }
    changes.put_vector(VectorRecord::Block(vector, under, number), block)
}

/// Makes room for one more entry in a list that has no place left: the list is made again
/// with as many more, and never past `most` entries, the bytes of the new one taken from
/// `held` before it is made and those of the old one given back once it is gone. What an
/// entry points at is the caller's to take.
pub(super) fn room<T>(list: &mut Vec<T>, most: usize, held: &mut Reservation) -> Result<()> {
    if list.len() == list.capacity() {
        let more = list.capacity().max(4).min(most - list.len());
        held.grow(((list.len() + more) * size_of::<T>()) as u64)?;
        list.reserve_exact(more);
        held.shrink((list.len() * size_of::<T>()) as u64);
    }
    Ok(())
}
