//! Sealing: moving the vectors on pending lists into blocks.

use super::binary::{code_len, codes_of};
use super::block::{BLOCK_BYTES, BROKEN, BlockReader, BlockWriter};
use super::directory;
use super::ivf;
use super::pack;
use super::tally::{position, tuples};
use crate::memory::{Budget, Reservation};
use crate::schema::Catalog;
use crate::store::{Cell, ChangeSet, Key, Read, VectorId, VectorNumber, VectorRecord};
use crate::types::{ColumnType, Error, Result, Value};

/// What a store is told when a pending record is not of a row that has a vector in a
/// partition of a VECTOR column, which [`super::track_unindexed`] keeps true of every one.
const UNINDEXED: &str = "a pending record of the vector part is of no row with a vector";

/// Stages the sealing of the first `most` pending vectors, in the order of column number,
/// partition number and row key.
///
/// Returns the vectors moved and the rows read, counted at their lookups.
///
/// The vectors of a partition are added to its last tail block while that block has room and
/// is not closed, and to new blocks after it, so every block from the first that is not closed
/// on but the last is full. Each is read from its row at full precision, one at a time, and
/// its pending record is removed. In a partition that has a mean every block that is staged
/// comes with the record of its binary codes, each taken from the mean. Nothing is staged when
/// nothing is pending.
///
/// Every byte is reserved before it is taken. The blocks, the removals and the partition
/// records it stages are charged to the change set's reservation as they are staged. What it
/// works with is held by one reservation it takes from `budget`, named `maintenance`: the keys
/// of the pending records it collected, one block being put together (`BLOCK_BYTES`, taken once
/// and kept for the call), the partition record it is adding to, the partition's mean, the
/// record of one block's binary codes until it is staged, and one row with its vector, given back
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
) -> Result<u64> {
    let mut done = 0;
    let mut remaining = changes.vector_number(VectorNumber::Pending)?;
    if remaining == 0 {
        return Ok(done);
    }
    // One block is put together at a time, so its bytes are taken once.
    let mut held = budget.reserve(BLOCK_BYTES as u64, "maintenance")?;
    let mut next = Some((VectorId(0), 0));
    while let Some(from) = next
        && (done as usize) < most
    {
        let mark = changes.mark();
        let room = most - done as usize;
        match seal_partition(changes, catalog, &mut held, from, room, remaining) {
            Ok((sealed, following)) => {
                done += sealed;
                remaining -= sealed;
                next = following;
            }
            Err(refused) => {
                changes.rewind(mark);
                let no_room = matches!(refused, Error::MemoryBudgetExceeded { .. });
                if done == 0 || !no_room {
                    return Err(refused);
                }
                break;
            }
        }
        held.shrink(held.bytes() - BLOCK_BYTES as u64);
    }
    Ok(done)
}

/// Bytes one pending vector will occupy in a block, beside its four-byte count.
pub(super) fn bytes(dimension: usize, key: &Key) -> u64 {
    (super::slot::slot_len(dimension) + 4 + key.as_bytes().len()) as u64
}

/// A column and partition number on the pending list.
type PartitionAt = (VectorId, u32);

/// Stages the sealing of one partition: the first that has a pending record at `from`, a
/// column and a partition number, or after it. Seals at most `most` of its vectors, of the
/// `remaining` that are pending over every column. Answers how many it sealed and the next
/// column and partition number with a pending record, `None` when it met none, and the rows read.
pub(super) fn seal_partition(
    changes: &mut ChangeSet,
    catalog: &Catalog,
    held: &mut Reservation,
    from: PartitionAt,
    most: usize,
    remaining: u64,
) -> Result<(u64, Option<PartitionAt>)> {
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
            held.room(&mut keys, most)?;
            held.grow(key.as_bytes().len() as u64)?;
            keys.push(key.clone());
            Ok(keys.len() < most)
        },
    )?;
    let Some((vector, number)) = found else {
        return Ok((0, None));
    };
    let unindexed = catalog.vector(vector);
    let (table, column) = unindexed.ok_or_else(|| changes.damaged(UNINDEXED))?;
    let at = position(table, &column.column, changes)?;
    let ColumnType::Vector(dimension) = table.meta.columns[at].column_type else {
        unreachable!("a VECTOR column is of the type VECTOR")
    };

    // The partition is found through the key tuple of its first pending row.
    let mut cover = held.passing();
    let mut first_row = changes.row(table.id, &keys[0], &mut cover)?;
    let (tuple, _) = tuples(table, column, first_row.as_ref(), None, &*changes, held)?;
    let tuple = tuple.ok_or_else(|| changes.damaged(UNINDEXED))?;
    held.grow(tuple.as_bytes().len() as u64 + 64)?;
    let partition = directory::partition(&*changes, column, &tuple)?;
    let mut partition = partition.ok_or_else(|| changes.damaged(UNINDEXED))?;

    // The mean is read once, and held until the partition is done.
    let mean = partition
        .ivf
        .filter(|g| g.groups > 1)
        .map(|g| ivf::mean(&*changes, vector, g.number, dimension, held))
        .transpose()?;
    // Unlike pack, seal holds BLOCK_BYTES throughout and the change set charges each staged record.
    let put = |changes: &mut ChangeSet, held: &mut Reservation, block, writer| {
        let reference = mean.as_deref().map(|mean| move |_| mean);
        let sink = |record, value| changes.put_vector(record, value);
        let held = (held, Reservation::shrink);
        let reference = (dimension, reference);
        put_block((vector, number, block), writer, held, reference, sink)
    };

    // Its last block is added to while it has room and is not closed. A writer is made, after
    // the block before it is staged, with room for exactly the slots still to come, which
    // `left` counts.
    let open = partition.blocks > partition.closed && partition.tail_room > 0;
    let mut block = partition.blocks - u32::from(open);
    let begun = |b: &[u8]| {
        pack::block(&*changes, b, dimension).map(|_| BlockWriter::new(dimension, Some(b), &keys))
    };
    let (mut writer, mut left) = if open {
        let last = VectorRecord::Block(vector, number, block);
        directory::kept(&*changes, last, begun)?.ok_or_else(|| changes.damaged(BROKEN))??
    } else {
        BlockWriter::new(dimension, None, &keys)
    };
    // A row is its cells and, when the row itself holds it, its vector; a vector kept beside
    // its row is counted as it is loaded.
    for (done, key) in keys.iter().enumerate() {
        let taken = held.bytes();
        let row = if done == 0 {
            first_row.take()
        } else {
            changes.row(table.id, key, &mut cover)?
        };
        let mut row = row.ok_or_else(|| changes.damaged(UNINDEXED))?;
        let numbers = match row.cells.swap_remove(at) {
            Cell::Here(numbers) => numbers,
            Cell::Beside(beside) => changes.load(&beside, held)?,
        };
        let Value::Vector(numbers) = numbers else {
            return Err(changes.damaged(UNINDEXED));
        };
        if left == 0 {
            // A block that was full before this call is left as it is.
            if writer.len() > 0 {
                put(changes, held, block, writer)?;
            }
            block += 1;
            (writer, left) = BlockWriter::new(dimension, None, &keys[done..]);
        }
        writer.push(key, &numbers);
        let slot = [number, block, writer.len() as u32 - 1];
        directory::hold(changes, (vector, &partition), key, Some(slot), held)?;
        left -= 1;
        drop((row, numbers));
        held.shrink(held.bytes() - taken);
        changes.delete_vector(VectorRecord::Pending(vector, number, key))?;
    }
    partition.tail_room = BLOCK_BYTES.saturating_sub(writer.bytes()) as u32;
    put(changes, held, block, writer)?;
    partition.blocks = block + 1;
    let sealed = keys.len() as u64;
    partition.slots += sealed;
    partition.sealed += sealed;
    let moved_bytes: u64 = keys.iter().map(|key| bytes(dimension, key)).sum();
    partition.pending_bytes = partition
        .pending_bytes
        .checked_sub(moved_bytes)
        .ok_or_else(|| changes.damaged(directory::SHORT))?;
    partition.unindexed = partition
        .unindexed
        .checked_sub(keys.len() as u32)
        .ok_or_else(|| changes.damaged(directory::SHORT))?;
    changes.put_vector(VectorRecord::Partition(vector, &tuple), partition.encode())?;
    let left = remaining.checked_sub(sealed);
    let left = left.ok_or_else(|| changes.damaged(directory::SHORT))?;
    changes.put_vector_number(VectorNumber::Pending, left)?;
    Ok((sealed, following))
}

/// Stages again the binary codes of a block of `under` from `mean`, or, with none, removes them.
pub(super) fn recode(
    changes: &mut ChangeSet,
    (vector, under, at): (VectorId, u32, u32),
    dimension: usize,
    mean: Option<&[f32]>,
    held: &mut Reservation,
) -> Result<()> {
    let code = VectorRecord::Binary(vector, under, at);
    let Some(mean) = mean else {
        return changes.delete_vector(code);
    };
    let block = VectorRecord::Block(vector, under, at);
    let codes = pack::with_block(&*changes, block, dimension, &mut |slots| {
        held.grow((4 + slots.len() * code_len(dimension)) as u64)?;
        Ok(codes_of(slots, dimension, &mut |_| mean))
    })?;
    held.shrink(codes.len() as u64);
    changes.put_vector(code, codes)
}

/// Finishes a block with its output reserved first, then writes its binary codes and block.
/// The caller releases each finished allocation before staging it, or retains it for its sink.
pub(super) fn put_block<'a>(
    (vector, under, number): (VectorId, u32, u32),
    writer: BlockWriter,
    (held, mut release): (&mut Reservation, impl FnMut(&mut Reservation, u64)),
    (dimension, reference): (usize, Option<impl FnMut((u32, u32)) -> &'a [f32]>),
    mut put: impl FnMut(VectorRecord<'static>, Vec<u8>) -> Result<()>,
) -> Result<()> {
    let bytes = writer.bytes() as u64;
    held.grow(bytes)?;
    let block = writer.finish();
    release(held, bytes);
    if let Some(mut reference) = reference {
        let slots = BlockReader::new(&block, dimension);
        let bytes = (4 + slots.len() * code_len(dimension)) as u64;
        held.grow(bytes)?;
        let codes = codes_of(&slots, dimension, &mut |slot| {
            reference((number, slot as u32))
        });
        release(held, bytes);
        put(VectorRecord::Binary(vector, under, number), codes)?;
    }
    put(VectorRecord::Block(vector, under, number), block)
}
