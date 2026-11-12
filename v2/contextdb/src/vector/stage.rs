//! What a commit's row changes mean for the vector part, worked out once, inside the commit
//! function, from the complete change set.

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;

use super::build::Build;
use super::directory::{self, Partition};
use super::pack;
use super::seal::room;
use super::tally::{position, tuples};
use crate::memory::{Budget, Reservation};
use crate::schema::Catalog;
use crate::sql::not_supported;
use crate::store::{Cell, ChangeSet, Key, Read, VectorId, VectorNumber, VectorRecord};
use crate::types::{Error, Result};

/// One staged row whose vector leaves a key tuple, arrives under one, or both.
struct Move {
    vector: VectorId,
    row: Key,
    leaves: Option<Key>,
    enters: Option<Key>,
}

/// One partition the commit's rows touch.
struct Touched {
    /// Its record at the committed moment, `None` when it has none.
    was: Option<Partition>,
    /// Its record as the commit leaves it: with no live vector, it has none.
    now: Partition,
}

/// What the commit does to the partitions of one column.
#[derive(Default)]
struct Count {
    /// How many it makes less how many it empties.
    change: i64,
    made: u64,
}

/// Stages the vector records that the rows staged in `changes` call for.
///
/// For every staged row of a table with a VECTOR column, the row as the change set's committed
/// moment has it is compared with the row as staged. A vector that is new, changed, or under
/// other partition key values goes on its partition's pending list; the list it was on, if it
/// was on one, lets it go, and a vector that was on none leaves a sealed slot, which a `Left`
/// record says from then on. A partition is made when its first vector arrives and removed,
/// with every record kept for it, when its last one leaves. A row whose vector and partition
/// key values are as they were stages nothing.
///
/// Every vector that leaves is taken out before any arrives, so at the limit a commit may
/// move the last vector of one partition into a new one.
///
/// The commit function calls this under the commit lock, after the change set is laid over the
/// newest moment and every other rule has passed. Refuses with `VectorPartitionLimitExceeded`
/// when a column would end the commit with more partitions than it may have, and stages
/// nothing for a change set that staged no row.
pub(crate) fn stage(changes: &mut ChangeSet, catalog: &Catalog, budget: &Budget) -> Result<()> {
    let mut held = budget.reserve(0, "transaction")?;
    let moves = moves(changes, catalog, &mut held)?;
    if moves.is_empty() {
        return Ok(());
    }
    let waited = changes.vector_number(VectorNumber::Pending)?;
    let mut pending = waited;
    let mut touched: BTreeMap<(VectorId, Key), Touched> = BTreeMap::new();
    let mut counts: BTreeMap<VectorId, Count> = BTreeMap::new();

    for moved in &moves {
        let Some(tuple) = &moved.leaves else { continue };
        let left = &mut touch(&mut touched, &*changes, moved.vector, tuple, &mut held)?.now;
        let fewer = left.live.checked_sub(1);
        left.live = fewer.ok_or_else(|| changes.damaged(directory::SHORT))?;
        let waiting = VectorRecord::Pending(moved.vector, left.number, &moved.row);
        if directory::kept(&*changes, waiting, |_| ())?.is_some() {
            changes.delete_vector(waiting)?;
            let fewer = pending.checked_sub(1);
            pending = fewer.ok_or_else(|| changes.damaged(directory::SHORT))?;
        } else {
            // The row leaves a sealed slot: every slot of its key sealed so far is stale.
            let sealed = left.sealed.to_le_bytes().to_vec();
            changes.put_vector(
                VectorRecord::Left(moved.vector, left.number, &moved.row),
                sealed,
            )?;
        }
    }
    // A partition whose last vector left goes, with every record kept for it.
    for ((vector, tuple), Touched { now: emptied, .. }) in &mut touched {
        if emptied.live == 0 {
            empty(changes, catalog, *vector, emptied, &mut held)?;
            let leaving =
                |moved: &&Move| moved.vector == *vector && moved.leaves.as_ref() == Some(tuple);
            for moved in moves.iter().filter(leaving) {
                changes.delete_vector(VectorRecord::Left(*vector, emptied.number, &moved.row))?;
            }
            counts.entry(*vector).or_default().change -= 1;
            *emptied = Partition::default();
        }
    }
    for moved in &moves {
        let Some(tuple) = &moved.enters else { continue };
        let entered = &mut touch(&mut touched, &*changes, moved.vector, tuple, &mut held)?.now;
        if entered.live == 0 {
            // A new partition takes the column's next number.
            let count = counts.entry(moved.vector).or_default();
            let next = VectorNumber::NextPartition(moved.vector);
            // A number is never given twice, and a column has 4,294,967,296 of them.
            let number = changes.vector_number(next)? + count.made;
            entered.number = u32::try_from(number).map_err(|_| {
                not_supported("more than 4,294,967,296 partitions over the life of a VECTOR column")
            })?;
            (count.change, count.made) = (count.change + 1, count.made + 1);
        }
        entered.live += 1;
        let waiting = VectorRecord::Pending(moved.vector, entered.number, &moved.row);
        changes.put_vector(waiting, Vec::new())?;
        pending += 1;
    }

    for ((vector, tuple), partition) in &touched {
        let record = VectorRecord::Partition(*vector, tuple);
        if partition.now.live == 0 {
            changes.delete_vector(record)?;
        } else if partition.was != Some(partition.now) {
            changes.put_vector(record, partition.now.encode())?;
        }
    }
    if pending != waited {
        changes.put_vector_number(VectorNumber::Pending, pending)?;
    }
    for (vector, count) in &counts {
        let partitions = VectorNumber::Partitions(*vector);
        let now = changes
            .vector_number(partitions)?
            .saturating_add_signed(count.change);
        changes.put_vector_number(partitions, now)?;
        if count.made == 0 {
            continue;
        }
        let next = VectorNumber::NextPartition(*vector);
        changes.put_vector_number(next, changes.vector_number(next)? + count.made)?;
        // A statement over the limit was refused when it ran, so this is the refusal only a
        // commit can see: another transaction made a partition while this one was open.
        if let Some((table, declared)) = catalog.vector(*vector)
            && let Some(limit) = declared.limit()
            && now > u64::from(limit)
        {
            return Err(Error::VectorPartitionLimitExceeded {
                index: table.vector_ref(declared),
                max_partitions: limit,
            });
        }
    }
    Ok(())
}

/// Stages the removal of every record kept for a partition but its entry and the `Left`
/// records this commit staged: its tail blocks with their sketches, its mean, the records of
/// its grouping and of its build, and its `Left` records, whose keys are held until then.
fn empty(
    changes: &mut ChangeSet,
    catalog: &Catalog,
    vector: VectorId,
    emptied: &Partition,
    held: &mut Reservation,
) -> Result<()> {
    let number = emptied.number;
    for block in emptied.first..emptied.blocks {
        changes.delete_vector(VectorRecord::Block(vector, number, block))?;
        changes.delete_vector(VectorRecord::Sketch(vector, number, block))?;
    }
    changes.delete_vector(VectorRecord::Mean(vector, number))?;
    let (table, column) = catalog.vector(vector).expect("a staged row's column");
    let dimension = table.dimension(column);
    if let Some(grouped) = emptied.grouping {
        for record in pack::records_of(vector, grouped, dimension) {
            changes.delete_vector(record)?;
        }
    }
    if emptied.building.is_some()
        && let Some(build) = Build::read(&*changes, vector, number)?
    {
        for record in build.written(vector, number, dimension) {
            changes.delete_vector(record)?;
        }
    }
    let first = Key::from_values(&[], &[])?;
    let mut left: Vec<Key> = Vec::new();
    changes.vectors(
        VectorRecord::Left(vector, number, &first),
        &mut |record, _| match record {
            VectorRecord::Left(column, partition, key)
                if (column, partition) == (vector, number) =>
            {
                room(&mut left, usize::MAX, held)?;
                held.grow(key.as_bytes().len() as u64)?;
                left.push(key.clone());
                Ok(true)
            }
            _ => Ok(false),
        },
    )?;
    for key in &left {
        changes.delete_vector(VectorRecord::Left(vector, number, key))?;
    }
    Ok(())
}

/// The staged rows whose vector leaves a key tuple or arrives under one, in the order of
/// table and row key. A row with a vector before and after moves when its key tuple or its
/// vector changed: the vector it had is loaded to tell, and given back.
fn moves(changes: &ChangeSet, catalog: &Catalog, held: &mut Reservation) -> Result<Vec<Move>> {
    let base = changes.base();
    let mut moves = Vec::new();
    for table in catalog.tables() {
        if table.vectors.is_empty() {
            continue;
        }
        let staged = changes.rows_changed();
        for (_, key, staged, _) in staged.filter(|staged| staged.0 == table.id) {
            let before = base.row(table.id, key)?;
            for column in &table.vectors {
                let (leaves, enters) = tuples(table, column, before.as_ref(), staged, base, held)?;
                if leaves == enters {
                    // The row has a vector under one tuple before and after, or none. A vector
                    // the row left where it is has not changed.
                    let at = position(table, &column.column);
                    let kept =
                        staged.is_some_and(|(_, left)| left.iter().any(|(slot, _)| *slot == at));
                    let (Some(row), Some((values, _)), true, false) =
                        (&before, staged, leaves.is_some(), kept)
                    else {
                        continue;
                    };
                    let taken = held.bytes();
                    let same = match &*table.cell(row, at) {
                        Cell::Here(vector) => *vector == values[at],
                        Cell::Beside(beside) => base.load(beside, held)? == values[at],
                    };
                    held.shrink(held.bytes() - taken);
                    if same {
                        continue;
                    }
                }
                let keys = [Some(key), leaves.as_ref(), enters.as_ref()];
                let keys = keys.into_iter().flatten();
                let bytes: usize = keys.map(|key| key.as_bytes().len()).sum();
                held.grow((size_of::<Move>() + bytes) as u64)?;
                moves.push(Move {
                    vector: column.id,
                    row: key.clone(),
                    leaves,
                    enters,
                });
            }
        }
    }
    Ok(moves)
}

/// The partition under a key tuple as the commit has it so far: read from the committed moment
/// the first time, and kept for the length of the call.
fn touch<'a>(
    touched: &'a mut BTreeMap<(VectorId, Key), Touched>,
    read: &dyn Read,
    vector: VectorId,
    tuple: &Key,
    held: &mut Reservation,
) -> Result<&'a mut Touched> {
    // The tuple is copied to look it up: its bytes are taken first, and kept with a new entry.
    let bytes = tuple.as_bytes().len() as u64 + 64;
    held.grow(bytes)?;
    Ok(match touched.entry((vector, tuple.clone())) {
        Entry::Occupied(known) => {
            let known = known.into_mut();
            held.shrink(bytes);
            known
        }
        Entry::Vacant(first) => {
            let was = directory::partition(read, vector, tuple)?;
            let now = was.unwrap_or_default();
            first.insert(Touched { was, now })
        }
    })
}
