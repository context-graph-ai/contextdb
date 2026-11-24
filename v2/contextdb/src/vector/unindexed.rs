//! What a commit's row changes mean for the vector part, worked out once, inside the commit
//! function, from the complete change set.

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;

use super::build::{self, Build};
use super::directory::{self, Partition};
use super::tally::{position, tuples};
use super::{pack, seal};
use crate::memory::{Budget, Reservation};
use crate::schema::{Catalog, VectorColumn};
use crate::sql::not_supported;
use crate::store::{Cell, ChangeSet, Key, Read, VectorId, VectorNumber, VectorRecord};
use crate::types::{Error, Result};

/// Derived column totals, reconstructed once at open and published only after a commit.
/// This handle state is covered by the open's fixed 1 MiB allowance.
#[derive(Clone, Default)]
pub(crate) struct Totals {
    values: BTreeMap<VectorId, u64>,
}

impl Totals {
    pub(crate) fn live(&self, vector: VectorId) -> u64 {
        self.values.get(&vector).copied().unwrap_or(0)
    }

    /// Takes the totals `changed` holds, and, given the catalog a declaration leaves, lets
    /// go of those of the columns it no longer has.
    pub(crate) fn merge(&mut self, changed: Totals, declared: Option<&Catalog>) {
        self.values.extend(changed.values);
        if let Some(catalog) = declared {
            self.values.retain(|id, _| catalog.vector(*id).is_some());
        }
    }

    pub(crate) fn load(read: &dyn Read, catalog: &Catalog) -> Result<Self> {
        let mut totals = Self::default();
        for column in catalog.tables().iter().flat_map(|table| &table.vectors) {
            let mut live = 0;
            build::entries(read, column, None, &mut |_, p| {
                live += p.live;
                Ok(true)
            })?;
            totals.values.insert(column.id, live);
        }
        Ok(totals)
    }
}

/// The totals of the columns a commit changed, to publish after the storage commit, and
/// whether a build became due.
pub(crate) struct Committed {
    pub totals: Totals,
    pub wake: bool,
}

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
pub(crate) fn track_unindexed(
    changes: &mut ChangeSet,
    catalog: &Catalog,
    budget: &Budget,
    before: &Totals,
) -> Result<Committed> {
    let mut result = Committed {
        totals: Totals::default(),
        wake: false,
    };
    let mut held = budget.reserve(0, "transaction")?;
    let moves = moves(changes, catalog, &mut held)?;
    let move_bytes = held.bytes();
    if moves.is_empty() {
        return Ok(result);
    }
    if changes.streaming() {
        changes.put_part(true)?;
    }
    let waited = changes.vector_number(VectorNumber::Pending)?;
    let mut pending = waited;
    let mut touched: BTreeMap<(VectorId, Key), Touched> = BTreeMap::new();
    let mut counts: BTreeMap<VectorId, Count> = BTreeMap::new();

    for moved in &moves {
        let Some(tuple) = &moved.leaves else { continue };
        let (table, column) = catalog.vector(moved.vector).expect("a staged column");
        let left = &mut touch(&mut touched, &*changes, column, tuple, &mut held)?.now;
        let fewer = left.live.checked_sub(1);
        left.live = fewer.ok_or_else(|| changes.damaged(directory::SHORT))?;
        let unindexed = VectorRecord::Pending(moved.vector, left.number, &moved.row);
        if directory::kept(&*changes, unindexed, |_| ())?.is_some() {
            changes.delete_vector(unindexed)?;
            left.pending_bytes = left
                .pending_bytes
                .checked_sub(seal::bytes(table.dimension(column), &moved.row))
                .ok_or_else(|| changes.damaged(directory::SHORT))?;
            left.unindexed = left
                .unindexed
                .checked_sub(1)
                .ok_or_else(|| changes.damaged(directory::SHORT))?;
            let fewer = pending.checked_sub(1);
            pending = fewer.ok_or_else(|| changes.damaged(directory::SHORT))?;
        } else {
            // The row leaves a sealed slot: every slot of its key sealed so far is stale. A
            // running build keeps the departure in force at its source view.
            let fence = changes.base().commit().0 + 1;
            let record = VectorRecord::Left(moved.vector, left.number, &moved.row);
            let mut through = (0, 0);
            if let Some((_, source)) = left.building {
                let at = |v: &[u8]| build::left_at(&*changes, left, v);
                if let Some([last, before]) = directory::kept(&*changes, record, at)?.transpose()? {
                    through = if last.1 <= source { last } else { before };
                }
            }
            let pairs = [left.sealed, fence, through.0, through.1].map(u64::to_le_bytes);
            changes.put_vector(record, pairs.concat())?;
        }
    }
    for moved in &moves {
        let Some(tuple) = &moved.enters else { continue };
        let (table, column) = catalog.vector(moved.vector).expect("a staged column");
        let entry = touch(&mut touched, &*changes, column, tuple, &mut held)?;
        let new = entry.was.is_none();
        let entered = &mut entry.now;
        if entered.live == 0 && new {
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
        let more = seal::bytes(table.dimension(column), &moved.row);
        // A tail that cannot take the arriving record is full for subsequent sealing.
        if u64::from(entered.tail_room) < more {
            entered.tail_room = 0;
        }
        let room = if entered.blocks > entered.closed && entered.tail_room > 0 {
            u64::from(entered.tail_room)
        } else {
            (super::block::BLOCK_BYTES - 4) as u64
        };
        let seal =
            |changes: &mut ChangeSet, entered: &mut Partition, pending: &mut u64| -> Result<()> {
                changes.put_vector(
                    VectorRecord::Partition(moved.vector, tuple),
                    entered.encode(),
                )?;
                changes.put_vector_number(VectorNumber::Pending, *pending)?;
                let bytes = (super::block::BLOCK_BYTES as u64).max(more + 4);
                let mut sealing = budget.reserve(bytes, "maintenance")?;
                let (done, _) = seal::seal_partition(
                    changes,
                    catalog,
                    &mut sealing,
                    (moved.vector, entered.number),
                    entered.unindexed as usize,
                    *pending,
                )?;
                *pending -= done;
                *entered =
                    directory::partition(&*changes, column, tuple)?.expect("a sealed partition");
                Ok(())
            };
        if entered.unindexed > 0 && entered.pending_bytes + more > room {
            seal(changes, entered, &mut pending)?;
        }
        entered.live += 1;
        entered.pending_bytes += more;
        entered.unindexed = entered
            .unindexed
            .checked_add(1)
            .ok_or_else(|| changes.damaged(directory::SHORT))?;
        let unindexed = VectorRecord::Pending(moved.vector, entered.number, &moved.row);
        changes.put_vector(unindexed, Vec::new())?;
        pending += 1;
        if more + 4 > super::block::BLOCK_BYTES as u64
            || changes
                .setting(5)?
                .is_some_and(|n| u64::from(entered.unindexed) >= n)
        {
            seal(changes, entered, &mut pending)?;
        }
    }

    drop(moves);
    held.shrink(move_bytes);
    if changes.streaming() {
        changes.put_part(false)?;
    }
    // Cleanup follows arrivals, after the row part and its owned moves have left memory.
    for ((vector, _), Touched { now: emptied, .. }) in &mut touched {
        if emptied.live == 0 {
            empty(changes, catalog, *vector, emptied, budget)?;
            counts.entry(*vector).or_default().change -= 1;
            *emptied = Partition::default();
        }
    }
    for ((vector, tuple), partition) in &touched {
        let record = VectorRecord::Partition(*vector, tuple);
        if partition.now.live == 0 {
            changes.delete_vector(record)?;
        } else if partition.was != Some(partition.now) {
            changes.put_vector(record, partition.now.encode())?;
        }
    }
    if pending != changes.vector_number(VectorNumber::Pending)? {
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
    // Derive each column's committed live total solely from the partitions already touched.
    for ((vector, _), partition) in &touched {
        let total = result.totals.values.entry(*vector);
        let total = total.or_insert_with(|| before.live(*vector));
        *total = *total - partition.was.map_or(0, |was| was.live) + partition.now.live;
    }
    for ((vector, _), partition) in &touched {
        let (_, column) = catalog.vector(*vector).expect("a staged vector's column");
        let old_total = before.live(*vector);
        let total = result.totals.values[vector];
        let was = partition.was.unwrap_or_default();
        result.wake |= (old_total < column.index_at()) != (total < column.index_at());
        result.wake |= partition.now.building.is_none()
            && build::due(
                &partition.now,
                u64::from(partition.now.unindexed),
                column,
                total,
            )
            && !build::due(&was, u64::from(was.unindexed), column, old_total);
    }
    Ok(result)
}

/// Removes a partition's tail blocks, codes, mean, IVF, build and departed-row records,
/// including records staged by this commit; its caller removes the partition entry.
fn empty(
    changes: &mut ChangeSet,
    catalog: &Catalog,
    vector: VectorId,
    emptied: &Partition,
    budget: &Budget,
) -> Result<()> {
    let number = emptied.number;
    for block in emptied.first..emptied.blocks {
        changes.delete_vector(VectorRecord::Block(vector, number, block))?;
        changes.delete_vector(VectorRecord::Binary(vector, number, block))?;
    }
    let (table, column) = catalog.vector(vector).expect("a staged row's column");
    let dimension = table.dimension(column);
    if let Some(grouped) = emptied.ivf {
        for record in pack::records_of(vector, grouped, dimension) {
            changes.delete_vector(record)?;
        }
    }
    let mut held = budget.reserve(0, "empty partition")?;
    abandon(changes, vector, emptied, dimension, &mut held)?;
    departed(changes, (vector, number), None, budget)?;
    Ok(())
}

/// The staged rows whose vector leaves a key tuple or arrives under one, in the order of
/// table and row key. A row with a vector before and after moves when its key tuple or its
/// vector changed: the vector it had is loaded to tell, and given back.
fn moves(changes: &ChangeSet, catalog: &Catalog, held: &mut Reservation) -> Result<Vec<Move>> {
    let base = changes.base();
    let mut moves = Vec::new();
    let mut cover = held.passing();
    for (id, _) in changes.changed_tables() {
        let Some(table) = catalog.by_id(id).filter(|t| !t.vectors.is_empty()) else {
            continue;
        };
        let staged = changes.rows_changed();
        for (_, key, staged, _) in staged.filter(|staged| staged.0 == table.id) {
            let before = base.row(table.id, key, &mut cover)?;
            for column in &table.vectors {
                let (leaves, enters) = tuples(table, column, before.as_ref(), staged, base, held)?;
                if leaves == enters {
                    // The row has a vector under one tuple before and after, or none. A vector
                    // the row left where it is has not changed.
                    let at = position(table, &column.column, base)?;
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
                held.grow(bytes as u64)?;
                held.room(&mut moves, usize::MAX)?;
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
    column: &VectorColumn,
    tuple: &Key,
    held: &mut Reservation,
) -> Result<&'a mut Touched> {
    // The tuple is copied to look it up: its bytes are taken first, and kept with a new entry.
    let bytes = tuple.as_bytes().len() as u64
        + crate::memory::node::<((VectorId, Key), Touched)>()
            * if touched.is_empty() { 4 } else { 1 };
    held.grow(bytes)?;
    Ok(match touched.entry((column.id, tuple.clone())) {
        Entry::Occupied(known) => {
            let known = known.into_mut();
            held.shrink(bytes);
            known
        }
        Entry::Vacant(first) => {
            let was = directory::partition(read, column, tuple)?;
            let now = was.unwrap_or_default();
            first.insert(Touched { was, now })
        }
    })
}

fn abandon(
    changes: &mut ChangeSet,
    vector: VectorId,
    partition: &Partition,
    dimension: usize,
    held: &mut Reservation,
) -> Result<()> {
    let number = partition.number;
    if partition.building.is_some()
        && let Some(build) = Build::read(&*changes, vector, number, dimension, held)?
    {
        for record in build.written(vector, number, dimension) {
            changes.delete_vector(record)?;
        }
    }
    Ok(())
}

/// Deletes a partition's `Left` records of noted rows, leapfrogging the two sorted runs
/// through the store's typed lower-bound note seek. True when it deleted one.
fn departed(
    changes: &mut ChangeSet,
    (id, partition): (VectorId, u32),
    table: Option<crate::store::TableId>,
    budget: &Budget,
) -> Result<bool> {
    let _keys = budget.reserve(3 * crate::store::KEY_LIMIT as u64, "departed keys")?;
    let (mut cursor, mut found) = (Key::from_bytes(&[]), false);
    loop {
        let (mut next, from) = (None, VectorRecord::Left(id, partition, &cursor));
        changes.vectors(from, &mut |record, _| {
            next = match record {
                VectorRecord::Left(v, p, key) if (v, p) == (id, partition) => Some(key.clone()),
                _ => None,
            };
            Ok(false)
        })?;
        let Some(key) = next else { return Ok(found) };
        let chosen = match table {
            Some(table) => changes.noted_from(table, &key, budget)?.map(|(key, _)| key),
            None => Some(key.clone()),
        };
        let Some(chosen) = chosen else {
            return Ok(found);
        };
        if chosen == key {
            changes.delete_vector(VectorRecord::Left(id, partition, &key))?;
            found = true;
        }
        cursor = chosen;
    }
}

/// Erasure repairs vector-derived records before the commit, including in Caller mode.
pub(crate) fn erase(changes: &mut ChangeSet, catalog: &Catalog, budget: &Budget) -> Result<()> {
    use super::ivf;
    let changed = changes
        .changed_tables()
        .filter_map(|(id, _)| catalog.by_id(id));
    let tables: Vec<_> = changed.filter(|t| !t.vectors.is_empty()).collect();
    for table in tables {
        if changes
            .noted_from(table.id, &Key::from_bytes(&[]), budget)?
            .is_none()
        {
            continue;
        }
        for column in &table.vectors {
            let (id, dim) = (column.id, table.dimension(column));
            let mut after = None;
            loop {
                let mut held = budget.reserve(0, "purge vectors")?;
                let mut partitions = Vec::new();
                build::entries(&*changes, column, after.as_ref(), &mut |tuple, p| {
                    held.room(&mut partitions, 128)?;
                    held.grow(tuple.as_bytes().len() as u64)?;
                    partitions.push((tuple.clone(), p));
                    Ok(partitions.len() < 128)
                })?;
                if partitions.is_empty() {
                    break;
                }
                after = partitions.last().map(|(key, _)| key.clone());
                for (tuple, mut p) in partitions {
                    if !departed(changes, (id, p.number), Some(table.id), budget)? {
                        continue;
                    }
                    let mut room = budget.reserve(0, "purge partition")?;
                    abandon(changes, id, &p, dim, &mut room)?;
                    p.building = None;
                    changes.put_vector(VectorRecord::Partition(id, &tuple), p.encode())?;
                    if let Some(g) = p.ivf {
                        repair_grouped(changes, (id, g, dim), table.id, budget, &mut room)?;
                    }
                    let mean = p
                        .ivf
                        .filter(|g| g.groups > 1)
                        .map(|g| ivf::mean(&*changes, id, g.number, dim, &mut room))
                        .transpose()?;
                    for block in p.first..p.blocks {
                        let (writer, erased) = erased_block(
                            &*changes,
                            (id, p.number, block, dim),
                            table.id,
                            budget,
                            &mut room,
                        )?;
                        if erased || mean.is_some() {
                            let reference = mean.as_deref().map(|mean| move |_| mean);
                            seal::put_block(
                                (id, p.number, block),
                                writer,
                                (&mut room, Reservation::shrink),
                                (dim, reference),
                                |r, v| changes.put_vector(r, v),
                            )?;
                        }
                        room.shrink(3 * super::block::BLOCK_BYTES as u64);
                    }
                }
            }
        }
    }
    Ok(())
}

fn repair_grouped(
    changes: &mut ChangeSet,
    (id, g, dim): (VectorId, super::directory::Grouped, usize),
    table: crate::store::TableId,
    budget: &Budget,
    room: &mut Reservation,
) -> Result<()> {
    use super::ivf::{self, Ivf, Place};
    use super::train;
    room.grow((g.groups as usize + 8 * dim) as u64)?;
    let mut ivf = Ivf::read(&*changes, id, g, room)?;
    let starts = &mut ivf.starts;
    let (many, groups) = (g.groups > 1, g.groups as usize);
    let per = ivf::centroids_per_record(dim);
    let index = (id, g.number, g.groups, dim);
    let (mut sum, mut hand, mut cached) = (vec![0.0; dim], Vec::new(), (u32::MAX, Vec::new()));
    let mut total = train::Mean::new(dim, room)?;
    let mut changed = vec![0u8; groups]; // 0: unchanged, 1: changed, 2: empty.
    for group in 0..groups {
        let mut live = false;
        sum.fill(0.0);
        let (from, end) = (starts[group], starts[group + 1]);
        for block in from.block..end.block + u32::from(end.slot > 0) {
            if cached.0 != block {
                room.shrink(std::mem::take(&mut cached.1).len() as u64);
                let stored = VectorRecord::Block(id, g.number, block);
                let bytes = directory::kept(&*changes, stored, |v| {
                    pack::block(&*changes, v, dim)?;
                    room.grow(v.len() as u64)?;
                    Ok::<_, Error>(v.to_vec())
                })?;
                let bytes = bytes.ok_or_else(|| changes.damaged(super::block::BROKEN))??;
                cached = (block, bytes);
            }
            let owned = super::block::BlockReader::new(&cached.1, dim);
            let (lo, len) = (from.max(Place { block, slot: 0 }).slot, owned.len() as u32);
            let hi = end.min(Place { block, slot: len }).slot;
            if (from.block == block && from.slot > len) || (end.block == block && end.slot > len) {
                return Err(changes.damaged("a group's bounds exceed its block"));
            }
            for slot in lo as usize..hi as usize {
                let key = owned.key(slot);
                let erased = changes.noted(table, &key, budget)?;
                changed[group] |= u8::from(erased);
                if !erased && !key.as_bytes().is_empty() {
                    live = true;
                    train::add(&mut sum, owned.slot(slot));
                    total.take(owned.slot(slot));
                }
            }
        }
        if !live {
            changed[group] = 2;
        }
        let record = group / per;
        if changed[group] > 0 && many {
            if hand.is_empty() {
                let range = (record * per) as u32..(record * per + per).min(groups) as u32;
                hand = ivf::centroid_range(&*changes, index, range, room)?;
            }
            let centre = &mut hand[(group % per) * dim..][..dim];
            centre.fill(0.0);
            train::normalize(&sum, centre);
        }
        if !hand.is_empty() && ((group + 1).is_multiple_of(per) || group + 1 == groups) {
            room.grow(4 * hand.len() as u64)?;
            ivf::put_centroids_at(changes, (id, g.number, record as u32), &hand)?;
            room.shrink(8 * std::mem::take(&mut hand).len() as u64);
        }
    }
    room.shrink(std::mem::take(&mut cached.1).len() as u64);
    drop(sum);
    room.shrink(8 * dim as u64);
    if many && changed.iter().any(|flag| *flag > 0) {
        room.grow(4 * dim as u64)?;
        let mean = total.finish();
        room.shrink(8 * dim as u64);
        room.grow(4 * dim as u64)?;
        ivf::put_mean(changes, id, g.number, &mean)?;
        drop(mean);
        room.shrink(8 * dim as u64);
    } else {
        drop(total);
        room.shrink(8 * dim as u64);
    }
    for block in 0..g.blocks {
        let first = starts.partition_point(|s| *s <= Place { block, slot: 0 }) - 1;
        let end = starts.partition_point(|s| s.block <= block).min(groups);
        if !changed[first..end].iter().any(|flag| *flag > 0) {
            continue;
        }
        let (writer, _) = erased_block(&*changes, (id, g.number, block, dim), table, budget, room)?;
        let bytes = writer.bytes() as u64;
        room.grow(bytes)?;
        let value = writer.finish();
        room.shrink(bytes);
        changes.put_vector(VectorRecord::Block(id, g.number, block), value)?;
        if many {
            changes.delete_vector(VectorRecord::Binary(id, g.number, block))?;
        }
        room.shrink(3 * super::block::BLOCK_BYTES as u64);
    }
    for group in (1..groups).rev() {
        if changed[group] == 2 {
            starts[group] = starts[group + 1];
        }
    }
    let first = changed.iter().position(|flag| *flag != 2).unwrap_or(groups);
    starts[..=first].fill(Place::default());
    for block in 0..g.blocks * u32::from(many) {
        use super::binary::{code_len, write_code};
        let stored = VectorRecord::Binary(id, g.number, block);
        if directory::kept(&*changes, stored, |_| ())?.is_some() {
            continue;
        }
        let codes = pack::with_block(
            &*changes,
            VectorRecord::Block(id, g.number, block),
            dim,
            &mut |slots| {
                room.grow((4 + slots.len() * code_len(dim)) as u64)?;
                let mut codes = vec![0; 4 + slots.len() * code_len(dim)];
                codes[..4].copy_from_slice(&(slots.len() as u32).to_le_bytes());
                let (mut record, mut centres) = (usize::MAX, Vec::new());
                for (slot, code) in codes[4..].chunks_exact_mut(code_len(dim)).enumerate() {
                    let place = Place {
                        block,
                        slot: slot as u32,
                    };
                    let group = (starts.partition_point(|s| *s <= place) - 1).min(groups - 1);
                    if record != group / per {
                        let bytes = 4 * centres.len() as u64;
                        drop(std::mem::take(&mut centres));
                        room.shrink(bytes);
                        record = group / per;
                        let from = record * per;
                        centres = ivf::centroid_range(
                            &*changes,
                            index,
                            from as u32..(from + per).min(groups) as u32,
                            room,
                        )?;
                    }
                    write_code(
                        slots.slot(slot),
                        &centres[(group % per) * dim..][..dim],
                        code,
                    );
                }
                let held = 4 * centres.len() as u64;
                drop(centres);
                room.shrink(held);
                Ok(codes)
            },
        )?;
        room.shrink(codes.len() as u64);
        changes.put_vector(stored, codes)?;
    }
    let bytes = (12 + 8 * starts.len()) as u64;
    room.grow(bytes)?;
    let value = ivf.encode();
    room.shrink(bytes);
    changes.put_vector(VectorRecord::Ivf(id, g.number), value)?;
    Ok(())
}

fn erased_block(
    read: &ChangeSet,
    (id, under, at, dim): (VectorId, u32, u32, usize),
    table: crate::store::TableId,
    budget: &Budget,
    held: &mut Reservation,
) -> Result<(super::block::BlockWriter, bool)> {
    pack::with_block(
        read,
        VectorRecord::Block(id, under, at),
        dim,
        &mut |block| {
            held.grow(3 * super::block::BLOCK_BYTES as u64)?;
            let (mut writer, _) = super::block::BlockWriter::new(dim, None, &[]);
            let blank = vec![0; super::slot::slot_len(dim)];
            let empty = Key::from_bytes(&[]);
            let mut changed = false;
            for slot in 0..block.len() {
                let key = block.key(slot);
                let erased = !key.as_bytes().is_empty() && read.noted(table, &key, budget)?;
                changed |= erased;
                writer.push_slot(
                    if erased { &empty } else { &key },
                    if erased { &blank } else { block.slot(slot) },
                );
            }

            Ok((writer, changed))
        },
    )
}
