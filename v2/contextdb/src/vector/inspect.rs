//! What inspection reads of a VECTOR column: its partitions as their records describe them.
//! It reads the vector part's small records and no block, no centroid and no row.

use std::collections::HashMap;
use std::sync::Arc;

use super::build::{self, Build, Phase};
use super::directory::Partition;
use super::ivf::Ivf;
use crate::memory::Reservation as Held;
use crate::schema::{Table, VectorColumn};
use crate::store::{Cell, Key, READING, Read, VectorRecord};
use crate::types::{Error, Result, Value};

/// What a partition is doing about its IVF, in the order a column's state is taken: the
/// last of these that any of its partitions is in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum State {
    /// Nothing is due.
    Idle,
    /// An IVF is due and its build has not begun.
    Pending,
    /// A build is under way.
    Building,
    /// A build is under way and its last step was refused for memory.
    Stalled,
}

/// One partition as its records describe it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Inspected {
    /// The partition's key tuple.
    pub tuple: Key,
    /// Rows with a vector in the partition.
    pub live: u64,
    /// Those of them not yet in a block.
    pub pending: u64,
    /// Slots in its blocks that rows have left behind, and whether they alone are enough for
    /// an IVF to be due.
    pub stale: u64,
    pub left_behind: bool,
    /// Slots in tail blocks: sealed, and in no IVF yet.
    pub tail: u64,
    /// The bytes of its slots; and of its IVF's centroids and record, its binary codes and its
    /// mean.
    pub slot_bytes: u64,
    pub index_bytes: u64,
    /// The groups of the IVF in force and the most of them a search scanned for the
    /// questions its build asked; zero and zero when there is none.
    pub groups: u32,
    pub probes: u32,
    /// Whether a search of the nearest groups may be given this partition.
    pub usable: bool,
    pub state: State,
    /// Of a build under way: the slots it reads and those it has packed; of an IVF that
    /// is due and not begun: the partition's slots and zero.
    pub total: u64,
    pub done: u64,
    /// The bytes a refused step needed; zero unless `state` is `Stalled`.
    pub needing: u64,
    /// True when the IVF in force cannot be read, or the writer's process met damage in it.
    pub damage: bool,
}

/// The partitions of a column in the order of their key tuples, leaving out the first `skip`
/// and stopping after `most`. A column with no PARTITION_KEY has its one partition before its
/// first vector.
pub(crate) fn partitions(
    read: &dyn Read,
    column: &VectorColumn,
    dimension: usize,
    skip: u64,
    most: Option<u64>,
    held: &mut Held,
) -> Result<Vec<Inspected>> {
    let mut entries = Vec::new();
    build::entries(read, column, None, &mut |tuple, partition| {
        held.room(&mut entries, usize::MAX)?;
        held.grow(tuple.as_bytes().len() as u64)?;
        entries.push((tuple.clone(), partition));
        Ok(true)
    })?;
    if entries.is_empty() && column.partition_key.is_empty() {
        entries.push((Key::from_values(&[], &[])?, Partition::default()));
    }
    // Whether an IVF is due depends on the rows with a vector in the whole column.
    let vectors = entries.iter().map(|(_, partition)| partition.live).sum();
    let count = |number: u64| usize::try_from(number).unwrap_or(usize::MAX);
    let asked = entries.into_iter().skip(count(skip));
    let asked = asked.take(most.map_or(usize::MAX, count));
    held.grow((asked.len() * size_of::<Inspected>()) as u64)?;
    asked
        .map(|(tuple, partition)| {
            let before = held.bytes();
            let found = inspected(
                read,
                column,
                dimension as u64,
                vectors,
                tuple,
                &partition,
                held,
            );
            held.shrink(held.bytes() - before);
            found
        })
        .collect()
}

/// One partition from its entry, its pending count, its IVF's record and its build's.
fn inspected(
    read: &dyn Read,
    column: &VectorColumn,
    n: u64,
    vectors: u64,
    tuple: Key,
    p: &Partition,
    held: &mut Held,
) -> Result<Inspected> {
    let pending = u64::from(p.unindexed);
    let grouped = p.ivf.map_or(0, |ivf| ivf.slots);
    let slots = grouped + p.slots;
    let mut index_bytes = 0;
    let (mut groups, mut probes, mut damage) = (0, 0, false);
    if let Some(ivf) = p.ivf {
        (groups, damage) = (ivf.groups, read.committed().marked(column.id, ivf.number));
        let (g, tail) = (u64::from(groups), p.blocks - p.first);
        index_bytes += 12 + 8 * (g + 1);
        if groups > 1 {
            let codes = slots * (n.div_ceil(8) + 2) + 4 * u64::from(ivf.blocks + tail);
            index_bytes += 4 * n * (g + 1) + codes;
        }
        match Ivf::read(read, column.id, ivf, held) {
            Ok(ivf) => probes = ivf.probes,
            Err(Error::StoreCorrupted { .. }) => damage = true,
            Err(other) => return Err(other),
        }
    }
    let build = Build::read(read, column.id, p.number, n as usize, held)?;
    let (state, total, done, needing) = match build {
        Some(build) => {
            let state = if build.needing > 0 {
                State::Stalled
            } else {
                State::Building
            };
            let done = build.slots.saturating_sub(build.unpacked());
            (state, build.slots, done, build.needing)
        }
        None if damage || build::due(p, pending, column, vectors) => (State::Pending, slots, 0, 0),
        None => (State::Idle, 0, 0, 0),
    };
    Ok(Inspected {
        tuple,
        live: p.live,
        pending,
        stale: (slots + pending).saturating_sub(p.live),
        left_behind: build::left_behind(p, pending, column, vectors),
        tail: p.slots,
        slot_bytes: slots * (8 + n),
        index_bytes,
        groups,
        probes,
        usable: !damage && build::usable(read, p, pending, column),
        state,
        total,
        done,
        needing,
        damage,
    })
}

/// Numbers owned by a column, resolved once before inspecting its records.
pub(crate) struct Owners(HashMap<u32, Arc<(Partition, Option<Build>)>>);

fn owned(
    id: crate::store::VectorId,
    p: Partition,
    b: Option<&Build>,
    n: usize,
) -> impl Iterator<Item = VectorRecord<'static>> + use<> {
    let from = b.and_then(|b| b.from).filter(|g| Some(*g) != p.ivf);
    let written = b.map(|b| b.written(id, p.number, n)).into_iter().flatten();
    p.ivf
        .into_iter()
        .chain(from)
        .flat_map(move |g| super::pack::records_of(id, g, n))
        .chain(written)
        .chain((p.first..p.blocks).map(move |at| VectorRecord::Block(id, p.number, at)))
}

pub(crate) fn owners(
    read: &dyn Read,
    column: &VectorColumn,
    n: usize,
    held: &mut Held,
) -> Result<Owners> {
    use super::{directory, ivf};
    let id = column.id;
    let mut owners = Owners(Default::default());
    for (_, p) in directory::selected(read, column, &[], held)? {
        READING.set(p.ivf.map(|grouped| (id, grouped.number)));
        let before = held.bytes();
        let b = Build::read(read, id, p.number, n, held)?;
        let building = b.as_ref().map(|b| b.number);
        if p.building.map(|(n, _)| n) != building {
            return Err(read.damaged("a vector record disagrees with its owner"));
        }
        let from = b.as_ref().and_then(|b| b.from);
        for grouped in p.ivf.into_iter().chain(from.filter(|g| Some(*g) != p.ivf)) {
            Ivf::read(read, id, grouped, held)?;
            if grouped.groups > 1 {
                ivf::check_centroids(read, (id, grouped.number, grouped.groups, n))?;
            }
        }
        if let Some(b) = &b
            && b.groups > 1
            && b.phase != (Phase::Train { round: 0 })
        {
            ivf::check_centroids(read, (id, b.number, b.groups, n))?;
        }
        for record in owned(id, p, b.as_ref(), n) {
            match record {
                VectorRecord::Block(..) => {
                    super::pack::companions(read, record, n, (&p, b.as_ref()), None, &mut |_| {
                        Ok(())
                    })?
                }
                VectorRecord::Mean(_, number) => {
                    ivf::mean(read, id, number, n, held)?;
                }
                _ => (),
            }
        }
        held.shrink(held.bytes() - before);
        let lists =
            |b: &Build| 4 * b.counts.len() + 8 * (b.starts.len() + b.tens.len() + b.sum.len());
        let lists = b.as_ref().map_or(0, lists);
        held.grow((lists + size_of::<(Partition, Option<Build>)>() + 16) as u64)?;
        let old = b.as_ref().and_then(|b| b.from.map(|g| g.number));
        let numbers = [Some(p.number), p.ivf.map(|g| g.number), old, building];
        let owner = Arc::new((p, b));
        for number in numbers.into_iter().flatten() {
            held.grow(64)?;
            owners.0.insert(number, owner.clone());
        }
    }
    Ok(owners)
}

/// Checks each payload with its column's already resolved owner.
pub(crate) fn verify_record(
    read: &dyn Read,
    table: &Table,
    column: &VectorColumn,
    owners: &Owners,
    record: VectorRecord<'_>,
    bytes: &[u8],
    cover: &mut crate::memory::Passing,
) -> Result<()> {
    use super::{directory, directory::kept, pack};
    use VectorRecord::*;
    let damage = || read.damaged("a vector record disagrees with its owner");
    let owner = |n| {
        let found = owners.0.get(&n).map(Arc::as_ref).ok_or_else(damage);
        found.inspect(|(p, _)| READING.set(p.ivf.map(|g| (column.id, g.number))))
    };
    let (id, n) = (column.id, table.dimension(column));
    let sound = match record {
        Partition(_, tuple) => return self::Partition::read(read, column, tuple, bytes).map(drop),
        Ivf(_, number) | Centroids(_, number, _) | Build(_, number) | Mean(_, number) => {
            let (p, b) = owner(number)?;
            owned(id, *p, b.as_ref(), n).any(|r| r == record)
        }
        Pending(_, number, key) => {
            owner(number)?;
            let at = table.column(&column.column)?;
            let row = read.row(table.id, key, cover)?;
            bytes.is_empty()
                && row.is_some_and(|r| !matches!(&*table.cell(&r, at), Cell::Here(Value::Null)))
        }
        Block(_, number, block) | Binary(_, number, block) => {
            let (p, b) = owner(number)?;
            let mut rows = |data: &super::block::BlockReader<'_>| {
                let len = if matches!(record, Block(..)) {
                    data.len()
                } else {
                    0
                };
                for at in 0..len {
                    let key = data.key(at);
                    // Every slot a row holds is noted in the row's `Slot` record.
                    let here = [number, block, at as u32];
                    let noted = |v: &[u8]| directory::slots(v).any(|slot| slot == here);
                    if !key.as_bytes().is_empty()
                        && kept(read, Slot(id, p.number, &key), noted)? != Some(true)
                    {
                        return Err(damage());
                    }
                    let at_left = |v: &[u8]| build::left_at(read, p, v);
                    if !key.as_bytes().is_empty()
                        && !read.contains(table.id, &key)?
                        && kept(read, Left(id, p.number, &key), at_left)?
                            .transpose()?
                            .is_none()
                    {
                        return Err(damage());
                    }
                }
                Ok(())
            };
            return pack::companions(read, record, n, (p, b.as_ref()), Some(bytes), &mut rows);
        }
        Assigned(_, number, place) => {
            let (p, build) = owner(number)?;
            let b = build.as_ref().ok_or_else(damage)?;
            if b.number != number || place >= b.blocks() {
                return Err(damage());
            }
            pack::with_block(read, b.block(id, p.number, place), n, &mut |block| {
                Ok(bytes.len() == 2 * block.len())
            })?
        }
        Left(_, number, _) => return build::left_at(read, &owner(number)?.0, bytes).map(drop),
        Slot(_, number, _) => {
            owner(number)?;
            !bytes.is_empty() && bytes.len().is_multiple_of(12)
        }
    };
    sound.then_some(()).ok_or_else(damage)
}
