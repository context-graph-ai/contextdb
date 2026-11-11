//! What inspection reads of a VECTOR column: its partitions as their records describe them.
//! It reads the vector part's small records and no block, no centre and no row.

use super::build::{self, Build};
use super::directory::Partition;
use super::grouping::Grouping;
use super::seal::room;
use crate::memory::Reservation;
use crate::schema::VectorColumn;
use crate::store::{Key, Read, VectorRecord};
use crate::types::{Error, Result};

/// What a partition is doing about its grouping, in the order a column's state is taken: the
/// last of these that any of its partitions is in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum State {
    /// Nothing is due.
    Idle,
    /// A grouping is due and its build has not begun.
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
    /// a grouping to be due.
    pub stale: u64,
    pub left_behind: bool,
    /// Slots in tail blocks: sealed, and in no grouping yet.
    pub tail: u64,
    /// The bytes of its slots; and of its grouping's centres and record, its sketches and its
    /// mean.
    pub slot_bytes: u64,
    pub index_bytes: u64,
    /// The groups of the grouping in force and the most of them a search scanned for the
    /// questions its build asked; zero and zero when there is none.
    pub groups: u32,
    pub probes: u32,
    /// Whether a search of the nearest groups may be given this partition.
    pub usable: bool,
    pub state: State,
    /// Of a build under way: the slots it reads and those it has packed; of a grouping that
    /// is due and not begun: the partition's slots and zero.
    pub total: u64,
    pub done: u64,
    /// The bytes a refused step needed; zero unless `state` is `Stalled`.
    pub needing: u64,
    /// The reason the grouping in force cannot be read, when it cannot.
    pub damage: Option<String>,
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
    held: &mut Reservation,
) -> Result<Vec<Inspected>> {
    let first = Key::from_values(&[], &[])?;
    let mut entries = Vec::new();
    let from = VectorRecord::Partition(column.id, &first);
    read.vectors(from, &mut |record, entry| match record {
        VectorRecord::Partition(vector, tuple) if vector == column.id => {
            room(&mut entries, usize::MAX, held)?;
            held.grow(tuple.as_bytes().len() as u64)?;
            entries.push((tuple.clone(), Partition::decode(read, entry)?));
            Ok(true)
        }
        _ => Ok(false),
    })?;
    if entries.is_empty() && column.partition_key.is_empty() {
        entries.push((first, Partition::default()));
    }
    // Whether a grouping is due depends on the rows with a vector in the whole column.
    let vectors = entries.iter().map(|(_, partition)| partition.live).sum();
    let count = |number: u64| usize::try_from(number).unwrap_or(usize::MAX);
    let asked = entries.into_iter().skip(count(skip));
    let asked = asked.take(most.map_or(usize::MAX, count));
    held.grow((asked.len() * size_of::<Inspected>()) as u64)?;
    asked
        .map(|(tuple, partition)| {
            inspected(read, column, dimension as u64, vectors, tuple, &partition)
        })
        .collect()
}

/// One partition from its entry, its pending count, its grouping's record and its build's.
fn inspected(
    read: &dyn Read,
    column: &VectorColumn,
    n: u64,
    vectors: u64,
    tuple: Key,
    partition: &Partition,
) -> Result<Inspected> {
    let pending = build::pending(read, column.id, partition.number)?;
    let grouped = partition.grouping.map_or(0, |grouping| grouping.slots);
    let slots = grouped + partition.slots;
    let mut index_bytes = 4 * n * u64::from(partition.mean);
    let (mut groups, mut probes, mut damage) = (0, 0, None);
    if let Some(grouping) = partition.grouping {
        groups = grouping.groups;
        let (g, tail) = (u64::from(groups), partition.blocks - partition.first);
        index_bytes += 12 + 8 * (g + 1);
        if groups > 1 {
            let sketches = slots * (n.div_ceil(8) + 2) + 4 * u64::from(grouping.blocks + tail);
            index_bytes += 4 * n * g + sketches;
        }
        match Grouping::read(read, column.id, grouping) {
            Ok(grouping) => probes = grouping.probes,
            Err(Error::StoreCorrupted { reason, .. }) => damage = Some(reason),
            Err(other) => return Err(other),
        }
    }
    let (state, total, done, needing) = match Build::read(read, column.id, partition.number)? {
        Some(build) => {
            let state = if build.needing > 0 {
                State::Stalled
            } else {
                State::Building
            };
            let done = build.slots.saturating_sub(build.unpacked());
            (state, build.slots, done, build.needing)
        }
        None if build::due(partition, pending, column, vectors) => (State::Pending, slots, 0, 0),
        None => (State::Idle, 0, 0, 0),
    };
    Ok(Inspected {
        tuple,
        live: partition.live,
        pending,
        stale: (slots + pending).saturating_sub(partition.live),
        left_behind: build::left_behind(partition, pending, column, vectors),
        tail: partition.slots,
        slot_bytes: slots * (8 + n),
        index_bytes,
        groups,
        probes,
        usable: build::usable(partition, pending, column),
        state,
        total,
        done,
        needing,
        damage,
    })
}
