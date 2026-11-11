//! The partitions of a VECTOR column: one record for each key tuple under which a row has a
//! vector.

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;

use crate::memory::Reservation;
use crate::store::{Key, Read, VectorId, VectorRecord};
use crate::types::Result;

/// What a column keeps about one partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(super) struct Partition {
    /// The partition's number within its column. Pending rows and blocks are kept under it.
    pub number: u32,
    /// How many rows have a vector in the partition, pending or in a block.
    pub live: u64,
    /// The number after the partition's last tail block. The tail blocks are the blocks
    /// sealing adds to: they are kept under `number` and numbered from `first`.
    pub blocks: u32,
    /// The number of the partition's first tail block. The tail blocks before it went into the
    /// grouping in force and are gone.
    pub first: u32,
    /// Tail blocks numbered below this are never added to again: a grouping being built reads
    /// them. Every tail block from here on but the last is full.
    pub closed: u32,
    /// How many slots the tail blocks hold, the slots rows have left behind among them.
    pub slots: u64,
    /// How many slots have ever been sealed into the partition's tail. It only rises: the tail
    /// slots are the last `slots` of that count, in the order they were sealed.
    pub sealed: u64,
    /// The grouping in force, `None` until one has been built.
    pub grouping: Option<Grouped>,
    /// The number a grouping being built is kept under, `None` when none is.
    pub building: Option<u32>,
    /// Whether the partition has a mean: its first build keeps one from its first round on,
    /// and from then every tail block at or after `closed` has its sketches.
    pub mean: bool,
}

/// Where a grouping's records are and how many it has: all a search of every slot, and a
/// commit that removes the grouping, need of it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Grouped {
    /// The number its blocks, its centres and its record are kept under.
    pub number: u32,
    /// How many blocks it has, numbered from zero; every one but the last is full.
    pub blocks: u32,
    /// How many groups it has.
    pub groups: u32,
    /// How many slots its blocks hold, the slots rows have left behind since among them.
    pub slots: u64,
    /// The partition's `sealed` when the build that made it began. A row that left its slot
    /// since, a `Left` record of that value or a greater, leaves a slot of this grouping stale.
    pub upto: u64,
}

impl Partition {
    /// The record of the partition, 73 bytes, every number low byte first: `number`, `live`,
    /// `blocks`, `first`, `closed`, `slots`, `sealed`, one byte of flags (1: it has a grouping;
    /// 2: it is building one; 4: it has a mean), the five numbers of the grouping in force and
    /// the number being built. A field its flag does not set is zero.
    pub(super) fn encode(&self) -> Vec<u8> {
        let grouping = self.grouping.unwrap_or(Grouped {
            number: 0,
            blocks: 0,
            groups: 0,
            slots: 0,
            upto: 0,
        });
        let flags = u8::from(self.grouping.is_some())
            | (u8::from(self.building.is_some()) << 1)
            | (u8::from(self.mean) << 2);
        let mut record = Vec::with_capacity(ENTRY);
        record.extend(self.number.to_le_bytes());
        record.extend(self.live.to_le_bytes());
        record.extend(self.blocks.to_le_bytes());
        record.extend(self.first.to_le_bytes());
        record.extend(self.closed.to_le_bytes());
        record.extend(self.slots.to_le_bytes());
        record.extend(self.sealed.to_le_bytes());
        record.push(flags);
        record.extend(grouping.number.to_le_bytes());
        record.extend(grouping.blocks.to_le_bytes());
        record.extend(grouping.groups.to_le_bytes());
        record.extend(grouping.slots.to_le_bytes());
        record.extend(grouping.upto.to_le_bytes());
        record.extend(self.building.unwrap_or(0).to_le_bytes());
        record
    }

    /// The partition a stored record was written from. A record of another length, with a
    /// flag above the three, with more slots in its tail than were ever sealed, with its first
    /// tail block after its closed one or its closed one after its last, with a grouping of no
    /// group, or with a grouping of more than one group and no mean, is damage.
    pub(super) fn decode(read: &dyn Read, record: &[u8]) -> Result<Partition> {
        if record.len() != ENTRY {
            return Err(read.damaged("a partition record of the vector part is not 73 bytes"));
        }
        let short = |at: usize| u32::from_le_bytes(std::array::from_fn(|byte| record[at + byte]));
        let long = |at: usize| u64::from_le_bytes(std::array::from_fn(|byte| record[at + byte]));
        let flags = record[40];
        let partition = Partition {
            number: short(0),
            live: long(4),
            blocks: short(12),
            first: short(16),
            closed: short(20),
            slots: long(24),
            sealed: long(32),
            grouping: (flags & 1 != 0).then(|| Grouped {
                number: short(41),
                blocks: short(45),
                groups: short(49),
                slots: long(53),
                upto: long(61),
            }),
            building: (flags & 2 != 0).then(|| short(69)),
            mean: flags & 4 != 0,
        };
        let groups = partition.grouping.map_or(1, |grouping| grouping.groups);
        if flags > 7
            || partition.sealed < partition.slots
            || partition.first > partition.closed
            || partition.closed > partition.blocks
            || groups == 0
            || (groups > 1 && !partition.mean)
        {
            return Err(read.damaged("a partition record of the vector part disagrees with itself"));
        }
        Ok(partition)
    }
}

/// Bytes of a partition's record.
const ENTRY: usize = 73;

/// What a store is told when a count the vector part keeps is below what it counts.
pub(super) const SHORT: &str = "a count of the vector part is below what it counts";

/// The partition a column has under a key tuple at the committed moment, `None` when it has
/// none.
pub(super) fn partition(
    read: &dyn Read,
    vector: VectorId,
    tuple: &Key,
) -> Result<Option<Partition>> {
    let record = VectorRecord::Partition(vector, tuple);
    kept(read, record, |entry| Partition::decode(read, entry))?.transpose()
}

/// What `read` gives for the value of one record of the vector part at the committed moment,
/// `None` when the record is not there.
pub(super) fn kept<T>(
    from: &dyn Read,
    record: VectorRecord<'_>,
    read: impl Fn(&[u8]) -> T,
) -> Result<Option<T>> {
    let mut found = None;
    from.vectors(record, &mut |first, value| {
        found = (first == record).then(|| read(value));
        Ok(false)
    })?;
    Ok(found)
}

/// Bytes [`selected`] reserves for each partition it answers, beside its key tuple: its entry
/// while the partitions are gathered and its entry in the list they are answered in.
pub(super) const HELD: u64 = 2 * size_of::<(Key, Partition)>() as u64;

/// The partitions a search covers: those whose key tuple begins with one of `scope`, or every
/// partition of the column when `scope` is empty. Each comes once with its key tuple, in the
/// order of the numbers, whatever the order of `scope`. A tuple no row has a vector under
/// selects nothing. [`HELD`] bytes and those of its key tuple are taken from `reservation` for
/// each before it is kept; the caller gives them back when it lets the list go.
pub(super) fn selected(
    read: &dyn Read,
    vector: VectorId,
    scope: &[Key],
    reservation: &mut Reservation,
) -> Result<Vec<(Key, Partition)>> {
    // Every key tuple begins with the key of no values.
    let every = [Key::from_values(&[], &[])?];
    let scope = if scope.is_empty() { &every } else { scope };
    let mut found = BTreeMap::new();
    for prefix in scope {
        read.vectors(
            VectorRecord::Partition(vector, prefix),
            &mut |record, entry| match record {
                VectorRecord::Partition(column, tuple)
                    if column == vector && tuple.starts_with(prefix) =>
                {
                    let partition = Partition::decode(read, entry)?;
                    if let Entry::Vacant(place) = found.entry(partition.number) {
                        reservation.grow(HELD + tuple.as_bytes().len() as u64)?;
                        place.insert((tuple.clone(), partition));
                    }
                    Ok(true)
                }
                _ => Ok(false),
            },
        )?;
    }
    Ok(found.into_values().collect())
}
