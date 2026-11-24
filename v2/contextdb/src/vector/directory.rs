//! The partitions of a VECTOR column: one record for each key tuple under which a row has a
//! vector.

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;

use crate::memory::Reservation;
use crate::schema::VectorColumn;
use crate::store::{Key, Read, VectorRecord};
use crate::types::Result;

/// What a column keeps about one partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(super) struct Partition {
    /// The partition's number within its column. Pending rows and blocks are kept under it.
    pub number: u32,
    /// How many rows have a vector in the partition, pending or in a block.
    pub live: u64,
    /// Rows not yet sealed into a block.
    pub unindexed: u32,
    /// Encoded slots, key ends and keys unindexed until sealed.
    pub pending_bytes: u64,
    /// Bytes still available in the last tail block, while it is open.
    pub tail_room: u32,
    /// The number after the partition's last tail block. The tail blocks are the blocks
    /// sealing adds to: they are kept under `number` and numbered from `first`.
    pub blocks: u32,
    /// The number of the partition's first tail block. The tail blocks before it went into the
    /// IVF in force and are gone.
    pub first: u32,
    /// Tail blocks numbered below this are never added to again: an IVF being built reads
    /// them. Every tail block from here on but the last is full.
    pub closed: u32,
    /// How many slots the tail blocks hold, the slots rows have left behind among them.
    pub slots: u64,
    /// How many slots have ever been sealed into the partition's tail. It only rises: the tail
    /// slots are the last `slots` of that count, in the order they were sealed.
    pub sealed: u64,
    /// The IVF in force, `None` until one has been built.
    pub ivf: Option<Grouped>,
    /// The number an IVF being built is kept under and the committed view it reads, whose
    /// departures alone belong to it; `None` when none is.
    pub building: Option<(u32, u64)>,
    /// Live outside slots assigned since the centres were last found anew.
    pub assigned: u64,
}

/// Where an IVF's records are and how many it has: all a search of every slot, and a
/// commit that removes the IVF, need of it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Grouped {
    /// The number its blocks, its centroids and its record are kept under.
    pub number: u32,
    /// How many blocks it has, numbered from zero; every one but the last is full.
    pub blocks: u32,
    /// How many groups it has.
    pub groups: u32,
    /// How many slots its blocks hold, the slots rows have left behind since among them.
    pub slots: u64,
    /// The partition's `sealed` when the build that made it began. A row that left its slot
    /// since, a `Left` record of that value or a greater, leaves a slot of this IVF stale.
    pub upto: u64,
}

impl Partition {
    /// The record of the partition, 105 bytes, every number low byte first: `number`, `live`,
    /// `blocks`, `first`, `closed`, `slots`, `sealed`, one byte of flags (1: it has an IVF;
    /// 2: it is building one), the five numbers of the IVF in force, the number being
    /// built, the pending counts, tail room, assigned count and the view being built from. An
    /// unset field is zero.
    pub(super) fn encode(&self) -> Vec<u8> {
        let ivf = self.ivf.unwrap_or(Grouped {
            number: 0,
            blocks: 0,
            groups: 0,
            slots: 0,
            upto: 0,
        });
        let flags = u8::from(self.ivf.is_some()) | (u8::from(self.building.is_some()) << 1);
        let mut record = Vec::with_capacity(ENTRY);
        record.extend(self.number.to_le_bytes());
        record.extend(self.live.to_le_bytes());
        record.extend(self.blocks.to_le_bytes());
        record.extend(self.first.to_le_bytes());
        record.extend(self.closed.to_le_bytes());
        record.extend(self.slots.to_le_bytes());
        record.extend(self.sealed.to_le_bytes());
        record.push(flags);
        record.extend(ivf.number.to_le_bytes());
        record.extend(ivf.blocks.to_le_bytes());
        record.extend(ivf.groups.to_le_bytes());
        record.extend(ivf.slots.to_le_bytes());
        record.extend(ivf.upto.to_le_bytes());
        let (building, source) = self.building.unwrap_or_default();
        record.extend(building.to_le_bytes());
        record.extend(self.unindexed.to_le_bytes());
        record.extend(self.pending_bytes.to_le_bytes());
        record.extend(self.tail_room.to_le_bytes());
        record.extend(self.assigned.to_le_bytes());
        record.extend(source.to_le_bytes());
        record
    }

    /// The partition a stored record was written from. A record of another length, with a
    /// flag above the two, with more slots in its tail than were ever sealed, building from a
    /// view after the reader's, with its first tail block after its closed one or its closed
    /// one after its last, with an IVF of no group, is damage.
    pub(super) fn read(
        read: &dyn Read,
        column: &VectorColumn,
        tuple: &Key,
        record: &[u8],
    ) -> Result<Partition> {
        if !tuple.has_arity(column.partition_key.len()) {
            return Err(read.damaged("a vector partition tuple differs from its declared arity"));
        }
        if record.len() != ENTRY {
            return Err(read.damaged("a partition record of the vector part is not 105 bytes"));
        }
        let short = |at: usize| u32::from_le_bytes(std::array::from_fn(|byte| record[at + byte]));
        let long = |at: usize| u64::from_le_bytes(std::array::from_fn(|byte| record[at + byte]));
        let flags = record[40];
        let partition = Partition {
            number: short(0),
            live: long(4),
            unindexed: short(73),
            pending_bytes: long(77),
            tail_room: short(85),
            blocks: short(12),
            first: short(16),
            closed: short(20),
            slots: long(24),
            sealed: long(32),
            ivf: (flags & 1 != 0).then(|| Grouped {
                number: short(41),
                blocks: short(45),
                groups: short(49),
                slots: long(53),
                upto: long(61),
            }),
            building: (flags & 2 != 0).then(|| (short(69), long(97))),
            assigned: long(89),
        };
        let groups = partition.ivf.map_or(1, |ivf| ivf.groups);
        if flags > 3
            || partition.tail_room as usize > super::block::BLOCK_BYTES - 4
            || (partition.unindexed == 0) != (partition.pending_bytes == 0)
            || u64::from(partition.unindexed) > partition.live
            || partition.sealed < partition.slots
            || partition.building.is_some_and(|b| b.1 > read.commit().0)
            || partition.first > partition.closed
            || partition.closed > partition.blocks
            || groups == 0
        {
            return Err(read.damaged("a partition record of the vector part disagrees with itself"));
        }
        Ok(partition)
    }
}

/// Bytes of a partition's record.
const ENTRY: usize = 105;

/// What a store is told when a count the vector part keeps is below what it counts.
pub(super) const SHORT: &str = "a count of the vector part is below what it counts";

/// The partition a column has under a key tuple at the committed moment, `None` when it has
/// none.
pub(super) fn partition(
    read: &dyn Read,
    column: &VectorColumn,
    tuple: &Key,
) -> Result<Option<Partition>> {
    let record = VectorRecord::Partition(column.id, tuple);
    let decode = |entry: &[u8]| Partition::read(read, column, tuple, entry);
    kept(read, record, decode)?.transpose()
}

/// What `read` gives for the value of one record of the vector part at the committed moment,
/// `None` when the record is not there.
pub(super) fn kept<T>(
    from: &dyn Read,
    record: VectorRecord<'_>,
    mut read: impl FnMut(&[u8]) -> T,
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
    column: &VectorColumn,
    scope: &[Key],
    reservation: &mut Reservation,
) -> Result<Vec<(Key, Partition)>> {
    // Every key tuple begins with the key of no values.
    let every = [Key::from_values(&[], &[])?];
    let scope = if scope.is_empty() { &every } else { scope };
    let mut found = BTreeMap::new();
    for prefix in scope {
        read.vectors(
            VectorRecord::Partition(column.id, prefix),
            &mut |record, entry| match record {
                VectorRecord::Partition(vector, tuple)
                    if vector == column.id && tuple.starts_with(prefix) =>
                {
                    let partition = Partition::read(read, column, tuple, entry)?;
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
