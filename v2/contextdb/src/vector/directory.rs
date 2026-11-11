//! The partitions of a VECTOR column: one record for each key tuple under which a row has a
//! vector.

use std::collections::BTreeMap;

use crate::store::{Key, Read, VectorId, VectorRecord};
use crate::types::Result;

/// What a column keeps about one partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(super) struct Partition {
    /// The partition's number within its column. Pending rows and blocks are kept under it.
    pub number: u32,
    /// How many rows have a vector in the partition, pending or in a block.
    pub live: u64,
    /// How many blocks the partition has. They are numbered from zero, and every one but the
    /// last is full.
    pub blocks: u32,
}

impl Partition {
    /// The record of the partition: its three numbers, low byte first.
    pub(super) fn encode(&self) -> Vec<u8> {
        let mut record = Vec::with_capacity(16);
        record.extend(self.number.to_le_bytes());
        record.extend(self.live.to_le_bytes());
        record.extend(self.blocks.to_le_bytes());
        record
    }

    /// The partition a record was written from.
    pub(super) fn decode(record: &[u8]) -> Partition {
        Partition {
            number: u32::from_le_bytes(std::array::from_fn(|at| record[at])),
            live: u64::from_le_bytes(std::array::from_fn(|at| record[4 + at])),
            blocks: u32::from_le_bytes(std::array::from_fn(|at| record[12 + at])),
        }
    }
}

/// What a store is told when a count the vector part keeps is below what it counts.
pub(super) const SHORT: &str = "a count of the vector part is below what it counts";

/// The partition a stored record was written from. A record of another length is damage.
fn checked(read: &dyn Read, record: &[u8]) -> Result<Partition> {
    if record.len() != 16 {
        return Err(read.damaged("a partition record of the vector part is not 16 bytes"));
    }
    Ok(Partition::decode(record))
}

/// The partition a column has under a key tuple at the committed moment, `None` when it has
/// none.
pub(super) fn partition(
    read: &dyn Read,
    vector: VectorId,
    tuple: &Key,
) -> Result<Option<Partition>> {
    let record = VectorRecord::Partition(vector, tuple);
    kept(read, record, |entry| checked(read, entry))?.transpose()
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

/// The partitions a search covers: those whose key tuple begins with one of `scope`, or every
/// partition of the column when `scope` is empty. Each comes once, in the order of the
/// numbers, whatever the order of `scope`. A tuple no row has a vector under selects nothing.
pub(super) fn selected(read: &dyn Read, vector: VectorId, scope: &[Key]) -> Result<Vec<Partition>> {
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
                    let partition = checked(read, entry)?;
                    found.insert(partition.number, partition);
                    Ok(true)
                }
                _ => Ok(false),
            },
        )?;
    }
    Ok(found.into_values().collect())
}
