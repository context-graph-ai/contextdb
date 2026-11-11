//! The grouping of a partition: its slots sorted by the centre each is nearest, packed into full
//! blocks, with the centres, the place where each group's slots begin, and a sketch of every
//! slot taken from its group's centre.
//!
//! A grouping is written once, by the build that made it, and never changed: the partition's
//! record names the grouping in force, and one commit replaces that name. A partition's mean
//! is written once too, by the first build that finds centres, and stays while the partition does.

use std::cmp::Ordering;

use super::block::BLOCK_BYTES;
use super::directory::{Grouped, kept};
use super::slot::summed;
use crate::memory::Reservation;
use crate::store::{ChangeSet, Read, VectorId, VectorRecord};
use crate::types::Result;

/// A slot of a grouping: the block it is in and its place in that block, both from zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub(super) struct Place {
    pub block: u32,
    pub slot: u32,
}

/// What a search of the nearest groups reads of a grouping.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Grouping {
    /// How far a search scans, in ten-thousandths of a product, from 0 to 20,000: every group
    /// whose centre's product with the vector of the search is within this of the best
    /// centre's. The most any of the questions the build asked needed for its ten nearest.
    pub gap: u32,
    /// The most groups that gap took in for any of those questions, from one to the number of
    /// groups: what inspection prints. No search reads it.
    pub probes: u32,
    /// How wide a search takes a sketch's estimate to be, in quarters of the sketch's factor:
    /// the least of 2 to 16 that kept 99 in 100 of the neighbours the build measured, and 0
    /// when none did, in which case a search screens the slots of its groups directly.
    pub width: u8,
    /// Where each group's slots begin, in the order of the groups, and after the last of them
    /// where the slots end: one more place than there are groups. Group `g` is the slots from
    /// `starts[g]` up to `starts[g + 1]`.
    pub starts: Vec<Place>,
}

impl Grouping {
    /// The record of the grouping: `gap`, `width` and `probes` as four bytes each, then every place
    /// as two numbers of four bytes, all low byte first.
    pub(super) fn encode(&self) -> Vec<u8> {
        let head = [self.gap, u32::from(self.width), self.probes];
        let places = self.starts.iter();
        let places = places.flat_map(|place| [place.block, place.slot]);
        let numbers = head.into_iter().chain(places);
        numbers.flat_map(u32::to_le_bytes).collect()
    }

    /// The grouping kept under `grouped.number`. A record that is missing, of a length other
    /// than `12 + 8 * (grouped.groups + 1)`, with a `gap` above 20,000, with `probes` outside
    /// `1..=grouped.groups`, with a
    /// `width` of 1 or above 16, with a first place other than the first slot, with a place
    /// before the one ahead of it or a last place past `grouped.blocks`, is damage.
    pub(super) fn read(read: &dyn Read, vector: VectorId, grouped: Grouped) -> Result<Grouping> {
        let end = Place {
            block: grouped.blocks,
            slot: 0,
        };
        let record = VectorRecord::Grouping(vector, grouped.number);
        let found = kept(read, record, |value| {
            if value.len() as u64 != 20 + 8 * u64::from(grouped.groups) {
                return None;
            }
            let (numbers, _) = value.as_chunks::<4>();
            let number = |at: usize| u32::from_le_bytes(numbers[at]);
            let (gap, width, probes) = (number(0), number(1), number(2));
            let places = (3..numbers.len()).step_by(2);
            let starts: Vec<Place> = places
                .map(|at| Place {
                    block: number(at),
                    slot: number(at + 1),
                })
                .collect();
            let agrees = gap <= 20_000
                && (1..=grouped.groups).contains(&probes)
                && (width == 0 || (2..=16).contains(&width))
                && starts[0] == Place::default()
                && starts.is_sorted()
                && starts[starts.len() - 1] <= end;
            agrees.then_some(Grouping {
                gap,
                probes,
                width: width as u8,
                starts,
            })
        })?;
        let damage = "a grouping of the vector part is not what its partition says";
        found.flatten().ok_or_else(|| read.damaged(damage))
    }
}

/// The numbers of four bytes, low byte first, that these bytes hold.
fn numbers(bytes: &[u8]) -> impl Iterator<Item = f32> {
    let (numbers, _) = bytes.as_chunks::<4>();
    numbers.iter().map(|number| f32::from_le_bytes(*number))
}

/// How many centres of this dimension one record holds: as many as fit the bytes of a block.
pub(super) fn centres_to_a_record(dimension: usize) -> usize {
    (BLOCK_BYTES / (4 * dimension)).max(1)
}

/// Stages the centres of a grouping, `centres_to_a_record` to a record, in the order of the
/// groups. Each centre is `dimension` numbers of four bytes, low byte first, of length one.
pub(super) fn put_centres(
    changes: &mut ChangeSet,
    vector: VectorId,
    number: u32,
    dimension: usize,
    centres: &[f32],
) -> Result<()> {
    let records = centres.chunks(centres_to_a_record(dimension) * dimension);
    for (place, record) in records.enumerate() {
        let value = record.iter().flat_map(|number| number.to_le_bytes());
        let record = VectorRecord::Centres(vector, number, place as u32);
        changes.put_vector(record, value.collect())?;
    }
    Ok(())
}

/// Hands `each` the bytes of every centre of a grouping, in the order of the groups, read in
/// place. Records that do not hold `groups` centres of this dimension are damage.
fn each_centre(
    read: &dyn Read,
    vector: VectorId,
    number: u32,
    groups: u32,
    dimension: usize,
    each: &mut dyn FnMut(&[u8]),
) -> Result<()> {
    let (mut count, mut whole) = (0, true);
    let from = VectorRecord::Centres(vector, number, 0);
    read.vectors(from, &mut |record, value| match record {
        VectorRecord::Centres(column, grouping, _) if column == vector && grouping == number => {
            let more = value.len() / (4 * dimension);
            whole = value.len() % (4 * dimension) == 0 && more <= (groups - count) as usize;
            if whole {
                value.chunks(4 * dimension).for_each(&mut *each);
                count += more as u32;
            }
            Ok(whole)
        }
        _ => Ok(false),
    })?;
    if whole && count == groups {
        Ok(())
    } else {
        Err(read.damaged("the centres of a grouping are not as many as its groups"))
    }
}

/// The centres of a grouping, one after another, read into memory that `held` is grown by
/// first: four bytes a number. Records that do not hold `groups` centres of this dimension, or
/// a number that is not finite, are damage.
pub(super) fn centres(
    read: &dyn Read,
    vector: VectorId,
    number: u32,
    groups: u32,
    dimension: usize,
    held: &mut Reservation,
) -> Result<Vec<f32>> {
    held.grow(4 * dimension as u64 * u64::from(groups))?;
    let mut centres = Vec::with_capacity(dimension * groups as usize);
    each_centre(read, vector, number, groups, dimension, &mut |centre| {
        centres.extend(numbers(centre));
    })?;
    if centres.iter().all(|number| number.is_finite()) {
        Ok(centres)
    } else {
        Err(read.damaged("a centre of a grouping has a number that is not finite"))
    }
}

/// Every group of a grouping with the product of its centre with `unit`, nearest first: by that product,
/// greatest first, groups of one product in the order of their numbers. The centres are read
/// in place; `held` is grown by eight bytes a group for the list. A grouping of one group has
/// no centre: its group is answered with a product of zero and nothing is read. A product
/// that is not finite is damage.
pub(super) fn nearest(
    read: &dyn Read,
    vector: VectorId,
    grouped: Grouped,
    unit: &[f32],
    held: &mut Reservation,
) -> Result<Vec<(u32, f32)>> {
    held.grow(8 * u64::from(grouped.groups))?;
    if grouped.groups == 1 {
        return Ok(vec![(0, 0.0)]);
    }
    let mut found = Vec::with_capacity(grouped.groups as usize);
    let centre = &mut |centre: &[u8]| {
        let (centre, _) = centre.as_chunks::<4>();
        found.push((found.len() as u32, summed(centre, unit, f32::from_le_bytes)));
    };
    let Grouped { number, groups, .. } = grouped;
    each_centre(read, vector, number, groups, unit.len(), centre)?;
    if found.iter().any(|(_, product)| !product.is_finite()) {
        return Err(read.damaged("a centre of a grouping has a number that is not finite"));
    }
    // The sort keeps groups of one product in the order they were read, that of their numbers.
    found.sort_by(|one, other| other.1.partial_cmp(&one.1).unwrap_or(Ordering::Equal));
    Ok(found)
}

/// Stages the mean of a partition: `dimension` numbers of four bytes, low byte first.
pub(super) fn put_mean(
    changes: &mut ChangeSet,
    vector: VectorId,
    partition: u32,
    mean: &[f32],
) -> Result<()> {
    let value = mean.iter().flat_map(|number| number.to_le_bytes());
    changes.put_vector(VectorRecord::Mean(vector, partition), value.collect())
}

/// The mean of a partition, read into memory that `held` is grown by first. A record that is
/// missing for a partition whose entry says it has one, of another length than four bytes a
/// number, or with a number that is not finite, is damage.
pub(super) fn mean(
    read: &dyn Read,
    vector: VectorId,
    partition: u32,
    dimension: usize,
    held: &mut Reservation,
) -> Result<Vec<f32>> {
    held.grow(4 * dimension as u64)?;
    let record = VectorRecord::Mean(vector, partition);
    let found = kept(read, record, |value| {
        (value.len() == 4 * dimension).then(|| numbers(value).collect::<Vec<f32>>())
    })?;
    let found = found.flatten();
    let found = found.filter(|mean| mean.iter().all(|number| number.is_finite()));
    found.ok_or_else(|| read.damaged("the mean of a partition is not what its entry says"))
}
