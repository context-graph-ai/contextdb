//! The IVF of a partition: its slots sorted by the centroid each is nearest, packed into full
//! blocks, with the centroids, the place where each group's slots begin, and a binary code of every
//! slot taken from its group's centroid.
//!
//! An IVF is written once, by the build that made it, and never changed: the partition's
//! record names the IVF in force, and one commit replaces that name. The IVF owns
//! its packed members' mean, which supplies the reference for the partition's tail codes.

use std::cmp::Ordering;

use super::block::BLOCK_BYTES;
use super::directory::{Grouped, kept};
use super::slot::summed;
use crate::memory::Reservation;
use crate::store::{ChangeSet, Read, VectorId, VectorRecord};
use crate::types::Result;

/// A slot of an IVF: the block it is in and its place in that block, both from zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub(super) struct Place {
    pub block: u32,
    pub slot: u32,
}

/// What a search of the nearest groups reads of an IVF.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Ivf {
    /// How far a search scans, in ten-thousandths of a product, from 0 to 20,000: every group
    /// whose centroid's product with the vector of the search is within this of the best
    /// centroid's. The most any of the questions the build asked needed for its ten nearest.
    pub gap: u32,
    /// The most groups that gap took in for any of those questions, from one to the number of
    /// groups: what inspection prints. No search reads it.
    pub probes: u32,
    /// How wide a search takes a binary code's estimate to be, in quarters of the binary code's factor:
    /// the least of 2 to 16 that kept 99 in 100 of the neighbours the build measured, and 0
    /// when none did, in which case a search screens the slots of its groups directly.
    pub width: u8,
    /// Where each group's slots begin, in the order of the groups, and after the last of them
    /// where the slots end: one more place than there are groups. Group `g` is the slots from
    /// `starts[g]` up to `starts[g + 1]`.
    pub starts: Vec<Place>,
}

impl Ivf {
    pub(super) fn nonempty(&self, group: u32) -> bool {
        let group = group as usize;
        self.starts[group] != self.starts[group + 1]
    }

    /// The record of the IVF: `gap`, `width` and `probes` as four bytes each, then every place
    /// as two numbers of four bytes, all low byte first.
    pub(super) fn encode(&self) -> Vec<u8> {
        let head = [self.gap, u32::from(self.width), self.probes];
        let places = self.starts.iter();
        let places = places.flat_map(|place| [place.block, place.slot]);
        let numbers = head.into_iter().chain(places);
        numbers.flat_map(u32::to_le_bytes).collect()
    }

    /// The IVF kept under `grouped.number`. A record that is missing, of a length other
    /// than `12 + 8 * (grouped.groups + 1)`, with a `gap` above 20,000, with `probes` outside
    /// `1..=grouped.groups`, with a
    /// `width` of 1 or above 16, with a first place other than the first slot, with a place
    /// before the one ahead of it or a last place past `grouped.blocks`, is damage.
    pub(super) fn read(
        read: &dyn Read,
        vector: VectorId,
        grouped: Grouped,
        held: &mut Reservation,
    ) -> Result<Ivf> {
        let end = Place {
            block: grouped.blocks,
            slot: 0,
        };
        let record = VectorRecord::Ivf(vector, grouped.number);
        let found = kept(read, record, |value| -> Result<_> {
            if value.len() as u64 != 20 + 8 * u64::from(grouped.groups) {
                return Ok(None);
            }
            held.grow(value.len() as u64)?;
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
            Ok(agrees.then_some(Ivf {
                gap,
                probes,
                width: width as u8,
                starts,
            }))
        })?
        .transpose()?;
        let damage = "an IVF of the vector part is not what its partition says";
        found.flatten().ok_or_else(|| read.damaged(damage))
    }
}

/// The numbers of four bytes, low byte first, that these bytes hold.
fn numbers(bytes: &[u8]) -> impl Iterator<Item = f32> {
    let (numbers, _) = bytes.as_chunks::<4>();
    numbers.iter().map(|number| f32::from_le_bytes(*number))
}

/// How many centroids of this dimension one record holds: as many as fit the bytes of a block.
pub(super) fn centroids_per_record(dimension: usize) -> usize {
    (BLOCK_BYTES / (4 * dimension)).max(1)
}

/// Stages the centroids of an IVF, `centroids_per_record` to a record, in the order of the
/// groups. Each centroid is `dimension` numbers of four bytes, low byte first, of length one.
pub(super) fn put_centroids(
    changes: &mut ChangeSet,
    vector: VectorId,
    number: u32,
    dimension: usize,
    centroids: &[f32],
) -> Result<()> {
    let records = centroids.chunks(centroids_per_record(dimension) * dimension);
    for (place, record) in records.enumerate() {
        put_centroids_at(changes, (vector, number, place as u32), record)?;
    }
    Ok(())
}

pub(super) fn put_centroids_at(
    changes: &mut ChangeSet,
    (vector, number, place): (VectorId, u32, u32),
    centres: &[f32],
) -> Result<()> {
    let value = centres.iter().flat_map(|n| n.to_le_bytes()).collect();
    changes.put_vector(VectorRecord::Centroids(vector, number, place), value)
}

/// Hands `each` the bytes of every centroid of an IVF, in the order of the groups, read in
/// place. Records that do not hold `groups` centroids of this dimension are damage.
fn each_centroid(
    read: &dyn Read,
    (vector, number, groups, dimension): (VectorId, u32, u32, usize),
    range: std::ops::Range<u32>,
    each: &mut dyn FnMut(&[u8]) -> Result<()>,
) -> Result<()> {
    let per = centroids_per_record(dimension) as u32;
    let (mut count, mut whole) = (range.start, range.start <= range.end && range.end <= groups);
    let mut place = range.start / per;
    let from = VectorRecord::Centroids(vector, number, place);
    read.vectors(from, &mut |record, value| match record {
        VectorRecord::Centroids(column, ivf, at) if column == vector && ivf == number => {
            whole &= at == place
                && at * per < groups
                && value.len() == (groups - at * per).min(per) as usize * 4 * dimension;
            if whole {
                for centroid in value
                    .chunks(4 * dimension)
                    .skip((count - at * per) as usize)
                    .take((range.end - count) as usize)
                {
                    each(centroid)?;
                    count += 1;
                }
                place += 1;
            }
            Ok(whole && (count < range.end || range.end == groups))
        }
        _ => Ok(false),
    })?;
    if whole && count == range.end {
        Ok(())
    } else {
        Err(read.damaged("the centroids of an IVF are not as many as its groups"))
    }
}

/// The centroids of an IVF, one after another, read into memory that `held` is grown by
/// first: four bytes a number. Records that do not hold `groups` centroids of this dimension, or
/// a number that is not finite, are damage.
pub(super) fn centroids(
    read: &dyn Read,
    vector: VectorId,
    number: u32,
    groups: u32,
    dimension: usize,
    held: &mut Reservation,
) -> Result<Vec<f32>> {
    centroid_range(read, (vector, number, groups, dimension), 0..groups, held)
}

pub(super) fn centroid_range(
    read: &dyn Read,
    ivf: (VectorId, u32, u32, usize),
    range: std::ops::Range<u32>,
    held: &mut Reservation,
) -> Result<Vec<f32>> {
    let (count, dimension) = (range.len(), ivf.3);
    held.grow((4 * dimension * count) as u64)?;
    let mut centroids = Vec::with_capacity(dimension * count);
    each_centroid(read, ivf, range, &mut |centroid| {
        centroids.extend(numbers(centroid));
        Ok(())
    })?;
    if centroids.iter().all(|number| number.is_finite()) {
        Ok(centroids)
    } else {
        Err(read.damaged("a centroid of an IVF has a number that is not finite"))
    }
}

/// Checks the centroids of an IVF record by record, keeping none: the same damage as `centroids`.
pub(super) fn check_centroids(read: &dyn Read, ivf: (VectorId, u32, u32, usize)) -> Result<()> {
    each_centroid(read, ivf, 0..ivf.2, &mut |centroid| {
        let finite = numbers(centroid).all(f32::is_finite);
        let damage = "a centroid of an IVF has a number that is not finite";
        finite.then_some(()).ok_or_else(|| read.damaged(damage))
    })
}

/// Every group of an IVF with the product of its centroid with `unit`, nearest first: by that product,
/// greatest first, groups of one product in the order of their numbers. The centroids are read
/// in place; `held` is grown by eight bytes a group for the list. An IVF of one group has
/// no centroid: its group is answered with a product of zero and nothing is read. A product
/// that is not finite is damage.
pub(super) fn nearest(
    read: &dyn Read,
    vector: VectorId,
    grouped: Grouped,
    unit: &[f32],
    held: &mut Reservation,
    count: Option<&dyn Fn(u64) -> Result<()>>,
) -> Result<Vec<(u32, f32)>> {
    held.grow(8 * u64::from(grouped.groups))?;
    if grouped.groups == 1 {
        return Ok(vec![(0, 0.0)]);
    }
    let mut found = Vec::with_capacity(grouped.groups as usize);
    let centroid = &mut |centroid: &[u8]| {
        if let Some(count) = count {
            count(1)?;
        }
        let (centroid, _) = centroid.as_chunks::<4>();
        found.push((
            found.len() as u32,
            summed(centroid, unit, f32::from_le_bytes),
        ));
        Ok(())
    };
    let ivf = (vector, grouped.number, grouped.groups, unit.len());
    each_centroid(read, ivf, 0..grouped.groups, centroid)?;
    if found.iter().any(|(_, product)| !product.is_finite()) {
        return Err(read.damaged("a centroid of an IVF has a number that is not finite"));
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
