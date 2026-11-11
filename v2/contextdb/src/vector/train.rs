//! Finding centres: the arithmetic of a build, over slots alone. Nothing here reads a store.
//!
//! The vector a slot keeps is its codes times its scale, a vector of length one within the
//! slot's rounding error. Centres are found from those, so a build reads blocks and no row.

use std::collections::BTreeMap;

use super::slot::{self, SLOT_HEADER};
use crate::memory::Reservation;
use crate::types::Result;

/// How many times a build goes over its sample.
pub(super) const ROUNDS: u32 = 10;

/// How many slots a build samples for each centre it looks for.
pub(super) const SAMPLE_TO_A_CENTRE: u64 = 64;

/// Bytes a group that [`first_centres`] takes while it draws: an entry of its list of moved
/// places and that entry's share of the list's nodes.
pub(super) const DRAWN: u64 = 48;

/// How many groups a partition of this many slots is given: the square root, rounded, and
/// from 1 to 65,535.
pub(super) fn groups_for(slots: u64) -> u32 {
    ((slots as f64).sqrt().round() as u32).clamp(1, 65_535)
}

/// Which of `slots` slots, counted from zero in the order the build reads them, a round
/// samples: `min(slots, 64 * groups)` of them, evenly spread, ascending.
pub(super) fn sampled(slots: u64, groups: u32) -> impl Iterator<Item = u64> {
    let sample = slots.min(SAMPLE_TO_A_CENTRE * u64::from(groups));
    (0..sample).map(move |at| (u128::from(at) * u128::from(slots) / u128::from(sample)) as u64)
}

/// Which of the sampled slots, counted from zero, the first centres are taken from: `groups`
/// different ones, ascending, the same for the same two numbers.
///
/// The choice is the start of a shuffle of the sampled slots by one seeded sequence: for each
/// centre in turn, the slot at its place changes places with one at or after it. Only the
/// places that have changed are kept, never the whole sample.
pub(super) fn first_centres(sampled: u64, groups: u32) -> Vec<u64> {
    let mut seeded: u64 = 0x5EED_C1A5_7E25 | 1;
    let mut moved = BTreeMap::new();
    let mut first = Vec::with_capacity(groups as usize);
    for place in 0..u64::from(groups) {
        seeded ^= seeded << 13;
        seeded ^= seeded >> 7;
        seeded ^= seeded << 17;
        let other = place + seeded % (sampled - place);
        let here = moved.get(&place).copied().unwrap_or(place);
        first.push(moved.insert(other, here).unwrap_or(other));
    }
    first.sort_unstable();
    first
}

/// The scale of a slot.
fn scale(slot: &[u8]) -> f32 {
    f32::from_le_bytes([slot[0], slot[1], slot[2], slot[3]])
}

/// The product of a centre with the vector a slot keeps.
pub(super) fn product(centre: &[f32], slot: &[u8]) -> f32 {
    scale(slot) * slot::product(&slot[SLOT_HEADER..], centre)
}

/// The centre nearest the vector a slot keeps, by [`product`], and that product: the greatest,
/// and of equal ones the centre with the lowest number.
pub(super) fn nearest(centres: &[f32], dimension: usize, slot: &[u8]) -> (u32, f32) {
    let mut best = (0, f32::NEG_INFINITY);
    for (number, centre) in centres.chunks_exact(dimension).enumerate() {
        let product = product(centre, slot);
        if product > best.1 {
            best = (number as u32, product);
        }
    }
    best
}

/// Adds the vector a slot keeps, its scale times each code, to `sum`.
fn add(sum: &mut [f64], slot: &[u8]) {
    let scale = f64::from(scale(slot));
    for (sum, code) in sum.iter_mut().zip(&slot[SLOT_HEADER..]) {
        *sum += scale * f64::from(code.cast_signed());
    }
}

/// One round over the sample: the sum of the vectors that are nearest each centre.
pub(super) struct Round {
    sums: Vec<f64>,
    counts: Vec<u32>,
    dimension: usize,
}

impl Round {
    /// A round for this many centres. `held` is grown by what the sums take: eight bytes a
    /// number and four a centre.
    pub(super) fn new(groups: u32, dimension: usize, held: &mut Reservation) -> Result<Round> {
        let groups = groups as usize;
        held.grow((8 * dimension * groups + 4 * groups) as u64)?;
        Ok(Round {
            sums: vec![0.0; dimension * groups],
            counts: vec![0; groups],
            dimension,
        })
    }

    /// Adds the vector a slot keeps to the sum of the centre it is nearest.
    pub(super) fn take(&mut self, centres: &[f32], slot: &[u8]) {
        let (group, _) = nearest(centres, self.dimension, slot);
        let at = group as usize * self.dimension;
        add(&mut self.sums[at..at + self.dimension], slot);
        self.counts[group as usize] += 1;
    }

    /// Moves every centre to the direction of its sum, at length one. A centre that no
    /// sampled slot was nearest, or whose sum has no direction, stays where it is.
    pub(super) fn finish(self, centres: &mut [f32]) {
        let sums = self.sums.chunks_exact(self.dimension);
        let centres = centres.chunks_exact_mut(self.dimension);
        for ((sum, centre), count) in sums.zip(centres).zip(self.counts) {
            let length = sum.iter().map(|x| x * x).sum::<f64>().sqrt();
            if count > 0 && length > 0.0 {
                for (number, sum) in centre.iter_mut().zip(sum) {
                    *number = (sum / length) as f32;
                }
            }
        }
    }
}

/// The mean of the vectors the sampled slots keep, which a partition's first build takes in
/// its first round.
pub(super) struct Mean {
    sum: Vec<f64>,
    count: u64,
}

impl Mean {
    /// A mean of vectors of this dimension. `held` is grown by eight bytes a number.
    pub(super) fn new(dimension: usize, held: &mut Reservation) -> Result<Mean> {
        held.grow(8 * dimension as u64)?;
        Ok(Mean {
            sum: vec![0.0; dimension],
            count: 0,
        })
    }

    /// Adds the vector a slot keeps.
    pub(super) fn take(&mut self, slot: &[u8]) {
        add(&mut self.sum, slot);
        self.count += 1;
    }

    /// The sum divided by the count, number by number; zeros when nothing was taken. The
    /// caller has taken its four bytes a number before it calls.
    pub(super) fn finish(self) -> Vec<f32> {
        let count = self.count.max(1) as f64;
        self.sum.iter().map(|sum| (sum / count) as f32).collect()
    }
}
