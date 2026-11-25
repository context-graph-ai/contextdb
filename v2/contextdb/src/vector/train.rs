//! Finding centroids: the arithmetic of a build, over slots alone. Nothing here reads a store.
//!
//! The vector a slot keeps is its codes times its scale, a vector of length one within the
//! slot's rounding error. Centroids are found from those, so a build reads blocks and no row.

use std::collections::BTreeMap;

use super::slot::{self, SLOT_HEADER};
use crate::memory::Reservation;
use crate::types::Result;

/// How many times a build goes over its sample.
pub(super) const ROUNDS: u32 = 10;

/// How many slots a build samples for each centroid it looks for.
pub(super) const SAMPLE_TO_A_CENTROID: u64 = 64;

/// Bytes a group that [`first_centroids`] takes while it draws: an entry of its list of moved
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
    let sample = slots.min(SAMPLE_TO_A_CENTROID * u64::from(groups));
    (0..sample).map(move |at| (u128::from(at) * u128::from(slots) / u128::from(sample)) as u64)
}

/// Which of the sampled slots, counted from zero, the first centroids are taken from: `groups`
/// different ones, ascending, the same for the same two numbers.
///
/// The choice is the start of a shuffle of the sampled slots by one seeded sequence: for each
/// centroid in turn, the slot at its place changes places with one at or after it. Only the
/// places that have changed are kept, never the whole sample.
pub(super) fn first_centroids(sampled: u64, groups: u32) -> Vec<u64> {
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

/// The product of a centroid with the vector a slot keeps.
pub(super) fn product(centroid: &[f32], slot: &[u8]) -> f32 {
    slot::scale(slot) * slot::product(&slot[SLOT_HEADER..], centroid)
}

/// The centroid nearest the vector a slot keeps, by [`product`], and that product: the greatest,
/// and of equal ones the centroid with the lowest number.
pub(super) fn nearest(centroids: &[f32], dimension: usize, slot: &[u8]) -> (u32, f32) {
    nearest_among(centroids, dimension, slot, None)
}

/// The nearest centroid among eligible groups; no filter while training a new IVF.
pub(super) fn nearest_among(
    centroids: &[f32],
    dimension: usize,
    slot: &[u8],
    eligible: Option<&dyn Fn(usize) -> bool>,
) -> (u32, f32) {
    let mut best = (0, f32::NEG_INFINITY);
    for (number, centroid) in centroids.chunks_exact(dimension).enumerate() {
        if eligible.is_some_and(|eligible| !eligible(number)) {
            continue;
        }
        let product = product(centroid, slot);
        if product > best.1 {
            best = (number as u32, product);
        }
    }
    best
}

/// Adds the vector a slot keeps, its scale times each code, to `sum`.
pub(super) fn add(sum: &mut [f64], slot: &[u8]) {
    scaled(sum, slot, 1.0);
}

/// Adds `by` times the vector a slot keeps to `sum`: -1 takes it away.
pub(super) fn scaled(sum: &mut [f64], slot: &[u8], by: f64) {
    let scale = by * f64::from(slot::scale(slot));
    for (sum, code) in sum.iter_mut().zip(&slot[SLOT_HEADER..]) {
        *sum += scale * f64::from(code.cast_signed());
    }
}

/// One round over the sample: the sum of the vectors that are nearest each centroid.
pub(super) struct Round {
    sums: Vec<f64>,
    counts: Vec<u32>,
    dimension: usize,
}

impl Round {
    /// A round for this many centroids. `held` is grown by what the sums take: eight bytes a
    /// number and four a centroid.
    pub(super) fn new(groups: u32, dimension: usize, held: &mut Reservation) -> Result<Round> {
        let groups = groups as usize;
        held.grow((8 * dimension * groups + 4 * groups) as u64)?;
        Ok(Round {
            sums: vec![0.0; dimension * groups],
            counts: vec![0; groups],
            dimension,
        })
    }

    /// Adds the vector a slot keeps to the sum of the centroid it is nearest.
    pub(super) fn take(&mut self, centroids: &[f32], slot: &[u8]) {
        let (group, _) = nearest(centroids, self.dimension, slot);
        let at = group as usize * self.dimension;
        add(&mut self.sums[at..at + self.dimension], slot);
        self.counts[group as usize] += 1;
    }

    /// Moves every centroid to the direction of its sum, at length one. A centroid that no
    /// sampled slot was nearest, or whose sum has no direction, stays where it is.
    pub(super) fn finish(self, centroids: &mut [f32]) {
        let sums = self.sums.chunks_exact(self.dimension);
        let centroids = centroids.chunks_exact_mut(self.dimension);
        for ((sum, centroid), count) in sums.zip(centroids).zip(self.counts) {
            if count > 0 {
                normalize(sum, centroid);
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
        mean(&self.sum, self.count)
    }
}

/// The mean of a persisted or accumulated sum; zeros when no slot was taken.
pub(super) fn mean(sum: &[f64], count: u64) -> Vec<f32> {
    let count = count.max(1) as f64;
    sum.iter().map(|sum| (sum / count) as f32).collect()
}

/// The direction of a sum; a sum without direction leaves its centroid unchanged.
pub(super) fn normalize(sum: &[f64], centroid: &mut [f32]) {
    let length = sum.iter().map(|x| x * x).sum::<f64>().sqrt();
    if length > 0.0 {
        for (number, sum) in centroid.iter_mut().zip(sum) {
            *number = (sum / length) as f32;
        }
    }
}
