//! The frozen cost order and +1/+1 lattice, with producer-priced capacity.
use super::super::*;
use contextdb_core::VectorEnginePolicy;
use contextdb_vector::ivf::{Policy, query_workspace_allocation_bytes};
use std::cmp::Ordering;

#[derive(Clone, Copy, Debug)]
pub(super) struct Point {
    pub probes: u32,
    pub factor: u32,
    pub shortlist: usize,
    cost: f64,
}
impl Point {
    fn compare(self, other: Self) -> Ordering {
        self.cost
            .total_cmp(&other.cost)
            .then(self.probes.cmp(&other.probes))
            .then(self.shortlist.cmp(&other.shortlist))
            .then(self.factor.cmp(&other.factor))
    }
}

/// Fixed and already-admitted query/output controls are debited by the caller.
/// Only the actual constructor allowance and its separately invoiced codebook
/// remain in this comparison. This function neither reserves nor chooses a payer.
pub(super) fn candidate_capacity(
    policy: Policy,
    output: usize,
    book_bytes: usize,
    available: usize,
) -> usize {
    if output == 0 {
        return 0;
    }
    let Some(available) = available.checked_sub(book_bytes) else {
        return 0;
    };
    let fits = |count: usize| {
        let Some(policy) = Policy::checked(
            policy.dimensions(),
            policy.lists(),
            policy.probes(),
            count.max(1),
            policy.buffer_cap_bytes(),
            policy.extend_to_k(),
        ) else {
            return false;
        };
        query_workspace_allocation_bytes(policy, output).is_ok_and(|bytes| bytes <= available)
    };
    if !fits(output) {
        return 0;
    }
    let mut low = output;
    let mut high = usize::MAX;
    while low < high {
        let difference = high - low;
        let mid = low + difference / 2 + difference % 2;
        if fits(mid) {
            low = mid;
        } else {
            high = mid - 1;
        }
    }
    low
}

pub(super) fn shortlist(depth: u32, factor: u32) -> Result<usize> {
    let count = u64::from(depth)
        .checked_mul(u64::from(factor))
        .ok_or_else(corrupt)?
        .div_ceil(100);
    usize::try_from(count.max(u64::from(depth))).map_err(|_| corrupt())
}

pub(super) struct Lattice<'a> {
    pub policy: &'a VectorEnginePolicy,
    pub active: u32,
    pub dimensions: usize,
    pub population: u64,
    pub depth: u32,
    pub capacity: usize,
    pub fixed_probes: Option<u32>,
    pub fixed_factor: Option<u32>,
}
impl Lattice<'_> {
    fn next_probe(&self, previous: u32) -> Option<u32> {
        if self.active == 0 {
            return None;
        }
        if let Some(fixed) = self.fixed_probes {
            return (fixed != 0 && previous == 0).then_some(fixed.min(self.active));
        }
        if previous >= self.active {
            return None;
        }
        let grid = self.policy.probe_grid.unwrap_or(16);
        if grid == 0 {
            return None;
        }
        // Jump past duplicate ceil(L*i/grid) values without traversing a
        // consumer-declared grid that is larger than the active list count.
        let next_i = u128::from(previous) * u128::from(grid) / u128::from(self.active) + 1;
        let probe = (u128::from(self.active) * next_i).div_ceil(u128::from(grid));
        u32::try_from(probe)
            .ok()
            .filter(|p| *p > previous && *p <= self.active)
    }
    fn next_factor(&self, previous: Option<u32>) -> Result<Option<u32>> {
        if let Some(fixed) = self.fixed_factor {
            return Ok(previous.is_none().then_some(fixed));
        }
        let factors = self
            .policy
            .refine_factors
            .as_deref()
            .unwrap_or(&[100, 400, 1000, 2000]);
        let next = factors
            .iter()
            .copied()
            .filter(|value| previous.is_none_or(|previous| *value > previous))
            .min();
        if let Some(next) = next {
            return Ok((shortlist(self.depth, next)? <= self.capacity).then_some(next));
        }
        let Some(last) = factors.iter().copied().max() else {
            return Ok(None);
        };
        let mut factor = last;
        let growth = self.policy.refine_growth.unwrap_or(200);
        if growth <= 100 {
            return Err(corrupt());
        }
        loop {
            // Beyond this point every eligible identity is already in the
            // shortlist; another growth step cannot change the route's set.
            if shortlist(self.depth, factor)? as u128 >= u128::from(self.population) {
                return Ok(None);
            }
            let grown = (u64::from(factor) * u64::from(growth)).div_ceil(100);
            let Ok(next) = u32::try_from(grown) else {
                return Ok(None);
            };
            if next <= factor {
                return Err(corrupt());
            }
            factor = next;
            if shortlist(self.depth, factor)? > self.capacity {
                return Ok(None);
            }
            if previous.is_none_or(|previous| factor > previous) {
                return Ok(Some(factor));
            }
        }
    }
    pub(super) fn point(&self, probes: u32, factor: u32) -> Result<Point> {
        let shortlist = shortlist(self.depth, factor)?;
        let candidates = (shortlist as u128).min(u128::from(self.population)) as f64;
        let projected = self.population as f64 * f64::from(probes) / f64::from(self.active);
        let (records, block_pages) =
            contextdb_vector::ivf::list_block_layout(self.dimensions).ok_or_else(corrupt)?;
        // The accepted selector's byte/score estimate orders trials. It is
        // not memory admission; the producer quote alone supplies capacity.
        let pages = (projected / records as f64).ceil() * block_pages as f64;
        let cost = (pages + 2.0 * candidates) * PAGE_BYTES as f64
            + self.dimensions as f64
                * (2.0 * f64::from(self.active) * 4.0 + 2.0 * projected + 2.0 * candidates * 4.0);
        Ok(Point {
            probes,
            factor,
            shortlist,
            cost,
        })
    }
    /// Enumerate in the frozen cost order without retaining a grid-sized array.
    pub fn next(
        &self,
        previous: Option<Point>,
        checkpoint: &dyn Fn() -> Result<()>,
    ) -> Result<Option<Point>> {
        if self.active == 0 || self.depth == 0 || self.population == 0 {
            return Ok(None);
        }
        let mut best = None;
        let mut probe = 0;
        while let Some(next) = self.next_probe(probe) {
            probe = next;
            let mut factor = None;
            while let Some(next) = self.next_factor(factor)? {
                checkpoint()?;
                factor = Some(next);
                let point = self.point(probe, next)?;
                // An explicit point is attempted whole by the real constructor;
                // an inadmissible explicit declaration is never silently reduced.
                if self.fixed_factor.is_none() && point.shortlist > self.capacity {
                    continue;
                }
                if previous.is_some_and(|previous| point.compare(previous) != Ordering::Greater) {
                    continue;
                }
                if best.is_none_or(|best| point.compare(best) == Ordering::Less) {
                    best = Some(point);
                }
            }
        }
        Ok(best)
    }
    pub fn cushion(&self, point: Point) -> Result<Point> {
        let probes = if self.fixed_probes.is_some() {
            point.probes
        } else {
            self.next_probe(point.probes).unwrap_or(point.probes)
        };
        let factor = if self.fixed_factor.is_some() {
            point.factor
        } else {
            self.next_factor(Some(point.factor))?
                .unwrap_or(point.factor)
        };
        self.point(probes, factor)
    }
}

pub(super) fn required_hits(target: u16, k: u32) -> Result<u32> {
    if target == 0 || target > 10_000 {
        return Err(corrupt());
    }
    u32::try_from((u64::from(target) * u64::from(k)).div_ceil(10_000)).map_err(|_| corrupt())
}
/// Count an identity once, including when a test answer supplies duplicates.
pub(super) fn prefix_hits(exact: &[u64], answer: &[u64], k: usize) -> u32 {
    answer
        .iter()
        .take(k)
        .enumerate()
        .filter(|(at, row)| {
            exact.iter().take(k).any(|exact| exact == *row) && !answer[..*at].contains(row)
        })
        .count() as u32
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn the_sample_target_uses_a_checked_ceiling_and_identity_intersection() {
        assert_eq!(required_hits(9500, 10).unwrap(), 10);
        assert_eq!(required_hits(9500, 40).unwrap(), 38);
        assert_eq!(required_hits(9900, 20).unwrap(), 20);
        assert_eq!(required_hits(10_000, u32::MAX).unwrap(), u32::MAX);
        assert_eq!(prefix_hits(&[1, 2, 3, 4], &[2, 2, 4, 9], 4), 2);
        assert_eq!(prefix_hits(&[1, 2, 3, 4], &[1, 2, 3, 4], 3), 3);
    }
    #[test]
    fn grid_deduplication_and_cushion_preserve_each_axis() {
        let policy = VectorEnginePolicy::default();
        let lattice = Lattice {
            policy: &policy,
            active: 64,
            dimensions: 16,
            population: 4000,
            depth: 10,
            capacity: 8000,
            fixed_probes: None,
            fixed_factor: None,
        };
        let first = lattice.next(None, &|| Ok(())).unwrap().unwrap();
        assert_eq!((first.probes, first.factor, first.shortlist), (4, 100, 10));
        let cushion = lattice.cushion(first).unwrap();
        assert_eq!(
            (cushion.probes, cushion.factor, cushion.shortlist),
            (8, 400, 40)
        );
        let small = Lattice {
            active: 3,
            ..lattice
        };
        assert_eq!(small.next_probe(0), Some(1));
        assert_eq!(small.next_probe(1), Some(2));
        assert_eq!(small.next_probe(2), Some(3));
        assert_eq!(small.next_probe(3), None);
        let mut prior = None;
        for _ in 0..512 {
            let next = small.next(prior, &|| Ok(())).unwrap();
            let Some(next) = next else {
                return;
            };
            if let Some(prior) = prior {
                assert_eq!(next.compare(prior), Ordering::Greater);
            }
            prior = Some(next);
        }
        panic!("the finite declared grid exhausts");
    }
    #[test]
    fn capacity_is_the_exact_producer_query_quote_plus_the_real_book_allowance() {
        let policy = Policy::checked(16, 64, 4, 1, 104, true).unwrap();
        let book = contextdb_vector::ivf::codebook_allocation_layout(16, 64, false)
            .unwrap()
            .charged_bytes;
        let available = 4 * 1024 * 1024;
        let capacity = candidate_capacity(policy, 10, book, available);
        assert!(capacity >= 10);
        let at = Policy::checked(16, 64, 4, capacity, 104, true).unwrap();
        assert!(query_workspace_allocation_bytes(at, 10).unwrap() + book <= available);
        let next = Policy::checked(16, 64, 4, capacity + 1, 104, true).unwrap();
        assert!(query_workspace_allocation_bytes(next, 10).unwrap() + book > available);
        assert_eq!(candidate_capacity(policy, 10, book, book), 0);
    }
}
