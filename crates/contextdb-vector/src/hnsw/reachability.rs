//! Every point of a maintained graph stays reachable over level zero from the
//! entry point, so a walk that runs out of frontier has reached every row its
//! graph holds.
//!
//! Neighbour selection prunes links by distance and direction alone, and on
//! clustered or near-duplicate vectors it can leave whole groups of points
//! with no incoming level-zero link: rows INDEXED could never reach. So every
//! graph also holds the links of a spanning tree that is a function of point
//! identity alone, needing no state of its own:
//!
//! - within a level, point `(level, rank)` is the child of
//!   `(level, (rank - 1) / TREE_CHILDREN)`: a heap rooted at `(level, 0)`;
//! - each level's first point `(level, 0)` is the child of the first point of
//!   the next lower level that holds points;
//! - the entry point links to the first point of the lowest such level.
//!
//! From the entry point that reaches every point. A vertex owes at most
//! `TREE_CHILDREN` heap children, the next level's first point and, while it
//! is the entry, the lowest level's first point: a level-zero list holds at
//! most the construction degree plus `LAYER0_SLACK` links.
//!
//! A build appends every owed link once its insertions are done. An
//! insertion can drop owed links only where its reciprocal pruning ran, at
//! the points it links to; and it changes what is owed only at its own
//! point, that point's tree parent, and the entry point. After each
//! insertion, before it is published, exactly those points have their owed
//! links appended again.

use anndists::dist::distances::Distance;
use hnsw_rs::hnsw::{Hnsw, PointId};

/// The fork's level count.
pub(crate) const LEVELS: usize = 16;

/// The most heap children one vertex owes within its level.
pub(crate) const TREE_CHILDREN: usize = 2;

/// The most links beyond the construction degree a level-zero list holds:
/// the heap children, the next level's first point, and the entry link.
pub(crate) const LAYER0_SLACK: usize = TREE_CHILDREN + 2;

/// What the owed links can add at one point while resident: each a shared
/// point handle and a distance.
pub(crate) const OWED_LINK_BYTES_PER_POINT: usize =
    LAYER0_SLACK * (2 * std::mem::size_of::<usize>() + std::mem::size_of::<f32>());

/// The points each level holds, and the entry point: all the owed links
/// depend on.
#[derive(Clone, Copy, Debug)]
pub(crate) struct TreeShape {
    pub(crate) counts: [usize; LEVELS],
    pub(crate) entry: Option<PointId>,
}

impl TreeShape {
    fn lowest_first(&self) -> Option<PointId> {
        (0..LEVELS)
            .find(|level| self.counts[*level] != 0)
            .map(|level| PointId(level as u8, 0))
    }

    /// The tree parent of `point`; `None` for the lowest level's first point.
    pub(crate) fn parent(&self, point: PointId) -> Option<PointId> {
        let (level, rank) = (point.0 as usize, usize::try_from(point.1).ok()?);
        if rank > 0 {
            return Some(PointId(point.0, ((rank - 1) / TREE_CHILDREN) as i32));
        }
        (0..level.min(LEVELS))
            .rev()
            .find(|lower| self.counts[*lower] != 0)
            .map(|lower| PointId(lower as u8, 0))
    }

    /// The level-zero links `point` owes, into `out`.
    pub(crate) fn owed(&self, point: PointId, out: &mut Vec<PointId>) {
        out.clear();
        let Ok(rank) = usize::try_from(point.1) else {
            return;
        };
        let level = point.0 as usize;
        if level >= LEVELS {
            return;
        }
        for child in 1..=TREE_CHILDREN {
            let child = rank.saturating_mul(TREE_CHILDREN).saturating_add(child);
            if child < self.counts[level] {
                out.push(PointId(point.0, child as i32));
            }
        }
        if rank == 0
            && let Some(higher) = (level + 1..LEVELS).find(|higher| self.counts[*higher] != 0)
        {
            out.push(PointId(higher as u8, 0));
        }
        if self.entry == Some(point)
            && let Some(lowest) = self.lowest_first()
            && lowest != point
            && !out.contains(&lowest)
        {
            out.push(lowest);
        }
    }
}

/// The tree shape of a resident graph.
pub(crate) fn shape<T, D>(graph: &Hnsw<'static, T, D>) -> TreeShape
where
    T: Clone + Send + Sync,
    D: Distance<T> + Send + Sync,
{
    let indexation = graph.get_point_indexation();
    let mut counts = [0usize; LEVELS];
    for (level, count) in counts.iter_mut().enumerate() {
        *count = indexation.get_layer_nb_point(level);
    }
    TreeShape {
        counts,
        entry: indexation.entry_point_id(),
    }
}

/// One point's level-zero links, by physical identity, into `out`.
fn level_zero_links<T, D>(graph: &Hnsw<'static, T, D>, point: PointId, out: &mut Vec<PointId>)
where
    T: Clone + Send + Sync,
    D: Distance<T> + Send + Sync,
{
    out.clear();
    let Some(point) = graph.get_point_indexation().resident_point(&point) else {
        return;
    };
    let mut links = Vec::with_capacity(point.neighbour_count(0));
    point.copy_neighbour_ids(0, &mut links);
    out.extend(links.into_iter().map(|(point, _)| point));
}

/// Append every link `point` owes and does not hold; returns how many.
fn append_owed<T, D>(
    graph: &Hnsw<'static, T, D>,
    shape: &TreeShape,
    point: PointId,
    owed: &mut Vec<PointId>,
) -> usize
where
    T: Clone + Send + Sync,
    D: Distance<T> + Send + Sync,
{
    shape.owed(point, owed);
    owed.iter()
        .filter(|child| graph.append_neighbour(point, **child, 0))
        .count()
}

/// Give every point of a built graph the links it owes; returns how many
/// were appended. Runs after every insertion worker joined and before the
/// graph is published or encoded. `checkpoint` runs before each level, so
/// the pass stops with the build.
pub(crate) fn link_built<T, D>(
    graph: &Hnsw<'static, T, D>,
    checkpoint: &mut dyn FnMut() -> contextdb_core::Result<()>,
) -> contextdb_core::Result<usize>
where
    T: Clone + Send + Sync,
    D: Distance<T> + Send + Sync,
{
    let shape = shape(graph);
    let mut owed = Vec::with_capacity(LAYER0_SLACK);
    let mut appended = 0usize;
    for level in 0..LEVELS {
        checkpoint()?;
        for rank in 0..shape.counts[level] {
            appended += append_owed(graph, &shape, PointId(level as u8, rank as i32), &mut owed);
        }
    }
    Ok(appended)
}

/// After `inserted` was inserted, and before the insertion is published,
/// append the links owed by every point whose owed links the insertion could
/// have dropped or changed: the points it links to, where its reciprocal
/// pruning ran; its own point; its tree parent; and the entry point. Returns
/// how many were appended.
pub(crate) fn after_insert<T, D>(graph: &Hnsw<'static, T, D>, inserted: PointId) -> usize
where
    T: Clone + Send + Sync,
    D: Distance<T> + Send + Sync,
{
    let shape = shape(graph);
    let mut touched = Vec::new();
    level_zero_links(graph, inserted, &mut touched);
    touched.push(inserted);
    touched.extend(shape.parent(inserted));
    touched.extend(shape.entry);
    touched.sort_unstable();
    touched.dedup();
    let mut owed = Vec::with_capacity(LAYER0_SLACK);
    touched
        .iter()
        .map(|point| append_owed(graph, &shape, *point, &mut owed))
        .sum()
}

/// How many points of `graph` level zero does not reach from the entry
/// point, and how many it holds, for proofs and diagnostics.
#[cfg(any(test, feature = "test-seams"))]
pub(crate) fn unreachable_points<T, D>(graph: &Hnsw<'static, T, D>) -> (usize, usize)
where
    T: Clone + Send + Sync,
    D: Distance<T> + Send + Sync,
{
    let total = graph.get_nb_point();
    let Some(entry) = graph.get_point_indexation().entry_point_id() else {
        return (0, total);
    };
    let mut reached = std::collections::HashSet::from([entry]);
    let mut pending = vec![entry];
    let mut links = Vec::new();
    while let Some(point) = pending.pop() {
        level_zero_links(graph, point, &mut links);
        for &next in &links {
            if reached.insert(next) {
                pending.push(next);
            }
        }
    }
    (total - reached.len(), total)
}

/// How many owed links `graph` lacks, for proofs.
#[cfg(any(test, feature = "test-seams"))]
pub(crate) fn missing_owed_links<T, D>(graph: &Hnsw<'static, T, D>) -> usize
where
    T: Clone + Send + Sync,
    D: Distance<T> + Send + Sync,
{
    let shape = shape(graph);
    let mut owed = Vec::new();
    let mut links = Vec::new();
    let mut missing = 0usize;
    for level in 0..LEVELS {
        for rank in 0..shape.counts[level] {
            let point = PointId(level as u8, rank as i32);
            shape.owed(point, &mut owed);
            level_zero_links(graph, point, &mut links);
            missing += owed.iter().filter(|child| !links.contains(child)).count();
        }
    }
    missing
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shape_of(counts: &[usize], entry: PointId) -> TreeShape {
        let mut all = [0usize; LEVELS];
        all[..counts.len()].copy_from_slice(counts);
        TreeShape {
            counts: all,
            entry: Some(entry),
        }
    }

    /// Following owed links from the entry reaches every point of every
    /// level; apart from the entry link, a point is owed only by its tree
    /// parent; and no point owes more than the slack.
    #[test]
    fn owed_links_span_every_point_from_the_entry() {
        for (counts, entry) in [
            (vec![1], PointId(0, 0)),
            (vec![9, 3, 0, 2], PointId(3, 1)),
            (vec![0, 0, 5], PointId(2, 4)),
            (vec![40, 0, 7, 0, 0, 1], PointId(5, 0)),
            (vec![0, 3, 3], PointId(2, 0)),
        ] {
            let shape = shape_of(&counts, entry);
            let lowest = shape.lowest_first().unwrap();
            let mut reached = std::collections::HashSet::from([entry]);
            let mut pending = vec![entry];
            let mut owed = Vec::new();
            while let Some(point) = pending.pop() {
                shape.owed(point, &mut owed);
                assert!(
                    owed.len() <= LAYER0_SLACK,
                    "{counts:?}: {point:?} owes too many"
                );
                for &child in &owed {
                    assert!(
                        shape.parent(child) == Some(point) || (point == entry && child == lowest),
                        "{counts:?}: {point:?} owes {child:?} only as its parent"
                    );
                    if reached.insert(child) {
                        pending.push(child);
                    }
                }
            }
            assert_eq!(
                reached.len(),
                counts.iter().sum::<usize>(),
                "{counts:?} from {entry:?}: every point is reached"
            );
        }
    }
}
