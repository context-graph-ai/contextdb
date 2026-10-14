//! A height-balanced search tree over the positions of an append-only list.
//!
//! Node `i` is list position `i`, so a node is two child positions and a
//! height and never a copy of its key: the caller orders positions by reading
//! its own list. Finding, inserting and bounded seeks each descend once,
//! O(log n) comparisons; inserting rebalances on the way back up the path the
//! descent recorded.

use std::cmp::Ordering;

/// The absent child; a list never holds `u32::MAX` positions.
pub(in crate::persistence) const NO_POSITION: u32 = u32::MAX;
/// A height-balanced tree of fewer than `u32::MAX` nodes is under
/// 1.45 log2(n + 2) high, so 64 entries always hold a root-to-leaf path.
const MAX_DEPTH: usize = 64;

#[derive(Clone, Copy)]
pub(in crate::persistence) struct PositionNode {
    left: u32,
    right: u32,
    height: u8,
}

pub(in crate::persistence) struct PositionTree {
    nodes: Vec<PositionNode>,
    root: u32,
}

/// Where a position that was not found would attach: the path from the root,
/// with the direction taken at each node.
pub(in crate::persistence) struct Vacancy {
    path: [(u32, bool); MAX_DEPTH],
    depth: usize,
}

impl PositionTree {
    /// Bytes one node occupies in the node vector.
    pub(in crate::persistence) const NODE_BYTES: usize = std::mem::size_of::<PositionNode>();

    pub(in crate::persistence) const fn new() -> Self {
        Self {
            nodes: Vec::new(),
            root: NO_POSITION,
        }
    }

    pub(in crate::persistence) fn len(&self) -> usize {
        self.nodes.len()
    }

    pub(in crate::persistence) fn capacity(&self) -> usize {
        self.nodes.capacity()
    }

    /// Move the nodes into a vector of `capacity` (which the caller admitted)
    /// and free the old one. Returns how many nodes were relocated.
    pub(in crate::persistence) fn grow(&mut self, capacity: usize) -> usize {
        debug_assert!(capacity > self.nodes.capacity());
        let mut next = Vec::with_capacity(capacity);
        next.extend_from_slice(&self.nodes);
        let moved = self.nodes.len();
        self.nodes = next;
        moved
    }

    /// Free the node vector.
    pub(in crate::persistence) fn clear(&mut self) {
        *self = Self::new();
    }

    pub(in crate::persistence) fn height(&self) -> u8 {
        self.height_of(self.root)
    }

    fn height_of(&self, node: u32) -> u8 {
        if node == NO_POSITION {
            0
        } else {
            self.nodes[node as usize].height
        }
    }

    fn update_height(&mut self, node: u32) {
        let PositionNode { left, right, .. } = self.nodes[node as usize];
        self.nodes[node as usize].height = 1 + self.height_of(left).max(self.height_of(right));
    }

    fn rotate_right(&mut self, node: u32) -> u32 {
        let pivot = self.nodes[node as usize].left;
        self.nodes[node as usize].left = self.nodes[pivot as usize].right;
        self.nodes[pivot as usize].right = node;
        self.update_height(node);
        self.update_height(pivot);
        pivot
    }

    fn rotate_left(&mut self, node: u32) -> u32 {
        let pivot = self.nodes[node as usize].right;
        self.nodes[node as usize].right = self.nodes[pivot as usize].left;
        self.nodes[pivot as usize].left = node;
        self.update_height(node);
        self.update_height(pivot);
        pivot
    }

    /// The root of `node`'s subtree once its heights differ by at most one.
    fn rebalance(&mut self, node: u32) -> u32 {
        self.update_height(node);
        let PositionNode { left, right, .. } = self.nodes[node as usize];
        if self.height_of(left) > self.height_of(right) + 1 {
            let inner = self.nodes[left as usize];
            if self.height_of(inner.left) < self.height_of(inner.right) {
                self.nodes[node as usize].left = self.rotate_left(left);
            }
            return self.rotate_right(node);
        }
        if self.height_of(right) > self.height_of(left) + 1 {
            let inner = self.nodes[right as usize];
            if self.height_of(inner.right) < self.height_of(inner.left) {
                self.nodes[node as usize].right = self.rotate_right(right);
            }
            return self.rotate_left(node);
        }
        node
    }

    /// Descend once. `order(position)` places the sought key relative to that
    /// position: `Less` goes left, `Greater` goes right, `Equal` is found.
    pub(in crate::persistence) fn search(
        &self,
        mut order: impl FnMut(u32) -> Ordering,
    ) -> Result<u32, Vacancy> {
        let mut vacancy = Vacancy {
            path: [(NO_POSITION, false); MAX_DEPTH],
            depth: 0,
        };
        let mut node = self.root;
        while node != NO_POSITION {
            let left = match order(node) {
                Ordering::Equal => return Ok(node),
                Ordering::Less => true,
                Ordering::Greater => false,
            };
            vacancy.path[vacancy.depth] = (node, left);
            vacancy.depth += 1;
            let entry = self.nodes[node as usize];
            node = if left { entry.left } else { entry.right };
        }
        Err(vacancy)
    }

    /// Attach `position`, the list's next position, where `vacancy` says, and
    /// rebalance up its path. The node vector must have room (its growth is
    /// the caller's to admit). Returns the rebalancing steps taken.
    pub(in crate::persistence) fn insert_at(&mut self, vacancy: Vacancy, position: u32) -> usize {
        debug_assert_eq!(position as usize, self.nodes.len());
        debug_assert!(self.nodes.len() < self.nodes.capacity());
        self.nodes.push(PositionNode {
            left: NO_POSITION,
            right: NO_POSITION,
            height: 1,
        });
        let Vacancy { path, mut depth } = vacancy;
        let steps = depth;
        let mut subtree = position;
        while let Some(parent) = depth.checked_sub(1) {
            depth = parent;
            let (node, left) = path[depth];
            if left {
                self.nodes[node as usize].left = subtree;
            } else {
                self.nodes[node as usize].right = subtree;
            }
            subtree = self.rebalance(node);
        }
        self.root = subtree;
        steps
    }

    /// The last position in tree order for which `holds` is true, when it is
    /// true for a prefix of that order.
    pub(in crate::persistence) fn last_where(
        &self,
        mut holds: impl FnMut(u32) -> bool,
    ) -> Option<u32> {
        let mut found = None;
        let mut node = self.root;
        while node != NO_POSITION {
            let entry = self.nodes[node as usize];
            if holds(node) {
                found = Some(node);
                node = entry.right;
            } else {
                node = entry.left;
            }
        }
        found
    }

    /// The first position in tree order for which `holds` is true, when it is
    /// true for a suffix of that order.
    pub(in crate::persistence) fn first_where(
        &self,
        mut holds: impl FnMut(u32) -> bool,
    ) -> Option<u32> {
        let mut found = None;
        let mut node = self.root;
        while node != NO_POSITION {
            let entry = self.nodes[node as usize];
            if holds(node) {
                found = Some(node);
                node = entry.left;
            } else {
                node = entry.right;
            }
        }
        found
    }
}
