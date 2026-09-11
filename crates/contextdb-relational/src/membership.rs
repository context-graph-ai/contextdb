//! Immutable current index membership. Updates copy only an AVL search path;
//! readers keep an owned root and never visit retired postings.
use crate::store::IndexEntry;
use contextdb_core::read_memory::{ReadCredit, ReadMemoryBudget};
use contextdb_core::{DirectedValue, IndexKey, Result, RowId, SnapshotId, TxId};
use std::cmp::Ordering;
use std::ops::Bound;
use std::sync::Arc;

type Link = Option<Arc<Node>>;
type Budget = Option<Arc<dyn ReadMemoryBudget>>;
const HINT: &str = "Release old readers or raise MEMORY_LIMIT before changing indexed rows.";

#[derive(Debug)]
struct Key {
    values: IndexKey,
    _credit: Option<ReadCredit>,
}

#[derive(Debug)]
struct Node {
    key: Arc<Key>,
    entry: IndexEntry,
    left: Link,
    right: Link,
    height: u16,
    _credit: Option<ReadCredit>,
}

fn credit(budget: &Budget, bytes: usize) -> Result<Option<ReadCredit>> {
    budget
        .as_ref()
        .map(|budget| {
            ReadCredit::try_new(
                budget.clone(),
                bytes,
                "relational_index",
                "membership",
                HINT,
            )
        })
        .transpose()
}

fn height(link: &Link) -> u16 {
    link.as_ref().map_or(0, |n| n.height)
}

fn node(
    key: Arc<Key>,
    entry: IndexEntry,
    left: Link,
    right: Link,
    budget: &Budget,
) -> Result<Arc<Node>> {
    let charge = credit(
        budget,
        std::mem::size_of::<Node>() + 2 * std::mem::size_of::<usize>(),
    )?;
    Ok(Arc::new(Node {
        key,
        entry,
        height: 1 + height(&left).max(height(&right)),
        left,
        right,
        _credit: charge,
    }))
}

fn balance(
    key: Arc<Key>,
    entry: IndexEntry,
    left: Link,
    right: Link,
    budget: &Budget,
) -> Result<Arc<Node>> {
    if height(&left) > height(&right) + 1 {
        let l = left.as_ref().expect("left-heavy node");
        if height(&l.left) >= height(&l.right) {
            let r = node(key, entry, l.right.clone(), right, budget)?;
            return node(
                l.key.clone(),
                l.entry.clone(),
                l.left.clone(),
                Some(r),
                budget,
            );
        }
        let m = l.right.as_ref().expect("left-right node");
        let a = node(
            l.key.clone(),
            l.entry.clone(),
            l.left.clone(),
            m.left.clone(),
            budget,
        )?;
        let b = node(key, entry, m.right.clone(), right, budget)?;
        return node(m.key.clone(), m.entry.clone(), Some(a), Some(b), budget);
    }
    if height(&right) > height(&left) + 1 {
        let r = right.as_ref().expect("right-heavy node");
        if height(&r.right) >= height(&r.left) {
            let l = node(key, entry, left, r.left.clone(), budget)?;
            return node(
                r.key.clone(),
                r.entry.clone(),
                Some(l),
                r.right.clone(),
                budget,
            );
        }
        let m = r.left.as_ref().expect("right-left node");
        let a = node(key, entry, left, m.left.clone(), budget)?;
        let b = node(
            r.key.clone(),
            r.entry.clone(),
            m.right.clone(),
            r.right.clone(),
            budget,
        )?;
        return node(m.key.clone(), m.entry.clone(), Some(a), Some(b), budget);
    }
    node(key, entry, left, right, budget)
}

fn insert(root: &Link, key: Arc<Key>, entry: IndexEntry, budget: &Budget) -> Result<Arc<Node>> {
    let Some(n) = root else {
        return node(key, entry, None, None, budget);
    };
    match (&key.values, entry.row_id).cmp(&(&n.key.values, n.entry.row_id)) {
        Ordering::Less => balance(
            n.key.clone(),
            n.entry.clone(),
            Some(insert(&n.left, key, entry, budget)?),
            n.right.clone(),
            budget,
        ),
        Ordering::Greater => balance(
            n.key.clone(),
            n.entry.clone(),
            n.left.clone(),
            Some(insert(&n.right, key, entry, budget)?),
            budget,
        ),
        Ordering::Equal => node(
            n.key.clone(),
            entry,
            n.left.clone(),
            n.right.clone(),
            budget,
        ),
    }
}

fn remove(root: &Link, key: &IndexKey, row: RowId, budget: &Budget) -> Result<Link> {
    let Some(n) = root else { return Ok(None) };
    Ok(Some(
        match (key, row).cmp(&(&n.key.values, n.entry.row_id)) {
            Ordering::Less => balance(
                n.key.clone(),
                n.entry.clone(),
                remove(&n.left, key, row, budget)?,
                n.right.clone(),
                budget,
            )?,
            Ordering::Greater => balance(
                n.key.clone(),
                n.entry.clone(),
                n.left.clone(),
                remove(&n.right, key, row, budget)?,
                budget,
            )?,
            Ordering::Equal => {
                if n.left.is_none() {
                    return Ok(n.right.clone());
                }
                if n.right.is_none() {
                    return Ok(n.left.clone());
                }
                let mut successor = n.right.as_deref().expect("right child");
                while let Some(left) = successor.left.as_deref() {
                    successor = left;
                }
                balance(
                    successor.key.clone(),
                    successor.entry.clone(),
                    n.left.clone(),
                    remove(
                        &n.right,
                        &successor.key.values,
                        successor.entry.row_id,
                        budget,
                    )?,
                    budget,
                )?
            }
        },
    ))
}

#[derive(Debug, Clone, Default)]
pub struct MembershipImage {
    root: Link,
    /// The image answers snapshots at or after every mutation it includes.
    since: TxId,
    budget: Budget,
}

impl MembershipImage {
    pub(crate) fn with_budget(budget: Arc<dyn ReadMemoryBudget>) -> Self {
        Self {
            budget: Some(budget),
            ..Self::default()
        }
    }

    pub(crate) fn since(&self) -> TxId {
        self.since
    }

    pub fn visible_at(&self, snapshot: SnapshotId) -> bool {
        snapshot.0 >= self.since.0
    }

    pub fn insert(&mut self, key: &IndexKey, entry: IndexEntry) -> Result<()> {
        let since = self
            .since
            .max(entry.created_tx)
            .max(entry.deleted_tx.unwrap_or_default());
        if entry.deleted_tx.is_some() {
            self.since = since;
            return Ok(());
        }
        let bytes = key.iter().fold(
            std::mem::size_of::<Key>()
                + 2 * std::mem::size_of::<usize>()
                + key.len() * std::mem::size_of::<DirectedValue>(),
            |bytes, value| {
                let value = match value {
                    DirectedValue::Asc(v) => &v.0,
                    DirectedValue::Desc(v) => &v.0,
                };
                bytes.saturating_add(value.estimated_bytes())
            },
        );
        let charge = credit(&self.budget, bytes)?;
        let key = Arc::new(Key {
            values: key.clone(),
            _credit: charge,
        });
        self.root = Some(insert(&self.root, key, entry, &self.budget)?);
        self.since = since;
        Ok(())
    }

    pub fn remove(&mut self, key: &IndexKey, row: RowId, tx: TxId) -> Result<()> {
        let root = remove(&self.root, key, row, &self.budget)?;
        self.root = root;
        self.since = self.since.max(tx);
        Ok(())
    }

    /// Admit an image built by a detached loader before it is published.
    pub fn admit(&mut self, budget: Arc<dyn ReadMemoryBudget>) -> Result<()> {
        if self.budget.is_some() {
            return Ok(());
        }
        let mut admitted = Self {
            root: None,
            since: self.since,
            budget: Some(budget),
        };
        for (key, entry) in self.range(Bound::Unbounded, Bound::Unbounded) {
            admitted.insert(key, entry.clone())?;
        }
        *self = admitted;
        Ok(())
    }

    /// No iterator stack or posting-list clone: a continuation seeks by the
    /// last complete (key, row) identity in logarithmic work.
    pub fn next<'a>(
        &'a self,
        after: Option<(&[DirectedValue], RowId)>,
        start: Bound<&[DirectedValue]>,
        reverse: bool,
    ) -> Option<(&'a IndexKey, &'a IndexEntry)> {
        let mut node = self.root.as_deref();
        let mut candidate = None;
        while let Some(n) = node {
            let key = n.key.values.as_slice();
            let eligible = if let Some((last, row)) = after {
                let order = (key, n.entry.row_id).cmp(&(last, row));
                if reverse {
                    order.is_lt()
                } else {
                    order.is_gt()
                }
            } else {
                match start {
                    Bound::Unbounded => true,
                    Bound::Included(edge) => {
                        if reverse {
                            key <= edge
                        } else {
                            key >= edge
                        }
                    }
                    Bound::Excluded(edge) => {
                        if reverse {
                            key < edge
                        } else {
                            key > edge
                        }
                    }
                }
            };
            if eligible {
                candidate = Some((&n.key.values, &n.entry));
                node = if reverse {
                    n.right.as_deref()
                } else {
                    n.left.as_deref()
                };
            } else {
                node = if reverse {
                    n.left.as_deref()
                } else {
                    n.right.as_deref()
                };
            }
        }
        candidate
    }

    pub fn range<'a>(
        &'a self,
        lower: Bound<&'a [DirectedValue]>,
        upper: Bound<&'a [DirectedValue]>,
    ) -> MembershipRange<'a> {
        MembershipRange {
            image: self,
            lower,
            upper,
            after: None,
            done: false,
        }
    }
}

pub struct MembershipRange<'a> {
    image: &'a MembershipImage,
    lower: Bound<&'a [DirectedValue]>,
    upper: Bound<&'a [DirectedValue]>,
    after: Option<(&'a [DirectedValue], RowId)>,
    done: bool,
}

impl<'a> Iterator for MembershipRange<'a> {
    type Item = (&'a IndexKey, &'a IndexEntry);
    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        let (key, entry) = self.image.next(self.after, self.lower, false)?;
        let inside = match self.upper {
            Bound::Unbounded => true,
            Bound::Included(edge) => key.as_slice() <= edge,
            Bound::Excluded(edge) => key.as_slice() < edge,
        };
        if !inside {
            self.done = true;
            return None;
        }
        self.after = Some((key.as_slice(), entry.row_id));
        Some((key, entry))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use contextdb_core::{Error, TotalOrdAsc, Value};
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    #[derive(Debug)]
    struct Meter {
        used: AtomicUsize,
        limit: AtomicUsize,
        allocations: AtomicUsize,
    }
    impl Meter {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                used: AtomicUsize::new(0),
                limit: AtomicUsize::new(usize::MAX),
                allocations: AtomicUsize::new(0),
            })
        }
        fn used(&self) -> usize {
            self.used.load(AtomicOrdering::SeqCst)
        }
    }
    impl ReadMemoryBudget for Meter {
        fn try_reserve(
            &self,
            bytes: usize,
            subsystem: &'static str,
            operation: &'static str,
            hint: &'static str,
        ) -> Result<()> {
            let limit = self.limit.load(AtomicOrdering::SeqCst);
            if bytes > limit.saturating_sub(self.used()) {
                return Err(Error::MemoryBudgetExceeded {
                    subsystem: subsystem.to_owned(),
                    operation: operation.to_owned(),
                    requested_bytes: bytes,
                    available_bytes: limit.saturating_sub(self.used()),
                    budget_limit_bytes: limit,
                    hint: hint.to_owned(),
                });
            }
            self.used.fetch_add(bytes, AtomicOrdering::SeqCst);
            self.allocations.fetch_add(1, AtomicOrdering::SeqCst);
            Ok(())
        }
        fn release(&self, bytes: usize) {
            assert!(
                self.used.fetch_sub(bytes, AtomicOrdering::SeqCst) >= bytes,
                "every allocation releases once"
            );
        }
    }
    fn key(value: i64) -> IndexKey {
        vec![DirectedValue::Asc(TotalOrdAsc(Value::Int64(value)))]
    }
    fn entry(id: u64, tx: TxId) -> IndexEntry {
        IndexEntry {
            row_id: RowId(id),
            created_tx: tx,
            deleted_tx: None,
        }
    }
    fn verify(link: &Link) -> (u16, usize) {
        let Some(n) = link else { return (0, 0) };
        let (left, a) = verify(&n.left);
        let (right, b) = verify(&n.right);
        assert!(left.abs_diff(right) <= 1);
        assert_eq!(n.height, 1 + left.max(right));
        (n.height, a + b + 1)
    }

    #[test]
    fn persistent_membership_paths_balance_and_release_when_the_last_reader_drops() {
        let meter = Meter::new();
        let mut image = MembershipImage::default();
        image.admit(meter.clone()).unwrap();
        let mut expected = BTreeMap::new();
        for id in 0..512 {
            let k = key((id * 73 % 127) as i64);
            image.insert(&k, entry(id, TxId(1))).unwrap();
            expected.insert((k, RowId(id)), TxId(1));
        }
        let old = image.clone();
        for id in 0..512 {
            let k = key((id * 73 % 127) as i64);
            let before = meter.allocations.load(AtomicOrdering::SeqCst);
            let h = height(&image.root) as usize;
            image.remove(&k, RowId(id), TxId(2)).unwrap();
            expected.remove(&(k.clone(), RowId(id)));
            if id % 2 == 0 {
                image.insert(&k, entry(id, TxId(2))).unwrap();
                expected.insert((k, RowId(id)), TxId(2));
            }
            assert!(
                meter.allocations.load(AtomicOrdering::SeqCst) - before <= 8 * (h + 1),
                "one edit copies search paths, never the complete index"
            );
            verify(&image.root);
            assert_eq!(
                image
                    .range(Bound::Unbounded, Bound::Unbounded)
                    .map(|(key, entry)| ((key.clone(), entry.row_id), entry.created_tx))
                    .collect::<BTreeMap<_, _>>(),
                expected
            );
        }
        assert_eq!(old.range(Bound::Unbounded, Bound::Unbounded).count(), 512);
        assert!(
            old.range(Bound::Unbounded, Bound::Unbounded)
                .all(|(_, entry)| entry.created_tx == TxId(1))
        );
        assert_eq!(verify(&image.root).1, 256);
        drop(image);
        assert!(meter.used() > 0, "the captured image still owns its nodes");
        drop(old);
        assert_eq!(meter.used(), 0);
    }

    #[test]
    fn bounded_seeks_preserve_duplicate_keys_direction_and_exclusive_edges() {
        let mut image = MembershipImage::default();
        for id in (0..20).rev() {
            image
                .insert(&key((id / 2) as i64), entry(id, TxId(1)))
                .unwrap();
        }
        let lower = key(3);
        let upper = key(6);
        assert_eq!(
            image
                .range(Bound::Excluded(&lower), Bound::Included(&upper))
                .map(|(_, entry)| entry.row_id.0)
                .collect::<Vec<_>>(),
            (8..14).collect::<Vec<_>>()
        );
        let mut after = None;
        let mut rows = Vec::new();
        while let Some((key, entry)) = image.next(after, Bound::Excluded(&upper), true) {
            if key < &lower {
                break;
            }
            rows.push(entry.row_id.0);
            after = Some((key.as_slice(), entry.row_id));
        }
        assert_eq!(rows, (6..12).rev().collect::<Vec<_>>());
        assert!(
            image
                .next(None, Bound::Excluded(key(100).as_slice()), false)
                .is_none()
        );
    }

    #[test]
    fn refused_path_copy_leaves_identity_visibility_and_charge_unchanged() {
        let meter = Meter::new();
        let mut image = MembershipImage::default();
        image.admit(meter.clone()).unwrap();
        for id in 0..64 {
            image.insert(&key(id as i64), entry(id, TxId(1))).unwrap();
        }
        let before = meter.used();
        let old = image.clone();
        meter.limit.store(before, AtomicOrdering::SeqCst);
        let large_key = vec![DirectedValue::Asc(TotalOrdAsc(Value::Text(
            "x".repeat(64 * 1024),
        )))];
        assert!(matches!(
            image.insert(&large_key, entry(100, TxId(2))),
            Err(Error::MemoryBudgetExceeded { .. })
        ));
        assert!(matches!(
            image.remove(&key(20), RowId(20), TxId(2)),
            Err(Error::MemoryBudgetExceeded { .. })
        ));
        assert_eq!(image.since, TxId(1));
        assert!(Arc::ptr_eq(
            image.root.as_ref().unwrap(),
            old.root.as_ref().unwrap()
        ));
        assert_eq!(meter.used(), before);
        drop(old);
        drop(image);
        assert_eq!(meter.used(), 0);
    }
}
