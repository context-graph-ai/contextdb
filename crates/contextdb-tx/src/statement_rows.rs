//! Immutable admitted statement views of staged table rows and index order.
use crate::StagedPostingCharge;
use contextdb_core::{IndexKey, Result, RowId, VersionedRow};
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Arc;

#[derive(Debug)]
struct RowOwner {
    row: VersionedRow,
    sequence: usize,
    keys: Vec<(String, IndexKey)>,
    _charge: Arc<dyn StagedPostingCharge>,
}

pub struct PreparedStatementRow {
    pub(crate) new_table: Option<Arc<StatementRows>>,
    owner: Arc<RowOwner>,
    copy_charge: Option<Arc<dyn StagedPostingCharge>>,
}

// Boundary probes sort before or after every full key sharing their logical
// prefix. Only Row keys are stored; prefix keys are admitted query scratch.
#[derive(Debug, Clone, PartialEq, Eq)]
enum StatementIndexKey {
    PrefixStart(IndexKey),
    Row((IndexKey, RowId)),
    PrefixEnd(IndexKey),
}
impl StatementIndexKey {
    fn key(&self) -> &IndexKey {
        match self {
            Self::PrefixStart(key) | Self::PrefixEnd(key) => key,
            Self::Row((key, _)) => key,
        }
    }
    fn rank(&self) -> u8 {
        match self {
            Self::PrefixStart(_) => 0,
            Self::Row(_) => 1,
            Self::PrefixEnd(_) => 2,
        }
    }
}
impl Ord for StatementIndexKey {
    fn cmp(&self, other: &Self) -> Ordering {
        let left = self.key();
        let right = other.key();
        let common = left.len().min(right.len());
        let order = left[..common].cmp(&right[..common]);
        if !order.is_eq() {
            return order;
        }
        match left.len().cmp(&right.len()) {
            Ordering::Less => {
                if matches!(self, Self::PrefixEnd(_)) {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
            Ordering::Greater => {
                if matches!(other, Self::PrefixEnd(_)) {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }
            Ordering::Equal => self
                .rank()
                .cmp(&other.rank())
                .then_with(|| match (self, other) {
                    (Self::Row((_, left)), Self::Row((_, right))) => left.cmp(right),
                    _ => Ordering::Equal,
                }),
        }
    }
}
impl PartialOrd for StatementIndexKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone, Default)]
pub struct StatementRows {
    rows: BTreeMap<RowId, Arc<RowOwner>>,
    sequence: BTreeMap<usize, RowId>,
    indexes: BTreeMap<String, BTreeMap<StatementIndexKey, RowId>>,
    next_sequence: usize,
    copy_charge: Option<Arc<dyn StagedPostingCharge>>,
    control_charge: Option<Arc<dyn StagedPostingCharge>>,
}
impl StatementRows {
    pub(crate) fn with_control(charge: Arc<dyn StagedPostingCharge>) -> Self {
        Self {
            control_charge: Some(charge),
            ..Self::default()
        }
    }

    pub fn retained_bytes(&self) -> usize {
        self.rows
            .values()
            .map(|owner| owner._charge.retained_bytes())
            .sum::<usize>()
            .saturating_add(
                self.copy_charge
                    .as_ref()
                    .map_or(0, |charge| charge.retained_bytes()),
            )
            .saturating_add(
                self.control_charge
                    .as_ref()
                    .map_or(0, |charge| charge.retained_bytes()),
            )
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
    pub fn contains(&self, row: &RowId) -> bool {
        self.rows.contains_key(row)
    }
    pub fn len(&self) -> usize {
        self.rows.len()
    }
    pub fn rows(&self) -> impl Iterator<Item = &VersionedRow> {
        self.sequence
            .values()
            .filter_map(|id| self.rows.get(id).map(|owner| &owner.row))
    }
    pub fn next_unordered(&self, after: Option<usize>) -> Option<(usize, &VersionedRow)> {
        let lower = after.map_or(Bound::Unbounded, Bound::Excluded);
        let (sequence, row) = self.sequence.range((lower, Bound::Unbounded)).next()?;
        Some((*sequence, &self.rows.get(row)?.row))
    }
    pub fn has_index(&self, index: &str) -> bool {
        self.indexes
            .get(index)
            .is_some_and(|postings| postings.len() == self.rows.len())
    }
    pub fn next_ordered(
        &self,
        index: &str,
        reverse: bool,
        after: Option<&(IndexKey, RowId)>,
    ) -> Option<(&(IndexKey, RowId), &VersionedRow)> {
        self.next_ordered_in_run(
            index,
            reverse,
            after,
            &Bound::Unbounded,
            &Bound::Unbounded,
            0,
        )
    }
    /// Seek the same logical run as the committed cursor. The caller admits
    /// endpoint copies before calling; no neighboring prefix is inspected.
    #[allow(clippy::too_many_arguments)]
    pub fn next_ordered_in_run(
        &self,
        index: &str,
        reverse: bool,
        after: Option<&(IndexKey, RowId)>,
        start: &Bound<IndexKey>,
        end: &Bound<IndexKey>,
        prefix_len: usize,
    ) -> Option<(&(IndexKey, RowId), &VersionedRow)> {
        let postings = self.indexes.get(index)?;
        let endpoint = |bound: &Bound<IndexKey>, lower: bool| match bound {
            Bound::Unbounded => Bound::Unbounded,
            Bound::Included(key) | Bound::Excluded(key) => {
                let key = key[..prefix_len.min(key.len())].to_vec();
                let before = matches!(bound, Bound::Included(_)) == lower;
                Bound::Included(if before {
                    StatementIndexKey::PrefixStart(key)
                } else {
                    StatementIndexKey::PrefixEnd(key)
                })
            }
        };
        let mut lower = endpoint(start, true);
        let mut upper = endpoint(end, false);
        if let Some(after) = after {
            let after = Bound::Excluded(StatementIndexKey::Row(after.clone()));
            if reverse {
                upper = after;
            } else {
                lower = after;
            }
        }
        fn key(bound: &Bound<StatementIndexKey>) -> Option<&StatementIndexKey> {
            match bound {
                Bound::Included(key) | Bound::Excluded(key) => Some(key),
                Bound::Unbounded => None,
            }
        }
        if let (Some(lower), Some(upper)) = (key(&lower), key(&upper))
            && lower >= upper
        {
            return None;
        }
        let mut range = postings.range((lower, upper));
        let (position, row) = if reverse {
            range.next_back()
        } else {
            range.next()
        }?;
        let StatementIndexKey::Row(position) = position else {
            unreachable!("only row keys are stored")
        };
        Some((position, &self.rows.get(row)?.row))
    }
    fn control_bytes(&self) -> usize {
        self.rows.values().fold(256usize, |bytes, owner| {
            owner
                .keys
                .iter()
                .fold(bytes.saturating_add(192), |bytes, (name, key)| {
                    bytes
                        .saturating_add(256)
                        .saturating_add(name.len())
                        .saturating_add(
                            key.iter()
                                .map(|v| match v {
                                    contextdb_core::DirectedValue::Asc(v) => v.0.estimated_bytes(),
                                    contextdb_core::DirectedValue::Desc(v) => v.0.estimated_bytes(),
                                })
                                .sum::<usize>(),
                        )
                })
        })
    }
    fn make_mut<'a>(
        snapshot: &'a mut Arc<Self>,
        reserve: &mut impl FnMut(usize) -> Result<Arc<dyn StagedPostingCharge>>,
    ) -> Result<&'a mut Self> {
        if Arc::strong_count(snapshot) != 1 {
            let charge = reserve(snapshot.control_bytes())?;
            let mut next = (**snapshot).clone();
            next.copy_charge = Some(charge);
            *snapshot = Arc::new(next);
        }
        Ok(Arc::get_mut(snapshot).expect("private statement row owner"))
    }
    pub(crate) fn prepare(
        snapshot: &Arc<Self>,
        row: &VersionedRow,
        keys: &[(String, IndexKey)],
        mut reserve: impl FnMut(usize) -> Result<Arc<dyn StagedPostingCharge>>,
    ) -> Result<PreparedStatementRow> {
        let bytes = row.estimated_bytes().saturating_add(256).saturating_add(
            keys.iter()
                .map(|(name, key)| {
                    name.len().saturating_add(256).saturating_add(
                        key.iter()
                            .map(|v| match v {
                                contextdb_core::DirectedValue::Asc(v) => {
                                    v.0.estimated_bytes().saturating_mul(3)
                                }
                                contextdb_core::DirectedValue::Desc(v) => {
                                    v.0.estimated_bytes().saturating_mul(3)
                                }
                            })
                            .sum::<usize>(),
                    )
                })
                .sum::<usize>(),
        );
        let charge = reserve(bytes)?;
        let copy_charge = if Arc::strong_count(snapshot) > 1 {
            Some(reserve(snapshot.control_bytes())?)
        } else {
            None
        };
        Ok(PreparedStatementRow {
            new_table: None,
            owner: Arc::new(RowOwner {
                row: row.clone(),
                sequence: snapshot.next_sequence,
                keys: keys.to_vec(),
                _charge: charge,
            }),
            copy_charge,
        })
    }

    pub(crate) fn publish(snapshot: &mut Arc<Self>, prepared: PreparedStatementRow) {
        if let Some(charge) = prepared.copy_charge {
            let mut next = (**snapshot).clone();
            next.copy_charge = Some(charge);
            *snapshot = Arc::new(next);
        }
        let rows = Arc::get_mut(snapshot)
            .expect("statement mutation was prepared under the write-set mutex");
        let owner = prepared.owner;
        let row_id = owner.row.row_id;
        rows.remove_private(row_id);
        rows.next_sequence = owner
            .sequence
            .checked_add(1)
            .expect("staged sequence fits usize");
        for (name, key) in &owner.keys {
            rows.indexes
                .entry(name.clone())
                .or_default()
                .insert(StatementIndexKey::Row((key.clone(), row_id)), row_id);
        }
        rows.sequence.insert(owner.sequence, row_id);
        rows.rows.insert(row_id, owner);
    }
    fn remove_private(&mut self, row: RowId) {
        if let Some(owner) = self.rows.remove(&row) {
            self.sequence.remove(&owner.sequence);
            for (name, key) in &owner.keys {
                if let Some(index) = self.indexes.get_mut(name) {
                    index.remove(&StatementIndexKey::Row((key.clone(), row)));
                }
            }
        }
    }
    pub(crate) fn remove(snapshot: &mut Arc<Self>, row: RowId) -> Result<()> {
        let Some(owner) = snapshot.rows.get(&row) else {
            return Ok(());
        };
        let charge = owner._charge.clone();
        Self::make_mut(snapshot, &mut |bytes| charge.reserve_copy(bytes))?.remove_private(row);
        Ok(())
    }
}
