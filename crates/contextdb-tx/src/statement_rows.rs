//! Immutable admitted statement views of staged table rows and index order.
use crate::StagedPostingCharge;
use contextdb_core::{IndexKey, Result, RowId, VersionedRow};
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::{Arc, Weak};

#[derive(Debug)]
pub(crate) struct RowOwner {
    pub(crate) row: VersionedRow,
    sequence: usize,
    pub(crate) keys: Vec<(String, IndexKey)>,
    pub(crate) schema: Arc<crate::ordered_staged::StagedSchema>,
    _charge: Arc<dyn StagedPostingCharge>,
}

pub struct PreparedStatementRow {
    pub(crate) new_table: Option<Arc<StatementRows>>,
    owner: Arc<RowOwner>,
    copy_charge: Option<Arc<dyn StagedPostingCharge>>,
    undo: Option<StatementRowUndo>,
}

#[derive(Debug)]
pub(crate) struct StatementRowUndo {
    prior_image: Weak<StatementRows>,
    prior_row: Option<Arc<RowOwner>>,
    prior_sequence: usize,
    prior_copy_charge: Option<Arc<dyn StagedPostingCharge>>,
    domains: Vec<(String, Option<StatementIndex>)>,
    schemas: Vec<(String, Arc<crate::ordered_staged::StagedSchema>)>,
    _charge: Arc<dyn StagedPostingCharge>,
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

pub struct PreparedStatementRemoval {
    replacement: Option<Arc<StatementRows>>,
}
pub(crate) struct PreparedStatementDelete {
    copy_charge: Option<Arc<dyn StagedPostingCharge>>,
    undo: StatementRowUndo,
}
#[derive(Debug, Clone)]
struct StatementIndex {
    schema: Arc<crate::ordered_staged::StagedSchema>,
    postings: BTreeMap<StatementIndexKey, RowId>,
}
#[derive(Debug, Clone, Default)]
pub struct StatementRows {
    rows: BTreeMap<RowId, Arc<RowOwner>>,
    sequence: BTreeMap<usize, RowId>,
    indexes: BTreeMap<String, StatementIndex>,
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
        // Each staged mutation owns one schema; index controls may outlive
        // that row and share its reservation. Count those owners once without
        // allocating scratch in this admission estimator.
        let mut schema_bytes = self
            .rows
            .values()
            .map(|owner| owner.schema.retained_bytes())
            .sum::<usize>();
        for (position, index) in self.indexes.values().enumerate() {
            if !self
                .rows
                .values()
                .any(|owner| Arc::ptr_eq(&owner.schema, &index.schema))
                && !self
                    .indexes
                    .values()
                    .take(position)
                    .any(|prior| Arc::ptr_eq(&prior.schema, &index.schema))
            {
                schema_bytes = schema_bytes.saturating_add(index.schema.retained_bytes());
            }
        }
        self.rows
            .values()
            .map(|owner| owner._charge.retained_bytes())
            .sum::<usize>()
            .saturating_add(schema_bytes)
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

    pub(crate) fn owner(&self, row: RowId) -> Option<Arc<RowOwner>> {
        self.rows.get(&row).cloned()
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
    pub fn has_index_for(&self, index: &str, meta: &contextdb_core::TableMeta) -> bool {
        self.indexes.get(index).is_some_and(|postings| {
            postings.postings.len() == self.rows.len() && postings.schema.matches_meta(meta, index)
        })
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
        let postings = &self.indexes.get(index)?.postings;
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
                        .saturating_add(crate::ordered_staged::staged_key_bytes(key))
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
        schema: Arc<crate::ordered_staged::StagedSchema>,
        mut reserve: impl FnMut(usize) -> Result<Arc<dyn StagedPostingCharge>>,
        retain_undo: bool,
    ) -> Result<PreparedStatementRow> {
        let bytes = row.estimated_bytes().saturating_add(256).saturating_add(
            keys.iter()
                .map(|(name, key)| {
                    name.len().saturating_add(256).saturating_add(
                        crate::ordered_staged::staged_key_bytes(key).saturating_mul(2),
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
        let undo_charge = if retain_undo {
            Some(reserve(snapshot.undo_control_bytes(row.row_id, keys))?)
        } else {
            None
        };
        let undo = undo_charge.map(|charge| StatementRowUndo {
            _charge: charge,
            prior_image: if copy_charge.is_some() {
                Arc::downgrade(snapshot)
            } else {
                Weak::new()
            },
            prior_row: snapshot.rows.get(&row.row_id).cloned(),
            prior_sequence: snapshot.next_sequence,
            prior_copy_charge: snapshot.copy_charge.clone(),
            domains: Vec::with_capacity(
                keys.len().saturating_add(
                    snapshot
                        .rows
                        .get(&row.row_id)
                        .map_or(0, |owner| owner.keys.len()),
                ),
            ),
            schemas: Vec::with_capacity(snapshot.indexes.len()),
        });
        Ok(PreparedStatementRow {
            undo,
            new_table: None,
            owner: Arc::new(RowOwner {
                row: row.clone(),
                sequence: snapshot.next_sequence,
                keys: keys.to_vec(),
                schema,
                _charge: charge,
            }),
            copy_charge,
        })
    }

    pub(crate) fn publish(
        snapshot: &mut Arc<Self>,
        mut prepared: PreparedStatementRow,
    ) -> Option<StatementRowUndo> {
        if let Some(charge) = prepared.copy_charge {
            let mut next = (**snapshot).clone();
            next.copy_charge = Some(charge);
            *snapshot = Arc::new(next);
        }
        let rows = Arc::get_mut(snapshot)
            .expect("statement mutation was prepared under the write-set mutex");
        let owner = prepared.owner;
        let row_id = owner.row.row_id;
        if let Some(undo) = prepared.undo.as_mut() {
            // Preserve incompatible comparator domains by moving their controls,
            // rather than cloning all staged keys for an ordinary mutation.
            for (name, index) in &rows.indexes {
                undo.schemas.push((name.clone(), index.schema.clone()));
            }
            for (name, _) in &owner.keys {
                let displace = rows.indexes.get(name).is_none_or(|index| {
                    !index.schema.matches_index(&owner.schema, name)
                        || (index.postings.len() == 1
                            && index.postings.values().next() == Some(&row_id))
                });
                if displace {
                    undo.domains.push((name.clone(), rows.indexes.remove(name)));
                }
            }
            if let Some(prior) = &undo.prior_row {
                for (name, _) in &prior.keys {
                    if !undo.domains.iter().any(|(old_name, _)| old_name == name)
                        && rows
                            .indexes
                            .get(name)
                            .is_some_and(|index| index.postings.len() == 1)
                    {
                        undo.domains.push((name.clone(), rows.indexes.remove(name)));
                    }
                }
            }
        }
        rows.remove_private(row_id);
        rows.next_sequence = owner
            .sequence
            .checked_add(1)
            .expect("staged sequence fits usize");
        for (name, key) in &owner.keys {
            let index = rows
                .indexes
                .entry(name.clone())
                .or_insert_with(|| StatementIndex {
                    schema: owner.schema.clone(),
                    postings: BTreeMap::new(),
                });
            if !index.schema.matches_index(&owner.schema, name) {
                index.postings.clear();
                index.schema = owner.schema.clone();
            }
            index.schema = owner.schema.clone();
            index
                .postings
                .insert(StatementIndexKey::Row((key.clone(), row_id)), row_id);
        }
        rows.sequence.insert(owner.sequence, row_id);
        rows.rows.insert(row_id, owner);
        prepared.undo
    }
    pub(crate) fn restore(snapshot: &mut Arc<Self>, row_id: RowId, undo: StatementRowUndo) {
        if let Some(prior) = undo.prior_image.upgrade() {
            *snapshot = prior;
            return;
        }
        // Only pre-mutation statement snapshots are published. Preparation made
        // this replacement private; inverse installation needs no new admission.
        let rows = Arc::get_mut(snapshot).expect("private mutation replacement");
        rows.remove_private(row_id);
        rows.next_sequence = undo.prior_sequence;
        rows.copy_charge = undo.prior_copy_charge;
        for (name, domain) in undo.domains {
            rows.indexes.remove(&name);
            if let Some(domain) = domain {
                rows.indexes.insert(name, domain);
            }
        }
        for (name, schema) in undo.schemas {
            if let Some(index) = rows.indexes.get_mut(&name) {
                index.schema = schema;
            }
        }
        if let Some(owner) = undo.prior_row {
            for (name, key) in &owner.keys {
                if let Some(index) = rows.indexes.get_mut(name)
                    && index.schema.matches_index(&owner.schema, name)
                {
                    index
                        .postings
                        .insert(StatementIndexKey::Row((key.clone(), row_id)), row_id);
                }
            }
            rows.sequence.insert(owner.sequence, row_id);
            rows.rows.insert(row_id, owner);
        }
    }
    pub(crate) fn undo_control_bytes(&self, row_id: RowId, keys: &[(String, IndexKey)]) -> usize {
        std::mem::size_of::<StatementRowUndo>()
            .saturating_add(
                self.indexes
                    .keys()
                    .map(|name| {
                        name.len().saturating_add(std::mem::size_of::<(
                            String,
                            Arc<crate::ordered_staged::StagedSchema>,
                        )>())
                    })
                    .sum::<usize>(),
            )
            .saturating_add(
                keys.iter()
                    .map(|(name, key)| {
                        name.len()
                            .saturating_add(std::mem::size_of::<(String, Option<StatementIndex>)>())
                            .saturating_add(crate::ordered_staged::staged_key_bytes(key))
                    })
                    .sum::<usize>(),
            )
            .saturating_add(self.rows.get(&row_id).map_or(0, |owner| {
                owner
                    .keys
                    .iter()
                    .map(|(name, key)| {
                        name.len()
                            .saturating_add(std::mem::size_of::<(String, Option<StatementIndex>)>())
                            .saturating_add(crate::ordered_staged::staged_key_bytes(key))
                    })
                    .sum::<usize>()
            }))
    }
    fn remove_private(&mut self, row: RowId) {
        if let Some(owner) = self.rows.remove(&row) {
            self.sequence.remove(&owner.sequence);
            for (name, key) in &owner.keys {
                if let Some(index) = self.indexes.get_mut(name)
                    && index.schema.matches_index(&owner.schema, name)
                {
                    index
                        .postings
                        .remove(&StatementIndexKey::Row((key.clone(), row)));
                }
            }
        }
        self.indexes.retain(|_, index| !index.postings.is_empty());
    }
    pub(crate) fn reserve_removal_control(
        &self,
        bytes: usize,
    ) -> Result<Arc<dyn StagedPostingCharge>> {
        self.rows
            .values()
            .next()
            .expect("a selected statement table has a row")
            ._charge
            .reserve_copy(bytes)
    }
    pub(crate) fn prepare_removal(snapshot: &Arc<Self>) -> Result<PreparedStatementRemoval> {
        let replacement = if Arc::strong_count(snapshot) > 1 {
            let Some(owner) = snapshot.rows.values().next() else {
                return Ok(PreparedStatementRemoval { replacement: None });
            };
            let charge = owner._charge.reserve_copy(snapshot.control_bytes())?;
            let mut next = (**snapshot).clone();
            next.copy_charge = Some(charge);
            Some(Arc::new(next))
        } else {
            None
        };
        Ok(PreparedStatementRemoval { replacement })
    }
    pub(crate) fn prepare_delete(
        snapshot: &Arc<Self>,
        row: RowId,
        mut reserve: impl FnMut(usize) -> Result<Arc<dyn StagedPostingCharge>>,
    ) -> Result<PreparedStatementDelete> {
        let owner = snapshot.rows.get(&row).expect("selected staged row");
        let copy_charge = if Arc::strong_count(snapshot) > 1 {
            Some(owner._charge.reserve_copy(snapshot.control_bytes())?)
        } else {
            None
        };
        let charge = reserve(snapshot.undo_control_bytes(row, &owner.keys))?;
        Ok(PreparedStatementDelete {
            undo: StatementRowUndo {
                _charge: charge,
                prior_image: if copy_charge.is_some() {
                    Arc::downgrade(snapshot)
                } else {
                    Weak::new()
                },
                prior_row: Some(owner.clone()),
                prior_sequence: snapshot.next_sequence,
                prior_copy_charge: snapshot.copy_charge.clone(),
                domains: Vec::with_capacity(owner.keys.len()),
                schemas: Vec::with_capacity(snapshot.indexes.len()),
            },
            copy_charge,
        })
    }
    pub(crate) fn publish_delete(
        snapshot: &mut Arc<Self>,
        row: RowId,
        mut prepared: PreparedStatementDelete,
    ) -> StatementRowUndo {
        if let Some(charge) = prepared.copy_charge {
            let mut next = (**snapshot).clone();
            next.copy_charge = Some(charge);
            *snapshot = Arc::new(next);
        }
        let rows = Arc::get_mut(snapshot).expect("staged deletion prepared under write-set mutex");
        for (name, index) in &rows.indexes {
            prepared
                .undo
                .schemas
                .push((name.clone(), index.schema.clone()));
        }
        let owner = prepared
            .undo
            .prior_row
            .as_ref()
            .expect("selected staged row");
        for (name, _) in &owner.keys {
            if rows.indexes.get(name).is_some_and(|index| {
                index.postings.len() == 1
                    && index.postings.values().next() == Some(&row)
                    && index.schema.matches_index(&owner.schema, name)
            }) {
                prepared
                    .undo
                    .domains
                    .push((name.clone(), rows.indexes.remove(name)));
            }
        }
        rows.remove_private(row);
        prepared.undo
    }
    pub(crate) fn publish_removal(
        snapshot: &mut Arc<Self>,
        prepared: PreparedStatementRemoval,
        rows: impl Iterator<Item = RowId>,
    ) {
        if let Some(replacement) = prepared.replacement {
            *snapshot = replacement;
        }
        let private = Arc::get_mut(snapshot)
            .expect("statement removals were prepared under the write-set mutex");
        for row in rows {
            private.remove_private(row);
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
