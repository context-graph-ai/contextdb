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
        crate::staged_source::compare_statement_keys(
            self.key(),
            self.rank(),
            match self {
                Self::Row((_, row)) => Some(*row),
                _ => None,
            },
            other.key(),
            other.rank(),
            match other {
                Self::Row((_, row)) => Some(*row),
                _ => None,
            },
        )
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
#[derive(Debug, Default)]
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
        if let Some(bytes) = self
            .control_charge
            .as_ref()
            .and_then(|charge| charge.statement_retained_bytes(self))
        {
            return bytes;
        }
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
        self.len() == 0
    }
    pub fn contains(&self, row: &RowId) -> bool {
        self.row(*row).is_some()
    }
    /// Borrow a row from this immutable admitted statement image.
    #[doc(hidden)]
    pub fn row(&self, row: RowId) -> Option<&VersionedRow> {
        if let Some(view) = self.view() {
            return view.row(row);
        }
        self.rows.get(&row).map(|owner| &owner.row)
    }
    pub fn len(&self) -> usize {
        if let Some(view) = self.view() {
            return view.len();
        }
        self.rows.len()
    }
    pub fn rows(&self) -> impl Iterator<Item = &VersionedRow> {
        let mut after = None;
        std::iter::from_fn(move || {
            let (sequence, row) = self.next_unordered(after)?;
            after = Some(sequence);
            Some(row)
        })
    }
    pub fn next_unordered(&self, after: Option<usize>) -> Option<(usize, &VersionedRow)> {
        if let Some(view) = self.view() {
            return view.next_unordered(after);
        }
        let lower = after.map_or(Bound::Unbounded, Bound::Excluded);
        let (sequence, row) = self.sequence.range((lower, Bound::Unbounded)).next()?;
        Some((*sequence, &self.rows.get(row)?.row))
    }
    pub fn has_index_for(&self, index: &str, meta: &contextdb_core::TableMeta) -> bool {
        if let Some(view) = self.view() {
            return view.has_index_for(index, meta);
        }
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
        if let Some(view) = self.view() {
            return view.next_ordered_in_run(index, reverse, after, start, end, prefix_len);
        }
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
        if Arc::strong_count(snapshot) != 1
            || snapshot
                .control_charge
                .as_ref()
                .and_then(|charge| charge.statement_image_shared())
                .unwrap_or(false)
        {
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
        let copy_charge = if Self::image_shared(snapshot) {
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
        rows.reconcile(prepared.undo.is_some());
        prepared.undo
    }
    pub(crate) fn restore(snapshot: &mut Arc<Self>, row_id: RowId, undo: StatementRowUndo) {
        if let Some(prior) = undo.prior_image.upgrade() {
            *snapshot = prior;
            snapshot.reconcile(true);
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
        rows.reconcile(true);
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
        let replacement = if Self::image_shared(snapshot) {
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
        let copy_charge = if Self::image_shared(snapshot) {
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
        rows.reconcile(true);
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
        private.reconcile(false);
    }
    pub(crate) fn remove(snapshot: &mut Arc<Self>, row: RowId) -> Result<()> {
        let Some(owner) = snapshot.rows.get(&row) else {
            return Ok(());
        };
        let charge = owner._charge.clone();
        let rows = Self::make_mut(snapshot, &mut |bytes| charge.reserve_copy(bytes))?;
        rows.remove_private(row);
        rows.reconcile(false);
        Ok(())
    }
}

/// A separately admitted eager body. The canonical original and the ordinary
/// WriteSet copy never pay for this row or its native key/name controls.
/// Public output can retain this private handle without receiving its Arc.
#[derive(Clone)]
pub(crate) struct NativeEagerRow(crate::staged_source::FundedArc<NativeEagerRowBody>);

struct NativeEagerRowBody {
    original: crate::staged_source::AdmittedEagerSourceRow,
    sequence: usize,
    keys: crate::staged_source::SourceBuffer<(
        crate::staged_source::SourceName,
        crate::staged_source::AdmittedSourceKey,
    )>,
    schema: crate::staged_source::SchemaHandle,
}

impl NativeEagerRow {
    /// The caller first pairs the genuine copied body, invoice and original
    /// reservation metadata in `original`. Every subsequent refusal leaves
    /// that complete owner on the stack; no published source is changed.
    pub(crate) fn prepare(
        original: crate::staged_source::AdmittedEagerSourceRow,
        sequence: usize,
        keys: &[(String, IndexKey)],
        schema: crate::staged_source::SchemaHandle,
        funder: &crate::staged_source::SourceFunder,
    ) -> Result<Self> {
        let control =
            crate::staged_source::FundedArc::<NativeEagerRowBody>::prepare(funder.clone())?;
        let mut owned_keys = crate::staged_source::SourceBuffer::with_capacity(keys.len(), funder)?;
        if owned_keys.capacity() != keys.len() {
            return Err(contextdb_core::Error::Other(
                "eager key buffer allocation capacity mismatch".into(),
            ));
        }
        for (name, key) in keys {
            let name = crate::staged_source::SourceName::new(name, funder)?;
            let key = funder.clone().copy_key(key)?;
            owned_keys.push((name, key));
        }
        Ok(Self(crate::staged_source::FundedArc::from_prepared(
            NativeEagerRowBody {
                original,
                sequence,
                keys: owned_keys,
                schema,
            },
            control,
        )))
    }

    pub(crate) fn row(&self) -> &VersionedRow {
        self.0.original.row()
    }

    pub(crate) fn retained_bytes(&self) -> usize {
        self.0
            .retained_bytes()
            .saturating_add(self.0.original.retained_bytes())
            .saturating_add(self.0.keys.retained_bytes())
            .saturating_add(
                self.0
                    .keys
                    .as_slice()
                    .iter()
                    .map(|(name, key)| name.retained_bytes().saturating_add(key.charge.bytes()))
                    .sum::<usize>(),
            )
    }
    pub(crate) fn sequence(&self) -> usize {
        self.0.sequence
    }

    pub(crate) fn keys(&self) -> impl Iterator<Item = (&str, &IndexKey)> {
        self.0
            .keys
            .as_slice()
            .iter()
            .map(|(name, key)| (name.as_str(), &key.key))
    }

    pub(crate) fn schema(&self) -> &crate::staged_source::SchemaHandle {
        &self.0.schema
    }

    pub(crate) fn reserve_private_original_copy(
        &self,
        bytes: usize,
    ) -> Result<crate::staged_source::OriginalCopyGrant> {
        self.0.original.reserve_private_original_copy(bytes)
    }
    pub(crate) fn ptr_eq(&self, other: &Self) -> bool {
        self.0.ptr_eq(&other.0)
    }
    pub(crate) fn terminal_owner(
        &self,
        schema: Arc<crate::ordered_staged::StagedSchema>,
    ) -> Arc<RowOwner> {
        Arc::new(RowOwner {
            row: self.row().clone(),
            sequence: self.sequence(),
            keys: self
                .keys()
                .map(|(name, key)| (name.to_owned(), key.clone()))
                .collect(),
            schema,
            _charge: Arc::new(TerminalRowCharge {
                origin: StatementOrigin(StatementOriginOwner::Row(self.clone())),
            }),
        })
    }
}

impl std::fmt::Debug for NativeEagerRow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeEagerRow")
            .field("row", self.row())
            .field("sequence", &self.sequence())
            .field("key_count", &self.0.keys.len())
            .field("schema", self.schema())
            .finish()
    }
}

/// Semantic image owners are counted separately from payload handles. A
/// source image and a terminal clone remain shared even before a public getter
/// creates its host shell. Unrelated scalar-vector row holders do not count.
pub(crate) struct NativeImage<T> {
    data: Option<crate::staged_source::FundedArc<NativeImageBody<T>>>,
    copy_charge: Option<crate::staged_source::OriginalCopyGrant>,
}

struct NativeImageBody<T> {
    value: T,
    holders: std::sync::atomic::AtomicUsize,
}

impl<T> NativeImage<T> {
    pub(crate) fn prepare(value: T, funder: &crate::staged_source::SourceFunder) -> Result<Self> {
        let control =
            crate::staged_source::FundedArc::<NativeImageBody<T>>::prepare(funder.clone())?;
        Ok(Self {
            data: Some(crate::staged_source::FundedArc::from_prepared(
                NativeImageBody {
                    value,
                    holders: std::sync::atomic::AtomicUsize::new(1),
                },
                control,
            )),
            copy_charge: None,
        })
    }

    pub(crate) fn get(&self) -> &T {
        &self.data.as_ref().expect("live native image").value
    }

    pub(crate) fn ptr_eq(&self, other: &Self) -> bool {
        self.data
            .as_ref()
            .expect("live native image")
            .ptr_eq(other.data.as_ref().expect("live native image"))
    }
    pub(crate) fn control_bytes(&self) -> usize {
        self.data
            .as_ref()
            .expect("live native image")
            .retained_bytes()
            .saturating_add(
                self.copy_charge
                    .as_ref()
                    .map_or(0, |charge| charge.retained_bytes()),
            )
    }
    pub(crate) fn is_shared(&self) -> bool {
        self.data
            .as_ref()
            .expect("live native image")
            .holders
            .load(std::sync::atomic::Ordering::Acquire)
            > 1
    }
}

impl<T> Clone for NativeImage<T> {
    fn clone(&self) -> Self {
        let data = self.data.as_ref().expect("live native image").clone();
        // Like Arc, an unrepresentable number of simultaneous owners cannot
        // be allowed to wrap the count and masquerade as private custody.
        let prior = data
            .holders
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if prior >= isize::MAX as usize {
            std::process::abort();
        }
        Self {
            data: Some(data),
            copy_charge: self.copy_charge.clone(),
        }
    }
}

impl<T> Drop for NativeImage<T> {
    fn drop(&mut self) {
        if let Some(data) = self.data.take() {
            data.holders
                .fetch_sub(1, std::sync::atomic::Ordering::Release);
            // FundedArc destroys the data allocation before its control debit.
            // T's payloads retain their separate actual invoices throughout.
            drop(data);
        }
        drop(self.copy_charge.take());
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for NativeImage<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeImage")
            .field("value", self.get())
            .field("shared", &self.is_shared())
            .finish()
    }
}

/// Immutable eager statement data retains its own real row, key and schema
/// owners. Every reference returned by the public shell borrows this root.
#[derive(Debug)]
pub(crate) struct NativeStatementRoot {
    by_id: crate::staged_source::Link<RowId, NativeEagerRow>,
    by_sequence: crate::staged_source::Link<usize, RowId>,
    indexes: crate::staged_source::Link<
        crate::staged_source::SourceName,
        crate::staged_source::FundedArc<NativeStatementIndex>,
    >,
    next_sequence: usize,
}
#[derive(Debug)]
struct NativeStatementIndex {
    schema: crate::staged_source::SchemaHandle,
    postings: crate::staged_source::Link<NativeStatementKey, RowId>,
}
#[derive(Debug, Clone)]
struct NativeStatementKey(crate::staged_source::FundedArc<NativeStatementKeyBody>);
#[derive(Debug)]
struct NativeStatementKeyBody {
    position: (IndexKey, RowId),
    _charge: crate::staged_source::SourceCharge,
}
impl NativeStatementKey {
    fn prepare(
        key: &IndexKey,
        row: RowId,
        funder: &crate::staged_source::SourceFunder,
    ) -> Result<Self> {
        let header =
            crate::staged_source::FundedArc::<NativeStatementKeyBody>::prepare(funder.clone())?;
        let admitted = funder.clone().copy_key(key)?;
        Ok(Self(crate::staged_source::FundedArc::from_prepared(
            NativeStatementKeyBody {
                position: (admitted.key, row),
                _charge: admitted.charge,
            },
            header,
        )))
    }
    fn position(&self) -> &(IndexKey, RowId) {
        &self.0.position
    }
}
impl PartialEq for NativeStatementKey {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for NativeStatementKey {}
impl PartialOrd for NativeStatementKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for NativeStatementKey {
    fn cmp(&self, other: &Self) -> Ordering {
        crate::staged_source::compare_statement_keys(
            &self.position().0,
            1,
            Some(self.position().1),
            &other.position().0,
            1,
            Some(other.position().1),
        )
    }
}
impl NativeStatementRoot {
    pub(crate) fn empty() -> Self {
        Self {
            by_id: None,
            by_sequence: None,
            indexes: None,
            next_sequence: 0,
        }
    }
    pub(crate) fn next_sequence(&self) -> usize {
        self.next_sequence
    }
    pub(crate) fn row(&self, row: RowId) -> Option<&NativeEagerRow> {
        crate::staged_source::get_by(&self.by_id, |key| row.cmp(key))
    }
    pub(crate) fn len(&self) -> usize {
        crate::staged_source::len(&self.by_id)
    }
    fn index(&self, name: &str) -> Option<&NativeStatementIndex> {
        crate::staged_source::get_by(&self.indexes, |key| name.cmp(key.as_str()))
            .map(|index| &**index)
    }
    pub(crate) fn has_index_for(&self, name: &str, meta: &contextdb_core::TableMeta) -> bool {
        self.index(name).is_some_and(|index| {
            crate::staged_source::len(&index.postings) == self.len()
                && index.schema.matches_meta(meta, name)
        })
    }
    pub(crate) fn next_unordered(&self, after: Option<usize>) -> Option<(usize, &VersionedRow)> {
        let (sequence, row) = crate::staged_source::seek_by(&self.by_sequence, false, |key| {
            if after.is_some_and(|after| *key <= after) {
                Ordering::Less
            } else {
                Ordering::Equal
            }
        })?;
        Some((*sequence, self.row(*row)?.row()))
    }
    pub(crate) fn next_ordered_in_run(
        &self,
        name: &str,
        reverse: bool,
        after: Option<&(IndexKey, RowId)>,
        start: &Bound<IndexKey>,
        end: &Bound<IndexKey>,
        prefix_len: usize,
    ) -> Option<(&(IndexKey, RowId), &VersionedRow)> {
        let index = self.index(name)?;
        let (key, row) = crate::staged_source::seek_by(&index.postings, reverse, |key| {
            let position = key.position();
            let compare = |bound: &IndexKey, rank: u8| {
                crate::staged_source::compare_statement_keys(
                    &position.0,
                    1,
                    Some(position.1),
                    &bound[..prefix_len.min(bound.len())],
                    rank,
                    None,
                )
            };
            let before = if !reverse && after.is_some() {
                false
            } else {
                match start {
                    Bound::Unbounded => false,
                    Bound::Included(bound) => compare(bound, 0) == Ordering::Less,
                    Bound::Excluded(bound) => compare(bound, 2) != Ordering::Greater,
                }
            };
            if before {
                return Ordering::Less;
            }
            let beyond = if reverse && after.is_some() {
                false
            } else {
                match end {
                    Bound::Unbounded => false,
                    Bound::Included(bound) => compare(bound, 2) == Ordering::Greater,
                    Bound::Excluded(bound) => compare(bound, 0) != Ordering::Less,
                }
            };
            if beyond {
                return Ordering::Greater;
            }
            if let Some(after) = after {
                let cmp = crate::staged_source::compare_statement_keys(
                    &position.0,
                    1,
                    Some(position.1),
                    &after.0,
                    1,
                    Some(after.1),
                );
                if reverse && cmp != Ordering::Less {
                    return Ordering::Greater;
                }
                if !reverse && cmp != Ordering::Greater {
                    return Ordering::Less;
                }
            }
            Ordering::Equal
        })?;
        Some((key.position(), self.row(*row)?.row()))
    }
    pub(crate) fn prepare_insert(
        &self,
        row: NativeEagerRow,
        funder: &crate::staged_source::SourceFunder,
    ) -> Result<NativeImage<Self>> {
        use crate::staged_source::{Entry, FundedArc, insert, remove_by};
        let next_sequence = self
            .next_sequence
            .checked_add(1)
            .ok_or_else(|| contextdb_core::Error::Other("staged sequence overflow".into()))?;
        assert_eq!(
            row.sequence(),
            self.next_sequence,
            "prepared statement sequence"
        );
        let id = row.row().row_id;
        let mut by_sequence = self.by_sequence.clone();
        let mut indexes = self.indexes.clone();
        if let Some(prior) = self.row(id) {
            by_sequence = remove_by(&by_sequence, &|key| prior.sequence().cmp(key), funder)?;
            indexes = Self::remove_postings(indexes, prior, funder)?;
        }
        for (name, key) in row.keys() {
            let prior = crate::staged_source::get_by(&indexes, |key| name.cmp(key.as_str()));
            let postings = prior
                .filter(|index| index.schema.matches_index(row.schema(), name))
                .and_then(|index| index.postings.clone());
            let postings = insert(
                &postings,
                FundedArc::try_new(
                    Entry {
                        key: NativeStatementKey::prepare(key, id, funder)?,
                        value: id,
                    },
                    funder.clone(),
                )?,
                funder,
            )?;
            let domain = FundedArc::try_new(
                NativeStatementIndex {
                    schema: row.schema().clone(),
                    postings,
                },
                funder.clone(),
            )?;
            let name = crate::staged_source::SourceName::new(name, funder)?;
            indexes = insert(
                &indexes,
                FundedArc::try_new(
                    Entry {
                        key: name,
                        value: domain,
                    },
                    funder.clone(),
                )?,
                funder,
            )?;
        }
        let by_sequence = insert(
            &by_sequence,
            FundedArc::try_new(
                Entry {
                    key: row.sequence(),
                    value: id,
                },
                funder.clone(),
            )?,
            funder,
        )?;
        let by_id = insert(
            &self.by_id,
            FundedArc::try_new(
                Entry {
                    key: id,
                    value: row,
                },
                funder.clone(),
            )?,
            funder,
        )?;
        NativeImage::prepare(
            Self {
                by_id,
                by_sequence,
                indexes,
                next_sequence,
            },
            funder,
        )
    }
    pub(crate) fn prepare_remove(
        &self,
        id: RowId,
        funder: &crate::staged_source::SourceFunder,
    ) -> Result<NativeImage<Self>> {
        let mut by_sequence = self.by_sequence.clone();
        let mut indexes = self.indexes.clone();
        if let Some(prior) = self.row(id) {
            by_sequence = crate::staged_source::remove_by(
                &by_sequence,
                &|key| prior.sequence().cmp(key),
                funder,
            )?;
            indexes = Self::remove_postings(indexes, prior, funder)?;
        }
        let by_id = crate::staged_source::remove_by(&self.by_id, &|key| id.cmp(key), funder)?;
        NativeImage::prepare(
            Self {
                by_id,
                by_sequence,
                indexes,
                next_sequence: self.next_sequence,
            },
            funder,
        )
    }
    fn remove_postings(
        mut indexes: crate::staged_source::Link<
            crate::staged_source::SourceName,
            crate::staged_source::FundedArc<NativeStatementIndex>,
        >,
        row: &NativeEagerRow,
        funder: &crate::staged_source::SourceFunder,
    ) -> Result<
        crate::staged_source::Link<
            crate::staged_source::SourceName,
            crate::staged_source::FundedArc<NativeStatementIndex>,
        >,
    > {
        use crate::staged_source::{Entry, FundedArc, get_by, insert, remove_by};
        for (name, key) in row.keys() {
            let Some(prior) = get_by(&indexes, |key| name.cmp(key.as_str())) else {
                continue;
            };
            if !prior.schema.matches_index(row.schema(), name) {
                continue;
            }
            let postings = remove_by(
                &prior.postings,
                &|other| {
                    crate::staged_source::compare_statement_keys(
                        key,
                        1,
                        Some(row.row().row_id),
                        &other.position().0,
                        1,
                        Some(other.position().1),
                    )
                },
                funder,
            )?;
            if postings.is_none() {
                indexes = remove_by(&indexes, &|key| name.cmp(key.as_str()), funder)?;
            } else {
                let schema = prior.schema.clone();
                let name = crate::staged_source::SourceName::new(name, funder)?;
                let domain =
                    FundedArc::try_new(NativeStatementIndex { schema, postings }, funder.clone())?;
                indexes = insert(
                    &indexes,
                    FundedArc::try_new(
                        Entry {
                            key: name,
                            value: domain,
                        },
                        funder.clone(),
                    )?,
                    funder,
                )?;
            }
        }
        Ok(indexes)
    }
}

impl NativeStatementRoot {
    /// Materialize real mutable Legacy maps. Payloads remain in their private
    /// native owners; each new std Arc header belongs to this terminal output.
    pub(crate) fn terminal_legacy(&self, alias: NativeImage<Self>) -> StatementRows {
        let mut output = StatementRows {
            next_sequence: self.next_sequence,
            control_charge: Some(Arc::new(TerminalStatementAlias::new(alias))),
            ..StatementRows::default()
        };
        let mut after = None;
        while let Some((sequence, _)) = self.next_unordered(after) {
            let id = *crate::staged_source::get_by(&self.by_sequence, |key| sequence.cmp(key))
                .expect("sequence row");
            let row = self.row(id).expect("native statement row").clone();
            let schema = self
                .rows_schema_shell(&output, row.schema())
                .unwrap_or_else(|| {
                    Arc::new(crate::ordered_staged::StagedSchema::from_native(
                        row.schema().clone(),
                    ))
                });
            let keys = row
                .keys()
                .map(|(name, key)| (name.to_owned(), key.clone()))
                .collect();
            output.rows.insert(
                id,
                Arc::new(RowOwner {
                    row: row.row().clone(),
                    sequence,
                    keys,
                    schema,
                    _charge: Arc::new(TerminalRowCharge {
                        origin: StatementOrigin(StatementOriginOwner::Row(row)),
                    }),
                }),
            );
            output.sequence.insert(sequence, id);
            after = Some(sequence);
        }
        let mut after_name: Option<&str> = None;
        while let Some((name, index)) = crate::staged_source::seek_by(&self.indexes, false, |key| {
            if after_name.is_some_and(|after| key.as_str() <= after) {
                Ordering::Less
            } else {
                Ordering::Equal
            }
        }) {
            let mut postings = BTreeMap::new();
            let mut after_key = None;
            while let Some((key, id)) =
                crate::staged_source::seek_by(&index.postings, false, |key| {
                    if after_key.is_some_and(|after: &NativeStatementKey| key <= after) {
                        Ordering::Less
                    } else {
                        Ordering::Equal
                    }
                })
            {
                postings.insert(StatementIndexKey::Row(key.position().clone()), *id);
                after_key = Some(key);
            }
            // Preserve domains whose installing row has since been removed.
            // Reuse a row's schema shell only for the same genuine native schema.
            let schema = self
                .rows_schema_shell(&output, &index.schema)
                .or_else(|| self.index_schema_shell(&output, &index.schema))
                .unwrap_or_else(|| {
                    Arc::new(crate::ordered_staged::StagedSchema::from_native(
                        index.schema.clone(),
                    ))
                });
            output.indexes.insert(
                name.as_str().to_owned(),
                StatementIndex { schema, postings },
            );
            after_name = Some(name.as_str());
        }
        output
    }
    fn rows_schema_shell(
        &self,
        output: &StatementRows,
        schema: &crate::staged_source::SchemaHandle,
    ) -> Option<Arc<crate::ordered_staged::StagedSchema>> {
        output.rows.iter().find_map(|(id, owner)| {
            self.row(*id)
                .filter(|row| row.schema().ptr_eq(schema))
                .map(|_| owner.schema.clone())
        })
    }
    pub(crate) fn retained_bytes(&self) -> usize {
        let mut bytes = crate::staged_source::retained_link_bytes(&self.by_id)
            .saturating_add(crate::staged_source::retained_link_bytes(&self.by_sequence))
            .saturating_add(crate::staged_source::retained_link_bytes(&self.indexes));
        let mut after = None;
        while let Some((id, row)) = crate::staged_source::seek_by(&self.by_id, false, |id| {
            if after.is_some_and(|after| *id <= after) {
                Ordering::Less
            } else {
                Ordering::Equal
            }
        }) {
            bytes = bytes.saturating_add(row.retained_bytes());
            if !self.schema_owned_before(row.schema(), *id) {
                bytes = bytes.saturating_add(row.schema().retained_bytes());
            }
            after = Some(*id);
        }
        let mut after_name: Option<&str> = None;
        while let Some((name, index)) =
            crate::staged_source::seek_by(&self.indexes, false, |name| {
                if after_name.is_some_and(|after| name.as_str() <= after) {
                    Ordering::Less
                } else {
                    Ordering::Equal
                }
            })
        {
            bytes = bytes
                .saturating_add(name.retained_bytes())
                .saturating_add(index.retained_bytes())
                .saturating_add(crate::staged_source::retained_link_bytes(&index.postings));
            if !self.has_schema_owner(&index.schema)
                && !self.index_schema_owned_before(&index.schema, name.as_str())
            {
                bytes = bytes.saturating_add(index.schema.retained_bytes());
            }
            let mut after_key = None;
            while let Some((key, _)) =
                crate::staged_source::seek_by(&index.postings, false, |key| {
                    if after_key.is_some_and(|after: &NativeStatementKey| key <= after) {
                        Ordering::Less
                    } else {
                        Ordering::Equal
                    }
                })
            {
                bytes = bytes
                    .saturating_add(key.0.retained_bytes())
                    .saturating_add(key.0._charge.bytes());
                after_key = Some(key);
            }
            after_name = Some(name.as_str());
        }
        bytes
    }
    fn has_schema_owner(&self, schema: &crate::staged_source::SchemaHandle) -> bool {
        let mut after = None;
        while let Some((id, row)) = crate::staged_source::seek_by(&self.by_id, false, |id| {
            if after.is_some_and(|after| *id <= after) {
                Ordering::Less
            } else {
                Ordering::Equal
            }
        }) {
            if row.schema().ptr_eq(schema) {
                return true;
            }
            after = Some(*id);
        }
        false
    }
}

impl NativeStatementRoot {
    fn schema_owned_before(
        &self,
        schema: &crate::staged_source::SchemaHandle,
        upper: RowId,
    ) -> bool {
        let mut after = None;
        while let Some((id, row)) = crate::staged_source::seek_by(&self.by_id, false, |id| {
            if *id >= upper {
                Ordering::Greater
            } else if after.is_some_and(|after| *id <= after) {
                Ordering::Less
            } else {
                Ordering::Equal
            }
        }) {
            if row.schema().ptr_eq(schema) {
                return true;
            }
            after = Some(*id);
        }
        false
    }
    fn index_schema_owned_before(
        &self,
        schema: &crate::staged_source::SchemaHandle,
        upper: &str,
    ) -> bool {
        let mut after: Option<&str> = None;
        while let Some((name, index)) =
            crate::staged_source::seek_by(&self.indexes, false, |name| {
                if name.as_str() >= upper {
                    Ordering::Greater
                } else if after.is_some_and(|after| name.as_str() <= after) {
                    Ordering::Less
                } else {
                    Ordering::Equal
                }
            })
        {
            if index.schema.ptr_eq(schema) {
                return true;
            }
            after = Some(name.as_str());
        }
        false
    }
}

impl NativeStatementRoot {
    fn index_schema_shell(
        &self,
        output: &StatementRows,
        schema: &crate::staged_source::SchemaHandle,
    ) -> Option<Arc<crate::ordered_staged::StagedSchema>> {
        output.indexes.iter().find_map(|(name, index)| {
            self.index(name)
                .filter(|domain| domain.schema.ptr_eq(schema))
                .map(|_| index.schema.clone())
        })
    }
    // activated by bounded-storage S3/S4; remove allow at activation
    #[allow(dead_code)]
    fn structural_bytes(&self) -> usize {
        let mut bytes = crate::staged_source::retained_link_bytes(&self.by_id)
            .saturating_add(crate::staged_source::retained_link_bytes(&self.by_sequence))
            .saturating_add(crate::staged_source::retained_link_bytes(&self.indexes));
        let mut after_name: Option<&str> = None;
        while let Some((name, index)) =
            crate::staged_source::seek_by(&self.indexes, false, |name| {
                if after_name.is_some_and(|after| name.as_str() <= after) {
                    Ordering::Less
                } else {
                    Ordering::Equal
                }
            })
        {
            bytes = bytes
                .saturating_add(name.retained_bytes())
                .saturating_add(index.retained_bytes())
                .saturating_add(crate::staged_source::retained_link_bytes(&index.postings));
            let mut after_key = None;
            while let Some((key, _)) =
                crate::staged_source::seek_by(&index.postings, false, |key| {
                    if after_key.is_some_and(|after: &NativeStatementKey| key <= after) {
                        Ordering::Less
                    } else {
                        Ordering::Equal
                    }
                })
            {
                bytes = bytes
                    .saturating_add(key.0.retained_bytes())
                    .saturating_add(key.0._charge.bytes());
                after_key = Some(key);
            }
            after_name = Some(name.as_str());
        }
        bytes
    }
}

pub(crate) struct NativeImageValidation<T> {
    data: Option<crate::staged_source::FundedArc<NativeImageBody<T>>>,
    _copy_charge: Option<crate::staged_source::OriginalCopyGrant>,
}
impl<T> NativeImageValidation<T> {
    pub(crate) fn matches(&self, image: &NativeImage<T>) -> bool {
        self.data
            .as_ref()
            .expect("live validation pin")
            .ptr_eq(image.data.as_ref().expect("live native image"))
    }
}
impl<T> NativeImage<T> {
    pub(crate) fn validation_handle(&self) -> NativeImageValidation<T> {
        NativeImageValidation {
            data: Some(self.data.as_ref().expect("live native image").clone()),
            _copy_charge: self.copy_charge.clone(),
        }
    }
    pub(crate) fn with_original_copy(
        mut self,
        charge: Option<crate::staged_source::OriginalCopyGrant>,
    ) -> Self {
        assert!(
            self.copy_charge.is_none(),
            "one original copy invoice per native image"
        );
        self.copy_charge = charge;
        self
    }
}
impl NativeStatementRoot {
    pub(crate) fn legacy_control_bytes(&self) -> usize {
        let mut bytes = 256usize;
        let mut after = None;
        while let Some((id, row)) = crate::staged_source::seek_by(&self.by_id, false, |id| {
            if after.is_some_and(|after| *id <= after) {
                Ordering::Less
            } else {
                Ordering::Equal
            }
        }) {
            bytes = bytes.saturating_add(192);
            for (name, key) in row.keys() {
                bytes = bytes
                    .saturating_add(256)
                    .saturating_add(name.len())
                    .saturating_add(crate::ordered_staged::staged_key_bytes(key));
            }
            after = Some(*id);
        }
        bytes
    }
}

/// Immutable, borrowed public view of private eager statement data.
#[doc(hidden)]
pub trait StatementImageView: Send + Sync {
    fn row(&self, id: RowId) -> Option<&VersionedRow>;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn next_unordered(&self, after: Option<usize>) -> Option<(usize, &VersionedRow)>;
    fn has_index_for(&self, index: &str, meta: &contextdb_core::TableMeta) -> bool;
    #[allow(clippy::too_many_arguments)]
    fn next_ordered_in_run(
        &self,
        index: &str,
        reverse: bool,
        after: Option<&(IndexKey, RowId)>,
        start: &Bound<IndexKey>,
        end: &Bound<IndexKey>,
        prefix_len: usize,
    ) -> Option<(&(IndexKey, RowId), &VersionedRow)>;
    fn clone_terminal(&self) -> StatementRows;
}
#[doc(hidden)]
pub struct StatementOrigin(pub(crate) StatementOriginOwner);
pub(crate) enum StatementOriginOwner {
    Row(NativeEagerRow),
    Schema(crate::staged_source::SchemaHandle),
}
impl std::fmt::Debug for StatementOrigin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StatementOrigin").finish_non_exhaustive()
    }
}
#[derive(Debug)]
pub(crate) struct NativeCopyCharge {
    pub(crate) grant: crate::staged_source::OriginalCopyGrant,
}
impl StagedPostingCharge for NativeCopyCharge {
    fn retained_bytes(&self) -> usize {
        self.grant.retained_bytes()
    }
    fn reserve_copy(&self, bytes: usize) -> Result<Arc<dyn StagedPostingCharge>> {
        self.grant.reserve_terminal_copy(bytes)
    }
}
#[derive(Debug)]
struct TerminalRowCharge {
    origin: StatementOrigin,
}
impl StagedPostingCharge for TerminalRowCharge {
    fn retained_bytes(&self) -> usize {
        let StatementOriginOwner::Row(row) = &self.origin.0 else {
            unreachable!()
        };
        row.retained_bytes()
    }
    fn reserve_copy(&self, bytes: usize) -> Result<Arc<dyn StagedPostingCharge>> {
        let StatementOriginOwner::Row(row) = &self.origin.0 else {
            unreachable!()
        };
        Ok(Arc::new(NativeCopyCharge {
            grant: row.reserve_private_original_copy(bytes)?,
        }))
    }
    fn statement_origin(&self) -> Option<&StatementOrigin> {
        Some(&self.origin)
    }
}
#[derive(Debug)]
pub(crate) struct TerminalSchemaCharge {
    pub(crate) origin: StatementOrigin,
}
impl StagedPostingCharge for TerminalSchemaCharge {
    fn retained_bytes(&self) -> usize {
        let StatementOriginOwner::Schema(schema) = &self.origin.0 else {
            unreachable!()
        };
        schema.retained_bytes()
    }
    fn statement_origin(&self) -> Option<&StatementOrigin> {
        Some(&self.origin)
    }
}
#[derive(Debug)]
struct TerminalStatementView {
    image: NativeImage<NativeStatementRoot>,
}
impl StatementImageView for TerminalStatementView {
    fn row(&self, id: RowId) -> Option<&VersionedRow> {
        self.image.get().row(id).map(NativeEagerRow::row)
    }
    fn len(&self) -> usize {
        self.image.get().len()
    }
    fn next_unordered(&self, after: Option<usize>) -> Option<(usize, &VersionedRow)> {
        self.image.get().next_unordered(after)
    }
    fn has_index_for(&self, index: &str, meta: &contextdb_core::TableMeta) -> bool {
        self.image.get().has_index_for(index, meta)
    }
    fn next_ordered_in_run(
        &self,
        index: &str,
        reverse: bool,
        after: Option<&(IndexKey, RowId)>,
        start: &Bound<IndexKey>,
        end: &Bound<IndexKey>,
        prefix_len: usize,
    ) -> Option<(&(IndexKey, RowId), &VersionedRow)> {
        self.image
            .get()
            .next_ordered_in_run(index, reverse, after, start, end, prefix_len)
    }
    fn clone_terminal(&self) -> StatementRows {
        self.image.get().terminal_legacy(self.image.clone())
    }
}
impl StagedPostingCharge for TerminalStatementView {
    fn statement_retained_bytes(&self, _rows: &StatementRows) -> Option<usize> {
        self.retained_union()
    }
    fn retained_bytes(&self) -> usize {
        self.image
            .control_bytes()
            .saturating_add(self.image.get().retained_bytes())
    }
    fn statement_view(&self) -> Option<&dyn StatementImageView> {
        Some(self)
    }
    fn statement_image_shared(&self) -> Option<bool> {
        Some(self.image.is_shared())
    }
}
struct TerminalAliasState {
    image: Option<NativeImage<NativeStatementRoot>>,
    pin: Option<NativeImageRestore<NativeStatementRoot>>,
}
struct TerminalStatementAlias {
    state: std::sync::Mutex<TerminalAliasState>,
}
impl TerminalStatementAlias {
    fn new(image: NativeImage<NativeStatementRoot>) -> Self {
        Self {
            state: std::sync::Mutex::new(TerminalAliasState {
                image: Some(image),
                pin: None,
            }),
        }
    }
    fn state(&self) -> std::sync::MutexGuard<'_, TerminalAliasState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}
impl std::fmt::Debug for TerminalStatementAlias {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TerminalStatementAlias")
            .finish_non_exhaustive()
    }
}
impl StagedPostingCharge for TerminalStatementAlias {
    fn statement_retained_bytes(&self, rows: &StatementRows) -> Option<usize> {
        self.retained_union(rows)
    }
    fn retained_bytes(&self) -> usize {
        let state = self.state();
        state.image.as_ref().map_or_else(
            || {
                state.pin.as_ref().map_or(0, |pin| {
                    pin.control_bytes()
                        .saturating_add(pin.get().retained_bytes())
                })
            },
            |image| {
                image
                    .control_bytes()
                    .saturating_add(image.get().retained_bytes())
            },
        )
    }
    fn statement_image_shared(&self) -> Option<bool> {
        Some(
            self.state()
                .image
                .as_ref()
                .is_some_and(NativeImage::is_shared),
        )
    }
    fn clone_statement_control(&self) -> Option<Arc<dyn StagedPostingCharge>> {
        let state = self.state();
        Some(Arc::new(Self {
            state: std::sync::Mutex::new(TerminalAliasState {
                image: state.image.clone(),
                pin: state.pin.clone(),
            }),
        }))
    }
    fn reconcile_statement_image(&self, rows: &StatementRows, keep_undo: bool) {
        let mut state = self.state();
        if state
            .image
            .as_ref()
            .is_some_and(|image| !image.get().matches_terminal(rows))
        {
            let image = state.image.take().expect("semantic image");
            if keep_undo {
                state.pin = Some(image.into_restore());
            } else {
                drop(image);
                state.pin = None;
            }
        } else if state
            .pin
            .as_ref()
            .is_some_and(|pin| pin.get().matches_terminal(rows))
        {
            state.image = Some(state.pin.take().expect("restored image").into_image());
        }
        if !keep_undo && state.image.is_none() {
            state.pin = None;
        }
    }
    fn settle_statement_image(&self) {
        self.state().pin = None;
    }
}
impl Clone for StatementRows {
    fn clone(&self) -> Self {
        if let Some(view) = self.view() {
            return view.clone_terminal();
        }
        Self {
            rows: self.rows.clone(),
            sequence: self.sequence.clone(),
            indexes: self.indexes.clone(),
            next_sequence: self.next_sequence,
            copy_charge: self.copy_charge.clone(),
            control_charge: self.control_charge.as_ref().map(|charge| {
                charge
                    .clone_statement_control()
                    .unwrap_or_else(|| charge.clone())
            }),
        }
    }
}
impl StatementRows {
    fn view(&self) -> Option<&dyn StatementImageView> {
        self.control_charge
            .as_ref()
            .and_then(|charge| charge.statement_view())
    }
    pub(crate) fn from_native(image: NativeImage<NativeStatementRoot>) -> Self {
        Self {
            next_sequence: image.get().next_sequence(),
            control_charge: Some(Arc::new(TerminalStatementView { image })),
            ..Self::default()
        }
    }
    fn image_shared(snapshot: &Arc<Self>) -> bool {
        Arc::strong_count(snapshot) > 1
            || snapshot
                .control_charge
                .as_ref()
                .and_then(|charge| charge.statement_image_shared())
                .unwrap_or(false)
    }
    fn reconcile(&self, keep_undo: bool) {
        if let Some(charge) = &self.control_charge {
            charge.reconcile_statement_image(self, keep_undo);
        }
    }
    pub(crate) fn settle_image(&self) {
        if let Some(charge) = &self.control_charge {
            charge.settle_statement_image();
        }
    }
}

/// A bookkeeping pin retains physical custody without a semantic image count.
pub(crate) struct NativeImageRestore<T> {
    data: Option<crate::staged_source::FundedArc<NativeImageBody<T>>>,
    copy_charge: Option<crate::staged_source::OriginalCopyGrant>,
}
impl<T> NativeImageRestore<T> {
    pub(crate) fn get(&self) -> &T {
        &self.data.as_ref().expect("live restore pin").value
    }
    pub(crate) fn control_bytes(&self) -> usize {
        self.data
            .as_ref()
            .expect("live restore pin")
            .retained_bytes()
            .saturating_add(
                self.copy_charge
                    .as_ref()
                    .map_or(0, |grant| grant.retained_bytes()),
            )
    }
    pub(crate) fn into_image(mut self) -> NativeImage<T> {
        let data = self.data.take().expect("live restore pin");
        let prior = data
            .holders
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if prior >= isize::MAX as usize {
            std::process::abort();
        }
        NativeImage {
            data: Some(data),
            copy_charge: self.copy_charge.take(),
        }
    }
}
impl<T> Clone for NativeImageRestore<T> {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            copy_charge: self.copy_charge.clone(),
        }
    }
}
impl<T> Drop for NativeImageRestore<T> {
    fn drop(&mut self) {
        drop(self.data.take());
        drop(self.copy_charge.take());
    }
}
impl<T> Drop for NativeImageValidation<T> {
    fn drop(&mut self) {
        drop(self.data.take());
        drop(self._copy_charge.take());
    }
}
impl<T> NativeImage<T> {
    pub(crate) fn restore_pin(&self) -> NativeImageRestore<T> {
        NativeImageRestore {
            data: self.data.clone(),
            copy_charge: self.copy_charge.clone(),
        }
    }
    pub(crate) fn into_restore(mut self) -> NativeImageRestore<T> {
        let data = self.data.take().expect("live native image");
        data.holders
            .fetch_sub(1, std::sync::atomic::Ordering::Release);
        NativeImageRestore {
            data: Some(data),
            copy_charge: self.copy_charge.take(),
        }
    }
}
impl NativeStatementRoot {
    fn matches_terminal(&self, rows: &StatementRows) -> bool {
        if rows.view().is_some()
            || rows.next_sequence != self.next_sequence
            || rows.rows.len() != self.len()
            || rows.sequence.len() != crate::staged_source::len(&self.by_sequence)
            || rows.indexes.len() != crate::staged_source::len(&self.indexes)
        {
            return false;
        }
        for (id, owner) in &rows.rows {
            let Some(native) = self.row(*id) else {
                return false;
            };
            let Some(origin) = owner._charge.statement_origin() else {
                return false;
            };
            let StatementOriginOwner::Row(original) = &origin.0 else {
                return false;
            };
            if !native.ptr_eq(original)
                || owner.sequence != native.sequence()
                || rows.sequence.get(&owner.sequence) != Some(id)
                || owner.keys.len() != native.keys().count()
                || !owner.keys.iter().zip(native.keys()).all(
                    |((name, key), (native_name, native_key))| {
                        name == native_name && key == native_key
                    },
                )
                || !owner.schema.has_native_origin(native.schema())
            {
                return false;
            }
        }
        for (sequence, id) in &rows.sequence {
            if crate::staged_source::get_by(&self.by_sequence, |key| sequence.cmp(key)) != Some(id)
            {
                return false;
            }
        }
        for (name, index) in &rows.indexes {
            let Some(native) = self.index(name) else {
                return false;
            };
            if !index.schema.has_native_origin(&native.schema)
                || index.postings.len() != crate::staged_source::len(&native.postings)
            {
                return false;
            }
            for (key, id) in &index.postings {
                let StatementIndexKey::Row(position) = key else {
                    return false;
                };
                if crate::staged_source::get_by(&native.postings, |key| {
                    crate::staged_source::compare_statement_keys(
                        &position.0,
                        1,
                        Some(position.1),
                        &key.position().0,
                        1,
                        Some(key.position().1),
                    )
                }) != Some(id)
                {
                    return false;
                }
            }
        }
        true
    }
}
impl NativeStatementRoot {
    pub(crate) fn first_row(&self) -> Option<&NativeEagerRow> {
        crate::staged_source::seek_by(&self.by_id, false, |_| Ordering::Equal).map(|(_, row)| row)
    }
}

/// Native carriers report actual invoice unions; unknown provider controls
/// propagate through retained_union_bytes instead of becoming a zero total.
impl NativeEagerRow {
    pub(crate) fn visit_retained_debits<'a>(
        &'a self,
        visit: &mut dyn FnMut(crate::staged_source::RetainedDebit<'a>),
    ) {
        self.0.visit_retained_debits(visit);
        self.0.original.visit_retained_debits(visit);
        self.0.keys.visit_retained_debits(visit);
        for (name, key) in self.0.keys.as_slice() {
            name.visit_retained_debits(visit);
            key.charge.visit_retained_debits(visit);
        }
        self.schema().visit_retained_debits(visit);
    }
}
impl<T> NativeImage<T> {
    pub(crate) fn visit_control_debits<'a>(
        &'a self,
        visit: &mut dyn FnMut(crate::staged_source::RetainedDebit<'a>),
    ) {
        self.data
            .as_ref()
            .expect("live native image")
            .visit_retained_debits(visit);
        if let Some(grant) = &self.copy_charge {
            grant.visit_retained_debits(visit);
        }
    }
}
impl<T> NativeImageRestore<T> {
    pub(crate) fn visit_control_debits<'a>(
        &'a self,
        visit: &mut dyn FnMut(crate::staged_source::RetainedDebit<'a>),
    ) {
        self.data
            .as_ref()
            .expect("live restore pin")
            .visit_retained_debits(visit);
        if let Some(grant) = &self.copy_charge {
            grant.visit_retained_debits(visit);
        }
    }
}
impl NativeStatementRoot {
    pub(crate) fn visit_retained_debits<'a>(
        &'a self,
        visit: &mut dyn FnMut(crate::staged_source::RetainedDebit<'a>),
    ) {
        use crate::staged_source::{seek_by, visit_link_debits};
        visit_link_debits(&self.by_id, visit);
        visit_link_debits(&self.by_sequence, visit);
        visit_link_debits(&self.indexes, visit);
        let mut after = None;
        while let Some((id, row)) = seek_by(&self.by_id, false, |id| {
            if after.is_some_and(|after| *id <= after) {
                Ordering::Less
            } else {
                Ordering::Equal
            }
        }) {
            row.visit_retained_debits(visit);
            after = Some(*id);
        }
        let mut after_name = None;
        while let Some((name, index)) = seek_by(&self.indexes, false, |name| {
            if after_name.is_some_and(|after| name.as_str() <= after) {
                Ordering::Less
            } else {
                Ordering::Equal
            }
        }) {
            name.visit_retained_debits(visit);
            index.visit_retained_debits(visit);
            index.schema.visit_retained_debits(visit);
            visit_link_debits(&index.postings, visit);
            let mut after_key = None;
            while let Some((key, _)) = seek_by(&index.postings, false, |key| {
                if after_key.is_some_and(|after| key <= after) {
                    Ordering::Less
                } else {
                    Ordering::Equal
                }
            }) {
                key.0.visit_retained_debits(visit);
                key.0._charge.visit_retained_debits(visit);
                after_key = Some(key);
            }
            after_name = Some(name.as_str());
        }
    }
}
impl TerminalStatementView {
    fn retained_union(&self) -> Option<usize> {
        crate::staged_source::retained_union_bytes(|visit| {
            self.image.visit_control_debits(visit);
            self.image.get().visit_retained_debits(visit);
        })
    }
}
impl TerminalStatementAlias {
    fn retained_union(&self, rows: &StatementRows) -> Option<usize> {
        let state = self.state();
        // Non-native host charges retain their original formula, but an opaque
        // provider cannot establish a whole-invoice union from it.
        if rows
            .rows
            .values()
            .any(|row| row._charge.statement_origin().is_none())
            || rows
                .indexes
                .values()
                .any(|index| index.schema.native_origin().is_none())
            || rows.copy_charge.is_some()
        {
            return None;
        }
        crate::staged_source::retained_union_bytes(|visit| {
            if let Some(image) = &state.image {
                image.visit_control_debits(visit);
                image.get().visit_retained_debits(visit);
            }
            if let Some(pin) = &state.pin {
                pin.visit_control_debits(visit);
                pin.get().visit_retained_debits(visit);
            }
            for owner in rows.rows.values() {
                if let Some(StatementOrigin(StatementOriginOwner::Row(row))) =
                    owner._charge.statement_origin()
                {
                    row.visit_retained_debits(visit);
                }
                if let Some(schema) = owner.schema.native_origin() {
                    schema.visit_retained_debits(visit);
                }
            }
            for index in rows.indexes.values() {
                if let Some(schema) = index.schema.native_origin() {
                    schema.visit_retained_debits(visit);
                }
            }
        })
    }
}

/// A private borrowed view never creates or retains a public std shell.
// activated by bounded-storage S3/S4; remove allow at activation
#[allow(dead_code)]
pub(crate) struct NativeStatementBorrow<'a> {
    root: &'a NativeStatementRoot,
}
// activated by bounded-storage S3/S4; remove allow at activation
#[allow(dead_code)]
impl<'a> NativeStatementBorrow<'a> {
    pub(crate) fn new(root: &'a NativeStatementRoot) -> Self {
        Self { root }
    }
    pub(crate) fn row(&self, id: RowId) -> Option<&'a VersionedRow> {
        self.root.row(id).map(NativeEagerRow::row)
    }
    pub(crate) fn len(&self) -> usize {
        self.root.len()
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub(crate) fn contains(&self, id: &RowId) -> bool {
        self.root.row(*id).is_some()
    }
    pub(crate) fn next_unordered(&self, after: Option<usize>) -> Option<(usize, &'a VersionedRow)> {
        self.root.next_unordered(after)
    }
    pub(crate) fn has_index_for(&self, name: &str, meta: &contextdb_core::TableMeta) -> bool {
        self.root.has_index_for(name, meta)
    }
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn next_ordered_in_run(
        &self,
        name: &str,
        reverse: bool,
        after: Option<&(IndexKey, RowId)>,
        start: &Bound<IndexKey>,
        end: &Bound<IndexKey>,
        prefix_len: usize,
    ) -> Option<(&'a (IndexKey, RowId), &'a VersionedRow)> {
        self.root
            .next_ordered_in_run(name, reverse, after, start, end, prefix_len)
    }
}
impl<T> NativeImage<T> {
    pub(crate) fn semantic_holders(&self) -> usize {
        self.data
            .as_ref()
            .expect("live native image")
            .holders
            .load(std::sync::atomic::Ordering::Acquire)
    }
}
// activated by bounded-storage S3/S4; remove allow at activation
#[allow(dead_code)]
impl NativeStatementBorrow<'_> {
    pub(crate) fn rows(&self) -> impl Iterator<Item = &VersionedRow> {
        let mut after = None;
        std::iter::from_fn(move || {
            let (sequence, row) = self.next_unordered(after)?;
            after = Some(sequence);
            Some(row)
        })
    }
    pub(crate) fn next_ordered(
        &self,
        name: &str,
        reverse: bool,
        after: Option<&(IndexKey, RowId)>,
    ) -> Option<(&(IndexKey, RowId), &VersionedRow)> {
        self.next_ordered_in_run(
            name,
            reverse,
            after,
            &Bound::Unbounded,
            &Bound::Unbounded,
            0,
        )
    }
}

impl NativeStatementRoot {
    pub(crate) fn prepare_exact_row(
        &self,
        row: NativeEagerRow,
        funder: &crate::staged_source::SourceFunder,
    ) -> Result<NativeImage<Self>> {
        use crate::staged_source::{Entry, FundedArc, insert, remove_by};
        if row.sequence() >= self.next_sequence {
            return Err(contextdb_core::Error::Other(
                "exact statement sequence mismatch".into(),
            ));
        }
        let next_sequence = self.next_sequence;
        let id = row.row().row_id;
        let mut by_sequence = self.by_sequence.clone();
        let mut indexes = self.indexes.clone();
        if let Some(prior) = self.row(id) {
            by_sequence = remove_by(&by_sequence, &|key| prior.sequence().cmp(key), funder)?;
            indexes = Self::remove_postings(indexes, prior, funder)?;
        }
        for (name, key) in row.keys() {
            let prior = crate::staged_source::get_by(&indexes, |key| name.cmp(key.as_str()));
            let postings = prior
                .filter(|index| index.schema.matches_index(row.schema(), name))
                .and_then(|index| index.postings.clone());
            let postings = insert(
                &postings,
                FundedArc::try_new(
                    Entry {
                        key: NativeStatementKey::prepare(key, id, funder)?,
                        value: id,
                    },
                    funder.clone(),
                )?,
                funder,
            )?;
            let domain = FundedArc::try_new(
                NativeStatementIndex {
                    schema: row.schema().clone(),
                    postings,
                },
                funder.clone(),
            )?;
            let name = crate::staged_source::SourceName::new(name, funder)?;
            indexes = insert(
                &indexes,
                FundedArc::try_new(
                    Entry {
                        key: name,
                        value: domain,
                    },
                    funder.clone(),
                )?,
                funder,
            )?;
        }
        let by_sequence = insert(
            &by_sequence,
            FundedArc::try_new(
                Entry {
                    key: row.sequence(),
                    value: id,
                },
                funder.clone(),
            )?,
            funder,
        )?;
        let by_id = insert(
            &self.by_id,
            FundedArc::try_new(
                Entry {
                    key: id,
                    value: row,
                },
                funder.clone(),
            )?,
            funder,
        )?;
        NativeImage::prepare(
            Self {
                by_id,
                by_sequence,
                indexes,
                next_sequence,
            },
            funder,
        )
    }
}

impl NativeEagerRow {
    pub(crate) fn prepare_exact_cell(
        cell: &crate::staged_source::RowCellHandle,
        funder: &crate::staged_source::SourceFunder,
    ) -> Result<Self> {
        let custody = cell.custody.lock();
        let Some(crate::staged_source::RowCustody::Sealed { row, .. }) = custody.as_ref() else {
            return Err(contextdb_core::Error::Other(
                "unsealed native exact cell".into(),
            ));
        };
        let original = funder.copy_eager_row(row)?;
        let control =
            crate::staged_source::FundedArc::<NativeEagerRowBody>::prepare(funder.clone())?;
        let mut keys = crate::staged_source::SourceBuffer::with_capacity(cell.keys.len(), funder)?;
        for (name, key) in cell.keys.as_slice() {
            keys.push((name.clone(), funder.copy_key(&key.position.0)?));
        }
        Ok(Self(crate::staged_source::FundedArc::from_prepared(
            NativeEagerRowBody {
                original,
                sequence: cell.identity.sequence,
                keys,
                schema: cell.identity.schema.clone(),
            },
            control,
        )))
    }
}

impl NativeEagerRow {
    pub(crate) fn validate_exact_funder(
        &self,
        funder: &crate::staged_source::SourceFunder,
    ) -> Result<()> {
        if !self.0.is_funded_by(funder) {
            return Err(contextdb_core::Error::Other(
                "native exact row control funding mismatch".into(),
            ));
        }
        self.0.original.validate_exact_funder(funder)
    }
}

impl NativeEagerRow {
    pub(crate) fn copy_original_row_for(
        &self,
        funder: &crate::staged_source::SourceFunder,
    ) -> Result<crate::staged_source::AdmittedSourceRow> {
        self.validate_exact_funder(funder)?;
        self.0
            .original
            .original_copy
            .copy_original_row_for(self.row(), funder)
    }
}

impl NativeEagerRow {
    pub(crate) fn copy_original_vector_input_for(
        &self,
        input: crate::staged_source::BorrowedOriginalVectorCopyInput<'_>,
        funder: &crate::staged_source::SourceFunder,
    ) -> Result<crate::staged_source::AdmittedEagerSourceVector> {
        self.validate_exact_funder(funder)?;
        if self.row().row_id != input.row_id()
            || self.row().created_tx != input.created_tx()
            || self.row().deleted_tx != input.deleted_tx()
            || self.row().lsn != input.lsn()
            || !self.schema().layout().iter().any(|(column, kind)| {
                column.as_str() == input.column()
                    && matches!(kind, contextdb_core::ColumnType::Vector(dimension)
                        if *dimension == input.vector().len())
            })
        {
            return Err(crate::ordered_staged::exact_native_error());
        }
        self.0
            .original
            .original_copy
            .copy_original_eager_vector_input_for(input, funder)
    }
}

impl NativeEagerRow {
    pub(crate) fn copy_changed_original_row_for(
        &self,
        expected: &VersionedRow,
        replacement: &VersionedRow,
        funder: &crate::staged_source::SourceFunder,
    ) -> Result<crate::staged_source::AdmittedSourceRow> {
        self.validate_exact_funder(funder)?;
        if !crate::staged_source::changed_original_rows_identical(self.row(), expected) {
            return Err(crate::ordered_staged::exact_native_error());
        }
        self.0
            .original
            .original_copy
            .copy_original_row_for(replacement, funder)
    }

    pub(crate) fn copy_changed_original_eager_row_for(
        &self,
        expected: &VersionedRow,
        replacement: &VersionedRow,
        funder: &crate::staged_source::SourceFunder,
    ) -> Result<crate::staged_source::AdmittedEagerSourceRow> {
        let copied = self.copy_changed_original_row_for(expected, replacement, funder)?;
        Ok(crate::staged_source::AdmittedEagerSourceRow::new(
            copied.row,
            copied.invoice,
            self.0.original.original_copy.clone(),
        ))
    }

    pub(crate) fn prepare_changed_exact_cell(
        cell: &crate::staged_source::RowCellHandle,
        original: crate::staged_source::AdmittedEagerSourceRow,
        funder: &crate::staged_source::SourceFunder,
    ) -> Result<Self> {
        original.validate_exact_funder(funder)?;
        if !cell.identity.matches_row(&original.row) {
            return Err(crate::ordered_staged::exact_native_error());
        }
        let control =
            crate::staged_source::FundedArc::<NativeEagerRowBody>::prepare(funder.clone())?;
        let mut keys = crate::staged_source::SourceBuffer::with_capacity(cell.keys.len(), funder)?;
        for (name, key) in cell.keys.as_slice() {
            keys.push((name.clone(), funder.copy_key(&key.position.0)?));
        }
        Ok(Self(crate::staged_source::FundedArc::from_prepared(
            NativeEagerRowBody {
                original,
                sequence: cell.identity.sequence,
                keys,
                schema: cell.identity.schema.clone(),
            },
            control,
        )))
    }
}
