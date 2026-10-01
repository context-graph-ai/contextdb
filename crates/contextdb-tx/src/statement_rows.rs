//! Immutable admitted statement views of staged table rows and index order.
use crate::StagedPostingCharge;
use contextdb_core::{IndexKey, Result, RowId, VersionedRow};
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

#[derive(Debug, Clone, Default)]
pub struct StatementRows {
    rows: BTreeMap<RowId, Arc<RowOwner>>,
    sequence: BTreeMap<usize, RowId>,
    indexes: BTreeMap<String, BTreeMap<(IndexKey, RowId), RowId>>,
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
        self.indexes.contains_key(index)
    }
    pub fn next_ordered(
        &self,
        index: &str,
        reverse: bool,
        after: Option<&(IndexKey, RowId)>,
    ) -> Option<(&(IndexKey, RowId), &VersionedRow)> {
        let postings = self.indexes.get(index)?;
        let edge = after.map_or(Bound::Unbounded, Bound::Excluded);
        let entry = if reverse {
            postings.range((Bound::Unbounded, edge)).next_back()
        } else {
            postings.range((edge, Bound::Unbounded)).next()
        }?;
        Some((entry.0, &self.rows.get(entry.1)?.row))
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
                .insert((key.clone(), row_id), row_id);
        }
        rows.sequence.insert(owner.sequence, row_id);
        rows.rows.insert(row_id, owner);
    }
    fn remove_private(&mut self, row: RowId) {
        if let Some(owner) = self.rows.remove(&row) {
            self.sequence.remove(&owner.sequence);
            for (name, key) in &owner.keys {
                if let Some(index) = self.indexes.get_mut(name) {
                    index.remove(&(key.clone(), row));
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
