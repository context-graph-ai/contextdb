//! Incrementally maintained index order for admitted transaction rows.
use contextdb_core::{DirectedValue, IndexKey, Result, RowId, TxId};
use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::ops::Bound;
use std::sync::Arc;

/// Engine admission ownership; dropping the final owner releases the charge.
/// Implemented by the engine's existing owned reservation, never a tx cache.
pub trait StagedPostingCharge: Debug + Send + Sync {}

pub type OrderedStagedBounds<'a> = (Bound<&'a (IndexKey, RowId)>, Bound<&'a (IndexKey, RowId)>);
type RowCharges = BTreeMap<RowId, (usize, Arc<dyn StagedPostingCharge>)>;

#[derive(Debug, Clone, Copy)]
pub struct StagedPosition {
    pub position: usize,
    pub row_id: RowId,
    pub created_tx: TxId,
}

#[derive(Debug, Default, Clone)]
struct IndexPostings {
    ordered: BTreeMap<(IndexKey, RowId), StagedPosition>,
    latest: BTreeMap<RowId, IndexKey>,
}

/// Retained postings contain identities/positions only, never row payloads.
/// A write-set clone is operation output: its caller must admit these bytes
/// along with the rows it copies, using `retained_bytes` before cloning.
#[derive(Debug, Default, Clone)]
pub struct OrderedStagedRows {
    tables: BTreeMap<String, BTreeMap<String, IndexPostings>>,
    charges: BTreeMap<String, RowCharges>,
}

pub struct OrderedStagedCursor<'a> {
    range: std::collections::btree_map::Range<'a, (IndexKey, RowId), StagedPosition>,
    reverse: bool,
}
impl Iterator for OrderedStagedCursor<'_> {
    type Item = StagedPosition;
    fn next(&mut self) -> Option<Self::Item> {
        let entry = if self.reverse {
            self.range.next_back()
        } else {
            self.range.next()
        };
        entry.map(|(_, position)| *position)
    }
}

impl OrderedStagedRows {
    fn key_bytes(key: &IndexKey) -> usize {
        key.iter().fold(
            key.capacity()
                .saturating_mul(std::mem::size_of::<DirectedValue>()),
            |bytes, value| {
                let value = match value {
                    DirectedValue::Asc(value) => &value.0,
                    DirectedValue::Desc(value) => &value.0,
                };
                bytes.saturating_add(value.estimated_bytes())
            },
        )
    }

    /// Input keys have already been projected under engine scratch admission.
    /// Reserve retained clones and map/posting overhead before any mutation.
    /// The control allowance also reserves canonicalization position scratch.
    /// `keys` must describe the complete current declared-index set for this row.
    pub fn stage<F>(
        &mut self,
        table: &str,
        position: StagedPosition,
        keys: &[(String, IndexKey)],
        mut reserve: F,
    ) -> Result<()>
    where
        F: FnMut(usize) -> Result<Arc<dyn StagedPostingCharge>>,
    {
        let bytes = keys.iter().fold(
            256usize.saturating_add(table.len()),
            |bytes, (index, key)| {
                bytes
                    .saturating_add(512)
                    .saturating_add(index.len())
                    .saturating_add(Self::key_bytes(key).saturating_mul(2))
            },
        );
        let charge = reserve(bytes)?;
        let indexes = self.tables.entry(table.to_owned()).or_default();
        for postings in indexes.values_mut() {
            if let Some(old_key) = postings.latest.remove(&position.row_id) {
                postings.ordered.remove(&(old_key, position.row_id));
            }
        }
        for (index, key) in keys {
            let postings = indexes.entry(index.clone()).or_default();
            postings.latest.insert(position.row_id, key.clone());
            postings
                .ordered
                .insert((key.clone(), position.row_id), position);
        }
        indexes.retain(|_, postings| !postings.ordered.is_empty());
        self.charges
            .entry(table.to_owned())
            .or_default()
            .insert(position.row_id, (bytes, charge));
        Ok(())
    }

    /// Remove the old posting and release its recorded admission owner.
    pub fn remove(&mut self, table: &str, row: RowId) {
        if let Some(indexes) = self.tables.get_mut(table) {
            for postings in indexes.values_mut() {
                if let Some(key) = postings.latest.remove(&row) {
                    postings.ordered.remove(&(key, row));
                }
            }
        }
        if let Some(charges) = self.charges.get_mut(table) {
            charges.remove(&row);
        }
        self.discard_empty_maps();
    }

    fn discard_empty_maps(&mut self) {
        self.tables.retain(|_, indexes| {
            indexes.retain(|_, postings| !postings.ordered.is_empty());
            !indexes.is_empty()
        });
        self.charges.retain(|_, rows| !rows.is_empty());
    }

    pub fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }

    /// A borrowed range freezes mutation through the write-set mutex. Each
    /// step returns only a position; the consumer admits its row clone first.
    /// Bounds use the index's DirectedValue ordering, including tie-break id.
    pub fn cursor<'a>(
        &'a self,
        table: &str,
        index: &str,
        lower: Bound<&(IndexKey, RowId)>,
        upper: Bound<&(IndexKey, RowId)>,
        reverse: bool,
    ) -> Option<OrderedStagedCursor<'a>> {
        let postings = self.tables.get(table)?.get(index)?;
        Some(OrderedStagedCursor {
            range: postings.ordered.range((lower, upper)),
            reverse,
        })
    }

    pub fn retained_bytes(&self) -> usize {
        self.charges
            .values()
            .flat_map(|rows| rows.values())
            .fold(0usize, |bytes, (charge, _)| bytes.saturating_add(*charge))
    }

    /// Canonicalization only changes positions/visibility identity, never keys.
    pub(crate) fn rebind(&mut self, positions: &HashMap<&str, HashMap<RowId, (usize, TxId)>>) {
        for (table, indexes) in &mut self.tables {
            for postings in indexes.values_mut() {
                postings.ordered.retain(|(_, row_id), position| {
                    if let Some((new_position, tx)) = positions
                        .get(table.as_str())
                        .and_then(|rows| rows.get(row_id))
                    {
                        position.position = *new_position;
                        position.created_tx = *tx;
                        true
                    } else {
                        false
                    }
                });
                postings.latest.retain(|row, _| {
                    positions
                        .get(table.as_str())
                        .is_some_and(|rows| rows.contains_key(row))
                });
            }
        }
        for (table, rows) in &mut self.charges {
            rows.retain(|row, _| {
                positions
                    .get(table.as_str())
                    .is_some_and(|rows| rows.contains_key(row))
            });
        }
        self.discard_empty_maps();
    }

    pub(crate) fn reassign_tx(&mut self, from: TxId, to: TxId) {
        for indexes in self.tables.values_mut() {
            for postings in indexes.values_mut() {
                for position in postings.ordered.values_mut() {
                    if position.created_tx == from {
                        position.created_tx = to;
                    }
                }
            }
        }
    }
}
