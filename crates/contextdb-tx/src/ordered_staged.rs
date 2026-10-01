//! Incrementally maintained index order for admitted transaction rows.
use contextdb_core::{
    ColumnType, DirectedValue, IndexDecl, IndexKey, Result, RowId, TableMeta, TxId, Value,
};
use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::ops::Bound;
use std::sync::Arc;

/// Engine admission ownership; dropping the final owner releases the charge.
/// Implemented by the engine's existing owned reservation, never a tx cache.
pub trait StagedPostingCharge: Debug + Send + Sync {
    fn retained_bytes(&self) -> usize {
        0
    }
    fn reserve_copy(&self, _bytes: usize) -> Result<Arc<dyn StagedPostingCharge>> {
        Err(contextdb_core::Error::Other(
            "staged snapshot copying requires admission".into(),
        ))
    }
}

/// Exact admitted schema identity for a staged comparator domain.
#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct StagedSchema {
    layout: Vec<(String, ColumnType)>,
    indexes: Vec<IndexDecl>,
    charge: Arc<dyn StagedPostingCharge>,
}
impl StagedSchema {
    pub fn from_meta(meta: &TableMeta, charge: Arc<dyn StagedPostingCharge>) -> Self {
        Self {
            layout: meta
                .columns
                .iter()
                .map(|c| (c.name.clone(), c.column_type.clone()))
                .collect(),
            indexes: meta.indexes.clone(),
            charge,
        }
    }
    pub fn matches_index(&self, other: &Self, index: &str) -> bool {
        self.layout == other.layout
            && self.indexes.iter().find(|i| i.name == index)
                == other.indexes.iter().find(|i| i.name == index)
    }
    pub fn matches_meta(&self, meta: &TableMeta, index: &str) -> bool {
        self.layout.len() == meta.columns.len()
            && self
                .layout
                .iter()
                .zip(&meta.columns)
                .all(|((name, kind), c)| name == &c.name && kind == &c.column_type)
            && self.indexes.iter().find(|i| i.name == index)
                == meta.indexes.iter().find(|i| i.name == index)
    }
    pub fn bytes_for_meta(meta: &TableMeta) -> usize {
        std::mem::size_of::<Self>()
            .saturating_add(
                meta.columns
                    .capacity()
                    .saturating_mul(std::mem::size_of::<(String, ColumnType)>()),
            )
            .saturating_add(
                meta.columns
                    .iter()
                    .map(|c| c.name.capacity())
                    .sum::<usize>(),
            )
            .saturating_add(
                meta.indexes
                    .capacity()
                    .saturating_mul(std::mem::size_of::<IndexDecl>()),
            )
            .saturating_add(
                meta.indexes
                    .iter()
                    .map(IndexDecl::estimated_bytes)
                    .sum::<usize>(),
            )
    }
    pub fn retained_bytes(&self) -> usize {
        self.charge.retained_bytes()
    }
}

/// Owned key slots plus scalar payload allocation, shared by posting/COW doors.
#[doc(hidden)]
pub fn staged_key_bytes(key: &IndexKey) -> usize {
    key.iter().fold(
        key.capacity()
            .saturating_mul(std::mem::size_of::<DirectedValue>()),
        |bytes, value| {
            let value = match value {
                DirectedValue::Asc(v) => &v.0,
                DirectedValue::Desc(v) => &v.0,
            };
            let payload = match value {
                Value::Text(text) => value
                    .estimated_bytes()
                    .saturating_add(text.capacity().saturating_sub(text.len())),
                Value::Vector(items) => value.estimated_bytes().saturating_add(
                    items
                        .capacity()
                        .saturating_sub(items.len())
                        .saturating_mul(std::mem::size_of::<f32>()),
                ),
                _ => value.estimated_bytes(),
            };
            bytes.saturating_add(payload)
        },
    )
}

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
    schema: Option<Arc<StagedSchema>>,
}

/// Retained postings contain identities/positions only, never row payloads.
/// A write-set clone is operation output: its caller must admit these bytes
/// along with the rows it copies, using `retained_bytes` before cloning.
#[derive(Debug, Default, Clone)]
pub struct OrderedStagedRows {
    tables: BTreeMap<String, BTreeMap<String, IndexPostings>>,
    charges: BTreeMap<String, RowCharges>,
    statement_rows: BTreeMap<String, Arc<crate::StatementRows>>,
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

pub struct OrderedStagedPrefixCursor<'a> {
    range: std::collections::btree_map::Range<'a, (IndexKey, RowId), StagedPosition>,
    prefix: &'a IndexKey,
    finished: bool,
}
impl Iterator for OrderedStagedPrefixCursor<'_> {
    type Item = StagedPosition;
    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        let ((key, _), position) = self.range.next()?;
        if !key.starts_with(self.prefix) {
            self.finished = true;
            return None;
        }
        Some(*position)
    }
}

impl OrderedStagedRows {
    pub fn statement_rows(&self, table: &str) -> Option<Arc<crate::StatementRows>> {
        self.statement_rows.get(table).cloned()
    }
    pub fn prepare_statement_row(
        &mut self,
        table: &str,
        row: &contextdb_core::VersionedRow,
        keys: &[(String, IndexKey)],
        schema: Arc<StagedSchema>,
        mut reserve: impl FnMut(usize) -> Result<Arc<dyn StagedPostingCharge>>,
    ) -> Result<crate::statement_rows::PreparedStatementRow> {
        if let Some(snapshot) = self.statement_rows.get(table) {
            return crate::StatementRows::prepare(snapshot, row, keys, schema, reserve);
        }
        let charge = reserve(
            std::mem::size_of::<crate::StatementRows>()
                .saturating_add(table.len())
                .saturating_add(192),
        )?;
        let snapshot = Arc::new(crate::StatementRows::with_control(charge));
        let mut prepared = crate::StatementRows::prepare(&snapshot, row, keys, schema, reserve)?;
        prepared.new_table = Some(snapshot);
        Ok(prepared)
    }
    pub fn publish_statement_row(
        &mut self,
        table: &str,
        mut prepared: crate::statement_rows::PreparedStatementRow,
    ) {
        if let Some(snapshot) = prepared.new_table.take() {
            self.statement_rows.insert(table.to_owned(), snapshot);
        }
        crate::StatementRows::publish(
            self.statement_rows
                .get_mut(table)
                .expect("prepared table owner"),
            prepared,
        );
    }
    pub fn remove_statement_row(&mut self, table: &str, row: RowId) -> Result<()> {
        if let Some(snapshot) = self.statement_rows.get_mut(table) {
            crate::StatementRows::remove(snapshot, row)?;
        }
        Ok(())
    }
    pub fn has_index_for(&self, table: &str, index: &str, meta: &TableMeta) -> bool {
        self.statement_rows
            .get(table)
            .is_some_and(|rows| rows.has_index_for(index, meta))
    }
    /// Prepare all table copies before publishing any removal.
    pub fn prepare_statement_removals(
        &self,
        selected: &std::collections::BTreeSet<(String, RowId)>,
    ) -> Result<Vec<(String, crate::statement_rows::PreparedStatementRemoval)>> {
        let mut prepared = Vec::new();
        for (table, snapshot) in &self.statement_rows {
            if selected
                .iter()
                .any(|(name, row)| name == table && snapshot.contains(row))
            {
                prepared.push((
                    table.clone(),
                    crate::StatementRows::prepare_removal(snapshot)?,
                ));
            }
        }
        Ok(prepared)
    }
    pub fn publish_statement_removals(
        &mut self,
        prepared: Vec<(String, crate::statement_rows::PreparedStatementRemoval)>,
        selected: &std::collections::BTreeSet<(String, RowId)>,
    ) {
        for (table, replacement) in prepared {
            crate::StatementRows::publish_removal(
                self.statement_rows
                    .get_mut(&table)
                    .expect("prepared statement table"),
                replacement,
                selected
                    .iter()
                    .filter(|(name, _)| name == &table)
                    .map(|(_, row)| *row),
            );
        }
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
        schema: &Arc<StagedSchema>,
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
                    .saturating_add(staged_key_bytes(key).saturating_mul(2))
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
            if postings
                .schema
                .as_ref()
                .is_some_and(|old| !old.matches_index(schema, index))
            {
                postings.ordered.clear();
                postings.latest.clear();
            }
            postings.schema = Some(schema.clone());
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

    /// The equality prefix is caller scratch. No row values are scanned or copied.
    pub fn prefix_cursor<'a>(
        &'a self,
        table: &str,
        index: &str,
        prefix: &'a IndexKey,
    ) -> Option<OrderedStagedPrefixCursor<'a>> {
        let postings = self.tables.get(table)?.get(index)?;
        let lower = (prefix.clone(), RowId(0));
        Some(OrderedStagedPrefixCursor {
            range: postings
                .ordered
                .range((Bound::Included(&lower), Bound::Unbounded)),
            prefix,
            finished: false,
        })
    }

    pub fn contains_row(&self, table: &str, row: RowId) -> bool {
        self.charges
            .get(table)
            .is_some_and(|rows| rows.contains_key(&row))
    }

    pub fn retained_bytes(&self) -> usize {
        self.charges
            .values()
            .flat_map(|rows| rows.values())
            .fold(0usize, |bytes, (charge, _)| bytes.saturating_add(*charge))
            .saturating_add(
                self.statement_rows
                    .values()
                    .map(|rows| rows.retained_bytes())
                    .sum::<usize>(),
            )
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
