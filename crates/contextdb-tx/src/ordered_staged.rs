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
    #[doc(hidden)]
    fn transfer_back(&self) -> usize {
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

/// An immutable vector selected at the mutation boundary. Statement capture
/// shares this capability; it never retains a mutable ordinary-write position.
#[doc(hidden)]
#[derive(Debug)]
pub struct StagedVectorCapability {
    row_id: RowId,
    column: String,
    payload: StagedVectorPayload,
    _charge: Arc<dyn StagedPostingCharge>,
}
#[derive(Debug)]
enum StagedVectorPayload {
    Scalar(Arc<crate::statement_rows::RowOwner>),
    Owned(Arc<AdmittedVectorPayload>),
}
#[derive(Debug)]
struct AdmittedVectorPayload {
    values: Vec<f32>,
    _charge: Arc<dyn StagedPostingCharge>,
}
impl StagedVectorCapability {
    pub fn row_id(&self) -> RowId {
        self.row_id
    }
    pub fn vector(&self) -> &[f32] {
        match &self.payload {
            StagedVectorPayload::Scalar(owner) => match owner.row.values.get(&self.column) {
                Some(Value::Vector(values)) => values,
                _ => unreachable!("immutable scalar vector owner"),
            },
            StagedVectorPayload::Owned(owner) => &owner.values,
        }
    }
}
#[derive(Debug, Clone)]
struct StagedVectorIndex {
    rows: BTreeMap<RowId, Arc<StagedVectorCapability>>,
    _charge: Arc<dyn StagedPostingCharge>,
}
#[doc(hidden)]
pub struct PreparedStagedVector {
    capability: Arc<StagedVectorCapability>,
    new_index: Option<StagedVectorIndex>,
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
#[derive(Debug, Default)]
pub struct OrderedStagedRows {
    tables: BTreeMap<String, BTreeMap<String, IndexPostings>>,
    charges: BTreeMap<String, RowCharges>,
    statement_rows: BTreeMap<String, Arc<crate::StatementRows>>,
    undo: Vec<StagedMutationUndo>,
    checkpoint_depth: usize,
    vector_undo: Vec<StagedVectorUndo>,
    journal_charges: Vec<Arc<dyn StagedPostingCharge>>,
    vector_prepared: usize,
    vectors: BTreeMap<String, BTreeMap<String, StagedVectorIndex>>,
}
impl Clone for OrderedStagedRows {
    fn clone(&self) -> Self {
        Self {
            tables: self.tables.clone(),
            charges: self.charges.clone(),
            statement_rows: self.statement_rows.clone(),
            undo: Vec::new(),
            checkpoint_depth: 0,
            vector_undo: Vec::new(),
            journal_charges: Vec::new(),
            vector_prepared: 0,
            vectors: self.vectors.clone(),
        }
    }
}
#[doc(hidden)]
#[derive(Debug)]
pub struct PreparedVectorUndo {
    index: contextdb_core::VectorIndexRef,
    row_id: RowId,
    inserts: Vec<(usize, contextdb_core::VectorEntry)>,
    moves: Vec<(usize, (contextdb_core::VectorIndexRef, RowId, RowId, TxId))>,
    deletes: Vec<(usize, (contextdb_core::VectorIndexRef, RowId, TxId))>,
    vector_index_existed: bool,
    old_vector: Option<Arc<StagedVectorCapability>>,
    old_source_vector: Option<Arc<StagedVectorCapability>>,
    installed_bytes: usize,
    preserve_inserts: bool,
    moving: Option<(RowId, Option<usize>)>,
    replacement_positions: Vec<usize>,
    admission: Arc<dyn StagedPostingCharge>,
    _charge: Arc<dyn StagedPostingCharge>,
}
type StagedVectorUndo = PreparedVectorUndo;

#[derive(Debug)]
struct LegacyIndexUndo {
    name: String,
    domain: Option<IndexPostings>,
    schema: Option<Arc<StagedSchema>>,
    key: Option<(IndexKey, StagedPosition)>,
    replace_domain: bool,
}
#[derive(Debug)]
struct StagedMutationUndo {
    table: String,
    row_id: RowId,
    legacy: Vec<LegacyIndexUndo>,
    old_charge: Option<(usize, Arc<dyn StagedPostingCharge>)>,
    statement: Option<crate::statement_rows::StatementRowUndo>,
    table_existed: bool,
    displaced: Vec<(usize, String, contextdb_core::VersionedRow)>,
    prior_admitted: Option<usize>,
    installed_admitted: usize,
    admission: Option<Arc<dyn StagedPostingCharge>>,
    _charge: Arc<dyn StagedPostingCharge>,
}

#[doc(hidden)]
pub struct PreparedStatementRemovals {
    tables: Vec<(String, crate::statement_rows::PreparedStatementRemoval)>,
    _charge: Option<Arc<dyn StagedPostingCharge>>,
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
            return crate::StatementRows::prepare(
                snapshot,
                row,
                keys,
                schema,
                reserve,
                self.checkpoint_depth != 0,
            );
        }
        let charge = reserve(
            std::mem::size_of::<crate::StatementRows>()
                .saturating_add(table.len())
                .saturating_add(192),
        )?;
        let snapshot = Arc::new(crate::StatementRows::with_control(charge));
        let mut prepared = crate::StatementRows::prepare(
            &snapshot,
            row,
            keys,
            schema,
            reserve,
            self.checkpoint_depth != 0,
        )?;
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
        let undo = crate::StatementRows::publish(
            self.statement_rows
                .get_mut(table)
                .expect("prepared table owner"),
            prepared,
        );
        if let Some(undo) = undo {
            self.undo
                .last_mut()
                .expect("prepared staged undo")
                .statement = Some(undo);
        }
    }
    fn retain_journal_charge(&mut self, charge: Arc<dyn StagedPostingCharge>) -> Result<()> {
        self.journal_charges.try_reserve_exact(1).map_err(|_| {
            contextdb_core::Error::Other("journal ownership allocation failed".into())
        })?;
        self.journal_charges.push(charge);
        Ok(())
    }
    #[doc(hidden)]
    pub fn checkpoint_active(&self) -> bool {
        self.checkpoint_depth != 0
    }
    #[doc(hidden)]
    pub fn begin_checkpoint(&mut self) -> [usize; 2] {
        self.checkpoint_depth += 1;
        [self.undo.len(), self.vector_undo.len()]
    }
    #[doc(hidden)]
    pub fn settle_checkpoint(&mut self) {
        self.checkpoint_depth -= 1;
        if self.checkpoint_depth == 0 {
            // Release journal storage before its admitted control owners.
            self.undo = Vec::new();
            self.vector_undo = Vec::new();
            self.journal_charges = Vec::new();
            self.vector_prepared = 0;
        }
    }
    #[allow(clippy::too_many_arguments)]
    #[doc(hidden)]
    pub fn prepare_vector_undo(
        &mut self,
        index: &contextdb_core::VectorIndexRef,
        row_id: RowId,
        inserts: &[contextdb_core::VectorEntry],
        moves: &[(contextdb_core::VectorIndexRef, RowId, RowId, TxId)],
        deletes: &[(contextdb_core::VectorIndexRef, RowId, TxId)],
        admission: Arc<dyn StagedPostingCharge>,
        mut reserve: impl FnMut(usize) -> Result<Arc<dyn StagedPostingCharge>>,
    ) -> Result<Option<PreparedVectorUndo>> {
        if self.checkpoint_depth == 0 {
            return Ok(None);
        }
        let ni = inserts
            .iter()
            .filter(|entry| entry.index == *index && entry.row_id == row_id)
            .count();
        let nm = moves
            .iter()
            .filter(|(pending, _from, to, _)| pending == index && *to == row_id)
            .count();
        let nd = deletes
            .iter()
            .filter(|(pending, row, _)| pending == index && *row == row_id)
            .count();
        let bytes = std::mem::size_of::<PreparedVectorUndo>()
            .saturating_add(index.table.len())
            .saturating_add(index.column.len())
            .saturating_add(
                ni.saturating_mul(std::mem::size_of::<(usize, contextdb_core::VectorEntry)>()),
            )
            .saturating_add(nm.saturating_mul(std::mem::size_of::<(
                usize,
                (contextdb_core::VectorIndexRef, RowId, RowId, TxId),
            )>()))
            .saturating_add(nd.saturating_mul(std::mem::size_of::<(
                usize,
                (contextdb_core::VectorIndexRef, RowId, TxId),
            )>()));
        let bytes = bytes.saturating_add(
            deletes
                .iter()
                .filter(|(pending, row, _)| pending == index && *row == row_id)
                .map(|(pending, _, _)| pending.table.len().saturating_add(pending.column.len()))
                .sum::<usize>(),
        );
        let charge =
            reserve(bytes.saturating_add(std::mem::size_of::<Arc<dyn StagedPostingCharge>>()))?;
        self.retain_journal_charge(charge.clone())?;
        self.vector_prepared += 1;
        self.vector_undo
            .try_reserve_exact(self.vector_prepared.saturating_sub(self.vector_undo.len()))
            .map_err(|_| {
                contextdb_core::Error::Other("vector undo control allocation failed".into())
            })?;
        Ok(Some(PreparedVectorUndo {
            index: index.clone(),
            row_id,
            inserts: Vec::with_capacity(ni),
            moves: Vec::with_capacity(nm),
            deletes: Vec::with_capacity(nd),
            vector_index_existed: self
                .vectors
                .get(&index.table)
                .is_some_and(|columns| columns.contains_key(&index.column)),
            old_vector: self.vector_capability(index, row_id),
            old_source_vector: None,
            installed_bytes: 0,
            preserve_inserts: false,
            moving: None,
            replacement_positions: Vec::new(),
            admission,
            _charge: charge,
        }))
    }
    #[allow(clippy::too_many_arguments)]
    #[doc(hidden)]
    pub fn prepare_vector_move_undo(
        &mut self,
        index: &contextdb_core::VectorIndexRef,
        from: RowId,
        to: RowId,
        inserts: &[contextdb_core::VectorEntry],
        moves: &[(contextdb_core::VectorIndexRef, RowId, RowId, TxId)],
        admission: Arc<dyn StagedPostingCharge>,
        mut reserve: impl FnMut(usize) -> Result<Arc<dyn StagedPostingCharge>>,
    ) -> Result<Option<PreparedVectorUndo>> {
        if self.checkpoint_depth == 0 {
            return Ok(None);
        }
        let moving_position = inserts
            .iter()
            .rposition(|entry| entry.index == *index && entry.row_id == from);
        let n = inserts
            .iter()
            .enumerate()
            .filter(|(position, entry)| {
                entry.index == *index
                    && ((to != from && entry.row_id == to)
                        || (entry.row_id == from && Some(*position) != moving_position))
            })
            .count();
        let nm = moves
            .iter()
            .filter(|(pending, source, destination, _)| {
                pending == index && (*source == from || *destination == from || *destination == to)
            })
            .count();
        let bytes =
            std::mem::size_of::<PreparedVectorUndo>()
                .saturating_add(index.table.len())
                .saturating_add(index.column.len())
                .saturating_add(
                    n.saturating_mul(
                        std::mem::size_of::<(usize, contextdb_core::VectorEntry)>()
                            .saturating_add(std::mem::size_of::<usize>()),
                    )
                    .saturating_add(
                        usize::from(moving_position.is_some()).saturating_mul(
                            std::mem::size_of::<(usize, contextdb_core::VectorEntry)>(),
                        ),
                    ),
                )
                .saturating_add(
                    moves
                        .iter()
                        .filter(|(pending, source, destination, _)| {
                            pending == index
                                && (*source == from || *destination == from || *destination == to)
                        })
                        .map(|(pending, _, _, _)| {
                            std::mem::size_of::<(
                                usize,
                                (contextdb_core::VectorIndexRef, RowId, RowId, TxId),
                            )>()
                            .saturating_add(pending.table.len())
                            .saturating_add(pending.column.len())
                        })
                        .sum::<usize>(),
                );
        let charge =
            reserve(bytes.saturating_add(std::mem::size_of::<Arc<dyn StagedPostingCharge>>()))?;
        self.retain_journal_charge(charge.clone())?;
        self.vector_prepared += 1;
        self.vector_undo
            .try_reserve_exact(self.vector_prepared.saturating_sub(self.vector_undo.len()))
            .map_err(|_| {
                contextdb_core::Error::Other("vector move undo allocation failed".into())
            })?;
        let mut positions = Vec::with_capacity(n);
        // The existing mover retires earlier source entries first, then destination entries.
        for (position, entry) in inserts.iter().enumerate() {
            if entry.index == *index && entry.row_id == from && Some(position) != moving_position {
                positions.push(position);
            }
        }
        for (position, entry) in inserts.iter().enumerate() {
            if entry.index == *index
                && to != from
                && entry.row_id == to
                && Some(position) != moving_position
            {
                positions.push(position);
            }
        }
        let mut prior_moves = Vec::with_capacity(nm);
        for (position, entry) in moves.iter().enumerate() {
            if entry.0 == *index && (entry.1 == from || entry.2 == from || entry.2 == to) {
                prior_moves.push((position, entry.clone()));
            }
        }
        Ok(Some(PreparedVectorUndo {
            index: index.clone(),
            row_id: to,
            inserts: Vec::with_capacity(n.saturating_add(usize::from(moving_position.is_some()))),
            moves: prior_moves,
            deletes: Vec::new(),
            vector_index_existed: self
                .vectors
                .get(&index.table)
                .is_some_and(|columns| columns.contains_key(&index.column)),
            old_vector: self.vector_capability(index, to),
            old_source_vector: self.vector_capability(index, from),
            installed_bytes: 0,
            preserve_inserts: false,
            moving: Some((from, moving_position)),
            replacement_positions: positions,
            admission,
            _charge: charge,
        }))
    }
    #[doc(hidden)]
    pub fn prepare_vector_move_cancellation(
        &mut self,
        index: &contextdb_core::VectorIndexRef,
        row_id: RowId,
        moves: &[(contextdb_core::VectorIndexRef, RowId, RowId, TxId)],
        admission: Arc<dyn StagedPostingCharge>,
        mut reserve: impl FnMut(usize) -> Result<Arc<dyn StagedPostingCharge>>,
    ) -> Result<Option<PreparedVectorUndo>> {
        if self.checkpoint_depth == 0 {
            return Ok(None);
        }
        let selected = |entry: &(contextdb_core::VectorIndexRef, RowId, RowId, TxId)| {
            entry.0 == *index && (entry.1 == row_id || entry.2 == row_id)
        };
        let count = moves.iter().filter(|entry| selected(entry)).count();
        let bytes = std::mem::size_of::<PreparedVectorUndo>()
            .saturating_add(std::mem::size_of::<Arc<dyn StagedPostingCharge>>())
            .saturating_add(index.table.len())
            .saturating_add(index.column.len())
            .saturating_add(
                moves
                    .iter()
                    .filter(|entry| selected(entry))
                    .map(|entry| {
                        std::mem::size_of::<(
                            usize,
                            (contextdb_core::VectorIndexRef, RowId, RowId, TxId),
                        )>()
                        .saturating_add(entry.0.table.len())
                        .saturating_add(entry.0.column.len())
                    })
                    .sum::<usize>(),
            );
        let charge = reserve(bytes)?;
        self.retain_journal_charge(charge.clone())?;
        self.vector_prepared += 1;
        self.vector_undo
            .try_reserve_exact(self.vector_prepared.saturating_sub(self.vector_undo.len()))
            .map_err(|_| {
                contextdb_core::Error::Other("vector cancellation undo allocation failed".into())
            })?;
        let mut prior_moves = Vec::with_capacity(count);
        for (position, entry) in moves.iter().enumerate() {
            if selected(entry) {
                prior_moves.push((position, entry.clone()));
            }
        }
        Ok(Some(PreparedVectorUndo {
            index: index.clone(),
            row_id,
            inserts: Vec::new(),
            moves: prior_moves,
            deletes: Vec::new(),
            vector_index_existed: self
                .vectors
                .get(&index.table)
                .is_some_and(|columns| columns.contains_key(&index.column)),
            old_vector: self.vector_capability(index, row_id),
            old_source_vector: None,
            installed_bytes: 0,
            preserve_inserts: true,
            moving: Some((row_id, None)),
            replacement_positions: Vec::new(),
            admission,
            _charge: charge,
        }))
    }
    #[doc(hidden)]
    pub fn publish_vector_move_undo(
        &mut self,
        mut prepared: PreparedVectorUndo,
        replaced: Vec<contextdb_core::VectorEntry>,
    ) {
        for (position, entry) in prepared.replacement_positions.drain(..).zip(replaced) {
            prepared.inserts.push((position, entry));
        }
        self.vector_undo.push(prepared);
    }
    #[doc(hidden)]
    pub fn publish_vector_undo(
        &mut self,
        mut prepared: PreparedVectorUndo,
        inserts: &mut Vec<contextdb_core::VectorEntry>,
        moves: &mut Vec<(contextdb_core::VectorIndexRef, RowId, RowId, TxId)>,
        deletes: &mut Vec<(contextdb_core::VectorIndexRef, RowId, TxId)>,
        replace: bool,
        installed_bytes: usize,
    ) {
        prepared.installed_bytes = installed_bytes;
        let mut position = 0;
        while position < inserts.len() {
            if inserts[position].index == prepared.index
                && inserts[position].row_id == prepared.row_id
            {
                prepared.inserts.push((position, inserts.remove(position)));
            } else {
                position += 1;
            }
        }
        let mut position = 0;
        while position < moves.len() {
            if moves[position].0 == prepared.index && moves[position].2 == prepared.row_id {
                prepared.moves.push((position, moves.remove(position)));
            } else {
                position += 1;
            }
        }
        // Preserve existing deletes without removing them: unchanged identities
        // are borrowed during preparation and cloned only under admitted control.
        for (position, entry) in deletes.iter().enumerate() {
            if entry.0 == prepared.index && entry.1 == prepared.row_id {
                prepared.deletes.push((position, entry.clone()));
            }
        }
        if replace {
            for (_, (index, source, _, tx)) in &prepared.moves {
                if !deletes
                    .iter()
                    .any(|(pending, row, _)| pending == index && row == source)
                {
                    deletes.push((index.clone(), *source, *tx));
                }
            }
        }
        self.vector_undo.push(prepared);
    }
    #[doc(hidden)]
    pub fn restore_vector_checkpoint(
        &mut self,
        marker: [usize; 2],
        inserts: &mut Vec<contextdb_core::VectorEntry>,
        moves: &mut Vec<(contextdb_core::VectorIndexRef, RowId, RowId, TxId)>,
        deletes: &mut Vec<(contextdb_core::VectorIndexRef, RowId, TxId)>,
        mut release: impl FnMut(usize),
    ) {
        while self.vector_undo.len() > marker[1] {
            let mut undo = self.vector_undo.pop().expect("vector undo");
            release(undo.installed_bytes);
            self.remove_vector_capability(&undo.index, undo.row_id);
            if let Some((from, _)) = undo.moving {
                self.remove_vector_capability(&undo.index, from);
                if let Some(prior) = undo.old_source_vector.take() {
                    self.restore_vector_capability(&undo.index, prior);
                }
            }
            if let Some(prior) = undo.old_vector.take() {
                self.restore_vector_capability(&undo.index, prior);
            }
            if let Some((from, Some(position))) = undo.moving
                && let Some(current) = inserts
                    .iter()
                    .rposition(|entry| entry.index == undo.index && entry.row_id == undo.row_id)
            {
                let mut entry = inserts.remove(current);
                entry.row_id = from;
                undo.inserts.push((position, entry));
            }
            if !undo.preserve_inserts {
                inserts.retain(|entry| entry.index != undo.index || entry.row_id != undo.row_id);
            }
            moves.retain(|(index, source, to, _)| {
                *index != undo.index
                    || (*to != undo.row_id
                        && undo
                            .moving
                            .is_none_or(|(from, _)| *source != from && *to != from))
            });
            if undo.moving.is_none() && !undo.preserve_inserts {
                deletes.retain(|(index, row, _)| *index != undo.index || *row != undo.row_id);
            }
            if !undo.vector_index_existed
                && let Some(columns) = self.vectors.get_mut(&undo.index.table)
            {
                let empty = columns
                    .get(&undo.index.column)
                    .is_some_and(|owners| owners.rows.is_empty());
                let removed_index = empty.then(|| columns.remove(&undo.index.column));
                let removed_table = columns
                    .is_empty()
                    .then(|| self.vectors.remove(&undo.index.table));
                if self.vectors.is_empty() {
                    self.vectors = BTreeMap::new();
                }
                // Keep admission until both outer/column empty leaves retire.
                drop(removed_table);
                drop(removed_index);
            }
            undo.admission.transfer_back();
            if undo.moving.is_some() {
                undo.inserts.sort_unstable_by_key(|(position, _)| *position);
                for (position, entry) in undo.inserts {
                    inserts.insert(position.min(inserts.len()), entry);
                }
            } else {
                for (position, entry) in undo.inserts.into_iter().rev() {
                    inserts.insert(position.min(inserts.len()), entry);
                }
            }
            for (position, entry) in undo.moves.into_iter().rev() {
                moves.insert(position.min(moves.len()), entry);
            }
            for (position, entry) in undo.deletes {
                deletes.insert(position.min(deletes.len()), entry);
            }
        }
    }
    #[doc(hidden)]
    pub fn retain_displaced(
        &mut self,
        rows: &mut Vec<(String, contextdb_core::VersionedRow)>,
        table: &str,
        row: RowId,
        prior_admitted: Option<usize>,
        admission: Option<Arc<dyn StagedPostingCharge>>,
    ) -> bool {
        if self.checkpoint_depth == 0 {
            return false;
        }
        let undo = self
            .undo
            .last_mut()
            .expect("replacement staging prepared undo");
        assert!(undo.table == table && undo.row_id == row);
        let mut position = 0;
        while position < rows.len() {
            if rows[position].0 == table && rows[position].1.row_id == row {
                let (name, old) = rows.remove(position);
                undo.displaced.push((position, name, old));
            } else {
                position += 1;
            }
        }
        undo.prior_admitted = prior_admitted;
        undo.admission = admission;
        true
    }
    /// Associate the actual payload admission with its prepared mutation undo.
    #[doc(hidden)]
    pub fn record_installed_admission(&mut self, table: &str, row: RowId, bytes: usize) {
        if let Some(undo) = self
            .undo
            .iter_mut()
            .rev()
            .find(|undo| undo.table == table && undo.row_id == row)
        {
            debug_assert_eq!(undo.installed_admitted, 0);
            undo.installed_admitted = bytes;
        }
    }
    #[doc(hidden)]
    pub fn restore_checkpoint(
        &mut self,
        marker: [usize; 2],
        inserts: &mut Vec<(String, contextdb_core::VersionedRow)>,
        admitted: &mut HashMap<String, HashMap<RowId, usize>>,
        mut release: impl FnMut(usize),
    ) {
        while self.undo.len() > marker[0] {
            let undo = self.undo.pop().expect("mutation undo");
            inserts.retain(|(table, row)| table != &undo.table || row.row_id != undo.row_id);
            release(undo.installed_admitted);
            if let Some(rows) = admitted.get_mut(&undo.table) {
                rows.remove(&undo.row_id);
            }
            if let Some(charge) = undo.admission {
                charge.transfer_back();
                if let Some(bytes) = undo.prior_admitted {
                    admitted
                        .entry(undo.table.clone())
                        .or_default()
                        .insert(undo.row_id, bytes);
                }
            }
            for (position, table, row) in undo.displaced.into_iter().rev() {
                inserts.insert(position.min(inserts.len()), (table, row));
            }
            self.remove(&undo.table, undo.row_id);
            if let Some(old) = undo.old_charge {
                self.charges
                    .entry(undo.table.clone())
                    .or_default()
                    .insert(undo.row_id, old);
            }
            for prior in undo.legacy {
                let indexes = self.tables.entry(undo.table.clone()).or_default();
                if prior.replace_domain {
                    indexes.remove(&prior.name);
                    if let Some(domain) = prior.domain {
                        indexes.insert(prior.name, domain);
                    }
                } else {
                    let postings = indexes.entry(prior.name).or_default();
                    postings.schema = prior.schema;
                    if let Some((key, position)) = prior.key {
                        postings.latest.insert(undo.row_id, key.clone());
                        postings.ordered.insert((key, undo.row_id), position);
                    }
                }
            }
            if undo.table_existed {
                if let Some(statement) = undo.statement {
                    crate::StatementRows::restore(
                        self.statement_rows
                            .get_mut(&undo.table)
                            .expect("staged statement owner"),
                        undo.row_id,
                        statement,
                    );
                }
            } else {
                self.statement_rows.remove(&undo.table);
            }
        }
        self.discard_empty_maps();
    }
    /// Prepare the same admitted inverse as replacement, retaining the old row owner.
    #[doc(hidden)]
    pub fn prepare_row_removal(
        &mut self,
        table: &str,
        row: RowId,
        retired_count: usize,
        mut reserve: impl FnMut(usize) -> Result<Arc<dyn StagedPostingCharge>>,
    ) -> Result<()> {
        if self.checkpoint_depth == 0 {
            return Ok(());
        }
        let Some(snapshot) = self.statement_rows.get(table) else {
            return Ok(());
        };
        let Some(owner) = snapshot.owner(row) else {
            return Ok(());
        };
        let prepared = crate::StatementRows::prepare_delete(snapshot, row, &mut reserve)?;
        self.stage_retiring_row(
            table,
            StagedPosition {
                row_id: row,
                position: 0,
                created_tx: owner.row.created_tx,
            },
            &owner.keys,
            &owner.schema,
            retired_count,
            &mut reserve,
        )?;
        let undo = crate::StatementRows::publish_delete(
            self.statement_rows
                .get_mut(table)
                .expect("prepared removal table"),
            row,
            prepared,
        );
        self.undo
            .last_mut()
            .expect("prepared removal undo")
            .statement = Some(undo);
        Ok(())
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
    /// Prepare every table copy and collection owner before any publication.
    pub fn prepare_statement_removals(
        &self,
        selected: &std::collections::BTreeSet<(String, RowId)>,
    ) -> Result<PreparedStatementRemovals> {
        let selected_table = |table: &String, snapshot: &Arc<crate::StatementRows>| {
            selected
                .iter()
                .any(|(name, row)| name == table && snapshot.contains(row))
        };
        let mut count = 0usize;
        let mut bytes = 0usize;
        let mut admission = None;
        for (table, snapshot) in &self.statement_rows {
            if selected_table(table, snapshot) {
                count = count.saturating_add(1);
                bytes = bytes
                    .saturating_add(table.len())
                    .saturating_add(std::mem::size_of::<(
                        String,
                        crate::statement_rows::PreparedStatementRemoval,
                    )>());
                admission.get_or_insert(snapshot);
            }
        }
        let charge = admission
            .map(|snapshot| snapshot.reserve_removal_control(bytes))
            .transpose()?;
        let mut tables = Vec::new();
        tables.try_reserve_exact(count).map_err(|_| {
            contextdb_core::Error::Other("statement removal control allocation failed".into())
        })?;
        for (table, snapshot) in &self.statement_rows {
            if selected_table(table, snapshot) {
                tables.push((
                    table.clone(),
                    crate::StatementRows::prepare_removal(snapshot)?,
                ));
            }
        }
        Ok(PreparedStatementRemovals {
            tables,
            _charge: charge,
        })
    }
    pub fn publish_statement_removals(
        &mut self,
        prepared: PreparedStatementRemovals,
        selected: &std::collections::BTreeSet<(String, RowId)>,
    ) {
        let PreparedStatementRemovals { tables, _charge } = prepared;
        for (table, replacement) in tables {
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
        reserve: F,
    ) -> Result<()>
    where
        F: FnMut(usize) -> Result<Arc<dyn StagedPostingCharge>>,
    {
        self.stage_retiring_row(table, position, keys, schema, 0, reserve)
    }
    #[doc(hidden)]
    pub fn stage_retiring_row<F>(
        &mut self,
        table: &str,
        position: StagedPosition,
        keys: &[(String, IndexKey)],
        schema: &Arc<StagedSchema>,
        retired_count: usize,
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
        if self.checkpoint_depth != 0 {
            let indexes = self.tables.get(table);
            let count = indexes.map_or(0, BTreeMap::len).saturating_add(keys.len());
            let undo_bytes = std::mem::size_of::<StagedMutationUndo>()
                .saturating_add(table.len())
                .saturating_add(count.saturating_mul(std::mem::size_of::<LegacyIndexUndo>()))
                .saturating_add(retired_count.saturating_mul(std::mem::size_of::<(
                    usize,
                    String,
                    contextdb_core::VersionedRow,
                )>()))
                .saturating_add(table.len())
                .saturating_add(indexes.map_or(0, |indexes| {
                    indexes
                        .iter()
                        .map(|(name, postings)| {
                            name.len().saturating_add(
                                postings
                                    .latest
                                    .get(&position.row_id)
                                    .map_or(0, |key| staged_key_bytes(key).saturating_mul(2)),
                            )
                        })
                        .sum::<usize>()
                }))
                .saturating_add(keys.iter().map(|(name, _)| name.len()).sum::<usize>());
            let undo_charge = reserve(
                undo_bytes
                    .saturating_add(std::mem::size_of::<Arc<dyn StagedPostingCharge>>())
                    .saturating_add(if retired_count == 0 {
                        0
                    } else {
                        // The engine transfer owner holds an accountant Arc and byte counter, plus the Arc control header.
                        4 * std::mem::size_of::<usize>()
                    }),
            )?;
            self.retain_journal_charge(undo_charge.clone())?;
            self.undo.try_reserve_exact(1).map_err(|_| {
                contextdb_core::Error::Other("mutation undo allocation failed".into())
            })?;
            let mut legacy = Vec::with_capacity(count);
            if let Some(indexes) = self.tables.get_mut(table) {
                for (name, postings) in indexes {
                    let incompatible =
                        keys.iter()
                            .find(|(index, _)| index == name)
                            .is_some_and(|_| {
                                postings
                                    .schema
                                    .as_ref()
                                    .is_some_and(|old| !old.matches_index(schema, name))
                            });
                    let singleton = postings.ordered.len() == 1
                        && postings.latest.contains_key(&position.row_id);
                    if incompatible || singleton {
                        legacy.push(LegacyIndexUndo {
                            name: name.clone(),
                            domain: Some(std::mem::take(postings)),
                            schema: None,
                            key: None,
                            replace_domain: true,
                        });
                    } else {
                        legacy.push(LegacyIndexUndo {
                            name: name.clone(),
                            domain: None,
                            schema: postings.schema.clone(),
                            key: postings.latest.get(&position.row_id).map(|key| {
                                (
                                    key.clone(),
                                    postings.ordered[&(key.clone(), position.row_id)],
                                )
                            }),
                            replace_domain: false,
                        });
                    }
                }
            }
            for (name, _) in keys {
                if !legacy.iter().any(|old| &old.name == name) {
                    legacy.push(LegacyIndexUndo {
                        name: name.clone(),
                        domain: None,
                        schema: None,
                        key: None,
                        replace_domain: true,
                    });
                }
            }
            self.undo.push(StagedMutationUndo {
                table: table.to_owned(),
                row_id: position.row_id,
                legacy,
                old_charge: self
                    .charges
                    .get_mut(table)
                    .and_then(|rows| rows.remove(&position.row_id)),
                statement: None,
                table_existed: self.statement_rows.contains_key(table),
                displaced: Vec::with_capacity(retired_count),
                prior_admitted: None,
                installed_admitted: 0,
                admission: None,
                _charge: undo_charge,
            });
        }
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

    /// Controls for a per-index root remain owned until the transaction ends;
    /// payload controls retire independently as rows are changed or withdrawn.
    #[doc(hidden)]
    pub fn vector_capability(
        &self,
        index: &contextdb_core::VectorIndexRef,
        row: RowId,
    ) -> Option<Arc<StagedVectorCapability>> {
        self.vectors
            .get(&index.table)?
            .get(&index.column)?
            .rows
            .get(&row)
            .cloned()
    }
    #[doc(hidden)]
    pub fn prepare_vector_capability(
        &self,
        index: &contextdb_core::VectorIndexRef,
        row: RowId,
        values: &[f32],
        mut reserve: impl FnMut(usize) -> Result<Arc<dyn StagedPostingCharge>>,
    ) -> Result<PreparedStagedVector> {
        let scalar = self
            .statement_rows
            .get(&index.table)
            .and_then(|rows| rows.owner(row))
            .filter(|owner| {
                matches!(owner.row.values.get(&index.column),
                Some(Value::Vector(have)) if have.as_slice() == values)
            });
        let payload = match scalar {
            Some(owner) => StagedVectorPayload::Scalar(owner),
            None => {
                let charge = reserve(
                    std::mem::size_of::<AdmittedVectorPayload>()
                        .saturating_add(2 * std::mem::size_of::<usize>())
                        .saturating_add(values.len().saturating_mul(std::mem::size_of::<f32>())),
                )?;
                StagedVectorPayload::Owned(Arc::new(AdmittedVectorPayload {
                    values: values.to_vec(),
                    _charge: charge,
                }))
            }
        };
        self.prepare_vector_capability_with_payload(index, row, payload, &mut reserve)
    }
    #[doc(hidden)]
    pub fn prepare_moved_vector_capability(
        &self,
        index: &contextdb_core::VectorIndexRef,
        from: RowId,
        to: RowId,
        mut reserve: impl FnMut(usize) -> Result<Arc<dyn StagedPostingCharge>>,
    ) -> Result<Option<PreparedStagedVector>> {
        let Some(prior) = self.vector_capability(index, from) else {
            return Ok(None);
        };
        let payload = match &prior.payload {
            StagedVectorPayload::Scalar(owner) => StagedVectorPayload::Scalar(owner.clone()),
            StagedVectorPayload::Owned(owner) => StagedVectorPayload::Owned(owner.clone()),
        };
        self.prepare_vector_capability_with_payload(index, to, payload, &mut reserve)
            .map(Some)
    }
    fn prepare_vector_capability_with_payload(
        &self,
        index: &contextdb_core::VectorIndexRef,
        row: RowId,
        payload: StagedVectorPayload,
        reserve: &mut impl FnMut(usize) -> Result<Arc<dyn StagedPostingCharge>>,
    ) -> Result<PreparedStagedVector> {
        // The shared staged-posting B-tree envelope plus actual capability and
        // owned column name; temporary preparation is admitted by the caller.
        let charge = reserve(
            std::mem::size_of::<StagedVectorCapability>()
                .saturating_add(2 * std::mem::size_of::<usize>())
                .saturating_add(256)
                .saturating_add(index.column.len()),
        )?;
        let new_index = if self
            .vectors
            .get(&index.table)
            .is_some_and(|columns| columns.contains_key(&index.column))
        {
            None
        } else {
            let charge =
                reserve(
                    std::mem::size_of::<StagedVectorIndex>()
                        .saturating_add(
                            128usize.saturating_add(
                                11usize.saturating_mul(
                                    std::mem::size_of::<String>()
                                        .saturating_add(std::mem::size_of::<StagedVectorIndex>()),
                                ),
                            ),
                        )
                        .saturating_add(if self.vectors.contains_key(&index.table) {
                            0
                        } else {
                            128usize
                                .saturating_add(11usize.saturating_mul(
                                    std::mem::size_of::<String>().saturating_add(
                                        std::mem::size_of::<BTreeMap<String, StagedVectorIndex>>(),
                                    ),
                                ))
                                .saturating_add(index.table.len())
                        })
                        .saturating_add(index.column.len()),
                )?;
            Some(StagedVectorIndex {
                rows: BTreeMap::new(),
                _charge: charge,
            })
        };
        Ok(PreparedStagedVector {
            capability: Arc::new(StagedVectorCapability {
                row_id: row,
                column: index.column.clone(),
                payload,
                _charge: charge,
            }),
            new_index,
        })
    }
    #[doc(hidden)]
    pub fn publish_vector_capability(
        &mut self,
        index: &contextdb_core::VectorIndexRef,
        prepared: PreparedStagedVector,
    ) {
        if let Some(new) = prepared.new_index {
            if !self.vectors.contains_key(&index.table) {
                self.vectors.insert(index.table.clone(), BTreeMap::new());
            }
            self.vectors
                .get_mut(&index.table)
                .expect("prepared vector table")
                .entry(index.column.clone())
                .or_insert(new);
        }
        self.vectors
            .get_mut(&index.table)
            .and_then(|columns| columns.get_mut(&index.column))
            .expect("prepared vector index")
            .rows
            .insert(prepared.capability.row_id, prepared.capability);
    }
    #[doc(hidden)]
    pub fn remove_vector_capability(&mut self, index: &contextdb_core::VectorIndexRef, row: RowId) {
        if let Some(owners) = self
            .vectors
            .get_mut(&index.table)
            .and_then(|columns| columns.get_mut(&index.column))
        {
            let removed = owners.rows.remove(&row);
            if owners.rows.is_empty() {
                // std BTreeMap may keep its empty leaf; retire it before the
                // final per-row control admission is released.
                owners.rows = BTreeMap::new();
            }
            drop(removed);
        }
    }
    fn restore_vector_capability(
        &mut self,
        index: &contextdb_core::VectorIndexRef,
        capability: Arc<StagedVectorCapability>,
    ) {
        self.vectors
            .get_mut(&index.table)
            .and_then(|columns| columns.get_mut(&index.column))
            .expect("retained vector index controls")
            .rows
            .insert(capability.row_id, capability);
    }
    #[doc(hidden)]
    pub fn vector_capability_retained_bytes(&self) -> usize {
        self.vectors
            .values()
            .flat_map(|columns| columns.values())
            .map(|owners| {
                owners._charge.retained_bytes().saturating_add(
                    owners
                        .rows
                        .values()
                        .map(|owner| {
                            owner
                                ._charge
                                .retained_bytes()
                                .saturating_add(match &owner.payload {
                                    StagedVectorPayload::Scalar(_) => 0,
                                    StagedVectorPayload::Owned(payload) => {
                                        payload._charge.retained_bytes()
                                    }
                                })
                        })
                        .sum::<usize>(),
                )
            })
            .sum()
    }

    pub fn contains_row(&self, table: &str, row: RowId) -> bool {
        self.charges
            .get(table)
            .is_some_and(|rows| rows.contains_key(&row))
    }

    pub fn retained_bytes(&self) -> usize {
        self.vector_capability_retained_bytes().saturating_add(
            self.charges
                .values()
                .flat_map(|rows| rows.values())
                .fold(0usize, |bytes, (charge, _)| bytes.saturating_add(*charge))
                .saturating_add(
                    self.statement_rows
                        .values()
                        .map(|rows| rows.retained_bytes())
                        .sum::<usize>(),
                ),
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
