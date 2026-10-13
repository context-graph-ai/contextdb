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
    #[doc(hidden)]
    fn reserve_private_original_copy(
        &self,
        bytes: usize,
    ) -> Result<crate::staged_source::OriginalCopyGrant> {
        self.reserve_copy(bytes)
            .map(crate::staged_source::OriginalCopyGrant::from_host)
    }
    #[doc(hidden)]
    fn visit_retained_controls<'a>(
        &'a self,
        _visitor: &mut dyn FnMut(crate::staged_source::RetainedControl<'a>),
    ) -> Option<()> {
        None
    }

    #[doc(hidden)]
    fn statement_view(&self) -> Option<&dyn crate::statement_rows::StatementImageView> {
        None
    }
    #[doc(hidden)]
    fn statement_image_shared(&self) -> Option<bool> {
        None
    }
    #[doc(hidden)]
    fn clone_statement_control(&self) -> Option<Arc<dyn StagedPostingCharge>> {
        None
    }
    #[doc(hidden)]
    fn reconcile_statement_image(&self, _rows: &crate::StatementRows, _keep_undo: bool) {}
    #[doc(hidden)]
    fn settle_statement_image(&self) {}
    #[doc(hidden)]
    fn statement_origin(&self) -> Option<&crate::statement_rows::StatementOrigin> {
        None
    }
    #[doc(hidden)]
    fn statement_retained_bytes(&self, _rows: &crate::StatementRows) -> Option<usize> {
        None
    }
    #[doc(hidden)]
    fn retained_control_bytes(&self) -> Option<usize> {
        None
    }
    #[doc(hidden)]
    fn reserve_terminal_copy(&self, bytes: usize) -> Result<Arc<dyn StagedPostingCharge>> {
        self.reserve_copy(bytes)
    }

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
    pub(crate) fn from_native(schema: crate::staged_source::SchemaHandle) -> Self {
        let layout = schema
            .layout()
            .iter()
            .map(|(name, kind)| (name.as_str().to_owned(), kind.clone()))
            .collect();
        let mut indexes = Vec::new();
        schema.for_each_index(|name, kind, columns| {
            indexes.push(IndexDecl {
                name: name.to_owned(),
                kind,
                columns: columns
                    .iter()
                    .map(|(name, direction)| (name.as_str().to_owned(), *direction))
                    .collect(),
            })
        });
        Self {
            layout,
            indexes,
            charge: Arc::new(crate::statement_rows::TerminalSchemaCharge {
                origin: crate::statement_rows::StatementOrigin(
                    crate::statement_rows::StatementOriginOwner::Schema(schema),
                ),
            }),
        }
    }
    pub(crate) fn has_native_origin(&self, schema: &crate::staged_source::SchemaHandle) -> bool {
        matches!(self.charge.statement_origin(),Some(crate::statement_rows::StatementOrigin(crate::statement_rows::StatementOriginOwner::Schema(original))) if original.ptr_eq(schema))
    }

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
// Box the name pair so an index node holds one pointer, rather than two
// strings and an intermediate map. Borrowed comparison allocates no lookup key.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct StagedVectorKey(Box<(String, String)>);
trait VectorIdentity {
    fn names(&self) -> (&str, &str);
}
impl VectorIdentity for StagedVectorKey {
    fn names(&self) -> (&str, &str) {
        (&self.0.0, &self.0.1)
    }
}
struct VectorLookup<'a>(&'a str, &'a str);
impl VectorIdentity for VectorLookup<'_> {
    fn names(&self) -> (&str, &str) {
        (self.0, self.1)
    }
}
impl PartialEq for dyn VectorIdentity + '_ {
    fn eq(&self, other: &Self) -> bool {
        self.names() == other.names()
    }
}
impl Eq for dyn VectorIdentity + '_ {}
impl PartialOrd for dyn VectorIdentity + '_ {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for dyn VectorIdentity + '_ {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.names().cmp(&other.names())
    }
}
impl<'a> std::borrow::Borrow<dyn VectorIdentity + 'a> for StagedVectorKey {
    fn borrow(&self) -> &(dyn VectorIdentity + 'a) {
        self
    }
}

#[derive(Debug, Clone)]
struct StagedVectorIndex {
    rows: BTreeMap<RowId, Arc<StagedVectorCapability>>,
    _charge: Arc<dyn StagedPostingCharge>,
}
// The first index needs no B-tree node. The optional boxed directory keeps
// WriteSet backing smaller; its actual allocation is admitted by preparation.
#[derive(Debug, Clone)]
enum StagedVectorRoots {
    Single(contextdb_core::VectorIndexRef, StagedVectorIndex),
    Many(BTreeMap<StagedVectorKey, StagedVectorIndex>),
}
#[repr(transparent)]
#[derive(Debug, Default)]
struct StagedVectorDirectory(Option<DirectoryOwner>);
impl StagedVectorDirectory {
    fn roots(&self) -> Option<&StagedVectorRoots> {
        if self.native_body().is_some() {
            return None;
        }
        self.0.as_deref()?.as_ref()
    }
    fn get(&self, index: &contextdb_core::VectorIndexRef) -> Option<&StagedVectorIndex> {
        match self.roots()? {
            StagedVectorRoots::Single(key, state) => (key == index).then_some(state),
            StagedVectorRoots::Many(entries) => {
                let lookup = VectorLookup(&index.table, &index.column);
                entries.get(&lookup as &dyn VectorIdentity)
            }
        }
    }
    fn get_mut(
        &mut self,
        index: &contextdb_core::VectorIndexRef,
    ) -> Option<&mut StagedVectorIndex> {
        match self.0.as_deref_mut()?.as_mut()? {
            StagedVectorRoots::Single(key, state) => (key == index).then_some(state),
            StagedVectorRoots::Many(entries) => {
                let lookup = VectorLookup(&index.table, &index.column);
                entries.get_mut(&lookup as &dyn VectorIdentity)
            }
        }
    }
    fn index_bytes(&self) -> usize {
        let directory = std::mem::size_of::<Option<StagedVectorRoots>>();
        match self.roots() {
            None => directory,
            Some(roots) => directory
                .saturating_add(
                    128usize.saturating_add(
                        11usize.saturating_mul(
                            std::mem::size_of::<StagedVectorKey>()
                                .saturating_add(std::mem::size_of::<StagedVectorIndex>()),
                        ),
                    ),
                )
                .saturating_add(std::mem::size_of::<(String, String)>())
                .saturating_add(if matches!(roots, StagedVectorRoots::Single(_, _)) {
                    // The first name pair moves into a boxed map key too.
                    std::mem::size_of::<(String, String)>()
                } else {
                    0
                }),
        }
    }
    fn insert(&mut self, index: &contextdb_core::VectorIndexRef, state: StagedVectorIndex) {
        if self.0.is_none() {
            self.0 = Some(DirectoryOwner::legacy(Box::new(Some(
                StagedVectorRoots::Single(index.clone(), state),
            ))));
            return;
        }
        let slot = self.0.as_deref_mut().expect("retained directory");
        if let Some(StagedVectorRoots::Many(entries)) = slot.as_mut() {
            entries.insert(
                StagedVectorKey(Box::new((index.table.clone(), index.column.clone()))),
                state,
            );
            return;
        }
        let Some(StagedVectorRoots::Single(old, old_state)) = slot.take() else {
            unreachable!("published directory has its root")
        };
        let mut entries = BTreeMap::new();
        entries.insert(
            StagedVectorKey(Box::new((old.table, old.column))),
            old_state,
        );
        entries.insert(
            StagedVectorKey(Box::new((index.table.clone(), index.column.clone()))),
            state,
        );
        *slot = Some(StagedVectorRoots::Many(entries));
    }
    fn remove(&mut self, index: &contextdb_core::VectorIndexRef) -> Option<StagedVectorIndex> {
        match self.roots()? {
            StagedVectorRoots::Single(key, _) if key != index => return None,
            StagedVectorRoots::Single(_, _) => {
                let mut allocation = self.0.take().expect("single directory");
                let root = allocation.take();
                drop(allocation);
                let Some(StagedVectorRoots::Single(key, state)) = root else {
                    unreachable!()
                };
                drop(key);
                return Some(state);
            }
            StagedVectorRoots::Many(_) => {}
        }
        let Some(StagedVectorRoots::Many(entries)) = self.0.as_deref_mut()?.as_mut() else {
            unreachable!()
        };
        let lookup = VectorLookup(&index.table, &index.column);
        let removed = entries.remove_entry(&lookup as &dyn VectorIdentity);
        if entries.is_empty() {
            // Free the empty leaf and directory before returning the charge.
            drop(self.0.take());
        }
        removed.map(|(key, state)| {
            drop(key);
            state
        })
    }
    fn values(&self) -> impl Iterator<Item = &StagedVectorIndex> {
        let (single, many) = match self.roots() {
            None => (None, None),
            Some(StagedVectorRoots::Single(_, state)) => (Some(state), None),
            Some(StagedVectorRoots::Many(entries)) => (None, Some(entries)),
        };
        single
            .into_iter()
            .chain(many.into_iter().flat_map(|entries| entries.values()))
    }
}
impl Drop for StagedVectorIndex {
    fn drop(&mut self) {
        while let Some((_, capability)) = self.rows.pop_first() {
            if self.rows.is_empty() {
                self.rows = BTreeMap::new();
            }
            drop(capability);
        }
    }
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
    vectors: StagedVectorDirectory,
}
impl Clone for OrderedStagedRows {
    fn clone(&self) -> Self {
        if let Some(root) = self.native_root() {
            return root.terminal_legacy();
        }
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
    range: OrderedStagedRange<'a>,
    reverse: bool,
}
enum OrderedStagedRange<'a> {
    Legacy(std::collections::btree_map::Range<'a, (IndexKey, RowId), StagedPosition>),
    Native(NativeOrderedRange<'a>),
}
impl Iterator for OrderedStagedCursor<'_> {
    type Item = StagedPosition;
    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.range {
            OrderedStagedRange::Legacy(range) => {
                let entry = if self.reverse {
                    range.next_back()
                } else {
                    range.next()
                };
                entry.map(|(_, position)| *position)
            }
            OrderedStagedRange::Native(range) => {
                range.next(self.reverse).map(|(_, position)| *position)
            }
        }
    }
}

pub struct OrderedStagedPrefixCursor<'a> {
    range: OrderedStagedRange<'a>,
    prefix: &'a IndexKey,
    finished: bool,
}
impl Iterator for OrderedStagedPrefixCursor<'_> {
    type Item = StagedPosition;
    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        let (key, position) = match &mut self.range {
            OrderedStagedRange::Legacy(range) => {
                range.next().map(|(key, position)| (&key.0, position))
            }
            OrderedStagedRange::Native(range) => range
                .next(false)
                .map(|(key, position)| (&key.position().0, position)),
        }?;
        if !key.starts_with(self.prefix) {
            self.finished = true;
            return None;
        }
        Some(*position)
    }
}

impl OrderedStagedRows {
    pub fn statement_rows(&self, table: &str) -> Option<Arc<crate::StatementRows>> {
        if let Some(root) = self.native_root() {
            return root
                .table(table)
                .map(|table| Arc::new(crate::StatementRows::from_native(table.statement.clone())));
        }
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
            for rows in self.statement_rows.values() {
                rows.settle_image();
            }
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
            vector_index_existed: self.vector_index(index).is_some(),
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
            vector_index_existed: self.vector_index(index).is_some(),
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
            vector_index_existed: self.vector_index(index).is_some(),
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
            if !undo.vector_index_existed {
                let empty = self
                    .vectors
                    .get(&undo.index)
                    .is_some_and(|owners| owners.rows.is_empty());
                let removed = empty.then(|| self.vectors.remove(&undo.index));
                // Retire the empty node before its control admission.
                drop(removed);
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
        if let Some(image) = self.native_image() {
            return image
                .get()
                .table(table)
                .is_some_and(|table| table.statement.get().has_index_for(index, meta));
        }
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
        if let Some(image) = self.native_image() {
            return image.get().is_empty();
        }
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
        if let Some(image) = self.native_image() {
            let index = image.get().index(table, index)?;
            // Match the existing BTreeMap range contract before seeking.
            if let (
                Bound::Included(start) | Bound::Excluded(start),
                Bound::Included(end) | Bound::Excluded(end),
            ) = (&lower, &upper)
            {
                assert!(
                    start <= end,
                    "range start is greater than range end in BTreeMap"
                );
                assert!(
                    start != end
                        || !matches!((&lower, &upper), (Bound::Excluded(_), Bound::Excluded(_))),
                    "range start and end are equal and excluded in BTreeMap"
                );
            }
            return Some(OrderedStagedCursor {
                range: OrderedStagedRange::Native(NativeOrderedRange::new(
                    &index.ordered,
                    lower,
                    upper,
                    None,
                )),
                reverse,
            });
        }
        let postings = self.tables.get(table)?.get(index)?;
        Some(OrderedStagedCursor {
            range: OrderedStagedRange::Legacy(postings.ordered.range((lower, upper))),
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
        if let Some(image) = self.native_image() {
            let index = image.get().index(table, index)?;
            return Some(OrderedStagedPrefixCursor {
                range: OrderedStagedRange::Native(NativeOrderedRange::new(
                    &index.ordered,
                    Bound::Unbounded,
                    Bound::Unbounded,
                    Some(prefix),
                )),
                prefix,
                finished: false,
            });
        }
        let postings = self.tables.get(table)?.get(index)?;
        let lower = (prefix.clone(), RowId(0));
        Some(OrderedStagedPrefixCursor {
            range: OrderedStagedRange::Legacy(
                postings
                    .ordered
                    .range((Bound::Included(&lower), Bound::Unbounded)),
            ),
            prefix,
            finished: false,
        })
    }

    /// Controls for a per-index root remain owned until the transaction ends;
    /// payload controls retire independently as rows are changed or withdrawn.
    fn vector_index(&self, index: &contextdb_core::VectorIndexRef) -> Option<&StagedVectorIndex> {
        self.vectors.get(index)
    }
    fn vector_index_mut(
        &mut self,
        index: &contextdb_core::VectorIndexRef,
    ) -> Option<&mut StagedVectorIndex> {
        self.vectors.get_mut(index)
    }

    #[doc(hidden)]
    pub fn vector_capability(
        &self,
        index: &contextdb_core::VectorIndexRef,
        row: RowId,
    ) -> Option<Arc<StagedVectorCapability>> {
        if let Some(image) = self.native_image() {
            return crate::staged_source::get_by(&image.get().vector_index(index)?.rows, |id| {
                row.cmp(id)
            })
            .map(NativeEagerVector::terminal_shell);
        }
        self.vector_index(index)?.rows.get(&row).cloned()
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
        let new_index = if self.vector_index(index).is_some() {
            None
        } else {
            let charge = reserve(
                std::mem::size_of::<StagedVectorIndex>()
                    .saturating_add(self.vectors.index_bytes())
                    .saturating_add(index.table.len())
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
            // Both the boxed names and map node were admitted at preparation.
            if self.vector_index(index).is_none() {
                self.vectors.insert(index, new);
            }
        }
        self.vector_index_mut(index)
            .expect("prepared vector index")
            .rows
            .insert(prepared.capability.row_id, prepared.capability);
    }
    #[doc(hidden)]
    pub fn remove_vector_capability(&mut self, index: &contextdb_core::VectorIndexRef, row: RowId) {
        if let Some(owners) = self.vector_index_mut(index) {
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
        self.vector_index_mut(index)
            .expect("retained vector index controls")
            .rows
            .insert(capability.row_id, capability);
    }
    #[doc(hidden)]
    pub fn vector_capability_retained_bytes(&self) -> usize {
        if let Some(root) = self.native_root() {
            return root.vector_retained_bytes();
        }
        self.vectors
            .values()
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
        if let Some(image) = self.native_image() {
            return image.get().table(table).is_some_and(|table| {
                crate::staged_source::get_by(&table.rows, |id| row.cmp(id)).is_some()
            });
        }
        self.charges
            .get(table)
            .is_some_and(|rows| rows.contains_key(&row))
    }

    pub fn retained_bytes(&self) -> usize {
        if let Some(bytes) = self.native_retained_bytes() {
            return bytes;
        }
        if let Some(root) = self.native_root() {
            return root
                .retained_bytes()
                .saturating_add(self.native_image().expect("active image").control_bytes())
                .saturating_add(
                    self.vectors
                        .native_body()
                        .expect("native directory")
                        .credit
                        .bytes(),
                );
        }

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

/// Private eager vector custody. Terminal std Arc shells may borrow this
/// immutable owner, but the engine never exports its retained allocation.
#[derive(Clone)]
pub(crate) struct NativeEagerVector(crate::staged_source::FundedArc<NativeEagerVectorBody>);

struct NativeEagerVectorBody {
    row_id: RowId,
    column: crate::staged_source::SourceName,
    payload: NativeEagerVectorPayload,
}

#[derive(Clone)]
enum NativeEagerVectorPayload {
    // activated by bounded-storage S3/S4; remove allow at activation
    #[allow(dead_code)]
    Scalar(crate::statement_rows::NativeEagerRow),
    Owned(crate::staged_source::FundedArc<crate::staged_source::AdmittedEagerSourceVector>),
}

impl NativeEagerVector {
    // activated by bounded-storage S3/S4; remove allow at activation
    #[allow(dead_code)]
    pub(crate) fn prepare_scalar(
        row: crate::statement_rows::NativeEagerRow,
        column: &str,
        values: &[f32],
        funder: &crate::staged_source::SourceFunder,
    ) -> Result<Option<Self>> {
        if !matches!(row.row().values.get(column),
            Some(Value::Vector(have)) if have.as_slice() == values)
        {
            return Ok(None);
        }
        let control =
            crate::staged_source::FundedArc::<NativeEagerVectorBody>::prepare(funder.clone())?;
        let column = crate::staged_source::SourceName::new(column, funder)?;
        Ok(Some(Self(crate::staged_source::FundedArc::from_prepared(
            NativeEagerVectorBody {
                row_id: row.row().row_id,
                column,
                payload: NativeEagerVectorPayload::Scalar(row),
            },
            control,
        ))))
    }

    /// `original` is the separately prepared eager vector copy, paired with
    /// its own actual invoice before entry. It is never a canonical-original
    /// invoice or the ordinary WriteSet vector's invoice.
    pub(crate) fn prepare_owned(
        original: crate::staged_source::AdmittedEagerSourceVector,
        funder: &crate::staged_source::SourceFunder,
    ) -> Result<Self> {
        original.validate_original_pairing(funder)?;
        let control =
            crate::staged_source::FundedArc::<NativeEagerVectorBody>::prepare(funder.clone())?;
        let payload_control = crate::staged_source::FundedArc::<
            crate::staged_source::AdmittedEagerSourceVector,
        >::prepare(funder.clone())?;
        let column = crate::staged_source::SourceName::new(&original.entry.index.column, funder)?;
        let row_id = original.entry.row_id;
        let payload = crate::staged_source::FundedArc::from_prepared(original, payload_control);
        Ok(Self(crate::staged_source::FundedArc::from_prepared(
            NativeEagerVectorBody {
                row_id,
                column,
                payload: NativeEagerVectorPayload::Owned(payload),
            },
            control,
        )))
    }

    fn retained_bytes(&self) -> usize {
        self.0
            .retained_bytes()
            .saturating_add(self.0.column.retained_bytes())
            .saturating_add(match &self.0.payload {
                NativeEagerVectorPayload::Scalar(_) => 0,
                NativeEagerVectorPayload::Owned(payload) => payload
                    .retained_bytes()
                    .saturating_add(payload.invoice.retained_bytes()),
            })
    }
    pub(crate) fn row_id(&self) -> RowId {
        self.0.row_id
    }

    pub(crate) fn vector(&self) -> &[f32] {
        match &self.0.payload {
            NativeEagerVectorPayload::Scalar(row) => {
                match row.row().values.get(self.0.column.as_str()) {
                    Some(Value::Vector(values)) => values,
                    _ => unreachable!("immutable eager scalar vector owner"),
                }
            }
            NativeEagerVectorPayload::Owned(owner) => &owner.entry.vector,
        }
    }

    /// A move preserves the payload and its distinct original invoice. Only
    /// the new capability header needs admission; the native name is shared.
    // activated by bounded-storage S3/S4; remove allow at activation
    #[allow(dead_code)]
    pub(crate) fn prepare_move(
        &self,
        to: RowId,
        funder: &crate::staged_source::SourceFunder,
    ) -> Result<Self> {
        let control =
            crate::staged_source::FundedArc::<NativeEagerVectorBody>::prepare(funder.clone())?;
        Ok(Self(crate::staged_source::FundedArc::from_prepared(
            NativeEagerVectorBody {
                row_id: to,
                column: self.0.column.clone(),
                payload: self.0.payload.clone(),
            },
            control,
        )))
    }
}

impl std::fmt::Debug for NativeEagerVector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeEagerVector")
            .field("row_id", &self.row_id())
            .field("column", &self.0.column.as_str())
            .field("dimensions", &self.vector().len())
            .finish()
    }
}

impl StagedPostingCharge for NativeEagerVector {
    fn retained_bytes(&self) -> usize {
        NativeEagerVector::retained_bytes(self)
    }
}

#[derive(Debug)]
pub(crate) struct NativeOrderedRoot {
    tables: crate::staged_source::Link<
        crate::staged_source::SourceName,
        crate::staged_source::FundedArc<NativeOrderedTable>,
    >,
    vectors: crate::staged_source::Link<
        NativeVectorName,
        crate::staged_source::FundedArc<NativeVectorIndex>,
    >,
    exact: NativeExactSlots,
}
#[derive(Debug)]
struct NativeOrderedTable {
    statement: crate::statement_rows::NativeImage<crate::statement_rows::NativeStatementRoot>,
    rows: crate::staged_source::Link<RowId, crate::staged_source::FundedArc<NativeOrderedRow>>,
    indexes: crate::staged_source::Link<
        crate::staged_source::SourceName,
        crate::staged_source::FundedArc<NativeOrderedIndex>,
    >,
    ordered_present: bool,
}
#[derive(Debug)]
struct NativeOrderedRow {
    position: StagedPosition,
    schema: crate::staged_source::SchemaHandle,
    keys: crate::staged_source::SourceBuffer<(crate::staged_source::SourceName, NativeOrderedKey)>,
}
#[derive(Debug)]
struct NativeOrderedIndex {
    schema: crate::staged_source::SchemaHandle,
    ordered: crate::staged_source::Link<NativeOrderedKey, StagedPosition>,
}
#[derive(Debug, Clone)]
struct NativeOrderedKey(crate::staged_source::FundedArc<NativeOrderedKeyBody>);
#[derive(Debug)]
struct NativeOrderedKeyBody {
    position: (IndexKey, RowId),
    _charge: crate::staged_source::SourceCharge,
}
impl NativeOrderedKey {
    fn prepare(
        key: &IndexKey,
        row: RowId,
        funder: &crate::staged_source::SourceFunder,
    ) -> Result<Self> {
        let header =
            crate::staged_source::FundedArc::<NativeOrderedKeyBody>::prepare(funder.clone())?;
        let key = funder.clone().copy_key(key)?;
        Ok(Self(crate::staged_source::FundedArc::from_prepared(
            NativeOrderedKeyBody {
                position: (key.key, row),
                _charge: key.charge,
            },
            header,
        )))
    }
    fn position(&self) -> &(IndexKey, RowId) {
        &self.0.position
    }
}
impl PartialEq for NativeOrderedKey {
    fn eq(&self, other: &Self) -> bool {
        self.position() == other.position()
    }
}
impl Eq for NativeOrderedKey {}
impl PartialOrd for NativeOrderedKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for NativeOrderedKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.position().cmp(other.position())
    }
}
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct NativeVectorName {
    table: crate::staged_source::SourceName,
    column: crate::staged_source::SourceName,
}
#[derive(Debug)]
struct NativeVectorIndex {
    rows: crate::staged_source::Link<RowId, NativeEagerVector>,
    control: crate::staged_source::FundedArc<NativeVectorControl>,
}
#[derive(Debug)]
struct NativeVectorControl {
    name: NativeVectorName,
}
impl NativeOrderedRoot {
    pub(crate) fn empty() -> Self {
        Self {
            tables: None,
            vectors: None,
            exact: NativeExactSlots::default(),
        }
    }
    fn table(&self, table: &str) -> Option<&NativeOrderedTable> {
        crate::staged_source::get_by(&self.tables, |name| table.cmp(name.as_str()))
            .map(|table| &**table)
    }
    fn index(&self, table: &str, index: &str) -> Option<&NativeOrderedIndex> {
        crate::staged_source::get_by(&self.table(table)?.indexes, |name| index.cmp(name.as_str()))
            .map(|index| &**index)
    }
    fn vector_index(&self, index: &contextdb_core::VectorIndexRef) -> Option<&NativeVectorIndex> {
        crate::staged_source::get_by(&self.vectors, |name| {
            (index.table.as_str(), index.column.as_str())
                .cmp(&(name.table.as_str(), name.column.as_str()))
        })
        .map(|index| &**index)
    }
    pub(crate) fn next_sequence(&self, table: &str) -> usize {
        self.table(table)
            .map_or(0, |table| table.statement.get().next_sequence())
    }
    pub(crate) fn prepare_row(
        &self,
        table: &str,
        position: StagedPosition,
        eager: crate::statement_rows::NativeEagerRow,
        funder: &crate::staged_source::SourceFunder,
    ) -> Result<crate::statement_rows::NativeImage<Self>> {
        use crate::staged_source::{Entry, FundedArc, SourceBuffer, SourceName, insert};
        let prior = self.table(table);
        let empty = crate::statement_rows::NativeStatementRoot::empty();
        let statement = prior
            .map_or(&empty, |table| table.statement.get())
            .prepare_insert(eager.clone(), funder)?;
        let header = FundedArc::<NativeOrderedRow>::prepare(funder.clone())?;
        let key_count = eager.keys().count();
        let mut keys = SourceBuffer::with_capacity(key_count, funder)?;
        if keys.capacity() != key_count {
            return Err(contextdb_core::Error::Other(
                "ordered key capacity mismatch".into(),
            ));
        }
        for (name, key) in eager.keys() {
            keys.push((
                SourceName::new(name, funder)?,
                NativeOrderedKey::prepare(key, position.row_id, funder)?,
            ));
        }
        let row = FundedArc::from_prepared(
            NativeOrderedRow {
                position,
                schema: eager.schema().clone(),
                keys,
            },
            header,
        );
        let mut indexes = prior.and_then(|table| table.indexes.clone());
        if let Some(old) = prior.and_then(|table| {
            crate::staged_source::get_by(&table.rows, |id| position.row_id.cmp(id))
        }) {
            indexes = Self::remove_postings(indexes, old, funder)?;
        }
        for (name, key) in row.keys.as_slice() {
            let old = crate::staged_source::get_by(&indexes, |key| name.cmp(key));
            let ordered = old
                .filter(|index| index.schema.matches_index(&row.schema, name.as_str()))
                .and_then(|index| index.ordered.clone());
            let ordered = insert(
                &ordered,
                FundedArc::try_new(
                    Entry {
                        key: key.clone(),
                        value: position,
                    },
                    funder.clone(),
                )?,
                funder,
            )?;
            let index = FundedArc::try_new(
                NativeOrderedIndex {
                    schema: row.schema.clone(),
                    ordered,
                },
                funder.clone(),
            )?;
            indexes = insert(
                &indexes,
                FundedArc::try_new(
                    Entry {
                        key: name.clone(),
                        value: index,
                    },
                    funder.clone(),
                )?,
                funder,
            )?;
        }
        let rows = insert(
            &prior.and_then(|table| table.rows.clone()),
            FundedArc::try_new(
                Entry {
                    key: position.row_id,
                    value: row,
                },
                funder.clone(),
            )?,
            funder,
        )?;
        let state = FundedArc::try_new(
            NativeOrderedTable {
                statement,
                rows,
                indexes,
                ordered_present: true,
            },
            funder.clone(),
        )?;
        let tables = insert(
            &self.tables,
            FundedArc::try_new(
                Entry {
                    key: SourceName::new(table, funder)?,
                    value: state,
                },
                funder.clone(),
            )?,
            funder,
        )?;
        crate::statement_rows::NativeImage::prepare(
            Self {
                tables,
                vectors: self.vectors.clone(),
                exact: self.exact.clone(),
            },
            funder,
        )
    }
    pub(crate) fn prepare_remove(
        &self,
        table: &str,
        row: RowId,
        funder: &crate::staged_source::SourceFunder,
        copy: Option<crate::staged_source::OriginalCopyGrant>,
    ) -> Result<crate::statement_rows::NativeImage<Self>> {
        use crate::staged_source::{Entry, FundedArc, SourceName, get_by, insert, remove_by};
        let Some(prior) = self.table(table) else {
            return crate::statement_rows::NativeImage::prepare(
                Self {
                    tables: self.tables.clone(),
                    vectors: self.vectors.clone(),
                    exact: self.exact.prepare_filter(
                        |slot| slot.table.as_str() != table || slot.body.row().row_id != row,
                        |_| true,
                        funder,
                    )?,
                },
                funder,
            );
        };
        let statement = prior
            .statement
            .get()
            .prepare_remove(row, funder)?
            .with_original_copy(copy);
        let mut indexes = prior.indexes.clone();
        if let Some(old) = get_by(&prior.rows, |id| row.cmp(id)) {
            indexes = Self::remove_postings(indexes, old, funder)?;
        }
        let rows = remove_by(&prior.rows, &|id| row.cmp(id), funder)?;
        let state = FundedArc::try_new(
            NativeOrderedTable {
                statement,
                rows,
                ordered_present: indexes.is_some(),
                indexes,
            },
            funder.clone(),
        )?;
        let tables = insert(
            &self.tables,
            FundedArc::try_new(
                Entry {
                    key: SourceName::new(table, funder)?,
                    value: state,
                },
                funder.clone(),
            )?,
            funder,
        )?;
        crate::statement_rows::NativeImage::prepare(
            Self {
                tables,
                vectors: self.vectors.clone(),
                exact: self.exact.prepare_filter(
                    |slot| slot.table.as_str() != table || slot.body.row().row_id != row,
                    |_| true,
                    funder,
                )?,
            },
            funder,
        )
    }
    fn remove_postings(
        mut indexes: crate::staged_source::Link<
            crate::staged_source::SourceName,
            crate::staged_source::FundedArc<NativeOrderedIndex>,
        >,
        row: &NativeOrderedRow,
        funder: &crate::staged_source::SourceFunder,
    ) -> Result<
        crate::staged_source::Link<
            crate::staged_source::SourceName,
            crate::staged_source::FundedArc<NativeOrderedIndex>,
        >,
    > {
        use crate::staged_source::{Entry, FundedArc, get_by, insert, remove_by};
        for (name, key) in row.keys.as_slice() {
            let Some(prior) = get_by(&indexes, |key| name.cmp(key)) else {
                continue;
            };
            if !prior.schema.matches_index(&row.schema, name.as_str()) {
                continue;
            }
            let ordered = remove_by(&prior.ordered, &|other| key.cmp(other), funder)?;
            if ordered.is_none() {
                indexes = remove_by(&indexes, &|key| name.cmp(key), funder)?;
            } else {
                let index = FundedArc::try_new(
                    NativeOrderedIndex {
                        schema: prior.schema.clone(),
                        ordered,
                    },
                    funder.clone(),
                )?;
                indexes = insert(
                    &indexes,
                    FundedArc::try_new(
                        Entry {
                            key: name.clone(),
                            value: index,
                        },
                        funder.clone(),
                    )?,
                    funder,
                )?;
            }
        }
        Ok(indexes)
    }
    pub(crate) fn prepare_vector(
        &self,
        index: &contextdb_core::VectorIndexRef,
        vector: NativeEagerVector,
        funder: &crate::staged_source::SourceFunder,
    ) -> Result<crate::statement_rows::NativeImage<Self>> {
        use crate::staged_source::{Entry, FundedArc, SourceName, insert};
        let prior = self.vector_index(index);
        let control = if let Some(prior) = prior {
            prior.control.clone()
        } else {
            FundedArc::try_new(
                NativeVectorControl {
                    name: NativeVectorName {
                        table: SourceName::new(&index.table, funder)?,
                        column: SourceName::new(&index.column, funder)?,
                    },
                },
                funder.clone(),
            )?
        };
        let rows = insert(
            &prior.and_then(|prior| prior.rows.clone()),
            FundedArc::try_new(
                Entry {
                    key: vector.row_id(),
                    value: vector,
                },
                funder.clone(),
            )?,
            funder,
        )?;
        let key = control.name.clone();
        let value = FundedArc::try_new(NativeVectorIndex { rows, control }, funder.clone())?;
        let vectors = insert(
            &self.vectors,
            FundedArc::try_new(Entry { key, value }, funder.clone())?,
            funder,
        )?;
        crate::statement_rows::NativeImage::prepare(
            Self {
                tables: self.tables.clone(),
                vectors,
                exact: self.exact.clone(),
            },
            funder,
        )
    }
    pub(crate) fn prepare_vector_removal(
        &self,
        index: &contextdb_core::VectorIndexRef,
        row: RowId,
        funder: &crate::staged_source::SourceFunder,
    ) -> Result<crate::statement_rows::NativeImage<Self>> {
        use crate::staged_source::{Entry, FundedArc, insert, remove_by};
        let Some(prior) = self.vector_index(index) else {
            return crate::statement_rows::NativeImage::prepare(
                Self {
                    tables: self.tables.clone(),
                    vectors: self.vectors.clone(),
                    exact: self.exact.prepare_filter(
                        |_| true,
                        |slot| {
                            slot.index.table.as_str() != index.table
                                || slot.index.column.as_str() != index.column
                                || slot.body.row_id() != row
                        },
                        funder,
                    )?,
                },
                funder,
            );
        };
        let rows = remove_by(&prior.rows, &|id| row.cmp(id), funder)?;
        let key = prior.control.name.clone();
        // Keep the existing funded per-index control, including empty indexes.
        let value = FundedArc::try_new(
            NativeVectorIndex {
                rows,
                control: prior.control.clone(),
            },
            funder.clone(),
        )?;
        let vectors = insert(
            &self.vectors,
            FundedArc::try_new(Entry { key, value }, funder.clone())?,
            funder,
        )?;
        crate::statement_rows::NativeImage::prepare(
            Self {
                tables: self.tables.clone(),
                vectors,
                exact: self.exact.prepare_filter(
                    |_| true,
                    |slot| {
                        slot.index.table.as_str() != index.table
                            || slot.index.column.as_str() != index.column
                            || slot.body.row_id() != row
                    },
                    funder,
                )?,
            },
            funder,
        )
    }
    fn terminal_legacy(&self) -> OrderedStagedRows {
        let mut output = OrderedStagedRows::default();
        let mut after_table: Option<&str> = None;
        while let Some((name, table)) = crate::staged_source::seek_by(&self.tables, false, |name| {
            if after_table.is_some_and(|after| name.as_str() <= after) {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Equal
            }
        }) {
            output.statement_rows.insert(
                name.as_str().to_owned(),
                Arc::new(
                    table
                        .statement
                        .get()
                        .terminal_legacy(table.statement.clone()),
                ),
            );
            let mut indexes = BTreeMap::new();
            let mut after_index: Option<&str> = None;
            while let Some((index, domain)) =
                crate::staged_source::seek_by(&table.indexes, false, |name| {
                    if after_index.is_some_and(|after| name.as_str() <= after) {
                        std::cmp::Ordering::Less
                    } else {
                        std::cmp::Ordering::Equal
                    }
                })
            {
                let mut postings = IndexPostings {
                    schema: Some(Arc::new(StagedSchema::from_native(domain.schema.clone()))),
                    ..IndexPostings::default()
                };
                let mut after_key = None;
                while let Some((key, position)) =
                    crate::staged_source::seek_by(&domain.ordered, false, |key| {
                        if after_key.is_some_and(|after: &NativeOrderedKey| key <= after) {
                            std::cmp::Ordering::Less
                        } else {
                            std::cmp::Ordering::Equal
                        }
                    })
                {
                    postings.ordered.insert(key.position().clone(), *position);
                    postings
                        .latest
                        .insert(position.row_id, key.position().0.clone());
                    after_key = Some(key);
                }
                indexes.insert(index.as_str().to_owned(), postings);
                after_index = Some(index.as_str());
            }
            if table.ordered_present {
                output.tables.insert(name.as_str().to_owned(), indexes);
            }
            let mut charges = BTreeMap::new();
            let mut after_row = None;
            while let Some((id, row)) = crate::staged_source::seek_by(&table.rows, false, |id| {
                if after_row.is_some_and(|after| *id <= after) {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Equal
                }
            }) {
                let charge = NativeOrderedCharge(row.clone());
                charges.insert(
                    *id,
                    (
                        charge.retained_bytes(),
                        Arc::new(charge) as Arc<dyn StagedPostingCharge>,
                    ),
                );
                after_row = Some(*id);
            }
            if !charges.is_empty() {
                output.charges.insert(name.as_str().to_owned(), charges);
            }
            after_table = Some(name.as_str());
        }
        let mut after_vector = None;
        while let Some((name, index)) =
            crate::staged_source::seek_by(&self.vectors, false, |name| {
                if after_vector.is_some_and(|after: &NativeVectorName| name <= after) {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Equal
                }
            })
        {
            let mut rows = BTreeMap::new();
            let mut after_row = None;
            while let Some((id, vector)) = crate::staged_source::seek_by(&index.rows, false, |id| {
                if after_row.is_some_and(|after| *id <= after) {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Equal
                }
            }) {
                rows.insert(*id, vector.terminal_shell());
                after_row = Some(*id);
            }
            output.vectors.insert(
                &contextdb_core::VectorIndexRef {
                    table: name.table.as_str().to_owned(),
                    column: name.column.as_str().to_owned(),
                },
                StagedVectorIndex {
                    rows,
                    _charge: Arc::new(NativeVectorDirectoryCharge(index.control.clone())),
                },
            );
            after_vector = Some(name);
        }
        output
    }
}
#[derive(Debug)]
struct NativeOrderedCharge(crate::staged_source::FundedArc<NativeOrderedRow>);
impl StagedPostingCharge for NativeOrderedCharge {
    fn retained_bytes(&self) -> usize {
        self.0
            .retained_bytes()
            .saturating_add(self.0.keys.retained_bytes())
            .saturating_add(
                self.0
                    .keys
                    .as_slice()
                    .iter()
                    .map(|(name, key)| {
                        name.retained_bytes()
                            .saturating_add(key.0.retained_bytes())
                            .saturating_add(key.0._charge.bytes())
                    })
                    .sum::<usize>(),
            )
    }
}
#[derive(Debug)]
struct NativeVectorDirectoryCharge(crate::staged_source::FundedArc<NativeVectorControl>);
impl StagedPostingCharge for NativeVectorDirectoryCharge {
    fn retained_bytes(&self) -> usize {
        self.0
            .retained_bytes()
            .saturating_add(self.0.name.table.retained_bytes())
            .saturating_add(self.0.name.column.retained_bytes())
    }
}

struct NativeOrderedRange<'a> {
    root: &'a crate::staged_source::Link<NativeOrderedKey, StagedPosition>,
    lower: Option<&'a NativeOrderedKey>,
    upper: Option<&'a NativeOrderedKey>,
    last: Option<&'a NativeOrderedKey>,
    prefix: Option<&'a IndexKey>,
}
impl<'a> NativeOrderedRange<'a> {
    fn new(
        root: &'a crate::staged_source::Link<NativeOrderedKey, StagedPosition>,
        lower: Bound<&(IndexKey, RowId)>,
        upper: Bound<&(IndexKey, RowId)>,
        prefix: Option<&'a IndexKey>,
    ) -> Self {
        let compare = |key: &NativeOrderedKey| {
            let position = key.position();
            match lower {
                Bound::Included(bound) if position < bound => return std::cmp::Ordering::Less,
                Bound::Excluded(bound) if position <= bound => return std::cmp::Ordering::Less,
                _ => {}
            }
            match upper {
                Bound::Included(bound) if position > bound => return std::cmp::Ordering::Greater,
                Bound::Excluded(bound) if position >= bound => return std::cmp::Ordering::Greater,
                _ => {}
            }
            std::cmp::Ordering::Equal
        };
        let first = crate::staged_source::seek_by(root, false, compare).map(|entry| entry.0);
        let final_key = crate::staged_source::seek_by(root, true, compare).map(|entry| entry.0);
        Self {
            root,
            lower: first,
            upper: final_key,
            last: None,
            prefix,
        }
    }
    fn next(&mut self, reverse: bool) -> Option<(&'a NativeOrderedKey, &'a StagedPosition)> {
        let lower = self.lower?;
        let upper = self.upper?;
        let last = self.last;
        let prefix = self.prefix;
        let entry = crate::staged_source::seek_by(self.root, reverse, |key| {
            let position = key.position();
            if key < lower {
                return std::cmp::Ordering::Less;
            }
            if key > upper {
                return std::cmp::Ordering::Greater;
            }
            if let Some(prefix) = prefix {
                let common = prefix.len().min(position.0.len());
                let order = position.0[..common].cmp(&prefix[..common]);
                if order != std::cmp::Ordering::Equal {
                    return order;
                }
                if position.0.len() < prefix.len() {
                    return std::cmp::Ordering::Less;
                }
            }
            if let Some(last) = last {
                if reverse && key >= last {
                    return std::cmp::Ordering::Greater;
                }
                if !reverse && key <= last {
                    return std::cmp::Ordering::Less;
                }
            }
            std::cmp::Ordering::Equal
        })?;
        self.last = Some(entry.0);
        Some(entry)
    }
}
impl NativeOrderedRoot {
    fn is_empty(&self) -> bool {
        let mut after: Option<&str> = None;
        while let Some((name, table)) = crate::staged_source::seek_by(&self.tables, false, |name| {
            if after.is_some_and(|after| name.as_str() <= after) {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Equal
            }
        }) {
            if table.ordered_present {
                return false;
            }
            after = Some(name.as_str());
        }
        true
    }
    fn vector_retained_bytes(&self) -> usize {
        let mut bytes = crate::staged_source::retained_link_bytes(&self.vectors);
        let mut after = None;
        while let Some((name, index)) =
            crate::staged_source::seek_by(&self.vectors, false, |name| {
                if after.is_some_and(|after: &NativeVectorName| name <= after) {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Equal
                }
            })
        {
            bytes = bytes
                .saturating_add(index.retained_bytes())
                .saturating_add(NativeVectorDirectoryCharge(index.control.clone()).retained_bytes())
                .saturating_add(crate::staged_source::retained_link_bytes(&index.rows));
            let mut after_row = None;
            while let Some((id, vector)) = crate::staged_source::seek_by(&index.rows, false, |id| {
                if after_row.is_some_and(|after| *id <= after) {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Equal
                }
            }) {
                bytes = bytes.saturating_add(vector.retained_bytes());
                after_row = Some(*id);
            }
            after = Some(name);
        }
        bytes
    }
    fn retained_bytes(&self) -> usize {
        let mut bytes = self
            .vector_retained_bytes()
            .saturating_add(crate::staged_source::retained_link_bytes(&self.tables));
        let mut after: Option<&str> = None;
        while let Some((name, table)) = crate::staged_source::seek_by(&self.tables, false, |name| {
            if after.is_some_and(|after| name.as_str() <= after) {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Equal
            }
        }) {
            bytes = bytes
                .saturating_add(name.retained_bytes())
                .saturating_add(table.retained_bytes())
                .saturating_add(table.statement.control_bytes())
                .saturating_add(table.statement.get().retained_bytes())
                .saturating_add(crate::staged_source::retained_link_bytes(&table.rows))
                .saturating_add(crate::staged_source::retained_link_bytes(&table.indexes));
            let mut after_row = None;
            while let Some((id, row)) = crate::staged_source::seek_by(&table.rows, false, |id| {
                if after_row.is_some_and(|after| *id <= after) {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Equal
                }
            }) {
                bytes = bytes.saturating_add(
                    row.retained_bytes()
                        .saturating_add(row.keys.retained_bytes())
                        .saturating_add(
                            row.keys
                                .as_slice()
                                .iter()
                                .map(|(name, key)| {
                                    name.retained_bytes()
                                        .saturating_add(key.0.retained_bytes())
                                        .saturating_add(key.0._charge.bytes())
                                })
                                .sum::<usize>(),
                        ),
                );
                after_row = Some(*id);
            }
            let mut after_index: Option<&str> = None;
            while let Some((name, index)) =
                crate::staged_source::seek_by(&table.indexes, false, |name| {
                    if after_index.is_some_and(|after| name.as_str() <= after) {
                        std::cmp::Ordering::Less
                    } else {
                        std::cmp::Ordering::Equal
                    }
                })
            {
                bytes = bytes
                    .saturating_add(name.retained_bytes())
                    .saturating_add(index.retained_bytes())
                    .saturating_add(crate::staged_source::retained_link_bytes(&index.ordered));
                after_index = Some(name.as_str());
            }
            after = Some(name.as_str());
        }
        bytes
    }
}

/// This token is a complete eager successor. It never imports bare mutable
/// maps, constructs a failure image or changes the original payload invoice.
pub(crate) struct PreparedNativeChange {
    expected: Option<crate::statement_rows::NativeImageValidation<NativeOrderedRoot>>,
    successor: NativeSuccessor,
    // Bulk's genuine first-row original collection grant stays after the
    // successor in destruction order until attached to its final native image.
    original_copy: Option<crate::staged_source::OriginalCopyGrant>,
}
enum NativeSuccessor {
    Unchanged,
    Replace(StagedVectorDirectory),
}
impl PreparedNativeChange {
    /// Compose positions against the admitted, unpublished removal successor.
    /// The original host validator and genuine grants survive preparation.
    pub(crate) fn prepare_rebind(
        mut self,
        positions: &HashMap<&str, HashMap<RowId, (usize, TxId)>>,
        funder: &crate::staged_source::SourceFunder,
    ) -> Result<Self> {
        let NativeSuccessor::Replace(directory) = &self.successor else {
            assert!(
                self.original_copy.is_none(),
                "unchanged image has no bulk grant"
            );
            return Ok(self);
        };
        let body = directory.native_body().expect("prepared native directory");
        let NativeDirectoryImage::Active(image) = &body.image else {
            unreachable!("prepared successor is active");
        };
        // Keep the whole token across every refusal: field destruction frees
        // its successor before refunding the genuine bulk collection grant.
        // Statement grants remain on their carried images through rebind.
        let successor = image
            .get()
            .prepare_positions(Some(positions), None, funder)?;
        // Free the intermediate native directory before a final directory
        // admission can refuse. The prepared image retains statement grants.
        self.successor = NativeSuccessor::Unchanged;
        let successor = StagedVectorDirectory::prepare_native(successor, funder)?;
        self.successor = NativeSuccessor::Replace(successor);
        Ok(self)
    }
    fn finish_original_copy(mut self) -> Self {
        if let Some(grant) = self.original_copy.take() {
            let NativeSuccessor::Replace(directory) = &mut self.successor else {
                unreachable!("bulk collection belongs to a prepared successor");
            };
            let body = directory
                .0
                .as_mut()
                .and_then(DirectoryOwner::native_body_mut)
                .expect("prepared native directory");
            let NativeDirectoryImage::Active(image) = &body.image else {
                unreachable!("prepared successor is active");
            };
            // A nonsemantic temporary pin allows moving the image out of its
            // already admitted directory without a placeholder allocation.
            let pin = image.restore_pin();
            let NativeDirectoryImage::Active(image) =
                std::mem::replace(&mut body.image, NativeDirectoryImage::Restore(pin))
            else {
                unreachable!("active successor")
            };
            body.image = NativeDirectoryImage::Active(image.with_original_copy(Some(grant)));
        }
        self
    }
}
impl PreparedNativeRemovals {
    pub(crate) fn needs_rebind(&self) -> bool {
        matches!(&self.0.successor, NativeSuccessor::Replace(_))
    }
    pub(crate) fn prepare_rebind(
        self,
        positions: &HashMap<&str, HashMap<RowId, (usize, TxId)>>,
        funder: &crate::staged_source::SourceFunder,
    ) -> Result<Self> {
        self.0.prepare_rebind(positions, funder).map(Self)
    }
}
impl OrderedStagedRows {
    fn unchanged_native(&self) -> PreparedNativeChange {
        PreparedNativeChange {
            expected: self
                .native_image()
                .map(crate::statement_rows::NativeImage::validation_handle),
            successor: NativeSuccessor::Unchanged,
            original_copy: None,
        }
    }
    fn native_preparation_state(&self) -> Result<()> {
        if !self.tables.is_empty()
            || !self.charges.is_empty()
            || !self.statement_rows.is_empty()
            || (self.vectors.roots().is_some() && self.native_image().is_none())
            || self.checkpoint_depth != 0
        {
            return Err(contextdb_core::Error::Other(
                "native eager preparation requires protected source custody".into(),
            ));
        }
        Ok(())
    }
    pub(crate) fn prepare_native_row(
        &self,
        table: &str,
        position: StagedPosition,
        row: &contextdb_core::VersionedRow,
        keys: &[(String, IndexKey)],
        schema: crate::staged_source::SchemaHandle,
        funder: &crate::staged_source::SourceFunder,
    ) -> Result<PreparedNativeChange> {
        self.native_preparation_state()?;
        if position.row_id != row.row_id || position.created_tx != row.created_tx {
            return Err(contextdb_core::Error::Other(
                "captured staged source identity mismatch".into(),
            ));
        }
        let empty = NativeOrderedRoot::empty();
        let root = self.native_root().unwrap_or(&empty);
        let copied = funder.clone().copy_eager_row(row)?;
        let eager = crate::statement_rows::NativeEagerRow::prepare(
            copied,
            root.next_sequence(table),
            keys,
            schema,
            funder,
        )?;
        let successor = root.prepare_row(table, position, eager, funder)?;
        Ok(PreparedNativeChange {
            expected: self
                .native_image()
                .map(crate::statement_rows::NativeImage::validation_handle),
            successor: NativeSuccessor::Replace(StagedVectorDirectory::prepare_native(
                successor, funder,
            )?),
            original_copy: None,
        })
    }
    pub(crate) fn prepare_native_removal_admission<'a>(
        &'a self,
        table: &'a str,
        row: RowId,
    ) -> Result<NativeRemovalAdmission<'a>> {
        self.native_preparation_state()?;
        let image = self.native_image();
        let selected = image.and_then(|image| image.get().table(table));
        let shared = image.is_some_and(crate::statement_rows::NativeImage::is_shared)
            || selected.is_some_and(|table| table.statement.is_shared());
        let grant = if shared {
            selected
                .and_then(|table| table.statement.get().row(row))
                .map(|owner| {
                    owner.reserve_private_original_copy(
                        selected
                            .expect("selected table")
                            .statement
                            .get()
                            .legacy_control_bytes(),
                    )
                })
                .transpose()?
        } else {
            None
        };
        Ok(NativeRemovalAdmission {
            source: self,
            table,
            row,
            grant,
        })
    }
    pub(crate) fn prepare_native_vector(
        &self,
        index: &contextdb_core::VectorIndexRef,
        vector: NativeEagerVector,
        funder: &crate::staged_source::SourceFunder,
    ) -> Result<PreparedNativeChange> {
        self.native_preparation_state()?;
        if vector.0.column.as_str() != index.column
            || matches!(&vector.0.payload,NativeEagerVectorPayload::Owned(original) if original.entry.index!=*index)
        {
            return Err(contextdb_core::Error::Other(
                "captured staged source identity mismatch".into(),
            ));
        }
        let empty = NativeOrderedRoot::empty();
        let successor = self
            .native_root()
            .unwrap_or(&empty)
            .prepare_vector(index, vector, funder)?;
        Ok(PreparedNativeChange {
            expected: self
                .native_image()
                .map(crate::statement_rows::NativeImage::validation_handle),
            successor: NativeSuccessor::Replace(StagedVectorDirectory::prepare_native(
                successor, funder,
            )?),
            original_copy: None,
        })
    }
    // activated by bounded-storage S3/S4; remove allow at activation
    #[allow(dead_code)]
    pub(crate) fn prepare_native_moved_vector(
        &self,
        index: &contextdb_core::VectorIndexRef,
        from: RowId,
        to: RowId,
        funder: &crate::staged_source::SourceFunder,
    ) -> Result<Option<PreparedNativeChange>> {
        self.native_preparation_state()?;
        if from == to {
            return Ok(Some(self.unchanged_native()));
        }
        let Some(root) = self.native_root() else {
            return Ok(None);
        };
        let Some(source) = root
            .vector_index(index)
            .and_then(|index| crate::staged_source::get_by(&index.rows, |id| from.cmp(id)))
        else {
            return Ok(None);
        };
        let vector = source.prepare_move(to, funder)?;
        self.prepare_native_vector(index, vector, funder).map(Some)
    }
    pub(crate) fn prepare_native_vector_remove(
        &self,
        index: &contextdb_core::VectorIndexRef,
        row: RowId,
        funder: &crate::staged_source::SourceFunder,
    ) -> Result<PreparedNativeChange> {
        self.native_preparation_state()?;
        if self
            .native_root()
            .and_then(|root| root.vector_index(index))
            .and_then(|index| crate::staged_source::get_by(&index.rows, |id| row.cmp(id)))
            .is_none()
        {
            return Ok(self.unchanged_native());
        }
        let empty = NativeOrderedRoot::empty();
        let successor = self
            .native_root()
            .unwrap_or(&empty)
            .prepare_vector_removal(index, row, funder)?;
        Ok(PreparedNativeChange {
            expected: self
                .native_image()
                .map(crate::statement_rows::NativeImage::validation_handle),
            successor: NativeSuccessor::Replace(StagedVectorDirectory::prepare_native(
                successor, funder,
            )?),
            original_copy: None,
        })
    }
    pub(crate) fn prepare_native_rebind(
        &self,
        positions: &HashMap<&str, HashMap<RowId, (usize, TxId)>>,
        funder: &crate::staged_source::SourceFunder,
    ) -> Result<PreparedNativeChange> {
        self.native_preparation_state()?;
        let empty = NativeOrderedRoot::empty();
        let successor = self.native_root().unwrap_or(&empty).prepare_positions(
            Some(positions),
            None,
            funder,
        )?;
        Ok(PreparedNativeChange {
            expected: self
                .native_image()
                .map(crate::statement_rows::NativeImage::validation_handle),
            successor: NativeSuccessor::Replace(StagedVectorDirectory::prepare_native(
                successor, funder,
            )?),
            original_copy: None,
        })
    }
    pub(crate) fn prepare_native_reassign(
        &self,
        from: TxId,
        to: TxId,
        funder: &crate::staged_source::SourceFunder,
    ) -> Result<PreparedNativeChange> {
        self.native_preparation_state()?;
        if from == to {
            return Ok(self.unchanged_native());
        }
        let empty = NativeOrderedRoot::empty();
        let successor = self.native_root().unwrap_or(&empty).prepare_positions(
            None,
            Some((from, to)),
            funder,
        )?;
        Ok(PreparedNativeChange {
            expected: self
                .native_image()
                .map(crate::statement_rows::NativeImage::validation_handle),
            successor: NativeSuccessor::Replace(StagedVectorDirectory::prepare_native(
                successor, funder,
            )?),
            original_copy: None,
        })
    }
    pub(crate) fn validate_native_change(
        &self,
        prepared: PreparedNativeChange,
    ) -> Result<ValidatedNativeChange> {
        self.native_preparation_state()?;
        let matches = match (self.native_image(), &prepared.expected) {
            (None, None) => true,
            (Some(actual), Some(expected)) => expected.matches(actual),
            _ => false,
        };
        if !matches {
            return Err(contextdb_core::Error::Other(
                "native eager prepared source changed".into(),
            ));
        }
        Ok(ValidatedNativeChange {
            successor: prepared.finish_original_copy().successor,
        })
    }
    /// The engine validates its node generation/scope/revision and every
    /// ordinary binding before calling this infallible final swap under its guard.
    pub(crate) fn publish_validated_native_change(&mut self, prepared: ValidatedNativeChange) {
        if let NativeSuccessor::Replace(directory) = prepared.successor {
            self.vectors = directory;
        }
    }
    // activated by bounded-storage S3/S4; remove allow at activation
    #[allow(dead_code)]
    pub(crate) fn publish_native_change(&mut self, prepared: PreparedNativeChange) -> Result<()> {
        let validated = self.validate_native_change(prepared)?;
        self.publish_validated_native_change(validated);
        Ok(())
    }
    pub(crate) fn prepare_native_checkpoint(
        &self,
        funder: &crate::staged_source::SourceFunder,
    ) -> Result<NativeOrderedCheckpoint> {
        self.native_preparation_state()?;
        let directory = if let Some(image) = self.native_image() {
            StagedVectorDirectory::prepare_native_body(
                NativeDirectoryImage::Restore(image.restore_pin()),
                funder,
            )?
        } else {
            StagedVectorDirectory::default()
        };
        Ok(NativeOrderedCheckpoint(directory))
    }
    pub(crate) fn restore_native_checkpoint(&mut self, mut prior: NativeOrderedCheckpoint) {
        prior.0.promote_restore();
        self.vectors = prior.0;
    }
}
/// Per-table staged row positions supplied to a native posting rebind.
type NativePositionsByTable<'a> = HashMap<&'a str, HashMap<RowId, (usize, TxId)>>;
impl NativeOrderedRoot {
    /// Rebind only the native posting directory and its current identity rows.
    /// Captured statement bodies, row sequence and comparator domains remain
    /// the exact earlier immutable objects, as in the public Legacy path.
    fn prepare_positions(
        &self,
        positions: Option<&NativePositionsByTable<'_>>,
        reassign: Option<(TxId, TxId)>,
        funder: &crate::staged_source::SourceFunder,
    ) -> Result<crate::statement_rows::NativeImage<Self>> {
        use crate::staged_source::{Entry, FundedArc, SourceBuffer, insert};
        let mut tables = None;
        let mut after_table: Option<&str> = None;
        while let Some((name, table)) = crate::staged_source::seek_by(&self.tables, false, |name| {
            if after_table.is_some_and(|after| name.as_str() <= after) {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Equal
            }
        }) {
            let mut rows = None;
            let mut after_row = None;
            while let Some((id, row)) = crate::staged_source::seek_by(&table.rows, false, |id| {
                if after_row.is_some_and(|after| *id <= after) {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Equal
                }
            }) {
                let replacement = positions.and_then(|positions| {
                    positions.get(name.as_str()).and_then(|rows| rows.get(id))
                });
                if positions.is_none() || replacement.is_some() {
                    let mut position = row.position;
                    if let Some((index, tx)) = replacement {
                        position.position = *index;
                        position.created_tx = *tx;
                    }
                    if let Some((from, to)) = reassign
                        && position.created_tx == from
                    {
                        position.created_tx = to;
                    }
                    let header = FundedArc::<NativeOrderedRow>::prepare(funder.clone())?;
                    let mut keys = SourceBuffer::with_capacity(row.keys.len(), funder)?;
                    if keys.capacity() != row.keys.len() {
                        return Err(contextdb_core::Error::Other(
                            "ordered key capacity mismatch".into(),
                        ));
                    }
                    for (name, key) in row.keys.as_slice() {
                        keys.push((name.clone(), key.clone()));
                    }
                    let row = FundedArc::from_prepared(
                        NativeOrderedRow {
                            position,
                            schema: row.schema.clone(),
                            keys,
                        },
                        header,
                    );
                    rows = insert(
                        &rows,
                        FundedArc::try_new(
                            Entry {
                                key: *id,
                                value: row,
                            },
                            funder.clone(),
                        )?,
                        funder,
                    )?;
                }
                after_row = Some(*id);
            }
            let mut indexes = None;
            let mut after_index: Option<&str> = None;
            while let Some((index, domain)) =
                crate::staged_source::seek_by(&table.indexes, false, |name| {
                    if after_index.is_some_and(|after| name.as_str() <= after) {
                        std::cmp::Ordering::Less
                    } else {
                        std::cmp::Ordering::Equal
                    }
                })
            {
                let mut ordered = None;
                let mut after_key = None;
                while let Some((key, position)) =
                    crate::staged_source::seek_by(&domain.ordered, false, |key| {
                        if after_key.is_some_and(|after: &NativeOrderedKey| key <= after) {
                            std::cmp::Ordering::Less
                        } else {
                            std::cmp::Ordering::Equal
                        }
                    })
                {
                    if let Some(row) =
                        crate::staged_source::get_by(&rows, |id| position.row_id.cmp(id))
                    {
                        ordered = insert(
                            &ordered,
                            FundedArc::try_new(
                                Entry {
                                    key: key.clone(),
                                    value: row.position,
                                },
                                funder.clone(),
                            )?,
                            funder,
                        )?;
                    }
                    after_key = Some(key);
                }
                if ordered.is_some() {
                    let value = FundedArc::try_new(
                        NativeOrderedIndex {
                            schema: domain.schema.clone(),
                            ordered,
                        },
                        funder.clone(),
                    )?;
                    indexes = insert(
                        &indexes,
                        FundedArc::try_new(
                            Entry {
                                key: index.clone(),
                                value,
                            },
                            funder.clone(),
                        )?,
                        funder,
                    )?;
                }
                after_index = Some(index.as_str());
            }
            let ordered_present = if positions.is_some() {
                indexes.is_some()
            } else {
                table.ordered_present
            };
            let value = FundedArc::try_new(
                NativeOrderedTable {
                    statement: table.statement.clone(),
                    rows,
                    indexes,
                    ordered_present,
                },
                funder.clone(),
            )?;
            tables = insert(
                &tables,
                FundedArc::try_new(
                    Entry {
                        key: name.clone(),
                        value,
                    },
                    funder.clone(),
                )?,
                funder,
            )?;
            after_table = Some(name.as_str());
        }
        crate::statement_rows::NativeImage::prepare(
            Self {
                tables,
                vectors: self.vectors.clone(),
                exact: self.exact.clone(),
            },
            funder,
        )
    }
}

pub(crate) struct NativeRemovalAdmission<'a> {
    source: &'a OrderedStagedRows,
    table: &'a str,
    row: RowId,
    grant: Option<crate::staged_source::OriginalCopyGrant>,
}
impl NativeRemovalAdmission<'_> {
    /// Caller C prepares its existing undo/posting/journal controls between
    /// the genuine O admission above and this allocation-only successor step.
    pub(crate) fn prepare(
        self,
        funder: &crate::staged_source::SourceFunder,
    ) -> Result<PreparedNativeChange> {
        if self
            .source
            .native_root()
            .and_then(|root| root.table(self.table))
            .and_then(|table| table.statement.get().row(self.row))
            .is_none()
        {
            return Ok(self.source.unchanged_native());
        }
        let empty = NativeOrderedRoot::empty();
        let successor = self
            .source
            .native_root()
            .unwrap_or(&empty)
            .prepare_remove(self.table, self.row, funder, self.grant)?;
        Ok(PreparedNativeChange {
            expected: self
                .source
                .native_image()
                .map(crate::statement_rows::NativeImage::validation_handle),
            successor: NativeSuccessor::Replace(StagedVectorDirectory::prepare_native(
                successor, funder,
            )?),
            original_copy: None,
        })
    }
}

pub(crate) struct NativeOrderedCheckpoint(StagedVectorDirectory);

/// The existing pointer-width slot owns exactly one of two concrete boxes.
#[repr(transparent)]
struct DirectoryOwner(std::ptr::NonNull<()>);
// SAFETY: both exclusive pointees contain only Send + Sync values. No pointer
// or mutable alias escapes; mutation requires &mut DirectoryOwner.
unsafe impl Send for DirectoryOwner {}
unsafe impl Sync for DirectoryOwner {}
impl DirectoryOwner {
    fn legacy(value: Box<Option<StagedVectorRoots>>) -> Self {
        Self(std::ptr::NonNull::from(Box::leak(value)).cast())
    }
    fn native(value: Box<NativeDirectoryBody>) -> Self {
        let pointer = std::ptr::NonNull::from(Box::leak(value)).cast::<()>();
        Self(pointer.map_addr(|address| address.checked_add(1).expect("aligned directory")))
    }
    fn is_native(&self) -> bool {
        self.0.addr().get() & 1 == 1
    }
    fn native_ptr(&self) -> std::ptr::NonNull<NativeDirectoryBody> {
        assert!(self.is_native());
        self.0
            .map_addr(|address| {
                std::num::NonZeroUsize::new(address.get() & !1).expect("native box")
            })
            .cast()
    }
    fn native_body(&self) -> Option<&NativeDirectoryBody> {
        if !self.is_native() {
            return None;
        }
        // SAFETY: tag selects the exact live typed allocation exclusively owned here.
        Some(unsafe { self.native_ptr().as_ref() })
    }
    fn native_body_mut(&mut self) -> Option<&mut NativeDirectoryBody> {
        if !self.is_native() {
            return None;
        }
        // SAFETY: exclusive receiver, exact typed allocation, no exported mutable aliases.
        Some(unsafe { self.native_ptr().as_mut() })
    }
}
impl std::ops::Deref for DirectoryOwner {
    type Target = Option<StagedVectorRoots>;
    fn deref(&self) -> &Self::Target {
        assert!(
            !self.is_native(),
            "legacy directory access requires a public terminal copy"
        );
        // SAFETY: untagged pointer is the original exact Box<Option<StagedVectorRoots>>.
        unsafe { self.0.cast::<Option<StagedVectorRoots>>().as_ref() }
    }
}
impl std::ops::DerefMut for DirectoryOwner {
    fn deref_mut(&mut self) -> &mut Self::Target {
        assert!(
            !self.is_native(),
            "legacy directory mutation requires a public terminal copy"
        );
        // SAFETY: exclusive receiver, original exact box, no aliases.
        unsafe { self.0.cast::<Option<StagedVectorRoots>>().as_mut() }
    }
}
impl Clone for StagedVectorDirectory {
    fn clone(&self) -> Self {
        if let Some(body) = self.native_body() {
            return body.image.get().terminal_legacy().vectors;
        }
        Self(
            self.0
                .as_ref()
                .map(|owner| DirectoryOwner::legacy(Box::new((**owner).clone()))),
        )
    }
}
impl std::fmt::Debug for DirectoryOwner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(body) = self.native_body() {
            body.image.get().fmt(f)
        } else {
            (**self).fmt(f)
        }
    }
}
impl Drop for DirectoryOwner {
    fn drop(&mut self) {
        if self.is_native() {
            let raw = self.native_ptr().as_ptr();
            // SAFETY: read moves the exact typed body to stack, then frees its
            // original global Box layout before any image or credit can drop.
            let body = unsafe { std::ptr::read(raw) };
            unsafe {
                std::alloc::dealloc(raw.cast(), std::alloc::Layout::new::<NativeDirectoryBody>())
            };
            // Field order frees images before box credit, including unwind.
            drop(body);
        } else {
            // SAFETY: pointer came from Box::leak of precisely this type/layout.
            let mut allocation =
                unsafe { Box::from_raw(self.0.cast::<Option<StagedVectorRoots>>().as_ptr()) };
            let roots = allocation.take();
            drop(allocation);
            match roots {
                Some(StagedVectorRoots::Single(key, state)) => {
                    drop(key);
                    drop(state);
                }
                Some(StagedVectorRoots::Many(mut entries)) => {
                    while let Some((key, state)) = entries.pop_first() {
                        if entries.is_empty() {
                            entries = BTreeMap::new();
                        }
                        drop(key);
                        drop(state);
                    }
                }
                None => {}
            }
        }
    }
}
struct NativeDirectoryBody {
    image: NativeDirectoryImage,
    credit: crate::staged_source::SourceCharge,
}
enum NativeDirectoryImage {
    Active(crate::statement_rows::NativeImage<NativeOrderedRoot>),
    Restore(crate::statement_rows::NativeImageRestore<NativeOrderedRoot>),
}
impl NativeDirectoryImage {
    fn get(&self) -> &NativeOrderedRoot {
        match self {
            Self::Active(image) => image.get(),
            Self::Restore(pin) => pin.get(),
        }
    }
}
impl StagedVectorDirectory {
    fn prepare_native(
        image: crate::statement_rows::NativeImage<NativeOrderedRoot>,
        funder: &crate::staged_source::SourceFunder,
    ) -> Result<Self> {
        Self::prepare_native_body(NativeDirectoryImage::Active(image), funder)
    }
    fn prepare_native_body(
        image: NativeDirectoryImage,
        funder: &crate::staged_source::SourceFunder,
    ) -> Result<Self> {
        let credit = crate::staged_source::SourceCharge::admit_funded(
            funder.clone(),
            std::alloc::Layout::new::<NativeDirectoryBody>().size(),
        )?;
        Ok(Self(Some(DirectoryOwner::native(Box::new(
            NativeDirectoryBody { image, credit },
        )))))
    }
    fn native_body(&self) -> Option<&NativeDirectoryBody> {
        self.0.as_ref()?.native_body()
    }
    fn promote_restore(&mut self) {
        if let Some(body) = self.0.as_mut().and_then(DirectoryOwner::native_body_mut)
            && let NativeDirectoryImage::Restore(pin) = &body.image
        {
            // A paired count clone allocates/admit nothing. The old pin drops
            // only after the promoted semantic image owns the same data/grant.
            let image = pin.clone().into_image();
            body.image = NativeDirectoryImage::Active(image);
        }
    }
}
impl OrderedStagedRows {
    fn native_image(&self) -> Option<&crate::statement_rows::NativeImage<NativeOrderedRoot>> {
        match &self.vectors.native_body()?.image {
            NativeDirectoryImage::Active(image) => Some(image),
            _ => None,
        }
    }
    fn native_root(&self) -> Option<&NativeOrderedRoot> {
        self.native_image()
            .map(crate::statement_rows::NativeImage::get)
    }
}

#[derive(Debug)]
struct TerminalVectorControl(NativeEagerVector);
impl StagedPostingCharge for TerminalVectorControl {
    fn retained_bytes(&self) -> usize {
        self.0
            .0
            .retained_bytes()
            .saturating_add(self.0.0.column.retained_bytes())
    }
}
#[derive(Debug)]
struct TerminalVectorPayload(
    crate::staged_source::FundedArc<crate::staged_source::AdmittedEagerSourceVector>,
);
impl StagedPostingCharge for TerminalVectorPayload {
    fn retained_bytes(&self) -> usize {
        self.0
            .retained_bytes()
            .saturating_add(self.0.invoice.retained_bytes())
            .saturating_add(self.0.original_copy.retained_bytes())
    }
    fn reserve_copy(&self, bytes: usize) -> Result<Arc<dyn StagedPostingCharge>> {
        Ok(Arc::new(crate::statement_rows::NativeCopyCharge {
            grant: self.0.original_copy.reserve_private_original_copy(bytes)?,
        }))
    }
}
impl NativeEagerVector {
    fn terminal_shell(&self) -> Arc<StagedVectorCapability> {
        let payload = match &self.0.payload {
            NativeEagerVectorPayload::Scalar(row) => StagedVectorPayload::Scalar(
                row.terminal_owner(Arc::new(StagedSchema::from_native(row.schema().clone()))),
            ),
            NativeEagerVectorPayload::Owned(payload) => {
                StagedVectorPayload::Owned(Arc::new(AdmittedVectorPayload {
                    values: payload.entry.vector.clone(),
                    _charge: Arc::new(TerminalVectorPayload(payload.clone())),
                }))
            }
        };
        Arc::new(StagedVectorCapability {
            row_id: self.row_id(),
            column: self.0.column.as_str().to_owned(),
            payload,
            _charge: Arc::new(TerminalVectorControl(self.clone())),
        })
    }
    // activated by bounded-storage S3/S4; remove allow at activation
    #[allow(dead_code)]
    fn reserve_private_original_copy(
        &self,
        bytes: usize,
    ) -> Result<crate::staged_source::OriginalCopyGrant> {
        match &self.0.payload {
            NativeEagerVectorPayload::Scalar(row) => row.reserve_private_original_copy(bytes),
            NativeEagerVectorPayload::Owned(payload) => {
                payload.original_copy.reserve_private_original_copy(bytes)
            }
        }
    }
}

pub(crate) struct ValidatedNativeChange {
    successor: NativeSuccessor,
}

const _: () = {
    assert!(std::mem::align_of::<Option<StagedVectorRoots>>() >= 2);
    assert!(std::mem::align_of::<NativeDirectoryBody>() >= 2);
    assert!(
        std::mem::size_of::<StagedVectorDirectory>()
            == std::mem::size_of::<Option<Box<Option<StagedVectorRoots>>>>()
    );
    assert!(
        std::mem::align_of::<StagedVectorDirectory>()
            == std::mem::align_of::<Option<Box<Option<StagedVectorRoots>>>>()
    );
};

/// Bulk selection stays borrowed; original grants remain on the preparation
/// stack until every original owner has admitted, before the caller's C door.
pub(crate) struct NativeBulkRemovalAdmission<'a> {
    source: &'a OrderedStagedRows,
    selected: &'a std::collections::BTreeSet<(String, RowId)>,
    collection: Option<crate::staged_source::OriginalCopyGrant>,
}
pub(crate) struct PreparedNativeRemovals(PreparedNativeChange);
impl OrderedStagedRows {
    pub(crate) fn prepare_native_bulk_removal_admission<'a>(
        &'a self,
        selected: &'a std::collections::BTreeSet<(String, RowId)>,
    ) -> Result<NativeBulkRemovalAdmission<'a>> {
        self.native_preparation_state()?;
        let mut bytes = 0usize;
        let mut first = None;
        if let Some(root) = self.native_root() {
            let mut after = None;
            while let Some((name, table)) = root.next_selected_table(selected, after) {
                bytes = bytes
                    .saturating_add(name.len())
                    .saturating_add(std::mem::size_of::<(
                        String,
                        crate::statement_rows::PreparedStatementRemoval,
                    )>());
                if first.is_none() {
                    first = table.statement.get().first_row();
                }
                after = Some(name);
            }
        }
        let collection = first
            .map(|row| row.reserve_private_original_copy(bytes))
            .transpose()?;
        Ok(NativeBulkRemovalAdmission {
            source: self,
            selected,
            collection,
        })
    }
    pub(crate) fn validate_native_removals(
        &self,
        prepared: PreparedNativeRemovals,
    ) -> Result<ValidatedNativeChange> {
        self.validate_native_change(prepared.0)
    }
}
impl NativeBulkRemovalAdmission<'_> {
    pub(crate) fn prepare(
        self,
        funder: &crate::staged_source::SourceFunder,
        caller_control: impl FnOnce() -> Result<()>,
    ) -> Result<PreparedNativeRemovals> {
        let Some(root) = self.source.native_root() else {
            return Ok(PreparedNativeRemovals(self.source.unchanged_native()));
        };
        if self.collection.is_none() {
            return Ok(PreparedNativeRemovals(self.source.unchanged_native()));
        }
        let mut caller_control = Some(caller_control);
        let successor = root
            .prepare_bulk_originals(
                self.selected,
                None,
                self.source
                    .native_image()
                    .expect("active native root")
                    .is_shared(),
                funder,
                &mut caller_control,
            )?
            .expect("selected bulk table");
        Ok(PreparedNativeRemovals(PreparedNativeChange {
            expected: self
                .source
                .native_image()
                .map(crate::statement_rows::NativeImage::validation_handle),
            successor: NativeSuccessor::Replace(StagedVectorDirectory::prepare_native(
                successor, funder,
            )?),
            original_copy: self.collection,
        }))
    }
}
impl NativeOrderedRoot {
    fn next_selected_table<'a>(
        &'a self,
        selected: &std::collections::BTreeSet<(String, RowId)>,
        after: Option<&str>,
    ) -> Option<(&'a str, &'a NativeOrderedTable)> {
        let mut after = after;
        loop {
            let (name, table) = crate::staged_source::seek_by(&self.tables, false, |name| {
                if after.is_some_and(|after| name.as_str() <= after) {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Equal
                }
            })?;
            if selected.iter().any(|(selected, id)| {
                selected == name.as_str() && table.statement.get().row(*id).is_some()
            }) {
                return Some((name.as_str(), table));
            }
            after = Some(name.as_str());
        }
    }
    fn prepare_bulk_originals(
        &self,
        selected: &std::collections::BTreeSet<(String, RowId)>,
        after: Option<&str>,
        root_shared: bool,
        funder: &crate::staged_source::SourceFunder,
        caller_control: &mut Option<impl FnOnce() -> Result<()>>,
    ) -> Result<Option<crate::statement_rows::NativeImage<Self>>> {
        let Some((name, table)) = self.next_selected_table(selected, after) else {
            caller_control.take().expect("one bulk caller admission")()?;
            return Ok(None);
        };
        let grant = if root_shared || table.statement.is_shared() {
            Some(
                table
                    .statement
                    .get()
                    .first_row()
                    .expect("selected nonempty table")
                    .reserve_private_original_copy(table.statement.get().legacy_control_bytes())?,
            )
        } else {
            None
        };
        // Ascending table order for O. Each stack frame owns its grant; neither
        // scratch allocation nor a caller admission occurs until all O succeed.
        let remaining =
            self.prepare_bulk_originals(selected, Some(name), root_shared, funder, caller_control)?;
        let prior = remaining
            .as_ref()
            .map_or(self, crate::statement_rows::NativeImage::get);
        let successor = prior.prepare_bulk_table(name, selected, grant, funder)?;
        Ok(Some(successor))
    }
    fn prepare_bulk_table(
        &self,
        name: &str,
        selected: &std::collections::BTreeSet<(String, RowId)>,
        grant: Option<crate::staged_source::OriginalCopyGrant>,
        funder: &crate::staged_source::SourceFunder,
    ) -> Result<crate::statement_rows::NativeImage<Self>> {
        use crate::staged_source::{Entry, FundedArc, SourceName, get_by, insert, remove_by};
        let prior = self.table(name).expect("selected table retained");
        let mut statement = None;
        let mut rows = prior.rows.clone();
        let mut indexes = prior.indexes.clone();
        for (table, id) in selected {
            if table != name || prior.statement.get().row(*id).is_none() {
                continue;
            }
            let old = statement.as_ref().map_or(
                prior.statement.get(),
                crate::statement_rows::NativeImage::get,
            );
            statement = Some(old.prepare_remove(*id, funder)?);
            if let Some(row) = get_by(&rows, |row| id.cmp(row)) {
                indexes = Self::remove_postings(indexes, row, funder)?;
            }
            rows = remove_by(&rows, &|row| id.cmp(row), funder)?;
        }
        let statement = statement
            .expect("selected table has a row")
            .with_original_copy(grant);
        let table = FundedArc::try_new(
            NativeOrderedTable {
                statement,
                rows,
                ordered_present: indexes.is_some(),
                indexes,
            },
            funder.clone(),
        )?;
        let tables = insert(
            &self.tables,
            FundedArc::try_new(
                Entry {
                    key: SourceName::new(name, funder)?,
                    value: table,
                },
                funder.clone(),
            )?,
            funder,
        )?;
        crate::statement_rows::NativeImage::prepare(
            Self {
                tables,
                vectors: self.vectors.clone(),
                exact: self.exact.prepare_filter(
                    |slot| {
                        !selected.iter().any(|(table, id)| {
                            table == slot.table.as_str() && *id == slot.body.row().row_id
                        })
                    },
                    |_| true,
                    funder,
                )?,
            },
            funder,
        )
    }
}
impl StagedSchema {
    pub(crate) fn native_origin(&self) -> Option<&crate::staged_source::SchemaHandle> {
        match self.charge.statement_origin() {
            Some(crate::statement_rows::StatementOrigin(
                crate::statement_rows::StatementOriginOwner::Schema(schema),
            )) => Some(schema),
            _ => None,
        }
    }
}
impl NativeEagerVector {
    fn visit_retained_debits<'a>(
        &'a self,
        visit: &mut dyn FnMut(crate::staged_source::RetainedDebit<'a>),
    ) {
        self.0.visit_retained_debits(visit);
        self.0.column.visit_retained_debits(visit);
        match &self.0.payload {
            NativeEagerVectorPayload::Scalar(row) => row.visit_retained_debits(visit),
            NativeEagerVectorPayload::Owned(payload) => {
                payload.visit_retained_debits(visit);
                (**payload).visit_retained_debits(visit);
            }
        }
    }
}
impl NativeOrderedRoot {
    fn visit_retained_debits<'a>(
        &'a self,
        visit: &mut dyn FnMut(crate::staged_source::RetainedDebit<'a>),
    ) {
        use crate::staged_source::{seek_by, visit_link_debits};
        self.exact.visit_retained_debits(visit);
        visit_link_debits(&self.tables, visit);
        visit_link_debits(&self.vectors, visit);
        let mut after = None;
        while let Some((name, table)) = seek_by(&self.tables, false, |name| {
            if after.is_some_and(|after| name.as_str() <= after) {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Equal
            }
        }) {
            name.visit_retained_debits(visit);
            table.visit_retained_debits(visit);
            table.statement.visit_control_debits(visit);
            table.statement.get().visit_retained_debits(visit);
            visit_link_debits(&table.rows, visit);
            visit_link_debits(&table.indexes, visit);
            let mut after_row = None;
            while let Some((id, row)) = seek_by(&table.rows, false, |id| {
                if after_row.is_some_and(|after| *id <= after) {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Equal
                }
            }) {
                row.visit_retained_debits(visit);
                row.schema.visit_retained_debits(visit);
                row.keys.visit_retained_debits(visit);
                for (name, key) in row.keys.as_slice() {
                    name.visit_retained_debits(visit);
                    key.0.visit_retained_debits(visit);
                    key.0._charge.visit_retained_debits(visit);
                }
                after_row = Some(*id);
            }
            let mut after_index = None;
            while let Some((name, index)) = seek_by(&table.indexes, false, |name| {
                if after_index.is_some_and(|after| name.as_str() <= after) {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Equal
                }
            }) {
                name.visit_retained_debits(visit);
                index.visit_retained_debits(visit);
                index.schema.visit_retained_debits(visit);
                visit_link_debits(&index.ordered, visit);
                let mut after_key = None;
                while let Some((key, _)) = seek_by(&index.ordered, false, |key| {
                    if after_key.is_some_and(|after| key <= after) {
                        std::cmp::Ordering::Less
                    } else {
                        std::cmp::Ordering::Equal
                    }
                }) {
                    key.0.visit_retained_debits(visit);
                    key.0._charge.visit_retained_debits(visit);
                    after_key = Some(key);
                }
                after_index = Some(name.as_str());
            }
            after = Some(name.as_str());
        }
        let mut after = None;
        while let Some((name, index)) = seek_by(&self.vectors, false, |name| {
            if after.is_some_and(|after| name <= after) {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Equal
            }
        }) {
            name.table.visit_retained_debits(visit);
            name.column.visit_retained_debits(visit);
            index.visit_retained_debits(visit);
            index.control.visit_retained_debits(visit);
            index.control.name.table.visit_retained_debits(visit);
            index.control.name.column.visit_retained_debits(visit);
            visit_link_debits(&index.rows, visit);
            let mut after_row = None;
            while let Some((id, vector)) = seek_by(&index.rows, false, |id| {
                if after_row.is_some_and(|after| *id <= after) {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Equal
                }
            }) {
                vector.visit_retained_debits(visit);
                after_row = Some(*id);
            }
            after = Some(name);
        }
    }
}
impl OrderedStagedRows {
    // activated by bounded-storage S3/S4; remove allow at activation
    #[allow(dead_code)]
    pub(crate) fn eager_statement(
        &self,
        table: &str,
    ) -> Option<crate::statement_rows::NativeStatementBorrow<'_>> {
        Some(crate::statement_rows::NativeStatementBorrow::new(
            self.native_root()?.table(table)?.statement.get(),
        ))
    }
    // activated by bounded-storage S3/S4; remove allow at activation
    #[allow(dead_code)]
    pub(crate) fn eager_vector(
        &self,
        index: &contextdb_core::VectorIndexRef,
        row: RowId,
    ) -> Option<&NativeEagerVector> {
        crate::staged_source::get_by(&self.native_root()?.vector_index(index)?.rows, |id| {
            row.cmp(id)
        })
    }
    pub(crate) fn native_retained_bytes(&self) -> Option<usize> {
        let body = self.vectors.native_body()?;
        crate::staged_source::retained_union_bytes(|visit| {
            body.credit.visit_retained_debits(visit);
            match &body.image {
                NativeDirectoryImage::Active(image) => {
                    image.visit_control_debits(visit);
                    image.get().visit_retained_debits(visit);
                }
                NativeDirectoryImage::Restore(pin) => {
                    pin.visit_control_debits(visit);
                    pin.get().visit_retained_debits(visit);
                }
            }
        })
    }
}

/// Engine semantic capture retains only private native custody.
// activated by bounded-storage S3/S4; remove allow at activation
#[allow(dead_code)]
pub(crate) struct NativeOrderedCapture {
    image: crate::statement_rows::NativeImage<NativeOrderedRoot>,
}
// activated by bounded-storage S3/S4; remove allow at activation
#[allow(dead_code)]
impl NativeOrderedCapture {
    pub(crate) fn eager_statement(
        &self,
        table: &str,
    ) -> Option<crate::statement_rows::NativeStatementBorrow<'_>> {
        Some(crate::statement_rows::NativeStatementBorrow::new(
            self.image.get().table(table)?.statement.get(),
        ))
    }
    pub(crate) fn eager_vector(
        &self,
        index: &contextdb_core::VectorIndexRef,
        row: RowId,
    ) -> Option<&NativeEagerVector> {
        crate::staged_source::get_by(&self.image.get().vector_index(index)?.rows, |id| {
            row.cmp(id)
        })
    }
    pub(crate) fn visit_retained_debits<'a>(
        &'a self,
        visit: &mut dyn FnMut(crate::staged_source::RetainedDebit<'a>),
    ) {
        self.image.visit_control_debits(visit);
        self.image.get().visit_retained_debits(visit);
    }
}
impl NativeOrderedCheckpoint {
    // activated by bounded-storage S3/S4; remove allow at activation
    #[allow(dead_code)]
    pub(crate) fn visit_retained_debits<'a>(
        &'a self,
        visit: &mut dyn FnMut(crate::staged_source::RetainedDebit<'a>),
    ) {
        if let Some(body) = self.0.native_body() {
            body.credit.visit_retained_debits(visit);
            match &body.image {
                NativeDirectoryImage::Active(image) => {
                    image.visit_control_debits(visit);
                    image.get().visit_retained_debits(visit);
                }
                NativeDirectoryImage::Restore(pin) => {
                    pin.visit_control_debits(visit);
                    pin.get().visit_retained_debits(visit);
                }
            }
        }
    }
}
impl OrderedStagedRows {
    // activated by bounded-storage S3/S4; remove allow at activation
    #[allow(dead_code)]
    pub(crate) fn capture_native(&self) -> Option<NativeOrderedCapture> {
        self.native_image().map(|image| NativeOrderedCapture {
            image: image.clone(),
        })
    }
    // activated by bounded-storage S3/S4; remove allow at activation
    #[allow(dead_code)]
    pub(crate) fn visit_native_debits<'a>(
        &'a self,
        visit: &mut dyn FnMut(crate::staged_source::RetainedDebit<'a>),
    ) {
        if let Some(body) = self.vectors.native_body() {
            body.credit.visit_retained_debits(visit);
            match &body.image {
                NativeDirectoryImage::Active(image) => {
                    image.visit_control_debits(visit);
                    image.get().visit_retained_debits(visit);
                }
                NativeDirectoryImage::Restore(pin) => {
                    pin.visit_control_debits(visit);
                    pin.get().visit_retained_debits(visit);
                }
            }
        }
    }
    // activated by bounded-storage S3/S4; remove allow at activation
    #[allow(dead_code)]
    pub(crate) fn native_retained_union(
        &self,
        captures: &[NativeOrderedCapture],
        restores: &[NativeOrderedCheckpoint],
    ) -> Option<usize> {
        // The manager supplies its actual retained captures/journal pins. This
        // method makes no claim about owners omitted from that inventory.
        self.native_image()?;
        crate::staged_source::retained_union_bytes(|visit| {
            self.visit_native_debits(visit);
            for capture in captures {
                capture.visit_retained_debits(visit);
            }
            for restore in restores {
                restore.visit_retained_debits(visit);
            }
        })
    }
}

impl OrderedStagedRows {
    pub(crate) fn prepare_native_owned_vector(
        &self,
        entry: &contextdb_core::VectorEntry,
        funder: &crate::staged_source::SourceFunder,
    ) -> Result<PreparedNativeChange> {
        self.native_preparation_state()?;
        // The concrete producer checks funder/original owner+epoch before its
        // original admission. It supplies the actual entry/invoice/bridge;
        // no byte buffer is promoted into a fabricated native original.
        let original = funder.copy_eager_vector(entry)?;
        let vector = NativeEagerVector::prepare_owned(original, funder)?;
        self.prepare_native_vector(&entry.index, vector, funder)
    }
}

// Keep the unsafe directory auto-trait argument checked against both exact
// pointee types if their members change in a later edit.
const _: () = {
    fn require_send_sync<T: Send + Sync>() {}
    let _ = require_send_sync::<Option<StagedVectorRoots>> as fn();
    let _ = require_send_sync::<NativeDirectoryBody> as fn();
};
impl OrderedStagedRows {
    /// None is no native observation, never a fabricated zero-holder image.
    pub(crate) fn native_semantic_holders(&self) -> Option<usize> {
        self.native_image()
            .map(crate::statement_rows::NativeImage::semantic_holders)
    }
    // activated by bounded-storage S3/S4; remove allow at activation
    #[allow(dead_code)]
    pub(crate) fn native_statement_semantic_holders(&self, name: &str) -> Option<usize> {
        Some(
            self.native_root()?
                .table(name)?
                .statement
                .semantic_holders(),
        )
    }
}

impl NativeOrderedRoot {
    pub(crate) fn prepare_exact_row(
        &self,
        table: &str,
        position: StagedPosition,
        eager: crate::statement_rows::NativeEagerRow,
        funder: &crate::staged_source::SourceFunder,
    ) -> Result<crate::statement_rows::NativeImage<Self>> {
        use crate::staged_source::{Entry, FundedArc, SourceBuffer, SourceName, insert};
        let prior = self.table(table);
        let empty = crate::statement_rows::NativeStatementRoot::empty();
        let statement = prior
            .map_or(&empty, |table| table.statement.get())
            .prepare_exact_row(eager.clone(), funder)?;
        let header = FundedArc::<NativeOrderedRow>::prepare(funder.clone())?;
        let key_count = eager.keys().count();
        let mut keys = SourceBuffer::with_capacity(key_count, funder)?;
        if keys.capacity() != key_count {
            return Err(contextdb_core::Error::Other(
                "ordered key capacity mismatch".into(),
            ));
        }
        for (name, key) in eager.keys() {
            keys.push((
                SourceName::new(name, funder)?,
                NativeOrderedKey::prepare(key, position.row_id, funder)?,
            ));
        }
        let row = FundedArc::from_prepared(
            NativeOrderedRow {
                position,
                schema: eager.schema().clone(),
                keys,
            },
            header,
        );
        let mut indexes = prior.and_then(|table| table.indexes.clone());
        if let Some(old) = prior.and_then(|table| {
            crate::staged_source::get_by(&table.rows, |id| position.row_id.cmp(id))
        }) {
            indexes = Self::remove_postings(indexes, old, funder)?;
        }
        for (name, key) in row.keys.as_slice() {
            let old = crate::staged_source::get_by(&indexes, |key| name.cmp(key));
            let ordered = old
                .filter(|index| index.schema.matches_index(&row.schema, name.as_str()))
                .and_then(|index| index.ordered.clone());
            let ordered = insert(
                &ordered,
                FundedArc::try_new(
                    Entry {
                        key: key.clone(),
                        value: position,
                    },
                    funder.clone(),
                )?,
                funder,
            )?;
            let index = FundedArc::try_new(
                NativeOrderedIndex {
                    schema: row.schema.clone(),
                    ordered,
                },
                funder.clone(),
            )?;
            indexes = insert(
                &indexes,
                FundedArc::try_new(
                    Entry {
                        key: name.clone(),
                        value: index,
                    },
                    funder.clone(),
                )?,
                funder,
            )?;
        }
        let rows = insert(
            &prior.and_then(|table| table.rows.clone()),
            FundedArc::try_new(
                Entry {
                    key: position.row_id,
                    value: row,
                },
                funder.clone(),
            )?,
            funder,
        )?;
        let state = FundedArc::try_new(
            NativeOrderedTable {
                statement,
                rows,
                indexes,
                ordered_present: true,
            },
            funder.clone(),
        )?;
        let tables = insert(
            &self.tables,
            FundedArc::try_new(
                Entry {
                    key: SourceName::new(table, funder)?,
                    value: state,
                },
                funder.clone(),
            )?,
            funder,
        )?;
        crate::statement_rows::NativeImage::prepare(
            Self {
                tables,
                vectors: self.vectors.clone(),
                exact: self.exact.clone(),
            },
            funder,
        )
    }
}

#[derive(Debug, Default, Clone)]
struct NativeExactSlots {
    rows: crate::staged_source::Link<usize, NativeExactRow>,
    vectors: crate::staged_source::Link<usize, NativeExactVector>,
}
#[derive(Debug, Clone)]
struct NativeExactRow {
    selector: usize,
    table: crate::staged_source::SourceName,
    created_tx: TxId,
    body: crate::statement_rows::NativeEagerRow,
}
#[derive(Debug, Clone)]
struct NativeExactVector {
    selector: usize,
    index: crate::staged_source::VectorName,
    body: NativeEagerVector,
}
pub(crate) struct NativeExactReplacement {
    pub(crate) old: crate::staged_source::RowCellHandle,
    pub(crate) cell: crate::staged_source::RowCellHandle,
    pub(crate) body: crate::statement_rows::NativeEagerRow,
}
/// Retained selectors borrow the protected original host. The actual O grant
/// is reserved before entering the caller's preparation closure.
pub(crate) struct NativeExactAdmission<'a> {
    source: &'a OrderedStagedRows,
    rows: &'a [crate::staged_source::RowCellHandle],
    vectors: &'a [crate::staged_source::VectorCellHandle],
    row_change: NativeExactRowChange,
    table_count: usize,
    collection: Option<crate::staged_source::OriginalCopyGrant>,
}
/// Borrowed input intent is known before any caller allocation. It selects
/// statement bodies, not ordered-position controls or archive nodes.
#[derive(Clone, Copy)]
pub(crate) enum NativeExactRowChange {
    Unchanged,
    RemoveRange { start: usize, end: usize },
    Replace { ordinal: usize, row: RowId },
}
impl NativeExactRowChange {
    fn row_at(self, ordinal: usize, cell: &crate::staged_source::RowCellHandle) -> Option<RowId> {
        match self {
            Self::RemoveRange { start, end } if (start..end).contains(&ordinal) => None,
            Self::Replace {
                ordinal: selected,
                row,
            } if ordinal == selected => Some(row),
            _ => Some(cell.identity.row),
        }
    }
    fn replaces(self, ordinal: usize) -> bool {
        matches!(self, Self::Replace { ordinal: selected, .. } if ordinal == selected)
    }
}
struct NativeExactTableGrants<'a> {
    entries: Vec<(
        &'a crate::staged_source::SourceName,
        crate::staged_source::OriginalCopyGrant,
    )>,
    _collection: Option<crate::staged_source::OriginalCopyGrant>,
}
pub(crate) fn exact_native_error() -> contextdb_core::Error {
    contextdb_core::Error::Other("native exact source selector mismatch".into())
}
impl NativeOrderedRoot {
    fn exact_row(
        &self,
        cell: &crate::staged_source::RowCellHandle,
    ) -> Result<crate::statement_rows::NativeEagerRow> {
        if let Some(slot) =
            crate::staged_source::get_by(&self.exact.rows, |key| cell.allocation_key().cmp(key))
        {
            if slot.selector != cell.allocation_key()
                || slot.table != cell.identity.table
                || slot.body.sequence() != cell.identity.sequence
                || slot.body.row().row_id != cell.identity.row
            {
                return Err(exact_native_error());
            }
            return Ok(slot.body.clone());
        }
        // A matching canonical header cannot prove custody of this exact slot.
        // Only the retained native producer may supply its original body.
        Err(exact_native_error())
    }
    fn exact_vector(
        &self,
        cell: &crate::staged_source::VectorCellHandle,
    ) -> Result<NativeEagerVector> {
        let slot =
            crate::staged_source::get_by(&self.exact.vectors, |key| cell.allocation_key().cmp(key))
                .ok_or_else(exact_native_error)?;
        if slot.selector != cell.allocation_key()
            || slot.index != cell.identity.index
            || slot.body.row_id() != cell.identity.row
        {
            return Err(exact_native_error());
        }
        Ok(slot.body.clone())
    }
    fn prepare_exact_slots(
        &self,
        exact: NativeExactSlots,
        funder: &crate::staged_source::SourceFunder,
    ) -> Result<crate::statement_rows::NativeImage<Self>> {
        crate::statement_rows::NativeImage::prepare(
            Self {
                tables: self.tables.clone(),
                vectors: self.vectors.clone(),
                exact,
            },
            funder,
        )
    }
}
impl PreparedNativeChange {
    fn active_image(&self) -> &crate::statement_rows::NativeImage<NativeOrderedRoot> {
        let NativeSuccessor::Replace(directory) = &self.successor else {
            unreachable!("native slot producer has a replacement image");
        };
        let NativeDirectoryImage::Active(image) = &directory
            .native_body()
            .expect("prepared native directory")
            .image
        else {
            unreachable!("prepared native slot image is active");
        };
        image
    }
    fn install_prepared_image(
        &mut self,
        image: crate::statement_rows::NativeImage<NativeOrderedRoot>,
        funder: &crate::staged_source::SourceFunder,
    ) -> Result<()> {
        // Free the intermediate directory before final admission. All native
        // body owners remain retained in the complete successor image.
        self.successor = NativeSuccessor::Unchanged;
        self.successor =
            NativeSuccessor::Replace(StagedVectorDirectory::prepare_native(image, funder)?);
        Ok(())
    }
    pub(crate) fn prepare_retain_row(
        mut self,
        cell: crate::staged_source::RowCellHandle,
        funder: &crate::staged_source::SourceFunder,
    ) -> Result<Self> {
        use crate::staged_source::{Entry, FundedArc, insert};
        let root = self.active_image().get();
        let body = root
            .table(cell.identity.table.as_str())
            .and_then(|table| table.statement.get().row(cell.identity.row))
            .filter(|body| {
                body.sequence() == cell.identity.sequence && cell.identity.matches_row(body.row())
            })
            .ok_or_else(exact_native_error)?
            .clone();
        let rows = insert(
            &root.exact.rows,
            FundedArc::try_new(
                Entry {
                    key: cell.allocation_key(),
                    value: NativeExactRow {
                        selector: cell.allocation_key(),
                        table: cell.identity.table.clone(),
                        created_tx: cell.identity.created_tx,
                        body,
                    },
                },
                funder.clone(),
            )?,
            funder,
        )?;
        let image = root.prepare_exact_slots(
            NativeExactSlots {
                rows,
                vectors: root.exact.vectors.clone(),
            },
            funder,
        )?;
        self.install_prepared_image(image, funder)?;
        Ok(self)
    }
    pub(crate) fn prepare_retain_vector(
        mut self,
        cell: crate::staged_source::VectorCellHandle,
        funder: &crate::staged_source::SourceFunder,
    ) -> Result<Self> {
        let root = self.active_image().get();
        let exact = root.exact.prepare_filter(
            |_| true,
            |slot| slot.index != cell.identity.index || slot.body.row_id() != cell.identity.row,
            funder,
        )?;
        let image = root.prepare_exact_slots(exact, funder)?;
        self.install_prepared_image(image, funder)?;
        self.prepare_retain_vector_slot(cell, funder)
    }
    pub(crate) fn prepare_retain_vector_slot(
        mut self,
        cell: crate::staged_source::VectorCellHandle,
        funder: &crate::staged_source::SourceFunder,
    ) -> Result<Self> {
        use crate::staged_source::{Entry, FundedArc, insert};
        let root = self.active_image().get();
        let body = crate::staged_source::get_by(&root.vectors, |name| {
            (
                cell.identity.index.table.as_str(),
                cell.identity.index.column.as_str(),
            )
                .cmp(&(name.table.as_str(), name.column.as_str()))
        })
        .and_then(|index| {
            crate::staged_source::get_by(&index.rows, |key| cell.identity.row.cmp(key))
        })
        .ok_or_else(exact_native_error)?
        .clone();
        let vectors = insert(
            &root.exact.vectors,
            FundedArc::try_new(
                Entry {
                    key: cell.allocation_key(),
                    value: NativeExactVector {
                        selector: cell.allocation_key(),
                        index: cell.identity.index.clone(),
                        body,
                    },
                },
                funder.clone(),
            )?,
            funder,
        )?;
        let image = root.prepare_exact_slots(
            NativeExactSlots {
                rows: root.exact.rows.clone(),
                vectors,
            },
            funder,
        )?;
        self.install_prepared_image(image, funder)?;
        Ok(self)
    }
}
impl OrderedStagedRows {
    pub(crate) fn prepare_native_exact_admission<'a>(
        &'a self,
        rows: &'a [crate::staged_source::RowCellHandle],
        vectors: &'a [crate::staged_source::VectorCellHandle],
        row_change: NativeExactRowChange,
    ) -> Result<NativeExactAdmission<'a>> {
        self.native_preparation_state()?;
        match row_change {
            NativeExactRowChange::RemoveRange { start, end } if start > end || end > rows.len() => {
                return Err(exact_native_error());
            }
            NativeExactRowChange::Replace { ordinal, .. } if ordinal >= rows.len() => {
                return Err(exact_native_error());
            }
            _ => {}
        }
        let root = self.native_root();
        // Validate every owner, not just the eventual canonical winner.
        if let Some(root) = root {
            for cell in rows {
                root.exact_row(cell)?;
            }
            for cell in vectors {
                root.exact_vector(cell)?;
            }
        } else if !rows.is_empty() || !vectors.is_empty() {
            return Err(exact_native_error());
        }
        let mut admission = NativeExactAdmission {
            source: self,
            rows,
            vectors,
            row_change,
            table_count: 0,
            collection: None,
        };
        if let Some(root) = root {
            let mut after = None;
            while let Some((name, table)) =
                crate::staged_source::seek_by(&root.tables, false, |name| {
                    if after.is_some_and(|after| name.as_str() <= after) {
                        std::cmp::Ordering::Less
                    } else {
                        std::cmp::Ordering::Equal
                    }
                })
            {
                if admission.needs_original_table(root, name, table)? {
                    admission.table_count = admission
                        .table_count
                        .checked_add(1)
                        .ok_or_else(exact_native_error)?;
                }
                after = Some(name.as_str());
            }
            // The only O-paid allocation is the actual table-grant Vec. Exact
            // archives, winner trees and directory controls are all C-funded.
            if admission.table_count != 0 {
                let bytes = std::alloc::Layout::array::<(
                    &crate::staged_source::SourceName,
                    crate::staged_source::OriginalCopyGrant,
                )>(admission.table_count)
                .map_err(|_| exact_native_error())?
                .size();
                let cell = rows.first().ok_or_else(exact_native_error)?;
                admission.collection =
                    Some(root.exact_row(cell)?.reserve_private_original_copy(bytes)?);
            }
        }
        Ok(admission)
    }
}
impl NativeExactAdmission<'_> {
    /// Gather changed shared statements' O admissions before invoking any C
    /// preparation. The actual table-grant buffer has its own O grant.
    pub(crate) fn prepare<T>(
        mut self,
        funder: &crate::staged_source::SourceFunder,
        caller: impl FnOnce() -> Result<(T, Option<NativeExactReplacement>)>,
        selection: for<'b> fn(&'b T) -> (&'b [usize], &'b [usize]),
    ) -> Result<(T, PreparedNativeChange)> {
        use crate::staged_source::{Entry, FundedArc, insert};
        let empty = NativeOrderedRoot::empty();
        let root = self.source.native_root().unwrap_or(&empty);
        let originals = self.prepare_original_tables(root)?;
        let (value, replacement) = caller()?;
        let (rows, vectors) = selection(&value);
        let mut image = self.prepare_successor(root, replacement, rows, vectors, funder)?;
        for (name, grant) in originals.entries {
            let next = image
                .get()
                .table(name.as_str())
                .expect("retained table control");
            let prior = root.table(name.as_str()).ok_or_else(exact_native_error)?;
            if prior.statement.ptr_eq(&next.statement) {
                return Err(exact_native_error());
            }
            let statement = next.statement.clone().with_original_copy(Some(grant));
            let table = FundedArc::try_new(
                NativeOrderedTable {
                    statement,
                    rows: next.rows.clone(),
                    indexes: next.indexes.clone(),
                    ordered_present: next.ordered_present,
                },
                funder.clone(),
            )?;
            let tables = insert(
                &image.get().tables,
                FundedArc::try_new(
                    Entry {
                        key: name.clone(),
                        value: table,
                    },
                    funder.clone(),
                )?,
                funder,
            )?;
            image = crate::statement_rows::NativeImage::prepare(
                NativeOrderedRoot {
                    tables,
                    vectors: image.get().vectors.clone(),
                    exact: image.get().exact.clone(),
                },
                funder,
            )?;
        }
        Ok((
            value,
            PreparedNativeChange {
                expected: self
                    .source
                    .native_image()
                    .map(crate::statement_rows::NativeImage::validation_handle),
                successor: NativeSuccessor::Replace(StagedVectorDirectory::prepare_native(
                    image, funder,
                )?),
                original_copy: None,
            },
        ))
    }
    fn prepare_original_tables<'a>(
        &mut self,
        root: &'a NativeOrderedRoot,
    ) -> Result<NativeExactTableGrants<'a>> {
        let count = self.table_count;
        if count != 0 && self.collection.is_none() {
            return Err(exact_native_error());
        }
        let mut originals = NativeExactTableGrants {
            entries: Vec::with_capacity(count),
            _collection: self.collection.take(),
        };
        if originals.entries.capacity() != count {
            return Err(exact_native_error());
        }
        if count == 0 {
            return Ok(originals);
        }
        let mut after = None;
        while let Some((name, table)) = crate::staged_source::seek_by(&root.tables, false, |name| {
            if after.is_some_and(|after| name.as_str() <= after) {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Equal
            }
        }) {
            if self.needs_original_table(root, name, table)? {
                let body = table
                    .statement
                    .get()
                    .first_row()
                    .ok_or_else(exact_native_error)?;
                let grant = body
                    .reserve_private_original_copy(table.statement.get().legacy_control_bytes())?;
                originals.entries.push((name, grant));
            }
            after = Some(name.as_str());
        }
        if originals.entries.len() != count {
            return Err(exact_native_error());
        }
        Ok(originals)
    }
    fn needs_original_table(
        &self,
        root: &NativeOrderedRoot,
        name: &crate::staged_source::SourceName,
        table: &NativeOrderedTable,
    ) -> Result<bool> {
        let shared = self
            .source
            .native_image()
            .is_some_and(crate::statement_rows::NativeImage::is_shared)
            || table.statement.is_shared();
        if !shared || table.statement.get().first_row().is_none() {
            return Ok(false);
        }
        match self.row_change {
            NativeExactRowChange::Unchanged => Ok(false),
            NativeExactRowChange::RemoveRange { start, end } => {
                for cell in &self.rows[start..end] {
                    if cell.identity.table != *name {
                        continue;
                    }
                    let current = table
                        .statement
                        .get()
                        .row(cell.identity.row)
                        .ok_or_else(exact_native_error)?;
                    // Removing an earlier duplicate only shifts ordered
                    // positions; it cannot change this statement's body.
                    if !current.ptr_eq(&root.exact_row(cell)?) {
                        continue;
                    }
                    let winner = self.rows.iter().enumerate().rev().find(|(ordinal, have)| {
                        have.identity.table == *name
                            && self.row_change.row_at(*ordinal, have) == Some(cell.identity.row)
                    });
                    let Some((_, winner)) = winner else {
                        return Ok(true);
                    };
                    if !current.ptr_eq(&root.exact_row(winner)?) {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            NativeExactRowChange::Replace { ordinal, row } => {
                let cell = &self.rows[ordinal];
                if cell.identity.table != *name {
                    return Ok(false);
                }
                // Only the old and supplied identities can change. Borrowed
                // scans allocate nothing before the genuine O reservations.
                for id in [cell.identity.row, row] {
                    let winner = self.rows.iter().enumerate().rev().find(|(ordinal, have)| {
                        have.identity.table == *name
                            && self.row_change.row_at(*ordinal, have) == Some(id)
                    });
                    let current = table.statement.get().row(id);
                    match (current, winner) {
                        (None, None) => {}
                        (Some(_), None) | (None, Some(_)) => return Ok(true),
                        (Some(current), Some((ordinal, winner))) => {
                            if self.row_change.replaces(ordinal)
                                || !current.ptr_eq(&root.exact_row(winner)?)
                            {
                                return Ok(true);
                            }
                        }
                    }
                }
                Ok(false)
            }
        }
    }
    fn prepare_successor(
        &self,
        root: &NativeOrderedRoot,
        replacement: Option<NativeExactReplacement>,
        remove_rows: &[usize],
        remove_vectors: &[usize],
        funder: &crate::staged_source::SourceFunder,
    ) -> Result<crate::statement_rows::NativeImage<NativeOrderedRoot>> {
        use crate::staged_source::{Entry, FundedArc, get_by, insert, seek_by};
        for (selected, len) in [
            (remove_rows, self.rows.len()),
            (remove_vectors, self.vectors.len()),
        ] {
            if selected.iter().any(|slot| *slot >= len)
                || selected.windows(2).any(|pair| pair[0] >= pair[1])
            {
                return Err(exact_native_error());
            }
        }
        let selection_matches = match self.row_change {
            NativeExactRowChange::Unchanged => remove_rows.is_empty() && replacement.is_none(),
            NativeExactRowChange::RemoveRange { start, end } => {
                replacement.is_none() && remove_rows.iter().copied().eq(start..end)
            }
            NativeExactRowChange::Replace { ordinal, row } => {
                remove_rows.is_empty()
                    && replacement.as_ref().is_some_and(|p| {
                        p.old.ptr_eq(&self.rows[ordinal]) && p.cell.identity.row == row
                    })
            }
        };
        if !selection_matches {
            return Err(exact_native_error());
        }
        if let Some(p) = &replacement {
            let ordinal = self
                .rows
                .iter()
                .position(|cell| cell.ptr_eq(&p.old))
                .ok_or_else(exact_native_error)?;
            p.body.validate_exact_funder(funder)?;
            if !p.cell.is_funded_by(funder)
                || p.old.identity.tx != p.cell.identity.tx
                || p.old.identity.generation != p.cell.identity.generation
                || !crate::manager::same_published_source(
                    &p.old.identity.base,
                    &p.cell.identity.base,
                )
                || remove_rows.binary_search(&ordinal).is_ok()
                || p.old.identity.table != p.cell.identity.table
                || p.old.identity.sequence != p.cell.identity.sequence
                || p.cell.identity.sequence != p.body.sequence()
                || !p.cell.identity.matches_row(p.body.row())
                || !p.cell.identity.schema.ptr_eq(p.body.schema())
            {
                return Err(exact_native_error());
            }
        }
        let mut exact = NativeExactSlots::default();
        let mut row_winners = None;
        let mut vector_winners = None;
        let mut row_position = 0usize;
        for (ordinal, old) in self.rows.iter().enumerate() {
            if remove_rows.binary_search(&ordinal).is_ok() {
                continue;
            }
            let (cell, body) = match replacement.as_ref().filter(|p| p.old.ptr_eq(old)) {
                Some(p) => (p.cell.clone(), p.body.clone()),
                None => (old.clone(), root.exact_row(old)?),
            };
            let key = (cell.identity.table.clone(), cell.identity.row);
            let value = NativeExactRow {
                selector: cell.allocation_key(),
                table: cell.identity.table.clone(),
                created_tx: cell.identity.created_tx,
                body,
            };
            exact.rows = insert(
                &exact.rows,
                FundedArc::try_new(
                    Entry {
                        key: value.selector,
                        value: value.clone(),
                    },
                    funder.clone(),
                )?,
                funder,
            )?;
            row_winners = insert(
                &row_winners,
                FundedArc::try_new(
                    Entry {
                        key,
                        value: (row_position, value),
                    },
                    funder.clone(),
                )?,
                funder,
            )?;
            row_position += 1;
        }
        for (ordinal, cell) in self.vectors.iter().enumerate() {
            if remove_vectors.binary_search(&ordinal).is_ok() {
                continue;
            }
            let body = root.exact_vector(cell)?;
            let value = NativeExactVector {
                selector: cell.allocation_key(),
                index: cell.identity.index.clone(),
                body,
            };
            exact.vectors = insert(
                &exact.vectors,
                FundedArc::try_new(
                    Entry {
                        key: cell.allocation_key(),
                        value: value.clone(),
                    },
                    funder.clone(),
                )?,
                funder,
            )?;
            let name = NativeVectorName {
                table: cell.identity.index.table.clone(),
                column: cell.identity.index.column.clone(),
            };
            vector_winners = insert(
                &vector_winners,
                FundedArc::try_new(
                    Entry {
                        key: (name, cell.identity.row),
                        value,
                    },
                    funder.clone(),
                )?,
                funder,
            )?;
        }
        let mut image = root.prepare_exact_slots(exact, funder)?;
        let mut after = None;
        while let Some((name, table)) = seek_by(&root.tables, false, |name| {
            if after.is_some_and(|after| name.as_str() <= after) {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Equal
            }
        }) {
            let mut after_row = None;
            while let Some((id, old)) = seek_by(&table.rows, false, |id| {
                if after_row.is_some_and(|after| *id <= after) {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Equal
                }
            }) {
                let winner = get_by(
                    &row_winners,
                    |key: &(crate::staged_source::SourceName, RowId)| {
                        (name, id).cmp(&(&key.0, &key.1))
                    },
                );
                let next_image = match winner {
                    Some((position, slot)) => {
                        let old_body = table.statement.get().row(*id).expect("native current row");
                        if old_body.ptr_eq(&slot.body) {
                            if old.position.position == *position
                                && old.position.created_tx == slot.created_tx
                            {
                                after_row = Some(*id);
                                continue;
                            }
                            image.get().prepare_exact_position(
                                name.as_str(),
                                StagedPosition {
                                    row_id: *id,
                                    created_tx: slot.created_tx,
                                    position: *position,
                                },
                                funder,
                            )?
                        } else {
                            image.get().prepare_exact_row(
                                name.as_str(),
                                StagedPosition {
                                    row_id: *id,
                                    created_tx: slot.created_tx,
                                    position: *position,
                                },
                                slot.body.clone(),
                                funder,
                            )?
                        }
                    }
                    None => image
                        .get()
                        .prepare_remove(name.as_str(), *id, funder, None)?,
                };
                image = next_image;
                after_row = Some(*id);
            }
            after = Some(name.as_str());
        }
        // A replacement may introduce a canonical identity absent from the old
        // directory. Preserve the selected original sequence when installing it.
        let mut after_winner = None;
        while let Some((key, (position, slot))) = seek_by(&row_winners, false, |key| {
            if after_winner.is_some_and(|after| key <= after) {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Equal
            }
        }) {
            if root
                .table(key.0.as_str())
                .and_then(|table| crate::staged_source::get_by(&table.rows, |id| key.1.cmp(id)))
                .is_none()
            {
                image = image.get().prepare_exact_row(
                    key.0.as_str(),
                    StagedPosition {
                        row_id: key.1,
                        created_tx: slot.created_tx,
                        position: *position,
                    },
                    slot.body.clone(),
                    funder,
                )?;
            }
            after_winner = Some(key);
        }
        let mut after = None;
        while let Some((name, index)) = seek_by(&root.vectors, false, |name| {
            if after.is_some_and(|after| name <= after) {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Equal
            }
        }) {
            let mut rows = index.rows.clone();
            let mut after_row = None;
            while let Some((id, old)) = seek_by(&index.rows, false, |id| {
                if after_row.is_some_and(|after| *id <= after) {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Equal
                }
            }) {
                let winner = get_by(&vector_winners, |key: &(NativeVectorName, RowId)| {
                    (name, id).cmp(&(&key.0, &key.1))
                });
                rows = match winner {
                    Some(slot) if old.0.ptr_eq(&slot.body.0) => {
                        after_row = Some(*id);
                        continue;
                    }
                    Some(slot) => insert(
                        &rows,
                        FundedArc::try_new(
                            Entry {
                                key: *id,
                                value: slot.body.clone(),
                            },
                            funder.clone(),
                        )?,
                        funder,
                    )?,
                    None => crate::staged_source::remove_by(&rows, &|key| id.cmp(key), funder)?,
                };
                after_row = Some(*id);
            }
            let value = FundedArc::try_new(
                NativeVectorIndex {
                    rows,
                    control: index.control.clone(),
                },
                funder.clone(),
            )?;
            let vectors = insert(
                &image.get().vectors,
                FundedArc::try_new(
                    Entry {
                        key: name.clone(),
                        value,
                    },
                    funder.clone(),
                )?,
                funder,
            )?;
            image = crate::statement_rows::NativeImage::prepare(
                NativeOrderedRoot {
                    tables: image.get().tables.clone(),
                    vectors,
                    exact: image.get().exact.clone(),
                },
                funder,
            )?;
            after = Some(name);
        }
        Ok(image)
    }
}

impl NativeExactSlots {
    fn visit_retained_debits<'a>(
        &'a self,
        visit: &mut dyn FnMut(crate::staged_source::RetainedDebit<'a>),
    ) {
        use crate::staged_source::{seek_by, visit_link_debits};
        visit_link_debits(&self.rows, visit);
        visit_link_debits(&self.vectors, visit);
        let mut after = None;
        while let Some((key, slot)) = seek_by(&self.rows, false, |key| {
            if after.is_some_and(|after| *key <= after) {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Equal
            }
        }) {
            slot.table.visit_retained_debits(visit);
            slot.body.visit_retained_debits(visit);
            after = Some(*key);
        }
        let mut after = None;
        while let Some((key, slot)) = seek_by(&self.vectors, false, |key| {
            if after.is_some_and(|after| *key <= after) {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Equal
            }
        }) {
            slot.index.table.visit_retained_debits(visit);
            slot.index.column.visit_retained_debits(visit);
            slot.body.visit_retained_debits(visit);
            after = Some(*key);
        }
    }
}

impl NativeExactSlots {
    fn prepare_filter(
        &self,
        keep_row: impl Fn(&NativeExactRow) -> bool,
        keep_vector: impl Fn(&NativeExactVector) -> bool,
        funder: &crate::staged_source::SourceFunder,
    ) -> Result<Self> {
        use crate::staged_source::{remove_by, seek_by};
        let mut rows = self.rows.clone();
        let mut after = None;
        while let Some((key, slot)) = seek_by(&self.rows, false, |key| {
            if after.is_some_and(|after| *key <= after) {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Equal
            }
        }) {
            if !keep_row(slot) {
                rows = remove_by(&rows, &|have| key.cmp(have), funder)?;
            }
            after = Some(*key);
        }
        let mut vectors = self.vectors.clone();
        let mut after = None;
        while let Some((key, slot)) = seek_by(&self.vectors, false, |key| {
            if after.is_some_and(|after| *key <= after) {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Equal
            }
        }) {
            if !keep_vector(slot) {
                vectors = remove_by(&vectors, &|have| key.cmp(have), funder)?;
            }
            after = Some(*key);
        }
        Ok(Self { rows, vectors })
    }
}
impl PreparedNativeChange {
    pub(crate) fn prepare_prune_canonical_slots(
        mut self,
        funder: &crate::staged_source::SourceFunder,
    ) -> Result<Self> {
        if matches!(&self.successor, NativeSuccessor::Unchanged) {
            return Ok(self);
        }
        let root = self.active_image().get();
        let exact = root.exact.prepare_filter(
            |slot| {
                root.table(slot.table.as_str())
                    .and_then(|table| table.statement.get().row(slot.body.row().row_id))
                    .is_some_and(|current| current.ptr_eq(&slot.body))
            },
            |_| true,
            funder,
        )?;
        let image = root.prepare_exact_slots(exact, funder)?;
        self.install_prepared_image(image, funder)?;
        Ok(self)
    }
    pub(crate) fn prepare_rekey_row(
        mut self,
        old: &crate::staged_source::RowCellHandle,
        new: &crate::staged_source::RowCellHandle,
        funder: &crate::staged_source::SourceFunder,
    ) -> Result<Self> {
        use crate::staged_source::{Entry, FundedArc, insert, remove_by};
        let root = self.active_image().get();
        let body = root.exact_row(old)?;
        let rows = remove_by(
            &root.exact.rows,
            &|key| old.allocation_key().cmp(key),
            funder,
        )?;
        let rows = insert(
            &rows,
            FundedArc::try_new(
                Entry {
                    key: new.allocation_key(),
                    value: NativeExactRow {
                        selector: new.allocation_key(),
                        table: new.identity.table.clone(),
                        created_tx: new.identity.created_tx,
                        body,
                    },
                },
                funder.clone(),
            )?,
            funder,
        )?;
        let image = root.prepare_exact_slots(
            NativeExactSlots {
                rows,
                vectors: root.exact.vectors.clone(),
            },
            funder,
        )?;
        self.install_prepared_image(image, funder)?;
        Ok(self)
    }
    pub(crate) fn prepare_rekey_vector(
        mut self,
        old: &crate::staged_source::VectorCellHandle,
        new: &crate::staged_source::VectorCellHandle,
        funder: &crate::staged_source::SourceFunder,
    ) -> Result<Self> {
        use crate::staged_source::{Entry, FundedArc, insert, remove_by};
        let root = self.active_image().get();
        let body = root.exact_vector(old)?;
        let vectors = remove_by(
            &root.exact.vectors,
            &|key| old.allocation_key().cmp(key),
            funder,
        )?;
        let vectors = insert(
            &vectors,
            FundedArc::try_new(
                Entry {
                    key: new.allocation_key(),
                    value: NativeExactVector {
                        selector: new.allocation_key(),
                        index: new.identity.index.clone(),
                        body,
                    },
                },
                funder.clone(),
            )?,
            funder,
        )?;
        let image = root.prepare_exact_slots(
            NativeExactSlots {
                rows: root.exact.rows.clone(),
                vectors,
            },
            funder,
        )?;
        self.install_prepared_image(image, funder)?;
        Ok(self)
    }
}
impl NativeOrderedRoot {
    pub(crate) fn prepare_exact_position(
        &self,
        table: &str,
        position: StagedPosition,
        funder: &crate::staged_source::SourceFunder,
    ) -> Result<crate::statement_rows::NativeImage<Self>> {
        use crate::staged_source::{Entry, FundedArc, SourceBuffer, SourceName, insert};
        let prior = self.table(table).ok_or_else(exact_native_error)?;
        let old = crate::staged_source::get_by(&prior.rows, |id| position.row_id.cmp(id))
            .ok_or_else(exact_native_error)?;
        let header = FundedArc::<NativeOrderedRow>::prepare(funder.clone())?;
        let mut keys = SourceBuffer::with_capacity(old.keys.len(), funder)?;
        for (name, key) in old.keys.as_slice() {
            keys.push((name.clone(), key.clone()));
        }
        let statement = prior.statement.clone();
        let row = FundedArc::from_prepared(
            NativeOrderedRow {
                position,
                schema: old.schema.clone(),
                keys,
            },
            header,
        );
        let mut indexes = prior.indexes.clone();
        {
            indexes = Self::remove_postings(indexes, old, funder)?;
        }
        for (name, key) in row.keys.as_slice() {
            let old = crate::staged_source::get_by(&indexes, |key| name.cmp(key));
            let ordered = old
                .filter(|index| index.schema.matches_index(&row.schema, name.as_str()))
                .and_then(|index| index.ordered.clone());
            let ordered = insert(
                &ordered,
                FundedArc::try_new(
                    Entry {
                        key: key.clone(),
                        value: position,
                    },
                    funder.clone(),
                )?,
                funder,
            )?;
            let index = FundedArc::try_new(
                NativeOrderedIndex {
                    schema: row.schema.clone(),
                    ordered,
                },
                funder.clone(),
            )?;
            indexes = insert(
                &indexes,
                FundedArc::try_new(
                    Entry {
                        key: name.clone(),
                        value: index,
                    },
                    funder.clone(),
                )?,
                funder,
            )?;
        }
        let rows = insert(
            &prior.rows,
            FundedArc::try_new(
                Entry {
                    key: position.row_id,
                    value: row,
                },
                funder.clone(),
            )?,
            funder,
        )?;
        let state = FundedArc::try_new(
            NativeOrderedTable {
                statement,
                rows,
                indexes,
                ordered_present: true,
            },
            funder.clone(),
        )?;
        let tables = insert(
            &self.tables,
            FundedArc::try_new(
                Entry {
                    key: SourceName::new(table, funder)?,
                    value: state,
                },
                funder.clone(),
            )?,
            funder,
        )?;
        crate::statement_rows::NativeImage::prepare(
            Self {
                tables,
                vectors: self.vectors.clone(),
                exact: self.exact.clone(),
            },
            funder,
        )
    }
}

impl OrderedStagedRows {
    pub(crate) fn copy_exact_original_row_for(
        &self,
        cell: &crate::staged_source::RowCellHandle,
        funder: &crate::staged_source::SourceFunder,
    ) -> Result<crate::staged_source::AdmittedSourceRow> {
        self.native_preparation_state()?;
        let root = self.native_root().ok_or_else(exact_native_error)?;
        let slot =
            crate::staged_source::get_by(&root.exact.rows, |key| cell.allocation_key().cmp(key))
                .ok_or_else(exact_native_error)?;
        if !cell.is_funded_by(funder)
            || slot.selector != cell.allocation_key()
            || slot.table != cell.identity.table
            || slot.created_tx != cell.identity.created_tx
            || slot.body.sequence() != cell.identity.sequence
            || !cell.identity.matches_row(slot.body.row())
            || !slot.body.schema().ptr_eq(&cell.identity.schema)
        {
            return Err(exact_native_error());
        }
        slot.body.copy_original_row_for(funder)
    }
}

impl NativeEagerVector {
    fn copy_exact_original_vector_for(
        &self,
        entry: &contextdb_core::VectorEntry,
        funder: &crate::staged_source::SourceFunder,
    ) -> Result<crate::staged_source::AdmittedEagerSourceVector> {
        if !self.0.is_funded_by(funder)
            || self.row_id() != entry.row_id
            || self.0.column.as_str() != entry.index.column
            || self.vector() != entry.vector.as_slice()
        {
            return Err(exact_native_error());
        }
        let mut copied = match &self.0.payload {
            NativeEagerVectorPayload::Scalar(row) => row.copy_original_vector_input_for(
                crate::staged_source::BorrowedOriginalVectorCopyInput::new(
                    &entry.index.table,
                    &entry.index.column,
                    self.vector(),
                    row.row().row_id,
                    row.row().created_tx,
                    row.row().deleted_tx,
                    row.row().lsn,
                ),
                funder,
            )?,
            NativeEagerVectorPayload::Owned(payload) => {
                payload.validate_original_pairing(funder)?;
                if !payload.is_funded_by(funder) || payload.entry.index != entry.index {
                    return Err(exact_native_error());
                }
                payload
                    .original_copy
                    .copy_original_eager_vector_for(&payload.entry, funder)?
            }
        };
        // Scalar aliases and moved capabilities keep their receiver's original
        // body; the validated exact slot supplies their scalar vector headers.
        copied.entry.row_id = entry.row_id;
        copied.entry.created_tx = entry.created_tx;
        copied.entry.deleted_tx = entry.deleted_tx;
        copied.entry.lsn = entry.lsn;
        Ok(copied)
    }
}

impl OrderedStagedRows {
    pub(crate) fn copy_exact_original_vector_for(
        &self,
        cell: &crate::staged_source::VectorCellHandle,
        entry: &contextdb_core::VectorEntry,
        funder: &crate::staged_source::SourceFunder,
    ) -> Result<crate::staged_source::AdmittedEagerSourceVector> {
        self.native_preparation_state()?;
        let root = self.native_root().ok_or_else(exact_native_error)?;
        let slot =
            crate::staged_source::get_by(&root.exact.vectors, |key| cell.allocation_key().cmp(key))
                .ok_or_else(exact_native_error)?;
        if !cell.is_funded_by(funder)
            || slot.selector != cell.allocation_key()
            || slot.index != cell.identity.index
            || !cell.identity.matches_entry(entry)
        {
            return Err(exact_native_error());
        }
        let custody = cell.custody.lock();
        let Some(crate::staged_source::VectorCustody::Sealed {
            entry: original, ..
        }) = custody.as_ref()
        else {
            return Err(exact_native_error());
        };
        if !cell.identity.matches_entry(original)
            || original.index != entry.index
            || original.vector != entry.vector
        {
            return Err(exact_native_error());
        }
        slot.body.copy_exact_original_vector_for(entry, funder)
    }

    pub(crate) fn copy_exact_original_vector_input_for_row(
        &self,
        cell: &crate::staged_source::RowCellHandle,
        column: &str,
        values: &[f32],
        funder: &crate::staged_source::SourceFunder,
    ) -> Result<crate::staged_source::AdmittedEagerSourceVector> {
        self.native_preparation_state()?;
        let root = self.native_root().ok_or_else(exact_native_error)?;
        let slot =
            crate::staged_source::get_by(&root.exact.rows, |key| cell.allocation_key().cmp(key))
                .ok_or_else(exact_native_error)?;
        if !cell.is_funded_by(funder)
            || slot.selector != cell.allocation_key()
            || slot.table != cell.identity.table
            || slot.created_tx != cell.identity.created_tx
            || slot.body.sequence() != cell.identity.sequence
            || !cell.identity.matches_row(slot.body.row())
            || !slot.body.schema().ptr_eq(&cell.identity.schema)
        {
            return Err(exact_native_error());
        }
        slot.body.copy_original_vector_input_for(
            crate::staged_source::BorrowedOriginalVectorCopyInput::new(
                cell.identity.table.as_str(),
                column,
                values,
                cell.identity.row,
                cell.identity.created_tx,
                cell.identity.deleted_tx,
                cell.identity.lsn,
            ),
            funder,
        )
    }
}

impl OrderedStagedRows {
    pub(crate) fn prepare_native_admitted_original_vector(
        &self,
        entry: &contextdb_core::VectorEntry,
        original: crate::staged_source::AdmittedEagerSourceVector,
        funder: &crate::staged_source::SourceFunder,
    ) -> Result<PreparedNativeChange> {
        if original.entry.index != entry.index
            || original.entry.row_id != entry.row_id
            || original.entry.vector != entry.vector
            || original.entry.created_tx != entry.created_tx
            || original.entry.deleted_tx != entry.deleted_tx
            || original.entry.lsn != entry.lsn
        {
            return Err(exact_native_error());
        }
        let vector = NativeEagerVector::prepare_owned(original, funder)?;
        self.prepare_native_vector(&entry.index, vector, funder)
    }
}

impl OrderedStagedRows {
    pub(crate) fn copy_changed_exact_original_rows_for(
        &self,
        cell: &crate::staged_source::RowCellHandle,
        expected: &contextdb_core::VersionedRow,
        replacement: &contextdb_core::VersionedRow,
        funder: &crate::staged_source::SourceFunder,
    ) -> Result<(
        crate::staged_source::AdmittedSourceRow,
        crate::staged_source::AdmittedEagerSourceRow,
    )> {
        self.native_preparation_state()?;
        let root = self.native_root().ok_or_else(exact_native_error)?;
        let slot =
            crate::staged_source::get_by(&root.exact.rows, |key| cell.allocation_key().cmp(key))
                .ok_or_else(exact_native_error)?;
        if !cell.is_funded_by(funder)
            || slot.selector != cell.allocation_key()
            || slot.table != cell.identity.table
            || slot.created_tx != cell.identity.created_tx
            || slot.body.sequence() != cell.identity.sequence
            || !cell.identity.matches_row(expected)
            || !cell.identity.matches_row(slot.body.row())
            || !crate::staged_source::changed_original_rows_identical(slot.body.row(), expected)
            || !slot.body.schema().ptr_eq(&cell.identity.schema)
        {
            return Err(exact_native_error());
        }
        // Keep the two genuine original admissions independent. A refusal of
        // the eager copy destroys the canonical body before refunding its O.
        let canonical = slot
            .body
            .copy_changed_original_row_for(expected, replacement, funder)?;
        let eager = slot
            .body
            .copy_changed_original_eager_row_for(expected, replacement, funder)?;
        Ok((canonical, eager))
    }
}

impl OrderedStagedRows {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn prepare_native_admitted_original_row(
        &self,
        table: &str,
        position: StagedPosition,
        row: &contextdb_core::VersionedRow,
        original: crate::staged_source::AdmittedEagerSourceRow,
        keys: &[(String, IndexKey)],
        schema: crate::staged_source::SchemaHandle,
        funder: &crate::staged_source::SourceFunder,
    ) -> Result<PreparedNativeChange> {
        self.native_preparation_state()?;
        if position.row_id != row.row_id || position.created_tx != row.created_tx {
            return Err(contextdb_core::Error::Other(
                "captured staged source identity mismatch".into(),
            ));
        }
        let empty = NativeOrderedRoot::empty();
        let root = self.native_root().unwrap_or(&empty);
        original.validate_exact_funder(funder)?;
        if !crate::staged_source::changed_original_rows_identical(&original.row, row) {
            return Err(exact_native_error());
        }
        let copied = original;
        let eager = crate::statement_rows::NativeEagerRow::prepare(
            copied,
            root.next_sequence(table),
            keys,
            schema,
            funder,
        )?;
        let successor = root.prepare_row(table, position, eager, funder)?;
        Ok(PreparedNativeChange {
            expected: self
                .native_image()
                .map(crate::statement_rows::NativeImage::validation_handle),
            successor: NativeSuccessor::Replace(StagedVectorDirectory::prepare_native(
                successor, funder,
            )?),
            original_copy: None,
        })
    }
}
