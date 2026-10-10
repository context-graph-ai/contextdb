//! A scoped borrow of the manager's admitted published root. Nested calls to
//! the same database retain that root; another database gets its own scope.
use super::*;
use crate::persistence::WriterSnapshot;
/// A declaration answer releases its charge with the answer, including while
/// consumed by an owning iterator, rather than with the statement root.
pub(super) struct DdlHistory {
    pub(super) entries: Vec<(Lsn, DdlChange)>,
    pub(super) memory: Option<contextdb_core::read_memory::ReadCredit>,
}
impl From<Vec<(Lsn, DdlChange)>> for DdlHistory {
    fn from(entries: Vec<(Lsn, DdlChange)>) -> Self {
        Self {
            entries,
            memory: None,
        }
    }
}
impl std::ops::Deref for DdlHistory {
    type Target = Vec<(Lsn, DdlChange)>;
    fn deref(&self) -> &Self::Target {
        &self.entries
    }
}
impl std::ops::DerefMut for DdlHistory {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.entries
    }
}
pub(super) struct DdlHistoryIntoIter {
    entries: std::vec::IntoIter<(Lsn, DdlChange)>,
    _memory: Option<contextdb_core::read_memory::ReadCredit>,
}
impl Iterator for DdlHistoryIntoIter {
    type Item = (Lsn, DdlChange);
    fn next(&mut self) -> Option<Self::Item> {
        self.entries.next()
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.entries.size_hint()
    }
}
impl IntoIterator for DdlHistory {
    type Item = (Lsn, DdlChange);
    type IntoIter = DdlHistoryIntoIter;
    fn into_iter(self) -> Self::IntoIter {
        DdlHistoryIntoIter {
            entries: self.entries.into_iter(),
            _memory: self.memory,
        }
    }
}
impl<'a> IntoIterator for &'a DdlHistory {
    type Item = &'a (Lsn, DdlChange);
    type IntoIter = std::slice::Iter<'a, (Lsn, DdlChange)>;
    fn into_iter(self) -> Self::IntoIter {
        self.entries.iter()
    }
}

/// A replay entry keeps its decoded source allocation funded through use.
pub(super) struct ChangeReplayEntry {
    pub(super) value: ChangeLogEntry,
    _source: Option<crate::persistence::writer_source::WriterWindow<()>>,
}
// The durable cursor retains its counted use inline. Boxing this variant
// would add a new independently admitted allocation solely to reduce enum size.
#[allow(clippy::large_enum_variant)]
pub(super) enum ChangeReplay {
    Durable(crate::persistence::writer_source::WriterReadCursor),
    Resident(std::vec::IntoIter<ChangeLogEntry>),
}
impl ChangeReplay {
    pub(super) fn next_entry(&mut self) -> Result<Option<ChangeReplayEntry>> {
        match self {
            Self::Durable(cursor) => Ok(cursor.next_change()?.map(|window| {
                let (value, source) = window.into_parts();
                ChangeReplayEntry {
                    value,
                    _source: Some(source),
                }
            })),
            Self::Resident(entries) => Ok(entries.next().map(|value| ChangeReplayEntry {
                value,
                _source: None,
            })),
        }
    }
}

/// Copies read for one replay entry end after that entry's source rows drop.
/// The surrounding operation retains its captured root and earlier credits.
pub(super) struct WriterOutputScope {
    owners: Option<OutputOwners>,
    start: usize,
}
impl Drop for WriterOutputScope {
    fn drop(&mut self) {
        if let Some(owners) = &self.owners {
            owners.borrow_mut().truncate(self.start);
        }
    }
}

type OutputOwners = std::rc::Rc<std::cell::RefCell<Vec<Box<dyn std::any::Any>>>>;
type ScopedRoot = (usize, Option<Arc<WriterSnapshot>>, OutputOwners);
thread_local! { static ROOT: std::cell::RefCell<Option<ScopedRoot>> = const { std::cell::RefCell::new(None) }; }

pub(super) fn scoped_root_for_manager(
    manager: &Arc<TransactionManager<DynStore>>,
) -> Option<Arc<WriterSnapshot>> {
    ROOT.with(|slot| {
        slot.borrow()
            .as_ref()
            .filter(|(id, _, _)| *id == Arc::as_ptr(manager) as usize)
            .and_then(|(_, root, _)| root.clone())
    })
}

/// Publication preloads its newly committed generation, before the enclosing
/// maintenance operation's earlier root can see it. Restore that root afterward.
pub(super) fn suspend_root_for_manager(
    manager: &Arc<TransactionManager<DynStore>>,
) -> Option<WriterRootScope> {
    ROOT.with(|slot| {
        let mut slot = slot.borrow_mut();
        if slot
            .as_ref()
            .is_some_and(|(id, _, _)| *id == Arc::as_ptr(manager) as usize)
        {
            Some(WriterRootScope {
                prior: slot.take(),
                _same_thread: std::marker::PhantomData,
            })
        } else {
            None
        }
    })
}

/// Preserve the consumer's typed refusal across the storage callback's shared
/// core-error boundary; the private sentinel never reaches the caller.
pub(super) fn next_window_with_work<T: Send, E: From<Error>>(
    cursor: &mut dyn contextdb_relational::source::RelationalCursor<T>,
    before: &mut dyn FnMut() -> std::result::Result<(), E>,
) -> std::result::Result<
    Option<
        contextdb_core::read_memory::OwnedReadBox<
            dyn contextdb_relational::source::RelationalWindow<T>,
        >,
    >,
    E,
> {
    let mut refused = None;
    let result = cursor.next_window_before(&mut || {
        before().map_err(|error| {
            refused = Some(error);
            Error::ReadCancelled
        })
    });
    match refused {
        Some(error) => Err(error),
        None => result.map_err(E::from),
    }
}

pub(super) fn visible_row_with_work<E: From<Error>>(
    source: &dyn contextdb_relational::source::RelationalReadSource,
    table: &str,
    row_id: RowId,
    snapshot: SnapshotId,
    before: &mut dyn FnMut() -> std::result::Result<(), E>,
) -> std::result::Result<
    Option<
        contextdb_core::read_memory::OwnedReadBox<
            dyn contextdb_relational::source::RelationalWindow<VersionedRow>,
        >,
    >,
    E,
> {
    if snapshot.0 > source.snapshot().0 {
        return Err(E::from(Error::Other(
            "relational snapshot exceeds the captured source frontier".into(),
        )));
    }
    let mut cursor = source
        .row_id_cursor(table, row_id, snapshot)
        .map_err(E::from)?;
    while let Some(window) = next_window_with_work(&mut cursor, before)? {
        if window.value().created_tx.0 <= snapshot.0 {
            return Ok(window.value().visible_at(snapshot).then_some(window));
        }
    }
    Ok(None)
}
pub(crate) struct WriterReadScope {
    prior: Option<ScopedRoot>,
    _decode: Option<crate::read_image_memory::ImageMemoryScope>,
    _read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    _same_thread: std::marker::PhantomData<std::rc::Rc<()>>,
}
pub(crate) struct WriterRootScope {
    prior: Option<ScopedRoot>,
    _same_thread: std::marker::PhantomData<std::rc::Rc<()>>,
}
impl Drop for WriterRootScope {
    fn drop(&mut self) {
        let prior = self.prior.take();
        ROOT.with(|slot| {
            *slot.borrow_mut() = prior;
        });
    }
}
impl Drop for WriterReadScope {
    fn drop(&mut self) {
        let prior = self.prior.take();
        ROOT.with(|slot| {
            *slot.borrow_mut() = prior;
        });
    }
}
impl Database {
    pub(crate) fn preflight_historical_graph<E: From<Error>>(
        &self, snapshot: SnapshotId,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        before: &mut dyn FnMut() -> std::result::Result<(), E>,
    ) -> std::result::Result<(), E> {
        let Some(root) = self.graph_read_root().map_err(E::from)? else { return Ok(()); };
        if snapshot == root.frontier() { return Ok(()); }
        before()?;
        let capture = root.snapshot(snapshot, read_memory).map_err(E::from)?;
        before()?;
        drop(capture);
        Ok(())
    }

    /// Reconcile an indexed plan input against this operation's captured root.
    /// No successful plan output may depend on whether this input was pulled.
    pub(crate) fn preflight_historical_index<E: From<Error>>(
        &self, table: &str, index: &str, snapshot: SnapshotId,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        before: &mut dyn FnMut() -> std::result::Result<(), E>,
    ) -> std::result::Result<(), E> {
        if let Some(source) = &self.retained_relational_source {
            if source.ordered_prefix_snapshot_is_current(snapshot) { return Ok(()); }
            let meta = self.table_meta(table).ok_or_else(|| E::from(Error::TableNotFound(table.into())))?;
            let declaration = meta.indexes.iter().find(|declaration| declaration.name == index)
                .ok_or_else(|| E::from(Error::IndexNotFound { table: table.into(), index: index.into() }))?;
            return source.historical_index_preflight(table, &meta, declaration, snapshot, read_memory, before);
        }
        let Some(root) = self.writer_read_root().map_err(E::from)? else { return Ok(()); };
        if snapshot == root.snapshot { return Ok(()); }
        let source = crate::persistence::writer_adapters::WriterRelationalSource::new_for_read_at_snapshot(
            root, read_memory, snapshot).map_err(E::from)?;
        let mut refusal = None;
        let result = source.historical_index_preflight(table, index, snapshot,
            &mut || before().map_err(|error| { refusal = Some(error); Error::ReadCancelled }));
        match refusal { Some(error) => Err(error), None => result.map_err(E::from) }
    }

    /// Retain a selected immutable staged row before touching its committed
    /// predecessor. Legacy plugin rows use a prepaid, revalidated copy.
    pub(super) fn writer_staged_row_matching(
        &self,
        tx: TxId,
        table: &str,
        snapshot: SnapshotId,
        replaces_deleted: bool,
        mut matches: impl FnMut(&VersionedRow) -> bool,

        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<Option<VersionedRow>> {
        let root = self.writer_read_root()?.ok_or_else(|| {
            Error::Other("staged writer lookup requires its captured root".into())
        })?;
        let source = crate::persistence::writer_adapters::WriterRelationalSource::new_for_read(
            root,
            read_memory.clone(),
        )?;
        let mut before = usize::MAX;
        loop {
            let selected = self.tx_mgr.with_write_set(tx, |ws| {
                let rows = ws.ordered_staged_rows.statement_rows(table);
                for (position, (name, row)) in ws.relational_inserts.iter().enumerate().rev() {
                    if name != table || position >= before || !matches(row) {
                        continue;
                    }
                    // Raw public WriteSet edits may leave superseded versions.
                    // Inspect that suffix only after a matching row exists;
                    // an absent/new key allocates no copied identity directory.
                    if ws.relational_inserts[position + 1..]
                        .iter()
                        .any(|(name, newer)| name == table && newer.row_id == row.row_id)
                    {
                        continue;
                    }
                    let deleted = ws
                        .relational_deletes
                        .iter()
                        .any(|(name, id, _)| name == table && *id == row.row_id);
                    let owner = rows
                        .as_ref()
                        .filter(|rows| rows.row(row.row_id).is_some_and(|owned| owned == row))
                        .cloned();
                    return Some((
                        position,
                        row.row_id,
                        row.created_tx,
                        row.lsn,
                        row.estimated_bytes(),
                        deleted,
                        owner,
                    ));
                }
                None
            })?;
            let Some((position, id, created, lsn, bytes, deleted, owner)) = selected else {
                return Ok(None);
            };
            if replaces_deleted
                && deleted
                && contextdb_relational::source::visible_row(&source, table, id, snapshot)?
                    .is_none()
            {
                before = position;
                continue;
            }
            let output = contextdb_core::read_memory::ReadCredit::try_new(
                read_memory.clone(),
                bytes,
                "writer_source",
                "staged_row_output",
                "Raise MEMORY_LIMIT to return this staged row.",
            )?;
            let row = if let Some(owner) = owner {
                Some(owner.row(id).expect("retained selected staged row").clone())
            } else {
                self.tx_mgr.with_write_set(tx, |ws| {
                    let (name, row) = ws.relational_inserts.get(position)?;
                    let now_deleted = ws
                        .relational_deletes
                        .iter()
                        .any(|(name, row_id, _)| name == table && *row_id == id);
                    (name == table
                        && row.row_id == id
                        && row.created_tx == created
                        && row.lsn == lsn
                        && row.estimated_bytes() <= bytes
                        && now_deleted == deleted
                        && matches(row))
                    .then(|| row.clone())
                })?
            };
            if let Some(row) = row {
                self.retain_writer_output(output);
                return Ok(Some(row));
            }
            // The live legacy write set changed while its copy was admitted.
            // Nothing has been exposed; reselect without rebinding the root.
            before = usize::MAX;
        }
    }

    /// Visit one decoded record at a time from the statement's captured root.
    /// `false` leaves the established borrowed-memory implementation to its caller.
    pub(super) fn visit_writer_rows<E: From<Error>>(
        &self,
        table: &str,
        mut visit: impl FnMut(&VersionedRow) -> std::result::Result<(), E>,

        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> std::result::Result<bool, E> {
        self.visit_writer_rows_phased(
            table,
            |row| match row {
                Some(row) => visit(row),
                None => Ok(()),
            },
            read_memory.clone(),
        )
    }

    /// The work phase precedes source decoding; the row phase borrows the
    /// admitted window. The caller supplies one error vocabulary for both.
    pub(super) fn visit_writer_rows_phased<E: From<Error>>(
        &self,
        table: &str,
        mut visit: impl FnMut(Option<&VersionedRow>) -> std::result::Result<(), E>,

        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> std::result::Result<bool, E> {
        use contextdb_relational::source::RelationalReadSource;
        use std::ops::Bound;
        let Some(root) = self.writer_read_root().map_err(E::from)? else {
            return Ok(false);
        };
        let source = crate::persistence::writer_adapters::WriterRelationalSource::new_for_read(
            root,
            read_memory.clone(),
        )
        .map_err(E::from)?;
        source.verify_historical_versions(table).map_err(E::from)?;
        let mut cursor = source
            .row_cursor(table, Bound::Unbounded, Bound::Unbounded, false)
            .map_err(E::from)?;
        while let Some(window) = next_window_with_work(&mut cursor, &mut || visit(None))? {
            visit(Some(window.value()))?;
        }
        Ok(true)
    }

    /// A declared exact/prefix probe and its payloads share one storage root.
    pub(super) fn writer_indexed_rows_for_values(
        &self,
        table: &str,
        columns: &[String],
        values: &[Value],
        snapshot: SnapshotId,

        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<Option<Vec<VersionedRow>>> {
        self.writer_indexed_rows_for_values_within(
            table,
            columns,
            values,
            snapshot,
            &mut |_| Ok(()),
            read_memory.clone(),
        )
    }

    pub(super) fn writer_indexed_rows_for_values_within(
        &self,
        table: &str,
        columns: &[String],
        values: &[Value],
        snapshot: SnapshotId,
        work: &mut dyn FnMut(crate::executor::GraphPredicateWork) -> Result<()>,

        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<Option<Vec<VersionedRow>>> {
        use contextdb_relational::source::{RelationalIndexRange, RelationalReadSource};
        use std::ops::Bound;
        work(crate::executor::GraphPredicateWork::Checkpoint)?;
        let Some(root) = self.writer_read_root()? else {
            return Ok(None);
        };
        let meta = root
            .borrow_captured_table_meta(table)
            .ok_or_else(|| Error::TableNotFound(table.to_owned()))?;
        let Some(index) = meta.indexes.iter().find(|index| {
            index.columns.len() >= columns.len()
                && (!index.is_exact_only() || index.columns.len() == columns.len())
                && index
                    .columns
                    .iter()
                    .zip(columns)
                    .all(|((have, _), want)| have == want)
        }) else {
            return Ok(None);
        };
        let source = crate::persistence::writer_adapters::WriterRelationalSource::new_for_read(
            root.clone(),
            read_memory.clone(),
        )?;
        let mut scratch = contextdb_core::read_memory::ReadCredit::try_new(
            read_memory.clone(),
            values
                .iter()
                .map(Value::estimated_bytes)
                .sum::<usize>()
                .saturating_mul(8)
                .saturating_add(256),
            "writer_source",
            "metadata_keys",
            "Raise MEMORY_LIMIT to inspect this metadata key.",
        )?;
        let key = index_key_from_values(&index.columns[..columns.len()], values);
        source.historical_index_preflight(table, &index.name, snapshot,
            &mut || work(crate::executor::GraphPredicateWork::Checkpoint))?;
        let current = snapshot == source.snapshot();
        let mut cursor = if index.columns.len() == columns.len() {
            source.exact_index_cursor(table, &index.name, current, &key)?
        } else {
            let lower = source.index_position_key(current, &key, RowId(0), TxId(0))?;
            source.index_cursor(
                table,
                &index.name,
                RelationalIndexRange {
                    current,
                    lower: Bound::Included(lower.as_bytes()),
                    upper: Bound::Unbounded,
                    reverse: false,
                },
            )?
        };
        let mut seen = HashSet::new();
        let mut rows = Vec::new();
        let mut output = contextdb_core::read_memory::ReadCredit::new(read_memory.clone());
        while let Some(posting) = next_window_with_work(&mut cursor, &mut || {
            work(crate::executor::GraphPredicateWork::Checkpoint)
        })? {
            let posting = posting.value();
            if !posting.key.starts_with(&key) {
                break;
            }
            if !posting.entry.visible_at(snapshot) || seen.contains(&posting.entry.row_id) {
                continue;
            }
            scratch.try_grow_for(
                96,
                "writer_source",
                "metadata_identities",
                "Raise MEMORY_LIMIT to inspect these metadata rows.",
            )?;
            seen.insert(posting.entry.row_id);
            if let Some(row) =
                visible_row_with_work(&source, table, posting.entry.row_id, snapshot, &mut || {
                    work(crate::executor::GraphPredicateWork::Checkpoint)
                })?
            {
                let bytes = row.value().estimated_bytes().saturating_add(128);
                output.try_grow_for(
                    bytes,
                    "writer_source",
                    "metadata_output",
                    "Raise MEMORY_LIMIT to return these metadata rows.",
                )?;
                rows.push(row.value().clone());
            }
        }
        self.retain_writer_output(output);
        Ok(Some(rows))
    }

    pub(super) fn writer_rank_join(
        &self,
        policy: &RankPolicy,
        join_value: &Value,
        snapshot: SnapshotId,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<Option<VersionedRow>> {
        use contextdb_relational::source::{
            RelationalIndexRange, RelationalReadSource, visible_row,
        };
        use std::ops::Bound;
        let root = self
            .writer_read_root()?
            .ok_or_else(|| Error::Other("rank join requires its captured root".into()))?;
        let meta = root
            .borrow_captured_table_meta(&policy.joined_table)
            .ok_or_else(|| Error::TableNotFound(policy.joined_table.clone()))?;
        let index = meta
            .indexes
            .iter()
            .find(|index| index.name == policy.protected_index)
            .ok_or_else(|| Error::IndexNotFound {
                table: policy.joined_table.clone(),
                index: policy.protected_index.clone(),
            })?;
        let Some((column, direction)) = index.columns.first() else {
            return Err(Error::Other(
                "rank policy protected index has no columns".into(),
            ));
        };
        if column != &policy.joined_column {
            return Err(Error::Other(
                "rank policy protected index no longer leads with its join column".into(),
            ));
        }
        let key = vec![match direction {
            SortDirection::Asc => DirectedValue::Asc(TotalOrdAsc(join_value.clone())),
            SortDirection::Desc => DirectedValue::Desc(TotalOrdDesc(join_value.clone())),
        }];
        let source = crate::persistence::writer_adapters::WriterRelationalSource::new_for_read(
            root.clone(),
            read_memory.clone(),
        )?;
        source.historical_index_preflight(&policy.joined_table, &index.name, snapshot, &mut || Ok(()))?;
        let current = snapshot == source.snapshot();
        let mut cursor = if index.columns.len() == 1 {
            source.exact_index_cursor(&policy.joined_table, &index.name, current, &key)?
        } else {
            let lower = source.index_position_key(current, &key, RowId(0), TxId(0))?;
            source.index_cursor(
                &policy.joined_table,
                &index.name,
                RelationalIndexRange {
                    current,
                    lower: Bound::Included(lower.as_bytes()),
                    upper: Bound::Unbounded,
                    reverse: false,
                },
            )?
        };
        let mut best = None;
        let mut output = contextdb_core::read_memory::ReadCredit::new(read_memory.clone());
        while let Some(posting) = cursor.next_window()? {
            if !posting.value().key.starts_with(&key) {
                break;
            }
            if !posting.value().entry.visible_at(snapshot) {
                continue;
            }
            let Some(row) = visible_row(
                &source,
                &policy.joined_table,
                posting.value().entry.row_id,
                snapshot,
            )?
            else {
                continue;
            };
            let row = row.value();
            if row
                .values
                .get(&policy.joined_column)
                .is_none_or(|value| !values_equal_for_rank_join(value, join_value))
                || !self.row_read_allowed_for_change(
                    &policy.joined_table,
                    row,
                    snapshot,
                    read_memory.clone(),
                )?
                || best
                    .as_ref()
                    .is_some_and(|best: &VersionedRow| best.row_id >= row.row_id)
            {
                continue;
            }
            let mut next = contextdb_core::read_memory::ReadCredit::try_new(
                read_memory.clone(),
                row.estimated_bytes(),
                "query",
                "rank_join_output",
                "Raise MEMORY_LIMIT to return this rank join row.",
            )?;
            best = Some(row.clone());
            std::mem::swap(&mut output, &mut next);
        }
        self.retain_writer_output(output);
        Ok(best)
    }

    /// Commit preparation needs the actual committed frontier, independently
    /// of an outer statement's retained transaction base.
    pub(super) fn committed_row_exists_at_current_frontier(
        &self,
        table: &str,
        row_id: RowId,

        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<bool> {
        let published = self.tx_mgr.capture_published_read();
        if let Some(root) = durable_root(published.handle)? {
            let source = crate::persistence::writer_adapters::WriterRelationalSource::new_for_read(
                root,
                read_memory.clone(),
            )?;
            return Ok(contextdb_relational::source::visible_row(
                &source,
                table,
                row_id,
                published.snapshot,
            )?
            .is_some());
        }
        Ok(self
            .relational_store
            .row_by_id(table, row_id, SnapshotId::from_raw_wire(u64::MAX))
            .is_some())
    }

    /// The sidecars must come from the same retained root as the row being
    /// selected; their resident maps are absent on a file-backed writer.
    pub(super) fn relational_sync_source_at_read_root(
        &self,
        table: &str,
        row_id: RowId,

        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<(
        Option<Lsn>,
        Option<contextdb_relational::store::SyncSourceKind>,
    )> {
        use contextdb_relational::source::RelationalReadSource;
        if let Some(root) = self.writer_read_root()? {
            let source = crate::persistence::writer_adapters::WriterRelationalSource::new_for_read(
                root,
                read_memory.clone(),
            )?;
            return Ok(source
                .sync_source(table, row_id)?
                .map_or((None, None), |(lsn, kind)| (Some(lsn), Some(kind))));
        }
        Ok((
            self.relational_store.sync_source_lsn(table, row_id),
            self.relational_store.sync_source_kind(table, row_id),
        ))
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn insert_relational_row_from_read(
        &self,
        tx: TxId,
        table: &str,
        row_id: RowId,
        values: HashMap<ColName, Value>,
        snapshot: SnapshotId,
        created_at: Option<Wallclock>,

        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<RowId> {
        if self.writer_read_root()?.is_none() {
            return match created_at {
                Some(created_at) => self
                    .relational
                    .insert_with_row_id_at(tx, table, row_id, values, snapshot, created_at),
                None => self
                    .relational
                    .insert_with_row_id(tx, table, row_id, values, snapshot),
            };
        }
        let meta = self
            .table_meta(table)
            .ok_or_else(|| Error::TableNotFound(table.into()))?;
        let existing = if meta
            .state_machine
            .as_ref()
            .is_some_and(|sm| matches!(values.get(&sm.column), Some(Value::Text(_))))
        {
            match values.get("id") {
                Some(id @ Value::Uuid(_)) => self.relational_point_with_tx(
                    Some(tx),
                    table,
                    "id",
                    id,
                    snapshot,
                    read_memory.clone(),
                )?,
                _ => None,
            }
        } else {
            None
        };
        self.relational.insert_with_row_id_from_descriptor(
            tx,
            table,
            row_id,
            values,
            &meta,
            existing.as_ref(),
            created_at,
        )
    }
    pub(super) fn scoped_writer_root(&self) -> Option<Arc<WriterSnapshot>> {
        ROOT.with(|slot| {
            slot.borrow()
                .as_ref()
                .filter(|(id, _, _)| *id == Arc::as_ptr(&self.tx_mgr) as usize)
                .and_then(|(_, root, _)| root.clone())
        })
    }
    /// Read maintained content totals from this operation's captured publication.
    pub(crate) fn writer_content_size_totals(
        &self,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        before_table: &mut dyn FnMut() -> Result<()>,
    ) -> Result<Option<crate::persistence::content_size::ContentSizeTotals>> {
        self.writer_read_root()?
            .map(|root| root.content_size_totals(read_memory, before_table))
            .transpose()
    }

    pub(crate) fn writer_read_root(&self) -> Result<Option<Arc<WriterSnapshot>>> {
        if self.persistence.is_none() {
            return Ok(None);
        }
        let owner = Arc::as_ptr(&self.tx_mgr) as usize;
        if let Some(root) = ROOT.with(|slot| {
            slot.borrow()
                .as_ref()
                .filter(|(id, _, _)| *id == owner)
                .and_then(|(_, root, _)| root.clone())
        }) {
            return Ok(Some(root));
        }
        let root = durable_root(self.tx_mgr.capture_published_read().handle)?;
        root.ok_or_else(|| Error::Other("writer publication has no durable root".into()))
            .map(Some)
    }
    pub(crate) fn enter_writer_read(
        &self,
        root: Option<Arc<WriterSnapshot>>,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> WriterReadScope {
        let owner = Arc::as_ptr(&self.tx_mgr) as usize;
        let owners = ROOT
            .with(|slot| {
                slot.borrow()
                    .as_ref()
                    .filter(|(id, _, _)| *id == owner)
                    .map(|(_, _, owners)| owners.clone())
            })
            .unwrap_or_default();
        let next = Some((owner, root, owners));
        let prior = ROOT.with(|slot| std::mem::replace(&mut *slot.borrow_mut(), next));
        WriterReadScope {
            prior,
            _decode: None,
            _read_memory: read_memory,
            _same_thread: std::marker::PhantomData,
        }
    }
    pub(super) fn writer_root_scope(&self) -> Result<WriterRootScope> {
        let owner = Arc::as_ptr(&self.tx_mgr) as usize;
        let owners = ROOT
            .with(|slot| {
                slot.borrow()
                    .as_ref()
                    .filter(|(id, _, _)| *id == owner)
                    .map(|(_, _, owners)| owners.clone())
            })
            .unwrap_or_default();
        let next = Some((owner, self.writer_read_root()?, owners));
        let prior = ROOT.with(|slot| std::mem::replace(&mut *slot.borrow_mut(), next));
        Ok(WriterRootScope {
            prior,
            _same_thread: std::marker::PhantomData,
        })
    }
    pub(super) fn writer_read_scope(
        &self,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<WriterReadScope> {
        let image = crate::read_image_memory::ImageMemory::operation_reader(
            self.accountant.clone(),
            read_memory.clone(),
        )?;
        let mut scope = self.enter_writer_read(self.writer_read_root()?, read_memory);
        scope._decode = Some(image.enter());
        Ok(scope)
    }

    /// Predicate rows are temporary even when their statement retains its root.
    pub(crate) fn writer_temporary_output_scope(
        &self,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<WriterReadScope> {
        let image = crate::read_image_memory::ImageMemory::operation_reader(
            self.accountant.clone(),
            read_memory.clone(),
        )?;
        let next = Some((
            Arc::as_ptr(&self.tx_mgr) as usize,
            self.writer_read_root()?,
            OutputOwners::default(),
        ));
        let prior = ROOT.with(|slot| std::mem::replace(&mut *slot.borrow_mut(), next));
        Ok(WriterReadScope {
            prior,
            _decode: Some(image.enter()),
            _read_memory: read_memory,
            _same_thread: std::marker::PhantomData,
        })
    }
    pub(crate) fn graph_predicate_memory(
        &self,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> contextdb_core::read_memory::ReadCredit {
        contextdb_core::read_memory::ReadCredit::new(read_memory.clone())
    }

    /// Visit only owners already held by this operation's captured root.
    pub(super) fn visit_writer_output_copy_credits(
        &self,
        visit: &mut dyn FnMut(usize, usize, bool),
    ) {
        ROOT.with(|slot| {
            let root = slot.borrow();
            if let Some((manager, _, owners)) = root.as_ref()
                && *manager == Arc::as_ptr(&self.tx_mgr) as usize
            {
                for owner in owners.borrow().iter() {
                    if let Some(image) =
                        owner.downcast_ref::<Arc<crate::read_image_memory::ImageMemory>>()
                    {
                        image.visit_retained_copy_credit(self.accountant.as_ref(), visit);
                    } else if let Some(credit) =
                        owner.downcast_ref::<contextdb_core::read_memory::ReadCredit>()
                        && credit.bytes() != 0 {
                            visit(credit as *const _ as usize, credit.bytes(), false);
                        }
                }
            }
        });
    }

    pub(crate) fn retain_writer_image(&self, image: Arc<crate::read_image_memory::ImageMemory>) {
        ROOT.with(|slot| {
            if let Some((owner, _, owners)) = slot.borrow().as_ref()
                && *owner == Arc::as_ptr(&self.tx_mgr) as usize
            {
                owners.borrow_mut().push(Box::new(image));
            }
        });
    }

    pub(super) fn read_config_value<T: serde::de::DeserializeOwned>(
        &self,
        key: &str,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<Option<T>> {
        let Some(root) = self.writer_read_root()? else {
            return Ok(None);
        };
        let image = crate::read_image_memory::ImageMemory::charging_reader(
            self.accountant.clone(),
            read_memory.clone(),
        );
        let mut cursor = root.config_cursor(
            crate::persistence::writer_source::SourceWindowOwner::Read(read_memory),
            key,
        )?;
        let value = cursor
            .next(|stored_key, bytes| {
                if stored_key != key.as_bytes() {
                    return Ok(None);
                }
                image
                    .scope(|| RedbPersistence::decode_config_value::<T>(bytes))
                    .map(Some)
            })?
            .and_then(|window| window.value);
        // Scalar configuration values have no owned allocation after decode.
        // Their decoder admission ends here, rather than accumulating per row
        // until the surrounding operation's captured root is released.
        if std::mem::needs_drop::<T>() {
            self.retain_writer_image(image);
        }
        Ok(value)
    }

    pub(super) fn read_config_values_with_prefix<T: serde::de::DeserializeOwned>(
        &self,
        prefix: &str,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<Vec<(String, T)>> {
        let Some(root) = self.writer_read_root()? else {
            return Ok(Vec::new());
        };
        let image = crate::read_image_memory::ImageMemory::charging_reader(
            self.accountant.clone(),
            read_memory.clone(),
        );
        let mut cursor = root.config_cursor(
            crate::persistence::writer_source::SourceWindowOwner::Read(read_memory.clone()),
            prefix,
        )?;
        let mut output = contextdb_core::read_memory::ReadCredit::new(read_memory);
        let mut values = Vec::new();
        while let Some(window) = cursor.next(|key, bytes| {
            output.try_grow_for(
                key.len().saturating_add(std::mem::size_of::<(String, T)>()),
                "sync",
                "config_output",
                "Raise MEMORY_LIMIT to return sync metadata.",
            )?;
            let key = std::str::from_utf8(key)
                .map_err(|_| Error::Other("invalid config key".into()))?
                .to_owned();
            image
                .scope(|| RedbPersistence::decode_config_value::<T>(bytes))
                .map(|value| (key, value))
        })? {
            values.push(window.value);
        }
        self.retain_writer_image(image);
        self.retain_writer_output(output);
        Ok(values)
    }

    pub(super) fn writer_output_scope(&self) -> WriterOutputScope {
        ROOT.with(|slot| {
            let owners = slot
                .borrow()
                .as_ref()
                .filter(|(owner, _, _)| *owner == Arc::as_ptr(&self.tx_mgr) as usize)
                .map(|(_, _, owners)| owners.clone());
            let start = owners.as_ref().map_or(0, |owners| owners.borrow().len());
            WriterOutputScope { owners, start }
        })
    }

    pub(crate) fn retain_writer_output(&self, output: contextdb_core::read_memory::ReadCredit) {
        ROOT.with(|slot| {
            if let Some((owner, _, owners)) = slot.borrow().as_ref()
                && *owner == Arc::as_ptr(&self.tx_mgr) as usize
            {
                owners.borrow_mut().push(Box::new(output));
            }
        });
    }
}

impl Database {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn writer_physical_next<E: From<Error>>(
        &self,
        source: &dyn contextdb_relational::source::RelationalReadSource,
        table: &str,
        cursor: &mut contextdb_relational::mem::BoundedPhysicalCursor,
        mut touch: impl FnMut() -> std::result::Result<(), E>,
        mut clone: impl FnMut(usize) -> std::result::Result<(), E>,
    ) -> std::result::Result<Option<VersionedRow>, E> {
        use std::ops::Bound;
        if cursor.is_exhausted() {
            return Ok(None);
        }
        let lower = cursor
            .source_position()
            .map_or(Bound::Unbounded, Bound::Excluded);
        let mut physical = source
            .row_cursor(table, lower, Bound::Unbounded, false)
            .map_err(E::from)?;
        let Some(window) = next_window_with_work(&mut physical, &mut touch)? else {
            cursor.source_exhaust();
            return Ok(None);
        };
        clone(
            window
                .value()
                .estimated_bytes()
                .saturating_add(window.position_key().len()),
        )?;
        let row = window.value().clone();
        cursor.source_advance(window.position_key().to_vec(), &row);
        Ok(Some(row))
    }

    pub(super) fn writer_ordered_next<E: From<Error>>(
        &self,
        source: &crate::persistence::writer_adapters::WriterRelationalSource,
        table: &str,
        snapshot: SnapshotId,
        index_components: usize,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        cursor: &mut contextdb_relational::mem::BoundedOrderedRowCursor,
        mut boundary: impl FnMut() -> std::result::Result<(), E>,
        mut touch: impl FnMut() -> std::result::Result<(), E>,
        mut observed: impl FnMut(&IndexKey, RowId) -> std::result::Result<(), E>,
        mut clone: impl FnMut(usize) -> std::result::Result<(), E>,
    ) -> std::result::Result<Option<VersionedRow>, E> {
        use contextdb_relational::source::{
            RelationalIndexRange, RelationalReadSource, visible_row,
        };
        use std::ops::Bound;
        let mut preflight_refusal = None;
        let preflight = source.historical_index_preflight(table,
            cursor.index_name().ok_or_else(|| E::from(Error::Other("ordered cursor has no index".into())))?,
            snapshot, &mut || touch().map_err(|error| {
                preflight_refusal = Some(error); Error::ReadCancelled
            }));
        if let Some(error) = preflight_refusal { return Err(error); }
        preflight.map_err(E::from)?;
        if cursor.is_exhausted()
            || cursor.source_prefix_stopped()
            || cursor.source_prefix_quota() == Some(0)
        {
            return Ok(None);
        }
        loop {
            let reverse = cursor.source_reverse();
            let current = snapshot == source.snapshot();
            let endpoint = if cursor.source_position().is_none() {
                match cursor.source_seek() {
                    Bound::Included(key) => Some((
                        true,
                        source
                            .index_position_key_before(
                                current,
                                key,
                                if reverse { RowId(u64::MAX) } else { RowId(0) },
                                if reverse { TxId(u64::MAX) } else { TxId(0) }, &mut touch)?,
                    )),
                    Bound::Excluded(key) => Some((
                        false,
                        source
                            .index_position_key_before(
                                current,
                                key,
                                if reverse { RowId(0) } else { RowId(u64::MAX) },
                                if reverse { TxId(0) } else { TxId(u64::MAX) }, &mut touch)?,
                    )),
                    Bound::Unbounded => None,
                }
            } else {
                None
            };
            let edge = match cursor.source_position() {
                Some(key) => Bound::Excluded(key),
                None => endpoint
                    .as_ref()
                    .map_or(Bound::Unbounded, |(included, key)| {
                        if *included {
                            Bound::Included(key.as_bytes())
                        } else {
                            Bound::Excluded(key.as_bytes())
                        }
                    }),
            };
            // A pinned run ends before the next prefix, even when its quota
            // exceeds the number of postings in that run. Bound storage before
            // it fetches (and charges) the first posting of the next run.
            let mut run_end_credit =
                contextdb_core::read_memory::ReadCredit::new(read_memory.clone());
            let (run_start, run_end, prefix_len) = cursor.source_run_bounds();
            // Match the cursor's membership domain for each endpoint. Far
            // edges may carry reverse-seek padding after the real prefix.
            let forward_run_edge = if reverse {
                None
            } else {
                match run_end {
                    Bound::Included(key) | Bound::Excluded(key) => {
                        let length = prefix_len.min(key.len());
                        (length > 0 && length < index_components).then(|| match run_end {
                            Bound::Included(_) => Bound::Included(&key[..length]),
                            Bound::Excluded(_) => Bound::Excluded(&key[..length]),
                            Bound::Unbounded => unreachable!(),
                        })
                    }
                    Bound::Unbounded => None,
                }
            };
            let pinned_prefix = if reverse {
                match run_start {
                    Bound::Included(key)
                        if prefix_len > 0
                            && prefix_len < index_components
                            && key.len() == prefix_len =>
                    {
                        Some(key.as_slice())
                    }
                    _ => None,
                }
            } else {
                match forward_run_edge {
                    Some(Bound::Included(key) | Bound::Excluded(key)) => Some(key),
                    _ => None,
                }
            };
            let physical_run_end = if !reverse {
                pinned_prefix
                    .map(|prefix| {
                        let length = if matches!(forward_run_edge, Some(Bound::Excluded(_))) {
                            prefix.len()
                        } else {
                            index_components.checked_add(1).ok_or_else(|| {
                                E::from(Error::Other("ordered run-end size overflow".into()))
                            })?
                        };
                        let bytes = prefix.iter().fold(
                            length
                                .saturating_mul(std::mem::size_of::<DirectedValue>())
                                .saturating_add(crate::sync_types::held::ALLOCATION_BYTES),
                            |bytes, part| match part {
                                DirectedValue::Asc(TotalOrdAsc(value))
                                | DirectedValue::Desc(TotalOrdDesc(value)) => {
                                    bytes.saturating_add(crate::sync_types::held::value(value))
                                }
                            },
                        );
                        run_end_credit
                            .try_grow_for(
                                bytes,
                                "reader",
                                "ordered_run_end",
                                "Raise the declared reader memory limit.",
                            )
                            .map_err(E::from)?;
                        let mut key = Vec::with_capacity(length);
                        key.extend(prefix.iter().cloned());
                        key.resize(
                            length,
                            DirectedValue::Desc(TotalOrdDesc(Value::Bool(false))),
                        );
                        Ok::<_, E>(key)
                    })
                    .transpose()?
            } else {
                None
            };
            let terminal_bound = if let Some(key) = physical_run_end.as_ref() {
                Bound::Excluded(key)
            } else if reverse && pinned_prefix.is_some() {
                match run_start {
                    Bound::Included(key) => Bound::Included(key),
                    _ => unreachable!(),
                }
            } else {
                cursor.source_terminal_bound()
            };
            let terminal = match terminal_bound {
                Bound::Included(key) => Some((
                    true,
                    source
                        .index_position_key_before(
                            current,
                            key,
                            if reverse { RowId(0) } else { RowId(u64::MAX) },
                            if reverse { TxId(0) } else { TxId(u64::MAX) }, &mut touch)?,
                )),
                Bound::Excluded(key) => Some((
                    false,
                    source
                        .index_position_key_before(
                            current,
                            key,
                            if reverse { RowId(u64::MAX) } else { RowId(0) },
                            if reverse { TxId(u64::MAX) } else { TxId(0) }, &mut touch)?,
                )),
                Bound::Unbounded => None,
            };
            let terminal = terminal
                .as_ref()
                .map_or(Bound::Unbounded, |(included, key)| {
                    if *included {
                        Bound::Included(key.as_bytes())
                    } else {
                        Bound::Excluded(key.as_bytes())
                    }
                });
            let range = RelationalIndexRange {
                current,
                lower: if reverse { terminal } else { edge },
                upper: if reverse { edge } else { terminal },
                reverse,
            };
            let index = cursor
                .index_name()
                .ok_or_else(|| E::from(Error::Other("ordered cursor has no index".into())))?;
            boundary()?;
            let mut physical = source.index_cursor(table, index, range).map_err(E::from)?;
            let mut callback_error = None;
            let fetched = physical.next_window_before(&mut || {
                touch().map_err(|error| {
                    callback_error = Some(error);
                    Error::Other("posting work admission refused".into())
                })
            });
            if let Some(error) = callback_error {
                return Err(error);
            }
            let Some(posting) = fetched.map_err(E::from)? else {
                if cursor.source_finish_run() {
                    continue;
                }
                return Ok(None);
            };
            observed(&posting.value().key, posting.value().entry.row_id)?;
            if !cursor.source_inside_run(&posting.value().key) {
                if cursor.source_finish_run() {
                    continue;
                }
                return Ok(None);
            }
            let row = if posting.value().entry.visible_at(snapshot) {
                let selected = if current {
                    let (_, key) = source
                        .row_head(table, posting.value().entry.row_id)
                        .map_err(E::from)?;
                    source.exact_row(table, &key).map_err(E::from)?
                } else {
                    visible_row(source, table, posting.value().entry.row_id, snapshot)
                        .map_err(E::from)?
                };
                if selected.is_none() && snapshot == source.snapshot() {
                    return Err(E::from(Error::StoreCorrupted {
                        path: table.to_owned(),
                        reason: "visible posting has no saved row version".into(),
                    }));
                }
                selected
            } else {
                None
            };
            let bytes = posting.position_key().len().saturating_add(
                posting
                    .value()
                    .key
                    .iter()
                    .map(|part| match part {
                        DirectedValue::Asc(value) => value.0.estimated_bytes(),
                        DirectedValue::Desc(value) => value.0.estimated_bytes(),
                    })
                    .sum::<usize>(),
            );
            clone(
                bytes.saturating_add(row.as_ref().map_or(0, |row| row.value().estimated_bytes())),
            )?;
            cursor
                .source_advance_charged(
                    posting.position_key().to_vec(),
                    posting.value().key.clone(),
                    posting.value().entry.row_id,
                    bytes,
                )
                .map_err(E::from)?;
            if let Some(row) = row {
                return Ok(Some(row.value().clone()));
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn writer_exact_next<E: From<Error>>(
        &self,
        source: &crate::persistence::writer_adapters::WriterRelationalSource,
        table: &str,
        index: &str,
        snapshot: SnapshotId,
        cursor: &mut contextdb_relational::mem::BoundedExactIndexCursor,
        mut storage_step: impl FnMut() -> std::result::Result<(), E>,
        mut touch: impl FnMut() -> std::result::Result<(), E>,
        mut retain: impl FnMut(usize) -> std::result::Result<(), E>,
        mut release: impl FnMut(usize) -> std::result::Result<(), E>,
        mut clone: impl FnMut(usize) -> std::result::Result<(), E>,
    ) -> std::result::Result<Option<VersionedRow>, E> {
        use contextdb_relational::source::{RelationalIndexRange, RelationalReadSource, visible_row};
        use std::ops::Bound;
        let mut preflight_refusal = None;
        let preflight = source.historical_index_preflight(table, index, snapshot,
            &mut || storage_step().map_err(|error| { preflight_refusal = Some(error); Error::ReadCancelled }));
        if let Some(error) = preflight_refusal { return Err(error); }
        preflight.map_err(E::from)?;
        while let Some(key) = cursor.source_probe() {
            let current = snapshot == source.snapshot();
            let start = source
                .index_position_key_before(current, key, RowId(0), TxId(0), &mut storage_step)?;
            let end = source
                .index_position_key_before(current, key, RowId(u64::MAX), TxId(u64::MAX), &mut storage_step)?;
            let lower = cursor
                .source_position()
                .map_or(Bound::Included(start.as_bytes()), Bound::Excluded);
            let mut physical = source
                .index_cursor(
                    table,
                    index,
                    RelationalIndexRange {
                        current,
                        lower,
                        upper: Bound::Included(end.as_bytes()),
                        reverse: false,
                    },
                )
                .map_err(E::from)?;
            let mut callback_error = None;
            let fetched = physical.next_window_before(&mut || {
                storage_step().map_err(|error| {
                    callback_error = Some(error);
                    Error::Other("posting work admission refused".into())
                })
            });
            if let Some(error) = callback_error {
                return Err(error);
            }
            let Some(posting) = fetched.map_err(E::from)? else {
                let bytes = cursor.retained_bytes();
                cursor.source_finish_probe();
                release(bytes)?;
                continue;
            };
            touch()?;
            let row = if posting.value().entry.visible_at(snapshot) {
                let selected = visible_row(source, table, posting.value().entry.row_id, snapshot)
                    .map_err(E::from)?;
                if selected.is_none() && snapshot == source.snapshot() {
                    return Err(E::from(Error::StoreCorrupted {
                        path: table.to_owned(),
                        reason: "visible posting has no saved row version".into(),
                    }));
                }
                selected
            } else {
                None
            };
            let bytes = posting.position_key().len();
            retain(bytes)?;
            clone(row.as_ref().map_or(0, |row| row.value().estimated_bytes()))?;
            let previous = cursor.retained_bytes();
            cursor.source_advance_charged(
                posting.position_key().to_vec(),
                posting.value().entry.row_id,
                bytes,
            );
            release(previous)?;
            if let Some(row) = row {
                return Ok(Some(row.value().clone()));
            }
        }
        Ok(None)
    }
}

impl Database {
    pub(super) fn relational_point_with_tx(
        &self,
        tx: Option<TxId>,
        table: &str,
        col: &str,
        value: &Value,
        snapshot: SnapshotId,

        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<Option<VersionedRow>> {
        if self.retained_relational_source.is_some() {
            if tx.is_some() {
                return Err(Error::Other(
                    "a committed-image relational source cannot carry a write transaction"
                        .to_owned(),
                ));
            }
            if let Some(row_id) = self.indexed_visible_row_id_by_columns(
                table,
                &[col.to_owned()],
                std::slice::from_ref(value),
                snapshot,
                &std::collections::HashSet::new(),
                None,
                read_memory.clone(),
            )? {
                return row_id
                    .map(|row_id| {
                        self.committed_row_at(table, row_id, snapshot, read_memory.clone())
                    })
                    .transpose()
                    .map(Option::flatten);
            }
            return Ok(self
                .committed_rows_at(table, snapshot, read_memory)?
                .into_iter()
                .find(|row| row.values.get(col) == Some(value)));
        }
        use contextdb_relational::source::{RelationalReadSource, visible_row};
        let Some(root) = self.writer_read_root()? else {
            return self
                .relational
                .point_lookup_with_tx(tx, table, col, value, snapshot);
        };
        let source = crate::persistence::writer_adapters::WriterRelationalSource::new_for_read(
            root,
            read_memory.clone(),
        )?;
        let meta = self
            .table_meta(table)
            .ok_or_else(|| Error::TableNotFound(table.to_owned()))?;
        let mut output = contextdb_core::read_memory::ReadCredit::new(read_memory.clone());
        if let Some(tx) = tx {
            let staged = self.writer_staged_row_matching(
                tx,
                table,
                snapshot,
                false,
                |row| row.values.get(col) == Some(value),
                read_memory.clone(),
            )?;
            if staged.is_some() {
                self.retain_writer_output(output);
                return Ok(staged);
            }
        }
        if let Some(index) = meta
            .indexes
            .iter()
            .find(|index| index.columns.len() == 1 && index.columns[0].0 == col)
        {
            let key = vec![match index.columns[0].1 {
                SortDirection::Asc => DirectedValue::Asc(TotalOrdAsc(value.clone())),
                SortDirection::Desc => DirectedValue::Desc(TotalOrdDesc(value.clone())),
            }];
            source.historical_index_preflight(table, &index.name, snapshot, &mut || Ok(()))?;
            let mut postings = source.exact_index_cursor(
                table,
                &index.name,
                snapshot == source.snapshot(),
                &key,
            )?;
            while let Some(posting) = postings.next_window()? {
                if !posting.value().entry.visible_at(snapshot) {
                    continue;
                }
                let hidden = match tx {
                    Some(tx) => self.tx_mgr.with_write_set(tx, |ws| {
                        ws.relational_deletes
                            .iter()
                            .any(|(t, row, _)| t == table && *row == posting.value().entry.row_id)
                            || ws.relational_inserts.iter().any(|(t, row)| {
                                t == table && row.row_id == posting.value().entry.row_id
                            })
                    })?,
                    None => false,
                };
                if hidden {
                    continue;
                }
                if let Some(row) =
                    visible_row(&source, table, posting.value().entry.row_id, snapshot)?
                {
                    if let Some(tx) = tx
                        && self.tx_mgr.with_write_set(tx, |ws| {
                            row_matches_delete_predicates(
                                &ws.relational_delete_predicates,
                                table,
                                row.value(),
                            )
                        })?
                    {
                        continue;
                    }
                    output.try_grow_for(
                        row.value().estimated_bytes(),
                        "writer_source",
                        "point_output",
                        "Raise MEMORY_LIMIT to return this row.",
                    )?;
                    let row = row.value().clone();
                    self.retain_writer_output(output);
                    return Ok(Some(row));
                }
            }
            return Ok(None);
        }
        let rows = self.relational.scan_filter_with_source(
            tx,
            table,
            snapshot,
            &source,
            &|row| row.values.get(col) == Some(value),
            |bytes| {
                output.try_grow_for(
                    bytes,
                    "writer_source",
                    "point_output",
                    "Raise MEMORY_LIMIT to find this row.",
                )
            },
        )?;
        let row = rows
            .into_iter()
            .find(|row| row.values.get(col) == Some(value));
        self.retain_writer_output(output);
        Ok(row)
    }

    pub(super) fn relational_scan_filter_with_tx(
        &self,
        tx: Option<TxId>,
        table: &str,
        snapshot: SnapshotId,
        predicate: &dyn Fn(&VersionedRow) -> bool,

        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<Vec<VersionedRow>> {
        if self.retained_relational_source.is_some() {
            if tx.is_some() {
                return Err(Error::Other(
                    "a committed-image relational source cannot carry a write transaction"
                        .to_owned(),
                ));
            }
            return Ok(self
                .committed_rows_at(table, snapshot, read_memory)?
                .into_iter()
                .filter(predicate)
                .collect());
        }
        let Some(root) = self.writer_read_root()? else {
            return self
                .relational
                .scan_filter_with_tx(tx, table, snapshot, predicate);
        };
        let source = crate::persistence::writer_adapters::WriterRelationalSource::new_for_read(
            root,
            read_memory.clone(),
        )?;
        let mut output = contextdb_core::read_memory::ReadCredit::new(read_memory.clone());
        let rows = self.relational.scan_filter_with_source(
            tx,
            table,
            snapshot,
            &source,
            predicate,
            |bytes| {
                output.try_grow_for(
                    bytes,
                    "writer_source",
                    "scan_output",
                    "Raise MEMORY_LIMIT to return these rows.",
                )
            },
        )?;
        self.retain_writer_output(output);
        Ok(rows)
    }
}

impl Database {
    pub(super) fn writer_constraint_staged_match(
        &self,
        tx: TxId,
        table: &str,
        columns: &[String],
        values: &[Value],
        skip: Option<RowId>,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<Option<RowId>> {
        let meta = self
            .table_meta(table)
            .ok_or_else(|| Error::TableNotFound(table.into()))?;
        let index = meta.indexes.iter().find(|index| {
            index.columns.len() >= columns.len()
                && index
                    .columns
                    .iter()
                    .zip(columns)
                    .all(|((column, _), wanted)| column == wanted)
        });
        let _scratch = contextdb_core::read_memory::ReadCredit::try_new(
            read_memory.clone(),
            values
                .iter()
                .map(Value::estimated_bytes)
                .sum::<usize>()
                .saturating_mul(8)
                .saturating_add(512),
            "writer_source",
            "staged_constraint_keys",
            "Raise MEMORY_LIMIT to validate this key.",
        )?;
        let key = index.map(|index| index_key_from_values(&index.columns[..columns.len()], values));
        self.tx_mgr.with_write_set(tx, |ws| {
            if let (Some(index), Some(key)) = (index, key.as_ref())
                && ws
                    .ordered_staged_rows
                    .has_index_for(table, &index.name, &meta)
                && let Some(cursor) = ws
                    .ordered_staged_rows
                    .prefix_cursor(table, &index.name, key)
            {
                for entry in cursor {
                    #[cfg(feature = "in-process-test-seams")]
                    crate::writer_query_test_seams::note_constraint_staged_row();
                    #[cfg(feature = "test-seams")]
                    crate::writer_query_test_seams::note_target_entry(table);
                    let Some((name, row)) = ws.relational_inserts.get(entry.position) else {
                        return Err(Error::Other("maintained staged position is absent".into()));
                    };
                    if name != table || row.row_id != entry.row_id {
                        return Err(Error::Other(
                            "maintained staged position names another row".into(),
                        ));
                    }
                    if Some(row.row_id) != skip {
                        return Ok(Some(row.row_id));
                    }
                }
                return Ok(None);
            }
            // Row owners are maintained even without a declared index, so an
            // empty target needs no scan of unrelated staged inserts.
            if ws
                .ordered_staged_rows
                .statement_rows(table)
                .is_none_or(|rows| rows.is_empty())
            {
                return Ok(None);
            }
            // A transaction may have staged its rows before this index was declared.
            // Preserve the existing bounded-memory scan until those rows are restaged.
            Ok(ws.relational_inserts.iter().rev().find_map(|(name, row)| {
                #[cfg(feature = "in-process-test-seams")]
                crate::writer_query_test_seams::note_constraint_staged_row();
                #[cfg(feature = "test-seams")]
                crate::writer_query_test_seams::note_target_entry(name);
                (name == table
                    && Some(row.row_id) != skip
                    && columns
                        .iter()
                        .zip(values)
                        .all(|(column, value)| row.values.get(column) == Some(value)))
                .then_some(row.row_id)
            }))
        })?
    }

    pub(super) fn writer_indexed_row(
        &self,
        table: &str,
        columns: &[String],
        values: &[Value],
        snapshot: SnapshotId,
        skip_deleted: &HashSet<RowId>,
        stats: Option<&mut CommitStageStats>,

        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<Option<Option<RowId>>> {
        self.writer_indexed_row_with_overlay(
            table,
            columns,
            values,
            snapshot,
            skip_deleted,
            stats,
            None,
            read_memory.clone(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn writer_indexed_row_with_overlay(
        &self,
        table: &str,
        columns: &[String],
        values: &[Value],
        snapshot: SnapshotId,
        skip_deleted: &HashSet<RowId>,
        stats: Option<&mut CommitStageStats>,
        overlay: Option<(TxId, Option<RowId>)>,

        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<Option<Option<RowId>>> {
        use contextdb_relational::source::{
            RelationalIndexRange, RelationalReadSource,
        };
        use std::ops::Bound;
        let Some(root) = self.writer_read_root()? else {
            return Ok(None);
        };
        let meta = self
            .table_meta(table)
            .ok_or_else(|| Error::TableNotFound(table.to_owned()))?;
        let Some(index) = meta.indexes.iter().find(|index| {
            index.columns.len() >= columns.len()
                && index
                    .columns
                    .iter()
                    .zip(columns)
                    .all(|((have, _), want)| have == want)
        }) else {
            return Ok(None);
        };
        let source = crate::persistence::writer_adapters::WriterRelationalSource::new_for_read(
            root,
            read_memory.clone(),
        )?;
        let mut scratch = contextdb_core::read_memory::ReadCredit::new(read_memory.clone());
        scratch.try_grow_for(
            values
                .iter()
                .map(Value::estimated_bytes)
                .sum::<usize>()
                .saturating_mul(8)
                .saturating_add(256),
            "writer_source",
            "constraint_keys",
            "Raise MEMORY_LIMIT to validate this key.",
        )?;
        let key = index_key_from_values(&index.columns[..columns.len()], values);
        source.indexed_point_preflight(table, &index.name, snapshot, &mut || Ok(()))?;
        let current = snapshot == source.snapshot();
        let mut cursor = if index.columns.len() == columns.len() {
            source.exact_index_cursor(table, &index.name, current, &key)?
        } else {
            let endpoint = source.index_position_key(current, &key, RowId(0), TxId(0))?;
            source.index_cursor(
                table,
                &index.name,
                RelationalIndexRange {
                    current,
                    lower: Bound::Included(endpoint.as_bytes()),
                    upper: Bound::Unbounded,
                    reverse: false,
                },
            )?
        };
        if let Some(stats) = stats {
            stats.indexed_probes = stats.indexed_probes.saturating_add(1);
        }
        while let Some(window) = cursor.next_window()? {
            #[cfg(feature = "in-process-test-seams")]
            crate::writer_query_test_seams::note_constraint_posting();
            #[cfg(feature = "test-seams")]
            crate::writer_query_test_seams::note_target_entry(table);
            let posting = window.value();
            if !posting.key.starts_with(&key) {
                break;
            }
            if skip_deleted.contains(&posting.entry.row_id) || !posting.entry.visible_at(snapshot) {
                continue;
            }
            if let Some((tx, skip)) = overlay
                && (skip == Some(posting.entry.row_id)
                    || self.tx_mgr.with_write_set(tx, |ws| {
                        ws.ordered_staged_rows
                            .contains_row(table, posting.entry.row_id)
                            || ws
                                .relational_deletes
                                .iter()
                                .any(|(name, row, _)| name == table && *row == posting.entry.row_id)
                    })?)
            {
                continue;
            }
            if let Some(row) = source.indexed_point_row(table, posting.entry.row_id, snapshot)? {
                // An unchanged index key can retain its older posting while
                // scalar row versions advance; compare the indexed values.
                if posting.key.len() != index.columns.len()
                    || index.columns.iter().zip(&posting.key).any(|((column, _), value)| {
                        let value = match value {
                            DirectedValue::Asc(value) => &value.0,
                            DirectedValue::Desc(value) => &value.0,
                        };
                        row.value().values.get(column) != Some(value)
                    })
                {
                    return Err(Error::StoreCorrupted { path: table.into(),
                        reason: "indexed point posting differs from its captured row".into() });
                }
                if let Some((tx, _)) = overlay
                    && self.tx_mgr.with_write_set(tx, |ws| {
                        row_matches_delete_predicates(
                            &ws.relational_delete_predicates,
                            table,
                            row.value(),
                        )
                    })?
                {
                    continue;
                }
                return Ok(Some(Some(posting.entry.row_id)));
            }
        }
        Ok(Some(None))
    }
}

impl Database {
    /// One dispatch for every graph consumer: images never ask for a writer
    /// publication, and memory databases alone have no captured source.
    pub(crate) fn graph_read_root(
        &self,
    ) -> Result<Option<crate::persistence::graph_read::GraphReadRoot>> {
        if let Some(source) = &self.retained_graph_source {
            return Ok(Some(crate::persistence::graph_read::GraphReadRoot::Image(
                source.clone(),
            )));
        }
        Ok(self
            .writer_read_root()?
            .map(crate::persistence::graph_read::GraphReadRoot::Writer))
    }
    pub(crate) fn graph_snapshot(
        &self,
        snapshot: SnapshotId,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<Option<Arc<dyn contextdb_graph::source::GraphSnapshot>>> {
        self.graph_read_root()?
            .map(|root| root.snapshot(snapshot, read_memory))
            .transpose()
    }
}

pub(super) fn durable_root(
    handle: Option<Arc<dyn std::any::Any + Send + Sync>>,
) -> Result<Option<Arc<WriterSnapshot>>> {
    handle
        .map(|handle| {
            let root = handle.downcast::<WriterSnapshot>().map_err(|_| {
                Error::Other("writer publication has an invalid durable root".into())
            })?;
            Ok(root)
        })
        .transpose()
}

impl Database {
    pub(super) fn capture_writer_statement(
        &self,
    ) -> Result<(SnapshotId, Option<Arc<WriterSnapshot>>)> {
        if self.already_closed() {
            return Err(closed_database_error());
        }
        if let Some(snapshot) = SNAPSHOT_OVERRIDE.with(|cell| *cell.borrow())
            && let Some(root) = self.snapshot_registry.pinned_root(snapshot) {
                return Ok((snapshot, Some(root)));
            }
        if let Some(root) = ROOT.with(|slot| {
            slot.borrow()
                .as_ref()
                .filter(|(owner, _, _)| *owner == Arc::as_ptr(&self.tx_mgr) as usize)
                .and_then(|(_, root, _)| root.clone())
        }) {
            let snapshot = SNAPSHOT_OVERRIDE.with(|cell| cell.borrow().unwrap_or(root.snapshot));
            return Ok((snapshot, Some(root)));
        }
        let published = self.tx_mgr.capture_published_read();
        let snapshot = SNAPSHOT_OVERRIDE.with(|cell| cell.borrow().unwrap_or(published.snapshot));
        Ok((snapshot, durable_root(published.handle)?))
    }
}

impl Database {
    /// Schema mutations use the actual writer frontier even when a callback
    /// runs inside an older accepted read. The enclosing read retains its
    /// original descriptor when this nested writer scope exits.
    pub(super) fn capture_writer_schema_statement(
        &self,
    ) -> Result<(SnapshotId, Option<Arc<WriterSnapshot>>)> {
        if self.already_closed() {
            return Err(closed_database_error());
        }
        let published = self.tx_mgr.capture_published_read();
        Ok((published.snapshot, durable_root(published.handle)?))
    }

    pub(super) fn capture_writer_transaction_statement(
        &self,
        tx: Option<TxId>,
        _read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<(SnapshotId, Option<Arc<WriterSnapshot>>)> {
        if self.persistence.is_none() || tx.is_none() {
            return self.capture_writer_statement();
        }
        if self.already_closed() {
            return Err(closed_database_error());
        }
        let published = self
            .tx_mgr
            .transaction_read(tx.expect("checked transaction"))?;
        if let Some(snapshot) = SNAPSHOT_OVERRIDE.with(|cell| *cell.borrow())
            && let Some(root) = self.snapshot_registry.pinned_root(snapshot) {
                return Ok((snapshot, Some(root)));
            }
        let snapshot = SNAPSHOT_OVERRIDE.with(|cell| cell.borrow().unwrap_or(published.snapshot));
        Ok((snapshot, durable_root(published.handle)?))
    }
}

impl Database {
    pub(super) fn clone_writer_row(
        &self,
        row: &VersionedRow,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<VersionedRow> {
        let output = contextdb_core::read_memory::ReadCredit::try_new(
            read_memory.clone(),
            row.estimated_bytes(),
            "writer_source",
            "row_output",
            "Raise MEMORY_LIMIT to return this row.",
        )?;
        let row = row.clone();
        self.retain_writer_output(output);
        Ok(row)
    }

    pub(super) fn writer_cursor_storage_pin(
        &self,
        snapshot: SnapshotId,
        root: Arc<WriterSnapshot>,
        recapture: bool,
        budget: &ReadRegistrationBudget,
        withdrawn: &OwnerReadCancellation,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<
        Option<(
            SnapshotId,
            Arc<WriterSnapshot>,
            crate::persistence::OwnerCursorStoragePin,
        )>,
    > {
        let persistence = self
            .persistence
            .as_ref()
            .ok_or_else(closed_database_error)?;
        let started = Instant::now();
        let allowance = budget.remaining()?;
        let wake = Arc::new((Mutex::new(()), Condvar::new()));
        let cancelled = wake.clone();
        let _listener = withdrawn.tell_on_cancel(move || {
            let _state = cancelled.0.lock();
            cancelled.1.notify_all();
        });
        let mut selected = Some((snapshot, root));
        loop {
            if withdrawn.is_cancelled() {
                return Ok(None);
            }
            budget.remaining()?;
            let Some(remaining) = allowance.checked_sub(started.elapsed()) else {
                return Err(budget.elapsed_timeout());
            };
            let root = &selected.as_ref().expect("selected cursor root").1;
            match persistence.owner_cursor_pin_for_root(root) {
                Ok(pin) => {
                    let (snapshot, root) = selected.take().expect("selected cursor root");
                    return Ok(Some((snapshot, root, pin)));
                }
                Err(crate::persistence::OwnerCursorPinAdmissionError::Refused(error)) => {
                    return Err(error);
                }
                Err(crate::persistence::OwnerCursorPinAdmissionError::Busy(_)) => {
                    // A failed unregistered attempt owns no accepted view.
                    // Release it before waiting and select a whole successor
                    // pair afterward. Graph registrations already accepted a
                    // view, and therefore preserve their original root.
                    if recapture {
                        drop(selected.take());
                    }
                    #[cfg(test)]
                    {
                        owner_cursor_pin_retry_tests::after_busy_attempt_for_test();
                        self.snapshot_registry
                            .owner_cursor_busy_test_pause
                            .maybe_pause();
                    }
                    let mut state = wake.0.lock();
                    if !withdrawn.is_cancelled() {
                        wake.1
                            .wait_for(&mut state, remaining.min(Duration::from_millis(5)));
                    }
                    drop(state);
                    if recapture && !withdrawn.is_cancelled() {
                        budget.remaining()?;
                        let (snapshot, root) = self.capture_writer_transaction_statement(
                            self.active_read_transaction(),
                            read_memory.clone(),
                        )?;
                        selected = Some((snapshot, root.ok_or_else(closed_database_error)?));
                    }
                }
            }
        }
    }
}

pub(super) struct WriterStagedPostingAdmission {
    accountant: Option<Arc<crate::memory_accounting::MemoryAccountant>>,
    writer_use: Option<crate::memory_accounting::WriterUse>,
    control: Option<crate::memory_accounting::ControlCredit>,
}
impl WriterStagedPostingAdmission {
    pub(super) fn prepare(accountant: Arc<crate::memory_accounting::MemoryAccountant>) -> Result<Arc<Self>> {
        let mut value = Self {
            writer_use: crate::memory_accounting::WriterUse::retain_if_writer(&accountant),
            accountant: Some(accountant),
            control: None,
        };
        let bytes = crate::memory_accounting::writer_adapter_layout(&value).size();
        value.control = Some(crate::memory_accounting::ControlCredit::admit_assembly(
            value.accountant.as_ref().expect("provider has its funder"),
            bytes, "staged_posting_provider",
        )?);
        Ok(Arc::new(value))
    }
}
impl contextdb_relational::mem::StagedPostingAdmission for WriterStagedPostingAdmission {
    fn reserve(&self, bytes: usize) -> Result<Arc<dyn contextdb_tx::StagedPostingCharge>> {
        crate::memory_accounting::OwnedMemoryReservation::try_new_for(
            self.accountant.as_ref().expect("live provider has its funder").clone(),
            bytes, "relational", "staged_index_posting",
            "Reduce staged rows or raise MEMORY_LIMIT.",
        )?.into_admitted_arc().map(|owner| owner as Arc<dyn contextdb_tx::StagedPostingCharge>)
    }
}
impl Drop for WriterStagedPostingAdmission {
    fn drop(&mut self) {
        let refund = self.control.take().map(crate::memory_accounting::ControlCredit::prepare);
        let mut cleanup = crate::memory_accounting::ReservationCleanup::new(
            self.accountant.take(), self.writer_use.take(), 0, refund,
        );
        cleanup.finish();
    }
}

/// Storage consumers capture the same root as the statement currently running
/// on this thread. The weak manager breaks the store/manager ownership cycle.
pub(super) struct ScopedGraphSource {
    manager: OnceLock<std::sync::Weak<TransactionManager<DynStore>>>,
}
impl ScopedGraphSource {
    pub(super) fn new() -> Self {
        Self {
            manager: OnceLock::new(),
        }
    }
    pub(super) fn bind(&self, manager: &Arc<TransactionManager<DynStore>>) {
        self.manager
            .set(Arc::downgrade(manager))
            .expect("graph source bound once");
    }
}
impl contextdb_graph::source::GraphReadSource for ScopedGraphSource {
    fn capture(
        &self,
        snapshot: SnapshotId,
        read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<Arc<dyn contextdb_graph::source::GraphSnapshot>> {
        let manager = self
            .manager
            .get()
            .and_then(std::sync::Weak::upgrade)
            .ok_or_else(|| Error::Other("graph publication is unavailable".into()))?;
        let owner = Arc::as_ptr(&manager) as usize;
        let scoped = ROOT.with(|slot| {
            slot.borrow()
                .as_ref()
                .filter(|(id, _, _)| *id == owner)
                .and_then(|(_, root, _)| root.clone())
        });
        let root = match scoped {
            Some(root) => root,
            None => durable_root(manager.capture_published_read().handle)?
                .ok_or_else(|| Error::Other("graph publication has no durable root".into()))?,
        };
        if snapshot.0 > root.snapshot.0 {
            return Err(Error::Other(
                "graph snapshot exceeds the captured source frontier".into(),
            ));
        }
        Ok(Arc::new(
            crate::persistence::writer_adapters::WriterGraphSnapshot::new_for_read(
                root,
                read_memory,
            )?,
        ))
    }
}

/// Probe only the selected endpoints. Reverse probes skip versions already
/// visited by a selected source, preserving one visit per physical version.
pub(super) fn visit_graph_versions_for_nodes(
    store: &GraphStore,
    root: Option<Arc<WriterSnapshot>>,
    nodes: &HashSet<NodeId>,
    mut visit: impl FnMut(&AdjEntry) -> Result<()>,
    read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<()> {
    for node in nodes {
        for reverse in [false, true] {
            let mut consume = |edge: &AdjEntry| -> Result<()> {
                #[cfg(feature = "test-seams")]
                resident_history_test_seams::note(|work| work.graph_keys_examined += 1);
                if !reverse || !nodes.contains(&edge.source) {
                    visit(edge)?;
                }
                Ok(())
            };
            if let Some(root) = &root {
                let lower = node.as_bytes().to_vec();
                let upper = node
                    .as_u128()
                    .checked_add(1)
                    .map(|next| next.to_be_bytes().to_vec());
                let mut cursor = root.cursor(
                    crate::persistence::writer_source::SourceWindowOwner::Read(read_memory.clone()),
                    if reverse { "graph_rev" } else { "graph_fwd" },
                    crate::persistence::writer_source::WriterCursorFamily::Bytes,
                    std::ops::Bound::Included(lower.as_slice()),
                    upper.as_ref().map_or(std::ops::Bound::Unbounded, |upper| {
                        std::ops::Bound::Excluded(upper.as_slice())
                    }),
                    false,
                )?;
                while let Some(window) = cursor.next_edge()? {
                    consume(&window.value)?;
                }
            } else {
                let adjacency = if reverse {
                    &store.reverse_adj
                } else {
                    &store.forward_adj
                };
                if let Some(edges) = adjacency.read().get(node) {
                    for edge in edges {
                        consume(edge)?;
                    }
                }
            }
        }
    }
    Ok(())
}

pub(super) fn visit_graph_versions(
    store: &GraphStore,
    root: Option<Arc<WriterSnapshot>>,
    mut visit: impl FnMut(&AdjEntry) -> Result<()>,
    read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<()> {
    if let Some(root) = root {
        // All-version enumeration is historical even at frontier equality.
        let graph = crate::persistence::graph_read::GraphReadRoot::Writer(root);
        let mut cursor = graph.cursor(false, std::ops::Bound::Unbounded,
            std::ops::Bound::Unbounded, read_memory.clone())?;
        while let Some(window) = cursor.next_edge()? { visit(window.edge())?; }
    } else {
        for edge in store.forward_adj.read().values().flatten() { visit(edge)?; }
    }
    Ok(())
}

impl Database {
    /// A durable store, writer or retained reader, keeps every relational
    /// posting, automatic or declared, in one ordered family, so a bounded
    /// cursor can walk a single-column automatic index in key order.
    pub(crate) fn automatic_indexes_are_ordered(&self) -> bool {
        self.persistence.is_some() || self.retained_relational_source.is_some()
    }

    /// Every committed adjacency version in one orientation: the reverse
    /// family is keyed by target, the forward family by source. A durable
    /// writer reads the statement's root; only a memory store has resident
    /// maps to walk.
    pub(super) fn visit_graph_adjacency(
        &self,
        reverse: bool,
        visit: impl FnMut(&AdjEntry) -> Result<()>,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<()> {
        self.visit_graph_adjacency_at_root(self.graph_read_root()?, reverse, visit, read_memory)
    }
    pub(super) fn visit_graph_adjacency_at_root(
        &self,
        root: Option<crate::persistence::graph_read::GraphReadRoot>,
        reverse: bool,
        mut visit: impl FnMut(&AdjEntry) -> Result<()>,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<()> {
        if let Some(root) = root {
            let mut cursor = root.cursor(
                reverse,
                std::ops::Bound::Unbounded,
                std::ops::Bound::Unbounded,
                read_memory,
            )?;
            while let Some(window) = cursor.next_edge()? {
                visit(window.edge())?;
            }
            return Ok(());
        }
        let adjacency = if reverse {
            &self.graph_store.reverse_adj
        } else {
            &self.graph_store.forward_adj
        };
        for edge in adjacency.read().values().flatten() {
            visit(edge)?;
        }
        Ok(())
    }
    pub(super) fn visit_graph_versions(
        &self,
        visit: impl FnMut(&AdjEntry) -> Result<()>,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<()> {
        self.visit_graph_adjacency(false, visit, read_memory)
    }
    pub(super) fn collect_graph_versions(
        &self,
        mut keep: impl FnMut(&AdjEntry) -> bool,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<Vec<AdjEntry>> {
        let mut edges = Vec::new();
        let mut memory = contextdb_core::read_memory::ReadCredit::new(read_memory.clone());
        self.visit_graph_versions(
            |edge| {
                if keep(edge) {
                    memory.try_grow_for(
                        edge.estimated_bytes().saturating_mul(2).saturating_add(128),
                        "writer_source",
                        "graph_output",
                        "Raise MEMORY_LIMIT to return these graph versions.",
                    )?;
                    edges.push(edge.clone());
                }
                Ok(())
            },
            read_memory.clone(),
        )?;
        self.retain_writer_output(memory);
        Ok(edges)
    }
}

impl Database {
    pub(super) fn graph_path_with_sync_overlay(
        &self,
        snapshot: SnapshotId,
        edge_type: &str,
        start: NodeId,
        goal: NodeId,
        overlay: &HashMap<(NodeId, EdgeType, NodeId), bool>,

        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<bool> {
        let root = self.graph_snapshot(snapshot, read_memory.clone())?;
        let mut memory = contextdb_core::read_memory::ReadCredit::new(read_memory.clone());
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();
        memory.try_grow_for(
            256,
            "writer_source",
            "sync_cycle_frontier",
            "Raise MEMORY_LIMIT to validate this graph traversal.",
        )?;
        visited.insert(start);
        queue.push_back(start);
        while let Some(node) = queue.pop_front() {
            let mut found = false;
            let mut enqueue = |target| -> Result<bool> {
                if target == goal {
                    found = true;
                    return Ok(false);
                }
                if !visited.contains(&target) {
                    memory.try_grow_for(
                        128,
                        "writer_source",
                        "sync_cycle_frontier",
                        "Raise MEMORY_LIMIT to validate this graph traversal.",
                    )?;
                    visited.insert(target);
                    queue.push_back(target);
                }
                Ok(true)
            };
            self.graph_store.visit_edges(
                root.as_ref(),
                contextdb_graph::source::GraphQuery {
                    node,
                    direction: Direction::Outgoing,
                    identity: None,
                },
                |entry| {
                    if entry.visible_at(snapshot)
                        && entry.edge_type == edge_type
                        && overlay
                            .get(&(entry.source, entry.edge_type.clone(), entry.target))
                            .copied()
                            .unwrap_or(true)
                    {
                        return enqueue(entry.target);
                    }
                    Ok(true)
                },
            )?;
            for ((source, kind, target), present) in overlay {
                if *source == node && kind == edge_type && *present && !enqueue(*target)? {
                    break;
                }
            }
            if found {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub(super) fn collect_change_log_versions(
        &self,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<Vec<ChangeLogEntry>> {
        let Some(root) = self.writer_read_root()? else {
            return Ok(self.change_log.read().clone());
        };
        let mut cursor = root.change_log_cursor(
            crate::persistence::writer_source::SourceWindowOwner::Read(read_memory.clone()),
            Lsn(0),
            root.lsn,
        )?;
        let mut memory = contextdb_core::read_memory::ReadCredit::new(read_memory.clone());
        let mut entries = Vec::new();
        while let Some(window) = cursor.next_change()? {
            memory.try_grow_for(
                crate::composite_store::change_log_entry_retained_bytes(&window.value)
                    .saturating_mul(2)
                    .saturating_add(128),
                "writer_source",
                "change_history_output",
                "Raise MEMORY_LIMIT to prepare this history output.",
            )?;
            entries.push(window.value);
        }
        self.retain_writer_output(memory);
        Ok(entries)
    }
}

impl Database {
    /// Every retained version of the rows that have carried one natural key
    /// in one table. A confirmed echo refreshes only those versions, so it
    /// seeks the key's postings instead of decoding every table once for each
    /// arriving row. A table with no index over exactly the key's columns
    /// streams that table alone.
    pub(super) fn visit_row_versions_for_natural_key(
        &self,
        table: &str,
        key: &NaturalKey,
        mut visit: impl FnMut(&VersionedRow) -> Result<()>,

        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<()> {
        use contextdb_relational::source::RelationalReadSource;
        let Some(root) = self.writer_read_root()? else {
            if let Some(rows) = self.relational_store.tables.read().get(table) {
                for row in rows {
                    visit(row)?;
                }
            }
            return Ok(());
        };
        let Some(meta) = root.borrow_captured_table_meta(table) else {
            return Ok(());
        };
        let columns = key.key_columns();
        let values = key.key_values();
        let index = meta
            .indexes
            .iter()
            .find(|index| {
                index.columns.len() == columns.len()
                    && index
                        .columns
                        .iter()
                        .zip(&columns)
                        .all(|((have, _), want)| have == want)
            })
            .cloned();
        let source = crate::persistence::writer_adapters::WriterRelationalSource::new_for_read(
            root,
            read_memory.clone(),
        )?;
        source.verify_historical_versions(table)?;
        let Some(index) = index else {
            let mut rows = source.row_cursor(
                table,
                std::ops::Bound::Unbounded,
                std::ops::Bound::Unbounded,
                false,
            )?;
            while let Some(row) = rows.next_window()? {
                visit(row.value())?;
            }
            return Ok(());
        };
        source.historical_index_versions_preflight(table, &index.name, &mut || Ok(()))?;
        let mut memory = contextdb_core::read_memory::ReadCredit::try_new(
            read_memory.clone(),
            values
                .iter()
                .map(Value::estimated_bytes)
                .sum::<usize>()
                .saturating_mul(8)
                .saturating_add(256),
            "writer_source",
            "echo_identity",
            "Raise MEMORY_LIMIT to reconcile this row identity.",
        )?;
        let tuple = index_key_from_values(&index.columns, &values);
        // The history family holds a posting for every retained version, so
        // a row whose visible version has since moved off this key is still
        // found by the version that carried it.
        let mut postings = source.exact_index_cursor(table, &index.name, false, &tuple)?;
        let mut seen = HashSet::new();
        while let Some(posting) = postings.next_window()? {
            let id = posting.value().entry.row_id;
            if seen.contains(&id) {
                continue;
            }
            memory.try_grow_for(
                std::mem::size_of::<RowId>().saturating_mul(4),
                "writer_source",
                "echo_identity",
                "Raise MEMORY_LIMIT to reconcile this row identity.",
            )?;
            seen.insert(id);
            let mut versions = source.row_id_cursor(table, id, source.snapshot())?;
            while let Some(row) = versions.next_window()? {
                visit(row.value())?;
            }
        }
        Ok(())
    }
}

#[cfg(all(test, feature = "test-seams"))]
mod indexed_lineage_growth_tests {
    use super::*;
    use crate::read_work_probe::{ReadWorkObservationScope, ReadWorkPhase};

    #[test]
    fn historical_indexed_lineage_point_work_is_flat_across_unrelated_history() {
        let directory = tempfile::tempdir().unwrap();
        let db = Database::open(directory.path().join("lineage-growth.db")).unwrap();
        db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        db.execute("CREATE TABLE notes (id INT PRIMARY KEY, body TEXT) HISTORY ALL", &HashMap::new()).unwrap();
        db.execute("INSERT INTO notes VALUES (0, 'original')", &HashMap::new()).unwrap();
        let snapshot = db.snapshot();
        let mut previous = 0;
        for count in [16, 32, 64] {
            for id in previous..count {
                db.execute("INSERT INTO notes VALUES ($id, 'neighbor')",
                    &HashMap::from([("id".into(), Value::Int64(id + 1))])).unwrap();
            }
            previous = count;
            let reader = db.declared_reader_memory().unwrap();
            let work = ReadWorkObservationScope::enter(ReadWorkPhase::Query);
            let row = db.required_indexed_visible_row_by_columns("notes", &["id".into()], &[Value::Int64(0)],
                snapshot, &HashSet::new(), reader).unwrap().unwrap();
            assert_eq!(row.values.get("body"), Some(&Value::Text("original".into())));
            let observed = work.counters();
            assert_eq!(observed.decoded_rows, 2, "one served lineage at history {count}: {observed:?}");
            assert_eq!(observed.hashed_rows, 0, "producer root witnesses replace history hashing");
            assert_eq!(observed.hashed_bytes, 0, "the indexed seek hashes no unrelated history");
            assert_eq!(observed.proof_nodes_hashed, 0, "producer witnesses avoid a history proof walk");
            println!("LINEAGE_GROWTH history={count} {observed:?}");
        }
    }
}

#[cfg(all(test, feature = "test-seams"))]
mod vector_write_copy_budget_tests {
    use super::*;

    fn staged_fixture(db: &Database) -> TxId {
        db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        db.execute("CREATE TABLE vectors (id INT PRIMARY KEY, v VECTOR(1024))", &HashMap::new()).unwrap();
        let tx = db.begin().unwrap();
        for id in 0..40 {
            db.execute_in_tx(tx, "INSERT INTO vectors VALUES ($id, $v)", &HashMap::from([
                ("id".into(), Value::Int64(id)), ("v".into(), Value::Vector(vec![1.0; 1024])),
            ])).unwrap();
        }
        tx
    }

    #[test]
    fn staged_insert_copies_use_writer_memory_without_consuming_the_reader_ceiling() {
        let directory = tempfile::tempdir().unwrap();
        let mut options = crate::DatabaseOpenOptions::default();
        options.owner_reads.limits.limits.memory = 128 * 1024;
        let db = Database::open_with_options(directory.path().join("write-copies.db"), options).unwrap();
        let tx = staged_fixture(&db);
        let reader_before = db.declared_read_memory.standing_control_bytes();
        db.commit(tx).expect("write copies exceed reader memory but fit writer memory");
        assert_eq!(db.declared_read_memory.standing_control_bytes(), reader_before,
            "publication releases its source-reader controls and owns INSERT copies on the writer");
    }

    #[test]
    fn staged_insert_copy_writer_refusal_preserves_the_committed_frontier() {
        let directory = tempfile::tempdir().unwrap();
        let db = Database::open(directory.path().join("write-copy-refusal.db")).unwrap();
        let tx = staged_fixture(&db);
        let frontier = db.current_lsn();
        let used = db.accountant().usage().used;
        db.set_memory_limit(Some(used + 16 * 1024)).unwrap();
        let error = db.commit(tx).expect_err("writer copy preparation must admit before mutation");
        assert!(matches!(&error, Error::MemoryBudgetExceeded { subsystem, operation, .. }
            if subsystem == "writer" && operation == "prepare_vector_insert_copies"), "{error:?}");
        assert_eq!(db.current_lsn(), frontier);
        db.set_memory_limit(None).unwrap();
        // A refused public commit already aborts the staged transaction.
        assert!(matches!(db.rollback(tx), Err(Error::TxNotFound(_))));
        let rows = db.execute("SELECT COUNT(*) FROM vectors", &HashMap::new()).unwrap();
        assert_eq!(rows.rows, vec![vec![Value::Int64(0)]]);
    }
}
