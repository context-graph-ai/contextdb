//! A scoped borrow of the manager's admitted published root. Nested calls to
//! the same database retain that root; another database gets its own scope.
use super::*;
use crate::persistence::WriterSnapshot;
type OutputOwners =
    std::rc::Rc<std::cell::RefCell<Vec<crate::memory_accounting::OwnedMemoryReservation>>>;
type ScopedRoot = (usize, Arc<WriterSnapshot>, OutputOwners);
thread_local! { static ROOT: std::cell::RefCell<Option<ScopedRoot>> = const { std::cell::RefCell::new(None) }; }

/// Preserve the consumer's typed refusal across the storage callback's shared
/// core-error boundary; the private sentinel never reaches the caller.
pub(super) fn next_window_with_work<T: Send, E: From<Error>>(
    cursor: &mut dyn contextdb_relational::source::RelationalCursor<T>,
    before: &mut dyn FnMut() -> std::result::Result<(), E>,
) -> std::result::Result<Option<Box<dyn contextdb_relational::source::RelationalWindow<T>>>, E> {
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
    Option<Box<dyn contextdb_relational::source::RelationalWindow<VersionedRow>>>,
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
    while let Some(window) = next_window_with_work(&mut *cursor, before)? {
        if window.value().created_tx.0 <= snapshot.0 {
            return Ok(window.value().visible_at(snapshot).then_some(window));
        }
    }
    Ok(None)
}
pub(crate) struct WriterReadScope {
    prior: Option<ScopedRoot>,
    _same_thread: std::marker::PhantomData<std::rc::Rc<()>>,
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
    /// Commit preparation needs the actual committed frontier, independently
    /// of an outer statement's retained transaction base.
    pub(super) fn committed_row_exists_at_current_frontier(
        &self,
        table: &str,
        row_id: RowId,
    ) -> Result<bool> {
        let published = self.tx_mgr.capture_published_read();
        if let Some(root) = durable_root(published.handle)? {
            let source = crate::persistence::writer_adapters::WriterRelationalSource::new(root)?;
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
    ) -> Result<(
        Option<Lsn>,
        Option<contextdb_relational::store::SyncSourceKind>,
    )> {
        use contextdb_relational::source::RelationalReadSource;
        if let Some(root) = self.writer_read_root()? {
            let source = crate::persistence::writer_adapters::WriterRelationalSource::new(root)?;
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
                Some(id @ Value::Uuid(_)) => {
                    self.relational_point_with_tx(Some(tx), table, "id", id, snapshot)?
                }
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
                .map(|(_, root, _)| root.clone())
        })
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
                .map(|(_, root, _)| root.clone())
        }) {
            return Ok(Some(root));
        }
        let root = durable_root(self.tx_mgr.capture_published_read().handle)?;
        root.ok_or_else(|| Error::Other("writer publication has no durable root".into()))
            .map(Some)
    }
    pub(crate) fn enter_writer_read(&self, root: Option<Arc<WriterSnapshot>>) -> WriterReadScope {
        let owner = Arc::as_ptr(&self.tx_mgr) as usize;
        let owners = ROOT
            .with(|slot| {
                slot.borrow()
                    .as_ref()
                    .filter(|(id, _, _)| *id == owner)
                    .map(|(_, _, owners)| owners.clone())
            })
            .unwrap_or_default();
        let next = root.map(|root| (owner, root, owners));
        let prior = ROOT.with(|slot| std::mem::replace(&mut *slot.borrow_mut(), next));
        WriterReadScope {
            prior,
            _same_thread: std::marker::PhantomData,
        }
    }
    pub(super) fn writer_read_scope(&self) -> Result<WriterReadScope> {
        Ok(self.enter_writer_read(self.writer_read_root()?))
    }

    pub(crate) fn retain_writer_output(
        &self,
        output: crate::memory_accounting::OwnedMemoryReservation,
    ) {
        ROOT.with(|slot| {
            if let Some((owner, _, owners)) = slot.borrow().as_ref()
                && *owner == Arc::as_ptr(&self.tx_mgr) as usize
            {
                owners.borrow_mut().push(output);
            }
        });
    }
}

impl Database {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn writer_ordered_next<E: From<Error>>(
        &self,
        source: &dyn contextdb_relational::source::RelationalReadSource,
        table: &str,
        snapshot: SnapshotId,
        cursor: &mut contextdb_relational::mem::BoundedOrderedRowCursor,
        mut boundary: impl FnMut() -> std::result::Result<(), E>,
        mut touch: impl FnMut() -> std::result::Result<(), E>,
        mut clone: impl FnMut(usize) -> std::result::Result<(), E>,
    ) -> std::result::Result<Option<VersionedRow>, E> {
        use contextdb_relational::source::{RelationalIndexRange, visible_row};
        use std::ops::Bound;
        if cursor.is_exhausted() {
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
                            .index_position_key(
                                current,
                                &key.to_vec(),
                                if reverse { RowId(u64::MAX) } else { RowId(0) },
                                if reverse { TxId(u64::MAX) } else { TxId(0) },
                            )
                            .map_err(E::from)?,
                    )),
                    Bound::Excluded(key) => Some((
                        false,
                        source
                            .index_position_key(
                                current,
                                &key.to_vec(),
                                if reverse { RowId(0) } else { RowId(u64::MAX) },
                                if reverse { TxId(0) } else { TxId(u64::MAX) },
                            )
                            .map_err(E::from)?,
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
            let range = RelationalIndexRange {
                current,
                lower: if reverse { Bound::Unbounded } else { edge },
                upper: if reverse { edge } else { Bound::Unbounded },
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
            if !cursor.source_inside_run(&posting.value().key) {
                if cursor.source_finish_run() {
                    continue;
                }
                return Ok(None);
            }
            let row = if posting.value().entry.visible_at(snapshot) {
                visible_row(source, table, posting.value().entry.row_id, snapshot)
                    .map_err(E::from)?
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
        source: &dyn contextdb_relational::source::RelationalReadSource,
        table: &str,
        index: &str,
        snapshot: SnapshotId,
        cursor: &mut contextdb_relational::mem::BoundedExactIndexCursor,
        mut touch: impl FnMut() -> std::result::Result<(), E>,
        mut retain: impl FnMut(usize) -> std::result::Result<(), E>,
        mut release: impl FnMut(usize) -> std::result::Result<(), E>,
        mut clone: impl FnMut(usize) -> std::result::Result<(), E>,
    ) -> std::result::Result<Option<VersionedRow>, E> {
        use contextdb_relational::source::{RelationalIndexRange, visible_row};
        use std::ops::Bound;
        while let Some(key) = cursor.source_probe() {
            let current = snapshot == source.snapshot();
            let start = source
                .index_position_key(current, key, RowId(0), TxId(0))
                .map_err(E::from)?;
            let end = source
                .index_position_key(current, key, RowId(u64::MAX), TxId(u64::MAX))
                .map_err(E::from)?;
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
                touch().map_err(|error| {
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
            let row = if posting.value().entry.visible_at(snapshot) {
                visible_row(source, table, posting.value().entry.row_id, snapshot)
                    .map_err(E::from)?
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
    ) -> Result<Option<VersionedRow>> {
        use contextdb_relational::source::{RelationalReadSource, visible_row};
        let Some(root) = self.writer_read_root()? else {
            return self
                .relational
                .point_lookup_with_tx(tx, table, col, value, snapshot);
        };
        let source = crate::persistence::writer_adapters::WriterRelationalSource::new(root)?;
        let meta = self
            .table_meta(table)
            .ok_or_else(|| Error::TableNotFound(table.to_owned()))?;
        let mut output =
            crate::memory_accounting::OwnedMemoryReservation::new(self.accountant.clone());
        if let Some(tx) = tx {
            let staged = self.tx_mgr.with_write_set(tx, |ws| {
                ws.relational_inserts
                    .iter()
                    .rev()
                    .find(|(t, row)| t == table && row.values.get(col) == Some(value))
                    .map(|(_, row)| {
                        output.try_grow_for(
                            row.estimated_bytes(),
                            "writer_source",
                            "point_output",
                            "Raise MEMORY_LIMIT to return this row.",
                        )?;
                        Ok::<_, Error>(row.clone())
                    })
                    .transpose()
            })??;
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
    ) -> Result<Vec<VersionedRow>> {
        let Some(root) = self.writer_read_root()? else {
            return self
                .relational
                .scan_filter_with_tx(tx, table, snapshot, predicate);
        };
        let source = crate::persistence::writer_adapters::WriterRelationalSource::new(root)?;
        let mut output =
            crate::memory_accounting::OwnedMemoryReservation::new(self.accountant.clone());
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
        let _scratch = crate::memory_accounting::OwnedMemoryReservation::try_new_for(
            self.accountant.clone(),
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
    ) -> Result<Option<Option<RowId>>> {
        self.writer_indexed_row_with_overlay(
            table,
            columns,
            values,
            snapshot,
            skip_deleted,
            stats,
            None,
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
    ) -> Result<Option<Option<RowId>>> {
        use contextdb_relational::source::{
            RelationalIndexRange, RelationalReadSource, visible_row,
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
        let source = crate::persistence::writer_adapters::WriterRelationalSource::new(root)?;
        let mut scratch =
            crate::memory_accounting::OwnedMemoryReservation::new(self.accountant.clone());
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
            if let Some(row) = visible_row(&source, table, posting.entry.row_id, snapshot)? {
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
    pub(crate) fn writer_graph_root(
        &self,
    ) -> Result<Option<Arc<dyn contextdb_graph::source::GraphSnapshot>>> {
        self.writer_read_root()?
            .map(|root| {
                crate::persistence::writer_adapters::WriterGraphSnapshot::new(root).map(|source| {
                    Arc::new(source) as Arc<dyn contextdb_graph::source::GraphSnapshot>
                })
            })
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
        if let Some(root) = ROOT.with(|slot| {
            slot.borrow()
                .as_ref()
                .filter(|(owner, _, _)| *owner == Arc::as_ptr(&self.tx_mgr) as usize)
                .map(|(_, root, _)| root.clone())
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
    pub(super) fn capture_writer_transaction_statement(
        &self,
        tx: Option<TxId>,
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
        let snapshot = SNAPSHOT_OVERRIDE.with(|cell| cell.borrow().unwrap_or(published.snapshot));
        Ok((snapshot, durable_root(published.handle)?))
    }
}

impl Database {
    pub(super) fn clone_writer_row(&self, row: &VersionedRow) -> Result<VersionedRow> {
        let output = crate::memory_accounting::OwnedMemoryReservation::try_new_for(
            self.accountant.clone(),
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
        root: &WriterSnapshot,
        budget: &ReadRegistrationBudget,
        withdrawn: &OwnerReadCancellation,
    ) -> Result<Option<crate::persistence::OwnerCursorStoragePin>> {
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
        loop {
            if withdrawn.is_cancelled() {
                return Ok(None);
            }
            budget.remaining()?;
            let Some(remaining) = allowance.checked_sub(started.elapsed()) else {
                return Err(budget.elapsed_timeout());
            };
            match persistence.owner_cursor_pin_for_root(root) {
                Ok(pin) => return Ok(Some(pin)),
                Err(crate::persistence::OwnerCursorPinAdmissionError::Refused(error)) => {
                    return Err(error);
                }
                Err(crate::persistence::OwnerCursorPinAdmissionError::Busy(_)) => {
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
                }
            }
        }
    }
}

pub(super) struct WriterStagedPostingAdmission(
    pub(super) Arc<crate::memory_accounting::MemoryAccountant>,
);
impl contextdb_relational::mem::StagedPostingAdmission for WriterStagedPostingAdmission {
    fn reserve(&self, bytes: usize) -> Result<Arc<dyn contextdb_tx::StagedPostingCharge>> {
        Ok(Arc::new(
            crate::memory_accounting::OwnedMemoryReservation::try_new_for(
                self.0.clone(),
                bytes,
                "relational",
                "staged_index_posting",
                "Reduce staged rows or raise MEMORY_LIMIT.",
            )?,
        ))
    }
}
