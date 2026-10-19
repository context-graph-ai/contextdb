//! Local erasure is staged and committed with ordinary transaction writes.
use super::*;
use crate::custody::records::{Record, RowRef};
use contextdb_core::EdgeDiscardMode;

pub(crate) struct Stage {
    pub(crate) durable: AuthoritativePurgePersistenceProjection,
    pub(crate) publish: Box<dyn FnOnce() + Send>,
    pub(crate) _vector_workspace: crate::read_image_memory::ImageVectorCopy,
}
pub(crate) type StageRegistry = Arc<Mutex<HashMap<Lsn, Stage>>>;

impl Database {
    pub(crate) fn discard_policy(
        &self,
        table: &str,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<Option<(String, EdgeDiscardMode)>> {
        // Stopping the server does not remove durable hub authority.
        if self.sync_relay_mode_enabled()
            || self
                .custody_authority(read_memory.clone())?
                .iter()
                .any(|record| match record {
                    Record::Control(control) => control.hub_node.is_some(),
                    Record::Policy(policy) => policy.first_bound_position.is_some(),
                    _ => false,
                })
        {
            return Err(Error::DiscardNotOnHub);
        }
        let meta = self
            .table_meta(table)
            .ok_or_else(|| Error::TableNotFound(table.into()))?;
        let direction = crate::executor::effective_sync_direction(&meta);
        if !matches!(
            direction,
            contextdb_core::SyncDirection::Push | contextdb_core::SyncDirection::None
        ) {
            return Err(Error::DiscardNotEligible {
                table: table.into(),
                direction: match direction {
                    contextdb_core::SyncDirection::Both => "two_way",
                    _ => "pull_only",
                }
                .into(),
            });
        }
        if direction == contextdb_core::SyncDirection::None {
            return Ok(None);
        }
        let Some(hub) = self.retention_sync_peer() else {
            return Ok(None);
        };
        let records = self.custody_authority(read_memory.clone())?;
        let destination = records.iter().find_map(|r| match r {
            Record::Destination(d) if d.namespace.hub_node == hub => Some(&d.namespace),
            _ => None,
        });
        let Some(mode) = records.iter().find_map(|r| match r {
            Record::Binding(b) if destination == Some(&b.namespace) => b
                .tables
                .iter()
                .find(|p| p.table == table)
                .map(|p| p.policy.edge_discard),
            _ => None,
        }) else {
            // Only bound tables carry a hub-declared discard mode.
            return Ok(None);
        };
        if mode == EdgeDiscardMode::Never {
            return Err(Error::EdgeDiscardDenied {
                hub_node_id: hub,
                table: table.into(),
                mode,
                pending_count: 0,
            });
        }
        Ok(Some((hub, mode)))
    }

    pub(crate) fn stage_discard(
        &self,
        tx: TxId,
        tables: &[(String, Vec<(NaturalKey, RowId)>)],

        read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<QueryResult> {
        let mut vector_copy_owner = crate::vector_read_memory::admission(read_memory.clone());
        let authority = self.custody_authority(read_memory.clone())?;
        let hub = self.retention_sync_peer();
        let destination = authority.iter().find_map(|r| match r {
            Record::Destination(d) if Some(d.namespace.hub_node.as_str()) == hub.as_deref() => {
                Some(&d.namespace)
            }
            _ => None,
        });
        let mut result = QueryResult::empty();
        result.columns = vec![
            "table".into(),
            "rows_affected".into(),
            "survivors".into(),
            "pending_units".into(),
        ];
        let native_registrations = if let DatabaseTransactionHost::Owned(handle) = &self.tx_mgr {
            handle.with_host(|host| self.original_discard_registration_refs(host, tx))?
        } else { MetadataBuffer::empty() };
        let registrations = if matches!(&self.tx_mgr, DatabaseTransactionHost::Legacy(_)) {
            self.pending_commit_metadata.lock().get(&tx)
                .map(|m| m.delivery_registrations.clone()).unwrap_or_default()
        } else { Vec::new() };
        let registration_refs = || registrations.iter().map(|r| (&r.root, &r.members))
            .chain(native_registrations.values.iter().map(|r| {
                let (root, members) = r.body.as_ref().expect("admitted discard registration");
                (root, members)
            }));
        let mut selections = Vec::new();
        let mut selected_rows = BTreeSet::new();
        let mut selected_references = Vec::new();
        for (table, rows) in tables {
            let policy = self.discard_policy(table, read_memory.clone())?;
            let mut pending = BTreeSet::new();
            for (key, row_id) in rows {
                let reference = RowRef {
                    table: table.clone(),
                    key: key.clone(),
                };
                let records =
                    self.custody_rows(std::slice::from_ref(&reference), read_memory.clone())?;
                for record in records.iter() {
                    if let Record::Manifest(m) = record && m.rows.iter().any(|e| e.reference == reference)
                        && records.iter().any(|r| matches!(r,Record::Root{submission,..} if *submission==m.id))
                        && !records.iter().any(|r| matches!(r,Record::Terminal{edge:true,record:t} if t.submission==m.id && destination==Some(&t.namespace))) { pending.insert(m.id.as_bytes().to_vec()); }
                }
                // Transaction-local rows participate in the same selection.
                // Newly staged units have no durable outcome and remain pending under the policy.
                for (root, members) in registration_refs() {
                    if *root == reference || members.contains(&reference) {
                        pending.insert(root.bytes());
                    }
                }
                let committed_key = self
                    .find_row_by_id_at(
                        table,
                        *row_id,
                        self.snapshot_for_read(),
                        read_memory.clone(),
                    )
                    .ok()
                    .and_then(|row| {
                        self.table_meta(table)
                            .and_then(|meta| natural_key_from_row_values(&meta, &row.values))
                    });
                if let Some(key) = committed_key {
                    selections.push(self.resolve_authoritative_purge_selection(
                        table,
                        &key,
                        read_memory.clone(),
                    )?);
                } else if self
                    .table_meta(table)
                    .is_some_and(|m| m.delivery_manifest_tables.is_some())
                    && !registration_refs().any(|(root, _)| *root == reference)
                {
                    pending.insert(reference.bytes());
                }
                selected_rows.insert((table.clone(), *row_id));
                selected_references.push(reference);
            }
            let pending_count = if policy.is_some() {
                pending.len() as u64
            } else {
                0
            };
            if let Some((hub, mode)) = policy
                && mode == EdgeDiscardMode::AfterOutcome
                && pending_count != 0
            {
                return Err(Error::EdgeDiscardDenied {
                    hub_node_id: hub,
                    table: table.clone(),
                    mode,
                    pending_count,
                });
            }
            // Pending units are reported once, under their root table.
            let reported_pending = if self
                .table_meta(table)
                .is_some_and(|m| m.delivery_manifest_tables.is_some())
            {
                pending_count
            } else {
                0
            };
            result.rows.push(vec![
                Value::Text(table.clone()),
                Value::Int64(rows.len() as i64),
                Value::Json(serde_json::json!([])),
                Value::Int64(reported_pending as i64),
            ]);
        }
        // Selection and every policy check have completed before changing the overlay.
        if let DatabaseTransactionHost::Owned(handle) = &self.tx_mgr {
            handle.with_host(|host| self.stage_original_discard_selection(
                host, tx, &selected_rows, &selected_references, &selections,
            ))?;
            result.rows_affected = selected_rows.len() as u64;
            return Ok(result);
        }
        let mut removed = WriteSet::default();
        self.tx_mgr.with_write_set(tx, |ws| -> Result<()> {
            let prepared = ws
                .ordered_staged_rows
                .prepare_statement_removals(&selected_rows)?;
            ws.ordered_staged_rows
                .publish_statement_removals(prepared, &selected_rows);
            // The admitted figure travels with the version it describes, so
            // the release below answers what storing the row took instead of
            // re-pricing it against the table's current shape.
            let discarded_admissions = ws
                .relational_insert_admitted_bytes
                .iter()
                .flat_map(|(table, rows)| {
                    rows.iter()
                        .map(move |(row_id, bytes)| (table.clone(), *row_id, *bytes))
                })
                .filter(|(table, row_id, _)| selected_rows.contains(&(table.clone(), *row_id)))
                .collect::<Vec<_>>();
            for (table, row_id, bytes) in discarded_admissions {
                removed.set_relational_insert_admitted_bytes(table, row_id, bytes);
            }
            ws.relational_inserts.retain(|(table, row)| {
                if selected_rows.contains(&(table.clone(), row.row_id)) {
                    removed
                        .relational_inserts
                        .push((table.clone(), row.clone()));
                    false
                } else {
                    true
                }
            });
            ws.forget_admitted_bytes_for_unstaged_rows();
            for entry in &ws.vector_inserts {
                if selected_rows.contains(&(entry.index.table.clone(), entry.row_id)) {
                    ws.ordered_staged_rows
                        .remove_vector_capability(&entry.index, entry.row_id);
                }
            }
            ws.vector_inserts.retain(|entry| {
                if selected_rows.contains(&(entry.index.table.clone(), entry.row_id)) {
                    removed.vector_inserts.push(entry.clone());
                    false
                } else {
                    true
                }
            });
            ws.vector_deletes
                .retain(|(index, id, _)| !selected_rows.contains(&(index.table.clone(), *id)));
            ws.vector_moves.retain(|(index, from, to, _)| {
                !selected_rows.contains(&(index.table.clone(), *from))
                    && !selected_rows.contains(&(index.table.clone(), *to))
            });
            ws.relational_deletes
                .retain(|(table, id, _)| !selected_rows.contains(&(table.clone(), *id)));
            for selection in &selections {
                ws.relational_deletes
                    .push((selection.table.clone(), selection.local_row_id, tx));
            }
            ws.rebind_ordered_staged_rows();
            Ok(())
        })??;
        self.release_insert_allocations(&removed, &mut vector_copy_owner)?;
        result.rows_affected = selected_rows.len() as u64;
        let mut pending = self.pending_commit_metadata.lock();
        let metadata = pending.entry(tx).or_default();
        metadata
            .delivery_registrations
            .retain(|r| !selected_references.contains(&r.root));
        metadata
            .conditional_update_guards
            .retain(|g| !selected_rows.contains(&(g.table.clone(), g.row_id)));
        metadata
            .upsert_intents
            .retain(|g| !selected_rows.contains(&(g.table.clone(), g.row_id)));
        metadata
            .sync_lineage_guards
            .retain(|g| !selected_rows.contains(&(g.table.clone(), g.row_id)));
        metadata.discard_selections.extend(selections);
        Ok(result)
    }

    fn original_discard_registration_refs(
        &self, host: &EngineDatabaseTransactions, tx: TxId,
    ) -> Result<MetadataBuffer<AdmittedMetadataValue<(RowRef, Vec<RowRef>)>>> {
        host.events.with_owned_persistence_original(|original| {
            let records = host.metadata.records.lock();
            let Some(record) = EngineMetadataJournal::record(&records, tx) else {
                return Ok(MetadataBuffer::empty());
            };
            let mut copied = MetadataBuffer::prepare(record.metadata.delivery_registrations.values.len(), original)?;
            for entry in &record.metadata.delivery_registrations.values {
                let entry = entry.body.as_ref().ok_or_else(EngineMetadataJournal::identity_error)?;
                let mut bytes = discard_reference_copy_bytes(&entry.root)?
                    .checked_add(crate::custody::preparation::preparation_array_bytes::<RowRef>(entry.members.len())?)
                    .ok_or_else(EngineMetadataJournal::identity_error)?;
                for member in &entry.members {
                    bytes = bytes.checked_add(discard_reference_copy_bytes(member)?)
                        .ok_or_else(EngineMetadataJournal::identity_error)?;
                }
                let invoice = crate::memory_accounting::reserve_preparation_from_original(
                    original, bytes, "relational", "discard_registration_selection",
                    "Reduce the discard selection or raise MEMORY_LIMIT.",
                )?;
                let mut body = AdmittedMetadataValue::new((discard_copy_reference(&entry.root)?, Vec::new()), invoice);
                let (_, members) = body.body.as_mut().expect("prepared discard references");
                *members = Vec::with_capacity(entry.members.len());
                if members.capacity() != entry.members.len() { return Err(EngineMetadataJournal::identity_error()); }
                for member in &entry.members { members.push(discard_copy_reference(member)?); }
                copied.push(body);
            }
            Ok(copied)
        })
    }

    pub(super) fn capture_original_discard_selections(
        &self, host: &EngineDatabaseTransactions, tx: TxId,
    ) -> Result<OriginalDiscardSelections> {
        host.events.with_owned_persistence_original(|original| {
            let frame = crate::persistence::OriginalPersistenceFrame::new(original)?;
            let records = host.metadata.records.lock();
            let entries = EngineMetadataJournal::record(&records, tx)
                .map(|record| record.metadata.discard_selections.values.as_slice()).unwrap_or(&[]);
            frame.array::<AuthoritativePurgeSelection>(entries.len())?;
            let mut values = Vec::with_capacity(entries.len());
            if values.capacity() != entries.len() { return Err(EngineMetadataJournal::identity_error()); }
            for entry in entries {
                let selection = entry.body.as_ref().ok_or_else(EngineMetadataJournal::identity_error)?;
                frame.reserve(discard_selection_copy_bytes(selection)?)?;
                values.push(discard_copy_selection(selection)?);
            }
            drop(records);
            Ok(OriginalDiscardSelections { values, _memory: frame.into_reservation() })
        })
    }

    pub(super) fn capture_original_discard_blob_hashes(
        &self, host:&EngineDatabaseTransactions, selections:&[AuthoritativePurgeSelection],
        reader:Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<Arc<crate::persistence::blob_selector::FrozenBlobSelector>> {
        host.events.with_owned_persistence_original(|original| {
            let published=self.tx_mgr.capture_published_read();
            let root=writer_read::durable_root(published.handle)?.ok_or_else(EngineMetadataJournal::identity_error)?;
            let _source=self.enter_writer_read(Some(root.clone()),reader.clone());
            let persistence=self.persistence.clone().ok_or_else(EngineMetadataJournal::identity_error)?;
            let mut hashes=crate::persistence::blob_selector::BlobSelectorBuilder::new(persistence,root,&host.events,original)?;
            self.original_discard_selected_blob_hashes(selections,reader,&mut hashes)?;
            hashes.seal()
        })
    }
    fn original_discard_selected_blob_hashes(
        &self, selections:&[AuthoritativePurgeSelection],
        reader:Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        hashes:&mut crate::persistence::blob_selector::BlobSelectorBuilder,
    ) -> Result<()> {
        let snapshot = self.snapshot_for_read();

        for selected in selections.iter().filter(|selected| selected.table == "work_jobs") {
            let mut visible = 0usize;
            self.visit_original_discard_selected_versions("work_jobs", selected.local_row_id, |row| {
                if row.row_id == selected.local_row_id && row.visible_at(snapshot) {
                    visible = visible.saturating_add(1);
                }
                Ok(true)
            }, reader.clone())?;
            if visible != 1 {
                return Err(Error::SyncError((if visible == 0 {
                    "authoritative purge selected work job is not current-visible"
                } else { "authoritative purge selected work job has multiple visible versions" }).into()));
            }
            self.visit_original_discard_selected_versions("work_jobs", selected.local_row_id, |row| {
                if row.row_id != selected.local_row_id { return Ok(true); }
                let input = row.values.get("input_refs").ok_or_else(|| Error::SyncError(
                    "canonical selected work_jobs history is missing input_refs".into()))?;
                visit_discard_blob_hashes(input, |_| Ok(())).map_err(|error| Error::SyncError((format!(
                    "authoritative purge selected work-job history has malformed input_refs: {error}")).into()))?;
                visit_discard_blob_hashes(input, |hash| {
                    hashes.push(hash)
                })?;
                Ok(true)
            }, reader.clone())?;
        }
        Ok(())
    }

    pub(super) fn visit_original_discard_selected_versions(
        &self, table: &str, selected: RowId,
        mut visit: impl FnMut(&VersionedRow) -> Result<bool>,
        reader: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<()> {
        let root = self.writer_read_root()?.ok_or_else(EngineMetadataJournal::identity_error)?;
        let owner = crate::persistence::writer_source::SourceWindowOwner::Read(reader);
        // The retired pool is keyed by exact occurrence. Match the existing
        // authored-before-canonical precedence without a retained key set.
        if let Some(mut authored) = root.authored_row_cursor(owner.clone(), table, selected)? {
            while let Some(window) = authored.next_authored_row(table)? {
                if !visit(&window.value)? { return Ok(()); }
            }
        }
        if root.borrow_captured_table_meta(table).is_some() {
            let mut canonical = root.row_cursor(owner.clone(), table, selected)?;
            while let Some(window) = canonical.next_row()? {
                let key: &[u8; 24] = window.position().try_into()
                    .map_err(|_| EngineMetadataJournal::identity_error())?;
                // Both windows are independently read-admitted. No row body
                // or identity population survives this fixed overlap.
                if root.authored_row_at(owner.clone(), table, key)?.is_some() { continue; }
                if !visit(&window.value)? { return Ok(()); }
            }
        }
        Ok(())
    }

    fn visit_original_discard_work_rows(
        &self, mut visit: impl FnMut(&VersionedRow) -> Result<bool>,
        reader: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<()> {
        match self.visit_writer_rows("work_jobs", |row| {
            if visit(row).map_err(OriginalDiscardRowVisit::Error)? { Ok(()) }
            else { Err(OriginalDiscardRowVisit::Stop) }
        }, reader) {
            Ok(true) | Err(OriginalDiscardRowVisit::Stop) => return Ok(()),
            Err(OriginalDiscardRowVisit::Error(error)) => return Err(error),
            Ok(false) => {}
        }
        if let Some(rows) = self.relational_store.tables.read().get("work_jobs") {
            for row in rows { if !visit(row)? { break; } }
        }
        Ok(())
    }

    fn original_discard_current_blob_references(
        &self, host:&EngineDatabaseTransactions, original:&OwnedMemoryReservation,
        selections:&[AuthoritativePurgeSelection],
        selected:&Arc<crate::persistence::blob_selector::FrozenBlobSelector>,
        reader:Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        staged:&contextdb_tx::manager::EngineGuard<'_>,
    ) -> Result<Arc<crate::persistence::blob_selector::FrozenBlobSelector>> {
        let root=self.writer_read_root()?.ok_or_else(EngineMetadataJournal::identity_error)?;
        let persistence=self.persistence.clone().ok_or_else(EngineMetadataJournal::identity_error)?;
        let mut survivors=crate::persistence::blob_selector::BlobSelectorBuilder::new(persistence,root,&host.events,original)?;
        if selected.len()==0 { return survivors.seal(); }
        let snapshot=self.snapshot_for_read();
        self.visit_original_discard_work_rows(|row| {
            if !row.visible_at(snapshot) || selections.iter().any(|s|s.table=="work_jobs" && s.local_row_id==row.row_id) {return Ok(true);}
            let input=row.values.get("input_refs").ok_or_else(|| Error::SyncError(
                "authoritative purge surviving work job is missing input_refs".into()))?;
            visit_discard_blob_hashes(input,|_|Ok(())).map_err(|err| Error::SyncError(format!(
                "authoritative purge surviving work job has malformed input_refs: {err}").into()))?;
            visit_discard_blob_hashes(input,|hash| {if selected.contains(&hash)? {survivors.push(hash)?;} Ok(())})?;
            Ok(true)
        },reader)?;
        staged.with_legacy(|ws| -> Result<()> {
            for (table,row) in &ws.relational_inserts {
                if table=="work_jobs" {
                    let refs=row.values.get("input_refs").ok_or_else(||Error::SyncError(
                        "authoritative purge surviving work job is missing input_refs".into()))?;
                    visit_discard_blob_hashes(refs,|_|Ok(())).map_err(|err|Error::SyncError(format!(
                        "authoritative purge surviving work job has malformed input_refs: {err}").into()))?;
                    visit_discard_blob_hashes(refs,|hash| {if selected.contains(&hash)? {survivors.push(hash)?;} Ok(())})?;
                }
            } Ok(())
        })?;
        survivors.seal()
    }

    fn stage_original_discard_selection(
        &self, host: &EngineDatabaseTransactions, tx: TxId,
        rows: &BTreeSet<(String, RowId)>, references: &[RowRef],
        selections: &[AuthoritativePurgeSelection],
    ) -> Result<()> {
        let batch = host.events.with_owned_persistence_original(|original| {
            let mut batch = MetadataBuffer::prepare(selections.len(), original)?;
            for selection in selections {
                let bytes = discard_selection_copy_bytes(selection)?;
                let invoice = crate::memory_accounting::reserve_preparation_from_original(
                    original, bytes, "relational", "discard_selection",
                    "Reduce the discard selection or raise MEMORY_LIMIT.",
                )?;
                let body = discard_copy_selection(selection)?;
                batch.push(AdmittedMetadataValue::new(body, invoice));
            }
            Ok(AdmittedMetadataBatch::Discard(batch))
        })?;
        let checkpoint = self.write_set_checkpoint_owned(host, tx)?;
        let result = (|| {
            host.transactions.with_transactions(|transactions| transactions.with_engine(tx, |held| {
                let removal = held.prepare_remove_rows(rows)?;
                held.publish(removal);
                self.remove_vector_positions_in_restricted_source(held, |entry| {
                    rows.iter().any(|(table, row)| table == &entry.index.table && *row == entry.row_id)
                })?;
                // One reverse-position removal at a time keeps selection scratch
                // constant while the lower journal retains each original inverse.
                macro_rules! remove_aux {
                    ($field:ident, $variant:ident, $test:expr) => {
                        loop {
                            let position = held.read().$field.iter().rposition($test);
                            let Some(position) = position else { break; };
                            let empty = held.copy_aux(contextdb_tx::staged_source::AuxBorrow::$variant(&[]))?;
                            let removal = held.prepare_aux_splice(position, &[position], empty)?;
                            held.publish_tx(removal);
                        }
                    };
                }
                remove_aux!(vector_deletes, VectorDeletes, |(index, id, _)| rows.iter().any(|(t, r)| t == &index.table && r == id));
                remove_aux!(vector_moves, VectorMoves, |(index, from, to, _)| rows.iter().any(|(t, r)| t == &index.table && (r == from || r == to)));
                remove_aux!(relational_deletes, RelationalDeletes, |(table, id, _)| rows.iter().any(|(t, r)| t == table && r == id));
                for selection in selections {
                    self.with_preparation_original(|original| {
                        let invoice = crate::memory_accounting::reserve_preparation_from_original(
                            original, selection.table.len(), "relational", "discard_row_marker",
                            "Reduce the discard selection or raise MEMORY_LIMIT.",
                        )?;
                        let body = (crate::memory_accounting::preparation_copy_string(&selection.table)?, selection.local_row_id, tx);
                        let copied = held.copy_aux(contextdb_tx::staged_source::AuxBorrow::RelationalDeletes(std::slice::from_ref(&body)))?;
                        let prepared = held.prepare_aux_splice(held.read().relational_deletes.len(), &[], copied)?;
                        held.publish_tx(prepared);
                        drop(body); drop(invoice);
                        Ok(())
                    })?;
                }
                macro_rules! prune_metadata {
                    ($field:ident, $variant:ident, $test:expr) => {
                        loop {
                            let position = {
                                let records = host.metadata.records.lock();
                                EngineMetadataJournal::record(&records, tx).and_then(|record| {
                                    record.metadata.$field.values.iter().rposition(|entry| {
                                        entry.body.as_ref().is_some_and($test)
                                    })
                                })
                            };
                            let Some(position) = position else { break; };
                            let prepared = host.metadata.prepare_metadata_splice(tx, position, &[position],
                                AdmittedMetadataBatch::$variant(MetadataBuffer::empty()))?;
                            host.metadata.publish_metadata(prepared);
                        }
                    };
                }
                prune_metadata!(delivery_registrations, Delivery, |r| references.contains(&r.root));
                prune_metadata!(conditional_update_guards, Conditional, |g| rows.iter().any(|(t, r)| t == &g.table && *r == g.row_id));
                prune_metadata!(upsert_intents, Upsert, |g| rows.iter().any(|(t, r)| t == &g.table && *r == g.row_id));
                prune_metadata!(sync_lineage_guards, SyncLineage, |g| rows.iter().any(|(t, r)| t == &g.table && *r == g.row_id));
                let prepared = host.metadata.prepare_metadata_append(tx, batch)?;
                host.metadata.publish_metadata(prepared);
                Ok(())
            }))
        })();
        if result.is_err() { self.restore_write_set_checkpoint(tx, checkpoint)?; }
        else { checkpoint.settle(); }
        result
    }

    pub(super) fn prepare_discard_commit(
        &self,
        ws: &mut WriteSet,
        selections: &[AuthoritativePurgeSelection],
        token: event_bus::AuthoritativePurgeQueueMutationToken,
        guard: &crate::blob_repository::BlobExclusiveHashSetGuard,
        read_publication: &AuthoritativePurgePublicationGuard,
        _vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<CommitStageRegistration<Stage>> {
        let lsn = ws
            .commit_lsn
            .ok_or_else(crate::custody::canonical::invalid)?;
        // Selected immutable row identities are revalidated under the ordinary commit mutex.
        let mut vector_workspace =
            self.reserve_authoritative_erasure_workspace(selections, read_memory.clone())?;
        let prepared = self.prepare_authoritative_purge_set_batch(
            selections,
            token,
            crate::custody::erasure::PurgeReportShape::None,
            lsn,
            &mut vector_workspace.owner,
            read_memory.clone(),
        )?;
        let (selected_hashes, mut survivors) = self.authoritative_purge_current_blob_references(
            selections,
            true,
            read_memory.clone(),
        )?;
        for (table, row) in &ws.relational_inserts {
            if table == "work_jobs"
                && let Some(refs) = row.values.get("input_refs")
            {
                for hash in crate::work_ledger::canonical_blob_hashes_from_input_refs(refs)? {
                    survivors.insert(crate::work_ledger::BlobHash::from_hex(&hash)?.as_bytes());
                }
            }
        }
        let hashes = selected_hashes.difference(&survivors).copied().collect();
        let blob_purge = self
            .blob_repository
            .prepare_authoritative_purge(guard, &hashes, lsn)?;
        let mut keys = crate::custody::erasure::metadata_keys(
            self,
            &selections
                .iter()
                .map(|s| RowRef {
                    table: s.table.clone(),
                    key: s.natural_key.clone(),
                })
                .collect::<Vec<_>>(),
            read_memory.clone(),
        )?;
        for owner in &prepared.lineage_config_owners {
            keys.extend([
                owner.row_sidecar_key.clone(),
                owner.creation_lineage_key.clone(),
                owner.accepted_author_key.clone(),
            ]);
            // A record of an earlier life at the key, such as a purge
            // tombstone or an owed delete, is not the discarded life's.
            if owner.lifecycle_record_is_selected_life {
                keys.push(owner.lifecycle_record_key.clone());
            }
        }
        keys.extend(prepared.graph_arrival_config_keys.iter().cloned());
        // Discarded sidecars cannot be rewritten by earlier work in this transaction.
        ws.config_writes.retain(|(key, _)| !keys.contains(key));
        ws.config_deletes.extend(keys.iter().cloned());
        let erased_tables = selections
            .iter()
            .map(|selection| selection.table.clone())
            .collect::<BTreeSet<_>>();
        // Canonical local controls carry DISCARD across snapshot export without
        // manufacturing a row tombstone or a fleet instruction.
        let frontier_bytes = erased_tables.iter().try_fold(
            erased_tables
                .len()
                .checked_mul(std::mem::size_of::<(String, Vec<u8>)>())
                .ok_or_else(crate::custody::canonical::invalid)?,
            |bytes, table| {
                bytes
                    .checked_add("local_read_erasure_frontier.v1.".len())
                    .and_then(|bytes| bytes.checked_add(table.len()))
                    .and_then(|bytes| bytes.checked_add(std::mem::size_of::<Lsn>()))
                    .ok_or_else(crate::custody::canonical::invalid)
            },
        )?;
        vector_workspace.owner.reserve_for(
            frontier_bytes,
            "erasure",
            "prepare_read_frontiers",
            "Raise MEMORY_LIMIT to prepare local erasure controls.",
        )?;
        let mut local_frontiers = Vec::with_capacity(erased_tables.len());
        for table in &erased_tables {
            let mut key =
                String::with_capacity("local_read_erasure_frontier.v1.".len() + table.len());
            key.push_str("local_read_erasure_frontier.v1.");
            key.push_str(table);
            local_frontiers.push((key, lsn.0.to_le_bytes().to_vec()));
        }
        let durable = AuthoritativePurgePersistenceProjection {
            row_versions: prepared
                .row_version_keys
                .iter()
                .map(|r| (r.table.clone(), r.row_id, r.created_tx, r.lsn))
                .collect(),
            source_provenance: prepared.disk_source_provenance,
            vectors: prepared.vector_entries,
            vector_partition_identities: prepared.vector_partition_identities,
            vector_generation_candidates: prepared.vector_generation_candidates,
            graph_entries: prepared.canonical_graph_entries,
            row_graph_entries: prepared.row_graph_entries,
            row_graph_credit: prepared.row_graph_credit,
            sink_entries: prepared
                .durable_sink_entries
                .into_iter()
                .map(|e| {
                    Ok((
                        e.sink,
                        e.queue_id,
                        e.durable_digest
                            .ok_or_else(crate::custody::canonical::invalid)?,
                    ))
                })
                .collect::<Result<_>>()?,
            change_log_entries: prepared.change_log_entries,
            config_keys_removed: keys,
            lifecycle_records: local_frontiers,
            purge_delivery_items: Vec::new(),
            blob_purge,
        };
        let lineage_keys = prepared.lineage_config_owners.iter().map(|owner| (
            owner.row_sidecar_key.clone(), owner.creation_lineage_key.clone(),
            owner.lifecycle_record_is_selected_life.then(|| owner.lifecycle_record_key.clone()),
        )).collect::<Vec<_>>();
        let pending = {
            let state = self.lineage_state_lock.lock();
            if self.snapshot_registry.pending_delete_source.lock().is_some() {
                Some(self.prepare_resident_pending_changes(&state,
                    lineage_keys.iter().filter_map(|(_, _, key)| key.as_deref().map(|key| (key, None))))?)
            } else { None }
        };
        let erased_tables = selections
            .iter()
            .map(|selection| selection.table.clone())
            .collect::<BTreeSet<_>>();
        let memory_erasure = if self.persistence.is_none() {
            Some(self.lineage_state_lock.lock().erasure_frontiers.prepare(
                &erased_tables, lsn, self.accountant.clone())?)
        } else { None };
        let writer_erasure = self
            .persistence
            .as_ref()
            .map(|persistence| {
                persistence.prepare_writer_erasure(
                    self.accountant.clone(),
                    &erased_tables,
                    &durable.graph_entries,
                )
            })
            .transpose()?;
        let read_gate = read_publication.gate.clone();
        let publication = prepared.publication_replacements;
        let mut memory = PreparedMemorySwap::prepare(
            self.accountant.clone(),
            publication.memory_swap.old_bytes,
            publication.memory_swap.new_bytes,
            publication.memory_swap.retired_bytes_released_by_store,
        )?;
        let relational_store = self.relational_store.clone();
        let change_log = self.change_log.clone();
        let change_log_table_index = self.change_log_table_index.clone();
        let change_log_lsn_refcounts = self.change_log_lsn_refcounts.clone();
        let graph_store = self.graph_store.clone();
        let vector_store = self.vector_store.clone();
        let event_bus = self.event_bus.clone();
        let accepted_sync_row_authors = self.accepted_sync_row_authors.clone();
        let sync_graph_arrivals = self.sync_graph_arrivals.clone();
        let lineage_state_lock = self.lineage_state_lock.clone();
        let custody_cache = self.custody_cache.clone();
        let accountant = self.accountant.vector_budget()?;
        self.local_erasure_stages.lock().insert(
            lsn,
            Stage {
                durable,
                _vector_workspace: vector_workspace,
                publish: Box::new(move || {
                    if let Some(erasure) = writer_erasure {
                        erasure.publish();
                    }
                    // The enclosing commit still owns the matching writer fence.
                    read_gate.invalidate_cursors();
                    relational_store.publish_prepared_received_schema(publication.relational);
                    *change_log.write() = publication.change_log;
                    *change_log_table_index.write() = publication.change_log_table_index;
                    *change_log_lsn_refcounts.write() = publication.change_log_lsn_refcounts;
                    graph_store.publish_prepared_received_schema(publication.graph);
                    vector_store.publish_prepared_received_schema(publication.vector, accountant);
                    event_bus.publish_prepared_authoritative_purge_queue_replacement(
                        publication.event_bus,
                    );
                    {
                        let mut authors = accepted_sync_row_authors.write();
                        for key in publication.accepted_author_memory_keys_removed { authors.remove(&key); }
                    }
                    {
                        let mut arrivals = sync_graph_arrivals.write();
                        for (identity, selected) in publication.graph_arrival_memory_removed {
                            if arrivals.get(&identity).map(|record| &**record) == Some(&selected) { arrivals.remove(&identity); }
                        }
                    }
                    {
                        let mut lineage = lineage_state_lock.lock();
                        if let Some(memory_erasure) = memory_erasure {
                            lineage.erasure_frontiers = memory_erasure.publish();
                        }
                        for (sidecar, creation, lifecycle) in lineage_keys {
                            lineage.row_sidecars.remove(&sidecar);
                            lineage.unbound_creations.remove(&creation);
                            if let Some(key) = lifecycle { lineage.records.remove(&key); }
                        }
                    }
                    if let Some(pending) = pending { pending.publish(); }
                    *custody_cache.lock() = None;
                    memory.commit_after_swap();
                }),
            },
        );
        Ok(CommitStageRegistration::new(
            &self.local_erasure_stages,
            lsn,
        ))
    }
}

impl StartupState<'_> {
    pub(crate) fn with_owned_encoded_event_source<R>(
        &self,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        f: impl for<'s> FnOnce(&'s crate::persistence::OwnedEncodedEventSource<'s>) -> Result<R>,
    ) -> Result<R> {
        match self {
            Self::Durable(persistence) => {
                persistence.with_owned_encoded_event_source(read_memory, f)
            }
            Self::Image(image) => image.config_source.with_owned_encoded_image_source(
                &image.sink_queues,
                read_memory,
                f,
            ),
        }
    }
}

/// Private terminal/discard custody, without a second queue or stage map.
/// The companion publication retains its native frozen append transfer and
/// purge token; all payload/control destruction remains in that companion.
pub(crate) struct OwnedDiscardQueueCustody {
    events: event_bus::OwnedEventBusHandle,
    publication: event_bus::OwnedPurgeQueuePublication,
}

impl OwnedDiscardQueueCustody {
    /// The caller already owns the original commit mutex and read-erasure
    /// fence. Its own prepared DELETE events already exclude selected discard
    /// rows. Purge filtering applies to old queues, never to this own append.
    #[allow(
        private_interfaces,
        reason = "the type is crate-private on purpose; the crate-visible item is only reachable inside the crate"
    )]
    pub(crate) fn prepare_for_commit_locked_discard(
        events: &event_bus::OwnedEventBusHandle,
        own_append: &mut event_bus::OwnedPreparedSinkEvents,
        drain: event_bus::AuthoritativePurgeQueueDrainGuard,
        durable_names: &BTreeSet<String>,
        selections: &BTreeMap<String, HashSet<RowId>>,
        persistence: Option<&RedbPersistence>,
    ) -> Result<Self> {
        // Clone pairs its external original control before taking a count.
        // No companion queue/stage guard is held while the original freeze
        // waits for ALL remaining active mutations, not just pending entries.
        let events = events.clone();
        events.suspend_owned_append_for_discard(own_append)?;
        let token = drain.freeze_for_commit_locked_purge();
        let publication = events.prepare_owned_purge_queue_replacement(
            token,
            durable_names,
            selections,
            Some(own_append),
            persistence,
        )?;
        Ok(Self {
            events,
            publication,
        })
    }

    pub(crate) fn visit_unconfirmed(
        &self, visit: impl FnMut(&str, u64) -> Result<()>,
    ) -> Result<()> {
        self.publication.visit_unconfirmed(visit)
    }

    pub(crate) fn with_sink_names<R>(&self, visit: impl FnOnce(&[String]) -> R) -> R {
        self.publication.with_sink_names(visit)
    }

    /// Invoke at the original local-erasure publication point after durable
    /// success, before ordinary rows. The companion performs the funded map
    /// swap and installs its prebuilt own-append lease BEFORE thawing.
    pub(crate) fn publish(self) {
        let Self {
            events,
            publication,
        } = self;
        events.publish_owned_purge_queue_replacement(publication);
    }
}

/// External custody for the original registry allocation and physical closure
/// Box. Neither invoice is hidden inside the FnOnce payload that Box frees.
pub(crate) struct OriginalDiscardStageHolder {
    registry: StageRegistry,
    lsn: Lsn,
    identity: contextdb_tx::manager::EngineTerminalIdentity,
    visibility_tx: TxId,
    publication: usize,
    history: Option<crate::persistence::discard_history::OriginalDiscardHistory>,
    preparation: Option<crate::memory_accounting::OwnedMemoryReservation>,
    closure: Option<crate::memory_accounting::OwnedMemoryReservation>,
    registry_memory: Option<crate::memory_accounting::OwnedMemoryReservation>,
}
impl OriginalDiscardStageHolder {
    fn matches(&self, stage: &Stage) -> bool {
        (&*stage.publish as *const (dyn FnOnce() + Send) as *const ()) as usize == self.publication
    }
    fn take(&self) -> Option<Stage> {
        let mut registry = self.registry.lock();
        let stage = if registry
            .get(&self.lsn)
            .is_some_and(|stage| self.matches(stage))
        {
            registry.remove(&self.lsn)
        } else {
            None
        };
        drop(registry);
        stage
    }
    fn release_controls(&mut self) {
        // The exact terminal was removed first. Retire empty backing outside
        // the guard before its original bucket/control debit can be refunded.
        let retired = {
            let mut registry = self.registry.lock();
            if registry.is_empty() {
                Some(std::mem::take(&mut *registry))
            } else {
                None
            }
        };
        drop(retired);
        drop(self.preparation.take());
        drop(self.closure.take());
        drop(self.registry_memory.take());
    }
    fn publish(&mut self) {
        assert!(self.history.is_none(), "discard publication follows consumption of its same write");
        let stage = self
            .take()
            .expect("native discard retains its exact terminal publication");
        let Stage {
            durable,
            publish,
            _vector_workspace,
        } = stage;
        // Box<dyn FnOnce> consumption frees the physical closure after its
        // payload is destroyed, while this external holder still owns credit.
        publish();
        drop(durable);
        drop(_vector_workspace);
        self.release_controls();
    }
}
impl Drop for OriginalDiscardStageHolder {
    fn drop(&mut self) {
        let stage = self.take();
        drop(stage);
        drop(self.history.take());
        self.release_controls();
    }
}

pub(crate) struct OriginalDiscardTerminal {
    identity: Option<contextdb_tx::manager::EngineTerminalIdentity>,
    stage: Option<OriginalDiscardStageHolder>,
}
impl OriginalDiscardTerminal {
    pub(crate) fn new() -> Self {
        Self {
            identity: None,
            stage: None,
        }
    }
    pub(crate) fn install(&mut self, holder: OriginalDiscardStageHolder) -> Result<()> {
        if self.stage.is_some() || self.identity.is_some_and(|id| id != holder.identity) {
            return Err(Error::Other(
                "native discard terminal identity changed".into()));
        }
        self.identity = Some(holder.identity);
        self.stage = Some(holder);
        Ok(())
    }
}
thread_local! {static ORIGINAL_DISCARD_TERMINAL:std::cell::Cell<*mut OriginalDiscardTerminal>=const{std::cell::Cell::new(std::ptr::null_mut())};}
struct OriginalDiscardScope(*mut OriginalDiscardTerminal);
impl Drop for OriginalDiscardScope {
    fn drop(&mut self) {
        ORIGINAL_DISCARD_TERMINAL.with(|slot| slot.set(self.0));
    }
}

pub(crate) fn with_original_discard_terminal<R>(
    terminal: &mut OriginalDiscardTerminal,
    f: impl FnOnce() -> R,
) -> R {
    let old = ORIGINAL_DISCARD_TERMINAL.with(|slot| slot.replace(terminal));
    let scope = OriginalDiscardScope(old);
    let result = f();
    drop(scope);
    result
}

pub(crate) fn validate_original_discard_for_apply(
    ws: &WriteSet,
    registry: &StageRegistry,
) -> Result<()> {
    let lsn = ws.commit_lsn;
    ORIGINAL_DISCARD_TERMINAL.with(|slot| {
        let ptr = slot.get();
        if ptr.is_null() {
            if lsn.is_some_and(|lsn| registry.lock().contains_key(&lsn)) {
                return Err(Error::Other(
                    "native discard requires its exact scoped terminal holder".into()));
            }
            return Ok(());
        }
        // SAFETY: with_original_discard_terminal is synchronous; its scope
        // restores this pointer before stack teardown, including unwind.
        let terminal = unsafe { &*ptr };
        if let Some(holder) = &terminal.stage {
            if terminal.identity != Some(holder.identity)
                || lsn != Some(holder.lsn)
                || ws.finalized_tx != Some(holder.visibility_tx)
                || !Arc::ptr_eq(registry, &holder.registry)
                || !registry
                    .lock()
                    .get(&holder.lsn)
                    .is_some_and(|stage| holder.matches(stage))
            {
                return Err(Error::Other(
                    "native discard LSN/transaction/generation publication mismatch".into()));
            }
        } else if lsn.is_some_and(|lsn| registry.lock().contains_key(&lsn)) {
            return Err(Error::Other(
                "native discard stage has no scoped owner".into()));
        }
        Ok(())
    })
}

pub(crate) fn publish_original_discard_for_apply(ws: &WriteSet) {
    ORIGINAL_DISCARD_TERMINAL.with(|slot| {
        let ptr = slot.get();
        if ptr.is_null() {
            return;
        }
        // SAFETY: scoped synchronous apply has exclusive access to this slot.
        let terminal = unsafe { &mut *ptr };
        if let Some(mut holder) = terminal.stage.take() {
            assert_eq!(ws.commit_lsn, Some(holder.lsn));
            assert_eq!(ws.finalized_tx, Some(holder.visibility_tx));
            assert_eq!(terminal.identity, Some(holder.identity));
            holder.publish();
            drop(holder);
        }
    });
}

fn original_discard_box<F: FnOnce() + Send + 'static>(
    original: &crate::memory_accounting::OwnedMemoryReservation,
    publication: F,
) -> Result<(
    Box<dyn FnOnce() + Send>,
    crate::memory_accounting::OwnedMemoryReservation,
)> {
    // F is a stack value; its concrete physical Box layout is known without
    // allocating. The caller's separate preparation owner funds its captures.
    let memory = crate::memory_accounting::reserve_preparation_from_original(
        original,
        std::mem::size_of::<F>(),
        "discard",
        "publication_control",
        "Raise MEMORY_LIMIT to prepare discard publication.",
    )?;
    Ok((Box::new(publication), memory))
}

pub(super) fn bind_original_discard_identity(
    identity: contextdb_tx::manager::EngineTerminalIdentity,
) -> Result<()> {
    ORIGINAL_DISCARD_TERMINAL.with(|slot| {
        let ptr = slot.get();
        if ptr.is_null() {
            return Err(Error::Other(
                "native discard preparation is outside its terminal scope".into()));
        }
        // SAFETY: this callback runs synchronously in the existing host gate.
        let terminal = unsafe { &mut *ptr };
        if terminal.identity.is_some_and(|prior| prior != identity) {
            return Err(Error::Other(
                "native discard original transaction/generation changed".into()));
        }
        terminal.identity = Some(identity);
        Ok(())
    })
}

pub(super) fn install_original_discard_stage(holder: OriginalDiscardStageHolder) -> Result<()> {
    ORIGINAL_DISCARD_TERMINAL.with(|slot| {
        let ptr = slot.get();
        if ptr.is_null() {
            return Err(Error::Other(
                "native discard registration is outside its terminal scope".into()));
        }
        // SAFETY: no preparation reference to the terminal is exported; apply
        // receives it only after the existing prepare callback has returned.
        let terminal = unsafe { &mut *ptr };
        terminal.install(holder)
    })
}


impl Database {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn prepare_original_discard_commit(&self, host: &EngineDatabaseTransactions,
        original: &OwnedMemoryReservation, identity: contextdb_tx::manager::EngineTerminalIdentity,
        held: &mut contextdb_tx::manager::EngineGuard<'_>, selections: &[AuthoritativePurgeSelection],
        drain: event_bus::AuthoritativePurgeQueueDrainGuard, guard: &crate::blob_repository::BlobExclusiveSelectorGuard,
        read_publication: &AuthoritativePurgePublicationGuard, append: &mut event_bus::OwnedPreparedSinkEvents,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>) -> Result<OriginalDiscardStageHolder> {
        let mut holder: Option<OriginalDiscardStageHolder> = None;
        let mut preparation_slot: Option<OwnedMemoryReservation> = None;
        self.validate_owned_event_host(&host.events, host)?;
        let frame = crate::persistence::OriginalPersistenceFrame::new(original)?;
        let lsn = (*held.read().commit_lsn).ok_or_else(EngineMetadataJournal::identity_error)?;
        let visibility_tx = (*held.read().finalized_tx).ok_or_else(EngineMetadataJournal::identity_error)?;
        let tables = selections.iter().enumerate().filter(|(position, selection)|
            !selections[..*position].iter().any(|prior| prior.table == selection.table)).count();
        frame.tree::<String, HashSet<RowId>>(tables)?;
        frame.array::<(String, RowId)>(selections.len())?;
        let mut selected = BTreeMap::<String, HashSet<RowId>>::new();
        let mut row_keys = Vec::with_capacity(selections.len());
        for selection in selections {
            frame.reserve(selection.table.len().saturating_mul(2))?;
            if !selected.contains_key(&selection.table) {
                let count = selections.iter().filter(|item| item.table == selection.table).count();
                frame.map::<RowId, ()>(count)?;
                selected.insert(selection.table.clone(), HashSet::with_capacity(count));
            }
            if !selected.get_mut(&selection.table).expect("prepared discard table").insert(selection.local_row_id) {
                return Err(Error::SyncError("erasure selected one pinned row twice".into()));
            }
            row_keys.push((selection.table.clone(), selection.local_row_id));
        }
        let mut quote_owner = contextdb_vector::VectorCopyAdmission::new(
            crate::memory_accounting::persistence_original_projection(original)?.vector_budget()?);
        let (vector_bytes, disk_bytes) = self.vector_store.purge_workspace_bounds(&row_keys, &mut quote_owner)?;
        if disk_bytes != 0 { self.maintenance_context().admit_generation_disk(disk_bytes)?; }
        // The image and its budget/control owners remain inside this complete
        // stage scope. Published vector states retain their own existing paid
        // target owners; no reader credit is converted into an original.
        let image = crate::memory_accounting::image_memory_for_original_work(&frame, "vector",
            "prepare_authoritative_vector_purge",
            "Raise MEMORY_LIMIT or reduce the selected purge set before preparing its replacement vector image.")?;
        image.reserve(vector_bytes.max(1))?;
        let mut workspace = image.vector_copy(vector_bytes.max(1))?;
        let durable_names = self.authoritative_purge_durable_sink_names()?;
        let queue = OwnedDiscardQueueCustody::prepare_for_commit_locked_discard(&host.events, append, drain, &durable_names, &selected, self.persistence.as_deref())?;
        // The same atomic discard transaction removes the selected actions
        // and records the actual surviving unconfirmed markers. Historical
        // uncertainty is cumulative and is deliberately left unchanged.
        held.with_tx_checkpoint(|held| queue.visit_unconfirmed(|sink, count| {
            let key = crate::persistence::OwnedEventKey::concatenated(original,
                event_bus::UNCONFIRMED_COUNT_CONFIG_PREFIX, sink)?;
            let encoded = RedbPersistence::encode_owned_event(original, &count)?;
            frame.reserve(key.value.len().checked_add(encoded.bytes.len())
                .ok_or_else(EngineMetadataJournal::identity_error)?)?;
            let value = [(crate::memory_accounting::preparation_copy_string(&key.value)?,
                encoded.bytes.clone())];
            let copied = held.copy_aux(contextdb_tx::staged_source::AuxBorrow::ConfigWrites(&value))?;
            let at = held.read().config_writes.len();
            let append = held.prepare_aux_splice(at, &[], copied)?;
            held.publish_tx(append);
            Ok(())
        }))?;
        let names = queue.with_sink_names(|names| -> Result<BTreeSet<String>> {
            frame.tree::<String, ()>(names.len())?;
            for name in names { frame.reserve(name.len())?; }
            Ok(names.iter().cloned().collect())
        })?;
        let persistence = self.persistence.as_ref().ok_or_else(EngineMetadataJournal::identity_error)?;
        let published = self.tx_mgr.capture_published_read();
        let root = writer_read::durable_root(published.handle)?.ok_or_else(EngineMetadataJournal::identity_error)?;
        let mut history = crate::persistence::discard_history::OriginalDiscardHistory::begin(persistence, original, &root)?;
        history.retain_original_owner(&host.events)?;
        let prepared = persistence.with_owned_encoded_event_source(read_memory.clone(), |source|
            self.prepare_original_discard_non_event_set_batch(selections, &history, &frame, source, &names,
                crate::custody::erasure::PurgeReportShape::None, lsn, &mut workspace.owner, read_memory.clone()))?;
        let selected_hashes=self.capture_original_discard_blob_hashes(host,selections,read_memory.clone())?;
        if !selected_hashes.is_subset_of(guard.selector())? {
            return Err(Error::Other("discard blob union changed after commit preflight".into()));
        }
        let survivors=self.original_discard_current_blob_references(host,original,selections,&selected_hashes,read_memory.clone(),held)?;
        let blob_purge=self.blob_repository.prepare_original_authoritative_purge(
            original,&history,guard,&selected_hashes,&survivors,lsn)?;
        frame.array::<RowRef>(selections.len())?;
        let mut references = Vec::with_capacity(selections.len());
        if references.capacity() != selections.len() { return Err(EngineMetadataJournal::identity_error()); }
        for selection in selections {
            frame.reserve(selection.table.len().checked_add(discard_key_copy_bytes(&selection.natural_key)?)
                .ok_or_else(EngineMetadataJournal::identity_error)?)?;
            references.push(RowRef { table: crate::memory_accounting::preparation_copy_string(&selection.table)?,
                key: discard_copy_key(&selection.natural_key)? });
        }
        let selected_keys = crate::custody::erasure::metadata_keys(self, &references, read_memory.clone())?;
        let owner_keys = prepared.lineage_config_owners.iter().try_fold(0usize, |n, owner|
            n.checked_add(3 + usize::from(owner.lifecycle_record_is_selected_life))
                .ok_or_else(EngineMetadataJournal::identity_error))?;
        let key_count = selected_keys.len().checked_add(owner_keys)
            .and_then(|n| n.checked_add(prepared.graph_arrival_config_keys.len()))
            .ok_or_else(EngineMetadataJournal::identity_error)?;
        frame.array::<String>(key_count)?;
        let mut keys = Vec::with_capacity(key_count);
        if keys.capacity() != key_count { return Err(EngineMetadataJournal::identity_error()); }
        for key in &selected_keys { keys.push(frame.copy_text(key)?); }
        for owner in &prepared.lineage_config_owners {
            for key in [&owner.row_sidecar_key, &owner.creation_lineage_key, &owner.accepted_author_key] {
                keys.push(frame.copy_text(key)?);
            }
            if owner.lifecycle_record_is_selected_life { keys.push(frame.copy_text(&owner.lifecycle_record_key)?); }
        }
        for key in &prepared.graph_arrival_config_keys { keys.push(frame.copy_text(key)?); }
        drop(selected_keys);
        drop(references);
        held.with_tx_checkpoint(|held| {
            for position in (0..held.read().config_writes.len()).rev() {
                if keys.contains(&held.read().config_writes[position].0)
                    || history.contains_config(original, &held.read().config_writes[position].0)? {
                    let empty = held.copy_aux(contextdb_tx::staged_source::AuxBorrow::ConfigWrites(&[]))?;
                    let remove = held.prepare_aux_splice(position, &[position], empty)?; held.publish_tx(remove);
                }
            }
            let copied = held.copy_aux(contextdb_tx::staged_source::AuxBorrow::ConfigDeletes(&keys))?;
            let at = held.read().config_deletes.len();
            let append = held.prepare_aux_splice(at, &[], copied)?; held.publish_tx(append);
            Ok(())
        })?;
        frame.array::<(String, u64, [u8; 32])>(prepared.durable_sink_entries.len())?;
        let durable = AuthoritativePurgePersistenceProjection {
            row_graph_entries: Vec::new(),
            row_graph_credit: contextdb_core::read_memory::ReadCredit::new(
                crate::memory_accounting::persistence_original_projection(original)?.read_budget()?),
            row_versions: Vec::new(),
            source_provenance: prepared.disk_source_provenance, vectors: prepared.vector_entries,
            vector_partition_identities: prepared.vector_partition_identities, vector_generation_candidates: prepared.vector_generation_candidates,
            graph_entries: Vec::new(),
            sink_entries: prepared.durable_sink_entries.into_iter().map(|entry| Ok((entry.sink, entry.queue_id,
                entry.durable_digest.ok_or_else(crate::custody::canonical::invalid)?))).collect::<Result<_>>()?,
            change_log_entries: Vec::new(), config_keys_removed: keys, lifecycle_records: Vec::new(),
            purge_delivery_items: Vec::new(), blob_purge,
        };
        frame.array::<(String, String, Option<String>)>(prepared.lineage_config_owners.len())?;
        let mut lineage_keys: Vec<(String, String, Option<String>)> = Vec::with_capacity(prepared.lineage_config_owners.len());
        if lineage_keys.capacity() != prepared.lineage_config_owners.len() { return Err(EngineMetadataJournal::identity_error()); }
        for owner in &prepared.lineage_config_owners {
            lineage_keys.push((frame.copy_text(&owner.row_sidecar_key)?, frame.copy_text(&owner.creation_lineage_key)?,
                if owner.lifecycle_record_is_selected_life { Some(frame.copy_text(&owner.lifecycle_record_key)?) } else { None }));
        }
        let pending = {
            let state = self.lineage_state_lock.lock();
            if self.snapshot_registry.pending_delete_source.lock().is_some() {
                Some(self.prepare_resident_pending_changes(&state, lineage_keys.iter()
                    .filter_map(|(_, _, key)| key.as_deref().map(|key| (key, None))))?)
            } else { None }
        };
        frame.tree::<String, ()>(tables)?;
        let mut erased_tables = BTreeSet::new();
        for selection in selections {
            if !erased_tables.contains(&selection.table) { erased_tables.insert(frame.copy_text(&selection.table)?); }
        }
        let writer_erasure = Some(persistence.prepare_original_discard_writer_erasure(original, &erased_tables, &history)?);
        history.seal(original)?;
        let publication = prepared.publication_replacements;
        // Control owners are declared before the eventual local map/body, so
        // every error destroys that body before any control invoice settles.
        let registry_memory = crate::memory_accounting::reserve_preparation_from_original(original,
            crate::memory_accounting::preparation_native_map_bytes::<Lsn, Stage>(1)?,
            "discard", "stage_registry", "Raise MEMORY_LIMIT to register discard publication.")?;
        let mut next_registry = HashMap::with_capacity(1);
        let read_gate = read_publication.gate.clone();
        let relational_store = self.relational_store.clone();
        let graph_store = self.graph_store.clone(); let vector_store = self.vector_store.clone();
        let change_log = self.change_log.clone(); let change_index = self.change_log_table_index.clone();
        let change_counts = self.change_log_lsn_refcounts.clone();
        let authors = self.accepted_sync_row_authors.clone(); let arrivals = self.sync_graph_arrivals.clone();
        let lineage = self.lineage_state_lock.clone(); let custody = self.custody_cache.clone();
        let vector_accountant = crate::memory_accounting::persistence_original_projection(original)?.vector_budget()?;
        preparation_slot = Some(frame.into_reservation());
        let memory = PreparedMemorySwap::from_original_preparation(preparation_slot.as_mut().expect("held original preparation"),
            publication.memory_swap.old_bytes, publication.memory_swap.new_bytes,
            publication.memory_swap.retired_bytes_released_by_store)?;
        let replacement = OriginalDiscardReplacement { publication, memory };
        let closure = move || {
            let mut replacement = replacement;
            if let Some(erasure) = writer_erasure { erasure.publish(); }
            read_gate.invalidate_cursors();
            relational_store.publish_prepared_received_schema(replacement.publication.relational);
            *change_log.write() = replacement.publication.change_log;
            *change_index.write() = replacement.publication.change_log_table_index;
            *change_counts.write() = replacement.publication.change_log_lsn_refcounts;
            graph_store.publish_prepared_received_schema(replacement.publication.graph);
            vector_store.publish_prepared_received_schema(replacement.publication.vector, vector_accountant);
            queue.publish();
            { let mut authors = authors.write(); for key in replacement.publication.accepted_author_memory_keys_removed { authors.remove(&key); } }
            { let mut arrivals = arrivals.write(); for (key, selected) in replacement.publication.graph_arrival_memory_removed {
                if arrivals.get(&key).map(|entry| &**entry) == Some(&selected) { arrivals.remove(&key); }
            } }
            { let mut state = lineage.lock(); for (sidecar, creation, lifecycle) in lineage_keys {
                state.row_sidecars.remove(&sidecar); state.unbound_creations.remove(&creation);
                if let Some(key) = lifecycle { state.records.remove(&key); }
            } }
            if let Some(pending) = pending { pending.publish(); }
            *custody.lock() = None;
            replacement.memory.commit_after_swap();
        };
        let boxed = original_discard_box(original, closure)?;
        let address = (&*boxed.0 as *const (dyn FnOnce() + Send) as *const ()) as usize;
        holder = Some(OriginalDiscardStageHolder { registry: self.local_erasure_stages.clone(), lsn,
            identity, visibility_tx, publication: address, history: Some(history), preparation: preparation_slot.take(), closure: Some(boxed.1),
            registry_memory: Some(registry_memory) });
        next_registry.insert(lsn, Stage { durable, publish: boxed.0, _vector_workspace: workspace });
        {
            let mut registry = self.local_erasure_stages.lock();
            if !registry.is_empty() || registry.capacity() != 0 { return Err(EngineMetadataJournal::identity_error()); }
            std::mem::swap(&mut *registry, &mut next_registry);
        }
        drop(next_registry);
        Ok(holder.take().expect("registered original discard holder"))
    }
}

struct OriginalDiscardReplacement {
    publication: OriginalPurgeNonEventPublication,
    memory: PreparedMemorySwap,
}

fn discard_key_copy_bytes(key: &NaturalKey) -> Result<usize> {
    use crate::memory_accounting::preparation_value_copy_bytes;
    let mut bytes = key.column.len().checked_add(preparation_value_copy_bytes(&key.value)?)
        .and_then(|n| n.checked_add(std::alloc::Layout::array::<(String, Value)>(key.rest.len()).ok()?.size()))
        .ok_or_else(EngineMetadataJournal::identity_error)?;
    for (name, value) in &key.rest {
        bytes = bytes.checked_add(name.len()).and_then(|n| n.checked_add(preparation_value_copy_bytes(value).ok()?))
            .ok_or_else(EngineMetadataJournal::identity_error)?;
    }
    Ok(bytes)
}
fn discard_copy_key(key: &NaturalKey) -> Result<NaturalKey> {
    use crate::memory_accounting::{preparation_copy_string, preparation_copy_value};
    let mut rest = Vec::with_capacity(key.rest.len());
    if rest.capacity() != key.rest.len() { return Err(EngineMetadataJournal::identity_error()); }
    for (name, value) in &key.rest { rest.push((preparation_copy_string(name)?, preparation_copy_value(value))); }
    Ok(NaturalKey { column: preparation_copy_string(&key.column)?, value: preparation_copy_value(&key.value), rest })
}
fn discard_reference_copy_bytes(reference: &RowRef) -> Result<usize> {
    reference.table.len().checked_add(discard_key_copy_bytes(&reference.key)?)
        .ok_or_else(EngineMetadataJournal::identity_error)
}
fn discard_copy_reference(reference: &RowRef) -> Result<RowRef> {
    Ok(RowRef { table: crate::memory_accounting::preparation_copy_string(&reference.table)?, key: discard_copy_key(&reference.key)? })
}
pub(super) fn discard_selection_copy_bytes(selection: &AuthoritativePurgeSelection) -> Result<usize> {
    selection.table.len().checked_add(selection.lineage_root.len())
        .and_then(|n| n.checked_add(discard_key_copy_bytes(&selection.natural_key).ok()?))
        .ok_or_else(EngineMetadataJournal::identity_error)
}
fn discard_copy_selection(selection: &AuthoritativePurgeSelection) -> Result<AuthoritativePurgeSelection> {
    Ok(AuthoritativePurgeSelection {
        table: crate::memory_accounting::preparation_copy_string(&selection.table)?,
        table_generation: selection.table_generation,
        natural_key: discard_copy_key(&selection.natural_key)?,
        lineage_root: crate::memory_accounting::preparation_copy_string(&selection.lineage_root)?,
        local_row_id: selection.local_row_id,
    })
}

pub(super) struct OriginalDiscardSelections {
    pub(super) values: Vec<AuthoritativePurgeSelection>,
    _memory: OwnedMemoryReservation,
}

fn visit_discard_blob_hashes(value: &Value, mut visit: impl FnMut([u8; 32]) -> Result<()>) -> Result<()> {
    let Value::Json(value) = value else {
        return Err(Error::Other("work ledger: input_refs must be JSON".into()));
    };
    let items = value.as_array().ok_or_else(|| Error::Other("work ledger: input_refs must be a JSON array".into()))?;
    // Match the canonical decoder's two phases: validate every kind before
    // inspecting blob details. Both phases borrow the existing row window.
    for item in items {
        if item.get("kind").and_then(|kind| kind.as_str()).is_none() {
            return Err(Error::Other("work ledger: every input ref needs a string kind".into()));
        }
    }
    for item in items {
        if item.get("kind").and_then(|kind| kind.as_str()) != Some(crate::work_ledger::REF_KIND_BLOB_REF) { continue; }
        let hash = item.get("detail").and_then(|detail| detail.get("hash"))
            .and_then(|hash| hash.as_str()).ok_or_else(|| Error::Other("work ledger: blob_ref input needs a string hash".into()))?;
        visit(crate::work_ledger::BlobHash::from_hex(hash)?.as_bytes())?;
    }
    Ok(())
}

enum OriginalDiscardRowVisit { Stop, Error(Error) }
impl From<Error> for OriginalDiscardRowVisit {
    fn from(error: Error) -> Self { Self::Error(error) }
}

pub(super) fn discard_key_pairs_copy_bytes(key: &NaturalKey) -> Result<usize> {
    discard_key_copy_bytes(key)?.checked_add(std::mem::size_of::<(String, Value)>())
        .ok_or_else(EngineMetadataJournal::identity_error)
}
pub(super) fn discard_copy_key_pairs(key: &NaturalKey) -> Result<Vec<(String, Value)>> {
    let count = key.rest.len().checked_add(1).ok_or_else(EngineMetadataJournal::identity_error)?;
    let mut pairs = Vec::with_capacity(count);
    if pairs.capacity() != count { return Err(EngineMetadataJournal::identity_error()); }
    pairs.push((crate::memory_accounting::preparation_copy_string(&key.column)?,
        crate::memory_accounting::preparation_copy_value(&key.value)));
    for (name, value) in &key.rest {
        pairs.push((crate::memory_accounting::preparation_copy_string(name)?,
            crate::memory_accounting::preparation_copy_value(value)));
    }
    Ok(pairs)
}

// Only the exact scoped terminal may move its already prepared write into
// persistence. A missing/foreign Original, store, LSN or visibility identity
// refuses before any public write is staged.
pub(crate) fn take_original_discard_write(
    store: &RedbPersistence, original: &OwnedMemoryReservation, ws: &WriteSet,
) -> Result<Option<crate::persistence::received_working::ReceivedWorkingWrite>> {
    ORIGINAL_DISCARD_TERMINAL.with(|slot| {
        let ptr = slot.get();
        if ptr.is_null() { return Ok(None); }
        // SAFETY: synchronous terminal scope owns this holder exclusively.
        let terminal = unsafe { &mut *ptr };
        let Some(holder) = terminal.stage.as_mut() else { return Ok(None); };
        if terminal.identity != Some(holder.identity) || ws.commit_lsn != Some(holder.lsn)
            || ws.finalized_tx != Some(holder.visibility_tx) { return Err(EngineMetadataJournal::identity_error()); }
        let history = holder.history.take().ok_or_else(EngineMetadataJournal::identity_error)?;
        history.into_write(store, original).map(Some)
    })
}
