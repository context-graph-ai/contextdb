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
        let registrations = self
            .pending_commit_metadata
            .lock()
            .get(&tx)
            .map(|m| m.delivery_registrations.clone())
            .unwrap_or_default();
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
                for registration in &registrations {
                    if registration.root == reference || registration.members.contains(&reference) {
                        pending.insert(registration.root.bytes());
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
                    && !registrations.iter().any(|r| r.root == reference)
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
            lifecycle_records: Vec::new(),
            purge_delivery_items: Vec::new(),
            blob_purge,
        };
        let mut lineage = self.lineage_state_lock.lock().clone();
        for owner in &prepared.lineage_config_owners {
            lineage.row_sidecars.remove(&owner.row_sidecar_key);
            lineage
                .unbound_creations
                .remove(&owner.creation_lineage_key);
            if owner.lifecycle_record_is_selected_life {
                lineage.records.remove(&owner.lifecycle_record_key);
            }
        }
        let erased_tables = selections
            .iter()
            .map(|selection| selection.table.clone())
            .collect::<BTreeSet<_>>();
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
                    *accepted_sync_row_authors.write() = publication.accepted_author_memory_mirror;
                    *sync_graph_arrivals.write() = publication.graph_arrival_memory_mirror;
                    *lineage_state_lock.lock() = lineage;
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
        )?;
        Ok(Self {
            events,
            publication,
        })
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
                "native discard terminal identity changed".into(),
            ));
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
                    "native discard requires its exact scoped terminal holder".into(),
                ));
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
                    "native discard LSN/transaction/generation publication mismatch".into(),
                ));
            }
        } else if lsn.is_some_and(|lsn| registry.lock().contains_key(&lsn)) {
            return Err(Error::Other(
                "native discard stage has no scoped owner".into(),
            ));
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
                "native discard preparation is outside its terminal scope".into(),
            ));
        }
        // SAFETY: this callback runs synchronously in the existing host gate.
        let terminal = unsafe { &mut *ptr };
        if terminal.identity.is_some_and(|prior| prior != identity) {
            return Err(Error::Other(
                "native discard original transaction/generation changed".into(),
            ));
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
                "native discard registration is outside its terminal scope".into(),
            ));
        }
        // SAFETY: no preparation reference to the terminal is exported; apply
        // receives it only after the existing prepare callback has returned.
        let terminal = unsafe { &mut *ptr };
        terminal.install(holder)
    })
}

impl Database {
    pub(super) fn prepare_discard_commit_owned(
        &self,
        host: &EngineDatabaseTransactions,
        events: &event_bus::OwnedEventBusHandle,
        terminal_guard: &mut contextdb_tx::manager::EngineGuard<'_>,
        identity: contextdb_tx::manager::EngineTerminalIdentity,
        own_append: &mut event_bus::OwnedPreparedSinkEvents,
        vector_workspace: crate::read_image_memory::ImageVectorCopy,
        selections: &[AuthoritativePurgeSelection],
        drain: event_bus::AuthoritativePurgeQueueDrainGuard,
        guard: &crate::blob_repository::BlobExclusiveHashSetGuard,
        read_publication: &AuthoritativePurgePublicationGuard,
        _vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<OriginalDiscardStageHolder> {
        events.with_owned_persistence_original(|original| {
            crate::memory_accounting::validate_owned_original(
                original,
                &self.finalization.accountant,
                &host.funder,
            )?;
            let frame = crate::persistence::OriginalPersistenceFrame::new(original)?;
            let (lsn, visibility_tx) = terminal_guard.with_legacy(|ws| {
                Ok::<_, Error>((
                    ws.commit_lsn
                        .ok_or_else(crate::custody::canonical::invalid)?,
                    ws.finalized_tx
                        .ok_or_else(crate::custody::canonical::invalid)?,
                ))
            })?;
            let mut selected = BTreeMap::<String, HashSet<RowId>>::new();
            frame.tree::<String, HashSet<RowId>>(selections.len())?;
            for selection in selections {
                frame.reserve(selection.table.len())?;
                frame.map::<RowId, ()>(selections.len())?;
                selected
                    .entry(selection.table.clone())
                    .or_default()
                    .insert(selection.local_row_id);
            }
            events.suspend_owned_append_for_discard(own_append)?;
            let token = drain.freeze_for_commit_locked_purge();
            let persistence = self.persistence.as_ref().ok_or_else(|| {
                Error::SyncError("authoritative purge requires a file-backed database".into())
            })?;
            persistence.with_owned_encoded_event_source(read_memory.clone(), |source| {
                let mut durable_names = BTreeSet::new();
                source.visit_sinks(|name| {
                    frame.tree::<String, ()>(1)?;
                    durable_names.insert(frame.copy_text(name)?);
                    Ok(())
                })?;
                let queue_publication = events.prepare_owned_purge_queue_replacement(
                    token,
                    &durable_names,
                    &selected,
                    Some(own_append),
                )?;
                let queue_custody = OwnedDiscardQueueCustody {
                    events: events.clone(),
                    publication: queue_publication,
                };
                let mut vector_workspace = vector_workspace;
                let prepared = self.prepare_original_discard_non_event_set_batch(
                    selections,
                    &frame,
                    source,
                    &durable_names,
                    crate::custody::erasure::PurgeReportShape::None,
                    lsn,
                    &mut vector_workspace.owner,
                    read_memory.clone(),
                )?;
                let (selected_hashes, mut survivors) = self
                    .authoritative_purge_current_blob_references(
                        selections,
                        true,
                        read_memory.clone(),
                    )?;
                terminal_guard.with_legacy(|ws| -> Result<()> {
                    for (table, row) in &ws.relational_inserts {
                        if table == "work_jobs"
                            && let Some(refs) = row.values.get("input_refs")
                        {
                            for hash in
                                crate::work_ledger::canonical_blob_hashes_from_input_refs(refs)?
                            {
                                survivors.insert(
                                    crate::work_ledger::BlobHash::from_hex(&hash)?.as_bytes(),
                                );
                            }
                        }
                    }
                    Ok(())
                })?;
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
                let (positions, at) = terminal_guard.with_legacy(|ws| -> Result<_> {
                    frame.array::<usize>(ws.config_writes.len())?;
                    Ok((
                        ws.config_writes
                            .iter()
                            .enumerate()
                            .filter_map(|(i, (key, _))| keys.contains(key).then_some(i))
                            .collect::<Vec<_>>(),
                        ws.config_deletes.len(),
                    ))
                })?;
                if !positions.is_empty() {
                    let empty =
                        terminal_guard.copy_aux(contextdb_tx::AuxBorrow::ConfigWrites(&[]))?;
                    let removal = terminal_guard.prepare_aux_splice(0, &positions, empty)?;
                    terminal_guard.publish_tx(removal);
                }
                if !keys.is_empty() {
                    let insert =
                        terminal_guard.copy_aux(contextdb_tx::AuxBorrow::ConfigDeletes(&keys))?;
                    let mutation = terminal_guard.prepare_aux_splice(at, &[], insert)?;
                    terminal_guard.publish_tx(mutation);
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
                    lifecycle_records: Vec::new(),
                    purge_delivery_items: Vec::new(),
                    blob_purge,
                };
                let mut lineage = self.lineage_state_lock.lock().clone();
                for owner in &prepared.lineage_config_owners {
                    lineage.row_sidecars.remove(&owner.row_sidecar_key);
                    lineage
                        .unbound_creations
                        .remove(&owner.creation_lineage_key);
                    if owner.lifecycle_record_is_selected_life {
                        lineage.records.remove(&owner.lifecycle_record_key);
                    }
                }
                let erased_tables = selections
                    .iter()
                    .map(|selection| selection.table.clone())
                    .collect::<BTreeSet<_>>();
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
                let mut memory = PreparedMemorySwap::prepare_original(
                    original,
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
                let accepted_sync_row_authors = self.accepted_sync_row_authors.clone();
                let sync_graph_arrivals = self.sync_graph_arrivals.clone();
                let lineage_state_lock = self.lineage_state_lock.clone();
                let custody_cache = self.custody_cache.clone();
                let accountant = self.accountant.vector_budget()?;
                let registry = self.local_erasure_stages.clone();
                if !registry.lock().is_empty() {
                    return Err(Error::Other(
                        "native discard registry already has a terminal publication".into(),
                    ));
                }
                let (mut successor, registry_memory) =
                    crate::memory_accounting::new_owned_native_map::<Lsn, Stage>(original, 1)?;
                let (publish, closure) = original_discard_box(original, move || {
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
                    queue_custody.publish();
                    *accepted_sync_row_authors.write() = publication.accepted_author_memory_mirror;
                    *sync_graph_arrivals.write() = publication.graph_arrival_memory_mirror;
                    *lineage_state_lock.lock() = lineage;
                    *custody_cache.lock() = None;
                    memory.commit_after_swap();
                })?;
                let pointer = (&*publish as *const (dyn FnOnce() + Send) as *const ()) as usize;
                let holder = OriginalDiscardStageHolder {
                    registry: registry.clone(),
                    lsn,
                    identity,
                    visibility_tx,
                    publication: pointer,
                    preparation: Some(frame.into_reservation()),
                    closure: Some(closure),
                    registry_memory: Some(registry_memory),
                };
                successor.insert(
                    lsn,
                    Stage {
                        durable,
                        publish,
                        _vector_workspace: vector_workspace,
                    },
                );
                let retired = {
                    let mut live = registry.lock();
                    if !live.is_empty() {
                        drop(live);
                        drop(successor);
                        return Err(Error::Other(
                            "native discard registry changed before registration".into(),
                        ));
                    }
                    std::mem::replace(&mut *live, successor)
                };
                drop(retired);
                Ok(holder)
            })
        })
    }
}
