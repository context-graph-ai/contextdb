//! Local erasure is staged and committed with ordinary transaction writes.
use super::*;
use crate::custody::records::{Record, RowRef};
use contextdb_core::EdgeDiscardMode;

pub(crate) struct Stage {
    pub(crate) durable: AuthoritativePurgePersistenceProjection,
    pub(crate) _vector_workspace: contextdb_vector::VectorWorkspaceReservation,
    pub(crate) publish: Box<dyn FnOnce() + Send>,
}
pub(crate) type StageRegistry = Arc<Mutex<HashMap<Lsn, Stage>>>;

impl Database {
    pub(crate) fn discard_policy(&self, table: &str) -> Result<Option<(String, EdgeDiscardMode)>> {
        // Stopping the server does not remove durable hub authority.
        if self.sync_relay_mode_enabled()
            || self.custody_authority()?.iter().any(|record| match record {
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
        let records = self.custody_authority()?;
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
    ) -> Result<QueryResult> {
        let authority = self.custody_authority()?;
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
            let policy = self.discard_policy(table)?;
            let mut pending = BTreeSet::new();
            for (key, row_id) in rows {
                let reference = RowRef {
                    table: table.clone(),
                    key: key.clone(),
                };
                let records = self.custody_rows(std::slice::from_ref(&reference))?;
                for record in &records {
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
                    .find_row_by_id_at(table, *row_id, self.snapshot_for_read())
                    .ok()
                    .and_then(|row| {
                        self.table_meta(table)
                            .and_then(|meta| natural_key_from_row_values(&meta, &row.values))
                    });
                if let Some(key) = committed_key {
                    selections.push(self.resolve_authoritative_purge_selection(table, &key)?);
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
        self.tx_mgr.with_write_set(tx, |ws| {
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
        })?;
        self.release_insert_allocations(&removed);
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
    ) -> Result<()> {
        let lsn = ws
            .commit_lsn
            .ok_or_else(crate::custody::canonical::invalid)?;
        // Selected immutable row identities are revalidated under the ordinary commit mutex.
        let vector_workspace = self.reserve_authoritative_erasure_workspace(selections)?;
        let prepared = self.prepare_authoritative_purge_set_batch(
            selections,
            token,
            crate::custody::erasure::PurgeReportShape::None,
            lsn,
        )?;
        let (selected_hashes, mut survivors) =
            self.authoritative_purge_current_blob_references(selections, true)?;
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
                        e.durable_bytes
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
        let accountant = self.accountant.clone();
        self.local_erasure_stages.lock().insert(
            lsn,
            Stage {
                durable,
                _vector_workspace: vector_workspace,
                publish: Box::new(move || {
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
        Ok(())
    }
}
