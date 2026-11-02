use super::*;

impl Database {
    /// Resolve exactly one currently visible row life for the private
    /// authoritative-purge kernel.  This only reads durable creation/lineage
    /// evidence; it does not reserve, mutate, or publish anything.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn resolve_authoritative_purge_selection(
        &self,
        table: &str,
        natural_key: &NaturalKey,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<AuthoritativePurgeSelection> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        #[cfg(test)]
        let _purge_enclosing_producer = crate::persistence::content_identity::proof_allocation_oracle_tests::PurgeProducerGuard::site(
            "Database::resolve_authoritative_purge_selection");
        let _operation = self.open_operation()?;
        let table_generation = self.durable_lineage_table_generation(table, read_memory.clone())?;
        let snapshot = self.snapshot_for_read();
        let local_row_id = self
            .visible_row_by_natural_key(
                table,
                natural_key,
                snapshot,
                &HashSet::new(),
                read_memory.clone(),
            )?
            .map(|row| row.row_id)
            .ok_or_else(|| {
                Error::SyncError((format!("cannot purge missing {table} row")).into())
            })?;
        let row_sidecar_key =
            Self::durable_row_lineage_config_key(table, natural_key, table_generation);
        let creation_lineage_key = Self::durable_unbound_creation_config_key(
            table,
            natural_key,
            table_generation,
            local_row_id,
        );
        let lifecycle_record_key =
            Self::durable_lineage_config_key(table, natural_key, table_generation);
        let lineage_state = self.lineage_state_lock.lock();
        let lineage_root = if let Some(sidecar) = self
            .load_row_lineage_sidecar(&lineage_state, &row_sidecar_key)?
            .filter(|sidecar| {
                sidecar.table_generation == table_generation
                    && sidecar.local_row_id == Some(local_row_id)
            }) {
            sidecar.lineage_root
        } else if let Some((_, creation_lsn)) = self
            .load_unbound_creation_lineage(&lineage_state, &creation_lineage_key)?
            .filter(|evidence| {
                evidence.table == table
                    && evidence.natural_key == *natural_key
                    && evidence.table_generation == table_generation
                    && evidence.local_row_id == local_row_id
            })
            .and_then(|evidence| evidence.creation_lsn.map(|lsn| (evidence, lsn)))
        {
            format!(
                "unbound:{}:{}",
                Self::hex_component(&sync_identity_key(table, natural_key)),
                creation_lsn.0
            )
        } else if let Some(record) = self
            .load_lineage_record(&lineage_state, &lifecycle_record_key)?
            .filter(|record| {
                record.table == table
                    && record.natural_key == *natural_key
                    && record.table_generation == table_generation
                    && record.local_row_id == Some(local_row_id)
                    // A purged life is never the creation witness of a live row.
                    && record.delete_obligation != DurableDeleteObligation::Purged
            })
        {
            record.lineage_root
        } else {
            return Err(Error::SyncError(
                (format!("cannot purge {table} row without exact creation lineage evidence"))
                    .into(),
            ));
        };
        Ok(AuthoritativePurgeSelection {
            table: table.to_string(),
            table_generation,
            natural_key: natural_key.clone(),
            lineage_root,
            local_row_id,
            retained: false,
        })
    }

    /// Re-check the selected life immediately before inventorying owned
    /// copies.  A natural key is not enough: a delete/reinsert race may have
    /// created another life at the same key, so every durable witness must
    /// still name the pinned local RowId and the same lineage root.
    ///
    /// The key's lifecycle record may instead describe an earlier life: one
    /// deleted or purged before the pinned row was written. Such a record
    /// names another lineage root and another row, is not evidence about the
    /// selected life, and is returned as `lifecycle: None`. The pinned row is
    /// still visible, so no committed delete or purge can describe it; a
    /// record naming its row or its root must agree with it exactly.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn validate_authoritative_purge_selection(
        &self,
        selection: &AuthoritativePurgeSelection,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<AuthoritativePurgeLineageWitness> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let snapshot = self.snapshot_for_read();
        let retired_generation = self.table_meta(&selection.table).is_none()
            || self.durable_lineage_table_generation(&selection.table, read_memory.clone())?
                != selection.table_generation;
        if !retired_generation
            && !selection.retained
            && self
                .visible_row_by_natural_key(
                    &selection.table,
                    &selection.natural_key,
                    snapshot,
                    &HashSet::new(),
                    read_memory.clone(),
                )?
                .map(|row| row.row_id)
                != Some(selection.local_row_id)
        {
            return Err(Error::SyncError(
                ("authoritative purge selection no longer names its pinned local row".to_string())
                    .into(),
            ));
        }

        let row_sidecar_key = Self::durable_row_lineage_config_key(
            &selection.table,
            &selection.natural_key,
            selection.table_generation,
        );
        let creation_lineage_key = Self::durable_unbound_creation_config_key(
            &selection.table,
            &selection.natural_key,
            selection.table_generation,
            selection.local_row_id,
        );
        let lifecycle_record_key = Self::durable_lineage_config_key(
            &selection.table,
            &selection.natural_key,
            selection.table_generation,
        );
        let state = self.lineage_state_lock.lock();
        let sidecar = self
            .load_row_lineage_sidecar(&state, &row_sidecar_key)?
            // A retained life shares its key with a later life; that life's
            // sidecar is not evidence about this one.
            .filter(|sidecar| {
                !selection.retained || sidecar.local_row_id == Some(selection.local_row_id)
            });
        let creation = self.load_unbound_creation_lineage(&state, &creation_lineage_key)?;
        let lifecycle = self
            .load_lineage_record(&state, &lifecycle_record_key)?
            .filter(|record| {
                record.lineage_root == selection.lineage_root
                    || record.local_row_id == Some(selection.local_row_id)
            });

        if sidecar.is_none() && creation.is_none() && lifecycle.is_none() {
            return Err(Error::SyncError(
                ("authoritative purge selection lost all exact lineage evidence".to_string())
                    .into(),
            ));
        }
        if let Some(sidecar) = &sidecar
            && (sidecar.table_generation != selection.table_generation
                || sidecar.local_row_id != Some(selection.local_row_id)
                || sidecar.lineage_root != selection.lineage_root)
        {
            return Err(Error::SyncError(
                ("authoritative purge sidecar no longer proves the selected lineage root"
                    .to_string())
                .into(),
            ));
        }
        if let Some(creation) = &creation {
            if creation.table != selection.table
                || creation.natural_key != selection.natural_key
                || creation.table_generation != selection.table_generation
                || creation.local_row_id != selection.local_row_id
            {
                return Err(Error::SyncError(
                    ("authoritative purge creation evidence no longer names the pinned row"
                        .to_string())
                    .into(),
                ));
            }
            if selection.lineage_root.starts_with("unbound:") {
                let Some(creation_lsn) = creation.creation_lsn else {
                    return Err(Error::SyncError(
                        ("authoritative purge unbound root has no committed creation position"
                            .to_string())
                        .into(),
                    ));
                };
                let expected_root = format!(
                    "unbound:{}:{}",
                    Self::hex_component(&sync_identity_key(
                        &selection.table,
                        &selection.natural_key,
                    )),
                    creation_lsn.0
                );
                if expected_root != selection.lineage_root {
                    return Err(Error::SyncError(
                        ("authoritative purge creation evidence names a different lineage root"
                            .to_string())
                        .into(),
                    ));
                }
            }
        }
        if let Some(record) = &lifecycle
            && (record.table != selection.table
                || record.natural_key != selection.natural_key
                || record.table_generation != selection.table_generation
                || record.local_row_id != Some(selection.local_row_id)
                || record.lineage_root != selection.lineage_root)
        {
            return Err(Error::SyncError(
                ("authoritative purge lifecycle record no longer proves the selected lineage root"
                    .to_string())
                .into(),
            ));
        }
        Ok(AuthoritativePurgeLineageWitness {
            sidecar,
            creation,
            lifecycle,
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn authoritative_purge_lifecycle_template(
        selection: &AuthoritativePurgeSelection,
        witness: &AuthoritativePurgeLineageWitness,
    ) -> Result<AuthoritativePurgeLifecycleTemplate> {
        if let Some(sidecar) = &witness.sidecar {
            return Ok(AuthoritativePurgeLifecycleTemplate {
                table: selection.table.clone(),
                natural_key: selection.natural_key.clone(),
                table_generation: sidecar.table_generation,
                local_row_id: sidecar.local_row_id.ok_or_else(|| {
                    Error::SyncError(
                        ("authoritative purge sidecar lacks the pinned local row".to_string())
                            .into(),
                    )
                })?,
                locally_created: sidecar.locally_created,
                lineage_root: sidecar.lineage_root.clone(),
                lineage_attestation: sidecar.lineage_attestation.clone(),
                author_node_id: Some(sidecar.author_node_id.clone()),
                author_database_incarnation: Some(sidecar.author_database_incarnation.to_hex()),
                author_local_mutation_position: sidecar.author_local_mutation_position.0,
            });
        }
        if let Some(record) = &witness.lifecycle {
            return Ok(AuthoritativePurgeLifecycleTemplate {
                table: record.table.clone(),
                natural_key: record.natural_key.clone(),
                table_generation: record.table_generation,
                local_row_id: record.local_row_id.ok_or_else(|| {
                    Error::SyncError(
                        ("authoritative purge lifecycle witness lacks the pinned local row"
                            .to_string())
                        .into(),
                    )
                })?,
                locally_created: record.locally_created,
                lineage_root: record.lineage_root.clone(),
                lineage_attestation: record.lineage_attestation.clone(),
                author_node_id: record.author_node_id.clone(),
                author_database_incarnation: record.author_database_incarnation.clone(),
                author_local_mutation_position: record.author_local_mutation_position,
            });
        }
        let creation = witness.creation.as_ref().ok_or_else(|| {
            Error::SyncError(
                ("authoritative purge has no exact witness for its permanent lifecycle"
                    .to_string())
                .into(),
            )
        })?;
        let creation_lsn = creation.creation_lsn.ok_or_else(|| {
            Error::SyncError(
                ("authoritative purge unbound witness lacks a committed creation position"
                    .to_string())
                .into(),
            )
        })?;
        Ok(AuthoritativePurgeLifecycleTemplate {
            table: creation.table.clone(),
            natural_key: creation.natural_key.clone(),
            table_generation: creation.table_generation,
            local_row_id: creation.local_row_id,
            locally_created: true,
            lineage_root: selection.lineage_root.clone(),
            lineage_attestation: Vec::new(),
            author_node_id: None,
            author_database_incarnation: None,
            author_local_mutation_position: creation_lsn.0,
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn prepare_authoritative_purge_survivor_report(
        report_requested: bool,
        table: &str,
        selected_row_ids: &HashSet<RowId>,
        selected_table_rows: &[VersionedRow],
        surviving_table_rows: &[VersionedRow],
        snapshot: SnapshotId,
    ) -> Result<OwnedQueryResult> {
        if !report_requested || table != "work_jobs" {
            return Ok(OwnedQueryResult::empty());
        }

        for selected_row_id in selected_row_ids {
            let mut rows = selected_table_rows
                .iter()
                .filter(|row| row.row_id == *selected_row_id && row.visible_at(snapshot));
            rows.next().ok_or_else(|| {
                Error::SyncError(
                    ("authoritative purge selected work job is not current-visible".to_string())
                        .into(),
                )
            })?;
            if rows.next().is_some() {
                return Err(Error::SyncError(
                    ("authoritative purge selected work job has multiple visible versions"
                        .to_string())
                    .into(),
                ));
            }
        }

        let mut selected_hashes = BTreeSet::new();
        for row in selected_table_rows
            .iter()
            .filter(|row| selected_row_ids.contains(&row.row_id))
        {
            let input_refs = row.values.get("input_refs").ok_or_else(|| {
                Error::SyncError(
                    ("canonical work_jobs row is missing input_refs".to_string()).into(),
                )
            })?;
            selected_hashes.extend(crate::work_ledger::canonical_blob_hashes_from_input_refs(
                input_refs,
            )?);
        }

        if selected_hashes.is_empty() {
            return Ok(OwnedQueryResult::empty());
        }

        let mut surviving_pairs = BTreeSet::new();
        for row in surviving_table_rows
            .iter()
            .filter(|row| !selected_row_ids.contains(&row.row_id) && row.visible_at(snapshot))
        {
            let job_id = match row.values.get("job_id") {
                Some(Value::Text(job_id)) => job_id,
                _ => {
                    return Err(Error::SyncError(
                        ("canonical work_jobs row is missing its text job_id".to_string()).into(),
                    ));
                }
            };
            let input_refs = row.values.get("input_refs").ok_or_else(|| {
                Error::SyncError(
                    ("canonical work_jobs row is missing input_refs".to_string()).into(),
                )
            })?;
            for hash in crate::work_ledger::canonical_blob_hashes_from_input_refs(input_refs)? {
                if selected_hashes.contains(&hash) {
                    surviving_pairs.insert((hash, job_id.clone()));
                }
            }
        }

        if surviving_pairs.is_empty() {
            return Ok(OwnedQueryResult::empty());
        }
        Ok(OwnedQueryResult::new(QueryResultData {
            columns: vec!["blob_hash".to_string(), "remaining_job_id".to_string()],
            rows: surviving_pairs
                .into_iter()
                .map(|(hash, job_id)| vec![Value::Text(hash), Value::Text(job_id)])
                .collect(),
            rows_affected: 0,
            trace: QueryTrace::scan(),
            cascade: None,
        }))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn authoritative_purge_selected_row_versions(
        &self,
        table: &str,
        selected: &HashSet<RowId>,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Vec<VersionedRow>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let mut rows = Vec::new();
        let mut keys = HashSet::new();
        let mut control = contextdb_core::read_memory::ReadCredit::new(read_memory.clone());
        let mut append = |row: &VersionedRow| -> Result<()> {
            let key = (row.row_id, row.created_tx, row.lsn);
            if keys.contains(&key) {
                return Ok(());
            }
            control.try_grow_for(
                std::mem::size_of::<VersionedRow>()
                    + row.estimated_bytes()
                    + 4 * std::mem::size_of::<(RowId, TxId, Lsn)>(),
                "writer_source",
                "selected_history_erasure",
                "Raise MEMORY_LIMIT to erase selected history.",
            )?;
            rows.reserve_exact(1);
            keys.reserve(1);
            rows.push(row.clone());
            keys.insert(key);
            Ok(())
        };
        if let Some(root) = self.writer_read_root()? {
            for row_id in selected {
                let owner =
                    crate::persistence::writer_source::SourceWindowOwner::Read(read_memory.clone());
                if let Some(mut retired) =
                    root.authored_row_cursor(owner.clone(), table, *row_id)?
                {
                    while let Some(window) = retired.next_authored_row(table)? {
                        append(&window.value)?;
                    }
                }
                if root.borrow_captured_table_meta(table).is_some() {
                    let mut cursor = root.row_cursor(owner, table, *row_id)?;
                    while let Some(window) = cursor.next_row()? {
                        append(&window.value)?;
                    }
                }
            }
        } else {
            for row in selected {
                self.relational_store
                    .visit_authored_row_versions(table, *row, &mut append)?;
            }
            let tables = self.relational_store.tables.read();
            if let Some(current) = tables.get(table) {
                for row in current.iter().filter(|row| selected.contains(&row.row_id)) {
                    append(row)?;
                }
            }
        }
        // The key set is temporary; its full admitted overlap remains paid
        // until after it has physically dropped.
        drop(keys);
        self.retain_writer_output(control);
        Ok(rows)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn authoritative_purge_selected_blob_hashes(
        &self,
        selections: &[AuthoritativePurgeSelection],
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<BTreeSet<[u8; 32]>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if !selections.iter().any(|s| s.table == "work_jobs") {
            return Ok(BTreeSet::new());
        }
        let snapshot = self.snapshot_for_read();
        let ids = selections
            .iter()
            .filter(|selection| selection.table == "work_jobs")
            .map(|selection| selection.local_row_id)
            .collect::<HashSet<_>>();
        let rows =
            self.authoritative_purge_selected_row_versions("work_jobs", &ids, read_memory.clone())?;
        let mut hashes = BTreeSet::new();
        for selected in selections.iter().filter(|s| s.table == "work_jobs") {
            let mut visible = rows
                .iter()
                .filter(|row| row.row_id == selected.local_row_id && row.visible_at(snapshot));
            let row = visible.next().ok_or_else(|| {
                Error::SyncError(
                    ("authoritative purge selected work job is not current-visible".to_string())
                        .into(),
                )
            })?;
            if visible.next().is_some() {
                return Err(Error::SyncError(
                    ("authoritative purge selected work job has multiple visible versions"
                        .to_string())
                    .into(),
                ));
            }
            let _ = row;
            for historical in rows
                .iter()
                .filter(|row| row.row_id == selected.local_row_id)
            {
                let input_refs = historical.values.get("input_refs").ok_or_else(|| {
                    Error::SyncError(
                        ("canonical selected work_jobs history is missing input_refs".to_string())
                            .into(),
                    )
                })?;
                let row_hashes =
                    crate::work_ledger::canonical_blob_hashes_from_input_refs(input_refs).map_err(
                        |err| {
                            Error::SyncError((format!(
                                "authoritative purge selected work-job history has malformed input_refs: {err}"
                            )).into())
                        },
                    )?;
                for hash in row_hashes {
                    hashes.insert(crate::work_ledger::BlobHash::from_hex(&hash)?.as_bytes());
                }
            }
        }
        Ok(hashes)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn authoritative_purge_current_blob_references(
        &self,
        selections: &[AuthoritativePurgeSelection],
        retain_all_on_malformed_survivor: bool,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<AuthoritativePurgeBlobReferences> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let selected_hashes =
            self.authoritative_purge_selected_blob_hashes(selections, read_memory.clone())?;
        if selected_hashes.is_empty() {
            return Ok((selected_hashes, BTreeSet::new()));
        }
        let selected_row_ids = selections
            .iter()
            .filter(|s| s.table == "work_jobs")
            .map(|selection| selection.local_row_id)
            .collect::<HashSet<_>>();
        let snapshot = self.snapshot_for_read();
        let rows = self.committed_rows_at("work_jobs", snapshot, read_memory.clone())?;
        let mut survivor_hashes = BTreeSet::new();
        for row in rows
            .iter()
            .filter(|row| !selected_row_ids.contains(&row.row_id) && row.visible_at(snapshot))
        {
            let Some(input_refs) = row.values.get("input_refs") else {
                if retain_all_on_malformed_survivor {
                    return Ok((selected_hashes.clone(), selected_hashes));
                }
                return Err(Error::SyncError(
                    ("authoritative purge surviving work job is missing input_refs".to_string())
                        .into(),
                ));
            };
            let row_hashes =
                match crate::work_ledger::canonical_blob_hashes_from_input_refs(input_refs) {
                    Ok(hashes) => hashes,
                    Err(err) if retain_all_on_malformed_survivor => {
                        let _ = err;
                        return Ok((selected_hashes.clone(), selected_hashes));
                    }
                    Err(err) => {
                        return Err(Error::SyncError((format!(
                            "authoritative purge surviving work job has malformed input_refs: {err}"
                        )).into()));
                    }
                };
            for hash in row_hashes {
                let hash = crate::work_ledger::BlobHash::from_hex(&hash)?.as_bytes();
                if selected_hashes.contains(&hash) {
                    survivor_hashes.insert(hash);
                }
            }
        }
        Ok((selected_hashes, survivor_hashes))
    }

    /// Reserve the complete transient vector image and replacement-generation
    /// disk envelope before either erasure path faults a selected partition in.
    /// Erasure owns every engine-held copy and completes on the durable owner.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn authoritative_purge_store(&self) -> Result<&Arc<RedbPersistence>> {
        self.persistence.as_ref().ok_or_else(|| {
            Error::SyncError(
                ("authoritative purge requires a file-backed database".to_string()).into(),
            )
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn reserve_authoritative_erasure_workspace(
        &self,
        selections: &[AuthoritativePurgeSelection],
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<crate::read_image_memory::ImageVectorCopy> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();
        self.authoritative_purge_store()?;

        #[cfg(test)]
        let _purge_enclosing_producer = crate::persistence::content_identity::proof_allocation_oracle_tests::PurgeProducerGuard::site(
            "Database::reserve_authoritative_erasure_workspace");
        let selected_rows = selections
            .iter()
            .map(|selection| (selection.table.clone(), selection.local_row_id))
            .collect::<Vec<_>>();
        let mut vector_copy_owner = crate::vector_read_memory::admission(read_memory.clone());
        let (vector_bound, disk_bound) = self
            .vector_store
            .purge_workspace_bounds(&selected_rows, &mut vector_copy_owner)?;
        if disk_bound != 0 {
            self.maintenance_context()?
                .admit_generation_disk(disk_bound)?;
        }
        let rows_bound = self
            .relational_store
            .tables
            .read()
            .iter()
            .filter(|(table, _)| {
                selections
                    .iter()
                    .any(|selection| &selection.table == *table)
            })
            .flat_map(|(_, rows)| rows)
            .fold(0usize, |bytes, row| {
                bytes.saturating_add(row.estimated_bytes())
            });
        let metadata_bound = self
            .relational_store
            .table_meta
            .read()
            .iter()
            .filter(|(table, _)| {
                selections
                    .iter()
                    .any(|selection| &selection.table == *table)
            })
            .fold(0usize, |bytes, (table, meta)| {
                bytes
                    .saturating_add(table.len())
                    .saturating_add(meta.estimated_bytes())
            });
        let edges_bound = self
            .graph_store
            .forward_adj
            .read()
            .values()
            .flatten()
            .fold(0usize, |bytes, edge| {
                bytes.saturating_add(edge.retained_graph_bytes())
            });
        let bytes = vector_bound
            .saturating_add(rows_bound.saturating_mul(6))
            .saturating_add(metadata_bound.saturating_mul(4))
            .saturating_add(edges_bound.saturating_mul(4))
            .max(1);
        // An authoritative purge is writer work: its replacement image is engine
        // write working memory, which the writer budget covers. The caller's
        // declared read memory funds only the reads it asked for.
        drop(read_memory);
        let image = crate::read_image_memory::ImageMemory::charging_for(
            crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
            "vector",
            "prepare_authoritative_vector_purge",
            "Raise MEMORY_LIMIT or reduce the selected purge set before preparing its replacement vector image.",
        );
        image.reserve(bytes)?;
        image.vector_copy(bytes)
    }

    /// Enumerate every owned copy for a previously resolved immutable root.
    /// This is deliberately a pure read: the later commit kernel receives this
    /// prepared set but is the only code allowed to remove or publish state.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn prepare_authoritative_purge_set_batch(
        &self,
        selections: &[AuthoritativePurgeSelection],
        queue_mutation_token: event_bus::AuthoritativePurgeQueueMutationToken,
        report_shape: crate::custody::erasure::PurgeReportShape,
        _frontier: Lsn,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<AuthoritativePurgePreparedSet> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();
        let persistence = self.authoritative_purge_store()?;

        #[cfg(test)]
        let _purge_enclosing_producer = crate::persistence::content_identity::proof_allocation_oracle_tests::PurgeProducerGuard::site(
            "Database::prepare_authoritative_purge_set_batch");
        use crate::custody::erasure::PurgeReportShape;
        let report_requested = report_shape != PurgeReportShape::None;
        let _operation = self.open_operation()?;
        // An explicit node-local instruction may have no hub rows.

        // Ownership is a (table,row) pair throughout one union.
        let mut selected_by_table = BTreeMap::<String, HashSet<RowId>>::new();
        for candidate in selections {
            if !selected_by_table
                .entry(candidate.table.clone())
                .or_default()
                .insert(candidate.local_row_id)
            {
                return Err(Error::SyncError(
                    "erasure selected one pinned row twice".into(),
                ));
            }
        }
        let selected = |table: &str, row_id: &RowId| {
            selected_by_table
                .get(table)
                .is_some_and(|ids| ids.contains(row_id))
        };
        let current_generations = selected_by_table
            .keys()
            .map(|table| {
                let generation = if self.table_meta(table).is_some() {
                    Some(self.durable_lineage_table_generation(table, read_memory.clone())?)
                } else {
                    None
                };
                Ok((table.clone(), generation))
            })
            .collect::<Result<HashMap<_, _>>>()?;
        let lifecycle_templates = selections
            .iter()
            .map(|candidate| {
                self.validate_authoritative_purge_selection(candidate, read_memory.clone())
                    .and_then(|witness| {
                        Self::authoritative_purge_lifecycle_template(candidate, &witness)
                            .map(|template| (template, witness.lifecycle.is_some()))
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        let prepared_snapshot = self.snapshot();
        let selected_table_rows = selected_by_table
            .iter()
            .map(|(table, ids)| {
                self.authoritative_purge_selected_row_versions(table, ids, read_memory.clone())
                    .map(|rows| (table.clone(), rows))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        let mut row_versions = Vec::new();
        let mut selected_versions = Vec::new();
        for candidate in selections {
            let mut found = false;
            for row in &selected_table_rows[&candidate.table] {
                if row.row_id != candidate.local_row_id {
                    continue;
                }
                found = true;
                row_versions.push(AuthoritativePurgeRowVersionKey {
                    table: candidate.table.clone(),
                    table_generation: candidate.table_generation,
                    key: candidate.natural_key.pairs(),
                    lineage_root: candidate.lineage_root.clone(),
                    row_id: row.row_id,
                    created_tx: row.created_tx,
                    lsn: row.lsn,
                });
                selected_versions.push(row);
            }
            if !found {
                return Err(Error::SyncError("erasure lost a pinned row".into()));
            }
        }
        let (row_graph_entries, row_graph_credit) = self.selected_row_graph_witnesses(
            selections,
            &selected_table_rows,
            read_memory.clone(),
        )?;
        // Standalone edges have no owning row and are outside row erasure.
        let surviving_forward_entries = Vec::<AdjEntry>::new();
        let canonical_graph_entries = Vec::<AdjEntry>::new();
        let change_log = self.collect_change_log_versions(read_memory.clone())?;
        let mut edge_insert_witnesses = canonical_graph_entries
            .iter()
            .map(|entry| {
                (
                    entry.source,
                    entry.target,
                    entry.edge_type.clone(),
                    entry.lsn,
                )
            })
            .collect::<Vec<_>>();
        let commit_index = self
            .tx_mgr
            .try_commit_index_prefix(self.tx_mgr.current_lsn())?;
        let mut edge_delete_witnesses = canonical_graph_entries
            .iter()
            .filter_map(|entry| {
                entry.deleted_tx.map(|deleted_tx| {
                    let deletion_lsn =
                        commit_index
                            .iter()
                            .find_map(|(lsn, tx)| (*tx == deleted_tx).then_some(*lsn))
                            .ok_or_else(|| {
                                Error::SyncError((
                                "authoritative purge edge deletion lacks its commit-index LSN"
                                    .to_string()).into())
                            })?;
                    Ok((
                        entry.source,
                        entry.target,
                        entry.edge_type.clone(),
                        deletion_lsn,
                    ))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let selected_graph_occurrences = edge_insert_witnesses
            .iter()
            .map(|(source, target, edge_type, lsn)| {
                (*source, *target, edge_type.clone(), *lsn, false)
            })
            .chain(
                edge_delete_witnesses
                    .iter()
                    .map(|(source, target, edge_type, lsn)| {
                        (*source, *target, edge_type.clone(), *lsn, true)
                    }),
            )
            .collect::<HashSet<_>>();
        let removed_graph_arrivals = {
            let mut removed = Vec::new();
            for (source, target, edge_type, lsn, deleted) in &selected_graph_occurrences {
                let identity = (*source, *target, edge_type.clone());
                self.with_graph_arrival(
                    &identity,
                    |marker| {
                        if let Some(marker) = marker
                            && marker.local_lsn == *lsn
                            && marker.deleted == *deleted
                        {
                            removed.push((identity.clone(), marker.clone()));
                        }
                        Ok(())
                    },
                    read_memory.clone(),
                )?;
            }
            removed
        };
        let graph_arrival_config_keys = removed_graph_arrivals
            .iter()
            .map(|(_, marker)| Self::graph_arrival_config_key(marker))
            .collect::<Result<Vec<_>>>()?;

        let selected_change_log_positions = change_log
            .iter()
            .enumerate()
            .filter_map(|(position, entry)| match entry {
                ChangeLogEntry::RowInsert { table, row_id, .. }
                | ChangeLogEntry::RowDelete { table, row_id, .. } => {
                    selected(table, row_id).then_some(position)
                }
                ChangeLogEntry::VectorInsert { index, row_id, .. }
                | ChangeLogEntry::VectorDelete { index, row_id, .. } => {
                    selected(&index.table, row_id).then_some(position)
                }
                ChangeLogEntry::EdgeInsert {
                    source,
                    target,
                    edge_type,
                    lsn,
                } => edge_insert_witnesses
                    .iter()
                    .position(
                        |(witness_source, witness_target, witness_type, witness_lsn)| {
                            source == witness_source
                                && target == witness_target
                                && edge_type == witness_type
                                && lsn == witness_lsn
                        },
                    )
                    .map(|witness_position| {
                        edge_insert_witnesses.remove(witness_position);
                        position
                    }),
                ChangeLogEntry::EdgeDelete {
                    source,
                    target,
                    edge_type,
                    lsn,
                } => edge_delete_witnesses
                    .iter()
                    .position(
                        |(witness_source, witness_target, witness_type, witness_lsn)| {
                            source == witness_source
                                && target == witness_target
                                && edge_type == witness_type
                                && lsn == witness_lsn
                        },
                    )
                    .map(|witness_position| {
                        edge_delete_witnesses.remove(witness_position);
                        position
                    }),
            })
            .collect::<Vec<_>>();
        if !edge_insert_witnesses.is_empty() || !edge_delete_witnesses.is_empty() {
            return Err(Error::SyncError(
                ("authoritative purge edge change-log ownership is incomplete".to_string()).into(),
            ));
        }
        let change_log_entries = selected_change_log_positions
            .iter()
            .copied()
            .map(|position| change_log[position].clone())
            .collect::<Vec<_>>();
        // The relational selector names rows, not vector partitions. Resolve
        // those rows through the lightweight vector directories and fault in
        // only partitions that contain a selected lineage before building the
        // purge image. Reading every registered partition here would turn a
        // one-row forget into a whole-database memory spike.
        for (table, row_ids) in &selected_by_table {
            self.vector_store.ensure_raw_partitions_containing_rows(
                table,
                row_ids,
                vector_copy_owner,
            )?;
        }
        let mut all_partitioned_vector_entries = Vec::new();
        for (table, row_ids) in &selected_by_table {
            all_partitioned_vector_entries.extend(
                self.vector_store
                    .durable_entries_of_partitions_containing_rows(
                        table,
                        row_ids,
                        vector_copy_owner,
                    )?,
            );
        }
        let selected_partitioned_vector_entries = all_partitioned_vector_entries
            .iter()
            .filter(|entry| selected(&entry.entry.index.table, &entry.entry.row_id))
            .cloned()
            .collect::<Vec<_>>();
        let mut vector_entries = Vec::new();
        let mut vector_partition_selectors = Vec::new();
        for selected in &selected_partitioned_vector_entries {
            if !vector_entries.contains(&selected.entry) {
                vector_entries.push(selected.entry.clone());
            }
            let selector = VectorPartitionVersionSelector {
                index: selected.entry.index.clone(),
                partition_key: selected.partition_key.clone(),
                row_id: selected.entry.row_id,
                vector_created_tx: selected.entry.created_tx,
                vector_lsn: selected.entry.lsn,
            };
            if !vector_partition_selectors.contains(&selector) {
                vector_partition_selectors.push(selector);
            }
        }
        let vector_partition_identities =
            persistence.resolve_vector_partition_version_identities(&vector_partition_selectors)?;
        let mut affected_vector_partitions = selected_partitioned_vector_entries
            .iter()
            .map(|entry| {
                VectorPartitionRef::new(entry.entry.index.clone(), entry.partition_key.clone())
            })
            .collect::<Vec<_>>();
        affected_vector_partitions.sort_by(|left, right| {
            left.index
                .table
                .cmp(&right.index.table)
                .then(left.index.column.cmp(&right.index.column))
                .then(left.partition_key.cmp(&right.partition_key))
        });
        affected_vector_partitions.dedup();

        let mut disk_source_provenance = Vec::new();
        for (table, ids) in &selected_by_table {
            disk_source_provenance
                .extend(self.authoritative_purge_disk_source_provenance(table, ids)?);
        }
        let lineage_config_owners = selections
            .iter()
            .zip(lifecycle_templates)
            .map(
                |(candidate, (lifecycle_template, lifecycle_record_is_selected_life))| {
                    AuthoritativePurgeLineageConfigOwners {
                        row_sidecar_key: Self::durable_row_lineage_config_key(
                            &candidate.table,
                            &candidate.natural_key,
                            candidate.table_generation,
                        ),
                        creation_lineage_key: Self::durable_unbound_creation_config_key(
                            &candidate.table,
                            &candidate.natural_key,
                            candidate.table_generation,
                            candidate.local_row_id,
                        ),
                        accepted_author_key: format!(
                            "sync_row_author.{}",
                            Self::hex_component(&sync_identity_key(
                                &candidate.table,
                                &candidate.natural_key
                            ))
                        ),
                        accepted_author_memory_key: (
                            candidate.table.clone(),
                            sync_identity_key(&candidate.table, &candidate.natural_key),
                        ),
                        erase_current_key_metadata: current_generations[&candidate.table]
                            == Some(candidate.table_generation),
                        lifecycle_record_key: Self::durable_lineage_config_key(
                            &candidate.table,
                            &candidate.natural_key,
                            candidate.table_generation,
                        ),
                        lifecycle_record_is_selected_life,
                        lifecycle_template,
                    }
                },
            )
            .collect::<Vec<_>>();
        // The future purge kernel consumed its phase-one drain guard only
        // after acquiring the transaction commit mutex. This phase-two token
        // already freezes queue mutation and remains in `event_bus` through
        // the matching durable purge commit/direct swap.
        let durable_sink_names = self.authoritative_purge_durable_sink_names()?;
        let mut event_bus = self
            .event_bus
            .prepare_authoritative_purge_queue_replacement(
                queue_mutation_token,
                durable_sink_names,
                &selected_by_table,
            )?;
        let sink_names = event_bus.authoritative_purge_sink_names().clone();
        let mut durable_sink_entries = Vec::new();
        let sink_projection_charge = match &self.persistence {
            Some(_) => Some(crate::read_image_memory::ImageMemory::operation_writer(
                crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
            )?),
            None => None,
        };
        let mut survivor_report = OwnedQueryResult::empty();
        if report_shape == PurgeReportShape::PerTable {
            survivor_report.columns =
                vec!["table".into(), "rows_affected".into(), "survivors".into()];
        }
        let mut projections = Vec::new();
        let mut affected_rows = BTreeMap::new();
        for (table, ids) in &selected_by_table {
            durable_sink_entries.extend(self.authoritative_purge_durable_sink_entries(
                table,
                ids,
                &sink_names,
                sink_projection_charge.as_ref(),
            )?);
            let before = &selected_table_rows[table];
            let after = if self.relational_store.uses_disk_backed_committed_bodies() {
                if report_requested && table == "work_jobs" {
                    self.committed_rows_at(table, prepared_snapshot, read_memory.clone())?
                        .into_iter()
                        .filter(|row| !ids.contains(&row.row_id))
                        .collect::<Vec<_>>()
                } else {
                    Vec::new()
                }
            } else {
                self.relational_store
                    .tables
                    .read()
                    .get(table)
                    .into_iter()
                    .flatten()
                    .filter(|row| !ids.contains(&row.row_id))
                    .cloned()
                    .collect::<Vec<_>>()
            };
            let report = Self::prepare_authoritative_purge_survivor_report(
                report_requested,
                table,
                ids,
                before,
                &after,
                prepared_snapshot,
            )?;
            if report_shape == PurgeReportShape::PerTable {
                survivor_report.rows.push(vec![
                    Value::Text(table.clone()),
                    Value::Int64(ids.len() as i64),
                    Value::Json(
                        serde_json::to_value(&report.rows)
                            .map_err(|e| Error::Other((e.to_string()).into()))?,
                    ),
                ]);
            } else if report_shape == PurgeReportShape::Single {
                // A single-table selection preserves the established single-table report.
                survivor_report = report;
            }
            let Some(_) = self.relational_store.table_meta.read().get(table) else {
                // Only the archived immutable occurrences remain. Publishing a
                // table projection here would recreate a dropped declaration.
                continue;
            };
            projections.push((table.clone(), after.clone()));
            affected_rows.insert(
                table.clone(),
                if self.relational_store.uses_disk_backed_committed_bodies() {
                    Vec::new()
                } else {
                    after
                },
            );
        }
        let relational = self
            .relational_store
            .prepare_erasure_publication(projections, selected)?;
        let mut relational =
            relational.with_history(self.relational_store.prepare_schema_history_erasure(
                row_versions.iter().map(|key| (key.row_id, key.lsn)),
                self.engine_sync_read_memory()?,
            )?);
        relational.admit_current_memberships(self.finalization.accountant.read_budget()?)?;
        let replacement_change_log = Vec::new();
        let mut replacement_change_log_table_index = ChangeLogTableIndex::new();
        let mut replacement_change_log_lsn_refcounts = ChangeLogLsnRefcounts::new();
        record_change_log_entries(
            &mut replacement_change_log_table_index,
            &mut replacement_change_log_lsn_refcounts,
            &replacement_change_log,
        );
        let survivor_graph_entries = surviving_forward_entries;
        let graph = GraphStore::prepare_received_schema_publication(
            if self.graph_store.is_source_backed() {
                Vec::new()
            } else {
                survivor_graph_entries
                    .iter()
                    .cloned()
                    .map(Into::into)
                    .collect()
            },
        );
        let vector_registry_entries = all_partitioned_vector_entries
            .into_iter()
            .filter(|entry| !selected(&entry.entry.index.table, &entry.entry.row_id))
            .collect::<Vec<_>>();
        // Borrow the live registry to select declarations before copying;
        // unrelated columns retain their exact cells and policy owners.
        let vector_schemas = self.vector_store.copy_erasure_layouts(
            |table| selected_by_table.contains_key(table),
            vector_copy_owner,
        )?;
        admit_erasure_vector_input_copies(
            &vector_schemas,
            &vector_registry_entries,
            &affected_vector_partitions,
            vector_copy_owner,
        )?;
        let vector = self
            .vector_store
            .prepare_selected_authoritative_purge_publication(
                vector_schemas.clone(),
                vector_registry_entries.clone(),
                affected_vector_partitions.clone(),
                contextdb_vector::MemoryBudgetOwner::clone(&self.finalization.vector_accountant),
                vector_copy_owner,
            )?;
        let old_edges = if self.graph_store.is_source_backed() {
            Vec::new()
        } else {
            self.collect_graph_versions(|_| true, read_memory.clone())?
        };
        let (old_bytes, new_bytes) = {
            let rows = self.relational_store.tables.read();
            let catalog = self.relational_store.table_meta.read();
            let mut old_bytes = 0usize;
            let mut new_bytes = 0usize;
            for (table, after) in &affected_rows {
                if let Some(meta) = catalog.get(table) {
                    old_bytes = old_bytes.saturating_add(self.received_schema_table_rows_bytes(
                        table,
                        meta,
                        rows.get(table).map(Vec::as_slice).unwrap_or(&[]),
                    ));
                    new_bytes = new_bytes
                        .saturating_add(self.received_schema_table_rows_bytes(table, meta, after));
                }
            }
            for edge in &old_edges {
                old_bytes = old_bytes.saturating_add(edge.retained_graph_bytes());
            }
            for edge in &survivor_graph_entries {
                new_bytes = new_bytes.saturating_add(edge.retained_graph_bytes());
            }
            (old_bytes, new_bytes)
        };
        let memory_swap = PreparedMemorySwap::accounting(old_bytes, new_bytes, 0);
        let accepted_author_memory_keys_removed = lineage_config_owners
            .iter()
            .filter(|owner| owner.erase_current_key_metadata)
            .map(|owner| owner.accepted_author_memory_key.clone())
            .collect();
        let publication_replacements = AuthoritativePurgePublicationReplacements {
            relational,
            change_log: replacement_change_log,
            change_log_table_index: replacement_change_log_table_index,
            change_log_lsn_refcounts: replacement_change_log_lsn_refcounts,
            graph,
            vector,
            event_bus: {
                for entry in &durable_sink_entries {
                    event_bus.record_durable_sink_removal(&entry.sink);
                }
                event_bus
            },
            accepted_author_memory_keys_removed,
            graph_arrival_memory_removed: removed_graph_arrivals,
            memory_swap,
        };
        Ok(AuthoritativePurgePreparedSet {
            row_version_keys: row_versions,
            change_log_entries,
            vector_entries,
            vector_partition_identities,
            canonical_graph_entries,
            row_graph_entries,
            row_graph_credit,
            disk_source_provenance,
            lineage_config_owners,
            durable_sink_entries,
            _sink_projection_charge: sink_projection_charge,
            graph_arrival_config_keys,
            publication_replacements,
            survivor_report,
        })
    }

    /// Resolve the exact visible row lives selected by one public `PURGE` and
    /// pass their same-table union to the one durable destruction boundary.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn commit_authoritative_purge_tables(
        &self,
        tables: &[(String, Vec<(NaturalKey, RowId)>)],
        node_local: &[(String, Vec<u8>)],
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<OwnedQueryResult> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        #[cfg(test)]
        let _purge_enclosing_producer = crate::persistence::content_identity::proof_allocation_oracle_tests::PurgeProducerGuard::site(
            "Database::commit_authoritative_purge_tables");
        // A no-match PURGE must not falsely certify erasure while unknown
        // native storage may still contain the selected lineage.
        self.authoritative_purge_store()?;
        let mut selections = Vec::new();
        for (table, matched) in tables {
            for (key, row_id) in matched {
                let selection =
                    self.resolve_authoritative_purge_selection(table, key, read_memory.clone())?;
                if selection.local_row_id != *row_id {
                    return Err(Error::SyncError(
                        "erasure row changed after selection".into(),
                    ));
                }
                selections.push(selection);
            }
        }
        let mut result = if selections.is_empty() && node_local.is_empty() {
            let mut result = OwnedQueryResult::empty();
            if tables.len() > 1 {
                result.columns = vec!["table".into(), "rows_affected".into(), "survivors".into()];
            }
            result
        } else {
            self.commit_authoritative_purge_kernel_batch(
                &selections,
                &[],
                true,
                if tables.len() == 1 {
                    crate::custody::erasure::PurgeReportShape::Single
                } else {
                    crate::custody::erasure::PurgeReportShape::PerTable
                },
                purge_predicate::Instructions {
                    outgoing: node_local,
                    applied: &[],
                },
                vector_copy_owner,
                read_memory.clone(),
            )?
            .survivor_report
        };
        for (table, rows) in tables {
            if rows.is_empty() && tables.len() > 1 {
                result.rows.push(vec![
                    Value::Text(table.clone()),
                    Value::Int64(0),
                    Value::Json(serde_json::json!([])),
                ]);
            }
        }
        result.rows_affected = selections.len() as u64;
        Ok(result)
    }

    pub(super) fn authoritative_purge_delivery_key(frontier: Lsn, ordinal: u32) -> String {
        format!(
            "{AUTHORITATIVE_PURGE_DELIVERY_PREFIX}{:020}.{:010}",
            frontier.0, ordinal
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn authoritative_purge_delivery_items_since(
        &self,
        since_lsn: Lsn,
    ) -> Result<Vec<AuthoritativePurgeDeliveryItem>> {
        let _operation = self.open_operation()?;
        let Some(persistence) = &self.persistence else {
            return Ok(Vec::new());
        };
        let mut items = persistence
            .load_config_values_with_prefix::<AuthoritativePurgeDeliveryItem>(
                AUTHORITATIVE_PURGE_DELIVERY_PREFIX,
            )?
            .into_iter()
            .filter_map(|(key, item)| (item.frontier > since_lsn).then_some((key, item)))
            .collect::<Vec<_>>();
        items.sort_by_key(|(_, item)| (item.frontier, item.ordinal));
        for (key, item) in &items {
            if key != &Self::authoritative_purge_delivery_key(item.frontier, item.ordinal) {
                return Err(Error::SyncError(
                    ("authoritative purge delivery journal key does not match its payload"
                        .to_string())
                    .into(),
                ));
            }
        }
        Ok(items.into_iter().map(|(_, item)| item).collect())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn authoritative_purge_targets_retired_generation(
        &self,
        item: &AuthoritativePurgeDeliveryItem,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<bool> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        // Node-local table generations are intentionally independent.
        if item.node_local_predicate.is_some() {
            return Ok(false);
        }
        Ok(self.table_meta(&item.table).is_none()
            || self.durable_lineage_table_generation(&item.table, read_memory.clone())?
                != item.table_generation)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn retained_unbound_purge_selection(
        &self,
        item: &AuthoritativePurgeDeliveryItem,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<AuthoritativePurgeSelection>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let _scope = self.writer_read_scope(read_memory.clone())?;
        let Some(root) = self.writer_read_root()? else {
            return Ok(None);
        };
        let mut controls = contextdb_core::read_memory::ReadCredit::new(read_memory.clone());
        controls.try_grow_for(
            item.table
                .len()
                .saturating_add(item.natural_key.held_bytes())
                .saturating_mul(16)
                .saturating_add(1024),
            "writer_source",
            "retired_creation_selection",
            "Raise MEMORY_LIMIT to select retained creation evidence.",
        )?;
        let identity = Self::hex_component(&sync_identity_key(&item.table, &item.natural_key));
        let prefix = format!("unbound:{identity}:");
        for lineage_root in &item.purged_lineage_roots {
            let Some(position) = lineage_root
                .strip_prefix(&prefix)
                .and_then(|value| value.parse::<u64>().ok())
            else {
                continue;
            };
            let mut cursor = root.change_log_cursor(
                crate::persistence::writer_source::SourceWindowOwner::Read(read_memory.clone()),
                Lsn(position),
                Lsn(position),
            )?;
            while let Some(window) = cursor.next_change()? {
                let ChangeLogEntry::RowInsert { table, row_id, lsn } = &window.value else {
                    continue;
                };
                if table != &item.table || lsn.0 != position {
                    continue;
                }
                let key = Self::durable_unbound_creation_config_key(
                    &item.table,
                    &item.natural_key,
                    item.table_generation,
                    *row_id,
                );
                let state = self.lineage_state_lock.lock();
                let evidence = self.load_unbound_creation_lineage(&state, &key)?;
                if evidence.is_some_and(|record| {
                    record.table == item.table
                        && record.natural_key == item.natural_key
                        && record.table_generation == item.table_generation
                        && record.local_row_id == *row_id
                        && record.creation_lsn == Some(*lsn)
                }) {
                    self.retain_writer_output(controls);
                    return Ok(Some(AuthoritativePurgeSelection {
                        table: table.clone(),
                        table_generation: item.table_generation,
                        natural_key: item.natural_key.clone(),
                        lineage_root: lineage_root.clone(),
                        local_row_id: *row_id,
                        retained: false,
                    }));
                }
            }
        }
        // Retention may have removed the first row version and its log entry
        // while later versions of the same immutable life remain. The existing
        // creation directory is durable authority even then. Stream its
        // generation range, retaining only one matching metadata window.
        let directory_prefix = format!("sync_creation_lineage.v1.{:016x}.", item.table_generation);
        let mut cursor = root.config_cursor(
            crate::persistence::writer_source::SourceWindowOwner::Read(read_memory.clone()),
            &directory_prefix,
        )?;
        while let Some(window) = cursor.next(|key, bytes| {
            if !key.ends_with(identity.as_bytes()) {
                return Ok(None);
            }
            let record: DurableUnboundCreationLineage =
                RedbPersistence::decode_config_value(bytes)?;
            if record.table != item.table
                || record.natural_key != item.natural_key
                || record.table_generation != item.table_generation
                || Self::durable_unbound_creation_config_key(
                    &record.table,
                    &record.natural_key,
                    record.table_generation,
                    record.local_row_id,
                )
                .as_bytes()
                    != key
            {
                return Ok(None);
            }
            Ok(Some(record))
        })? {
            let Some(record) = window.value else {
                continue;
            };
            let Some(creation) = record.creation_lsn else {
                continue;
            };
            let lineage_root = format!("{prefix}{}", creation.0);
            if item.purged_lineage_roots.contains(&lineage_root) {
                self.retain_writer_output(controls);
                return Ok(Some(AuthoritativePurgeSelection {
                    table: item.table.clone(),
                    table_generation: item.table_generation,
                    natural_key: item.natural_key.clone(),
                    lineage_root,
                    local_row_id: record.local_row_id,
                    retained: false,
                }));
            }
        }
        Ok(None)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn incoming_authoritative_purge_plan(
        &self,
        items: &[AuthoritativePurgeDeliveryItem],
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<IncomingAuthoritativePurgePlan> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let mut plan = IncomingAuthoritativePurgePlan::default();
        for item in items {
            if let Some(bytes) = &item.node_local_predicate {
                let instruction = purge_predicate::NodeLocalPredicate::decode(bytes)?;
                let key = instruction.receipt_key();
                if self
                    .persistence
                    .as_ref()
                    .map(|p| p.load_config_value::<bool>(&key))
                    .transpose()?
                    .flatten()
                    == Some(true)
                {
                    continue;
                }
                plan.selections
                    .entry(item.frontier)
                    .or_default()
                    .extend(instruction.select(
                        self,
                        &item.table,
                        vector_copy_owner,
                        read_memory.clone(),
                    )?);
                plan.instruction_receipts
                    .entry(item.frontier)
                    .or_default()
                    .push(key);
                continue;
            }

            let existing = self.durable_lineage_record_at_generation(
                &item.table,
                &item.natural_key,
                item.table_generation,
            )?;
            if let Some(record) = existing.as_ref()
                && (record.table != item.table
                    || record.natural_key != item.natural_key
                    || record.table_generation != item.table_generation)
            {
                return Err(Error::SyncError(
                    ("authoritative purge delivery does not match the durable lineage record"
                        .to_string())
                    .into(),
                ));
            }
            // Re-delivery of a purge this node already applied is idempotent,
            // whichever life the key holds now.
            if self
                .purged_lineage_records_at_generation(
                    &item.table,
                    &item.natural_key,
                    item.table_generation,
                )?
                .iter()
                .any(|record| item.purged_lineage_roots.contains(&record.lineage_root))
            {
                continue;
            }
            // A record naming none of the purged roots belongs to an earlier
            // life at the key. A purged one moves to its permanent history at
            // commit; an accepted delete is superseded. A delete this edge
            // still owes its hub must reach the hub before the purge applies.
            let existing = match existing {
                Some(record) if !item.purged_lineage_roots.contains(&record.lineage_root) => {
                    if record.delete_obligation == DurableDeleteObligation::Pending {
                        return Err(Error::SyncError((
                            "authoritative purge waits for the pending delete of an earlier life at the same key"
                                .to_string()).into()));
                    }
                    None
                }
                other => other,
            };
            // An absent table has no live row to erase. A live table must
            // have readable generation metadata before delivery can progress.
            let current_generation = if self.table_meta(&item.table).is_some() {
                Some(self.durable_lineage_table_generation(&item.table, read_memory.clone())?)
            } else {
                None
            };
            let current_row_exists = current_generation == Some(item.table_generation)
                && self
                    .row_id_for_natural_key_full(
                        &item.table,
                        &item.natural_key,
                        self.snapshot_for_read(),
                        read_memory.clone(),
                    )?
                    .is_some();
            let mut current_selection = None;
            if current_generation != Some(item.table_generation) {
                let state = self.lineage_state_lock.lock();
                let sidecar = self.load_row_lineage_sidecar(
                    &state,
                    &Self::durable_row_lineage_config_key(
                        &item.table,
                        &item.natural_key,
                        item.table_generation,
                    ),
                )?;
                let retained = sidecar
                    .as_ref()
                    .filter(|record| {
                        record.table_generation == item.table_generation
                            && item.purged_lineage_roots.contains(&record.lineage_root)
                    })
                    .and_then(|record| {
                        record
                            .local_row_id
                            .map(|id| (id, record.lineage_root.clone()))
                    })
                    .or_else(|| {
                        existing
                            .as_ref()
                            .filter(|record| {
                                item.purged_lineage_roots.contains(&record.lineage_root)
                            })
                            .and_then(|record| {
                                record
                                    .local_row_id
                                    .map(|id| (id, record.lineage_root.clone()))
                            })
                    });
                drop(state);
                let retained = match retained {
                    Some((local_row_id, lineage_root)) => Some(AuthoritativePurgeSelection {
                        table: item.table.clone(),
                        table_generation: item.table_generation,
                        natural_key: item.natural_key.clone(),
                        lineage_root,
                        local_row_id,
                        retained: false,
                    }),
                    None => self.retained_unbound_purge_selection(item, read_memory.clone())?,
                };
                if let Some(selection) = retained {
                    let local_row_id = selection.local_row_id;
                    // A retained witness is authority for this exact old life,
                    // never for the recreated table's same-key row.
                    self.validate_authoritative_purge_selection(&selection, read_memory.clone())?;
                    let rows = self.authoritative_purge_selected_row_versions(
                        &item.table,
                        &HashSet::from([local_row_id]),
                        read_memory.clone(),
                    )?;
                    if !rows.is_empty() {
                        current_selection = Some(selection);
                    }
                }
            }
            if current_row_exists {
                let selection = self.resolve_authoritative_purge_selection(
                    &item.table,
                    &item.natural_key,
                    read_memory.clone(),
                )?;
                // The visible row may be an earlier life that an earlier purge
                // in this same delivery erases first; this purge then only
                // records its frontier after it.
                let erased_earlier_in_delivery = selection.table_generation
                    == item.table_generation
                    && items.iter().any(|earlier| {
                        earlier.frontier < item.frontier
                            && earlier.node_local_predicate.is_none()
                            && earlier.table == item.table
                            && earlier.natural_key == item.natural_key
                            && earlier.table_generation == item.table_generation
                            && earlier
                                .purged_lineage_roots
                                .contains(&selection.lineage_root)
                    });
                if !erased_earlier_in_delivery {
                    current_selection = Some(selection);
                }
            }
            if let Some(selection) = current_selection {
                if selection.table_generation != item.table_generation
                    || !item.purged_lineage_roots.contains(&selection.lineage_root)
                {
                    return Err(Error::SyncError((
                        "authoritative purge delivery does not match immutable local lineage evidence"
                            .to_string()).into()));
                }
                plan.selections
                    .entry(item.frontier)
                    .or_default()
                    .push(selection);
                continue;
            }
            // Retain this node's known creator if present; an
            // empty node can adopt the first authenticated purged creator.
            let lineage_root = existing
                .as_ref()
                .map(|record| &record.lineage_root)
                .or_else(|| item.purged_lineage_roots.first())
                .filter(|root| item.purged_lineage_roots.contains(root))
                .cloned()
                .ok_or_else(|| {
                    Error::SyncError((
                        "authoritative purge frontier adoption requires matching lineage evidence"
                            .to_string()).into())
                })?;
            if existing
                .as_ref()
                .is_some_and(|record| record.lineage_root != lineage_root)
            {
                return Err(Error::SyncError(
                    ("authoritative purge frontier conflicts with existing lineage evidence"
                        .to_string())
                    .into(),
                ));
            }
            let adoption = IncomingPurgeFrontierAdoption {
                table: item.table.clone(),
                natural_key: item.natural_key.clone(),
                table_generation: item.table_generation,
                lineage_root,
                existing,
            };
            // Validate every opaque creator tuple before any durable group is
            // touched. The final local frontier is supplied at commit.
            let _ = adoption.at_frontier(Lsn(1))?;
            plan.frontier_adoptions
                .entry(item.frontier)
                .or_default()
                .push(adoption);
        }
        Ok(plan)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn preflight_incoming_authoritative_purge_batch_while_authoritative(
        &self,
        items: &[AuthoritativePurgeDeliveryItem],
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.incoming_authoritative_purge_plan(items, vector_copy_owner, read_memory.clone())
            .map(|_| ())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn apply_incoming_authoritative_purge_batch_while_authoritative(
        &self,
        items: &[AuthoritativePurgeDeliveryItem],
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        // Earlier units in this pull page may already have published writes.
        // Select this mutation from the current publication.
        let _inherited_read = self.enter_writer_read(None, read_memory.clone());
        let _plan_read = self.writer_read_scope(read_memory.clone())?;
        let mut plan =
            self.incoming_authoritative_purge_plan(items, vector_copy_owner, read_memory.clone())?;
        let mut groups = plan
            .selections
            .keys()
            .chain(plan.frontier_adoptions.keys())
            .chain(plan.instruction_receipts.keys())
            .cloned()
            .collect::<BTreeSet<_>>();
        for group in std::mem::take(&mut groups) {
            // A prior group has erased its old storage capabilities. Each
            // subsequent mutation must use the root that group published.
            let _prior_group_read = self.enter_writer_read(None, read_memory.clone());
            let _group_read = self.writer_read_scope(read_memory.clone())?;
            let selections = plan.selections.remove(&group).unwrap_or_default();
            let adoptions = plan.frontier_adoptions.remove(&group).unwrap_or_default();
            let receipts = plan.instruction_receipts.remove(&group).unwrap_or_default();
            if selections.is_empty() && receipts.is_empty() {
                self.commit_incoming_purge_frontier_adoptions(&adoptions)?;
            } else {
                let _ = self.commit_authoritative_purge_kernel_batch(
                    &selections,
                    &adoptions,
                    false,
                    crate::custody::erasure::PurgeReportShape::None,
                    purge_predicate::Instructions {
                        outgoing: &[],
                        applied: &receipts,
                    },
                    vector_copy_owner,
                    read_memory.clone(),
                )?;
            }
        }
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn commit_incoming_purge_frontier_adoptions(
        &self,
        adoptions: &[IncomingPurgeFrontierAdoption],
    ) -> Result<()> {
        if adoptions.is_empty() {
            return Ok(());
        }
        let Some(persistence) = self.persistence.as_ref() else {
            return Err(Error::SyncError(
                ("authoritative purge frontier adoption requires a file-backed database"
                    .to_string())
                .into(),
            ));
        };
        let tables = adoptions
            .iter()
            .map(|adoption| adoption.table.clone())
            .collect();
        let _publication = self.read_publication_gate.acquire_write(&tables);
        self.allocate_ddl_lsn(|frontier| {
            let records = adoptions
                .iter()
                .map(|adoption| {
                    let record = adoption.at_frontier(frontier)?;
                    let key = Self::durable_lineage_config_key(
                        &record.table,
                        &record.natural_key,
                        record.table_generation,
                    );
                    let bytes = RedbPersistence::encode_config_value(&record)?;
                    Ok((key, bytes, record))
                })
                .collect::<Result<Vec<_>>>()?;
            let purged_history = self.encoded_purged_lives_displaced_by(&records)?;
            // Keep canonical state before the pending source in the established
            // lock order until both prepared roots have published.
            let mut lineage_state = self.lineage_state_lock.lock_arc();
            let has_pending_source = self
                .snapshot_registry
                .pending_delete_source
                .lock()
                .is_some();
            let pending = if has_pending_source {
                Some(
                    self.prepare_resident_pending_changes(
                        &lineage_state,
                        records
                            .iter()
                            .map(|(key, _, record)| (key.as_str(), Some(record))),
                    )?,
                )
            } else {
                None
            };
            let replacements = records
                .iter()
                .chain(&purged_history)
                .filter(|(key, _, _)| {
                    lineage_state.records.contains_key(key)
                        || lineage_state.purged_history.contains_key(key)
                })
                .map(|(key, _, record)| (key.clone(), record.clone()))
                .collect::<Vec<_>>();
            self.publish_direct_metadata(|| {
                persistence.flush_encoded_config_values(
                    purged_history
                        .iter()
                        .chain(&records)
                        .map(|(key, bytes, _)| (key.as_str(), bytes.clone()))
                        .collect(),
                )
            })?;
            for (key, record) in replacements {
                if let Some(slot) = lineage_state.records.get_mut(&key) {
                    *slot = record;
                } else if let Some(slot) = lineage_state.purged_history.get_mut(&key) {
                    *slot = record;
                }
            }
            if let Some(pending) = pending {
                pending.publish();
            }
            Ok(())
        })
    }

    /// Private singular wrapper retained for the immutable-root test seam.
    #[cfg(test)]
    #[cfg_attr(
        not(feature = "test-seams"),
        allow(
            dead_code,
            reason = "KEEP-SEAM: live only under the test-seams feature; used from authoritative_purge_kernel_tests.rs"
        )
    )]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub(super) fn commit_authoritative_purge_kernel(
        &self,
        selection: &AuthoritativePurgeSelection,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: contextdb_core::read_memory::ReadMemoryOwner,
    ) -> Result<Lsn> {
        self.commit_authoritative_purge_kernel_batch(
            std::slice::from_ref(selection),
            &[],
            true,
            crate::custody::erasure::PurgeReportShape::None,
            Default::default(),
            vector_copy_owner,
            read_memory.clone(),
        )
        .map(|outcome| outcome.frontier)
    }

    /// One file-backed destruction kernel for the same-table union selected
    /// by a public `PURGE`.  It consumes one queue drain, one frontier, and
    /// one Redb write transaction before direct prebuilt publication.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn commit_authoritative_purge_kernel_batch(
        &self,
        named_selections: &[AuthoritativePurgeSelection],
        frontier_adoptions: &[IncomingPurgeFrontierAdoption],
        journal_outbound_delivery: bool,
        mut report_shape: crate::custody::erasure::PurgeReportShape,
        instructions: purge_predicate::Instructions<'_>,
        _vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<AuthoritativePurgeKernelOutcome> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        #[cfg(test)]
        let _purge_enclosing_producer = crate::persistence::content_identity::proof_allocation_oracle_tests::PurgeProducerGuard::site(
            "Database::commit_authoritative_purge_kernel_batch");
        (|| {
            let _operation = self.open_operation()?;
            let Some(persistence) = self.persistence.as_ref() else {
                return Err(Error::SyncError(
                    ("authoritative purge requires a file-backed database".to_string()).into(),
                ));
            };

            // Declared followers of the named rows join the same erasure. They
            // are found here, and again under the commit mutex below, so a
            // child committed meanwhile cannot outlive its parent.
            let mut follower_credit =
                contextdb_core::read_memory::ReadCredit::new(read_memory.clone());
            let mut working = named_selections.to_vec();
            let followers = self.authoritative_purge_followers(
                &working,
                &mut follower_credit,
                read_memory.clone(),
            )?;
            if !followers.is_empty()
                && report_shape == crate::custody::erasure::PurgeReportShape::Single
            {
                // A purge that erased more than its named table says so per table.
                report_shape = crate::custody::erasure::PurgeReportShape::PerTable;
            }
            working.extend(followers);
            // Every table that follows a selected table, transitively, is
            // gated from the first attempt, so a child committed meanwhile
            // is published under the same gate as its parent's erasure.
            let catalog = self
                .table_names()
                .into_iter()
                .filter_map(|name| self.table_meta(&name).map(|meta| (name, meta)))
                .collect::<Vec<_>>();
            let mut follower_tables = BTreeSet::<String>::new();
            let mut pending = working
                .iter()
                .map(|selection| selection.table.clone())
                .collect::<Vec<_>>();
            while let Some(parent) = pending.pop() {
                for follower in super::purge_followers::purge_followers_of(
                    catalog.iter().map(|(name, meta)| (name.as_str(), meta)),
                    &parent,
                ) {
                    if follower_tables.insert(follower.child_table.clone()) {
                        pending.push(follower.child_table);
                    }
                }
            }
            let selections: &[AuthoritativePurgeSelection] = &working;
            // Candidate discovery and the callback drain both happen before the
            // transaction commit mutex. The complete sorted hash set is then
            // quiesced and acquired atomically; a commit-locked discovery retry
            // drops the whole set before reacquiring the expanded union.
            let mut held_hashes =
                self.authoritative_purge_selected_blob_hashes(selections, read_memory.clone())?;
            let mut queue_drain = Some(self.event_bus.begin_authoritative_purge_queue_drain()?);
            loop {
                let selections: &[AuthoritativePurgeSelection] = &working;
                let publication_tables = selections
                    .iter()
                    .map(|selection| selection.table.clone())
                    .chain(follower_tables.iter().cloned())
                    .chain(instructions.outgoing.iter().map(|(table, _)| table.clone()))
                    .chain(
                        frontier_adoptions
                            .iter()
                            .map(|adoption| adoption.table.clone()),
                    )
                    .collect::<BTreeSet<_>>();
                let blob_guard = self
                    .blob_repository
                    .acquire_exclusive_sorted(&held_hashes)?;
                // Announce cross-process intent and wait out any result that has
                // already reached its final physical publication boundary before
                // taking the transaction commit mutex. New direct publications now
                // wait at admission, while ordinary writers keep committing.
                let mut physical_publication =
                    Some(persistence.acquire_authoritative_purge_publication()?);
                let _read_publication = self
                    .read_publication_gate
                    .acquire_write(&publication_tables);
                let mut newly_discovered = BTreeSet::new();
                let mut newly_followed = Vec::new();
                let attempt = self.allocate_ddl_lsn_maybe(|frontier| {
                    let (selected_hashes, survivor_hashes) = self
                        .authoritative_purge_current_blob_references(
                            selections,
                            !journal_outbound_delivery,
                            read_memory.clone(),
                        )?;
                    newly_discovered.extend(selected_hashes.difference(&held_hashes).copied());
                    if !newly_discovered.is_empty() {
                        return Ok(None);
                    }
                    // Under the commit mutex no child can commit: any follower
                    // found now restarts the attempt with the whole union.
                    newly_followed.extend(self.authoritative_purge_followers(
                        selections,
                        &mut follower_credit,
                        read_memory.clone(),
                    )?);
                    if !newly_followed.is_empty() {
                        return Ok(None);
                    }
                    let deletable_hashes = selected_hashes
                        .difference(&survivor_hashes)
                        .copied()
                        .collect::<BTreeSet<_>>();
                    let blob_purge = self.blob_repository.prepare_authoritative_purge(
                        &blob_guard,
                        &deletable_hashes,
                        frontier,
                    )?;
                    let mut vector_workspace = self
                        .reserve_authoritative_erasure_workspace(selections, read_memory.clone())?;
                    let _erasure_accountant = crate::memory_accounting::AccountantHandle::clone(
                        &self.finalization.accountant,
                    );
                    let queue_mutation_token = queue_drain
                        .take()
                        .expect("authoritative purge queue drain is consumed by one commit attempt")
                        .freeze_for_commit_locked_purge();
                    let prepared = self.prepare_authoritative_purge_set_batch(
                        selections,
                        queue_mutation_token,
                        report_shape,
                        frontier,
                        &mut vector_workspace.owner,
                        read_memory.clone(),
                    )?;

                    let mut memory_swap = PreparedMemorySwap::prepare(
                        crate::memory_accounting::AccountantHandle::clone(
                            &self.finalization.accountant,
                        ),
                        prepared.publication_replacements.memory_swap.old_bytes,
                        prepared.publication_replacements.memory_swap.new_bytes,
                        prepared
                            .publication_replacements
                            .memory_swap
                            .retired_bytes_released_by_store,
                    )?;
                    #[cfg(any(test, feature = "test-seams"))]
                    if take_vector_memory_fault(
                        &VECTOR_DURABLE_PURGE_FAILURE_ARMED,
                        &VECTOR_DURABLE_PURGE_FAILURE_CONSUMED,
                    ) {
                        return Err(Error::Other((
                            "injected durable vector purge prepublication failure".to_string()).into()));
                    }
                    let lifecycle_records = prepared
                        .lineage_config_owners
                        .iter()
                        .map(|owner| {
                            let lifecycle = owner.lifecycle_template.at_frontier(frontier);
                            RedbPersistence::encode_config_value(&lifecycle)
                                .map(|bytes| (owner.lifecycle_record_key.clone(), bytes, lifecycle))
                        })
                        .chain(frontier_adoptions.iter().map(|adoption| {
                            let lifecycle = adoption.at_frontier(frontier)?;
                            let key = Self::durable_lineage_config_key(
                                &lifecycle.table,
                                &lifecycle.natural_key,
                                lifecycle.table_generation,
                            );
                            RedbPersistence::encode_config_value(&lifecycle)
                                .map(|bytes| (key, bytes, lifecycle))
                        }))
                        .collect::<Result<Vec<_>>>()?;
                    // Read under the commit mutex: an earlier life's purged record
                    // at a selected key moves to its permanent history entry in
                    // this same write instead of being overwritten.
                    let purged_history =
                        self.encoded_purged_lives_displaced_by(&lifecycle_records)?;
                    // Key selections and node-local instructions share one frontier.
                    let purge_delivery_items = if journal_outbound_delivery {
                        let keyed = selections
                            .iter()
                            .filter(|s| {
                                !instructions
                                    .outgoing
                                    .iter()
                                    .any(|(table, _)| table == &s.table)
                            })
                            .map(|selection| {
                                Ok(AuthoritativePurgeDeliveryItem {
                                    frontier,
                                    ordinal: 0,
                                    table: selection.table.clone(),
                                    table_generation: selection.table_generation,
                                    natural_key: selection.natural_key.clone(),
                                    // Carry all known copies on the existing purge lane.
                                    purged_lineage_roots: crate::custody::erasure::lineage_roots(
                                        self,
                                        &selection.table,
                                        &selection.natural_key,
                                        selection.table_generation,
                                        &selection.lineage_root,
                                        read_memory.clone(),
                                    )?,
                                    node_local_predicate: None,
                                })
                            })
                            .collect::<Result<Vec<_>>>()?;
                        let local = instructions.outgoing.iter().map(|(table, predicate)| {
                            AuthoritativePurgeDeliveryItem {
                                frontier,
                                ordinal: 0,
                                table: table.clone(),
                                table_generation: 0,
                                natural_key: NaturalKey {
                                    column: String::new(),
                                    value: Value::Null,
                                    rest: Vec::new(),
                                },
                                purged_lineage_roots: Vec::new(),
                                node_local_predicate: Some(predicate.clone()),
                            }
                        });
                        keyed
                            .into_iter()
                            .chain(local)
                            .enumerate()
                            .map(|(ordinal, mut item)| {
                                item.ordinal = u32::try_from(ordinal).map_err(|_| {
                                    Error::SyncError(
                                        "authoritative purge has too many items".into())
                                })?;
                                let key =
                                    Self::authoritative_purge_delivery_key(frontier, item.ordinal);
                                RedbPersistence::encode_config_value(&item)
                                    .map(|bytes| (key, bytes))
                            })
                            .collect::<Result<Vec<_>>>()?
                    } else {
                        Vec::new()
                    };
                    let durable_sink_entries = prepared
                        .durable_sink_entries
                        .iter()
                        .map(|entry| {
                            entry
                                .durable_digest
                                .map(|digest| (entry.sink.clone(), entry.queue_id, digest))
                                .ok_or_else(|| {
                                    Error::SyncError((
                                "authoritative purge durable sink selection lost its raw witness"
                                    .to_string()).into())
                                })
                        })
                        .collect::<Result<Vec<_>>>()?;
                    let mut lineage_state = self.lineage_state_lock.lock_arc();
                    let has_pending_source = self.snapshot_registry.pending_delete_source.lock().is_some();
                    let pending = if has_pending_source {
                        Some(self.prepare_resident_pending_changes(
                            &lineage_state,
                            lifecycle_records.iter()
                                .map(|(key, _, record)| (key.as_str(), Some(record))),
                        )?)
                    } else {
                        None
                    };
                    let (row_graph_entries, row_graph_credit) = self
                        .copy_row_graph_entries(&prepared.row_graph_entries, read_memory.clone())?;
                    let lineage_memory_replacements = lifecycle_records.iter().chain(&purged_history)
                        .filter(|(key, _, _)| lineage_state.records.contains_key(key)
                            || lineage_state.purged_history.contains_key(key))
                        .map(|(key, _, record)| (key.clone(), record.clone()))
                        .collect::<Vec<_>>();
                    let persistence_projection = AuthoritativePurgePersistenceProjection {
                        row_versions: prepared
                            .row_version_keys
                            .iter()
                            .map(|row| (row.table.clone(), row.row_id, row.created_tx, row.lsn))
                            .collect(),
                        source_provenance: prepared.disk_source_provenance.clone(),
                        vectors: prepared.vector_entries.clone(),
                        vector_partition_identities: prepared.vector_partition_identities.clone(),
                        graph_entries: prepared.canonical_graph_entries.clone(),
                        row_graph_entries,
                        row_graph_credit,
                        sink_entries: durable_sink_entries,
                        change_log_entries: prepared.change_log_entries.clone(),
                        config_keys_removed: prepared
                            .lineage_config_owners
                            .iter()
                            .flat_map(|owner| {
                                let mut keys = vec![owner.row_sidecar_key.clone(),
                                    owner.creation_lineage_key.clone()];
                                if owner.erase_current_key_metadata {
                                    keys.push(owner.accepted_author_key.clone());
                                }
                                keys
                            })
                            .chain(prepared.graph_arrival_config_keys.iter().cloned())
                            .chain(self.authoritative_purge_custody_keys(selections, read_memory.clone())?)
                            .collect(),
                        lifecycle_records: purged_history
                            .into_iter()
                            .chain(lifecycle_records)
                            .map(|(key, bytes, _)| (key, bytes))
                            // Applying an instruction and its one-time receipt is atomic.
                            .chain(
                                instructions
                                    .applied
                                    .iter()
                                    .map(|key| {
                                        RedbPersistence::encode_config_value(&true)
                                            .map(|bytes| (key.clone(), bytes))
                                    })
                                    .collect::<Result<Vec<_>>>()?,
                            )
                            .collect(),
                        purge_delivery_items,
                        blob_purge,
                    };
                    let outcome = AuthoritativePurgeKernelOutcome {
                        #[cfg(test)]
                        frontier,
                        survivor_report: prepared.survivor_report,
                    };

                    let writer_erasure = persistence.prepare_writer_erasure(
                        crate::memory_accounting::AccountantHandle::clone(
                            &self.finalization.accountant,
                        ),
                        &publication_tables,
                        &prepared.canonical_graph_entries,
                    )?;
                    let publication = persistence
                        .prepare_acquired_authoritative_purge_publication(
                            physical_publication
                                .take()
                                .expect("one acquired purge publication boundary"),
                            frontier,
                            &publication_tables,
                        )?;
                    #[cfg(any(test, feature = "test-seams"))]
                    crash_at_purge_publication_boundary_for_test("before-durable-commit");
                    #[cfg(any(test, feature = "test-seams"))]
                    crate::persistence::read_persistence_test_scaffold::purge_before_durable_commit_for_test();
                    let marker = publication.as_ref().and_then(|publication| publication.marker());
                    if let Err(error) = persistence.commit_authoritative_purge(
                        &persistence_projection,
                        marker,
                        frontier,
                    ) {
                        if let Some(publication) = publication {
                            let _ = publication.abort();
                        }
                        return Err(error);
                    }
                    _read_publication.invalidate_cursors();
                    writer_erasure.publish();
                    #[cfg(any(test, feature = "test-seams"))]
                    crash_at_purge_publication_boundary_for_test("after-durable-commit");
                    let publication_error = if let Some(publication) = publication {
                        // Pending is already fail-closed. If this final clear
                        // publication is interrupted, reopen resolves it from the
                        // token committed in the erasure transaction.
                        publication.commit().err()
                    } else {
                        None
                    };
                    *self.custody_cache.lock() = None;

                    // Durability succeeded.  Every operation below is a direct,
                    // prebuilt replacement; do not add validation, persistence,
                    // allocation, or reconstruction after this point.
                    self.relational_store.publish_prepared_received_schema(
                        prepared.publication_replacements.relational,
                    );
                    {
                        let mut change_log = self.change_log.write();
                        let before = crate::composite_store::change_log_retained_bytes(&change_log);
                        *change_log = prepared.publication_replacements.change_log;
                        self.finalization.accountant.release(before.saturating_sub(
                            crate::composite_store::change_log_retained_bytes(&change_log),
                        ));
                    }
                    *self.change_log_table_index.write() =
                        prepared.publication_replacements.change_log_table_index;
                    *self.change_log_lsn_refcounts.write() =
                        prepared.publication_replacements.change_log_lsn_refcounts;
                    self.graph_store
                        .publish_prepared_received_schema(prepared.publication_replacements.graph);
                    self.vector_store
                        .publish_prepared_received_schema(prepared.publication_replacements.vector);
                    self.event_bus
                        .publish_prepared_authoritative_purge_queue_replacement(
                            prepared.publication_replacements.event_bus,
                        );
                    {
                        let mut authors = self.accepted_sync_row_authors.write();
                        for key in prepared.publication_replacements.accepted_author_memory_keys_removed {
                            authors.remove(&key);
                        }
                    }
                    {
                        let mut arrivals = self.sync_graph_arrivals.write();
                        for (identity, selected) in prepared.publication_replacements.graph_arrival_memory_removed {
                            if arrivals.get(&identity).map(|record| &**record) == Some(&selected) { arrivals.remove(&identity); }
                        }
                    }
                    for owner in &prepared.lineage_config_owners {
                        lineage_state.row_sidecars.remove(&owner.row_sidecar_key);
                        lineage_state.unbound_creations.remove(&owner.creation_lineage_key);
                    }
                    for (key, record) in lineage_memory_replacements {
                        if let Some(slot) = lineage_state.records.get_mut(&key) { *slot = record; }
                        else if let Some(slot) = lineage_state.purged_history.get_mut(&key) { *slot = record; }
                    }
                    if let Some(pending) = pending {
                        pending.publish();
                    }
                    memory_swap.commit_after_swap();
                    if let Some(error) = publication_error {
                        return Err(error);
                    }
                    Ok(Some(outcome))
                })?;
                match attempt {
                    Some(outcome) => return Ok(outcome),
                    None => {
                        drop(blob_guard);
                        held_hashes.extend(newly_discovered);
                        if !newly_followed.is_empty() {
                            if report_shape == crate::custody::erasure::PurgeReportShape::Single {
                                report_shape = crate::custody::erasure::PurgeReportShape::PerTable;
                            }
                            working.extend(newly_followed);
                            held_hashes.extend(self.authoritative_purge_selected_blob_hashes(
                                &working,
                                read_memory.clone(),
                            )?);
                        }
                    }
                }
            }
        })()
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn arm_authoritative_purge_point_remove_persistence_failure_for_test(&self) {
        let _operation = self.assert_open_operation();
        crate::persistence::arm_authoritative_purge_point_remove_persistence_failure_for_test();
    }

    #[cfg(test)]
    #[cfg_attr(
        not(feature = "test-seams"),
        allow(
            dead_code,
            reason = "KEEP-SEAM: live only under the test-seams feature; used from authoritative_purge_blob_storage_contract_tests.rs"
        )
    )]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub(super) fn install_authoritative_purge_complete_blob_fixture_for_test(
        &self,
        hash: &[u8; 32],
        bytes: &[u8],
        start_lsn: u64,
    ) -> Result<()> {
        let _operation = self.assert_open_operation();
        self.blob_repository
            .install_complete_fixture_for_test(*hash, bytes, start_lsn)
    }

    #[cfg(test)]
    #[cfg_attr(
        not(feature = "test-seams"),
        allow(
            dead_code,
            reason = "KEEP-SEAM: live only under the test-seams feature; used from authoritative_purge_blob_storage_contract_tests.rs"
        )
    )]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub(super) fn install_authoritative_purge_interrupted_blob_fixture_for_test(
        &self,
        hash: &[u8; 32],
        verified_prefix: &[u8],
        total_size: u64,
        start_lsn: u64,
    ) -> Result<()> {
        let _operation = self.assert_open_operation();
        self.blob_repository.install_interrupted_fixture_for_test(
            *hash,
            verified_prefix,
            total_size,
            start_lsn,
        )
    }

    #[cfg(test)]
    #[cfg_attr(
        not(feature = "test-seams"),
        allow(
            dead_code,
            reason = "KEEP-SEAM: live only under the test-seams feature; used from authoritative_purge_blob_storage_contract_tests.rs"
        )
    )]
    pub(super) fn authoritative_purge_blob_roles_for_test(
        &self,
        hash: &[u8; 32],
    ) -> BTreeSet<String> {
        let _operation = self.assert_open_operation();
        self.blob_repository
            .authoritative_purge_roles_for_test(hash)
            .expect("inspect authoritative-purge blob roles")
    }

    #[cfg(test)]
    #[cfg_attr(
        not(feature = "test-seams"),
        allow(
            dead_code,
            reason = "KEEP-SEAM: live only under the test-seams feature; used from authoritative_purge_blob_storage_contract_tests.rs"
        )
    )]
    pub(super) fn authoritative_purge_blob_bytes_for_test(
        &self,
        hash: &[u8; 32],
        max_bytes: usize,
    ) -> Option<Vec<u8>> {
        let _operation = self.assert_open_operation();
        self.blob_repository
            .bounded_payload_for_test(hash, max_bytes)
            .expect("read bounded authoritative-purge blob payload")
    }

    #[cfg(test)]
    #[cfg_attr(
        not(feature = "test-seams"),
        allow(
            dead_code,
            reason = "KEEP-SEAM: live only under the test-seams feature; used from authoritative_purge_blob_storage_contract_tests.rs"
        )
    )]
    pub(super) fn authoritative_purge_next_missing_range_for_test(
        &self,
        hash: &[u8; 32],
    ) -> Option<std::ops::Range<u64>> {
        let _operation = self.assert_open_operation();
        self.blob_repository
            .next_missing_range_for_test(hash)
            .expect("read durable authoritative-purge blob resume range")
    }

    #[cfg(test)]
    #[cfg_attr(
        not(feature = "test-seams"),
        allow(
            dead_code,
            reason = "KEEP-SEAM: live only under the test-seams feature; used from authoritative_purge_blob_storage_contract_tests.rs"
        )
    )]
    pub(super) fn arm_authoritative_purge_blob_commit_failure_for_test(&self) {
        let _operation = self.assert_open_operation();
        self.blob_repository
            .arm_authoritative_purge_commit_failure_for_test();
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn authoritative_purge_disk_source_provenance(
        &self,
        selected_table: &str,
        selected_row_ids: &HashSet<RowId>,
    ) -> Result<Vec<(String, RowId, Lsn, u8)>> {
        self.authoritative_purge_store()?;
        let memory = self.engine_sync_read_memory()?;
        let _source = self.writer_read_scope(memory.clone())?;
        let root = self
            .writer_read_root()?
            .ok_or_else(|| Error::Other("selected erasure has no writer source".into()))?;
        let mut output = contextdb_core::read_memory::ReadCredit::new(memory.clone());
        let mut rows = Vec::new();
        for row in selected_row_ids {
            if let Some((lsn, kind)) = root.sync_source(
                crate::persistence::writer_source::SourceWindowOwner::Read(memory.clone()),
                selected_table,
                *row,
            )? {
                output.try_grow_for(
                    selected_table.len() + std::mem::size_of::<(String, RowId, Lsn, u8)>() + 32,
                    "writer_source",
                    "selected_erasure_provenance",
                    "Raise MEMORY_LIMIT to erase selected lineage provenance.",
                )?;
                rows.reserve_exact(1);
                rows.push((selected_table.to_owned(), *row, lsn, kind));
            }
        }
        self.retain_writer_output(output);
        Ok(rows)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn authoritative_purge_durable_sink_names(&self) -> Result<BTreeSet<String>> {
        let mut names = BTreeSet::new();
        if let Some(persistence) = &self.persistence {
            names.extend(persistence.sink_queue_names()?);
        }
        Ok(names)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn authoritative_purge_durable_sink_entries(
        &self,
        table: &str,
        selected_row_ids: &HashSet<RowId>,
        sink_names: &BTreeSet<String>,
        projection: Option<&crate::read_image_memory::ImageMemoryHandle>,
    ) -> Result<Vec<AuthoritativePurgeSinkEntryKey>> {
        let (Some(persistence), Some(projection)) = (&self.persistence, projection) else {
            return Ok(Vec::new());
        };
        let mut entries = Vec::new();
        for sink in sink_names {
            // Stream the queue: each entry is decoded under its own writer
            // owner and destroyed, with its credit returned, before the next;
            // only the selected ones are kept, each as its id and a digest of
            // its durable value, admitted before it is kept. The admission
            // covers this list and the durable projection built from it.
            persistence.visit_sink_queue(sink, |queue_id, durable_bytes| {
                let entry_owner = crate::read_image_memory::ImageMemory::operation_writer(
                    crate::memory_accounting::AccountantHandle::clone(
                        &self.finalization.accountant,
                    ),
                )?;
                let selected = {
                    let _decode_scope = entry_owner.enter();
                    let entry = RedbPersistence::decode_config_value::<event_bus::SinkQueueEntry>(
                        durable_bytes,
                    )?;
                    (entry.event.table == table && selected_row_ids.contains(&entry.row_id))
                        .then_some(entry.row_id)
                };
                drop(entry_owner);
                if let Some(row_id) = selected {
                    let keep = std::mem::size_of::<AuthoritativePurgeSinkEntryKey>()
                        .saturating_add(std::mem::size_of::<(String, u64, [u8; 32])>())
                        .saturating_add(sink.len())
                        .saturating_mul(2);
                    projection.reserve(keep)?;
                    entries.push(AuthoritativePurgeSinkEntryKey {
                        sink: sink.clone(),
                        queue_id,
                        row_id,
                        durable_digest: Some(*blake3::hash(durable_bytes).as_bytes()),
                    });
                }
                Ok(())
            })?;
        }
        Ok(entries)
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn authoritative_purge_memory_sink_queue_for_test(
        &self,
        sink: &str,
    ) -> BTreeMap<RowId, HashMap<String, Value>> {
        let _operation = self.assert_open_operation();
        // A bounded queue mirrors only its resident window; the rest of its
        // pending entries are read from the store.
        self.pending_sink_entries_for_test(sink)
            .into_iter()
            .map(|entry| (entry.row_id, entry.event.row_values))
            .collect()
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn authoritative_purge_current_live_row_sidecar_for_test(
        &self,
        table: &str,
        natural_key: &NaturalKey,
    ) -> Option<AuthoritativePurgeLiveRowSidecarSnapshot> {
        let _operation = self.assert_open_operation();
        let read_memory = self
            .declared_reader_memory()
            .expect("sidecar observation read owner");
        let table_generation = self
            .durable_lineage_table_generation(table, read_memory.clone())
            .expect("sidecar generation read");
        let snapshot = self.snapshot_for_read();
        let local_row_id = self
            .row_id_for_natural_key_full(table, natural_key, snapshot, read_memory)
            .ok()??;
        let state = self.lineage_state_lock.lock();
        self.load_row_lineage_sidecar(
            &state,
            &Self::durable_row_lineage_config_key(table, natural_key, table_generation),
        )
        .ok()
        .flatten()
        .filter(|sidecar| {
            sidecar.table_generation == table_generation
                && sidecar.local_row_id == Some(local_row_id)
        })
        .map(|sidecar| AuthoritativePurgeLiveRowSidecarSnapshot {
            author_node_id: sidecar.author_node_id,
            author_database_incarnation: sidecar.author_database_incarnation,
            author_local_mutation_position: sidecar.author_local_mutation_position,
            table_generation: sidecar.table_generation,
            lineage_root: sidecar.lineage_root,
            lineage_attestation: sidecar.lineage_attestation,
            local_row_id: sidecar.local_row_id,
            locally_created: sidecar.locally_created,
        })
    }

    #[cfg(test)]
    #[cfg_attr(
        not(feature = "test-seams"),
        allow(
            dead_code,
            reason = "KEEP-SEAM: live only under the test-seams feature; used from authoritative_purge_kernel_tests.rs"
        )
    )]
    pub(super) fn authoritative_purge_live_row_sidecar_for_test(
        &self,
        selection: &AuthoritativePurgeSelection,
    ) -> Option<AuthoritativePurgeLiveRowSidecarSnapshot> {
        let _operation = self.assert_open_operation();
        let state = self.lineage_state_lock.lock();
        self.load_row_lineage_sidecar(
            &state,
            &Self::durable_row_lineage_config_key(
                &selection.table,
                &selection.natural_key,
                selection.table_generation,
            ),
        )
        .ok()
        .flatten()
        .filter(|sidecar| {
            sidecar.table_generation == selection.table_generation
                && sidecar.local_row_id == Some(selection.local_row_id)
                && sidecar.lineage_root == selection.lineage_root
        })
        .map(|sidecar| AuthoritativePurgeLiveRowSidecarSnapshot {
            author_node_id: sidecar.author_node_id,
            author_database_incarnation: sidecar.author_database_incarnation,
            author_local_mutation_position: sidecar.author_local_mutation_position,
            table_generation: sidecar.table_generation,
            lineage_root: sidecar.lineage_root,
            lineage_attestation: sidecar.lineage_attestation,
            local_row_id: sidecar.local_row_id,
            locally_created: sidecar.locally_created,
        })
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn classify_authoritative_purge_root_for_test(
        &self,
        table: &str,
        table_generation: u64,
        natural_key: &NaturalKey,
        lineage_root: &str,
    ) -> AuthoritativePurgeRootClassification {
        let _operation = self.assert_open_operation();
        let state = self.lineage_state_lock.lock();
        let record = self
            .purged_lineage_records(&state, table, natural_key, table_generation)
            .ok()
            .and_then(|records| {
                records
                    .into_iter()
                    .find(|record| record.lineage_root == lineage_root)
            });
        match record.and_then(|record| {
            (record.table == table
                && record.natural_key == *natural_key
                && record.table_generation == table_generation
                && record.lineage_root == lineage_root)
                .then(|| {
                    record
                        .purge_frontier
                        .and_then(|frontier| frontier.parse::<u64>().ok())
                })
                .flatten()
        }) {
            Some(frontier) => AuthoritativePurgeRootClassification::Purged {
                permanent_frontier: Lsn(frontier),
            },
            None => AuthoritativePurgeRootClassification::NotPurged,
        }
    }

    /// Read-only test inspection for the config-keyed deletion contract.
    /// Absent fields mean this key has no lineage record; production code
    /// cannot call this seam. Redb records survive reopen; memory records do
    /// not outlive the shared database handle.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn durable_deletion_state_for_test(
        &self,
        table: &str,
        natural_key: &Value,
    ) -> DurableDeletionStateSnapshot {
        let _operation = self.assert_open_operation();
        let Some(meta) = self.table_meta(table) else {
            return DurableDeletionStateSnapshot::default();
        };
        let Some(column) = natural_key_column_for_meta(&meta) else {
            return DurableDeletionStateSnapshot::default();
        };
        let key = NaturalKey::single(column, natural_key.clone());
        let Some(record) = self
            .durable_lineage_record(
                table,
                &key,
                self.declared_reader_memory()
                    .expect("deletion observation read owner"),
            )
            .expect("deletion lineage read")
        else {
            return DurableDeletionStateSnapshot::default();
        };
        DurableDeletionStateSnapshot {
            table_generation: Some(record.table_generation),
            lineage_root: Some(record.lineage_root),
            delete_obligation: (record.delete_obligation == DurableDeleteObligation::Pending)
                .then_some("pending".to_string()),
            accepted_delete_marker: (record.delete_obligation == DurableDeleteObligation::Accepted)
                .then_some("accepted".to_string()),
            purge_frontier: record.purge_frontier,
        }
    }
}
