use super::*;

impl Database {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn declaration_comparison_encoded_len(change: &DdlChange) -> Result<usize> {
        struct Counter {
            bytes: usize,
            overflowed: bool,
        }
        impl std::io::Write for Counter {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                let Some(total) = self.bytes.checked_add(bytes.len()) else {
                    self.overflowed = true;
                    return Err(std::io::Error::from(std::io::ErrorKind::OutOfMemory));
                };
                self.bytes = total;
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let mut counter = Counter {
            bytes: 0,
            overflowed: false,
        };
        let result = bincode::serde::encode_into_std_write(
            change,
            &mut counter,
            bincode::config::standard(),
        );
        if counter.overflowed {
            return Err(Error::StoreCorrupted {
                path: "ddl_eligible_occurrences".into(),
                reason: "declaration encoding byte count overflow".into(),
            });
        }
        result.map_err(|error| {
            Error::Other((format!("declaration encoding failed: {error}")).into())
        })?;
        Ok(counter.bytes)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn durable_superseded_local_ddl_occurrences(
        &self,
        received_ddl: &[DdlChange],
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Vec<((Lsn, u32), DurableSupersededDdlOccurrence)>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let root = self.writer_read_root()?.ok_or_else(|| {
            Error::Other("durable declaration comparison requires a writer root".into())
        })?;
        let mut output = contextdb_core::read_memory::ReadCredit::new(read_memory.clone());
        let mut candidates = Vec::<((Lsn, u32), DurableSupersededDdlOccurrence)>::new();
        let mut comparison_controls =
            contextdb_core::read_memory::ReadCredit::new(read_memory.clone());
        let mut selector_positions = HashMap::<[u8; 32], (usize, Option<(Lsn, u32)>)>::new();
        let mut selected_counts = HashMap::<Lsn, u64>::new();
        for (received_position, change) in received_ddl.iter().enumerate() {
            let encoded_len = Self::declaration_comparison_encoded_len(change)?;
            let _encoded_memory = contextdb_core::read_memory::ReadCredit::try_new(
                read_memory.clone(),
                encoded_len,
                "writer_source",
                "declaration_comparison",
                "Raise MEMORY_LIMIT to compare this received declaration.",
            )?;
            // The exact payload charge precedes allocation. This fixed slice cannot grow
            // while encoding; reverse local-drop order destroys it before returning credit.
            let mut encoded = vec![0u8; encoded_len];
            let written = bincode::serde::encode_into_slice(
                change,
                encoded.as_mut_slice(),
                bincode::config::standard(),
            )
            .map_err(|error| {
                Error::Other((format!("declaration encoding failed: {error}")).into())
            })?;
            if written != encoded_len {
                return Err(crate::funded_error::corruption_text(
                    Ok(read_memory.clone()),
                    "ddl_eligible_occurrences",
                    "declaration encoding length changed after admission".into(),
                ));
            }
            let kind = *encoded
                .first()
                .ok_or_else(|| Error::Other("empty encoded declaration".into()))?;
            // A declaration that names a table can only restate an occurrence
            // recorded for that same table. Select those occurrences from the
            // small per-occurrence generation records and decode only them, so
            // unrelated declarations (event routes with large filters, other
            // tables) are never read. An occurrence without a generation
            // record never qualifies below either way.
            if let Some(table) = Self::ddl_affected_table(change) {
                if let Some(found) = self.durable_superseded_table_occurrence(
                    &root,
                    table,
                    change,
                    kind,
                    &candidates,
                    read_memory.clone(),
                )? {
                    output.try_grow_for(
                        512,
                        "writer_source",
                        "declaration_comparison",
                        "Raise MEMORY_LIMIT to retain matched declaration evidence.",
                    )?;
                    let lsn = found.0.0;
                    if !selected_counts.contains_key(&lsn) {
                        comparison_controls.try_grow_for(
                            128,
                            "writer_source",
                            "declaration_comparison",
                            "Raise MEMORY_LIMIT to retain declaration comparison controls.",
                        )?;
                    }
                    *selected_counts.entry(lsn).or_default() += 1;
                    candidates.push(found);
                }
                continue;
            }
            let mut selector_hash = blake3::Hasher::new();
            selector_hash.update(b"contextdb-exact-ddl-selector\0");
            selector_hash.update(&encoded);
            let selector_hash = *selector_hash.finalize().as_bytes();
            let mut after = if let Some((prior, position)) = selector_positions.get(&selector_hash)
            {
                if &received_ddl[*prior] != change {
                    return Err(crate::funded_error::corruption_text(
                        Ok(read_memory.clone()),
                        "ddl_eligible_occurrences",
                        "exact declaration selector hash collision".into(),
                    ));
                }
                *position
            } else {
                comparison_controls.try_grow_for(
                    256,
                    "writer_source",
                    "declaration_comparison",
                    "Raise MEMORY_LIMIT to retain declaration comparison controls.",
                )?;
                selector_positions.insert(selector_hash, (received_position, None));
                None
            };
            loop {
                let Some(key) = RedbPersistence::first_eligible_ddl_occurrence(
                    &root,
                    crate::persistence::writer_source::SourceWindowOwner::Read(read_memory.clone()),
                    change,
                    after,
                )?
                else {
                    break;
                };
                if candidates.iter().any(|(candidate, _)| *candidate == key) {
                    after = Some(key);
                    continue;
                }
                let generation_key = format!("sync_ddl_generation.v1.{:020}.{:06}", key.0.0, key.1);
                let generation = root
                    .config_point::<DurableDdlGenerationSidecar>(
                        crate::persistence::writer_source::SourceWindowOwner::Read(
                            read_memory.clone(),
                        ),
                        &generation_key,
                    )?
                    .ok_or_else(|| {
                        crate::funded_error::corruption_text(
                            Ok(read_memory.clone()),
                            generation_key,
                            "eligible declaration lacks its generation sidecar".into(),
                        )
                    })?;
                let arrival_key = Self::durable_received_ddl_arrival_key(key.0, key.1);
                let superseded_key = Self::durable_superseded_ddl_occurrence_key(key.0, key.1);
                if root
                    .config_point::<DurableReceivedDdlArrival>(
                        crate::persistence::writer_source::SourceWindowOwner::Read(
                            read_memory.clone(),
                        ),
                        &arrival_key,
                    )?
                    .is_some()
                    || root
                        .config_point::<DurableSupersededDdlOccurrence>(
                            crate::persistence::writer_source::SourceWindowOwner::Read(
                                read_memory.clone(),
                            ),
                            &superseded_key,
                        )?
                        .is_some()
                {
                    return Err(crate::funded_error::corruption_text(
                        Ok(read_memory.clone()),
                        arrival_key,
                        "eligible declaration carries an exclusion marker".into(),
                    ));
                }
                output.try_grow_for(
                    512,
                    "writer_source",
                    "declaration_comparison",
                    "Raise MEMORY_LIMIT to retain matched declaration evidence.",
                )?;
                if !selected_counts.contains_key(&key.0) {
                    comparison_controls.try_grow_for(
                        128,
                        "writer_source",
                        "declaration_comparison",
                        "Raise MEMORY_LIMIT to retain declaration comparison controls.",
                    )?;
                }
                *selected_counts.entry(key.0).or_default() += 1;
                selector_positions
                    .get_mut(&selector_hash)
                    .expect("admitted selector position")
                    .1 = Some(key);
                candidates.push((
                    key,
                    DurableSupersededDdlOccurrence {
                        local_lsn: key.0,
                        persisted_ordinal: key.1,
                        digest: Self::canonical_local_ddl_arrival_digest(
                            change,
                            key.0,
                            key.1,
                            &generation.value,
                        )?,
                    },
                ));
                break;
            }
        }
        let mut complete_commits = HashSet::new();
        for (key, _) in &candidates {
            if complete_commits.contains(&key.0) {
                continue;
            }
            let selected = selected_counts[&key.0];
            let complete = selected
                == RedbPersistence::unmarked_ddl_occurrences(
                    &root,
                    crate::persistence::writer_source::SourceWindowOwner::Read(read_memory.clone()),
                    key.0,
                )?;
            if complete {
                complete_commits.insert(key.0);
            }
        }
        candidates.retain(|(key, _)| complete_commits.contains(&key.0));
        candidates.sort_by_key(|(key, _)| *key);
        self.retain_writer_output(output);
        Ok(candidates)
    }

    /// The first locally recorded, not yet received or superseded occurrence
    /// of `change` among the occurrences recorded for `table`, in log order.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn durable_superseded_table_occurrence(
        &self,
        root: &crate::persistence::WriterSnapshotHandle,
        table: &str,
        change: &DdlChange,
        kind: u8,
        candidates: &[((Lsn, u32), DurableSupersededDdlOccurrence)],
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<((Lsn, u32), DurableSupersededDdlOccurrence)>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        const PREFIX: &str = "sync_ddl_generation.v1.";
        let mut sidecars = root.config_cursor(
            crate::persistence::writer_source::SourceWindowOwner::Read(read_memory.clone()),
            PREFIX,
        )?;
        while let Some(window) = sidecars.next(|_, bytes| {
            RedbPersistence::decode_config_value_exact::<DurableDdlGenerationSidecar>(bytes)
        })? {
            let key = RedbPersistence::ddl_config_occurrence_coordinate(
                std::str::from_utf8(window.position()).map_err(|_| Error::StoreCorrupted {
                    path: "config".into(),
                    reason: "declaration generation key is not UTF-8".into(),
                })?,
            )?;
            if window.value.table.as_deref() != Some(table) {
                continue;
            }
            if self.with_received_ddl_arrival_at_root(root, key, read_memory.clone(), |value| {
                Ok(value.is_some())
            })? || self.with_superseded_ddl_occurrence_at_root(
                root,
                key,
                read_memory.clone(),
                |value| Ok(value.is_some()),
            )? || candidates.iter().any(|(candidate, _)| *candidate == key)
            {
                continue;
            }
            // Read only this occurrence's log position.
            let mut occurrences = root.ddl_history_cursor(
                crate::persistence::writer_source::SourceWindowOwner::Read(read_memory.clone()),
                Lsn(key.0.0.saturating_sub(1)),
            )?;
            let mut ordinal = 0u32;
            let mut local = None;
            while let Some(entry) = occurrences.next_ddl_candidate(kind)? {
                if entry.value.0 != key.0 {
                    break;
                }
                if ordinal == key.1 {
                    local = Some(entry);
                    break;
                }
                ordinal += 1;
            }
            let Some(local) = local else {
                continue;
            };
            if local.value.1.as_ref() != Some(change) {
                continue;
            }
            return Ok(Some((
                key,
                DurableSupersededDdlOccurrence {
                    local_lsn: key.0,
                    persisted_ordinal: key.1,
                    digest: Self::canonical_local_ddl_arrival_digest(
                        change,
                        key.0,
                        key.1,
                        &window.value,
                    )?,
                },
            )));
        }
        Ok(None)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn canonical_local_ddl_arrival_digest(
        ddl: &DdlChange,
        local_lsn: Lsn,
        persisted_ordinal: u32,
        generation: &DurableDdlGenerationSidecar,
    ) -> Result<Vec<u8>> {
        #[cfg(feature = "test-seams")]
        crate::outbound_ddl_test_seams::note_digest_checked((local_lsn, persisted_ordinal));
        // The receiver-local marker binds the durable DDL-log occurrence, not
        // the authenticated source spelling (whose full-fidelity digest is
        // validated separately before this point). The DDL-log codec is exact,
        // so the occurrence is the same before and after reopen.
        crate::protocol::canonical_ddl_provenance_digest(
            &crate::protocol::WireDdlChange::from(ddl.clone()),
            local_lsn,
            persisted_ordinal,
            generation.table.as_deref(),
            generation.table_generation,
        )
        .map_err(|err| Error::SyncError((err.to_string()).into()))
    }

    pub(super) fn durable_received_ddl_marker_key(
        tenant_id: &TenantId,
        source_node_id: &str,
        source_incarnation: Incarnation,
        source_ddl_lsn: Lsn,
        ordinal: u32,
    ) -> String {
        tenant_id.config_key(&format!(
            "sync_ddl_apply.v1.{}.inc.{}.lsn.{:020}.{:06}",
            Self::hex_component(source_node_id.as_bytes()),
            source_incarnation.to_hex(),
            source_ddl_lsn.0,
            ordinal,
        ))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn received_schema_manifest_markers(
        received: &crate::protocol::ReceivedDdlContext,
    ) -> Result<Vec<DurableReceivedSchemaManifestMarker>> {
        let mut seen = HashSet::new();
        let mut markers = Vec::with_capacity(received.entries.len());
        for entry in &received.entries {
            let key = Self::durable_received_ddl_marker_key(
                &received.tenant_id,
                &received.source_node_id,
                received.source_incarnation,
                entry.source_ddl_lsn,
                entry.ordinal,
            );
            if !seen.insert(key.clone()) {
                return Err(Error::SyncError(
                    ("received schema migration repeats an authenticated marker identity"
                        .to_string())
                    .into(),
                ));
            }
            markers.push(DurableReceivedSchemaManifestMarker {
                key,
                digest: entry.digest.clone(),
            });
        }
        Ok(markers)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn durable_received_schema_manifest_key(
        received: &crate::protocol::ReceivedDdlContext,
    ) -> Result<String> {
        let markers = Self::received_schema_manifest_markers(received)?;
        let ordered_identity = markers
            .iter()
            .map(|marker| Self::hex_component(marker.key.as_bytes()))
            .collect::<Vec<_>>()
            .join(".");
        Ok(received.tenant_id.config_key(&format!(
            "sync_ddl_manifest.v1.{}.inc.{}.count.{:06}.{ordered_identity}",
            Self::hex_component(received.source_node_id.as_bytes()),
            received.source_incarnation.to_hex(),
            markers.len(),
        )))
    }

    /// Read retry evidence while the private manifest lock is held through
    /// the later commit. File-backed databases read Redb; memory databases
    /// read the shared lifetime mirror.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn classify_received_schema_manifest(
        &self,
        received: Option<&crate::protocol::ReceivedDdlContext>,
    ) -> Result<ReceivedSchemaManifestClassification> {
        // An unattested unit carries no signed identity, so there is nothing to
        // recognise a retry by. It is applied or refused; it is never replayed
        // from a stored result, and it stores none.
        let Some(received) = received else {
            return Ok(ReceivedSchemaManifestClassification::Apply);
        };
        let markers = Self::received_schema_manifest_markers(received)?;
        let manifest_key = Self::durable_received_schema_manifest_key(received)?;
        let (manifest, stored_markers) = if let Some(persistence) = self.persistence.as_ref() {
            let manifest =
                persistence.load_config_value::<DurableReceivedSchemaManifest>(&manifest_key)?;
            let mut stored_markers = HashMap::new();
            for marker in &markers {
                if let Some(stored) =
                    persistence.load_config_value::<DurableReceivedDdlMarker>(&marker.key)?
                {
                    stored_markers.insert(marker.key.clone(), stored);
                }
            }
            (manifest, stored_markers)
        } else {
            let state = self.in_memory_received_schema_manifests.lock();
            (
                state.manifests.get(&manifest_key).cloned(),
                markers
                    .iter()
                    .filter_map(|marker| {
                        state
                            .markers
                            .get(&marker.key)
                            .cloned()
                            .map(|stored| (marker.key.clone(), stored))
                    })
                    .collect(),
            )
        };
        if let Some(manifest) = manifest {
            return if manifest.markers == markers {
                Ok(ReceivedSchemaManifestClassification::Replay(
                    manifest.result,
                ))
            } else {
                Err(Error::SyncError(
                    ("received schema retry changes the digest or order of an existing migration"
                        .to_string())
                    .into(),
                ))
            };
        }

        for marker in &markers {
            if let Some(stored) = stored_markers.get(&marker.key)
                && stored.digest != marker.digest
            {
                return Err(Error::SyncError(
                    ("received schema retry changes an existing marker digest".to_string()).into(),
                ));
            }
        }
        let present = stored_markers.len();
        if present == 0 {
            return Ok(ReceivedSchemaManifestClassification::Apply);
        }
        if present == markers.len() {
            return Err(Error::SyncError((
                "received schema migration recombines applied markers without its original manifest"
                    .to_string()).into()));
        }
        Err(Error::SyncError(
            ("received schema migration mixes applied and absent markers".to_string()).into(),
        ))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn received_schema_manifest(
        received: &crate::protocol::ReceivedDdlContext,
        result: &ApplyResult,
    ) -> Result<(String, DurableReceivedSchemaManifest)> {
        let manifest = DurableReceivedSchemaManifest {
            markers: Self::received_schema_manifest_markers(received)?,
            result: crate::protocol::WireApplyResult::from(result.clone()),
        };
        Ok((
            Self::durable_received_schema_manifest_key(received)?,
            manifest,
        ))
    }

    /// Borrow the committed catalog while selecting both directions of every
    /// dependency. Only the selected declarations become owned scratch state.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn selected_received_catalog(
        &self,
        changes: &ChangeSet,
        additional_seeds: impl IntoIterator<Item = String>,
    ) -> Result<(StagedCopyCharge, HashMap<String, TableMeta>)> {
        let committed = self.relational_store.table_meta.read();
        let mut incoming = Vec::new();
        let incoming_bytes = changes.ddl.iter().try_fold(0usize, |bytes, ddl| {
            // Count the wire payload without producing an encoded buffer. The
            // descriptor allowance covers owned containers built by parsing,
            // including structured constraints absent from column spelling.
            let encoded = bincode::serde::encode_into_std_write(
                ddl,
                &mut std::io::sink(),
                bincode::config::standard(),
            )
            .map_err(|error| {
                Error::Other((format!("price received declaration: {error}")).into())
            })?;
            let descriptors = match ddl {
                DdlChange::CreateTable {
                    columns,
                    constraints,
                    foreign_keys,
                    composite_foreign_keys,
                    composite_unique,
                    ..
                }
                | DdlChange::AlterTable {
                    columns,
                    constraints,
                    foreign_keys,
                    composite_foreign_keys,
                    composite_unique,
                    ..
                } => {
                    (columns.len()
                        + constraints.len()
                        + foreign_keys.len()
                        + composite_foreign_keys.len()
                        + composite_unique.len())
                        * 512
                        + composite_foreign_keys
                            .iter()
                            .map(|key| (key.child_columns.len() + key.parent_columns.len()) * 32)
                            .sum::<usize>()
                        + composite_unique
                            .iter()
                            .map(|columns| columns.len() * 32)
                            .sum::<usize>()
                }
                _ => 512,
            };
            Ok::<_, Error>(
                bytes
                    .saturating_add(encoded.saturating_mul(4))
                    .saturating_add(std::mem::size_of_val(ddl))
                    .saturating_add(descriptors),
            )
        })?;
        let incoming_charge = StagedCopyCharge::admit_for_host(
            &self.tx_mgr,
            &self.finalization.accountant,
            contextdb_vector::MemoryBudgetOwner::clone(&self.finalization.vector_accountant),
            incoming_bytes,
            "stage_received_catalog_dependencies",
        )?;
        for ddl in &changes.ddl {
            if let DdlChange::CreateTable {
                name: _,
                columns,
                constraints,
                foreign_keys,
                composite_foreign_keys,
                composite_unique,
            }
            | DdlChange::AlterTable {
                name: _,
                columns,
                constraints,
                foreign_keys,
                composite_foreign_keys,
                composite_unique,
            } = ddl
            {
                // Keep every intermediate dependency, including one removed by
                // a later statement in the same authored unit.
                incoming.push(rough_sync_table_meta(
                    columns,
                    constraints,
                    foreign_keys,
                    composite_foreign_keys,
                    composite_unique,
                ));
            }
        }
        let mut seeds = changes
            .ddl
            .iter()
            .filter_map(Self::ddl_affected_table)
            .map(str::to_owned)
            .collect::<HashSet<_>>();
        seeds.extend(additional_seeds);
        for meta in &incoming {
            seeds.extend(Self::received_table_dependencies(meta).map(str::to_owned));
        }
        let selected =
            Self::received_schema_dependency_closure(changes, &committed, &HashMap::new(), seeds);
        let bytes = selected
            .iter()
            .filter_map(|table| {
                committed.get(table).map(|meta| {
                    table.len()
                        + meta.metadata_bytes()
                        + std::mem::size_of::<(String, TableMeta)>() * 2
                })
            })
            .sum();
        let mut charge = StagedCopyCharge::admit_for_host(
            &self.tx_mgr,
            &self.finalization.accountant,
            contextdb_vector::MemoryBudgetOwner::clone(&self.finalization.vector_accountant),
            bytes,
            "stage_received_catalog",
        )?;
        let catalog = selected
            .into_iter()
            .filter_map(|table| committed.get(&table).map(|meta| (table, meta.clone())))
            .collect();
        drop(incoming);
        // The same admitted allowance now covers the incoming declarations
        // while the caller evolves its selected catalog in source order.
        charge.bytes = charge.bytes.saturating_add(incoming_charge.bytes);
        incoming_charge.transfer();
        Ok((charge, catalog))
    }

    pub(super) fn received_table_dependencies(meta: &TableMeta) -> impl Iterator<Item = &str> {
        meta.columns
            .iter()
            .flat_map(|column| {
                column
                    .references
                    .iter()
                    .map(|reference| reference.table.as_str())
                    .chain(
                        column
                            .rank_policy
                            .iter()
                            .map(|rank| rank.joined_table.as_str()),
                    )
                    .chain(
                        column
                            .acl_ref
                            .iter()
                            .map(|reference| reference.ref_table.as_str()),
                    )
            })
            .chain(
                meta.composite_foreign_keys
                    .iter()
                    .map(|reference| reference.parent_table.as_str()),
            )
            .chain(meta.propagation_rules.iter().filter_map(|rule| match rule {
                PropagationRule::ForeignKey {
                    referenced_table, ..
                } => Some(referenced_table.as_str()),
                _ => None,
            }))
    }

    #[cfg(test)]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub(super) fn project_received_schema_source_order(
        &self,
        ddl: &[DdlChange],
    ) -> Result<ReceivedSchemaSemanticProjection> {
        let changes = ChangeSet {
            ddl: ddl.to_vec(),
            ..ChangeSet::default()
        };
        let (charge, tables) = self.selected_received_catalog(&changes, std::iter::empty())?;
        let mut projection = self.project_received_schema_from_state(
            tables,
            self.received_event_bus_projection(),
            self.trigger.declarations.lock().clone(),
            ddl,
        )?;
        projection.catalog_charge = Some(charge);
        Ok(projection)
    }

    /// Rebuild one schema vector from no prior declarations. Synthetic
    /// snapshots use this to prove that their live schema is exactly the
    /// schema described by durable received-DDL evidence, rather than merely
    /// sharing one old received marker with it.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn project_received_schema_from_empty(
        &self,
        ddl: &[DdlChange],
    ) -> Result<ReceivedSchemaSemanticProjection> {
        self.project_received_schema_from_state(
            HashMap::new(),
            event_bus::EventBusDefinitions::default(),
            BTreeMap::new(),
            ddl,
        )
    }

    /// A schema reconstruction under a history answer's bounds: the
    /// checkpoint runs per statement, and each statement's growth of the
    /// reconstructed schema -- no more than the statement itself carries --
    /// is admitted before it is applied.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn project_received_schema_from_empty_projected(
        &self,
        ddl: &[DdlChange],
        projection: &ChangeProjection<'_>,
    ) -> Result<ReceivedSchemaSemanticProjection> {
        self.project_received_schema_from_state_projected(
            HashMap::new(),
            event_bus::EventBusDefinitions::default(),
            BTreeMap::new(),
            ddl,
            projection,
        )
    }

    // Table shape changes neither consult nor replace event/trigger declarations.
    // Data and DROP TABLE do: the former can fire callbacks, and the latter
    // removes declarations bound to the dropped table in authored order.
    pub(super) fn received_side_metadata_required(changes: &ChangeSet) -> bool {
        !changes.rows.is_empty()
            || !changes.edges.is_empty()
            || !changes.vectors.is_empty()
            || changes.ddl.iter().any(|ddl| {
                !matches!(
                    ddl,
                    DdlChange::CreateTable { .. }
                        | DdlChange::AlterTable { .. }
                        | DdlChange::CreateIndex { .. }
                        | DdlChange::DropIndex { .. }
                )
            })
    }

    /// Admit retained declarations before any projection, replay, or encoded
    /// publication copies exist. Counting serialization borrows the live maps;
    /// the multiplier covers simultaneous semantic, replay and publication
    /// images, their encoded values, and collection node overhead.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn admit_received_side_metadata(
        &self,
        changes: &ChangeSet,
    ) -> Result<StagedCopyCharge> {
        let bytes = if Self::received_side_metadata_required(changes) {
            let event_bytes = self.received_event_bus_projection_bytes()?;
            let trigger_bytes = self.received_trigger_projection_bytes()?;
            let incoming_bytes = changes.ddl.iter().try_fold(0usize, |bytes, ddl| {
                let entries = match ddl {
                    DdlChange::CreateRoute { where_in, .. } => {
                        where_in.as_ref().map_or(0, |(_, values)| values.len())
                    }
                    DdlChange::CreateTrigger { on_events, .. }
                    | DdlChange::CreateTriggerIncludingSync { on_events, .. } => on_events.len(),
                    DdlChange::CreateEventType { .. }
                    | DdlChange::CreateSink { .. }
                    | DdlChange::DropRoute { .. }
                    | DdlChange::DropTrigger { .. }
                    | DdlChange::DropTable { .. } => 0,
                    _ => return Ok::<_, Error>(bytes),
                };
                let encoded = bincode::serde::encode_into_std_write(
                    ddl,
                    &mut std::io::sink(),
                    bincode::config::standard(),
                )
                .map_err(|error| {
                    Error::Other((format!("price received side declaration: {error}")).into())
                })?;
                Ok(bytes
                    .saturating_add(encoded)
                    .saturating_add(512)
                    .saturating_add(entries.saturating_mul(32)))
            })?;
            event_bytes
                .saturating_add(trigger_bytes)
                .saturating_add(incoming_bytes)
                .saturating_mul(16)
        } else {
            0
        };
        StagedCopyCharge::admit_for_host(
            &self.tx_mgr,
            &self.finalization.accountant,
            contextdb_vector::MemoryBudgetOwner::clone(&self.finalization.vector_accountant),
            bytes,
            "stage_received_side_metadata",
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn project_received_schema_from_state(
        &self,
        tables: HashMap<String, TableMeta>,
        event_bus: event_bus::EventBusDefinitions,
        trigger: BTreeMap<String, TriggerDeclaration>,
        ddl: &[DdlChange],
    ) -> Result<ReceivedSchemaSemanticProjection> {
        self.project_received_schema_from_state_projected(
            tables,
            event_bus,
            trigger,
            ddl,
            &ChangeProjection::unbounded(),
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn project_received_schema_from_state_projected(
        &self,
        mut tables: HashMap<String, TableMeta>,
        mut event_bus: event_bus::EventBusDefinitions,
        mut trigger: BTreeMap<String, TriggerDeclaration>,
        ddl: &[DdlChange],
        projection: &ChangeProjection<'_>,
    ) -> Result<ReceivedSchemaSemanticProjection> {
        for change in ddl {
            (projection.checkpoint)()?;
            Self::validate_received_column_declarations(change)?;
        }
        for change in ddl {
            (projection.checkpoint)()?;
            if projection.bounded {
                (projection.admit)(change.estimated_bytes())?;
            }
            Self::validate_sync_table_ddl_against_projected_meta(&tables, change)?;
            Self::apply_sync_table_ddl_to_projected_meta(&mut tables, change);
            Self::validate_projected_foreign_key_schema(&tables)?;
            self.apply_sync_trigger_ddl_to_projection(&mut trigger, change, &tables)?;
            self.apply_received_event_bus_ddl_to_projection(&mut event_bus, change, &tables)?;
        }
        Ok(ReceivedSchemaSemanticProjection {
            catalog_charge: None,
            tables,
            event_bus,
            trigger,
        })
    }

    pub(super) fn received_schema_memory_mirrors(
        &self,
        _local_lsn: Lsn,
        source_order: &[ReceivedSchemaOrderEntry],
        accepted_authors: HashMap<(String, Vec<u8>), String>,
    ) -> ReceivedSchemaMemoryMirrors {
        let mut mirrors = ReceivedSchemaMemoryMirrors::default();
        for entry in source_order {
            mirrors.ddl_generations.insert(
                (entry.local_arrival.local_lsn, entry.persisted_ordinal),
                (
                    entry.generation.table.clone(),
                    entry.generation.table_generation,
                ),
            );
            mirrors.received_ddl_arrivals.insert(
                (entry.local_arrival.local_lsn, entry.persisted_ordinal),
                entry.local_arrival.clone(),
            );
            if let (Some(table), Some(generation)) =
                (&entry.generation.table, entry.generation.table_generation)
            {
                mirrors.table_generations.insert(table.clone(), generation);
            }
        }
        mirrors.accepted_authors.extend(accepted_authors);
        mirrors
    }

    /// Turn the already transport-validated creator sidecars into the durable
    /// row provenance and accepted-author writes for this private stage.
    /// The forwarding peer in `ReceivedDdlContext` is deliberately absent:
    /// a relay is not the immutable author of the row it carries.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn validate_received_schema_lineage_sidecars(
        &self,
        changes: &ChangeSet,
        validated_lineages: &[(String, NaturalKey, Lsn, crate::protocol::WireRowLineage)],
    ) -> Result<()> {
        if validated_lineages.len() != changes.rows.len() {
            return Err(Error::SyncError(
                ("received schema rows require exactly one validated lineage sidecar per row"
                    .to_string())
                .into(),
            ));
        }
        let mut consumed = HashSet::new();
        for row in &changes.rows {
            let matching = validated_lineages
                .iter()
                .enumerate()
                .filter(|(_, (table, natural_key, lsn, _))| {
                    table == &row.table && natural_key == &row.natural_key && *lsn == row.lsn
                })
                .collect::<Vec<_>>();
            if matching.len() != 1 || !consumed.insert(matching[0].0) {
                return Err(Error::SyncError((format!(
                    "received schema row {} {:?} at source LSN {} lacks exactly one validated lineage",
                    row.table, row.natural_key, row.lsn.0
                )).into()));
            }
            let lineage = &matching[0].1.3;
            let expected_root = format!(
                "author:{}:{}:{}",
                lineage.author_node_id,
                lineage.author_database_incarnation.to_hex(),
                lineage.author_local_mutation_position.0,
            );
            if lineage.lineage_root != expected_root {
                return Err(Error::SyncError(
                    ("validated row lineage root does not match its immutable creator tuple"
                        .to_string())
                    .into(),
                ));
            }
        }
        if consumed.len() != validated_lineages.len() {
            return Err(Error::SyncError(
                ("received schema lineage sidecar does not match any retained row".to_string())
                    .into(),
            ));
        }
        Ok(())
    }

    // Vector publications admit their own directory, payload and graph credits.
    // This engine image swap prices only the relational/graph state released by
    // RetainedNonVectorMemoryOwner; unchanged vector holders require no transfer.
    pub(super) fn received_schema_replacement_bytes<E: GraphRetainedBytes>(
        &self,
        tables: &HashMap<String, TableMeta>,
        rows: &HashMap<String, Vec<VersionedRow>>,
        edges: &[E],
    ) -> usize {
        let metadata = tables.values().fold(0usize, |bytes, meta| {
            bytes.saturating_add(retained_table_metadata_bytes(&self.relational_store, meta))
        });
        let rows = rows.iter().fold(0usize, |bytes, (table, rows)| {
            bytes.saturating_add(tables.get(table).map_or(0, |meta| {
                self.received_schema_table_rows_bytes(table, meta, rows)
            }))
        });
        let edges = if self.graph_store.is_source_backed() {
            0
        } else {
            edges.iter().fold(0usize, |bytes, edge| {
                bytes.saturating_add(edge.retained_graph_bytes())
            })
        };
        metadata.saturating_add(rows).saturating_add(edges)
    }

    pub(super) fn received_schema_table_rows_bytes(
        &self,
        table: &str,
        meta: &TableMeta,
        rows: &[VersionedRow],
    ) -> usize {
        if self.relational_store.uses_disk_backed_committed_bodies() {
            return 0;
        }
        rows.iter()
            .filter(|row| {
                row.deleted_tx.is_none()
                    || self
                        .relational_store
                        .is_retention_expired(table, row.row_id, row.created_tx)
            })
            .fold(0usize, |bytes, row| {
                bytes.saturating_add(retained_row_bytes_for_meta(table, &row.values, meta, true))
            })
    }

    pub(super) fn received_schema_dependency_closure(
        changes: &ChangeSet,
        before: &HashMap<String, TableMeta>,
        after: &HashMap<String, TableMeta>,
        additional_seeds: impl IntoIterator<Item = String>,
    ) -> HashSet<String> {
        let mut affected = changes
            .ddl
            .iter()
            .filter_map(Self::ddl_affected_table)
            .map(str::to_string)
            .chain(changes.rows.iter().map(|row| row.table.clone()))
            .chain(
                changes
                    .vectors
                    .iter()
                    .map(|vector| vector.index.table.clone()),
            )
            .collect::<HashSet<_>>();
        affected.extend(additional_seeds);
        loop {
            let mut added = false;
            for (table, meta) in before.iter().chain(after.iter()) {
                if affected.contains(table)
                    || Self::received_table_dependencies(meta).any(|name| affected.contains(name))
                {
                    added |= affected.insert(table.clone());
                    for referenced in Self::received_table_dependencies(meta) {
                        added |= affected.insert(referenced.to_owned());
                    }
                }
            }
            if !added {
                break;
            }
        }
        affected
    }

    /// Clone only declarations needed to validate locally authored CREATE /
    /// ALTER ADD / trigger DDL. Row, edge, and vector payloads remain solely
    /// in the live database and are never duplicated by a long-lived BEGIN.
    ///
    /// The image runs on the live database's own accountant: the Legacy host
    /// keeps the live writer registration through `from_handle`, the owned
    /// host keeps its Original. Everything it holds is admitted by the ordinary
    /// allocator before it is allocated.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn detached_local_schema_working_database(&self) -> Result<Database> {
        let working = if matches!(self.tx_mgr, DatabaseTransactionHost::Owned(_)) {
            self.with_preparation_original(|original| {
                Database::open_private_working_image(
                    Arc::new(CorePlugin),
                    WriterBudget::from_original(original)?,
                    self.declared_read_memory.clone(),
                    None,
                    None,
                    None,
                )
            })?
        } else {
            // A private image, not a database a caller opened: it is built
            // directly on the live writer, never through `:memory:`, which
            // names a durable scratch database of its own.
            Database::open_private_working_image(
                Arc::new(CorePlugin),
                WriterBudget::from_handle(crate::memory_accounting::AccountantHandle::clone(
                    &self.finalization.accountant,
                ))?,
                crate::vector_read_memory::DeclaredReadMemory::from_reader_limit(
                    OwnerReadConfig::default().limits.limits.memory,
                )?,
                None,
                None,
                None,
            )?
        };
        working
            .finalization
            .local_policy_metadata_enabled
            .store(false, Ordering::SeqCst);
        // A private schema image never owns a maintenance schedule.
        working
            .maintenance_caller_driven
            .store(true, Ordering::SeqCst);
        let read_memory = working.declared_reader_memory()?;
        // The image's retained-state owner returns its metadata fold when it
        // drops. Admit exactly that fold before the clone exists, so a shortage
        // refuses before anything is copied.
        let metadata_bytes =
            self.relational_store
                .table_meta
                .read()
                .values()
                .fold(0usize, |bytes, meta| {
                    bytes.saturating_add(retained_table_metadata_bytes(
                        &working.relational_store,
                        meta,
                    ))
                });
        working.finalization.accountant.try_allocate_for(
            metadata_bytes,
            "ddl",
            "local_schema_stage",
            "Reduce the schema transaction size or raise MEMORY_LIMIT before retrying.",
        )?;
        // Until the table image holds the fold, this guard owns it: any
        // failure before the tables are installed returns the fold.
        let mut fold = PreadmittedMetadataFold {
            accountant: &working.finalization.accountant,
            bytes: metadata_bytes,
        };
        #[cfg(test)]
        super::statement_execution::local_schema_test_seams::preparation_fault_at(
            super::statement_execution::local_schema_test_seams::PreparationFault::AfterFoldBeforeTables,
        )?;
        let metadata = self.relational_store.table_meta.read().clone();
        let mut vector_copy_owner = crate::vector_read_memory::admission(read_memory.clone());
        for (table, meta) in &metadata {
            working.relational_store.create_table(table, meta.clone());
            for index in &meta.indexes {
                if index.kind == IndexKind::Auto {
                    working.relational_store.create_exact_index_storage(
                        table,
                        &index.name,
                        index.columns.clone(),
                    );
                } else {
                    working.relational_store.create_index_storage(
                        table,
                        &index.name,
                        index.columns.clone(),
                    );
                }
            }
        }
        // The table image now holds the fold; its retained-state owner
        // returns it when the image drops.
        fold.transfer_to_image();
        drop(fold);
        // The admitted fold must be exactly what the image now holds, or its
        // drop would refund a different figure than was admitted.
        let held_bytes =
            working
                .relational_store
                .table_meta
                .read()
                .values()
                .fold(0usize, |bytes, meta| {
                    bytes.saturating_add(retained_table_metadata_bytes(
                        &working.relational_store,
                        meta,
                    ))
                });
        if held_bytes != metadata_bytes {
            return Err(Error::Other(
                "private schema image holds a different metadata fold than it admitted".into(),
            ));
        }
        // Register declarations only; row/vector payloads stay live until
        // bounded commit preparation.
        for (table, meta) in metadata {
            for column in &meta.columns {
                working.register_vector_index_for_column(
                    &table,
                    column,
                    &mut vector_copy_owner,
                    read_memory.clone(),
                )?;
            }
        }
        #[cfg(test)]
        super::statement_execution::local_schema_test_seams::preparation_fault_at(
            super::statement_execution::local_schema_test_seams::PreparationFault::AfterRegistration,
        )?;
        drop(vector_copy_owner);
        drop(read_memory);
        Ok(working)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn new_received_metadata_copy_charges(&self) -> Result<MetadataCopyCharges> {
        match &self.tx_mgr {
            DatabaseTransactionHost::Legacy(_) => {
                Ok(MetadataCopyCharges::new(&self.finalization.accountant))
            }
            DatabaseTransactionHost::Owned(handle) => handle.with_host(|host| {
                host.events
                    .with_owned_persistence_original(MetadataCopyCharges::from_original)
            }),
        }
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn received_metadata_copy_charges_from_array<const N: usize>(
        &self,
        charges: [StagedCopyCharge; N],
    ) -> Result<MetadataCopyCharges> {
        match &self.tx_mgr {
            DatabaseTransactionHost::Legacy(_) => {
                MetadataCopyCharges::from_array(&self.finalization.accountant, charges)
            }
            DatabaseTransactionHost::Owned(handle) => handle.with_host(|host| {
                host.events.with_owned_persistence_original(|original| {
                    MetadataCopyCharges::from_original_array(original, charges)
                })
            }),
        }
    }

    /// Copy the affected state into a private adjudicator with its own
    /// transaction manager and row-id cursor, on the same unpublished live
    /// write production uses, and run `inspect` while that write's working
    /// source is current for the image. The door keeps custody of the write
    /// and its preparation owner for the whole inspection, then drops the
    /// image, its metadata charges, the write (an abort) and the owner, in
    /// that order, before it returns.
    #[cfg(test)]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub(super) fn with_detached_received_schema_working_database<R>(
        &self,
        projected_tables: &HashMap<String, TableMeta>,
        affected_tables: &HashSet<String>,
        affected_graph_edge_types: &HashSet<String>,
        copy_side_metadata: bool,
        terminal_context: Option<&TerminalRefusalPullContext>,
        receipt: Option<&SyncApplyReceipt>,
        push_outcome_key: Option<&SyncPushOutcomeContext>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: contextdb_core::read_memory::ReadMemoryOwner,
        inspect: impl FnOnce(&MetadataCopyCharges, &Database) -> Result<R>,
    ) -> Result<R> {
        self.with_writer_preparation_original(|original| {
            let owner =
                crate::persistence::received_working::ReceivedPreparationOwner::prepare(original)?;
            let original = owner.original();
            let store = self.persistence.as_ref().ok_or_else(|| {
                Error::Other("received working image fixture requires a durable store".into())
            })?;
            let mut write =
                crate::persistence::received_working::ReceivedWorkingWrite::begin(store, original)?;
            write.retain_preparation_owner(&owner)?;
            let root = self.writer_read_root()?.ok_or_else(|| {
                Error::Other("received working image fixture lost its captured publication".into())
            })?;
            write.prepare_vectors(store, original, root.clone(), read_memory.clone())?;
            let catalog =
                crate::persistence::received_working_vector::WorkingVectorCatalogOwner::prepare(
                    original,
                    owner.funder(),
                )?;
            write.install_vector_catalog(original, catalog.clone())?;
            let (metadata_copy_charges, working) = self
                .detached_received_schema_working_database_with_sources(
                    projected_tables,
                    self.tx_mgr.peek_next_tx(),
                    original,
                    FundedWorkingGraphSource::prepare(owner.funder())?,
                    catalog,
                    affected_tables,
                    copy_side_metadata,
                    terminal_context,
                    &[],
                    receipt,
                    push_outcome_key,
                    vector_copy_owner,
                    read_memory.clone(),
                )?;
            write.prepare_graph(store, original, root.clone())?;
            for kind in affected_graph_edge_types {
                write.select_graph_type(original, kind)?;
            }
            let snapshot = working.tx_mgr.capture_published_read().snapshot;
            let lsn = root.lsn;
            let result = write.with_source(
                original,
                store,
                &working.relational_store,
                affected_tables,
                working.tx_mgr.identity(),
                working.id,
                root,
                snapshot,
                lsn,
                |_| inspect(&metadata_copy_charges, &working),
            );
            drop(working);
            drop(metadata_copy_charges);
            drop(write);
            drop(owner);
            result
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn detached_received_schema_working_database_with_sources(
        &self,
        projected_tables: &HashMap<String, TableMeta>,
        receiver_tx: TxId,
        original: &crate::memory_accounting::OwnedMemoryReservation,
        working_graph: FundedWorkingGraphSource,
        working_vectors: crate::persistence::received_working_vector::WorkingVectorCatalogOwner,
        affected_tables: &HashSet<String>,
        copy_side_metadata: bool,
        terminal_context: Option<&TerminalRefusalPullContext>,
        terminal_rows: &[RowChange],
        receipt: Option<&SyncApplyReceipt>,
        push_outcome_key: Option<&SyncPushOutcomeContext>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<(MetadataCopyCharges, Database)> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();
        let _decode_scope = crate::read_image_memory::ImageMemory::operation_reader(
            crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
            read_memory.clone(),
        )?
        .enter();

        // The copy this worker makes is the widest point a receiver ever
        // reaches -- for a moment it holds the old image and the arriving one
        // at once -- and an operator's declared MEMORY_LIMIT is entitled to
        // bound THAT, not just the shape the unit settles to. A worker with
        // its own accountant puts the whole copy somewhere no declared limit
        // can reach. It shares the receiver's.
        //
        // Sharing it is only half the answer: `RetainedNonVectorMemoryOwner`
        // releases the fold over everything this worker holds when it drops,
        // so every helper that installs a copy below has to admit exactly what
        // that fold will give back. Otherwise a refusal after copying
        // subtracts bytes from the live receiver's bill that nothing added.
        let mut metadata_copy_charges = self.new_received_metadata_copy_charges()?;
        // Copied committed versions must be visible before the first private
        // commit. Fresh private transactions also need identities beyond all
        // copied versions so later interval restamping cannot confuse them.
        // Rows, edges and vectors stay in the live store's unpublished write;
        // the image reads them through its working source.
        let mut working = Database::open_private_working_image(
            Arc::new(CorePlugin),
            WriterBudget::from_original(original)?,
            self.declared_read_memory.clone(),
            Some((receiver_tx, self.tx_mgr.current_tx_max())),
            Some(working_graph),
            Some(working_vectors),
        )?;
        // Replay enforces the receiving handle's row visibility, including
        // context-scoped pulls; a private image must not widen it to admin.
        let mut access_bytes = event_bus::owned_event_btree_bytes::<ContextId, ()>(
            self.access.contexts.as_ref().map_or(0, BTreeSet::len),
        )?
        .saturating_add(event_bus::owned_event_btree_bytes::<ScopeLabel, ()>(
            self.access.scope_labels.as_ref().map_or(0, BTreeSet::len),
        )?);
        if let Some(labels) = &self.access.scope_labels {
            for label in labels {
                access_bytes = access_bytes.saturating_add(label.0.len());
            }
        }
        if let Some(Principal::Agent(id) | Principal::Human(id)) = &self.access.principal {
            access_bytes = access_bytes.saturating_add(id.len());
        }
        metadata_copy_charges.push(StagedCopyCharge::admit_for_host(
            &self.tx_mgr,
            &self.finalization.accountant,
            contextdb_vector::MemoryBudgetOwner::clone(&self.finalization.vector_accountant),
            access_bytes,
            "stage_received_access",
        )?)?;
        working.access = AccessConstraints {
            contexts: self.access.contexts.as_ref().map(|contexts| {
                let mut copied = BTreeSet::new();
                for context in contexts {
                    copied.insert(context.clone());
                }
                copied
            }),
            scope_labels: self.access.scope_labels.as_ref().map(|labels| {
                let mut copied = BTreeSet::new();
                for label in labels {
                    copied.insert(label.clone());
                }
                copied
            }),
            principal: self.access.principal.clone(),
        };
        // An unpublished schema image never owns a maintenance schedule.
        working
            .maintenance_caller_driven
            .store(true, Ordering::SeqCst);
        working
            .relational_store
            .enable_disk_backed_committed_bodies();
        // This unpublished memory store retains tombstoned bodies until its
        // final projection is spooled. Move their existing release invoice
        // into that same standing owner rather than refunding live history.
        working
            .finalization
            .non_vector_memory
            .lock()
            .as_mut()
            .ok_or_else(|| {
                Error::Other("private received image lost its original row owner".into())
            })?
            .defer_private_row_refunds = true;
        let generation_bytes = affected_tables.iter().map(|table| table.len() + 128).sum();
        metadata_copy_charges.push(StagedCopyCharge::admit_for_host(
            &self.tx_mgr,
            &self.finalization.accountant,
            contextdb_vector::MemoryBudgetOwner::clone(&self.finalization.vector_accountant),
            generation_bytes,
            "stage_received_generations",
        )?)?;
        let mut authoritative_table_generations = HashMap::new();
        for table in affected_tables {
            let has_schema = self.relational_store.table_meta.read().contains_key(table);
            let generation = if let Some(persistence) = &self.persistence {
                let key = Self::durable_lineage_table_generation_key(table);
                match persistence.load_config_value_raw(&key)? {
                    Some(raw) => {
                        let generation = RedbPersistence::decode_config_value_exact::<u64>(&raw)
                            .map_err(|error| crate::funded_error::corruption_display(Ok(read_memory.clone()), &key, format_args!(
                                    "table-generation value is not an exact canonical u64: {error}"
                                )))?;
                        if generation == 0
                            || RedbPersistence::encode_config_value(&generation)? != raw
                        {
                            return Err(crate::funded_error::corruption_text(
                                Ok(read_memory.clone()),
                                &key,
                                "table-generation value is zero or noncanonical".into(),
                            ));
                        }
                        generation
                    }
                    None if has_schema => {
                        return Err(crate::funded_error::corruption_display(
                            Ok(read_memory.clone()),
                            &key,
                            format_args!(
                                "table {table} has schema metadata but no authoritative generation"
                            ),
                        ));
                    }
                    None => 0,
                }
            } else if has_schema {
                self.durable_lineage_table_generation(table, read_memory.clone())?
            } else {
                self.authoritative_table_generation_counter(table)?
            };
            if generation != 0 {
                authoritative_table_generations.insert(table.clone(), generation);
            }
        }
        // Construction already seeded the copied image's committed watermark
        // and allocator beyond the retained versions. Do not perform a private
        // metadata commit before its graph/vector source loan is installed.
        *working.in_memory_table_generations.lock() = authoritative_table_generations;
        // Reserve metadata before payload copying, but materialize it only
        // after rows and vectors have passed their own admission doors. A
        // refused payload copy never needs an owned provenance snapshot.
        let prepare_metadata =
            |admit_only: bool, metadata_copy_charges: &mut MetadataCopyCharges| -> Result<()> {
                // Each source lock is borrowed through selection and admission. The
                // guards remain owned by the stage until publication or abandonment.
                macro_rules! copy_selected {
                    ($source:expr, $keep:expr) => {{
                        let source = $source;
                        let keep = $keep;
                        if admit_only {
                            let bytes = source.iter().filter(|entry| keep(entry)).try_fold(
                                0usize,
                                |bytes, entry| {
                                    bincode::serde::encode_into_std_write(
                                        entry,
                                        &mut std::io::sink(),
                                        bincode::config::standard(),
                                    )
                                    .map(|encoded| {
                                        bytes
                                            .saturating_add(encoded.saturating_mul(4))
                                            .saturating_add(256)
                                    })
                                    .map_err(|error| {
                                        Error::Other(
                                            (format!("price received metadata: {error}")).into(),
                                        )
                                    })
                                },
                            )?;
                            metadata_copy_charges.push(StagedCopyCharge::admit_for_host(
                                &self.tx_mgr,
                                &self.finalization.accountant,
                                contextdb_vector::MemoryBudgetOwner::clone(
                                    &self.finalization.vector_accountant,
                                ),
                                bytes,
                                "stage_received_metadata",
                            )?)?;
                            Default::default()
                        } else {
                            source
                                .iter()
                                .filter(|entry| keep(entry))
                                .map(|(key, value)| (key.clone(), value.clone()))
                                .collect()
                        }
                    }};
                }
                if !admit_only {
                    let source = self.in_memory_ddl_generations.lock();
                    let prepared = working.prepare_memory_history_entries(
                        std::iter::empty(),
                        source
                            .iter()
                            .filter(|(_, (table, _))| {
                                table
                                    .as_ref()
                                    .is_some_and(|table| affected_tables.contains(table))
                            })
                            .map(|(key, (table, generation))| (*key, (table, *generation))),
                    )?;
                    working.replace_memory_schema_generations(prepared);
                }
                let selected_ddl = self.in_memory_ddl_generations.lock();
                *working.received_ddl_arrivals.write() = copy_selected!(
                    self.received_ddl_arrivals.read(),
                    |entry: &(&(Lsn, u32), &DurableReceivedDdlArrival)| selected_ddl
                        .get(entry.0)
                        .and_then(|(table, _)| table.as_ref())
                        .is_some_and(|table| affected_tables.contains(table))
                );
                *working.superseded_ddl_occurrences.write() = copy_selected!(
                    self.superseded_ddl_occurrences.read(),
                    |entry: &(&(Lsn, u32), &DurableSupersededDdlOccurrence)| selected_ddl
                        .get(entry.0)
                        .and_then(|(table, _)| table.as_ref())
                        .is_some_and(|table| affected_tables.contains(table))
                );
                drop(selected_ddl);
                {
                    let sources = self.sync_tombstone_arrivals.read();
                    if admit_only {
                        let bytes = sources
                            .iter()
                            .filter(|((table, _), _)| affected_tables.contains(table))
                            .map(|((table, key), _)| table.len() + key.len() + 256)
                            .sum();
                        metadata_copy_charges.push(StagedCopyCharge::admit_for_host(
                            &self.tx_mgr,
                            &self.finalization.accountant,
                            contextdb_vector::MemoryBudgetOwner::clone(
                                &self.finalization.vector_accountant,
                            ),
                            bytes,
                            "stage_received_sources",
                        )?)?;
                    } else {
                        *working.sync_tombstone_arrivals.write() = sources
                            .iter()
                            .filter(|((table, _), _)| affected_tables.contains(table))
                            .map(|(key, value)| (key.clone(), *value))
                            .collect();
                    }
                }
                *working.accepted_sync_row_authors.write() = copy_selected!(
                    self.accepted_sync_row_authors.read(),
                    |entry: &(&(String, Vec<u8>), &String)| affected_tables.contains(&entry.0.0)
                );
                *working.terminal_refusal_markers.write() = copy_selected!(
                    self.terminal_refusal_markers.read(),
                    |entry: &(&TerminalRefusalMarkerKey, &TerminalRefusalMarkerRecord)| {
                        affected_tables.contains(&entry.0.2)
                    }
                );
                let context_key = terminal_context.map(Self::terminal_refusal_context_key);
                *working.terminal_refusal_scans.write() = copy_selected!(
                    self.terminal_refusal_scans.read(),
                    |entry: &(&TerminalRefusalContextKey, &TerminalRefusalScanState)| context_key
                        .as_ref()
                        == Some(entry.0)
                );
                if let Some(context) = terminal_context {
                    // The unpublished receiver has no persistence handle. Seed
                    // its existing adjudicator from the receiver's captured
                    // durable context, with the same original copy admission.
                    // The private image needs only this unit's marker keys;
                    // unrelated refusal history stays in its durable source.
                    for row in terminal_rows {
                        let config_key = Self::terminal_refusal_marker_config_key(
                            context,
                            &row.table,
                            &row.natural_key,
                        );
                        let Some(record) = self.read_config_value::<TerminalRefusalMarkerRecord>(
                            &config_key,
                            read_memory.clone(),
                        )?
                        else {
                            continue;
                        };
                        if record.table != row.table || record.natural_key != row.natural_key {
                            return Err(Error::SyncError(
                                "terminal marker has a mismatched row identity".into(),
                            ));
                        }
                        if admit_only {
                            let encoded = bincode::serde::encode_into_std_write(
                                &record,
                                &mut std::io::sink(),
                                bincode::config::standard(),
                            )
                            .map_err(|error| {
                                Error::Other(format!("price terminal marker: {error}").into())
                            })?;
                            metadata_copy_charges.push(StagedCopyCharge::admit_for_host(
                                &self.tx_mgr,
                                &self.finalization.accountant,
                                contextdb_vector::MemoryBudgetOwner::clone(
                                    &self.finalization.vector_accountant,
                                ),
                                config_key
                                    .len()
                                    .saturating_add(encoded)
                                    .saturating_mul(4)
                                    .saturating_add(256),
                                "stage_received_metadata",
                            )?)?;
                        } else {
                            let key = Self::terminal_refusal_marker_key(
                                context,
                                &record.table,
                                &record.natural_key,
                            );
                            working.terminal_refusal_markers.write().insert(key, record);
                        }
                    }
                    if let Some(scan) = self.read_config_value::<TerminalRefusalScanState>(
                        &Self::terminal_refusal_scan_config_key(context),
                        read_memory.clone(),
                    )? {
                        if admit_only {
                            let encoded = bincode::serde::encode_into_std_write(
                                &scan,
                                &mut std::io::sink(),
                                bincode::config::standard(),
                            )
                            .map_err(|error| {
                                Error::Other(format!("price terminal scan: {error}").into())
                            })?;
                            metadata_copy_charges.push(StagedCopyCharge::admit_for_host(
                                &self.tx_mgr,
                                &self.finalization.accountant,
                                contextdb_vector::MemoryBudgetOwner::clone(
                                    &self.finalization.vector_accountant,
                                ),
                                context
                                    .tenant_id
                                    .as_str()
                                    .len()
                                    .saturating_add(context.hub_node_id.len())
                                    .saturating_add(encoded)
                                    .saturating_mul(4)
                                    .saturating_add(256),
                                "stage_received_metadata",
                            )?)?;
                        } else {
                            working
                                .terminal_refusal_scans
                                .write()
                                .insert(Self::terminal_refusal_context_key(context), scan);
                        }
                    }
                }
                let receipt_keys = receipt.map(|receipt| {
                    [
                        Self::applied_push_watermark_node_incarnation_key(
                            &receipt.tenant_id,
                            &receipt.node_id,
                            receipt.incarnation,
                        ),
                        receipt.tenant_id.as_str().to_string(),
                    ]
                });
                *working.in_memory_applied_push_watermarks.lock() = copy_selected!(
                    self.in_memory_applied_push_watermarks.lock(),
                    |entry: &(&String, &Lsn)| receipt_keys
                        .as_ref()
                        .is_some_and(|keys| keys.contains(entry.0))
                );
                let lineage = self.lineage_state_lock.lock();
                let identity_prefixes = affected_tables
                    .iter()
                    .map(|table| {
                        let mut identity = (table.len() as u64).to_be_bytes().to_vec();
                        identity.extend_from_slice(table.as_bytes());
                        Self::hex_component(&identity)
                    })
                    .collect::<Vec<_>>();
                let selected_sidecar = |key: &str| {
                    key.rsplit('.').next().is_some_and(|identity| {
                        identity_prefixes
                            .iter()
                            .any(|prefix| identity.starts_with(prefix))
                    })
                };
                let mut selected_lineage = working.lineage_state_lock.lock();
                selected_lineage.unbound_creations = copy_selected!(
                    &lineage.unbound_creations,
                    |entry: &(&String, &DurableUnboundCreationLineage)| affected_tables
                        .contains(&entry.1.table)
                );
                selected_lineage.row_sidecars = copy_selected!(
                    &lineage.row_sidecars,
                    |entry: &(&String, &DurableRowLineageSidecar)| { selected_sidecar(entry.0) }
                );
                selected_lineage.records = copy_selected!(&lineage.records, |entry: &(
                    &String,
                    &DurableLineageRecord
                )| {
                    affected_tables.contains(&entry.1.table)
                });
                selected_lineage.purged_history = copy_selected!(
                    &lineage.purged_history,
                    |entry: &(&String, &DurableLineageRecord)| {
                        affected_tables.contains(&entry.1.table)
                    }
                );
                selected_lineage.push_outcomes = copy_selected!(
                    &lineage.push_outcomes,
                    |entry: &(&String, &DurableSyncPushOutcome)| {
                        push_outcome_key.is_some_and(|outcome| outcome.key.storage_key == *entry.0)
                    }
                );
                drop(selected_lineage);
                drop(lineage);
                if !admit_only && copy_side_metadata {
                    *working.trigger.declarations.lock() = self.trigger.declarations.lock().clone();
                    *working.trigger.callbacks.write() = self.trigger.callbacks.read().clone();
                    working
                        .trigger
                        .ready
                        .store(self.trigger.ready.load(Ordering::SeqCst), Ordering::SeqCst);
                }
                Ok(())
            };
        prepare_metadata(true, &mut metadata_copy_charges)?;
        {
            let metadata = self.relational_store.table_meta.read();
            for table in affected_tables {
                let Some(meta) = metadata.get(table) else {
                    continue;
                };
                #[cfg(any(test, feature = "test-seams"))]
                record_received_schema_staged_table_image_for_test(table);
                // The same figure `RetainedNonVectorMemoryOwner` releases for a
                // copied declaration, admitted before the copy exists.
                self.finalization.accountant.try_allocate_for(
                    retained_table_metadata_bytes(&working.relational_store, meta),
                    "sync",
                    "stage_received_unit_table_image",
                    STAGE_RECEIVED_UNIT_COPY_HINT,
                )?;
                working.relational_store.create_table(table, meta.clone());
                for index in &meta.indexes {
                    // Budgeted constructors: index membership images built during
                    // the copy are admitted node by node as postings arrive
                    // (`try_insert_posting` below), the same door
                    // `Database::open` already uses to rebuild indexes from a
                    // persisted image. The unbudgeted constructors this replaced
                    // built the images for free.
                    if index.kind == IndexKind::Auto {
                        working
                            .relational_store
                            .create_auto_index_storage_with_budget(
                                table,
                                &index.name,
                                index.columns.clone(),
                                self.finalization.accountant.read_budget()?,
                            );
                    } else {
                        working.relational_store.create_index_storage_with_budget(
                            table,
                            &index.name,
                            index.columns.clone(),
                            self.finalization.accountant.read_budget()?,
                        );
                    }
                }
            }
        }
        working
            .relational_store
            .set_next_row_id(self.relational_store.next_row_id());
        let sources = self
            .relational_store
            .sync_source_sidecars_for_tables(affected_tables);
        working.relational_store.set_sync_source_lsns(
            sources
                .iter()
                .map(|((table, row_id), (lsn, _))| (table.clone(), *row_id, *lsn)),
        );
        working.relational_store.set_sync_source_kinds(
            sources
                .into_iter()
                .map(|((table, row_id), (_, kind))| (table, row_id, kind)),
        );
        // Schema planning must preserve empty vector declarations too. Build
        // their complete layouts from the copied ColumnDefs. A registry entry
        // whose declaration no longer exists is retired state, not schema;
        // do not smuggle it into the detached image as an implicit index.
        {
            let metadata = working.relational_store.table_meta.read();
            register_vector_layouts(
                &working.vector_store,
                &metadata,
                contextdb_vector::MemoryBudgetOwner::clone(&working.finalization.vector_accountant),
                vector_copy_owner,
                read_memory.clone(),
            )?;
        }
        register_vector_layouts(
            &working.vector_store,
            projected_tables,
            contextdb_vector::MemoryBudgetOwner::clone(&working.finalization.vector_accountant),
            vector_copy_owner,
            read_memory.clone(),
        )?;
        self.copy_received_row_graph_witnesses(&working, affected_tables, read_memory.clone())?;
        // Replacement consumes the prepaid copy admission and adopts the
        // retained payload charges before publishing the detached image.
        prepare_metadata(false, &mut metadata_copy_charges)?;
        Ok((metadata_copy_charges, working))
    }

    /// Copy canonical bodies with their exact source placement records.
    /// The returned executing credit remains attached through publication.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn copy_partitioned_vector_membership_image(
        &self,
        matches_index: impl Fn(&VectorIndexRef) -> bool,
        owner: contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<(
        Vec<PartitionedVectorEntry>,
        contextdb_vector::VectorCopyAdmission,
    )> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let (_, source_root) = self.capture_writer_statement()?;
        let result = if let Some(root) = source_root {
            use std::ops::Bound;
            let mut owner = owner;
            let mut entries = Vec::new();
            let mut memberships = root.cursor(
                crate::persistence::writer_source::SourceWindowOwner::Read(read_memory.clone()),
                redb::TableHandle::name(&crate::persistence::VECTOR_PARTITION_MEMBERSHIP_TABLE),
                crate::persistence::writer_source::WriterCursorFamily::Bytes,
                Bound::Unbounded,
                Bound::Unbounded,
                false,
            )?;
            while let Some(placement) = memberships.next_membership()? {
                let membership = &placement.value;
                if !matches_index(&membership.index) {
                    continue;
                }
                let body = root.export_vector_body(
                    crate::persistence::writer_source::SourceWindowOwner::Read(read_memory.clone()),
                    membership,
                )?;
                owner.reserve(
                    body.value
                        .vector
                        .len()
                        .saturating_mul(std::mem::size_of::<f32>())
                        .saturating_add(body.value.index.table.len())
                        .saturating_add(body.value.index.column.len())
                        .saturating_add(membership.partition_key.estimated_bytes())
                        .saturating_add(std::mem::size_of::<PartitionedVectorEntry>())
                        .saturating_mul(4),
                )?;
                let canonical_deleted_tx = body.value.deleted_tx;
                let mut entry = body.value.clone();
                entry.deleted_tx = membership.deleted_tx;
                entries.push(PartitionedVectorEntry::with_membership(
                    membership.partition_key.clone(),
                    entry,
                    contextdb_vector::store::VectorMembershipProvenance::new(
                        membership.row_created_tx,
                        membership.row_lsn,
                        membership.visible_from,
                        membership.deleted_tx,
                    ),
                    canonical_deleted_tx,
                ));
            }
            (entries, owner)
        } else {
            self.vector_store
                .try_resident_partitioned_entries_with_membership_matching(
                    owner,
                    |index, _layout| matches_index(index),
                )?
                .into_parts()
        };
        Ok(result)
    }

    /// The detached executor is allowed to choose rows and conflict outcomes,
    /// but its transaction manager starts at one.  Before its image becomes a
    /// received-schema stage, restamp every mutation it finalized with the
    /// caller-reserved receiver commit identity.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn restamp_detached_received_schema_commit(
        working: &Database,
        finalized: &mut WriteSet,
        local_lsn: Lsn,
        receiver_tx: TxId,
        fallback_arrival_tombstones: &HashSet<(String, String)>,
        connected: Option<(&ConnectedCoordinates, &ConnectedOverlay)>,
        accepted_positions: &BTreeMap<(String, RowId), Lsn>,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<ReceivedRestampedVectorImage> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        Self::restamp_received_row_graph(
            working,
            finalized,
            local_lsn,
            receiver_tx,
            accepted_positions,
            read_memory.clone(),
        )?;
        let local_tx = receiver_tx;
        let detached_lsn = finalized.commit_lsn;
        // The identity each delete below currently names, captured BEFORE the
        // stamps are rewritten. An ordinary update emits a delete of the old
        // version and an insert of the new one under ONE row id, so "the newest
        // version with this row id" names the version the insert just wrote,
        // not the version the delete meant. The detached transaction that
        // stamped the old version is what tells them apart.
        // These are scalar BEFORE coordinates, never a second row/vector
        // body image. Keep their explicit operation owner through the final
        // consumer; all names/control are admitted before any snapshot copy.
        let _coordinates = contextdb_core::read_memory::ReadCredit::try_new(
            read_memory.clone(),
            finalized
                .relational_deletes
                .iter()
                .map(|(table, _, _)| table.len().saturating_add(256))
                .chain(
                    finalized
                        .relational_inserts
                        .iter()
                        .map(|(table, _)| table.len().saturating_add(256)),
                )
                .chain(finalized.vector_inserts.iter().map(|entry| {
                    entry
                        .index
                        .table
                        .len()
                        .saturating_add(entry.index.column.len())
                        .saturating_add(256)
                }))
                .chain(finalized.vector_deletes.iter().map(|(index, _, _)| {
                    index
                        .table
                        .len()
                        .saturating_add(index.column.len())
                        .saturating_add(256)
                }))
                .chain(
                    finalized
                        .adj_inserts
                        .iter()
                        .map(|entry| entry.edge_type.len().saturating_add(256)),
                )
                .chain(
                    finalized
                        .adj_deletes
                        .iter()
                        .map(|(_, kind, _, _)| kind.len().saturating_add(256)),
                )
                .fold(0usize, usize::saturating_add),
            "sync",
            "private_commit_coordinates",
            "Raise the declared reader memory limit to restamp accepted history.",
        )?;
        let deleted_rows = finalized.relational_deletes.clone();
        let detached_inserted_rows = finalized
            .relational_inserts
            .iter()
            .map(|(table, row)| (table.clone(), row.row_id, row.created_tx, row.lsn))
            .collect::<Vec<_>>();
        let detached_inserted_vectors = finalized
            .vector_inserts
            .iter()
            .map(|entry| {
                (
                    entry.index.clone(),
                    entry.row_id,
                    entry.created_tx,
                    entry.lsn,
                )
            })
            .collect::<Vec<_>>();
        let detached_deleted_vectors = finalized.vector_deletes.clone();
        let detached_inserted_edges = finalized
            .adj_inserts
            .iter()
            .map(|entry| {
                (
                    entry.source,
                    entry.edge_type.clone(),
                    entry.target,
                    entry.created_tx,
                    entry.lsn,
                )
            })
            .collect::<Vec<_>>();
        let detached_deleted_edges = finalized.adj_deletes.clone();
        finalized.commit_lsn = Some(local_lsn);
        finalized.finalized_tx = Some(receiver_tx);
        // An absent incoming arbitration stamp is ordered by this one public
        // acceptance. Never resolve that sentinel against a private subgroup
        // coordinate or the detached executor's unrelated commit counter.
        for sources in finalized.relational_insert_source_lsns.values_mut() {
            for source in sources.values_mut() {
                if *source == SYNC_SOURCE_LSN_OWN_COMMIT {
                    *source = local_lsn;
                }
            }
        }
        for (_, _, source, _) in &mut finalized.sync_source_provenance_marks {
            if *source == SYNC_SOURCE_LSN_OWN_COMMIT {
                *source = local_lsn;
            }
        }
        working.relational_store.set_sync_source_lsns(
            finalized
                .relational_insert_source_lsns
                .iter()
                .flat_map(|(table, rows)| {
                    rows.iter()
                        .map(move |(row, source)| (table.clone(), *row, *source))
                })
                .chain(
                    finalized
                        .sync_source_provenance_marks
                        .iter()
                        .map(|(table, row, source, _)| (table.clone(), *row, *source)),
                ),
        );
        for (table, row) in &mut finalized.relational_inserts {
            row.created_tx = local_tx;
            row.deleted_tx = None;
            row.lsn = accepted_positions
                .get(&(table.clone(), row.row_id))
                .copied()
                .unwrap_or(local_lsn);
        }
        for (_, _, deleted_tx) in &mut finalized.relational_deletes {
            *deleted_tx = local_tx;
        }
        let private_graph_source =
            crate::persistence::received_working_source::WorkingReadSource::current_for(
                working.tx_mgr.identity(),
            )?;
        let captures_graph = private_graph_source
            .as_ref()
            .map(|source| source.private_graph_capturing())
            .transpose()?
            .unwrap_or(false);
        let graph_position = |origin| -> Result<Lsn> {
            use crate::persistence::received_working_graph::WorkingGraphOrigin;
            match origin {
                WorkingGraphOrigin::Received { source_lsn } => connected
                    .map(|(coordinates, _)| coordinates.local(source_lsn))
                    .transpose()?
                    .ok_or_else(|| {
                        Error::Other("received graph occurrence lost its source coordinates".into())
                    }),
                WorkingGraphOrigin::Callback { phase_position, .. } => Ok(phase_position),
                WorkingGraphOrigin::CommittedCarry => Ok(local_lsn),
            }
        };
        if captures_graph {
            let source = private_graph_source
                .as_ref()
                .expect("captured graph retains its source");
            for entry in &mut finalized.adj_inserts {
                let coordinate =
                    source
                        .pending_private_graph_coordinate(entry)?
                        .ok_or_else(|| {
                            Error::Other("final graph insert has no captured occurrence".into())
                        })?;
                entry.created_tx = local_tx;
                entry.lsn = graph_position(coordinate.occurrence.origin())?;
                // The current pending occurrence is live; retained predecessor
                // tombstones remain in their own exact native spool records.
                entry.deleted_tx = None;
            }
        } else {
            for entry in &mut finalized.adj_inserts {
                entry.created_tx = local_tx;
                entry.deleted_tx = None;
                entry.lsn = connected
                    .map(|(coordinates, overlay)| {
                        overlay
                            .projected
                            .edges
                            .iter()
                            .rev()
                            .find(|source| {
                                source.source == entry.source
                                    && source.target == entry.target
                                    && source.edge_type == entry.edge_type
                            })
                            .map(|source| coordinates.local(source.lsn))
                            .transpose()
                    })
                    .transpose()?
                    .flatten()
                    .unwrap_or(local_lsn);
            }
        }
        for (_, _, _, deleted_tx) in &mut finalized.adj_deletes {
            *deleted_tx = local_tx;
        }
        for entry in &mut finalized.vector_inserts {
            entry.created_tx = local_tx;
            entry.deleted_tx = None;
            entry.lsn = finalized
                .relational_inserts
                .iter()
                .rev()
                .find(|(table, row)| *table == entry.index.table && row.row_id == entry.row_id)
                .map(|(_, row)| row.lsn)
                .unwrap_or(local_lsn);
        }
        for (_, _, deleted_tx) in &mut finalized.vector_deletes {
            *deleted_tx = local_tx;
        }
        for (_, _, _, moved_tx) in &mut finalized.vector_moves {
            *moved_tx = local_tx;
        }

        let inserted_rows = &finalized.relational_inserts;
        {
            let mut tables = working.relational_store.tables.write();
            for (table, row_id, detached_tx) in &deleted_rows {
                if let Some(current) = tables.get_mut(table).and_then(|rows| {
                    rows.iter_mut().rev().find(|current| {
                        current.row_id == *row_id && current.deleted_tx == Some(*detached_tx)
                    })
                }) {
                    current.deleted_tx = Some(local_tx);
                }
            }
            for (position, (table, row)) in inserted_rows.iter().enumerate() {
                let (_, row_id, created_tx, lsn) = &detached_inserted_rows[position];
                if let Some(current) = tables.get_mut(table).and_then(|rows| {
                    rows.iter_mut().rev().find(|current| {
                        current.row_id == *row_id
                            && current.created_tx == *created_tx
                            && current.lsn == *lsn
                    })
                }) {
                    // The adjudicator already applied these exact values.
                    // Only the reserved receiver coordinates change here.
                    current.created_tx = row.created_tx;
                    current.deleted_tx = row.deleted_tx;
                    current.lsn = row.lsn;
                    current.created_at = row.created_at;
                }
            }
        }
        if working.relational_store.uses_disk_backed_committed_bodies()
            && let Some(source) =
                crate::persistence::received_working_source::WorkingReadSource::current_for(
                    working.tx_mgr.identity(),
                )?
        {
            let tables = detached_inserted_rows
                .iter()
                .map(|(table, _, _, _)| table)
                .chain(deleted_rows.iter().map(|(table, _, _)| table));
            for (position, table) in tables.clone().enumerate() {
                if tables.clone().take(position).any(|prior| prior == table) {
                    continue;
                }
                if !working
                    .relational_store
                    .table_meta
                    .read()
                    .contains_key(table)
                {
                    continue;
                }
                source.rewrite_private_row_identities(table, |row| {
                    if let Some(position) =
                        detached_inserted_rows
                            .iter()
                            .position(|(name, id, created, lsn)| {
                                name == table
                                    && *id == row.row_id
                                    && *created == *row.created_tx
                                    && *lsn == *row.lsn
                            })
                    {
                        let replacement = &finalized.relational_inserts[position].1;
                        *row.created_tx = replacement.created_tx;
                        *row.deleted_tx = replacement.deleted_tx;
                        *row.lsn = replacement.lsn;
                        *row.created_at = replacement.created_at;
                    }
                    if deleted_rows.iter().any(|(name, id, deleted)| {
                        name == table && *id == row.row_id && *row.deleted_tx == Some(*deleted)
                    }) {
                        *row.deleted_tx = Some(local_tx);
                    }
                    Ok(())
                })?;
            }
        }
        let stamp_edge =
            |entry: &mut AdjEntry| {
                if let Some(position) = detached_inserted_edges.iter().position(
                    |(source, kind, target, created, lsn)| {
                        *source == entry.source
                            && *kind == entry.edge_type
                            && *target == entry.target
                            && *created == entry.created_tx
                            && *lsn == entry.lsn
                    },
                ) {
                    let replacement = &finalized.adj_inserts[position];
                    entry.created_tx = replacement.created_tx;
                    entry.deleted_tx = replacement.deleted_tx;
                    entry.lsn = replacement.lsn;
                }
                if detached_deleted_edges
                    .iter()
                    .any(|(source, kind, target, deleted)| {
                        *source == entry.source
                            && *target == entry.target
                            && *kind == entry.edge_type
                            && entry.deleted_tx == Some(*deleted)
                    })
                {
                    entry.deleted_tx = Some(local_tx);
                }
            };
        if working.graph_store.is_source_backed() {
            let source =
                crate::persistence::received_working_source::WorkingReadSource::current_for(
                    working.tx_mgr.identity(),
                )?
                .ok_or_else(|| {
                    Error::Other("received graph restamp is outside its working loan".into())
                })?;
            if captures_graph {
                source.restamp_private_graph_occurrences(local_tx, &graph_position)?;
            } else {
                source.rewrite_private_graph(|entry| {
                    let before = (entry.created_tx, entry.deleted_tx, entry.lsn);
                    stamp_edge(entry);
                    Ok(
                        if before == (entry.created_tx, entry.deleted_tx, entry.lsn) {
                            crate::persistence::received_working_graph::GraphRewrite::Keep
                        } else {
                            crate::persistence::received_working_graph::GraphRewrite::Replace
                        },
                    )
                })?;
            }
        } else {
            for entry in working
                .graph_store
                .forward_adj
                .write()
                .values_mut()
                .flatten()
            {
                if entry.row_owner.is_none() {
                    stamp_edge(&mut entry.entry);
                }
            }
            for entry in working
                .graph_store
                .reverse_adj
                .write()
                .values_mut()
                .flatten()
            {
                if entry.row_owner.is_none() {
                    stamp_edge(&mut entry.entry);
                }
            }
        }
        if working.graph_store.is_source_backed() {
            let source =
                crate::persistence::received_working_source::WorkingReadSource::current_for(
                    working.tx_mgr.identity(),
                )?
                .ok_or_else(|| {
                    Error::Other("received vector restamp is outside its working loan".into())
                })?;
            source.rewrite_private_vectors(
                |_frame, body| {
                    if let Some(position) =
                        detached_inserted_vectors
                            .iter()
                            .position(|(index, row, created, lsn)| {
                                *index == *body.index
                                    && *row == *body.row_id
                                    && *created == *body.created_tx
                                    && *lsn == *body.lsn
                            })
                    {
                        let replacement = &finalized.vector_inserts[position];
                        *body.created_tx = replacement.created_tx;
                        *body.deleted_tx = replacement.deleted_tx;
                        *body.lsn = replacement.lsn;
                    }
                    if detached_deleted_vectors
                        .iter()
                        .any(|(index, row, deleted)| {
                            *index == *body.index
                                && *row == *body.row_id
                                && *body.deleted_tx == Some(*deleted)
                        })
                        || deleted_rows.iter().any(|(table, row, deleted)| {
                            *table == body.index.table
                                && *row == *body.row_id
                                && *body.deleted_tx == Some(*deleted)
                        })
                    {
                        *body.deleted_tx = Some(local_tx);
                    }
                    Ok(true)
                },
                |_frame, member| {
                    let mut changed = false;
                    if let Some(position) =
                        detached_inserted_vectors
                            .iter()
                            .position(|(index, row, created, lsn)| {
                                *index == member.index
                                    && *row == member.row_id
                                    && *created == member.vector_created_tx
                                    && *lsn == member.vector_lsn
                            })
                    {
                        let replacement = &finalized.vector_inserts[position];
                        member.vector_created_tx = replacement.created_tx;
                        member.vector_lsn = replacement.lsn;
                        changed = true;
                    }
                    if detached_inserted_rows
                        .iter()
                        .any(|(table, row, created, lsn)| {
                            *table == member.index.table
                                && *row == member.row_id
                                && *created == member.row_created_tx
                                && *lsn == member.row_lsn
                        })
                    {
                        member.row_created_tx = local_tx;
                        member.row_lsn = finalized
                            .relational_inserts
                            .iter()
                            .rev()
                            .find(|(table, row)| {
                                *table == member.index.table && row.row_id == member.row_id
                            })
                            .map(|(_, row)| row.lsn)
                            .unwrap_or(local_lsn);
                        changed = true;
                    }
                    if changed {
                        member.visible_from = member.row_created_tx.max(member.vector_created_tx);
                    }
                    if deleted_rows.iter().any(|(table, row, deleted)| {
                        *table == member.index.table
                            && *row == member.row_id
                            && member.deleted_tx == Some(*deleted)
                    }) || detached_deleted_vectors
                        .iter()
                        .any(|(index, row, deleted)| {
                            *index == member.index
                                && *row == member.row_id
                                && member.deleted_tx == Some(*deleted)
                        })
                    {
                        member.deleted_tx = Some(local_tx);
                    }
                    Ok(true)
                },
            )?;
        }
        let vector_copy_owner = contextdb_vector::VectorCopyAdmission::new(
            contextdb_vector::MemoryBudgetOwner::clone(&working.finalization.vector_accountant),
        );
        let parts = working
            .vector_store
            .try_resident_partitioned_entries_with_membership(vector_copy_owner)?
            .into_parts();
        let mut image = ReceivedRestampedVectorImage {
            entries: parts.0,
            owner: parts.1,
        };
        for partitioned in &mut image.entries {
            let entry = &mut partitioned.entry;
            if let Some(position) =
                detached_inserted_vectors
                    .iter()
                    .position(|(index, row, created, lsn)| {
                        *index == entry.index
                            && *row == entry.row_id
                            && *created == entry.created_tx
                            && *lsn == entry.lsn
                    })
            {
                *entry = finalized.vector_inserts[position].clone();
                partitioned.canonical_deleted_tx = entry.deleted_tx;
            }
            let membership = &mut partitioned.membership;
            if detached_inserted_rows
                .iter()
                .any(|(table, row, created, lsn)| {
                    *table == entry.index.table
                        && *row == entry.row_id
                        && *created == membership.owning_row_tx
                        && *lsn == membership.row_lsn
                })
            {
                membership.owning_row_tx = local_tx;
                membership.row_lsn = finalized
                    .relational_inserts
                    .iter()
                    .rev()
                    .find(|(table, row)| *table == entry.index.table && row.row_id == entry.row_id)
                    .map(|(_, row)| row.lsn)
                    .unwrap_or(local_lsn);
                membership.visible_from = local_tx;
            }
            if deleted_rows.iter().any(|(table, row_id, deleted_tx)| {
                *table == entry.index.table
                    && *row_id == entry.row_id
                    && membership.deleted_tx == Some(*deleted_tx)
            }) || detached_deleted_vectors
                .iter()
                .any(|(index, row_id, deleted_tx)| {
                    *index == entry.index
                        && *row_id == entry.row_id
                        && membership.deleted_tx == Some(*deleted_tx)
                })
            {
                membership.deleted_tx = Some(local_tx);
            }
            if detached_deleted_vectors
                .iter()
                .any(|(index, row_id, deleted_tx)| {
                    *index == entry.index
                        && *row_id == entry.row_id
                        && partitioned.canonical_deleted_tx == Some(*deleted_tx)
                })
            {
                partitioned.canonical_deleted_tx = Some(local_tx);
            }
            entry.deleted_tx = membership.deleted_tx;
            // Preserve the exact stored-value round trip the former private
            // replacement performed, without constructing another registry
            // and exporting every body again. Original export funding stays
            // with this image; only overlap scratch dies at this record.
            let quantization = working
                .vector_store
                .with_index_layout(&entry.index, |layout| layout.quantization)?;
            let mut scratch = image.owner.scratch();
            scratch.reserve(entry.vector.len().saturating_mul(8).saturating_add(512))?;
            entry.vector = contextdb_vector::stored_vector_value(&entry.vector, quantization);
        }
        let deleted_row_ids = finalized
            .relational_deletes
            .iter()
            .map(|(_, row_id, _)| *row_id)
            .collect::<HashSet<_>>();
        for (marker_key, marker) in working.sync_tombstone_arrivals.write().iter_mut() {
            if detached_lsn.is_some_and(|lsn| marker.delete_lsn == lsn)
                && marker
                    .row_id
                    .is_some_and(|row_id| deleted_row_ids.contains(&row_id))
            {
                marker.delete_lsn = marker
                    .row_id
                    .and_then(|row_id| {
                        accepted_positions
                            .get(&(marker_key.0.clone(), row_id))
                            .copied()
                    })
                    .unwrap_or(local_lsn);
                // Only a wire entry explicitly mapped to `None` used the
                // detached commit LSN as its arrival fallback. An explicit
                // source arrival may have that same numeric value, so its
                // provenance, not equality with the detached LSN, decides.
                if fallback_arrival_tombstones.contains(marker_key) {
                    marker.arrival = local_lsn;
                }
            }
        }
        for (key, bytes) in &mut finalized.config_writes {
            let _record = (key.starts_with(SYNC_PUSH_OUTCOME_PREFIX)
                || key.starts_with(SYNC_GRAPH_ARRIVAL_PREFIX)
                || key.starts_with("sync_lineage.v1."))
            .then(|| {
                contextdb_core::read_memory::ReadCredit::try_new(
                    read_memory.clone(),
                    bytes
                        .len()
                        .saturating_mul(64)
                        .saturating_add(key.len())
                        .saturating_add(4096),
                    "sync",
                    "private_commit_config_record",
                    "Raise the declared reader memory limit to restamp accepted history.",
                )
            })
            .transpose()?;
            if key.starts_with(SYNC_PUSH_OUTCOME_PREFIX) {
                let mut result: DurableSyncPushOutcome =
                    RedbPersistence::decode_config_value(bytes)?;
                result.result.new_lsn = local_lsn;
                *bytes = RedbPersistence::encode_config_value(&result)?;
            }
            if key.starts_with(SYNC_GRAPH_ARRIVAL_PREFIX) {
                let mut record: DurableGraphArrival = RedbPersistence::decode_config_value(bytes)?;
                record.local_lsn = if captures_graph {
                    private_graph_source
                        .as_ref()
                        .expect("captured graph source")
                        .last_private_graph_coordinate(
                            record.source,
                            &record.edge_type,
                            record.target,
                        )?
                        .map(|coordinate| graph_position(coordinate.occurrence.origin()))
                        .transpose()?
                        .unwrap_or(local_lsn)
                } else {
                    connected
                        .map(|(coordinates, overlay)| {
                            overlay
                                .projected
                                .edges
                                .iter()
                                .rev()
                                .find(|edge| {
                                    edge.source == record.source
                                        && edge.target == record.target
                                        && edge.edge_type == record.edge_type
                                })
                                .map(|edge| coordinates.local(edge.lsn))
                                .transpose()
                        })
                        .transpose()?
                        .flatten()
                        .unwrap_or(local_lsn)
                };
                *bytes = RedbPersistence::encode_config_value(&record)?;
                let (identity, record) = working.prepare_graph_arrival_memory(&record)?;
                working.sync_graph_arrivals.write().insert(identity, record);
            }
            if key.starts_with("sync_lineage.v1.")
                && let Ok(mut record) =
                    RedbPersistence::decode_config_value::<DurableLineageRecord>(bytes)
                && detached_lsn.is_some_and(|lsn| record.delete_lsn == lsn.0)
            {
                record.delete_lsn = record
                    .local_row_id
                    .and_then(|row_id| {
                        accepted_positions
                            .get(&(record.table.clone(), row_id))
                            .copied()
                    })
                    .unwrap_or(local_lsn)
                    .0;
                *bytes = RedbPersistence::encode_config_value(&record)?;
            }
        }
        Ok(image)
    }

    /// Assemble the private received-schema stage by running the ordinary
    /// conflict/adoption adjudicator against a detached projected image.  The
    /// wire `ChangeSet` contributes authenticated intent only; it never
    /// directly assigns a receiver row id, MVCC identity, vector owner, or
    /// delete/terminal outcome in the prepared image.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    #[cfg(test)]
    pub(super) fn prepare_received_schema_stage(
        &self,
        local_lsn: Lsn,
        changes: &ChangeSet,
        received: &crate::protocol::ReceivedDdlContext,
        validated_lineages: &[(String, NaturalKey, Lsn, crate::protocol::WireRowLineage)],
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<ReceivedSchemaStage> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let arrivals = HashMap::new();
        self.prepare_received_schema_stage_with_adjudication(
            local_lsn,
            changes,
            received,
            validated_lineages,
            ReceivedSchemaAdjudicationInputs {
                push_outcome_key: None,
                apply_role: SyncApplyRole::PullLeg,
                arrivals: &arrivals,
                adoption: SyncAdoption::Continuing,
                receipt: None,
                dependency_complete: false,
                terminal_refusal_context: None,
                hub_local_author: None,
                receiver_tx: self.tx_mgr.peek_next_tx(),
                unattested_policies: None,
            },
            vector_copy_owner,
            read_memory.clone(),
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    #[cfg(test)]
    pub(super) fn prepare_received_schema_stage_with_adjudication(
        &self,
        local_lsn: Lsn,
        changes: &ChangeSet,
        received: &crate::protocol::ReceivedDdlContext,
        validated_lineages: &[(String, NaturalKey, Lsn, crate::protocol::WireRowLineage)],
        inputs: ReceivedSchemaAdjudicationInputs<'_>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<ReceivedSchemaStage> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.prepare_received_schema_stage_with_delivery(
            local_lsn,
            changes,
            Some(received),
            validated_lineages,
            inputs,
            None,
            vector_copy_owner,
            read_memory.clone(),
        )
    }

    /// Tables whose registered vector index no longer has a vector column.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn received_retired_vector_tables(
        &self,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<HashSet<String>> {
        let committed = self.relational_store.table_meta.read();
        Ok(self
            .vector_store
            .index_infos(vector_copy_owner)?
            .into_iter()
            .filter(|info| {
                !committed.get(&info.index.table).is_some_and(|meta| {
                    meta.columns.iter().any(|column| {
                        column.name == info.index.column
                            && matches!(column.column_type, ColumnType::Vector(_))
                    })
                })
            })
            .map(|info| info.index.table)
            .collect())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn received_vector_lifetime_changes(
        &self,
        live: &HashMap<String, TableMeta>,
        projected: &HashMap<String, TableMeta>,
        changes: &ChangeSet,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<(HashSet<VectorIndexRef>, HashSet<VectorIndexRef>)> {
        // Output routes outlive each schema decoder's scratch. Keep their
        // copied names and collection backing with the enclosing apply owner.
        vector_copy_owner.reserve(
            live.values()
                .map(TableMeta::metadata_bytes)
                .sum::<usize>()
                .saturating_mul(16)
                .saturating_add(8192),
        )?;
        let mut changed_routes = HashSet::new();
        let mut retired_lifetimes = HashSet::new();
        for (name, previous) in live {
            let Some(meta) = projected.get(name) else {
                for column in previous
                    .columns
                    .iter()
                    .filter(|column| matches!(column.column_type, ColumnType::Vector(_)))
                {
                    changed_routes.insert(VectorIndexRef::new(name, &column.name));
                    retired_lifetimes.insert(VectorIndexRef::new(name, &column.name));
                }
                continue;
            };
            let (changed, retired) = if let Some(persistence) = &self.persistence {
                persistence.received_changed_vector_incarnations(name, meta, &changes.ddl)?
            } else {
                let _scratch = StagedCopyCharge::admit_for_host(
                    &self.tx_mgr,
                    &self.finalization.accountant,
                    contextdb_vector::MemoryBudgetOwner::clone(
                        &self.finalization.vector_accountant,
                    ),
                    previous
                        .metadata_bytes()
                        .saturating_add(meta.metadata_bytes())
                        .saturating_mul(64)
                        .saturating_add(8192),
                    "project_schema_incarnations",
                )?;
                RedbPersistence::received_changed_vector_lifetimes_in_memory(
                    name,
                    previous,
                    meta,
                    &changes.ddl,
                )?
            };
            changed_routes.extend(changed);
            retired_lifetimes.extend(retired);
        }
        Ok((changed_routes, retired_lifetimes))
    }

    /// Vector indexes whose declared layout this received unit changes. An open
    /// transaction that staged a vector against one of them must learn at
    /// commit that the index changed under it.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn received_unit_changed_vector_refs(
        &self,
        changes: &ChangeSet,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<Vec<VectorIndexRef>> {
        if changes.ddl.is_empty() {
            return Ok(Vec::new());
        }
        let retired_vector_tables = self.received_retired_vector_tables(vector_copy_owner)?;
        let (_live_catalog_charge, live_tables) =
            self.selected_received_catalog(changes, retired_vector_tables.iter().cloned())?;
        let side_metadata_required = Self::received_side_metadata_required(changes);
        let live_event_bus = if side_metadata_required {
            self.received_event_bus_projection()
        } else {
            Default::default()
        };
        let live_trigger = if side_metadata_required {
            self.trigger.declarations.lock().clone()
        } else {
            Default::default()
        };
        let (_projected_charge, projected_tables) =
            self.selected_received_catalog(changes, retired_vector_tables.iter().cloned())?;
        let projected = self.project_received_schema_from_state(
            projected_tables,
            live_event_bus.clone(),
            live_trigger.clone(),
            &changes.ddl,
        )?;
        if Self::received_schema_unit_is_projection_noop(
            changes,
            &live_tables,
            &projected,
            &live_event_bus,
            &live_trigger,
            false,
        ) {
            return Ok(Vec::new());
        }
        let live = vector_layouts_from_meta(&live_tables)
            .into_iter()
            .collect::<HashMap<_, _>>();
        let after = vector_layouts_from_meta(&projected.tables)
            .into_iter()
            .collect::<HashMap<_, _>>();
        let (mut changed, _) = self.received_vector_lifetime_changes(
            &live_tables,
            &projected.tables,
            changes,
            vector_copy_owner,
        )?;
        changed.extend(
            live.keys()
                .chain(after.keys())
                .filter(|index| live.get(*index) != after.get(*index))
                .cloned(),
        );
        Ok(changed.into_iter().collect())
    }

    /// Production file receivers use the same unpublished paged source as
    /// original-funded preparation, retaining the live Legacy writer identity.
    #[expect(
        clippy::too_many_arguments,
        reason = "Private preparation preserves the received interval's distinct authenticated inputs."
    )]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error retains inline typed admission details without allocating on refusal."
    )]
    fn prepare_legacy_file_received_schema_stage(
        &self,
        local_lsn: Lsn,
        coordinates: Option<&ConnectedCoordinates>,
        changes: &ChangeSet,
        received: Option<&crate::protocol::ReceivedDdlContext>,
        validated_lineages: &[(String, NaturalKey, Lsn, crate::protocol::WireRowLineage)],
        inputs: ReceivedSchemaAdjudicationInputs<'_>,
        delivery_prepare: Option<&mut dyn crate::custody::preparation::CommitPreparation>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: contextdb_core::read_memory::ReadMemoryOwner,
    ) -> Result<ReceivedSchemaStage> {
        self.with_writer_preparation_original(|original| {
            let owner =
                crate::persistence::received_working::ReceivedPreparationOwner::prepare(original)?;
            let original = owner.original();
            let store = self.persistence.as_ref().ok_or_else(|| {
                Error::Other("file received preparation lost its persistence owner".into())
            })?;
            let mut write =
                crate::persistence::received_working::ReceivedWorkingWrite::begin(store, original)?;
            write.retain_preparation_owner(&owner)?;
            let loan = ReceivedWorkingPreparation {
                write: &write,
                original,
                funder: owner.funder(),
                store,
            };
            let mut stage = self.prepare_received_schema_stage_with_interval(
                local_lsn,
                coordinates,
                changes,
                received,
                validated_lineages,
                inputs,
                delivery_prepare,
                vector_copy_owner,
                read_memory,
                Some(original),
                &loan,
            )?;
            if !stage.trigger_audits.is_empty() {
                stage.native_audit_memory = Some(self.prepare_original_audit_memory_iter(
                    original,
                    stage.trigger_audits.iter().map(|(_, entry)| entry),
                )?);
            }
            *stage.durable_projection.working_write.get_mut() = Some(write);
            stage.durable_projection.preparation_owner = Some(owner.clone());
            stage._received_preparation_owner = Some(owner);
            Ok(stage)
        })
    }

    /// An old CREATE can describe the current table only as part of its
    /// complete, exactly matched local history. Rebuild the arriving table
    /// image independently; applying CREATE over the live image would preserve
    /// later local columns and falsely certify a partial historical echo.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn received_schema_restatement_is_complete(
        &self,
        changes: &ChangeSet,
        all_occurrences_match: bool,
    ) -> Result<bool> {
        if !all_occurrences_match
            || !changes
                .ddl
                .iter()
                .any(|ddl| matches!(ddl, DdlChange::CreateTable { .. }))
        {
            return Ok(false);
        }
        let (_live_charge, current) =
            self.selected_received_catalog(changes, std::iter::empty())?;
        let (_incoming_charge, mut incoming) =
            self.selected_received_catalog(changes, std::iter::empty())?;
        for ddl in &changes.ddl {
            if let DdlChange::CreateTable { name, .. } = ddl {
                incoming.remove(name);
            }
        }
        for ddl in &changes.ddl {
            Self::apply_sync_table_ddl_to_projected_meta(&mut incoming, ddl);
        }
        let comparison_bytes = current
            .iter()
            .chain(incoming.iter())
            .map(|(table, meta)| {
                table.len() + meta.metadata_bytes() + std::mem::size_of::<(String, TableMeta)>() * 2
            })
            .sum();
        let _comparison_charge = StagedCopyCharge::admit_for_host(
            &self.tx_mgr,
            &self.finalization.accountant,
            contextdb_vector::MemoryBudgetOwner::clone(&self.finalization.vector_accountant),
            comparison_bytes,
            "compare_received_bootstrap_catalog",
        )?;
        Ok(Self::canonical_declared_schema_tables(&current)
            == Self::canonical_declared_schema_tables(&incoming))
    }

    /// Whether a schema-only unit with no signed provenance would change
    /// nothing here. Such a unit has no replay identity or receipt to record,
    /// so installing it would only allocate a local commit position for work
    /// that already exists -- an echo a peer would then pull back.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn unattested_schema_unit_changes_nothing(
        &self,
        changes: &ChangeSet,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<bool> {
        if changes.ddl.is_empty() || changes.data_entry_count() != 0 {
            return Ok(false);
        }
        let retired_vector_tables = self.received_retired_vector_tables(vector_copy_owner)?;
        let (_live_catalog_charge, live_tables) =
            self.selected_received_catalog(changes, retired_vector_tables.iter().cloned())?;
        let side_metadata_required = Self::received_side_metadata_required(changes);
        let live_event_bus = if side_metadata_required {
            self.received_event_bus_projection()
        } else {
            Default::default()
        };
        let live_trigger = if side_metadata_required {
            self.trigger.declarations.lock().clone()
        } else {
            Default::default()
        };
        let (_projected_charge, projected_tables) =
            self.selected_received_catalog(changes, retired_vector_tables.iter().cloned())?;
        let side_semantic = self.project_received_schema_from_state(
            projected_tables,
            live_event_bus.clone(),
            live_trigger.clone(),
            &changes.ddl,
        )?;
        Ok(Self::received_schema_unit_is_projection_noop(
            changes,
            &live_tables,
            &side_semantic,
            &live_event_bus,
            &live_trigger,
            false,
        ))
    }

    /// Whether a received schema unit leaves the receiver's schema exactly as
    /// it is: every declaration it carries is already in force here.
    pub(super) fn received_schema_unit_is_projection_noop(
        changes: &ChangeSet,
        live_tables: &HashMap<String, TableMeta>,
        side_semantic: &ReceivedSchemaSemanticProjection,
        live_event_bus: &event_bus::EventBusDefinitions,
        live_trigger: &BTreeMap<String, TriggerDeclaration>,
        restates_complete_local_history: bool,
    ) -> bool {
        // A CREATE that restates a table the receiver already has, but with a
        // different column list or structured descriptors that CONTRADICT it,
        // is a schema conflict: the per-unit apply raises `SchemaInvalid
        // { "structured constraints differ" }` for the descriptors and reports
        // a "schema mismatch" conflict for the columns. The projection leaves
        // an existing table alone, so the arriving shape and the live shape
        // compare equal and the unit would take the no-op short-circuit and
        // come back `Ok` with no conflict, silently accepting a peer whose
        // table disagrees with this node's. The unit is not a no-op; let it
        // reach the apply that reports it.
        // Exact complete local occurrence matches prove an authored CREATE
        // followed by ALTER already happened here. Its initial CREATE need
        // not match the current, later shape; the final projection still must.
        let restated_shape_disagrees = !restates_complete_local_history
            && changes.ddl.iter().any(|ddl| match ddl {
                DdlChange::CreateTable {
                    name,
                    columns,
                    constraints,
                    foreign_keys,
                    composite_foreign_keys,
                    composite_unique,
                } => live_tables.get(name).is_some_and(|local| {
                    !sync_table_shape_matches(
                        local,
                        columns,
                        constraints,
                        foreign_keys,
                        composite_foreign_keys,
                        composite_unique,
                    ) || !sync_table_columns_match(local, columns)
                }),
                _ => false,
            });
        // Dropping a table the receiver holds is never nothing, even when a
        // later CREATE in the same unit restores the same shape: the table's
        // rows go, and the recreated table is a new incarnation with a new
        // generation. Comparing only the final shape would call that unit a
        // no-op and keep the old table, its rows and its identity.
        let drops_a_held_table = Self::sync_dropped_tables(&changes.ddl)
            .iter()
            .any(|table| live_tables.contains_key(table));
        // A drop is never a restatement. Either it removes something held
        // here, or it removes what the same unit created -- a table, trigger,
        // index or route declared and dropped in one authored transaction --
        // whose final shape matches the live one but whose declarations must
        // still replay into the log in author order, or it names something
        // unknown, which the per-item apply judges.
        let carries_a_drop = changes.ddl.iter().any(|ddl| {
            matches!(
                ddl,
                DdlChange::DropTable { .. }
                    | DdlChange::DropIndex { .. }
                    | DdlChange::DropTrigger { .. }
                    | DdlChange::DropRoute { .. }
            )
        });
        // A novel signed rename must visit the per-declaration validation.
        // Matching final names do not prove that this receiver replayed the
        // author's operation; only its recorded marker identity does.
        let carries_a_column_rename = changes.ddl.iter().any(|ddl| {
            matches!(ddl, DdlChange::AlterTable { constraints, .. }
                if !sync_column_renames_from_constraints(constraints).is_empty())
        });
        // A repeated final declaration must not hide a dropped/recreated
        // column or an intervening representation change in the authored unit.
        let changes_column_lifetime = changes.ddl.iter().any(|ddl| {
            let DdlChange::AlterTable {
                name,
                columns,
                constraints,
                foreign_keys,
                composite_foreign_keys,
                composite_unique,
            } = ddl
            else {
                return false;
            };
            if !sync_column_drops_from_constraints(constraints).is_empty() {
                return true;
            }
            live_tables.get(name).is_some_and(|existing| {
                let incoming = rough_sync_table_meta(
                    columns,
                    constraints,
                    foreign_keys,
                    composite_foreign_keys,
                    composite_unique,
                );
                Self::sync_alter_vector_shape_changed(existing, &incoming, constraints)
            })
        });
        !changes.ddl.is_empty()
            && !restated_shape_disagrees
            && !drops_a_held_table
            && !carries_a_drop
            && !carries_a_column_rename
            && !changes_column_lifetime
            && Self::canonical_received_schema_tables(live_tables)
                == Self::canonical_received_schema_tables(&side_semantic.tables)
            && *live_event_bus == side_semantic.event_bus
            && *live_trigger == side_semantic.trigger
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn received_schema_unit_changes_only_vector_policy(
        changes: &ChangeSet,
        live_tables: &HashMap<String, TableMeta>,
        owner: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<bool> {
        for ddl in &changes.ddl {
            let DdlChange::AlterTable {
                name,
                columns,
                constraints,
                foreign_keys,
                composite_foreign_keys,
                composite_unique,
            } = ddl
            else {
                return Ok(false);
            };
            let Some(old) = live_tables.get(name) else {
                return Ok(false);
            };
            if !sync_column_drops_from_constraints(constraints).is_empty()
                || !sync_column_renames_from_constraints(constraints).is_empty()
            {
                return Ok(false);
            }
            let mut scratch = owner.scratch();
            scratch.reserve(
                old.estimated_bytes().saturating_mul(8).saturating_add(
                    columns
                        .iter()
                        .map(|(name, ty)| name.len().saturating_add(ty.len()))
                        .sum::<usize>()
                        .saturating_mul(8),
                ),
            )?;
            let incoming = rough_sync_table_meta(
                columns,
                constraints,
                foreign_keys,
                composite_foreign_keys,
                composite_unique,
            );
            let mut next = old.clone();
            Self::apply_sync_alter_to_projected_table_meta(name, &mut next, incoming, constraints);
            // The comparator borrows table structure and copies one vector
            // declaration at a time, including any new policy-owned buffers.
            scratch.reserve(next.estimated_bytes().saturating_mul(4))?;
            if !crate::executor::metadata_equal_except_vector_policy(old, &next) {
                return Ok(false);
            }
        }
        Ok(!changes.ddl.is_empty())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    #[cfg(test)]
    pub(super) fn prepare_received_schema_stage_with_delivery(
        &self,
        local_lsn: Lsn,
        changes: &ChangeSet,
        received: Option<&crate::protocol::ReceivedDdlContext>,
        validated_lineages: &[(String, NaturalKey, Lsn, crate::protocol::WireRowLineage)],
        inputs: ReceivedSchemaAdjudicationInputs<'_>,
        delivery_prepare: Option<&mut dyn crate::custody::preparation::CommitPreparation>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<ReceivedSchemaStage> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        // The test door prepares exactly as production does: the loan is an
        // unpublished write on the live store, and the returned stage keeps
        // custody of that write and its preparation owner.
        self.prepare_legacy_file_received_schema_stage(
            local_lsn,
            None,
            changes,
            received,
            validated_lineages,
            inputs,
            delivery_prepare,
            vector_copy_owner,
            read_memory,
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn check_received_terminal_refusals(
        &self,
        tenant: &TenantId,
        rows: &[RowChange],
        override_context: Option<&TerminalRefusalPullContext>,
        read_memory: contextdb_core::read_memory::ReadMemoryOwner,
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let Some(root) = self.writer_read_root()? else {
            return Err(received_schema_requires_durable_receiver());
        };
        // Rebinding leaves rows intact; markers from a retired hub cannot
        // block units from the store's current authoritative destination.
        let authoritative_hub =
            self.read_config_value::<String>(RETENTION_SYNC_PEER_CONFIG_KEY, read_memory.clone())?;
        // Read one admitted metadata window at a time. Marker namespaces are
        // length-prefixed hex, so dotted tenant/hub names cannot alias.
        let same_hex = |encoded: &str, value: &[u8]| {
            value.len().checked_mul(2) == Some(encoded.len())
                && value.iter().enumerate().all(|(index, byte)| {
                    encoded.as_bytes()[index * 2] == b"0123456789abcdef"[(byte >> 4) as usize]
                        && encoded.as_bytes()[index * 2 + 1]
                            == b"0123456789abcdef"[(byte & 15) as usize]
                })
        };
        let mut cursor = root.config_cursor(
            crate::persistence::writer_source::SourceWindowOwner::Read(read_memory.clone()),
            "sync_terminal_refusal.v1.",
        )?;
        while let Some(window) = cursor.next(|key, bytes| -> Result<bool> {
            let key = std::str::from_utf8(key)
                .map_err(|_| Error::SyncError("invalid terminal-refusal metadata key".into()))?;
            let mut fields = key.split('.');
            let _family = fields.next();
            let _version = fields.next();
            let tenant_len = fields
                .next()
                .and_then(|value| usize::from_str_radix(value, 16).ok());
            let hub_len = fields
                .next()
                .and_then(|value| usize::from_str_radix(value, 16).ok());
            let tenant_hex = fields.next().unwrap_or_default();
            let hub_hex = fields.next().unwrap_or_default();
            if fields.next() != Some("marker")
                || tenant_len != Some(tenant.as_str().len())
                || !same_hex(tenant_hex, tenant.as_str().as_bytes())
                || authoritative_hub.as_ref().is_some_and(|hub| {
                    hub_len != Some(hub.len()) || !same_hex(hub_hex, hub.as_bytes())
                })
            {
                return Ok(false);
            }
            let image = crate::read_image_memory::ImageMemory::charging_reader(
                self.accountant.clone(),
                read_memory.clone(),
            );
            image.scope(|| {
                let marker: TerminalRefusalMarkerRecord =
                    RedbPersistence::decode_config_value(bytes)?;
                if !marker.active
                    || !rows.iter().any(|row| {
                        row.table == marker.table && row.natural_key == marker.natural_key
                    })
                    || !self.terminal_refusal_marker_is_current(
                        &marker.table,
                        &marker.natural_key,
                        Lsn(marker.lsn),
                        RowId(marker.row_id),
                        read_memory.clone(),
                    )?
                {
                    return Ok(false);
                }
                Ok(!override_context.is_some_and(|context| {
                    context.tenant_id == *tenant
                        && context.generation == marker.generation
                        && hub_len == Some(context.hub_node_id.len())
                        && same_hex(hub_hex, context.hub_node_id.as_bytes())
                }))
            })
        })? {
            if window.value {
                return Err(Error::SyncError(
                    "received unit requires its matching terminal-refusal override".into(),
                ));
            }
        }
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn prepare_received_schema_stage_with_interval(
        &self,
        local_lsn: Lsn,
        coordinates: Option<&ConnectedCoordinates>,
        changes: &ChangeSet,
        received: Option<&crate::protocol::ReceivedDdlContext>,
        validated_lineages: &[(String, NaturalKey, Lsn, crate::protocol::WireRowLineage)],
        inputs: ReceivedSchemaAdjudicationInputs<'_>,
        mut delivery_prepare: Option<&mut dyn crate::custody::preparation::CommitPreparation>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        preparation_original: Option<&OwnedMemoryReservation>,
        working_preparation: &ReceivedWorkingPreparation<'_>,
    ) -> Result<ReceivedSchemaStage> {
        for change in &changes.ddl {
            Self::validate_received_column_declarations(change)?;
        }
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();
        // Its working preparation is a loan from the live store's write.
        if self.persistence.is_none() {
            return Err(received_schema_requires_durable_receiver());
        }
        // Every received image keeps its rows in the live store's unpublished
        // write; a receiver whose committed bodies are not on disk has no such
        // source to page from.
        if !self.relational_store.uses_disk_backed_committed_bodies() {
            return Err(Error::Other(
                "received schema preparation requires a durable receiver with disk-backed bodies"
                    .into(),
            ));
        }

        // The private working vector source borrows this executing reader
        // while the same received operation prepares and validates its image.
        let _reader_scope = crate::read_image_memory::ImageMemory::operation_reader(
            crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
            read_memory.clone(),
        )?
        .enter();
        // Declared before every possible target config buffer, so errors
        // destroy those buffers before this independently copied-body fee.
        let mut native_trigger_config_fee = None;
        let loan = working_preparation;
        if !preparation_original.is_some_and(|original| std::ptr::eq(original, loan.original)) {
            return Err(EngineMetadataJournal::identity_error());
        }
        loan.write.require_original(loan.original)?;
        if let Some(tenant) = received
            .map(|context| &context.tenant_id)
            .or_else(|| inputs.receipt.as_ref().map(|receipt| &receipt.tenant_id))
        {
            self.check_received_terminal_refusals(
                tenant,
                &changes.rows,
                inputs.terminal_refusal_context,
                read_memory.clone(),
            )?;
        }

        let mut publication_vector_copy_owner = Some(contextdb_vector::VectorCopyAdmission::new(
            match preparation_original {
                Some(original) => {
                    crate::memory_accounting::persistence_original_projection(original)?
                        .vector_budget()?
                }
                None => {
                    contextdb_vector::MemoryBudgetOwner::clone(&self.finalization.vector_accountant)
                }
            },
        ));
        let (mut source_order, mut persistence) =
            plan_received_schema_source_order_with_coordinates(
                local_lsn,
                &changes.ddl,
                &changes.ddl_lsn,
                received,
                coordinates,
            )?;
        // Vector mutations in this received unit are adjudicated against the
        // schema produced by the same authenticated DDL vector. Register that
        // projected layout on the private working image before replaying the
        // rows/vectors; publication still remains staged and atomic.
        let retired_vector_tables = self.received_retired_vector_tables(vector_copy_owner)?;
        let (mut _live_catalog_charge, mut live_tables) = {
            let (charge, tables) =
                self.selected_received_catalog(changes, retired_vector_tables.iter().cloned())?;
            (Some(charge), tables)
        };
        let side_metadata_required = Self::received_side_metadata_required(changes);
        let mut side_metadata_charge = Some(self.admit_received_side_metadata(changes)?);
        let live_event_bus = if side_metadata_required {
            self.received_event_bus_projection()
        } else {
            Default::default()
        };
        let live_trigger = if side_metadata_required {
            self.trigger.declarations.lock().clone()
        } else {
            Default::default()
        };
        let (projected_charge, projected_tables) =
            self.selected_received_catalog(changes, retired_vector_tables.iter().cloned())?;
        let mut side_semantic = self.project_received_schema_from_state(
            projected_tables,
            live_event_bus.clone(),
            live_trigger.clone(),
            &changes.ddl,
        )?;
        side_semantic.catalog_charge = Some(projected_charge);
        let private_source_candidate = coordinates.is_some()
            && Self::private_source_order_supported(
                changes,
                &live_tables,
                &side_semantic.tables,
                read_memory.clone(),
            )?;
        let mut connected_overlay = coordinates
            .map(|_| {
                self.prepare_connected_overlay_with_vectors(
                    changes,
                    read_memory.clone(),
                    private_source_candidate,
                )
            })
            .transpose()?;
        let private_source_coordinates = if private_source_candidate
            && connected_overlay
                .as_ref()
                .is_some_and(|overlay| overlay.source_vectors_complete(changes))
        {
            coordinates
        } else {
            None
        };
        if private_source_coordinates.is_some() {
            connected_overlay
                .as_mut()
                .expect("source coordinates own an overlay")
                .admit_source_vector_archives(changes)?;
        }
        let replay_changes = if private_source_coordinates.is_some() {
            changes
        } else {
            connected_overlay
                .as_ref()
                .map_or(changes, |overlay| &overlay.projected)
        };
        // Schema this node authored before the vector arrived, which the
        // vector itself restates, stops being outbound work. The comparison
        // uses the log as it stood before the received occurrences were
        // appended so the vector cannot match itself.
        let mut superseded_occurrences =
            self.durable_superseded_local_ddl_occurrences(&changes.ddl, read_memory.clone())?;
        for (_, occurrence) in superseded_occurrences.iter() {
            persistence.superseded_occurrence_values.push((
                Self::durable_superseded_ddl_occurrence_key(
                    occurrence.local_lsn,
                    occurrence.persisted_ordinal,
                ),
                RedbPersistence::encode_config_value(occurrence)?,
            ));
        }
        // Matching historical statements alone is insufficient: an old
        // CREATE without its later ALTER must not bless a different current
        // shape. Reconstruct the arriving table declarations independently,
        // using the same admitted, dependency-selected catalog owner.
        let restates_complete_local_history = self.received_schema_restatement_is_complete(
            changes,
            superseded_occurrences.len() == changes.ddl.len(),
        )?;
        let schema_projection_noop = Self::received_schema_unit_is_projection_noop(
            changes,
            &live_tables,
            &side_semantic,
            &live_event_bus,
            &live_trigger,
            restates_complete_local_history,
        );
        // Lets a test see whether vector rows reached this received schema
        // unit or arrived in a separate data unit.
        #[cfg(any(test, feature = "test-seams"))]
        record_received_schema_profile_work_for_test(
            "received-unit-vectors",
            changes.vectors.len() as u64,
        );
        let policy_only = !schema_projection_noop
            && changes.rows.is_empty()
            && changes.edges.is_empty()
            && changes.vectors.is_empty()
            && delivery_prepare.is_none()
            && Self::received_schema_unit_changes_only_vector_policy(
                changes,
                &live_tables,
                vector_copy_owner,
            )?;
        let schema_noop = (schema_projection_noop || policy_only)
            && changes.rows.is_empty()
            && changes.edges.is_empty()
            && changes.vectors.is_empty()
            && delivery_prepare.is_none();
        if schema_noop {
            // Metadata equality does not identify a delivery. The manifest
            // rejects duplicate authenticated units before preparation; each
            // new occurrence still owns its DDL and exact arrival evidence.
            // Keep this path free of row/index/vector replacement work.
            let ddl_append = if restates_complete_local_history {
                persistence.ddl_log_replacement.clear();
                persistence.generation_values.clear();
                persistence.received_arrival_values.clear();
                Vec::new()
            } else {
                persistence.ddl_log_replacement.clone()
            };
            let policy_tables = changes
                .ddl
                .iter()
                .filter_map(Self::ddl_affected_table)
                .filter(|_| policy_only)
                .collect::<HashSet<_>>();
            let old_metadata_bytes = policy_tables
                .iter()
                .map(|table| {
                    retained_table_metadata_bytes(&self.relational_store, &live_tables[*table])
                })
                .sum();
            let new_metadata_bytes = policy_tables
                .iter()
                .map(|table| {
                    retained_table_metadata_bytes(
                        &self.relational_store,
                        &side_semantic.tables[*table],
                    )
                })
                .sum();
            let policy_columns = policy_tables
                .iter()
                .map(|table| {
                    side_semantic.tables[*table]
                        .columns
                        .iter()
                        .filter(|column| matches!(column.column_type, ColumnType::Vector(_)))
                        .count()
                })
                .sum::<usize>();
            vector_copy_owner.reserve(policy_columns.saturating_mul(std::mem::size_of::<
                contextdb_vector::PreparedVectorPolicyLayout,
            >()))?;
            let mut vector_policy_layouts = Vec::with_capacity(policy_columns);
            for (index, layout) in vector_layouts_from_meta(&side_semantic.tables) {
                if !policy_tables.contains(index.table.as_str()) {
                    continue;
                }
                if let Some(prepared) = self.vector_store.prepare_policy_layout_update(
                    &index,
                    layout,
                    vector_copy_owner,
                )? {
                    vector_policy_layouts.push(prepared);
                }
            }
            let apply_result = ApplyResult {
                applied_rows: 0,
                skipped_rows: 0,
                conflicts: Vec::new(),
                new_lsn: local_lsn,
            };
            let received_schema_manifest = received
                .map(|received| Self::received_schema_manifest(received, &apply_result))
                .transpose()?;
            if let Some((key, manifest)) = received_schema_manifest.as_ref() {
                persistence
                    .config_values
                    .push((key.clone(), RedbPersistence::encode_config_value(manifest)?));
            }
            let mut write_set = WriteSet::default();
            write_set.commit_lsn = Some(local_lsn);
            write_set.requires_commit_lsn = true;
            write_set.visibility_floor = Some(inputs.receiver_tx);
            if let Some(receipt) = inputs.receipt.as_ref() {
                let edge_key = Self::applied_push_watermark_node_incarnation_key(
                    &receipt.tenant_id,
                    &receipt.node_id,
                    receipt.incarnation,
                );
                let tenant_key = receipt.tenant_id.config_key("sync_applied_push_watermark");
                let encoded = RedbPersistence::encode_config_value(&receipt.source_lsn.0)?;
                write_set
                    .config_max_u64_keys
                    .extend([edge_key.clone(), tenant_key.clone()]);
                write_set
                    .config_writes
                    .extend([(edge_key, encoded.clone()), (tenant_key, encoded)]);
            }
            if let Some(context) = inputs.push_outcome_key {
                let mut context = context.clone();
                context.committed_lsn = Some(local_lsn);
                write_set.config_writes.push((
                    context.key.storage_key.clone(),
                    RedbPersistence::encode_config_value(&Self::durable_sync_push_outcome(
                        &context,
                        apply_result.clone(),
                    )?)?,
                ));
            }
            if let Some(coordinates) = coordinates {
                coordinates.stage_membership(&mut write_set.config_writes)?;
            }
            let mut mirrors = self.received_schema_memory_mirrors(
                local_lsn,
                if restates_complete_local_history {
                    &[][..]
                } else {
                    &source_order
                },
                HashMap::new(),
            );
            mirrors
                .superseded_ddl_occurrences
                .extend(std::mem::take(&mut superseded_occurrences));
            if let Some(receipt) = inputs.receipt.as_ref() {
                let edge_key = Self::applied_push_watermark_node_incarnation_key(
                    &receipt.tenant_id,
                    &receipt.node_id,
                    receipt.incarnation,
                );
                for key in [edge_key, receipt.tenant_id.as_str().to_string()] {
                    mirrors
                        .receipt_and_applied_watermarks
                        .entry(key)
                        .and_modify(|current| *current = (*current).max(receipt.source_lsn))
                        .or_insert(receipt.source_lsn);
                }
            }
            let mut durable_config_values = persistence.generation_values.clone();
            durable_config_values.extend(persistence.marker_values.iter().cloned());
            durable_config_values.extend(persistence.received_arrival_values.iter().cloned());
            durable_config_values.extend(persistence.superseded_occurrence_values.iter().cloned());
            durable_config_values.extend(persistence.config_values.iter().cloned());
            let noop_memory_swap = match &self.tx_mgr {
                DatabaseTransactionHost::Owned(_) => {
                    let mut original = match preparation_original {
                        Some(original) => {
                            crate::memory_accounting::reserve_preparation_from_original(
                                original,
                                new_metadata_bytes,
                                "sync",
                                "prepare_received_schema_noop",
                                STAGE_RECEIVED_UNIT_COPY_HINT,
                            )?
                        }
                        None => self.reserve_original_preparation_for(
                            new_metadata_bytes,
                            "sync",
                            "prepare_received_schema_noop",
                            STAGE_RECEIVED_UNIT_COPY_HINT,
                        )?,
                    };
                    PreparedMemorySwap::from_original_preparation(
                        &mut original,
                        old_metadata_bytes,
                        new_metadata_bytes,
                        0,
                    )?
                }
                DatabaseTransactionHost::Legacy(_) => PreparedMemorySwap::prepare(
                    crate::memory_accounting::AccountantHandle::clone(
                        &self.finalization.accountant,
                    ),
                    old_metadata_bytes,
                    new_metadata_bytes,
                    0,
                )?,
            };
            let memory_history = None;
            return Ok(ReceivedSchemaStage {
                memory_history,
                _working_row_owner: None,
                operational_copy_admission: None,
                _vector_copy_owner: publication_vector_copy_owner
                    .take()
                    .expect("one received publication owner"),
                metadata_copy_charges: self.received_metadata_copy_charges_from_array([
                    side_metadata_charge
                        .take()
                        .expect("one side metadata owner"),
                    _live_catalog_charge.take().expect("one catalog owner"),
                    side_semantic
                        .catalog_charge
                        .take()
                        .expect("admitted catalog"),
                ])?,
                custody_memory: None,
                source_order,
                relational: Vec::new(),
                affected_tables: HashSet::new(),
                graph: None,
                affected_graph_edge_types: HashSet::new(),
                dag_edge_types: None,
                rank_formulas: None,
                vector: None,
                vector_policy_layouts,
                vector_policy_tables: HashSet::new(),
                event_bus: None,
                queue_mutation_lease: None,
                trigger: None,
                native_trigger: None,
                sink_events: Vec::new(),
                next_sink_queue_id: 0,
                trigger_audits: Vec::new(),
                native_audit_memory: None,
                next_trigger_audit_index: 0,
                mirrors,
                memory_swap: noop_memory_swap,
                next_row_id: self.relational_store.next_row_id(),
                persistence,
                apply_result,
                write_set: parking_lot::Mutex::new(write_set),
                retired_custody_keys: parking_lot::Mutex::new(Vec::new()),
                durable_projection: ReceivedSchemaPersistenceProjection {
                    row_adj_inserts: Vec::new(),
                    row_adj_deletes: Vec::new(),
                    dropped_row_graph: Vec::new(),
                    retired_row_graph_tables: Vec::new(),
                    working_write: parking_lot::Mutex::new(None),
                    preparation_owner: None,
                    schema_noop: true,
                    affected_tables: HashSet::new(),
                    replace_graph: false,
                    affected_graph_edge_types: HashSet::new(),
                    graph_selection: None,
                    graph_occurrences: false,
                    replace_vectors: false,
                    vector_image_memory: crate::read_image_memory::ImageMemory::charging_for(
                        crate::memory_accounting::AccountantHandle::clone(
                            &self.finalization.accountant,
                        ),
                        "sync",
                        "prepare_vector_partition_image",
                        STAGE_RECEIVED_UNIT_COPY_HINT,
                    ),
                    retired_vector_construction: Mutex::new(Vec::new()),
                    table_meta: side_semantic
                        .tables
                        .iter()
                        .filter(|(table, _)| policy_tables.contains(table.as_str()))
                        .map(|(table, meta)| (table.clone(), meta.clone()))
                        .collect(),
                    row_changed_tables: HashSet::new(),
                    authored_pending_rows: Vec::new(),
                    rows: HashMap::new(),
                    row_records: None,
                    private_rows: None,
                    sync_sources: Vec::new(),
                    sync_source_records: None,
                    edges: Vec::new(),
                    vectors: Vec::new(),
                    vector_memberships: Vec::new(),
                    vector_records: None,
                    ddl_log: ddl_append,
                    config_values: durable_config_values,
                    config_max_u64_keys: Vec::new(),
                    operational_write_set: WriteSet::default(),
                    operational_table_meta: HashMap::new(),
                    operational_deleted_rows: HashMap::new(),
                    structurally_dropped_tables: HashSet::new(),
                },
                structurally_dropped_tables: HashSet::new(),
                change_log_entries: Vec::new(),
                operational_memberships: None,
                _connected_credits: coordinates.zip(connected_overlay.as_ref()).map(
                    |(coordinates, overlay)| {
                        (coordinates._credit.clone(), overlay.retained_credit())
                    },
                ),
                _private_prefix_effects: parking_lot::Mutex::new(None),
                _native_trigger_config_fee: None,
                _received_edge_input_original: None,
                _received_preparation_owner: None,
            });
        }
        let live_vector_layouts = vector_layouts_from_meta(&live_tables)
            .into_iter()
            .collect::<HashMap<_, _>>();
        let projected_vector_layouts = vector_layouts_from_meta(&side_semantic.tables)
            .into_iter()
            .collect::<HashMap<_, _>>();
        let (changed_vector_routes, replaced_vector_refs) = self.received_vector_lifetime_changes(
            &live_tables,
            &side_semantic.tables,
            changes,
            vector_copy_owner,
        )?;
        let mut additional_seeds = retired_vector_tables;
        additional_seeds.extend(
            changed_vector_routes
                .iter()
                .map(|index| index.table.clone()),
        );
        // A layout change that keeps the vectors' storage shape (dimension,
        // quantization and partition key) is policy only: search mode,
        // AUTO_INDEX_AT, MAX_PARTITIONS or consolidation. A local ALTER
        // applies it to the live column in place and keeps every partition
        // and index, so the received ALTER does the same: it neither seeds a
        // vector replacement nor makes this unit copy the vectors.
        let policy_only = |index: &VectorIndexRef| match (
            live_vector_layouts.get(index),
            projected_vector_layouts.get(index),
        ) {
            (Some(live), Some(projected)) => {
                live != projected
                    && same_vector_storage_shape(live, projected)
                    && !changed_vector_routes.contains(index)
                    && !replaced_vector_refs.contains(index)
            }
            _ => false,
        };
        // Admit the copied declarations, names and map before building it.
        let (policy_only_count, policy_only_bytes) = live_vector_layouts
            .keys()
            .filter(|index| policy_only(index))
            .fold((0usize, 0usize), |(count, bytes), index| {
                (
                    count + 1,
                    bytes
                        .saturating_add(index.table.len())
                        .saturating_add(index.column.len())
                        .saturating_add(projected_vector_layouts[index].estimated_bytes()),
                )
            });
        vector_copy_owner.reserve(
            policy_only_bytes.saturating_add(policy_only_count.saturating_mul(
                2 * std::mem::size_of::<(VectorIndexRef, VectorIndexLayout)>() + 1,
            )),
        )?;
        let mut policy_only_vector_layouts = HashMap::with_capacity(policy_only_count);
        for index in live_vector_layouts.keys() {
            if policy_only(index) {
                policy_only_vector_layouts
                    .insert(index.clone(), projected_vector_layouts[index].clone());
            } else if live_vector_layouts.get(index) != projected_vector_layouts.get(index) {
                additional_seeds.insert(index.table.clone());
            }
        }
        for index in projected_vector_layouts.keys() {
            if !live_vector_layouts.contains_key(index) {
                additional_seeds.insert(index.table.clone());
            }
        }
        let vector_schema_changed = !additional_seeds.is_empty();
        // A row's lineage sidecar is AUTHENTICATED evidence of who authored it.
        // An unattested unit carries none, and demanding one would refuse every
        // unit that has no signed provenance -- which is not what this door is
        // for. The rows are adjudicated by the same detached apply either way.
        if received.is_some() {
            self.validate_received_schema_lineage_sidecars(changes, validated_lineages)?;
        }
        // An including-sync trigger runs its transactional callback on the
        // received rows inside this unit, and its records are validated and
        // commit together with the unit. The private image holds only the
        // unit's dependency-complete affected set; a callback that reaches a
        // table outside it is answered by adding that table (with its own
        // dependency closure) and rebuilding the image, so only tables a
        // callback actually touches are ever copied.
        let including_sync_rows = changes.rows.iter().any(|row| {
            // A declaration can run before a later DROP removes it from the
            // final image. It still needs the same private dependency access
            // and missing-table retry as a declaration that survives the unit.
            live_trigger
                .values()
                .chain(side_semantic.trigger.values())
                .any(|declaration| declaration.including_sync && declaration.table == row.table)
                || changes.ddl.iter().any(|ddl| {
                    matches!(ddl,
                    DdlChange::CreateTriggerIncludingSync { table, .. } if table == &row.table)
                })
        });
        let live_table_count = self.relational_store.table_meta.read().len();
        let mut callback_tables = HashSet::<String>::new();
        loop {
            loan.write.reset_scratch()?;
            let mut stage_shape_changes = changes.clone();
            if schema_projection_noop {
                stage_shape_changes.ddl.clear();
                stage_shape_changes.ddl_lsn.clear();
            }
            let affected_tables = Self::received_schema_dependency_closure(
                &stage_shape_changes,
                &live_tables,
                &side_semantic.tables,
                additional_seeds
                    .iter()
                    .cloned()
                    .chain(callback_tables.iter().cloned()),
            );
            let selected_layouts = |tables: &HashMap<String, TableMeta>| {
                vector_layouts_from_meta(
                    &tables
                        .iter()
                        .filter(|(table, _)| affected_tables.contains(*table))
                        .map(|(table, meta)| (table.clone(), meta.clone()))
                        .collect(),
                )
                .into_iter()
                .collect::<HashMap<_, _>>()
            };
            // Only a storage-shape difference needs the vectors copied; a
            // policy-only one is applied in place after publication.
            let selected_shapes = |tables: &HashMap<String, TableMeta>| {
                selected_layouts(tables)
                    .into_iter()
                    .map(|(index, layout)| {
                        (
                            index,
                            (
                                layout.dimension,
                                layout.quantization,
                                layout.partition_key_columns,
                            ),
                        )
                    })
                    .collect::<HashMap<_, _>>()
            };
            let copy_vectors = vector_schema_changed
                || (private_source_coordinates.is_some()
                    && live_tables.iter().any(|(table, meta)| {
                        affected_tables.contains(table)
                            && meta
                                .columns
                                .iter()
                                .any(|column| matches!(column.column_type, ColumnType::Vector(_)))
                    }))
                || (coordinates.is_some()
                    && changes.rows.iter().any(|row| {
                        live_tables.get(&row.table).is_some_and(|meta| {
                            meta.columns
                                .iter()
                                .any(|column| matches!(column.column_type, ColumnType::Vector(_)))
                        })
                    }))
                || !changes.vectors.is_empty()
                || selected_shapes(&live_tables) != selected_shapes(&side_semantic.tables);
            let mut dag_types = |tables: &HashMap<String, TableMeta>| -> Result<BTreeSet<String>> {
                let mut types = BTreeSet::new();
                for edge_type in tables
                    .iter()
                    .filter(|(table, _)| affected_tables.contains(*table))
                    .flat_map(|(_, meta)| meta.dag_edge_types.iter())
                {
                    if !types.contains(edge_type) {
                        vector_copy_owner.reserve(edge_type.len().saturating_add(256))?;
                        types.insert(edge_type.clone());
                    }
                }
                Ok(types)
            };
            let old_dag_types = dag_types(&live_tables)?;
            let new_dag_types = dag_types(&side_semantic.tables)?;
            let mut affected_graph_edge_types = HashSet::new();
            let mut retain_type = |edge_type: &str| -> Result<()> {
                if !affected_graph_edge_types.contains(edge_type) {
                    vector_copy_owner.reserve(edge_type.len().saturating_add(512))?;
                    affected_graph_edge_types.insert(edge_type.to_owned());
                }
                Ok(())
            };
            for edge_type in changes
                .edges
                .iter()
                .map(|edge| edge.edge_type.as_str())
                .chain(
                    old_dag_types
                        .symmetric_difference(&new_dag_types)
                        .map(String::as_str),
                )
            {
                retain_type(edge_type)?;
            }
            // Ordinary graph-shaped tables need no DAG declaration. Their
            // visible row types still identify the exact graph replacement.
            // Visit one original source window instead of collecting every
            // retained body twice just to read this scalar column.
            self.visit_link_types_of_dropped_link_tables(
                &stage_shape_changes.ddl,
                &live_tables,
                read_memory.clone(),
                &mut retain_type,
            )?;
            let mut copy_graph = !affected_graph_edge_types.is_empty()
                || affected_tables.iter().any(|table| {
                    live_tables
                        .get(table)
                        .is_some_and(has_graph_edge_table_shape)
                        || side_semantic
                            .tables
                            .get(table)
                            .is_some_and(has_graph_edge_table_shape)
                });
            // All row shapes use the same paged mutable source, including
            // owning rows of vector/graph replacements. Their final ownership
            // checks below probe exact source coordinates instead of an empty
            // resident row map. Vector/edge payload gathering remains separate.
            let root = self.writer_read_root()?.ok_or_else(|| {
                Error::Other("native vector preparation lost its captured publication".into())
            })?;
            loan.write
                .prepare_vectors(loan.store, loan.original, root, read_memory.clone())?;
            let working_vectors =
                crate::persistence::received_working_vector::WorkingVectorCatalogOwner::prepare(
                    loan.original,
                    loan.funder,
                )?;
            loan.write
                .install_vector_catalog(loan.original, working_vectors.clone())?;
            let (mut metadata_copy_charges, mut working) = self
                .detached_received_schema_working_database_with_sources(
                    &side_semantic.tables,
                    inputs.receiver_tx,
                    loan.original,
                    FundedWorkingGraphSource::prepare(loan.funder)?,
                    working_vectors.clone(),
                    &affected_tables,
                    side_metadata_required,
                    inputs.terminal_refusal_context,
                    &replay_changes.rows,
                    inputs.receipt.as_ref(),
                    inputs.push_outcome_key,
                    vector_copy_owner,
                    read_memory.clone(),
                )?;
            if including_sync_rows {
                working.received_staging_access =
                    Some(ReceivedStagingAccess::prepare(self, &affected_tables)?);
            }
            let dropped_row_graph = self.received_row_graph_drop_witnesses(
                &working,
                &Self::sync_dropped_tables(&stage_shape_changes.ddl),
                vector_copy_owner,
            )?;
            let generations_before_replay = working.in_memory_table_generations.lock().clone();
            #[cfg(any(test, feature = "test-seams"))]
            take_received_unit_fault_for_test(ReceivedUnitFaultPoint::AfterWorkingCopy)?;
            if private_source_coordinates.is_none() {
                register_vector_layouts(
                    &working.vector_store,
                    &side_semantic.tables,
                    contextdb_vector::MemoryBudgetOwner::clone(
                        &working.finalization.vector_accountant,
                    ),
                    vector_copy_owner,
                    read_memory.clone(),
                )?;
            }
            // The detached row/table image starts with no event-bus state. Seed it
            // from the receiver before replaying authenticated DDL so a later
            // source-ordered route can be idempotent against independently
            // received event-type and sink declarations.
            if side_metadata_required {
                working.seed_received_event_bus_projection(self.received_event_bus_projection());
                working.seed_private_sink_cursor(self.event_bus.next_action_id());
            }
            working
                .capture_detached_sync_write_set
                .store(true, Ordering::SeqCst);
            *working.detached_sync_write_set.lock() = None;
            let detached_policies = match (received, inputs.unattested_policies) {
                (None, Some(declared)) => declared.clone(),
                _ => Self::declared_sync_policies(true),
            };
            // The outer received-schema transaction owns the hub contact row so
            // it can commit with the schema receipt. The detached adjudicator only
            // needs the receipt's source identity and watermark; replaying its
            // contact field would try to write a hub-local table outside this
            // received projection.
            let mut detached_receipt = inputs.receipt.clone();
            if let Some(receipt) = detached_receipt.as_mut() {
                receipt.hub_contact_ms = None;
            }
            // This is the only semantic application of the received rows, edges,
            // and vectors during stage preparation.  It preserves the established
            // whole-key adoption, empty-vector delete, remote-owner remapping and
            // conflict branches instead of trying to recreate them from wire
            // fields here.  Its commit is private to `working`.
            // Keep a distinct original-funded CURRENT scope through the
            // synchronous source loan; the stage may move its affected set.
            let graph_schema_scope = {
                let frame = crate::persistence::OriginalPersistenceFrame::new(loan.original)?;
                frame.reserve(affected_tables.iter().fold(0usize, |bytes, name| {
                    bytes.saturating_add(name.len()).saturating_add(256)
                }))?;
                (affected_tables.clone(), frame.into_reservation())
            };
            let mut refused_result = None;
            // Replay declaration lifetimes separately from the final schema image.
            // The list and its strings are admitted before allocating and retained
            // with the stage; the scratch catalog has its own existing owner.
            let retired_graph_bytes = stage_shape_changes
                .ddl
                .iter()
                .filter_map(Self::ddl_affected_table)
                .map(|table| table.len().saturating_add(std::mem::size_of::<String>()))
                .sum();
            let retired_graph_charge = StagedCopyCharge::admit_for_host(
                &self.tx_mgr,
                &self.finalization.accountant,
                contextdb_vector::MemoryBudgetOwner::clone(&self.finalization.vector_accountant),
                retired_graph_bytes,
                "received_row_graph_lifetimes",
            )?;
            let mut retired_row_graph_tables = Vec::with_capacity(
                stage_shape_changes
                    .ddl
                    .iter()
                    .filter_map(Self::ddl_affected_table)
                    .count(),
            );
            {
                let (_catalog_charge, mut catalog) =
                    self.selected_received_catalog(&stage_shape_changes, std::iter::empty())?;
                for ddl in &stage_shape_changes.ddl {
                    let before = Self::ddl_affected_table(ddl)
                        .and_then(|table| catalog.get(table))
                        .is_some_and(has_graph_edge_table_shape);
                    Self::apply_sync_table_ddl_to_projected_meta(&mut catalog, ddl);
                    if let Some(table) = Self::ddl_affected_table(ddl)
                        && before
                        && !catalog.get(table).is_some_and(has_graph_edge_table_shape)
                        && !retired_row_graph_tables.iter().any(|name| name == table)
                    {
                        retired_row_graph_tables.push(table.to_owned());
                    }
                }
            }
            // Proved here, against the receiver's own DDL history: the
            // private working store replays without one.
            let _own_history_charge = StagedCopyCharge::admit_for_host(
                &self.tx_mgr,
                &self.finalization.accountant,
                contextdb_vector::MemoryBudgetOwner::clone(&self.finalization.vector_accountant),
                replay_changes
                    .ddl
                    .len()
                    .saturating_mul(std::mem::size_of::<usize>()),
                "prove_received_create_own_history",
            )?;
            let mut own_history_creates = Vec::with_capacity(replay_changes.ddl.len());
            for (position, ddl) in replay_changes.ddl.iter().enumerate() {
                if self.received_create_restates_current_generation(ddl, read_memory.clone())? {
                    own_history_creates.push(position);
                }
            }
            let prepare_attempt = || -> Result<Option<ReceivedSchemaStage>> {
                working_vectors.activate(&working.vector_store, vector_copy_owner)?;
                let applied = working.apply_changes_with_private_source_order(
                    replay_changes.clone(),
                    &detached_policies,
                    inputs.arrivals,
                    inputs.adoption,
                    inputs.apply_role,
                    detached_receipt,
                    inputs.dependency_complete,
                    inputs.terminal_refusal_context,
                    inputs.hub_local_author,
                    validated_lineages,
                    // A verified lineage is AUTHENTICATED evidence of who authored a
                    // row. An unattested unit carries none, so there is nothing to
                    // stage and demanding one would refuse the unit outright.
                    received.is_some() || !validated_lineages.is_empty(),
                    None,
                    inputs.push_outcome_key,
                    // The copy this worker made is already charged to the receiver's
                    // accountant item by item; the unit holds no separate reservation
                    // here -- see the preflight's own note.
                    None,
                    vector_copy_owner,
                    read_memory.clone(),
                    Some(connected_replay::PrivateReplayContext {
                        coordinates: private_source_coordinates,
                        authoritative_generations: received.map(|_| source_order.as_slice()),
                        receiver_tx: inputs.receiver_tx,
                        end: local_lsn,
                        own_history_creates: &own_history_creates,
                    }),
                );
                if let Some(access) = &working.received_staging_access {
                    let miss = access.miss.lock().take();
                    match miss {
                        Some(ReceivedStagingMiss::Refused(error)) => return Err(error),
                        Some(ReceivedStagingMiss::Table { name, .. })
                            if !affected_tables.contains(&name)
                                && callback_tables.len() < live_table_count
                                && self.relational_store.table_meta.read().contains_key(&name) =>
                        {
                            // Even a successful callback may have swallowed the
                            // private lookup refusal. Discard that incomplete image
                            // and replay with the missing table's dependency closure.
                            callback_tables.insert(name);
                            return Ok(None);
                        }
                        _ => {}
                    }
                }
                let mut apply_result = match applied {
                    Ok(apply_result) => apply_result,
                    Err(Error::TableNotFound(table))
                        if including_sync_rows
                            && !affected_tables.contains(&table)
                            && callback_tables.len() < live_table_count
                            && self.relational_store.table_meta.read().contains_key(&table) =>
                    {
                        callback_tables.insert(table);
                        return Ok(None);
                    }
                    Err(error) => return Err(error),
                };
                if working
                    .detached_sync_write_set
                    .lock()
                    .as_ref()
                    .is_some_and(|capture| capture.refused)
                {
                    refused_result = Some(apply_result);
                    return Ok(None);
                }
                // An unattested CREATE still needs a locally significant generation,
                // exactly as the item-by-item door this replaces minted through the
                // ordinary local CREATE path. The private replay above ran that same
                // path, seeded with this receiver's authoritative counters, so it has
                // already done the only correct thing: a CREATE that restates a table
                // the receiver holds installed nothing and left its generation alone,
                // and each real CREATE -- including every incarnation of a table the
                // unit drops and recreates -- advanced the counter once, in author
                // order, because a drop retains the counter. Read that result rather
                // than allocating ahead of the replay. It is attached only to the
                // per-table record, never to the per-item sidecar the planner already
                // serialized: nobody signed these counters.
                if received.is_none() {
                    let replayed_generations = working.in_memory_table_generations.lock().clone();
                    for entry in source_order.iter_mut() {
                        if entry.generation.table_generation.is_none()
                            && let DdlChange::CreateTable { .. } = &entry.change
                            && let Some(table) = entry.generation.table.as_ref()
                            && let Some(generation) = replayed_generations.get(table).copied()
                            && generations_before_replay.get(table).copied() != Some(generation)
                        {
                            entry.generation.table_generation = Some(generation);
                        }
                    }
                }
                // The funding of the ownership keys custody retirement moves into the
                // write set, declared first so every exit drops the write set before it.
                let mut retired_custody_keys = Vec::with_capacity(1);
                let captured = working
                    .detached_sync_write_set
                    .lock()
                    .take()
                    // DDL-only units use the legacy DDL mutation path and therefore
                    // have no data WriteSet.  Their projected schema is still valid;
                    // there simply are no receipt/lineage/tombstone writes to carry.
                    .unwrap_or_default();
                // Declare the invoice/effects before their write-set body: a later
                // preparation refusal destroys the body before returning its debit.
                let mut private_prefix_effects = captured.prefix;
                let mut captured_rows_invoice = captured.rows_invoice;
                let mut finalized_write_set = captured.write_set;
                if let Some(effects) = private_prefix_effects.as_mut() {
                    effects.take_captured_rows_invoice(captured_rows_invoice.take());
                } else if captured_rows_invoice.is_some() {
                    return Err(Error::Other(
                        "paged captured rows have no owning effects carrier".into(),
                    ));
                }
                if let Some(coordinates) = coordinates {
                    coordinates.stage_membership(&mut finalized_write_set.config_writes)?;
                }
                let fallback_arrival_tombstones = changes
                    .rows
                    .iter()
                    .filter(|row| {
                        row.deleted && matches!(inputs.arrivals.get(&row.lsn), Some(None))
                    })
                    .map(|row| Self::sync_tombstone_key(&row.table, &row.natural_key))
                    .collect::<HashSet<_>>();
                let mut accepted_positions = match (coordinates, connected_overlay.as_ref()) {
                    (Some(coordinates), Some(overlay)) => overlay.accepted_row_positions(
                        &finalized_write_set,
                        coordinates,
                        private_source_coordinates.is_some(),
                        read_memory.clone(),
                    )?,
                    _ => BTreeMap::new(),
                };
                if connected_overlay
                    .as_ref()
                    .is_some_and(connected_replay::ConnectedOverlay::row_only)
                {
                    for position in accepted_positions.values_mut() {
                        *position = local_lsn;
                    }
                }
                if let Some(effects) = private_prefix_effects.as_mut() {
                    effects.add_derived_positions(&finalized_write_set, &mut accepted_positions)?;
                }
                let restamped_vector_image = Self::restamp_detached_received_schema_commit(
                    &working,
                    &mut finalized_write_set,
                    local_lsn,
                    inputs.receiver_tx,
                    &fallback_arrival_tombstones,
                    coordinates.zip(connected_overlay.as_ref()),
                    &accepted_positions,
                    read_memory.clone(),
                )?;
                {
                    {
                        let metadata = working.relational_store.table_meta.read();
                        for table in metadata
                            .keys()
                            .filter(|table| affected_tables.contains(*table))
                        {
                            loan.write.prepare_rows(loan.store, loan.original, table)?;
                        }
                        loan.write.apply_row_replacements(
                            loan.store,
                            loan.original,
                            &metadata,
                            &finalized_write_set.relational_deletes,
                            &finalized_write_set.relational_inserts,
                        )?;
                        loan.write
                            .apply_provenance(loan.original, &finalized_write_set)?;
                    }
                    let source=crate::persistence::received_working_source::WorkingReadSource::current_for(working.tx_mgr.identity())?
                .ok_or_else(||Error::Other("native working projection lost its synchronous source".into()))?;
                    source.advance_frontier(
                        working.tx_mgr.capture_published_read().snapshot,
                        local_lsn,
                    )?;
                }
                let mut authored_pending_rows = match (coordinates, connected_overlay.as_ref()) {
                    (Some(coordinates), Some(overlay)) => overlay.authored_rows(
                        &finalized_write_set,
                        coordinates,
                        inputs.receiver_tx,
                        private_source_coordinates.is_some(),
                        changes,
                        read_memory.clone(),
                    )?,
                    _ => Vec::new(),
                };
                let mut accepted_row_history = match (coordinates, connected_overlay.as_ref()) {
                    (Some(coordinates), Some(overlay)) => overlay.accepted_row_history(
                        &finalized_write_set,
                        coordinates,
                        private_source_coordinates.is_some(),
                        read_memory.clone(),
                    )?,
                    _ => Vec::new(),
                };
                // Exact input members are distinct from callback-authored history.
                // Fund their durable config bodies before encoding or collecting them.
                metadata_copy_charges.push(StagedCopyCharge::admit_for_host(
                    &self.tx_mgr,
                    &self.finalization.accountant,
                    contextdb_vector::MemoryBudgetOwner::clone(
                        &self.finalization.vector_accountant,
                    ),
                    changes.rows.len().saturating_mul(1024),
                    "stage_received_members",
                )?)?;
                let received_members = Self::stage_received_row_members(
                    changes,
                    connected_overlay.as_ref(),
                    coordinates,
                    local_lsn,
                    private_source_coordinates.is_some(),
                    &finalized_write_set,
                    read_memory.clone(),
                )?;
                finalized_write_set.config_writes.extend(received_members);
                if let Some(effects) = private_prefix_effects.as_mut() {
                    authored_pending_rows.append(&mut effects.derived_rows);
                    accepted_row_history.append(&mut effects.derived_history);
                }
                finalized_write_set
                    .config_writes
                    .retain(|(key, _)| !key.starts_with("__contextdb.detached.accepted_row.v1."));
                if let Some(prepare) = delivery_prepare.as_deref_mut() {
                    // The detached image supplies final schema/rows; custody authority
                    // remains the receiver's current committed view until publication.
                    *working.sync_incarnations.lock() = self.sync_incarnations.lock().clone();
                    // The working image reads the receiver's committed journal
                    // durably; nothing of the journal is copied into it. Pinned to
                    // the receiver's committed root at preparation: every custody
                    // read of the unit sees that one root.
                    *working.custody_cache.lock() =
                        Some(crate::custody::store::Store::durable_pinned(
                            self.latest_custody_root()?,
                            self.accountant.clone(),
                        )?);
                    for record in prepare.prepare(&working, &mut finalized_write_set)? {
                        record.verify()?;
                        finalized_write_set
                            .config_writes
                            .push((record.key()?, crate::custody::records::encode(&record)?));
                    }
                }
                let custody_memory = self.prepare_custody_commit(
                    &mut finalized_write_set,
                    read_memory.clone(),
                    &mut retired_custody_keys,
                )?;
                // The detached apply owns no externally visible receiver LSN. The
                // active transaction reserved this identity before preparation, so
                // preserve every observed outcome field and restamp only `new_lsn`.
                apply_result.new_lsn = local_lsn;
                let received_schema_manifest = received
                    .map(|received| Self::received_schema_manifest(received, &apply_result))
                    .transpose()?;
                if let Some((key, manifest)) = received_schema_manifest.as_ref() {
                    persistence
                        .config_values
                        .push((key.clone(), RedbPersistence::encode_config_value(manifest)?));
                }
                // Schema-bound side effects evolve from the receiver's current live
                // definitions in exact authored DDL order. The detached database is
                // authoritative for rows/tables, but begins without retained
                // event/trigger definitions.
                let semantic = ReceivedSchemaSemanticProjection {
                    catalog_charge: side_semantic.catalog_charge.take(),
                    tables: working.relational_store.table_meta.read().clone(),
                    event_bus: std::mem::take(&mut side_semantic.event_bus),
                    trigger: std::mem::take(&mut side_semantic.trigger),
                };
                let source_backed = self.relational_store.uses_disk_backed_committed_bodies();
                if source_backed {
                    for rows in working.relational_store.tables.write().values_mut() {
                        rows.sort_unstable_by_key(|row| (row.row_id, row.created_tx, row.lsn));
                    }
                }
                // The completed private image already owns these bodies and their
                // admission. Borrow it for preparation; its existing final owner is
                // moved into the stage below, rather than cloning the full row image.
                let prepared_rows_for_memory = working.relational_store.tables.read();
                let source_sidecars = if source_backed {
                    HashMap::new()
                } else {
                    working.relational_store.sync_source_sidecars_snapshot()
                };
                let projected_next_row_id = working.relational_store.next_row_id();
                // Keep the exact config writes the established adjudicator finalized:
                // receipt/watermark max semantics, accepted-row lineage, accepted
                // delete lineage, terminal clears and author evidence all belong to
                // that one WriteSet.  Rebuilding them from every wire row would make
                // a refused row look accepted.
                for (key, value) in finalized_write_set.config_writes.iter().cloned() {
                    if key.starts_with("sync_row_lineage.") {
                        persistence.lineage_values.push((key, value));
                    } else if key.starts_with("sync_row_author.") {
                        persistence.accepted_author_values.push((key, value));
                    } else if key.starts_with("sync_terminal_refusal.v1.") {
                        persistence.terminal_values.push((key, value));
                    } else if key.starts_with("sync_applied_push_watermark_node.") {
                        persistence.applied_push_watermark_values.push((key, value));
                    } else {
                        // Includes accepted-delete `DurableLineageRecord` values;
                        // its durable key is intentionally distinct from the
                        // immutable row-lineage sidecar above.
                        persistence.config_values.push((key, value));
                    }
                }
                persistence
                    .config_max_u64_keys
                    .extend(finalized_write_set.config_max_u64_keys.iter().cloned());
                // Reopen must retain the authoritative generation for tables born in
                // this received unit.  The per-DDL generation sidecars establish
                // provenance for the source vector; the table key is the ordinary
                // local DML lookup used by later grant-table deletes/updates.
                for entry in &source_order {
                    if let (Some(table), Some(generation)) =
                        (&entry.generation.table, entry.generation.table_generation)
                    {
                        persistence.config_values.push((
                            Self::durable_lineage_table_generation_key(table),
                            RedbPersistence::encode_config_value(&generation)?,
                        ));
                    }
                }
                // The replacement metadata below is a distinct physical copy. Move
                // its existing admission before allocation and retain the authentic
                // original until that same debit becomes the retained store invoice.
                let original_target_bytes = if preparation_original.is_some() {
                    let graph_bytes = if self.graph_store.is_source_backed() {
                        0
                    } else {
                        working
                            .graph_store
                            .forward_adj
                            .read()
                            .values()
                            .flat_map(|entries| entries.iter())
                            .fold(0usize, |bytes, edge| {
                                bytes.saturating_add(edge.estimated_bytes())
                            })
                    };
                    Some(
                        self.received_schema_replacement_bytes::<AdjEntry>(
                            &semantic.tables,
                            &prepared_rows_for_memory,
                            &[],
                        )
                        .saturating_add(graph_bytes),
                    )
                } else {
                    None
                };
                // The persistence input is a distinct physical edge copy from the
                // replacement graph's forward/reverse bodies. Its invoice stays with
                // the stage after the target invoice transfers into the live store.
                let (received_edge_input_original, _received_edge_input_count) = if let Some(
                    original,
                ) =
                    preparation_original
                {
                    let (count, bytes) = working
                        .graph_store
                        .forward_adj
                        .read()
                        .values()
                        .flat_map(|entries| entries.iter())
                        .try_fold((0usize, 0usize), |(count, bytes), edge| {
                            Ok::<_, Error>((
                                count.saturating_add(1),
                                bytes
                                    .saturating_add(std::mem::size_of::<AdjEntry>())
                                    .saturating_add(edge.edge_type.len())
                                    .saturating_add(
                                        crate::memory_accounting::preparation_values_copy_bytes(
                                            &edge.properties,
                                        )?,
                                    ),
                            ))
                        })?;
                    (
                        Some(crate::memory_accounting::reserve_preparation_from_original(
                            original,
                            bytes,
                            "sync",
                            "prepare_received_graph_input",
                            "Reduce the replicated graph working set or raise MEMORY_LIMIT before receiving schema.",
                        )?),
                        count,
                    )
                } else {
                    (None, 0)
                };
                let mut received_image_original = match original_target_bytes {
                    Some(bytes) => {
                        Some(crate::memory_accounting::reserve_preparation_from_original(
                            preparation_original.expect("received target keeps its original loan"),
                            bytes,
                            "sync",
                            "prepare_received_schema_replacement",
                            "Reduce the replicated schema/data working set or raise MEMORY_LIMIT before receiving schema.",
                        )?)
                    }
                    None => None,
                };
                // Declaration order keeps a prepared swap's debit alive until every
                // replacement body has physically dropped on any later refusal.
                let mut memory_swap;
                // Durable publication installs only metadata/source routes. Keep one
                // staged row image for its durable writer and do not build the row and
                // posting copies that the source-backed publisher immediately discards.
                let mut relational = semantic
                    .tables
                    .iter()
                    .filter(|(table, _)| affected_tables.contains(*table))
                    .map(|(table, meta)| {
                        let projection = RelationalStore::table_projection(
                            table.clone(),
                            meta.clone(),
                            if source_backed {
                                Vec::new()
                            } else {
                                prepared_rows_for_memory
                                    .get(table)
                                    .cloned()
                                    .unwrap_or_default()
                            },
                        );
                        let sources = source_sidecars
                            .iter()
                            .filter(|_| !source_backed)
                            .filter_map(|((source_table, row_id), (lsn, kind))| {
                                (source_table == table).then_some((*row_id, *lsn, *kind))
                            })
                            .collect();
                        RelationalStore::with_sync_sources(projection, sources)
                    })
                    .collect::<Vec<_>>();
                for projection in &mut relational {
                    projection
                        .admit_current_memberships(self.finalization.accountant.read_budget()?)?;
                }

                // Discovery/callbacks can select additional types, including an empty
                // replacement. Freeze only after restamp and complete validation.
                let graph_occurrences = loan.write.graph_capturing(loan.original)?;
                let graph_selection = {
                    for kind in &affected_graph_edge_types {
                        loan.write.select_graph_type(loan.original, kind)?;
                    }
                    copy_graph |= loan.write.graph_selection_dirty(loan.original)?;
                    Some(loan.write.freeze_graph_selection(loan.original)?)
                };
                let edges = Self::received_row_graph_image(
                    &working.graph_store,
                    &affected_graph_edge_types,
                    &affected_tables,
                    vector_copy_owner,
                )?;
                let graph = if copy_graph {
                    Some(Self::prepare_received_row_graph_image(
                        if self.graph_store.is_source_backed() {
                            &[]
                        } else {
                            &edges
                        },
                        vector_copy_owner,
                    )?)
                } else {
                    None
                };
                let dag_edge_types = MemGraphExecutor::<DynStore>::prepare_dag_edge_types(
                    self.relational_store
                        .table_meta
                        .read()
                        .iter()
                        .filter(|(table, _)| !affected_tables.contains(*table))
                        .map(|(_, meta)| meta)
                        .chain(semantic.tables.values())
                        .flat_map(|meta| meta.dag_edge_types.iter().cloned()),
                );
                let rank_formulas = Self::prepare_rank_formula_cache(&semantic.tables)?;

                // A vector registry can contain a retired index while a database is
                // opening/recovering.  The projected schema remains authoritative:
                // only entries whose table and declared vector column survived the
                // detached DDL lifecycle may enter the prepared image.
                // Move the SAME once-exported restamped image. Declare its invoice
                // before the entries so all later preparation failures destroy bodies
                // before the executing owner; no whole-registry rebuild/re-export.
                let vector_image_owner = restamped_vector_image.owner;
                let vectors = restamped_vector_image.entries;
                // Memory-only publication keeps its existing borrowed directory.
                // The durable rows are already sorted in place and need no second
                // population-sized array of row references.
                let row_count = if source_backed {
                    0
                } else {
                    prepared_rows_for_memory
                        .values()
                        .map(Vec::len)
                        .sum::<usize>()
                };
                vector_copy_owner.reserve(
                    row_count.saturating_mul(std::mem::size_of::<(&str, &VersionedRow)>()),
                )?;
                let mut row_versions = Vec::with_capacity(row_count);
                if !source_backed {
                    for (table, rows) in &*prepared_rows_for_memory {
                        row_versions.extend(rows.iter().map(|row| (table.as_str(), row)));
                    }
                    row_versions.sort_unstable_by_key(|(table, row)| {
                        (*table, row.row_id, row.created_tx, row.lsn)
                    });
                }
                let mut vectors = vectors;
                let mut ownership_error = None;
                vectors.retain(|placed| {
                    if ownership_error.is_some() {
                        return false;
                    }
                    let entry = &placed.entry;
                    let owns_row = match loan.write.exact_version(
                        loan.original,
                        &entry.index.table,
                        entry.row_id,
                        placed.membership.owning_row_tx,
                        placed.membership.row_lsn,
                    ) {
                        Ok(row) => row.is_some(),
                        Err(error) => {
                            ownership_error = Some(error);
                            false
                        }
                    };
                    owns_row
                        && semantic.tables.get(&entry.index.table).is_some_and(|meta| {
                            meta.columns.iter().any(|column| {
                                column.name == entry.index.column
                                    && matches!(column.column_type, ColumnType::Vector(_))
                            })
                        })
                });
                if let Some(error) = ownership_error {
                    return Err(error);
                }
                drop(row_versions);
                vectors.sort_by(|left, right| {
                    left.entry
                        .index
                        .table
                        .cmp(&right.entry.index.table)
                        .then(left.entry.index.column.cmp(&right.entry.index.column))
                        .then(left.entry.row_id.cmp(&right.entry.row_id))
                        .then(left.entry.created_tx.cmp(&right.entry.created_tx))
                        .then(left.entry.lsn.cmp(&right.entry.lsn))
                        .then(left.partition_key.cmp(&right.partition_key))
                });
                let schemas: Vec<_> = vector_layouts_from_meta(&semantic.tables)
                    .into_iter()
                    .filter(|(index, _)| affected_tables.contains(&index.table))
                    .collect();
                let same_body = |left: &PartitionedVectorEntry, right: &PartitionedVectorEntry| {
                    left.entry.index == right.entry.index
                        && left.entry.row_id == right.entry.row_id
                        && left.entry.created_tx == right.entry.created_tx
                        && left.entry.lsn == right.entry.lsn
                };
                let canonical_count = vectors
                    .iter()
                    .enumerate()
                    .filter(|(index, placed)| {
                        *index == 0 || !same_body(&vectors[*index - 1], placed)
                    })
                    .count();
                if !(source_backed && copy_vectors) {
                    vector_copy_owner.reserve(
                        vectors
                            .len()
                            .saturating_mul(std::mem::size_of::<VectorPartitionMembershipRecord>())
                            .saturating_add(
                                canonical_count.saturating_mul(std::mem::size_of::<VectorEntry>()),
                            ),
                    )?;
                }
                let mut vector_records = if source_backed && copy_vectors {
                    Some(
                        self.persistence
                            .as_ref()
                            .ok_or_else(|| {
                                Error::Other("durable vector projection lost its store".into())
                            })?
                            .prepare_received_vector_records(vector_copy_owner)?,
                    )
                } else {
                    None
                };
                let mut durable_vectors: Vec<VectorEntry> =
                    Vec::with_capacity(if vector_records.is_some() {
                        0
                    } else {
                        canonical_count
                    });
                let mut durable_memberships = Vec::with_capacity(if vector_records.is_some() {
                    0
                } else {
                    vectors.len()
                });
                for (position, placed) in vectors.iter().enumerate() {
                    let mut record_window = vector_copy_owner.scratch();
                    let record_owner = if vector_records.is_some() {
                        &mut record_window
                    } else {
                        &mut *vector_copy_owner
                    };
                    record_owner.reserve(
                        placed
                            .entry
                            .index
                            .table
                            .len()
                            .saturating_add(placed.entry.index.column.len())
                            .saturating_add(placed.partition_key.estimated_bytes())
                            .saturating_add(std::mem::size_of::<VectorPartitionMembershipRecord>()),
                    )?;
                    if position == 0 || !same_body(&vectors[position - 1], placed) {
                        record_owner.reserve(
                            placed
                                .entry
                                .vector
                                .len()
                                .saturating_mul(std::mem::size_of::<f32>())
                                .saturating_add(placed.entry.index.table.len())
                                .saturating_add(placed.entry.index.column.len()),
                        )?;
                        let mut body = placed.entry.clone();
                        body.deleted_tx = placed.canonical_deleted_tx;
                        if let Some(records) = &mut vector_records {
                            if affected_tables.contains(&body.index.table) {
                                records.bodies.push(&body)?;
                            }
                        } else {
                            durable_vectors.push(body);
                        }
                    } else {
                        let previous = &vectors[position - 1];
                        if previous.canonical_deleted_tx != placed.canonical_deleted_tx
                            || previous.entry.vector.len() != placed.entry.vector.len()
                            || previous
                                .entry
                                .vector
                                .iter()
                                .zip(placed.entry.vector.iter())
                                .any(|(left, right)| left.to_bits() != right.to_bits())
                        {
                            return Err(crate::funded_error::corruption_text(Ok(read_memory.clone()), self.persistence.as_ref().map(|store| store.path().display().to_string())
                            .unwrap_or_else(|| ":memory:".into()), "vector partition durability record is corrupt: received placements disagree on their canonical body".into()));
                        }
                    }
                    let membership = VectorPartitionMembershipRecord {
                        index: placed.entry.index.clone(),
                        partition_key: placed.partition_key.clone(),
                        row_id: placed.entry.row_id,
                        row_created_tx: placed.membership.owning_row_tx,
                        row_lsn: placed.membership.row_lsn,
                        vector_created_tx: placed.entry.created_tx,
                        vector_lsn: placed.entry.lsn,
                        visible_from: placed.membership.visible_from,
                        deleted_tx: placed.membership.deleted_tx,
                    };
                    if let Some(records) = &mut vector_records {
                        if affected_tables.contains(&membership.index.table) {
                            records.memberships.push(&membership)?;
                        }
                    } else {
                        durable_memberships.push(membership);
                    }
                }
                if copy_vectors {
                    let records = vector_records.as_mut().ok_or_else(|| {
                        Error::Other("paged vector image lost its record sink".into())
                    })?;
                    loan.write.visit_vector_memberships_original(
                        loan.original,
                        |member, _, _| {
                            if !affected_tables.contains(&member.index.table) {
                                return Ok(());
                            }
                            let Some(meta) = semantic.tables.get(&member.index.table) else {
                                return Ok(());
                            };
                            if !meta.columns.iter().any(|column| {
                                column.name == member.index.column
                                    && matches!(column.column_type, ColumnType::Vector(_))
                            }) {
                                return Ok(());
                            }
                            if loan
                                .write
                                .exact_version(
                                    loan.original,
                                    &member.index.table,
                                    member.row_id,
                                    member.row_created_tx,
                                    member.row_lsn,
                                )?
                                .is_none()
                            {
                                return Ok(());
                            }
                            let mut record_window = vector_copy_owner.scratch();
                            record_window.reserve(
                                member
                                    .index
                                    .table
                                    .len()
                                    .saturating_add(member.index.column.len())
                                    .saturating_add(member.partition_key.estimated_bytes())
                                    .saturating_add(std::mem::size_of::<
                                        VectorPartitionMembershipRecord,
                                    >()),
                            )?;
                            records.memberships.push(member)?;
                            let body = loan
                                .write
                                .vector_body_for_membership(loan.original, member)?;
                            record_window.reserve(
                                body.value()
                                    .vector
                                    .len()
                                    .saturating_mul(std::mem::size_of::<f32>())
                                    .saturating_add(body.value().index.table.len())
                                    .saturating_add(body.value().index.column.len()),
                            )?;
                            // Several retained placements can name one immutable body.
                            // The existing paged final-image validator deduplicates that
                            // physical identity and verifies every repeated bit pattern.
                            records.bodies.push(body.value())?;
                            Ok(())
                        },
                    )?;
                }
                // A unit that copies vectors replaces these columns with the arriving
                // layout; otherwise the live columns take only the new policy.
                let mut vector_policy_layouts = Vec::new();
                let mut vector_policy_tables = HashSet::new();
                if !copy_vectors && !policy_only_vector_layouts.is_empty() {
                    vector_copy_owner.reserve(policy_only_vector_layouts.len().saturating_mul(
                        std::mem::size_of::<contextdb_vector::PreparedVectorPolicyLayout>()
                            + 2 * std::mem::size_of::<String>()
                            + 1,
                    ))?;
                    vector_policy_layouts.reserve_exact(policy_only_vector_layouts.len());
                    vector_policy_tables.reserve(policy_only_vector_layouts.len());
                    for (index, layout) in policy_only_vector_layouts.drain() {
                        if let Some(prepared) = self.vector_store.prepare_policy_layout_update(
                            &index,
                            layout,
                            vector_copy_owner,
                        )? {
                            vector_policy_layouts.push(prepared);
                        }
                        vector_policy_tables.insert(index.table);
                    }
                }
                // Declaration-only changes keep continuing columns bound to their
                // committed partition sources, including dormant retained placements.
                let mut declarations_only = source_backed
                    && changes.rows.is_empty()
                    && changes.vectors.is_empty()
                    && changes.edges.is_empty();
                if declarations_only {
                    for (index, layout) in &schemas {
                        if self.vector_store.contains_index(index) {
                            declarations_only &=
                                self.vector_store.with_index_layout(index, |old| {
                                    old.dimension == layout.dimension
                                        && old.quantization == layout.quantization
                                        && old.partition_key_columns == layout.partition_key_columns
                                })?;
                        }
                    }
                }
                let vector = if copy_vectors && declarations_only {
                    Some(
                        self.vector_store
                            .prepare_received_schema_declarations_publication(
                                schemas,
                                &affected_tables,
                                |table| {
                                    stage_shape_changes.ddl.iter().any(|ddl| {
                            matches!(ddl, DdlChange::DropTable { name } if name == table)
                        })
                                },
                                contextdb_vector::MemoryBudgetOwner::clone(
                                    &self.finalization.vector_accountant,
                                ),
                                vector_image_owner,
                            )?,
                    )
                } else {
                    let vector = copy_vectors
                        .then(|| {
                            self.vector_store
                                .prepare_partitioned_received_schema_publication(
                                    schemas,
                                    vectors,
                                    contextdb_vector::MemoryBudgetOwner::clone(
                                        &self.finalization.vector_accountant,
                                    ),
                                    vector_image_owner,
                                )
                        })
                        .transpose()?;
                    vector
                        .map(|publication| {
                            self.vector_store
                                .prepare_received_schema_tables_publication(
                                    publication,
                                    &affected_tables,
                                    vector_copy_owner,
                                )
                        })
                        .transpose()?
                };
                let new_bytes = self.received_schema_replacement_bytes(
                    &semantic.tables,
                    &prepared_rows_for_memory,
                    &edges,
                );

                let old_tables = std::mem::take(&mut live_tables)
                    .into_iter()
                    .filter(|(table, _)| affected_tables.contains(table))
                    .collect::<HashMap<_, _>>();
                // Retirement pricing borrows the outgoing image rather than copying it.
                let old_row_bytes = self
                    .relational_store
                    .tables
                    .read()
                    .iter()
                    .filter(|(table, _)| affected_tables.contains(*table))
                    .fold(0usize, |bytes, (table, rows)| {
                        bytes.saturating_add(old_tables.get(table).map_or(0, |meta| {
                            self.received_schema_table_rows_bytes(table, meta, rows)
                        }))
                    });
                let old_edges = if copy_graph {
                    Self::received_row_graph_image(
                        &self.graph_store,
                        &affected_graph_edge_types,
                        &affected_tables,
                        vector_copy_owner,
                    )?
                } else {
                    Vec::new()
                };
                let old_bytes = self
                    .received_schema_replacement_bytes(&old_tables, &HashMap::new(), &old_edges)
                    .saturating_add(old_row_bytes);
                // A transaction another caller holds open over one of these tables
                // staged its rows under the shape this unit is replacing. Its rows have
                // to be re-priced at the moment the new shape becomes the live one, or
                // the release hands back the figure admission never took -- which is
                // what happens when an arriving CREATE INDEX changes the index set
                // under an open transaction. The local authoring door already does
                // exactly this; the received door has to do it too, because it is now
                // the door every arriving index change takes.
                let mut staged_old = 0usize;
                let mut staged_new = 0usize;
                let mut staged_repricing: HashMap<TxId, Vec<(String, RowId, usize)>> =
                    HashMap::new();
                {
                    let live_meta = self.relational_store.table_meta.read();
                    for (table, new_meta) in semantic
                        .tables
                        .iter()
                        .filter(|(table, _)| affected_tables.contains(*table))
                    {
                        let Some(old_meta) = live_meta.get(table) else {
                            continue;
                        };
                        let (old, new, per_tx) =
                            self.staged_rows_priced_across_shape_change(table, old_meta, new_meta);
                        staged_old = staged_old.saturating_add(old);
                        staged_new = staged_new.saturating_add(new);
                        for (tx, rows) in per_tx {
                            staged_repricing.entry(tx).or_default().extend(rows);
                        }
                    }
                }
                // Complete fallible history preparation before transferring the target
                // debit. On refusal the original local still outlives all target bodies.
                let prepared_history = if !self.relational_store.uses_disk_backed_committed_bodies()
                {
                    let mut history = self.relational_store.prepare_schema_generation(
                        affected_tables.iter(),
                        self.engine_sync_read_memory()?,
                    )?;
                    for (table, meta, row) in &authored_pending_rows {
                        history.retain_owned(
                            table,
                            meta,
                            row,
                            self.finalization.accountant.read_budget()?,
                        )?;
                    }
                    Some(history)
                } else {
                    None
                };
                memory_swap = match received_image_original.as_mut() {
                    Some(original) => {
                        if original_target_bytes != Some(new_bytes) {
                            return Err(EngineMetadataJournal::identity_error());
                        }
                        // Rows an open transaction staged under the outgoing shape are
                        // re-priced under the new one; that extra is part of the
                        // target image and is admitted from the same original.
                        if staged_new != 0 {
                            crate::memory_accounting::grow_preparation_from_original(
                                original,
                                staged_new,
                                "sync",
                                "prepare_received_staged_repricing",
                                "Reduce the open transactions staged over the received tables or raise MEMORY_LIMIT before receiving schema.",
                            )?;
                        }
                        PreparedMemorySwap::from_original_preparation(
                            original,
                            old_bytes.saturating_add(staged_old),
                            new_bytes.saturating_add(staged_new),
                            0,
                        )?
                    }
                    None => PreparedMemorySwap::prepare(
                        crate::memory_accounting::AccountantHandle::clone(
                            &self.finalization.accountant,
                        ),
                        old_bytes.saturating_add(staged_old),
                        new_bytes.saturating_add(staged_new),
                        0,
                    )?,
                }
                .carrying_staged_admissions(StagedAdmissionRepricing {
                    tx_mgr: self.tx_mgr.clone(),
                    per_tx: staged_repricing,
                });
                memory_swap.history = prepared_history;
                // File-backed readers use the same-commit authored pool through their
                // pinned WriterSnapshot. Keeping a second resident body/declaration
                // root would retain the whole unit again without serving any read.

                let queue_mutation_lease = self.event_bus.begin_queue_mutation();
                let prefix_sinks = private_prefix_effects
                    .as_mut()
                    .map(|effects| effects.take_prepared_sinks(self.event_bus.next_action_id()))
                    .transpose()?
                    .flatten();
                let prepared_sink_events = match prefix_sinks {
                    Some(events) => events,
                    None => working.prepare_received_sink_events_from_source(
                        &finalized_write_set,
                        &semantic.event_bus,
                        loan.write,
                        loan.original,
                        self.event_bus.next_action_id(),
                        publication_vector_copy_owner
                            .as_mut()
                            .expect("one publication owner"),
                        read_memory.clone(),
                    )?,
                };
                self.event_bus
                    .ensure_queue_capacity(&prepared_sink_events.events)?;
                // A row received in the same source transaction as a trigger's
                // declaration fires it, even when the transaction drops the trigger
                // again after the row: the audit sees every trigger the unit declared,
                // not only the unit's final set.
                let unit_declares_triggers = changes.ddl.iter().any(|ddl| {
                    matches!(
                        ddl,
                        DdlChange::CreateTrigger { .. }
                            | DdlChange::CreateTriggerIncludingSync { .. }
                    )
                });
                let audit_projection = if unit_declares_triggers {
                    let mut projection = self.sync_pull_trigger_audit_projection(&changes.ddl)?;
                    projection.extend(
                        semantic
                            .trigger
                            .iter()
                            .map(|(name, declaration)| (name.clone(), declaration.clone())),
                    );
                    projection
                } else {
                    semantic.trigger.clone()
                };
                let retained_plain_firings = if private_source_coordinates.is_some() {
                    private_prefix_effects
                        .as_mut()
                        .map(|effects| effects.synthetic_firings_for_commit())
                        .transpose()?
                } else {
                    None
                };
                let mut trigger_audit_entries = self
                    .committed_sync_pull_trigger_audits_with_source_order(
                        &finalized_write_set,
                        local_lsn,
                        Some(&audit_projection),
                        coordinates.map(|coordinates| (changes, coordinates)),
                        retained_plain_firings,
                        read_memory.clone(),
                    )?;
                if let Some(effects) = private_prefix_effects.as_mut() {
                    trigger_audit_entries.extend(effects.committed_audits(inputs.receiver_tx)?);
                    trigger_audit_entries.sort_by_key(|entry| entry.firing_lsn);
                }
                let (trigger_audits, next_trigger_audit_index) = self
                    .trigger
                    .prepare_received_persistence_audits(&trigger_audit_entries);
                let event_bus = side_metadata_required
                    .then(|| {
                        self.prepare_received_event_bus_publication_from_projection(
                            semantic.event_bus,
                        )
                    })
                    .transpose()?;
                let (trigger, native_trigger) = if side_metadata_required {
                    if let Some(original) = preparation_original {
                        (
                            None,
                            Some(self.prepare_original_trigger_publication(
                                original,
                                &semantic.trigger,
                            )?),
                        )
                    } else {
                        (
                            Some(self.prepare_received_trigger_publication_from_projection(
                                semantic.trigger,
                            )?),
                            None,
                        )
                    }
                } else {
                    (None, None)
                };
                if let Some(publication) = &event_bus {
                    persistence
                        .config_values
                        .extend(publication.config_values.iter().cloned());
                }
                if let Some(publication) = &trigger {
                    persistence
                        .config_values
                        .extend(publication.config_values.iter().cloned());
                }
                if let Some(publication) = &native_trigger {
                    let original =
                        preparation_original.ok_or_else(EngineMetadataJournal::identity_error)?;
                    let bytes = publication
                        .publication
                        .config_values
                        .iter()
                        .try_fold(0usize, |bytes, (key, value)| {
                            bytes
                                .checked_add(key.len())
                                .and_then(|bytes| bytes.checked_add(value.len()))
                                .ok_or_else(EngineMetadataJournal::identity_error)
                        })?
                        .checked_mul(2)
                        .ok_or_else(EngineMetadataJournal::identity_error)?;
                    // Persistence transport and the durable projection are independent
                    // copies. Their existing enclosing metadata owner prices vector
                    // controls; this fee owns both actual key/encoded byte populations.
                    native_trigger_config_fee =
                        Some(crate::memory_accounting::reserve_preparation_from_original(
                            original,
                            bytes,
                            "trigger",
                            "received_trigger_config_copies",
                            "Raise MEMORY_LIMIT to prepare trigger configuration.",
                        )?);
                    persistence
                        .config_values
                        .extend(publication.publication.config_values.iter().cloned());
                }
                let mut mirrors = self.received_schema_memory_mirrors(
                    local_lsn,
                    &source_order,
                    working.accepted_sync_row_authors.read().clone(),
                );
                mirrors
                    .superseded_ddl_occurrences
                    .extend(std::mem::take(&mut superseded_occurrences));
                // Only records actually finalized by this acceptance enter the delta.
                // The working map owns each admitted payload; publication shares that
                // root instead of cloning the full receiver map or refunding live data.
                mirrors.source_state = working.sync_tombstone_arrivals.read().clone();
                mirrors.accepted_authors = working.accepted_sync_row_authors.read().clone();
                mirrors.terminal_markers = working.terminal_refusal_markers.read().clone();
                mirrors.terminal_scans = working.terminal_refusal_scans.read().clone();
                mirrors.receipt_and_applied_watermarks =
                    working.in_memory_applied_push_watermarks.lock().clone();
                if let Some(receipt) = inputs.receipt.as_ref() {
                    let edge_key = Self::applied_push_watermark_node_incarnation_key(
                        &receipt.tenant_id,
                        &receipt.node_id,
                        receipt.incarnation,
                    );
                    for key in [edge_key, receipt.tenant_id.as_str().to_string()] {
                        mirrors
                            .receipt_and_applied_watermarks
                            .entry(key)
                            .and_modify(|current| *current = (*current).max(receipt.source_lsn))
                            .or_insert(receipt.source_lsn);
                    }
                }
                let mut durable_config_values = persistence.generation_values.clone();
                durable_config_values.extend(persistence.marker_values.iter().cloned());
                durable_config_values.extend(persistence.received_arrival_values.iter().cloned());
                durable_config_values
                    .extend(persistence.superseded_occurrence_values.iter().cloned());
                durable_config_values.extend(persistence.lineage_values.iter().cloned());
                durable_config_values.extend(persistence.accepted_author_values.iter().cloned());
                durable_config_values.extend(persistence.source_state_values.iter().cloned());
                durable_config_values.extend(persistence.receipt_values.iter().cloned());
                durable_config_values
                    .extend(persistence.applied_push_watermark_values.iter().cloned());
                durable_config_values.extend(persistence.terminal_values.iter().cloned());
                durable_config_values.extend(persistence.config_values.iter().cloned());
                if !prepared_sink_events.events.is_empty() {
                    durable_config_values.push((
                        event_bus::NEXT_ACTION_ID_CONFIG_KEY.to_string(),
                        RedbPersistence::encode_config_value(&prepared_sink_events.next_queue_id)?,
                    ));
                }
                let mut staged_deleted_rows =
                    HashMap::<String, HashMap<RowId, VersionedRow>>::new();
                for (table, row_id, _) in &finalized_write_set.relational_deletes {
                    {
                        if let Some(row) = loan.write.latest_row(loan.original, table, *row_id)? {
                            publication_vector_copy_owner
                                .as_mut()
                                .expect("one publication owner")
                                .reserve(
                                    crate::memory_accounting::preparation_row_copy_bytes(
                                        row.value(),
                                    )?
                                    .saturating_add(table.len().saturating_mul(2))
                                    .saturating_add(512),
                                )?;
                            staged_deleted_rows
                                .entry(table.clone())
                                .or_default()
                                .insert(
                                    *row_id,
                                    crate::memory_accounting::preparation_copy_row(row.value())?,
                                );
                        }
                    }
                }
                let structurally_dropped_tables =
                    Self::sync_dropped_tables(&stage_shape_changes.ddl);
                let mut change_log_entries = build_received_schema_change_log_entries(
                    &finalized_write_set,
                    &semantic.tables,
                    &staged_deleted_rows,
                    &structurally_dropped_tables,
                );
                for entry in &mut change_log_entries {
                    match entry {
                        ChangeLogEntry::RowDelete {
                            table, row_id, lsn, ..
                        } => {
                            if let Some(position) =
                                accepted_positions.get(&(table.clone(), *row_id))
                            {
                                *lsn = *position;
                            }
                        }
                        ChangeLogEntry::VectorDelete { index, row_id, lsn } => {
                            if let Some(position) =
                                accepted_positions.get(&(index.table.clone(), *row_id))
                            {
                                *lsn = *position;
                            }
                        }
                        ChangeLogEntry::EdgeDelete {
                            source,
                            target,
                            edge_type,
                            lsn,
                        } => {
                            if let Some((coordinates, overlay)) =
                                coordinates.zip(connected_overlay.as_ref())
                                && let Some(edge) =
                                    overlay.projected.edges.iter().rev().find(|edge| {
                                        edge.source == *source
                                            && edge.target == *target
                                            && edge.edge_type == *edge_type
                                    })
                            {
                                *lsn = coordinates.local(edge.lsn)?;
                            }
                        }
                        _ => {}
                    }
                }
                for accepted in accepted_row_history {
                    let previous =
                        change_log_entries
                            .iter_mut()
                            .find(|entry| match (&**entry, &accepted) {
                                (
                                    ChangeLogEntry::RowInsert {
                                        table: a,
                                        row_id: b,
                                        lsn: c,
                                    },
                                    ChangeLogEntry::RowInsert {
                                        table: x,
                                        row_id: y,
                                        lsn: z,
                                    },
                                )
                                | (
                                    ChangeLogEntry::RowDelete {
                                        table: a,
                                        row_id: b,
                                        lsn: c,
                                        ..
                                    },
                                    ChangeLogEntry::RowDelete {
                                        table: x,
                                        row_id: y,
                                        lsn: z,
                                        ..
                                    },
                                ) => a == x && b == y && c == z,
                                _ => false,
                            });
                    if let Some(previous) = previous {
                        *previous = accepted;
                    } else {
                        change_log_entries.push(accepted);
                    }
                }
                change_log_entries.sort_by_key(ChangeLogEntry::lsn);
                let row_changed_tables = changes
                    .rows
                    .iter()
                    .map(|row| row.table.clone())
                    .chain(
                        finalized_write_set
                            .relational_inserts
                            .iter()
                            .map(|(table, _)| table.clone()),
                    )
                    .chain(
                        finalized_write_set
                            .relational_deletes
                            .iter()
                            .map(|(table, _, _)| table.clone()),
                    )
                    .collect();
                vector_copy_owner.reserve(
                    finalized_write_set
                        .row_adj_inserts
                        .len()
                        .checked_mul(std::mem::size_of::<contextdb_tx::OwnedAdjEntry>())
                        .and_then(|n| {
                            finalized_write_set
                                .row_adj_deletes
                                .len()
                                .checked_mul(
                                    std::mem::size_of::<(contextdb_tx::OwnedAdjEntry, TxId)>(),
                                )
                                .and_then(|m| n.checked_add(m))
                        })
                        .ok_or_else(|| {
                            Error::Other("received row graph projection size overflow".into())
                        })?,
                )?;
                for edge in &finalized_write_set.row_adj_inserts {
                    vector_copy_owner.reserve(edge.estimated_bytes())?;
                }
                for (edge, _) in &finalized_write_set.row_adj_deletes {
                    vector_copy_owner.reserve(edge.estimated_bytes())?;
                }
                // Native spool controls/serialization derive from the exact lent
                // original epoch, never the database's current writer registration.
                let row_stream_owner = if preparation_original.is_some() {
                    publication_vector_copy_owner
                        .as_ref()
                        .expect("one received publication owner")
                } else {
                    &*vector_copy_owner
                };
                let mut row_records = if source_backed {
                    Some(
                        self.persistence
                            .as_ref()
                            .ok_or_else(|| {
                                Error::Other("durable row projection lost its store".into())
                            })?
                            .prepare_received_row_records(row_stream_owner)?,
                    )
                } else {
                    None
                };
                if let Some(records) = &mut row_records {
                    {
                        for table in semantic
                            .tables
                            .keys()
                            .filter(|table| affected_tables.contains(*table))
                        {
                            loan.write.spool_rows(loan.original, table, records)?;
                        }
                    }
                }
                let mut sync_source_records = if source_backed {
                    Some(
                        self.persistence
                            .as_ref()
                            .ok_or_else(|| {
                                Error::Other("durable source projection lost its store".into())
                            })?
                            .prepare_received_sync_source_records(row_stream_owner)?,
                    )
                } else {
                    None
                };
                if let Some(records) = &mut sync_source_records {
                    {
                        for table in semantic
                            .tables
                            .keys()
                            .filter(|table| affected_tables.contains(*table))
                        {
                            let mut previous = None;
                            loan.write.visit_rows(loan.original, table, |row| {
                                if previous == Some(row.row_id) {
                                    return Ok(());
                                }
                                previous = Some(row.row_id);
                                if let Some((lsn, kind)) =
                                    loan.write.sync_source(loan.original, table, row.row_id)?
                                {
                                    records.push(table, row.row_id, lsn, kind)?;
                                }
                                Ok(())
                            })?;
                        }
                    }
                }
                let durable_projection = ReceivedSchemaPersistenceProjection {
                    row_adj_inserts: finalized_write_set.row_adj_inserts.clone(),
                    row_adj_deletes: finalized_write_set.row_adj_deletes.clone(),
                    dropped_row_graph,
                    retired_row_graph_tables,
                    working_write: parking_lot::Mutex::new(None),
                    preparation_owner: None,
                    schema_noop: false,
                    affected_tables: affected_tables.clone(),
                    replace_graph: !affected_graph_edge_types.is_empty(),
                    affected_graph_edge_types: affected_graph_edge_types.clone(),
                    graph_selection,
                    graph_occurrences,
                    replace_vectors: copy_vectors,
                    vector_image_memory: crate::read_image_memory::ImageMemory::charging_for(
                        crate::memory_accounting::AccountantHandle::clone(
                            &self.finalization.accountant,
                        ),
                        "sync",
                        "prepare_vector_partition_image",
                        STAGE_RECEIVED_UNIT_COPY_HINT,
                    ),
                    retired_vector_construction: Mutex::new(Vec::new()),
                    table_meta: semantic
                        .tables
                        .iter()
                        .filter(|(table, _)| affected_tables.contains(*table))
                        .map(|(table, meta)| (table.clone(), meta.clone()))
                        .collect(),
                    row_changed_tables,
                    authored_pending_rows,
                    private_rows: None,
                    row_records,
                    rows: if source_backed {
                        HashMap::new()
                    } else {
                        prepared_rows_for_memory
                            .iter()
                            .filter(|(table, _)| affected_tables.contains(*table))
                            .map(|(table, rows)| {
                                let mut rows = rows.clone();
                                rows.sort_unstable_by_key(|row| {
                                    (row.row_id, row.created_tx, row.lsn)
                                });
                                (table.clone(), rows)
                            })
                            .collect()
                    },
                    sync_source_records,
                    sync_sources: source_sidecars
                        .into_iter()
                        .filter(|((table, _), _)| affected_tables.contains(table))
                        .map(|((table, row_id), (lsn, kind))| (table, row_id, lsn, kind))
                        .collect(),
                    edges: edges
                        .into_iter()
                        .filter(|edge| {
                            edge.row_owner.is_none()
                                && affected_graph_edge_types.contains(&edge.edge_type)
                        })
                        .map(|edge| edge.entry)
                        .collect(),
                    vectors: durable_vectors
                        .into_iter()
                        .filter(|vector| affected_tables.contains(&vector.index.table))
                        .collect(),
                    vector_records,
                    vector_memberships: durable_memberships
                        .into_iter()
                        .filter(|membership| affected_tables.contains(&membership.index.table))
                        .collect(),
                    ddl_log: source_order
                        .iter()
                        .map(|entry| (entry.local_arrival.local_lsn, entry.change.clone()))
                        .collect(),
                    config_values: durable_config_values,
                    config_max_u64_keys: persistence.config_max_u64_keys.clone(),
                    operational_write_set: WriteSet::default(),
                    operational_table_meta: HashMap::new(),
                    operational_deleted_rows: HashMap::new(),
                    structurally_dropped_tables: structurally_dropped_tables.clone(),
                };
                #[cfg(any(test, feature = "test-seams"))]
                if RECEIVED_VECTOR_IMAGE_FAILURE_ARMED.with(std::cell::Cell::take) {
                    RECEIVED_VECTOR_IMAGE_FAILURE_CONSUMED.with(|consumed| consumed.set(true));
                    return Err(Error::Other(
                        ("injected received vector image prepublication failure".to_string())
                            .into(),
                    ));
                }
                metadata_copy_charges.push(retired_graph_charge)?;
                metadata_copy_charges.push(
                    side_metadata_charge
                        .take()
                        .expect("one side metadata owner"),
                )?;
                metadata_copy_charges
                    .push(_live_catalog_charge.take().expect("one catalog owner"))?;
                if let Some(charge) = semantic.catalog_charge {
                    metadata_copy_charges.push(charge)?;
                }
                drop(prepared_rows_for_memory);
                if let Some(source) =
                    crate::persistence::received_working_source::WorkingReadSource::current_for(
                        working.tx_mgr.identity(),
                    )?
                {
                    if let Some(selection) = durable_projection.graph_selection.as_ref() {
                        source.check_private_graph_capture(selection)?;
                    }
                    source.check()?;
                }
                // Prepare this fallible empty carrier before any terminal payload or
                // frozen row invoice moves into its final publication owner.
                let empty_metadata_copy_charges = self.new_received_metadata_copy_charges()?;
                let working_row_owner = if source_backed {
                    {
                        let mut slot = working.finalization.non_vector_memory.lock();
                        slot.as_mut()
                            .ok_or_else(|| {
                                Error::Other(
                                    "private received row image lost its original owner".into(),
                                )
                            })?
                            .freeze_before_spooled_row_eviction();
                    }
                    // Every consumer now uses the completed spool. Free ALL private
                    // non-vector payload before transferring its frozen invoice: the
                    // working database stays alive until this source closure exits,
                    // including when the scope rejects a swallowed callback refusal.
                    working
                        .relational_store
                        .enable_disk_backed_committed_bodies();
                    *working.relational_store.indexes.write() = HashMap::new();
                    *working.relational_store.table_meta.write() = HashMap::new();
                    *working.graph_store.forward_adj.write() = HashMap::new();
                    *working.graph_store.reverse_adj.write() = HashMap::new();
                    Some(
                        working
                            .finalization
                            .non_vector_memory
                            .lock()
                            .take()
                            .ok_or_else(|| {
                                Error::Other(
                                    "private received row owner changed before transfer".into(),
                                )
                            })?,
                    )
                } else {
                    None
                };
                let memory_history = None;
                Ok(Some(ReceivedSchemaStage {
                    memory_history,
                    _working_row_owner: working_row_owner,
                    operational_copy_admission: None,
                    _vector_copy_owner: publication_vector_copy_owner
                        .take()
                        .expect("one received publication owner"),
                    metadata_copy_charges: std::mem::replace(
                        &mut metadata_copy_charges,
                        empty_metadata_copy_charges,
                    ),
                    custody_memory,
                    source_order: std::mem::take(&mut source_order),
                    relational,
                    affected_tables,
                    graph,
                    affected_graph_edge_types,
                    dag_edge_types: Some(dag_edge_types),
                    rank_formulas: Some(rank_formulas),
                    vector,
                    vector_policy_layouts,
                    vector_policy_tables,
                    event_bus,
                    queue_mutation_lease: Some(queue_mutation_lease),
                    trigger,
                    native_trigger,
                    sink_events: prepared_sink_events.events,
                    next_sink_queue_id: prepared_sink_events.next_queue_id,
                    trigger_audits,
                    native_audit_memory: None,
                    next_trigger_audit_index,
                    mirrors,
                    memory_swap,
                    next_row_id: projected_next_row_id,
                    persistence: std::mem::take(&mut persistence),
                    apply_result,
                    write_set: parking_lot::Mutex::new(finalized_write_set),
                    retired_custody_keys: parking_lot::Mutex::new(retired_custody_keys),
                    durable_projection,
                    structurally_dropped_tables,
                    change_log_entries,
                    operational_memberships: None,
                    _connected_credits: coordinates.zip(connected_overlay.as_ref()).map(
                        |(coordinates, overlay)| {
                            (coordinates._credit.clone(), overlay.retained_credit())
                        },
                    ),
                    _private_prefix_effects: parking_lot::Mutex::new(private_prefix_effects),
                    _native_trigger_config_fee: native_trigger_config_fee.take(),
                    _received_edge_input_original: received_edge_input_original,
                    _received_preparation_owner: None,
                }))
            };
            let prepared = {
                let root = self.writer_read_root()?.ok_or_else(|| {
                    Error::Other(
                        "native received working source lost its captured publication".into(),
                    )
                })?;
                let snapshot = working.tx_mgr.capture_published_read().snapshot;
                let lsn = root.lsn;
                if working.graph_store.is_source_backed() {
                    loan.write
                        .prepare_graph(loan.store, loan.original, root.clone())?;
                    loan.write.prepare_vectors(
                        loan.store,
                        loan.original,
                        root.clone(),
                        read_memory.clone(),
                    )?;
                }
                loan.write.with_source(
                    loan.original,
                    loan.store,
                    &working.relational_store,
                    &graph_schema_scope.0,
                    working.tx_mgr.identity(),
                    working.id,
                    root,
                    snapshot,
                    lsn,
                    |_| prepare_attempt(),
                )?
            };
            if let Some(mut stage) = prepared {
                if stage.durable_projection.graph_selection.is_some() {
                    // with_source has returned successfully and its loan
                    // has ended. Retain the same frozen graph under the
                    // authentic write/Original for terminal persistence.
                    stage.durable_projection.graph_selection =
                        Some(loan.write.freeze_graph_selection(loan.original)?);
                }
                return Ok(stage);
            }
            // The synchronous source loan ends before either retry or
            // refusal destroys the private image and its existing invoices.
            if let Some(mut apply_result) = refused_result {
                let captured = working
                    .detached_sync_write_set
                    .lock()
                    .take()
                    .ok_or_else(|| Error::Other("private refusal capture disappeared".into()))?;
                let funding = connected_replay::PrivatePrefixEffects::for_refusal_metadata(
                    &captured.write_set.config_writes,
                    &apply_result,
                    read_memory.clone(),
                )?;
                // Transfer diagnostic/config custody before returning the copied
                // working image's admission; no result body can outlive its payer.
                drop(working);
                drop(metadata_copy_charges);
                let mut write_set = captured.write_set;
                write_set.commit_lsn = Some(local_lsn);
                write_set.finalized_tx = Some(inputs.receiver_tx);
                write_set.visibility_floor = Some(inputs.receiver_tx);
                write_set.requires_commit_lsn = true;
                for (key, value) in &mut write_set.config_writes {
                    if key.starts_with(SYNC_PUSH_OUTCOME_PREFIX) {
                        let mut outcome: DurableSyncPushOutcome =
                            RedbPersistence::decode_config_value(value)?;
                        outcome.result.new_lsn = local_lsn;
                        *value = RedbPersistence::encode_config_value(&outcome)?;
                    }
                }
                if let Some(coordinates) = coordinates {
                    coordinates.stage_membership(&mut write_set.config_writes)?;
                }
                // The existing empty projection builds only receiver bookkeeping;
                // it neither copies bodies nor publishes an authenticated DDL.
                let mut stage = self.prepare_received_schema_stage_with_interval(
                    local_lsn,
                    coordinates,
                    &ChangeSet::default(),
                    None,
                    &[],
                    ReceivedSchemaAdjudicationInputs {
                        push_outcome_key: inputs.push_outcome_key,
                        apply_role: inputs.apply_role,
                        arrivals: inputs.arrivals,
                        adoption: inputs.adoption,
                        receipt: inputs.receipt.clone(),
                        dependency_complete: inputs.dependency_complete,
                        terminal_refusal_context: inputs.terminal_refusal_context,
                        hub_local_author: inputs.hub_local_author,
                        receiver_tx: inputs.receiver_tx,
                        unattested_policies: inputs.unattested_policies,
                    },
                    None,
                    vector_copy_owner,
                    read_memory.clone(),
                    preparation_original,
                    loan,
                )?;
                apply_result.new_lsn = local_lsn;
                stage.apply_result = apply_result;
                stage.write_set = parking_lot::Mutex::new(write_set);
                stage._connected_credits =
                    coordinates
                        .zip(connected_overlay.as_ref())
                        .map(|(coordinates, overlay)| {
                            (coordinates._credit.clone(), overlay.retained_credit())
                        });
                *stage._private_prefix_effects.get_mut() = Some(funding);
                return Ok(stage);
            }
        }
    }

    /// Register the complete, private stage under the local commit LSN that
    /// the transaction manager reserved. Failure cleanup in the later Redb
    /// integration removes this entry before any memory publication can
    /// occur.
    ///
    /// A unit that declares a table and carries that table's rows/vectors in
    /// the same delivery has not published its schema yet, so its own
    /// commit's `apply_exact` cannot resolve those vector writes against the
    /// receiver's live state. Two things are staged here, under the same
    /// commit LSN, so it resolves against the shape the unit is installing
    /// instead: the arriving table metadata (`apply_exact` reads it back),
    /// and -- for exactly the tables this delivery is creating, never one
    /// that already exists live -- a real vector-index registration, because
    /// preparing a partitioned batch needs a live declared layout to compute
    /// against, not just a table_meta entry. Both are removed unconditionally
    /// by this registration's guard; a table that already exists keeps
    /// resolving against the receiver's current declaration exactly as
    /// before, which is what protects an in-flight vector shape change from
    /// seeing its own arriving shape early.
    #[must_use = "an unheld registration removes the stage again immediately"]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn register_received_schema_stage(
        &self,
        local_lsn: Lsn,
        mut stage: ReceivedSchemaStage,
    ) -> Result<ReceivedSchemaStageRegistration> {
        // Reserve the destination and arm its cleanup before any index or
        // arriving-metadata effect. Existing logical entries stay visible until
        // publication, and every early Result/unwind cancels this token.
        let occupancy =
            schema_stage_registry::ReceivedToken::reserve(&self.received_schema_stages, local_lsn)?;
        let arriving_table_meta = stage.durable_projection.table_meta.clone();
        let layouts = vector_layouts_from_meta(&arriving_table_meta);
        let mut registration = ReceivedSchemaStageRegistration {
            registry: self.received_schema_stages.clone(),
            arriving_table_meta: self.received_arriving_table_meta.clone(),
            arriving_vector_indexes: Vec::new(),
            next_arriving_index_cleanup: 0,
            arriving_metadata_cleaned: false,
            arriving_metadata_installed: false,
            published: false,
            occupancy: Some(occupancy),
            vector_store: self.vector_store.clone(),
            accountant: crate::memory_accounting::AccountantHandle::clone(
                &self.finalization.accountant,
            ),
            lsn: local_lsn,
            armed: true,
        };
        for (index, layout) in layouts {
            if self.vector_store.contains_index(&index) {
                continue;
            }
            self.vector_store.register_index_with_layout(
                index.clone(),
                layout,
                contextdb_vector::MemoryBudgetOwner::clone(&self.finalization.vector_accountant),
                &mut stage._vector_copy_owner,
            )?;
            // Record a successful installation immediately in the existing
            // preparation vector, before any subsequent effect or callback.
            registration.arriving_vector_indexes.push(index);
        }
        self.received_arriving_table_meta
            .lock()
            .insert(local_lsn, Arc::new(arriving_table_meta));
        registration.arriving_metadata_installed = true;
        registration.published = true;
        registration
            .occupancy
            .take()
            .expect("registration owns publication occupancy")
            .publish(stage);
        Ok(registration)
    }

    pub(super) fn take_received_schema_stage(&self, local_lsn: Lsn) -> Option<ReceivedSchemaStage> {
        self.received_schema_stages.lock().remove(&local_lsn)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn attach_received_schema_operational_rows(
        &self,
        stage: &mut ReceivedSchemaStage,
        operational: WriteSet,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if operational.relational_inserts.is_empty() && operational.relational_deletes.is_empty() {
            return Ok(());
        }
        let operational_tables = operational
            .relational_inserts
            .iter()
            .map(|(table, _)| table.clone())
            .chain(
                operational
                    .relational_deletes
                    .iter()
                    .map(|(table, _, _)| table.clone()),
            )
            .collect::<HashSet<_>>();
        if operational_tables != HashSet::from(["work_node_contacts".to_string()])
            || !operational.adj_inserts.is_empty()
            || !operational.adj_deletes.is_empty()
            || !operational.vector_inserts.is_empty()
            || !operational.vector_deletes.is_empty()
            || !operational.vector_moves.is_empty()
            || !operational.config_writes.is_empty()
            || !operational.config_deletes.is_empty()
            || !operational.sync_source_provenance_marks.is_empty()
        {
            return Err(Error::Other(
                ("received-schema operational fusion contains non-contact work".to_string()).into(),
            ));
        }
        if operational_tables
            .iter()
            .any(|table| stage.affected_tables.contains(table))
        {
            return Err(Error::Other(
                ("received-schema operational table overlaps the received image".to_string())
                    .into(),
            ));
        }
        let mut table_meta = HashMap::new();
        for table in &operational_tables {
            let meta = self.table_meta(table).ok_or_else(|| {
                Error::Other(
                    (format!("received-schema operational table {table} has no live declaration"))
                        .into(),
                )
            })?;
            table_meta.insert(table.clone(), meta);
        }
        let mut deleted_rows = HashMap::<String, HashMap<RowId, VersionedRow>>::new();
        for (table, row_id, _) in &operational.relational_deletes {
            if let Some(row) = self.committed_row_at(
                table,
                *row_id,
                self.snapshot_for_read(),
                read_memory.clone(),
            )? {
                deleted_rows
                    .entry(table.clone())
                    .or_default()
                    .insert(*row_id, row);
            }
        }
        let operational_log = build_received_schema_change_log_entries(
            &operational,
            &table_meta,
            &deleted_rows,
            &HashSet::new(),
        );
        let operational_memberships = self.relational_store.prepare_memberships(
            &operational.relational_deletes,
            &operational.relational_inserts,
            self.finalization.accountant.read_budget()?,
        )?;
        stage
            .write_set
            .get_mut()
            .relational_inserts
            .extend(operational.relational_inserts.iter().cloned());
        stage
            .write_set
            .get_mut()
            .relational_deletes
            .extend(operational.relational_deletes.iter().cloned());
        stage.change_log_entries.extend(operational_log);
        stage.durable_projection.operational_write_set = operational;
        stage.durable_projection.operational_table_meta = table_meta;
        stage.durable_projection.operational_deleted_rows = deleted_rows;
        stage.operational_memberships = Some(operational_memberships);
        Ok(())
    }

    /// The post-durability half of received-schema apply.  It performs no
    /// parsing, persistence, or fallible operation: the complete
    /// table projections and event/trigger definitions were prepared before
    /// the matching Redb transaction started.
    #[cfg(test)]
    pub(super) fn publish_received_schema_stage(&self, mut stage: ReceivedSchemaStage) {
        self.publish_received_schema_stage_retaining(&mut stage);
    }

    /// Publish prepared payloads while their original preparation owners can
    /// remain with the returned committing write set through its last hook.
    pub(super) fn publish_received_schema_stage_retaining(&self, stage: &mut ReceivedSchemaStage) {
        // The durable commit is done and no publication component has run:
        // the custody delta below is the first one.
        #[cfg(any(test, feature = "test-seams"))]
        self.received_schema_pre_publish_pause.maybe_pause();
        #[cfg(any(test, feature = "test-seams"))]
        take_received_unit_unwind_fault_for_test(
            ReceivedUnitFaultPoint::AfterDurableCommitBeforePublish,
        );
        // The schema/unit boundary publishes the same metadata delta.
        if let Some(delta) = stage.custody_memory.take() {
            self.publish_custody_delta(delta);
        }
        if stage.durable_projection.schema_noop {
            let mut metadata = self.relational_store.table_meta.write();
            for (table, replacement) in std::mem::take(&mut stage.durable_projection.table_meta) {
                let current = metadata
                    .get_mut(&table)
                    .expect("validated received policy table");
                *current = replacement;
            }
        }
        self.relational_store
            .publish_received_table_projections_with_generation(
                std::mem::take(&mut stage.relational),
                &stage.structurally_dropped_tables,
                stage.memory_swap.history.take(),
            );
        #[cfg(any(test, feature = "test-seams"))]
        take_received_unit_unwind_fault_for_test(
            ReceivedUnitFaultPoint::BetweenPublicationComponents,
        );
        // A table whose only vector change was policy kept its vectors and
        // graphs: it awaits no replacement image, and the flag keeps its
        // prior state.
        let awaits_vector_image = stage
            .affected_tables
            .iter()
            .filter(|table| !stage.vector_policy_tables.contains(*table))
            .any(|table| {
                self.relational_store.table_meta(table).is_some_and(|meta| {
                    meta.columns
                        .iter()
                        .any(|column| matches!(column.column_type, ColumnType::Vector(_)))
                })
            });
        if let Some(memberships) = stage.operational_memberships.take() {
            self.relational_store.apply_prepared_rows(
                &stage
                    .durable_projection
                    .operational_write_set
                    .relational_deletes,
                &stage
                    .durable_projection
                    .operational_write_set
                    .relational_inserts,
                memberships,
            );
        }
        if let Some(graph) = stage.graph.take() {
            self.graph_store.publish_prepared_received_schema_tables(
                graph,
                &stage.affected_graph_edge_types,
                &stage.affected_tables,
            );
        }
        if let Some(dag_edge_types) = stage.dag_edge_types.take() {
            self.graph.publish_prepared_dag_edge_types(dag_edge_types);
        }
        if let Some(rank_formulas) = stage.rank_formulas.take() {
            let mut cache = self.rank_formula_cache.write();
            cache.retain(|(table, _), _| !stage.affected_tables.contains(table));
            cache.extend(rank_formulas);
            self.rank_policy_formula_parse_count
                .store(cache.len() as u64, Ordering::SeqCst);
        }
        if let Some(vector) = stage.vector.take() {
            self.vector_store
                .publish_prepared_received_schema_tables(vector);
        }
        let vector_policy_changed = !stage.vector_policy_layouts.is_empty();
        for prepared in std::mem::take(&mut stage.vector_policy_layouts) {
            self.vector_store.publish_policy_layout_update(prepared);
        }
        if vector_policy_changed {
            self.reconcile_maintenance_thread();
        }
        // The replacement registry has retired the old in-process builders.
        // Only successfully committed cancellation receipts release their holds;
        // surviving restart records retain the original fixed snapshot floor.
        for receipt in stage
            .durable_projection
            .retired_vector_construction
            .lock()
            .drain(..)
        {
            self.snapshot_registry
                .release_construction_floor(&receipt.0);
            // Keep the body-first tuple intact: destructuring made the
            // later invoice binding drop before the owned partition strings.
            drop(receipt);
        }
        if awaits_vector_image {
            self.received_vector_image_pending
                .store(true, Ordering::SeqCst);
        }
        if let Some(event_bus) = stage.event_bus.take() {
            self.publish_prepared_event_bus_publication_under_queue_mutation_lease(
                event_bus,
                std::mem::take(&mut stage.sink_events),
            );
            self.advance_received_sink_event_id(stage.next_sink_queue_id);
        }
        if let Some(publication) = stage.native_trigger.take() {
            self.publish_original_trigger_publication(publication);
        } else if let Some(trigger) = stage.trigger.take() {
            self.publish_prepared_trigger_publication(trigger);
        }
        if let Some(publication) = stage.native_audit_memory.take() {
            self.publish_original_audit_memory(publication);
            self.trigger
                .next_audit_index
                .fetch_max(stage.next_trigger_audit_index, Ordering::SeqCst);
        } else {
            self.publish_received_trigger_audits(
                std::mem::take(&mut stage.trigger_audits),
                stage.next_trigger_audit_index,
            );
        }
        self.in_memory_table_generations
            .lock()
            .extend(std::mem::take(&mut stage.mirrors.table_generations));
        self.publish_memory_schema_history(stage.memory_history.take());
        {
            let mut sources = self.sync_tombstone_arrivals.write();
            sources.retain(|(table, _), _| !stage.affected_tables.contains(table));
            sources.extend(std::mem::take(&mut stage.mirrors.source_state));
        }
        {
            let mut authors = self.accepted_sync_row_authors.write();
            authors.retain(|(table, _), _| !stage.affected_tables.contains(table));
            authors.extend(std::mem::take(&mut stage.mirrors.accepted_authors));
        }
        {
            let mut markers = self.terminal_refusal_markers.write();
            markers.retain(|(_, _, table, _), _| !stage.affected_tables.contains(table));
            markers.extend(std::mem::take(&mut stage.mirrors.terminal_markers));
        }
        self.terminal_refusal_scans
            .write()
            .extend(std::mem::take(&mut stage.mirrors.terminal_scans));
        {
            let mut watermarks = self.in_memory_applied_push_watermarks.lock();
            for (key, value) in std::mem::take(&mut stage.mirrors.receipt_and_applied_watermarks) {
                watermarks
                    .entry(key)
                    .and_modify(|current| *current = (*current).max(value))
                    .or_insert(value);
            }
        }
        self.relational_store
            .fetch_max_next_row_id(stage.next_row_id);
        stage.memory_swap.commit_after_swap();
        // Retain funding, never publication locks: post-commit handlers may
        // acknowledge queues or perform another transaction immediately.
        drop(stage.queue_mutation_lease.take());
    }

    /// Test helper for the received-schema transaction-manager hand-off.
    #[cfg(test)]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub(super) fn commit_received_schema_stage_for_test(
        &self,
        changes: &ChangeSet,
        received: &crate::protocol::ReceivedDdlContext,
        validated_lineages: &[(String, NaturalKey, Lsn, crate::protocol::WireRowLineage)],
        arrivals: &HashMap<Lsn, Option<Lsn>>,
        fail_after_stage_registration: bool,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: contextdb_core::read_memory::ReadMemoryOwner,
    ) -> Result<()> {
        self.commit_received_schema_stage_with_receipt_for_test(
            changes,
            received,
            validated_lineages,
            arrivals,
            None,
            fail_after_stage_registration,
            vector_copy_owner,
            read_memory.clone(),
        )
        .map(|_| ())
    }

    /// Commit one authenticated received-schema unit through the prepared
    /// whole-migration stage. An identical durable manifest returns its
    /// stored result before beginning another transaction.
    #[allow(clippy::too_many_arguments)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn commit_received_schema_stage<F, G>(
        &self,
        changes: &ChangeSet,
        received: Option<&crate::protocol::ReceivedDdlContext>,
        validated_lineages: &[(String, NaturalKey, Lsn, crate::protocol::WireRowLineage)],
        arrivals: &HashMap<Lsn, Option<Lsn>>,
        adoption: SyncAdoption,
        apply_role: SyncApplyRole,
        receipt: Option<SyncApplyReceipt>,
        dependency_complete: bool,
        terminal_refusal_context: Option<&TerminalRefusalPullContext>,
        hub_local_author: Option<&str>,
        mut delivery_prepare: Option<&mut dyn crate::custody::preparation::CommitPreparation>,
        push_outcome_key: Option<&SyncPushOutcomeContext>,
        unattested_policies: Option<&ConflictPolicies>,
        apply_preflight: F,
        after_stage_registration: G,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<crate::protocol::WireApplyResult>
    where
        F: FnOnce(
            &mut contextdb_vector::VectorCopyAdmission,
        ) -> Result<Option<crate::memory_accounting::SchemaUnitReservation>>,
        G: FnOnce() -> Result<()>,
    {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        #[cfg(any(test, feature = "test-seams"))]
        let apply_started = std::time::Instant::now();
        let published = self.tx_mgr.capture_published_read();
        let _writer_read = self.enter_writer_read(
            writer_read::durable_root(published.handle)?,
            read_memory.clone(),
        );
        let _manifest_lock = self.received_schema_manifest_lock.lock();
        #[cfg(any(test, feature = "test-seams"))]
        let _observation_scope = begin_received_schema_apply_observation_for_test(
            self.received_schema_apply_observer.lock().clone(),
        );
        #[cfg(any(test, feature = "test-seams"))]
        let classify_started = std::time::Instant::now();
        let manifest_classification = self.classify_received_schema_manifest(received)?;
        #[cfg(any(test, feature = "test-seams"))]
        {
            record_received_schema_profile_nanos_for_test(
                "manifest-classify",
                classify_started.elapsed().as_nanos(),
            );
            record_received_schema_profile_work_for_test(
                "manifest-marker-lookups",
                received.map_or(0, |received| received.entries.len()) as u64,
            );
        }
        if let ReceivedSchemaManifestClassification::Replay(result) = manifest_classification {
            if let Some(receipt) = receipt.as_ref()
                && (push_outcome_key.is_some()
                    || self
                        .persisted_sync_applied_push_watermark_for_node_incarnation_with_read_memory(
                            &receipt.tenant_id,
                            &receipt.node_id,
                            receipt.incarnation,
                         read_memory.clone())?
                        .is_none_or(|watermark| watermark < receipt.source_lsn))
            {
                // The original schema vector is already durable. A pushed
                // retry is answered from its own durable outcome slot, so the
                // slot is rebound to this exact request even when the receipt
                // frontier already covers it. A retry can also carry a later
                // authenticated delivery frontier, so advance its receipt
                // atomically even when this leg has no push-outcome slot.
                let context = push_outcome_key.map(|context| {
                    let mut context = context.clone();
                    context.committed_lsn = Some(result.new_lsn);
                    context
                });
                self.commit_sync_apply_outcome_only(
                    receipt,
                    context.as_ref(),
                    &result.clone().into(),
                    vector_copy_owner,
                    read_memory.clone(),
                )?;
            }
            return Ok(result);
        }
        #[cfg(any(test, feature = "test-seams"))]
        let commit_started = std::time::Instant::now();
        // Whatever this unit's preflight reserved is held here, across the
        // whole preparation the prepare phase below performs -- not dropped
        // at the end of the preflight closure. Entering its scope right
        // before the detached copy hands that room back to this accountant
        // for the copy's own per-object charges to retake, and opens the
        // unit's bounded excess; the reservation itself becomes a no-op
        // release once that happens, so holding it this long costs nothing.
        let unit_reservation = apply_preflight(vector_copy_owner)?;
        // Judged after the unit's preflight, so a cold handle's callback gate
        // still refuses it first.
        if received.is_none()
            && receipt.is_none()
            && push_outcome_key.is_none()
            && delivery_prepare.is_none()
            // An unattested ALTER still represents an authored instruction;
            // matching metadata does not erase its occurrence in ordered replay.
            && !changes
                .ddl
                .iter()
                .any(|ddl| matches!(ddl, DdlChange::AlterTable { .. }))
            && self.unattested_schema_unit_changes_nothing(changes, vector_copy_owner)?
        {
            return Ok(ApplyResult {
                applied_rows: 0,
                skipped_rows: 0,
                conflicts: Vec::new(),
                new_lsn: self.current_lsn(),
            }
            .into());
        }
        #[cfg(feature = "sync-orchestration")]
        if receipt
            .as_ref()
            .and_then(|receipt| receipt.hub_contact_ms)
            .is_some()
        {
            // The first authenticated contact owns installing this engine
            // table. Its contact row still joins the received-schema receipt
            // transaction below.
            crate::sync_system_tables::ensure_node_contacts_schema(self, read_memory.clone())?;
        }
        // Each index whose layout this unit changes gets a new schema epoch at
        // publication, still under the commit lock, so a transaction that
        // staged a vector against the old layout is refused at its commit,
        // exactly as a direct ALTER refuses it.
        let changed_vector_refs =
            self.received_unit_changed_vector_refs(changes, vector_copy_owner)?;
        // The after-apply publisher consumes the stage and records its result
        // only after Redb succeeds, immediately before publication moves the
        // prepared memory image into place.
        let committed_result = std::cell::RefCell::new(None);
        // The stage's write set moves into the transaction (never copied);
        // the funding of the custody ownership keys retirement moved into it
        // moves here with it. Declared before the commit's result, which owns
        // that write set afterwards, so the funding is released only after
        // every holder of those keys is gone.
        let moved_retired_custody_keys = std::cell::RefCell::new(Vec::new());
        // The native apply may publish and consume the stage before its
        // committing WriteSet result is destroyed. Keep the private unit's
        // exact credit outside that result through every success/error exit.
        let moved_private_prefix_effects =
            std::cell::RefCell::new(None::<connected_replay::PrivatePrefixEffects>);
        let moved_preparation_custody =
            std::cell::RefCell::new(None::<ReceivedSchemaPreparationCustody>);
        let tx = self.begin()?;
        let mut terminal_cleanup = matches!(&self.tx_mgr, DatabaseTransactionHost::Legacy(_))
            .then(|| ReceivedTransactionCleanup::new(self, tx, read_memory.clone(), false));
        if apply_role == SyncApplyRole::HubPushApply
            && let Some(receipt) = receipt.as_ref()
            && let Some(contact_ms) = receipt.hub_contact_ms
            && let Err(error) =
                self.stage_hub_node_contact(tx, &receipt.node_id, contact_ms, read_memory.clone())
        {
            if terminal_cleanup.is_none() {
                let _ = self.rollback(tx);
            }
            return Err(error);
        }
        let applied = std::cell::Cell::new(false);
        let published_stage = std::cell::RefCell::new(None::<ReceivedSchemaStage>);
        let policy_releases = std::cell::RefCell::new(Vec::new());
        let policy_release_memory = std::cell::RefCell::new(Vec::new());
        // Held outside the callbacks so it outlives every one of them. Its
        // `Drop` runs on an error return AND on a panic anywhere between
        // registration and publication, which is the case no `Result` branch
        // can reach.
        let registration: std::cell::RefCell<Option<ReceivedSchemaStageRegistration>> =
            std::cell::RefCell::new(None);
        // Armed just before the durable apply; see `PublicationInterruptionWatch`.
        let publication_watch = std::cell::RefCell::new(None::<PublicationInterruptionWatch>);
        let marker = [(
            "__contextdb.received_schema_stage".to_string(),
            RedbPersistence::encode_config_value(&true)?,
        )];
        self.tx_mgr.append_config_writes(tx, marker)?;
        let (_positions_credit, positions) =
            self.connected_source_positions(changes, read_memory.clone())?;
        // Source chronology is independent of the unit's conflict policy.
        // Ordinary units keep partial-adoption rules while reserving the same
        // private interval for their actual ordered source positions.
        let interval_positions = positions.len().max(1) as u64;
        let committed = self
            .tx_mgr
            .commit_received_interval(
                tx,
                interval_positions,
                read_memory.clone(),
                |scope, start, lsn, receiver_tx| {
                    // Refund the original outer rows before the manager's
                    // terminal guard can drop them on preparation unwind.
                    // Returned errors remain owned by the enclosing cleanup.
                    let _active_cleanup = scope.is_none().then(|| {
                        ReceivedTransactionCleanup::new(self, tx, read_memory.clone(), true)
                    });
                    // The outer request may have waited for the commit lock.
                    // Bind preparation and all its callbacks to the publication
                    // actually preceding this SAME unpublished write.
                    let published = self.tx_mgr.capture_published_read();
                    let _preparation_read = self.enter_writer_read(
                        writer_read::durable_root(published.handle)?,
                        read_memory.clone(),
                    );
                    let coordinates = (interval_positions > 1)
                        .then(|| {
                            ConnectedCoordinates::prepare(changes, start, lsn, read_memory.clone())
                        })
                        .transpose()?;
                    #[cfg(any(test, feature = "test-seams"))]
                    let prepare_started = std::time::Instant::now();
                    // This unit was already admitted as a whole, ahead of
                    // every item -- see `apply_changes_single_lsn_group`,
                    // which enters the same scope for the direct door. While
                    // preparation installs the detached copy, an allocation
                    // the price did not foresee draws on the reservation
                    // this scope just handed back to the accountant, and
                    // past that on a bounded excess. Dropped the moment
                    // preparation is done.
                    let _admitted_unit = unit_reservation
                        .as_ref()
                        .map(crate::memory_accounting::AdmittedSchemaUnit::enter);
                    let inputs = ReceivedSchemaAdjudicationInputs {
                        push_outcome_key,
                        apply_role,
                        arrivals,
                        adoption,
                        receipt: receipt.clone(),
                        dependency_complete,
                        terminal_refusal_context,
                        hub_local_author,
                        receiver_tx,
                        unattested_policies,
                    };
                    let mut stage = match scope {
                        // Unjoined boundary: the interval's active_prepare
                        // callback requires bare Error. The funded stage must
                        // retain its carrier through a migrated terminal path.
                        Some(scope) => scope.prepare_received_schema_stage(
                            self,
                            &scope.host.events,
                            lsn,
                            coordinates.as_ref(),
                            changes,
                            received,
                            validated_lineages,
                            inputs,
                            delivery_prepare.take(),
                            vector_copy_owner,
                            read_memory.clone(),
                        )?,
                        None => {
                            // A receiver without a durable store has no live
                            // write to lend the private image; refuse before
                            // any working write begins.
                            if self.persistence.is_none()
                                || !self.relational_store.uses_disk_backed_committed_bodies()
                            {
                                return Err(received_schema_requires_durable_receiver());
                            }
                            self.prepare_legacy_file_received_schema_stage(
                                lsn,
                                coordinates.as_ref(),
                                changes,
                                received,
                                validated_lineages,
                                inputs,
                                delivery_prepare.take(),
                                vector_copy_owner,
                                read_memory.clone(),
                            )?
                        }
                    };
                    let operational = match scope {
                        Some(scope) => {
                            scope
                                .host
                                .events
                                .with_owned_persistence_original(|original| {
                                    stage.copy_operational_from(&self.tx_mgr, tx, original)
                                })?
                        }
                        None => self.tx_mgr.with_ordinary_read(tx, |write_set| WriteSet {
                            relational_inserts: write_set.relational_inserts.clone(),
                            relational_deletes: write_set.relational_deletes.clone(),
                            commit_lsn: write_set.commit_lsn,
                            finalized_tx: write_set.finalized_tx,
                            visibility_floor: write_set.visibility_floor,
                            ..WriteSet::default()
                        })?,
                    };
                    self.attach_received_schema_operational_rows(
                        &mut stage,
                        operational,
                        read_memory.clone(),
                    )?;
                    if let Some(received) = received {
                        let releases =
                            self.policy_hold_releases_for_correction(changes, received)?;
                        policy_release_memory.borrow_mut().push(releases.memory);
                        for (key, release) in releases.entries {
                            let encoded = RedbPersistence::encode_config_value(&release)?;
                            stage
                                .write_set
                                .get_mut()
                                .config_writes
                                .push((key.clone(), encoded.clone()));
                            stage
                                .persistence
                                .config_values
                                .push((key.clone(), encoded.clone()));
                            stage
                                .durable_projection
                                .config_values
                                .push((key.clone(), encoded));
                            policy_releases.borrow_mut().push((key, release));
                        }
                    }
                    #[cfg(any(test, feature = "test-seams"))]
                    record_received_schema_profile_nanos_for_test(
                        "stage-prepare",
                        prepare_started.elapsed().as_nanos(),
                    );
                    *registration.borrow_mut() =
                        Some(self.register_received_schema_stage(lsn, stage)?);
                    after_stage_registration()?;
                    #[cfg(any(test, feature = "test-seams"))]
                    take_received_unit_fault_for_test(
                        ReceivedUnitFaultPoint::AfterStageRegistration,
                    )?;
                    Ok(())
                },
                |write_set| {
                    let lsn = write_set.commit_lsn.ok_or_else(|| {
                        Error::Other(("received-schema commit lacks an LSN".to_string()).into())
                    })?;
                    let replacement = self
                        .received_schema_stages
                        .lock()
                        .get_mut(&lsn)
                        .map(|stage| {
                            // Empty replacements allocate nothing and retain
                            // the same already captured writer/accountant.
                            let empty_metadata =
                                MetadataCopyCharges::new(&self.finalization.accountant);
                            let empty_vector = contextdb_vector::VectorCopyAdmission::new(
                                contextdb_vector::MemoryBudgetOwner::clone(
                                    &self.finalization.vector_accountant,
                                ),
                            );
                            *moved_preparation_custody.borrow_mut() =
                                Some(ReceivedSchemaPreparationCustody {
                                    _working_row_owner: stage._working_row_owner.take(),
                                    _metadata_copy_charges: std::mem::replace(
                                        &mut stage.metadata_copy_charges,
                                        empty_metadata,
                                    ),
                                    _vector_copy_owner: std::mem::replace(
                                        &mut stage._vector_copy_owner,
                                        empty_vector,
                                    ),
                                    _connected_credits: stage._connected_credits.take(),
                                    _received_preparation_owner: stage
                                        ._received_preparation_owner
                                        .take(),
                                });
                            *moved_retired_custody_keys.borrow_mut() =
                                std::mem::take(&mut *stage.retired_custody_keys.lock());
                            *moved_private_prefix_effects.borrow_mut() =
                                stage._private_prefix_effects.lock().take();
                            std::mem::take(&mut *stage.write_set.lock())
                        })
                        .ok_or_else(|| {
                            Error::Other(
                                ("received-schema stage disappeared before prepare".to_string())
                                    .into(),
                            )
                        })?;
                    let finalized_tx = write_set.finalized_tx;
                    if let Some(cleanup) = terminal_cleanup.as_ref() {
                        cleanup.replace_original_body(write_set, replacement);
                    } else {
                        *write_set = replacement;
                    }
                    write_set.finalized_tx = finalized_tx;
                    self.plugin.pre_commit(write_set, CommitSource::SyncPull)?;
                    #[cfg(any(test, feature = "test-seams"))]
                    self.received_schema_pre_commit_pause.maybe_pause();
                    #[cfg(any(test, feature = "test-seams"))]
                    take_received_unit_fault_for_test(ReceivedUnitFaultPoint::BeforeDurableCommit)?;
                    *publication_watch.borrow_mut() = Some(PublicationInterruptionWatch::arm(
                        &self.publication_interrupted,
                    ));
                    Ok(())
                },
                |host, guard| {
                    let lsn = (*guard.read().commit_lsn).ok_or_else(|| {
                        Error::Other("received-schema commit lacks an LSN".into())
                    })?;
                    // Loan the selected original before registry/stage guards.
                    let adoption = host.events.with_owned_persistence_original(|original| {
                        let stages = self.received_schema_stages.lock();
                        let stage = stages.get(&lsn).ok_or_else(|| {
                            Error::Other(
                                "received-schema stage disappeared before native prepare".into(),
                            )
                        })?;
                        stage.prepare_native_adoption(guard, original, &host.funder)
                    })?;
                    guard.publish_adoption(adoption);
                    // No stage/queue guard is held across a plugin callback.
                    guard.with_legacy(|ws| self.plugin.pre_commit(ws, CommitSource::SyncPull))?;
                    #[cfg(any(test, feature = "test-seams"))]
                    self.received_schema_pre_commit_pause.maybe_pause();
                    #[cfg(any(test, feature = "test-seams"))]
                    take_received_unit_fault_for_test(ReceivedUnitFaultPoint::BeforeDurableCommit)?;
                    *publication_watch.borrow_mut() = Some(PublicationInterruptionWatch::arm(
                        &self.publication_interrupted,
                    ));
                    Ok(())
                },
                |lsn, _write_set| {
                    if let Some(cleanup) = terminal_cleanup.as_ref() {
                        cleanup.disable_original_refund();
                    }
                    applied.set(true);
                    debug_assert!(
                        registration
                            .borrow()
                            .as_ref()
                            .is_none_or(|held| held.lsn == lsn),
                        "the applied callback publishes the stage this commit registered"
                    );
                    let staged = registration
                        .borrow_mut()
                        .as_mut()
                        .and_then(ReceivedSchemaStageRegistration::take);
                    if let Some(stage) = staged {
                        *committed_result.borrow_mut() = Some(
                            crate::protocol::WireApplyResult::from(stage.apply_result.clone()),
                        );
                        // Published below, once the commit boundary is released.
                        *published_stage.borrow_mut() = Some(stage);
                    }
                },
            )
            .map_err(|mut failure| {
                let retained_original = terminal_cleanup
                    .as_ref()
                    .is_some_and(|cleanup| failure.retain_legacy_original(cleanup));
                let notify_failure = |write_set: Option<&WriteSet>| {
                    if let Some(write_set) = write_set {
                        self.plugin.commit_failed(
                            write_set,
                            CommitSource::SyncPull,
                            failure.error().engine_diagnostic(),
                        );
                        if let Some(lsn) = write_set.commit_lsn {
                            let _ = self.take_received_schema_stage(lsn);
                        }
                    }
                };
                match terminal_cleanup.as_ref().filter(|_| retained_original) {
                    Some(cleanup) => cleanup.with_original_body(notify_failure),
                    None => failure.with_write_set(notify_failure),
                }
                failure.into_error()
            });
        // Only success certifies terminal handoff. On a returned failure the
        // guard checks whether any pre-handoff entry still needs retirement.
        if let Some(cleanup) = terminal_cleanup.as_mut() {
            cleanup.armed = committed.is_err();
        }
        // Two orders meet here. Durable order: this stage's commit, and its
        // storage-version publication, completed under the commit boundary
        // above; nothing below writes. In-memory order: the stage is published
        // only now, after that boundary is released, so a commit or sink
        // acknowledgement queued behind the boundary never waits on this
        // in-memory publication. Past this point publication does no fallible
        // work: every schema/index artifact it installs was prepared and
        // admitted before the durable commit.
        if let Some(stage) = published_stage.borrow_mut().as_mut() {
            self.publish_received_schema_stage_retaining(stage);
            self.vector_schema_gates.bump_epochs(&changed_vector_refs);
        }
        // Whatever the outcome, the registration stops holding the stage here.
        drop(registration.into_inner());
        // Publication returned, or the commit failed with an ordinary error:
        // either way nothing unwound past the durable commit.
        if let Some(watch) = publication_watch.into_inner() {
            watch.completed();
        }
        let (_, committed_write_set) = committed?;
        #[cfg(any(test, feature = "test-seams"))]
        self.emit_writer_memory_ownership_receipt_for_test("commit");
        #[cfg(any(test, feature = "test-seams"))]
        record_received_schema_profile_nanos_for_test(
            "transaction-commit-total",
            commit_started.elapsed().as_nanos(),
        );
        if applied.get() {
            committed_write_set
                .with_write_set(|ws| self.plugin.post_commit(ws, CommitSource::SyncPull));
        }
        let result = committed_result.into_inner().ok_or_else(|| {
            Error::Other(
                ("received-schema commit completed without publishing its staged result"
                    .to_string())
                .into(),
            )
        })?;
        // Received schema changes can create, alter, or drop retention/history
        // declarations. Reconcile only after the prepared image is durably
        // committed and published, so the receiver's worker sees the same
        // schema the caller can now observe.
        self.reconcile_maintenance_thread();
        #[cfg(any(test, feature = "test-seams"))]
        record_received_schema_profile_nanos_for_test(
            "apply-total",
            apply_started.elapsed().as_nanos(),
        );
        Ok(result)
    }

    /// Test-only received-schema bridge which carries the authenticated
    /// receipt and returns the detached adjudicator's committed outcome.
    #[cfg(test)]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub(super) fn commit_received_schema_stage_with_receipt_for_test(
        &self,
        changes: &ChangeSet,
        received: &crate::protocol::ReceivedDdlContext,
        validated_lineages: &[(String, NaturalKey, Lsn, crate::protocol::WireRowLineage)],
        arrivals: &HashMap<Lsn, Option<Lsn>>,
        receipt: Option<SyncApplyReceipt>,
        fail_after_stage_registration: bool,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: contextdb_core::read_memory::ReadMemoryOwner,
    ) -> Result<crate::protocol::WireApplyResult> {
        let dependency_complete = receipt
            .as_ref()
            .is_some_and(|receipt| receipt.dependency_complete);
        self.commit_received_schema_stage(
            changes,
            Some(received),
            validated_lineages,
            arrivals,
            SyncAdoption::Continuing,
            SyncApplyRole::PullLeg,
            receipt,
            dependency_complete,
            None,
            None,
            None,
            None,
            None,
            |_| Ok(None),
            || {
                if fail_after_stage_registration {
                    return Err(Error::Other(
                        ("injected received-schema prepare failure".to_string()).into(),
                    ));
                }
                Ok(())
            },
            vector_copy_owner,
            read_memory.clone(),
        )
    }

    pub(super) fn durable_row_lineage_config_key(
        table: &str,
        natural_key: &NaturalKey,
        generation: u64,
    ) -> String {
        format!(
            "sync_row_lineage.v1.{generation:016x}.{}",
            Self::hex_component(&sync_identity_key(table, natural_key))
        )
    }

    pub(super) fn durable_unbound_creation_config_key(
        table: &str,
        natural_key: &NaturalKey,
        generation: u64,
        local_row_id: RowId,
    ) -> String {
        format!(
            "sync_creation_lineage.v1.{generation:016x}.{:016x}.{}",
            local_row_id.0,
            Self::hex_component(&sync_identity_key(table, natural_key))
        )
    }

    #[allow(clippy::too_many_arguments)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn lineage_attestation_bytes(
        tenant_id: &TenantId,
        table: &str,
        natural_key: &NaturalKey,
        generation: u64,
        author_node_id: &str,
        author_database_incarnation: Incarnation,
        author_local_mutation_position: Lsn,
        lineage_root: &str,
    ) -> Result<Vec<u8>> {
        rmp_serde::to_vec(&(
            "contextdb.row-lineage-attestation.v1",
            tenant_id.as_str(),
            table,
            natural_key.pairs(),
            generation,
            author_node_id,
            author_database_incarnation.to_hex(),
            author_local_mutation_position.0,
            lineage_root,
        ))
        .map_err(|err| {
            Error::SyncError((format!("cannot encode lineage attestation: {err}")).into())
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn verify_lineage_attestation(
        tenant_id: &TenantId,
        table: &str,
        natural_key: &NaturalKey,
        lineage: &crate::protocol::WireRowLineage,
    ) -> Result<()> {
        if lineage.attestation.is_empty() {
            return Err(Error::SyncError(
                ("authenticated row lineage is missing its creator signature".to_string()).into(),
            ));
        }
        let bytes = Self::lineage_attestation_bytes(
            tenant_id,
            table,
            natural_key,
            lineage.table_generation,
            &lineage.author_node_id,
            lineage.author_database_incarnation,
            lineage.author_local_mutation_position,
            &lineage.lineage_root,
        )?;
        crate::identity::FabricIdentity::verify_lineage_by_node_id(
            &lineage.author_node_id,
            &bytes,
            &lineage.attestation,
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn stage_local_creation_lineage(
        &self,
        tx: TxId,
        table: &str,
        natural_key: NaturalKey,
        local_row_id: RowId,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if let DatabaseTransactionHost::Owned(handle) = &self.tx_mgr {
            return handle.with_host(|host| {
                self.stage_original_creation_lineage(
                    host,
                    tx,
                    table,
                    &natural_key,
                    local_row_id,
                    read_memory,
                )
            });
        }

        let generation =
            self.transaction_lineage_table_generation(tx, table, read_memory.clone())?;
        let record = DurableUnboundCreationLineage {
            table: table.to_string(),
            natural_key: natural_key.clone(),
            table_generation: generation,
            local_row_id,
            creation_lsn: None,
        };
        self.tx_mgr.with_write_set(tx, |ws| {
            ws.config_writes.push((
                Self::durable_unbound_creation_config_key(
                    table,
                    &natural_key,
                    generation,
                    local_row_id,
                ),
                RedbPersistence::encode_config_value(&record)?,
            ));
            Ok(())
        })?
    }

    /// Encode the existing config record directly into its original admitted
    /// buffers. The borrowed serialization has the same field order and bytes
    /// as DurableUnboundCreationLineage; no transient identity Vec is copied.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn stage_original_creation_lineage(
        &self,
        host: &EngineDatabaseTransactions,
        tx: TxId,
        table: &str,
        natural_key: &NaturalKey,
        local_row_id: RowId,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        #[derive(serde::Serialize)]
        struct BorrowedLineage<'a> {
            table: &'a str,
            natural_key: &'a NaturalKey,
            table_generation: u64,
            local_row_id: RowId,
            creation_lsn: Option<Lsn>,
        }
        struct ByteCount(usize);
        impl std::io::Write for ByteCount {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0 = self
                    .0
                    .checked_add(bytes.len())
                    .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::OutOfMemory))?;
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        // Stream the exact existing identity bytes as hex, avoiding an
        // allocate-first sync_identity_key/hex_component temporary.
        struct HexWriter<'a>(&'a mut String);
        impl std::io::Write for HexWriter<'_> {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                const HEX: &[u8; 16] = b"0123456789abcdef";
                for byte in bytes {
                    self.0.push(HEX[(byte >> 4) as usize] as char);
                    self.0.push(HEX[(byte & 15) as usize] as char);
                }
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        self.tx_mgr.with_read(tx, |_| ())?;
        let root = self.writer_read_root()?;
        let _scope = self.enter_writer_read(root, read_memory.clone());
        let generation = self.durable_lineage_table_generation(table, read_memory)?;
        let record = BorrowedLineage {
            table,
            natural_key,
            table_generation: generation,
            local_row_id,
            creation_lsn: None,
        };
        let mut encoded_count = ByteCount(0);
        bincode::serde::encode_into_std_write(
            &record,
            &mut encoded_count,
            bincode::config::standard(),
        )
        .map_err(|error| Error::Other((format!("bincode encode error: {error}")).into()))?;
        let mut identity_count = ByteCount(0);
        serde::Serialize::serialize(
            natural_key,
            &mut rmp_serde::Serializer::new(&mut identity_count),
        )
        .map_err(|error| {
            Error::Other((format!("sync natural key encode error: {error}")).into())
        })?;
        let key_bytes = 8usize
            .checked_add(table.len())
            .and_then(|bytes| bytes.checked_add(identity_count.0))
            .and_then(|bytes| bytes.checked_mul(2))
            .and_then(|bytes| {
                bytes.checked_add("sync_creation_lineage.v1.".len() + 16 + 1 + 16 + 1)
            })
            .ok_or_else(|| Error::Other("creation lineage key size overflow".into()))?;
        let body_bytes = key_bytes
            .checked_add(encoded_count.0)
            .ok_or_else(|| Error::Other("creation lineage body size overflow".into()))?;
        struct OriginalConfig {
            body: (String, Vec<u8>),
            admission: OwnedMemoryReservation,
        }
        // The reservation precedes both original native allocations. It
        // follows the same actual writer use retained by the paired host.
        let admission = OwnedMemoryReservation::try_new_for(
            crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
            body_bytes,
            "relational",
            "creation_lineage_config",
            "Reduce staged lineage records or raise MEMORY_LIMIT.",
        )?;
        let mut original = OriginalConfig {
            body: (
                String::with_capacity(key_bytes),
                Vec::with_capacity(encoded_count.0),
            ),
            admission,
        };
        std::fmt::Write::write_fmt(
            &mut original.body.0,
            format_args!(
                "sync_creation_lineage.v1.{generation:016x}.{:016x}.",
                local_row_id.0,
            ),
        )
        .map_err(|_| Error::Other("creation lineage key formatting failed".into()))?;
        {
            let mut hex = HexWriter(&mut original.body.0);
            std::io::Write::write_all(&mut hex, &(table.len() as u64).to_be_bytes()).map_err(
                |error| {
                    Error::Other((format!("creation lineage key encode error: {error}")).into())
                },
            )?;
            std::io::Write::write_all(&mut hex, table.as_bytes()).map_err(|error| {
                Error::Other((format!("creation lineage key encode error: {error}")).into())
            })?;
            serde::Serialize::serialize(natural_key, &mut rmp_serde::Serializer::new(&mut hex))
                .map_err(|error| {
                    Error::Other((format!("sync natural key encode error: {error}")).into())
                })?;
        }
        bincode::serde::encode_into_std_write(
            &record,
            &mut original.body.1,
            bincode::config::standard(),
        )
        .map_err(|error| Error::Other((format!("bincode encode error: {error}")).into()))?;
        if original.body.0.len() != key_bytes
            || original.body.0.capacity() != key_bytes
            || original.body.1.len() != encoded_count.0
            || original.body.1.capacity() != encoded_count.0
        {
            return Err(Error::Other(
                "creation lineage original capacity mismatch".into(),
            ));
        }
        let invoice = original
            .admission
            .into_source_aux_invoice_for(&host.funder)?;
        host.transactions.with_transactions(|transactions| {
            transactions.with_engine(tx, |guard| {
                let at = guard.read().config_writes.len();
                let batch = guard.prepare_owned_aux(
                    contextdb_tx::staged_source::AuxBody::ConfigWrite(original.body),
                    invoice,
                )?;
                let prepared = guard.prepare_aux_splice(at, &[], batch)?;
                guard.publish_tx(prepared);
                Ok(())
            })
        })
    }
}

/// The typed internal refusal for a received schema stage on a database that
/// has no durable store to lend its private image a working write.
pub(super) fn received_schema_requires_durable_receiver() -> Error {
    Error::Other(
        "received schema stages require a durable receiver; this database has no durable store"
            .into(),
    )
}

/// The local schema image's metadata fold between its admission and the
/// moment the table image holds it. Dropped while it still owns the fold (a
/// failure before the tables are installed), it returns the fold.
struct PreadmittedMetadataFold<'a> {
    accountant: &'a crate::memory_accounting::AccountantHandle,
    bytes: usize,
}

impl PreadmittedMetadataFold<'_> {
    /// The table image now holds the fold and returns it when it drops.
    fn transfer_to_image(&mut self) {
        self.bytes = 0;
    }
}

impl Drop for PreadmittedMetadataFold<'_> {
    fn drop(&mut self) {
        self.accountant.release(std::mem::take(&mut self.bytes));
    }
}
