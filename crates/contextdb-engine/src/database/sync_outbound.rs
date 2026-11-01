use super::*;

impl Database {
    /// The pull cursor bound to the specific store that issued it —
    /// `(source incarnation, lsn)` — loaded as ONE record so a crash between
    /// persisting the two halves separately can never pair a NEW cursor with
    /// the OLD source (or vice versa) after a Redb reopen or process restart.
    /// Memory retains the same atomic pair only for reconstructed or scoped
    /// clients sharing this database life; `None` means no combined pull cursor
    /// has yet been recorded for this database life.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn persisted_sync_pull_cursor(
        &self,
        tenant_id: &TenantId,
    ) -> Result<Option<(Incarnation, Lsn)>> {
        let read_memory = self.declared_reader_memory()?;
        self.persisted_sync_pull_cursor_with_read_memory(tenant_id, read_memory)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn persisted_sync_pull_cursor_with_read_memory(
        &self,
        tenant_id: &TenantId,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<(Incarnation, Lsn)>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let _operation = self.open_operation()?;
        let _writer_read = self.writer_read_scope(read_memory.clone())?;
        let Some(_persistence) = &self.persistence else {
            return Ok(self
                .in_memory_sync_progress
                .lock()
                .tenants
                .get(tenant_id.as_str())
                .and_then(|progress| progress.pull_cursor));
        };
        let key = tenant_id.config_key("sync_pull_cursor");
        match self.read_config_value::<(String, u64)>(&key, read_memory.clone())? {
            Some((hex, lsn)) => {
                let source = Incarnation::from_hex(&hex).ok_or_else(|| {
                    crate::funded_error::corruption_display(
                        Ok(read_memory.clone()),
                        &key,
                        format_args!(
                            "persisted sync pull cursor source is not valid hex: {hex} — {}",
                            RedbPersistence::CORRUPT_STORE_NEXT_STEP
                        ),
                    )
                })?;
                Ok(Some((source, Lsn(lsn))))
            }
            None => Ok(None),
        }
    }

    sync_test_seam! {
    #[cfg(any(test, feature = "test-seams"))]
    /// Persist the pull cursor's `(source incarnation, lsn)` pair as ONE
    /// record (see [`Self::persisted_sync_pull_cursor`]).
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn persist_sync_pull_cursor(
        &self,
        tenant_id: &TenantId,
        source: Incarnation,
        watermark: Lsn,
    ) -> Result<()> {
        let _operation = self.open_operation()?;
        if let Some(persistence) = &self.persistence {
            self.publish_direct_metadata(|| persistence.flush_config_value(
                &tenant_id.config_key("sync_pull_cursor"),
                &(source.to_hex(), watermark.0),
            ))?;
        } else {
            self.in_memory_sync_progress
                .lock()
                .tenants
                .entry(tenant_id.as_str().to_string())
                .or_default()
                .pull_cursor = Some((source, watermark));
        }
        Ok(())
    }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn persist_sync_pull_cursor_while_authoritative(
        &self,
        tenant_id: &TenantId,
        source: Incarnation,
        watermark: Lsn,
    ) -> Result<()> {
        if let Some(persistence) = &self.persistence {
            self.publish_direct_metadata(|| {
                persistence.flush_config_value(
                    &tenant_id.config_key("sync_pull_cursor"),
                    &(source.to_hex(), watermark.0),
                )
            })?;
        } else {
            self.in_memory_sync_progress
                .lock()
                .tenants
                .entry(tenant_id.as_str().to_string())
                .or_default()
                .pull_cursor = Some((source, watermark));
        }
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn existing_sync_incarnation(
        &self,
        tenant: &TenantId,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<Incarnation>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let _operation = self.open_operation()?;
        let _writer_read = self.writer_read_scope(read_memory.clone())?;
        if let Some(incarnation) = self.sync_incarnations.lock().get(tenant.as_str()).copied() {
            return Ok(Some(incarnation));
        }
        self.persistence
            .as_ref()
            .map(|_p| {
                self.read_config_value::<String>(
                    &tenant.config_key("sync_incarnation"),
                    read_memory.clone(),
                )
            })
            .transpose()?
            .flatten()
            .map(|s| Incarnation::from_hex(&s).ok_or_else(crate::custody::canonical::invalid))
            .transpose()
    }

    sync_test_seam! {
    /// This database life's sync incarnation for `tenant_id`, minted the first
    /// time it is asked for and stable thereafter. A file-backed database
    /// persists it as a config value, so a plain reopen loads the same one; a
    /// wipe-and-recreate loses the config with the data and re-mints a fresh one.
    /// An in-memory database holds it in this handle's cache only — every
    /// in-memory open is a fresh life. The edge stamps it on every push and
    /// status exchange so the hub can key its per-edge record by
    /// `(node_id, incarnation)`.
    #[expect(clippy::result_large_err, reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating.")]
    #[cfg_attr(not(feature = "test-seams"), allow(dead_code, reason = "KEEP-SEAM: live only under the test-seams feature"))]
    fn sync_incarnation(&self, tenant_id: &TenantId) -> Result<Incarnation> {
        let read_memory = self.declared_reader_memory()?;
        self.sync_incarnation_with_read_memory(tenant_id, read_memory)
    }

    #[expect(clippy::result_large_err, reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating.")]
    pub(crate) fn sync_incarnation_with_read_memory(&self, tenant_id: &TenantId,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Incarnation> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let _operation = self.open_operation()?;
        let operation = || {

        let _writer_read = self.writer_read_scope(read_memory.clone())?;
        let mut cache = self.sync_incarnations.lock();
        if let Some(incarnation) = cache.get(tenant_id.as_str()) {
            return Ok(*incarnation);
        }
        let key = tenant_id.config_key("sync_incarnation");
        let incarnation = if let Some(persistence) = &self.persistence {
            match self.read_config_value::<String>(&key, read_memory.clone())? {
                Some(hex) => Incarnation::from_hex(&hex).ok_or_else(|| crate::funded_error::corruption_display(Ok(read_memory.clone()), &key, format_args!(
                        "persisted sync incarnation is not valid hex: {hex} — {}",
                        RedbPersistence::CORRUPT_STORE_NEXT_STEP
                    )))?,
                None => {
                    let minted = Incarnation::mint();
                    self.publish_direct_metadata(|| persistence.flush_config_value(&key, &minted.to_hex()))?;
                    minted
                }
            }
        } else {
            Incarnation::mint()
        };
        cache.insert(tenant_id.as_str().to_string(), incarnation);
        Ok(incarnation)

        };
        if self.tx_mgr.commit_boundary_held_by_current_thread() {
            operation()
        } else {
            self.tx_mgr.with_maintenance_publication(|_| operation())
        }
    }
    }

    /// Read the database life's durable sync incarnation for the fixed
    /// installed-release identity proof. This exists only in the separately
    /// built verifier feature; ordinary engine consumers get no new surface.
    #[cfg(feature = "production-smoke-driver")]
    pub fn production_smoke_sync_incarnation(&self, tenant_id: &TenantId) -> Result<Incarnation> {
        let read_memory = self.declared_reader_memory()?;
        self.production_smoke_sync_incarnation_with_read_memory(tenant_id, read_memory)
    }

    #[cfg(feature = "production-smoke-driver")]
    pub(crate) fn production_smoke_sync_incarnation_with_read_memory(
        &self,
        tenant_id: &TenantId,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Incarnation> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.sync_incarnation_with_read_memory(tenant_id, read_memory)
    }

    /// Test-only access to the same durable per-tenant identity stamped on
    /// real sync requests and receipts.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn sync_incarnation_for_test(&self, tenant_id: &TenantId) -> Result<Incarnation> {
        self.sync_incarnation_with_read_memory(tenant_id, self.declared_reader_memory()?)
    }

    /// Per-tenant record of the highest edge-LSN this database has applied
    /// from sync pushes. Stored as a config value so `export_snapshot` carries
    /// it with the artifact at the snapshot point, exactly like the tenant sync
    /// watermark keys. In-memory databases retain this value for the life of
    /// the handle; `None` means no push has been applied for the tenant.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn persisted_sync_applied_push_watermark(
        &self,
        tenant_id: &TenantId,
    ) -> Result<Option<Lsn>> {
        let read_memory = self.declared_reader_memory()?;
        self.persisted_sync_applied_push_watermark_with_read_memory(tenant_id, read_memory)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn persisted_sync_applied_push_watermark_with_read_memory(
        &self,
        tenant_id: &TenantId,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<Lsn>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let _operation = self.open_operation()?;
        let _writer_read = self.writer_read_scope(read_memory.clone())?;
        if let Some(_persistence) = &self.persistence {
            return Ok(self
                .read_config_value::<u64>(
                    &tenant_id.config_key("sync_applied_push_watermark"),
                    read_memory.clone(),
                )?
                .map(Lsn));
        }
        Ok(self
            .in_memory_applied_push_watermarks
            .lock()
            .get(tenant_id.as_str())
            .copied())
    }

    sync_test_seam! {
    /// Persist the per-tenant applied-push watermark. Callers enforce
    /// monotonic advancement; this only writes the config value or the
    /// in-memory equivalent for ephemeral databases.
    #[expect(clippy::result_large_err, reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating.")]
    #[cfg_attr(not(feature = "test-seams"), allow(dead_code, reason = "KEEP-SEAM: live only under the test-seams feature; used from main.rs"))]
    fn persist_sync_applied_push_watermark(
        &self,
        tenant_id: &TenantId,
        watermark: Lsn,
    ) -> Result<()> {
        let _operation = self.open_operation()?;
        if let Some(persistence) = &self.persistence {
            self.publish_direct_metadata(|| persistence.flush_config_value(
                &tenant_id.config_key("sync_applied_push_watermark"),
                &watermark.0,
            ))?;
        } else {
            self.in_memory_applied_push_watermarks
                .lock()
                .insert(tenant_id.as_str().to_string(), watermark);
        }
        Ok(())
    }
    }

    /// The prefix every per-`(tenant, edge, incarnation)` applied-push key shares
    /// for one edge. The incarnation hex is appended to it and the tenant is the
    /// `":<tenant>"` suffix `config_key` adds, so all of one edge's incarnation
    /// records across tenants sort together under this prefix — which is how the
    /// edge-lifetime lookup below collects them. The `.inc.` delimiter cannot
    /// appear inside a transport node id, so one edge's keys never bleed into
    /// another's.
    pub(super) fn applied_push_watermark_node_prefix(node_id: &str) -> String {
        format!("sync_applied_push_watermark_node.{node_id}.inc.")
    }

    /// The one place the per-`(tenant, edge, incarnation)` applied-push key is
    /// authored, so the on-disk format stays byte-identical across readers and
    /// writers.
    pub(super) fn applied_push_watermark_node_incarnation_key(
        tenant_id: &TenantId,
        node_id: &str,
        incarnation: Incarnation,
    ) -> String {
        tenant_id.config_key(&format!(
            "{}{}",
            Self::applied_push_watermark_node_prefix(node_id),
            incarnation.to_hex()
        ))
    }

    sync_test_seam! {
    #[cfg(any(test, feature = "test-seams"))]
    /// Seed a durable per-edge receipt only for a test fixture whose schema
    /// was installed outside the sync stream. Production receipt writes stay
    /// coupled to accepted source changes in the ordinary apply path.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn persist_sync_applied_push_watermark_for_node_incarnation(
        &self,
        tenant_id: &TenantId,
        node_id: &str,
        incarnation: Incarnation,
        watermark: Lsn,
    ) -> Result<()> {
        let _operation = self.open_operation()?;
        let key = Self::applied_push_watermark_node_incarnation_key(tenant_id, node_id, incarnation);
        if let Some(persistence) = &self.persistence {
            self.publish_direct_metadata(|| persistence.flush_config_value(&key, &watermark.0))?;
        } else {
            self.in_memory_applied_push_watermarks
                .lock()
                .insert(key, watermark);
        }
        Ok(())
    }
    }

    /// Per-`(tenant, edge, incarnation)` record of the highest edge-LSN this
    /// database has applied from THAT life of THAT edge's pushes. The per-tenant
    /// record above is raised by whichever edge pushed last, so with more than
    /// one edge it cannot answer "what do you hold from ME?"; and keying by edge
    /// alone would let a wiped-and-recreated edge — same node id, LSNs reset —
    /// be false-confirmed by the prior life's stale-high watermark. Keying by
    /// incarnation makes a rebuilt life an unknown edge, answered `None`. Stored
    /// the same way as the per-tenant record (a config value, carried by
    /// `export_snapshot`; in-memory for ephemeral databases). `None` means this
    /// hub has applied nothing from that life of that edge.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn persisted_sync_applied_push_watermark_for_node_incarnation(
        &self,
        tenant_id: &TenantId,
        node_id: &str,
        incarnation: Incarnation,
    ) -> Result<Option<Lsn>> {
        let read_memory = self.declared_reader_memory()?;
        self.persisted_sync_applied_push_watermark_for_node_incarnation_with_read_memory(
            tenant_id,
            node_id,
            incarnation,
            read_memory,
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn persisted_sync_applied_push_watermark_for_node_incarnation_with_read_memory(
        &self,
        tenant_id: &TenantId,
        node_id: &str,
        incarnation: Incarnation,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<Lsn>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let _operation = self.open_operation()?;
        let _writer_read = self.writer_read_scope(read_memory.clone())?;
        let key =
            Self::applied_push_watermark_node_incarnation_key(tenant_id, node_id, incarnation);
        if let Some(_persistence) = &self.persistence {
            return Ok(self
                .read_config_value::<u64>(&key, read_memory.clone())?
                .map(Lsn));
        }
        Ok(self
            .in_memory_applied_push_watermarks
            .lock()
            .get(&key)
            .copied())
    }

    /// The highest edge-LSN this database has applied from `node_id` across ALL
    /// of that edge's lives (incarnations) — the operator-facing "what have I
    /// ever taken from this edge?" figure, computed from the per-incarnation
    /// records so there is a single source of truth. `None` when this hub has
    /// applied nothing from the edge. The delivery-confirmation path never reads
    /// this aggregate; it reads the exact `(node, incarnation)` record above so a
    /// rebuilt life cannot be vouched for by an older one.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn persisted_sync_applied_push_watermark_for_node(
        &self,
        tenant_id: &TenantId,
        node_id: &str,
    ) -> Result<Option<Lsn>> {
        let _operation = self.open_operation()?;
        let prefix = Self::applied_push_watermark_node_prefix(node_id);
        let tenant_suffix = format!(":{}", tenant_id.as_str());
        if let Some(persistence) = &self.persistence {
            let max = persistence
                .load_config_values_with_prefix::<u64>(&prefix)?
                .into_iter()
                .filter(|(key, _)| key.ends_with(&tenant_suffix))
                .map(|(_, value)| value)
                .max();
            return Ok(max.map(Lsn));
        }
        Ok(self
            .in_memory_applied_push_watermarks
            .lock()
            .iter()
            .filter(|(key, _)| key.starts_with(&prefix) && key.ends_with(&tenant_suffix))
            .map(|(_, value)| *value)
            .max())
    }

    /// Return ordered change occurrences after the watermark from one captured root.
    /// Durable integrity and memory-admission refusals reach the caller unchanged.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn change_log_since(&self, since_lsn: Lsn) -> Result<Vec<ChangeLogEntry>> {
        let read_memory = self.declared_reader_memory()?;
        self.change_log_since_with_read_memory(since_lsn, read_memory)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn change_log_since_with_read_memory(
        &self,
        since_lsn: Lsn,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Vec<ChangeLogEntry>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let _operation = self.open_operation()?;
        if !self.access_is_admin() {
            let mut vector_copy_owner = crate::vector_read_memory::admission(read_memory.clone());
            return self.gated_change_log_since(since_lsn, &mut vector_copy_owner, read_memory);
        }
        self.with_commit_lock(|| self.change_log_since_unlocked(since_lsn, read_memory.clone()))
    }

    /// What committing the change-log entries recorded since `since_lsn`
    /// actually charged the accountant: `change_log_retained_bytes` over
    /// those entries -- the exact arithmetic
    /// `composite_store::apply` performs at commit time
    /// (`crates/contextdb-engine/src/composite_store.rs:1341-1343`). A test
    /// that must prove the accountant charges exactly what a commit's own
    /// retained change-log entries cost, with no slack, cannot observe that
    /// arithmetic any other way from outside the crate.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __change_log_commit_charge_since_for_test(&self, since_lsn: Lsn) -> usize {
        let entries = self
            .change_log_since_with_read_memory(
                since_lsn,
                self.declared_reader_memory()
                    .expect("change-log observation read owner"),
            )
            .expect("change-log charge observation must admit its source read");
        crate::composite_store::change_log_retained_bytes(&entries)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn gated_change_log_since(
        &self,
        since_lsn: Lsn,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Vec<ChangeLogEntry>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let mut selection_credit =
            contextdb_core::read_memory::ReadCredit::new(read_memory.clone());
        let (changes, _, _) = self.checked_changes_since_with_arrivals(
            since_lsn,
            vector_copy_owner,
            read_memory.clone(),
            &mut selection_credit,
            None,
        )?;
        let mut entries = Vec::new();
        for row in changes.rows {
            if row.deleted {
                entries.push(ChangeLogEntry::RowDelete {
                    table: row.table,
                    row_id: RowId(0),
                    natural_key: row.natural_key,
                    lsn: row.lsn,
                });
            } else if let Some(row_id) = self
                .visible_row_by_natural_key(
                    &row.table,
                    &row.natural_key,
                    self.history_change_snapshot_at(
                        row.lsn,
                        read_memory.clone(),
                        &unbounded_checkpoint,
                    )?,
                    &HashSet::new(),
                    read_memory.clone(),
                )?
                .map(|row| row.row_id)
            {
                entries.push(ChangeLogEntry::RowInsert {
                    table: row.table,
                    row_id,
                    lsn: row.lsn,
                });
            }
        }
        for edge in changes.edges {
            if matches!(edge.properties.get("__deleted"), Some(Value::Bool(true))) {
                entries.push(ChangeLogEntry::EdgeDelete {
                    source: edge.source,
                    target: edge.target,
                    edge_type: edge.edge_type,
                    lsn: edge.lsn,
                });
            } else {
                entries.push(ChangeLogEntry::EdgeInsert {
                    source: edge.source,
                    target: edge.target,
                    edge_type: edge.edge_type,
                    lsn: edge.lsn,
                });
            }
        }
        for vector in changes.vectors {
            if vector.vector.is_empty() {
                entries.push(ChangeLogEntry::VectorDelete {
                    index: vector.index,
                    row_id: vector.row_id,
                    lsn: vector.lsn,
                });
            } else {
                entries.push(ChangeLogEntry::VectorInsert {
                    index: vector.index,
                    row_id: vector.row_id,
                    lsn: vector.lsn,
                });
            }
        }
        entries.sort_by_key(ChangeLogEntry::lsn);
        Ok(entries)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn change_replay_since_unlocked(
        &self,
        since_lsn: Lsn,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<writer_read::ChangeReplay> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        #[cfg(any(test, feature = "test-seams"))]
        let since_lsn = Lsn(since_lsn
            .0
            .max(self.forced_change_log_gap_through.load(Ordering::SeqCst)));
        let (_, root) = self.capture_writer_statement()?;
        if let Some(root) = root {
            if since_lsn >= root.lsn {
                return Ok(writer_read::ChangeReplay::Resident(Vec::new().into_iter()));
            }
            return root
                .change_log_cursor(
                    crate::persistence::writer_source::SourceWindowOwner::Read(read_memory.clone()),
                    Lsn(since_lsn.0 + 1),
                    root.lsn,
                )
                .map(writer_read::ChangeReplay::Durable);
        }
        self.change_log_since_unlocked(since_lsn, read_memory)
            .map(|entries| writer_read::ChangeReplay::Resident(entries.into_iter()))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn change_log_since_unlocked(
        &self,
        since_lsn: Lsn,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Vec<ChangeLogEntry>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        // The forced gap hides the same committed prefix in resident and
        // native logs; writes after the seam remain available for replay.
        #[cfg(any(test, feature = "test-seams"))]
        let since_lsn = Lsn(since_lsn
            .0
            .max(self.forced_change_log_gap_through.load(Ordering::SeqCst)));
        let (_, root) = self.capture_writer_statement()?;
        if let Some(root) = root {
            if since_lsn >= root.lsn {
                return Ok(Vec::new());
            }
            let mut cursor = root.change_log_cursor(
                crate::persistence::writer_source::SourceWindowOwner::Read(read_memory.clone()),
                Lsn(since_lsn.0 + 1),
                root.lsn,
            )?;
            let mut output = contextdb_core::read_memory::ReadCredit::new(read_memory.clone());
            let mut entries = Vec::new();
            while let Some(window) = cursor.next_change()? {
                output.try_grow_for(
                    answer_list_entry_bytes::<ChangeLogEntry>(change_log_entry_copy_bytes(
                        &window.value,
                    )),
                    "writer_source",
                    "change_log_output",
                    "Raise MEMORY_LIMIT to return these change-log entries.",
                )?;
                entries.push(window.value);
            }
            self.retain_writer_output(output);
            return Ok(entries);
        }
        if let Some(history) = self.image_history.as_ref() {
            let mut output = contextdb_core::read_memory::ReadCredit::new(read_memory.clone());
            let mut entries = Vec::new();
            history.source.visit_changes_since(
                since_lsn,
                read_memory,
                &|| Ok(()),
                &mut |entry| {
                    output.try_grow_for(
                        answer_list_entry_bytes::<ChangeLogEntry>(change_log_entry_copy_bytes(
                            &entry,
                        )),
                        "history_source",
                        "history_output",
                        "Raise the reader limit to return history.",
                    )?;
                    entries.push(entry);
                    Ok(())
                },
            )?;
            self.retain_writer_output(output);
            return Ok(entries);
        }
        let log = self.change_log.read();
        let start = log.partition_point(|e| {
            #[cfg(any(test, feature = "test-seams"))]
            resident_history_test_seams::note(|work| work.log_keys_examined += 1);
            e.lsn() <= since_lsn
        });
        #[cfg(any(test, feature = "test-seams"))]
        resident_history_test_seams::note(|work| work.log_keys_examined += log.len() - start);
        Ok(log[start..].to_vec())
    }

    /// Returns declaration history, propagating durable read and admission failures.
    /// Callers without administrative access receive an empty history.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn ddl_log_since(&self, since_lsn: Lsn) -> Result<Vec<DdlChange>> {
        let read_memory = self.declared_reader_memory()?;
        self.ddl_log_since_with_read_memory(since_lsn, read_memory)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn ddl_log_since_with_read_memory(
        &self,
        since_lsn: Lsn,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Vec<DdlChange>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let _operation = self.open_operation()?;
        if !self.access_is_admin() {
            return Ok(Vec::new());
        }
        self.with_commit_lock(|| self.ddl_log_since_unlocked(since_lsn, read_memory.clone()))
    }

    /// The latest durable declaration for each locally known table name.
    ///
    /// Pull receives entries ordered by the remote server's LSN clock, so it
    /// must not compare those LSNs with this database's history. A DROP leaves
    /// the last declaration intact here: the local policy still decides
    /// whether remote work for that former generation may be applied.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn declared_directions_for_tables<'a>(
        &self,
        tables: impl Iterator<Item = &'a str>,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<HashMap<String, SyncDirection>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let Some(root) = self.writer_read_root()? else {
            return self.latest_declared_table_directions(read_memory.clone());
        };
        let mut directions = HashMap::new();
        for table in tables {
            if !directions.contains_key(table)
                && let Some(direction) = RedbPersistence::declared_direction_in_read(
                    &root,
                    &crate::persistence::writer_source::SourceWindowOwner::Read(
                        read_memory.clone(),
                    ),
                    table,
                    None,
                )?
            {
                directions.insert(table.to_string(), direction);
            }
        }
        Ok(directions)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn try_sync_direction_history_for_changes(
        &self,
        changes: &ChangeSet,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<SyncDirectionHistory> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let Some(root) = self.writer_read_root()? else {
            return self.try_sync_direction_history(read_memory.clone());
        };
        let mut history = SyncDirectionHistory::default();
        let positions = changes
            .rows
            .iter()
            .map(|row| (row.table.as_str(), row.lsn))
            .chain(
                changes
                    .vectors
                    .iter()
                    .map(|vector| (vector.index.table.as_str(), vector.lsn)),
            )
            .collect::<BTreeSet<_>>();
        for (table, lsn) in positions {
            let direction = RedbPersistence::declared_direction_in_read(
                &root,
                &crate::persistence::writer_source::SourceWindowOwner::Read(read_memory.clone()),
                table,
                Some(lsn),
            )?
            .unwrap_or(SyncDirection::Both);
            history.record_create(table.to_string(), lsn, direction);
        }
        Ok(history)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn latest_declared_table_directions(
        &self,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<HashMap<String, SyncDirection>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let _operation = self.assert_open_operation();
        let mut directions = HashMap::new();
        let ddl_log = self.checked_ddl_entries_since(Lsn(0), read_memory.clone())?;
        let mut ddl_entries = ddl_log.iter().collect::<Vec<_>>();
        ddl_entries.sort_by_key(|(lsn, _)| *lsn);
        for (_, change) in ddl_entries {
            match change {
                DdlChange::CreateTable {
                    name, constraints, ..
                } => {
                    let direction = constraints
                        .iter()
                        .find_map(|constraint| constraint_declares_sync_direction(constraint))
                        .unwrap_or(contextdb_core::DEFAULT_SYNC_DIRECTION);
                    directions.insert(name.clone(), direction);
                }
                // An ALTER with no direction clause preserves the declaration
                // made by its CREATE or a previous ALTER.
                DdlChange::AlterTable {
                    name, constraints, ..
                } => {
                    if let Some(direction) = constraints
                        .iter()
                        .find_map(|constraint| constraint_declares_sync_direction(constraint))
                    {
                        directions.insert(name.clone(), direction);
                    }
                }
                // Do not remove the declaration: a local DROP must not make
                // previously local-only work become eligible on a later pull.
                DdlChange::DropTable { .. } => {}
                _ => {}
            }
        }
        Ok(directions)
    }

    /// Reconstructs the durable declaration that governed every table
    /// generation. Live metadata is deliberately not consulted: a DROP removes
    /// it, while outbound history still needs the dropped generation's policy.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn try_sync_direction_history(
        &self,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<SyncDirectionHistory> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let _operation = self.assert_open_operation();
        let mut history = SyncDirectionHistory::default();
        let ddl_log = self.checked_ddl_entries_since(Lsn(0), read_memory.clone())?;
        let mut ddl_entries = ddl_log.iter().collect::<Vec<_>>();
        ddl_entries.sort_by_key(|(lsn, _)| *lsn);
        for (lsn, change) in ddl_entries {
            match change {
                DdlChange::CreateTable {
                    name, constraints, ..
                } => {
                    let direction = constraints
                        .iter()
                        .find_map(|constraint| constraint_declares_sync_direction(constraint))
                        .unwrap_or(contextdb_core::DEFAULT_SYNC_DIRECTION);
                    history.record_create(name.clone(), *lsn, direction);
                }
                DdlChange::AlterTable {
                    name, constraints, ..
                } => {
                    if let Some(direction) = constraints
                        .iter()
                        .find_map(|constraint| constraint_declares_sync_direction(constraint))
                    {
                        history.record_alter(name.clone(), *lsn, direction);
                    }
                }
                DdlChange::DropTable { name } => history.record_drop(name.clone(), *lsn),
                _ => {}
            }
        }
        Ok(history)
    }

    #[cfg(test)]
    pub(crate) fn sync_direction_history(&self) -> SyncDirectionHistory {
        let fixture_operation_reader_49 = self
            .declared_reader_memory()
            .expect("admit the actual fixture reader");

        self.try_sync_direction_history(fixture_operation_reader_49.clone())
            .expect("fixture declaration history")
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn checked_ddl_entries_since(
        &self,
        since: Lsn,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<writer_read::DdlHistory> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.ddl_log_entries_since_unlocked(since, read_memory.clone())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn durable_ddl_entries_since(
        &self,
        since: Lsn,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<writer_read::DdlHistory> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let (_, root) = self.capture_writer_statement()?;
        let root = root.ok_or_else(|| {
            Error::Other("durable declaration history requires a captured writer source".into())
        })?;
        if since >= root.lsn {
            return Ok(Vec::new().into());
        }
        let mut cursor = root.ddl_history_cursor(
            crate::persistence::writer_source::SourceWindowOwner::Read(read_memory.clone()),
            since,
        )?;
        let mut output = contextdb_core::read_memory::ReadCredit::new(read_memory.clone());
        let mut entries = Vec::new();
        while let Some(window) = cursor.next_ddl_history()? {
            output.try_grow_for(
                answer_list_entry_bytes::<(Lsn, DdlChange)>(window.value.1.estimated_bytes()),
                "writer_source",
                "declaration_history_output",
                "Raise MEMORY_LIMIT to return declaration history.",
            )?;
            entries.push(window.value);
        }
        Ok(writer_read::DdlHistory {
            entries,
            memory: Some(output),
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn ddl_log_since_unlocked(
        &self,
        since_lsn: Lsn,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Vec<DdlChange>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        Ok(self
            .ddl_log_entries_since_unlocked(since_lsn, read_memory.clone())?
            .into_iter()
            .map(|(_, change)| change)
            .collect())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn ddl_log_entries_since_unlocked(
        &self,
        since_lsn: Lsn,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<writer_read::DdlHistory> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if self.persistence.is_some() {
            return self.durable_ddl_entries_since(since_lsn, read_memory.clone());
        }
        if let Some(history) = self.image_history.as_ref() {
            let mut output = contextdb_core::read_memory::ReadCredit::new(read_memory.clone());
            let mut entries = Vec::new();
            history.source.visit_ddl_since(
                since_lsn,
                read_memory,
                &|| Ok(()),
                &mut |lsn, entry| {
                    output.try_grow_for(
                        answer_list_entry_bytes::<(Lsn, DdlChange)>(entry.estimated_bytes()),
                        "history_source",
                        "declaration_output",
                        "Raise the reader limit to return declaration history.",
                    )?;
                    entries.push((lsn, entry));
                    Ok(())
                },
            )?;
            entries.sort_by_key(|(lsn, _)| *lsn);
            return Ok(writer_read::DdlHistory {
                entries,
                memory: Some(output),
            });
        }
        let ddl = self.ddl_log.read();
        let mut entries = ddl
            .iter()
            .filter(|(lsn, _)| *lsn > since_lsn)
            .cloned()
            .collect::<Vec<_>>();
        entries.sort_by_key(|(lsn, _)| *lsn);
        Ok(entries.into())
    }

    /// Read one immutable persisted-log occurrence. Received-arrival digests
    /// bind exactly this authored form.
    /// When the in-memory log no longer covers it (a persisted-state
    /// extraction), the durable log answers.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn raw_ddl_log_occurrence(
        &self,
        lsn: Lsn,
        ordinal: u32,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<DdlChange> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if let Some(history) = self.image_history.as_ref() {
            let mut seen = 0u32;
            let mut found = None;
            let mut output = contextdb_core::read_memory::ReadCredit::new(read_memory.clone());
            history.source.visit_ddl_since(
                Lsn(lsn.0.saturating_sub(1)),
                read_memory,
                &|| Ok(()),
                &mut |entry_lsn, entry| {
                    if entry_lsn == lsn {
                        if seen == ordinal {
                            output.try_grow_for(
                                entry.estimated_bytes(),
                                "history_source",
                                "ddl_occurrence",
                                "Raise the reader limit to read declaration history.",
                            )?;
                            found = Some(entry);
                        }
                        seen = seen.checked_add(1).ok_or_else(|| {
                            Error::Other("DDL occurrence ordinal overflow".into())
                        })?;
                    }
                    Ok(())
                },
            )?;
            return found.ok_or_else(|| Error::SyncError((format!(
                "received DDL arrival lacks raw persisted occurrence at local LSN {} ordinal {}", lsn.0, ordinal)).into()));
        }
        if let (_, Some(root)) = self.capture_writer_statement()? {
            let mut cursor = root.ddl_history_cursor(
                crate::persistence::writer_source::SourceWindowOwner::Read(read_memory.clone()),
                Lsn(lsn.0.saturating_sub(1)),
            )?;
            let mut seen = 0u32;
            while let Some(window) = cursor.next_ddl_history()? {
                if window.value.0 != lsn {
                    break;
                }
                if seen == ordinal {
                    return Ok(window.value.1);
                }
                seen += 1;
            }
            return Err(Error::SyncError((format!(
                "received DDL arrival lacks raw persisted occurrence at local LSN {} ordinal {}",
                lsn.0, ordinal
            )).into()));
        }
        let occurrence = |log: &[(Lsn, DdlChange)]| {
            log.iter()
                .filter(|(entry_lsn, _)| *entry_lsn == lsn)
                .nth(ordinal as usize)
                .map(|(_, change)| change.clone())
        };
        let in_memory = occurrence(&self.ddl_log.read());
        let found = match in_memory {
            Some(change) => Some(change),
            None if self.persistence.is_some() => {
                occurrence(&self.durable_ddl_history_for_provenance(read_memory.clone())?)
            }
            None => None,
        };
        found.ok_or_else(|| {
            Error::SyncError((format!(
                "received DDL arrival lacks raw persisted occurrence at local LSN {} ordinal {}",
                lsn.0, ordinal
            )).into())
        })
    }

    /// Builds a complete snapshot of all live data as a ChangeSet.
    /// Used as fallback when change_log/ddl_log cannot serve a watermark.
    pub(super) fn full_state_snapshot_ddl(&self) -> Vec<DdlChange> {
        if !self.access_is_admin() {
            return Vec::new();
        }
        let meta_guard = self.relational_store.table_meta.read();
        let mut ddl = full_snapshot_ddl(&meta_guard);
        let live_tables = meta_guard.keys().cloned().collect::<HashSet<_>>();
        drop(meta_guard);
        ddl.extend(self.trigger_snapshot_ddl_for_tables(&live_tables));
        ddl.extend(self.event_bus_snapshot_ddl_for_tables(&live_tables));
        ddl
    }

    /// Builds a complete snapshot of all live data as a ChangeSet.
    /// Used as fallback when change_log/ddl_log cannot serve a watermark.
    #[cfg(all(feature = "test-seams", unix))]
    #[doc(hidden)]
    pub fn arm_persisted_state_schema_frontier_pause_for_test(&self, socket_path: PathBuf) {
        PERSISTED_STATE_SCHEMA_FRONTIER_PAUSES
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
            .insert(self as *const Self as usize, socket_path);
    }

    #[cfg(all(feature = "test-seams", unix))]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub(super) fn pause_at_persisted_state_schema_frontier_for_test(&self) -> Result<()> {
        use std::io::{Read, Write};
        use std::os::unix::net::UnixStream;

        let Some(socket_path) = PERSISTED_STATE_SCHEMA_FRONTIER_PAUSES
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
            .remove(&(self as *const Self as usize))
        else {
            return Ok(());
        };
        let mut stream = UnixStream::connect(&socket_path).map_err(|error| {
            Error::SyncError(
                (format!(
                    "connect persisted-state schema-frontier test pause {}: {error}",
                    socket_path.display()
                ))
                .into(),
            )
        })?;
        stream.write_all(&[1]).map_err(|error| {
            Error::SyncError(
                (format!("announce persisted-state schema-frontier test pause: {error}")).into(),
            )
        })?;
        let mut release = [0u8; 1];
        stream.read_exact(&mut release).map_err(|error| {
            Error::SyncError(
                (format!("wait for persisted-state schema-frontier test release: {error}")).into(),
            )
        })?;
        if release != [1] {
            return Err(Error::SyncError(
                ("persisted-state schema-frontier test release was malformed".to_string()).into(),
            ));
        }
        Ok(())
    }

    #[cfg(all(feature = "test-seams", unix))]
    pub(super) fn persisted_state_schema_frontier_pause_is_armed_for_test(&self) -> bool {
        PERSISTED_STATE_SCHEMA_FRONTIER_PAUSES
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
            .contains_key(&(self as *const Self as usize))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn full_state_snapshot(
        &self,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<ChangeSet> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        // Freeze the source ceiling before enumerating any body. A commit that
        // lands during enumeration belongs to the next extraction; using a
        // later global clock as this snapshot's DDL position would claim that
        // unseen commit was already represented here.
        let snapshot_ceiling = self
            .scoped_writer_root()
            .map_or_else(|| self.current_lsn(), |root| root.lsn);
        let mut rows = Vec::new();
        let mut edges = Vec::new();
        let mut vectors = Vec::new();
        let ddl = self.full_state_snapshot_ddl();

        let acl_snapshot = self.snapshot_for_read();
        let meta_guard = self.relational_store.table_meta.read();

        // Rows (live only) — collect row_ids that have live rows for orphan vector filtering
        let mut live_row_ids: HashSet<RowId> = HashSet::new();
        let empty_skip_deleted = HashSet::new();
        self.visit_authoritative_live_rows_for_outbound(
            |table_name, row| {
                let meta = match meta_guard.get(table_name) {
                    Some(m) => m,
                    None => return Ok(()),
                };
                if natural_key_columns_for_meta(meta).is_none() {
                    return Ok(());
                }
                if row.lsn > snapshot_ceiling {
                    return Ok(());
                }
                if !self.row_read_allowed_for_change(
                    table_name,
                    row,
                    acl_snapshot,
                    read_memory.clone(),
                )? {
                    return Ok(());
                }
                let Some(natural_key) = natural_key_from_row_values(meta, &row.values) else {
                    return Ok(());
                };
                let Some(current) = self.visible_row_by_natural_key(
                    table_name,
                    &natural_key,
                    acl_snapshot,
                    &empty_skip_deleted,
                    read_memory.clone(),
                )?
                else {
                    return Ok(());
                };
                if current.row_id != row.row_id {
                    return Ok(());
                }
                live_row_ids.insert(row.row_id);
                rows.push(RowChange {
                    table: table_name.to_owned(),
                    natural_key,
                    values: self.outbound_row_values(
                        table_name,
                        row,
                        vector_copy_owner,
                        read_memory.clone(),
                    )?,
                    deleted: false,
                    lsn: row.lsn,
                    created_at: row.created_at,
                });
                Ok(())
            },
            read_memory.clone(),
        )?;
        drop(meta_guard);

        // Edges (live only)
        self.visit_graph_versions(
            |entry| {
                if !(entry.deleted_tx.is_none() && entry.lsn <= snapshot_ceiling) {
                    return Ok(());
                }
                if !self.graph_edge_read_allowed_for_change(
                    entry.source,
                    entry.target,
                    &entry.edge_type,
                    acl_snapshot,
                    read_memory.clone(),
                )? {
                    return Ok(());
                }
                edges.push(EdgeChange {
                    source: entry.source,
                    target: entry.target,
                    edge_type: entry.edge_type.clone(),
                    properties: entry.properties.clone(),
                    lsn: entry.lsn,
                });
                Ok(())
            },
            read_memory.clone(),
        )?;

        // Vectors (live only, skip orphans)
        for entry in self
            .vector_store
            .try_all_entries(vector_copy_owner)?
            .into_iter()
            .filter(|v| v.deleted_tx.is_none() && v.lsn <= snapshot_ceiling)
        {
            if !live_row_ids.contains(&entry.row_id) {
                continue; // skip orphan vectors
            }
            vectors.push(VectorChange {
                index: entry.index.clone(),
                row_id: entry.row_id,
                vector: entry.vector,
                lsn: entry.lsn,
            });
        }

        #[cfg(all(feature = "test-seams", unix))]
        self.pause_at_persisted_state_schema_frontier_for_test()?;

        let first_data_lsn = rows
            .iter()
            .map(|row| row.lsn)
            .chain(edges.iter().map(|edge| edge.lsn))
            .chain(vectors.iter().map(|vector| vector.lsn))
            .min()
            .unwrap_or(snapshot_ceiling);
        let snapshot_schema_lsn = if rows.is_empty() && edges.is_empty() && vectors.is_empty() {
            snapshot_ceiling
        } else {
            Lsn(first_data_lsn.0.saturating_sub(1))
        };
        let ddl_lsn = vec![snapshot_schema_lsn; ddl.len()];
        Ok(ChangeSet {
            rows,
            edges,
            vectors,
            ddl,
            ddl_lsn,
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn visit_authoritative_row_versions(
        &self,
        mut visit: impl FnMut(&str, &VersionedRow) -> Result<()>,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if let Some(root) = self.writer_read_root()? {
            use std::ops::Bound;
            for (table, _) in root.captured_table_metadata() {
                let mut cursor = root.cursor(
                    crate::persistence::writer_source::SourceWindowOwner::Read(read_memory.clone()),
                    &RedbPersistence::rel_table_name(table),
                    crate::persistence::writer_source::WriterCursorFamily::Bytes,
                    Bound::Unbounded,
                    Bound::Unbounded,
                    false,
                )?;
                while let Some(window) = cursor.next_row()? {
                    visit(table, &window.value)?;
                }
            }
        } else {
            let tables = self.relational_store.tables.read();
            for (table, rows) in tables.iter() {
                for row in rows {
                    visit(table, row)?;
                }
            }
        }
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn visit_authoritative_live_rows_for_outbound(
        &self,
        mut visit: impl FnMut(&str, &VersionedRow) -> Result<()>,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if let Some(root) = self.writer_read_root()? {
            use std::ops::Bound;
            for (table, _) in root.captured_table_metadata() {
                let mut cursor = root.cursor(
                    crate::persistence::writer_source::SourceWindowOwner::Read(read_memory.clone()),
                    &RedbPersistence::rel_table_name(table),
                    crate::persistence::writer_source::WriterCursorFamily::Bytes,
                    Bound::Unbounded,
                    Bound::Unbounded,
                    true,
                )?;
                let mut selected = None;
                while let Some(window) = cursor.next_row()? {
                    let row = &window.value;
                    if selected == Some(row.row_id) || row.created_tx.0 > root.snapshot.0 {
                        continue;
                    }
                    selected = Some(row.row_id);
                    if row.visible_at(root.snapshot) {
                        visit(table, row)?;
                    }
                }
            }
            return Ok(());
        }
        for (table, rows) in self.authoritative_live_rows_for_outbound() {
            for row in rows.values() {
                visit(&table, row)?;
            }
        }
        Ok(())
    }

    /// Enumerate each row instance through the current row-position map so a
    /// stale historical version cannot reappear after its latest position is
    /// deleted or replaced by a same-key row with a new RowId.
    pub(super) fn authoritative_live_rows_for_outbound(
        &self,
    ) -> HashMap<String, HashMap<RowId, VersionedRow>> {
        let row_ids = self
            .relational_store
            .tables
            .read()
            .iter()
            .flat_map(|(table, rows)| rows.iter().map(|row| (table.clone(), row.row_id)))
            .collect::<Vec<_>>();
        self.relational_store.live_rows_by_id(&row_ids)
    }

    /// A full-state snapshot answered as the change set from the first
    /// position: when the durable schema log still projects to the snapshot's
    /// schema, it keeps its authored order and provenance instead of the
    /// rendered declarations.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn persisted_state_from_snapshot(
        &self,
        changes: ChangeSet,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<(ChangeSet, DdlProvenanceSource)> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.persisted_state_from_snapshot_projected(
            changes,
            &ChangeProjection::unbounded(),
            read_memory.clone(),
        )
    }

    /// [`Self::persisted_state_from_snapshot`] under a history answer's
    /// bounds. An answer that carries no schema reconstructs none. Otherwise
    /// the schema-log copy is admitted before it is taken, each statement's
    /// growth of the reconstructed schema is admitted before the statement
    /// is applied with the checkpoint between statements, and the canonical
    /// copies the comparison makes are admitted before they exist.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn persisted_state_from_snapshot_projected(
        &self,
        mut changes: ChangeSet,
        projection: &ChangeProjection<'_>,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<(ChangeSet, DdlProvenanceSource)> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if !projection.ddl {
            let source = DdlProvenanceSource::synthetic_snapshot(&changes);
            return Ok((changes, source));
        }
        // A gap in the row/change log does not erase the durable DDL log.
        // When that history still projects to the current schema, retain
        // its source transaction order and provenance instead of replacing
        // it with the alphabetically rendered snapshot declaration.
        if projection.bounded {
            let ddl_log = self.ddl_log.read();
            let copied = ddl_log
                .iter()
                .map(|(_, change)| {
                    // The copy, and the list slots it is split into.
                    std::mem::size_of::<(Lsn, DdlChange)>()
                        .saturating_add(std::mem::size_of::<DdlChange>())
                        .saturating_add(std::mem::size_of::<Lsn>())
                        .saturating_add(change.estimated_bytes())
                })
                .sum::<usize>();
            drop(ddl_log);
            (projection.admit)(copied)?;
        }
        let history = self.ddl_log_entries_since_unlocked(Lsn(0), read_memory.clone())?;
        if !history.is_empty() {
            let (history_lsn, history_ddl): (Vec<_>, Vec<_>) = history.into_iter().unzip();
            let historical =
                self.project_received_schema_from_empty_projected(&history_ddl, projection)?;
            let snapshot =
                self.project_received_schema_from_empty_projected(&changes.ddl, projection)?;
            (projection.checkpoint)()?;
            if projection.bounded {
                (projection.admit)(
                    historical
                        .tables
                        .values()
                        .chain(snapshot.tables.values())
                        .map(|meta| meta.estimated_bytes())
                        .sum(),
                )?;
            }
            if Self::canonical_received_schema_tables(&historical.tables)
                == Self::canonical_received_schema_tables(&snapshot.tables)
                && historical.event_bus == snapshot.event_bus
                && historical.trigger == snapshot.trigger
            {
                if self.has_received_ddl_arrivals()? {
                    self.refuse_unattributed_received_history(Lsn(0), read_memory.clone())?;
                }
                changes.ddl = history_ddl;
                changes.ddl_lsn = history_lsn;
                let source = DdlProvenanceSource::historical(&changes);
                return Ok((changes, source));
            }
        }
        if !changes.ddl.is_empty() && self.has_received_ddl_arrivals()? {
            let (ddl, ddl_lsn) =
                self.received_schema_as_authored(Lsn(0), &changes.ddl, read_memory.clone())?;
            changes.ddl = ddl;
            changes.ddl_lsn = ddl_lsn;
            let source = DdlProvenanceSource::historical(&changes);
            return Ok((changes, source));
        }
        let source = DdlProvenanceSource::synthetic_snapshot(&changes);
        Ok((changes, source))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn persisted_state_since_with_ddl_provenance(
        &self,
        since_lsn: Lsn,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<(ChangeSet, DdlProvenanceSource)> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.persisted_state_since_through(since_lsn, None, false, vector_copy_owner, read_memory)
    }

    /// The persisted-state extraction, ending at `through` when it is one page
    /// of a longer history: rows, links, vectors and schema history after
    /// `through` wait for the next page. With `row_vectors`, each selected
    /// row's vectors come from that row's own values rather than from a copy
    /// of the whole vector store, so a page's memory follows the page.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn persisted_state_since_through(
        &self,
        since_lsn: Lsn,
        through: Option<Lsn>,
        row_vectors: bool,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<(ChangeSet, DdlProvenanceSource)> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if since_lsn == Lsn(0) && !row_vectors {
            return self.persisted_state_from_snapshot(
                self.full_state_snapshot(vector_copy_owner, read_memory.clone())?,
                read_memory.clone(),
            );
        }

        // This ceiling and every selected row/vector/edge form one immutable
        // outbound snapshot. Work committed later must retain a larger LSN so
        // the next request can still see it.
        let snapshot_ceiling = self
            .scoped_writer_root()
            .map_or_else(|| self.current_lsn(), |root| root.lsn);
        let snapshot_ceiling =
            through.map_or(snapshot_ceiling, |through| snapshot_ceiling.min(through));

        let mut rows = Vec::new();
        let mut edges = Vec::new();
        let mut vectors = Vec::new();
        let mut ddl = Vec::new();

        let acl_snapshot = self.snapshot_for_read();
        let meta_guard = self.relational_store.table_meta.read();

        let mut live_row_ids: HashSet<RowId> = HashSet::new();
        let empty_skip_deleted = HashSet::new();
        self.visit_authoritative_live_rows_for_outbound(
            |table_name, row| {
                let meta = match meta_guard.get(table_name) {
                    Some(meta) => meta,
                    None => return Ok(()),
                };
                if natural_key_columns_for_meta(meta).is_none() {
                    return Ok(());
                }
                if row.lsn > snapshot_ceiling {
                    return Ok(());
                }
                // A page reads nothing for the rows an earlier page served:
                // their access and current-version checks would be charged to
                // this request, so its memory would grow with the history.
                if row_vectors && row.lsn <= since_lsn {
                    return Ok(());
                }
                if !self.row_read_allowed_for_change(
                    table_name,
                    row,
                    acl_snapshot,
                    read_memory.clone(),
                )? {
                    return Ok(());
                }
                let Some(natural_key) = natural_key_from_row_values(meta, &row.values) else {
                    return Ok(());
                };
                let Some(current) = self.visible_row_by_natural_key(
                    table_name,
                    &natural_key,
                    acl_snapshot,
                    &empty_skip_deleted,
                    read_memory.clone(),
                )?
                else {
                    return Ok(());
                };
                if current.row_id != row.row_id {
                    return Ok(());
                }
                live_row_ids.insert(row.row_id);
                if row.lsn <= since_lsn {
                    return Ok(());
                }
                let values = self.outbound_row_values(
                    table_name,
                    row,
                    vector_copy_owner,
                    read_memory.clone(),
                )?;
                if row_vectors {
                    // One page takes each selected row's vectors from the
                    // row's own values, as the change-log path does, rather
                    // than copying the vector store.
                    for column in meta
                        .columns
                        .iter()
                        .filter(|column| matches!(column.column_type, ColumnType::Vector(_)))
                    {
                        if let Some(Value::Vector(vector)) = values.get(&column.name) {
                            vectors.push(VectorChange {
                                index: VectorIndexRef::new(table_name, column.name.clone()),
                                row_id: row.row_id,
                                vector: vector.clone(),
                                lsn: row.lsn,
                            });
                        }
                    }
                }
                rows.push(RowChange {
                    table: table_name.to_owned(),
                    natural_key,
                    values,
                    deleted: false,
                    lsn: row.lsn,
                    created_at: row.created_at,
                });
                Ok(())
            },
            read_memory.clone(),
        )?;
        drop(meta_guard);

        self.visit_graph_versions(
            |entry| {
                if !(entry.deleted_tx.is_none()
                    && entry.lsn > since_lsn
                    && entry.lsn <= snapshot_ceiling)
                {
                    return Ok(());
                }
                if !self.graph_edge_read_allowed_for_change(
                    entry.source,
                    entry.target,
                    &entry.edge_type,
                    SnapshotId(entry.lsn.0),
                    read_memory.clone(),
                )? {
                    return Ok(());
                }
                edges.push(EdgeChange {
                    source: entry.source,
                    target: entry.target,
                    edge_type: entry.edge_type.clone(),
                    properties: entry.properties.clone(),
                    lsn: entry.lsn,
                });
                Ok(())
            },
            read_memory.clone(),
        )?;

        if !row_vectors {
            for entry in self
                .vector_store
                .try_all_entries(vector_copy_owner)?
                .into_iter()
                .filter(|entry| {
                    entry.deleted_tx.is_none()
                        && entry.lsn > since_lsn
                        && entry.lsn <= snapshot_ceiling
                })
            {
                if !live_row_ids.contains(&entry.row_id) {
                    continue;
                }
                vectors.push(VectorChange {
                    index: entry.index.clone(),
                    row_id: entry.row_id,
                    vector: entry.vector,
                    lsn: entry.lsn,
                });
            }
        }
        self.restore_vector_owner_rows(
            &mut rows,
            &vectors,
            vector_copy_owner,
            read_memory.clone(),
        )?;

        #[cfg(all(feature = "test-seams", unix))]
        self.pause_at_persisted_state_schema_frontier_for_test()?;

        // A persisted-state fallback has no historical DDL log to replay. If
        // this delta is paged at an older purge frontier, omitting schema here
        // would make the current snapshot disappear after the purge-only page.
        // Re-emit the complete current declaration at the first surviving data
        // frontier (or the current frontier for schema-only state). Grouping it
        // with that first data commit keeps schema ahead of its rows while
        // making the synthetic declaration strictly replayable after
        // `since_lsn`.
        let snapshot_schema_lsn = rows
            .iter()
            .map(|row| row.lsn)
            .chain(edges.iter().map(|edge| edge.lsn))
            .chain(vectors.iter().map(|vector| vector.lsn))
            .min()
            .unwrap_or(snapshot_ceiling);
        let snapshot_ddl = self.full_state_snapshot_ddl();
        let history = self.ddl_log_entries_since_unlocked(Lsn(0), read_memory.clone())?;
        let complete_history = if history.is_empty() {
            false
        } else {
            let historical = self.project_received_schema_from_empty(
                &history
                    .iter()
                    .map(|(_, ddl)| ddl.clone())
                    .collect::<Vec<_>>(),
            )?;
            let snapshot = self.project_received_schema_from_empty(&snapshot_ddl)?;
            Self::canonical_received_schema_tables(&historical.tables)
                == Self::canonical_received_schema_tables(&snapshot.tables)
                && historical.event_bus == snapshot.event_bus
                && historical.trigger == snapshot.trigger
        };
        // Schema this node received never leaves it as a restated snapshot:
        // the exact authored occurrences go instead, or the extraction refuses.
        let restates_received_schema = !complete_history
            && snapshot_schema_lsn > since_lsn
            && !snapshot_ddl.is_empty()
            && self.has_received_ddl_arrivals()?;
        if complete_history
            && snapshot_schema_lsn > since_lsn
            && self.has_received_ddl_arrivals()?
        {
            self.refuse_unattributed_received_history(since_lsn, read_memory.clone())?;
        }
        let ddl_lsn = if complete_history {
            let (lsns, changes): (Vec<_>, Vec<_>) = history
                .into_iter()
                .filter(|(lsn, _)| {
                    *lsn > since_lsn && through.is_none_or(|through| *lsn <= through)
                })
                .unzip();
            ddl = changes;
            lsns
        } else if restates_received_schema {
            let (authored, lsns) =
                self.received_schema_as_authored(since_lsn, &snapshot_ddl, read_memory.clone())?;
            ddl = authored;
            lsns
        } else if snapshot_schema_lsn > since_lsn {
            ddl = snapshot_ddl;
            vec![snapshot_schema_lsn; ddl.len()]
        } else {
            Vec::new()
        };

        let changes = ChangeSet {
            rows,
            edges,
            vectors,
            ddl,
            ddl_lsn,
        };
        let source = if changes.ddl.is_empty() || complete_history || restates_received_schema {
            DdlProvenanceSource::historical(&changes)
        } else {
            DdlProvenanceSource::synthetic_snapshot(&changes)
        };
        Ok((changes, source))
    }

    /// Serving the durable declaration history in place of a snapshot keeps
    /// every received occurrence exactly as authored, so each one in the
    /// served range must still carry its author's signed identity. Without
    /// it the extraction refuses, naming the table, exactly as
    /// [`Self::received_schema_as_authored`] does; it never serves received
    /// schema unattributed.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn refuse_unattributed_received_history(
        &self,
        since_lsn: Lsn,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.visit_received_ddl_arrivals(
            |key, arrival| {
                if key.0 <= since_lsn || arrival.author.is_some() {
                    return Ok(());
                }
                let table = self
                    .recorded_ddl_generation_sidecar(key.0, key.1)?
                    .and_then(|sidecar| sidecar.table)
                    .unwrap_or_else(|| format!("received at local LSN {}", key.0.0));
                Err(Error::ReceivedSchemaAuthorshipUnavailable {
                    table,
                    reason: "this node no longer holds the author's signed identity for the \
                         declaration it received"
                        .to_string(),
                })
            },
            read_memory.clone(),
        )
    }

    /// The schema a persisted-state extraction serves when this node holds
    /// schema it RECEIVED and its logs cannot cover the request.
    ///
    /// A snapshot of the current schema would present every received
    /// declaration as this node's own migration, under its signature and in
    /// its rendering. Instead this returns the exact durable occurrences, in
    /// commit order, after `since_lsn`: each received one still carries its
    /// arrival record with the author's signed block, so it leaves exactly as
    /// the author wrote it, and this node's own declarations keep their
    /// recorded positions. When that cannot be done honestly -- the durable
    /// history no longer reproduces the current schema, a received
    /// declaration's occurrence is gone, or its author's signed identity is
    /// missing -- it refuses, naming the table, rather than fabricate one.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn received_schema_as_authored(
        &self,
        since_lsn: Lsn,
        snapshot_ddl: &[DdlChange],
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<(Vec<DdlChange>, Vec<Lsn>)> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let history = self.durable_ddl_history_for_provenance(read_memory.clone())?;
        let mut next_ordinal = HashMap::<Lsn, u32>::new();
        let occurrences = history
            .iter()
            .map(|(lsn, ddl)| {
                let ordinal = next_ordinal.entry(*lsn).or_default();
                let key = (*lsn, *ordinal);
                *ordinal = ordinal.saturating_add(1);
                (key, ddl)
            })
            .collect::<HashMap<_, _>>();
        let mut first_arrival = None;
        let received_subject = |key: &(Lsn, u32)| -> Result<String> {
            if let Some(ddl) = occurrences.get(key) {
                return Ok(Self::declaration_subject(ddl));
            }
            Ok(self
                .recorded_ddl_generation_sidecar(key.0, key.1)?
                .and_then(|sidecar| sidecar.table)
                .unwrap_or_else(|| format!("received at local LSN {}", key.0.0)))
        };
        self.visit_received_ddl_arrivals(
            |key, arrival| {
                first_arrival = Some(first_arrival.map_or(key, |first: (Lsn, u32)| first.min(key)));
                if key.0 <= since_lsn {
                    return Ok(());
                }
                if !occurrences.contains_key(&key) {
                    return Err(Error::ReceivedSchemaAuthorshipUnavailable {
                        table: received_subject(&key)?,
                        reason:
                            "this node's durable schema history no longer holds the declaration \
                             it received"
                                .to_string(),
                    });
                }
                if arrival.author.is_none() {
                    return Err(Error::ReceivedSchemaAuthorshipUnavailable {
                        table: received_subject(&key)?,
                        reason: "this node no longer holds the author's signed identity for the \
                             declaration it received"
                            .to_string(),
                    });
                }
                Ok(())
            },
            read_memory.clone(),
        )?;
        let history_ddl = history
            .iter()
            .map(|(_, ddl)| ddl.clone())
            .collect::<Vec<_>>();
        let historical = self.project_received_schema_from_empty(&history_ddl)?;
        let snapshot = self.project_received_schema_from_empty(snapshot_ddl)?;
        if Self::canonical_received_schema_tables(&historical.tables)
            != Self::canonical_received_schema_tables(&snapshot.tables)
            || historical.event_bus != snapshot.event_bus
            || historical.trigger != snapshot.trigger
        {
            let table = match first_arrival {
                Some(key) => received_subject(&key)?,
                None => "unknown".to_string(),
            };
            return Err(Error::ReceivedSchemaAuthorshipUnavailable {
                table,
                reason: "this node's durable schema history no longer reproduces its current \
                         schema, so the received declarations cannot be served as authored"
                    .to_string(),
            });
        }
        Ok(history
            .into_iter()
            .filter(|(lsn, _)| *lsn > since_lsn)
            .map(|(lsn, ddl)| (ddl, lsn))
            .unzip())
    }

    /// The table a declaration belongs to, or the declaration's own name when
    /// it belongs to none.
    pub(super) fn declaration_subject(ddl: &DdlChange) -> String {
        if let Some(table) = Self::ddl_affected_table(ddl) {
            return table.to_string();
        }
        match ddl {
            DdlChange::CreateTable { name, .. }
            | DdlChange::DropTable { name }
            | DdlChange::AlterTable { name, .. }
            | DdlChange::CreateTrigger { name, .. }
            | DdlChange::DropTrigger { name }
            | DdlChange::CreateEventType { name, .. }
            | DdlChange::CreateSink { name, .. }
            | DdlChange::CreateRoute { name, .. }
            | DdlChange::DropRoute { name, .. }
            | DdlChange::CreateTriggerIncludingSync { name, .. } => name.clone(),
            DdlChange::CreateIndex { table, .. } | DdlChange::DropIndex { table, .. } => {
                table.clone()
            }
        }
    }

    /// Price the whole arriving change set, refuse it if it does not fit, and
    /// HOLD what the schema unit in it was priced at until the unit is
    /// installed or refused.
    ///
    /// The returned reservation is the difference between a comparison and a
    /// promise. Without it this read what was available, found the unit fitted
    /// and held nothing, so any admission between here and the unit's first
    /// item spent the room the unit had just been told it had -- and the unit,
    /// once admitted, installed anyway.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn preflight_sync_apply_memory(
        &self,
        changes: &ChangeSet,
        policies: &ConflictPolicies,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<Option<crate::memory_accounting::SchemaUnitReservation>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let usage = self.finalization.accountant.usage();
        let Some(limit) = usage.limit else {
            return Ok(None);
        };
        let available = usage.available.unwrap_or(limit);
        let unit_bytes = self.projected_sync_ddl_unit_bytes(
            &changes.ddl,
            vector_copy_owner,
            read_memory.clone(),
        )?;
        let mut required = unit_bytes;
        let (_catalog_charge, projected_meta) = self.projected_sync_table_meta(changes)?;

        let empty_skip_deleted = HashSet::new();
        for row in &changes.rows {
            let _row_outputs = self.writer_output_scope();
            if row.deleted || row.values.is_empty() {
                continue;
            }
            let table_meta = projected_meta
                .get(&row.table)
                .ok_or_else(|| Error::TableNotFound(row.table.clone()))?;

            let policy =
                Self::sync_conflict_policy_for_table(policies, Some(table_meta), &row.table);
            let existing = if self.table_meta(&row.table).is_some() {
                // Probe the WHOLE natural key, matching the apply path
                // (`sync_visible_point_lookup`). A table-level composite PK
                // auto-indexes only the whole tuple, so a leading-column-only
                // probe finds no covering index and aborts the entire apply.
                self.required_indexed_visible_row_by_columns(
                    &row.table,
                    &row.natural_key.key_columns(),
                    &row.natural_key.key_values(),
                    self.snapshot(),
                    &empty_skip_deleted,
                    read_memory.clone(),
                )?
            } else {
                None
            };

            if existing.is_some()
                && matches!(
                    policy,
                    ConflictPolicy::InsertIfNotExists | ConflictPolicy::ServerWins
                )
            {
                continue;
            }

            required = required.saturating_add(retained_row_bytes_for_meta(
                &row.table,
                &row.values,
                table_meta,
                true,
            ));
        }

        for edge in &changes.edges {
            // The same estimate the apply itself will charge, so a preflight
            // that says the batch fits is not followed by an apply that
            // refuses it.
            required = required.saturating_add(estimate_edge_bytes(
                edge.source,
                edge.target,
                &edge.edge_type,
                &edge.properties,
            ));
        }

        for vector in &changes.vectors {
            if vector.vector.is_empty() {
                continue;
            }
            let bytes = projected_meta
                .get(&vector.index.table)
                .and_then(|meta| {
                    meta.columns
                        .iter()
                        .find(|column| column.name == vector.index.column)
                        .map(|column| column.quantization.storage_bytes(vector.vector.len()))
                })
                .unwrap_or_else(|| {
                    vector
                        .vector
                        .len()
                        .saturating_mul(std::mem::size_of::<f32>())
                });
            required = required.saturating_add(24 + bytes);
        }

        if required > available {
            return Err(Error::MemoryBudgetExceeded {
                subsystem: ("sync".to_string()).into(),
                operation: ("apply_changes".to_string()).into(),
                requested_bytes: required,
                available_bytes: available,
                budget_limit_bytes: limit,
                hint:
                    ("Reduce sync batch size, split the push, or raise MEMORY_LIMIT on the server."
                        .to_string())
                    .into(),
            });
        }

        if unit_bytes == 0 {
            return Ok(None);
        }
        // Only the SCHEMA unit's price is held. The rows, edges and vectors
        // above are admitted one by one as they apply and are refused the
        // ordinary way if the room goes, so holding them here as well would
        // count them twice. The schema unit is the one that cannot be refused
        // half way through, so it is the one whose room has to still be there.
        self.finalization
            .accountant
            .reserve_schema_unit(
                unit_bytes,
                "sync",
                "received_schema_unit",
                "Raise MEMORY_LIMIT on the server, or send the schema change in smaller units.",
            )
            .map(Some)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn projected_sync_table_meta(
        &self,
        changes: &ChangeSet,
    ) -> Result<(StagedCopyCharge, HashMap<String, TableMeta>)> {
        let (charge, mut projected) =
            self.selected_received_catalog(changes, std::iter::empty())?;

        for change in &changes.ddl {
            Self::apply_sync_table_ddl_to_projected_meta(&mut projected, change);
        }

        Ok((charge, projected))
    }

    pub(super) fn apply_sync_table_ddl_to_projected_meta(
        projected: &mut HashMap<String, TableMeta>,
        change: &DdlChange,
    ) {
        match change {
            DdlChange::CreateTable {
                name,
                columns,
                constraints,
                foreign_keys,
                composite_foreign_keys,
                composite_unique,
            } => {
                let incoming = rough_sync_table_meta(
                    columns,
                    constraints,
                    foreign_keys,
                    composite_foreign_keys,
                    composite_unique,
                );
                match projected.get_mut(name) {
                    // A pre-existing table ADOPTS the arriving declaration's
                    // policy, direction, and retention — the same overwrite the
                    // real apply performs — so the memory charge is computed
                    // against the policy apply will actually run under, not the
                    // pre-adoption default. Keeping the old policy here can
                    // over-charge (a keep-latest projection charges an update a
                    // keep-first apply skips) or under-charge (the reverse).
                    // For one of the four engine-owned built-in ledger tables
                    // the real apply now PRESERVES a silent axis rather than
                    // clearing it (`engine_owned_merged_policy`) -- mirrored
                    // here too, so this projection never tells a LATER DDL in
                    // the same batch (against the same table) that an axis
                    // was cleared when the real apply will not clear it.
                    Some(meta) => {
                        let merged =
                            crate::executor::engine_owned_merged_policy(name, meta, &incoming);
                        let table_is_engine_owned = merged.is_some();
                        let (conflict, direction, ttl, sync_safe, unit, history) = match merged {
                            Some(merged) => (
                                merged.conflict_policy,
                                merged.sync_direction,
                                merged.default_ttl_seconds,
                                merged.sync_safe,
                                merged.retain_declared_unit,
                                merged.history_policy,
                            ),
                            None => (
                                incoming.conflict_policy,
                                incoming.sync_direction,
                                incoming.default_ttl_seconds,
                                incoming.sync_safe,
                                incoming.retain_declared_unit,
                                incoming.history_policy,
                            ),
                        };
                        meta.conflict_policy = conflict;
                        meta.sync_direction = direction;
                        meta.default_ttl_seconds = ttl;
                        meta.sync_safe = sync_safe;
                        meta.retain_declared_unit = unit;
                        meta.history_policy = history;
                        // An engine-owned table declares no maintained count,
                        // so a merged shape preserves what is there; a plain
                        // table takes the author's declaration verbatim, like
                        // every other axis above.
                        if !table_is_engine_owned {
                            meta.maintained_counts = incoming.maintained_counts.clone();
                        }
                    }
                    None => {
                        projected.insert(name.clone(), incoming);
                    }
                }
            }
            DdlChange::AlterTable {
                name,
                columns,
                constraints,
                foreign_keys,
                composite_foreign_keys,
                composite_unique,
            } => {
                if let Some(meta) = projected.get_mut(name) {
                    let incoming = rough_sync_table_meta(
                        columns,
                        constraints,
                        foreign_keys,
                        composite_foreign_keys,
                        composite_unique,
                    );
                    Self::apply_sync_alter_to_projected_table_meta(
                        name,
                        meta,
                        incoming,
                        constraints,
                    );
                }
            }
            DdlChange::DropTable { name } => {
                projected.remove(name);
            }
            DdlChange::CreateIndex {
                table,
                name,
                columns,
            } => {
                if let Some(meta) = projected.get_mut(table)
                    && !meta.indexes.iter().any(|index| index.name == *name)
                {
                    meta.indexes.push(IndexDecl {
                        name: name.clone(),
                        columns: columns.clone(),
                        kind: IndexKind::UserDeclared,
                    });
                }
            }
            DdlChange::DropIndex { table, name } => {
                if let Some(meta) = projected.get_mut(table)
                    && crate::executor::reserved_index_prefix(name).is_none()
                {
                    meta.indexes.retain(|index| index.name != *name);
                }
            }
            DdlChange::CreateEventType { .. }
            | DdlChange::CreateTrigger { .. }
            | DdlChange::CreateTriggerIncludingSync { .. }
            | DdlChange::DropTrigger { .. }
            | DdlChange::CreateSink { .. }
            | DdlChange::CreateRoute { .. }
            | DdlChange::DropRoute { .. } => {}
        }
    }

    pub(super) fn sync_meta_column_names(meta: &TableMeta) -> HashSet<String> {
        meta.columns
            .iter()
            .map(|column| column.name.clone())
            .collect()
    }

    pub(super) fn sync_vector_columns(
        meta: &TableMeta,
    ) -> BTreeMap<String, (usize, VectorQuantization)> {
        meta.columns
            .iter()
            .filter_map(|column| match column.column_type {
                ColumnType::Vector(dimension) => {
                    Some((column.name.clone(), (dimension, column.quantization)))
                }
                _ => None,
            })
            .collect()
    }

    pub(super) fn sync_alter_is_full_shape(
        existing: &TableMeta,
        incoming: &TableMeta,
        constraints: &[String],
    ) -> bool {
        let incoming_column_names = Self::sync_meta_column_names(incoming);
        if existing
            .columns
            .iter()
            .filter(|column| !matches!(column.column_type, ColumnType::Vector(_)))
            .any(|column| !incoming_column_names.contains(&column.name))
        {
            return false;
        }

        let existing_vector_columns = Self::sync_vector_columns(existing);
        let incoming_vector_columns = Self::sync_vector_columns(incoming);
        let existing_column_names = Self::sync_meta_column_names(existing);
        let added_non_vector = incoming.columns.iter().any(|column| {
            !matches!(column.column_type, ColumnType::Vector(_))
                && !existing_column_names.contains(&column.name)
        });
        if existing_vector_columns.iter().any(|(column, shape)| {
            incoming_vector_columns
                .get(column)
                .is_some_and(|incoming_shape| incoming_shape != shape)
        }) {
            return true;
        }
        let marked_rename =
            Self::sync_marked_vector_rename(existing, incoming, constraints).is_some();

        // Only a vector column the author explicitly dropped or renamed away
        // leaves; one the ALTER merely omits is kept.
        let removed =
            Self::sync_marked_removed_vector_columns(existing, incoming, constraints).len();
        let added = incoming_vector_columns
            .keys()
            .filter(|column| !existing_vector_columns.contains_key(*column))
            .count();
        if removed == 0 {
            return added > 0;
        }
        if added == 0 {
            return !added_non_vector;
        }
        marked_rename
    }

    /// The vector columns a received ALTER removes: those its author named in
    /// a `COLUMN_DROP(col)` marker, plus the source of a marked vector rename.
    /// A vector column the ALTER merely omits is never among them.
    pub(super) fn sync_marked_removed_vector_columns(
        existing: &TableMeta,
        incoming: &TableMeta,
        constraints: &[String],
    ) -> Vec<String> {
        let incoming_vector_columns = Self::sync_vector_columns(incoming);
        let dropped = sync_column_drops_from_constraints(constraints);
        let renamed_from =
            Self::sync_marked_vector_rename(existing, incoming, constraints).map(|(from, _)| from);
        Self::sync_vector_columns(existing)
            .into_keys()
            .filter(|column| {
                !incoming_vector_columns.contains_key(column)
                    && (dropped.contains(column) || renamed_from.as_ref() == Some(column))
            })
            .collect()
    }

    pub(super) fn sync_marked_vector_rename(
        existing: &TableMeta,
        incoming: &TableMeta,
        constraints: &[String],
    ) -> Option<(String, String)> {
        let marked = sync_vector_rename_from_constraints(constraints)?;
        let existing_vector_columns = Self::sync_vector_columns(existing);
        let incoming_vector_columns = Self::sync_vector_columns(incoming);
        let (from, to) = marked;
        if incoming_vector_columns.contains_key(&from) || existing_vector_columns.contains_key(&to)
        {
            return None;
        }
        (existing_vector_columns.get(&from) == incoming_vector_columns.get(&to))
            .then_some((from, to))
    }

    pub(super) fn merged_sync_full_shape_vector_alter_meta(
        table_name: &str,
        existing: &TableMeta,
        incoming: TableMeta,
        constraints: &[String],
    ) -> TableMeta {
        let existing_columns = existing
            .columns
            .iter()
            .map(|column| (column.name.clone(), column.clone()))
            .collect::<HashMap<_, _>>();
        let renamed_vector_column =
            Self::sync_marked_vector_rename(existing, &incoming, constraints);
        // A column the ALTER leaves out and its author neither dropped nor
        // renamed stays exactly where it was, with its data.
        let incoming_column_names = Self::sync_meta_column_names(&incoming);
        let authored_drops = sync_column_drops_from_constraints(constraints);
        let authored_renames = sync_column_renames_from_constraints(constraints);
        let kept_omitted = existing
            .columns
            .iter()
            .enumerate()
            .filter(|(_, column)| {
                !incoming_column_names.contains(&column.name)
                    && !authored_drops.contains(&column.name)
                    && renamed_vector_column
                        .as_ref()
                        .is_none_or(|(from, _)| *from != column.name)
                    && !authored_renames.iter().any(|(from, to)| {
                        *from == column.name && incoming_column_names.contains(to)
                    })
            })
            .map(|(position, column)| (position, column.clone()))
            .collect::<Vec<_>>();
        let mut merged = existing.clone();
        merged.columns = incoming
            .columns
            .into_iter()
            .map(|incoming_column| {
                let existing_column = existing_columns
                    .get(&incoming_column.name)
                    .or_else(|| {
                        renamed_vector_column
                            .as_ref()
                            .and_then(|(from, to)| (to == &incoming_column.name).then_some(from))
                            .and_then(|from| existing_columns.get(from))
                    })
                    .cloned();
                if let Some(mut column) = existing_column {
                    let incoming_references = incoming_column.references.clone();
                    let incoming_name = incoming_column.name.clone();
                    let incoming_column_type = incoming_column.column_type.clone();
                    let incoming_quantization = incoming_column.quantization;
                    column.name = incoming_name;
                    column.column_type = incoming_column_type;
                    column.quantization = incoming_quantization;
                    merge_sync_alter_existing_column(&mut column, incoming_column);
                    column.references = incoming_references;
                    column
                } else {
                    incoming_column
                }
            })
            .collect();
        for (position, column) in kept_omitted {
            let position = position.min(merged.columns.len());
            merged.columns.insert(position, column);
        }
        if !incoming.unique_constraints.is_empty() {
            merged.unique_constraints = incoming.unique_constraints;
        }
        if !incoming.composite_foreign_keys.is_empty() {
            merged.composite_foreign_keys = incoming.composite_foreign_keys;
        }
        let final_column_names = Self::sync_meta_column_names(&merged);
        let user_indexes = existing
            .indexes
            .iter()
            .filter(|index| {
                index.kind == IndexKind::UserDeclared
                    && index
                        .columns
                        .iter()
                        .all(|(column, _)| final_column_names.contains(column))
            })
            .cloned()
            .collect::<Vec<_>>();
        merged.indexes = crate::executor::auto_indexes_for_table_meta(&merged);
        merged.indexes.extend(user_indexes);
        resolve_sync_rank_policies(table_name, &mut merged);
        merged
    }

    pub(super) fn sync_alter_vector_shape_changed(
        existing: &TableMeta,
        incoming: &TableMeta,
        constraints: &[String],
    ) -> bool {
        Self::sync_alter_is_full_shape(existing, incoming, constraints)
            && Self::sync_vector_columns(existing) != Self::sync_vector_columns(incoming)
    }

    pub(crate) fn apply_sync_alter_to_projected_table_meta(
        table_name: &str,
        meta: &mut TableMeta,
        incoming: TableMeta,
        constraints: &[String],
    ) {
        // Every emitted ALTER carries the complete declared table policy.
        // Keep the planning projection aligned with the real received ALTER
        // path so canonical no-op classification cannot erase a policy-only
        // change.
        if let Some(policy) =
            crate::executor::engine_owned_merged_policy(table_name, meta, &incoming)
        {
            meta.default_ttl_seconds = policy.default_ttl_seconds;
            meta.sync_safe = policy.sync_safe;
            meta.retain_declared_unit = policy.retain_declared_unit;
            meta.sync_direction = policy.sync_direction;
            meta.conflict_policy = policy.conflict_policy;
            meta.history_policy = policy.history_policy;
        } else {
            meta.default_ttl_seconds = incoming.default_ttl_seconds;
            meta.sync_safe = incoming.sync_safe;
            meta.retain_declared_unit = incoming.retain_declared_unit;
            meta.sync_direction = incoming.sync_direction;
            meta.conflict_policy = incoming.conflict_policy;
            meta.history_policy = incoming.history_policy;
            meta.maintained_counts = incoming.maintained_counts.clone();
        }
        if Self::sync_alter_vector_shape_changed(meta, &incoming, constraints) {
            *meta = Self::merged_sync_full_shape_vector_alter_meta(
                table_name,
                meta,
                incoming,
                constraints,
            );
            return;
        }

        // A received ALTER removes or renames a column only on its author's
        // own marker (`COLUMN_DROP(col)`, `COLUMN_RENAME(from,to)`), exactly
        // as the received replay does. A column the ALTER merely leaves out
        // -- with or without the primary key restated -- is kept with its
        // data; the projection never infers a drop from omission or a
        // rename from matching types.
        let incoming_column_names = Self::sync_meta_column_names(&incoming);
        for (from, to) in sync_column_renames_from_constraints(constraints) {
            let renames_held_column = meta.columns.iter().any(|column| {
                column.name == from && !matches!(column.column_type, ColumnType::Vector(_))
            }) && !incoming_column_names.contains(&from)
                && incoming_column_names.contains(&to)
                && meta.columns.iter().all(|column| column.name != to);
            if !renames_held_column {
                continue;
            }
            if let Some(column) = meta.columns.iter_mut().find(|column| column.name == from) {
                column.name = to.clone();
            }
            for index in &mut meta.indexes {
                for (column, _) in &mut index.columns {
                    if *column == from {
                        *column = to.clone();
                    }
                }
            }
            if meta.expires_column.as_deref() == Some(from.as_str()) {
                meta.expires_column = Some(to);
            }
        }
        let dropped_columns = sync_column_drops_from_constraints(constraints)
            .into_iter()
            .filter(|dropped| {
                !incoming_column_names.contains(dropped)
                    && meta.columns.iter().any(|column| {
                        column.name == *dropped
                            && !matches!(column.column_type, ColumnType::Vector(_))
                    })
            })
            .collect::<HashSet<_>>();

        let mut existing = Self::sync_meta_column_names(meta);
        for column in incoming.columns {
            if existing.insert(column.name.clone()) {
                meta.columns.push(column);
            } else if let Some(current) = meta
                .columns
                .iter_mut()
                .find(|current| current.name == column.name)
            {
                merge_sync_alter_existing_column(current, column);
            }
        }
        if !dropped_columns.is_empty() {
            meta.columns
                .retain(|column| !dropped_columns.contains(&column.name));
            if let Some(expires) = &meta.expires_column
                && dropped_columns.contains(expires)
            {
                meta.expires_column = None;
            }
        }
        if !incoming.unique_constraints.is_empty() {
            meta.unique_constraints = incoming.unique_constraints;
        }
        if !incoming.composite_foreign_keys.is_empty() {
            meta.composite_foreign_keys = incoming.composite_foreign_keys;
        }
        let user_indexes = meta
            .indexes
            .iter()
            .filter(|index| index.kind == IndexKind::UserDeclared)
            .cloned()
            .collect::<Vec<_>>();
        meta.indexes = crate::executor::auto_indexes_for_table_meta(meta);
        meta.indexes.extend(user_indexes);
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn projected_sync_ddl_unit_bytes(
        &self,
        ddl: &[DdlChange],
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<usize> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let changes = ChangeSet {
            ddl: ddl.to_vec(),
            ..ChangeSet::default()
        };
        let (_catalog_charge, mut projected) =
            self.selected_received_catalog(&changes, std::iter::empty())?;
        let mut running: i128 = 0;
        let mut peak: i128 = 0;

        for change in ddl {
            match change {
                DdlChange::CreateTable {
                    name,
                    columns,
                    constraints,
                    foreign_keys,
                    composite_foreign_keys,
                    composite_unique,
                } => {
                    if !projected.contains_key(name) {
                        let meta = rough_sync_table_meta(
                            columns,
                            constraints,
                            foreign_keys,
                            composite_foreign_keys,
                            composite_unique,
                        );
                        running +=
                            retained_table_metadata_bytes(&self.relational_store, &meta) as i128;
                        projected.insert(name.clone(), meta);
                    }
                }
                DdlChange::AlterTable {
                    name,
                    columns,
                    constraints,
                    foreign_keys,
                    composite_foreign_keys,
                    composite_unique,
                } => {
                    if let Some(meta) = projected.get_mut(name) {
                        let before = meta.clone();
                        let incoming = rough_sync_table_meta(
                            columns,
                            constraints,
                            foreign_keys,
                            composite_foreign_keys,
                            composite_unique,
                        );
                        Self::apply_sync_alter_to_projected_table_meta(
                            name,
                            meta,
                            incoming,
                            constraints,
                        );
                        let after = meta.clone();
                        // An ALTER refreshes the table's auto indexes, so the
                        // rows already in it are re-priced under the new shape
                        // exactly as a CREATE/DROP INDEX re-prices them.
                        running += self.projected_shape_change_bytes(name, &before, &after);
                    }
                }
                DdlChange::DropTable { name } => {
                    // A drop RELEASES: its metadata and every posting its rows
                    // held. Counted as the negative it is, so a unit that drops
                    // one table and builds an index on another is priced at
                    // what it holds at its widest point, not at the sum.
                    if projected.remove(name).is_some() {
                        running -= crate::executor::estimate_drop_table_bytes(
                            self,
                            name,
                            vector_copy_owner,
                            read_memory.clone(),
                        )? as i128;
                    }
                }
                DdlChange::CreateIndex {
                    table,
                    name,
                    columns,
                } => {
                    if let Some(meta) = projected.get_mut(table)
                        && !meta.indexes.iter().any(|index| index.name == *name)
                    {
                        let before = meta.clone();
                        meta.indexes.push(IndexDecl {
                            name: name.clone(),
                            columns: columns.clone(),
                            kind: IndexKind::UserDeclared,
                        });
                        let after = meta.clone();
                        running += self.projected_shape_change_bytes(table, &before, &after);
                    }
                }
                DdlChange::DropIndex { table, name } => {
                    if let Some(meta) = projected.get_mut(table)
                        && crate::executor::reserved_index_prefix(name).is_none()
                    {
                        let before = meta.clone();
                        meta.indexes.retain(|index| index.name != *name);
                        let after = meta.clone();
                        running += self.projected_shape_change_bytes(table, &before, &after);
                    }
                }
                DdlChange::CreateEventType { .. }
                | DdlChange::CreateTrigger { .. }
                | DdlChange::CreateTriggerIncludingSync { .. }
                | DdlChange::DropTrigger { .. }
                | DdlChange::CreateSink { .. }
                | DdlChange::CreateRoute { .. }
                | DdlChange::DropRoute { .. } => {}
            }
            // The item's own shape is installed, and its DURABLE half runs on
            // top of that -- so the allowance raises the peak HERE, after the
            // item's delta, and is held only while that item runs rather than
            // accumulating across the unit.
            peak = peak.max(running + RECEIVED_DDL_ITEM_DURABLE_ALLOWANCE_BYTES as i128);
        }

        Ok(usize::try_from(peak.max(0)).unwrap_or(usize::MAX))
    }

    /// What the WHOLE arriving DDL unit is priced at, for a proof that pins that
    /// price against what its items really cost.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn projected_sync_ddl_unit_bytes_for_test(&self, ddl: &[DdlChange]) -> usize {
        let read_memory = self
            .declared_reader_memory()
            .expect("schema projection read owner");
        let mut vector_copy_owner = crate::vector_read_memory::admission(read_memory.clone());
        self.projected_sync_ddl_unit_bytes(ddl, &mut vector_copy_owner, read_memory)
            .expect("schema projection admitted")
    }

    /// What re-shaping one table costs: its metadata delta plus one posting per
    /// row of it, for every row already committed and every row an open
    /// transaction has staged into it. This is the term the received unit's
    /// price used to be missing, and the reason a limit with room for two of
    /// three arriving indexes installed two of them and kept them.
    pub(super) fn projected_shape_change_bytes(
        &self,
        table: &str,
        old_meta: &TableMeta,
        new_meta: &TableMeta,
    ) -> i128 {
        let (staged_old, staged_new, _) =
            self.staged_rows_priced_across_shape_change(table, old_meta, new_meta);
        let old_bytes = retained_table_metadata_bytes(&self.relational_store, old_meta)
            .saturating_add(self.committed_rows_priced_under(table, old_meta))
            .saturating_add(staged_old) as i128;
        let new_bytes = retained_table_metadata_bytes(&self.relational_store, new_meta)
            .saturating_add(self.committed_rows_priced_under(table, new_meta))
            .saturating_add(staged_new) as i128;
        // The postings are only part of what an index costs. Building one also
        // has the relational store admit its tree's owned keys and its current
        // membership image -- bytes taken OUTSIDE the row term above, which is
        // why a price built from the row term alone let a unit of three indexes
        // through a limit with room for two. What one index on THIS table with
        // THESE rows costs is not guessed: it is measured from the indexes the
        // table already has.
        //
        // The exact-probe bitmap is NOT in that measurement and must not be.
        // The resident backend metadata delta already carries its filter;
        // source-backed descriptors allocate no filter. Adding an
        // average that contained it charged an arriving user-declared index a
        // bitmap it never allocates, and an arriving exact-only one the same
        // bitmap twice. Either way a unit the limit could hold was refused and
        // parked in a durable hold.
        let index_structures = {
            let (bytes, count) = self.relational_store.table_index_structure_bytes(table);
            let per_index = bytes.checked_div(count).unwrap_or(0) as i128;
            let old_indexes = old_meta.indexes.len() as i128;
            let new_indexes = new_meta.indexes.len() as i128;
            (new_indexes - old_indexes) * per_index
        };
        new_bytes - old_bytes + index_structures
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn preflight_sync_ddl_mixed_apply(
        &self,
        changes: &ChangeSet,
        policies: &ConflictPolicies,
        arrivals: &HashMap<Lsn, Option<Lsn>>,
        adoption: SyncAdoption,
        fresh_creator_deletes: &[AuthenticatedFreshCreatorDelete],

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if changes.ddl.is_empty() {
            return Ok(());
        }

        let side_metadata_required = Self::received_side_metadata_required(changes);
        let _side_metadata_charge = self.admit_received_side_metadata(changes)?;
        let callback_required_triggers = if side_metadata_required {
            self.preflight_sync_trigger_callback_required(changes)?
        } else {
            HashSet::new()
        };
        let mut ddl_prefix = Vec::new();
        let mut event_bus_ddl_prefix = Vec::new();
        let mut incoming_fk_values = HashMap::<String, Vec<HashMap<String, Value>>>::new();
        let mut deleted_committed_fk_row_ids = HashMap::<String, HashSet<RowId>>::new();
        let (_catalog_charge, mut projected_table_meta) =
            self.selected_received_catalog(changes, std::iter::empty())?;
        let mut projected_trigger_declarations = if side_metadata_required {
            self.trigger.declarations.lock().clone()
        } else {
            BTreeMap::new()
        };
        let snapshot = self.snapshot();
        for group in changes.clone().split_by_data_lsn() {
            self.preflight_sync_trigger_data_gate(
                &group.rows,
                &group.edges,
                &group.vectors,
                &projected_trigger_declarations,
                &callback_required_triggers,
            )?;
            ddl_prefix.extend(group.ddl.iter().cloned());
            event_bus_ddl_prefix.extend(group.ddl.iter().filter_map(|ddl| match ddl {
                DdlChange::CreateEventType { .. }
                | DdlChange::CreateSink { .. }
                | DdlChange::CreateRoute { .. }
                | DdlChange::DropRoute { .. } => Some(ddl.clone()),
                _ => None,
            }));
            for ddl in &group.ddl {
                Self::validate_sync_table_ddl_against_projected_meta(&projected_table_meta, ddl)?;
                Self::apply_sync_table_ddl_to_projected_meta(&mut projected_table_meta, ddl);
                Self::validate_projected_foreign_key_schema(&projected_table_meta)?;
                self.apply_sync_trigger_ddl_to_projection(
                    &mut projected_trigger_declarations,
                    ddl,
                    &projected_table_meta,
                )?;
            }

            self.preflight_sync_rows_against_projected_schema(
                &group.rows,
                &projected_table_meta,
                read_memory.clone(),
            )?;
            self.preflight_sync_vectors_against_projected_schema(
                &group.vectors,
                &projected_table_meta,
            )?;
            self.preflight_sync_foreign_keys_against_lsn_prefix(
                &group.rows,
                &projected_table_meta,
                snapshot,
                &mut incoming_fk_values,
                &mut deleted_committed_fk_row_ids,
                policies,
                arrivals,
                adoption,
                fresh_creator_deletes,
                read_memory.clone(),
            )?;

            self.preflight_sync_trigger_data_gate(
                &group.rows,
                &group.edges,
                &group.vectors,
                &projected_trigger_declarations,
                &callback_required_triggers,
            )?;

            let dropped_tables = Self::sync_dropped_tables(&ddl_prefix);
            let projected_event_bus_ddl = event_bus_ddl_prefix
                .iter()
                .filter(|ddl| {
                    !Self::skip_event_bus_ddl_for_dropped_table(ddl, &dropped_tables, |table| {
                        projected_table_meta.contains_key(table)
                    })
                })
                .cloned()
                .collect::<Vec<_>>();
            self.validate_event_bus_ddl_batch_with_table_lookup(
                &projected_event_bus_ddl,
                |table| projected_table_meta.contains_key(table),
            )?;
        }

        let projected_dag_edge_types = self.projected_sync_dag_edge_types(&changes.ddl);
        self.preflight_sync_edge_cycles(changes, &projected_dag_edge_types, read_memory.clone())?;

        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn preflight_sync_trigger_callback_required(
        &self,
        changes: &ChangeSet,
    ) -> Result<HashSet<String>> {
        let (_charge, projected_trigger_declarations) =
            self.preflight_sync_trigger_projection(changes)?;
        Ok(self.sync_triggers_requiring_ready_gate(&projected_trigger_declarations))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn preflight_sync_trigger_projection(
        &self,
        changes: &ChangeSet,
    ) -> Result<(StagedCopyCharge, BTreeMap<String, TriggerDeclaration>)> {
        let side_metadata_charge = self.admit_received_side_metadata(changes)?;
        let (_catalog_charge, mut projected_table_meta) =
            self.selected_received_catalog(changes, std::iter::empty())?;
        let mut projected_trigger_declarations = self.trigger.declarations.lock().clone();
        for group in changes.clone().split_by_data_lsn() {
            for ddl in &group.ddl {
                Self::validate_sync_table_ddl_against_projected_meta(&projected_table_meta, ddl)?;
                Self::apply_sync_table_ddl_to_projected_meta(&mut projected_table_meta, ddl);
                Self::validate_projected_foreign_key_schema(&projected_table_meta)?;
                self.apply_sync_trigger_ddl_to_projection(
                    &mut projected_trigger_declarations,
                    ddl,
                    &projected_table_meta,
                )?;
            }
        }
        Ok((side_metadata_charge, projected_trigger_declarations))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn validate_projected_foreign_key_schema(
        projected: &HashMap<String, TableMeta>,
    ) -> Result<()> {
        for (table, meta) in projected {
            crate::executor::validate_exact_constraint_keys_for_meta(table, meta)?;
            crate::executor::validate_single_column_foreign_keys_for_meta(table, meta, |parent| {
                projected.get(parent).cloned()
            })?;
            crate::executor::validate_composite_foreign_keys_for_meta(table, meta, |parent| {
                projected.get(parent).cloned()
            })?;
        }
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn replace_table_meta_and_refresh_auto_indexes(
        &self,
        table: &str,
        old_meta: &TableMeta,
        mut new_meta: TableMeta,
    ) -> Result<TableMeta> {
        let old_auto_names = old_meta
            .indexes
            .iter()
            .filter(|index| index.kind == IndexKind::Auto)
            .map(|index| index.name.clone())
            .collect::<HashSet<_>>();
        let old_auto_columns = old_meta
            .indexes
            .iter()
            .filter(|index| index.kind == IndexKind::Auto)
            .map(|index| (index.name.clone(), index.columns.clone()))
            .collect::<HashMap<_, _>>();
        let auto_indexes = crate::executor::auto_indexes_for_table_meta(&new_meta);
        let new_auto_names = auto_indexes
            .iter()
            .map(|index| index.name.clone())
            .collect::<HashSet<_>>();
        let changed_auto_names = auto_indexes
            .iter()
            .filter(|index| {
                old_auto_columns
                    .get(&index.name)
                    .is_some_and(|old_columns| old_columns != &index.columns)
            })
            .map(|index| index.name.clone())
            .collect::<HashSet<_>>();

        new_meta = self.project_table_meta_with_auto_indexes(new_meta);
        let mut graph_copies = contextdb_vector::VectorCopyAdmission::new(
            contextdb_vector::MemoryBudgetOwner::clone(&self.finalization.vector_accountant),
        );
        let graph_shape_loss = self.prepare_graph_shape_loss(
            table,
            old_meta,
            &new_meta,
            &mut graph_copies,
            self.declared_reader_memory()?,
        )?;
        // The automatic indexes this refresh adds or removes change what every
        // row already in the table costs, so the difference is settled here,
        // before any of it happens.
        let mut index_set_memory =
            self.prepare_index_set_change_memory(table, old_meta, &new_meta)?;
        {
            let store = self.relational_store();
            let mut metas = store.table_meta.write();
            if !metas.contains_key(table) {
                return Err(Error::TableNotFound(table.to_string()));
            }
            metas.insert(table.to_string(), new_meta.clone());
        }

        let store = self.relational_store();
        for old_name in old_auto_names.difference(&new_auto_names) {
            store.drop_index_storage(table, old_name);
        }
        for changed_name in &changed_auto_names {
            store.drop_index_storage(table, changed_name);
        }
        for index in auto_indexes {
            store.create_exact_index_storage(table, &index.name, index.columns.clone());
            store.rebuild_index(table, &index.name);
        }
        self.publish_graph_shape_loss(graph_shape_loss);
        index_set_memory.commit_after_swap();

        Ok(new_meta)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn validate_sync_table_ddl_against_projected_meta(
        projected: &HashMap<String, TableMeta>,
        change: &DdlChange,
    ) -> Result<()> {
        // Judge explicit author markers against this exact source prefix,
        // before parsing/copying the incoming shape or applying any mutation.
        refuse_dag_defining_column_sync_ddl(projected, change)?;
        // A delivery promise with nowhere to deliver is illegal EVERYWHERE,
        // not only where a local operator types it: a peer that spells it in
        // arriving DDL is refused here, before any of the statement is
        // applied, exactly as the local CREATE and ALTER paths refuse it.
        refuse_undeliverable_promise_sync_ddl(projected, change)?;
        refuse_keyless_sync_safe_sync_ddl(projected, change)?;
        refuse_engine_owned_policy_sync_ddl(projected, change)?;
        refuse_reclaimed_history_sync_ddl(change)?;
        match change {
            DdlChange::CreateTable {
                name,
                columns,
                constraints,
                foreign_keys,
                composite_foreign_keys,
                composite_unique,
            } => Self::validate_sync_table_shape_ddl(
                projected,
                name,
                columns,
                constraints,
                foreign_keys,
                composite_foreign_keys,
                composite_unique,
            ),
            DdlChange::AlterTable {
                name,
                columns,
                constraints,
                foreign_keys,
                composite_foreign_keys,
                composite_unique,
            } => Self::validate_sync_alter_table_shape_ddl(
                projected,
                name,
                columns,
                constraints,
                foreign_keys,
                composite_foreign_keys,
                composite_unique,
            ),
            DdlChange::CreateIndex {
                table,
                name,
                columns,
            } => {
                let Some(meta) = projected.get(table) else {
                    return Err(Error::TableNotFound(table.clone()));
                };
                if let Some(prefix) = crate::executor::reserved_index_prefix(name) {
                    return Err(Error::ReservedIndexName {
                        table: table.clone(),
                        name: name.clone(),
                        prefix: prefix.to_string(),
                    });
                }
                for (column, _) in columns {
                    if !meta
                        .columns
                        .iter()
                        .any(|candidate| candidate.name == *column)
                    {
                        return Err(Error::ColumnNotFound {
                            table: table.clone(),
                            column: column.clone(),
                        });
                    }
                }
                for (column, _) in columns {
                    let column_meta = meta
                        .columns
                        .iter()
                        .find(|candidate| candidate.name == *column)
                        .expect("column existence verified above");
                    if !crate::executor::btree_indexable(&column_meta.column_type) {
                        return Err(Error::ColumnNotIndexable {
                            table: table.clone(),
                            column: column.clone(),
                            column_type: column_meta.column_type.clone(),
                        });
                    }
                }
                Ok(())
            }
            DdlChange::DropIndex { table, name } => {
                if projected
                    .get(table)
                    .is_some_and(|meta| meta.indexes.iter().any(|index| index.name == *name))
                    && let Some(prefix) = crate::executor::reserved_index_prefix(name)
                {
                    return Err(Error::ReservedIndexName {
                        table: table.clone(),
                        name: name.clone(),
                        prefix: prefix.to_string(),
                    });
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn validate_sync_alter_table_shape_ddl(
        projected: &HashMap<String, TableMeta>,
        name: &str,
        columns: &[(String, String)],
        constraints: &[String],
        foreign_keys: &[SingleColumnForeignKey],
        composite_foreign_keys: &[CompositeForeignKey],
        composite_unique: &[Vec<String>],
    ) -> Result<()> {
        let sql = sync_create_table_sql(
            name,
            columns,
            constraints,
            foreign_keys,
            composite_foreign_keys,
            composite_unique,
        );
        let stmt = contextdb_parser::parse(&sql)?;
        let _ = contextdb_planner::plan(&stmt)?;
        if let Statement::CreateTable(create) = &stmt {
            crate::executor::validate_create_table_vector_declarations(name, create)?;
        }

        if let Some(current) = projected.get(name) {
            for (from, to) in sync_column_renames_from_constraints(constraints) {
                let has_source = current.columns.iter().any(|column| column.name == from);
                let has_target = current.columns.iter().any(|column| column.name == to);
                if !has_source || has_target {
                    return Err(Error::SchemaInvalid {
                        reason: format!(
                            "received rename of {name}.{from} to {to} requires the source column and an unused target name"
                        ),
                    });
                }
            }
        }

        let incoming = rough_sync_table_meta(
            columns,
            constraints,
            foreign_keys,
            composite_foreign_keys,
            composite_unique,
        );
        let mut candidate = projected.get(name).cloned().unwrap_or_default();
        Self::apply_sync_alter_to_projected_table_meta(name, &mut candidate, incoming, constraints);
        crate::executor::validate_vector_partition_declarations(name, &candidate)?;
        let candidate_lookup = candidate.clone();
        crate::executor::validate_exact_constraint_keys_for_meta(name, &candidate)?;
        crate::executor::validate_single_column_foreign_keys_for_meta(
            name,
            &candidate,
            |parent| {
                if parent == name {
                    Some(candidate_lookup.clone())
                } else {
                    projected.get(parent).cloned()
                }
            },
        )?;
        crate::executor::validate_composite_foreign_keys_for_meta(name, &candidate, |parent| {
            if parent == name {
                Some(candidate_lookup.clone())
            } else {
                projected.get(parent).cloned()
            }
        })?;
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn validate_sync_table_shape_ddl(
        projected: &HashMap<String, TableMeta>,
        name: &str,
        columns: &[(String, String)],
        constraints: &[String],
        foreign_keys: &[SingleColumnForeignKey],
        composite_foreign_keys: &[CompositeForeignKey],
        composite_unique: &[Vec<String>],
    ) -> Result<()> {
        let sql = sync_create_table_sql(
            name,
            columns,
            constraints,
            foreign_keys,
            composite_foreign_keys,
            composite_unique,
        );
        let stmt = contextdb_parser::parse(&sql)?;
        let _ = contextdb_planner::plan(&stmt)?;
        if let Statement::CreateTable(create) = &stmt {
            crate::executor::validate_create_table_vector_declarations(name, create)?;
        }
        let candidate = rough_sync_table_meta(
            columns,
            constraints,
            foreign_keys,
            composite_foreign_keys,
            composite_unique,
        );
        crate::executor::validate_vector_partition_declarations(name, &candidate)?;
        let candidate_lookup = candidate.clone();
        crate::executor::validate_exact_constraint_keys_for_meta(name, &candidate)?;
        crate::executor::validate_single_column_foreign_keys_for_meta(
            name,
            &candidate,
            |parent| {
                if parent == name {
                    Some(candidate_lookup.clone())
                } else {
                    projected.get(parent).cloned()
                }
            },
        )?;
        crate::executor::validate_composite_foreign_keys_for_meta(name, &candidate, |parent| {
            if parent == name {
                Some(candidate_lookup.clone())
            } else {
                projected.get(parent).cloned()
            }
        })?;
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn preflight_sync_rows_against_projected_schema(
        &self,
        rows: &[RowChange],
        projected_table_meta: &HashMap<String, TableMeta>,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        for row in rows {
            if row.values.is_empty() {
                continue;
            }
            let table_meta = projected_table_meta
                .get(&row.table)
                .ok_or_else(|| Error::TableNotFound(row.table.clone()))?;
            if !row.deleted
                && !self.sync_incoming_values_allowed_for_access(
                    &row.table,
                    table_meta,
                    &row.values,
                    row.lsn,
                    read_memory.clone(),
                )?
            {
                continue;
            }
            let expected_natural_key = natural_key_columns_for_meta(table_meta)
                .ok_or_else(|| Error::NotSyncEligible(row.table.clone()))?;
            let arriving_key = row.natural_key.key_columns();
            if arriving_key != expected_natural_key {
                if let Some(unknown) = arriving_key
                    .iter()
                    .find(|column| !table_meta.columns.iter().any(|c| c.name == **column))
                {
                    return Err(Error::ColumnNotFound {
                        table: row.table.clone(),
                        column: unknown.clone(),
                    });
                }
                return Err(Error::SyncError(
                    (format!(
                        "sync row natural key column mismatch for {}: got {}, expected {}",
                        row.table,
                        arriving_key.join(", "),
                        expected_natural_key.join(", ")
                    ))
                    .into(),
                ));
            }
            if !row.deleted {
                if !row.natural_key.matches_values(&row.values) {
                    return Err(Error::SyncError(
                        (format!(
                            "sync row natural key value mismatch for {}.{}",
                            row.table,
                            expected_natural_key.join(", ")
                        ))
                        .into(),
                    ));
                }
                for (column, value) in &row.values {
                    let is_vector_bypass = matches!(value, Value::Vector(_))
                        && table_meta
                            .columns
                            .iter()
                            .find(|candidate| candidate.name == *column)
                            .map(|candidate| matches!(candidate.column_type, ColumnType::Vector(_)))
                            .unwrap_or(false);
                    if !is_vector_bypass {
                        crate::executor::coerce_into_column_with_meta(
                            &row.table,
                            table_meta,
                            column,
                            value.clone(),
                            None,
                            None,
                        )?;
                    }
                }
                for column in &table_meta.columns {
                    if column.nullable || column.default.is_some() {
                        continue;
                    }
                    match row.values.get(&column.name) {
                        None | Some(Value::Null) => {
                            return Err(Error::ColumnNotNullable {
                                table: row.table.clone(),
                                column: column.name.clone(),
                            });
                        }
                        _ => {}
                    }
                }
            }
        }
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn preflight_sync_vectors_against_projected_schema(
        &self,
        vectors: &[VectorChange],
        projected_table_meta: &HashMap<String, TableMeta>,
    ) -> Result<()> {
        for vector in vectors {
            let Some(table_meta) = projected_table_meta.get(&vector.index.table) else {
                return Err(Error::UnknownVectorIndex {
                    index: vector.index.clone(),
                });
            };
            let Some(column) = table_meta
                .columns
                .iter()
                .find(|column| column.name == vector.index.column)
            else {
                return Err(Error::UnknownVectorIndex {
                    index: vector.index.clone(),
                });
            };
            let ColumnType::Vector(expected) = column.column_type else {
                return Err(Error::UnknownVectorIndex {
                    index: vector.index.clone(),
                });
            };
            if !vector.vector.is_empty() && vector.vector.len() != expected {
                return Err(Error::VectorIndexDimensionMismatch {
                    index: vector.index.clone(),
                    expected,
                    actual: vector.vector.len(),
                });
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn preflight_sync_foreign_keys_against_lsn_prefix(
        &self,
        rows: &[RowChange],
        projected_table_meta: &HashMap<String, TableMeta>,
        snapshot: SnapshotId,
        incoming_values: &mut HashMap<String, Vec<HashMap<String, Value>>>,
        deleted_committed_row_ids: &mut HashMap<String, HashSet<RowId>>,
        policies: &ConflictPolicies,
        arrivals: &HashMap<Lsn, Option<Lsn>>,
        adoption: SyncAdoption,
        fresh_creator_deletes: &[AuthenticatedFreshCreatorDelete],

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if rows.is_empty() {
            return Ok(());
        }
        let mut deleted_projected_rows = Vec::<(String, HashMap<String, Value>)>::new();
        let mut current_deleted_committed_row_ids = Vec::<(String, RowId)>::new();
        let mut applying_rows = Vec::<&RowChange>::new();
        for row in rows.iter().filter(|row| row.deleted) {
            let policy = Self::sync_conflict_policy_for_table(
                policies,
                projected_table_meta.get(&row.table),
                &row.table,
            );
            let empty_deleted = HashSet::new();
            let skip_deleted = deleted_committed_row_ids
                .get(&row.table)
                .unwrap_or(&empty_deleted);
            if self.table_meta(&row.table).is_some()
                && let Some(committed) = self.visible_row_by_natural_key(
                    &row.table,
                    &row.natural_key,
                    snapshot,
                    skip_deleted,
                    read_memory.clone(),
                )?
            {
                if Self::authenticated_fresh_creator_delete_permission(
                    row,
                    &committed,
                    policy,
                    fresh_creator_deletes,
                )
                .is_none()
                    && !self.sync_row_applies_over_committed(
                        row,
                        &committed,
                        policy,
                        Self::resolve_incoming_arrival(row, arrivals),
                        adoption,
                        read_memory.clone(),
                    )?
                {
                    continue;
                }
                current_deleted_committed_row_ids.push((row.table.clone(), committed.row_id));
                deleted_projected_rows.push((row.table.clone(), committed.values));
                continue;
            }
            if let Some(incoming) = incoming_values.get(&row.table).and_then(|table_values| {
                table_values
                    .iter()
                    .find(|values| row.natural_key.matches_values(values))
            }) {
                deleted_projected_rows.push((row.table.clone(), incoming.clone()));
            }
        }

        let mut post_incoming_values = incoming_values.clone();
        for row in rows.iter().filter(|row| row.deleted) {
            let empty_deleted = HashSet::new();
            let skip_deleted = deleted_committed_row_ids
                .get(&row.table)
                .unwrap_or(&empty_deleted);
            let deletes_committed = self.table_meta(&row.table).is_some()
                && self
                    .visible_row_by_natural_key(
                        &row.table,
                        &row.natural_key,
                        snapshot,
                        skip_deleted,
                        read_memory.clone(),
                    )?
                    .is_some();
            if deletes_committed {
                let policy = Self::sync_conflict_policy_for_table(
                    policies,
                    projected_table_meta.get(&row.table),
                    &row.table,
                );
                if let Some(committed) = self.visible_row_by_natural_key(
                    &row.table,
                    &row.natural_key,
                    snapshot,
                    skip_deleted,
                    read_memory.clone(),
                )? && Self::authenticated_fresh_creator_delete_permission(
                    row,
                    &committed,
                    policy,
                    fresh_creator_deletes,
                )
                .is_none()
                    && !self.sync_row_applies_over_committed(
                        row,
                        &committed,
                        policy,
                        Self::resolve_incoming_arrival(row, arrivals),
                        adoption,
                        read_memory.clone(),
                    )?
                {
                    continue;
                }
            }
            if let Some(table_values) = post_incoming_values.get_mut(&row.table) {
                table_values.retain(|values| !row.natural_key.matches_values(values));
            }
        }
        let mut post_deleted_committed_row_ids = deleted_committed_row_ids.clone();
        for (table, row_id) in current_deleted_committed_row_ids {
            post_deleted_committed_row_ids
                .entry(table)
                .or_default()
                .insert(row_id);
        }
        for row in rows {
            if row.deleted || row.values.is_empty() {
                continue;
            }
            if let Some(table_meta) = projected_table_meta.get(&row.table)
                && !self.sync_incoming_values_allowed_for_access(
                    &row.table,
                    table_meta,
                    &row.values,
                    row.lsn,
                    read_memory.clone(),
                )?
            {
                continue;
            }
            let policy = Self::sync_conflict_policy_for_table(
                policies,
                projected_table_meta.get(&row.table),
                &row.table,
            );
            let mut replaces_incoming_prefix = false;
            if let Some(table_values) = post_incoming_values.get_mut(&row.table) {
                let mut retained = Vec::with_capacity(table_values.len());
                for values in table_values.drain(..) {
                    if row.natural_key.matches_values(&values) {
                        replaces_incoming_prefix = true;
                        deleted_projected_rows.push((row.table.clone(), values));
                    } else {
                        retained.push(values);
                    }
                }
                *table_values = retained;
            }
            let empty_deleted = HashSet::new();
            let skip_deleted = post_deleted_committed_row_ids
                .get(&row.table)
                .unwrap_or(&empty_deleted);
            if self.table_meta(&row.table).is_some()
                && let Some(committed) = self.visible_row_by_natural_key(
                    &row.table,
                    &row.natural_key,
                    snapshot,
                    skip_deleted,
                    read_memory.clone(),
                )?
            {
                if !replaces_incoming_prefix
                    && !self.sync_row_applies_over_committed(
                        row,
                        &committed,
                        policy,
                        Some(row.lsn),
                        adoption,
                        read_memory.clone(),
                    )?
                {
                    continue;
                }
                post_deleted_committed_row_ids
                    .entry(row.table.clone())
                    .or_default()
                    .insert(committed.row_id);
                deleted_projected_rows.push((row.table.clone(), committed.values));
            }
            post_incoming_values
                .entry(row.table.clone())
                .or_default()
                .push(row.values.clone());
            applying_rows.push(row);
        }

        for row in applying_rows {
            let table_meta = projected_table_meta
                .get(&row.table)
                .ok_or_else(|| Error::TableNotFound(row.table.clone()))?;
            for column in &table_meta.columns {
                let Some(reference) = &column.references else {
                    continue;
                };
                let Some(value) = row.values.get(&column.name) else {
                    continue;
                };
                if *value == Value::Null {
                    continue;
                }
                if !projected_table_meta.contains_key(&reference.table) {
                    return Err(Error::ForeignKeyViolation {
                        child_table: row.table.clone(),
                        child_columns: vec![column.name.clone()],
                        parent_table: reference.table.clone(),
                        parent_columns: vec![reference.column.clone()],
                    });
                }
                let incoming_match =
                    post_incoming_values
                        .get(&reference.table)
                        .is_some_and(|rows| {
                            rows.iter()
                                .any(|values| values.get(&reference.column) == Some(value))
                        });
                if incoming_match {
                    continue;
                }
                let empty_deleted = HashSet::new();
                let deleted_parent_row_ids = post_deleted_committed_row_ids
                    .get(&reference.table)
                    .unwrap_or(&empty_deleted);
                let committed_match = if self.table_meta(&reference.table).is_some() {
                    self.required_indexed_visible_row_by_column(
                        &reference.table,
                        &reference.column,
                        value,
                        snapshot,
                        deleted_parent_row_ids,
                        read_memory.clone(),
                    )?
                    .is_some()
                } else {
                    false
                };
                if !committed_match {
                    return Err(Error::ForeignKeyViolation {
                        child_table: row.table.clone(),
                        child_columns: vec![column.name.clone()],
                        parent_table: reference.table.clone(),
                        parent_columns: vec![reference.column.clone()],
                    });
                }
            }
        }
        let mut reverse_refs = Vec::new();
        for (child_table, meta) in projected_table_meta {
            let current_child_meta = self.table_meta(child_table);
            for column in &meta.columns {
                let Some(reference) = &column.references else {
                    continue;
                };
                let current_child_column = current_child_meta.as_ref().and_then(|current| {
                    current
                        .columns
                        .iter()
                        .find(|current_column| current_column.name == column.name)
                });
                let current_relationship_exists = current_child_column
                    .and_then(|current_column| current_column.references.as_ref())
                    == Some(reference);
                reverse_refs.push((
                    reference.table.clone(),
                    reference.column.clone(),
                    child_table.clone(),
                    column.name.clone(),
                    current_child_column.is_some(),
                    current_relationship_exists,
                ));
            }
        }

        for (parent_table, parent_values) in &deleted_projected_rows {
            for (
                ref_table,
                ref_column,
                child_table,
                child_column,
                current_child_column_exists,
                current_relationship_exists,
            ) in &reverse_refs
            {
                if ref_table != parent_table {
                    continue;
                }
                let Some(parent_value) = parent_values.get(ref_column) else {
                    continue;
                };
                if *parent_value == Value::Null {
                    continue;
                }
                let parent_replaced_with_same_key = post_incoming_values
                    .get(parent_table)
                    .is_some_and(|table_values| {
                        table_values
                            .iter()
                            .any(|values| values.get(ref_column) == Some(parent_value))
                    });
                if parent_replaced_with_same_key {
                    continue;
                }
                let incoming_child_match =
                    post_incoming_values
                        .get(child_table)
                        .is_some_and(|table_values| {
                            table_values
                                .iter()
                                .any(|values| values.get(child_column) == Some(parent_value))
                        });
                if incoming_child_match {
                    return Err(Error::ForeignKeyViolation {
                        child_table: child_table.clone(),
                        child_columns: vec![child_column.clone()],
                        parent_table: parent_table.clone(),
                        parent_columns: vec![ref_column.clone()],
                    });
                }
                let empty_deleted = HashSet::new();
                let deleted_child_row_ids = post_deleted_committed_row_ids
                    .get(child_table)
                    .unwrap_or(&empty_deleted);
                if !current_relationship_exists && !current_child_column_exists {
                    continue;
                }
                if self
                    .required_indexed_visible_row_by_column(
                        child_table,
                        child_column,
                        parent_value,
                        snapshot,
                        deleted_child_row_ids,
                        read_memory.clone(),
                    )?
                    .is_some()
                {
                    return Err(Error::ForeignKeyViolation {
                        child_table: child_table.clone(),
                        child_columns: vec![child_column.clone()],
                        parent_table: parent_table.clone(),
                        parent_columns: vec![ref_column.clone()],
                    });
                }
            }
        }

        *incoming_values = post_incoming_values;
        *deleted_committed_row_ids = post_deleted_committed_row_ids;
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn sync_composite_fk_violation_for_values(
        &self,
        tx: TxId,
        child_table: &str,
        child_meta: &TableMeta,
        values: &HashMap<String, Value>,
        incoming_batch_values: &HashMap<String, Vec<HashMap<String, Value>>>,
        projected_deleted_committed_row_ids: &HashMap<String, HashSet<RowId>>,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<Error>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        for fk in &child_meta.composite_foreign_keys {
            let mut tuple = Vec::with_capacity(fk.child_columns.len());
            let mut has_null = false;
            for column in &fk.child_columns {
                match values.get(column) {
                    Some(Value::Null) | None => {
                        has_null = true;
                        break;
                    }
                    Some(value) => tuple.push(value.clone()),
                }
            }
            if has_null {
                continue;
            }

            let incoming_match =
                incoming_batch_values
                    .get(&fk.parent_table)
                    .is_some_and(|table_values| {
                        table_values.iter().any(|row_values| {
                            fk.parent_columns
                                .iter()
                                .zip(tuple.iter())
                                .all(|(column, value)| row_values.get(column) == Some(value))
                        })
                    });
            if incoming_match {
                continue;
            }

            let staged_match = self.tx_mgr.with_ordinary_read(tx, |ws| {
                staged_tuple_exists(ws, &fk.parent_table, &fk.parent_columns, &tuple)
            })?;
            if staged_match {
                continue;
            }

            let mut parent_deletes = projected_deleted_committed_row_ids
                .get(&fk.parent_table)
                .cloned()
                .unwrap_or_default();
            let staged_parent_deletes = self.tx_mgr.with_ordinary_read(tx, |ws| {
                Self::deleted_row_ids_for_table(ws, &fk.parent_table)
            })?;
            parent_deletes.extend(staged_parent_deletes);
            let committed_match = self.indexed_visible_row_exists_by_columns(
                &fk.parent_table,
                &fk.parent_columns,
                &tuple,
                self.snapshot(),
                &parent_deletes,
                None,
                read_memory.clone(),
            )?;
            if !committed_match {
                return Ok(Some(Error::ForeignKeyViolation {
                    child_table: child_table.to_string(),
                    child_columns: fk.child_columns.clone(),
                    parent_table: fk.parent_table.clone(),
                    parent_columns: fk.parent_columns.clone(),
                }));
            }
        }

        Ok(None)
    }

    /// The private policy input for a sync apply. Ordinary tables always resolve
    /// from their durable declaration (or the product-ratified keep-first
    /// default). The only entries here are ContextDB-owned work-ledger and
    /// peer-directory role mechanics; no application can construct or pass this
    /// map. A hub applies those system invariants as receiver-side rules, while
    /// an edge reverses them when adopting the hub's settled row.
    pub(super) fn declared_sync_policies(receiving_hub_push: bool) -> ConflictPolicies {
        let mut policies = ConflictPolicies::uniform(ConflictPolicy::InsertIfNotExists);
        crate::work_ledger::apply_work_ledger_policy_overrides(&mut policies);
        crate::peer_directory::apply_peer_directory_policy_overrides(&mut policies);
        if !receiving_hub_push {
            for policy in policies.per_table.values_mut() {
                *policy = match *policy {
                    ConflictPolicy::ServerWins => ConflictPolicy::EdgeWins,
                    ConflictPolicy::EdgeWins => ConflictPolicy::ServerWins,
                    declared => declared,
                };
            }
        }
        policies
    }

    /// The conflict policy that resolves an arriving row for `table`. An
    /// ordinary table's durable `TableMeta` is the only authority; the private
    /// system map above exists solely for ContextDB-authored fabric tables.
    pub(super) fn sync_conflict_policy_for_table(
        policies: &ConflictPolicies,
        meta: Option<&TableMeta>,
        table: &str,
    ) -> ConflictPolicy {
        if let Some(policy) = policies.per_table.get(table) {
            return *policy;
        }
        if let Some(policy) = meta.and_then(|meta| meta.conflict_policy) {
            return if policy == contextdb_core::ConflictPolicy::KEEP_LATEST {
                ConflictPolicy::LatestWins
            } else {
                ConflictPolicy::InsertIfNotExists
            };
        }
        policies.default
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn sync_incoming_values_allowed_for_access(
        &self,
        table: &str,
        meta: &TableMeta,
        values: &HashMap<String, Value>,
        lsn: Lsn,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<bool> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if self.access_is_admin() {
            return Ok(true);
        }
        let row = VersionedRow {
            row_id: RowId(0),
            values: values.clone(),
            created_tx: TxId(0),
            deleted_tx: None,
            lsn,
            created_at: None,
        };
        self.read_allowed_for_row(table, meta, &row, self.snapshot(), read_memory.clone())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn sync_row_applies_over_committed(
        &self,
        row: &RowChange,
        committed: &VersionedRow,
        policy: ConflictPolicy,
        incoming_arrival: Option<Lsn>,
        adoption: SyncAdoption,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<bool> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        Ok(match policy {
            ConflictPolicy::InsertIfNotExists | ConflictPolicy::ServerWins => false,
            ConflictPolicy::EdgeWins => true,
            ConflictPolicy::LatestWins => {
                let committed_source = self
                    .relational_sync_source_at_read_root(
                        &row.table,
                        committed.row_id,
                        read_memory.clone(),
                    )?
                    .0;
                self.sync_latest_wins_incoming(
                    row,
                    committed,
                    committed_source,
                    incoming_arrival,
                    adoption,
                )
            }
        })
    }

    /// LatestWins per-row comparison. `committed_source` is the row's stored
    /// sync provenance LSN (the sidecar), resolved once by the caller so the
    /// hot apply path performs a single sidecar probe per row.
    ///
    /// `incoming_arrival` is the incoming row's own ordering position — the
    /// value some earlier ACCEPTING node stamped for it, never the sender's
    /// own unrelated local clock (see `resolve_incoming_arrival`). `None`
    /// means the incoming row carries no established fleet-lineage ordering
    /// at all (freshly authored at the sender, never before synced anywhere),
    /// in which case THIS node is the one ordering it: it always wins,
    /// unconditionally, over whatever is already held — comparing it against
    /// `committed_lsn` would compare two machines' unrelated clocks, which is
    /// the defect this rule exists to close. When an arrival IS carried, it is
    /// compared against the stored position exactly as before: equal-or-lower
    /// is a stale echo (the caller treats that as a silent no-op, never a
    /// conflict), strictly higher wins — UNLESS `adoption` is
    /// `ReadoptingSource` and `committed_source` is established: the stored
    /// position was minted by a source this store just stopped addressing
    /// (see [`SyncAdoption`]), so it is incomparable to the incoming arrival
    /// from the newly adopted one, and the served value wins unconditionally.
    /// `committed_source` being absent means the current committed value is
    /// either purely local or has been locally edited since its last sync
    /// (a local write always clears the sidecar — see
    /// `Self::sync_source_lsn_updates`), so it is never treated as
    /// unconditionally stale even during re-adoption: it falls through to the
    /// same numeric comparison the stable path uses.
    pub(super) fn sync_latest_wins_incoming(
        &self,
        row: &RowChange,
        committed: &VersionedRow,
        committed_source: Option<Lsn>,
        incoming_arrival: Option<Lsn>,
        adoption: SyncAdoption,
    ) -> bool {
        let Some(arrival) = incoming_arrival else {
            return true;
        };
        if adoption == SyncAdoption::ReadoptingSource && committed_source.is_some() {
            return true;
        }
        let committed_lsn =
            Self::resolve_sync_source_lsn(committed_source, committed.lsn).unwrap_or(committed.lsn);
        if arrival != committed_lsn {
            return arrival > committed_lsn;
        }
        for (col, incoming_val) in &row.values {
            if let (Value::TxId(incoming_tx), Some(Value::TxId(local_tx))) =
                (incoming_val, committed.values.get(col))
            {
                if incoming_tx.0 > local_tx.0 {
                    return true;
                }
                if incoming_tx.0 < local_tx.0 {
                    return false;
                }
            }
        }
        row.values == committed.values
    }

    /// Resolve the ordering position an incoming row carries, from a per-row
    /// arrival map keyed by the row's own `.lsn` (rows sharing an `.lsn` share
    /// one accepted commit and its fate, so the key is never ambiguous within
    /// one changeset). A row whose `.lsn` is absent from `arrivals` is the
    /// LEGACY path every direct/engine-only caller of `apply_changes` gets:
    /// its own `.lsn` IS its arrival, unchanged from before this concept
    /// existed. An entry present but mapped to `None` is the real, wire-driven
    /// case: the sender carries no established ordering position for this row
    /// at all.
    pub(super) fn resolve_incoming_arrival(
        row: &RowChange,
        arrivals: &HashMap<Lsn, Option<Lsn>>,
    ) -> Option<Lsn> {
        match arrivals.get(&row.lsn) {
            Some(explicit) => *explicit,
            None => Some(row.lsn),
        }
    }

    /// Resolve one `sync_source_lsn` sidecar reading against the row version
    /// it was read alongside. The sentinel (`SYNC_SOURCE_LSN_OWN_COMMIT`)
    /// always means "this row's own committed LSN", so it resolves to
    /// `own_committed_lsn` -- the `.lsn` of the SAME committed row version
    /// the sidecar lookup was keyed by. Any other stored value, or `None`
    /// (no sidecar entry at all), passes through unchanged. Every reader of
    /// the sidecar must call this before treating the value as a real
    /// arrival; seeing the raw sentinel anywhere outside `set_sync_row_
    /// source_lsn`'s own write path and this resolver is a bug.
    pub(super) fn resolve_sync_source_lsn(
        stored: Option<Lsn>,
        own_committed_lsn: Lsn,
    ) -> Option<Lsn> {
        match stored {
            Some(SYNC_SOURCE_LSN_OWN_COMMIT) => Some(own_committed_lsn),
            other => other,
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn projected_sync_incoming_values_for_applied_rows(
        &self,
        rows: &[RowChange],
        policies: &ConflictPolicies,
        arrivals: &HashMap<Lsn, Option<Lsn>>,
        adoption: SyncAdoption,
        fresh_creator_deletes: &[AuthenticatedFreshCreatorDelete],

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<ProjectedSyncApply> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let snapshot = self.snapshot();
        let mut projected_rows = Vec::<ProjectedSyncRow>::new();
        let mut projected_rows_cache = HashMap::new();
        let mut deleted_committed_row_ids = HashMap::<String, HashSet<RowId>>::new();
        let mut synthetic_row_ids = HashSet::<RowId>::new();
        let mut next_synthetic_row_id = u64::MAX;

        for row in rows {
            // Existing source rows are needed only to resolve this entry.
            let _row_outputs = self.writer_output_scope();
            if row.values.is_empty() {
                continue;
            }
            if !row.deleted
                && let Some(table_meta) = self.table_meta(&row.table)
                && !self.sync_incoming_values_allowed_for_access(
                    &row.table,
                    &table_meta,
                    &row.values,
                    row.lsn,
                    read_memory.clone(),
                )?
            {
                continue;
            }
            let empty_deleted = HashSet::new();
            let skip_deleted = deleted_committed_row_ids
                .get(&row.table)
                .unwrap_or(&empty_deleted);
            let existing = sync_visible_point_lookup(
                self,
                &projected_rows_cache,
                &row.table,
                &row.natural_key,
                snapshot,
                skip_deleted,
                read_memory.clone(),
            )?;
            if !row.deleted
                && existing.is_none()
                && self.incoming_row_is_stale_against_sync_tombstone(row, arrivals, adoption)?
            {
                continue;
            }

            // Keep preflight's projected world identical to the apply loop:
            // a continuing pull cannot mutate a current Pending row while its
            // source lacks fresh ordering. Counting it here would invent an
            // FK/DDL effect that the real apply then skips.
            if adoption == SyncAdoption::Continuing
                && existing
                    .as_ref()
                    .map(|local| {
                        self.relational_sync_source_at_read_root(
                            &row.table,
                            local.row_id,
                            read_memory.clone(),
                        )
                    })
                    .transpose()?
                    .is_some_and(|(_, kind)| {
                        matches!(
                            kind,
                            Some(contextdb_relational::store::SyncSourceKind::AcceptedLocalPending)
                        )
                    })
            {
                continue;
            }

            if row.deleted {
                let policy = Self::sync_conflict_policy_for_table(
                    policies,
                    self.table_meta(&row.table).as_ref(),
                    &row.table,
                );
                let applies = if let Some(local) = existing.as_ref() {
                    Self::authenticated_fresh_creator_delete_permission(
                        row,
                        local,
                        policy,
                        fresh_creator_deletes,
                    )
                    .is_some()
                        || self.sync_row_applies_over_committed(
                            row,
                            local,
                            policy,
                            Self::resolve_incoming_arrival(row, arrivals),
                            adoption,
                            read_memory.clone(),
                        )?
                } else {
                    false
                };
                if applies && let Some(local) = existing {
                    remove_cached_row(&mut projected_rows_cache, &row.table, local.row_id);
                    if !synthetic_row_ids.remove(&local.row_id) {
                        deleted_committed_row_ids
                            .entry(row.table.clone())
                            .or_default()
                            .insert(local.row_id);
                    }
                    projected_rows.retain(|projected| {
                        projected.table != row.table || projected.natural_key != row.natural_key
                    });
                }
                continue;
            }

            let mut values = row.values.clone();
            values.remove("__deleted");
            let row_meta = self.table_meta(&row.table);
            let policy =
                Self::sync_conflict_policy_for_table(policies, row_meta.as_ref(), &row.table);
            let applies = match existing.as_ref() {
                None => self
                    .sync_insert_constraint_error_for_values(
                        &row.table,
                        &values,
                        &projected_rows_cache,
                        deleted_committed_row_ids
                            .get(&row.table)
                            .unwrap_or(&empty_deleted),
                        read_memory.clone(),
                    )?
                    .is_none(),
                Some(local) => {
                    self.sync_row_applies_over_committed(
                        row,
                        local,
                        policy,
                        Self::resolve_incoming_arrival(row, arrivals),
                        adoption,
                        read_memory.clone(),
                    )? && self
                        .sync_upsert_constraint_error_for_values(
                            &row.table,
                            &values,
                            local,
                            &projected_rows_cache,
                            deleted_committed_row_ids
                                .get(&row.table)
                                .unwrap_or(&empty_deleted),
                            read_memory.clone(),
                        )?
                        .is_none()
                }
            };
            if !applies {
                continue;
            }

            if let Some(local) = existing.as_ref()
                && !synthetic_row_ids.contains(&local.row_id)
            {
                deleted_committed_row_ids
                    .entry(row.table.clone())
                    .or_default()
                    .insert(local.row_id);
            }
            projected_rows.retain(|projected| {
                projected.table != row.table || projected.natural_key != row.natural_key
            });
            let row_id = existing.as_ref().map(|row| row.row_id).unwrap_or_else(|| {
                let row_id = RowId(next_synthetic_row_id);
                synthetic_row_ids.insert(row_id);
                next_synthetic_row_id = next_synthetic_row_id.saturating_sub(1);
                row_id
            });
            upsert_cached_projection(
                &mut projected_rows_cache,
                &row.table,
                row_id,
                values.clone(),
                row.lsn,
            );
            projected_rows.push(ProjectedSyncRow {
                table: row.table.clone(),
                natural_key: row.natural_key.clone(),
                values,
            });
        }

        self.retain_projected_rows_with_valid_composite_fks(
            &mut projected_rows,
            &deleted_committed_row_ids,
            read_memory.clone(),
        )?;
        // The later apply checks inspect only incoming parent keys. Move those
        // values into one writer-funded projection; payloads used to choose a
        // conflict winner have finished their work here.
        let (incoming_values, incoming_values_owner) =
            self.projected_sync_parent_keys(projected_rows)?;
        Ok(ProjectedSyncApply {
            incoming_values,
            deleted_committed_row_ids,
            _incoming_values_owner: incoming_values_owner,
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn sync_insert_constraint_error_for_values(
        &self,
        table: &str,
        values: &HashMap<String, Value>,
        projected_rows_cache: &HashMap<String, Vec<VersionedRow>>,
        deleted_committed_row_ids: &HashSet<RowId>,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<String>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.sync_projected_row_constraint_error_for_values(
            table,
            values,
            projected_rows_cache,
            None,
            deleted_committed_row_ids,
            read_memory.clone(),
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn sync_projected_row_constraint_error_for_values(
        &self,
        table: &str,
        values: &HashMap<String, Value>,
        projected_rows_cache: &HashMap<String, Vec<VersionedRow>>,
        skip_row_id: Option<RowId>,
        deleted_committed_row_ids: &HashSet<RowId>,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<String>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let meta = self
            .table_meta(table)
            .ok_or_else(|| Error::TableNotFound(table.to_string()))?;

        for col_def in &meta.columns {
            if !col_def.nullable && col_def.default.is_none() {
                match values.get(&col_def.name) {
                    None | Some(Value::Null) => {
                        return Ok(Some(format!(
                            "NOT NULL constraint violated: {}.{}",
                            table, col_def.name
                        )));
                    }
                    _ => {}
                }
            }
        }

        for col_def in &meta.columns {
            if (col_def.primary_key || col_def.unique)
                && let Some(new_value) = values.get(&col_def.name)
                && *new_value != Value::Null
                && projected_rows_cache.get(table).is_some_and(|rows| {
                    rows.iter().any(|row| {
                        skip_row_id != Some(row.row_id)
                            && row.values.get(&col_def.name) == Some(new_value)
                    })
                })
            {
                return Ok(Some(format!(
                    "UNIQUE constraint violated: {}.{}",
                    table, col_def.name
                )));
            }
            if (col_def.primary_key || col_def.unique)
                && let Some(new_value) = values.get(&col_def.name)
                && *new_value != Value::Null
                && let Some(conflict) = self.required_indexed_visible_row_by_column(
                    table,
                    &col_def.name,
                    new_value,
                    self.snapshot(),
                    deleted_committed_row_ids,
                    read_memory.clone(),
                )?
                && skip_row_id != Some(conflict.row_id)
            {
                return Ok(Some(format!(
                    "UNIQUE constraint violated: {}.{}",
                    table, col_def.name
                )));
            }
        }

        for unique_columns in &meta.unique_constraints {
            let Some(tuple) = tuple_values_from_map(values, unique_columns) else {
                continue;
            };
            if projected_rows_cache.get(table).is_some_and(|rows| {
                rows.iter().any(|row| {
                    skip_row_id != Some(row.row_id)
                        && tuple_values_for_row(row, unique_columns).as_ref() == Some(&tuple)
                })
            }) {
                return Ok(Some(format!(
                    "UNIQUE constraint violated: {}({})",
                    table,
                    unique_columns.join(", ")
                )));
            }
            if let Some(conflict) = self.required_indexed_visible_row_by_columns(
                table,
                unique_columns,
                &tuple,
                self.snapshot(),
                deleted_committed_row_ids,
                read_memory.clone(),
            )? && skip_row_id != Some(conflict.row_id)
            {
                return Ok(Some(format!(
                    "UNIQUE constraint violated: {}({})",
                    table,
                    unique_columns.join(", ")
                )));
            }
        }

        Ok(None)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn sync_committed_unique_conflict_for_values(
        &self,
        table: &str,
        values: &HashMap<String, Value>,
        deleted_committed_row_ids: &HashSet<RowId>,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<VersionedRow>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let meta = self
            .table_meta(table)
            .ok_or_else(|| Error::TableNotFound(table.to_string()))?;

        for column in meta
            .columns
            .iter()
            .filter(|column| column.unique && !column.primary_key)
        {
            let Some(value) = values.get(&column.name) else {
                continue;
            };
            if *value == Value::Null {
                continue;
            }
            if let Some(conflict) = self.required_indexed_visible_row_by_column(
                table,
                &column.name,
                value,
                self.snapshot(),
                deleted_committed_row_ids,
                read_memory.clone(),
            )? {
                return Ok(Some(conflict));
            }
        }

        for columns in &meta.unique_constraints {
            let Some(tuple) = tuple_values_from_map(values, columns) else {
                continue;
            };
            if let Some(conflict) = self.required_indexed_visible_row_by_columns(
                table,
                columns,
                &tuple,
                self.snapshot(),
                deleted_committed_row_ids,
                read_memory.clone(),
            )? {
                return Ok(Some(conflict));
            }
        }

        Ok(None)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn sync_upsert_constraint_error_for_values(
        &self,
        table: &str,
        values: &HashMap<String, Value>,
        existing: &VersionedRow,
        projected_rows_cache: &HashMap<String, Vec<VersionedRow>>,
        deleted_committed_row_ids: &HashSet<RowId>,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<String>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let values = match self.coerce_row_for_insert(table, values.clone(), None, None) {
            Ok(values) => values,
            Err(err) if is_fatal_sync_apply_error(&err) => return Err(err),
            Err(err) => return Ok(Some(format!("{err}"))),
        };

        if let Some(meta) = self.table_meta(table) {
            for col_def in meta.columns.iter().filter(|column| column.immutable) {
                let Some(incoming) = values.get(&col_def.name) else {
                    continue;
                };
                if existing.values.get(&col_def.name) != Some(incoming) {
                    return Ok(Some(format!(
                        "{}",
                        Error::ImmutableColumn {
                            table: table.to_string(),
                            column: col_def.name.clone(),
                        }
                    )));
                }
            }
        }

        if let Some(err) = self.sync_projected_row_constraint_error_for_values(
            table,
            &values,
            projected_rows_cache,
            Some(existing.row_id),
            deleted_committed_row_ids,
            read_memory.clone(),
        )? {
            return Ok(Some(err));
        }

        let snapshot = self.snapshot_for_read();
        for check in [
            self.assert_row_write_allowed(
                table,
                existing.row_id,
                &existing.values,
                snapshot,
                read_memory.clone(),
            ),
            self.assert_row_write_allowed(
                table,
                existing.row_id,
                &values,
                snapshot,
                read_memory.clone(),
            ),
            self.validate_commit_time_upsert_state_transition(table, existing, &values),
        ] {
            if let Err(err) = check {
                if is_fatal_sync_apply_error(&err) {
                    return Err(err);
                }
                return Ok(Some(format!("{err}")));
            }
        }

        Ok(None)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn retain_projected_rows_with_valid_composite_fks(
        &self,
        projected_rows: &mut Vec<ProjectedSyncRow>,
        deleted_committed_row_ids: &HashMap<String, HashSet<RowId>>,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        loop {
            let mut remove_indexes = Vec::new();
            for (idx, row) in projected_rows.iter().enumerate() {
                let meta = self
                    .table_meta(&row.table)
                    .ok_or_else(|| Error::TableNotFound(row.table.clone()))?;
                if self.projected_composite_fk_violation_for_values(
                    &meta,
                    &row.values,
                    projected_rows,
                    deleted_committed_row_ids,
                    read_memory.clone(),
                )? {
                    remove_indexes.push(idx);
                }
            }
            if remove_indexes.is_empty() {
                return Ok(());
            }
            for idx in remove_indexes.into_iter().rev() {
                projected_rows.remove(idx);
            }
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn projected_sync_parent_keys(
        &self,
        rows: Vec<ProjectedSyncRow>,
    ) -> Result<(
        HashMap<String, Vec<HashMap<String, Value>>>,
        SyncParentKeyProjectionOwner,
    )> {
        let metadata = self.relational_store.table_meta.read();
        let needs_column = |table: &str, column: &str| {
            rows.iter().any(|child| {
                metadata.get(&child.table).is_some_and(|meta| {
                    meta.composite_foreign_keys.iter().any(|fk| {
                        fk.parent_table == table
                            && fk.parent_columns.iter().any(|name| name == column)
                    })
                })
            })
        };
        let mut owner = OwnedMemoryReservation::new(
            crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
        );
        let mut bytes = 0usize;
        for row in &rows {
            let mut key_bytes = 0usize;
            let mut key_count = 0usize;
            for (column, value) in &row.values {
                if needs_column(&row.table, column) {
                    key_count += 1;
                    key_bytes = key_bytes.saturating_add(
                        estimate_value_key_bytes(column).saturating_add(value.estimated_bytes()),
                    );
                }
            }
            if key_count != 0 {
                // Same row-map pricing basis as ordinary writer value copies,
                // once, without retained-row/index/sidecar multipliers.
                bytes = bytes
                    .saturating_add(96)
                    .saturating_add(row.table.len())
                    .saturating_add(key_bytes);
            }
        }
        owner.try_grow_for(
            bytes,
            "sync_apply",
            "project_parent_keys",
            "Reduce the incoming key batch or raise the declared writer memory limit.",
        )?;
        let mut incoming_values = HashMap::<String, Vec<HashMap<String, Value>>>::new();
        // Filter while the borrowed batch still identifies all child tables.
        // Moving these maps afterwards avoids another payload copy.
        let mut rows = rows;
        for index in 0..rows.len() {
            let (before, at_and_after) = rows.split_at_mut(index);
            let (row, after) = at_and_after.split_first_mut().expect("known row position");
            let table = &row.table;
            let values = std::mem::take(&mut row.values);
            row.values = values
                .into_iter()
                .filter(|(column, _)| {
                    before
                        .iter()
                        .chain(after.iter())
                        .map(|child| child.table.as_str())
                        .chain(std::iter::once(table.as_str()))
                        .any(|child_table| {
                            metadata.get(child_table).is_some_and(|meta| {
                                meta.composite_foreign_keys.iter().any(|fk| {
                                    fk.parent_table == *table
                                        && fk.parent_columns.iter().any(|name| name == column)
                                })
                            })
                        })
                })
                .collect();
        }
        for row in rows {
            if !row.values.is_empty() {
                incoming_values
                    .entry(row.table)
                    .or_default()
                    .push(row.values);
            }
        }
        Ok((incoming_values, SyncParentKeyProjectionOwner::new(owner)))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn projected_composite_fk_violation_for_values(
        &self,
        child_meta: &TableMeta,
        values: &HashMap<String, Value>,
        incoming_rows: &[ProjectedSyncRow],
        deleted_committed_row_ids: &HashMap<String, HashSet<RowId>>,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<bool> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        for fk in &child_meta.composite_foreign_keys {
            let Some(tuple) = tuple_values_from_map(values, &fk.child_columns) else {
                continue;
            };
            let incoming_match = incoming_rows.iter().any(|row| {
                row.table == fk.parent_table
                    && fk
                        .parent_columns
                        .iter()
                        .zip(tuple.iter())
                        .all(|(column, value)| row.values.get(column) == Some(value))
            });
            if incoming_match {
                continue;
            }

            let empty_deleted = HashSet::new();
            let deleted_parent_row_ids = deleted_committed_row_ids
                .get(&fk.parent_table)
                .unwrap_or(&empty_deleted);
            let committed_match = self.indexed_visible_row_exists_by_columns(
                &fk.parent_table,
                &fk.parent_columns,
                &tuple,
                self.snapshot(),
                deleted_parent_row_ids,
                None,
                read_memory.clone(),
            )?;
            if !committed_match {
                return Ok(true);
            }
        }

        Ok(false)
    }

    pub(super) fn projected_sync_dag_edge_types(&self, ddl: &[DdlChange]) -> HashSet<EdgeType> {
        let mut edge_types = HashSet::new();
        {
            let meta = self.relational_store.table_meta.read();
            for table_meta in meta.values() {
                edge_types.extend(table_meta.dag_edge_types.iter().cloned());
            }
        }

        for change in ddl {
            match change {
                DdlChange::CreateTable { constraints, .. }
                | DdlChange::AlterTable { constraints, .. } => {
                    edge_types.extend(ddl_dag_edge_types(constraints));
                }
                DdlChange::DropTable { .. }
                | DdlChange::CreateIndex { .. }
                | DdlChange::DropIndex { .. }
                | DdlChange::CreateTrigger { .. }
                | DdlChange::CreateTriggerIncludingSync { .. }
                | DdlChange::DropTrigger { .. }
                | DdlChange::CreateEventType { .. }
                | DdlChange::CreateSink { .. }
                | DdlChange::CreateRoute { .. }
                | DdlChange::DropRoute { .. } => {}
            }
        }

        edge_types
    }

    /// The edge types whose links a received unit's table drops take with
    /// them. A dropped table shaped as a link table owns the links its rows
    /// name, exactly as a local drop removes them; those types join the graph
    /// image the unit replaces, so the staged drop removes the links and the
    /// durable install retires them.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn visit_link_types_of_dropped_link_tables(
        &self,
        ddl: &[DdlChange],
        live_tables: &HashMap<String, TableMeta>,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        visit: &mut dyn FnMut(&str) -> Result<()>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let _keys = contextdb_core::read_memory::ReadCredit::try_new(
            read_memory.clone(),
            ddl.len().saturating_mul(256),
            "sync",
            "dropped_graph_table_keys",
            "Raise the declared reader memory limit to inspect dropped graph tables.",
        )?;
        let tables = ddl
            .iter()
            .filter_map(|change| match change {
                DdlChange::DropTable { name }
                    if live_tables
                        .get(name)
                        .is_some_and(has_graph_edge_table_shape) =>
                {
                    Some(name.as_str())
                }
                _ => None,
            })
            .collect::<BTreeSet<_>>();
        if tables.is_empty() {
            return Ok(());
        }
        let (snapshot, root) = self.capture_writer_schema_statement()?;
        if let Some(root) = root {
            use contextdb_relational::source::RelationalReadSource;
            let source = self.statement_relational_source(root, read_memory)?;
            for table in tables {
                source.verify_historical_versions(table)?;
                let mut cursor = source.row_cursor(
                    table,
                    std::ops::Bound::Unbounded,
                    std::ops::Bound::Unbounded,
                    false,
                )?;
                while let Some(window) = cursor.next_window()? {
                    let row = window.value();
                    if row.visible_at(snapshot)
                        && let Some(edge_type) =
                            row.values.get("edge_type").and_then(Value::as_text)
                    {
                        visit(edge_type)?;
                    }
                }
            }
        } else {
            let rows = self.relational_store.tables.read();
            for table in tables {
                for row in rows
                    .get(table)
                    .into_iter()
                    .flatten()
                    .filter(|row| row.visible_at(snapshot))
                {
                    if let Some(edge_type) = row.values.get("edge_type").and_then(Value::as_text) {
                        visit(edge_type)?;
                    }
                }
            }
        }
        Ok(())
    }

    pub(super) fn sync_dropped_tables(ddl: &[DdlChange]) -> HashSet<String> {
        ddl.iter()
            .filter_map(|change| match change {
                DdlChange::DropTable { name } => Some(name.clone()),
                _ => None,
            })
            .collect()
    }

    pub(super) fn skip_event_bus_ddl_for_dropped_table(
        ddl: &DdlChange,
        dropped_tables: &HashSet<String>,
        table_exists_after_apply: impl Fn(&str) -> bool,
    ) -> bool {
        match ddl {
            DdlChange::CreateEventType { table, .. } | DdlChange::CreateRoute { table, .. } => {
                !table.is_empty()
                    && !table_exists_after_apply(table)
                    && dropped_tables.contains(table)
            }
            _ => false,
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn preflight_sync_edge_cycles(
        &self,
        changes: &ChangeSet,
        dag_edge_types: &HashSet<EdgeType>,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if dag_edge_types.is_empty() || changes.edges.is_empty() {
            return Ok(());
        }

        let snapshot = self.snapshot_for_read();
        let mut overlay = HashMap::new();
        let mut memory = crate::memory_accounting::OwnedMemoryReservation::new(
            crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
        );

        for edge in &changes.edges {
            if !dag_edge_types.contains(&edge.edge_type) {
                continue;
            }

            let identity = (edge.source, edge.edge_type.clone(), edge.target);
            let is_delete = matches!(edge.properties.get("__deleted"), Some(Value::Bool(true)));
            if !overlay.contains_key(&identity) {
                memory.try_grow_for(
                    256 + edge.edge_type.len(),
                    "writer_source",
                    "sync_cycle_overlay",
                    "Raise MEMORY_LIMIT to validate this graph transaction.",
                )?;
            }
            if is_delete {
                overlay.insert(identity, false);
                continue;
            }

            if edge.source == edge.target
                || self.graph_path_with_sync_overlay(
                    snapshot,
                    &edge.edge_type,
                    edge.target,
                    edge.source,
                    &overlay,
                    read_memory.clone(),
                )?
            {
                return Err(Error::CycleDetected {
                    edge_type: edge.edge_type.clone(),
                    source_node: edge.source,
                    target_node: edge.target,
                });
            }

            overlay.insert(identity, true);
        }

        Ok(())
    }

    /// Extracts changes from this database since the given LSN.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn changes_since_base(
        &self,
        since_lsn: Lsn,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<(ChangeSet, DdlProvenanceSource)> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.changes_since_base_projected(
            since_lsn,
            &ChangeProjection::unbounded(),
            vector_copy_owner,
            read_memory.clone(),
        )
    }

    /// A changed dependent joins an atomic unit only when its currently
    /// referenced parent was absent at the caller's confirmed source frontier.
    /// This is an indexed current-state probe plus an indexed historical
    /// existence probe; it deliberately never replays the source history.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn dependency_row_has_unresolved_parent_at_frontier(
        &self,
        row: &RowChange,
        snapshot: SnapshotId,
        confirmed_snapshot: SnapshotId,
        skip_deleted: &HashSet<RowId>,
        history: &SyncDirectionHistory,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<bool> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let Some(meta) = self.table_meta(&row.table) else {
            return Ok(false);
        };
        for column in &meta.columns {
            let Some(reference) = &column.references else {
                continue;
            };
            let Some(value) = row.values.get(&column.name) else {
                continue;
            };
            if matches!(value, Value::Null) {
                continue;
            }
            let Some(parent) = self.current_sync_row_by_columns(
                &reference.table,
                std::slice::from_ref(&reference.column),
                std::slice::from_ref(value),
                snapshot,
                skip_deleted,
                vector_copy_owner,
                read_memory.clone(),
            )?
            else {
                continue;
            };
            if history.includes(
                &parent.table,
                parent.lsn,
                &[SyncDirection::Push, SyncDirection::Both],
            ) && self
                .visible_row_by_natural_key(
                    &parent.table,
                    &parent.natural_key,
                    confirmed_snapshot,
                    skip_deleted,
                    read_memory.clone(),
                )?
                .is_none()
            {
                return Ok(true);
            }
        }
        for reference in &meta.composite_foreign_keys {
            let values = reference
                .child_columns
                .iter()
                .map(|column| row.values.get(column))
                .collect::<Option<Vec<_>>>();
            let Some(values) = values else {
                continue;
            };
            if values.iter().any(|value| matches!(value, Value::Null)) {
                continue;
            }
            let Some(parent) = self.current_sync_row_by_columns(
                &reference.parent_table,
                &reference.parent_columns,
                &values.into_iter().cloned().collect::<Vec<_>>(),
                snapshot,
                skip_deleted,
                vector_copy_owner,
                read_memory.clone(),
            )?
            else {
                continue;
            };
            if history.includes(
                &parent.table,
                parent.lsn,
                &[SyncDirection::Push, SyncDirection::Both],
            ) && self
                .visible_row_by_natural_key(
                    &parent.table,
                    &parent.natural_key,
                    confirmed_snapshot,
                    skip_deleted,
                    read_memory.clone(),
                )?
                .is_none()
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn current_sync_row_by_columns(
        &self,
        table: &str,
        columns: &[String],
        values: &[Value],
        snapshot: SnapshotId,
        skip_deleted: &HashSet<RowId>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<RowChange>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let Some(row) = self.required_indexed_visible_row_by_columns(
            table,
            columns,
            values,
            snapshot,
            skip_deleted,
            read_memory.clone(),
        )?
        else {
            return Ok(None);
        };
        Ok(self
            .row_change_values_from_row(table, &row, vector_copy_owner, read_memory.clone())?
            .map(|(natural_key, values)| RowChange {
                table: table.to_string(),
                natural_key,
                values,
                deleted: false,
                lsn: row.lsn,
                created_at: row.created_at,
            }))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn validate_dependency_complete_unit(&self, changes: &ChangeSet) -> Result<()> {
        if !changes.ddl.is_empty() {
            changes
                .validate_ddl_lsn_cardinality()
                .map_err(|error| Error::SyncError(error.into()))?;
            // Schema-bearing connected units are source-ordered pending
            // overlays, including data before and after their DDL groups.
            // Validate that overlay rather than requiring every row to share
            // a schema statement's transport position.
            let _overlay =
                self.prepare_connected_overlay(changes, self.declared_reader_memory()?)?;
            // DDL-only migrations have no relational component to inspect.
            // Their paired source LSN cardinality above is the declaration
            // contract; requiring a foreign key here would reject an
            // authenticated schema bootstrap before it reached the received
            // schema gate.
            return Ok(());
        }
        let live_rows = changes
            .rows
            .iter()
            .filter(|row| !row.deleted)
            .collect::<Vec<_>>();
        let mut related = HashSet::<Vec<u8>>::new();
        for child in &live_rows {
            let Some(meta) = self.table_meta(&child.table) else {
                return Err(Error::TableNotFound(child.table.clone()));
            };
            for column in meta.columns.iter().filter_map(|column| {
                column
                    .references
                    .as_ref()
                    .map(|reference| (column, reference))
            }) {
                let Some(value) = child.values.get(&column.0.name) else {
                    continue;
                };
                if matches!(value, Value::Null) {
                    continue;
                }
                related.insert(sync_row_identity(child));
                if let Some(parent) = live_rows.iter().find(|parent| {
                    parent.table == column.1.table
                        && parent.values.get(&column.1.column) == Some(value)
                }) {
                    related.insert(sync_row_identity(parent));
                }
            }
            for reference in &meta.composite_foreign_keys {
                let values = reference
                    .child_columns
                    .iter()
                    .map(|column| child.values.get(column))
                    .collect::<Option<Vec<_>>>();
                let Some(values) = values else {
                    continue;
                };
                if values.iter().any(|value| matches!(value, Value::Null)) {
                    continue;
                }
                related.insert(sync_row_identity(child));
                if let Some(parent) = live_rows.iter().find(|parent| {
                    parent.table == reference.parent_table
                        && reference
                            .parent_columns
                            .iter()
                            .zip(values.iter())
                            .all(|(column, value)| parent.values.get(column) == Some(*value))
                }) {
                    related.insert(sync_row_identity(parent));
                }
            }
        }
        if related.is_empty()
            || live_rows
                .iter()
                .any(|row| !related.contains(&sync_row_identity(row)))
        {
            return Err(Error::SyncError(
                ("dependency-complete push must contain one declared foreign-key component"
                    .to_string())
                .into(),
            ));
        }
        Ok(())
    }

    /// Extract ordered changes and preserve every materialization refusal.
    /// The caller retains the owner through consumption and encoding.
    ///
    /// A received atomic unit may retain several ordered replay positions.
    /// Only its final acceptance position is a committed snapshot. Resuming
    /// from an interior position returns the complete containing unit, so
    /// callers must tolerate replay and advance to the unit's final position.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn changes_since(&self, since_lsn: Lsn) -> Result<ChangeSet> {
        let read_memory = self.declared_reader_memory()?;
        let mut vector_copy_owner = crate::vector_read_memory::admission(read_memory.clone());
        self.changes_since_with_read_memory(since_lsn, &mut vector_copy_owner, read_memory)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn changes_since_under_schema_gate(
        &self,
        since_lsn: Lsn,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<ChangeSet> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let (base, _) =
            self.changes_since_base(since_lsn, vector_copy_owner, read_memory.clone())?;
        let pending = self.durable_pending_deletes_since(since_lsn, read_memory.clone())?;
        Ok(Self::reconcile_durable_pending_deletes(base, pending))
    }

    /// Same as [`Self::changes_since`], but additionally returns each row's
    /// own arrival: this store's sync provenance sidecar for it
    /// (`sync_source_lsn`), `None` when none exists — content this store
    /// authored itself, never yet ordered anywhere else in the fleet. Keyed
    /// by each row's own `.lsn` — the one channel `arrival` crosses the wire
    /// on ([`crate::sync_types::RowChange`] itself carries no such field) and
    /// back into [`Self::apply_synced_changes`]. Resolved by a fresh
    /// point-lookup per row rather than threaded through `changes_since`'s own
    /// change-log walk, so this covers every one of its internal paths
    /// (change-log replay AND the post-restart persisted-state fallback)
    /// uniformly, at the cost of one extra lookup per row.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn changes_since_with_arrivals(
        &self,
        since_lsn: Lsn,
    ) -> Result<(ChangeSet, HashMap<Lsn, Option<Lsn>>)> {
        let read_memory = self.declared_reader_memory()?;
        let mut vector_copy_owner = crate::vector_read_memory::admission(read_memory.clone());
        self.changes_since_with_arrivals_with_read_memory(
            since_lsn,
            &mut vector_copy_owner,
            read_memory,
        )
    }

    /// [`Self::changes_since_with_arrivals`] funded by an explicit read
    /// budget, for fixtures whose changes hold rows larger than the default.
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn changes_since_with_arrivals_with_read_memory_for_test(
        &self,
        since_lsn: Lsn,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<(ChangeSet, HashMap<Lsn, Option<Lsn>>)> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let mut vector_copy_owner = crate::vector_read_memory::admission(read_memory.clone());
        self.changes_since_with_arrivals_with_read_memory(
            since_lsn,
            &mut vector_copy_owner,
            read_memory,
        )
    }

    /// Build the arrival sidecar with a checkpoint and admission per row.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn sync_arrivals_for_changes_within(
        &self,
        changes: &ChangeSet,
        checkpoint: &dyn Fn() -> Result<()>,
        admit: &dyn Fn(usize) -> Result<()>,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<HashMap<Lsn, Option<Lsn>>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let snapshot = self.snapshot_for_read();
        let empty_skip_deleted = HashSet::new();
        let mut arrivals = HashMap::new();
        for row in &changes.rows {
            checkpoint()?;
            if arrivals.contains_key(&row.lsn) {
                continue;
            }
            let arrival = if row.deleted {
                self.sync_tombstone_arrivals
                    .read()
                    .get(&Self::sync_tombstone_key(&row.table, &row.natural_key))
                    .and_then(|marker| {
                        (!matches!(
                            marker.kind,
                            contextdb_relational::store::SyncSourceKind::AcceptedLocalPending
                        ) && !marker.accepted_local_order_invalidated
                            && (marker.delete_lsn == row.lsn
                                || marker.delete_lsn == SYNC_SOURCE_LSN_OWN_COMMIT))
                            .then(|| Self::resolve_sync_source_lsn(Some(marker.arrival), row.lsn))
                            .flatten()
                    })
            } else if self.table_meta(&row.table).is_none() {
                None
            } else {
                self.visible_row_by_natural_key(
                    &row.table,
                    &row.natural_key,
                    snapshot,
                    &empty_skip_deleted,
                    read_memory.clone(),
                )?
                .filter(|versioned| versioned.lsn == row.lsn)
                .map(|versioned| {
                    self.relational_sync_source_at_read_root(
                        &row.table,
                        versioned.row_id,
                        read_memory.clone(),
                    )
                })
                .transpose()?
                .and_then(|(lsn, kind)| {
                    (!matches!(
                        kind,
                        Some(contextdb_relational::store::SyncSourceKind::AcceptedLocalPending)
                    ))
                    .then_some(lsn)
                    .flatten()
                })
            };
            // The sentinel never crosses the wire: resolve it to this row's
            // own committed position before a peer ever sees it (the wire
            // format itself is unchanged -- see `SYNC_SOURCE_LSN_OWN_COMMIT`).
            let arrival = Self::resolve_sync_source_lsn(arrival, row.lsn);
            admit(ARRIVAL_ENTRY_BYTES)?;
            arrivals.insert(row.lsn, arrival);
        }
        Ok(arrivals)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn ddl_provenance_sidecar_for_source_entry(
        &self,
        source: &DdlProvenanceSource,
        entry: &SourceDdlIdentity,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<DurableDdlGenerationSidecar> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if source.kind == DdlProvenanceKind::SyntheticSnapshot {
            let table = Self::ddl_affected_table(&entry.ddl).map(str::to_string);
            let table_generation = table
                .as_deref()
                .map(|table| self.durable_lineage_table_generation(table, read_memory.clone()))
                .transpose()?;
            return Ok(DurableDdlGenerationSidecar {
                table,
                table_generation,
            });
        }
        self.recorded_ddl_generation_sidecar(entry.source_lsn, entry.ordinal)?
            .ok_or_else(|| {
                Error::SyncError(
                    (format!(
                        "DDL at source LSN {} ordinal {} lacks immutable generation evidence",
                        entry.source_lsn.0, entry.ordinal
                    ))
                    .into(),
                )
            })
    }

    /// Read the immutable generation sidecar one DDL occurrence recorded when
    /// it committed. Absence is returned rather than raised: a caller deciding
    /// whether an occurrence may carry new evidence must be able to leave an
    /// occurrence alone, while a caller validating existing evidence still
    /// treats absence as corrupt provenance.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn recorded_ddl_generation_sidecar(
        &self,
        lsn: Lsn,
        ordinal: u32,
    ) -> Result<Option<DurableDdlGenerationSidecar>> {
        if let Some(persistence) = &self.persistence {
            persistence.load_config_value::<DurableDdlGenerationSidecar>(
                &Self::durable_ddl_generation_sidecar_key(lsn, ordinal),
            )
        } else {
            Ok(self
                .in_memory_ddl_generations
                .lock()
                .get(&(lsn, ordinal))
                .cloned()
                .map(|(table, table_generation)| DurableDdlGenerationSidecar {
                    table,
                    table_generation,
                }))
        }
    }

    /// Return the durable raw DDL history rather than the ephemeral in-memory
    /// log when this database has persistence. A forced log gap must not turn
    /// old received schema into unprovable synthetic output after a move.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn durable_ddl_history_for_provenance(
        &self,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<writer_read::DdlHistory> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if self.persistence.is_some() {
            self.durable_ddl_entries_since(Lsn(0), read_memory.clone())
        } else {
            Ok(self.ddl_log.read().clone().into())
        }
    }

    pub(super) fn canonical_received_schema_tables(
        tables: &HashMap<String, TableMeta>,
    ) -> HashMap<String, TableMeta> {
        let mut canonical = tables.clone();
        for meta in canonical.values_mut() {
            meta.composite_foreign_keys.retain(|reference| {
                if reference.child_columns.len() != 1 || reference.parent_columns.len() != 1 {
                    return true;
                }
                let child = &reference.child_columns[0];
                let parent = &reference.parent_columns[0];
                !meta.columns.iter().any(|column| {
                    column.name == *child
                        && column.references.as_ref().is_some_and(|declared| {
                            declared.table == reference.parent_table && declared.column == *parent
                        })
                })
            });
        }
        canonical
    }

    /// Validate receiver-local arrival evidence for every captured occurrence.
    /// If one occurrence of a local DDL group is marked received, the entire
    /// group must be marked and each marker must rebind to that exact local
    /// DDL, ordinal, table, and generation. A partial or mismatched group is
    /// corrupt provenance, so outbound work stops rather than guessing.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn received_ddl_arrival_marks(
        &self,
        source: &DdlProvenanceSource,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<ReceivedDdlArrivalValidation> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if source.kind == DdlProvenanceKind::SyntheticSnapshot {
            if !self.has_received_ddl_arrivals()? {
                return Ok(ReceivedDdlArrivalValidation::Marked(HashSet::new()));
            }
            return Err(Error::SyncError(
                ("synthetic snapshot cannot carry received DDL authorship".to_string()).into(),
            ));
        }
        let mut groups = HashMap::<Lsn, (bool, bool)>::new();
        let mut marked = HashSet::new();
        for entry in source.entries.iter() {
            let key = (entry.source_lsn, entry.ordinal);
            let is_marked = self.with_received_ddl_arrival(
                key,
                |arrival| {
                    Ok(if let Some(arrival) = arrival {
                        let raw_ddl = self.raw_ddl_log_occurrence(
                            entry.source_lsn,
                            entry.ordinal,
                            read_memory.clone(),
                        )?;
                        let sidecar = self.ddl_provenance_sidecar_for_source_entry(
                            source,
                            entry,
                            read_memory.clone(),
                        )?;
                        let expected = Self::canonical_local_ddl_arrival_digest(
                            &raw_ddl,
                            entry.source_lsn,
                            entry.ordinal,
                            &sidecar,
                        )?;
                        if arrival.local_lsn != entry.source_lsn
                            || arrival.persisted_ordinal != entry.ordinal
                            || arrival.digest != expected
                        {
                            return Err(Error::SyncError((
                            "received DDL arrival digest does not bind its local schema occurrence"
                                .to_string()).into()));
                        }
                        marked.insert(key);
                        true
                    } else {
                        false
                    })
                },
                read_memory.clone(),
            )?;
            let group = groups.entry(entry.source_lsn).or_insert((false, true));
            group.0 |= is_marked;
            group.1 &= is_marked;
        }
        if groups
            .values()
            .any(|(some_marked, all_marked)| *some_marked && !*all_marked)
        {
            return Err(Error::SyncError(
                ("received DDL arrival evidence marks only part of one local schema commit"
                    .to_string())
                .into(),
            ));
        }
        Ok(ReceivedDdlArrivalValidation::Marked(marked))
    }

    /// Validate superseded evidence for every captured occurrence. Each mark
    /// must rebind to that exact local DDL, ordinal, table, and generation, and
    /// a local schema commit must be superseded whole; anything else is corrupt
    /// provenance, so outbound work stops rather than guessing.
    ///
    /// A synthetic snapshot carries replay positions rather than local log
    /// positions, so it holds no superseded occurrences: its own evidence is
    /// the received-arrival proof against the destination frontier.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn superseded_ddl_occurrence_marks(
        &self,
        source: &DdlProvenanceSource,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<HashSet<(Lsn, u32)>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if source.kind == DdlProvenanceKind::SyntheticSnapshot {
            return Ok(HashSet::new());
        }
        let mut groups = HashMap::<Lsn, (bool, bool)>::new();
        let mut marked = HashSet::new();
        for entry in source.entries.iter() {
            let key = (entry.source_lsn, entry.ordinal);
            let is_superseded = self.with_superseded_ddl_occurrence(
                key,
                |occurrence| {
                    Ok(if let Some(occurrence) = occurrence {
                        let raw_ddl = self.raw_ddl_log_occurrence(
                            entry.source_lsn,
                            entry.ordinal,
                            read_memory.clone(),
                        )?;
                        let sidecar = self.ddl_provenance_sidecar_for_source_entry(
                            source,
                            entry,
                            read_memory.clone(),
                        )?;
                        let expected = Self::canonical_local_ddl_arrival_digest(
                            &raw_ddl,
                            entry.source_lsn,
                            entry.ordinal,
                            &sidecar,
                        )?;
                        if occurrence.local_lsn != entry.source_lsn
                            || occurrence.persisted_ordinal != entry.ordinal
                            || occurrence.digest != expected
                        {
                            return Err(Error::SyncError((
                                "superseded DDL evidence does not bind its local schema occurrence"
                                    .to_string()).into()));
                        }
                        marked.insert(key);
                        true
                    } else {
                        false
                    })
                },
                read_memory.clone(),
            )?;
            let group = groups.entry(entry.source_lsn).or_insert((false, true));
            group.0 |= is_superseded;
            group.1 &= is_superseded;
        }
        if groups
            .values()
            .any(|(some_marked, all_marked)| *some_marked && !*all_marked)
        {
            return Err(Error::SyncError(
                ("superseded DDL evidence marks only part of one local schema commit".to_string())
                    .into(),
            ));
        }
        Ok(marked)
    }

    /// Apply received-DDL echo policy without changing the public change-set
    /// shape. Ordinary push never re-sends DDL learned from a hub. A destination
    /// rebuild re-sends only DDL that the edge already held at the move's
    /// frontier. Newer received DDL is suppressed; newer unmarked DDL fails
    /// closed because absence of a received marker is not positive local-origin
    /// evidence.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn filter_outbound_received_ddl(
        &self,
        mut changes: ChangeSet,
        source: &DdlProvenanceSource,
        destination_frontier: Option<Lsn>,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<ChangeSet> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let entries = source.surviving_entries(&changes)?;
        let arrivals = self.received_ddl_arrival_marks(source, read_memory.clone())?;
        let superseded = self.superseded_ddl_occurrence_marks(source, read_memory.clone())?;
        let mut ddl = Vec::with_capacity(changes.ddl.len());
        let mut ddl_lsn = Vec::with_capacity(changes.ddl_lsn.len());
        for ((change, lsn), entry) in changes.ddl.into_iter().zip(changes.ddl_lsn).zip(entries) {
            // Locally authored schema an authenticated vector has already
            // restated is settled provenance, so it never reaches the
            // fail-closed origin check below.
            if superseded.contains(&(entry.source_lsn, entry.ordinal)) {
                continue;
            }
            let is_received = arrivals.is_received(&(entry.source_lsn, entry.ordinal));
            if let Some(frontier) = destination_frontier
                && entry.source_lsn > frontier
                && !is_received
            {
                return Err(Error::SyncError((format!(
                    "schema published after the move at source LSN {} has no durable origin evidence",
                    entry.source_lsn.0
                )).into()));
            }
            if !is_received
                || destination_frontier.is_some_and(|frontier| entry.source_lsn <= frontier)
            {
                ddl.push(change);
                ddl_lsn.push(lsn);
            }
        }
        changes.ddl = ddl;
        changes.ddl_lsn = ddl_lsn;
        Ok(changes)
    }

    /// Build wire-only schema-instance evidence without changing the
    /// public `ChangeSet`/`DdlChange` API.  Normal history reads immutable
    /// sidecars; the synthetic full snapshot has no historical DDL entry, so
    /// it deliberately reads the current authoritative generation instead.
    ///
    /// Schema this node wrote is signed here with this node's own key, so
    /// every later holder checks authorship against the author's public key
    /// instead of trusting whichever peer delivered it. Received schema keeps
    /// the author's stored signature, or stays explicitly unattested when it
    /// arrived through the unattested apply door.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn outbound_ddl_provenance(
        &self,
        changes: &ChangeSet,
        source: &DdlProvenanceSource,
        tenant_id: &TenantId,
        local_node_id: &str,
        local_incarnation: Incarnation,
        signer: &dyn Fn(&[u8]) -> Result<Vec<u8>>,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Vec<crate::protocol::WireDdlProvenance>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if changes.ddl.is_empty() {
            return Ok(Vec::new());
        }
        // A mixed received-plus-local synthetic snapshot is reduced above to
        // exact durable local occurrences. Those occurrences carry ordinary
        // recorded sidecars and therefore resume historical provenance here.
        let historical_fallback;
        let source = if source.is_synthetic_snapshot() && source.surviving_entries(changes).is_err()
        {
            historical_fallback = DdlProvenanceSource::historical(changes);
            &historical_fallback
        } else {
            source
        };
        let source_entries = source.surviving_entries(changes)?;
        // Historical units retain the authored count and ordinal captured
        // before filtering. A synthetic snapshot has no authored positions:
        // sign only the application statements that remain after filtering.
        let mut unit_statements = HashMap::<Lsn, u32>::new();
        if source.is_synthetic_snapshot() {
            for entry in &source_entries {
                *unit_statements.entry(entry.source_lsn).or_default() += 1;
            }
        } else {
            for entry in source.entries.iter() {
                *unit_statements.entry(entry.source_lsn).or_default() += 1;
            }
        }
        let mut synthetic_ordinals = HashMap::<Lsn, u32>::new();
        // Schema this node received keeps its author's identity when it is
        // served onward: the arrival record for the occurrence holds it.
        //
        // A synthetic snapshot restates the current schema; it is NOT the
        // record of any authored migration. Extraction never builds one while
        // this node holds received schema -- it serves the exact authored
        // occurrences instead, or refuses (`received_schema_as_authored`). So
        // a snapshot is signable only as this node's own declarations, and
        // only while this node holds no received schema: signing one then
        // would re-author someone else's migration under this node's name.
        if source.is_synthetic_snapshot() && self.has_received_ddl_arrivals()? {
            return Err(Error::ReceivedSchemaAuthorshipUnavailable {
                table: changes
                    .ddl
                    .first()
                    .map(Self::declaration_subject)
                    .unwrap_or_default(),
                reason: "a restated schema snapshot cannot be signed by a node that holds \
                         received schema"
                    .to_string(),
            });
        }
        let authors = if source.is_synthetic_snapshot() {
            vec![None; source_entries.len()]
        } else {
            source_entries
                .iter()
                .map(|entry| {
                    self.with_received_ddl_arrival(
                        (entry.source_lsn, entry.ordinal),
                        |arrival| Ok(arrival.map(|arrival| arrival.author.clone())),
                        read_memory.clone(),
                    )
                })
                .collect::<Result<Vec<_>>>()?
        };
        let mut provenance = Vec::with_capacity(changes.ddl.len());
        for (((ddl, lsn), entry), author) in changes
            .ddl
            .iter()
            .zip(changes.ddl_lsn.iter())
            .zip(source_entries)
            .zip(authors)
        {
            let ordinal = if source.is_synthetic_snapshot() {
                let next = synthetic_ordinals.entry(entry.source_lsn).or_default();
                let ordinal = *next;
                *next += 1;
                ordinal
            } else {
                entry.ordinal
            };
            let sidecar =
                self.ddl_provenance_sidecar_for_source_entry(source, entry, read_memory.clone())?;
            let wire_ddl = crate::protocol::WireDdlChange::from(ddl.clone());
            let digest = crate::protocol::canonical_ddl_provenance_digest(
                &wire_ddl,
                *lsn,
                ordinal,
                sidecar.table.as_deref(),
                sidecar.table_generation,
            )
            .map_err(|err| Error::SyncError((err.to_string()).into()))?;
            let (author, unattested) = match author {
                Some(Some(author)) => (Some(author), false),
                Some(None) => (None, true),
                None => (
                    Some(Self::signed_local_ddl_author(
                        tenant_id,
                        local_node_id,
                        local_incarnation,
                        &wire_ddl,
                        *lsn,
                        ordinal,
                        unit_statements.get(&entry.source_lsn).copied().unwrap_or(0),
                        sidecar.table.as_deref(),
                        sidecar.table_generation,
                        &digest,
                        signer,
                    )?),
                    false,
                ),
            };
            provenance.push(crate::protocol::WireDdlProvenance {
                source_ddl_lsn: *lsn,
                ordinal,
                table: sidecar.table,
                table_generation: sidecar.table_generation,
                digest,
                author,
                unattested,
            });
        }
        Ok(provenance)
    }

    /// This node's own signature over one schema record it authored. The
    /// signature is over the same bytes the custody lane's schema dependency
    /// signs, so one attestation shape covers both delivery lanes, and it is
    /// deterministic: re-emitting the same record to another peer, or after a
    /// restart, reproduces the identical signature.
    #[allow(clippy::too_many_arguments)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn signed_local_ddl_author(
        tenant_id: &TenantId,
        local_node_id: &str,
        local_incarnation: Incarnation,
        wire_ddl: &crate::protocol::WireDdlChange,
        source_ddl_lsn: Lsn,
        ordinal: u32,
        unit_statements: u32,
        table: Option<&str>,
        table_generation: Option<u64>,
        digest: &[u8],
        signer: &dyn Fn(&[u8]) -> Result<Vec<u8>>,
    ) -> Result<crate::protocol::WireDdlAuthor> {
        if local_node_id.is_empty() {
            return Err(Error::SyncError(
                ("sending authored schema requires this node's authenticated identity".to_string())
                    .into(),
            ));
        }
        let ddl_bytes = rmp_serde::to_vec(wire_ddl).map_err(|err| {
            Error::SyncError((format!("cannot encode authored DDL: {err}")).into())
        })?;
        let bytes = crate::custody::records::ddl_author_attestation_bytes(
            tenant_id,
            local_node_id,
            local_incarnation,
            source_ddl_lsn,
            ordinal,
            unit_statements,
            table,
            table_generation,
            &ddl_bytes,
            digest,
        )?;
        Ok(crate::protocol::WireDdlAuthor {
            node_id: local_node_id.to_string(),
            incarnation: local_incarnation,
            source_ddl_lsn,
            ordinal,
            table: table.map(str::to_string),
            table_generation,
            digest: digest.to_vec(),
            attestation: signer(&bytes)?,
            unit_statements,
        })
    }

    /// Test-scoped read seam: the row-author lineages and DDL provenance a
    /// production sync client would attach to this exact outbound batch,
    /// computed via the same production functions the client calls
    /// (`outbound_row_lineages`, `outbound_ddl_provenance`) over the same
    /// durable state -- so a test can build its own reference encoding of a
    /// push that matches what the client actually sends, instead of
    /// guessing the byte cost of lineage and DDL provenance.
    ///
    /// WARNING -- despite the name, this is NOT read-only: `outbound_row_lineages`
    /// calls `bind_unbound_creation_lineage_with_state` (see that function,
    /// this file), which durably signs and persists a row's creation-lineage
    /// sidecar the first time it is computed and immutably reuses it on every
    /// later call. Calling this accessor for a row BEFORE the real push that
    /// is supposed to describe it permanently binds that row's lineage to
    /// whichever `signer` this call happened to pass; the real push then
    /// reuses that already-bound (and possibly wrong) signature and fails
    /// hub-side verification. Only call this after the push whose outbound
    /// batch it is reconstructing, once every row's lineage is already
    /// durably bound by that push -- never before it.
    #[cfg(all(feature = "test-seams", feature = "sync-orchestration"))]
    #[doc(hidden)]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn outbound_row_lineages_and_ddl_provenance_for_test(
        &self,
        changes: &ChangeSet,
        tenant_id: &TenantId,
        local_node_id: &str,
        local_incarnation: Incarnation,
        signer: &dyn Fn(&[u8]) -> Result<Vec<u8>>,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<OutboundPushBatchProvenance> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let lineages = self.outbound_row_lineages(
            changes,
            tenant_id,
            local_node_id,
            local_incarnation,
            signer,
            read_memory.clone(),
        )?;
        let source = DdlProvenanceSource::historical(changes);
        let ddl_provenance = self.outbound_ddl_provenance(
            changes,
            &source,
            tenant_id,
            local_node_id,
            local_incarnation,
            signer,
            read_memory.clone(),
        )?;
        Ok((lineages, ddl_provenance))
    }

    /// Source-owned canonical provenance for a schema-only integration
    /// fixture. Unlike the combined row helper above, this performs no
    /// lineage binding. The sender still signs schema it authored with its
    /// own key, exactly as a production push does.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn outbound_ddl_provenance_for_test(
        &self,
        changes: &ChangeSet,
        tenant_id: &TenantId,
        local_node_id: &str,
        local_incarnation: Incarnation,
        signer: &dyn Fn(&[u8]) -> Result<Vec<u8>>,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Vec<crate::protocol::WireDdlProvenance>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if !changes.rows.is_empty() || !changes.edges.is_empty() || !changes.vectors.is_empty() {
            return Err(Error::SyncError(
                ("DDL provenance test bridge accepts schema-only units".to_string()).into(),
            ));
        }
        self.outbound_ddl_provenance(
            changes,
            &DdlProvenanceSource::historical(changes),
            tenant_id,
            local_node_id,
            local_incarnation,
            signer,
            read_memory.clone(),
        )
    }

    /// Stages authenticated authorship only for incoming rows that actually
    /// reached this transaction's accepted path. The config write shares the
    /// row commit, so a crash cannot expose a winner without its evidence (or
    /// evidence for a row whose mutation rolled back).
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn stage_accepted_sync_row_authors(
        &self,
        tx: TxId,
        author_node_id: &str,
        rows: &[RowChange],
    ) -> Result<Vec<(String, Vec<u8>, String)>> {
        // Received-schema planning obtains the immutable author from the
        // accepted lineage sidecar below.  A relay receipt identifies the
        // forwarding peer, not the author, so it must not overwrite that
        // provenance in the detached finalized payload.
        if self.capture_detached_sync_write_set.load(Ordering::SeqCst) {
            return Ok(Vec::new());
        }
        let mut entries = HashMap::new();
        for row in rows.iter().filter(|row| !row.deleted) {
            entries.insert(
                (
                    row.table.clone(),
                    sync_identity_key(&row.table, &row.natural_key),
                ),
                author_node_id.to_string(),
            );
        }
        if entries.is_empty() {
            return Ok(Vec::new());
        }
        if self.persistence.is_some() || self.capture_detached_sync_write_set.load(Ordering::SeqCst)
        {
            let writes = entries
                .iter()
                .map(|((_table, identity), author)| {
                    Ok((
                        format!("sync_row_author.{}", Self::hex_component(identity)),
                        RedbPersistence::encode_config_value(author)?,
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            self.tx_mgr.append_config_writes(tx, writes)?;
        }
        Ok(entries
            .into_iter()
            .map(|((table, identity), author)| (table, identity, author))
            .collect())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn accepted_sync_row_author(
        &self,
        table: &str,
        natural_key: &NaturalKey,
    ) -> Result<Option<String>> {
        let identity = sync_identity_key(table, natural_key);
        if let Some(author) = self
            .accepted_sync_row_authors
            .read()
            .get(&(table.to_string(), identity.clone()))
            .cloned()
        {
            return Ok(Some(author));
        }
        let Some(persistence) = &self.persistence else {
            return Ok(None);
        };
        persistence.load_config_value(&format!(
            "sync_row_author.{}",
            Self::hex_component(&identity)
        ))
    }

    pub(super) fn hex_component(bytes: &[u8]) -> String {
        let mut encoded = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            use std::fmt::Write;
            let _ = write!(&mut encoded, "{byte:02x}");
        }
        encoded
    }

    /// The prefix is deliberately made of length-prefixed hex components. A
    /// tenant or node id may contain dots (or any other display character),
    /// but it must never make one reconciliation context match another when
    /// persistent config values are loaded by prefix.
    pub(super) fn terminal_refusal_marker_context_prefix(
        context: &TerminalRefusalPullContext,
    ) -> String {
        let tenant = context.tenant_id.as_str().as_bytes();
        let hub = context.hub_node_id.as_bytes();
        format!(
            "sync_terminal_refusal.v1.{:016x}.{:016x}.",
            tenant.len(),
            hub.len(),
        ) + &Self::hex_component(tenant)
            + "."
            + &Self::hex_component(hub)
            + "."
    }

    pub(super) fn terminal_refusal_marker_key(
        context: &TerminalRefusalPullContext,
        table: &str,
        natural_key: &NaturalKey,
    ) -> TerminalRefusalMarkerKey {
        (
            context.tenant_id.as_str().to_string(),
            context.hub_node_id.clone(),
            table.to_string(),
            sync_identity_key(table, natural_key),
        )
    }

    pub(super) fn terminal_refusal_context_key(
        context: &TerminalRefusalPullContext,
    ) -> TerminalRefusalContextKey {
        (
            context.tenant_id.as_str().to_string(),
            context.hub_node_id.clone(),
        )
    }

    pub(super) fn terminal_refusal_marker_config_key(
        context: &TerminalRefusalPullContext,
        table: &str,
        natural_key: &NaturalKey,
    ) -> String {
        Self::terminal_refusal_marker_context_prefix(context)
            + "marker."
            + &Self::hex_component(&sync_identity_key(table, natural_key))
    }

    pub(super) fn terminal_refusal_scan_config_key(context: &TerminalRefusalPullContext) -> String {
        Self::terminal_refusal_marker_context_prefix(context) + "scan"
    }

    pub(super) fn terminal_refusal_marker_config_prefix(
        context: &TerminalRefusalPullContext,
    ) -> String {
        Self::terminal_refusal_marker_context_prefix(context) + "marker."
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn terminal_refusal_marker_is_current(
        &self,
        table: &str,
        natural_key: &NaturalKey,
        marker_lsn: Lsn,
        marker_row_id: RowId,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<bool> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let snapshot = self.snapshot();
        let no_deleted = HashSet::new();
        if let Some(current) = self.visible_row_by_natural_key(
            table,
            natural_key,
            snapshot,
            &no_deleted,
            read_memory.clone(),
        )? {
            return Ok(current.lsn == marker_lsn && current.row_id == marker_row_id);
        }
        // The marked local mutation was a delete. Require that the latest
        // durable tombstone for this natural key is still the exact row and
        // LSN we marked; a later local write/delete cannot consume it.
        Ok(self
            .change_log
            .read()
            .iter()
            .rev()
            .find_map(|entry| {
                let ChangeLogEntry::RowDelete {
                    table: tombstone_table,
                    row_id,
                    natural_key: tombstone_key,
                    lsn,
                } = entry
                else {
                    return None;
                };
                (tombstone_table == table && tombstone_key == natural_key)
                    .then_some((*lsn, *row_id))
            })
            .is_some_and(|(lsn, row_id)| lsn == marker_lsn && row_id == marker_row_id))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn terminal_refusal_marker_matches(
        &self,
        context: &TerminalRefusalPullContext,
        row: &RowChange,
        current: Option<&VersionedRow>,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<bool> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let key = Self::terminal_refusal_marker_key(context, &row.table, &row.natural_key);
        let in_memory_marker = self.terminal_refusal_markers.read().get(&key).cloned();
        let marker = match in_memory_marker {
            Some(marker) if marker.active && marker.generation == context.generation => {
                Some((Lsn(marker.lsn), RowId(marker.row_id)))
            }
            Some(_) => None,
            None => match &self.persistence {
                Some(persistence) => persistence
                    .load_config_value::<TerminalRefusalMarkerRecord>(
                        &Self::terminal_refusal_marker_config_key(
                            context,
                            &row.table,
                            &row.natural_key,
                        ),
                    )?
                    .filter(|record| record.active && record.generation == context.generation)
                    .filter(|record| {
                        record.table == row.table && record.natural_key == row.natural_key
                    })
                    .map(|record| (Lsn(record.lsn), RowId(record.row_id))),
                None => None,
            },
        };
        let Some((marker_lsn, marker_row_id)) = marker else {
            return Ok(false);
        };
        let current_matches = match current {
            Some(current) => marker_lsn == current.lsn && marker_row_id == current.row_id,
            None => self.terminal_refusal_marker_is_current(
                &row.table,
                &row.natural_key,
                marker_lsn,
                marker_row_id,
                read_memory.clone(),
            )?,
        };
        Ok(current_matches)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn has_terminal_refusal_markers(
        &self,
        context: &TerminalRefusalPullContext,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<bool> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let mut records =
            BTreeMap::<String, (TerminalRefusalMarkerRecord, TerminalRefusalMarkerKey)>::new();
        for (key, record) in self.terminal_refusal_markers.read().iter() {
            if key.0 == context.tenant_id.as_str() && key.1 == context.hub_node_id {
                records.insert(
                    Self::terminal_refusal_marker_config_key(
                        context,
                        &record.table,
                        &record.natural_key,
                    ),
                    (record.clone(), key.clone()),
                );
            }
        }
        if let Some(persistence) = &self.persistence {
            for (config_key, record) in persistence
                .load_config_values_with_prefix::<TerminalRefusalMarkerRecord>(
                    &Self::terminal_refusal_marker_config_prefix(context),
                )?
            {
                let key =
                    Self::terminal_refusal_marker_key(context, &record.table, &record.natural_key);
                records.insert(config_key, (record, key));
            }
        }
        let mut stale = Vec::new();
        let mut valid = false;
        for (config_key, (record, key)) in &records {
            if !record.active {
                continue;
            }
            if self.terminal_refusal_marker_is_current(
                &record.table,
                &record.natural_key,
                Lsn(record.lsn),
                RowId(record.row_id),
                read_memory.clone(),
            )? {
                valid = true;
            } else {
                stale.push((config_key.clone(), record.clone(), key.clone()));
            }
        }
        if !stale.is_empty() || !valid {
            if self.persistence.is_some() {
                let tx = self.begin()?;
                let writes = stale
                    .iter()
                    .map(|(config_key, record, _)| {
                        let mut inactive = record.clone();
                        inactive.active = false;
                        Ok((
                            config_key.clone(),
                            RedbPersistence::encode_config_value(&inactive)?,
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;
                self.tx_mgr.append_config_writes(tx, writes)?;
                self.commit_with_source(
                    tx,
                    CommitSource::SyncPull,
                    vector_copy_owner,
                    read_memory.clone(),
                )?;
            }
            let mut in_memory = self.terminal_refusal_markers.write();
            for (_, _, key) in stale {
                in_memory.remove(&key);
            }
        }
        Ok(valid)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn terminal_refusal_scan_state(
        &self,
        context: &TerminalRefusalPullContext,
    ) -> Result<Option<TerminalRefusalScanState>> {
        if let Some(state) = self
            .terminal_refusal_scans
            .read()
            .get(&Self::terminal_refusal_context_key(context))
            .cloned()
        {
            return Ok(Some(state));
        }
        let Some(persistence) = &self.persistence else {
            return Ok(None);
        };
        let state = persistence.load_config_value::<TerminalRefusalScanState>(
            &Self::terminal_refusal_scan_config_key(context),
        )?;
        if let Some(state) = state.as_ref() {
            self.terminal_refusal_scans
                .write()
                .insert(Self::terminal_refusal_context_key(context), state.clone());
        }
        Ok(state)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn persist_terminal_refusal_scan_state(
        &self,
        context: &TerminalRefusalPullContext,
        generation: u64,
        source: Incarnation,
        next_lsn: Lsn,
    ) -> Result<()> {
        let _operation = self.open_operation()?;
        // All lineage and coordination locks nest inside the commit boundary.
        // The stores inherit this guard and owe publication after it unlocks.
        let operation = || {
            if context.generation != generation {
                return Ok(());
            }
            let key = Self::terminal_refusal_context_key(context);
            let mut scans = self.terminal_refusal_scans.write();
            let Some(state) = scans.get_mut(&key) else {
                return Ok(());
            };
            if state.generation != generation {
                return Ok(());
            }
            let previous = state.clone();
            state.source = Some(source);
            state.next_lsn = next_lsn;
            state.active = true;
            let state = state.clone();
            if let Some(persistence) = &self.persistence
                && let Err(err) = self.publish_direct_metadata(|| {
                    persistence.flush_config_value(
                        &Self::terminal_refusal_scan_config_key(context),
                        &state,
                    )
                })
            {
                *scans.get_mut(&key).expect("scan state remains locked") = previous;
                return Err(err);
            }
            Ok(())
        };
        if self.tx_mgr.commit_boundary_held_by_current_thread() {
            operation()
        } else {
            self.tx_mgr.with_maintenance_publication(|_| operation())
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn clear_terminal_refusal_scan_state(
        &self,
        context: &TerminalRefusalPullContext,
        generation: u64,
    ) -> Result<()> {
        let _operation = self.open_operation()?;
        // All lineage and coordination locks nest inside the commit boundary.
        // The stores inherit this guard and owe publication after it unlocks.
        let operation = || {
            if context.generation != generation {
                return Ok(());
            }
            let key = Self::terminal_refusal_context_key(context);
            let mut scans = self.terminal_refusal_scans.write();
            let Some(state) = scans.get_mut(&key) else {
                return Ok(());
            };
            if state.generation != generation {
                return Ok(());
            }
            let previous = state.clone();
            state.source = None;
            state.next_lsn = Lsn(0);
            state.active = false;
            let state = state.clone();
            if let Some(persistence) = &self.persistence
                && let Err(err) = self.publish_direct_metadata(|| {
                    persistence.flush_config_value(
                        &Self::terminal_refusal_scan_config_key(context),
                        &state,
                    )
                })
            {
                *scans.get_mut(&key).expect("scan state remains locked") = previous;
                return Err(err);
            }
            Ok(())
        };
        if self.tx_mgr.commit_boundary_held_by_current_thread() {
            operation()
        } else {
            self.tx_mgr.with_maintenance_publication(|_| operation())
        }
    }

    /// Arm one fault at one point of taking delivery. It fires once and
    /// disarms itself.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn arm_received_unit_fault_for_test(
        point: ReceivedUnitFaultPoint,
        fault: ReceivedUnitCopyFault,
    ) {
        RECEIVED_UNIT_FAULT.with(|slot| slot.set(Some((point, fault))));
    }

    /// Clear a fault that was armed and never reached, so a later delivery on
    /// this thread runs undisturbed.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn disarm_received_unit_fault_for_test() {
        RECEIVED_UNIT_FAULT.with(|slot| slot.set(None));
    }

    /// How many prepared received-schema stages this database is holding. A
    /// delivery that ended, however it ended, holds none.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn received_schema_stage_count_for_test(&self) -> usize {
        self.received_schema_stages.lock().len()
    }

    /// Returns this database's latest committed acceptance position.
    ///
    /// One received atomic unit can reserve several ordered replay positions,
    /// so a successful apply can advance this value by more than one. Its
    /// interior positions are not separately committed snapshots.
    pub fn current_lsn(&self) -> Lsn {
        let _operation = self.assert_open_operation();
        if let Some(lsn) = CRON_LSN_OVERRIDE.with(|slot| *slot.borrow()) {
            return lsn;
        }
        self.tx_mgr.current_lsn()
    }

    /// Returns the highest-committed TxId on this database.
    pub fn committed_watermark(&self) -> TxId {
        let _operation = self.assert_open_operation();
        self.tx_mgr.current_tx_max()
    }

    /// Returns the next TxId the allocator will issue on this database.
    ///
    /// A transaction that left no trace never happened, and its id was never
    /// spent. The counter is durable only through committed work, so an id
    /// allocated by a transaction that committed nothing may be handed out
    /// again after a restart. Nothing observes that reuse: uncommitted work
    /// never syncs and is never served, so no reader, peer, or reopened store
    /// can hold the earlier id. Revisit this only if a surface ever exposes
    /// allocated-but-uncommitted ids, which would make the reuse visible.
    pub fn next_tx(&self) -> TxId {
        let _operation = self.assert_open_operation();
        self.tx_mgr.peek_next_tx()
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub fn pause_after_relational_apply_for_test(&self) -> ApplyPhasePauseGuard {
        let _operation = self.assert_open_operation();
        let inner = self.apply_phase_pause.clone();
        let generation = inner.arm();
        ApplyPhasePauseGuard { inner, generation }
    }

    /// Test-only: stop a received-schema apply after its complete staged
    /// projection (including sink queue entries) is durable, but before that
    /// projection replaces live memory. This exposes acknowledgement races
    /// without relying on dispatcher timing; production never arms the pause.
    #[cfg(any(test, feature = "test-seams"))]
    pub fn pause_before_received_schema_publish_for_test(&self) -> ApplyPhasePauseGuard {
        let _operation = self.assert_open_operation();
        ApplyPhasePauseGuard::arm_for_test(self.received_schema_pre_publish_pause.clone())
    }

    /// Arm a crash inside the one write transaction that installs a new
    /// `TableMeta`, on THIS thread. Reached from the dedicated crash child of
    /// `declaring_a_count_on_a_table_with_rows_is_atomic_and_charged`, which
    /// arms and writes on the same thread and aborts at the boundary; the
    /// seam itself stays `pub(crate)` in `persistence`.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn arm_table_meta_rewrite_crash_for_test(&self, boundary: u8) {
        crate::persistence::arm_table_meta_rewrite_crash_for_test(boundary);
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn pause_before_local_schema_durable_commit_for_test(&self) -> ApplyPhasePauseGuard {
        ApplyPhasePauseGuard::arm_for_test(self.local_schema_pre_commit_pause.clone())
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn pause_before_local_schema_publish_for_test(&self) -> ApplyPhasePauseGuard {
        ApplyPhasePauseGuard::arm_for_test(self.local_schema_pre_publish_pause.clone())
    }

    /// Fail this thread's next received-schema durable write immediately
    /// before its one Redb commit, after every physical and identity change
    /// of the unit is staged in that transaction.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn fail_next_received_schema_durable_commit_for_test(&self) {
        crate::persistence::arm_received_schema_pre_commit_fault_for_test();
    }

    /// Stop a local commit between its schema image and custody publication.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn pause_between_local_publication_components_for_test(&self) -> ApplyPhasePauseGuard {
        ApplyPhasePauseGuard::arm_for_test(self.local_schema_between_publication_pause.clone())
    }

    /// Stop after the complete received-schema write set is finalized and
    /// immediately before the durable store apply.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn pause_before_received_schema_durable_commit_for_test(&self) -> ApplyPhasePauseGuard {
        let _operation = self.assert_open_operation();
        ApplyPhasePauseGuard::arm_for_test(self.received_schema_pre_commit_pause.clone())
    }

    /// Inspect only already-published memory while a received-schema writer
    /// holds the commit lock before its durable commit. Each field is sampled
    /// under its own lock, which is released before the next field is read.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn published_state_snapshot_for_test(&self) -> PublishedStateSnapshotForTest {
        let table_names = self.relational_store.table_names();
        // A durable writer's declaration history lives only in its published
        // root; the in-memory log is the memory store's.
        let ddl_changes = self
            .ddl_log_entries_since_unlocked(
                Lsn(0),
                self.declared_reader_memory()
                    .expect("published state read owner"),
            )
            .expect("published declaration history")
            .into_iter()
            .map(|(_, change)| change)
            .collect();
        let (event_type_names, sink_names, route_names) =
            self.published_definition_names_for_test();
        let trigger_names = self.trigger.declarations.lock().keys().cloned().collect();
        let mut applied_push_watermarks: Vec<_> = self
            .in_memory_applied_push_watermarks
            .lock()
            .iter()
            .map(|(node, lsn)| (node.clone(), *lsn))
            .collect();
        applied_push_watermarks.sort_by(|a, b| a.0.cmp(&b.0));
        PublishedStateSnapshotForTest {
            table_names,
            ddl_changes,
            event_type_names,
            sink_names,
            route_names,
            trigger_names,
            applied_push_watermarks,
        }
    }

    /// Test-only: arm the sync-apply pre-commit checkpoint (see
    /// `sync_apply_pre_commit_pause`'s doc comment for the exact boundary --
    /// after every row/edge/vector mutation for a changeset has been
    /// staged, but immediately before the commit that assigns that
    /// changeset's own LSN; a DIFFERENT boundary from
    /// `pause_after_relational_apply_for_test`, which fires INSIDE an
    /// already-committing transaction, holding the commit lock). Call this
    /// on any thread (typically the test's main thread) to arm the shared
    /// mechanism, then call `Self::mark_this_thread_for_sync_apply_pre_
    /// commit_pause_for_test` on EACH thread that should actually stop
    /// there -- a thread that never marks itself passes this checkpoint
    /// unconditionally even while armed, so a test can pause exactly one
    /// concurrent apply while a second, unmarked apply on another thread
    /// commits normally.
    #[cfg(any(test, feature = "test-seams"))]
    pub fn pause_before_sync_apply_commit_for_test(&self) -> ApplyPhasePauseGuard {
        let _operation = self.assert_open_operation();
        let inner = self.sync_apply_pre_commit_pause.clone();
        let generation = inner.arm();
        ApplyPhasePauseGuard { inner, generation }
    }

    /// Pause export after its immutable snapshot point but before any artifact
    /// is published. This is a deterministic, production-dead test fence.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn pause_after_export_snapshot_capture_for_test(&self) -> ApplyPhasePauseGuard {
        let _operation = self.assert_open_operation();
        let inner = self.export_after_capture_pause.clone();
        let generation = inner.arm();
        ApplyPhasePauseGuard { inner, generation }
    }

    /// Test-only: mark the CALLING thread so its next sync-apply pre-commit
    /// checkpoint actually pauses once `pause_before_sync_apply_commit_for_
    /// test` has armed the shared mechanism. Thread-local and one-shot
    /// (consumed the moment the checkpoint reads it) -- must be called on
    /// the same thread that will perform the paused `apply_changes`/
    /// `apply_synced_changes` call.
    #[cfg(any(test, feature = "test-seams"))]
    pub fn mark_this_thread_for_sync_apply_pre_commit_pause_for_test(&self) {
        let _operation = self.assert_open_operation();
        SYNC_APPLY_PRE_COMMIT_PAUSE_ARMED_HERE.with(|f| f.set(true));
    }

    /// Test-only: pause a version-cleanup removal pass (`compact_currency_
    /// versions`/`compact_currency_versions_inner`) immediately after it
    /// samples its registered-snapshot set and committed watermark, before
    /// it applies any persisted removal -- see `SnapshotFloorRegistry::
    /// begin_removal_pass`. Lets a test prove a concurrent `pin_snapshot`
    /// call genuinely blocks while a pass is mid-flight, rather than racing
    /// past it.
    #[cfg(any(test, feature = "test-seams"))]
    pub fn pause_after_currency_floor_sample_for_test(&self) -> ApplyPhasePauseGuard {
        let _operation = self.assert_open_operation();
        let inner = self.snapshot_registry.removal_pass_test_pause.clone();
        let generation = inner.arm();
        ApplyPhasePauseGuard { inner, generation }
    }

    /// Observe the canonical removal-pass marker without changing its state.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn active_removal_pass_for_test(&self) -> bool {
        self.snapshot_registry.state.lock().active_pass.is_some()
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __arm_one_shot_vector_maintenance_preparation_pause_for_test(
        &self,
        partition: &VectorPartitionRef,
        phase: VectorMaintenancePreparationPhaseForTest,
    ) -> VectorLifecyclePauseHandle {
        let _operation = self.assert_open_operation();
        self.vector_store
            .arm_maintenance_preparation_pause_for_test(partition, phase)
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __arm_vector_journal_truncation_crash_for_test(
        &self,
        partition: &VectorPartitionRef,
        phase: VectorJournalTruncationPhaseForTest,
    ) -> VectorJournalTruncationFaultHandle {
        let _operation = self.assert_open_operation();
        self.vector_store
            .arm_journal_truncation_fault_for_test(partition, phase)
    }

    /// Pause between two committed steps of a vector journal cleanup, after
    /// the earlier step has committed and before the next one starts.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __arm_one_shot_vector_journal_cleanup_step_gap_pause_for_test(
        &self,
    ) -> ApplyPhasePauseGuard {
        let _operation = self.assert_open_operation();
        let inner = vector_journal_cleanup_step_gap_pause_for_test();
        let generation = inner.arm();
        ApplyPhasePauseGuard { inner, generation }
    }

    /// Pause after cleanup owns and charges its physical journal-key copies,
    /// while the durable deletion transaction is still uncommitted.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __arm_one_shot_vector_journal_cleanup_pause_for_test(&self) -> ApplyPhasePauseGuard {
        let _operation = self.assert_open_operation();
        let inner = vector_journal_cleanup_pause_for_test();
        let generation = inner.arm();
        ApplyPhasePauseGuard { inner, generation }
    }

    /// Pause while repair owns both the sampled snapshot Vec and the retained
    /// Arc slice, after their complete overlap was admitted and before the Vec
    /// is released.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __arm_one_shot_vector_repair_snapshot_sample_transition_pause_for_test(
        &self,
    ) -> ApplyPhasePauseGuard {
        let _operation = self.assert_open_operation();
        let inner = vector_repair_snapshot_sample_transition_pause_for_test();
        let generation = inner.arm();
        ApplyPhasePauseGuard { inner, generation }
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __try_global_commit_lock_for_test(&self) -> bool {
        let _operation = self.assert_open_operation();
        let manager = self.tx_mgr.clone();
        let (sent, received) = std::sync::mpsc::sync_channel(1);
        std::thread::spawn(move || {
            manager.with_commit_lock(|| {
                let _ = sent.send(());
            });
        });
        received
            .recv_timeout(std::time::Duration::from_millis(100))
            .is_ok()
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn __corrupt_vector_partition_journal_record_for_test(
        path: &Path,
        partition: &VectorPartitionRef,
    ) -> Result<()> {
        RedbPersistence::corrupt_vector_partition_journal_record_for_test(path, partition)
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn __corrupt_last_vector_partition_journal_record_for_test(
        path: &Path,
        partition: &VectorPartitionRef,
    ) -> Result<()> {
        RedbPersistence::corrupt_last_vector_partition_journal_record_for_test(path, partition)
    }

    /// Declared relational indexes whose physical table no longer matches the
    /// root its writer last published. Reads that consume an index's physical
    /// postings refuse such an index; a read that asks only the canonical
    /// tree still answers and records it here. Repair: recreate the store.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn index_root_damage(&self) -> Result<Vec<(String, String)>> {
        let _operation = self.open_operation()?;
        let Some(persistence) = self.persistence.as_ref() else {
            return Ok(Vec::new());
        };
        let indexes = self
            .relational_store
            .table_meta
            .read()
            .iter()
            .flat_map(|(table, meta)| {
                meta.indexes
                    .iter()
                    .map(|index| (table.clone(), index.name.clone()))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        persistence.index_root_damage(&indexes)
    }

    /// Indexes a canonical-only read of the store at `path`, in this process,
    /// found disagreeing with their writer's published root.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn observed_index_root_damage(path: impl AsRef<Path>) -> Result<Vec<(String, String)>> {
        let path = canonical_database_path(path.as_ref())?;
        Ok(crate::persistence::history_root_witness::observed_index_root_damage(&path))
    }

    /// Strict index-table root checks made on this thread. Every read that
    /// opens a physical index cursor, an ordered window or a maintained group
    /// makes one.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __strict_index_root_checks_for_test() -> u64 {
        crate::persistence::history_root_witness::strict_index_root_checks_for_test()
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub fn vector_store_for_test(&self) -> Arc<VectorStore> {
        let _operation = self.assert_open_operation();
        self.vector_store.clone()
    }

    /// Read the vector store's event receipt without inspecting cache state or
    /// performing lifecycle work itself.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __vector_passive_activity_counters_for_test(&self) -> VectorPassiveActivityCounters {
        let _operation = self.assert_open_operation();
        self.vector_store.passive_activity_counters_for_test()
    }

    /// Count of full candidate rows the vector search arm's candidate
    /// subtree has materialized for this database and its derived handles.
    /// Never a derived view of `rows_examined` or any residency counter.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __candidate_derivation_counters_for_test(&self) -> u64 {
        let _operation = self.assert_open_operation();
        self.candidate_rows_materialized.load(Ordering::SeqCst)
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __reset_passive_disclosure_work_for_test(&self) {
        let _operation = self.assert_open_operation();
        PASSIVE_DISCLOSURE_HIDDEN_WORK.with(|work| work.set((0, 0)));
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __passive_disclosure_work_for_test(&self) -> (u64, u64) {
        let _operation = self.assert_open_operation();
        PASSIVE_DISCLOSURE_HIDDEN_WORK.with(std::cell::Cell::get)
    }

    /// Record actual candidate row materialization: once per stored/staged
    /// row in the shared kernel, or for the legacy candidate result batch.
    /// Identity-only membership reads do not increment this counter.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __bump_candidate_rows_materialized_for_test(&self, count: u64) {
        let _operation = self.assert_open_operation();
        self.candidate_rows_materialized
            .fetch_add(count, Ordering::SeqCst);
    }

    /// Pause the next real maintenance build for this partition after it has
    /// traversed `after_vectors` stored vectors. Dropping the returned guard
    /// releases a reached worker, so an assertion panic cannot strand it.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __arm_vector_maintenance_progress_pause_for_test(
        &self,
        partition: &VectorPartitionRef,
        after_vectors: usize,
    ) -> VectorMaintenanceProgressPauseHandle {
        let _operation = self.assert_open_operation();
        self.vector_store
            .arm_maintenance_progress_pause_for_test(partition, after_vectors)
    }

    /// Subscribe to commit events. Returns a receiver that yields a `CommitEvent`
    /// after each commit.
    pub fn subscribe(&self) -> Receiver<CommitEvent> {
        let _operation = self.assert_open_operation();
        self.subscribe_with_capacity(DEFAULT_SUBSCRIPTION_CAPACITY)
    }

    /// Subscribe with a custom channel capacity.
    pub fn subscribe_with_capacity(&self, capacity: usize) -> Receiver<CommitEvent> {
        let _operation = self.assert_open_operation();
        let (tx, rx) = mpsc::sync_channel(capacity.max(1));
        self.subscriptions.lock().subscribers.push(tx);
        rx
    }

    /// Returns health metrics for the subscription system.
    pub fn subscription_health(&self) -> SubscriptionMetrics {
        let _operation = self.assert_open_operation();
        let subscriptions = self.subscriptions.lock();
        SubscriptionMetrics {
            active_channels: subscriptions.subscribers.len(),
            events_sent: subscriptions.events_sent,
            events_dropped: subscriptions.events_dropped,
        }
    }

    /// Marks trigger registration complete after all declared callbacks have
    /// been registered.
    ///
    /// Trigger callbacks use the canonical callback contract documented on
    /// [`Error::CallbackActiveCrossThread`]. Same-DB cross-thread writers wait
    /// and proceed; unrelated cross-DB writers proceed independently;
    /// same-thread callback reentry returns [`Error::CallbackReentry`].
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn complete_initialization(&self) -> Result<()> {
        let _operation = self.open_operation()?;
        let declarations = self.trigger.declarations.lock();
        let callbacks = self.trigger.callbacks.read();
        for name in declarations.keys() {
            if !callbacks.contains_key(name) {
                return Err(Error::TriggerCallbackMissing {
                    trigger_name: name.clone(),
                });
            }
        }
        self.trigger.ready.store(true, Ordering::SeqCst);
        Ok(())
    }
}
