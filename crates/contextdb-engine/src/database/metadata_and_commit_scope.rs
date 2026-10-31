use super::*;

impl DeclaredReadRefusal {
    /// What the refused reader is told, in the words it needs to declare
    /// something this writer can serve -- or nothing at all: both identities,
    /// named as identities rather than dumped as Rust values.
    pub(crate) fn stated(&self) -> String {
        match self {
            Self::PrincipalConflict { declared, serving } => format!(
                "this session declared it reads as {}, and the writer holding this store reads \
                 as {}",
                identity_wording(declared),
                identity_wording(serving)
            ),
        }
    }
}

/// One identity in the words a person reads.
pub(super) fn identity_wording(principal: &Principal) -> String {
    match principal {
        Principal::System => "the system identity".to_owned(),
        Principal::Agent(name) => format!("the agent {name}"),
        Principal::Human(name) => format!("the person {name}"),
    }
}

pub(super) fn narrowed_constraint_set<T: Ord + Clone>(
    parent: &Option<BTreeSet<T>>,
    child: Option<BTreeSet<T>>,
) -> Option<BTreeSet<T>> {
    match (parent, child) {
        (Some(parent), Some(child)) => Some(parent.intersection(&child).cloned().collect()),
        (Some(parent), None) => Some(parent.clone()),
        (None, child) => child,
    }
}
impl GraphRetainedBytes for AdjEntry {
    fn retained_graph_bytes(&self) -> usize {
        self.estimated_bytes()
    }
}
impl GraphRetainedBytes for contextdb_graph::GraphAdjEntry {
    fn retained_graph_bytes(&self) -> usize {
        self.estimated_bytes()
    }
}
impl Drop for LegacyWriteSetCheckpoint<'_> {
    fn drop(&mut self) {
        if self.active {
            let _ = self
                .database
                .tx_mgr
                .with_write_set(self.tx, |ws| ws.ordered_staged_rows.settle_checkpoint());
            self.database.settle_legacy_row_graph_journal(self.tx);
        }
    }
}

impl ReceivedSchemaStage {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn copy_operational_from(
        &mut self,
        transactions: &DatabaseTransactionHost,
        tx: TxId,
        original: &OwnedMemoryReservation,
    ) -> Result<WriteSet> {
        if self.operational_copy_admission.is_some() {
            return Err(Error::Other(
                "received operational image already owns its admission".into(),
            ));
        }
        #[expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )]
        fn add(a: usize, b: usize) -> Result<usize> {
            a.checked_add(b)
                .ok_or_else(|| Error::Other("operational copy size overflow".into()))
        }
        #[expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )]
        fn array<T>(len: usize) -> Result<usize> {
            std::alloc::Layout::array::<T>(len)
                .map(|layout| layout.size())
                .map_err(|_| Error::Other("operational copy size overflow".into()))
        }
        let destination = self.write_set.get_mut();
        let quote = |source: &WriteSet| -> Result<usize> {
            let mut bytes = add(
                array::<(String, VersionedRow)>(source.relational_inserts.len())?,
                array::<(String, RowId, TxId)>(source.relational_deletes.len())?,
            )?;
            if destination
                .relational_inserts
                .len()
                .saturating_add(source.relational_inserts.len())
                > destination.relational_inserts.capacity()
            {
                bytes = add(
                    bytes,
                    array::<(String, VersionedRow)>(add(
                        destination.relational_inserts.len(),
                        source.relational_inserts.len(),
                    )?)?,
                )?;
            }
            if destination
                .relational_deletes
                .len()
                .saturating_add(source.relational_deletes.len())
                > destination.relational_deletes.capacity()
            {
                bytes = add(
                    bytes,
                    array::<(String, RowId, TxId)>(add(
                        destination.relational_deletes.len(),
                        source.relational_deletes.len(),
                    )?)?,
                )?;
            }
            for (table, row) in &source.relational_inserts {
                let body = add(
                    table.len(),
                    crate::memory_accounting::preparation_row_copy_bytes(row)?,
                )?;
                // One operational image and one stage-terminal copy.
                bytes = add(add(bytes, body)?, body)?;
            }
            for (table, _, _) in &source.relational_deletes {
                bytes = add(add(bytes, table.len())?, table.len())?;
            }
            Ok(bytes)
        };
        let bytes = transactions.with_ordinary_read(tx, quote)??;
        let admission = crate::memory_accounting::reserve_preparation_from_original(
            original,
            bytes,
            "sync",
            "received_operational_copy",
            "Raise MEMORY_LIMIT to admit received operational rows.",
        )?;
        let operational = transactions.with_ordinary_read(tx, |source| {
            if quote(source)? > bytes {
                return Err(Error::Other(
                    "operational source changed during preparation".into(),
                ));
            }
            let mut result = WriteSet {
                relational_inserts: Vec::with_capacity(source.relational_inserts.len()),
                relational_deletes: Vec::with_capacity(source.relational_deletes.len()),
                commit_lsn: source.commit_lsn,
                finalized_tx: source.finalized_tx,
                visibility_floor: source.visibility_floor,
                ..WriteSet::default()
            };
            for (table, row) in &source.relational_inserts {
                result.relational_inserts.push((
                    table.as_str().to_owned(),
                    crate::memory_accounting::preparation_copy_row(row)?,
                ));
            }
            for (table, row, tx) in &source.relational_deletes {
                result
                    .relational_deletes
                    .push((table.as_str().to_owned(), *row, *tx));
            }
            Ok::<_, Error>(result)
        })??;
        // All fallible source copies completed before growing the destination.
        // Exact reserve prevents unpriced geometric control growth at fusion.
        destination
            .relational_inserts
            .reserve_exact(operational.relational_inserts.len());
        destination
            .relational_deletes
            .reserve_exact(operational.relational_deletes.len());
        self.operational_copy_admission = Some(admission);
        Ok(operational)
    }

    /// Build a separate native terminal image while the complete stage and its
    /// original capture invoices remain alive. These are newly admitted bodies,
    /// never a relabeling of the stage's legacy numeric byte debit.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn prepare_native_adoption(
        &self,
        guard: &contextdb_tx::manager::EngineGuard<'_>,
        original: &OwnedMemoryReservation,
        funder: &contextdb_tx::SourceFunder,
    ) -> Result<contextdb_tx::manager::PreparedSourceAdoption> {
        let source = self.write_set.lock();
        guard.prepare_general_adoption(|target| {
            let flags = target.prepare_flags(contextdb_tx::manager::EngineFlags {
                commit_lsn: source.commit_lsn,
                finalized_tx: *guard.read().finalized_tx,
                requires_commit_lsn: source.requires_commit_lsn,
                visibility_floor: source.visibility_floor,
                propagation_in_progress: source.propagation_in_progress,
            })?;
            target.publish_tx(flags);
            for (table, row) in &source.relational_inserts {
                let meta = self
                    .durable_projection
                    .table_meta
                    .get(table)
                    .or_else(|| self.durable_projection.operational_table_meta.get(table))
                    .or_else(|| {
                        self.durable_projection
                            .authored_pending_rows
                            .iter()
                            .find(|(authored_table, _, authored)| {
                                authored_table == table
                                    && authored.row_id == row.row_id
                                    && authored.created_tx == row.created_tx
                                    && authored.lsn == row.lsn
                                    && authored.deleted_tx == row.deleted_tx
                                    && authored.created_at == row.created_at
                                    && authored.values == row.values
                            })
                            .map(|(_, shape, _)| &shape.meta)
                    })
                    .ok_or_else(|| {
                        Error::Other(
                            "received native row lacks its exact prepared declaration".into(),
                        )
                    })?;
                let admission = crate::memory_accounting::reserve_preparation_from_original(
                    original,
                    crate::memory_accounting::preparation_row_copy_bytes(row)?,
                    "relational",
                    "received_terminal_copy",
                    "Raise MEMORY_LIMIT to admit received rows.",
                )?;
                let mut body = OriginalDatabaseRow {
                    row: crate::memory_accounting::preparation_copy_row(row)?,
                    admission,
                };
                let invoice = body.admission.into_source_row_invoice_for(funder)?;
                let mutation = target.prepare_row_from_meta(table, body.row, meta, invoice)?;
                target.publish(mutation);
            }
            struct OriginalVector {
                body: VectorEntry,
                admission: OwnedMemoryReservation,
            }
            for entry in &source.vector_inserts {
                let admission = crate::memory_accounting::reserve_preparation_from_original(
                    original,
                    crate::memory_accounting::preparation_vector_copy_bytes(entry)?,
                    "vector",
                    "received_terminal_copy",
                    "Raise MEMORY_LIMIT to admit received vectors.",
                )?;
                let mut owned = OriginalVector {
                    body: crate::memory_accounting::preparation_copy_vector(entry)?,
                    admission,
                };
                let invoice = owned.admission.into_source_vector_invoice_for(funder)?;
                let mutation = target.prepare_append_vector_slot(owned.body, None, invoice)?;
                target.publish(mutation);
            }
            use contextdb_tx::staged_source::AuxBorrow;
            for input in [
                AuxBorrow::RelationalDeletes(&source.relational_deletes),
                AuxBorrow::Predicates(&source.relational_delete_predicates),
                AuxBorrow::AdjInserts(&source.adj_inserts),
                AuxBorrow::AdjDeletes(&source.adj_deletes),
                AuxBorrow::VectorDeletes(&source.vector_deletes),
                AuxBorrow::VectorMoves(&source.vector_moves),
                AuxBorrow::Provenance(&source.sync_source_provenance_marks),
                AuxBorrow::ConfigWrites(&source.config_writes),
                AuxBorrow::ConfigDeletes(&source.config_deletes),
                AuxBorrow::ConfigMaxU64Keys(&source.config_max_u64_keys),
            ] {
                if input.is_empty() {
                    continue;
                }
                let batch = target.copy_aux(input)?;
                let mutation = target.prepare_aux_splice(0, &[], batch)?;
                target.publish_tx(mutation);
            }
            let coordinates = target.prepare_source_coordinates(
                &source.relational_insert_source_lsns,
                &source.relational_insert_source_kinds,
            )?;
            target.publish_tx(coordinates);
            Ok(())
        })
    }
}

impl EngineDatabaseTransactions {
    pub(super) fn with_metadata_publication_scope<R>(&self, f: impl FnOnce() -> R) -> R {
        match self.persistent_store_address {
            Some(store) => crate::persistent_store::with_owned_metadata_publication_scope(store, f),
            None => f(),
        }
    }

    pub(super) fn with_terminal_apply_scope<R>(&self, f: impl FnOnce() -> R) -> R {
        match self.persistent_store_address {
            Some(store) => crate::persistent_store::with_owned_terminal_apply_scope(store, || {
                crate::persistent_store::with_owned_metadata_publication_scope(store, f)
            }),
            None => f(),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn bind_terminal_apply(
        &self,
        guard: &contextdb_tx::manager::EngineGuard<'_>,
        identity: contextdb_tx::manager::EngineTerminalIdentity,
    ) -> Result<()> {
        if let Some(store) = self.persistent_store_address {
            let lsn = (*guard.read().commit_lsn)
                .ok_or_else(|| Error::Other("native terminal has no assigned LSN".into()))?;
            crate::persistent_store::bind_owned_terminal_apply(store, lsn, identity)?;
        }
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn rollback_terminal(&self, tx: TxId) -> Result<OwnedDatabaseTerminal> {
        self.transactions.with_transactions(|lower| {
            lower.rollback_owned_with_peer(
                tx,
                |identity| self.metadata.validate_terminal(identity),
                ValidatedMetadataTerminal::consume_nonfallible,
            )
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn rollback_empty_terminal(&self, tx: TxId) -> Result<OwnedDatabaseTerminal> {
        self.transactions.with_transactions(|lower| {
            lower.rollback_empty_owned_with_peer(
                tx,
                |identity| self.metadata.validate_terminal(identity),
                ValidatedMetadataTerminal::consume_nonfallible,
            )
        })
    }

    pub(super) fn drain_terminals(&self) -> OwnedDatabaseDrain {
        self.transactions.with_transactions(|lower| {
            lower.drain_owned_with_peer(
                |sources| self.metadata.validate_drain(sources),
                ValidatedMetadataDrain::consume_nonfallible,
                OwnedTerminalMetadataChain::take_front_nonfallible,
            )
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn commit_reserved_terminal(
        &self,
        tx: TxId,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        before_commit: impl FnOnce(Lsn) -> Result<()>,
        prepare: impl for<'g, 'h, 'p> FnOnce(
            &'g mut contextdb_tx::manager::EngineGuard<'h>,
            &'p mut OwnedTerminalMetadata,
        ) -> Result<()>,
    ) -> std::result::Result<(Lsn, OwnedDatabaseTerminal), OwnedDatabaseTerminalFailure> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let identity = std::cell::Cell::new(None);
        self.with_terminal_apply_scope(|| {
            self.transactions.with_transactions(|lower| {
                lower.commit_owned_with_reserved_lsn_callback_with_peer(
                    tx,
                    read_memory,
                    before_commit,
                    |guard, peer| {
                        self.bind_terminal_apply(
                            guard,
                            identity
                                .get()
                                .ok_or_else(EngineMetadataJournal::identity_error)?,
                        )?;
                        prepare(guard, peer)
                    },
                    |terminal| {
                        identity.set(Some(terminal));
                        self.metadata.validate_terminal(terminal)
                    },
                    ValidatedMetadataTerminal::consume_nonfallible,
                )
            })
        })
    }

    /// Consume the actual producer debit through this host's original funder.
    /// Validation and the caller's row identity/time selection precede this
    /// door; native preparation retains sealed and distinct ordinary bodies.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn stage_original_row(
        &self,
        store: Arc<RelationalStore>,
        tx: TxId,
        table: &str,
        row: VersionedRow,
        admission: OwnedMemoryReservation,
    ) -> Result<RowId> {
        let mut original = OriginalDatabaseRow { row, admission };
        let invoice = original
            .admission
            .into_source_row_invoice_for(&self.funder)?;
        let relational = MemRelationalExecutor::new_engine(store, self.transactions.clone());
        relational.stage_engine_row(tx, table, original.row, invoice)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn try_new(
        transactions: EngineTransactionHandle<DynStore>,
        accountant: &crate::memory_accounting::AccountantHandle,
        events: event_bus::OwnedEventBusHandle,
        persistent_store_address: Option<usize>,
        funder: contextdb_tx::SourceFunder,
    ) -> Result<Self> {
        if !transactions.matches_funder(&funder) {
            return Err(Error::InvalidStateTransition(
                "engine database host requires the lower handle's original source funder".into(),
            ));
        }
        events.with_owned_persistence_original(|original| {
            crate::memory_accounting::validate_owned_original(original, accountant, &funder)
        })?;
        Ok(Self {
            transactions,
            metadata: EngineMetadataJournal {
                events: events.clone(),
                records: Mutex::new(None),
            },
            events,
            persistent_store_address,
            funder,
        })
    }
}

impl EngineDatabaseTransactions {
    #[allow(clippy::result_large_err)]
    pub(super) fn commit_interval_terminal_scoped(
        &self,
        tx: TxId,
        positions: u64,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        requires_commit: impl FnOnce() -> Result<bool>,
        active_prepare: impl FnOnce(
            &DatabaseCommitScope<'_, '_, '_>,
            Lsn,
            Lsn,
            TxId,
        ) -> PreparationResult<()>,
        prepare: impl for<'g, 'h, 'p> FnOnce(
            &'g mut contextdb_tx::manager::EngineGuard<'h>,
            &'p mut OwnedTerminalMetadata,
            contextdb_tx::manager::EngineTerminalIdentity,
        ) -> Result<()>,
        after_apply: impl for<'w> FnOnce(Lsn, &'w WriteSet),
    ) -> std::result::Result<(Lsn, OwnedDatabaseTerminal), OwnedDatabaseTerminalFailure> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.commit_interval_terminal_scoped_if(
            tx,
            positions,
            read_memory,
            || Ok(true),
            requires_commit,
            active_prepare,
            prepare,
            after_apply,
        )
        .map(|result| result.expect("unconditional engine terminal preflight"))
    }

    #[allow(clippy::result_large_err)]
    pub(super) fn commit_interval_terminal_scoped_if(
        &self,
        tx: TxId,
        positions: u64,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        preflight: impl FnOnce() -> Result<bool>,
        requires_commit: impl FnOnce() -> Result<bool>,
        active_prepare: impl FnOnce(
            &DatabaseCommitScope<'_, '_, '_>,
            Lsn,
            Lsn,
            TxId,
        ) -> PreparationResult<()>,
        prepare: impl for<'g, 'h, 'p> FnOnce(
            &'g mut contextdb_tx::manager::EngineGuard<'h>,
            &'p mut OwnedTerminalMetadata,
            contextdb_tx::manager::EngineTerminalIdentity,
        ) -> Result<()>,
        after_apply: impl for<'w> FnOnce(Lsn, &'w WriteSet),
    ) -> std::result::Result<Option<(Lsn, OwnedDatabaseTerminal)>, OwnedDatabaseTerminalFailure>
    {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let result = self.with_terminal_apply_scope(|| {
            self.transactions.with_transactions(|lower| {
                lower.commit_owned_private_lsn_interval_with_peer_scoped_if(
                    tx,
                    positions,
                    read_memory,
                    preflight,
                    requires_commit,
                    |scope, start, lsn, visibility_tx| {
                        active_prepare(
                            &DatabaseCommitScope {
                                host: self,
                                lower: scope,
                            },
                            start,
                            lsn,
                            visibility_tx,
                        )
                    },
                    |guard, peer: &mut OwnedTemporaryMetadata| {
                        self.bind_terminal_apply(guard, peer.identity)?;
                        prepare(guard, &mut peer.original, peer.identity)
                    },
                    after_apply,
                    |identity| {
                        let terminal = self.metadata.validate_terminal(identity);
                        let original_present =
                            EngineMetadataJournal::record(&terminal.records, identity.original_tx)
                                .is_some();
                        ValidatedTemporaryMetadata {
                            terminal,
                            original_present,
                        }
                    },
                    consume_temporary_nonfallible,
                )
            })
        });
        result
            .map(|result| result.map(|(lsn, pair)| (lsn, pair.map_peer(unwrap_temporary_peer))))
            .map_err(|failure| failure.map_peer(unwrap_temporary_peer))
    }
}

impl EngineDatabaseHandle {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn try_new(
        host: EngineDatabaseTransactions,
        funder: &contextdb_tx::SourceFunder,
    ) -> Result<Self> {
        if !host.transactions.matches_funder(&host.funder)
            || !host.transactions.matches_funder(funder)
        {
            return Err(Error::InvalidStateTransition(
                "engine database handle requires one matching original source funder".into(),
            ));
        }
        // The charge is admitted while `host` remains a stack value.  A
        // refusal therefore drops the complete lower facade and metadata
        // before its temporary invoice is released.
        let credit = contextdb_tx::SourceCharge::admit_funded(
            funder.clone(),
            contextdb_tx::staged_source::source_arc_layout::<EngineDatabaseTransactions>().size(),
        )?;
        Ok(Self {
            data: Some(Arc::new(host)),
            credit: Some(credit),
        })
    }

    pub(super) fn with_host<'h, R>(
        &'h self,
        f: impl FnOnce(&'h EngineDatabaseTransactions) -> R,
    ) -> R {
        f(self.data.as_deref().expect("live engine database handle"))
    }

    pub(super) fn identity(&self) -> usize {
        self.with_host(|host| host.transactions.identity())
    }

    pub(super) fn downgrade(&self) -> EngineDatabaseWeak {
        // Clone credit before making the matching native weak count visible.
        let credit = self
            .credit
            .as_ref()
            .expect("live engine database credit")
            .clone();
        let data = Arc::downgrade(self.data.as_ref().expect("live engine database handle"));
        EngineDatabaseWeak {
            data: Some(data),
            credit: Some(credit),
        }
    }
}

impl Clone for EngineDatabaseHandle {
    fn clone(&self) -> Self {
        let credit = self
            .credit
            .as_ref()
            .expect("live engine database credit")
            .clone();
        let data = self
            .data
            .as_ref()
            .expect("live engine database handle")
            .clone();
        Self {
            data: Some(data),
            credit: Some(credit),
        }
    }
}

impl Drop for EngineDatabaseHandle {
    fn drop(&mut self) {
        drop(self.data.take());
        drop(self.credit.take());
    }
}

impl EngineDatabaseWeak {
    pub(super) fn upgrade(&self) -> Option<EngineDatabaseHandle> {
        // The tentative invoice is intentionally acquired before upgrade.  On
        // failure it simply drops; on success it accompanies the new count.
        let credit = self
            .credit
            .as_ref()
            .expect("live engine database credit")
            .clone();
        let data = self
            .data
            .as_ref()
            .expect("live engine database weak")
            .upgrade()?;
        Some(EngineDatabaseHandle {
            data: Some(data),
            credit: Some(credit),
        })
    }
}

impl Clone for EngineDatabaseWeak {
    fn clone(&self) -> Self {
        let credit = self
            .credit
            .as_ref()
            .expect("live engine database credit")
            .clone();
        let data = self
            .data
            .as_ref()
            .expect("live engine database weak")
            .clone();
        Self {
            data: Some(data),
            credit: Some(credit),
        }
    }
}

impl Drop for EngineDatabaseWeak {
    fn drop(&mut self) {
        drop(self.data.take());
        drop(self.credit.take());
    }
}
impl OriginalEpochInputs {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn new(original: &OwnedMemoryReservation) -> Result<Self> {
        Ok(Self {
            values: Vec::new(),
            owner: crate::memory_accounting::reserve_preparation_from_original(
                original,
                0,
                "relational",
                "vector_schema_epochs",
                "Reduce staged metadata or raise MEMORY_LIMIT.",
            )?,
            control_bytes: 0,
        })
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn push_index(
        &mut self,
        original: &OwnedMemoryReservation,
        index: &VectorIndexRef,
    ) -> Result<()> {
        if self.values.len() == self.values.capacity() {
            let capacity = self
                .values
                .capacity()
                .checked_mul(2)
                .ok_or_else(EngineMetadataJournal::identity_error)?
                .max(1);
            let bytes = std::alloc::Layout::array::<(VectorIndexRef, u64)>(capacity)
                .map_err(|_| EngineMetadataJournal::identity_error())?
                .size();
            let mut credit = crate::memory_accounting::reserve_preparation_from_original(
                original,
                bytes,
                "relational",
                "vector_schema_epochs",
                "Reduce staged metadata or raise MEMORY_LIMIT.",
            )?;
            let mut replacement = Vec::with_capacity(capacity);
            if replacement.capacity() != capacity {
                return Err(EngineMetadataJournal::identity_error());
            }
            crate::memory_accounting::merge_preparation_original(&mut self.owner, &mut credit)?;
            replacement.append(&mut self.values);
            drop(std::mem::replace(&mut self.values, replacement));
            crate::memory_accounting::release_preparation_original(
                &mut self.owner,
                self.control_bytes,
            );
            self.control_bytes = bytes;
        }
        let bytes = index
            .table
            .len()
            .checked_add(index.column.len())
            .ok_or_else(EngineMetadataJournal::identity_error)?;
        crate::memory_accounting::grow_preparation_from_original(
            &mut self.owner,
            bytes,
            "relational",
            "vector_schema_epochs",
            "Reduce staged metadata or raise MEMORY_LIMIT.",
        )?;
        let index = VectorIndexRef {
            table: crate::memory_accounting::preparation_copy_string(&index.table)?,
            column: crate::memory_accounting::preparation_copy_string(&index.column)?,
        };
        self.values.push((index, 0));
        Ok(())
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn sort_dedup(&mut self) -> Result<()> {
        self.values
            .sort_unstable_by(|a, b| a.0.table.cmp(&b.0.table).then(a.0.column.cmp(&b.0.column)));
        let removed = self
            .values
            .windows(2)
            .filter(|pair| pair[0].0 == pair[1].0)
            .try_fold(0usize, |bytes, pair| {
                bytes
                    .checked_add(pair[1].0.table.len())
                    .and_then(|bytes| bytes.checked_add(pair[1].0.column.len()))
                    .ok_or_else(EngineMetadataJournal::identity_error)
            })?;
        self.values.dedup_by(|a, b| a.0 == b.0);
        crate::memory_accounting::release_preparation_original(&mut self.owner, removed);
        Ok(())
    }
}
impl Drop for NativeLineageGenerations {
    fn drop(&mut self) {
        let mut entries = self.entries.get_mut().take();
        while let Some(mut entry) = entries {
            entries = entry.get_mut().next.take();
            drop(entry);
        }
    }
}
impl<T> AdmittedMetadataValue<T> {
    pub(super) fn new(body: T, invoice: OwnedMemoryReservation) -> Self {
        Self {
            body: Some(body),
            invoice: Some(invoice),
        }
    }
}
impl<T> Drop for AdmittedMetadataValue<T> {
    fn drop(&mut self) {
        drop(self.body.take());
        drop(self.invoice.take());
    }
}
impl<T> MetadataBuffer<T> {
    pub(super) fn empty() -> Self {
        Self {
            values: Vec::new(),
            credit: None,
        }
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn prepare(capacity: usize, original: &OwnedMemoryReservation) -> Result<Self> {
        let layout = std::alloc::Layout::array::<T>(capacity)
            .map_err(|_| EngineMetadataJournal::identity_error())?;
        let credit = if layout.size() == 0 {
            None
        } else {
            Some(crate::memory_accounting::reserve_preparation_from_original(
                original,
                layout.size(),
                "relational",
                "mutation_checkpoint",
                "Reduce staged metadata or raise MEMORY_LIMIT.",
            )?)
        };
        let values = Vec::with_capacity(capacity);
        if std::mem::size_of::<T>() != 0 && values.capacity() != capacity {
            drop(values);
            return Err(EngineMetadataJournal::identity_error());
        }
        Ok(Self { values, credit })
    }
    pub(super) fn push(&mut self, value: T) {
        assert!(
            self.values.len() < self.values.capacity(),
            "prepared metadata capacity"
        );
        self.values.push(value);
    }
    pub(super) fn move_into(&mut self, target: &mut Self) {
        assert!(
            target.values.capacity() - target.values.len() >= self.values.len(),
            "preserved metadata capacity"
        );
        target.values.append(&mut self.values);
    }
}

impl<T> Drop for MetadataBuffer<T> {
    fn drop(&mut self) {
        drop(std::mem::take(&mut self.values));
        drop(self.credit.take());
    }
}

impl<T> MetadataBuffer<AdmittedMetadataValue<T>> {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn prepare_one(
        body: T,
        invoice: OwnedMemoryReservation,
        original: &OwnedMemoryReservation,
    ) -> Result<Self> {
        let body = AdmittedMetadataValue::new(body, invoice);
        let mut buffer = Self::prepare(1, original)?;
        buffer.push(body);
        Ok(buffer)
    }
}
impl EnginePendingCommitMetadata {
    pub(super) fn empty() -> Self {
        Self {
            discard_selections: MetadataBuffer::empty(),
            delivery_registrations: MetadataBuffer::empty(),
            conditional_update_guards: MetadataBuffer::empty(),
            sync_lineage_guards: MetadataBuffer::empty(),
            upsert_intents: MetadataBuffer::empty(),
            vector_schema_epochs: None,
            lineage_generations: None,
        }
    }
}
impl<T> MetadataSplice<T> {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn prepare(
        at: usize,
        selections: &[usize],
        inserted: MetadataBuffer<AdmittedMetadataValue<T>>,
        original_len: usize,
        original_capacity: usize,
        original: &OwnedMemoryReservation,
    ) -> Result<Self> {
        if selections.iter().any(|position| *position >= original_len)
            || selections
                .windows(2)
                .any(|positions| positions[0] >= positions[1])
        {
            return Err(EngineMetadataJournal::identity_error());
        }
        let remaining = original_len
            .checked_sub(selections.len())
            .ok_or_else(EngineMetadataJournal::identity_error)?;
        if at > remaining {
            return Err(EngineMetadataJournal::identity_error());
        }
        let installed = inserted.values.len();
        let new_len = remaining
            .checked_add(installed)
            .ok_or_else(EngineMetadataJournal::identity_error)?;
        let mut selection_buffer = MetadataBuffer::prepare(selections.len(), original)?;
        for position in selections {
            selection_buffer.push(*position);
        }
        let removed = MetadataBuffer::prepare(selections.len(), original)?;
        let storage = if new_len > original_capacity {
            Some(MetadataBuffer::prepare(new_len, original)?)
        } else {
            None
        };
        Ok(Self {
            at,
            installed,
            original_len,
            original_capacity,
            selections: selection_buffer,
            inserted,
            removed,
            storage,
        })
    }
    pub(super) fn validate(&self, field: &MetadataBuffer<AdmittedMetadataValue<T>>) {
        assert!(
            field.values.len() == self.original_len
                && field.values.capacity() == self.original_capacity
                && self.inserted.values.len() == self.installed
                && self.removed.values.is_empty(),
            "prepared metadata image"
        );
        assert!(
            self.selections
                .values
                .iter()
                .all(|position| *position < self.original_len)
                && self
                    .selections
                    .values
                    .windows(2)
                    .all(|positions| positions[0] < positions[1]),
            "prepared metadata selections"
        );
        let remaining = self.original_len - self.selections.values.len();
        let required = remaining
            .checked_add(self.installed)
            .expect("prepared metadata length");
        assert!(self.at <= remaining, "prepared metadata insertion position");
        let capacity = self
            .storage
            .as_ref()
            .map_or(self.original_capacity, |buffer| {
                assert!(
                    buffer.values.is_empty() && buffer.values.capacity() >= self.original_len,
                    "prepared metadata growth"
                );
                buffer.values.capacity()
            });
        assert!(
            capacity >= required
                && self
                    .inserted
                    .values
                    .iter()
                    .all(|value| value.body.is_some() && value.invoice.is_some()),
            "prepared metadata bodies and capacity"
        );
    }
    pub(super) fn publish(&mut self, field: &mut MetadataBuffer<AdmittedMetadataValue<T>>) {
        if let Some(mut working) = self.storage.take() {
            field.move_into(&mut working);
            self.storage = Some(std::mem::replace(field, working));
        }
        // Original positions descend on removal and ascend on inverse replay.
        for position in self.selections.values.iter().rev() {
            self.removed
                .push((*position, field.values.remove(*position)));
        }
        for (offset, value) in self.inserted.values.drain(..).enumerate() {
            field.values.insert(self.at + offset, value);
        }
    }
    pub(super) fn validate_restore(&self, field: &mut (usize, usize)) {
        let (len, capacity) = *field;
        assert!(
            self.at <= len && self.installed <= len - self.at,
            "installed metadata splice range"
        );
        assert!(
            self.inserted.values.is_empty() && self.inserted.values.capacity() >= self.installed,
            "prepaid metadata retirement capacity"
        );
        let remaining = len - self.installed;
        let original_capacity = self
            .storage
            .as_ref()
            .map_or(capacity, |storage| storage.values.capacity());
        assert!(
            self.storage
                .as_ref()
                .is_none_or(|storage| storage.values.is_empty()),
            "preserved metadata storage"
        );
        assert!(
            original_capacity >= self.original_len && remaining <= original_capacity,
            "original metadata storage capacity"
        );
        let mut restored = remaining;
        for (position, _) in self.removed.values.iter().rev() {
            assert!(
                *position <= restored && restored < original_capacity,
                "original metadata inverse position"
            );
            restored += 1;
        }
        assert_eq!(
            restored, self.original_len,
            "original metadata inverse length"
        );
        *field = (restored, original_capacity);
    }
    pub(super) fn restore(&mut self, field: &mut MetadataBuffer<AdmittedMetadataValue<T>>) {
        for _ in 0..self.installed {
            self.inserted.push(field.values.remove(self.at));
        }
        if let Some(mut original) = self.storage.take() {
            field.move_into(&mut original);
            self.storage = Some(std::mem::replace(field, original));
        }
        while let Some((position, value)) = self.removed.values.pop() {
            assert!(
                field.values.len() < field.values.capacity(),
                "original metadata capacity"
            );
            field.values.insert(position, value);
        }
        assert_eq!(
            field.values.len(),
            self.original_len,
            "restored metadata length"
        );
    }
}
impl EngineMetadataChange {
    pub(super) fn validate(&self, metadata: &EnginePendingCommitMetadata) {
        match self {
            Self::Discard(splice) => splice.validate(&metadata.discard_selections),
            Self::Delivery(splice) => splice.validate(&metadata.delivery_registrations),
            Self::Conditional(splice) => splice.validate(&metadata.conditional_update_guards),
            Self::SyncLineage(splice) => splice.validate(&metadata.sync_lineage_guards),
            Self::Upsert(splice) => splice.validate(&metadata.upsert_intents),
            Self::Epochs(change) => assert!(
                change.prior.is_none()
                    && change.displaced.is_none()
                    && change
                        .candidate
                        .as_ref()
                        .is_some_and(|map| map.body.is_some() && map.invoice.is_some()),
                "prepared complete epoch map"
            ),
            Self::Memo(change) => assert!(
                change.prior.is_none() && change.displaced.is_none(),
                "prepared lineage memo"
            ),
        }
    }
    pub(super) fn publish(&mut self, metadata: &mut EnginePendingCommitMetadata) {
        match self {
            Self::Discard(splice) => splice.publish(&mut metadata.discard_selections),
            Self::Delivery(splice) => splice.publish(&mut metadata.delivery_registrations),
            Self::Conditional(splice) => splice.publish(&mut metadata.conditional_update_guards),
            Self::SyncLineage(splice) => splice.publish(&mut metadata.sync_lineage_guards),
            Self::Upsert(splice) => splice.publish(&mut metadata.upsert_intents),
            Self::Epochs(change) => {
                change.prior = metadata.vector_schema_epochs.take();
                metadata.vector_schema_epochs = change.candidate.take();
            }
            Self::Memo(change) => {
                change.prior = metadata.lineage_generations.take();
                metadata.lineage_generations = change.candidate.take();
            }
        }
    }
    pub(super) fn restore(&mut self, metadata: &mut EnginePendingCommitMetadata) {
        match self {
            Self::Discard(splice) => splice.restore(&mut metadata.discard_selections),
            Self::Delivery(splice) => splice.restore(&mut metadata.delivery_registrations),
            Self::Conditional(splice) => splice.restore(&mut metadata.conditional_update_guards),
            Self::SyncLineage(splice) => splice.restore(&mut metadata.sync_lineage_guards),
            Self::Upsert(splice) => splice.restore(&mut metadata.upsert_intents),
            Self::Epochs(change) => {
                change.displaced = metadata.vector_schema_epochs.take();
                metadata.vector_schema_epochs = change.prior.take();
            }
            Self::Memo(change) => {
                change.displaced = metadata.lineage_generations.take();
                // The original memo remains physical inverse custody, never
                // becomes current again after rollback of a schema attempt.
            }
        }
    }
}
impl<T> MetadataNode<T> {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn prepare(value: T, original: &OwnedMemoryReservation) -> Result<Self> {
        let credit = crate::memory_accounting::reserve_preparation_from_original(
            original,
            std::mem::size_of::<T>(),
            "relational",
            "mutation_checkpoint",
            "Reduce staged metadata or raise MEMORY_LIMIT.",
        )?;
        Ok(Self {
            value: Some(Box::new(value)),
            credit: Some(credit),
        })
    }
    pub(super) fn get(&self) -> &T {
        self.value.as_deref().expect("metadata node")
    }
    pub(super) fn get_mut(&mut self) -> &mut T {
        self.value.as_deref_mut().expect("metadata node")
    }
}
impl<T> Drop for MetadataNode<T> {
    fn drop(&mut self) {
        drop(self.value.take());
        drop(self.credit.take());
    }
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn validate_temporary_detach(
    journal: &EngineMetadataJournal,
    identity: contextdb_tx::manager::EngineTerminalIdentity,
) -> Result<ValidatedTemporaryMetadata<'_>> {
    let records = journal.records.lock();
    let mut cursor = records.as_ref();
    let mut original_present = false;
    while let Some(node) = cursor {
        if node.credit.is_none() {
            return Err(EngineMetadataJournal::identity_error());
        }
        let record = node
            .value
            .as_deref()
            .ok_or_else(EngineMetadataJournal::identity_error)?;
        if record.tx == identity.original_tx {
            if original_present
                || record.generation != identity.generation
                || record.depth != 0
                || record.frames.is_some()
                || record.undo.is_some()
                || record.undo_len != 0
            {
                return Err(EngineMetadataJournal::identity_error());
            }
            macro_rules! validate_buffer {
                ($buffer:expr) => {{
                    let buffer = &$buffer;
                    if (buffer.values.capacity() != 0 && buffer.credit.is_none())
                        || buffer
                            .values
                            .iter()
                            .any(|entry| entry.body.is_none() || entry.invoice.is_none())
                    {
                        return Err(EngineMetadataJournal::identity_error());
                    }
                }};
            }
            validate_buffer!(record.metadata.discard_selections);
            validate_buffer!(record.metadata.delivery_registrations);
            validate_buffer!(record.metadata.conditional_update_guards);
            validate_buffer!(record.metadata.sync_lineage_guards);
            validate_buffer!(record.metadata.upsert_intents);
            if record
                .metadata
                .vector_schema_epochs
                .as_ref()
                .is_some_and(|entry| entry.body.is_none() || entry.invoice.is_none())
            {
                return Err(EngineMetadataJournal::identity_error());
            }

            original_present = true;
        }
        cursor = record.next.as_ref();
    }
    Ok(ValidatedTemporaryMetadata {
        terminal: ValidatedMetadataTerminal { records, identity },
        original_present,
    })
}

pub(super) fn consume_temporary_nonfallible(
    ticket: ValidatedTemporaryMetadata<'_>,
) -> OwnedTemporaryMetadata {
    let ValidatedTemporaryMetadata {
        terminal,
        original_present,
    } = ticket;
    let identity = terminal.identity;
    let original = terminal.consume_nonfallible();
    OwnedTemporaryMetadata {
        identity,
        original_present,
        original,
    }
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn validate_temporary_return<'j>(
    journal: &'j EngineMetadataJournal,
    identity: contextdb_tx::manager::EngineTerminalIdentity,
    peer: &OwnedTemporaryMetadata,
) -> Result<ValidatedMetadataReturn<'j>> {
    // The caller holds active_txs before this guard. Both tickets stay live
    // until the original source and optional journal singleton are installed.
    let records = journal.records.lock();
    if peer.identity != identity || peer.original_present != peer.original.node.is_some() {
        return Err(EngineMetadataJournal::identity_error());
    }
    if let Some(node) = peer.original.node.as_ref() {
        if node.credit.is_none() {
            return Err(EngineMetadataJournal::identity_error());
        }
        let record = node
            .value
            .as_deref()
            .ok_or_else(EngineMetadataJournal::identity_error)?;
        if record.tx != identity.original_tx
            || record.generation != identity.generation
            || record.depth != 0
            || record.frames.is_some()
            || record.undo.is_some()
            || record.undo_len != 0
            || record.next.is_some()
        {
            return Err(EngineMetadataJournal::identity_error());
        }
        macro_rules! validate_buffer {
            ($buffer:expr) => {{
                let buffer = &$buffer;
                if (buffer.values.capacity() != 0 && buffer.credit.is_none())
                    || buffer
                        .values
                        .iter()
                        .any(|entry| entry.body.is_none() || entry.invoice.is_none())
                {
                    return Err(EngineMetadataJournal::identity_error());
                }
            }};
        }
        validate_buffer!(record.metadata.discard_selections);
        validate_buffer!(record.metadata.delivery_registrations);
        validate_buffer!(record.metadata.conditional_update_guards);
        validate_buffer!(record.metadata.sync_lineage_guards);
        validate_buffer!(record.metadata.upsert_intents);
        if record
            .metadata
            .vector_schema_epochs
            .as_ref()
            .is_some_and(|entry| entry.body.is_none() || entry.invoice.is_none())
        {
            return Err(EngineMetadataJournal::identity_error());
        }
    }
    let mut cursor = records.as_ref();
    while let Some(node) = cursor {
        if node.credit.is_none() {
            return Err(EngineMetadataJournal::identity_error());
        }
        let record = node
            .value
            .as_deref()
            .ok_or_else(EngineMetadataJournal::identity_error)?;
        if record.tx == identity.original_tx {
            return Err(EngineMetadataJournal::identity_error());
        }
        cursor = record.next.as_ref();
    }
    Ok(ValidatedMetadataReturn {
        records,
        original_present: peer.original_present,
    })
}

pub(super) fn reinstall_temporary_nonfallible(
    mut ticket: ValidatedMetadataReturn<'_>,
    peer: OwnedTemporaryMetadata,
) {
    if ticket.original_present {
        terminal_metadata_push(&mut ticket.records, peer.original.node);
    }
    // For original absence there is no node, allocation, or invoice to move.
}

pub(super) fn unwrap_temporary_peer(peer: OwnedTemporaryMetadata) -> OwnedTerminalMetadata {
    peer.original
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn take_owned_conditions(
    peer: &mut OwnedTemporaryMetadata,
) -> Result<ConditionalBuffer> {
    match peer.original.node.as_mut() {
        None if !peer.original_present => Ok(MetadataBuffer::empty()),
        None => Err(EngineMetadataJournal::identity_error()),
        Some(node) => {
            if node.credit.is_none() {
                return Err(EngineMetadataJournal::identity_error());
            }
            let record = node
                .value
                .as_deref_mut()
                .ok_or_else(EngineMetadataJournal::identity_error)?;
            if !peer.original_present
                || record.tx != peer.identity.original_tx
                || record.generation != peer.identity.generation
            {
                return Err(EngineMetadataJournal::identity_error());
            }
            let buffer = &mut record.metadata.conditional_update_guards;
            if (buffer.values.capacity() != 0 && buffer.credit.is_none())
                || buffer
                    .values
                    .iter()
                    .any(|entry| entry.body.is_none() || entry.invoice.is_none())
            {
                return Err(EngineMetadataJournal::identity_error());
            }
            Ok(std::mem::replace(buffer, MetadataBuffer::empty()))
        }
    }
}
impl FundedWorkingGraphSource {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn prepare(funder: &contextdb_tx::SourceFunder) -> Result<Self> {
        let control = contextdb_tx::SourceCharge::admit_funded(
            funder.clone(),
            contextdb_tx::staged_source::source_arc_layout::<writer_read::ScopedGraphSource>()
                .size(),
        )?;
        let source = Arc::new(writer_read::ScopedGraphSource::new());
        Ok(Self { source, control })
    }
}
impl Clone for FundedWorkingGraphSource {
    fn clone(&self) -> Self {
        let control = self.control.clone();
        let source = self.source.clone();
        Self { source, control }
    }
}
impl DatabaseCommitScope<'_, '_, '_> {
    /// Native received preparation borrows the actual companion seed before
    /// any stage/queue guard. The existing lower scope proves the original
    /// commit mutex is already held before the public write is opened.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn prepare_received_schema_stage(
        &self,
        database: &Database,
        events: &event_bus::OwnedEventBusHandle,
        local_lsn: Lsn,
        coordinates: Option<&ConnectedCoordinates>,
        changes: &ChangeSet,
        received: Option<&crate::protocol::ReceivedDdlContext>,
        validated_lineages: &[(String, NaturalKey, Lsn, crate::protocol::WireRowLineage)],
        inputs: ReceivedSchemaAdjudicationInputs<'_>,
        delivery_prepare: Option<&mut dyn crate::custody::preparation::CommitPreparation>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<ReceivedSchemaStage> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.prepare_received_schema_stage_in(
            database,
            events,
            local_lsn,
            coordinates,
            changes,
            received,
            validated_lineages,
            inputs,
            delivery_prepare,
            vector_copy_owner,
            read_memory,
            crate::persistence::received_working::ReceivedWorkingWrite::begin,
            |error| error,
        )
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn prepare_received_schema_stage_in<E>(
        &self,
        database: &Database,
        events: &event_bus::OwnedEventBusHandle,
        local_lsn: Lsn,
        coordinates: Option<&ConnectedCoordinates>,
        changes: &ChangeSet,
        received: Option<&crate::protocol::ReceivedDdlContext>,
        validated_lineages: &[(String, NaturalKey, Lsn, crate::protocol::WireRowLineage)],
        inputs: ReceivedSchemaAdjudicationInputs<'_>,
        delivery_prepare: Option<&mut dyn crate::custody::preparation::CommitPreparation>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        begin: impl FnOnce(
            &RedbPersistence,
            &OwnedMemoryReservation,
        ) -> std::result::Result<
            crate::persistence::received_working::ReceivedWorkingWrite,
            E,
        >,
        existing: impl Fn(Error) -> E,
    ) -> std::result::Result<ReceivedSchemaStage, E> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();
        // A receiver without a durable store has no live write to lend its
        // private image. Refuse before any host validation or working write.
        if database.persistence.is_none() {
            return Err(existing(
                super::received_schema_apply::received_schema_requires_durable_receiver(),
            ));
        }

        database
            .validate_owned_received_event_host(events, self.host)
            .map_err(&existing)?;
        if !self.host.events.downgrade().matches(events) {
            return Err(existing(Error::Other(
                "received preparation requires the host's original event companion".into(),
            )));
        }
        // The seed loan remains Error-only. Its success value carries the
        // entire private result, so callback return never extracts custody.
        events
            .with_owned_persistence_original(|original| {
                Ok((|| {
                    // The private image keeps its rows in one unpublished write
                    // on the live store, retained through the prepared stage. A
                    // receiver without a durable store is refused before any
                    // working write begins.
                    let Some(store) = database.persistence.as_ref() else {
                        return Err(existing(
                            super::received_schema_apply::received_schema_requires_durable_receiver(
                            ),
                        ));
                    };
                    let mut write = begin(store, original)?;
                    write.retain_original_owner(events).map_err(&existing)?;
                    let loan = ReceivedWorkingPreparation {
                        write: &write,
                        original,
                        funder: &self.host.funder,
                        store,
                    };
                    let mut stage = database
                        .prepare_received_schema_stage_with_interval(
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
                        )
                        .map_err(&existing)?;
                    if !stage.trigger_audits.is_empty() {
                        stage.native_audit_memory = Some(
                            database
                                .prepare_original_audit_memory_iter(
                                    original,
                                    stage.trigger_audits.iter().map(|(_, entry)| entry),
                                )
                                .map_err(&existing)?,
                        );
                    }
                    *stage.durable_projection.working_write.get_mut() = Some(write);
                    Ok(stage)
                })())
            })
            .map_err(&existing)?
    }

    #[allow(
        clippy::result_large_err,
        reason = "the error is contextdb_tx::manager::EngineTerminalFailure, a public type of another crate; boxing it would change that crate's API"
    )]
    pub(super) fn prepare_temporary<R, A>(
        &self,
        tx: TxId,
        before_guard: impl for<'p> FnOnce(&'p mut OwnedTemporaryMetadata) -> Result<A>,
        prepare: impl for<'g, 'h, 'p> FnOnce(
            &'g mut contextdb_tx::manager::EngineGuard<'h>,
            &'p mut OwnedTemporaryMetadata,
            A,
        ) -> Result<R>,
    ) -> PreparationResult<R> {
        self.lower.with_engine_peer_detached(
            tx,
            |identity| validate_temporary_detach(&self.host.metadata, identity),
            consume_temporary_nonfallible,
            before_guard,
            prepare,
            |identity, peer| validate_temporary_return(&self.host.metadata, identity, peer),
            reinstall_temporary_nonfallible,
        )
    }
}

impl EngineMetadataJournal {
    pub(super) fn validate_terminal(
        &self,
        identity: contextdb_tx::manager::EngineTerminalIdentity,
    ) -> ValidatedMetadataTerminal<'_> {
        let records = self.records.lock();
        let mut cursor = records.as_ref();
        while let Some(node) = cursor {
            // Every traversed body is checked before either source/peer unlink.
            assert!(node.credit.is_some(), "original metadata node reservation");
            let record = node.value.as_deref().expect("original metadata node body");
            if record.tx == identity.original_tx {
                Self::validate_terminal_record(record, identity);
            }
            cursor = record.next.as_ref();
        }
        ValidatedMetadataTerminal { records, identity }
    }

    pub(super) fn validate_terminal_record(
        record: &EngineMetadataRecord,
        identity: contextdb_tx::manager::EngineTerminalIdentity,
    ) {
        assert!(
            record.tx == identity.original_tx && record.generation == identity.generation,
            "terminal metadata original identity"
        );
        assert!(
            record.depth == 0
                && record.frames.is_none()
                && record.undo.is_none()
                && record.undo_len == 0,
            "terminal metadata requires consumed coupled checkpoints"
        );
    }

    pub(super) fn validate_drain<'j>(
        &'j self,
        sources: contextdb_tx::manager::EngineTerminalInventory<'_>,
    ) -> ValidatedMetadataDrain<'j> {
        let records = self.records.lock();
        let mut cursor = records.as_ref();
        let mut len = 0usize;
        while let Some(node) = cursor {
            assert!(node.credit.is_some(), "original metadata node reservation");
            let record = node.value.as_deref().expect("original metadata node body");
            let identity = sources
                .identity(record.tx)
                .expect("terminal metadata has no source");
            Self::validate_terminal_record(record, identity);
            len = len.checked_add(1).expect("terminal metadata count");
            cursor = record.next.as_ref();
        }
        // The sole insertion door checks absence under records before linking.
        // That private uniqueness invariant avoids a quadratic duplicate scan.
        assert!(len <= sources.len(), "terminal metadata subset count");
        ValidatedMetadataDrain { records, len }
    }
}

impl ValidatedMetadataTerminal<'_> {
    pub(super) fn consume_nonfallible(mut self) -> OwnedTerminalMetadata {
        let mut cursor = &mut *self.records;
        loop {
            let matches = cursor
                .as_ref()
                .and_then(|node| node.value.as_deref())
                .is_some_and(|record| record.tx == self.identity.original_tx);
            if matches {
                let node = terminal_metadata_pop(cursor);
                return OwnedTerminalMetadata { node };
            }
            match cursor {
                Some(node) => match node.value.as_deref_mut() {
                    Some(record) => cursor = &mut record.next,
                    None => return OwnedTerminalMetadata { node: None },
                },
                None => return OwnedTerminalMetadata { node: None },
            }
        }
    }
}

// These kernels only move Option links. Validation precedes all movement;
// no original node/value/credit is replaced, unwrapped, freed or re-admitted.
pub(super) fn terminal_metadata_pop(head: &mut MetadataHead) -> MetadataHead {
    let mut node = head.take();
    if let Some(record) = node.as_mut().and_then(|node| node.value.as_deref_mut()) {
        *head = record.next.take();
    }
    node
}

pub(super) fn terminal_metadata_push(head: &mut MetadataHead, mut node: MetadataHead) {
    if let Some(record) = node.as_mut().and_then(|node| node.value.as_deref_mut()) {
        record.next = head.take();
    }
    *head = node;
}

pub(super) fn terminal_metadata_reverse(mut input: MetadataHead) -> MetadataHead {
    let mut output = None;
    while input.is_some() {
        let node = terminal_metadata_pop(&mut input);
        terminal_metadata_push(&mut output, node);
    }
    output
}

pub(super) fn terminal_metadata_split(head: &mut MetadataHead, width: usize) -> MetadataHead {
    let mut cursor = head;
    for _ in 0..width {
        match cursor {
            Some(node) => match node.value.as_deref_mut() {
                Some(record) => cursor = &mut record.next,
                None => return None,
            },
            None => return None,
        }
    }
    cursor.take()
}

pub(super) fn terminal_metadata_sort(mut head: MetadataHead, len: usize) -> MetadataHead {
    let mut width = 1usize;
    while width < len {
        let mut input = head;
        let mut reversed = None;
        while input.is_some() {
            let mut left = input;
            let mut right = terminal_metadata_split(&mut left, width);
            input = terminal_metadata_split(&mut right, width);
            while left.is_some() || right.is_some() {
                let left_key = left
                    .as_ref()
                    .and_then(|node| node.value.as_deref())
                    .map(|record| record.tx);
                let right_key = right
                    .as_ref()
                    .and_then(|node| node.value.as_deref())
                    .map(|record| record.tx);
                let from_left = right.is_none() || (left.is_some() && left_key <= right_key);
                let node = if from_left {
                    terminal_metadata_pop(&mut left)
                } else {
                    terminal_metadata_pop(&mut right)
                };
                terminal_metadata_push(&mut reversed, node);
            }
        }
        head = terminal_metadata_reverse(reversed);
        if width >= len - width {
            break;
        }
        width += width;
    }
    head
}

impl ValidatedMetadataDrain<'_> {
    pub(super) fn consume_nonfallible(mut self) -> OwnedTerminalMetadataChain {
        let head = self.records.take();
        OwnedTerminalMetadataChain {
            head: terminal_metadata_sort(head, self.len),
        }
    }
}
impl OwnedTerminalMetadataChain {
    pub(super) fn take_front_nonfallible(
        &mut self,
        identity: contextdb_tx::manager::EngineTerminalIdentity,
    ) -> OwnedTerminalMetadata {
        let matches = self
            .head
            .as_ref()
            .and_then(|node| node.value.as_deref())
            .is_some_and(|record| {
                record.tx == identity.original_tx && record.generation == identity.generation
            });
        OwnedTerminalMetadata {
            node: if matches {
                terminal_metadata_pop(&mut self.head)
            } else {
                None
            },
        }
    }
}
impl Drop for TerminalMetadataCleanup {
    fn drop(&mut self) {
        while self.head.is_some() {
            let node = terminal_metadata_pop(&mut self.head);
            drop(node);
        }
    }
}
impl Drop for OwnedTerminalMetadataChain {
    fn drop(&mut self) {
        // The paired drain's source field has already retired. Keep self's
        // field empty during cleanup, including cleanup after a first panic.
        let mut cleanup = TerminalMetadataCleanup {
            head: self.head.take(),
        };
        while cleanup.head.is_some() {
            let node = terminal_metadata_pop(&mut cleanup.head);
            drop(node);
        }
    }
}
impl Drop for RetiredEngineMetadata {
    fn drop(&mut self) {
        while let Some(mut inverse) = self.inverses.take() {
            self.inverses = inverse.get_mut().next.take();
            drop(inverse);
        }
        drop(self.frame.take());
        drop(self.record.take());
    }
}
impl EngineMetadataJournal {
    pub(super) fn record(
        head: &Option<MetadataNode<EngineMetadataRecord>>,
        tx: TxId,
    ) -> Option<&EngineMetadataRecord> {
        let mut cursor = head.as_ref();
        while let Some(node) = cursor {
            if node.get().tx == tx {
                return Some(node.get());
            }
            cursor = node.get().next.as_ref();
        }
        None
    }
    pub(super) fn record_mut(
        head: &mut Option<MetadataNode<EngineMetadataRecord>>,
        tx: TxId,
    ) -> Option<&mut EngineMetadataRecord> {
        let mut cursor = head.as_mut();
        while let Some(node) = cursor {
            if node.get().tx == tx {
                return Some(node.get_mut());
            }
            cursor = node.get_mut().next.as_mut();
        }
        None
    }
    pub(super) fn take_record(
        head: &mut Option<MetadataNode<EngineMetadataRecord>>,
        tx: TxId,
    ) -> Option<MetadataNode<EngineMetadataRecord>> {
        let mut cursor = head;
        loop {
            if cursor.as_ref()?.get().tx == tx {
                let mut node = cursor.take().expect("located record");
                *cursor = node.get_mut().next.take();
                return Some(node);
            }
            cursor = &mut cursor.as_mut().expect("record link").get_mut().next;
        }
    }
    pub(super) fn identity_error() -> Error {
        Error::Other("metadata checkpoint identity mismatch".into())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn prepare_metadata_checkpoint(
        &self,
        identity: (TxId, u64, u64, usize),
    ) -> Result<PreparedMetadataCheckpoint> {
        self.events.with_owned_persistence_original(|original| {
            self.prepare_metadata_checkpoint_from_original(identity, original)
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn prepare_metadata_checkpoint_from_original(
        &self,
        identity: (TxId, u64, u64, usize),
        original: &OwnedMemoryReservation,
    ) -> Result<PreparedMetadataCheckpoint> {
        let (tx, generation, nonce, depth) = identity;
        if nonce == 0 {
            return Err(Self::identity_error());
        }
        let (revision, marker, existed, needs_record) = {
            let records = self.records.lock();
            match Self::record(&records, tx) {
                Some(record)
                    if record.generation == generation
                        && record.depth.checked_add(1) == Some(depth) =>
                {
                    Some((record.revision, record.undo_len, record.existed, false))
                }
                None if depth == 1 => Some((0, 0, false, true)),
                _ => None,
            }
        }
        .ok_or_else(Self::identity_error)?;
        // Preserve enough monotonic revision space to close every live frame.
        revision
            .checked_add(u64::try_from(depth).map_err(|_| Self::identity_error())?)
            .and_then(|revision| revision.checked_add(1))
            .ok_or_else(Self::identity_error)?;
        let record = if needs_record {
            Some(MetadataNode::prepare(
                EngineMetadataRecord {
                    tx,
                    generation,
                    revision,
                    metadata: EnginePendingCommitMetadata::empty(),
                    existed: false,
                    undo: None,
                    undo_len: 0,
                    frames: None,
                    depth: 0,
                    next: None,
                },
                original,
            )?)
        } else {
            None
        };
        let frame = MetadataNode::prepare(
            EngineMetadataFrame {
                identity,
                marker,
                existed,
                stale_memo: None,
                previous: None,
            },
            original,
        )?;
        Ok(PreparedMetadataCheckpoint {
            expected_revision: revision,
            record,
            frame,
        })
    }

    pub(super) fn begin_metadata_checkpoint(
        &self,
        mut prepared: PreparedMetadataCheckpoint,
    ) -> InstalledMetadataCheckpoint<'_> {
        let identity = prepared.frame.get().identity;
        let mut records = self.records.lock();
        {
            match Self::record(&records, identity.0) {
                Some(record) => assert!(
                    prepared.record.is_none()
                        && record.generation == identity.1
                        && record.revision == prepared.expected_revision
                        && record.depth + 1 == identity.3,
                    "prepared metadata frame changed"
                ),
                None => assert!(
                    prepared.record.is_some() && identity.3 == 1,
                    "prepared metadata node disappeared"
                ),
            }
            if let Some(mut node) = prepared.record.take() {
                node.get_mut().next = records.take();
                *records = Some(node);
            }
            let record = Self::record_mut(&mut records, identity.0).expect("prepared record");
            prepared.frame.get_mut().previous = record.frames.take();
            record.frames = Some(prepared.frame);
            record.depth = identity.3;
            record.revision += 1;
        }
        InstalledMetadataCheckpoint {
            checkpoint: EngineMetadataCheckpoint {
                journal: self,
                identity,
            },
            records,
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn mutation_binding(
        &self,
        tx: TxId,
    ) -> Result<((TxId, u64, u64, usize), u64, u64, usize)> {
        let binding = {
            let records = self.records.lock();
            Self::record(&records, tx).and_then(|record| {
                record
                    .frames
                    .as_ref()
                    .map(|frame| (frame.get().identity, record.revision, record.undo_len))
            })
        }
        .ok_or_else(Self::identity_error)?;
        let (identity, revision, undo_len) = binding;
        let next_revision = revision.checked_add(1).ok_or_else(Self::identity_error)?;
        next_revision
            .checked_add(u64::try_from(identity.3).map_err(|_| Self::identity_error())?)
            .ok_or_else(Self::identity_error)?;
        let next_len = undo_len.checked_add(1).ok_or_else(Self::identity_error)?;
        Ok((identity, revision, next_revision, next_len))
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn prepare_change(
        &self,
        binding: ((TxId, u64, u64, usize), u64, u64, usize),
        change: EngineMetadataChange,
    ) -> Result<PreparedMetadataMutation> {
        let (identity, expected_revision, next_revision, next_undo_len) = binding;
        let inverse = self.events.with_owned_persistence_original(|original| {
            MetadataNode::prepare(EngineMetadataInverse { change, next: None }, original)
        })?;
        Ok(PreparedMetadataMutation {
            identity,
            expected_revision,
            next_revision,
            next_undo_len,
            inverse: Some(inverse),
        })
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn prepare_metadata_append(
        &self,
        tx: TxId,
        batch: AdmittedMetadataBatch,
    ) -> Result<PreparedMetadataMutation> {
        let at = {
            let records = self.records.lock();
            let record = Self::record(&records, tx).ok_or_else(Self::identity_error)?;
            match &batch {
                AdmittedMetadataBatch::Discard(_) => {
                    record.metadata.discard_selections.values.len()
                }
                AdmittedMetadataBatch::Delivery(_) => {
                    record.metadata.delivery_registrations.values.len()
                }
                AdmittedMetadataBatch::Conditional(_) => {
                    record.metadata.conditional_update_guards.values.len()
                }
                AdmittedMetadataBatch::SyncLineage(_) => {
                    record.metadata.sync_lineage_guards.values.len()
                }
                AdmittedMetadataBatch::Upsert(_) => record.metadata.upsert_intents.values.len(),
            }
        };
        self.prepare_metadata_splice(tx, at, &[], batch)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn prepare_metadata_splice(
        &self,
        tx: TxId,
        at: usize,
        remove: &[usize],
        batch: AdmittedMetadataBatch,
    ) -> Result<PreparedMetadataMutation> {
        let binding = self.mutation_binding(tx)?;
        let shape = {
            let records = self.records.lock();
            Self::record(&records, tx)
                .filter(|record| record.revision == binding.1)
                .map(|record| {
                    let metadata = &record.metadata;
                    match &batch {
                        AdmittedMetadataBatch::Discard(_) => (
                            metadata.discard_selections.values.len(),
                            metadata.discard_selections.values.capacity(),
                        ),
                        AdmittedMetadataBatch::Delivery(_) => (
                            metadata.delivery_registrations.values.len(),
                            metadata.delivery_registrations.values.capacity(),
                        ),
                        AdmittedMetadataBatch::Conditional(_) => (
                            metadata.conditional_update_guards.values.len(),
                            metadata.conditional_update_guards.values.capacity(),
                        ),
                        AdmittedMetadataBatch::SyncLineage(_) => (
                            metadata.sync_lineage_guards.values.len(),
                            metadata.sync_lineage_guards.values.capacity(),
                        ),
                        AdmittedMetadataBatch::Upsert(_) => (
                            metadata.upsert_intents.values.len(),
                            metadata.upsert_intents.values.capacity(),
                        ),
                    }
                })
        }
        .ok_or_else(Self::identity_error)?;
        let (len, capacity) = shape;
        self.events.with_owned_persistence_original(|original| {
            let change = match batch {
                AdmittedMetadataBatch::Discard(values) => EngineMetadataChange::Discard(
                    MetadataSplice::prepare(at, remove, values, len, capacity, original)?,
                ),
                AdmittedMetadataBatch::Delivery(values) => EngineMetadataChange::Delivery(
                    MetadataSplice::prepare(at, remove, values, len, capacity, original)?,
                ),
                AdmittedMetadataBatch::Conditional(values) => EngineMetadataChange::Conditional(
                    MetadataSplice::prepare(at, remove, values, len, capacity, original)?,
                ),
                AdmittedMetadataBatch::SyncLineage(values) => EngineMetadataChange::SyncLineage(
                    MetadataSplice::prepare(at, remove, values, len, capacity, original)?,
                ),
                AdmittedMetadataBatch::Upsert(values) => EngineMetadataChange::Upsert(
                    MetadataSplice::prepare(at, remove, values, len, capacity, original)?,
                ),
            };
            self.prepare_change(binding, change)
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn prepare_metadata_epochs(
        &self,
        tx: TxId,
        map: HashMap<VectorIndexRef, u64>,
        invoice: OwnedMemoryReservation,
    ) -> Result<PreparedMetadataMutation> {
        // Pair the actual complete prepared native map and its debit FIRST.
        let candidate = AdmittedMetadataValue::new(map, invoice);
        let binding = self.mutation_binding(tx)?;
        self.prepare_change(
            binding,
            EngineMetadataChange::Epochs(MetadataEpochChange {
                candidate: Some(candidate),
                prior: None,
                displaced: None,
            }),
        )
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn prepare_metadata_memo(
        &self,
        tx: TxId,
        candidate: Option<OriginalLineageMemo>,
    ) -> Result<PreparedMetadataMutation> {
        // These existing memo owners already carry their actual node/control
        // reservations; no new Arc or replacement owner is manufactured here.
        let binding = self.mutation_binding(tx)?;
        self.prepare_change(
            binding,
            EngineMetadataChange::Memo(MetadataMemoChange {
                candidate,
                prior: None,
                displaced: None,
            }),
        )
    }

    pub(super) fn publish_metadata(&self, mut prepared: PreparedMetadataMutation) {
        let mut records = self.records.lock();
        let record =
            Self::record_mut(&mut records, prepared.identity.0).expect("metadata transaction");
        assert!(
            record.generation == prepared.identity.1
                && record.revision == prepared.expected_revision
                && record
                    .frames
                    .as_ref()
                    .is_some_and(|frame| frame.get().identity == prepared.identity),
            "metadata token must publish into its original top frame"
        );
        assert_eq!(
            record.undo_len.checked_add(1),
            Some(prepared.next_undo_len),
            "prepared journal length"
        );
        prepared
            .inverse
            .as_ref()
            .expect("prepared inverse")
            .get()
            .change
            .validate(&record.metadata);
        let mut inverse = prepared.inverse.take().expect("prepared inverse");
        inverse.get_mut().change.publish(&mut record.metadata);
        inverse.get_mut().next = record.undo.take();
        record.undo = Some(inverse);
        record.undo_len = prepared.next_undo_len;
        record.existed = true;
        record.revision = prepared.next_revision;
    }

    pub(super) fn validate_checkpoint(
        &self,
        identity: (TxId, u64, u64, usize),
        settle: bool,
    ) -> ValidatedMetadataClose<'_> {
        let records = self.records.lock();
        let record = Self::record(&records, identity.0).expect("metadata transaction");
        assert!(
            record.generation == identity.1
                && record.depth == identity.3
                && identity.3 != 0
                && record
                    .frames
                    .as_ref()
                    .is_some_and(|frame| frame.get().identity == identity),
            "metadata checkpoint must consume its original top frame"
        );
        let frame = record
            .frames
            .as_ref()
            .expect("validated metadata frame")
            .get();
        assert!(
            record.revision < u64::MAX
                && frame.marker <= record.undo_len
                && frame.stale_memo.is_none(),
            "prepaid metadata close"
        );
        let mut frames = record.frames.as_ref();
        let mut depth = record.depth;
        while let Some(node) = frames {
            let saved = node.get();
            assert!(
                saved.identity.0 == identity.0
                    && saved.identity.1 == identity.1
                    && saved.identity.3 == depth,
                "original metadata frame stack"
            );
            assert!(
                depth != 0 && saved.marker <= record.undo_len,
                "metadata frame depth and marker"
            );
            depth -= 1;
            frames = saved.previous.as_ref();
        }
        assert_eq!(depth, 0, "original metadata depth");
        let mut inverses = record.undo.as_ref();
        let mut count = record.undo_len;
        while let Some(node) = inverses {
            assert!(count != 0, "original metadata inverse length");
            count -= 1;
            inverses = node.get().next.as_ref();
        }
        assert_eq!(count, 0, "original metadata inverse count");
        // Check the complete replay's lengths/capacities before lower consumption.
        // This visits existing nodes only and neither allocates nor restores data.
        if !settle {
            let metadata = &record.metadata;
            let mut fields = [
                (
                    metadata.discard_selections.values.len(),
                    metadata.discard_selections.values.capacity(),
                ),
                (
                    metadata.delivery_registrations.values.len(),
                    metadata.delivery_registrations.values.capacity(),
                ),
                (
                    metadata.conditional_update_guards.values.len(),
                    metadata.conditional_update_guards.values.capacity(),
                ),
                (
                    metadata.sync_lineage_guards.values.len(),
                    metadata.sync_lineage_guards.values.capacity(),
                ),
                (
                    metadata.upsert_intents.values.len(),
                    metadata.upsert_intents.values.capacity(),
                ),
            ];
            let mut cursor = record.undo.as_ref();
            for _ in frame.marker..record.undo_len {
                let inverse = cursor.expect("prepared metadata inverse").get();
                match &inverse.change {
                    EngineMetadataChange::Discard(splice) => {
                        splice.validate_restore(&mut fields[0])
                    }
                    EngineMetadataChange::Delivery(splice) => {
                        splice.validate_restore(&mut fields[1])
                    }
                    EngineMetadataChange::Conditional(splice) => {
                        splice.validate_restore(&mut fields[2])
                    }
                    EngineMetadataChange::SyncLineage(splice) => {
                        splice.validate_restore(&mut fields[3])
                    }
                    EngineMetadataChange::Upsert(splice) => splice.validate_restore(&mut fields[4]),
                    EngineMetadataChange::Epochs(change) => assert!(
                        change.candidate.is_none() && change.displaced.is_none(),
                        "installed epoch inverse"
                    ),
                    EngineMetadataChange::Memo(change) => assert!(
                        change.candidate.is_none() && change.displaced.is_none(),
                        "installed memo inverse"
                    ),
                }
                cursor = inverse.next.as_ref();
            }
        }
        ValidatedMetadataClose {
            records,
            identity,
            settle,
        }
    }
}
impl Drop for EngineMetadataJournal {
    fn drop(&mut self) {
        while let Some(mut record) = self.records.get_mut().take() {
            *self.records.get_mut() = record.get_mut().next.take();
            let mut cleanup = RetiredEngineMetadata {
                inverses: record.get_mut().undo.take(),
                frame: None,
                record: None,
            };
            while let Some(mut frame) = record.get_mut().frames.take() {
                record.get_mut().frames = frame.get_mut().previous.take();
                drop(frame);
            }
            cleanup.record = Some(record);
            drop(cleanup);
        }
    }
}
impl ValidatedMetadataClose<'_> {
    pub(super) fn consume(mut self) -> RetiredEngineMetadata {
        let mut retired = RetiredEngineMetadata::default();
        let record = EngineMetadataJournal::record_mut(&mut self.records, self.identity.0)
            .expect("validated metadata transaction");
        let mut frame = record.frames.take().expect("validated metadata frame");
        record.frames = frame.get_mut().previous.take();
        if !self.settle {
            while record.undo_len > frame.get().marker {
                let mut inverse = record.undo.take().expect("validated metadata inverse");
                record.undo = inverse.get_mut().next.take();
                inverse.get_mut().change.restore(&mut record.metadata);
                record.undo_len -= 1;
                inverse.get_mut().next = retired.inverses.take();
                retired.inverses = Some(inverse);
            }
            frame.get_mut().stale_memo = record.metadata.lineage_generations.take();
            record.existed = frame.get().existed;
        }
        record.depth -= 1;
        record.revision += 1;
        if record.depth == 0 && self.settle {
            // Child settle retains its entire inverse prefix in the parent.
            retired.inverses = record.undo.take();
            record.undo_len = 0;
        }
        let remove_record = record.depth == 0 && !record.existed;
        retired.frame = Some(frame);
        if remove_record {
            retired.record = EngineMetadataJournal::take_record(&mut self.records, self.identity.0);
        }
        // Only the lock dies here. P carries every native allocation to the
        // caller after the protected lower door has released the active map.
        retired
    }
}
impl OwnedWriteSetCheckpoint<'_> {
    pub(super) fn consume(&mut self, settle: bool) {
        let Some(pair) = self.pair.take() else {
            return;
        };
        let metadata = pair.metadata;
        let lower_identity = pair.transaction.identity();
        let transaction = std::mem::ManuallyDrop::into_inner(pair.transaction);
        let validate = || {
            assert_eq!(
                lower_identity, metadata.identity,
                "paired checkpoint identity"
            );
            metadata
                .journal
                .validate_checkpoint(metadata.identity, settle)
        };
        let retired = if settle {
            transaction.settle_with_peer(validate, ValidatedMetadataClose::consume)
        } else {
            transaction.restore_with_peer(validate, ValidatedMetadataClose::consume)
        };
        drop(retired);
    }
    pub(super) fn restore_for_tx(mut self, tx: TxId) {
        // Detach before checking the caller's identity: a rejected restore must
        // not run either token's destructor as an accidental close.
        let pair = self.pair.take().expect("active coupled checkpoint");
        assert_eq!(
            pair.metadata.identity.0, tx,
            "coupled checkpoint transaction"
        );
        assert_eq!(
            pair.transaction.identity(),
            pair.metadata.identity,
            "paired checkpoint identity"
        );
        self.pair = Some(pair);
        self.consume(false);
    }
    pub(super) fn finish(mut self, settle: bool) {
        self.consume(settle);
    }
}
impl Drop for OwnedWriteSetCheckpoint<'_> {
    fn drop(&mut self) {
        self.consume(false);
    }
}
impl WriteSetCheckpoint<'_> {
    pub(crate) fn settle(mut self) {
        match self.branch.take().expect("active coupled checkpoint") {
            WriteSetCheckpointBranch::Legacy(legacy) => drop(legacy),
            WriteSetCheckpointBranch::Owned(owned) => owned.finish(true),
        }
    }
}
impl Drop for WriteSetCheckpoint<'_> {
    fn drop(&mut self) {
        // Legacy Drop still settles; Owned Drop restores both halves.
        drop(self.branch.take());
    }
}

#[cfg(test)]
pub(super) fn query_trace_rows_examined_capture_pause_for_test() -> Arc<ApplyPhasePause> {
    QUERY_TRACE_ROWS_EXAMINED_CAPTURE_PAUSE
        .get_or_init(|| Arc::new(ApplyPhasePause::new()))
        .clone()
}

#[cfg(any(test, feature = "test-seams"))]
pub(super) fn vector_journal_cleanup_step_gap_pause_for_test() -> Arc<ApplyPhasePause> {
    VECTOR_JOURNAL_CLEANUP_STEP_GAP_PAUSE
        .get_or_init(|| Arc::new(ApplyPhasePause::new()))
        .clone()
}

/// Pause between two committed steps of a vector journal cleanup.
#[cfg(any(test, feature = "test-seams"))]
pub(crate) fn maybe_pause_between_vector_journal_cleanup_steps_for_test() {
    vector_journal_cleanup_step_gap_pause_for_test().maybe_pause();
}

#[cfg(any(test, feature = "test-seams"))]
pub(super) fn vector_journal_cleanup_pause_for_test() -> Arc<ApplyPhasePause> {
    VECTOR_JOURNAL_CLEANUP_PAUSE
        .get_or_init(|| Arc::new(ApplyPhasePause::new()))
        .clone()
}

#[cfg(any(test, feature = "test-seams"))]
pub(super) fn vector_repair_snapshot_sample_transition_pause_for_test() -> Arc<ApplyPhasePause> {
    VECTOR_REPAIR_SNAPSHOT_SAMPLE_TRANSITION_PAUSE
        .get_or_init(|| Arc::new(ApplyPhasePause::new()))
        .clone()
}

#[cfg(any(test, feature = "test-seams"))]
pub(crate) fn maybe_pause_vector_journal_cleanup_for_test() {
    vector_journal_cleanup_pause_for_test().maybe_pause();
}

#[cfg(any(test, feature = "test-seams"))]
pub(super) fn maybe_pause_vector_repair_snapshot_sample_transition_for_test() {
    vector_repair_snapshot_sample_transition_pause_for_test().maybe_pause();
}

#[cfg(test)]
pub(super) fn arm_query_trace_rows_examined_capture_pause_for_test() -> ApplyPhasePauseGuard {
    let inner = query_trace_rows_examined_capture_pause_for_test();
    let generation = inner.arm();
    ApplyPhasePauseGuard { inner, generation }
}

#[cfg(test)]
pub(super) fn mark_this_thread_for_query_trace_rows_examined_capture_pause_for_test() {
    QUERY_TRACE_ROWS_EXAMINED_CAPTURE_PAUSE_ARMED_HERE.with(|flag| flag.set(true));
}

pub(super) fn open_file_registry() -> &'static OpenFileRegistry {
    OPEN_FILE_DATABASES.get_or_init(|| OpenFileRegistry {
        entries: Mutex::new(BTreeMap::new()),
        waiters: Condvar::new(),
    })
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(crate) fn canonical_database_path(path: &Path) -> Result<PathBuf> {
    canonical_database_path_inner(path, 0)
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn canonical_database_path_inner(path: &Path, depth: usize) -> Result<PathBuf> {
    if depth > 32 {
        return Err(Error::Other(
            (format!(
                "too many symlink levels while canonicalizing {}",
                path.display()
            ))
            .into(),
        ));
    }
    if let Ok(metadata) = std::fs::symlink_metadata(path)
        && metadata.file_type().is_symlink()
    {
        let target = std::fs::read_link(path)
            .map_err(|err| Error::Other((format!("read_link {}: {err}", path.display())).into()))?;
        let resolved = if target.is_absolute() {
            target
        } else {
            path.parent()
                .filter(|parent| !parent.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new("."))
                .join(target)
        };
        return canonical_database_path_inner(&resolved, depth + 1);
    }
    if path.exists() {
        return std::fs::canonicalize(path).map_err(|err| {
            Error::Other((format!("canonicalize {}: {err}", path.display())).into())
        });
    }
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let file_name = path.file_name().ok_or_else(|| {
        Error::Other((format!("database path must include a file name: {}", path.display())).into())
    })?;
    let canonical_parent = std::fs::canonicalize(parent).map_err(|err| {
        Error::Other((format!("canonicalize {}: {err}", parent.display())).into())
    })?;
    Ok(canonical_parent.join(file_name))
}

impl HandleRole {
    /// Whether the store waits for this handle before it is released.
    pub(super) const fn holds_the_store(self) -> bool {
        matches!(self, Self::Owner | Self::Reader)
    }
}
impl<T: contextdb_tx::WriteSetApplicator> AdmittedDynStore<T> {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn prepare(
        payload: T,
        accountant: &crate::memory_accounting::AccountantHandle,
    ) -> Result<DynStore>
    where
        T: 'static,
    {
        let mut value = Self {
            payload: std::mem::ManuallyDrop::new(payload),
            control: None,
        };
        value.control = Some(crate::memory_accounting::ControlCredit::admit_assembly(
            accountant,
            std::alloc::Layout::for_value(&value).size(),
            "dyn_store_control",
        )?);
        Ok(Box::new(value))
    }
}
impl<T: contextdb_tx::WriteSetApplicator> contextdb_tx::WriteSetApplicator for AdmittedDynStore<T> {
    fn apply(
        &self,
        ws: &contextdb_tx::WriteSet,
        read_memory: contextdb_core::read_memory::ReadMemoryOwner,
    ) -> Result<()> {
        self.payload.apply(ws, read_memory)
    }
    fn new_row_id(&self) -> RowId {
        self.payload.new_row_id()
    }
    fn reserve_metadata_publication(&self) -> Result<Option<Box<dyn std::any::Any + Send>>> {
        self.payload.reserve_metadata_publication()
    }
    fn complete_metadata_publication(
        &self,
        reservation: Option<Box<dyn std::any::Any + Send>>,
        snapshot: SnapshotId,
        lsn: Lsn,
    ) {
        self.payload
            .complete_metadata_publication(reservation, snapshot, lsn);
    }
    fn publication_interrupted(&self) -> bool {
        self.payload.publication_interrupted()
    }
    fn publication_handle(&self) -> Option<Arc<dyn std::any::Any + Send + Sync>> {
        self.payload.publication_handle()
    }
    fn native_publication_handle(&self) -> Option<contextdb_tx::native_capture::NativeCapture> {
        self.payload.native_publication_handle()
    }
}
impl<T: contextdb_tx::WriteSetApplicator> Drop for AdmittedDynStore<T> {
    fn drop(&mut self) {
        let refund = self
            .control
            .take()
            .map(crate::memory_accounting::ControlCredit::prepare);
        // The bounded refund remains live through complete payload unwind.
        // No automatic payload destructor remains after the final refund.
        unsafe {
            std::mem::ManuallyDrop::drop(&mut self.payload);
        }
        drop(refund);
    }
}
impl OwnedPersistentDynStore {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn prepare(
        payload: crate::persistent_store::OwnedPersistentCompositeStore,
        funder: &contextdb_tx::SourceFunder,
    ) -> Result<PreparedOwnedPersistentDynStore> {
        let events = payload.events.clone();
        events.with_owned_persistence_original(|original| {
            let mut owner = crate::memory_accounting::reserve_preparation_from_original(
                original,
                std::mem::size_of::<Self>(),
                "writer_source",
                "dyn_store_control",
                "Raise MEMORY_LIMIT to admit writer assembly.",
            )?;
            // Transfer the existing debit before allocating. Its new control is
            // admitted by the same original funder; the payload debit is never refunded.
            let control = owner.into_source_control_charge_for(funder)?;
            let boxed = Box::new(Self { payload });
            let address = &boxed.payload
                as *const crate::persistent_store::OwnedPersistentCompositeStore
                as usize;
            Ok(PreparedOwnedPersistentDynStore {
                store: boxed as DynStore,
                address,
                control,
            })
        })
    }
}
impl WriteSetApplicator for OwnedPersistentDynStore {
    fn apply(
        &self,
        ws: &WriteSet,
        memory: contextdb_core::read_memory::ReadMemoryOwner,
    ) -> Result<()> {
        self.payload.apply(ws, memory)
    }
    fn new_row_id(&self) -> RowId {
        self.payload.new_row_id()
    }
    fn reserve_metadata_publication(&self) -> Result<Option<Box<dyn std::any::Any + Send>>> {
        self.payload.reserve_metadata_publication()
    }
    fn complete_metadata_publication(
        &self,
        reservation: Option<Box<dyn std::any::Any + Send>>,
        snapshot: SnapshotId,
        lsn: Lsn,
    ) {
        self.payload
            .complete_metadata_publication(reservation, snapshot, lsn);
    }
    fn publication_interrupted(&self) -> bool {
        self.payload.publication_interrupted()
    }
    fn publication_handle(&self) -> Option<Arc<dyn std::any::Any + Send + Sync>> {
        self.payload.publication_handle()
    }
    fn native_publication_handle(&self) -> Option<contextdb_tx::native_capture::NativeCapture> {
        self.payload.native_publication_handle()
    }
}
impl OwnedMemoryDynStore {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn prepare(
        payload: CompositeStore,
        events: event_bus::OwnedEventBusHandle,
        funder: &contextdb_tx::SourceFunder,
    ) -> Result<PreparedOwnedMemoryDynStore> {
        let companion = events.clone();
        companion.with_owned_persistence_original(|original| {
            let mut owner = crate::memory_accounting::reserve_preparation_from_original(
                original,
                std::mem::size_of::<Self>(),
                "writer_source",
                "memory_dyn_store_control",
                "Raise MEMORY_LIMIT to admit writer assembly.",
            )?;
            let control = owner.into_source_control_charge_for(funder)?;
            let store = Box::new(Self { payload, events }) as DynStore;
            Ok(PreparedOwnedMemoryDynStore { store, control })
        })
    }
}
impl WriteSetApplicator for OwnedMemoryDynStore {
    fn apply(
        &self,
        ws: &WriteSet,
        memory: contextdb_core::read_memory::ReadMemoryOwner,
    ) -> Result<()> {
        crate::persistent_store::apply_owned_memory_store(&self.payload, &self.events, ws, memory)
    }
    fn new_row_id(&self) -> RowId {
        self.payload.new_row_id()
    }
}

impl Drop for OwnerReadStoreHold {
    fn drop(&mut self) {
        if self.finalization.note_holder_released()
            && let Some(release) = self.finalization.take_pending()
        {
            release_store(release);
        }
    }
}

impl RetainedNonVectorMemoryOwner {
    pub(super) fn new(
        relational: Arc<RelationalStore>,
        graph: Arc<GraphStore>,
        accountant: crate::memory_accounting::AccountantHandle,
    ) -> Self {
        Self {
            frozen_bytes: None,
            defer_private_row_refunds: false,
            aborted_private_effects: None,
            _writer_use: crate::memory_accounting::WriterUse::retain_if_writer(&accountant),
            relational: std::mem::ManuallyDrop::new(relational),
            graph: std::mem::ManuallyDrop::new(graph),
            accountant,
        }
    }

    /// Drop the held handles without returning the retained-state charge,
    /// for an owner whose state was never charged.
    pub(super) fn release_handles_without_refund(self) {
        let owner = std::mem::ManuallyDrop::new(self);
        // SAFETY: `owner` is never used or dropped again; each field is
        // moved out exactly once and dropped normally.
        unsafe {
            drop(std::mem::ManuallyDrop::into_inner(std::ptr::read(
                &owner.relational,
            )));
            drop(std::mem::ManuallyDrop::into_inner(std::ptr::read(
                &owner.graph,
            )));
            drop(std::ptr::read(&owner.aborted_private_effects));
            drop(std::ptr::read(&owner.accountant));
            drop(std::ptr::read(&owner._writer_use));
        }
    }
}

impl RetainedNonVectorMemoryOwner {
    pub(super) fn retained_bytes(&self) -> usize {
        let metadata = self.relational.table_meta.read();
        let metadata_bytes = metadata.values().fold(0usize, |bytes, meta| {
            bytes.saturating_add(retained_table_metadata_bytes(&self.relational, meta))
        });
        let row_bytes =
            self.relational
                .tables
                .read()
                .iter()
                .fold(0usize, |bytes, (table, rows)| {
                    let meta = metadata.get(table);
                    // Public databases retain their established live-set
                    // invoice. The unpublished received image admits every
                    // historical copy and retains tombstone invoices until
                    // physical disposal, so its owner folds every version.
                    bytes.saturating_add(
                        rows.iter()
                            .filter(|row| {
                                self.defer_private_row_refunds
                                    || row.deleted_tx.is_none()
                                    || self.relational.is_retention_expired(
                                        table,
                                        row.row_id,
                                        row.created_tx,
                                    )
                            })
                            .fold(0usize, |row_bytes, row| {
                                row_bytes.saturating_add(retained_row_bytes_with_optional_meta(
                                    table, row, meta,
                                ))
                            }),
                    )
                });
        let edge_bytes = self
            .graph
            .forward_adj
            .read()
            .values()
            .flatten()
            .fold(0usize, |bytes, edge| {
                bytes.saturating_add(edge.retained_graph_bytes())
            });
        metadata_bytes
            .saturating_add(row_bytes)
            .saturating_add(edge_bytes)
    }
    pub(super) fn freeze_before_spooled_row_eviction(&mut self) {
        if self.frozen_bytes.is_none() {
            self.frozen_bytes = Some(self.retained_bytes());
        }
    }
}
impl Drop for RetainedNonVectorMemoryOwner {
    fn drop(&mut self) {
        let bytes = self.frozen_bytes.unwrap_or_else(|| self.retained_bytes());
        // SAFETY: these two handles are initialized once and are never
        // moved except by release_handles_without_refund, which suppresses
        // this destructor. Destroy payload ownership before its last invoice.
        unsafe {
            std::mem::ManuallyDrop::drop(&mut self.relational);
            std::mem::ManuallyDrop::drop(&mut self.graph);
        }
        // Private post-commit failure can leave source-funded callbacks and
        // metadata in this unpublished store without a captured WriteSet.
        // Their actual owner outlives both store handles on that path too.
        drop(self.aborted_private_effects.take());
        self.accountant.release(bytes);
    }
}

impl StoreFinalization {
    pub(super) fn with_writer_original<R>(
        &self,
        prepare: impl for<'o> FnOnce(&'o OwnedMemoryReservation) -> R,
    ) -> std::result::Result<R, contextdb_core::read_memory::NativeRefusal> {
        let held =
            self._writer_use
                .as_ref()
                .ok_or(contextdb_core::read_memory::NativeRefusal::Other(
                    "read image has no held original writer",
                ))?;
        crate::memory_accounting::loan_preparation_original(held, &self.accountant, prepare)
    }

    /// A store loaded from disk owes nothing for its loaded rows, edges and
    /// metadata until `account_loaded_state` has charged them, so it has no
    /// retained-state owner until then: an open that fails earlier has
    /// nothing to refund that it never charged.
    pub(super) fn defer_loaded_state_owner(&self) {
        if let Some(owner) = self.non_vector_memory.lock().take() {
            owner.release_handles_without_refund();
        }
    }

    pub(super) fn install_loaded_state_owner(&self, owner: RetainedNonVectorMemoryOwner) {
        let previous = self.non_vector_memory.lock().replace(owner);
        assert!(previous.is_none(), "one retained-state owner per store");
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn prepare(
        relational: Arc<RelationalStore>,
        graph: Arc<GraphStore>,
        vector: Arc<VectorStore>,
        budget: DatabaseAssemblyBudget,
    ) -> Result<Arc<Self>> {
        use std::mem::ManuallyDrop;
        let (accountant, writer_use, vector_accountant) = budget.into_accountant()?;
        let provider = writer_read::WriterStagedPostingAdmission::prepare(accountant.clone())?;
        let non_vector_memory =
            RetainedNonVectorMemoryOwner::new(relational, graph, accountant.clone());
        let mut value = Self {
            #[cfg(feature = "test-seams")]
            publication_drives_for_test: Mutex::default(),
            #[cfg(feature = "test-seams")]
            release_completion_for_test: Arc::new(ReleaseCompletionForTest::new()),
            live_holders: AtomicUsize::new(1),
            loaded_history: Mutex::new(None),
            local_policy_metadata_enabled: AtomicBool::new(true),
            schema_history_memory: ManuallyDrop::new(Mutex::new(None)),
            pending: ManuallyDrop::new(Mutex::new(None)),
            vector: ManuallyDrop::new(vector),
            accountant: ManuallyDrop::new(accountant.clone()),
            non_vector_memory: ManuallyDrop::new(Mutex::new(Some(non_vector_memory))),
            staged_posting_admission: ManuallyDrop::new(provider),
            vector_accountant: ManuallyDrop::new(vector_accountant),
            _writer_use: ManuallyDrop::new(writer_use),
            control: None,
        };
        let bytes = crate::memory_accounting::writer_adapter_layout(&value).size();
        value.control = Some(crate::memory_accounting::ControlCredit::admit_assembly(
            &value.accountant,
            bytes,
            "store_finalization_control",
        )?);
        Ok(Arc::new(value))
    }

    pub(super) fn note_new_holder(&self) {
        self.live_holders.fetch_add(1, Ordering::SeqCst);
    }

    /// Whether the holder letting go now is the last one on this store.
    ///
    /// Counting past zero would be a bookkeeping fault rather than a release,
    /// so a count already at zero stays there and answers "not the last": the
    /// store has already been let go.
    pub(super) fn note_holder_released(&self) -> bool {
        let last = self
            .live_holders
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |live| {
                live.checked_sub(1)
            })
            .is_ok_and(|previous| previous == 1);
        if last {
            // Internal helper handles can outlive the last caller-facing
            // holder. They do not keep the store open and therefore must not
            // keep vector bodies or relational/F32 projection charges alive.
            let receipt = self.vector.teardown_all_without_copy();
            self.accountant.release(receipt.legacy_live_raw_bytes);
            // A receipt observer is Internal and can keep this Arc alive. It
            // must not extend history residency after the last actual reader.
            drop(self.schema_history_memory.lock().take());
            drop(self.loaded_history.lock().take());
            self.non_vector_memory.lock().take();
        }
        last
    }

    pub(super) fn defer(&self, release: PendingStoreRelease) {
        *self.pending.lock() = Some(release);
    }

    pub(super) fn take_pending(&self) -> Option<PendingStoreRelease> {
        self.pending.lock().take()
    }
}
impl StoreFinalizationCleanup {
    pub(super) fn finish(&mut self) {
        use std::mem::ManuallyDrop;
        while self.next < 10 {
            let step = self.next;
            self.next += 1;
            // Each initialized field is consumed once, before own refund.
            // Advancing first makes the live stack guard resume on unwind.
            unsafe {
                match step {
                    0 => ManuallyDrop::drop(&mut (*self.source).pending),
                    1 => ManuallyDrop::drop(&mut (*self.source).vector),
                    2 => ManuallyDrop::drop(&mut (*self.source).non_vector_memory),
                    3 => drop((*self.source).loaded_history.get_mut().take()),
                    4 => ManuallyDrop::drop(&mut (*self.source).schema_history_memory),
                    5 => ManuallyDrop::drop(&mut (*self.source).staged_posting_admission),
                    6 => ManuallyDrop::drop(&mut (*self.source).vector_accountant),
                    7 => ManuallyDrop::drop(&mut (*self.source).accountant),
                    8 => ManuallyDrop::drop(&mut (*self.source)._writer_use),
                    9 => drop(self.refund.take()),
                    _ => unreachable!(),
                }
            }
        }
    }
}
impl Drop for StoreFinalizationCleanup {
    fn drop(&mut self) {
        self.finish();
    }
}
impl Drop for StoreFinalization {
    fn drop(&mut self) {
        let refund = self
            .control
            .take()
            .map(crate::memory_accounting::ControlCredit::prepare);
        let mut cleanup = StoreFinalizationCleanup {
            source: self,
            next: 0,
            refund,
        };
        if let Some(release) = self.take_pending() {
            release_store(release);
        }
        cleanup.finish();
    }
}

/// Let go of everything an owner acquired for one store.
pub(super) fn release_store(release: PendingStoreRelease) {
    release.resource_closed.store(true, Ordering::SeqCst);
    if let Some(service) = release
        .owner_read_service
        .as_ref()
        .and_then(std::sync::Weak::upgrade)
    {
        service.release_channel();
    }
    if let Some(status) = &release.owner_read_status {
        // The channel is gone and this owner will not take new work, so the
        // record beside the store says exactly that for as long as this writer
        // still holds it. A reader arriving inside that window is told the
        // owner is winding down instead of meeting a lock it cannot explain.
        if let Some(persistence) = &release.persistence {
            let _ = persistence.record_owner_read_status(status.clone());
        }
    }
    if let Some(persistence) = &release.persistence {
        persistence.close();
    }
    if let Some(path) = &release.open_registry_path {
        release_open_registry_path(path);
    }
    #[cfg(feature = "test-seams")]
    let _released_for_test =
        ResourceReleaseCompletionForTest(Arc::clone(&release.release_completion_for_test));
    // The plugin hears about the close from whoever ends the store. The handle
    // that opened it went away while a reader was still inside, so nothing
    // else is left to tell it, and it is told exactly once -- here.
    if let Some(plugin) = &release.plugin {
        let _closed = plugin.on_close();
    }
}

#[cfg(feature = "test-seams")]
impl Drop for ResourceReleaseCompletionForTest {
    fn drop(&mut self) {
        self.0.resources_released();
    }
}

#[cfg(feature = "test-seams")]
impl ReleaseCompletionForTest {
    pub(super) fn new() -> Self {
        Self {
            generation: uuid::Uuid::new_v4(),
            state: Mutex::new(ReleaseStateForTest {
                owner_tasks_observed: false,
                resources_released: false,
                owner_tasks_joined: true,
                error: None,
            }),
            changed: Condvar::new(),
        }
    }

    pub(crate) fn observe_owner_tasks(&self) -> bool {
        let mut state = self.state.lock();
        if state.owner_tasks_observed {
            return false;
        }
        state.owner_tasks_observed = true;
        state.owner_tasks_joined = false;
        true
    }

    pub(crate) fn owner_tasks_joined(&self, result: std::result::Result<(), String>) {
        let mut state = self.state.lock();
        match result {
            Ok(()) => state.owner_tasks_joined = true,
            Err(error) => state.error = Some(error),
        }
        self.changed.notify_all();
    }

    pub(super) fn resources_released(&self) {
        self.state.lock().resources_released = true;
        self.changed.notify_all();
    }
}

#[cfg(feature = "test-seams")]
impl LastHandleReleaseForTest {
    #[doc(hidden)]
    pub fn is_released_for_test(&self) -> bool {
        let state = self.completion.state.lock();
        state.resources_released && state.owner_tasks_joined && state.error.is_none()
    }

    #[doc(hidden)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn wait_for_release_for_test(&self) -> Result<LastHandleReleaseReceipt> {
        let mut state = self.completion.state.lock();
        while !(state.resources_released && state.owner_tasks_joined) && state.error.is_none() {
            self.completion.changed.wait(&mut state);
        }
        if let Some(error) = &state.error {
            return Err(Error::Other(error.clone().into()));
        }
        Ok(LastHandleReleaseReceipt {
            generation: self.completion.generation,
        })
    }
}

#[cfg(feature = "test-seams")]
impl Database {
    #[doc(hidden)]
    pub fn last_handle_release_for_test(&self) -> LastHandleReleaseForTest {
        let completion = Arc::clone(&self.finalization.release_completion_for_test);
        if let Some(service) = &self.owner_read_service {
            service.observe_last_handle_release_for_test(Arc::clone(&completion));
        }
        LastHandleReleaseForTest { completion }
    }
}

impl OpenRegistryState {
    pub(super) fn pid(self) -> u32 {
        match self {
            Self::Opening { pid } | Self::Open { pid } => pid,
        }
    }

    pub(super) fn is_opening(self) -> bool {
        matches!(self, Self::Opening { .. })
    }
}

impl std::task::Wake for ReadPublicationWake {
    fn wake(self: Arc<Self>) {
        if let Some(gate) = self.0.upgrade() {
            let mut state = gate.state.lock();
            state.wake_generation = state.wake_generation.wrapping_add(1);
            gate.changed.notify_all();
        }
    }

    fn wake_by_ref(self: &Arc<Self>) {
        if let Some(gate) = self.0.upgrade() {
            let mut state = gate.state.lock();
            state.wake_generation = state.wake_generation.wrapping_add(1);
            gate.changed.notify_all();
        }
    }
}

pub(super) fn publication_scopes_conflict<'a, 'b>(
    left: impl IntoIterator<Item = &'a String> + Copy,
    right: impl IntoIterator<Item = &'b String> + Copy,
) -> bool {
    left.into_iter().any(|table| table == "*")
        || right.into_iter().any(|table| table == "*")
        || left
            .into_iter()
            .any(|table| right.into_iter().any(|other| other == table))
}

impl ReadPublicationGate {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn acquire_read(
        self: &Arc<Self>,
        scope: &crate::executor::ReadPublicationScope,
    ) -> Result<AuthoritativeReadPublicationGuard> {
        scope.check_publication_budget()?;
        let registration = ReadPublicationRegistration::new(scope)?;
        let budget = &scope.budget;
        // Active time expires only after the boundary, not at equality.
        let deadline_ms = budget.deadline_ms();
        let mut expiry = budget.clock.wait_until(deadline_ms);
        let mut expiry_completed = false;
        let waker = std::task::Waker::from(Arc::new(ReadPublicationWake(Arc::downgrade(self))));
        let mut context = std::task::Context::from_waker(&waker);
        let weak = Arc::downgrade(self);
        let _cancellation = budget.cancellation.try_tell_on_cancel(move || {
            if let Some(gate) = weak.upgrade() {
                let mut state = gate.state.lock();
                state.wake_generation = state.wake_generation.wrapping_add(1);
                gate.changed.notify_all();
            }
        })?;
        let mut state = self.state.lock();
        loop {
            if budget.cancellation.is_cancelled() {
                return Err(Error::ReadCancelled);
            }
            if budget.elapsed() {
                return Err(Error::ReadFailure(budget.exceeded_failure()));
            }
            let conflicts = state
                .writer_scope
                .as_ref()
                .is_some_and(|writer| publication_scopes_conflict(&scope.tables, writer));
            if !conflicts {
                // writer_scope is claimed before a writer waits; a conflicting
                // late reader cannot pass it while we link this registration.
                let guard = AuthoritativeReadPublicationGuard {
                    gate: Arc::clone(self),
                    registration: Some(registration.clone()),
                };
                *registration.next.lock() = state.readers.head.take();
                state.readers.head = Some(registration);
                return Ok(guard);
            }
            // The timer governs time spent waiting behind an authoritative
            // purge. Polling it on an uncontended publication lets a clock's
            // wake implementation veto a result that never waited at all.
            let observed_wake = state.wake_generation;
            drop(state);
            if !expiry_completed {
                expiry_completed =
                    std::future::Future::poll(std::pin::Pin::as_mut(&mut expiry), &mut context)
                        .is_ready();
            }
            if budget.elapsed() {
                return Err(Error::ReadFailure(budget.exceeded_failure()));
            }
            state = self.state.lock();
            // Timer, cancellation, and publication release all advance this
            // generation while holding the same mutex. A wake between polling
            // and sleeping is therefore observed here instead of being lost.
            if state.wake_generation == observed_wake {
                if expiry_completed {
                    match budget.remaining_wait() {
                        crate::executor::ReadPublicationDeadline::Active(remaining) => {
                            self.changed.wait_for(&mut state, remaining);
                        }
                        crate::executor::ReadPublicationDeadline::Elapsed => continue,
                    }
                } else {
                    self.changed.wait(&mut state);
                }
            }
        }
    }

    pub(super) fn acquire_write(
        self: &Arc<Self>,
        tables: &BTreeSet<String>,
    ) -> AuthoritativePurgePublicationGuard {
        let mut state = self.state.lock();
        while state.writer_scope.is_some() {
            self.changed.wait(&mut state);
        }
        state.writer_scope = Some(tables.clone());
        while state.conflicts_with_readers(tables) {
            #[cfg(any(test, feature = "test-seams"))]
            crate::writer_commit_test_seams::reached(
                crate::writer_commit_test_seams::CommitBoundary::ErasureWaitingForReaders,
            );
            self.changed.wait(&mut state);
        }
        state.writer_active = true;
        drop(state);
        AuthoritativePurgePublicationGuard {
            gate: Arc::clone(self),
        }
    }
}

impl Drop for AuthoritativeReadPublicationGuard {
    fn drop(&mut self) {
        let registration = self
            .registration
            .take()
            .expect("live publication registration");
        let mut state = self.gate.state.lock();
        state.unlink_reader(&registration);
        state.wake_generation = state.wake_generation.wrapping_add(1);
        drop(state);
        // Unlink the gate alias before the guard releases the final paid node.
        drop(registration);
        self.gate.changed.notify_all();
    }
}

impl ReadPublicationGate {
    pub(super) fn invalidate_cursors(&self) {
        let cursors = {
            let mut state = self.state.lock();
            let tables = state.writer_scope.as_ref().expect("purge scope is held");
            let cursors = state
                .cursors
                .iter()
                .filter_map(std::sync::Weak::upgrade)
                .filter(|cursor| publication_scopes_conflict(tables, &cursor.publication.tables))
                .collect::<Vec<_>>();
            state.cursors.retain(|cursor| cursor.strong_count() != 0);
            cursors
        };
        for cursor in cursors {
            cursor.release_for_erasure();
        }
    }
}

impl AuthoritativePurgePublicationGuard {
    pub(super) fn invalidate_cursors(&self) {
        self.gate.invalidate_cursors();
    }
}

impl Drop for AuthoritativePurgePublicationGuard {
    fn drop(&mut self) {
        let mut state = self.gate.state.lock();
        state.writer_active = false;
        state.writer_scope = None;
        state.wake_generation = state.wake_generation.wrapping_add(1);
        drop(state);
        self.gate.changed.notify_all();
    }
}

impl Drop for DatabaseOperationGuard<'_> {
    fn drop(&mut self) {
        DB_OPERATION_STACK.with(|stack| {
            let popped = stack.borrow_mut().pop();
            debug_assert_eq!(
                popped,
                Some(self.db_id),
                "database operation stack mismatch"
            );
        });
    }
}

impl DatabaseOperationGuard<'_> {
    /// A maintenance cycle keeps the close barrier but must not pin one
    /// schema publication across long vector preparation. The vector path
    /// samples its declaration and rejects a candidate whose revision changed
    /// before publication, so online policy DDL can proceed while an old
    /// candidate is being built.
    pub(super) fn release_schema_publication_for_online_maintenance(&mut self) {
        self._schema_publication.take();
        // Passes capture their own finite source; the cycle must not keep its
        // opening storage root alive through closing physical reclamation.
        self._writer_read.take();
    }
}

impl Drop for SchemaPublicationGuard<'_> {
    fn drop(&mut self) {
        SCHEMA_PUBLICATION_STACK.with(|stack| {
            let popped = stack.borrow_mut().pop();
            debug_assert_eq!(
                popped,
                Some(self.gate_id),
                "schema publication stack mismatch"
            );
        });
    }
}

impl<'a> TriggerCloseWaiterGuard<'a> {
    pub(super) fn new(trigger: &'a TriggerState) -> Self {
        trigger.close_waiter_count.fetch_add(1, Ordering::SeqCst);
        Self { trigger }
    }
}

impl Drop for TriggerCloseWaiterGuard<'_> {
    fn drop(&mut self) {
        self.trigger
            .close_waiter_count
            .fetch_sub(1, Ordering::SeqCst);
        self.trigger.waiters.notify_all();
    }
}

impl OpenRegistryReservation {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn acquire(path: PathBuf) -> Result<Self> {
        let registry = open_file_registry();
        let mut entries = registry.entries.lock();
        loop {
            match entries.get(&path).copied() {
                Some(state) if state.is_opening() => {
                    registry.waiters.wait(&mut entries);
                }
                Some(state) => {
                    return Err(Error::DatabaseLocked {
                        holder_pid: state.pid(),
                        path,
                    });
                }
                None => {
                    entries.insert(
                        path.clone(),
                        OpenRegistryState::Opening {
                            pid: std::process::id(),
                        },
                    );
                    return Ok(Self { path, active: true });
                }
            }
        }
    }

    pub(super) fn disarm(mut self) -> PathBuf {
        let registry = open_file_registry();
        let mut entries = registry.entries.lock();
        entries.insert(
            self.path.clone(),
            OpenRegistryState::Open {
                pid: std::process::id(),
            },
        );
        registry.waiters.notify_all();
        self.active = false;
        self.path.clone()
    }
}

impl Drop for OpenRegistryReservation {
    fn drop(&mut self) {
        if self.active {
            let registry = open_file_registry();
            let mut entries = registry.entries.lock();
            if entries
                .get(&self.path)
                .copied()
                .is_some_and(|state| state.is_opening() && state.pid() == std::process::id())
            {
                entries.remove(&self.path);
                registry.waiters.notify_all();
            }
        }
    }
}

/// `readers` is the runtime directory this deployment's direct readers publish
/// themselves in, so a refusal names the readers actually holding the store.
/// `None` means nobody stated one and the platform default is where they are.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn acquire_registry_and_persistence(
    canonical_path: &Path,
    open_disposition: OpenDisposition,
    cache_bytes: usize,
    runtime_dir: Option<&Path>,
    accountant: crate::memory_accounting::AccountantHandle,
) -> Result<(OpenRegistryReservation, Arc<RedbPersistence>)> {
    let retry_deadline = Instant::now() + SAME_PROCESS_REOPEN_RETRY;
    let cross_process_retry_deadline = Instant::now() + CROSS_PROCESS_LOCK_SETTLE_RETRY;

    loop {
        let registry_reservation =
            match OpenRegistryReservation::acquire(canonical_path.to_path_buf()) {
                Ok(reservation) => reservation,
                Err(Error::DatabaseLocked { holder_pid, path })
                    if holder_pid == std::process::id()
                        && path == canonical_path
                        && Instant::now() < retry_deadline =>
                {
                    wait_for_open_registry_change(Duration::from_millis(1));
                    continue;
                }
                Err(err) => return Err(err),
            };

        // This is the one place a writable open decides between taking a
        // store and bringing one into being, so it is the only place the
        // disposition can be enforced without inventing a second door. Under
        // `ExistingOnly` the existence question is not asked at all -- asking
        // it and then acting on the answer is precisely the race the
        // disposition exists to remove -- the ordinary open is attempted, and
        // the attempt's own "nothing here" is the typed refusal.
        let persistence = match open_disposition {
            OpenDisposition::ExistingOnly => {
                RedbPersistence::open_existing_only_with_cache_in_runtime_dir(
                    canonical_path,
                    cache_bytes,
                    runtime_dir,
                )
            }
            OpenDisposition::CreateIfMissing => {
                if canonical_path.exists() {
                    RedbPersistence::open_with_cache_in_runtime_dir(
                        canonical_path,
                        cache_bytes,
                        runtime_dir,
                    )
                } else {
                    RedbPersistence::create_with_accountant_in_runtime_dir(
                        canonical_path,
                        Some(accountant.clone()),
                        cache_bytes,
                        runtime_dir,
                    )
                }
            }
        };

        match persistence {
            Ok(persistence) => {
                persistence.attach_memory_accountant(accountant.clone());
                return Ok((registry_reservation, Arc::new(persistence)));
            }
            Err(Error::DatabaseLocked { holder_pid, path })
                if holder_pid == std::process::id()
                    && path == canonical_path
                    && Instant::now() < retry_deadline =>
            {
                drop(registry_reservation);
                wait_for_open_registry_change(Duration::from_millis(1));
            }
            Err(Error::DatabaseLocked { holder_pid, path })
                if holder_pid != std::process::id()
                    && path == canonical_path
                    && Instant::now() < cross_process_retry_deadline =>
            {
                drop(registry_reservation);
                wait_for_open_registry_change(Duration::from_millis(1));
            }
            Err(Error::ReadFailure(failure))
                if failure.kind() == ReadFailureKind::HeldByWriter
                    && Instant::now() < cross_process_retry_deadline =>
            {
                drop(registry_reservation);
                wait_for_open_registry_change(Duration::from_millis(1));
            }
            // HeldByReaders deliberately falls through: Redb has already
            // proved reader contention, so writer open refuses on this one
            // attempt instead of spending the writer-settle retry window.
            Err(err) => return Err(err),
        }
    }
}

pub(super) fn wait_for_open_registry_change(timeout: Duration) {
    let registry = open_file_registry();
    let mut entries = registry.entries.lock();
    registry.waiters.wait_for(&mut entries, timeout);
}

pub(super) fn release_open_registry_path(path: &Path) {
    let registry = open_file_registry();
    {
        let mut entries = registry.entries.lock();
        if entries
            .get(path)
            .copied()
            .is_some_and(|state| state.pid() == std::process::id())
        {
            entries.remove(path);
            registry.waiters.notify_all();
        }
    }
    // The file is released, so a scratch database's directory can go.
    crate::scratch::release(path);
}

pub(super) fn closed_database_error() -> Error {
    Error::Other(("database handle is closed".to_string()).into())
}

pub(super) fn bounded_snapshot_publication_timeout() -> Error {
    Error::ReadFailure(
        ReadFailure::new(
            ReadFailureKind::OwnerTimeout,
            ReadFailureDetail::Reason {
                reason: "the writer has not published a complete read snapshot".to_string(),
            },
        )
        .expect("owner timeout accepts a reason"),
    )
}

impl ReadRegistrationBudget {
    pub(crate) fn new(clock: Arc<dyn DeadlineClock>, started_ms: u64, active_ms: u64) -> Self {
        Self {
            clock,
            started_ms,
            active_ms,
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn remaining(&self) -> Result<Duration> {
        let elapsed = self
            .clock
            .now_ms()
            .checked_sub(self.started_ms)
            .ok_or_else(|| {
                Error::Other(("bounded deadline clock moved backwards".to_string()).into())
            })?;
        if elapsed > self.active_ms {
            return Err(Error::ReadFailure(ReadFailure::owner_limit_exceeded(
                contextdb_core::read_contract::OwnerLimitExceededDetail {
                    limit: contextdb_core::read_contract::ReadFailureLimit::ActiveMs,
                    value: self.active_ms,
                    required: None,
                    statement: None,
                },
            )));
        }
        // The established active-time contract admits the exact boundary;
        // one millisecond of wait allowance lets the next clock observation
        // decide whether it has crossed that boundary.
        Ok(Duration::from_millis(
            (self.active_ms - elapsed).saturating_add(1),
        ))
    }

    pub(super) fn elapsed_timeout(&self) -> Error {
        self.remaining()
            .err()
            .unwrap_or_else(bounded_snapshot_publication_timeout)
    }
}

impl Database {
    pub(crate) fn custody_runtime(&self) -> Option<crate::custody::policy::Runtime> {
        self.custody_runtime.lock().clone()
    }
    pub(crate) fn set_custody_runtime(
        &self,
        tenant: TenantId,
        node: String,
        signer: crate::custody::preparation::LineageSigner,
    ) {
        *self.custody_runtime.lock() = Some(crate::custody::policy::Runtime {
            tenant,
            node,
            signer,
        });
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn stage_delivery_registration(
        &self,
        tx: TxId,
        registration: crate::custody::preparation::SourceRegistration,

        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if let DatabaseTransactionHost::Owned(handle) = &self.tx_mgr {
            #[derive(serde::Serialize)]
            struct RegistrationInput<'a> {
                tenant: &'a TenantId,
                edge: &'a str,
                root: &'a crate::custody::records::RowRef,
                members: &'a [crate::custody::records::RowRef],
            }
            #[derive(serde::Deserialize)]
            struct RegistrationCopy {
                tenant: TenantId,
                edge: String,
                root: crate::custody::records::RowRef,
                members: Vec<crate::custody::records::RowRef>,
            }
            let result = handle.with_host(|host| {
                host.events.with_owned_persistence_original(|original| {
                    // Lend before source/metadata guards, and couple the flag and
                    // exact copied registration through the existing checkpoint.
                    let checkpoint = self.write_set_checkpoint(tx)?;
                    host.transactions.with_transactions(|transactions| {
                        transactions.with_engine(tx, |guard| {
                            crate::custody::preparation::validate_registration(
                                self,
                                guard,
                                &registration,
                                read_memory.clone(),
                            )
                        })
                    })?;
                    let encoded = RedbPersistence::encode_owned_event(
                        original,
                        &RegistrationInput {
                            tenant: &registration.tenant,
                            edge: &registration.edge,
                            root: &registration.root,
                            members: &registration.members,
                        },
                    )?;
                    let decoded = RedbPersistence::decode_owned_event::<RegistrationCopy>(
                        original,
                        &encoded.bytes,
                    )?;
                    let batch = AdmittedMetadataBatch::Delivery(MetadataBuffer::prepare_one(
                        crate::custody::preparation::SourceRegistration {
                            tenant: decoded.value.tenant,
                            edge: decoded.value.edge,
                            root: decoded.value.root,
                            members: decoded.value.members,
                            signer: registration.signer.clone(),
                        },
                        decoded.allocation,
                        original,
                    )?);
                    self.tx_mgr
                        .update_flags(tx, |flags| flags.requires_commit_lsn = true)?;
                    self.append_owned_metadata(tx, batch)?;
                    checkpoint.settle();
                    Ok(())
                })
            });
            if let Err(error) = result {
                self.rollback(tx)?;
                return Err(error);
            }
            return Ok(());
        }
        // A rejected registration aborts the writing transaction.
        let validated = self.tx_mgr.with_write_set(tx, |ws| {
            crate::custody::preparation::validate_registration(
                self,
                ws,
                &registration,
                read_memory.clone(),
            )?;
            ws.requires_commit_lsn = true;
            Ok(())
        })?;
        if let Err(error) = validated {
            self.rollback(tx)?;
            return Err(error);
        }
        self.pending_commit_metadata
            .lock()
            .entry(tx)
            .or_default()
            .delivery_registrations
            .push(registration);
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn commit_delivery_prepared(
        &self,
        tx: TxId,
        prepare: &mut dyn crate::custody::preparation::CommitPreparation,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.tx_mgr
            .update_flags(tx, |flags| flags.requires_commit_lsn = true)?;
        self.commit_with_source_and_sync_ddl_and_trigger_audit_projection(
            tx,
            CommitSource::User,
            &[],
            &[],
            None,
            None,
            false,
            Some(prepare),
            vector_copy_owner,
            read_memory.clone(),
        )
        .map(|_| ())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn rotate_custody_incarnation(
        &self,
        tenant: &TenantId,
        hub: &str,
        previous: Incarnation,
        next: Incarnation,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        use crate::custody::{canonical::*, preparation::*, records::*};
        let runtime = self.custody_runtime().ok_or_else(invalid)?;
        if runtime.tenant != *tenant || runtime.node != hub || self.retention_sync_peer().is_some()
        {
            return Err(invalid());
        }
        let signer = runtime.signer;
        let tx = self.begin()?;
        let mut installed = false;
        self.commit_delivery_prepared(
            tx,
            &mut crate::custody::preparation::restricted(|db, ws| {
                if db.existing_sync_incarnation(tenant, read_memory.clone())? != Some(previous) {
                    return Ok(Vec::new());
                }
                let state = StateRecord {
                    state: StateRef {
                        logical_store_id: uuid::Uuid::new_v4(),
                        incarnation: next,
                        birth_id: uuid::Uuid::new_v4(),
                        token: random_digest(),
                    },
                    parent: None,
                    sequence: 0,
                    retirement_root: crate::custody::authority::empty_history_root(),
                };
                let old = db.custody_records(read_memory.clone())?;
                let mut bindings = old
                    .iter()
                    .filter_map(|r| match r {
                        Record::Binding(b)
                            if b.namespace.tenant == *tenant
                                && b.namespace.hub_node == hub
                                && b.namespace.hub_incarnation == previous =>
                        {
                            Some(b.clone())
                        }
                        _ => None,
                    })
                    .collect::<Vec<_>>();
                // A declared snapshot can predate every binding.
                // The authenticated hub runtime already owns the tenant and identity.
                let mut records =
                    state_records(db, state.clone(), tenant, hub, read_memory.clone())?;
                for b in &mut bindings {
                    b.namespace.hub_incarnation = next;
                    b.issuance_state = state.state.clone();
                    b.issuance_source = (ws.commit_lsn().ok_or_else(invalid)?, 0);
                    b.signature = signer(&b.bytes()?)?;
                    records.push(Record::Binding(b.clone()));
                }
                ws.push_config((
                    tenant.config_key("sync_incarnation"),
                    RedbPersistence::encode_config_value(&next.to_hex())?,
                ))?;
                installed = true;
                Ok(records)
            }),
            vector_copy_owner,
            read_memory.clone(),
        )?;
        if installed {
            self.sync_incarnations
                .lock()
                .insert(tenant.as_str().into(), next);
        }
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn commit_delivery_metadata<F>(
        &self,
        prepare: F,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()>
    where
        F: FnOnce(Lsn) -> Result<Vec<crate::custody::records::Record>>,
    {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let tx = self.begin()?;
        let mut prepare = Some(prepare);
        self.commit_delivery_prepared(
            tx,
            &mut crate::custody::preparation::restricted(|_, ws| {
                prepare
                    .take()
                    .ok_or_else(crate::custody::canonical::invalid)?(
                    ws.commit_lsn()
                        .ok_or_else(crate::custody::canonical::invalid)?,
                )
            }),
            vector_copy_owner,
            read_memory.clone(),
        )
    }

    /// A snapshot-in-time read of "is this handle closed right now" — reads
    /// the SAME two atomics `open_operation` checks, but takes NO lock and
    /// holds nothing: unlike `open_operation`'s guard, this cannot be held
    /// across a park/wait. Exists ONLY to let a `Result`-returning surface
    /// reject an ALREADY-closed handle before doing any work that would
    /// otherwise panic on it (`execute`'s snapshot-registration pre-flight,
    /// which resolves through the panicking public `Database::snapshot`) —
    /// never as a substitute for `open_operation`'s own real guard, whose
    /// `operation_gate` read lock is exactly what a parked waiter (e.g. a
    /// `BEGIN` waiting on an in-flight trigger callback) must NOT hold for
    /// the duration of its wait, since `close()` needs that gate's WRITE
    /// lock to ever complete. Racing this check against a close in
    /// progress is fine either way: a `false` here just means the function
    /// proceeds to its own real, later `open_operation()`/wait logic, which
    /// resolves the race correctly either way.
    pub(super) fn already_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst) || self.resource_closed.load(Ordering::SeqCst)
    }
}

impl std::fmt::Debug for Database {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Database")
            .field("instance_id", &self.instance_id)
            .finish_non_exhaustive()
    }
}

impl SubscriptionState {
    pub(super) fn new() -> Self {
        Self {
            subscribers: Vec::new(),
            events_sent: 0,
            events_dropped: 0,
        }
    }
}

impl MaintenanceWorkerClock {
    /// Wake the real maintenance wait in every build. Shutdown callers set
    /// the flag first, then notify, so a worker either observes shutdown
    /// before waiting or is released from this wait to observe it.
    pub(super) fn wake(&self) {
        // Pair notification with the worker's predicate check under this
        // mutex. `wait_for` releases the mutex as it begins to wait, so the
        // notifier cannot slip between that check and the wait.
        let _state = self.state.lock();
        self.changed.notify_all();
    }

    pub(super) fn reschedule(&self) {
        let _state = self.state.lock();
        self.changed.notify_all();
    }

    pub(super) fn wait_for_cycle(
        &self,
        shutdown: &AtomicBool,
        interval_ms: &AtomicU64,
    ) -> Option<u64> {
        let mut state = self.state.lock();
        loop {
            if shutdown.load(Ordering::SeqCst) {
                #[cfg(any(test, feature = "test-seams"))]
                {
                    state.waiting_while_held = false;
                }
                self.changed.notify_all();
                return None;
            }
            #[cfg(any(test, feature = "test-seams"))]
            if state.held {
                state.waiting_while_held = true;
                self.changed.notify_all();
                if state.requested_serial > state.completed_serial {
                    state.waiting_while_held = false;
                    return Some(state.requested_serial);
                }
                self.changed.wait(&mut state);
                continue;
            }
            let interval = Duration::from_millis(interval_ms.load(Ordering::SeqCst).max(1));
            if self.changed.wait_for(&mut state, interval).timed_out() {
                return Some(0);
            }
        }
    }

    pub(super) fn complete(&self, _serial: u64) {
        #[cfg(any(test, feature = "test-seams"))]
        if _serial != 0 {
            let mut state = self.state.lock();
            state.completed_serial = _serial;
            state.completed_thread_id = Some(thread::current().id());
            self.changed.notify_all();
        }
    }

    pub(super) fn mark_worker_exited(&self) {
        #[cfg(any(test, feature = "test-seams"))]
        {
            let mut state = self.state.lock();
            state.waiting_while_held = false;
            state.worker_exited = true;
        }
        self.changed.notify_all();
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub(super) fn hold(&self) {
        self.state.lock().held = true;
        self.changed.notify_all();
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub(super) fn release(&self) {
        self.state.lock().held = false;
        self.changed.notify_all();
    }

    #[cfg(test)]
    pub(super) fn wait_until_held_asleep(&self) {
        let mut state = self.state.lock();
        while !state.waiting_while_held && !state.worker_exited {
            self.changed.wait(&mut state);
        }
    }

    #[cfg(test)]
    pub(super) fn worker_exited(&self) -> bool {
        self.state.lock().worker_exited
    }

    #[cfg(any(test, feature = "test-seams"))]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub(super) fn wake_once(
        &self,
        shutdown: &AtomicBool,
    ) -> Result<VectorMaintenanceWorkerCycleForTest> {
        let mut state = self.state.lock();
        if !state.held {
            return Err(Error::Other(
                ("engine-owned vector maintenance clock is not held".to_string()).into(),
            ));
        }
        state.requested_serial = state.requested_serial.saturating_add(1);
        let requested = state.requested_serial;
        self.changed.notify_all();
        while state.completed_serial < requested && !shutdown.load(Ordering::SeqCst) {
            self.changed.wait(&mut state);
        }
        if state.completed_serial < requested {
            return Err(Error::Other(
                ("engine-owned vector maintenance worker stopped before completing its wake"
                    .to_string())
                .into(),
            ));
        }
        Ok(VectorMaintenanceWorkerCycleForTest {
            cycle_serial: requested,
            worker_thread_id: state.completed_thread_id.ok_or_else(|| {
                Error::Other(
                    ("maintenance wake completed without a worker identity".to_string()).into(),
                )
            })?,
        })
    }
}

#[cfg(any(test, feature = "test-seams"))]
impl VectorMaintenanceWorkerClockGuardForTest {
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn wake_once(&self) -> Result<VectorMaintenanceWorkerCycleForTest> {
        self.clock.wake_once(&self.shutdown)
    }

    #[cfg(test)]
    pub(super) fn wait_until_held_asleep(&self) {
        self.clock.wait_until_held_asleep();
    }

    #[cfg(test)]
    pub(super) fn worker_exited(&self) -> bool {
        self.clock.worker_exited()
    }
}

#[cfg(any(test, feature = "test-seams"))]
impl Drop for VectorMaintenanceWorkerClockGuardForTest {
    fn drop(&mut self) {
        self.clock.release();
    }
}

impl SnapshotPinPool {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn acquire(
        self: &Arc<Self>,
        reader: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> contextdb_core::Result<SnapshotPinSlot> {
        let reader: contextdb_core::read_memory::ReadMemoryOwner = reader.into();

        let mut available = self.available.load(Ordering::SeqCst);
        loop {
            if available == 0 {
                return contextdb_core::read_memory::ReadCredit::try_new(
                    reader,
                    self.slot_bytes,
                    "snapshot_pin",
                    "admit_pin",
                    "Drop a SnapshotPin or raise the declared reader memory limit.",
                )
                .map(|credit| SnapshotPinSlot::Reader { _credit: credit });
            }
            match self.available.compare_exchange(
                available,
                available - 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return Ok(SnapshotPinSlot::Prepaid(self.clone())),
                Err(observed) => available = observed,
            }
        }
    }
}

impl Drop for SnapshotPinSlot {
    fn drop(&mut self) {
        if let Self::Prepaid(pool) = self {
            pool.available.fetch_add(1, Ordering::SeqCst);
        }
    }
}

impl Default for SnapshotFloorRegistry {
    fn default() -> Self {
        Self {
            next_token: AtomicU64::new(0),
            construction_floors: Mutex::new(HashMap::new()),
            state: Arc::new(Mutex::new(SnapshotFloorState::default())),
            pin_slots: Mutex::new(None),
            pending_delete_source: Mutex::new(None),
            pass_finished: Arc::new(Condvar::new()),
            removal_pass_test_pause: Arc::new(ApplyPhasePause::new()),
            #[cfg(any(test, feature = "test-seams"))]
            pull_capture_test_pause: Arc::new(ApplyPhasePause::new()),
            #[cfg(test)]
            durable_admission_test_pause: Arc::new(ApplyPhasePause::new()),
            #[cfg(test)]
            owner_cursor_busy_test_pause: Arc::new(ApplyPhasePause::new()),
        }
    }
}

impl SnapshotFloorRegistry {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn fund_pin_slots(
        &self,
        accountant: crate::memory_accounting::AccountantHandle,
    ) -> Result<()> {
        let mut configured = self.pin_slots.lock();
        if configured.is_some() {
            return Ok(());
        }
        let slots = contextdb_core::read_contract::ReadLimits::SHIPPED_SNAPSHOT_PIN_SLOTS;
        let slot_bytes = std::mem::size_of::<RegisteredSnapshot>()
            .saturating_add(std::mem::size_of::<SnapshotRegistration>())
            .saturating_add(3 * std::mem::size_of::<usize>());
        let bytes = slots.saturating_mul(slot_bytes);
        // This fixed capacity belongs to opening the engine, like the other
        // assembly controls. It must not consume write payload headroom.
        let credit = crate::memory_accounting::ControlCredit::admit_open_residue(
            &crate::memory_accounting::StoreAccountantHandle::new(accountant),
            bytes,
            "snapshot_pin_slots_open_residue",
        )?;
        self.state.lock().active.reserve(slots);
        *configured = Some(Arc::new(SnapshotPinPool {
            available: AtomicUsize::new(slots),
            slots,
            slot_bytes,
            _credit: credit,
        }));
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn acquire_pin_slot(
        &self,
        reader: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> contextdb_core::Result<SnapshotPinSlot> {
        self.pin_slots
            .lock()
            .as_ref()
            .expect("snapshot pin pool is funded when the database opens")
            .acquire(reader)
    }
}

impl RegisteredSnapshot {
    /// Whether removal must still defer to this registration. A holder past
    /// its own idle deadline is refused its next page, so deferring to it
    /// would hold rows back for a reader that can never read them again.
    pub(super) fn holder_still_entitled(&self) -> bool {
        self.idle_window
            .as_ref()
            .is_none_or(|window| window.holder_still_entitled())
    }
}
