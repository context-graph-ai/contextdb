//! Exact row-version ownership of automatically derived graph adjacency.
use super::*;
use contextdb_graph::source::GraphQuery;
use contextdb_tx::staged_source::{AuxBody, AuxBorrow};
use contextdb_tx::{OwnedAdjEntry, RowEdgeOwner};

#[derive(Debug, Default)]
pub(super) struct LegacyRowGraphJournal {
    state: Mutex<LegacyRowGraphJournalState>,
    _control: Option<OwnedMemoryReservation>,
}
#[derive(Debug, Default)]
struct LegacyRowGraphJournalState {
    sequence: usize,
    head: Option<Box<LegacyRowGraphUndo>>,
}
#[derive(Debug)]
struct LegacyRowGraphUndo {
    sequence: usize,
    action: Option<LegacyRowGraphAction>,
    next: Option<Box<LegacyRowGraphUndo>>,
    _control: OwnedMemoryReservation,
}
#[derive(Debug)]
enum LegacyRowGraphAction {
    Insert {
        at: usize,
    },
    Remove {
        at: usize,
        body: OwnedAdjEntry,
        charge: Arc<crate::memory_accounting::TransferredRowReservation>,
    },
}
impl Drop for LegacyRowGraphJournalState {
    fn drop(&mut self) {
        while let Some(mut undo) = self.head.take() {
            self.head = undo.next.take();
        }
    }
}

impl Database {
    pub(crate) fn resident_row_graph_bytes_for_table_drop(&self, table: &str) -> usize {
        if self.graph_store.is_source_backed() {
            return 0;
        }
        self.graph_store
            .forward_adj
            .read()
            .values()
            .flatten()
            .filter(|edge| {
                edge.row_owner
                    .as_ref()
                    .is_some_and(|owner| owner.table == table)
            })
            .fold(0usize, |bytes, edge| {
                bytes.saturating_add(edge.estimated_bytes())
            })
    }

    /// Existing graph proof seams cover standalone contributions. This
    /// production-dead fixture hook covers missing row-owned artifacts.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    #[cfg_attr(
        any(test, feature = "test-seams"),
        expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )
    )]
    pub fn remove_row_graph_witness_for_test(
        path: &Path,
        table: &str,
        row: RowId,
        reverse: bool,
    ) -> Result<()> {
        RedbPersistence::remove_row_graph_witness_for_test(path, table, row, reverse)
    }

    pub(super) fn legacy_row_graph_marker(&self, tx: TxId) -> usize {
        self.pending_commit_metadata
            .lock()
            .get(&tx)
            .and_then(|m| m.row_graph_undo.as_ref())
            .map_or(0, |journal| journal.state.lock().sequence)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn prepare_legacy_row_graph_undo(
        &self,
        tx: TxId,
    ) -> Result<Option<(Arc<LegacyRowGraphJournal>, Box<LegacyRowGraphUndo>)>> {
        if !self
            .tx_mgr
            .with_write_set(tx, |ws| ws.ordered_staged_rows.checkpoint_active())?
        {
            return Ok(None);
        }
        let mut pending = self.pending_commit_metadata.lock();
        let metadata = pending.entry(tx).or_default();
        if metadata.row_graph_undo.is_none() {
            let control = OwnedMemoryReservation::try_new_for(
                crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
                std::mem::size_of::<LegacyRowGraphJournal>() + 2 * std::mem::size_of::<usize>(),
                "graph",
                "row_edge_undo",
                "Reduce staged graph rows or raise MEMORY_LIMIT.",
            )?;
            metadata.row_graph_undo = Some(Arc::new(LegacyRowGraphJournal {
                state: Mutex::new(LegacyRowGraphJournalState::default()),
                _control: Some(control),
            }));
        }
        let journal = metadata
            .row_graph_undo
            .as_ref()
            .expect("admitted journal")
            .clone();
        drop(pending);
        let control = OwnedMemoryReservation::try_new_for(
            crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
            std::mem::size_of::<LegacyRowGraphUndo>(),
            "graph",
            "row_edge_undo",
            "Reduce staged graph rows or raise MEMORY_LIMIT.",
        )?;
        let sequence = journal
            .state
            .lock()
            .sequence
            .checked_add(1)
            .ok_or_else(|| Error::Other("row graph journal sequence overflow".into()))?;
        Ok(Some((
            journal,
            Box::new(LegacyRowGraphUndo {
                sequence,
                action: None,
                next: None,
                _control: control,
            }),
        )))
    }

    pub(super) fn restore_legacy_row_graph_journal(
        &self,
        journal: Option<&Arc<LegacyRowGraphJournal>>,
        marker: usize,
        ws: &mut WriteSet,
    ) {
        use contextdb_tx::StagedPostingCharge;
        let Some(journal) = journal else {
            return;
        };
        let mut state = journal.state.lock();
        while state
            .head
            .as_ref()
            .is_some_and(|undo| undo.sequence > marker)
        {
            let mut undo = state.head.take().expect("checked undo");
            state.head = undo.next.take();
            match undo.action.take().expect("published graph inverse") {
                LegacyRowGraphAction::Insert { at } => {
                    let body = ws.row_adj_inserts.remove(at);
                    self.finalization.accountant.release(body.estimated_bytes());
                }
                LegacyRowGraphAction::Remove { at, body, charge } => {
                    ws.row_adj_inserts.insert(at, body);
                    charge.transfer_back();
                }
            }
        }
        state.sequence = marker;
    }

    pub(super) fn settle_legacy_row_graph_journal(&self, tx: TxId) {
        let active = self
            .tx_mgr
            .with_write_set(tx, |ws| ws.ordered_staged_rows.checkpoint_active());
        if matches!(active, Ok(false))
            && let Some(metadata) = self.pending_commit_metadata.lock().get_mut(&tx)
        {
            metadata.row_graph_undo = None;
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn stage_row_graph_insert(
        &self,
        tx: TxId,
        table: &str,
        row: RowId,
        values: &HashMap<ColName, Value>,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let meta = self
            .table_meta(table)
            .ok_or_else(|| Error::TableNotFound(table.into()))?;
        if !crate::executor::has_edge_columns(&meta) {
            return Ok(());
        }
        let (Some(Value::Uuid(source)), Some(Value::Uuid(target)), Some(Value::Text(kind))) = (
            values.get("source_id"),
            values.get("target_id"),
            values.get("edge_type"),
        ) else {
            return Ok(());
        };
        let snapshot = self.snapshot_for_read();
        self.assert_node_write_allowed(*source, snapshot, read_memory.clone())?;
        self.assert_node_write_allowed(*target, snapshot, read_memory.clone())?;
        let generation =
            self.transaction_lineage_table_generation(tx, table, read_memory.clone())?;
        // Original strings are funded before allocation. Native array/header funding
        // is supplied by the auxiliary splice, independently of these bodies.
        let mut admission = OwnedMemoryReservation::try_new_for(
            crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
            table.len().saturating_add(kind.len()),
            "graph",
            "row_edge_body",
            "Reduce staged graph rows or raise MEMORY_LIMIT.",
        )?;
        let owned = OwnedAdjEntry {
            owner: RowEdgeOwner {
                table: table.to_owned(),
                table_generation: generation,
                row_id: row,
                created_tx: tx,
                lsn: Lsn(0),
            },
            entry: AdjEntry {
                source: *source,
                target: *target,
                edge_type: kind.to_owned(),
                properties: HashMap::new(),
                created_tx: tx,
                deleted_tx: None,
                lsn: Lsn(0),
            },
        };
        self.graph.validate_row_edge(tx, &owned, read_memory)?;
        match &self.tx_mgr {
            DatabaseTransactionHost::Owned(handle) => handle.with_host(|host| {
                host.transactions.with_transactions(|transactions| {
                    transactions.with_engine(tx, |guard| {
                        let at = guard.read().row_adj_inserts.len();
                        let invoice = admission.into_source_aux_invoice_for(&host.funder)?;
                        let batch =
                            guard.prepare_owned_aux(AuxBody::RowAdjInsert(owned), invoice)?;
                        let prepared = guard.prepare_aux_splice(at, &[], batch)?;
                        guard.publish_tx(prepared);
                        Ok(())
                    })
                })
            }),
            DatabaseTransactionHost::Legacy(_) => {
                let undo = self.prepare_legacy_row_graph_undo(tx)?;
                let retained = owned.estimated_bytes();
                self.finalization.accountant.try_allocate(retained)?;
                let result = self.tx_mgr.with_write_set(tx, |ws| {
                    if let Some((journal, mut undo)) = undo {
                        let mut state = journal.state.lock();
                        undo.action = Some(LegacyRowGraphAction::Insert {
                            at: ws.row_adj_inserts.len(),
                        });
                        undo.next = state.head.take();
                        state.sequence = undo.sequence;
                        state.head = Some(undo);
                    }
                    ws.row_adj_inserts.push(owned)
                });
                if result.is_err() {
                    self.finalization.accountant.release(retained);
                }
                result
            }
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn retire_row_graph(
        &self,
        tx: TxId,
        table: &str,
        row_id: RowId,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        // Remove a staged successor through the matching admitted mutation
        // capability. Its inverse stays with every active checkpoint.
        loop {
            let at = self.tx_mgr.with_read(tx, |ws| {
                ws.row_adj_inserts
                    .iter()
                    .rposition(|e| e.owner.table == table && e.owner.row_id == row_id)
            })?;
            let Some(_) = at else {
                break;
            };
            match &self.tx_mgr {
                DatabaseTransactionHost::Owned(handle) => {
                    handle.with_host(|host| {
                        host.transactions.with_transactions(|transactions| {
                            transactions.with_engine(tx, |guard| {
                                let Some(at) = guard.read().row_adj_inserts.iter().rposition(|e| {
                                    e.owner.table == table && e.owner.row_id == row_id
                                }) else {
                                    return Ok(());
                                };
                                let batch = guard.copy_aux(AuxBorrow::RowAdjInserts(&[]))?;
                                let prepared = guard.prepare_aux_splice(at, &[at], batch)?;
                                guard.publish_tx(prepared);
                                Ok(())
                            })
                        })
                    })?
                }
                DatabaseTransactionHost::Legacy(_) => {
                    let undo = self.prepare_legacy_row_graph_undo(tx)?;
                    // Prepare only the transfer control outside the write-set
                    // guard. The payload still owns its credit until removal.
                    let transferred = undo
                        .as_ref()
                        .map(|_| {
                            crate::memory_accounting::TransferredRowReservation::try_from_admitted(
                                crate::memory_accounting::AccountantHandle::clone(
                                    &self.finalization.accountant,
                                ),
                                0,
                            )
                        })
                        .transpose()?;
                    self.tx_mgr.with_write_set(tx, |ws| {
                        let Some(at) = ws
                            .row_adj_inserts
                            .iter()
                            .rposition(|e| e.owner.table == table && e.owner.row_id == row_id)
                        else {
                            return;
                        };
                        let body = ws.row_adj_inserts.remove(at);
                        let charge = body.estimated_bytes();
                        if let Some(transferred) = &transferred {
                            transferred.adopt(charge);
                        }
                        if let Some((journal, mut undo)) = undo {
                            let mut state = journal.state.lock();
                            undo.action = Some(LegacyRowGraphAction::Remove {
                                at,
                                body,
                                charge: transferred.expect("prepared inverse charge"),
                            });
                            undo.next = state.head.take();
                            state.sequence = undo.sequence;
                            state.head = Some(undo);
                        } else {
                            drop(body);
                            self.finalization.accountant.release(charge);
                        }
                    })?;
                }
            }
        }
        let Some(row) =
            self.committed_row_at(table, row_id, self.snapshot_for_read(), read_memory.clone())?
        else {
            return Ok(());
        };
        let generation =
            self.transaction_lineage_table_generation(tx, table, read_memory.clone())?;
        self.with_row_graph_witness(table, generation, &row, read_memory, |edge, owner| {
            let exists = self.tx_mgr.with_read(tx, |ws| {
                ws.row_adj_deletes
                    .iter()
                    .any(|(before, _)| before.owner == *owner)
            })?;
            if exists {
                return Ok(());
            }
            let mut admission = OwnedMemoryReservation::try_new_for(
                crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
                owner.table.len().saturating_add(edge.edge_type.len()),
                "graph",
                "row_edge_delete",
                "Reduce staged graph rows or raise MEMORY_LIMIT.",
            )?;
            let before = OwnedAdjEntry {
                owner: owner.clone(),
                entry: edge.clone(),
            };
            match &self.tx_mgr {
                DatabaseTransactionHost::Owned(handle) => handle.with_host(|host| {
                    host.transactions.with_transactions(|transactions| {
                        transactions.with_engine(tx, |guard| {
                            let at = guard.read().row_adj_deletes.len();
                            let invoice = admission.into_source_aux_invoice_for(&host.funder)?;
                            let batch = guard
                                .prepare_owned_aux(AuxBody::RowAdjDelete((before, tx)), invoice)?;
                            let prepared = guard.prepare_aux_splice(at, &[], batch)?;
                            guard.publish_tx(prepared);
                            Ok(())
                        })
                    })
                }),
                DatabaseTransactionHost::Legacy(_) => {
                    let bytes = before.estimated_bytes();
                    self.finalization.accountant.try_allocate(bytes)?;
                    let result = self
                        .tx_mgr
                        .with_write_set(tx, |ws| ws.row_adj_deletes.push((before, tx)));
                    if result.is_err() {
                        self.finalization.accountant.release(bytes);
                    }
                    result
                }
            }
        })?;
        Ok(())
    }

    /// Borrow the selected version through its exact owner key in both storage
    /// modes. Missing ownership is corruption, never a fallback to a sibling.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn with_row_graph_witness<R>(
        &self,
        table: &str,
        generation: u64,
        row: &VersionedRow,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        use_witness: impl FnOnce(&AdjEntry, &RowEdgeOwner) -> Result<R>,
    ) -> Result<Option<R>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let meta = self
            .table_meta(table)
            .ok_or_else(|| Error::TableNotFound(table.into()))?;
        if !crate::executor::has_edge_columns(&meta) {
            return Ok(None);
        }
        let (Some(Value::Uuid(source)), Some(Value::Uuid(target)), Some(Value::Text(kind))) = (
            row.values.get("source_id"),
            row.values.get("target_id"),
            row.values.get("edge_type"),
        ) else {
            return Ok(None);
        };
        let mut memory = contextdb_core::read_memory::ReadCredit::new(read_memory.clone());
        memory.try_grow_for(
            table.len(),
            "graph",
            "row_edge_owner",
            "Raise MEMORY_LIMIT to read this row's graph witness.",
        )?;
        let owner = RowEdgeOwner {
            table: table.to_owned(),
            table_generation: generation,
            row_id: row.row_id,
            created_tx: row.created_tx,
            lsn: row.lsn,
        };
        let root = self.graph_snapshot(self.snapshot_for_read(), read_memory)?;
        let mut callback = Some(use_witness);
        let mut result = None;
        self.graph_store.visit_row_edges(
            root.as_ref(),
            GraphQuery {
                node: *source,
                direction: Direction::Outgoing,
                identity: Some((*target, kind)),
            },
            &owner,
            |edge| {
                if edge.source != *source
                    || edge.target != *target
                    || edge.edge_type != *kind
                    || edge.created_tx != row.created_tx
                    || edge.lsn != row.lsn
                    || edge.deleted_tx != row.deleted_tx
                    || !edge.properties.is_empty()
                {
                    return Err(Error::SyncError(
                        "row-derived graph witness disagrees with its owning row".into(),
                    ));
                }
                let f = callback
                    .take()
                    .ok_or_else(|| Error::SyncError("duplicate exact row graph witness".into()))?;
                result = Some(f(edge, &owner)?);
                Ok(true)
            },
        )?;
        if callback.is_some() {
            return Err(Error::SyncError(
                "owning edge row has no exact graph witness".into(),
            ));
        }
        Ok(result)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn selected_row_graph_witnesses(
        &self,
        selections: &[AuthoritativePurgeSelection],
        rows: &BTreeMap<String, Vec<VersionedRow>>,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<(Vec<OwnedAdjEntry>, contextdb_core::read_memory::ReadCredit)> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let count: usize = rows.values().map(Vec::len).sum();
        let mut memory = contextdb_core::read_memory::ReadCredit::new(read_memory.clone());
        memory.try_grow_for(
            count.saturating_mul(std::mem::size_of::<OwnedAdjEntry>()),
            "graph",
            "selected_row_edges",
            "Raise MEMORY_LIMIT to prepare selected row witnesses.",
        )?;
        let mut selected = Vec::with_capacity(count);
        for candidate in selections {
            for row in &rows[&candidate.table] {
                if row.row_id != candidate.local_row_id {
                    continue;
                }
                self.with_row_graph_witness(
                    &candidate.table,
                    candidate.table_generation,
                    row,
                    read_memory.clone(),
                    |edge, owner| {
                        memory.try_grow_for(
                            owner.table.len().saturating_add(edge.edge_type.len()),
                            "graph",
                            "selected_row_edges",
                            "Raise MEMORY_LIMIT to prepare selected row witnesses.",
                        )?;
                        selected.push(OwnedAdjEntry {
                            owner: owner.clone(),
                            entry: edge.clone(),
                        });
                        Ok(())
                    },
                )?;
            }
        }
        Ok((selected, memory))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn copy_row_graph_entries(
        &self,
        entries: &[OwnedAdjEntry],
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<(Vec<OwnedAdjEntry>, contextdb_core::read_memory::ReadCredit)> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let mut memory = contextdb_core::read_memory::ReadCredit::new(read_memory);
        let bytes = entries.iter().fold(
            entries
                .len()
                .saturating_mul(std::mem::size_of::<OwnedAdjEntry>()),
            |n, e| {
                n.saturating_add(e.owner.table.len())
                    .saturating_add(e.entry.edge_type.len())
            },
        );
        memory.try_grow_for(
            bytes,
            "graph",
            "purge_projection",
            "Raise MEMORY_LIMIT to retain the selected row witnesses.",
        )?;
        let mut copied = Vec::with_capacity(entries.len());
        for entry in entries {
            copied.push(entry.clone());
        }
        Ok((copied, memory))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn visit_row_graph_adjacency_at_root(
        &self,
        root: Option<crate::persistence::graph_read::EngineGraphReadRoot>,
        mut visit: impl FnMut(&AdjEntry, &RowEdgeOwner) -> Result<()>,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if let Some(root) = root {
            let mut cursor = root.cursor_lane(
                false,
                true,
                std::ops::Bound::Unbounded,
                std::ops::Bound::Unbounded,
                read_memory,
            )?;
            while let Some(window) = cursor.next_edge_with(&|| Ok(()))? {
                let owner = window
                    .row_owner()
                    .ok_or_else(|| Error::SyncError("row graph lane lost ownership".into()))?;
                visit(window.edge(), owner)?;
            }
        } else {
            for edge in self.graph_store.forward_adj.read().values().flatten() {
                if let Some(owner) = &edge.row_owner {
                    visit(&edge.entry, owner)?;
                }
            }
        }
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn row_graph_witnesses_for_table_drop(
        &self,
        table: &str,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Vec<OwnedAdjEntry>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if self.persistence.is_none() {
            return Ok(Vec::new());
        }
        let generation = self.durable_lineage_table_generation(table, read_memory.clone())?;
        let mut memory = contextdb_core::read_memory::ReadCredit::new(read_memory.clone());
        let mut selected = Vec::new();
        self.visit_writer_rows(
            table,
            |row| {
                self.with_row_graph_witness(
                    table,
                    generation,
                    row,
                    read_memory.clone(),
                    |edge, owner| {
                        memory.try_grow_for(
                            std::mem::size_of::<OwnedAdjEntry>()
                                .saturating_mul(2)
                                .saturating_add(owner.table.len())
                                .saturating_add(edge.edge_type.len()),
                            "graph",
                            "table_drop_witness",
                            "Raise MEMORY_LIMIT to drop these row witnesses.",
                        )?;
                        selected.push(OwnedAdjEntry {
                            owner: owner.clone(),
                            entry: edge.clone(),
                        });
                        Ok(())
                    },
                )?;
                Ok::<_, Error>(())
            },
            read_memory.clone(),
        )?;
        self.retain_writer_output(memory);
        Ok(selected)
    }

    /// A surrogate id identifies the stored row. A wider business key
    /// distinguishes edge bindings; an exact triad key still names the triad.
    pub(super) fn row_graph_uses_triple(meta: &TableMeta) -> bool {
        let triple = |columns: &[String]| {
            columns.len() == 3
                && ["source_id", "target_id", "edge_type"]
                    .iter()
                    .all(|name| columns.iter().any(|column| column == name))
        };
        if triple(&meta.primary_key_columns)
            || meta.unique_constraints.iter().any(|key| triple(key))
        {
            return true;
        }
        meta.unique_constraints.is_empty()
            && meta.primary_key_columns.iter().all(|key| key == "id")
            && meta
                .natural_key_column
                .as_deref()
                .is_none_or(|key| key == "id")
            && meta
                .columns
                .iter()
                .all(|column| !column.primary_key || column.name == "id")
    }

    /// The graph door names only the triple. Literal/NULL defaults can name
    /// one wider-key row; volatile defaults cannot supply a stable identity.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error retains inline refusal diagnostics."
    )]
    pub(super) fn row_graph_aliases_standalone(
        &self,
        tx: Option<TxId>,
        owner: &RowEdgeOwner,
        snapshot: SnapshotId,
        memory: contextdb_core::read_memory::ReadMemoryOwner,
    ) -> Result<bool> {
        let captured = self.captured_graph_decisions();
        let live_meta;
        let meta = if let Some(captured) = &captured {
            captured.catalog.get(&owner.table)
        } else {
            live_meta = self.table_meta(&owner.table);
            live_meta.as_ref()
        }
        .ok_or_else(|| Error::TableNotFound(owner.table.clone()))?;
        if Self::row_graph_uses_triple(meta) {
            return Ok(true);
        }
        let extra_identity_column = |name: &str| {
            !["source_id", "target_id", "edge_type"].contains(&name)
                && (meta
                    .unique_constraints
                    .iter()
                    .any(|key| key.iter().any(|column| column == name))
                    || (meta.primary_key_columns.len() > 1 || name != "id")
                        && meta.primary_key_columns.iter().any(|column| column == name)
                    || name != "id"
                        && (meta.natural_key_column.as_deref() == Some(name)
                            || meta
                                .columns
                                .iter()
                                .any(|column| column.name == name && column.primary_key)))
        };
        // Do not evaluate NOW() (or any function) while reading an identity.
        // It describes no value that the graph-door caller actually wrote.
        if meta
            .columns
            .iter()
            .filter(|column| extra_identity_column(&column.name))
            .any(|column| {
                column.default.as_deref().is_some_and(|default| {
                    // Match the stored literal encoding, not words inside a
                    // text literal. Every non-constant expression stays distinct.
                    !(default.starts_with("Literal(")
                        || default.starts_with('\'') && default.ends_with('\'')
                        || default.eq_ignore_ascii_case("NULL")
                        || default.eq_ignore_ascii_case("TRUE")
                        || default.eq_ignore_ascii_case("FALSE")
                        || default.parse::<i64>().is_ok()
                        || default.parse::<f64>().is_ok())
                })
            })
        {
            return Ok(false);
        }
        let mut credit = contextdb_core::read_memory::ReadCredit::new(memory.clone());
        let row = self
            .bounded_row_by_identity(
                memory,
                tx,
                &owner.table,
                owner.row_id,
                snapshot,
                &mut || Ok(()),
                &mut |bytes| {
                    credit.try_grow_for(
                        bytes,
                        "graph",
                        "edge_default_identity",
                        "Raise MEMORY_LIMIT to read this edge's declared identity.",
                    )
                },
            )?
            .ok_or_else(|| Error::SyncError("visible graph owner has no exact row".into()))?;
        if row.created_tx != owner.created_tx || row.lsn != owner.lsn {
            return Err(Error::SyncError(
                "visible graph owner selects a different row version".into(),
            ));
        }
        for column in meta
            .columns
            .iter()
            .filter(|column| extra_identity_column(&column.name))
        {
            // The expression and its result remain charged with the exact row.
            credit.try_grow_for(
                column
                    .default
                    .as_ref()
                    .map_or(0, |value| value.len())
                    .saturating_mul(4)
                    .saturating_add(std::mem::size_of::<Value>()),
                "graph",
                "edge_default_identity",
                "Raise MEMORY_LIMIT to read this edge's declared identity.",
            )?;
            let expected = match &column.default {
                Some(default) => crate::executor::evaluate_stored_default_expr(default)?,
                None => Value::Null,
            };
            let expected = match (&column.column_type, expected) {
                (ColumnType::Real, Value::Int64(value)) => Value::Float64(value as f64),
                (_, value) => value,
            };
            let expected = crate::executor::coerce_into_column_with_meta(
                &owner.table,
                meta,
                &column.name,
                expected,
                None,
                tx,
            )?;
            if row.values.get(&column.name).unwrap_or(&Value::Null) != &expected {
                return Ok(false);
            }
        }
        Ok(true)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error retains inline refusal diagnostics."
    )]
    pub(super) fn row_graph_insert_allowed(
        &self,
        tx: TxId,
        table: &str,
        values: &HashMap<ColName, Value>,
        memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<bool> {
        let (Some(Value::Uuid(source)), Some(Value::Uuid(target)), Some(Value::Text(kind))) = (
            values.get("source_id"),
            values.get("target_id"),
            values.get("edge_type"),
        ) else {
            return Ok(true);
        };
        self.row_graph_identity_allowed(tx, table, RowId(0), *source, *target, kind, memory)
    }

    /// Probe only this triad, respecting the transaction's exact row deletes.
    /// The row keeps its witness; a graph-door property write keeps its own.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error retains inline refusal diagnostics."
    )]
    pub(super) fn has_implicit_row_graph_binding(
        &self,
        tx: TxId,
        source: NodeId,
        target: NodeId,
        kind: &str,
        memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<bool> {
        let memory = memory.into();
        let snapshot = self.snapshot_for_read();
        let root = self.graph_snapshot(snapshot, memory.clone())?;
        let mut found = false;
        self.graph_store.visit_edges_with_owner(
            root.as_ref(),
            GraphQuery {
                node: source,
                direction: Direction::Outgoing,
                identity: Some((target, kind)),
            },
            |edge, owner| {
                let Some(owner) = owner else {
                    return Ok(true);
                };
                let visible = edge.visible_at(snapshot)
                    && !self.tx_mgr.with_read(tx, |ws| {
                        contextdb_graph::source::GraphOverlay {
                            inserts: ws.adj_inserts,
                            deletes: ws.adj_deletes,
                            row_inserts: ws.row_adj_inserts,
                            row_deletes: ws.row_adj_deletes,
                        }
                        .hides_owned(edge, Some(owner))
                    })?;
                if visible
                    && self.row_graph_aliases_standalone(
                        Some(tx),
                        owner,
                        snapshot,
                        memory.clone(),
                    )?
                {
                    found = true;
                    return Ok(false);
                }
                Ok(true)
            },
        )?;
        if found {
            return Ok(true);
        }
        // Release the write-set borrow before resolving its exact row. Only
        // one funded owner is copied at a time, independent of sibling count.
        let mut at = 0;
        loop {
            let mut credit = contextdb_core::read_memory::ReadCredit::new(memory.clone());
            let candidate = self.tx_mgr.with_read(tx, |ws| {
                while let Some(edge) = ws.row_adj_inserts.get(at) {
                    at += 1;
                    if edge.entry.source == source
                        && edge.entry.target == target
                        && edge.entry.edge_type == kind
                    {
                        credit.try_grow_for(
                            edge.owner.table.len(),
                            "graph",
                            "edge_default_identity",
                            "Raise MEMORY_LIMIT to read this edge's declared identity.",
                        )?;
                        return Ok::<_, Error>(Some(edge.owner.clone()));
                    }
                }
                Ok(None)
            })??;
            let Some(owner) = candidate else {
                return Ok(false);
            };
            if self.row_graph_aliases_standalone(Some(tx), &owner, snapshot, memory.clone())? {
                return Ok(true);
            }
        }
    }

    /// Declared identities accept siblings. The existing no-key SQL rule
    /// still checks the logical triad, excluding the just-staged row.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn row_graph_identity_allowed(
        &self,
        tx: TxId,
        table: &str,
        row: RowId,
        source: NodeId,
        target: NodeId,
        kind: &str,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<bool> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if self
            .table_meta(table)
            .as_ref()
            .is_some_and(|meta| !Self::row_graph_uses_triple(meta))
        {
            return Ok(true);
        }
        let snapshot = self.snapshot_for_read();
        let root = self.graph_snapshot(snapshot, read_memory)?;
        let committed = self.tx_mgr.with_read(tx, |ws| {
            let overlay = contextdb_graph::source::GraphOverlay {
                inserts: ws.adj_inserts,
                deletes: ws.adj_deletes,
                row_inserts: ws.row_adj_inserts,
                row_deletes: ws.row_adj_deletes,
            };
            let mut exists = false;
            self.graph_store.visit_edges_with_owner(
                root.as_ref(),
                GraphQuery {
                    node: source,
                    direction: Direction::Outgoing,
                    identity: Some((target, kind)),
                },
                |edge, owner| {
                    if edge.visible_at(snapshot)
                        && !overlay.hides_owned(edge, owner)
                        && owner.is_some_and(|owner| owner.table == table && owner.row_id != row)
                    {
                        exists = true;
                        return Ok(false);
                    }
                    Ok(true)
                },
            )?;
            Ok::<_, Error>(
                exists
                    || ws.row_adj_inserts.iter().any(|e| {
                        e.entry.source == source
                            && e.entry.target == target
                            && e.entry.edge_type == kind
                            && e.owner.table == table
                            && e.owner.row_id != row
                    }),
            )
        })??;
        Ok(!committed)
    }
}

impl Database {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn prepare_row_graph_projection(
        &self,
        table: &str,
        row: &VersionedRow,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<crate::custody::preparation::OwnedPreparationValue<OwnedAdjEntry>>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let meta = self
            .table_meta(table)
            .ok_or_else(|| Error::TableNotFound(table.into()))?;
        if !crate::executor::has_edge_columns(&meta) {
            return Ok(None);
        }
        let (Some(Value::Uuid(source)), Some(Value::Uuid(target)), Some(Value::Text(kind))) = (
            row.values.get("source_id"),
            row.values.get("target_id"),
            row.values.get("edge_type"),
        ) else {
            return Ok(None);
        };
        let generation = self.durable_lineage_table_generation(table, read_memory)?;
        crate::custody::preparation::OwnedPreparationValue::try_produce(
            self,
            table.len().saturating_add(kind.len()),
            || {
                Ok(OwnedAdjEntry {
                    owner: RowEdgeOwner {
                        table: table.to_owned(),
                        table_generation: generation,
                        row_id: row.row_id,
                        created_tx: row.created_tx,
                        lsn: row.lsn,
                    },
                    entry: AdjEntry {
                        source: *source,
                        target: *target,
                        edge_type: kind.clone(),
                        properties: HashMap::new(),
                        created_tx: row.created_tx,
                        deleted_tx: row.deleted_tx,
                        lsn: row.lsn,
                    },
                })
            },
        )
        .map(Some)
    }

    /// Replace the exact witness using the caller's checkpoint and guard.
    /// Equal owner headers do not preserve an obsolete projected edge body.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn replace_row_graph_in_held_guard(
        &self,
        held: &mut contextdb_tx::manager::EngineGuard<'_>,
        table: &str,
        retired_row: RowId,
        successor: &VersionedRow,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let projection =
            self.prepare_row_graph_projection(table, successor, read_memory.clone())?;
        if projection.is_none() {
            return Ok(());
        }
        loop {
            let position = held.read().row_adj_inserts.iter().rposition(|entry| {
                entry.owner.table == table
                    && (entry.owner.row_id == retired_row || entry.owner.row_id == successor.row_id)
            });
            let Some(position) = position else {
                break;
            };
            let empty = held.copy_aux(AuxBorrow::RowAdjInserts(&[]))?;
            let prepared = held.prepare_aux_splice(position, &[position], empty)?;
            held.publish_tx(prepared);
        }
        let mut credit = contextdb_core::read_memory::ReadCredit::new(read_memory.clone());
        if let Some(before) = self.committed_row_at_admitted(
            table,
            successor.row_id,
            self.snapshot_for_read(),
            &mut credit,
            read_memory.clone(),
        )? {
            let generation = self.durable_lineage_table_generation(table, read_memory.clone())?;
            self.with_row_graph_witness(table, generation, &before, read_memory, |edge, owner| {
                if held
                    .read()
                    .row_adj_deletes
                    .iter()
                    .any(|(entry, _)| entry.owner == *owner)
                {
                    return Ok(());
                }
                let body = crate::custody::preparation::OwnedPreparationValue::try_produce(
                    self,
                    owner.table.len().saturating_add(edge.edge_type.len()),
                    || {
                        Ok((
                            OwnedAdjEntry {
                                owner: owner.clone(),
                                entry: edge.clone(),
                            },
                            successor.created_tx,
                        ))
                    },
                )?;
                let prepared = body.with_value(|body| {
                    let batch =
                        held.copy_aux(AuxBorrow::RowAdjDeletes(std::slice::from_ref(body)))?;
                    held.prepare_aux_splice(held.read().row_adj_deletes.len(), &[], batch)
                })?;
                held.publish_tx(prepared);
                Ok(())
            })?;
        }
        projection.expect("edge projection").with_value(|entry| {
            let batch = held.copy_aux(AuxBorrow::RowAdjInserts(std::slice::from_ref(entry)))?;
            let prepared =
                held.prepare_aux_splice(held.read().row_adj_inserts.len(), &[], batch)?;
            held.publish_tx(prepared);
            Ok(())
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn replace_row_graph_in_prepared_write_set(
        &self,
        ws: &mut WriteSet,
        table: &str,
        retired_row: RowId,
        successor: &VersionedRow,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let Some(projection) =
            self.prepare_row_graph_projection(table, successor, read_memory.clone())?
        else {
            return Ok(());
        };
        // Admit the successor before changing any already admitted entry.
        let successor = projection.with_value(|entry| -> Result<_> {
            self.finalization
                .accountant
                .try_allocate(entry.estimated_bytes())?;
            Ok(entry.clone())
        })?;
        let mut credit = contextdb_core::read_memory::ReadCredit::new(read_memory.clone());
        let prepared_delete = (|| -> Result<Option<OwnedAdjEntry>> {
            let Some(before) = self.committed_row_at_admitted(
                table,
                successor.owner.row_id,
                self.snapshot_for_read(),
                &mut credit,
                read_memory.clone(),
            )?
            else {
                return Ok(None);
            };
            let generation = self.durable_lineage_table_generation(table, read_memory.clone())?;
            self.with_row_graph_witness(table, generation, &before, read_memory, |edge, owner| {
                if ws
                    .row_adj_deletes
                    .iter()
                    .any(|(entry, _)| entry.owner == *owner)
                {
                    return Ok(None);
                }
                let bytes = edge
                    .estimated_bytes()
                    .saturating_add(owner.estimated_bytes().saturating_mul(2));
                self.finalization.accountant.try_allocate(bytes)?;
                Ok(Some(OwnedAdjEntry {
                    owner: owner.clone(),
                    entry: edge.clone(),
                }))
            })
            .map(Option::flatten)
        })();
        let deletion = match prepared_delete {
            Ok(value) => value,
            Err(error) => {
                self.finalization
                    .accountant
                    .release(successor.estimated_bytes());
                return Err(error);
            }
        };
        let mut position = 0;
        while position < ws.row_adj_inserts.len() {
            if ws.row_adj_inserts[position].owner.table == table
                && (ws.row_adj_inserts[position].owner.row_id == retired_row
                    || ws.row_adj_inserts[position].owner.row_id == successor.owner.row_id)
            {
                let old = ws.row_adj_inserts.remove(position);
                self.finalization.accountant.release(old.estimated_bytes());
            } else {
                position += 1;
            }
        }
        if let Some(deletion) = deletion {
            ws.row_adj_deletes
                .push((deletion, successor.entry.created_tx));
        }
        ws.row_adj_inserts.push(successor);
        Ok(())
    }
}

pub(super) fn row_graph_matches_final_row(
    entry: &OwnedAdjEntry,
    table: &str,
    row: &VersionedRow,
) -> bool {
    entry.owner.table == table
        && entry.owner.row_id == row.row_id
        && entry.owner.created_tx == row.created_tx
        && entry.owner.lsn == row.lsn
        && entry.entry.created_tx == row.created_tx
        && entry.entry.lsn == row.lsn
        && row.values.get("source_id") == Some(&Value::Uuid(entry.entry.source))
        && row.values.get("target_id") == Some(&Value::Uuid(entry.entry.target))
        && row.values.get("edge_type").and_then(Value::as_text)
            == Some(entry.entry.edge_type.as_str())
}
impl Database {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn canonicalize_row_graph_in_held_guard(
        &self,
        held: &mut contextdb_tx::manager::EngineGuard<'_>,
    ) -> Result<()> {
        let mut position = held.read().row_adj_inserts.len();
        while position != 0 {
            position -= 1;
            let read = held.read();
            let entry = &read.row_adj_inserts[position];
            let valid = read
                .relational_inserts
                .iter()
                .rev()
                .find(|(table, row)| {
                    *table == entry.owner.table && row.row_id == entry.owner.row_id
                })
                .is_some_and(|(table, row)| row_graph_matches_final_row(entry, table, row));
            if !valid {
                let empty = held.copy_aux(AuxBorrow::RowAdjInserts(&[]))?;
                let prepared = held.prepare_aux_splice(position, &[position], empty)?;
                held.publish_tx(prepared);
            }
        }
        Ok(())
    }
    pub(super) fn canonicalize_row_graph_in_prepared_write_set(&self, ws: &mut WriteSet) {
        let mut position = ws.row_adj_inserts.len();
        while position != 0 {
            position -= 1;
            let entry = &ws.row_adj_inserts[position];
            let valid = ws
                .relational_inserts
                .iter()
                .rev()
                .find(|(table, row)| {
                    *table == entry.owner.table && row.row_id == entry.owner.row_id
                })
                .is_some_and(|(table, row)| row_graph_matches_final_row(entry, table, row));
            if !valid {
                let entry = ws.row_adj_inserts.remove(position);
                self.finalization
                    .accountant
                    .release(entry.estimated_bytes());
            }
        }
    }
}

impl Database {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn prepare_graph_shape_loss(
        &self,
        table: &str,
        before: &TableMeta,
        after: &TableMeta,
        copies: &mut contextdb_vector::VectorCopyAdmission,
        memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<(PreparedGraphPublication, usize)>> {
        let memory: contextdb_core::read_memory::ReadMemoryOwner = memory.into();

        if !has_graph_edge_table_shape(before)
            || has_graph_edge_table_shape(after)
            || self.graph_store.is_source_backed()
        {
            return Ok(None);
        }
        let bytes = self.resident_row_graph_bytes_for_table_drop(table);
        let survivors = self.graph_edges_after_table_drop(table, memory)?;
        Ok(Some((
            Self::prepare_received_row_graph_image(&survivors, copies)?,
            bytes,
        )))
    }
    pub(crate) fn publish_graph_shape_loss(
        &self,
        prepared: Option<(PreparedGraphPublication, usize)>,
    ) {
        if let Some((graph, bytes)) = prepared {
            self.graph_store.publish_prepared_received_schema(graph);
            self.finalization.accountant.release(bytes);
        }
    }
}
