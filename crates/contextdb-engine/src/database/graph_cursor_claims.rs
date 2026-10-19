//! Immutable transaction decisions retained by a captured graph read.
use super::*;
use contextdb_core::read_memory::{ReadCredit, ReadMemoryBudget};

#[derive(Clone)]
pub(crate) struct GraphCursorReadGuard(Arc<GraphDecisionContext>);
pub(super) struct GraphDecisionContext {
    pub(super) tx: TxId,
    owner_id: usize,
    pub(super) decisions: WriteSet,
    directory: Option<contextdb_tx::staged_source::CanonicalStatementDirectory>,
    legacy_rows: HashMap<String, Arc<contextdb_tx::StatementRows>>,
    pub(super) catalog: HashMap<String, TableMeta>,
    pub(super) root: Option<crate::persistence::WriterSnapshotHandle>,
    pub(super) reader: Arc<dyn ReadMemoryBudget>,
    _memory: ReadCredit,
    _rows_memory: ReadCredit,
}
thread_local! {
    static CAPTURED: std::cell::RefCell<Option<(usize, GraphCursorReadGuard)>> = const { std::cell::RefCell::new(None) };
}
pub(crate) struct GraphDecisionScope(Option<(usize, GraphCursorReadGuard)>);
impl Drop for GraphDecisionScope {
    fn drop(&mut self) {
        CAPTURED.with(|slot| *slot.borrow_mut() = self.0.take());
    }
}
impl GraphCursorReadGuard {
    pub(crate) fn enter(&self) -> GraphDecisionScope {
        GraphDecisionScope(
            CAPTURED.with(|slot| slot.borrow_mut().replace((self.0.owner_id, self.clone()))),
        )
    }
}
impl GraphDecisionContext {
    pub(super) fn rows(&self, table: &str) -> Option<Arc<contextdb_tx::StatementRows>> {
        self.legacy_rows.get(table).cloned()
    }
    pub(super) fn sealed_rows(
        &self,
        table: &str,
        memory: Arc<dyn ReadMemoryBudget>,
    ) -> Result<Option<contextdb_tx::staged_source::CanonicalStatementRows>> {
        self.directory
            .as_ref()
            .map(|directory| directory.table_rows(table, memory))
            .transpose()
    }
    fn capture_rows(
        ws: &WriteSet,
        memory: Arc<dyn ReadMemoryBudget>,
    ) -> Result<(
        HashMap<String, Arc<contextdb_tx::StatementRows>>,
        ReadCredit,
    )> {
        let bytes = ws
            .relational_inserts
            .iter()
            .map(|(table, _)| table.len().saturating_add(256))
            .sum::<usize>();
        let credit = ReadCredit::try_new(
            memory,
            bytes,
            "graph",
            "captured_row_handles",
            "Raise READ_MEMORY_LIMIT.",
        )?;
        let mut rows = HashMap::new();
        for (table, _) in &ws.relational_inserts {
            if !rows.contains_key(table) {
                if let Some(image) = ws.ordered_staged_rows.statement_rows(table) {
                    rows.insert(table.clone(), image);
                }
            }
        }
        Ok((rows, credit))
    }
    pub(super) fn check_table(&self, table: &str) -> Result<()> {
        if let Some(root) = &self.root {
            root.check_table_scope(table)?;
        }
        Ok(())
    }
    pub(super) fn check(&self) -> Result<()> {
        for edge in &self.decisions.row_adj_inserts {
            self.check_table(&edge.owner.table)?;
        }
        Ok(())
    }
}
impl Database {
    pub(super) fn captured_graph_decisions(&self) -> Option<Arc<GraphDecisionContext>> {
        CAPTURED.with(|slot| {
            slot.borrow()
                .as_ref()
                .filter(|(id, _)| *id == self.tx_mgr.identity())
                .map(|(_, guard)| guard.0.clone())
        })
    }
    fn capture_graph_decision_metadata(
        &self,
        read: contextdb_tx::manager::EngineRead<'_>,
        directory: Option<contextdb_tx::staged_source::CanonicalStatementDirectory>,
        legacy_rows: HashMap<String, Arc<contextdb_tx::StatementRows>>,
        rows_memory: ReadCredit,
        root: Option<crate::persistence::WriterSnapshotHandle>,
        tx: TxId,
        memory: Arc<dyn ReadMemoryBudget>,
    ) -> Result<GraphCursorReadGuard> {
        let catalog = self.relational_store.table_meta.read();
        // Only decisions and control metadata are copied. Row bodies and
        // vector/property payloads stay in their original admitted cells.
        let catalog_bytes = if let Some(root) = &root {
            root.captured_table_metadata()
                .map(|(name, meta)| {
                    name.len()
                        .saturating_add(meta.metadata_bytes())
                        .saturating_add(128)
                })
                .sum::<usize>()
        } else {
            catalog
                .iter()
                .map(|(name, meta)| {
                    name.len()
                        .saturating_add(meta.metadata_bytes())
                        .saturating_add(128)
                })
                .sum::<usize>()
        };
        let bytes = catalog_bytes
            .saturating_add(
                read.adj_inserts
                    .iter()
                    .map(|e| e.edge_type.len() + std::mem::size_of::<AdjEntry>())
                    .sum::<usize>(),
            )
            .saturating_add(
                read.row_adj_inserts
                    .iter()
                    .map(|e| e.estimated_bytes())
                    .sum::<usize>(),
            )
            .saturating_add(
                read.relational_deletes
                    .iter()
                    .map(|(name, _, _)| {
                        name.len()
                            .saturating_add(std::mem::size_of::<(String, RowId, TxId)>())
                    })
                    .sum::<usize>(),
            )
            .saturating_add(
                read.relational_delete_predicates
                    .iter()
                    .map(|p| {
                        p.table.len().saturating_add(128).saturating_add(
                            p.predicates
                                .iter()
                                .map(|(name, value)| {
                                    name.len()
                                        .saturating_add(value.estimated_bytes())
                                        .saturating_add(96)
                                })
                                .sum::<usize>(),
                        )
                    })
                    .sum::<usize>(),
            )
            .saturating_add(
                read.adj_deletes
                    .iter()
                    .map(|(_, kind, _, _)| {
                        kind.len().saturating_add(std::mem::size_of::<(
                            NodeId,
                            EdgeType,
                            NodeId,
                            TxId,
                        )>())
                    })
                    .sum::<usize>(),
            )
            .saturating_add(
                read.row_adj_deletes
                    .iter()
                    .map(|(edge, _)| {
                        edge.estimated_bytes()
                            .saturating_add(std::mem::size_of::<TxId>())
                    })
                    .sum::<usize>(),
            )
            .saturating_add(
                legacy_rows
                    .keys()
                    .map(|name| name.len() + 128)
                    .sum::<usize>(),
            )
            .saturating_add(std::mem::size_of::<GraphDecisionContext>() + 128);
        let credit = ReadCredit::try_new(
            memory.clone(),
            bytes,
            "graph",
            "captured_decisions",
            "Reduce staged graph metadata or raise READ_MEMORY_LIMIT.",
        )?;
        let mut decisions = WriteSet::new();
        decisions.relational_deletes = read.relational_deletes.to_vec();
        decisions.relational_delete_predicates = read.relational_delete_predicates.to_vec();
        decisions.adj_deletes = read.adj_deletes.to_vec();
        decisions.row_adj_deletes = read.row_adj_deletes.to_vec();
        decisions.row_adj_inserts = read.row_adj_inserts.to_vec();
        decisions.adj_inserts = read
            .adj_inserts
            .iter()
            .map(|edge| AdjEntry {
                source: edge.source,
                target: edge.target,
                edge_type: edge.edge_type.clone(),
                properties: HashMap::new(),
                created_tx: edge.created_tx,
                deleted_tx: edge.deleted_tx,
                lsn: edge.lsn,
            })
            .collect();
        let catalog = if let Some(root) = &root {
            root.captured_table_metadata()
                .map(|(name, meta)| (name.to_owned(), meta.clone()))
                .collect()
        } else {
            catalog.clone()
        };
        Ok(GraphCursorReadGuard(Arc::new(GraphDecisionContext {
            tx,
            owner_id: self.tx_mgr.identity(),
            decisions,
            directory,
            legacy_rows,
            catalog,
            root: root.clone(),
            reader: memory.clone(),
            _memory: credit,
            _rows_memory: rows_memory,
        })))
    }
    pub(super) fn graph_cursor_prepared_read_guard(
        &self,
        ws: &WriteSet,
        tx: TxId,
        memory: Arc<dyn ReadMemoryBudget>,
    ) -> Result<GraphCursorReadGuard> {
        let root = self.writer_read_root()?;
        let (rows, credit) = GraphDecisionContext::capture_rows(ws, memory.clone())?;
        self.capture_graph_decision_metadata(
            DatabaseTransactionHost::legacy_read(ws),
            None,
            rows,
            credit,
            root,
            tx,
            memory,
        )
    }
    pub(super) fn graph_cursor_held_read_guard(
        &self,
        held: &contextdb_tx::manager::EngineGuard<'_>,
        tx: TxId,
        memory: Arc<dyn ReadMemoryBudget>,
    ) -> Result<GraphCursorReadGuard> {
        let root = self.writer_read_root()?;
        let directory = held.capture_sealed_directory(memory.clone())?;
        let (rows, credit) =
            held.with_legacy(|ws| GraphDecisionContext::capture_rows(ws, memory.clone()))?;
        self.capture_graph_decision_metadata(
            held.read(),
            Some(directory),
            rows,
            credit,
            root,
            tx,
            memory,
        )
    }
    pub(crate) fn graph_cursor_read_guard(
        &self,
        tx: Option<TxId>,
        memory: Arc<dyn ReadMemoryBudget>,
    ) -> Result<Option<GraphCursorReadGuard>> {
        let Some(tx) = tx else {
            return Ok(None);
        };
        if let Some(capture) = self.captured_graph_decisions().filter(|c| c.tx == tx) {
            return Ok(Some(GraphCursorReadGuard(capture)));
        }
        let root = self.writer_read_root()?;
        let captured = match &self.tx_mgr {
            DatabaseTransactionHost::Owned(handle) => handle.with_host(|host| {
                host.transactions.with_transactions(|transactions| {
                    transactions.with_engine(tx, |guard| {
                        let directory = guard.capture_sealed_directory(memory.clone())?;
                        let (rows, credit) = guard.with_legacy(|ws| {
                            GraphDecisionContext::capture_rows(ws, memory.clone())
                        })?;
                        self.capture_graph_decision_metadata(
                            guard.read(),
                            Some(directory),
                            rows,
                            credit,
                            root.clone(),
                            tx,
                            memory.clone(),
                        )
                    })
                })
            })?,
            DatabaseTransactionHost::Legacy(manager) => manager.with_write_set(tx, |ws| {
                let (rows, credit) = GraphDecisionContext::capture_rows(ws, memory.clone())?;
                self.capture_graph_decision_metadata(
                    DatabaseTransactionHost::legacy_read(ws),
                    None,
                    rows,
                    credit,
                    root.clone(),
                    tx,
                    memory.clone(),
                )
            })??,
        };
        Ok(Some(captured))
    }
}
#[derive(Clone, Copy)]
pub(super) struct StagedGraphPosition {
    row_owned: bool,
    index: usize,
}
pub(crate) struct EngineGraphBfsCursor {
    pub(super) inner: contextdb_graph::mem::BoundedBfsCursor,
    pub(super) forward: Vec<StagedGraphPosition>,
    pub(super) reverse: Vec<StagedGraphPosition>,
    pub(super) lease: Option<GraphCursorReadGuard>,
    _order_memory: ReadCredit,
}
impl std::ops::Deref for EngineGraphBfsCursor {
    type Target = contextdb_graph::mem::BoundedBfsCursor;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
impl std::ops::DerefMut for EngineGraphBfsCursor {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}
impl EngineGraphBfsCursor {
    pub(crate) fn into_retained_bytes(self) -> usize {
        self.inner.retained_bytes()
    }
}
fn staged_binding<'a>(
    read: &contextdb_tx::manager::EngineRead<'a>,
    position: StagedGraphPosition,
) -> Option<(&'a AdjEntry, Option<&'a contextdb_tx::RowEdgeOwner>)> {
    if position.row_owned {
        read.row_adj_inserts
            .get(position.index)
            .map(|owned| (&owned.entry, Some(&owned.owner)))
    } else {
        read.adj_inserts
            .get(position.index)
            .map(|edge| (edge, None))
    }
}
impl Database {
    pub(super) fn wrap_graph_bfs_cursor(
        &self,
        _tx: Option<TxId>,
        inner: contextdb_graph::mem::BoundedBfsCursor,
        lease: Option<GraphCursorReadGuard>,
        memory: Arc<dyn ReadMemoryBudget>,
    ) -> Result<EngineGraphBfsCursor> {
        let mut credit = ReadCredit::new(memory);
        let mut forward = Vec::new();
        let mut reverse = Vec::new();
        if let Some(capture) = lease.as_ref() {
            let read = DatabaseTransactionHost::legacy_read(&capture.0.decisions);
            (|| -> Result<()> {
                let count = read
                    .adj_inserts
                    .len()
                    .checked_add(read.row_adj_inserts.len())
                    .ok_or_else(|| Error::Other("staged graph count overflow".into()))?;
                let bytes = count
                    .checked_mul(2 * std::mem::size_of::<StagedGraphPosition>())
                    .ok_or_else(|| Error::Other("staged graph order size overflow".into()))?;
                credit.try_grow_for(
                    bytes,
                    "graph",
                    "staged_graph_order",
                    "Reduce staged edge rows or raise MEMORY_LIMIT.",
                )?;
                forward
                    .try_reserve_exact(count)
                    .map_err(|_| Error::Other("staged graph order allocation failed".into()))?;
                reverse
                    .try_reserve_exact(count)
                    .map_err(|_| Error::Other("staged graph order allocation failed".into()))?;
                forward.extend(
                    (0..read.adj_inserts.len()).map(|index| StagedGraphPosition {
                        row_owned: false,
                        index,
                    }),
                );
                forward.extend(
                    (0..read.row_adj_inserts.len()).map(|index| StagedGraphPosition {
                        row_owned: true,
                        index,
                    }),
                );
                reverse.extend_from_slice(&forward);
                for (order, incoming) in [(&mut forward, false), (&mut reverse, true)] {
                    order.sort_unstable_by(|left, right| {
                        let (left, left_owner) =
                            staged_binding(&read, *left).expect("captured staged position");
                        let (right, right_owner) =
                            staged_binding(&read, *right).expect("captured staged position");
                        let a = if incoming {
                            (left.target, left.source)
                        } else {
                            (left.source, left.target)
                        };
                        let b = if incoming {
                            (right.target, right.source)
                        } else {
                            (right.source, right.target)
                        };
                        a.cmp(&b)
                            .then_with(|| left.edge_type.cmp(&right.edge_type))
                            .then_with(|| left_owner.cmp(&right_owner))
                            .then_with(|| left.created_tx.cmp(&right.created_tx))
                            .then_with(|| left.lsn.cmp(&right.lsn))
                    });
                }
                Ok(())
            })()?;
        }
        Ok(EngineGraphBfsCursor {
            inner,
            forward,
            reverse,
            lease,
            _order_memory: credit,
        })
    }
    pub(super) fn copy_ordered_staged_traversal_edge<E: From<Error>>(
        &self,
        capture: &GraphCursorReadGuard,
        position: StagedGraphPosition,
        before_retain: &mut impl FnMut(usize) -> std::result::Result<(), E>,
    ) -> std::result::Result<Option<(contextdb_graph::mem::StagedTraversalEdge, usize)>, E> {
        let read = DatabaseTransactionHost::legacy_read(&capture.0.decisions);
        (|| {
            let Some((edge, owner)) = staged_binding(&read, position) else {
                return Err(E::from(Error::SyncError(
                    "a graph cursor's staged binding was removed while open".into())));
            };
            let bytes = Self::staged_edge_type_bytes(edge.edge_type.len())
                .saturating_add(owner.map_or(0, |owner| owner.table.len()));
            before_retain(bytes)?;
            Ok(Some((
                contextdb_graph::mem::StagedTraversalEdge {
                    row_owner: owner.cloned(),
                    source: edge.source,
                    target: edge.target,
                    edge_type: edge.edge_type.clone(),
                },
                bytes,
            )))
        })()
    }
}
