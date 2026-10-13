use crate::composite_store::CompositeStore;
use crate::database::event_bus::{EventBusState, MAX_SINK_QUEUE_DEPTH};
use crate::database::trigger::TriggerState;
use crate::database::{LocalSchemaStageRegistry, ReceivedSchemaStageRegistry};
use crate::persistence::{
    FlushDataOptions, FlushDataSnapshots, LocalErasurePersistence, RedbPersistence,
    SchemaDdlPersistence,
};
use contextdb_core::{Error, Lsn, Result, RowId, SnapshotId};
use contextdb_tx::{WriteSet, WriteSetApplicator};
use contextdb_vector::VectorPartitionRef;
use std::collections::HashSet;
use std::sync::Arc;

#[cfg(any(test, feature = "test-seams"))]
thread_local! {
    static FAIL_NEXT_LOCAL_APPLY: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(any(test, feature = "test-seams"))]
pub(crate) fn fail_next_local_apply_for_test() {
    FAIL_NEXT_LOCAL_APPLY.with(|armed| armed.set(true));
}

pub struct PersistentCompositeStore {
    inner: CompositeStore,
    persistence: Arc<RedbPersistence>,
    event_bus: Option<Arc<EventBusState>>,
    trigger: Option<Arc<TriggerState>>,
    received_schema_stages: ReceivedSchemaStageRegistry,
    local_schema_stages: LocalSchemaStageRegistry,
    prepared_publication:
        parking_lot::Mutex<Option<Arc<crate::persistence::writer_source::WriterSnapshot>>>,
    _precommit_source_memory: PersistentSourceMemory,
    publication_interrupted: std::sync::atomic::AtomicBool,
}

// Keeps the pre2 Arc-plus-byte footprint while retaining one real active
// writer use independently of the inner store and persistence payloads.
struct PersistentSourceMemory {
    accountant: crate::memory_accounting::StoreAccountantHandle,
    bytes: usize,
}
impl Drop for PersistentSourceMemory {
    fn drop(&mut self) {
        self.accountant.release(self.bytes);
    }
}

impl PersistentCompositeStore {
    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn source_control_bytes_for_test(&self) -> usize {
        self._precommit_source_memory.bytes
    }

    pub(crate) fn new(
        inner: CompositeStore,
        persistence: Arc<RedbPersistence>,
        initial_root: Arc<crate::persistence::WriterSnapshot>,
        event_bus: Option<Arc<EventBusState>>,
        trigger: Option<Arc<TriggerState>>,
        received_schema_stages: ReceivedSchemaStageRegistry,
        local_schema_stages: LocalSchemaStageRegistry,
    ) -> Result<Self> {
        // Like the initial published root, startup charges this fixed control
        // without refusing open under a smaller persisted limit. Subsequent
        // commit admission still sees every retained byte.
        let source_bytes = std::mem::size_of::<
            parking_lot::Mutex<std::sync::Weak<crate::persistence::WriterSnapshot>>,
        >();
        *inner.precommit_vector_root.lock() = Arc::downgrade(&initial_root);
        inner.accountant.charge_retained(source_bytes);
        let source_memory = PersistentSourceMemory {
            accountant: inner.accountant.clone(),
            bytes: source_bytes,
        };
        Ok(Self {
            inner,
            persistence,
            event_bus,
            trigger,
            received_schema_stages,
            local_schema_stages,
            prepared_publication: parking_lot::Mutex::new(None),
            _precommit_source_memory: source_memory,
            publication_interrupted: std::sync::atomic::AtomicBool::new(false),
        })
    }
}

impl PersistentCompositeStore {
    /// Apply one commit using only reclamation evidence captured by Database
    /// while its snapshot-removal guard is held. Explicit refs remain for the
    /// low-level prepared-store contract; ordinary commits supply the exact
    /// registered-snapshot sample staged under this commit's LSN.
    fn apply_with_vector_reclamation(
        &self,
        ws: &WriteSet,
        reclaimable_partitions: HashSet<VectorPartitionRef>,
        registered_snapshots: Option<&[SnapshotId]>,
        read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<()> {
        if let Some(lsn) = ws.commit_lsn {
            let mut local_stages = self.local_schema_stages.lock();
            if let Some(stage) = local_stages.get(&lsn) {
                let result = self
                    .persistence
                    .flush_local_schema_stage(ws, &stage.durable_projection);
                if result.is_err() {
                    local_stages.remove(&lsn);
                }
                return result;
            }
            let mut stages = self.received_schema_stages.lock();
            if let Some(stage) = stages.get(&lsn) {
                let result = self.persistence.flush_data_with_logs_and_sink_events(
                    ws,
                    &stage.change_log_entries,
                    FlushDataOptions {
                        local_erasure: None,
                        sink_events: &stage.sink_events,
                        trigger_audits: &stage.trigger_audits,
                        schema_ddl: SchemaDdlPersistence {
                            event_bus: None,
                            trigger: None,
                        },
                        max_sink_queue_depth: MAX_SINK_QUEUE_DEPTH,
                        snapshots: FlushDataSnapshots::default(),
                        received_schema: Some(&stage.durable_projection),
                        partition_mutations: None,
                    },
                );
                if result.is_err() {
                    stages.remove(&lsn);
                }
                return result;
            }
        }
        let sink_events = ws.commit_lsn.and_then(|lsn| {
            self.event_bus
                .as_ref()
                .and_then(|event_bus| event_bus.staged_sink_events_for_persistence(lsn))
        });
        let event_bus_ddl = ws.commit_lsn.and_then(|lsn| {
            self.event_bus
                .as_ref()
                .and_then(|event_bus| event_bus.staged_event_bus_persistence_commit(lsn))
        });
        let trigger_ddl = ws.commit_lsn.and_then(|lsn| {
            self.trigger
                .as_ref()
                .and_then(|trigger| trigger.staged_trigger_persistence_commit(lsn))
        });
        let trigger_audits = ws
            .commit_lsn
            .and_then(|lsn| {
                self.trigger
                    .as_ref()
                    .map(|trigger| trigger.take_staged_persistence_audits(lsn))
            })
            .unwrap_or_default();
        #[cfg(feature = "test-seams")]
        let mut stage_started = std::time::Instant::now();
        let table_meta = self.inner.relational.table_meta.read();
        #[cfg(feature = "test-seams")]
        crate::persistence::observe_append_stage(
            crate::persistence::AppendStage::TableMetaClone,
            &mut stage_started,
        );
        let mut accepted_row_memory = crate::memory_accounting::OwnedMemoryReservation::new(
            self.inner.accountant.as_handle(),
        );
        let deleted_rows = self.deleted_rows_snapshot_for_write_set(
            ws,
            &mut accepted_row_memory,
            read_memory.clone(),
        )?;
        let log_entries = self.inner.build_change_log_entries_with_snapshots(
            ws,
            Some(&table_meta),
            deleted_rows.as_ref(),
        );
        let mut vector_copy_owner = crate::vector_read_memory::admission(read_memory.clone());
        let partition_mutations = match registered_snapshots {
            Some(registered_snapshots) => self
                .inner
                .prepare_vector_partition_mutations_with_registered_snapshots(
                    ws,
                    &table_meta,
                    deleted_rows.as_ref(),
                    registered_snapshots,
                    &mut vector_copy_owner,
                    read_memory.clone(),
                )?,
            None => self
                .inner
                .prepare_vector_partition_mutations_with_reclaimable_partitions(
                    ws,
                    &table_meta,
                    deleted_rows.as_ref(),
                    reclaimable_partitions,
                    &mut vector_copy_owner,
                    read_memory.clone(),
                )?,
        };
        let erasure_stages = self.inner.local_erasure_stages.lock();
        let erasure = ws.commit_lsn.and_then(|lsn| erasure_stages.get(&lsn));
        #[cfg(feature = "test-seams")]
        crate::persistence::observe_append_stage(
            crate::persistence::AppendStage::WriteSetPreparation,
            &mut stage_started,
        );
        self.persistence.flush_data_with_logs_and_sink_events(
            ws,
            &log_entries,
            FlushDataOptions {
                local_erasure: erasure.map(|stage| LocalErasurePersistence {
                    projection: &stage.durable,
                    accountant: stage._vector_workspace.accountant().clone(),
                    operation: "discard",
                }),
                sink_events: sink_events.as_deref().map(Vec::as_slice).unwrap_or(&[]),
                trigger_audits: &trigger_audits,
                schema_ddl: SchemaDdlPersistence {
                    event_bus: event_bus_ddl.as_ref(),
                    trigger: trigger_ddl.as_ref(),
                },
                max_sink_queue_depth: MAX_SINK_QUEUE_DEPTH,
                snapshots: FlushDataSnapshots {
                    table_meta: Some(&table_meta),
                    deleted_rows,
                },
                received_schema: None,
                partition_mutations: Some(&partition_mutations),
            },
        )?;
        drop(erasure_stages);
        drop(table_meta);
        #[cfg(feature = "in-process-test-seams")]
        crate::writer_commit_test_seams::reached(
            crate::writer_commit_test_seams::CommitBoundary::BeforeMemoryPublication,
        );
        self.inner
            .apply_exact_with_log_entries(ws, log_entries, partition_mutations)
    }

    /// Retain admitted current bodies only for row/vector identities this commit
    /// touches. The same prepared publication owner supplies the precommit
    /// physical source; no resident row directory or history walk is needed.
    fn deleted_rows_snapshot_for_write_set(
        &self,
        ws: &WriteSet,
        memory: &mut crate::memory_accounting::OwnedMemoryReservation,
        read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<
        Option<
            std::collections::HashMap<
                String,
                std::collections::HashMap<RowId, contextdb_core::VersionedRow>,
            >,
        >,
    > {
        use crate::sync_types::held;
        use contextdb_relational::source::{RelationalReadSource, visible_row};
        if ws.relational_deletes.is_empty()
            && ws.vector_inserts.is_empty()
            && ws.vector_deletes.is_empty()
            && ws.vector_moves.is_empty()
        {
            return Ok(None);
        }
        let root = self
            .inner
            .precommit_vector_root
            .lock()
            .upgrade()
            .ok_or_else(|| {
                Error::Other("writer publication no longer owns its commit source".into())
            })?;
        let source = crate::persistence::writer_adapters::WriterRelationalSource::new_for_read(
            root,
            read_memory,
        )?;
        let mut rows = std::collections::HashMap::<
            String,
            std::collections::HashMap<RowId, contextdb_core::VersionedRow>,
        >::new();
        let mut retain = |table: &str,
                          row_id: RowId,
                          subsystem: &'static str,
                          operation: &'static str|
         -> Result<()> {
            if rows
                .get(table)
                .is_some_and(|by_id| by_id.contains_key(&row_id))
            {
                return Ok(());
            }
            let Some(window) = visible_row(&source, table, row_id, source.snapshot())? else {
                return Ok(());
            };
            if !rows.contains_key(table) {
                let before = held::map::<
                    String,
                    std::collections::HashMap<RowId, contextdb_core::VersionedRow>,
                >(rows.len());
                let after = held::map::<
                    String,
                    std::collections::HashMap<RowId, contextdb_core::VersionedRow>,
                >(rows.len().saturating_add(1));
                memory.try_grow_for(
                    after
                        .saturating_sub(before)
                        .saturating_add(held::text(table)),
                    subsystem,
                    operation,
                    "Reduce changed identities or raise MEMORY_LIMIT.",
                )?;
                rows.try_reserve(1)
                    .map_err(|_| Error::Other("accepted row table allocation failed".into()))?;
                rows.insert(table.to_owned(), std::collections::HashMap::new());
            }
            let by_id = rows.get_mut(table).expect("admitted table control");
            let before = held::map::<RowId, contextdb_core::VersionedRow>(by_id.len());
            let after =
                held::map::<RowId, contextdb_core::VersionedRow>(by_id.len().saturating_add(1));
            memory.try_grow_for(
                after
                    .saturating_sub(before)
                    .saturating_add(held::values(&window.value().values)),
                subsystem,
                operation,
                "Reduce changed row bodies or raise MEMORY_LIMIT.",
            )?;
            by_id
                .try_reserve(1)
                .map_err(|_| Error::Other("accepted row identity allocation failed".into()))?;
            by_id.insert(row_id, window.value().clone());
            Ok(())
        };
        for (table, row, _) in &ws.relational_deletes {
            retain(table, *row, "relational", "delete_pre_image")?;
        }
        for entry in &ws.vector_inserts {
            retain(
                &entry.index.table,
                entry.row_id,
                "vector",
                "accepted_row_source",
            )?;
        }
        for (index, row, _) in &ws.vector_deletes {
            retain(&index.table, *row, "vector", "accepted_row_source")?;
        }
        for (index, from, to, _) in &ws.vector_moves {
            retain(&index.table, *from, "vector", "accepted_row_source")?;
            retain(&index.table, *to, "vector", "accepted_row_source")?;
        }
        Ok((!rows.is_empty()).then_some(rows))
    }
}

impl PersistentCompositeStore {
    fn prepare_publication(
        &self,
        control: crate::memory_accounting::OwnedMemoryReservation,
        snapshot: SnapshotId,
        lsn: contextdb_core::Lsn,
    ) {
        let root = self
            .persistence
            .prepare_writer_snapshot(control, snapshot, lsn)
            .unwrap_or_else(|error| {
                self.publication_interrupted
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                panic!("durable publication root preparation failed: {error}");
            });
        *self.inner.precommit_vector_root.lock() = Arc::downgrade(&root);
        *self.prepared_publication.lock() = Some(root);
    }
}
impl WriteSetApplicator for PersistentCompositeStore {
    fn reserve_metadata_publication(&self) -> Result<Option<Box<dyn std::any::Any + Send>>> {
        Ok(Some(Box::new(self.persistence.reserve_writer_snapshot()?)))
    }
    fn complete_metadata_publication(
        &self,
        reservation: Option<Box<dyn std::any::Any + Send>>,
        snapshot: SnapshotId,
        lsn: contextdb_core::Lsn,
    ) {
        let control = *reservation
            .expect("reserved metadata publication")
            .downcast::<crate::memory_accounting::OwnedMemoryReservation>()
            .unwrap_or_else(|_| panic!("invalid publication reservation"));
        self.prepare_publication(control, snapshot, lsn);
    }
    fn publication_interrupted(&self) -> bool {
        self.publication_interrupted
            .load(std::sync::atomic::Ordering::SeqCst)
    }
    fn publication_handle(&self) -> Option<Arc<dyn std::any::Any + Send + Sync>> {
        self.prepared_publication
            .lock()
            .take()
            .map(|root| root as Arc<dyn std::any::Any + Send + Sync>)
    }

    fn apply(
        &self,
        ws: &WriteSet,
        read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<()> {
        #[cfg(any(test, feature = "test-seams"))]
        if FAIL_NEXT_LOCAL_APPLY.with(|armed| armed.replace(false)) {
            return Err(Error::Other(
                "injected local durable commit failure".to_string(),
            ));
        }
        let registered_snapshots = self
            .inner
            .take_vector_partition_snapshot_stage(ws.commit_lsn);
        let snapshot = SnapshotId::from_tx(ws.finalized_tx.ok_or_else(|| {
            contextdb_core::Error::Other(
                "durable commit has no finalized transaction identity".into(),
            )
        })?);
        let lsn = ws.commit_lsn.ok_or_else(|| {
            contextdb_core::Error::Other("durable commit has no finalized LSN".into())
        })?;
        let control = self.persistence.reserve_writer_snapshot()?;
        self.apply_with_vector_reclamation(
            ws,
            HashSet::new(),
            registered_snapshots.as_deref(),
            read_memory,
        )?;
        self.prepare_publication(control, snapshot, lsn);
        Ok(())
    }

    fn new_row_id(&self) -> RowId {
        self.inner.new_row_id()
    }
}

/// Dormant companion store. Its original store payload and the one paired
/// event companion are the entire layout; actual factories remain Legacy.
pub(crate) struct OwnedPersistentCompositeStore {
    pub(crate) legacy: PersistentCompositeStore,
    pub(crate) events: crate::database::event_bus::OwnedEventBusHandle,
}

pub(crate) enum OwnedFlushInput<'e> {
    Local(&'e crate::persistence::LocalSchemaPersistenceProjection),
    Received {
        change_log: &'e [crate::composite_store::ChangeLogEntry],
        projection: &'e crate::persistence::ReceivedSchemaPersistenceProjection,
        trigger_audits: &'e [(u64, crate::database::TriggerAuditEntry)],
    },
    Ordinary,
}

// Only original transaction/generation supplied by native preparation may
// qualify the generation-less WriteSetApplicator call. This scalar stack scope
// is per store and nested; no stage map lookup manufactures its identity.
#[derive(Clone, Copy)]
struct OwnedTerminalApplyBinding {
    store: usize,
    terminal: Option<(Lsn, contextdb_tx::manager::EngineTerminalIdentity)>,
}
thread_local! {
    static OWNED_TERMINAL_APPLY: std::cell::Cell<Option<OwnedTerminalApplyBinding>> = const { std::cell::Cell::new(None) };
}
struct OwnedTerminalApplyScope {
    previous: Option<OwnedTerminalApplyBinding>,
    control: crate::memory_accounting::OwnedMemoryReservation,
}
impl Drop for OwnedTerminalApplyScope {
    fn drop(&mut self) {
        OWNED_TERMINAL_APPLY.with(|slot| slot.set(self.previous));
    }
}
impl OwnedPersistentCompositeStore {
    /// Wrap the synchronous native commit. Its reserved prepare callback binds
    /// the authentic identity before the original trait apply is invoked.
    pub(crate) fn with_owned_terminal_apply_scope<R>(&self, f: impl FnOnce() -> R) -> Result<R> {
        self.events.with_owned_persistence_original(|original| {
            let bytes = std::mem::size_of::<OwnedTerminalApplyScope>()
                .checked_add(std::mem::size_of::<OwnedTerminalApplyBinding>())
                .ok_or_else(|| Error::Other("native event apply control size overflow".into()))?;
            let control = crate::memory_accounting::reserve_preparation_from_original(
                original,
                bytes,
                "events",
                "terminal_apply_control",
                "Reduce prepared event controls or raise MEMORY_LIMIT.",
            )?;
            let binding = OwnedTerminalApplyBinding {
                store: self as *const Self as usize,
                terminal: None,
            };
            let previous = OWNED_TERMINAL_APPLY.with(|slot| slot.replace(Some(binding)));
            let scope = OwnedTerminalApplyScope { previous, control };
            let result = f();
            drop(scope);
            Ok(result)
        })
    }
    pub(crate) fn bind_owned_terminal_apply_identity(
        &self,
        lsn: Lsn,
        identity: contextdb_tx::manager::EngineTerminalIdentity,
    ) -> Result<()> {
        OWNED_TERMINAL_APPLY.with(|slot| {
            let mut binding = slot.get().ok_or_else(|| {
                Error::Other("native event apply requires its supplied terminal scope".into())
            })?;
            if binding.store != self as *const Self as usize
                || binding.terminal.is_some_and(|old| old != (lsn, identity))
            {
                return Err(Error::Other(
                    "native event apply terminal binding mismatch".into(),
                ));
            }
            binding.terminal = Some((lsn, identity));
            slot.set(Some(binding));
            Ok(())
        })
    }
    pub(crate) fn bind_owned_registered_event_apply(
        &self,
        registration: &crate::database::event_bus::OwnedEventStageRegistration,
    ) -> Result<()> {
        self.events
            .with_owned_registered_stage_identity(registration, |lsn, identity| {
                self.bind_owned_terminal_apply_identity(lsn, identity)
            })
    }
    fn owned_terminal_apply_identity(
        &self,
        ws: &WriteSet,
    ) -> Result<contextdb_tx::manager::EngineTerminalIdentity> {
        OWNED_TERMINAL_APPLY.with(|slot| {
            let binding = slot
                .get()
                .filter(|binding| binding.store == self as *const Self as usize)
                .ok_or_else(|| {
                    Error::Other("native event apply requires its supplied terminal scope".into())
                })?;
            let (lsn, identity) = binding.terminal.ok_or_else(|| {
                Error::Other(
                    "native event apply requires original transaction and generation".into(),
                )
            })?;
            if ws.commit_lsn != Some(lsn) {
                return Err(Error::Other(
                    "native event apply terminal LSN mismatch".into(),
                ));
            }
            Ok(identity)
        })
    }
    /// Preserve the actual local/received early returns while replacing both
    /// event getters with one synchronous combined-stage loan. The supplied
    /// consumer may flush only: memory apply, take, Drop and callbacks happen
    /// after this method returns and releases its stage guards.
    pub(crate) fn with_owned_flush_inputs<R>(
        &self,
        ws: &WriteSet,
        identity: contextdb_tx::manager::EngineTerminalIdentity,
        flush: impl for<'e> FnOnce(
            OwnedFlushInput<'e>,
            Option<crate::database::event_bus::OwnedStageBorrow<'e>>,
        ) -> Result<R>,
    ) -> Result<R> {
        if let Some(lsn) = ws.commit_lsn {
            let mut local_stages = self.legacy.local_schema_stages.lock();
            if let Some(stage) = local_stages.get(&lsn) {
                let result = crate::composite_store::with_owned_commit_events_for_apply(
                    &self.events,
                    Some(lsn),
                    identity,
                    |events| flush(OwnedFlushInput::Local(&stage.durable_projection), events),
                );
                let retired = if result.is_err() {
                    local_stages.remove(&lsn)
                } else {
                    None
                };
                drop(local_stages);
                drop(retired);
                return result;
            }
            drop(local_stages);
            let mut stages = self.legacy.received_schema_stages.lock();
            if let Some(stage) = stages.get(&lsn) {
                let result = crate::composite_store::with_owned_commit_events_for_apply(
                    &self.events,
                    Some(lsn),
                    identity,
                    |events| {
                        flush(
                            OwnedFlushInput::Received {
                                change_log: &stage.change_log_entries,
                                projection: &stage.durable_projection,
                                trigger_audits: &stage.trigger_audits,
                            },
                            events,
                        )
                    },
                );
                let retired = if result.is_err() {
                    stages.remove(&lsn)
                } else {
                    None
                };
                drop(stages);
                drop(retired);
                return result;
            }
            drop(stages);
        }
        crate::composite_store::with_owned_commit_events_for_apply(
            &self.events,
            ws.commit_lsn,
            identity,
            |events| flush(OwnedFlushInput::Ordinary, events),
        )
    }
}

impl OwnedPersistentCompositeStore {
    fn deleted_rows_snapshot_for_owned_write_set(
        &self,
        ws: &WriteSet,
        memory: &mut crate::memory_accounting::OwnedMemoryReservation,
        read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<
        Option<
            std::collections::HashMap<
                String,
                std::collections::HashMap<RowId, contextdb_core::VersionedRow>,
            >,
        >,
    > {
        use contextdb_relational::source::{RelationalReadSource, visible_row};
        if ws.relational_deletes.is_empty()
            && ws.vector_inserts.is_empty()
            && ws.vector_deletes.is_empty()
            && ws.vector_moves.is_empty()
        {
            return Ok(None);
        }
        let root = self
            .legacy
            .inner
            .precommit_vector_root
            .lock()
            .upgrade()
            .ok_or_else(|| {
                Error::Other("writer publication no longer owns its commit source".into())
            })?;
        let source = crate::persistence::writer_adapters::WriterRelationalSource::new_for_read(
            root,
            read_memory,
        )?;
        let tables = ws
            .relational_deletes
            .iter()
            .map(|(table, _, _)| table.as_str())
            .chain(
                ws.vector_inserts
                    .iter()
                    .map(|entry| entry.index.table.as_str()),
            )
            .chain(
                ws.vector_deletes
                    .iter()
                    .map(|(index, _, _)| index.table.as_str()),
            )
            .chain(
                ws.vector_moves
                    .iter()
                    .map(|(index, _, _, _)| index.table.as_str()),
            );
        let table_count = tables
            .clone()
            .enumerate()
            .filter(|(at, table)| !tables.clone().take(*at).any(|prior| prior == *table))
            .count();
        let (mut rows, mut directory) = crate::memory_accounting::new_owned_native_map::<
            String,
            std::collections::HashMap<RowId, contextdb_core::VersionedRow>,
        >(memory, table_count)?;
        crate::memory_accounting::merge_preparation_original(memory, &mut directory)?;
        let mut retain = |table: &str,
                          row_id: RowId,
                          subsystem: &'static str,
                          operation: &'static str|
         -> Result<()> {
            if rows
                .get(table)
                .is_some_and(|by_id| by_id.contains_key(&row_id))
            {
                return Ok(());
            }
            let Some(window) = visible_row(&source, table, row_id, source.snapshot())? else {
                return Ok(());
            };
            if !rows.contains_key(table) {
                let count = ws
                    .relational_deletes
                    .iter()
                    .filter(|(name, _, _)| name == table)
                    .count()
                    .checked_add(
                        ws.vector_inserts
                            .iter()
                            .filter(|entry| entry.index.table == table)
                            .count(),
                    )
                    .and_then(|n| {
                        n.checked_add(
                            ws.vector_deletes
                                .iter()
                                .filter(|(index, _, _)| index.table == table)
                                .count(),
                        )
                    })
                    .and_then(|n| {
                        n.checked_add(
                            ws.vector_moves
                                .iter()
                                .filter(|(index, _, _, _)| index.table == table)
                                .count()
                                .checked_mul(2)?,
                        )
                    })
                    .ok_or_else(|| Error::Other("selected row identity count overflow".into()))?;
                let (by_id, mut control) = crate::memory_accounting::new_owned_native_map::<
                    RowId,
                    contextdb_core::VersionedRow,
                >(memory, count)?;
                crate::memory_accounting::merge_preparation_original(memory, &mut control)?;
                let (name, mut key_control) =
                    crate::memory_accounting::copy_owned_native_string(memory, table)?;
                crate::memory_accounting::merge_preparation_original(memory, &mut key_control)?;
                rows.insert(name, by_id);
            }
            // The source window remains alive until the actual nested copy is
            // complete; no decode-first or current-owner replacement occurs.
            let bytes = crate::memory_accounting::preparation_row_copy_bytes(window.value())?;
            crate::memory_accounting::grow_preparation_from_original(
                memory,
                bytes,
                subsystem,
                operation,
                "Reduce changed row bodies or raise MEMORY_LIMIT.",
            )?;
            let row = crate::memory_accounting::preparation_copy_row(window.value())?;
            rows.get_mut(table)
                .expect("admitted selected table")
                .insert(row_id, row);
            Ok(())
        };
        for (table, row, _) in &ws.relational_deletes {
            retain(table, *row, "relational", "delete_pre_image")?;
        }
        for entry in &ws.vector_inserts {
            retain(
                &entry.index.table,
                entry.row_id,
                "vector",
                "accepted_row_source",
            )?;
        }
        for (index, row, _) in &ws.vector_deletes {
            retain(&index.table, *row, "vector", "accepted_row_source")?;
        }
        for (index, from, to, _) in &ws.vector_moves {
            retain(&index.table, *from, "vector", "accepted_row_source")?;
            retain(&index.table, *to, "vector", "accepted_row_source")?;
        }
        Ok((!rows.is_empty()).then_some(rows))
    }
}

struct OriginalMetadataPublicationOwner {
    store: usize,
    box_pointer: Option<usize>,
    box_credit: Option<crate::memory_accounting::OwnedMemoryReservation>,
}
struct OriginalMetadataPublicationReservation {
    store: usize,
    root: Option<crate::memory_accounting::OwnedMemoryReservation>,
}
thread_local! {static ORIGINAL_METADATA_PUBLICATION:std::cell::Cell<*mut OriginalMetadataPublicationOwner>=const{std::cell::Cell::new(std::ptr::null_mut())};}
struct OriginalMetadataPublicationScope(*mut OriginalMetadataPublicationOwner);
impl Drop for OriginalMetadataPublicationScope {
    fn drop(&mut self) {
        ORIGINAL_METADATA_PUBLICATION.with(|slot| slot.set(self.0));
    }
}

impl OwnedPersistentCompositeStore {
    /// The private host keeps this control owner outside the Any Box across
    /// the existing metadata publication gate, including failure and unwind.
    pub(crate) fn with_owned_metadata_publication<R>(&self, f: impl FnOnce() -> R) -> R {
        let mut owner = OriginalMetadataPublicationOwner {
            store: self as *const Self as usize,
            box_pointer: None,
            box_credit: None,
        };
        let prior = ORIGINAL_METADATA_PUBLICATION.with(|slot| slot.replace(&mut owner));
        let scope = OriginalMetadataPublicationScope(prior);
        let result = f();
        drop(scope);
        drop(owner);
        result
    }

    pub(crate) fn apply_owned_with_vector_reclamation(
        &self,
        ws: &WriteSet,
        reclaimable_partitions: HashSet<VectorPartitionRef>,
        registered_snapshots: Option<&[SnapshotId]>,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<()> {
        let identity = self.owned_terminal_apply_identity(ws)?;
        self.apply_owned_with_vector_reclamation_for_terminal(
            ws,
            identity,
            reclaimable_partitions,
            registered_snapshots,
            read_memory,
        )
    }
    pub(crate) fn apply_owned_with_vector_reclamation_for_terminal(
        &self,
        ws: &WriteSet,
        identity: contextdb_tx::manager::EngineTerminalIdentity,
        reclaimable_partitions: HashSet<VectorPartitionRef>,
        registered_snapshots: Option<&[SnapshotId]>,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<()> {
        self.events.with_owned_persistence_original(|original| {
            // This seed loan is immutable and outside all stage/queue/state
            // guards; its private handle keeps the physical storage alive.
            let reader = crate::read_image_memory::ImageMemory::operation_reader(
                original.accountant().clone(),
                read_memory.clone(),
            )?;
            let _read_scope = reader.enter();
            crate::database::discard::validate_original_discard_for_apply(
                ws,
                &self.legacy.inner.local_erasure_stages,
            )?;
            if ws
                .commit_lsn
                .is_some_and(|lsn| self.legacy.local_schema_stages.lock().contains_key(&lsn))
            {
                return self.with_owned_flush_inputs(ws, identity, |input, stage| {
                    let OwnedFlushInput::Local(projection) = input else {
                        return Err(Error::Other(
                            "local native flush input changed while lending".into(),
                        ));
                    };
                    self.legacy.persistence.flush_owned_local_schema_stage(
                        original,
                        ws,
                        projection,
                        stage.and_then(|stage| stage.schema_config),
                        crate::persistence::original_schema_event_config_position(
                            &projection.config_values,
                        ),
                    )
                });
            }
            if ws
                .commit_lsn
                .is_some_and(|lsn| self.legacy.received_schema_stages.lock().contains_key(&lsn))
            {
                return self.with_owned_flush_inputs(ws, identity, |input, stage| {
                    let OwnedFlushInput::Received {
                        change_log,
                        projection,
                        trigger_audits,
                    } = input
                    else {
                        return Err(Error::Other(
                            "received native flush input changed while lending".into(),
                        ));
                    };
                    self.legacy.persistence.flush_owned_received_schema_stage(
                        original,
                        ws,
                        change_log,
                        projection,
                        stage.as_ref().map_or(&[], |stage| stage.events),
                        trigger_audits,
                        MAX_SINK_QUEUE_DEPTH,
                        stage.and_then(|stage| stage.schema_config),
                        crate::persistence::original_schema_event_config_position(
                            &projection.config_values,
                        ),
                    )
                });
            }
            let trigger_ddl = ws.commit_lsn.and_then(|lsn| {
                self.legacy
                    .trigger
                    .as_ref()
                    .and_then(|trigger| trigger.staged_trigger_persistence_commit(lsn))
            });
            let trigger_audits = ws
                .commit_lsn
                .and_then(|lsn| {
                    self.legacy
                        .trigger
                        .as_ref()
                        .map(|trigger| trigger.take_staged_persistence_audits(lsn))
                })
                .unwrap_or_default();
            let table_meta = self.legacy.inner.relational.table_meta.read();
            let mut accepted_row_memory =
                crate::memory_accounting::reserve_preparation_from_original(
                    original,
                    0,
                    "writer_source",
                    "selected_commit_bodies",
                    "Raise MEMORY_LIMIT to prepare selected commit bodies.",
                )?;
            let deleted_rows = self.deleted_rows_snapshot_for_owned_write_set(
                ws,
                &mut accepted_row_memory,
                read_memory.clone(),
            )?;
            let (log_entries, mut log_memory) = self
                .legacy
                .inner
                .prepare_owned_change_log_entries_with_snapshots(
                    original,
                    ws,
                    Some(&table_meta),
                    deleted_rows.as_ref(),
                )?;
            let projection = crate::memory_accounting::persistence_original_projection(original)?;
            let mut vector_copy_owner =
                crate::vector_read_memory::admission(projection.read_budget()?);
            let partition_mutations = match registered_snapshots {
                Some(snapshots) => self
                    .legacy
                    .inner
                    .prepare_vector_partition_mutations_with_registered_snapshots(
                        ws,
                        &table_meta,
                        deleted_rows.as_ref(),
                        snapshots,
                        &mut vector_copy_owner,
                        read_memory.clone(),
                    )?,
                None => self
                    .legacy
                    .inner
                    .prepare_vector_partition_mutations_with_reclaimable_partitions(
                        ws,
                        &table_meta,
                        deleted_rows.as_ref(),
                        reclaimable_partitions,
                        &mut vector_copy_owner,
                        read_memory.clone(),
                    )?,
            };
            crate::memory_accounting::merge_preparation_original(
                &mut log_memory,
                &mut accepted_row_memory,
            )?;
            let prepared = crate::composite_store::OwnedPreparedMemoryApply::prepare(
                original,
                ws,
                log_entries,
                partition_mutations,
                log_memory,
            )?;
            let erasure_stages = self.legacy.inner.local_erasure_stages.lock();
            let erasure = ws.commit_lsn.and_then(|lsn| erasure_stages.get(&lsn));
            self.with_owned_flush_inputs(ws, identity, |input, stage| {
                if !matches!(input, OwnedFlushInput::Ordinary) {
                    return Err(Error::Other(
                        "ordinary native flush input changed while lending".into(),
                    ));
                }
                self.legacy
                    .persistence
                    .flush_owned_data_with_logs_and_sink_events(
                        original,
                        ws,
                        &prepared.log_entries,
                        FlushDataOptions {
                            local_erasure: erasure.map(|stage| LocalErasurePersistence {
                                projection: &stage.durable,
                                accountant: stage._vector_workspace.accountant().clone(),
                                operation: "discard",
                            }),
                            sink_events: stage.as_ref().map_or(&[], |stage| stage.events),
                            trigger_audits: &trigger_audits,
                            schema_ddl: SchemaDdlPersistence {
                                event_bus: stage.as_ref().and_then(|stage| stage.ddl),
                                trigger: trigger_ddl.as_ref(),
                            },
                            max_sink_queue_depth: MAX_SINK_QUEUE_DEPTH,
                            snapshots: FlushDataSnapshots {
                                table_meta: Some(&table_meta),
                                deleted_rows,
                            },
                            received_schema: None,
                            partition_mutations: prepared.partition_mutations.as_ref(),
                        },
                        stage.and_then(|stage| stage.schema_config),
                        0,
                    )
            })?;
            // Both combined-stage and non-event stage loans have ended before
            // memory publication can take/drop a stage or invoke its closure.
            drop(erasure_stages);
            drop(table_meta);
            #[cfg(feature = "in-process-test-seams")]
            crate::writer_commit_test_seams::reached(
                crate::writer_commit_test_seams::CommitBoundary::BeforeMemoryPublication,
            );
            self.legacy
                .inner
                .apply_owned_exact_with_log_entries(ws, prepared)
        })
    }
}

impl WriteSetApplicator for OwnedPersistentCompositeStore {
    fn reserve_metadata_publication(&self) -> Result<Option<Box<dyn std::any::Any + Send>>> {
        let store = self as *const Self as usize;
        self.events.with_owned_persistence_original(|original| {
            ORIGINAL_METADATA_PUBLICATION.with(|slot| {
                let pointer = slot.get();
                if pointer.is_null() {
                    return Err(Error::Other(
                        "native metadata publication requires its scoped external Box control"
                            .into(),
                    ));
                }
                // SAFETY: with_owned_metadata_publication retains the concrete
                // stack owner synchronously through this existing host gate.
                let owner = unsafe { &mut *pointer };
                if owner.store != store || owner.box_credit.is_some() {
                    return Err(Error::Other(
                        "native metadata publication control identity mismatch".into(),
                    ));
                }
                let root = self
                    .legacy
                    .persistence
                    .reserve_owned_writer_snapshot(original)?;
                let credit = crate::memory_accounting::reserve_preparation_from_original(
                    original,
                    std::mem::size_of::<OriginalMetadataPublicationReservation>(),
                    "writer_source",
                    "publication_box",
                    "Raise MEMORY_LIMIT to admit publication control.",
                )?;
                owner.box_credit = Some(credit);
                let boxed = Box::new(OriginalMetadataPublicationReservation {
                    store,
                    root: Some(root),
                });
                owner.box_pointer =
                    Some((&*boxed as *const OriginalMetadataPublicationReservation) as usize);
                Ok(Some(boxed as Box<dyn std::any::Any + Send>))
            })
        })
    }
    fn complete_metadata_publication(
        &self,
        reservation: Option<Box<dyn std::any::Any + Send>>,
        snapshot: SnapshotId,
        lsn: contextdb_core::Lsn,
    ) {
        let mut boxed = reservation
            .expect("reserved native metadata publication")
            .downcast::<OriginalMetadataPublicationReservation>()
            .unwrap_or_else(|_| panic!("invalid native publication reservation"));
        let pointer = (&*boxed as *const OriginalMetadataPublicationReservation) as usize;
        let store = boxed.store;
        let root = boxed
            .root
            .take()
            .expect("original publication root reservation");
        drop(boxed);
        // The Box allocation is physically gone before external credit drops.
        ORIGINAL_METADATA_PUBLICATION.with(|slot| {
            let ptr = slot.get();
            assert!(
                !ptr.is_null(),
                "native metadata publication retains its external control"
            );
            let owner = unsafe { &mut *ptr };
            assert_eq!(owner.store, store);
            assert_eq!(owner.box_pointer, Some(pointer));
            owner.box_pointer = None;
            drop(owner.box_credit.take());
        });
        assert_eq!(store, self as *const Self as usize);
        self.legacy.prepare_publication(root, snapshot, lsn);
    }
    fn publication_interrupted(&self) -> bool {
        self.legacy
            .publication_interrupted
            .load(std::sync::atomic::Ordering::SeqCst)
    }
    fn publication_handle(&self) -> Option<Arc<dyn std::any::Any + Send + Sync>> {
        self.legacy
            .prepared_publication
            .lock()
            .take()
            .map(|root| root as Arc<dyn std::any::Any + Send + Sync>)
    }
    fn apply(
        &self,
        ws: &WriteSet,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<()> {
        #[cfg(any(test, feature = "test-seams"))]
        if FAIL_NEXT_LOCAL_APPLY.with(|armed| armed.replace(false)) {
            return Err(Error::Other(
                "injected local durable commit failure".to_string(),
            ));
        }
        let identity = self.owned_terminal_apply_identity(ws)?;
        let snapshots = self
            .legacy
            .inner
            .take_vector_partition_snapshot_stage(ws.commit_lsn);
        let snapshot = SnapshotId::from_tx(ws.finalized_tx.ok_or_else(|| {
            Error::Other("durable commit has no finalized transaction identity".into())
        })?);
        let lsn = ws
            .commit_lsn
            .ok_or_else(|| Error::Other("durable commit has no finalized LSN".into()))?;
        let control = self.events.with_owned_persistence_original(|original| {
            self.legacy
                .persistence
                .reserve_owned_writer_snapshot(original)
        })?;
        self.apply_owned_with_vector_reclamation_for_terminal(
            ws,
            identity,
            HashSet::new(),
            snapshots.as_deref(),
            read_memory,
        )?;
        self.legacy.prepare_publication(control, snapshot, lsn);
        Ok(())
    }
    fn new_row_id(&self) -> RowId {
        self.legacy.inner.new_row_id()
    }
}

/// Memory-only Native hosts use the same erase/source/history publication
/// kernel. Preparation is completed while the original source is still lent;
/// no combined-stage guard crosses the actual memory apply.
pub(crate) fn apply_owned_memory_store(
    store: &CompositeStore,
    events: &crate::database::event_bus::OwnedEventBusHandle,
    ws: &WriteSet,
    read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<()> {
    events.with_owned_persistence_original(|original| {
        crate::database::discard::validate_original_discard_for_apply(
            ws,
            &store.local_erasure_stages,
        )?;
        let meta = store.relational.table_meta.read();
        let (entries, credit) = store.prepare_owned_change_log_entries_with_snapshots(
            original,
            ws,
            Some(&meta),
            None,
        )?;
        let original_projection =
            crate::memory_accounting::persistence_original_projection(original)?;
        let mut vector_copy_owner =
            crate::vector_read_memory::admission(original_projection.read_budget()?);
        let batch = store.prepare_vector_partition_mutations_with_reclaimable_partitions(
            ws,
            &meta,
            None,
            HashSet::new(),
            &mut vector_copy_owner,
            read_memory,
        )?;
        let prepared = crate::composite_store::OwnedPreparedMemoryApply::prepare(
            original, ws, entries, batch, credit,
        )?;
        drop(meta);
        store.apply_owned_exact_with_log_entries(ws, prepared)
    })
}

/// Stack-owned preparation; decoded bytes die before their original invoice.
/// The original itself is a loan of the companion field, never a new owner.
pub(crate) struct OwnedDurableQueueUpdate<'o> {
    original: &'o crate::memory_accounting::OwnedMemoryReservation,
    sink: &'o str,
    entry_id: u64,
    decoded: crate::persistence::OriginalDecoded<crate::database::event_bus::SinkQueueEntry>,
    uncertain: bool,
    unconfirmed_delta: i8,
}
pub(crate) struct OwnedDurableQueueRemovals<'o> {
    original: &'o crate::memory_accounting::OwnedMemoryReservation,
    sink: &'o str,
    ids: &'o [u64],
    unconfirmed: u64,
}
impl crate::database::event_bus::OwnedSinkQueuePersistence for RedbPersistence {
    type Update<'o> = OwnedDurableQueueUpdate<'o>;
    type Removals<'o> = OwnedDurableQueueRemovals<'o>;
    fn is_durable(&self) -> bool {
        true
    }
    fn prepare_update<'o>(
        &self,
        original: &'o crate::memory_accounting::OwnedMemoryReservation,
        sink: &'o str,
        entry_id: u64,
        encoded: &[u8],
        uncertain: bool,
        unconfirmed_delta: i8,
    ) -> Result<Self::Update<'o>> {
        let decoded = Self::decode_owned_event::<crate::database::event_bus::SinkQueueEntry>(
            original, encoded,
        )?;
        if decoded.value.id != entry_id {
            return Err(Error::Other(
                "prepared native sink entry identity mismatch".into(),
            ));
        }
        Ok(OwnedDurableQueueUpdate {
            original,
            sink,
            entry_id,
            decoded,
            uncertain,
            unconfirmed_delta,
        })
    }
    fn commit_update(&self, prepared: Self::Update<'_>) -> Result<()> {
        self.update_owned_sink_queue_entry(
            prepared.original,
            prepared.sink,
            Some(prepared.entry_id),
            Some((prepared.entry_id, &prepared.decoded.value)),
            prepared.uncertain,
            prepared.unconfirmed_delta,
        )
    }
    fn commit_update_at_boundary(
        &self,
        prepared: Self::Update<'_>,
        completion: &crate::database::event_bus::OwnedQueueCommitToken<'_>,
        failure: &mut crate::database::event_bus::OwnedNativeQueueFailure,
    ) {
        self.update_owned_sink_queue_entry_at_boundary(
            prepared.original,
            prepared.sink,
            Some(prepared.entry_id),
            Some((prepared.entry_id, &prepared.decoded.value)),
            prepared.uncertain,
            prepared.unconfirmed_delta,
            completion,
            failure,
        );
        // prepared.decoded retires here, after the caller marker is visible.
    }
    fn prepare_removals<'o>(
        &self,
        original: &'o crate::memory_accounting::OwnedMemoryReservation,
        sink: &'o str,
        ids: &'o [u64],
        unconfirmed: u64,
    ) -> Result<Self::Removals<'o>> {
        // Only borrowed stack fields: the worker's admitted batch keeps the
        // identities and companion original alive until this commit returns.
        Ok(OwnedDurableQueueRemovals {
            original,
            sink,
            ids,
            unconfirmed,
        })
    }
    fn commit_removals(&self, prepared: Self::Removals<'_>) -> Result<()> {
        self.remove_owned_sink_queue_entries(
            prepared.original,
            prepared.sink,
            prepared.ids,
            prepared.unconfirmed,
        )
    }
    fn commit_removals_at_boundary(
        &self,
        prepared: Self::Removals<'_>,
        completion: &crate::database::event_bus::OwnedQueueCommitToken<'_>,
        failure: &mut crate::database::event_bus::OwnedNativeQueueFailure,
    ) {
        self.remove_owned_sink_queue_entries_at_boundary(
            prepared.original,
            prepared.sink,
            prepared.ids,
            prepared.unconfirmed,
            completion,
            failure,
        );
    }
}

impl crate::database::event_bus::OwnedSinkQueuePersistence for std::sync::Arc<RedbPersistence> {
    type Update<'o> =
        <RedbPersistence as crate::database::event_bus::OwnedSinkQueuePersistence>::Update<'o>;
    type Removals<'o> =
        <RedbPersistence as crate::database::event_bus::OwnedSinkQueuePersistence>::Removals<'o>;
    fn is_durable(&self) -> bool {
        <RedbPersistence as crate::database::event_bus::OwnedSinkQueuePersistence>::is_durable(
            self.as_ref(),
        )
    }
    fn prepare_update<'o>(
        &self,
        original: &'o crate::memory_accounting::OwnedMemoryReservation,
        sink: &'o str,
        entry_id: u64,
        encoded: &[u8],
        uncertain: bool,
        delta: i8,
    ) -> Result<Self::Update<'o>> {
        <RedbPersistence as crate::database::event_bus::OwnedSinkQueuePersistence>::prepare_update(
            self.as_ref(),
            original,
            sink,
            entry_id,
            encoded,
            uncertain,
            delta,
        )
    }
    fn commit_update(&self, prepared: Self::Update<'_>) -> Result<()> {
        <RedbPersistence as crate::database::event_bus::OwnedSinkQueuePersistence>::commit_update(
            self.as_ref(),
            prepared,
        )
    }
    fn commit_update_at_boundary(
        &self,
        prepared: Self::Update<'_>,
        completion: &crate::database::event_bus::OwnedQueueCommitToken<'_>,
        failure: &mut crate::database::event_bus::OwnedNativeQueueFailure,
    ) {
        <RedbPersistence as crate::database::event_bus::OwnedSinkQueuePersistence>::commit_update_at_boundary(self.as_ref(), prepared, completion, failure)
    }
    fn prepare_removals<'o>(
        &self,
        original: &'o crate::memory_accounting::OwnedMemoryReservation,
        sink: &'o str,
        ids: &'o [u64],
        unconfirmed: u64,
    ) -> Result<Self::Removals<'o>> {
        <RedbPersistence as crate::database::event_bus::OwnedSinkQueuePersistence>::prepare_removals(
            self.as_ref(),
            original,
            sink,
            ids,
            unconfirmed,
        )
    }
    fn commit_removals(&self, prepared: Self::Removals<'_>) -> Result<()> {
        <RedbPersistence as crate::database::event_bus::OwnedSinkQueuePersistence>::commit_removals(
            self.as_ref(),
            prepared,
        )
    }
    fn commit_removals_at_boundary(
        &self,
        prepared: Self::Removals<'_>,
        completion: &crate::database::event_bus::OwnedQueueCommitToken<'_>,
        failure: &mut crate::database::event_bus::OwnedNativeQueueFailure,
    ) {
        <RedbPersistence as crate::database::event_bus::OwnedSinkQueuePersistence>::commit_removals_at_boundary(self.as_ref(), prepared, completion, failure)
    }
}
