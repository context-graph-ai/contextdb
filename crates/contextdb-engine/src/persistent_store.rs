use crate::composite_store::CompositeStore;
use crate::database::event_bus::{EventBusState, MAX_SINK_QUEUE_DEPTH};
use crate::database::trigger::TriggerState;
use crate::database::{LocalSchemaStageRegistry, ReceivedSchemaStageRegistry};
use crate::persistence::{
    FlushDataOptions, FlushDataSnapshots, RedbPersistence, SchemaDdlPersistence,
};
use contextdb_core::{Error, Result, RowId, SnapshotId};
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
    precommit_source: parking_lot::Mutex<std::sync::Weak<crate::persistence::WriterSnapshot>>,
    _precommit_source_memory: crate::memory_accounting::OwnedMemoryReservation,
    publication_interrupted: std::sync::atomic::AtomicBool,
}

impl PersistentCompositeStore {
    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn source_control_bytes_for_test(&self) -> usize {
        self._precommit_source_memory.bytes()
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
        let source_memory = crate::memory_accounting::OwnedMemoryReservation::try_new_for(
            inner.accountant.clone(),
            std::mem::size_of::<
                parking_lot::Mutex<std::sync::Weak<crate::persistence::WriterSnapshot>>,
            >(),
            "writer_source",
            "commit_root_reference",
            "Raise MEMORY_LIMIT to retain commit source control.",
        )?;
        Ok(Self {
            inner,
            persistence,
            event_bus,
            trigger,
            received_schema_stages,
            local_schema_stages,
            prepared_publication: parking_lot::Mutex::new(None),
            precommit_source: parking_lot::Mutex::new(Arc::downgrade(&initial_root)),
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
        let table_meta = self.inner.relational.table_meta.read().clone();
        #[cfg(feature = "test-seams")]
        crate::persistence::observe_append_stage(
            crate::persistence::AppendStage::TableMetaClone,
            &mut stage_started,
        );
        let mut accepted_row_memory =
            crate::memory_accounting::OwnedMemoryReservation::new(self.inner.accountant.clone());
        let deleted_rows =
            self.deleted_rows_snapshot_for_write_set(ws, &mut accepted_row_memory)?;
        let log_entries = self.inner.build_change_log_entries_with_snapshots(
            ws,
            Some(&table_meta),
            deleted_rows.as_ref(),
        );
        let partition_mutations = match registered_snapshots {
            Some(registered_snapshots) => self
                .inner
                .prepare_vector_partition_mutations_with_registered_snapshots(
                    ws,
                    &table_meta,
                    deleted_rows.as_ref(),
                    registered_snapshots,
                )?,
            None => self
                .inner
                .prepare_vector_partition_mutations_with_reclaimable_partitions(
                    ws,
                    &table_meta,
                    deleted_rows.as_ref(),
                    reclaimable_partitions,
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
                local_erasure: erasure.map(|stage| &stage.durable),
                sink_events: sink_events.as_deref().map(Vec::as_slice).unwrap_or(&[]),
                trigger_audits: &trigger_audits,
                schema_ddl: SchemaDdlPersistence {
                    event_bus: event_bus_ddl.as_ref(),
                    trigger: trigger_ddl.as_ref(),
                },
                max_sink_queue_depth: MAX_SINK_QUEUE_DEPTH,
                snapshots: FlushDataSnapshots {
                    table_meta: Some(table_meta),
                    deleted_rows,
                },
                received_schema: None,
                partition_mutations: Some(&partition_mutations),
            },
        )?;
        drop(erasure_stages);
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
        let root = self.precommit_source.lock().upgrade().ok_or_else(|| {
            Error::Other("writer publication no longer owns its commit source".into())
        })?;
        let source = crate::persistence::writer_adapters::WriterRelationalSource::new(root)?;
        let mut rows = std::collections::HashMap::<
            String,
            std::collections::HashMap<RowId, contextdb_core::VersionedRow>,
        >::new();
        let mut retain = |table: &str, row_id: RowId| -> Result<()> {
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
                    "vector",
                    "accepted_row_source",
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
                "vector",
                "accepted_row_source",
                "Reduce changed row bodies or raise MEMORY_LIMIT.",
            )?;
            by_id
                .try_reserve(1)
                .map_err(|_| Error::Other("accepted row identity allocation failed".into()))?;
            by_id.insert(row_id, window.value().clone());
            Ok(())
        };
        for (table, row, _) in &ws.relational_deletes {
            retain(table, *row)?;
        }
        for entry in &ws.vector_inserts {
            retain(&entry.index.table, entry.row_id)?;
        }
        for (index, row, _) in &ws.vector_deletes {
            retain(&index.table, *row)?;
        }
        for (index, from, to, _) in &ws.vector_moves {
            retain(&index.table, *from)?;
            retain(&index.table, *to)?;
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
        *self.precommit_source.lock() = Arc::downgrade(&root);
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

    fn apply(&self, ws: &WriteSet) -> Result<()> {
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
        self.apply_with_vector_reclamation(ws, HashSet::new(), registered_snapshots.as_deref())?;
        self.prepare_publication(control, snapshot, lsn);
        Ok(())
    }

    fn new_row_id(&self) -> RowId {
        self.inner.new_row_id()
    }
}
