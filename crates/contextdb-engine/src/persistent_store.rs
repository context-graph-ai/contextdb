use crate::composite_store::CompositeStore;
use crate::database::event_bus::{EventBusState, MAX_SINK_QUEUE_DEPTH};
use crate::database::trigger::TriggerState;
use crate::database::{LocalSchemaStageRegistry, ReceivedSchemaStageRegistry};
use crate::persistence::{
    FlushDataOptions, FlushDataSnapshots, RedbPersistence, SchemaDdlPersistence,
};
#[cfg(any(test, feature = "test-seams"))]
use contextdb_core::Error;
use contextdb_core::{Result, RowId, SnapshotId};
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
}

impl PersistentCompositeStore {
    pub(crate) fn new(
        inner: CompositeStore,
        persistence: Arc<RedbPersistence>,
        event_bus: Option<Arc<EventBusState>>,
        trigger: Option<Arc<TriggerState>>,
        received_schema_stages: ReceivedSchemaStageRegistry,
        local_schema_stages: LocalSchemaStageRegistry,
    ) -> Self {
        Self {
            inner,
            persistence,
            event_bus,
            trigger,
            received_schema_stages,
            local_schema_stages,
        }
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
        let deleted_rows = self.deleted_rows_snapshot_for_write_set(ws)?;
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

    /// Capture complete pre-delete bodies from the durable source.  The
    /// in-memory relational image of a file-backed writer is intentionally a
    /// lightweight directory; using it here would overwrite retained history
    /// with that projection when persistence stamps `deleted_tx`.
    fn deleted_rows_snapshot_for_write_set(
        &self,
        ws: &WriteSet,
    ) -> Result<
        Option<
            std::collections::HashMap<
                String,
                std::collections::HashMap<RowId, contextdb_core::VersionedRow>,
            >,
        >,
    > {
        if ws.relational_deletes.is_empty() {
            return Ok(None);
        }
        let metadata = self.inner.relational.table_meta.read();
        let mut rows = std::collections::HashMap::<
            String,
            std::collections::HashMap<RowId, contextdb_core::VersionedRow>,
        >::new();
        for (table, row_id, deleted_tx) in &ws.relational_deletes {
            if rows
                .get(table)
                .is_some_and(|by_id| by_id.contains_key(row_id))
            {
                continue;
            }
            let Some(meta) = metadata.get(table) else {
                continue;
            };
            let snapshot = SnapshotId::from_raw_wire(deleted_tx.0.saturating_sub(1));
            if let Some(row) = self
                .persistence
                .load_relational_row_by_id_at(table, meta, *row_id, snapshot)?
            {
                rows.entry(table.clone()).or_default().insert(*row_id, row);
            }
        }
        drop(metadata);
        if rows.is_empty() {
            Ok(self.inner.deleted_rows_snapshot_for_write_set(ws))
        } else {
            Ok(Some(rows))
        }
    }
}

impl WriteSetApplicator for PersistentCompositeStore {
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
        self.apply_with_vector_reclamation(ws, HashSet::new(), registered_snapshots.as_deref())
    }

    fn new_row_id(&self) -> RowId {
        self.inner.new_row_id()
    }
}
