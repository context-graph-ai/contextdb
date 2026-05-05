use crate::composite_store::CompositeStore;
use crate::database::event_bus::{EventBusState, MAX_SINK_QUEUE_DEPTH};
use crate::database::trigger::TriggerState;
use crate::persistence::{RedbPersistence, SchemaDdlPersistence};
use contextdb_core::{Result, RowId};
use contextdb_tx::{WriteSet, WriteSetApplicator};
use std::sync::Arc;

pub struct PersistentCompositeStore {
    inner: CompositeStore,
    persistence: Arc<RedbPersistence>,
    event_bus: Option<Arc<EventBusState>>,
    trigger: Option<Arc<TriggerState>>,
}

impl PersistentCompositeStore {
    pub(crate) fn new(
        inner: CompositeStore,
        persistence: Arc<RedbPersistence>,
        event_bus: Option<Arc<EventBusState>>,
        trigger: Option<Arc<TriggerState>>,
    ) -> Self {
        Self {
            inner,
            persistence,
            event_bus,
            trigger,
        }
    }
}

impl WriteSetApplicator for PersistentCompositeStore {
    fn apply(&self, ws: WriteSet) -> Result<()> {
        let log_entries = self.inner.build_change_log_entries(&ws);
        let sink_events = ws
            .commit_lsn
            .and_then(|lsn| {
                self.event_bus
                    .as_ref()
                    .map(|event_bus| event_bus.take_staged_sink_events_for_persistence(lsn))
            })
            .unwrap_or_default();
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
        self.persistence.flush_data_with_logs_and_sink_events(
            &ws,
            &log_entries,
            &sink_events,
            &trigger_audits,
            SchemaDdlPersistence {
                event_bus: event_bus_ddl.as_ref(),
                trigger: trigger_ddl.as_ref(),
            },
            MAX_SINK_QUEUE_DEPTH,
        )?;
        self.inner.apply_exact(ws)
    }

    fn new_row_id(&self) -> RowId {
        self.inner.new_row_id()
    }
}
