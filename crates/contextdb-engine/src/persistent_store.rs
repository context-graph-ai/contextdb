use crate::composite_store::CompositeStore;
use crate::database::event_bus::{EventBusState, MAX_SINK_QUEUE_DEPTH};
use crate::persistence::RedbPersistence;
use contextdb_core::{Result, RowId};
use contextdb_tx::{WriteSet, WriteSetApplicator};
use std::sync::Arc;

pub struct PersistentCompositeStore {
    inner: CompositeStore,
    persistence: Arc<RedbPersistence>,
    event_bus: Option<Arc<EventBusState>>,
}

impl PersistentCompositeStore {
    pub(crate) fn new(
        inner: CompositeStore,
        persistence: Arc<RedbPersistence>,
        event_bus: Option<Arc<EventBusState>>,
    ) -> Self {
        Self {
            inner,
            persistence,
            event_bus,
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
        self.persistence.flush_data_with_logs_and_sink_events(
            &ws,
            &log_entries,
            &sink_events,
            event_bus_ddl.as_ref(),
            MAX_SINK_QUEUE_DEPTH,
        )?;
        self.inner.apply_exact(ws)
    }

    fn new_row_id(&self) -> RowId {
        self.inner.new_row_id()
    }
}
