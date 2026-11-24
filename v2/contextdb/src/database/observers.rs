//! Best-effort subscriptions own their queued memory and publish only after confirmation.
use super::*;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};

thread_local! { static HOST: RefCell<Vec<*const Open>> = const { RefCell::new(Vec::new()) }; }
pub(super) fn active(open: &Open) -> bool {
    HOST.with(|held| held.borrow().contains(&(open as *const Open)))
}
struct Host;
impl Drop for Host {
    fn drop(&mut self) {
        HOST.with(|held| {
            held.borrow_mut().pop();
        });
    }
}

/// A durable commit's summary. Receivers retain its memory charge until the event is dropped.
/// Its private reservation keeps queued bytes charged; instances come from subscriptions.
#[derive(Clone)]
pub struct CommitEvent {
    pub source: CommitSource,
    pub lsn: TxId,
    pub row_count: usize,
    tables_changed: Option<Arc<(Box<[String]>, Reservation)>>,
}
impl CommitEvent {
    /// The names of the tables the commit changed, in name order.
    pub fn tables_changed(&self) -> &[String] {
        self.tables_changed.as_ref().map_or(&[][..], |t| &*t.0)
    }
}
impl std::fmt::Debug for CommitEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommitEvent")
            .field("source", &self.source)
            .field("lsn", &self.lsn)
            .field("tables_changed", &self.tables_changed())
            .field("row_count", &self.row_count)
            .finish()
    }
}
impl PartialEq for CommitEvent {
    fn eq(&self, other: &Self) -> bool {
        (self.source, self.lsn, self.tables_changed(), self.row_count)
            == (
                other.source,
                other.lsn,
                other.tables_changed(),
                other.row_count,
            )
    }
}
/// The open subscriptions, and how many notices were sent to them and dropped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionMetrics {
    pub active_channels: usize,
    pub events_sent: u64,
    pub events_dropped: u64,
}
type Sent = (Vec<(SyncSender<CommitEvent>, Reservation)>, u64, u64);
#[derive(Default)]
pub(super) struct Subscribers(Mutex<Sent>);
impl Database {
    /// Whether this open created or recreated its data file.
    pub fn created(&self) -> Result<bool> {
        Ok(self.opened()?.store.created())
    }
    /// The last completed cycle; numeric figures are withheld on narrowed handles.
    pub fn last_maintenance_cycle(&self) -> Result<Option<MaintenanceReport>> {
        let open = self.opened()?;
        Ok(locked(&open.last_cycle).map(|r| {
            if self.reader.is_admin() {
                r
            } else {
                r.withheld()
            }
        }))
    }
    /// Best-effort commit wake-ups, with room for 1,024 events. Narrowed handles are refused.
    pub fn subscribe(&self) -> Result<Receiver<CommitEvent>> {
        self.subscription(1024, "subscribe")
    }
    /// A full channel drops its notification and never blocks a commit. Zero means one slot.
    pub fn subscribe_with_capacity(&self, capacity: usize) -> Result<Receiver<CommitEvent>> {
        self.subscription(capacity.max(1), "subscribe_with_capacity")
    }
    fn subscription(&self, capacity: usize, name: &str) -> Result<Receiver<CommitEvent>> {
        admin(&self.reader, Some(name))?;
        let open = self.opened()?;
        let bytes = (capacity as u64).saturating_mul((size_of::<CommitEvent>() + 64) as u64);
        let held = open
            .budget
            .reserve(bytes.saturating_add(128), "commit subscription")?;
        let (tx, rx) = mpsc::sync_channel(capacity);
        locked(&open.subscribers.0).0.push((tx, held));
        Ok(rx)
    }
    /// The open subscriptions and the notices sent and dropped since the store was opened.
    pub fn subscription_health(&self) -> Result<SubscriptionMetrics> {
        admin(&self.reader, Some("subscription_health"))?;
        let open = self.opened()?;
        let (subscribers, sent, dropped) = &*locked(&open.subscribers.0);
        Ok(SubscriptionMetrics {
            active_channels: subscribers.len(),
            events_sent: *sent,
            events_dropped: *dropped,
        })
    }
    /// Host-supplied health; panics on a closed handle, as the carried plain-value call does.
    pub fn plugin_health(&self) -> PluginHealth {
        self.opened()
            .expect("open database")
            .host(|plugin| plugin.health())
    }
    /// Host-supplied description; panics on a closed handle.
    pub fn plugin_describe(&self) -> serde_json::Value {
        self.opened()
            .expect("open database")
            .host(|plugin| plugin.describe())
    }
    /// Declared events and confirmed delivery counters; requires an unnarrowed handle.
    pub fn event_bus_status(&self) -> Result<crate::Owned<EventBusStatus>> {
        admin(&self.reader, Some("event_bus_status"))?;
        let open = self.opened()?;
        let (view, catalog) = open.newest()?;
        let cron = open.triggers.cron_names();
        open.deliveries
            .status(&view, &catalog, &open.budget, true, &cron)
    }
    /// Confirmed outcomes and committed pending depth; requires an unnarrowed handle.
    pub fn sink_metrics(&self, sink: &str) -> Result<SinkMetrics> {
        admin(&self.reader, Some("sink_metrics"))?;
        let open = self.opened()?;
        let (view, _) = open.newest()?;
        open.deliveries.metrics(&open, &view, sink)
    }
}
impl Open {
    pub(super) fn host<T>(&self, call: impl FnOnce(&dyn DatabasePlugin) -> T) -> T {
        if self.plugin.is_none() {
            return call(&CorePlugin);
        }
        HOST.with(|held| held.borrow_mut().push(self as *const Open));
        let _host = Host;
        call(self.plugin())
    }
    pub(super) fn plugin(&self) -> &dyn DatabasePlugin {
        self.plugin.as_deref().unwrap_or(&CorePlugin)
    }
    pub(super) fn failed(&self, changes: &ChangeSet, source: CommitSource, error: &Error) {
        self.triggers.audit_failed(error);
        let _ = catch_unwind(AssertUnwindSafe(|| {
            self.host(|plugin| plugin.commit_failed(changes, source, error))
        }));
    }
    pub(super) fn observed(
        &self,
        sql: &str,
        run: impl FnOnce() -> Result<QueryResult>,
    ) -> Result<QueryResult> {
        let Some(_) = &self.plugin else {
            return run();
        };
        let start = self.clock.now();
        let result = self
            .host(|plugin| plugin.on_query(sql))
            .and_then(|()| run());
        let duration = self.clock.now().duration_since(start).unwrap_or_default();
        let outcome = match &result {
            Ok(result) => QueryOutcome::Success {
                row_count: result.rows().len().max(result.rows_affected() as usize),
            },
            Err(error) => QueryOutcome::Error {
                error: error.to_string(),
            },
        };
        let _ = catch_unwind(AssertUnwindSafe(|| {
            self.host(|plugin| plugin.post_query(sql, duration, &outcome))
        }));
        result
    }
    pub(super) fn confirmed(
        &self,
        changes: &ChangeSet,
        source: CommitSource,
        event: Option<CommitEvent>,
    ) {
        let _ = catch_unwind(AssertUnwindSafe(|| {
            self.host(|plugin| plugin.post_commit(changes, source))
        }));
        if let Some(event) = event {
            let (subscribers, sent, dropped) = &mut *locked(&self.subscribers.0);
            subscribers.retain(|(tx, _)| {
                let result = tx.try_send(event.clone());
                *sent += u64::from(result.is_ok());
                *dropped += u64::from(matches!(result, Err(TrySendError::Full(_))));
                !matches!(result, Err(TrySendError::Disconnected(_)))
            });
        }
    }
    pub(super) fn summary(
        &self,
        changes: &mut ChangeSet,
        catalog: &Catalog,
        source: CommitSource,
    ) -> Option<CommitEvent> {
        if locked(&self.subscribers.0).0.is_empty() {
            return None;
        }
        let mut tables_changed = std::collections::BTreeSet::new();
        let mut row_count = 0;
        for (id, count) in changes.changed_tables() {
            row_count += count as usize;
            if let Some(table) = catalog.by_id(id) {
                tables_changed.insert(table.name.clone());
            }
        }
        let names: Box<[String]> = tables_changed.into_iter().collect();
        let held = changes.summary_charge(names.len() as u64);
        Some(CommitEvent {
            source,
            lsn: TxId(0),
            row_count,
            tables_changed: held.map(|held| Arc::new((names, held))),
        })
    }
}
