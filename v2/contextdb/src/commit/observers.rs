//! Best-effort subscriptions own their queued memory and publish only after confirmation.
use super::*;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::mpsc::{SyncSender, TrySendError};

thread_local! { static HOST: RefCell<Vec<*const Open>> = const { RefCell::new(Vec::new()) }; }
pub(crate) fn active(open: &Open) -> bool {
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
pub(crate) type Sent = (Vec<(SyncSender<CommitEvent>, Reservation)>, u64, u64);
#[derive(Default)]
pub(crate) struct Subscribers(pub(super) Mutex<Sent>);
impl Open {
    pub(crate) fn host<T>(&self, call: impl FnOnce(&dyn DatabasePlugin) -> T) -> T {
        if self.plugin.is_none() {
            return call(&CorePlugin);
        }
        HOST.with(|held| held.borrow_mut().push(self as *const Open));
        let _host = Host;
        call(self.plugin())
    }
    pub(crate) fn plugin(&self) -> &dyn DatabasePlugin {
        self.plugin.as_deref().unwrap_or(&CorePlugin)
    }
    pub(crate) fn failed(&self, changes: &ChangeSet, source: CommitSource, error: &Error) {
        let _ = catch_unwind(AssertUnwindSafe(|| {
            self.host(|plugin| plugin.commit_failed(changes, source, error))
        }));
    }
    pub(crate) fn observed(
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
    pub(crate) fn confirmed(
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
    pub(crate) fn summary(
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
