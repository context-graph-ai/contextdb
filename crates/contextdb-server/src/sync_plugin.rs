use contextdb_core::{AtomicLsn, Lsn};
use contextdb_engine::plugin::{CommitSource, DatabasePlugin};
use contextdb_tx::WriteSet;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::mpsc;

/// Plugin that marks auto-sync as active.
/// Sends change notifications to the background push task via an mpsc channel.
pub struct SyncPlugin {
    tx: std::sync::Mutex<Option<mpsc::UnboundedSender<()>>>,
    auto_enabled: AtomicBool,
    pending_lsn: AtomicLsn,
}

impl SyncPlugin {
    pub fn new(tx: mpsc::UnboundedSender<()>) -> Self {
        Self {
            tx: std::sync::Mutex::new(Some(tx)),
            auto_enabled: AtomicBool::new(false),
            pending_lsn: AtomicLsn::new(Lsn(0)),
        }
    }

    /// Enable or disable auto-sync.
    pub fn set_auto(&self, enabled: bool) {
        self.auto_enabled.store(enabled, Ordering::SeqCst);
    }

    /// Check if auto-sync is enabled.
    pub fn is_auto(&self) -> bool {
        self.auto_enabled.load(Ordering::SeqCst)
    }

    pub fn pending_lsn(&self) -> Lsn {
        self.pending_lsn.load(Ordering::SeqCst)
    }

    /// Signal the background push task that a DML change occurred.
    pub fn notify_change(&self) -> Result<(), &'static str> {
        match self.tx.lock() {
            Ok(guard) => {
                if let Some(tx) = guard.as_ref() {
                    if tx.send(()).is_err() {
                        tracing::warn!("sync plugin receiver dropped; change notification lost");
                        return Err("auto-sync worker unavailable");
                    }
                    Ok(())
                } else {
                    Err("auto-sync worker unavailable")
                }
            }
            Err(_) => {
                tracing::warn!("sync plugin mutex poisoned; skipping change notification");
                Err("auto-sync worker unavailable")
            }
        }
    }

    /// Shutdown: drop the sender to close the channel and stop the background task.
    pub fn shutdown(&self) {
        match self.tx.lock() {
            Ok(mut guard) => {
                let _ = guard.take();
            }
            Err(_) => tracing::warn!("sync plugin mutex poisoned during shutdown"),
        }
    }
}

impl DatabasePlugin for SyncPlugin {
    fn post_commit(&self, ws: &WriteSet, source: CommitSource) {
        if !self.is_auto() || source == CommitSource::SyncPull || ws.is_empty() {
            return;
        }
        if let Some(lsn) = ws.commit_lsn {
            self.pending_lsn.fetch_max(lsn, Ordering::SeqCst);
        }
        let _ = self.notify_change();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn sync_03_plugin_survives_poisoned_mutex() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let plugin = Arc::new(SyncPlugin::new(tx));
        let poison_plugin = plugin.clone();
        let _ = std::thread::spawn(move || {
            let _guard = poison_plugin.tx.lock().unwrap();
            panic!("poison sync_plugin mutex");
        })
        .join();

        let panic = std::panic::catch_unwind(|| {
            let _ = plugin.notify_change();
        });
        assert!(
            panic.is_ok(),
            "notify_change should not panic on a poisoned sync plugin mutex"
        );
    }

    #[test]
    fn sync_04_plugin_queues_multiple_notifications() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let plugin = SyncPlugin::new(tx);

        plugin.notify_change().unwrap();
        plugin.notify_change().unwrap();

        assert_eq!(rx.try_recv(), Ok(()));
        assert_eq!(rx.try_recv(), Ok(()));
    }

    #[test]
    fn sync_05_plugin_reports_closed_receiver() {
        let (tx, rx) = mpsc::unbounded_channel();
        let plugin = SyncPlugin::new(tx);
        drop(rx);

        assert_eq!(plugin.notify_change(), Err("auto-sync worker unavailable"));
    }

    #[test]
    fn sync_06_post_commit_notifies_only_for_local_writes_when_enabled() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let plugin = SyncPlugin::new(tx);
        let mut ws = WriteSet::new();
        ws.relational_inserts.push((
            "t".to_string(),
            contextdb_core::VersionedRow {
                row_id: contextdb_core::RowId(1),
                values: std::collections::HashMap::new(),
                created_tx: contextdb_core::TxId(1),
                deleted_tx: None,
                lsn: Lsn(1),
                created_at: None,
            },
        ));

        plugin.post_commit(&ws, CommitSource::AutoCommit);
        assert!(rx.try_recv().is_err(), "disabled auto-sync must stay quiet");

        plugin.set_auto(true);
        plugin.post_commit(&ws, CommitSource::SyncPull);
        assert!(
            rx.try_recv().is_err(),
            "sync-pull commits must not trigger another auto-sync push"
        );

        plugin.post_commit(&ws, CommitSource::AutoCommit);
        assert_eq!(rx.try_recv(), Ok(()));
    }

    #[test]
    fn sync_07_post_commit_tracks_latest_pending_lsn() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let plugin = SyncPlugin::new(tx);
        plugin.set_auto(true);

        let mut ws = WriteSet::new();
        ws.commit_lsn = Some(Lsn(7));
        ws.relational_deletes.push((
            "t".to_string(),
            contextdb_core::RowId(1),
            contextdb_core::TxId(7),
        ));
        plugin.post_commit(&ws, CommitSource::AutoCommit);
        assert_eq!(plugin.pending_lsn(), Lsn(7));

        let mut newer = WriteSet::new();
        newer.commit_lsn = Some(Lsn(11));
        newer.relational_deletes.push((
            "t".to_string(),
            contextdb_core::RowId(2),
            contextdb_core::TxId(11),
        ));
        plugin.post_commit(&newer, CommitSource::AutoCommit);
        assert_eq!(plugin.pending_lsn(), Lsn(11));
    }
}
