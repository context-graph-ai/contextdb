use contextdb_engine::plugin::{CommitSource, DatabasePlugin};
use contextdb_tx::WriteSet;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::mpsc;

/// Plugin that marks auto-sync as active.
/// Sends change notifications to the background push task via an mpsc channel.
pub struct SyncPlugin {
    tx: std::sync::Mutex<Option<mpsc::Sender<()>>>,
    auto_enabled: AtomicBool,
}

impl SyncPlugin {
    pub fn new(tx: mpsc::Sender<()>) -> Self {
        Self {
            tx: std::sync::Mutex::new(Some(tx)),
            auto_enabled: AtomicBool::new(false),
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

    /// Signal the background push task that a DML change occurred.
    pub fn notify_change(&self) {
        if let Some(tx) = self.tx.lock().unwrap().as_ref() {
            let _ = tx.try_send(());
        }
    }

    /// Shutdown: drop the sender to close the channel and stop the background task.
    pub fn shutdown(&self) {
        let _ = self.tx.lock().unwrap().take();
    }
}

impl DatabasePlugin for SyncPlugin {
    fn post_commit(&self, _ws: &WriteSet, _source: CommitSource) {
        // No-op: auto-push is driven from the REPL loop, not from the plugin hook.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn sync_03_plugin_survives_poisoned_mutex() {
        let (tx, _rx) = mpsc::channel(4);
        let plugin = Arc::new(SyncPlugin::new(tx));
        let poison_plugin = plugin.clone();
        let _ = std::thread::spawn(move || {
            let _guard = poison_plugin.tx.lock().unwrap();
            panic!("poison sync_plugin mutex");
        })
        .join();

        let panic = std::panic::catch_unwind(|| plugin.notify_change());
        assert!(
            panic.is_ok(),
            "notify_change should not panic on a poisoned sync plugin mutex"
        );
    }
}
