//! Optional writer checkpoint receipts.
use super::*;
use std::io::Write;

static EMISSION: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

impl Database {
    pub(super) fn writer_receipt_observer_for_maintenance(&self) -> Option<Arc<Database>> {
        std::env::var_os("CONTEXTDB_TEST_SEAMS_RECEIPT_PATH")?;
        // The existing internal view shares the stores and their charges.
        // Only an enabled observer retains this view; the maintenance worker
        // drops it when close stops and joins that worker.
        let mut observer = self.scoped_with_constraints(None, None, None);
        observer.id = self.id;
        Some(Arc::new(observer))
    }

    pub(super) fn emit_writer_memory_ownership_receipt_for_test(&self, checkpoint: &str) {
        let Some(path) = std::env::var_os("CONTEXTDB_TEST_SEAMS_RECEIPT_PATH") else {
            return;
        };
        // No caller holds the commit boundary here. Serialize samples before
        // capturing their roots so concurrent commits cannot reorder the lines.
        let _emission = EMISSION.lock();
        let (receipt, lsn, schema_history_charge) = self
            .with_writer_memory_ownership_receipt_for_test(|receipt| {
                (
                    receipt,
                    self.tx_mgr.capture_published_read().lsn,
                    self.schema_history_retained_bytes(),
                )
            });
        let rss_bytes = current_process_rss_bytes();
        let serde_json::Value::Object(mut owners) = serde_json::to_value(&receipt)
            .expect("writer ownership observations serialize as primitive fields")
        else {
            unreachable!("writer ownership observations are an object");
        };
        owners.insert(
            "schema_history_charge".into(),
            serde_json::json!(schema_history_charge),
        );
        let line = serde_json::json!({
            "kind": "writer_memory_ownership",
            "checkpoint": checkpoint,
            "lsn": lsn.0,
            "pid": std::process::id(),
            "database_id": self.id.0,
            "wallclock_ms": Wallclock::now().0,
            "rss_bytes": rss_bytes,
            "receipt": owners,
        });
        let result = append_receipt(&path, &line);
        if let Err(error) = result {
            tracing::warn!(?path, %error, "test-seams writer receipt emission failed");
        }
    }
}

fn current_process_rss_bytes() -> Option<u64> {
    std::fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find_map(|line| {
            line.strip_prefix("VmRSS:")?
                .split_whitespace()
                .next()?
                .parse::<u64>()
                .ok()?
                .checked_mul(1024)
        })
}

// The caller holds EMISSION; each writer/search sample is one complete line.
fn append_receipt(path: &std::ffi::OsStr, line: &serde_json::Value) -> std::io::Result<()> {
    let mut bytes = serde_json::to_vec(line).map_err(std::io::Error::other)?;
    bytes.push(b'\n');
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?
        .write_all(&bytes)
}

pub(crate) fn emit_vector_search_receipt(
    ordinal: u64,
    builds: u64,
    inserts: u64,
    candidates: u64,
    elapsed_us: u128,
) {
    let Some(path) = std::env::var_os("CONTEXTDB_TEST_SEAMS_RECEIPT_PATH") else {
        return;
    };
    let _emission = EMISSION.lock();
    let line = serde_json::json!({
        "kind": "vector_search",
        "search_ordinal": ordinal,
        "graph_builds_observed": builds,
        "graph_insertions_observed": inserts,
        "candidates": candidates,
        "elapsed_us": elapsed_us,
        "pid": std::process::id(),
        "thread_id": format!("{:?}", crate::owned_worker::current_thread_id()),
    });
    if let Err(error) = append_receipt(&path, &line) {
        tracing::warn!(%error, "cannot append vector search test receipt");
    }
}
