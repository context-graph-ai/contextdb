//! Committing a delivery re-verifies the pending-delivery membership only of
//! the roots that delivery touches. A record a commit merely restates (the
//! authority certificate every delivery repeats) re-verifies nothing, so the
//! verification work per block of deliveries stays flat as the delivered
//! history grows, instead of re-verifying every current root on every push.
//!
//! Runs in a fresh child process, where the process-wide re-verification
//! count sees only this test's work.
use crate::custody_fixture::*;
use contextdb_engine::Database;
use contextdb_server::InProcessBroker;
use std::sync::Arc;

const MODE: &str = "CONTEXTDB_PENDING_MAINTENANCE_FLAT";
const BLOCK: usize = 32;
const BLOCKS: usize = 3;

async fn blocks() {
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker, Arc::new(Database::open_memory())).await;
    let dir = tempfile::tempdir().unwrap();
    let (edge, client) = bound_edge(&broker, dir.path(), None).await;
    let mut per_block = Vec::with_capacity(BLOCKS);
    for block in 0..BLOCKS {
        let before = Database::__custody_pending_rechecks_for_test();
        for unit in block * BLOCK..(block + 1) * BLOCK {
            commit_unit(&edge, unit);
            if (unit + 1).is_multiple_of(16) {
                client.push().await.unwrap();
            }
        }
        per_block.push(Database::__custody_pending_rechecks_for_test() - before);
    }
    println!("S3_PENDING_RECHECKS per_block={per_block:?}");
    assert!(per_block[0] > 0, "the re-verification count is wired");
    assert!(
        per_block[BLOCKS - 1] <= per_block[0] + BLOCK as u64,
        "re-verification per block of {BLOCK} deliveries grows with the delivered history: \
         {per_block:?}"
    );
    drop(client);
    drop(edge);
    hub.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn pending_maintenance_stays_flat_as_delivered_history_grows() {
    if std::env::var(MODE).is_ok() {
        blocks().await;
        return;
    }
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            &crate::child_test_filter(
                module_path!(),
                "pending_maintenance_stays_flat_as_delivered_history_grows",
            ),
            "--nocapture",
            "--test-threads=1",
        ])
        .env(MODE, "1")
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    for line in stdout
        .lines()
        .filter(|line| line.contains("S3_PENDING_RECHECKS"))
    {
        eprintln!("{line}");
    }
    assert!(
        output.status.success(),
        "{stdout}\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
}
