//! Production reclaim-driver coverage: the owned cadence actually reclaims.
//!
//! The frozen suite drives `reclaim_unreferenced` explicitly. This proves the
//! OWNED driver — `BlobStore::spawn_reclaim_driver` — actually invokes that
//! reclaim on its own, so a holder's blob store
//! cannot fill without bound in a real deployment (a review found the
//! cadence un-driven). It is NOT a frozen acceptance test;
//! it exercises the driver seam a host would wire.

use contextdb_engine::Database;
use contextdb_engine::work_ledger::{
    BlobHash, InputRef, JobSpec, MovementPolicy, cancel_job, install_work_ledger_schema, submit_job,
};
use contextdb_server::FabricIdentity;
use contextdb_server::blob_resolver::BlobStore;
use std::sync::Arc;
use std::time::Duration;

fn blob_job(job_id: &str, submitter: &str, hash: &BlobHash) -> JobSpec {
    JobSpec::builder(job_id, "media.demo", "batch", submitter)
        .input_refs(vec![InputRef::blob_ref(hash.clone())])
        .build()
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn spawn_reclaim_driver_reclaims_a_terminal_past_grace_blob_on_its_own() {
    let _clock = contextdb_core::types::Wallclock::test_clock_guard(|| 1_000_000);
    let dir = tempfile::tempdir().expect("dir");
    let key = dir.path().join("fabric-identity.key");
    let node = FabricIdentity::load_or_generate(&key)
        .expect("identity")
        .node_id();

    let db = Arc::new(Database::open_memory());
    install_work_ledger_schema(&db).expect("schema");

    // A blob whose only referencing job is terminal far in the past — so with
    // grace 0 it is immediately reclaim-eligible against the frozen clock
    // the driver reads (the job cancel time is epoch-0).
    let content = b"reclaim-driver-blob".to_vec();
    let h = BlobHash::of(&content);
    submit_job(&db, &blob_job("job-1", &node, &h), &[] as &[&[u8]]).expect("submit");
    cancel_job(&db, "job-1", "operator", None, 0).expect("cancel");

    let service = Arc::new(BlobStore::new(
        db,
        MovementPolicy {
            auto_propagate: true,
        },
        key,
    ));
    assert_eq!(service.ingest_bytes(&content).expect("ingest"), h);
    assert_eq!(
        service.servable_count_for_test(),
        1,
        "the blob is servable before the driver sweeps"
    );

    // Observe the real driver after its first completed sweep.
    let observed = service.observe_next_reclaim_sweep_for_test();
    let handle = service.clone().spawn_reclaim_driver(0);
    let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
    let (watchdog_tx, watchdog_rx) = tokio::sync::oneshot::channel();
    let watchdog = std::thread::spawn(move || {
        if done_rx.recv_timeout(Duration::from_secs(60)).is_err() {
            let _ = watchdog_tx.send(());
        }
    });
    tokio::select! {
        result = observed => result.expect("reclaim sweep never delivered"),
        _ = watchdog_rx => panic!("reclaim sweep never delivered"),
    }
    let _ = done_tx.send(());
    watchdog.join().expect("reclaim watchdog");
    let reclaimed = service.servable_count_for_test() == 0;
    handle.abort();

    assert!(
        reclaimed,
        "the spawned reclaim driver must reclaim the terminal-past-grace blob on its own"
    );
}

/// `ingest_file` must accept a RELATIVE path (the backing store requires an
/// absolute one; the adapter canonicalizes). Regression guard for the opaque
/// `add path: Error::Io` a relative path produced before the fix.
#[tokio::test]
async fn ingest_file_accepts_a_relative_path() {
    let dir = tempfile::tempdir().expect("dir");
    let key = dir.path().join("fabric-identity.key");

    // A file written under a scratch cwd, addressed by a RELATIVE path.
    let cwd = tempfile::tempdir().expect("cwd");
    let prev = std::env::current_dir().expect("cwd read");
    std::env::set_current_dir(cwd.path()).expect("chdir");
    let content = b"relative-path-ingest".to_vec();
    std::fs::write("clip.bin", &content).expect("write clip");

    let db = std::sync::Arc::new(Database::open_memory());
    install_work_ledger_schema(&db).expect("schema");
    let holder = BlobStore::new(
        db,
        MovementPolicy {
            auto_propagate: true,
        },
        key,
    );
    let via_relative = holder
        .ingest_file(std::path::Path::new("clip.bin"))
        .expect("ingest_file must accept a relative path");
    std::env::set_current_dir(&prev).expect("chdir back");

    assert_eq!(
        via_relative,
        BlobHash::of(&content),
        "a relative-path ingest must content-address exactly like the bytes"
    );
}
