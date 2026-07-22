//! Production reclaim-driver coverage: the owned cadence actually reclaims.
//!
//! The frozen suite drives `reclaim_unreferenced` explicitly. This proves the
//! OWNED driver — `BlobService::spawn_reclaim_driver` — actually invokes that
//! reclaim on its own, at the real fabric wall-clock, so a holder's blob store
//! cannot fill without bound in a real deployment (a review found the
//! cadence un-driven). It is NOT a frozen acceptance test;
//! it exercises the driver seam a host would wire.

use contextdb_engine::Database;
use contextdb_engine::work_ledger::{
    BlobHash, InputRef, JobSpec, MovementPolicy, cancel_job, install_work_ledger_schema, submit_job,
};
use contextdb_server::FabricIdentity;
use contextdb_server::blob_resolver::BlobService;
use std::sync::Arc;
use std::time::Duration;

fn blob_job(job_id: &str, submitter: &str, hash: &BlobHash) -> JobSpec {
    JobSpec::builder(job_id, "media.demo", "batch", submitter)
        .input_refs(vec![InputRef::blob_ref(hash.clone())])
        .build()
}

#[tokio::test]
async fn spawn_reclaim_driver_reclaims_a_terminal_past_grace_blob_on_its_own() {
    let dir = tempfile::tempdir().expect("dir");
    let key = dir.path().join("fabric-identity.key");
    let node = FabricIdentity::load_or_generate(&key)
        .expect("identity")
        .node_id();

    let db = Arc::new(Database::open_memory());
    install_work_ledger_schema(&db).expect("schema");

    // A blob whose only referencing job is terminal far in the past — so with
    // grace 0 it is immediately reclaim-eligible against the REAL wall-clock
    // the driver reads (the job's cancel time is epoch-0, decades behind now).
    let content = b"reclaim-driver-blob".to_vec();
    let h = BlobHash::of(&content);
    submit_job(&db, &blob_job("job-1", &node, &h), &[] as &[&[u8]]).expect("submit");
    cancel_job(&db, "job-1", "operator", None, 0).expect("cancel");

    let service = Arc::new(BlobService::new(
        db,
        MovementPolicy {
            auto_propagate: true,
        },
        key,
    ));
    // No test clock is injected: the driver runs at the real fabric wall-clock,
    // exactly as in production.
    assert_eq!(service.ingest_bytes(&content).expect("ingest"), h);
    assert_eq!(
        service.servable_count_for_test(),
        1,
        "the blob is servable before the driver sweeps"
    );

    // The driver's first interval tick fires immediately, so a bounded poll
    // observes the sweep within a moment — no waiting on the full interval.
    let handle = service.clone().spawn_reclaim_driver(0);

    let mut reclaimed = false;
    for _ in 0..100 {
        if service.servable_count_for_test() == 0 {
            reclaimed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
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
    let holder = BlobService::new(
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
