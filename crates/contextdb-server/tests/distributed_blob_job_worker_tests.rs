//! Distributed blob-job execution: a `blob_ref` job must be executable by
//! the standard work-ledger worker loop, not just readable via
//! `BlobService::resolve_blob_ref` directly. Today `poll_and_execute_once`
//! never consults a `WorkerConfig`'s blob resolver, so
//! `ledger::materialize_inputs` refuses every `blob_ref` input with
//! `Error::InputRequiresBlobResolver` and the job is recorded Failed instead
//! of executed.

use contextdb_engine::Database;
use contextdb_engine::work_ledger::{
    BlobHash, ExecutionInputs, InputRef, JobSnapshot, JobSpec, MovementPolicy,
    install_work_ledger_schema, job_result, job_state, submit_job,
};
use contextdb_server::blob_resolver::BlobService;
use contextdb_server::work_ledger::{
    ExecutionOutput, ExecutionVerdict, PollOutcome, WorkExecutor, WorkerConfig,
    poll_and_execute_once,
};
use contextdb_server::{FabricIdentity, InProcessBroker, SyncClient};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[path = "work_ledger_support/mod.rs"]
mod work_ledger_support;
use work_ledger_support::*;

async fn within<F: std::future::Future>(fut: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(30), fut)
        .await
        .expect("bounded distributed-blob-job operation exceeded 30s")
}

fn identity_file(dir: &tempfile::TempDir) -> PathBuf {
    dir.path().join("fabric-identity.key")
}

fn node_id_of(key: &Path) -> String {
    FabricIdentity::load_or_generate(key)
        .expect("identity")
        .node_id()
}

fn marked(marker: &str, size: usize) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(size);
    bytes.extend_from_slice(marker.as_bytes());
    let mut n: u8 = 0;
    while bytes.len() < size {
        bytes.push(n);
        n = n.wrapping_add(1);
    }
    bytes.truncate(size);
    bytes
}

fn blob_job(job_id: &str, submitter: &str, hash: &BlobHash) -> JobSpec {
    JobSpec::builder(job_id, "media.demo", "batch", submitter)
        .input_refs(vec![InputRef::blob_ref(hash.clone())])
        .submitted_at_ms(T0)
        .build()
}

/// A worker executor that computes over the REAL materialized bytes (never a
/// rigged shape): a wrong or empty materialization changes the output and
/// fails the assertion below.
struct ReceivedBytesExecutor {
    received: Mutex<Option<Vec<u8>>>,
}

impl WorkExecutor for ReceivedBytesExecutor {
    fn execute(
        &self,
        _job: &JobSnapshot,
        inputs: &ExecutionInputs,
        _should_abandon: &dyn Fn() -> bool,
    ) -> ExecutionVerdict {
        let mut all = Vec::new();
        for (_, chunk) in inputs {
            all.extend_from_slice(chunk);
        }
        *self.received.lock().expect("received lock") = Some(all.clone());
        ExecutionVerdict::Completed(ExecutionOutput {
            output: all,
            receipt: serde_json::json!({ "backend": "test-double" }),
        })
    }
}

// ---------------------------------------------------------------------------
// A blob_ref job submitted by a holder node must be claimable AND executable
// by a worker elsewhere, with the worker's WorkExecutor receiving the real
// resolved bytes fetched node-to-node — not just refused at materialization.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn blob_ref_job_resolves_and_executes_via_worker_loop() {
    let broker = InProcessBroker::new();
    let tenant = "wl-blob-01";
    let (hub_db, shutdown, task) = start_hub(&broker, tenant);

    // The holder: has the bytes, submits the job referencing them, serves
    // its blob door.
    let holder_dir = tempfile::tempdir().expect("holder dir");
    let holder_key = identity_file(&holder_dir);
    let holder_node = node_id_of(&holder_key);
    let content = marked("DISTRIBUTED-COMPUTE-BLOB", 128 * 1024);
    let hash = BlobHash::of(&content);

    let holder_db = Arc::new(Database::open_memory());
    install_work_ledger_schema(&holder_db).expect("holder ledger schema");
    let holder_client = Arc::new(SyncClient::with_transport(
        holder_db.clone(),
        broker.client(),
        contextdb_core::TenantId::from(tenant),
    ));
    let holder_blob_service = BlobService::new(
        holder_db.clone(),
        MovementPolicy {
            auto_propagate: true,
        },
        holder_key.clone(),
    );
    holder_blob_service.set_test_clock(T0);
    // Wire the holder's own ledger-refresh hook to its ordinary sync pull:
    // the worker's claim lands on the hub via its own push, and this is
    // what lets the holder's entitlement check catch a claim that arrived
    // after this node's local mirror was last refreshed, without polling.
    let holder_client_for_refresh = holder_client.clone();
    holder_blob_service.set_claim_refresh(Some(Arc::new(move || {
        let holder_client_for_refresh = holder_client_for_refresh.clone();
        Box::pin(async move {
            let _ = holder_client_for_refresh.pull_default().await;
        })
    })));
    assert_eq!(
        holder_blob_service.ingest_bytes(&content).expect("ingest"),
        hash
    );
    // Binding + serving registers the holder's ticket in the peer
    // directory too, which rides the next push like any other coordination
    // row.
    let holder_endpoint = within(holder_blob_service.bind_and_serve_for_test(&holder_key))
        .await
        .expect("bind holder endpoint");

    submit_job(
        &holder_db,
        &blob_job("job-blob-01", &holder_node, &hash),
        &[] as &[&[u8]],
    )
    .expect("submit blob job");
    within(holder_client.push()).await.expect("holder push");

    // The worker: a different node, with its own blob service, wired into
    // its WorkerConfig — the new (today-inert) scaffold field.
    let worker_dir = tempfile::tempdir().expect("worker dir");
    let worker_key = identity_file(&worker_dir);
    let worker_node = node_id_of(&worker_key);

    let worker_db = Arc::new(Database::open_memory());
    install_work_ledger_schema(&worker_db).expect("worker ledger schema");
    let worker_client = SyncClient::with_transport(
        worker_db.clone(),
        broker.client(),
        contextdb_core::TenantId::from(tenant),
    );
    let worker_blob_service = Arc::new(BlobService::new(
        worker_db.clone(),
        MovementPolicy {
            auto_propagate: true,
        },
        worker_key,
    ));
    worker_blob_service.set_test_clock(T0);

    let config = WorkerConfig {
        node_id: worker_node.clone(),
        advertised_tags: Vec::new(),
        movement_policy: MovementPolicy {
            auto_propagate: true,
        },
        lease_duration_ms: LEASE,
        blob_service: Some(worker_blob_service),
        defer_own_submissions_until_deadline: false,
        writes_are_canonical: false,
    };
    let executor = ReceivedBytesExecutor {
        received: Mutex::new(None),
    };

    let outcome = within(poll_and_execute_once(
        &worker_client,
        &config,
        &executor,
        T0 + 1,
    ))
    .await
    .expect("poll must not error even when materialization is refused");

    assert_eq!(
        outcome,
        PollOutcome::Executed {
            job_id: "job-blob-01".to_string(),
            attempt: 1,
        },
        "a worker carrying a blob_service must resolve the blob_ref input and execute the job \
         end to end, not fail it at materialization"
    );
    assert_eq!(
        executor.received.lock().expect("received lock").as_deref(),
        Some(content.as_slice()),
        "the executor must receive the REAL resolved bytes fetched from the holder"
    );
    assert_eq!(
        job_state(&worker_db, "job-blob-01", T0 + 2).expect("state"),
        contextdb_engine::work_ledger::JobState::Done
    );

    within(worker_client.push())
        .await
        .expect("worker push result");
    assert_eq!(
        count_rows(&hub_db, "work_results"),
        1,
        "exactly one result row on the hub"
    );
    let result = job_result(&worker_db, "job-blob-01")
        .expect("read")
        .expect("result row");
    assert_eq!(result.output, content);

    // Sanity: the input really is a blob_ref (never a ledger_input silently
    // substituted), and it really is refused by materialize_inputs directly —
    // pinning down that the ONLY new path is the worker's blob resolution,
    // not a change to the engine's refusal.
    let direct = contextdb_engine::work_ledger::materialize_inputs(
        &worker_db,
        "job-blob-01",
        &worker_node,
        &MovementPolicy {
            auto_propagate: true,
        },
    );
    assert!(
        matches!(
            direct,
            Err(contextdb_core::Error::InputRequiresBlobResolver { .. })
        ),
        "materialize_inputs alone must still refuse a blob_ref input directly: {direct:?}"
    );

    shutdown.store(true, Ordering::SeqCst);
    let _ = task.await;
    holder_endpoint.close().await;
}
