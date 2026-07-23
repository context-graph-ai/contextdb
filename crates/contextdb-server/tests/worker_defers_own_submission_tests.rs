//! The distributed-compute self-submission gap: a node running a standing worker loop
//! must not race its OWN submissions against a deadline-bearing job it just
//! submitted — today `poll_and_execute_once` claims a matching job the
//! instant it is visible, regardless of who submitted it or whether a
//! deadline has passed, so a single-node deployment always executes its own
//! work immediately and never gets a chance to hand it to a less-loaded peer
//! first.

use contextdb_engine::work_ledger::{
    ExecutionInputs, InputRef, JobSnapshot, JobSpec, JobState, MovementPolicy,
    advertise_capability, job_state, submit_job,
};
use contextdb_server::InProcessBroker;
use contextdb_server::work_ledger::{
    ExecutionOutput, ExecutionVerdict, PollOutcome, WorkExecutor, WorkerConfig,
    poll_and_execute_once,
};
use std::sync::atomic::Ordering;
use std::time::Duration;

#[path = "work_ledger_support/mod.rs"]
mod work_ledger_support;
use work_ledger_support::*;

const DEADLINE_OFFSET: i64 = 2 * MINUTE;

async fn within<F: std::future::Future>(fut: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(30), fut)
        .await
        .expect("bounded defer-own-submission operation exceeded 30s")
}

fn deadline_job(job_id: &str, submitter: &str, deadline_ms: i64) -> JobSpec {
    JobSpec::builder(job_id, "wl.demo", "demo-transform", submitter)
        .requirement_tags(tags(&["class:wl.demo"]))
        .input_refs(vec![InputRef::ledger_input()])
        .priority(5)
        .deadline_ms(Some(deadline_ms))
        .submitted_at_ms(T0)
        .build()
}

fn deferring_worker_config(node_id: &str) -> WorkerConfig {
    WorkerConfig {
        node_id: node_id.to_string(),
        advertised_tags: tags(&["class:wl.demo"]),
        movement_policy: MovementPolicy {
            auto_propagate: true,
        },
        lease_duration_ms: LEASE,
        blob_store: None,
        defer_own_submissions_until_deadline: true,
        writes_are_canonical: false,
    }
}

struct DemoExecutor;

impl WorkExecutor for DemoExecutor {
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
        ExecutionVerdict::Completed(ExecutionOutput {
            output: all,
            receipt: serde_json::json!({ "backend": "test-double" }),
        })
    }
}

// ---------------------------------------------------------------------------
// A node running its own standing worker loop must not claim its own
// deadline-bearing submission before the deadline (leaving room for a peer
// to pick it up), but must claim and execute it once the deadline passes and
// no one else has.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn worker_defers_then_reclaims_own_submission_after_deadline() {
    let broker = InProcessBroker::new();
    let tenant = "wl-defer-01";
    let (hub_db, shutdown, task) = start_hub(&broker, tenant);

    let (a_db, a_client) = edge(&broker, tenant);
    advertise_capability(&a_db, "node-a", "wl.demo", &tags(&["class:wl.demo"]), T0)
        .expect("A advertises");

    let deadline_ms = T0 + DEADLINE_OFFSET;
    submit_job(
        &a_db,
        &deadline_job("job-defer-01", "node-a", deadline_ms),
        &[b"defer payload"],
    )
    .expect("submit own job");
    within(a_client.push()).await.expect("A push");

    // Before the deadline: node A must NOT claim its own submission — the
    // job must stay Pending, leaving a window for another node to pick it
    // up first.
    let before = within(poll_and_execute_once(
        &a_client,
        &deferring_worker_config("node-a"),
        &DemoExecutor,
        T0 + 1,
    ))
    .await
    .expect("A poll before deadline");
    assert_eq!(
        before,
        PollOutcome::NoMatchingWork,
        "a deferring worker must not claim its own deadline-bearing submission before the deadline"
    );
    assert_eq!(
        job_state(&a_db, "job-defer-01", T0 + 1).expect("state before deadline"),
        JobState::Pending,
        "the job must remain unclaimed before the deadline"
    );
    assert_eq!(
        count_rows(&hub_db, "work_claims"),
        0,
        "no claim may exist on the hub before the deadline"
    );

    // After the deadline: no one else claimed it, so node A must now claim
    // and execute its own submission.
    let after_deadline = deadline_ms + 1;
    let after = within(poll_and_execute_once(
        &a_client,
        &deferring_worker_config("node-a"),
        &DemoExecutor,
        after_deadline,
    ))
    .await
    .expect("A poll after deadline");
    assert_eq!(
        after,
        PollOutcome::Executed {
            job_id: "job-defer-01".to_string(),
            attempt: 1,
        },
        "once the deadline has passed, the deferring worker must claim and execute its own submission"
    );
    assert_eq!(
        job_state(&a_db, "job-defer-01", after_deadline + 1).expect("state after deadline"),
        JobState::Done
    );

    shutdown.store(true, Ordering::SeqCst);
    let _ = task.await;
}
