//! Work ledger — distributed-half contract tests, all over the in-process
//! transport fake: no Docker, no live endpoints. Every hub here is constructed
//! exactly the way the shipped binary constructs one — `uniform(LatestWins)` —
//! so these tests continuously prove the ledger's baked per-table policies make
//! arbitration correct regardless of how the hub is constructed.

use contextdb_core::Value;
use contextdb_engine::Database;
use contextdb_engine::work_ledger::{
    ExecutionInputs, InputRef, JobSnapshot, JobSpec, JobState, MovementPolicy,
    advertise_capability, advertised_tags, cancel_job, install_work_ledger_schema, job_result,
    job_state, submit_job,
};
use contextdb_server::work_ledger::{
    ClaimOutcome, ExecutionOutput, ExecutionVerdict, PollOutcome, WorkExecutor, WorkerConfig,
    claim_job, poll_and_execute_once, run_worker_loop,
};
use contextdb_server::{InProcessBroker, SyncClient};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

#[path = "work_ledger_support/mod.rs"]
mod work_ledger_support;
use work_ledger_support::*;

async fn within<F: std::future::Future>(fut: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(20), fut)
        .await
        .expect("bounded ledger operation exceeded 20s")
}

/// An edge whose transport presents `node_id` as its authenticated identity —
/// modelling the real sync path, where the hub reads the dialing endpoint's
/// authenticated peer id. Exercises the hub's per-node last-contact recording,
/// which is keyed on that transport identity (not on the changeset contents),
/// so a status-only exchange still attributes contact to the right node.
fn edge_as(broker: &InProcessBroker, tenant: &str, node_id: &str) -> (Arc<Database>, SyncClient) {
    let db = Arc::new(Database::open_memory());
    install_work_ledger_schema(&db).expect("install ledger schema on edge");
    let client = SyncClient::with_transport(
        db.clone(),
        broker.client_as(node_id),
        contextdb_core::TenantId::from(tenant),
    );
    (db, client)
}

fn demo_spec(job_id: &str, submitter: &str) -> JobSpec {
    JobSpec::builder(job_id, "wl.demo", "demo-transform", submitter)
        .requirement_tags(tags(&["class:wl.demo"]))
        .input_refs(vec![InputRef::ledger_input()])
        .priority(5)
        .submitted_at_ms(T0)
        .build()
}

fn worker_config(node_id: &str) -> WorkerConfig {
    WorkerConfig {
        node_id: node_id.to_string(),
        advertised_tags: tags(&["class:wl.demo"]),
        movement_policy: MovementPolicy {
            auto_propagate: true,
        },
        lease_duration_ms: LEASE,
        blob_store: None,
        defer_own_submissions_until_deadline: false,
        writes_are_canonical: false,
    }
}

fn hub_claim_holder(hub: &Database, job_id: &str, attempt: i64) -> Option<String> {
    let result = hub
        .execute(
            "SELECT node_id, job_id, attempt FROM work_claims",
            &HashMap::new(),
        )
        .expect("hub claim scan");
    let node_idx = result
        .columns
        .iter()
        .position(|c| c == "node_id")
        .expect("node_id col");
    let job_idx = result
        .columns
        .iter()
        .position(|c| c == "job_id")
        .expect("job_id col");
    let attempt_idx = result
        .columns
        .iter()
        .position(|c| c == "attempt")
        .expect("attempt col");
    result.rows.iter().find_map(|row| {
        let job_matches = matches!(&row[job_idx], Value::Text(j) if j == job_id);
        let attempt_matches = matches!(&row[attempt_idx], Value::Int64(a) if *a == attempt);
        if job_matches && attempt_matches {
            match &row[node_idx] {
                Value::Text(node) => Some(node.clone()),
                other => panic!("node_id must be TEXT, got {other:?}"),
            }
        } else {
            None
        }
    })
}

/// The demo executor: content-faithful and deterministic. It computes over
/// the REAL materialized bytes (never a rigged shape), so a wrong
/// materialization changes the output and fails the assertions.
struct DemoExecutor;

fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for b in bytes {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn demo_transform(inputs: &ExecutionInputs) -> Vec<u8> {
    let mut all = Vec::new();
    for (_, chunk) in inputs {
        all.extend_from_slice(chunk);
    }
    format!("len={};fnv={:016x}", all.len(), fnv1a(&all)).into_bytes()
}

impl WorkExecutor for DemoExecutor {
    fn execute(
        &self,
        _job: &JobSnapshot,
        inputs: &ExecutionInputs,
        _should_abandon: &dyn Fn() -> bool,
    ) -> ExecutionVerdict {
        ExecutionVerdict::Completed(ExecutionOutput {
            output: demo_transform(inputs),
            receipt: serde_json::json!({ "backend": "test-double", "elapsed_ms": 1 }),
        })
    }
}

struct FailingExecutor;

impl WorkExecutor for FailingExecutor {
    fn execute(
        &self,
        _job: &JobSnapshot,
        _inputs: &ExecutionInputs,
        _should_abandon: &dyn Fn() -> bool,
    ) -> ExecutionVerdict {
        ExecutionVerdict::Failed("simulated execution failure".to_string())
    }
}

// ---------------------------------------------------------------------------
// claim attribution from the push reply, with the claim riding in a
// push that carries other rows (criterion C3 — the load-bearing scenario)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn push_reply_attributes_the_claim_even_among_other_rows() {
    let broker = InProcessBroker::new();
    let tenant = "wl-s01";
    let (hub_db, shutdown, task) = start_hub(&broker, tenant);

    let (a_db, a_client) = edge(&broker, tenant);
    let (b_db, b_client) = edge(&broker, tenant);

    // The job is born on A alongside unrelated user rows, so A's claim never
    // travels alone.
    a_db.execute(
        "CREATE TABLE user_notes (id TEXT PRIMARY KEY, body TEXT)",
        &HashMap::new(),
    )
    .expect("user table on A");
    let mut note = HashMap::new();
    note.insert("id".to_string(), Value::Text("note-1".into()));
    note.insert("body".to_string(), Value::Text("rides along".into()));
    a_db.execute(
        "INSERT INTO user_notes (id, body) VALUES ($id, $body)",
        &note,
    )
    .expect("user row on A");
    submit_job(&a_db, &demo_spec("job-s01", "node-a"), &[b"payload"]).expect("submit on A");
    within(a_client.push()).await.expect("A push");

    // B pulls the job while NO claim exists anywhere: its later claim is a
    // genuine blind race that only the hub can settle.
    within(b_client.pull_default()).await.expect("B pull");

    let a_outcome = within(claim_job(&a_client, "job-s01", 1, "node-a", T0 + LEASE, T0))
        .await
        .expect("A claim");
    assert_eq!(
        a_outcome,
        ClaimOutcome::Won { synced: true },
        "a clean push reply means the hub filed A's claim"
    );

    // B loses the race for the same (job, attempt) — with B's own unrelated
    // rows riding in the same push.
    b_db.execute(
        "CREATE TABLE b_notes (id TEXT PRIMARY KEY, body TEXT)",
        &HashMap::new(),
    )
    .expect("user table on B");
    let mut b_note = HashMap::new();
    b_note.insert("id".to_string(), Value::Text("b-note-1".into()));
    b_note.insert("body".to_string(), Value::Text("also rides along".into()));
    b_db.execute(
        "INSERT INTO b_notes (id, body) VALUES ($id, $body)",
        &b_note,
    )
    .expect("user row on B");

    let b_outcome = within(claim_job(
        &b_client,
        "job-s01",
        1,
        "node-b",
        T0 + LEASE,
        T0 + 1,
    ))
    .await
    .expect("B claim");
    assert_eq!(
        b_outcome,
        ClaimOutcome::Lost {
            holder: Some("node-a".to_string())
        },
        "B must learn from its own push reply that its specific claim lost, and read back the winner"
    );

    // The hub holds exactly one claim row for (job-s01, 1): A's. B's other
    // rows still applied — losing a claim must not poison the rest of the push.
    assert_eq!(
        hub_claim_holder(&hub_db, "job-s01", 1),
        Some("node-a".to_string())
    );
    assert_eq!(count_rows(&hub_db, "work_claims"), 1);
    assert_eq!(
        count_rows(&hub_db, "b_notes"),
        1,
        "B's unrelated rows must land despite the lost claim"
    );

    // B's local losing claim row reconciles to the winner on pull.
    within(b_client.pull_default())
        .await
        .expect("B reconciling pull");
    let b_state = job_state(&b_db, "job-s01", T0 + 2).expect("B state");
    assert_eq!(
        b_state,
        JobState::Leased {
            node_id: "node-a".to_string(),
            attempt: 1,
            lease_deadline_ms: T0 + LEASE,
        },
        "after pull, B's durable ledger must name the true holder — a restarted B may not believe it holds the lease"
    );

    shutdown.store(true, Ordering::SeqCst);
    let _ = task.await;
}

// ---------------------------------------------------------------------------
// two workers race one job end to end: exactly one claim, one
// execution, one result (criterion C2)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn two_workers_race_exactly_one_wins_and_one_result_exists() {
    let broker = InProcessBroker::new();
    let tenant = "wl-s02";
    let (hub_db, shutdown, task) = start_hub(&broker, tenant);

    let (submitter_db, submitter_client) = edge(&broker, tenant);
    let (a_db, a_client) = edge(&broker, tenant);
    let (b_db, b_client) = edge(&broker, tenant);

    submit_job(
        &submitter_db,
        &demo_spec("job-s02", "node-sub"),
        &[b"race payload"],
    )
    .expect("submit");
    within(submitter_client.push())
        .await
        .expect("submitter push");

    advertise_capability(&a_db, "node-a", "wl.demo", &tags(&["class:wl.demo"]), T0)
        .expect("A advertises");
    advertise_capability(&b_db, "node-b", "wl.demo", &tags(&["class:wl.demo"]), T0)
        .expect("B advertises");

    let a_out = within(poll_and_execute_once(
        &a_client,
        &worker_config("node-a"),
        Arc::new(DemoExecutor),
        T0 + 1,
    ))
    .await
    .expect("A poll");
    assert_eq!(
        a_out,
        PollOutcome::Executed {
            job_id: "job-s02".to_string(),
            attempt: 1
        },
        "the first worker claims and executes"
    );

    let b_out = within(poll_and_execute_once(
        &b_client,
        &worker_config("node-b"),
        Arc::new(DemoExecutor),
        T0 + 2,
    ))
    .await
    .expect("B poll");
    assert_eq!(
        b_out,
        PollOutcome::NoMatchingWork,
        "the second worker sees the job leased/done after pulling and does not execute"
    );

    assert_eq!(
        count_rows(&hub_db, "work_results"),
        1,
        "exactly one result row on the hub"
    );
    assert_eq!(
        count_rows(&hub_db, "work_claims"),
        1,
        "exactly one claim row on the hub"
    );
    let result_holder = job_result(&a_db, "job-s02")
        .expect("A result")
        .expect("result row");
    assert_eq!(result_holder.executor_node_id, "node-a");
    assert_eq!(
        result_holder.output,
        demo_transform(&vec![(0, b"race payload".to_vec())])
    );

    shutdown.store(true, Ordering::SeqCst);
    let _ = task.await;
}

// ---------------------------------------------------------------------------
// a worker dies mid-job: the lease expires, attempt 2 succeeds, and
// the revived worker does not redo or double-write (criterion C2)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn lease_expiry_opens_attempt_two_and_the_revived_worker_stands_down() {
    let broker = InProcessBroker::new();
    let tenant = "wl-s03";
    let (hub_db, shutdown, task) = start_hub(&broker, tenant);

    let (submitter_db, submitter_client) = edge(&broker, tenant);
    let (a_db, a_client) = edge(&broker, tenant);
    let (b_db, b_client) = edge(&broker, tenant);

    submit_job(
        &submitter_db,
        &demo_spec("job-s03", "node-sub"),
        &[b"crash payload"],
    )
    .expect("submit");
    within(submitter_client.push())
        .await
        .expect("submitter push");

    // Worker A claims... and "crashes": it never executes, never records.
    within(a_client.pull_default()).await.expect("A pull");
    let a_claim = within(claim_job(&a_client, "job-s03", 1, "node-a", T0 + LEASE, T0))
        .await
        .expect("A claim");
    assert_eq!(a_claim, ClaimOutcome::Won { synced: true });

    // Before the lease passes, B sees the job leased and stays away.
    advertise_capability(&b_db, "node-b", "wl.demo", &tags(&["class:wl.demo"]), T0)
        .expect("B advertises");
    let b_early = within(poll_and_execute_once(
        &b_client,
        &worker_config("node-b"),
        Arc::new(DemoExecutor),
        T0 + 1,
    ))
    .await
    .expect("B early poll");
    assert_eq!(
        b_early,
        PollOutcome::NoMatchingWork,
        "a live lease blocks other claimants"
    );

    // After the deadline, B opens attempt 2 and completes the job.
    let after_expiry = T0 + LEASE + 1;
    let b_late = within(poll_and_execute_once(
        &b_client,
        &worker_config("node-b"),
        Arc::new(DemoExecutor),
        after_expiry,
    ))
    .await
    .expect("B late poll");
    assert_eq!(
        b_late,
        PollOutcome::Executed {
            job_id: "job-s03".to_string(),
            attempt: 2
        },
        "a passed lease deadline puts the job up for grabs as the next attempt"
    );
    assert_eq!(count_rows(&hub_db, "work_results"), 1);

    // The revived A pulls, finds the result, and stands down everywhere:
    // its poll finds nothing, and the exactly-once rules hold.
    let a_revived = within(poll_and_execute_once(
        &a_client,
        &worker_config("node-a"),
        Arc::new(DemoExecutor),
        after_expiry + 1,
    ))
    .await
    .expect("A revived poll");
    assert_eq!(
        a_revived,
        PollOutcome::NoMatchingWork,
        "the revived worker must not redo done work"
    );
    assert_eq!(
        count_rows(&hub_db, "work_results"),
        1,
        "still exactly one result row"
    );
    let result = job_result(&a_db, "job-s03")
        .expect("A result")
        .expect("result");
    assert_eq!(result.executor_node_id, "node-b");
    assert_eq!(result.attempt, 2);

    shutdown.store(true, Ordering::SeqCst);
    let _ = task.await;
}

// ---------------------------------------------------------------------------
// failures accumulate to the ceiling and go terminal on every node
// (criterion C2)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn failure_rows_reach_the_ceiling_and_the_job_is_terminal_everywhere() {
    let broker = InProcessBroker::new();
    let tenant = "wl-s04";
    let (hub_db, shutdown, task) = start_hub(&broker, tenant);

    let (submitter_db, submitter_client) = edge(&broker, tenant);
    let (b_db, b_client) = edge(&broker, tenant);

    // max_attempts = 2 (from demo_spec).
    submit_job(
        &submitter_db,
        &demo_spec("job-s04", "node-sub"),
        &[b"doomed payload"],
    )
    .expect("submit");
    within(submitter_client.push())
        .await
        .expect("submitter push");

    advertise_capability(&b_db, "node-b", "wl.demo", &tags(&["class:wl.demo"]), T0)
        .expect("B advertises");

    let first = within(poll_and_execute_once(
        &b_client,
        &worker_config("node-b"),
        Arc::new(FailingExecutor),
        T0 + 1,
    ))
    .await
    .expect("first failing poll");
    assert_eq!(
        first,
        PollOutcome::Failed {
            job_id: "job-s04".to_string(),
            attempt: 1
        }
    );

    // The failure row makes attempt 2 legal immediately (no lease wait).
    let second = within(poll_and_execute_once(
        &b_client,
        &worker_config("node-b"),
        Arc::new(FailingExecutor),
        T0 + 2,
    ))
    .await
    .expect("second failing poll");
    assert_eq!(
        second,
        PollOutcome::Failed {
            job_id: "job-s04".to_string(),
            attempt: 2
        }
    );

    assert_eq!(
        count_rows(&hub_db, "work_failures"),
        2,
        "one failure row per attempt on the hub"
    );

    // Terminal: no third attempt is ever opened.
    let third = within(poll_and_execute_once(
        &b_client,
        &worker_config("node-b"),
        Arc::new(FailingExecutor),
        T0 + 3,
    ))
    .await
    .expect("post-terminal poll");
    assert_eq!(
        third,
        PollOutcome::NoMatchingWork,
        "failed is terminal — never silently retried"
    );

    // The submitter sees the terminal state after pulling.
    within(submitter_client.pull_default())
        .await
        .expect("submitter pull");
    assert_eq!(
        job_state(&submitter_db, "job-s04", T0 + 4).expect("state"),
        JobState::Failed
    );

    shutdown.store(true, Ordering::SeqCst);
    let _ = task.await;
}

// ---------------------------------------------------------------------------
// cancellation stops a long job between chunks; a cancellation that
// loses to the result is a no-op (criterion C2)
// ---------------------------------------------------------------------------

/// Executes chunk by chunk and consults the abandonment check between
/// chunks — after the first chunk, the test's cancellation (arriving in the
/// worker's own store, as a synced-in row would) must stop it.
struct ChunkedCancellableExecutor {
    db: Arc<Database>,
    chunks_processed: AtomicUsize,
}

impl WorkExecutor for ChunkedCancellableExecutor {
    fn execute(
        &self,
        job: &JobSnapshot,
        inputs: &ExecutionInputs,
        should_abandon: &dyn Fn() -> bool,
    ) -> ExecutionVerdict {
        for (i, _chunk) in inputs.iter().enumerate() {
            if should_abandon() {
                return ExecutionVerdict::Abandoned;
            }
            self.chunks_processed.fetch_add(1, Ordering::SeqCst);
            if i == 0 {
                // The cancellation lands while the job is mid-flight.
                cancel_job(
                    &self.db,
                    &job.job_id,
                    "node-sub",
                    Some("changed my mind"),
                    T0 + 10,
                )
                .expect("cancel mid-execution");
            }
        }
        ExecutionVerdict::Completed(ExecutionOutput {
            output: b"should never complete".to_vec(),
            receipt: serde_json::json!({}),
        })
    }
}

#[tokio::test]
async fn cancellation_stops_between_chunks_and_loses_to_a_result() {
    let broker = InProcessBroker::new();
    let tenant = "wl-s05";
    let (hub_db, shutdown, task) = start_hub(&broker, tenant);

    let (submitter_db, submitter_client) = edge(&broker, tenant);
    let (b_db, b_client) = edge(&broker, tenant);

    submit_job(
        &submitter_db,
        &demo_spec("job-s05", "node-sub"),
        &[b"chunk-0", b"chunk-1", b"chunk-2"],
    )
    .expect("submit");
    within(submitter_client.push())
        .await
        .expect("submitter push");

    advertise_capability(&b_db, "node-b", "wl.demo", &tags(&["class:wl.demo"]), T0)
        .expect("B advertises");
    let executor = Arc::new(ChunkedCancellableExecutor {
        db: b_db.clone(),
        chunks_processed: AtomicUsize::new(0),
    });
    let outcome = within(poll_and_execute_once(
        &b_client,
        &worker_config("node-b"),
        executor.clone(),
        T0 + 1,
    ))
    .await
    .expect("cancelled poll");
    assert_eq!(
        outcome,
        PollOutcome::Abandoned {
            job_id: "job-s05".to_string()
        },
        "the worker must observe the cancellation between chunks and stop"
    );
    assert_eq!(
        executor.chunks_processed.load(Ordering::SeqCst),
        1,
        "exactly one chunk ran before the cancellation was observed"
    );
    assert!(
        job_result(&b_db, "job-s05").expect("read").is_none(),
        "an abandoned execution records no result"
    );
    assert_eq!(
        job_state(&b_db, "job-s05", T0 + 11).expect("state"),
        JobState::Cancelled
    );

    // And the reverse race: a result that lands first beats a late
    // cancellation — proven in the engine suite (wl_e03); here we prove the
    // cancellation row syncs so every node computes Cancelled.
    within(b_client.push()).await.expect("B push");
    within(submitter_client.pull_default())
        .await
        .expect("submitter pull");
    assert_eq!(
        job_state(&submitter_db, "job-s05", T0 + 12).expect("state"),
        JobState::Cancelled,
        "the cancellation row rides sync like any other row"
    );
    assert_eq!(count_rows(&hub_db, "work_results"), 0);

    shutdown.store(true, Ordering::SeqCst);
    let _ = task.await;
}

// ---------------------------------------------------------------------------
// the product moment, generic form: submitted on one node, executed
// on another, result + receipt visible back on the submitter (criterion C1)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn submit_on_a_execute_on_b_result_and_receipt_apply_back_on_a() {
    let broker = InProcessBroker::new();
    let tenant = "wl-s06";
    let (_hub_db, shutdown, task) = start_hub(&broker, tenant);

    let (a_db, a_client) = edge(&broker, tenant);
    let (b_db, b_client) = edge(&broker, tenant);

    let chunk_a: Vec<u8> = vec![1, 2, 3, 250, 251];
    let chunk_b: Vec<u8> = b"second chunk".to_vec();
    submit_job(
        &a_db,
        &demo_spec("job-s06", "node-a"),
        &[&chunk_a, &chunk_b],
    )
    .expect("submit on A");
    within(a_client.push()).await.expect("A push");

    advertise_capability(&b_db, "node-b", "wl.demo", &tags(&["class:wl.demo"]), T0)
        .expect("B advertises");
    let outcome = within(poll_and_execute_once(
        &b_client,
        &worker_config("node-b"),
        Arc::new(DemoExecutor),
        T0 + 1,
    ))
    .await
    .expect("B poll");
    assert_eq!(
        outcome,
        PollOutcome::Executed {
            job_id: "job-s06".to_string(),
            attempt: 1
        }
    );

    // A pulls: done, executed by B, with the receipt — A never said where
    // the work would run.
    within(a_client.pull_default()).await.expect("A pull");
    assert_eq!(
        job_state(&a_db, "job-s06", T0 + 2).expect("state"),
        JobState::Done
    );
    let result = job_result(&a_db, "job-s06")
        .expect("read")
        .expect("result row");
    assert_eq!(result.executor_node_id, "node-b");
    let expected = demo_transform(&vec![(0, chunk_a.clone()), (1, chunk_b.clone())]);
    assert_eq!(
        result.output, expected,
        "the output must be computed from the real materialized bytes, in order"
    );
    assert_eq!(result.receipt["backend"], serde_json::json!("test-double"));

    // Idempotent re-delivery: pulling again changes nothing.
    within(a_client.pull_default())
        .await
        .expect("A second pull");
    assert_eq!(count_rows(&a_db, "work_results"), 1);
    assert_eq!(
        job_result(&a_db, "job-s06")
            .expect("read")
            .expect("row")
            .output,
        expected
    );

    shutdown.store(true, Ordering::SeqCst);
    let _ = task.await;
}

// ---------------------------------------------------------------------------
// the offline mode: a disconnected node drains its own job, then
// reconciles on reconnect with exactly-once effects (criterion C2)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn offline_node_drains_its_own_job_and_reconciles_on_reconnect() {
    let broker = InProcessBroker::new();
    let tenant = "wl-s07";

    // No hub is running yet: every push/pull fails as a transport miss.
    let (a_db, a_client) = edge(&broker, tenant);
    advertise_capability(&a_db, "node-a", "wl.demo", &tags(&["class:wl.demo"]), T0)
        .expect("A advertises");

    // The offline claim contract, on its own job: the local insert IS the
    // lease when the hub is unreachable.
    submit_job(
        &a_db,
        &demo_spec("job-s07-claimed", "node-a"),
        &[b"claim probe"],
    )
    .expect("submit probe");
    let claim = within(claim_job(
        &a_client,
        "job-s07-claimed",
        1,
        "node-a",
        T0 + LEASE,
        T0,
    ))
    .await
    .expect("offline claim");
    assert_eq!(
        claim,
        ClaimOutcome::Won { synced: false },
        "with the hub unreachable the claim is held locally — the offline mode"
    );

    // The offline drain: the full poll pass claims, materializes, executes,
    // and records — every hub round-trip tolerated as unreachable.
    submit_job(
        &a_db,
        &demo_spec("job-s07", "node-a"),
        &[b"offline payload"],
    )
    .expect("submit offline");
    let outcome = within(poll_and_execute_once(
        &a_client,
        &worker_config("node-a"),
        Arc::new(DemoExecutor),
        T0 + 1,
    ))
    .await
    .expect("offline poll");
    assert_eq!(
        outcome,
        PollOutcome::Executed {
            job_id: "job-s07".to_string(),
            attempt: 1
        },
        "an offline node drains its own submission entirely locally"
    );
    assert_eq!(
        job_state(&a_db, "job-s07", T0 + 2).expect("state"),
        JobState::Done
    );

    // The hub comes up; reconnecting pushes the whole story.
    let (hub_db, shutdown, task) = start_hub(&broker, tenant);
    a_client.reconnect().await;
    within(a_client.push()).await.expect("reconnect push");
    assert_eq!(
        count_rows(&hub_db, "work_results"),
        1,
        "exactly one result after reconnect"
    );
    assert_eq!(
        count_rows(&hub_db, "work_jobs"),
        2,
        "both offline-born jobs converge"
    );
    assert_eq!(
        hub_claim_holder(&hub_db, "job-s07-claimed", 1),
        Some("node-a".to_string()),
        "the locally-held claim enters hub arbitration on reconnect"
    );

    // A second edge pulls the whole converged story.
    let (c_db, c_client) = edge(&broker, tenant);
    within(c_client.pull_default()).await.expect("C pull");
    assert_eq!(
        job_state(&c_db, "job-s07", T0 + 3).expect("state"),
        JobState::Done
    );
    assert_eq!(
        job_result(&c_db, "job-s07")
            .expect("read")
            .expect("row")
            .executor_node_id,
        "node-a"
    );

    shutdown.store(true, Ordering::SeqCst);
    let _ = task.await;
}

// ---------------------------------------------------------------------------
// the movement policy withholds foreign-input jobs at claim time
// (criterion C9)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn movement_policy_withholds_foreign_jobs_from_a_closed_worker() {
    let broker = InProcessBroker::new();
    let tenant = "wl-s08";
    let (_hub_db, shutdown, task) = start_hub(&broker, tenant);

    let (a_db, a_client) = edge(&broker, tenant);
    let (b_db, b_client) = edge(&broker, tenant);

    submit_job(
        &a_db,
        &demo_spec("job-s08", "node-a"),
        &[b"private-ish payload"],
    )
    .expect("submit on A");
    within(a_client.push()).await.expect("A push");

    advertise_capability(&b_db, "node-b", "wl.demo", &tags(&["class:wl.demo"]), T0)
        .expect("B advertises");

    let mut closed = worker_config("node-b");
    closed.movement_policy = MovementPolicy {
        auto_propagate: false,
    };
    let withheld = within(poll_and_execute_once(
        &b_client,
        &closed,
        Arc::new(DemoExecutor),
        T0 + 1,
    ))
    .await
    .expect("closed poll");
    assert_eq!(
        withheld,
        PollOutcome::NoMatchingWork,
        "a closed movement policy must withhold another node's ledger-input job"
    );
    assert!(
        job_result(&b_db, "job-s08").expect("read").is_none(),
        "nothing may execute under the closed policy"
    );

    // Flipping the knob is what turns on distributed acceleration.
    let open = worker_config("node-b");
    let executed = within(poll_and_execute_once(
        &b_client,
        &open,
        Arc::new(DemoExecutor),
        T0 + 2,
    ))
    .await
    .expect("open poll");
    assert_eq!(
        executed,
        PollOutcome::Executed {
            job_id: "job-s08".to_string(),
            attempt: 1
        }
    );

    shutdown.store(true, Ordering::SeqCst);
    let _ = task.await;
}

// ---------------------------------------------------------------------------
// the standing worker loop: a thin wall-clock wrapper that polls
// until shutdown (the library helper the CLI/examples call)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn worker_loop_polls_until_shutdown_and_executes_arriving_work() {
    let broker = InProcessBroker::new();
    let tenant = "wl-s10";
    let (hub_db, shutdown, task) = start_hub(&broker, tenant);

    let (a_db, a_client) = edge(&broker, tenant);
    let (b_db, b_client) = edge(&broker, tenant);
    advertise_capability(&b_db, "node-b", "wl.demo", &tags(&["class:wl.demo"]), T0)
        .expect("B advertises");

    let loop_shutdown = Arc::new(AtomicBool::new(false));
    let loop_task = tokio::spawn({
        let shutdown = loop_shutdown.clone();
        async move {
            run_worker_loop(
                &b_client,
                &worker_config("node-b"),
                Arc::new(DemoExecutor),
                Duration::from_millis(10),
                shutdown,
            )
            .await
        }
    });

    // Work arrives while the loop is already running.
    submit_job(&a_db, &demo_spec("job-s10", "node-a"), &[b"loop payload"]).expect("submit on A");
    within(a_client.push()).await.expect("A push");

    // The loop picks it up without any external nudge.
    within(async {
        loop {
            if count_rows(&hub_db, "work_results") == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;

    loop_shutdown.store(true, Ordering::SeqCst);
    within(loop_task)
        .await
        .expect("join loop")
        .expect("loop exits cleanly");

    within(a_client.pull_default()).await.expect("A pull");
    let result = job_result(&a_db, "job-s10").expect("read").expect("row");
    assert_eq!(result.executor_node_id, "node-b");

    shutdown.store(true, Ordering::SeqCst);
    let _ = task.await;
}

fn hub_capability_row_count(hub: &Database, node_id: &str) -> usize {
    // Before the hub has ever applied a synced changeset the table itself
    // does not exist yet (schema rides the first successful push, same as
    // any other work-ledger table) — that is "zero rows", not an error.
    let result = match hub.execute("SELECT node_id FROM work_capabilities", &HashMap::new()) {
        Ok(result) => result,
        Err(_) => return 0,
    };
    let node_idx = result
        .columns
        .iter()
        .position(|c| c == "node_id")
        .expect("node_id col");
    result
        .rows
        .iter()
        .filter(|row| matches!(&row[node_idx], Value::Text(n) if n == node_id))
        .count()
}

/// The worker loop advertises its capability and pushes exactly once, before
/// entering the poll loop. `SyncClient::push` itself already retries a
/// transient miss (5 attempts, backing off up to ~5s total) — that internal
/// retry is NOT the contract under test here. This test holds the hub down
/// for LONGER than that internal budget so the startup push exhausts its own
/// retries and returns an `Err` that `run_worker_loop` discards
/// (`let _ = client.push().await;`). Once the hub finally comes up, nothing
/// in the poll loop ever re-pushes the outstanding capability (the poll loop
/// only pushes as a side effect of claiming work, and there is no work
/// here) — so a hub that is merely running late, rather than permanently
/// gone, must still end up with the worker's capability row.
#[tokio::test(start_paused = true)]
async fn worker_repushes_capabilities_until_hub_receives_them() {
    let broker = InProcessBroker::new();
    let tenant = "wl-s11";

    // The worker starts before the hub does: no routes are registered on the
    // broker yet, so the loop's startup push fails with NoResponder on every
    // one of its 5 internal attempts.
    let (_w_db, w_client) = edge(&broker, tenant);

    let loop_shutdown = Arc::new(AtomicBool::new(false));
    let loop_task = tokio::spawn({
        let shutdown = loop_shutdown.clone();
        async move {
            run_worker_loop(
                &w_client,
                &worker_config("node-w"),
                Arc::new(DemoExecutor),
                Duration::from_millis(20),
                shutdown,
            )
            .await
        }
    });

    // Outlast the startup push's own retry budget (5 attempts, backing off
    // 0/500/1000/1500/2000ms = 5s) plus slack, so the swallowed-Err path is
    // the one under test, not the built-in single-call retry. The runtime is
    // paused (virtual time): this and every internal backoff auto-advance
    // instantly and DETERMINISTICALLY — ordering guaranteed, zero wall-clock.
    tokio::time::sleep(Duration::from_millis(6_500)).await;

    // The hub comes up late.
    let (hub_db, shutdown, task) = start_hub(&broker, tenant);

    // The loop must eventually re-push the outstanding capability now that
    // the hub can receive it — bounded wait over many poll cycles (the loop
    // polls every 20ms), not a single fixed sleep.
    within(async {
        loop {
            if hub_capability_row_count(&hub_db, "node-w") >= 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;

    loop_shutdown.store(true, Ordering::SeqCst);
    let _ = loop_task.await;
    shutdown.store(true, Ordering::SeqCst);
    let _ = task.await;
}

// ---------------------------------------------------------------------------
// Capability currency + liveness over a real sync pair. A node's CURRENT
// detector capability is its LATEST-advertised row, and that refresh must
// PROPAGATE to the hub; and the hub must record a hub-local last-contact per
// node so a dead worker can be aged out.
// ---------------------------------------------------------------------------

fn hub_capability_advertised_at(hub: &Database, node_id: &str, capability_id: &str) -> Option<i64> {
    let key = format!("{node_id}#{capability_id}");
    let mut params = HashMap::new();
    params.insert("capability_key".to_string(), Value::Text(key));
    let result = hub
        .execute(
            "SELECT advertised_at FROM work_capabilities WHERE capability_key = $capability_key",
            &params,
        )
        .ok()?;
    let idx = result.columns.iter().position(|c| c == "advertised_at")?;
    match result.rows.first()?.get(idx)? {
        Value::Timestamp(ms) => Some(*ms),
        _ => None,
    }
}

/// The hub-local last-contact timestamp for `node_id`, in ms. ASSUMED KNOB:
/// the fix records this on every sync exchange the hub serves, in a hub-local
/// `work_node_contacts(node_id, last_contact_ms)` row that never syncs to
/// edges (`SyncDirection::None`). Returns `None` when no record exists yet
/// (today: the table/record does not exist at all) — mirrors
/// `hub_capability_row_count`'s "no table yet is zero, not an error".
fn hub_last_contact_ms(hub: &Database, node_id: &str) -> Option<i64> {
    let mut params = HashMap::new();
    params.insert("node_id".to_string(), Value::Text(node_id.to_string()));
    let result = hub
        .execute(
            "SELECT last_contact_ms FROM work_node_contacts WHERE node_id = $node_id",
            &params,
        )
        .ok()?;
    let idx = result.columns.iter().position(|c| c == "last_contact_ms")?;
    match result.rows.first()?.get(idx)? {
        Value::Timestamp(ms) => Some(*ms),
        Value::Int64(ms) => Some(*ms),
        _ => None,
    }
}

/// A worker advertises a capability, syncs, then re-advertises the SAME
/// (node, capability) key later and syncs again — the hub's row must converge
/// to the NEWER advertised_at. Fails today: the local re-advertise is a
/// write-once no-op AND `work_capabilities` syncs `InsertIfNotExists`, so the
/// refresh never reaches the hub (it stays at the first advertised_at).
#[tokio::test]
async fn re_advertised_capability_currency_propagates_to_the_hub() {
    let broker = InProcessBroker::new();
    let tenant = "wl-s14";
    let (hub_db, shutdown, task) = start_hub(&broker, tenant);

    let (w_db, w_client) = edge(&broker, tenant);

    advertise_capability(&w_db, "node-w", "wl.demo", &tags(&["class:wl.demo"]), T0)
        .expect("first advertise");
    within(w_client.push()).await.expect("first push");
    assert_eq!(
        hub_capability_advertised_at(&hub_db, "node-w", "wl.demo"),
        Some(T0),
        "the hub receives the first advertisement at T0"
    );

    // The worker is restarted / promoted and re-advertises the same key later.
    advertise_capability(
        &w_db,
        "node-w",
        "wl.demo",
        &tags(&["class:wl.demo"]),
        T0 + MINUTE,
    )
    .expect("re-advertise same key later");
    within(w_client.push()).await.expect("second push");

    assert_eq!(
        hub_capability_advertised_at(&hub_db, "node-w", "wl.demo"),
        Some(T0 + MINUTE),
        "the hub's capability row must converge to the newer advertised_at"
    );

    shutdown.store(true, Ordering::SeqCst);
    let _ = task.await;
}

/// The hub records a hub-local last-contact per node on every sync exchange
/// it serves, and that timestamp advances across exchanges. Fails today: no
/// such record exists.
#[tokio::test]
async fn hub_records_last_contact_per_node_and_it_advances() {
    let broker = InProcessBroker::new();
    let tenant = "wl-s15";
    let (hub_db, shutdown, task) = start_hub(&broker, tenant);

    // The edge presents "node-w" as its transport-authenticated identity, so
    // the hub attributes every exchange it serves — including the status-only
    // second push below — to node-w.
    let (w_db, w_client) = edge_as(&broker, tenant, "node-w");

    advertise_capability(&w_db, "node-w", "wl.demo", &tags(&["class:wl.demo"]), T0)
        .expect("advertise");
    within(w_client.push()).await.expect("first exchange");

    let first = hub_last_contact_ms(&hub_db, "node-w");
    assert!(
        first.is_some(),
        "the hub must record a last-contact for node-w after it serves an exchange; got {first:?}"
    );

    // A later exchange must advance the recorded last-contact. The hub stamps
    // real wall-clock millis, so poll bounded on the CONDITION (a later
    // exchange with a strictly newer stamp) instead of betting one fixed sleep
    // covers the clock's granularity.
    let mut second = None;
    within(async {
        loop {
            within(w_client.push()).await.expect("later exchange");
            second = hub_last_contact_ms(&hub_db, "node-w");
            if matches!((first, second), (Some(a), Some(b)) if b > a) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await;
    assert!(
        matches!((first, second), (Some(a), Some(b)) if b > a),
        "a later exchange must advance the recorded last-contact; first={first:?} second={second:?}"
    );

    shutdown.store(true, Ordering::SeqCst);
    let _ = task.await;
}

// ---------------------------------------------------------------------------
// Vocabulary guard: the ledger's public surface exports what downstream
// consumers (the understanding and detector work-class plans) build on.
// ---------------------------------------------------------------------------

#[test]
fn advertised_tags_survive_a_round_trip_through_the_registry() {
    let db = Database::open_memory();
    install_work_ledger_schema(&db).expect("install");
    advertise_capability(
        &db,
        "node-x",
        "wl.demo",
        &tags(&["class:wl.demo", "output:hex", "ref:ledger_input"]),
        T0,
    )
    .expect("advertise");
    let mut round_trip = advertised_tags(&db, "node-x").expect("tags");
    round_trip.sort();
    assert_eq!(
        round_trip,
        tags(&["class:wl.demo", "output:hex", "ref:ledger_input"])
    );
}

// ---------------------------------------------------------------------------
// a re-delivered own claim must never read as a loss (criterion C3,
// the verify-by-pull arm, previously unasserted)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn redelivered_own_claim_reads_as_won_not_lost() {
    let broker = InProcessBroker::new();
    let tenant = "wl-s12";
    let (hub_db, shutdown, task) = start_hub(&broker, tenant);

    let (a_db, a_client) = edge(&broker, tenant);
    submit_job(&a_db, &demo_spec("job-s12", "node-a"), &[b"payload"]).expect("submit");
    let first = within(claim_job(&a_client, "job-s12", 1, "node-a", T0 + LEASE, T0))
        .await
        .expect("first claim");
    assert_eq!(first, ClaimOutcome::Won { synced: true });

    // The same node loses its local store (a wiped disk) and re-creates the
    // job and its claim from scratch BEFORE ever pulling: a fresh store
    // under the SAME node identity re-inserts the same claim key and pushes.
    // The hub already holds that key, so the push reply carries the refusal
    // — and the verify-by-pull must recognize the node as the standing
    // holder and conclude Won, never Lost.
    let (b_db, b_client) = edge(&broker, tenant);
    submit_job(&b_db, &demo_spec("job-s12", "node-a"), &[b"payload"]).expect("re-created job");
    let redelivered = within(claim_job(
        &b_client,
        "job-s12",
        1,
        "node-a",
        T0 + LEASE,
        T0 + 1,
    ))
    .await
    .expect("re-delivered claim");
    assert_eq!(
        redelivered,
        ClaimOutcome::Won { synced: true },
        "a refused re-delivery of the node's own winning claim must read as Won"
    );
    assert_eq!(count_rows(&hub_db, "work_claims"), 1);
    assert_eq!(
        hub_claim_holder(&hub_db, "job-s12", 1),
        Some("node-a".to_string())
    );

    shutdown.store(true, Ordering::SeqCst);
    let _ = task.await;
}

// ---------------------------------------------------------------------------
// a REMOTE cancellation (from the submitter, through the hub) stops
// a long-running worker between chunks. Regression guard: the between-chunks
// check previously read only the worker's local store and the worker never
// pulled while executing, so a cancellation synced from another machine could
// not stop the job. the between-chunks cancellation test covers the
// local-write case; THIS test drives the real cross-machine path.
// Multi-thread flavor: the executor blocks its task's thread while the hub,
// the submitter's cancel, and the worker's in-flight pulls must keep moving.
// ---------------------------------------------------------------------------

/// Chunk-by-chunk executor that NEVER writes any row itself: after the first
/// chunk it hands control to the test (which cancels remotely) and proceeds
/// only when told — then the between-chunks check must observe the synced-in
/// cancellation.
struct RemoteCancelProbeExecutor {
    reached_chunk_zero: std::sync::mpsc::Sender<()>,
    proceed: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
    chunks_processed: AtomicUsize,
}

impl WorkExecutor for RemoteCancelProbeExecutor {
    fn execute(
        &self,
        _job: &JobSnapshot,
        inputs: &ExecutionInputs,
        should_abandon: &dyn Fn() -> bool,
    ) -> ExecutionVerdict {
        for (i, _chunk) in inputs.iter().enumerate() {
            if should_abandon() {
                return ExecutionVerdict::Abandoned;
            }
            self.chunks_processed.fetch_add(1, Ordering::SeqCst);
            if i == 0 {
                self.reached_chunk_zero.send(()).expect("signal chunk zero");
                // A plain std recv with a bound: it blocks THIS OS thread
                // only (what a long executor does anyway), and the bound
                // guarantees a clean test failure instead of a runtime-
                // shutdown deadlock when the control side gives up.
                if self
                    .proceed
                    .lock()
                    .expect("proceed receiver")
                    .recv_timeout(Duration::from_secs(15))
                    .is_err()
                {
                    return ExecutionVerdict::Failed(
                        "proceed signal never came (control gave up)".to_string(),
                    );
                }
            }
        }
        ExecutionVerdict::Completed(ExecutionOutput {
            output: b"must never complete".to_vec(),
            receipt: serde_json::json!({}),
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remote_cancellation_stops_the_worker_between_chunks() {
    let broker = InProcessBroker::new();
    let tenant = "wl-s13";
    let (hub_db, shutdown, task) = start_hub(&broker, tenant);

    let (a_db, a_client) = edge(&broker, tenant);
    let (b_db, b_client) = edge(&broker, tenant);

    submit_job(
        &a_db,
        &demo_spec("job-s13", "node-a"),
        &[b"chunk-0", b"chunk-1", b"chunk-2"],
    )
    .expect("submit");
    within(a_client.push()).await.expect("A push");

    advertise_capability(&b_db, "node-b", "wl.demo", &tags(&["class:wl.demo"]), T0)
        .expect("B advertises");

    let (reached_tx, reached_rx) = std::sync::mpsc::channel();
    let (proceed_tx, proceed_rx) = std::sync::mpsc::channel();
    let executor = Arc::new(RemoteCancelProbeExecutor {
        reached_chunk_zero: reached_tx,
        proceed: std::sync::Mutex::new(proceed_rx),
        chunks_processed: AtomicUsize::new(0),
    });

    let config = worker_config("node-b");
    let poll = poll_and_execute_once(&b_client, &config, executor.clone(), T0 + 1);
    let control = async {
        within(async {
            loop {
                match reached_rx.try_recv() {
                    Ok(()) => break,
                    Err(std::sync::mpsc::TryRecvError::Empty) => {
                        tokio::time::sleep(Duration::from_millis(50)).await
                    }
                    Err(err) => panic!("worker never reached chunk zero: {err}"),
                }
            }
        })
        .await;
        // The submitter cancels while the job is mid-flight — on ITS machine,
        // through the hub. The worker's own store knows nothing yet.
        cancel_job(&a_db, "job-s13", "node-a", Some("remote cancel"), T0 + 5).expect("cancel on A");
        within(a_client.push())
            .await
            .expect("A pushes the cancellation");
        // Wait until the cancellation has genuinely ARRIVED in B's store (the
        // worker must be pulling while it executes), then let it proceed to
        // the between-chunks check.
        within(async {
            loop {
                if job_state(&b_db, "job-s13", T0 + 6).expect("B state") == JobState::Cancelled {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await;
        proceed_tx.send(()).expect("proceed");
    };
    let (outcome, ()) = tokio::join!(poll, control);
    assert_eq!(
        outcome.expect("poll"),
        PollOutcome::Abandoned {
            job_id: "job-s13".to_string()
        },
        "the between-chunks check must observe a REMOTE cancellation and stop the worker"
    );
    assert_eq!(
        executor.chunks_processed.load(Ordering::SeqCst),
        1,
        "no chunk may run after the remote cancellation was observable"
    );
    assert!(
        job_result(&b_db, "job-s13").expect("read").is_none(),
        "an abandoned execution records no result"
    );
    assert_eq!(count_rows(&hub_db, "work_results"), 0);

    shutdown.store(true, Ordering::SeqCst);
    let _ = task.await;
}

/// Operator honesty on the ledger-owning node: a
/// worker whose `writes_are_canonical` is set hosts the hub over its OWN db —
/// its rows are already in the canonical store, so the worker loop must not
/// push at all: not the startup capability push, not the `NoMatchingWork`
/// re-push. Observable here as the broker-side hub db NEVER receiving the
/// worker-loop capability row, while a plain edge (the control) delivers it.
/// The edge's loud re-push-until-received delivery contract is pinned
/// elsewhere (`worker_repushes_capabilities_until_hub_receives_them`) and
/// must stay untouched by this gate.
#[tokio::test(start_paused = true)]
async fn canonical_worker_never_pushes_its_advertisement() {
    let broker = InProcessBroker::new();
    let (hub_db, shutdown, task) = start_hub(&broker, "t-canon");

    let (_edge_db, client) = edge_as(&broker, "t-canon", "canon-node");
    let config = WorkerConfig {
        node_id: "canon-node".to_string(),
        advertised_tags: vec!["class:test".to_string()],
        movement_policy: MovementPolicy {
            auto_propagate: true,
        },
        lease_duration_ms: 60_000,
        blob_store: None,
        defer_own_submissions_until_deadline: false,
        writes_are_canonical: true,
    };
    let executor: Arc<dyn WorkExecutor> = Arc::new(DemoExecutor);
    let worker_shutdown = Arc::new(AtomicBool::new(false));
    let loop_task = {
        let worker_shutdown = worker_shutdown.clone();
        async move {
            let _ = run_worker_loop(
                &client,
                &config,
                executor,
                Duration::from_millis(50),
                worker_shutdown,
            )
            .await;
        }
    };
    let handle = tokio::spawn(loop_task);
    // Paused virtual time: 600 ms advances instantly but still guarantees
    // >= 12 deterministic 50 ms poll cycles ran before the negative below —
    // a stronger "never pushes" witness than a wall-clock window.
    tokio::time::sleep(Duration::from_millis(600)).await;
    worker_shutdown.store(true, Ordering::SeqCst);
    let _ = handle.await;

    let received = hub_capability_row_count(&hub_db, "canon-node");
    assert!(
        received == 0,
        "a canonical worker's advertisement must never be PUSHED to a broker \
         hub — its own db is the canonical store; hub received {received} \
         capability row(s)"
    );

    shutdown.store(true, Ordering::SeqCst);
    let _ = task.await;
}

// ---------------------------------------------------------------------------
// The worker stops blocking its caller's runtime task.
//
// `poll_and_execute_once` calls `executor.execute(...)` SYNCHRONOUSLY inside
// `std::thread::scope`, blocking whichever OS thread is driving this
// future's poll() for the executor's entire duration — with no yield point.
// An async sibling multiplexed onto the SAME task (exactly the shape
// `tokio::join!` produces, and exactly the shape the pinned remote-
// cancellation test uses) cannot be polled until the executor returns. This
// test is isolated so a fix is credited to a real engine change (moving the
// executor call onto `spawn_blocking`), never to a test-only rewrite: a
// public async fn that blocks a runtime task for an arbitrary consumer
// callback is hostile to every consumer that links this crate directly as a
// library.
// ---------------------------------------------------------------------------

/// An executor that signals it has started, then blocks its OS thread in two
/// bounded halves (mirrors `RemoteCancelProbeExecutor`'s own
/// `recv_timeout`-bounded block above — never a real-time sleep-as-sync).
/// Between the two halves it takes a single STATE READ of the caller's own
/// progress counter — not an elapsed-time assertion — so the recorded value
/// proves whether an async sibling joined onto the same task was ever
/// scheduled while this call was in flight.
struct StarvationProbeExecutor {
    reached: std::sync::mpsc::Sender<()>,
    sibling_ticks: Arc<AtomicUsize>,
    ticks_observed_mid_execution: Arc<AtomicUsize>,
}

impl WorkExecutor for StarvationProbeExecutor {
    fn execute(
        &self,
        _job: &JobSnapshot,
        _inputs: &ExecutionInputs,
        _should_abandon: &dyn Fn() -> bool,
    ) -> ExecutionVerdict {
        let _ = self.reached.send(());
        let (_first_tx, first_rx) = std::sync::mpsc::channel::<()>();
        let _ = first_rx.recv_timeout(Duration::from_millis(150));
        self.ticks_observed_mid_execution
            .store(self.sibling_ticks.load(Ordering::SeqCst), Ordering::SeqCst);
        let (_second_tx, second_rx) = std::sync::mpsc::channel::<()>();
        let _ = second_rx.recv_timeout(Duration::from_millis(150));
        ExecutionVerdict::Completed(ExecutionOutput {
            output: b"starvation-probe-done".to_vec(),
            receipt: serde_json::json!({}),
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_caller_keeps_making_progress_while_the_executor_runs() {
    let broker = InProcessBroker::new();
    let tenant = "wl-s3-starvation";
    let (_hub_db, shutdown, task) = start_hub(&broker, tenant);

    let (a_db, a_client) = edge(&broker, tenant);
    let (b_db, b_client) = edge(&broker, tenant);

    submit_job(
        &a_db,
        &demo_spec("job-s3-starve", "node-a"),
        &[b"chunk-0", b"chunk-1"],
    )
    .expect("submit");
    within(a_client.push()).await.expect("A push");
    advertise_capability(&b_db, "node-b", "wl.demo", &tags(&["class:wl.demo"]), T0)
        .expect("B advertises");

    let (reached_tx, reached_rx) = std::sync::mpsc::channel();
    let sibling_ticks = Arc::new(AtomicUsize::new(0));
    let ticks_observed_mid_execution = Arc::new(AtomicUsize::new(0));
    let executor: Arc<dyn WorkExecutor> = Arc::new(StarvationProbeExecutor {
        reached: reached_tx,
        sibling_ticks: sibling_ticks.clone(),
        ticks_observed_mid_execution: ticks_observed_mid_execution.clone(),
    });
    let config = worker_config("node-b");

    let poll = poll_and_execute_once(&b_client, &config, executor, T0 + 1);
    let sibling = {
        let sibling_ticks = sibling_ticks.clone();
        async move {
            // Bounded, event-driven wait for the executor to genuinely
            // start — cooperative yields only, no sleep.
            for _ in 0..100_000 {
                match reached_rx.try_recv() {
                    Ok(()) => break,
                    Err(std::sync::mpsc::TryRecvError::Empty) => {
                        tokio::task::yield_now().await;
                    }
                    Err(err) => panic!("worker never reached the executor: {err}"),
                }
            }
            // A caller multiplexed onto poll_and_execute_once's own task
            // (exactly what tokio::join! produces) must still be pollable
            // while the executor is mid-call — each tick is one genuine
            // poll of this sibling future.
            for _ in 0..500 {
                sibling_ticks.fetch_add(1, Ordering::SeqCst);
                tokio::task::yield_now().await;
            }
        }
    };

    let (outcome, ()) = within(async { tokio::join!(poll, sibling) }).await;
    assert!(
        outcome.is_ok(),
        "poll_and_execute_once must not error: {outcome:?}"
    );
    assert!(
        ticks_observed_mid_execution.load(Ordering::SeqCst) > 0,
        "a caller multiplexed onto poll_and_execute_once's own task must \
         keep making progress while the executor is mid-call — the \
         sibling's progress counter was still {} when the executor sampled \
         it at its own midpoint, meaning the sibling was never polled at \
         all during the first half of the executor's call",
        ticks_observed_mid_execution.load(Ordering::SeqCst)
    );

    shutdown.store(true, Ordering::SeqCst);
    let _ = task.await;
}
