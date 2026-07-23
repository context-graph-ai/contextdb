//! The distributed half of the work ledger: claim-by-push and the worker
//! poll loop. Library surface only — the CLI, examples, and any future
//! server are callers of these functions, never the surface itself.
//!
//! Everything here goes through the transport-neutral sync client; nothing
//! names a pipe. The lease is exactly two protections stacked: the local
//! primary key (same machine) and the hub's per-table conflict rule (across
//! machines) — see [`contextdb_engine::work_ledger`] for the table contract.
//!
//! There is no ledger-specific error type: every fallible function here and
//! in [`contextdb_engine::work_ledger`] returns `contextdb_core::Error` —
//! match on that type (see e.g. [`contextdb_core::Error::Other`] and
//! [`contextdb_core::Error::InputRequiresBlobResolver`]).

use contextdb_core::{Error, Value};
use contextdb_engine::work_ledger::{
    self as ledger, ClaimInsert, ExecutionInputs, JobSnapshot, MovementPolicy,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crate::sync_client::SyncClient;

/// How often an EXECUTING worker pulls from the hub so its between-chunks
/// abandonment check can observe remote cancellations and results.
const EXECUTION_PULL_INTERVAL_MS: u64 = 250;

/// The capability id a generic worker advertises for its standing loop. Its
/// `advertised_at` is refreshed on every poll cycle (see [`run_worker_loop`]),
/// so it doubles as the node's liveness heartbeat: a consumer with no
/// hub-local last-contact record reads the node's freshest advertisement to
/// decide whether it is still alive.
const WORKER_LOOP_CAPABILITY: &str = "worker-loop";

/// The hub-local table recording, per node, the wall-clock ms of that node's
/// most recent contact with this hub. Written ONLY by the hub's serve path
/// (see [`record_node_contact`]) and never synced to edges — the pull path
/// filters it out, so an edge that queries its own store finds no row and
/// falls back to the capability's `advertised_at` for liveness.
pub const WORK_NODE_CONTACTS_TABLE: &str = "work_node_contacts";

const CREATE_WORK_NODE_CONTACTS: &str = "CREATE TABLE work_node_contacts (\
     node_id TEXT PRIMARY KEY, \
     last_contact_ms TIMESTAMP NOT NULL)";

/// Create the hub-local `work_node_contacts` table if absent. Idempotent;
/// safe to call at every hub open and on every recorded contact.
pub fn install_node_contacts_schema(db: &contextdb_engine::Database) -> Result<(), Error> {
    if db
        .table_names()
        .iter()
        .any(|name| name == WORK_NODE_CONTACTS_TABLE)
    {
        return Ok(());
    }
    db.execute(CREATE_WORK_NODE_CONTACTS, &HashMap::new())?;
    Ok(())
}

/// Record that `node_id` contacted this hub at `now_ms`. This is HUB-LOCAL
/// current-truth — a per-node liveness clock the fleet's aging-out consults,
/// advanced on EVERY sync exchange the hub serves (push/pull/status), so a
/// worker that stops contacting the hub ages out even though its capability
/// row lingers. Advancing an existing node's row is an UPSERT by design (the
/// registry carries the present contact time, not a history of contacts); it
/// is the named exception to the ledger's append-only guard, alongside the
/// capability registry. Never synced to edges.
pub fn record_node_contact(
    db: &contextdb_engine::Database,
    node_id: &str,
    now_ms: i64,
) -> Result<(), Error> {
    install_node_contacts_schema(db)?;
    let mut params = HashMap::new();
    params.insert("node_id".to_string(), Value::Text(node_id.to_string()));
    params.insert("last_contact_ms".to_string(), Value::Timestamp(now_ms));
    db.execute(
        "INSERT INTO work_node_contacts (node_id, last_contact_ms) \
         VALUES ($node_id, $last_contact_ms) ON CONFLICT (node_id) DO UPDATE SET \
         last_contact_ms = $last_contact_ms",
        &params,
    )?;
    Ok(())
}

/// The cross-machine claim outcome, learned from the push reply.
#[derive(Debug, Clone, PartialEq)]
pub enum ClaimOutcome {
    /// This node holds the lease. `synced: false` means the hub was
    /// unreachable and the claim is held locally (the offline mode); it
    /// enters hub arbitration on the next successful push.
    Won { synced: bool },
    /// Another claimant beat this node to (job, attempt).
    Lost { holder: Option<String> },
}

/// What a worker produced for a completed job.
#[derive(Debug, Clone, PartialEq)]
pub struct ExecutionOutput {
    pub output: Vec<u8>,
    /// The execution receipt: operational metadata (elapsed, backend, model,
    /// token counts, reliability). Opaque to the ledger.
    pub receipt: serde_json::Value,
}

/// How one execution attempt ended.
#[derive(Debug, Clone, PartialEq)]
pub enum ExecutionVerdict {
    Completed(ExecutionOutput),
    Failed(String),
    /// The worker observed (via the abandonment check) that the job's result
    /// already landed, the job was cancelled, or its claim was reconciled to
    /// another holder. No rows are written.
    Abandoned,
}

/// The product layer's execution seam. The ledger hands it opaque inputs and
/// records whatever comes back; meaning lives entirely on the caller's side.
/// Long-running executors should call `should_abandon` between chunks.
pub trait WorkExecutor: Send + Sync {
    fn execute(
        &self,
        job: &JobSnapshot,
        inputs: &ExecutionInputs,
        should_abandon: &dyn Fn() -> bool,
    ) -> ExecutionVerdict;
}

/// One worker's standing configuration.
#[derive(Clone)]
pub struct WorkerConfig {
    /// The fabric identity's canonical public-key string. Never minted here.
    pub node_id: String,
    pub advertised_tags: Vec<String>,
    pub movement_policy: MovementPolicy,
    pub lease_duration_ms: i64,
    /// Optional blob resolver a worker uses to fetch `blob_ref` job inputs
    /// node-to-node before execution. `None` (the default every existing
    /// caller gets) preserves today's behavior exactly: a `blob_ref` job
    /// still fails at [`ledger::materialize_inputs`] with
    /// [`contextdb_core::Error::InputRequiresBlobResolver`]. Inert until
    /// `poll_and_execute_once` is taught to consult it.
    pub blob_store: Option<Arc<crate::blob_resolver::BlobStore>>,
    /// When `true`, this worker must not claim its OWN submissions ahead of
    /// their `deadline_ms` — leaving them for another node to pick up first
    /// — and may only claim them itself once the deadline has passed.
    /// `false` (the default every existing caller gets) preserves today's
    /// behavior exactly: a node claims its own submissions immediately,
    /// deadline or not. Inert until `poll_and_execute_once`'s claim
    /// ordering is taught to consult it.
    pub defer_own_submissions_until_deadline: bool,
    /// When `true`, this worker's ledger db IS the canonical store (the node
    /// hosts the hub over the same `Arc<Database>`), so pushing its rows
    /// anywhere is a category error, not a delivery step: the worker loop
    /// skips its startup capability push and the `NoMatchingWork` re-push
    /// entirely instead of burning the client's retry ladder against a dial
    /// spec that can never resolve. `false` (the default every existing
    /// caller gets) preserves edge behavior exactly — including the loud
    /// re-push-until-received delivery contract. Supersedes nothing about
    /// arbitration: claim/verdict rows still ride their existing
    /// watermark-gated paths.
    pub writes_are_canonical: bool,
}

impl std::fmt::Debug for WorkerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerConfig")
            .field("node_id", &self.node_id)
            .field("advertised_tags", &self.advertised_tags)
            .field("movement_policy", &self.movement_policy)
            .field("lease_duration_ms", &self.lease_duration_ms)
            .field("blob_store", &self.blob_store.is_some())
            .field(
                "defer_own_submissions_until_deadline",
                &self.defer_own_submissions_until_deadline,
            )
            .field("writes_are_canonical", &self.writes_are_canonical)
            .finish()
    }
}

/// What one poll pass did.
#[derive(Debug, Clone, PartialEq)]
pub enum PollOutcome {
    NoMatchingWork,
    Executed {
        job_id: String,
        attempt: i64,
    },
    Failed {
        job_id: String,
        attempt: i64,
    },
    Abandoned {
        job_id: String,
    },
    /// Matching jobs existed but every claim was lost to another node.
    LostAllClaims,
}

/// Claim (job, attempt) by insert-then-push. The push reply is the
/// arbitration verdict: a clean reply means the hub filed this claim row; a
/// conflict naming this claim's key means someone else already holds it —
/// the claim is then re-read after a pull, which also reconciles the losing
/// local row to the hub's winner. An unreachable hub degrades to a
/// locally-held claim (`Won { synced: false }`).
pub async fn claim_job(
    client: &SyncClient,
    job_id: &str,
    attempt: i64,
    node_id: &str,
    lease_deadline_ms: i64,
    now_ms: i64,
) -> Result<ClaimOutcome, Error> {
    match ledger::insert_claim(
        client.db(),
        job_id,
        attempt,
        node_id,
        lease_deadline_ms,
        now_ms,
    )? {
        ClaimInsert::AlreadyHeld { holder } if holder != node_id => {
            return Ok(ClaimOutcome::Lost {
                holder: Some(holder),
            });
        }
        ClaimInsert::AlreadyHeld { .. } | ClaimInsert::Inserted => {}
    }

    let claim_key = format!("{job_id}#{attempt}");
    match client.push().await {
        Ok(result) => {
            let refused = result.conflicts.iter().any(|conflict| {
                conflict.natural_key.column == "claim_key"
                    && matches!(&conflict.natural_key.value, Value::Text(v) if v == &claim_key)
            });
            if !refused {
                return Ok(ClaimOutcome::Won { synced: true });
            }
            // The hub refused this claim key. Pull to reconcile the local row
            // to the hub's winner, then read who actually holds it — the
            // refusal alone cannot distinguish "someone else won" from "my
            // own earlier win was re-delivered".
            let _ = client.pull_default().await;
            match ledger::claim_holder(client.db(), job_id, attempt)? {
                Some(holder) if holder == node_id => Ok(ClaimOutcome::Won { synced: true }),
                holder => Ok(ClaimOutcome::Lost { holder }),
            }
        }
        // An unreachable hub is the offline mode: the local primary key was
        // the arbiter, the claim rides the next successful push.
        Err(_) => Ok(ClaimOutcome::Won { synced: false }),
    }
}

/// Does `job_id` carry any recorded failure row? Read straight off
/// `work_failures` rather than through [`ledger::job_state`] (which folds
/// failures into a terminal `Failed` verdict only at the attempt ceiling) —
/// the defer-own-submission rule needs "has this job EVER failed an
/// attempt", regardless of how many attempts remain.
fn job_has_failure_rows(db: &contextdb_engine::Database, job_id: &str) -> Result<bool, Error> {
    let mut params = HashMap::new();
    params.insert("job_id".to_string(), Value::Text(job_id.to_string()));
    let result = db.execute(
        "SELECT failure_key FROM work_failures WHERE job_id = $job_id",
        &params,
    )?;
    Ok(!result.rows.is_empty())
}

/// Must `job`, submitted by `config.node_id` itself, be left unclaimed by
/// this same node for now? True only while ALL hold: deferral is enabled,
/// the job carries a future deadline (no deadline means never deferred),
/// that deadline has not yet passed, and no attempt has failed yet (a
/// failure releases the job for immediate self-reclaim, deadline or not).
fn defers_own_submission(
    db: &contextdb_engine::Database,
    config: &WorkerConfig,
    job: &JobSnapshot,
    now_ms: i64,
) -> Result<bool, Error> {
    if !config.defer_own_submissions_until_deadline {
        return Ok(false);
    }
    if job.submitter_node_id != config.node_id {
        return Ok(false);
    }
    let Some(deadline_ms) = job.deadline_ms else {
        return Ok(false);
    };
    if now_ms >= deadline_ms {
        return Ok(false);
    }
    Ok(!job_has_failure_rows(db, &job.job_id)?)
}

/// Decode a `work_inputs.payload` hex string back to bytes. Mirrors the
/// engine's own `encode_payload`/`decode_payload` pair (a plain 2-hex-digit-
/// per-byte codec) — that helper is private to `contextdb_engine`, so a
/// caller assembling bytes alongside a resolved `blob_ref` input reads the
/// table directly and decodes with the identical convention.
fn decode_hex_payload(encoded: &str) -> Result<Vec<u8>, Error> {
    if !encoded.len().is_multiple_of(2) {
        return Err(Error::Other(format!(
            "work ledger: malformed payload encoding (odd length {})",
            encoded.len()
        )));
    }
    let mut out = Vec::with_capacity(encoded.len() / 2);
    let bytes = encoded.as_bytes();
    for pair in bytes.chunks(2) {
        let hi = (pair[0] as char).to_digit(16);
        let lo = (pair[1] as char).to_digit(16);
        match (hi, lo) {
            (Some(hi), Some(lo)) => out.push((hi * 16 + lo) as u8),
            _ => {
                return Err(Error::Other(
                    "work ledger: malformed payload encoding (non-hex byte)".to_string(),
                ));
            }
        }
    }
    Ok(out)
}

/// Read every `work_inputs` row for `job_id`, keyed by its `seq`. Used only
/// to recover the ledger-carried bytes alongside a `blob_ref` input in a
/// mixed job — [`ledger::materialize_inputs`] refuses the whole job the
/// instant ANY input is a `blob_ref`, so a mixed job's ledger-carried bytes
/// must be read directly.
fn read_ledger_input_rows(
    db: &contextdb_engine::Database,
    job_id: &str,
) -> Result<HashMap<i64, Vec<u8>>, Error> {
    let mut params = HashMap::new();
    params.insert("job_id".to_string(), Value::Text(job_id.to_string()));
    let result = db.execute(
        "SELECT seq, payload FROM work_inputs WHERE job_id = $job_id",
        &params,
    )?;
    let seq_idx = result
        .columns
        .iter()
        .position(|c| c == "seq")
        .ok_or_else(|| Error::Other("work_inputs: missing column seq".to_string()))?;
    let payload_idx = result
        .columns
        .iter()
        .position(|c| c == "payload")
        .ok_or_else(|| Error::Other("work_inputs: missing column payload".to_string()))?;
    let mut out = HashMap::new();
    for row in &result.rows {
        let seq = match &row[seq_idx] {
            Value::Int64(n) => *n,
            Value::Timestamp(n) => *n,
            other => {
                return Err(Error::Other(format!(
                    "work_inputs: seq must be a number, found {other:?}"
                )));
            }
        };
        let payload = match &row[payload_idx] {
            Value::Text(s) => decode_hex_payload(s)?,
            other => {
                return Err(Error::Other(format!(
                    "work_inputs: payload must be TEXT, found {other:?}"
                )));
            }
        };
        out.insert(seq, payload);
    }
    Ok(out)
}

/// Resolve a job's inputs through the worker's blob store, for the ONE
/// case [`ledger::materialize_inputs`] itself refuses:
/// [`contextdb_core::Error::InputRequiresBlobResolver`], i.e. at least one
/// input is a `blob_ref`. Nothing here names a work class (Rule: class-
/// blind) — it only walks `job.input_refs` in order, resolving each
/// `blob_ref` node-to-node via `blob_store.resolve_blob_ref` (dialing the
/// submitter's ticket from the fabric peer directory) and recovering any
/// interleaved ledger-carried bytes straight from `work_inputs`, assembling
/// the result in the job's own input-sequence order.
async fn resolve_blob_job_inputs(
    db: &contextdb_engine::Database,
    job: &JobSnapshot,
    blob_store: &crate::blob_resolver::BlobStore,
) -> Result<ExecutionInputs, Error> {
    let mut assembled: ExecutionInputs = Vec::new();
    let mut ledger_rows: Option<HashMap<i64, Vec<u8>>> = None;
    let mut ledger_seq: i64 = 0;
    for (idx, input_ref) in job.input_refs.iter().enumerate() {
        if input_ref.kind == ledger::REF_KIND_BLOB_REF {
            let hash_hex = input_ref
                .detail
                .get("hash")
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    Error::Other(format!(
                        "work ledger: blob_ref input on job {} carries no hash detail",
                        job.job_id
                    ))
                })?;
            let hash = ledger::BlobHash::from_hex(hash_hex)?;
            let ticket =
                contextdb_engine::peer_directory::lookup_peer_ticket(db, &job.submitter_node_id)?
                    .ok_or_else(|| {
                    Error::Other(format!(
                        "work ledger: no peer_directory ticket enrolled for submitter {}",
                        job.submitter_node_id
                    ))
                })?;
            let mut sink: Vec<u8> = Vec::new();
            blob_store
                .resolve_blob_ref(&hash, &ticket, &mut sink)
                .await
                .map_err(|err| {
                    Error::Other(format!(
                        "work ledger: blob_ref resolve failed for job {}: {err}",
                        job.job_id
                    ))
                })?;
            assembled.push((idx as i64, sink));
        } else {
            if ledger_rows.is_none() {
                ledger_rows = Some(read_ledger_input_rows(db, &job.job_id)?);
            }
            let bytes = ledger_rows
                .as_ref()
                .expect("loaded above")
                .get(&ledger_seq)
                .cloned()
                .unwrap_or_default();
            assembled.push((idx as i64, bytes));
            ledger_seq += 1;
        }
    }
    Ok(assembled)
}

/// One full worker pass: pull, list claimable jobs, claim in order, run the
/// executor on the first claim won, record the verdict, push. Every hub
/// round-trip tolerates an unreachable hub (the single-node / offline mode).
pub async fn poll_and_execute_once(
    client: &SyncClient,
    config: &WorkerConfig,
    executor: &dyn WorkExecutor,
    now_ms: i64,
) -> Result<PollOutcome, Error> {
    let _ = client.pull_default().await;
    let db = client.db();
    let jobs = ledger::claimable_jobs(
        db,
        &config.node_id,
        &config.advertised_tags,
        &config.movement_policy,
        now_ms,
    )?;
    let mut jobs_to_try = Vec::with_capacity(jobs.len());
    for job in jobs {
        if defers_own_submission(db, config, &job, now_ms)? {
            continue;
        }
        jobs_to_try.push(job);
    }
    if jobs_to_try.is_empty() {
        // No claimable work — but the standing loop is still this node's only
        // chance to (re-)deliver its outstanding advertisement to the hub. The
        // one-shot startup push may have missed a hub that was not yet
        // reachable, and nothing else ever re-pushes it (the poll loop
        // otherwise pushes only as a side effect of claiming work, and an
        // idle worker with no submitted jobs never claims anything). The
        // push is watermark-gated — a no-op status exchange once the hub
        // already holds the rows — and tolerates an unreachable hub (the
        // offline mode); a persistent miss is surfaced, not swallowed.
        // A canonical node's rows are already home — there is nothing to
        // re-deliver and no hub to dial (writes_are_canonical).
        if !config.writes_are_canonical
            && let Err(err) = client.push().await
        {
            tracing::debug!(
                node_id = %config.node_id,
                error = %err,
                "worker capability re-push deferred; hub unreachable"
            );
        }
        return Ok(PollOutcome::NoMatchingWork);
    }

    let mut lost_any = false;
    for job in jobs_to_try {
        let attempt = ledger::next_attempt(db, &job.job_id)?;
        let lease_deadline_ms = now_ms + config.lease_duration_ms;
        match claim_job(
            client,
            &job.job_id,
            attempt,
            &config.node_id,
            lease_deadline_ms,
            now_ms,
        )
        .await?
        {
            ClaimOutcome::Lost { .. } => {
                lost_any = true;
                continue;
            }
            ClaimOutcome::Won { .. } => {}
        }

        // A claim is a licence to TRY, never to skip the durable-state
        // check: the job may already be done or cancelled.
        if ledger::should_abandon(db, &job.job_id, attempt, &config.node_id, now_ms)? {
            return Ok(PollOutcome::Abandoned { job_id: job.job_id });
        }

        let inputs = match ledger::materialize_inputs(
            db,
            &job.job_id,
            &config.node_id,
            &config.movement_policy,
        ) {
            Ok(inputs) => inputs,
            Err(Error::InputRequiresBlobResolver { .. }) if config.blob_store.is_some() => {
                let blob_store = config.blob_store.as_ref().expect("checked Some above");
                match resolve_blob_job_inputs(db, &job, blob_store).await {
                    Ok(inputs) => inputs,
                    Err(err) => {
                        ledger::record_failure(
                            db,
                            &job.job_id,
                            attempt,
                            &config.node_id,
                            &format!("input materialization failed: {err}"),
                            now_ms,
                        )?;
                        let _ = client.push().await;
                        return Ok(PollOutcome::Failed {
                            job_id: job.job_id,
                            attempt,
                        });
                    }
                }
            }
            Err(err) => {
                ledger::record_failure(
                    db,
                    &job.job_id,
                    attempt,
                    &config.node_id,
                    &format!("input materialization failed: {err}"),
                    now_ms,
                )?;
                let _ = client.push().await;
                return Ok(PollOutcome::Failed {
                    job_id: job.job_id,
                    attempt,
                });
            }
        };

        let job_id_for_check = job.job_id.clone();
        let node_for_check = config.node_id.clone();
        let check = move || {
            ledger::should_abandon(db, &job_id_for_check, attempt, &node_for_check, now_ms)
                .unwrap_or(true)
        };

        // The between-chunks abandonment check must see REMOTE state — a
        // cancellation from the submitter, a result someone else landed — so
        // while the executor runs, a sidecar thread keeps pulling into this
        // store. It is a plain OS thread with its own small runtime because
        // the executor call blocks this task's thread, which would starve
        // any async sibling on a current-thread runtime. Pull failures are
        // tolerated (the offline mode); the thread stops the moment the
        // executor returns. (Without this, a remote cancellation could never
        // stop a running job.)
        let verdict = {
            let stop_pulling = AtomicBool::new(false);
            // Stop the sidecar even when the executor PANICS: without this,
            // the scope's join would wait forever on the still-looping
            // puller and the worker would wedge on unwind instead of
            // propagating the panic.
            struct StopOnUnwind<'a>(&'a AtomicBool);
            impl Drop for StopOnUnwind<'_> {
                fn drop(&mut self) {
                    self.0.store(true, Ordering::SeqCst);
                }
            }
            std::thread::scope(|scope| {
                let stop_guard = StopOnUnwind(&stop_pulling);
                scope.spawn(|| {
                    let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                    else {
                        return;
                    };
                    while !stop_pulling.load(Ordering::SeqCst) {
                        let _ = runtime.block_on(client.pull_default());
                        for _ in 0..(EXECUTION_PULL_INTERVAL_MS / 50) {
                            if stop_pulling.load(Ordering::SeqCst) {
                                return;
                            }
                            std::thread::sleep(Duration::from_millis(50));
                        }
                    }
                });
                let verdict = executor.execute(&job, &inputs, &check);
                drop(stop_guard);
                verdict
            })
        };

        return match verdict {
            ExecutionVerdict::Completed(output) => {
                ledger::record_result(
                    db,
                    &job.job_id,
                    attempt,
                    &config.node_id,
                    &output.output,
                    output.receipt,
                    now_ms,
                )?;
                let _ = client.push().await;
                Ok(PollOutcome::Executed {
                    job_id: job.job_id,
                    attempt,
                })
            }
            ExecutionVerdict::Failed(error) => {
                ledger::record_failure(db, &job.job_id, attempt, &config.node_id, &error, now_ms)?;
                let _ = client.push().await;
                Ok(PollOutcome::Failed {
                    job_id: job.job_id,
                    attempt,
                })
            }
            ExecutionVerdict::Abandoned => {
                // No rows to write; push whatever else is pending (for
                // example a cancellation observed locally) and move on.
                let _ = client.push().await;
                Ok(PollOutcome::Abandoned { job_id: job.job_id })
            }
        };
    }

    Ok(if lost_any {
        PollOutcome::LostAllClaims
    } else {
        PollOutcome::NoMatchingWork
    })
}

fn wall_now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// The standing worker loop: advertise once, then poll on an interval until
/// `shutdown` flips. A thin wall-clock wrapper over
/// [`poll_and_execute_once`]; hosts that need custom pacing call that
/// directly.
pub async fn run_worker_loop(
    client: &SyncClient,
    config: &WorkerConfig,
    executor: &dyn WorkExecutor,
    poll_interval: Duration,
    shutdown: Arc<AtomicBool>,
) -> Result<(), Error> {
    ledger::advertise_capability(
        client.db(),
        &config.node_id,
        WORKER_LOOP_CAPABILITY,
        &config.advertised_tags,
        wall_now_ms(),
    )?;
    // The startup push can miss a hub that is not yet reachable; it is not
    // swallowed silently — a miss is surfaced, and the standing poll loop below
    // re-pushes the outstanding advertisement on its cadence (the
    // `NoMatchingWork` path) until it lands. A canonical node (it hosts the
    // hub over this same db) skips the push outright — its advertisement is
    // already in the canonical store, and there is no dialable hub to miss.
    if !config.writes_are_canonical
        && let Err(err) = client.push().await
    {
        tracing::debug!(
            node_id = %config.node_id,
            error = %err,
            "worker startup capability push missed; will re-push on the poll cadence"
        );
    }
    while !shutdown.load(Ordering::SeqCst) {
        // Keep this node's advertisement CURRENT, not merely re-delivered:
        // re-advertise (upsert) on the poll cadence so `advertised_at` tracks
        // liveness. A consumer edge has no hub-local last-contact clock
        // (`work_node_contacts` never syncs to edges), so the freshest synced
        // advertisement is its ONLY "still alive" signal for a remote worker;
        // a frozen advertisement — re-pushed but never refreshed — would age a
        // perfectly live worker out of the fleet's render + routing after the
        // TTL. The upsert keeps a single row (capability_key primary key), and
        // work_capabilities' LatestWins sync propagates the refresh to the hub
        // and onward to every consumer edge on its next pull.
        ledger::advertise_capability(
            client.db(),
            &config.node_id,
            WORKER_LOOP_CAPABILITY,
            &config.advertised_tags,
            wall_now_ms(),
        )?;
        poll_and_execute_once(client, config, executor, wall_now_ms()).await?;
        tokio::time::sleep(poll_interval).await;
    }
    Ok(())
}
