//! The work-fabric demo caller: a minimal product layer over the work
//! ledger that demonstrates submit → claim → execute → result across real
//! machines.
//!
//! This binary is a CALLER of the library surface — the ledger itself knows
//! nothing about it. Its "work class" (`wl.demo`) is demo-layer meaning: the
//! worker hashes the input bytes; a payload chunk starting with `FAIL!` makes
//! the attempt fail (bounded-retry demonstration); `--chunk-delay-ms` makes
//! jobs long enough to kill a worker mid-job or cancel between chunks.
//!
//! Every node derives its identity from the fabric identity file next to its
//! database (`<db>.fabric-identity.key`) — the same file the sync transport
//! uses, so the consumer/ledger/transport all agree on who this machine is.

use clap::{Parser, Subcommand};
use contextdb_engine::Database;
use contextdb_engine::work_ledger::{
    self as ledger, ExecutionInputs, InputRef, JobSnapshot, JobSpec, MovementPolicy,
};
use contextdb_server::exit_codes::EXIT_ERROR;
use contextdb_server::work_ledger::{
    ClaimOutcome, ExecutionOutput, ExecutionVerdict, PollOutcome, WorkExecutor, WorkerConfig,
    claim_job, poll_and_execute_once,
};
use contextdb_server::{FabricIdentity, SyncClient};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

#[derive(Parser)]
#[command(name = "work_fabric_demo", about = "Demo caller for the work ledger")]
struct Args {
    /// Database path (the fabric identity lives at <db>.fabric-identity.key).
    #[arg(long = "db-path")]
    db: String,
    /// Sync endpoint: the hub's enrollment ticket / endpoint spec.
    #[arg(long = "sync-endpoint")]
    endpoint: String,
    /// Tenant id shared by every node in the fabric.
    #[arg(long = "tenant-id")]
    tenant: String,
    /// May job input auto-propagate to the tenant's other machines?
    #[arg(long, default_value = "on")]
    propagate: String,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Submit a demo job whose input is TEXT (copied into the ledger).
    Submit {
        #[arg(long)]
        job_id: String,
        /// Input text; each --text repeats as one chunk.
        #[arg(long, required = true)]
        text: Vec<String>,
        #[arg(long, default_value_t = 2)]
        max_attempts: i64,
        #[arg(long, default_value_t = 5)]
        priority: i64,
    },
    /// Print a job's computed state and (if done) its result + receipt.
    Status {
        #[arg(long)]
        job_id: String,
    },
    /// Write the one cancellation row for a job.
    Cancel {
        #[arg(long)]
        job_id: String,
    },
    /// Claim one attempt of a job BLIND — the exact library claim path a
    /// worker uses (insert + push + read the reply), without pulling first.
    /// Prints the verdict with the claim key and, on a loss, the holder the
    /// hub's refusal reconciled — the push-reply attribution, witnessed live.
    Claim {
        #[arg(long)]
        job_id: String,
        #[arg(long, default_value_t = 1)]
        attempt: i64,
        #[arg(long, default_value_t = 30_000)]
        lease_ms: i64,
    },
    /// Run the demo worker until Ctrl-C.
    Worker {
        #[arg(long, default_value_t = 30_000)]
        lease_ms: i64,
        /// Per-chunk artificial work time, so a job is long enough to
        /// observe kills and cancellations mid-flight.
        #[arg(long, default_value_t = 0)]
        chunk_delay_ms: u64,
        #[arg(long, default_value_t = 1_000)]
        poll_ms: u64,
    },
    /// Remove input copies of terminal jobs older than the grace period.
    Retention {
        #[arg(long, default_value_t = 60_000)]
        grace_ms: i64,
    },
}

fn wall_now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for b in bytes {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

struct DemoExecutor {
    chunk_delay: Duration,
}

impl WorkExecutor for DemoExecutor {
    fn execute(
        &self,
        job: &JobSnapshot,
        inputs: &ExecutionInputs,
        should_abandon: &dyn Fn() -> bool,
    ) -> ExecutionVerdict {
        let started = std::time::Instant::now();
        let mut all = Vec::new();
        for (seq, chunk) in inputs {
            if should_abandon() {
                eprintln!("[worker] job {} abandoned between chunks", job.job_id);
                return ExecutionVerdict::Abandoned;
            }
            if chunk.starts_with(b"FAIL!") {
                return ExecutionVerdict::Failed(format!(
                    "demo payload requested failure at chunk {seq}"
                ));
            }
            std::thread::sleep(self.chunk_delay);
            all.extend_from_slice(chunk);
            eprintln!("[worker] job {} processed chunk {seq}", job.job_id);
        }
        ExecutionVerdict::Completed(ExecutionOutput {
            output: format!("len={};fnv={:016x}", all.len(), fnv1a(&all)).into_bytes(),
            receipt: serde_json::json!({
                "backend": "work-fabric-demo",
                "elapsed_ms": started.elapsed().as_millis() as i64,
                "chunks": inputs.len(),
            }),
        })
    }
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let policy = MovementPolicy {
        auto_propagate: args.propagate == "on",
    };

    let identity_path = format!("{}.fabric-identity.key", args.db);
    let identity =
        FabricIdentity::load_or_generate(std::path::Path::new(&identity_path)).expect("identity");
    let node_id = identity.node_id();
    eprintln!("[demo] node id: {node_id}");

    let db = Arc::new(Database::open(&args.db).unwrap_or_else(|err| {
        eprintln!("cannot open {}: {err}", args.db);
        eprintln!(
            "(one process per store: if a worker or another demo command is running against \
             this database, stop it first)"
        );
        std::process::exit(EXIT_ERROR);
    }));
    ledger::install_work_ledger_schema(&db).expect("install work ledger schema");

    // Attach this node's persistent identity to the endpoint spec unless the
    // operator already did. Pure string handling: the demo names no
    // transport — the endpoint spec is operator-supplied config data.
    let endpoint = if args.endpoint.contains('?') && !args.endpoint.contains("identity=") {
        format!("{}&identity={}", args.endpoint, identity_path)
    } else {
        args.endpoint.clone()
    };
    let client = SyncClient::new(
        db.clone(),
        &endpoint,
        contextdb_core::TenantId::from(&args.tenant),
    );

    match args.command {
        Command::Submit {
            job_id,
            text,
            max_attempts,
            priority,
        } => {
            let spec =
                JobSpec::builder(job_id.clone(), "wl.demo", "demo-transform", node_id.clone())
                    .requirement_tags(vec!["class:wl.demo".to_string()])
                    .input_refs(vec![InputRef::ledger_input()])
                    .priority(priority)
                    .max_attempts(max_attempts)
                    .provenance(Some(serde_json::json!({ "origin": "work_fabric_demo" })))
                    .submitted_at_ms(wall_now_ms())
                    .build();
            let chunks: Vec<&[u8]> = text.iter().map(|t| t.as_bytes()).collect();
            ledger::submit_job(&db, &spec, &chunks).expect("submit");
            match client.push().await {
                Ok(result) => println!(
                    "submitted {job_id}: pushed (applied {}, skipped {})",
                    result.applied_rows, result.skipped_rows
                ),
                Err(err) => {
                    println!("submitted {job_id}: hub unreachable ({err}); will sync later")
                }
            }
        }
        Command::Status { job_id } => {
            // A status touch synchronizes BOTH ways: push any local backlog
            // first (offline-born rows after a reconnect), then pull.
            let _ = client.push().await;
            let _ = client.pull_default().await;
            let Ok(state) = ledger::job_state(&db, &job_id, wall_now_ms()) else {
                println!("job {job_id}: unknown on this node (not yet synced here)");
                return;
            };
            println!("job {job_id}: {state:?}");
            if let Some(result) = ledger::job_result(&db, &job_id).expect("result read") {
                println!(
                    "  executed by {} (attempt {})",
                    result.executor_node_id, result.attempt
                );
                println!("  output: {}", String::from_utf8_lossy(&result.output));
                println!("  receipt: {}", result.receipt);
            }
            // The exact-count audit lines the live smoke asserts on: every
            // claim row and the result-row count, read straight from this
            // store's tables (a demo-layer read; the ledger stays opaque).
            let mut params = std::collections::HashMap::new();
            params.insert(
                "job_id".to_string(),
                contextdb_core::Value::Text(job_id.clone()),
            );
            let claims = db
                .execute(
                    "SELECT attempt, node_id, lease_deadline FROM work_claims \
                     WHERE job_id = $job_id",
                    &params,
                )
                .expect("claims scan");
            println!("  claim rows: {}", claims.rows.len());
            for row in &claims.rows {
                println!("    claim: {row:?}");
            }
            let results = db
                .execute(
                    "SELECT job_id FROM work_results WHERE job_id = $job_id",
                    &params,
                )
                .expect("results scan");
            println!("  result rows: {}", results.rows.len());
        }
        Command::Cancel { job_id } => {
            ledger::cancel_job(&db, &job_id, &node_id, Some("demo cancel"), wall_now_ms())
                .expect("cancel");
            let _ = client.push().await;
            println!("cancelled {job_id}");
        }
        Command::Claim {
            job_id,
            attempt,
            lease_ms,
        } => {
            let now = wall_now_ms();
            let outcome = claim_job(&client, &job_id, attempt, &node_id, now + lease_ms, now)
                .await
                .expect("claim");
            match outcome {
                ClaimOutcome::Won { synced } => println!(
                    "claim WON {job_id}#{attempt} (synced: {synced}) — this node holds the lease"
                ),
                ClaimOutcome::Lost { holder } => println!(
                    "claim LOST {job_id}#{attempt} — the hub's push reply refused this claim \
                     key; holder: {}",
                    holder.as_deref().unwrap_or("<unknown>")
                ),
            }
        }
        Command::Worker {
            lease_ms,
            chunk_delay_ms,
            poll_ms,
        } => {
            ledger::advertise_capability(
                &db,
                &node_id,
                "wl.demo",
                &["class:wl.demo".to_string()],
                wall_now_ms(),
            )
            .expect("advertise");
            let _ = client.push().await;
            let config = WorkerConfig {
                node_id: node_id.clone(),
                advertised_tags: vec!["class:wl.demo".to_string()],
                movement_policy: policy,
                lease_duration_ms: lease_ms,
                blob_store: None,
                defer_own_submissions_until_deadline: false,
                writes_are_canonical: false,
            };
            let executor = DemoExecutor {
                chunk_delay: Duration::from_millis(chunk_delay_ms),
            };
            let shutdown = Arc::new(AtomicBool::new(false));
            let flag = shutdown.clone();
            tokio::spawn(async move {
                let _ = tokio::signal::ctrl_c().await;
                flag.store(true, Ordering::SeqCst);
            });
            eprintln!("[worker] polling every {poll_ms}ms; Ctrl-C to stop");
            while !shutdown.load(Ordering::SeqCst) {
                match poll_and_execute_once(&client, &config, &executor, wall_now_ms()).await {
                    Ok(PollOutcome::NoMatchingWork) => {}
                    Ok(outcome) => eprintln!("[worker] {outcome:?}"),
                    Err(err) => eprintln!("[worker] poll error: {err}"),
                }
                tokio::time::sleep(Duration::from_millis(poll_ms)).await;
            }
        }
        Command::Retention { grace_ms } => {
            let removed =
                ledger::run_input_retention(&db, wall_now_ms(), grace_ms).expect("retention");
            let _ = client.push().await;
            println!("removed {removed} input rows");
        }
    }
}
