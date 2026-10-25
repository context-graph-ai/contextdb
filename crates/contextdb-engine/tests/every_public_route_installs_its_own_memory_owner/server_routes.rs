//! Server work routes: the node-contact table on a store, and claiming and
//! executing ledger work through a sync client connected to a hub.

use super::fixture::Store;
use super::network_routes::host_provider;
use super::sync_routes::Pair;
use super::{EventLog, Row, call, call_async_expecting};
use contextdb_core::Wallclock;
use contextdb_engine::work_ledger::{
    ExecutionInputs, JobSnapshot, JobSpec, MovementPolicy, install_work_ledger_schema, submit_job,
};
use contextdb_server::work_ledger::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

const JOB: &str = "work-1";
const WORKER: &str = "node-w";
const TAG: &str = "class:route";
const LEASE: i64 = 5 * 60_000;

fn now_ms() -> i64 {
    Wallclock::now().0 as i64
}

fn job(submitter: &str) -> JobSpec {
    JobSpec::builder(JOB, "route", "batch", submitter)
        .requirement_tags(vec![TAG.to_string()])
        .submitted_at_ms(1_000)
        .build()
}

fn config() -> WorkerConfig {
    WorkerConfig {
        node_id: WORKER.to_string(),
        advertised_tags: vec![TAG.to_string()],
        movement_policy: MovementPolicy {
            auto_propagate: true,
        },
        lease_duration_ms: LEASE,
        blob_store: None,
        defer_own_submissions_until_deadline: false,
        writes_are_canonical: false,
    }
}

/// Completes every job it is handed, and raises `done` (and `stop`, when
/// it drives a loop) when it has.
struct Completes {
    done: Arc<AtomicBool>,
    stop: Option<Arc<AtomicBool>>,
}

impl WorkExecutor for Completes {
    fn execute(
        &self,
        _job: &JobSnapshot,
        inputs: &ExecutionInputs,
        _should_abandon: &dyn Fn() -> bool,
    ) -> ExecutionVerdict {
        self.done.store(true, Ordering::SeqCst);
        if let Some(stop) = &self.stop {
            stop.store(true, Ordering::SeqCst);
        }
        ExecutionVerdict::Completed(ExecutionOutput {
            output: inputs.iter().flat_map(|(_, bytes)| bytes.clone()).collect(),
            receipt: serde_json::json!({ "route": true }),
        })
    }
}

fn executor(done: &Arc<AtomicBool>) -> Arc<dyn WorkExecutor> {
    Arc::new(Completes {
        done: done.clone(),
        stop: None,
    })
}

/// A hub whose ledger holds one job for the worker's tag, and a worker edge
/// with the ledger schema.
fn hub_with_work() -> Pair {
    let pair = Pair::serve(true);
    install_work_ledger_schema(&pair.hub.db).expect("fixture hub ledger");
    install_work_ledger_schema(&pair.edge.db).expect("fixture edge ledger");
    submit_job(&pair.hub.db, &job("node-hub"), &[b"work input".as_slice()])
        .expect("fixture hub job");
    pair
}

/// A worker edge that submitted its own job locally.
fn edge_with_own_work() -> Pair {
    let pair = Pair::serve(true);
    install_work_ledger_schema(&pair.hub.db).expect("fixture hub ledger");
    install_work_ledger_schema(&pair.edge.db).expect("fixture edge ledger");
    submit_job(&pair.edge.db, &job(WORKER), &[b"work input".as_slice()])
        .expect("fixture edge job");
    pair
}

pub fn rows() -> Vec<Row> {
    vec![
        Row {
            route: "contextdb_server::work_ledger::install_node_contacts_schema",
            drive: || {
                let store = Store::open();
                call(|| install_node_contacts_schema(&store.db))
            },
        },
        Row {
            route: "contextdb_server::work_ledger::install_node_contacts_schema_with_read_memory",
            drive: || {
                let store = Store::open();
                call(|| {
                    install_node_contacts_schema_with_read_memory(
                        &store.db,
                        store
                            .db
                            .declared_reader_memory()
                            .map_err(|error| format!("{error:?}"))
                            .expect("fixture reader"),
                    )
                })
            },
        },
        Row {
            route: "contextdb_server::work_ledger::record_node_contact",
            drive: || {
                let store = Store::open();
                install_node_contacts_schema(&store.db).expect("fixture contacts schema");
                call(|| record_node_contact(&store.db, "node-a", 2_000))
            },
        },
        Row {
            route: "contextdb_server::work_ledger::record_node_contact_with_read_memory",
            drive: || {
                let store = Store::open();
                install_node_contacts_schema(&store.db).expect("fixture contacts schema");
                let memory = store.db.declared_reader_memory().expect("fixture reader");
                call(|| record_node_contact_with_read_memory(&store.db, memory, "node-a", 2_000))
            },
        },
        Row {
            // A reachable hub must record the claim: `Won { synced: false }`
            // is the answer for a hub that could not be reached.
            route: "contextdb_server::work_ledger::claim_job",
            drive: || {
                let pair = edge_with_own_work();
                call_async_expecting(
                    || async {
                        let now = now_ms();
                        claim_job(&pair.client()?, JOB, 1, WORKER, now + LEASE, now).await
                    },
                    synced_claim,
                )
            },
        },
        Row {
            route: "contextdb_server::work_ledger::claim_job_with_read_memory",
            drive: || {
                let pair = edge_with_own_work();
                call_async_expecting(
                    || async {
                        let now = now_ms();
                        claim_job_with_read_memory(
                            &pair.client()?,
                            pair.edge.db.declared_reader_memory()?,
                            JOB,
                            1,
                            WORKER,
                            now + LEASE,
                            now,
                        )
                        .await
                    },
                    synced_claim,
                )
            },
        },
        Row {
            // The hub holds one job for this worker's tag; one poll must
            // execute it.
            route: "contextdb_server::work_ledger::poll_and_execute_once",
            drive: || {
                let pair = hub_with_work();
                let done = Arc::new(AtomicBool::new(false));
                call_async_expecting(
                    || async {
                        let log = EventLog::at(tracing::Level::DEBUG);
                        let _events = log.install();
                        let polled =
                            poll_and_execute_once(&pair.client()?, &config(), executor(&done), now_ms())
                                .await?;
                        Ok::<_, contextdb_core::Error>((polled, log.text()))
                    },
                    executed_the_hub_job,
                )
            },
        },
        Row {
            route: "contextdb_server::work_ledger::poll_and_execute_once_with_read_memory",
            drive: || {
                let pair = hub_with_work();
                let done = Arc::new(AtomicBool::new(false));
                call_async_expecting(
                    || async {
                        let log = EventLog::at(tracing::Level::DEBUG);
                        let _events = log.install();
                        let polled = poll_and_execute_once_with_read_memory(
                            &pair.client()?,
                            pair.edge.db.declared_reader_memory()?,
                            &config(),
                            executor(&done),
                            now_ms(),
                        )
                        .await?;
                        Ok::<_, contextdb_core::Error>((polled, log.text()))
                    },
                    executed_the_hub_job,
                )
            },
        },
        Row {
            route: "contextdb_server::work_ledger::run_worker_loop",
            drive: || {
                let pair = hub_with_work();
                call_async_expecting(
                    || async {
                        let client = pair.client()?;
                        let worker = Worker::new();
                        let config = config();
                        let looping = run_worker_loop(
                            &client,
                            &config,
                            worker.executor(),
                            Duration::ZERO,
                            worker.stop.clone(),
                        );
                        Ok::<_, contextdb_core::Error>(worker.drive(looping).await)
                    },
                    loop_executed_the_hub_job,
                )
            },
        },
        Row {
            route: "contextdb_server::work_ledger::run_worker_loop_with_read_budget_provider",
            drive: || {
                let pair = hub_with_work();
                call_async_expecting(
                    || async {
                        let client = pair.client()?;
                        let worker = Worker::new();
                        let config = config();
                        let looping = run_worker_loop_with_read_budget_provider(
                            &client,
                            host_provider(),
                            &config,
                            worker.executor(),
                            Duration::ZERO,
                            worker.stop.clone(),
                        );
                        Ok::<_, contextdb_core::Error>(worker.drive(looping).await)
                    },
                    loop_executed_the_hub_job,
                )
            },
        },
    ]
}

fn synced_claim(outcome: ClaimOutcome) -> Result<(), String> {
    match outcome {
        ClaimOutcome::Won { synced: true } => Ok(()),
        other => Err(format!("a reachable hub did not record the claim: {other:?}")),
    }
}

fn executed_the_hub_job((polled, events): (PollOutcome, String)) -> Result<(), String> {
    match polled {
        PollOutcome::Executed { job_id, .. } if job_id == JOB => Ok(()),
        other => Err(format!(
            "the poll did not execute the hub's job: {other:?}; events: {events}"
        )),
    }
}

fn loop_executed_the_hub_job(
    (ended, executed, events): (Result<(), contextdb_core::Error>, bool, String),
) -> Result<(), String> {
    match ended {
        Ok(()) if executed => Ok(()),
        Ok(()) => Err(format!(
            "the loop reported the hub unreachable instead of executing its job; events: {events}"
        )),
        Err(error) => Err(format!("the loop ended with {error:?}; events: {events}")),
    }
}

/// A worker loop driven to one of its two ends: the executor completed the
/// hub's job and raised the stop flag, or the loop logged that it treats
/// the hub as unreachable (it then idles forever, so the flag is raised
/// here). With the hub's job claimable by this worker, a loop that can
/// pull and push executes it on its first poll, and one that cannot logs
/// the unreachable-hub event; a bounded step count stops anything else.
struct Worker {
    stop: Arc<AtomicBool>,
    executed: Arc<AtomicBool>,
    log: EventLog,
}

impl Worker {
    fn new() -> Self {
        Self {
            stop: Arc::new(AtomicBool::new(false)),
            executed: Arc::new(AtomicBool::new(false)),
            log: EventLog::at(tracing::Level::DEBUG),
        }
    }

    fn executor(&self) -> Arc<dyn WorkExecutor> {
        Arc::new(Completes {
            done: self.executed.clone(),
            stop: Some(self.stop.clone()),
        })
    }

    async fn drive(
        &self,
        looping: impl std::future::Future<Output = Result<(), contextdb_core::Error>>,
    ) -> (Result<(), contextdb_core::Error>, bool, String) {
        let _events = self.log.install();
        let mut looping = std::pin::pin!(looping);
        // A loop that neither executes the job nor logs an unreachable hub
        // within this many scheduler steps is stopped and reported, so the
        // row can never wait without end.
        let mut steps_left: u32 = 2_000_000;
        loop {
            tokio::select! {
                biased;
                ended = &mut looping => {
                    return (ended, self.executed.load(Ordering::SeqCst), self.log.text());
                }
                _ = tokio::task::yield_now() => {
                    steps_left = steps_left.saturating_sub(1);
                    if steps_left == 0 {
                        self.log.note("the loop neither executed the hub's job nor reported the hub unreachable");
                        self.stop.store(true, Ordering::SeqCst);
                    }
                    if self.log.find("unreachable").is_some() || self.log.find("push missed").is_some() {
                        self.stop.store(true, Ordering::SeqCst);
                    }
                }
            }
        }
    }
}
