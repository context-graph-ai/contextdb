//! Routes that need a real endpoint: sync over a locally bound Iroh hub,
//! blob serving and resolution between two local nodes, and the blob
//! reclaim driver with its sweep observed. Every endpoint binds on this
//! host with no relay; nothing leaves the machine.

use super::fixture::{Store, TENANT};
use super::{EventLog, Outcome, Row, call_async};
use contextdb_core::read_memory::ReadMemoryBudget;
use contextdb_core::{TenantId, Wallclock};
use contextdb_engine::vector_read_memory::ReadRequestBudgetProvider;
use contextdb_engine::work_ledger::{
    BlobHash, ClaimInsert, InputRef, JobSpec, MovementPolicy, cancel_job, insert_claim,
    install_work_ledger_schema, submit_job,
};
use contextdb_engine::{BlobStore, FabricIdentity, SyncClient, SyncServer};
use contextdb_server::transport::iroh::IrohServer;
use contextdb_server::{peer_bind_spec, peer_dial_spec};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

/// A read budget a host supplies for its own requests: it admits every
/// reservation and keeps a count.
#[derive(Debug, Default)]
struct HostBudget(AtomicUsize);

impl ReadMemoryBudget for HostBudget {
    fn try_reserve(
        &self,
        bytes: usize,
        _subsystem: &'static str,
        _operation: &'static str,
        _hint: &'static str,
    ) -> contextdb_core::Result<()> {
        self.0.fetch_add(bytes, Ordering::SeqCst);
        Ok(())
    }

    fn release(&self, bytes: usize) {
        self.0.fetch_sub(bytes, Ordering::SeqCst);
    }
}

#[derive(Debug, Default)]
struct HostProvider(Arc<HostBudget>);

impl ReadRequestBudgetProvider for HostProvider {
    fn request_budget(&self) -> contextdb_core::Result<Arc<dyn ReadMemoryBudget>> {
        Ok(self.0.clone())
    }
}

pub fn host_budget() -> Arc<dyn ReadMemoryBudget> {
    Arc::new(HostBudget::default())
}

pub fn host_provider() -> Arc<dyn ReadRequestBudgetProvider> {
    Arc::new(HostProvider::default())
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("fixture endpoint runtime")
}

/// A populated hub store with a bound local Iroh endpoint on its own
/// runtime, and a populated edge store with its own identity file.
struct IrohHub {
    hub: Store,
    edge: Store,
    bound: Option<IrohServer>,
    edge_identity: PathBuf,
    // Declared last so it outlives everything served on it.
    runtime: tokio::runtime::Runtime,
}

impl IrohHub {
    fn bind() -> Self {
        let hub = Store::open();
        let edge = Store::open();
        let runtime = runtime();
        let identity = hub.sibling("hub.fabric-identity.key");
        let endpoint = runtime
            .block_on(IrohServer::bind(&peer_bind_spec(&identity)))
            .expect("fixture hub endpoint");
        let edge_identity = edge.sibling("edge.fabric-identity.key");
        Self {
            hub,
            edge,
            bound: Some(endpoint),
            edge_identity,
            runtime,
        }
    }

    fn endpoint(&self) -> &IrohServer {
        self.bound.as_ref().expect("fixture endpoint is bound")
    }

    fn dial(&self) -> String {
        peer_dial_spec(&self.endpoint().ticket(), &self.edge_identity)
    }

    fn server(&self) -> Arc<SyncServer> {
        let _context = self.runtime.enter();
        Arc::new(
            SyncServer::new(self.hub.db.clone(), self.endpoint(), TenantId::from(TENANT))
                .expect("fixture hub server"),
        )
    }

    /// Pull once from the edge through whatever the hub is serving with.
    fn pull_through(&self) -> Outcome {
        let dial = self.dial();
        call_async(|| async {
            SyncClient::new(self.edge.db.clone(), &dial, TenantId::from(TENANT))?
                .pull_default()
                .await
        })
    }
}

impl Drop for IrohHub {
    fn drop(&mut self) {
        // The endpoint closes inside its own runtime; the runtime then drops
        // last and cancels whatever it is still serving.
        let _context = self.runtime.enter();
        self.bound.take();
    }
}

fn now_ms() -> i64 {
    Wallclock::now().0 as i64
}

fn identity_node(path: &std::path::Path) -> String {
    FabricIdentity::load_or_generate(path)
        .expect("fixture identity")
        .node_id()
}

/// Seed a blob job submitted by `submitter` and claimed by `claimant`, as
/// both the holder and the consumer ledger must show it.
fn entitle(db: &contextdb_engine::Database, submitter: &str, claimant: &str, hash: &BlobHash) {
    install_work_ledger_schema(db).expect("fixture ledger schema");
    let now = now_ms();
    let job = JobSpec::builder("blob-job", "media", "batch", submitter)
        .input_refs(vec![InputRef::blob_ref(hash.clone())])
        .submitted_at_ms(now)
        .build();
    submit_job(db, &job, &[] as &[&[u8]]).expect("fixture blob job");
    match insert_claim(db, "blob-job", 1, claimant, now + 10 * 60_000, now)
        .expect("fixture blob claim")
    {
        ClaimInsert::Inserted => {}
        other => panic!("fixture claim must insert: {other:?}"),
    }
}

const PAYLOAD: &[u8] = b"route blob payload";

/// A holder serving one entitled blob over a bound endpoint, and a consumer
/// whose ledger carries the same claim.
struct BlobPair {
    _holder: BlobStore,
    hub: IrohHub,
    consumer: BlobStore,
    ticket: String,
    hash: BlobHash,
}

#[expect(
    clippy::clone_on_copy,
    reason = "The fixture keeps an explicit copy at the point where the policy is passed to a new holder."
)]
fn blob_pair() -> BlobPair {
    let hub = IrohHub::bind();
    let holder_key = hub.hub.sibling("hub.fabric-identity.key");
    let consumer_key = hub.edge_identity.clone();
    let holder_node = identity_node(&holder_key);
    let consumer_node = identity_node(&consumer_key);
    let hash = BlobHash::of(PAYLOAD);
    entitle(&hub.hub.db, &holder_node, &consumer_node, &hash);
    entitle(&hub.edge.db, &holder_node, &consumer_node, &hash);
    let policy = MovementPolicy {
        auto_propagate: true,
    };
    let holder = BlobStore::new(hub.hub.db.clone(), policy.clone(), holder_key);
    holder.ingest_bytes(PAYLOAD).expect("fixture holder blob");
    {
        let _context = hub.runtime.enter();
        holder
            .serve_on(hub.endpoint())
            .expect("fixture holder serving");
    }
    let consumer = BlobStore::new(hub.edge.db.clone(), policy, consumer_key);
    let ticket = hub.endpoint().ticket();
    BlobPair {
        hub,
        _holder: holder,
        consumer,
        ticket,
        hash,
    }
}

/// A store holding one blob whose only job was cancelled long ago, so a
/// sweep with no grace reclaims it.
fn reclaimable() -> (Store, Arc<BlobStore>) {
    let store = Store::open();
    let key = store.sibling("blob.fabric-identity.key");
    let node = identity_node(&key);
    install_work_ledger_schema(&store.db).expect("fixture ledger schema");
    let hash = BlobHash::of(PAYLOAD);
    let job = JobSpec::builder("old-job", "media", "batch", &node)
        .input_refs(vec![InputRef::blob_ref(hash)])
        .build();
    submit_job(&store.db, &job, &[] as &[&[u8]]).expect("fixture old job");
    cancel_job(&store.db, "old-job", "operator", None, 0).expect("fixture cancel");
    let blobs = Arc::new(BlobStore::new(
        store.db.clone(),
        MovementPolicy {
            auto_propagate: true,
        },
        key,
    ));
    blobs.ingest_bytes(PAYLOAD).expect("fixture blob");
    (store, blobs)
}

/// Spawn the driver on this thread's runtime and step it until its first
/// sweep either reclaimed the blob or logged a failure.
async fn first_sweep(
    blobs: &Arc<BlobStore>,
    failures: &EventLog,
    handle: tokio::task::JoinHandle<()>,
) -> Result<(), String> {
    loop {
        if blobs.servable_count_for_test() == 0 {
            handle.abort();
            return Ok(());
        }
        if let Some(failure) = failures.first() {
            handle.abort();
            return Err(failure);
        }
        tokio::task::yield_now().await;
    }
}

pub fn rows() -> Vec<Row> {
    vec![
        Row {
            route: "contextdb_engine::SyncServer::new",
            drive: || {
                let hub = IrohHub::bind();
                call_async(|| async {
                    SyncServer::new(hub.hub.db.clone(), hub.endpoint(), TenantId::from(TENANT))
                })
            },
        },
        Row {
            route: "contextdb_engine::SyncServer::new_with_read_budget_provider",
            drive: || {
                let hub = IrohHub::bind();
                call_async(|| async {
                    SyncServer::new_with_read_budget_provider(
                        hub.hub.db.clone(),
                        hub.endpoint(),
                        TenantId::from(TENANT),
                        host_provider(),
                    )
                })
            },
        },
        Row {
            route: "contextdb_engine::SyncServer::run",
            drive: || {
                // `run` serves until its runtime stops; the pull it serves is
                // its answer.
                let hub = IrohHub::bind();
                let server = hub.server();
                hub.runtime.spawn(async move { server.run().await });
                hub.pull_through()
            },
        },
        Row {
            route: "contextdb_engine::SyncServer::native_stop",
            drive: || {
                let hub = IrohHub::bind();
                let server = hub.server();
                call_async(|| async { server.native_stop(Arc::new(AtomicBool::new(false))) })
            },
        },
        Row {
            route: "contextdb_engine::SyncServer::run_until_stopped",
            drive: || {
                let hub = IrohHub::bind();
                let server = hub.server();
                let flag = Arc::new(AtomicBool::new(false));
                let stop = {
                    let _context = hub.runtime.enter();
                    server
                        .native_stop(flag.clone())
                        .expect("fixture native stop")
                };
                hub.runtime
                    .spawn(async move { server.run_until_stopped(stop).await });
                let outcome = hub.pull_through();
                flag.store(true, Ordering::SeqCst);
                outcome
            },
        },
        Row {
            route: "contextdb_engine::SyncClient::new",
            drive: || {
                let hub = IrohHub::bind();
                let dial = hub.dial();
                call_async(|| async {
                    SyncClient::new(hub.edge.db.clone(), &dial, TenantId::from(TENANT))
                })
            },
        },
        Row {
            route: "contextdb_engine::SyncClient::new_with_read_memory",
            drive: || {
                let hub = IrohHub::bind();
                let dial = hub.dial();
                call_async(|| async {
                    SyncClient::new_with_read_memory(
                        hub.edge.db.clone(),
                        &dial,
                        TenantId::from(TENANT),
                        hub.edge.db.declared_reader_memory()?,
                    )
                })
            },
        },
        Row {
            route: "contextdb_engine::BlobStore::serve_on",
            drive: || {
                let hub = IrohHub::bind();
                let holder = BlobStore::new(
                    hub.hub.db.clone(),
                    MovementPolicy::default(),
                    hub.hub.sibling("hub.fabric-identity.key"),
                );
                call_async(|| async { holder.serve_on(hub.endpoint()) })
            },
        },
        Row {
            route: "contextdb_engine::BlobStore::serve_on_with_read_budget_provider",
            drive: || {
                let hub = IrohHub::bind();
                let holder = BlobStore::new(
                    hub.hub.db.clone(),
                    MovementPolicy::default(),
                    hub.hub.sibling("hub.fabric-identity.key"),
                );
                call_async(|| async {
                    holder.serve_on_with_read_budget_provider(hub.endpoint(), host_provider())
                })
            },
        },
        Row {
            route: "contextdb_engine::BlobStore::resolve_blob_ref",
            drive: || {
                let pair = blob_pair();
                call_async(|| async {
                    let mut sink = Vec::new();
                    pair.consumer
                        .resolve_blob_ref(&pair.hash, &pair.ticket, &mut sink)
                        .await
                })
            },
        },
        Row {
            route: "contextdb_engine::BlobStore::resolve_blob_ref_with_read_memory",
            drive: || {
                let pair = blob_pair();
                call_async(|| async {
                    let memory = pair
                        .consumer_memory()
                        .map_err(|error| format!("{error:?}"))?;
                    let mut sink = Vec::new();
                    pair.consumer
                        .resolve_blob_ref_with_read_memory(
                            memory,
                            &pair.hash,
                            &pair.ticket,
                            &mut sink,
                        )
                        .await
                        .map_err(|error| format!("{error:?}"))
                })
            },
        },
        Row {
            route: "contextdb_engine::BlobStore::spawn_reclaim_driver",
            drive: || {
                let (_store, blobs) = reclaimable();
                call_async(|| async {
                    let failures = EventLog::at(tracing::Level::WARN);
                    let _subscriber = failures.install();
                    let handle = blobs.clone().spawn_reclaim_driver(0);
                    first_sweep(&blobs, &failures, handle).await
                })
            },
        },
        Row {
            route: "contextdb_engine::BlobStore::spawn_reclaim_driver_with_read_budget_provider",
            drive: || {
                let (_store, blobs) = reclaimable();
                call_async(|| async {
                    let failures = EventLog::at(tracing::Level::WARN);
                    let _subscriber = failures.install();
                    let handle = blobs
                        .clone()
                        .spawn_reclaim_driver_with_read_budget_provider(host_provider(), 0);
                    first_sweep(&blobs, &failures, handle).await
                })
            },
        },
    ]
}

impl BlobPair {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn consumer_memory(
        &self,
    ) -> contextdb_core::Result<contextdb_core::read_memory::ReadMemoryOwner> {
        self.hub.edge.db.declared_reader_memory()
    }
}
