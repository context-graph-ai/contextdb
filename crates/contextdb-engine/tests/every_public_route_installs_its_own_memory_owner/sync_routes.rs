//! Sync routes between a populated hub and an edge, over the in-process
//! authenticated transport. The hub serves on its own runtime; each client
//! route runs on a fresh thread with that thread's own runtime.

use super::fixture::{Store, TENANT, run};
use super::{Row, call, call_async};
use contextdb_core::TenantId;
use contextdb_engine::sync_client::ApplicationTablePolicyExpectation;
use contextdb_engine::{Database, SyncClient, SyncServer};
use contextdb_server::subjects::push_subject;
use contextdb_server::{FabricIdentity, InProcessBroker};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// A populated hub served over the in-process broker, and an edge store.
pub struct Pair {
    pub hub: Store,
    pub edge: Store,
    pub broker: InProcessBroker,
    shutdown: Arc<AtomicBool>,
    runtime: Option<tokio::runtime::Runtime>,
}

impl Pair {
    /// File-backed stores; `edge_population` declares and fills the edge's
    /// own tables first.
    pub fn serve(edge_population: bool) -> Self {
        let hub = Store::open();
        let edge = if edge_population {
            Store::open()
        } else {
            let dir = tempfile::tempdir().expect("fixture directory");
            let path = dir.path().join("edge.db");
            let db = Database::open(&path).expect("fixture edge open");
            db.set_maintenance_policy(contextdb_engine::MaintenancePolicy::CallerDriven);
            Store {
                dir,
                path,
                db: Arc::new(db),
            }
        };
        Self::serving(hub, edge)
    }

    /// The same pair with both stores in memory.
    pub fn serve_memory() -> Self {
        let edge = Database::open_memory();
        edge.set_maintenance_policy(contextdb_engine::MaintenancePolicy::CallerDriven);
        let in_memory = |db: Arc<Database>| Store {
            dir: tempfile::tempdir().expect("fixture directory"),
            path: std::path::PathBuf::new(),
            db,
        };
        Self::serving(
            in_memory(super::fixture::memory_store()),
            in_memory(Arc::new(edge)),
        )
    }

    fn serving(hub: Store, edge: Store) -> Self {
        let broker = InProcessBroker::new();
        let identity = Arc::new(FabricIdentity::generate());
        let node = identity.node_id();
        let server = Arc::new(
            SyncServer::with_authenticated_transport_and_identity_for_test(
                hub.db.clone(),
                broker.server_as(&node),
                TenantId::from(TENANT),
                node,
                identity,
            )
            .expect("fixture hub endpoint"),
        );
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("fixture hub runtime");
        let shutdown = Arc::new(AtomicBool::new(false));
        runtime.spawn({
            let shutdown = shutdown.clone();
            async move { server.run_until(shutdown).await }
        });
        runtime.block_on(broker.wait_for_registered_route_for_test(&push_subject(TENANT)));
        Self {
            hub,
            edge,
            broker,
            shutdown,
            runtime: Some(runtime),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub fn client(&self) -> Result<SyncClient, contextdb_core::Error> {
        let identity = Arc::new(FabricIdentity::generate());
        SyncClient::with_authenticated_transport_and_identity_for_test(
            self.edge.db.clone(),
            self.broker.client_as(&identity.node_id()),
            TenantId::from(TENANT),
            identity,
        )
    }
}

impl Drop for Pair {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
    }
}

fn expectation() -> ApplicationTablePolicyExpectation {
    ApplicationTablePolicyExpectation::new()
        .expect_table("notes", "SYNC TWO WAY SYNC CONFLICT KEEP LATEST")
        .expect("fixture policy expectation")
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub fn rows() -> Vec<Row> {
    vec![
        Row {
            route: "contextdb_engine::SyncClient::pull_default",
            drive: || {
                // A file-backed pair, then the same journey in memory.
                let pair = Pair::serve(false);
                let memory = Pair::serve_memory();
                call_async(|| async {
                    for pair in [&pair, &memory] {
                        let client = pair.client()?;
                        client.pull_default().await?;
                        run(
                            &pair.hub.db,
                            "INSERT INTO notes (id, bucket, body) VALUES (60, 0, 'later')",
                        );
                        client.pull_default().await?;
                    }
                    Ok::<_, contextdb_core::Error>(())
                })
            },
        },
        Row {
            route: "contextdb_engine::SyncClient::pull_default_with_read_memory",
            drive: || {
                let pair = Pair::serve(false);
                call_async(|| async {
                    let client = pair.client()?;
                    client
                        .pull_default_with_read_memory(pair.edge.db.declared_reader_memory()?)
                        .await
                })
            },
        },
        Row {
            route: "contextdb_engine::SyncServer::run_until",
            drive: || {
                // The served pull is what the running endpoint answers.
                let pair = Pair::serve(false);
                call_async(|| async { pair.client()?.pull_default().await })
            },
        },
        Row {
            route: "contextdb_engine::SyncClient::push",
            drive: || {
                let pair = Pair::serve(true);
                run(
                    &pair.edge.db,
                    "INSERT INTO notes (id, bucket, body) VALUES (61, 1, 'pushed')",
                );
                call_async(|| async { pair.client()?.push().await })
            },
        },
        Row {
            route: "contextdb_engine::SyncClient::push_with_read_memory",
            drive: || {
                let pair = Pair::serve(true);
                run(
                    &pair.edge.db,
                    "INSERT INTO notes (id, bucket, body) VALUES (62, 0, 'pushed')",
                );
                call_async(|| async {
                    pair.client()?
                        .push_with_read_memory(pair.edge.db.declared_reader_memory()?)
                        .await
                })
            },
        },
        Row {
            route: "contextdb_engine::SyncClient::change_destination",
            drive: || {
                let pair = Pair::serve(true);
                call(|| pair.client()?.change_destination("another-hub"))
            },
        },
        Row {
            route: "contextdb_engine::SyncClient::has_pending_push_changes",
            drive: || {
                let pair = Pair::serve(true);
                call(|| pair.client()?.has_pending_push_changes())
            },
        },
        Row {
            route: "contextdb_engine::SyncClient::pending_push_change_count",
            drive: || {
                let pair = Pair::serve(true);
                call(|| pair.client()?.pending_push_change_count())
            },
        },
        Row {
            route: "contextdb_engine::SyncClient::fetch_delivery_outcomes",
            drive: || {
                let pair = Pair::serve(true);
                call_async(|| async { pair.client()?.fetch_delivery_outcomes(None).await })
            },
        },
        Row {
            route: "contextdb_engine::SyncClient::fetch_delivery_outcomes_with_read_memory",
            drive: || {
                let pair = Pair::serve(true);
                call_async(|| async {
                    pair.client()?
                        .fetch_delivery_outcomes_with_read_memory(
                            None,
                            pair.edge.db.declared_reader_memory()?,
                        )
                        .await
                })
            },
        },
        Row {
            route: "contextdb_engine::SyncClient::bind_application_table_policy",
            drive: || {
                let pair = Pair::serve(true);
                call_async(|| async {
                    pair.client()?
                        .bind_application_table_policy(expectation())
                        .await
                })
            },
        },
        Row {
            route: "contextdb_engine::SyncClient::bind_application_table_policy_with_read_memory",
            drive: || {
                let pair = Pair::serve(true);
                call_async(|| async {
                    pair.client()?
                        .bind_application_table_policy_with_read_memory(
                            expectation(),
                            pair.edge.db.declared_reader_memory()?,
                        )
                        .await
                })
            },
        },
    ]
}
