//! Received indexed purges include hook-local commits, or explain a typed refusal.
use super::*;
use crate::transport::in_process::InProcessBroker;
use crate::{FabricIdentity, SyncClient, SyncServer};
use std::sync::Weak;
use std::sync::mpsc::{self, Receiver};
use std::thread::JoinHandle;
const BOUND: Duration = Duration::from_secs(10);
fn bounded(body: impl FnOnce() + Send + 'static) {
    let operation = task(body);
    operation
        .result
        .recv_timeout(BOUND * 5)
        .expect("indexed-purge fixture finishes within its bound");
    operation.join_completed();
}
fn sql(db: &Database, statement: &str) {
    db.execute(statement, &HashMap::new()).unwrap();
}
fn rows(db: &Database) -> Vec<Vec<Value>> {
    db.execute("SELECT id FROM notes ORDER BY id", &HashMap::new())
        .unwrap()
        .rows()
        .to_vec()
}
pub(super) struct Task<T> {
    pub result: Receiver<T>,
    handle: JoinHandle<()>,
}

pub(super) fn task<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> Task<T> {
    let (send, result) = mpsc::channel();
    let handle = std::thread::spawn(move || {
        let value = f();
        let _ = send.send(value);
    });
    Task { result, handle }
}

impl<T> Task<T> {
    pub fn join_completed(self) {
        bounded_join(self.handle);
    }
}

// Even thread-exit/TLS cleanup is bounded. A stuck joiner is detached on
// failure, so libtest can report the failure and exit instead of hanging.
fn bounded_join(handle: JoinHandle<()>) {
    let (send, completed) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = send.send(handle.join());
    });
    completed
        .recv_timeout(BOUND)
        .expect("operation thread blocked during exit")
        .expect("operation thread panicked");
}

struct Network {
    runtime: Arc<tokio::runtime::Runtime>,
    hub: Arc<Database>,
    edge: Arc<Database>,
    client: Arc<SyncClient>,
    stop: Arc<AtomicBool>,
    server: Arc<SyncServer>,
    serving: tokio::task::JoinHandle<()>,
    _directory: tempfile::TempDir,
}
impl Network {
    fn new(plugin: Arc<dyn DatabasePlugin>) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap(),
        );
        let options = || crate::read_session::DatabaseOpenOptions {
            memory_limit: Some(16 << 20),
            owner_reads: crate::read_session::OwnerReadConfig {
                enabled: false,
                ..Default::default()
            },
            maintenance_policy: MaintenancePolicy::CallerDriven,
            ..Default::default()
        };
        let hub = Arc::new(
            Database::open_with_options(directory.path().join("hub.db"), options()).unwrap(),
        );
        let mut configuration = options();
        configuration.plugin = plugin;
        let edge = Arc::new(
            Database::open_with_options(directory.path().join("edge.db"), configuration).unwrap(),
        );
        assert!(matches!(&edge.tx_mgr, DatabaseTransactionHost::Legacy(_)));
        sql(
            &edge,
            "CREATE TABLE hook_log (id INTEGER PRIMARY KEY) SYNC OFF",
        );
        sql(
            &hub,
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT) SYNC TWO WAY SYNC CONFLICT KEEP LATEST HISTORY CURRENT ONLY",
        );
        sql(&hub, "INSERT INTO notes VALUES (0, 'baseline')");
        let broker = InProcessBroker::new();
        let hub_identity = Arc::new(FabricIdentity::generate());
        let tenant = contextdb_core::TenantId::from("received-risk-paths");
        let server = Arc::new(
            SyncServer::with_authenticated_transport_and_identity_for_test(
                hub.clone(),
                broker.server_as(&hub_identity.node_id()),
                tenant.clone(),
                hub_identity.node_id(),
                hub_identity,
            )
            .unwrap(),
        );
        let stop = Arc::new(AtomicBool::new(false));
        let serving = runtime.spawn({
            let server = server.clone();
            let stop = stop.clone();
            async move { server.run_until(stop).await }
        });
        runtime.block_on(
            broker.wait_for_registered_route_for_test(&crate::subjects::push_subject(
                tenant.as_str(),
            )),
        );
        let identity = Arc::new(FabricIdentity::generate());
        let client = Arc::new(
            SyncClient::with_authenticated_transport_and_identity_for_test(
                edge.clone(),
                broker.client_as(&identity.node_id()),
                tenant,
                identity,
            )
            .unwrap(),
        );
        let network = Self {
            runtime,
            hub,
            edge,
            client,
            stop,
            server,
            serving,
            _directory: directory,
        };
        network.pull().unwrap();
        assert_eq!(rows(&network.edge), vec![vec![Value::Int64(0)]]);
        network
    }
    fn pull(&self) -> Result<ApplyResult> {
        self.runtime.block_on(self.client.pull_default())
    }
    fn close(self) {
        drop(self.client);
        self.stop.store(true, Ordering::SeqCst);
        self.runtime.block_on(async {
            tokio::time::timeout(BOUND, self.serving)
                .await
                .expect("server stop bounded")
                .unwrap();
        });
        drop(self.server);
        self.edge.close().unwrap();
        self.hub.close().unwrap();
    }
}

#[derive(Default)]
struct LocalCommitHook {
    db: Mutex<Weak<Database>>,
    armed: AtomicBool,
    created_local: AtomicBool,
}
impl DatabasePlugin for LocalCommitHook {
    fn on_sync_pull(&self, changes: &mut ChangeSet) -> Result<()> {
        if self.armed.load(Ordering::SeqCst)
            && changes.rows.iter().any(|row| {
                row.table == "notes"
                    && !row.deleted
                    && row.values.get("id") == Some(&Value::Int64(1))
            })
            && !self.created_local.swap(true, Ordering::SeqCst)
        {
            self.db
                .lock()
                .upgrade()
                .unwrap()
                .execute("INSERT INTO hook_log VALUES (1)", &HashMap::new())?;
            eprintln!(
                "CDB258 real pull hook committed its ordinary transaction and triggered local creation before the received indexed purge"
            );
        }
        Ok(())
    }
}

#[test]
fn received_indexed_purge_after_pull_hook_local_commit_erases_or_returns_typed_refusal() {
    bounded(|| {
        let hook = Arc::new(LocalCommitHook::default());
        let network = Network::new(hook.clone());
        *hook.db.lock() = Arc::downgrade(&network.edge);
        for db in [&network.hub, &network.edge] {
            sql(
                db,
                "CREATE TABLE erased_local (id INTEGER PRIMARY KEY, body TEXT) SYNC OFF",
            );
        }
        sql(
            &network.edge,
            "CREATE TRIGGER derive_erased_local ON hook_log WHEN INSERT",
        );
        let fired = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        network
            .edge
            .register_trigger_callback("derive_erased_local", {
                let fired = fired.clone();
                move |handle, context| {
                    handle.execute_in_tx(
                        context.tx,
                        "INSERT INTO erased_local VALUES ($id, 'private callback content')",
                        &HashMap::from([("id".into(), context.row_values["id"].clone())]),
                    )?;
                    fired.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            })
            .unwrap();
        network.edge.complete_initialization().unwrap();
        hook.armed.store(true, Ordering::SeqCst);
        sql(
            &network.hub,
            "INSERT INTO notes VALUES (1, 'incoming page unit')",
        );
        // PRIMARY KEY supplies the index. This hub instruction selects the
        // row created locally by the hook, with real committed lineage.
        sql(&network.hub, "PURGE FROM erased_local WHERE id = 1");
        let outcome = network.pull();
        assert_eq!(
            fired.load(Ordering::SeqCst),
            1,
            "real trigger created the selected life"
        );
        assert!(hook.created_local.load(Ordering::SeqCst));
        eprintln!("CDB258 received indexed purge outcome={outcome:?}");
        match outcome {
            Ok(_) => {
                assert!(
                    network
                        .edge
                        .execute("SELECT id FROM erased_local", &HashMap::new())
                        .unwrap()
                        .rows()
                        .is_empty(),
                    "a successful received purge erases the hook-created row"
                );
                assert!(
                    network
                        .edge
                        .durable_deletion_state_for_test("erased_local", &Value::Int64(1))
                        .purge_frontier
                        .is_some(),
                    "success publishes the selected life's durable purge frontier"
                );
            }
            Err(error) => {
                // Inspect admitted wrappers too; changing ownership of an
                // untyped diagnostic cannot satisfy the typed-refusal promise.
                assert!(
                    !matches!(
                        error.engine_diagnostic(),
                        Error::Other(_)
                            | Error::SyncError(_)
                            | Error::FundedEngine(_)
                            | Error::NativeEngineOther(
                                contextdb_core::read_memory::NativeRefusal::Other(_)
                            )
                    ),
                    "a received PURGE on an indexed column after a pull hook's local commit must erase the row or refuse with a typed cause: {error:?}"
                );
            }
        }
        network.close();
    });
}

// Only the receiver pull thread arms this observer, after the hub has
// committed its journal. It observes the real irreversible purge cutpoint.
thread_local! {
    static PURGE_COMMITTED: std::cell::RefCell<Option<Arc<AtomicBool>>> = const { std::cell::RefCell::new(None) };
}
pub(in crate::database) fn purge_committed_for_test() {
    PURGE_COMMITTED.with(|slot| {
        if let Some(observed) = slot.borrow().as_ref() {
            observed.store(true, Ordering::SeqCst);
        }
    });
}

#[derive(Default)]
struct ErasureObserver {
    db: Mutex<std::sync::Weak<Database>>,
    created_local: AtomicBool,
    armed: AtomicBool,
    purged: Arc<AtomicBool>,
    origin_deliveries: std::sync::atomic::AtomicUsize,
    delivery_preceded_erasure: Mutex<Vec<bool>>,
}
impl DatabasePlugin for ErasureObserver {
    fn on_sync_pull(&self, changes: &mut ChangeSet) -> Result<()> {
        if self.armed.load(Ordering::SeqCst)
            && changes.rows.iter().any(|row| {
                row.table == "notes"
                    && !row.deleted
                    && row.values.get("id") == Some(&Value::Int64(1))
            })
            && !self.created_local.swap(true, Ordering::SeqCst)
        {
            self.db
                .lock()
                .upgrade()
                .unwrap()
                .execute("INSERT INTO hook_log VALUES (1)", &HashMap::new())?;
            eprintln!(
                "CDB258 real pull hook committed its ordinary transaction and triggered local creation before the received indexed purge"
            );
        }
        Ok(())
    }

    fn post_commit(&self, ws: &WriteSet, _source: CommitSource) {
        if !self.armed.load(Ordering::SeqCst) {
            return;
        }
        self.origin_deliveries.fetch_add(
            ws.relational_inserts
                .iter()
                .filter(|(table, row)| {
                    table == "hook_log" && row.values.get("id") == Some(&Value::Int64(1))
                })
                .count(),
            Ordering::SeqCst,
        );
        for (_, row) in ws
            .relational_inserts
            .iter()
            .filter(|(table, _)| table == "erased_local")
        {
            assert_eq!(
                row.values.get("body"),
                Some(&Value::Text("private callback content".into()))
            );
            // A SQL read here could inherit an older statement snapshot.
            // Compare delivery with actual erasure publication instead.
            self.delivery_preceded_erasure
                .lock()
                .push(!self.purged.load(Ordering::SeqCst));
        }
    }
}

#[test]
fn received_indexed_purge_after_pull_hook_never_delivers_erased_callback_rows() {
    bounded(|| {
        let hook = Arc::new(ErasureObserver::default());
        let network = Network::new(hook.clone());
        *hook.db.lock() = Arc::downgrade(&network.edge);
        for db in [&network.hub, &network.edge] {
            sql(
                db,
                "CREATE TABLE erased_local (id INTEGER PRIMARY KEY, body TEXT) SYNC OFF",
            );
            assert!(db.column_has_covering_index("erased_local", "id"));
        }
        sql(
            &network.edge,
            "CREATE TRIGGER derive_erased_local ON hook_log WHEN INSERT",
        );
        let fired = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        network
            .edge
            .register_trigger_callback("derive_erased_local", {
                let fired = fired.clone();
                move |handle, context| {
                    handle.execute_in_tx(
                        context.tx,
                        "INSERT INTO erased_local VALUES ($id, 'private callback content')",
                        &HashMap::from([("id".into(), context.row_values["id"].clone())]),
                    )?;
                    fired.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            })
            .unwrap();
        network.edge.complete_initialization().unwrap();
        hook.armed.store(true, Ordering::SeqCst);
        sql(
            &network.hub,
            "INSERT INTO notes VALUES (1, 'incoming page unit')",
        );
        sql(&network.hub, "PURGE FROM erased_local WHERE id = 1");
        PURGE_COMMITTED.with(|slot| *slot.borrow_mut() = Some(hook.purged.clone()));
        let pulled = network.pull();
        PURGE_COMMITTED.with(|slot| *slot.borrow_mut() = None);
        assert_eq!(
            fired.load(Ordering::SeqCst),
            1,
            "real transactional callback created the local row"
        );
        assert!(hook.created_local.load(Ordering::SeqCst));
        eprintln!("CDB258_ERASURE indexed pull outcome={pulled:?}");
        pulled.expect("indexed purge must succeed before checking erased callback delivery");
        assert!(
            hook.purged.load(Ordering::SeqCst),
            "the actual receiver purge crossed irreversible publication"
        );
        assert!(
            network
                .edge
                .execute("SELECT id FROM erased_local", &HashMap::new())
                .unwrap()
                .rows()
                .is_empty()
        );
        assert!(
            network
                .edge
                .durable_deletion_state_for_test("erased_local", &Value::Int64(1))
                .purge_frontier
                .is_some(),
            "the selected life was actually erased"
        );
        assert_eq!(
            hook.origin_deliveries.load(Ordering::SeqCst),
            1,
            "the non-erased originating unit is delivered once; dropping the entire tail is not a repair"
        );
        assert_eq!(
            network
                .edge
                .execute("SELECT id FROM hook_log", &HashMap::new())
                .unwrap()
                .rows(),
            vec![vec![Value::Int64(1)]]
        );
        let preceded = hook.delivery_preceded_erasure.lock().clone();
        eprintln!(
            "CDB258_ERASURE delivery_preceded_irreversible_erasure={preceded:?} trigger_fired=1 purge_frontier=present final_rows=0"
        );
        network.close();
        assert!(
            preceded.iter().all(|before| *before),
            "a callback must never receive a row after the page has erased it: {preceded:?}"
        );
    });
}
