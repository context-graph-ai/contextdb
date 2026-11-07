//! Public DISCARD and authenticated received PURGE must finish while finite
//! sink callbacks perform ordinary database work, including after checkout renewal.
#![cfg(all(feature = "test-seams", feature = "sync-orchestration"))]
use super::super::received_indexed_purge_hook_tests::task;
const BOUND: Duration = Duration::from_secs(10);
use super::super::*;
use crate::transport::in_process::InProcessBroker;
use crate::{FabricIdentity, SyncClient, SyncServer};
use std::sync::mpsc;

fn sql(db: &Database, statement: &str) {
    db.execute(statement, &HashMap::new()).unwrap();
}
fn rows(db: &Database) -> Vec<Vec<Value>> {
    db.execute("SELECT id FROM notes ORDER BY id", &HashMap::new())
        .unwrap()
        .rows()
        .to_vec()
}

struct Network {
    runtime: Arc<tokio::runtime::Runtime>,
    hub: Arc<Database>,
    edge: Arc<Database>,
    client: Arc<SyncClient>,
    stop: Arc<AtomicBool>,
    server: Arc<SyncServer>,
    serving: tokio::task::JoinHandle<()>,
    hub_identity: Arc<FabricIdentity>,
    _directory: tempfile::TempDir,
}
impl Network {
    fn new(owned: bool) -> Self {
        Self::with_plugin(owned, Arc::new(crate::plugin::CorePlugin))
    }
    fn with_plugin(owned: bool, plugin: Arc<dyn crate::plugin::DatabasePlugin>) -> Self {
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
            if owned {
                Database::open_owned_for_test(&directory.path().join("edge.db"), configuration)
            } else {
                Database::open_with_options(directory.path().join("edge.db"), configuration)
            }
            .unwrap(),
        );
        assert_eq!(
            matches!(&edge.tx_mgr, DatabaseTransactionHost::Owned(_)),
            owned
        );
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
                hub_identity.clone(),
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
            hub_identity,
            _directory: directory,
        };
        network.pull().unwrap();
        assert_eq!(rows(&network.edge), vec![vec![Value::Int64(0)]]);
        network
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
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

// Received erasure and local DISCARD share the normal schema/queue paths.
// A finite callback must finish its database work while those paths contend.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn discard_cycle_with_traffic(owned: bool, renewals: usize) {
    let controller = task(move || {
        let network = Network::new(owned);
        for statement in [
            "CREATE TABLE local_notes (id INTEGER PRIMARY KEY) SYNC OFF",
            "INSERT INTO local_notes VALUES (1)",
            "CREATE TABLE traffic (id INTEGER PRIMARY KEY) SYNC OFF",
            "CREATE EVENT TYPE traffic_inserted WHEN INSERT ON traffic",
            "CREATE SINK traffic_sink TYPE callback",
            "CREATE ROUTE traffic_route EVENT traffic_inserted TO traffic_sink",
        ] {
            sql(&network.edge, statement);
        }
        sql(
            &network.hub,
            "INSERT INTO notes VALUES (1, 'received purge target')",
        );
        network.pull().unwrap();
        let (entered, entries) = mpsc::channel();
        let (release, released) = mpsc::channel();
        let (completed, completions) = mpsc::channel();
        let released = Mutex::new(released);
        let edge = Arc::downgrade(&network.edge);
        let sequence = std::sync::atomic::AtomicUsize::new(0);
        network
            .edge
            .register_sink("traffic_sink", None, move |_| {
                let id = sequence.fetch_add(1, Ordering::SeqCst);
                entered.send(id).unwrap();
                released
                    .lock()
                    .recv_timeout(BOUND * 5)
                    .expect("finite callback is released by controller");
                let result = edge.upgrade().unwrap().execute(
                    &format!("INSERT INTO hook_log VALUES ({id})"),
                    &HashMap::new(),
                );
                let success = result.is_ok();
                completed.send((id, success)).unwrap();
                result.expect("healthy callback database write succeeds");
                Ok(())
            })
            .unwrap();
        network.edge.complete_initialization().unwrap();
        // Renew actual checkout bodies before the received erasure arrives.
        // Each callback is finite and proves its database write committed.
        for id in 0..=renewals {
            sql(&network.edge, &format!("INSERT INTO traffic VALUES ({id})"));
            assert_eq!(entries.recv_timeout(BOUND).unwrap(), id);
            if id < renewals {
                release.send(()).unwrap();
                assert_eq!(completions.recv_timeout(BOUND).unwrap(), (id, true));
            }
        }
        // The active checkout captured one action. More committed work stays
        // available behind it; successful purge cannot require queue exhaustion.
        if renewals != 0 {
            for id in renewals + 1..=renewals + 8 {
                sql(&network.edge, &format!("INSERT INTO traffic VALUES ({id})"));
            }
        }
        sql(&network.hub, "PURGE FROM notes WHERE id = 1");
        // Exercise the same authoritative wrapper, preflight, and purge batch
        // entry points used by SyncClient, with the hub already authenticated
        // by the bootstrap pulls. The ordinary closure boundary coordinates
        // execution without a process-wide pause shared by unrelated stores.
        let items = network
            .hub
            .authoritative_purge_delivery_items_since(Lsn(0))
            .unwrap();
        assert_eq!(items.len(), 1);
        let hub = network.edge.retention_sync_peer().unwrap();
        let edge = network.edge.clone();
        let (entered_apply, applying) = mpsc::channel();
        let (continue_apply, continued) = mpsc::channel();
        let pulling = task(move || {
            edge.with_authoritative_hub_received_apply(&hub, true, |db| {
                entered_apply.send(()).unwrap();
                continued
                    .recv_timeout(BOUND * 5)
                    .expect("received application is released");
                let reader = db.declared_reader_memory()?;
                let mut copies = crate::vector_read_memory::admission(reader.clone());
                db.preflight_incoming_authoritative_purge_batch_while_authoritative(
                    &items,
                    &mut copies,
                    reader.clone(),
                )?;
                db.apply_incoming_authoritative_purge_batch_while_authoritative(
                    &items,
                    &mut copies,
                    reader,
                )
            })
        });
        // Queue-first admission may wait for the callback before entering the
        // apply closure; schema-first admission reaches the closure first.
        // Observe actual shipping state and release the same finite work for
        // either order. The enclosing task supplies the failure watchdog.
        loop {
            if network
                .edge
                .event_bus
                .queue_mutation_state
                .lock()
                .checkout_paused
                || applying.try_recv().is_ok()
            {
                break;
            }
            std::thread::yield_now();
        }
        let edge = network.edge.clone();
        let (started, start) = mpsc::channel();
        let discarding = task(move || {
            started.send(()).unwrap();
            edge.execute("DISCARD FROM local_notes WHERE id = 1", &HashMap::new())
        });
        start.recv_timeout(BOUND).unwrap();
        release.send(()).unwrap();
        continue_apply.send(()).unwrap();
        // Later queued callbacks are finite too, without controller round trips.
        for _ in 0..8 {
            let _ = release.send(());
        }
        let discard = discarding.result.recv_timeout(BOUND);
        let pull = pulling.result.recv_timeout(BOUND);
        // Detach blocked operations so a failing liveness assertion is reported
        // without waiting forever in a DB/runtime destructor or thread join.
        assert!(
            discard.is_ok() && pull.is_ok(),
            "public DISCARD and received PURGE must both finish: owned={owned} renewals={renewals} discard={discard:?} pull={pull:?}"
        );
        discarding.join_completed();
        pulling.join_completed();
        assert_eq!(
            discard
                .unwrap()
                .expect("eligible SYNC OFF DISCARD succeeds")
                .rows_affected(),
            1
        );
        pull.unwrap()
            .expect("authenticated received PURGE succeeds");
        assert_eq!(completions.recv_timeout(BOUND).unwrap(), (renewals, true));
        assert_eq!(rows(&network.edge), vec![vec![Value::Int64(0)]]);
        assert!(
            network
                .edge
                .execute("SELECT id FROM local_notes", &HashMap::new())
                .unwrap()
                .rows()
                .is_empty()
        );
        assert!(
            network
                .edge
                .durable_deletion_state_for_test("notes", &Value::Int64(1))
                .purge_frontier
                .is_some()
        );
        network.close();
    });
    controller
        .result
        .recv_timeout(BOUND * 5)
        .expect("cycle controller is bounded, including failure cleanup");
    controller.join_completed();
}

#[test]
fn received_purge_and_public_discard_finish_legacy() {
    discard_cycle_with_traffic(false, 0);
}
#[test]
#[ignore = "owned host cannot apply pulled rows"]
fn received_purge_and_public_discard_finish_owned() {
    discard_cycle_with_traffic(true, 0);
}
#[test]
fn received_purge_progresses_under_healthy_checkout_renewal_legacy() {
    discard_cycle_with_traffic(false, 4);
}
#[test]
#[ignore = "owned host cannot apply pulled rows"]
fn received_purge_progresses_under_healthy_checkout_renewal_owned() {
    discard_cycle_with_traffic(true, 4);
}

#[test]
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn ordinary_received_page_does_not_drain_legacy_checkout() {
    let controller = task(|| {
        let network = Network::new(false);
        for statement in [
            "CREATE TABLE traffic (id INTEGER PRIMARY KEY) SYNC OFF",
            "CREATE EVENT TYPE traffic_inserted WHEN INSERT ON traffic",
            "CREATE SINK traffic_sink TYPE callback",
            "CREATE ROUTE traffic_route EVENT traffic_inserted TO traffic_sink",
        ] {
            sql(&network.edge, statement);
        }
        let (entered, entries) = mpsc::channel();
        let (release, released) = mpsc::channel();
        let (completed, completions) = mpsc::channel();
        let released = Mutex::new(released);
        let edge = Arc::downgrade(&network.edge);
        network
            .edge
            .register_sink("traffic_sink", None, move |_| {
                entered.send(()).unwrap();
                released
                    .lock()
                    .recv_timeout(BOUND * 5)
                    .expect("ordinary-page callback has a finite release");
                let result = edge
                    .upgrade()
                    .unwrap()
                    .execute("INSERT INTO hook_log VALUES (1)", &HashMap::new());
                completed.send(result.is_ok()).unwrap();
                result.expect("ordinary-page callback database write succeeds");
                Ok(())
            })
            .unwrap();
        network.edge.complete_initialization().unwrap();
        sql(&network.edge, "INSERT INTO traffic VALUES (1)");
        entries
            .recv_timeout(BOUND)
            .expect("real checkout entered its callback");
        sql(
            &network.hub,
            "INSERT INTO notes VALUES (1, 'ordinary page')",
        );
        assert!(
            network
                .hub
                .authoritative_purge_delivery_items_since(Lsn(0))
                .unwrap()
                .is_empty(),
            "ordinary page has no purge deliveries"
        );
        let runtime = network.runtime.clone();
        let client = network.client.clone();
        let pulling = task(move || runtime.block_on(client.pull_default()));
        // The callback cannot acknowledge checkout until released below. A
        // completed real pull therefore proves this page did not drain it.
        let pull = pulling.result.recv_timeout(BOUND);
        assert!(
            matches!(completions.try_recv(), Err(mpsc::TryRecvError::Empty)),
            "checkout callback must still be held while ordinary pull completes"
        );
        release.send(()).unwrap();
        pull.expect("ordinary received page must finish without draining active checkout")
            .expect("ordinary received page applies successfully");
        pulling.join_completed();
        assert!(
            completions
                .recv_timeout(BOUND)
                .expect("released checkout callback finishes its database write")
        );
        assert_eq!(
            rows(&network.edge),
            vec![vec![Value::Int64(0)], vec![Value::Int64(1)]]
        );
        assert_eq!(
            network
                .edge
                .execute("SELECT id FROM hook_log", &HashMap::new())
                .unwrap()
                .rows(),
            &[vec![Value::Int64(1)]]
        );
        network.close();
    });
    controller
        .result
        .recv_timeout(BOUND * 5)
        .expect("ordinary-page controller is bounded, including failure cleanup");
    controller.join_completed();
}

#[test]
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn failed_received_purge_then_local_discard_keeps_delivery_flowing_legacy() {
    let controller = task(|| {
        let network = Network::new(false);
        for statement in [
            "CREATE TABLE local_notes (id INTEGER PRIMARY KEY) SYNC OFF",
            "INSERT INTO local_notes VALUES (1)",
            "CREATE TABLE traffic (id INTEGER PRIMARY KEY) SYNC OFF",
            "CREATE EVENT TYPE traffic_inserted WHEN INSERT ON traffic",
            "CREATE SINK traffic_sink TYPE callback",
            "CREATE ROUTE traffic_route EVENT traffic_inserted TO traffic_sink",
        ] {
            sql(&network.edge, statement);
        }
        let (delivered, deliveries) = mpsc::channel();
        network
            .edge
            .register_sink("traffic_sink", None, move |event| {
                delivered.send(event.row_values.get("id").cloned()).unwrap();
                Ok(())
            })
            .unwrap();
        network.edge.complete_initialization().unwrap();
        // Existing dispatcher drive holds automatic workers and later performs
        // the actual checkout/delivery; no negative timing assertion is needed.
        let drive = network.edge.sink_dispatch_drive_for_test().unwrap();
        sql(&network.edge, "INSERT INTO traffic VALUES (7)");
        let prefix = drive.committed_prefix_for_test();
        assert_eq!(prefix.entries_for_test().len(), 1);
        network
            .edge
            .execute(
                "UPDATE notes SET body = $body WHERE id = 0",
                &HashMap::from([("body".into(), Value::Text("wide selected row".repeat(8192)))]),
            )
            .expect("a live wide descendant is committed before the hub erases its lineage");
        // The shipping planner must refuse when its supplied reader budget
        // cannot admit the incoming purge, before entering a commit kernel.
        sql(&network.hub, "PURGE FROM notes WHERE id = 0");
        let items = network
            .hub
            .authoritative_purge_delivery_items_since(Lsn(0))
            .unwrap();
        assert!(!items.is_empty(), "page carries an actual hub purge");
        let hub = network.edge.retention_sync_peer().unwrap();
        let limited = crate::read_image_memory::ImageMemory::new(Some(4096))
            .expect("small reader owner is valid before purge preflight");
        let failed = network
            .edge
            .with_authoritative_hub_received_apply(&hub, true, |db| {
                let reader: contextdb_core::read_memory::ReadMemoryOwner = limited.clone().into();
                let mut copies = crate::vector_read_memory::admission(reader.clone());
                db.preflight_incoming_authoritative_purge_batch_while_authoritative(
                    &items,
                    &mut copies,
                    reader,
                )
            });
        assert!(
            matches!(failed, Err(Error::MemoryBudgetExceeded { .. })),
            "real received purge must refuse its reader budget during preflight, before promotion: {failed:?}"
        );
        assert_eq!(rows(&network.edge), vec![vec![Value::Int64(0)]]);
        assert!(
            network
                .edge
                .durable_deletion_state_for_test("notes", &Value::Int64(0))
                .purge_frontier
                .is_none()
        );
        assert_eq!(
            network
                .edge
                .execute("DISCARD FROM local_notes WHERE id = 1", &HashMap::new())
                .expect("later local DISCARD commits")
                .rows_affected(),
            1
        );
        assert!(
            network
                .edge
                .execute("SELECT id FROM local_notes", &HashMap::new())
                .unwrap()
                .rows()
                .is_empty()
        );
        let receipt = drive
            .drive_through_for_test(prefix)
            .expect("pending delivery can be checked out after refused purge and local DISCARD");
        assert!(receipt.is_complete_for_test());
        assert_eq!(receipt.processed_positions.len(), 1);
        assert_eq!(
            deliveries.recv_timeout(BOUND).unwrap(),
            Some(Value::Int64(7))
        );
        assert!(matches!(
            deliveries.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
        drop(drive);
        network.close();
    });
    controller
        .result
        .recv_timeout(BOUND * 5)
        .expect("after a failed received purge, local DISCARD must leave sink delivery flowing");
    controller.join_completed();
}

struct PagePausePlugin {
    armed: AtomicBool,
    entered: mpsc::Sender<()>,
    release: Mutex<mpsc::Receiver<()>>,
    add_ddl: bool,
}
impl crate::plugin::DatabasePlugin for PagePausePlugin {
    fn on_sync_pull(&self, changes: &mut ChangeSet) -> Result<()> {
        if changes.rows.iter().any(|row| row.table == "notes")
            && self.armed.swap(false, Ordering::SeqCst)
        {
            self.entered.send(()).unwrap();
            self.release
                .lock()
                .recv_timeout(BOUND * 5)
                .expect("page plugin has a finite release");
            if self.add_ddl {
                changes.ddl.push(DdlChange::CreateTable {
                    name: "plugin_table".into(),
                    columns: vec![("id".into(), "INTEGER PRIMARY KEY".into())],
                    constraints: Vec::new(),
                    foreign_keys: Vec::new(),
                    composite_foreign_keys: Vec::new(),
                    composite_unique: Vec::new(),
                });
                changes.ddl_lsn.push(changes.rows[0].lsn);
            }
        }
        Ok(())
    }
}

fn paused_page(
    add_ddl: bool,
) -> (
    Network,
    Arc<PagePausePlugin>,
    mpsc::Receiver<()>,
    mpsc::Sender<()>,
) {
    let (entered, entries) = mpsc::channel();
    let (release, released) = mpsc::channel();
    let plugin = Arc::new(PagePausePlugin {
        armed: AtomicBool::new(false),
        entered,
        release: Mutex::new(released),
        add_ddl,
    });
    let network = Network::with_plugin(false, plugin.clone());
    sql(
        &network.hub,
        "INSERT INTO notes VALUES (1, 'data-only unit')",
    );
    plugin.armed.store(true, Ordering::SeqCst);
    (network, plugin, entries, release)
}

#[test]
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn ordinary_received_page_keeps_select_and_current_lsn_answering_legacy() {
    let controller = task(|| {
        let (network, _plugin, entered, release) = paused_page(false);
        let before_apply = network.edge.current_lsn();
        let runtime = network.runtime.clone();
        let client = network.client.clone();
        let pulling = task(move || runtime.block_on(client.pull_default()));
        entered
            .recv_timeout(BOUND)
            .expect("actual received data unit reaches plugin");
        let edge = network.edge.clone();
        let selecting = task(move || rows(&edge));
        let edge = network.edge.clone();
        let reading_lsn = task(move || edge.current_lsn());
        let selected = selecting.result.recv_timeout(BOUND);
        let lsn = reading_lsn.result.recv_timeout(BOUND);
        release.send(()).unwrap();
        assert!(
            selected.is_ok() && lsn.is_ok(),
            "SELECT and current_lsn must answer while an ordinary received page is applying: select={selected:?} lsn={lsn:?}"
        );
        assert_eq!(selected.unwrap(), vec![vec![Value::Int64(0)]]);
        assert_eq!(lsn.unwrap(), before_apply);
        selecting.join_completed();
        reading_lsn.join_completed();
        pulling
            .result
            .recv_timeout(BOUND)
            .unwrap()
            .expect("received data page applies");
        pulling.join_completed();
        assert_eq!(
            rows(&network.edge),
            vec![vec![Value::Int64(0)], vec![Value::Int64(1)]]
        );
        network.close();
    });
    controller
        .result
        .recv_timeout(BOUND * 5)
        .expect("ordinary-page readers are bounded");
    controller.join_completed();
}

#[test]
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn plugin_added_ddl_refusal_keeps_destination_getter_live_legacy() {
    let controller = task(|| {
        // Authored DDL is the positive lock-order control; the plugin-added
        // attempted data-to-DDL change below must refuse without blocking the getter.
        {
            let network = Network::new(false);
            sql(
                &network.hub,
                "CREATE TABLE authored_table (id INTEGER PRIMARY KEY) SYNC TWO WAY",
            );
            let expected = network.edge.retention_sync_peer();
            let edge = network.edge.clone();
            let schema = edge.enter_schema_publication_gate(false);
            let runtime = network.runtime.clone();
            let client = network.client.clone();
            let (started, starts) = mpsc::channel();
            let pulling = task(move || {
                started.send(()).unwrap();
                runtime.block_on(client.pull_default())
            });
            starts.recv_timeout(BOUND).unwrap();
            assert_eq!(
                edge.retention_sync_peer(),
                expected,
                "destination getter with schema read must finish beside an authored DDL unit"
            );
            drop(schema);
            pulling
                .result
                .recv_timeout(BOUND)
                .unwrap()
                .expect("authored DDL applies after getter releases schema");
            pulling.join_completed();
            assert!(network.edge.table_meta("authored_table").is_some());
            network.close();
        }
        let (network, _plugin, entered, release) = paused_page(true);
        let hub = network.edge.retention_sync_peer().unwrap();
        let runtime = network.runtime.clone();
        let client = network.client.clone();
        let pulling = task(move || runtime.block_on(client.pull_default()));
        entered
            .recv_timeout(BOUND)
            .expect("data unit reaches DDL-adding plugin");
        let edge = network.edge.clone();
        let (held, schema_held) = mpsc::channel();
        let getter = task(move || {
            let _schema = edge.enter_schema_publication_gate(false);
            held.send(()).unwrap();
            edge.retention_sync_peer()
        });
        let shared = schema_held.recv_timeout(BOUND);
        release.send(()).unwrap();
        assert!(
            shared.is_ok(),
            "destination getter must acquire schema read while the plugin sees a data-only unit: {shared:?}"
        );
        assert_eq!(
            getter
                .result
                .recv_timeout(BOUND)
                .expect("destination getter must finish while plugin-added DDL is validated"),
            Some(hub)
        );
        getter.join_completed();
        // This authenticated data-only unit has no whole-unit schema plan.
        // The shipping lineage validator refuses plugin-added DDL before a
        // schema upgrade. Keep both operations bounded and require that exact
        // refusal, rather than inventing permission to publish the new schema.
        let refused = pulling
            .result
            .recv_timeout(BOUND)
            .expect("plugin-added DDL refusal must finish after destination getter");
        pulling.join_completed();
        assert!(
            matches!(refused, Err(Error::SyncError(ref detail))
                if detail.ends_with("received rows beside schema DDL require an authenticated whole-unit schema plan")),
            "unattested plugin-added DDL must retain its existing whole-unit-plan refusal: {refused:?}"
        );
        assert!(
            network.edge.table_meta("plugin_table").is_none(),
            "refused plugin schema must not be published"
        );
        assert_eq!(
            rows(&network.edge),
            vec![vec![Value::Int64(0)]],
            "the refused unit's data must not be published"
        );
        network.close();
    });
    controller
        .result
        .recv_timeout(BOUND * 5)
        .expect("DDL/getter pair is bounded");
    controller.join_completed();
}

type UnitLineages = Vec<(String, NaturalKey, Lsn, crate::protocol::WireRowLineage)>;
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn page_unit(network: &Network, after: Lsn) -> (ChangeSet, UnitLineages) {
    let changes = network.hub.changes_since(after).unwrap();
    assert_eq!(changes.rows.len(), 1);
    assert!(changes.ddl.is_empty());
    let tenant = contextdb_core::TenantId::from("received-risk-paths");
    let reader = network.hub.declared_reader_memory().unwrap();
    let incarnation = network
        .hub
        .existing_sync_incarnation(&tenant, reader.clone())
        .unwrap()
        .unwrap();
    let mut lineages = network
        .hub
        .outbound_row_lineages(
            &changes,
            &tenant,
            &network.hub_identity.node_id(),
            incarnation,
            &|bytes| Ok(network.hub_identity.sign_lineage(bytes)),
            reader,
        )
        .unwrap();
    let rows = changes
        .rows
        .iter()
        .map(|row| {
            let lineage = lineages
                .remove(&(
                    row.table.clone(),
                    rmp_serde::to_vec(&row.natural_key).unwrap(),
                    row.lsn,
                ))
                .unwrap();
            (row.table.clone(), row.natural_key.clone(), row.lsn, lineage)
        })
        .collect();
    (changes, rows)
}
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn apply_page_unit(
    db: &Database,
    hub: &str,
    changes: ChangeSet,
    lineages: &UnitLineages,
) -> Result<ApplyResult> {
    let reader = db.declared_reader_memory()?;
    let mut copies = crate::vector_read_memory::admission(reader.clone());
    db.apply_authenticated_received_changes_with_lineages_from_hub(
        hub,
        changes,
        &HashMap::new(),
        SyncAdoption::Continuing,
        None,
        &contextdb_core::TenantId::from("received-risk-paths"),
        lineages,
        None,
        false,
        &mut copies,
        reader,
    )
}

#[test]
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn mid_page_hub_rebind_keeps_earlier_unit_and_refuses_old_hub_effects_legacy() {
    let controller = task(|| {
        let network = Network::new(false);
        let before = network.hub.current_lsn();
        sql(&network.hub, "INSERT INTO notes VALUES (1, 'first unit')");
        let first = page_unit(&network, before);
        let before = network.hub.current_lsn();
        sql(&network.hub, "INSERT INTO notes VALUES (2, 'later unit')");
        let second = page_unit(&network, before);
        sql(&network.hub, "PURGE FROM notes WHERE id = 0");
        let purge_items = network
            .hub
            .authoritative_purge_delivery_items_since(Lsn(0))
            .unwrap();
        assert!(!purge_items.is_empty());

        let old_hub = network.edge.retention_sync_peer().unwrap();
        let edge = network.edge.clone();
        let old = old_hub.clone();
        let (committed, commits) = mpsc::channel();
        let (resume, resumed) = mpsc::channel();
        let effects = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let attempted = effects.clone();
        let applying = task(move || {
            edge.with_authoritative_hub_received_apply(&old, true, |db| {
                assert_eq!(
                    apply_page_unit(db, &old, first.0, &first.1)?.applied_rows,
                    1
                );
                committed.send(()).unwrap();
                resumed
                    .recv_timeout(BOUND * 5)
                    .expect("page resumes after rebind attempt");
                let later = apply_page_unit(db, &old, second.0, &second.1);
                let reconciliation: Result<()> = db.with_authoritative_hub_reply(&old, |db| {
                    attempted.fetch_add(1, Ordering::SeqCst);
                    db.refresh_confirmed_pending_rows_with_read_memory(
                        &[],
                        &HashMap::new(),
                        db.declared_reader_memory()?,
                    )?;
                    Ok(())
                });
                let purge: Result<()> = db.with_authoritative_hub_reply(&old, |db| {
                    attempted.fetch_add(1, Ordering::SeqCst);
                    let reader = db.declared_reader_memory()?;
                    let mut copies = crate::vector_read_memory::admission(reader.clone());
                    db.preflight_incoming_authoritative_purge_batch_while_authoritative(
                        &purge_items,
                        &mut copies,
                        reader.clone(),
                    )?;
                    db.apply_incoming_authoritative_purge_batch_while_authoritative(
                        &purge_items,
                        &mut copies,
                        reader,
                    )
                });
                Ok((later, reconciliation, purge))
            })
        });
        commits
            .recv_timeout(BOUND)
            .expect("first unit committed before hub switch");
        let edge = network.edge.clone();
        let new_hub = FabricIdentity::generate().node_id();
        let expected = new_hub.clone();
        let rebinding = task(move || {
            edge.change_retention_sync_peer(
                &contextdb_core::TenantId::from("received-risk-paths"),
                &new_hub,
            )
        });
        let rebound = rebinding.result.recv_timeout(BOUND);
        resume.send(()).unwrap();
        assert!(
            rebound.is_ok(),
            "destination rebind must finish between received units, while the page remains open: {rebound:?}"
        );
        rebound
            .unwrap()
            .expect("public destination change succeeds");
        rebinding.join_completed();
        let (later, reconciliation, purge) = applying
            .result
            .recv_timeout(BOUND)
            .expect("old page finishes with refusals after rebind")
            .unwrap();
        applying.join_completed();
        for refused in [later.map(|_| ()), reconciliation, purge] {
            assert!(
                matches!(refused, Err(Error::SyncError(_))),
                "later old-hub effects must refuse: {refused:?}"
            );
        }
        assert_eq!(effects.load(Ordering::SeqCst), 0);
        assert_eq!(network.edge.retention_sync_peer(), Some(expected));
        assert_eq!(
            rows(&network.edge),
            vec![vec![Value::Int64(0)], vec![Value::Int64(1)]]
        );
        network.close();
    });
    controller
        .result
        .recv_timeout(BOUND * 5)
        .expect("mid-page rebind is bounded");
    controller.join_completed();
}
