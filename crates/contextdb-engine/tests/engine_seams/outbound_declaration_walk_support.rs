//! Shared setup for the rows proving that an outbound push or pending read
//! holds none of the received declarations it suppresses.
//!
//! Every row runs at two history sizes and compares them. A run is
//! deterministic: fixed fabric seeds, pinned sync incarnations and one fixed
//! wall clock, because the push envelope encodes each payload byte at or above
//! 0x80 in two bytes and so moves with any random or clock-derived byte.
#![allow(
    dead_code,
    reason = "each row uses the part of the shared setup it needs"
)]

use contextdb_core::{Incarnation, Lsn, TenantId, Wallclock, WallclockTestClockGuard};
use contextdb_engine::database::DdlOccurrenceForTest;
use contextdb_engine::outbound_ddl_test_seams::{OutboundDdlWork, observe};
use contextdb_engine::{Database, DatabaseOpenOptions, MaintenancePolicy};
use contextdb_server::{FabricIdentity, InProcessBroker, SyncClient, SyncServer};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// The two declaration-history sizes every row compares.
pub(crate) const SIZES: [usize; 2] = [16, 1_024];

const FIXED_WALLCLOCK_MS: u64 = 1_800_000_000_000;

pub(crate) fn tenant() -> TenantId {
    TenantId::from("outbound-declaration-walk")
}

/// One fixed clock on this thread and every runtime worker.
pub(crate) fn fixed_clock() -> WallclockTestClockGuard {
    Wallclock::test_clock_guard(|| FIXED_WALLCLOCK_MS)
}

pub(crate) fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .on_thread_start(|| Wallclock::set_test_clock(|| FIXED_WALLCLOCK_MS))
        .enable_all()
        .build()
        .unwrap()
}

pub(crate) fn open(path: &Path) -> Arc<Database> {
    Arc::new(
        Database::open_with_options(
            path,
            DatabaseOpenOptions {
                memory_limit: Some(64 << 20),
                maintenance_policy: MaintenancePolicy::CallerDriven,
                ..Default::default()
            },
        )
        .unwrap(),
    )
}

pub(crate) fn sql(db: &Database, statement: &str) {
    db.execute(statement, &HashMap::new())
        .unwrap_or_else(|error| panic!("{statement}: {error:?}"));
}

/// One transaction holding every statement, so all of them share one LSN.
pub(crate) fn one_commit(db: &Database, statements: &[String]) {
    sql(db, "BEGIN");
    for statement in statements {
        sql(db, statement);
    }
    sql(db, "COMMIT");
}

/// `declarations` unrelated declarations, four per cycle, each its own commit.
pub(crate) fn churn(db: &Database, prefix: &str, declarations: usize) {
    assert_eq!(declarations % 4, 0);
    for cycle in 0..declarations / 4 {
        let table = format!("{prefix}_{cycle}");
        sql(
            db,
            &format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY) SYNC OFF"),
        );
        sql(db, &format!("ALTER TABLE {table} ADD COLUMN spare INTEGER"));
        sql(db, &format!("ALTER TABLE {table} SET SYNC OFF"));
        sql(db, &format!("DROP TABLE {table}"));
    }
}

/// The statements of one large schema commit: `declarations` declarations on
/// one table whose shape stays small (a transaction cannot drop a table).
pub(crate) fn large_commit(prefix: &str, declarations: usize) -> Vec<String> {
    let table = format!("{prefix}_table");
    std::iter::once(format!(
        "CREATE TABLE {table} (id INTEGER PRIMARY KEY) SYNC OFF"
    ))
    .chain((1..declarations).map(|index| {
        if index % 2 == 1 {
            format!("ALTER TABLE {table} ADD COLUMN spare INTEGER")
        } else {
            format!("ALTER TABLE {table} DROP COLUMN spare")
        }
    }))
    .collect()
}

pub(crate) fn ids(db: &Database, table: &str) -> Vec<i64> {
    db.execute(
        &format!("SELECT id FROM {table} ORDER BY id"),
        &HashMap::new(),
    )
    .unwrap()
    .rows()
    .iter()
    .map(|row| match &row[0] {
        contextdb_core::Value::Int64(id) => *id,
        other => panic!("{table}.id is an integer, not {other:?}"),
    })
    .collect()
}

pub(crate) fn occurrences(db: &Database) -> Vec<DdlOccurrenceForTest> {
    db.ddl_occurrences_for_test()
        .expect("read the durable declaration occurrences")
}

/// The local source LSNs whose every occurrence is marked received, with the
/// group sizes, in history order.
pub(crate) fn received_groups(db: &Database) -> Vec<(Lsn, u32)> {
    let mut groups = Vec::<(Lsn, u32, bool)>::new();
    for occurrence in occurrences(db) {
        match groups.last_mut() {
            Some((lsn, size, all)) if *lsn == occurrence.lsn => {
                *size += 1;
                *all &= occurrence.received;
            }
            _ => groups.push((occurrence.lsn, 1, occurrence.received)),
        }
    }
    groups
        .into_iter()
        .filter(|(_, _, all)| *all)
        .map(|(lsn, size, _)| (lsn, size))
        .collect()
}

/// A fabric identity from a fixed seed.
fn fixed_identity(directory: &Path, name: &str, byte: u8) -> Arc<FabricIdentity> {
    let path = directory.join(format!("{name}.identity"));
    std::fs::write(&path, [byte; 32]).unwrap();
    Arc::new(FabricIdentity::load_or_generate(&path).unwrap())
}

/// One store with a fixed identity and a pinned sync incarnation.
pub(crate) struct Peer {
    pub(crate) db: Arc<Database>,
    pub(crate) path: PathBuf,
    pub(crate) identity: Arc<FabricIdentity>,
}

impl Peer {
    pub(crate) fn new(directory: &Path, name: &str, seed: u8) -> Self {
        let path = directory.join(format!("{name}.redb"));
        let db = open(&path);
        db.pin_sync_incarnation_for_test(
            &tenant(),
            Incarnation(0x5eed_0000_0000_0000_0000_0000_0000_0000 | u128::from(seed)),
        )
        .unwrap();
        Self {
            db,
            path,
            identity: fixed_identity(directory, name, seed),
        }
    }

    pub(crate) fn node_id(&self) -> String {
        self.identity.node_id().to_string()
    }
}

/// A hub serving one store over the in-process authenticated transport.
pub(crate) struct Hub {
    stop: Arc<AtomicBool>,
    task: Option<tokio::task::JoinHandle<()>>,
    server: Option<Arc<SyncServer>>,
}

impl Hub {
    pub(crate) fn serve(
        runtime: &tokio::runtime::Runtime,
        broker: &InProcessBroker,
        hub: &Peer,
    ) -> Self {
        let node = hub.identity.node_id();
        let server = Arc::new(
            SyncServer::with_authenticated_transport_and_identity_for_test(
                hub.db.clone(),
                broker.server_as(&node),
                tenant(),
                node,
                hub.identity.clone(),
            )
            .expect("construct the authenticated sync endpoint"),
        );
        let stop = Arc::new(AtomicBool::new(false));
        let task = runtime.spawn({
            let server = server.clone();
            let stop = stop.clone();
            async move {
                server.run_until(stop).await;
            }
        });
        runtime.block_on(broker.wait_for_registered_route_for_test(
            &contextdb_server::subjects::push_subject(tenant().as_str()),
        ));
        Self {
            stop,
            task: Some(task),
            server: Some(server),
        }
    }

    pub(crate) fn stop(mut self, runtime: &tokio::runtime::Runtime) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(task) = self.task.take() {
            runtime.block_on(task).unwrap();
        }
        drop(self.server.take());
    }
}

pub(crate) fn client(broker: &InProcessBroker, edge: &Peer) -> SyncClient {
    SyncClient::with_authenticated_transport_and_identity_for_test(
        edge.db.clone(),
        broker.client_as(&edge.identity.node_id()),
        tenant(),
        edge.identity.clone(),
    )
    .expect("construct the authenticated sync endpoint")
}

/// One measured outbound operation on `edge`: its result, the accountant peak
/// above the baseline, and what the outbound declaration path did.
pub(crate) struct Measured<T> {
    pub(crate) value: T,
    pub(crate) peak: usize,
    pub(crate) work: OutboundDdlWork,
}

pub(crate) fn measure<T>(edge: &Database, operation: impl FnOnce() -> T) -> Measured<T> {
    edge.accountant().reset_high_water_for_test();
    let baseline = edge.accountant().usage().used;
    let (value, work) = observe(operation);
    let peak = edge
        .accountant()
        .high_water_for_test()
        .checked_sub(baseline)
        .unwrap();
    Measured { value, peak, work }
}

/// The owner-level facts every row asserts about one measured extraction that
/// suppressed received declarations.
pub(crate) fn assert_suppression_holds_nothing(what: &str, size: usize, work: &OutboundDdlWork) {
    assert!(
        work.suppressed > 0,
        "{what} at {size}: the measured extraction must actually suppress received declarations: {work:?}"
    );
    assert_eq!(
        work.suppressed_output_peak_bytes, 0,
        "{what} at {size}: no suppressed declaration may sit in outbound output: {work:?}"
    );
    assert!(
        work.suppressed_held_peak <= 1,
        "{what} at {size}: at most one suppressed declaration window may be alive at once: {work:?}"
    );
    assert_no_fallback(what, size, work);
}

/// The outbound fixture path never reaches a whole-history collector, and a
/// file store prices no resident declaration suffix.
pub(crate) fn assert_no_fallback(what: &str, size: usize, work: &OutboundDdlWork) {
    assert_eq!(
        work.fallback_collector_hits, [0; 4],
        "{what} at {size}: the outbound path reached a whole-history collector: {work:?}"
    );
    assert_eq!(
        work.resident_ddl_priced_bytes, 0,
        "{what} at {size}: a file store's outbound read priced a resident declaration suffix: {work:?}"
    );
}

/// The facts compared across the two history sizes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Comparable {
    pub(crate) peak: usize,
    pub(crate) validation_records_peak: u64,
}

pub(crate) fn comparable<T>(measured: &Measured<T>) -> Comparable {
    Comparable {
        peak: measured.peak,
        validation_records_peak: measured.work.validation_records_peak,
    }
}

/// Print the receipt line for one measured extraction.
pub(crate) fn receipt<T>(row: &str, size: usize, measured: &Measured<T>) {
    eprintln!(
        "receipt {row} size={size} peak={} walked={} collected={} suppressed={} \
         suppressed_held_peak={} suppressed_output_peak_bytes={} validation_records_peak={} \
         resident_ddl_priced_bytes={} reupload_epoch_reads={} fallback={:?}",
        measured.peak,
        measured.work.walked,
        measured.work.collected,
        measured.work.suppressed,
        measured.work.suppressed_held_peak,
        measured.work.suppressed_output_peak_bytes,
        measured.work.validation_records_peak,
        measured.work.resident_ddl_priced_bytes,
        measured.work.reupload_epoch_reads,
        measured.work.fallback_collector_hits,
    );
}

pub(crate) fn assert_same_across_sizes(what: &str, samples: &[Comparable]) {
    assert_eq!(
        samples[0], samples[1],
        "{what}: the outbound work must not grow with received declaration history ({SIZES:?})"
    );
}

impl Peer {
    /// Reopen a store this setup created, keeping its identity and pinned
    /// incarnation.
    pub(crate) fn reopen(directory: &Path, name: &str) -> Self {
        let path = directory.join(format!("{name}.redb"));
        let identity_path = directory.join(format!("{name}.identity"));
        Self {
            db: open(&path),
            identity: Arc::new(FabricIdentity::load_or_generate(&identity_path).unwrap()),
            path,
        }
    }

    pub(crate) fn close(self) {
        let db = self.db;
        db.close().unwrap();
        drop(db);
    }
}

/// Copy every file of a closed setup into a fresh directory.
pub(crate) fn copy_setup(from: &Path, to: &Path) {
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            std::fs::create_dir_all(&target).unwrap();
            copy_setup(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target).unwrap();
        }
    }
}

/// An edge and its hub, connected and serving.
pub(crate) struct Network {
    pub(crate) edge: Peer,
    pub(crate) hub: Peer,
    pub(crate) broker: InProcessBroker,
    pub(crate) server: Option<Hub>,
    pub(crate) client: SyncClient,
}

impl Network {
    pub(crate) fn connect(runtime: &tokio::runtime::Runtime, edge: Peer, hub: Peer) -> Self {
        let broker = InProcessBroker::new();
        let server = Hub::serve(runtime, &broker, &hub);
        let client = client(&broker, &edge);
        Self {
            edge,
            hub,
            broker,
            server: Some(server),
            client,
        }
    }

    pub(crate) fn close(mut self, runtime: &tokio::runtime::Runtime) {
        drop(self.client);
        if let Some(server) = self.server.take() {
            server.stop(runtime);
        }
        self.edge.close();
        self.hub.close();
    }
}

/// The hub's one multi-statement schema commit, so the edge receives at least
/// one received group of several occurrences.
pub(crate) fn hub_unit() -> Vec<String> {
    large_commit("hub_unit", 3)
}

/// An edge that authored `notes` and pushed it, while its hub authored `size`
/// unrelated declarations and one multi-statement unit. The edge then pulled:
/// its own `notes` echoed back plus everything the hub authored, all marked
/// received above its push frontier. `before_pull` runs on the edge between
/// the push and the pull.
pub(crate) fn pulled_edge(
    runtime: &tokio::runtime::Runtime,
    directory: &Path,
    size: usize,
    before_pull: &dyn Fn(&Database),
) -> Network {
    let edge = Peer::new(directory, "edge", 0x11);
    let hub = Peer::new(directory, "hub", 0x22);
    sql(
        &edge.db,
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT) SYNC TWO WAY",
    );
    sql(&edge.db, "INSERT INTO notes VALUES (1, 'first')");
    let network = Network::connect(runtime, edge, hub);
    runtime.block_on(network.client.push()).unwrap();
    churn(&network.hub.db, "unrelated", size);
    one_commit(&network.hub.db, &hub_unit());
    before_pull(&network.edge.db);
    runtime.block_on(network.client.pull_default()).unwrap();
    network
}

/// The last received group of more than one occurrence.
pub(crate) fn last_multi_occurrence_received_group(db: &Database) -> (Lsn, u32) {
    *received_groups(db)
        .iter()
        .rev()
        .find(|(_, size)| *size > 1)
        .expect("the hub's multi-statement unit arrives as one received group")
}

/// Where the fixed two-declaration `prefix_a` schema an edge holds at its move
/// comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Prefix {
    /// The first hub authors it and the edge pulls it, so it is received.
    ReceivedFromTheFirstHub,
    /// The edge authors it, so it is local.
    AuthoredByTheEdge,
}

/// An edge that synced `notes` with a first hub, holds the fixed
/// two-declaration `prefix_a` schema, then moved to a second hub. The move's
/// frontier F is the edge's LSN at the move, so the prefix sits at or below F.
/// The edge then pulls the second hub's `size` declarations, which arrive above
/// F. `after_move` runs on the edge after that pull.
pub(crate) struct MovedEdge {
    pub(crate) network: Network,
    pub(crate) frontier: Lsn,
    pub(crate) first_hub_node: String,
}

pub(crate) fn moved_edge(
    runtime: &tokio::runtime::Runtime,
    directory: &Path,
    size: usize,
    prefix: Prefix,
    after_move: &dyn Fn(&Database),
) -> MovedEdge {
    let edge = Peer::new(directory, "edge", 0x11);
    let first = Peer::new(directory, "first_hub", 0x22);
    let second = Peer::new(directory, "second_hub", 0x33);
    let first_hub_node = first.node_id();
    sql(
        &edge.db,
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT) SYNC TWO WAY",
    );
    sql(&edge.db, "INSERT INTO notes VALUES (1, 'first')");
    let network = Network::connect(runtime, edge, first);
    runtime.block_on(network.client.push()).unwrap();
    let author = match prefix {
        Prefix::ReceivedFromTheFirstHub => &network.hub.db,
        Prefix::AuthoredByTheEdge => &network.edge.db,
    };
    sql(
        author,
        "CREATE TABLE prefix_a (id INTEGER PRIMARY KEY) SYNC TWO WAY",
    );
    sql(author, "ALTER TABLE prefix_a ADD COLUMN extra INTEGER");
    if prefix == Prefix::ReceivedFromTheFirstHub {
        runtime.block_on(network.client.pull_default()).unwrap();
    }
    let frontier = network.edge.db.current_lsn();
    let Network {
        edge,
        hub: first,
        broker: _broker,
        server,
        client,
    } = network;
    drop(client);
    if let Some(server) = server {
        server.stop(runtime);
    }
    first.close();
    let network = Network::connect(runtime, edge, second);
    network
        .client
        .change_destination(&network.hub.node_id())
        .unwrap();
    churn(&network.hub.db, "moved", size);
    runtime.block_on(network.client.pull_default()).unwrap();
    after_move(&network.edge.db);
    MovedEdge {
        network,
        frontier,
        first_hub_node,
    }
}

/// Whether a declaration names a table whose name starts with `prefix`.
pub(crate) fn names(ddl: &contextdb_engine::sync_types::DdlChange, prefix: &str) -> bool {
    format!("{ddl:?}").contains(&format!("\"{prefix}"))
}
