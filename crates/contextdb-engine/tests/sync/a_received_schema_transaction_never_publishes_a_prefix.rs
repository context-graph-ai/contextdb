//! Received schema is one authenticated, durable, restart-safe migration.

use contextdb_core::{Error, Incarnation, Lsn, TenantId, Value};
use contextdb_engine::Database;
use contextdb_engine::database::{
    ReceivedSchemaApplyObservationForTest, ReceivedSchemaApplyObserverForTest,
};
use contextdb_engine::plugin::{CommitSource, DatabasePlugin};
use contextdb_engine::sync_types::{DdlChange, SchemaSyncCapability, SchemaSyncHoldback};
use contextdb_server::subjects::{pull_subject, push_subject};
use contextdb_server::{FabricIdentity, InProcessBroker, SyncClient, SyncServer};
use contextdb_tx::WriteSet;
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write as _};
use std::os::unix::process::CommandExt as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::time::Duration;
use uuid::Uuid;

const TENANT: &str = "received-schema-unit";
const CHILD_ROOT_ENV: &str = "CONTEXTDB_RECEIVED_SCHEMA_CHILD_ROOT";
const CHILD_BOUNDARY_ENV: &str = "CONTEXTDB_RECEIVED_SCHEMA_CHILD_BOUNDARY";
const BOUNDARY_MARKER: &str = "RECEIVED_SCHEMA_BOUNDARY";
const CHILD_TIMEOUT: Duration = Duration::from_secs(30);

// Supplemental declarations retain the original sink/route and behavioral probes.
// Every migration below also includes the entire source-extracted pinned cg startup.
const SOURCE_SCHEMA: &[&str] = &[
    "CREATE TABLE receiver_contexts (id UUID PRIMARY KEY)",
    "CREATE TABLE receiver_weights (id UUID PRIMARY KEY, passage_id UUID NOT NULL, weight REAL NOT NULL)",
    "CREATE INDEX receiver_weights_passage ON receiver_weights(passage_id)",
    "CREATE TABLE receiver_passages (id UUID PRIMARY KEY, context_id UUID NOT NULL REFERENCES receiver_contexts(id), embedding VECTOR(2) PARTITION_KEY (context_id) RANK_POLICY (JOIN receiver_weights ON passage_id, FORMULA '{vector_score} * coalesce({weight}, 1.0)', SORT_KEY receiver_rank))",
    "CREATE TABLE receiver_edges (id UUID PRIMARY KEY, source_id UUID NOT NULL REFERENCES receiver_contexts(id), target_id UUID NOT NULL REFERENCES receiver_contexts(id), edge_type TEXT NOT NULL) DAG('CITES')",
    "CREATE INDEX receiver_passages_context ON receiver_passages(context_id)",
    "CREATE TABLE receiver_events (id UUID PRIMARY KEY, context_id UUID NOT NULL REFERENCES receiver_contexts(id))",
    "CREATE EVENT TYPE receiver_event WHEN INSERT ON receiver_events",
    "CREATE TRIGGER receiver_trigger ON receiver_events WHEN INSERT INCLUDING SYNC",
    "CREATE SINK receiver_sink TYPE callback",
    "CREATE ROUTE receiver_route EVENT receiver_event TO receiver_sink",
];

/// WHAT THE RECEIVER ALREADY HELD, WHOLE, BEFORE THE MIGRATION ARRIVED.
///
/// The gate is "exactly the old complete state or exactly the complete cg
/// startup schema, never a proper subset", so the old state has to be a real
/// state with something in it -- a receiver that started empty can only ever
/// prove the weaker "nothing arrived". These tables are `SYNC OFF`, so their
/// CONTENT is the receiver's own and never travels to the peers below, while
/// their declarations do; they survive every fault before durability with
/// their rows intact and must still be whole after a fault past it.
const RECEIVER_BASELINE: &[&str] = &[
    "CREATE TABLE baseline_ledger (id UUID PRIMARY KEY, label TEXT NOT NULL, weight REAL NOT NULL) SYNC OFF",
    "CREATE INDEX baseline_ledger_label ON baseline_ledger (label)",
    "CREATE TABLE baseline_notes (id UUID PRIMARY KEY, ledger_id UUID NOT NULL REFERENCES baseline_ledger(id), body TEXT NOT NULL) SYNC OFF",
];

const BASELINE_ROWS: u128 = 8;

#[path = "../fixtures/schema_wall/mod.rs"]
mod support;

fn source_statements() -> Vec<&'static str> {
    support::pinned::LOCAL
        .iter()
        .chain(SOURCE_SCHEMA)
        .copied()
        .collect()
}

/// The receiver's complete state once the migration has installed: what it
/// already held, plus the entire authored migration, and nothing else.
fn whole_receiver_image() -> Vec<&'static str> {
    RECEIVER_BASELINE
        .iter()
        .copied()
        .chain(source_statements())
        .collect()
}

fn install_receiver_baseline(db: &Database) {
    support::install(db, RECEIVER_BASELINE);
    for row in 0..BASELINE_ROWS {
        db.execute(
            "INSERT INTO baseline_ledger VALUES ($id, $label, $weight)",
            &HashMap::from([
                ("id".to_string(), Value::Uuid(Uuid::from_u128(0xB000 + row))),
                ("label".to_string(), Value::Text(format!("ledger-{row}"))),
                ("weight".to_string(), Value::Float64(row as f64)),
            ]),
        )
        .expect("the receiver's own prior table accepts a row");
        db.execute(
            "INSERT INTO baseline_notes VALUES ($id, $ledger_id, $body)",
            &HashMap::from([
                ("id".to_string(), Value::Uuid(Uuid::from_u128(0xB100 + row))),
                (
                    "ledger_id".to_string(),
                    Value::Uuid(Uuid::from_u128(0xB000 + row)),
                ),
                ("body".to_string(), Value::Text(format!("note-{row}"))),
            ]),
        )
        .expect("the receiver's own prior dependent table accepts a row");
    }
}

/// The old state is complete only if its CONTENT survived too, not just its
/// declarations: every row, readable through its own declared index.
fn assert_baseline_content(db: &Database, phase: &str) {
    let labels = db
        .execute("SELECT label FROM baseline_ledger ORDER BY label", &empty())
        .unwrap_or_else(|error| {
            panic!("{phase}: the receiver's prior table must read back: {error}")
        })
        .rows;
    assert_eq!(
        labels.len(),
        BASELINE_ROWS as usize,
        "{phase}: the receiver's prior content must survive whole"
    );
    assert!(
        db.column_has_covering_index("baseline_ledger", "label"),
        "{phase}: the receiver's prior index must survive whole"
    );
    assert_eq!(
        db.execute("SELECT body FROM baseline_notes", &empty())
            .unwrap_or_else(|error| panic!(
                "{phase}: the receiver's prior dependent table must read back: {error}"
            ))
            .rows
            .len(),
        BASELINE_ROWS as usize
    );
}

fn assert_full_cg_subset(db: &Database) {
    let reference = support::reference(support::pinned::LOCAL);
    let actual = support::schema(db);
    for declaration in support::schema(&reference) {
        let rendered_name = declaration.lines().next().unwrap_or_default();
        assert!(
            actual.contains(&declaration),
            "full pinned cg artifact missing or changed: {declaration}\nactual matching declaration: {:?}",
            actual
                .iter()
                .find(|candidate| candidate.lines().next() == Some(rendered_name))
        );
    }
}

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("build the bounded receiver fixture runtime")
}

fn install_source_schema(db: &Database) {
    let tx = db.begin().expect("begin the source schema transaction");
    for ddl in source_statements() {
        db.execute_in_tx(tx, ddl, &empty())
            .expect("stage every source declaration in the one transaction");
    }
    db.commit(tx)
        .expect("commit the source schema as one durable transaction");
}

fn source_schema(db: &Database) -> (Vec<DdlChange>, Lsn) {
    let changes = db
        .changes_since(Lsn(0))
        .expect("read or construct the declared fixture result");
    let source_lsn = changes.max_lsn().expect("source schema has a frontier");
    assert_eq!(changes.ddl.len(), source_statements().len());
    assert!(changes.ddl_lsn.iter().all(|lsn| *lsn == source_lsn));
    (changes.ddl, source_lsn)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CrashBoundary {
    BeforeDurableCommit,
    AfterDurableBeforePublication,
    AfterPublicationBeforeReply,
}

impl CrashBoundary {
    const ALL: [Self; 3] = [
        Self::BeforeDurableCommit,
        Self::AfterDurableBeforePublication,
        Self::AfterPublicationBeforeReply,
    ];

    fn name(self) -> &'static str {
        match self {
            Self::BeforeDurableCommit => "before-durable-commit",
            Self::AfterDurableBeforePublication => "after-durable-before-publication",
            Self::AfterPublicationBeforeReply => "after-publication-before-reply",
        }
    }

    fn parse(value: &str) -> Self {
        Self::ALL
            .into_iter()
            .find(|boundary| boundary.name() == value)
            .unwrap_or_else(|| panic!("unknown received-schema child boundary {value}"))
    }

    fn committed(self) -> bool {
        self != Self::BeforeDurableCommit
    }
}

struct CrashFixture {
    _root: tempfile::TempDir,
    edge_path: PathBuf,
    hub_path: PathBuf,
    edge_identity_path: PathBuf,
    hub_identity_path: PathBuf,
    edge_node_id: String,
    edge_incarnation: Incarnation,
    source_lsn: Lsn,
    source_ddl: Vec<DdlChange>,
}

impl CrashFixture {
    fn new() -> Self {
        let root = tempfile::tempdir().expect("create received-schema crash fixture");
        let edge_path = root.path().join("edge.db");
        let hub_path = root.path().join("hub.db");
        let edge_identity_path = root.path().join("edge.identity");
        let hub_identity_path = root.path().join("hub.identity");
        let edge_identity = FabricIdentity::load_or_generate(&edge_identity_path)
            .expect("persist the source edge identity");
        FabricIdentity::load_or_generate(&hub_identity_path)
            .expect("persist the receiver identity");
        let edge_node_id = edge_identity.node_id();
        let edge = Database::open(&edge_path).expect("open source edge");
        install_source_schema(&edge);
        let edge_incarnation = edge
            .sync_incarnation_for_test(&TenantId::from(TENANT))
            .expect("persist the source database incarnation");
        let (source_ddl, source_lsn) = source_schema(&edge);
        edge.close().expect("close source fixture before child");
        let hub = Database::open(&hub_path).expect("create receiver fixture");
        install_receiver_baseline(&hub);
        hub.close().expect("close receiver fixture before child");
        Self {
            _root: root,
            edge_path,
            hub_path,
            edge_identity_path,
            hub_identity_path,
            edge_node_id,
            edge_incarnation,
            source_lsn,
            source_ddl,
        }
    }

    fn root(&self) -> &Path {
        self._root.path()
    }

    fn marker(&self, boundary: CrashBoundary) -> String {
        format!(
            "{BOUNDARY_MARKER} boundary={} source_lsn={} node={} incarnation={}",
            boundary.name(),
            self.source_lsn.0,
            self.edge_node_id,
            self.edge_incarnation.to_hex(),
        )
    }
}

fn fixture_paths(root: &Path) -> (PathBuf, PathBuf, PathBuf, PathBuf) {
    (
        root.join("edge.db"),
        root.join("hub.db"),
        root.join("edge.identity"),
        root.join("hub.identity"),
    )
}

fn boundary_marker(
    boundary: CrashBoundary,
    edge: &Database,
    edge_identity: &FabricIdentity,
) -> String {
    let (_, source_lsn) = source_schema(edge);
    let incarnation = edge
        .sync_incarnation_for_test(&TenantId::from(TENANT))
        .expect("read child source incarnation");
    format!(
        "{BOUNDARY_MARKER} boundary={} source_lsn={} node={} incarnation={}",
        boundary.name(),
        source_lsn.0,
        edge_identity.node_id(),
        incarnation.to_hex(),
    )
}

fn emit_marker(marker: &str) {
    println!("{marker}");
    std::io::stdout()
        .flush()
        .expect("flush the boundary receipt to the parent");
}

struct RejectReceivedSchemaCommit;

impl DatabasePlugin for RejectReceivedSchemaCommit {
    fn pre_commit(
        &self,
        _write_set: &WriteSet,
        source: CommitSource,
    ) -> contextdb_core::Result<()> {
        if source == CommitSource::SyncPull {
            return Err(Error::Other(
                "injected received-schema pre-commit refusal".to_string().into(),
            ));
        }
        Ok(())
    }
}

struct RunningServer {
    server: Arc<SyncServer>,
    shutdown: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

impl RunningServer {
    async fn stop(self) {
        self.shutdown.store(true, Ordering::SeqCst);
        self.task.await.expect("receiver server task must stop");
    }
}

async fn start_server(
    broker: &InProcessBroker,
    tenant: &str,
    db: Arc<Database>,
    identity: Arc<FabricIdentity>,
) -> RunningServer {
    let node_id = identity.node_id();
    let server = Arc::new(
        SyncServer::with_authenticated_transport_and_identity_for_test(
            db,
            broker.server_as(&node_id),
            TenantId::from(tenant),
            node_id,
            identity,
        )
        .expect("read or construct the declared fixture result"),
    );
    let shutdown = Arc::new(AtomicBool::new(false));
    let task = tokio::spawn({
        let server = server.clone();
        let shutdown = shutdown.clone();
        async move { server.run_until(shutdown).await }
    });
    broker
        .wait_for_registered_route_for_test(&push_subject(tenant))
        .await;
    RunningServer {
        server,
        shutdown,
        task,
    }
}

#[test]
fn received_schema_crash_child() {
    let Some(root) = std::env::var_os(CHILD_ROOT_ENV) else {
        let db = Database::open_memory();
        support::install(
            &db,
            &["CREATE TABLE receiver_child_control (id INTEGER PRIMARY KEY)"],
        );
        assert!(db.table_meta("receiver_child_control").is_some());
        return;
    };
    let boundary = CrashBoundary::parse(
        &std::env::var(CHILD_BOUNDARY_ENV).expect("child boundary environment is present"),
    );
    let root = PathBuf::from(root);
    let (edge_path, hub_path, edge_identity_path, hub_identity_path) = fixture_paths(&root);
    let edge_identity = Arc::new(
        FabricIdentity::load_or_generate(&edge_identity_path).expect("load child edge identity"),
    );
    let hub_identity = Arc::new(
        FabricIdentity::load_or_generate(&hub_identity_path).expect("load child hub identity"),
    );
    let edge = Arc::new(Database::open(&edge_path).expect("open child source"));
    let child_source_ddl = source_schema(&edge).0;
    let marker = boundary_marker(boundary, &edge, &edge_identity);
    let hub = Arc::new(Database::open(&hub_path).expect("open child receiver"));
    runtime().block_on(async move {
        let broker = InProcessBroker::new();
        let durable_pause = (boundary == CrashBoundary::BeforeDurableCommit)
            .then(|| hub.pause_before_received_schema_durable_commit_for_test());
        let publication_pause = (boundary == CrashBoundary::AfterDurableBeforePublication)
            .then(|| hub.pause_before_received_schema_publish_for_test());
        let running = start_server(&broker, TENANT, hub, hub_identity).await;
        let reply_pause = (boundary == CrashBoundary::AfterPublicationBeforeReply).then(|| {
            running
                .server
                .pause_after_completed_apply_before_reply_for_test()
        });
        let edge_node_id = edge_identity.node_id();
        let client = SyncClient::with_authenticated_transport_and_identity_for_test(
            edge,
            broker.client_as(&edge_node_id),
            TenantId::from(TENANT),
            edge_identity,
        )
        .expect("read or construct the declared fixture result");
        let push = tokio::spawn(async move { client.push().await });
        match boundary {
            CrashBoundary::BeforeDurableCommit => assert!(
                durable_pause
                    .as_ref()
                    .expect("durable pause is armed")
                    .wait_until_reached(CHILD_TIMEOUT),
                "received schema never reached the finalized-write-set/pre-durable boundary"
            ),
            CrashBoundary::AfterDurableBeforePublication => assert!(
                publication_pause
                    .as_ref()
                    .expect("publication pause is armed")
                    .wait_until_reached(CHILD_TIMEOUT),
                "received schema never reached the post-durable/pre-publication boundary"
            ),
            CrashBoundary::AfterPublicationBeforeReply => assert!(
                reply_pause
                    .as_ref()
                    .expect("reply pause is armed")
                    .wait_until_reached(CHILD_TIMEOUT),
                "received schema never reached the post-publication/pre-response boundary"
            ),
        }
        if boundary == CrashBoundary::AfterPublicationBeforeReply {
            assert_complete_schema(
                running.server.db(),
                &child_source_ddl,
                "live post-publication/pre-response boundary",
            );
        }
        // Public schema inspection takes the publication read gate, so it is
        // deliberately blocked while either pre-publication pause holds the
        // corresponding write gate. The parent kills the process here and
        // proves absence after reopening the same durable store.
        let _push = push;
        emit_marker(&marker);
        loop {
            std::thread::park();
        }
    });
}

struct CrashChild {
    child: Child,
    receipts: mpsc::Receiver<Result<String, String>>,
    terminated: bool,
}

impl CrashChild {
    fn spawn(fixture: &CrashFixture, boundary: CrashBoundary) -> Self {
        let mut command = Command::new(
            std::env::current_exe().expect("locate the current sync integration-test binary"),
        );
        command
            .arg("--exact")
            .arg(crate::child_test_filter(
                module_path!(),
                "received_schema_crash_child",
            ))
            .arg("--nocapture")
            .env(CHILD_ROOT_ENV, fixture.root())
            .env(CHILD_BOUNDARY_ENV, boundary.name())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());
        command.process_group(0);
        let mut child = command.spawn().expect("spawn received-schema crash child");
        let stdout = child.stdout.take().expect("capture child boundary receipt");
        let (sender, receipts) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                match line {
                    Ok(line) if line.contains(BOUNDARY_MARKER) => {
                        let at = line.find(BOUNDARY_MARKER).unwrap();
                        let _ = sender.send(Ok(line[at..].to_string()));
                        return;
                    }
                    Ok(_) => {}
                    Err(error) => {
                        let _ = sender.send(Err(error.to_string()));
                        return;
                    }
                }
            }
            let _ = sender.send(Err(
                "child exited before emitting its boundary receipt".to_string()
            ));
        });
        Self {
            child,
            receipts,
            terminated: false,
        }
    }

    fn wait_for_boundary(&self) -> String {
        self.receipts
            .recv_timeout(CHILD_TIMEOUT)
            .unwrap_or_else(|error| panic!("timed out waiting for child boundary: {error}"))
            .unwrap_or_else(|error| panic!("failed to read child boundary: {error}"))
    }

    fn terminate(&mut self) -> Option<ExitStatus> {
        if self.terminated {
            return self.child.try_wait().ok().flatten();
        }
        self.terminated = true;
        let pid = self.child.id() as i32;
        // The child is its own process-group leader. Kill the group so a
        // failed fixture cannot strand transport/runtime descendants.
        unsafe {
            libc::kill(-pid, libc::SIGKILL);
        }
        self.child.wait().ok()
    }

    fn kill_at_boundary(mut self) {
        let status = self
            .terminate()
            .expect("SIGKILLed boundary child must be reaped");
        assert!(
            !status.success(),
            "boundary child must not exit successfully"
        );
    }
}

impl Drop for CrashChild {
    fn drop(&mut self) {
        let _ = self.terminate();
    }
}

fn find_schema_vector(
    changes: &contextdb_engine::sync_types::ChangeSet,
    expected: &[DdlChange],
) -> usize {
    changes
        .ddl
        .windows(expected.len())
        .position(|window| window == expected)
        .unwrap_or_else(|| {
            panic!(
                "receiver DDL history does not contain the exact authored vector in order: {:?}",
                changes.ddl
            )
        })
}

fn assert_complete_schema(db: &Database, expected: &[DdlChange], phase: &str) {
    assert_full_cg_subset(db);
    for table in [
        "receiver_contexts",
        "receiver_weights",
        "receiver_passages",
        "receiver_edges",
        "receiver_events",
    ] {
        assert!(db.table_meta(table).is_some(), "{phase}: missing {table}");
    }
    let passages = db
        .table_meta("receiver_passages")
        .expect("passage metadata exists");
    let embedding = passages
        .columns
        .iter()
        .find(|column| column.name == "embedding")
        .expect("embedding metadata exists");
    assert!(
        embedding.rank_policy.is_some(),
        "{phase}: rank policy missing"
    );
    assert_eq!(
        embedding.partition_key_columns.as_deref(),
        Some(["context_id".to_string()].as_slice()),
        "{phase}: vector partition identity missing"
    );
    assert_eq!(
        db.table_meta("receiver_edges")
            .expect("edge metadata exists")
            .dag_edge_types,
        vec!["CITES".to_string()],
        "{phase}: DAG declaration missing"
    );
    assert!(
        db.column_has_covering_index("receiver_passages", "context_id"),
        "{phase}: declared relational index missing"
    );
    assert!(
        db.list_triggers()
            .iter()
            .any(|trigger| trigger.name == "receiver_trigger" && trigger.including_sync),
        "{phase}: INCLUDING SYNC trigger missing"
    );
    let events = db.event_bus_status();
    assert!(
        events
            .event_types
            .iter()
            .any(|event| event.name == "receiver_event")
    );
    assert!(events.sinks.iter().any(|sink| sink.name == "receiver_sink"));
    assert!(events.routes.iter().any(|route| {
        route.name == "receiver_route"
            && route.event_type == "receiver_event"
            && route.sink == "receiver_sink"
    }));
    let changes = db
        .changes_since(Lsn(0))
        .expect("read or construct the declared fixture result");
    let start = find_schema_vector(&changes, expected);
    let relevant = changes
        .ddl
        .iter()
        .filter(|ddl| expected.contains(ddl))
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(
        relevant, expected,
        "{phase}: retry must not retain a duplicate proper prefix"
    );
    let migration_lsns = &changes.ddl_lsn[start..start + expected.len()];
    assert!(
        migration_lsns.iter().all(|lsn| *lsn == migration_lsns[0]),
        "{phase}: receiver split one authored migration across local commits"
    );
}

fn assert_schema_absent(db: &Database, phase: &str) {
    for ddl in support::pinned::LOCAL {
        if let Some(declaration) = ddl.strip_prefix("CREATE TABLE ") {
            let name = declaration.split_whitespace().next().unwrap();
            assert!(
                db.table_meta(name).is_none(),
                "{phase}: partial cg table {name}"
            );
        }
    }
    for table in [
        "receiver_contexts",
        "receiver_weights",
        "receiver_passages",
        "receiver_edges",
        "receiver_events",
    ] {
        assert!(db.table_meta(table).is_none(), "{phase}: published {table}");
    }
    assert!(
        db.list_triggers().is_empty(),
        "{phase}: published a trigger"
    );
    let events = db.event_bus_status();
    assert!(events.event_types.is_empty());
    assert!(events.sinks.is_empty());
    assert!(events.routes.is_empty());
}

/// EXACTLY THE OLD COMPLETE STATE. Nothing of the migration is queryable,
/// what the receiver already held is whole -- declarations, index and rows --
/// and no stray extra declaration survived either.
fn assert_old_whole_state(db: &Database, phase: &str) {
    assert_schema_absent(db, phase);
    support::assert_whole_image_of(db, RECEIVER_BASELINE, phase);
    assert_baseline_content(db, phase);
}

/// EXACTLY THE COMPLETE STARTUP SCHEMA. Every declaration of the whole cg
/// startup migration, in the author's order, kept once under one commit
/// boundary, beside the receiver's own prior state still whole.
fn assert_new_whole_state(db: &Database, expected: &[DdlChange], phase: &str) {
    assert_complete_schema(db, expected, phase);
    support::assert_whole_image_of(db, &whole_receiver_image(), phase);
    support::assert_unit_kept_once(db, expected, phase);
    assert_baseline_content(db, phase);
}

fn assert_receipt(db: &Database, fixture: &CrashFixture, expected: Option<Lsn>, phase: &str) {
    assert_eq!(
        db.persisted_sync_applied_push_watermark_for_node_incarnation(
            &TenantId::from(TENANT),
            &fixture.edge_node_id,
            fixture.edge_incarnation,
        )
        .expect("read the durable authenticated receiver receipt"),
        expected,
        "{phase}: wrong tenant/source-node/source-incarnation receipt"
    );
}

fn assert_zero_image_work(observation: &ReceivedSchemaApplyObservationForTest, phase: &str) {
    support::assert_zero_images(observation);
    assert_eq!(observation.staged_table_images_copied, 0, "{phase}");
    assert_eq!(observation.staged_rows_copied, 0, "{phase}");
    assert_eq!(observation.staged_graph_edges_copied, 0, "{phase}");
    assert_eq!(observation.staged_vectors_copied, 0, "{phase}");
    assert_eq!(observation.durable_table_rewrites, 0, "{phase}");
    assert_eq!(observation.durable_rows_rewritten, 0, "{phase}");
}

fn assert_components_usable(db: &Database, seed: u128) {
    support::seed_cg_artifacts(db, seed + 0x10000);
    support::assert_cg_artifacts(db, seed + 0x10000);
    let context = Uuid::from_u128(seed);
    let target = Uuid::from_u128(seed + 1);
    let passage = Uuid::from_u128(seed + 2);
    db.execute(
        "INSERT INTO receiver_contexts VALUES ($id)",
        &HashMap::from([("id".to_string(), Value::Uuid(context))]),
    )
    .expect("received relation accepts a row");
    db.execute(
        "INSERT INTO receiver_contexts VALUES ($id)",
        &HashMap::from([("id".to_string(), Value::Uuid(target))]),
    )
    .expect("received relation accepts a target row");
    db.execute(
        "INSERT INTO receiver_passages VALUES ($id, $context_id, $embedding)",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(passage)),
            ("context_id".to_string(), Value::Uuid(context)),
            ("embedding".to_string(), Value::Vector(vec![1.0, 0.0])),
        ]),
    )
    .expect("received vector relation accepts a row");
    db.execute(
        "INSERT INTO receiver_weights VALUES ($id, $passage_id, 1.0)",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(Uuid::from_u128(seed + 3))),
            ("passage_id".to_string(), Value::Uuid(passage)),
        ]),
    )
    .expect("received rank join accepts a row");
    db.execute(
        "INSERT INTO receiver_edges VALUES ($id, $source_id, $target_id, 'CITES')",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(Uuid::from_u128(seed + 4))),
            ("source_id".to_string(), Value::Uuid(context)),
            ("target_id".to_string(), Value::Uuid(target)),
        ]),
    )
    .expect("received DAG accepts an acyclic edge");
    db.execute(
        "INSERT INTO receiver_events VALUES ($id, $context_id)",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(Uuid::from_u128(seed + 5))),
            ("context_id".to_string(), Value::Uuid(context)),
        ]),
    )
    .expect("received event declarations accept an event row");
    assert_eq!(
        db.execute(
            "SELECT id FROM receiver_passages WHERE context_id = $context_id ORDER BY embedding <=> $query USE RANK receiver_rank LIMIT 1",
            &HashMap::from([
                ("context_id".to_string(), Value::Uuid(context)),
                ("query".to_string(), Value::Vector(vec![1.0, 0.0])),
            ]),
        )
        .expect("received vector/rank/index surface answers")
        .rows,
        vec![vec![Value::Uuid(passage)]]
    );
}

/// SECOND-EDGE READBACK. Deliver the recovered receiver's state onward to a
/// further peer and read it back there: the same whole image, `.schema` equal
/// on both, and the author's order kept once under one commit boundary. A
/// declaration travels whatever its table declares about row sync, so the
/// receiver's own `SYNC OFF` prior tables arrive as declarations; their ROWS
/// are the receiver's alone and must not.
async fn assert_second_edge_readback(
    fixture: &CrashFixture,
    broker: &InProcessBroker,
    hub: &Database,
    phase: &str,
) {
    let second_path = fixture
        .root()
        .join(format!("second-edge-{phase}.db").replace([' ', ','], "-"));
    let second = Arc::new(Database::open(&second_path).expect("open the further peer"));
    let identity = Arc::new(FabricIdentity::generate());
    let client = SyncClient::with_authenticated_transport_and_identity_for_test(
        second.clone(),
        broker.client_as(&identity.node_id()),
        TenantId::from(TENANT),
        identity,
    )
    .expect("read or construct the declared fixture result");
    client
        .pull_default()
        .await
        .expect("a further peer pulls the recovered receiver's whole schema");
    let phase = format!("{phase}: second edge");
    support::assert_whole_image_of(&second, &whole_receiver_image(), &phase);
    support::assert_unit_kept_once(&second, &fixture.source_ddl, &phase);
    support::assert_whole_image(
        &second,
        hub,
        &format!("{phase}: equal to the recovered receiver's own schema"),
    );
    assert!(
        second
            .execute("SELECT label FROM baseline_ledger", &empty())
            .expect("the further peer holds the declaration")
            .rows
            .is_empty(),
        "{phase}: a table the receiver declared SYNC OFF must send no rows onward"
    );
    drop(client);
    second.close().expect("close the further peer");
}

async fn retry_after_crash(fixture: &CrashFixture, boundary: CrashBoundary) {
    let broker = InProcessBroker::new();
    let edge = Arc::new(Database::open(&fixture.edge_path).expect("reopen crashed source"));
    let hub = Arc::new(Database::open(&fixture.hub_path).expect("reopen crashed receiver"));
    let edge_identity = Arc::new(
        FabricIdentity::load_or_generate(&fixture.edge_identity_path)
            .expect("reload stable source identity"),
    );
    let hub_identity = Arc::new(
        FabricIdentity::load_or_generate(&fixture.hub_identity_path)
            .expect("reload stable receiver identity"),
    );
    let running = start_server(&broker, TENANT, hub.clone(), hub_identity).await;
    let client = SyncClient::with_authenticated_transport_and_identity_for_test(
        edge.clone(),
        broker.client_as(&fixture.edge_node_id),
        TenantId::from(TENANT),
        edge_identity,
    )
    .expect("read or construct the declared fixture result");
    let observer = ReceivedSchemaApplyObserverForTest::new();
    let observation_scope = hub.observe_received_schema_applies_for_test(&observer);
    client
        .push()
        .await
        .expect("restart must safely apply or replay the complete source migration");
    drop(observation_scope);
    let observation = observer.storage_observation();
    assert!(
        observation.durable_observer_attached,
        "retry must observe the actual durable instance"
    );
    if boundary.committed() {
        assert_zero_image_work(
            &observation,
            "a committed crash retry must do no image work",
        );
    } else {
        assert!(
            !observation.durable_table_names.is_empty(),
            "the pre-commit crash retry must execute the physical schema stage: {observation:?}"
        );
    }
    let sent_units = support::assert_authored_wire(
        &broker,
        &fixture.source_ddl,
        fixture.source_lsn,
        fixture.edge_incarnation,
    );
    if sent_units == 0 {
        assert!(
            boundary.committed(),
            "an uncommitted migration must cross the actual wire"
        );
        assert_eq!(
            client.push_watermark(),
            fixture.source_lsn,
            "skipping a receipted retry requires source reconciliation"
        );
        assert_eq!(client.pending_push_change_count().unwrap(), 0);
    }
    assert_new_whole_state(&hub, &fixture.source_ddl, "after crash retry");
    assert_receipt(&hub, fixture, Some(fixture.source_lsn), "after crash retry");

    // SECOND-EDGE READBACK. The receiver that came back from the crash is now
    // the one serving: a further peer pulls from it and must end up with the
    // same schema -- every declaration equal, in the author's order, once --
    // and nothing of the receiver's own `SYNC OFF` prior state, which was
    // never the author's to send.
    assert_second_edge_readback(fixture, &broker, &hub, "after crash retry").await;

    assert_components_usable(&hub, 0xA000 + boundary as u128 * 0x100);

    if boundary == CrashBoundary::AfterPublicationBeforeReply {
        let observed_replay = observer.storage_observation();
        edge.execute(
            "CREATE TABLE observer_unrelated_after_scope (id INTEGER PRIMARY KEY)",
            &empty(),
        )
        .expect("author a subsequent unrelated schema operation");
        client
            .push()
            .await
            .expect("subsequent unrelated schema operation crosses normally");
        assert_eq!(
            observer.storage_observation(),
            observed_replay,
            "an operation after scope exit must not be charged to the target trace"
        );
    }

    drop(client);
    running.stop().await;
    hub.close().expect("close retried receiver");
    edge.close().expect("close retried source");
    let file = contextdb_engine::ReadSession::open(&fixture.hub_path).unwrap();
    assert_eq!(file.route(), contextdb_core::read_contract::ReadRoute::File);
    support::assert_cg_artifact_queries(
        |sql, p| file.execute(sql, p).unwrap().rows,
        0x1A000 + boundary as u128 * 0x100,
    );
}

fn receiver_crash_at(boundary: CrashBoundary) {
    let fixture = CrashFixture::new();
    let before_identity = support::committed_identity(&fixture.hub_path);
    let child = CrashChild::spawn(&fixture, boundary);
    assert_eq!(
        child.wait_for_boundary(),
        fixture.marker(boundary),
        "child must identify the exact source life and durable boundary"
    );
    child.kill_at_boundary();
    // Perform the production writable crash-recovery opener before asking a
    // read-only File session to attest the complete committed image.
    Database::open(&fixture.hub_path).unwrap().close().unwrap();
    let recovered_identity = support::committed_identity(&fixture.hub_path);
    if boundary.committed() {
        assert_ne!(
            recovered_identity, before_identity,
            "one whole durable received schema changes the verified content identity"
        );
    }

    let receiver = Database::open(&fixture.hub_path).expect("reopen killed receiver");
    if boundary.committed() {
        assert_new_whole_state(&receiver, &fixture.source_ddl, "after committed crash");
        assert_receipt(
            &receiver,
            &fixture,
            Some(fixture.source_lsn),
            "after committed crash",
        );
    } else {
        assert_old_whole_state(&receiver, "after pre-commit crash");
        assert_receipt(&receiver, &fixture, None, "after pre-commit crash");
    }
    receiver.close().expect("close crash inspection handle");
    assert_eq!(
        support::committed_identity(&fixture.hub_path),
        recovered_identity,
        "reopen keeps the same committed identity"
    );
    runtime().block_on(retry_after_crash(&fixture, boundary));
}

// THE BOUNDARIES INSIDE TAKING DELIVERY THAT THE THREE TRANSPORT-LEVEL
// CRASHES ABOVE CANNOT REACH.
//
// The three above stop the receiver at boundaries the transport itself can
// see: before the durable commit, after it and before publication, and after
// publication and before the reply. Taking delivery has three more of its own
// -- the private working copy made and nothing else done, the prepared stage
// registered in the database's own map, and publication stopped between two of
// its components -- and the gate names every durable/publication boundary, not
// the ones a pause happens to expose. Each of these fires
// `std::process::abort()` from inside the fault callback itself: a real
// SIGABRT, no unwind, no `Drop`, no panic hook, and no timing window to race,
// because the abort is the very next thing that call does. The fault is
// thread-local, so the child takes delivery on its own thread through the
// authenticated received-schema bridge rather than through a server's worker
// pool -- the boundaries are the same engine boundaries either way.
const UNIT_FAULT_ROOT_ENV: &str = "CONTEXTDB_RECEIVED_UNIT_FAULT_CHILD_ROOT";
const UNIT_FAULT_BOUNDARY_ENV: &str = "CONTEXTDB_RECEIVED_UNIT_FAULT_CHILD_BOUNDARY";
const UNIT_FAULT_MARKER: &str = "RECEIVED_UNIT_FAULT_BOUNDARY";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum UnitFaultBoundary {
    AfterWorkingCopy,
    AfterStageRegistration,
    BetweenPublicationComponents,
}

impl UnitFaultBoundary {
    fn name(self) -> &'static str {
        match self {
            Self::AfterWorkingCopy => "after-working-copy",
            Self::AfterStageRegistration => "after-stage-registration",
            Self::BetweenPublicationComponents => "between-publication-components",
        }
    }

    fn parse(value: &str) -> Self {
        for boundary in [
            Self::AfterWorkingCopy,
            Self::AfterStageRegistration,
            Self::BetweenPublicationComponents,
        ] {
            if boundary.name() == value {
                return boundary;
            }
        }
        panic!("unknown received-unit fault boundary {value}")
    }

    fn point(self) -> contextdb_engine::ReceivedUnitFaultPoint {
        match self {
            Self::AfterWorkingCopy => contextdb_engine::ReceivedUnitFaultPoint::AfterWorkingCopy,
            Self::AfterStageRegistration => {
                contextdb_engine::ReceivedUnitFaultPoint::AfterStageRegistration
            }
            Self::BetweenPublicationComponents => {
                contextdb_engine::ReceivedUnitFaultPoint::BetweenPublicationComponents
            }
        }
    }

    /// Whether the durable commit had already happened when the process died.
    fn committed(self) -> bool {
        self == Self::BetweenPublicationComponents
    }
}

/// The database life the fixture author signs its schema in. It is the
/// author's own account of itself, checked with the signature, never against
/// whichever peer happens to deliver the unit.
const AUTHORED_INCARNATION: Incarnation = Incarnation(1);

/// The identity a fixture schema author signs with: a real ed25519 key, so a
/// receiver checks authorship against the author's public key rather than
/// trusting the session the unit arrived on.
fn schema_author() -> &'static contextdb_engine::identity::FabricIdentity {
    static AUTHOR: std::sync::OnceLock<contextdb_engine::identity::FabricIdentity> =
        std::sync::OnceLock::new();
    AUTHOR.get_or_init(contextdb_engine::identity::FabricIdentity::generate)
}

fn signed_by_the_author(
    sender: &Database,
    unit: &contextdb_engine::sync_types::ChangeSet,
) -> Vec<contextdb_engine::protocol::WireDdlProvenance> {
    let fixture_operation_reader_1 = sender
        .declared_reader_memory()
        .expect("admit the actual fixture reader");

    sender
        .outbound_ddl_provenance_for_test(
            unit,
            &TenantId::from(TENANT),
            &schema_author().node_id(),
            AUTHORED_INCARNATION,
            &|bytes| Ok(schema_author().sign_for_test(bytes)),
            fixture_operation_reader_1.clone(),
        )
        .expect("the author signs its own schema unit")
}

/// The authored migration as one schema-only unit, with the author's own
/// signed provenance for each statement.
fn source_unit(
    edge: &Database,
) -> (
    contextdb_engine::sync_types::ChangeSet,
    Vec<contextdb_engine::protocol::WireDdlProvenance>,
) {
    let (ddl, source_lsn) = source_schema(edge);
    let unit = contextdb_engine::sync_types::ChangeSet {
        ddl_lsn: vec![source_lsn; ddl.len()],
        ddl,
        ..Default::default()
    };
    let provenance = signed_by_the_author(edge, &unit);
    (unit, provenance)
}

fn take_delivery(
    hub: &Database,
    fixture_node: &str,
    incarnation: Incarnation,
    edge: &Database,
) -> contextdb_core::Result<()> {
    let (unit, provenance) = source_unit(edge);
    let fixture_operation_reader_2 = hub
        .declared_reader_memory()
        .expect("admit the actual fixture reader");
    let mut fixture_operation_copy_2 =
        contextdb_engine::vector_read_memory::admission(fixture_operation_reader_2.clone());

    hub.apply_received_schema_for_test(
        unit,
        provenance,
        TenantId::from(TENANT),
        fixture_node,
        incarnation,
        &mut fixture_operation_copy_2,
        fixture_operation_reader_2.clone(),
    )
    .map(|_| ())
}

#[test]
fn received_unit_fault_crash_child() {
    let Some(root) = std::env::var_os(UNIT_FAULT_ROOT_ENV) else {
        // An ordinary enumeration of this binary is a real positive control,
        // never an empty child-only success.
        let db = Database::open_memory();
        support::install(
            &db,
            &["CREATE TABLE received_unit_fault_control (id INTEGER PRIMARY KEY)"],
        );
        assert!(db.table_meta("received_unit_fault_control").is_some());
        return;
    };
    let boundary = UnitFaultBoundary::parse(
        &std::env::var(UNIT_FAULT_BOUNDARY_ENV).expect("child boundary environment is present"),
    );
    let root = PathBuf::from(root);
    let (edge_path, hub_path, edge_identity_path, _) = fixture_paths(&root);
    let edge_identity =
        FabricIdentity::load_or_generate(&edge_identity_path).expect("load child edge identity");
    let edge = Database::open(&edge_path).expect("open child source");
    let incarnation = edge
        .sync_incarnation_for_test(&TenantId::from(TENANT))
        .expect("read child source incarnation");
    let hub = Database::open(&hub_path).expect("open child receiver");
    emit_marker(&format!("{UNIT_FAULT_MARKER} boundary={}", boundary.name()));
    Database::arm_received_unit_fault_for_test(
        boundary.point(),
        contextdb_engine::ReceivedUnitCopyFault::Abort,
    );
    let _ = take_delivery(&hub, &edge_identity.node_id(), incarnation, &edge);
    panic!("the armed fault must abort the process before taking delivery ever returns");
}

fn spawn_unit_fault_child(fixture: &CrashFixture, boundary: UnitFaultBoundary) {
    let mut command = Command::new(
        std::env::current_exe().expect("locate the current sync integration-test binary"),
    );
    command
        .arg("--exact")
        .arg(crate::child_test_filter(
            module_path!(),
            "received_unit_fault_crash_child",
        ))
        .arg("--nocapture")
        .env(UNIT_FAULT_ROOT_ENV, fixture.root())
        .env(UNIT_FAULT_BOUNDARY_ENV, boundary.name())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    command.process_group(0);
    let mut child = command.spawn().expect("spawn received-unit fault child");
    let stdout = child.stdout.take().expect("capture child boundary receipt");
    let mut seen = false;
    for line in BufReader::new(stdout).lines() {
        let line = line.expect("read the fault child's receipt");
        if line.contains(UNIT_FAULT_MARKER) {
            assert!(
                line.contains(&format!("boundary={}", boundary.name())),
                "the child must name the boundary it is about to die at: {line}"
            );
            seen = true;
        }
    }
    assert!(
        seen,
        "the fault child must name its boundary before the armed abort kills it"
    );
    let status = child.wait().expect("reap the aborted fault child");
    assert!(
        !status.success(),
        "the armed fault must abort the child, not let it exit: {status:?}"
    );
    assert_eq!(
        status.code(),
        None,
        "the child must die of a real signal, not return an exit code: {status:?}"
    );
}

/// Retry the same unit through the same door the dead process used, then hand
/// the recovered receiver's state on to a further peer.
async fn retry_and_read_back(fixture: &CrashFixture, phase: &str) {
    let hub = Arc::new(Database::open(&fixture.hub_path).expect("reopen the aborted receiver"));
    let edge = Database::open(&fixture.edge_path).expect("reopen the source");
    take_delivery(&hub, &fixture.edge_node_id, fixture.edge_incarnation, &edge)
        .expect("the retried unit installs whole, once");
    assert_new_whole_state(&hub, &fixture.source_ddl, phase);
    let broker = InProcessBroker::new();
    let hub_identity = Arc::new(
        FabricIdentity::load_or_generate(&fixture.hub_identity_path)
            .expect("reload the receiver identity"),
    );
    let running = start_server(&broker, TENANT, hub.clone(), hub_identity).await;
    assert_second_edge_readback(fixture, &broker, &hub, phase).await;
    running.stop().await;
    edge.close().expect("close the source");
    hub.close().expect("close the recovered receiver");
}

fn receiver_aborts_at(boundary: UnitFaultBoundary) {
    let fixture = CrashFixture::new();
    let before_identity = support::committed_identity(&fixture.hub_path);
    spawn_unit_fault_child(&fixture, boundary);
    // The production writable crash-recovery opener runs before a read-only
    // File session is asked to attest the committed image.
    Database::open(&fixture.hub_path).unwrap().close().unwrap();
    let recovered_identity = support::committed_identity(&fixture.hub_path);
    let receiver = Database::open(&fixture.hub_path).expect("reopen the aborted receiver");
    let phase = format!("after abort at {}", boundary.name());
    if boundary.committed() {
        assert_new_whole_state(&receiver, &fixture.source_ddl, &phase);
        assert_ne!(
            recovered_identity, before_identity,
            "{phase}: one whole durable received schema changes the verified content identity"
        );
    } else {
        assert_old_whole_state(&receiver, &phase);
        assert_eq!(
            recovered_identity, before_identity,
            "{phase}: a process that died before durability published no content identity"
        );
        assert_receipt(&receiver, &fixture, None, &phase);
    }
    receiver.close().expect("close the crash inspection handle");
    runtime().block_on(retry_and_read_back(&fixture, &format!("{phase}, retried")));
}

#[test]
fn an_abort_with_only_the_working_copy_made_leaves_the_old_complete_state() {
    receiver_aborts_at(UnitFaultBoundary::AfterWorkingCopy);
}

#[test]
fn an_abort_with_the_prepared_stage_registered_leaves_the_old_complete_state() {
    receiver_aborts_at(UnitFaultBoundary::AfterStageRegistration);
}

#[test]
fn an_abort_between_publication_components_leaves_the_complete_startup_schema() {
    receiver_aborts_at(UnitFaultBoundary::BetweenPublicationComponents);
}

#[test]
fn a_received_schema_transaction_never_publishes_a_prefix() {
    receiver_crash_at(CrashBoundary::BeforeDurableCommit);
}
#[test]
fn complete_received_schema_recovers_after_durable_before_publication_crash() {
    receiver_crash_at(CrashBoundary::AfterDurableBeforePublication);
}
#[test]
fn complete_received_schema_reconciles_after_publication_before_reply_crash() {
    receiver_crash_at(CrashBoundary::AfterPublicationBeforeReply);
}

#[test]
fn received_schema_pre_commit_failure_publishes_neither_schema_nor_receipt() {
    let fixture = CrashFixture::new();
    runtime().block_on(async {
        let broker = InProcessBroker::new();
        let edge = Arc::new(Database::open(&fixture.edge_path).expect("open rejecting source"));
        let hub = Arc::new(
            Database::open_with_plugin(&fixture.hub_path, Arc::new(RejectReceivedSchemaCommit))
                .expect("open rejecting receiver"),
        );
        let edge_identity = Arc::new(
            FabricIdentity::load_or_generate(&fixture.edge_identity_path)
                .expect("load rejecting source identity"),
        );
        let hub_identity = Arc::new(
            FabricIdentity::load_or_generate(&fixture.hub_identity_path)
                .expect("load rejecting receiver identity"),
        );
        let running = start_server(&broker, TENANT, hub.clone(), hub_identity).await;
        let client = SyncClient::with_authenticated_transport_and_identity_for_test(
            edge.clone(),
            broker.client_as(&fixture.edge_node_id),
            TenantId::from(TENANT),
            edge_identity,
        )
        .expect("read or construct the declared fixture result");
        let error = client
            .push()
            .await
            .expect_err("the receiver plugin must reject the schema commit");
        assert!(error.to_string().contains("pre-commit refusal"));
        assert_schema_absent(&hub, "after rejected pre-commit");
        assert_receipt(&hub, &fixture, None, "after rejected pre-commit");
        drop(client);
        running.stop().await;
        hub.close().expect("close rejecting receiver");
        edge.close().expect("close rejecting source");
    });
    let reopened = Database::open(&fixture.hub_path).expect("reopen rejected receiver");
    assert_schema_absent(&reopened, "after rejected receiver restart");
    assert_receipt(&reopened, &fixture, None, "after rejected receiver restart");
}

fn attach_then_return(db: &Database, observer: &ReceivedSchemaApplyObserverForTest) {
    let _scope = db.observe_received_schema_applies_for_test(observer);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn received_schema_observation_scope_restores_after_nesting_return_and_panic() {
    let broker = InProcessBroker::new();
    let source = Arc::new(Database::open_memory());
    let receiver = Arc::new(Database::open_memory());
    let hub_identity = Arc::new(FabricIdentity::generate());
    let running = start_server(
        &broker,
        "received-schema-observer",
        receiver.clone(),
        hub_identity,
    )
    .await;
    let edge_identity = Arc::new(FabricIdentity::generate());
    let edge_node_id = edge_identity.node_id();
    let client = SyncClient::with_authenticated_transport_and_identity_for_test(
        source.clone(),
        broker.client_as(&edge_node_id),
        TenantId::from("received-schema-observer"),
        edge_identity,
    )
    .expect("read or construct the declared fixture result");
    let outer = ReceivedSchemaApplyObserverForTest::new();
    let inner = ReceivedSchemaApplyObserverForTest::new();
    let panic_observer = ReceivedSchemaApplyObserverForTest::new();
    let returned = ReceivedSchemaApplyObserverForTest::new();
    let outer_scope = receiver.observe_received_schema_applies_for_test(&outer);
    {
        let _inner_scope = receiver.observe_received_schema_applies_for_test(&inner);
        source
            .execute(
                "CREATE TABLE observed_inner (id INTEGER PRIMARY KEY)",
                &empty(),
            )
            .expect("author nested observed schema");
        client.push().await.expect("push nested observed schema");
    }
    assert!(inner.observation().is_some());
    assert_eq!(outer.observation(), None);

    source
        .execute(
            "CREATE TABLE observed_outer (id INTEGER PRIMARY KEY)",
            &empty(),
        )
        .expect("author restored outer schema");
    client.push().await.expect("push restored outer schema");
    let outer_before_panic = outer.observation().expect("outer observer restored");
    let panic_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _panic_scope = receiver.observe_received_schema_applies_for_test(&panic_observer);
        panic!("exercise observation-scope unwind");
    }));
    assert!(panic_result.is_err());
    source
        .execute(
            "CREATE TABLE observed_after_panic (id INTEGER PRIMARY KEY)",
            &empty(),
        )
        .expect("author schema after caught panic");
    client.push().await.expect("push schema after caught panic");
    assert_ne!(outer.observation(), Some(outer_before_panic));
    assert_eq!(panic_observer.observation(), None);
    drop(outer_scope);

    attach_then_return(&receiver, &returned);
    let outer_after_scope = outer.observation();
    source
        .execute(
            "CREATE TABLE observed_after_return (id INTEGER PRIMARY KEY)",
            &empty(),
        )
        .expect("author schema after early return");
    client.push().await.expect("push schema after early return");
    assert_eq!(returned.observation(), None);
    assert_eq!(outer.observation(), outer_after_scope);
    drop(client);
    running.stop().await;
}

/// The unrelated table that must keep syncing to a peer whose capability gap
/// is holding the whole migration back.
const HOLDBACK_CONTROL: &[&str] =
    &["CREATE TABLE receiver_control (id INTEGER PRIMARY KEY) SYNC TWO WAY"];

/// The complete state an edge ends in once the held migration is released:
/// the unrelated table it never stopped receiving, plus the whole authored
/// migration, and nothing else.
fn holdback_image() -> Vec<&'static str> {
    HOLDBACK_CONTROL
        .iter()
        .copied()
        .chain(source_statements())
        .collect()
}

/// The author's own source boundary, unmoved by anything a held peer does.
/// Holding a migration back, releasing it and retrying it must not renumber,
/// re-ordinal or re-digest a single statement at the author -- if any of that
/// moved, the peer that eventually takes delivery would be taking delivery of
/// a different transaction from the one it was held on.
fn source_boundary(author: &Database, migration: &[DdlChange], migration_lsn: Lsn) -> Vec<String> {
    signed_by_the_author(
        author,
        &contextdb_engine::sync_types::ChangeSet {
            ddl: migration.to_vec(),
            ddl_lsn: vec![migration_lsn; migration.len()],
            ..Default::default()
        },
    )
    .iter()
    .map(|entry| {
        format!(
            "{}:{}:{:?}:{:?}:{:?}",
            entry.source_ddl_lsn.0,
            entry.ordinal,
            entry.table,
            entry.table_generation,
            entry.digest
        )
    })
    .collect()
}

fn control_rows(db: &Database) -> Vec<i64> {
    db.execute("SELECT id FROM receiver_control ORDER BY id", &empty())
        .expect("read unrelated control rows")
        .rows
        .into_iter()
        .map(|row| match row.into_iter().next() {
            Some(Value::Int64(value)) => value,
            other => panic!("expected one integer control value, got {other:?}"),
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_whole_authored_migration_waits_together_while_another_edge_keeps_syncing() {
    let root = tempfile::tempdir().expect("create holdback restart fixture");
    let source_path = root.path().join("holdback-source.db");
    let old_path = root.path().join("old-edge.db");
    let current_path = root.path().join("current-edge.db");
    let source = Arc::new(Database::open(&source_path).expect("open holdback source"));
    support::declare(&source, HOLDBACK_CONTROL);
    source
        .execute("INSERT INTO receiver_control VALUES (1)", &empty())
        .expect("author first unrelated control row");
    let migration_start = source
        .changes_since(Lsn(0))
        .expect("read or construct the declared fixture result")
        .max_lsn()
        .unwrap();
    install_source_schema(&source);
    let migration = source
        .changes_since(migration_start)
        .expect("read or construct the declared fixture result");
    assert_eq!(migration.ddl.len(), source_statements().len());
    let migration_lsn = migration.max_lsn().expect("migration frontier exists");
    assert!(migration.ddl_lsn.iter().all(|lsn| *lsn == migration_lsn));
    let authored_boundary = source_boundary(&source, &migration.ddl, migration_lsn);

    let old = Arc::new(Database::open(&old_path).expect("open older receiver"));
    let current = Arc::new(Database::open(&current_path).expect("open current receiver"));
    let broker = InProcessBroker::new();
    let hub_identity = Arc::new(FabricIdentity::generate());
    let running = start_server(
        &broker,
        "whole-schema-holdback",
        source.clone(),
        hub_identity.clone(),
    )
    .await;
    broker
        .wait_for_registered_route_for_test(&pull_subject("whole-schema-holdback"))
        .await;
    let old_identity = Arc::new(FabricIdentity::generate());
    let old_node = old_identity.node_id();
    let old_client = SyncClient::with_authenticated_transport_and_identity_for_test(
        old.clone(),
        broker.client_as(&old_node),
        TenantId::from("whole-schema-holdback"),
        old_identity,
    )
    .expect("read or construct the declared fixture result");
    running
        .server
        .set_peer_vector_schema_support_for_test(&old_node, false);
    old_client
        .pull_default()
        .await
        .expect("compatible control work remains available to the older edge");

    let current_identity = Arc::new(FabricIdentity::generate());
    let current_node = current_identity.node_id();
    let current_client = SyncClient::with_authenticated_transport_and_identity_for_test(
        current.clone(),
        broker.client_as(&current_node),
        TenantId::from("whole-schema-holdback"),
        current_identity,
    )
    .expect("read or construct the declared fixture result");
    current_client
        .pull_default()
        .await
        .expect("an unrelated capable edge must continue syncing");
    assert_complete_schema(&current, &migration.ddl, "capable second edge");
    support::assert_whole_image_of(&current, &holdback_image(), "capable second edge");
    support::assert_unit_kept_once(&current, &migration.ddl, "capable second edge");
    support::initialize_fixture_callbacks(&current);
    assert_eq!(control_rows(&current), vec![1]);

    // UNRELATED TABLES KEEP SYNCING WHILE THE MIGRATION IS HELD. Not once --
    // repeatedly, to both edges, while the edge that cannot take the migration
    // still holds none of it.
    for value in [2, 3, 4] {
        source
            .execute(
                &format!("INSERT INTO receiver_control VALUES ({value})"),
                &empty(),
            )
            .expect("author control work while migration is held");
        current_client
            .pull_default()
            .await
            .expect("the capable second edge keeps syncing during the first edge's holdback");
        old_client
            .pull_default()
            .await
            .expect("older edge keeps receiving unrelated work");
        assert_schema_absent(&old, "while unrelated work keeps flowing to the held edge");
    }
    assert_eq!(control_rows(&current), vec![1, 2, 3, 4]);
    assert_eq!(control_rows(&old), vec![1, 2, 3, 4]);
    let expected_holdbacks = source
        .table_names()
        .into_iter()
        .filter(|name| {
            source
                .table_meta(name)
                .unwrap()
                .columns
                .iter()
                .any(|column| column.partition_key_columns.is_some())
        })
        .map(|table| SchemaSyncHoldback {
            table,
            capability: SchemaSyncCapability::VectorPartitioning,
            node_to_upgrade: old_node.clone(),
        })
        .collect::<Vec<_>>();
    let actual_holdbacks = running.server.schema_sync_holdbacks();
    assert_eq!(actual_holdbacks.len(), expected_holdbacks.len());
    assert!(
        expected_holdbacks
            .iter()
            .all(|entry| actual_holdbacks.contains(entry))
    );
    assert_schema_absent(&old, "while the authored migration is held");

    drop(current_client);
    running.stop().await;
    source.close().expect("close holdback source for restart");
    drop(source);
    let reopened_source = Arc::new(Database::open(&source_path).expect("reopen holdback source"));
    let restarted = start_server(
        &broker,
        "whole-schema-holdback",
        reopened_source.clone(),
        hub_identity,
    )
    .await;
    restarted
        .server
        .set_peer_vector_schema_support_for_test(&old_node, false);
    old_client
        .pull_default()
        .await
        .expect("restart preserves whole-migration holdback and compatible progress");
    assert_schema_absent(&old, "after sender restart while still held");
    restarted
        .server
        .set_peer_vector_schema_support_for_test(&old_node, true);
    old_client
        .pull_default()
        .await
        .expect("capability upgrade releases the whole authored migration");
    // THE HELD MIGRATION COMMITS COMPLETE, ONCE. Every declaration the author
    // wrote and nothing else, in the author's order, under one receiver commit
    // boundary, carrying the author's own source boundary.
    assert_complete_schema(&old, &migration.ddl, "after holdback release");
    support::assert_whole_image_of(&old, &holdback_image(), "after holdback release");
    support::assert_unit_kept_once(&old, &migration.ddl, "after holdback release");
    assert_eq!(
        source_boundary(&reopened_source, &migration.ddl, migration_lsn),
        authored_boundary,
        "after holdback release: holding the migration back and releasing it moved the author's \
         own source boundary"
    );
    support::initialize_fixture_callbacks(&old);
    old_client
        .pull_default()
        .await
        .expect("callback bootstrap acknowledges the recovered migration");
    assert_eq!(control_rows(&old), vec![1, 2, 3, 4]);
    assert!(
        restarted.server.schema_sync_holdbacks().is_empty(),
        "holdbacks remain after recovery: {:?}",
        restarted.server.schema_sync_holdbacks()
    );
    let old_schema = old
        .changes_since(Lsn(0))
        .expect("read or construct the declared fixture result");
    let start = find_schema_vector(&old_schema, &migration.ddl);
    assert_eq!(
        old_schema.ddl[start..start + migration.ddl.len()],
        migration.ddl,
        "restart release must preserve exact authored order without duplication"
    );

    // A RETRY NEVER APPLIES A PREFIX TWICE. The same peer pulls again, twice,
    // and its durable DDL log does not move at all.
    for attempt in 0..2 {
        old_client
            .pull_default()
            .await
            .expect("a repeated pull after release is accepted");
        let again = old
            .changes_since(Lsn(0))
            .expect("read or construct the declared fixture result");
        assert_eq!(
            again.ddl, old_schema.ddl,
            "retry {attempt} re-applied part of the released migration"
        );
        assert_eq!(
            again.ddl_lsn, old_schema.ddl_lsn,
            "retry {attempt} moved the released migration's commit boundary"
        );
    }
    support::assert_unit_kept_once(&old, &migration.ddl, "after repeated retry");
    support::assert_whole_image_of(&old, &holdback_image(), "after repeated retry");
    assert_eq!(
        source_boundary(&reopened_source, &migration.ddl, migration_lsn),
        authored_boundary,
        "after repeated retry: a retry moved the author's own source boundary"
    );
    drop(old_client);
    restarted.stop().await;
    reopened_source
        .close()
        .expect("close restarted holdback source");
    old.close().expect("close older receiver");
    current.close().expect("close current receiver");
}

#[cfg(feature = "iroh")]
#[path = "schema_migration_chunked_transport.rs"]
mod schema_migration_chunked_transport;
