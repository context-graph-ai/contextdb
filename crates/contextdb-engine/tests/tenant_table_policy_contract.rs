//! The tenant declares its complete table policy before a writer creates governed tables.

use contextdb_core::{
    ConflictPolicy, Error, HistoryPolicy, RetainUnit, SyncDirection, TableMeta, TenantId, Value,
    Wallclock,
};
use contextdb_engine::Database;
use contextdb_engine::sync_types::DdlChange;
use contextdb_server::protocol::{
    MessageType, PullResponse, WireDdlChange, WireDdlProvenance, canonical_ddl_provenance_digest,
    decode, encode,
};
use contextdb_server::sync_client::ApplicationTablePolicyExpectation;
use contextdb_server::transport::iroh::IrohServer;
use contextdb_server::transport::{
    ClientTransport, TransportError, TransportFuture, TransportResult, TransportStatusFuture,
};
use contextdb_server::{FabricIdentity, InProcessBroker, SyncClient, SyncServer, peer_dial_spec};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

/// A fixed millisecond reading for every persisted stamp this file asserts on.
const T0: u64 = 1_700_000_000_000;

/// The root table's declared clauses, as the hub operator writes them.
const ROOT_CLAUSES: &str = "SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST IMMUTABLE \
     DELIVERY MANIFEST OVER record_parts";
/// The member table's declared clauses. It names no member of its own.
const MEMBER_CLAUSES: &str = "SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST IMMUTABLE";
/// The adversarial expectation: `ROOT_CLAUSES` with exactly ONE token changed —
/// the conflict word. Everything else is character-for-character identical, so
/// a comparison that stops at "close enough" passes it and this file fails.
const ROOT_CLAUSES_ONE_TOKEN_OFF: &str = "SYNC PUSH ONLY SYNC CONFLICT KEEP LATEST IMMUTABLE \
     DELIVERY MANIFEST OVER record_parts";

fn p() -> HashMap<String, Value> {
    HashMap::new()
}

async fn within<F: std::future::Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(60), future)
        .await
        .expect("a sync exchange must complete within 60s")
}

fn bind_spec(identity_path: &Path) -> String {
    format!("iroh:?identity={}", identity_path.display())
}

struct Hub {
    db: Arc<Database>,
    ticket: String,
    node_id: String,
    stop: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

impl Hub {
    async fn stop(self) {
        self.stop.store(true, Ordering::SeqCst);
        within(self.task).await.expect("hub stops cleanly");
    }
}

/// Opens `<root>/<name>.db` as a file-backed store and serves it under
/// `tenant`. File-backed because a declaration, a binding and an outcome are
/// durable engine state, and a store with no persistence holds none of them.
async fn start_hub(root: &Path, name: &str, tenant: &str) -> Hub {
    let identity_path = root.join(format!("{name}.db.fabric-identity.key"));
    let endpoint = IrohServer::bind(&bind_spec(&identity_path))
        .await
        .expect("bind the authoritative hub endpoint");
    let ticket = endpoint.ticket();
    let node_id = endpoint.node_id();
    let db = Arc::new(Database::open(root.join(format!("{name}.db"))).expect("open the hub store"));
    let server = Arc::new(SyncServer::new(
        db.clone(),
        &endpoint,
        TenantId::from(tenant),
    ));
    let stop = Arc::new(AtomicBool::new(false));
    let task = tokio::spawn({
        let server = server.clone();
        let stop = stop.clone();
        async move { server.run_until(stop).await }
    });
    Hub {
        db,
        ticket,
        node_id,
        stop,
        task,
    }
}

fn declare(db: &Database, table: &str, clauses: &str) -> contextdb_core::Result<()> {
    db.execute(
        &format!("DECLARE TENANT TABLE POLICY {table} {clauses}"),
        &p(),
    )
    .map(|_| ())
}

fn edge_client(db: &Arc<Database>, ticket: &str, identity_path: &Path, tenant: &str) -> SyncClient {
    SyncClient::new(
        db.clone(),
        &peer_dial_spec(ticket, identity_path),
        TenantId::from(tenant),
    )
}

fn expectation(pairs: &[(&str, &str)]) -> ApplicationTablePolicyExpectation {
    let mut built = ApplicationTablePolicyExpectation::new();
    for (table, clauses) in pairs {
        built = built
            .expect_table(*table, clauses)
            .expect("an expectation is written in the declaration's own clause words");
    }
    built
}

/// The rendered answer of a `SHOW`, as `(columns, rows)`, with every cell
/// rendered to text so a comparison is exact rather than shape-only.
fn shown(db: &Database, sql: &str) -> (Vec<String>, Vec<Vec<String>>) {
    let result = db
        .execute(sql, &p())
        .unwrap_or_else(|err| panic!("{sql} must answer: {err}"));
    let rows = result
        .rows
        .iter()
        .map(|row| row.iter().map(|cell| format!("{cell:?}")).collect())
        .collect();
    (result.columns.clone(), rows)
}

fn named_cell<'a>(columns: &[String], row: &'a [Value], name: &str) -> &'a Value {
    let index = columns
        .iter()
        .position(|column| column == name)
        .unwrap_or_else(|| panic!("the result includes its {name} column"));
    &row[index]
}

/// The clause values a table's persisted policy actually carries. This is the
/// whole declared axis set the arriving-schema door can move, so comparing it
/// before and after proves the local policy is untouched rather than merely
/// untouched on the one axis the test happened to look at.
type DeclaredAxes = (
    Option<ConflictPolicy>,
    Option<SyncDirection>,
    Option<u64>,
    bool,
    Option<RetainUnit>,
    Option<HistoryPolicy>,
    bool,
    Option<Vec<String>>,
    Option<contextdb_core::EdgeDiscardMode>,
);

fn declared_axes(meta: &TableMeta) -> DeclaredAxes {
    (
        meta.conflict_policy,
        meta.sync_direction,
        meta.default_ttl_seconds,
        meta.sync_safe,
        meta.retain_declared_unit,
        meta.history_policy,
        meta.immutable,
        meta.delivery_manifest_tables.clone(),
        meta.edge_discard,
    )
}

/// Forwards a real authenticated hub pull, then replaces only
/// its received representation with the otherwise-unproducible bound-table
/// declaration. This fixture is deliberately transport-local: production
/// hubs must never manufacture this invalid policy.
struct AdversarialBoundTablePull {
    inner: Arc<dyn ClientTransport>,
}

impl AdversarialBoundTablePull {
    fn alter_received_pull(&self, bytes: Vec<u8>) -> TransportResult<Vec<u8>> {
        let envelope = decode(&bytes).map_err(|error| TransportError::Other(error.to_string()))?;
        // Adapter: the ordinary route may carry dependency-complete
        // units. Preserve those real units and mutate their received DDL only.
        let mut complete = if envelope.message_type == MessageType::DependencyCompletePullResponse {
            Some(rmp_serde::from_slice::<contextdb_server::protocol::DependencyCompletePullResponse>(&envelope.payload)
                .map_err(|error| TransportError::Other(error.to_string()))?)
        } else {
            None
        };
        let mut response: PullResponse = if let Some(complete) = &complete {
            let mut response = complete.ordinary.clone();
            if response.changeset.rows.is_empty() {
                response.changeset = complete
                    .units
                    .first()
                    .ok_or_else(|| TransportError::Other("missing actual pull unit".into()))?
                    .clone();
            }
            response
        } else {
            rmp_serde::from_slice(&envelope.payload)
                .map_err(|error| TransportError::Other(error.to_string()))?
        };
        let ddl_lsn = response
            .changeset
            .rows
            .first()
            .map(|row| row.lsn)
            .ok_or_else(|| {
                TransportError::Other("fixture requires the real unrelated hub row".to_string())
            })?;
        let ddl: WireDdlChange = DdlChange::CreateTable {
            name: "records".into(),
            columns: vec![
                ("id".into(), "UUID PRIMARY KEY".into()),
                ("body".into(), "TEXT".into()),
            ],
            constraints: vec![ROOT_CLAUSES_ONE_TOKEN_OFF.into()],
            foreign_keys: Vec::new(),
            composite_foreign_keys: Vec::new(),
            composite_unique: Vec::new(),
        }
        .into();
        let provenance = WireDdlProvenance {
            source_ddl_lsn: ddl_lsn,
            ordinal: 0,
            table: Some("records".into()),
            table_generation: Some(1),
            digest: canonical_ddl_provenance_digest(&ddl, ddl_lsn, 0, Some("records"), Some(1))
                .map_err(|error| TransportError::Other(error.to_string()))?,
        };
        response.changeset.ddl.insert(0, ddl);
        response.changeset.ddl_lsn.insert(0, ddl_lsn);
        response.changeset.ddl_provenance.insert(0, provenance);
        if let Some(complete) = &mut complete {
            if complete.ordinary.changeset.rows.is_empty() {
                complete.units[0] = response.changeset;
            } else {
                complete.ordinary = response;
            }
            encode(MessageType::DependencyCompletePullResponse, complete)
        } else {
            encode(MessageType::PullResponse, &response)
        }
        .map_err(|error| TransportError::Other(error.to_string()))
    }
}

impl ClientTransport for AdversarialBoundTablePull {
    fn ensure_connected<'a>(&'a self) -> TransportFuture<'a, ()> {
        self.inner.ensure_connected()
    }

    fn reconnect<'a>(&'a self) -> TransportFuture<'a, ()> {
        self.inner.reconnect()
    }

    fn is_connected<'a>(&'a self) -> TransportStatusFuture<'a> {
        self.inner.is_connected()
    }

    fn peer_node_id(&self) -> Option<String> {
        self.inner.peer_node_id()
    }

    fn local_node_id(&self) -> Option<String> {
        self.inner.local_node_id()
    }

    fn has_stable_edge_identity(&self) -> bool {
        self.inner.has_stable_edge_identity()
    }

    fn request<'a>(
        &'a self,
        subject: &'a str,
        request_bytes: Vec<u8>,
        timeout: Duration,
    ) -> TransportFuture<'a, Vec<u8>> {
        let inner = self.inner.clone();
        Box::pin(async move {
            let response = inner.request(subject, request_bytes, timeout).await?;
            self.alter_received_pull(response)
        })
    }

    fn request_single_reply<'a>(
        &'a self,
        subject: &'a str,
        request_bytes: Vec<u8>,
        timeout: Duration,
    ) -> TransportFuture<'a, Vec<u8>> {
        self.inner
            .request_single_reply(subject, request_bytes, timeout)
    }

    fn ensure_single_reply_retry_safe(&self, request_bytes: &[u8]) -> TransportResult<()> {
        self.inner.ensure_single_reply_retry_safe(request_bytes)
    }

    fn shutdown<'a>(&'a self) -> TransportFuture<'a, ()> {
        self.inner.shutdown()
    }
}

// ---------------------------------------------------------------------------

// The hub declares durable table policy; an edge must be refused.
#[tokio::test]
async fn a_declaration_persists_across_restart_and_restore_renders_with_version_and_digest_and_is_refused_on_an_edge()
 {
    let _clock = Wallclock::test_clock_guard(|| T0);
    let root = tempfile::tempdir().expect("temporary test directory");
    let tenant = "declared-tenant-policy";
    let hub = start_hub(root.path(), "hub", tenant).await;

    declare(&hub.db, "records", ROOT_CLAUSES).expect("the hub operator declares the root table");
    declare(&hub.db, "record_parts", MEMBER_CLAUSES)
        .expect("the hub operator declares the member table");

    let (columns, rows) = shown(&hub.db, "SHOW TENANT TABLE POLICY");
    for name in [
        "table",
        "version",
        "digest",
        "sync_direction",
        "sync_conflict",
        "immutable",
        "manifest_tables",
        "edge_discard",
    ] {
        assert!(
            columns.iter().any(|column| column == name),
            "policy reports {name}"
        );
    }
    assert_eq!(
        rows.len(),
        2,
        "both declared tables render, and only those two"
    );

    let root_row = shown(&hub.db, "SHOW TENANT TABLE POLICY FOR records");
    assert_eq!(
        root_row.1.len(),
        1,
        "the single-table form renders exactly the table asked for"
    );
    let rendered = &root_row.1[0];
    assert_eq!(
        rendered[0], "Text(\"records\")",
        "the rendered row names the declared table"
    );
    assert_eq!(rendered[2], "Int64(1)", "a first declaration is version 1");
    let digest = rendered[3].clone();
    assert_eq!(
        digest.len(),
        "Text(\"\")".len() + 64,
        "the declaration carries a full canonical digest over its clauses"
    );
    let raw_root_policy = hub
        .db
        .execute("SHOW TENANT TABLE POLICY FOR records", &p())
        .expect("the declared root policy answers");
    assert_eq!(
        named_cell(
            &raw_root_policy.columns,
            &raw_root_policy.rows[0],
            "manifest_tables",
        ),
        &Value::Json(serde_json::json!(["record_parts"])),
        "the root's declaration names the member table its units may draw from as the \
         policy array the query surface returns"
    );
    assert_eq!(
        named_cell(
            &raw_root_policy.columns,
            &raw_root_policy.rows[0],
            "edge_discard"
        ),
        &Value::Text("after_outcome".into()),
        "silence on the discard clause renders as the effective mode, because that \
         value refuses a later erasure"
    );

    assert_eq!(
        named_cell(
            &raw_root_policy.columns,
            &raw_root_policy.rows[0],
            "tenant_id"
        ),
        &Value::Text(tenant.into()),
        "the declaration belongs to the tenant this hub serves"
    );

    // A table that was never declared answers with nothing, rather than with a
    // default someone could mistake for a declaration.
    assert!(
        shown(&hub.db, "SHOW TENANT TABLE POLICY FOR record_extracts")
            .1
            .is_empty(),
        "an undeclared table renders no policy row"
    );

    // Restart: stop and reap the original owner before reopening its file.
    let original_node_id = hub.node_id.clone();
    hub.stop().await;
    let restarted =
        Database::open(root.path().join("hub.db")).expect("reopen the hub store from its file");
    assert_eq!(
        shown(&restarted, "SHOW TENANT TABLE POLICY"),
        (columns.clone(), rows.clone()),
        "the declaration is durable engine state and reopening changes none of it"
    );

    // Checkpoint and restore: the copy answers exactly as the original.
    let snapshot = root.path().join("hub-snapshot.db");
    restarted
        .export_snapshot(&snapshot)
        .expect("checkpoint the declaring store");
    let restored = Database::open(&snapshot).expect("open the restored checkpoint");
    assert_eq!(
        shown(&restored, "SHOW TENANT TABLE POLICY"),
        (columns, rows),
        "a restored checkpoint carries the declaration with its version and digest"
    );
    drop(restored);
    drop(restarted);

    // Restart the same store and identity before proving the enrolled-edge
    // refusal. The binding below must refer to this restarted authority.
    let hub = start_hub(root.path(), "hub", tenant).await;
    assert_eq!(
        hub.node_id, original_node_id,
        "the restarted hub keeps the identity the enrolled edge records"
    );

    // The same statement on an enrolled edge is refused, and the refusal names
    // the node the operator must run it on.
    let edge_identity = root.path().join("edge.db.fabric-identity.key");
    let edge = Arc::new(Database::open(root.path().join("edge.db")).expect("open the edge store"));
    let client = edge_client(&edge, &hub.ticket, &edge_identity, tenant);
    within(client.push())
        .await
        .expect("the edge enrols with its one authoritative hub");
    assert_eq!(
        edge.retention_sync_peer().as_deref(),
        Some(hub.node_id.as_str()),
        "premise: this node is an enrolled edge of the hub under test"
    );

    match declare(&edge, "records", ROOT_CLAUSES) {
        Err(Error::DeclareRequiresAuthoritativeHub { hub_node_id }) => assert_eq!(
            hub_node_id, hub.node_id,
            "the refusal names the node the operator must declare on"
        ),
        other => panic!("an edge must be refused the declaration, got {other:?}"),
    }
    assert!(
        shown(&edge, "SHOW TENANT TABLE POLICY").1.is_empty(),
        "the refused declaration wrote nothing on the edge"
    );

    within(client.shutdown()).await;
    hub.stop().await;
}

// The first binding freezes a declaration; earlier replacement advances its version.
#[tokio::test]
async fn the_first_binding_freezes_the_declaration_and_replacement_before_any_binding_bumps_the_version()
 {
    let _clock = Wallclock::test_clock_guard(|| T0);
    let root = tempfile::tempdir().expect("temporary test directory");
    let tenant = "frozen-after-first-binding";
    let hub = start_hub(root.path(), "hub", tenant).await;

    declare(&hub.db, "records", MEMBER_CLAUSES).expect("the first declaration");
    let first_rows = shown(&hub.db, "SHOW TENANT TABLE POLICY FOR records").1;
    assert_eq!(
        first_rows.len(),
        1,
        "the initial declaration is durably inspectable before replacement"
    );
    let first_digest = first_rows[0][3].clone();

    // Before any binding, the operator may correct the declaration.
    declare(&hub.db, "records", ROOT_CLAUSES).expect("replacement before any binding succeeds");
    let replaced = shown(&hub.db, "SHOW TENANT TABLE POLICY FOR records").1[0].clone();
    assert_eq!(
        replaced[2], "Int64(2)",
        "a replacement made before any binding bumps the version"
    );
    assert_ne!(
        replaced[3], first_digest,
        "the replacement changed the clauses, so it changed the digest"
    );

    declare(&hub.db, "record_parts", MEMBER_CLAUSES).expect("the member table's declaration");

    // The first binding freezes it.
    let edge_identity = root.path().join("edge.db.fabric-identity.key");
    let edge = Arc::new(Database::open(root.path().join("edge.db")).expect("open the edge store"));
    let client = edge_client(&edge, &hub.ticket, &edge_identity, tenant);
    let binding = within(client.bind_application_table_policy(expectation(&[
        ("records", ROOT_CLAUSES),
        ("record_parts", MEMBER_CLAUSES),
    ])))
    .await
    .expect("an expectation equal to the declaration binds");
    assert_eq!(
        binding.tables()["records"].version(),
        2,
        "the binding carries the declaration version it agreed to"
    );

    let frozen = shown(&hub.db, "SHOW TENANT TABLE POLICY FOR records").1[0].clone();
    match declare(&hub.db, "records", MEMBER_CLAUSES) {
        Err(Error::TenantPolicyBound { table }) => {
            assert_eq!(table, "records", "the refusal names the frozen table");
        }
        other => panic!("a bound declaration must refuse replacement, got {other:?}"),
    }
    assert_eq!(
        shown(&hub.db, "SHOW TENANT TABLE POLICY FOR records").1[0],
        frozen,
        "the refused replacement changed nothing — not the clauses, not the version, \
         not the digest, not the homing"
    );

    within(client.shutdown()).await;
    hub.stop().await;
}

// Binding compares declared clauses and repeats without changing schema or rows.
#[tokio::test]
async fn a_matching_expectation_binds_a_differing_clause_and_an_undeclared_table_are_typed_refusals_and_none_writes_on_either_side()
 {
    let _clock = Wallclock::test_clock_guard(|| T0);
    let root = tempfile::tempdir().expect("temporary test directory");
    let tenant = "expectation-comparison";
    let hub = start_hub(root.path(), "hub", tenant).await;

    hub.db
        .__seed_tenant_table_policies_for_test(
            TenantId::from(tenant),
            expectation(&[("records", ROOT_CLAUSES), ("record_parts", MEMBER_CLAUSES)]),
        )
        .expect("canonical declarations before binding target");

    let edge_identity = root.path().join("edge.db.fabric-identity.key");
    let edge = Arc::new(Database::open(root.path().join("edge.db")).expect("open the edge store"));
    let client = edge_client(&edge, &hub.ticket, &edge_identity, tenant);

    let binding = within(client.bind_application_table_policy(expectation(&[
        ("records", ROOT_CLAUSES),
        ("record_parts", MEMBER_CLAUSES),
    ])))
    .await
    .expect("the matching expectation binds");
    assert_eq!(
        binding.hub_node_id(),
        hub.node_id,
        "the binding names the hub that answered, read off the authenticated \
         connection rather than off the reply's own bytes"
    );
    assert_eq!(
        binding.tenant_id(),
        &TenantId::from(tenant),
        "the binding names the tenant the hub serves"
    );
    assert_eq!(
        binding.tables()["records"].version(),
        1,
        "the binding carries the declaration version"
    );
    assert_eq!(
        format!(
            "Text(\"{}\")",
            binding.tables()["records"]
                .digest()
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        ),
        shown(&hub.db, "SHOW TENANT TABLE POLICY FOR records").1[0][3],
        "the binding's digest is the hub's own declaration digest, not a second \
         opinion computed at the edge"
    );
    assert_eq!(
        binding.tables().len(),
        2,
        "the binding carries the effective clauses of every table it agreed"
    );

    assert_eq!(
        binding.hub_incarnation(),
        hub.db.sync_incarnation(&TenantId::from(tenant)).unwrap()
    );
    let expected = expectation(&[("records", ROOT_CLAUSES), ("record_parts", MEMBER_CLAUSES)]);
    for (table, policy) in &expected.tables {
        assert_eq!(binding.tables()[table].policy(), policy);
        assert!(edge.table_meta(table).is_none());
        assert!(hub.db.table_meta(table).is_none());
    }
    let before_repeat = (hub.db.current_lsn(), edge.current_lsn());
    assert_eq!(
        within(client.bind_application_table_policy(expected))
            .await
            .unwrap(),
        binding
    );
    assert_eq!(
        (hub.db.current_lsn(), edge.current_lsn()),
        before_repeat,
        "repeating an identical binding writes nothing"
    );

    // Everything a refusal must not disturb, captured before the two refusals.
    let hub_policy_before = shown(&hub.db, "SHOW TENANT TABLE POLICY");
    let hub_lsn_before = hub.db.current_lsn();
    let edge_bindings_before = shown(&edge, "SHOW SYNC BINDINGS");
    assert_eq!(
        edge_bindings_before.1.len(),
        2,
        "the persisted binding is what the refusals below must leave alone"
    );
    assert_ne!(
        edge_bindings_before.1[0][2], "Null",
        "the binding names which life of the hub database agreed to it"
    );
    let edge_lsn_before = edge.current_lsn();

    // One token differs: KEEP LATEST where the hub declared KEEP FIRST.
    match within(
        client
            .bind_application_table_policy(expectation(&[("records", ROOT_CLAUSES_ONE_TOKEN_OFF)])),
    )
    .await
    {
        Err(Error::TenantPolicyMismatch { table, clause }) => {
            assert_eq!(table, "records", "the refusal names the table");
            assert_eq!(
                clause, "conflict",
                "the refusal names the one clause that differs, so the operator does \
                 not have to diff two declarations by eye"
            );
        }
        other => panic!("a differing clause must be a typed refusal, got {other:?}"),
    }

    // A table the hub never declared.
    match within(
        client.bind_application_table_policy(expectation(&[("record_extracts", MEMBER_CLAUSES)])),
    )
    .await
    {
        Err(Error::TenantPolicyNotDeclared { table }) => assert_eq!(
            table, "record_extracts",
            "the refusal names the table that carries no declaration"
        ),
        other => panic!("an undeclared table must be a typed refusal, got {other:?}"),
    }

    assert_eq!(
        shown(&hub.db, "SHOW TENANT TABLE POLICY"),
        hub_policy_before,
        "neither refusal wrote anything on the hub"
    );
    assert_eq!(
        hub.db.current_lsn(),
        hub_lsn_before,
        "neither refusal advanced the hub's log"
    );
    assert_eq!(
        shown(&edge, "SHOW SYNC BINDINGS"),
        edge_bindings_before,
        "neither refusal installed a binding on the edge"
    );
    assert_eq!(
        edge.current_lsn(),
        edge_lsn_before,
        "neither refusal advanced the edge's log"
    );
    assert!(
        edge.table_meta("record_extracts").is_none(),
        "a refused expectation never creates the table it asked about"
    );

    within(client.shutdown()).await;
    hub.stop().await;
}

// Only the registered hub installs a binding, which survives restart.
#[tokio::test]
async fn a_binding_reply_from_a_foreign_node_is_refused_and_a_persisted_binding_survives_restart_and_renders_without_rows()
 {
    let _clock = Wallclock::test_clock_guard(|| T0);
    let root = tempfile::tempdir().expect("temporary test directory");
    let tenant = "one-authoritative-hub";
    let registered = start_hub(root.path(), "hub", tenant).await;
    // A second, fully working hub serving the same tenant and holding the same
    // declaration. Its answer is correct in every respect except who sent it.
    // Its existing in-process exchange ledger witnesses that the
    // edge refuses before the bind request can reach this foreign hub.
    let foreign_broker = InProcessBroker::new();
    let foreign_identity = Arc::new(FabricIdentity::generate());
    let foreign_node_id = foreign_identity.node_id();
    let foreign_db = Arc::new(Database::open_memory());
    let foreign_server = Arc::new(
        SyncServer::with_authenticated_transport_and_identity_for_test(
            foreign_db.clone(),
            foreign_broker.server_as(&foreign_node_id),
            TenantId::from(tenant),
            foreign_node_id.clone(),
            foreign_identity,
        ),
    );
    let foreign_stop = Arc::new(AtomicBool::new(false));
    let foreign_task = tokio::spawn({
        let foreign_server = foreign_server.clone();
        let foreign_stop = foreign_stop.clone();
        async move { foreign_server.run_until(foreign_stop).await }
    });
    within(
        foreign_broker.wait_for_registered_route_for_test(
            &contextdb_server::subjects::binding_subject(tenant),
        ),
    )
    .await;
    assert_ne!(
        registered.node_id, foreign_node_id,
        "premise: the two hubs are different nodes"
    );

    for hub in [&registered.db, &foreign_db] {
        hub.__seed_tenant_table_policies_for_test(
            TenantId::from(tenant),
            expectation(&[("records", ROOT_CLAUSES), ("record_parts", MEMBER_CLAUSES)]),
        )
        .expect("canonical declarations before authenticated binding");
    }

    let edge_identity = root.path().join("edge.db.fabric-identity.key");
    let edge_path = root.path().join("edge.db");
    let edge = Arc::new(Database::open(&edge_path).expect("open the edge store"));

    // Enrol with the registered hub first, so the edge has one authoritative
    // hub on record and the foreign reply is provably the wrong sender rather
    // than merely the first one.
    let registered_client = edge_client(&edge, &registered.ticket, &edge_identity, tenant);
    within(registered_client.push())
        .await
        .expect("the edge enrols with its one authoritative hub");

    let foreign_edge_identity = Arc::new(FabricIdentity::generate());
    let foreign_client = SyncClient::with_authenticated_transport_and_identity_for_test(
        edge.clone(),
        foreign_broker.client_as(&foreign_edge_identity.node_id()),
        TenantId::from(tenant),
        foreign_edge_identity,
    );
    assert!(
        foreign_broker.recorded_exchanges().is_empty(),
        "premise: no foreign exchange occurred before the refused bind"
    );
    let refusal = within(
        foreign_client.bind_application_table_policy(expectation(&[("records", ROOT_CLAUSES)])),
    )
    .await;
    assert!(
        refusal.is_err(),
        "a bind targeted at a node other than the registered hub is refused, got {refusal:?}"
    );
    assert!(
        foreign_broker.recorded_exchanges().is_empty(),
        "the foreign authoritative hub received no bind exchange"
    );
    assert!(
        shown(&edge, "SHOW SYNC BINDINGS").1.is_empty(),
        "the refused reply installed no binding"
    );

    let binding = within(
        registered_client.bind_application_table_policy(expectation(&[
            ("records", ROOT_CLAUSES),
            ("record_parts", MEMBER_CLAUSES),
        ])),
    )
    .await
    .expect("the registered hub's reply binds");
    assert_eq!(binding.hub_node_id(), registered.node_id);

    within(registered_client.shutdown()).await;
    within(foreign_client.shutdown()).await;
    drop(registered_client);
    drop(foreign_client);
    drop(edge);

    let reopened = Database::open(&edge_path).expect("reopen the edge store from its file");
    let (columns, rows) = shown(&reopened, "SHOW SYNC BINDINGS");
    for name in [
        "table",
        "tenant_id",
        "hub_node_id",
        "hub_incarnation",
        "version",
        "digest",
    ] {
        assert!(
            columns.iter().any(|column| column == name),
            "binding reports {name}"
        );
    }
    assert_eq!(rows.len(), 2, "both bound tables survive the restart");
    let records_row = rows
        .iter()
        .find(|row| row[0] == "Text(\"records\")")
        .expect("the root table's binding survives");
    assert_eq!(
        records_row[2],
        format!("Text({:?})", registered.node_id),
        "the surviving binding names the hub it was agreed with"
    );
    assert_eq!(
        records_row[6], "Int64(1)",
        "the binding retains the declaration version"
    );

    registered.stop().await;
    foreign_stop.store(true, Ordering::SeqCst);
    within(foreign_task)
        .await
        .expect("foreign hub stops cleanly");
}

// Both DDL doors enforce bound clauses while other tables still apply.
#[tokio::test]
async fn a_bound_edge_refuses_a_mismatching_local_or_arriving_declaration_per_table_while_other_tables_keep_applying()
 {
    let _clock = Wallclock::test_clock_guard(|| T0);
    let root = tempfile::tempdir().expect("temporary test directory");
    let tenant = "bound-table-doors";
    let hub = start_hub(root.path(), "hub", tenant).await;
    hub.db
        .__seed_tenant_table_policies_for_test(
            TenantId::from(tenant),
            expectation(&[("records", ROOT_CLAUSES), ("record_parts", MEMBER_CLAUSES)]),
        )
        .expect("canonical declarations before DDL enforcement");
    let edge_identity = root.path().join("edge.db.fabric-identity.key");
    let edge = Arc::new(Database::open(root.path().join("edge.db")).expect("open the edge store"));
    let client = edge_client(&edge, &hub.ticket, &edge_identity, tenant);
    within(
        client.__seed_application_table_policy_binding_for_test(expectation(&[
            ("records", ROOT_CLAUSES),
            ("record_parts", MEMBER_CLAUSES),
        ])),
    )
    .await
    .expect("the edge binds both tables");

    // The local door: one token off is refused, naming the clause.
    match edge.execute(
        &format!(
            "CREATE TABLE records (id UUID PRIMARY KEY, body TEXT) {ROOT_CLAUSES_ONE_TOKEN_OFF}"
        ),
        &p(),
    ) {
        Err(Error::TableBindingMismatch { table, clause }) => {
            assert_eq!(table, "records", "the refusal names the bound table");
            assert_eq!(clause, "conflict", "the refusal names the differing clause");
        }
        other => panic!("a bound table refuses a differing local creation, got {other:?}"),
    }
    assert!(
        edge.table_meta("records").is_none(),
        "the refused creation installed no table"
    );

    // The same door admits the shape the edge agreed to.
    edge.execute(
        &format!("CREATE TABLE records (id UUID PRIMARY KEY, body TEXT) {ROOT_CLAUSES}"),
        &p(),
    )
    .expect("the bound shape is admitted");
    edge.execute(
        &format!("CREATE TABLE record_parts (id UUID PRIMARY KEY, records_id UUID, body TEXT) {MEMBER_CLAUSES}"),
        &p(),
    )
    .expect("the bound member shape is admitted");
    // A table this edge never bound is not touched by any of it.
    edge.execute(
        "CREATE TABLE unrelated_notes (id UUID PRIMARY KEY, body TEXT) \
         SYNC TWO WAY SYNC CONFLICT KEEP LATEST",
        &p(),
    )
    .expect("an unbound table is unaffected by the binding");

    let bound_axes = declared_axes(
        &edge
            .table_meta("records")
            .expect("the bound table's policy"),
    );

    edge.execute("ALTER TABLE records SET SYNC CONFLICT KEEP FIRST", &p())
        .expect("a matching alteration is admitted");

    // The alter door asks the same question of the post-alter shape.
    match edge.execute("ALTER TABLE records SET SYNC CONFLICT KEEP LATEST", &p()) {
        Err(Error::TableBindingMismatch { table, clause }) => {
            assert_eq!(table, "records");
            assert_eq!(clause, "conflict");
        }
        other => panic!("a bound table refuses a differing local alteration, got {other:?}"),
    }
    assert_eq!(
        declared_axes(
            &edge
                .table_meta("records")
                .expect("the bound table's policy")
        ),
        bound_axes,
        "the refused alteration moved no clause of the bound table"
    );

    // Prepare the unrelated table through real hub writes, then place an
    // adversarial declaration in that SAME received batch. The hub cannot
    // legally create this mismatching bound shape as a fixture prerequisite.
    hub.db
        .execute(
            "CREATE TABLE unrelated_notes (id UUID PRIMARY KEY, body TEXT) \
             SYNC TWO WAY SYNC CONFLICT KEEP LATEST",
            &p(),
        )
        .expect("the hub's copy of the unbound table");
    hub.db
        .execute(
            "INSERT INTO unrelated_notes (id, body) VALUES ($id, $body)",
            &HashMap::from([
                ("id".to_string(), Value::Uuid(uuid::Uuid::new_v4())),
                (
                    "body".to_string(),
                    Value::Text("carried in the same received batch".to_string()),
                ),
            ]),
        )
        .expect("a row on the unbound table");

    // The hub remains the source of the unrelated row. A local
    // authenticated adapter changes only that real pull response to carry the
    // adversarial DDL that an honest hub cannot create, so SyncClient's actual
    // receive handler applies both entries in the same exchange.
    let broker = InProcessBroker::new();
    let hub_identity = Arc::new(
        FabricIdentity::load_or_generate(&root.path().join("hub.db.fabric-identity.key"))
            .expect("load the real hub identity for the authenticated route"),
    );
    assert_eq!(hub_identity.node_id(), hub.node_id);
    let routed_hub = Arc::new(
        SyncServer::with_authenticated_transport_and_identity_for_test(
            hub.db.clone(),
            broker.server_as(&hub.node_id),
            TenantId::from(tenant),
            hub.node_id.clone(),
            hub_identity,
        ),
    );
    let routed_stop = Arc::new(AtomicBool::new(false));
    let routed_task = tokio::spawn({
        let routed_hub = routed_hub.clone();
        let routed_stop = routed_stop.clone();
        async move { routed_hub.run_until(routed_stop).await }
    });
    within(
        broker
            .wait_for_registered_route_for_test(&contextdb_server::subjects::pull_subject(tenant)),
    )
    .await;
    let routed_edge_identity = Arc::new(
        FabricIdentity::load_or_generate(&edge_identity)
            .expect("load the edge identity already registered with the hub"),
    );
    let adversarial_client = SyncClient::with_authenticated_transport_and_identity_for_test(
        edge.clone(),
        Arc::new(AdversarialBoundTablePull {
            inner: broker.client_as(&routed_edge_identity.node_id()),
        }),
        TenantId::from(tenant),
        routed_edge_identity,
    );
    let applied = within(adversarial_client.pull_default())
        .await
        .expect("a declaration refusal is per table, not a whole-batch failure");
    assert_eq!(
        applied.applied_rows, 1,
        "the other table applies in this batch"
    );
    assert_eq!(
        applied.conflicts.len(),
        1,
        "only the differing table is refused"
    );
    let refusal = &applied.conflicts[0];
    let expected = Error::TableBindingMismatch {
        table: "records".into(),
        clause: "conflict".into(),
    }
    .to_string();
    assert_eq!(
        refusal.reason.as_deref(),
        Some(expected.as_str()),
        "arriving DDL reports TableBindingMismatch for records.conflict"
    );
    assert!(
        refusal.table.is_none(),
        "this is a schema refusal, not a row conflict"
    );

    assert_eq!(
        declared_axes(
            &edge
                .table_meta("records")
                .expect("the bound table's policy")
        ),
        bound_axes,
        "arriving schema for a bound table is refused per table and moves no clause"
    );
    assert_eq!(
        edge.execute("SELECT id FROM unrelated_notes", &p())
            .expect("the unbound table still answers")
            .rows
            .len(),
        1,
        "the unrelated row from the same received batch was committed"
    );

    assert!(
        broker.recorded_exchanges().iter().any(|exchange| {
            exchange.subject == contextdb_server::subjects::pull_subject(tenant)
        }),
        "the result came through one authenticated hub-to-edge pull"
    );

    within(adversarial_client.shutdown()).await;
    routed_stop.store(true, Ordering::SeqCst);
    within(routed_task)
        .await
        .expect("the authenticated test route stops cleanly");
    within(client.shutdown()).await;
    hub.stop().await;
}

// Explicit local policy survives arriving DDL; undeclared policy adopts and relays.
#[tokio::test]
async fn a_declared_local_policy_is_preserved_against_arriving_ddl_and_an_undeclared_table_still_adopts_and_relays()
 {
    let _clock = Wallclock::test_clock_guard(|| T0);
    let root = tempfile::tempdir().expect("temporary test directory");
    let tenant = "declared-policy-preserved";
    let hub = start_hub(root.path(), "hub", tenant).await;

    // The receiving node's own explicit declaration. The operator wrote the
    // conflict word here on purpose.
    hub.db
        .execute(
            "CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT) \
             SYNC TWO WAY SYNC CONFLICT KEEP FIRST",
            &p(),
        )
        .expect("the receiving node declares its own policy for this table");
    // A second table the receiving node holds with NO declared clause of its
    // own. Adoption for this one must keep working exactly as it does today.
    hub.db
        .execute("CREATE TABLE memos (id UUID PRIMARY KEY, body TEXT)", &p())
        .expect("the receiving node holds an undeclared table");

    let declared_before =
        declared_axes(&hub.db.table_meta("notes").expect("declared table policy"));
    assert_eq!(
        declared_before.0,
        Some(ConflictPolicy::KEEP_FIRST),
        "premise: the receiving node's policy for this table is explicitly declared"
    );
    assert_eq!(
        hub.db
            .table_meta("memos")
            .expect("undeclared table policy")
            .conflict_policy,
        None,
        "premise: the receiving node declared no conflict policy for the second table"
    );

    // The sender declares a DIFFERENT policy for the declared table and the
    // same different policy for the undeclared one, and pushes both on one
    // connection.
    let edge_identity = root.path().join("edge.db.fabric-identity.key");
    let edge = Arc::new(Database::open(root.path().join("edge.db")).expect("open the edge store"));
    edge.execute(
        "CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT) \
         SYNC TWO WAY SYNC CONFLICT KEEP LATEST",
        &p(),
    )
    .expect("the sender's copy of the declared table");
    edge.execute(
        "CREATE TABLE memos (id UUID PRIMARY KEY, body TEXT) \
         SYNC TWO WAY SYNC CONFLICT KEEP LATEST",
        &p(),
    )
    .expect("the sender's copy of the undeclared table");
    for (table, body) in [("notes", "sender note"), ("memos", "sender memo")] {
        edge.execute(
            &format!("INSERT INTO {table} (id, body) VALUES ($id, $body)"),
            &HashMap::from([
                ("id".to_string(), Value::Uuid(uuid::Uuid::new_v4())),
                ("body".to_string(), Value::Text(body.to_string())),
            ]),
        )
        .expect("a row so the schema travels with ordinary work");
    }

    let client = edge_client(&edge, &hub.ticket, &edge_identity, tenant);
    let applied = within(client.push())
        .await
        .expect("the connection carries both tables");

    assert_eq!(
        declared_axes(&hub.db.table_meta("notes").expect("declared table policy")),
        declared_before,
        "an arriving declaration never replaces a policy this node declared for \
         itself: the operator's own clause set must stand, clause for clause"
    );

    assert_eq!(
        applied.conflicts.len(),
        1,
        "the arriving declaration is refused for that one table while its peer continues"
    );
    let refusal = &applied.conflicts[0];
    let expected = Error::DeclaredPolicyPreserved {
        table: "notes".into(),
        clause: "conflict".into(),
    }
    .to_string();
    assert_eq!(
        refusal.reason.as_deref(),
        Some(expected.as_str()),
        "arriving DDL reports DeclaredPolicyPreserved for notes.conflict"
    );
    assert!(
        refusal.table.is_none(),
        "this refusal is about a table's policy, not about one transmitted row, and \
         the sender's accounting must be able to tell the two apart"
    );

    assert_eq!(
        hub.db
            .table_meta("memos")
            .expect("undeclared table policy")
            .conflict_policy,
        Some(ConflictPolicy::KEEP_LATEST),
        "a table this node never declared for itself still adopts the arriving \
         declaration exactly as it does today"
    );

    let relay_db = Arc::new(Database::open_memory());
    let relay = edge_client(
        &relay_db,
        &hub.ticket,
        &root.path().join("relay.key"),
        tenant,
    );
    within(relay.pull_default()).await.unwrap();
    assert_eq!(
        relay_db.table_meta("memos").unwrap().conflict_policy,
        Some(ConflictPolicy::KEEP_LATEST)
    );
    assert_eq!(
        hub.db.execute("SELECT body FROM memos", &p()).unwrap().rows,
        vec![vec![Value::Text("sender memo".into())]]
    );
    assert_eq!(
        relay_db
            .execute("SELECT body FROM memos", &p())
            .unwrap()
            .rows,
        vec![vec![Value::Text("sender memo".into())]]
    );
    within(relay.shutdown()).await;

    // DECLARE protects a name before CREATE and before binding.
    declare(&hub.db, "records", ROOT_CLAUSES).unwrap();
    declare(&hub.db, "record_parts", MEMBER_CLAUSES).unwrap();
    let declared = shown(&hub.db, "SHOW TENANT TABLE POLICY");
    let stranger_db = Arc::new(Database::open(root.path().join("stranger.db")).unwrap());
    let stranger = edge_client(
        &stranger_db,
        &hub.ticket,
        &root.path().join("stranger.key"),
        tenant,
    );
    stranger_db.execute("BEGIN", &p()).unwrap();
    stranger_db.execute("CREATE TABLE records (id UUID PRIMARY KEY, body TEXT) SYNC TWO WAY SYNC CONFLICT KEEP LATEST",&p()).unwrap();
    stranger_db
        .execute(
            "CREATE TABLE ordinary_install (id INTEGER PRIMARY KEY)",
            &p(),
        )
        .unwrap();
    stranger_db.execute("COMMIT", &p()).unwrap();
    stranger_db
        .execute("INSERT INTO ordinary_install VALUES (1)", &p())
        .unwrap();
    let response = within(stranger.push()).await.unwrap();
    assert_eq!(response.conflicts.len(), 1);
    assert_eq!(
        response.conflicts[0].reason.as_deref(),
        Some(
            Error::DeclaredPolicyPreserved {
                table: "records".into(),
                clause: "direction".into()
            }
            .to_string()
            .as_str()
        )
    );
    assert!(hub.db.table_meta("records").is_none());
    assert_eq!(shown(&hub.db, "SHOW TENANT TABLE POLICY"), declared);
    assert_eq!(
        hub.db
            .execute("SELECT * FROM ordinary_install", &p())
            .unwrap()
            .rows
            .len(),
        1
    );
    within(stranger.shutdown()).await;
    let legitimate_db = Arc::new(Database::open(root.path().join("legitimate.db")).unwrap());
    let legitimate = edge_client(
        &legitimate_db,
        &hub.ticket,
        &root.path().join("legitimate.key"),
        tenant,
    );
    within(legitimate.bind_application_table_policy(expectation(&[
        ("records", ROOT_CLAUSES),
        ("record_parts", MEMBER_CLAUSES),
    ])))
    .await
    .unwrap();
    legitimate_db
        .execute(
            &format!("CREATE TABLE records (id UUID PRIMARY KEY, body TEXT) {ROOT_CLAUSES}"),
            &p(),
        )
        .unwrap();
    legitimate_db.execute(&format!("CREATE TABLE record_parts (id UUID PRIMARY KEY, record_id UUID REFERENCES records(id), body TEXT) {MEMBER_CLAUSES}"),&p()).unwrap();
    let id = uuid::Uuid::new_v4();
    let tx = legitimate_db.begin().unwrap();
    legitimate_db
        .insert_row(
            tx,
            "records",
            HashMap::from([
                ("id".into(), Value::Uuid(id)),
                ("body".into(), Value::Text("legitimate".into())),
            ]),
        )
        .unwrap();
    legitimate_db
        .register_delivery_manifest(
            tx,
            contextdb_engine::DeliveryManifest {
                root_table: "records",
                root_key: contextdb_engine::sync_types::NaturalKey::single(
                    "id".into(),
                    Value::Uuid(id),
                ),
                members: vec![],
            },
        )
        .unwrap();
    legitimate_db.commit(tx).unwrap();
    within(legitimate.push()).await.unwrap();
    assert_eq!(
        hub.db
            .execute("SELECT body FROM records", &p())
            .unwrap()
            .rows,
        vec![vec![Value::Text("legitimate".into())]]
    );
    assert_eq!(
        legitimate_db.delivery_status("records").unwrap().accepted,
        1
    );
    within(legitimate.shutdown()).await;

    within(client.shutdown()).await;
    hub.stop().await;
}
