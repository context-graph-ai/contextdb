//! The unshipped protocol v6 includes a distinct PURGE instruction lane and a
//! structured authority refusal together with its arrival/source fields.
//! `decode` already refuses any envelope whose
//! version is not exactly `PROTOCOL_VERSION` (`protocol.rs`), so the refusal
//! mechanism itself is not new; this test freezes the completed v6 wire shape
//! and its actionable authority result.
//!
//! Contract: `PROTOCOL_VERSION` is 6. A version-mismatched peer is refused
//! loudly on push, pull, AND the dedicated status exchange; no rows are ever
//! applied and no watermark ever advances on either side; the refusal names
//! the remedy (upgrade both ends), not just the two version numbers.
//!
//! Discipline: no sleeps, no elapsed-time assertions, no raw clock reads.

use contextdb_core::{Incarnation, Lsn, TenantId, Value};
use contextdb_engine::Database;
use contextdb_engine::composite_store::ChangeLogEntry;
use contextdb_server::protocol::{
    Envelope, MessageType, PROTOCOL_VERSION, PullResponse, SyncStatusRequest, WireChangeSet,
    WireNaturalKey, WireRowChange, encode,
};
use contextdb_server::subjects::status_subject;
use contextdb_server::transport::{ClientTransport, TransportFuture};
use contextdb_server::work_ledger::WORK_NODE_CONTACTS_TABLE;
use contextdb_server::{FabricIdentity, InProcessBroker, SyncClient, SyncServer};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

const DDL: &str =
    "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT) SYNC CONFLICT KEEP LATEST";
/// A version guaranteed to differ from whatever `PROTOCOL_VERSION` is on
/// this tree today or after a future bump — never a peer this tree could
/// legitimately be.
const BOGUS_VERSION: u8 = 250;

fn p() -> HashMap<String, Value> {
    HashMap::new()
}

async fn within<F: std::future::Future>(fut: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(20), fut)
        .await
        .expect("bounded sync exchange exceeded 20s")
}

/// A client transport that rewrites the version byte of every outgoing
/// envelope before handing it to the real transport — simulating a peer
/// that speaks a protocol version this hub/edge does not support, without
/// needing two different `PROTOCOL_VERSION` constants linked into one
/// process.
struct RewriteEnvelopeVersion {
    inner: Arc<dyn ClientTransport>,
    version: u8,
}

fn rewrite_envelope_version(bytes: &[u8], version: u8) -> Vec<u8> {
    let mut envelope: Envelope =
        rmp_serde::from_slice(bytes).expect("decode envelope for version mutation");
    envelope.version = version;
    rmp_serde::to_vec(&envelope).expect("re-encode mutated envelope")
}

impl ClientTransport for RewriteEnvelopeVersion {
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
        let mutated = rewrite_envelope_version(&request_bytes, self.version);
        self.inner.request(subject, mutated, timeout)
    }
}

struct RunningHub {
    shutdown: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

impl RunningHub {
    async fn stop(self) {
        self.shutdown.store(true, Ordering::SeqCst);
        let _ = self.task.await;
    }
}

fn start_hub(broker: &InProcessBroker, tenant: &str, hub_db: Arc<Database>) -> RunningHub {
    let identity = Arc::new(FabricIdentity::generate());
    let node_id = identity.node_id();
    let server = Arc::new(
        SyncServer::with_authenticated_transport_and_identity_for_test(
            hub_db,
            broker.server_as(&node_id),
            TenantId::from(tenant),
            node_id,
            identity,
        ),
    );
    let shutdown = Arc::new(AtomicBool::new(false));
    let task = tokio::spawn({
        let server = server.clone();
        let shutdown = shutdown.clone();
        async move { server.run_until(shutdown).await }
    });
    RunningHub { shutdown, task }
}

fn row_count(db: &Database, table: &str) -> usize {
    db.execute(&format!("SELECT * FROM {table}"), &p())
        .unwrap_or_else(|err| panic!("scan {table}: {err}"))
        .rows
        .len()
}

fn assert_only_optional_peer_contact_changes_since(db: &Database, before_lsn: Lsn, exchange: &str) {
    let changes = db.change_log_since(before_lsn);
    assert!(
        changes.iter().all(|entry| matches!(
            entry,
            ChangeLogEntry::RowInsert { table, .. } | ChangeLogEntry::RowDelete { table, .. }
                if table == WORK_NODE_CONTACTS_TABLE
        )),
        "the refused {exchange} may record only work_node_contacts peer-contact changes: {changes:?}"
    );
}

/// The completed greenfield v6 contract carries the distinct PURGE lane and
/// structured authority refusal before any v6 peer ships.
#[test]
fn protocol_version_is_amended_v6() {
    assert_eq!(
        PROTOCOL_VERSION, 6,
        "the unshipped protocol v6 must include the distinct PURGE instruction lane and \
         structured authoritative-hub refusal"
    );
}

/// The two v6 fields exist and round-trip: `WireRowChange.arrival` and
/// `PullResponse.source`. They remain part of the completed v6 wire contract, and their
/// absent defaults retain the legacy meaning.
#[test]
fn the_v6_wire_fields_exist_and_round_trip() {
    let row = WireRowChange {
        table: "notes".to_string(),
        natural_key: WireNaturalKey {
            column: "id".to_string(),
            value: Value::Int64(1),
            rest: Vec::new(),
        },
        values: HashMap::new(),
        deleted: false,
        lsn: Lsn(5),
        created_at: None,
        arrival: Some(Lsn(3)),
        lineage: None,
    };
    let row_bytes = rmp_serde::to_vec(&row).expect("WireRowChange encode");
    let row_back: WireRowChange = rmp_serde::from_slice(&row_bytes).expect("WireRowChange decode");
    assert_eq!(
        row_back.arrival,
        Some(Lsn(3)),
        "WireRowChange.arrival must round-trip"
    );
    assert_eq!(
        WireRowChange::default().arrival,
        None,
        "an unstamped arrival must decode as absent, not a false zero"
    );

    let response = PullResponse {
        changeset: WireChangeSet::default(),
        has_more: false,
        cursor: None,
        source: Some(Incarnation(42)),
    };
    let response_bytes = rmp_serde::to_vec(&response).expect("PullResponse encode");
    let response_back: PullResponse =
        rmp_serde::from_slice(&response_bytes).expect("PullResponse decode");
    assert_eq!(
        response_back.source,
        Some(Incarnation(42)),
        "PullResponse.source must round-trip"
    );
    assert_eq!(
        PullResponse::default().source,
        None,
        "an unstamped serving-store source must decode as absent, not a \
         false identity"
    );
}

#[test]
fn protocol_v6_purge_instruction_and_typed_authority_error_round_trip() {
    let changeset = WireChangeSet {
        purges: vec![contextdb_server::protocol::WirePurgeChange {
            table: "notes".to_string(),
            table_generation: 3,
            natural_key: WireNaturalKey {
                column: "id".to_string(),
                value: Value::Int64(7),
                rest: Vec::new(),
            },
            purged_lineage_roots: vec!["lineage-root-7".to_string()],
            purge_frontier: Lsn(12),
        }],
        ..WireChangeSet::default()
    };
    let changeset_bytes = rmp_serde::to_vec(&changeset).expect("WireChangeSet encode");
    let changeset_back: WireChangeSet =
        rmp_serde::from_slice(&changeset_bytes).expect("WireChangeSet decode");
    assert_eq!(
        changeset_back, changeset,
        "nonempty PURGE lane must round-trip"
    );

    let response = contextdb_server::protocol::PushResponse {
        result: None,
        error: None,
        application_error: Some(
            contextdb_server::protocol::WirePushError::PurgeRequiresAuthoritativeHub {
                hub_node_id: "authoritative-hub".to_string(),
            },
        ),
    };
    let response_bytes = rmp_serde::to_vec(&response).expect("PushResponse encode");
    let response_back: contextdb_server::protocol::PushResponse =
        rmp_serde::from_slice(&response_bytes).expect("PushResponse decode");
    assert_eq!(
        response_back, response,
        "structured PURGE authority refusal must round-trip"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_version_mismatched_peer_is_refused_on_push_moving_no_rows_and_advancing_no_watermark() {
    let broker = InProcessBroker::new();
    let tenant = "v6-mismatch-push";
    let hub_db = Arc::new(Database::open_memory());
    hub_db.execute(DDL, &p()).expect("hub ddl");
    let hub = start_hub(&broker, tenant, hub_db.clone());

    let edge_db = Arc::new(Database::open_memory());
    edge_db.execute(DDL, &p()).expect("edge ddl");
    let mut row = p();
    row.insert("id".to_string(), Value::Int64(1));
    row.insert(
        "body".to_string(),
        Value::Text("never-should-land".to_string()),
    );
    edge_db
        .execute("INSERT INTO notes (id, body) VALUES ($id, $body)", &row)
        .expect("edge write");

    let edge_identity = Arc::new(FabricIdentity::generate());
    let node_id = edge_identity.node_id();
    let transport = Arc::new(RewriteEnvelopeVersion {
        inner: broker.client_as(&node_id),
        version: BOGUS_VERSION,
    });
    let edge_client = SyncClient::with_authenticated_transport_and_identity_for_test(
        edge_db.clone(),
        transport,
        TenantId::from(tenant),
        edge_identity,
    );

    let tenant_id = TenantId::from(tenant);
    let hub_watermark_before = hub_db
        .persisted_sync_applied_push_watermark_for_node(&tenant_id, &node_id)
        .expect("read hub per-edge watermark before push");
    assert_eq!(
        hub_watermark_before, None,
        "fixture: the hub must hold no per-edge applied-push watermark for \
         this edge before the mismatched push"
    );

    let err = within(edge_client.push())
        .await
        .expect_err("a version-mismatched push must be refused, not silently accepted");
    let message = err.to_string();
    assert!(
        message.contains("protocol version mismatch") || message.contains("version"),
        "the refusal must name the protocol version mismatch: {message}"
    );
    assert!(
        message.to_lowercase().contains("upgrade"),
        "the refusal must name the remedy (upgrade both ends), not just the \
         two version numbers: {message}"
    );

    assert_eq!(
        row_count(&hub_db, "notes"),
        0,
        "no row may move on a version-mismatched push"
    );
    assert_eq!(
        edge_client.push_watermark(),
        contextdb_core::Lsn(0),
        "the push watermark must not advance on a refused version mismatch"
    );
    let hub_watermark_after = hub_db
        .persisted_sync_applied_push_watermark_for_node(&tenant_id, &node_id)
        .expect("read hub per-edge watermark after push");
    assert_eq!(
        hub_watermark_after, hub_watermark_before,
        "the hub's persisted per-edge applied-push watermark for this edge \
         must not advance on a refused version-mismatched push"
    );

    hub.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_version_mismatched_peer_is_refused_on_pull_moving_no_rows_and_advancing_no_watermark() {
    let broker = InProcessBroker::new();
    let tenant = "v6-mismatch-pull";
    let hub_db = Arc::new(Database::open_memory());
    hub_db.execute(DDL, &p()).expect("hub ddl");
    let mut row = p();
    row.insert("id".to_string(), Value::Int64(1));
    row.insert(
        "body".to_string(),
        Value::Text("never-should-arrive".to_string()),
    );
    hub_db
        .execute("INSERT INTO notes (id, body) VALUES ($id, $body)", &row)
        .expect("hub write");
    let hub = start_hub(&broker, tenant, hub_db.clone());

    let edge_db = Arc::new(Database::open_memory());
    edge_db.execute(DDL, &p()).expect("edge ddl");
    let edge_identity = Arc::new(FabricIdentity::generate());
    let edge_node_id = edge_identity.node_id();
    let transport = Arc::new(RewriteEnvelopeVersion {
        inner: broker.client_as(&edge_node_id),
        version: BOGUS_VERSION,
    });
    let edge_client = SyncClient::with_authenticated_transport_and_identity_for_test(
        edge_db.clone(),
        transport,
        TenantId::from(tenant),
        edge_identity,
    );

    let tenant_id = TenantId::from(tenant);
    let hub_lsn_before = hub_db.current_lsn();
    let edge_persisted_before = edge_db
        .persisted_sync_watermarks(&tenant_id)
        .expect("read edge persisted watermarks before pull");

    let err = within(edge_client.pull_default())
        .await
        .expect_err("a version-mismatched pull must be refused, not silently accepted");
    let message = err.to_string();
    assert!(
        message.to_lowercase().contains("upgrade"),
        "the refusal must name the remedy (upgrade both ends): {message}"
    );

    assert_eq!(
        row_count(&edge_db, "notes"),
        0,
        "no row may move on a version-mismatched pull"
    );
    assert_eq!(
        edge_client.pull_watermark(),
        contextdb_core::Lsn(0),
        "the pull watermark must not advance on a refused version mismatch"
    );
    assert_only_optional_peer_contact_changes_since(
        &hub_db,
        hub_lsn_before,
        "version-mismatched pull",
    );
    let edge_persisted_after = edge_db
        .persisted_sync_watermarks(&tenant_id)
        .expect("read edge persisted watermarks after pull");
    assert_eq!(
        edge_persisted_after, edge_persisted_before,
        "the client's stored cursor must be unchanged (checked against the \
         PERSISTED record, not just the in-memory watermark) by a refused \
         version-mismatched pull"
    );

    hub.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_version_mismatched_peer_is_refused_on_the_status_exchange() {
    let broker = InProcessBroker::new();
    let tenant = "v6-mismatch-status";
    let hub_db = Arc::new(Database::open_memory());
    hub_db.execute(DDL, &p()).expect("hub ddl");
    let hub = start_hub(&broker, tenant, hub_db.clone());

    // This transport authenticates its peer identity. Best-effort contact
    // bookkeeping may run even when the envelope version is refused, so the
    // check below permits only `work_node_contacts` entries while still proving
    // that no sync progress advances.
    let transport = Arc::new(RewriteEnvelopeVersion {
        inner: broker.client_as("protocol-version-status-edge"),
        version: BOGUS_VERSION,
    });
    let request = SyncStatusRequest {
        incarnation: contextdb_core::Incarnation::mint(),
    };
    let encoded = encode(MessageType::StatusRequest, &request).expect("encode status request");

    let tenant_id = TenantId::from(tenant);
    let hub_watermark_before = hub_db
        .persisted_sync_applied_push_watermark(&tenant_id)
        .expect("read hub applied-push watermark before status exchange");
    let hub_lsn_before = hub_db.current_lsn();

    let result = within(transport.request_single_reply(
        &status_subject(tenant),
        encoded,
        Duration::from_secs(5),
    ))
    .await;
    assert!(
        result.is_err(),
        "a version-mismatched status exchange must be refused, not silently \
         answered: {result:?}"
    );

    let hub_watermark_after = hub_db
        .persisted_sync_applied_push_watermark(&tenant_id)
        .expect("read hub applied-push watermark after status exchange");
    assert_eq!(
        hub_watermark_after, hub_watermark_before,
        "a refused version-mismatched status exchange must not move the \
         hub's applied-push watermark"
    );
    assert_only_optional_peer_contact_changes_since(
        &hub_db,
        hub_lsn_before,
        "version-mismatched status exchange",
    );

    hub.stop().await;
}

// ---------------------------------------------------------------------------
// `WirePushError::ReplaysAcceptedDelete` (commit 861588c) added a NEW enum
// variant to the wire. Standing policy: no version bump is needed
// for this — everything on this wire is greenfield, nothing has been
// released, and every consumer builds from the same `dev` path and rebuilds
// together, so the wire may change in place until the first public release
// (version-bump discipline applies starting at that first release, not
// before it). These two tests are both permanent, GREEN pins, not defect
// proofs:
// ---------------------------------------------------------------------------

/// GREEN pin: freezes today's actual encoded bytes for the new variant, so
/// any future accidental reshaping of `WirePushError::ReplaysAcceptedDelete`
/// is caught here rather than discovered as a silent wire break.
#[test]
fn replays_accepted_delete_wire_bytes_are_frozen() {
    let err = contextdb_server::protocol::WirePushError::ReplaysAcceptedDelete {
        table: "notes".to_string(),
        key: vec![("id".to_string(), Value::Int64(7))],
    };
    let bytes = rmp_serde::to_vec(&err).expect("encode ReplaysAcceptedDelete");
    let frozen: &[u8] = &[
        129, 181, 82, 101, 112, 108, 97, 121, 115, 65, 99, 99, 101, 112, 116, 101, 100, 68, 101,
        108, 101, 116, 101, 146, 165, 110, 111, 116, 101, 115, 145, 146, 162, 105, 100, 129, 165,
        73, 110, 116, 54, 52, 7,
    ];
    assert_eq!(
        bytes, frozen,
        "the on-wire encoding of WirePushError::ReplaysAcceptedDelete has changed; if this \
         is deliberate, refreeze the literal AND re-check every already-shipped peer's \
         decode compatibility with the new shape"
    );
}

/// GREEN, permanent documentation of the compat boundary this repo is
/// deliberately relying on pre-release, not a defect proof: `rmp_serde::to_vec`
/// (compact mode, used by `encode`/`decode` above) tags an enum externally BY
/// VARIANT NAME, not by index — so a `WirePushError` shape compiled BEFORE
/// this variant existed cannot recognize the "ReplaysAcceptedDelete" tag at
/// all and fails to decode any `PushResponse` carrying it. This is exactly
/// WHY every machine on this wire must rebuild from `dev` together until the
/// first public release — there is no version
/// negotiation covering this gap today, only the "everyone tracks `dev`"
/// discipline. Once a release ships, this same fact is the argument FOR
/// applying version-bump discipline from then on.
#[test]
fn pre_release_wire_additions_are_incompatible_with_a_stale_build_not_covered_by_a_version_bump() {
    #[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
    enum StaleWirePushError {
        PurgeRequiresAuthoritativeHub { hub_node_id: String },
    }

    let new_variant = contextdb_server::protocol::WirePushError::ReplaysAcceptedDelete {
        table: "notes".to_string(),
        key: vec![("id".to_string(), Value::Int64(7))],
    };
    let bytes = rmp_serde::to_vec(&new_variant).expect("encode new variant");

    let decoded = rmp_serde::from_slice::<StaleWirePushError>(&bytes);
    assert!(
        decoded.is_err(),
        "a build compiled before this variant existed must fail to decode it \
         (proving why every machine must rebuild from dev together pre-release), \
         but it decoded as {decoded:?}"
    );

    // The pre-existing variant, unchanged, still decodes cleanly under the
    // stale shape — this is an addition-only gap, not a wholesale one.
    let old_variant = contextdb_server::protocol::WirePushError::PurgeRequiresAuthoritativeHub {
        hub_node_id: "hub-1".to_string(),
    };
    let old_bytes = rmp_serde::to_vec(&old_variant).expect("encode old variant");
    let old_decoded = rmp_serde::from_slice::<StaleWirePushError>(&old_bytes)
        .expect("the pre-existing variant must remain decodable under the stale shape");
    assert_eq!(
        old_decoded,
        StaleWirePushError::PurgeRequiresAuthoritativeHub {
            hub_node_id: "hub-1".to_string()
        }
    );
}
