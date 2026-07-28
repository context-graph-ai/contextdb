//! Protocol v6: the arrival position on a row change and the serving
//! store's identity on a pull response ride ONE version bump; no other wire
//! shape moves. `decode` already refuses any envelope whose version is not
//! exactly `PROTOCOL_VERSION` (`protocol.rs`), so the refusal mechanism
//! itself is not new — what is new is the bump itself, the two carried
//! fields, and an actionable message.
//!
//! Contract: `PROTOCOL_VERSION` is 6. A version-mismatched peer is refused
//! loudly on push, pull, AND the dedicated status exchange; no rows are ever
//! applied and no watermark ever advances on either side; the refusal names
//! the remedy (upgrade both ends), not just the two version numbers.
//!
//! Discipline: no sleeps, no elapsed-time assertions, no raw clock reads.

use contextdb_core::{Incarnation, Lsn, TenantId, Value};
use contextdb_engine::Database;
use contextdb_engine::sync_types::{ConflictPolicies, ConflictPolicy};
use contextdb_server::protocol::{
    Envelope, MessageType, PROTOCOL_VERSION, PullResponse, SyncStatusRequest, WireChangeSet,
    WireNaturalKey, WireRowChange, encode,
};
use contextdb_server::subjects::status_subject;
use contextdb_server::transport::{ClientTransport, TransportFuture};
use contextdb_server::{InProcessBroker, SyncClient, SyncServer};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

const DDL: &str = "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)";
/// A version guaranteed to differ from whatever `PROTOCOL_VERSION` is on
/// this tree today or after the v6 bump — never a peer this tree could
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
    let server = Arc::new(SyncServer::with_transport(
        hub_db,
        broker.server(),
        TenantId::from(tenant),
        ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    ));
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

/// The v6 bump itself: `PROTOCOL_VERSION` must be 6, carrying the arrival
/// position and the serving store's identity in one change. Fails until the
/// bump lands (the tree is at v5 today); everything else in this file
/// compiles and is meaningful either side of the bump.
#[test]
fn protocol_version_is_v6() {
    assert_eq!(
        PROTOCOL_VERSION, 6,
        "protocol v5->v6 must carry the arrival position on the row change \
         and the serving store's identity on the pull response in ONE bump"
    );
}

/// The two v6 fields exist and round-trip: `WireRowChange.arrival` and
/// `PullResponse.source`. Neither is populated by any real sender or server
/// yet (both default to absent, the legacy behavior every existing caller
/// gets — see the stub notes on the field definitions), but the shape is
/// live on the wire type today, ahead of the version bump itself.
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

    let node_id = "edge-mismatch-push";
    let transport = Arc::new(RewriteEnvelopeVersion {
        inner: broker.client_as(node_id),
        version: BOGUS_VERSION,
    });
    let edge_client =
        SyncClient::with_transport(edge_db.clone(), transport, TenantId::from(tenant));

    let tenant_id = TenantId::from(tenant);
    let hub_watermark_before = hub_db
        .persisted_sync_applied_push_watermark_for_node(&tenant_id, node_id)
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
        .persisted_sync_applied_push_watermark_for_node(&tenant_id, node_id)
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
    let transport = Arc::new(RewriteEnvelopeVersion {
        inner: broker.client(),
        version: BOGUS_VERSION,
    });
    let edge_client =
        SyncClient::with_transport(edge_db.clone(), transport, TenantId::from(tenant));

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
    assert_eq!(
        hub_db.current_lsn(),
        hub_lsn_before,
        "the hub state used to serve cursors must be untouched by a refused \
         version-mismatched pull"
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

    // Anonymous transport, deliberately: `client_as` would carry an
    // authenticated node id, and the hub's per-node last-contact clock
    // advances on ANY contact regardless of message validity (an existing,
    // separate design choice) — that would make the hub's durable position
    // position an unreliable proxy for "did the mismatched exchange move
    // anything sync-relevant." Anonymous keeps that side effect out of the
    // picture entirely, so the checks below are unambiguous.
    let transport = Arc::new(RewriteEnvelopeVersion {
        inner: broker.client(),
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
    assert_eq!(
        hub_db.current_lsn(),
        hub_lsn_before,
        "a refused version-mismatched status exchange must not move the \
         hub's own commit position either"
    );

    hub.stop().await;
}
