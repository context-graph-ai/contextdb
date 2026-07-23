//! The Iroh adapter's acceptance tests. Everything here runs over REAL
//! localhost Iroh endpoints — no relay, no internet, no third-party contact,
//! no Docker — and is the default-gate replacement for the deprecated broker
//! suites.

use contextdb_core::Value;
use contextdb_engine::Database;
use contextdb_engine::sync_types::{ConflictPolicies, ConflictPolicy};
use contextdb_server::transport::iroh::{
    EndpointSpec, IrohServer, LookupChoice, PeerRequest, PublishChoice, RelayChoice, SYNC_ALPN,
    is_iroh_endpoint, peer_connect, peer_request,
};
use contextdb_server::{FabricIdentity, SyncClient, SyncServer};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use uuid::Uuid;

async fn within<F: std::future::Future>(fut: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(30), fut)
        .await
        .expect("bounded iroh transport operation exceeded 30s")
}

fn identity_file(dir: &tempfile::TempDir) -> PathBuf {
    dir.path().join("fabric-identity.key")
}

fn bind_spec(identity: &Path) -> String {
    format!("iroh:?identity={}", identity.display())
}

fn bind_spec_with_port(identity: &Path, port: u16) -> String {
    format!("iroh:?identity={}&port={port}", identity.display())
}

/// The UDP ports named by a parsed ticket's direct socket addresses (there is
/// no accessor for this on `EndpointTicket`/`EndpointAddr`, so it is read off
/// each address's own `Display` text — `"ip:<socket-addr>"` for an IP
/// address, which trims to a plain `SocketAddr` the standard parser accepts).
/// A restart on one explicit bound port produces the SAME port on every
/// discovered address, even when which addresses got discovered varies.
fn ticket_ports(
    ticket: &iroh_tickets::endpoint::EndpointTicket,
) -> std::collections::BTreeSet<u16> {
    ticket
        .endpoint_addr()
        .addrs
        .iter()
        .filter_map(|addr| {
            addr.to_string()
                .strip_prefix("ip:")
                .and_then(|rest| rest.parse::<std::net::SocketAddr>().ok())
                .map(|socket| socket.port())
        })
        .collect()
}

fn create_notes_table(db: &Database) {
    db.execute(
        "CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT)",
        &HashMap::new(),
    )
    .expect("create notes table");
}

fn insert_note(db: &Database, id: Uuid, body: &str) {
    let mut row = HashMap::new();
    row.insert("id".to_string(), Value::Uuid(id));
    row.insert("body".to_string(), Value::Text(body.to_string()));
    db.execute("INSERT INTO notes (id, body) VALUES ($id, $body)", &row)
        .expect("insert note");
}

fn note_body(db: &Database, id: Uuid) -> Option<String> {
    let mut key = HashMap::new();
    key.insert("id".to_string(), Value::Uuid(id));
    let result = db
        .execute("SELECT body FROM notes WHERE id = $id", &key)
        .expect("select note");
    let idx = result
        .columns
        .iter()
        .position(|c| c == "body")
        .expect("body column");
    result.rows.first().map(|row| match &row[idx] {
        Value::Text(text) => text.clone(),
        other => panic!("notes.body must be text, got {other:?}"),
    })
}

struct RunningHub {
    server_db: Arc<Database>,
    ticket: String,
    node_id: String,
    shutdown: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

async fn start_hub(spec: &str, tenant: &str) -> RunningHub {
    // The IrohServer handle is dropped at the end of this block, BEFORE the
    // sync server starts serving: transport() must hand the SyncServer a
    // self-sufficient serving side, so dropping the bind handle never tears
    // down an actively serving endpoint.
    let (ticket, node_id, transport) = {
        let endpoint = IrohServer::bind(spec)
            .await
            .unwrap_or_else(|err| panic!("hub endpoint must bind for spec {spec}: {err}"));
        (endpoint.ticket(), endpoint.node_id(), endpoint.transport())
    };
    let server_db = Arc::new(Database::open_memory());
    let server = Arc::new(SyncServer::with_transport(
        server_db.clone(),
        transport,
        contextdb_core::TenantId::from(tenant),
        ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    ));
    let shutdown = Arc::new(AtomicBool::new(false));
    let task = tokio::spawn({
        let server = server.clone();
        let shutdown = shutdown.clone();
        async move { server.run_until(shutdown).await }
    });
    RunningHub {
        server_db,
        ticket,
        node_id,
        shutdown,
        task,
    }
}

impl RunningHub {
    async fn stop(self) {
        self.shutdown.store(true, Ordering::SeqCst);
        let _ = self.task.await;
    }
}

// Identity is fabric-owned.

#[test]
fn identity_keypair_persists_and_yields_same_node_id() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = identity_file(&dir);

    let first = FabricIdentity::load_or_generate(&path)
        .expect("first load_or_generate must create an identity");
    assert!(path.exists(), "identity file must be persisted at {path:?}");
    let first_id = first.node_id();
    assert_eq!(
        first_id.len(),
        64,
        "node_id must be the lowercase hex of a 32-byte ed25519 public key, got {first_id:?}"
    );
    assert_eq!(
        first_id.to_lowercase(),
        first_id,
        "node_id must be lowercase hex"
    );

    let second = FabricIdentity::load_or_generate(&path)
        .expect("second load_or_generate must load the same identity");
    assert_eq!(
        second.node_id(),
        first_id,
        "the same identity file must always yield the same node_id"
    );
    assert_eq!(
        second.public_key_bytes(),
        first.public_key_bytes(),
        "public key bytes must be stable across loads"
    );
}

#[test]
fn fresh_data_root_yields_new_identity() {
    let dir_a = tempfile::tempdir().expect("tempdir a");
    let dir_b = tempfile::tempdir().expect("tempdir b");
    let a = FabricIdentity::load_or_generate(&identity_file(&dir_a)).expect("identity a");
    let b = FabricIdentity::load_or_generate(&identity_file(&dir_b)).expect("identity b");
    assert_ne!(
        a.node_id(),
        b.node_id(),
        "distinct data roots must mint distinct identities"
    );
}

#[cfg(unix)]
#[test]
fn identity_file_written_with_owner_only_permissions() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().expect("tempdir");
    let path = identity_file(&dir);
    FabricIdentity::load_or_generate(&path).expect("identity");
    let mode = std::fs::metadata(&path)
        .expect("identity file metadata")
        .permissions()
        .mode();
    assert_eq!(
        mode & 0o777,
        0o600,
        "the secret key file must be readable by the owner only"
    );
}

#[tokio::test]
async fn bound_endpoint_uses_the_fabric_identity_not_a_transport_minted_one() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = identity_file(&dir);
    let identity = FabricIdentity::load_or_generate(&path).expect("identity");

    let endpoint = within(IrohServer::bind(&bind_spec(&path)))
        .await
        .expect("bind with existing identity");
    assert_eq!(
        endpoint.node_id(),
        identity.node_id(),
        "the endpoint's identity must BE the fabric identity handed to it — never one the transport minted"
    );
}

// No third-party contact by default; opt-ins are explicit.

#[test]
fn default_endpoint_config_disables_relay_and_publishing() {
    let spec = EndpointSpec::parse("iroh:?identity=/tmp/k.key")
        .expect("a bind spec must parse as an iroh endpoint");
    assert_eq!(
        spec.relay(),
        &RelayChoice::Disabled,
        "the default relay posture is disabled: LAN needs no relay and no internet"
    );
    assert!(
        !spec.publishes_address_lookup(),
        "the adapter must never publish node addresses to an external address-lookup service"
    );
}

#[test]
fn relay_use_requires_explicit_opt_in() {
    let self_hosted =
        EndpointSpec::parse("iroh:?identity=/tmp/k.key&relay=https://relay.example.net")
            .expect("self-hosted relay spec must parse");
    assert_eq!(
        self_hosted.relay(),
        &RelayChoice::SelfHosted("https://relay.example.net".to_string()),
        "relay=<url> selects the operator's own relay"
    );
    assert!(!self_hosted.publishes_address_lookup());

    let n0 = EndpointSpec::parse("iroh:?identity=/tmp/k.key&relay=n0")
        .expect("public-relay opt-in spec must parse");
    assert_eq!(
        n0.relay(),
        &RelayChoice::N0Public,
        "relay=n0 is the explicit opt-in to the free public relays"
    );

    let none = EndpointSpec::parse("iroh:?identity=/tmp/k.key&relay=none")
        .expect("relay=none spec must parse");
    assert_eq!(none.relay(), &RelayChoice::Disabled);
}

#[test]
fn broker_urls_are_not_iroh_endpoints() {
    for spec in [
        "nats://localhost:4222",
        "ws://localhost:9222",
        "wss://hub.example.net:9222",
        "tls://hub.example.net:4222",
    ] {
        assert!(
            !is_iroh_endpoint(spec),
            "broker URL {spec} must route to the deprecated broker adapter, not iroh"
        );
    }
}

// The sync protocol end to end over real localhost Iroh endpoints.

#[tokio::test]
async fn sync_push_pull_status_over_iroh() {
    let dir = tempfile::tempdir().expect("tempdir");
    let tenant = "iroh-e2e";
    let hub = start_hub(&bind_spec(&identity_file(&dir)), tenant).await;

    assert!(
        is_iroh_endpoint(&hub.ticket),
        "the hub ticket must itself be a dialable endpoint spec"
    );
    assert_eq!(
        hub.node_id.len(),
        64,
        "the hub must report its fabric node_id"
    );

    // Edge A pushes.
    let edge_a = Arc::new(Database::open_memory());
    create_notes_table(&edge_a);
    let note_a = Uuid::new_v4();
    insert_note(&edge_a, note_a, "from-edge-a");
    // The polymorphic constructor: the plain ticket string in the slot where
    // a NATS URL used to go. This is exactly what lets a downstream
    // application's smoke run over Iroh with zero downstream code change.
    let client_a = SyncClient::new(
        edge_a.clone(),
        &hub.ticket,
        contextdb_core::TenantId::from(tenant),
    );
    within(client_a.push()).await.expect("edge a push");
    assert_eq!(
        note_body(&hub.server_db, note_a).as_deref(),
        Some("from-edge-a"),
        "pushed row must land on the hub"
    );

    // Edge B pulls what A pushed.
    let edge_b = Arc::new(Database::open_memory());
    let client_b = SyncClient::new(
        edge_b.clone(),
        &hub.ticket,
        contextdb_core::TenantId::from(tenant),
    );
    within(client_b.pull(&ConflictPolicies::uniform(ConflictPolicy::LatestWins)))
        .await
        .expect("edge b pull");
    assert_eq!(
        note_body(&edge_b, note_a).as_deref(),
        Some("from-edge-a"),
        "pulled row must land on edge b"
    );

    // Connectivity status reflects the live endpoint.
    within(client_a.ensure_connected())
        .await
        .expect("ensure_connected against a live hub");
    assert!(client_a.is_connected().await);

    hub.stop().await;
}

#[tokio::test]
async fn large_changeset_moves_in_one_stream_over_iroh() {
    // Over NATS this payload would be fragmented by the chunk/ack machinery
    // (~1MB broker ceiling). Over an Iroh stream it must arrive as one framed
    // message with full integrity.
    let dir = tempfile::tempdir().expect("tempdir");
    let tenant = "iroh-large";
    let hub = start_hub(&bind_spec(&identity_file(&dir)), tenant).await;

    let edge = Arc::new(Database::open_memory());
    create_notes_table(&edge);
    let id = Uuid::new_v4();
    let big_body = "x".repeat(2 * 1024 * 1024 + 137);
    insert_note(&edge, id, &big_body);

    let client = SyncClient::new(
        edge.clone(),
        &hub.ticket,
        contextdb_core::TenantId::from(tenant),
    );
    within(client.push()).await.expect("large push");
    assert_eq!(
        note_body(&hub.server_db, id).map(|body| body.len()),
        Some(big_body.len()),
        "the >1MB row must arrive intact on the hub"
    );

    let reader = Arc::new(Database::open_memory());
    let reader_client = SyncClient::new(
        reader.clone(),
        &hub.ticket,
        contextdb_core::TenantId::from(tenant),
    );
    within(reader_client.pull(&ConflictPolicies::uniform(ConflictPolicy::LatestWins)))
        .await
        .expect("large pull");
    assert_eq!(
        note_body(&reader, id).map(|body| body.len()),
        Some(big_body.len()),
        "the >1MB row must arrive intact on the pulling edge"
    );

    hub.stop().await;
}

#[tokio::test]
async fn reconnect_after_hub_restart_over_iroh() {
    let dir = tempfile::tempdir().expect("tempdir");
    let tenant = "iroh-restart";
    let identity = identity_file(&dir);
    let port = free_udp_port();
    let spec = bind_spec_with_port(&identity, port);

    let hub = start_hub(&spec, tenant).await;
    let ticket = hub.ticket.clone();

    let edge = Arc::new(Database::open_memory());
    create_notes_table(&edge);
    let first = Uuid::new_v4();
    insert_note(&edge, first, "before-restart");
    let client = SyncClient::new(
        edge.clone(),
        &ticket,
        contextdb_core::TenantId::from(tenant),
    );
    within(client.push()).await.expect("push before restart");
    let server_db_before = hub.server_db.clone();
    assert_eq!(
        note_body(&server_db_before, first).as_deref(),
        Some("before-restart")
    );
    hub.stop().await;

    // Same identity, same port: the ticket the edge already holds must keep
    // working across a hub restart. The reprinted ticket's set of direct
    // socket addresses is allowed to differ across a restart — which local
    // interfaces get discovered by the time the endpoint prints its ticket
    // can vary run to run, and that snapshot was never part of the promise.
    // What must stay identical is the node's own identity and the UDP port
    // it is bound to, since a caller only needs those two to keep dialing
    // the same node at the same address.
    let hub = start_hub(&spec, tenant).await;
    let before: iroh_tickets::endpoint::EndpointTicket =
        ticket.parse().expect("previous ticket must parse");
    let after: iroh_tickets::endpoint::EndpointTicket =
        hub.ticket.parse().expect("reprinted ticket must parse");
    assert_eq!(
        after.endpoint_addr().id,
        before.endpoint_addr().id,
        "a hub restart with the same identity file must reprint the same node identity"
    );
    assert_eq!(
        ticket_ports(&after),
        ticket_ports(&before),
        "a hub restart on the same explicit port must reprint the same port"
    );
    let second = Uuid::new_v4();
    insert_note(&edge, second, "after-restart");
    client.reconnect().await;
    within(client.push()).await.expect("push after restart");
    assert_eq!(
        note_body(&hub.server_db, second).as_deref(),
        Some("after-restart"),
        "the edge must reach the restarted hub through its existing ticket"
    );

    hub.stop().await;
}

#[tokio::test]
async fn backlog_accumulated_offline_reaches_hub_over_iroh() {
    let dir = tempfile::tempdir().expect("tempdir");
    let tenant = "iroh-backlog";
    let identity = identity_file(&dir);
    let port = free_udp_port();
    let spec = bind_spec_with_port(&identity, port);

    // Learn the ticket, then take the hub down.
    let hub = start_hub(&spec, tenant).await;
    let ticket = hub.ticket.clone();
    hub.stop().await;

    // Edge accumulates rows while the hub is down; pushing fails loudly.
    let edge = Arc::new(Database::open_memory());
    create_notes_table(&edge);
    let ids: Vec<Uuid> = (0..5).map(|_| Uuid::new_v4()).collect();
    for (n, id) in ids.iter().enumerate() {
        insert_note(&edge, *id, &format!("offline-{n}"));
    }
    let client = SyncClient::new(
        edge.clone(),
        &ticket,
        contextdb_core::TenantId::from(tenant),
    );
    assert!(
        within(client.push()).await.is_err(),
        "pushing at a down hub must fail, not hang or silently drop"
    );

    // Hub returns; the full backlog converges.
    let hub = start_hub(&spec, tenant).await;
    client.reconnect().await;
    within(client.push()).await.expect("backlog push");
    for (n, id) in ids.iter().enumerate() {
        assert_eq!(
            note_body(&hub.server_db, *id).as_deref(),
            Some(format!("offline-{n}").as_str()),
            "backlog row {n} must reach the hub"
        );
    }

    hub.stop().await;
}

#[tokio::test]
async fn unreachable_hub_maps_to_transport_neutral_errors() {
    let dir = tempfile::tempdir().expect("tempdir");
    // Bind to learn a real ticket, then stop serving entirely.
    let hub = start_hub(&bind_spec(&identity_file(&dir)), "iroh-downed").await;
    let ticket = hub.ticket.clone();
    hub.stop().await;

    let edge = Arc::new(Database::open_memory());
    create_notes_table(&edge);
    insert_note(&edge, Uuid::new_v4(), "never-arrives");
    let client = SyncClient::new(
        edge.clone(),
        &ticket,
        contextdb_core::TenantId::from("iroh-downed"),
    );
    let err = within(client.push())
        .await
        .expect_err("push at a downed hub must error")
        .to_string()
        .to_ascii_lowercase();
    assert!(
        err.contains("unreachable") || err.contains("no responder") || err.contains("timed out"),
        "the failure must surface in transport-neutral vocabulary, got: {err}"
    );
    assert!(
        !err.contains("iroh"),
        "sync-level errors must not name the concrete transport, got: {err}"
    );
}

// Ticket enrollment.

#[tokio::test]
async fn ticket_round_trips_as_opaque_config_string() {
    let dir = tempfile::tempdir().expect("tempdir");
    let hub = start_hub(&bind_spec(&identity_file(&dir)), "iroh-ticket").await;

    let ticket = hub.ticket.clone();
    assert!(
        is_iroh_endpoint(&ticket),
        "the printed ticket must be accepted as an endpoint spec verbatim"
    );
    let prefixed = format!("iroh:{ticket}");
    assert!(
        is_iroh_endpoint(&prefixed),
        "an iroh:-prefixed ticket must also be accepted"
    );
    let parsed = EndpointSpec::parse(&ticket).expect("ticket parses");
    assert_eq!(
        parsed.dial_ticket(),
        Some(ticket.as_str()),
        "a ticket spec is a dial spec"
    );
    assert_eq!(
        parsed.relay(),
        &RelayChoice::Disabled,
        "dialing by ticket must not silently enable any relay"
    );

    hub.stop().await;
}

// The fabric-internal peer surface (what the media-transfer path builds on).

#[tokio::test]
async fn second_alpn_peer_stream_exchanges_bytes_without_hub() {
    let dir = tempfile::tempdir().expect("tempdir");
    let endpoint = within(IrohServer::bind(&bind_spec(&identity_file(&dir))))
        .await
        .expect("bind peer node");
    let ticket = endpoint.ticket();

    let echo_alpn = b"contextdb.test.echo.v1".to_vec();
    endpoint.register_protocol(
        echo_alpn.clone(),
        Arc::new(|request: PeerRequest| {
            Box::pin(async move {
                // Echo back WHO asked plus the bytes: the handler must see
                // the caller's authenticated fabric identity, because the
                // media-transfer path authorizes fetches by node identity.
                let mut reply = format!("from={}:", request.remote_node_id).into_bytes();
                reply.extend_from_slice(&request.bytes);
                Ok(reply)
            }) as contextdb_server::transport::TransportFuture<'static, Vec<u8>>
        }),
    );
    assert_ne!(
        echo_alpn.as_slice(),
        SYNC_ALPN,
        "the peer surface must be an ADDITIONAL protocol label, not the sync ALPN"
    );

    // The caller dials AS its enrolled fabric identity (fabric-owned identity
    // applies to the dialing side too — never a transport-minted throwaway).
    let caller_dir = tempfile::tempdir().expect("caller tempdir");
    let caller_key = identity_file(&caller_dir);
    let caller = FabricIdentity::load_or_generate(&caller_key).expect("caller identity");
    let reply = within(peer_request(
        &caller_key,
        &ticket,
        &echo_alpn,
        b"blob-bytes".to_vec(),
        Duration::from_secs(10),
    ))
    .await
    .expect("peer request must succeed node-to-node");
    let expected = format!("from={}:blob-bytes", caller.node_id()).into_bytes();
    assert_eq!(
        reply, expected,
        "the peer handler must see the caller's ENROLLED node_id, proving the connection originated from the fabric identity"
    );
}

// The sync client can also dial AS a pinned fabric identity.

#[tokio::test]
async fn dial_spec_with_identity_pins_the_edge_fabric_identity() {
    let hub_dir = tempfile::tempdir().expect("hub tempdir");
    let tenant = "iroh-edge-id";
    let hub = start_hub(&bind_spec(&identity_file(&hub_dir)), tenant).await;

    let edge_dir = tempfile::tempdir().expect("edge tempdir");
    let edge_key = edge_dir.path().join("edge-fabric-identity.key");
    let combined = format!("iroh:?to={}&identity={}", hub.ticket, edge_key.display());
    let parsed = EndpointSpec::parse(&combined)
        .expect("the combined to+identity dial form must parse as an iroh endpoint");
    assert_eq!(
        parsed.dial_ticket(),
        Some(hub.ticket.as_str()),
        "the combined form carries the ticket to dial"
    );
    assert_eq!(
        parsed.identity_path().map(|p| p.to_path_buf()),
        Some(edge_key.clone()),
        "the combined form pins the edge's own key file"
    );

    let edge = Arc::new(Database::open_memory());
    create_notes_table(&edge);
    let id = Uuid::new_v4();
    insert_note(&edge, id, "identified-edge");
    let client = SyncClient::new(
        edge.clone(),
        &combined,
        contextdb_core::TenantId::from(tenant),
    );
    within(client.push()).await.expect("identified push");
    assert_eq!(
        note_body(&hub.server_db, id).as_deref(),
        Some("identified-edge"),
        "sync over the identified dial spec must work end to end"
    );
    let first = FabricIdentity::load_or_generate(&edge_key)
        .expect("the edge key file must have been created by the dial")
        .node_id();
    client.reconnect().await;
    within(client.push()).await.expect("second push");
    let second = FabricIdentity::load_or_generate(&edge_key)
        .expect("edge key reload")
        .node_id();
    assert_eq!(
        first, second,
        "the edge identity must persist across reconnects — same key file, same node_id"
    );

    hub.stop().await;
}

// Cross-network: a ticket that names a relay must be dialable THROUGH it.

/// Spawns a localhost relay standing in for the operator's self-hosted one,
/// capturing its self-signed certificate to a file — the same artifact a
/// real operator points `relay-ca=` at.
async fn run_local_relay_with_ca_file(
    dir: &tempfile::TempDir,
) -> (String, PathBuf, iroh_relay::server::Server) {
    use iroh_relay::server::{
        CertConfig, QuicConfig, RelayConfig as RelayServerConfig, Server, ServerConfig, TlsConfig,
    };
    use std::net::Ipv4Addr;
    use std::sync::Arc;

    let (certs, server_config) = iroh_relay::server::testing::self_signed_tls_certs_and_config();
    let ca_path = dir.path().join("relay-ca.der");
    std::fs::write(&ca_path, certs[0].as_ref()).expect("write relay ca cert");

    let tls = TlsConfig::new(
        (Ipv4Addr::LOCALHOST, 0),
        CertConfig::Manual { server_config },
    );
    let mut relay = RelayServerConfig::new((Ipv4Addr::LOCALHOST, 0));
    relay.tls = Some(tls);
    relay.key_cache_capacity = Some(1024);
    relay.access = Arc::new(iroh_relay::server::AllowAll);
    let mut config = ServerConfig::default();
    config.relay = Some(relay);
    config.quic = Some(QuicConfig::new((Ipv4Addr::LOCALHOST, 0)));
    let server = Server::spawn(config).await.expect("spawn local relay");
    let url = format!("https://{}", server.https_addr().expect("relay https addr"));
    (url, ca_path, server)
}

#[tokio::test]
async fn ticket_relay_url_enables_relay_dialing() {
    let relay_dir = tempfile::tempdir().expect("relay tempdir");
    let (relay_url, relay_ca, _relay_guard) = run_local_relay_with_ca_file(&relay_dir).await;

    // Hub opts into that relay via its endpoint spec; the relay's
    // certificate is trusted EXPLICITLY via its CA file — the operator knob
    // for self-hosted relays with private/self-signed certs, never a default.
    let hub_dir = tempfile::tempdir().expect("hub tempdir");
    let tenant = "iroh-relayed";
    let spec = format!(
        "iroh:?identity={}&relay={}&relay-ca={}",
        identity_file(&hub_dir).display(),
        relay_url,
        relay_ca.display()
    );
    let hub = start_hub(&spec, tenant).await;

    // Build a RELAY-ONLY ticket: same endpoint id, no direct addresses —
    // the shape a cross-network edge actually receives when no direct path
    // exists. Dialing it can only succeed through the relay.
    let full: iroh_tickets::endpoint::EndpointTicket =
        hub.ticket.parse().expect("hub ticket parses");
    let relay_only_addr = iroh::EndpointAddr::new(full.endpoint_addr().id)
        .with_relay_url(relay_url.parse().expect("relay url parses"));
    let relay_only_ticket =
        iroh_tickets::endpoint::EndpointTicket::new(relay_only_addr).to_string();

    let edge = Arc::new(Database::open_memory());
    create_notes_table(&edge);
    let id = Uuid::new_v4();
    insert_note(&edge, id, "via-relay");
    let dial_spec = format!(
        "iroh:?to={relay_only_ticket}&relay-ca={}",
        relay_ca.display()
    );
    let client = SyncClient::new(
        edge.clone(),
        &dial_spec,
        contextdb_core::TenantId::from(tenant),
    );
    within(client.push())
        .await
        .expect("push through a relay-only ticket must succeed: the ticket names the relay, so the dialing endpoint must enable exactly that relay");
    assert_eq!(
        note_body(&hub.server_db, id).as_deref(),
        Some("via-relay"),
        "the relayed push must land on the hub"
    );

    hub.stop().await;
}

// The deprecated broker path errors actionably when not built.

#[cfg(not(feature = "nats"))]
#[tokio::test]
async fn broker_url_without_nats_feature_errors_actionably() {
    let edge = Arc::new(Database::open_memory());
    let client = SyncClient::new(
        edge,
        "nats://localhost:4222",
        contextdb_core::TenantId::from("iroh-compat"),
    );
    let err = within(client.ensure_connected())
        .await
        .expect_err("a broker URL without the nats feature must error")
        .to_string()
        .to_ascii_lowercase();
    assert!(
        err.contains("nats") && err.contains("feature"),
        "the error must tell the operator the deprecated NATS adapter is not built and how to enable it, got: {err}"
    );
}

// Transport neutrality holds structurally.

#[test]
fn iroh_word_confined_to_adapter_and_config_surface() {
    let workspace_crates = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crates dir");
    let allowed_suffixes = [
        // The adapter itself.
        "contextdb-server/src/transport/iroh.rs",
        // The media-plane fetch backend: the second adapter file under
        // src/transport/, the sole sanctioned home of the iroh-blobs backend
        // (Rule 2), enforced by media_transfer_containment.rs.
        "contextdb-server/src/transport/iroh_blobs_adapter.rs",
        // The seam wiring: feature gate + factory routing only.
        "contextdb-server/src/transport/mod.rs",
        // The endpoint/relay config surface (flags, defaults, help text).
        "contextdb-server/src/main.rs",
        "contextdb-cli/src/main.rs",
        "contextdb-cli/src/repl.rs",
        "contextdb-cli/src/sync_status.rs",
        // The media-transfer acceptance scaffold binds real localhost peer
        // endpoints. Its own containment guard enforces the stricter rule that
        // backend names stay in the adapter and only IrohServer crosses into
        // the resolver.
        "contextdb-server/src/blob_resolver.rs",
        "contextdb-server/src/lib.rs",
        "contextdb-server/tests/media_transfer_tests.rs",
        "contextdb-server/tests/media_transfer_memory.rs",
        "contextdb-server/tests/media_transfer_containment.rs",
        "contextdb-server/benches/blob_transfer.rs",
        // The media-fetch retention test drives the iroh-blobs backend fetch
        // path directly, so it is legitimately backend-aware.
        "contextdb-server/tests/media_transfer_fetch_retention.rs",
        // The shared media test-support module hoisted out of the media
        // suites inherits that same backend-aware role.
        "contextdb-server/tests/media_support/mod.rs",
        // The media-transfer demo binds real iroh endpoints end-to-end; a
        // deliberately backend-aware example surface.
        "contextdb-server/examples/media_transfer_fabric_demo.rs",
        // This test file names the module under test.
        "contextdb-server/tests/iroh_transport_tests.rs",
        // The bounded-tables blob-plane receipt test binds a real localhost
        // endpoint to prove per-peer serve/fetch counters, so it names the
        // adapter for the same reason the media suites do.
        "contextdb-server/tests/bounded_tables_sync_tests.rs",
        // The bounded-tables live-smoke driver must name the real transport it
        // drives — the live-smoke requires the real endpoint path, not a test
        // double.
        "contextdb-server/examples/bounded_tables_smoke.rs",
        // The test-estate ratchet audit names test FILES (including this one)
        // in its per-file sleep counts — filenames, not transport use.
        "contextdb-core/tests/test_estate_audit.rs",
    ];

    let mut sources = Vec::new();
    collect_rust_sources(workspace_crates, &mut sources);
    assert!(
        sources.len() > 50,
        "source sweep must actually walk the workspace, found only {}",
        sources.len()
    );

    let mut violations = Vec::new();
    for path in sources {
        let normalized = path.to_string_lossy().replace('\\', "/");
        if allowed_suffixes
            .iter()
            .any(|suffix| normalized.ends_with(suffix))
        {
            continue;
        }
        let contents =
            std::fs::read_to_string(&path).unwrap_or_else(|err| panic!("read {path:?}: {err}"));
        if contents.to_ascii_lowercase().contains("iroh") {
            violations.push(normalized);
        }
    }
    assert!(
        violations.is_empty(),
        "the word 'iroh' may appear only in the adapter and the endpoint config surface; found in: {violations:?}"
    );
}

fn collect_rust_sources(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap_or_else(|err| panic!("read {dir:?}: {err}")) {
        let entry = entry.unwrap_or_else(|err| panic!("read entry under {dir:?}: {err}"));
        let path = entry.path();
        let name = entry.file_name();
        if name == "target" {
            continue;
        }
        if path.is_dir() {
            collect_rust_sources(&path, out);
        } else if path.extension().and_then(|ext| ext.to_str()) == Some("rs") {
            out.push(path);
        }
    }
}

// Operator-trap regressions.

#[tokio::test]
async fn hub_restart_without_port_keeps_the_same_ticket() {
    // Port stickiness by default: a hub bound WITHOUT port= records its
    // chosen port beside the identity key and reuses it, so tickets survive
    // restarts (a live-smoke trap: a restarted hub minted a new random port
    // and every issued ticket went stale).
    let dir = tempfile::tempdir().expect("tempdir");
    let spec = bind_spec(&identity_file(&dir));

    let first = within(IrohServer::bind(&spec)).await.expect("first bind");
    let first_ticket = first.ticket();
    within(first.close()).await;

    let second = within(IrohServer::bind(&spec)).await.expect("second bind");
    let second_ticket = second.ticket();
    within(second.close()).await;

    // The reprinted ticket's set of direct socket addresses is allowed to
    // differ across a restart — which local interfaces get discovered by the
    // time the endpoint prints its ticket can vary run to run, and that
    // snapshot was never part of the promise. What this test is actually
    // about is port stickiness: the node's own identity and the UDP port it
    // is bound to (recorded beside the identity key and reused without an
    // explicit port=) must stay identical.
    let first: iroh_tickets::endpoint::EndpointTicket =
        first_ticket.parse().expect("first ticket must parse");
    let second: iroh_tickets::endpoint::EndpointTicket =
        second_ticket.parse().expect("second ticket must parse");
    assert_eq!(
        second.endpoint_addr().id,
        first.endpoint_addr().id,
        "a restarted hub with the same identity must reprint the same node identity"
    );
    assert_eq!(
        ticket_ports(&second),
        ticket_ports(&first),
        "a restarted hub without an explicit port= must reuse the same recorded port"
    );
}

#[tokio::test]
async fn iroh_spec_with_unknown_parameter_errors_loudly() {
    // A typo inside an iroh:-prefixed spec must NEVER silently fall through
    // to the deprecated broker path — it errors at the transport, naming the
    // offending parameter.
    let edge = Arc::new(Database::open_memory());
    let client = SyncClient::new(
        edge,
        "iroh:?identtiy=/tmp/k.key&prot=4433",
        contextdb_core::TenantId::from("iroh-typo"),
    );
    let err = within(client.ensure_connected())
        .await
        .expect_err("a malformed iroh spec must error")
        .to_ascii_lowercase();
    assert!(
        err.contains("unknown parameter") && err.contains("identtiy"),
        "the error must name the unknown parameter, got: {err}"
    );
    assert!(
        !err.contains("nats") && !err.contains("broker"),
        "a malformed iroh spec must not be routed toward the deprecated broker path, got: {err}"
    );
}

// Every reachability capability the transport library provides is an explicit
// operator KNOB — off by default, never amputated.

#[test]
fn lookup_and_publish_are_explicit_opt_in_knobs() {
    // Defaults: absent — nothing published, nothing resolved beyond tickets.
    let default_spec =
        EndpointSpec::parse("iroh:?identity=/tmp/k.key").expect("default bind spec parses");
    assert_eq!(default_spec.publish(), None);
    assert_eq!(default_spec.lookup(), None);
    assert!(!default_spec.publishes_address_lookup());

    // Explicit opt-ins parse to their choices.
    let n0 = EndpointSpec::parse("iroh:?identity=/tmp/k.key&publish=n0&lookup=n0")
        .expect("n0 opt-in spec must parse");
    assert_eq!(n0.publish(), Some(&PublishChoice::N0));
    assert_eq!(n0.lookup(), Some(&LookupChoice::N0));
    assert!(n0.publishes_address_lookup());

    let self_hosted = EndpointSpec::parse(
        "iroh:?identity=/tmp/k.key&publish=https://pkarr.example.net&lookup=https://dns.example.net",
    )
    .expect("self-hosted lookup infrastructure spec must parse");
    assert_eq!(
        self_hosted.publish(),
        Some(&PublishChoice::Custom(
            "https://pkarr.example.net".to_string()
        ))
    );
    assert_eq!(
        self_hosted.lookup(),
        Some(&LookupChoice::Custom("https://dns.example.net".to_string()))
    );

    let mdns = EndpointSpec::parse("iroh:?identity=/tmp/k.key&lookup=mdns")
        .expect("LAN-local mdns lookup spec must parse");
    assert_eq!(mdns.lookup(), Some(&LookupChoice::Mdns));

    let dns_origin = EndpointSpec::parse("iroh:?identity=/tmp/k.key&lookup=dns:lookup.example.net")
        .expect("self-hosted DNS zone lookup spec must parse");
    assert_eq!(
        dns_origin.lookup(),
        Some(&LookupChoice::DnsOrigin("lookup.example.net".to_string()))
    );

    // The dial form takes the knobs too (an edge resolving a stale-address
    // ticket needs lookup on the DIALING side).
    let dir = tempfile::tempdir().expect("tempdir");
    let _ = dir; // ticket-bearing dial specs are exercised end to end below
}

#[tokio::test]
async fn published_hub_announces_to_the_operator_lookup_service() {
    // The PUBLISH knob end to end against a real (local) lookup service: the
    // hub opted into publish=<url> and its signed announce must arrive there.
    let (dns_pkarr, _lookup_url, pkarr_url) = start_local_dns_pkarr().await;

    let hub_dir = tempfile::tempdir().expect("hub tempdir");
    let spec = format!(
        "iroh:?identity={}&publish={pkarr_url}",
        identity_file(&hub_dir).display()
    );
    let hub = start_hub(&spec, "iroh-publish").await;

    let full: iroh_tickets::endpoint::EndpointTicket =
        hub.ticket.parse().expect("hub ticket parses");
    dns_pkarr
        .on_endpoint(&full.endpoint_addr().id, Duration::from_secs(10))
        .await
        .expect("the hub's announce must land on the operator's lookup service");

    hub.stop().await;
}

#[cfg(feature = "mdns")]
#[tokio::test]
async fn mdns_lookup_resolves_identity_only_tickets_on_the_lan() {
    // The LOOKUP knob end to end with zero third-party anything: both sides
    // opt into lookup=mdns (LAN-local), the edge's ticket carries ONLY the
    // hub's identity — no addresses — and the dial resolves via mDNS. An
    // IP/port change can never strand this edge.
    let hub_dir = tempfile::tempdir().expect("hub tempdir");
    let tenant = "iroh-mdns";
    let spec = format!(
        "iroh:?identity={}&lookup=mdns",
        identity_file(&hub_dir).display()
    );
    let hub = start_hub(&spec, tenant).await;

    let full: iroh_tickets::endpoint::EndpointTicket =
        hub.ticket.parse().expect("hub ticket parses");
    let id_only_ticket = iroh_tickets::endpoint::EndpointTicket::new(iroh::EndpointAddr::new(
        full.endpoint_addr().id,
    ))
    .to_string();

    let edge = Arc::new(Database::open_memory());
    create_notes_table(&edge);
    let id = Uuid::new_v4();
    insert_note(&edge, id, "via-mdns");
    let dial = format!("iroh:?to={id_only_ticket}&lookup=mdns");
    let client = SyncClient::new(edge.clone(), &dial, contextdb_core::TenantId::from(tenant));

    // mDNS convergence can take a few announce cycles; retry the push until
    // the resolve succeeds (bounded by the outer test timeout).
    let mut last_err = String::new();
    let mut pushed = false;
    for _ in 0..6 {
        match within(client.push()).await {
            Ok(_) => {
                pushed = true;
                break;
            }
            Err(err) => {
                last_err = err.to_string();
                client.reconnect().await;
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
    assert!(
        pushed,
        "push via an identity-only ticket must succeed once mDNS resolves the hub, last error: {last_err}"
    );
    assert_eq!(
        note_body(&hub.server_db, id).as_deref(),
        Some("via-mdns"),
        "the mdns-resolved push must land on the hub"
    );

    hub.stop().await;
}

#[tokio::test]
async fn peer_connection_protocol_exposes_raw_streams() {
    // The streaming half of the peer surface: the protocol owner gets the
    // raw connection (plus the caller's authenticated identity) and drives
    // its own streams — the media-transfer path's substrate.
    let dir = tempfile::tempdir().expect("tempdir");
    let endpoint = within(IrohServer::bind(&bind_spec(&identity_file(&dir))))
        .await
        .expect("bind peer node");
    let ticket = endpoint.ticket();

    let stream_alpn = b"contextdb.test.stream.v1".to_vec();
    endpoint.register_connection_protocol(
        stream_alpn.clone(),
        Arc::new(|peer: contextdb_server::transport::iroh::PeerConnection| {
            Box::pin(async move {
                let (mut send, mut recv) = peer.connection.accept_bi().await.map_err(|e| {
                    contextdb_server::transport::TransportError::Other(e.to_string())
                })?;
                let payload = recv.read_to_end(1024 * 1024).await.map_err(|e| {
                    contextdb_server::transport::TransportError::Other(e.to_string())
                })?;
                let mut reply = format!("stream-from={}:", peer.remote_node_id).into_bytes();
                reply.extend_from_slice(&payload);
                send.write_all(&reply).await.map_err(|e| {
                    contextdb_server::transport::TransportError::Other(e.to_string())
                })?;
                send.finish().map_err(|e| {
                    contextdb_server::transport::TransportError::Other(e.to_string())
                })?;
                Ok(())
            }) as contextdb_server::transport::TransportFuture<'static, ()>
        }),
    );

    let caller_dir = tempfile::tempdir().expect("caller tempdir");
    let caller_key = identity_file(&caller_dir);
    let caller = FabricIdentity::load_or_generate(&caller_key).expect("caller identity");
    let peer = within(peer_connect(&caller_key, &ticket, &stream_alpn))
        .await
        .expect("streaming peer connect must succeed");
    let (mut send, mut recv) = peer.connection.open_bi().await.expect("open raw stream");
    send.write_all(b"raw-bytes").await.expect("write");
    send.finish().expect("finish");
    let reply = recv.read_to_end(1024 * 1024).await.expect("read reply");
    let expected = format!("stream-from={}:raw-bytes", caller.node_id()).into_bytes();
    assert_eq!(
        reply, expected,
        "the raw-stream echo must round-trip and carry the caller's enrolled node_id"
    );
}

/// Spins a LOCAL DNS + pkarr pair (iroh test-utils) standing in for an
/// operator's self-hosted lookup service. Returns (guard, lookup-url,
/// publish-url) — both URLs point at the same local pkarr service.
async fn start_local_dns_pkarr() -> (iroh::test_utils::DnsPkarrServer, String, String) {
    let server = iroh::test_utils::DnsPkarrServer::run()
        .await
        .expect("local dns+pkarr servers");
    let url = server.pkarr_url().to_string();
    (server, url.clone(), url)
}

// Operator-trap regressions: fail fast, tell the truth about liveness, and
// never print a ticket whose stability was not persisted.

#[cfg(not(feature = "nats"))]
#[tokio::test]
async fn broker_spec_without_nats_feature_fails_serve_immediately() {
    // A server pointed at a broker URL on a build without the deprecated
    // NATS adapter must FAIL FAST with the actionable error — not park
    // silently until shutdown.
    use contextdb_server::transport::server_transport;
    let transport = server_transport("nats://localhost:4222");
    // The 2 s timeout wrapper IS the promptness proof: a serve() that parks
    // until shutdown trips it. (A tighter elapsed bound here was redundant
    // wall-clock slop that flipped under load — removed.)
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        transport.serve(Vec::new(), Arc::new(AtomicBool::new(false))),
    )
    .await;
    let err = result
        .expect("serve must return promptly, not park until shutdown")
        .expect_err("serve on the disabled broker path must error");
    let rendered = err.to_string().to_ascii_lowercase();
    assert!(
        rendered.contains("nats") && rendered.contains("feature"),
        "the error must name the deprecated adapter and its feature, got: {rendered}"
    );
}

#[tokio::test]
async fn sync_status_reports_unreachable_after_hub_dies() {
    let dir = tempfile::tempdir().expect("tempdir");
    let tenant = "iroh-liveness";
    let hub = start_hub(&bind_spec(&identity_file(&dir)), tenant).await;

    let edge = Arc::new(Database::open_memory());
    create_notes_table(&edge);
    insert_note(&edge, Uuid::new_v4(), "liveness-probe");
    let client = SyncClient::new(
        edge.clone(),
        &hub.ticket,
        contextdb_core::TenantId::from(tenant),
    );
    within(client.push()).await.expect("push while hub is up");
    assert!(
        client.is_connected().await,
        "connected while the hub serves"
    );

    hub.stop().await;
    // Bounded poll on the observable condition (was a fixed 1 s settle sleep):
    // after the hub dies the liveness probe must eventually report dead. The
    // deadline is a failure ceiling, not a timing assertion.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while client.is_connected().await {
        assert!(
            std::time::Instant::now() < deadline,
            "is_connected must report the dead hub within the failure ceiling"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    // The operator-facing health answer must tell the truth: the cached
    // connection is dead, so the client is NOT connected, and a fresh
    // ensure_connected must fail rather than hand back the corpse.
    let still_connected = client.is_connected().await;
    let reconnect_probe = within(client.ensure_connected()).await;
    assert!(
        !still_connected || reconnect_probe.is_err(),
        "after the hub dies, is_connected must be false (got {still_connected}) or ensure_connected must error (got {reconnect_probe:?})"
    );

    // And the strong form: is_connected alone must not report a dead link.
    assert!(
        !client.is_connected().await,
        "is_connected must probe liveness, not merely cached state"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn unwritable_identity_dir_fails_bind_loudly_before_ticket() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().expect("tempdir");
    let identity = identity_file(&dir);
    // Create the identity first (readable), then freeze the directory so the
    // sticky-port sibling cannot be written.
    FabricIdentity::load_or_generate(&identity).expect("identity");
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o555))
        .expect("freeze dir");

    let result = within(IrohServer::bind(&bind_spec(&identity))).await;
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o755)).expect("thaw dir");

    let err = match result {
        Ok(_) => panic!(
            "bind must fail loudly when the remembered-port file cannot be persisted — a ticket printed without port stickiness goes stale on restart"
        ),
        Err(err) => err,
    };
    let rendered = err.to_string().to_ascii_lowercase();
    assert!(
        rendered.contains("port"),
        "the error must name the port persistence problem, got: {rendered}"
    );
}

fn free_udp_port() -> u16 {
    let socket = std::net::UdpSocket::bind("127.0.0.1:0").expect("bind probe socket");
    let port = socket.local_addr().expect("probe addr").port();
    drop(socket);
    port
}

// Bounded tables — the shipped transport must authenticate its hub, because two
// protections read that identity and both go inert when it is `None`:
// `bind_retention_hub` (the multi-hub refusal never arms) and per-peer transfer
// receipts (a real edge keeps none). Only the in-process broker implemented
// `peer_node_id`, so every in-process test passed while the shipped path was
// silently unprotected.

#[tokio::test]
async fn iroh_client_reports_the_dialed_hub_as_its_authenticated_peer() {
    let dir = tempfile::tempdir().expect("tempdir");
    let hub = start_hub(&bind_spec(&identity_file(&dir)), "peer-identity").await;

    let client_transport = contextdb_server::transport::client_transport(&hub.ticket);
    assert_eq!(
        client_transport.peer_node_id().as_deref(),
        Some(hub.node_id.as_str()),
        "a client dials its hub BY KEY, so the shipped transport must report that \
         key as the authenticated peer — the default `None` leaves every \
         identity-keyed protection inert"
    );

    hub.stop().await;
}

#[tokio::test]
async fn iroh_push_arms_the_retention_hub_and_records_client_receipts() {
    use contextdb_server::{TransferDirection, TransferPlane};

    let dir = tempfile::tempdir().expect("tempdir");
    let tenant = "peer-identity-consequences";
    let hub = start_hub(&bind_spec(&identity_file(&dir)), tenant).await;

    let edge = Arc::new(Database::open_memory());
    edge.execute(
        "CREATE TABLE windows (id INTEGER PRIMARY KEY, body TEXT) \
         RETAIN 48 HOURS SYNC SAFE SYNC PUSH ONLY",
        &HashMap::new(),
    )
    .expect("retained table");
    for id in 1..6i64 {
        let mut row = HashMap::new();
        row.insert("id".to_string(), Value::Int64(id));
        row.insert("body".to_string(), Value::Text(format!("window-{id}")));
        edge.execute("INSERT INTO windows (id, body) VALUES ($id, $body)", &row)
            .expect("window insert");
    }

    let client = SyncClient::new(
        edge.clone(),
        &hub.ticket,
        contextdb_core::TenantId::from(tenant),
    );
    within(client.push()).await.expect("push over iroh");

    // Consequence 1: the multi-hub refusal is armed against a REAL hub identity.
    assert_eq!(
        edge.retention_sync_peer().as_deref(),
        Some(hub.node_id.as_str()),
        "a real push of a retained table must register the hub it delivered to; \
         an unregistered hub means the multi-hub refusal can never fire"
    );

    // Consequence 2: the edge keeps client-side receipts, keyed by that identity.
    let receipts = client.transfer_receipts();
    assert!(
        !receipts.is_empty(),
        "a real edge must keep transfer receipts for a delivered push: {receipts:?}"
    );
    assert!(
        receipts
            .iter()
            .all(|receipt| receipt.peer_node_id == hub.node_id),
        "every receipt must be keyed by the authenticated hub node id \
         ({}): {receipts:?}",
        hub.node_id
    );
    let sent = receipts
        .iter()
        .find(|receipt| {
            receipt.plane == TransferPlane::Sync && receipt.direction == TransferDirection::Sent
        })
        .unwrap_or_else(|| panic!("no Sync/Sent receipt in {receipts:?}"));
    assert!(
        sent.counters.items >= 5,
        "the receipt must count the rows actually delivered: {receipts:?}"
    );

    hub.stop().await;
}
