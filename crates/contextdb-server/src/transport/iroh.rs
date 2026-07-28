//! Iroh transport adapter: fills the transport seam with dial-by-key
//! networking. A machine is reached by its public key instead of a broker
//! address; two machines on one LAN sync with nothing in the middle.
//!
//! Reachability posture (pinned product position, architecture Section 10
//! rule 7): the default configuration contacts NO third-party infrastructure —
//! no public relays, no address-lookup publishing. Relays are explicit opt-in
//! (`relay=<url>` self-hosted, or `relay=n0` for the free public ones). The
//! adapter never publishes node addresses to any external lookup service;
//! reachability is tickets (and, when opted in, a relay).
//!
//! The word "iroh" may appear only in this module and the endpoint/relay
//! config surface — sync and protocol code stay transport-neutral.

use super::{
    ClientTransport, HandlerRegistration, IncomingRequest, Responder, ServerTransport,
    TransportError, TransportFuture, TransportResult, TransportStatusFuture,
};
use crate::identity::FabricIdentity;
use iroh::endpoint::{Connection, RelayMode};
use iroh::{Endpoint, EndpointAddr, RelayUrl, SecretKey, Watcher};
use iroh_tickets::endpoint::EndpointTicket;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// The sync protocol's ALPN. The trailing version is this adapter's own framing
/// version, distinct from the opaque payload's `PROTOCOL_VERSION`. This adapter
/// moves bytes and does not read the payload, so a payload-version bump does not
/// change the framing and this ALPN does not move — two peers built from this
/// code always agree on it, and a payload-version skew is caught later at the
/// envelope version check.
pub const SYNC_ALPN: &[u8] = b"contextdb.sync.v4";

/// Ceiling on one framed request or reply. Batching above the seam keeps
/// real payloads far below this; the ceiling only bounds memory against a
/// corrupt or hostile frame length.
const MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;
/// Bound on establishing one connection (dial + handshake).
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// QUIC idle ceiling: a silent connection is reaped after this. Near iroh's
/// default; paired with [`QUIC_KEEP_ALIVE`] so a wanted-but-idle connection
/// stays alive via keepalive frames and only a dead peer times out.
const QUIC_MAX_IDLE: Duration = Duration::from_secs(30);
/// Keepalive cadence, well under [`QUIC_MAX_IDLE`].
const QUIC_KEEP_ALIVE: Duration = Duration::from_secs(10);
/// Bound on waiting for the freshly bound endpoint to learn its own direct
/// addresses (needed before a ticket can be minted).
const ADDR_READY_TIMEOUT: Duration = Duration::from_secs(10);
/// How often the serving loop re-checks the shutdown flag.
const SHUTDOWN_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Reply status bytes on the wire.
const REPLY_OK: u8 = 0;
const REPLY_NO_HANDLER: u8 = 1;
const REPLY_HANDLER_ERROR: u8 = 2;

/// Address-lookup PUBLISHING choice: where this endpoint announces its
/// addresses. `None` (absent) is the default — nothing is published anywhere.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublishChoice {
    /// The free public pkarr relay run by n0. Explicit opt-in.
    N0,
    /// An operator-run pkarr relay, named by URL.
    Custom(String),
}

/// Address-lookup RESOLUTION choice: how this endpoint resolves other
/// endpoints' current addresses. `None` (absent) is the default — dialing
/// uses only the addresses a ticket carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LookupChoice {
    /// The free public DNS lookup run by n0. Explicit opt-in.
    N0,
    /// LAN-local mDNS — no third party, local network broadcast only.
    Mdns,
    /// An operator-run pkarr-relay lookup service, named by URL.
    Custom(String),
    /// An operator-run DNS lookup zone: `dns:<origin-domain>`, resolved with
    /// the system resolver (the self-hosted iroh-dns deployment shape).
    DnsOrigin(String),
}

/// Relay posture resolved from an endpoint spec. `Disabled` is the default:
/// LAN operation needs no relay and no internet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelayChoice {
    /// No relay: direct connections only. The default.
    Disabled,
    /// An operator-run relay, named by URL. Explicit opt-in.
    SelfHosted(String),
    /// The free public relays. Explicit opt-in.
    N0Public,
}

/// A parsed transport endpoint spec.
///
/// Server (bind) form: `iroh:?identity=<key-file>[&port=<u16>][&relay=<none|n0|url>]`
/// Client (dial) form: a ticket string printed by the server, optionally
/// prefixed `iroh:`.
#[derive(Debug, Clone)]
pub struct EndpointSpec {
    identity_path: Option<PathBuf>,
    port: Option<u16>,
    relay: RelayChoice,
    relay_ca: Option<PathBuf>,
    publish: Option<PublishChoice>,
    lookup: Option<LookupChoice>,
    dial_ticket: Option<String>,
}

// Grammar note: the dial form may pin the edge's own fabric identity —
// `iroh:?to=<ticket>&identity=<key-file>` — so the dialing endpoint
// originates from the enrolled identity instead of an ephemeral key.

impl EndpointSpec {
    /// Parse a spec string. Returns `None` when the string is not
    /// iroh-shaped (e.g. a `nats://` URL), so the factory can route it to
    /// another adapter. Malformed iroh-shaped specs also return `None` here;
    /// use [`EndpointSpec::parse_detailed`] to distinguish them.
    pub fn parse(spec: &str) -> Option<Self> {
        Self::parse_detailed(spec).ok().flatten()
    }

    /// Three-way parse: `Ok(Some)` = a valid spec for this adapter;
    /// `Ok(None)` = not iroh-shaped (route to another adapter); `Err` =
    /// iroh-shaped but INVALID. An `iroh:`-prefixed string never silently
    /// falls through to another transport — a typo errors loudly instead of
    /// dying later on the deprecated broker path.
    pub fn parse_detailed(spec: &str) -> Result<Option<Self>, String> {
        let trimmed = spec.trim();
        if let Some(rest) = trimmed.strip_prefix("iroh:") {
            if rest.is_empty() {
                return Ok(Some(Self::bind_defaults()));
            }
            if let Some(query) = rest.strip_prefix('?') {
                return Self::parse_bind_query(query).map(Some);
            }
            // `iroh:<ticket>`: an explicitly tagged dial spec.
            return match Self::parse_ticket(rest) {
                Some(parsed) => Ok(Some(parsed)),
                None => Err(format!(
                    "not a valid enrollment ticket after `iroh:` in sync endpoint spec: {trimmed}"
                )),
            };
        }
        // A bare ticket string is accepted verbatim — it is what the server
        // prints and what operators paste into config.
        Ok(Self::parse_ticket(trimmed))
    }

    fn bind_defaults() -> Self {
        Self {
            identity_path: None,
            port: None,
            relay: RelayChoice::Disabled,
            relay_ca: None,
            publish: None,
            lookup: None,
            dial_ticket: None,
        }
    }

    fn parse_bind_query(query: &str) -> Result<Self, String> {
        let mut spec = Self::bind_defaults();
        for pair in query.split('&').filter(|pair| !pair.is_empty()) {
            let Some((key, value)) = pair.split_once('=') else {
                return Err(format!(
                    "malformed parameter `{pair}` in sync endpoint spec (expected key=value)"
                ));
            };
            match key {
                "identity" => spec.identity_path = Some(PathBuf::from(value)),
                "port" => {
                    spec.port = Some(value.parse().map_err(|_| {
                        format!("invalid value `{value}` for `port` in sync endpoint spec")
                    })?)
                }
                "relay" => {
                    spec.relay = match value {
                        "none" => RelayChoice::Disabled,
                        "n0" => RelayChoice::N0Public,
                        url => RelayChoice::SelfHosted(url.to_string()),
                    }
                }
                "to" => {
                    let _: EndpointTicket = value.parse().map_err(|_| {
                        "invalid enrollment ticket in `to=` of sync endpoint spec".to_string()
                    })?;
                    spec.dial_ticket = Some(value.to_string());
                }
                // Explicit trust for a self-hosted relay's private/self-signed
                // certificate: a PEM or DER cert file. An operator knob,
                // never a default.
                "relay-ca" => spec.relay_ca = Some(PathBuf::from(value)),
                // Address-lookup knobs — off by default, the operator's
                // switch.
                "publish" => {
                    spec.publish = Some(match value {
                        "n0" => PublishChoice::N0,
                        url => PublishChoice::Custom(url.to_string()),
                    })
                }
                "lookup" => {
                    spec.lookup = Some(match value {
                        "n0" => LookupChoice::N0,
                        "mdns" => LookupChoice::Mdns,
                        origin if origin.starts_with("dns:") => {
                            LookupChoice::DnsOrigin(origin["dns:".len()..].to_string())
                        }
                        url => LookupChoice::Custom(url.to_string()),
                    })
                }
                unknown => {
                    return Err(format!(
                        "unknown parameter `{unknown}` in sync endpoint spec (accepted: identity, port, relay, relay-ca, publish, lookup, to)"
                    ));
                }
            }
        }
        Ok(spec)
    }

    fn parse_ticket(candidate: &str) -> Option<Self> {
        let ticket: EndpointTicket = candidate.parse().ok()?;
        let _ = ticket;
        Some(Self {
            identity_path: None,
            port: None,
            relay: RelayChoice::Disabled,
            relay_ca: None,
            publish: None,
            lookup: None,
            dial_ticket: Some(candidate.to_string()),
        })
    }

    pub fn identity_path(&self) -> Option<&Path> {
        self.identity_path.as_deref()
    }

    pub fn port(&self) -> Option<u16> {
        self.port
    }

    pub fn relay(&self) -> &RelayChoice {
        &self.relay
    }

    /// Whether this endpoint will publish its addresses to any external
    /// address-lookup service. `false` unless the operator explicitly opted
    /// in with `publish=` — OFF is the default, the switch is the operator's.
    pub fn publishes_address_lookup(&self) -> bool {
        self.publish.is_some()
    }

    /// The ticket to dial, when this is a client (dial) spec.
    pub fn dial_ticket(&self) -> Option<&str> {
        self.dial_ticket.as_deref()
    }

    /// The operator-supplied relay CA certificate file, when explicitly
    /// trusting a self-hosted relay's private/self-signed certificate.
    pub fn relay_ca(&self) -> Option<&Path> {
        self.relay_ca.as_deref()
    }

    /// Where this endpoint publishes its addresses, when the operator opted
    /// in. Absent by default — nothing is announced anywhere.
    pub fn publish(&self) -> Option<&PublishChoice> {
        self.publish.as_ref()
    }

    /// How this endpoint resolves other endpoints' addresses, when the
    /// operator opted in. Absent by default — tickets carry the addresses.
    pub fn lookup(&self) -> Option<&LookupChoice> {
        self.lookup.as_ref()
    }
}

/// True when `spec` names this adapter (a bind spec or a dialable ticket).
pub fn is_iroh_endpoint(spec: &str) -> bool {
    EndpointSpec::parse(spec).is_some()
}

/// Build a server bind spec from an identity key-file path, in this
/// adapter's own spec grammar. Centralizes the format string so a consumer
/// never spells the URI scheme literal itself; re-exported transport-
/// neutrally as `peer_bind_spec` at the crate root for exactly that reason.
pub fn bind_spec(identity_path: &Path) -> String {
    format!("iroh:?identity={}", identity_path.display())
}

/// Build a dial spec that enrolls against a hub's printed ticket while
/// pinning the dialing endpoint to its own identity key-file path.
/// Re-exported transport-neutrally as `peer_dial_spec` at the crate root.
pub fn dial_spec(ticket: &str, identity_path: &Path) -> String {
    format!("iroh:?to={ticket}&identity={}", identity_path.display())
}

/// One request arriving on a registered peer protocol. Carries WHO is
/// asking — the caller's fabric identity (`node_id`) as authenticated by the
/// transport handshake — because the media-transfer path authorizes fetches
/// by node identity.
pub struct PeerRequest {
    pub remote_node_id: String,
    pub bytes: Vec<u8>,
}

/// Handler for an additional peer protocol registered on a serving endpoint:
/// an authenticated peer request in, complete reply bytes out. This is the
/// fabric-internal peer surface the media-transfer path builds on.
pub type PeerHandler = Arc<dyn Fn(PeerRequest) -> TransportFuture<'static, Vec<u8>> + Send + Sync>;

type PeerProtocols = Arc<Mutex<HashMap<Vec<u8>, PeerHandler>>>;
type PeerConnectionProtocols = Arc<Mutex<HashMap<Vec<u8>, PeerConnectionHandler>>>;
type SyncRoutes = Arc<Mutex<Option<Arc<HashMap<String, super::RequestHandler>>>>>;

/// A bound serving endpoint: holds the fabric identity, accepts sync
/// connections on [`SYNC_ALPN`], and can register additional protocol labels
/// for node-to-node streams that never touch the hub.
///
/// One accept loop per endpoint (spawned at bind) dispatches every incoming
/// connection by its ALPN: sync connections go to the routes the sync
/// `ServerTransport` installed, everything else to a registered peer
/// protocol. The loop holds its own endpoint handle, so dropping this struct
/// never tears down an actively serving transport; the endpoint is closed
/// when the sync serve loop exits (freeing the port for a rebind).
pub struct IrohServer {
    endpoint: Endpoint,
    ticket: String,
    node_id: String,
    peer_protocols: PeerProtocols,
    peer_connection_protocols: PeerConnectionProtocols,
    sync_routes: SyncRoutes,
}

impl IrohServer {
    /// Bind an endpoint per `spec` (must be a bind spec with an identity
    /// path). The keypair is loaded or generated by the fabric identity
    /// module and handed to the endpoint — never minted by the transport.
    pub async fn bind(spec: &str) -> TransportResult<Self> {
        let parsed = EndpointSpec::parse(spec).ok_or_else(|| {
            TransportError::Unreachable(format!("not a bindable endpoint spec: {spec}"))
        })?;
        if parsed.dial_ticket().is_some() {
            return Err(TransportError::Unreachable(format!(
                "cannot bind a serving endpoint from a dial ticket: {spec}"
            )));
        }
        let identity_path = parsed.identity_path().ok_or_else(|| {
            TransportError::Unreachable(format!(
                "a serving endpoint spec must carry identity=<key-file>: {spec}"
            ))
        })?;
        let identity = FabricIdentity::load_or_generate(identity_path)
            .map_err(|err| TransportError::Unreachable(err.to_string()))?;
        // Port stickiness: a hub bound WITHOUT port= records its chosen port
        // beside the identity key and reuses it on restart, so issued tickets
        // stay valid. An explicit port= always wins; a sticky port that can
        // no longer bind fails LOUDLY rather than silently minting a new
        // port, which would strand any already-issued address-only ticket
        // (the default no-lookup config). This is not identity rotation —
        // the identity above is loaded solely from the key file and is
        // unaffected by which port the endpoint binds.
        let sticky = match parsed.port() {
            Some(_) => None,
            None => Some(sticky_port_path(identity_path)),
        };
        let remembered = sticky.as_deref().and_then(read_sticky_ports);
        let remembered_v4 = remembered.map(|(v4, _)| v4);
        let remembered_v6 = remembered.and_then(|(_, v6)| v6);
        let bind_port = parsed.port().or(remembered_v4);
        // The IPv6 socket must be pinned too: the endpoint builder always
        // opens a second socket on `[::]`, and on a host with a global IPv6
        // address that socket's port lands in the ticket — left random, it
        // would change the ticket on every restart (the multi-homed / VPS
        // hub case). An explicit port= pins both families (v6 best-effort,
        // so v4-only hosts still bind); a remembered v6 port re-binds
        // required and fails as loudly as v4; a first bind leaves the
        // builder's default v6 socket to pick the port that then gets
        // remembered.
        let v6_pin = match (parsed.port(), remembered_v6) {
            (Some(port), _) => Some((port, false)),
            (None, Some(port)) => Some((port, true)),
            (None, None) => None,
        };
        let endpoint = build_endpoint(
            &identity,
            bind_port,
            v6_pin,
            parsed.relay(),
            parsed.relay_ca(),
            parsed.publish(),
            parsed.lookup(),
        )
        .await
        .map_err(|err| match (remembered, &sticky) {
            (Some((v4, v6)), Some(sticky_path)) => {
                let ports = match v6 {
                    Some(v6) => format!("{v4} (and v6 {v6})"),
                    None => v4.to_string(),
                };
                TransportError::Unreachable(format!(
                    "{err} (while re-binding the remembered sync port {ports} from {}; free the \
                     port, or pass an explicit port= — port=0 picks a fresh random port, which \
                     strands address-only tickets issued under the old port but does not rotate \
                     the hub's identity)",
                    sticky_path.display()
                ))
            }
            _ => err,
        })?;

        // A relay-opted hub waits until it is homed on its relay before
        // minting the ticket, so the ticket can carry the relay address.
        if parsed.relay() != &RelayChoice::Disabled {
            let _ = tokio::time::timeout(ADDR_READY_TIMEOUT, endpoint.online()).await;
        }

        // Every post-bind failure closes the endpoint before returning, so a
        // refused bind never aborts its socket ungracefully.
        let addr = match wait_for_direct_addrs(&endpoint).await {
            Ok(addr) => addr,
            Err(err) => {
                endpoint.close().await;
                return Err(err);
            }
        };
        // Persist the ACTUAL bound ports, family-aware, read from the local
        // sockets (never from discovered addresses, whose arrival order is
        // timing-dependent). Both ports must survive a restart for the
        // ticket to survive one.
        if let Some(sticky_path) = &sticky {
            let mut v4_port = None;
            let mut v6_port = None;
            for sock in endpoint.bound_sockets() {
                match sock {
                    SocketAddr::V4(s) if v4_port.is_none() => v4_port = Some(s.port()),
                    SocketAddr::V6(s) if v6_port.is_none() => v6_port = Some(s.port()),
                    _ => {}
                }
            }
            if let Some(v4) = v4_port
                && remembered != Some((v4, v6_port))
            {
                let rendered = match v6_port {
                    Some(v6) => format!("{v4},{v6}"),
                    None => v4.to_string(),
                };
                if let Err(err) = std::fs::write(sticky_path, rendered) {
                    // A ticket whose port stickiness was not persisted goes
                    // stale on restart — refuse loudly rather than print a
                    // lying ticket.
                    endpoint.close().await;
                    return Err(TransportError::Unreachable(format!(
                        "cannot persist the remembered sync port to {}: {err} (the enrollment \
                         ticket would not survive a restart; fix the permissions or pass an \
                         explicit port=)",
                        sticky_path.display()
                    )));
                }
            }
        }
        let ticket = EndpointTicket::new(addr).to_string();
        let node_id = identity.node_id();
        let bound_id_bytes: [u8; 32] = *endpoint.id().as_bytes();
        debug_assert_eq!(
            bound_id_bytes,
            identity.public_key_bytes(),
            "the endpoint identity must be the fabric identity handed in"
        );

        let server = Self {
            endpoint,
            ticket,
            node_id,
            peer_protocols: Arc::new(Mutex::new(HashMap::new())),
            peer_connection_protocols: Arc::new(Mutex::new(HashMap::new())),
            sync_routes: Arc::new(Mutex::new(None)),
        };
        server.spawn_accept_loop();
        Ok(server)
    }

    /// The enrollment ticket: this node's public key plus how to reach it.
    /// Pasting this string into another machine's sync-endpoint config pairs
    /// the two.
    pub fn ticket(&self) -> String {
        self.ticket.clone()
    }

    /// This node's fabric identity as recorded in rows (lowercase hex of the
    /// ed25519 public key). Matches `FabricIdentity::node_id`.
    pub fn node_id(&self) -> String {
        self.node_id.clone()
    }

    /// The serving side of the sync seam over this endpoint. Self-sufficient:
    /// it holds its own handles, so dropping the `IrohServer` never tears
    /// down an actively serving transport.
    pub fn transport(&self) -> Arc<dyn ServerTransport> {
        Arc::new(IrohServerTransport {
            endpoint: self.endpoint.clone(),
            sync_routes: self.sync_routes.clone(),
        })
    }

    /// Close the endpoint gracefully, releasing its port.
    pub async fn close(self) {
        self.endpoint.close().await;
    }

    /// Register a CONNECTION-level protocol label: the handler receives each
    /// accepted connection (with the caller's authenticated identity) and
    /// owns its streams. The streaming half of the peer surface.
    pub fn register_connection_protocol(&self, alpn: Vec<u8>, handler: PeerConnectionHandler) {
        {
            let mut protocols = self
                .peer_connection_protocols
                .lock()
                .unwrap_or_else(|err| err.into_inner());
            protocols.insert(alpn, handler);
        }
        self.refresh_accepted_alpns();
    }

    /// Register an additional protocol label (ALPN) served by this endpoint.
    /// Peers dial it with [`peer_request`]; bytes go node-to-node, never
    /// through the hub.
    pub fn register_protocol(&self, alpn: Vec<u8>, handler: PeerHandler) {
        {
            let mut protocols = self
                .peer_protocols
                .lock()
                .unwrap_or_else(|err| err.into_inner());
            protocols.insert(alpn, handler);
        }
        self.refresh_accepted_alpns();
    }

    fn refresh_accepted_alpns(&self) {
        let mut alpns = vec![SYNC_ALPN.to_vec()];
        {
            let protocols = self
                .peer_protocols
                .lock()
                .unwrap_or_else(|err| err.into_inner());
            alpns.extend(protocols.keys().cloned());
        }
        {
            let protocols = self
                .peer_connection_protocols
                .lock()
                .unwrap_or_else(|err| err.into_inner());
            alpns.extend(protocols.keys().cloned());
        }
        self.endpoint.set_alpns(alpns);
    }

    /// The single accept loop for this endpoint: dispatches every incoming
    /// connection by ALPN.
    fn spawn_accept_loop(&self) {
        let endpoint = self.endpoint.clone();
        let protocols = self.peer_protocols.clone();
        let connection_protocols = self.peer_connection_protocols.clone();
        let sync_routes = self.sync_routes.clone();
        tokio::spawn(async move {
            while let Some(incoming) = endpoint.accept().await {
                let protocols = protocols.clone();
                let connection_protocols = connection_protocols.clone();
                let sync_routes = sync_routes.clone();
                tokio::spawn(async move {
                    let Ok(connection) = incoming.await else {
                        return;
                    };
                    let alpn = connection.alpn().to_vec();
                    if alpn == SYNC_ALPN {
                        let routes = {
                            let slot = sync_routes.lock().unwrap_or_else(|err| err.into_inner());
                            slot.clone()
                        };
                        let Some(routes) = routes else {
                            connection.close(1u32.into(), b"sync not serving");
                            return;
                        };
                        serve_sync_connection(connection, routes).await;
                        return;
                    }
                    let handler = {
                        let protocols = protocols.lock().unwrap_or_else(|err| err.into_inner());
                        protocols.get(&alpn).cloned()
                    };
                    if let Some(handler) = handler {
                        serve_peer_connection(connection, handler).await;
                        return;
                    }
                    let connection_handler = {
                        let protocols = connection_protocols
                            .lock()
                            .unwrap_or_else(|err| err.into_inner());
                        protocols.get(&alpn).cloned()
                    };
                    let Some(handler) = connection_handler else {
                        connection.close(1u32.into(), b"unknown protocol");
                        return;
                    };
                    let keepalive = connection.clone();
                    let peer = PeerConnection {
                        remote_node_id: hex_node_id(&connection.remote_id()),
                        connection,
                        endpoint: None,
                    };
                    if let Err(err) = handler(peer).await {
                        tracing::debug!(error = %err, "peer connection handler ended with error");
                    }
                    // Keep the server side alive until the remote closes, so
                    // replies written by the handler are never cut off by an
                    // early local drop.
                    let _ = keepalive.closed().await;
                });
            }
        });
    }
}

async fn serve_peer_connection(connection: Connection, handler: PeerHandler) {
    while let Ok((mut send, mut recv)) = connection.accept_bi().await {
        let Ok(request) = read_length_prefixed(&mut recv, MAX_FRAME_BYTES).await else {
            return;
        };
        let request = PeerRequest {
            remote_node_id: hex_node_id(&connection.remote_id()),
            bytes: request,
        };
        match handler(request).await {
            Ok(reply) => {
                if write_reply(&mut send, REPLY_OK, &reply).await.is_err() {
                    return;
                }
            }
            Err(err) => {
                let detail = err.to_string();
                let _ = write_reply(&mut send, REPLY_HANDLER_ERROR, detail.as_bytes()).await;
            }
        }
    }
}

/// Open a connection to the node named by `target_ticket` under `alpn`, send
/// `request`, and return the reply. The fabric-internal peer surface.
/// Connections originate from the ENROLLED node identity at `identity_path`
/// (loaded or created there), never from a transport-minted throwaway key —
/// the fabric-owned-identity rule applies to the dialing side too.
pub async fn peer_request(
    identity_path: &Path,
    target_ticket: &str,
    alpn: &[u8],
    request: Vec<u8>,
    timeout: Duration,
) -> TransportResult<Vec<u8>> {
    let (target, parsed) = parse_dial_target(target_ticket)?;
    let identity = dialing_identity(Some(identity_path))?;
    tokio::time::timeout(timeout, async move {
        let relay = relay_choice_for_target(&target);
        let endpoint = build_endpoint(
            &identity,
            None,
            None,
            &relay,
            parsed.relay_ca(),
            parsed.publish(),
            parsed.lookup(),
        )
        .await?;
        let connection = connect(&endpoint, target, alpn).await?;
        let reply = exchange_frame(&connection, None, &request).await;
        endpoint.close().await;
        reply
    })
    .await
    .map_err(|_| TransportError::Timeout)?
}

/// An accepted (or dialed) peer-protocol connection: the caller's
/// authenticated fabric identity plus the raw transport connection, on which
/// the protocol owner opens/accepts its own streams. This is the streaming
/// half of the fabric-internal peer surface (the media-transfer path's
/// substrate); [`peer_request`] remains the one-shot request-reply half.
pub struct PeerConnection {
    pub remote_node_id: String,
    pub connection: Connection,
    /// Present on DIALED connections: the dialing endpoint, kept alive for
    /// the connection's lifetime. Close gracefully via
    /// [`PeerConnection::close`].
    endpoint: Option<Endpoint>,
}

impl PeerConnection {
    /// Gracefully close the connection (and the dialing endpoint, when this
    /// side dialed).
    pub async fn close(self) {
        self.connection.close(0u32.into(), b"done");
        if let Some(endpoint) = self.endpoint {
            endpoint.close().await;
        }
    }
}

/// Handler for a connection-level peer protocol: receives every accepted
/// connection on the registered label.
pub type PeerConnectionHandler =
    Arc<dyn Fn(PeerConnection) -> TransportFuture<'static, ()> + Send + Sync>;

/// Dial `target_ticket` under `alpn` AS the enrolled identity at
/// `identity_path`, returning the raw connection for protocol-owned streams.
pub async fn peer_connect(
    identity_path: &Path,
    target_ticket: &str,
    alpn: &[u8],
) -> TransportResult<PeerConnection> {
    let (target, parsed) = parse_dial_target(target_ticket)?;
    let identity = dialing_identity(Some(identity_path))?;
    let relay = relay_choice_for_target(&target);
    let endpoint = build_endpoint(
        &identity,
        None,
        None,
        &relay,
        parsed.relay_ca(),
        parsed.publish(),
        parsed.lookup(),
    )
    .await?;
    let remote_node_id = hex_node_id(&target.id);
    let connection = match connect(&endpoint, target, alpn).await {
        Ok(connection) => connection,
        Err(err) => {
            endpoint.close().await;
            return Err(err);
        }
    };
    Ok(PeerConnection {
        remote_node_id,
        connection,
        endpoint: Some(endpoint),
    })
}

pub(super) fn client(spec: &str) -> Arc<dyn ClientTransport> {
    Arc::new(IrohClientTransport {
        spec: spec.to_string(),
        state: tokio::sync::Mutex::new(None),
    })
}

pub(super) fn server(spec: &str) -> Arc<dyn ServerTransport> {
    Arc::new(LazyBoundServerTransport {
        spec: spec.to_string(),
    })
}

struct ClientState {
    endpoint: Endpoint,
    connection: Connection,
}

struct IrohClientTransport {
    spec: String,
    state: tokio::sync::Mutex<Option<ClientState>>,
}

impl IrohClientTransport {
    async fn connected_state(&self) -> TransportResult<Connection> {
        let mut state = self.state.lock().await;
        if let Some(existing) = state.as_ref() {
            // Liveness, not just cache presence: a connection the remote
            // closed (hub death/restart) must not be handed back.
            if existing.connection.close_reason().is_none() {
                return Ok(existing.connection.clone());
            }
            if let Some(dead) = state.take() {
                dead.endpoint.close().await;
            }
        }
        let (target, parsed) = parse_dial_target(&self.spec)?;
        let identity = dialing_identity(parsed.identity_path())?;
        let relay = relay_choice_for_target(&target);
        let endpoint = build_endpoint(
            &identity,
            None,
            None,
            &relay,
            parsed.relay_ca(),
            parsed.publish(),
            parsed.lookup(),
        )
        .await?;
        let connection = match connect(&endpoint, target, SYNC_ALPN).await {
            Ok(connection) => connection,
            Err(err) => {
                endpoint.close().await;
                return Err(err);
            }
        };
        *state = Some(ClientState {
            endpoint,
            connection: connection.clone(),
        });
        Ok(connection)
    }

    async fn drop_state(&self) {
        let mut state = self.state.lock().await;
        if let Some(old) = state.take() {
            old.connection.close(0u32.into(), b"reconnect");
            old.endpoint.close().await;
        }
    }

    async fn request_once(
        &self,
        subject: &str,
        request_bytes: &[u8],
        timeout: Duration,
    ) -> TransportResult<Vec<u8>> {
        let connection = self.connected_state().await?;
        let result = tokio::time::timeout(
            timeout,
            exchange_frame(&connection, Some(subject), request_bytes),
        )
        .await
        .map_err(|_| TransportError::Timeout)?;
        if result.is_err() {
            // A failed exchange usually means the connection died (hub
            // restart). Drop it so the next request redials.
            self.drop_state().await;
        }
        result
    }
}

impl ClientTransport for IrohClientTransport {
    /// The hub this client is authenticated against. A sync client dials its
    /// hub BY KEY, so the identity is carried by the dial ticket itself and is
    /// known before any connection exists — the same derivation
    /// [`peer_connect`] performs for the media plane.
    ///
    /// Read from the spec rather than the live connection on purpose: this is a
    /// synchronous trait method and the connection sits behind an async mutex,
    /// so consulting it would mean blocking on that lock inside a sync fn. The
    /// ticket yields the identical value, needs no lock, and is available on
    /// the FIRST push — which matters, because the retention hub-binding and
    /// the per-peer transfer receipts both read this before a connection is
    /// necessarily established. `None` only when the spec is not a dialable
    /// ticket, which is the honest answer: no authenticated peer.
    fn peer_node_id(&self) -> Option<String> {
        let (target, _) = parse_dial_target(&self.spec).ok()?;
        Some(hex_node_id(&target.id))
    }

    fn has_stable_edge_identity(&self) -> bool {
        parse_dial_target(&self.spec).is_ok_and(|(_, parsed)| parsed.identity_path().is_some())
    }

    fn ensure_connected<'a>(&'a self) -> TransportFuture<'a, ()> {
        Box::pin(async move {
            self.connected_state().await?;
            Ok(())
        })
    }

    fn reconnect<'a>(&'a self) -> TransportFuture<'a, ()> {
        Box::pin(async move {
            self.drop_state().await;
            self.connected_state().await?;
            Ok(())
        })
    }

    fn is_connected<'a>(&'a self) -> TransportStatusFuture<'a> {
        Box::pin(async move {
            self.state
                .lock()
                .await
                .as_ref()
                .is_some_and(|state| state.connection.close_reason().is_none())
        })
    }

    fn shutdown<'a>(&'a self) -> TransportFuture<'a, ()> {
        Box::pin(async move {
            self.drop_state().await;
            Ok(())
        })
    }

    fn request<'a>(
        &'a self,
        subject: &'a str,
        request_bytes: Vec<u8>,
        timeout: Duration,
    ) -> TransportFuture<'a, Vec<u8>> {
        Box::pin(async move {
            match self.request_once(subject, &request_bytes, timeout).await {
                Ok(reply) => Ok(reply),
                Err(TransportError::NoResponder) => Err(TransportError::NoResponder),
                Err(TransportError::Status(detail)) => Err(TransportError::Status(detail)),
                Err(_first) => {
                    // One redial covers the hub-restart case; a second
                    // failure is the real answer.
                    self.request_once(subject, &request_bytes, timeout).await
                }
            }
        })
    }
}

/// Serving transport for `SyncServer::new(db, "iroh:?...", ...)`: binds at
/// serve time from the spec.
struct LazyBoundServerTransport {
    spec: String,
}

impl ServerTransport for LazyBoundServerTransport {
    fn serve<'a>(
        &'a self,
        handlers: Vec<HandlerRegistration>,
        shutdown: Arc<AtomicBool>,
    ) -> TransportFuture<'a, ()> {
        Box::pin(async move {
            let server = IrohServer::bind(&self.spec).await?;
            tracing::info!(
                ticket = %server.ticket(),
                node_id = %server.node_id(),
                "sync endpoint bound; enroll edges with this ticket"
            );
            let transport = server.transport();
            transport.serve(handlers, shutdown).await
        })
    }
}

/// The bound serving side: installs the sync route table on the endpoint's
/// accept loop, parks until shutdown, then uninstalls the routes and closes
/// the endpoint (freeing the port for a rebind — a restarted hub with the
/// same identity and port reproduces the same ticket).
struct IrohServerTransport {
    endpoint: Endpoint,
    sync_routes: SyncRoutes,
}

impl ServerTransport for IrohServerTransport {
    fn serve<'a>(
        &'a self,
        handlers: Vec<HandlerRegistration>,
        shutdown: Arc<AtomicBool>,
    ) -> TransportFuture<'a, ()> {
        Box::pin(async move {
            let mut routes: HashMap<String, super::RequestHandler> = HashMap::new();
            for registration in handlers {
                routes.insert(registration.subject, registration.handler);
            }
            {
                let mut slot = self
                    .sync_routes
                    .lock()
                    .unwrap_or_else(|err| err.into_inner());
                *slot = Some(Arc::new(routes));
            }

            let mut shutdown_poll = tokio::time::interval(SHUTDOWN_POLL_INTERVAL);
            while !shutdown.load(Ordering::SeqCst) {
                shutdown_poll.tick().await;
            }

            {
                let mut slot = self
                    .sync_routes
                    .lock()
                    .unwrap_or_else(|err| err.into_inner());
                *slot = None;
            }
            self.endpoint.close().await;
            Ok(())
        })
    }
}

async fn serve_sync_connection(
    connection: Connection,
    routes: Arc<HashMap<String, super::RequestHandler>>,
) {
    // The dialing edge's authenticated fabric identity, constant for the life
    // of this connection — the hub attributes every exchange on it (push /
    // pull / status) to this node when recording last-contact.
    let remote_node_id = hex_node_id(&connection.remote_id());
    loop {
        let Ok((mut send, mut recv)) = connection.accept_bi().await else {
            return;
        };
        let Ok((subject, payload)) = read_request_frame(&mut recv).await else {
            return;
        };
        let Some(handler) = routes.get(&subject).cloned() else {
            let _ = write_reply(&mut send, REPLY_NO_HANDLER, subject.as_bytes()).await;
            continue;
        };
        // The responder owns the send half; the handler may reply from a
        // spawned task (the push apply path does).
        let send_slot = Arc::new(tokio::sync::Mutex::new(Some(send)));
        let responder: Responder = Box::new({
            let send_slot = send_slot.clone();
            move |response_bytes| {
                Box::pin(async move {
                    let mut slot = send_slot.lock().await;
                    let Some(mut send) = slot.take() else {
                        return Err(TransportError::Other(
                            "reply already sent on this stream".to_string(),
                        ));
                    };
                    write_reply(&mut send, REPLY_OK, &response_bytes).await
                }) as TransportFuture<'static, ()>
            }
        });
        if let Err(err) = handler(IncomingRequest {
            bytes: payload,
            responder,
            node_id: Some(remote_node_id.clone()),
        })
        .await
        {
            let detail = err.to_string();
            let mut slot = send_slot.lock().await;
            if let Some(mut send) = slot.take() {
                let _ = write_reply(&mut send, REPLY_HANDLER_ERROR, detail.as_bytes()).await;
            }
            tracing::error!(error = %detail, "sync transport request failed");
        }
    }
}

async fn build_endpoint(
    identity: &FabricIdentity,
    port: Option<u16>,
    v6_pin: Option<(u16, bool)>,
    relay: &RelayChoice,
    relay_ca: Option<&Path>,
    publish: Option<&PublishChoice>,
    lookup: Option<&LookupChoice>,
) -> TransportResult<Endpoint> {
    // `presets::Minimal` installs the crypto provider but adds NO address
    // lookup and NO relays — the endpoint is not published anywhere. This is
    // the load-bearing line for the no-third-party-contact posture.
    let secret_key = SecretKey::from_bytes(&identity.secret_seed());
    // Explicit QUIC idle bound: a connection that goes silent — a peer that
    // opened a stream and stopped, or a half-dead NAT binding — is reaped at
    // MAX_IDLE, so a stalled holder can never pin a serve task or a socket
    // indefinitely. KEEP_ALIVE (well under MAX_IDLE) sends keepalive frames so
    // a wanted-but-idle connection (an edge parked on the sync hub between
    // pushes) survives; only a genuinely dead peer times out. This is the
    // wire-level complement to the media path's own per-progress FETCH_IDLE.
    let transport_config = iroh::endpoint::QuicTransportConfig::builder()
        .max_idle_timeout(Some(
            iroh::endpoint::IdleTimeout::try_from(QUIC_MAX_IDLE)
                .expect("30s is a representable QUIC idle timeout"),
        ))
        .keep_alive_interval(QUIC_KEEP_ALIVE)
        .build();
    let mut builder = Endpoint::builder(iroh::endpoint::presets::Minimal)
        .secret_key(secret_key)
        .transport_config(transport_config)
        .alpns(vec![SYNC_ALPN.to_vec()]);
    builder = match relay {
        RelayChoice::Disabled => builder.relay_mode(RelayMode::Disabled),
        RelayChoice::N0Public => builder.relay_mode(RelayMode::Default),
        RelayChoice::SelfHosted(url) => {
            let relay_url: RelayUrl = url.parse().map_err(|_| {
                TransportError::Unreachable(format!("invalid relay url in endpoint spec: {url}"))
            })?;
            builder.relay_mode(RelayMode::Custom(relay_url.into()))
        }
    };
    if let Some(ca_path) = relay_ca {
        let roots = load_relay_ca_certs(ca_path)?;
        builder = builder.ca_tls_config(iroh::tls::CaTlsConfig::custom_roots(roots));
    }
    // Address-lookup knobs: OFF by default, every capability the library
    // provides is an explicit operator switch.
    if let Some(publish) = publish {
        // Unfiltered: publish direct addresses too — on the no-relay default
        // a relay-only filter would announce nothing useful.
        let unfiltered = iroh::address_lookup::AddrFilter::unfiltered();
        builder = match publish {
            PublishChoice::N0 => builder.address_lookup(
                iroh::address_lookup::PkarrPublisher::n0_dns().addr_filter(unfiltered),
            ),
            PublishChoice::Custom(url) => {
                let url: url::Url = url.parse().map_err(|_| {
                    TransportError::Unreachable(format!(
                        "invalid publish service url in sync endpoint spec: {url}"
                    ))
                })?;
                builder.address_lookup(
                    iroh::address_lookup::PkarrPublisher::builder(url).addr_filter(unfiltered),
                )
            }
        };
    }
    if let Some(lookup) = lookup {
        builder = match lookup {
            LookupChoice::N0 => {
                builder.address_lookup(iroh::address_lookup::DnsAddressLookup::n0_dns())
            }
            #[cfg(feature = "mdns")]
            LookupChoice::Mdns => {
                builder.address_lookup(iroh_mdns_address_lookup::MdnsAddressLookup::builder())
            }
            #[cfg(not(feature = "mdns"))]
            LookupChoice::Mdns => {
                return Err(TransportError::Unreachable(
                    "lookup=mdns needs a build with the `mdns` cargo feature \
                     (cargo build -p contextdb-server --features mdns)"
                        .to_string(),
                ));
            }
            LookupChoice::Custom(url) => {
                let url: url::Url = url.parse().map_err(|_| {
                    TransportError::Unreachable(format!(
                        "invalid lookup service url in sync endpoint spec: {url}"
                    ))
                })?;
                builder.address_lookup(iroh::address_lookup::PkarrResolver::builder(url))
            }
            LookupChoice::DnsOrigin(origin) => builder.address_lookup(
                iroh::address_lookup::DnsAddressLookup::builder(origin.clone()),
            ),
        };
    }
    let bind_addr: SocketAddr = format!("0.0.0.0:{}", port.unwrap_or(0))
        .parse()
        .expect("static bind address is valid");
    let mut builder = builder
        .bind_addr(bind_addr)
        .map_err(|err| TransportError::Unreachable(format!("invalid bind address: {err}")))?;
    // Pin the IPv6 socket when a port for it is known (see the caller): the
    // builder's default `[::]` socket otherwise picks a fresh random port on
    // every bind. `required: false` lets a v4-only host proceed without v6.
    if let Some((v6_port, required)) = v6_pin {
        let v6_addr: SocketAddr = format!("[::]:{v6_port}")
            .parse()
            .expect("static v6 bind address is valid");
        let opts = iroh::endpoint::BindOpts::default().set_is_required(required);
        builder = builder.bind_addr_with_opts(v6_addr, opts).map_err(|err| {
            TransportError::Unreachable(format!("invalid v6 bind address: {err}"))
        })?;
    }
    builder
        .bind()
        .await
        .map_err(|err| TransportError::Unreachable(format!("cannot bind sync endpoint: {err}")))
}

async fn wait_for_direct_addrs(endpoint: &Endpoint) -> TransportResult<EndpointAddr> {
    let deadline = tokio::time::Instant::now() + ADDR_READY_TIMEOUT;
    loop {
        let addr = endpoint.addr();
        if addr.ip_addrs().next().is_some() {
            return Ok(addr);
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(TransportError::Unreachable(
                "sync endpoint did not learn its direct addresses in time".to_string(),
            ));
        }
        let mut watcher = endpoint.watch_addr();
        let _ = tokio::time::timeout_at(deadline, watcher.updated()).await;
    }
}

fn parse_dial_target(spec: &str) -> TransportResult<(EndpointAddr, EndpointSpec)> {
    let parsed = EndpointSpec::parse(spec).ok_or_else(|| {
        TransportError::Unreachable(format!("not a dialable endpoint spec: {spec}"))
    })?;
    let ticket_str = parsed.dial_ticket().ok_or_else(|| {
        TransportError::Unreachable(format!(
            "a client needs the server's ticket, not a bind spec: {spec}"
        ))
    })?;
    let ticket: EndpointTicket = ticket_str
        .parse()
        .map_err(|_| TransportError::Unreachable(format!("invalid endpoint ticket: {spec}")))?;
    Ok((ticket.endpoint_addr().clone(), parsed))
}

/// Load the operator-supplied relay CA certificate(s): PEM (possibly a
/// bundle) or a single DER certificate.
fn load_relay_ca_certs(
    path: &Path,
) -> TransportResult<Vec<rustls_pki_types::CertificateDer<'static>>> {
    let bytes = std::fs::read(path).map_err(|err| {
        TransportError::Unreachable(format!("cannot read relay-ca file {path:?}: {err}"))
    })?;
    if bytes.starts_with(b"-----BEGIN") {
        use rustls_pki_types::pem::PemObject;
        let certs: Result<Vec<_>, _> =
            rustls_pki_types::CertificateDer::pem_slice_iter(&bytes).collect();
        let certs = certs.map_err(|err| {
            TransportError::Unreachable(format!("invalid PEM in relay-ca file {path:?}: {err}"))
        })?;
        if certs.is_empty() {
            return Err(TransportError::Unreachable(format!(
                "relay-ca file {path:?} contains no certificates"
            )));
        }
        Ok(certs)
    } else {
        Ok(vec![rustls_pki_types::CertificateDer::from(bytes)])
    }
}

/// The dialing endpoint's identity: the ENROLLED key when the spec pins one
/// (fabric-owned identity applies to the dialing side too), else ephemeral —
/// documented as sync-only anonymous dialing for callers with no data root.
fn dialing_identity(identity_path: Option<&Path>) -> TransportResult<FabricIdentity> {
    match identity_path {
        Some(path) => FabricIdentity::load_or_generate(path)
            .map_err(|err| TransportError::Unreachable(err.to_string())),
        None => Ok(FabricIdentity::generate()),
    }
}

/// A ticket that names a relay is explicit operator configuration: the
/// dialing endpoint enables exactly that relay (still no relay by default —
/// LAN tickets carry only direct addresses).
fn relay_choice_for_target(target: &EndpointAddr) -> RelayChoice {
    for addr in &target.addrs {
        if let iroh::TransportAddr::Relay(url) = addr {
            return RelayChoice::SelfHosted(url.to_string());
        }
    }
    RelayChoice::Disabled
}

async fn connect(
    endpoint: &Endpoint,
    target: EndpointAddr,
    alpn: &[u8],
) -> TransportResult<Connection> {
    tokio::time::timeout(CONNECT_TIMEOUT, endpoint.connect(target, alpn))
        .await
        .map_err(|_| {
            TransportError::Unreachable("sync endpoint unreachable: dial timed out".to_string())
        })?
        .map_err(|_| {
            TransportError::Unreachable(
                "sync endpoint unreachable: connection could not be established".to_string(),
            )
        })
}

/// One request/reply exchange on a fresh bi-stream. `subject` is framed for
/// sync-channel requests and omitted for peer-protocol requests.
async fn exchange_frame(
    connection: &Connection,
    subject: Option<&str>,
    payload: &[u8],
) -> TransportResult<Vec<u8>> {
    let (mut send, mut recv) = connection
        .open_bi()
        .await
        .map_err(|_| TransportError::Unreachable("sync endpoint connection lost".to_string()))?;

    if let Some(subject) = subject {
        let subject_bytes = subject.as_bytes();
        let subject_len = u16::try_from(subject_bytes.len())
            .map_err(|_| TransportError::Other("channel name too long".to_string()))?;
        write_all(&mut send, &subject_len.to_be_bytes()).await?;
        write_all(&mut send, subject_bytes).await?;
    }
    let payload_len = u32::try_from(payload.len())
        .map_err(|_| TransportError::Other("request exceeds the frame ceiling".to_string()))?;
    write_all(&mut send, &payload_len.to_be_bytes()).await?;
    write_all(&mut send, payload).await?;
    send.finish()
        .map_err(|_| TransportError::Unreachable("sync endpoint connection lost".to_string()))?;

    let mut status = [0u8; 1];
    recv.read_exact(&mut status).await.map_err(|_| {
        TransportError::Unreachable("sync endpoint closed before replying".to_string())
    })?;
    let reply = read_length_prefixed(&mut recv, MAX_FRAME_BYTES).await?;
    match status[0] {
        REPLY_OK => Ok(reply),
        REPLY_NO_HANDLER => Err(TransportError::NoResponder),
        REPLY_HANDLER_ERROR => Err(TransportError::Status(
            String::from_utf8_lossy(&reply).into_owned(),
        )),
        other => Err(TransportError::Other(format!(
            "unknown reply status byte {other}"
        ))),
    }
}

async fn read_request_frame(
    recv: &mut iroh::endpoint::RecvStream,
) -> TransportResult<(String, Vec<u8>)> {
    let mut subject_len = [0u8; 2];
    recv.read_exact(&mut subject_len)
        .await
        .map_err(|err| TransportError::IncompleteReply(err.to_string()))?;
    let mut subject_bytes = vec![0u8; u16::from_be_bytes(subject_len) as usize];
    recv.read_exact(&mut subject_bytes)
        .await
        .map_err(|err| TransportError::IncompleteReply(err.to_string()))?;
    let subject = String::from_utf8(subject_bytes)
        .map_err(|err| TransportError::IncompleteReply(err.to_string()))?;
    let payload = read_length_prefixed(recv, MAX_FRAME_BYTES).await?;
    Ok((subject, payload))
}

async fn read_length_prefixed(
    recv: &mut iroh::endpoint::RecvStream,
    max_bytes: usize,
) -> TransportResult<Vec<u8>> {
    let mut len_bytes = [0u8; 4];
    recv.read_exact(&mut len_bytes)
        .await
        .map_err(|err| TransportError::IncompleteReply(err.to_string()))?;
    let len = u32::from_be_bytes(len_bytes) as usize;
    if len > max_bytes {
        return Err(TransportError::IncompleteReply(format!(
            "frame of {len} bytes exceeds the {max_bytes}-byte ceiling"
        )));
    }
    let mut payload = vec![0u8; len];
    recv.read_exact(&mut payload)
        .await
        .map_err(|err| TransportError::IncompleteReply(err.to_string()))?;
    Ok(payload)
}

fn sticky_port_path(identity_path: &Path) -> PathBuf {
    let mut file_name = identity_path
        .file_name()
        .map(|name| name.to_os_string())
        .unwrap_or_default();
    file_name.push(".port");
    identity_path.with_file_name(file_name)
}

/// Remembered bound ports: `(v4, optional v6)`. The file carries `P4` or
/// `P4,P6`; legacy single-port files stay readable, and the v6 half is
/// learned and appended on the next bind.
fn read_sticky_ports(path: &Path) -> Option<(u16, Option<u16>)> {
    let raw = std::fs::read_to_string(path).ok()?;
    let mut parts = raw.trim().split(',');
    let v4 = parts.next()?.trim().parse().ok()?;
    let v6 = parts.next().and_then(|part| part.trim().parse().ok());
    Some((v4, v6))
}

/// Render an endpoint id in the fabric's node_id form (lowercase hex of the
/// ed25519 public key) — matches `FabricIdentity::node_id`.
fn hex_node_id(id: &iroh::EndpointId) -> String {
    let mut out = String::with_capacity(64);
    for byte in id.as_bytes() {
        use std::fmt::Write as _;
        let _ = write!(out, "{byte:02x}");
    }
    out
}

async fn write_all(send: &mut iroh::endpoint::SendStream, bytes: &[u8]) -> TransportResult<()> {
    send.write_all(bytes)
        .await
        .map_err(|_| TransportError::Unreachable("sync endpoint connection lost".to_string()))
}

async fn write_reply(
    send: &mut iroh::endpoint::SendStream,
    status: u8,
    payload: &[u8],
) -> TransportResult<()> {
    write_all(send, &[status]).await?;
    let len = u32::try_from(payload.len())
        .map_err(|_| TransportError::Other("reply exceeds the frame ceiling".to_string()))?;
    write_all(send, &len.to_be_bytes()).await?;
    write_all(send, payload).await?;
    send.finish()
        .map_err(|_| TransportError::Unreachable("sync endpoint connection lost".to_string()))?;
    Ok(())
}

#[cfg(test)]
mod sticky_port_tests {
    use super::{EndpointSpec, read_sticky_ports};

    #[test]
    fn reads_legacy_single_port_and_the_port_pair() {
        let dir = std::env::temp_dir().join(format!("wl-sticky-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("tempdir");
        let legacy = dir.join("legacy.port");
        std::fs::write(&legacy, "4433\n").expect("write legacy");
        assert_eq!(read_sticky_ports(&legacy), Some((4433, None)));
        let pair = dir.join("pair.port");
        std::fs::write(&pair, "4433,4501").expect("write pair");
        assert_eq!(read_sticky_ports(&pair), Some((4433, Some(4501))));
        let junk = dir.join("junk.port");
        std::fs::write(&junk, "not-a-port").expect("write junk");
        assert_eq!(read_sticky_ports(&junk), None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn parser_distinguishes_pinned_and_ephemeral_edge_identities() {
        let pinned = EndpointSpec::parse("iroh:?identity=/tmp/edge.key")
            .expect("pinned endpoint spec parses");
        let ephemeral = EndpointSpec::parse("iroh:").expect("ephemeral endpoint spec parses");
        assert!(
            pinned.identity_path().is_some(),
            "a key path pins the edge identity"
        );
        assert!(
            ephemeral.identity_path().is_none(),
            "a dial ticket alone selects the hub, not the edge identity"
        );
    }
}
