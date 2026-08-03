//! Iroh transport adapter: fills the transport seam with dial-by-key
//! networking. A machine is reached by its public key; two machines on one
//! LAN sync with nothing in the middle.
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

#[cfg(test)]
use super::large_request_staging::stage_large_response;
use super::large_request_staging::{
    CONTROL_SUBJECT as LARGE_REQUEST_CONTROL_SUBJECT,
    FRAGMENT_BYTES as LARGE_REQUEST_FRAGMENT_BYTES, LargeRequestBegin, LargeRequestControl,
    LargeRequestControlReply, LargeRequestFragment, LargeRequestProgress, StageOutcome,
    accept_descriptor_fragment as accept_large_request_fragment,
    begin_request as begin_large_request, fragment_count as large_request_fragment_count,
    remove_completed_stage as remove_completed_large_request_stage,
};
use super::large_request_staging::{
    LargeResponseChunk, LargeResponseControl, LargeResponseManifest,
    MAX_RESPONSE_CHUNK_ENVELOPE_BYTES, MAX_RESPONSE_CONTROL_BYTES,
    RESPONSE_CHUNK_BYTES as LARGE_RESPONSE_CHUNK_BYTES,
    RESPONSE_CONTROL_SUBJECT as LARGE_RESPONSE_CONTROL_SUBJECT, abandon_large_response,
    complete_large_response, enforce_response_stage_budget, release_large_response,
    response_chunk as read_large_response_chunk, response_completion_path, response_stage_path,
    stage_large_response_with_budget, validate_large_response_completion,
};
#[cfg(feature = "test-seams")]
use super::large_request_staging::{
    response_completion_receipt_exists, response_stage_counts as large_response_stage_counts,
    snapshot_all_stages as snapshot_all_large_request_stages,
    snapshot_stage as snapshot_large_request_stage,
};
use super::{
    ClientTransport, HandlerRegistration, IncomingRequest, LineageSigner, Responder,
    ServerTransport, TransportError, TransportFuture, TransportResult, TransportStatusFuture,
};
use crate::identity::FabricIdentity;
use iroh::endpoint::{Connection, RecvStream, RelayMode, SendStream};
use iroh::{Endpoint, EndpointAddr, RelayUrl, SecretKey, Watcher};
use iroh_tickets::endpoint::EndpointTicket;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
#[cfg(feature = "production-smoke-driver")]
use std::sync::OnceLock;
#[cfg(feature = "test-seams")]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::sync::{Mutex as AsyncMutex, Notify, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;

/// The sync protocol's ALPN. The trailing version is this adapter's own framing
/// version, distinct from the opaque payload's `PROTOCOL_VERSION`. This adapter
/// moves bytes and does not read the payload, so a payload-version bump does not
/// change the framing and this ALPN does not move — two peers built from this
/// code always agree on it, and a payload-version skew is caught later at the
/// envelope version check.
pub const SYNC_ALPN: &[u8] = b"contextdb.sync.v6";

/// Ceiling on one framed request or reply. Fitting sync requests keep this
/// exact one-frame path. A larger opaque request uses the authenticated
/// durable-fragment path below, while replies remain bounded by this ceiling.
const MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;
/// Safe defaults for the server-local pre-admission resource policy. Operators
/// can declare tighter or wider bounds in the bind endpoint spec.
const DEFAULT_PRE_ADMISSION_CONNECTIONS: usize = 128;
const DEFAULT_PRE_ADMISSION_BYTES: usize = MAX_FRAME_BYTES;
const DEFAULT_REQUEST_READ_IDLE: Duration = Duration::from_secs(30);
/// Bound on establishing one connection (dial + handshake).
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// QUIC idle ceiling: a silent connection is reaped after this. Near iroh's
/// default; paired with [`QUIC_KEEP_ALIVE`] so a wanted-but-idle connection
/// stays alive via keepalive frames and only a dead peer times out.
const QUIC_MAX_IDLE: Duration = Duration::from_secs(30);
/// Keepalive cadence, well under [`QUIC_MAX_IDLE`].
const QUIC_KEEP_ALIVE: Duration = Duration::from_secs(10);
/// A graceful stop waits this long for already admitted work and any
/// authenticated oversized-response continuation to reach a terminal reply.
/// A silent or hostile peer cannot hold process shutdown forever.
const GRACEFUL_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);
/// One terminal reply receipt is bounded independently of connection
/// keepalives, which can otherwise keep a non-responsive peer alive forever.
const REPLY_RECEIPT_TIMEOUT: Duration = Duration::from_secs(30);
/// A published oversized-response manifest remains eligible for resume while
/// its authenticated edge keeps making progress. An edge silent for this long
/// has stalled, so its in-memory drain reservation may be reclaimed.
const RESPONSE_TRANSFER_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
/// Bounds the small in-memory registry before any new oversized response is
/// staged. Each entry represents at least one response larger than 64 MiB, so
/// this ceiling is intentionally far above practical healthy concurrency while
/// still preventing an abandoned-manifest memory leak.
const MAX_TRACKED_RESPONSE_TRANSFERS: usize = 1024;
/// Bound on waiting for the freshly bound endpoint to learn its own direct
/// addresses (needed before a ticket can be minted).
const ADDR_READY_TIMEOUT: Duration = Duration::from_secs(10);
/// How often the serving loop re-checks the shutdown flag.
const SHUTDOWN_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Reply status bytes on the wire.
const REPLY_OK: u8 = 0;
const REPLY_NO_HANDLER: u8 = 1;
const REPLY_HANDLER_ERROR: u8 = 2;
/// An explicitly framed durable oversized-response manifest. This is never
/// inferred from opaque application bytes, which retain `REPLY_OK` exactly.
const REPLY_LARGE_RESPONSE: u8 = 3;
/// An explicit oversized-request progress envelope. Application replies keep
/// `REPLY_OK` even when their bytes happen to resemble a control message.
const REPLY_LARGE_REQUEST_PROGRESS: u8 = 4;
/// Receipt sent on the request half of the same bidirectional stream only
/// after the client has read the complete length-delimited reply.
const REPLY_RECEIVED_ACK: u8 = 0xA5;

/// Two exact crash boundaries used only by the separately built installed
/// release verifier. The feature never ships in an ordinary server build and
/// does not enable `test-seams`.
#[cfg(feature = "production-smoke-driver")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProductionSmokeGateKind {
    ObserveOnly,
    AfterFirstDurableRequestFragment,
    AfterCompletedApplyBeforeReply,
}

#[cfg(feature = "production-smoke-driver")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProductionSmokeCheckpoint {
    RoutesReady,
    PushRequestPath {
        request_digest: [u8; blake3::OUT_LEN],
        chunked: bool,
    },
    DurableRequestFragment {
        transfer_digest: [u8; blake3::OUT_LEN],
        sequence: u32,
        next_missing: u32,
    },
    CompletedApplyBeforeReply {
        request_digest: [u8; blake3::OUT_LEN],
        authenticated_node_id: String,
        source_lsn: u64,
        hub_lsn: u64,
        dependency_complete: bool,
    },
}

#[cfg(feature = "production-smoke-driver")]
struct ProductionSmokeGate {
    kind: ProductionSmokeGateKind,
    checkpoints: std::sync::mpsc::Sender<ProductionSmokeCheckpoint>,
    fired: AtomicBool,
}

#[cfg(feature = "production-smoke-driver")]
static PRODUCTION_SMOKE_GATE: OnceLock<ProductionSmokeGate> = OnceLock::new();

/// Arm one fixed production-path checkpoint for the installed verifier. The
/// caller receives read-only checkpoint facts; the transport blocks forever
/// only at the selected boundary so the external harness can kill this exact
/// process. There is deliberately no in-process resume, reset, payload, or
/// mutation control.
#[cfg(feature = "production-smoke-driver")]
pub fn arm_production_smoke_gate(
    kind: ProductionSmokeGateKind,
) -> std::result::Result<std::sync::mpsc::Receiver<ProductionSmokeCheckpoint>, &'static str> {
    let (checkpoints, receiver) = std::sync::mpsc::channel();
    PRODUCTION_SMOKE_GATE
        .set(ProductionSmokeGate {
            kind,
            checkpoints,
            fired: AtomicBool::new(false),
        })
        .map_err(|_| "production smoke gate is already armed in this process")?;
    Ok(receiver)
}

#[cfg(feature = "production-smoke-driver")]
fn production_smoke_routes_ready() {
    if let Some(gate) = PRODUCTION_SMOKE_GATE.get() {
        let _ = gate
            .checkpoints
            .send(ProductionSmokeCheckpoint::RoutesReady);
    }
}

#[cfg(feature = "production-smoke-driver")]
fn production_smoke_push_request_path(
    subject: &str,
    request_digest: [u8; blake3::OUT_LEN],
    chunked: bool,
) {
    let Some(gate) = PRODUCTION_SMOKE_GATE.get() else {
        return;
    };
    if gate.kind == ProductionSmokeGateKind::ObserveOnly
        && subject.starts_with("sync.")
        && subject.ends_with(".push")
    {
        let _ = gate
            .checkpoints
            .send(ProductionSmokeCheckpoint::PushRequestPath {
                request_digest,
                chunked,
            });
    }
}

#[cfg(feature = "production-smoke-driver")]
fn production_smoke_block(
    expected: ProductionSmokeGateKind,
    checkpoint: ProductionSmokeCheckpoint,
) {
    let Some(gate) = PRODUCTION_SMOKE_GATE.get() else {
        return;
    };
    if gate.kind != expected || gate.fired.swap(true, Ordering::SeqCst) {
        return;
    }
    if gate.checkpoints.send(checkpoint).is_err() {
        return;
    }
    loop {
        std::thread::park();
    }
}

/// Pause the separately built installed verifier only after the ordinary
/// authenticated push path has committed both the dependency-complete unit
/// and its durable receipt, but before any success reply is handed back to
/// the transport. Keeping this signal at the sync boundary prevents a failed
/// apply response from masquerading as the lost-final-ack state.
#[cfg(feature = "production-smoke-driver")]
pub(crate) fn production_smoke_completed_apply_before_reply(
    request_digest: [u8; blake3::OUT_LEN],
    authenticated_node_id: String,
    source_lsn: u64,
    hub_lsn: u64,
    dependency_complete: bool,
) {
    production_smoke_block(
        ProductionSmokeGateKind::AfterCompletedApplyBeforeReply,
        ProductionSmokeCheckpoint::CompletedApplyBeforeReply {
            request_digest,
            authenticated_node_id,
            source_lsn,
            hub_lsn,
            dependency_complete,
        },
    );
}

enum FrameReply {
    Payload(Vec<u8>),
    LargeResponseManifest(Vec<u8>),
    LargeRequestProgress(Vec<u8>),
}

enum LargeResponseControlReply {
    Chunk(LargeResponseChunk),
    CompleteAck(Vec<u8>),
    ReleaseAck(Vec<u8>),
}

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
/// Server (bind) form: `iroh:?identity=<key-file>[&port=<u16>][&relay=<none|n0|url>][&response-staging-bytes=<positive-u64>][&pre-admission-connections=<positive-usize>][&pre-admission-bytes=<positive-u32>][&request-read-idle-ms=<positive-u64>]`
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
    response_staging_bytes: Option<u64>,
    pre_admission_connections: Option<usize>,
    pre_admission_bytes: Option<usize>,
    request_read_idle_ms: Option<u64>,
    dial_ticket: Option<String>,
}

// Grammar note: the dial form may pin the edge's own fabric identity —
// `iroh:?to=<ticket>&identity=<key-file>` — so the dialing endpoint
// originates from the enrolled identity instead of an ephemeral key.

impl EndpointSpec {
    /// Parse a spec string. Returns `None` when the string is not Iroh-shaped
    /// or is malformed; use [`EndpointSpec::parse_detailed`] to distinguish
    /// those cases.
    pub fn parse(spec: &str) -> Option<Self> {
        Self::parse_detailed(spec).ok().flatten()
    }

    /// Three-way parse: `Ok(Some)` = a valid Iroh spec; `Ok(None)` = not
    /// Iroh-shaped; `Err` = Iroh-shaped but invalid. An `iroh:`-prefixed
    /// string with a typo always errors loudly.
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
                None => Err(
                    "not a valid enrollment ticket after `iroh:` in sync endpoint spec".to_string(),
                ),
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
            response_staging_bytes: None,
            pre_admission_connections: None,
            pre_admission_bytes: None,
            request_read_idle_ms: None,
            dial_ticket: None,
        }
    }

    fn parse_bind_query(query: &str) -> Result<Self, String> {
        let mut spec = Self::bind_defaults();
        for (position, pair) in query.split('&').filter(|pair| !pair.is_empty()).enumerate() {
            let position = position + 1;
            let Some((key, value)) = pair.split_once('=') else {
                return Err(format!(
                    "malformed parameter at position {position} in sync endpoint spec (expected key=value)"
                ));
            };
            match key {
                "identity" => spec.identity_path = Some(PathBuf::from(value)),
                "port" => {
                    spec.port = Some(value.parse().map_err(|_| {
                        "invalid value for `port` in sync endpoint spec".to_string()
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
                "response-staging-bytes" => {
                    let bytes: u64 = value.parse().map_err(|_| {
                        "invalid value for `response-staging-bytes` in sync endpoint spec (expected a positive byte count)".to_string()
                    })?;
                    if bytes == 0 {
                        return Err("invalid value for `response-staging-bytes` in sync endpoint spec (expected a positive byte count)".to_string());
                    }
                    spec.response_staging_bytes = Some(bytes);
                }
                "pre-admission-connections" => {
                    let connections: usize = value.parse().map_err(|_| {
                        "invalid value for `pre-admission-connections` in sync endpoint spec (expected a positive connection count)".to_string()
                    })?;
                    if connections == 0 || connections > Semaphore::MAX_PERMITS {
                        return Err("invalid value for `pre-admission-connections` in sync endpoint spec (expected a positive connection count)".to_string());
                    }
                    spec.pre_admission_connections = Some(connections);
                }
                "pre-admission-bytes" => {
                    let bytes: usize = value.parse().map_err(|_| {
                        "invalid value for `pre-admission-bytes` in sync endpoint spec (expected a positive byte count no larger than u32::MAX)".to_string()
                    })?;
                    if bytes == 0 || u32::try_from(bytes).is_err() {
                        return Err("invalid value for `pre-admission-bytes` in sync endpoint spec (expected a positive byte count no larger than u32::MAX)".to_string());
                    }
                    spec.pre_admission_bytes = Some(bytes);
                }
                "request-read-idle-ms" => {
                    let millis: u64 = value.parse().map_err(|_| {
                        "invalid value for `request-read-idle-ms` in sync endpoint spec (expected a positive millisecond count)".to_string()
                    })?;
                    if millis == 0 {
                        return Err("invalid value for `request-read-idle-ms` in sync endpoint spec (expected a positive millisecond count)".to_string());
                    }
                    spec.request_read_idle_ms = Some(millis);
                }
                _ => {
                    return Err(format!(
                        "unknown parameter at position {position} in sync endpoint spec (accepted: identity, port, relay, relay-ca, publish, lookup, response-staging-bytes, pre-admission-connections, pre-admission-bytes, request-read-idle-ms, to)"
                    ));
                }
            }
        }
        if spec.dial_ticket.is_some()
            && (spec.response_staging_bytes.is_some()
                || spec.pre_admission_connections.is_some()
                || spec.pre_admission_bytes.is_some()
                || spec.request_read_idle_ms.is_some())
        {
            return Err("server-local resource policy cannot be used with to=".to_string());
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
            response_staging_bytes: None,
            pre_admission_connections: None,
            pre_admission_bytes: None,
            request_read_idle_ms: None,
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

    /// Optional server-local durable oversized-response storage budget. It is
    /// not carried in enrollment tickets and defaults to no pressure eviction.
    pub fn response_staging_bytes(&self) -> Option<u64> {
        self.response_staging_bytes
    }

    /// Maximum simultaneous incoming connection/handshake tasks. This is a
    /// server-local resource policy and is never carried in enrollment tickets.
    pub fn pre_admission_connections(&self) -> usize {
        self.pre_admission_connections
            .unwrap_or(DEFAULT_PRE_ADMISSION_CONNECTIONS)
    }

    /// Maximum aggregate bytes reserved by sync request frames before they
    /// have reached route admission.
    pub fn pre_admission_bytes(&self) -> usize {
        self.pre_admission_bytes
            .unwrap_or(DEFAULT_PRE_ADMISSION_BYTES)
    }

    /// Maximum time a request-frame read may make no application-byte progress.
    pub fn request_read_idle(&self) -> Duration {
        Duration::from_millis(
            self.request_read_idle_ms
                .unwrap_or(DEFAULT_REQUEST_READ_IDLE.as_millis() as u64),
        )
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
type SyncRoutes = Arc<SyncRouteState>;
type ResponseTransferKey = (String, [u8; blake3::OUT_LEN]);

struct TrackedResponseTransfer {
    count: usize,
    last_activity: Instant,
    paths: ResponseTransferPaths,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ResponseTransferPaths {
    stage: PathBuf,
    completion: PathBuf,
}

#[derive(Default)]
struct SyncRouteLifecycle {
    routes: Option<Arc<HashMap<String, super::RequestHandler>>>,
    active_requests: usize,
    reserved_response_transfers: usize,
    tracked_response_transfers: usize,
    response_transfers: HashMap<ResponseTransferKey, TrackedResponseTransfer>,
    draining_response_transfers: HashMap<ResponseTransferKey, usize>,
    reply_streams: Vec<Weak<AsyncMutex<Option<(SendStream, RecvStream)>>>>,
}

#[derive(Default)]
struct SyncRouteState {
    lifecycle: Mutex<SyncRouteLifecycle>,
    idle: Notify,
    reply_owners_cancelled: AtomicBool,
    reply_owner_cancelled: Notify,
    #[cfg(feature = "test-seams")]
    successful_reply_receipts: AtomicUsize,
    #[cfg(feature = "test-seams")]
    terminal_reply_receipt_failures: AtomicUsize,
}

impl SyncRouteState {
    fn begin_serving(&self, routes: Arc<HashMap<String, super::RequestHandler>>) {
        let mut lifecycle = self.lifecycle.lock().unwrap_or_else(|err| err.into_inner());
        self.reply_owners_cancelled.store(false, Ordering::SeqCst);
        lifecycle.routes = Some(routes);
        lifecycle.reserved_response_transfers = 0;
        lifecycle.tracked_response_transfers = 0;
        lifecycle.response_transfers.clear();
        lifecycle.draining_response_transfers.clear();
        lifecycle.reply_streams.clear();
    }

    fn admit(
        self: &Arc<Self>,
    ) -> Option<(
        Arc<HashMap<String, super::RequestHandler>>,
        ActiveSyncRequest,
    )> {
        let mut lifecycle = self.lifecycle.lock().unwrap_or_else(|err| err.into_inner());
        let routes = lifecycle.routes.clone()?;
        lifecycle.active_requests += 1;
        Some((
            routes,
            ActiveSyncRequest {
                state: self.clone(),
            },
        ))
    }

    fn stop_accepting(&self) {
        let mut lifecycle = self.lifecycle.lock().unwrap_or_else(|err| err.into_inner());
        Self::prune_idle_response_transfers(&mut lifecycle, Instant::now());
        lifecycle.routes = None;
        lifecycle.draining_response_transfers = lifecycle
            .response_transfers
            .iter()
            .map(|(key, transfer)| (key.clone(), transfer.count))
            .collect();
    }

    fn register_reply_stream(
        &self,
        slot: &Arc<AsyncMutex<Option<(SendStream, RecvStream)>>>,
    ) -> bool {
        let mut lifecycle = self.lifecycle.lock().unwrap_or_else(|err| err.into_inner());
        if self.reply_owners_cancelled.load(Ordering::SeqCst) {
            return false;
        }
        lifecycle
            .reply_streams
            .retain(|stream| stream.strong_count() > 0);
        lifecycle.reply_streams.push(Arc::downgrade(slot));
        true
    }

    async fn cancel_reply_owners(&self) {
        self.reply_owners_cancelled.store(true, Ordering::SeqCst);
        self.reply_owner_cancelled.notify_waiters();
        let slots = {
            let mut lifecycle = self.lifecycle.lock().unwrap_or_else(|err| err.into_inner());
            lifecycle
                .reply_streams
                .drain(..)
                .filter_map(|stream| stream.upgrade())
                .collect::<Vec<_>>()
        };
        for slot in slots {
            drop(slot.lock().await.take());
        }
    }

    async fn reply_owner_cancelled(&self) {
        loop {
            let cancelled = self.reply_owner_cancelled.notified();
            tokio::pin!(cancelled);
            cancelled.as_mut().enable();
            if self.reply_owners_cancelled.load(Ordering::SeqCst) {
                return;
            }
            cancelled.as_mut().await;
        }
    }

    fn reserve_response_transfer(self: &Arc<Self>) -> Option<ResponseTransferReservation> {
        let mut lifecycle = self.lifecycle.lock().unwrap_or_else(|err| err.into_inner());
        Self::prune_idle_response_transfers(&mut lifecycle, Instant::now());
        if lifecycle.tracked_response_transfers >= MAX_TRACKED_RESPONSE_TRANSFERS {
            return None;
        }
        lifecycle.reserved_response_transfers += 1;
        lifecycle.tracked_response_transfers += 1;
        Some(ResponseTransferReservation {
            state: self.clone(),
            active: true,
        })
    }

    fn register_reserved_response_transfer(
        &self,
        key: ResponseTransferKey,
        paths: ResponseTransferPaths,
    ) {
        let mut lifecycle = self.lifecycle.lock().unwrap_or_else(|err| err.into_inner());
        debug_assert!(lifecycle.reserved_response_transfers > 0);
        lifecycle.reserved_response_transfers =
            lifecycle.reserved_response_transfers.saturating_sub(1);
        let transfer =
            lifecycle
                .response_transfers
                .entry(key.clone())
                .or_insert(TrackedResponseTransfer {
                    count: 0,
                    last_activity: Instant::now(),
                    paths: paths.clone(),
                });
        debug_assert_eq!(transfer.paths, paths);
        transfer.count += 1;
        transfer.last_activity = Instant::now();
        if lifecycle.routes.is_none() {
            *lifecycle
                .draining_response_transfers
                .entry(key)
                .or_default() += 1;
        }
    }

    fn ensure_response_transfer(
        &self,
        key: &ResponseTransferKey,
        paths: ResponseTransferPaths,
    ) -> bool {
        let mut lifecycle = self.lifecycle.lock().unwrap_or_else(|err| err.into_inner());
        if let Some(transfer) = lifecycle.response_transfers.get_mut(key) {
            debug_assert_eq!(transfer.paths, paths);
            transfer.last_activity = Instant::now();
            return true;
        }
        if lifecycle.routes.is_some() {
            Self::prune_idle_response_transfers(&mut lifecycle, Instant::now());
        }
        if lifecycle.tracked_response_transfers >= MAX_TRACKED_RESPONSE_TRANSFERS {
            return false;
        }
        lifecycle.tracked_response_transfers += 1;
        lifecycle.response_transfers.insert(
            key.clone(),
            TrackedResponseTransfer {
                count: 1,
                last_activity: Instant::now(),
                paths,
            },
        );
        if lifecycle.routes.is_none() {
            lifecycle.draining_response_transfers.insert(key.clone(), 1);
        }
        true
    }

    fn protected_response_paths(&self) -> Vec<PathBuf> {
        let lifecycle = self.lifecycle.lock().unwrap_or_else(|err| err.into_inner());
        lifecycle
            .response_transfers
            .values()
            .flat_map(|transfer| {
                [
                    transfer.paths.stage.clone(),
                    transfer.paths.completion.clone(),
                ]
            })
            .collect()
    }

    fn admit_response_continuation(
        self: &Arc<Self>,
        key: &ResponseTransferKey,
    ) -> Option<ActiveSyncRequest> {
        let mut lifecycle = self.lifecycle.lock().unwrap_or_else(|err| err.into_inner());
        if !lifecycle.draining_response_transfers.contains_key(key) {
            return None;
        }
        if let Some(transfer) = lifecycle.response_transfers.get_mut(key) {
            transfer.last_activity = Instant::now();
        }
        lifecycle.active_requests += 1;
        Some(ActiveSyncRequest {
            state: self.clone(),
        })
    }

    fn complete_response_transfer(&self, key: &ResponseTransferKey) -> bool {
        let (became_idle, removed_final_reference) = {
            let mut lifecycle = self.lifecycle.lock().unwrap_or_else(|err| err.into_inner());
            let completed = lifecycle.response_transfers.get_mut(key).map(|transfer| {
                transfer.count = transfer.count.saturating_sub(1);
                transfer.count == 0
            });
            if let Some(remove_transfer) = completed {
                lifecycle.tracked_response_transfers =
                    lifecycle.tracked_response_transfers.saturating_sub(1);
                if remove_transfer {
                    lifecycle.response_transfers.remove(key);
                }
            }
            if let Some(count) = lifecycle.draining_response_transfers.get_mut(key) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    lifecycle.draining_response_transfers.remove(key);
                }
            }
            (
                lifecycle.active_requests == 0 && lifecycle.draining_response_transfers.is_empty(),
                completed == Some(true),
            )
        };
        if became_idle {
            self.idle.notify_waiters();
        }
        removed_final_reference
    }

    fn abandon_response_transfers(&self) {
        let became_idle = {
            let mut lifecycle = self.lifecycle.lock().unwrap_or_else(|err| err.into_inner());
            lifecycle.response_transfers.clear();
            lifecycle.draining_response_transfers.clear();
            lifecycle.tracked_response_transfers = lifecycle.reserved_response_transfers;
            lifecycle.active_requests == 0
        };
        if became_idle {
            self.idle.notify_waiters();
        }
    }

    async fn wait_idle(&self) {
        loop {
            let notified = self.idle.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let is_idle = {
                let lifecycle = self.lifecycle.lock().unwrap_or_else(|err| err.into_inner());
                lifecycle.active_requests == 0 && lifecycle.draining_response_transfers.is_empty()
            };
            if is_idle {
                return;
            }
            notified.as_mut().await;
        }
    }

    #[cfg(feature = "test-seams")]
    fn record_reply_receipt_for_test(&self, consumed: bool) {
        let counter = if consumed {
            &self.successful_reply_receipts
        } else {
            &self.terminal_reply_receipt_failures
        };
        counter.fetch_add(1, Ordering::SeqCst);
    }

    fn prune_idle_response_transfers(lifecycle: &mut SyncRouteLifecycle, now: Instant) {
        let before = lifecycle
            .response_transfers
            .values()
            .map(|transfer| transfer.count)
            .sum::<usize>();
        lifecycle.response_transfers.retain(|_, transfer| {
            now.saturating_duration_since(transfer.last_activity) < RESPONSE_TRANSFER_IDLE_TIMEOUT
        });
        let after = lifecycle
            .response_transfers
            .values()
            .map(|transfer| transfer.count)
            .sum::<usize>();
        lifecycle.tracked_response_transfers = lifecycle
            .tracked_response_transfers
            .saturating_sub(before.saturating_sub(after));
    }
}

struct ResponseTransferReservation {
    state: SyncRoutes,
    active: bool,
}

impl ResponseTransferReservation {
    fn activate(mut self, key: ResponseTransferKey, paths: ResponseTransferPaths) {
        self.state.register_reserved_response_transfer(key, paths);
        self.active = false;
    }
}

impl Drop for ResponseTransferReservation {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let mut lifecycle = self
            .state
            .lifecycle
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        lifecycle.reserved_response_transfers =
            lifecycle.reserved_response_transfers.saturating_sub(1);
        lifecycle.tracked_response_transfers =
            lifecycle.tracked_response_transfers.saturating_sub(1);
    }
}

struct ActiveSyncRequest {
    state: SyncRoutes,
}

impl Drop for ActiveSyncRequest {
    fn drop(&mut self) {
        let became_idle = {
            let mut lifecycle = self
                .state
                .lifecycle
                .lock()
                .unwrap_or_else(|err| err.into_inner());
            debug_assert!(lifecycle.active_requests > 0);
            lifecycle.active_requests = lifecycle.active_requests.saturating_sub(1);
            lifecycle.active_requests == 0 && lifecycle.draining_response_transfers.is_empty()
        };
        if became_idle {
            self.state.idle.notify_waiters();
        }
    }
}

fn mark_test_shutdown_quiesced(sync_routes: &SyncRouteState) {
    let Some(path) = std::env::var_os("CONTEXTDB_TEST_SHUTDOWN_QUIESCED_FILE") else {
        return;
    };
    let lifecycle = sync_routes
        .lifecycle
        .lock()
        .unwrap_or_else(|err| err.into_inner());
    let marker = format!(
        "sync-admission-closed active_requests={} draining_transfers={}",
        lifecycle.active_requests,
        lifecycle
            .draining_response_transfers
            .values()
            .sum::<usize>()
    );
    let _ = std::fs::write(path, marker);
}

fn append_test_shutdown_state(sync_routes: &SyncRouteState, event: &str) {
    use std::io::Write as _;

    let Some(path) = std::env::var_os("CONTEXTDB_TEST_SHUTDOWN_QUIESCED_FILE") else {
        return;
    };
    let lifecycle = sync_routes
        .lifecycle
        .lock()
        .unwrap_or_else(|err| err.into_inner());
    let Ok(mut marker) = std::fs::OpenOptions::new().append(true).open(path) else {
        return;
    };
    let _ = write!(
        marker,
        "\n{event} active_requests={} draining_transfers={}",
        lifecycle.active_requests,
        lifecycle
            .draining_response_transfers
            .values()
            .sum::<usize>()
    );
}

#[derive(Clone)]
struct DurableLargeRequestStage {
    root: PathBuf,
    lock: Arc<tokio::sync::Mutex<()>>,
    response_staging_budget: Option<u64>,
    #[cfg(feature = "test-seams")]
    control: Arc<LargeRequestTestControlState>,
}

/// Read-only inspection of one durable oversized request. This type exists
/// only in the crate's test-seam build and contains no path or mutation API.
#[cfg(feature = "test-seams")]
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LargeRequestStageSnapshot {
    pub subject: String,
    pub authenticated_node_id: String,
    pub unit_digest: [u8; blake3::OUT_LEN],
    pub total_bytes: u64,
    pub total_fragments: u32,
    pub fragments: Vec<LargeRequestFragmentSnapshot>,
}

#[cfg(feature = "test-seams")]
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LargeRequestFragmentSnapshot {
    pub sequence: u32,
    pub bytes: u64,
    pub digest: [u8; blake3::OUT_LEN],
}

/// Read-only observations from the real authenticated oversized-request path.
/// These exist only in test-seam builds so a transport regression can prove
/// that reconciliation did not send the completed request twice.
#[cfg(feature = "test-seams")]
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LargeRequestTestObservations {
    pub completed_handler_dispatches: usize,
    pub injected_reply_resets: usize,
    pub authenticated_status_probes_after_reset: usize,
    pub accepted_fragment_sequences: Vec<LargeRequestFragmentSequence>,
    pub authenticated_pull_reply_bytes_after_reset: Vec<usize>,
    pub staged_response_manifests: Vec<LargeResponseManifestObservation>,
    pub requested_response_chunks: Vec<LargeResponseChunkSequence>,
    pub served_response_chunks: Vec<LargeResponseChunkSequence>,
    pub completed_response_transfers: Vec<LargeResponseLifecycleObservation>,
    pub released_response_transfers: Vec<LargeResponseLifecycleObservation>,
    pub durable_response_complete_outcomes: Vec<LargeResponseCompleteOutcomeObservation>,
    pub response_complete_controls_received: usize,
    pub successful_response_complete_ack_writes: usize,
    pub successful_reply_receipts: usize,
    pub terminal_reply_receipt_failures: usize,
    pub injected_pre_durable_complete_resets: usize,
    pub injected_post_durable_complete_ack_resets: usize,
}

/// Read-only identity and size data from a real staged oversized response.
#[cfg(feature = "test-seams")]
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LargeResponseManifestObservation {
    pub transfer_digest: [u8; blake3::OUT_LEN],
    pub authenticated_node_id: String,
    pub subject: String,
    pub request_digest: [u8; blake3::OUT_LEN],
    pub response_digest: [u8; blake3::OUT_LEN],
    pub total_bytes: u64,
    pub total_chunks: u64,
}

/// One authenticated durable-response chunk control seen on the live Iroh
/// connection. A response chunk sequence is `u64` by protocol contract.
#[cfg(feature = "test-seams")]
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LargeResponseChunkSequence {
    pub transfer_digest: [u8; blake3::OUT_LEN],
    pub authenticated_node_id: String,
    pub sequence: u64,
    pub total_chunks: u64,
}

/// A successful durable response lifecycle control observed on the live Iroh
/// connection.
#[cfg(feature = "test-seams")]
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LargeResponseLifecycleObservation {
    pub transfer_digest: [u8; blake3::OUT_LEN],
    pub authenticated_node_id: String,
}

/// Read-only counts of durable response stages and completion receipts.
#[cfg(feature = "test-seams")]
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LargeResponseStageCounts {
    pub stages: usize,
    pub receipts: usize,
}

#[cfg(feature = "test-seams")]
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LargeResponseCompleteOutcomeObservation {
    pub transfer_digest: [u8; blake3::OUT_LEN],
    pub authenticated_node_id: String,
    pub receipt_preexisted: bool,
}

/// Test-only control of the real client retry path for a durable response
/// chunk. It is compiled out of production builds.
#[cfg(feature = "test-seams")]
#[doc(hidden)]
#[derive(Clone)]
pub struct IrohClientTestController {
    control: Arc<IrohClientTestControlState>,
}

#[cfg(feature = "test-seams")]
struct IrohClientTestControlState {
    pause_after_response_chunk_failure: Mutex<Option<u64>>,
    response_chunk_retry_paused: AtomicBool,
    response_chunk_retry_released: AtomicBool,
    response_chunk_retry_paused_notice: Notify,
    response_chunk_retry_resume_notice: Notify,
}

#[cfg(feature = "test-seams")]
impl Default for IrohClientTestControlState {
    fn default() -> Self {
        Self {
            pause_after_response_chunk_failure: Mutex::new(None),
            response_chunk_retry_paused: AtomicBool::new(false),
            response_chunk_retry_released: AtomicBool::new(false),
            response_chunk_retry_paused_notice: Notify::new(),
            response_chunk_retry_resume_notice: Notify::new(),
        }
    }
}

#[cfg(feature = "test-seams")]
impl IrohClientTestController {
    pub fn pause_after_response_chunk_failure_before_retry_for_test(&self, sequence: u64) {
        *self
            .control
            .pause_after_response_chunk_failure
            .lock()
            .unwrap_or_else(|err| err.into_inner()) = Some(sequence);
        self.control
            .response_chunk_retry_paused
            .store(false, Ordering::SeqCst);
        self.control
            .response_chunk_retry_released
            .store(false, Ordering::SeqCst);
    }

    pub async fn wait_until_response_chunk_retry_paused_for_test(&self) {
        loop {
            let notice = self.control.response_chunk_retry_paused_notice.notified();
            tokio::pin!(notice);
            notice.as_mut().enable();
            if self
                .control
                .response_chunk_retry_paused
                .load(Ordering::SeqCst)
            {
                return;
            }
            notice.await;
        }
    }

    pub fn resume_response_chunk_retry_for_test(&self) {
        self.control
            .response_chunk_retry_released
            .store(true, Ordering::SeqCst);
        self.control
            .response_chunk_retry_resume_notice
            .notify_waiters();
    }
}

#[cfg(feature = "test-seams")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResponseControlGate {
    ChunkBeforeReply(u64),
    CompletePreDurable,
    CompletePostDurableAck,
}

#[cfg(feature = "test-seams")]
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LargeRequestFragmentSequence {
    pub unit_digest: [u8; blake3::OUT_LEN],
    pub sequence: u32,
    pub total_fragments: u32,
    pub total_bytes: u64,
}

#[cfg(feature = "test-seams")]
struct LargeRequestTestControlState {
    pause_after_sequence: Mutex<Option<u32>>,
    paused: AtomicBool,
    released: AtomicBool,
    paused_notice: Notify,
    resume_notice: Notify,
    drop_next_complete_reply: AtomicBool,
    completed_handler_dispatches: AtomicUsize,
    injected_reply_resets: AtomicUsize,
    reply_reset_injected: AtomicBool,
    authenticated_status_probes_after_reset: AtomicUsize,
    accepted_fragment_sequences: Mutex<Vec<LargeRequestFragmentSequence>>,
    authenticated_pull_reply_bytes_after_reset: Mutex<Vec<usize>>,
    staged_response_manifests: Mutex<Vec<LargeResponseManifestObservation>>,
    staged_response_manifest_notice: Notify,
    requested_response_chunks: Mutex<Vec<LargeResponseChunkSequence>>,
    served_response_chunks: Mutex<Vec<LargeResponseChunkSequence>>,
    completed_response_transfers: Mutex<Vec<LargeResponseLifecycleObservation>>,
    released_response_transfers: Mutex<Vec<LargeResponseLifecycleObservation>>,
    durable_response_complete_outcomes: Mutex<Vec<LargeResponseCompleteOutcomeObservation>>,
    response_complete_controls_received: AtomicUsize,
    successful_response_complete_ack_writes: AtomicUsize,
    injected_pre_durable_complete_resets: AtomicUsize,
    injected_post_durable_complete_ack_resets: AtomicUsize,
    response_control_gate: Mutex<Option<ResponseControlGate>>,
    response_control_paused: AtomicBool,
    response_control_reset: AtomicBool,
    response_control_paused_notice: Notify,
    response_control_resume_notice: Notify,
    routes_ready: AtomicBool,
    routes_ready_notice: Notify,
    shutdown_admission_closed: AtomicBool,
    shutdown_admission_closed_notice: Notify,
    force_graceful_drain_timeout: AtomicBool,
}

#[cfg(feature = "test-seams")]
impl Default for LargeRequestTestControlState {
    fn default() -> Self {
        Self {
            pause_after_sequence: Mutex::new(None),
            paused: AtomicBool::new(false),
            released: AtomicBool::new(false),
            paused_notice: Notify::new(),
            resume_notice: Notify::new(),
            drop_next_complete_reply: AtomicBool::new(false),
            completed_handler_dispatches: AtomicUsize::new(0),
            injected_reply_resets: AtomicUsize::new(0),
            reply_reset_injected: AtomicBool::new(false),
            authenticated_status_probes_after_reset: AtomicUsize::new(0),
            accepted_fragment_sequences: Mutex::new(Vec::new()),
            authenticated_pull_reply_bytes_after_reset: Mutex::new(Vec::new()),
            staged_response_manifests: Mutex::new(Vec::new()),
            staged_response_manifest_notice: Notify::new(),
            requested_response_chunks: Mutex::new(Vec::new()),
            served_response_chunks: Mutex::new(Vec::new()),
            completed_response_transfers: Mutex::new(Vec::new()),
            released_response_transfers: Mutex::new(Vec::new()),
            durable_response_complete_outcomes: Mutex::new(Vec::new()),
            response_complete_controls_received: AtomicUsize::new(0),
            successful_response_complete_ack_writes: AtomicUsize::new(0),
            injected_pre_durable_complete_resets: AtomicUsize::new(0),
            injected_post_durable_complete_ack_resets: AtomicUsize::new(0),
            response_control_gate: Mutex::new(None),
            response_control_paused: AtomicBool::new(false),
            response_control_reset: AtomicBool::new(false),
            response_control_paused_notice: Notify::new(),
            response_control_resume_notice: Notify::new(),
            routes_ready: AtomicBool::new(false),
            routes_ready_notice: Notify::new(),
            shutdown_admission_closed: AtomicBool::new(false),
            shutdown_admission_closed_notice: Notify::new(),
            force_graceful_drain_timeout: AtomicBool::new(false),
        }
    }
}

/// Test-only controls for the real Iroh serving path. They are absent from
/// default builds and are not connected to endpoint configuration.
#[cfg(feature = "test-seams")]
#[doc(hidden)]
#[derive(Clone)]
pub struct LargeRequestTestController {
    stage: DurableLargeRequestStage,
    sync_routes: SyncRoutes,
}

#[cfg(feature = "test-seams")]
impl LargeRequestTestController {
    pub fn pause_after_persisted_fragment_for_test(&self, sequence: u32) {
        *self
            .stage
            .control
            .pause_after_sequence
            .lock()
            .unwrap_or_else(|err| err.into_inner()) = Some(sequence);
        self.stage.control.paused.store(false, Ordering::SeqCst);
        self.stage.control.released.store(false, Ordering::SeqCst);
    }

    pub async fn wait_until_paused_for_test(&self) {
        loop {
            let notice = self.stage.control.paused_notice.notified();
            tokio::pin!(notice);
            notice.as_mut().enable();
            if self.stage.control.paused.load(Ordering::SeqCst) {
                return;
            }
            notice.await;
        }
    }

    pub fn resume_for_test(&self) {
        self.stage.control.released.store(true, Ordering::SeqCst);
        self.stage.control.resume_notice.notify_waiters();
    }

    pub fn drop_next_completed_reply_for_test(&self) {
        self.stage
            .control
            .drop_next_complete_reply
            .store(true, Ordering::SeqCst);
    }

    pub fn pause_before_serving_response_chunk_for_test(&self, sequence: u64) {
        self.arm_response_control_gate_for_test(ResponseControlGate::ChunkBeforeReply(sequence));
    }

    pub fn pause_before_durable_response_completion_for_test(&self) {
        self.arm_response_control_gate_for_test(ResponseControlGate::CompletePreDurable);
    }

    pub fn pause_after_durable_response_completion_before_ack_for_test(&self) {
        self.arm_response_control_gate_for_test(ResponseControlGate::CompletePostDurableAck);
    }

    fn arm_response_control_gate_for_test(&self, gate: ResponseControlGate) {
        *self
            .stage
            .control
            .response_control_gate
            .lock()
            .unwrap_or_else(|err| err.into_inner()) = Some(gate);
        self.stage
            .control
            .response_control_paused
            .store(false, Ordering::SeqCst);
        self.stage
            .control
            .response_control_reset
            .store(false, Ordering::SeqCst);
    }

    pub async fn wait_until_response_control_paused_for_test(&self) {
        loop {
            let notice = self.stage.control.response_control_paused_notice.notified();
            tokio::pin!(notice);
            notice.as_mut().enable();
            if self
                .stage
                .control
                .response_control_paused
                .load(Ordering::SeqCst)
            {
                return;
            }
            notice.await;
        }
    }

    pub async fn wait_until_staged_response_manifests_for_test(&self, expected: usize) {
        loop {
            let notice = self
                .stage
                .control
                .staged_response_manifest_notice
                .notified();
            tokio::pin!(notice);
            notice.as_mut().enable();
            if self
                .stage
                .control
                .staged_response_manifests
                .lock()
                .unwrap_or_else(|err| err.into_inner())
                .len()
                >= expected
            {
                return;
            }
            notice.await;
        }
    }

    pub fn reset_paused_response_control_for_test(&self) {
        self.stage
            .control
            .response_control_reset
            .store(true, Ordering::SeqCst);
        self.stage
            .control
            .response_control_resume_notice
            .notify_waiters();
    }

    pub async fn wait_until_routes_ready_for_test(&self) {
        loop {
            let notice = self.stage.control.routes_ready_notice.notified();
            tokio::pin!(notice);
            notice.as_mut().enable();
            if self.stage.control.routes_ready.load(Ordering::SeqCst) {
                return;
            }
            notice.await;
        }
    }

    pub fn force_graceful_drain_timeout_for_test(&self) {
        self.stage
            .control
            .force_graceful_drain_timeout
            .store(true, Ordering::SeqCst);
    }

    /// Wait until every accepted ordinary request has either received its
    /// reply receipt or reached its terminal error path. This observation is
    /// test-only: production shutdown keeps its independent graceful drain.
    pub async fn wait_until_requests_idle_for_test(&self) {
        self.sync_routes.wait_idle().await;
    }

    pub async fn wait_until_shutdown_admission_closed_for_test(&self) {
        loop {
            let notice = self
                .stage
                .control
                .shutdown_admission_closed_notice
                .notified();
            tokio::pin!(notice);
            notice.as_mut().enable();
            if self
                .stage
                .control
                .shutdown_admission_closed
                .load(Ordering::SeqCst)
            {
                return;
            }
            notice.await;
        }
    }

    /// Snapshot production-dead counters and accepted fragment identities.
    /// The caller cannot alter transport state through this observation.
    pub fn observations_for_test(&self) -> LargeRequestTestObservations {
        LargeRequestTestObservations {
            completed_handler_dispatches: self
                .stage
                .control
                .completed_handler_dispatches
                .load(Ordering::SeqCst),
            injected_reply_resets: self
                .stage
                .control
                .injected_reply_resets
                .load(Ordering::SeqCst),
            authenticated_status_probes_after_reset: self
                .stage
                .control
                .authenticated_status_probes_after_reset
                .load(Ordering::SeqCst),
            accepted_fragment_sequences: self
                .stage
                .control
                .accepted_fragment_sequences
                .lock()
                .unwrap_or_else(|err| err.into_inner())
                .clone(),
            authenticated_pull_reply_bytes_after_reset: self
                .stage
                .control
                .authenticated_pull_reply_bytes_after_reset
                .lock()
                .unwrap_or_else(|err| err.into_inner())
                .clone(),
            staged_response_manifests: self
                .stage
                .control
                .staged_response_manifests
                .lock()
                .unwrap_or_else(|err| err.into_inner())
                .clone(),
            requested_response_chunks: self
                .stage
                .control
                .requested_response_chunks
                .lock()
                .unwrap_or_else(|err| err.into_inner())
                .clone(),
            served_response_chunks: self
                .stage
                .control
                .served_response_chunks
                .lock()
                .unwrap_or_else(|err| err.into_inner())
                .clone(),
            completed_response_transfers: self
                .stage
                .control
                .completed_response_transfers
                .lock()
                .unwrap_or_else(|err| err.into_inner())
                .clone(),
            released_response_transfers: self
                .stage
                .control
                .released_response_transfers
                .lock()
                .unwrap_or_else(|err| err.into_inner())
                .clone(),
            durable_response_complete_outcomes: self
                .stage
                .control
                .durable_response_complete_outcomes
                .lock()
                .unwrap_or_else(|err| err.into_inner())
                .clone(),
            response_complete_controls_received: self
                .stage
                .control
                .response_complete_controls_received
                .load(Ordering::SeqCst),
            successful_response_complete_ack_writes: self
                .stage
                .control
                .successful_response_complete_ack_writes
                .load(Ordering::SeqCst),
            successful_reply_receipts: self
                .sync_routes
                .successful_reply_receipts
                .load(Ordering::SeqCst),
            terminal_reply_receipt_failures: self
                .sync_routes
                .terminal_reply_receipt_failures
                .load(Ordering::SeqCst),
            injected_pre_durable_complete_resets: self
                .stage
                .control
                .injected_pre_durable_complete_resets
                .load(Ordering::SeqCst),
            injected_post_durable_complete_ack_resets: self
                .stage
                .control
                .injected_post_durable_complete_ack_resets
                .load(Ordering::SeqCst),
        }
    }

    /// Reads the durable response inventory without changing transport state.
    pub fn response_stage_counts_for_test(&self) -> TransportResult<LargeResponseStageCounts> {
        let (stages, receipts) = large_response_stage_counts(&self.stage.root)?;
        Ok(LargeResponseStageCounts { stages, receipts })
    }

    pub fn stage_snapshot_for_test(
        &self,
        authenticated_node_id: &str,
        subject: &str,
        unit_digest: [u8; blake3::OUT_LEN],
        total_bytes: u64,
    ) -> TransportResult<Option<LargeRequestStageSnapshot>> {
        snapshot_large_request_stage(
            &self.stage.root,
            authenticated_node_id,
            subject,
            unit_digest,
            total_bytes,
        )
        .map(|snapshot| {
            snapshot.map(|snapshot| LargeRequestStageSnapshot {
                subject: snapshot.subject,
                authenticated_node_id: snapshot.authenticated_node_id,
                unit_digest: snapshot.unit_digest,
                total_bytes: snapshot.total_bytes,
                total_fragments: snapshot.total_fragments,
                fragments: snapshot
                    .fragments
                    .into_iter()
                    .map(|fragment| LargeRequestFragmentSnapshot {
                        sequence: fragment.sequence,
                        bytes: fragment.bytes,
                        digest: fragment.digest,
                    })
                    .collect(),
            })
        })
    }

    pub fn stage_snapshots_for_test(&self) -> TransportResult<Vec<LargeRequestStageSnapshot>> {
        snapshot_all_large_request_stages(&self.stage.root).map(|snapshots| {
            snapshots
                .into_iter()
                .map(|snapshot| LargeRequestStageSnapshot {
                    subject: snapshot.subject,
                    authenticated_node_id: snapshot.authenticated_node_id,
                    unit_digest: snapshot.unit_digest,
                    total_bytes: snapshot.total_bytes,
                    total_fragments: snapshot.total_fragments,
                    fragments: snapshot
                        .fragments
                        .into_iter()
                        .map(|fragment| LargeRequestFragmentSnapshot {
                            sequence: fragment.sequence,
                            bytes: fragment.bytes,
                            digest: fragment.digest,
                        })
                        .collect(),
                })
                .collect()
        })
    }
}

impl DurableLargeRequestStage {
    #[cfg(feature = "test-seams")]
    async fn pause_after_persisted_fragment_for_test(&self, sequence: u32) {
        let armed = *self
            .control
            .pause_after_sequence
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            == Some(sequence);
        if !armed {
            return;
        }
        self.control.paused.store(true, Ordering::SeqCst);
        self.control.paused_notice.notify_waiters();
        while !self.control.released.load(Ordering::SeqCst) {
            self.control.resume_notice.notified().await;
        }
        *self
            .control
            .pause_after_sequence
            .lock()
            .unwrap_or_else(|err| err.into_inner()) = None;
    }

    #[cfg(feature = "test-seams")]
    fn take_completed_reply_drop_for_test(&self) -> bool {
        self.control
            .drop_next_complete_reply
            .swap(false, Ordering::SeqCst)
    }

    #[cfg(feature = "test-seams")]
    fn record_accepted_fragment_for_test(
        &self,
        begin: &LargeRequestBegin,
        fragment: &LargeRequestFragment,
    ) {
        self.control
            .accepted_fragment_sequences
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .push(LargeRequestFragmentSequence {
                unit_digest: begin.unit_digest,
                sequence: fragment.sequence,
                total_fragments: fragment.total_fragments,
                total_bytes: fragment.total_bytes,
            });
    }

    #[cfg(feature = "test-seams")]
    fn record_completed_handler_dispatch_for_test(&self) {
        self.control
            .completed_handler_dispatches
            .fetch_add(1, Ordering::SeqCst);
    }

    #[cfg(feature = "test-seams")]
    fn record_injected_reply_reset_for_test(&self) {
        self.control
            .injected_reply_resets
            .fetch_add(1, Ordering::SeqCst);
        self.control
            .reply_reset_injected
            .store(true, Ordering::SeqCst);
    }

    #[cfg(feature = "test-seams")]
    fn record_authenticated_status_probe_after_reset_for_test(&self, subject: &str) {
        if self.control.reply_reset_injected.load(Ordering::SeqCst)
            && subject.starts_with("sync.")
            && subject.ends_with(".status")
        {
            self.control
                .authenticated_status_probes_after_reset
                .fetch_add(1, Ordering::SeqCst);
        }
    }

    #[cfg(feature = "test-seams")]
    fn record_authenticated_pull_reply_after_reset_for_test(&self, subject: &str, bytes: usize) {
        if self.control.reply_reset_injected.load(Ordering::SeqCst)
            && subject.starts_with("sync.")
            && subject.ends_with(".pull")
        {
            self.control
                .authenticated_pull_reply_bytes_after_reset
                .lock()
                .unwrap_or_else(|err| err.into_inner())
                .push(bytes);
        }
    }

    #[cfg(feature = "test-seams")]
    fn response_manifest_observation_for_test(
        manifest: &LargeResponseManifest,
    ) -> LargeResponseManifestObservation {
        let encoded = manifest
            .encode()
            .expect("staged oversized response manifest remains encodable");
        LargeResponseManifestObservation {
            transfer_digest: *blake3::hash(&encoded).as_bytes(),
            authenticated_node_id: manifest.authenticated_node_id.clone(),
            subject: manifest.subject.clone(),
            request_digest: manifest.request_digest,
            response_digest: manifest.response_digest,
            total_bytes: manifest.total_bytes,
            total_chunks: manifest.total_chunks,
        }
    }

    #[cfg(feature = "test-seams")]
    fn record_staged_response_for_test(&self, manifest: &LargeResponseManifest) {
        self.control
            .staged_response_manifests
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .push(Self::response_manifest_observation_for_test(manifest));
        self.control
            .staged_response_manifest_notice
            .notify_waiters();
    }

    #[cfg(feature = "test-seams")]
    fn response_chunk_observation_for_test(
        authenticated_node_id: &str,
        chunk: &LargeResponseChunk,
    ) -> LargeResponseChunkSequence {
        LargeResponseChunkSequence {
            transfer_digest: chunk.transfer_digest,
            authenticated_node_id: authenticated_node_id.to_string(),
            sequence: chunk.sequence,
            total_chunks: chunk.total_chunks,
        }
    }

    #[cfg(feature = "test-seams")]
    fn record_response_chunk_requested_for_test(
        &self,
        manifest: &LargeResponseManifest,
        sequence: u64,
    ) {
        let manifest = Self::response_manifest_observation_for_test(manifest);
        let observation = LargeResponseChunkSequence {
            transfer_digest: manifest.transfer_digest,
            authenticated_node_id: manifest.authenticated_node_id,
            sequence,
            total_chunks: manifest.total_chunks,
        };
        self.control
            .requested_response_chunks
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .push(observation);
    }

    #[cfg(feature = "test-seams")]
    fn record_response_chunk_served_for_test(
        &self,
        authenticated_node_id: &str,
        chunk: &LargeResponseChunk,
    ) {
        let observation = Self::response_chunk_observation_for_test(authenticated_node_id, chunk);
        self.control
            .served_response_chunks
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .push(observation);
    }

    #[cfg(feature = "test-seams")]
    fn record_response_lifecycle_for_test(&self, manifest: &LargeResponseManifest, complete: bool) {
        let manifest = Self::response_manifest_observation_for_test(manifest);
        let observation = LargeResponseLifecycleObservation {
            transfer_digest: manifest.transfer_digest,
            authenticated_node_id: manifest.authenticated_node_id,
        };
        let observations = if complete {
            &self.control.completed_response_transfers
        } else {
            &self.control.released_response_transfers
        };
        observations
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .push(observation);
    }

    #[cfg(feature = "test-seams")]
    async fn pause_response_control_for_test(&self, gate: ResponseControlGate) -> bool {
        let armed = *self
            .control
            .response_control_gate
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            == Some(gate);
        if !armed {
            return false;
        }
        self.control
            .response_control_paused
            .store(true, Ordering::SeqCst);
        self.control.response_control_paused_notice.notify_waiters();
        while !self.control.response_control_reset.load(Ordering::SeqCst) {
            self.control.response_control_resume_notice.notified().await;
        }
        *self
            .control
            .response_control_gate
            .lock()
            .unwrap_or_else(|err| err.into_inner()) = None;
        true
    }

    #[cfg(feature = "test-seams")]
    fn record_complete_received_for_test(&self) {
        self.control
            .response_complete_controls_received
            .fetch_add(1, Ordering::SeqCst);
    }

    #[cfg(feature = "test-seams")]
    fn record_durable_complete_outcome_for_test(
        &self,
        manifest: &LargeResponseManifest,
        receipt_preexisted: bool,
    ) {
        let manifest = Self::response_manifest_observation_for_test(manifest);
        self.control
            .durable_response_complete_outcomes
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .push(LargeResponseCompleteOutcomeObservation {
                transfer_digest: manifest.transfer_digest,
                authenticated_node_id: manifest.authenticated_node_id,
                receipt_preexisted,
            });
    }

    #[cfg(feature = "test-seams")]
    fn record_successful_complete_ack_for_test(&self) {
        self.control
            .successful_response_complete_ack_writes
            .fetch_add(1, Ordering::SeqCst);
    }
}

/// A bound serving endpoint: holds the fabric identity, accepts sync
/// connections on [`SYNC_ALPN`], and can register additional protocol labels
/// for node-to-node streams that never touch the hub.
///
/// One accept loop per endpoint (spawned at bind) dispatches every incoming
/// connection by its ALPN: sync connections go to the routes the sync
/// `ServerTransport` installed, everything else to a registered peer
/// protocol. The loop holds its own endpoint handle, so dropping this struct
/// never tears down an actively serving transport. A joined sync serve loop
/// relinquishes its bound-transport handle; once every other endpoint owner
/// also drops, Iroh releases the port for a rebind.
pub struct IrohServer {
    endpoint: Endpoint,
    ticket: String,
    node_id: String,
    identity: Arc<FabricIdentity>,
    peer_protocols: PeerProtocols,
    peer_connection_protocols: PeerConnectionProtocols,
    sync_routes: SyncRoutes,
    large_request_stage: DurableLargeRequestStage,
    pre_admission: PreAdmissionGuardrails,
    accept_loop: Arc<AcceptLoopLifecycle>,
    transport_endpoint: Arc<AsyncMutex<Option<Endpoint>>>,
    serve_lifecycle: Arc<ServeLifecycle>,
}

#[derive(Clone)]
struct PreAdmissionGuardrails {
    connection_permits: Arc<Semaphore>,
    payload_permits: Arc<Semaphore>,
    request_read_idle: Duration,
}

impl PreAdmissionGuardrails {
    fn new(connections: usize, bytes: usize, request_read_idle: Duration) -> Self {
        Self {
            connection_permits: Arc::new(Semaphore::new(connections)),
            payload_permits: Arc::new(Semaphore::new(bytes)),
            request_read_idle,
        }
    }

    fn try_reserve_connection(&self) -> Option<OwnedSemaphorePermit> {
        self.connection_permits.clone().try_acquire_owned().ok()
    }

    fn try_reserve_payload(&self, bytes: usize) -> TransportResult<OwnedSemaphorePermit> {
        let permits = u32::try_from(bytes.max(1)).map_err(|_| {
            TransportError::IncompleteReply(
                "request frame exceeds the configured pre-admission byte budget".to_string(),
            )
        })?;
        self.payload_permits
            .clone()
            .try_acquire_many_owned(permits)
            .map_err(|_| {
                TransportError::IncompleteReply(
                    "request frame exceeds the available pre-admission byte budget".to_string(),
                )
            })
    }
}

/// The accept loop owns an endpoint clone.  Iroh releases its UDP sockets
/// only after every clone drops, so shutdown must await this task after asking
/// the endpoint to close.
struct AcceptLoopLifecycle {
    handle: AsyncMutex<Option<JoinHandle<()>>>,
}

const SERVE_IDLE: u8 = 0;
const SERVE_ACTIVE: u8 = 1;
const SERVE_RELEASE_FAILED: u8 = 2;

struct ServeLifecycle {
    state: std::sync::atomic::AtomicU8,
    idle: tokio::sync::Notify,
}

impl ServeLifecycle {
    fn new() -> Self {
        Self {
            state: std::sync::atomic::AtomicU8::new(SERVE_IDLE),
            idle: tokio::sync::Notify::new(),
        }
    }

    fn begin(self: &Arc<Self>, endpoint: Endpoint) -> ActiveServingEndpoint {
        self.state
            .compare_exchange(
                SERVE_IDLE,
                SERVE_ACTIVE,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .expect("the one-shot Iroh serving lease cannot be active twice");
        ActiveServingEndpoint {
            endpoint: Some(endpoint),
            lifecycle: self.clone(),
        }
    }

    fn finish(&self) {
        self.state.store(SERVE_IDLE, Ordering::Release);
        self.idle.notify_waiters();
    }

    fn fail(&self) {
        self.state.store(SERVE_RELEASE_FAILED, Ordering::Release);
        self.idle.notify_waiters();
    }

    async fn wait_idle(&self) -> Result<(), String> {
        loop {
            let idle = self.idle.notified();
            match self.state.load(Ordering::Acquire) {
                SERVE_IDLE => return Ok(()),
                SERVE_RELEASE_FAILED => {
                    return Err(
                        "the active Iroh serving socket could not be synchronously released"
                            .to_string(),
                    );
                }
                SERVE_ACTIVE => idle.await,
                state => return Err(format!("invalid Iroh serve lifecycle state {state}")),
            }
        }
    }
}

struct ActiveServingEndpoint {
    endpoint: Option<Endpoint>,
    lifecycle: Arc<ServeLifecycle>,
}

impl std::ops::Deref for ActiveServingEndpoint {
    type Target = Endpoint;

    fn deref(&self) -> &Self::Target {
        self.endpoint
            .as_ref()
            .expect("an active serving endpoint retains its lease")
    }
}

impl ActiveServingEndpoint {
    async fn release(mut self) -> Result<(), String> {
        let endpoint = self
            .endpoint
            .take()
            .expect("an active serving endpoint releases exactly once");
        let lifecycle = self.lifecycle.clone();
        let failed_lifecycle = lifecycle.clone();
        let (released, release_complete) = tokio::sync::oneshot::channel();
        let release_thread = std::thread::Builder::new()
            .name("contextdb-iroh-serving-socket-release".to_string())
            .spawn(move || {
                let outcome =
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(endpoint)));
                if outcome.is_ok() {
                    lifecycle.finish();
                } else {
                    lifecycle.fail();
                }
                let _ = released.send(outcome.is_ok());
                if let Err(payload) = outcome {
                    std::panic::resume_unwind(payload);
                }
            })
            .map_err(|err| {
                failed_lifecycle.fail();
                format!("cannot start the Iroh serving socket-release thread: {err}")
            })?;
        let released = release_complete
            .await
            .map_err(|_| "the Iroh serving socket-release thread stopped early".to_string())?;
        let joined = release_thread.join();
        if !released || joined.is_err() {
            self.lifecycle.fail();
            return Err("the Iroh serving socket-release thread panicked".to_string());
        }
        Ok(())
    }
}

impl Drop for ActiveServingEndpoint {
    fn drop(&mut self) {
        let Some(endpoint) = self.endpoint.take() else {
            return;
        };
        let lifecycle = self.lifecycle.clone();
        let failed_lifecycle = lifecycle.clone();
        if let Err(err) = std::thread::Builder::new()
            .name("contextdb-iroh-cancelled-serve-release".to_string())
            .spawn(move || {
                let outcome =
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(endpoint)));
                if outcome.is_ok() {
                    lifecycle.finish();
                } else {
                    lifecycle.fail();
                }
            })
        {
            failed_lifecycle.fail();
            panic!("cannot start the cancelled Iroh serve-release thread: {err}");
        }
    }
}

impl AcceptLoopLifecycle {
    fn new() -> Self {
        Self {
            handle: AsyncMutex::new(None),
        }
    }

    async fn install(&self, handle: JoinHandle<()>) {
        *self.handle.lock().await = Some(handle);
    }

    async fn await_termination(&self) {
        let mut handle = self.handle.lock().await;
        if let Some(task) = handle.as_mut() {
            if let Err(err) = task.await {
                tracing::warn!(error = %err, "Iroh accept loop ended with a join error during shutdown");
            }
            *handle = None;
        }
    }
}

/// Drop closed Iroh endpoint handles outside Tokio's runtime context and wait
/// until their operating-system sockets are actually released.
///
/// `iroh` 1.0 delegates its final `netwatch::UdpSocket` drop to an unjoined
/// `spawn_blocking` task when the last `Endpoint` is dropped on a Tokio
/// thread.  That makes `Endpoint::close().await` followed by an immediate
/// same-port bind race the deferred `libc::close`.  On a plain thread,
/// netwatch performs that final close synchronously; the acknowledgement keeps
/// this function as the deterministic release boundary.
async fn release_closed_endpoint_handles(endpoints: Vec<Endpoint>) -> Result<(), String> {
    let (released, release_complete) = tokio::sync::oneshot::channel();
    let release_thread = std::thread::Builder::new()
        .name("contextdb-iroh-socket-release".to_string())
        .spawn(move || {
            let outcome =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(endpoints)));
            let _ = released.send(outcome.is_ok());
            if let Err(payload) = outcome {
                std::panic::resume_unwind(payload);
            }
        })
        .map_err(|err| format!("cannot start the Iroh socket-release thread: {err}"))?;
    let released = release_complete
        .await
        .map_err(|_| "the Iroh socket-release thread stopped early".to_string())?;
    let joined = release_thread.join();
    if !released || joined.is_err() {
        return Err("the Iroh socket-release thread panicked".to_string());
    }
    Ok(())
}

impl IrohServer {
    /// Bind an endpoint per `spec` (must be a bind spec with an identity
    /// path). The keypair is loaded or generated by the fabric identity
    /// module and handed to the endpoint — never minted by the transport.
    pub async fn bind(spec: &str) -> TransportResult<Self> {
        let parsed = EndpointSpec::parse(spec).ok_or_else(|| {
            TransportError::Unreachable("not a bindable endpoint spec".to_string())
        })?;
        if parsed.dial_ticket().is_some() {
            return Err(TransportError::Unreachable(
                "cannot bind a serving endpoint from a dial ticket".to_string(),
            ));
        }
        let identity_path = parsed.identity_path().ok_or_else(|| {
            TransportError::Unreachable(
                "a serving endpoint spec must carry identity=<key-file>".to_string(),
            )
        })?;
        let identity = Arc::new(
            FabricIdentity::load_or_generate(identity_path)
                .map_err(|err| TransportError::Unreachable(err.to_string()))?,
        );
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
            (Some((v4, v6)), Some(_)) => {
                let ports = match v6 {
                    Some(v6) => format!("{v4} (and v6 {v6})"),
                    None => v4.to_string(),
                };
                TransportError::Unreachable(format!(
                    "{err} (while re-binding the remembered sync port {ports}; free the \
                     port, or pass an explicit port= — port=0 picks a fresh random port, which \
                     strands address-only tickets issued under the old port but does not rotate \
                     the hub's identity)"
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
                release_closed_endpoint_handles(vec![endpoint])
                    .await
                    .map_err(TransportError::Other)?;
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
                if std::fs::write(sticky_path, rendered).is_err() {
                    // A ticket whose port stickiness was not persisted goes
                    // stale on restart — refuse loudly rather than print a
                    // lying ticket.
                    endpoint.close().await;
                    release_closed_endpoint_handles(vec![endpoint])
                        .await
                        .map_err(TransportError::Other)?;
                    return Err(TransportError::Unreachable(
                        "cannot persist the remembered sync port (the enrollment ticket would not survive a restart; fix permissions or pass an explicit port=)".to_string(),
                    ));
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

        let stage_root = durable_large_request_stage_path(identity_path);
        if let Some(budget) = parsed.response_staging_bytes() {
            let cleanup_root = stage_root.clone();
            let cleanup = tokio::task::spawn_blocking(move || {
                enforce_response_stage_budget(&cleanup_root, budget, &[])
            })
            .await
            .map_err(|err| {
                TransportError::Other(format!("response staging cleanup task failed: {err}"))
            })
            .and_then(|result| result);
            if let Err(err) = cleanup {
                endpoint.close().await;
                release_closed_endpoint_handles(vec![endpoint])
                    .await
                    .map_err(TransportError::Other)?;
                return Err(err);
            }
        }
        // Every transport() handle shares this one serving lease. That keeps
        // pre-serve Arc clones useful while making a second serve a loud
        // error rather than a second retained UDP-socket owner.
        let transport_endpoint = Arc::new(AsyncMutex::new(Some(endpoint.clone())));
        let server = Self {
            endpoint,
            ticket,
            node_id,
            identity,
            peer_protocols: Arc::new(Mutex::new(HashMap::new())),
            peer_connection_protocols: Arc::new(Mutex::new(HashMap::new())),
            sync_routes: Arc::new(SyncRouteState::default()),
            large_request_stage: DurableLargeRequestStage {
                root: stage_root,
                lock: Arc::new(tokio::sync::Mutex::new(())),
                response_staging_budget: parsed.response_staging_bytes(),
                #[cfg(feature = "test-seams")]
                control: Arc::new(LargeRequestTestControlState::default()),
            },
            pre_admission: PreAdmissionGuardrails::new(
                parsed.pre_admission_connections(),
                parsed.pre_admission_bytes(),
                parsed.request_read_idle(),
            ),
            accept_loop: Arc::new(AcceptLoopLifecycle::new()),
            transport_endpoint,
            serve_lifecycle: Arc::new(ServeLifecycle::new()),
        };
        server.spawn_accept_loop().await;
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
            // Iroh releases the UDP socket only when its final Endpoint clone
            // drops. `serve` takes this clone and drops it before reporting
            // shutdown complete, so retaining SyncServer after run_until()
            // cannot keep a stopped hub bound.
            endpoint: self.transport_endpoint.clone(),
            serve_lifecycle: self.serve_lifecycle.clone(),
            sync_routes: self.sync_routes.clone(),
            accept_loop: self.accept_loop.clone(),
            #[cfg(feature = "test-seams")]
            control: self.large_request_stage.control.clone(),
        })
    }

    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn large_request_test_controller(&self) -> LargeRequestTestController {
        LargeRequestTestController {
            stage: self.large_request_stage.clone(),
            sync_routes: self.sync_routes.clone(),
        }
    }

    pub(crate) fn lineage_signer(&self) -> LineageSigner {
        let identity = self.identity.clone();
        Arc::new(move |bytes| Ok(identity.sign_lineage(bytes)))
    }

    /// Ask every endpoint clone to close and wait for the accept loop. A
    /// self-sufficient serving transport remains responsible for dropping its
    /// own handle when its serve task exits.
    pub async fn close(self) {
        self.endpoint.close().await;
        self.accept_loop.await_termination().await;
        // Iroh closes its UDP sockets only after every Endpoint clone drops.
        // Drop both bound handles before this async close reports completion;
        // leaving them as fields of the completed future makes an immediate
        // sticky-port rebind depend on when the caller drops that future.
        let mut endpoints = vec![self.endpoint];
        if let Some(endpoint) = self.transport_endpoint.lock().await.take() {
            endpoints.push(endpoint);
        }
        release_closed_endpoint_handles(endpoints)
            .await
            .unwrap_or_else(|err| panic!("Iroh socket release failed during shutdown: {err}"));
        self.serve_lifecycle
            .wait_idle()
            .await
            .unwrap_or_else(|err| {
                panic!("Iroh active serve release failed during shutdown: {err}")
            });
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
    async fn spawn_accept_loop(&self) {
        let endpoint = self.endpoint.clone();
        let protocols = self.peer_protocols.clone();
        let connection_protocols = self.peer_connection_protocols.clone();
        let sync_routes = self.sync_routes.clone();
        let large_request_stage = self.large_request_stage.clone();
        let pre_admission = self.pre_admission.clone();
        let task = tokio::spawn(async move {
            let mut connection_tasks = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    incoming = endpoint.accept() => {
                        let Some(incoming) = incoming else {
                            break;
                        };
                        let Some(connection_permit) = pre_admission.try_reserve_connection() else {
                            incoming.refuse();
                            continue;
                        };
                        let protocols = protocols.clone();
                        let connection_protocols = connection_protocols.clone();
                        let sync_routes = sync_routes.clone();
                        let large_request_stage = large_request_stage.clone();
                        let pre_admission = pre_admission.clone();
                        connection_tasks.spawn(async move {
                            let _connection_permit = connection_permit;
                            let Ok(connection) = incoming.await else {
                                return;
                            };
                            let alpn = connection.alpn().to_vec();
                            if alpn == SYNC_ALPN {
                                serve_sync_connection(
                                    connection,
                                    sync_routes,
                                    large_request_stage,
                                    pre_admission,
                                )
                                .await;
                                return;
                            }
                            let handler = {
                                let protocols =
                                    protocols.lock().unwrap_or_else(|err| err.into_inner());
                                protocols.get(&alpn).cloned()
                            };
                            if let Some(handler) = handler {
                                serve_peer_connection(connection, handler, pre_admission).await;
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
                                tracing::debug!(
                                    error = %err,
                                    "peer connection handler ended with error"
                                );
                            }
                            // Keep the server side alive until the remote closes, so
                            // replies written by the handler are never cut off by an
                            // early local drop.
                            let _ = keepalive.closed().await;
                        });
                    }
                    completed = connection_tasks.join_next(),
                        if !connection_tasks.is_empty() =>
                    {
                        if let Some(Err(err)) = completed
                            && !err.is_cancelled()
                        {
                            tracing::warn!(
                                error = %err,
                                "Iroh connection task ended with a join error"
                            );
                        }
                    }
                }
            }
            // Endpoint close is the hard boundary after the graceful route
            // drain. No detached request or peer task may retain connection
            // state—and therefore the bound socket—past the joined accept loop.
            connection_tasks.abort_all();
            while let Some(result) = connection_tasks.join_next().await {
                if let Err(err) = result
                    && !err.is_cancelled()
                {
                    tracing::warn!(
                        error = %err,
                        "Iroh connection task ended with a join error during shutdown"
                    );
                }
            }
        });
        self.accept_loop.install(task).await;
    }
}

async fn serve_peer_connection(
    connection: Connection,
    handler: PeerHandler,
    pre_admission: PreAdmissionGuardrails,
) {
    while let Ok((mut send, mut recv)) = connection.accept_bi().await {
        let Ok((request, payload_permit)) =
            read_pre_admission_payload(&mut recv, MAX_FRAME_BYTES, &pre_admission).await
        else {
            return;
        };
        let request = PeerRequest {
            remote_node_id: hex_node_id(&connection.remote_id()),
            bytes: request,
        };
        let outcome = handler(request).await;
        drop(payload_permit);
        match outcome {
            Ok(reply) => {
                if write_reply(&mut send, &mut recv, REPLY_OK, &reply)
                    .await
                    .is_err()
                {
                    return;
                }
            }
            Err(err) => {
                let detail = err.to_string();
                let _ =
                    write_reply(&mut send, &mut recv, REPLY_HANDLER_ERROR, detail.as_bytes()).await;
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
        match reply? {
            FrameReply::Payload(reply) => Ok(reply),
            FrameReply::LargeResponseManifest(_) => Err(TransportError::IncompleteReply(
                "peer protocol received an unexpected oversized sync response".to_string(),
            )),
            FrameReply::LargeRequestProgress(_) => Err(TransportError::IncompleteReply(
                "peer protocol received an unexpected oversized request progress reply".to_string(),
            )),
        }
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

#[cfg(test)]
pub(super) fn client(spec: &str) -> Arc<dyn ClientTransport> {
    client_with_lineage_signer(spec).0
}

pub(super) fn client_with_lineage_signer(
    spec: &str,
) -> (Arc<dyn ClientTransport>, Option<LineageSigner>) {
    let identity = parse_dial_target(spec)
        .ok()
        .and_then(|(_, parsed)| dialing_identity(parsed.identity_path()).ok())
        .map(Arc::new);
    let has_stable_edge_identity = parse_dial_target(spec)
        .is_ok_and(|(_, parsed)| parsed.identity_path().is_some())
        && identity.is_some();
    let signer = identity.as_ref().map(|identity| {
        let identity = Arc::clone(identity);
        Arc::new(move |bytes: &[u8]| Ok(identity.sign_lineage(bytes))) as LineageSigner
    });
    (
        Arc::new(IrohClientTransport {
            spec: spec.to_string(),
            identity,
            has_stable_edge_identity,
            state: tokio::sync::Mutex::new(None),
            #[cfg(feature = "test-seams")]
            test_control: Arc::new(IrohClientTestControlState::default()),
        }),
        signer,
    )
}

/// Construct the database-owned sync client. An explicit endpoint identity
/// wins over the adjacent database identity; without either, this transport
/// refuses before opening a connection and produces no lineage signer.
pub(super) fn sync_client_with_lineage_signer(
    spec: &str,
    default_identity_path: Option<PathBuf>,
) -> (Arc<dyn ClientTransport>, Option<LineageSigner>) {
    let selected_identity_path = parse_dial_target(spec).ok().and_then(|(_, parsed)| {
        parsed
            .identity_path()
            .map(Path::to_path_buf)
            .or(default_identity_path)
    });
    let identity = selected_identity_path
        .as_deref()
        .and_then(|path| dialing_identity(Some(path)).ok())
        .map(Arc::new);
    let has_stable_edge_identity = identity.is_some();
    let signer = identity.as_ref().map(|identity| {
        let identity = Arc::clone(identity);
        Arc::new(move |bytes: &[u8]| Ok(identity.sign_lineage(bytes))) as LineageSigner
    });
    (
        Arc::new(IrohClientTransport {
            spec: spec.to_string(),
            identity,
            has_stable_edge_identity,
            state: tokio::sync::Mutex::new(None),
            #[cfg(feature = "test-seams")]
            test_control: Arc::new(IrohClientTestControlState::default()),
        }),
        signer,
    )
}

/// Construct the normal authenticated Iroh client with test-only retry
/// controls. This factory is absent from production builds.
#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub fn client_with_test_controller_for_test(
    spec: &str,
) -> (Arc<dyn ClientTransport>, IrohClientTestController) {
    let identity = parse_dial_target(spec)
        .ok()
        .and_then(|(_, parsed)| dialing_identity(parsed.identity_path()).ok())
        .map(Arc::new);
    let has_stable_edge_identity = parse_dial_target(spec)
        .is_ok_and(|(_, parsed)| parsed.identity_path().is_some())
        && identity.is_some();
    let control = Arc::new(IrohClientTestControlState::default());
    (
        Arc::new(IrohClientTransport {
            spec: spec.to_string(),
            identity,
            has_stable_edge_identity,
            state: tokio::sync::Mutex::new(None),
            test_control: Arc::clone(&control),
        }),
        IrohClientTestController { control },
    )
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
    identity: Option<Arc<FabricIdentity>>,
    has_stable_edge_identity: bool,
    state: tokio::sync::Mutex<Option<ClientState>>,
    #[cfg(feature = "test-seams")]
    test_control: Arc<IrohClientTestControlState>,
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
        let identity = self.identity.as_ref().ok_or_else(|| {
            TransportError::Unreachable("cannot load the dialing fabric identity".to_string())
        })?;
        let relay = relay_choice_for_target(&target);
        let endpoint = build_endpoint(
            identity,
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

    #[cfg(feature = "test-seams")]
    async fn pause_after_response_chunk_failure_for_test(&self, sequence: u64, attempt: usize) {
        if attempt != 0 {
            return;
        }
        let armed = {
            let mut gate = self
                .test_control
                .pause_after_response_chunk_failure
                .lock()
                .unwrap_or_else(|err| err.into_inner());
            if *gate == Some(sequence) {
                *gate = None;
                true
            } else {
                false
            }
        };
        if !armed {
            return;
        }
        self.test_control
            .response_chunk_retry_paused
            .store(true, Ordering::SeqCst);
        self.test_control
            .response_chunk_retry_paused_notice
            .notify_waiters();
        loop {
            let notice = self
                .test_control
                .response_chunk_retry_resume_notice
                .notified();
            tokio::pin!(notice);
            notice.as_mut().enable();
            if self
                .test_control
                .response_chunk_retry_released
                .load(Ordering::SeqCst)
            {
                return;
            }
            notice.await;
        }
    }

    async fn request_once(
        &self,
        subject: &str,
        request_bytes: &[u8],
        timeout: Duration,
    ) -> TransportResult<FrameReply> {
        let connection = self.connected_state().await?;
        let result = if request_bytes.len() > MAX_FRAME_BYTES {
            let node_id = self
                .identity
                .as_ref()
                .expect("connected authenticated Iroh client has an identity")
                .node_id();
            exchange_large_request(&connection, &node_id, subject, request_bytes, timeout).await
        } else {
            tokio::time::timeout(
                timeout,
                exchange_frame(&connection, Some(subject), request_bytes),
            )
            .await
            .map_err(|_| TransportError::Timeout)?
        };
        if result.is_err() {
            // A failed exchange usually means the connection died (hub
            // restart). Drop it so the next request redials.
            self.drop_state().await;
        }
        result
    }

    async fn collect_large_response(
        &self,
        subject: &str,
        request_bytes: &[u8],
        manifest: LargeResponseManifest,
        timeout: Duration,
    ) -> TransportResult<Vec<u8>> {
        let node_id = self
            .identity
            .as_ref()
            .ok_or_else(|| {
                TransportError::Unreachable("cannot load the dialing fabric identity".to_string())
            })?
            .node_id();
        manifest.validate_for(&node_id, subject, *blake3::hash(request_bytes).as_bytes())?;
        let total = usize::try_from(manifest.total_bytes).map_err(|_| {
            TransportError::IncompleteReply(
                "oversized response length cannot be represented".to_string(),
            )
        })?;
        let transfer_digest = manifest.transfer_digest()?;
        let mut assembled = Vec::new();
        assembled.try_reserve_exact(total).map_err(|err| {
            TransportError::Other(format!(
                "cannot reserve {total} bytes for oversized response: {err}"
            ))
        })?;
        for sequence in 0..manifest.total_chunks {
            let chunk_offset = usize::try_from(sequence)
                .ok()
                .and_then(|sequence| sequence.checked_mul(LARGE_RESPONSE_CHUNK_BYTES))
                .ok_or_else(|| {
                    TransportError::IncompleteReply(
                        "oversized response chunk offset overflows".to_string(),
                    )
                })?;
            let expected_chunk_bytes = total
                .checked_sub(chunk_offset)
                .map(|remaining| remaining.min(LARGE_RESPONSE_CHUNK_BYTES))
                .ok_or_else(|| {
                    TransportError::IncompleteReply(
                        "oversized response chunk offset exceeds the manifest length".to_string(),
                    )
                })?;
            let control = rmp_serde::to_vec_named(&LargeResponseControl::Chunk {
                manifest: manifest.clone(),
                sequence,
            })
            .map_err(|err| {
                TransportError::Other(format!(
                    "cannot encode oversized response chunk request: {err}"
                ))
            })?;
            let mut last_error = None;
            for attempt in 0..2 {
                #[cfg(not(feature = "test-seams"))]
                let _ = attempt;
                let connection = match self.connected_state().await {
                    Ok(connection) => connection,
                    Err(err) => {
                        last_error = Some(err);
                        self.drop_state().await;
                        continue;
                    }
                };
                match tokio::time::timeout(
                    timeout,
                    exchange_frame(&connection, Some(LARGE_RESPONSE_CONTROL_SUBJECT), &control),
                )
                .await
                {
                    Ok(Ok(FrameReply::Payload(bytes))) => {
                        if bytes.len() > MAX_RESPONSE_CHUNK_ENVELOPE_BYTES {
                            last_error = Some(TransportError::IncompleteReply(
                                "oversized response chunk envelope exceeds its ceiling".to_string(),
                            ));
                            self.drop_state().await;
                            continue;
                        }
                        let decoded = tokio::task::spawn_blocking(move || {
                            rmp_serde::from_slice::<LargeResponseChunk>(&bytes)
                                .map_err(|err| err.to_string())
                        })
                        .await;
                        match decoded {
                            Ok(Ok(chunk))
                                if chunk.transfer_digest == transfer_digest
                                    && chunk.sequence == sequence
                                    && chunk.total_chunks == manifest.total_chunks
                                    && chunk.bytes.len() == expected_chunk_bytes
                                    && assembled.len().checked_add(chunk.bytes.len())
                                        == Some(chunk_offset + expected_chunk_bytes)
                                    && *blake3::hash(&chunk.bytes).as_bytes() == chunk.digest =>
                            {
                                assembled.extend_from_slice(&chunk.bytes);
                                last_error = None;
                                break;
                            }
                            Ok(Ok(_)) | Ok(Err(_)) | Err(_) => {
                                last_error = Some(TransportError::IncompleteReply(
                                    "oversized response chunk envelope, length, or integrity validation failed".to_string(),
                                ));
                            }
                        }
                    }
                    Ok(Ok(FrameReply::LargeResponseManifest(_))) => {
                        last_error = Some(TransportError::IncompleteReply(
                            "oversized response chunk used an invalid reply frame".to_string(),
                        ))
                    }
                    Ok(Ok(FrameReply::LargeRequestProgress(_))) => {
                        last_error = Some(TransportError::IncompleteReply(
                            "oversized response chunk used an oversized request progress reply"
                                .to_string(),
                        ))
                    }
                    Ok(Err(err)) => last_error = Some(err),
                    Err(_) => last_error = Some(TransportError::Timeout),
                }
                self.drop_state().await;
                #[cfg(feature = "test-seams")]
                self.pause_after_response_chunk_failure_for_test(sequence, attempt)
                    .await;
            }
            if let Some(err) = last_error {
                return Err(err);
            }
        }
        if assembled.len() != total
            || *blake3::hash(&assembled).as_bytes() != manifest.response_digest
        {
            return Err(TransportError::IncompleteReply(
                "complete oversized response failed integrity validation".to_string(),
            ));
        }
        let completion = rmp_serde::to_vec_named(&LargeResponseControl::Complete {
            manifest: manifest.clone(),
        })
        .map_err(|err| {
            TransportError::Other(format!(
                "cannot encode oversized response completion: {err}"
            ))
        })?;
        let mut last_error = None;
        for _ in 0..2 {
            let connection = match self.connected_state().await {
                Ok(connection) => connection,
                Err(err) => {
                    last_error = Some(err);
                    self.drop_state().await;
                    continue;
                }
            };
            match tokio::time::timeout(
                timeout,
                exchange_frame(
                    &connection,
                    Some(LARGE_RESPONSE_CONTROL_SUBJECT),
                    &completion,
                ),
            )
            .await
            {
                Ok(Ok(FrameReply::Payload(ack)))
                    if ack == b"contextdb-large-response-complete-v1" =>
                {
                    let release = rmp_serde::to_vec_named(&LargeResponseControl::Release {
                        manifest: manifest.clone(),
                    })
                    .map_err(|err| {
                        TransportError::Other(format!(
                            "cannot encode oversized response release: {err}"
                        ))
                    })?;
                    if let Ok(connection) = self.connected_state().await {
                        let _ = tokio::time::timeout(
                            timeout,
                            exchange_frame(
                                &connection,
                                Some(LARGE_RESPONSE_CONTROL_SUBJECT),
                                &release,
                            ),
                        )
                        .await;
                    }
                    return Ok(assembled);
                }
                Ok(Ok(FrameReply::Payload(_)))
                | Ok(Ok(FrameReply::LargeResponseManifest(_)))
                | Ok(Ok(FrameReply::LargeRequestProgress(_))) => {
                    last_error = Some(TransportError::IncompleteReply(
                        "oversized response completion acknowledgement is invalid".to_string(),
                    ))
                }
                Ok(Err(err)) => last_error = Some(err),
                Err(_) => last_error = Some(TransportError::Timeout),
            }
            self.drop_state().await;
        }
        Err(last_error.unwrap_or_else(|| {
            TransportError::IncompleteReply(
                "oversized response completion acknowledgement is missing".to_string(),
            )
        }))
    }

    async fn finish_reply(
        &self,
        subject: &str,
        request_bytes: &[u8],
        reply: FrameReply,
        timeout: Duration,
    ) -> TransportResult<Vec<u8>> {
        match reply {
            FrameReply::Payload(reply) => Ok(reply),
            FrameReply::LargeResponseManifest(bytes) => {
                let manifest = LargeResponseManifest::decode(&bytes).ok_or_else(|| {
                    TransportError::IncompleteReply(
                        "oversized response manifest is invalid".to_string(),
                    )
                })?;
                self.collect_large_response(subject, request_bytes, manifest, timeout)
                    .await
            }
            FrameReply::LargeRequestProgress(_) => Err(TransportError::IncompleteReply(
                "ordinary request received an oversized request progress reply".to_string(),
            )),
        }
    }
}

impl ClientTransport for IrohClientTransport {
    fn local_node_id(&self) -> Option<String> {
        self.identity.as_ref().map(|identity| identity.node_id())
    }

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
        self.has_stable_edge_identity
    }

    fn ensure_single_reply_retry_safe(&self, request_bytes: &[u8]) -> TransportResult<()> {
        if request_bytes.len() > MAX_FRAME_BYTES {
            // A fragmented request can have reached the authenticated handler
            // before its final reply is lost.  The sync client must use its
            // single-attempt, reconciliation-aware path instead of replaying
            // the whole request through the small-reply retry loop.
            return Err(TransportError::RetryUnsafe(
                "oversized Iroh request requires a single-attempt send".to_string(),
            ));
        }
        Ok(())
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
                Ok(reply) => {
                    self.finish_reply(subject, &request_bytes, reply, timeout)
                        .await
                }
                Err(TransportError::NoResponder) => Err(TransportError::NoResponder),
                Err(TransportError::Status(detail)) => Err(TransportError::Status(detail)),
                Err(TransportError::IndeterminateComplete(detail)) => {
                    Err(TransportError::IndeterminateComplete(detail))
                }
                Err(_first) => {
                    // One redial covers the hub-restart case; a second
                    // failure is the real answer.
                    let reply = self.request_once(subject, &request_bytes, timeout).await?;
                    self.finish_reply(subject, &request_bytes, reply, timeout)
                        .await
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
            let result = transport.serve(handlers, shutdown).await;
            server.close().await;
            result
        })
    }
}

/// The bound serving side: installs the sync route table on the endpoint's
/// accept loop, parks until shutdown, then uninstalls the routes and closes
/// the endpoint (freeing the port for a rebind — a restarted hub with the
/// same identity and port reproduces the same ticket).
struct IrohServerTransport {
    // A bound transport is single-use: taking the endpoint into serve makes
    // its ownership and its post-shutdown drop point explicit. In particular,
    // an otherwise long-lived SyncServer may retain this transport after its
    // serving task exits without retaining the UDP socket.
    endpoint: Arc<AsyncMutex<Option<Endpoint>>>,
    serve_lifecycle: Arc<ServeLifecycle>,
    sync_routes: SyncRoutes,
    accept_loop: Arc<AcceptLoopLifecycle>,
    #[cfg(feature = "test-seams")]
    control: Arc<LargeRequestTestControlState>,
}

impl ServerTransport for IrohServerTransport {
    fn serve<'a>(
        &'a self,
        handlers: Vec<HandlerRegistration>,
        shutdown: Arc<AtomicBool>,
    ) -> TransportFuture<'a, ()> {
        Box::pin(async move {
            let endpoint = {
                let mut lease = self.endpoint.lock().await;
                let endpoint = lease.take().ok_or_else(|| {
                    TransportError::Other(
                        "a bound sync endpoint may be served only once; bind a new endpoint before restarting"
                            .to_string(),
                    )
                })?;
                self.serve_lifecycle.begin(endpoint)
            };
            let mut routes: HashMap<String, super::RequestHandler> = HashMap::new();
            for registration in handlers {
                routes.insert(registration.subject, registration.handler);
            }
            self.sync_routes.begin_serving(Arc::new(routes));
            #[cfg(feature = "test-seams")]
            {
                self.control
                    .shutdown_admission_closed
                    .store(false, Ordering::SeqCst);
                self.control.routes_ready.store(true, Ordering::SeqCst);
                self.control.routes_ready_notice.notify_waiters();
            }
            #[cfg(feature = "production-smoke-driver")]
            production_smoke_routes_ready();

            let mut shutdown_poll = tokio::time::interval(SHUTDOWN_POLL_INTERVAL);
            loop {
                tokio::select! {
                    _ = shutdown_poll.tick() => {
                        if shutdown.load(Ordering::SeqCst) {
                            break;
                        }
                    }
                    _ = endpoint.closed() => break,
                }
            }

            self.sync_routes.stop_accepting();
            #[cfg(feature = "test-seams")]
            {
                self.control
                    .shutdown_admission_closed
                    .store(true, Ordering::SeqCst);
                self.control
                    .shutdown_admission_closed_notice
                    .notify_waiters();
            }
            mark_test_shutdown_quiesced(&self.sync_routes);
            #[cfg(feature = "test-seams")]
            let force_graceful_drain_timeout = self
                .control
                .force_graceful_drain_timeout
                .swap(false, Ordering::SeqCst);
            #[cfg(not(feature = "test-seams"))]
            let force_graceful_drain_timeout = false;
            if force_graceful_drain_timeout {
                self.sync_routes.abandon_response_transfers();
                self.sync_routes.cancel_reply_owners().await;
            } else {
                tokio::select! {
                    drain = tokio::time::timeout(
                        GRACEFUL_DRAIN_TIMEOUT,
                        self.sync_routes.wait_idle(),
                    ) => {
                        if drain.is_err() {
                            tracing::warn!(
                                timeout_seconds = GRACEFUL_DRAIN_TIMEOUT.as_secs(),
                                "sync graceful drain deadline elapsed; closing stalled peer requests"
                            );
                            self.sync_routes.abandon_response_transfers();
                            self.sync_routes.cancel_reply_owners().await;
                        }
                    }
                    _ = endpoint.closed() => {
                        // An owner-forced endpoint close is the crash/restart path:
                        // durable response stages resume on the rebound endpoint,
                        // while this endpoint can no longer serve continuations.
                        self.sync_routes.abandon_response_transfers();
                        self.sync_routes.cancel_reply_owners().await;
                    }
                }
            }
            append_test_shutdown_state(&self.sync_routes, "drain-complete");
            endpoint.close().await;
            self.accept_loop.await_termination().await;
            // `endpoint` is deliberately owned by this serve invocation, not
            // the retained transport. Its release is joined so a successful
            // shutdown cannot race netwatch's deferred operating-system close.
            endpoint.release().await.map_err(TransportError::Other)?;
            Ok(())
        })
    }
}

fn response_transfer_key(
    remote_node_id: &str,
    control: &LargeResponseControl,
) -> TransportResult<ResponseTransferKey> {
    let manifest = match control {
        LargeResponseControl::Chunk { manifest, .. }
        | LargeResponseControl::Complete { manifest }
        | LargeResponseControl::Release { manifest } => manifest,
    };
    Ok((remote_node_id.to_string(), manifest.transfer_digest()?))
}

async fn serve_sync_connection(
    connection: Connection,
    sync_routes: SyncRoutes,
    large_request_stage: DurableLargeRequestStage,
    pre_admission: PreAdmissionGuardrails,
) {
    // The dialing edge's authenticated fabric identity, constant for the life
    // of this connection — the hub attributes every exchange on it (push /
    // pull / status) to this node when recording last-contact.
    let remote_node_id = hex_node_id(&connection.remote_id());
    // A fragment is meaningful only after this authenticated connection has
    // presented its bounded descriptor. Durable resume lookup never consults
    // this cache: `BeginV1` reconstructs it from the stage manifest after a
    // hub restart.
    let mut request_descriptors: HashMap<[u8; blake3::OUT_LEN], LargeRequestBegin> = HashMap::new();
    loop {
        let Ok((mut send, mut recv)) = connection.accept_bi().await else {
            return;
        };
        let Ok((subject, payload, payload_permit)) =
            read_request_frame(&mut recv, &pre_admission).await
        else {
            return;
        };
        let mut predecoded_response_control = None;
        let (routes, request_lease) = match sync_routes.admit() {
            Some((routes, request_lease)) => (Some(routes), request_lease),
            None if subject == LARGE_RESPONSE_CONTROL_SUBJECT => {
                let control = match decode_large_response_control_frame(&payload) {
                    Ok(control) => control,
                    Err(_) => {
                        connection.close(1u32.into(), b"sync not serving");
                        return;
                    }
                };
                let key = match response_transfer_key(&remote_node_id, &control) {
                    Ok(key) => key,
                    Err(_) => {
                        connection.close(1u32.into(), b"sync not serving");
                        return;
                    }
                };
                let Some(request_lease) = sync_routes.admit_response_continuation(&key) else {
                    connection.close(1u32.into(), b"sync not serving");
                    return;
                };
                predecoded_response_control = Some(control);
                (None, request_lease)
            }
            None => {
                connection.close(1u32.into(), b"sync not serving");
                return;
            }
        };
        drop(payload_permit);
        let request_lease = Arc::new(request_lease);
        if subject == LARGE_RESPONSE_CONTROL_SUBJECT {
            let control: LargeResponseControl = match predecoded_response_control {
                Some(control) => control,
                None => match tokio::task::spawn_blocking(move || {
                    decode_large_response_control_frame(&payload)
                })
                .await
                {
                    Ok(Ok(control)) => control,
                    Ok(Err(err)) => {
                        let _ = write_reply(
                            &mut send,
                            &mut recv,
                            REPLY_HANDLER_ERROR,
                            format!("invalid oversized response control: {err}").as_bytes(),
                        )
                        .await;
                        continue;
                    }
                    Err(err) => {
                        let _ = write_reply(
                            &mut send,
                            &mut recv,
                            REPLY_HANDLER_ERROR,
                            format!("oversized response control task failed: {err}").as_bytes(),
                        )
                        .await;
                        continue;
                    }
                },
            };
            let response_transfer_key = match response_transfer_key(&remote_node_id, &control) {
                Ok(key) => key,
                Err(err) => {
                    let _ = write_reply(
                        &mut send,
                        &mut recv,
                        REPLY_HANDLER_ERROR,
                        err.to_string().as_bytes(),
                    )
                    .await;
                    continue;
                }
            };
            let response_transfer_paths = match &control {
                LargeResponseControl::Chunk { manifest, .. }
                | LargeResponseControl::Complete { manifest }
                | LargeResponseControl::Release { manifest } => ResponseTransferPaths {
                    stage: response_stage_path(&large_request_stage.root, manifest),
                    completion: response_completion_path(&large_request_stage.root, manifest),
                },
            };
            let completing_response_transfer = match &control {
                LargeResponseControl::Complete { .. } => Some(response_transfer_key.clone()),
                _ => None,
            };
            #[cfg(feature = "test-seams")]
            if matches!(&control, LargeResponseControl::Complete { .. }) {
                large_request_stage.record_complete_received_for_test();
                if large_request_stage
                    .pause_response_control_for_test(ResponseControlGate::CompletePreDurable)
                    .await
                {
                    large_request_stage
                        .control
                        .injected_pre_durable_complete_resets
                        .fetch_add(1, Ordering::SeqCst);
                    let _ = send.reset(0u32.into());
                    continue;
                }
            }
            #[allow(
                clippy::bind_instead_of_map,
                reason = "the test-seam completion receipt remains fallible after the durable completion succeeds"
            )]
            let outcome = {
                let _guard = large_request_stage.lock.lock().await;
                let stage_root = large_request_stage.root.clone();
                let authenticated_node_id = remote_node_id.clone();
                match control {
                    LargeResponseControl::Chunk { manifest, sequence } => {
                        #[cfg(feature = "test-seams")]
                        large_request_stage
                            .record_response_chunk_requested_for_test(&manifest, sequence);
                        tokio::task::spawn_blocking(move || {
                            read_large_response_chunk(
                                &stage_root,
                                &authenticated_node_id,
                                &manifest,
                                sequence,
                            )
                        })
                        .await
                        .map_err(|err| {
                            TransportError::Other(format!(
                                "oversized response chunk task failed: {err}"
                            ))
                        })
                        .and_then(|result| result)
                        .and_then(|chunk| {
                            if sync_routes.ensure_response_transfer(
                                &response_transfer_key,
                                response_transfer_paths.clone(),
                            ) {
                                Ok(LargeResponseControlReply::Chunk(chunk))
                            } else {
                                Err(TransportError::Other(
                                    "too many unfinished oversized response transfers".to_string(),
                                ))
                            }
                        })
                    }
                    LargeResponseControl::Complete { manifest } => {
                        #[cfg(feature = "test-seams")]
                        let observed_manifest = manifest.clone();
                        #[cfg(feature = "test-seams")]
                        let receipt_preexisted =
                            response_completion_receipt_exists(&stage_root, &manifest);
                        let validation_root = stage_root.clone();
                        let validation_node = authenticated_node_id.clone();
                        let validation_manifest = manifest.clone();
                        let validation = tokio::task::spawn_blocking(move || {
                            validate_large_response_completion(
                                &validation_root,
                                &validation_node,
                                &validation_manifest,
                            )
                        })
                        .await
                        .map_err(|err| {
                            TransportError::Other(format!(
                                "oversized response completion validation task failed: {err}"
                            ))
                        })
                        .and_then(|result| result);
                        match validation {
                            Err(err) => Err(err),
                            Ok(())
                                if !sync_routes.ensure_response_transfer(
                                    &response_transfer_key,
                                    response_transfer_paths.clone(),
                                ) =>
                            {
                                Err(TransportError::Other(
                                    "too many unfinished oversized response transfers".to_string(),
                                ))
                            }
                            Ok(()) => tokio::task::spawn_blocking(move || {
                                complete_large_response(
                                    &stage_root,
                                    &authenticated_node_id,
                                    &manifest,
                                )
                            })
                            .await
                            .map_err(|err| {
                                TransportError::Other(format!(
                                    "oversized response completion task failed: {err}"
                                ))
                            })
                            .and_then(|result| result)
                            .and_then(|()| {
                                #[cfg(feature = "test-seams")]
                                return receipt_preexisted.map(|receipt_preexisted| {
                                    large_request_stage.record_response_lifecycle_for_test(
                                        &observed_manifest,
                                        true,
                                    );
                                    large_request_stage.record_durable_complete_outcome_for_test(
                                        &observed_manifest,
                                        receipt_preexisted,
                                    );
                                    LargeResponseControlReply::CompleteAck(
                                        b"contextdb-large-response-complete-v1".to_vec(),
                                    )
                                });
                                #[cfg(not(feature = "test-seams"))]
                                Ok(LargeResponseControlReply::CompleteAck(
                                    b"contextdb-large-response-complete-v1".to_vec(),
                                ))
                            }),
                        }
                    }
                    LargeResponseControl::Release { manifest } => {
                        #[cfg(feature = "test-seams")]
                        let observed_manifest = manifest.clone();
                        tokio::task::spawn_blocking(move || {
                            release_large_response(&stage_root, &authenticated_node_id, &manifest)
                        })
                        .await
                        .map_err(|err| {
                            TransportError::Other(format!(
                                "oversized response release task failed: {err}"
                            ))
                        })
                        .and_then(|result| result)
                        .map(|()| {
                            #[cfg(feature = "test-seams")]
                            large_request_stage
                                .record_response_lifecycle_for_test(&observed_manifest, false);
                            LargeResponseControlReply::ReleaseAck(
                                b"contextdb-large-response-release-v1".to_vec(),
                            )
                        })
                    }
                }
            };
            match outcome {
                Ok(LargeResponseControlReply::Chunk(chunk)) => {
                    match rmp_serde::to_vec_named(&chunk)
                        .map_err(|err| {
                            TransportError::Other(format!(
                                "cannot encode oversized response chunk: {err}"
                            ))
                        })
                        .and_then(|bytes| {
                            if bytes.len() > MAX_RESPONSE_CHUNK_ENVELOPE_BYTES {
                                Err(TransportError::Other(
                                    "oversized response chunk envelope exceeds its ceiling"
                                        .to_string(),
                                ))
                            } else {
                                Ok(bytes)
                            }
                        }) {
                        Ok(reply) => {
                            #[cfg(feature = "test-seams")]
                            if large_request_stage
                                .pause_response_control_for_test(
                                    ResponseControlGate::ChunkBeforeReply(chunk.sequence),
                                )
                                .await
                            {
                                let _ = send.reset(0u32.into());
                                continue;
                            }
                            let wrote = write_reply(&mut send, &mut recv, REPLY_OK, &reply).await;
                            #[cfg(feature = "test-seams")]
                            if wrote.is_ok() {
                                large_request_stage
                                    .record_response_chunk_served_for_test(&remote_node_id, &chunk);
                            }
                            #[cfg(not(feature = "test-seams"))]
                            let _ = wrote;
                        }
                        Err(err) => {
                            let _ = write_reply(
                                &mut send,
                                &mut recv,
                                REPLY_HANDLER_ERROR,
                                err.to_string().as_bytes(),
                            )
                            .await;
                        }
                    }
                }
                Ok(LargeResponseControlReply::CompleteAck(reply)) => {
                    #[cfg(feature = "test-seams")]
                    if large_request_stage
                        .pause_response_control_for_test(
                            ResponseControlGate::CompletePostDurableAck,
                        )
                        .await
                    {
                        large_request_stage
                            .control
                            .injected_post_durable_complete_ack_resets
                            .fetch_add(1, Ordering::SeqCst);
                        let _ = send.reset(0u32.into());
                        continue;
                    }
                    let wrote = write_reply(&mut send, &mut recv, REPLY_OK, &reply).await;
                    #[cfg(feature = "test-seams")]
                    if wrote.is_ok() {
                        large_request_stage.record_successful_complete_ack_for_test();
                    }
                    if wrote.is_ok()
                        && let Some(key) = completing_response_transfer.as_ref()
                    {
                        sync_routes.complete_response_transfer(key);
                    }
                }
                Ok(LargeResponseControlReply::ReleaseAck(reply)) => {
                    let _ = write_reply(&mut send, &mut recv, REPLY_OK, &reply).await;
                }
                Err(err) => {
                    let detail = err.to_string();
                    let _ =
                        write_reply(&mut send, &mut recv, REPLY_HANDLER_ERROR, detail.as_bytes())
                            .await;
                }
            }
            continue;
        }
        let routes = routes.expect("only an authenticated response continuation omits sync routes");
        let (subject, payload, completed_stage) = if subject == LARGE_REQUEST_CONTROL_SUBJECT {
            let control =
                match tokio::task::spawn_blocking(move || LargeRequestControl::decode(&payload))
                    .await
                {
                    Ok(Ok(control)) => control,
                    Ok(Err(err)) => {
                        let detail = err.to_string();
                        let _ = write_reply(
                            &mut send,
                            &mut recv,
                            REPLY_HANDLER_ERROR,
                            detail.as_bytes(),
                        )
                        .await;
                        continue;
                    }
                    Err(err) => {
                        let detail = format!("oversized request control decode task failed: {err}");
                        let _ = write_reply(
                            &mut send,
                            &mut recv,
                            REPLY_HANDLER_ERROR,
                            detail.as_bytes(),
                        )
                        .await;
                        continue;
                    }
                };
            let (begin, fragment, is_begin) = match control {
                LargeRequestControl::BeginV1(begin) => (begin, None, true),
                LargeRequestControl::FragmentV1(fragment) => {
                    let Some(begin) = request_descriptors.get(&fragment.transfer_digest).cloned()
                    else {
                        let _ = write_reply(
                            &mut send,
                            &mut recv,
                            REPLY_HANDLER_ERROR,
                            b"oversized request fragment arrived before its descriptor",
                        )
                        .await;
                        continue;
                    };
                    (begin, Some(fragment), false)
                }
            };
            let descriptor_digest = match begin.descriptor_digest() {
                Ok(digest) => digest,
                Err(err) => {
                    let detail = err.to_string();
                    let _ =
                        write_reply(&mut send, &mut recv, REPLY_HANDLER_ERROR, detail.as_bytes())
                            .await;
                    continue;
                }
            };
            let outcome = {
                let _guard = large_request_stage.lock.lock().await;
                let stage_root = large_request_stage.root.clone();
                let authenticated_node_id = remote_node_id.clone();
                let begin_for_stage = begin.clone();
                let fragment_for_stage = fragment.clone();
                match tokio::task::spawn_blocking(move || match fragment_for_stage {
                    Some(fragment) => accept_large_request_fragment(
                        &stage_root,
                        &authenticated_node_id,
                        &begin_for_stage,
                        &fragment,
                    ),
                    None => {
                        begin_large_request(&stage_root, &authenticated_node_id, &begin_for_stage)
                    }
                })
                .await
                {
                    Ok(outcome) => outcome,
                    Err(err) => Err(TransportError::Other(format!(
                        "oversized request staging task failed: {err}"
                    ))),
                }
            };
            #[cfg(feature = "test-seams")]
            if outcome.is_ok()
                && let Some(fragment) = fragment.as_ref()
            {
                large_request_stage.record_accepted_fragment_for_test(&begin, fragment);
            }
            match outcome {
                Ok(StageOutcome::Pending { next_missing }) => {
                    request_descriptors.insert(begin.transfer_digest, begin.clone());
                    #[cfg(feature = "test-seams")]
                    if !is_begin && next_missing > 0 {
                        large_request_stage
                            .pause_after_persisted_fragment_for_test(next_missing - 1)
                            .await;
                    }
                    #[cfg(feature = "production-smoke-driver")]
                    if let Some(fragment) = fragment.as_ref()
                        && fragment.sequence == 0
                        && next_missing == 1
                    {
                        production_smoke_block(
                            ProductionSmokeGateKind::AfterFirstDurableRequestFragment,
                            ProductionSmokeCheckpoint::DurableRequestFragment {
                                transfer_digest: begin.transfer_digest,
                                sequence: 0,
                                next_missing,
                            },
                        );
                    }
                    let progress = LargeRequestProgress {
                        node_id: remote_node_id.clone(),
                        descriptor_digest,
                        transfer_digest: begin.transfer_digest,
                        next_missing,
                    };
                    let reply = if is_begin {
                        LargeRequestControlReply::BeginProgressV1(progress)
                    } else {
                        LargeRequestControlReply::FragmentProgressV1(progress)
                    };
                    let reply = match reply.encode() {
                        Ok(reply) => reply,
                        Err(err) => {
                            let detail = err.to_string();
                            let _ = write_reply(
                                &mut send,
                                &mut recv,
                                REPLY_HANDLER_ERROR,
                                detail.as_bytes(),
                            )
                            .await;
                            continue;
                        }
                    };
                    if write_reply(&mut send, &mut recv, REPLY_LARGE_REQUEST_PROGRESS, &reply)
                        .await
                        .is_err()
                    {
                        return;
                    }
                    continue;
                }
                Ok(StageOutcome::Complete {
                    subject,
                    payload,
                    completed_path,
                }) => {
                    request_descriptors.remove(&begin.transfer_digest);
                    (subject, payload, Some(completed_path))
                }
                Err(err) => {
                    let detail = err.to_string();
                    let _ =
                        write_reply(&mut send, &mut recv, REPLY_HANDLER_ERROR, detail.as_bytes())
                            .await;
                    continue;
                }
            }
        } else {
            (subject, payload, None)
        };
        let request_digest = *blake3::hash(&payload).as_bytes();
        #[cfg(feature = "production-smoke-driver")]
        production_smoke_push_request_path(&subject, request_digest, completed_stage.is_some());
        #[cfg(feature = "test-seams")]
        large_request_stage.record_authenticated_status_probe_after_reset_for_test(&subject);
        let Some(handler) = routes.get(&subject).cloned() else {
            let _ = write_reply(&mut send, &mut recv, REPLY_NO_HANDLER, subject.as_bytes()).await;
            if let Some(path) = completed_stage {
                remove_stage_after_reply(&large_request_stage, path).await;
            }
            continue;
        };
        #[cfg(feature = "test-seams")]
        if completed_stage.is_some() {
            large_request_stage.record_completed_handler_dispatch_for_test();
        }
        // The responder owns both stream halves; the handler may reply from a
        // spawned task (the push apply path does).
        let send_slot = Arc::new(tokio::sync::Mutex::new(Some((send, recv))));
        if !sync_routes.register_reply_stream(&send_slot) {
            drop(send_slot.lock().await.take());
            drop(request_lease);
            return;
        }
        let response_subject = subject.clone();
        let response_request_digest = request_digest;
        let response_node_id = remote_node_id.clone();
        let responder: Responder = Box::new({
            let send_slot = send_slot.clone();
            let large_request_stage = large_request_stage.clone();
            let completed_stage = completed_stage.clone();
            let response_subject = response_subject.clone();
            let response_node_id = response_node_id.clone();
            let responder_request_lease = request_lease.clone();
            let responder_sync_routes = sync_routes.clone();
            move |response_bytes| {
                let response_subject = response_subject.clone();
                let response_node_id = response_node_id.clone();
                Box::pin(async move {
                    let cancellation = responder_sync_routes.clone();
                    let reply = async move {
                        // Keep admission charged until the peer has acknowledged
                        // the reply. An unused underscore binding may be dropped
                        // before the later awaits, which lets shutdown close the
                        // endpoint after commit but before confirmation reaches
                        // the edge.
                        let request_lease = responder_request_lease;
                        let oversized_response = response_bytes.len() > MAX_FRAME_BYTES;
                        let response_transfer_reservation = if oversized_response {
                            Some(
                                responder_sync_routes
                                    .reserve_response_transfer()
                                    .ok_or_else(|| {
                                        TransportError::Other(
                                            "too many unfinished oversized response transfers"
                                                .to_string(),
                                        )
                                    })?,
                            )
                        } else {
                            None
                        };
                        let mut slot = send_slot.lock().await;
                        let Some((mut send, mut recv)) = slot.take() else {
                            return Err(TransportError::Other(
                                "reply already sent on this stream".to_string(),
                            ));
                        };
                        #[cfg(feature = "test-seams")]
                        large_request_stage.record_authenticated_pull_reply_after_reset_for_test(
                            &response_subject,
                            response_bytes.len(),
                        );
                        #[cfg(feature = "test-seams")]
                        if completed_stage.is_some()
                            && large_request_stage.take_completed_reply_drop_for_test()
                        {
                            // Reset the actual reply stream instead of merely
                            // withholding its bytes. The client observes a
                            // deterministic transport failure and takes its
                            // ordinary reconnect/reconciliation path.
                            large_request_stage.record_injected_reply_reset_for_test();
                            let _ = send.reset(0u32.into());
                            if let Some(path) = completed_stage {
                                remove_stage_after_reply(&large_request_stage, path).await;
                            }
                            return Err(TransportError::Unreachable(
                                "sync endpoint closed before replying".to_string(),
                            ));
                        }
                        let mut registered_response_transfer = None;
                        let reply = if response_bytes.len() > MAX_FRAME_BYTES {
                            let _guard = large_request_stage.lock.lock().await;
                            let stage_root = large_request_stage.root.clone();
                            let response_staging_budget =
                                large_request_stage.response_staging_budget;
                            let protected = responder_sync_routes.protected_response_paths();
                            let subject = response_subject.clone();
                            let node_id = response_node_id.clone();
                            match tokio::task::spawn_blocking(move || {
                                stage_large_response_with_budget(
                                    &stage_root,
                                    &node_id,
                                    &subject,
                                    response_request_digest,
                                    &response_bytes,
                                    response_staging_budget,
                                    &protected,
                                )
                            })
                            .await
                            {
                                Ok(Ok(manifest)) => {
                                    let encoded = manifest.encode()?;
                                    let transfer_key =
                                        (response_node_id.clone(), manifest.transfer_digest()?);
                                    response_transfer_reservation
                                        .expect("oversized response reserved before staging")
                                        .activate(
                                            transfer_key.clone(),
                                            ResponseTransferPaths {
                                                stage: response_stage_path(
                                                    &large_request_stage.root,
                                                    &manifest,
                                                ),
                                                completion: response_completion_path(
                                                    &large_request_stage.root,
                                                    &manifest,
                                                ),
                                            },
                                        );
                                    registered_response_transfer =
                                        Some((transfer_key, manifest.clone()));
                                    #[cfg(feature = "test-seams")]
                                    large_request_stage.record_staged_response_for_test(&manifest);
                                    Ok(encoded)
                                }
                                Ok(Err(err)) => Err(err),
                                Err(err) => Err(TransportError::Other(format!(
                                    "oversized response staging task failed: {err}"
                                ))),
                            }
                        } else {
                            Ok(response_bytes)
                        };
                        let (result, _receipt_consumed) = match reply {
                            Ok(reply) if oversized_response => {
                                let result =
                                    write_reply(&mut send, &mut recv, REPLY_LARGE_RESPONSE, &reply)
                                        .await;
                                let consumed = result.is_ok();
                                (result, consumed)
                            }
                            Ok(reply) => {
                                let result =
                                    write_reply(&mut send, &mut recv, REPLY_OK, &reply).await;
                                let consumed = result.is_ok();
                                (result, consumed)
                            }
                            Err(err) => {
                                let detail = err.to_string();
                                let consumed = write_reply(
                                    &mut send,
                                    &mut recv,
                                    REPLY_HANDLER_ERROR,
                                    detail.as_bytes(),
                                )
                                .await
                                .is_ok();
                                (Err(err), consumed)
                            }
                        };
                        #[cfg(feature = "test-seams")]
                        responder_sync_routes.record_reply_receipt_for_test(_receipt_consumed);
                        let reply_event = match &result {
                            Ok(()) => "reply-acknowledged".to_string(),
                            Err(err) => format!("reply-failed error={err}"),
                        };
                        append_test_shutdown_state(&responder_sync_routes, &reply_event);
                        if result.is_err()
                            && let Some((key, manifest)) = registered_response_transfer.as_ref()
                            && responder_sync_routes.complete_response_transfer(key)
                        {
                            let _guard = large_request_stage.lock.lock().await;
                            let stage_root = large_request_stage.root.clone();
                            let authenticated_node_id = response_node_id.clone();
                            let manifest = manifest.clone();
                            match tokio::task::spawn_blocking(move || {
                                abandon_large_response(
                                    &stage_root,
                                    &authenticated_node_id,
                                    &manifest,
                                )
                            })
                            .await
                            {
                                Ok(Ok(())) => {}
                                Ok(Err(err)) => tracing::warn!(
                                    error = %err,
                                    "undelivered oversized response stage could not be removed"
                                ),
                                Err(err) => tracing::warn!(
                                    error = %err,
                                    "undelivered oversized response cleanup task failed"
                                ),
                            }
                        }
                        if let Some(path) = completed_stage {
                            remove_stage_after_reply(&large_request_stage, path).await;
                        }
                        drop(request_lease);
                        result
                    };
                    tokio::select! {
                        result = reply => result,
                        _ = cancellation.reply_owner_cancelled() => Err(
                            TransportError::Unreachable(
                                "sync endpoint closed before replying".to_string(),
                            ),
                        ),
                    }
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
            if let Some((mut send, mut recv)) = slot.take() {
                let sent =
                    write_reply(&mut send, &mut recv, REPLY_HANDLER_ERROR, detail.as_bytes()).await;
                if sent.is_ok()
                    && let Some(path) = completed_stage
                {
                    remove_stage_after_reply(&large_request_stage, path).await;
                }
            }
            tracing::error!(error = %detail, "sync transport request failed");
        }
        drop(request_lease);
    }
}

fn decode_large_response_control_frame(bytes: &[u8]) -> TransportResult<LargeResponseControl> {
    if bytes.len() > MAX_RESPONSE_CONTROL_BYTES {
        return Err(TransportError::IncompleteReply(
            "oversized response control exceeds its frame ceiling".to_string(),
        ));
    }
    rmp_serde::from_slice(bytes).map_err(|err| {
        TransportError::IncompleteReply(format!("invalid oversized response control: {err}"))
    })
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
                TransportError::Unreachable("invalid relay url in endpoint spec".to_string())
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
                    TransportError::Unreachable(
                        "invalid publish service url in sync endpoint spec".to_string(),
                    )
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
                    TransportError::Unreachable(
                        "invalid lookup service url in sync endpoint spec".to_string(),
                    )
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
    let parsed = EndpointSpec::parse(spec)
        .ok_or_else(|| TransportError::Unreachable("not a dialable endpoint spec".to_string()))?;
    let ticket_str = parsed.dial_ticket().ok_or_else(|| {
        TransportError::Unreachable(
            "a client needs the server's ticket, not a bind spec".to_string(),
        )
    })?;
    let ticket: EndpointTicket = ticket_str
        .parse()
        .map_err(|_| TransportError::Unreachable("invalid endpoint ticket".to_string()))?;
    Ok((ticket.endpoint_addr().clone(), parsed))
}

/// Load the operator-supplied relay CA certificate(s): PEM (possibly a
/// bundle) or a single DER certificate.
fn load_relay_ca_certs(
    path: &Path,
) -> TransportResult<Vec<rustls_pki_types::CertificateDer<'static>>> {
    let bytes = std::fs::read(path)
        .map_err(|_| TransportError::Unreachable("cannot read relay-ca file".to_string()))?;
    if bytes.starts_with(b"-----BEGIN") {
        use rustls_pki_types::pem::PemObject;
        let certs: Result<Vec<_>, _> =
            rustls_pki_types::CertificateDer::pem_slice_iter(&bytes).collect();
        let certs = certs
            .map_err(|_| TransportError::Unreachable("invalid PEM in relay-ca file".to_string()))?;
        if certs.is_empty() {
            return Err(TransportError::Unreachable(
                "relay-ca file contains no certificates".to_string(),
            ));
        }
        Ok(certs)
    } else {
        Ok(vec![rustls_pki_types::CertificateDer::from(bytes)])
    }
}

/// The raw dialing endpoint's identity: the enrolled key when the spec pins
/// one, else ephemeral. Durable sync construction refuses the latter
/// case through its separate factory; raw transport callers retain it for
/// protocol-level anonymous-refusal coverage without a database participant.
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
                "sync endpoint unreachable: connection could not be established; verify both peers use the same ContextDB transport version and upgrade both ends"
                    .to_string(),
            )
        })
}

/// One request/reply exchange on a fresh bi-stream. `subject` is framed for
/// sync-channel requests and omitted for peer-protocol requests.
async fn exchange_frame(
    connection: &Connection,
    subject: Option<&str>,
    payload: &[u8],
) -> TransportResult<FrameReply> {
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
    let mut status = [0u8; 1];
    recv.read_exact(&mut status).await.map_err(|_| {
        TransportError::Unreachable("sync endpoint closed before replying".to_string())
    })?;
    let reply = read_length_prefixed(&mut recv, MAX_FRAME_BYTES).await?;
    write_all(&mut send, &[REPLY_RECEIVED_ACK]).await?;
    send.finish()
        .map_err(|_| TransportError::Unreachable("sync endpoint connection lost".to_string()))?;
    match send.stopped().await {
        Ok(None) => {}
        Ok(Some(_)) | Err(_) => {
            return Err(TransportError::Unreachable(
                "sync endpoint closed before the reply receipt was transport-confirmed".to_string(),
            ));
        }
    }
    match status[0] {
        REPLY_OK => Ok(FrameReply::Payload(reply)),
        REPLY_LARGE_RESPONSE => Ok(FrameReply::LargeResponseManifest(reply)),
        REPLY_LARGE_REQUEST_PROGRESS => Ok(FrameReply::LargeRequestProgress(reply)),
        REPLY_NO_HANDLER => Err(TransportError::NoResponder),
        REPLY_HANDLER_ERROR => Err(TransportError::Status(
            String::from_utf8_lossy(&reply).into_owned(),
        )),
        other => Err(TransportError::Other(format!(
            "unknown reply status byte {other}"
        ))),
    }
}

async fn exchange_large_request(
    connection: &Connection,
    local_node_id: &str,
    subject: &str,
    payload: &[u8],
    fragment_timeout: Duration,
) -> TransportResult<FrameReply> {
    let unit_digest = *blake3::hash(payload).as_bytes();
    let begin = LargeRequestBegin::new(subject, unit_digest, payload.len())?;
    let descriptor_digest = begin.descriptor_digest()?;
    let total_fragments = large_request_fragment_count(payload.len());
    let begin_frame = LargeRequestControl::BeginV1(begin.clone()).encode()?;
    let begin_reply = tokio::time::timeout(
        fragment_timeout,
        exchange_frame(
            connection,
            Some(LARGE_REQUEST_CONTROL_SUBJECT),
            &begin_frame,
        ),
    )
    .await
    .map_err(|_| {
        TransportError::IndeterminateComplete("oversized request begin timed out".to_string())
    })?
    .map_err(|err| TransportError::IndeterminateComplete(err.to_string()))?;
    let next_missing = match begin_reply {
        FrameReply::LargeRequestProgress(reply) => match LargeRequestControlReply::decode(&reply) {
            Some(LargeRequestControlReply::BeginProgressV1(progress)) => {
                validate_large_request_progress(
                    &progress,
                    local_node_id,
                    descriptor_digest,
                    begin.transfer_digest,
                    begin.total_fragments,
                    None,
                )?;
                progress.next_missing
            }
            Some(_) => {
                return Err(TransportError::IncompleteReply(
                    "oversized request begin received the wrong acknowledgement kind".to_string(),
                ));
            }
            None => {
                return Err(TransportError::IncompleteReply(
                    "oversized request begin received an invalid progress envelope".to_string(),
                ));
            }
        },
        reply => return Ok(reply),
    };
    for (sequence, fragment) in payload
        .chunks(LARGE_REQUEST_FRAGMENT_BYTES)
        .enumerate()
        .skip(next_missing as usize)
    {
        let encoded = LargeRequestFragment::encode(
            subject,
            unit_digest,
            payload.len(),
            sequence,
            total_fragments,
            fragment,
        )?;
        let encoded =
            LargeRequestControl::FragmentV1(rmp_serde::from_slice(&encoded).map_err(|err| {
                TransportError::Other(format!(
                    "cannot decode encoded oversized request fragment: {err}"
                ))
            })?)
            .encode()?;
        let reply = tokio::time::timeout(
            fragment_timeout,
            exchange_frame(connection, Some(LARGE_REQUEST_CONTROL_SUBJECT), &encoded),
        )
        .await
        .map_err(|_| {
            if sequence + 1 == total_fragments {
                TransportError::IndeterminateComplete(
                    "oversized request final fragment timed out".to_string(),
                )
            } else {
                TransportError::Timeout
            }
        })?;
        let reply = match reply {
            Ok(reply) => reply,
            Err(err) if sequence + 1 == total_fragments => {
                return Err(TransportError::IndeterminateComplete(err.to_string()));
            }
            Err(err) => return Err(err),
        };
        match reply {
            FrameReply::LargeRequestProgress(reply) => {
                match LargeRequestControlReply::decode(&reply) {
                    Some(LargeRequestControlReply::FragmentProgressV1(progress)) => {
                        validate_large_request_progress(
                            &progress,
                            local_node_id,
                            descriptor_digest,
                            begin.transfer_digest,
                            begin.total_fragments,
                            Some(u32::try_from(sequence + 1).map_err(|_| {
                                TransportError::Other(
                                    "oversized request has too many fragments".to_string(),
                                )
                            })?),
                        )?;
                        if sequence + 1 == total_fragments {
                            return Err(TransportError::IncompleteReply(
                                "oversized request remains incomplete after its final fragment"
                                    .to_string(),
                            ));
                        }
                    }
                    Some(_) => {
                        return Err(TransportError::IncompleteReply(
                            "oversized request fragment received the wrong acknowledgement kind"
                                .to_string(),
                        ));
                    }
                    None => {
                        return Err(TransportError::IncompleteReply(format!(
                            "oversized request fragment {sequence} received an invalid acknowledgement"
                        )));
                    }
                }
            }
            reply if sequence + 1 == total_fragments => return Ok(reply),
            _ => {
                return Err(TransportError::IncompleteReply(
                    "oversized request received an invalid reply frame".to_string(),
                ));
            }
        }
    }
    if next_missing == begin.total_fragments {
        return Err(TransportError::IncompleteReply(
            "oversized request completed stage did not return its handler reply".to_string(),
        ));
    }
    Err(TransportError::Other(
        "oversized request contained no fragments".to_string(),
    ))
}

fn validate_large_request_progress(
    progress: &LargeRequestProgress,
    local_node_id: &str,
    descriptor_digest: [u8; blake3::OUT_LEN],
    transfer_digest: [u8; blake3::OUT_LEN],
    total_fragments: u32,
    expected_next_missing: Option<u32>,
) -> TransportResult<()> {
    if progress.node_id != local_node_id
        || progress.descriptor_digest != descriptor_digest
        || progress.transfer_digest != transfer_digest
        || progress.next_missing > total_fragments
        || expected_next_missing.is_some_and(|expected| progress.next_missing != expected)
    {
        return Err(TransportError::IncompleteReply(
            "oversized request progress does not match the authenticated descriptor".to_string(),
        ));
    }
    Ok(())
}

async fn remove_stage_after_reply(stage: &DurableLargeRequestStage, path: PathBuf) {
    let _guard = stage.lock.lock().await;
    let stage_path = path.clone();
    let root = stage.root.clone();
    match tokio::task::spawn_blocking(move || {
        remove_completed_large_request_stage(&root, &stage_path)
    })
    .await
    {
        Ok(Ok(())) => {}
        Ok(Err(err)) => {
            tracing::warn!(
                stage = %path.display(),
                error = %err,
                "completed oversized request stage could not be removed"
            );
        }
        Err(err) => {
            tracing::warn!(
                stage = %path.display(),
                error = %err,
                "completed oversized request cleanup task failed"
            );
        }
    }
}

async fn read_request_frame(
    recv: &mut iroh::endpoint::RecvStream,
    pre_admission: &PreAdmissionGuardrails,
) -> TransportResult<(String, Vec<u8>, OwnedSemaphorePermit)> {
    let mut subject_len = [0u8; 2];
    read_exact_with_progress(recv, &mut subject_len, pre_admission.request_read_idle).await?;
    let mut subject_bytes = vec![0u8; u16::from_be_bytes(subject_len) as usize];
    read_exact_with_progress(recv, &mut subject_bytes, pre_admission.request_read_idle).await?;
    let subject = String::from_utf8(subject_bytes)
        .map_err(|err| TransportError::IncompleteReply(err.to_string()))?;
    let (payload, payload_permit) =
        read_pre_admission_payload(recv, MAX_FRAME_BYTES, pre_admission).await?;
    Ok((subject, payload, payload_permit))
}

async fn read_pre_admission_payload(
    recv: &mut iroh::endpoint::RecvStream,
    max_bytes: usize,
    pre_admission: &PreAdmissionGuardrails,
) -> TransportResult<(Vec<u8>, OwnedSemaphorePermit)> {
    let mut len_bytes = [0u8; 4];
    read_exact_with_progress(recv, &mut len_bytes, pre_admission.request_read_idle).await?;
    let len = u32::from_be_bytes(len_bytes) as usize;
    if len > max_bytes {
        return Err(TransportError::IncompleteReply(format!(
            "frame of {len} bytes exceeds the {max_bytes}-byte ceiling"
        )));
    }
    let payload_permit = pre_admission.try_reserve_payload(len)?;
    let mut payload = vec![0u8; len];
    read_exact_with_progress(recv, &mut payload, pre_admission.request_read_idle).await?;
    Ok((payload, payload_permit))
}

async fn read_exact_with_progress<R: AsyncRead + Unpin>(
    reader: &mut R,
    buffer: &mut [u8],
    idle: Duration,
) -> TransportResult<()> {
    let mut filled = 0;
    while filled < buffer.len() {
        let read = tokio::time::timeout(idle, reader.read(&mut buffer[filled..]))
            .await
            .map_err(|_| {
                TransportError::IncompleteReply(
                    "request frame made no byte progress before its idle deadline".to_string(),
                )
            })?
            .map_err(|err| TransportError::IncompleteReply(err.to_string()))?;
        if read == 0 {
            return Err(TransportError::IncompleteReply(
                "request frame ended before its declared length".to_string(),
            ));
        }
        filled += read;
    }
    Ok(())
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

fn durable_large_request_stage_path(identity_path: &Path) -> PathBuf {
    let mut file_name = identity_path
        .file_name()
        .map(|name| name.to_os_string())
        .unwrap_or_else(|| "contextdb-sync-identity".into());
    file_name.push(".sync-staging");
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
    recv: &mut iroh::endpoint::RecvStream,
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
    tokio::time::timeout(REPLY_RECEIPT_TIMEOUT, async {
        let mut receipt = [0u8; 1];
        recv.read_exact(&mut receipt).await.map_err(|_| {
            TransportError::Unreachable(
                "sync endpoint connection lost before the reply was acknowledged".to_string(),
            )
        })?;
        if receipt[0] != REPLY_RECEIVED_ACK {
            return Err(TransportError::Other(
                "sync peer returned an invalid reply receipt".to_string(),
            ));
        }
        recv.read_to_end(0).await.map_err(|err| match err {
            iroh::endpoint::ReadToEndError::TooLong => TransportError::Other(
                "sync peer sent trailing bytes after the reply receipt".to_string(),
            ),
            iroh::endpoint::ReadToEndError::Read(_) => TransportError::Unreachable(
                "sync endpoint connection lost before the reply receipt stream finished"
                    .to_string(),
            ),
        })?;
        Ok(())
    })
    .await
    .map_err(|_| TransportError::Timeout)??;
    Ok(())
}

#[cfg(test)]
mod sticky_port_tests {
    use super::{
        EndpointSpec, IrohServer, MAX_FRAME_BYTES, MAX_RESPONSE_CONTROL_BYTES,
        MAX_TRACKED_RESPONSE_TRANSFERS, PreAdmissionGuardrails, RESPONSE_TRANSFER_IDLE_TIMEOUT,
        ResponseTransferPaths, SyncRouteState, TransportError, client, client_with_lineage_signer,
        complete_large_response, decode_large_response_control_frame,
        durable_large_request_stage_path, read_exact_with_progress, read_sticky_ports,
        stage_large_response, stage_large_response_with_budget, sync_client_with_lineage_signer,
        validate_large_request_progress, validate_large_response_completion,
    };
    use crate::identity::FabricIdentity;
    use crate::transport::large_request_staging::LargeRequestProgress;
    use crate::transport::large_request_staging::{response_completion_path, response_stage_path};
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Instant;

    fn test_response_paths(name: &str) -> ResponseTransferPaths {
        ResponseTransferPaths {
            stage: std::path::PathBuf::from(format!("responses/{name}")),
            completion: std::path::PathBuf::from(format!("response-completions/{name}")),
        }
    }

    fn regular_file_bytes(path: &std::path::Path) -> u64 {
        std::fs::read_dir(path)
            .expect("read durable response tree")
            .map(|entry| {
                let entry = entry.expect("read durable response entry");
                let kind = entry.file_type().expect("read durable response entry type");
                if kind.is_dir() {
                    regular_file_bytes(&entry.path())
                } else if kind.is_file() {
                    entry
                        .metadata()
                        .expect("read durable response file metadata")
                        .len()
                } else {
                    panic!("durable response test tree contains an unexpected file type")
                }
            })
            .sum()
    }

    #[test]
    fn restart_complete_registration_protects_its_receipt_from_runtime_pressure() {
        let temp = tempfile::tempdir().expect("response stage tempdir");
        let payload = vec![0x62; super::LARGE_RESPONSE_CHUNK_BYTES + 1];
        let first = stage_large_response(
            temp.path(),
            "edge-a",
            "pull",
            *blake3::hash(b"first").as_bytes(),
            &payload,
        )
        .expect("stage response before restart");
        let budget = regular_file_bytes(&response_stage_path(temp.path(), &first));

        let restarted = Arc::new(SyncRouteState::default());
        restarted.begin_serving(Arc::new(HashMap::new()));
        validate_large_response_completion(temp.path(), "edge-a", &first)
            .expect("restart validates the durable completion identity");
        let key = (
            "edge-a".to_string(),
            first.transfer_digest().expect("first transfer digest"),
        );
        let paths = ResponseTransferPaths {
            stage: response_stage_path(temp.path(), &first),
            completion: response_completion_path(temp.path(), &first),
        };
        assert!(restarted.ensure_response_transfer(&key, paths.clone()));
        complete_large_response(temp.path(), "edge-a", &first)
            .expect("restart records durable completion");
        assert!(paths.completion.exists());

        let error = stage_large_response_with_budget(
            temp.path(),
            "edge-a",
            "pull",
            *blake3::hash(b"second").as_bytes(),
            &payload,
            Some(budget),
            &restarted.protected_response_paths(),
        )
        .expect_err("new runtime staging cannot evict the acknowledged-in-flight receipt");
        assert!(
            error
                .to_string()
                .contains("active transfers remain protected")
        );
        assert!(
            paths.completion.exists(),
            "a lost Complete acknowledgement can still retry against its protected receipt"
        );
    }

    #[test]
    fn pre_admission_policy_refuses_excess_connections_and_bytes_without_waiting() {
        let guardrails = PreAdmissionGuardrails::new(2, 7, std::time::Duration::from_secs(30));
        let first_connection = guardrails
            .try_reserve_connection()
            .expect("first connection is admitted");
        let second_connection = guardrails
            .try_reserve_connection()
            .expect("second connection is admitted");
        assert!(guardrails.try_reserve_connection().is_none());

        let first_payload = guardrails
            .try_reserve_payload(4)
            .expect("first payload reserves its exact bytes");
        let second_payload = guardrails
            .try_reserve_payload(3)
            .expect("second payload fills the byte budget");
        assert!(guardrails.try_reserve_payload(1).is_err());

        drop(first_connection);
        assert!(guardrails.try_reserve_connection().is_some());
        drop(first_payload);
        assert!(guardrails.try_reserve_payload(4).is_ok());
        drop(second_connection);
        drop(second_payload);
    }

    #[tokio::test(start_paused = true)]
    async fn request_frame_idle_deadline_uses_virtual_time() {
        let (_writer, mut reader) = tokio::io::duplex(8);
        let read = tokio::spawn(async move {
            let mut byte = [0u8; 1];
            read_exact_with_progress(&mut reader, &mut byte, std::time::Duration::from_secs(30))
                .await
        });
        tokio::task::yield_now().await;
        tokio::time::advance(std::time::Duration::from_secs(30)).await;
        let error = read
            .await
            .expect("idle read task joins")
            .expect_err("idle request frame is refused");
        assert!(error.to_string().contains("no byte progress"));
    }

    #[test]
    fn response_transfer_registry_counts_identical_transfers_independently() {
        let state = Arc::new(SyncRouteState::default());
        state.begin_serving(Arc::new(HashMap::new()));
        let key = ("edge-a".to_string(), [0x31; blake3::OUT_LEN]);
        state
            .reserve_response_transfer()
            .expect("reserve first transfer")
            .activate(key.clone(), test_response_paths("same"));
        state
            .reserve_response_transfer()
            .expect("reserve identical transfer")
            .activate(key.clone(), test_response_paths("same"));
        state.stop_accepting();
        {
            let lifecycle = state.lifecycle.lock().expect("inspect transfer registry");
            assert_eq!(lifecycle.response_transfers[&key].count, 2);
            assert_eq!(lifecycle.draining_response_transfers[&key], 2);
        }
        state.complete_response_transfer(&key);
        {
            let lifecycle = state.lifecycle.lock().expect("inspect first completion");
            assert_eq!(lifecycle.response_transfers[&key].count, 1);
            assert_eq!(lifecycle.draining_response_transfers[&key], 1);
        }
        state.complete_response_transfer(&key);
        let lifecycle = state.lifecycle.lock().expect("inspect final completion");
        assert!(lifecycle.response_transfers.is_empty());
        assert!(lifecycle.draining_response_transfers.is_empty());
        assert_eq!(lifecycle.tracked_response_transfers, 0);
    }

    #[test]
    fn response_transfer_registry_is_bounded_and_reclaims_idle_entries() {
        let state = Arc::new(SyncRouteState::default());
        state.begin_serving(Arc::new(HashMap::new()));
        let mut reservations = (0..MAX_TRACKED_RESPONSE_TRANSFERS)
            .map(|_| {
                state
                    .reserve_response_transfer()
                    .expect("registry accepts work below its ceiling")
            })
            .collect::<Vec<_>>();
        assert!(state.reserve_response_transfer().is_none());
        reservations.pop();
        assert!(state.reserve_response_transfer().is_some());
        drop(reservations);

        let key = ("edge-a".to_string(), [0x42; blake3::OUT_LEN]);
        state
            .reserve_response_transfer()
            .expect("reserve transfer for idle reclaim")
            .activate(key.clone(), test_response_paths("idle"));
        {
            let mut lifecycle = state.lifecycle.lock().expect("age tracked transfer");
            lifecycle
                .response_transfers
                .get_mut(&key)
                .expect("tracked transfer")
                .last_activity = Instant::now()
                .checked_sub(RESPONSE_TRANSFER_IDLE_TIMEOUT)
                .expect("represent idle instant");
        }
        let reservation = state
            .reserve_response_transfer()
            .expect("idle entry is reclaimed before reserving new work");
        let lifecycle = state.lifecycle.lock().expect("inspect reclaimed registry");
        assert!(!lifecycle.response_transfers.contains_key(&key));
        assert_eq!(lifecycle.tracked_response_transfers, 1);
        drop(lifecycle);
        drop(reservation);
    }

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

    fn test_ticket() -> String {
        let secret = iroh::SecretKey::from_bytes(&[9; 32]);
        iroh_tickets::endpoint::EndpointTicket::new(iroh::EndpointAddr::new(secret.public()))
            .to_string()
    }

    #[test]
    fn sync_factory_uses_the_database_adjacent_identity_for_a_bare_ticket() {
        let dir = tempfile::tempdir().expect("tempdir");
        let identity_path = dir.path().join("edge.db.fabric-identity.key");
        let (transport, signer) =
            sync_client_with_lineage_signer(&test_ticket(), Some(identity_path.clone()));

        let expected = FabricIdentity::load_or_generate(&identity_path)
            .expect("read database-adjacent identity")
            .node_id();
        assert!(
            signer.is_some(),
            "durable identity supplies a lineage signer"
        );
        assert!(transport.has_stable_edge_identity());
        assert_eq!(
            transport.local_node_id().as_deref(),
            Some(expected.as_str())
        );
    }

    #[test]
    fn raw_factory_keeps_explicit_identity_stable_and_bare_dialing_ephemeral() {
        let dir = tempfile::tempdir().expect("tempdir");
        let identity_path = dir.path().join("explicit.key");
        let spec = format!(
            "iroh:?to={}&identity={}",
            test_ticket(),
            identity_path.display()
        );
        let (explicit, explicit_signer) = client_with_lineage_signer(&spec);
        let expected = FabricIdentity::load_or_generate(&identity_path)
            .expect("read explicit identity")
            .node_id();
        assert!(explicit_signer.is_some());
        assert!(explicit.has_stable_edge_identity());
        assert_eq!(explicit.local_node_id().as_deref(), Some(expected.as_str()));

        let (bare, bare_signer) = client_with_lineage_signer(&test_ticket());
        assert!(
            bare_signer.is_some(),
            "raw dialing retains its ephemeral signer"
        );
        assert!(bare.local_node_id().is_some());
        assert!(
            !bare.has_stable_edge_identity(),
            "a bare raw dial is not a persisted edge identity"
        );
    }

    #[test]
    fn sync_factory_prefers_an_explicit_identity_over_the_database_default() {
        let dir = tempfile::tempdir().expect("tempdir");
        let explicit_path = dir.path().join("explicit.key");
        let default_path = dir.path().join("edge.db.fabric-identity.key");
        let spec = format!(
            "iroh:?to={}&identity={}",
            test_ticket(),
            explicit_path.display()
        );
        let (transport, signer) =
            sync_client_with_lineage_signer(&spec, Some(default_path.clone()));

        let expected = FabricIdentity::load_or_generate(&explicit_path)
            .expect("read explicit identity")
            .node_id();
        assert!(
            signer.is_some(),
            "explicit durable identity supplies a signer"
        );
        assert!(transport.has_stable_edge_identity());
        assert_eq!(
            transport.local_node_id().as_deref(),
            Some(expected.as_str())
        );
        assert!(
            !default_path.exists(),
            "the database default must not be created when identity= is explicit"
        );
    }

    #[tokio::test]
    async fn sync_factory_refuses_a_bare_ticket_without_a_database_identity() {
        let (transport, signer) = sync_client_with_lineage_signer(&test_ticket(), None);

        assert!(
            signer.is_none(),
            "a bare memory client has no lineage signer"
        );
        assert!(!transport.has_stable_edge_identity());
        let error = transport
            .ensure_connected()
            .await
            .expect_err("a bare memory client is refused before dialing");
        assert!(
            error
                .to_string()
                .contains("cannot load the dialing fabric identity"),
            "refusal explains the missing persisted identity"
        );
    }

    #[test]
    fn parser_accepts_only_positive_server_local_staging_budget() {
        let parsed =
            EndpointSpec::parse(
                "iroh:?identity=/tmp/hub.key&response-staging-bytes=1048576&pre-admission-connections=12&pre-admission-bytes=8388608&request-read-idle-ms=45000",
            )
                .expect("positive staging budget parses");
        assert_eq!(parsed.response_staging_bytes(), Some(1_048_576));
        assert_eq!(parsed.pre_admission_connections(), 12);
        assert_eq!(parsed.pre_admission_bytes(), 8_388_608);
        assert_eq!(
            parsed.request_read_idle(),
            std::time::Duration::from_secs(45)
        );
        assert!(
            EndpointSpec::parse_detailed("iroh:?identity=/tmp/hub.key&response-staging-bytes=0")
                .is_err()
        );
        assert!(
            EndpointSpec::parse_detailed("iroh:?identity=/tmp/hub.key&response-staging-bytes=nope")
                .is_err()
        );
        for parameter in [
            "pre-admission-connections=0",
            "pre-admission-connections=nope",
            "pre-admission-bytes=0",
            "pre-admission-bytes=4294967296",
            "request-read-idle-ms=0",
            "request-read-idle-ms=nope",
        ] {
            assert!(
                EndpointSpec::parse_detailed(&format!("iroh:?identity=/tmp/hub.key&{parameter}"))
                    .is_err(),
                "invalid server-local policy must be refused: {parameter}"
            );
        }
        let secret = iroh::SecretKey::from_bytes(&[7; 32]);
        let ticket =
            iroh_tickets::endpoint::EndpointTicket::new(iroh::EndpointAddr::new(secret.public()))
                .to_string();
        assert!(
            EndpointSpec::parse_detailed(&format!("iroh:?response-staging-bytes=1&to={ticket}"))
                .is_err()
        );
        assert!(
            EndpointSpec::parse_detailed(&format!("iroh:?to={ticket}&response-staging-bytes=1"))
                .is_err()
        );
        for parameter in [
            "pre-admission-connections=1",
            "pre-admission-bytes=1",
            "request-read-idle-ms=1",
        ] {
            assert!(
                EndpointSpec::parse_detailed(&format!("iroh:?to={ticket}&{parameter}")).is_err(),
                "dial specs must refuse server-local policy: {parameter}"
            );
        }
    }

    #[test]
    fn oversized_request_preflight_uses_the_single_attempt_path_only_above_the_frame_ceiling() {
        let transport = client("not-an-iroh-ticket");
        let payload = vec![0u8; MAX_FRAME_BYTES + 1];
        assert!(
            transport
                .ensure_single_reply_retry_safe(&payload[..MAX_FRAME_BYTES])
                .is_ok()
        );
        assert!(matches!(
            transport.ensure_single_reply_retry_safe(&payload),
            Err(TransportError::RetryUnsafe(_))
        ));
    }

    #[test]
    fn response_control_decoder_refuses_malformed_and_oversize_frames() {
        assert!(decode_large_response_control_frame(b"not-msgpack").is_err());
        assert!(
            decode_large_response_control_frame(&vec![0; MAX_RESPONSE_CONTROL_BYTES + 1]).is_err()
        );
    }

    #[test]
    fn oversized_request_progress_requires_its_authenticated_descriptor_and_exact_advance() {
        let descriptor = [0x11; blake3::OUT_LEN];
        let transfer = [0x22; blake3::OUT_LEN];
        let progress = LargeRequestProgress {
            node_id: "edge-a".to_string(),
            descriptor_digest: descriptor,
            transfer_digest: transfer,
            next_missing: 2,
        };
        assert!(
            validate_large_request_progress(&progress, "edge-a", descriptor, transfer, 4, Some(2))
                .is_ok()
        );
        for wrong in [
            LargeRequestProgress {
                node_id: "edge-b".to_string(),
                ..progress.clone()
            },
            LargeRequestProgress {
                descriptor_digest: [0x33; blake3::OUT_LEN],
                ..progress.clone()
            },
            LargeRequestProgress {
                transfer_digest: [0x44; blake3::OUT_LEN],
                ..progress.clone()
            },
            LargeRequestProgress {
                next_missing: 5,
                ..progress.clone()
            },
            LargeRequestProgress {
                next_missing: 1,
                ..progress.clone()
            },
        ] {
            assert!(
                validate_large_request_progress(&wrong, "edge-a", descriptor, transfer, 4, Some(2))
                    .is_err()
            );
        }
    }

    #[tokio::test]
    async fn response_startup_pressure_preserves_unset_state_and_closes_failed_bind() {
        let dir = tempfile::tempdir().expect("tempdir");
        let identity = dir.path().join("hub.key");
        let root = durable_large_request_stage_path(&identity);
        let payload = vec![0x31; super::LARGE_RESPONSE_CHUNK_BYTES + 1];
        let first = stage_large_response(
            &root,
            "edge-a",
            "pull",
            *blake3::hash(b"one").as_bytes(),
            &payload,
        )
        .expect("stage abandoned response");
        let second = stage_large_response(
            &root,
            "edge-a",
            "pull",
            *blake3::hash(b"two").as_bytes(),
            &payload,
        )
        .expect("stage completed response");
        complete_large_response(&root, "edge-a", &second).expect("retain unreleased receipt");
        let spec = format!("iroh:?identity={}", identity.display());
        let server = IrohServer::bind(&spec)
            .await
            .expect("unset threshold preserves state");
        assert!(response_stage_path(&root, &first).exists());
        assert!(response_completion_path(&root, &second).exists());
        server.close().await;
        let pressured = format!("{spec}&response-staging-bytes=1");
        let server = IrohServer::bind(&pressured)
            .await
            .expect("configured startup cleanup");
        assert!(!response_stage_path(&root, &first).exists());
        assert!(!response_completion_path(&root, &second).exists());
        server.close().await;

        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;
            let outside = tempfile::tempdir().expect("outside");
            let _ = std::fs::remove_dir_all(&root);
            symlink(outside.path(), &root).expect("link staging root");
            let port = std::net::UdpSocket::bind("127.0.0.1:0")
                .expect("reserve port")
                .local_addr()
                .expect("port")
                .port();
            let failed_spec = format!(
                "iroh:?identity={}&port={port}&response-staging-bytes=1",
                identity.display()
            );
            assert!(IrohServer::bind(&failed_spec).await.is_err());
            std::fs::remove_file(&root).expect("remove staging-root link");
            let rebound = IrohServer::bind(&failed_spec)
                .await
                .expect("failed sweep closed the explicit port");
            rebound.close().await;
        }
    }
}
