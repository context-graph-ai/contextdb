//! Transport seam: the interface between the sync logic and the thing that
//! moves bytes. Concrete transports live in submodules; the sync logic names
//! only these types.

pub mod in_process;
#[cfg(feature = "iroh")]
pub mod iroh;
#[cfg(feature = "iroh")]
pub(crate) mod iroh_blobs_adapter;
// A neutral-named re-export of the fetch-backend module, established HERE
// (a file physically under src/transport/, so this line never has to be
// re-derived outside the adapter boundary). The resolver's containment
// contract (media_transfer_containment.rs) requires blob_resolver.rs to
// name neither "iroh_blobs" nor any `transport::`-prefixed path beyond the
// sanctioned `IrohServer` import; crate-root wiring in lib.rs consumes this
// alias so the resolver can reach the fetch backend without either.
#[cfg(feature = "iroh")]
pub(crate) use iroh_blobs_adapter as adapter;

// Transport-neutral surface for embedding consumers (e.g. a downstream fabric
// runtime): a peer-endpoint alias plus scheme-free spec builders, so an
// embedding consumer never spells the adapter module name or the `iroh:?`
// URI scheme literal itself. Established HERE, a file physically under the
// adapter boundary, per the same pattern as the `adapter` alias above;
// crate-root wiring in lib.rs re-exports these for consumers.
#[cfg(feature = "iroh")]
pub use iroh::EndpointSpec as PeerEndpointSpec;
#[cfg(feature = "iroh")]
pub use iroh::IrohServer as PeerEndpoint;
#[cfg(feature = "iroh")]
pub use iroh::{bind_spec as peer_bind_spec, dial_spec as peer_dial_spec};

#[cfg(feature = "nats")]
#[path = "nats.rs"]
mod concrete_transport;

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

/// Transport-neutral failure surface. Each adapter maps its own errors into
/// these variants; the sync logic never names a concrete transport.
#[derive(Debug)]
pub enum TransportError {
    /// No server is listening on the channel.
    NoResponder,
    /// The request deadline elapsed with no reply.
    Timeout,
    /// The transport returned a status/control reply instead of bytes.
    Status(String),
    /// The endpoint could not be reached; the string stays operator-actionable
    /// (e.g. it carries the address).
    Unreachable(String),
    /// A fragmented reply started arriving but did not complete.
    IncompleteReply(String),
    /// The adapter would have to use a retry-unsafe send path. This must be
    /// returned before any bytes are sent.
    RetryUnsafe(String),
    /// Any other transport-level failure.
    Other(String),
}

impl fmt::Display for TransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TransportError::NoResponder => write!(f, "no responder on channel"),
            TransportError::Timeout => write!(f, "transport request timed out"),
            TransportError::Status(detail) => write!(f, "transport status reply: {detail}"),
            TransportError::Unreachable(detail) => write!(f, "transport unreachable: {detail}"),
            TransportError::IncompleteReply(detail) => write!(f, "incomplete reply: {detail}"),
            TransportError::RetryUnsafe(detail) => write!(f, "retry-unsafe request: {detail}"),
            TransportError::Other(detail) => write!(f, "transport error: {detail}"),
        }
    }
}

impl std::error::Error for TransportError {}

pub type TransportResult<T> = Result<T, TransportError>;
pub type TransportFuture<'a, T> = Pin<Box<dyn Future<Output = TransportResult<T>> + Send + 'a>>;
pub type TransportStatusFuture<'a> = Pin<Box<dyn Future<Output = bool> + Send + 'a>>;
pub type Responder = Box<dyn FnOnce(Vec<u8>) -> TransportFuture<'static, ()> + Send + 'static>;
pub type RequestHandler =
    Arc<dyn Fn(IncomingRequest) -> TransportFuture<'static, ()> + Send + Sync + 'static>;

pub struct IncomingRequest {
    pub bytes: Vec<u8>,
    pub responder: Responder,
    /// The transport-authenticated identity of the node that sent this
    /// request, when the transport authenticates one (the iroh sync path
    /// carries the dialing endpoint's `node_id`). `None` when the transport
    /// has no authenticated peer identity (the deprecated broker path). The
    /// hub uses it to record a per-node last-contact on every exchange it
    /// serves.
    pub node_id: Option<String>,
}

pub struct HandlerRegistration {
    pub subject: String,
    pub handler: RequestHandler,
}

/// Client (edge) side of the seam: send complete request bytes to a named
/// channel, get complete reply bytes back.
pub trait ClientTransport: Send + Sync {
    fn ensure_connected<'a>(&'a self) -> TransportFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    fn reconnect<'a>(&'a self) -> TransportFuture<'a, ()> {
        self.ensure_connected()
    }

    fn is_connected<'a>(&'a self) -> TransportStatusFuture<'a> {
        Box::pin(async { true })
    }

    /// The transport-authenticated identity of the peer this client talks to,
    /// when the transport authenticates one. The default sync transport dials
    /// its hub BY KEY, so a client always knows who is on the other end; a
    /// transport with no authenticated peer identity (the deprecated broker
    /// path) returns `None` and consequently earns no transfer receipts.
    fn peer_node_id(&self) -> Option<String> {
        None
    }

    /// True only when this client presents one stable authenticated edge
    /// identity to the server. Status's applied-push frontier is meaningful
    /// for status-ahead recovery only under that condition; the remote peer's
    /// identity alone says nothing about which edge the frontier belongs to.
    fn has_stable_edge_identity(&self) -> bool {
        false
    }

    fn request<'a>(
        &'a self,
        subject: &'a str,
        request_bytes: Vec<u8>,
        timeout: Duration,
    ) -> TransportFuture<'a, Vec<u8>>;

    /// Send a request whose response is known by the sync protocol to be one
    /// small message. Adapters may use a cheaper request path that does not
    /// preserve fragmented response collection. Pull must use `request`,
    /// because its reply may be chunked.
    fn request_single_reply<'a>(
        &'a self,
        subject: &'a str,
        request_bytes: Vec<u8>,
        timeout: Duration,
    ) -> TransportFuture<'a, Vec<u8>> {
        self.request(subject, request_bytes, timeout)
    }

    /// Check whether a request with a known single-message response can be
    /// retried safely if no valid reply arrives. This is a borrowed preflight:
    /// adapters must not send bytes from this method.
    fn ensure_single_reply_retry_safe(&self, _request_bytes: &[u8]) -> TransportResult<()> {
        Ok(())
    }

    /// Release transport resources gracefully (connections, sockets) before
    /// process exit. Adapters with owned endpoints override this; the default
    /// is a no-op.
    fn shutdown<'a>(&'a self) -> TransportFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
}

/// Serving (hub) side of the seam: accept complete request bytes on named
/// channels and publish reply bytes back.
pub trait ServerTransport: Send + Sync {
    fn serve<'a>(
        &'a self,
        handlers: Vec<HandlerRegistration>,
        shutdown: Arc<AtomicBool>,
    ) -> TransportFuture<'a, ()>;
}

/// Route an endpoint spec to its adapter. An iroh-shaped spec (a ticket or a
/// bind spec) selects the Iroh adapter; broker-URL specs select the
/// deprecated NATS adapter when it is built, and an actionable error
/// otherwise.
pub fn client_transport(endpoint: &str) -> Arc<dyn ClientTransport> {
    #[cfg(feature = "iroh")]
    match iroh::EndpointSpec::parse_detailed(endpoint) {
        Ok(Some(_)) => return iroh::client(endpoint),
        // An iroh-shaped spec with a typo must error loudly, never fall
        // through to another transport.
        Err(message) => return Arc::new(InvalidSpecTransport { message }),
        Ok(None) => {}
    }
    broker_client_or_disabled(endpoint)
}

pub fn server_transport(endpoint: &str) -> Arc<dyn ServerTransport> {
    #[cfg(feature = "iroh")]
    match iroh::EndpointSpec::parse_detailed(endpoint) {
        Ok(Some(_)) => return iroh::server(endpoint),
        Err(message) => return Arc::new(InvalidSpecTransport { message }),
        Ok(None) => {}
    }
    broker_server_or_disabled(endpoint)
}

/// Both-sides transport for a spec that names this stack's endpoint form but
/// fails to parse: every operation reports the parse error verbatim.
#[cfg(feature = "iroh")]
struct InvalidSpecTransport {
    message: String,
}

#[cfg(feature = "iroh")]
impl ClientTransport for InvalidSpecTransport {
    fn ensure_connected<'a>(&'a self) -> TransportFuture<'a, ()> {
        Box::pin(async move { Err(TransportError::Unreachable(self.message.clone())) })
    }

    fn is_connected<'a>(&'a self) -> TransportStatusFuture<'a> {
        Box::pin(async { false })
    }

    fn request<'a>(
        &'a self,
        _subject: &'a str,
        _request_bytes: Vec<u8>,
        _timeout: Duration,
    ) -> TransportFuture<'a, Vec<u8>> {
        Box::pin(async move { Err(TransportError::Unreachable(self.message.clone())) })
    }
}

#[cfg(feature = "iroh")]
impl ServerTransport for InvalidSpecTransport {
    fn serve<'a>(
        &'a self,
        _handlers: Vec<HandlerRegistration>,
        _shutdown: Arc<AtomicBool>,
    ) -> TransportFuture<'a, ()> {
        Box::pin(async move { Err(TransportError::Unreachable(self.message.clone())) })
    }
}

#[cfg(feature = "nats")]
fn broker_client_or_disabled(endpoint: &str) -> Arc<dyn ClientTransport> {
    concrete_transport::client(endpoint)
}

#[cfg(not(feature = "nats"))]
fn broker_client_or_disabled(endpoint: &str) -> Arc<dyn ClientTransport> {
    Arc::new(DisabledClientTransport {
        endpoint: endpoint.to_string(),
    })
}

#[cfg(feature = "nats")]
fn broker_server_or_disabled(endpoint: &str) -> Arc<dyn ServerTransport> {
    concrete_transport::server(endpoint)
}

#[cfg(not(feature = "nats"))]
fn broker_server_or_disabled(endpoint: &str) -> Arc<dyn ServerTransport> {
    Arc::new(DisabledServerTransport {
        endpoint: endpoint.to_string(),
    })
}

#[cfg(not(feature = "nats"))]
struct DisabledClientTransport {
    endpoint: String,
}

#[cfg(not(feature = "nats"))]
struct DisabledServerTransport {
    endpoint: String,
}

#[cfg(not(feature = "nats"))]
impl ClientTransport for DisabledClientTransport {
    fn ensure_connected<'a>(&'a self) -> TransportFuture<'a, ()> {
        Box::pin(async move { Err(deprecated_broker_not_built(&self.endpoint)) })
    }

    fn is_connected<'a>(&'a self) -> TransportStatusFuture<'a> {
        Box::pin(async { false })
    }

    fn request<'a>(
        &'a self,
        _subject: &'a str,
        _request_bytes: Vec<u8>,
        _timeout: Duration,
    ) -> TransportFuture<'a, Vec<u8>> {
        Box::pin(async move { Err(deprecated_broker_not_built(&self.endpoint)) })
    }
}

#[cfg(not(feature = "nats"))]
fn deprecated_broker_not_built(endpoint: &str) -> TransportError {
    TransportError::Unreachable(format!(
        "{endpoint} is a broker URL, but the deprecated NATS transport is not built; \
         the default transport is the sync endpoint (ticket) form — pass the server's \
         ticket instead, or rebuild with the `nats` cargo feature to use the deprecated broker path"
    ))
}

#[cfg(not(feature = "nats"))]
impl ServerTransport for DisabledServerTransport {
    fn serve<'a>(
        &'a self,
        _handlers: Vec<HandlerRegistration>,
        shutdown: Arc<AtomicBool>,
    ) -> TransportFuture<'a, ()> {
        Box::pin(async move {
            // Fail fast: a server pointed at a broker URL on a build
            // without the deprecated adapter must error immediately, not
            // park silently until shutdown.
            let _ = shutdown;
            Err(deprecated_broker_not_built(&self.endpoint))
        })
    }
}
