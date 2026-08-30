use super::LocalTransportError;
#[cfg(unix)]
use super::carrier::frame_readiness;
#[cfg(unix)]
use super::framing::{LocalHandshake, LocalReadDeclaration, validate_handshake};
#[cfg(unix)]
use super::{DeadlineOperationWait, LocalDeadlineOperation, handshake_with_deadline};
#[cfg(unix)]
use contextdb_core::read_contract::DeadlineClock;
use contextdb_core::read_contract::{
    LocalUserIdentity, ReadFailure, ReadFailureDetail, ReadFailureKind,
};

/// A verified principal supplied by a local carrier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadPrincipal {
    LocalUser(LocalUserIdentity),
}

/// Apply the fixed same-operating-system-user authorization rule after the
/// carrier has obtained credentials.
pub fn authenticate_peer(
    owner: LocalUserIdentity,
    observed: Result<LocalUserIdentity, LocalTransportError>,
) -> Result<ReadPrincipal, LocalTransportError> {
    let observed = observed?;
    if observed == owner {
        Ok(ReadPrincipal::LocalUser(observed))
    } else {
        let refusal = ReadFailure::new(ReadFailureKind::OwnerUserMismatch, ReadFailureDetail::None)
            .expect("owner user mismatch accepts an empty detail");
        Err(LocalTransportError::Refusal(refusal))
    }
}

#[cfg(target_os = "linux")]
pub fn peer_user_from_stream(
    stream: &std::os::unix::net::UnixStream,
) -> Result<LocalUserIdentity, LocalTransportError> {
    use nix::sys::socket::{getsockopt, sockopt};

    let credentials = getsockopt(stream, sockopt::PeerCredentials)
        .map_err(|_| LocalTransportError::CredentialsUnavailable)?;
    Ok(LocalUserIdentity(credentials.uid() as u64))
}

#[cfg(target_os = "macos")]
pub fn peer_user_from_stream(
    stream: &std::os::unix::net::UnixStream,
) -> Result<LocalUserIdentity, LocalTransportError> {
    let (user, _) =
        nix::unistd::getpeereid(stream).map_err(|_| LocalTransportError::CredentialsUnavailable)?;
    Ok(LocalUserIdentity(user.as_raw() as u64))
}

#[cfg(all(unix, not(any(target_os = "linux", target_os = "macos"))))]
pub fn peer_user_from_stream(
    _stream: &std::os::unix::net::UnixStream,
) -> Result<LocalUserIdentity, LocalTransportError> {
    Err(LocalTransportError::CredentialsUnavailable)
}

#[cfg(not(unix))]
pub fn peer_user_from_stream<T>(_: &T) -> Result<LocalUserIdentity, LocalTransportError> {
    Err(LocalTransportError::CredentialsUnavailable)
}

/// Compose the carrier-provided Unix peer credential with the protocol
/// handshake before a request can enter admission.
#[cfg(unix)]
pub fn authenticate_stream_handshake(
    stream: &std::os::unix::net::UnixStream,
    expected: &LocalHandshake,
    presented: &LocalHandshake,
) -> Result<ReadPrincipal, LocalTransportError> {
    let peer_user = peer_user_from_stream(stream)?;
    let principal = authenticate_peer(expected.owner_user, Ok(peer_user))?;
    validate_handshake(expected, presented, peer_user)?;
    Ok(principal)
}

/// One admitted reader: who the operating system says it is, and what its
/// handshake declared about the visibility its session reads under.
///
/// The two travel together because they arrive together and are decided
/// together -- the declaration is only worth reading once the peer presenting
/// it has been authenticated -- but they answer different questions. The
/// principal says whether this peer may be served at all; the declaration says
/// how narrow a view it asked to be served through, and the owner decides what
/// to do with it rather than trusting it: it intersects the declared contexts
/// and scope labels with its own access, and it either reads as the declared
/// identity or refuses the session, never serving its own wider identity in
/// place of the declared one.
#[cfg(unix)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmittedReader {
    pub principal: ReadPrincipal,
    pub declared: Option<LocalReadDeclaration>,
}

/// Read one bounded length-prefixed handshake from the connected carrier,
/// extract the operating-system peer credential, and validate every recorded
/// owner field before request admission.
///
/// The operating-system peer user is decided first, before any handshake read
/// worker exists: a peer belonging to another user is refused without reading
/// a byte from it, so it can neither hold the handshake deadline open nor make
/// the channel spend a frame reader on it.
#[cfg(unix)]
pub fn authenticate_framed_stream_handshake<'a>(
    stream: &'a mut std::os::unix::net::UnixStream,
    expected: &'a LocalHandshake,
    clock: &'a dyn DeadlineClock,
    deadline_ms: u64,
) -> DeadlineOperationWait<'a, AdmittedReader> {
    let admitted_peer = peer_user_from_stream(stream).and_then(|peer_user| {
        let principal = authenticate_peer(expected.owner_user, Ok(peer_user))?;
        Ok((peer_user, principal))
    });
    let operation: LocalDeadlineOperation<'a, AdmittedReader> = match admitted_peer {
        Err(error) => Box::pin(std::future::ready(Err(error))),
        Ok((peer_user, principal)) => match frame_readiness(stream) {
            Err(error) => Box::pin(std::future::ready(Err(error))),
            Ok(reader) => Box::pin(async move {
                let bytes = reader.await.map_err(|_| super::invalid_channel_data())?;
                // A decode that ended in a REFUSAL has already named what was
                // wrong -- a peer speaking another protocol version is the
                // case that matters -- and that word is the one this peer is
                // owed. Only bytes that are not a handshake at all become the
                // channel-data refusal.
                let presented = super::decode_handshake_exact(&bytes).map_err(|error| {
                    if matches!(error, LocalTransportError::Refusal(_)) {
                        error
                    } else {
                        super::invalid_channel_data()
                    }
                })?;
                validate_handshake(expected, &presented, peer_user)?;
                // The declaration is read only now, after this peer has been
                // admitted: nothing a refused peer said about itself is ever
                // carried forward.
                Ok(AdmittedReader {
                    principal,
                    declared: presented.declared,
                })
            }),
        },
    };
    handshake_with_deadline(clock, deadline_ms, operation)
}
