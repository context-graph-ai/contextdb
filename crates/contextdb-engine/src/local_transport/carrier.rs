#[cfg(unix)]
use super::FinalStaleIdentityObservation;
#[cfg(unix)]
use super::stale::{observe_final_stale_identity, remove_channel_if_identity_matches};
#[cfg(unix)]
use super::{
    DeadlineOperationWait, FrameViolation, LivenessEvidence, LocalDeadlineOperation,
    LocalHandshake, LocalInboundKind, LocalInboundMessage, LocalOutboundMessage,
    LocalProtocolBoundary, LocalRequestEnvelope, LocalResponse, LocalResponseExpectation,
    LocalTransportError, MAX_FRAME_BYTES, StaleChannelAction, StaleChannelEvidence,
    StaleFilesystemEvidence, StaleIdentityEvidence, connect_with_deadline, frame_read_refusal,
    invalid_channel_data, owner_disconnected, receive_response_with_deadline,
    retry_route_with_deadline, write_request_with_deadline,
};
#[cfg(unix)]
use contextdb_core::read_contract::{DeadlineClock, ReadFailureKind};
#[cfg(unix)]
use std::future::Future;
#[cfg(unix)]
use std::io::{Read, Write};
#[cfg(unix)]
use std::pin::Pin;
#[cfg(unix)]
use std::task::{Context, Poll};

/// The Unix carrier boundary. Channel lifecycle and request servicing remain
/// outside this protocol module.
#[cfg(unix)]
#[derive(Debug, Clone, Copy, Default)]
pub struct UnixLocalCarrier;

#[cfg(all(unix, feature = "test-seams"))]
std::thread_local! {
    static FRAME_READINESS_CREATIONS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
}

/// Test interposition at the final race window. Implementations invoke this
/// only after the last pathname identity inspection and immediately before
/// the identity-bound removal primitive.
#[cfg(unix)]
#[doc(hidden)]
pub trait StaleRemovalInterlock: Send + Sync {
    fn after_final_identity_inspection(
        &self,
        path: &std::path::Path,
        observation: FinalStaleIdentityObservation,
    ) -> Result<(), LocalTransportError>;
}

#[cfg(unix)]
struct NoopStaleRemovalInterlock;

#[cfg(unix)]
impl StaleRemovalInterlock for NoopStaleRemovalInterlock {
    fn after_final_identity_inspection(
        &self,
        _path: &std::path::Path,
        _observation: FinalStaleIdentityObservation,
    ) -> Result<(), LocalTransportError> {
        Ok(())
    }
}

#[cfg(unix)]
impl UnixLocalCarrier {
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn reset_frame_readiness_creations_for_test() {
        FRAME_READINESS_CREATIONS.with(|creations| creations.set(0));
    }

    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn frame_readiness_creations_for_test() -> usize {
        FRAME_READINESS_CREATIONS.with(std::cell::Cell::get)
    }

    pub fn listen(
        &self,
        path: &std::path::Path,
    ) -> Result<std::os::unix::net::UnixListener, LocalTransportError> {
        use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};

        // The channel is bound at the address the kernel can hold, which is
        // not always the pathname the channel lives at. Everything after the
        // bind -- permissions, the re-inspection that proves what was created
        // -- runs against the real pathname, because that is what an operator
        // and every later probe see.
        let kernel_address = super::ChannelKernelAddress::resolve(path)?;
        let listener = std::os::unix::net::UnixListener::bind(kernel_address.as_path())
            .map_err(|error| LocalTransportError::FilesystemInspection(error.to_string()))?;
        std::fs::set_permissions(
            path,
            std::fs::Permissions::from_mode(super::OWNER_ONLY_MODE),
        )
        .map_err(|error| LocalTransportError::FilesystemInspection(error.to_string()))?;
        let metadata = std::fs::symlink_metadata(path)
            .map_err(|error| LocalTransportError::FilesystemInspection(error.to_string()))?;
        let current_user = nix::unistd::geteuid().as_raw() as u64;
        if !metadata.file_type().is_socket()
            || metadata.uid() as u64 != current_user
            || metadata.mode() & 0o777 != super::OWNER_ONLY_MODE
        {
            return Err(LocalTransportError::StaleChannelUnverifiable);
        }
        Ok(listener)
    }

    pub fn connect<'a>(
        &'a self,
        path: &'a std::path::Path,
        clock: &'a dyn DeadlineClock,
        deadline_ms: u64,
    ) -> DeadlineOperationWait<'a, std::os::unix::net::UnixStream> {
        let operation: LocalDeadlineOperation<'a, std::os::unix::net::UnixStream> =
            match connect_readiness(path) {
                Ok(connect) => Box::pin(async move {
                    match connect.await? {
                        ConnectAttempt::Connected(stream) => Ok(stream),
                        // Nothing answered at the pathname. That is not a
                        // disconnection -- there was never an owner on the
                        // other end to lose. Saying so in its own words is
                        // what lets a caller tell "this store has no owner"
                        // from "the owner I was talking to went away", which
                        // are different answers and lead to different places.
                        ConnectAttempt::NoListener | ConnectAttempt::AddressAbsent => {
                            Err(super::refusal(ReadFailureKind::OwnerNotRunning))
                        }
                    }
                }),
                Err(error) => Box::pin(std::future::ready(Err(error))),
            };
        connect_with_deadline(clock, deadline_ms, operation)
    }

    pub fn send_message(
        &self,
        stream: &mut std::os::unix::net::UnixStream,
        boundary: &LocalProtocolBoundary,
        message: LocalOutboundMessage<'_>,
    ) -> Result<(), LocalTransportError> {
        let ordinary_response = matches!(
            message,
            LocalOutboundMessage::Response {
                expectation,
                ..
            } if *expectation == LocalResponseExpectation::OrdinaryResult
        );
        let frame = boundary.encode_frame(message)?;
        let result = write_readiness(stream, frame).and_then(|write| write.wait());
        if result.is_err() && ordinary_response {
            boundary.reset_ordinary_outbound();
        }
        result
    }

    pub fn receive_message(
        &self,
        stream: &mut std::os::unix::net::UnixStream,
        boundary: &LocalProtocolBoundary,
        kind: LocalInboundKind,
    ) -> Result<LocalInboundMessage, LocalTransportError> {
        let payload = frame_readiness(stream)?
            .wait()
            .map_err(frame_read_refusal)?;
        boundary
            .decode_payload(kind, &payload)
            .map_err(|_| invalid_channel_data())
    }

    pub fn write_request<'a>(
        &'a self,
        stream: &'a mut std::os::unix::net::UnixStream,
        boundary: &'a LocalProtocolBoundary,
        request: &'a LocalRequestEnvelope,
        clock: &'a dyn DeadlineClock,
        deadline_ms: u64,
    ) -> DeadlineOperationWait<'a> {
        let operation = match boundary.encode_frame(LocalOutboundMessage::Request(request)) {
            Ok(frame) => match write_readiness(stream, frame) {
                Ok(write) => Box::pin(write) as LocalDeadlineOperation<'a>,
                Err(error) => Box::pin(std::future::ready(Err(error))),
            },
            Err(error) => Box::pin(std::future::ready(Err(error))),
        };
        write_request_with_deadline(clock, deadline_ms, operation)
    }

    pub fn receive_response<'a>(
        &'a self,
        stream: &'a mut std::os::unix::net::UnixStream,
        boundary: &'a LocalProtocolBoundary,
        expectation: LocalResponseExpectation,
        clock: &'a dyn DeadlineClock,
        deadline_ms: u64,
    ) -> DeadlineOperationWait<'a, LocalResponse> {
        let admission_reset = OrdinaryInboundReset::new(
            boundary,
            expectation == LocalResponseExpectation::OrdinaryResult,
        );
        let operation: LocalDeadlineOperation<'a, LocalResponse> = match frame_readiness(stream) {
            Ok(reader) => Box::pin(async move {
                let mut admission_reset = admission_reset;
                let payload = reader.await.map_err(frame_read_refusal)?;
                let inbound = boundary
                    .decode_payload(LocalInboundKind::Response(expectation), &payload)
                    .map_err(|_| invalid_channel_data())?;
                let LocalInboundMessage::Response(response) = inbound else {
                    return Err(invalid_channel_data());
                };
                admission_reset.disarm();
                Ok(response)
            }),
            Err(error) => {
                drop(admission_reset);
                Box::pin(std::future::ready(Err(error)))
            }
        };
        receive_response_with_deadline(clock, deadline_ms, operation)
    }

    pub fn retry_connect<'a>(
        &'a self,
        path: &'a std::path::Path,
        clock: &'a dyn DeadlineClock,
        deadline_ms: u64,
    ) -> DeadlineOperationWait<'a, std::os::unix::net::UnixStream> {
        let operation: LocalDeadlineOperation<'a, std::os::unix::net::UnixStream> =
            match connect_readiness(path) {
                Ok(connect) => Box::pin(async move {
                    match connect.await {
                        Ok(ConnectAttempt::Connected(stream)) => Ok(stream),
                        Ok(ConnectAttempt::NoListener | ConnectAttempt::AddressAbsent) => {
                            std::future::pending().await
                        }
                        Err(error) => Err(error),
                    }
                }),
                Err(error) => Box::pin(std::future::ready(Err(error))),
            };
        retry_route_with_deadline(clock, deadline_ms, operation)
    }

    pub fn probe_stale(
        &self,
        path: &std::path::Path,
        expected_owner: &LocalHandshake,
        clock: &dyn DeadlineClock,
        deadline_ms: u64,
    ) -> Result<StaleChannelEvidence, LocalTransportError> {
        use std::os::unix::fs::FileTypeExt;

        let metadata = std::fs::symlink_metadata(path)
            .map_err(|_| LocalTransportError::StaleChannelUnverifiable)?;
        if !metadata.file_type().is_socket() {
            return Err(LocalTransportError::StaleChannelUnverifiable);
        }
        let filesystem = StaleFilesystemEvidence::Exact(
            super::channel_filesystem_identity(path)
                .map_err(|_| LocalTransportError::StaleChannelUnverifiable)?,
        );
        let connect = connect_readiness(path)?;
        let operation: LocalDeadlineOperation<'_, ConnectAttempt> = Box::pin(connect);
        let attempt = wait_for_wake(connect_with_deadline(clock, deadline_ms, operation));
        let stream = match attempt {
            Ok(ConnectAttempt::Connected(stream)) => stream,
            Ok(ConnectAttempt::NoListener) => {
                return Ok(StaleChannelEvidence {
                    identity: StaleIdentityEvidence::VerifiedOwner,
                    liveness: LivenessEvidence::NoResponderBeforeDeadline,
                    filesystem,
                });
            }
            Ok(ConnectAttempt::AddressAbsent) => {
                return Err(LocalTransportError::StaleChannelUnverifiable);
            }
            Err(error) if error == super::owner_timeout() => {
                return Ok(StaleChannelEvidence {
                    identity: StaleIdentityEvidence::VerifiedOwner,
                    liveness: LivenessEvidence::NoResponderBeforeDeadline,
                    filesystem,
                });
            }
            Err(error) => return Err(error),
        };
        let mut stream = stream;
        match wait_for_wake(super::authenticate_framed_stream_handshake(
            &mut stream,
            expected_owner,
            clock,
            deadline_ms,
        )) {
            Ok(_) => Ok(StaleChannelEvidence {
                identity: StaleIdentityEvidence::VerifiedOwner,
                liveness: LivenessEvidence::LiveResponder,
                filesystem,
            }),
            Err(error) if error == super::owner_timeout() => Ok(StaleChannelEvidence {
                identity: StaleIdentityEvidence::VerifiedOwner,
                liveness: LivenessEvidence::NoResponderBeforeDeadline,
                filesystem,
            }),
            Err(error) => Err(error),
        }
    }

    pub fn remove_after_proven_stale(
        &self,
        path: &std::path::Path,
        evidence: StaleChannelEvidence,
    ) -> Result<StaleChannelAction, LocalTransportError> {
        self.remove_after_proven_stale_with_interlock(path, evidence, &NoopStaleRemovalInterlock)
    }

    #[doc(hidden)]
    pub fn remove_after_proven_stale_with_interlock(
        &self,
        path: &std::path::Path,
        evidence: StaleChannelEvidence,
        interlock: &dyn StaleRemovalInterlock,
    ) -> Result<StaleChannelAction, LocalTransportError> {
        if evidence.identity != StaleIdentityEvidence::VerifiedOwner
            || evidence.liveness != LivenessEvidence::NoResponderBeforeDeadline
            || !matches!(evidence.filesystem, StaleFilesystemEvidence::Exact(_))
        {
            return Ok(StaleChannelAction::Preserve);
        }
        let StaleFilesystemEvidence::Exact(expected_identity) = evidence.filesystem else {
            return Ok(StaleChannelAction::Preserve);
        };
        let observation = observe_final_stale_identity(path)?;
        if observation.identity() != expected_identity {
            return Ok(StaleChannelAction::Preserve);
        }
        interlock.after_final_identity_inspection(path, observation)?;
        remove_channel_if_identity_matches(path, observation)
    }
}

#[cfg(unix)]
struct OrdinaryInboundReset<'a> {
    boundary: &'a LocalProtocolBoundary,
    armed: bool,
}

#[cfg(unix)]
impl<'a> OrdinaryInboundReset<'a> {
    const fn new(boundary: &'a LocalProtocolBoundary, armed: bool) -> Self {
        Self { boundary, armed }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

#[cfg(unix)]
impl Drop for OrdinaryInboundReset<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.boundary.reset_ordinary_inbound();
        }
    }
}

#[cfg(unix)]
struct ReadinessState<T> {
    result: Option<Result<T, LocalTransportError>>,
    waker: Option<std::task::Waker>,
    cancelled: bool,
}

/// A single safe readiness bridge for Unix connect, frame read, and complete
/// frame write operations. The worker blocks in the operating system, while
/// the future stores and wakes the executor task exactly when I/O completes.
#[cfg(unix)]
pub(crate) struct UnixReadiness<T> {
    shared: std::sync::Arc<(std::sync::Mutex<ReadinessState<T>>, std::sync::Condvar)>,
    cancellation: Option<std::os::unix::net::UnixStream>,
    worker: Option<std::thread::JoinHandle<()>>,
    completed: bool,
}

#[cfg(unix)]
impl<T: Send + 'static> UnixReadiness<T> {
    fn ready(result: Result<T, LocalTransportError>) -> Self {
        Self {
            shared: std::sync::Arc::new((
                std::sync::Mutex::new(ReadinessState {
                    result: Some(result),
                    waker: None,
                    cancelled: false,
                }),
                std::sync::Condvar::new(),
            )),
            cancellation: None,
            worker: None,
            completed: false,
        }
    }

    fn spawn(
        name: &'static str,
        cancellation: std::os::unix::net::UnixStream,
        operation: impl FnOnce() -> Result<T, LocalTransportError> + Send + 'static,
    ) -> Result<Self, LocalTransportError> {
        let shared = std::sync::Arc::new((
            std::sync::Mutex::new(ReadinessState {
                result: None,
                waker: None,
                cancelled: false,
            }),
            std::sync::Condvar::new(),
        ));
        let worker_shared = std::sync::Arc::clone(&shared);
        let worker = std::thread::Builder::new()
            .name(name.to_owned())
            .spawn(move || {
                let result = operation();
                let (state, completion) = &*worker_shared;
                let mut state = state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                let waker = if state.cancelled {
                    None
                } else {
                    state.result = Some(result);
                    state.waker.take()
                };
                completion.notify_all();
                drop(state);
                if let Some(waker) = waker {
                    waker.wake();
                }
            })
            .map_err(|error| LocalTransportError::FilesystemInspection(error.to_string()))?;
        Ok(Self {
            shared,
            cancellation: Some(cancellation),
            worker: Some(worker),
            completed: false,
        })
    }

    fn wait(mut self) -> Result<T, LocalTransportError> {
        let result = {
            let (state, completion) = &*self.shared;
            let mut state = state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            loop {
                if let Some(result) = state.result.take() {
                    break result;
                }
                state = completion
                    .wait(state)
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
            }
        };
        self.completed = true;
        self.cancellation.take();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
        result
    }
}

#[cfg(unix)]
impl<T> Future for UnixReadiness<T> {
    type Output = Result<T, LocalTransportError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let result = {
            let (state, _) = &*this.shared;
            let mut state = state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some(result) = state.result.take() {
                Some(result)
            } else {
                if state
                    .waker
                    .as_ref()
                    .is_none_or(|registered| !registered.will_wake(context.waker()))
                {
                    state.waker = Some(context.waker().clone());
                }
                None
            }
        };
        let Some(result) = result else {
            return Poll::Pending;
        };
        this.completed = true;
        this.cancellation.take();
        if let Some(worker) = this.worker.take() {
            let _ = worker.join();
        }
        Poll::Ready(result)
    }
}

#[cfg(unix)]
impl<T> Drop for UnixReadiness<T> {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        let (state, completion) = &*self.shared;
        let mut state = state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.cancelled = true;
        state.waker.take();
        completion.notify_all();
        drop(state);
        if let Some(cancellation) = self.cancellation.take() {
            let _ = cancellation.shutdown(std::net::Shutdown::Both);
        }
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

#[cfg(unix)]
fn cloned_blocking_streams(
    stream: &std::os::unix::net::UnixStream,
) -> Result<
    (
        std::os::unix::net::UnixStream,
        std::os::unix::net::UnixStream,
    ),
    LocalTransportError,
> {
    stream
        .set_nonblocking(false)
        .map_err(|error| LocalTransportError::FilesystemInspection(error.to_string()))?;
    let worker = stream
        .try_clone()
        .map_err(|error| LocalTransportError::FilesystemInspection(error.to_string()))?;
    let cancellation = stream
        .try_clone()
        .map_err(|error| LocalTransportError::FilesystemInspection(error.to_string()))?;
    Ok((worker, cancellation))
}

#[cfg(unix)]
pub(crate) fn frame_readiness(
    stream: &std::os::unix::net::UnixStream,
) -> Result<UnixReadiness<Vec<u8>>, LocalTransportError> {
    let (mut worker_stream, cancellation) = cloned_blocking_streams(stream)?;
    #[cfg(feature = "test-seams")]
    FRAME_READINESS_CREATIONS.with(|creations| {
        creations.set(creations.get().saturating_add(1));
    });
    UnixReadiness::spawn("contextdb-local-read", cancellation, move || {
        let mut prefix = [0_u8; 4];
        worker_stream.read_exact(&mut prefix).map_err(|error| {
            if error.kind() == std::io::ErrorKind::UnexpectedEof {
                LocalTransportError::Frame(FrameViolation::TruncatedLength)
            } else {
                owner_disconnected()
            }
        })?;
        let length = u32::from_be_bytes(prefix) as usize;
        if length > MAX_FRAME_BYTES {
            return Err(LocalTransportError::Frame(
                FrameViolation::LengthExceedsMaximum,
            ));
        }
        let mut payload = vec![0_u8; length];
        worker_stream.read_exact(&mut payload).map_err(|error| {
            if error.kind() == std::io::ErrorKind::UnexpectedEof {
                LocalTransportError::Frame(FrameViolation::TruncatedPayload)
            } else {
                owner_disconnected()
            }
        })?;
        Ok(payload)
    })
}

#[cfg(unix)]
fn write_readiness(
    stream: &std::os::unix::net::UnixStream,
    frame: Vec<u8>,
) -> Result<UnixReadiness<()>, LocalTransportError> {
    let (mut worker_stream, cancellation) = cloned_blocking_streams(stream)?;
    UnixReadiness::spawn("contextdb-local-write", cancellation, move || {
        worker_stream
            .write_all(&frame)
            .map_err(|_| owner_disconnected())
    })
}

#[cfg(unix)]
enum ConnectAttempt {
    Connected(std::os::unix::net::UnixStream),
    NoListener,
    AddressAbsent,
}

#[cfg(unix)]
enum ConnectWait {
    Completion,
    /// Linux reports a saturated Unix listener backlog as `EAGAIN`. This
    /// attempt remains retained until its enclosing absolute deadline cancels it.
    BacklogCapacity,
}

#[cfg(unix)]
fn connect_readiness(
    path: &std::path::Path,
) -> Result<UnixReadiness<ConnectAttempt>, LocalTransportError> {
    use nix::sys::socket::{AddressFamily, SockFlag, SockType, UnixAddr, connect, socket};
    use std::os::fd::AsRawFd;

    // Same address rule as the bind: what the kernel is shown may be the
    // held-directory form, while the pathname stays what everything else uses.
    let kernel_address = super::ChannelKernelAddress::resolve(path)?;
    let address = UnixAddr::new(kernel_address.as_path())
        .map_err(|error| LocalTransportError::FilesystemInspection(error.to_string()))?;
    let socket = socket(
        AddressFamily::Unix,
        SockType::Stream,
        SockFlag::SOCK_CLOEXEC | SockFlag::SOCK_NONBLOCK,
        None,
    )
    .map_err(|error| LocalTransportError::FilesystemInspection(error.to_string()))?;
    let stream = std::os::unix::net::UnixStream::from(socket);
    let wait = match connect(stream.as_raw_fd(), &address) {
        Ok(()) | Err(nix::errno::Errno::EISCONN) => {
            return Ok(UnixReadiness::ready(finish_nonblocking_connect(stream)));
        }
        Err(nix::errno::Errno::EINPROGRESS | nix::errno::Errno::EALREADY) => {
            ConnectWait::Completion
        }
        Err(nix::errno::Errno::EAGAIN) => ConnectWait::BacklogCapacity,
        Err(error) => return Ok(UnixReadiness::ready(classify_connect_error(error))),
    };
    let (cancellation_reader, cancellation_writer) = std::os::unix::net::UnixStream::pair()
        .map_err(|error| LocalTransportError::FilesystemInspection(error.to_string()))?;
    UnixReadiness::spawn(
        "contextdb-local-connect",
        cancellation_writer,
        move || match wait {
            ConnectWait::Completion => wait_for_nonblocking_connect(stream, cancellation_reader),
            ConnectWait::BacklogCapacity => {
                wait_for_backlog_cancellation(stream, cancellation_reader)
            }
        },
    )
}

#[cfg(unix)]
fn wait_for_nonblocking_connect(
    stream: std::os::unix::net::UnixStream,
    cancellation: std::os::unix::net::UnixStream,
) -> Result<ConnectAttempt, LocalTransportError> {
    use nix::errno::Errno;
    use nix::poll::{PollFd, PollFlags, PollTimeout, poll};
    use std::os::fd::AsFd;

    let (cancellation_events, socket_events) = loop {
        let mut readiness = [
            PollFd::new(
                cancellation.as_fd(),
                PollFlags::POLLIN | PollFlags::POLLERR | PollFlags::POLLHUP,
            ),
            PollFd::new(
                stream.as_fd(),
                PollFlags::POLLOUT | PollFlags::POLLERR | PollFlags::POLLHUP,
            ),
        ];
        match poll(&mut readiness, PollTimeout::NONE) {
            Ok(0) => {
                return Err(LocalTransportError::FilesystemInspection(
                    "Unix connect readiness ended without an event".to_owned(),
                ));
            }
            Ok(_) => break (readiness[0].revents(), readiness[1].revents()),
            Err(Errno::EINTR) => continue,
            Err(error) => {
                return Err(LocalTransportError::FilesystemInspection(error.to_string()));
            }
        }
    };
    if cancellation_events.is_some_and(|events| !events.is_empty()) {
        return Err(owner_disconnected());
    }
    let socket_events = socket_events.ok_or_else(|| {
        LocalTransportError::FilesystemInspection(
            "Unix connect readiness returned unknown events".to_owned(),
        )
    })?;
    if !socket_events.intersects(PollFlags::POLLOUT | PollFlags::POLLERR | PollFlags::POLLHUP) {
        return Err(LocalTransportError::FilesystemInspection(
            "Unix connect readiness returned no completion event".to_owned(),
        ));
    }
    match pending_socket_error(&stream)? {
        None | Some(Errno::EISCONN) => verify_connected_stream(stream),
        Some(Errno::EAGAIN) => wait_for_backlog_cancellation(stream, cancellation),
        Some(error) => classify_connect_error(error),
    }
}

#[cfg(unix)]
fn wait_for_backlog_cancellation(
    _retained_stream: std::os::unix::net::UnixStream,
    cancellation: std::os::unix::net::UnixStream,
) -> Result<ConnectAttempt, LocalTransportError> {
    use nix::errno::Errno;
    use nix::poll::{PollFd, PollFlags, PollTimeout, poll};
    use std::os::fd::AsFd;

    loop {
        let mut readiness = [PollFd::new(
            cancellation.as_fd(),
            PollFlags::POLLIN | PollFlags::POLLERR | PollFlags::POLLHUP,
        )];
        match poll(&mut readiness, PollTimeout::NONE) {
            Ok(0) => {
                return Err(LocalTransportError::FilesystemInspection(
                    "Unix connect cancellation ended without an event".to_owned(),
                ));
            }
            Ok(_) => return Err(owner_disconnected()),
            Err(Errno::EINTR) => continue,
            Err(error) => {
                return Err(LocalTransportError::FilesystemInspection(error.to_string()));
            }
        }
    }
}

#[cfg(unix)]
fn finish_nonblocking_connect(
    stream: std::os::unix::net::UnixStream,
) -> Result<ConnectAttempt, LocalTransportError> {
    match pending_socket_error(&stream)? {
        None | Some(nix::errno::Errno::EISCONN) => verify_connected_stream(stream),
        Some(error) => classify_connect_error(error),
    }
}

#[cfg(unix)]
fn pending_socket_error(
    stream: &std::os::unix::net::UnixStream,
) -> Result<Option<nix::errno::Errno>, LocalTransportError> {
    use nix::sys::socket::{getsockopt, sockopt};

    let error = getsockopt(stream, sockopt::SocketError)
        .map_err(|error| LocalTransportError::FilesystemInspection(error.to_string()))?;
    Ok((error != 0).then_some(nix::errno::Errno::from_raw(error)))
}

#[cfg(unix)]
fn verify_connected_stream(
    stream: std::os::unix::net::UnixStream,
) -> Result<ConnectAttempt, LocalTransportError> {
    use nix::sys::socket::{UnixAddr, getpeername};
    use std::os::fd::AsRawFd;

    getpeername::<UnixAddr>(stream.as_raw_fd())
        .map_err(|error| LocalTransportError::FilesystemInspection(error.to_string()))?;
    stream
        .set_nonblocking(false)
        .map_err(|error| LocalTransportError::FilesystemInspection(error.to_string()))?;
    Ok(ConnectAttempt::Connected(stream))
}

#[cfg(unix)]
fn classify_connect_error(error: nix::errno::Errno) -> Result<ConnectAttempt, LocalTransportError> {
    match error {
        nix::errno::Errno::ECONNREFUSED => Ok(ConnectAttempt::NoListener),
        nix::errno::Errno::ENOENT => Ok(ConnectAttempt::AddressAbsent),
        other => Err(LocalTransportError::FilesystemInspection(other.to_string())),
    }
}

#[cfg(unix)]
fn wait_for_wake<T>(future: impl Future<Output = T>) -> T {
    struct ThreadWake(std::thread::Thread);

    impl std::task::Wake for ThreadWake {
        fn wake(self: std::sync::Arc<Self>) {
            self.0.unpark();
        }

        fn wake_by_ref(self: &std::sync::Arc<Self>) {
            self.0.unpark();
        }
    }

    let waker = std::task::Waker::from(std::sync::Arc::new(ThreadWake(std::thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = Box::pin(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending => std::thread::park(),
        }
    }
}
