//! Thin owner client over one selected local carrier connection.

use super::{OwnerReadScaffoldError, OwnerReadScaffoldResult, response_expectation};
use crate::local_transport::{
    FrameBufferAllocator, FrameReader, LocalHandshake, LocalInboundKind, LocalInboundMessage,
    LocalOutboundMessage, LocalProtocolBoundary, LocalRequest, LocalRequestEnvelope, LocalResponse,
    LocalResponseExpectation, OrdinaryResultReceiver, ResultReceiveOutcome,
};
#[cfg(unix)]
use crate::local_transport::{LocalDeadlineOperation, UnixLocalCarrier, handshake_with_deadline};
use crate::read_progress::ReadProgressObserver;
use contextdb_core::read_contract::{
    DeadlineClock, OwnerReadCancellation, ReadClientTimeouts, ReadLimits,
};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

#[cfg(unix)]
use std::net::Shutdown;
#[cfg(unix)]
use std::os::unix::net::UnixStream;

/// Internal owner-route client. It owns one actual carrier stream and has no
/// direct-file dependency, service pointer, fallback, or rerouting state.
pub struct OwnerClient {
    channel_path: PathBuf,
    presented_handshake: LocalHandshake,
    timeouts: ReadClientTimeouts,
    clock: Arc<dyn DeadlineClock>,
    requests_sent: u64,
    cancellations: Arc<CancelCoordination>,
    /// What the owner said about itself when it accepted this reader.
    accepted_status: Option<contextdb_core::read_contract::OwnerReadStatus>,
    #[cfg(feature = "test-seams")]
    response_frames: Option<Arc<dyn Fn(bool) + Send + Sync>>,
    #[cfg(unix)]
    stream: UnixStream,
}

impl std::fmt::Debug for OwnerClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OwnerClient")
            .field("channel_path", &self.channel_path)
            .field("presented_handshake", &self.presented_handshake)
            .field("timeouts", &self.timeouts)
            .finish_non_exhaustive()
    }
}

impl OwnerClient {
    /// Connect only through the production carrier and its named connect
    /// deadline. The operating-system principal is deliberately absent from
    /// this API; only the accepting carrier may derive it.
    #[cfg(unix)]
    pub async fn connect(
        channel_path: impl AsRef<Path>,
        presented_handshake: LocalHandshake,
        timeouts: ReadClientTimeouts,
        clock: Arc<dyn DeadlineClock>,
    ) -> OwnerReadScaffoldResult<Self> {
        timeouts.validate()?;
        let channel_path = channel_path.as_ref().to_path_buf();
        let deadline_ms = checked_deadline(clock.as_ref(), timeouts.connect_ms)?;
        let carrier = UnixLocalCarrier;
        let mut stream = carrier
            .connect(&channel_path, clock.as_ref(), deadline_ms)
            .await
            .map_err(OwnerReadScaffoldError::from_local)?;
        let boundary = LocalProtocolBoundary::with_effective_limits(ReadLimits::default());
        let handshake: LocalDeadlineOperation<'_> = Box::pin(async {
            UnixLocalCarrier::send_message(
                &carrier,
                &mut stream,
                &boundary,
                LocalOutboundMessage::Handshake(&presented_handshake),
            )
        });
        handshake_with_deadline(clock.as_ref(), deadline_ms, handshake)
            .await
            .map_err(OwnerReadScaffoldError::from_local)?;
        let mut client = Self {
            channel_path,
            presented_handshake,
            timeouts,
            clock,
            requests_sent: 0,
            cancellations: Arc::new(CancelCoordination::default()),
            accepted_status: None,
            #[cfg(feature = "test-seams")]
            response_frames: None,
            stream,
        };
        client.confirm_the_owner_accepted_this_reader().await?;
        Ok(client)
    }

    /// Do not hand back a connected client until the owner has said it accepts
    /// this reader.
    ///
    /// Sending the handshake is not the same as having it accepted: the owner
    /// reads it when the connection is accepted and answers a peer it refuses,
    /// but a client that returns as soon as the bytes are written would hand
    /// its caller a "selected" route and only discover the refusal at the
    /// first real read. Choosing a route is a decision that has to be final
    /// when it is made, so the owner's verdict is collected here.
    ///
    /// The verdict is collected by asking the owner what it is -- a question
    /// it answers without taking an admission slot, so an owner at capacity is
    /// still able to accept or refuse a reader.
    #[cfg(unix)]
    async fn confirm_the_owner_accepted_this_reader(&mut self) -> OwnerReadScaffoldResult<()> {
        let answered = self
            .request(LocalRequestEnvelope {
                limits: ReadLimits::default(),
                request: LocalRequest::OwnerStatus,
            })
            .await?;
        for response in answered {
            match response {
                // Kept, not just counted: the reader choosing a route on this
                // connection has to know whether this owner is actually
                // serving, and this answer is the owner's own word for it.
                LocalResponse::OwnerStatus { status } => {
                    self.accepted_status = Some(status.status);
                    return Ok(());
                }
                LocalResponse::Failure { failure } => {
                    return Err(OwnerReadScaffoldError::Refused(failure));
                }
                _ => {}
            }
        }
        Err(OwnerReadScaffoldError::Refused(
            contextdb_core::read_contract::ReadFailure::new(
                contextdb_core::read_contract::ReadFailureKind::OwnerDisconnected,
                contextdb_core::read_contract::ReadFailureDetail::None,
            )
            .expect("an owner-disconnected refusal carries no further detail"),
        ))
    }

    #[cfg(not(unix))]
    pub async fn connect(
        channel_path: impl AsRef<Path>,
        presented_handshake: LocalHandshake,
        timeouts: ReadClientTimeouts,
        clock: Arc<dyn DeadlineClock>,
    ) -> OwnerReadScaffoldResult<Self> {
        let _ = (channel_path.as_ref(), presented_handshake, timeouts, clock);
        Err(OwnerReadScaffoldError::unimplemented(
            "local owner client carrier on this platform",
        ))
    }

    pub fn channel_path(&self) -> &Path {
        &self.channel_path
    }

    /// How the owner described itself when it accepted this reader.
    ///
    /// An owner that is winding down still answers a question about itself,
    /// and it will still refuse the first statement anybody asks it. A caller
    /// deciding whether this connection is worth keeping reads the owner's own
    /// word here rather than finding out at its first read.
    pub fn accepted_status(&self) -> Option<contextdb_core::read_contract::OwnerReadStatus> {
        self.accepted_status.clone()
    }

    /// Watch this connection's reply frames arrive, one call per frame, with
    /// the frame's own terminality. A caller that must act between the frames
    /// of one answer -- to prove a partial answer is discarded when its owner
    /// dies mid-reply -- has nowhere else to stand, and re-reading the carrier
    /// elsewhere would be a second transport.
    #[cfg(feature = "test-seams")]
    #[doc(hidden)]
    pub fn observe_response_frames_for_test(
        &mut self,
        observer: Option<Arc<dyn Fn(bool) + Send + Sync>>,
    ) {
        self.response_frames = observer;
    }

    /// Decode one request only after the length prefix has passed the shared
    /// pre-allocation admission check. This reads through the same typed
    /// protocol boundary the carrier uses, but keeps the refusal decode
    /// itself produced (e.g. one naming an exceeded memory ceiling) instead
    /// of the carrier's own blanket collapse for unauthenticated socket
    /// peers -- this is not a live connection, so that collapse does not
    /// apply here.
    pub fn read_envelope(
        reader: &mut dyn FrameReader,
        allocator: &mut dyn FrameBufferAllocator,
    ) -> OwnerReadScaffoldResult<LocalRequestEnvelope> {
        let boundary = LocalProtocolBoundary::with_effective_limits(ReadLimits::default());
        match boundary
            .receive_frame_preserving_refusal(LocalInboundKind::Request, reader, allocator)
            .map_err(OwnerReadScaffoldError::from_local)?
        {
            LocalInboundMessage::Request(envelope) => Ok(envelope),
            LocalInboundMessage::Handshake(_) | LocalInboundMessage::Response(_) => {
                Err(OwnerReadScaffoldError::from_local(
                    crate::local_transport::LocalTransportError::Unimplemented,
                ))
            }
        }
    }

    /// Encode a request through the shared payload and frame codecs, via the
    /// same typed protocol boundary the carrier uses to write one.
    pub fn encode_envelope(envelope: &LocalRequestEnvelope) -> OwnerReadScaffoldResult<Vec<u8>> {
        let boundary = LocalProtocolBoundary::with_effective_limits(ReadLimits::default());
        boundary
            .encode_frame(LocalOutboundMessage::Request(envelope))
            .map_err(OwnerReadScaffoldError::from_local)
    }

    /// Send one dynamic request and receive its complete response only over
    /// the selected carrier. Ordinary chunks remain in this private vector
    /// until terminal success; any failure or carrier error drops them.
    #[cfg(unix)]
    pub async fn request(
        &mut self,
        envelope: LocalRequestEnvelope,
    ) -> OwnerReadScaffoldResult<Vec<LocalResponse>> {
        self.request_watching(envelope, None, None).await
    }

    /// Send one request, hear what the owner's read is doing while it runs,
    /// and interrupt it if the caller decides to.
    ///
    /// Both halves are the same exchange, not a second connection: the
    /// owner's nonterminal progress frames are handed to the caller's
    /// observer as they arrive, and a cancellation the caller makes -- from
    /// inside that observer or from anywhere else -- is sent to the owner as
    /// an interrupt naming THIS request, over a second handle onto the same
    /// carrier, while this side is still blocked awaiting the reply. Nothing
    /// here polls or sleeps: the reader acts on frames the owner sends, and
    /// the owner reports at the same interval it does for a caller in its own
    /// process.
    #[cfg(unix)]
    pub async fn request_watching(
        &mut self,
        envelope: LocalRequestEnvelope,
        progress: Option<&Arc<dyn ReadProgressObserver>>,
        cancellation: Option<&OwnerReadCancellation>,
    ) -> OwnerReadScaffoldResult<Vec<LocalResponse>> {
        // A statement its caller has already withdrawn is not sent at all.
        // Sending it and interrupting it a moment later makes the answer a
        // race the caller cannot see or influence: an owner that finishes the
        // read before the interrupt lands hands back rows for a statement
        // nobody is waiting for any more. A cancelled statement is never
        // executed and never answered.
        if cancellation.is_some_and(OwnerReadCancellation::is_cancelled) {
            return Err(OwnerReadScaffoldError::Database(
                contextdb_core::Error::ReadCancelled,
            ));
        }
        let deadline_ms = checked_deadline(self.clock.as_ref(), self.timeouts.response_ms)?;
        // The ordinal this request will carry, read before it is sent so the
        // interrupt below names the very request this call is about and never
        // the one after it. The handle is a second one onto the same carrier,
        // taken here because the client itself is about to be busy reading.
        let interrupt = match cancellation {
            Some(_) => Some((self.next_request_ordinal(), self.cancel_handle()?)),
            None => None,
        };
        self.requests_sent = self.requests_sent.saturating_add(1);
        let boundary = LocalProtocolBoundary::with_effective_limits(envelope.limits);
        let expectation = response_expectation(&envelope.request);
        let carrier = UnixLocalCarrier;
        // An owner that refuses a connection answers it and then closes, so
        // the request going out and the answer coming back cross: the write
        // can fail on a socket the owner has already closed while the answer
        // it sent is sitting unread in this reader's buffer. Reporting the
        // closed socket here would throw that answer away and tell the caller
        // the owner went away, when the owner in fact told them exactly what
        // was wrong with what they presented -- the one thing they can act
        // on. So a write that fails for that reason falls through to the read
        // below; only a read that finds nothing reports a departed owner.
        match UnixLocalCarrier::write_request(
            &carrier,
            &mut self.stream,
            &boundary,
            &envelope,
            self.clock.as_ref(),
            deadline_ms,
        )
        .await
        {
            Ok(()) => {}
            Err(error) if crate::local_transport::is_owner_disconnected(&error) => {}
            Err(error) => return Err(OwnerReadScaffoldError::from_local(error)),
        }

        // Armed only once the request is on the wire, because an interrupt
        // that arrives BEFORE the request it names is an interrupt for
        // nothing: the owner has no work by that name yet, drops it, and the
        // read it was meant to stop then runs to the end. A caller that had
        // already cancelled before it asked is told at once, right here, so
        // the interrupt still goes out immediately -- just behind the request
        // rather than ahead of it.
        //
        // The token tells this listener; the listener writes the interrupt
        // while this thread is still blocked reading the reply. Nothing polls,
        // nothing sleeps, and nothing here depends on the owner sending
        // anything: a read that reports nothing is interrupted exactly as well
        // as one that reports constantly.
        let _interrupt = match (interrupt, cancellation) {
            (Some((ordinal, handle)), Some(cancellation)) => {
                let handle = Mutex::new(Some(handle));
                let limits = envelope.limits;
                Some(cancellation.tell_on_cancel(move || {
                    // Taken, so one token cancelled twice sends one interrupt:
                    // a second would name a request the owner has already
                    // stopped.
                    let taken = handle
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .take();
                    if let Some(handle) = taken {
                        let _sent = handle.cancel(limits, ordinal);
                    }
                }))
            }
            _ => None,
        };

        let mut unpublished = Vec::new();
        let mut ordinary_receiver = if expectation == LocalResponseExpectation::OrdinaryResult {
            Some(
                OrdinaryResultReceiver::with_effective_ceilings(
                    envelope.limits.result_bytes,
                    envelope.limits.memory,
                )
                .map_err(OwnerReadScaffoldError::from_local)?,
            )
        } else {
            None
        };
        // This thread is now the one that takes frames off this connection, so
        // it is the one that will see an owner's acknowledgement -- and the
        // one that must never be made to wait for it. Whoever cancels from
        // another thread waits here; whoever cancels from inside this loop's
        // own observer callback cannot, and is told so by this registration.
        let _reading = self.cancellations.clone().reading_here();
        loop {
            let response = match UnixLocalCarrier::receive_response(
                &carrier,
                &mut self.stream,
                &boundary,
                expectation.clone(),
                self.clock.as_ref(),
                deadline_ms,
            )
            .await
            {
                Ok(response) => response,
                Err(error) => {
                    if let Some(receiver) = ordinary_receiver.as_mut() {
                        let _discarded = receiver
                            .disconnect()
                            .map_err(OwnerReadScaffoldError::from_local)?;
                    }
                    return Err(OwnerReadScaffoldError::from_local(error));
                }
            };
            if let LocalResponse::CancelApplied { request_ordinal } = &response {
                // Nonterminal, like a progress frame: it ends nothing and
                // carries no part of the answer. It releases the thread that
                // cancelled, which is waiting to know the owner has really
                // stopped the work it named.
                self.cancellations.note_applied(*request_ordinal);
                continue;
            }
            if let LocalResponse::Progress { progress: reported } = &response {
                // Nonterminal: it belongs to this request but carries no part
                // of the answer, so the assembly the owner is still building
                // stays untouched and the exchange goes on.
                if let Some(observer) = progress {
                    observer.progress(*reported);
                }
                continue;
            }
            if let Some(receiver) = ordinary_receiver.as_mut() {
                let outcome = receiver
                    .receive(response.clone())
                    .map_err(OwnerReadScaffoldError::from_local)?;
                #[cfg(feature = "test-seams")]
                if let Some(observe) = self.response_frames.as_ref() {
                    observe(!matches!(outcome, ResultReceiveOutcome::Pending));
                }
                match outcome {
                    ResultReceiveOutcome::Pending => unpublished.push(response),
                    ResultReceiveOutcome::Published(_) => {
                        unpublished.push(response);
                        return Ok(unpublished);
                    }
                    ResultReceiveOutcome::Failed(failure) => {
                        return Err(OwnerReadScaffoldError::Refused(failure));
                    }
                    ResultReceiveOutcome::EngineFailed(failure) => {
                        return Err(OwnerReadScaffoldError::Database(
                            failure.into_error(envelope.limits.memory),
                        ));
                    }
                    ResultReceiveOutcome::Disconnected => {
                        return Err(OwnerReadScaffoldError::unimplemented(
                            "ordinary terminal receiver disconnected without carrier EOF/HUP",
                        ));
                    }
                }
            } else {
                #[cfg(feature = "test-seams")]
                if let Some(observe) = self.response_frames.as_ref() {
                    observe(true);
                }
                match response {
                    // A leading piece of an answer larger than one local
                    // frame. The boundary below is holding it and will hand
                    // the whole answer back on the frame that ends the
                    // exchange, so nothing is decided here.
                    LocalResponse::ResultChunk { .. } => continue,
                    LocalResponse::Failure { failure } => {
                        return Err(OwnerReadScaffoldError::Refused(failure));
                    }
                    LocalResponse::EngineFailure { failure } => {
                        return Err(OwnerReadScaffoldError::Database(
                            failure.into_error(envelope.limits.memory),
                        ));
                    }
                    response => return Ok(vec![response]),
                }
            }
        }
    }

    #[cfg(not(unix))]
    pub async fn request(
        &mut self,
        envelope: LocalRequestEnvelope,
    ) -> OwnerReadScaffoldResult<Vec<LocalResponse>> {
        let _ = envelope;
        Err(OwnerReadScaffoldError::unimplemented(
            "local owner request carrier on this platform",
        ))
    }

    /// A chunk remains private to the shared terminal receiver until a
    /// `TerminalSuccess` arrives.
    pub fn receive_ordinary(
        receiver: &mut OrdinaryResultReceiver,
        response: LocalResponse,
    ) -> OwnerReadScaffoldResult<ResultReceiveOutcome> {
        Ok(receiver.receive(response)?)
    }

    /// The ordinal the next request on this connection will carry. A reader
    /// takes this before it blocks so an interrupt can name the very request
    /// it is waiting on, never the one after it.
    pub const fn next_request_ordinal(&self) -> u64 {
        self.requests_sent.saturating_add(1)
    }

    /// Create a real carrier handle that can interrupt the request already in
    /// flight while the reading thread is blocked awaiting its reply.
    #[cfg(unix)]
    pub fn cancel_handle(&self) -> OwnerReadScaffoldResult<OwnerCancelHandle> {
        let stream = self.stream.try_clone().map_err(|_| {
            OwnerReadScaffoldError::unimplemented("clone selected owner carrier for cancellation")
        })?;
        Ok(OwnerCancelHandle {
            stream,
            cancellations: Arc::clone(&self.cancellations),
            patience: std::time::Duration::from_millis(self.timeouts.response_ms),
        })
    }

    /// Create a real carrier shutdown handle before moving the client into a
    /// blocked request worker.
    #[cfg(unix)]
    pub fn disconnect_handle(&self) -> OwnerReadScaffoldResult<OwnerDisconnectHandle> {
        let stream = self.stream.try_clone().map_err(|_| {
            OwnerReadScaffoldError::unimplemented("clone selected owner carrier for EOF/HUP")
        })?;
        Ok(OwnerDisconnectHandle { stream })
    }

    /// EOF/HUP discards staged result chunks and closes only the selected
    /// carrier. It cannot call a service object or choose another route.
    #[cfg(unix)]
    pub fn disconnect(
        &self,
        receiver: &mut OrdinaryResultReceiver,
    ) -> OwnerReadScaffoldResult<ResultReceiveOutcome> {
        let outcome = receiver.disconnect()?;
        self.stream
            .shutdown(Shutdown::Both)
            .map_err(|_| OwnerReadScaffoldError::unimplemented("selected owner carrier EOF/HUP"))?;
        Ok(outcome)
    }
}

/// Who is taking frames off this connection, and which interrupts are waiting
/// to hear that the owner applied them.
///
/// The acknowledgement arrives on the connection's own frame stream, so only
/// the thread inside the receive loop can see it. That makes two things true
/// and both are enforced here: an interrupt made from another thread is
/// answered by that loop and may wait for it, and an interrupt made from
/// INSIDE that loop -- a caller cancelling from its own progress callback --
/// must not wait at all, because the thread that would have to read the answer
/// is the one that would be waiting for it. A reader that stops reading
/// releases everything still waiting: the request those interrupts name is
/// over, which is the strongest form of stopped.
#[derive(Default)]
pub(crate) struct CancelCoordination {
    reading: Mutex<Option<std::thread::ThreadId>>,
    awaiting: Mutex<std::collections::BTreeMap<u64, Arc<CancelApplication>>>,
}

impl CancelCoordination {
    /// Mark this thread as the connection's reader for as long as the returned
    /// value lives.
    fn reading_here(self: Arc<Self>) -> ReadingHere {
        *self
            .reading
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(std::thread::current().id());
        ReadingHere { coordination: self }
    }

    /// Whether a thread OTHER than this one will take the acknowledgement off
    /// the wire, which is the only case where waiting for it can end.
    fn another_thread_is_reading(&self) -> bool {
        match *self
            .reading
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
        {
            Some(reader) => reader != std::thread::current().id(),
            None => false,
        }
    }

    fn await_application(&self, request_ordinal: u64) -> Arc<CancelApplication> {
        let application = Arc::new(CancelApplication::default());
        self.awaiting
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(request_ordinal, Arc::clone(&application));
        application
    }

    fn stop_awaiting(&self, request_ordinal: u64) {
        let _abandoned = self
            .awaiting
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&request_ordinal);
    }

    fn note_applied(&self, request_ordinal: u64) {
        let application = self
            .awaiting
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&request_ordinal);
        if let Some(application) = application {
            application.note_applied();
        }
    }

    fn release_every_waiter(&self) {
        let waiting = std::mem::take(
            &mut *self
                .awaiting
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        );
        for (_ordinal, application) in waiting {
            application.note_applied();
        }
    }
}

/// The connection's reading role, held for one request's frame loop.
struct ReadingHere {
    coordination: Arc<CancelCoordination>,
}

impl Drop for ReadingHere {
    fn drop(&mut self) {
        *self
            .coordination
            .reading
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
        // Nobody is reading this connection any more, so no acknowledgement
        // can arrive. The request every waiting interrupt names has ended --
        // it is not running, which is what the caller asked for.
        self.coordination.release_every_waiter();
    }
}

/// What a canceller waits on until the owner says it has applied the
/// interrupt.
///
/// Cancelling over the channel stops the owning process's execution of the
/// statement, not merely this reader's wait, so `cancel` does not return
/// while that is still only in flight. The owner's acknowledgement arrives on
/// the request's own frame stream, which this reader is already blocked
/// reading, so the frame is handed across here rather than read twice.
#[derive(Default)]
pub(crate) struct CancelApplication {
    applied: Mutex<bool>,
    changed: std::sync::Condvar,
}

impl CancelApplication {
    pub(crate) fn note_applied(&self) {
        let mut applied = self
            .applied
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *applied = true;
        self.changed.notify_all();
    }

    /// Wait for the owner to say it has applied the interrupt. Answers false
    /// when `patience` ran out first -- a dead owner must not hold the thread
    /// that cancelled, and it must not be mistaken for one that stopped.
    pub(crate) fn wait_until_applied(&self, patience: std::time::Duration) -> bool {
        let mut applied = self
            .applied
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let deadline = std::time::Instant::now() + patience;
        while !*applied {
            let Some(remaining) = deadline.checked_duration_since(std::time::Instant::now()) else {
                return false;
            };
            let (guard, timeout) = self
                .changed
                .wait_timeout(applied, remaining)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            applied = guard;
            if timeout.timed_out() && !*applied {
                return false;
            }
        }
        true
    }
}

/// A second handle onto the selected carrier, used only to name the request
/// in flight. It carries no route, service pointer, or fallback.
#[cfg(unix)]
pub struct OwnerCancelHandle {
    stream: UnixStream,
    cancellations: Arc<CancelCoordination>,
    patience: std::time::Duration,
}

#[cfg(unix)]
impl OwnerCancelHandle {
    /// Interrupt the request this handle names, and do not return until the
    /// owner has applied it.
    ///
    /// Cancelling a read over the channel stops the OWNING PROCESS's
    /// execution of that statement, not merely this reader's wait. If this
    /// returned as soon as the interrupt was written, everything the caller
    /// did next would race the owner's own thread -- the statement would
    /// still be running while the caller had been told it was stopped. The
    /// wait is bounded by the request's own patience, so an owner that has
    /// died cannot hold the thread that cancelled.
    ///
    /// A caller that cancels from inside its own read's progress callback is
    /// the one exception, and it is not a weaker promise: that caller IS the
    /// thread the owner's acknowledgement has to be read by, so waiting for it
    /// there could only ever end in the deadline. It returns as soon as the
    /// interrupt is on the wire and hears the answer where it was always going
    /// to hear it -- in the read's own outcome, which is the cancellation.
    pub fn cancel(
        mut self,
        limits: ReadLimits,
        request_ordinal: u64,
    ) -> OwnerReadScaffoldResult<()> {
        let boundary = LocalProtocolBoundary::with_effective_limits(limits);
        let carrier = UnixLocalCarrier;
        let envelope = LocalRequestEnvelope {
            limits,
            request: LocalRequest::CancelInFlight { request_ordinal },
        };
        // Registered before the interrupt goes out: the owner can answer it
        // the instant it lands, and an acknowledgement that arrives before
        // this caller looks for it must not be missed.
        let awaited = self
            .cancellations
            .another_thread_is_reading()
            .then(|| self.cancellations.await_application(request_ordinal));
        let sent = UnixLocalCarrier::send_message(
            &carrier,
            &mut self.stream,
            &boundary,
            LocalOutboundMessage::Request(&envelope),
        )
        .map_err(OwnerReadScaffoldError::from_local);
        if let Err(error) = sent {
            self.cancellations.stop_awaiting(request_ordinal);
            return Err(error);
        }
        let Some(awaited) = awaited else {
            return Ok(());
        };
        if awaited.wait_until_applied(self.patience) {
            return Ok(());
        }
        // The interrupt went out and nothing came back. The caller must not be
        // told the statement was stopped when nobody has said so.
        self.cancellations.stop_awaiting(request_ordinal);
        Err(OwnerReadScaffoldError::from_local(
            crate::local_transport::owner_timeout(),
        ))
    }
}

#[cfg(unix)]
pub struct OwnerDisconnectHandle {
    stream: UnixStream,
}

#[cfg(unix)]
impl OwnerDisconnectHandle {
    /// End the reading side of this carrier. The owner sees end-of-file on
    /// the connection and stops the work it is doing, then answers the
    /// request that was in flight with the disconnected refusal -- so the
    /// caller learns its work is over only once the owner has actually put
    /// every resource down, rather than while it is still running.
    pub fn disconnect(self) -> OwnerReadScaffoldResult<()> {
        self.stream
            .shutdown(Shutdown::Write)
            .map_err(|_| OwnerReadScaffoldError::unimplemented("selected owner carrier EOF/HUP"))
    }
}

fn checked_deadline(clock: &dyn DeadlineClock, duration_ms: u64) -> OwnerReadScaffoldResult<u64> {
    clock
        .now_ms()
        .checked_add(duration_ms)
        .ok_or_else(|| OwnerReadScaffoldError::unimplemented("validated local deadline arithmetic"))
}
