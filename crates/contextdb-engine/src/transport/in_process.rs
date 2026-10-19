use super::{
    ClientTransport, HandlerRegistration, IncomingRequest, Responder, ServerTransport,
    TransportError, TransportFuture,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::oneshot;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordedExchange {
    pub subject: String,
    pub request_bytes: Vec<u8>,
    pub response_bytes: Vec<u8>,
}

/// Links a client and a server through an in-memory hub so the full
/// push / pull / status / batch-split / apply / conflict path runs with no
/// broker. `.client()` and `.server()` hand out the two ends.
pub struct InProcessBroker {
    inner: Arc<Inner>,
}

struct Inner {
    routes: Mutex<HashMap<String, super::RequestHandler>>,
    exchanges: Mutex<Vec<RecordedExchange>>,
    routes_changed: tokio::sync::Notify,
    request_attempts: Mutex<HashMap<String, usize>>,
    request_attempted: tokio::sync::Notify,
    /// The identity the served side presents, set by
    /// [`InProcessBroker::server_as`]. A client dialing this broker knows it
    /// the same way the real transport knows the endpoint it dialed by key.
    server_node_id: Mutex<Option<String>>,
    // Original broker baseline, never relabelled as request-paid storage.
    #[cfg(target_os = "linux")]
    native: Mutex<Option<contextdb_core::read_memory::ReadShared<native_serving::Mailbox>>>,
    /// Legacy subjects served beside the live native root. Broker baseline,
    /// removed from `routes` when that root retires.
    #[cfg(target_os = "linux")]
    legacy_beside_native: Mutex<Vec<String>>,
}

struct InProcessClient {
    inner: Arc<Inner>,
    /// The authenticated node identity this client presents to the server,
    /// modelling the real transport's per-connection authenticated peer id.
    /// `None` for callers that do not exercise per-node hub behavior.
    node_id: Option<String>,
}

struct InProcessServer {
    inner: Arc<Inner>,
}

impl ClientTransport for InProcessClient {
    fn request_stream_admitted<'a>(
        &'a self,
        subject: &'a str,
        body: super::BoundedWireBody,
        timeout: Duration,
    ) -> super::WireResult<super::DispatchedWireFuture<'a, super::BoundedWireReply>> {
        #[cfg(target_os = "linux")]
        {
            let mailbox = {
                let installed = self.inner.native.lock().unwrap_or_else(|p| p.into_inner());
                installed
                    .as_ref()
                    .map(native_serving::Mailbox::acquire_client)
                    .transpose()?
            };
            // A subject outside the native root keeps its Legacy route.
            if let Some(mailbox) = mailbox.filter(|mailbox| mailbox.serves(subject)) {
                return native_serving::request(
                    mailbox,
                    subject,
                    body,
                    timeout,
                    self.node_id.as_deref(),
                );
            }
        }
        Ok(super::DispatchedWireFuture::legacy(
            self.request_stream(subject, body, timeout),
        ))
    }

    fn local_node_id(&self) -> Option<String> {
        self.node_id.clone()
    }

    fn peer_node_id(&self) -> Option<String> {
        self.inner
            .server_node_id
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .clone()
    }

    fn has_stable_edge_identity(&self) -> bool {
        self.node_id.is_some()
    }

    fn request<'a>(
        &'a self,
        subject: &'a str,
        request_bytes: Vec<u8>,
        timeout: Duration,
    ) -> TransportFuture<'a, Vec<u8>> {
        Box::pin(async move {
            let subject = subject.to_string();
            let request_for_log = request_bytes.clone();
            let inner = self.inner.clone();
            tokio::time::timeout(timeout, async move {
                tokio::task::yield_now().await;
                {
                    let mut attempts = inner
                        .request_attempts
                        .lock()
                        .unwrap_or_else(|err| err.into_inner());
                    *attempts.entry(subject.clone()).or_default() += 1;
                }
                inner.request_attempted.notify_waiters();
                let handler = {
                    let routes = inner.routes.lock().unwrap_or_else(|err| err.into_inner());
                    routes.get(&subject).cloned()
                };
                let Some(handler) = handler else {
                    return Err(TransportError::NoResponder);
                };

                let (reply_tx, reply_rx) = oneshot::channel::<Vec<u8>>();
                let responder: Responder = Box::new(move |response_bytes| {
                    Box::pin(async move {
                        let _ = reply_tx.send(response_bytes);
                        Ok(())
                    })
                });

                handler(IncomingRequest {
                    bytes: request_bytes,
                    responder,
                    node_id: self.node_id.clone(),
                })
                .await?;

                let response_bytes = reply_rx.await.map_err(|_| {
                    TransportError::Other(("responder dropped without replying".to_string()).into())
                })?;

                inner
                    .exchanges
                    .lock()
                    .unwrap_or_else(|err| err.into_inner())
                    .push(RecordedExchange {
                        subject,
                        request_bytes: request_for_log,
                        response_bytes: response_bytes.clone(),
                    });
                Ok(response_bytes)
            })
            .await
            .map_err(|_| TransportError::Timeout)?
        })
    }
}

impl ServerTransport for InProcessServer {
    fn serve_native_installation<'a>(
        &'a self,
        legacy: Vec<HandlerRegistration>,
        bound: super::ReadBoundStreamInstallation,
        stop: super::NativeStop,
    ) -> super::WireResult<super::DispatchedWireFuture<'a, ()>> {
        if bound.native_routes().is_none() {
            return self.serve_read_bound_installation(legacy, bound, stop.flag());
        }
        #[cfg(target_os = "linux")]
        {
            // Legacy companions keep their ordinary, unpaid route-map entries
            // beside the native root, exactly as `serve` installs them. No
            // unpaid callback enters the native installation itself.
            if self
                .inner
                .native
                .lock()
                .unwrap_or_else(|err| err.into_inner())
                .is_some()
            {
                return Err(TransportError::NoResponder.into());
            }
            let subjects = legacy
                .iter()
                .map(|handler| handler.subject.clone())
                .collect::<Vec<_>>();
            {
                let mut routes = self
                    .inner
                    .routes
                    .lock()
                    .unwrap_or_else(|err| err.into_inner());
                for handler in legacy {
                    routes.insert(handler.subject, handler.handler);
                }
            }
            *self
                .inner
                .legacy_beside_native
                .lock()
                .unwrap_or_else(|err| err.into_inner()) = subjects;
            let serving = native_serving::serve(&self.inner, bound, stop).inspect_err(|_| {
                self.inner.retire_legacy_beside_native();
            })?;
            // Readiness is announced only once the native root is live too.
            self.inner.routes_changed.notify_waiters();
            Ok(serving)
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (legacy, bound, stop);
            Err(TransportError::NoResponder.into())
        }
    }

    fn serve<'a>(
        &'a self,
        handlers: Vec<HandlerRegistration>,
        shutdown: Arc<AtomicBool>,
    ) -> TransportFuture<'a, ()> {
        Box::pin(async move {
            let subjects = handlers
                .iter()
                .map(|handler| handler.subject.clone())
                .collect::<Vec<_>>();
            {
                let mut routes = self
                    .inner
                    .routes
                    .lock()
                    .unwrap_or_else(|err| err.into_inner());
                for handler in handlers {
                    routes.insert(handler.subject, handler.handler);
                }
            }
            self.inner.routes_changed.notify_waiters();

            while !shutdown.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }

            let mut routes = self
                .inner
                .routes
                .lock()
                .unwrap_or_else(|err| err.into_inner());
            for subject in subjects {
                routes.remove(&subject);
            }
            Ok(())
        })
    }
}

#[cfg(target_os = "linux")]
impl Inner {
    fn retire_legacy_beside_native(&self) {
        let subjects = std::mem::take(
            &mut *self
                .legacy_beside_native
                .lock()
                .unwrap_or_else(|err| err.into_inner()),
        );
        let mut routes = self.routes.lock().unwrap_or_else(|err| err.into_inner());
        for subject in subjects {
            routes.remove(&subject);
        }
    }
}

impl InProcessBroker {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                routes: Mutex::new(HashMap::new()),
                exchanges: Mutex::new(Vec::new()),
                routes_changed: tokio::sync::Notify::new(),
                request_attempts: Mutex::new(HashMap::new()),
                request_attempted: tokio::sync::Notify::new(),
                server_node_id: Mutex::new(None),
                #[cfg(target_os = "linux")]
                native: Mutex::new(None),
                #[cfg(target_os = "linux")]
                legacy_beside_native: Mutex::new(Vec::new()),
            }),
        }
    }

    pub fn client(&self) -> Arc<dyn ClientTransport> {
        Arc::new(InProcessClient {
            inner: self.inner.clone(),
            node_id: None,
        })
    }

    /// A client that presents `node_id` as its authenticated identity, so the
    /// served requests carry it exactly as the real sync path carries the
    /// dialing endpoint's authenticated peer id. Used to exercise the hub's
    /// per-node last-contact recording without a real transport.
    pub fn client_as(&self, node_id: &str) -> Arc<dyn ClientTransport> {
        Arc::new(InProcessClient {
            inner: self.inner.clone(),
            node_id: Some(node_id.to_string()),
        })
    }

    pub fn server(&self) -> Arc<dyn ServerTransport> {
        Arc::new(InProcessServer {
            inner: self.inner.clone(),
        })
    }

    /// A server that presents `node_id` as its own authenticated identity, the
    /// mirror of [`Self::client_as`]: every client of this broker then knows
    /// the hub it is talking to, exactly as a client that dialed a real
    /// endpoint by key does.
    pub fn server_as(&self, node_id: &str) -> Arc<dyn ServerTransport> {
        *self
            .inner
            .server_node_id
            .lock()
            .unwrap_or_else(|err| err.into_inner()) = Some(node_id.to_string());
        Arc::new(InProcessServer {
            inner: self.inner.clone(),
        })
    }

    pub fn recorded_exchanges(&self) -> Vec<RecordedExchange> {
        self.inner
            .exchanges
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .clone()
    }

    /// Wait until the serving side has installed `subject`. This is the
    /// deterministic readiness seam for harnesses that must issue a request
    /// immediately after spawning `serve`.
    #[doc(hidden)]
    pub async fn wait_for_registered_route_for_test(&self, subject: &str) {
        loop {
            let notified = self.inner.routes_changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self
                .inner
                .routes
                .lock()
                .unwrap_or_else(|err| err.into_inner())
                .contains_key(subject)
            {
                return;
            }
            notified.await;
        }
    }

    /// Wait until a client has attempted `subject`, including a no-responder
    /// attempt made before the serving route exists.
    #[doc(hidden)]
    pub async fn wait_for_request_attempt_for_test(&self, subject: &str) {
        loop {
            let notified = self.inner.request_attempted.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self
                .inner
                .request_attempts
                .lock()
                .unwrap_or_else(|err| err.into_inner())
                .get(subject)
                .copied()
                .unwrap_or_default()
                > 0
            {
                return;
            }
            notified.await;
        }
    }
}

impl Default for InProcessBroker {
    fn default() -> Self {
        Self::new()
    }
}

/// The producer retains its own input custody. This native receive allocates
/// only the exact admitted copy, after the original operation has begun.
#[cfg(test)]
fn receive_native_read_borrowed(
    operation: &crate::owner_read::NativeReadRequest,
    request: &[u8],
) -> super::WireResult<super::BoundedWireBody> {
    let bytes = super::AdmittedWireVec::copy_for_native(request, operation)?;
    let length = bytes.bytes.len() as u64;
    Ok(super::BoundedWireBody {
        storage: super::WireBodyStorage::Legacy(bytes),
        length,
        position: 0,
        reader: operation.owner(),
    })
}

/// Caller gets the original cancellation before polling the returned driver.
/// Reply custody is opaque and remains charged until its final physical drop.
#[cfg(test)]
#[allow(
    clippy::type_complexity,
    reason = "the begun request's three parts are returned together and named at the one call site"
)]
pub(crate) fn begin_native_in_process(
    installation: &super::ReadBoundStreamInstallation,
    subject: &str,
    request: &[u8],
    node_id: Option<&str>,
) -> super::WireResult<(
    contextdb_core::read_contract::NativeReadCancellation,
    super::DispatchedWireFuture<'static, ()>,
    contextdb_core::read_memory::ReadShared<Mutex<Option<super::BoundedWireBody>>>,
)> {
    use contextdb_core::read_memory::ReadShared;
    let routes = installation
        .native_routes()
        .ok_or(super::TransportError::NoResponder)?;
    let route = routes
        .find(subject)
        .ok_or(super::TransportError::NoResponder)?;
    // begin is the ONLY provider acquisition, before receive/control/frame.
    let (operation, cancellation) = match route.begin_request()? {
        super::native_wire::NativeReadStreamBegin::Native {
            operation,
            cancellation,
        } => (operation, cancellation),
        super::native_wire::NativeReadStreamBegin::Legacy(_) => {
            return Err(super::TransportError::NoResponder.into());
        }
    };
    let owner = operation.owner();
    let body = receive_native_read_borrowed(&operation, request)?;
    let invoice = super::native_wire::wire_credit(
        owner.clone(),
        ReadShared::<Mutex<Option<super::BoundedWireBody>>>::layout().size(),
        "native_in_process_reply",
    )?;
    let reply = ReadShared::try_from_prepaid_with(invoice, || Ok(Mutex::new(None)))?;
    let sink = reply.clone();
    let finish_owner = owner.clone();
    let finish_cancellation = cancellation.clone();
    let responder = super::native_wire::NativeStreamResponder::try_new_with(owner, || {
        Ok(move |body| {
            // Admission includes the concrete body and slot captures, before
            // constructing this callback future. No oneshot/Notify/waker clone.
            super::native_wire::admit_wire_future_with(finish_owner, || {
                Ok(async move {
                    let mut slot = sink.lock().unwrap_or_else(|p| p.into_inner());
                    if finish_cancellation.is_cancelled() {
                        return Err(contextdb_core::Error::ReadCancelled.into());
                    }
                    if slot.is_some() {
                        return Err(super::TransportError::NoResponder.into());
                    }
                    *slot = Some(body);
                    Ok(())
                })
            })
        })
    })?;
    let future = route.dispatch_native(
        super::ReadBoundIncomingRequest {
            body,
            responder: super::ReadBoundResponder::Native(responder),
            // Test-driver identity: the broker's paid identity copy is in `request`.
            node_id: node_id.map(str::to_owned),
        },
        operation,
    )?;
    Ok((cancellation, future, reply))
}

// Native siblings run on the caller's existing thread. Each operation/frame
// and wake control is admitted before construction; no executor task is spawned.
#[cfg(target_os = "linux")]
mod native_serving {
    use super::*;
    use contextdb_core::read_contract::{
        NativeCancellationListener, NativeDriverPark, NativeReadCancellation,
    };
    use contextdb_core::read_memory::{ReadMemoryOwner, ReadShared};
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::AtomicU32;
    use std::task::{Context, Poll};
    fn shared<T>(
        owner: ReadMemoryOwner,
        make: impl FnOnce() -> contextdb_core::Result<T>,
    ) -> super::super::WireResult<ReadShared<T>> {
        let invoice = super::super::native_wire::wire_credit(
            owner,
            ReadShared::<T>::layout().size(),
            "in_process_control",
        )?;
        Ok(ReadShared::try_from_prepaid_with(invoice, make)?)
    }

    struct NativeInProcessWake {
        ready: AtomicU32,
    }

    impl NativeInProcessWake {
        fn try_new_owned(owner: ReadMemoryOwner) -> super::super::WireResult<ReadShared<Self>> {
            shared(owner, || {
                Ok(Self {
                    ready: AtomicU32::new(0),
                })
            })
        }

        fn park_once(&self, timeout_ms: Option<u64>) -> contextdb_core::Result<()> {
            if self.ready.swap(0, Ordering::AcqRel) != 0 {
                return Ok(());
            }
            // Relative FUTEX_WAIT uses CLOCK_MONOTONIC. The entire timespec
            // stays on this already-owned caller's stack through the syscall.
            // Cap only to the platform's representable relative interval; the
            // next operation poll rereads the supplied clock after any return.
            let timeout = timeout_ms.map(|ms| libc::timespec {
                tv_sec: libc::time_t::try_from(ms / 1_000).unwrap_or(libc::time_t::MAX),
                tv_nsec: ((ms % 1_000) * 1_000_000) as libc::c_long,
            });
            let timeout_pointer = timeout
                .as_ref()
                .map_or(std::ptr::null(), |value| value as *const libc::timespec);
            // SAFETY: the paid shared borrow keeps the aligned word live. A
            // concurrent wake stores one before FUTEX_WAKE; expected zero
            // prevents losing that wake between our swap and kernel enqueue.
            let result = unsafe {
                libc::syscall(
                    libc::SYS_futex,
                    self.ready.as_ptr(),
                    libc::FUTEX_WAIT | libc::FUTEX_PRIVATE_FLAG,
                    0u32,
                    timeout_pointer,
                )
            };
            if result == -1 {
                // No diagnostic allocation, retry loop, worker or timer registry.
                let error = unsafe { *libc::__errno_location() };
                if error != libc::EAGAIN && error != libc::EINTR && error != libc::ETIMEDOUT {
                    return Err(contextdb_core::Error::ReadSessionNotImplemented);
                }
            }
            Ok(())
        }
    }

    impl contextdb_core::read_memory::NativeReadWake for NativeInProcessWake {
        fn wake_read(&self) {
            self.ready.store(1, Ordering::Release);
            // SAFETY: this callback retains the same paid cell; wake neither
            // copies a host waker nor creates a scheduler/task/channel node.
            unsafe {
                libc::syscall(
                    libc::SYS_futex,
                    self.ready.as_ptr(),
                    libc::FUTEX_WAKE | libc::FUTEX_PRIVATE_FLAG,
                    1i32,
                );
            }
        }
    }

    // Inline adapter only: the original paid cell remains the sole physical
    // control. Its handle is part of the admitted drain frame, with no std Arc
    // wrapper or second allocation. A local type keeps the companion impls
    // within Rust's trait coherence rules.
    struct NativeInProcessPark {
        wake: ReadShared<NativeInProcessWake>,
    }
    impl contextdb_core::read_memory::NativeReadWake for NativeInProcessPark {
        fn wake_read(&self) {
            contextdb_core::read_memory::NativeReadWake::wake_read(&*self.wake);
        }
    }
    impl contextdb_core::read_contract::NativeDriverPark for NativeInProcessPark {
        fn park(&self) -> contextdb_core::Result<()> {
            self.wake.park_once(None)
        }
        fn park_for_ms(&self, ms: u64) -> contextdb_core::Result<()> {
            self.wake.park_once(Some(ms))
        }
        fn wake_lease(&self) -> contextdb_core::read_memory::NativeReadWakeLease {
            self.wake.clone().into_native_wake_lease()
        }
    }

    pub(super) struct Mailbox {
        routes: ReadShared<super::super::native_wire::NativeWireRoutes>,
        stop: super::super::NativeStop,
        wake: ReadShared<NativeInProcessWake>,
        state: Mutex<MailboxState>,
        completion: std::ptr::NonNull<ReleaseCompletion>,
    }
    // The paid outer completion owns this cell until all counted publishers
    // have unlocked it, on both final Ready and cancellation/drop.
    struct ReleaseCompletion {
        handles: Mutex<usize>,
        released: std::sync::Condvar,
        wake: ReadShared<NativeInProcessWake>,
    }
    impl ReleaseCompletion {
        fn wait_released(&self) {
            let mut handles = self.handles.lock().unwrap_or_else(|p| p.into_inner());
            while *handles != 0 {
                // This inline condition variable is part of the prepaid cell's
                // exact layout. Release is the event; no count/time polling.
                handles = self
                    .released
                    .wait(handles)
                    .unwrap_or_else(|p| p.into_inner());
            }
        }
    }
    // Only the installed root can acquire a client handle, under inner.native.
    // Retirement removes that root before the completion owner can be freed.
    pub(super) struct ClientHandle {
        mailbox: Option<ReadShared<Mailbox>>,
        completion: std::ptr::NonNull<ReleaseCompletion>,
    }
    // SAFETY: the counted handle pins completion through the outer owner's
    // release wait; all completion accesses are serialized by handles.
    unsafe impl Send for ClientHandle {}
    unsafe impl Sync for ClientHandle {}
    unsafe impl Send for Mailbox {}
    unsafe impl Sync for Mailbox {}
    impl ClientHandle {
        fn mailbox(&self) -> &Mailbox {
            self.mailbox.as_ref().expect("live client handle")
        }
        pub(super) fn serves(&self, subject: &str) -> bool {
            self.mailbox().routes.find(subject).is_some()
        }
    }
    impl Drop for ClientHandle {
        fn drop(&mut self) {
            // No mailbox/routes/clock access is permitted after this free.
            drop(self.mailbox.take());
            // SAFETY: the outer paid owner waits for this counted publisher.
            let completion = unsafe { self.completion.as_ref() };
            let mut handles = completion.handles.lock().unwrap_or_else(|p| p.into_inner());
            contextdb_core::read_memory::NativeReadWake::wake_read(&*completion.wake);
            *handles -= 1;
            completion.released.notify_all();
            // Unlock is our final cell access. The owner observes zero only
            // after that unlock and can then physically destroy the cell.
        }
    }
    struct MailboxState {
        accepting: bool,
        retired: bool,
        builders: usize,
        pending: Option<ReadShared<RequestNode>>,
    }
    // A reservation is admitted at the gate before begin_request invokes the
    // provider. Stop waits for these original reservations, including Err/panic.
    struct BeginTicket {
        handle: ClientHandle,
    }
    impl Drop for BeginTicket {
        fn drop(&mut self) {
            let mut state = self
                .handle
                .mailbox()
                .state
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            state.builders -= 1;
            drop(state);
            contextdb_core::read_memory::NativeReadWake::wake_read(&*self.handle.mailbox().wake);
        }
    }
    impl Mailbox {
        pub(super) fn acquire_client(
            mailbox: &ReadShared<Self>,
        ) -> super::super::WireResult<ClientHandle> {
            // Caller holds inner.native, which excludes root retirement and
            // serializes this acquisition with shutdown's admission barrier.
            let state = mailbox.state.lock().unwrap_or_else(|p| p.into_inner());
            if !state.accepting || mailbox.stop.is_stopped() {
                return Err(TransportError::NoResponder.into());
            }
            let completion = unsafe { mailbox.completion.as_ref() };
            let mut handles = completion.handles.lock().unwrap_or_else(|p| p.into_inner());
            *handles = handles.checked_add(1).ok_or(TransportError::NoResponder)?;
            Ok(ClientHandle {
                mailbox: Some(mailbox.clone()),
                completion: mailbox.completion,
            })
        }
        fn begin(handle: ClientHandle) -> super::super::WireResult<BeginTicket> {
            {
                let mailbox = handle.mailbox();
                let mut state = mailbox.state.lock().unwrap_or_else(|p| p.into_inner());
                if !state.accepting || mailbox.stop.is_stopped() {
                    return Err(TransportError::NoResponder.into());
                }
                state.builders = state
                    .builders
                    .checked_add(1)
                    .ok_or(TransportError::NoResponder)?;
            }
            Ok(BeginTicket { handle })
        }
        fn enqueue(&self, node: ReadShared<RequestNode>) -> super::super::WireResult<()> {
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            if state.retired {
                drop(state);
                node.cancel();
                node.finish(Err(contextdb_core::Error::ReadCancelled.into()));
                drop(node);
                return Err(contextdb_core::Error::ReadCancelled.into());
            }
            *node.next.lock().unwrap_or_else(|p| p.into_inner()) = state.pending.take();
            state.pending = Some(node);
            drop(state);
            contextdb_core::read_memory::NativeReadWake::wake_read(&*self.wake);
            Ok(())
        }
    }
    struct ReplyState {
        body: Option<super::super::BoundedWireBody>,
        completed: Option<super::super::WireResult<()>>,
        abandoned: bool,
    }
    // Request-funded bridge: callers and cancellation snapshots can retain
    // it after finish, but its installation wake is detached before publication.
    struct RequestWake {
        coordinator: Mutex<Option<ReadShared<NativeInProcessWake>>>,
    }
    impl RequestWake {
        fn wake(&self) {
            let coordinator = self.coordinator.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(wake) = coordinator.as_ref() {
                contextdb_core::read_memory::NativeReadWake::wake_read(&**wake);
            }
        }
        fn detach(&self) {
            let mut coordinator = self.coordinator.lock().unwrap_or_else(|p| p.into_inner());
            // Free under the lock: an in-flight callback cannot retain a clone.
            drop(coordinator.take());
        }
    }
    // Until queue transfer succeeds, this stack guard clears even a bridge
    // retained by an in-flight callback when a later admission refuses.
    struct RequestWakeAdmission {
        wake: ReadShared<RequestWake>,
        transferred: bool,
    }
    impl Drop for RequestWakeAdmission {
        fn drop(&mut self) {
            if !self.transferred {
                self.wake.detach();
            }
        }
    }
    struct RequestNode {
        // Worker physically drops before publishing completion to the caller.
        worker: Mutex<Option<super::super::DispatchedWireFuture<'static, ()>>>,
        listener: Mutex<Option<NativeCancellationListener>>,
        request_wake: ReadShared<RequestWake>,
        cancellation: Option<NativeReadCancellation>,
        response: ReadShared<Mutex<ReplyState>>,
        reply_wake: ReadShared<NativeInProcessWake>,
        deadline: u64,
        next: Mutex<Option<ReadShared<RequestNode>>>,
    }
    impl RequestNode {
        fn finish(&self, result: super::super::WireResult<()>) {
            let worker = self.worker.lock().unwrap_or_else(|p| p.into_inner()).take();
            // Physical dispatch destruction (including all nested futures and
            // their original invoices) precedes the terminal reply publication.
            drop(worker);
            drop(
                self.listener
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .take(),
            );
            self.request_wake.detach();
            let mut response = self.response.lock().unwrap_or_else(|p| p.into_inner());
            response.completed = Some(result);
            drop(response);
            contextdb_core::read_memory::NativeReadWake::wake_read(&*self.reply_wake);
        }
        fn cancel(&self) {
            if let Some(cancel) = &self.cancellation {
                cancel.cancel();
            }
        }
    }
    impl Drop for RequestNode {
        fn drop(&mut self) {
            if self
                .worker
                .get_mut()
                .unwrap_or_else(|p| p.into_inner())
                .is_some()
            {
                self.cancel();
                self.finish(Err(contextdb_core::Error::ReadCancelled.into()));
            }
            // A dispatch-construction refusal can leave no worker but still
            // have an in-flight cancellation snapshot holding the bridge.
            drop(
                self.listener
                    .get_mut()
                    .unwrap_or_else(|p| p.into_inner())
                    .take(),
            );
            self.request_wake.detach();
            // Pending worker then frees before listener/token/response fields.
            // Detach list iteratively, avoiding recursive list destruction.
            let mut next = self
                .next
                .get_mut()
                .unwrap_or_else(|p| p.into_inner())
                .take();
            while let Some(node) = next {
                next = node.next.lock().unwrap_or_else(|p| p.into_inner()).take();
                node.cancel();
                node.finish(Err(contextdb_core::Error::ReadCancelled.into()));
                drop(node);
            }
        }
    }
    struct ClientWait {
        response: ReadShared<Mutex<ReplyState>>,
        wake: ReadShared<NativeInProcessWake>,
        request_wake: ReadShared<RequestWake>,
        cancellation: Option<NativeReadCancellation>,
        completed: bool,
    }
    impl Future for ClientWait {
        type Output = super::super::WireResult<super::super::BoundedWireReply>;
        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
            let this = self.get_mut();
            // Synchronous native driver: no host Waker is cloned or stored.
            loop {
                let mut response = this.response.lock().unwrap_or_else(|p| p.into_inner());
                if let Some(result) = response.completed.take() {
                    this.completed = true;
                    return Poll::Ready(result.and_then(|()| {
                        response
                            .body
                            .take()
                            .map(super::super::BoundedWireReply::legacy)
                            .ok_or_else(|| TransportError::NoResponder.into())
                    }));
                }
                drop(response);
                if let Err(error) = this.wake.park_once(None) {
                    return Poll::Ready(Err(error.into()));
                }
            }
        }
    }
    impl Drop for ClientWait {
        fn drop(&mut self) {
            if !self.completed {
                self.response
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .abandoned = true;
                if let Some(cancel) = &self.cancellation {
                    cancel.cancel();
                }
                self.request_wake.wake();
            }
        }
    }

    pub(super) fn request(
        handle: ClientHandle,
        subject: &str,
        input: super::super::BoundedWireBody,
        timeout: Duration,
        node_id: Option<&str>,
    ) -> super::super::WireResult<
        super::super::DispatchedWireFuture<'static, super::super::BoundedWireReply>,
    > {
        let ticket = Mailbox::begin(handle)?;
        let mailbox = ticket.handle.mailbox();
        let route = mailbox
            .routes
            .find(subject)
            .ok_or(TransportError::NoResponder)?;
        // Provider selection is exactly once, using the original installed root.
        let selected = route.begin_request()?;
        let (owner, cancellation) = match &selected {
            super::super::native_wire::NativeReadStreamBegin::Native {
                operation,
                cancellation,
            } => (operation.owner(), Some(cancellation.clone())),
            super::super::native_wire::NativeReadStreamBegin::Legacy(acquired) => {
                let crate::vector_read_memory::RequestReadRoute::Legacy(reader) =
                    acquired.share().into_route()
                else {
                    return Err(TransportError::NoResponder.into());
                };
                (ReadMemoryOwner::from_legacy(reader), None)
            }
        };
        let timeout_ms =
            u64::try_from(timeout.as_millis()).map_err(|_| TransportError::NoResponder)?;
        let deadline = mailbox
            .routes
            .clock()
            .now_ms()
            .checked_add(timeout_ms)
            .ok_or(TransportError::NoResponder)?;
        let future = super::super::native_wire::admit_wire_future_with(owner.clone(), || {
            let bytes = input
                .native_contiguous_bytes()
                .ok_or(TransportError::NoResponder)?;
            let body = super::super::BoundedWireBody::from_native_reply(
                super::super::AdmittedWireVec::copy_from(bytes, owner.clone())?,
            );
            let response = shared(owner.clone(), || {
                Ok(Mutex::new(ReplyState {
                    body: None,
                    completed: None,
                    abandoned: false,
                }))
            })?;
            let reply_wake = NativeInProcessWake::try_new_owned(owner.clone())?;
            let sink = response.clone();
            let sink_owner = owner.clone();
            let finish_owner = owner.clone();
            let responder = super::super::native_wire::NativeStreamResponder::try_new_with(
                owner.clone(),
                || {
                    Ok(move |body: super::super::BoundedWireBody| {
                        super::super::native_wire::admit_wire_future_with(finish_owner, || {
                            Ok(async move {
                                if !body.owner().same_owner(&sink_owner) {
                                    return Err(TransportError::NoResponder.into());
                                }
                                let mut slot = sink.lock().unwrap_or_else(|p| p.into_inner());
                                if slot.abandoned {
                                    return Err(contextdb_core::Error::ReadCancelled.into());
                                }
                                if slot.body.is_some() {
                                    return Err(TransportError::NoResponder.into());
                                }
                                slot.body = Some(body);
                                Ok(())
                            })
                        })
                    })
                },
            )?;
            // Identity bytes carry their own same-owner invoice through worker
            // destruction; String adopts the admitted Vec without allocation.
            let identity = match node_id {
                Some(value) => Some(super::super::AdmittedWireVec::copy_from(
                    value.as_bytes(),
                    owner.clone(),
                )?),
                None => None,
            };
            let (identity_value, identity_credit) = match identity {
                Some(super::super::AdmittedWireVec {
                    bytes,
                    _credit,
                    reader: _,
                }) => (
                    Some(unsafe { String::from_utf8_unchecked(bytes) }),
                    Some(_credit),
                ),
                None => (None, None),
            };
            let request = super::super::ReadBoundIncomingRequest {
                body,
                responder: super::super::ReadBoundResponder::Native(responder),
                node_id: identity_value,
            };
            let request_wake = shared(owner.clone(), || {
                Ok(RequestWake {
                    coordinator: Mutex::new(Some(mailbox.wake.clone())),
                })
            })?;
            let mut wake_admission = RequestWakeAdmission {
                wake: request_wake.clone(),
                transferred: false,
            };
            let listener = match &cancellation {
                Some(cancel) => {
                    let wake = request_wake.clone();
                    Some(cancel.try_tell_on_cancel(move || {
                        wake.wake();
                    })?)
                }
                None => None,
            };
            let node = shared(owner.clone(), || {
                Ok(RequestNode {
                    worker: Mutex::new(None),
                    listener: Mutex::new(listener),
                    request_wake: request_wake.clone(),
                    cancellation: cancellation.clone(),
                    response: response.clone(),
                    reply_wake: reply_wake.clone(),
                    deadline,
                    next: Mutex::new(None),
                })
            })?;
            let worker = match selected {
                super::super::native_wire::NativeReadStreamBegin::Native { operation, .. } => {
                    super::super::native_wire::admit_native_operation_future_with(
                        operation,
                        |original| {
                            let worker = route.dispatch_native(request, original.share())?;
                            Ok(async move {
                                let _identity_credit = identity_credit;
                                worker.await
                            })
                        },
                    )?
                }
                super::super::native_wire::NativeReadStreamBegin::Legacy(acquired) => {
                    // Selected Legacy executes the original companion with the
                    // SAME acquired owner and actual paid NativeStreamResponder.
                    super::super::DispatchedWireFuture::native(
                        super::super::native_wire::admit_wire_future_with(owner.clone(), || {
                            let original = acquired.share();
                            let worker = route.dispatch_legacy(request, original)?;
                            Ok(async move {
                                let _original_parent = acquired;
                                let _identity_credit = identity_credit;
                                worker.await
                            })
                        })?,
                    )
                }
            };
            *node.worker.lock().unwrap_or_else(|p| p.into_inner()) = Some(worker);
            mailbox.enqueue(node)?;
            wake_admission.transferred = true;
            drop(wake_admission);
            Ok(ClientWait {
                response,
                wake: reply_wake,
                request_wake,
                cancellation,
                completed: false,
            })
        })?;
        // All route/mailbox borrows end here. The ticket owns the only client
        // mailbox handle, including provider refusal and unwind paths.
        drop(ticket);
        Ok(super::super::DispatchedWireFuture::native(future))
    }

    struct Serving<'a> {
        active: Option<ReadShared<RequestNode>>,
        mailbox: ReadShared<Mailbox>,
        inner: &'a Inner,
        _listener: super::super::NativeStopListener,
        drain_target: Option<u64>,
        _installation: super::super::ReadBoundStreamInstallation,
        drain_failed: bool,
        completion: ReadShared<ReleaseCompletion>,
    }
    impl Future for Serving<'_> {
        type Output = super::super::WireResult<()>;
        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
            let this = self.get_mut();
            let wake = this.mailbox.wake.clone();
            let park = NativeInProcessPark { wake: wake.clone() };
            let waker = wake.into_read_waker();
            let mut cx = Context::from_waker(&waker);
            // This producer owns its caller thread until stop/drain completes.
            loop {
                let stopping = this.mailbox.stop.is_stopped();
                if stopping {
                    this.close_admission();
                }
                let (mut pending, builders) = {
                    let mut state = this.mailbox.state.lock().unwrap_or_else(|p| p.into_inner());
                    if stopping {
                        state.accepting = false;
                    }
                    (state.pending.take(), state.builders)
                };
                while let Some(node) = pending {
                    pending = node.next.lock().unwrap_or_else(|p| p.into_inner()).take();
                    *node.next.lock().unwrap_or_else(|p| p.into_inner()) = this.active.take();
                    this.active = Some(node);
                }
                let mut work = this.active.take();
                let mut nearest = this.drain_target;
                while let Some(node) = work {
                    work = node.next.lock().unwrap_or_else(|p| p.into_inner()).take();
                    let polled = {
                        let mut worker = node.worker.lock().unwrap_or_else(|p| p.into_inner());
                        match worker.as_mut() {
                            Some(worker) => Pin::new(worker).poll(&mut cx),
                            None => Poll::Ready(Err(contextdb_core::Error::ReadCancelled.into())),
                        }
                    };
                    // Ready wins at any deadline and before observed cancellation.
                    let terminal = match polled {
                        Poll::Ready(result) => Some(result),
                        Poll::Pending => {
                            let abandoned = node
                                .response
                                .lock()
                                .unwrap_or_else(|p| p.into_inner())
                                .abandoned;
                            let now = this.mailbox.routes.clock().now_ms();
                            if abandoned
                                || node
                                    .cancellation
                                    .as_ref()
                                    .is_some_and(|cancel| cancel.is_cancelled())
                            {
                                node.cancel();
                                Some(Err(contextdb_core::Error::ReadCancelled.into()))
                            } else if this.drain_target.is_some_and(|target| now >= target) {
                                this.drain_failed = true;
                                node.cancel();
                                Some(Err(contextdb_core::Error::OwnerReadDrainTimeout.into()))
                            } else if now >= node.deadline {
                                node.cancel();
                                Some(Err(TransportError::Timeout.into()))
                            } else {
                                None
                            }
                        }
                    };
                    if let Some(result) = terminal {
                        node.finish(result);
                        drop(node);
                        #[cfg(test)]
                        super::super::release_events::signal();
                    } else {
                        nearest = Some(nearest.map_or(node.deadline, |old| old.min(node.deadline)));
                        *node.next.lock().unwrap_or_else(|p| p.into_inner()) = this.active.take();
                        this.active = Some(node);
                    }
                }
                if stopping && this.drain_target.is_none() {
                    this.drain_target = Some(
                        this.mailbox
                            .routes
                            .clock()
                            .now_ms()
                            .checked_add(this.mailbox.routes.shutdown_drain_ms())
                            .ok_or(TransportError::NoResponder)?,
                    );
                    nearest = Some(nearest.map_or(this.drain_target.unwrap(), |old| {
                        old.min(this.drain_target.unwrap())
                    }));
                }
                if stopping && builders == 0 && this.active.is_none() {
                    // Recheck under gate: no admitted builder can publish after
                    // final finish, and every original dispatch is already free.
                    let state = this.mailbox.state.lock().unwrap_or_else(|p| p.into_inner());
                    if state.builders == 0
                        && state.pending.is_none()
                        && *this
                            .completion
                            .handles
                            .lock()
                            .unwrap_or_else(|p| p.into_inner())
                            == 0
                    {
                        return Poll::Ready(if this.drain_failed {
                            Err(contextdb_core::Error::OwnerReadDrainTimeout.into())
                        } else {
                            Ok(())
                        });
                    }
                    drop(state);
                }
                // Once pending workers are physically gone, a building
                // admission alone wakes this caller when it publishes/releases.
                // An expired drain target must not make that wait busy-poll.
                if this.active.is_none() {
                    nearest = None;
                }
                match nearest {
                    Some(target) => this.mailbox.routes.clock().park_until(target, &park)?,
                    None => park.park()?,
                }
            }
        }
    }
    impl Serving<'_> {
        fn close_admission(&self) {
            // Finish every acquisition already under inner.native before
            // examining the physical-release count. Keep the installed root
            // as a refusal gate through drain; clients must not fall to Legacy.
            let _native = self.inner.native.lock().unwrap_or_else(|p| p.into_inner());
            self.mailbox
                .state
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .accepting = false;
        }
        fn retire_root(&self) {
            let mut native = self.inner.native.lock().unwrap_or_else(|p| p.into_inner());
            if native
                .as_ref()
                .is_some_and(|current| std::ptr::eq(&**current, &*self.mailbox))
            {
                drop(native.take());
                drop(native);
                self.inner.retire_legacy_beside_native();
            }
        }
    }
    impl Drop for Serving<'_> {
        fn drop(&mut self) {
            self.retire_root();
            let pending = {
                let mut state = self.mailbox.state.lock().unwrap_or_else(|p| p.into_inner());
                state.accepting = false;
                state.retired = true;
                state.pending.take()
            };
            let mut active = self.active.take();
            while let Some(node) = active {
                active = node.next.lock().unwrap_or_else(|p| p.into_inner()).take();
                node.cancel();
                node.finish(Err(contextdb_core::Error::ReadCancelled.into()));
                drop(node);
            }
            let mut pending = pending;
            while let Some(node) = pending {
                pending = node.next.lock().unwrap_or_else(|p| p.into_inner()).take();
                node.cancel();
                node.finish(Err(contextdb_core::Error::ReadCancelled.into()));
                drop(node);
            }
            // Installation/source remains retained through all dispatch free.
            // Source has no worker to join; its physical destruction follows.
        }
    }
    struct ServingCompletion<'a> {
        frame: Option<super::super::AdmittedWireFuture<'a, ()>>,
        completion: Option<ReadShared<ReleaseCompletion>>,
    }
    impl Drop for ServingCompletion<'_> {
        fn drop(&mut self) {
            drop(self.frame.take());
            if let Some(completion) = self.completion.take() {
                completion.wait_released();
                drop(completion);
            }
        }
    }
    impl Future for ServingCompletion<'_> {
        type Output = super::super::WireResult<()>;
        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let this = self.get_mut();
            let Some(frame) = this.frame.as_mut() else {
                return Poll::Ready(Err(contextdb_core::Error::ReadCancelled.into()));
            };
            match Pin::new(frame).poll(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(result) => {
                    drop(this.frame.take());
                    if let Some(completion) = this.completion.take() {
                        completion.wait_released();
                        drop(completion);
                    }
                    Poll::Ready(result)
                }
            }
        }
    }
    pub(super) fn serve<'a>(
        inner: &'a Inner,
        installation: super::super::ReadBoundStreamInstallation,
        stop: super::super::NativeStop,
    ) -> super::super::WireResult<super::super::DispatchedWireFuture<'a, ()>> {
        let routes = installation
            .native_routes()
            .ok_or(TransportError::NoResponder)?
            .clone();
        let owner = routes.installation_owner();
        if !stop.is_funded_by(&owner) {
            return Err(TransportError::NoResponder.into());
        }
        // Price the entire completion frame before building its nested
        // standing coordinator. Completion frees that coordinator physically
        // (and its clock binding) before the caller observes final Ready.
        let completion = super::super::native_wire::admit_wire_future_with(owner.clone(), || {
            let wake = NativeInProcessWake::try_new_owned(owner.clone())?;
            let completion = shared(owner.clone(), || {
                Ok(ReleaseCompletion {
                    handles: Mutex::new(0),
                    released: std::sync::Condvar::new(),
                    wake: wake.clone(),
                })
            })?;
            let frame = super::super::native_wire::admit_wire_future_with(owner.clone(), || {
                let listener = stop.try_register_wake(wake.clone())?;
                let mailbox = shared(owner, || {
                    Ok(Mailbox {
                        routes: routes.clone(),
                        stop,
                        wake,
                        state: Mutex::new(MailboxState {
                            accepting: true,
                            retired: false,
                            builders: 0,
                            pending: None,
                        }),
                        completion: std::ptr::NonNull::from(&*completion),
                    })
                })?;
                {
                    let mut installed = inner.native.lock().unwrap_or_else(|p| p.into_inner());
                    if installed.is_some() {
                        return Err(TransportError::NoResponder.into());
                    }
                    *installed = Some(mailbox.clone());
                }
                Ok(Serving {
                    active: None,
                    mailbox,
                    inner,
                    _listener: listener,
                    drain_target: None,
                    _installation: installation,
                    drain_failed: false,
                    completion: completion.clone(),
                })
            })?;
            Ok(ServingCompletion {
                frame: Some(frame),
                completion: Some(completion),
            })
        })?;
        Ok(super::super::DispatchedWireFuture::native(completion))
    }
}
