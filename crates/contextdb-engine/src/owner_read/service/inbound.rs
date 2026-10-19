use super::*;
use crate::local_transport::inbound_query::RequestPrefix;
use crate::local_transport::query_funding::{self, AcquiredQueryOwner, Funded, QueryBudget};
use contextdb_core::read_memory::{NativeRefusal, ReadMemoryBudget, ReadShared};

/// The decode invoice survives moved strings and parameters. The carrier
/// keeps an alias outside the request future and joins its worker before
/// releasing it, including panic and failure while constructing an error.
pub(super) struct QueryIngress {
    _decoded: Mutex<Option<ReadCredit>>,
    _memory: Mutex<Option<Funded<Arc<crate::read_image_memory::ImageMemory>>>>,
    operation: QueryBudget,
    _owner: AcquiredQueryOwner,
}
impl std::ops::Deref for QueryIngress {
    type Target = QueryBudget;
    fn deref(&self) -> &QueryBudget { &self.operation }
}

pub(super) struct ReceivedRequest {
    pub(super) envelope: LocalRequestEnvelope,
    // A queue node has already been prepaid before its allocation. Its
    // credit moves out with the request after pop physically frees the node.
    pub(super) node: Option<ReadCredit>,
    pub(super) query: Option<ReadShared<QueryIngress>>,
    pub(super) controls: Option<RequestControls>,
}

/// A missing child and an acquired child whose later constructor refused
/// have different custody. Neither needs a request DTO or response Vec.
pub(super) enum DeferredCause {
    MissingChild(NativeRefusal),
    Acquired {
        error: OwnerReadScaffoldError,
        _ingress: Option<ReadShared<QueryIngress>>,
        _owner: AcquiredQueryOwner,
    },
}
pub(super) enum AuthenticatedWork {
    Request(LocalRequest),
    Refused(DeferredCause),
}
impl DeferredCause {
    pub(super) fn into_error(self) -> OwnerReadScaffoldError {
        match self {
            Self::MissingChild(error) => OwnerReadScaffoldError::Database(native_error(error)),
            Self::Acquired { error, .. } => error,
        }
    }
}
pub(super) struct DeferredQuery {
    pub(super) limits: ReadLimits,
    pub(super) cause: DeferredCause,
    pub(super) controls: Option<RequestControls>,
    pub(super) node: Option<ReadCredit>,
}

/// Original slot/progress and publication-future allocations. The keeper
/// lives outside all three blocks and outlasts the joined worker. The
/// connection owns one standing keeper exclusively for failed acquisition;
/// each acquired query carries its own keeper from the actual child supplier.
pub(super) struct RequestControls {
    pub(super) slot: Arc<RequestSlot>,
    pub(super) progress: Arc<ProgressRelay>,
    pub(super) registration: CancellationRegistration,
    _invoice: ReadCredit,
}
impl RequestControls {
    #[cfg(unix)]
    pub(super) fn bytes() -> usize {
        fn arc_bytes<T>() -> usize {
            std::alloc::Layout::new::<(std::sync::atomic::AtomicUsize,
                std::sync::atomic::AtomicUsize)>()
                .extend(std::alloc::Layout::new::<T>()).expect("Arc control layout")
                .0.pad_to_align().size()
        }
        arc_bytes::<RequestSlot>() + arc_bytes::<ProgressRelay>() + RequestPublication::bytes()
            + CancellationRegistration::bytes()
    }
    pub(super) fn from_prepaid(mut invoice: ReadCredit, owner: contextdb_core::read_memory::ReadMemoryOwner) -> Self {
        let registration = invoice.try_split_off(CancellationRegistration::bytes())
            .expect("original supplier prepaid cancellation and registration");
        let registration = CancellationRegistration::from_prepaid(owner, registration);
        Self { slot: Arc::new(RequestSlot::default()),
            progress: Arc::new(ProgressRelay::default()), registration, _invoice: invoice }
    }
}

fn node_credit(owner: Arc<dyn ReadMemoryBudget>) -> contextdb_core::Result<ReadCredit> {
    ReadCredit::try_new(owner, queued_event_bytes(), "reader", "request_inbox",
        "Reduce simultaneous requests or raise the declared reader memory limit.")
}

#[cfg(unix)]
pub(super) fn receive(
    stream: &mut impl std::io::Read,
    boundary: &LocalProtocolBoundary,
    reading: &Database,
    inbox: &ConnectionInbox,
) -> Result<ConnectionEvent, crate::local_transport::LocalTransportError> {
    let prefix = RequestPrefix::read(stream)?;
    if !prefix.query {
        // Preserve all other request grammars at the original typed decoder.
        let mut payload = vec![0; prefix.length];
        prefix.complete(stream, &mut payload)?;
        let LocalInboundMessage::Request(envelope) = boundary
            .decode_payload(LocalInboundKind::Request, &payload)? else {
                return Err(crate::local_transport::invalid_channel_data());
            };
        let node = node_credit(reading.query_request_provider().clone())
            .map_err(|error| query_funding::transport_error(error, prefix.limits.memory))?;
        return Ok(ConnectionEvent::Request(ReceivedRequest {
            envelope, query: None, node: Some(node), controls: None,
        }));
    }

    // Only another query needs the single refusal window. Keep receiving
    // interrupts and EOF while its predecessor is in the ordinary waiter.
    if !inbox.wait_refusal_retired() {
        return Err(crate::local_transport::owner_disconnected());
    }
    let (mut acquired, mut prepaid) = match AcquiredQueryOwner::native_with_controls(
        reading.query_request_provider(), RequestControls::bytes() + queued_event_bytes(),
    ) {
        Ok(acquired) => acquired,
        Err(error) => {
            prefix.discard_query(stream)?;
            return Ok(ConnectionEvent::Deferred(DeferredQuery {
                limits: prefix.limits, cause: DeferredCause::MissingChild(error),
                controls: None, node: None,
            }));
        }
    };
    let node = prepaid.try_split_off(queued_event_bytes())
        .expect("child supplier prepaid the exact FIFO node");
    let controls = RequestControls::from_prepaid(prepaid, acquired.funding_owner());
    let limit = prefix.limits.memory.min(boundary.effective_limits().memory);
    let shared = match acquired.prepay_control(ReadShared::<QueryIngress>::layout().size()) {
        Ok(shared) => shared,
        Err(error) => {
            prefix.discard_query(stream)?;
            return Ok(ConnectionEvent::Deferred(DeferredQuery {
                limits: prefix.limits,
                cause: DeferredCause::Acquired { error: error.into(), _ingress: None, _owner: acquired },
                controls: Some(controls), node: Some(node),
            }));
        }
    };
    let operation = match query_funding::query_operation_retained(&acquired, limit) {
        Ok(operation) => operation,
        Err(error) => {
            prefix.discard_query(stream)?;
            return Ok(ConnectionEvent::Deferred(DeferredQuery {
                limits: prefix.limits,
                cause: DeferredCause::Acquired { error: error.into(), _ingress: None, _owner: acquired },
                controls: Some(controls), node: Some(node),
            }));
        }
    };
    // Its invoice is on the native child, not the Arc limiter inside it.
    // No decoder, payload or node alias exists if this allocation fails.
    let ingress = match ReadShared::try_from_prepaid(QueryIngress {
        _decoded: Mutex::new(None), _memory: Mutex::new(None), operation,
        _owner: acquired.clone(),
    }, shared) {
        Ok(ingress) => ingress,
        Err(error) => {
            prefix.discard_query(stream)?;
            return Ok(ConnectionEvent::Deferred(DeferredQuery {
                limits: prefix.limits,
                cause: DeferredCause::Acquired { error: error.into(), _ingress: None, _owner: acquired },
                controls: Some(controls), node: Some(node),
            }));
        }
    };
    // Every growing destination and queue node is admitted before its
    // allocation. All aliases created here die before the ingress keeper.
    let prepared = (|| -> contextdb_core::Result<_> {
        let memory = crate::read_image_memory::ImageMemory::query_reader_for(
            reading.writer_accountant_handle(), ingress.budget.clone())?;
        let payload = Funded::allocate(Some(ingress.budget.clone()), prefix.length,
            || vec![0; prefix.length])?;
        *ingress._memory.lock().unwrap_or_else(|p| p.into_inner()) = Some(memory);
        Ok(payload)
    })();
    let mut payload = match prepared {
        Ok(prepared) => prepared,
        Err(error) => {
            prefix.discard_query(stream)?;
            return Ok(ConnectionEvent::Deferred(DeferredQuery {
                limits: prefix.limits,
                cause: DeferredCause::Acquired {
                    error: error.into(), _ingress: Some(ingress), _owner: acquired,
                },
                controls: Some(controls), node: Some(node),
            }));
        }
    };
    prefix.complete(stream, &mut payload.value)?;
    // The existing typed decoder remains the successful route. It admits
    // each allocation on this exact child, without a separate decoder root.
    let request = {
        let memory = ingress._memory.lock().unwrap_or_else(|p| p.into_inner());
        boundary.decode_query_request(&payload.value,
            &memory.as_ref().expect("prepared query decoder").value)
    };
    let request = match request {
        Ok(request) => request,
        Err(error @ crate::local_transport::LocalTransportError::Payload(
            crate::local_transport::PayloadViolation::MemoryCeilingExceeded { .. },
        )) => {
            // Admission can stop the typed decoder before later JSON, values
            // or limits are checked. Reuse the complete refused-query grammar
            // on these same bytes before treating the request as valid. A
            // syntax/protocol error still takes the original transport path.
            prefix.validate_completed_query(&payload.value)?;
            drop(payload);
            return Ok(ConnectionEvent::Deferred(DeferredQuery {
                limits: prefix.limits,
                cause: DeferredCause::Acquired {
                    error: OwnerReadScaffoldError::LocalTransport(error),
                    _ingress: Some(ingress), _owner: acquired,
                },
                controls: Some(controls), node: Some(node),
            }));
        }
        Err(error) => return Err(error),
    };
    drop(payload);
    *ingress._decoded.lock().unwrap_or_else(|p| p.into_inner()) = request.credit;
    Ok(ConnectionEvent::Request(ReceivedRequest {
        envelope: request.value, query: Some(ingress), node: Some(node), controls: Some(controls),
    }))
}

fn native_error(error: NativeRefusal) -> contextdb_core::Error {
    match error {
        NativeRefusal::MemoryBudgetExceeded {
            subsystem, operation, requested_bytes, available_bytes, budget_limit_bytes, hint,
        } => contextdb_core::Error::MemoryBudgetExceeded {
            subsystem: subsystem.into(), operation: operation.into(), requested_bytes,
            available_bytes, budget_limit_bytes, hint: hint.into(),
        },
        NativeRefusal::Other(reason) => contextdb_core::Error::Other(reason.into()),
        // The connection uses the native declared-reader supplier; these
        // variants are not produced by that supplier.
        NativeRefusal::NativeUnsupported { .. } =>
            contextdb_core::Error::Other("native reader admission unsupported: reader/request_owner".into()),
        NativeRefusal::ProcessMemoryBudgetExceeded {
            operation, requested_bytes, available_bytes, budget_limit_bytes, hint,
        } => contextdb_core::Error::MemoryBudgetExceeded {
            subsystem: "process_native_ceiling".into(), operation: operation.into(),
            requested_bytes, available_bytes, budget_limit_bytes, hint: hint.into(),
        },
    }
}

impl OwnerReadService {
    #[cfg(unix)]
    pub(super) fn dispatch_deferred_query(
        &self, connection_id: [u8; 16], principal: ReadPrincipal,
        limits: ReadLimits, cause: DeferredCause, registration: CancellationRegistration,
    ) -> CompletedOwnerRequest {
        // This is a worker dispatch input, not a synthetic wire Query. The
        // ordinary authenticated handler owns limit/draining/admission/registry
        // precedence and its original ActiveRequest/RequestLease unwind chain.
        self.handle_authenticated(connection_id, principal, limits,
            AuthenticatedWork::Refused(cause), None, registration)
            .unwrap_or_else(CompletedOwnerRequest::terminal)
    }

    pub(super) fn check_serving(&self) -> OwnerReadScaffoldResult<()> {
        let draining = matches!(*self.state.lock().expect("owner service state"),
            ServiceState::DrainingGracefully | ServiceState::Draining);
        if draining || !self.admission.is_accepting() {
            return Err(OwnerReadScaffoldError::Refused(simple_failure(ReadFailureKind::OwnerNotServing)));
        }
        Ok(())
    }
}

#[cfg(unix)]
pub(super) fn send_terminal_error(
    stream: &mut std::os::unix::net::UnixStream, error: &OwnerReadScaffoldError,
) -> OwnerReadScaffoldResult<()> {
    match error {
        OwnerReadScaffoldError::Database(error) =>
            crate::local_transport::write_query_error(stream, error).map_err(Into::into),
        OwnerReadScaffoldError::Refused(failure) =>
            crate::local_transport::write_query_refusal(stream, failure).map_err(Into::into),
        OwnerReadScaffoldError::LocalTransport(error) => {
            use crate::local_transport::{LocalTransportError as E, PayloadViolation};
            let failure = match error {
                E::Refusal(failure) => return crate::local_transport::write_query_refusal(
                    stream, failure).map_err(Into::into),
                E::Payload(PayloadViolation::MemoryCeilingExceeded { ceiling, required }) =>
                    Some(crate::local_transport::memory_ceiling_read_failure(*ceiling, *required)),
                E::Frame(_) | E::Payload(_) => Some(simple_failure(ReadFailureKind::InvalidChannelData)),
                _ => None,
            };
            if let Some(failure) = failure {
                crate::local_transport::write_query_refusal(stream, &failure).map_err(Into::into)
            } else {
                crate::local_transport::write_query_diagnostic(stream,
                    &OwnerTransportDiagnostic(error)).map_err(Into::into)
            }
        }
        other => crate::local_transport::write_query_diagnostic(stream, other).map_err(Into::into),
    }
}

#[cfg(unix)]
pub(super) fn send_terminal_panic(
    stream: &mut std::os::unix::net::UnixStream, payload: &(dyn std::any::Any + Send),
) -> OwnerReadScaffoldResult<()> {
    struct Diagnostic<'a>(&'a (dyn std::any::Any + Send));
    impl std::fmt::Display for Diagnostic<'_> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let reason = self.0.downcast_ref::<&str>().copied()
                .or_else(|| self.0.downcast_ref::<String>().map(String::as_str))
                .unwrap_or("no reason recorded");
            write!(f, "the owner's read ended unexpectedly: {reason}")
        }
    }
    crate::local_transport::write_query_diagnostic(stream, &Diagnostic(payload)).map_err(Into::into)
}

struct OwnerTransportDiagnostic<'a>(&'a crate::local_transport::LocalTransportError);
impl std::fmt::Display for OwnerTransportDiagnostic<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "local transport prerequisite failed: {}", self.0)
    }
}

#[cfg(all(test, unix))]
mod completed_query_refusal_tests {
    use super::*;
    use crate::local_transport::{
        encode_message, encode_payload_frame, LocalInboundKind, LocalInboundMessage,
        LocalResponseExpectation, UnixLocalCarrier,
        LocalTransportError, PayloadViolation,
    };
    use contextdb_core::{Value, TxId};
    use std::collections::BTreeMap;
    use std::io::Cursor;

    fn envelope(value: Value, memory: u64) -> LocalRequestEnvelope {
        LocalRequestEnvelope {
            limits: ReadLimits { memory, ..ReadLimits::default() },
            request: LocalRequest::Query {
                statement: "SELECT 1".into(),
                params: BTreeMap::from([("a".into(), value)]),
            },
        }
    }

    fn expanded_json() -> Value {
        // Compact wire text, but thousands of real native JSON elements.
        // This exercises decoder admission after the carrier fits, without
        // replacing the parser, injecting an Error, or guessing a layout size.
        Value::Json(serde_json::Value::Array(vec![serde_json::json!(0); 8192]))
    }

    fn payload(value: Value, memory: u64) -> Vec<u8> {
        encode_message(&envelope(value, memory)).expect("real request serializer")
    }

    fn replace_once(bytes: &[u8], from: &[u8], to: &[u8]) -> Vec<u8> {
        let positions: Vec<_> = bytes.windows(from.len()).enumerate()
            .filter_map(|(index, part)| (part == from).then_some(index)).collect();
        assert_eq!(positions.len(), 1, "mutation must identify one serialized field");
        let mut changed = bytes.to_vec();
        changed.splice(positions[0]..positions[0] + from.len(), to.iter().copied());
        changed
    }

    fn typed(bytes: &[u8]) -> Result<(), LocalTransportError> {
        let memory = query_funding::QueryMemory::new(ReadLimits::SHIPPED_MEMORY)
            .expect("real native query owner");
        LocalProtocolBoundary::with_effective_limits(ReadLimits::default())
            .decode_query_request(bytes, &memory.image()).map(drop)
    }

    // Acceptance: complete refused frames use the existing typed grammar,
    // including limits, JSON end(), native values and exact frame exhaustion.
    // Only pre-existing APIs are named, so completed691 must compile too.
    // Decisive mutants: skip discard validation / JSON end / limit validation.
    // This is a new acceptance matrix, not another response-send timing row.
    #[test]
    fn completed_query_validation_matches_the_real_typed_decoder() {
        let limit = 64 * 1024;
        let database = Database::open_memory();
        let inbox = ConnectionInbox::new(None);
        let before = database.query_request_provider().charged_bytes();
        let mut cases = Vec::new();
        for value in [
            Value::Null, Value::Bool(true), Value::Int64(i64::MIN),
            Value::Int64(i64::MAX), Value::Float64(-3.5),
            Value::Text("é水😀".into()), Value::Uuid(uuid::Uuid::nil()),
            Value::Timestamp(i64::MIN), Value::TxId(TxId(u64::MAX)),
            Value::Vector(vec![-1.0, 0.0, 1.5]),
            Value::Json(serde_json::json!({"é": [null, true, -1, 2.5, "😀"]})),
            expanded_json(),
        ] {
            cases.push(("valid typed value", payload(value, limit), true));
        }
        let json = payload(Value::Json(serde_json::json!([1, 2, 3])), limit);
        for bad in [b"[1,2,,]".as_slice(), b"[1,2,3 ", b"[1] 0  "] {
            cases.push(("malformed JSON or JSON trailing token",
                replace_once(&json, b"[1,2,3]", bad), false));
        }
        let string = payload(Value::Json(serde_json::json!("ABCD")), limit);
        for bad in [b"\"\\uDC00\"".as_slice(), b"\"\\uD800\"", b"\"\\x0000\"", b"\"\x01BCD\""] {
            // The JSON string's encoded length is intentionally kept fixed
            // for the six-byte variant below; longer bad strings are separate
            // equal-length mutations of a serializer-produced string.
            if bad.len() == 6 {
                cases.push(("JSON control character", replace_once(&string, b"\"ABCD\"", bad), false));
            } else {
                let base = payload(Value::Json(serde_json::json!("ABCDEF")), limit);
                cases.push(("JSON invalid escape or surrogate",
                    replace_once(&base, b"\"ABCDEF\"", bad), false));
            }
        }
        for value in [Value::Uuid(uuid::Uuid::nil()), Value::Vector(vec![1.0, 2.0])] {
            let mut truncated = payload(value, limit);
            truncated.pop();
            cases.push(("truncated native value", truncated, false));
        }
        let mut invalid_bool = payload(Value::Bool(true), limit);
        *invalid_bool.last_mut().unwrap() = 2;
        cases.push(("invalid bool", invalid_bool, false));
        let mut invalid_tag = payload(Value::Null, limit);
        *invalid_tag.last_mut().unwrap() = 10;
        cases.push(("invalid Value tag", invalid_tag, false));
        let mut invalid_integer = payload(Value::TxId(TxId(0)), limit);
        *invalid_integer.last_mut().unwrap() = 254;
        cases.push(("invalid integer width", invalid_integer, false));
        let text = payload(Value::Text("UNIQUE-UTF8".into()), limit);
        cases.push(("invalid UTF8", replace_once(&text, b"UNIQUE-UTF8", b"UNIQUE-UT\xff8"), false));
        let mut trailing = payload(Value::Null, limit);
        trailing.push(0);
        cases.push(("frame trailing byte", trailing, false));
        for field in 0..9 {
            let mut request = envelope(Value::Null, limit);
            match field {
                0 => request.limits.result_rows = 0,
                1 => request.limits.result_bytes = 0,
                2 => request.limits.work = 0,
                3 => request.limits.active_ms = 0,
                4 => request.limits.memory = 0,
                5 => request.limits.cursor_page_rows = 0,
                6 => request.limits.cursor_page_bytes = 0,
                7 => request.limits.cursor_idle_ms = 0,
                _ => request.limits.cursor_lifetime_ms = 0,
            }
            cases.push(("zero limit", encode_message(&request).unwrap(), false));
        }
        for field in 0..3 {
            let mut request = envelope(Value::Null, limit);
            match field {
                0 => request.limits.cursor_page_rows = request.limits.result_rows + 1,
                1 => request.limits.cursor_page_bytes = request.limits.result_bytes + 1,
                _ => request.limits.cursor_idle_ms = request.limits.cursor_lifetime_ms + 1,
            }
            cases.push(("limit relationship", encode_message(&request).unwrap(), false));
        }
        for (name, bytes, valid) in cases {
            let decoded = typed(&bytes);
            assert_eq!(decoded.is_ok(), valid, "typed control: {name}");
            let received = receive_bytes(&bytes, &database, &inbox);
            assert_eq!(received.is_ok(), valid,
                "real inbound request or deferred refusal: {name}");
            if !valid {
                let error = match received { Err(error) => error, Ok(_) => unreachable!() };
                match (decoded.unwrap_err(), error) {
                    (LocalTransportError::Payload(a), LocalTransportError::Payload(b)) => assert_eq!(a, b, "payload diagnosis: {name}"),
                    (LocalTransportError::Refusal(a), LocalTransportError::Refusal(b)) => assert_eq!(a, b, "typed diagnosis: {name}"),
                    (a, b) => panic!("original error precedence changed for {name}: {a:?} / {b:?}"),
                }
            } else { drop(received); }
            assert_eq!(database.query_request_provider().charged_bytes(), before,
                "request, partial DTO and child released: {name}");
            let frame = encode_payload_frame(&bytes).unwrap();
            let mut input = Cursor::new(frame);
            let prefix = RequestPrefix::read(&mut input).expect("valid prefix in matrix");
            assert_eq!(prefix.discard_query(&mut input).is_ok(), valid,
                "original streamed validator: {name}");
        }
    }

    fn receive_bytes(bytes: &[u8], database: &Database, inbox: &ConnectionInbox)
        -> Result<ConnectionEvent, LocalTransportError>
    {
        let frame = encode_payload_frame(bytes).unwrap();
        let mut input = Cursor::new(frame.clone());
        let result = receive(&mut input,
            &LocalProtocolBoundary::with_effective_limits(ReadLimits::default()), database, inbox);
        assert_eq!(input.position() as usize, frame.len(), "one complete payload consumed");
        result
    }

    // Acceptance: actual decoder memory refusal keeps the acquired child,
    // original queue credit and controls, without becoming reader Ended.
    // Decisive mutant: restore the completed691 decode_query_request(...)?;
    // No earlier mapped test exercises this completed-payload failure branch.
    #[test]
    fn actual_decoder_refusal_keeps_child_fifo_and_exact_transport_diagnosis() {
        for constrain_parent in [false, true] {
        let database = Database::open_memory();
        let parent = database.query_request_provider();
        let before = parent.charged_bytes();
        let inbox = ConnectionInbox::new(None);
        let original = before;
        let pressure = if constrain_parent {
            Some(ReadCredit::try_new(parent.clone(),
                (ReadLimits::SHIPPED_MEMORY - before - 64 * 1024) as usize,
                "reader", "test_pressure", "test pressure").unwrap())
        } else { None };
        let before = parent.charged_bytes();
        let request_memory = if constrain_parent { ReadLimits::SHIPPED_MEMORY } else { 64 * 1024 };
        let bytes = payload(expanded_json(), request_memory);
        assert!(typed(&bytes).is_ok(), "same bytes decode with available memory");
        let event = receive_bytes(&bytes, &database, &inbox)
            .unwrap_or_else(|error| panic!("a genuine decoder refusal must be deferred: {error:?}"));
        let ConnectionEvent::Deferred(deferred) = &event else { panic!("decoder failure must defer") };
        let DeferredCause::Acquired { _ingress, _owner, .. } = &deferred.cause
            else { panic!("the decoder already acquired a child") };
        assert!(_owner.funding_owner().is_native());
        assert!(_ingress.is_some());
        assert!(deferred.controls.is_some());
        assert!(deferred.node.is_some());
        assert_eq!(deferred.limits.memory, request_memory);
        let ingress = _ingress.as_ref().unwrap();
        let memory = ingress._memory.lock().unwrap();
        assert!(memory.is_some(), "prepared full payload reached the real decoder");
        assert_eq!(memory.as_ref().unwrap().value.held(), 0,
            "partial decoder allocations returned before deferred publication");
        drop(memory);
        assert!(parent.charged_bytes() > before, "real child remains retained");
        // A real earlier request precedes the deferred event, then an actual
        // later request. Inspect the original queue, not a parallel wrapper.
        let first = receive_bytes(&payload(Value::Null, ReadLimits::SHIPPED_MEMORY), &database, &inbox).unwrap();
        let last = receive_bytes(&payload(Value::Bool(true), ReadLimits::SHIPPED_MEMORY), &database, &inbox).unwrap();
        inbox.push(first);
        inbox.push(event);
        inbox.push(last);
        assert!(front_interrupt(&inbox.state.lock().unwrap()).is_none());
        assert!(matches!(inbox.blocking_next(), ConnectionEvent::Request(_)));
        let ConnectionEvent::Deferred(deferred) = inbox.blocking_next()
            else { panic!("the real deferred event stays second") };
        let (ingress_hold, owner_hold) = match &deferred.cause {
            DeferredCause::Acquired { _ingress, _owner, .. } =>
                (_ingress.clone(), _owner.clone()),
            _ => panic!("acquired custody survives FIFO"),
        };
        let controls = deferred.controls;
        let node = deferred.node;
        // Both endpoint types convert through existing From implementations.
        // These APIs exist on completed691 as well as the repair;
        // the old receive must fail at runtime, not on a changed carrier type.
        let error: OwnerReadScaffoldError = deferred.cause.into_error().into();
        let OwnerReadScaffoldError::LocalTransport(LocalTransportError::Payload(
            PayloadViolation::MemoryCeilingExceeded { ceiling, required })) = &error
            else { panic!("must retain exact decoder transport class, not Database") };
        assert_eq!(*ceiling, ReadLimits::SHIPPED_MEMORY);
        assert_eq!(*required, None);
        let (mut terminal, mut peer) = std::os::unix::net::UnixStream::pair().unwrap();
        send_terminal_error(&mut terminal, &error).expect("existing terminal serializer");
        let LocalInboundMessage::Response(LocalResponse::Failure { failure }) =
            UnixLocalCarrier.receive_message(&mut peer,
                &LocalProtocolBoundary::with_effective_limits(ReadLimits::default()),
                LocalInboundKind::Response(LocalResponseExpectation::OrdinaryResult)).unwrap()
            else { panic!("typed transport refusal") };
        assert_eq!(failure, crate::local_transport::memory_ceiling_read_failure(*ceiling, *required));

        assert!(parent.charged_bytes() > before, "original acquired child funds terminal custody");
        drop(error);
        drop(controls);
        drop(node);
        drop(ingress_hold);
        drop(owner_hold);
        inbox.retire_refusal();
        assert!(matches!(inbox.blocking_next(), ConnectionEvent::Request(_)));
        assert_eq!(parent.charged_bytes(), before, "all original child custody released");

        // Later invalid JSON and invalid limits must beat the earlier decode
        // allocation refusal; a subsequent frame is still consumed exactly.
        let json = serde_json::to_vec(&serde_json::Value::Array(vec![serde_json::json!(0); 8192])).unwrap();
        let mut bad_json = json.clone();
        *bad_json.last_mut().unwrap() = b',';
        let malformed = replace_once(&bytes, &json, &bad_json);
        let mut invalid_limits = envelope(expanded_json(), request_memory);
        invalid_limits.limits.work = 0;
        for bad in [malformed, encode_message(&invalid_limits).unwrap()] {
            assert!(matches!(receive_bytes(&bad, &database, &inbox),
                Err(LocalTransportError::Refusal(failure))
                    if failure.kind() == ReadFailureKind::InvalidChannelData));
            assert_eq!(parent.charged_bytes(), before);
        }
        drop(pressure);
        assert_eq!(parent.charged_bytes(), original);
        }
    }

    // Acceptance: failed acquisition has no acquired child and keeps standing
    // connection custody distinct from a completed-payload decoder refusal.
    // Decisive mutant: fabricate acquired custody after the native supplier fails.
    #[test]
    fn failed_child_acquisition_has_no_child_and_does_not_consume_the_next_frame() {
        let database = Database::open_memory();
        let parent = database.query_request_provider();
        let before = parent.charged_bytes();
        let pressure = ReadCredit::try_new(parent.clone(),
            (ReadLimits::SHIPPED_MEMORY - before) as usize, "reader", "test_pressure", "test pressure").unwrap();
        let held = parent.charged_bytes();
        let inbox = ConnectionInbox::new(None);
        let bytes = payload(Value::Null, ReadLimits::SHIPPED_MEMORY);
        let frame = encode_payload_frame(&bytes).unwrap();
        let mut frames = frame.clone();
        frames.extend_from_slice(&frame);
        let mut input = Cursor::new(frames);
        let boundary = LocalProtocolBoundary::with_effective_limits(ReadLimits::default());
        let event = receive(&mut input, &boundary, &database, &inbox).unwrap();
        assert_eq!(input.position() as usize, frame.len());
        let ConnectionEvent::Deferred(deferred) = event else { panic!("failed child must defer") };
        assert!(matches!(&deferred.cause, DeferredCause::MissingChild(
            NativeRefusal::MemoryBudgetExceeded { operation: "request_owner", .. })));
        assert!(deferred.controls.is_none());
        assert!(deferred.node.is_none());
        drop(deferred);
        assert_eq!(parent.charged_bytes(), held);
        drop(pressure);
        assert!(matches!(receive(&mut input, &boundary, &database, &inbox).unwrap(), ConnectionEvent::Request(_)));
        assert_eq!(input.position() as usize, 2 * frame.len());
        assert_eq!(parent.charged_bytes(), before);
    }
}
