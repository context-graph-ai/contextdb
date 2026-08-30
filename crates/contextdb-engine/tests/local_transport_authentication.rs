use contextdb_core::read_contract::{
    DatabaseIdentity, LocalUserIdentity, ReadFailure, ReadFailureDetail, ReadFailureKind,
    WriterRunNumber,
};
#[cfg(unix)]
use contextdb_engine::local_transport::{
    AdmittedReader, DeadlineStage, ManualDeadlineClock, UnixLocalCarrier,
    authenticate_framed_stream_handshake, encode_message, encode_payload_frame,
    peer_user_from_stream,
};
use contextdb_engine::local_transport::{
    LocalHandshake, LocalTransportError, ReadPrincipal, authenticate_peer,
};

#[cfg(unix)]
fn run_ready<F: std::future::Future>(future: F) -> F::Output {
    struct YieldWake;
    impl std::task::Wake for YieldWake {
        fn wake(self: std::sync::Arc<Self>) {}
    }

    let waker = std::task::Waker::from(std::sync::Arc::new(YieldWake));
    let mut context = std::task::Context::from_waker(&waker);
    let mut future = Box::pin(future);
    for _ in 0..100_000 {
        if let std::task::Poll::Ready(output) = future.as_mut().poll(&mut context) {
            return output;
        }
        std::thread::yield_now();
    }
    panic!("bounded local operation did not become ready")
}

fn refusal(kind: ReadFailureKind) -> LocalTransportError {
    LocalTransportError::Refusal(
        ReadFailure::new(kind, ReadFailureDetail::None)
            .expect("this refusal accepts an empty detail"),
    )
}

#[test]
fn same_user_cross_user_and_unavailable_credentials_have_distinct_outcomes() {
    let owner = LocalUserIdentity(41);
    assert_eq!(
        authenticate_peer(owner, Ok(owner)),
        Ok(ReadPrincipal::LocalUser(owner))
    );
    assert_eq!(
        authenticate_peer(owner, Ok(LocalUserIdentity(owner.0 + 1))),
        Err(refusal(ReadFailureKind::OwnerUserMismatch))
    );
    assert_eq!(
        authenticate_peer(owner, Err(LocalTransportError::CredentialsUnavailable)),
        Err(LocalTransportError::CredentialsUnavailable)
    );
}

#[cfg(unix)]
fn current_user() -> LocalUserIdentity {
    LocalUserIdentity(nix::unistd::geteuid().as_raw() as u64)
}

#[cfg(unix)]
fn expected_handshake() -> LocalHandshake {
    let mut handshake = LocalHandshake::current(
        DatabaseIdentity([0x11; 16]),
        WriterRunNumber([0x22; 16]),
        current_user(),
    );
    handshake.owner_user = current_user();
    handshake
}

#[cfg(unix)]
fn framed_handshake_for_user(user: LocalUserIdentity) -> Vec<u8> {
    let handshake = LocalHandshake::current(
        DatabaseIdentity([0x11; 16]),
        WriterRunNumber([0x22; 16]),
        user,
    );
    let payload = encode_message(&handshake).expect("encode the presented handshake");
    encode_payload_frame(&payload).expect("frame the presented handshake")
}

#[cfg(unix)]
fn framed_handshake_for_current_user() -> Vec<u8> {
    framed_handshake_for_user(current_user())
}

#[cfg(unix)]
fn authenticate_fixture(
    expected: LocalHandshake,
    framed_bytes: Vec<u8>,
) -> (
    LocalUserIdentity,
    Result<AdmittedReader, LocalTransportError>,
) {
    use std::io::Write;

    let root = tempfile::tempdir().expect("temporary authenticated channel root");
    let path = root.path().join("owner.sock");
    let carrier = UnixLocalCarrier;
    let listener = carrier
        .listen(&path)
        .expect("listen through the Unix carrier");
    listener
        .set_nonblocking(true)
        .expect("make test accept nonblocking");
    let client_carrier = carrier;
    let client_path = path.clone();
    let client = std::thread::spawn(move || -> Result<(), String> {
        let clock = ManualDeadlineClock::at(100);
        let connect = client_carrier.connect(&client_path, &clock, 105);
        assert_eq!(connect.stage(), DeadlineStage::Connect);
        let mut stream = run_ready(connect).map_err(|error| error.to_string())?;
        stream
            .write_all(&framed_bytes)
            .map_err(|error| error.to_string())?;
        stream
            .shutdown(std::net::Shutdown::Write)
            .map_err(|error| error.to_string())?;
        Ok(())
    });
    let mut accepted = None;
    for _ in 0..100_000 {
        match listener.accept() {
            Ok(connection) => {
                accepted = Some(connection);
                break;
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                if client.is_finished() {
                    let outcome = client.join().expect("carrier client thread completes");
                    panic!("carrier client ended before acceptance: {outcome:?}");
                }
                std::thread::yield_now();
            }
            Err(error) => panic!("accept carrier connection: {error}"),
        }
    }
    let (mut server, _) = accepted.expect("carrier connection becomes acceptable");
    let observed = peer_user_from_stream(&server).expect("extract the actual peer user");
    let clock = ManualDeadlineClock::at(100);
    let handshake = authenticate_framed_stream_handshake(&mut server, &expected, &clock, 105);
    assert_eq!(handshake.stage(), DeadlineStage::Handshake);
    let authenticated = run_ready(handshake);
    client
        .join()
        .expect("handshake writer thread completes")
        .expect("handshake writer completes");
    (observed, authenticated)
}

#[test]
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn carrier_credentials_and_framed_handshake_authenticate_the_actual_same_user() {
    let expected = expected_handshake();
    let (observed, authenticated) =
        authenticate_fixture(expected.clone(), framed_handshake_for_current_user());
    assert_eq!(observed, current_user());
    assert_eq!(
        authenticated.map(|admitted| admitted.principal),
        Ok(ReadPrincipal::LocalUser(current_user()))
    );
}

#[test]
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn actual_peer_credentials_refuse_a_recorded_different_owner_user() {
    let mut expected = expected_handshake();
    expected.owner_user = LocalUserIdentity(current_user().0 + 1);
    let (observed, authenticated) =
        authenticate_fixture(expected, framed_handshake_for_current_user());
    assert_eq!(observed, current_user());
    assert_eq!(
        authenticated,
        Err(refusal(ReadFailureKind::OwnerUserMismatch))
    );
}

#[test]
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn every_authenticated_handshake_identity_field_is_checked_independently() {
    let expected = expected_handshake();
    let base = framed_handshake_for_current_user();
    let cases = [
        (4, ReadFailureKind::LocalProtocolMismatch, "marker"),
        (24, ReadFailureKind::LocalProtocolMismatch, "version"),
        (25, ReadFailureKind::OwnerMismatch, "database identity"),
        (41, ReadFailureKind::OwnerMismatch, "writer run"),
    ];

    for (offset, expected_kind, field) in cases {
        let mut mutated = base.clone();
        mutated[offset] ^= 1;
        let (observed, authenticated) = authenticate_fixture(expected.clone(), mutated);
        assert_eq!(observed, current_user());
        assert_eq!(
            authenticated,
            Err(refusal(expected_kind)),
            "the {field} mutation must be refused before admission"
        );
    }

    let (_, authenticated) = authenticate_fixture(
        expected,
        framed_handshake_for_user(LocalUserIdentity(current_user().0 + 1)),
    );
    assert_eq!(
        authenticated,
        Err(refusal(ReadFailureKind::OwnerUserMismatch)),
        "the presented owner user mutation must be refused before admission"
    );
}

#[test]
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn malformed_and_truncated_framed_handshakes_are_channel_data_refusals() {
    let expected = expected_handshake();
    let cases = [
        vec![0, 0, 0, 2, 0xff, 0xfe],
        vec![0, 0, 0, 54, 0x63, 0x6f, 0x6e],
        vec![0, 0, 0],
    ];
    for bytes in cases {
        let (observed, authenticated) = authenticate_fixture(expected.clone(), bytes);
        assert_eq!(observed, current_user());
        assert_eq!(
            authenticated,
            Err(refusal(ReadFailureKind::InvalidChannelData))
        );
    }
}

#[test]
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn stalled_production_handshake_expires_at_the_exact_manual_boundary_without_peer_release() {
    use std::future::Future;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll, Wake, Waker};

    struct ThreadWake {
        thread: std::thread::Thread,
        wakes: Arc<AtomicUsize>,
    }

    impl Wake for ThreadWake {
        fn wake(self: Arc<Self>) {
            self.wakes.fetch_add(1, Ordering::SeqCst);
            self.thread.unpark();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.wakes.fetch_add(1, Ordering::SeqCst);
            self.thread.unpark();
        }
    }

    let root = tempfile::tempdir().expect("temporary stalled handshake root");
    let path = root.path().join("owner.sock");
    let carrier = UnixLocalCarrier;
    let listener = carrier
        .listen(&path)
        .expect("listen through production carrier");
    let connect_clock = ManualDeadlineClock::at(100);
    let connect = carrier.connect(&path, &connect_clock, 105);
    assert_eq!(connect.stage(), DeadlineStage::Connect);
    let client = run_ready(connect).expect("connect through production carrier");
    let (mut server, _) = listener.accept().expect("accept production connection");

    let clock = ManualDeadlineClock::at(200);
    let server_clock = clock.clone();
    let wakes = Arc::new(AtomicUsize::new(0));
    let server_wakes = Arc::clone(&wakes);
    let expected = expected_handshake();
    let operation = std::thread::spawn(move || {
        let waker = Waker::from(Arc::new(ThreadWake {
            thread: std::thread::current(),
            wakes: server_wakes,
        }));
        let mut context = Context::from_waker(&waker);
        let handshake =
            authenticate_framed_stream_handshake(&mut server, &expected, &server_clock, 205);
        assert_eq!(handshake.stage(), DeadlineStage::Handshake);
        let mut future = Box::pin(handshake);
        loop {
            match future.as_mut().poll(&mut context) {
                Poll::Ready(output) => break output,
                Poll::Pending => std::thread::park(),
            }
        }
    });

    for _ in 0..100_000 {
        if clock.registered_waiter_count() == 1 || operation.is_finished() {
            break;
        }
        std::thread::yield_now();
    }
    if clock.registered_waiter_count() != 1 {
        drop(client);
        operation.thread().unpark();
        let early = operation
            .join()
            .expect("early handshake operation completes");
        panic!("production handshake did not register its named deadline: {early:?}");
    }
    assert_eq!(clock.registrations_created(), 1);
    clock.advance_to(204);
    assert!(!operation.is_finished());
    assert_eq!(wakes.load(Ordering::SeqCst), 0);
    clock.advance_to(205);
    assert_eq!(
        operation.join().expect("deadline-terminated handshake"),
        Err(refusal(ReadFailureKind::OwnerTimeout))
    );
    assert_eq!(wakes.load(Ordering::SeqCst), 1);
    assert_eq!(clock.registered_waiter_count(), 0);
    drop(client);
}

#[test]
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn peer_uid_mismatch_refuses_before_a_silent_peer_can_consume_the_handshake_deadline() {
    use std::future::Future;
    use std::sync::Arc;
    use std::task::{Context, Poll, Wake, Waker};

    struct NoopWake;

    impl Wake for NoopWake {
        fn wake(self: Arc<Self>) {}
    }

    let root = tempfile::tempdir().expect("temporary silent-peer root");
    let path = root.path().join("owner.sock");
    let carrier = UnixLocalCarrier;
    let listener = carrier
        .listen(&path)
        .expect("listen through production carrier");
    let connect_clock = ManualDeadlineClock::at(100);
    let client = run_ready(carrier.connect(&path, &connect_clock, 105))
        .expect("connect silent peer through production carrier");
    let (mut server, _) = listener.accept().expect("accept silent production peer");
    assert_eq!(peer_user_from_stream(&server), Ok(current_user()));

    let mut expected = expected_handshake();
    expected.owner_user = LocalUserIdentity(current_user().0.wrapping_add(1));
    let clock = ManualDeadlineClock::at(200);
    UnixLocalCarrier::reset_frame_readiness_creations_for_test();
    LocalHandshake::reset_decode_entries_for_test();
    let operation = authenticate_framed_stream_handshake(&mut server, &expected, &clock, 205);
    assert_eq!(operation.stage(), DeadlineStage::Handshake);
    assert_eq!(
        UnixLocalCarrier::frame_readiness_creations_for_test(),
        0,
        "constructing a mismatched-UID authentication must not create frame readiness or its read worker"
    );
    assert_eq!(LocalHandshake::decode_entries_for_test(), 0);
    let mut operation = Box::pin(operation);
    let waker = Waker::from(Arc::new(NoopWake));
    let mut context = Context::from_waker(&waker);

    let observed = operation.as_mut().poll(&mut context);
    assert_eq!(
        UnixLocalCarrier::frame_readiness_creations_for_test(),
        0,
        "the immediate UID refusal must not start a handshake read while it is polled"
    );
    assert_eq!(
        LocalHandshake::decode_entries_for_test(),
        0,
        "the immediate UID refusal must not enter the exact handshake decoder"
    );
    assert_eq!(
        observed,
        Poll::Ready(Err(refusal(ReadFailureKind::OwnerUserMismatch))),
        "the peer UID mismatch is decided before readiness, frame decoding, or the handshake timeout"
    );
    assert_eq!(clock.registered_waiter_count(), 0);
    assert_eq!(clock.registrations_created(), 0);
    drop(operation);
    drop(client);
}
