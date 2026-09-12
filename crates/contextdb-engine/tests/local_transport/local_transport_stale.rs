use contextdb_engine::local_transport::{
    ChannelFilesystemIdentity, LivenessEvidence, StaleChannelAction, StaleChannelEvidence,
    StaleFilesystemEvidence, StaleIdentityEvidence, decide_stale_channel_action,
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
    panic!("bounded carrier operation did not become ready")
}
#[cfg(unix)]
use contextdb_core::read_contract::{
    DatabaseIdentity, LocalUserIdentity, ReadFailure, ReadFailureDetail, ReadFailureKind,
    WriterRunNumber,
};
#[cfg(unix)]
use contextdb_engine::local_transport::{
    FinalStaleIdentityObservation, LocalHandshake, LocalTransportError, ManualDeadlineClock,
    StaleRemovalInterlock, UnixLocalCarrier, channel_filesystem_identity, encode_message,
    encode_payload_frame,
};

fn filesystem() -> StaleFilesystemEvidence {
    StaleFilesystemEvidence::Exact(ChannelFilesystemIdentity {
        device: 11,
        inode: 12,
    })
}

#[test]
fn stale_channel_removal_requires_identity_liveness_and_exact_filesystem_evidence() {
    let cases = [
        (
            StaleChannelEvidence {
                identity: StaleIdentityEvidence::VerifiedOwner,
                liveness: LivenessEvidence::NoResponderBeforeDeadline,
                filesystem: filesystem(),
            },
            StaleChannelAction::RemoveAndRetry,
        ),
        (
            StaleChannelEvidence {
                identity: StaleIdentityEvidence::VerifiedOwner,
                liveness: LivenessEvidence::LiveResponder,
                filesystem: filesystem(),
            },
            StaleChannelAction::Preserve,
        ),
        (
            StaleChannelEvidence {
                identity: StaleIdentityEvidence::WrongResponder,
                liveness: LivenessEvidence::NoResponderBeforeDeadline,
                filesystem: filesystem(),
            },
            StaleChannelAction::Preserve,
        ),
        (
            StaleChannelEvidence {
                identity: StaleIdentityEvidence::Unverifiable,
                liveness: LivenessEvidence::NoResponderBeforeDeadline,
                filesystem: filesystem(),
            },
            StaleChannelAction::Preserve,
        ),
        (
            StaleChannelEvidence {
                identity: StaleIdentityEvidence::VerifiedOwner,
                liveness: LivenessEvidence::NoResponderBeforeDeadline,
                filesystem: StaleFilesystemEvidence::Unverifiable,
            },
            StaleChannelAction::Preserve,
        ),
    ];

    for (evidence, expected) in cases {
        assert_eq!(decide_stale_channel_action(evidence), Ok(expected));
    }
}

#[cfg(unix)]
fn current_user() -> LocalUserIdentity {
    LocalUserIdentity(nix::unistd::geteuid().as_raw() as u64)
}

#[cfg(unix)]
fn expected_owner() -> LocalHandshake {
    LocalHandshake::current(
        DatabaseIdentity([0x11; 16]),
        WriterRunNumber([0x22; 16]),
        current_user(),
    )
}

#[cfg(unix)]
fn framed_handshake(handshake: &LocalHandshake) -> Vec<u8> {
    let payload = encode_message(handshake).expect("encode responder handshake");
    encode_payload_frame(&payload).expect("frame responder handshake")
}

#[cfg(unix)]
fn spawn_responder(
    listener: std::os::unix::net::UnixListener,
    bytes: Vec<u8>,
) -> (
    std::sync::Arc<std::sync::atomic::AtomicBool>,
    std::thread::JoinHandle<()>,
) {
    use std::io::Write;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    listener
        .set_nonblocking(true)
        .expect("make responder accept nonblocking");
    let stop = Arc::new(AtomicBool::new(false));
    let thread_stop = Arc::clone(&stop);
    let handle = std::thread::spawn(move || {
        loop {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    stream.write_all(&bytes).expect("write responder identity");
                    break;
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    if thread_stop.load(Ordering::SeqCst) {
                        break;
                    }
                    std::thread::yield_now();
                }
                Err(error) => panic!("accept bounded probe: {error}"),
            }
        }
    });
    (stop, handle)
}

#[cfg(unix)]
fn owner_mismatch() -> LocalTransportError {
    LocalTransportError::Refusal(
        ReadFailure::new(ReadFailureKind::OwnerMismatch, ReadFailureDetail::None)
            .expect("owner mismatch accepts an empty detail"),
    )
}

#[test]
#[cfg(unix)]
fn actual_stale_socket_is_removed_only_with_evidence_for_its_exact_identity() {
    let root = tempfile::tempdir().expect("temporary stale-channel root");
    let path = root.path().join("stale.sock");
    let carrier = UnixLocalCarrier;
    let listener = carrier
        .listen(&path)
        .expect("create stale channel through carrier");
    let identity = channel_filesystem_identity(&path).expect("inspect stale channel identity");
    drop(listener);

    let clock = ManualDeadlineClock::at(100);
    let evidence = carrier
        .probe_stale(&path, &expected_owner(), &clock, 105)
        .expect("bounded stale probe");
    assert_eq!(evidence.identity, StaleIdentityEvidence::VerifiedOwner);
    assert_eq!(
        evidence.liveness,
        LivenessEvidence::NoResponderBeforeDeadline
    );
    assert_eq!(
        evidence.filesystem,
        StaleFilesystemEvidence::Exact(identity)
    );
    assert_eq!(
        carrier.remove_after_proven_stale(&path, evidence),
        Ok(StaleChannelAction::RemoveAndRetry)
    );
    assert!(!path.exists());
}

#[test]
#[cfg(unix)]
fn live_expected_and_live_wrong_responders_are_never_removed() {
    let root = tempfile::tempdir().expect("temporary live-channel root");
    let carrier = UnixLocalCarrier;
    let clock = ManualDeadlineClock::at(200);

    let live_path = root.path().join("live.sock");
    let live_listener = carrier
        .listen(&live_path)
        .expect("listen for expected responder");
    let (live_stop, live_thread) =
        spawn_responder(live_listener, framed_handshake(&expected_owner()));
    let live_result = carrier.probe_stale(&live_path, &expected_owner(), &clock, 205);
    live_stop.store(true, std::sync::atomic::Ordering::SeqCst);
    live_thread.join().expect("expected responder completes");
    let live = live_result.expect("probe expected responder");
    assert_eq!(live.liveness, LivenessEvidence::LiveResponder);
    assert_eq!(
        carrier.remove_after_proven_stale(&live_path, live),
        Ok(StaleChannelAction::Preserve)
    );
    assert!(live_path.exists());

    let wrong_path = root.path().join("wrong.sock");
    let wrong_listener = carrier
        .listen(&wrong_path)
        .expect("listen for wrong responder");
    let mut wrong = expected_owner();
    wrong.writer_run.0[0] ^= 1;
    let (wrong_stop, wrong_thread) = spawn_responder(wrong_listener, framed_handshake(&wrong));
    let wrong_result = carrier.probe_stale(&wrong_path, &expected_owner(), &clock, 205);
    wrong_stop.store(true, std::sync::atomic::Ordering::SeqCst);
    wrong_thread.join().expect("wrong responder completes");
    assert_eq!(wrong_result, Err(owner_mismatch()));
    assert!(wrong_path.exists());
}

#[test]
#[cfg(unix)]
fn unverifiable_non_socket_path_is_preserved() {
    let root = tempfile::tempdir().expect("temporary unverifiable-channel root");
    let path = root.path().join("not-a-socket");
    std::fs::write(&path, b"not a local channel").expect("write regular file");
    let carrier = UnixLocalCarrier;
    let clock = ManualDeadlineClock::at(300);
    assert_eq!(
        carrier.probe_stale(&path, &expected_owner(), &clock, 305),
        Err(LocalTransportError::StaleChannelUnverifiable)
    );
    assert!(path.exists());
}

#[test]
#[cfg(unix)]
fn stalled_responder_finishes_at_the_manual_deadline_without_wall_clock_waiting() {
    let root = tempfile::tempdir().expect("temporary stalled-channel root");
    let path = root.path().join("stalled.sock");
    let carrier = UnixLocalCarrier;
    let listener = carrier.listen(&path).expect("listen for stalled responder");
    listener
        .set_nonblocking(true)
        .expect("make stalled accept nonblocking");
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let responder_stop = std::sync::Arc::clone(&stop);
    let (accepted_sender, accepted_receiver) = std::sync::mpsc::channel();
    let (release_sender, release_receiver) = std::sync::mpsc::sync_channel(0);
    let responder = std::thread::spawn(move || {
        loop {
            match listener.accept() {
                Ok((stream, _)) => {
                    accepted_sender.send(()).expect("report accepted probe");
                    release_receiver.recv().expect("release stalled responder");
                    drop(stream);
                    break;
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    if responder_stop.load(std::sync::atomic::Ordering::SeqCst) {
                        break;
                    }
                    std::thread::yield_now();
                }
                Err(error) => panic!("accept stalled probe: {error}"),
            }
        }
    });

    let clock = ManualDeadlineClock::at(400);
    let probe_clock = clock.clone();
    let probe_path = path.clone();
    let probe = std::thread::spawn(move || {
        carrier.probe_stale(&probe_path, &expected_owner(), &probe_clock, 405)
    });
    let mut accepted = false;
    for _ in 0..100_000 {
        if accepted_receiver.try_recv().is_ok() {
            accepted = true;
            break;
        }
        if probe.is_finished() {
            break;
        }
        std::thread::yield_now();
    }
    if !accepted {
        stop.store(true, std::sync::atomic::Ordering::SeqCst);
        responder.join().expect("stalled responder stops");
        let early = probe.join().expect("early probe thread completes");
        panic!("stale probe ended before reaching the responder: {early:?}");
    }
    for _ in 0..100_000 {
        if clock.registered_waiter_count() == 1 || probe.is_finished() {
            break;
        }
        std::thread::yield_now();
    }
    if clock.registered_waiter_count() != 1 {
        release_sender.send(()).expect("release stalled responder");
        responder.join().expect("stalled responder completes");
        let early = probe.join().expect("early probe thread completes");
        panic!("stalled probe did not register its manual deadline: {early:?}");
    }
    clock.advance_to(404);
    assert_eq!(clock.registered_waiter_count(), 1);
    clock.advance_to(405);
    let evidence = probe
        .join()
        .expect("bounded stalled probe completes")
        .expect("probe evidence");
    assert_eq!(
        evidence.liveness,
        LivenessEvidence::NoResponderBeforeDeadline
    );
    assert_eq!(clock.registered_waiter_count(), 0);
    assert_eq!(clock.registrations_created(), 1);
    release_sender.send(()).expect("release stalled responder");
    responder.join().expect("stalled responder completes");
}

#[test]
#[cfg(unix)]
fn replacement_in_the_final_inspection_to_removal_window_is_preserved() {
    use std::sync::atomic::{AtomicBool, Ordering};

    struct ReplaceAfterInspection {
        staged: std::path::PathBuf,
        invoked: AtomicBool,
    }

    impl StaleRemovalInterlock for ReplaceAfterInspection {
        fn after_final_identity_inspection(
            &self,
            path: &std::path::Path,
            observation: FinalStaleIdentityObservation,
        ) -> Result<(), LocalTransportError> {
            assert_eq!(
                channel_filesystem_identity(path)?,
                observation.identity(),
                "the hook runs immediately after the production final observation"
            );
            std::fs::rename(&self.staged, path)
                .map_err(|error| LocalTransportError::FilesystemInspection(error.to_string()))?;
            self.invoked.store(true, Ordering::SeqCst);
            Ok(())
        }
    }

    let root = tempfile::tempdir().expect("temporary replacement-channel root");
    let path = root.path().join("stale.sock");
    let staged = root.path().join("replacement.sock");
    let carrier = UnixLocalCarrier;
    let stale_listener = carrier.listen(&path).expect("create original channel");
    let replacement = carrier
        .listen(&staged)
        .expect("create the replacement concurrently at a staging name");
    let old_identity = channel_filesystem_identity(&path).expect("inspect original identity");
    let replacement_identity =
        channel_filesystem_identity(&staged).expect("inspect replacement identity");
    assert_ne!(old_identity, replacement_identity);
    drop(stale_listener);

    let evidence = StaleChannelEvidence {
        identity: StaleIdentityEvidence::VerifiedOwner,
        liveness: LivenessEvidence::NoResponderBeforeDeadline,
        filesystem: StaleFilesystemEvidence::Exact(old_identity),
    };
    let interlock = ReplaceAfterInspection {
        staged,
        invoked: AtomicBool::new(false),
    };

    assert_eq!(
        carrier.remove_after_proven_stale_with_interlock(&path, evidence, &interlock),
        Ok(StaleChannelAction::Preserve)
    );
    assert!(interlock.invoked.load(Ordering::SeqCst));
    assert_eq!(
        channel_filesystem_identity(&path).expect("replacement still exists"),
        replacement_identity
    );
    let connect_clock = ManualDeadlineClock::at(500);
    let connection = run_ready(carrier.connect(&path, &connect_clock, 505))
        .expect("connect to preserved replacement");
    drop(connection);
    drop(replacement);
}
