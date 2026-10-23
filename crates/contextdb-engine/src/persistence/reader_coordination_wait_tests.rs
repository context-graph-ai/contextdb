//! A stopped caller returns while the reader's real OFD byte is still held.
//! The private row fixes the schedule after arming/check/probe and before sleep;
//! public rows exercise the callee without claiming to observe its wait entry.

use super::*;
use std::sync::mpsc;
use std::time::Duration;

// Recovery guard, not a latency oracle. A missing answer is saved as state;
// the holder is closed and the waiter joined before any verdict is asserted.
const CLEANUP_GUARD: Duration = Duration::from_secs(30);

#[derive(Clone, Copy)]
enum Row {
    QueuedCancel,
    PublicCancel,
    PublicRelease,
}

fn held_reader_row(row: Row) {
    let directory = tempfile::TempDir::new().expect("task-scoped coordination fixture");
    let root = directory.path().join("runtime");
    std::fs::create_dir(&root).unwrap();
    use std::os::unix::fs::PermissionsExt as _;
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
    let store = directory.path().join("reader.db");
    drop(RedbPersistence::create(&store).expect("seed and release a real store"));
    let companion = companion_path(&store);
    let reader = open_trusted_companion(&companion).expect("trusted reader description");
    let observer = open_trusted_companion_for_exclusive_hold(&companion).unwrap();
    let waiting = open_trusted_companion_for_exclusive_hold(&companion).unwrap();
    assert_eq!(
        take_companion_range_lock_shared(&reader, COMPANION_READER_HOLD_LOCK_OFFSET).unwrap(),
        RangeLockOutcome::Taken
    );
    assert_eq!(
        take_companion_range_lock_exclusive(&observer, COMPANION_READER_HOLD_LOCK_OFFSET).unwrap(),
        RangeLockOutcome::HeldByAnother,
        "a separately opened OFD must observe the reader's live hold"
    );
    let identity = current_reader_breadcrumb().expect("authenticate the actual reader process");
    let breadcrumb = ReaderBreadcrumbGuard::create(&store, &root, Some(&identity)).unwrap();
    assert_eq!(
        live_reader_holders(&store, Some(&root)),
        (1, vec![identity.clone()])
    );

    // Reader presence and writer ownership are different bytes. Keep this
    // independent control beside the cancellation and release rows.
    let writer = CompanionGuard::acquire(&store, CompanionAdmission::Existing)
        .expect("a reader byte does not prevent a real writer claim");
    let rival = open_trusted_companion_for_exclusive_hold(&companion).unwrap();
    assert!(
        !try_lock_exclusive(&rival).unwrap(),
        "a second writer cannot steal ownership"
    );

    let stop = OwnerReadCancellation::new();
    let waiting_stop = stop.clone();
    let wait_path = store.clone();
    let wait_root = root.clone();
    let (prepared, preparation) = mpsc::channel();
    let (proceed, permission) = mpsc::channel();
    let (tell, heard) = mpsc::channel();
    let asking = std::thread::spawn(move || {
        let watch = matches!(row, Row::QueuedCancel)
            .then(|| CoordinationWatch::arm(&companion, Some(&waiting_stop)).unwrap());
        assert!(
            !waiting_stop.is_cancelled(),
            "check precedes the queued cancellation"
        );
        assert_eq!(
            take_companion_range_lock_exclusive(&waiting, COMPANION_READER_HOLD_LOCK_OFFSET)
                .unwrap(),
            RangeLockOutcome::HeldByAnother
        );
        prepared
            .send(watch.as_ref().map(|watch| Arc::downgrade(&watch.wake)))
            .unwrap();
        permission.recv().unwrap();
        let answer = match row {
            Row::QueuedCancel => {
                // The token was cancelled after the check and failed probe,
                // while this armed listener owned the real wake pipe. No
                // fabricated wake, pre-cancelled arm, or production seam.
                assert!(waiting_stop.is_cancelled());
                let watch = watch.as_ref().unwrap();
                use std::os::fd::AsRawFd as _;
                let mut queued = libc::pollfd {
                    fd: watch.wake.reader.as_raw_fd(),
                    events: libc::POLLIN,
                    revents: 0,
                };
                assert_eq!(unsafe { libc::poll(&mut queued, 1, 0) }, 1);
                assert_ne!(
                    queued.revents & libc::POLLIN,
                    0,
                    "real token listener queued a wake"
                );
                watch
                    .sleep(&waiting, COMPANION_READER_HOLD_LOCK_OFFSET, None)
                    .map(|()| ReaderReleaseWait::Stopped)
            }
            Row::PublicCancel | Row::PublicRelease => {
                let runtime = RuntimeDirectory::supplied(&wait_root);
                Ok(wait_for_reader_release(
                    &wait_path,
                    Some(&runtime),
                    &waiting_stop,
                ))
            }
        };
        drop(watch);
        drop(waiting);
        let _ = tell.send(answer);
    });

    // Save setup failure instead of panicking with a blocked holder/waiter.
    let wake_lifetime = preparation.recv_timeout(CLEANUP_GUARD);
    let prepared_ok = wake_lifetime.is_ok();
    if matches!(row, Row::QueuedCancel) && prepared_ok {
        stop.cancel();
    }
    let _ = proceed.send(());
    if matches!(row, Row::PublicCancel) {
        // This is public API coverage, not the deterministic boundary proof:
        // cancellation can precede its first check on this schedule.
        stop.cancel();
    }

    let mut while_held = None;
    let mut still_held = None;
    let mut still_identified = None;
    if prepared_ok && !matches!(row, Row::PublicRelease) {
        while_held = Some(heard.recv_timeout(CLEANUP_GUARD));
        still_held = Some(
            take_companion_range_lock_exclusive(&observer, COMPANION_READER_HOLD_LOCK_OFFSET)
                .unwrap(),
        );
        still_identified = Some(live_reader_holders(&store, Some(&root)));
    }

    // Cleanup comes BEFORE the decisive assertion, including on the old
    // Darwin blocking acquisition. Closing this exact OFD wakes its queue.
    drop(reader);
    drop(breadcrumb);
    let final_answer = if while_held.as_ref().is_some_and(|answer| answer.is_ok()) {
        None
    } else {
        Some(heard.recv_timeout(CLEANUP_GUARD))
    };
    // If public release keeps waiting after release, cancellation
    // gives it the ordinary escape path before joining the owned thread.
    stop.cancel();
    let joined = asking.join();
    let wake_dropped = wake_lifetime
        .as_ref()
        .is_ok_and(|wake| wake.as_ref().is_none_or(|wake| wake.upgrade().is_none()));
    let reacquired =
        take_companion_range_lock_exclusive(&observer, COMPANION_READER_HOLD_LOCK_OFFSET).unwrap();
    release_companion_range_lock(&observer, COMPANION_READER_HOLD_LOCK_OFFSET);
    let no_readers = live_reader_holders(&store, Some(&root));
    let writer_still_owns = !try_lock_exclusive(&rival).unwrap();
    drop(writer);
    let next_writer = CompanionGuard::acquire(&store, CompanionAdmission::Existing);

    assert!(
        prepared_ok,
        "the waiter observed a real held reader byte before proceeding"
    );
    assert!(
        joined.is_ok(),
        "join the owned waiter after releasing its holder"
    );
    assert!(
        wake_dropped,
        "the directly observed watch and listener release their wake pipe"
    );
    assert_eq!(
        reacquired,
        RangeLockOutcome::Taken,
        "no abandoned exclusive hold remains"
    );
    assert_eq!(
        no_readers,
        (0, vec![]),
        "reader identity and lifetime marker are cleaned up"
    );
    assert!(
        writer_still_owns,
        "cancelling or releasing a reader wait cannot release a writer"
    );
    assert!(
        next_writer.is_ok(),
        "a distinct writer can reacquire after the real writer closes"
    );
    drop(next_writer);

    if matches!(row, Row::PublicRelease) {
        assert!(
            matches!(final_answer, Some(Ok(Ok(ReaderReleaseWait::Released)))),
            "the actual public callee reports Released without cancellation"
        );
    } else {
        assert_eq!(still_held, Some(RangeLockOutcome::HeldByAnother));
        assert_eq!(still_identified, Some((1, vec![identity])));
        assert!(
            matches!(while_held, Some(Ok(Ok(ReaderReleaseWait::Stopped)))),
            "cancellation must answer while the authenticated reader byte is still held"
        );
    }
}

#[test]
fn queued_cancellation_after_check_and_arm_returns_before_reader_release() {
    held_reader_row(Row::QueuedCancel);
}

#[test]
fn public_reader_wait_cancellation_preserves_reader_and_writer_ownership() {
    held_reader_row(Row::PublicCancel);
}

#[test]
fn public_reader_wait_release_without_cancellation_cleans_up_and_reacquires() {
    held_reader_row(Row::PublicRelease);
}
