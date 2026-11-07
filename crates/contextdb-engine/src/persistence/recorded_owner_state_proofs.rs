use super::*;

/// A store whose companion carries no published writer run at all has no
/// recorded verdict to give, whoever is holding it.
#[test]
fn a_store_with_no_published_record_records_no_verdict() {
    let directory = tempfile::TempDir::new().expect("task-scoped store directory");
    let store = directory.path().join("unpublished.db");
    std::fs::write(&store, b"").expect("the store exists");

    assert!(
        recorded_unserved_owner(&store).is_none(),
        "a store with nothing published beside it refuses nobody"
    );
}

/// The control this whole consult depends on: a record left behind by a
/// writer that has GONE refuses nobody. Only a lock held right now makes a
/// recorded reason answerable, and that lock is observed through the same
/// open file the record was read from.
#[test]
fn a_record_nobody_is_holding_refuses_nobody() {
    #[cfg(unix)]
    {
        // A reader hold and a writer claim are independent facts. Exercise
        // their actual primitives before the recorded-owner control below.
        let directory = tempfile::TempDir::new().expect("task-scoped lock directory");
        let store = directory.path().join("independent-locks.db");
        std::fs::write(&store, b"").expect("the store exists");
        let initial = CompanionGuard::acquire(&store, CompanionAdmission::NewStore)
            .expect("create the trusted companion");
        initial.publish_writer_run(&store, true, None).unwrap();
        drop(initial);

        let path = companion_path(&store);
        let reader = open_trusted_companion(&path).expect("separate reader description");
        let contender = open_trusted_companion_for_exclusive_hold(&path)
            .expect("separate competing writer description");
        assert_eq!(
            take_companion_range_lock_shared(&reader, COMPANION_READER_HOLD_LOCK_OFFSET).unwrap(),
            RangeLockOutcome::Taken
        );
        let writer =
            CompanionGuard::acquire(&store, CompanionAdmission::Existing).unwrap_or_else(|error| {
                panic!("a reader byte hold must coexist with a writer claim: {error}")
            });
        writer.publish_writer_run(&store, false, None).unwrap();

        let assert_writer_still_owns = || {
            assert!(
                !try_lock_exclusive(&contender).unwrap(),
                "a separate open in this process must not steal writer ownership"
            );
            assert!(matches!(
                CompanionGuard::acquire(&store, CompanionAdmission::Existing),
                Err(Error::ReadFailure(failure))
                    if failure.kind() == ReadFailureKind::HeldByWriter
            ));
        };
        assert_writer_still_owns();
        assert_eq!(
            take_companion_range_lock_shared(&contender, COMPANION_CLAIM_WINDOW_LOCK_OFFSET)
                .unwrap(),
            RangeLockOutcome::HeldByAnother
        );
        assert_eq!(
            take_companion_range_lock_exclusive(&contender, COMPANION_READER_HOLD_LOCK_OFFSET)
                .unwrap(),
            RangeLockOutcome::HeldByAnother,
            "same-process opens must also contend for the reader byte"
        );

        drop(open_trusted_companion(&path).expect("unrelated open and close"));
        assert_writer_still_owns();
        assert_eq!(
            take_companion_range_lock_exclusive(&contender, COMPANION_READER_HOLD_LOCK_OFFSET)
                .unwrap(),
            RangeLockOutcome::HeldByAnother,
            "an unrelated descriptor close must not release the reader hold"
        );

        writer.close_claim_window();
        assert_writer_still_owns();
        assert_eq!(
            take_companion_range_lock_exclusive(&contender, COMPANION_CLAIM_WINDOW_LOCK_OFFSET)
                .unwrap(),
            RangeLockOutcome::Taken
        );
        release_companion_range_lock(&contender, COMPANION_CLAIM_WINDOW_LOCK_OFFSET);
        assert_writer_still_owns();
        {
            let state = writer.lock_state().unwrap();
            assert_eq!(
                take_companion_range_lock_shared(&state.file, COMPANION_READER_HOLD_LOCK_OFFSET)
                    .unwrap(),
                RangeLockOutcome::Taken
            );
            release_companion_range_lock(&state.file, COMPANION_READER_HOLD_LOCK_OFFSET);
        }
        assert_writer_still_owns();
        assert_eq!(
            take_companion_range_lock_exclusive(&contender, COMPANION_READER_HOLD_LOCK_OFFSET)
                .unwrap(),
            RangeLockOutcome::HeldByAnother,
            "precise unlock on the writer must preserve the other reader's hold"
        );

        release_companion_range_lock(&reader, COMPANION_READER_HOLD_LOCK_OFFSET);
        assert_writer_still_owns();
        assert_eq!(
            take_companion_range_lock_exclusive(&contender, COMPANION_READER_HOLD_LOCK_OFFSET)
                .unwrap(),
            RangeLockOutcome::Taken,
            "the reader byte becomes free while the writer still owns the store"
        );
        release_companion_range_lock(&contender, COMPANION_READER_HOLD_LOCK_OFFSET);
        assert_writer_still_owns();
        drop(writer);
        assert!(try_lock_exclusive(&contender).unwrap());
        unlock_file(&contender);
    }

    let directory = tempfile::TempDir::new().expect("task-scoped store directory");
    let store = directory.path().join("released.db");
    std::fs::write(&store, b"").expect("the store exists");
    let companion =
        CompanionGuard::acquire(&store, CompanionAdmission::NewStore).expect("claim the store");
    companion
        .publish_writer_run(&store, true, None)
        .expect("the writer publishes its run");
    companion
        .record_owner_read_status(
            &store,
            OwnerReadStatus {
                state: OwnerServingState::ServingDisabled,
                reason: Some(OwnerServingReason::DisabledByConfiguration),
            },
        )
        .expect("the writer records what it decided about serving");

    let held = recorded_unserved_owner(&store);
    assert!(
        matches!(
            held,
            Some(OwnerReadStatus {
                state: OwnerServingState::ServingDisabled,
                ..
            })
        ),
        "a writer holding the store with inspection off says so: {held:?}"
    );

    drop(companion);
    assert!(
        recorded_unserved_owner(&store).is_none(),
        "the record outlives the writer; the verdict must not"
    );
}

/// A writer that IS serving is reached through its channel, not through
/// this consult, so a serving record refuses nobody either.
#[test]
fn a_serving_record_refuses_nobody() {
    let directory = tempfile::TempDir::new().expect("task-scoped store directory");
    let store = directory.path().join("serving.db");
    std::fs::write(&store, b"").expect("the store exists");
    let companion =
        CompanionGuard::acquire(&store, CompanionAdmission::NewStore).expect("claim the store");
    companion
        .publish_writer_run(&store, true, None)
        .expect("the writer publishes its run");
    companion
        .record_owner_read_status(
            &store,
            OwnerReadStatus {
                state: OwnerServingState::Serving,
                reason: None,
            },
        )
        .expect("the writer records that it serves");

    assert!(
        recorded_unserved_owner(&store).is_none(),
        "a serving owner answers on its channel, not out of its record"
    );
}

/// A companion reached through a SYMLINK is not this store's companion,
/// whatever it contains. Following one would let anybody who can write a
/// name beside the store hand every reader a locked, checksummed verdict
/// about a store nobody owns.
#[cfg(unix)]
#[test]
fn a_symlinked_companion_produces_no_owner_verdict() {
    let directory = tempfile::TempDir::new().expect("task-scoped store directory");
    let store = directory.path().join("symlinked.db");
    std::fs::write(&store, b"").expect("the store exists");

    // A real, held companion for a DIFFERENT store, carrying a real
    // recorded refusal.
    let planted = directory.path().join("planted.db");
    std::fs::write(&planted, b"").expect("the planted store exists");
    let holder = CompanionGuard::acquire(&planted, CompanionAdmission::NewStore)
        .expect("hold the planted store");
    holder
        .publish_writer_run(&planted, true, None)
        .expect("the planted writer publishes its run");
    holder
        .record_owner_read_status(
            &planted,
            OwnerReadStatus {
                state: OwnerServingState::ServingDisabled,
                reason: Some(OwnerServingReason::DisabledByConfiguration),
            },
        )
        .expect("the planted writer records a refusal worth stealing");

    std::os::unix::fs::symlink(companion_path(&planted), companion_path(&store))
        .expect("point this store's companion name at the planted one");

    assert!(
        recorded_unserved_owner(&store).is_none(),
        "a companion that is a symbolic link is not a companion this reader may trust"
    );
}

/// A companion whose inode is swapped underneath the observation is not
/// one observation of one writer, so it yields no verdict.
#[cfg(unix)]
#[test]
fn a_companion_replaced_mid_observation_produces_no_owner_verdict() {
    use std::os::unix::fs::PermissionsExt as _;

    let directory = tempfile::TempDir::new().expect("task-scoped store directory");
    let store = directory.path().join("replaced.db");
    std::fs::write(&store, b"").expect("the store exists");
    let companion =
        CompanionGuard::acquire(&store, CompanionAdmission::NewStore).expect("claim the store");
    companion
        .publish_writer_run(&store, true, None)
        .expect("the writer publishes its run");
    companion
        .record_owner_read_status(
            &store,
            OwnerReadStatus {
                state: OwnerServingState::ServingDisabled,
                reason: Some(OwnerServingReason::DisabledByConfiguration),
            },
        )
        .expect("the writer records its decision");

    // The pathname now names a different inode than the one a reader
    // would open and lock: the validation must refuse to speak for it.
    let substitute = directory.path().join("substitute.lock");
    std::fs::write(&substitute, b"").expect("write the substitute inode");
    std::fs::set_permissions(&substitute, std::fs::Permissions::from_mode(0o600))
        .expect("make the substitute look like a companion");
    std::fs::rename(&substitute, companion_path(&store)).expect("swap the companion inode");

    assert!(
        recorded_unserved_owner(&store).is_none(),
        "an inode swapped in under the companion's name speaks for nobody"
    );
}

/// The handover this consult must never mis-report: writer A records a
/// reason and goes; writer B takes the store and records its own. What a
/// reader is told is B's reason bound to B's run, never A's validated by
/// B's lock.
#[test]
fn a_handover_never_reports_the_departed_writers_reason() {
    let directory = tempfile::TempDir::new().expect("task-scoped store directory");
    let store = directory.path().join("handover.db");
    std::fs::write(&store, b"").expect("the store exists");

    let first = CompanionGuard::acquire(&store, CompanionAdmission::NewStore)
        .expect("the first writer claims the store");
    first
        .publish_writer_run(&store, true, None)
        .expect("the first writer publishes its run");
    first
        .record_owner_read_status(
            &store,
            OwnerReadStatus {
                state: OwnerServingState::ServingDisabled,
                reason: Some(OwnerServingReason::DisabledByConfiguration),
            },
        )
        .expect("the first writer records its decision");
    let departed = inspect_companion_record(&store)
        .expect("the first writer published a run")
        .fields
        .writer_run_number;
    drop(first);

    let second = CompanionGuard::acquire(&store, CompanionAdmission::Existing)
        .expect("the second writer claims the store");
    second
        .publish_writer_run(&store, false, None)
        .expect("the second writer publishes its own run");
    second
        .record_owner_read_status(
            &store,
            OwnerReadStatus {
                state: OwnerServingState::NotServing,
                reason: Some(OwnerServingReason::StartupFailure("no channel".to_owned())),
            },
        )
        .expect("the second writer records its own decision");

    let arriving = inspect_companion_record(&store)
        .expect("the second writer published a run")
        .fields
        .writer_run_number;
    assert_ne!(
        departed, arriving,
        "the two writers are distinguishable, which is what binds a reason to its writer"
    );

    let told = recorded_unserved_owner(&store).expect("a live writer that will not serve");
    assert_eq!(
        told.state,
        OwnerServingState::NotServing,
        "the reader is told the holder's own reason, not the departed writer's: {told:?}"
    );
}
