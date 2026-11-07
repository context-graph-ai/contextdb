use super::*;
use std::os::fd::AsRawFd as _;
use std::os::unix::fs::MetadataExt as _;

fn snapshot_of(file: &File) -> RegisteredReaderSnapshot {
    let metadata = file.metadata().expect("stat the held file");
    RegisteredReaderSnapshot {
        physical_identity: (metadata.dev(), metadata.ino()),
        owner_user: contextdb_core::read_contract::LocalUserIdentity(
            reader_runtime_user().expect("a unix user"),
        ),
        database_identity: None,
        storage_snapshot: redb::StorageSnapshotIdentity {
            transaction_id: 1,
            user_root: None,
        },
        handle_identity: [7; 16],
        process: current_reader_registration_process()
            .expect("this platform records the reader's process start"),
        descriptor_number: u32::try_from(file.as_raw_fd()).expect("a descriptor number"),
    }
}

/// the writer's check that a registered reader still holds
/// its store answers yes for a live reader and no for each forged or stale fact -- another
/// process start, another file, a closed descriptor. Before the fix, a non-Linux platform
/// answered no for every reader, live ones included.
#[test]
fn a_live_reader_is_confirmed_and_every_stale_fact_is_refused() {
    let directory = tempfile::TempDir::new().expect("task-scoped directory");
    let held = File::create(directory.path().join("held.db")).expect("a held file");
    let other = File::create(directory.path().join("other.db")).expect("another file");
    let live = snapshot_of(&held);
    assert!(
        reader_snapshot_owns_descriptor(&live),
        "a live reader holding its descriptor is confirmed"
    );

    let mut restarted = live.clone();
    restarted.process.process_start =
        contextdb_core::read_contract::ProcessStartIdentity(live.process.process_start.0 ^ 1);
    assert!(
        !reader_snapshot_owns_descriptor(&restarted),
        "a different process start is not this reader"
    );

    let mut elsewhere = live.clone();
    elsewhere.physical_identity = snapshot_of(&other).physical_identity;
    assert!(
        !reader_snapshot_owns_descriptor(&elsewhere),
        "a descriptor on another file is not this reader's store"
    );

    let closed = snapshot_of(&other);
    drop(other);
    assert!(
        !reader_snapshot_owns_descriptor(&closed),
        "a closed descriptor is not a live reader"
    );
}
