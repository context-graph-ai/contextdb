use super::*;
use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

fn private_directory(parent: &Path, name: &str) -> PathBuf {
    let directory = parent.join(name);
    std::fs::create_dir(&directory).unwrap();
    std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700)).unwrap();
    directory
}

fn store(directory: &Path) -> (PathBuf, (u64, u64)) {
    let path = directory.join("gate.db");
    drop(RedbPersistence::create(&path).expect("claim and release a real store"));
    let metadata = std::fs::metadata(&path).unwrap();
    (path, (metadata.dev(), metadata.ino()))
}

fn gate_granted(path: &Path, identity: (u64, u64)) -> bool {
    with_last_reader_gate(path, identity, |granted| granted)
}

#[test]
fn the_data_gate_is_refused_while_a_writer_claims_or_owns_the_store() {
    use physical_admission::{
        ByteLock, CLAIM_OFFSET, Granted, release_byte, try_byte, try_data,
    };
    let directory = tempfile::TempDir::new().unwrap();
    let (path, identity) = store(directory.path());
    let writer = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    assert!(matches!(
        try_byte(&writer, CLAIM_OFFSET, ByteLock::Exclusive).unwrap(),
        Granted::Yes
    ));
    assert!(
        !gate_granted(&path, identity),
        "a writer that has claimed the store refuses the cleaner, so nothing may be removed"
    );
    release_byte(&writer, CLAIM_OFFSET).unwrap();
    assert!(matches!(
        try_data(&writer, ByteLock::Exclusive).unwrap(),
        Granted::Yes
    ));
    assert!(
        !gate_granted(&path, identity),
        "a writer that owns the store refuses the cleaner"
    );
    physical_admission::release_data(&writer).unwrap();
    assert!(gate_granted(&path, identity));
    assert!(
        !gate_granted(&path, (identity.0, identity.1.wrapping_add(1))),
        "a descriptor naming another physical file is not the store's gate"
    );
}

#[test]
fn the_data_gate_is_its_own_lock_owner_whatever_a_shared_guard_does() {
    use physical_admission::{ByteLock, Granted, try_data};
    let directory = tempfile::TempDir::new().unwrap();
    let (path, identity) = store(directory.path());
    let opening_writer = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    with_last_reader_gate(&path, identity, |granted| {
        assert!(granted, "no writer holds the store");
        // A session's descriptor is shared between guards through one
        // Arc; its lock and unlock act on that one open file description.
        let session = Arc::new(File::open(&path).unwrap());
        let other_guard = Arc::clone(&session);
        assert!(matches!(
            try_data(session.as_ref(), ByteLock::Shared).unwrap(),
            Granted::Yes
        ));
        physical_admission::release_data(other_guard.as_ref()).unwrap();
        assert!(
            matches!(
                try_data(&opening_writer, ByteLock::Exclusive).unwrap(),
                Granted::HeldByAnother
            ),
            "the gate still excludes a writer after another guard released its own descriptor"
        );
    });
    assert!(
        matches!(
            try_data(&opening_writer, ByteLock::Exclusive).unwrap(),
            Granted::Yes
        ),
        "a writer owns the store once the gate is released"
    );
}

fn registry(root: &Path, identity: (u64, u64)) -> (PathBuf, ReaderSnapshotRegistryLock) {
    let mut lock = ReaderSnapshotRegistryLock::create(root, identity).unwrap();
    lock.lock_exclusive().unwrap();
    let directory = reader_snapshot_directory(root, identity);
    create_reader_breadcrumb_directory(&directory).unwrap();
    (directory, lock)
}

#[test]
fn a_registry_is_removed_only_with_the_gate_and_only_when_nothing_else_is_in_it() {
    let directory = tempfile::TempDir::new().unwrap();
    let base = private_directory(directory.path(), "platform");
    let runtime = RuntimeDirectory::within(&base);
    create_reader_breadcrumb_directory(runtime.path()).unwrap();
    let identity = (7, 11);
    let (registry_directory, lock) = registry(runtime.path(), identity);

    remove_registry_if_last(&registry_directory, &lock.path, false, Some(runtime.path()));
    assert!(
        registry_directory.is_dir(),
        "without the data gate nothing is removed"
    );

    let foreign = registry_directory.join("someone-elses-registration");
    std::fs::write(&foreign, b"held").unwrap();
    remove_registry_if_last(&registry_directory, &lock.path, true, Some(runtime.path()));
    assert!(
        foreign.exists(),
        "a registration of any process keeps the whole registry"
    );
    std::fs::remove_file(&foreign).unwrap();

    remove_registry_if_last(&registry_directory, &lock.path, true, Some(runtime.path()));
    assert!(!registry_directory.exists());
    assert!(
        !lock.path.exists(),
        "the registry lock goes with its registry"
    );
    assert!(
        !runtime.path().exists(),
        "the empty `contextdb` child this process created goes too"
    );
    assert!(base.is_dir(), "the platform directory itself stays");
}

#[test]
fn a_supplied_root_is_never_removed_with_its_registry() {
    let directory = tempfile::TempDir::new().unwrap();
    let supplied = private_directory(directory.path(), "supplied");
    let identity = (13, 17);
    let (registry_directory, lock) = registry(&supplied, identity);
    remove_registry_if_last(&registry_directory, &lock.path, true, None);
    assert!(!registry_directory.exists());
    assert!(!lock.path.exists());
    assert!(
        supplied.is_dir(),
        "an operator-supplied root is the operator's"
    );
}
