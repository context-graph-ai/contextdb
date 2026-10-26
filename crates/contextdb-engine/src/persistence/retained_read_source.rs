use super::*;

#[cfg(not(unix))]
pub(super) fn registry_lock_descriptor_departed(_file: &File) -> bool {
    false
}

#[cfg(unix)]
pub(super) fn reader_snapshot_registry_lock_path_still_names(
    path: &Path,
    inspected: &std::fs::Metadata,
) -> bool {
    std::fs::symlink_metadata(path).is_ok_and(|current| {
        trusted_reader_snapshot_registry_lock_metadata(&current)
            && same_reader_breadcrumb_inode(inspected, &current)
    })
}

#[cfg(not(unix))]
pub(super) fn reader_snapshot_registry_lock_path_still_names(
    _path: &Path,
    _inspected: &std::fs::Metadata,
) -> bool {
    false
}

#[cfg(unix)]
pub(super) fn trusted_reader_snapshot_directory_metadata(metadata: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt as _;

    metadata.file_type().is_dir()
        && metadata.uid() == nix::unistd::Uid::effective().as_raw()
        && metadata.mode() & 0o077 == 0
}

#[cfg(not(unix))]
pub(super) fn trusted_reader_snapshot_directory_metadata(_metadata: &std::fs::Metadata) -> bool {
    false
}

#[cfg(unix)]
pub(super) fn same_reader_snapshot_directory(
    left: &std::fs::Metadata,
    right: &std::fs::Metadata,
) -> bool {
    use std::os::unix::fs::MetadataExt as _;

    trusted_reader_snapshot_directory_metadata(right)
        && left.dev() == right.dev()
        && left.ino() == right.ino()
        && left.uid() == right.uid()
        && left.mode() == right.mode()
}

#[cfg(not(unix))]
pub(super) fn same_reader_snapshot_directory(
    _left: &std::fs::Metadata,
    _right: &std::fs::Metadata,
) -> bool {
    false
}

#[cfg(unix)]
pub(super) fn reader_snapshot_path_still_names(path: &Path, inspected: &std::fs::Metadata) -> bool {
    std::fs::symlink_metadata(path).is_ok_and(|current| {
        trusted_reader_snapshot_metadata(&current)
            && same_reader_breadcrumb_inode(inspected, &current)
    })
}

#[cfg(not(unix))]
pub(super) fn reader_snapshot_path_still_names(
    _path: &Path,
    _inspected: &std::fs::Metadata,
) -> bool {
    false
}

#[cfg(unix)]
pub(super) fn open_trusted_reader_snapshot_directory(
    path: &Path,
) -> std::io::Result<Option<(File, std::fs::Metadata)>> {
    use nix::fcntl::OFlag;
    use nix::sys::stat::Mode;

    let descriptor = match nix::fcntl::open(
        path,
        OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
        Mode::empty(),
    ) {
        Ok(descriptor) => descriptor,
        Err(nix::errno::Errno::ENOENT) => return Ok(None),
        Err(error) => return Err(std::io::Error::from(error)),
    };
    let directory = File::from(descriptor);
    let descriptor_metadata = directory.metadata()?;
    if !trusted_reader_snapshot_directory_metadata(&descriptor_metadata) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "reader snapshot registry is not a trusted private directory",
        ));
    }
    Ok(Some((directory, descriptor_metadata)))
}

#[cfg(not(unix))]
pub(super) fn open_trusted_reader_snapshot_directory(
    _path: &Path,
) -> std::io::Result<Option<(File, std::fs::Metadata)>> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "reader snapshot registries require unix directory descriptors",
    ))
}

/// Enumerate names through a duplicate of `directory`, never through the
/// registry pathname. `fdopendir` owns and closes only the duplicate.
#[cfg(unix)]
pub(super) fn reader_snapshot_directory_entries(directory: &File) -> std::io::Result<Vec<PathBuf>> {
    use std::os::fd::AsRawFd as _;
    use std::os::unix::ffi::OsStringExt as _;

    let duplicate = unsafe { libc::dup(directory.as_raw_fd()) };
    if duplicate < 0 {
        return Err(std::io::Error::last_os_error());
    }
    let stream = unsafe { libc::fdopendir(duplicate) };
    if stream.is_null() {
        unsafe { libc::close(duplicate) };
        return Err(std::io::Error::last_os_error());
    }
    let mut names = Vec::new();
    loop {
        nix::errno::Errno::clear();
        let entry = unsafe { libc::readdir(stream) };
        if entry.is_null() {
            let error = std::io::Error::last_os_error();
            unsafe { libc::closedir(stream) };
            if error.raw_os_error().is_some_and(|code| code != 0) {
                return Err(error);
            }
            break;
        }
        let name = unsafe { std::ffi::CStr::from_ptr((*entry).d_name.as_ptr()) }.to_bytes();
        if name == b"." || name == b".." {
            continue;
        }
        names.push(PathBuf::from(std::ffi::OsString::from_vec(name.to_vec())));
    }
    Ok(names)
}

#[cfg(not(unix))]
pub(super) fn reader_snapshot_directory_entries(
    _directory: &File,
) -> std::io::Result<Vec<PathBuf>> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "reader snapshot registries require unix directory descriptors",
    ))
}

/// Open a registry entry without allowing a malformed FIFO to wait while the
/// registry lock is held. The descriptor is still authenticated below before
/// any entry is used.
#[cfg(unix)]
pub(super) fn open_reader_snapshot_directory_entry(
    directory: &File,
    name: &Path,
) -> std::io::Result<File> {
    let owned = nix::fcntl::openat(
        directory,
        name,
        nix::fcntl::OFlag::O_RDONLY
            | nix::fcntl::OFlag::O_NONBLOCK
            | nix::fcntl::OFlag::O_NOFOLLOW
            | nix::fcntl::OFlag::O_CLOEXEC,
        nix::sys::stat::Mode::empty(),
    )
    .map_err(std::io::Error::from)?;
    Ok(File::from(owned))
}

#[cfg(unix)]
pub(super) fn open_trusted_reader_snapshot(
    directory: &File,
    name: &Path,
) -> std::io::Result<(File, std::fs::Metadata)> {
    let file = open_reader_snapshot_directory_entry(directory, name)?;
    let descriptor_metadata = file.metadata()?;
    if !trusted_reader_snapshot_metadata(&descriptor_metadata) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "reader snapshot entry is not a trusted private regular file",
        ));
    }
    Ok((file, descriptor_metadata))
}

#[cfg(not(unix))]
pub(super) fn open_trusted_reader_snapshot(
    _directory: &File,
    _name: &Path,
) -> std::io::Result<(File, std::fs::Metadata)> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "reader snapshot registries require unix directory descriptors",
    ))
}

#[cfg(unix)]
pub(super) fn reader_snapshot_entry_still_names(
    directory: &File,
    name: &Path,
    inspected: &std::fs::Metadata,
) -> bool {
    open_reader_snapshot_directory_entry(directory, name).is_ok_and(|file| {
        file.metadata().is_ok_and(|current| {
            trusted_reader_snapshot_metadata(&current)
                && same_reader_breadcrumb_inode(inspected, &current)
        })
    })
}

#[cfg(not(unix))]
pub(super) fn reader_snapshot_entry_still_names(
    _directory: &File,
    _name: &Path,
    _inspected: &std::fs::Metadata,
) -> bool {
    false
}

#[cfg(target_os = "linux")]
pub(super) fn reader_snapshot_owns_descriptor(snapshot: &RegisteredReaderSnapshot) -> bool {
    use std::os::unix::fs::MetadataExt as _;

    if linux_process_start(snapshot.process.process_id).ok() != Some(snapshot.process.process_start)
    {
        return false;
    }
    let descriptor = PathBuf::from(format!(
        "/proc/{}/fd/{}",
        snapshot.process.process_id, snapshot.descriptor_number
    ));
    std::fs::metadata(descriptor)
        .is_ok_and(|metadata| (metadata.dev(), metadata.ino()) == snapshot.physical_identity)
}

/// The same three facts as on Linux, asked of the kernel directly: the
/// process that published the record is the one that started at the recorded
/// instant, it still has the recorded descriptor open, and that descriptor is
/// the recorded physical store.
#[cfg(target_os = "macos")]
pub(super) fn reader_snapshot_owns_descriptor(snapshot: &RegisteredReaderSnapshot) -> bool {
    let stored = snapshot.process.process_start;
    if stored == contextdb_core::read_contract::ProcessStartIdentity(0) {
        return false;
    }
    if macos_process_start(snapshot.process.process_id).ok() != Some(stored) {
        return false;
    }
    if macos_descriptor_identity(snapshot.process.process_id, snapshot.descriptor_number).ok()
        != Some(snapshot.physical_identity)
    {
        return false;
    }
    macos_process_start(snapshot.process.process_id).ok() == Some(stored)
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub(super) fn reader_snapshot_owns_descriptor(_snapshot: &RegisteredReaderSnapshot) -> bool {
    false
}

/// When the process started, in microseconds since the epoch, from the
/// kernel's own record of it.
#[cfg(target_os = "macos")]
pub(super) fn macos_process_start(
    process_id: u32,
) -> std::io::Result<contextdb_core::read_contract::ProcessStartIdentity> {
    macos_process_info(process_id).map(|(_, start)| start)
}

/// The `(device, inode)` a process's descriptor refers to, as `stat` reports
/// them for the same file, or `None` when the descriptor is not open or is
/// not a file.
#[cfg(target_os = "macos")]
pub(super) fn macos_descriptor_identity(
    process_id: u32,
    descriptor: u32,
) -> std::io::Result<(u64, u64)> {
    // libc 0.2.182 declares `vnode_info` and not `vnode_fdinfo`. Flavor 1 is
    // `proc_fileinfo` plus `vnode_info`. Flavor 2 also applies a path check.
    #[repr(C)]
    struct ProcFileInfo {
        fi_openflags: u32,
        fi_status: u32,
        fi_offset: libc::off_t,
        fi_type: i32,
        fi_guardflags: u32,
    }
    #[repr(C)]
    struct VnodeFdInfo {
        pfi: ProcFileInfo,
        pvi: libc::vnode_info,
    }
    const PROC_PIDFDVNODEINFO: libc::c_int = 1;
    // `enum vtype`: `VNON` is 0 and `VREG` is 1.
    const VREG: libc::c_int = 1;
    const _: () = assert!(std::mem::size_of::<ProcFileInfo>() == 24);
    const _: () = assert!(std::mem::size_of::<libc::vinfo_stat>() == 136);
    const _: () = assert!(std::mem::size_of::<libc::vnode_info>() == 152);
    const _: () = assert!(std::mem::size_of::<VnodeFdInfo>() == 176);

    let pid = libc::c_int::try_from(process_id)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?;
    if pid == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "a process id of 0 has no reader descriptor",
        ));
    }
    let fd = libc::c_int::try_from(descriptor)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?;
    // SAFETY: the record is plain integers. All-zero is a valid value.
    let mut info: VnodeFdInfo = unsafe { std::mem::zeroed() };
    let size = libc::c_int::try_from(std::mem::size_of::<VnodeFdInfo>())
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?;
    // SAFETY: `info` is a live flavor-1 record and `size` is its byte length.
    let written = unsafe {
        libc::proc_pidfdinfo(
            pid,
            fd,
            PROC_PIDFDVNODEINFO,
            std::ptr::addr_of_mut!(info).cast(),
            size,
        )
    };
    if written != size {
        return Err(std::io::Error::last_os_error());
    }
    if info.pfi.fi_type != libc::PROX_FDTYPE_VNODE
        || info.pvi.vi_type != VREG
        || (info.pvi.vi_stat.vst_mode & libc::S_IFMT) != libc::S_IFREG
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "reader descriptor is not the registered regular file",
        ));
    }
    // `MetadataExt::dev` casts Apple `dev_t` (`i32`) with `as u64`, which
    // sign-extends. `vst_dev` holds those same bits in a `u32`.
    let device = (info.pvi.vi_stat.vst_dev as i32) as u64;
    Ok((device, info.pvi.vi_stat.vst_ino))
}

#[cfg(target_os = "linux")]
pub(super) fn capture_reader_snapshot_registry_under_admission_hold(
    identity: (u64, u64),
    runtime_dir: Option<&Path>,
    hold: &writer_file::ReaderAdmissionHold,
) -> ReaderSnapshotRegistry {
    if hold.identity() != identity {
        return ReaderSnapshotRegistry::Indeterminate(
            runtime_dir
                .map(Path::to_path_buf)
                .unwrap_or_else(|| std::path::PathBuf::from("admission_hold_identity_mismatch")),
        );
    }
    capture_reader_snapshot_registry_scanned(identity, runtime_dir, Some(hold))
}

#[cfg(target_os = "linux")]
pub(super) fn capture_reader_snapshot_registry(
    identity: (u64, u64),
    runtime_dir: Option<&Path>,
) -> ReaderSnapshotRegistry {
    capture_reader_snapshot_registry_scanned(identity, runtime_dir, None)
}

#[cfg(target_os = "linux")]
pub(super) fn capture_reader_snapshot_registry_scanned(
    identity: (u64, u64),
    runtime_dir: Option<&Path>,
    hold: Option<&writer_file::ReaderAdmissionHold>,
) -> ReaderSnapshotRegistry {
    let roots = reader_inventory_directories(runtime_dir);
    if roots.is_empty() {
        return ReaderSnapshotRegistry::Unavailable;
    }
    let mut combined = Vec::new();
    for runtime in roots {
        let _serializer = match serialize_runtime_root(runtime.root(), true) {
            Ok(serializer) => Some(serializer),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock && hold.is_some() => None,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                return ReaderSnapshotRegistry::Contended(runtime.path().to_path_buf());
            }
            Err(_) => return ReaderSnapshotRegistry::Indeterminate(runtime.path().to_path_buf()),
        };
        match capture_reader_snapshot_registry_one(identity, &runtime) {
            ReaderSnapshotRegistry::Complete(snapshots) => combined.extend(snapshots),
            ReaderSnapshotRegistry::Contended(path) => {
                return ReaderSnapshotRegistry::Contended(path);
            }
            ReaderSnapshotRegistry::Indeterminate(path) => {
                return ReaderSnapshotRegistry::Indeterminate(path);
            }
            ReaderSnapshotRegistry::Unavailable => return ReaderSnapshotRegistry::Unavailable,
        }
    }
    combined.sort_by_key(|snapshot| snapshot.handle_identity);
    combined.dedup_by_key(|snapshot| snapshot.handle_identity);
    ReaderSnapshotRegistry::Complete(combined)
}

#[cfg(all(unix, not(target_os = "linux")))]
pub(super) fn capture_reader_snapshot_registry(
    identity: (u64, u64),
    runtime_dir: Option<&Path>,
) -> ReaderSnapshotRegistry {
    let roots = reader_inventory_directories(runtime_dir);
    if roots.is_empty() {
        return ReaderSnapshotRegistry::Unavailable;
    }
    let mut combined = Vec::new();
    for runtime in roots {
        let _serializer = match serialize_runtime_root(runtime.root(), true) {
            Ok(serializer) => serializer,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                return ReaderSnapshotRegistry::Contended(runtime.path().to_path_buf());
            }
            Err(_) => return ReaderSnapshotRegistry::Indeterminate(runtime.path().to_path_buf()),
        };
        match capture_reader_snapshot_registry_one(identity, &runtime) {
            ReaderSnapshotRegistry::Complete(snapshots) => combined.extend(snapshots),
            ReaderSnapshotRegistry::Contended(path) => {
                return ReaderSnapshotRegistry::Contended(path);
            }
            ReaderSnapshotRegistry::Indeterminate(path) => {
                return ReaderSnapshotRegistry::Indeterminate(path);
            }
            ReaderSnapshotRegistry::Unavailable => return ReaderSnapshotRegistry::Unavailable,
        }
    }
    combined.sort_by_key(|snapshot| snapshot.handle_identity);
    combined.dedup_by_key(|snapshot| snapshot.handle_identity);
    ReaderSnapshotRegistry::Complete(combined)
}

#[cfg(unix)]
pub(super) fn capture_reader_snapshot_registry_one(
    identity: (u64, u64),
    runtime: &RuntimeDirectory,
) -> ReaderSnapshotRegistry {
    #[cfg(test)]
    let _purge_enclosing_producer = crate::persistence::content_identity::proof_allocation_oracle_tests::PurgeProducerGuard::site(
        "capture_reader_snapshot_registry_one");
    let directory = reader_snapshot_directory(runtime.path(), identity);
    // The lock is cooperative exclusion only. Atomically acquire and validate
    // the actual registry directory before the pause/lock window so a
    // same-user rename cannot turn a live registry into a pathname ENOENT
    // after the lock is acquired.
    let inspected_directory = match open_trusted_reader_snapshot_directory(&directory) {
        Ok(directory) => directory,
        Err(_) => return ReaderSnapshotRegistry::Indeterminate(runtime.path().to_path_buf()),
    };
    #[cfg(any(test, feature = "test-seams"))]
    read_persistence_test_scaffold::reader_snapshot_registry_directory_inspected_for_test(
        identity, &directory,
    );
    let mut registry_lock = match ReaderSnapshotRegistryLock::open(runtime.path(), identity) {
        Ok(lock) => lock,
        Err(error)
            if error.kind() == std::io::ErrorKind::NotFound && inspected_directory.is_none() =>
        {
            return ReaderSnapshotRegistry::Complete(Vec::new());
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            // A physical inode can acquire a trusted empty registry directory
            // before this process has ever needed its lock (for example, after
            // a replacement reuses an old runtime-directory identity).  A
            // reader publishes its lock before its directory and before it
            // releases the data-file admission lock; this writer acquired that
            // admission lock before arriving here.  Create a trusted lock for
            // the scan, then authenticate and enumerate the directory below.
            // In particular, do not classify this branch as empty before
            // checking every extant registration.
            match ReaderSnapshotRegistryLock::create(runtime.path(), identity) {
                Ok(lock) => lock,
                Err(_) => {
                    return ReaderSnapshotRegistry::Indeterminate(runtime.path().to_path_buf());
                }
            }
        }
        Err(_) => return ReaderSnapshotRegistry::Indeterminate(runtime.path().to_path_buf()),
    };
    // A scan never waits for the registry: a writer that holds the data lock
    // must not be held up by a reader publishing or tearing down. Positively
    // identified contention is reported as Contended, which a writer answers
    // by retaining every page (never by permitting erasure); arbitrary I/O and
    // failed trust checks stay terminal.
    let acquired = registry_lock.try_lock_exclusive();
    if let Err(error) = acquired {
        return if error.kind() == std::io::ErrorKind::WouldBlock {
            ReaderSnapshotRegistry::Contended(runtime.path().to_path_buf())
        } else {
            ReaderSnapshotRegistry::Indeterminate(runtime.path().to_path_buf())
        };
    }
    let (pinned_directory, inspected_directory) = match inspected_directory {
        Some(pinned_directory) => pinned_directory,
        None if std::fs::symlink_metadata(&directory)
            .is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound) =>
        {
            // A registry that was absent both before and while holding the
            // lock has no lock-coordinated snapshot publication.
            return ReaderSnapshotRegistry::Complete(Vec::new());
        }
        _ => return ReaderSnapshotRegistry::Indeterminate(runtime.path().to_path_buf()),
    };
    // A final coordinated reader may have unlinked this directory while the
    // scan waited for the registry lock. The held descriptor still names the
    // exact, trusted registry, but a pathname that now names another directory
    // proves replacement. An unlinked descriptor is only authenticated teardown
    // when its later enumeration is empty.
    let current_directory = match pinned_directory.metadata() {
        Ok(metadata) => metadata,
        Err(_) => return ReaderSnapshotRegistry::Indeterminate(runtime.path().to_path_buf()),
    };
    if !same_reader_snapshot_directory(&inspected_directory, &current_directory) {
        return ReaderSnapshotRegistry::Indeterminate(runtime.path().to_path_buf());
    }
    let unlinked_directory = match std::fs::symlink_metadata(&directory) {
        Ok(named) if same_reader_snapshot_directory(&inspected_directory, &named) => false,
        Ok(_) => return ReaderSnapshotRegistry::Indeterminate(runtime.path().to_path_buf()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => true,
        Err(_) => return ReaderSnapshotRegistry::Indeterminate(runtime.path().to_path_buf()),
    };
    let mut paths = match reader_snapshot_directory_entries(&pinned_directory) {
        Ok(paths) => paths,
        Err(_) => return ReaderSnapshotRegistry::Indeterminate(runtime.path().to_path_buf()),
    };
    paths.sort();
    if unlinked_directory && !paths.is_empty() {
        return ReaderSnapshotRegistry::Indeterminate(runtime.path().to_path_buf());
    }
    let mut snapshots = Vec::new();
    for name in paths {
        if name.extension().and_then(|extension| extension.to_str()) != Some("snapshot") {
            continue;
        }
        let (mut file, inspected) = match open_trusted_reader_snapshot(&pinned_directory, &name) {
            Ok(opened) => opened,
            Err(_) => return ReaderSnapshotRegistry::Indeterminate(runtime.path().to_path_buf()),
        };
        match fs2::FileExt::try_lock_exclusive(&file) {
            Ok(()) => {
                if reader_snapshot_entry_still_names(&pinned_directory, &name, &inspected) {
                    let _ = nix::unistd::unlinkat(
                        &pinned_directory,
                        &name,
                        nix::unistd::UnlinkatFlags::NoRemoveDir,
                    );
                }
                let _ = fs2::FileExt::unlock(&file);
            }
            Err(error)
                if error.kind() == std::io::ErrorKind::WouldBlock
                    || error.raw_os_error() == fs2::lock_contended_error().raw_os_error() =>
            {
                let length = match usize::try_from(inspected.len()) {
                    Ok(length) => length,
                    Err(_) => {
                        return ReaderSnapshotRegistry::Indeterminate(runtime.path().to_path_buf());
                    }
                };
                let mut bytes = vec![0; length];
                if file.seek(SeekFrom::Start(0)).is_err() || file.read_exact(&mut bytes).is_err() {
                    return ReaderSnapshotRegistry::Indeterminate(runtime.path().to_path_buf());
                }
                let snapshot = match decode_reader_snapshot(&bytes) {
                    Ok(snapshot)
                        if snapshot.physical_identity == identity
                            && Some(snapshot.owner_user.0) == reader_runtime_user() =>
                    {
                        snapshot
                    }
                    _ => {
                        return ReaderSnapshotRegistry::Indeterminate(runtime.path().to_path_buf());
                    }
                };
                #[cfg(all(test, target_os = "linux"))]
                owner_cursor_admission_tests::before_reader_descriptor_validation_for_test(
                    &snapshot,
                );
                if !reader_snapshot_owns_descriptor(&snapshot) {
                    // Nonblocking owner teardown can release the lifetime lock
                    // and data descriptor after our first lock probe. Prove the
                    // entry is now unlocked before discarding it as stale; a
                    // still-locked entry with invalid identity remains unsafe.
                    if fs2::FileExt::try_lock_exclusive(&file).is_err() {
                        return ReaderSnapshotRegistry::Indeterminate(runtime.path().to_path_buf());
                    }
                    if reader_snapshot_entry_still_names(&pinned_directory, &name, &inspected) {
                        let _ = nix::unistd::unlinkat(
                            &pinned_directory,
                            &name,
                            nix::unistd::UnlinkatFlags::NoRemoveDir,
                        );
                    }
                    let _ = fs2::FileExt::unlock(&file);
                    continue;
                }
                snapshots.push(snapshot);
            }
            Err(_) => return ReaderSnapshotRegistry::Indeterminate(runtime.path().to_path_buf()),
        }
    }
    snapshots.sort_by_key(|snapshot| snapshot.handle_identity);
    ReaderSnapshotRegistry::Complete(snapshots)
}

pub(super) fn read_locked_reader_breadcrumb(
    file: &mut File,
    path: &Path,
    inspected: &std::fs::Metadata,
) -> Option<ReaderBreadcrumb> {
    let length = usize::try_from(inspected.len()).ok()?;
    if length > MAX_READER_BREADCRUMB_BYTES {
        return None;
    }
    let mut bytes = vec![0_u8; length];
    file.seek(SeekFrom::Start(0)).ok()?;
    file.read_exact(&mut bytes).ok()?;
    let after_read = file.metadata().ok()?;
    #[cfg(unix)]
    if !trusted_reader_breadcrumb_metadata(&after_read)
        || !same_reader_breadcrumb_inode(inspected, &after_read)
        || !reader_breadcrumb_path_still_names(path, inspected)
    {
        return None;
    }
    #[cfg(not(unix))]
    if !reader_breadcrumb_path_still_names(path, inspected) {
        return None;
    }
    decode_reader_breadcrumb(&bytes).ok()
}

pub(super) fn reclaim_reader_breadcrumb_if_same(path: &Path, inspected: &std::fs::Metadata) {
    if reader_breadcrumb_path_still_names(path, inspected) {
        let _ = std::fs::remove_file(path);
        remove_empty_breadcrumb_directories(path, None);
    }
}

pub(crate) fn locked_reader_breadcrumbs(
    database_path: &Path,
    runtime_root: &Path,
) -> std::io::Result<Vec<LockedReaderBreadcrumb>> {
    let directory = reader_breadcrumb_directory(database_path, runtime_root)?;
    let entries = match std::fs::read_dir(&directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    let mut paths = entries
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<std::io::Result<Vec<_>>>()?;
    paths.sort();
    let mut locked = Vec::new();
    for path in paths {
        let extension = path.extension().and_then(|extension| extension.to_str());
        if extension != Some("reader") {
            continue;
        }
        let (mut file, inspected) = match open_trusted_reader_breadcrumb(&path) {
            Ok(opened) => opened,
            Err(_) => continue,
        };
        match fs2::FileExt::try_lock_exclusive(&file) {
            Ok(()) => {
                // Keep the inspected inode exclusively locked through the
                // final identity check and unlink. Unlocking first would let
                // a new cooperative hydration claim the stale pathname in
                // the cleanup gap.
                reclaim_reader_breadcrumb_if_same(&path, &inspected);
                let _ = fs2::FileExt::unlock(&file);
                drop(file);
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                if reader_breadcrumb_path_still_names(&path, &inspected) {
                    let breadcrumb = read_locked_reader_breadcrumb(&mut file, &path, &inspected);
                    if reader_breadcrumb_path_still_names(&path, &inspected) {
                        locked.push(LockedReaderBreadcrumb {
                            #[cfg(any(test, feature = "test-seams"))]
                            path,
                            breadcrumb,
                        });
                    }
                }
            }
            Err(_) => continue,
        }
    }
    Ok(locked)
}

impl WriterWaitScope {
    pub(crate) fn enter(
        stop: Option<OwnerReadCancellation>,
        deadline: Option<crate::ReaderWaitDeadline>,
        payer: crate::memory_accounting::StoreAccountantHandle,
    ) -> Self {
        let previous = WRITER_WAIT.with(|slot| {
            slot.replace(Some(WriterWaitContext {
                stop,
                deadline,
                payer: Some(payer),
            }))
        });
        Self { previous }
    }
}

impl Drop for WriterWaitScope {
    fn drop(&mut self) {
        let previous = self.previous.take();
        WRITER_WAIT.with(|slot| *slot.borrow_mut() = previous);
    }
}

pub(super) fn current_writer_wait() -> WriterWaitContext {
    WRITER_WAIT.with(|slot| slot.borrow().clone().unwrap_or_default())
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn writer_open_diagnostic_budget() -> Result<contextdb_core::read_memory::ReadMemoryOwner>
{
    match current_writer_wait().payer {
        Some(original) => original
            .wait_memory_owner()
            .map_err(Error::NativeEngineOther),
        None => crate::read_image_memory::active_owner()?.diagnostic_budget(),
    }
}

/// A provisional registration: published before admission, retaining every
/// page, its exact root not yet known.
pub(crate) fn snapshot_is_provisional(snapshot: &redb::StorageSnapshotIdentity) -> bool {
    snapshot.transaction_id == 0 && snapshot.user_root.is_none()
}

pub(crate) fn snapshot_is_failed(snapshot: &redb::StorageSnapshotIdentity) -> bool {
    snapshot.transaction_id == FAILED_READER_TRANSACTION && snapshot.user_root.is_none()
}

/// Wait until no direct reader is holding this store.
///
/// The wait is answered by the READERS, never by what happens to be written
/// down about them. Every direct reader takes a shared advisory hold on the
/// companion beside this store for exactly as long as it holds the committed
/// image, so asking for that hold exclusively is the same question a caller
/// really has -- "may I take this store" -- put to the only thing that can
/// answer it. A reader whose breadcrumb could not be published is still a
/// reader and still holds the wait; a diagnostic directory that cannot be read
/// says nothing about whether the store is free; and a store nobody is holding
/// is released at once, whatever can or cannot be read about readers.
///
/// It is EVENT-DRIVEN, and deliberately has no timer anywhere in it. The
/// blocking acquisition puts this thread to sleep in the kernel until the last
/// holder lets go -- or dies, which the kernel treats as the same event. The
/// wake IS the release; nothing polls, and no interval decides how quickly a
/// waiter learns. Nothing in the store folder is opened, read, or locked.
///
/// Known runtime directories can name an unobservable hold. The wait verdict
/// itself comes from the companion hold.
///
/// `stop` ends the wait immediately: the caller is told the moment the token
/// is cancelled, through the token's own listener, so an interrupt is as
/// prompt as a release. A wait ended that way answers `Stopped` while the
/// kernel wait it started finishes on its own; that helper releases nothing
/// and holds nothing once it does.
/// The same wait, told the runtime ROOT an operator supplied.
///
/// The root selects the breadcrumbs used to explain an unobservable hold.
/// The companion still decides release, so a malformed channel path inside
/// a usable root cannot prevent this wait from completing.
///
/// A root that cannot be read does not turn a released companion hold into
/// `Unobservable`; open validation reports the root's configuration refusal.
///
/// Every other part of the contract is [`wait_for_reader_release`]'s -- the
/// kernel wake IS the event, there is no timer anywhere in it, and `stop` is
/// answered as promptly as a release.
pub fn wait_for_reader_release_in_runtime_dir(
    path: &Path,
    runtime_root: Option<&Path>,
    stop: &OwnerReadCancellation,
) -> ReaderReleaseWait {
    let runtime = runtime_root.map(RuntimeDirectory::supplied);
    wait_for_reader_release(path, runtime.as_ref(), stop)
}

pub fn wait_for_reader_release(
    path: &Path,
    runtime: Option<&RuntimeDirectory>,
    stop: &OwnerReadCancellation,
) -> ReaderReleaseWait {
    // A caller that has already stopped is answered before anything at all is
    // asked of the store's surroundings.
    if stop.is_cancelled() {
        return ReaderReleaseWait::Stopped;
    }
    let companion = companion_path(path);
    let held = match open_trusted_companion_for_exclusive_hold(&companion) {
        Ok(file) => file,
        Err(error) => return unobservable_reader_hold(path, runtime, error),
    };
    // Armed BEFORE the first ask, so a reader that lets go in the gap between
    // the ask and the sleep has already left a notification queued rather than
    // slipping past unseen.
    let watch = match CoordinationWatch::arm(&companion, Some(stop)) {
        Ok(watch) => watch,
        Err(error) => return unobservable_reader_hold(path, runtime, error),
    };
    loop {
        if stop.is_cancelled() {
            return ReaderReleaseWait::Stopped;
        }
        match take_companion_range_lock_exclusive(&held, COMPANION_READER_HOLD_LOCK_OFFSET) {
            Ok(RangeLockOutcome::Taken) => {
                release_companion_range_lock(&held, COMPANION_READER_HOLD_LOCK_OFFSET);
                return ReaderReleaseWait::Released;
            }
            Ok(RangeLockOutcome::HeldByAnother) => {}
            Err(error) => return unobservable_reader_hold(path, runtime, error),
        }
        #[cfg(any(test, feature = "test-seams"))]
        read_persistence_test_scaffold::note_reader_release_block_for_test(
            read_persistence_test_scaffold::ReaderReleaseBlockForTest::UntilHolderReleased,
        );
        // The sleep IS the wait: this thread wakes when the kernel says
        // something happened to the companion -- a holder closing its
        // descriptor, which is what letting go looks like and what the kernel
        // does for a holder that dies -- or when this caller asks to stop. No
        // deadline of this wait's own is supplied, because a deadline is a poll
        // interval and a poll interval is how long a caller keeps a free store
        // hostage after it is free. And the sleeping is done HERE, on the
        // caller's own thread, so a caller that stops waiting leaves nothing
        // anywhere still queued for a hold nobody wants.
        if let Err(error) = watch.sleep(&held, COMPANION_READER_HOLD_LOCK_OFFSET, None) {
            return unobservable_reader_hold(path, runtime, error);
        }
    }
}

/// A wait that cannot ask the readers themselves says so, and names whoever
/// the diagnostic notes DO know about so the caller has somewhere to look.
///
/// This is the one answer where those notes earn their keep: they never decide
/// whether the store is free, but when the thing that decides cannot be
/// reached, a name is better than a bare refusal. A caller is told its wait
/// could not be observed -- never `Released` about a store nobody checked.
pub(super) fn unobservable_reader_hold(
    path: &Path,
    runtime: Option<&RuntimeDirectory>,
    error: std::io::Error,
) -> ReaderReleaseWait {
    let (observed, verified) = live_reader_holders(path, runtime.map(RuntimeDirectory::path));
    if observed == 0 {
        return ReaderReleaseWait::Unobservable(error);
    }
    let named = verified
        .iter()
        .map(|reader| format!("{} ({})", reader.process_id, reader.process_name))
        .collect::<Vec<_>>();
    let who = if named.is_empty() {
        "none of them identifiable".to_owned()
    } else {
        named.join(", ")
    };
    ReaderReleaseWait::Unobservable(std::io::Error::other(format!(
        "{error}; {observed} direct reader(s) are recorded as hydrating or finishing with this store: {who}"
    )))
}

pub(super) fn observed_reader_holders(
    database_path: &Path,
    runtime: &RuntimeDirectory,
) -> std::io::Result<(u64, Vec<ReaderBreadcrumb>)> {
    let locked = locked_reader_breadcrumbs(database_path, runtime.path())?;
    let observed = u64::try_from(locked.len()).unwrap_or(u64::MAX);
    let mut verified = locked
        .into_iter()
        .filter_map(|locked| locked.breadcrumb)
        .filter_map(verify_locked_reader_breadcrumb)
        .collect::<Vec<_>>();
    verified.sort_by(|left, right| {
        (left.process_id, left.process_start.0, &left.process_name).cmp(&(
            right.process_id,
            right.process_start.0,
            &right.process_name,
        ))
    });
    verified.dedup_by(|left, right| {
        left.process_id == right.process_id && left.process_start == right.process_start
    });
    Ok((observed, verified))
}

/// Direct reader identities from each known coordination directory.
pub(super) fn live_reader_holders(
    database_path: &Path,
    runtime_dir: Option<&Path>,
) -> (u64, Vec<ReaderBreadcrumb>) {
    let mut observed = 0;
    let mut verified = Vec::new();
    for runtime in reader_inventory_directories(runtime_dir) {
        if let Ok((count, readers)) = observed_reader_holders(database_path, &runtime) {
            observed += count;
            verified.extend(readers);
        }
    }
    verified.sort_by_key(|reader| (reader.process_id, reader.process_start.0));
    verified.dedup_by_key(|reader| (reader.process_id, reader.process_start.0));
    (observed, verified)
}

/// The default runtime directory, retained for tests and default-only callers.
#[cfg(any(test, feature = "test-seams"))]
pub(super) fn reader_breadcrumb_runtime_directory() -> Option<RuntimeDirectory> {
    reader_runtime_directory(None)
}

/// True when the operating system, not the store's contents, refused the
/// open. Every other Redb open failure keeps its existing classification.
pub(super) fn open_was_refused_by_permissions(error: &redb::DatabaseError) -> bool {
    matches!(
        error,
        redb::DatabaseError::Storage(redb::StorageError::Io(io))
            if io.kind() == std::io::ErrorKind::PermissionDenied
    )
}

/// The open reached the path and found nothing there.
///
/// This is the store's OWN attempt reporting back, not a separate existence
/// question asked beforehand -- which is the whole point, because an answer
/// asked for separately can go stale between the asking and the open.
pub(super) fn open_found_no_store(error: &redb::DatabaseError) -> bool {
    matches!(
        error,
        redb::DatabaseError::Storage(redb::StorageError::Io(io))
            if io.kind() == std::io::ErrorKind::NotFound
    )
}

impl SourceMemoryReservation {
    pub(super) fn configure_cache(&self, selected: usize) {
        let mut funding = self
            .cache_funding
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        funding.fixed = self
            .bytes
            .load(Ordering::Acquire)
            .checked_sub(selected)
            .expect("cache is inside source reserve");
        funding.selected = selected;
    }

    #[expect(
        clippy::result_large_err,
        reason = "Admission failures retain their original inline diagnostics."
    )]
    pub(super) fn select_cache_capacity(
        &self,
        selected: usize,
        held: impl FnOnce() -> usize,
    ) -> Result<usize> {
        let mut funding = self
            .cache_funding
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let next = funding
            .fixed
            .checked_add(selected.max(held()))
            .ok_or_else(|| Error::Other("reader cache reserve overflow".into()))?;
        let previous = self.bytes.load(Ordering::Acquire);
        if next > previous {
            self.memory.reserve(next - previous)?;
        }
        self.bytes.store(next, Ordering::Release);
        let old = funding.selected;
        funding.selected = selected;
        if next < previous {
            self.memory.release(previous - next);
        }
        Ok(old)
    }

    pub(super) fn settle_cache_free(&self, held: &std::sync::atomic::AtomicUsize) {
        let funding = self
            .cache_funding
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let next = funding
            .fixed
            .saturating_add(funding.selected.max(held.load(Ordering::Acquire)));
        let previous = self.bytes.load(Ordering::Acquire);
        // This callback never admits storage: the selecting caller prepaid the
        // capacity, and a detached holder keeps any physical excess until free.
        if next < previous {
            self.bytes.store(next, Ordering::Release);
            self.memory.release(previous - next);
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn new(
        memory: crate::read_image_memory::ImageMemoryHandle,
        bytes: usize,
    ) -> Result<SourceShared<Self>> {
        let layout = SourceShared::<Self>::layout();
        let control = memory
            .direct_source_native_credit(layout.size())
            .map_err(|refusal| {
                native_storage_admission::original_storage_error(
                    native_storage_admission::NativeStorageFailure::Refused(refusal),
                )
            })?;
        memory.reserve(bytes)?;
        SourceShared::try_new_prepaid(
            Self {
                memory,
                bytes: std::sync::atomic::AtomicUsize::new(bytes),
                cache_funding: std::sync::Mutex::new(SourceCacheFunding {
                    selected: 0,
                    fixed: bytes,
                }),
            },
            control,
        )
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn detach_control(
        &self,
        bytes: usize,
    ) -> Result<crate::memory_accounting::ControlCredit> {
        let mut funding = self
            .cache_funding
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        #[cfg(test)]
        let _transfer_observation =
            direct_source_contract_rows::TransferObservation::begin(self, bytes);
        self.bytes
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |held| {
                held.checked_sub(bytes)
            })
            .map_err(|_| {
                Error::Other("source control exceeds its existing fixed reservation".into())
            })?;
        match self.memory.detach_direct_source_credit(bytes) {
            Ok(credit) => {
                funding.fixed = funding
                    .fixed
                    .checked_sub(bytes)
                    .expect("retained control is in fixed reserve");
                Ok(credit)
            }
            Err(error) => {
                self.bytes.fetch_add(bytes, Ordering::SeqCst);
                Err(error)
            }
        }
    }
}

impl Drop for SourceMemoryReservation {
    fn drop(&mut self) {
        self.memory.release(self.bytes.load(Ordering::SeqCst));
    }
}

impl WritableRawVectorBodyScan {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn bind_stream_selection(
        &mut self,
        partition: &VectorPartitionRef,
        receipt: &dyn std::any::Any,
    ) -> Result<()> {
        self.selected_stream_identity = None;
        let cursor = receipt
            .downcast_ref::<vector_membership_source::MembershipCursor>()
            .ok_or_else(|| {
                vector_membership_source::damage("raw body stream received another receipt type")
            })?;
        let owner = vector_membership_source::MembershipOwner::Writable(self.source.clone());
        self.selected_stream_identity =
            Some(cursor.validate_selected(&owner, &self._transaction, partition)?);
        Ok(())
    }

    pub(crate) fn consume_stream_selection(&mut self, identity: &RawVectorDirectoryEntry) -> bool {
        self.selected_stream_identity
            .take()
            .is_some_and(|selected| selected == *identity)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn read_partition_body(
        &mut self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        identity: &RawVectorDirectoryEntry,
        expected_dimension: usize,
        quantization: VectorQuantization,
        membership_authenticated: bool,
    ) -> Result<VectorEntry> {
        self.read_partition_body_before(
            index,
            partition_key,
            identity,
            expected_dimension,
            quantization,
            membership_authenticated,
            &mut || Ok(()),
        )
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn read_partition_body_before(
        &mut self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        identity: &RawVectorDirectoryEntry,
        expected_dimension: usize,
        quantization: VectorQuantization,
        membership_authenticated: bool,
        before: &mut dyn FnMut() -> Result<()>,
    ) -> Result<VectorEntry> {
        self.read_partition_body_with_owning_row_before(
            index,
            partition_key,
            identity,
            expected_dimension,
            quantization,
            membership_authenticated,
            None,
            before,
        )
    }
    #[expect(
        clippy::result_large_err,
        reason = "Native refusal diagnostics remain inline when the original budget is exhausted."
    )]
    pub(super) fn read_partition_body_with_owning_row_before(
        &mut self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        identity: &RawVectorDirectoryEntry,
        expected_dimension: usize,
        quantization: VectorQuantization,
        membership_authenticated: bool,
        row_evidence: Option<&vector_membership_source::AuthenticatedOwningRow>,
        before: &mut dyn FnMut() -> Result<()>,
    ) -> Result<VectorEntry> {
        let owner = vector_membership_source::MembershipOwner::Writable(self.source.clone());
        let historical_memory =
            vector_membership_source::historical_operation_memory(&owner, &self._transaction)?;
        let mut body = || {
            let mut checkpoint = || {
                vector_membership_source::operation_checkpoint()?;
                before()
            };
            let before: &mut dyn FnMut() -> Result<()> = &mut checkpoint;
            vector_membership_source::authenticate_body_schema(
                &owner,
                &self._transaction,
                index,
                expected_dimension,
                quantization,
            )?;
            self.read_partition_body_in_capture(
                index,
                partition_key,
                identity,
                expected_dimension,
                quantization,
                membership_authenticated,
                row_evidence,
                before,
            )
        };
        match historical_memory {
            Some(memory) => graph_read::with_pages(memory, body),
            None => body(),
        }
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn read_partition_body_in_capture(
        &mut self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        identity: &RawVectorDirectoryEntry,
        expected_dimension: usize,
        quantization: VectorQuantization,
        membership_authenticated: bool,
        row_evidence: Option<&vector_membership_source::AuthenticatedOwningRow>,
        before: &mut dyn FnMut() -> Result<()>,
    ) -> Result<VectorEntry> {
        before()?;
        let source = &self.source;
        if let Some(root) = self
            ._root
            .as_ref()
            .and_then(|root| root.downcast_ref::<WriterSnapshot>())
        {
            root.check_table_scope(&index.table)?;
        }
        if !membership_authenticated {
            let memberships = self
                ._transaction
                .open_table(VECTOR_PARTITION_MEMBERSHIP_TABLE)
                .map_err(|error| match error {
                    redb::TableError::TableDoesNotExist(_) => source.vector_partition_corruption(
                        "a raw-vector candidate has no partition membership table",
                    ),
                    error => vector_membership_pages::metadata_error(error),
                })?;
            let row_prefix = RedbPersistence::vector_partition_membership_row_prefix(
                index,
                partition_key,
                identity.row_id,
            );
            let mut matched = None;
            let mut saw_vector_identity = false;
            for item in memberships
                .range(row_prefix.as_slice()..)
                .map_err(vector_membership_pages::metadata_error)?
            {
                vector_membership_source::operation_checkpoint()?;
                let (key, value) = item.map_err(vector_membership_pages::metadata_error)?;
                if !key.value().starts_with(&row_prefix) {
                    break;
                }
                let membership = vector_membership_source::decode_forward_for_route(
                    index,
                    partition_key,
                    key.value(),
                    value.value(),
                )?;
                if RedbPersistence::vector_partition_membership_key(&membership).as_slice()
                    != key.value()
                    || membership.index != *index
                    || membership.partition_key != *partition_key
                    || membership.row_id != identity.row_id
                {
                    return Err(source.vector_partition_corruption(
                        "a raw-vector candidate membership disagrees with its point key",
                    ));
                }
                if membership.vector_created_tx != identity.created_tx
                    || membership.vector_lsn != identity.lsn
                {
                    continue;
                }
                saw_vector_identity = true;
                if membership.deleted_tx != identity.deleted_tx {
                    continue;
                }
                if vector_membership_source::body_identity_for_record(&membership) != *identity {
                    return Err(vector_membership_source::damage(
                        "candidate provenance differs from its captured membership",
                    ));
                }
                if membership.visible_from
                    < TxId(
                        membership
                            .row_created_tx
                            .0
                            .max(membership.vector_created_tx.0),
                    )
                    || membership
                        .deleted_tx
                        .is_some_and(|deleted| deleted.0 <= membership.visible_from.0)
                {
                    return Err(source.vector_partition_corruption(
                        "a raw-vector candidate membership has an invalid lifecycle boundary",
                    ));
                }
                if matched.replace(membership).is_some() {
                    return Err(source.vector_partition_corruption(
                        "a raw-vector candidate identity has duplicate partition memberships",
                    ));
                }
            }
            matched.ok_or_else(|| {
                source.vector_partition_corruption(if saw_vector_identity {
                    "a raw-vector candidate deletion transaction changed outside its directory"
                } else {
                    "a raw-vector candidate identity has no durable partition membership"
                })
            })?;
        }
        let vectors = self.vectors.as_ref().ok_or_else(|| {
            source.vector_partition_corruption(
                "a raw-vector candidate membership has no vector body table",
            )
        })?;
        let key = RedbPersistence::vector_identity_key(
            index,
            identity.row_id,
            identity.created_tx,
            identity.lsn,
        );
        #[cfg(any(test, feature = "test-seams"))]
        crate::writer_query_test_seams::note_physical_source(
            crate::writer_query_test_seams::PhysicalFamily::Vector,
            &self._transaction,
        );
        let stored = {
            #[cfg(any(test, feature = "test-seams"))]
            let _body_reads = crate::writer_query_test_seams::physical_operation(
                crate::writer_query_test_seams::PhysicalFamily::Vector,
                &index.table,
            );
            vectors
                .get_bounded(key.as_slice())
                .map_err(vector_membership_pages::metadata_error)?
                .ok_or_else(|| {
                    source.vector_partition_corruption(
                        "a raw-vector candidate identity has no durable body",
                    )
                })?
        };
        #[cfg(any(test, feature = "test-seams"))]
        {
            crate::read_work_probe::note_vector_body_access();
            crate::read_work_probe::note_decoded_vector();
        }
        contextdb_core::find_attribution::count("decoded_vectors", 1);
        validate_raw_vector_entry_encoded_width(
            stored.value(),
            index,
            expected_dimension,
            |reason| source.vector_partition_corruption(reason),
        )?;
        let mut entry = RedbPersistence::decode_vector_entry(stored.value()).map_err(|error| {
            if preserves_typed_read_refusal(&error) {
                return error;
            }
            source.vector_partition_corruption(format!(
                "a raw-vector candidate body cannot be decoded: {error}"
            ))
        })?;
        if entry.index != *index
            || entry.row_id != identity.row_id
            || entry.created_tx != identity.created_tx
            || entry.lsn != identity.lsn
        {
            return Err(source.vector_partition_corruption(
                "a raw-vector candidate body disagrees with its membership identity",
            ));
        }
        if entry.vector.len() != expected_dimension {
            return Err(source.vector_partition_corruption(format!(
                "raw-vector candidate dimension {} does not match declared dimension {expected_dimension}",
                entry.vector.len()
            )));
        }
        let owner = vector_membership_source::MembershipOwner::Writable(self.source.clone());
        let historical_memory =
            vector_membership_source::historical_operation_memory(&owner, &self._transaction)?;
        if let Some(memory) = historical_memory {
            let _controls = contextdb_core::read_memory::ReadCredit::try_new(
                memory.clone(),
                std::mem::size_of::<content_identity::ContentIdentityReadVerifier>()
                    .checked_add(std::mem::size_of::<content_identity::ContentIdentityMutation>())
                    .and_then(|n| n.checked_add(entry.vector.len().checked_mul(4)?))
                    .and_then(|n| n.checked_add(8 + 32 + 256))
                    .ok_or_else(|| {
                        vector_membership_source::damage(
                            "historical vector proof capacity overflow",
                        )
                    })?,
                "reader",
                "historical_vector_body_proof",
                "Raise the declared reader memory limit.",
            )?;
            let verifier = content_identity::ContentIdentityReadVerifier::open(
                &self._transaction,
                Path::new("historical vector body"),
                None,
            )?;
            verifier.verify_with_read_memory_before(
                &content_identity::decoded_vector_version_mutation(&entry, quantization),
                memory,
                before,
            )?;
            if let Some(evidence) = row_evidence {
                evidence.verify(
                    &owner,
                    &self._transaction,
                    index,
                    identity,
                    Some(entry.deleted_tx),
                    before,
                )?;
            } else {
                vector_membership_source::authenticate_owning_row(
                    &owner,
                    &self._transaction,
                    index,
                    identity,
                    Some(entry.deleted_tx),
                    before,
                )?;
            }
        } else if !self.producer_body_root {
            // The authenticated producer-root path never uses a content
            // verifier. Keep this fallback verifier on the stack for the call,
            // rather than retaining all its table handles in every heap cursor.
            let verifier = content_identity::ContentIdentityReadVerifier::open(
                &self._transaction,
                &self.source.path,
                self.source.proof_accountant(),
            )?;
            verifier.verify_with_nodes(
                &mut self.proof,
                &content_identity::decoded_vector_version_mutation(&entry, quantization),
            )?;
            if entry.deleted_tx.is_none() {
                verifier.verify_with_nodes(
                    &mut self.proof,
                    &content_identity::decoded_vector_mutation(&entry, quantization),
                )?;
            }
        }
        before()?;
        entry.deleted_tx = identity.deleted_tx;
        Ok(entry)
    }
}

impl RetainedRawVectorBodyScan {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn bind_stream_selection(
        &mut self,
        partition: &VectorPartitionRef,
        receipt: &dyn std::any::Any,
    ) -> Result<()> {
        self.selected_stream_identity = None;
        let cursor = receipt
            .downcast_ref::<vector_membership_source::MembershipCursor>()
            .ok_or_else(|| {
                vector_membership_source::damage("raw body stream received another receipt type")
            })?;
        let owner = vector_membership_source::MembershipOwner::Retained(self.source.clone());
        self.selected_stream_identity =
            Some(cursor.validate_selected(&owner, &self._transaction, partition)?);
        Ok(())
    }

    pub(crate) fn consume_stream_selection(&mut self, identity: &RawVectorDirectoryEntry) -> bool {
        self.selected_stream_identity
            .take()
            .is_some_and(|selected| selected == *identity)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn read_partition_body(
        &mut self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        identity: &RawVectorDirectoryEntry,
        expected_dimension: usize,
        quantization: VectorQuantization,
        membership_authenticated: bool,
    ) -> Result<VectorEntry> {
        self.read_partition_body_before(
            index,
            partition_key,
            identity,
            expected_dimension,
            quantization,
            membership_authenticated,
            &mut || Ok(()),
        )
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn read_partition_body_before(
        &mut self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        identity: &RawVectorDirectoryEntry,
        expected_dimension: usize,
        quantization: VectorQuantization,
        membership_authenticated: bool,
        before: &mut dyn FnMut() -> Result<()>,
    ) -> Result<VectorEntry> {
        self.read_partition_body_with_owning_row_before(
            index,
            partition_key,
            identity,
            expected_dimension,
            quantization,
            membership_authenticated,
            None,
            before,
        )
    }
    #[expect(
        clippy::result_large_err,
        reason = "Native refusal diagnostics remain inline when the original budget is exhausted."
    )]
    pub(super) fn read_partition_body_with_owning_row_before(
        &mut self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        identity: &RawVectorDirectoryEntry,
        expected_dimension: usize,
        quantization: VectorQuantization,
        membership_authenticated: bool,
        row_evidence: Option<&vector_membership_source::AuthenticatedOwningRow>,
        before: &mut dyn FnMut() -> Result<()>,
    ) -> Result<VectorEntry> {
        let owner = vector_membership_source::MembershipOwner::Retained(self.source.clone());
        let historical_memory =
            vector_membership_source::historical_operation_memory(&owner, &self._transaction)?;
        let mut body = || {
            let mut checkpoint = || {
                vector_membership_source::operation_checkpoint()?;
                before()
            };
            let before: &mut dyn FnMut() -> Result<()> = &mut checkpoint;
            vector_membership_source::authenticate_body_schema(
                &owner,
                &self._transaction,
                index,
                expected_dimension,
                quantization,
            )?;
            self.read_partition_body_in_capture(
                index,
                partition_key,
                identity,
                expected_dimension,
                quantization,
                membership_authenticated,
                row_evidence,
                before,
            )
        };
        match historical_memory {
            Some(memory) => graph_read::with_pages(memory, body),
            None => body(),
        }
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn read_partition_body_in_capture(
        &mut self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        identity: &RawVectorDirectoryEntry,
        expected_dimension: usize,
        quantization: VectorQuantization,
        membership_authenticated: bool,
        row_evidence: Option<&vector_membership_source::AuthenticatedOwningRow>,
        before: &mut dyn FnMut() -> Result<()>,
    ) -> Result<VectorEntry> {
        before()?;
        let source = &self.source;
        if !membership_authenticated {
            let memberships = self
                ._transaction
                .open_table(VECTOR_PARTITION_MEMBERSHIP_TABLE)
                .map_err(|error| match error {
                    redb::TableError::TableDoesNotExist(_) => source.raw_vector_corruption(
                        "a raw-vector candidate has no partition membership table",
                    ),
                    error => vector_membership_pages::metadata_error(error),
                })?;
            source.raw_vector_candidate_membership(&memberships, index, partition_key, identity)?;
        }
        let vectors = self.vectors.as_ref().ok_or_else(|| {
            source
                .raw_vector_corruption("a raw-vector candidate membership has no vector body table")
        })?;
        let mut entry =
            source.raw_vector_candidate_body(vectors, index, identity, expected_dimension)?;
        let owner = vector_membership_source::MembershipOwner::Retained(self.source.clone());
        let historical_memory =
            vector_membership_source::historical_operation_memory(&owner, &self._transaction)?;
        if let Some(memory) = historical_memory {
            let _controls = contextdb_core::read_memory::ReadCredit::try_new(
                memory.clone(),
                std::mem::size_of::<content_identity::ContentIdentityReadVerifier>()
                    .checked_add(std::mem::size_of::<content_identity::ContentIdentityMutation>())
                    .and_then(|n| n.checked_add(entry.vector.len().checked_mul(4)?))
                    .and_then(|n| n.checked_add(8 + 32 + 256))
                    .ok_or_else(|| {
                        vector_membership_source::damage(
                            "historical vector proof capacity overflow",
                        )
                    })?,
                "reader",
                "historical_vector_body_proof",
                "Raise the declared reader memory limit.",
            )?;
            let verifier = content_identity::ContentIdentityReadVerifier::open(
                &self._transaction,
                Path::new("historical vector body"),
                None,
            )?;
            verifier.verify_with_read_memory_before(
                &content_identity::decoded_vector_version_mutation(&entry, quantization),
                memory,
                before,
            )?;
            if let Some(evidence) = row_evidence {
                evidence.verify(
                    &owner,
                    &self._transaction,
                    index,
                    identity,
                    Some(entry.deleted_tx),
                    before,
                )?;
            } else {
                vector_membership_source::authenticate_owning_row(
                    &owner,
                    &self._transaction,
                    index,
                    identity,
                    Some(entry.deleted_tx),
                    before,
                )?;
            }
        } else if !self.producer_body_root {
            let mut proof = source
                .content_proof
                .lock()
                .map_err(|_| source.content_identity_corruption("content proof lock poisoned"))?;
            let verifier = &source.content_identity;
            verifier
                .verify_with_nodes(
                    &mut proof,
                    &content_identity::decoded_vector_version_mutation(&entry, quantization),
                )
                .map_err(|error| source.content_identity_error(error))?;
            if entry.deleted_tx.is_none() {
                verifier
                    .verify_with_nodes(
                        &mut proof,
                        &content_identity::decoded_vector_mutation(&entry, quantization),
                    )
                    .map_err(|error| source.content_identity_error(error))?;
            }
        }
        #[cfg(any(test, feature = "test-seams"))]
        crate::read_work_probe::note_authoritative_body_verified();
        before()?;
        entry.deleted_tx = identity.deleted_tx;
        Ok(entry)
    }
}
impl content_identity::ProofPoolGrowth for CommittedCountProofMemory {
    fn ceiling(&self) -> usize {
        usize::MAX
    }
    fn grow(&self, bytes: usize) -> bool {
        match self.memory.reserve(bytes) {
            Ok(()) => true,
            Err(error) => {
                *self.refusal.lock().unwrap_or_else(|e| e.into_inner()) = Some(error);
                false
            }
        }
    }
}

/// Encode a transient leading-prefix boundary with exact prepaid storage.
/// Borrowed components avoid copying the caller's strings just to seek.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn charged_count_group_boundary(
    prefix: &[DirectedValue],
    memory: &crate::read_image_memory::ImageMemoryHandle,
) -> Result<Vec<u8>> {
    let slots = prefix
        .len()
        .checked_mul(std::mem::size_of::<(bool, &Value)>())
        .ok_or_else(|| Error::Other("count boundary size overflow".into()))?;
    memory.reserve(slots)?;
    let parts: Vec<_> = prefix
        .iter()
        .map(|part| match part {
            DirectedValue::Asc(TotalOrdAsc(value)) => (false, value),
            DirectedValue::Desc(TotalOrdDesc(value)) => (true, value),
        })
        .collect();
    struct ByteCount(usize);
    impl std::io::Write for ByteCount {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 = self
                .0
                .checked_add(bytes.len())
                .ok_or_else(|| std::io::Error::other("count boundary size overflow"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let value = (&parts, RowId(0), TxId(0));
    let mut count = ByteCount(0);
    bincode::serde::encode_into_std_write(value, &mut count, bincode::config::standard())
        .map_err(|e| Error::Other((format!("count boundary encode: {e}")).into()))?;
    memory.reserve(count.0)?;
    let mut encoded = Vec::with_capacity(count.0);
    bincode::serde::encode_into_std_write(value, &mut encoded, bincode::config::standard())
        .map_err(|e| Error::Other((format!("count boundary encode: {e}")).into()))?;
    drop(parts);
    memory.release(slots);
    Ok(encoded)
}

// Check and charge before advancing the physical iterator, including its
// terminal probe. A refused charge must leave the source untouched.
#[allow(dead_code)] // Exercised by the unit-level precharge contract.
pub(super) fn charged_ordered_next<I: Iterator, E>(
    entries: &mut I,
    check_boundary: &mut dyn FnMut() -> std::result::Result<(), E>,
    before_touch: &mut dyn FnMut() -> std::result::Result<(), E>,
) -> std::result::Result<Option<I::Item>, E> {
    check_boundary()?;
    before_touch()?;
    Ok(entries.next())
}

impl Drop for VerifiedOrderedRunMemory {
    fn drop(&mut self) {
        self.memory.release(self.held());
    }
}

impl VerifiedOrderedRunMemory {
    pub(super) fn held(&self) -> usize {
        self.bytes.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub(super) fn shrink_to(&self, bytes: usize) {
        let held = self.held();
        let bytes = bytes.min(held);
        self.memory.release(held.saturating_sub(bytes));
        self.bytes.store(bytes, std::sync::atomic::Ordering::SeqCst);
    }
}

impl content_identity::ProofPoolGrowth for VerifiedOrderedRunMemory {
    fn ceiling(&self) -> usize {
        self.ceiling
    }

    fn grow(&self, bytes: usize) -> bool {
        use std::sync::atomic::Ordering;
        match self.held().checked_add(bytes) {
            Some(target) if target <= self.ceiling => {}
            _ => return false,
        }
        loop {
            if self.memory.reserve(bytes).is_ok() {
                self.bytes.fetch_add(bytes, Ordering::SeqCst);
                return true;
            }
            // Evict before admitting. A cached window is a cache; the walk
            // asking for this page is the work. No budget is raised to serve
            // it, and a caller whose session genuinely has no room for one
            // more page still gets the existing typed refusal.
            let Some(cache) = self.cache.upgrade() else {
                return false;
            };
            if !evict_oldest_verified_run(&cache) {
                return false;
            }
        }
    }
}

/// Give back the oldest cached window, returning whether there was one. The
/// bytes it held are released when the last cursor lets go of it.
pub(super) fn evict_oldest_from_verified_runs(state: &mut VerifiedOrderedRunCacheState) -> bool {
    let Some(oldest) = state.insertion_order.pop_front() else {
        return false;
    };
    if let Some(evicted) = state.runs.remove(&oldest) {
        state.bytes = state.bytes.saturating_sub(evicted._memory.held());
        #[cfg(any(test, feature = "test-seams"))]
        verified_ordered_run_test_seams::note_capacity_displacement_for_test();
    }
    true
}

pub(super) fn evict_oldest_verified_run(state: &Mutex<VerifiedOrderedRunCacheState>) -> bool {
    let mut state = state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    evict_oldest_from_verified_runs(&mut state)
}

impl VerifiedOrderedRunCache {
    pub(super) fn new(memory: crate::read_image_memory::ImageMemoryHandle) -> Self {
        let max_bytes = memory
            .accountant
            .usage()
            .limit
            .map_or(VERIFIED_ORDERED_RUN_CACHE_MAX_BYTES, |limit| {
                (limit / 16).min(VERIFIED_ORDERED_RUN_CACHE_MAX_BYTES)
            });
        Self {
            memory,
            max_bytes,
            state: Arc::new(Mutex::new(VerifiedOrderedRunCacheState::default())),
            epoch: std::sync::atomic::AtomicU64::new(0),
        }
    }

    pub(super) fn get(&self, key: &[u8; 32]) -> Option<Arc<VerifiedOrderedRun>> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .runs
            .get(key)
            .cloned()
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub(super) fn clear_for_test(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.runs.clear();
        state.insertion_order.clear();
        state.bytes = 0;
        self.epoch.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    pub(super) fn begin_build(
        &self,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Option<Arc<VerifiedOrderedRunMemory>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        #[cfg(any(test, feature = "test-seams"))]
        if verified_ordered_run_test_seams::verified_run_memory_exhausted_for_test() {
            return None;
        }
        // The ceiling this build may grow to. Unchanged: the cache's share of
        // the caller's declared budget, capped by the build maximum. Nothing
        // is reserved until the walk names the bytes it actually needs.
        let ceiling = self.max_bytes.min(VERIFIED_ORDERED_RUN_BUILD_MAX_BYTES);
        if ceiling == 0 {
            return None;
        }
        Some(Arc::new(VerifiedOrderedRunMemory {
            memory: crate::read_image_memory::ImageMemory::charging_reader(
                self.memory.accountant.clone(),
                read_memory,
            ),
            cache: Arc::downgrade(&self.state),
            ceiling,
            bytes: std::sync::atomic::AtomicUsize::new(0),
        }))
    }

    pub(super) fn insert(
        &self,
        key: [u8; 32],
        postings: Vec<VerifiedOrderedPosting>,
        more: bool,
        quota_stopped: bool,
        memory: Arc<VerifiedOrderedRunMemory>,
        bytes: usize,
    ) -> Arc<VerifiedOrderedRun> {
        memory.shrink_to(bytes);
        let run = Arc::new(VerifiedOrderedRun {
            postings,
            more,
            quota_stopped,
            _memory: memory,
        });
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(existing) = state.runs.get(&key) {
            return Arc::clone(existing);
        }
        while state.bytes.saturating_add(bytes) > self.max_bytes {
            if !evict_oldest_from_verified_runs(&mut state) {
                break;
            }
        }
        state.bytes = state.bytes.saturating_add(bytes);
        state.insertion_order.push_back(key);
        state.runs.insert(key, Arc::clone(&run));
        run
    }
}

/// The cursor's opaque proof slot retains its original source reservation.
/// Page reconciliation must preserve that invoice until the last proof holder
/// frees the decoded pages. This observes admitted bytes, never a new reserve.
pub(crate) fn ordered_cursor_proof_bytes(
    cursor: &contextdb_relational::mem::BoundedOrderedRowCursor,
) -> usize {
    cursor
        .source_proof()
        .and_then(|slot| slot.0.downcast_ref::<Mutex<RetainedOrderedProof>>())
        .map_or(0, |proof| {
            proof
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                ._memory
                .held()
        })
}

impl ExportBodyAdmission {
    pub(super) fn empty(memory: crate::read_image_memory::ImageMemoryHandle) -> Self {
        Self {
            memory,
            decoded: None,
            decoded_controls: None,
        }
    }
}

impl CheckpointReadSource {
    /// Keep an identity failure that already carries the typed corruption
    /// error instead of wrapping its text a second time, and keep a memory
    /// refusal a memory refusal: a proof there was no room to walk is a budget
    /// to raise, not a store to distrust.
    pub(super) fn content_identity_error(&self, error: Error) -> Error {
        match error {
            Error::StoreCorrupted { .. } | Error::ReadCancelled | Error::ReadFailure(_) => error,
            error
                if crate::read_image_memory::is_memory_refusal(&error)
                    || crate::read_image_memory::is_missing_decode_owner(&error) =>
            {
                error
            }
            other => self.content_identity_corruption(other),
        }
    }

    pub(super) fn content_identity_corruption(&self, reason: impl std::fmt::Display) -> Error {
        crate::funded_error::corruption_display(
            self.decode_memory.diagnostic_budget(),
            self.database_path.display(),
            format_args!(
                "checkpoint source does not authenticate against its captured root: {reason}"
            ),
        )
    }

    /// Restore each exported row's stored values from the pinned source.
    ///
    /// A committed body the in-memory directory no longer carries -- a large
    /// text payload, a vector -- still belongs in the artifact, and it has to
    /// come from the same captured snapshot the rest of the artifact does.
    /// Fetching it from the live store instead would put a body a later commit
    /// wrote into an artifact that claims one commit boundary.
    ///
    /// The body is not taken on trust either: it is proved against the
    /// committed root of that same captured snapshot before it is copied. A
    /// table the source does not hold, a row it does not hold, a body whose
    /// physical key disagrees with it, and a live body that does not
    /// authenticate are all the same answer -- the export stops with the
    /// established corruption error and no artifact is finished. An exported
    /// row with a hollow body would otherwise restore as a row that silently
    /// lost its payload.
    ///
    /// Every body is admitted through the database's own accountant before it
    /// is copied or decoded, and the admission is handed back with the rows so
    /// it lasts as long as they do. A run stops at the last body the budget
    /// admits and says how many it filled, so a table of large bodies is
    /// exported in admitted runs rather than one whole-batch copy; a single
    /// body that does not fit on its own is the typed memory refusal, because
    /// there is no smaller run left to try.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn load_export_row_values_with_read_memory(
        &self,
        name: &str,
        meta: &TableMeta,
        rows: &mut [VersionedRow],
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<(usize, ExportBodyAdmission)> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        // The native image retains its exact control invoice itself. Keep
        // the existing return slot empty rather than charging that block twice.
        let controls = contextdb_core::read_memory::ReadCredit::new(read_memory.clone());
        let bridge = match &self.accountant {
            Some(accountant) => accountant.clone(),
            None => crate::memory_accounting::AccountantHandle::reader(
                None,
                read_memory.clone(),
                false,
            )?,
        };
        let decoded = crate::read_image_memory::ImageMemory::admitted_reader_for(
            bridge,
            read_memory,
            "reader",
            "materialize_answer",
            "Reduce the statement's output or raise the declared reader memory limit.",
        )?;
        let (filled, mut admission) =
            decoded.scope(|| self.load_export_row_values(name, meta, rows))?;
        admission.decoded = Some(decoded);
        admission.decoded_controls = Some(controls);
        Ok((filled, admission))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn load_export_row_values(
        &self,
        name: &str,
        meta: &TableMeta,
        rows: &mut [VersionedRow],
    ) -> Result<(usize, ExportBodyAdmission)> {
        // Explicit export readers supply their own child. Standalone writer
        // exports retain the captured source's writer funder.
        let memory =
            crate::read_image_memory::active_owner().unwrap_or_else(|_| self.decode_memory.clone());
        let admission = ExportBodyAdmission::empty(memory.row_owner()?);
        let operation = admission.memory.enter();
        let filled =
            self.load_export_row_values_in_scope(&operation, name, meta, rows, &admission)?;
        Ok((filled, admission))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn load_export_row_values_in_scope(
        &self,
        operation: &crate::read_image_memory::ImageMemoryScope,
        name: &str,
        meta: &TableMeta,
        rows: &mut [VersionedRow],
        admission: &ExportBodyAdmission,
    ) -> Result<usize> {
        let _operation = operation.reenter();
        if rows.is_empty() {
            return Ok(0);
        }
        let table_name = RedbPersistence::rel_table_name(name);
        let definition: TableDefinition<&[u8], &[u8]> = TableDefinition::new(&table_name);
        let relation = self
            .read
            .open_table(definition)
            .map_err(|error| match error {
                redb::TableError::TableDoesNotExist(_) => crate::funded_error::corruption_display(
                    self.decode_memory.diagnostic_budget(),
                    self.database_path.display(),
                    format_args!("checkpoint source relational table {name} is absent"),
                ),
                other => RedbPersistence::storage_error(other),
            })?;
        let mut filled = 0usize;
        let loaded = (|| -> Result<()> {
            for row in rows.iter_mut() {
                let key = RedbPersistence::rel_row_key(row);
                let stored = relation
                    .get(key.as_slice())
                    .map_err(RedbPersistence::storage_error)?
                    .ok_or_else(|| crate::funded_error::corruption_display(self.decode_memory.diagnostic_budget(), self.database_path.display(), format_args!(
                            "checkpoint source lost relational table {name} row {} at tx {} lsn {}",
                            row.row_id.0, row.created_tx.0, row.lsn.0
                        )))?;
                let durable = match admission.memory.scope(|| {
                    RedbPersistence::decode_relational_source_row(name, meta, stored.value())
                }) {
                    Ok(row) => row,
                    Err(error)
                        if filled != 0 && crate::read_image_memory::is_memory_refusal(&error) =>
                    {
                        break;
                    }
                    Err(error) => return Err(error),
                };
                if RedbPersistence::rel_row_key(&durable) != key {
                    return Err(self.content_identity_corruption(format!(
                        "relational table {name} row body disagrees with its physical key"
                    )));
                }
                // The committed root carries one record per LOGICAL row, so it is
                // the live version of a row that it can prove. A superseded
                // version carries the transaction that replaced it and is history
                // the root says nothing about; its physical key is still checked,
                // and the artifact's own restore reads the live body.
                if durable.deleted_tx.is_none() {
                    let mutation = content_identity::row_mutation(name, &durable, Some(meta))
                        .map_err(|error| self.content_identity_error(error))?;
                    self.content_identity
                        .verify(&mutation)
                        .map_err(|error| self.content_identity_error(error))?;
                }
                // Only the values come from the source. Visibility stays what the
                // export's own watermark already decided for this row.
                row.values = durable.values;
                filled += 1;
            }
            Ok(())
        })();
        if let Err(error) = loaded {
            // Payloads already adopted into the mutable batch must die before
            // the failed run returns its credit, including authentication errors.
            for row in &mut rows[..filled] {
                row.values.clear();
            }
            return Err(error);
        }
        Ok(filled)
    }

    /// Resolve the canonical body through this exact row version's membership.
    /// Row replacement stamps identify the placement, never the body version.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn export_row_vector_identity(
        &self,
        operation: &crate::read_image_memory::ImageMemoryScope,
        index: &VectorIndexRef,
        row: &VersionedRow,
    ) -> Result<(TxId, Lsn)> {
        let _operation = operation.reenter();
        let prefix_bytes = index
            .table
            .len()
            .saturating_add(index.column.len())
            .saturating_add(40);
        let _prefix_memory = self
            .accountant
            .as_ref()
            .map(|accountant| {
                crate::memory_accounting::OwnedMemoryReservation::try_new_for(
                    accountant.clone(),
                    prefix_bytes,
                    "checkpoint_export",
                    "membership_identity",
                    "Raise MEMORY_LIMIT to authenticate an exported vector placement.",
                )
            })
            .transpose()?;
        let mut prefix = Vec::with_capacity(prefix_bytes);
        RedbPersistence::append_partition_key_component(&mut prefix, index.table.as_bytes());
        RedbPersistence::append_partition_key_component(&mut prefix, index.column.as_bytes());
        prefix.extend_from_slice(&row.row_id.0.to_be_bytes());
        prefix.extend_from_slice(&row.created_tx.0.to_be_bytes());
        prefix.extend_from_slice(&row.lsn.0.to_be_bytes());
        let missing_membership = |error| match error {
            redb::TableError::TableDoesNotExist(_) => self.content_identity_corruption(
                "an exported vector row version has no durable membership table",
            ),
            other => RedbPersistence::storage_error(other),
        };
        let reverse = self
            .read
            .open_table(vector_membership_pages::REVERSE)
            .map_err(&missing_membership)?;
        let primary = self
            .read
            .open_table(VECTOR_PARTITION_MEMBERSHIP_TABLE)
            .map_err(missing_membership)?;
        let mut identity = None;
        for item in reverse
            .prefix_bounded(prefix.as_slice())
            .map_err(RedbPersistence::storage_error)?
        {
            let (key, value) = item.map_err(RedbPersistence::storage_error)?;
            let _window = self
                .accountant
                .as_ref()
                .map(|accountant| {
                    crate::memory_accounting::OwnedMemoryReservation::try_new_for(
                        accountant.clone(),
                        value.value().len().saturating_mul(16).saturating_add(4096),
                        "checkpoint_export",
                        "membership_identity",
                        "Raise MEMORY_LIMIT to authenticate an exported vector placement.",
                    )
                })
                .transpose()?;
            let member: VectorPartitionMembershipRecord = decode_vector_partition_record_payload(
                VectorPartitionRecordKind::Membership,
                value.value(),
            )
            .map_err(|reason| self.content_identity_corruption(reason))?;
            if member.index != *index
                || member.row_id != row.row_id
                || member.row_created_tx != row.created_tx
                || member.row_lsn != row.lsn
                || vector_membership_pages::reverse_key(&member).as_slice() != key.value()
            {
                return Err(self.content_identity_corruption(
                    "exported row membership disagrees with its exact reverse key",
                ));
            }
            let primary_key = RedbPersistence::vector_partition_membership_key(&member);
            let companion = primary
                .get_bounded(primary_key.as_slice())
                .map_err(RedbPersistence::storage_error)?;
            if companion
                .as_ref()
                .is_none_or(|stored| stored.value() != value.value())
            {
                return Err(self.content_identity_corruption(
                    "exported row membership disagrees with its primary companion",
                ));
            }
            let found = (member.vector_created_tx, member.vector_lsn);
            if identity.is_some_and(|previous| previous != found) {
                return Err(self.content_identity_corruption(
                    "one exported row version names different canonical vector bodies",
                ));
            }
            identity = Some(found);
        }
        identity.ok_or_else(|| {
            self.content_identity_corruption(
                "an exported vector row version has no exact durable membership",
            )
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn overlay_export_row_f32_projections_with_read_memory(
        &self,
        name: &str,
        meta: &TableMeta,
        rows: &mut [VersionedRow],
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<(
        crate::read_image_memory::ImageMemoryHandle,
        contextdb_core::read_memory::ReadCredit,
    )> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        // The native image retains its exact control invoice itself. Keep
        // the existing return slot empty rather than charging that block twice.
        let controls = contextdb_core::read_memory::ReadCredit::new(read_memory.clone());
        let bridge = match &self.accountant {
            Some(accountant) => accountant.clone(),
            None => crate::memory_accounting::AccountantHandle::reader(
                None,
                read_memory.clone(),
                false,
            )?,
        };
        let decoded = crate::read_image_memory::ImageMemory::admitted_reader_for(
            bridge,
            read_memory,
            "reader",
            "materialize_answer",
            "Reduce the statement's output or raise the declared reader memory limit.",
        )?;
        decoded.scope(|| self.overlay_export_row_f32_projections(name, meta, rows))?;
        Ok((decoded, controls))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn overlay_export_row_f32_projections(
        &self,
        name: &str,
        meta: &TableMeta,
        rows: &mut [VersionedRow],
    ) -> Result<ExportBodyAdmission> {
        // Explicit export readers supply their own child. Standalone writer
        // exports retain the captured source's writer funder.
        let memory =
            crate::read_image_memory::active_owner().unwrap_or_else(|_| self.decode_memory.clone());
        let admission = ExportBodyAdmission::empty(memory.row_owner()?);
        let _operation_scope = admission.memory.enter();
        if let Err(error) = self
            .overlay_export_row_f32_projections_with_admission(name, meta, rows, &admission, false)
        {
            for row in rows {
                row.values.clear();
            }
            return Err(error);
        }
        Ok(admission)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn overlay_export_row_f32_projections_with_admission(
        &self,
        name: &str,
        meta: &TableMeta,
        rows: &mut [VersionedRow],
        admission: &ExportBodyAdmission,
        replace_prepaid: bool,
    ) -> Result<()> {
        let _operation_scope = admission.memory.enter();
        if rows.is_empty() {
            return Ok(());
        }
        let f32_count = meta
            .columns
            .iter()
            .filter(|column| {
                matches!(column.column_type, ColumnType::Vector(_))
                    && matches!(column.quantization, VectorQuantization::F32)
            })
            .count();
        let _columns_credit = contextdb_core::read_memory::ReadCredit::try_new_owned(
            admission.memory.owner(),
            f32_count
                .checked_mul(std::mem::size_of::<(&str, usize, bool)>())
                .ok_or_else(|| Error::Other("export vector columns size overflow".into()))?,
            "checkpoint_export",
            "vector_columns",
            "Raise the reader memory limit to inspect exported vector columns.",
        )?;
        let mut f32_columns = Vec::with_capacity(f32_count);
        for column in &meta.columns {
            if let ColumnType::Vector(dimension) = column.column_type
                && matches!(column.quantization, VectorQuantization::F32)
            {
                f32_columns.push((column.name.as_str(), dimension, column.nullable));
            }
        }
        if f32_columns.is_empty() {
            return Ok(());
        }
        let table_name = RedbPersistence::rel_table_name(name);
        let definition: TableDefinition<&[u8], &[u8]> = TableDefinition::new(&table_name);
        let relation = self
            .read
            .open_table(definition)
            .map_err(|error| match error {
                redb::TableError::TableDoesNotExist(_) => crate::funded_error::corruption_display(
                    self.decode_memory.diagnostic_budget(),
                    self.database_path.display(),
                    format_args!("checkpoint source relational table {name} is absent"),
                ),
                other => RedbPersistence::storage_error(other),
            })?;
        for row in rows {
            let key = RedbPersistence::rel_row_key(row);
            let stored = relation
                .get(key.as_slice())
                .map_err(RedbPersistence::storage_error)?
                .ok_or_else(|| {
                    crate::funded_error::corruption_display(
                        self.decode_memory.diagnostic_budget(),
                        self.database_path.display(),
                        format_args!(
                            "checkpoint source lost relational table {name} row {} at tx {} lsn {}",
                            row.row_id.0, row.created_tx.0, row.lsn.0
                        ),
                    )
                })?;
            let row_memory = admission.memory.row_owner()?;
            let _row_scope = row_memory.enter();
            let durable = row_memory.scope(|| {
                RedbPersistence::decode_relational_source_row(name, meta, stored.value())
            })?;
            if RedbPersistence::rel_row_key(&durable) != key {
                return Err(self.content_identity_corruption(format!(
                    "relational table {name} row body disagrees with its physical key"
                )));
            }
            for (column, dimension, nullable) in &f32_columns {
                match durable.values.get(*column) {
                    Some(Value::Vector(vector)) => {
                        if vector.len() != *dimension {
                            return Err(self.content_identity_corruption(format!(
                                "{name}.{column} row {} has vector dimension {} instead of {dimension}",
                                durable.row_id.0,
                                vector.len()
                            )));
                        }
                        let index = VectorIndexRef::new(name, *column);
                        let (vector_created_tx, vector_lsn) =
                            self.export_row_vector_identity(&_row_scope, &index, &durable)?;
                        self.content_identity
                            .verify(&content_identity::decoded_vector_version_value_mutation(
                                &index,
                                durable.row_id,
                                vector_created_tx,
                                vector_lsn,
                                vector,
                                VectorQuantization::F32,
                            ))
                            .map_err(|error| self.content_identity_error(error))?;
                        let copied_bytes = vector
                            .len()
                            .checked_mul(std::mem::size_of::<f32>())
                            .ok_or_else(|| {
                                Error::Other("export vector copy size overflow".into())
                            })?;
                        admission.memory.reserve(copied_bytes)?;
                        let incoming_key = contextdb_core::read_memory::ReadCredit::try_new_owned(
                            admission.memory.owner(),
                            column.len(),
                            "checkpoint_export",
                            "vector_overlay_key",
                            "Raise the reader memory limit to export this vector column.",
                        )?;
                        let existing_key = row.values.contains_key(*column);
                        let prior = row
                            .values
                            .insert((*column).to_owned(), Value::Vector(vector.clone()));
                        let prior_bytes = prior.as_ref().map_or(0, |value| match value {
                            Value::Vector(vector) => vector.capacity() * std::mem::size_of::<f32>(),
                            _ => 0,
                        });
                        drop(prior);
                        if replace_prepaid {
                            admission.memory.release(prior_bytes);
                        }
                        // The existing key stays in the map on replacement.
                        if existing_key {
                            drop(incoming_key);
                        } else {
                            incoming_key.transfer();
                        }
                    }
                    None | Some(Value::Null) if *nullable => {
                        row.values.remove(*column);
                    }
                    None | Some(Value::Null) => {
                        return Err(self.content_identity_corruption(format!(
                            "{name}.{column} row {} is NULL although the vector column is required",
                            durable.row_id.0
                        )));
                    }
                    Some(_) => {
                        return Err(self.content_identity_corruption(format!(
                            "{name}.{column} row {} contains a non-vector value",
                            durable.row_id.0
                        )));
                    }
                }
            }
        }
        Ok(())
    }
}

impl RetainedReadSource {
    pub(crate) fn prepaid_cache_index_bytes(&self) -> usize {
        self.database.read_cache_index_bytes()
    }

    pub(crate) fn with_cache_page_window<R>(&self, operation: impl FnOnce() -> R) -> (R, f64) {
        self.database.with_read_cache_page_window(operation)
    }

    pub(crate) fn held_cache_page_bytes(&self) -> usize {
        self.database.read_cache_held_bytes()
    }

    #[expect(
        clippy::result_large_err,
        reason = "Cache admission retains original inline refusal diagnostics."
    )]
    pub(crate) fn select_cache_capacity(&self, capacity: usize) -> Result<()> {
        let source = &self._source_memory;
        let old = source
            .cache_funding
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .selected;
        let growing = capacity > old;
        if growing {
            source.select_cache_capacity(capacity, || self.database.read_cache_held_bytes())?;
        }
        let settled = self.database.with_read_cache_capacity(capacity, || {
            source.select_cache_capacity(capacity, || self.database.read_cache_held_bytes())
        });
        match settled {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(error)) => {
                self.database
                    .set_read_cache_capacity(old)
                    .map_err(RedbPersistence::storage_error)?;
                source.select_cache_capacity(old, || self.database.read_cache_held_bytes())?;
                Err(error)
            }
            Err(error) => {
                if growing {
                    source.select_cache_capacity(old, || self.database.read_cache_held_bytes())?;
                }
                Err(RedbPersistence::storage_error(error))
            }
        }
    }
    #[cfg(test)]
    pub(crate) fn write_control_for_test(&self) -> redb::CacheWriteControlForTest {
        self.database.write_control_for_test()
    }
    #[cfg(test)]
    pub(crate) fn cached_header_for_test(&self) -> redb::CachePageForTest {
        self.database.cached_header_for_test().unwrap()
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn cache_index_charge_for_test(&self) -> SourceCacheChargeForTest {
        let mut allocations = [(0, 0, 0); 140];
        let mut allocation_count = 0;
        let preparation = self.database.cache_allocations_for_test(&mut |request| {
            allocations[allocation_count] = request;
            allocation_count += 1;
        });
        let (preparation, preparation_invoice, payer_identity) =
            cache_reserve::direct_observation_for_test(preparation);
        let m = SourceShared::<SourceMemoryReservation>::layout();
        let r = Self::layout();
        SourceCacheChargeForTest {
            index_bytes: self.database.cache_index_bytes(),
            remaining_base: self._source_memory.bytes.load(Ordering::SeqCst),
            source_control: (self._source_memory.identity(), m.size(), m.align()),
            retained_control: (self.identity(), r.size(), r.align()),
            preparation,
            preparation_invoice,
            payer_identity,
            allocations,
            allocation_count,
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn durable_membership_summary(
        &self,
        partition: &VectorPartitionRef,
        owner: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<contextdb_vector::store::RawVectorMembershipSummary> {
        let mut scratch = owner.scratch();
        scratch.reserve(
            partition
                .index
                .table
                .len()
                .saturating_add(partition.index.column.len())
                .saturating_add(partition.partition_key.estimated_bytes())
                .saturating_mul(32)
                .saturating_add(4096),
        )?;

        self.with_read(|read| vector_membership_source::read_summary(read, partition))
    }

    pub(crate) fn ordered_prefix_snapshot_is_current(&self, snapshot: SnapshotId) -> bool {
        snapshot == self.logical_frontier
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn durable_membership_partitions(
        &self,
        owner: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<
        Vec<(
            VectorPartitionRef,
            contextdb_vector::store::RawVectorMembershipSummary,
        )>,
    > {
        self.with_read(|read| vector_membership_source::membership_partitions_in_read(read, owner))
    }

    pub(crate) fn storage_identity(&self) -> redb::StorageSnapshotIdentity {
        self.snapshot
    }

    pub(crate) fn registration(&self) -> &Arc<ReadSnapshotRegistrationGuard> {
        &self.registration
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn arm_verified_ordered_run_eviction_for_test(&self, postings: usize) {
        let _ = self;
        verified_ordered_run_test_seams::arm_eviction_after_cached_postings_for_test(postings);
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn begin_read(&self) -> Result<redb::ReadTransaction> {
        if self.registration.snapshot.storage_snapshot != self.snapshot {
            return Err(crate::funded_error::corruption_text(
                Ok(self.read_memory.clone()),
                self.database_path.display(),
                "retained source root differs from its authenticated registration".into(),
            ));
        }
        if self.registration.registration.is_none() {
            return Err(Error::Other((
                "retained snapshot read refused because its authenticated page hold was not published"
                    .to_owned()).into()));
        }
        let transaction = self
            .database
            .begin_external_read(self.snapshot)
            .map_err(|error| {
                crate::database::ImageHistoryError::store(
                    self.read_memory.clone(),
                    self.database_path.display(),
                    read_transaction_error(error),
                )
                .into_engine_error()
            })?;
        if transaction.storage_snapshot_identity() != self.snapshot {
            return Err(crate::funded_error::corruption_text(
                Ok(self.read_memory.clone()),
                self.database_path.display(),
                "retained source transaction escaped its registered snapshot".into(),
            ));
        }
        Ok(transaction)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn content_size_totals(
        &self,
        memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        before_table: &mut dyn FnMut() -> Result<()>,
    ) -> Result<content_size::ContentSizeTotals> {
        let memory: contextdb_core::read_memory::ReadMemoryOwner = memory.into();

        self.with_read(|read| {
            content_size::read_totals(
                read,
                &self.database_path.display().to_string(),
                memory,
                before_table,
            )
        })
    }

    // Snapshot metadata authentication may run outside a kernel page call.
    // Re-enter this capture's admitted owner when no request scope is active.
    #[expect(
        clippy::result_large_err,
        reason = "Keep the original typed memory refusal."
    )]
    pub(super) fn decode_scope(&self) -> Result<crate::read_image_memory::ImageMemoryScope> {
        match crate::read_image_memory::active_owner() {
            Ok(owner) => owner.enter_call(),
            Err(_) => self._source_memory.memory.enter_call(),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn with_read<T>(
        &self,
        read: impl FnOnce(&redb::ReadTransaction) -> Result<T>,
    ) -> Result<T> {
        let _decode_scope = self.decode_scope()?;
        let transaction = self.begin_read()?;
        read(&transaction)
    }

    /// Read a configuration fact from this image's pinned snapshot and prove
    /// its value against the committed configuration root before decoding it.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn load_image_config_value<T: serde::de::DeserializeOwned>(
        &self,
        key: &str,
    ) -> Result<Option<T>> {
        let _decode_scope = self.decode_scope()?;
        let mut cache = self
            .image_config_cache
            .lock()
            .map_err(RedbPersistence::storage_error)?;
        if let Some(cached) = cache.get(key) {
            return cached.as_deref().map(RedbPersistence::decode).transpose();
        }
        let bytes = self.with_read(|read| {
            let table = match read.open_table(CONFIG_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
                Err(error) => return Err(RedbPersistence::storage_error(error)),
            };
            let Some(value) = table.get(key).map_err(RedbPersistence::storage_error)? else {
                self.authenticate_mutation(&content_identity::config_mutations(key, None)[0])?;
                return Ok(None);
            };
            let bytes = value.value();
            self.authenticate_mutation(&content_identity::config_mutations(key, Some(bytes))[0])?;
            self._source_memory
                .memory
                .reserve(key.len().saturating_add(bytes.len()))?;
            Ok(Some(bytes.to_vec()))
        })?;
        cache.insert(key.to_owned(), bytes);
        cache
            .get(key)
            .and_then(Option::as_deref)
            .map(RedbPersistence::decode)
            .transpose()
    }

    /// A scalar historical coordinate is never retained in the image's
    /// startup cache. Walking history must not accumulate absent config keys.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn image_connected_membership_point(&self, key: &str) -> Result<Option<(u64, u64)>> {
        self.with_read(|read| {
            let table = match read.open_table(CONFIG_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => {
                    self.authenticate_mutation(&content_identity::config_mutations(key, None)[0])?;
                    return Ok(None);
                }
                Err(error) => return Err(RedbPersistence::storage_error(error)),
            };
            let value = table.get(key).map_err(RedbPersistence::storage_error)?;
            self.authenticate_mutation(
                &content_identity::config_mutations(key, value.as_ref().map(|value| value.value()))
                    [0],
            )?;
            value
                .map(|value| RedbPersistence::decode_config_value_exact(value.value()))
                .transpose()
        })
    }

    /// Compare the complete copied startup sequence with the projection
    /// committed in the canonical Configuration root record. The writer
    /// publishes both in one transaction; unrelated Configuration keys do
    /// not enter this open-time proof.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn authenticate_open_config(&self, copied: &[(String, Vec<u8>)]) -> Result<()> {
        self.content_identity.verify_open_config_projection(copied)
    }

    /// Load custody metadata only when a custody query asks for it. The
    /// writer's compact key/value directory is complete for this namespace
    /// and bound to the same committed snapshot as the ordinary config tree.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn load_image_custody_config(&self) -> Result<Vec<(String, Vec<u8>)>> {
        self.with_read(|read| {
            let table = read
                .open_table(READ_CUSTODY_CONFIG_TABLE)
                .map_err(RedbPersistence::storage_error)?;
            let root = table
                .get(READ_OPEN_CONFIG_ROOT_KEY)
                .map_err(RedbPersistence::storage_error)?
                .ok_or_else(|| self.content_identity_corruption("custody config root is absent"))?;
            let root = root.value();
            if root.len() != 40 {
                return Err(
                    self.content_identity_corruption("custody config root has invalid size")
                );
            }
            let metadata = read
                .open_table(CONTENT_IDENTITY_METADATA_TABLE)
                .map_err(RedbPersistence::storage_error)?;
            let commitment = metadata
                .get(READ_CUSTODY_CONFIG_COMMITMENT_KEY)
                .map_err(RedbPersistence::storage_error)?
                .ok_or_else(|| {
                    self.content_identity_corruption("custody config commitment is absent")
                })?;
            if commitment.value() != blake3::hash(root).as_bytes() {
                return Err(self.content_identity_corruption(
                    "custody config commitment disagrees with its root",
                ));
            }
            let mut entries = Vec::new();
            let mut count = 0_u64;
            let mut xor = [0_u8; 32];
            let mut projection = content_identity::SetCommitment::empty(
                content_identity::CompactIndexSet::CustodyIndex,
            );
            for entry in table.iter().map_err(RedbPersistence::storage_error)? {
                let (key, value) = entry.map_err(RedbPersistence::storage_error)?;
                if key.value() == READ_OPEN_CONFIG_ROOT_KEY {
                    continue;
                }
                if !crate::custody::records::owned_key(key.value()) {
                    return Err(self.content_identity_corruption(
                        "custody config index contains an unrelated key",
                    ));
                }
                let mut hasher = blake3::Hasher::new();
                hasher.update(key.value().as_bytes());
                hasher.update(value.value());
                let witness = hasher.finalize();
                for (dst, src) in xor.iter_mut().zip(witness.as_bytes()) {
                    *dst ^= src;
                }
                count = count.saturating_add(1);
                projection.insert(key.value().as_bytes(), witness.as_bytes())?;
                self.authenticate_mutation(
                    &content_identity::config_mutations(key.value(), Some(value.value()))[0],
                )?;
                self._source_memory
                    .memory
                    .reserve(key.value().len().saturating_add(value.value().len()))?;
                entries.push((key.value().to_owned(), value.value().to_vec()));
            }
            if root[..8] != count.to_be_bytes() || root[8..] != xor {
                return Err(self.content_identity_corruption(
                    "custody config index aggregate disagrees with its values",
                ));
            }
            self.content_identity
                .verify_custody_config_projection(projection.root())?;
            Ok(entries)
        })
    }

    /// Visit this image's lineage records whose key starts with `prefix`, in
    /// key order, read on request from the pinned snapshot. Before each record
    /// is copied for authentication, the projection's checkpoint runs and the
    /// copy's bytes are admitted through it (released once that record is done).
    /// Every record is authenticated, and the complete set is checked against
    /// the commitment published on the committed Configuration root before
    /// this returns, so a dropped, altered or added record refuses the answer
    /// instead of shortening it.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn visit_image_lineage_config(
        &self,
        prefix: &str,
        projection: &crate::database::ChangeProjection<'_>,
        visit: &mut dyn FnMut(&str, &[u8]) -> Result<()>,
    ) -> Result<()> {
        self.with_read(|read| {
            let table = read
                .open_table(READ_LINEAGE_CONFIG_TABLE)
                .map_err(RedbPersistence::storage_error)?;
            let root = table
                .get(READ_OPEN_CONFIG_ROOT_KEY)
                .map_err(RedbPersistence::storage_error)?
                .ok_or_else(|| self.content_identity_corruption("lineage config root is absent"))?;
            let root = root.value().to_vec();
            if root.len() != 40 {
                return Err(
                    self.content_identity_corruption("lineage config root has invalid size")
                );
            }
            let metadata = read
                .open_table(CONTENT_IDENTITY_METADATA_TABLE)
                .map_err(RedbPersistence::storage_error)?;
            let commitment = metadata
                .get(READ_LINEAGE_CONFIG_COMMITMENT_KEY)
                .map_err(RedbPersistence::storage_error)?
                .ok_or_else(|| {
                    self.content_identity_corruption("lineage config commitment is absent")
                })?;
            if commitment.value() != blake3::hash(&root).as_bytes() {
                return Err(self.content_identity_corruption(
                    "lineage config commitment disagrees with its root",
                ));
            }
            let mut count = 0_u64;
            let mut xor = [0_u8; 32];
            let mut set = content_identity::SetCommitment::empty(
                content_identity::CompactIndexSet::LineageIndex,
            );
            for entry in table.iter().map_err(RedbPersistence::storage_error)? {
                let (key, value) = entry.map_err(RedbPersistence::storage_error)?;
                if key.value() == READ_OPEN_CONFIG_ROOT_KEY {
                    continue;
                }
                (projection.checkpoint)()?;
                if !read_image_lineage_config_key(key.value()) {
                    return Err(self.content_identity_corruption(
                        "lineage config index contains an unrelated key",
                    ));
                }
                let mut hasher = blake3::Hasher::new();
                hasher.update(key.value().as_bytes());
                hasher.update(value.value());
                let witness = hasher.finalize();
                for (dst, src) in xor.iter_mut().zip(witness.as_bytes()) {
                    *dst ^= src;
                }
                count = count.saturating_add(1);
                set.insert(key.value().as_bytes(), witness.as_bytes())?;
                // The authentication mutation owns copies of the key and the
                // value; their bytes are admitted before either is made.
                let scratch = key
                    .value()
                    .len()
                    .saturating_add(value.value().len())
                    .saturating_mul(2);
                #[cfg(any(test, feature = "test-seams"))]
                if LINEAGE_SCRATCH_REFUSAL.swap(false, std::sync::atomic::Ordering::SeqCst) {
                    return Err(Error::MemoryBudgetExceeded {
                        subsystem: "lineage_config".into(),
                        operation: "authenticate".into(),
                        requested_bytes: scratch,
                        available_bytes: 0,
                        budget_limit_bytes: 0,
                        hint:
                            "Raise the reader's memory limit to authenticate this lineage record."
                                .into(),
                    });
                }
                (projection.admit)(scratch)?;
                let authenticated = self.authenticate_mutation(
                    &content_identity::config_mutations(key.value(), Some(value.value()))[0],
                );
                (projection.release)(scratch)?;
                authenticated?;
                if key.value().starts_with(prefix) {
                    visit(key.value(), value.value())?;
                }
            }
            if root[..8] != count.to_be_bytes() || root[8..] != xor {
                return Err(self.content_identity_corruption(
                    "lineage config index aggregate disagrees with its values",
                ));
            }
            self.content_identity
                .verify_lineage_config_projection(set.root())?;
            Ok(())
        })
    }

    pub(super) fn raw_vector_corruption(&self, reason: impl std::fmt::Display) -> Error {
        crate::funded_error::corruption_display(
            Ok(self.read_memory.clone()),
            self.database_path.display(),
            format_args!("vector partition durability record is corrupt: {reason}"),
        )
    }

    /// Keep an identity failure that already carries the typed corruption
    /// error instead of wrapping its text a second time, and keep a memory
    /// refusal a memory refusal. A proof this read had no room to walk says
    /// nothing about the bytes on disk: calling it corruption sends an
    /// operator to a restore when what they have is a ceiling to raise.
    pub(super) fn content_identity_error(&self, error: Error) -> Error {
        match error {
            Error::StoreCorrupted { .. } | Error::ReadCancelled | Error::ReadFailure(_) => error,
            error
                if crate::read_image_memory::is_memory_refusal(&error)
                    || crate::read_image_memory::is_missing_decode_owner(&error) =>
            {
                error
            }
            other => self.content_identity_corruption(other),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn verify_history_root_in_read(
        &self,
        read: &redb::ReadTransaction,
        name: &str,
    ) -> Result<()> {
        RedbPersistence::verify_history_root(read, name).map_err(|error| match error {
            Error::StoreCorrupted { reason, .. } => self.content_identity_corruption(reason),
            other => other,
        })
    }

    pub(super) fn content_identity_corruption(&self, reason: impl std::fmt::Display) -> Error {
        crate::funded_error::corruption_display(
            Ok(self.read_memory.clone()),
            self.database_path.display(),
            format_args!(
                "loaded content does not authenticate against the committed image root: {reason}"
            ),
        )
    }

    /// The identity route could not hold the authenticated run that proves its
    /// range is complete. This is the existing typed budget refusal, not a new
    /// policy: the answer is withheld because the declared budget cannot admit
    /// the correct operation, never because the data is wrong.
    pub(super) fn unprovable_ordered_range(&self, index: &str) -> Error {
        let usage = self.verified_ordered_runs.memory.accountant.usage();
        Error::MemoryBudgetExceeded {
            subsystem: ("content_identity".to_owned()).into(),
            operation: ("authenticated_index_range".to_owned()).into(),
            requested_bytes: VERIFIED_ORDERED_RUN_BUILD_MAX_BYTES,
            available_bytes: usage.available.unwrap_or(0),
            budget_limit_bytes: usage.limit.unwrap_or(0),
            hint: (format!(
                "Raise MEMORY_LIMIT so the authenticated run for index {index} can be held; \
                 an unproven range is never answered from the physical index alone."
            ))
            .into(),
        }
    }

    /// Read the change log, schema-change log and commit index from this
    /// pinned snapshot as the complete committed history, in its authored
    /// order. Each history's committed range is walked in order against the
    /// pinned root, in lockstep with the stored table: every committed member
    /// must meet the stored record at its exact position with its committed
    /// value, and the table may hold nothing else. A missing, extra or moved
    /// record -- or a missing table -- is damage, never a shorter or
    /// reordered answer. Pages, collection slots, proof scratch and record
    /// decodes are admitted against `read_memory` before acquisition. The
    /// accountant is retained only as the image decoder's bridge identity.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn load_history(
        &self,
        accountant: &crate::memory_accounting::AccountantHandle,
        checkpoint: &dyn Fn() -> Result<()>,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> std::result::Result<LoadedReadHistory, crate::database::ImageHistoryError> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        checkpoint()?;
        let mut reservation = contextdb_core::read_memory::ReadCredit::new(read_memory.clone());
        reservation.try_grow_for(
            std::mem::size_of::<LoadedReadHistory>()
                .saturating_add(std::mem::size_of::<crate::read_image_memory::ImageMemory>())
                .saturating_add(2 * std::mem::size_of::<usize>())
                .saturating_add(std::mem::size_of::<redb::ReadOnlyTable<&str, &[u8]>>())
                .saturating_add(std::mem::size_of::<redb::Range<'_, &str, &[u8]>>())
                .saturating_add(std::mem::size_of::<redb::ReadOnlyTable<u64, u64>>())
                .saturating_add(std::mem::size_of::<redb::Range<'_, u64, u64>>()),
            "reader",
            "read_history_controls",
            "Reduce the requested history or raise the declared reader memory limit.",
        )?;
        let decoded = crate::read_image_memory::ImageMemory::charging_reader(
            accountant.clone(),
            read_memory.clone(),
        );
        let diagnosis = std::cell::RefCell::new(None);
        let result = graph_read::with_pages(read_memory.clone(), || {
            self.with_read(|read| {
                self.check_history_directory_in_read(read, read_memory.clone(), checkpoint)?;
                self.history_open_control.check_ddl(read)?;
                checkpoint()?;
                let mut change_log = Vec::new();
                self.walk_stored_history::<graph_history_record::StoredChangeLogEntry>(
                    read,
                    CHANGE_LOG_TABLE,
                    OpenHistoryClass::ChangeLog,
                    read_memory.clone(),
                    &mut reservation,
                    &decoded,
                    checkpoint,
                    &|key, raw| {
                        let record = RedbPersistence::decode_stored_change_log(raw)?;
                        if let Err(error) = self.check_requested_tombstone_commit_in_read(read, &record) {
                            let engine = Error::ReadFailure(contextdb_core::read_contract::ReadFailure::new(
                                contextdb_core::read_contract::ReadFailureKind::DirectReadRequiresWriter,
                                contextdb_core::read_contract::ReadFailureDetail::Reason { reason: error.to_string() },
                            ).expect("a deferred history refusal carries its reason"));
                            diagnosis.replace(Some(error));
                            return Err(engine);
                        }
                        self.verify_stored_graph_history_in_read(
                            read,
                            key,
                            raw,
                            read_memory.clone(),
                        )?;
                        RedbPersistence::decode_stored_change_log(raw)
                    },
                    &|_, raw| RedbPersistence::stored_change_log_value_hash(raw),
                    &mut |_, entry| {
                        change_log.push(entry.entry);
                        Ok(())
                    },
                )?;
                let mut ddl_log = Vec::new();
                self.walk_stored_history::<DdlChange>(
                    read,
                    DDL_LOG_TABLE,
                    OpenHistoryClass::DdlLog,
                    read_memory.clone(),
                    &mut reservation,
                    &decoded,
                    checkpoint,
                    &|_, raw| RedbPersistence::decode(raw),
                    &|record, _| content_identity::history_record_value_hash(record),
                    &mut |key, change| {
                        let raw_lsn = key.split_once(':').map(|(lsn, _)| lsn).unwrap_or(key);
                        let lsn = raw_lsn
                            .parse::<u64>()
                            .map(Lsn)
                            .map_err(|error| self.content_identity_corruption(error))?;
                        ddl_log.push((lsn, change));
                        Ok(())
                    },
                )?;
                Ok(LoadedReadHistory {
                    change_log,
                    ddl_log,
                    charge: ReadHistoryCharge {
                        _slots: reservation,
                        _decoded: decoded,
                    },
                })
            })
        });
        result.map_err(|error| match diagnosis.into_inner() {
            Some(diagnosis) => crate::database::ImageHistoryError::store(
                self.read_memory.clone(),
                self.database_path.display(),
                diagnosis,
            ),
            None => crate::database::ImageHistoryError::Engine(error),
        })
    }

    pub(crate) fn database_path(&self) -> &Path {
        &self.database_path
    }

    pub(super) fn history_damage(&self, reason: impl std::fmt::Display) -> Error {
        self.content_identity_corruption(format!(
            "the stored history is not the committed history: {reason}"
        ))
    }

    /// The history walk could not hold the proof of its range. A typed budget
    /// refusal: the history is withheld because the declared memory cannot
    /// prove it, never because it is wrong, and never answered short.
    pub(super) fn unprovable_history(
        &self,
        accountant: &crate::memory_accounting::MemoryAccountant,
        kind: content_identity::HistoryKind,
    ) -> Error {
        let usage = accountant.usage();
        Error::MemoryBudgetExceeded {
            subsystem: ("content_identity".to_owned()).into(),
            operation: ("authenticated_history_range".to_owned()).into(),
            requested_bytes: READ_HISTORY_PROOF_POOL_BYTES,
            available_bytes: usage.available.unwrap_or(0),
            budget_limit_bytes: usage.limit.unwrap_or(0),
            hint: (format!(
                "Raise MEMORY_LIMIT so the proof of the {kind:?} history range can be held; \
                 history is never answered without its complete proof."
            ))
            .into(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn walk_stored_history<T>(
        &self,
        read: &redb::ReadTransaction,
        definition: TableDefinition<&'static str, &'static [u8]>,
        class: OpenHistoryClass,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        reservation: &mut contextdb_core::read_memory::ReadCredit,
        decoded: &crate::read_image_memory::ImageMemoryHandle,
        checkpoint: &dyn Fn() -> Result<()>,
        decode_record: &dyn Fn(&str, &[u8]) -> Result<T>,
        value_hash: &dyn Fn(&T, &[u8]) -> Result<[u8; 32]>,
        keep: &mut dyn FnMut(&str, T) -> Result<()>,
    ) -> Result<()>
    where
        T: content_identity::HistoryRecord,
    {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        const HINT: &str = "Raise MEMORY_LIMIT so this reader can hold the store's change history.";
        let kind = <T as content_identity::HistoryRecord>::KIND;
        let table = open_optional_read_table(read, definition)?;
        let mut stored = match &table {
            Some(table) => Some(
                table
                    .iter()
                    .map_err(RedbPersistence::storage_error)?
                    .inspect(|_entry| {
                        #[cfg(any(test, feature = "test-seams"))]
                        if let Ok((key, _)) = _entry {
                            crate::read_work_probe::note_graph(
                                "history",
                                "iteration_step",
                                key.value().as_bytes(),
                                None,
                                Some("complete_history"),
                            );
                        }
                    }),
            ),
            None => None,
        };
        let mut ddl_last = None;
        let _ddl_controls = if kind == content_identity::HistoryKind::DdlLog {
            Some(contextdb_core::read_memory::ReadCredit::try_new(
                read_memory.clone(),
                std::mem::size_of::<Option<(u64, Lsn, [u8; 32], [u8; 32])>>()
                    + std::mem::size_of::<blake3::Hasher>(),
                "reader",
                "ddl_complete_controls",
                "Raise the declared reader memory limit.",
            )?)
        } else {
            None
        };
        let proof_owner = ReadHistoryProofOwner::new(read_memory.clone())?;
        let proof = content_identity::ProofMemory::growable(0, proof_owner.clone());
        let complete = self.content_identity.visit_history::<Error>(
            kind,
            false,
            &proof,
            &mut || checkpoint(),
            &mut |coordinate, committed| {
                let shown = String::from_utf8_lossy(coordinate);
                let Some(entry) = stored.as_mut().and_then(Iterator::next) else {
                    return Err(self.history_damage(format_args!(
                        "committed {kind:?} record {shown} is missing from the store"
                    )));
                };
                let (key, value) = entry.map_err(RedbPersistence::storage_error)?;
                let key = key.value();
                match key.as_bytes().cmp(coordinate) {
                    std::cmp::Ordering::Less => {
                        return Err(self.history_damage(format_args!(
                            "stored {kind:?} record {key} is not in the committed history"
                        )));
                    }
                    std::cmp::Ordering::Greater => {
                        return Err(self.history_damage(format_args!(
                            "committed {kind:?} record {shown} is missing from the store"
                        )));
                    }
                    std::cmp::Ordering::Equal => {}
                }
                // The stored bytes are read in place, never copied. The slot the
                // record takes in its collection is admitted first, then every
                // allocation the decode makes is admitted as it is made, so an
                // oversized record is refused before it is built.
                let raw = value.value();
                reservation.try_grow_for(
                    key.len().saturating_add(std::mem::size_of::<T>()),
                    "direct_read",
                    "read_history",
                    HINT,
                )?;
                note_open_history(class, raw.len());
                let before = decoded.held();
                let record: T =
                    decoded
                        .scope(|| decode_record(key, raw))
                        .map_err(|error| match error {
                            Error::Other(ref reason)
                                if reason.starts_with("bincode decode error:") =>
                            {
                                self.history_damage(format_args!("{key}: {reason}"))
                            }
                            other => other,
                        })?;
                // The canonical form the committed hash is taken over is
                // transient: a value tree no larger than a few times the
                // decoded record, and its text. Admitted before it is built,
                // given back once it is hashed.
                let mut canonical =
                    contextdb_core::read_memory::ReadCredit::new(read_memory.clone());
                canonical.try_grow_for(
                    decoded
                        .held()
                        .saturating_sub(before)
                        .saturating_mul(4)
                        .saturating_add(raw.len().saturating_mul(2))
                        .saturating_add(1024),
                    "direct_read",
                    "read_history",
                    HINT,
                )?;
                // Hashing private history validates its codec again; that
                // transient decode shares this operation and releases here.
                let hash_decode = crate::read_image_memory::ImageMemory::charging_reader(
                    decoded.accountant.clone(),
                    read_memory.clone(),
                );
                let actual = hash_decode.scope(|| value_hash(&record, raw))?;
                drop(hash_decode);
                if actual != committed {
                    return Err(self.history_damage(format_args!(
                        "stored {kind:?} record {key} is not the committed record at its position"
                    )));
                }
                if kind == content_identity::HistoryKind::DdlLog {
                    ddl_last = Some((
                        u64::try_from(key.len())
                            .map_err(|_| self.history_damage("DDL coordinate length overflow"))?,
                        RedbPersistence::ddl_lsn_from_key(key)?,
                        content_identity::history_open_control::ddl_coordinate_digest(
                            key.as_bytes(),
                            Some(checkpoint),
                        )?,
                        actual,
                    ));
                }
                drop(canonical);
                keep(key, record)?;
                Ok(true)
            },
        );
        if let Some(error) = proof_owner.refusal.lock().take() {
            return Err(error);
        }
        let complete = complete?;
        if !complete {
            return Err(self.unprovable_history(&self._source_memory.memory.accountant, kind));
        }
        if let Some(entry) = stored.as_mut().and_then(Iterator::next) {
            let (key, _) = entry.map_err(RedbPersistence::storage_error)?;
            return Err(self.history_damage(format_args!(
                "stored {kind:?} record {} is not in the committed history",
                key.value()
            )));
        }
        if kind == content_identity::HistoryKind::DdlLog {
            self.history_open_control.check_complete_ddl(ddl_last)?;
        }
        checkpoint()?;
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn authenticate_mutation(
        &self,
        mutation: &content_identity::ContentIdentityMutation,
    ) -> Result<()> {
        if let Some(memory) = crate::read_image_memory::current_reader_budget() {
            self.content_identity
                .verify_with_read_memory(mutation, memory)
        } else {
            self.content_identity
                .verify(mutation)
                .map_err(|error| self.content_identity_error(error))
        }
    }

    /// The authenticated chunks this pinned snapshot is holding, their bytes,
    /// and how many of those bytes are charged to the declared session.
    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn resident_proof_chunks_for_test(&self) -> (usize, usize, usize) {
        self.content_identity.resident_proof_chunks_for_test()
    }

    /// The most this reader may hold in proved chunks.
    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn resident_proof_chunk_ceiling_for_test(&self) -> usize {
        self.content_identity
            .resident_proof_chunk_ceiling_for_test()
    }

    /// The bytes this statement's shared node/leaf proof-page set
    /// (`content_proof`, capped at `RETAINED_CONTENT_PROOF_BYTES`) is
    /// currently tracked at (first), and how many of those bytes are
    /// actually accounted for by a session charge or the reader's free floor
    /// (second) -- not necessarily anything close to the first.
    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn retained_content_proof_bytes_for_test(&self) -> (usize, usize) {
        let proof = self
            .content_proof
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        proof.retained_bytes_and_accounted(|node| {
            self.content_identity.resident_still_holds_for_test(node)
        })
    }

    /// The bytes this statement's shared proof-page set is charged for
    /// directly through its own admission, separate from whatever the
    /// resident set charges or floor-covers for the same decoded pages.
    /// Released together with the resident set's own charge when both are
    /// cleared (`evict_resident_proof_chunks_for_test`).
    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn statement_own_charge_for_test(&self) -> usize {
        let proof = self
            .content_proof
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        proof.own_charge_for_test()
    }

    /// Drop every retained authenticated chunk, as capacity eviction does. The
    /// next statement proves them against the committed root again.
    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn evict_resident_proof_chunks_for_test(&self) -> usize {
        let evicted = self.content_identity.evict_resident_proof_chunks_for_test();
        self.clear_statement_proofs();
        evicted
    }

    /// Drop every resident proof chunk WITHOUT resetting this statement's own
    /// retained proof-page set, so a page this statement copied out of
    /// residency stays reachable at exactly its pre-eviction bytes while the
    /// resident set's charge for it disappears. `evict_resident_proof_chunks_for_test`
    /// above resets both together, which a genuine forced-eviction-while-held
    /// reproduction cannot use.
    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn evict_resident_proof_chunks_only_for_test(&self) -> usize {
        self.content_identity.evict_resident_proof_chunks_for_test()
    }

    /// Give back this statement's shared leaf pages. Row and exact-body
    /// reads of one statement reuse them; a later statement, a forced
    /// window eviction, or open's startup clear starts from the resident
    /// chunk cache instead.
    pub(crate) fn clear_statement_proofs(&self) {
        let mut proof = self
            .content_proof
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *proof = content_identity::ReadAuthenticationNodes::new(
            RETAINED_CONTENT_PROOF_BYTES,
            self.content_identity.admission_accountant(),
        );
    }

    /// Drop the remembered result-record witnesses only.
    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn evict_record_witnesses_for_test(&self) -> usize {
        self.content_identity.evict_record_witnesses_for_test()
    }

    /// How many distinct prefix values this table currently stores a
    /// maintained count for, over exactly these columns. Inspection only: it
    /// counts records, never rows.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn stored_maintained_count_values(
        &self,
        table: &str,
        columns: &[(String, contextdb_core::SortDirection)],
    ) -> Result<u64> {
        let _decode_scope = self.verified_ordered_runs.memory.enter();
        self.with_read(|read| {
            let counts = match read.open_table(RELATIONAL_INDEX_COUNT_TABLE) {
                Ok(counts) => counts,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(0),
                Err(error) => return Err(RedbPersistence::storage_error(error)),
            };
            let mut stored = 0u64;
            for entry in counts.iter().map_err(RedbPersistence::storage_error)? {
                let (key, _) = entry.map_err(RedbPersistence::storage_error)?;
                let decoded: EncodedRelationalIndexCountKey = RedbPersistence::decode(key.value())?;
                if decoded.table == table && decoded.columns == columns {
                    stored = stored.saturating_add(1);
                }
            }
            Ok(stored)
        })
    }

    /// Enumerate current groups from authenticated ordered postings. Each
    /// descent stops on the first member, then seeks to the upper edge of that
    /// group's encoded leading prefix. Its exact count is separately proved by
    /// point lookup; no posting population is scanned or counted.
    #[allow(clippy::too_many_arguments)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn committed_maintained_count_groups<E: From<Error>>(
        &self,
        table: &str,
        index: &str,
        columns: &[(String, contextdb_core::SortDirection)],
        prefix: &[DirectedValue],
        snapshot: SnapshotId,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        before_touch: &mut dyn FnMut() -> std::result::Result<(), E>,
        validate_position: &mut dyn FnMut(
            &RelationalIndexPosition,
        ) -> std::result::Result<(), Error>,
        visit: &mut dyn FnMut(IndexKey, u64) -> std::result::Result<(), E>,
    ) -> std::result::Result<bool, E> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if !self.ordered_prefix_snapshot_is_current(snapshot) {
            return Ok(false);
        }
        if prefix.len() > columns.len() {
            return Err(E::from(Error::Other(
                "count prefix exceeds column list".into(),
            )));
        }
        let make_memory = || {
            crate::read_image_memory::ImageMemory::charging_reader_for(
                self._source_memory.memory.accountant.clone(),
                read_memory.clone(),
                "committed_count",
                "authenticated_groups",
                "Raise the declared reader memory limit.",
            )
        };
        let growth = Arc::new(CommittedCountProofMemory {
            memory: make_memory(),
            refusal: Mutex::new(None),
        });
        let proof = content_identity::ProofMemory::growable(
            0,
            growth.clone() as Arc<dyn content_identity::ProofPoolGrowth>,
        );
        let mut batch = content_identity::OrderedProofBatch::default();
        let memory = make_memory();
        let encoded = charged_count_group_boundary(prefix, &memory).map_err(E::from)?;
        let mut boundary = (encoded, memory);
        let mut after_group = false;
        // The readiness probe belongs to the first seek, including an empty
        // table. It cannot emit a group before the declared image is ready.
        before_touch()?;
        boundary
            .1
            .reserve(
                table
                    .len()
                    .saturating_add(index.len())
                    .saturating_mul(2)
                    .saturating_add(256),
            )
            .map_err(E::from)?;
        let ready = self
            .with_read(|read| {
                self.verify_history_root_in_read(read, &RedbPersistence::rel_table_name(table))?;
                self.verify_history_root_in_read(
                    read,
                    &RedbPersistence::relational_index_table_name(table, index),
                )?;
                let ready = match read.open_table(RELATIONAL_INDEX_READY_TABLE) {
                    Ok(ready) => ready,
                    Err(redb::TableError::TableDoesNotExist(_)) => return Ok(false),
                    Err(error) => return Err(RedbPersistence::storage_error(error)),
                };
                Ok(ready
                    .get(table)
                    .map_err(RedbPersistence::storage_error)?
                    .is_some_and(|version| version.value() == RELATIONAL_INDEX_READY_VERSION))
            })
            .map_err(E::from)?;
        if !ready {
            return Ok(false);
        }
        self.authenticate_mutation(&content_identity::status_mutation(
            RELATIONAL_INDEX_COUNT_READY_IDENTITY_NAMESPACE,
            table.as_bytes(),
            RELATIONAL_INDEX_READY_VERSION,
        ))
        .map_err(E::from)?;
        loop {
            let decoded_memory = make_memory();
            let mut position = None;
            self.content_identity
                .visit_relational_index_group_first_in::<E>(
                    table,
                    index,
                    &boundary.0,
                    after_group,
                    &proof,
                    &mut batch,
                    &mut |bytes| {
                        // Fund the borrowed key's two concrete owned images before
                        // copying: the durable key and its canonical re-encoding.
                        decoded_memory
                            .reserve(bytes.len().saturating_mul(2))
                            .map_err(E::from)?;
                        position = Some(
                            decoded_memory
                                .scope(|| {
                                    DurableRelationalIndexKey(bytes.to_vec()).decode_canonical()
                                })
                                .map_err(E::from)?,
                        );
                        Ok(())
                    },
                )?;
            if proof.exhausted() {
                let error = growth
                    .refusal
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .take()
                    .unwrap_or_else(|| Error::Other("committed count proof exhausted".into()));
                return Err(E::from(error));
            }
            let Some(mut position) = position else {
                return Ok(true);
            };
            validate_position(&position).map_err(E::from)?;
            if position.created_tx != TxId(0) {
                return Err(E::from(self.content_identity_corruption(
                    "logical count posting has transaction",
                )));
            }
            if !position.key.starts_with(prefix) {
                return Ok(true);
            }
            position.key.truncate(columns.len());
            let next =
                charged_count_group_boundary(&position.key, &decoded_memory).map_err(E::from)?;
            before_touch()?;
            let count = self
                .authenticated_relational_index_prefix_count(
                    table,
                    index,
                    columns,
                    &position.key,
                    read_memory.clone(),
                )
                .map_err(E::from)?
                .filter(|count| *count > 0)
                .ok_or_else(|| {
                    E::from(self.content_identity_corruption(
                        "a live maintained group has an absent or zero authenticated count",
                    ))
                })?;
            visit(position.key, count)?;
            boundary = (next, decoded_memory);
            after_group = true;
            before_touch()?;
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn authenticated_relational_index_prefix_count(
        &self,
        table: &str,
        index: &str,
        columns: &[(String, contextdb_core::SortDirection)],
        prefix: &IndexKey,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<u64>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.verify_relational_source_roots(
            table,
            index,
            IndexRootCheck::Canonical,
            read_memory.clone(),
        )?;
        let memory = crate::read_image_memory::ImageMemory::charging_reader(
            self._source_memory.memory.accountant.clone(),
            read_memory,
        );
        memory.scope(|| {
            let key = RedbPersistence::relational_index_count_key(table, columns, prefix)?;
            let count = self.with_read(|read| {
                let ready = match read.open_table(RELATIONAL_INDEX_READY_TABLE) {
                    Ok(ready) => ready,
                    Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
                    Err(error) => return Err(RedbPersistence::storage_error(error)),
                };
                let Some(version) = ready.get(table).map_err(RedbPersistence::storage_error)?
                else {
                    return Ok(None);
                };
                if version.value() != RELATIONAL_INDEX_READY_VERSION {
                    return Ok(None);
                }
                let counts = read
                    .open_table(RELATIONAL_INDEX_COUNT_TABLE)
                    .map_err(|error| match error {
                        redb::TableError::TableDoesNotExist(_) => self.content_identity_corruption(
                            "authenticated relational index counts are absent",
                        ),
                        other => RedbPersistence::storage_error(other),
                    })?;
                counts
                    .get(key.as_slice())
                    .map_err(RedbPersistence::storage_error)
                    .map(|count| Some(count.map(|count| count.value())))
            })?;
            let Some(count) = count else {
                return Ok(None);
            };
            self.authenticate_mutation(&content_identity::status_mutation(
                RELATIONAL_INDEX_COUNT_READY_IDENTITY_NAMESPACE,
                table.as_bytes(),
                RELATIONAL_INDEX_READY_VERSION,
            ))?;
            match count {
                Some(count) => {
                    self.authenticate_mutation(&RedbPersistence::relational_index_count_mutation(
                        &key, count,
                    ))?;
                    Ok(Some(count))
                }
                None => {
                    self.authenticate_mutation(
                        &RedbPersistence::remove_relational_index_count_mutation(&key),
                    )?;
                    // An absent record is the absence of a SHORTCUT, never the
                    // number zero. Since the default is NONE, most prefixes have
                    // no record at all, and
                    // reading absence as zero answered `COUNT(*)` with 0 over a
                    // prefix holding rows. A declared list whose count reaches
                    // zero also has its record removed, so absence cannot tell
                    // the two apart here; the caller falls through to the
                    // ordinary index-range aggregate, which counts the live
                    // postings and returns the true number either way.
                    Ok(None)
                }
            }
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn authenticate_selected_vector(
        &self,
        index: &VectorIndexRef,
        row_id: RowId,
        vector: &[f32],
        quantization: VectorQuantization,
    ) -> Result<()> {
        self.authenticate_mutation(&content_identity::decoded_vector_value_mutation(
            index,
            row_id,
            vector,
            quantization,
        ))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn authenticate_version_bounds(
        &self,
        table: &str,
        lsn: Lsn,
        created: TxId,
        deleted: Option<TxId>,
    ) -> Result<()> {
        self.with_read(|read| {
            validate_read_version_bounds(
                read,
                table,
                lsn,
                created,
                deleted,
                TxId(self.logical_frontier.0),
                self.graph_lsn_frontier,
            )
            .map_err(|error| {
                crate::database::ImageHistoryError::store(
                    self.read_memory.clone(),
                    self.database_path.display(),
                    error,
                )
                .into_engine_error()
            })
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn authenticate_relational_row(
        &self,
        table: &str,
        meta: &TableMeta,
        row: &VersionedRow,
    ) -> Result<bool> {
        let mut proof = self
            .content_proof
            .lock()
            .map_err(|_| self.content_identity_corruption("content proof lock poisoned"))?;
        self.authenticate_relational_row_with_nodes(table, meta, row, &mut proof)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn authenticate_relational_row_with_nodes(
        &self,
        table: &str,
        meta: &TableMeta,
        row: &VersionedRow,
        proof: &mut content_identity::ReadAuthenticationNodes,
    ) -> Result<bool> {
        // Authentication owns only temporary canonical evidence. Its invoice
        // must end with that evidence, before the decoded row's credit moves.
        let memory = crate::read_image_memory::active_owner()?.row_owner()?;
        let mutation = memory
            .scope(|| content_identity::admitted_row_mutation(table, row, Some(meta)))
            .map_err(|error| self.content_identity_error(error))?;
        self.content_identity
            .verify_with_nodes(proof, &mutation)
            .map_err(|error| self.content_identity_error(error))?;
        if row.deleted_tx.is_some() {
            return Ok(false);
        }
        for column in &meta.columns {
            let contextdb_core::ColumnType::Vector(dimension) = column.column_type else {
                continue;
            };
            if !matches!(column.quantization, VectorQuantization::F32) {
                continue;
            }
            let index = VectorIndexRef::new(table, &column.name);
            match row.values.get(&column.name) {
                Some(Value::Vector(vector)) => {
                    if vector.len() != dimension {
                        return Err(self.content_identity_corruption(format!(
                            "{table}.{} row {} has vector dimension {} instead of {dimension}",
                            column.name,
                            row.row_id.0,
                            vector.len()
                        )));
                    }
                    self.content_identity
                        .verify_with_nodes(
                            proof,
                            &content_identity::decoded_vector_value_mutation(
                                &index,
                                row.row_id,
                                vector,
                                column.quantization,
                            ),
                        )
                        .map_err(|error| self.content_identity_error(error))?;
                }
                Some(Value::Null) | None if column.nullable => {
                    self.content_identity
                        .verify_with_nodes(
                            proof,
                            &content_identity::remove_vector_mutation(
                                &index,
                                row.row_id,
                                column.quantization,
                            ),
                        )
                        .map_err(|error| self.content_identity_error(error))?;
                }
                Some(Value::Null) | None => {
                    return Err(self.content_identity_corruption(format!(
                        "{table}.{} row {} is NULL although the vector column is required",
                        column.name, row.row_id.0
                    )));
                }
                Some(_) => {
                    return Err(self.content_identity_corruption(format!(
                        "{table}.{} row {} contains a non-vector value",
                        column.name, row.row_id.0
                    )));
                }
            }
        }
        Ok(true)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn authenticate_relational_physical_key(
        &self,
        table: &str,
        key: &[u8],
        row: &VersionedRow,
    ) -> Result<()> {
        if RedbPersistence::rel_row_key_from_parts(row.row_id, row.created_tx, row.lsn) != key {
            return Err(self.content_identity_corruption(format!(
                "relational table {table} row body disagrees with its physical key"
            )));
        }
        self.authenticate_version_bounds(table, row.lsn, row.created_tx, row.deleted_tx)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn authenticate_vector_entry(
        &self,
        entry: &VectorEntry,
        quantization: VectorQuantization,
    ) -> Result<()> {
        self.authenticate_version_bounds(
            &entry.index.table,
            entry.lsn,
            entry.created_tx,
            entry.deleted_tx,
        )?;
        self.authenticate_mutation(&content_identity::decoded_vector_version_mutation(
            entry,
            quantization,
        ))?;
        if entry.deleted_tx.is_none() {
            self.authenticate_mutation(&content_identity::decoded_vector_mutation(
                entry,
                quantization,
            ))?;
        }
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn load_raw_vector_entries_for_memberships(
        &self,
        memberships: &[VectorPartitionMembershipRecord],
        expected_dimension: usize,
        quantization: VectorQuantization,
    ) -> Result<Vec<VectorEntry>> {
        if memberships.is_empty() {
            return Ok(Vec::new());
        }
        let captured = vector_membership_source::operation_read_for_retained(self);
        let load = |read: &redb::ReadTransaction| {
            self.load_raw_vector_entries_for_memberships_in_read(
                read,
                memberships,
                expected_dimension,
                quantization,
            )
        };
        match captured {
            Some(read) => load(&read),
            None => self.with_read(load),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn load_raw_vector_entries_for_memberships_in_read(
        &self,
        read: &redb::ReadTransaction,
        memberships: &[VectorPartitionMembershipRecord],
        expected_dimension: usize,
        quantization: VectorQuantization,
    ) -> Result<Vec<VectorEntry>> {
        self.load_raw_vector_bodies_for_memberships_in_read(
            read,
            memberships,
            expected_dimension,
            quantization,
        )
        .map(|entries| entries.into_iter().map(|(entry, _)| entry).collect())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn load_raw_vector_bodies_for_memberships_in_read(
        &self,
        read: &redb::ReadTransaction,
        memberships: &[VectorPartitionMembershipRecord],
        expected_dimension: usize,
        quantization: VectorQuantization,
    ) -> Result<Vec<(VectorEntry, Option<TxId>)>> {
        #[cfg(any(test, feature = "test-seams"))]
        crate::writer_query_test_seams::note_physical_source(
            crate::writer_query_test_seams::PhysicalFamily::Vector,
            read,
        );
        let table = read
            .open_table(VECTORS_TABLE)
            .map_err(|error| match error {
                redb::TableError::TableDoesNotExist(_) => self.raw_vector_corruption(
                    "a non-empty raw-vector directory has no vector body table",
                ),
                other => RedbPersistence::storage_error(other),
            })?;
        let mut entries = Vec::with_capacity(memberships.len());
        for membership in memberships {
            vector_membership_source::operation_checkpoint()?;
            vector_membership_source::authenticate_retained_historical_body(
                self,
                read,
                None,
                membership,
                expected_dimension,
                quantization,
            )?;
            let key = RedbPersistence::vector_identity_key(
                &membership.index,
                membership.row_id,
                membership.vector_created_tx,
                membership.vector_lsn,
            );
            let stored = table
                .get(key.as_slice())
                .map_err(RedbPersistence::storage_error)?
                .ok_or_else(|| {
                    self.raw_vector_corruption(
                        "a raw-vector directory identity has no durable body",
                    )
                })?;
            #[cfg(any(test, feature = "test-seams"))]
            crate::read_work_probe::note_decoded_vector();
            contextdb_core::find_attribution::count("decoded_vectors", 1);
            let mut entry = decode_retained_raw_vector_entry(
                stored.value(),
                membership,
                expected_dimension,
                |reason| self.raw_vector_corruption(reason),
            )?;
            if !vector_membership_source::authenticate_retained_historical_body(
                self,
                read,
                Some(&entry),
                membership,
                expected_dimension,
                quantization,
            )? {
                self.authenticate_vector_entry(&entry, quantization)?;
            }
            let canonical_deleted_tx = entry.deleted_tx;
            entry.deleted_tx = membership.deleted_tx;
            entries.push((entry, canonical_deleted_tx));
        }
        Ok(entries)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn stream_raw_vector_memberships<T>(
        &self,
        partition: &VectorPartitionRef,
        dimension: usize,
        quantization: VectorQuantization,
        prepare: impl FnOnce(usize) -> Result<T>,
        mut visit: impl FnMut(
            &mut T,
            VectorPartitionMembershipRecord,
            &mut dyn FnMut(&VectorPartitionMembershipRecord) -> Result<(VectorEntry, Option<TxId>)>,
        ) -> Result<()>,
    ) -> Result<T> {
        let captured = vector_membership_source::operation_read_for_retained(self);
        let stream = |read: &redb::ReadTransaction| {
            // Memberships and bodies share this pinned root. The callback must
            // never reacquire persistence ownership while close/recycle waits.
            let mut load_body = |membership: &VectorPartitionMembershipRecord| {
                let mut entries = self.load_raw_vector_bodies_for_memberships_in_read(
                    read,
                    std::slice::from_ref(membership),
                    dimension,
                    quantization,
                )?;
                Ok(entries.pop().expect("one membership loads one body"))
            };
            stream_raw_partition_memberships(
                read,
                partition,
                prepare,
                |state, membership| visit(state, membership, &mut load_body),
                |reason| self.raw_vector_corruption(reason),
            )
        };
        match captured {
            Some(read) => stream(&read),
            None => self.with_read(stream),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn load_raw_vector_entry_for_partition_identity(
        &self,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        identity: &RawVectorDirectoryEntry,
        expected_dimension: usize,
        quantization: VectorQuantization,
    ) -> Result<VectorEntry> {
        self.with_read(|read| {
            let memberships = read.open_table(VECTOR_PARTITION_MEMBERSHIP_TABLE).map_err(
                |error| match error {
                    redb::TableError::TableDoesNotExist(_) => self.raw_vector_corruption(
                        "a raw-vector candidate has no partition membership table",
                    ),
                    other => RedbPersistence::storage_error(other),
                },
            )?;
            self.raw_vector_candidate_membership(&memberships, index, partition_key, identity)?;
            let vectors = read
                .open_table(VECTORS_TABLE)
                .map_err(|error| match error {
                    redb::TableError::TableDoesNotExist(_) => self.raw_vector_corruption(
                        "a raw-vector candidate membership has no vector body table",
                    ),
                    other => RedbPersistence::storage_error(other),
                })?;
            let mut entry =
                self.raw_vector_candidate_body(&vectors, index, identity, expected_dimension)?;
            self.authenticate_vector_entry(&entry, quantization)?;
            entry.deleted_tx = identity.deleted_tx;
            Ok(entry)
        })
    }

    /// Open one read of stored vector bodies that a statement reuses for every
    /// body it compares: one transaction pinned at this source's snapshot and
    /// the body table opened once. Authentication pages belong to that pinned
    /// source, so this scan reuses proofs from the preceding row walk.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn open_raw_vector_body_scan(&self) -> Result<RetainedRawVectorBodyScan> {
        let owner = vector_membership_source::MembershipOwner::Retained(self.clone());
        let read = match vector_membership_source::operation_read(&owner) {
            Some(read) => read,
            None => Arc::new(self.begin_read()?),
        };
        self.open_raw_vector_body_scan_at(read)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn open_raw_vector_body_scan_at(
        &self,
        transaction: Arc<redb::ReadTransaction>,
    ) -> Result<RetainedRawVectorBodyScan> {
        let owner = vector_membership_source::MembershipOwner::Retained(self.clone());
        let memory = vector_membership_source::historical_operation_memory(&owner, &transaction)?;
        let open = || {
            vector_membership_source::operation_checkpoint()?;
            if transaction.storage_snapshot_identity() != self.storage_identity() {
                return Err(
                    self.raw_vector_corruption("raw body read escaped its retained physical root")
                );
            }
            vector_membership_pages::validate_in_read(&transaction)?;
            let producer_body_root = vector_scan_body_root_matches(&transaction)?;

            let vectors = match transaction.open_table(VECTORS_TABLE) {
                Ok(table) => Some(table),
                Err(redb::TableError::TableDoesNotExist(_)) => None,
                Err(error) => return Err(vector_membership_pages::metadata_error(error)),
            };
            Ok(RetainedRawVectorBodyScan {
                producer_body_root,
                selected_stream_identity: None,
                source: self.clone(),

                vectors,
                _transaction: transaction,
            })
        };
        match memory {
            Some(memory) => graph_read::with_pages(memory, open),
            None => open(),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn raw_vector_candidate_membership(
        &self,
        memberships: &redb::ReadOnlyTable<&'static [u8], &'static [u8]>,
        index: &VectorIndexRef,
        partition_key: &VectorPartitionKey,
        identity: &RawVectorDirectoryEntry,
    ) -> Result<VectorPartitionMembershipRecord> {
        let row_prefix = RedbPersistence::vector_partition_membership_row_prefix(
            index,
            partition_key,
            identity.row_id,
        );
        let mut matched = None;
        let mut saw_vector_identity = false;
        for item in memberships
            .prefix_bounded(&row_prefix)
            .map_err(RedbPersistence::storage_error)?
        {
            vector_membership_source::operation_checkpoint()?;
            let (key, value) = item.map_err(RedbPersistence::storage_error)?;
            if !key.value().starts_with(&row_prefix) {
                break;
            }
            let membership: VectorPartitionMembershipRecord =
                decode_vector_partition_record_payload(
                    VectorPartitionRecordKind::Membership,
                    value.value(),
                )
                .map_err(|reason| self.raw_vector_corruption(reason))?;
            if RedbPersistence::vector_partition_membership_key(&membership).as_slice()
                != key.value()
            {
                return Err(self.raw_vector_corruption(
                    "a raw-vector candidate membership disagrees with its point key",
                ));
            }
            if membership.index != *index || membership.partition_key != *partition_key {
                return Err(self.raw_vector_corruption(
                    "a raw-vector candidate membership disagrees with its point key",
                ));
            }
            if membership.vector_created_tx != identity.created_tx
                || membership.vector_lsn != identity.lsn
            {
                continue;
            }
            saw_vector_identity = true;
            if membership.deleted_tx != identity.deleted_tx {
                continue;
            }
            if vector_membership_source::body_identity_for_record(&membership) != *identity {
                return Err(vector_membership_source::damage(
                    "candidate provenance differs from its captured membership",
                ));
            }
            if membership.visible_from
                < TxId(
                    membership
                        .row_created_tx
                        .0
                        .max(membership.vector_created_tx.0),
                )
                || membership
                    .deleted_tx
                    .is_some_and(|deleted| deleted.0 <= membership.visible_from.0)
            {
                return Err(self.raw_vector_corruption(
                    "a raw-vector candidate membership has an invalid lifecycle boundary",
                ));
            }
            if matched.replace(membership).is_some() {
                return Err(self.raw_vector_corruption(
                    "a raw-vector candidate identity has duplicate partition memberships",
                ));
            }
        }
        matched.ok_or_else(|| {
            self.raw_vector_corruption(if saw_vector_identity {
                "a raw-vector candidate deletion transaction changed outside its directory"
            } else {
                "a raw-vector candidate identity has no durable partition membership"
            })
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn raw_vector_candidate_body(
        &self,
        vectors: &redb::ReadOnlyTable<&'static [u8], &'static [u8]>,
        index: &VectorIndexRef,
        identity: &RawVectorDirectoryEntry,
        expected_dimension: usize,
    ) -> Result<VectorEntry> {
        let key = RedbPersistence::vector_identity_key(
            index,
            identity.row_id,
            identity.created_tx,
            identity.lsn,
        );
        let stored = vectors
            .get_bounded(key.as_slice())
            .map_err(vector_membership_pages::metadata_error)?
            .ok_or_else(|| {
                self.raw_vector_corruption("a raw-vector candidate identity has no durable body")
            })?;
        #[cfg(any(test, feature = "test-seams"))]
        crate::read_work_probe::note_decoded_vector();
        contextdb_core::find_attribution::count("decoded_vectors", 1);
        validate_raw_vector_entry_encoded_width(
            stored.value(),
            index,
            expected_dimension,
            |reason| self.raw_vector_corruption(reason),
        )?;
        let entry = RedbPersistence::decode_vector_entry(stored.value()).map_err(|error| {
            if preserves_typed_read_refusal(&error) {
                return error;
            }
            self.raw_vector_corruption(format!(
                "a raw-vector candidate body cannot be decoded: {error}"
            ))
        })?;
        self.authenticate_version_bounds(
            &entry.index.table,
            entry.lsn,
            entry.created_tx,
            identity.deleted_tx,
        )?;
        if entry.index != *index
            || entry.row_id != identity.row_id
            || entry.created_tx != identity.created_tx
            || entry.lsn != identity.lsn
        {
            return Err(self.raw_vector_corruption(
                "a raw-vector candidate body disagrees with its membership identity",
            ));
        }
        if entry.vector.len() != expected_dimension {
            return Err(self.raw_vector_corruption(format!(
                "raw-vector candidate dimension {} does not match declared dimension {expected_dimension}",
                entry.vector.len()
            )));
        }
        Ok(entry)
    }

    /// The private history commitment includes its durable deletion witness,
    /// which the public change projection intentionally does not carry.
    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn change_history_bodies_for_test(&self) -> Result<Vec<(String, Vec<u8>)>> {
        self.with_read(|read| {
            let table = match read.open_table(CHANGE_LOG_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                Err(error) => return Err(RedbPersistence::storage_error(error)),
            };
            let mut bodies = Vec::new();
            for entry in table.iter().map_err(RedbPersistence::storage_error)? {
                let (key, value) = entry.map_err(RedbPersistence::storage_error)?;
                bodies.push((key.value().to_owned(), value.value().to_vec()));
            }
            Ok(bodies)
        })
    }

    /// Disposable oracle input: physical row bodies from the pinned root,
    /// independent of the maintained identity records and vector projection.
    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn relational_version_bodies_for_test(
        &self,
        table_meta: &HashMap<String, TableMeta>,
    ) -> Result<Vec<(String, Vec<u8>)>> {
        self.with_read(|read| {
            let mut bodies = Vec::new();
            for name in table_meta.keys() {
                let table_name = RedbPersistence::rel_table_name(name);
                let definition: TableDefinition<&[u8], &[u8]> =
                    TableDefinition::new(table_name.as_str());
                let table = match read.open_table(definition) {
                    Ok(table) => table,
                    Err(redb::TableError::TableDoesNotExist(_)) => continue,
                    Err(error) => return Err(RedbPersistence::storage_error(error)),
                };
                for entry in table.iter().map_err(RedbPersistence::storage_error)? {
                    let (_, value) = entry.map_err(RedbPersistence::storage_error)?;
                    bodies.push((name.clone(), value.value().to_vec()));
                }
            }
            Ok(bodies)
        })
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub(super) fn compatibility_vector_projection_for_test(
        &self,
        table_meta: &HashMap<String, TableMeta>,
        ddl_log: &[(Lsn, DdlChange)],
    ) -> Result<(
        HashMap<String, Vec<VersionedRow>>,
        Vec<VectorEntry>,
        HashMap<(VectorIndexRef, RowId), Vec<f32>>,
    )> {
        self.with_read(|read| {
            let mut relational_tables = HashMap::new();
            let mut names = table_meta.keys().cloned().collect::<Vec<_>>();
            names.sort();
            for name in names {
                let meta = table_meta.get(&name).ok_or_else(|| {
                    Error::Other((format!("test projection lost table metadata for {name}")).into())
                })?;
                relational_tables.insert(
                    name.clone(),
                    decode_read_relational_table(read, &name, meta)
                        .map_err(|error| Error::Other((error.to_string()).into()))?,
                );
            }
            let vectors = decode_read_vectors(read)
                .map_err(|error| Error::Other((error.to_string()).into()))?;
            let current = validate_and_collect_current_vectors(
                table_meta,
                &mut relational_tables,
                &vectors,
                ddl_log,
            )
            .map_err(|error| Error::Other((error.to_string()).into()))?;
            Ok((relational_tables, vectors, current))
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn row_by_id_in_read(
        &self,
        read: &redb::ReadTransaction,
        table: &str,
        meta: &TableMeta,
        row_id: RowId,
        snapshot: SnapshotId,
    ) -> Result<Option<(VersionedRow, crate::read_image_memory::ImageMemoryHandle)>> {
        let _decode_scope = crate::read_image_memory::ImageMemory::enter_decode_call()?;
        self.reconciled_row_with_owner(
            read,
            table,
            meta,
            row_id,
            snapshot,
            self.read_memory.clone(),
            true,
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn reconciled_row_with_owner(
        &self,
        read: &redb::ReadTransaction,
        table: &str,
        meta: &TableMeta,
        row_id: RowId,
        snapshot: SnapshotId,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        addressed: bool,
    ) -> Result<Option<(VersionedRow, crate::read_image_memory::ImageMemoryHandle)>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let memory = crate::read_image_memory::ImageMemory::operation_reader(
            self._source_memory.memory.accountant.clone(),
            read_memory.clone(),
        )?;
        let mut credit = contextdb_core::read_memory::ReadCredit::new(read_memory.clone());
        let mut last = None;
        let mut adopt = |admission: crate::read_image_memory::RowReadAdmission<'_>| {
            admission.retain_into(
                &mut credit,
                "reader",
                "selected_row",
                "Raise the declared reader memory limit.",
            )
        };
        let row = if addressed {
            graph_read::with_pages(read_memory.clone(), || {
                self.reconcile_addressed_relational_group::<Error>(
                    read,
                    table,
                    meta,
                    row_id,
                    snapshot,
                    None,
                    read_memory.clone(),
                    &mut || Ok(()),
                    None,
                    &mut adopt,
                    &mut last,
                )
            })?
        } else {
            self.reconcile_relational_group::<Error>(
                read,
                table,
                meta,
                row_id,
                snapshot,
                None,
                read_memory.clone(),
                &mut || Ok(()),
                None,
                &mut adopt,
                &mut last,
            )?
        };
        memory.adopt_reader_credit(&read_memory, credit.bytes())?;
        credit.transfer();
        Ok(row.map(|row| (row, memory)))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn row_by_id_in_read_with_read_memory(
        &self,
        read: &redb::ReadTransaction,
        table: &str,
        meta: &TableMeta,
        row_id: RowId,
        snapshot: SnapshotId,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<(VersionedRow, crate::read_image_memory::ImageMemoryHandle)>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.reconciled_row_with_owner(read, table, meta, row_id, snapshot, read_memory, false)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn row_identity_by_id_in_read(
        &self,
        read: &redb::ReadTransaction,
        table: &str,
        meta: &TableMeta,
        row_id: RowId,
        snapshot: SnapshotId,
        _authentication: Option<&mut content_identity::ReadAuthenticationBatch<'_>>,
    ) -> Result<Option<(VersionedRow, crate::read_image_memory::ImageMemoryHandle)>> {
        let _decode_scope = crate::read_image_memory::ImageMemory::enter_decode_call()?;
        self.reconciled_row_with_owner(
            read,
            table,
            meta,
            row_id,
            snapshot,
            self.read_memory.clone(),
            true,
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn row_identity_by_id_in_read_with_read_memory(
        &self,
        read: &redb::ReadTransaction,
        table: &str,
        meta: &TableMeta,
        row_id: RowId,
        snapshot: SnapshotId,
        _authentication: Option<&mut content_identity::ReadAuthenticationBatch<'_>>,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<(VersionedRow, crate::read_image_memory::ImageMemoryHandle)>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.reconciled_row_with_owner(read, table, meta, row_id, snapshot, read_memory, false)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn relational_row_by_id_lsn(
        &self,
        table: &str,
        meta: &TableMeta,
        row_id: RowId,
        lsn: Lsn,
    ) -> Result<Option<VersionedRow>> {
        let read_memory = self.read_memory.clone();
        graph_read::with_pages(read_memory.clone(), || {
            self.relational_row_by_id_lsn_with_read_memory(table, meta, row_id, lsn, read_memory)
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn relational_row_by_id_lsn_with_read_memory(
        &self,
        table: &str,
        meta: &TableMeta,
        row_id: RowId,
        lsn: Lsn,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<Option<VersionedRow>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.relational_row_by_id_lsn_with_checkpoint(
            table,
            meta,
            row_id,
            lsn,
            read_memory,
            &mut || Ok(()),
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn relational_row_by_id_lsn_with_checkpoint(
        &self,
        table: &str,
        meta: &TableMeta,
        row_id: RowId,
        lsn: Lsn,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        checkpoint: &mut dyn FnMut() -> Result<()>,
    ) -> Result<Option<VersionedRow>> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        graph_read::with_pages(read_memory.clone(), || {
            self.with_read(|read| {
                let mut last = None;
                self.reconcile_addressed_relational_group::<Error>(
                    read,
                    table,
                    meta,
                    row_id,
                    self.logical_frontier,
                    Some(lsn),
                    read_memory.clone(),
                    checkpoint,
                    None,
                    &mut |admission: crate::read_image_memory::RowReadAdmission<'_>| {
                        admission
                            .retain_into_image(crate::read_image_memory::active_owner()?.as_ref())
                    },
                    &mut last,
                )
            })
        })
    }

    pub(crate) fn bounded_relational_row_by_identity_admitted<E>(
        &self,
        table: &str,
        meta: &TableMeta,
        row_id: RowId,
        snapshot: SnapshotId,
        before_touch: &mut dyn FnMut() -> std::result::Result<(), E>,
        before_clone: &mut dyn FnMut(
            crate::read_image_memory::RowReadAdmission<'_>,
        ) -> std::result::Result<(), E>,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> std::result::Result<Option<VersionedRow>, E>
    where
        E: From<Error>,
    {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let operation = crate::read_image_memory::ImageMemory::operation_reader(
            self.verified_ordered_runs.memory.accountant.clone(),
            read_memory.clone(),
        )
        .map_err(E::from)?;
        let _scope = operation.enter();

        let read = self.begin_read().map_err(E::from)?;
        let mut last = None;
        self.reconcile_relational_group(
            &read,
            table,
            meta,
            row_id,
            snapshot,
            None,
            read_memory,
            before_touch,
            None,
            before_clone,
            &mut last,
        )
    }

    pub(crate) fn bounded_visit_relational_rows<E>(
        &self,
        table: &str,
        meta: &TableMeta,
        snapshot: SnapshotId,
        mut visit: impl FnMut(&VersionedRow) -> std::result::Result<(), E>,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> std::result::Result<(), E>
    where
        E: From<Error>,
    {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let mut cursor = contextdb_relational::mem::BoundedPhysicalCursor::default();
        loop {
            let mut output = contextdb_core::read_memory::ReadCredit::new(read_memory.clone());
            let row = self.bounded_relational_physical_next_admitted(
                table,
                meta,
                snapshot,
                &mut cursor,
                &mut || Ok(()),
                &mut |_| Ok(()),
                &mut |admission: crate::read_image_memory::RowReadAdmission<'_>| {
                    admission
                        .retain_into(
                            &mut output,
                            "reader",
                            "version_visit_output",
                            "Raise the declared reader memory limit.",
                        )
                        .map_err(E::from)
                },
                read_memory.clone(),
            )?;
            let Some(row) = row else { return Ok(()) };
            visit(&row)?;
        }
    }

    /// The cache identity of one working WINDOW: the run it belongs to, plus
    /// the exclusive boundary it begins after. Two cursors standing at the same
    /// proved key in the same range share a window; a cursor that has moved on
    /// asks for the next one instead of the one it already drained.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn verified_ordered_run_key(
        &self,
        table: &str,
        index: &str,
        snapshot: SnapshotId,
        cursor: &contextdb_relational::mem::BoundedOrderedRowCursor,
    ) -> Result<[u8; 32]> {
        let (start, end, prefix_len) = cursor.source_run_bounds();
        let mut hash = blake3::Hasher::new();
        for part in [
            table.as_bytes(),
            index.as_bytes(),
            &snapshot.0.to_le_bytes(),
        ] {
            hash.update(&(part.len() as u64).to_le_bytes());
            hash.update(part);
        }
        struct HashWriter<'a>(&'a mut blake3::Hasher);
        impl std::io::Write for HashWriter<'_> {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0.update(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        // Hash borrowed typed endpoints without making temporary key/value
        // clones or an unadmitted serialized byte vector.
        for bound in [start, end] {
            let (tag, key) = match bound {
                std::ops::Bound::Unbounded => (0u8, None),
                std::ops::Bound::Included(key) => (1, Some(key)),
                std::ops::Bound::Excluded(key) => (2, Some(key)),
            };
            hash.update(&[tag]);
            if let Some(key) = key {
                hash.update(&(key.len() as u64).to_le_bytes());
                for component in key {
                    let (direction, value) = match component {
                        DirectedValue::Asc(TotalOrdAsc(value)) => (0u8, value),
                        DirectedValue::Desc(TotalOrdDesc(value)) => (1u8, value),
                    };
                    hash.update(&[direction]);
                    bincode::serde::encode_into_std_write(
                        value,
                        &mut HashWriter(&mut hash),
                        bincode::config::standard(),
                    )
                    .map_err(|error| {
                        Error::Other((format!("ordered endpoint cache identity: {error}")).into())
                    })?;
                }
            }
        }
        hash.update(&(prefix_len as u64).to_le_bytes());
        hash.update(&[u8::from(cursor.source_reverse())]);
        match cursor.source_prefix_quota() {
            None => {
                hash.update(&[0]);
            }
            Some(quota) => {
                hash.update(&[1]);
                hash.update(&quota.to_le_bytes());
            }
        }

        let after = cursor.source_position().unwrap_or_default();
        hash.update(&(after.len() as u64).to_le_bytes());
        hash.update(after);
        Ok(*hash.finalize().as_bytes())
    }

    /// A DESC NULL component is the first region in that component's
    /// comparator domain. Excluding its short prefix does not exclude its
    /// longer keys. Complete its high edge only where every omitted component
    /// has the existing ASC NULL-high sentinel.
    pub(super) fn complete_ordered_seek(
        declaration: &IndexDecl,
        cursor: &contextdb_relational::mem::BoundedOrderedRowCursor,
    ) -> std::ops::Bound<IndexKey> {
        let seek = cursor.source_seek().map(|key| key.to_vec());
        if cursor.source_reverse() {
            return seek;
        }
        match seek {
            std::ops::Bound::Excluded(mut key)
                if key.len() < declaration.columns.len()
                    && matches!(
                        key.last(),
                        Some(DirectedValue::Desc(TotalOrdDesc(Value::Null)))
                    )
                    && declaration.columns[key.len()..]
                        .iter()
                        .all(|(_, direction)| *direction == contextdb_core::SortDirection::Asc) =>
            {
                key.resize_with(declaration.columns.len(), || {
                    DirectedValue::Asc(TotalOrdAsc(Value::Null))
                });
                std::ops::Bound::Excluded(key)
            }
            other => other,
        }
    }

    pub(super) fn verified_ordered_posting_bytes(posting: &VerifiedOrderedPosting) -> usize {
        posting.source_key.capacity()
    }

    pub(super) fn build_verified_ordered_run<E>(
        &self,
        table: &str,
        meta: &TableMeta,
        declaration: &IndexDecl,
        snapshot: SnapshotId,
        cursor: &mut contextdb_relational::mem::BoundedOrderedRowCursor,
        cache_key: [u8; 32],
        check_boundary: &mut dyn FnMut() -> std::result::Result<(), E>,
        before_touch: &mut dyn FnMut() -> std::result::Result<(), E>,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> std::result::Result<Option<Arc<VerifiedOrderedRun>>, E>
    where
        E: From<Error>,
    {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if cursor.source_prefix_quota() == Some(0) {
            cursor.source_stop_prefix();
            return Ok(None);
        }
        let _attribution = contextdb_core::find_attribution::stage("window_build");
        let Some(memory) = self.verified_ordered_runs.begin_build(read_memory.clone()) else {
            return Ok(None);
        };
        // The proof walk and the posting identities it produces draw from the
        // one reservation this build already had admitted. Nothing is assumed
        // about how many pages a descent holds: the walk charges the chunks it
        // is holding, the key it is comparing and the pages it keeps, so a
        // deeper tree or a wider key simply leaves less room for payload and
        // the run is not cached instead of quietly exceeding the reservation.
        let proof = content_identity::ProofMemory::growable(
            memory.held(),
            Arc::clone(&memory) as Arc<dyn content_identity::ProofPoolGrowth>,
        );
        let retained = match cursor
            .source_proof()
            .and_then(|slot| {
                Arc::clone(&slot.0)
                    .downcast::<Mutex<RetainedOrderedProof>>()
                    .ok()
            })
            .filter(|state| {
                let state = state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                state.table == table
                    && state.index == declaration.name
                    && state.snapshot == snapshot
                    && Arc::ptr_eq(&state.cache, &self.verified_ordered_runs.state)
                    && state.epoch
                        == self
                            .verified_ordered_runs
                            .epoch
                            .load(std::sync::atomic::Ordering::SeqCst)
            }) {
            Some(state) => state,
            None => {
                let Some(proof_memory) =
                    self.verified_ordered_runs.begin_build(read_memory.clone())
                else {
                    return Ok(None);
                };
                // A small bounded share lets the first descent retain its
                // upper pages. If this optional reservation is unavailable,
                // the proof still runs and grows only for pages it needs.
                let cache_share = proof_memory.ceiling.min(64 * 1024);
                let _ = content_identity::ProofPoolGrowth::grow(proof_memory.as_ref(), cache_share);
                let state = Arc::new(Mutex::new(RetainedOrderedProof {
                    table: table.to_owned(),
                    index: declaration.name.clone(),
                    snapshot,
                    cache: Arc::clone(&self.verified_ordered_runs.state),
                    epoch: self
                        .verified_ordered_runs
                        .epoch
                        .load(std::sync::atomic::Ordering::SeqCst),
                    proof: content_identity::ProofMemory::growable(
                        proof_memory.held(),
                        Arc::clone(&proof_memory) as Arc<dyn content_identity::ProofPoolGrowth>,
                    ),
                    _memory: proof_memory,
                    batch: content_identity::OrderedProofBatch::default(),
                }));
                cursor.set_source_proof(contextdb_relational::mem::SourceReadSlot(
                    Arc::clone(&state) as Arc<dyn std::any::Any + Send + Sync>,
                ));
                state
            }
        };
        let mut retained = retained
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if retained.proof.exhausted() {
            let RetainedOrderedProof { proof, batch, .. } = &mut *retained;
            batch.evict(proof);
        }
        let mut charged_payload = 0usize;
        let bound_scratch = cursor.source_run_bounds();
        let mut seek_bytes = declaration
            .columns
            .len()
            .saturating_mul(std::mem::size_of::<DirectedValue>())
            .saturating_mul(8);
        for bound in [bound_scratch.0, bound_scratch.1] {
            if let std::ops::Bound::Included(key) | std::ops::Bound::Excluded(key) = bound {
                for component in key {
                    let value = match component {
                        DirectedValue::Asc(TotalOrdAsc(value))
                        | DirectedValue::Desc(TotalOrdDesc(value)) => value,
                    };
                    seek_bytes =
                        seek_bytes.saturating_add(value.estimated_bytes().saturating_mul(8));
                }
            }
        }
        seek_bytes = seek_bytes
            .saturating_add(
                cursor
                    .source_position()
                    .map_or(0, <[u8]>::len)
                    .saturating_mul(4),
            )
            .saturating_add(256);
        let Some(_seek_charge) = proof.charge_scratch(seek_bytes) else {
            return Ok(None);
        };
        let seek = Self::complete_ordered_seek(declaration, cursor);
        let reverse = cursor.source_reverse();
        let mut postings = Vec::new();
        let mut payload_bytes = 0usize;
        // The last key this cursor has already proved and delivered. It is
        // encoded with the terminal transaction id for its direction, so it is
        // an exclusive boundary in tree order on both the proof walk and the
        // physical walk: the proved prefix is skipped at descent rather than
        // re-walked, and nothing between the two windows can fall in a gap.
        let after = cursor.source_position().map(<[u8]>::to_vec);
        // What one window may retain. This is scheduling only: the pool admits
        // the payload as it is kept and grows for it, so the cap decides how
        // much one window hands back, never which keys or results a caller may
        // have. The descent's own pages are admitted the same way beside it.
        let window_limit = VERIFIED_ORDERED_WINDOW_MAX_BYTES;
        #[cfg(any(test, feature = "test-seams"))]
        let window_limit = verified_ordered_run_test_seams::window_bytes().unwrap_or(window_limit);
        let logical_seek = match (after.clone(), seek, reverse) {
            (Some(after), _, _) => std::ops::Bound::Excluded(after),
            (None, std::ops::Bound::Included(key), true) => std::ops::Bound::Included(
                DurableRelationalIndexKey::new(key, RowId(u64::MAX), TxId(0))?.0,
            ),
            (None, std::ops::Bound::Excluded(key), true) => {
                std::ops::Bound::Excluded(DurableRelationalIndexKey::new(key, RowId(0), TxId(0))?.0)
            }
            (None, std::ops::Bound::Included(key), false) => {
                std::ops::Bound::Included(DurableRelationalIndexKey::new(key, RowId(0), TxId(0))?.0)
            }
            (None, std::ops::Bound::Excluded(key), false) => std::ops::Bound::Excluded(
                DurableRelationalIndexKey::new(key, RowId(u64::MAX), TxId(0))?.0,
            ),
            (None, std::ops::Bound::Unbounded, _) => std::ops::Bound::Unbounded,
        };
        // `window_full` says this window ended because it was full, not because
        // the range did. `complete` stays for the one case that is still a
        // refusal: a pool that cannot admit even one posting.
        let mut window_full = false;
        let mut quota_stopped = false;
        let quota = cursor.source_prefix_quota();
        let mut complete = true;
        let boundary = std::cell::RefCell::new(&mut *check_boundary);
        let touched = std::cell::RefCell::new(&mut *before_touch);
        let mut before_page = || {
            (boundary.borrow_mut())()?;
            (touched.borrow_mut())()
        };
        let RetainedOrderedProof {
            proof: shared_proof,
            batch: shared_batch,
            ..
        } = &mut *retained;
        self.content_identity.visit_relational_index_postings_in(
            table,
            &declaration.name,
            logical_seek,
            reverse,
            shared_proof,
            shared_batch,
            &mut before_page,
            &mut |logical_key| {
                // A logical posting is actual examined work independently
                // from the page authentication that delivered it.
                (boundary.borrow_mut())()?;
                (touched.borrow_mut())()?;
                // This callback copies the delivered key, decodes it into an
                // owned position and re-encodes a continuation key before it
                // decides to keep anything. All three are live at once, so all
                // three are admitted from this build's pool before the first
                // allocation and released as soon as only the posting is kept.
                if payload_bytes >= window_limit {
                    window_full = true;
                    return Ok(false);
                }
                let scratch = logical_key.len().saturating_mul(3).saturating_add(256);
                let Some(scratch_charge) = proof.charge_scratch(scratch) else {
                    if postings.is_empty() {
                        complete = false;
                    } else {
                        window_full = true;
                    }
                    return Ok(false);
                };
                // A posting delivered by the proof walk is physical stored data:
                // decode it exactly and canonically before anything orders,
                // compares or continues from it.
                #[cfg(any(test, feature = "test-seams"))]
                crate::writer_query_test_seams::note_target_entry(table);
                let logical = DurableRelationalIndexKey(logical_key.to_vec())
                    .decode_canonical()
                    .map_err(|error| self.content_identity_error(error))
                    .map_err(E::from)?;
                if !cursor.source_inside_run(&logical.key) {
                    return Ok(false);
                }
                let posting = VerifiedOrderedPosting {
                    source_key: DurableRelationalIndexKey::new(
                        logical.key.clone(),
                        logical.row_id,
                        if reverse { TxId(0) } else { TxId(u64::MAX) },
                    )?
                    .0,
                    row_id: logical.row_id,
                };
                drop(logical);
                drop(scratch_charge);
                payload_bytes =
                    payload_bytes.saturating_add(Self::verified_ordered_posting_bytes(&posting));
                let retained = payload_bytes
                    .saturating_add(
                        postings
                            .len()
                            .saturating_add(1)
                            .next_power_of_two()
                            // Vec's first push can allocate four slots. Admit
                            // that capacity before keeping a one-posting window.
                            .max(4)
                            .saturating_mul(std::mem::size_of::<VerifiedOrderedPosting>()),
                    )
                    .saturating_add(256);
                if !proof.charge_payload(retained.saturating_sub(charged_payload)) {
                    if postings.is_empty() {
                        complete = false;
                    } else {
                        window_full = true;
                    }
                    return Ok(false);
                }
                charged_payload = retained;
                postings.push(posting);
                // Stop on the callback retaining the final required posting:
                // no next leaf callback may decode a key past this demand.
                if quota.is_some_and(|quota| postings.len() as u64 >= quota) {
                    quota_stopped = true;
                    return Ok(false);
                }
                Ok(true)
            },
        )?;
        let _ = before_page;
        drop(touched);
        drop(boundary);
        // The window stops short of the range when it filled, or when the
        // descent itself ran out of pool part way. Either way the range
        // continues past its last posting.
        let more = quota_stopped || window_full || proof.exhausted() || shared_proof.exhausted();
        if !complete || (postings.is_empty() && more) {
            // Not one posting fits beside the descent this range needs. That is
            // the one remaining budget refusal; everything else is a window.
            return Ok(None);
        }
        if postings.is_empty() {
            // Empty canonical ranges still owe a physical-extra check below.
            if !proof.charge_payload(256) {
                return Ok(None);
            }
            if after.is_none() {
                contextdb_core::find_attribution::count("verified_runs_pruned_empty", 1);
            }
        }
        let bytes = payload_bytes
            .saturating_add(
                postings
                    .capacity()
                    .saturating_mul(std::mem::size_of::<VerifiedOrderedPosting>()),
            )
            .saturating_add(256);
        if bytes > memory.held() {
            return Ok(None);
        }
        // The canonical proof commits the logical set, but the physical index
        // must also contain exactly that live set. Reconcile once per WINDOW
        // with a charged sequential walk, without fetching row bodies. Windows
        // partition the range: this one runs from the key the cursor last
        // proved (exclusive) to the last key proved here, and the next one
        // starts exclusively after that, so no live posting falls between two
        // windows unexamined.
        // One read and one opened physical index for every window this
        // cursor builds: the statement's runs all reconcile against the same
        // retained snapshot, so opening them again per window buys nothing.
        let physical = match cursor
            .source_read()
            .and_then(|slot| Arc::clone(&slot.0).downcast::<RetainedIndexRead>().ok())
            .filter(|read| {
                read.table == table
                    && read.index == declaration.name
                    && read.current.is_some() == self.ordered_prefix_snapshot_is_current(snapshot)
            }) {
            Some(read) => read,
            None => {
                let read = self.begin_read().map_err(E::from)?;
                let name = RedbPersistence::relational_index_table_name(table, &declaration.name);
                self.verify_history_root_in_read(&read, &name)
                    .map_err(E::from)?;
                self.verify_history_root_in_read(&read, &RedbPersistence::rel_table_name(table))
                    .map_err(E::from)?;
                let definition: TableDefinition<DurableRelationalIndexKey, &[u8]> =
                    TableDefinition::new(name.as_str());
                #[cfg(any(test, feature = "test-seams"))]
                crate::writer_query_test_seams::note_physical_source(
                    crate::writer_query_test_seams::PhysicalFamily::Posting,
                    &read,
                );
                let index = read.open_table(definition).map_err(|error| {
                    E::from(self.content_identity_corruption(format!(
                        "cannot read physical index {}: {error}",
                        declaration.name
                    )))
                })?;
                contextdb_core::find_attribution::count("window_reads_opened", 1);
                #[cfg(any(test, feature = "test-seams"))]
                crate::read_work_probe::note_verified_read_opened();
                // CURRENT is the logical set only at this root's proved
                // committed frontier. Earlier snapshots keep historical reads.
                let current = if self.ordered_prefix_snapshot_is_current(snapshot) {
                    let name = RedbPersistence::current_relational_index_table_name(
                        table,
                        &declaration.name,
                    );
                    let definition: TableDefinition<DurableRelationalIndexKey, &[u8]> =
                        TableDefinition::new(name.as_str());
                    Some(read.open_table(definition).map_err(|error| {
                        E::from(self.content_identity_corruption(format!(
                            "cannot read current index {}: {error}",
                            declaration.name
                        )))
                    })?)
                } else {
                    None
                };
                let opened = Arc::new(RetainedIndexRead {
                    table: table.to_owned(),
                    index: declaration.name.clone(),
                    physical: index,
                    current,
                    _read: read,
                });
                cursor.set_source_read(contextdb_relational::mem::SourceReadSlot(
                    Arc::clone(&opened) as Arc<dyn std::any::Any + Send + Sync>,
                ));
                opened
            }
        };
        #[cfg(any(test, feature = "test-seams"))]
        crate::writer_query_test_seams::note_physical_source(
            crate::writer_query_test_seams::PhysicalFamily::Posting,
            &physical._read,
        );
        // Only data-page fetches belong to the targeted cold-posting observation.
        // Transaction admission and catalog/table setup are observed separately.
        #[cfg(any(test, feature = "test-seams"))]
        let _target_reads = crate::writer_query_test_seams::posting_operation(table, cursor);
        let index = physical.current.as_ref().unwrap_or(&physical.physical);
        use std::ops::Bound::{Excluded, Included, Unbounded};
        let completed_seek = Self::complete_ordered_seek(declaration, cursor);
        let bounds = match (after.as_deref(), completed_seek, reverse) {
            (Some(after), _, true) => (
                Unbounded,
                Excluded(DurableRelationalIndexKey(after.to_vec())),
            ),
            (Some(after), _, false) => (
                Excluded(DurableRelationalIndexKey(after.to_vec())),
                Unbounded,
            ),
            (None, Included(key), true) => (
                Unbounded,
                Included(DurableRelationalIndexKey::new(
                    key,
                    RowId(u64::MAX),
                    TxId(u64::MAX),
                )?),
            ),
            (None, Excluded(key), true) => (
                Unbounded,
                Excluded(DurableRelationalIndexKey::new(key, RowId(0), TxId(0))?),
            ),
            (None, Included(key), false) => (
                Included(DurableRelationalIndexKey::new(key, RowId(0), TxId(0))?),
                Unbounded,
            ),
            (None, Excluded(key), false) => (
                Excluded(DurableRelationalIndexKey::new(
                    key,
                    RowId(u64::MAX),
                    TxId(u64::MAX),
                )?),
                Unbounded,
            ),
            (None, Unbounded, _) => (Unbounded, Unbounded),
        };
        let mut range = index
            .range(bounds)
            .map_err(RedbPersistence::storage_error)
            .map_err(E::from)?;
        let mut entries = std::iter::from_fn(|| {
            if reverse {
                range.next_back()
            } else {
                range.next()
            }
        });
        let mut matched = 0usize;
        loop {
            // The window's far boundary: once every proven posting has been
            // matched and the range continues, what comes next belongs to the
            // next window, which reconciles it from the same key.
            if more && matched == postings.len() {
                break;
            }
            let Some(entry) = charged_ordered_next(&mut entries, check_boundary, before_touch)?
            else {
                break;
            };
            let entry = entry
                .map_err(RedbPersistence::storage_error)
                .map_err(E::from)?;
            #[cfg(any(test, feature = "test-seams"))]
            crate::writer_query_test_seams::note_target_entry(table);
            let physical_scratch = entry
                .0
                .value()
                .0
                .len()
                .saturating_mul(4)
                .saturating_add(
                    declaration
                        .columns
                        .len()
                        .saturating_mul(std::mem::size_of::<DirectedValue>())
                        .saturating_mul(3),
                )
                .saturating_add(256);
            let Some(_physical_charge) = proof.charge_scratch(physical_scratch) else {
                return Ok(None);
            };
            let (position, deleted_tx) = if physical.current.is_some() {
                let (key, value) = entry;
                let mut position = key
                    .value()
                    .decode_canonical()
                    .map_err(|error| E::from(self.content_identity_error(error)))?;
                if position.created_tx != TxId(0) {
                    return Err(E::from(self.content_identity_corruption(
                        "current posting has a nonzero key transaction",
                    )));
                }
                let creating: TxId =
                    RedbPersistence::decode_exact(value.value()).map_err(E::from)?;
                if creating.0 > snapshot.0 {
                    return Err(E::from(self.content_identity_corruption(
                        "current posting was created past the pinned frontier",
                    )));
                }
                // One exact creating-version probe, never a retired-history
                // walk. The key can remain unchanged across later row versions.
                check_boundary()?;
                before_touch()?;
                let historical_key = DurableRelationalIndexKey::new(
                    position.key.clone(),
                    position.row_id,
                    creating,
                )?;
                let historical = physical
                    .physical
                    .get(&historical_key)
                    .map_err(RedbPersistence::storage_error)
                    .map_err(E::from)?
                    .ok_or_else(|| {
                        E::from(self.content_identity_corruption(
                            "current posting names a missing creating posting",
                        ))
                    })?;
                let deleted: Option<TxId> =
                    RedbPersistence::decode_exact(historical.value()).map_err(E::from)?;
                if deleted.is_some() {
                    return Err(E::from(self.content_identity_corruption(
                        "current posting names a retired creating posting",
                    )));
                }
                position.created_tx = creating;
                (position, deleted)
            } else {
                let (_, position, deleted) =
                    decode_relational_index_entry(&self.database_path, Some(entry))
                        .map_err(E::from)?
                        .expect("one physical posting");
                (position, deleted)
            };
            // The stored key is typed data this walk is about to compare and
            // deliver. Refuse a posting that disagrees with its declaration
            // before it can reach a row or an ordering.
            RedbPersistence::validated_index_position(declaration, meta, &position)
                .map_err(|reason| E::from(self.content_identity_corruption(reason)))?;
            if !cursor.source_inside_run(&position.key) {
                break;
            }
            if position.created_tx.0 > snapshot.0 || deleted_tx.is_some_and(|tx| tx.0 <= snapshot.0)
            {
                continue;
            }
            let source_key = DurableRelationalIndexKey::new(
                position.key,
                position.row_id,
                if reverse { TxId(0) } else { TxId(u64::MAX) },
            )?
            .0;
            if postings
                .get(matched)
                .is_none_or(|posting| posting.source_key != source_key)
            {
                return Err(E::from(self.content_identity_corruption(format!(
                    "physical index {} has an extra or changed live posting",
                    declaration.name
                ))));
            }
            matched += 1;
        }
        if matched != postings.len() {
            return Err(E::from(self.content_identity_corruption(format!(
                "physical index {} is missing an authenticated live posting",
                declaration.name
            ))));
        }
        #[cfg(any(test, feature = "test-seams"))]
        crate::read_work_probe::note_verified_run_built(postings.len());
        contextdb_core::find_attribution::count("verified_runs_built", 1);
        contextdb_core::find_attribution::count("verified_postings_walked", postings.len() as u64);
        if postings.is_empty() {
            contextdb_core::find_attribution::count("verified_runs_built_empty", 1);
        }
        #[cfg(any(test, feature = "test-seams"))]
        if std::env::var_os("CONTEXTDB_P1_WINDOW_WHY").is_some() {
            eprintln!(
                "WINDOW postings={} more={more} window_full={window_full} exhausted={} \
                 payload_bytes={payload_bytes} window_limit={window_limit} bytes={bytes} \
                 held={} peak={}",
                postings.len(),
                proof.exhausted(),
                memory.held(),
                proof.peak(),
            );
        }
        // The build scratch no longer owns any endpoint/iterator allocation
        // when its reservation is reduced to the retained cache payload.
        drop(entries);
        drop(range);
        drop(after);
        drop(_seek_charge);
        Ok(Some(self.verified_ordered_runs.insert(
            cache_key,
            postings,
            more,
            quota_stopped,
            memory,
            bytes,
        )))
    }

    /// Whether the committed canonical index tree holds no posting under ANY
    /// of `prefixes`, decided by one authenticated descent to each prefix's
    /// first key. The descents share one proof pool and one batch of
    /// authenticated pages, so the upper levels they have in common are read
    /// and proved once for the statement. `None` when the pool cannot hold a
    /// descent: the caller then has no proof and must not treat anything as
    /// empty.
    ///
    /// Only the canonical tree is asked. It is what every window's postings
    /// are proved against, so an empty answer here means no window over these
    /// prefixes could deliver an identity; the physical index is not consulted
    /// because nothing is read from it.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn verify_relational_source_roots(
        &self,
        table: &str,
        index: &str,
        mode: IndexRootCheck,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<()> {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let memory = crate::read_image_memory::ImageMemory::charging_reader(
            self._source_memory.memory.accountant.clone(),
            read_memory,
        );
        memory.reserve(
            table
                .len()
                .saturating_add(index.len())
                .saturating_mul(2)
                .saturating_add(256),
        )?;
        self.with_read(|read| {
            self.verify_history_root_in_read(read, &RedbPersistence::rel_table_name(table))?;
            let name = RedbPersistence::relational_index_table_name(table, index);
            match mode {
                IndexRootCheck::Physical => self.verify_history_root_in_read(read, &name),
                // Nothing is read from the physical index, so its root
                // disagreeing with the producer's witness changes no answer.
                // The row table stays strict above; the mismatch is disclosed
                // and every read that consumes physical postings still refuses.
                IndexRootCheck::Canonical => {
                    match RedbPersistence::verify_history_root_canonical(read, &name) {
                        Ok(matches) => {
                            history_root_witness::note_index_root_damage(
                                &self.database_path,
                                table,
                                index,
                                !matches,
                            );
                            Ok(())
                        }
                        Err(error) => Err(match error {
                            Error::StoreCorrupted { reason, .. } => {
                                self.content_identity_corruption(reason)
                            }
                            other => other,
                        }),
                    }
                }
            }
        })
    }

    pub(crate) fn verified_index_prefixes_are_empty<E>(
        &self,
        table: &str,
        declaration: &IndexDecl,
        prefixes: &[contextdb_core::IndexKey],
        check_boundary: &mut dyn FnMut() -> std::result::Result<(), E>,
        before_probe: &mut dyn FnMut() -> std::result::Result<(), E>,
        before_native_touch: &mut dyn FnMut() -> std::result::Result<(), E>,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> std::result::Result<Option<bool>, E>
    where
        E: From<Error>,
    {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        check_boundary()?;
        before_native_touch()?;
        self.verify_relational_source_roots(
            table,
            &declaration.name,
            IndexRootCheck::Canonical,
            read_memory.clone(),
        )
        .map_err(E::from)?;
        let _attribution = contextdb_core::find_attribution::stage("window_probe");
        let Some(memory) = self.verified_ordered_runs.begin_build(read_memory.clone()) else {
            return Ok(None);
        };
        let proof = content_identity::ProofMemory::growable(
            memory.held(),
            Arc::clone(&memory) as Arc<dyn content_identity::ProofPoolGrowth>,
        );
        let mut batch = content_identity::OrderedProofBatch::default();
        for prefix in prefixes {
            check_boundary()?;
            before_probe()?;
            let mut before_page = || {
                check_boundary()?;
                before_native_touch()
            };
            let seek = DurableRelationalIndexKey::new(prefix.clone(), RowId(0), TxId(0))?.0;
            let mut inside = None::<bool>;
            self.content_identity.visit_relational_index_postings_in(
                table,
                &declaration.name,
                std::ops::Bound::Included(seek),
                false,
                &proof,
                &mut batch,
                &mut before_page,
                &mut |logical_key| {
                    #[cfg(any(test, feature = "test-seams"))]
                    crate::writer_query_test_seams::note_target_entry(table);
                    let logical = DurableRelationalIndexKey(logical_key.to_vec())
                        .decode_canonical()
                        .map_err(|error| self.content_identity_error(error))
                        .map_err(E::from)?;
                    inside = Some(
                        logical.key.len() >= prefix.len()
                            && logical.key[..prefix.len()] == prefix[..],
                    );
                    Ok(false)
                },
            )?;
            if inside.is_none() && proof.exhausted() {
                return Ok(None);
            }
            contextdb_core::find_attribution::count("verified_prefix_probes", 1);
            #[cfg(any(test, feature = "test-seams"))]
            crate::read_work_probe::note_verified_prefix_probe();
            if inside == Some(true) {
                return Ok(Some(false));
            }
        }
        Ok(Some(true))
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn bounded_visit_verified_ordered_identities<E>(
        &self,
        table: &str,
        index: &str,
        snapshot: SnapshotId,
        cursor: &mut contextdb_relational::mem::BoundedOrderedRowCursor,
        before_touch: &mut dyn FnMut() -> std::result::Result<(), E>,
        visit: &mut dyn FnMut(RowId) -> std::result::Result<(), E>,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> std::result::Result<bool, E>
    where
        E: From<Error>,
    {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        if snapshot != self.logical_frontier {
            let meta = self
                .with_read(|read| RedbPersistence::load_table_meta_in_read_txn(read, table))
                .map_err(E::from)?
                .ok_or_else(|| E::from(Error::TableNotFound(table.into())))?;
            let declaration = meta
                .indexes
                .iter()
                .find(|declaration| declaration.name == index)
                .ok_or_else(|| {
                    E::from(Error::IndexNotFound {
                        table: table.into(),
                        index: index.into(),
                    })
                })?;
            self.historical_index_preflight(
                table,
                &meta,
                declaration,
                snapshot,
                read_memory.clone(),
                before_touch,
            )?;
        }
        let memory = crate::read_image_memory::ImageMemory::charging_reader(
            self._source_memory.memory.accountant.clone(),
            read_memory,
        );
        memory.scope(|| {
            loop {
                let cache_key = match cursor.source_cache_key() {
                    Some(key) => key,
                    None => self
                        .verified_ordered_run_key(table, index, snapshot, cursor)
                        .map_err(E::from)?,
                };
                // This drain only delivers windows the cache already holds; opening
                // a new one is the full pull path's work, where a refusal can be
                // typed and the build can be charged.
                let Some(run) = self.verified_ordered_runs.get(&cache_key) else {
                    return Ok(false);
                };
                if cursor.source_cache_key().is_none() {
                    cursor.source_begin_cached_run(cache_key, false);
                }
                while let Some(posting) = run.postings.get(cursor.source_cached_position()) {
                    if !cursor.source_run_precharged() {
                        before_touch()?;
                    }
                    cursor.source_advance_cached_position();
                    cursor.source_advance_cached_identity(&posting.source_key, posting.row_id);
                    visit(posting.row_id)?;
                    #[cfg(any(test, feature = "test-seams"))]
                    if verified_ordered_run_test_seams::evict_after_cached_posting_for_test() {
                        self.verified_ordered_runs.clear_for_test();
                        self.clear_statement_proofs();
                        return Ok(false);
                    }
                }
                if run.more || run.quota_stopped {
                    // The window is drained and the range continues past it. The
                    // cursor carries its last proved key, which is where the next
                    // window begins.
                    cursor.source_abandon_cached_run();
                    continue;
                }
                cursor.source_finish_run();
                return Ok(true);
            }
        })
    }

    pub(crate) fn bounded_relational_physical_next<E>(
        &self,
        table: &str,
        meta: &TableMeta,
        snapshot: SnapshotId,
        cursor: &mut contextdb_relational::mem::BoundedPhysicalCursor,
        before_touch: &mut dyn FnMut() -> std::result::Result<(), E>,
        observed: &mut dyn FnMut(&VersionedRow) -> std::result::Result<(), E>,
        before_clone: &mut dyn FnMut(usize) -> std::result::Result<(), E>,
    ) -> std::result::Result<Option<VersionedRow>, E>
    where
        E: From<Error>,
    {
        let read_memory = self.read_memory.clone();
        graph_read::with_pages_result(
            read_memory.clone(),
            || {
                self.bounded_relational_physical_next_admitted(
                    table,
                    meta,
                    snapshot,
                    cursor,
                    before_touch,
                    observed,
                    &mut |admission: crate::read_image_memory::RowReadAdmission<'_>| {
                        before_clone(match admission {
                            crate::read_image_memory::RowReadAdmission::Copy(bytes) => bytes,
                            crate::read_image_memory::RowReadAdmission::Move(memory) => {
                                memory.retained_payload_bytes()
                            }
                        })
                    },
                    read_memory,
                )
            },
            E::from,
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn bounded_relational_physical_next_admitted<E>(
        &self,
        table: &str,
        meta: &TableMeta,
        snapshot: SnapshotId,
        cursor: &mut contextdb_relational::mem::BoundedPhysicalCursor,
        before_touch: &mut dyn FnMut() -> std::result::Result<(), E>,
        observed: &mut dyn FnMut(&VersionedRow) -> std::result::Result<(), E>,
        before_clone: &mut dyn FnMut(
            crate::read_image_memory::RowReadAdmission<'_>,
        ) -> std::result::Result<(), E>,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> std::result::Result<Option<VersionedRow>, E>
    where
        E: From<Error>,
    {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let witness_memory = crate::read_image_memory::ImageMemory::charging_reader(
            self._source_memory.memory.accountant.clone(),
            read_memory.clone(),
        );
        witness_memory
            .reserve(table.len().saturating_add(64))
            .map_err(E::from)?;
        self.with_read(|read| {
            self.verify_history_root_in_read(read, &RedbPersistence::rel_table_name(table))
        })
        .map_err(E::from)?;
        if cursor.is_exhausted() {
            return Ok(None);
        }
        let mut physical_rows = self.physical_rows.lock().map_err(|_| {
            E::from(self.content_identity_corruption("physical row scan lock poisoned"))
        })?;
        if physical_rows
            .as_ref()
            .is_none_or(|scan| scan.table_name != table)
        {
            let read = self.begin_read().map_err(E::from)?;
            let name = RedbPersistence::rel_table_name(table);
            let definition: TableDefinition<&[u8], &[u8]> = TableDefinition::new(name.as_str());
            let relation = read
                .open_table(definition)
                .map_err(|error| RedbPersistence::relational_table_error(table, error))
                .map_err(E::from)?;
            *physical_rows = Some(RetainedPhysicalRowScan {
                table_name: table.to_owned(),
                relation,
                _transaction: read,
            });
        }
        let scan = physical_rows.as_ref().expect("scan opened");
        loop {
            if snapshot == self.logical_frontier {
                // Native CURRENT enumeration never consults the historical
                // row universe. The held transaction is this source's root.
                before_touch()?;
                let first = match cursor.source_position() {
                    Some(after) => scan
                        .relation
                        .range::<&[u8]>((
                            std::ops::Bound::Excluded(after),
                            std::ops::Bound::Unbounded,
                        ))
                        .map_err(|error| RedbPersistence::relational_storage_error(table, error))
                        .map_err(E::from)?
                        .next()
                        .transpose()
                        .map_err(|error| RedbPersistence::relational_storage_error(table, error))
                        .map_err(E::from)?,
                    None => scan
                        .relation
                        .first()
                        .map_err(|error| RedbPersistence::relational_storage_error(table, error))
                        .map_err(E::from)?,
                };
                let Some((key, body)) = first else {
                    cursor.source_exhaust();
                    return Ok(None);
                };
                let identity: [u8; 24] = key.value().try_into().map_err(|_| {
                    E::from(self.content_identity_corruption("current scan key has invalid width"))
                })?;
                let row_id = RowId(u64::from_be_bytes(
                    identity[..8].try_into().expect("row identity"),
                ));
                drop((key, body));
                let mut last = None;
                let row = self.current_relational_group(
                    &scan._transaction,
                    table,
                    meta,
                    row_id,
                    snapshot,
                    read_memory.clone(),
                    before_touch,
                    Some(&mut *observed),
                    before_clone,
                    &mut last,
                )?;
                let last = last.ok_or_else(|| {
                    E::from(self.content_identity_corruption("current row group disappeared"))
                })?;
                let position = VersionedRow {
                    row_id,
                    values: HashMap::new(),
                    created_tx: TxId(u64::from_be_bytes(last[8..16].try_into().expect("created"))),
                    deleted_tx: None,
                    lsn: Lsn(u64::from_be_bytes(last[16..].try_into().expect("lsn"))),
                    created_at: None,
                };
                let bytes = last.len().checked_add(contextdb_relational::mem::BoundedPhysicalCursor::source_position_owner_bytes())
                    .ok_or_else(|| E::from(self.content_identity_corruption("cursor layout overflow")))?;
                let credit = contextdb_core::read_memory::ReadCredit::try_new(
                    read_memory.clone(),
                    bytes,
                    "reader",
                    "current_scan_position",
                    "Raise the declared reader memory limit.",
                )
                .map_err(E::from)?;
                cursor.source_advance_with_credit(
                    last.to_vec(),
                    &Self::physical_row_position(&position),
                    credit,
                );
                if row.is_some() {
                    return Ok(row);
                }
                continue;
            }
            let bound_credit = contextdb_core::read_memory::ReadCredit::try_new(
                read_memory.clone(),
                table.len().checked_add(72).ok_or_else(|| {
                    E::from(self.content_identity_corruption("manifest bound overflow"))
                })?,
                "reader",
                "version_scan_bound",
                "Raise the declared reader memory limit.",
            )
            .map_err(E::from)?;
            let mut start =
                content_identity::version_manifest::manifest_prefix(table).map_err(E::from)?;
            if let Some(after) = cursor.source_position() {
                if after.len() != 24 {
                    return Err(E::from(
                        self.content_identity_corruption("invalid physical cursor position"),
                    ));
                }
                start.extend_from_slice(&after[..8]);
                start.extend_from_slice(&[255; 57]);
            }
            let next = self.manifest_next(start, read_memory.clone(), before_touch)?;
            before_touch()?;
            let physical = match cursor.source_position() {
                Some(after) => scan
                    .relation
                    .range::<&[u8]>((std::ops::Bound::Excluded(after), std::ops::Bound::Unbounded))
                    .map_err(|error| RedbPersistence::relational_storage_error(table, error))
                    .map_err(E::from)?
                    .next()
                    .transpose()
                    .map_err(|error| RedbPersistence::relational_storage_error(table, error))
                    .map_err(E::from)?,
                None => scan
                    .relation
                    .first()
                    .map_err(|error| RedbPersistence::relational_storage_error(table, error))
                    .map_err(E::from)?,
            };
            let witness = next
                .as_ref()
                .map(|next| content_identity::version_manifest::parse_manifest(&next.coordinate))
                .transpose()
                .map_err(|e| E::from(self.content_identity_error(e)))?;
            let witness = witness.filter(|witness| witness.table == table);
            let Some(witness) = witness else {
                if physical.is_some() {
                    return Err(E::from(
                        self.content_identity_corruption("unwitnessed physical table suffix"),
                    ));
                }
                cursor.source_exhaust();
                return Ok(None);
            };
            if physical
                .as_ref()
                .is_none_or(|(key, _)| key.value() != witness.identity)
            {
                return Err(E::from(self.content_identity_corruption(
                    "physical scan gap disagrees with independent manifest expectation",
                )));
            }
            drop(physical);
            let row_id = RowId(u64::from_be_bytes(
                witness.identity[..8].try_into().expect("row id"),
            ));
            let mut last = None;
            let row = self.reconcile_relational_group(
                &scan._transaction,
                table,
                meta,
                row_id,
                snapshot,
                None,
                read_memory.clone(),
                before_touch,
                Some(&mut *observed),
                before_clone,
                &mut last,
            )?;
            let last = last.ok_or_else(|| {
                E::from(self.content_identity_corruption("expected row group had no members"))
            })?;
            let position = VersionedRow {
                row_id,
                values: HashMap::new(),
                created_tx: TxId(u64::from_be_bytes(last[8..16].try_into().expect("created"))),
                deleted_tx: None,
                lsn: Lsn(u64::from_be_bytes(last[16..].try_into().expect("lsn"))),
                created_at: None,
            };
            let position_bytes = last
                .len()
                .checked_add(
                    contextdb_relational::mem::BoundedPhysicalCursor::source_position_owner_bytes(),
                )
                .ok_or_else(|| {
                    E::from(self.content_identity_corruption("cursor position layout overflow"))
                })?;
            let position_credit = contextdb_core::read_memory::ReadCredit::try_new(
                read_memory.clone(),
                position_bytes,
                "reader",
                "version_scan_position",
                "Raise the declared reader memory limit.",
            )
            .map_err(E::from)?;
            cursor.source_advance_with_credit(
                last.to_vec(),
                &Self::physical_row_position(&position),
                position_credit,
            );
            drop(bound_credit);
            if row.is_some() {
                return Ok(row);
            }
        }
    }

    /// A version's identity without its values: all a physical cursor needs
    /// to resume after it.
    pub(super) fn physical_row_position(row: &VersionedRow) -> VersionedRow {
        VersionedRow {
            row_id: row.row_id,
            values: HashMap::new(),
            created_tx: row.created_tx,
            deleted_tx: row.deleted_tx,
            lsn: row.lsn,
            created_at: row.created_at,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn bounded_relational_ordered_next<E>(
        &self,
        table: &str,
        meta: &TableMeta,
        declaration: &IndexDecl,
        snapshot: SnapshotId,
        cursor: &mut contextdb_relational::mem::BoundedOrderedRowCursor,
        check_boundary: &mut dyn FnMut() -> std::result::Result<(), E>,
        before_touch: &mut dyn FnMut() -> std::result::Result<(), E>,
        before_clone: &mut dyn FnMut(
            crate::read_image_memory::RowReadAdmission<'_>,
        ) -> std::result::Result<(), E>,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> std::result::Result<Option<VersionedRow>, E>
    where
        E: From<Error>,
    {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.bounded_relational_ordered_next_observed(
            table,
            meta,
            declaration,
            snapshot,
            cursor,
            check_boundary,
            before_touch,
            &mut |_, _| Ok(()),
            before_clone,
            read_memory,
        )
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn bounded_relational_ordered_next_observed<E>(
        &self,
        table: &str,
        meta: &TableMeta,
        declaration: &IndexDecl,
        snapshot: SnapshotId,
        cursor: &mut contextdb_relational::mem::BoundedOrderedRowCursor,
        check_boundary: &mut dyn FnMut() -> std::result::Result<(), E>,
        before_touch: &mut dyn FnMut() -> std::result::Result<(), E>,
        observed: &mut dyn FnMut(&IndexKey, RowId) -> std::result::Result<(), E>,
        before_clone: &mut dyn FnMut(
            crate::read_image_memory::RowReadAdmission<'_>,
        ) -> std::result::Result<(), E>,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> std::result::Result<Option<VersionedRow>, E>
    where
        E: From<Error>,
    {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        let operation = crate::read_image_memory::ImageMemory::operation_reader(
            self.verified_ordered_runs.memory.accountant.clone(),
            read_memory.clone(),
        )
        .map_err(E::from)?;
        let _scope = operation.enter();
        check_boundary()?;
        self.verify_relational_source_roots(
            table,
            &declaration.name,
            IndexRootCheck::Physical,
            read_memory.clone(),
        )
        .map_err(E::from)?;
        self.historical_index_preflight(
            table,
            meta,
            declaration,
            snapshot,
            read_memory.clone(),
            before_touch,
        )?;
        if cursor.source_prefix_quota() == Some(0) {
            cursor.source_stop_prefix();
            return Ok(None);
        }
        loop {
            if cursor.source_identities_only() {
                loop {
                    let mut cached = cursor
                        .source_cache_key()
                        .and_then(|key| self.verified_ordered_runs.get(&key));
                    if cursor.source_cache_key().is_some() && cached.is_none() {
                        // The working window this cursor was part-way through
                        // was evicted. The rest of the range is still owed an
                        // authenticated answer, and the cursor already carries
                        // the last key it proved, so it continues from there -
                        // it neither re-proves the prefix it delivered nor
                        // finishes from the physical index, which would deliver
                        // whatever that index happens to hold.
                        cursor.source_abandon_cached_run();
                    }
                    if cached.is_none() {
                        let cache_key = self
                            .verified_ordered_run_key(table, &declaration.name, snapshot, cursor)
                            .map_err(E::from)?;
                        if let Some(run) = self.verified_ordered_runs.get(&cache_key) {
                            cursor.source_begin_cached_run(cache_key, false);
                            cached = Some(run);
                        } else if let Some(run) = self.build_verified_ordered_run(
                            table,
                            meta,
                            declaration,
                            snapshot,
                            cursor,
                            cache_key,
                            check_boundary,
                            before_touch,
                            read_memory.clone(),
                        )? {
                            cursor.source_begin_cached_run(cache_key, true);
                            cached = Some(run);
                        }
                    }
                    let Some(run) = cached else {
                        // The identities this route delivers are only sound
                        // because the authenticated window proves its span is
                        // complete. With too little proof memory to hold even
                        // one posting beside the descent there is no such
                        // proof, and the physical index below cannot supply
                        // one: a posting removed behind the proof's back would
                        // simply be a row nobody reports. Refuse with the
                        // existing typed budget error rather than answer from
                        // the physical index alone.
                        return Err(E::from(self.unprovable_ordered_range(&declaration.name)));
                    };
                    let delivered = run
                        .postings
                        .get(cursor.source_cached_position())
                        .map(|posting| posting.row_id);
                    if let Some(row_id) = delivered {
                        if !cursor.source_run_precharged() {
                            before_touch()?;
                        }
                        let source_key = run.postings[cursor.source_cached_position()]
                            .source_key
                            .clone();
                        cursor.source_advance_cached_position();
                        cursor.source_advance_cached_identity(&source_key, row_id);
                        // A cached posting is handed back here as well as by
                        // the bulk drain. Once the continuation key is
                        // recorded, a window the cache cannot keep is dropped
                        // so the next posting is proved again from that key.
                        #[cfg(any(test, feature = "test-seams"))]
                        if verified_ordered_run_test_seams::evict_after_cached_posting_for_test() {
                            drop(run);
                            self.verified_ordered_runs.clear_for_test();
                            self.clear_statement_proofs();
                            cursor.source_abandon_cached_run();
                        }
                        let identity = VersionedRow {
                            row_id,
                            values: HashMap::new(),
                            created_tx: TxId(0),
                            deleted_tx: None,
                            lsn: Lsn(0),
                            created_at: None,
                        };
                        before_clone(crate::read_image_memory::RowReadAdmission::Copy(
                            identity.estimated_bytes(),
                        ))?;
                        return Ok(Some(identity));
                    }
                    if run.more || run.quota_stopped {
                        // Drained, and the range continues: open the next
                        // window at the key this cursor last proved.
                        cursor.source_abandon_cached_run();
                        continue;
                    }
                    cursor.source_finish_run();
                    return Ok(None);
                }
            }
            let after = cursor.source_position().map(ToOwned::to_owned);
            let seek = cursor.source_seek().map(|key| key.to_vec());
            let reverse = cursor.source_reverse();
            check_boundary()?;
            before_touch()?;
            let next = self
                .with_read(|read| {
                    let name =
                        RedbPersistence::relational_index_table_name(table, &declaration.name);
                    let definition: TableDefinition<DurableRelationalIndexKey, &[u8]> =
                        TableDefinition::new(name.as_str());
                    let index = match read.open_table(definition) {
                        Ok(index) => index,
                        Err(redb::TableError::TableDoesNotExist(_)) => {
                            return Err(crate::funded_error::corruption_display(
                                Ok(read_memory.clone()),
                                self.database_path.display(),
                                format_args!(
                                    "relational index {} is absent from the retained root",
                                    declaration.name
                                ),
                            ));
                        }
                        Err(error) => return Err(RedbPersistence::storage_error(error)),
                    };
                    let entry = match (after, seek, reverse) {
                        (Some(after), _, true) => index
                            .range(..DurableRelationalIndexKey(after))
                            .map_err(RedbPersistence::storage_error)?
                            .next_back(),
                        (Some(after), _, false) => index
                            .range((
                                std::ops::Bound::Excluded(DurableRelationalIndexKey(after)),
                                std::ops::Bound::Unbounded,
                            ))
                            .map_err(RedbPersistence::storage_error)?
                            .next(),
                        (None, std::ops::Bound::Included(key), true) => index
                            .range(
                                ..=DurableRelationalIndexKey::new(
                                    key,
                                    RowId(u64::MAX),
                                    TxId(u64::MAX),
                                )?,
                            )
                            .map_err(RedbPersistence::storage_error)?
                            .next_back(),
                        (None, std::ops::Bound::Excluded(key), true) => index
                            .range(..DurableRelationalIndexKey::new(key, RowId(0), TxId(0))?)
                            .map_err(RedbPersistence::storage_error)?
                            .next_back(),
                        (None, std::ops::Bound::Included(key), false) => index
                            .range(DurableRelationalIndexKey::new(key, RowId(0), TxId(0))?..)
                            .map_err(RedbPersistence::storage_error)?
                            .next(),
                        (None, std::ops::Bound::Excluded(key), false) => index
                            .range((
                                std::ops::Bound::Excluded(DurableRelationalIndexKey::new(
                                    key,
                                    RowId(u64::MAX),
                                    TxId(u64::MAX),
                                )?),
                                std::ops::Bound::Unbounded,
                            ))
                            .map_err(RedbPersistence::storage_error)?
                            .next(),
                        (None, std::ops::Bound::Unbounded, true) => {
                            return decode_relational_index_entry(
                                &self.database_path,
                                index.last().map_err(RedbPersistence::storage_error)?,
                            );
                        }
                        (None, std::ops::Bound::Unbounded, false) => {
                            return decode_relational_index_entry(
                                &self.database_path,
                                index.first().map_err(RedbPersistence::storage_error)?,
                            );
                        }
                    };
                    decode_relational_index_entry(
                        &self.database_path,
                        entry.transpose().map_err(RedbPersistence::storage_error)?,
                    )
                })
                .map_err(E::from)?;
            let Some((source_key, position, deleted_tx)) = next else {
                cursor.source_finish_run();
                return Ok(None);
            };
            observed(&position.key, position.row_id)?;
            RedbPersistence::validated_index_position(declaration, meta, &position)
                .map_err(|reason| E::from(self.content_identity_corruption(reason)))?;
            if !cursor.source_inside_run(&position.key) {
                cursor.source_finish_run();
                return Ok(None);
            }
            cursor.source_advance(source_key, position.key.clone(), position.row_id);
            if position.created_tx.0 > snapshot.0
                || deleted_tx.is_some_and(|deleted| deleted.0 <= snapshot.0)
            {
                continue;
            }
            if cursor.source_identities_only() {
                let row = self
                    .with_read(|read| {
                        self.row_identity_by_id_in_read_with_read_memory(
                            read,
                            table,
                            meta,
                            position.row_id,
                            snapshot,
                            None,
                            read_memory.clone(),
                        )
                    })
                    .map_err(E::from)?;
                let Some((row, memory)) = row else {
                    return Err(E::from(self.content_identity_corruption(format!(
                        "durable relational index {} names a missing visible row {}",
                        declaration.name, position.row_id.0
                    ))));
                };
                let actual =
                    contextdb_relational::index_key_for_row(&declaration.columns, &row.values);
                if actual != position.key {
                    return Err(E::from(self.content_identity_corruption(format!(
                        "durable relational index {} points row {} at a different key",
                        declaration.name, position.row_id.0
                    ))));
                }
                let identity = VersionedRow {
                    row_id: row.row_id,
                    values: HashMap::new(),
                    created_tx: row.created_tx,
                    deleted_tx: None,
                    lsn: row.lsn,
                    created_at: row.created_at,
                };
                drop(row);
                drop(memory);
                before_clone(crate::read_image_memory::RowReadAdmission::Copy(
                    identity.estimated_bytes(),
                ))?;
                return Ok(Some(identity));
            }
            let row = self
                .with_read(|read| {
                    self.row_by_id_in_read_with_read_memory(
                        read,
                        table,
                        meta,
                        position.row_id,
                        snapshot,
                        read_memory.clone(),
                    )
                })
                .map_err(E::from)?;
            let Some((row, memory)) = row else {
                return Err(E::from(self.content_identity_corruption(format!(
                    "durable relational index {} names a missing visible row {}",
                    declaration.name, position.row_id.0
                ))));
            };
            let actual = contextdb_relational::index_key_for_row(&declaration.columns, &row.values);
            if actual != position.key {
                return Err(E::from(crate::funded_error::corruption_display(
                    Ok(read_memory.clone()),
                    self.database_path.display(),
                    format_args!(
                        "durable relational index {} points row {} at a different key",
                        declaration.name, position.row_id.0
                    ),
                )));
            }
            before_clone(crate::read_image_memory::RowReadAdmission::Move(&memory))?;
            return Ok(Some(row));
        }
    }

    #[allow(clippy::too_many_arguments)]
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn bounded_relational_exact_next<E>(
        &self,
        table: &str,
        meta: &TableMeta,
        declaration: &IndexDecl,
        snapshot: SnapshotId,
        cursor: &mut contextdb_relational::mem::BoundedExactIndexCursor,
        before_storage_step: &mut dyn FnMut() -> std::result::Result<(), E>,
        before_touch: &mut dyn FnMut() -> std::result::Result<(), E>,
        before_clone: &mut dyn FnMut(
            crate::read_image_memory::RowReadAdmission<'_>,
        ) -> std::result::Result<(), E>,
        read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> std::result::Result<Option<VersionedRow>, E>
    where
        E: From<Error>,
    {
        let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

        self.historical_index_preflight(
            table,
            meta,
            declaration,
            snapshot,
            read_memory.clone(),
            before_storage_step,
        )?;
        loop {
            let decode_owner = crate::read_image_memory::ImageMemory::operation_reader(
                self._source_memory.memory.accountant.clone(),
                read_memory.clone(),
            )
            .map_err(E::from)?;
            let _decode_scope = decode_owner.enter();
            let Some(probe) = cursor.source_probe().cloned() else {
                return Ok(None);
            };
            let after = cursor.source_position().map(ToOwned::to_owned);
            let next = self
                .with_read(|read| {
                    let name =
                        RedbPersistence::relational_index_table_name(table, &declaration.name);
                    let definition: TableDefinition<DurableRelationalIndexKey, &[u8]> =
                        TableDefinition::new(name.as_str());
                    let index = match read.open_table(definition) {
                        Ok(index) => index,
                        Err(redb::TableError::TableDoesNotExist(_)) => {
                            return Err(crate::funded_error::corruption_display(
                                Ok(read_memory.clone()),
                                self.database_path.display(),
                                format_args!(
                                    "relational index {} is absent from the retained root",
                                    declaration.name
                                ),
                            ));
                        }
                        Err(error) => return Err(RedbPersistence::storage_error(error)),
                    };
                    let entry = match after {
                        Some(after) => index
                            .range((
                                std::ops::Bound::Excluded(DurableRelationalIndexKey(after)),
                                std::ops::Bound::Unbounded,
                            ))
                            .map_err(RedbPersistence::storage_error)?
                            .next(),
                        None => index
                            .range(
                                DurableRelationalIndexKey::new(probe.clone(), RowId(0), TxId(0))?..,
                            )
                            .map_err(RedbPersistence::storage_error)?
                            .next(),
                    };
                    decode_relational_index_entry(
                        &self.database_path,
                        entry.transpose().map_err(RedbPersistence::storage_error)?,
                    )
                })
                .map_err(E::from)?;
            let Some((source_key, position, deleted_tx)) = next else {
                cursor.source_finish_probe();
                return Ok(None);
            };
            RedbPersistence::validated_index_position(declaration, meta, &position)
                .map_err(|reason| E::from(self.content_identity_corruption(reason)))?;
            if position.key != probe {
                cursor.source_finish_probe();
                return Ok(None);
            }
            before_touch()?;
            cursor.source_advance(source_key, position.row_id);
            if position.created_tx.0 > snapshot.0
                || deleted_tx.is_some_and(|deleted| deleted.0 <= snapshot.0)
            {
                continue;
            }
            if cursor.source_identities_only() {
                let row = VersionedRow {
                    row_id: position.row_id,
                    values: HashMap::new(),
                    created_tx: position.created_tx,
                    deleted_tx: None,
                    lsn: Lsn(0),
                    created_at: None,
                };
                before_clone(crate::read_image_memory::RowReadAdmission::Copy(
                    row.estimated_bytes(),
                ))?;
                return Ok(Some(row));
            }
            let row = self
                .with_read(|read| {
                    self.row_by_id_in_read_with_read_memory(
                        read,
                        table,
                        meta,
                        position.row_id,
                        snapshot,
                        read_memory.clone(),
                    )
                })
                .map_err(E::from)?;
            let Some((row, memory)) = row else {
                return Err(E::from(self.content_identity_corruption(format!(
                    "durable relational index {} names a missing visible row {}",
                    declaration.name, position.row_id.0
                ))));
            };
            if contextdb_relational::index_key_for_row(&declaration.columns, &row.values) != probe {
                return Err(E::from(crate::funded_error::corruption_display(
                    Ok(read_memory.clone()),
                    self.database_path.display(),
                    format_args!(
                        "durable relational index {} points row {} at a different key",
                        declaration.name, position.row_id.0
                    ),
                )));
            }
            before_clone(crate::read_image_memory::RowReadAdmission::Move(&memory))?;
            return Ok(Some(row));
        }
    }
}
