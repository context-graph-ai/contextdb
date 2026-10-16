//! Physical writer ownership is independent of a redb allocator/cache generation.

use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Weak};

#[derive(Clone, Copy, Debug)]
pub(super) struct DiskGrowthRefusal {
    pub(super) current_bytes: u64,
    pub(super) projected_bytes: u64,
    pub(super) limit_bytes: u64,
}

impl std::fmt::Display for DiskGrowthRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "projected storage growth from {} to {} bytes exceeds the {} byte limit",
            self.current_bytes, self.projected_bytes, self.limit_bytes
        )
    }
}

impl std::error::Error for DiskGrowthRefusal {}

thread_local! {
    static LAST_DISK_GROWTH_REFUSAL: std::cell::Cell<Option<DiskGrowthRefusal>> = const {
        std::cell::Cell::new(None)
    };
    static LAST_READER_COORDINATION_REFUSAL: std::cell::RefCell<Option<PathBuf>> = const {
        std::cell::RefCell::new(None)
    };
}

pub(super) fn take_disk_growth_refusal() -> Option<DiskGrowthRefusal> {
    LAST_DISK_GROWTH_REFUSAL.take()
}

pub(super) fn take_reader_coordination_refusal() -> Option<PathBuf> {
    LAST_READER_COORDINATION_REFUSAL.with(|slot| slot.borrow_mut().take())
}

/// A provider scan met a trusted, occupied registry lock. Carry this marker
/// inside the storage I/O error instead of classifying every WouldBlock as busy.
#[derive(Debug)]
struct ReaderRegistryScanContention;

impl std::fmt::Display for ReaderRegistryScanContention {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("reader snapshot registry scan is held by another operation")
    }
}

impl std::error::Error for ReaderRegistryScanContention {}

pub(super) fn is_reader_registry_scan_contention(error: &redb::TransactionError) -> bool {
    matches!(error,
        redb::TransactionError::Storage(redb::StorageError::Io(error))
            if error.get_ref().is_some_and(|cause| cause.is::<ReaderRegistryScanContention>())
    )
}

/// The sole owner of the data-file lock. It is exclusive while writing and
/// shared only after the database has sealed its committed root for drain.
/// Backend views never unlock it.
pub(super) struct LockedWriterFile {
    file: File,
    runtime_dir: Option<PathBuf>,
    #[cfg(unix)]
    identity: (u64, u64),
    reader_snapshots: std::sync::OnceLock<super::ReaderSnapshotRegistry>,
    reader_database_identity:
        std::sync::OnceLock<Option<contextdb_core::read_contract::DatabaseIdentity>>,
    owner_cursor_database_identity:
        std::sync::OnceLock<contextdb_core::read_contract::DatabaseIdentity>,
    disk_limit: std::sync::atomic::AtomicU64,
    projected_growth_bytes: std::sync::Mutex<u64>,
    #[cfg(any(test, feature = "test-seams"))]
    live_views: std::sync::atomic::AtomicUsize,
}

impl LockedWriterFile {
    /// Consume a descriptor already locked by the writer admission path.
    pub(super) fn from_locked(file: File, runtime_dir: Option<&Path>) -> io::Result<Arc<Self>> {
        #[cfg(unix)]
        let identity = {
            use std::os::unix::fs::MetadataExt as _;
            let metadata = file.metadata()?;
            (metadata.dev(), metadata.ino())
        };
        Ok(Arc::new(Self {
            file,
            runtime_dir: runtime_dir.map(Path::to_path_buf),
            #[cfg(unix)]
            identity,
            reader_snapshots: std::sync::OnceLock::new(),
            reader_database_identity: std::sync::OnceLock::new(),
            owner_cursor_database_identity: std::sync::OnceLock::new(),
            disk_limit: std::sync::atomic::AtomicU64::new(0),
            projected_growth_bytes: std::sync::Mutex::new(0),
            #[cfg(any(test, feature = "test-seams"))]
            live_views: std::sync::atomic::AtomicUsize::new(0),
        }))
    }

    pub(super) fn lock(
        file: File,
        runtime_dir: Option<&Path>,
    ) -> Result<Arc<Self>, redb::DatabaseError> {
        match fs2::FileExt::try_lock_exclusive(&file) {
            Ok(()) => Self::from_locked(file, runtime_dir).map_err(Into::into),
            Err(error)
                if error.kind() == io::ErrorKind::WouldBlock
                    || error.raw_os_error() == fs2::lock_contended_error().raw_os_error() =>
            {
                Err(redb::DatabaseError::DatabaseAlreadyOpen)
            }
            Err(error) => Err(error.into()),
        }
    }

    pub(super) fn view(self: &Arc<Self>) -> WriterFileView {
        #[cfg(any(test, feature = "test-seams"))]
        self.live_views
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        WriterFileView {
            owner: Arc::downgrade(self),
            #[cfg(any(test, feature = "test-seams"))]
            closed: std::sync::atomic::AtomicBool::new(false),
        }
    }

    #[cfg(unix)]
    pub(super) fn identity(&self) -> (u64, u64) {
        self.identity
    }

    pub(super) fn retain_reader_snapshots(
        &self,
        snapshots: super::ReaderSnapshotRegistry,
        database_identity: Option<contextdb_core::read_contract::DatabaseIdentity>,
    ) -> io::Result<()> {
        self.reader_database_identity
            .set(database_identity)
            .map_err(|_| {
                io::Error::other("writer reader-snapshot database identity was already captured")
            })?;
        self.reader_snapshots
            .set(snapshots)
            .map_err(|_| io::Error::other("writer reader-snapshot registry was already captured"))
    }

    pub(super) fn admit_owner_cursor_identity(
        &self,
        identity: contextdb_core::read_contract::DatabaseIdentity,
    ) -> io::Result<()> {
        let existing = self.reader_database_identity.get().copied().flatten();
        if existing.is_some_and(|existing| existing != identity) {
            return Err(io::Error::other(
                "owner cursor database identity differs from admitted writer",
            ));
        }
        let published = self.owner_cursor_database_identity.get_or_init(|| identity);
        if *published != identity {
            return Err(io::Error::other("owner cursor database identity changed"));
        }
        Ok(())
    }

    pub(super) fn set_disk_limit(&self, limit: Option<u64>) -> io::Result<()> {
        let _reservation = self
            .projected_growth_bytes
            .lock()
            .map_err(|_| io::Error::other("projected file-growth reservation was poisoned"))?;
        self.disk_limit
            .store(limit.unwrap_or(0), std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }

    /// The owning database has stopped every writer and reclaimer. Convert
    /// this descriptor's exclusive flock to a shared one atomically so a
    /// fresh direct reader can load the committed root during owner drain.
    /// The separate companion lock still excludes another ContextDB writer.
    pub(super) fn make_quiescent(&self) -> io::Result<()> {
        fs2::FileExt::lock_shared(&self.file)
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub(super) fn projected_growth_bytes(&self) -> u64 {
        *self.projected_growth_bytes.lock().unwrap()
    }

    fn storage_snapshot_identities(
        &self,
        registry: super::ReaderSnapshotRegistry,
    ) -> io::Result<Vec<redb::StorageSnapshotIdentity>> {
        match registry {
            super::ReaderSnapshotRegistry::Complete(snapshots) => {
                #[cfg(target_os = "linux")]
                let unknown = super::reader_presence::has_unknown(
                    &self.file,
                    snapshots
                        .iter()
                        .filter(|snapshot| self.recognized_reader_snapshot(snapshot))
                        .map(|snapshot| snapshot.handle_identity),
                )
                .inspect_err(|_| {
                    LAST_READER_COORDINATION_REFUSAL.with(|slot| {
                        *slot.borrow_mut() = Some(self.file_path());
                    });
                })?;
                let mut identities: Vec<_> = snapshots
                    .into_iter()
                    .filter(|snapshot| self.recognized_reader_snapshot(snapshot))
                    .map(|snapshot| snapshot.storage_snapshot)
                    .collect();
                #[cfg(target_os = "linux")]
                if unknown {
                    // A root unknown to this writer may hold any old page.
                    // Ordinary commits remain possible but cannot reclaim it.
                    identities.push(redb::StorageSnapshotIdentity {
                        transaction_id: 0,
                        user_root: None,
                    });
                }
                #[cfg(all(unix, not(target_os = "linux")))]
                if super::default_reader_runtime_directory().is_none() {
                    // Without OFD locks or a common platform root, an
                    // unconfigured reader root cannot be ruled out.
                    identities.push(redb::StorageSnapshotIdentity {
                        transaction_id: 0,
                        user_root: None,
                    });
                }
                Ok(identities)
            }
            super::ReaderSnapshotRegistry::Contended(path) => {
                LAST_READER_COORDINATION_REFUSAL.with(|slot| {
                    *slot.borrow_mut() = Some(path);
                });
                Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    ReaderRegistryScanContention,
                ))
            }
            super::ReaderSnapshotRegistry::Indeterminate(path) => {
                LAST_READER_COORDINATION_REFUSAL.with(|slot| {
                    *slot.borrow_mut() = Some(path);
                });
                Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "authenticated reader snapshot registry is indeterminate",
                ))
            }
            super::ReaderSnapshotRegistry::Unavailable => {
                // No candidate directory exists. A regular file supplied as a
                // channel directory cannot hold reader registrations, and
                // channel startup failure does not veto ordinary store work.
                #[cfg(target_os = "linux")]
                if super::reader_presence::has_unknown(&self.file, std::iter::empty()).inspect_err(
                    |_| {
                        LAST_READER_COORDINATION_REFUSAL.with(|slot| {
                            *slot.borrow_mut() = Some(self.file_path());
                        });
                    },
                )? {
                    return Ok(vec![redb::StorageSnapshotIdentity {
                        transaction_id: 0,
                        user_root: None,
                    }]);
                }
                #[cfg(all(unix, not(target_os = "linux")))]
                if super::default_reader_runtime_directory().is_none() {
                    return Ok(vec![redb::StorageSnapshotIdentity {
                        transaction_id: 0,
                        user_root: None,
                    }]);
                }
                Ok(Vec::new())
            }
        }
    }

    #[cfg(target_os = "linux")]
    fn file_path(&self) -> PathBuf {
        std::fs::read_link(format!(
            "/proc/self/fd/{}",
            std::os::fd::AsRawFd::as_raw_fd(&self.file)
        ))
        .unwrap_or_else(|_| PathBuf::from("database file"))
    }

    /// macOS has no /proc; the kernel reports the open file's path through
    /// `F_GETPATH`, as raw bytes that need not be UTF-8.
    #[cfg(target_os = "macos")]
    fn file_path(&self) -> PathBuf {
        use std::os::fd::AsRawFd as _;
        use std::os::unix::ffi::OsStrExt as _;

        let mut buffer = [0_u8; libc::PATH_MAX as usize];
        let made = unsafe {
            libc::fcntl(
                self.file.as_raw_fd(),
                libc::F_GETPATH,
                buffer.as_mut_ptr().cast::<libc::c_char>(),
            )
        };
        if made < 0 {
            return PathBuf::from("database file");
        }
        let length = buffer
            .iter()
            .position(|byte| *byte == 0)
            .unwrap_or(buffer.len());
        PathBuf::from(std::ffi::OsStr::from_bytes(&buffer[..length]))
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    fn file_path(&self) -> PathBuf {
        PathBuf::from("database file")
    }

    fn recognized_reader_snapshot(&self, snapshot: &super::RegisteredReaderSnapshot) -> bool {
        snapshot.database_identity == self.reader_database_identity.get().copied().flatten()
            || self
                .owner_cursor_database_identity
                .get()
                .is_some_and(|identity| snapshot.database_identity == Some(*identity))
    }

    #[cfg(target_os = "linux")]
    pub(super) fn unknown_reader_presence(
        &self,
        registry: &super::ReaderSnapshotRegistry,
    ) -> io::Result<bool> {
        match registry {
            super::ReaderSnapshotRegistry::Complete(snapshots) => {
                super::reader_presence::has_unknown(
                    &self.file,
                    snapshots
                        .iter()
                        .filter(|snapshot| self.recognized_reader_snapshot(snapshot))
                        .map(|snapshot| snapshot.handle_identity),
                )
            }
            super::ReaderSnapshotRegistry::Unavailable => {
                super::reader_presence::has_unknown(&self.file, std::iter::empty())
            }
            super::ReaderSnapshotRegistry::Contended(path) => Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                format!(
                    "candidate reader registry scan is contended: {}",
                    path.display()
                ),
            )),
            super::ReaderSnapshotRegistry::Indeterminate(path) => Err(io::Error::other(format!(
                "candidate reader registry is indeterminate: {}",
                path.display()
            ))),
        }
    }

    pub(super) fn external_snapshot_provider(self: &Arc<Self>) -> redb::ExternalSnapshotProvider {
        let owner = Arc::downgrade(self);
        Arc::new(move || {
            LAST_READER_COORDINATION_REFUSAL.with(|slot| *slot.borrow_mut() = None);
            let owner = owner.upgrade().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotConnected,
                    "writer file ownership is closed",
                )
            })?;
            #[cfg(unix)]
            let registry = super::capture_reader_snapshot_registry(
                owner.identity,
                owner.runtime_dir.as_deref(),
            );
            #[cfg(not(unix))]
            let registry = super::ReaderSnapshotRegistry::Unavailable;
            owner.storage_snapshot_identities(registry)
        })
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub(super) fn reader_snapshots(&self) -> Option<&super::ReaderSnapshotRegistry> {
        self.reader_snapshots.get()
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub(super) fn observe(
        self: &Arc<Self>,
        boundary: super::read_persistence_test_scaffold::WriterFileRecycleBoundary,
    ) {
        use super::read_persistence_test_scaffold::{
            WriterFileLifetime, WriterFileRecycleObservation, note_writer_file_recycle_for_test,
        };
        note_writer_file_recycle_for_test(WriterFileRecycleObservation {
            boundary,
            #[cfg(unix)]
            identity: {
                use std::os::unix::fs::MetadataExt as _;
                let metadata = self
                    .file
                    .metadata()
                    .expect("observe the held writer descriptor");
                (metadata.dev(), metadata.ino())
            },
            live_views: self.live_views.load(std::sync::atomic::Ordering::Relaxed),
            owner: WriterFileLifetime(Arc::downgrade(self)),
        });
    }
}

impl std::fmt::Debug for LockedWriterFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut state = f.debug_struct("LockedWriterFile");
        state.field("file", &self.file);
        #[cfg(unix)]
        state.field("identity", &self.identity);
        state.finish_non_exhaustive()
    }
}

impl Drop for LockedWriterFile {
    fn drop(&mut self) {
        // The outer holder closes redb before releasing this owner. Any I/O
        // already in progress keeps its temporary strong borrow until it returns.
        // File's own drop also releases the descriptor if explicit unlock fails.
        let _ = fs2::FileExt::unlock(&self.file);
    }
}

#[derive(Debug)]
pub(super) struct WriterFileView {
    owner: Weak<LockedWriterFile>,
    #[cfg(any(test, feature = "test-seams"))]
    closed: std::sync::atomic::AtomicBool,
}

impl WriterFileView {
    fn owner(&self) -> io::Result<Arc<LockedWriterFile>> {
        self.owner
            .upgrade()
            .ok_or_else(|| io::Error::other("writer file ownership is closed"))
    }

    #[cfg(any(test, feature = "test-seams"))]
    fn note_closed(&self) {
        if !self.closed.swap(true, std::sync::atomic::Ordering::Relaxed)
            && let Some(owner) = self.owner.upgrade()
        {
            owner
                .live_views
                .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        }
    }
}

#[cfg(any(test, feature = "test-seams"))]
impl Drop for WriterFileView {
    fn drop(&mut self) {
        self.note_closed();
    }
}

impl redb::StorageBackend for WriterFileView {
    fn len(&self) -> io::Result<u64> {
        self.owner()?.file.metadata().map(|metadata| metadata.len())
    }

    #[cfg(unix)]
    fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        use std::os::unix::fs::FileExt as _;
        self.owner()?.file.read_exact_at(out, offset)
    }

    #[cfg(windows)]
    fn read(&self, mut offset: u64, mut out: &mut [u8]) -> io::Result<()> {
        use std::os::windows::fs::FileExt as _;
        let owner = self.owner()?;
        while !out.is_empty() {
            match owner.file.seek_read(out, offset) {
                Ok(0) => return Err(io::ErrorKind::UnexpectedEof.into()),
                Ok(count) => {
                    offset += count as u64;
                    out = &mut out[count..];
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        LAST_DISK_GROWTH_REFUSAL.set(None);
        let owner = self.owner()?;
        let mut reserved = owner
            .projected_growth_bytes
            .lock()
            .map_err(|_| io::Error::other("projected file-growth reservation was poisoned"))?;
        let current = owner.file.metadata()?.len();
        *reserved = len.saturating_sub(current);
        let result = if *reserved > 0 {
            let limit = owner.disk_limit.load(std::sync::atomic::Ordering::SeqCst);
            if limit != 0 && len > limit {
                let refusal = DiskGrowthRefusal {
                    current_bytes: current,
                    projected_bytes: len,
                    limit_bytes: limit,
                };
                LAST_DISK_GROWTH_REFUSAL.set(Some(refusal));
                Err(io::Error::new(io::ErrorKind::QuotaExceeded, refusal))
            } else {
                owner.file.set_len(len)
            }
        } else {
            owner.file.set_len(len)
        };
        *reserved = 0;
        result
    }

    fn sync_data(&self) -> io::Result<()> {
        self.owner()?.file.sync_data()
    }

    #[cfg(unix)]
    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        use std::os::unix::fs::FileExt as _;
        self.owner()?.file.write_all_at(data, offset)
    }

    #[cfg(windows)]
    fn write(&self, mut offset: u64, mut data: &[u8]) -> io::Result<()> {
        use std::os::windows::fs::FileExt as _;
        let owner = self.owner()?;
        while !data.is_empty() {
            match owner.file.seek_write(data, offset) {
                Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
                Ok(count) => {
                    offset += count as u64;
                    data = &data[count..];
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    fn close(&self) -> io::Result<()> {
        // The view owns no descriptor or lock. A closed redb transaction may
        // retain its cache object; it must not retain physical writer ownership.
        #[cfg(any(test, feature = "test-seams"))]
        self.note_closed();
        Ok(())
    }
}

/// Field order also preserves flush-before-unlock when construction unwinds.
pub(super) struct OpenedWriterDatabase {
    pub(super) database: redb::Database,
    pub(super) owner: Arc<LockedWriterFile>,
}
