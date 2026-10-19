#![allow(dead_code)] // Existing compatibility and test seam shape.
//! Crash-recoverable, physical-store publication fencing for authoritative erasure.

use super::{
    CoordinationWatch, CoordinationWatchFailure, RangeLockMode, RangeLockOutcome, announce_companion_release,
    block_on_companion_range_lock, create_reader_breadcrumb_directory, reader_breadcrumb_root,
    reader_snapshot_directory, release_companion_range_lock, take_companion_range_lock_shared,
    validate_reader_breadcrumb_directory, validate_reader_runtime_directory,
};
use contextdb_core::read_contract::DatabaseIdentity;
use contextdb_core::{Error, Lsn, Result};
use std::collections::BTreeSet;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const MAGIC: &[u8] = b"contextdb-purge-frontier";
const VERSION: u16 = 2;
const CONTROL_SLOT_BYTES: u64 = 224;
const CONTROL_SLOT_COUNT: u64 = 3;
const DATA_OFFSET: u64 = CONTROL_SLOT_BYTES * CONTROL_SLOT_COUNT;
const BUCKET_BYTES: u64 = 88;
const INITIAL_CAPACITY: u64 = 64;
const PUBLICATION_LOCK_OFFSET: u64 = 1 << 40;
const PUBLICATION_ADMISSION_LOCK_OFFSET: u64 = PUBLICATION_LOCK_OFFSET + 1;
const WILDCARD_TABLE: &[u8] = b"contextdb-purge-publication-any-table";

#[derive(Debug, Clone, PartialEq, Eq)]
struct ClearState {
    sequence: u64,
    generation: u64,
    frontier: Lsn,
    index_offset: u64,
    capacity: u64,
    count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PendingState {
    sequence: u64,
    generation: u64,
    frontier: Lsn,
    token: [u8; 16],
    prior: ClearState,
    index_offset: u64,
    capacity: u64,
    count: u64,
    manifest_offset: u64,
    manifest_len: u64,
    manifest_hash: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum State {
    Clear(ClearState),
    Pending(PendingState),
}

impl State {
    fn sequence(&self) -> u64 {
        match self {
            Self::Clear(state) => state.sequence,
            Self::Pending(state) => state.sequence,
        }
    }
}

pub(crate) struct Fence {
    file: File,
    path: PathBuf,
    reader_baseline: Option<u64>,
    writer_acquired: bool,
}

impl Fence {
    /// Take this reader's baseline again, at the instant its exact root is
    /// captured, without waiting: while the reader holds the store's data lock
    /// no writer can be publishing a purge, and while its provisional
    /// registration exists no cleanup can be removing the fence, so a refused
    /// try is an invariant failure, answered as one.
    pub(crate) fn rebaseline_without_waiting(&mut self) -> std::io::Result<()> {
        acquire_reader_lock(
            &self.file,
            PUBLICATION_ADMISSION_LOCK_OFFSET,
            RangeLockMode::Shared,
            true,
        )?;
        if let Err(error) = acquire_reader_lock(
            &self.file,
            PUBLICATION_LOCK_OFFSET,
            RangeLockMode::Shared,
            true,
        ) {
            release_companion_range_lock(&self.file, PUBLICATION_ADMISSION_LOCK_OFFSET);
            return Err(error);
        }
        let readable = read_latest(&mut self.file);
        release_companion_range_lock(&self.file, PUBLICATION_LOCK_OFFSET);
        release_companion_range_lock(&self.file, PUBLICATION_ADMISSION_LOCK_OFFSET);
        self.reader_baseline = Some(match readable? {
            State::Clear(clear) => clear.generation,
            State::Pending(pending) => pending.prior.generation,
        });
        Ok(())
    }

    /// Whether this fence's path still names the inode this descriptor holds.
    /// A last departing reader may have removed a Clear fence after this one
    /// was opened; a registration published against a departed fence would
    /// never see a later publication, so it reopens instead.
    pub(crate) fn path_still_names(&self) -> bool {
        match (self.file.metadata(), std::fs::symlink_metadata(&self.path)) {
            (Ok(held), Ok(named)) => trusted(&named) && same_inode(&held, &named),
            _ => false,
        }
    }

    /// A continuation of the same physical reader keeps the earliest
    /// publication generation it observed for this registry. A purge that
    /// commits while the exact-root registration is installed must not become
    /// the continuation's new baseline.
    pub(crate) fn inherit_reader_baseline(&mut self, prior: &Self) {
        if self.path == prior.path
            && let Some(baseline) = prior.reader_baseline
        {
            self.reader_baseline = Some(
                self.reader_baseline
                    .map_or(baseline, |current| current.min(baseline)),
            );
        }
    }
}

pub(crate) struct PreparedPublication {
    fence: Fence,
    prior: ClearState,
    pending: PendingState,
    tables: Vec<[u8; 32]>,
    resolved: bool,
}

/// Physical exclusion is independent of whether durable reader fences exist.
pub(crate) struct AcquiredPurgePublication {
    pub(super) fences: Option<FenceSet>,
    #[cfg(target_os = "linux")]
    pub(super) physical: super::writer_file::ReaderAudit,
}

pub(crate) struct PreparedPurgePublication {
    fences: Option<PreparedPublicationSet>,
    #[cfg(target_os = "linux")]
    _physical: super::writer_file::ReaderAudit,
}

impl AcquiredPurgePublication {
    pub(crate) fn prepare(
        self,
        frontier: Lsn,
        tables: &BTreeSet<String>,
    ) -> Result<PreparedPurgePublication> {
        let fences = self
            .fences
            .map(|fences| fences.prepare(frontier, tables))
            .transpose()?;
        Ok(PreparedPurgePublication {
            fences,
            #[cfg(target_os = "linux")]
            _physical: self.physical,
        })
    }
}
impl PreparedPurgePublication {
    pub(crate) fn marker(&self) -> Option<DurableMarker> {
        self.fences.as_ref().map(PreparedPublicationSet::marker)
    }
    pub(crate) fn commit(self) -> Result<()> {
        if let Some(fences) = self.fences {
            fences.commit()?;
        }
        Ok(())
    }
    pub(crate) fn abort(self) -> Result<()> {
        if let Some(fences) = self.fences {
            fences.abort()?;
        }
        Ok(())
    }
}

pub(crate) struct FenceSet(pub(crate) Vec<Fence>);

pub(crate) struct PreparedPublicationSet(Vec<PreparedPublication>);

impl FenceSet {
    pub(crate) fn prepare(
        self,
        frontier: Lsn,
        tables: &BTreeSet<String>,
    ) -> Result<PreparedPublicationSet> {
        let token = uuid::Uuid::new_v4().into_bytes();
        let mut prepared = Vec::new();
        for fence in self.0 {
            let path = fence.path.display().to_string();
            match fence.prepare_with_token(frontier, tables, token) {
                Ok(publication) => prepared.push(publication),
                Err(error) => {
                    for publication in prepared {
                        let _ = publication.abort();
                    }
                    return Err(Error::ReaderCoordinationUnavailable {
                        operation: "authoritative purge".to_owned(),
                        path,
                        reason: error.to_string(),
                    });
                }
            }
        }
        Ok(PreparedPublicationSet(prepared))
    }
}

impl PreparedPublicationSet {
    pub(crate) fn marker(&self) -> DurableMarker {
        self.0
            .first()
            .expect("a prepared set contains a fence")
            .marker()
    }

    pub(crate) fn commit(self) -> Result<()> {
        for publication in self.0 {
            let path = publication.fence.path.display().to_string();
            publication
                .commit()
                .map_err(|error| Error::ReaderCoordinationUnavailable {
                    operation: "authoritative purge".to_owned(),
                    path,
                    reason: error.to_string(),
                })?;
        }
        Ok(())
    }

    pub(crate) fn abort(self) -> Result<()> {
        for publication in self.0 {
            let path = publication.fence.path.display().to_string();
            publication
                .abort()
                .map_err(|error| Error::ReaderCoordinationUnavailable {
                    operation: "authoritative purge".to_owned(),
                    path,
                    reason: error.to_string(),
                })?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DurableMarker {
    pub(crate) token: [u8; 16],
    pub(crate) frontier: Lsn,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Bucket {
    table: [u8; 32],
    generation: u64,
    frontier: Lsn,
}

fn table_identity(table: &str) -> [u8; 32] {
    *blake3::hash(table.as_bytes()).as_bytes()
}

fn wildcard_identity() -> [u8; 32] {
    *blake3::hash(WILDCARD_TABLE).as_bytes()
}

fn path(runtime_root: &Path, identity: (u64, u64), database_identity: DatabaseIdentity) -> PathBuf {
    reader_snapshot_directory(runtime_root, identity).join(format!(
        "purge-publication-{}.fence",
        uuid::Uuid::from_bytes(database_identity.0).simple()
    ))
}

fn sync_directory(path: &Path) -> std::io::Result<()> {
    File::open(path)?.sync_all()
}

#[cfg(unix)]
fn trusted(metadata: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt as _;
    metadata.file_type().is_file()
        && metadata.uid() == nix::unistd::Uid::effective().as_raw()
        && metadata.mode() & 0o7777 == 0o600
        && metadata.nlink() == 1
}

#[cfg(not(unix))]
fn trusted(_metadata: &std::fs::Metadata) -> bool {
    false
}

#[cfg(unix)]
fn same_inode(left: &std::fs::Metadata, right: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt as _;
    left.dev() == right.dev() && left.ino() == right.ino() && left.uid() == right.uid()
}

#[cfg(not(unix))]
fn same_inode(_left: &std::fs::Metadata, _right: &std::fs::Metadata) -> bool {
    false
}

fn open_existing(path: &Path) -> std::io::Result<File> {
    let before = std::fs::symlink_metadata(path)?;
    if !trusted(&before) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "purge publication fence is not a trusted private regular file",
        ));
    }
    let mut options = OpenOptions::new();
    options.read(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    }
    let file = options.open(path)?;
    let after = file.metadata()?;
    if !trusted(&after) || !same_inode(&before, &after) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "purge publication fence changed while it was opened",
        ));
    }
    Ok(file)
}

fn create(
    runtime_root: &Path,
    identity: (u64, u64),
    database_identity: DatabaseIdentity,
) -> std::io::Result<Fence> {
    create_with_admission_mode(runtime_root, identity, database_identity, false)
}

fn create_with_admission_mode(
    runtime_root: &Path,
    identity: (u64, u64),
    database_identity: DatabaseIdentity,
    nonblocking: bool,
) -> std::io::Result<Fence> {
    validate_reader_runtime_directory(runtime_root)?;
    let contextdb_directory = reader_breadcrumb_root(runtime_root);
    let directory = reader_snapshot_directory(runtime_root, identity);
    create_reader_breadcrumb_directory(&contextdb_directory)?;
    sync_directory(runtime_root)?;
    create_reader_breadcrumb_directory(&directory)?;
    sync_directory(&contextdb_directory)?;
    validate_reader_breadcrumb_directory(runtime_root, &directory)?;
    let path = path(runtime_root, identity, database_identity);
    let mut options = OpenOptions::new();
    options.read(true).write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    }
    match options.open(&path) {
        Ok(mut file) => {
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt as _;
                file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
            }
            acquire_reader_lock(
                &file,
                PUBLICATION_LOCK_OFFSET,
                RangeLockMode::Exclusive,
                nonblocking,
            )?;
            let result = initialize(&mut file);
            release_companion_range_lock(&file, PUBLICATION_LOCK_OFFSET);
            result?;
            sync_directory(&directory)?;
            Ok(Fence {
                file,
                path,
                reader_baseline: None,
                writer_acquired: false,
            })
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => Ok(Fence {
            file: open_existing(&path)?,
            path,
            reader_baseline: None,
            writer_acquired: false,
        }),
        Err(error) => Err(error),
    }
}

fn initialize(file: &mut File) -> std::io::Result<()> {
    file.set_len(index_end(DATA_OFFSET, INITIAL_CAPACITY)?)?;
    zero_range(
        file,
        DATA_OFFSET,
        INITIAL_CAPACITY
            .checked_mul(BUCKET_BYTES)
            .ok_or_else(overflow)?,
    )?;
    write_state(
        file,
        &State::Clear(ClearState {
            sequence: 0,
            generation: 0,
            frontier: Lsn(0),
            index_offset: DATA_OFFSET,
            capacity: INITIAL_CAPACITY,
            count: 0,
        }),
    )
}

/// Whether `name` is a publication fence's file name inside a registry.
pub(crate) fn is_fence_file_name(name: &std::ffi::OsStr) -> bool {
    name.to_str()
        .is_some_and(|name| name.starts_with("purge-publication-") && name.ends_with(".fence"))
}

/// What the last departing reader may do with one fence it found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FenceRemoval {
    /// The fence was Clear, held by nobody, named by its path, and is gone.
    Removed,
    /// Something about the fence could not be proven, so it stays.
    Kept,
}

/// Remove a fence only when every condition holds at once: no other holder of
/// either range lock (both taken exclusive by try), a readable Clear state, and
/// a path that still names the very inode those locks are on. Both locks are
/// held through the unlink. A Pending, unreadable or ambiguous fence is crash
/// recovery state that only a writer may reconcile, so it is always kept.
pub(crate) fn remove_if_clear(path: &Path) -> FenceRemoval {
    let mut file = match open_existing(path) {
        Ok(file) => file,
        Err(_) => return FenceRemoval::Kept,
    };
    if !matches!(
        super::take_companion_range_lock_exclusive(&file, PUBLICATION_ADMISSION_LOCK_OFFSET),
        Ok(RangeLockOutcome::Taken)
    ) {
        return FenceRemoval::Kept;
    }
    if !matches!(
        super::take_companion_range_lock_exclusive(&file, PUBLICATION_LOCK_OFFSET),
        Ok(RangeLockOutcome::Taken)
    ) {
        release_companion_range_lock(&file, PUBLICATION_ADMISSION_LOCK_OFFSET);
        return FenceRemoval::Kept;
    }
    let removal = match (
        read_latest(&mut file),
        file.metadata(),
        std::fs::symlink_metadata(path),
    ) {
        (Ok(State::Clear(_)), Ok(held), Ok(named))
            if trusted(&named) && same_inode(&held, &named) =>
        {
            if std::fs::remove_file(path).is_ok() {
                FenceRemoval::Removed
            } else {
                FenceRemoval::Kept
            }
        }
        _ => FenceRemoval::Kept,
    };
    release_companion_range_lock(&file, PUBLICATION_LOCK_OFFSET);
    release_companion_range_lock(&file, PUBLICATION_ADMISSION_LOCK_OFFSET);
    removal
}

pub(crate) fn open_for_reader(
    runtime_root: &Path,
    identity: (u64, u64),
    database_identity: DatabaseIdentity,
) -> std::io::Result<Fence> {
    open_reader_with_admission_mode(runtime_root, identity, database_identity, false)
}

/// Owner cursor admission holds the commit mutex; an occupied purge fence
/// must refuse immediately rather than waiting for its publisher.
pub(crate) fn open_for_owner_cursor(
    runtime_root: &Path,
    identity: (u64, u64),
    database_identity: DatabaseIdentity,
) -> std::io::Result<Fence> {
    open_reader_with_admission_mode(runtime_root, identity, database_identity, true)
}

#[cfg(all(test, target_os = "linux"))]
pub(super) fn hold_reader_lock_for_test(
    runtime_root: &Path,
    identity: (u64, u64),
    database_identity: DatabaseIdentity,
    admission: bool,
) -> File {
    let fence = open_for_reader(runtime_root, identity, database_identity)
        .expect("initialize the reader fence before contention");
    let held = open_existing(&fence.path).expect("open a separate fence description");
    let offset = if admission {
        PUBLICATION_ADMISSION_LOCK_OFFSET
    } else {
        PUBLICATION_LOCK_OFFSET
    };
    assert_eq!(
        super::take_companion_range_lock_exclusive(&held, offset).unwrap(),
        RangeLockOutcome::Taken,
    );
    held
}

fn acquire_reader_lock(
    file: &File,
    offset: u64,
    mode: RangeLockMode,
    nonblocking: bool,
) -> std::io::Result<()> {
    if !nonblocking {
        return block_on_companion_range_lock(file, offset, mode);
    }
    let outcome = match mode {
        RangeLockMode::Shared => take_companion_range_lock_shared(file, offset),
        RangeLockMode::Exclusive => super::take_companion_range_lock_exclusive(file, offset),
    }?;
    match outcome {
        RangeLockOutcome::Taken => Ok(()),
        RangeLockOutcome::HeldByAnother => Err(std::io::Error::new(
            std::io::ErrorKind::WouldBlock,
            "authoritative erasure publication fence is held by another operation",
        )),
    }
}

fn open_reader_with_admission_mode(
    runtime_root: &Path,
    identity: (u64, u64),
    database_identity: DatabaseIdentity,
    nonblocking: bool,
) -> std::io::Result<Fence> {
    let mut fence =
        create_with_admission_mode(runtime_root, identity, database_identity, nonblocking)?;
    acquire_reader_lock(
        &fence.file,
        PUBLICATION_ADMISSION_LOCK_OFFSET,
        RangeLockMode::Shared,
        nonblocking,
    )?;
    if let Err(error) = acquire_reader_lock(
        &fence.file,
        PUBLICATION_LOCK_OFFSET,
        RangeLockMode::Shared,
        nonblocking,
    ) {
        release_companion_range_lock(&fence.file, PUBLICATION_ADMISSION_LOCK_OFFSET);
        return Err(error);
    }
    let readable = read_latest(&mut fence.file);
    release_companion_range_lock(&fence.file, PUBLICATION_LOCK_OFFSET);
    release_companion_range_lock(&fence.file, PUBLICATION_ADMISSION_LOCK_OFFSET);
    // A pending publication can outlive its writer. The reader still needs
    // its exact-root registration while recovery decides whether the purge
    // committed. Use the generation before that decision: publication below
    // refuses Pending, and a committed recovery must remain visible to this
    // reader rather than becoming its starting baseline.
    fence.reader_baseline = Some(match readable? {
        State::Clear(clear) => clear.generation,
        State::Pending(pending) => pending.prior.generation,
    });
    Ok(fence)
}

pub(crate) fn open_for_writer(
    runtime_root: &Path,
    identity: (u64, u64),
    database_identity: DatabaseIdentity,
) -> std::io::Result<Option<Fence>> {
    let path = path(runtime_root, identity, database_identity);
    match open_existing(&path) {
        Ok(file) => Ok(Some(Fence {
            file,
            path,
            reader_baseline: None,
            writer_acquired: false,
        })),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

fn overflow() -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        "purge frontier storage offset overflow",
    )
}

fn index_end(offset: u64, capacity: u64) -> std::io::Result<u64> {
    offset
        .checked_add(capacity.checked_mul(BUCKET_BYTES).ok_or_else(overflow)?)
        .ok_or_else(overflow)
}

fn put_u64(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn take<const N: usize>(payload: &[u8], offset: &mut usize) -> std::io::Result<[u8; N]> {
    let end = offset.checked_add(N).ok_or_else(overflow)?;
    let bytes = payload.get(*offset..end).ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, "purge fence is truncated")
    })?;
    *offset = end;
    bytes.try_into().map_err(|_| overflow())
}

fn take_u64(payload: &[u8], offset: &mut usize) -> std::io::Result<u64> {
    Ok(u64::from_le_bytes(take(payload, offset)?))
}

fn encode(state: &State) -> std::io::Result<Vec<u8>> {
    let mut payload = Vec::with_capacity((CONTROL_SLOT_BYTES - 32) as usize);
    payload.extend_from_slice(MAGIC);
    payload.extend_from_slice(&VERSION.to_le_bytes());
    match state {
        State::Clear(clear) => {
            payload.push(0);
            put_u64(&mut payload, clear.sequence);
            put_u64(&mut payload, clear.generation);
            put_u64(&mut payload, clear.frontier.0);
            payload.extend_from_slice(&[0; 16]);
            put_u64(&mut payload, clear.index_offset);
            put_u64(&mut payload, clear.capacity);
            put_u64(&mut payload, clear.count);
            put_u64(&mut payload, 0);
            put_u64(&mut payload, 0);
            payload.extend_from_slice(&[0; 32]);
            put_u64(&mut payload, 0);
            put_u64(&mut payload, 0);
            put_u64(&mut payload, 0);
            put_u64(&mut payload, 0);
        }
        State::Pending(pending) => {
            payload.push(1);
            put_u64(&mut payload, pending.sequence);
            put_u64(&mut payload, pending.generation);
            put_u64(&mut payload, pending.frontier.0);
            payload.extend_from_slice(&pending.token);
            put_u64(&mut payload, pending.index_offset);
            put_u64(&mut payload, pending.capacity);
            put_u64(&mut payload, pending.count);
            put_u64(&mut payload, pending.manifest_offset);
            put_u64(&mut payload, pending.manifest_len);
            payload.extend_from_slice(&pending.manifest_hash);
            put_u64(&mut payload, pending.prior.index_offset);
            put_u64(&mut payload, pending.prior.capacity);
            put_u64(&mut payload, pending.prior.count);
            put_u64(&mut payload, pending.prior.frontier.0);
        }
    }
    let payload_len = (CONTROL_SLOT_BYTES - 32) as usize;
    if payload.len() > payload_len {
        return Err(overflow());
    }
    payload.resize(payload_len, 0);
    let checksum = blake3::hash(&payload);
    payload.extend_from_slice(checksum.as_bytes());
    Ok(payload)
}

fn decode(slot: &[u8]) -> std::io::Result<State> {
    let payload_len = (CONTROL_SLOT_BYTES - 32) as usize;
    if slot.len() != CONTROL_SLOT_BYTES as usize
        || blake3::hash(&slot[..payload_len]).as_bytes() != &slot[payload_len..]
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "purge publication control checksum mismatch",
        ));
    }
    let payload = &slot[..payload_len];
    let mut offset = 0;
    if payload.get(..MAGIC.len()) != Some(MAGIC) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "purge publication fence magic mismatch",
        ));
    }
    offset += MAGIC.len();
    if u16::from_le_bytes(take(payload, &mut offset)?) != VERSION {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "purge publication fence version is unsupported",
        ));
    }
    let tag = take::<1>(payload, &mut offset)?[0];
    let sequence = take_u64(payload, &mut offset)?;
    let generation = take_u64(payload, &mut offset)?;
    let frontier = Lsn(take_u64(payload, &mut offset)?);
    let token = take(payload, &mut offset)?;
    let index_offset = take_u64(payload, &mut offset)?;
    let capacity = take_u64(payload, &mut offset)?;
    let count = take_u64(payload, &mut offset)?;
    let manifest_offset = take_u64(payload, &mut offset)?;
    let manifest_len = take_u64(payload, &mut offset)?;
    let manifest_hash = take(payload, &mut offset)?;
    let prior_offset = take_u64(payload, &mut offset)?;
    let prior_capacity = take_u64(payload, &mut offset)?;
    let prior_count = take_u64(payload, &mut offset)?;
    let prior_frontier = Lsn(take_u64(payload, &mut offset)?);
    validate_index(index_offset, capacity, count)?;
    match tag {
        0 if token == [0; 16]
            && manifest_offset == 0
            && manifest_len == 0
            && manifest_hash == [0; 32]
            && prior_offset == 0
            && prior_capacity == 0
            && prior_count == 0
            && prior_frontier == Lsn(0) =>
        {
            Ok(State::Clear(ClearState {
                sequence,
                generation,
                frontier,
                index_offset,
                capacity,
                count,
            }))
        }
        1 if token != [0; 16] && manifest_len > 0 && manifest_len % 32 == 0 => {
            validate_index(prior_offset, prior_capacity, prior_count)?;
            Ok(State::Pending(PendingState {
                sequence,
                generation,
                frontier,
                token,
                prior: ClearState {
                    sequence: sequence.saturating_sub(1),
                    generation: generation.saturating_sub(1),
                    frontier: prior_frontier,
                    index_offset: prior_offset,
                    capacity: prior_capacity,
                    count: prior_count,
                },
                index_offset,
                capacity,
                count,
                manifest_offset,
                manifest_len,
                manifest_hash,
            }))
        }
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "purge publication fence state is invalid",
        )),
    }
}

fn validate_index(offset: u64, capacity: u64, count: u64) -> std::io::Result<()> {
    if offset < DATA_OFFSET
        || !capacity.is_power_of_two()
        || capacity < INITIAL_CAPACITY
        || count > capacity
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "purge publication frontier index metadata is invalid",
        ));
    }
    index_end(offset, capacity).map(|_| ())
}

fn read_records(file: &mut File) -> std::io::Result<Vec<State>> {
    let mut records = Vec::new();
    for slot_number in 0..CONTROL_SLOT_COUNT {
        file.seek(SeekFrom::Start(slot_number * CONTROL_SLOT_BYTES))?;
        let mut slot = vec![0; CONTROL_SLOT_BYTES as usize];
        if file.read_exact(&mut slot).is_ok()
            && let Ok(state) = decode(&slot)
        {
            records.push(state);
        }
    }
    records.sort_by_key(State::sequence);
    if records.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "purge publication fence has no recoverable generation",
        ));
    }
    Ok(records)
}

fn read_latest(file: &mut File) -> std::io::Result<State> {
    read_records(file)?.pop().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "purge publication fence has no latest generation",
        )
    })
}

fn write_state(file: &mut File, state: &State) -> std::io::Result<()> {
    let slot = state.sequence() % CONTROL_SLOT_COUNT;
    file.seek(SeekFrom::Start(slot * CONTROL_SLOT_BYTES))?;
    file.write_all(&encode(state)?)?;
    file.sync_data()
}

fn bucket_offset(index_offset: u64, slot: u64) -> std::io::Result<u64> {
    index_offset
        .checked_add(slot.checked_mul(BUCKET_BYTES).ok_or_else(overflow)?)
        .ok_or_else(overflow)
}

fn bucket_start(table: &[u8; 32], capacity: u64) -> u64 {
    u64::from_le_bytes(table[..8].try_into().expect("fixed table hash")) & (capacity - 1)
}

fn read_bucket(file: &mut File, index_offset: u64, slot: u64) -> std::io::Result<Option<Bucket>> {
    file.seek(SeekFrom::Start(bucket_offset(index_offset, slot)?))?;
    let mut bytes = [0; BUCKET_BYTES as usize];
    file.read_exact(&mut bytes)?;
    if bytes == [0; BUCKET_BYTES as usize] {
        return Ok(None);
    }
    if bytes[0] != 1
        || bytes[1..8] != [0; 7]
        || blake3::hash(&bytes[..56]).as_bytes() != &bytes[56..]
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "purge publication frontier bucket is corrupt",
        ));
    }
    Ok(Some(Bucket {
        table: bytes[8..40].try_into().expect("fixed bucket table"),
        generation: u64::from_le_bytes(bytes[40..48].try_into().expect("fixed generation")),
        frontier: Lsn(u64::from_le_bytes(
            bytes[48..56].try_into().expect("fixed frontier"),
        )),
    }))
}

fn write_bucket(
    file: &mut File,
    index_offset: u64,
    slot: u64,
    bucket: Bucket,
) -> std::io::Result<()> {
    let mut bytes = [0; BUCKET_BYTES as usize];
    bytes[0] = 1;
    bytes[8..40].copy_from_slice(&bucket.table);
    bytes[40..48].copy_from_slice(&bucket.generation.to_le_bytes());
    bytes[48..56].copy_from_slice(&bucket.frontier.0.to_le_bytes());
    let checksum = blake3::hash(&bytes[..56]);
    bytes[56..].copy_from_slice(checksum.as_bytes());
    file.seek(SeekFrom::Start(bucket_offset(index_offset, slot)?))?;
    file.write_all(&bytes)
}

fn find_bucket(
    file: &mut File,
    index_offset: u64,
    capacity: u64,
    table: &[u8; 32],
) -> std::io::Result<(u64, Option<Bucket>)> {
    let start = bucket_start(table, capacity);
    for probe in 0..capacity {
        let slot = (start + probe) & (capacity - 1);
        match read_bucket(file, index_offset, slot)? {
            Some(bucket) if &bucket.table == table => return Ok((slot, Some(bucket))),
            Some(_) => {}
            None => return Ok((slot, None)),
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        "purge publication frontier index is full",
    ))
}

fn zero_range(file: &mut File, offset: u64, length: u64) -> std::io::Result<()> {
    file.seek(SeekFrom::Start(offset))?;
    let zeros = [0; 8192];
    let mut remaining = length;
    while remaining > 0 {
        let amount = usize::try_from(remaining.min(zeros.len() as u64)).map_err(|_| overflow())?;
        file.write_all(&zeros[..amount])?;
        remaining -= amount as u64;
    }
    Ok(())
}

fn manifest_tables(file: &mut File, pending: &PendingState) -> std::io::Result<Vec<[u8; 32]>> {
    let length = usize::try_from(pending.manifest_len).map_err(|_| overflow())?;
    let mut bytes = vec![0; length];
    file.seek(SeekFrom::Start(pending.manifest_offset))?;
    file.read_exact(&mut bytes)?;
    if blake3::hash(&bytes).as_bytes() != &pending.manifest_hash {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "purge publication pending manifest is corrupt",
        ));
    }
    let mut tables = Vec::with_capacity(length / 32);
    for chunk in bytes.chunks_exact(32) {
        tables.push(chunk.try_into().expect("exact manifest chunk"));
    }
    if !tables.windows(2).all(|pair| pair[0] < pair[1]) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "purge publication pending manifest is not canonical",
        ));
    }
    Ok(tables)
}

fn apply_pending(
    file: &mut File,
    pending: &PendingState,
    tables: &[[u8; 32]],
) -> std::io::Result<()> {
    if pending.index_offset != pending.prior.index_offset {
        let end = index_end(pending.index_offset, pending.capacity)?;
        file.set_len(end)?;
        zero_range(
            file,
            pending.index_offset,
            pending
                .capacity
                .checked_mul(BUCKET_BYTES)
                .ok_or_else(overflow)?,
        )?;
        for slot in 0..pending.prior.capacity {
            if let Some(bucket) = read_bucket(file, pending.prior.index_offset, slot)? {
                let (target, existing) =
                    find_bucket(file, pending.index_offset, pending.capacity, &bucket.table)?;
                if existing.is_some() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "purge publication frontier index repeats a table",
                    ));
                }
                write_bucket(file, pending.index_offset, target, bucket)?;
            }
        }
    }
    for table in tables {
        let (slot, _) = find_bucket(file, pending.index_offset, pending.capacity, table)?;
        write_bucket(
            file,
            pending.index_offset,
            slot,
            Bucket {
                table: *table,
                generation: pending.generation,
                frontier: pending.frontier,
            },
        )?;
    }
    file.sync_data()
}

/// Let go of the admission byte an EXCLUSIVE holder took, and make the letting
/// go audible to the readers waiting behind it.
///
/// The unlock alone is silent to a watcher, so a reader that armed after this
/// holder's last write would sleep to its own deadline over a byte that is
/// free. The commit path happens to close the file straight afterwards and the
/// recovery probe does not, which is exactly the kind of coupling a promise
/// should not rest on.
fn release_exclusive_admission(file: &File) {
    release_companion_range_lock(file, PUBLICATION_ADMISSION_LOCK_OFFSET);
    announce_companion_release(file);
}

fn publication_refusal(reason: impl Into<String>) -> Error {
    Error::ReadFailure(
        contextdb_core::read_contract::ReadFailure::new(
            contextdb_core::read_contract::ReadFailureKind::AuthoritativeErasure,
            contextdb_core::read_contract::ReadFailureDetail::Reason {
                reason: reason.into(),
            },
        )
        .expect("authoritative-erasure refusal carries ordinary reason detail"),
    )
}

#[cfg(feature = "test-seams")]
fn pause_before_publication_unlock_for_test() {
    if std::env::var_os("CONTEXTDB_PURGE_PUBLICATION_STOP_AT_FINAL")
        .is_some_and(|armed| PathBuf::from(armed).exists())
    {
        unsafe {
            libc::raise(libc::SIGSTOP);
        }
        return;
    }
    // Armed test: pause here and block until released, over the same
    // control-socket protocol the calling test already uses for its
    // ready/go handshake (a fresh connection -- the listener accepts once
    // per handshake -- since this call is reached deep inside a read, with
    // no access to whichever stream the test's own top-level handshake
    // opened). No sleep-poll: `read_exact` blocks on the peer's one release
    // byte, arriving the instant the test writes it.
    //
    // `armed`'s existence is still the temporal gate it always was: the
    // calling test creates that marker only once its own ready/go handshake
    // has completed, so an earlier read that reaches this same seam (e.g. a
    // reader child's own startup probe, before it is armed) passes through
    // untouched instead of pausing on a connection nobody is listening for
    // yet.
    if let Some(control_socket) = std::env::var_os("CONTEXTDB_PURGE_PUBLICATION_CONTROL_SOCKET") {
        let Some(armed) = std::env::var_os("CONTEXTDB_PURGE_PUBLICATION_PAUSE_ARMED") else {
            return;
        };
        if !PathBuf::from(armed).exists() {
            return;
        }
        let Ok(mut stream) = std::os::unix::net::UnixStream::connect(control_socket) else {
            return;
        };
        let _ = stream.write_all(b"p");
        let mut release = [0_u8; 1];
        let _ = stream.read_exact(&mut release);
        return;
    }
    // Legacy file-based handshake, kept for a caller that has no listener to
    // connect to.
    let (Some(armed), Some(ready), Some(release)) = (
        std::env::var_os("CONTEXTDB_PURGE_PUBLICATION_PAUSE_ARMED"),
        std::env::var_os("CONTEXTDB_PURGE_PUBLICATION_PAUSE_READY"),
        std::env::var_os("CONTEXTDB_PURGE_PUBLICATION_PAUSE_RELEASE"),
    ) else {
        return;
    };
    if !PathBuf::from(armed).exists() {
        return;
    }
    let ready = PathBuf::from(ready);
    let release = PathBuf::from(release);
    std::fs::write(&ready, b"ready").expect("publish direct-reader pause marker");
    while !release.exists() {
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
}

/// What a test may do at a chosen instant inside a publication's admission
/// wait, on the waiting thread itself.
///
/// The admission wait's correctness is all in the ORDER of three things -- the
/// ask for the byte, arming the watch that will hear it released, and going to
/// sleep -- and in what happens when a holder lets go between two of them. A
/// test cannot hit those windows by racing a second thread against them; it
/// has to be let into the window. These are the two windows, and nothing is
/// installed in production, where both calls compile to nothing.
#[cfg(any(test, feature = "test-seams"))]
type PublicationWaitHook = Box<dyn Fn()>;

#[cfg(any(test, feature = "test-seams"))]
thread_local! {
    /// Between a take that found the byte held and arming the watch: the
    /// window the lazy arming creates, where a release raises no notification
    /// anybody is listening for yet.
    static AFTER_HELD_ADMISSION_TAKE: std::cell::RefCell<Option<PublicationWaitHook>> =
        const { std::cell::RefCell::new(None) };
    /// Between the last ask and the sleep: the window where a release must
    /// raise a notification, because nothing will ask again until one arrives.
    static BEFORE_ADMISSION_SLEEP: std::cell::RefCell<Option<PublicationWaitHook>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(any(test, feature = "test-seams"))]
fn run_publication_wait_hook(
    slot: &'static std::thread::LocalKey<std::cell::RefCell<Option<PublicationWaitHook>>>,
) {
    let hook = slot.with(|installed| installed.borrow_mut().take());
    if let Some(hook) = hook {
        hook();
    }
}

/// Install a one-shot hook for the window named by `slot`, taken by the first
/// wait that reaches it on this thread.
#[cfg(any(test, feature = "test-seams"))]
pub(crate) fn install_after_held_admission_take_hook_for_test(hook: PublicationWaitHook) {
    AFTER_HELD_ADMISSION_TAKE.with(|slot| *slot.borrow_mut() = Some(hook));
}

#[cfg(any(test, feature = "test-seams"))]
pub(crate) fn install_before_admission_sleep_hook_for_test(hook: PublicationWaitHook) {
    BEFORE_ADMISSION_SLEEP.with(|slot| *slot.borrow_mut() = Some(hook));
}

impl Fence {
    fn acquire_reader_admission(
        &self,
        budget: Option<&crate::executor::ReadPublicationBudget>,
    ) -> Result<()> {
        let Some(budget) = budget else {
            return block_on_companion_range_lock(
                &self.file,
                PUBLICATION_ADMISSION_LOCK_OFFSET,
                RangeLockMode::Shared,
            )
            .map_err(|error| {
                publication_refusal(format!(
                    "cannot enter authoritative erasure publication: {error}"
                ))
            });
        };
        // Almost every publication finds this byte free and takes it at once:
        // nobody is erasing, so nothing waits. What a wait needs -- something
        // to sleep on, and something to wake it by -- is therefore built only
        // once the byte is actually held by somebody else. A watch costs a
        // pipe and a notification channel; a deadline costs an operating-
        // system thread, started and joined around the wait. A reader that
        // pays both per statement pays them nine times to render one page,
        // and the statement itself takes microseconds.
        //
        // Ordering is what makes building them late safe. The watch is armed
        // and then the byte is asked for AGAIN before anything sleeps, so a
        // holder that lets go in the gap has already queued its notification
        // rather than slipped past unseen.
        //
        // The active-time contract expires only after the declared value has
        // elapsed (`>` below), so arm the wake for the first millisecond that
        // can actually refuse rather than for the still-admissible boundary.
        // The deadline is an absolute instant, so arming it at the first real
        // wait bounds exactly the instant it always bounded.
        let deadline_ms = budget.deadline_ms();
        let mut watch: Option<CoordinationWatch> = None;
        let mut expiry: Option<contextdb_core::read_contract::DeadlineWait<'_>> = None;
        let mut expiry_completed = false;
        loop {
            if budget.cancellation.is_cancelled() {
                return Err(Error::ReadCancelled);
            }
            if budget.elapsed() {
                return Err(Error::ReadFailure(budget.exceeded_failure()));
            }
            match take_companion_range_lock_shared(&self.file, PUBLICATION_ADMISSION_LOCK_OFFSET) {
                Ok(RangeLockOutcome::Taken) => return Ok(()),
                Ok(RangeLockOutcome::HeldByAnother) => {}
                Err(error) => {
                    return Err(publication_refusal(format!(
                        "cannot enter authoritative erasure publication: {error}"
                    )));
                }
            }
            #[cfg(any(test, feature = "test-seams"))]
            run_publication_wait_hook(&AFTER_HELD_ADMISSION_TAKE);
            if watch.is_none() {
                watch = Some(
                    CoordinationWatch::arm_funded(&self.path, &budget.cancellation).map_err(
                        |error| match error {
                            CoordinationWatchFailure::Admission(error) => error,
                            CoordinationWatchFailure::Io(error) => publication_refusal(format!(
                                "cannot watch authoritative erasure publication admission: {error}"
                            )),
                        },
                    )?,
                );
                // Ask again now that the watch is armed, so a release between
                // the ask above and the sleep below cannot be missed.
                continue;
            }
            let watch = watch.as_ref().expect("the watch is armed above");
            if expiry.is_none() {
                expiry = Some(budget.clock.wait_until(deadline_ms));
            }
            let expiry = expiry.as_mut().expect("the deadline wake is armed above");
            let waker = watch.waker();
            let mut context = std::task::Context::from_waker(&waker);
            // This future bounds an actual wait behind purge publication.
            // A free admission lock publishes immediately and never gives an
            // unused timer a chance to refuse the completed read.
            if !expiry_completed {
                expiry_completed =
                    std::future::Future::poll(std::pin::Pin::as_mut(expiry), &mut context)
                        .is_ready();
            }
            if budget.elapsed() {
                return Err(Error::ReadFailure(budget.exceeded_failure()));
            }
            let timeout = if expiry_completed {
                match budget.remaining_wait() {
                    crate::executor::ReadPublicationDeadline::Active(remaining) => Some(remaining),
                    crate::executor::ReadPublicationDeadline::Elapsed => continue,
                }
            } else {
                None
            };
            #[cfg(any(test, feature = "test-seams"))]
            run_publication_wait_hook(&BEFORE_ADMISSION_SLEEP);
            watch
                .sleep(&self.file, PUBLICATION_ADMISSION_LOCK_OFFSET, timeout)
                .map_err(|error| {
                    publication_refusal(format!(
                        "cannot wait for authoritative erasure publication: {error}"
                    ))
                })?;
        }
    }

    pub(crate) fn acquire_for_prepare(mut self) -> Result<Self> {
        block_on_companion_range_lock(
            &self.file,
            PUBLICATION_ADMISSION_LOCK_OFFSET,
            RangeLockMode::Exclusive,
        )
        .map_err(|error| Error::Other((format!("acquire purge publication admission: {error}")).into()))?;
        if let Err(error) = block_on_companion_range_lock(
            &self.file,
            PUBLICATION_LOCK_OFFSET,
            RangeLockMode::Exclusive,
        ) {
            release_exclusive_admission(&self.file);
            return Err(Error::Other((format!(
                "acquire purge publication fence: {error}"
            )).into()));
        }
        self.writer_acquired = true;
        Ok(self)
    }

    fn release_writer(&mut self) {
        if self.writer_acquired {
            release_companion_range_lock(&self.file, PUBLICATION_LOCK_OFFSET);
            release_exclusive_admission(&self.file);
            self.writer_acquired = false;
        }
    }

    pub(crate) fn is_pending(&mut self) -> Result<bool> {
        block_on_companion_range_lock(
            &self.file,
            PUBLICATION_ADMISSION_LOCK_OFFSET,
            RangeLockMode::Exclusive,
        )
        .map_err(|error| Error::Other((format!("acquire purge recovery admission: {error}")).into()))?;
        if let Err(error) = block_on_companion_range_lock(
            &self.file,
            PUBLICATION_LOCK_OFFSET,
            RangeLockMode::Exclusive,
        ) {
            release_exclusive_admission(&self.file);
            return Err(Error::Other((format!(
                "acquire purge recovery probe: {error}"
            )).into()));
        }
        let pending = read_latest(&mut self.file)
            .map(|state| matches!(state, State::Pending(_)))
            .map_err(|error| Error::Other((format!("read purge recovery probe: {error}")).into()));
        release_companion_range_lock(&self.file, PUBLICATION_LOCK_OFFSET);
        release_exclusive_admission(&self.file);
        pending
    }

    #[cfg(test)]
    pub(crate) fn publish<T>(
        &mut self,
        snapshot_lsn: Lsn,
        tables: &BTreeSet<String>,
        value: T,
    ) -> Result<T> {
        self.publish_with_budget(snapshot_lsn, tables, value, None)
    }

    pub(crate) fn publish_with_budget<T>(
        &mut self,
        snapshot_lsn: Lsn,
        tables: &BTreeSet<String>,
        value: T,
        budget: Option<&crate::executor::ReadPublicationBudget>,
    ) -> Result<T> {
        #[cfg(any(test, feature = "test-seams"))]
        super::read_persistence_test_scaffold::note_erasure_publication_for_test();
        self.acquire_reader_admission(budget)?;
        if let Err(error) = block_on_companion_range_lock(
            &self.file,
            PUBLICATION_LOCK_OFFSET,
            RangeLockMode::Shared,
        ) {
            release_companion_range_lock(&self.file, PUBLICATION_ADMISSION_LOCK_OFFSET);
            return Err(publication_refusal(format!(
                "cannot coordinate authoritative erasure publication: {error}"
            )));
        }
        let verdict = (|| {
            #[cfg(any(test, feature = "test-seams"))]
            super::read_persistence_test_scaffold::note_erasure_frontier_read_for_test();
            let state = read_latest(&mut self.file).map_err(|error| {
                publication_refusal(format!(
                    "authoritative erasure publication is unresolved: {error}"
                ))
            })?;
            let clear = match state {
                State::Clear(clear) => clear,
                State::Pending(_) => {
                    return Err(publication_refusal(
                        "authoritative erasure publication is pending commit recovery",
                    ));
                }
            };
            let baseline = self.reader_baseline.ok_or_else(|| {
                publication_refusal("authoritative erasure reader baseline is unavailable")
            })?;
            let mut affected = false;
            for table in tables {
                let identity = if table == "*" {
                    wildcard_identity()
                } else {
                    table_identity(table)
                };
                let (_, bucket) = find_bucket(
                    &mut self.file,
                    clear.index_offset,
                    clear.capacity,
                    &identity,
                )
                .map_err(|error| {
                    publication_refusal(format!(
                        "authoritative erasure frontier is unreadable: {error}"
                    ))
                })?;
                affected |= bucket.is_some_and(|bucket| bucket.generation > baseline);
                if affected {
                    break;
                }
            }
            if affected {
                return Err(publication_refusal(format!(
                    "authoritative erasure at frontier {} invalidated this result's snapshot {}",
                    clear.frontier.0, snapshot_lsn.0
                )));
            }
            Ok(value)
        })();
        #[cfg(feature = "test-seams")]
        pause_before_publication_unlock_for_test();
        release_companion_range_lock(&self.file, PUBLICATION_LOCK_OFFSET);
        release_companion_range_lock(&self.file, PUBLICATION_ADMISSION_LOCK_OFFSET);
        verdict
    }

    pub(crate) fn prepare(
        self,
        frontier: Lsn,
        tables: &BTreeSet<String>,
    ) -> Result<PreparedPublication> {
        self.prepare_with_token(frontier, tables, uuid::Uuid::new_v4().into_bytes())
    }

    fn prepare_with_token(
        mut self,
        frontier: Lsn,
        tables: &BTreeSet<String>,
        token: [u8; 16],
    ) -> Result<PreparedPublication> {
        if !self.writer_acquired {
            self = self.acquire_for_prepare()?;
        }
        let result = (|| {
            let prior = match read_latest(&mut self.file)
                .map_err(|error| Error::Other((format!("read purge publication fence: {error}")).into()))?
            {
                State::Clear(clear) => clear,
                State::Pending(_) => {
                    return Err(Error::Other((
                        "authoritative purge publication is awaiting crash recovery".to_owned()).into()));
                }
            };
            let generation = prior
                .generation
                .checked_add(1)
                .ok_or_else(|| Error::Other(("purge publication generation overflow".to_owned()).into()))?;
            let sequence = prior
                .sequence
                .checked_add(1)
                .ok_or_else(|| Error::Other(("purge publication sequence overflow".to_owned()).into()))?;
            let mut identities = BTreeSet::from([wildcard_identity()]);
            identities.extend(tables.iter().map(|table| table_identity(table)));
            let tables: Vec<_> = identities.into_iter().collect();
            let mut additions = 0_u64;
            for table in &tables {
                if find_bucket(&mut self.file, prior.index_offset, prior.capacity, table)
                    .map_err(|error| Error::Other((format!("read purge frontier index: {error}")).into()))?
                    .1
                    .is_none()
                {
                    additions = additions.checked_add(1).ok_or_else(|| {
                        Error::Other(("purge publication table count overflow".to_owned()).into())
                    })?;
                }
            }
            let count = prior
                .count
                .checked_add(additions)
                .ok_or_else(|| Error::Other(("purge publication table count overflow".to_owned()).into()))?;
            let mut capacity = prior.capacity;
            while count.checked_mul(10).ok_or_else(|| {
                Error::Other(("purge publication capacity arithmetic overflow".to_owned()).into())
            })? > capacity.checked_mul(7).ok_or_else(|| {
                Error::Other(("purge publication capacity arithmetic overflow".to_owned()).into())
            })? {
                capacity = capacity.checked_mul(2).ok_or_else(|| {
                    Error::Other(("purge publication frontier capacity overflow".to_owned()).into())
                })?;
            }
            let current_end = index_end(prior.index_offset, prior.capacity)
                .map_err(|error| Error::Other((format!("size purge frontier index: {error}")).into()))?;
            if self
                .file
                .metadata()
                .map_err(|error| Error::Other((error.to_string()).into()))?
                .len()
                > current_end
            {
                self.file.set_len(current_end).map_err(|error| {
                    Error::Other((format!("trim purge publication tail: {error}")).into())
                })?;
            }
            let manifest_offset = current_end;
            let mut manifest = Vec::with_capacity(tables.len() * 32);
            for table in &tables {
                manifest.extend_from_slice(table);
            }
            self.file
                .seek(SeekFrom::Start(manifest_offset))
                .and_then(|_| self.file.write_all(&manifest))
                .and_then(|_| self.file.sync_data())
                .map_err(|error| {
                    Error::Other((format!("write purge publication manifest: {error}")).into())
                })?;
            let manifest_len = u64::try_from(manifest.len()).map_err(|_| {
                Error::Other(("purge publication manifest length overflow".to_owned()).into())
            })?;
            let index_offset = if capacity == prior.capacity {
                prior.index_offset
            } else {
                manifest_offset.checked_add(manifest_len).ok_or_else(|| {
                    Error::Other(("purge publication index offset overflow".to_owned()).into())
                })?
            };
            let pending = PendingState {
                sequence,
                generation,
                frontier,
                token,
                prior: prior.clone(),
                index_offset,
                capacity,
                count,
                manifest_offset,
                manifest_len,
                manifest_hash: *blake3::hash(&manifest).as_bytes(),
            };
            write_state(&mut self.file, &State::Pending(pending.clone())).map_err(|error| {
                Error::Other((format!("prepare durable purge publication fence: {error}")).into())
            })?;
            Ok((prior, pending, tables))
        })();
        match result {
            Ok((prior, pending, tables)) => Ok(PreparedPublication {
                fence: self,
                prior,
                pending,
                tables,
                resolved: false,
            }),
            Err(error) => {
                self.release_writer();
                Err(error)
            }
        }
    }

    pub(crate) fn recover(&mut self, marker: Option<DurableMarker>) -> Result<()> {
        block_on_companion_range_lock(
            &self.file,
            PUBLICATION_ADMISSION_LOCK_OFFSET,
            RangeLockMode::Exclusive,
        )
        .map_err(|error| Error::Other((format!("acquire purge recovery admission: {error}")).into()))?;
        if let Err(error) = block_on_companion_range_lock(
            &self.file,
            PUBLICATION_LOCK_OFFSET,
            RangeLockMode::Exclusive,
        ) {
            release_exclusive_admission(&self.file);
            return Err(Error::Other((format!(
                "acquire purge recovery fence: {error}"
            )).into()));
        }
        let result = (|| {
            let State::Pending(pending) = read_latest(&mut self.file)
                .map_err(|error| Error::Other((format!("read purge recovery fence: {error}")).into()))?
            else {
                return Ok(());
            };
            let committed = marker.is_some_and(|marker| {
                marker.token == pending.token && marker.frontier == pending.frontier
            });
            if committed {
                let tables = manifest_tables(&mut self.file, &pending).map_err(|error| {
                    Error::Other((format!("read purge recovery manifest: {error}")).into())
                })?;
                finish_commit(&mut self.file, &pending, &tables)
            } else {
                finish_abort(&mut self.file, &pending.prior, &pending)
            }
        })();
        release_companion_range_lock(&self.file, PUBLICATION_LOCK_OFFSET);
        release_exclusive_admission(&self.file);
        result
    }
}

fn finish_commit(file: &mut File, pending: &PendingState, tables: &[[u8; 32]]) -> Result<()> {
    apply_pending(file, pending, tables)
        .map_err(|error| Error::Other((format!("apply purge publication frontiers: {error}")).into()))?;
    let clear = ClearState {
        sequence: pending
            .sequence
            .checked_add(1)
            .ok_or_else(|| Error::Other(("purge publication sequence overflow".to_owned()).into()))?,
        generation: pending.generation,
        frontier: pending.frontier,
        index_offset: pending.index_offset,
        capacity: pending.capacity,
        count: pending.count,
    };
    write_state(file, &State::Clear(clear.clone()))
        .map_err(|error| Error::Other((format!("commit purge publication fence: {error}")).into()))?;
    if pending.index_offset == pending.prior.index_offset {
        file.set_len(
            index_end(clear.index_offset, clear.capacity).map_err(|error| {
                Error::Other((format!("size committed purge frontier index: {error}")).into())
            })?,
        )
        .and_then(|_| file.sync_data())
        .map_err(|error| Error::Other((format!("trim committed purge manifest: {error}")).into()))?;
    }
    Ok(())
}

fn finish_abort(file: &mut File, prior: &ClearState, pending: &PendingState) -> Result<()> {
    let clear = ClearState {
        sequence: pending
            .sequence
            .checked_add(1)
            .ok_or_else(|| Error::Other(("purge publication sequence overflow".to_owned()).into()))?,
        ..prior.clone()
    };
    write_state(file, &State::Clear(clear.clone()))
        .map_err(|error| Error::Other((format!("abort purge publication fence: {error}")).into()))?;
    file.set_len(
        index_end(clear.index_offset, clear.capacity)
            .map_err(|error| Error::Other((format!("size aborted purge frontier index: {error}")).into()))?,
    )
    .and_then(|_| file.sync_data())
    .map_err(|error| Error::Other((format!("trim aborted purge manifest: {error}")).into()))?;
    Ok(())
}

impl PreparedPublication {
    pub(crate) fn marker(&self) -> DurableMarker {
        DurableMarker {
            token: self.pending.token,
            frontier: self.pending.frontier,
        }
    }

    pub(crate) fn commit(mut self) -> Result<()> {
        #[cfg(any(test, feature = "test-seams"))]
        if super::read_persistence_test_scaffold::take_purge_pending_for_test() {
            // Left exactly as a writer that died here leaves it: Pending.
            return Err(Error::Other((
                "injected: the purge publication was left Pending".to_owned()).into()));
        }
        finish_commit(&mut self.fence.file, &self.pending, &self.tables)?;
        self.resolved = true;
        self.fence.release_writer();
        Ok(())
    }

    pub(crate) fn abort(mut self) -> Result<()> {
        finish_abort(&mut self.fence.file, &self.prior, &self.pending)?;
        self.resolved = true;
        self.fence.release_writer();
        Ok(())
    }
}

impl Drop for PreparedPublication {
    fn drop(&mut self) {
        if !self.resolved {
            self.fence.release_writer();
        }
    }
}

impl Drop for Fence {
    fn drop(&mut self) {
        self.release_writer();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::local_transport::MonotonicDeadlineClock;
    use contextdb_core::read_contract::{DeadlineClock, DeadlineWait, OwnerReadCancellation};
    use std::future::poll_fn;
    use std::os::unix::fs::PermissionsExt as _;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
    use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
    use std::sync::{Arc, Barrier};

    struct ControlledReadyClock {
        now: AtomicU64,
        advance_on_poll: Option<u64>,
        first_poll: std::sync::Mutex<Option<SyncSender<()>>>,
        release_first_poll: Option<Arc<Barrier>>,
        polls: AtomicUsize,
    }

    impl DeadlineClock for ControlledReadyClock {
        fn now_ms(&self) -> u64 {
            self.now.load(Ordering::SeqCst)
        }

        fn wait_until(&self, _deadline_ms: u64) -> DeadlineWait<'_> {
            Box::pin(poll_fn(move |_| {
                let poll = self.polls.fetch_add(1, Ordering::SeqCst);
                if let Some(now) = self.advance_on_poll {
                    self.now.store(now, Ordering::SeqCst);
                }
                if poll == 0 {
                    if let Some(started) = self.first_poll.lock().unwrap().take() {
                        let _ = started.send(());
                    }
                    if let Some(release) = self.release_first_poll.as_ref() {
                        release.wait();
                    }
                }
                std::task::Poll::Ready(())
            }))
        }
    }

    fn controlled_clock(
        now: u64,
        advance_on_poll: Option<u64>,
        block_first_poll: bool,
    ) -> (
        Arc<ControlledReadyClock>,
        Receiver<()>,
        Option<Arc<Barrier>>,
    ) {
        let (sent, received) = sync_channel(1);
        let release = block_first_poll.then(|| Arc::new(Barrier::new(2)));
        (
            Arc::new(ControlledReadyClock {
                now: AtomicU64::new(now),
                advance_on_poll,
                first_poll: std::sync::Mutex::new(Some(sent)),
                release_first_poll: release.clone(),
                polls: AtomicUsize::new(0),
            }),
            received,
            release,
        )
    }

    fn budget(
        clock: Arc<dyn DeadlineClock>,
        cancellation: OwnerReadCancellation,
    ) -> crate::executor::ReadPublicationBudget {
        crate::executor::ReadPublicationBudget {
            clock,
            cancellation,
            started_ms: 0,
            active_ms: 20,
        }
    }

    fn contended_fences() -> (tempfile::TempDir, Fence, Fence) {
        let runtime = runtime();
        let identity = (71, 73);
        let database_identity = DatabaseIdentity([79; 16]);
        let reader = open_for_reader(runtime.path(), identity, database_identity)
            .expect("open direct publication reader");
        let writer = open_for_writer(runtime.path(), identity, database_identity)
            .expect("open direct publication writer")
            .expect("the reader created the shared fence")
            .acquire_for_prepare()
            .expect("hold direct publication admission");
        (runtime, reader, writer)
    }

    fn runtime() -> tempfile::TempDir {
        let runtime = tempfile::tempdir().expect("temporary purge publication runtime");
        std::fs::set_permissions(runtime.path(), std::fs::Permissions::from_mode(0o700))
            .expect("private purge publication runtime");
        runtime
    }

    fn assert_erased(result: Result<()>) {
        assert!(matches!(
            result,
            Err(Error::ReadFailure(failure))
                if failure.kind()
                    == contextdb_core::read_contract::ReadFailureKind::AuthoritativeErasure
        ));
    }

    #[test]
    fn file_publication_wait_has_no_false_timer_refusal_repoll_or_lost_release_wake() {
        let (_runtime, mut reader, writer) = contended_fences();
        let (clock, polled, release) = controlled_clock(20, None, true);
        let read_budget = budget(clock.clone(), OwnerReadCancellation::new());
        let publish = std::thread::spawn(move || {
            reader.publish_with_budget(
                Lsn(1),
                &BTreeSet::from(["items".to_owned()]),
                (),
                Some(&read_budget),
            )
        });
        polled
            .recv()
            .expect("the contended file reader polls its timer");
        drop(writer);
        release.expect("first file poll is controlled").wait();
        assert!(
            publish
                .join()
                .expect("file publication reader exits")
                .is_ok(),
            "timer readiness at the exact active-time boundary cannot refuse file publication"
        );
        assert_eq!(
            clock.polls.load(Ordering::SeqCst),
            1,
            "the completed early timer is not repolled after the file-lock release"
        );
    }

    #[test]
    fn file_publication_wait_wakes_for_cancellation_and_refuses_only_after_elapsed_deadline() {
        let (_runtime, mut reader, writer) = contended_fences();
        let cancellation = OwnerReadCancellation::new();
        let (clock, polled, release) = controlled_clock(0, None, true);
        let read_budget = budget(clock.clone(), cancellation.clone());
        let publish = std::thread::spawn(move || {
            reader.publish_with_budget(
                Lsn(1),
                &BTreeSet::from(["items".to_owned()]),
                (),
                Some(&read_budget),
            )
        });
        polled
            .recv()
            .expect("the cancellable file reader polls its timer");
        cancellation.cancel();
        release.expect("first file poll is controlled").wait();
        assert!(matches!(
            publish.join().expect("cancelled file reader exits"),
            Err(Error::ReadCancelled)
        ));
        assert_eq!(clock.polls.load(Ordering::SeqCst), 1);
        drop(writer);

        let (_runtime, mut deadline_reader, _deadline_writer) = contended_fences();
        let (deadline_clock, _, _) = controlled_clock(0, Some(21), false);
        let error = deadline_reader
            .publish_with_budget(
                Lsn(1),
                &BTreeSet::from(["items".to_owned()]),
                (),
                Some(&budget(deadline_clock, OwnerReadCancellation::new())),
            )
            .expect_err("elapsed contended file publication must refuse");
        assert!(matches!(
            error,
            Error::ReadFailure(failure)
                if failure.kind()
                    == contextdb_core::read_contract::ReadFailureKind::OwnerLimitExceeded
        ));
    }

    #[test]
    fn file_publication_timer_arm_failure_uses_the_real_remaining_budget_once() {
        let (_runtime, mut reader, writer) = contended_fences();
        let clock = Arc::new(MonotonicDeadlineClock::with_timer_arm_failure_for_test());
        let read_budget = budget(clock.clone(), OwnerReadCancellation::new());
        let (sent, received) = sync_channel(1);
        let publish = std::thread::spawn(move || {
            let result = reader.publish_with_budget(
                Lsn(1),
                &BTreeSet::from(["items".to_owned()]),
                (),
                Some(&read_budget),
            );
            let _ = sent.send(result);
        });

        let error = received
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("the timed poll bounds a failed file-publication timer arm")
            .expect_err("the file writer still owns publication after the budget");
        assert!(matches!(
            error,
            Error::ReadFailure(failure)
                if failure.kind()
                    == contextdb_core::read_contract::ReadFailureKind::OwnerLimitExceeded
        ));
        assert_eq!(clock.timer_arm_failure_waits_for_test(), 1);
        assert_eq!(clock.timer_arm_failure_polls_for_test(), 1);
        drop(writer);
        publish.join().expect("file arm-failure reader exits");
    }

    struct DeadlineCrossingClock {
        now_reads: AtomicUsize,
        polls: AtomicUsize,
    }

    impl DeadlineClock for DeadlineCrossingClock {
        fn now_ms(&self) -> u64 {
            let read = self.now_reads.fetch_add(1, Ordering::SeqCst);
            if read < 2 { 20 } else { 21 }
        }

        fn wait_until(&self, _deadline_ms: u64) -> DeadlineWait<'_> {
            Box::pin(poll_fn(move |_| {
                self.polls.fetch_add(1, Ordering::SeqCst);
                std::task::Poll::Ready(())
            }))
        }
    }

    #[test]
    fn file_publication_deadline_crossing_never_becomes_an_unbounded_wait() {
        let (_runtime, mut reader, writer) = contended_fences();
        let clock = Arc::new(DeadlineCrossingClock {
            now_reads: AtomicUsize::new(0),
            polls: AtomicUsize::new(0),
        });
        let read_budget = budget(clock.clone(), OwnerReadCancellation::new());
        let (sent, received) = sync_channel(1);
        let publish = std::thread::spawn(move || {
            let result = reader.publish_with_budget(
                Lsn(1),
                &BTreeSet::from(["items".to_owned()]),
                (),
                Some(&read_budget),
            );
            let _ = sent.send(result);
        });

        let error = received
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("crossing the deadline must answer without file-lock release")
            .expect_err("the elapsed file-publication budget must refuse");
        assert!(matches!(
            error,
            Error::ReadFailure(failure)
                if failure.kind()
                    == contextdb_core::read_contract::ReadFailureKind::OwnerLimitExceeded
        ));
        assert_eq!(clock.polls.load(Ordering::SeqCst), 1);
        drop(writer);
        publish.join().expect("deadline-crossing file reader exits");
    }

    /// A holder the journey can make let go at a chosen instant, either by
    /// dropping it -- which releases the byte AND closes the descriptor -- or
    /// by unlocking the byte alone and staying open. The two are different
    /// events to anybody watching the file, and the wait must survive both.
    fn shared_holder(writer: Fence) -> Arc<std::sync::Mutex<Option<Fence>>> {
        Arc::new(std::sync::Mutex::new(Some(writer)))
    }

    fn let_go_by_closing(held: &Arc<std::sync::Mutex<Option<Fence>>>) {
        drop(held.lock().unwrap().take());
    }

    fn let_go_without_closing(held: &Arc<std::sync::Mutex<Option<Fence>>>) {
        let mut held = held.lock().unwrap();
        held.as_mut()
            .expect("the holder is still holding")
            .release_writer();
    }

    /// Long enough that a wait which misses the release has to be refused by
    /// its own deadline rather than by chance. Nothing here asserts elapsed
    /// time; this is the value the refusal would name.
    const PATIENT_MS: u64 = 5_000;

    fn patient_budget(
        cancellation: OwnerReadCancellation,
    ) -> crate::executor::ReadPublicationBudget {
        crate::executor::ReadPublicationBudget {
            clock: Arc::new(MonotonicDeadlineClock::new()),
            cancellation,
            started_ms: 0,
            active_ms: PATIENT_MS,
        }
    }

    /// The window the lazy arming creates: the holder lets go after the ask
    /// that found the byte held and before the watch that would hear it is
    /// armed, so no notification of the release exists anywhere. The wait
    /// answers because it asks again after arming, not because it heard
    /// anything.
    #[test]
    fn a_release_between_the_ask_and_the_watch_is_not_missed() {
        for close_it in [true, false] {
            let (_runtime, mut reader, writer) = contended_fences();
            let held = shared_holder(writer);
            let letting_go = Arc::clone(&held);
            install_after_held_admission_take_hook_for_test(Box::new(move || {
                if close_it {
                    let_go_by_closing(&letting_go);
                } else {
                    let_go_without_closing(&letting_go);
                }
            }));
            assert!(
                reader
                    .publish_with_budget(
                        Lsn(1),
                        &BTreeSet::from(["items".to_owned()]),
                        (),
                        Some(&patient_budget(OwnerReadCancellation::new())),
                    )
                    .is_ok(),
                "a holder that let go (closing: {close_it}) between the ask and the watch left \
                 the wait asleep until its own deadline, so a read the store was ready to answer \
                 was refused",
            );
        }
    }

    /// The holder lets go while the wait is about to sleep, and does NOT close
    /// the file afterwards -- which is what the recovery probe does, since it
    /// releases the admission byte and then keeps its fence to recover with.
    ///
    /// An `fcntl` unlock writes nothing, changes no metadata and closes no
    /// descriptor, so on its own it raises none of the notifications the wait
    /// sleeps on. If the release is not made visible, the wait sleeps to its
    /// own deadline and refuses a read that the store was ready to answer for
    /// almost all of it.
    #[test]
    fn a_release_the_holder_never_closes_after_still_reaches_the_sleeping_wait() {
        let (_runtime, mut reader, writer) = contended_fences();
        let held = shared_holder(writer);
        let letting_go = Arc::clone(&held);
        install_before_admission_sleep_hook_for_test(Box::new(move || {
            let_go_without_closing(&letting_go);
        }));
        assert!(
            reader
                .publish_with_budget(
                    Lsn(1),
                    &BTreeSet::from(["items".to_owned()]),
                    (),
                    Some(&patient_budget(OwnerReadCancellation::new())),
                )
                .is_ok(),
            "the holder let the byte go and stayed open, and the wait slept through it to its \
             own deadline -- so a read the store was ready to answer was refused for a hold \
             nobody had",
        );
    }

    /// Each thing the wait needs is built at most once, however many times the
    /// wait goes round, and a wait that never sleeps builds neither.
    #[test]
    fn a_contended_wait_costs_one_pipe_and_one_thread_and_an_uncontended_one_costs_nothing() {
        let (_runtime, mut reader, writer) = contended_fences();
        let held = shared_holder(writer);
        let letting_go = Arc::clone(&held);
        install_before_admission_sleep_hook_for_test(Box::new(move || {
            let_go_by_closing(&letting_go);
        }));
        crate::persistence::read_persistence_test_scaffold::reset_read_os_setup_for_test();
        assert!(
            reader
                .publish_with_budget(
                    Lsn(1),
                    &BTreeSet::from(["items".to_owned()]),
                    (),
                    Some(&patient_budget(OwnerReadCancellation::new())),
                )
                .is_ok(),
            "the contended wait never got the byte the holder let go of"
        );
        let contended =
            crate::persistence::read_persistence_test_scaffold::read_os_setup_for_test();
        assert_eq!(
            (
                contended.coordination_wake_pipes,
                contended.deadline_timer_threads
            ),
            (1, 1),
            "one contended wait built {contended:?}; each thing it waits on is built once, \
             however many times the wait goes round"
        );

        crate::persistence::read_persistence_test_scaffold::reset_read_os_setup_for_test();
        assert!(
            reader
                .publish_with_budget(
                    Lsn(1),
                    &BTreeSet::from(["items".to_owned()]),
                    (),
                    Some(&patient_budget(OwnerReadCancellation::new())),
                )
                .is_ok(),
            "the second publication has nobody to wait for"
        );
        let free = crate::persistence::read_persistence_test_scaffold::read_os_setup_for_test();
        assert_eq!(
            (free.coordination_wake_pipes, free.deadline_timer_threads),
            (0, 0),
            "a publication that found the byte free built {free:?}"
        );
    }

    #[test]
    fn pending_fence_fails_closed_then_recovers_abort_or_commit() {
        let runtime = runtime();
        let identity = (17, 29);
        let database_identity = DatabaseIdentity([41; 16]);
        let tables = BTreeSet::from(["erased".to_owned()]);
        let unrelated = BTreeSet::from(["unrelated".to_owned()]);
        let mut reader = open_for_reader(runtime.path(), identity, database_identity).unwrap();

        reader.publish(Lsn(4), &tables, ()).unwrap();
        let prepared = open_for_writer(runtime.path(), identity, database_identity)
            .unwrap()
            .unwrap()
            .prepare(Lsn(8), &tables)
            .unwrap();
        drop(prepared);
        let mut registered_while_pending =
            open_for_reader(runtime.path(), identity, database_identity)
                .expect("pending recovery must not prevent exact-root registration");
        assert_erased(reader.publish(Lsn(4), &tables, ()));
        assert_erased(registered_while_pending.publish(Lsn(4), &tables, ()));
        open_for_writer(runtime.path(), identity, database_identity)
            .unwrap()
            .unwrap()
            .recover(None)
            .unwrap();
        reader.publish(Lsn(4), &tables, ()).unwrap();
        registered_while_pending
            .publish(Lsn(4), &tables, ())
            .unwrap();

        let prepared = open_for_writer(runtime.path(), identity, database_identity)
            .unwrap()
            .unwrap()
            .prepare(Lsn(9), &tables)
            .unwrap();
        let marker = prepared.marker();
        drop(prepared);
        let mut registered_before_commit_recovery =
            open_for_reader(runtime.path(), identity, database_identity)
                .expect("register old root while committed purge awaits recovery");
        assert_erased(registered_before_commit_recovery.publish(Lsn(4), &tables, ()));
        open_for_writer(runtime.path(), identity, database_identity)
            .unwrap()
            .unwrap()
            .recover(Some(marker))
            .unwrap();
        assert_erased(reader.publish(Lsn(4), &tables, ()));
        assert_erased(registered_before_commit_recovery.publish(Lsn(4), &tables, ()));
        reader.publish(Lsn(4), &unrelated, ()).unwrap();
        registered_before_commit_recovery
            .publish(Lsn(4), &unrelated, ())
            .unwrap();
    }

    #[test]
    fn frontier_index_grows_past_the_old_capacity_without_global_invalidation() {
        let runtime = runtime();
        let identity = (31, 37);
        let database_identity = DatabaseIdentity([43; 16]);
        let tables: BTreeSet<_> = (0..16_500)
            .map(|index| format!("table_{index:05}"))
            .collect();
        let unrelated = BTreeSet::from(["not_purged".to_owned()]);
        let mut old_reader = open_for_reader(runtime.path(), identity, database_identity).unwrap();
        let prepared = open_for_writer(runtime.path(), identity, database_identity)
            .unwrap()
            .unwrap()
            .prepare(Lsn(21), &tables)
            .unwrap();
        let marker = prepared.marker();
        drop(prepared);
        open_for_writer(runtime.path(), identity, database_identity)
            .unwrap()
            .unwrap()
            .recover(Some(marker))
            .unwrap();

        assert_erased(old_reader.publish(Lsn(20), &BTreeSet::from(["table_16499".to_owned()]), ()));
        old_reader.publish(Lsn(20), &unrelated, ()).unwrap();
        let mut fresh = open_for_reader(runtime.path(), identity, database_identity).unwrap();
        let first_table = BTreeSet::from(["table_00000".to_owned()]);
        fresh.publish(Lsn(21), &first_table, ()).unwrap();
        open_for_writer(runtime.path(), identity, database_identity)
            .unwrap()
            .unwrap()
            .prepare(Lsn(22), &unrelated)
            .unwrap()
            .commit()
            .unwrap();
        fresh.publish(Lsn(21), &first_table, ()).unwrap();
        assert_erased(fresh.publish(Lsn(21), &unrelated, ()));
        assert!(
            std::fs::metadata(path(runtime.path(), identity, database_identity))
                .unwrap()
                .len()
                < 8 * 1024 * 1024
        );
    }

    #[test]
    fn corrupt_frontier_bucket_fails_closed_and_incarnations_are_independent() {
        let runtime = runtime();
        let database_identity = DatabaseIdentity([47; 16]);
        let first = (41, 43);
        let second = (41, 44);
        let tables = BTreeSet::from(["erased".to_owned()]);
        let mut first_reader = open_for_reader(runtime.path(), first, database_identity).unwrap();
        let mut second_reader = open_for_reader(runtime.path(), second, database_identity).unwrap();
        open_for_writer(runtime.path(), first, database_identity)
            .unwrap()
            .unwrap()
            .prepare(Lsn(5), &tables)
            .unwrap()
            .commit()
            .unwrap();
        second_reader.publish(Lsn(1), &tables, ()).unwrap();

        let mut file = open_existing(&path(runtime.path(), first, database_identity)).unwrap();
        let State::Clear(clear) = read_latest(&mut file).unwrap() else {
            panic!("clear")
        };
        let (slot, Some(_)) = find_bucket(
            &mut file,
            clear.index_offset,
            clear.capacity,
            &table_identity("erased"),
        )
        .unwrap() else {
            panic!("bucket")
        };
        file.seek(SeekFrom::Start(
            bucket_offset(clear.index_offset, slot).unwrap() + 12,
        ))
        .unwrap();
        file.write_all(&[0xff]).unwrap();
        file.sync_data().unwrap();
        assert_erased(first_reader.publish(Lsn(1), &tables, ()));
    }

    fn fence_path(runtime: &Path, identity: (u64, u64), database: DatabaseIdentity) -> PathBuf {
        path(runtime, identity, database)
    }

    #[test]
    fn a_clear_unheld_fence_is_removed_and_its_opener_sees_it_depart() {
        let runtime = runtime();
        let identity = (61, 67);
        let database_identity = DatabaseIdentity([61; 16]);
        let reader = open_for_reader(runtime.path(), identity, database_identity).unwrap();
        assert!(reader.path_still_names());
        let fence = fence_path(runtime.path(), identity, database_identity);
        assert_eq!(remove_if_clear(&fence), FenceRemoval::Removed);
        assert!(!fence.exists());
        assert!(
            !reader.path_still_names(),
            "an opener of a removed fence must reopen before it registers"
        );
        let reopened = open_for_reader(runtime.path(), identity, database_identity).unwrap();
        assert!(reopened.path_still_names());
    }

    #[test]
    fn a_pending_fence_is_never_removed() {
        let runtime = runtime();
        let identity = (71, 73);
        let database_identity = DatabaseIdentity([71; 16]);
        let tables = BTreeSet::from(["erased".to_owned()]);
        drop(open_for_reader(runtime.path(), identity, database_identity).unwrap());
        let prepared = open_for_writer(runtime.path(), identity, database_identity)
            .unwrap()
            .unwrap()
            .prepare(Lsn(8), &tables)
            .unwrap();
        drop(prepared);
        let fence = fence_path(runtime.path(), identity, database_identity);
        assert_eq!(
            remove_if_clear(&fence),
            FenceRemoval::Kept,
            "a Pending fence is crash-recovery state only a writer may reconcile"
        );
        assert!(fence.exists());
        open_for_writer(runtime.path(), identity, database_identity)
            .unwrap()
            .unwrap()
            .recover(None)
            .unwrap();
        assert_eq!(remove_if_clear(&fence), FenceRemoval::Removed);
    }

    #[test]
    fn an_unreadable_fence_is_never_removed() {
        let runtime = runtime();
        let identity = (79, 83);
        let database_identity = DatabaseIdentity([79; 16]);
        drop(open_for_reader(runtime.path(), identity, database_identity).unwrap());
        let fence = fence_path(runtime.path(), identity, database_identity);
        std::fs::OpenOptions::new()
            .write(true)
            .open(&fence)
            .unwrap()
            .set_len(3)
            .unwrap();
        assert_eq!(remove_if_clear(&fence), FenceRemoval::Kept);
        assert!(fence.exists());
    }

    #[test]
    fn a_fence_whose_locks_are_held_is_never_removed() {
        for admission in [true, false] {
            let runtime = runtime();
            let identity = (89, 97);
            let database_identity = DatabaseIdentity([89; 16]);
            let held =
                hold_reader_lock_for_test(runtime.path(), identity, database_identity, admission);
            let fence = fence_path(runtime.path(), identity, database_identity);
            assert_eq!(
                remove_if_clear(&fence),
                FenceRemoval::Kept,
                "a holder of the {} lock keeps the fence",
                if admission {
                    "admission"
                } else {
                    "publication"
                }
            );
            drop(held);
            assert_eq!(remove_if_clear(&fence), FenceRemoval::Removed);
        }
    }

    #[test]
    fn a_fence_path_naming_another_file_is_never_removed() {
        let runtime = runtime();
        let identity = (101, 103);
        let database_identity = DatabaseIdentity([101; 16]);
        drop(open_for_reader(runtime.path(), identity, database_identity).unwrap());
        let fence = fence_path(runtime.path(), identity, database_identity);
        let displaced = fence.with_extension("displaced");
        std::fs::rename(&fence, &displaced).unwrap();
        std::fs::hard_link(&displaced, &fence).unwrap();
        assert_eq!(
            remove_if_clear(&fence),
            FenceRemoval::Kept,
            "a fence with a second name is not a trusted single-name fence"
        );
        assert!(fence.exists());
    }
}
