use crate::blob_repository::{
    BlobAuthoritativePurgeProjection, apply_authoritative_purge_in_write,
};
use crate::composite_store::{ChangeLogEntry, sync_source_lsn_updates};
use crate::database::TriggerAuditEntry;
use crate::database::event_bus::{EventBusPersistenceCommit, PreparedSinkEvent, SinkQueueEntry};
use crate::database::trigger::TriggerPersistenceCommit;
use crate::local_transport::{RuntimeDirectory, derive_channel_address};
use crate::sync_types::DdlChange;
use contextdb_core::read_contract::{
    ChannelAddress, DatabaseIdentity, HeldByReadersDetail, HeldByWriterDetail, LocalUserIdentity,
    OwnerReadCancellation, OwnerReadStatus, OwnerServingReason, OwnerServingState, ReadFailure,
    ReadFailureDetail, ReadFailureKind, ReaderBreadcrumb, ReaderProcessIdentity, WriterRunNumber,
};
use contextdb_core::{
    AdjEntry, ColumnType, Error, ForeignKeyReference, IndexDecl, Lsn, NodeId, PropagationRule,
    RankPolicy, Result, RowId, StateMachineConstraint, TableMeta, TxId, Value, VectorEntry,
    VectorIndexRef, VectorQuantization, VersionedRow, Wallclock,
};
use contextdb_relational::store::SyncSourceKind;
use contextdb_tx::WriteSet;
use redb::{
    Key as RedbKey, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition,
    TableHandle, Value as RedbValue,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fmt::Write as _;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Serializes the process-global panic-hook swap in `open_hook_suppressed` so
/// two concurrent store opens cannot interleave `take_hook`/`set_hook` and
/// leave the no-op hook permanently installed.
static HOOK_SWAP: Mutex<()> = Mutex::new(());
static READER_BREADCRUMB_SEQUENCE: AtomicU64 = AtomicU64::new(0);

const COMPANION_RECORD_MAGIC: &[u8] = b"contextdb-companion";
const COMPANION_FILE_MAGIC: &[u8; 16] = b"CTXDB-COMPANION1";
const COMPANION_SLOT_PAYLOAD_CAPACITY: usize = 4 * 1024;
const COMPANION_SLOT_BYTES: usize = 4 + COMPANION_SLOT_PAYLOAD_CAPACITY + 32;
const COMPANION_FIRST_SLOT_OFFSET: usize = COMPANION_FILE_MAGIC.len();
const COMPANION_SELECTOR_OFFSET: usize = COMPANION_FIRST_SLOT_OFFSET + 2 * COMPANION_SLOT_BYTES;
const COMPANION_SELECTOR_MAGIC: &[u8; 8] = b"ACTIVEV1";
const COMPANION_SELECTOR_BODY_BYTES: usize = 24;
const COMPANION_SELECTOR_BYTES: usize = COMPANION_SELECTOR_BODY_BYTES + 32;
const COMPANION_FILE_BYTES: usize = COMPANION_SELECTOR_OFFSET + COMPANION_SELECTOR_BYTES;
const MAX_COMPANION_STARTUP_REASON_BYTES: usize = 256;
const COMPANION_PENDING_MAGIC: &[u8] = b"contextdb-replacement-v1";

#[derive(Debug, Clone, PartialEq, Eq)]
struct CompanionRecord {
    format_version: u16,
    generation: u64,
    database_identity: DatabaseIdentity,
    writer_run_number: WriterRunNumber,
    owner_user: LocalUserIdentity,
    channel_address: ChannelAddress,
    process_id: u32,
    owner_read_status: OwnerReadStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct EncodedCompanionRecord {
    fields: CompanionRecord,
    checksummed_payload: Vec<u8>,
    stored_checksum: [u8; 32],
}

fn companion_status_tags(status: &OwnerReadStatus) -> std::result::Result<(u8, u8), String> {
    status.validate().map_err(|error| error.to_string())?;
    let state = match status.state {
        OwnerServingState::Serving => 0,
        OwnerServingState::ServingDisabled => 1,
        OwnerServingState::NotServing => 2,
        OwnerServingState::NotApplicable => 3,
    };
    let reason = match &status.reason {
        None => 0,
        Some(OwnerServingReason::DisabledByConfiguration) => 1,
        Some(OwnerServingReason::StartupFailure(_)) => 2,
        Some(OwnerServingReason::ShutdownDraining) => 3,
        Some(OwnerServingReason::PlatformUnsupported) => 4,
    };
    Ok((state, reason))
}

fn encode_companion_record(
    fields: &CompanionRecord,
) -> std::result::Result<EncodedCompanionRecord, String> {
    let (state, reason) = companion_status_tags(&fields.owner_read_status)?;
    let startup_reason = match &fields.owner_read_status.reason {
        Some(OwnerServingReason::StartupFailure(reason)) => reason.as_bytes(),
        _ => &[],
    };
    if startup_reason.len() > MAX_COMPANION_STARTUP_REASON_BYTES {
        return Err(format!(
            "companion startup-failure reason exceeds {MAX_COMPANION_STARTUP_REASON_BYTES} bytes"
        ));
    }
    let startup_reason_len = u32::try_from(startup_reason.len())
        .map_err(|_| "companion startup-failure reason is too long".to_owned())?;
    let mut payload = Vec::with_capacity(
        COMPANION_RECORD_MAGIC.len() + 2 + 8 + 16 + 16 + 8 + 32 + 4 + 2 + 4 + startup_reason.len(),
    );
    payload.extend_from_slice(COMPANION_RECORD_MAGIC);
    payload.extend_from_slice(&fields.format_version.to_le_bytes());
    payload.extend_from_slice(&fields.generation.to_le_bytes());
    payload.extend_from_slice(&fields.database_identity.0);
    payload.extend_from_slice(&fields.writer_run_number.0);
    payload.extend_from_slice(&fields.owner_user.0.to_le_bytes());
    payload.extend_from_slice(&fields.channel_address.0);
    payload.extend_from_slice(&fields.process_id.to_le_bytes());
    payload.push(state);
    payload.push(reason);
    if reason == 2 {
        payload.extend_from_slice(&startup_reason_len.to_le_bytes());
        payload.extend_from_slice(startup_reason);
    }
    let checksum = *blake3::hash(&payload).as_bytes();
    Ok(EncodedCompanionRecord {
        fields: fields.clone(),
        checksummed_payload: payload,
        stored_checksum: checksum,
    })
}

struct CompanionPayloadCursor<'a> {
    payload: &'a [u8],
    offset: usize,
}

impl<'a> CompanionPayloadCursor<'a> {
    fn take(&mut self, length: usize) -> std::result::Result<&'a [u8], String> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or_else(|| "companion payload offset overflow".to_owned())?;
        let bytes = self
            .payload
            .get(self.offset..end)
            .ok_or_else(|| "companion payload is truncated".to_owned())?;
        self.offset = end;
        Ok(bytes)
    }

    fn array<const N: usize>(&mut self) -> std::result::Result<[u8; N], String> {
        self.take(N)?
            .try_into()
            .map_err(|_| "companion payload field has the wrong width".to_owned())
    }

    fn u8(&mut self) -> std::result::Result<u8, String> {
        Ok(self.array::<1>()?[0])
    }

    fn u16(&mut self) -> std::result::Result<u16, String> {
        Ok(u16::from_le_bytes(self.array()?))
    }

    fn u32(&mut self) -> std::result::Result<u32, String> {
        Ok(u32::from_le_bytes(self.array()?))
    }

    fn u64(&mut self) -> std::result::Result<u64, String> {
        Ok(u64::from_le_bytes(self.array()?))
    }

    fn finish(self) -> std::result::Result<(), String> {
        if self.offset == self.payload.len() {
            Ok(())
        } else {
            Err("companion payload has trailing bytes".to_owned())
        }
    }
}

fn decode_companion_record(
    payload: &[u8],
    stored_checksum: [u8; 32],
) -> std::result::Result<EncodedCompanionRecord, String> {
    if *blake3::hash(payload).as_bytes() != stored_checksum {
        return Err("companion record checksum mismatch".to_owned());
    }
    let mut cursor = CompanionPayloadCursor { payload, offset: 0 };
    if cursor.take(COMPANION_RECORD_MAGIC.len())? != COMPANION_RECORD_MAGIC {
        return Err("companion record magic mismatch".to_owned());
    }
    let format_version = cursor.u16()?;
    let generation = cursor.u64()?;
    let database_identity = DatabaseIdentity(cursor.array()?);
    let writer_run_number = WriterRunNumber(cursor.array()?);
    let owner_user = LocalUserIdentity(cursor.u64()?);
    let channel_address = ChannelAddress(cursor.array()?);
    let process_id = cursor.u32()?;
    let state = match cursor.u8()? {
        0 => OwnerServingState::Serving,
        1 => OwnerServingState::ServingDisabled,
        2 => OwnerServingState::NotServing,
        3 => OwnerServingState::NotApplicable,
        other => return Err(format!("unknown companion serving-state tag {other}")),
    };
    let reason = match cursor.u8()? {
        0 => None,
        1 => Some(OwnerServingReason::DisabledByConfiguration),
        2 => {
            let length = usize::try_from(cursor.u32()?)
                .map_err(|_| "companion reason length does not fit usize".to_owned())?;
            if length > MAX_COMPANION_STARTUP_REASON_BYTES {
                return Err(format!(
                    "companion startup-failure reason exceeds {MAX_COMPANION_STARTUP_REASON_BYTES} bytes"
                ));
            }
            let reason = String::from_utf8(cursor.take(length)?.to_vec())
                .map_err(|error| format!("companion startup reason is not UTF-8: {error}"))?;
            Some(OwnerServingReason::StartupFailure(reason))
        }
        3 => Some(OwnerServingReason::ShutdownDraining),
        4 => Some(OwnerServingReason::PlatformUnsupported),
        other => return Err(format!("unknown companion serving-reason tag {other}")),
    };
    cursor.finish()?;
    let owner_read_status = OwnerReadStatus { state, reason };
    owner_read_status
        .validate()
        .map_err(|error| error.to_string())?;
    Ok(EncodedCompanionRecord {
        fields: CompanionRecord {
            format_version,
            generation,
            database_identity,
            writer_run_number,
            owner_user,
            channel_address,
            process_id,
            owner_read_status,
        },
        checksummed_payload: payload.to_vec(),
        stored_checksum,
    })
}

fn bounded_companion_startup_reason(reason: &str) -> String {
    let normalized = reason.to_ascii_lowercase();
    for (needle, public_reason) in [
        ("permission denied", "permission denied"),
        ("address already in use", "address already in use"),
        ("no such file", "path not found"),
        ("not found", "path not found"),
        ("connection refused", "connection refused"),
        ("timed out", "operation timed out"),
        ("unsupported", "platform unsupported"),
        ("invalid input", "invalid operating-system input"),
        ("resource busy", "resource busy"),
        ("read-only file system", "read-only file system"),
        ("too many open files", "file-descriptor limit reached"),
    ] {
        if normalized.contains(needle) {
            return public_reason.to_owned();
        }
    }
    "operating-system error (details redacted)".to_owned()
}

fn bounded_companion_status(status: OwnerReadStatus) -> OwnerReadStatus {
    let reason = match status.reason {
        Some(OwnerServingReason::StartupFailure(reason)) => Some(
            OwnerServingReason::StartupFailure(bounded_companion_startup_reason(&reason)),
        ),
        other => other,
    };
    OwnerReadStatus {
        state: status.state,
        reason,
    }
}

#[allow(dead_code)]
fn prepare_companion_record_for_publication(
    fields: &CompanionRecord,
) -> std::result::Result<EncodedCompanionRecord, String> {
    let record = encode_companion_record(fields)?;
    #[cfg(feature = "test-seams")]
    read_persistence_test_scaffold::companion_update_planned_for_test(&record);
    Ok(record)
}

#[allow(dead_code)]
fn recover_companion_record(
    payload: &[u8],
    stored_checksum: [u8; 32],
) -> std::result::Result<EncodedCompanionRecord, String> {
    decode_companion_record(payload, stored_checksum)
}

/// The identity a live writer published beside its store: which database this
/// is, which run of it is current, whose account owns it, and the channel
/// address a reader would dial. A reader derives none of this itself, so an
/// owner that answers on a channel answers as exactly the run recorded here.
pub(crate) fn published_writer_identity(
    database_path: &Path,
) -> Option<(
    DatabaseIdentity,
    WriterRunNumber,
    LocalUserIdentity,
    ChannelAddress,
)> {
    let record = inspect_companion_record(database_path).ok()?;
    Some((
        record.fields.database_identity,
        record.fields.writer_run_number,
        record.fields.owner_user,
        record.fields.channel_address,
    ))
}

/// Whether the trusted companion proves a writer owns this store right now.
///
/// A published writer identity is durable and can outlive its process. The
/// advisory lock is the live fact route selection needs when a pathname
/// accepts a connection but never authenticates an owner. This probe touches
/// only the companion beside the store; the committed file is never opened.
/// `None` is deliberately indeterminate: an absent, untrusted, replaced, or
/// unprobeable companion never licenses a direct-file fallback.
pub(crate) fn observed_writer_hold(database_path: &Path) -> Option<bool> {
    let companion = companion_path(database_path);
    let file = open_trusted_companion(&companion)?;
    let before_probe = file.metadata().ok()?;
    validate_companion_metadata(&companion, &file, &before_probe).ok()?;
    let held = writer_holds_companion(&file)?;
    validate_companion_metadata(&companion, &file, &before_probe).ok()?;
    Some(held)
}

/// What the writer holding this store recorded about serving inspection.
///
/// Two facts have to agree before a reader may believe a record. The
/// record itself is durable and is never unlinked, so the last writer's
/// state outlives that writer: on its own it could describe a process that is
/// gone. The advisory companion lock is what says the recorded owner is STILL
/// there, and probing it touches neither the store file nor a single byte of
/// its contents.
pub(crate) fn recorded_held_owner_status(database_path: &Path) -> Option<OwnerReadStatus> {
    // ONE open. The record and the lock that proves the record is still THIS
    // writer's are observed through the same companion file, and the writer's
    // own run identity is read on both sides of the lock probe. Two separate
    // opens could span a handover -- one writer exiting, the next acquiring --
    // and the arriving writer's lock would then vouch for the departed
    // writer's reason, telling a reader why a process that is no longer there
    // could not serve it.
    let companion = companion_path(database_path);
    // The same trust boundary the writer's own companion path applies: no
    // symlink is followed, and the inode this reader is looking at must be one
    // current-user-owned 0600 regular file that is STILL the file at this
    // pathname on both sides of the probe. Without that, a symlinked or
    // substituted companion could hand a reader a locked, checksummed
    // "the owner will not serve you" about a store nobody owns.
    let mut file = open_trusted_companion(&companion)?;
    let before_probe = file.metadata().ok()?;
    for _ in 0..RECORDED_OWNER_OBSERVATION_ATTEMPTS {
        validate_companion_metadata(&companion, &file, &before_probe).ok()?;
        // A writer takes the claim-window byte before replacing the previous
        // run's durable record. While that byte is held, the bytes below can
        // still describe the departed writer and must not be attributed to
        // the live holder merely because the whole-file writer lock is held.
        // The claim-window path is what waits for the arriving writer's own
        // decision; this status path deliberately has no answer yet.
        match take_companion_range_lock_shared(&file, COMPANION_CLAIM_WINDOW_LOCK_OFFSET).ok()? {
            RangeLockOutcome::Taken => {
                release_companion_range_lock(&file, COMPANION_CLAIM_WINDOW_LOCK_OFFSET);
            }
            RangeLockOutcome::HeldByAnother => return None,
        }
        let before = active_companion_record(&mut file)?;
        if writer_holds_companion(&file) != Some(true) {
            return None;
        }
        let after = active_companion_record(&mut file)?;
        validate_companion_metadata(&companion, &file, &before_probe).ok()?;
        match take_companion_range_lock_shared(&file, COMPANION_CLAIM_WINDOW_LOCK_OFFSET).ok()? {
            RangeLockOutcome::Taken => {
                release_companion_range_lock(&file, COMPANION_CLAIM_WINDOW_LOCK_OFFSET);
            }
            RangeLockOutcome::HeldByAnother => return None,
        }
        if before.fields.writer_run_number != after.fields.writer_run_number
            || before.fields.database_identity != after.fields.database_identity
        {
            // The store changed hands around the probe. Nothing observed here
            // describes one writer, so it is observed again rather than
            // reported.
            continue;
        }
        return Some(after.fields.owner_read_status);
    }
    None
}

/// What a live holder recorded when it decided not to serve inspection.
///
/// `Serving` is reached through the channel itself. `NotApplicable` is the
/// deliberately undecided placeholder published while a writer is inside its
/// claim window. Neither is an unserved-owner refusal.
pub(crate) fn recorded_unserved_owner(database_path: &Path) -> Option<OwnerReadStatus> {
    let status = recorded_held_owner_status(database_path)?;
    if matches!(
        status.state,
        OwnerServingState::Serving | OwnerServingState::NotApplicable
    ) {
        return None;
    }
    Some(status)
}

/// The two coordination facts a store's companion carries as advisory
/// byte-range locks rather than as bytes.
///
/// Both live far past the end of the companion's record area, at offsets no
/// byte of the record will ever occupy, and neither is ever read or written:
/// taking one costs a lock call and nothing else, the companion's own content
/// is untouched, and the kernel drops the hold the moment the holder's process
/// dies. Byte-range locks are independent of the whole-file advisory lock a
/// writer takes to claim the store, so a hold here never contends with a
/// claim -- and both are on the companion beside the store, so nothing here
/// opens, reads, locks, or creates anything in the store folder.
///
/// `CLAIM_WINDOW` is held by a writer from the moment it claims the store
/// until it publishes its serving decision, so a caller arriving inside that
/// window has something to sleep on instead of a verdict formed before the
/// writer decided anything.
///
/// `READER_HOLD` is held, shared, by every direct reader for exactly as long
/// as it holds the committed image open, so a caller waiting to become the
/// writer sleeps on the readers themselves rather than on what happens to be
/// written down about them.
const COMPANION_CLAIM_WINDOW_LOCK_OFFSET: u64 = 1 << 40;
const COMPANION_READER_HOLD_LOCK_OFFSET: u64 = (1 << 40) + 1;

/// Whether one advisory byte-range lock was taken.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RangeLockOutcome {
    Taken,
    HeldByAnother,
}

/// Take, or release, one advisory byte-range lock on an already-open trusted
/// companion.
///
/// The lock is tied to this open file description, so it survives every other
/// open and close of the same pathname in this process -- which is what lets a
/// reader and a writer inside ONE process contend exactly as two processes do,
/// and what makes a proof of this behavior a proof of the deployed behavior.
#[cfg(unix)]
fn companion_range_lock(
    file: &File,
    offset: u64,
    lock_type: libc::c_short,
    blocking: bool,
) -> std::io::Result<RangeLockOutcome> {
    use std::os::unix::io::AsRawFd as _;

    let mut request: libc::flock = unsafe { std::mem::zeroed() };
    request.l_type = lock_type;
    request.l_whence = libc::SEEK_SET as libc::c_short;
    request.l_start = offset as libc::off_t;
    request.l_len = 1;
    let command = if blocking {
        libc::F_OFD_SETLKW
    } else {
        libc::F_OFD_SETLK
    };
    let answered = unsafe { libc::fcntl(file.as_raw_fd(), command, &request) };
    if answered != -1 {
        return Ok(RangeLockOutcome::Taken);
    }
    let error = std::io::Error::last_os_error();
    match error.raw_os_error() {
        Some(libc::EAGAIN) | Some(libc::EACCES) => Ok(RangeLockOutcome::HeldByAnother),
        _ => Err(error),
    }
}

#[cfg(not(unix))]
fn companion_range_lock(
    _file: &File,
    _offset: u64,
    _lock_type: i16,
    _blocking: bool,
) -> std::io::Result<RangeLockOutcome> {
    Err(std::io::Error::other(
        "advisory byte-range coordination is not implemented on this platform",
    ))
}

#[cfg(unix)]
fn take_companion_range_lock_shared(file: &File, offset: u64) -> std::io::Result<RangeLockOutcome> {
    companion_range_lock(file, offset, RangeLockMode::Shared.lock_type(), false)
}

#[cfg(unix)]
fn take_companion_range_lock_exclusive(
    file: &File,
    offset: u64,
) -> std::io::Result<RangeLockOutcome> {
    companion_range_lock(file, offset, RangeLockMode::Exclusive.lock_type(), false)
}

/// The two ways a caller can ask for one of these bytes: shared, which several
/// holders may have at once, and exclusive, which nobody else may hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RangeLockMode {
    Shared,
    Exclusive,
}

#[cfg(unix)]
impl RangeLockMode {
    fn lock_type(self) -> libc::c_short {
        match self {
            Self::Shared => libc::F_RDLCK as libc::c_short,
            Self::Exclusive => libc::F_WRLCK as libc::c_short,
        }
    }
}

#[cfg(unix)]
fn block_on_companion_range_lock(
    file: &File,
    offset: u64,
    mode: RangeLockMode,
) -> std::io::Result<()> {
    companion_range_lock(file, offset, mode.lock_type(), true).map(|_| ())
}

#[cfg(unix)]
fn release_companion_range_lock(file: &File, offset: u64) {
    let _ = companion_range_lock(file, offset, libc::F_UNLCK as libc::c_short, false);
}

#[cfg(not(unix))]
fn take_companion_range_lock_shared(
    _file: &File,
    _offset: u64,
) -> std::io::Result<RangeLockOutcome> {
    Err(std::io::Error::other(
        "advisory byte-range coordination is not implemented on this platform",
    ))
}

#[cfg(not(unix))]
fn take_companion_range_lock_exclusive(
    _file: &File,
    _offset: u64,
) -> std::io::Result<RangeLockOutcome> {
    Err(std::io::Error::other(
        "advisory byte-range coordination is not implemented on this platform",
    ))
}

#[cfg(not(unix))]
fn release_companion_range_lock(_file: &File, _offset: u64) {}

/// A pipe whose readable end a wait can sleep on and whose write end anybody
/// may poke to wake it.
///
/// A coordination wait sleeps on the kernel telling it something happened. A
/// caller cancelling, and a deadline arriving, are two more things that happen,
/// and they reach a sleeping wait the same way -- through a descriptor it is
/// already watching -- rather than by interrupting it. That is what lets a
/// wait be ended by its caller without a signal, a timer, or a thread left
/// behind holding the descriptor open.
#[cfg(unix)]
struct WakePipe {
    reader: std::os::fd::OwnedFd,
    writer: std::os::fd::OwnedFd,
}

#[cfg(unix)]
impl WakePipe {
    fn new() -> std::io::Result<Self> {
        use std::os::fd::FromRawFd as _;

        let mut ends = [0_i32; 2];
        let made = unsafe { libc::pipe2(ends.as_mut_ptr(), libc::O_CLOEXEC | libc::O_NONBLOCK) };
        if made != 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(Self {
            reader: unsafe { std::os::fd::OwnedFd::from_raw_fd(ends[0]) },
            writer: unsafe { std::os::fd::OwnedFd::from_raw_fd(ends[1]) },
        })
    }

    fn wake(&self) {
        use std::os::fd::AsRawFd as _;

        let byte = 1_u8;
        let _ = unsafe {
            libc::write(
                self.writer.as_raw_fd(),
                std::ptr::from_ref(&byte).cast::<libc::c_void>(),
                1,
            )
        };
    }

    fn drain(&self) {
        use std::os::fd::AsRawFd as _;

        let mut scratch = [0_u8; 64];
        loop {
            let read = unsafe {
                libc::read(
                    self.reader.as_raw_fd(),
                    scratch.as_mut_ptr().cast::<libc::c_void>(),
                    scratch.len(),
                )
            };
            if read <= 0 {
                return;
            }
        }
    }
}

/// A waker that pokes a wake pipe, so a future's completion reaches a wait
/// that is asleep on descriptors rather than on a condvar.
#[cfg(unix)]
struct PipeWaker(Arc<WakePipe>);

#[cfg(unix)]
impl std::task::Wake for PipeWaker {
    fn wake(self: Arc<Self>) {
        self.0.wake();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.wake();
    }
}

/// What a coordination wait sleeps on while a companion byte it wants is held
/// by somebody else.
///
/// The hold itself is not something the kernel will let a thread wait on and
/// also be woken from, so the wait does not try: it sleeps on NOTIFICATIONS
/// about the companion instead -- a holder closing its descriptor, a writer
/// publishing a record -- and re-asks for the byte each time one arrives. That
/// keeps every property the blocking acquisition had (the wake IS the event,
/// nothing polls, no interval decides how quickly a caller learns) and adds
/// the one it lacked: the wait belongs to the caller's own thread, so when the
/// caller stops waiting there is nothing left anywhere still queued for a hold
/// nobody wants.
#[cfg(unix)]
struct CoordinationWatch {
    wake: Arc<WakePipe>,
    #[cfg(target_os = "linux")]
    notifications: std::os::fd::OwnedFd,
    _listener: Option<contextdb_core::read_contract::CancellationListener>,
}

#[cfg(unix)]
impl CoordinationWatch {
    /// Arm the watch BEFORE asking for the byte, so a release that happens
    /// between the ask and the sleep is already queued rather than missed.
    fn arm(companion: &Path, stop: Option<&OwnerReadCancellation>) -> std::io::Result<Self> {
        let wake = Arc::new(WakePipe::new()?);
        let listener = stop.map(|stop| {
            let told = Arc::clone(&wake);
            stop.tell_on_cancel(move || told.wake())
        });
        #[cfg(target_os = "linux")]
        let notifications = companion_notifications(companion)?;
        #[cfg(not(target_os = "linux"))]
        let _ = companion;
        Ok(Self {
            wake,
            #[cfg(target_os = "linux")]
            notifications,
            _listener: listener,
        })
    }

    fn waker(&self) -> std::task::Waker {
        std::task::Waker::from(Arc::new(PipeWaker(Arc::clone(&self.wake))))
    }

    /// Sleep until something about this companion happens, or until somebody
    /// pokes the wake pipe. On a platform with no notification channel the
    /// caller's own thread takes the blocking acquisition instead: the answer
    /// is the same and nothing is left running either way, but a stop is
    /// noticed at the next wake rather than at once.
    #[cfg(target_os = "linux")]
    fn sleep(&self, held: &File, offset: u64) -> std::io::Result<()> {
        use std::os::fd::AsRawFd as _;

        let _ = (held, offset);
        let mut watched = [
            libc::pollfd {
                fd: self.notifications.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: self.wake.reader.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            },
        ];
        loop {
            let ready = unsafe { libc::poll(watched.as_mut_ptr(), 2, -1) };
            if ready >= 0 {
                break;
            }
            let error = std::io::Error::last_os_error();
            if error.kind() != std::io::ErrorKind::Interrupted {
                return Err(error);
            }
        }
        drain_companion_notifications(&self.notifications);
        self.wake.drain();
        Ok(())
    }

    #[cfg(not(target_os = "linux"))]
    fn sleep(&self, held: &File, offset: u64) -> std::io::Result<()> {
        self.wake.drain();
        block_on_companion_range_lock(held, offset, RangeLockMode::Exclusive)?;
        release_companion_range_lock(held, offset);
        Ok(())
    }
}

/// Watch a store's companion for the things that mean a coordination byte may
/// have changed hands: a holder closing its descriptor, which is what letting
/// go of a hold looks like from outside and is what the kernel does for a
/// holder that dies, and a writer publishing into the record.
#[cfg(target_os = "linux")]
fn companion_notifications(companion: &Path) -> std::io::Result<std::os::fd::OwnedFd> {
    use std::os::fd::FromRawFd as _;
    use std::os::unix::ffi::OsStrExt as _;

    let started = unsafe { libc::inotify_init1(libc::IN_NONBLOCK | libc::IN_CLOEXEC) };
    if started < 0 {
        return Err(std::io::Error::last_os_error());
    }
    let notifications = unsafe { std::os::fd::OwnedFd::from_raw_fd(started) };
    let pathname = std::ffi::CString::new(companion.as_os_str().as_bytes())
        .map_err(|_| std::io::Error::other("companion pathname contains an interior NUL"))?;
    let watched = unsafe {
        use std::os::fd::AsRawFd as _;
        libc::inotify_add_watch(
            notifications.as_raw_fd(),
            pathname.as_ptr(),
            libc::IN_CLOSE | libc::IN_MODIFY | libc::IN_ATTRIB,
        )
    };
    if watched < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(notifications)
}

/// Take every queued notification before re-asking for the byte, so a burst of
/// them costs one wake rather than one wake each.
#[cfg(target_os = "linux")]
fn drain_companion_notifications(notifications: &std::os::fd::OwnedFd) {
    use std::os::fd::AsRawFd as _;

    let mut scratch = [0_u8; 4096];
    loop {
        let read = unsafe {
            libc::read(
                notifications.as_raw_fd(),
                scratch.as_mut_ptr().cast::<libc::c_void>(),
                scratch.len(),
            )
        };
        if read <= 0 {
            return;
        }
    }
}

/// A live writer's claim on this store that has said nothing about serving
/// yet.
///
/// The trusted companion this was observed through travels with it, so the
/// wait that follows sleeps on the SAME open file description the observation
/// was made on: nothing can substitute the file underneath a caller between
/// seeing the claim and waiting for it.
pub(crate) struct UnsettledClaim {
    companion: File,
    /// The pathname the descriptor above was opened from, so the wait can
    /// watch the same file it is asking about.
    pathname: PathBuf,
}

/// How a wait inside a writer's claim window ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ClaimSettlement {
    /// The holder published its decision. Ask the store again; its own answer
    /// is there now.
    Settled,
    /// The caller's own declared budget ran out with the claim still held, or
    /// the window itself could not be observed here. Either way a writer is
    /// holding this store, so the caller is owed a not-serving answer and
    /// never an absent one.
    StillHeld,
}

/// A writer that holds this store and has not yet published what it decided
/// about serving inspection.
///
/// The question is asked of the CLAIM ITSELF, not of anything published: a
/// writer holds the companion's claim-window byte from the moment it first
/// takes the store until its decision is there to be read, and the kernel
/// takes that hold away if the writer dies. So a held byte means "somebody
/// owns this store and has not answered yet" no matter what the record says --
/// including when the record still describes a PREVIOUS run, and when there is
/// no record at all because the store is being created. Waiting for a
/// published `not_applicable` would have missed both, which is exactly the
/// stretch in which a caller was being told the store was free.
///
/// The store file is not opened, read, or locked, and nothing here delays the
/// writer: the companion beside the store carries the whole answer, and asking
/// for a shared byte a writer holds exclusively never stands in that writer's
/// way.
pub(crate) fn observe_unsettled_claim(database_path: &Path) -> Option<UnsettledClaim> {
    let companion = companion_path(database_path);
    // The same trust boundary every other companion reader applies: no symlink
    // followed, one current-user-owned regular file, still the file at this
    // pathname on both sides of the probe.
    let file = open_trusted_companion(&companion)?;
    let before_probe = file.metadata().ok()?;
    validate_companion_metadata(&companion, &file, &before_probe).ok()?;
    let held = match take_companion_range_lock_shared(&file, COMPANION_CLAIM_WINDOW_LOCK_OFFSET) {
        Ok(RangeLockOutcome::Taken) => {
            release_companion_range_lock(&file, COMPANION_CLAIM_WINDOW_LOCK_OFFSET);
            false
        }
        Ok(RangeLockOutcome::HeldByAnother) => true,
        Err(_) => return None,
    };
    validate_companion_metadata(&companion, &file, &before_probe).ok()?;
    held.then_some(UnsettledClaim {
        companion: file,
        pathname: companion,
    })
}

/// Wait out a writer's claim window inside the budget the caller declared.
///
/// Nothing polls and nothing sleeps for an interval: the caller's thread sleeps
/// on the companion's own notifications and re-asks for the byte each time one
/// arrives, so the writer publishing its decision IS what wakes this. The other
/// thing that may end it is the caller's own declared budget, and that budget
/// is spent against the CLOCK the caller supplied -- never against real time --
/// so a proof advances a clock instead of waiting an hour.
///
/// The writer is never blocked or delayed: a shared request never stands in the
/// way of the exclusive hold it already owns, and the store file is untouched.
pub(crate) fn wait_for_claim_settlement(
    claim: UnsettledClaim,
    budget_ms: u64,
    clock: &dyn contextdb_core::read_contract::DeadlineClock,
) -> ClaimSettlement {
    #[cfg(not(unix))]
    {
        let _ = (claim, budget_ms, clock);
        return ClaimSettlement::StillHeld;
    }
    #[cfg(unix)]
    {
        let UnsettledClaim {
            companion,
            pathname,
        } = claim;
        let deadline_ms = clock.now_ms().saturating_add(budget_ms);
        let Ok(watch) = CoordinationWatch::arm(&pathname, None) else {
            return ClaimSettlement::StillHeld;
        };
        let waker = watch.waker();
        let mut context = std::task::Context::from_waker(&waker);
        let mut expiry = clock.wait_until(deadline_ms);
        loop {
            match take_companion_range_lock_shared(&companion, COMPANION_CLAIM_WINDOW_LOCK_OFFSET) {
                Ok(RangeLockOutcome::Taken) => {
                    release_companion_range_lock(&companion, COMPANION_CLAIM_WINDOW_LOCK_OFFSET);
                    return ClaimSettlement::Settled;
                }
                Ok(RangeLockOutcome::HeldByAnother) => {}
                Err(_) => return ClaimSettlement::StillHeld,
            }
            if std::pin::Pin::as_mut(&mut expiry)
                .poll(&mut context)
                .is_ready()
            {
                return ClaimSettlement::StillHeld;
            }
            if watch
                .sleep(&companion, COMPANION_CLAIM_WINDOW_LOCK_OFFSET)
                .is_err()
            {
                return ClaimSettlement::StillHeld;
            }
        }
    }
}

/// How many times one reader will re-observe a companion that changed hands
/// underneath it before giving no verdict at all. A handover is a moment, not
/// a state, so a couple of attempts settle it; a reader that still cannot see
/// one writer says nothing rather than guessing which one it saw.
const RECORDED_OWNER_OBSERVATION_ATTEMPTS: usize = 3;

/// Open a companion the way every trusted holder of one opens it: no symlink
/// followed, not inherited across an exec, and read-only because a reader has
/// no business writing one.
#[cfg(unix)]
fn open_trusted_companion(companion: &Path) -> Option<File> {
    use std::os::unix::fs::OpenOptionsExt as _;

    OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(companion)
        .ok()
}

#[cfg(not(unix))]
fn open_trusted_companion(companion: &Path) -> Option<File> {
    File::open(companion).ok()
}

/// Open a companion the way a would-be WRITER of this store opens one to ask
/// whether readers are still holding it: no symlink followed, not inherited
/// across an exec, and readable AND writable, because the exclusive side of an
/// advisory byte-range lock is only available on a descriptor open for writing.
/// Nothing is ever written through it.
#[cfg(unix)]
fn open_trusted_companion_for_exclusive_hold(companion: &Path) -> std::io::Result<File> {
    use std::os::unix::fs::OpenOptionsExt as _;

    OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(companion)
}

#[cfg(not(unix))]
fn open_trusted_companion_for_exclusive_hold(companion: &Path) -> std::io::Result<File> {
    OpenOptions::new().read(true).write(true).open(companion)
}

/// A direct reader's proof that it is holding this store's committed image.
///
/// The hold is a shared advisory byte-range lock on the companion beside the
/// store -- taken before the committed file is opened and let go only after
/// the decoded image no longer needs it -- so it is exactly as long-lived as
/// the reader's grip on the store and not one instant longer. It is the thing
/// a would-be writer waits on, and unlike a breadcrumb it cannot fail to be
/// published and then leave a real reader invisible: the kernel takes it away
/// when this process dies, whatever else does or does not get cleaned up.
///
/// The store folder is untouched. This is the companion, beside the store.
struct ReaderStoreHold {
    file: File,
}

impl ReaderStoreHold {
    /// Take the hold, waiting out the momentary exclusive request a waiter
    /// makes. Waiting here is what stops a reader arriving in that instant
    /// from holding the store with nothing to show for it -- which is the
    /// invisible hold this whole mechanism exists to abolish.
    fn take(database_path: &Path) -> Option<Self> {
        let companion = companion_path(database_path);
        let file = open_trusted_companion(&companion)?;
        block_on_companion_range_lock(
            &file,
            COMPANION_READER_HOLD_LOCK_OFFSET,
            RangeLockMode::Shared,
        )
        .ok()?;
        Some(Self { file })
    }
}

impl Drop for ReaderStoreHold {
    fn drop(&mut self) {
        release_companion_range_lock(&self.file, COMPANION_READER_HOLD_LOCK_OFFSET);
    }
}

/// The published record in the companion this file is open on.
fn active_companion_record(file: &mut File) -> Option<EncodedCompanionRecord> {
    read_companion_state(file).ok()?.active_record
}

/// Whether a writer is holding this store right now, asked of the advisory
/// companion this file is already open on.
///
/// A reader is the only caller, and a reader must not open the store to find
/// out: taking the shared side of the same lock a writer takes exclusively
/// answers it and is released immediately.
fn writer_holds_companion(file: &File) -> Option<bool> {
    match fs2::FileExt::try_lock_shared(file) {
        Ok(()) => {
            unlock_file(file);
            Some(false)
        }
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => Some(true),
        Err(_) => None,
    }
}

#[allow(dead_code)]
fn inspect_companion_record(
    database_path: &Path,
) -> std::result::Result<EncodedCompanionRecord, String> {
    read_active_companion_slot(database_path)
        .and_then(|(payload, checksum)| recover_companion_record(&payload, checksum))
}

#[allow(dead_code)]
fn read_active_companion_slot(
    database_path: &Path,
) -> std::result::Result<(Vec<u8>, [u8; 32]), String> {
    let path = companion_path(database_path);
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    }
    let mut file = options
        .open(&path)
        .map_err(|error| format!("open companion {}: {error}", path.display()))?;
    let metadata = file
        .metadata()
        .map_err(|error| format!("inspect companion {}: {error}", path.display()))?;
    validate_companion_metadata(&path, &file, &metadata).map_err(|error| error.to_string())?;
    let state = read_companion_state(&mut file)?;
    let record = state
        .active_record
        .ok_or_else(|| "companion has no published record".to_owned())?;
    Ok((record.checksummed_payload, record.stored_checksum))
}

/// The committed image reconstructed by the side-effect-free persistence
/// loader. The direct backend will consume this type instead of reopening the
/// writable database path.
#[allow(dead_code)]
pub(crate) struct ReadPersistenceImage {
    // Frozen test-adapter compatibility projection. The direct backend uses
    // `current_vectors` below, whose key retains both table and column.
    vectors: BTreeMap<(String, i64), Vec<f32>>,
    table_meta: HashMap<String, TableMeta>,
    relational_tables: HashMap<String, Vec<VersionedRow>>,
    forward_edges: Vec<AdjEntry>,
    reverse_edges: Vec<AdjEntry>,
    vector_entries: Vec<VectorEntry>,
    current_vectors: HashMap<(VectorIndexRef, RowId), Vec<f32>>,
    sync_source_lsns: HashMap<(String, RowId), Lsn>,
    sync_source_kinds: HashMap<(String, RowId), SyncSourceKind>,
    change_log: Vec<ChangeLogEntry>,
    ddl_log: Vec<(Lsn, DdlChange)>,
    commit_index: BTreeMap<Lsn, TxId>,
    config_values: Vec<(String, Vec<u8>)>,
    sink_audit: Vec<(String, Vec<u8>)>,
    trigger_audit: Vec<(String, Vec<u8>)>,
    trigger_audit_stamps: Vec<(String, u64)>,
    sink_queues: BTreeMap<String, Vec<(u64, Vec<u8>)>>,
    /// How large the store file was when this image was read out of it.
    ///
    /// It is the same number a writer reports as disk usage -- the length of
    /// the same file -- measured once, where the reader had the file open,
    /// and carried with the image it belongs to. Absent only if the file's
    /// length could not be read at all.
    store_file_bytes: Option<u64>,
}

/// Handle-free state consumed by the direct backend after hydration has
/// released the read transaction, Redb database, source file, and runtime
/// breadcrumb. Keeping this type crate-private prevents persistence handles
/// from becoming part of the read surface.
#[allow(dead_code)]
pub(crate) struct ReadPersistenceImageParts {
    pub(crate) table_meta: HashMap<String, TableMeta>,
    pub(crate) relational_tables: HashMap<String, Vec<VersionedRow>>,
    pub(crate) forward_edges: Vec<AdjEntry>,
    pub(crate) reverse_edges: Vec<AdjEntry>,
    pub(crate) vector_entries: Vec<VectorEntry>,
    pub(crate) current_vectors: HashMap<(VectorIndexRef, RowId), Vec<f32>>,
    pub(crate) sync_source_lsns: HashMap<(String, RowId), Lsn>,
    pub(crate) sync_source_kinds: HashMap<(String, RowId), SyncSourceKind>,
    pub(crate) change_log: Vec<ChangeLogEntry>,
    pub(crate) ddl_log: Vec<(Lsn, DdlChange)>,
    pub(crate) commit_index: BTreeMap<Lsn, TxId>,
    pub(crate) config_values: Vec<(String, Vec<u8>)>,
    pub(crate) sink_audit: Vec<(String, Vec<u8>)>,
    pub(crate) trigger_audit: Vec<(String, Vec<u8>)>,
    pub(crate) trigger_audit_stamps: Vec<(String, u64)>,
    pub(crate) sink_queues: BTreeMap<String, Vec<(u64, Vec<u8>)>>,
    pub(crate) store_file_bytes: Option<u64>,
}

impl ReadPersistenceImage {
    pub(crate) fn into_runtime_parts(self) -> ReadPersistenceImageParts {
        ReadPersistenceImageParts {
            table_meta: self.table_meta,
            relational_tables: self.relational_tables,
            forward_edges: self.forward_edges,
            reverse_edges: self.reverse_edges,
            vector_entries: self.vector_entries,
            current_vectors: self.current_vectors,
            sync_source_lsns: self.sync_source_lsns,
            sync_source_kinds: self.sync_source_kinds,
            change_log: self.change_log,
            ddl_log: self.ddl_log,
            commit_index: self.commit_index,
            config_values: self.config_values,
            sink_audit: self.sink_audit,
            trigger_audit: self.trigger_audit,
            trigger_audit_stamps: self.trigger_audit_stamps,
            sink_queues: self.sink_queues,
            store_file_bytes: self.store_file_bytes,
        }
    }
}

pub(crate) struct ReadPersistenceReleaseReceipt {
    breadcrumb_path: Option<PathBuf>,
    source_accesses: u64,
    _seal: ReadPersistenceReleaseSeal,
}

struct ReadPersistenceReleaseSeal;

impl ReadPersistenceReleaseReceipt {
    pub(crate) fn breadcrumb_path(&self) -> Option<&Path> {
        self.breadcrumb_path.as_deref()
    }

    pub(crate) fn source_accesses(&self) -> u64 {
        self.source_accesses
    }

    pub(crate) fn validate_released(&self) -> std::result::Result<(), LoadReadImageError> {
        if self.source_accesses == 0 {
            return Err(LoadReadImageError::Release(
                "sealed release receipt records no persistence source access".to_owned(),
            ));
        }
        let Some(path) = &self.breadcrumb_path else {
            return Ok(());
        };
        match std::fs::symlink_metadata(path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(LoadReadImageError::Release(format!(
                "cannot verify removal of reader breadcrumb {}: {error}",
                path.display()
            ))),
            Ok(_) => Err(LoadReadImageError::Release(format!(
                "reader breadcrumb still exists after source release: {}",
                path.display()
            ))),
        }
    }
}

pub(crate) struct ReadPersistenceLoad {
    image: ReadPersistenceImage,
    release_receipt: ReadPersistenceReleaseReceipt,
}

impl ReadPersistenceLoad {
    pub(crate) fn into_parts(self) -> (ReadPersistenceImage, ReadPersistenceReleaseReceipt) {
        (self.image, self.release_receipt)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum ReadImageRequiresWriterCause {
    #[error("non-monotonic commit index")]
    NonMonotonicCommitIndex,
    #[error("invalid vector reference")]
    InvalidVectorReference,
    #[error("relational and vector values diverge")]
    RowVectorDivergence,
    #[error("writable hydration would sanitize or supplement state")]
    WritableSanitizerWouldChange,
    #[error("commit index is incomplete")]
    MissingCommitIndex,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum ReadImageCorruptionCause {
    #[error("malformed durable record")]
    MalformedRecord,
    #[error("unsupported legacy layout")]
    LegacyLayout,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum LoadReadImageErrorKind {
    NotImplemented,
    RequiresWriter(ReadImageRequiresWriterCause),
    StoreMissing,
    Contention,
    Corrupt(ReadImageCorruptionCause),
    InvalidState,
    Io,
    Release,
}

#[derive(Debug, thiserror::Error)]
#[allow(dead_code)]
pub(crate) enum LoadReadImageError {
    #[error("load_read_image production path is not implemented")]
    NotImplemented,
    #[error("read-image source failed: {0}")]
    Source(String),
    #[error("read-image release proof failed: {0}")]
    Release(String),
    #[error("direct read requires a writer ({cause}): {reason}")]
    RequiresWriter {
        cause: ReadImageRequiresWriterCause,
        reason: String,
    },
    #[error("read-image store is not there: {reason}")]
    StoreMissing { reason: String },
    #[error("read-image source is contended: {reason}")]
    Contention { reason: String },
    #[error("read-image durable state is corrupt ({cause}): {reason}")]
    Corrupt {
        cause: ReadImageCorruptionCause,
        reason: String,
    },
    #[error("read-image durable state is invalid: {reason}")]
    InvalidState { reason: String },
    #[error("read-image I/O failed during {operation}: {reason}")]
    Io {
        operation: &'static str,
        reason: String,
    },
}

#[allow(dead_code)]
impl LoadReadImageError {
    pub(crate) fn kind(&self) -> LoadReadImageErrorKind {
        match self {
            Self::NotImplemented => LoadReadImageErrorKind::NotImplemented,
            Self::Source(_) => LoadReadImageErrorKind::Io,
            Self::Release(_) => LoadReadImageErrorKind::Release,
            Self::RequiresWriter { cause, .. } => LoadReadImageErrorKind::RequiresWriter(*cause),
            Self::StoreMissing { .. } => LoadReadImageErrorKind::StoreMissing,
            Self::Contention { .. } => LoadReadImageErrorKind::Contention,
            Self::Corrupt { cause, .. } => LoadReadImageErrorKind::Corrupt(*cause),
            Self::InvalidState { .. } => LoadReadImageErrorKind::InvalidState,
            Self::Io { .. } => LoadReadImageErrorKind::Io,
        }
    }

    pub(crate) fn requires_writer_cause(&self) -> Option<ReadImageRequiresWriterCause> {
        match self.kind() {
            LoadReadImageErrorKind::RequiresWriter(cause) => Some(cause),
            _ => None,
        }
    }

    pub(crate) fn corruption_cause(&self) -> Option<ReadImageCorruptionCause> {
        match self.kind() {
            LoadReadImageErrorKind::Corrupt(cause) => Some(cause),
            _ => None,
        }
    }

    /// The reader-facing answer to a refused hydration.
    ///
    /// Persistence owns the damage vocabulary, so it also owns the single
    /// translation of that vocabulary into the words a reader is given: what
    /// is wrong with the store, and whether a writable reopen is the way out.
    /// Keeping the translation here is what lets the reading surface stay
    /// closed over its own types instead of learning how persistence spells a
    /// decode failure.
    pub(crate) fn into_direct_reader_error(
        self,
    ) -> crate::direct_file_reader::DirectFileReaderError {
        use crate::direct_file_reader::{
            DirectFileReaderError, DirectReaderPrerequisite, DirectRepairRequiredCause,
            DirectStoreDiagnostic,
        };

        let reason = self.to_string();
        match self {
            Self::Corrupt {
                cause: ReadImageCorruptionCause::MalformedRecord,
                ..
            } => {
                DirectFileReaderError::CorruptStore(DirectStoreDiagnostic::MalformedRecord, reason)
            }
            Self::Corrupt {
                cause: ReadImageCorruptionCause::LegacyLayout,
                ..
            } => DirectFileReaderError::LegacyLayout(DirectStoreDiagnostic::LegacyLayout, reason),
            Self::RequiresWriter { cause, .. } => {
                let cause = match cause {
                    ReadImageRequiresWriterCause::NonMonotonicCommitIndex => {
                        DirectRepairRequiredCause::NonMonotonicCommitIndex
                    }
                    ReadImageRequiresWriterCause::InvalidVectorReference => {
                        DirectRepairRequiredCause::InvalidVectorReference
                    }
                    ReadImageRequiresWriterCause::RowVectorDivergence => {
                        DirectRepairRequiredCause::RowVectorDivergence
                    }
                    ReadImageRequiresWriterCause::WritableSanitizerWouldChange => {
                        DirectRepairRequiredCause::WritableSanitizerWouldChange
                    }
                    ReadImageRequiresWriterCause::MissingCommitIndex => {
                        DirectRepairRequiredCause::MissingCommitIndex
                    }
                };
                let failure = ReadFailure::new(
                    ReadFailureKind::DirectReadRequiresWriter,
                    ReadFailureDetail::Reason { reason },
                )
                .expect("direct-read refusal carries a plain reason");
                DirectFileReaderError::DirectReadRequiresWriter { failure, cause }
            }
            Self::StoreMissing { .. } => DirectFileReaderError::StoreNotFound {
                failure: ReadFailure::new(
                    ReadFailureKind::StoreNotFound,
                    ReadFailureDetail::Reason { reason },
                )
                .expect("a missing-store refusal carries a plain reason"),
            },
            Self::Contention { .. } => DirectFileReaderError::Contended { reason },
            Self::NotImplemented
            | Self::Source(_)
            | Self::Release(_)
            | Self::InvalidState { .. }
            | Self::Io { .. } => DirectFileReaderError::MissingPrerequisite(
                DirectReaderPrerequisite::PersistenceLoadReadImage,
            ),
        }
    }
}

/// Sole persistence entry point for direct-file hydration. Its implementation
/// must create the runtime breadcrumb, acquire a real
/// `redb::ReadOnlyDatabase`, decode one complete committed image, release the
/// redb handle, and remove that breadcrumb. The test adapter below delegates
/// only to this function and never supplies a decoder fallback.
#[allow(dead_code)]
pub(crate) fn load_read_image(
    path: &Path,
) -> std::result::Result<ReadPersistenceLoad, LoadReadImageError> {
    let mut source = ReadImageSource::start(path)?;
    let image = source.with_read_transaction(decode_complete_read_image)?;
    source.finish(image)
}

/// Sole identity source for the companion record's owner-user field. The
/// record writer must not substitute database-file metadata for this value.
#[allow(dead_code)]
fn companion_effective_user_identity() -> LocalUserIdentity {
    #[cfg(feature = "test-seams")]
    read_persistence_test_scaffold::note_companion_effective_user_source_call_for_test();
    #[cfg(feature = "test-seams")]
    if let Some(identity) =
        read_persistence_test_scaffold::companion_effective_user_override_for_test()
    {
        return identity;
    }
    #[cfg(unix)]
    {
        LocalUserIdentity(nix::unistd::Uid::effective().as_raw() as u64)
    }
    #[cfg(not(unix))]
    {
        LocalUserIdentity(0)
    }
}

fn encode_channel_address(address: ChannelAddress) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(address.0.len() * 2);
    for byte in address.0 {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

/// Canonical runtime directory shared by the per-hydration breadcrumb files
/// for one database: the directory beside that database's channel socket.
/// Both the runtime directory and the address are derived by local transport
/// from the runtime ROOT every caller here is handed, so a reader publishing
/// its identity and a writer looking for it reach the same directory.
#[allow(dead_code)]
fn reader_breadcrumb_directory(
    database_path: &Path,
    runtime_root: &Path,
) -> std::io::Result<std::path::PathBuf> {
    let address = derive_channel_address(database_path)
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    Ok(reader_breadcrumb_root(runtime_root).join(encode_channel_address(address)))
}

/// The runtime directory every reader breadcrumb directory for this process
/// sits in: the runtime root's `contextdb` child, derived where local
/// transport derives the channel directory and never guessed from how the
/// root happens to be spelled. Both sides of a deployment derive it from the
/// SAME root, which is what keeps a writer looking where its readers publish.
fn reader_breadcrumb_root(runtime_root: &Path) -> PathBuf {
    RuntimeDirectory::within(runtime_root).path().to_path_buf()
}

const READER_BREADCRUMB_MAGIC: &[u8] = b"contextdb-reader";
const ANONYMOUS_READER_BREADCRUMB: &[u8] = b"contextdb-reader-anonymous-v1";
const MAX_READER_PROCESS_NAME_BYTES: usize = 255;
const MAX_READER_BREADCRUMB_BYTES: usize = READER_BREADCRUMB_MAGIC.len()
    + std::mem::size_of::<u32>()
    + std::mem::size_of::<u64>()
    + std::mem::size_of::<u32>()
    + MAX_READER_PROCESS_NAME_BYTES;

fn safe_reader_process_display_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= MAX_READER_PROCESS_NAME_BYTES
        && name
            .bytes()
            .all(|byte| matches!(byte, b' '..=b'~') && byte != b'/' && byte != b'\\')
}

fn encode_reader_breadcrumb(breadcrumb: &ReaderBreadcrumb) -> std::io::Result<Vec<u8>> {
    let name = breadcrumb.process_name.as_bytes();
    if name.len() > MAX_READER_PROCESS_NAME_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "reader process name exceeds the breadcrumb limit",
        ));
    }
    let name_len = u32::try_from(name.len()).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "reader process name is too long",
        )
    })?;
    let mut bytes = Vec::with_capacity(READER_BREADCRUMB_MAGIC.len() + 4 + 8 + 4 + name.len());
    bytes.extend_from_slice(READER_BREADCRUMB_MAGIC);
    bytes.extend_from_slice(&breadcrumb.process_id.to_le_bytes());
    bytes.extend_from_slice(&breadcrumb.process_start.0.to_le_bytes());
    bytes.extend_from_slice(&name_len.to_le_bytes());
    bytes.extend_from_slice(name);
    Ok(bytes)
}

fn decode_reader_breadcrumb(bytes: &[u8]) -> std::io::Result<ReaderBreadcrumb> {
    let mut cursor = CompanionPayloadCursor {
        payload: bytes,
        offset: 0,
    };
    if cursor
        .take(READER_BREADCRUMB_MAGIC.len())
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?
        != READER_BREADCRUMB_MAGIC
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "reader breadcrumb magic mismatch",
        ));
    }
    let process_id = cursor
        .u32()
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    let process_start = contextdb_core::read_contract::ProcessStartIdentity(
        cursor
            .u64()
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?,
    );
    let name_len = usize::try_from(
        cursor
            .u32()
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?,
    )
    .map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "reader process-name length does not fit usize",
        )
    })?;
    let process_name = String::from_utf8(
        cursor
            .take(name_len)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?
            .to_vec(),
    )
    .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    cursor
        .finish()
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    Ok(ReaderBreadcrumb {
        process_id,
        process_name,
        process_start,
    })
}

fn reader_runtime_user() -> Option<u64> {
    #[cfg(unix)]
    {
        Some(nix::unistd::Uid::effective().as_raw() as u64)
    }
    #[cfg(not(unix))]
    {
        None
    }
}

fn validate_reader_runtime_directory(runtime_directory: &Path) -> std::io::Result<()> {
    if !runtime_directory.is_absolute() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "reader runtime directory is not absolute",
        ));
    }
    let metadata = std::fs::symlink_metadata(runtime_directory)?;
    if metadata.file_type().is_symlink() || !metadata.file_type().is_dir() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "reader runtime directory is not a direct directory",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;

        let Some(user) = reader_runtime_user() else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "reader runtime user is unavailable",
            ));
        };
        if metadata.uid() as u64 != user
            || metadata.mode() & 0o077 != 0
            || metadata.mode() & 0o200 == 0
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "reader runtime directory is not owner-only and writable",
            ));
        }
    }
    #[cfg(not(unix))]
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "reader runtime validation is unavailable on this platform",
        ));
    }
    Ok(())
}

fn validate_reader_breadcrumb_directory(
    runtime_root: &Path,
    directory: &Path,
) -> std::io::Result<()> {
    let contextdb_directory = reader_breadcrumb_root(runtime_root);
    for path in [&contextdb_directory, directory] {
        let metadata = std::fs::symlink_metadata(path)?;
        if metadata.file_type().is_symlink() || !metadata.file_type().is_dir() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "reader breadcrumb directory is not a direct directory",
            ));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt as _;

            let Some(user) = reader_runtime_user() else {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "reader runtime user is unavailable",
                ));
            };
            if metadata.uid() as u64 != user || metadata.mode() & 0o077 != 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "reader breadcrumb directory is not owner-only",
                ));
            }
        }
    }
    Ok(())
}

fn create_reader_breadcrumb_directory(path: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt as _;

        let mut builder = std::fs::DirBuilder::new();
        builder.mode(0o700);
        match builder.create(path) {
            Ok(()) => {
                use std::os::unix::fs::PermissionsExt as _;
                std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
            Err(error) => Err(error),
        }
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "reader breadcrumb directories are unavailable on this platform",
        ))
    }
}

/// The runtime directory this process reads and publishes reader breadcrumbs
/// in when nobody has supplied one: the platform base's owner-only `contextdb`
/// child, resolved ONCE here so no caller re-derives it from a root.
fn default_reader_runtime_directory() -> Option<RuntimeDirectory> {
    #[cfg(target_os = "linux")]
    let requested = std::env::var_os("XDG_RUNTIME_DIR")
        .map(PathBuf::from)
        .or_else(|| {
            reader_runtime_user().map(|user| PathBuf::from("/run/user").join(user.to_string()))
        })?;
    #[cfg(target_os = "macos")]
    let requested = {
        let environment = crate::local_transport::ProcessRuntimeDirectoryEnvironment;
        crate::local_transport::RuntimeDirectoryEnvironment::macos_user_temporary_directory(
            &environment,
        )
        .ok()?
    };
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        return None;
    }
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        validate_reader_runtime_directory(&requested)
            .ok()
            .map(|()| RuntimeDirectory::within(&requested))
    }
}

fn current_reader_breadcrumb() -> std::io::Result<ReaderBreadcrumb> {
    let process_id = std::process::id();
    #[cfg(target_os = "linux")]
    {
        let stat = std::fs::read_to_string("/proc/self/stat")?;
        let after_name = stat
            .rfind(") ")
            .map(|index| &stat[index + 2..])
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Linux process stat has no command terminator",
                )
            })?;
        let value = after_name
            .split_whitespace()
            .nth(19)
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Linux process stat has no start identity",
                )
            })?
            .parse::<u64>()
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
        let process_start = contextdb_core::read_contract::ProcessStartIdentity(value);
        let mut names = Vec::new();
        if let Ok(name) = std::fs::read_to_string("/proc/self/comm") {
            names.push(name.strip_suffix('\n').unwrap_or(&name).to_owned());
        }
        if let Ok(executable) = std::fs::read_link("/proc/self/exe")
            && let Some(name) = executable.file_name().and_then(|name| name.to_str())
        {
            names.push(name.to_owned());
        }
        if let Ok(command_line) = std::fs::read("/proc/self/cmdline")
            && let Some(argument_zero) = command_line.split(|byte| *byte == 0).next()
            && let Ok(argument_zero) = std::str::from_utf8(argument_zero)
            && let Some(name) = Path::new(argument_zero)
                .file_name()
                .and_then(|name| name.to_str())
        {
            names.push(name.to_owned());
        }
        names.sort();
        names.dedup();
        for process_name in names
            .into_iter()
            .filter(|name| safe_reader_process_display_name(name))
        {
            let breadcrumb = ReaderBreadcrumb {
                process_id,
                process_name,
                process_start,
            };
            if verified_linux_reader_process(&breadcrumb).is_some() {
                return Ok(breadcrumb);
            }
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "the current process name could not be verified",
        ))
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = process_id;
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "the operating system does not expose a verified process-start identity",
        ))
    }
}

fn next_reader_breadcrumb_path(
    database_path: &Path,
    runtime_root: &Path,
    breadcrumb: &ReaderBreadcrumb,
) -> std::io::Result<PathBuf> {
    let directory = reader_breadcrumb_directory(database_path, runtime_root)?;
    let sequence = READER_BREADCRUMB_SEQUENCE.fetch_add(1, Ordering::SeqCst);
    let name = format!(
        "{:010}-{:016x}-{sequence:016x}.reader",
        breadcrumb.process_id, breadcrumb.process_start.0
    );
    Ok(directory.join(name))
}

fn next_anonymous_reader_breadcrumb_path(
    database_path: &Path,
    runtime_root: &Path,
) -> std::io::Result<PathBuf> {
    let directory = reader_breadcrumb_directory(database_path, runtime_root)?;
    let sequence = READER_BREADCRUMB_SEQUENCE.fetch_add(1, Ordering::SeqCst);
    let name = format!(
        "anonymous-{}-{sequence:016x}.reader",
        uuid::Uuid::new_v4().simple()
    );
    Ok(directory.join(name))
}

struct ReaderBreadcrumbGuard {
    file: Option<File>,
    path: PathBuf,
    metadata: std::fs::Metadata,
    removed: bool,
}

/// Take away the directories this reader created for its own breadcrumb, and
/// nothing above them: the per-store directory and the `contextdb` child this
/// process created to hold it. The runtime ROOT above them is never touched --
/// a directory an operator supplied is theirs, and removing it the moment no
/// reader is using it would take the owner's channel down with it.
fn remove_empty_breadcrumb_directories(path: &Path) {
    let Some(store_directory) = path.parent() else {
        return;
    };
    if std::fs::remove_dir(store_directory).is_ok()
        && let Some(contextdb_directory) = store_directory.parent()
    {
        let _ = std::fs::remove_dir(contextdb_directory);
    }
}

impl ReaderBreadcrumbGuard {
    fn create(
        database_path: &Path,
        runtime_root: &Path,
        breadcrumb: Option<&ReaderBreadcrumb>,
    ) -> std::io::Result<Self> {
        validate_reader_runtime_directory(runtime_root)?;
        let path = match breadcrumb {
            Some(breadcrumb) => {
                next_reader_breadcrumb_path(database_path, runtime_root, breadcrumb)?
            }
            None => next_anonymous_reader_breadcrumb_path(database_path, runtime_root)?,
        };
        let directory = path.parent().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "reader breadcrumb has no runtime directory",
            )
        })?;
        let contextdb_directory = reader_breadcrumb_root(runtime_root);
        create_reader_breadcrumb_directory(&contextdb_directory)?;
        create_reader_breadcrumb_directory(directory)?;
        validate_reader_breadcrumb_directory(runtime_root, directory)?;
        let mut options = OpenOptions::new();
        options.read(true).write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options
                .mode(0o600)
                .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
        }
        let mut file = options.open(&path)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
        }
        let publish = (|| -> std::io::Result<std::fs::Metadata> {
            fs2::FileExt::lock_exclusive(&file)?;
            match breadcrumb {
                Some(breadcrumb) => file.write_all(&encode_reader_breadcrumb(breadcrumb)?)?,
                None => file.write_all(ANONYMOUS_READER_BREADCRUMB)?,
            }
            file.sync_data()?;
            let metadata = file.metadata()?;
            if !reader_breadcrumb_path_still_names(&path, &metadata) {
                return Err(std::io::Error::other(
                    "reader breadcrumb path changed during publication",
                ));
            }
            Ok(metadata)
        })();
        let metadata = match publish {
            Ok(metadata) => metadata,
            Err(error) => {
                if let Ok(metadata) = file.metadata()
                    && reader_breadcrumb_path_still_names(&path, &metadata)
                {
                    let _ = std::fs::remove_file(&path);
                }
                let _ = fs2::FileExt::unlock(&file);
                drop(file);
                return Err(error);
            }
        };
        Ok(Self {
            file: Some(file),
            path,
            metadata,
            removed: false,
        })
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn release(mut self) -> std::io::Result<PathBuf> {
        let removal = match std::fs::symlink_metadata(&self.path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
            Ok(_) if reader_breadcrumb_path_still_names(&self.path, &self.metadata) => {
                std::fs::remove_file(&self.path)
            }
            Ok(_) => Err(std::io::Error::other(
                "reader breadcrumb path no longer names the published inode",
            )),
        };
        let unlock = if let Some(file) = self.file.take() {
            let result = fs2::FileExt::unlock(&file);
            drop(file);
            result
        } else {
            Ok(())
        };
        removal?;
        unlock?;
        remove_empty_breadcrumb_directories(&self.path);
        self.removed = true;
        Ok(self.path.clone())
    }
}

impl Drop for ReaderBreadcrumbGuard {
    fn drop(&mut self) {
        if let Some(file) = self.file.take() {
            if reader_breadcrumb_path_still_names(&self.path, &self.metadata) {
                let _ = std::fs::remove_file(&self.path);
            }
            let _ = fs2::FileExt::unlock(&file);
            drop(file);
        }
        if !self.removed {
            remove_empty_breadcrumb_directories(&self.path);
        }
    }
}

#[cfg(target_os = "linux")]
fn verified_linux_reader_process(breadcrumb: &ReaderBreadcrumb) -> Option<ReaderProcessIdentity> {
    if !safe_reader_process_display_name(&breadcrumb.process_name) {
        return None;
    }
    let process_directory = PathBuf::from(format!("/proc/{}", breadcrumb.process_id));
    let stat = std::fs::read_to_string(process_directory.join("stat")).ok()?;
    let after_name = stat.rfind(") ").map(|index| &stat[index + 2..])?;
    let process_start = after_name
        .split_whitespace()
        .nth(19)?
        .parse::<u64>()
        .ok()
        .map(contextdb_core::read_contract::ProcessStartIdentity)?;
    let mut names = HashSet::new();
    if let Ok(name) = std::fs::read_to_string(process_directory.join("comm")) {
        names.insert(name.strip_suffix('\n').unwrap_or(&name).to_owned());
    }
    if let Ok(executable) = std::fs::read_link(process_directory.join("exe"))
        && let Some(name) = executable.file_name().and_then(|name| name.to_str())
    {
        names.insert(name.to_owned());
    }
    if let Ok(command_line) = std::fs::read(process_directory.join("cmdline"))
        && let Some(argument_zero) = command_line.split(|byte| *byte == 0).next()
        && let Ok(argument_zero) = std::str::from_utf8(argument_zero)
        && let Some(name) = Path::new(argument_zero)
            .file_name()
            .and_then(|name| name.to_str())
    {
        names.insert(name.to_owned());
    }
    if !names.contains(&breadcrumb.process_name) {
        return None;
    }
    Some(ReaderProcessIdentity {
        process_id: breadcrumb.process_id,
        process_start,
    })
}

fn verify_locked_reader_breadcrumb(breadcrumb: ReaderBreadcrumb) -> Option<ReaderBreadcrumb> {
    #[cfg(target_os = "linux")]
    {
        let observed = verified_linux_reader_process(&breadcrumb)?;
        breadcrumb.is_live_for(&observed).then_some(breadcrumb)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = breadcrumb;
        None
    }
}

pub(crate) struct LockedReaderBreadcrumb {
    pub(crate) path: PathBuf,
    pub(crate) breadcrumb: Option<ReaderBreadcrumb>,
}

#[cfg(unix)]
fn trusted_reader_breadcrumb_metadata(metadata: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt as _;

    metadata.file_type().is_file()
        && metadata.uid() == nix::unistd::Uid::effective().as_raw()
        && metadata.mode() & 0o7777 == 0o600
        && metadata.nlink() == 1
        && metadata.len() <= MAX_READER_BREADCRUMB_BYTES as u64
}

#[cfg(unix)]
fn same_reader_breadcrumb_inode(left: &std::fs::Metadata, right: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt as _;

    left.dev() == right.dev()
        && left.ino() == right.ino()
        && left.uid() == right.uid()
        && left.mode() == right.mode()
        && left.nlink() == right.nlink()
        && left.len() == right.len()
}

#[cfg(unix)]
fn open_trusted_reader_breadcrumb(path: &Path) -> std::io::Result<(File, std::fs::Metadata)> {
    let entry_metadata = std::fs::symlink_metadata(path)?;
    if !trusted_reader_breadcrumb_metadata(&entry_metadata) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "reader breadcrumb entry is not a trusted private regular file",
        ));
    }
    let mut options = OpenOptions::new();
    options.read(true).write(true);
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    }
    let file = options.open(path)?;
    let descriptor_metadata = file.metadata()?;
    if !trusted_reader_breadcrumb_metadata(&descriptor_metadata)
        || !same_reader_breadcrumb_inode(&entry_metadata, &descriptor_metadata)
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "reader breadcrumb changed while it was opened",
        ));
    }
    Ok((file, descriptor_metadata))
}

#[cfg(not(unix))]
fn open_trusted_reader_breadcrumb(_path: &Path) -> std::io::Result<(File, std::fs::Metadata)> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "reader breadcrumb inspection requires Unix no-follow opens",
    ))
}

#[cfg(unix)]
fn reader_breadcrumb_path_still_names(path: &Path, inspected: &std::fs::Metadata) -> bool {
    std::fs::symlink_metadata(path).is_ok_and(|current| {
        trusted_reader_breadcrumb_metadata(&current)
            && same_reader_breadcrumb_inode(inspected, &current)
    })
}

#[cfg(not(unix))]
fn reader_breadcrumb_path_still_names(_path: &Path, _inspected: &std::fs::Metadata) -> bool {
    false
}

fn read_locked_reader_breadcrumb(
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

fn reclaim_reader_breadcrumb_if_same(path: &Path, inspected: &std::fs::Metadata) {
    if reader_breadcrumb_path_still_names(path, inspected) {
        let _ = std::fs::remove_file(path);
        remove_empty_breadcrumb_directories(path);
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
                        locked.push(LockedReaderBreadcrumb { path, breadcrumb });
                    }
                }
            }
            Err(_) => continue,
        }
    }
    Ok(locked)
}

/// How a wait for this store's direct readers to let go of it ended.
#[derive(Debug)]
pub enum ReaderReleaseWait {
    /// Nobody is holding the store any more. A writer may take it.
    Released,
    /// The caller asked to stop waiting, and is answered at once.
    Stopped,
    /// Who is holding the store cannot be observed at all, so this wait can
    /// say nothing about it. The caller decides what to do about that rather
    /// than being told a reassuring answer nobody checked.
    Unobservable(std::io::Error),
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
/// `_runtime` is the runtime directory a caller resolved for this store's
/// owner CHANNEL. It is accepted because consumers hold one, and it chooses
/// nothing here: the verdict comes from the readers themselves, and the notes
/// that let an unobservable wait name them are in the default per-user runtime
/// location, where a writer restarted with no flag at all looks.
///
/// `stop` ends the wait immediately: the caller is told the moment the token
/// is cancelled, through the token's own listener, so an interrupt is as
/// prompt as a release. A wait ended that way answers `Stopped` while the
/// kernel wait it started finishes on its own; that helper releases nothing
/// and holds nothing once it does.
/// The same wait, told the runtime ROOT an operator supplied.
///
/// The root is owner-CHANNEL addressing, and it is accepted here for
/// compatibility with consumers that hold one and pass it on every call. It
/// does not take part in this answer. Whether this store's readers have let go
/// of it is decided by the readers themselves -- each holds a shared advisory
/// hold on the companion beside the store for exactly as long as it is
/// hydrating -- so the verdict is the same whatever root is passed, and
/// `None` is the same wait as any other.
///
/// A root that was never created, or points at a file, or cannot be read, is
/// therefore never a verdict about readers. It is a channel misconfiguration
/// and it surfaces on its own, as the `owner_not_serving` answer an operator
/// gets when they try to reach that channel. Deciding `Unobservable` from it
/// here would tell a caller the readers cannot be watched when they are
/// perfectly watchable, and that caller would give up on a wait that would
/// have ended on its own.
///
/// Every other part of the contract is [`wait_for_reader_release`]'s -- the
/// kernel wake IS the event, there is no timer anywhere in it, and `stop` is
/// answered as promptly as a release.
pub fn wait_for_reader_release_in_runtime_dir(
    path: &Path,
    runtime_root: Option<&Path>,
    stop: &OwnerReadCancellation,
) -> ReaderReleaseWait {
    // Where the owner's CHANNEL lives and whether this store's readers have
    // let go of it are two unrelated facts that happen to arrive through one
    // argument, and only the second is being asked here.
    let _ = runtime_root;
    wait_for_reader_release(path, None, stop)
}

pub fn wait_for_reader_release(
    path: &Path,
    _runtime: Option<&RuntimeDirectory>,
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
        Err(error) => return unobservable_reader_hold(path, error),
    };
    // Armed BEFORE the first ask, so a reader that lets go in the gap between
    // the ask and the sleep has already left a notification queued rather than
    // slipping past unseen.
    let watch = match CoordinationWatch::arm(&companion, Some(stop)) {
        Ok(watch) => watch,
        Err(error) => return unobservable_reader_hold(path, error),
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
            Err(error) => return unobservable_reader_hold(path, error),
        }
        #[cfg(feature = "test-seams")]
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
        if let Err(error) = watch.sleep(&held, COMPANION_READER_HOLD_LOCK_OFFSET) {
            return unobservable_reader_hold(path, error);
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
fn unobservable_reader_hold(path: &Path, error: std::io::Error) -> ReaderReleaseWait {
    let (observed, verified) = live_reader_holders(path);
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
        "{error}; {observed} direct reader(s) are recorded as hydrating this store: {who}"
    )))
}

fn observed_reader_holders(
    database_path: &Path,
    runtime: &RuntimeDirectory,
) -> std::io::Result<(u64, Vec<ReaderBreadcrumb>)> {
    let locked = locked_reader_breadcrumbs(database_path, runtime.root())?;
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

/// The direct readers holding this store right now, as the production
/// breadcrumb reader sees them.
///
/// They are looked for in the DEFAULT per-user runtime location, always. The
/// readers of a store and the writers of it are not one deployment: a person
/// runs a reader against a store and passes the deployment's runtime root to
/// reach its owner channel, while a supervisor restarts the writer with
/// nothing but the store path. If the breadcrumb followed either side's flag,
/// the two would disagree exactly when the flag is what differs between them,
/// and the writer would report a store held by readers as held by nobody it
/// can name. One location, no flag to disagree on. A runtime location this
/// process cannot use observes nobody rather than guessing.
fn live_reader_holders(database_path: &Path) -> (u64, Vec<ReaderBreadcrumb>) {
    let Some(runtime) = reader_breadcrumb_runtime_directory() else {
        return (0, Vec::new());
    };
    observed_reader_holders(database_path, &runtime).unwrap_or((0, Vec::new()))
}

/// The ONE runtime location this process publishes reader breadcrumbs in and
/// looks for other processes' breadcrumbs in: the platform's default per-user
/// runtime location. The `--owner-read-runtime-dir` /
/// `CONTEXTDB_OWNER_READ_RUNTIME_DIR` override names where the owner CHANNEL is
/// bound and dialled, which is a different question with a different answer,
/// and it does not move this.
fn reader_breadcrumb_runtime_directory() -> Option<RuntimeDirectory> {
    default_reader_runtime_directory()
}

/// True when the operating system, not the store's contents, refused the
/// open. Every other Redb open failure keeps its existing classification.
fn open_was_refused_by_permissions(error: &redb::DatabaseError) -> bool {
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
fn open_found_no_store(error: &redb::DatabaseError) -> bool {
    matches!(
        error,
        redb::DatabaseError::Storage(redb::StorageError::Io(io))
            if io.kind() == std::io::ErrorKind::NotFound
    )
}

/// What an open says when its attempt finds nothing at the path.
///
/// The two answers are for two different callers. An opener that arrived here
/// because a store WAS there is describing a file that went away underneath a
/// door it was already committed to, and the existing taxonomy is what it has
/// always said. An opener that promised never to create a store is being asked
/// the question directly, and "there is no store here" is its answer -- typed,
/// naming the path, and never confused with a store that is there and cannot
/// be read.
#[derive(Clone, Copy, PartialEq, Eq)]
enum AbsentStoreAnswer {
    Taxonomy,
    Typed,
}

#[allow(dead_code)]
struct ReadImageSource {
    database: Option<redb::ReadOnlyDatabase>,
    /// This reader's own proof that it is holding the store, taken before the
    /// committed file is opened and let go only after it is closed. A
    /// would-be writer waits on THIS, so it exists whether or not the
    /// diagnostic breadcrumb beside it could be published.
    hold: Option<ReaderStoreHold>,
    breadcrumb: Option<ReaderBreadcrumbGuard>,
    source_accesses: u64,
    /// The store file's length, taken where the file is open, so the answer a
    /// reader gives about disk usage is the same measurement of the same file
    /// a writer gives.
    store_file_bytes: Option<u64>,
}

impl ReadImageSource {
    fn start(database_path: &Path) -> std::result::Result<Self, LoadReadImageError> {
        // The hold comes FIRST, before the committed file is opened at all, so
        // there is no instant in which this reader has the store and nothing
        // says so. It is the definitive fact; the breadcrumb below is the
        // best-effort note that lets a refusal name a process.
        let hold = ReaderStoreHold::take(database_path);
        let breadcrumb = current_reader_breadcrumb().ok();
        // The note goes in the DEFAULT per-user runtime location, whatever
        // runtime directory this reader was pointed at. That flag says where
        // the owner CHANNEL is; the writer that will be refused by this reader
        // was started by somebody else, very likely with no flag at all, and
        // it looks here. One location, no flag for the two sides to disagree
        // on. Publication is best effort, and identity discovery is not a
        // prerequisite: an unverifiable process simply remains anonymous.
        let breadcrumb_guard = reader_breadcrumb_runtime_directory().and_then(|runtime| {
            ReaderBreadcrumbGuard::create(database_path, runtime.root(), breadcrumb.as_ref()).ok()
        });
        let opened =
            RedbPersistence::open_hook_suppressed(|| redb_builder().open_read_only(database_path));
        let database = match opened {
            Ok(Ok(database)) => database,
            Ok(Err(error)) => return Err(read_database_open_error(error)),
            Err(_) => {
                return Err(read_image_corruption(
                    ReadImageCorruptionCause::MalformedRecord,
                    "read-only Redb open panicked while reading the store",
                ));
            }
        };
        #[cfg(feature = "test-seams")]
        crate::read_probe::note_persistence_read_only_open();
        #[cfg(feature = "test-seams")]
        read_persistence_test_scaffold::note_read_image_source_start_for_test(
            breadcrumb_guard.as_ref().map(|guard| guard.path()),
        );
        #[cfg(feature = "test-seams")]
        {
            let checkpoint_breadcrumb = breadcrumb_guard.as_ref().and(breadcrumb.as_ref());
            let checkpoint_path = checkpoint_breadcrumb
                .and_then(|_| breadcrumb_guard.as_ref().map(|guard| guard.path()));
            read_persistence_test_scaffold::read_image_hydration_checkpoint_for_test(
                checkpoint_path,
                checkpoint_breadcrumb,
            );
        }
        Ok(Self {
            database: Some(database),
            hold,
            breadcrumb: breadcrumb_guard,
            source_accesses: 0,
            // A stat of the file already open: it reads nothing, changes
            // nothing, and creates nothing, which is what a direct read is
            // allowed to do.
            store_file_bytes: std::fs::metadata(database_path)
                .map(|metadata| metadata.len())
                .ok(),
        })
    }

    fn with_read_transaction<T>(
        &mut self,
        read: impl FnOnce(&redb::ReadTransaction) -> std::result::Result<T, LoadReadImageError>,
    ) -> std::result::Result<T, LoadReadImageError> {
        let transaction = self
            .database
            .as_ref()
            .ok_or_else(|| invalid_read_image_state("read-only database is absent"))?
            .begin_read()
            .map_err(read_transaction_error)?;
        self.source_accesses = self.source_accesses.checked_add(1).ok_or_else(|| {
            invalid_read_image_state("read-only persistence source-access count overflowed")
        })?;
        #[cfg(feature = "test-seams")]
        crate::read_probe::note_persistence_source_access();
        #[cfg(feature = "test-seams")]
        read_persistence_test_scaffold::note_read_image_source_access_for_test();
        let result = read(&transaction);
        drop(transaction);
        result
    }

    fn finish(
        mut self,
        image: ReadPersistenceImage,
    ) -> std::result::Result<ReadPersistenceLoad, LoadReadImageError> {
        let image = ReadPersistenceImage {
            store_file_bytes: self.store_file_bytes,
            ..image
        };
        if self.source_accesses == 0 {
            return Err(LoadReadImageError::Release(
                "no persistence source access was observed".to_owned(),
            ));
        }
        drop(self.database.take());
        // The committed file is closed, so this reader is no longer holding
        // the store and a caller waiting to take it is woken here.
        drop(self.hold.take());
        #[cfg(feature = "test-seams")]
        read_persistence_test_scaffold::note_read_image_source_handles_dropped_for_test();
        let breadcrumb_path = match self.breadcrumb.take() {
            Some(breadcrumb) => Some(
                breadcrumb
                    .release()
                    .map_err(|error| LoadReadImageError::Release(error.to_string()))?,
            ),
            None => None,
        };
        #[cfg(feature = "test-seams")]
        crate::read_probe::note_persistence_release_receipt();
        #[cfg(feature = "test-seams")]
        read_persistence_test_scaffold::note_read_image_source_release_for_test(
            breadcrumb_path.as_deref(),
        );
        Ok(ReadPersistenceLoad {
            image,
            release_receipt: ReadPersistenceReleaseReceipt {
                breadcrumb_path,
                source_accesses: self.source_accesses,
                _seal: ReadPersistenceReleaseSeal,
            },
        })
    }
}

fn read_image_corruption(
    cause: ReadImageCorruptionCause,
    reason: impl std::fmt::Display,
) -> LoadReadImageError {
    let reason = reason.to_string();
    LoadReadImageError::Corrupt { cause, reason }
}

fn read_image_error(error: impl std::fmt::Display) -> LoadReadImageError {
    read_image_corruption(ReadImageCorruptionCause::MalformedRecord, error)
}

fn read_storage_error(error: redb::StorageError) -> LoadReadImageError {
    match error {
        redb::StorageError::Io(error) => read_image_io("reading Redb storage", error),
        redb::StorageError::PreviousIo => read_image_io(
            "reading Redb storage",
            "a prior storage I/O operation failed",
        ),
        redb::StorageError::Corrupted(reason) => {
            read_image_corruption(ReadImageCorruptionCause::MalformedRecord, reason)
        }
        redb::StorageError::DatabaseClosed => {
            invalid_read_image_state("read-only Redb database closed during hydration")
        }
        other => read_image_corruption(ReadImageCorruptionCause::MalformedRecord, other),
    }
}

fn read_table_error(error: redb::TableError) -> LoadReadImageError {
    match error {
        redb::TableError::Storage(error) => read_storage_error(error),
        other => read_image_corruption(ReadImageCorruptionCause::MalformedRecord, other),
    }
}

fn read_transaction_error(error: redb::TransactionError) -> LoadReadImageError {
    match error {
        redb::TransactionError::Storage(error) => read_storage_error(error),
        other => invalid_read_image_state(other),
    }
}

fn direct_read_requires_writer(reason: impl std::fmt::Display) -> LoadReadImageError {
    direct_read_requires_writer_for(
        ReadImageRequiresWriterCause::WritableSanitizerWouldChange,
        reason,
    )
}

fn direct_read_requires_writer_for(
    cause: ReadImageRequiresWriterCause,
    reason: impl std::fmt::Display,
) -> LoadReadImageError {
    let reason = reason.to_string();
    LoadReadImageError::RequiresWriter { cause, reason }
}

fn legacy_read_layout(reason: impl std::fmt::Display) -> LoadReadImageError {
    read_image_corruption(ReadImageCorruptionCause::LegacyLayout, reason)
}

fn invalid_read_image_state(reason: impl std::fmt::Display) -> LoadReadImageError {
    let reason = reason.to_string();
    LoadReadImageError::InvalidState { reason }
}

fn read_image_contention(reason: impl std::fmt::Display) -> LoadReadImageError {
    let reason = reason.to_string();
    LoadReadImageError::Contention { reason }
}

fn read_image_io(operation: &'static str, reason: impl std::fmt::Display) -> LoadReadImageError {
    let reason = reason.to_string();
    LoadReadImageError::Io { operation, reason }
}

fn read_database_open_error(error: redb::DatabaseError) -> LoadReadImageError {
    match error {
        redb::DatabaseError::DatabaseAlreadyOpen => {
            read_image_contention("the store has an exclusive writer")
        }
        redb::DatabaseError::RepairAborted => direct_read_requires_writer(
            "Redb requires writable recovery before this committed image can be read",
        ),
        redb::DatabaseError::UpgradeRequired(version) => legacy_read_layout(format!(
            "Redb file format version {version} requires an upgrade"
        )),
        redb::DatabaseError::TransactionInProgress => invalid_read_image_state(
            "Redb reported a transaction in progress during read-only open",
        ),
        // What the caller must do next depends entirely on which of these it
        // is: a store that is not there is a dead end, a store somebody else
        // holds means asking that holder instead.
        redb::DatabaseError::Storage(redb::StorageError::Io(error)) => match error.kind() {
            std::io::ErrorKind::NotFound => LoadReadImageError::StoreMissing {
                reason: error.to_string(),
            },
            std::io::ErrorKind::WouldBlock => read_image_contention(error),
            // The file is there and the machine handed its bytes over
            // perfectly well; the bytes are simply not a store. That is the
            // same finding a writable open makes about the same file, so it
            // is reported as what it is -- a store that cannot be read --
            // rather than as an unbuilt capability, which is what an operator
            // used to be told and could do nothing with.
            std::io::ErrorKind::InvalidData => {
                read_image_corruption(ReadImageCorruptionCause::MalformedRecord, error)
            }
            _ => read_image_io("opening the read-only Redb database", error),
        },
        redb::DatabaseError::Storage(redb::StorageError::Corrupted(reason)) => {
            read_image_corruption(ReadImageCorruptionCause::MalformedRecord, reason)
        }
        other => read_image_corruption(ReadImageCorruptionCause::MalformedRecord, other),
    }
}

fn decode_read_table_meta(
    read_txn: &redb::ReadTransaction,
) -> std::result::Result<HashMap<String, TableMeta>, LoadReadImageError> {
    let meta_table = match read_txn.open_table(META_TABLE) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(HashMap::new()),
        Err(error) => return Err(read_table_error(error)),
    };
    let mut table_meta = HashMap::new();
    for entry in meta_table.iter().map_err(read_storage_error)? {
        let (key, value) = entry.map_err(read_storage_error)?;
        let Some(name) = key.value().strip_prefix("table:") else {
            continue;
        };
        let (meta, legacy) = RedbPersistence::decode_table_meta_versioned(value.value())
            .map_err(read_image_error)?;
        if legacy {
            return Err(legacy_read_layout(format!(
                "table {name} uses a legacy metadata layout"
            )));
        }
        table_meta.insert(name.to_owned(), meta);
    }
    Ok(table_meta)
}

fn decode_read_relational_table(
    read_txn: &redb::ReadTransaction,
    name: &str,
    meta: &TableMeta,
) -> std::result::Result<Vec<VersionedRow>, LoadReadImageError> {
    let table_name = RedbPersistence::rel_table_name(name);
    let definition: TableDefinition<&[u8], &[u8]> = TableDefinition::new(table_name.as_str());
    let table = match read_txn.open_table(definition) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
        Err(redb::TableError::TableTypeMismatch { .. }) => {
            return Err(legacy_read_layout(format!(
                "table {name} uses a legacy relational layout"
            )));
        }
        Err(error) => return Err(read_table_error(error)),
    };
    let mut rows = Vec::new();
    for entry in table.iter().map_err(read_storage_error)? {
        let (_, value) = entry.map_err(read_storage_error)?;
        // Loading a store is most of what opening one costs, and a caller
        // waiting on a large store is told how far it has got. What arrives is
        // counted where it arrives: the stored bytes of one row.
        crate::read_progress::note_hydrated_bytes(value.value().len() as u64);
        let row = RedbPersistence::decode_versioned_row(value.value(), Some(meta))
            .map_err(read_image_error)?;
        validate_read_row(name, &row, meta)?;
        rows.push(row);
    }
    Ok(rows)
}

fn validate_read_row(
    table: &str,
    row: &VersionedRow,
    meta: &TableMeta,
) -> std::result::Result<(), LoadReadImageError> {
    if i64::try_from(row.row_id.0).is_err() {
        return Err(invalid_read_image_state(format!(
            "row {} in table {table} is outside the relational identifier range",
            row.row_id.0
        )));
    }
    for (column, value) in &row.values {
        let Some(definition) = meta
            .columns
            .iter()
            .find(|definition| definition.name == *column)
        else {
            return Err(direct_read_requires_writer(format!(
                "row {} in table {table} carries unknown column {column}",
                row.row_id.0
            )));
        };
        if let ColumnType::Vector(dimension) = definition.column_type {
            // An absent embedding is a value, not damage -- but only where the
            // declaration allows one. A nullable vector column holds NULL
            // wherever a row was written before its embedding existed, and on a
            // quantized column the empty slot is the deliberate placeholder for
            // the independently durable vector entry the row is hydrated from.
            // A NULL anywhere else sits in a cell the schema says must hold an
            // embedding, so `NOT NULL` keeps its meaning through a direct read.
            let absent_embedding_is_legal =
                definition.nullable || !matches!(definition.quantization, VectorQuantization::F32);
            match value {
                Value::Vector(vector) if vector.len() == dimension => {}
                Value::Null if absent_embedding_is_legal => {}
                _ => {
                    return Err(direct_read_requires_writer(format!(
                        "row {} in table {table} carries an invalid vector value for {column}",
                        row.row_id.0
                    )));
                }
            }
        }
    }
    Ok(())
}

fn remove_quantized_placeholders(row: &mut VersionedRow, meta: &TableMeta) {
    for column in &meta.columns {
        if matches!(column.column_type, ColumnType::Vector(_))
            && !matches!(column.quantization, VectorQuantization::F32)
            && matches!(row.values.get(&column.name), Some(Value::Null))
        {
            row.values.remove(&column.name);
        }
    }
}

fn decode_read_graph_table(
    read_txn: &redb::ReadTransaction,
    definition: TableDefinition<&[u8], &[u8]>,
) -> std::result::Result<Vec<AdjEntry>, LoadReadImageError> {
    let table = match read_txn.open_table(definition) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
        Err(error) => return Err(read_table_error(error)),
    };
    let mut entries = Vec::new();
    for entry in table.iter().map_err(read_storage_error)? {
        let (_, value) = entry.map_err(read_storage_error)?;
        crate::read_progress::note_hydrated_bytes(value.value().len() as u64);
        entries.push(RedbPersistence::decode(value.value()).map_err(read_image_error)?);
    }
    Ok(entries)
}

fn decode_read_vectors(
    read_txn: &redb::ReadTransaction,
) -> std::result::Result<Vec<VectorEntry>, LoadReadImageError> {
    let table = match read_txn.open_table(VECTORS_TABLE) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
        Err(error) => return Err(read_table_error(error)),
    };
    let mut vectors = Vec::new();
    for entry in table.iter().map_err(read_storage_error)? {
        let (_, value) = entry.map_err(read_storage_error)?;
        crate::read_progress::note_hydrated_bytes(value.value().len() as u64);
        vectors
            .push(RedbPersistence::decode_vector_entry(value.value()).map_err(read_image_error)?);
    }
    Ok(vectors)
}

fn validate_and_collect_current_vectors(
    table_meta: &HashMap<String, TableMeta>,
    relational_tables: &mut HashMap<String, Vec<VersionedRow>>,
    vector_entries: &[VectorEntry],
    ddl_log: &[(Lsn, DdlChange)],
) -> std::result::Result<HashMap<(VectorIndexRef, RowId), Vec<f32>>, LoadReadImageError> {
    let vector_specs = read_vector_specs_from_meta(table_meta);
    let renames = read_vector_renames_from_ddl_log(ddl_log);
    let mut current = HashMap::new();
    let mut current_order = HashMap::<(VectorIndexRef, RowId), (Lsn, TxId)>::new();
    for entry in vector_entries {
        if i64::try_from(entry.row_id.0).is_err() {
            return Err(invalid_read_image_state(format!(
                "vector {}.{} has out-of-range row id {}",
                entry.index.table, entry.index.column, entry.row_id.0
            )));
        }
        let Some(resolved_index) =
            resolve_read_vector_index(&entry.index, entry.lsn, &vector_specs, &renames)
        else {
            return Err(direct_read_requires_writer_for(
                ReadImageRequiresWriterCause::InvalidVectorReference,
                format!(
                    "vector index {}.{} cannot be reconciled with current metadata",
                    entry.index.table, entry.index.column
                ),
            ));
        };
        if resolved_index != entry.index {
            return Err(direct_read_requires_writer_for(
                ReadImageRequiresWriterCause::InvalidVectorReference,
                format!(
                    "vector index {}.{} requires writable rename reconciliation to {}.{}",
                    entry.index.table,
                    entry.index.column,
                    resolved_index.table,
                    resolved_index.column
                ),
            ));
        }
        let Some(&dimension) = vector_specs.get(&entry.index) else {
            return Err(direct_read_requires_writer_for(
                ReadImageRequiresWriterCause::InvalidVectorReference,
                format!(
                    "vector index {}.{} has no current metadata",
                    entry.index.table, entry.index.column
                ),
            ));
        };
        if entry.vector.len() != dimension {
            return Err(direct_read_requires_writer_for(
                ReadImageRequiresWriterCause::InvalidVectorReference,
                format!(
                    "vector {}.{} row {} has dimension {} instead of {dimension}",
                    entry.index.table,
                    entry.index.column,
                    entry.row_id.0,
                    entry.vector.len()
                ),
            ));
        }
        let rows = relational_tables.get(&entry.index.table).ok_or_else(|| {
            direct_read_requires_writer_for(
                ReadImageRequiresWriterCause::InvalidVectorReference,
                format!(
                    "vector index references unavailable table {}",
                    entry.index.table
                ),
            )
        })?;
        let matching_row = rows.iter().find(|row| {
            row.row_id == entry.row_id && row.created_tx == entry.created_tx && row.lsn == entry.lsn
        });
        if !rows.iter().any(|row| row.row_id == entry.row_id) {
            return Err(direct_read_requires_writer_for(
                ReadImageRequiresWriterCause::InvalidVectorReference,
                format!(
                    "vector {}.{} row {} has no relational owner",
                    entry.index.table, entry.index.column, entry.row_id.0
                ),
            ));
        }
        let column = table_meta
            .get(&entry.index.table)
            .ok_or_else(|| {
                direct_read_requires_writer_for(
                    ReadImageRequiresWriterCause::InvalidVectorReference,
                    format!(
                        "vector index references unavailable table {}",
                        entry.index.table
                    ),
                )
            })?
            .columns
            .iter()
            .find(|column| column.name == entry.index.column)
            .ok_or_else(|| {
                direct_read_requires_writer_for(
                    ReadImageRequiresWriterCause::InvalidVectorReference,
                    format!(
                        "vector index references unavailable column {}.{}",
                        entry.index.table, entry.index.column
                    ),
                )
            })?;
        // Compaction can legitimately retire the exact relational version
        // that first owned a still-live vector. Compare exact occurrences,
        // or a live body-only successor for a live vector; a historical
        // deleted vector without its compacted row version has no row value
        // left to compare.
        let comparison_row = matching_row.or_else(|| {
            if entry.deleted_tx.is_none() {
                rows.iter()
                    .filter(|row| row.row_id == entry.row_id && row.deleted_tx.is_none())
                    .max_by_key(|row| (row.lsn, row.created_tx))
            } else {
                None
            }
        });
        if let Some(row) = comparison_row {
            let row_value = row.values.get(&entry.index.column);
            let row_matches_vector = match column.quantization {
                VectorQuantization::F32 => {
                    matches!(row_value, Some(Value::Vector(row_vector)) if row_vector == &entry.vector)
                }
                // A quantized column keeps no copy in the row: the store's
                // entry -- the one being checked here -- IS the value. So an
                // empty slot and an absent slot are both what a healthy row
                // looks like, and only a row carrying a DIFFERENT vector has
                // diverged from the store.
                VectorQuantization::SQ4 | VectorQuantization::SQ8 => {
                    !matches!(row_value, Some(Value::Vector(row_vector)) if row_vector != &entry.vector)
                }
            };
            if !row_matches_vector {
                return Err(direct_read_requires_writer_for(
                    ReadImageRequiresWriterCause::RowVectorDivergence,
                    format!(
                        "row and vector state diverge for {}.{} row {}",
                        entry.index.table, entry.index.column, entry.row_id.0
                    ),
                ));
            }
        }
        if entry.deleted_tx.is_some()
            || !rows
                .iter()
                .any(|row| row.row_id == entry.row_id && row.deleted_tx.is_none())
        {
            continue;
        }
        let key = (entry.index.clone(), entry.row_id);
        let order = (entry.lsn, entry.created_tx);
        if current_order
            .get(&key)
            .is_none_or(|existing| order > *existing)
        {
            current_order.insert(key.clone(), order);
            current.insert(key, entry.vector.clone());
        }
    }

    ensure_no_vector_supplement_needed(table_meta, relational_tables, vector_entries)?;
    ensure_required_quantized_vectors_are_present(table_meta, relational_tables, &current)?;

    // A quantized column's value is NOT reconstructed into the row. The row
    // keeps an empty slot, here as on the writer's own reopen, and a reader
    // that names the column takes the value from the vector entries this
    // image carries. Writing it in would give the image a full-precision copy
    // per row that no read asked for. The loop still runs, because it is also
    // what proves every entry names a column this image really has.
    for entry in vector_entries {
        let column = table_meta
            .get(&entry.index.table)
            .and_then(|meta| {
                meta.columns
                    .iter()
                    .find(|column| column.name == entry.index.column)
            })
            .ok_or_else(|| {
                direct_read_requires_writer_for(
                    ReadImageRequiresWriterCause::InvalidVectorReference,
                    format!(
                        "vector index references unavailable column {}.{}",
                        entry.index.table, entry.index.column
                    ),
                )
            })?;
        if matches!(column.quantization, VectorQuantization::F32) {
            continue;
        }
        let rows = relational_tables
            .get_mut(&entry.index.table)
            .ok_or_else(|| {
                direct_read_requires_writer_for(
                    ReadImageRequiresWriterCause::InvalidVectorReference,
                    format!(
                        "vector index references unavailable table {}",
                        entry.index.table
                    ),
                )
            })?;
        let _ = rows;
    }
    for (table, meta) in table_meta {
        if let Some(rows) = relational_tables.get_mut(table) {
            for row in rows {
                remove_quantized_placeholders(row, meta);
            }
        }
    }
    Ok(current)
}

#[derive(Clone)]
struct ReadVectorRename {
    lsn: Lsn,
    from: VectorIndexRef,
    to: VectorIndexRef,
}

fn read_vector_specs_from_meta(
    table_meta: &HashMap<String, TableMeta>,
) -> HashMap<VectorIndexRef, usize> {
    let mut specs = HashMap::new();
    for (table, meta) in table_meta {
        for column in &meta.columns {
            if let ColumnType::Vector(dimension) = column.column_type {
                specs.insert(
                    VectorIndexRef::new(table.clone(), column.name.clone()),
                    dimension,
                );
            }
        }
    }
    specs
}

fn read_vector_rename_constraint(constraints: &[String]) -> Option<(String, String)> {
    const PREFIX: &str = "VECTOR_RENAME(";
    for constraint in constraints {
        let trimmed = constraint.trim();
        if !trimmed.to_ascii_uppercase().starts_with(PREFIX) || !trimmed.ends_with(')') {
            continue;
        }
        let body = &trimmed[PREFIX.len()..trimmed.len().saturating_sub(1)];
        let (from, to) = body.split_once(',')?;
        let from = from
            .trim()
            .trim_matches(|ch| ch == '"' || ch == '\'' || ch == '`')
            .to_owned();
        let to = to
            .trim()
            .trim_matches(|ch| ch == '"' || ch == '\'' || ch == '`')
            .to_owned();
        if !from.is_empty() && !to.is_empty() {
            return Some((from, to));
        }
    }
    None
}

fn read_vector_renames_from_ddl_log(ddl_log: &[(Lsn, DdlChange)]) -> Vec<ReadVectorRename> {
    let mut renames = Vec::new();
    for (lsn, change) in ddl_log {
        let DdlChange::AlterTable {
            name, constraints, ..
        } = change
        else {
            continue;
        };
        if let Some((from, to)) = read_vector_rename_constraint(constraints) {
            renames.push(ReadVectorRename {
                lsn: *lsn,
                from: VectorIndexRef::new(name, from),
                to: VectorIndexRef::new(name, to),
            });
        }
    }
    renames.sort_by(|left, right| {
        left.lsn
            .cmp(&right.lsn)
            .then(left.from.table.cmp(&right.from.table))
            .then(left.from.column.cmp(&right.from.column))
            .then(left.to.table.cmp(&right.to.table))
            .then(left.to.column.cmp(&right.to.column))
    });
    renames
}

fn resolve_read_vector_index(
    index: &VectorIndexRef,
    entry_lsn: Lsn,
    vector_specs: &HashMap<VectorIndexRef, usize>,
    renames: &[ReadVectorRename],
) -> Option<VectorIndexRef> {
    let mut current = index.clone();
    let mut seen = HashSet::new();
    for _ in 0..=renames.len() {
        if !seen.insert(current.clone()) {
            return None;
        }
        if let Some(rename) = renames
            .iter()
            .filter(|rename| rename.from == current && entry_lsn <= rename.lsn)
            .min_by_key(|rename| rename.lsn)
        {
            current = rename.to.clone();
            continue;
        }
        return vector_specs.contains_key(&current).then_some(current);
    }
    None
}

fn ensure_no_vector_supplement_needed(
    table_meta: &HashMap<String, TableMeta>,
    relational_tables: &HashMap<String, Vec<VersionedRow>>,
    vector_entries: &[VectorEntry],
) -> std::result::Result<(), LoadReadImageError> {
    let seen_occurrences = vector_entries
        .iter()
        .map(|entry| {
            (
                entry.index.clone(),
                entry.row_id,
                entry.created_tx,
                entry.lsn,
            )
        })
        .collect::<HashSet<_>>();
    let known_owners = vector_entries
        .iter()
        .map(|entry| (entry.index.clone(), entry.row_id))
        .collect::<HashSet<_>>();
    for (table, meta) in table_meta {
        let Some(rows) = relational_tables.get(table) else {
            continue;
        };
        for column in &meta.columns {
            let ColumnType::Vector(dimension) = column.column_type else {
                continue;
            };
            let index = VectorIndexRef::new(table.clone(), column.name.clone());
            for row in rows {
                let Some(Value::Vector(vector)) = row.values.get(&column.name) else {
                    continue;
                };
                if vector.len() != dimension {
                    continue;
                }
                if row.deleted_tx.is_none() && known_owners.contains(&(index.clone(), row.row_id)) {
                    continue;
                }
                if !seen_occurrences.contains(&(index.clone(), row.row_id, row.created_tx, row.lsn))
                {
                    return Err(direct_read_requires_writer(format!(
                        "row {} in {}.{} needs writable vector supplementation",
                        row.row_id.0, table, column.name
                    )));
                }
            }
        }
    }
    Ok(())
}

/// A quantized vector column keeps its embedding outside the relational row:
/// the cell holds an empty placeholder and the reader serves the value from
/// the separately durable vector entry the row owns. Where such a column is
/// declared `NOT NULL`, the schema promises every live row an embedding, so
/// the placeholder is only legal while that entry is still there. Proving it
/// here is what keeps `NOT NULL` meaning the same thing on a quantized column
/// as on a full-precision one: a file that has lost the entry can no longer
/// keep the promise it records, and an operator reading it directly is told a
/// writer is the next step instead of being handed a row whose required column
/// has quietly vanished.
fn ensure_required_quantized_vectors_are_present(
    table_meta: &HashMap<String, TableMeta>,
    relational_tables: &HashMap<String, Vec<VersionedRow>>,
    current_vectors: &HashMap<(VectorIndexRef, RowId), Vec<f32>>,
) -> std::result::Result<(), LoadReadImageError> {
    for (table, meta) in table_meta {
        let Some(rows) = relational_tables.get(table) else {
            continue;
        };
        for column in &meta.columns {
            if !matches!(column.column_type, ColumnType::Vector(_))
                || column.nullable
                || matches!(column.quantization, VectorQuantization::F32)
            {
                continue;
            }
            let index = VectorIndexRef::new(table.clone(), column.name.clone());
            for row in rows {
                // A deleted row version is history; only what a read would
                // serve has to keep the declaration.
                if row.deleted_tx.is_some() {
                    continue;
                }
                if !matches!(row.values.get(&column.name), Some(Value::Null)) {
                    continue;
                }
                if current_vectors.contains_key(&(index.clone(), row.row_id)) {
                    continue;
                }
                return Err(direct_read_requires_writer(format!(
                    "row {} in {}.{} has no stored embedding for a required column",
                    row.row_id.0, table, column.name
                )));
            }
        }
    }
    Ok(())
}

fn decode_read_sync_source_lsns(
    read_txn: &redb::ReadTransaction,
) -> std::result::Result<HashMap<(String, RowId), Lsn>, LoadReadImageError> {
    let table = match read_txn.open_table(SYNC_ROW_SOURCE_LSN_TABLE) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(HashMap::new()),
        Err(error) => return Err(read_table_error(error)),
    };
    let mut values = HashMap::new();
    for entry in table.iter().map_err(read_storage_error)? {
        let (key, value) = entry.map_err(read_storage_error)?;
        let Some(key) = RedbPersistence::decode_sync_row_source_lsn_key(key.value()) else {
            return Err(invalid_read_image_state(
                "sync provenance has an invalid row key",
            ));
        };
        values.insert(key, Lsn(value.value()));
    }
    Ok(values)
}

fn decode_read_sync_source_kinds(
    read_txn: &redb::ReadTransaction,
) -> std::result::Result<HashMap<(String, RowId), u8>, LoadReadImageError> {
    let table = match read_txn.open_table(SYNC_ROW_SOURCE_KIND_TABLE) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(HashMap::new()),
        Err(error) => return Err(read_table_error(error)),
    };
    let mut values = HashMap::new();
    for entry in table.iter().map_err(read_storage_error)? {
        let (key, value) = entry.map_err(read_storage_error)?;
        let Some(key) = RedbPersistence::decode_sync_row_source_lsn_key(key.value()) else {
            return Err(invalid_read_image_state(
                "sync provenance kind has an invalid row key",
            ));
        };
        values.insert(key, value.value());
    }
    Ok(values)
}

fn decode_read_change_log(
    read_txn: &redb::ReadTransaction,
) -> std::result::Result<Vec<ChangeLogEntry>, LoadReadImageError> {
    let table = match read_txn.open_table(CHANGE_LOG_TABLE) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
        Err(error) => return Err(read_table_error(error)),
    };
    let mut entries = Vec::new();
    for entry in table.iter().map_err(read_storage_error)? {
        let (_, value) = entry.map_err(read_storage_error)?;
        crate::read_progress::note_hydrated_bytes(value.value().len() as u64);
        entries.push(RedbPersistence::decode(value.value()).map_err(read_image_error)?);
    }
    Ok(entries)
}

fn decode_read_ddl_log(
    read_txn: &redb::ReadTransaction,
) -> std::result::Result<Vec<(Lsn, DdlChange)>, LoadReadImageError> {
    let table = match read_txn.open_table(DDL_LOG_TABLE) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
        Err(error) => return Err(read_table_error(error)),
    };
    let mut entries = Vec::new();
    for entry in table.iter().map_err(read_storage_error)? {
        let (key, value) = entry.map_err(read_storage_error)?;
        let raw_lsn = key
            .value()
            .split_once(':')
            .map(|(lsn, _)| lsn)
            .unwrap_or(key.value());
        let lsn = raw_lsn.parse::<u64>().map(Lsn).map_err(read_image_error)?;
        entries.push((
            lsn,
            RedbPersistence::decode(value.value()).map_err(read_image_error)?,
        ));
    }
    Ok(entries)
}

fn decode_read_commit_index(
    read_txn: &redb::ReadTransaction,
) -> std::result::Result<BTreeMap<Lsn, TxId>, LoadReadImageError> {
    let table = match read_txn.open_table(COMMIT_INDEX_TABLE) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(BTreeMap::new()),
        Err(error) => return Err(read_table_error(error)),
    };
    let mut values = BTreeMap::new();
    let mut previous = TxId(0);
    for entry in table.iter().map_err(read_storage_error)? {
        let (lsn, tx) = entry.map_err(read_storage_error)?;
        let tx = TxId(tx.value());
        if tx < previous {
            return Err(direct_read_requires_writer_for(
                ReadImageRequiresWriterCause::NonMonotonicCommitIndex,
                "commit-index transaction order needs writable repair",
            ));
        }
        previous = tx;
        values.insert(Lsn(lsn.value()), tx);
    }
    Ok(values)
}

fn decode_read_raw_string_table(
    read_txn: &redb::ReadTransaction,
    definition: TableDefinition<&str, &[u8]>,
) -> std::result::Result<Vec<(String, Vec<u8>)>, LoadReadImageError> {
    let table = match read_txn.open_table(definition) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
        Err(error) => return Err(read_table_error(error)),
    };
    let mut entries = Vec::new();
    for entry in table.iter().map_err(read_storage_error)? {
        let (key, value) = entry.map_err(read_storage_error)?;
        entries.push((key.value().to_owned(), value.value().to_vec()));
    }
    Ok(entries)
}

fn decode_read_trigger_audit_stamps(
    read_txn: &redb::ReadTransaction,
) -> std::result::Result<Vec<(String, u64)>, LoadReadImageError> {
    let table = match read_txn.open_table(TRIGGER_AUDIT_STAMPS_TABLE) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
        Err(error) => return Err(read_table_error(error)),
    };
    let mut entries = Vec::new();
    for entry in table.iter().map_err(read_storage_error)? {
        let (key, value) = entry.map_err(read_storage_error)?;
        entries.push((key.value().to_owned(), value.value()));
    }
    Ok(entries)
}

#[allow(clippy::type_complexity)]
fn decode_read_sink_queues(
    read_txn: &redb::ReadTransaction,
) -> std::result::Result<BTreeMap<String, Vec<(u64, Vec<u8>)>>, LoadReadImageError> {
    let mut names = read_txn
        .list_tables()
        .map_err(read_storage_error)?
        .filter_map(|handle| {
            handle
                .name()
                .strip_prefix("__sink_queue_")
                .map(str::to_owned)
        })
        .collect::<Vec<_>>();
    names.sort();
    let mut queues = BTreeMap::new();
    for name in names {
        let table_name = RedbPersistence::sink_queue_table_name(&name);
        let definition: TableDefinition<u64, &[u8]> = TableDefinition::new(table_name.as_str());
        let table = read_txn.open_table(definition).map_err(read_table_error)?;
        let mut entries = Vec::new();
        for entry in table.iter().map_err(read_storage_error)? {
            let (key, value) = entry.map_err(read_storage_error)?;
            entries.push((key.value(), value.value().to_vec()));
        }
        queues.insert(name, entries);
    }
    Ok(queues)
}

fn validate_reverse_graph_projection(
    forward_edges: &[AdjEntry],
    reverse_edges: &[AdjEntry],
) -> std::result::Result<(), LoadReadImageError> {
    /// What an edge IS, independent of the order anything happened to store
    /// it in.
    ///
    /// The two projections hold the same edges, so the question here is
    /// whether they hold the same CONTENT. An edge's properties are a map,
    /// and the durable encoder writes a map in iteration order, so comparing
    /// encoded bytes asks a second question nobody meant to ask -- whether
    /// two separately decoded copies happened to walk their keys the same
    /// way. Rendering the properties through an ordered map fixes one order
    /// for equal content, so equal edges render equal -- and it stays on the
    /// durable encoder, which can carry every value an edge may hold.
    fn content(edge: &AdjEntry) -> std::result::Result<Vec<u8>, LoadReadImageError> {
        let properties = edge
            .properties
            .iter()
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect::<BTreeMap<_, _>>();
        RedbPersistence::encode(&(
            edge.source,
            edge.target,
            edge.edge_type.as_str(),
            properties,
            edge.created_tx,
            edge.deleted_tx,
            edge.lsn,
        ))
        .map_err(read_image_error)
    }

    fn multiset(
        edges: &[AdjEntry],
    ) -> std::result::Result<BTreeMap<Vec<u8>, u64>, LoadReadImageError> {
        let mut values = BTreeMap::new();
        for edge in edges {
            *values.entry(content(edge)?).or_insert(0) += 1;
        }
        Ok(values)
    }

    if multiset(forward_edges)? != multiset(reverse_edges)? {
        return Err(direct_read_requires_writer(
            "forward and reverse graph projections need writable reconciliation",
        ));
    }
    Ok(())
}

fn resolve_read_tombstone(versions: &mut [(Lsn, TxId)], threshold: Lsn) -> Option<TxId> {
    versions.sort_by_key(|(lsn, _)| *lsn);
    let boundary = versions.partition_point(|(lsn, _)| *lsn < threshold);
    (boundary != 0).then(|| versions[boundary - 1].1)
}

fn reconstruct_read_commit_index(
    relational_tables: &HashMap<String, Vec<VersionedRow>>,
    forward_edges: &[AdjEntry],
    vector_entries: &[VectorEntry],
    change_log: &[ChangeLogEntry],
) -> BTreeMap<Lsn, TxId> {
    let mut index = BTreeMap::new();
    let mut add_entry = |lsn: Lsn, tx: TxId| {
        if lsn != Lsn(0) {
            index
                .entry(lsn)
                .and_modify(|current: &mut TxId| *current = (*current).max(tx))
                .or_insert(tx);
        }
    };
    let mut row_tombstones = HashMap::<(String, RowId), Vec<(Lsn, TxId)>>::new();
    for (table, rows) in relational_tables {
        for row in rows {
            add_entry(row.lsn, row.created_tx);
            if let Some(deleted_tx) = row.deleted_tx {
                row_tombstones
                    .entry((table.clone(), row.row_id))
                    .or_default()
                    .push((row.lsn, deleted_tx));
            }
        }
    }
    let mut edge_tombstones = HashMap::<(NodeId, String, NodeId), Vec<(Lsn, TxId)>>::new();
    for edge in forward_edges {
        add_entry(edge.lsn, edge.created_tx);
        if let Some(deleted_tx) = edge.deleted_tx {
            edge_tombstones
                .entry((edge.source, edge.edge_type.clone(), edge.target))
                .or_default()
                .push((edge.lsn, deleted_tx));
        }
    }
    let mut vector_tombstones = HashMap::<(VectorIndexRef, RowId), Vec<(Lsn, TxId)>>::new();
    for entry in vector_entries {
        add_entry(entry.lsn, entry.created_tx);
        if let Some(deleted_tx) = entry.deleted_tx {
            vector_tombstones
                .entry((entry.index.clone(), entry.row_id))
                .or_default()
                .push((entry.lsn, deleted_tx));
        }
    }
    for entry in change_log {
        match entry {
            ChangeLogEntry::RowInsert { .. }
            | ChangeLogEntry::EdgeInsert { .. }
            | ChangeLogEntry::VectorInsert { .. } => {}
            ChangeLogEntry::RowDelete {
                table, row_id, lsn, ..
            } => {
                if let Some(versions) = row_tombstones.get_mut(&(table.clone(), *row_id))
                    && let Some(tx) = resolve_read_tombstone(versions, *lsn)
                {
                    add_entry(*lsn, tx);
                }
            }
            ChangeLogEntry::EdgeDelete {
                source,
                target,
                edge_type,
                lsn,
            } => {
                if let Some(versions) =
                    edge_tombstones.get_mut(&(*source, edge_type.clone(), *target))
                    && let Some(tx) = resolve_read_tombstone(versions, *lsn)
                {
                    add_entry(*lsn, tx);
                }
            }
            ChangeLogEntry::VectorDelete { index, row_id, lsn } => {
                if let Some(versions) = vector_tombstones.get_mut(&(index.clone(), *row_id))
                    && let Some(tx) = resolve_read_tombstone(versions, *lsn)
                {
                    add_entry(*lsn, tx);
                }
            }
        }
    }
    index
}

fn validate_read_commit_index(
    persisted: &BTreeMap<Lsn, TxId>,
    reconstructed: &BTreeMap<Lsn, TxId>,
) -> std::result::Result<(), LoadReadImageError> {
    for (lsn, tx) in reconstructed {
        match persisted.get(lsn) {
            Some(persisted_tx) if persisted_tx == tx => {}
            Some(persisted_tx) => {
                return Err(invalid_read_image_state(format!(
                    "commit index maps LSN {} to transaction {} instead of {}",
                    lsn.0, persisted_tx.0, tx.0
                )));
            }
            None => {
                return Err(direct_read_requires_writer_for(
                    ReadImageRequiresWriterCause::MissingCommitIndex,
                    format!(
                        "commit index is missing LSN {} and needs writable reconstruction",
                        lsn.0
                    ),
                ));
            }
        }
    }
    Ok(())
}

#[allow(clippy::type_complexity)]
fn logical_read_sync_provenance(
    relational_tables: &HashMap<String, Vec<VersionedRow>>,
    persisted_lsns: HashMap<(String, RowId), Lsn>,
    persisted_kinds: HashMap<(String, RowId), u8>,
) -> (
    HashMap<(String, RowId), Lsn>,
    HashMap<(String, RowId), SyncSourceKind>,
) {
    let live_rows = relational_tables
        .iter()
        .map(|(table, rows)| {
            (
                table.clone(),
                rows.iter()
                    .filter(|row| row.deleted_tx.is_none())
                    .map(|row| row.row_id)
                    .collect::<HashSet<_>>(),
            )
        })
        .collect::<HashMap<_, _>>();
    let source_lsns = persisted_lsns
        .into_iter()
        .filter(|((table, row_id), _)| {
            live_rows
                .get(table)
                .is_some_and(|rows| rows.contains(row_id))
        })
        .collect::<HashMap<_, _>>();
    let mut source_kinds = persisted_kinds
        .into_iter()
        .filter_map(|((table, row_id), kind)| {
            source_lsns
                .contains_key(&(table.clone(), row_id))
                .then_some((
                    (table, row_id),
                    match kind {
                        1 => SyncSourceKind::AcceptedLocal,
                        2 => SyncSourceKind::AcceptedLocalPending,
                        _ => SyncSourceKind::Pulled,
                    },
                ))
        })
        .collect::<HashMap<_, _>>();
    for key in source_lsns.keys() {
        source_kinds
            .entry(key.clone())
            .or_insert(SyncSourceKind::Pulled);
    }
    (source_lsns, source_kinds)
}

/// The integer key a caller names a row by: the value in its declared
/// single-column integer primary key. The internal `RowId` is an allocator
/// counter, so projecting it would answer a lookup for row `42` with whatever
/// row happened to be inserted forty-second.
fn read_row_relational_identifier(
    table: &str,
    row_id: RowId,
    table_meta: &HashMap<String, TableMeta>,
    relational_tables: &HashMap<String, Vec<VersionedRow>>,
) -> Option<i64> {
    let meta = table_meta.get(table)?;
    if meta.primary_key_columns.len() > 1 {
        return None;
    }
    let key_column = meta.primary_key_columns.first().cloned().or_else(|| {
        let mut declared = meta.columns.iter().filter(|column| column.primary_key);
        let column = declared.next()?;
        declared.next().is_none().then(|| column.name.clone())
    })?;
    let row = relational_tables
        .get(table)?
        .iter()
        .filter(|row| row.row_id == row_id && row.deleted_tx.is_none())
        .max_by_key(|row| (row.lsn, row.created_tx))?;
    match row.values.get(&key_column) {
        Some(Value::Int64(value)) => Some(*value),
        _ => None,
    }
}

fn compatibility_read_vector_projection(
    current_vectors: &HashMap<(VectorIndexRef, RowId), Vec<f32>>,
    table_meta: &HashMap<String, TableMeta>,
    relational_tables: &HashMap<String, Vec<VersionedRow>>,
) -> std::result::Result<BTreeMap<(String, i64), Vec<f32>>, LoadReadImageError> {
    let mut entries = current_vectors.iter().collect::<Vec<_>>();
    entries.sort_by(
        |((left_index, left_row), _), ((right_index, right_row), _)| {
            (&left_index.table, &left_index.column, left_row).cmp(&(
                &right_index.table,
                &right_index.column,
                right_row,
            ))
        },
    );
    let mut vectors = BTreeMap::new();
    for ((index, row_id), vector) in entries {
        let identifier = match read_row_relational_identifier(
            &index.table,
            *row_id,
            table_meta,
            relational_tables,
        ) {
            Some(identifier) => identifier,
            None => i64::try_from(row_id.0).map_err(|_| {
                direct_read_requires_writer_for(
                    ReadImageRequiresWriterCause::InvalidVectorReference,
                    format!(
                        "vector row id {} for {}.{} is outside the relational identifier range",
                        row_id.0, index.table, index.column
                    ),
                )
            })?,
        };
        vectors.insert((index.table.clone(), identifier), vector.clone());
    }
    Ok(vectors)
}

fn decode_complete_read_image(
    read_txn: &redb::ReadTransaction,
) -> std::result::Result<ReadPersistenceImage, LoadReadImageError> {
    let format_table = match read_txn.open_table(FORMAT_METADATA_TABLE) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => {
            return Err(legacy_read_layout("format metadata is absent"));
        }
        Err(error) => return Err(read_table_error(error)),
    };
    let format = format_table
        .get(FORMAT_VERSION_KEY)
        .map_err(read_storage_error)?
        .ok_or_else(|| legacy_read_layout("format marker is absent"))?;
    let format: String = RedbPersistence::decode(format.value()).map_err(read_image_error)?;
    if format != CURRENT_FORMAT_VERSION {
        return Err(legacy_read_layout(format!(
            "format marker {format:?} is not current"
        )));
    }
    drop(format_table);

    let table_meta = decode_read_table_meta(read_txn)?;
    let mut relational_tables = HashMap::new();
    let mut names = table_meta.keys().cloned().collect::<Vec<_>>();
    names.sort();
    for name in names {
        let meta = table_meta.get(&name).ok_or_else(|| {
            invalid_read_image_state(format!("collected table metadata disappeared for {name}"))
        })?;
        relational_tables.insert(
            name.clone(),
            decode_read_relational_table(read_txn, &name, meta)?,
        );
    }
    let forward_edges = decode_read_graph_table(read_txn, GRAPH_FWD_TABLE)?;
    let reverse_edges = decode_read_graph_table(read_txn, GRAPH_REV_TABLE)?;
    validate_reverse_graph_projection(&forward_edges, &reverse_edges)?;
    let vector_entries = decode_read_vectors(read_txn)?;
    let change_log = decode_read_change_log(read_txn)?;
    let ddl_log = decode_read_ddl_log(read_txn)?;
    let current_vectors = validate_and_collect_current_vectors(
        &table_meta,
        &mut relational_tables,
        &vector_entries,
        &ddl_log,
    )?;
    let vectors =
        compatibility_read_vector_projection(&current_vectors, &table_meta, &relational_tables)?;
    let (sync_source_lsns, sync_source_kinds) = logical_read_sync_provenance(
        &relational_tables,
        decode_read_sync_source_lsns(read_txn)?,
        decode_read_sync_source_kinds(read_txn)?,
    );
    let commit_index = decode_read_commit_index(read_txn)?;
    let reconstructed_commit_index = reconstruct_read_commit_index(
        &relational_tables,
        &forward_edges,
        &vector_entries,
        &change_log,
    );
    validate_read_commit_index(&commit_index, &reconstructed_commit_index)?;
    let config_values = decode_read_raw_string_table(read_txn, CONFIG_TABLE)?;
    let sink_audit = decode_read_raw_string_table(read_txn, SINK_AUDIT_TABLE)?;
    let trigger_audit = decode_read_raw_string_table(read_txn, TRIGGER_AUDIT_TABLE)?;
    let trigger_audit_stamps = decode_read_trigger_audit_stamps(read_txn)?;
    let sink_queues = decode_read_sink_queues(read_txn)?;
    Ok(ReadPersistenceImage {
        vectors,
        table_meta,
        relational_tables,
        forward_edges,
        reverse_edges,
        vector_entries,
        current_vectors,
        sync_source_lsns,
        sync_source_kinds,
        change_log,
        ddl_log,
        commit_index,
        config_values,
        sink_audit,
        trigger_audit,
        trigger_audit_stamps,
        sink_queues,
        // Filled in by the source that has the file open; the decoder itself
        // is handed a read transaction and never sees a path.
        store_file_bytes: None,
    })
}

/// The path of a store's companion lock file.
///
/// Delegates to `contextdb_core::store_companion_path`, the one source of
/// truth for this name: vigil links contextdb-core unconditionally but
/// contextdb-engine only behind its `fabric` feature, so the naming rule
/// must live where every consumer can reach it. This function stays public
/// here purely so existing engine callers keep working; see
/// `contextdb_core::store_companion_path` for the append contract and the
/// "Stable companion lock" authority reference.
pub fn store_companion_path(store_path: &Path) -> PathBuf {
    contextdb_core::store_companion_path(store_path)
}

fn companion_path(path: &Path) -> PathBuf {
    store_companion_path(path)
}

#[derive(Clone)]
struct CompanionState {
    needs_initialization: bool,
    active_slot: Option<usize>,
    active_record: Option<EncodedCompanionRecord>,
    pending_slot: Option<usize>,
    pending_replacement: Option<PendingReplacement>,
    pending_matches_store: Option<bool>,
}

#[derive(Clone)]
struct PendingReplacement {
    store_fingerprint: [u8; 32],
    intended_record: EncodedCompanionRecord,
    checksummed_payload: Vec<u8>,
    stored_checksum: [u8; 32],
}

enum ParsedCompanionSlot {
    Vacant,
    Record(EncodedCompanionRecord),
    Pending(PendingReplacement),
}

struct CompanionGuardState {
    file: File,
    companion: CompanionState,
}

/// Exclusive ownership of one verified, never-unlinked companion inode.
/// Separate opens in the same process contend exactly like separate
/// processes; migration receives access only through its already-open
/// `LegacyMigrationSource` and never reacquires by pathname.
pub(crate) struct CompanionGuard {
    path: PathBuf,
    state: Mutex<CompanionGuardState>,
}

/// A writer refusal that names the store it is about and, when the holder has
/// published its companion record, the exact process holding it. The record is
/// read without the lock the holder owns, which is safe because every
/// generation carries its own checksum: an unpublished or torn read leaves the
/// refusal naming only the store rather than inventing a process number.
fn held_by_writer_error(database_path: &Path, companion: &mut File) -> Error {
    let process_id = read_companion_state(companion)
        .ok()
        .and_then(|state| state.active_record)
        .map(|record| u64::from(record.fields.process_id));
    Error::ReadFailure(
        ReadFailure::new(
            ReadFailureKind::HeldByWriter,
            ReadFailureDetail::HeldByWriter(HeldByWriterDetail {
                process_id,
                store_path: database_path.display().to_string(),
            }),
        )
        .expect("held-by-writer carries the writer-contention detail"),
    )
}

/// Whether this pathname still names the exact companion file the caller is
/// holding open. A removal is bound to the inode it was promised, never to a
/// name somebody else may have since put something else at.
#[cfg(unix)]
fn companion_still_names(companion: &Path, held: &File) -> bool {
    use std::os::unix::fs::MetadataExt as _;

    let Ok(open) = held.metadata() else {
        return false;
    };
    let Ok(named) = std::fs::symlink_metadata(companion) else {
        return false;
    };
    named.file_type().is_file() && open.dev() == named.dev() && open.ino() == named.ino()
}

#[cfg(not(unix))]
fn companion_still_names(companion: &Path, _held: &File) -> bool {
    companion.is_file()
}

fn companion_slot_offset(slot: usize) -> std::result::Result<usize, String> {
    if slot >= 2 {
        return Err(format!("companion slot {slot} is out of range"));
    }
    Ok(COMPANION_FIRST_SLOT_OFFSET + slot * COMPANION_SLOT_BYTES)
}

fn companion_selector_bytes(
    active_slot: usize,
    generation: u64,
) -> std::result::Result<[u8; COMPANION_SELECTOR_BYTES], String> {
    if active_slot >= 2 {
        return Err(format!(
            "companion active slot {active_slot} is out of range"
        ));
    }
    let mut selector = [0_u8; COMPANION_SELECTOR_BYTES];
    selector[..COMPANION_SELECTOR_MAGIC.len()].copy_from_slice(COMPANION_SELECTOR_MAGIC);
    selector[8] = active_slot as u8;
    selector[16..24].copy_from_slice(&generation.to_le_bytes());
    let checksum = blake3::hash(&selector[..COMPANION_SELECTOR_BODY_BYTES]);
    selector[COMPANION_SELECTOR_BODY_BYTES..].copy_from_slice(checksum.as_bytes());
    Ok(selector)
}

fn initialize_companion_inode(file: &mut File) -> std::result::Result<CompanionState, String> {
    let selector = companion_selector_bytes(0, 0)?;
    // Every intermediate is recognizable as an incomplete generation-zero
    // initialization. No write can turn a prior complete record into this
    // state because this function is called only when no valid slot exists.
    file.set_len(0)
        .map_err(|error| format!("clear companion inode: {error}"))?;
    file.sync_all()
        .map_err(|error| format!("sync cleared companion inode: {error}"))?;
    file.set_len(COMPANION_FILE_BYTES as u64)
        .map_err(|error| format!("size companion inode: {error}"))?;
    file.sync_all()
        .map_err(|error| format!("sync sized companion inode: {error}"))?;
    file.seek(SeekFrom::Start(0))
        .map_err(|error| format!("seek companion inode: {error}"))?;
    file.write_all(COMPANION_FILE_MAGIC)
        .map_err(|error| format!("write companion magic: {error}"))?;
    file.sync_data()
        .map_err(|error| format!("sync companion magic: {error}"))?;
    file.seek(SeekFrom::Start(COMPANION_SELECTOR_OFFSET as u64))
        .map_err(|error| format!("seek companion selector: {error}"))?;
    file.write_all(&selector)
        .map_err(|error| format!("write initial companion selector: {error}"))?;
    file.sync_data()
        .map_err(|error| format!("sync initial companion selector: {error}"))?;
    Ok(CompanionState {
        needs_initialization: false,
        active_slot: None,
        active_record: None,
        pending_slot: None,
        pending_replacement: None,
        pending_matches_store: None,
    })
}

fn encode_pending_replacement(
    store_fingerprint: [u8; 32],
    intended_record: EncodedCompanionRecord,
) -> std::result::Result<PendingReplacement, String> {
    let intended_length = u32::try_from(intended_record.checksummed_payload.len())
        .map_err(|_| "pending replacement record is too large".to_owned())?;
    let mut payload = Vec::with_capacity(
        COMPANION_PENDING_MAGIC.len() + 32 + 4 + intended_record.checksummed_payload.len() + 32,
    );
    payload.extend_from_slice(COMPANION_PENDING_MAGIC);
    payload.extend_from_slice(&store_fingerprint);
    payload.extend_from_slice(&intended_length.to_le_bytes());
    payload.extend_from_slice(&intended_record.checksummed_payload);
    payload.extend_from_slice(&intended_record.stored_checksum);
    if payload.len() > COMPANION_SLOT_PAYLOAD_CAPACITY {
        return Err("pending replacement exceeds its fixed companion slot".to_owned());
    }
    let stored_checksum = *blake3::hash(&payload).as_bytes();
    Ok(PendingReplacement {
        store_fingerprint,
        intended_record,
        checksummed_payload: payload,
        stored_checksum,
    })
}

fn decode_pending_replacement(
    payload: &[u8],
    stored_checksum: [u8; 32],
) -> std::result::Result<PendingReplacement, String> {
    if *blake3::hash(payload).as_bytes() != stored_checksum {
        return Err("pending replacement checksum mismatch".to_owned());
    }
    let mut cursor = CompanionPayloadCursor { payload, offset: 0 };
    if cursor.take(COMPANION_PENDING_MAGIC.len())? != COMPANION_PENDING_MAGIC {
        return Err("pending replacement magic mismatch".to_owned());
    }
    let store_fingerprint = cursor.array::<32>()?;
    let intended_length = usize::try_from(cursor.u32()?)
        .map_err(|_| "pending replacement length does not fit usize".to_owned())?;
    let intended_payload = cursor.take(intended_length)?.to_vec();
    let intended_checksum = cursor.array::<32>()?;
    cursor.finish()?;
    let intended_record = decode_companion_record(&intended_payload, intended_checksum)?;
    Ok(PendingReplacement {
        store_fingerprint,
        intended_record,
        checksummed_payload: payload.to_vec(),
        stored_checksum,
    })
}

fn parse_companion_slot(bytes: &[u8]) -> std::result::Result<ParsedCompanionSlot, String> {
    if bytes.len() != COMPANION_SLOT_BYTES {
        return Err(format!(
            "companion slot has {} bytes, expected {COMPANION_SLOT_BYTES}",
            bytes.len()
        ));
    }
    let length = u32::from_le_bytes(
        bytes[..4]
            .try_into()
            .map_err(|_| "companion slot length is truncated".to_owned())?,
    ) as usize;
    let payload = &bytes[4..4 + COMPANION_SLOT_PAYLOAD_CAPACITY];
    let checksum: [u8; 32] = bytes[4 + COMPANION_SLOT_PAYLOAD_CAPACITY..]
        .try_into()
        .map_err(|_| "companion slot checksum is truncated".to_owned())?;
    if length == 0 {
        if payload.iter().any(|byte| *byte != 0) || checksum != [0; 32] {
            return Err("vacant companion slot contains record bytes".to_owned());
        }
        return Ok(ParsedCompanionSlot::Vacant);
    }
    if length > COMPANION_SLOT_PAYLOAD_CAPACITY {
        return Err(format!(
            "companion slot payload length {length} exceeds its capacity"
        ));
    }
    if payload[length..].iter().any(|byte| *byte != 0) {
        return Err("companion slot has nonzero padding".to_owned());
    }
    let payload = &payload[..length];
    if payload.starts_with(COMPANION_PENDING_MAGIC) {
        decode_pending_replacement(payload, checksum).map(ParsedCompanionSlot::Pending)
    } else {
        decode_companion_record(payload, checksum).map(ParsedCompanionSlot::Record)
    }
}

fn recoverable_generation_zero_initialization(bytes: &[u8]) -> bool {
    if bytes.len() > COMPANION_FILE_BYTES {
        return false;
    }
    let Ok(selector) = companion_selector_bytes(0, 0) else {
        return false;
    };
    let mut intended = vec![0_u8; COMPANION_FILE_BYTES];
    intended[..COMPANION_FILE_MAGIC.len()].copy_from_slice(COMPANION_FILE_MAGIC);
    intended[COMPANION_SELECTOR_OFFSET..].copy_from_slice(&selector);
    bytes
        .iter()
        .zip(intended)
        .all(|(observed, intended)| *observed == 0 || *observed == intended)
}

fn parse_companion_selector(bytes: &[u8]) -> Option<(usize, u64)> {
    let selector = bytes.get(COMPANION_SELECTOR_OFFSET..COMPANION_FILE_BYTES)?;
    let body = selector.get(..COMPANION_SELECTOR_BODY_BYTES)?;
    if body.get(..COMPANION_SELECTOR_MAGIC.len())? != COMPANION_SELECTOR_MAGIC {
        return None;
    }
    let active_slot = usize::from(*body.get(8)?);
    if active_slot >= 2 || body.get(9..16)?.iter().any(|byte| *byte != 0) {
        return None;
    }
    let generation = u64::from_le_bytes(body.get(16..24)?.try_into().ok()?);
    let checksum: [u8; 32] = selector
        .get(COMPANION_SELECTOR_BODY_BYTES..COMPANION_SELECTOR_BYTES)?
        .try_into()
        .ok()?;
    (*blake3::hash(body).as_bytes() == checksum).then_some((active_slot, generation))
}

fn read_companion_state(file: &mut File) -> std::result::Result<CompanionState, String> {
    file.seek(SeekFrom::Start(0))
        .map_err(|error| format!("seek companion: {error}"))?;
    let mut bytes = vec![0_u8; COMPANION_FILE_BYTES];
    let mut length = 0;
    while length < COMPANION_FILE_BYTES {
        let read = match file.read(&mut bytes[length..]) {
            Ok(read) => read,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(format!("read companion: {error}")),
        };
        if read == 0 {
            break;
        }
        length += read;
    }
    bytes.truncate(length);
    if length == COMPANION_FILE_BYTES {
        let mut oversize = [0_u8; 1];
        if file
            .read(&mut oversize)
            .map_err(|error| format!("probe companion length: {error}"))?
            != 0
        {
            return Err(format!(
                "companion inode exceeds its fixed {COMPANION_FILE_BYTES}-byte layout"
            ));
        }
    }
    if recoverable_generation_zero_initialization(&bytes)
        && (bytes.len() != COMPANION_FILE_BYTES || !bytes.starts_with(COMPANION_FILE_MAGIC))
    {
        return Ok(CompanionState {
            needs_initialization: true,
            active_slot: None,
            active_record: None,
            pending_slot: None,
            pending_replacement: None,
            pending_matches_store: None,
        });
    }
    if bytes.len() != COMPANION_FILE_BYTES || !bytes.starts_with(COMPANION_FILE_MAGIC) {
        return Err("companion inode has an unsupported layout".to_owned());
    }
    let slots: [std::result::Result<ParsedCompanionSlot, String>; 2] =
        std::array::from_fn(|slot| {
            let offset = companion_slot_offset(slot).expect("fixed companion slot is in range");
            parse_companion_slot(&bytes[offset..offset + COMPANION_SLOT_BYTES])
        });
    let selected =
        parse_companion_selector(&bytes).and_then(|(slot, generation)| match &slots[slot] {
            Ok(ParsedCompanionSlot::Record(record)) if record.fields.generation == generation => {
                Some((slot, record.clone()))
            }
            _ => None,
        });
    // A malformed/torn selector is not a single point of failure. Choose the
    // highest checksummed normal slot; either the old or the new synced
    // generation is therefore recoverable after every selector boundary.
    let fallback = slots
        .iter()
        .enumerate()
        .filter_map(|(slot, value)| match value {
            Ok(ParsedCompanionSlot::Record(record)) => Some((slot, record.clone())),
            _ => None,
        })
        .max_by_key(|(slot, record)| (record.fields.generation, *slot));
    let active = selected.or(fallback);
    let pending = slots
        .iter()
        .enumerate()
        .filter_map(|(slot, value)| match value {
            Ok(ParsedCompanionSlot::Pending(pending)) => Some((slot, pending.clone())),
            _ => None,
        })
        .max_by_key(|(slot, pending)| (pending.intended_record.fields.generation, *slot));
    let all_vacant = slots
        .iter()
        .all(|slot| matches!(slot, Ok(ParsedCompanionSlot::Vacant)));
    // Generation zero deliberately has no published identity yet. If its
    // selector is still intact, an interrupted first-slot payload/checksum
    // write is just another incomplete initialization: discard no complete
    // record, reinitialize in place, and let the next writer publish afresh.
    if active.is_none() && pending.is_none() && parse_companion_selector(&bytes) == Some((0, 0)) {
        return Ok(CompanionState {
            needs_initialization: true,
            active_slot: None,
            active_record: None,
            pending_slot: None,
            pending_replacement: None,
            pending_matches_store: None,
        });
    }
    if active.is_none() && pending.is_none() && !all_vacant {
        let damage = slots
            .iter()
            .filter_map(|slot| slot.as_ref().err())
            .cloned()
            .collect::<Vec<_>>()
            .join("; ");
        return Err(format!("companion has no recoverable slot: {damage}"));
    }
    if active.is_none() && pending.is_none() {
        if recoverable_generation_zero_initialization(&bytes) {
            return Ok(CompanionState {
                needs_initialization: true,
                active_slot: None,
                active_record: None,
                pending_slot: None,
                pending_replacement: None,
                pending_matches_store: None,
            });
        }
        return Err("empty companion has no valid generation-zero selector".to_owned());
    }
    let (active_slot, active_record) = active
        .map(|(slot, record)| (Some(slot), Some(record)))
        .unwrap_or((None, None));
    let (pending_slot, pending_replacement) = pending
        .map(|(slot, pending)| (Some(slot), Some(pending)))
        .unwrap_or((None, None));
    Ok(CompanionState {
        needs_initialization: false,
        active_slot,
        active_record,
        pending_slot,
        pending_replacement,
        pending_matches_store: None,
    })
}

fn companion_writer_record(
    path: &Path,
    previous: Option<&EncodedCompanionRecord>,
    new_database_identity: bool,
) -> Result<EncodedCompanionRecord> {
    let generation = match previous {
        Some(record) => record
            .fields
            .generation
            .checked_add(1)
            .ok_or_else(|| Error::Other("companion generation overflow".to_owned()))?,
        None => 1,
    };
    let database_identity = if new_database_identity {
        DatabaseIdentity(uuid::Uuid::new_v4().into_bytes())
    } else {
        previous
            .map(|record| record.fields.database_identity)
            .unwrap_or_else(|| DatabaseIdentity(uuid::Uuid::new_v4().into_bytes()))
    };
    // A writer that has just CLAIMED the store has not yet decided anything
    // about serving inspection -- its channel has not been tried. So the run
    // it publishes says nothing about serving rather than inventing a verdict
    // a later reader would be told: a reader arriving inside that window sees
    // a store whose owner has not answered the question yet, and asks again,
    // instead of being handed a startup failure that never happened. The real
    // decision is re-recorded the moment it exists.
    let owner_read_status = bounded_companion_status(OwnerReadStatus {
        state: OwnerServingState::NotApplicable,
        reason: None,
    });
    let record = CompanionRecord {
        format_version: 1,
        generation,
        database_identity,
        writer_run_number: WriterRunNumber(uuid::Uuid::new_v4().into_bytes()),
        owner_user: companion_effective_user_identity(),
        channel_address: derive_channel_address(path)
            .map_err(|error| Error::Other(format!("derive companion channel address: {error}")))?,
        process_id: std::process::id(),
        owner_read_status,
    };
    prepare_companion_record_for_publication(&record)
        .map_err(|error| Error::Other(format!("encode companion record: {error}")))
}

fn companion_exists(path: &Path) -> Result<bool> {
    let companion = companion_path(path);
    match std::fs::symlink_metadata(&companion) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(Error::StoreCorrupted {
            path: companion.display().to_string(),
            reason: "companion path is a symbolic link".to_owned(),
        }),
        Ok(metadata) if !metadata.file_type().is_file() => Err(Error::StoreCorrupted {
            path: companion.display().to_string(),
            reason: "companion path is not a regular file".to_owned(),
        }),
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(RedbPersistence::storage_error(error)),
    }
}

#[cfg(unix)]
fn validate_companion_metadata(
    path: &Path,
    file: &File,
    before_lock: &std::fs::Metadata,
) -> Result<()> {
    use std::os::unix::fs::MetadataExt as _;

    let after_lock = file.metadata().map_err(RedbPersistence::storage_error)?;
    let path_metadata = std::fs::symlink_metadata(path).map_err(RedbPersistence::storage_error)?;
    let same_inode = |left: &std::fs::Metadata, right: &std::fs::Metadata| {
        left.dev() == right.dev() && left.ino() == right.ino()
    };
    let expected_user = nix::unistd::Uid::effective().as_raw();
    if !before_lock.file_type().is_file()
        || !after_lock.file_type().is_file()
        || path_metadata.file_type().is_symlink()
        || !path_metadata.file_type().is_file()
        || before_lock.uid() != expected_user
        || after_lock.uid() != expected_user
        || path_metadata.uid() != expected_user
        || before_lock.mode() & 0o7777 != 0o600
        || after_lock.mode() & 0o7777 != 0o600
        || path_metadata.mode() & 0o7777 != 0o600
        || before_lock.nlink() != 1
        || after_lock.nlink() != 1
        || path_metadata.nlink() != 1
        || !same_inode(before_lock, &after_lock)
        || !same_inode(&after_lock, &path_metadata)
    {
        return Err(Error::StoreCorrupted {
            path: path.display().to_string(),
            reason: "companion must remain one current-user-owned 0600 regular inode across lock acquisition"
                .to_owned(),
        });
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_companion_metadata(
    path: &Path,
    file: &File,
    before_lock: &std::fs::Metadata,
) -> Result<()> {
    let after_lock = file.metadata().map_err(RedbPersistence::storage_error)?;
    let path_metadata = std::fs::symlink_metadata(path).map_err(RedbPersistence::storage_error)?;
    if !before_lock.file_type().is_file()
        || !after_lock.file_type().is_file()
        || path_metadata.file_type().is_symlink()
        || !path_metadata.file_type().is_file()
    {
        return Err(Error::StoreCorrupted {
            path: path.display().to_string(),
            reason: "companion must be a direct regular file".to_owned(),
        });
    }
    Ok(())
}

fn store_file_fingerprint(path: &Path) -> Result<[u8; 32]> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    }
    let mut file = options.open(path).map_err(RedbPersistence::storage_error)?;
    let metadata = file.metadata().map_err(RedbPersistence::storage_error)?;
    if !metadata.file_type().is_file() {
        return Err(Error::Other(format!(
            "replacement database is not a regular file: {}",
            path.display()
        )));
    }
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"contextdb-store-fingerprint-v1");
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        hasher.update(&metadata.dev().to_le_bytes());
        hasher.update(&metadata.ino().to_le_bytes());
    }
    hasher.update(&metadata.len().to_le_bytes());
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(RedbPersistence::storage_error)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    let after = file.metadata().map_err(RedbPersistence::storage_error)?;
    let path_metadata = std::fs::symlink_metadata(path).map_err(RedbPersistence::storage_error)?;
    if !after.file_type().is_file()
        || path_metadata.file_type().is_symlink()
        || !path_metadata.file_type().is_file()
        || after.len() != metadata.len()
        || path_metadata.len() != metadata.len()
    {
        return Err(Error::Other(format!(
            "replacement database path changed while it was fingerprinted: {}",
            path.display()
        )));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        // More than one name for this inode means the pathname is not proof
        // of which file was measured: another name can be renamed over it, or
        // this one unlinked, with the inode surviving. A fingerprint taken
        // here cannot identify a generation, so anything destructive decided
        // on it is refused rather than run on the pathname's word.
        if metadata.nlink() != 1 || after.nlink() != 1 || path_metadata.nlink() != 1 {
            return Err(Error::StoreIdentityUnprovable {
                path: path.display().to_string(),
            });
        }
        if metadata.dev() != after.dev()
            || metadata.ino() != after.ino()
            || metadata.dev() != path_metadata.dev()
            || metadata.ino() != path_metadata.ino()
        {
            return Err(Error::Other(format!(
                "the inode at {} changed while it was fingerprinted, so the fingerprint identifies no generation",
                path.display()
            )));
        }
    }
    Ok(*hasher.finalize().as_bytes())
}

/// Hash an already-open store descriptor exactly as [`store_file_fingerprint`]
/// hashes a pathname. The bytes are identical; the difference is that the
/// inode is named by a descriptor this call holds, so nothing swapped into
/// the pathname afterwards can change what was measured.
#[cfg(unix)]
fn store_descriptor_fingerprint(file: &mut File, metadata: &std::fs::Metadata) -> Result<[u8; 32]> {
    use std::os::unix::fs::MetadataExt as _;

    let mut hasher = blake3::Hasher::new();
    hasher.update(b"contextdb-store-fingerprint-v1");
    hasher.update(&metadata.dev().to_le_bytes());
    hasher.update(&metadata.ino().to_le_bytes());
    hasher.update(&metadata.len().to_le_bytes());
    file.seek(SeekFrom::Start(0))
        .map_err(RedbPersistence::storage_error)?;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(RedbPersistence::storage_error)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    let after = file.metadata().map_err(RedbPersistence::storage_error)?;
    if after.dev() != metadata.dev()
        || after.ino() != metadata.ino()
        || after.len() != metadata.len()
    {
        return Err(Error::Other(
            "replacement candidate changed while it was fingerprinted".to_owned(),
        ));
    }
    Ok(*hasher.finalize().as_bytes())
}

#[cfg(unix)]
fn open_directory_entry(directory: &File, name: &Path) -> std::io::Result<File> {
    let owned = nix::fcntl::openat(
        directory,
        name,
        nix::fcntl::OFlag::O_RDONLY | nix::fcntl::OFlag::O_NOFOLLOW | nix::fcntl::OFlag::O_CLOEXEC,
        nix::sys::stat::Mode::empty(),
    )
    .map_err(std::io::Error::from)?;
    Ok(File::from(owned))
}

/// Unlink `name` under `directory` only while that name still resolves to
/// `identity`.
///
/// The re-check and the removal go through the SAME directory descriptor and
/// the identity comes from a descriptor this call opens, so an entry swapped
/// in the parent between the two cannot redirect the unlink onto a file this
/// store never recorded. Anything that cannot be proven to be `identity` is
/// LEFT IN PLACE and reported as not removed -- never deleted on a guess.
#[cfg(unix)]
fn unlink_verified_entry(directory: &File, name: &Path, identity: (u64, u64)) -> Result<bool> {
    use std::os::unix::fs::MetadataExt as _;

    let candidate = match open_directory_entry(directory, name) {
        Ok(candidate) => candidate,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(RedbPersistence::storage_error(error)),
    };
    let metadata = candidate
        .metadata()
        .map_err(RedbPersistence::storage_error)?;
    if !metadata.file_type().is_file() || (metadata.dev(), metadata.ino()) != identity {
        return Ok(false);
    }
    nix::unistd::unlinkat(directory, name, nix::unistd::UnlinkatFlags::NoRemoveDir)
        .map_err(|error| RedbPersistence::storage_error(std::io::Error::from(error)))?;
    drop(candidate);
    Ok(true)
}

/// Remove the exact store a durable replacement intent recorded, together
/// with the companion beside it.
///
/// The intent carries a checksummed store fingerprint, never a pathname, so
/// the target is IDENTIFIED by fingerprinting each store-adjacent candidate
/// through a descriptor and removing only the one whose fingerprint is the
/// recorded value. A pathname pattern bounds the scan; it never decides a
/// removal, so a complete store that merely looks generated is untouched.
///
/// Every open and the unlink go through one held directory descriptor, and
/// the inode is re-proven on that descriptor immediately before the unlink.
/// A candidate whose identity cannot be proven is left alone; failing to
/// remove the RECORDED target is reported typed rather than passed over,
/// because that is the operator artifact this call exists to clear.
#[cfg(unix)]
fn remove_abandoned_replacement_target(path: &Path, fingerprint: &[u8; 32]) -> Result<()> {
    use std::os::unix::fs::MetadataExt as _;

    let (Some(directory_path), Some(store_name)) = (path.parent(), path.file_name()) else {
        return Ok(());
    };
    let mut prefix = store_name.to_os_string();
    prefix.push(".");
    let companion_name = companion_path(path)
        .file_name()
        .map(std::ffi::OsStr::to_os_string);
    let directory = match File::open(directory_path) {
        Ok(directory) => directory,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(RedbPersistence::storage_error(error)),
    };
    let entries = match std::fs::read_dir(directory_path) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(RedbPersistence::storage_error(error)),
    };
    let mut names = Vec::new();
    for entry in entries {
        let name = entry.map_err(RedbPersistence::storage_error)?.file_name();
        if name == *store_name || companion_name.as_deref() == Some(name.as_os_str()) {
            continue;
        }
        if !name
            .as_encoded_bytes()
            .starts_with(prefix.as_encoded_bytes())
        {
            continue;
        }
        names.push(PathBuf::from(name));
    }
    names.sort();
    let mut removed = false;
    for name in names {
        // A candidate that cannot be opened and measured through this
        // directory descriptor -- a directory, a symlink, a file another
        // owner holds -- is by definition not the inode this store recorded,
        // so it is passed over rather than guessed about.
        let Ok(mut candidate) = open_directory_entry(&directory, &name) else {
            continue;
        };
        let Ok(metadata) = candidate.metadata() else {
            continue;
        };
        if !metadata.file_type().is_file() {
            continue;
        }
        let identity = (metadata.dev(), metadata.ino());
        let Ok(observed) = store_descriptor_fingerprint(&mut candidate, &metadata) else {
            continue;
        };
        if observed != *fingerprint {
            continue;
        }
        drop(candidate);
        if !unlink_verified_entry(&directory, &name, identity)? {
            return Err(Error::StoreCorrupted {
                path: directory_path.join(&name).display().to_string(),
                reason: "the recorded replacement target could not be re-proven at its own name, so it was left in place".to_owned(),
            });
        }
        let sidecar = contextdb_core::store_companion_path(&name);
        if let Ok(companion) = open_directory_entry(&directory, &sidecar)
            && let Ok(companion_metadata) = companion.metadata()
            && companion_metadata.file_type().is_file()
        {
            let companion_identity = (companion_metadata.dev(), companion_metadata.ino());
            drop(companion);
            unlink_verified_entry(&directory, &sidecar, companion_identity)?;
        }
        removed = true;
    }
    if removed {
        sync_database_parent(path)?;
    }
    Ok(())
}

/// Without `unlinkat`, an inode proven by a descriptor cannot be removed by
/// that identity, only by a name another process could have re-pointed. The
/// abandoned target is therefore LEFT in place rather than removed on a
/// pathname's word.
#[cfg(not(unix))]
fn remove_abandoned_replacement_target(_path: &Path, _fingerprint: &[u8; 32]) -> Result<()> {
    Ok(())
}

/// Remove the companion the installed replacement was generated with, at the
/// pathname it was generated under.
///
/// A replacement is installed by renaming its store onto this pathname, which
/// leaves the companion it was built with orphaned at the name the store just
/// left. The intent records a fingerprint, never a pathname, so that orphan is
/// IDENTIFIED the same way the abandoned target's sidecar is -- by the
/// companion naming rule -- and then removed only when TWO independent proofs
/// hold on the candidate itself: its own durable record decodes and names
/// exactly the store pathname it sits beside, and no store remains at that
/// pathname. A file that does not decode as a companion generation, and a
/// companion whose store is still there, are therefore never touched, so a
/// generated-shaped decoy survives recovery whole.
///
/// Every open and the unlink go through one held directory descriptor, and the
/// inode is re-proven on that descriptor immediately before the unlink.
#[cfg(unix)]
fn remove_installed_replacement_companion(path: &Path) -> Result<()> {
    use std::os::unix::fs::MetadataExt as _;

    let (Some(directory_path), Some(store_name)) = (path.parent(), path.file_name()) else {
        return Ok(());
    };
    let mut prefix = store_name.to_os_string();
    prefix.push(".");
    let own_companion = companion_path(path)
        .file_name()
        .map(std::ffi::OsStr::to_os_string);
    // The address a companion records is derived from its store's resolved
    // pathname, and the store this one named is already gone, so the
    // directory is resolved once here and the vanished name joined onto it.
    let canonical_directory = match std::fs::canonicalize(directory_path) {
        Ok(directory) => directory,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(RedbPersistence::storage_error(error)),
    };
    let directory = match File::open(directory_path) {
        Ok(directory) => directory,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(RedbPersistence::storage_error(error)),
    };
    let entries = match std::fs::read_dir(directory_path) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(RedbPersistence::storage_error(error)),
    };
    let mut names = Vec::new();
    for entry in entries {
        let name = entry.map_err(RedbPersistence::storage_error)?.file_name();
        if name == *store_name || own_companion.as_deref() == Some(name.as_os_str()) {
            continue;
        }
        if !name
            .as_encoded_bytes()
            .starts_with(prefix.as_encoded_bytes())
        {
            continue;
        }
        names.push(PathBuf::from(name));
    }
    names.sort();
    let mut removed = false;
    for name in names {
        // The companion naming rule is the store's own pathname with `.lock`
        // appended (`companion_path`), so the stem below is that store name.
        if name.extension().and_then(std::ffi::OsStr::to_str) != Some("lock") {
            continue;
        }
        // The store pathname this candidate is the companion OF -- the name
        // the installed replacement occupied before it was renamed here.
        let Some(generated_name) = name.file_stem().map(std::ffi::OsStr::to_os_string) else {
            continue;
        };
        let generated_name = PathBuf::from(generated_name);
        // A companion whose store is still at its own name belongs to that
        // store, not to the generation installed here.
        if open_directory_entry(&directory, &generated_name).is_ok() {
            continue;
        }
        let recorded_address = ChannelAddress(
            *blake3::hash(
                canonical_directory
                    .join(&generated_name)
                    .as_os_str()
                    .as_encoded_bytes(),
            )
            .as_bytes(),
        );
        let Ok(mut candidate) = open_directory_entry(&directory, &name) else {
            continue;
        };
        let Ok(metadata) = candidate.metadata() else {
            continue;
        };
        if !metadata.file_type().is_file() {
            continue;
        }
        let identity = (metadata.dev(), metadata.ino());
        let Ok(state) = read_companion_state(&mut candidate) else {
            continue;
        };
        let Some(record) = state.active_record else {
            continue;
        };
        if record.fields.channel_address != recorded_address {
            continue;
        }
        drop(candidate);
        if unlink_verified_entry(&directory, &name, identity)? {
            removed = true;
        }
    }
    if removed {
        sync_database_parent(path)?;
    }
    Ok(())
}

/// Without `unlinkat`, a companion proven by a descriptor cannot be removed by
/// that identity, only by a name another process could have re-pointed. The
/// orphaned companion is therefore LEFT in place rather than removed on a
/// pathname's word.
#[cfg(not(unix))]
fn remove_installed_replacement_companion(_path: &Path) -> Result<()> {
    Ok(())
}

/// What to do with the store a standing replacement intent names when the
/// surviving generation is the prior one.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum AbandonedTarget {
    /// Establishing ownership: the recorded target is residue and goes.
    Remove,
    /// Re-recording: the recorded target is the live replacement and stays.
    Retain,
}

/// Why a companion inode is being opened. The variants differ only in what an
/// occupant of the companion pathname is allowed to be: an ordinary open
/// demands a recoverable generation, while a genuinely new store's companion
/// pathname can only be holding residue of a store that no longer exists.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum CompanionAdmission {
    /// The companion must already exist and must carry a recoverable
    /// generation.
    Existing,
    /// Create the companion when it is missing; an existing one must still
    /// carry a recoverable generation.
    CreateIfMissing,
    /// The store itself is being created at this pathname, so anything
    /// already occupying the companion pathname described a store that is
    /// gone. It is re-initialized in place under this exclusive lock instead
    /// of making the pathname permanently unusable.
    NewStore,
}

impl CompanionAdmission {
    const fn may_create(self) -> bool {
        !matches!(self, Self::Existing)
    }

    const fn reinitializes_residue(self) -> bool {
        matches!(self, Self::NewStore)
    }
}

impl CompanionGuard {
    fn acquire(path: &Path, admission: CompanionAdmission) -> Result<Self> {
        Self::acquire_inner(path, admission, true)
    }

    fn acquire_inner(path: &Path, admission: CompanionAdmission, may_create: bool) -> Result<Self> {
        let companion_path = companion_path(path);
        let exists = companion_exists(path)?;
        let create = !exists && may_create && admission.may_create();
        if !exists && !create {
            return Err(Error::Other(format!(
                "store companion does not exist: {}",
                companion_path.display()
            )));
        }
        let mut options = OpenOptions::new();
        options.read(true).write(true).truncate(false);
        if create {
            options.create_new(true);
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options
                .mode(0o600)
                .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
        }
        let mut file = match options.open(&companion_path) {
            Ok(file) => file,
            Err(error) if create && error.kind() == std::io::ErrorKind::AlreadyExists => {
                return Self::acquire_inner(path, admission, false);
            }
            Err(error) => return Err(RedbPersistence::storage_error(error)),
        };
        // A created companion is owner-only from its first byte (the `mode`
        // above), never by a chmod that would leave a readable window. An
        // inode that already existed is repaired to the same invariant --
        // only when it is one regular file this user owns, so nothing another
        // user could have planted is ever adopted -- because refusing it
        // would make an ordinary pathname permanently unopenable.
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt as _;
            use std::os::unix::fs::PermissionsExt as _;
            let opened = file.metadata().map_err(RedbPersistence::storage_error)?;
            if opened.file_type().is_file()
                && opened.uid() == nix::unistd::Uid::effective().as_raw()
                && opened.nlink() == 1
                && opened.mode() & 0o7777 != 0o600
            {
                file.set_permissions(std::fs::Permissions::from_mode(0o600))
                    .map_err(RedbPersistence::storage_error)?;
            }
            if create {
                file.sync_all().map_err(RedbPersistence::storage_error)?;
            }
        }
        let before_lock = file.metadata().map_err(RedbPersistence::storage_error)?;
        validate_companion_metadata(&companion_path, &file, &before_lock)?;
        if !try_lock_exclusive(&file)? {
            return Err(held_by_writer_error(path, &mut file));
        }
        if let Err(error) = validate_companion_metadata(&companion_path, &file, &before_lock) {
            unlock_file(&file);
            return Err(error);
        }
        // This IS the moment the store becomes owned. Everything a reader can
        // dial is published later, so between here and that publication a
        // process genuinely holds the store and nothing published says so --
        // and a caller told "nobody owns this" there goes off to create and
        // take a store somebody else is already holding. Holding the
        // claim-window byte from here is what makes that stretch answerable.
        // Taking it never blocks and never fails the claim: a window that
        // could not be held simply costs a later caller its declared budget.
        let _ = take_companion_range_lock_exclusive(&file, COMPANION_CLAIM_WINDOW_LOCK_OFFSET);
        crate::read_session::note_writer_open_event(
            crate::read_session::ReadSessionEvent::CompanionClaimTaken,
        );
        if create && let Err(error) = sync_database_parent(&companion_path) {
            unlock_file(&file);
            return Err(error);
        }
        let companion = match read_companion_state(&mut file) {
            Ok(companion) => companion,
            // The store is being created here, so an unreadable companion is
            // residue rather than this store's lost generation.
            // `publish_writer_run` rewrites the whole fixed layout.
            Err(_) if admission.reinitializes_residue() => CompanionState {
                needs_initialization: true,
                active_slot: None,
                active_record: None,
                pending_slot: None,
                pending_replacement: None,
                pending_matches_store: None,
            },
            Err(error) => {
                unlock_file(&file);
                return Err(Error::StoreCorrupted {
                    path: companion_path.display().to_string(),
                    reason: format!("companion cannot be recovered: {error}"),
                });
            }
        };
        let pending_matches_store = companion
            .pending_replacement
            .as_ref()
            .map(|pending| {
                store_file_fingerprint(path)
                    .map(|fingerprint| fingerprint == pending.store_fingerprint)
            })
            .transpose()?;
        let mut companion = companion;
        companion.pending_matches_store = pending_matches_store;
        Ok(Self {
            path: companion_path,
            state: Mutex::new(CompanionGuardState { file, companion }),
        })
    }

    /// Take back a companion this open created for itself, because the open
    /// is being refused.
    ///
    /// On the road where a store has no companion yet, the claim is taken
    /// before the root has been proven readable and current -- it has to be,
    /// because the writer already owns the store from that moment and a caller
    /// arriving there must not be told the store is free. The other half of
    /// that promise is here: an open that is then refused, because the file is
    /// a legacy layout or a corrupt root or not a store at all, leaves the
    /// directory exactly as it found it. An operator who points a writer at
    /// the wrong file is told so and is not left with a companion sitting
    /// beside it.
    ///
    /// The removal happens while the companion is still held exclusively, so
    /// no other opener can adopt the inode in the gap, and only while the
    /// pathname still names the file this guard is holding.
    ///
    /// A process that dies part-way through such an open leaves the companion
    /// behind, exactly as a process that dies part-way through creating a new
    /// store does today. That residue is what the next opener's
    /// residue-reinitializing admission is for; it is not a new condition.
    fn discard_created(self, database_path: &Path) {
        let companion = companion_path(database_path);
        let Ok(state) = self.lock_state() else {
            return;
        };
        if companion_still_names(&companion, &state.file) {
            let _ = std::fs::remove_file(&companion);
        }
    }

    fn lock_state(&self) -> Result<std::sync::MutexGuard<'_, CompanionGuardState>> {
        self.state.lock().map_err(|_| Error::StoreCorrupted {
            path: self.path.display().to_string(),
            reason: "companion guard state was poisoned by an interrupted operation".to_owned(),
        })
    }

    fn finish_pending_replacement(
        state: &mut CompanionGuardState,
    ) -> Result<EncodedCompanionRecord> {
        let pending_slot = state
            .companion
            .pending_slot
            .ok_or_else(|| Error::Other("pending replacement has no companion slot".to_owned()))?;
        let pending = state
            .companion
            .pending_replacement
            .clone()
            .ok_or_else(|| Error::Other("pending replacement record is absent".to_owned()))?;
        let record_slot = 1 - pending_slot;
        write_companion_record(&mut state.file, record_slot, &pending.intended_record)
            .map_err(RedbPersistence::storage_error)?;
        write_companion_record_copy(&mut state.file, pending_slot, &pending.intended_record)
            .map_err(RedbPersistence::storage_error)?;
        state.companion = CompanionState {
            needs_initialization: false,
            active_slot: Some(record_slot),
            active_record: Some(pending.intended_record.clone()),
            pending_slot: None,
            pending_replacement: None,
            pending_matches_store: None,
        };
        Ok(pending.intended_record)
    }

    /// Resolve a recorded intent while establishing ownership of the store.
    /// A recorded target the surviving generation did not adopt is residue,
    /// and this is the moment it is removed.
    fn resolve_pending_replacement(path: &Path, state: &mut CompanionGuardState) -> Result<()> {
        Self::resolve_pending_replacement_inner(path, state, AbandonedTarget::Remove)
    }

    /// Resolve a recorded intent while a replacement is being re-recorded.
    /// The target named by the standing record is the one about to be named
    /// again, so removing it here would delete the live replacement.
    fn retain_pending_replacement(path: &Path, state: &mut CompanionGuardState) -> Result<()> {
        Self::resolve_pending_replacement_inner(path, state, AbandonedTarget::Retain)
    }

    /// Resolve a recorded intent against the store exactly as it stands,
    /// without publishing a writer run. The recorded intent names its
    /// generation by a fingerprint of that generation's bytes, so anything
    /// that rewrites the store -- redb's own crash repair above all -- must
    /// let this resolution complete FIRST, while the fingerprint still
    /// selects a generation.
    /// A companion with nothing recorded is left byte-for-byte untouched.
    fn settle_pending_replacement(&self, path: &Path) -> Result<()> {
        let mut state = self.lock_state()?;
        if state.companion.pending_replacement.is_none() {
            return Ok(());
        }
        Self::resolve_pending_replacement(path, &mut state)
    }

    fn resolve_pending_replacement_inner(
        path: &Path,
        state: &mut CompanionGuardState,
        abandoned: AbandonedTarget,
    ) -> Result<()> {
        if state.companion.pending_replacement.is_none() {
            return Ok(());
        }
        match state.companion.pending_matches_store {
            Some(true) => {
                Self::finish_pending_replacement(state)?;
                // The recorded generation is the store standing here, so the
                // companion it was generated with is orphaned at the name it
                // was renamed away from. Removing it while this guard owns
                // the store is what keeps an interrupted replacement from
                // leaving an operator artifact, exactly as the abandoned
                // branch below removes the target it recorded.
                if abandoned == AbandonedTarget::Remove {
                    remove_installed_replacement_companion(path)?;
                }
                Ok(())
            }
            Some(false) if state.companion.active_record.is_some() => {
                // The prior generation is the surviving store, so the target
                // this companion recorded is abandoned. Removing it here --
                // while this guard still owns the store -- is what keeps a
                // crashed replacement from leaving an operator artifact.
                let recorded = state
                    .companion
                    .pending_replacement
                    .as_ref()
                    .map(|pending| pending.store_fingerprint);
                if let (AbandonedTarget::Remove, Some(fingerprint)) = (abandoned, recorded) {
                    remove_abandoned_replacement_target(path, &fingerprint)?;
                }
                Ok(())
            }
            Some(false) => Err(Error::StoreCorrupted {
                path: path.display().to_string(),
                reason: "pending replacement does not identify this store and no prior companion generation survives"
                    .to_owned(),
            }),
            None => Err(Error::StoreCorrupted {
                path: path.display().to_string(),
                reason: "pending replacement could not be matched to the store".to_owned(),
            }),
        }
    }

    fn publish_writer_run(&self, path: &Path, new_database_identity: bool) -> Result<()> {
        let mut state = self.lock_state()?;
        if state.companion.needs_initialization {
            state.companion = initialize_companion_inode(&mut state.file)
                .map_err(RedbPersistence::storage_error)?;
        }
        Self::resolve_pending_replacement(path, &mut state)?;
        let record = companion_writer_record(
            path,
            state.companion.active_record.as_ref(),
            new_database_identity,
        )?;
        let active_slot = state.companion.active_slot.unwrap_or(1);
        let inactive_slot = 1 - active_slot;
        write_companion_record(&mut state.file, inactive_slot, &record)
            .map_err(RedbPersistence::storage_error)?;
        state.companion = CompanionState {
            needs_initialization: false,
            active_slot: Some(inactive_slot),
            active_record: Some(record),
            pending_slot: None,
            pending_replacement: None,
            pending_matches_store: None,
        };
        // The claim-window byte is already held: this writer took it when it
        // first claimed the companion, which is when it began to own the
        // store. The run published here still says nothing about serving, so
        // the window stays open until the decision exists.
        Ok(())
    }

    /// Re-record what the writer holding this store decided about serving
    /// inspection, keeping every other published fact exactly as it is.
    ///
    /// The record a writer publishes at open is written BEFORE it knows
    /// whether its channel came up, so the state has to be re-recorded once
    /// the answer exists and again whenever it changes. It is the one thing a
    /// later reader has to go on, so a reader that cannot reach a channel is
    /// told the writer's own word rather than the storage layer's complaint.
    /// The writer run number and channel address are untouched: this is the
    /// same run saying more about itself, never a new one.
    /// End this writer's claim window because what it decided can now be seen
    /// by a caller asking about the store.
    ///
    /// A writer whose channel is up has already answered the only question the
    /// window exists for: the caller dials it and is told, by the writer
    /// itself, that it is serving. Waiting for the record to be written before
    /// letting that caller go would keep it asleep past the moment the answer
    /// existed. A writer that is NOT serving has no channel to be asked, so its
    /// window closes where its decision becomes durable instead.
    fn close_claim_window(&self) {
        let Ok(state) = self.lock_state() else {
            return;
        };
        release_companion_range_lock(&state.file, COMPANION_CLAIM_WINDOW_LOCK_OFFSET);
    }

    fn record_owner_read_status(&self, path: &Path, status: OwnerReadStatus) -> Result<()> {
        let mut state = self.lock_state()?;
        let recorded = Self::publish_decided_owner_read_status(path, &mut state, status);
        // This writer has decided, and whatever it decided is now beside the
        // store, so its claim window is over. Releasing only here is what makes
        // the wake honest: a caller sleeping on this byte wakes to a record
        // that already says what this writer decided, never to the window it
        // was waiting out.
        release_companion_range_lock(&state.file, COMPANION_CLAIM_WINDOW_LOCK_OFFSET);
        recorded
    }

    fn publish_decided_owner_read_status(
        path: &Path,
        state: &mut CompanionGuardState,
        status: OwnerReadStatus,
    ) -> Result<()> {
        let Some(previous) = state.companion.active_record.clone() else {
            return Ok(());
        };
        let status = bounded_companion_status(status);
        if previous.fields.owner_read_status == status {
            return Ok(());
        }
        Self::resolve_pending_replacement(path, state)?;
        let Some(previous) = state.companion.active_record.clone() else {
            return Ok(());
        };
        let mut fields = previous.fields.clone();
        fields.owner_read_status = status;
        fields.generation = fields
            .generation
            .checked_add(1)
            .ok_or_else(|| Error::Other("companion generation overflow".to_owned()))?;
        let record = prepare_companion_record_for_publication(&fields)
            .map_err(|error| Error::Other(format!("encode companion record: {error}")))?;
        let active_slot = state.companion.active_slot.unwrap_or(1);
        let inactive_slot = 1 - active_slot;
        write_companion_record(&mut state.file, inactive_slot, &record)
            .map_err(RedbPersistence::storage_error)?;
        state.companion.active_slot = Some(inactive_slot);
        state.companion.active_record = Some(record);
        Ok(())
    }

    fn prepare_replacement_database(&self, path: &Path, replacement_path: &Path) -> Result<()> {
        let fingerprint = store_file_fingerprint(replacement_path)?;
        let mut state = self.lock_state()?;
        if state.companion.needs_initialization {
            state.companion = initialize_companion_inode(&mut state.file)
                .map_err(RedbPersistence::storage_error)?;
        }
        Self::retain_pending_replacement(path, &mut state)?;
        let record = companion_writer_record(path, state.companion.active_record.as_ref(), true)?;
        let pending = encode_pending_replacement(fingerprint, record)
            .map_err(RedbPersistence::storage_error)?;
        let active_slot = state
            .companion
            .active_slot
            .ok_or_else(|| Error::StoreCorrupted {
                path: companion_path(path).display().to_string(),
                reason: "replacement requires a published prior companion generation".to_owned(),
            })?;
        let pending_slot = 1 - active_slot;
        write_companion_pending_slot(&mut state.file, pending_slot, &pending)
            .map_err(RedbPersistence::storage_error)?;
        state.companion.pending_slot = Some(pending_slot);
        state.companion.pending_replacement = Some(pending);
        state.companion.pending_matches_store = Some(false);
        Ok(())
    }

    fn publish_replacement_database(&self, path: &Path) -> Result<()> {
        let mut state = self.lock_state()?;
        let pending = state
            .companion
            .pending_replacement
            .as_ref()
            .ok_or_else(|| Error::Other("replacement was not prepared".to_owned()))?;
        let fingerprint = store_file_fingerprint(path)?;
        if fingerprint != pending.store_fingerprint {
            return Err(Error::StoreCorrupted {
                path: path.display().to_string(),
                reason: "replacement path does not match the checksummed pending database identity"
                    .to_owned(),
            });
        }
        state.companion.pending_matches_store = Some(true);
        Self::finish_pending_replacement(&mut state)?;
        Ok(())
    }

    fn publish_replacement_if_current(&self, path: &Path) -> Result<()> {
        let mut state = self.lock_state()?;
        let Some(pending) = state.companion.pending_replacement.as_ref() else {
            return Ok(());
        };
        let fingerprint = store_file_fingerprint(path)?;
        if fingerprint != pending.store_fingerprint {
            // The prepared temporary database was not installed. Preserve
            // the old complete generation; a later writer publication will
            // safely overwrite the inactive pending slot.
            return Ok(());
        }
        state.companion.pending_matches_store = Some(true);
        Self::finish_pending_replacement(&mut state)?;
        Ok(())
    }
}

impl Drop for CompanionGuard {
    fn drop(&mut self) {
        if let Ok(state) = self.state.lock() {
            unlock_file(&state.file);
        }
    }
}

fn write_companion_region(file: &mut File, offset: u64, bytes: &[u8]) -> std::io::Result<()> {
    file.seek(SeekFrom::Start(offset))?;
    file.write_all(bytes)
}

/// Durable companion updates use this primitive for the inactive slot's
/// payload. The crash checkpoint cannot be moved away from the write without
/// replacing the operation the companion writer is required to call.
#[allow(dead_code)]
fn write_companion_slot_payload(
    file: &mut File,
    offset: u64,
    payload: &[u8],
) -> std::io::Result<()> {
    write_companion_region(file, offset, payload)?;
    #[cfg(feature = "test-seams")]
    read_persistence_test_scaffold::companion_write_checkpoint(
        read_persistence_test_scaffold::CompanionWriteBoundary::SlotPayloadWrite,
    );
    Ok(())
}

#[allow(dead_code)]
fn sync_companion_slot_payload(file: &File) -> std::io::Result<()> {
    file.sync_data()?;
    #[cfg(feature = "test-seams")]
    read_persistence_test_scaffold::companion_write_checkpoint(
        read_persistence_test_scaffold::CompanionWriteBoundary::SlotPayloadSync,
    );
    Ok(())
}

#[allow(dead_code)]
fn write_companion_slot_checksum(
    file: &mut File,
    offset: u64,
    checksum: &[u8; 32],
) -> std::io::Result<()> {
    write_companion_region(file, offset, checksum)?;
    #[cfg(feature = "test-seams")]
    read_persistence_test_scaffold::companion_write_checkpoint(
        read_persistence_test_scaffold::CompanionWriteBoundary::SlotChecksumWrite,
    );
    Ok(())
}

#[allow(dead_code)]
fn sync_companion_slot_checksum(file: &File) -> std::io::Result<()> {
    file.sync_data()?;
    #[cfg(feature = "test-seams")]
    read_persistence_test_scaffold::companion_write_checkpoint(
        read_persistence_test_scaffold::CompanionWriteBoundary::SlotChecksumSync,
    );
    Ok(())
}

#[allow(dead_code)]
fn write_companion_active_slot(
    file: &mut File,
    offset: u64,
    active_slot: &[u8],
) -> std::io::Result<()> {
    write_companion_region(file, offset, active_slot)?;
    #[cfg(feature = "test-seams")]
    read_persistence_test_scaffold::companion_write_checkpoint(
        read_persistence_test_scaffold::CompanionWriteBoundary::ActiveSlotWrite,
    );
    Ok(())
}

#[allow(dead_code)]
fn sync_companion_active_slot(file: &File) -> std::io::Result<()> {
    file.sync_data()?;
    #[cfg(feature = "test-seams")]
    read_persistence_test_scaffold::companion_write_checkpoint(
        read_persistence_test_scaffold::CompanionWriteBoundary::ActiveSlotSync,
    );
    Ok(())
}

fn write_companion_record(
    file: &mut File,
    slot: usize,
    record: &EncodedCompanionRecord,
) -> std::io::Result<()> {
    #[cfg(feature = "test-seams")]
    crate::read_probe::note_persistence_companion_mutation();
    let slot_offset = companion_slot_offset(slot)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?;
    let payload_length = u32::try_from(record.checksummed_payload.len()).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "companion payload is too large",
        )
    })?;
    if record.checksummed_payload.len() > COMPANION_SLOT_PAYLOAD_CAPACITY {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "companion payload exceeds its fixed slot",
        ));
    }
    let mut slot_payload = vec![0_u8; 4 + COMPANION_SLOT_PAYLOAD_CAPACITY];
    slot_payload[..4].copy_from_slice(&payload_length.to_le_bytes());
    slot_payload[4..4 + record.checksummed_payload.len()]
        .copy_from_slice(&record.checksummed_payload);
    write_companion_slot_payload(file, slot_offset as u64, &slot_payload)?;
    sync_companion_slot_payload(file)?;
    write_companion_slot_checksum(
        file,
        (slot_offset + 4 + COMPANION_SLOT_PAYLOAD_CAPACITY) as u64,
        &record.stored_checksum,
    )?;
    sync_companion_slot_checksum(file)?;
    let selector = companion_selector_bytes(slot, record.fields.generation)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?;
    write_companion_active_slot(file, COMPANION_SELECTOR_OFFSET as u64, &selector)?;
    sync_companion_active_slot(file)
}

fn write_companion_slot_without_selector(
    file: &mut File,
    slot: usize,
    payload: &[u8],
    checksum: &[u8; 32],
) -> std::io::Result<()> {
    let slot_offset = companion_slot_offset(slot)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?;
    let payload_length = u32::try_from(payload.len()).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "companion payload is too large",
        )
    })?;
    if payload.len() > COMPANION_SLOT_PAYLOAD_CAPACITY {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "companion payload exceeds its fixed slot",
        ));
    }
    let mut slot_payload = vec![0_u8; 4 + COMPANION_SLOT_PAYLOAD_CAPACITY];
    slot_payload[..4].copy_from_slice(&payload_length.to_le_bytes());
    slot_payload[4..4 + payload.len()].copy_from_slice(payload);
    write_companion_region(file, slot_offset as u64, &slot_payload)?;
    file.sync_data()?;
    write_companion_region(
        file,
        (slot_offset + 4 + COMPANION_SLOT_PAYLOAD_CAPACITY) as u64,
        checksum,
    )?;
    file.sync_data()
}

fn write_companion_pending_slot(
    file: &mut File,
    slot: usize,
    pending: &PendingReplacement,
) -> std::io::Result<()> {
    write_companion_slot_without_selector(
        file,
        slot,
        &pending.checksummed_payload,
        &pending.stored_checksum,
    )
}

fn write_companion_record_copy(
    file: &mut File,
    slot: usize,
    record: &EncodedCompanionRecord,
) -> std::io::Result<()> {
    write_companion_slot_without_selector(
        file,
        slot,
        &record.checksummed_payload,
        &record.stored_checksum,
    )
}

/// Test-only observability for the read-persistence coordination contract.
///
/// The production companion implementation calls the checkpoint at each
/// durable write boundary while this feature is enabled. The test process
/// drives the real writer in a child process and turns one checkpoint into a
/// process abort, so recovery is always exercised against the bytes the
/// production writer had actually reached. This module never manufactures a
/// lock, reader, companion record, or recovery result.
#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub mod read_persistence_test_scaffold {
    use contextdb_core::read_contract::{
        ChannelAddress, DatabaseIdentity, LocalUserIdentity, OwnerReadStatus, ReaderBreadcrumb,
        WriterRunNumber,
    };
    use redb::{ReadableDatabase as _, ReadableTable as _, TableDefinition};
    use std::collections::{BTreeSet, VecDeque};
    use std::io::{BufRead as _, Write as _};
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Condvar, Mutex, OnceLock};

    /// Timeless companion codec choice: canonical resolved paths are hashed
    /// with BLAKE3 into the fixed 32-byte channel address.
    pub const COMPANION_CHANNEL_ADDRESS_CODEC: &str = "blake3-canonical-path-v1";

    /// Canonical companion payload fields. The byte codec is fixed as:
    /// `contextdb-companion`, little-endian format/generation, fixed identity
    /// arrays, little-endian user/process values, state and reason tags,
    /// little-endian reason length, then UTF-8 reason bytes. BLAKE3 covers the
    /// complete payload.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct CompanionRecordFields {
        pub format_version: u16,
        pub generation: u64,
        pub database_identity: DatabaseIdentity,
        pub writer_run_number: WriterRunNumber,
        pub owner_user: LocalUserIdentity,
        pub channel_address: ChannelAddress,
        pub process_id: u32,
        pub owner_read_status: OwnerReadStatus,
    }

    /// Every durable boundary of a dual-slot companion update.  The
    /// production writer must checkpoint after completing each named action,
    /// before starting the next one.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum CompanionWriteBoundary {
        SlotPayloadWrite,
        SlotPayloadSync,
        SlotChecksumWrite,
        SlotChecksumSync,
        ActiveSlotWrite,
        ActiveSlotSync,
    }

    impl CompanionWriteBoundary {
        pub const ALL: [Self; 6] = [
            Self::SlotPayloadWrite,
            Self::SlotPayloadSync,
            Self::SlotChecksumWrite,
            Self::SlotChecksumSync,
            Self::ActiveSlotWrite,
            Self::ActiveSlotSync,
        ];
    }

    /// A successfully decoded durable companion record.  Inspection returns
    /// only a checksummed, complete record; a torn slot must recover to one
    /// of these rather than exposing partial bytes to the test.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct CompanionRecordObservation {
        pub fields: CompanionRecordFields,
        /// The exact durable record payload covered by `stored_checksum`.
        pub checksummed_payload: Vec<u8>,
        pub stored_checksum: [u8; 32],
    }

    /// Read-only capture of the exact replacement state that exists before
    /// any recovery opener can resolve or overwrite it.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct ReplacementIntentObservation {
        pub active_record: Option<CompanionRecordObservation>,
        pub pending_intended_record: Option<CompanionRecordObservation>,
        pub pending_store_fingerprint: Option<[u8; 32]>,
        pub current_store_fingerprint: [u8; 32],
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum RawRedbWriterOpenObservation {
        Acquired,
        DatabaseAlreadyOpen,
        Other(String),
    }

    /// What Redb itself says when asked to open a store WITHOUT writing to it.
    ///
    /// `RepairAborted` is the answer that matters: a store whose last writer
    /// died with it open cannot be read at all until Redb's own crash repair
    /// runs, and that repair rewrites the file. Whether a root really is in
    /// that state is not something a proof can infer from the outside -- a
    /// healthy root and a crash-dirty one look alike from every angle that
    /// does not open them -- so a proof about what may or may not repair one
    /// has to be able to ask.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum RawRedbReadOnlyOpenObservation {
        Acquired,
        RepairAborted,
        Other(String),
    }

    fn companion_fields_for_production(fields: &CompanionRecordFields) -> super::CompanionRecord {
        super::CompanionRecord {
            format_version: fields.format_version,
            generation: fields.generation,
            database_identity: fields.database_identity,
            writer_run_number: fields.writer_run_number,
            owner_user: fields.owner_user,
            channel_address: fields.channel_address,
            process_id: fields.process_id,
            owner_read_status: fields.owner_read_status.clone(),
        }
    }

    fn companion_observation(record: super::EncodedCompanionRecord) -> CompanionRecordObservation {
        CompanionRecordObservation {
            fields: CompanionRecordFields {
                format_version: record.fields.format_version,
                generation: record.fields.generation,
                database_identity: record.fields.database_identity,
                writer_run_number: record.fields.writer_run_number,
                owner_user: record.fields.owner_user,
                channel_address: record.fields.channel_address,
                process_id: record.fields.process_id,
                owner_read_status: record.fields.owner_read_status,
            },
            checksummed_payload: record.checksummed_payload,
            stored_checksum: record.stored_checksum,
        }
    }

    pub fn encode_companion_record_for_test(
        fields: &CompanionRecordFields,
    ) -> std::result::Result<CompanionRecordObservation, String> {
        super::prepare_companion_record_for_publication(&companion_fields_for_production(fields))
            .map(companion_observation)
    }

    pub fn decode_companion_record_for_test(
        payload: &[u8],
        checksum: [u8; 32],
    ) -> std::result::Result<CompanionRecordObservation, String> {
        super::recover_companion_record(payload, checksum).map(companion_observation)
    }

    /// Checkpoints in the destructive store replacement while the original
    /// companion guard is required to remain continuously owned.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum StoreReplacementBoundary {
        GuardAcquired,
        BeforeAtomicReplacement,
        AfterAtomicReplacement,
        ReplacementPublishedBeforeGuardRelease,
    }

    impl StoreReplacementBoundary {
        pub const ALL: [Self; 4] = [
            Self::GuardAcquired,
            Self::BeforeAtomicReplacement,
            Self::AfterAtomicReplacement,
            Self::ReplacementPublishedBeforeGuardRelease,
        ];
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum StoreReplacementEvent {
        Checkpoint(StoreReplacementBoundary),
        CompletedAfterGuardRelease,
        FinishedBeforeExpectedCheckpoint {
            expected: StoreReplacementBoundary,
        },
        UnexpectedCheckpoint {
            expected: StoreReplacementBoundary,
            actual: StoreReplacementBoundary,
        },
    }

    #[derive(Debug, Default)]
    struct StoreReplacementState {
        armed: bool,
        expected: VecDeque<StoreReplacementBoundary>,
        reached: Option<StoreReplacementBoundary>,
        released: bool,
        finished: bool,
        unexpected: Option<(StoreReplacementBoundary, StoreReplacementBoundary)>,
    }

    static WRITER_OPEN_ATTEMPTS: AtomicUsize = AtomicUsize::new(0);
    static COMPANION_EFFECTIVE_USER_SOURCE_CALLS: AtomicUsize = AtomicUsize::new(0);
    static ARMED_COMPANION_CRASH: Mutex<Option<CompanionWriteBoundary>> = Mutex::new(None);
    static COMPANION_EFFECTIVE_USER_OVERRIDE: Mutex<Option<LocalUserIdentity>> = Mutex::new(None);
    static READ_IMAGE_HYDRATION_PAUSE: AtomicBool = AtomicBool::new(false);
    static READ_IMAGE_SOURCE_EVENTS: Mutex<Vec<ReadImageSourceEvent>> = Mutex::new(Vec::new());
    static STORE_REPLACEMENT: OnceLock<(Mutex<StoreReplacementState>, Condvar)> = OnceLock::new();
    static TWO_READ_IMAGE_HYDRATIONS: OnceLock<(Mutex<TwoReadImageHydrationState>, Condvar)> =
        OnceLock::new();

    #[derive(Debug, Default)]
    struct TwoReadImageHydrationState {
        next_token: u64,
        active: Option<TwoReadImageHydrationRun>,
    }

    #[derive(Debug)]
    struct TwoReadImageHydrationRun {
        token: u64,
        next_participant: u64,
        started_attempts: usize,
        finished_attempts: usize,
        reached: Vec<ReadImageHydrationParticipantObservation>,
        released: BTreeSet<u64>,
        departed: BTreeSet<u64>,
    }

    fn two_read_image_hydrations() -> &'static (Mutex<TwoReadImageHydrationState>, Condvar) {
        TWO_READ_IMAGE_HYDRATIONS.get_or_init(|| {
            (
                Mutex::new(TwoReadImageHydrationState::default()),
                Condvar::new(),
            )
        })
    }

    fn store_replacement() -> &'static (Mutex<StoreReplacementState>, Condvar) {
        STORE_REPLACEMENT
            .get_or_init(|| (Mutex::new(StoreReplacementState::default()), Condvar::new()))
    }

    pub fn reset_writer_open_attempts_for_test() {
        WRITER_OPEN_ATTEMPTS.store(0, Ordering::SeqCst);
    }

    pub fn writer_open_attempts_for_test() -> usize {
        WRITER_OPEN_ATTEMPTS.load(Ordering::SeqCst)
    }

    pub fn set_companion_effective_user_identity_for_test(identity: LocalUserIdentity) {
        *COMPANION_EFFECTIVE_USER_OVERRIDE
            .lock()
            .expect("companion effective-user override mutex poisoned") = Some(identity);
    }

    pub fn clear_companion_effective_user_identity_for_test() {
        *COMPANION_EFFECTIVE_USER_OVERRIDE
            .lock()
            .expect("companion effective-user override mutex poisoned") = None;
    }

    pub fn companion_effective_user_identity_for_test() -> LocalUserIdentity {
        super::companion_effective_user_identity()
    }

    pub fn reset_companion_effective_user_source_calls_for_test() {
        COMPANION_EFFECTIVE_USER_SOURCE_CALLS.store(0, Ordering::SeqCst);
    }

    pub fn companion_effective_user_source_calls_for_test() -> usize {
        COMPANION_EFFECTIVE_USER_SOURCE_CALLS.load(Ordering::SeqCst)
    }

    pub(super) fn note_companion_effective_user_source_call_for_test() {
        COMPANION_EFFECTIVE_USER_SOURCE_CALLS.fetch_add(1, Ordering::SeqCst);
    }

    pub(super) fn companion_effective_user_override_for_test() -> Option<LocalUserIdentity> {
        *COMPANION_EFFECTIVE_USER_OVERRIDE
            .lock()
            .expect("companion effective-user override mutex poisoned")
    }

    pub(crate) fn note_writer_open_attempt_for_test() {
        WRITER_OPEN_ATTEMPTS.fetch_add(1, Ordering::SeqCst);
    }

    pub fn arm_companion_crash_for_test(boundary: CompanionWriteBoundary) {
        *ARMED_COMPANION_CRASH
            .lock()
            .expect("companion crash seam mutex poisoned") = Some(boundary);
    }

    pub(super) fn companion_write_checkpoint(boundary: CompanionWriteBoundary) {
        let armed = ARMED_COMPANION_CRASH
            .lock()
            .expect("companion crash seam mutex poisoned")
            .take();
        if armed == Some(boundary) {
            let stderr = std::io::stderr();
            let mut stderr = stderr.lock();
            writeln!(stderr, "COMPANION_CRASH_AFTER={boundary:?}")
                .expect("write companion crash boundary marker");
            stderr
                .flush()
                .expect("flush companion crash boundary marker");
            std::process::abort();
        }
        if let Some(armed) = armed {
            *ARMED_COMPANION_CRASH
                .lock()
                .expect("companion crash seam mutex poisoned") = Some(armed);
        }
    }

    fn encode_hex(bytes: &[u8]) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut encoded = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            encoded.push(HEX[(byte >> 4) as usize] as char);
            encoded.push(HEX[(byte & 0x0f) as usize] as char);
        }
        encoded
    }

    pub(super) fn companion_update_planned_for_test(record: &super::EncodedCompanionRecord) {
        if ARMED_COMPANION_CRASH
            .lock()
            .expect("companion crash seam mutex poisoned")
            .is_none()
        {
            return;
        }
        let stderr = std::io::stderr();
        let mut stderr = stderr.lock();
        writeln!(
            stderr,
            "COMPANION_PLANNED_RECORD={}:{}",
            encode_hex(&record.checksummed_payload),
            encode_hex(&record.stored_checksum)
        )
        .expect("write planned companion record marker");
        stderr
            .flush()
            .expect("flush planned companion record marker");
    }

    pub fn arm_store_replacement_sequence_for_test() {
        let (state, _) = store_replacement();
        let mut state = state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert!(!state.armed, "store replacement sequence is already armed");
        *state = StoreReplacementState {
            armed: true,
            expected: StoreReplacementBoundary::ALL.into_iter().collect(),
            reached: None,
            released: false,
            finished: false,
            unexpected: None,
        };
    }

    pub fn next_store_replacement_event_for_test() -> StoreReplacementEvent {
        let (state, changed) = store_replacement();
        let mut state = state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        loop {
            if let Some((expected, actual)) = state.unexpected.take() {
                *state = StoreReplacementState::default();
                changed.notify_all();
                return StoreReplacementEvent::UnexpectedCheckpoint { expected, actual };
            }
            if let Some(boundary) = state.reached {
                return StoreReplacementEvent::Checkpoint(boundary);
            }
            if state.finished {
                let event = match state.expected.front().copied() {
                    Some(expected) => {
                        StoreReplacementEvent::FinishedBeforeExpectedCheckpoint { expected }
                    }
                    None => StoreReplacementEvent::CompletedAfterGuardRelease,
                };
                *state = StoreReplacementState::default();
                changed.notify_all();
                return event;
            }
            state = changed
                .wait(state)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }

    pub fn release_store_replacement_checkpoint_for_test() {
        let (state, changed) = store_replacement();
        let mut state = state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let reached = state
            .reached
            .take()
            .expect("store replacement checkpoint was not reached");
        assert_eq!(state.expected.pop_front(), Some(reached));
        state.released = true;
        changed.notify_all();
    }

    pub fn finish_store_replacement_sequence_for_test() {
        let (state, changed) = store_replacement();
        let mut state = state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.armed {
            state.finished = true;
            changed.notify_all();
        }
    }

    pub fn cancel_store_replacement_sequence_for_test() {
        let (state, changed) = store_replacement();
        let mut state = state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *state = StoreReplacementState::default();
        changed.notify_all();
    }

    pub(crate) fn store_replacement_checkpoint_for_test(boundary: StoreReplacementBoundary) {
        let (state, changed) = store_replacement();
        let mut state = state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !state.armed {
            return;
        }
        let Some(expected) = state.expected.front().copied() else {
            return;
        };
        if expected != boundary {
            state.unexpected = Some((expected, boundary));
            changed.notify_all();
            return;
        }
        state.reached = Some(boundary);
        state.released = false;
        changed.notify_all();
        while state.armed && !state.released {
            state = changed
                .wait(state)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
        state.released = false;
    }

    #[derive(Debug, Clone, PartialEq)]
    pub struct ReadImageObservation {
        pub vectors: std::collections::BTreeMap<(String, i64), Vec<f32>>,
        pub released_breadcrumb_path: Option<PathBuf>,
        pub source_accesses: u64,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum ReadImageSourceEvent {
        Started { breadcrumb_path: Option<PathBuf> },
        SourceAccess,
        SourceHandlesDropped,
        Released { breadcrumb_path: Option<PathBuf> },
    }

    pub fn reset_read_image_source_events_for_test() {
        READ_IMAGE_SOURCE_EVENTS
            .lock()
            .expect("read-image source event mutex poisoned")
            .clear();
    }

    pub fn read_image_source_events_for_test() -> Vec<ReadImageSourceEvent> {
        READ_IMAGE_SOURCE_EVENTS
            .lock()
            .expect("read-image source event mutex poisoned")
            .clone()
    }

    pub(super) fn note_read_image_source_start_for_test(breadcrumb_path: Option<&Path>) {
        READ_IMAGE_SOURCE_EVENTS
            .lock()
            .expect("read-image source event mutex poisoned")
            .push(ReadImageSourceEvent::Started {
                breadcrumb_path: breadcrumb_path.map(Path::to_path_buf),
            });
    }

    pub(super) fn note_read_image_source_access_for_test() {
        READ_IMAGE_SOURCE_EVENTS
            .lock()
            .expect("read-image source event mutex poisoned")
            .push(ReadImageSourceEvent::SourceAccess);
    }

    pub(super) fn note_read_image_source_handles_dropped_for_test() {
        READ_IMAGE_SOURCE_EVENTS
            .lock()
            .expect("read-image source event mutex poisoned")
            .push(ReadImageSourceEvent::SourceHandlesDropped);
    }

    pub(super) fn note_read_image_source_release_for_test(breadcrumb_path: Option<&Path>) {
        READ_IMAGE_SOURCE_EVENTS
            .lock()
            .expect("read-image source event mutex poisoned")
            .push(ReadImageSourceEvent::Released {
                breadcrumb_path: breadcrumb_path.map(Path::to_path_buf),
            });
    }

    #[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
    pub enum LoadReadImageAdapterError {
        #[error("load_read_image production path is not implemented")]
        NotImplemented,
        #[error("read-image source failed: {0}")]
        Source(String),
        #[error("read-image release proof failed: {0}")]
        Release(String),
        #[error("requested vector is absent from the committed read image: {table}/{id}")]
        VectorNotFound { table: String, id: i64 },
    }

    /// Thin adapter over the sole `load_read_image` production path.
    ///
    /// It takes no runtime directory, because a reader's breadcrumb goes in
    /// the default per-user runtime location and nothing a caller passes moves
    /// it. A proof that wants that location somewhere else sets it for the
    /// whole process, the way a deployed reader receives it.
    pub fn load_read_image_for_test_adapter(
        path: &Path,
    ) -> std::result::Result<ReadImageObservation, LoadReadImageAdapterError> {
        match super::load_read_image(path) {
            Ok(load) => {
                let (image, receipt) = load.into_parts();
                receipt.validate_released().map_err(|error| match error {
                    super::LoadReadImageError::Release(error) => {
                        LoadReadImageAdapterError::Release(error)
                    }
                    other => LoadReadImageAdapterError::Release(other.to_string()),
                })?;
                Ok(ReadImageObservation {
                    vectors: image.vectors,
                    released_breadcrumb_path: receipt.breadcrumb_path().map(Path::to_path_buf),
                    source_accesses: receipt.source_accesses(),
                })
            }
            Err(super::LoadReadImageError::NotImplemented) => {
                Err(LoadReadImageAdapterError::NotImplemented)
            }
            Err(super::LoadReadImageError::Source(error)) => {
                Err(LoadReadImageAdapterError::Source(error))
            }
            Err(super::LoadReadImageError::Release(error)) => {
                Err(LoadReadImageAdapterError::Release(error))
            }
            // Compatibility adaptation: this persistence-only proof adapter
            // exposes ordinary load failures as its established source string,
            // including typed categories it does not consume.
            Err(other) => Err(LoadReadImageAdapterError::Source(other.to_string())),
        }
    }

    pub fn load_read_image_vector_for_test(
        path: &Path,
        table: &str,
        id: i64,
    ) -> std::result::Result<Vec<f32>, LoadReadImageAdapterError> {
        let image = load_read_image_for_test_adapter(path)?;
        image
            .vectors
            .get(&(table.to_owned(), id))
            .cloned()
            .ok_or_else(|| LoadReadImageAdapterError::VectorNotFound {
                table: table.to_owned(),
                id,
            })
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct ReadImageHydrationParticipantObservation {
        pub participant: u64,
        pub breadcrumb_path: Option<PathBuf>,
        pub breadcrumb: Option<ReaderBreadcrumb>,
    }

    /// One process-local controller for exactly two production hydration
    /// intervals. Each interval blocks at the real post-open/pre-decode
    /// checkpoint until its own participant is released. Dropping the
    /// controller cancels every waiter so a failed assertion cannot strand
    /// a read-only Redb handle or breadcrumb.
    #[derive(Debug)]
    pub struct TwoReadImageHydrationController {
        token: u64,
        next_observation: usize,
        finished: bool,
    }

    impl TwoReadImageHydrationController {
        pub fn next_participant_for_test(&mut self) -> ReadImageHydrationParticipantObservation {
            let (state, changed) = two_read_image_hydrations();
            let mut state = state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            loop {
                let run = state
                    .active
                    .as_ref()
                    .filter(|run| run.token == self.token)
                    .expect("two-hydration controller was cancelled");
                if let Some(observation) = run.reached.get(self.next_observation).cloned() {
                    self.next_observation += 1;
                    return observation;
                }
                assert!(
                    !(run.finished_attempts > 0 && run.reached.len() < run.started_attempts),
                    "a production hydration finished before reaching its armed checkpoint"
                );
                state = changed
                    .wait(state)
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
            }
        }

        pub fn release_participant_for_test(&self, participant: u64) {
            let (state, changed) = two_read_image_hydrations();
            let mut state = state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let run = state
                .active
                .as_mut()
                .filter(|run| run.token == self.token)
                .expect("two-hydration controller was cancelled");
            assert!(
                run.reached
                    .iter()
                    .any(|observation| observation.participant == participant),
                "cannot release a hydration participant before it reaches the checkpoint"
            );
            assert!(
                run.released.insert(participant),
                "hydration participant was released twice"
            );
            changed.notify_all();
        }

        pub fn finish_for_test(mut self) {
            let (state, changed) = two_read_image_hydrations();
            let mut state = state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let run = state
                .active
                .take()
                .filter(|run| run.token == self.token)
                .expect("two-hydration controller was cancelled");
            changed.notify_all();
            drop(state);
            assert_eq!(run.reached.len(), 2, "both hydration checkpoints must run");
            assert_eq!(
                run.started_attempts, 2,
                "both hydration attempts must start"
            );
            assert_eq!(
                run.finished_attempts, 2,
                "both hydration attempts must finish"
            );
            assert_eq!(run.released.len(), 2, "both hydrations must be released");
            assert_eq!(
                run.departed.len(),
                2,
                "both hydration intervals must leave the checkpoint"
            );
            self.finished = true;
        }
    }

    impl Drop for TwoReadImageHydrationController {
        fn drop(&mut self) {
            if self.finished {
                return;
            }
            let (state, changed) = two_read_image_hydrations();
            let mut state = state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if state.active.as_ref().map(|run| run.token) == Some(self.token) {
                state.active = None;
                changed.notify_all();
            }
        }
    }

    pub fn arm_two_read_image_hydrations_for_test() -> TwoReadImageHydrationController {
        let (state, _) = two_read_image_hydrations();
        let mut state = state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert!(
            state.active.is_none(),
            "two-hydration controller is already armed"
        );
        state.next_token = state.next_token.wrapping_add(1);
        let token = state.next_token;
        state.active = Some(TwoReadImageHydrationRun {
            token,
            next_participant: 0,
            started_attempts: 0,
            finished_attempts: 0,
            reached: Vec::new(),
            released: BTreeSet::new(),
            departed: BTreeSet::new(),
        });
        TwoReadImageHydrationController {
            token,
            next_observation: 0,
            finished: false,
        }
    }

    #[derive(Debug)]
    pub struct TwoReadImageHydrationAttemptGuard {
        token: u64,
    }

    impl Drop for TwoReadImageHydrationAttemptGuard {
        fn drop(&mut self) {
            let (state, changed) = two_read_image_hydrations();
            let mut state = state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some(run) = state.active.as_mut().filter(|run| run.token == self.token) {
                run.finished_attempts += 1;
                changed.notify_all();
            }
        }
    }

    pub fn begin_two_read_image_hydration_attempt_for_test() -> TwoReadImageHydrationAttemptGuard {
        let (state, _) = two_read_image_hydrations();
        let mut state = state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let run = state
            .active
            .as_mut()
            .expect("two-hydration controller is not armed");
        assert!(
            run.started_attempts < 2,
            "unexpected third hydration attempt"
        );
        run.started_attempts += 1;
        TwoReadImageHydrationAttemptGuard { token: run.token }
    }

    fn two_read_image_hydration_checkpoint_for_test(
        breadcrumb_path: Option<&Path>,
        breadcrumb: Option<&ReaderBreadcrumb>,
    ) {
        let (state, changed) = two_read_image_hydrations();
        let mut state = state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(run) = state.active.as_mut() else {
            return;
        };
        assert!(
            run.reached.len() < 2,
            "two-hydration controller observed an unexpected third participant"
        );
        let participant = run.next_participant;
        run.next_participant = run.next_participant.wrapping_add(1);
        let token = run.token;
        run.reached.push(ReadImageHydrationParticipantObservation {
            participant,
            breadcrumb_path: breadcrumb_path.map(Path::to_path_buf),
            breadcrumb: breadcrumb.cloned(),
        });
        changed.notify_all();
        loop {
            let waiting = state
                .active
                .as_ref()
                .is_some_and(|run| run.token == token && !run.released.contains(&participant));
            if !waiting {
                break;
            }
            state = changed
                .wait(state)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
        if let Some(run) = state.active.as_mut().filter(|run| run.token == token) {
            run.departed.insert(participant);
            changed.notify_all();
        }
    }

    pub fn arm_read_image_hydration_pause_for_test() {
        assert!(
            !READ_IMAGE_HYDRATION_PAUSE.swap(true, Ordering::SeqCst),
            "read-image hydration pause is already armed"
        );
    }

    /// The real loader calls this only after its runtime breadcrumb exists
    /// and its `redb::ReadOnlyDatabase` has acquired the hydration lock, and
    /// before decoding or releasing either resource.
    pub(crate) fn read_image_hydration_checkpoint_for_test(
        breadcrumb_path: Option<&Path>,
        breadcrumb: Option<&ReaderBreadcrumb>,
    ) {
        two_read_image_hydration_checkpoint_for_test(breadcrumb_path, breadcrumb);
        if !READ_IMAGE_HYDRATION_PAUSE.swap(false, Ordering::SeqCst) {
            return;
        }
        match (breadcrumb_path, breadcrumb) {
            (Some(path), Some(breadcrumb)) => println!(
                "READ_IMAGE_HYDRATION_HELD={}:{}:{}:{}",
                encode_hex(path.as_os_str().as_encoded_bytes()),
                breadcrumb.process_id,
                breadcrumb.process_start.0,
                encode_hex(breadcrumb.process_name.as_bytes())
            ),
            (None, None) => println!("READ_IMAGE_HYDRATION_HELD=unverified"),
            _ => panic!("hydration checkpoint path and breadcrumb must appear together"),
        }
        std::io::stdout()
            .flush()
            .expect("flush read-image hydration marker");
        let mut command = String::new();
        std::io::stdin()
            .lock()
            .read_line(&mut command)
            .expect("read hydration release command");
        assert_eq!(command.trim(), "release");
    }

    /// Rewrites the runtime breadcrumb through the production breadcrumb
    /// codec/location helper. No test-side file format fallback is permitted.
    pub fn replace_reader_breadcrumb_for_test(
        database_path: &Path,
        runtime_directory: &Path,
        expected_current: &ReaderBreadcrumb,
        replacement: &ReaderBreadcrumb,
    ) -> std::result::Result<PathBuf, LoadReadImageAdapterError> {
        let mut matching = super::locked_reader_breadcrumbs(database_path, runtime_directory)
            .map_err(|error| LoadReadImageAdapterError::Source(error.to_string()))?
            .into_iter()
            .filter(|locked| locked.breadcrumb.as_ref() == Some(expected_current));
        let locked = matching.next().ok_or_else(|| {
            LoadReadImageAdapterError::Source(
                "the expected locked reader breadcrumb is absent".to_owned(),
            )
        })?;
        if matching.next().is_some() {
            return Err(LoadReadImageAdapterError::Source(
                "the expected reader breadcrumb is not unique".to_owned(),
            ));
        }
        let encoded = super::encode_reader_breadcrumb(replacement)
            .map_err(|error| LoadReadImageAdapterError::Source(error.to_string()))?;
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&locked.path)
            .map_err(|error| LoadReadImageAdapterError::Source(error.to_string()))?;
        file.write_all(&encoded)
            .and_then(|_| file.sync_data())
            .map_err(|error| LoadReadImageAdapterError::Source(error.to_string()))?;
        Ok(locked.path)
    }

    pub fn create_unlocked_reader_breadcrumb_for_test(
        database_path: &Path,
        runtime_directory: &Path,
        breadcrumb: &ReaderBreadcrumb,
    ) -> std::result::Result<PathBuf, String> {
        let path = super::next_reader_breadcrumb_path(database_path, runtime_directory, breadcrumb)
            .map_err(|error| error.to_string())?;
        let directory = path
            .parent()
            .ok_or_else(|| "reader breadcrumb has no runtime directory".to_owned())?;
        std::fs::create_dir_all(directory).map_err(|error| error.to_string())?;
        let mut options = std::fs::OpenOptions::new();
        options.read(true).write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.mode(0o600);
        }
        let mut file = options.open(&path).map_err(|error| error.to_string())?;
        let encoded =
            super::encode_reader_breadcrumb(breadcrumb).map_err(|error| error.to_string())?;
        file.write_all(&encoded)
            .and_then(|_| file.sync_data())
            .map_err(|error| error.to_string())?;
        Ok(path)
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct LockedReaderBreadcrumbObservation {
        pub path: PathBuf,
        pub breadcrumb: Option<ReaderBreadcrumb>,
    }

    pub fn locked_reader_breadcrumbs_for_test(
        database_path: &Path,
        runtime_directory: &Path,
    ) -> std::result::Result<Vec<LockedReaderBreadcrumbObservation>, String> {
        super::locked_reader_breadcrumbs(database_path, runtime_directory)
            .map(|locked| {
                locked
                    .into_iter()
                    .map(|locked| LockedReaderBreadcrumbObservation {
                        path: locked.path,
                        breadcrumb: locked.breadcrumb,
                    })
                    .collect()
            })
            .map_err(|error| error.to_string())
    }

    pub fn canonical_reader_breadcrumb_directory_for_test(
        database_path: &Path,
        runtime_directory: &Path,
    ) -> std::result::Result<PathBuf, String> {
        super::reader_breadcrumb_directory(database_path, runtime_directory)
            .map_err(|error| error.to_string())
    }

    /// The ONE runtime root this process publishes reader breadcrumbs in and
    /// looks for other processes' breadcrumbs in.
    ///
    /// A proof that wants to see what a reader wrote down asks production
    /// where that is, rather than assuming the runtime directory anybody was
    /// handed: the breadcrumb location is the platform default and an
    /// `--owner-read-runtime-dir` override does not move it. `None` means this
    /// machine has no usable per-user runtime location at all.
    pub fn reader_breadcrumb_runtime_root_for_test() -> Option<PathBuf> {
        super::reader_breadcrumb_runtime_directory().map(|runtime| runtime.root().to_path_buf())
    }

    /// This is deliberately an observation door, not a record constructor.
    /// The real dual-slot decoder supplies it once the durable companion
    /// exists; until then the proof reports that the production record is
    /// absent rather than fabricating one in test code.
    pub fn inspect_companion_record_for_test(
        path: &Path,
    ) -> std::result::Result<CompanionRecordObservation, String> {
        super::inspect_companion_record(path).map(companion_observation)
    }

    pub fn store_fingerprint_for_test(path: &Path) -> std::result::Result<[u8; 32], String> {
        super::store_file_fingerprint(path).map_err(|error| error.to_string())
    }

    pub fn inspect_replacement_intent_for_test(
        path: &Path,
    ) -> std::result::Result<ReplacementIntentObservation, String> {
        let companion_path = super::companion_path(path);
        let mut options = std::fs::OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
        }
        let mut file = options
            .open(&companion_path)
            .map_err(|error| format!("open companion {}: {error}", companion_path.display()))?;
        let metadata = file
            .metadata()
            .map_err(|error| format!("inspect companion {}: {error}", companion_path.display()))?;
        super::validate_companion_metadata(&companion_path, &file, &metadata)
            .map_err(|error| error.to_string())?;
        let state = super::read_companion_state(&mut file)?;
        let active_record = state.active_record.map(companion_observation);
        let (pending_intended_record, pending_store_fingerprint) = state
            .pending_replacement
            .map(|pending| {
                (
                    Some(companion_observation(pending.intended_record)),
                    Some(pending.store_fingerprint),
                )
            })
            .unwrap_or((None, None));
        Ok(ReplacementIntentObservation {
            active_record,
            pending_intended_record,
            pending_store_fingerprint,
            current_store_fingerprint: store_fingerprint_for_test(path)?,
        })
    }

    /// One exclusively locked migration-source descriptor, as the production
    /// migration path is holding it right now. `locked_*` is what the
    /// descriptor identified when the lock succeeded; `observed_*` comes from
    /// asking that same descriptor again. A pathname stat cannot answer this
    /// once the generation is unlinked, which is the whole point of the door.
    #[cfg(unix)]
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct LockedMigrationSourceObservation {
        pub path: PathBuf,
        pub locked_device: u64,
        pub locked_inode: u64,
        pub observed_device: u64,
        pub observed_inode: u64,
        pub observed_link_count: u64,
    }

    #[cfg(unix)]
    #[derive(Debug)]
    struct LockedMigrationSourceDescriptor {
        path: PathBuf,
        descriptor: std::os::fd::RawFd,
        device: u64,
        inode: u64,
    }

    #[cfg(unix)]
    static LOCKED_MIGRATION_SOURCES: Mutex<Vec<LockedMigrationSourceDescriptor>> =
        Mutex::new(Vec::new());

    /// Records the exact descriptor the migration source just locked. The
    /// production path keeps ownership; this door only remembers which
    /// descriptor and identity to ask about later.
    #[cfg(unix)]
    pub(super) fn note_locked_migration_source_for_test(path: &Path, file: &std::fs::File) {
        use std::os::fd::AsRawFd as _;
        use std::os::unix::fs::MetadataExt as _;
        let Ok(metadata) = file.metadata() else {
            return;
        };
        let mut held = LOCKED_MIGRATION_SOURCES
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        held.retain(|recorded| recorded.path.as_path() != path);
        held.push(LockedMigrationSourceDescriptor {
            path: path.to_path_buf(),
            descriptor: file.as_raw_fd(),
            device: metadata.dev(),
            inode: metadata.ino(),
        });
    }

    #[cfg(unix)]
    pub fn reset_locked_migration_sources_for_test() {
        LOCKED_MIGRATION_SOURCES
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clear();
    }

    /// Restats every recorded migration-source descriptor through the exact
    /// descriptor production holds. The borrowed descriptor is wrapped in
    /// `ManuallyDrop` so observation never closes what production owns; a
    /// descriptor that no longer answers is omitted rather than guessed at.
    #[cfg(unix)]
    pub fn observe_locked_migration_sources_for_test() -> Vec<LockedMigrationSourceObservation> {
        use std::os::fd::FromRawFd as _;
        use std::os::unix::fs::MetadataExt as _;
        let held = LOCKED_MIGRATION_SOURCES
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        held.iter()
            .filter_map(|recorded| {
                // SAFETY: the descriptor is owned by the live production
                // migration source; `ManuallyDrop` keeps this borrowed view
                // from closing it, and nothing here writes through it.
                let borrowed = std::mem::ManuallyDrop::new(unsafe {
                    std::fs::File::from_raw_fd(recorded.descriptor)
                });
                let metadata = borrowed.metadata().ok()?;
                Some(LockedMigrationSourceObservation {
                    path: recorded.path.clone(),
                    locked_device: recorded.device,
                    locked_inode: recorded.inode,
                    observed_device: metadata.dev(),
                    observed_inode: metadata.ino(),
                    observed_link_count: metadata.nlink(),
                })
            })
            .collect()
    }

    /// One blocking wait a reader-release wait actually performed.
    ///
    /// A wait that sleeps until the kernel reports a holder let go blocks
    /// once per hold and supplies no deadline of its own. A wait that polls
    /// supplies a deadline every time, and that deadline IS the poll
    /// interval. Recording the shape of each block separates the two
    /// without measuring how long anything took.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum ReaderReleaseBlockForTest {
        /// Blocked until the kernel reported that a real holder of the
        /// store let go or died. Nothing but that event decides when this
        /// wakes.
        UntilHolderReleased,
        /// Blocked with a deadline of the wait's own choosing, in
        /// milliseconds.
        UntilDeadline { after_ms: u64 },
    }

    fn reader_release_blocks() -> &'static Mutex<Vec<ReaderReleaseBlockForTest>> {
        static BLOCKS: OnceLock<Mutex<Vec<ReaderReleaseBlockForTest>>> = OnceLock::new();
        BLOCKS.get_or_init(|| Mutex::new(Vec::new()))
    }

    /// Start recording what reader-release waits block in, discarding
    /// anything recorded before. Recording is process-wide, so a proof
    /// that uses it owns its test binary.
    pub fn reset_reader_release_blocks_for_test() {
        reader_release_blocks()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clear();
    }

    /// Every block reader-release waits have performed since the reset, in
    /// order.
    pub fn reader_release_blocks_for_test() -> Vec<ReaderReleaseBlockForTest> {
        reader_release_blocks()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    /// Production reader-release waits call this once for every block they
    /// perform, whatever mechanism they block on.
    pub(super) fn note_reader_release_block_for_test(block: ReaderReleaseBlockForTest) {
        reader_release_blocks()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(block);
    }

    /// Bypasses every ContextDB companion check and asks Redb itself whether
    /// the supplied exact database pathname can be READ without repairing it.
    /// Opens read-only, so asking changes nothing.
    pub fn try_raw_redb_read_only_open_for_test(path: &Path) -> RawRedbReadOnlyOpenObservation {
        match super::redb_builder().open_read_only(path) {
            Ok(database) => {
                drop(database);
                RawRedbReadOnlyOpenObservation::Acquired
            }
            Err(redb::DatabaseError::RepairAborted) => {
                RawRedbReadOnlyOpenObservation::RepairAborted
            }
            Err(error) => RawRedbReadOnlyOpenObservation::Other(error.to_string()),
        }
    }

    /// Bypasses every ContextDB companion check and asks Redb itself whether
    /// a writable owner can open the supplied exact database pathname.
    pub fn try_raw_redb_writer_open_for_test(path: &Path) -> RawRedbWriterOpenObservation {
        match super::redb_builder().open(path) {
            Ok(database) => {
                drop(database);
                RawRedbWriterOpenObservation::Acquired
            }
            Err(redb::DatabaseError::DatabaseAlreadyOpen) => {
                RawRedbWriterOpenObservation::DatabaseAlreadyOpen
            }
            Err(error) => RawRedbWriterOpenObservation::Other(error.to_string()),
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum DurableStoreDamage {
        MalformedRecord,
        LegacyLayout,
        NonMonotonicCommitIndex,
        InvalidVectorReference,
        RowVectorDivergence,
        WritableSanitizerWouldChange,
        /// A NULL sitting in a full-precision vector cell whose column is
        /// declared `NOT NULL` -- state the schema forbids, so no writer
        /// could have produced it and no reader may serve it.
        NullInRequiredVectorCell,
        /// A value that is not an embedding sitting in a vector cell.
        NonVectorValueInVectorCell,
        /// The separately durable vector entry a required quantized column's
        /// stored placeholder stands for, removed from a live row while the
        /// row itself is left exactly as its writer wrote it. The schema still
        /// promises that row an embedding the file no longer holds.
        MissingQuantizedVectorEntry,
    }

    impl DurableStoreDamage {
        pub const ALL: [Self; 9] = [
            Self::MalformedRecord,
            Self::LegacyLayout,
            Self::NonMonotonicCommitIndex,
            Self::InvalidVectorReference,
            Self::RowVectorDivergence,
            Self::WritableSanitizerWouldChange,
            Self::NullInRequiredVectorCell,
            Self::NonVectorValueInVectorCell,
            Self::MissingQuantizedVectorEntry,
        ];
    }

    fn open_damage_fixture(path: &Path) -> std::result::Result<redb::Database, String> {
        super::redb_builder()
            .open(path)
            .map_err(|error| error.to_string())
    }

    fn damage_table_metadata(
        database: &redb::Database,
    ) -> std::result::Result<std::collections::HashMap<String, super::TableMeta>, String> {
        let read = database.begin_read().map_err(|error| error.to_string())?;
        let table = read
            .open_table(super::META_TABLE)
            .map_err(|error| error.to_string())?;
        let mut metadata = std::collections::HashMap::new();
        for entry in table.iter().map_err(|error| error.to_string())? {
            let (key, value) = entry.map_err(|error| error.to_string())?;
            if let Some(table_name) = key.value().strip_prefix("table:") {
                let (table_meta, _) =
                    super::RedbPersistence::decode_table_meta_versioned(value.value())
                        .map_err(|error| error.to_string())?;
                metadata.insert(table_name.to_owned(), table_meta);
            }
        }
        Ok(metadata)
    }

    /// Replaces one text cell through Redb's real row codec so a copied
    /// legacy fixture can carry a per-run witness in both its exact bytes and
    /// the later migrated query result.
    pub fn replace_legacy_text_witness_for_test(
        path: &Path,
        table_name: &str,
        column_name: &str,
        expected: &str,
        replacement: &str,
    ) -> std::result::Result<(), String> {
        enum RowKey {
            Legacy(u64),
            Current(Vec<u8>),
        }

        let database = open_damage_fixture(path)?;
        let metadata = damage_table_metadata(&database)?;
        let table_meta = metadata
            .get(table_name)
            .ok_or_else(|| format!("legacy witness table is absent: {table_name}"))?;
        let redb_name = super::RedbPersistence::rel_table_name(table_name);
        let read = database.begin_read().map_err(|error| error.to_string())?;
        let mut matches = Vec::new();
        let legacy_definition: TableDefinition<u64, &[u8]> =
            TableDefinition::new(redb_name.as_str());
        match read.open_table(legacy_definition) {
            Ok(table) => {
                for entry in table.iter().map_err(|error| error.to_string())? {
                    let (key, value) = entry.map_err(|error| error.to_string())?;
                    let row = super::RedbPersistence::decode_versioned_row(
                        value.value(),
                        Some(table_meta),
                    )
                    .map_err(|error| error.to_string())?;
                    if row.values.get(column_name) == Some(&super::Value::Text(expected.to_owned()))
                    {
                        matches.push((RowKey::Legacy(key.value()), row));
                    }
                }
            }
            Err(redb::TableError::TableTypeMismatch { .. }) => {
                let current_definition: TableDefinition<&[u8], &[u8]> =
                    TableDefinition::new(redb_name.as_str());
                let table = read
                    .open_table(current_definition)
                    .map_err(|error| error.to_string())?;
                for entry in table.iter().map_err(|error| error.to_string())? {
                    let (key, value) = entry.map_err(|error| error.to_string())?;
                    let row = super::RedbPersistence::decode_versioned_row(
                        value.value(),
                        Some(table_meta),
                    )
                    .map_err(|error| error.to_string())?;
                    if row.values.get(column_name) == Some(&super::Value::Text(expected.to_owned()))
                    {
                        matches.push((RowKey::Current(key.value().to_vec()), row));
                    }
                }
            }
            Err(error) => return Err(error.to_string()),
        }
        drop(read);
        if matches.len() != 1 {
            return Err(format!(
                "legacy witness must identify exactly one row, found {}",
                matches.len()
            ));
        }
        let (key, mut row) = matches.pop().expect("one legacy witness row");
        row.values.insert(
            column_name.to_owned(),
            super::Value::Text(replacement.to_owned()),
        );
        let encoded = super::RedbPersistence::encode_versioned_row(&row, Some(table_meta))
            .map_err(|error| error.to_string())?;
        let write = database.begin_write().map_err(|error| error.to_string())?;
        match key {
            RowKey::Legacy(key) => {
                let mut table = write
                    .open_table(legacy_definition)
                    .map_err(|error| error.to_string())?;
                table
                    .insert(key, encoded.as_slice())
                    .map_err(|error| error.to_string())?;
            }
            RowKey::Current(key) => {
                let definition: TableDefinition<&[u8], &[u8]> =
                    TableDefinition::new(redb_name.as_str());
                let mut table = write
                    .open_table(definition)
                    .map_err(|error| error.to_string())?;
                table
                    .insert(key.as_slice(), encoded.as_slice())
                    .map_err(|error| error.to_string())?;
            }
        }
        write.commit().map_err(|error| error.to_string())?;
        drop(database);
        Ok(())
    }

    fn damage_format_marker(
        database: &redb::Database,
        bytes: &[u8],
    ) -> std::result::Result<(), String> {
        let write = database.begin_write().map_err(|error| error.to_string())?;
        {
            let mut table = write
                .open_table(super::FORMAT_METADATA_TABLE)
                .map_err(|error| error.to_string())?;
            table
                .insert(super::FORMAT_VERSION_KEY, bytes)
                .map_err(|error| error.to_string())?;
        }
        write.commit().map_err(|error| error.to_string())
    }

    fn damage_commit_index(database: &redb::Database) -> std::result::Result<(), String> {
        let read = database.begin_read().map_err(|error| error.to_string())?;
        let table = read
            .open_table(super::COMMIT_INDEX_TABLE)
            .map_err(|error| error.to_string())?;
        let mut maximum_lsn = 0_u64;
        let mut maximum_tx = 0_u64;
        for entry in table.iter().map_err(|error| error.to_string())? {
            let (lsn, tx) = entry.map_err(|error| error.to_string())?;
            maximum_lsn = maximum_lsn.max(lsn.value());
            maximum_tx = maximum_tx.max(tx.value());
        }
        drop(table);
        drop(read);
        let first_lsn = maximum_lsn
            .checked_add(1)
            .ok_or_else(|| "fixture commit LSN overflow".to_owned())?;
        let second_lsn = maximum_lsn
            .checked_add(2)
            .ok_or_else(|| "fixture commit LSN overflow".to_owned())?;
        let first_tx = maximum_tx
            .checked_add(2)
            .ok_or_else(|| "fixture transaction overflow".to_owned())?;
        let second_tx = maximum_tx
            .checked_add(1)
            .ok_or_else(|| "fixture transaction overflow".to_owned())?;
        let write = database.begin_write().map_err(|error| error.to_string())?;
        {
            let mut table = write
                .open_table(super::COMMIT_INDEX_TABLE)
                .map_err(|error| error.to_string())?;
            table
                .insert(first_lsn, first_tx)
                .map_err(|error| error.to_string())?;
            table
                .insert(second_lsn, second_tx)
                .map_err(|error| error.to_string())?;
        }
        write.commit().map_err(|error| error.to_string())
    }

    fn first_vector_entry(
        database: &redb::Database,
        required_quantization: Option<super::VectorQuantization>,
    ) -> std::result::Result<(Vec<u8>, super::VectorEntry, super::VectorQuantization), String> {
        let metadata = damage_table_metadata(database)?;
        let quantizations = super::RedbPersistence::vector_quantization_map(&metadata);
        let read = database.begin_read().map_err(|error| error.to_string())?;
        let table = read
            .open_table(super::VECTORS_TABLE)
            .map_err(|error| error.to_string())?;
        for entry in table.iter().map_err(|error| error.to_string())? {
            let (key, value) = entry.map_err(|error| error.to_string())?;
            let vector = super::RedbPersistence::decode_vector_entry(value.value())
                .map_err(|error| error.to_string())?;
            let quantization = quantizations.get(&vector.index).copied().ok_or_else(|| {
                format!("fixture vector index has no metadata: {:?}", vector.index)
            })?;
            if required_quantization.is_none_or(|required| required == quantization) {
                return Ok((key.value().to_vec(), vector, quantization));
            }
        }
        Err("fixture contains no vector matching the requested quantization".to_owned())
    }

    fn damage_vector_reference(database: &redb::Database) -> std::result::Result<(), String> {
        let (old_key, mut vector, quantization) = first_vector_entry(database, None)?;
        vector.index = super::VectorIndexRef::new(
            "__missing_fixture_table",
            "__missing_fixture_vector_column",
        );
        let new_key = super::RedbPersistence::vector_key(&vector);
        let encoded = super::RedbPersistence::encode_vector_entry(&vector, quantization)
            .map_err(|error| error.to_string())?;
        let write = database.begin_write().map_err(|error| error.to_string())?;
        {
            let mut table = write
                .open_table(super::VECTORS_TABLE)
                .map_err(|error| error.to_string())?;
            table
                .remove(old_key.as_slice())
                .map_err(|error| error.to_string())?;
            table
                .insert(new_key.as_slice(), encoded.as_slice())
                .map_err(|error| error.to_string())?;
        }
        write.commit().map_err(|error| error.to_string())
    }

    /// Removes the stored vector entry of a required quantized column from the
    /// vector table and leaves the relational row untouched. A quantized
    /// column keeps no copy of the embedding in the row, so what remains is a
    /// live row whose `NOT NULL` column has nothing left to be served from --
    /// state no writer produces, diagnosed from the stored bytes alone.
    fn damage_missing_quantized_vector_entry(
        database: &redb::Database,
    ) -> std::result::Result<(), String> {
        let metadata = damage_table_metadata(database)?;
        let read = database.begin_read().map_err(|error| error.to_string())?;
        let table = read
            .open_table(super::VECTORS_TABLE)
            .map_err(|error| error.to_string())?;
        let mut removed = None;
        for entry in table.iter().map_err(|error| error.to_string())? {
            let (key, value) = entry.map_err(|error| error.to_string())?;
            let vector = super::RedbPersistence::decode_vector_entry(value.value())
                .map_err(|error| error.to_string())?;
            let required_and_quantized = metadata
                .get(&vector.index.table)
                .and_then(|table_meta| {
                    table_meta
                        .columns
                        .iter()
                        .find(|column| column.name == vector.index.column)
                })
                .is_some_and(|column| {
                    !column.nullable
                        && !matches!(column.quantization, super::VectorQuantization::F32)
                });
            if required_and_quantized {
                removed = Some(key.value().to_vec());
                break;
            }
        }
        drop(table);
        drop(read);
        let key = removed.ok_or_else(|| {
            "fixture holds no embedding for a quantized vector column declared NOT NULL".to_owned()
        })?;
        let write = database.begin_write().map_err(|error| error.to_string())?;
        {
            let mut table = write
                .open_table(super::VECTORS_TABLE)
                .map_err(|error| error.to_string())?;
            table
                .remove(key.as_slice())
                .map_err(|error| error.to_string())?;
        }
        write.commit().map_err(|error| error.to_string())
    }

    fn damage_row_vector_divergence(database: &redb::Database) -> std::result::Result<(), String> {
        let (key, mut vector, quantization) =
            first_vector_entry(database, Some(super::VectorQuantization::F32))?;
        let component = vector
            .vector
            .first_mut()
            .ok_or_else(|| "fixture F32 vector is empty".to_owned())?;
        *component = if component.is_finite() {
            *component + 0.5
        } else {
            0.5
        };
        let encoded = super::RedbPersistence::encode_vector_entry(&vector, quantization)
            .map_err(|error| error.to_string())?;
        let write = database.begin_write().map_err(|error| error.to_string())?;
        {
            let mut table = write
                .open_table(super::VECTORS_TABLE)
                .map_err(|error| error.to_string())?;
            table
                .insert(key.as_slice(), encoded.as_slice())
                .map_err(|error| error.to_string())?;
        }
        write.commit().map_err(|error| error.to_string())
    }

    fn first_relational_row(
        database: &redb::Database,
    ) -> std::result::Result<(String, super::TableMeta, Vec<u8>, super::VersionedRow), String> {
        let metadata = damage_table_metadata(database)?;
        let mut table_names = metadata.keys().cloned().collect::<Vec<_>>();
        table_names.sort();
        let read = database.begin_read().map_err(|error| error.to_string())?;
        for table_name in table_names {
            let redb_name = super::RedbPersistence::rel_table_name(&table_name);
            let definition: TableDefinition<&[u8], &[u8]> =
                TableDefinition::new(redb_name.as_str());
            let table = match read.open_table(definition) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => continue,
                Err(error) => return Err(error.to_string()),
            };
            let mut entries = table.iter().map_err(|error| error.to_string())?;
            if let Some(entry) = entries.next() {
                let (key, value) = entry.map_err(|error| error.to_string())?;
                let table_meta = metadata
                    .get(&table_name)
                    .cloned()
                    .ok_or_else(|| "fixture table metadata disappeared".to_owned())?;
                let row =
                    super::RedbPersistence::decode_versioned_row(value.value(), Some(&table_meta))
                        .map_err(|error| error.to_string())?;
                return Ok((table_name, table_meta, key.value().to_vec(), row));
            }
        }
        Err("fixture contains no relational row".to_owned())
    }

    /// Overwrites one stored relational row with the supplied replacement.
    fn write_relational_row(
        database: &redb::Database,
        table_name: &str,
        table_meta: &super::TableMeta,
        key: &[u8],
        row: &super::VersionedRow,
    ) -> std::result::Result<(), String> {
        let encoded = super::RedbPersistence::encode_versioned_row(row, Some(table_meta))
            .map_err(|error| error.to_string())?;
        let redb_name = super::RedbPersistence::rel_table_name(table_name);
        let definition: TableDefinition<&[u8], &[u8]> = TableDefinition::new(redb_name.as_str());
        let write = database.begin_write().map_err(|error| error.to_string())?;
        {
            let mut table = write
                .open_table(definition)
                .map_err(|error| error.to_string())?;
            table
                .insert(key, encoded.as_slice())
                .map_err(|error| error.to_string())?;
        }
        write.commit().map_err(|error| error.to_string())
    }

    /// Plants `planted` in the vector cell of the first stored row whose table
    /// declares a vector column the predicate accepts and which currently
    /// holds a real embedding there. Writing through the production row codec
    /// is what makes the damage durable: nothing marks the row, so a later
    /// open diagnoses it from the bytes alone.
    fn damage_vector_cell(
        database: &redb::Database,
        accepts: impl Fn(&contextdb_core::ColumnDef) -> bool,
        planted: super::Value,
        wanted: &str,
    ) -> std::result::Result<(), String> {
        let metadata = damage_table_metadata(database)?;
        let mut table_names = metadata.keys().cloned().collect::<Vec<_>>();
        table_names.sort();
        let read = database.begin_read().map_err(|error| error.to_string())?;
        for table_name in table_names {
            let table_meta = metadata
                .get(&table_name)
                .cloned()
                .ok_or_else(|| "fixture table metadata disappeared".to_owned())?;
            let Some(column) = table_meta
                .columns
                .iter()
                .find(|column| {
                    matches!(column.column_type, super::ColumnType::Vector(_)) && accepts(column)
                })
                .map(|column| column.name.clone())
            else {
                continue;
            };
            let redb_name = super::RedbPersistence::rel_table_name(&table_name);
            let definition: TableDefinition<&[u8], &[u8]> =
                TableDefinition::new(redb_name.as_str());
            let table = match read.open_table(definition) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => continue,
                Err(error) => return Err(error.to_string()),
            };
            let mut damaged = None;
            for entry in table.iter().map_err(|error| error.to_string())? {
                let (key, value) = entry.map_err(|error| error.to_string())?;
                let mut row =
                    super::RedbPersistence::decode_versioned_row(value.value(), Some(&table_meta))
                        .map_err(|error| error.to_string())?;
                if !matches!(row.values.get(&column), Some(super::Value::Vector(_))) {
                    continue;
                }
                row.values.insert(column.clone(), planted.clone());
                damaged = Some((key.value().to_vec(), row));
                break;
            }
            drop(table);
            if let Some((key, row)) = damaged {
                drop(read);
                return write_relational_row(database, &table_name, &table_meta, &key, &row);
            }
        }
        Err(format!(
            "fixture holds no embedding in a vector cell of {wanted}"
        ))
    }

    fn damage_writable_sanitizer(database: &redb::Database) -> std::result::Result<(), String> {
        let (table_name, table_meta, key, mut row) = first_relational_row(database)?;
        let mut column = "__unknown_fixture_column".to_owned();
        while row.values.contains_key(&column) {
            column.push('_');
        }
        row.values
            .insert(column, super::Value::Text("durable".to_owned()));
        let encoded = super::RedbPersistence::encode_versioned_row(&row, Some(&table_meta))
            .map_err(|error| error.to_string())?;
        let redb_name = super::RedbPersistence::rel_table_name(&table_name);
        let definition: TableDefinition<&[u8], &[u8]> = TableDefinition::new(redb_name.as_str());
        let write = database.begin_write().map_err(|error| error.to_string())?;
        {
            let mut table = write
                .open_table(definition)
                .map_err(|error| error.to_string())?;
            table
                .insert(key.as_slice(), encoded.as_slice())
                .map_err(|error| error.to_string())?;
        }
        write.commit().map_err(|error| error.to_string())
    }

    /// Creates the requested defect in Redb itself. No marker, sidecar, or
    /// test-only decoder participates in later diagnosis.
    pub fn prepare_durable_store_damage_for_test(
        path: &Path,
        damage: DurableStoreDamage,
    ) -> std::result::Result<(), String> {
        let database = open_damage_fixture(path)?;
        let result = match damage {
            DurableStoreDamage::MalformedRecord => {
                damage_format_marker(&database, &[0xff, 0x00, 0xfe])
            }
            DurableStoreDamage::LegacyLayout => {
                let legacy = super::RedbPersistence::encode(&"0.9.0".to_owned())
                    .map_err(|error| error.to_string())?;
                damage_format_marker(&database, &legacy)
            }
            DurableStoreDamage::NonMonotonicCommitIndex => damage_commit_index(&database),
            DurableStoreDamage::InvalidVectorReference => damage_vector_reference(&database),
            DurableStoreDamage::RowVectorDivergence => damage_row_vector_divergence(&database),
            DurableStoreDamage::WritableSanitizerWouldChange => {
                damage_writable_sanitizer(&database)
            }
            DurableStoreDamage::NullInRequiredVectorCell => damage_vector_cell(
                &database,
                |column| {
                    !column.nullable
                        && matches!(column.quantization, super::VectorQuantization::F32)
                },
                super::Value::Null,
                "a full-precision vector column declared NOT NULL",
            ),
            DurableStoreDamage::NonVectorValueInVectorCell => damage_vector_cell(
                &database,
                |_| true,
                super::Value::Int64(7),
                "a vector column",
            ),
            DurableStoreDamage::MissingQuantizedVectorEntry => {
                damage_missing_quantized_vector_entry(&database)
            }
        };
        drop(database);
        result
    }
}

/// Bound Redb's process-resident page cache independently of database size.
/// Large blob payloads remain disk-backed instead of accumulating in Redb's
/// one-GiB default cache.
const REDB_CACHE_BYTES: usize = 8 * 1024 * 1024;

fn redb_builder() -> redb::Builder {
    let mut builder = redb::Builder::new();
    builder.set_cache_size(REDB_CACHE_BYTES);
    builder
}

thread_local! {
    /// Test-only injection seam: when armed, the NEXT [`RedbPersistence::
    /// compact`]'s post-compact handle recycle (close the redb handle, then
    /// reopen the same on-disk file) fails its reopen, so a test can assert
    /// the on-disk file stays intact and a fresh `RedbPersistence::open`
    /// still recovers every row after a mid-recycle failure. Armed only by
    /// `arm_handle_recycle_reopen_fault_for_test`; default off, so
    /// production reads a thread-local that is never set.
    static HANDLE_RECYCLE_REOPEN_FAULT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// One-shot failure immediately before the received-schema Redb commit.
    /// It proves no private stage publishes memory when durability aborts.
    static RECEIVED_SCHEMA_PRE_COMMIT_FAULT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(test)]
thread_local! {
    /// One-shot failure at the received-schema side-effect checkpoint. The
    /// checkpoint is after the complete core image has been assembled in the
    /// live Redb write transaction and immediately before sink-queue and
    /// trigger-audit persistence; a failed checkpoint must leave all of that
    /// one transaction uncommitted.
    static RECEIVED_SCHEMA_SIDE_EFFECT_PERSISTENCE_FAULT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// One-shot failure after an authoritative purge has staged every point
    /// removal and lifecycle write, immediately before its sole Redb commit.
    /// The write transaction must roll back every copy class together.
    static AUTHORITATIVE_PURGE_PRE_COMMIT_FAULT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Arm the one-shot test injection seam: the NEXT `compact()` call on THIS
/// thread fails to reopen after closing the handle. Production-dead —
/// nothing but a test calls it.
#[cfg(any(test, feature = "test-seams"))]
pub(crate) fn arm_handle_recycle_reopen_fault_for_test() {
    HANDLE_RECYCLE_REOPEN_FAULT.with(|f| f.set(true));
}

/// Consume the armed injection flag, if any. Reads a thread-local that is
/// never set outside a test, so the production path takes the `false` arm.
fn take_handle_recycle_reopen_fault_for_test() -> bool {
    HANDLE_RECYCLE_REOPEN_FAULT.with(|f| f.replace(false))
}

#[cfg(test)]
pub(crate) fn arm_received_schema_pre_commit_fault_for_test() {
    RECEIVED_SCHEMA_PRE_COMMIT_FAULT.with(|fault| fault.set(true));
}

fn take_received_schema_pre_commit_fault_for_test() -> bool {
    RECEIVED_SCHEMA_PRE_COMMIT_FAULT.with(|fault| fault.replace(false))
}

/// Arm the one-shot received-schema side-effect checkpoint fault for this
/// test thread. This seam is absent from production builds.
#[cfg(test)]
pub(crate) fn arm_received_schema_side_effect_persistence_fault_for_test() {
    RECEIVED_SCHEMA_SIDE_EFFECT_PERSISTENCE_FAULT.with(|fault| fault.set(true));
}

#[cfg(test)]
fn take_received_schema_side_effect_persistence_fault_for_test() -> bool {
    RECEIVED_SCHEMA_SIDE_EFFECT_PERSISTENCE_FAULT.with(|fault| fault.replace(false))
}

#[cfg(test)]
pub(crate) fn arm_authoritative_purge_point_remove_persistence_failure_for_test() {
    AUTHORITATIVE_PURGE_PRE_COMMIT_FAULT.with(|fault| fault.set(true));
}

#[cfg(test)]
fn take_authoritative_purge_point_remove_persistence_failure_for_test() -> bool {
    AUTHORITATIVE_PURGE_PRE_COMMIT_FAULT.with(|fault| fault.replace(false))
}

const META_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");
const FORMAT_METADATA_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("metadata");
const CONFIG_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("config");
const CHANGE_LOG_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("change_log");
const DDL_LOG_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("ddl_log");
const COMMIT_INDEX_TABLE: TableDefinition<u64, u64> = TableDefinition::new("commit_index");
const SINK_AUDIT_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("__sink_audit");
const TRIGGER_AUDIT_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("__trigger_audit");
/// When each durable `__trigger_audit` row was written, keyed identically to
/// the audit row itself. Kept beside the history rather than inside them so
/// existing on-disk audit payloads decode unchanged; retention reads it to
/// decide what has aged out. A legacy row with no stamp is never pruned.
const TRIGGER_AUDIT_STAMPS_TABLE: TableDefinition<&str, u64> =
    TableDefinition::new("__trigger_audit_stamps");
const GRAPH_FWD_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("graph_fwd");
const GRAPH_REV_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("graph_rev");
const VECTORS_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("vector_entries");
const SYNC_ROW_SOURCE_LSN_TABLE: TableDefinition<&[u8], u64> =
    TableDefinition::new("sync_row_source_lsn");
const SYNC_ROW_SOURCE_KIND_TABLE: TableDefinition<&[u8], u8> =
    TableDefinition::new("sync_row_source_kind");
const FORMAT_VERSION_KEY: &str = "format_version";
pub(crate) const CURRENT_FORMAT_VERSION: &str = "1.0.0";
pub(crate) const TRIGGER_AUDIT_NEXT_INDEX_CONFIG_KEY: &str = "__trigger_audit_next_index";
pub(crate) const TRIGGER_AUDIT_RING_CONFIG_KEY: &str = "__trigger_audit_ring";

fn sync_source_kind_to_u8(kind: SyncSourceKind) -> u8 {
    match kind {
        SyncSourceKind::Pulled => 0,
        SyncSourceKind::AcceptedLocal => 1,
        SyncSourceKind::AcceptedLocalPending => 2,
    }
}

pub struct RedbPersistence {
    path: std::path::PathBuf,
    lock_file: Mutex<Option<CompanionGuard>>,
    db: Mutex<Option<redb::Database>>,
    /// A fresh migration target receives the source persistence capability
    /// explicitly during import. The target consumes it exactly once, while
    /// closing, to persist replacement intent before its Redb handle is
    /// released. No pathname or process-global reentrancy participates.
    migration_replacement_source: Mutex<Option<Arc<RedbPersistence>>>,
    /// Set the first time a table's meta decodes only via the exact
    /// pre-`contextdb migrate` `v1.0.0` layout (see
    /// [`Self::decode_table_meta_versioned`]) rather than the current one.
    /// `Database::open` refuses on this (`Error::LegacyVectorStoreDetected`,
    /// naming `contextdb migrate`); the migrate command's own loader reads
    /// past it. Never set back to `false` — one legacy table is enough to
    /// mark the whole root.
    used_legacy_table_meta_layout: AtomicBool,
}

/// Exclusive access to the exact legacy-store file descriptor before Redb
/// has had any opportunity to perform its open-time housekeeping write. The
/// migration command holds this through its byte-for-byte backup; hydration
/// then consumes the same descriptor into Redb without an unlock/reopen gap.
pub(crate) struct LegacyMigrationOpenCapability {
    path: PathBuf,
    companion: Option<CompanionGuard>,
    file: Option<File>,
    /// The device and inode the locked descriptor named when the lock was
    /// taken, kept beside it so the pre-migration backup can prove it is
    /// still reading the store it locked without ever resolving the source
    /// pathname again.
    #[cfg(unix)]
    identity: (u64, u64),
}

impl LegacyMigrationOpenCapability {
    /// Copy the exact store this capability locked into `destination`,
    /// reading only through the descriptor taken when the capability was
    /// acquired.
    ///
    /// The source pathname is never resolved again, so unlinking it or
    /// replacing it with a symlink after the lock was taken cannot redirect
    /// one byte of the backup. There is deliberately no pathname fallback: a
    /// capability whose descriptor has already been consumed into Redb is
    /// refused rather than quietly served whatever the pathname resolves to
    /// now.
    #[cfg(unix)]
    pub(crate) fn copy_locked_source_to(&self, destination: &Path) -> Result<u64> {
        use std::os::unix::fs::FileExt as _;
        use std::os::unix::fs::MetadataExt as _;
        use std::os::unix::fs::OpenOptionsExt as _;
        use std::os::unix::fs::PermissionsExt as _;

        let locked = self.file.as_ref().ok_or_else(|| {
            Error::Other(format!(
                "no locked migration source is held for {}",
                self.path.display()
            ))
        })?;
        let opened = locked.metadata().map_err(RedbPersistence::storage_error)?;
        if !opened.file_type().is_file() || (opened.dev(), opened.ino()) != self.identity {
            return Err(Error::StoreCorrupted {
                path: self.path.display().to_string(),
                reason:
                    "the locked migration source descriptor no longer names the store it locked"
                        .to_owned(),
            });
        }
        let mut options = OpenOptions::new();
        options
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
        let destination_file = options
            .open(destination)
            .map_err(RedbPersistence::storage_error)?;
        destination_file
            .set_permissions(std::fs::Permissions::from_mode(0o600))
            .map_err(RedbPersistence::storage_error)?;
        // Positioned reads, so the backup never moves the file offset this
        // descriptor shares with whatever else holds the same open-file
        // description.
        let mut copied = 0_u64;
        let mut buffer = vec![0_u8; 64 * 1024];
        loop {
            let read = locked
                .read_at(&mut buffer, copied)
                .map_err(RedbPersistence::storage_error)?;
            if read == 0 {
                break;
            }
            destination_file
                .write_all_at(&buffer[..read], copied)
                .map_err(RedbPersistence::storage_error)?;
            copied += read as u64;
        }
        let after = locked.metadata().map_err(RedbPersistence::storage_error)?;
        if (after.dev(), after.ino()) != self.identity
            || after.len() != opened.len()
            || copied != opened.len()
        {
            return Err(Error::StoreCorrupted {
                path: self.path.display().to_string(),
                reason: "the locked migration source changed while its backup was copied"
                    .to_owned(),
            });
        }
        destination_file
            .sync_all()
            .map_err(RedbPersistence::storage_error)?;
        sync_database_parent(destination)?;
        Ok(copied)
    }
}

#[derive(Debug)]
struct LockedMigrationFileBackend {
    file: Mutex<File>,
}

impl LockedMigrationFileBackend {
    fn file(&self) -> std::io::Result<std::sync::MutexGuard<'_, File>> {
        self.file
            .lock()
            .map_err(|_| std::io::Error::other("migration store file mutex poisoned"))
    }
}

impl redb::StorageBackend for LockedMigrationFileBackend {
    fn len(&self) -> std::io::Result<u64> {
        self.file()?.metadata().map(|metadata| metadata.len())
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> std::io::Result<()> {
        let mut file = self.file()?;
        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(out)
    }

    fn set_len(&self, len: u64) -> std::io::Result<()> {
        self.file()?.set_len(len)
    }

    fn sync_data(&self) -> std::io::Result<()> {
        self.file()?.sync_data()
    }

    fn write(&self, offset: u64, data: &[u8]) -> std::io::Result<()> {
        let mut file = self.file()?;
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(data)
    }

    fn close(&self) -> std::io::Result<()> {
        fs2::FileExt::unlock(&*self.file()?)
    }
}

pub(crate) struct RawSyncApplyStateDigest {
    pub(crate) digest: String,
    pub(crate) trigger_audit_entries: u64,
    pub(crate) sink_audit_entries: u64,
    pub(crate) sink_queue_entries: u64,
}

fn digest_field(hasher: &mut blake3::Hasher, bytes: &[u8]) {
    hasher.update(&(bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
}

fn digest_redb_table<K: RedbKey + 'static, V: RedbValue + 'static>(
    read_txn: &redb::ReadTransaction,
    definition: TableDefinition<K, V>,
    label: &str,
    hasher: &mut blake3::Hasher,
) -> Result<u64> {
    digest_field(hasher, label.as_bytes());
    let table = match read_txn.open_table(definition) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(0),
        Err(err) => {
            return Err(Error::Other(format!(
                "snapshot digest table open failed: {err}"
            )));
        }
    };
    let mut count = 0_u64;
    for entry in table
        .iter()
        .map_err(|err| Error::Other(format!("snapshot digest table iteration failed: {err}")))?
    {
        let (key, value) = entry
            .map_err(|err| Error::Other(format!("snapshot digest entry read failed: {err}")))?;
        let key_value = key.value();
        let value_value = value.value();
        let key_bytes = K::as_bytes(&key_value);
        let value_bytes = V::as_bytes(&value_value);
        digest_field(hasher, key_bytes.as_ref());
        digest_field(hasher, value_bytes.as_ref());
        count = count.saturating_add(1);
    }
    Ok(count)
}

/// Point-removed key counts from one scoped prune pass — real integer
/// counters, no rewritten-survivor cost, mirroring the `FkProbeStats`
/// precedent. Folded into the caller's per-table receipt
/// (`TableVersionCleanup`/`CurrencyCompactionReport`), and the bench's
/// proportionality assertions read these directly.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PruneScopedStats {
    pub row_keys_removed: u64,
    pub change_log_keys_removed: u64,
    pub vector_keys_removed: u64,
    pub edge_keys_removed: u64,
}

pub(crate) struct SchemaDdlPersistence<'a> {
    pub(crate) event_bus: Option<&'a EventBusPersistenceCommit>,
    pub(crate) trigger: Option<&'a TriggerPersistenceCommit>,
}

#[derive(Default)]
pub(crate) struct FlushDataSnapshots {
    pub(crate) table_meta: Option<HashMap<String, TableMeta>>,
    pub(crate) deleted_rows: Option<HashMap<String, HashMap<RowId, VersionedRow>>>,
}

pub(crate) struct FlushDataOptions<'a> {
    pub(crate) sink_events: &'a [PreparedSinkEvent],
    pub(crate) trigger_audits: &'a [(u64, TriggerAuditEntry)],
    pub(crate) schema_ddl: SchemaDdlPersistence<'a>,
    pub(crate) max_sink_queue_depth: usize,
    pub(crate) snapshots: FlushDataSnapshots,
    /// A complete prepared received-schema image. When present, it replaces
    /// the ordinary incremental Redb writes in this same transaction; memory
    /// publication remains the transaction manager's after-apply work.
    pub(crate) received_schema: Option<&'a ReceivedSchemaPersistenceProjection>,
}

/// The durable half of a private received-schema stage. It is fully owned and
/// encoded before Redb begins its write transaction, so persistence cannot
/// regroup source DDL or consult live schema state while committing it.
pub(crate) struct ReceivedSchemaPersistenceProjection {
    pub(crate) table_meta: HashMap<String, TableMeta>,
    pub(crate) rows: HashMap<String, Vec<VersionedRow>>,
    pub(crate) sync_sources: Vec<(String, RowId, Lsn, SyncSourceKind)>,
    pub(crate) edges: Vec<AdjEntry>,
    pub(crate) vectors: Vec<VectorEntry>,
    pub(crate) ddl_log: Vec<(Lsn, DdlChange)>,
    pub(crate) config_values: Vec<(String, Vec<u8>)>,
    pub(crate) config_max_u64_keys: Vec<String>,
    /// A received DROP TABLE retires this generation's data history. Its
    /// row/vector entries must leave the durable log in the same transaction
    /// that installs the replacement schema image.
    pub(crate) structurally_dropped_tables: HashSet<String>,
}

/// The narrow durable payload for one locally authored SQL schema commit.
/// Unlike a received-schema projection, it updates only the schema entries
/// the local transaction changed and never reconstructs unrelated rows,
/// graph state, vectors, or sidecars.
pub(crate) struct LocalSchemaPersistenceProjection {
    /// Metadata for exactly the tables affected by this local DDL vector.
    pub(crate) affected_table_meta: HashMap<String, TableMeta>,
    /// The ordered DDL vector, all durably recorded at the owning commit LSN.
    pub(crate) ddl: Vec<DdlChange>,
    /// Values already encoded by the schema preparation phase.
    pub(crate) config_values: Vec<(String, Vec<u8>)>,
    /// DDL-only WriteSets do not carry a row from which persistence can infer
    /// this identity, so retain the transaction manager's visibility choice.
    pub(crate) commit_index_tx: TxId,
}

/// Fully prepared, crate-private durable mutation for one authoritative
/// purge.  It contains only persistence-owned/public value types: database
/// lineage structs are encoded by the caller before Redb starts its write
/// transaction.  The point identities keep the storage layer from reading or
/// rediscovering mutable database state while it commits.
pub(crate) struct AuthoritativePurgePersistenceProjection {
    pub(crate) row_versions: Vec<(String, RowId, TxId, Lsn)>,
    pub(crate) source_provenance: Vec<(String, RowId, Lsn, u8)>,
    pub(crate) vectors: Vec<VectorEntry>,
    pub(crate) graph_entries: Vec<AdjEntry>,
    pub(crate) sink_entries: Vec<(String, u64, Vec<u8>)>,
    pub(crate) change_log_entries: Vec<ChangeLogEntry>,
    pub(crate) config_keys_removed: Vec<String>,
    pub(crate) lifecycle_records: Vec<(String, Vec<u8>)>,
    pub(crate) purge_delivery_items: Vec<(String, Vec<u8>)>,
    pub(crate) blob_purge: BlobAuthoritativePurgeProjection,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedVersionedRow {
    row_id: contextdb_core::RowId,
    values: HashMap<String, PersistedValue>,
    created_tx: contextdb_core::TxId,
    deleted_tx: Option<contextdb_core::TxId>,
    lsn: Lsn,
    created_at: Option<contextdb_core::Wallclock>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum PersistedValue {
    Plain(Value),
    Vector(PersistedVector),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedVectorEntry {
    index: VectorIndexRef,
    row_id: contextdb_core::RowId,
    vector: PersistedVector,
    created_tx: contextdb_core::TxId,
    deleted_tx: Option<contextdb_core::TxId>,
    lsn: Lsn,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SinkAuditEntry {
    lsn: Lsn,
    kind: SinkAuditKind,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum SinkAuditKind {
    QueueOverflow { sink: String, dropped_count: u64 },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum PersistedVector {
    F32(Vec<f32>),
    SQ8 {
        min: f32,
        max: f32,
        len: u32,
        payload: Vec<u8>,
    },
    SQ4 {
        min: f32,
        max: f32,
        len: u32,
        payload: Vec<u8>,
    },
}

impl PersistedVector {
    fn from_f32(vector: &[f32], quantization: VectorQuantization) -> Self {
        match quantization {
            VectorQuantization::F32 => PersistedVector::F32(vector.to_vec()),
            VectorQuantization::SQ8 => {
                let (min, max) = vector_min_max(vector);
                let range = max - min;
                let payload = if range <= f32::EPSILON {
                    vec![0; vector.len()]
                } else {
                    vector
                        .iter()
                        .map(|value| {
                            (((*value - min) / range) * 255.0).round().clamp(0.0, 255.0) as u8
                        })
                        .collect()
                };
                PersistedVector::SQ8 {
                    min,
                    max,
                    len: vector.len() as u32,
                    payload,
                }
            }
            VectorQuantization::SQ4 => {
                let (min, max) = vector_min_max(vector);
                let range = max - min;
                let mut payload = Vec::with_capacity(vector.len().div_ceil(2));
                let quantized = if range <= f32::EPSILON {
                    vec![0; vector.len()]
                } else {
                    vector
                        .iter()
                        .map(|value| {
                            (((*value - min) / range) * 15.0).round().clamp(0.0, 15.0) as u8
                        })
                        .collect::<Vec<_>>()
                };
                for pair in quantized.chunks(2) {
                    let hi = pair[0] & 0x0f;
                    let lo = pair.get(1).copied().unwrap_or(0) & 0x0f;
                    payload.push((hi << 4) | lo);
                }
                PersistedVector::SQ4 {
                    min,
                    max,
                    len: vector.len() as u32,
                    payload,
                }
            }
        }
    }

    fn to_f32(&self) -> Vec<f32> {
        match self {
            PersistedVector::F32(vector) => vector.clone(),
            PersistedVector::SQ8 {
                min,
                max,
                len,
                payload,
            } => {
                let range = *max - *min;
                payload
                    .iter()
                    .take(*len as usize)
                    .map(|byte| {
                        if range <= f32::EPSILON {
                            *min
                        } else {
                            *min + ((*byte as f32) / 255.0) * range
                        }
                    })
                    .collect()
            }
            PersistedVector::SQ4 {
                min,
                max,
                len,
                payload,
            } => {
                let range = *max - *min;
                // A malformed durable length must not reserve independently
                // of the bytes that can actually decode. Callers validate
                // the reconstructed dimension and classify a short payload.
                let mut values =
                    Vec::with_capacity(payload.len().saturating_mul(2).min(*len as usize));
                for byte in payload {
                    for q in [byte >> 4, byte & 0x0f] {
                        if values.len() == *len as usize {
                            break;
                        }
                        values.push(if range <= f32::EPSILON {
                            *min
                        } else {
                            *min + ((q as f32) / 15.0) * range
                        });
                    }
                }
                values
            }
        }
    }
}

fn vector_min_max(vector: &[f32]) -> (f32, f32) {
    let Some((first, rest)) = vector.split_first() else {
        return (0.0, 0.0);
    };
    rest.iter()
        .copied()
        .fold((*first, *first), |(min, max), value| {
            (min.min(value), max.max(value))
        })
}

/// The exact `ColumnDef` layout `v1.0.0` (git tag `v1.0.0`, commit `e6cf60c`)
/// wrote to disk — 11 fields, missing `context_id`/`scope_label`/`acl_ref`.
/// See `RedbPersistence::decode_table_meta_versioned` for why a genuinely
/// older-shaped payload needs its own struct rather than leaning on the
/// current `ColumnDef`'s own trailing-field tolerance (which only protects a
/// shorter CURRENT-shaped payload, never one from a release whose struct had
/// fewer fields than today's).
#[derive(Debug, Clone, Deserialize)]
struct LegacyColumnDefV1 {
    name: String,
    column_type: ColumnType,
    nullable: bool,
    primary_key: bool,
    #[serde(default)]
    unique: bool,
    #[serde(default)]
    default: Option<String>,
    #[serde(default)]
    references: Option<ForeignKeyReference>,
    #[serde(default)]
    expires: bool,
    #[serde(default)]
    immutable: bool,
    #[serde(default)]
    quantization: VectorQuantization,
    #[serde(default)]
    rank_policy: Option<RankPolicy>,
}

impl From<LegacyColumnDefV1> for contextdb_core::ColumnDef {
    fn from(legacy: LegacyColumnDefV1) -> Self {
        contextdb_core::ColumnDef {
            name: legacy.name,
            column_type: legacy.column_type,
            nullable: legacy.nullable,
            primary_key: legacy.primary_key,
            unique: legacy.unique,
            default: legacy.default,
            references: legacy.references,
            expires: legacy.expires,
            immutable: legacy.immutable,
            quantization: legacy.quantization,
            rank_policy: legacy.rank_policy,
            // Fields v1.0.0 never wrote: honest, unset defaults.
            context_id: false,
            scope_label: None,
            acl_ref: None,
        }
    }
}

/// The exact `TableMeta` layout `v1.0.0` wrote to disk — 11 fields, missing
/// `composite_foreign_keys`/`sync_direction`/`retain_declared_unit`/
/// `primary_key_columns`/`conflict_policy`.
#[derive(Debug, Clone, Deserialize)]
struct LegacyTableMetaV1 {
    columns: Vec<LegacyColumnDefV1>,
    immutable: bool,
    state_machine: Option<StateMachineConstraint>,
    #[serde(default)]
    dag_edge_types: Vec<String>,
    #[serde(default)]
    unique_constraints: Vec<Vec<String>>,
    natural_key_column: Option<String>,
    #[serde(default)]
    propagation_rules: Vec<PropagationRule>,
    #[serde(default)]
    default_ttl_seconds: Option<u64>,
    #[serde(default)]
    sync_safe: bool,
    #[serde(default)]
    expires_column: Option<String>,
    #[serde(default)]
    indexes: Vec<IndexDecl>,
}

impl From<LegacyTableMetaV1> for TableMeta {
    fn from(legacy: LegacyTableMetaV1) -> Self {
        TableMeta {
            columns: legacy.columns.into_iter().map(Into::into).collect(),
            immutable: legacy.immutable,
            state_machine: legacy.state_machine,
            dag_edge_types: legacy.dag_edge_types,
            unique_constraints: legacy.unique_constraints,
            natural_key_column: legacy.natural_key_column,
            propagation_rules: legacy.propagation_rules,
            default_ttl_seconds: legacy.default_ttl_seconds,
            sync_safe: legacy.sync_safe,
            expires_column: legacy.expires_column,
            indexes: legacy.indexes,
            // Fields v1.0.0 never wrote: honest, unset defaults.
            composite_foreign_keys: Vec::new(),
            sync_direction: None,
            retain_declared_unit: None,
            primary_key_columns: Vec::new(),
            conflict_policy: None,
            history_policy: None,
        }
    }
}

fn replacement_temp_path(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(format!(
        ".replacement-{}.tmp",
        uuid::Uuid::new_v4().simple()
    ));
    PathBuf::from(name)
}

fn create_replacement_placeholder(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    }
    options.open(path).map_err(RedbPersistence::storage_error)
}

fn sync_database_parent(path: &Path) -> Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(RedbPersistence::storage_error)
}

fn open_locked_migration_store(path: &Path) -> Result<File> {
    let before = std::fs::symlink_metadata(path).map_err(RedbPersistence::storage_error)?;
    if before.file_type().is_symlink() || !before.file_type().is_file() {
        return Err(Error::StoreCorrupted {
            path: path.display().to_string(),
            reason: "migration source must be a direct regular file".to_owned(),
        });
    }

    let mut options = OpenOptions::new();
    options.read(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    }
    let file = options.open(path).map_err(RedbPersistence::storage_error)?;
    let opened = file.metadata().map_err(RedbPersistence::storage_error)?;
    let after = std::fs::symlink_metadata(path).map_err(RedbPersistence::storage_error)?;
    if !opened.file_type().is_file()
        || after.file_type().is_symlink()
        || !after.file_type().is_file()
    {
        return Err(Error::StoreCorrupted {
            path: path.display().to_string(),
            reason: "migration source changed while its exact file was opened".to_owned(),
        });
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        if before.dev() != opened.dev()
            || before.ino() != opened.ino()
            || opened.dev() != after.dev()
            || opened.ino() != after.ino()
        {
            return Err(Error::StoreCorrupted {
                path: path.display().to_string(),
                reason: "migration source inode changed while its exact file was opened".to_owned(),
            });
        }
    }
    match fs2::FileExt::try_lock_exclusive(&file) {
        Ok(()) => {
            #[cfg(all(unix, feature = "test-seams"))]
            read_persistence_test_scaffold::note_locked_migration_source_for_test(path, &file);
            Ok(file)
        }
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
            Err(RedbPersistence::held_store_error(path))
        }
        Err(error) => Err(RedbPersistence::storage_error(error)),
    }
}

impl RedbPersistence {
    fn lock_companion_slot(&self) -> Result<std::sync::MutexGuard<'_, Option<CompanionGuard>>> {
        self.lock_file.lock().map_err(|_| Error::StoreCorrupted {
            path: self.path.display().to_string(),
            reason: "in-process companion ownership state was poisoned".to_owned(),
        })
    }

    /// Publish this writer's REAL serving state beside the store, so a later
    /// reader is told why inspection is unavailable instead of meeting this
    /// writer's lock on the committed file and reporting whatever the storage
    /// layer said.
    /// End this writer's claim window now that its inspection channel is up
    /// and can answer for itself. See [`CompanionGuard::close_claim_window`].
    pub(crate) fn close_claim_window(&self) {
        let Ok(guard) = self.lock_companion_slot() else {
            return;
        };
        if let Some(companion) = guard.as_ref() {
            companion.close_claim_window();
        }
    }

    pub(crate) fn record_owner_read_status(&self, status: OwnerReadStatus) -> Result<()> {
        let guard = self.lock_companion_slot()?;
        let Some(companion) = guard.as_ref() else {
            return Ok(());
        };
        let path = self.path.clone();
        companion.record_owner_read_status(&path, status)
    }

    fn lock_database(&self) -> Result<std::sync::MutexGuard<'_, Option<redb::Database>>> {
        self.db.lock().map_err(|_| {
            Error::Other(format!(
                "in-process Redb handle state was poisoned for {}",
                self.path.display()
            ))
        })
    }

    fn lock_migration_source(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, Option<Arc<RedbPersistence>>>> {
        self.migration_replacement_source.lock().map_err(|_| {
            Error::Other(format!(
                "in-process migration replacement state was poisoned for {}",
                self.path.display()
            ))
        })
    }

    /// Acquire migration ownership without opening Redb writable. This is the
    /// only route that can produce a [`LegacyMigrationOpenCapability`], and
    /// the exact locked descriptor it contains is later consumed by
    /// [`Self::open_legacy_migration_capability`].
    pub(crate) fn acquire_legacy_migration_capability(
        path: &Path,
    ) -> Result<LegacyMigrationOpenCapability> {
        let companion_preexists = companion_exists(path)?;
        let (companion, file) = if companion_preexists {
            let companion = CompanionGuard::acquire(path, CompanionAdmission::Existing)?;
            let file = match open_locked_migration_store(path) {
                Ok(file) => file,
                Err(error) => {
                    drop(companion);
                    return Err(error);
                }
            };
            (companion, file)
        } else {
            // A companion-less existing store must prove Redb-level
            // exclusivity before migration creates its permanent companion.
            let file = open_locked_migration_store(path)?;
            let companion = match CompanionGuard::acquire(path, CompanionAdmission::CreateIfMissing)
            {
                Ok(companion) => companion,
                Err(error) => {
                    drop(file);
                    return Err(error);
                }
            };
            (companion, file)
        };
        if let Err(error) = companion.publish_writer_run(path, false) {
            drop(file);
            drop(companion);
            return Err(error);
        }
        // The identity is taken from the descriptor this capability now owns,
        // never from the pathname, so the backup below can prove it is
        // reading the store that was locked.
        #[cfg(unix)]
        let identity = {
            use std::os::unix::fs::MetadataExt as _;
            let metadata = file.metadata().map_err(Self::storage_error)?;
            (metadata.dev(), metadata.ino())
        };
        Ok(LegacyMigrationOpenCapability {
            path: path.to_path_buf(),
            companion: Some(companion),
            file: Some(file),
            #[cfg(unix)]
            identity,
        })
    }

    /// Consume the one migration capability into Redb. The backend owns the
    /// already-exclusively-locked descriptor, so no unrelated same-process
    /// opener can enter between backup completion and writable hydration.
    pub(crate) fn open_legacy_migration_capability(
        mut capability: LegacyMigrationOpenCapability,
    ) -> Result<Self> {
        let path = capability.path.clone();
        let file = capability.file.take().ok_or_else(|| {
            Error::Other("migration store capability was already consumed".to_owned())
        })?;
        let backend = LockedMigrationFileBackend {
            file: Mutex::new(file),
        };
        let opened = Self::open_hook_suppressed(|| redb_builder().create_with_backend(backend));
        let db = match opened {
            Ok(Ok(db)) => db,
            Ok(Err(redb::DatabaseError::DatabaseAlreadyOpen)) => {
                return Err(Self::held_store_error(&path));
            }
            Ok(Err(error)) => return Err(Self::storage_open_error(&path, error)),
            Err(_) => {
                return Err(Error::StoreCorrupted {
                    path: path.display().to_string(),
                    reason: format!(
                        "metadata/format read panicked; store may be truncated or corrupt — {}",
                        Self::CORRUPT_STORE_NEXT_STEP
                    ),
                });
            }
        };
        if let Err(error) = Self::validate_format_marker(&db, &path) {
            drop(db);
            return Err(error);
        }
        let companion = capability.companion.take().ok_or_else(|| {
            Error::Other("migration companion capability was already consumed".to_owned())
        })?;
        Ok(Self {
            path,
            lock_file: Mutex::new(Some(companion)),
            db: Mutex::new(Some(db)),
            migration_replacement_source: Mutex::new(None),
            used_legacy_table_meta_layout: AtomicBool::new(false),
        })
    }

    /// What to tell a caller whose claim on a companion-less path could not be
    /// created at all.
    ///
    /// The claim is what would have let this open judge the file, so without
    /// it the honest thing left is the read-only classification: a legacy
    /// layout is named with the release that reads it, a root that cannot be
    /// decoded is named with the one next step that exists, and a file that
    /// really is a current store leaves the claim's own refusal standing,
    /// because then the directory is the only thing wrong.
    fn refusal_without_a_claim(
        path: &Path,
        claim_error: Error,
        absent: AbsentStoreAnswer,
    ) -> Error {
        // Strictly read-only. Nothing is claimed on this road, and redb's
        // crash repair REWRITES the store it repairs: running it here would
        // rewrite a store this process was just refused the right to hold, and
        // two openers meeting the same crash-dirty root would rewrite it at
        // the same time. So a root that cannot be read without repairing it is
        // simply not judged from here, and the claim's own refusal stands --
        // which is the answer that road gave before the claim moved earlier,
        // with the file left byte-for-byte as it was found.
        match Self::classify_format(path, false, absent) {
            Ok(Some(true)) => Error::LegacyVectorStoreDetected {
                found_format_marker: "legacy or missing durable layout".to_owned(),
                expected_release: CURRENT_FORMAT_VERSION.to_owned(),
            },
            Ok(Some(false)) | Ok(None) => claim_error,
            Err(classification) => classification,
        }
    }

    pub fn create(path: &Path) -> Result<Self> {
        #[cfg(feature = "test-seams")]
        crate::read_probe::note_persistence_writer_open();
        let lock_file = CompanionGuard::acquire(path, CompanionAdmission::NewStore)?;
        Self::create_with_lock(path, lock_file)
    }

    /// Recreate a store while holding its existing coordination lock. The lock
    /// file is never unlinked: its OS lock is the ownership boundary that
    /// prevents another process from opening the path between removal and the
    /// fresh format marker being written.
    pub fn recreate(path: &Path) -> Result<Self> {
        let companion_preexists = companion_exists(path)?;
        let (lock_file, old_db) = if companion_preexists {
            let lock_file = CompanionGuard::acquire(path, CompanionAdmission::Existing)?;
            let old_db = Self::open_outgoing_store(path)?;
            (lock_file, old_db)
        } else {
            // An existing companion-less root is the one lock-order
            // exception: prove Redb exclusivity first so a reader refusal
            // cannot leave a newly-created `.lock` behind.
            let old_db = Self::open_outgoing_store(path)?;
            let lock_file = match CompanionGuard::acquire(path, CompanionAdmission::CreateIfMissing)
            {
                Ok(lock_file) => lock_file,
                Err(error) => {
                    drop(old_db);
                    return Err(error);
                }
            };
            (lock_file, old_db)
        };
        lock_file.publish_writer_run(path, false)?;
        #[cfg(feature = "test-seams")]
        read_persistence_test_scaffold::store_replacement_checkpoint_for_test(
            read_persistence_test_scaffold::StoreReplacementBoundary::GuardAcquired,
        );

        let temp_path = replacement_temp_path(path);
        let temp_file = create_replacement_placeholder(&temp_path)?;
        let created = Self::open_hook_suppressed(|| redb_builder().create_file(temp_file));
        let replacement_db = match created {
            Ok(Ok(database)) => database,
            Ok(Err(error)) => {
                let _ = std::fs::remove_file(&temp_path);
                return Err(Self::storage_open_error(&temp_path, error));
            }
            Err(_) => {
                let _ = std::fs::remove_file(&temp_path);
                return Err(Error::StoreCorrupted {
                    path: temp_path.display().to_string(),
                    reason: "replacement Redb creation panicked".to_owned(),
                });
            }
        };
        if let Err(error) = Self::write_format_marker(&replacement_db) {
            drop(replacement_db);
            let _ = std::fs::remove_file(&temp_path);
            return Err(error);
        }
        if let Err(error) = lock_file.prepare_replacement_database(path, &temp_path) {
            drop(replacement_db);
            let _ = std::fs::remove_file(&temp_path);
            return Err(error);
        }
        #[cfg(feature = "test-seams")]
        read_persistence_test_scaffold::store_replacement_checkpoint_for_test(
            read_persistence_test_scaffold::StoreReplacementBoundary::BeforeAtomicReplacement,
        );
        if let Err(error) = std::fs::rename(&temp_path, path) {
            drop(replacement_db);
            let _ = std::fs::remove_file(&temp_path);
            return Err(Self::storage_error(error));
        }
        #[cfg(feature = "test-seams")]
        read_persistence_test_scaffold::store_replacement_checkpoint_for_test(
            read_persistence_test_scaffold::StoreReplacementBoundary::AfterAtomicReplacement,
        );
        sync_database_parent(path)?;
        lock_file.publish_replacement_database(path)?;
        #[cfg(feature = "test-seams")]
        read_persistence_test_scaffold::store_replacement_checkpoint_for_test(
            read_persistence_test_scaffold::StoreReplacementBoundary::ReplacementPublishedBeforeGuardRelease,
        );
        // Both exclusive Redb handles and the original companion guard have
        // survived every replacement and publication boundary. Only now may
        // the unlinked old-store inode be released.
        drop(old_db);
        Ok(Self {
            path: path.to_path_buf(),
            lock_file: Mutex::new(Some(lock_file)),
            db: Mutex::new(Some(replacement_db)),
            migration_replacement_source: Mutex::new(None),
            used_legacy_table_meta_layout: AtomicBool::new(false),
        })
    }

    fn create_with_lock(path: &Path, lock_file: CompanionGuard) -> Result<Self> {
        // The caller has already established companion-first ownership for a
        // genuinely new database. Reserve the store itself with create-new
        // and no-follow semantics, then hand that exact descriptor to Redb;
        // a path race can therefore never redirect creation through a symlink.
        let store_file = match create_replacement_placeholder(path) {
            Ok(file) => file,
            Err(error) => {
                drop(lock_file);
                return Err(error);
            }
        };
        let created = Self::open_hook_suppressed(|| redb_builder().create_file(store_file));
        let create_result = match created {
            Ok(inner) => inner.map_err(|err| Self::storage_open_error(path, err)),
            Err(_) => Err(Error::StoreCorrupted {
                path: path.display().to_string(),
                reason: format!(
                    "metadata/format read panicked; store may be truncated or corrupt — {}",
                    Self::CORRUPT_STORE_NEXT_STEP
                ),
            }),
        };
        match create_result {
            Ok(db) => match Self::write_format_marker(&db)
                .and_then(|()| lock_file.publish_writer_run(path, true))
            {
                Ok(()) => Ok(Self {
                    path: path.to_path_buf(),
                    lock_file: Mutex::new(Some(lock_file)),
                    db: Mutex::new(Some(db)),
                    migration_replacement_source: Mutex::new(None),
                    used_legacy_table_meta_layout: AtomicBool::new(false),
                }),
                Err(err) => {
                    drop(db);
                    drop(lock_file);
                    Err(err)
                }
            },
            Err(err) => {
                drop(lock_file);
                Err(err)
            }
        }
    }

    pub fn open(path: &Path) -> Result<Self> {
        Self::open_inner(path, AbsentStoreAnswer::Taxonomy)
    }

    /// Open a store that must already be there, creating nothing.
    ///
    /// There is no existence question here and no second door: the ordinary
    /// open is attempted, and the attempt's own "nothing at this path" becomes
    /// [`Error::StoreMissing`] naming the store. Every refusal on this road
    /// leaves the directory as it found it -- including the companion this
    /// open takes for itself while it is still deciding, which is taken back
    /// before the refusal is returned.
    pub fn open_existing_only(path: &Path) -> Result<Self> {
        Self::open_inner(path, AbsentStoreAnswer::Typed)
    }

    fn open_inner(path: &Path, absent: AbsentStoreAnswer) -> Result<Self> {
        #[cfg(feature = "test-seams")]
        crate::read_probe::note_persistence_writer_open();
        #[cfg(feature = "test-seams")]
        crate::read_probe::note_persistence_writer_open_attempt();
        #[cfg(feature = "test-seams")]
        read_persistence_test_scaffold::note_writer_open_attempt_for_test();
        let companion_preexists = companion_exists(path)?;
        let (lock_file, db) = if companion_preexists {
            let lock_file = CompanionGuard::acquire(path, CompanionAdmission::Existing)?;
            match Self::open_db_checked(path, absent) {
                Ok(db) => (lock_file, db),
                Err(error) => {
                    drop(lock_file);
                    return Err(error);
                }
            }
        } else {
            // This road is where ownership and publication are furthest apart.
            // The writer proves the root readable and current through an
            // exclusive writable open, and that open IS the moment it owns the
            // store -- everything a caller could dial comes long afterwards.
            // So the claim is taken FIRST and spans the whole stretch, carrying
            // NO record: the companion codec is untouched and nothing is said
            // about serving. A caller arriving mid-open is told the store is
            // owned and not answering yet, never that nobody owns it.
            //
            // Refusing a legacy or corrupt root must still leave the directory
            // exactly as it was found, and that promise is kept the other way
            // round now: every refusal below takes the companion away again.
            let lock_file = match CompanionGuard::acquire(path, CompanionAdmission::CreateIfMissing)
            {
                Ok(lock_file) => lock_file,
                Err(error) => {
                    // Nothing was claimed. A directory that will not take a
                    // new file is the ordinary cause, and it says nothing
                    // about the FILE the caller pointed at -- which is the
                    // thing they are owed an answer about, with its recovery
                    // instruction, rather than a complaint about companion
                    // creation. Classifying read-only opens nothing writable
                    // and creates nothing, and with no claim in existence an
                    // owner-only caller is still told, truly, that nobody owns
                    // this path. A companion that DOES exist means the claim
                    // was refused by a live holder, and that answer stands.
                    if companion_exists(path).unwrap_or(true) {
                        return Err(error);
                    }
                    return Err(Self::refusal_without_a_claim(path, error, absent));
                }
            };
            // Classify through the real read-only decoder before any writable
            // open, so Redb's housekeeping never alters a file that is about
            // to be refused.
            match Self::legacy_format_verdict(path, absent) {
                Ok(false) => {}
                Ok(true) => {
                    lock_file.discard_created(path);
                    return Err(Error::LegacyVectorStoreDetected {
                        found_format_marker: "legacy or missing durable layout".to_owned(),
                        expected_release: CURRENT_FORMAT_VERSION.to_owned(),
                    });
                }
                Err(error) => {
                    lock_file.discard_created(path);
                    return Err(error);
                }
            }
            let db = match Self::open_db_checked(path, absent) {
                Ok(db) => db,
                Err(error) => {
                    lock_file.discard_created(path);
                    return Err(error);
                }
            };
            if let Err(error) = Self::validate_format_marker(&db, path) {
                drop(db);
                lock_file.discard_created(path);
                return Err(error);
            }
            (lock_file, db)
        };
        let publish = if companion_preexists {
            Self::validate_format_marker(&db, path)
                .and_then(|()| lock_file.publish_writer_run(path, false))
        } else {
            lock_file.publish_writer_run(path, false)
        };
        match publish {
            Ok(()) => Ok(Self {
                path: path.to_path_buf(),
                lock_file: Mutex::new(Some(lock_file)),
                db: Mutex::new(Some(db)),
                migration_replacement_source: Mutex::new(None),
                used_legacy_table_meta_layout: AtomicBool::new(false),
            }),
            Err(err) => {
                drop(db);
                if companion_preexists {
                    drop(lock_file);
                } else {
                    // The companion on that road exists only because this open
                    // created it, so an open that ends here leaves nothing
                    // beside the store either.
                    lock_file.discard_created(path);
                }
                Err(err)
            }
        }
    }

    pub(crate) fn prepare_replacement_database(&self, replacement_path: &Path) -> Result<()> {
        let guard = self.lock_companion_slot()?;
        let guard = guard
            .as_ref()
            .ok_or_else(|| Error::Other("migration source companion is closed".to_owned()))?;
        // Match every ordinary persistence operation's mutex order:
        // companion ownership first, then the Redb handle. Holding both
        // proves the old exclusive store open remains live through intent
        // publication without creating an inverse-order deadlock.
        let db = self.lock_database()?;
        if db.is_none() {
            return Err(Error::Other(
                "closed migration source cannot prepare replacement".to_owned(),
            ));
        }
        guard.prepare_replacement_database(&self.path, replacement_path)
    }

    /// Bind one fresh target to the exact already-open migration source.
    /// Passing the source object is the authority; no pathname lookup can
    /// synthesize or borrow this capability.
    pub(crate) fn arm_migration_replacement_source(
        &self,
        source: Arc<RedbPersistence>,
    ) -> Result<()> {
        let mut slot = self.lock_migration_source()?;
        if slot.is_some() {
            return Err(Error::Other(
                "migration target already has a replacement source".to_owned(),
            ));
        }
        *slot = Some(source);
        Ok(())
    }

    /// Called by the target database's ordinary close path before its Redb
    /// handle is released. This writes the pending generation while both the
    /// source guard/source Redb handle and the complete target remain live.
    pub(crate) fn prepare_armed_migration_replacement(&self) -> Result<()> {
        let source = self.lock_migration_source()?.take();
        match source {
            Some(source) => source.prepare_replacement_database(&self.path),
            None => Ok(()),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn publish_replacement_database(&self) -> Result<()> {
        sync_database_parent(&self.path)?;
        let guard = self.lock_companion_slot()?;
        let guard = guard
            .as_ref()
            .ok_or_else(|| Error::Other("migration source companion is closed".to_owned()))?;
        let db = self.lock_database()?;
        if db.is_none() {
            return Err(Error::Other(
                "closed migration source cannot publish replacement".to_owned(),
            ));
        }
        guard.publish_replacement_database(&self.path)
    }

    /// Complete a prepared migration publication only when the source path
    /// now names the exact target fingerprint. On a pre-swap failure the old
    /// generation remains authoritative and close can release normally.
    pub(crate) fn publish_prepared_replacement_if_current(&self) -> Result<()> {
        sync_database_parent(&self.path)?;
        let guard = self.lock_companion_slot()?;
        let guard = guard
            .as_ref()
            .ok_or_else(|| Error::Other("migration source companion is closed".to_owned()))?;
        let db = self.lock_database()?;
        if db.is_none() {
            return Err(Error::Other(
                "closed migration source cannot publish replacement".to_owned(),
            ));
        }
        guard.publish_replacement_if_current(&self.path)
    }

    pub fn close(&self) {
        let db = self
            .db
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        drop(db);
        let lock_file = self
            .lock_file
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        drop(lock_file);
        let migration_source = self
            .migration_replacement_source
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        drop(migration_source);
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Whether any table's meta on this root decoded only via the legacy
    /// pre-`contextdb migrate` layout (see
    /// [`Self::decode_table_meta_versioned`]) rather than the current one.
    /// Set the first time [`Self::load_all_table_meta`] hits that fallback;
    /// never cleared. `Database::open` consults this to refuse a
    /// schema-level-legacy root instead of silently loading it.
    pub fn used_legacy_table_meta_layout(&self) -> bool {
        self.used_legacy_table_meta_layout.load(Ordering::Relaxed)
    }

    /// Bind the companion to the generation that is actually present at
    /// `path` before redb's crash repair rewrites it. A crashed replacement
    /// leaves a recorded intent naming its target by the target's exact
    /// bytes; that record is what decides whether the surviving store is the
    /// prior generation or the replacement, and therefore which database
    /// identity the reopened store carries. Repairing first would change
    /// those bytes and leave the record matching neither generation, so the
    /// store would come back carrying the other generation's identity.
    ///
    /// A root with no companion, or a companion with nothing recorded, is
    /// left byte-for-byte untouched.
    fn settle_pending_replacement_before_repair(path: &Path) -> Result<()> {
        if !companion_exists(path)? {
            return Ok(());
        }
        let companion = CompanionGuard::acquire(path, CompanionAdmission::Existing)?;
        companion.settle_pending_replacement(path)
    }

    /// Whether the store at `path` is legacy-format: either its top-level
    /// format-version marker does not match [`CURRENT_FORMAT_VERSION`], or
    /// the marker matches but the underlying `TableMeta`/`ColumnDef` schema
    /// layout still only decodes via [`Self::decode_table_meta_versioned`]'s
    /// legacy fallback (the `v1.0.0` case this whole module exists for).
    ///
    /// Decided through a `redb::ReadOnlyDatabase` handle SPECIFICALLY so
    /// this check itself never mutates the file: a plain read-write
    /// `redb::Database::open` performs a housekeeping write on every open —
    /// independent of any application-level transaction — which would break
    /// `contextdb migrate`'s "refuse an already-current-format root
    /// untouched" contract if this detection used one.
    ///
    /// The one exception is a store redb itself refuses to open read-only
    /// because its last writer died mid-run (`RepairAborted`): that root is
    /// unreadable until redb's own crash repair runs, which needs a
    /// read-write handle, so it is re-opened read-write and classified. That
    /// is the crash-recovery path; a cleanly shut store — the only kind a
    /// refused `migrate`/`repair` ever inspects — is never touched.
    pub fn is_legacy_format_store(path: &Path) -> Result<bool> {
        Self::legacy_format_verdict(path, AbsentStoreAnswer::Taxonomy)
    }

    /// The same verdict, told how to answer when the root it went to classify
    /// turns out not to be there at all.
    fn legacy_format_verdict(path: &Path, absent: AbsentStoreAnswer) -> Result<bool> {
        // The ordinary caller of this holds the store, so repairing a
        // crash-dirty root is its business to do.
        Ok(Self::classify_format(path, true, absent)?.unwrap_or(false))
    }

    /// The same classification, told whether it may let redb repair a
    /// crash-dirty root in order to read it.
    ///
    /// `Some` is a verdict; `None` means the root cannot be judged without a
    /// repair the caller is not allowed to run. Only the claim-less road
    /// forbids it, and it forbids it for a reason: redb's crash repair
    /// REWRITES the store, and doing that with no claim held would let two
    /// openers meeting the same crash-dirty root rewrite it at the same time,
    /// and would rewrite a store this process was refused the right to hold.
    fn classify_format(
        path: &Path,
        may_repair: bool,
        absent: AbsentStoreAnswer,
    ) -> Result<Option<bool>> {
        // Which handle answered the classification. A cleanly shut store is
        // read through the untouched read-only handle; only a store redb
        // itself refuses to open read-only is re-opened read-write.
        enum ClassificationHandle {
            CleanlyShut(redb::ReadOnlyDatabase),
            CrashRepaired(redb::Database),
        }
        // Suppressed + `catch_unwind`-guarded exactly like `open_db_checked`'s
        // read-write open: a truncated/corrupt file can trip redb's internal
        // page_manager assertion (a panic, not a clean `Err`) on the
        // READ-ONLY path too, and this call must never let that raw panic
        // (or its backtrace) escape to `repair`/`migrate`'s caller.
        let opened = Self::open_hook_suppressed(|| redb_builder().open_read_only(path));
        let handle = match opened {
            Ok(Ok(db)) => ClassificationHandle::CleanlyShut(db),
            // A store whose last writer died mid-run carries no persisted
            // allocator state (or a header still marked recovery-required),
            // and redb refuses to repair either through a read-only handle:
            // `DatabaseError::RepairAborted`. Classifying exactly that root
            // is the FIRST thing crash recovery has to do -- a store left
            // dirty by a crashed `migrate` is unreadable until redb's own
            // crash repair runs, which needs a read-write handle. So this
            // ONE refusal, and only it, re-opens read-write. A cleanly shut
            // store -- the only kind a refused `migrate`/`repair` inspects --
            // never reaches this arm, so the untouched-detection contract
            // documented above still holds for every healthy root.
            Ok(Err(redb::DatabaseError::RepairAborted)) if !may_repair => return Ok(None),
            Ok(Err(redb::DatabaseError::RepairAborted)) => {
                // redb's crash repair rewrites the store it repairs, and a
                // recorded replacement intent names its generation by that
                // generation's exact bytes. So the companion is settled
                // against the store as the crash left it -- before the
                // repair below changes those bytes -- and the reopened root
                // therefore keeps the database identity bound to the
                // generation the fingerprint selected, instead of the other
                // generation's.
                Self::settle_pending_replacement_before_repair(path)?;
                ClassificationHandle::CrashRepaired(Self::open_db_checked(path, absent)?)
            }
            Ok(Err(err)) => return Err(Self::refused_open_error(path, err, absent)),
            Err(_) => {
                return Err(Error::StoreCorrupted {
                    path: path.display().to_string(),
                    reason: format!(
                        "metadata/format read panicked; store may be truncated or corrupt — {}",
                        Self::CORRUPT_STORE_NEXT_STEP
                    ),
                });
            }
        };
        let read_txn = match &handle {
            ClassificationHandle::CleanlyShut(db) => db.begin_read(),
            ClassificationHandle::CrashRepaired(db) => db.begin_read(),
        }
        .map_err(Self::storage_error)?;
        let format_table = match read_txn.open_table(FORMAT_METADATA_TABLE) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Some(true)),
            Err(err) => return Err(Self::storage_error(err)),
        };
        let Some(marker_bytes) = format_table
            .get(FORMAT_VERSION_KEY)
            .map_err(Self::storage_error)?
        else {
            return Ok(Some(true));
        };
        let marker: String = Self::decode(marker_bytes.value())?;
        if marker != CURRENT_FORMAT_VERSION {
            return Ok(Some(true));
        }

        let meta_table = match read_txn.open_table(META_TABLE) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Some(false)),
            Err(err) => return Err(Self::storage_error(err)),
        };
        for entry in meta_table.iter().map_err(Self::storage_error)? {
            let (key, value) = entry.map_err(Self::storage_error)?;
            if key.value().strip_prefix("table:").is_some() {
                let (_, via_legacy) = Self::decode_table_meta_versioned(value.value())?;
                if via_legacy {
                    return Ok(Some(true));
                }
            }
        }
        Ok(Some(false))
    }

    /// One-line actionable recovery step appended to a corrupt-store error so
    /// the user is never left with a dead end: run `contextdb diagnose` to see
    /// what is salvageable (never modifies the store), or `contextdb reset
    /// --force` to recreate it once you've restored from a backup or a
    /// healthy sync peer if you need the data first.
    pub(crate) const CORRUPT_STORE_NEXT_STEP: &'static str = "run `contextdb diagnose <path>` to see what is \
        salvageable (it never modifies the store), or `contextdb reset <path> --force` to \
        recreate it — restore from a backup or a healthy sync peer first if you need the \
        existing data";

    /// Run a redb store-open closure with the default panic hook suppressed.
    ///
    /// redb can panic (an internal assertion) when a truncated/corrupt file has
    /// a valid-looking header but an inconsistent page layout. `catch_unwind`
    /// catches the unwind, but it does NOT stop the default panic hook from
    /// printing the raw crate location (`redb-x/.../page_manager.rs:NN`) to
    /// stderr first — an implementation leak into the user's terminal. Silence
    /// the hook for the minimal window around the open, then restore it.
    ///
    /// CAVEAT: `set_hook`/`take_hook` are process-global, so the swap is
    /// serialized by `HOOK_SWAP` to stop two concurrent opens from interleaving
    /// (which could permanently leave the no-op hook installed). The window is
    /// kept as tight as possible — only around the one open call. The mutex
    /// cannot protect an UNRELATED concurrent panic on another thread from being
    /// silenced during that tiny window; that is inherent to global-hook
    /// suppression and is accepted.
    fn open_hook_suppressed<T>(f: impl FnOnce() -> T) -> std::thread::Result<T> {
        // Poison-tolerant: a panic is exactly what we wrap, so a poisoned guard
        // is expected — recover the inner `()` and carry on.
        let _guard = HOOK_SWAP.lock().unwrap_or_else(|e| e.into_inner());
        let prev_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let outcome = catch_unwind(AssertUnwindSafe(f));
        std::panic::set_hook(prev_hook);
        outcome
    }

    /// The store a recreate is about to replace, opened only so its exclusivity
    /// is proven and its inode stays held until the replacement is published.
    /// A store whose own bytes cannot be read is exactly what recreate exists
    /// to replace, so that unreadability yields no handle instead of aborting;
    /// every other refusal -- a live holder, a permission wall -- still fails
    /// closed and stops the recreate before anything is changed.
    fn open_outgoing_store(path: &Path) -> Result<Option<redb::Database>> {
        match Self::open_db_checked(path, AbsentStoreAnswer::Taxonomy) {
            Ok(database) => Ok(Some(database)),
            Err(Error::StoreCorrupted { .. }) => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Say what a refused store open means, and what to do about it.
    ///
    /// A person who points a door at a file that cannot be a store is stuck
    /// until they are told what to go and do, so every refusal that is about
    /// the file's contents ends with the one recovery sentence this build
    /// publishes. Contention and permissions are different problems with
    /// different answers, and keep their own wording.
    fn refused_open_error(
        path: &Path,
        err: redb::DatabaseError,
        absent: AbsentStoreAnswer,
    ) -> Error {
        if matches!(err, redb::DatabaseError::DatabaseAlreadyOpen) {
            return Self::held_store_error(path);
        }
        // Nothing at the path is only a corruption verdict if something was
        // supposed to be there. For a door that promised to create nothing it
        // is the answer the caller asked for, and telling them to run a repair
        // on a file that does not exist would be worse than useless.
        if absent == AbsentStoreAnswer::Typed && open_found_no_store(&err) {
            return Error::StoreMissing {
                path: path.display().to_string(),
            };
        }
        if open_was_refused_by_permissions(&err) {
            return Self::permission_refused_open_error(path, err);
        }
        Error::StoreCorrupted {
            path: path.display().to_string(),
            reason: format!(
                "metadata/format could not be read: {err} — {}",
                Self::CORRUPT_STORE_NEXT_STEP
            ),
        }
    }

    fn open_db_checked(path: &Path, absent: AbsentStoreAnswer) -> Result<redb::Database> {
        let opened = Self::open_hook_suppressed(|| redb_builder().open(path));
        match opened {
            Ok(Ok(db)) => Ok(db),
            Ok(Err(err)) => Err(Self::refused_open_error(path, err, absent)),
            Err(_) => Err(Error::StoreCorrupted {
                path: path.display().to_string(),
                reason: format!(
                    "metadata/format read panicked; store may be truncated or corrupt — {}",
                    Self::CORRUPT_STORE_NEXT_STEP
                ),
            }),
        }
    }

    fn write_format_marker(db: &redb::Database) -> Result<()> {
        let write_txn = db.begin_write().map_err(Self::storage_error)?;
        {
            let mut table = write_txn
                .open_table(FORMAT_METADATA_TABLE)
                .map_err(Self::storage_error)?;
            let encoded = Self::encode(&CURRENT_FORMAT_VERSION.to_string())?;
            table
                .insert(FORMAT_VERSION_KEY, encoded.as_slice())
                .map_err(Self::storage_error)?;
        }
        write_txn.commit().map_err(Self::storage_error)?;
        Ok(())
    }

    fn validate_format_marker(db: &redb::Database, path: &Path) -> Result<()> {
        let read_txn = db.begin_read().map_err(|err| Error::StoreCorrupted {
            path: path.display().to_string(),
            reason: format!(
                "metadata read failed: {err} — {}",
                Self::CORRUPT_STORE_NEXT_STEP
            ),
        })?;
        let table = match read_txn.open_table(FORMAT_METADATA_TABLE) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => {
                return Err(Error::LegacyVectorStoreDetected {
                    found_format_marker: String::new(),
                    expected_release: CURRENT_FORMAT_VERSION.to_string(),
                });
            }
            Err(err) => {
                return Err(Error::StoreCorrupted {
                    path: path.display().to_string(),
                    reason: format!(
                        "metadata table could not be read: {err} — {}",
                        Self::CORRUPT_STORE_NEXT_STEP
                    ),
                });
            }
        };
        let value = table
            .get(FORMAT_VERSION_KEY)
            .map_err(|err| Error::StoreCorrupted {
                path: path.display().to_string(),
                reason: format!(
                    "metadata format_version could not be read: {err} — {}",
                    Self::CORRUPT_STORE_NEXT_STEP
                ),
            })?
            .ok_or_else(|| Error::StoreCorrupted {
                path: path.display().to_string(),
                reason: format!(
                    "metadata table is missing format_version — {}",
                    Self::CORRUPT_STORE_NEXT_STEP
                ),
            })?;
        let marker: String = Self::decode(value.value()).map_err(|err| Error::StoreCorrupted {
            path: path.display().to_string(),
            reason: format!(
                "metadata format_version is corrupt: {err} — {}",
                Self::CORRUPT_STORE_NEXT_STEP
            ),
        })?;
        if marker == CURRENT_FORMAT_VERSION {
            Ok(())
        } else {
            Err(Error::LegacyVectorStoreDetected {
                found_format_marker: marker,
                expected_release: CURRENT_FORMAT_VERSION.to_string(),
            })
        }
    }

    pub fn flush_data(&self, ws: &WriteSet) -> Result<()> {
        self.flush_data_with_logs(ws, &[])
    }

    pub fn flush_data_with_logs(&self, ws: &WriteSet, change_log: &[ChangeLogEntry]) -> Result<()> {
        self.flush_data_with_logs_and_sink_events(
            ws,
            change_log,
            FlushDataOptions {
                sink_events: &[],
                trigger_audits: &[],
                schema_ddl: SchemaDdlPersistence {
                    event_bus: None,
                    trigger: None,
                },
                max_sink_queue_depth: usize::MAX,
                snapshots: FlushDataSnapshots::default(),
                received_schema: None,
            },
        )
    }

    pub(crate) fn flush_data_with_logs_and_sink_events(
        &self,
        ws: &WriteSet,
        change_log: &[ChangeLogEntry],
        options: FlushDataOptions<'_>,
    ) -> Result<()> {
        let table_meta = match options.snapshots.table_meta {
            Some(table_meta) => table_meta,
            None => self.load_all_table_meta()?,
        };
        let deleted_rows_snapshot = options.snapshots.deleted_rows;
        let sink_events = options.sink_events;
        let trigger_audits = options.trigger_audits;
        let schema_ddl = options.schema_ddl;
        let max_sink_queue_depth = options.max_sink_queue_depth;
        if let Some(received_schema) = options.received_schema {
            return self.flush_received_schema_stage(
                ws,
                change_log,
                received_schema,
                sink_events,
                trigger_audits,
                max_sink_queue_depth,
            );
        }
        let has_vector_changes = !ws.vector_deletes.is_empty()
            || !ws.vector_inserts.is_empty()
            || !ws.vector_moves.is_empty();
        let vector_quantization = if has_vector_changes {
            Self::vector_quantization_map(&table_meta)
        } else {
            HashMap::new()
        };
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;

            let mut relational_deletes_by_table = BTreeMap::<&str, Vec<(RowId, TxId)>>::new();
            for (table, row_id, deleted_tx) in &ws.relational_deletes {
                relational_deletes_by_table
                    .entry(table.as_str())
                    .or_default()
                    .push((*row_id, *deleted_tx));
            }
            for (table, deletes) in relational_deletes_by_table {
                let table_name = Self::rel_table_name(table);
                let table_def: TableDefinition<&[u8], &[u8]> =
                    TableDefinition::new(table_name.as_str());
                let mut redb_table = write_txn
                    .open_table(table_def)
                    .map_err(Self::storage_error)?;
                let mut encoded = Vec::new();
                for (row_id, deleted_tx) in deletes {
                    if let Some(row) = deleted_rows_snapshot
                        .as_ref()
                        .and_then(|by_table| by_table.get(table))
                        .and_then(|by_row| by_row.get(&row_id))
                    {
                        if row.deleted_tx.is_some() {
                            continue;
                        }
                        let key = Self::rel_row_key(row);
                        Self::encode_versioned_row_with_deleted_tx_into(
                            row,
                            Some(deleted_tx),
                            table_meta.get(table),
                            &mut encoded,
                        )?;
                        redb_table
                            .insert(key.as_slice(), encoded.as_slice())
                            .map_err(Self::storage_error)?;
                        continue;
                    }
                    let (lower, upper) = Self::rel_row_key_range(row_id);
                    let latest_version = {
                        let mut range = redb_table
                            .range(lower.as_slice()..upper.as_slice())
                            .map_err(Self::storage_error)?;
                        range
                            .next_back()
                            .transpose()
                            .map_err(Self::storage_error)?
                            .map(|(key, value)| {
                                let row = Self::decode_versioned_row(
                                    value.value(),
                                    table_meta.get(table),
                                )?;
                                Ok::<_, Error>((key.value().to_vec(), row))
                            })
                            .transpose()?
                    };
                    let Some((key, mut row)) = latest_version else {
                        continue;
                    };
                    if row.row_id != row_id || row.deleted_tx.is_some() {
                        continue;
                    }
                    row.deleted_tx = Some(deleted_tx);
                    Self::encode_versioned_row_into(&row, table_meta.get(table), &mut encoded)?;
                    redb_table
                        .insert(key.as_slice(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }

            let mut relational_inserts_by_table = BTreeMap::<&str, Vec<&VersionedRow>>::new();
            for (table, row) in &ws.relational_inserts {
                relational_inserts_by_table
                    .entry(table.as_str())
                    .or_default()
                    .push(row);
            }
            for (table, rows) in relational_inserts_by_table {
                let table_name = Self::rel_table_name(table);
                let table_def: TableDefinition<&[u8], &[u8]> =
                    TableDefinition::new(table_name.as_str());
                let mut redb_table = write_txn
                    .open_table(table_def)
                    .map_err(Self::storage_error)?;
                let mut encoded = Vec::new();
                for row in rows {
                    Self::encode_versioned_row_into(row, table_meta.get(table), &mut encoded)?;
                    let key = Self::rel_row_key(row);
                    redb_table
                        .insert(key.as_slice(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }

            let (clear_source_lsns, set_source_lsns) = sync_source_lsn_updates(ws);
            if !clear_source_lsns.is_empty() || !set_source_lsns.is_empty() {
                let mut source_lsn_table = write_txn
                    .open_table(SYNC_ROW_SOURCE_LSN_TABLE)
                    .map_err(Self::storage_error)?;
                let mut source_kind_table = write_txn
                    .open_table(SYNC_ROW_SOURCE_KIND_TABLE)
                    .map_err(Self::storage_error)?;
                for (table, row_id) in clear_source_lsns {
                    let key = Self::sync_row_source_lsn_key(&table, row_id);
                    source_lsn_table
                        .remove(key.as_slice())
                        .map_err(Self::storage_error)?;
                    source_kind_table
                        .remove(key.as_slice())
                        .map_err(Self::storage_error)?;
                }
                for (table, row_id, source_lsn, kind) in set_source_lsns {
                    let key = Self::sync_row_source_lsn_key(&table, row_id);
                    source_lsn_table
                        .insert(key.as_slice(), source_lsn.0)
                        .map_err(Self::storage_error)?;
                    source_kind_table
                        .insert(key.as_slice(), kind)
                        .map_err(Self::storage_error)?;
                }
            }

            if !ws.adj_deletes.is_empty() || !ws.adj_inserts.is_empty() {
                let mut fwd_table = write_txn
                    .open_table(GRAPH_FWD_TABLE)
                    .map_err(Self::storage_error)?;
                let mut rev_table = write_txn
                    .open_table(GRAPH_REV_TABLE)
                    .map_err(Self::storage_error)?;

                let mut encoded = Vec::new();
                for (source, edge_type, target, deleted_tx) in &ws.adj_deletes {
                    let mut live_versions = Vec::new();
                    for entry in fwd_table.iter().map_err(Self::storage_error)? {
                        let (key, value) = entry.map_err(Self::storage_error)?;
                        let edge: AdjEntry = Self::decode(value.value())?;
                        if edge.source == *source
                            && edge.target == *target
                            && edge.edge_type == *edge_type
                            && edge.deleted_tx.is_none()
                        {
                            live_versions.push((key.value().to_vec(), edge));
                        }
                    }
                    if live_versions.is_empty() {
                        return Err(Error::NotFound(format!(
                            "edge {source} -[{edge_type}]-> {target} in graph_fwd"
                        )));
                    }
                    for (fwd_key, mut edge) in live_versions {
                        edge.deleted_tx = Some(*deleted_tx);
                        Self::encode_into(&edge, &mut encoded)?;
                        let rev_key = Self::graph_rev_key(&edge);

                        fwd_table
                            .insert(fwd_key.as_slice(), encoded.as_slice())
                            .map_err(Self::storage_error)?;
                        rev_table
                            .insert(rev_key.as_slice(), encoded.as_slice())
                            .map_err(Self::storage_error)?;
                    }
                }

                for entry in &ws.adj_inserts {
                    Self::encode_into(entry, &mut encoded)?;
                    let fwd_key = Self::graph_fwd_key(entry);
                    let rev_key = Self::graph_rev_key(entry);
                    fwd_table
                        .insert(fwd_key.as_slice(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                    rev_table
                        .insert(rev_key.as_slice(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }

            if has_vector_changes {
                let mut vectors_table = write_txn
                    .open_table(VECTORS_TABLE)
                    .map_err(Self::storage_error)?;

                for (index, row_id, deleted_tx) in &ws.vector_deletes {
                    let mut live_versions = Vec::new();
                    for entry in vectors_table.iter().map_err(Self::storage_error)? {
                        let (key, value) = entry.map_err(Self::storage_error)?;
                        let vector_entry = Self::decode_vector_entry(value.value())?;
                        if vector_entry.index == *index
                            && vector_entry.row_id == *row_id
                            && vector_entry.deleted_tx.is_none()
                        {
                            live_versions.push((key.value().to_vec(), vector_entry));
                        }
                    }
                    if live_versions.is_empty() {
                        return Err(Error::NotFound(format!("vector row {row_id}")));
                    }
                    for (key, mut entry) in live_versions {
                        entry.deleted_tx = Some(*deleted_tx);
                        let quantization = vector_quantization
                            .get(&entry.index)
                            .copied()
                            .unwrap_or_default();
                        let encoded = Self::encode_vector_entry(&entry, quantization)?;
                        vectors_table
                            .insert(key.as_slice(), encoded.as_slice())
                            .map_err(Self::storage_error)?;
                    }
                }

                for entry in &ws.vector_inserts {
                    let quantization = vector_quantization
                        .get(&entry.index)
                        .copied()
                        .unwrap_or_default();
                    let encoded = Self::encode_vector_entry(entry, quantization)?;
                    let key = Self::vector_key(entry);
                    vectors_table
                        .insert(key.as_slice(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }

                for (index, old_row_id, new_row_id, tx) in &ws.vector_moves {
                    let mut live_versions = Vec::new();
                    for entry in vectors_table.iter().map_err(Self::storage_error)? {
                        let (key, value) = entry.map_err(Self::storage_error)?;
                        let vector_entry = Self::decode_vector_entry(value.value())?;
                        if vector_entry.index == *index
                            && vector_entry.row_id == *old_row_id
                            && vector_entry.deleted_tx.is_none()
                        {
                            live_versions.push((key.value().to_vec(), vector_entry));
                        }
                    }
                    if live_versions.is_empty() {
                        return Err(Error::NotFound(format!("vector row {old_row_id}")));
                    }
                    for (old_key, mut old_entry) in live_versions {
                        old_entry.deleted_tx = Some(*tx);
                        let quantization = vector_quantization
                            .get(&old_entry.index)
                            .copied()
                            .unwrap_or_default();
                        let old_encoded = Self::encode_vector_entry(&old_entry, quantization)?;
                        vectors_table
                            .insert(old_key.as_slice(), old_encoded.as_slice())
                            .map_err(Self::storage_error)?;

                        let mut new_entry = old_entry;
                        new_entry.row_id = *new_row_id;
                        new_entry.created_tx = *tx;
                        new_entry.deleted_tx = None;
                        new_entry.lsn = ws.commit_lsn.unwrap_or(Lsn(0));
                        let new_key = Self::vector_key(&new_entry);
                        let new_encoded = Self::encode_vector_entry(&new_entry, quantization)?;
                        vectors_table
                            .insert(new_key.as_slice(), new_encoded.as_slice())
                            .map_err(Self::storage_error)?;
                    }
                }
            }

            if let (Some(lsn), Some(tx)) = (ws.commit_lsn, Self::write_set_visibility_tx(ws)) {
                let mut table = write_txn
                    .open_table(COMMIT_INDEX_TABLE)
                    .map_err(Self::storage_error)?;
                table.insert(lsn.0, tx.0).map_err(Self::storage_error)?;
            }

            if !change_log.is_empty() {
                let mut table = write_txn
                    .open_table(CHANGE_LOG_TABLE)
                    .map_err(Self::storage_error)?;
                let lsn = ws.commit_lsn.unwrap_or(Lsn(0));
                let mut encoded = Vec::new();
                let mut key = String::with_capacity(Self::change_log_entry_key_len());
                for (index, entry) in change_log.iter().enumerate() {
                    Self::write_change_log_entry_key(lsn, index, &mut key);
                    Self::encode_into(entry, &mut encoded)?;
                    table
                        .insert(key.as_str(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }

            if !sink_events.is_empty() {
                let lsn = ws.commit_lsn.unwrap_or(Lsn(0));
                let mut overflow_audits = Vec::new();
                let mut by_sink: BTreeMap<&str, Vec<(usize, &SinkQueueEntry)>> = BTreeMap::new();
                for (index, (sink, entry)) in sink_events.iter().enumerate() {
                    by_sink
                        .entry(sink.as_str())
                        .or_default()
                        .push((index, entry));
                }
                for (sink, entries) in by_sink {
                    let table_name = Self::sink_queue_table_name(sink);
                    let table_def: TableDefinition<u64, &[u8]> =
                        TableDefinition::new(table_name.as_str());
                    let mut table = write_txn
                        .open_table(table_def)
                        .map_err(Self::storage_error)?;
                    let mut existing = table.len().map_err(Self::storage_error)? as usize;
                    for (index, entry) in entries {
                        let overflow_id = if existing >= max_sink_queue_depth {
                            table
                                .first()
                                .map_err(Self::storage_error)?
                                .map(|(key, _)| key.value())
                        } else {
                            None
                        };
                        if let Some(id) = overflow_id {
                            table.remove(id).map_err(Self::storage_error)?;
                            existing = existing.saturating_sub(1);
                            overflow_audits.push((index, sink.to_string()));
                        }
                        let encoded = Self::encode(entry)?;
                        table
                            .insert(entry.id, encoded.as_slice())
                            .map_err(Self::storage_error)?;
                        existing = existing.saturating_add(1);
                    }
                }
                if !overflow_audits.is_empty() {
                    let mut audit_table = write_txn
                        .open_table(SINK_AUDIT_TABLE)
                        .map_err(Self::storage_error)?;
                    for (index, sink) in overflow_audits {
                        let key = Self::sink_audit_key(lsn, index, &sink);
                        let audit = SinkAuditEntry {
                            lsn,
                            kind: SinkAuditKind::QueueOverflow {
                                sink,
                                dropped_count: 1,
                            },
                        };
                        let encoded = Self::encode(&audit)?;
                        audit_table
                            .insert(key.as_str(), encoded.as_slice())
                            .map_err(Self::storage_error)?;
                    }
                }
            }

            if !trigger_audits.is_empty() {
                {
                    let mut audit_table = write_txn
                        .open_table(TRIGGER_AUDIT_TABLE)
                        .map_err(Self::storage_error)?;
                    let mut stamps = write_txn
                        .open_table(TRIGGER_AUDIT_STAMPS_TABLE)
                        .map_err(Self::storage_error)?;
                    let stamped_at = Wallclock::now().0;
                    for (index, entry) in trigger_audits {
                        let key = Self::trigger_audit_key(*index, &entry.trigger_name);
                        let encoded = Self::encode(entry)?;
                        audit_table
                            .insert(key.as_str(), encoded.as_slice())
                            .map_err(Self::storage_error)?;
                        stamps
                            .insert(key.as_str(), stamped_at)
                            .map_err(Self::storage_error)?;
                    }
                }
                {
                    let mut config_table = write_txn
                        .open_table(CONFIG_TABLE)
                        .map_err(Self::storage_error)?;
                    let mut ring: Vec<TriggerAuditEntry> = config_table
                        .get(TRIGGER_AUDIT_RING_CONFIG_KEY)
                        .map_err(Self::storage_error)?
                        .map(|value| Self::decode(value.value()))
                        .transpose()?
                        .unwrap_or_default();
                    ring.extend(trigger_audits.iter().map(|(_, entry)| entry.clone()));
                    let overflow = ring
                        .len()
                        .saturating_sub(crate::database::trigger::TRIGGER_AUDIT_RING_CAPACITY);
                    if overflow > 0 {
                        ring.drain(0..overflow);
                    }
                    let current_next_index = config_table
                        .get(TRIGGER_AUDIT_NEXT_INDEX_CONFIG_KEY)
                        .map_err(Self::storage_error)?
                        .map(|value| Self::decode::<u64>(value.value()))
                        .transpose()?
                        .unwrap_or(0);
                    let next_index = trigger_audits
                        .iter()
                        .map(|(index, _)| index.saturating_add(1))
                        .max()
                        .unwrap_or(0)
                        .max(current_next_index);
                    let encoded_ring = Self::encode(&ring)?;
                    config_table
                        .insert(TRIGGER_AUDIT_RING_CONFIG_KEY, encoded_ring.as_slice())
                        .map_err(Self::storage_error)?;
                    let encoded_next_index = Self::encode(&next_index)?;
                    config_table
                        .insert(
                            TRIGGER_AUDIT_NEXT_INDEX_CONFIG_KEY,
                            encoded_next_index.as_slice(),
                        )
                        .map_err(Self::storage_error)?;
                }
            }

            if schema_ddl.event_bus.is_some() || schema_ddl.trigger.is_some() {
                {
                    let mut config_table = write_txn
                        .open_table(CONFIG_TABLE)
                        .map_err(Self::storage_error)?;
                    if let Some(event_bus_ddl) = schema_ddl.event_bus {
                        for (key, encoded) in &event_bus_ddl.config_values {
                            config_table
                                .insert(key.as_str(), encoded.as_slice())
                                .map_err(Self::storage_error)?;
                        }
                    }
                    if let Some(trigger_ddl) = schema_ddl.trigger {
                        for (key, encoded) in &trigger_ddl.config_values {
                            config_table
                                .insert(key.as_str(), encoded.as_slice())
                                .map_err(Self::storage_error)?;
                        }
                    }
                }
                let ddl_entries = schema_ddl
                    .event_bus
                    .into_iter()
                    .flat_map(|commit| commit.ddl.iter())
                    .chain(
                        schema_ddl
                            .trigger
                            .into_iter()
                            .flat_map(|commit| commit.ddl.iter()),
                    )
                    .collect::<Vec<_>>();
                if !ddl_entries.is_empty() {
                    let lsn = ws.commit_lsn.unwrap_or(Lsn(0));
                    let mut ddl_table = write_txn
                        .open_table(DDL_LOG_TABLE)
                        .map_err(Self::storage_error)?;
                    for (index, change) in ddl_entries.iter().enumerate() {
                        let key = Self::ddl_log_key_for_index(lsn, index, ddl_entries.len());
                        let encoded = Self::encode(change)?;
                        ddl_table
                            .insert(key.as_str(), encoded.as_slice())
                            .map_err(Self::storage_error)?;
                    }
                }
            }

            if !ws.config_writes.is_empty() {
                let mut config_table = write_txn
                    .open_table(CONFIG_TABLE)
                    .map_err(Self::storage_error)?;
                for (key, encoded) in &ws.config_writes {
                    let encoded = if ws.config_max_u64_keys.iter().any(|max_key| max_key == key) {
                        let incoming = Self::decode::<u64>(encoded)?;
                        let current = config_table
                            .get(key.as_str())
                            .map_err(Self::storage_error)?
                            .map(|value| Self::decode::<u64>(value.value()))
                            .transpose()?
                            .unwrap_or(0);
                        Self::encode(&current.max(incoming))?
                    } else {
                        encoded.clone()
                    };
                    config_table
                        .insert(key.as_str(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }

            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    /// Commit a complete received-schema projection in the transaction that
    /// also records its finalized WriteSet identity.  The normal incremental
    /// writer cannot be used here: a DROP or same-name recreation needs the
    /// durable image to become the prepared full replacement atomically.
    fn flush_received_schema_stage(
        &self,
        ws: &WriteSet,
        change_log: &[ChangeLogEntry],
        stage: &ReceivedSchemaPersistenceProjection,
        sink_events: &[PreparedSinkEvent],
        trigger_audits: &[(u64, TriggerAuditEntry)],
        max_sink_queue_depth: usize,
    ) -> Result<()> {
        let prior_tables = self.load_all_table_meta()?;
        let vector_quantization = Self::vector_quantization_map(&stage.table_meta);
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;

            {
                let mut meta_table = write_txn
                    .open_table(META_TABLE)
                    .map_err(Self::storage_error)?;
                for table in prior_tables.keys() {
                    let key = Self::meta_key(table);
                    meta_table
                        .remove(key.as_str())
                        .map_err(Self::storage_error)?;
                }
                for (table, meta) in &stage.table_meta {
                    let key = Self::meta_key(table);
                    let encoded = Self::encode(meta)?;
                    meta_table
                        .insert(key.as_str(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }

            let mut relational_tables = prior_tables
                .keys()
                .chain(stage.table_meta.keys())
                .collect::<HashSet<_>>()
                .into_iter()
                .cloned()
                .collect::<Vec<_>>();
            relational_tables.sort();
            relational_tables.dedup();
            for table in relational_tables {
                let table_name = Self::rel_table_name(&table);
                let table_def: TableDefinition<&[u8], &[u8]> =
                    TableDefinition::new(table_name.as_str());
                match write_txn.delete_table(table_def) {
                    Ok(_) | Err(redb::TableError::TableDoesNotExist(_)) => {}
                    Err(redb::TableError::TableTypeMismatch { .. }) => {}
                    Err(error) => return Err(Self::storage_error(error)),
                }
                let legacy_table_def: TableDefinition<u64, &[u8]> =
                    TableDefinition::new(table_name.as_str());
                match write_txn.delete_table(legacy_table_def) {
                    Ok(_) | Err(redb::TableError::TableDoesNotExist(_)) => {}
                    Err(redb::TableError::TableTypeMismatch { .. }) => {}
                    Err(error) => return Err(Self::storage_error(error)),
                }
            }
            for (table, rows) in &stage.rows {
                let table_name = Self::rel_table_name(table);
                let table_def: TableDefinition<&[u8], &[u8]> =
                    TableDefinition::new(table_name.as_str());
                let mut redb_table = write_txn
                    .open_table(table_def)
                    .map_err(Self::storage_error)?;
                for row in rows {
                    let encoded = Self::encode_versioned_row(row, stage.table_meta.get(table))?;
                    let key = Self::rel_row_key(row);
                    redb_table
                        .insert(key.as_slice(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }

            let _ = write_txn.delete_table(SYNC_ROW_SOURCE_LSN_TABLE);
            let _ = write_txn.delete_table(SYNC_ROW_SOURCE_KIND_TABLE);
            {
                let mut lsn_table = write_txn
                    .open_table(SYNC_ROW_SOURCE_LSN_TABLE)
                    .map_err(Self::storage_error)?;
                let mut kind_table = write_txn
                    .open_table(SYNC_ROW_SOURCE_KIND_TABLE)
                    .map_err(Self::storage_error)?;
                for (table, row_id, lsn, kind) in &stage.sync_sources {
                    let key = Self::sync_row_source_lsn_key(table, *row_id);
                    lsn_table
                        .insert(key.as_slice(), lsn.0)
                        .map_err(Self::storage_error)?;
                    kind_table
                        .insert(key.as_slice(), sync_source_kind_to_u8(*kind))
                        .map_err(Self::storage_error)?;
                }
            }

            let _ = write_txn.delete_table(GRAPH_FWD_TABLE);
            let _ = write_txn.delete_table(GRAPH_REV_TABLE);
            {
                let mut fwd = write_txn
                    .open_table(GRAPH_FWD_TABLE)
                    .map_err(Self::storage_error)?;
                let mut rev = write_txn
                    .open_table(GRAPH_REV_TABLE)
                    .map_err(Self::storage_error)?;
                for edge in &stage.edges {
                    let encoded = Self::encode(edge)?;
                    let fwd_key = Self::graph_fwd_key(edge);
                    let rev_key = Self::graph_rev_key(edge);
                    fwd.insert(fwd_key.as_slice(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                    rev.insert(rev_key.as_slice(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }

            let _ = write_txn.delete_table(VECTORS_TABLE);
            {
                let mut vectors = write_txn
                    .open_table(VECTORS_TABLE)
                    .map_err(Self::storage_error)?;
                for entry in &stage.vectors {
                    let quantization = vector_quantization
                        .get(&entry.index)
                        .copied()
                        .unwrap_or_default();
                    let encoded = Self::encode_vector_entry(entry, quantization)?;
                    let key = Self::vector_key(entry);
                    vectors
                        .insert(key.as_slice(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }

            let _ = write_txn.delete_table(DDL_LOG_TABLE);
            {
                let mut ddl_table = write_txn
                    .open_table(DDL_LOG_TABLE)
                    .map_err(Self::storage_error)?;
                let mut grouped = BTreeMap::<Lsn, Vec<&DdlChange>>::new();
                for (lsn, ddl) in &stage.ddl_log {
                    grouped.entry(*lsn).or_default().push(ddl);
                }
                for (lsn, ddl) in grouped {
                    for (index, change) in ddl.iter().enumerate() {
                        let key = Self::ddl_log_key_for_index(lsn, index, ddl.len());
                        let encoded = Self::encode(*change)?;
                        ddl_table
                            .insert(key.as_str(), encoded.as_slice())
                            .map_err(Self::storage_error)?;
                    }
                }
            }

            if let (Some(lsn), Some(tx)) = (ws.commit_lsn, Self::write_set_visibility_tx(ws)) {
                let mut index = write_txn
                    .open_table(COMMIT_INDEX_TABLE)
                    .map_err(Self::storage_error)?;
                index.insert(lsn.0, tx.0).map_err(Self::storage_error)?;
            }
            if !stage.structurally_dropped_tables.is_empty() {
                // Decode first and point-remove second: Redb does not permit
                // mutating the table while its iterator holds value borrows.
                // Both phases remain inside this received-schema write
                // transaction, so a later failure cannot leave a new schema
                // image paired with old-generation change history.
                let retired_keys = {
                    let log = write_txn
                        .open_table(CHANGE_LOG_TABLE)
                        .map_err(Self::storage_error)?;
                    let mut retired_keys = Vec::new();
                    for item in log.iter().map_err(Self::storage_error)? {
                        let (key, value) = item.map_err(Self::storage_error)?;
                        let entry = Self::decode::<ChangeLogEntry>(value.value())?;
                        if Self::change_log_entry_references_any_table(
                            &entry,
                            &stage.structurally_dropped_tables,
                        ) {
                            retired_keys.push(key.value().to_string());
                        }
                    }
                    retired_keys
                };
                if !retired_keys.is_empty() {
                    let mut log = write_txn
                        .open_table(CHANGE_LOG_TABLE)
                        .map_err(Self::storage_error)?;
                    for key in retired_keys {
                        log.remove(key.as_str()).map_err(Self::storage_error)?;
                    }
                }
            }
            if !change_log.is_empty() {
                let mut log = write_txn
                    .open_table(CHANGE_LOG_TABLE)
                    .map_err(Self::storage_error)?;
                let lsn = ws.commit_lsn.unwrap_or(Lsn(0));
                let mut key = String::with_capacity(Self::change_log_entry_key_len());
                let mut encoded = Vec::new();
                for (index, entry) in change_log.iter().enumerate() {
                    Self::write_change_log_entry_key(lsn, index, &mut key);
                    Self::encode_into(entry, &mut encoded)?;
                    log.insert(key.as_str(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }
            {
                let mut config = write_txn
                    .open_table(CONFIG_TABLE)
                    .map_err(Self::storage_error)?;
                for (key, value) in &stage.config_values {
                    let value = if stage
                        .config_max_u64_keys
                        .iter()
                        .any(|max_key| max_key == key)
                    {
                        let incoming = Self::decode::<u64>(value)?;
                        let current = config
                            .get(key.as_str())
                            .map_err(Self::storage_error)?
                            .map(|value| Self::decode::<u64>(value.value()))
                            .transpose()?
                            .unwrap_or(0);
                        Self::encode(&current.max(incoming))?
                    } else {
                        value.clone()
                    };
                    config
                        .insert(key.as_str(), value.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }
            // The complete received-schema core image now exists only inside
            // this live Redb transaction. Sink queues and trigger audits for
            // received rows belong immediately below this checkpoint and must
            // commit with that image, never in a later transaction.
            #[cfg(test)]
            if take_received_schema_side_effect_persistence_fault_for_test() {
                return Err(Error::Other(
                    "injected received-schema side-effect persistence failure".to_string(),
                ));
            }
            if !sink_events.is_empty() {
                let lsn = ws.commit_lsn.unwrap_or(Lsn(0));
                let mut overflow_audits = Vec::new();
                let mut by_sink: BTreeMap<&str, Vec<(usize, &SinkQueueEntry)>> = BTreeMap::new();
                for (index, (sink, entry)) in sink_events.iter().enumerate() {
                    by_sink
                        .entry(sink.as_str())
                        .or_default()
                        .push((index, entry));
                }
                for (sink, entries) in by_sink {
                    let table_name = Self::sink_queue_table_name(sink);
                    let table_def: TableDefinition<u64, &[u8]> =
                        TableDefinition::new(table_name.as_str());
                    let mut table = write_txn
                        .open_table(table_def)
                        .map_err(Self::storage_error)?;
                    let mut existing = table.len().map_err(Self::storage_error)? as usize;
                    for (index, entry) in entries {
                        let overflow_id = if existing >= max_sink_queue_depth {
                            table
                                .first()
                                .map_err(Self::storage_error)?
                                .map(|(key, _)| key.value())
                        } else {
                            None
                        };
                        if let Some(id) = overflow_id {
                            table.remove(id).map_err(Self::storage_error)?;
                            existing = existing.saturating_sub(1);
                            overflow_audits.push((index, sink.to_string()));
                        }
                        let encoded = Self::encode(entry)?;
                        table
                            .insert(entry.id, encoded.as_slice())
                            .map_err(Self::storage_error)?;
                        existing = existing.saturating_add(1);
                    }
                }
                if !overflow_audits.is_empty() {
                    let mut audit_table = write_txn
                        .open_table(SINK_AUDIT_TABLE)
                        .map_err(Self::storage_error)?;
                    for (index, sink) in overflow_audits {
                        let key = Self::sink_audit_key(lsn, index, &sink);
                        let audit = SinkAuditEntry {
                            lsn,
                            kind: SinkAuditKind::QueueOverflow {
                                sink,
                                dropped_count: 1,
                            },
                        };
                        let encoded = Self::encode(&audit)?;
                        audit_table
                            .insert(key.as_str(), encoded.as_slice())
                            .map_err(Self::storage_error)?;
                    }
                }
            }
            if !trigger_audits.is_empty() {
                {
                    let mut audit_table = write_txn
                        .open_table(TRIGGER_AUDIT_TABLE)
                        .map_err(Self::storage_error)?;
                    let mut stamps = write_txn
                        .open_table(TRIGGER_AUDIT_STAMPS_TABLE)
                        .map_err(Self::storage_error)?;
                    let stamped_at = Wallclock::now().0;
                    for (index, entry) in trigger_audits {
                        let key = Self::trigger_audit_key(*index, &entry.trigger_name);
                        let encoded = Self::encode(entry)?;
                        audit_table
                            .insert(key.as_str(), encoded.as_slice())
                            .map_err(Self::storage_error)?;
                        stamps
                            .insert(key.as_str(), stamped_at)
                            .map_err(Self::storage_error)?;
                    }
                }
                {
                    let mut config_table = write_txn
                        .open_table(CONFIG_TABLE)
                        .map_err(Self::storage_error)?;
                    let mut ring: Vec<TriggerAuditEntry> = config_table
                        .get(TRIGGER_AUDIT_RING_CONFIG_KEY)
                        .map_err(Self::storage_error)?
                        .map(|value| Self::decode(value.value()))
                        .transpose()?
                        .unwrap_or_default();
                    ring.extend(trigger_audits.iter().map(|(_, entry)| entry.clone()));
                    let overflow = ring
                        .len()
                        .saturating_sub(crate::database::trigger::TRIGGER_AUDIT_RING_CAPACITY);
                    if overflow > 0 {
                        ring.drain(0..overflow);
                    }
                    let current_next_index = config_table
                        .get(TRIGGER_AUDIT_NEXT_INDEX_CONFIG_KEY)
                        .map_err(Self::storage_error)?
                        .map(|value| Self::decode::<u64>(value.value()))
                        .transpose()?
                        .unwrap_or(0);
                    let next_index = trigger_audits
                        .iter()
                        .map(|(index, _)| index.saturating_add(1))
                        .max()
                        .unwrap_or(0)
                        .max(current_next_index);
                    let encoded_ring = Self::encode(&ring)?;
                    config_table
                        .insert(TRIGGER_AUDIT_RING_CONFIG_KEY, encoded_ring.as_slice())
                        .map_err(Self::storage_error)?;
                    let encoded_next_index = Self::encode(&next_index)?;
                    config_table
                        .insert(
                            TRIGGER_AUDIT_NEXT_INDEX_CONFIG_KEY,
                            encoded_next_index.as_slice(),
                        )
                        .map_err(Self::storage_error)?;
                }
            }
            if take_received_schema_pre_commit_fault_for_test() {
                return Err(Error::Other(
                    "injected received-schema Redb pre-commit failure".to_string(),
                ));
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    /// Persist a locally authored schema stage without replacing the received
    /// schema image. The caller has already prepared the exact affected table
    /// metadata and encoded sidecars; this transaction deliberately leaves
    /// all unrelated relation rows and durable state untouched.
    pub(crate) fn flush_local_schema_stage(
        &self,
        ws: &WriteSet,
        stage: &LocalSchemaPersistenceProjection,
    ) -> Result<()> {
        let lsn = ws.commit_lsn.ok_or_else(|| {
            Error::Other("local schema stage is missing its commit LSN".to_string())
        })?;
        if stage.ddl.is_empty() {
            return Err(Error::Other(
                "local schema stage contains no DDL".to_string(),
            ));
        }
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;

            {
                let mut meta_table = write_txn
                    .open_table(META_TABLE)
                    .map_err(Self::storage_error)?;
                for (table, meta) in &stage.affected_table_meta {
                    let key = Self::meta_key(table);
                    let encoded = Self::encode(meta)?;
                    meta_table
                        .insert(key.as_str(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }

            // `open_table` is both the Redb table creation path and a no-op
            // for an existing relation. Do not delete, scan, or rewrite rows:
            // another committed local table must remain byte-for-byte intact.
            for table in stage.affected_table_meta.keys() {
                let table_name = Self::rel_table_name(table);
                let table_def: TableDefinition<&[u8], &[u8]> =
                    TableDefinition::new(table_name.as_str());
                let _relation = write_txn
                    .open_table(table_def)
                    .map_err(Self::storage_error)?;
            }

            {
                let mut config_table = write_txn
                    .open_table(CONFIG_TABLE)
                    .map_err(Self::storage_error)?;
                for (key, encoded) in &stage.config_values {
                    config_table
                        .insert(key.as_str(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }

            {
                let mut ddl_table = write_txn
                    .open_table(DDL_LOG_TABLE)
                    .map_err(Self::storage_error)?;
                for (index, change) in stage.ddl.iter().enumerate() {
                    let key = Self::ddl_log_key_for_index(lsn, index, stage.ddl.len());
                    let encoded = Self::encode(change)?;
                    ddl_table
                        .insert(key.as_str(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }

            {
                let mut index = write_txn
                    .open_table(COMMIT_INDEX_TABLE)
                    .map_err(Self::storage_error)?;
                index
                    .insert(lsn.0, stage.commit_index_tx.0)
                    .map_err(Self::storage_error)?;
            }

            if take_received_schema_pre_commit_fault_for_test() {
                return Err(Error::Other(
                    "injected received-schema Redb pre-commit failure".to_string(),
                ));
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    fn change_log_entry_references_any_table(
        entry: &ChangeLogEntry,
        tables: &HashSet<String>,
    ) -> bool {
        match entry {
            ChangeLogEntry::RowInsert { table, .. } | ChangeLogEntry::RowDelete { table, .. } => {
                tables.contains(table)
            }
            ChangeLogEntry::VectorInsert { index, .. }
            | ChangeLogEntry::VectorDelete { index, .. } => tables.contains(&index.table),
            ChangeLogEntry::EdgeInsert { .. } | ChangeLogEntry::EdgeDelete { .. } => false,
        }
    }
    pub fn flush_table_meta(&self, name: &str, meta: &TableMeta) -> Result<()> {
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            {
                let mut meta_table = write_txn
                    .open_table(META_TABLE)
                    .map_err(Self::storage_error)?;
                let key = Self::meta_key(name);
                let encoded = Self::encode(meta)?;
                meta_table
                    .insert(key.as_str(), encoded.as_slice())
                    .map_err(Self::storage_error)?;
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    /// Durable CREATE TABLE foundation: the table declaration, its one
    /// authoritative schema generation, immutable DDL provenance, and the
    /// matching DDL log entry must become visible together. This prevents
    /// CREATE from retaining the former meta-then-log crash window.
    pub fn flush_table_meta_with_config_values_and_append_ddl_log(
        &self,
        name: &str,
        meta: &TableMeta,
        config_values: Vec<(&str, Vec<u8>)>,
        lsn: Lsn,
        ddl: &DdlChange,
    ) -> Result<()> {
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            {
                let mut meta_table = write_txn
                    .open_table(META_TABLE)
                    .map_err(Self::storage_error)?;
                let key = Self::meta_key(name);
                let encoded = Self::encode(meta)?;
                meta_table
                    .insert(key.as_str(), encoded.as_slice())
                    .map_err(Self::storage_error)?;
            }
            {
                let mut config_table = write_txn
                    .open_table(CONFIG_TABLE)
                    .map_err(Self::storage_error)?;
                for (key, encoded) in config_values {
                    config_table
                        .insert(key, encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }
            {
                let mut ddl_table = write_txn
                    .open_table(DDL_LOG_TABLE)
                    .map_err(Self::storage_error)?;
                let key = Self::ddl_log_key(lsn);
                let encoded = Self::encode(ddl)?;
                ddl_table
                    .insert(key.as_str(), encoded.as_slice())
                    .map_err(Self::storage_error)?;
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    pub fn remove_table_meta(&self, name: &str) -> Result<()> {
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            {
                let mut meta_table = write_txn
                    .open_table(META_TABLE)
                    .map_err(Self::storage_error)?;
                let key = Self::meta_key(name);
                meta_table
                    .remove(key.as_str())
                    .map_err(Self::storage_error)?;
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    pub fn flush_config_value<T: serde::Serialize>(&self, key: &str, value: &T) -> Result<()> {
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            {
                let mut config_table = write_txn
                    .open_table(CONFIG_TABLE)
                    .map_err(Self::storage_error)?;
                let encoded = Self::encode(value)?;
                config_table
                    .insert(key, encoded.as_slice())
                    .map_err(Self::storage_error)?;
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    pub(crate) fn encode_config_value<T: serde::Serialize>(value: &T) -> Result<Vec<u8>> {
        Self::encode(value)
    }

    pub(crate) fn decode_config_value<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
        Self::decode(bytes)
    }

    /// Strict config decode for callers whose durable bytes are themselves an
    /// authority boundary. Unlike the compatibility decoder above, a valid
    /// prefix followed by stray bytes is corrupt rather than silently accepted.
    pub(crate) fn decode_config_value_exact<T: serde::de::DeserializeOwned>(
        bytes: &[u8],
    ) -> Result<T> {
        Self::decode_exact(bytes)
    }

    pub fn flush_encoded_config_values_and_append_ddl_log(
        &self,
        config_values: Vec<(&str, Vec<u8>)>,
        lsn: Lsn,
        ddl: &[DdlChange],
    ) -> Result<()> {
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            {
                let mut config_table = write_txn
                    .open_table(CONFIG_TABLE)
                    .map_err(Self::storage_error)?;
                for (key, encoded) in config_values {
                    config_table
                        .insert(key, encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }
            {
                let mut ddl_table = write_txn
                    .open_table(DDL_LOG_TABLE)
                    .map_err(Self::storage_error)?;
                for (index, change) in ddl.iter().enumerate() {
                    let key = Self::ddl_log_key_for_index(lsn, index, ddl.len());
                    let encoded = Self::encode(change)?;
                    ddl_table
                        .insert(key.as_str(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    pub fn flush_encoded_config_values(&self, config_values: Vec<(&str, Vec<u8>)>) -> Result<()> {
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            {
                let mut config_table = write_txn
                    .open_table(CONFIG_TABLE)
                    .map_err(Self::storage_error)?;
                for (key, encoded) in config_values {
                    config_table
                        .insert(key, encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    pub fn remove_config_value(&self, key: &str) -> Result<()> {
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            {
                let mut config_table = write_txn
                    .open_table(CONFIG_TABLE)
                    .map_err(Self::storage_error)?;
                config_table.remove(key).map_err(Self::storage_error)?;
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    pub fn load_sink_queue<T: serde::de::DeserializeOwned>(&self, sink: &str) -> Result<Vec<T>> {
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let table_name = Self::sink_queue_table_name(sink);
            let table_def: TableDefinition<u64, &[u8]> = TableDefinition::new(table_name.as_str());
            let table = match read_txn.open_table(table_def) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                Err(err) => return Err(Self::storage_error(err)),
            };
            let mut entries = Vec::new();
            for entry in table.iter().map_err(Self::storage_error)? {
                let (_, value) = entry.map_err(Self::storage_error)?;
                entries.push(Self::decode(value.value())?);
            }
            Ok(entries)
        })
    }

    pub fn update_sink_queue_entry<T: serde::Serialize>(
        &self,
        sink: &str,
        remove_id: Option<u64>,
        put: Option<(u64, &T)>,
    ) -> Result<()> {
        if remove_id.is_none() && put.is_none() {
            return Ok(());
        }
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            {
                let table_name = Self::sink_queue_table_name(sink);
                let table_def: TableDefinition<u64, &[u8]> =
                    TableDefinition::new(table_name.as_str());
                let mut table = write_txn
                    .open_table(table_def)
                    .map_err(Self::storage_error)?;
                if let Some(id) = remove_id {
                    table.remove(id).map_err(Self::storage_error)?;
                }
                if let Some((id, entry)) = put {
                    let encoded = Self::encode(entry)?;
                    table
                        .insert(id, encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    pub fn remove_sink_queue_entries(&self, sink: &str, remove_ids: &[u64]) -> Result<()> {
        if remove_ids.is_empty() {
            return Ok(());
        }
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            {
                let table_name = Self::sink_queue_table_name(sink);
                let table_def: TableDefinition<u64, &[u8]> =
                    TableDefinition::new(table_name.as_str());
                let mut table = write_txn
                    .open_table(table_def)
                    .map_err(Self::storage_error)?;
                for id in remove_ids {
                    table.remove(*id).map_err(Self::storage_error)?;
                }
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    pub fn append_change_log(&self, lsn: Lsn, entries: &[ChangeLogEntry]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            {
                let mut table = write_txn
                    .open_table(CHANGE_LOG_TABLE)
                    .map_err(Self::storage_error)?;
                let mut encoded = Vec::new();
                let mut key = String::with_capacity(Self::change_log_entry_key_len());
                for (index, entry) in entries.iter().enumerate() {
                    Self::write_change_log_entry_key(lsn, index, &mut key);
                    Self::encode_into(entry, &mut encoded)?;
                    table
                        .insert(key.as_str(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    pub fn append_ddl_log(&self, lsn: Lsn, change: &DdlChange) -> Result<()> {
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            {
                let mut table = write_txn
                    .open_table(DDL_LOG_TABLE)
                    .map_err(Self::storage_error)?;
                let key = Self::ddl_log_key(lsn);
                let encoded = Self::encode(change)?;
                table
                    .insert(key.as_str(), encoded.as_slice())
                    .map_err(Self::storage_error)?;
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    pub fn append_trigger_audit(&self, index: u64, entry: &TriggerAuditEntry) -> Result<()> {
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            {
                let mut table = write_txn
                    .open_table(TRIGGER_AUDIT_TABLE)
                    .map_err(Self::storage_error)?;
                let key = Self::trigger_audit_key(index, &entry.trigger_name);
                let encoded = Self::encode(entry)?;
                table
                    .insert(key.as_str(), encoded.as_slice())
                    .map_err(Self::storage_error)?;
                let mut stamps = write_txn
                    .open_table(TRIGGER_AUDIT_STAMPS_TABLE)
                    .map_err(Self::storage_error)?;
                stamps
                    .insert(key.as_str(), Wallclock::now().0)
                    .map_err(Self::storage_error)?;
            }
            {
                let mut config_table = write_txn
                    .open_table(CONFIG_TABLE)
                    .map_err(Self::storage_error)?;
                let mut ring: Vec<TriggerAuditEntry> = config_table
                    .get(TRIGGER_AUDIT_RING_CONFIG_KEY)
                    .map_err(Self::storage_error)?
                    .map(|value| Self::decode(value.value()))
                    .transpose()?
                    .unwrap_or_default();
                ring.push(entry.clone());
                let overflow = ring
                    .len()
                    .saturating_sub(crate::database::trigger::TRIGGER_AUDIT_RING_CAPACITY);
                if overflow > 0 {
                    ring.drain(0..overflow);
                }
                let current_next_index = config_table
                    .get(TRIGGER_AUDIT_NEXT_INDEX_CONFIG_KEY)
                    .map_err(Self::storage_error)?
                    .map(|value| Self::decode::<u64>(value.value()))
                    .transpose()?
                    .unwrap_or(0);
                let next_index = index.saturating_add(1).max(current_next_index);
                let encoded_ring = Self::encode(&ring)?;
                config_table
                    .insert(TRIGGER_AUDIT_RING_CONFIG_KEY, encoded_ring.as_slice())
                    .map_err(Self::storage_error)?;
                let encoded_next_index = Self::encode(&next_index)?;
                config_table
                    .insert(
                        TRIGGER_AUDIT_NEXT_INDEX_CONFIG_KEY,
                        encoded_next_index.as_slice(),
                    )
                    .map_err(Self::storage_error)?;
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    pub fn remove_table_data(&self, name: &str) -> Result<()> {
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            let table_name = Self::rel_table_name(name);
            let table_def: TableDefinition<&[u8], &[u8]> =
                TableDefinition::new(table_name.as_str());
            match write_txn.delete_table(table_def) {
                Ok(_) | Err(redb::TableError::TableDoesNotExist(_)) => {}
                Err(redb::TableError::TableTypeMismatch { .. }) => {}
                Err(err) => return Err(Self::storage_error(err)),
            }
            let legacy_table_def: TableDefinition<u64, &[u8]> =
                TableDefinition::new(table_name.as_str());
            match write_txn.delete_table(legacy_table_def) {
                Ok(_) | Err(redb::TableError::TableDoesNotExist(_)) => {}
                Err(redb::TableError::TableTypeMismatch { .. }) => {}
                Err(err) => return Err(Self::storage_error(err)),
            }
            Self::remove_sync_source_provenance_for_table(&write_txn, name)?;
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    pub fn remove_table_with_config_values_and_ddl_log(
        &self,
        name: &str,
        config_values: Vec<(&str, Vec<u8>)>,
        lsn: Lsn,
        ddl: &[DdlChange],
    ) -> Result<()> {
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            {
                let mut meta_table = write_txn
                    .open_table(META_TABLE)
                    .map_err(Self::storage_error)?;
                let key = Self::meta_key(name);
                meta_table
                    .remove(key.as_str())
                    .map_err(Self::storage_error)?;
            }
            let table_name = Self::rel_table_name(name);
            let table_def: TableDefinition<&[u8], &[u8]> =
                TableDefinition::new(table_name.as_str());
            match write_txn.delete_table(table_def) {
                Ok(_) | Err(redb::TableError::TableDoesNotExist(_)) => {}
                Err(redb::TableError::TableTypeMismatch { .. }) => {}
                Err(err) => return Err(Self::storage_error(err)),
            }
            let legacy_table_def: TableDefinition<u64, &[u8]> =
                TableDefinition::new(table_name.as_str());
            match write_txn.delete_table(legacy_table_def) {
                Ok(_) | Err(redb::TableError::TableDoesNotExist(_)) => {}
                Err(redb::TableError::TableTypeMismatch { .. }) => {}
                Err(err) => return Err(Self::storage_error(err)),
            }
            Self::remove_sync_source_provenance_for_table(&write_txn, name)?;
            if !config_values.is_empty() {
                let mut config_table = write_txn
                    .open_table(CONFIG_TABLE)
                    .map_err(Self::storage_error)?;
                for (key, encoded) in config_values {
                    config_table
                        .insert(key, encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }
            if !ddl.is_empty() {
                let mut ddl_table = write_txn
                    .open_table(DDL_LOG_TABLE)
                    .map_err(Self::storage_error)?;
                for (index, change) in ddl.iter().enumerate() {
                    let key = Self::ddl_log_key_for_index(lsn, index, ddl.len());
                    let encoded = Self::encode(change)?;
                    ddl_table
                        .insert(key.as_str(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    pub fn remove_table_rewrite_aux_with_config_values_and_ddl_log(
        &self,
        name: &str,
        config_values: Vec<(&str, Vec<u8>)>,
        lsn: Lsn,
        ddl: &[DdlChange],
        graph_edges: &[AdjEntry],
        vectors: &[VectorEntry],
    ) -> Result<()> {
        let mut table_meta = self.load_all_table_meta()?;
        table_meta.remove(name);
        let vector_quantization = Self::vector_quantization_map(&table_meta);
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            {
                let mut meta_table = write_txn
                    .open_table(META_TABLE)
                    .map_err(Self::storage_error)?;
                let key = Self::meta_key(name);
                meta_table
                    .remove(key.as_str())
                    .map_err(Self::storage_error)?;
            }
            let table_name = Self::rel_table_name(name);
            let table_def: TableDefinition<&[u8], &[u8]> =
                TableDefinition::new(table_name.as_str());
            match write_txn.delete_table(table_def) {
                Ok(_) | Err(redb::TableError::TableDoesNotExist(_)) => {}
                Err(redb::TableError::TableTypeMismatch { .. }) => {}
                Err(err) => return Err(Self::storage_error(err)),
            }
            let legacy_table_def: TableDefinition<u64, &[u8]> =
                TableDefinition::new(table_name.as_str());
            match write_txn.delete_table(legacy_table_def) {
                Ok(_) | Err(redb::TableError::TableDoesNotExist(_)) => {}
                Err(redb::TableError::TableTypeMismatch { .. }) => {}
                Err(err) => return Err(Self::storage_error(err)),
            }
            Self::remove_sync_source_provenance_for_table(&write_txn, name)?;

            let _ = write_txn.delete_table(GRAPH_FWD_TABLE);
            let _ = write_txn.delete_table(GRAPH_REV_TABLE);
            {
                let mut fwd_table = write_txn
                    .open_table(GRAPH_FWD_TABLE)
                    .map_err(Self::storage_error)?;
                let mut rev_table = write_txn
                    .open_table(GRAPH_REV_TABLE)
                    .map_err(Self::storage_error)?;
                for entry in graph_edges {
                    let encoded = Self::encode(entry)?;
                    let fwd_key = Self::graph_fwd_key(entry);
                    let rev_key = Self::graph_rev_key(entry);
                    fwd_table
                        .insert(fwd_key.as_slice(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                    rev_table
                        .insert(rev_key.as_slice(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }

            let _ = write_txn.delete_table(VECTORS_TABLE);
            {
                let mut table = write_txn
                    .open_table(VECTORS_TABLE)
                    .map_err(Self::storage_error)?;
                for entry in vectors {
                    let quantization = vector_quantization
                        .get(&entry.index)
                        .copied()
                        .unwrap_or_default();
                    let encoded = Self::encode_vector_entry(entry, quantization)?;
                    let key = Self::vector_key(entry);
                    table
                        .insert(key.as_slice(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }

            if !config_values.is_empty() {
                let mut config_table = write_txn
                    .open_table(CONFIG_TABLE)
                    .map_err(Self::storage_error)?;
                for (key, encoded) in config_values {
                    config_table
                        .insert(key, encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }
            if !ddl.is_empty() {
                let mut ddl_table = write_txn
                    .open_table(DDL_LOG_TABLE)
                    .map_err(Self::storage_error)?;
                for (index, change) in ddl.iter().enumerate() {
                    let key = Self::ddl_log_key_for_index(lsn, index, ddl.len());
                    let encoded = Self::encode(change)?;
                    ddl_table
                        .insert(key.as_str(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    pub fn rewrite_table_rows(&self, name: &str, rows: &[VersionedRow]) -> Result<()> {
        let table_meta = self.load_all_table_meta()?;
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            {
                let table_name = Self::rel_table_name(name);
                let table_def: TableDefinition<&[u8], &[u8]> =
                    TableDefinition::new(table_name.as_str());
                let _ = write_txn.delete_table(table_def);
                let legacy_table_def: TableDefinition<u64, &[u8]> =
                    TableDefinition::new(table_name.as_str());
                let _ = write_txn.delete_table(legacy_table_def);
                let mut redb_table = write_txn
                    .open_table(table_def)
                    .map_err(Self::storage_error)?;
                for row in rows {
                    let encoded = Self::encode_versioned_row(row, table_meta.get(name))?;
                    let key = Self::rel_row_key(row);
                    redb_table
                        .insert(key.as_slice(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }
            Self::retain_sync_source_provenance_for_table_rows(&write_txn, name, rows)?;
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    pub fn rewrite_vectors(&self, vectors: &[VectorEntry]) -> Result<()> {
        let table_meta = self.load_all_table_meta()?;
        let vector_quantization = Self::vector_quantization_map(&table_meta);
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            let _ = write_txn.delete_table(VECTORS_TABLE);
            {
                let mut table = write_txn
                    .open_table(VECTORS_TABLE)
                    .map_err(Self::storage_error)?;
                for entry in vectors {
                    let quantization = vector_quantization
                        .get(&entry.index)
                        .copied()
                        .unwrap_or_default();
                    let encoded = Self::encode_vector_entry(entry, quantization)?;
                    let key = Self::vector_key(entry);
                    table
                        .insert(key.as_slice(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "one schema rewrite transaction must replace metadata, rows, vectors, config, and its DDL log together"
    )]
    pub fn rewrite_table_meta_rows_vectors_and_append_ddl_log(
        &self,
        name: &str,
        meta: &TableMeta,
        rows: &[VersionedRow],
        vectors: &[VectorEntry],
        lsn: Lsn,
        ddl: &[DdlChange],
        config_values: Vec<(&str, Vec<u8>)>,
    ) -> Result<()> {
        let mut table_meta = self.load_all_table_meta()?;
        table_meta.insert(name.to_string(), meta.clone());
        let vector_quantization = Self::vector_quantization_map(&table_meta);
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            {
                let mut meta_table = write_txn
                    .open_table(META_TABLE)
                    .map_err(Self::storage_error)?;
                let key = Self::meta_key(name);
                let encoded = Self::encode(meta)?;
                meta_table
                    .insert(key.as_str(), encoded.as_slice())
                    .map_err(Self::storage_error)?;
            }
            {
                let table_name = Self::rel_table_name(name);
                let table_def: TableDefinition<&[u8], &[u8]> =
                    TableDefinition::new(table_name.as_str());
                let _ = write_txn.delete_table(table_def);
                let legacy_table_def: TableDefinition<u64, &[u8]> =
                    TableDefinition::new(table_name.as_str());
                let _ = write_txn.delete_table(legacy_table_def);
                let mut redb_table = write_txn
                    .open_table(table_def)
                    .map_err(Self::storage_error)?;
                for row in rows {
                    let encoded = Self::encode_versioned_row(row, Some(meta))?;
                    let key = Self::rel_row_key(row);
                    redb_table
                        .insert(key.as_slice(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }
            Self::retain_sync_source_provenance_for_table_rows(&write_txn, name, rows)?;
            let _ = write_txn.delete_table(VECTORS_TABLE);
            {
                let mut table = write_txn
                    .open_table(VECTORS_TABLE)
                    .map_err(Self::storage_error)?;
                for entry in vectors {
                    let quantization = vector_quantization
                        .get(&entry.index)
                        .copied()
                        .unwrap_or_default();
                    let encoded = Self::encode_vector_entry(entry, quantization)?;
                    let key = Self::vector_key(entry);
                    table
                        .insert(key.as_slice(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }
            {
                let mut config_table = write_txn
                    .open_table(CONFIG_TABLE)
                    .map_err(Self::storage_error)?;
                for (key, encoded) in config_values {
                    config_table
                        .insert(key, encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }
            {
                let mut ddl_table = write_txn
                    .open_table(DDL_LOG_TABLE)
                    .map_err(Self::storage_error)?;
                for (index, change) in ddl.iter().enumerate() {
                    let key = Self::ddl_log_key_for_index(lsn, index, ddl.len());
                    let encoded = Self::encode(change)?;
                    ddl_table
                        .insert(key.as_str(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    pub fn rewrite_graph_edges(&self, edges: &[AdjEntry]) -> Result<()> {
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            let _ = write_txn.delete_table(GRAPH_FWD_TABLE);
            let _ = write_txn.delete_table(GRAPH_REV_TABLE);
            {
                let mut fwd_table = write_txn
                    .open_table(GRAPH_FWD_TABLE)
                    .map_err(Self::storage_error)?;
                let mut rev_table = write_txn
                    .open_table(GRAPH_REV_TABLE)
                    .map_err(Self::storage_error)?;

                for entry in edges {
                    let encoded = Self::encode(entry)?;
                    let fwd_key = Self::graph_fwd_key(entry);
                    let rev_key = Self::graph_rev_key(entry);
                    fwd_table
                        .insert(fwd_key.as_slice(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                    rev_table
                        .insert(rev_key.as_slice(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    pub fn load_all_table_meta(&self) -> Result<HashMap<String, TableMeta>> {
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let meta_table = match read_txn.open_table(META_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(HashMap::new()),
                Err(err) => return Err(Self::storage_error(err)),
            };

            let mut tables = HashMap::new();
            for entry in meta_table.iter().map_err(Self::storage_error)? {
                let (key, value) = entry.map_err(Self::storage_error)?;
                let key = key.value();
                if let Some(name) = key.strip_prefix("table:") {
                    let (meta, via_legacy) = Self::decode_table_meta_versioned(value.value())?;
                    if via_legacy {
                        self.used_legacy_table_meta_layout
                            .store(true, Ordering::Relaxed);
                    }
                    tables.insert(name.to_string(), meta);
                }
            }
            Ok(tables)
        })
    }

    /// Every config value whose key starts with `prefix`, decoded, in key order.
    /// The config keys are sorted, so a range from `prefix` and a stop at the
    /// first key that no longer shares it collects exactly the contiguous run.
    pub fn load_config_values_with_prefix<T: serde::de::DeserializeOwned>(
        &self,
        prefix: &str,
    ) -> Result<Vec<(String, T)>> {
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let config_table = match read_txn.open_table(CONFIG_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                Err(err) => return Err(Self::storage_error(err)),
            };
            let mut out = Vec::new();
            for entry in config_table.range(prefix..).map_err(Self::storage_error)? {
                let (key, value) = entry.map_err(Self::storage_error)?;
                let key = key.value();
                if !key.starts_with(prefix) {
                    break;
                }
                out.push((key.to_string(), Self::decode(value.value())?));
            }
            Ok(out)
        })
    }

    /// Every raw config value whose key starts with `prefix`, in key order.
    /// Callers that treat the stored bytes as an authority boundary can apply
    /// their own exact decoder without broadening generic config compatibility.
    pub(crate) fn load_config_values_raw_with_prefix(
        &self,
        prefix: &str,
    ) -> Result<Vec<(String, Vec<u8>)>> {
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let config_table = match read_txn.open_table(CONFIG_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                Err(err) => return Err(Self::storage_error(err)),
            };
            let mut out = Vec::new();
            for entry in config_table.range(prefix..).map_err(Self::storage_error)? {
                let (key, value) = entry.map_err(Self::storage_error)?;
                let key = key.value();
                if !key.starts_with(prefix) {
                    break;
                }
                out.push((key.to_string(), value.value().to_vec()));
            }
            Ok(out)
        })
    }

    pub fn load_config_value<T: serde::de::DeserializeOwned>(
        &self,
        key: &str,
    ) -> Result<Option<T>> {
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let config_table = match read_txn.open_table(CONFIG_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
                Err(err) => return Err(Self::storage_error(err)),
            };
            let value = match config_table.get(key).map_err(Self::storage_error)? {
                Some(value) => Some(Self::decode(value.value())?),
                None => None,
            };
            Ok(value)
        })
    }

    pub fn load_relational_table(&self, name: &str) -> Result<Vec<VersionedRow>> {
        let (rows, migrate_legacy_table) = self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let table_meta = Self::load_table_meta_in_read_txn(&read_txn, name)?;
            let table_name = Self::rel_table_name(name);
            let table_def: TableDefinition<&[u8], &[u8]> =
                TableDefinition::new(table_name.as_str());
            match read_txn.open_table(table_def) {
                Ok(table) => {
                    let mut rows = Vec::new();
                    for entry in table.iter().map_err(Self::storage_error)? {
                        let (_, value) = entry.map_err(Self::storage_error)?;
                        rows.push(Self::decode_versioned_row(
                            value.value(),
                            table_meta.as_ref(),
                        )?);
                    }
                    return Ok((rows, false));
                }
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok((Vec::new(), false)),
                Err(redb::TableError::TableTypeMismatch { .. }) => {}
                Err(err) => return Err(Self::storage_error(err)),
            };

            let legacy_table_def: TableDefinition<u64, &[u8]> =
                TableDefinition::new(table_name.as_str());
            let table = match read_txn.open_table(legacy_table_def) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok((Vec::new(), false)),
                Err(err) => return Err(Self::storage_error(err)),
            };
            let mut rows = Vec::new();
            for entry in table.iter().map_err(Self::storage_error)? {
                let (_, value) = entry.map_err(Self::storage_error)?;
                rows.push(Self::decode_versioned_row(
                    value.value(),
                    table_meta.as_ref(),
                )?);
            }
            Ok((rows, true))
        })?;
        if migrate_legacy_table {
            self.rewrite_table_rows(name, &rows)?;
        }
        Ok(rows)
    }

    pub fn load_all_tables(&self) -> Result<HashMap<String, Vec<VersionedRow>>> {
        let mut all_tables = HashMap::new();
        for name in self.load_all_table_meta()?.into_keys() {
            let rows = self.load_relational_table(&name)?;
            all_tables.insert(name, rows);
        }
        Ok(all_tables)
    }

    pub fn load_forward_edges(&self) -> Result<Vec<AdjEntry>> {
        self.load_graph_table(GRAPH_FWD_TABLE)
    }

    pub fn load_reverse_edges(&self) -> Result<Vec<AdjEntry>> {
        self.load_graph_table(GRAPH_REV_TABLE)
    }

    pub fn load_vectors(&self) -> Result<Vec<VectorEntry>> {
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let table = match read_txn.open_table(VECTORS_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                Err(err) => return Err(Self::storage_error(err)),
            };

            let mut vectors = Vec::new();
            for entry in table.iter().map_err(Self::storage_error)? {
                let (_, value) = entry.map_err(Self::storage_error)?;
                vectors.push(Self::decode_vector_entry(value.value())?);
            }
            Ok(vectors)
        })
    }

    pub fn load_sync_source_lsns(&self) -> Result<HashMap<(String, RowId), Lsn>> {
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let table = match read_txn.open_table(SYNC_ROW_SOURCE_LSN_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(HashMap::new()),
                Err(err) => return Err(Self::storage_error(err)),
            };

            let mut source_lsns = HashMap::new();
            for entry in table.iter().map_err(Self::storage_error)? {
                let (key, value) = entry.map_err(Self::storage_error)?;
                if let Some((table, row_id)) = Self::decode_sync_row_source_lsn_key(key.value()) {
                    source_lsns.insert((table, row_id), Lsn(value.value()));
                }
            }
            Ok(source_lsns)
        })
    }

    pub fn load_sync_source_kinds(&self) -> Result<HashMap<(String, RowId), u8>> {
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let table = match read_txn.open_table(SYNC_ROW_SOURCE_KIND_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(HashMap::new()),
                Err(err) => return Err(Self::storage_error(err)),
            };
            let mut kinds = HashMap::new();
            for entry in table.iter().map_err(Self::storage_error)? {
                let (key, value) = entry.map_err(Self::storage_error)?;
                if let Some((table, row_id)) = Self::decode_sync_row_source_lsn_key(key.value()) {
                    kinds.insert((table, row_id), value.value());
                }
            }
            Ok(kinds)
        })
    }

    pub fn load_change_log(&self) -> Result<Vec<ChangeLogEntry>> {
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let table = match read_txn.open_table(CHANGE_LOG_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                Err(err) => return Err(Self::storage_error(err)),
            };

            let mut entries = Vec::new();
            for entry in table.iter().map_err(Self::storage_error)? {
                let (_, value) = entry.map_err(Self::storage_error)?;
                entries.push(Self::decode(value.value())?);
            }
            Ok(entries)
        })
    }

    /// Rewrite the persisted change log to exactly `entries`, in order. Used by
    /// currency-table version compaction to drop, in lockstep with the pruned
    /// superseded row versions, the `RowInsert`/`RowDelete` entries that
    /// referenced them — so `changes_since`'s change-log replay (which reads the
    /// row version AT each logged LSN) never encounters an entry whose version
    /// was removed and silently substitutes the latest one. The table is fully
    /// cleared and rebuilt from `entries`; keys are re-derived as
    /// `{lsn:020}:{index}` with `index` restarting per LSN (entries sharing an
    /// LSN are contiguous, exactly as the commit path wrote them), so the load
    /// order is preserved byte-for-byte with what a fresh sequence of commits
    /// would have produced.
    pub fn rewrite_change_log(&self, entries: &[ChangeLogEntry]) -> Result<()> {
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            match write_txn.delete_table(CHANGE_LOG_TABLE) {
                Ok(_) | Err(redb::TableError::TableDoesNotExist(_)) => {}
                Err(err) => return Err(Self::storage_error(err)),
            }
            {
                let mut table = write_txn
                    .open_table(CHANGE_LOG_TABLE)
                    .map_err(Self::storage_error)?;
                let mut key = String::with_capacity(Self::change_log_entry_key_len());
                let mut encoded = Vec::new();
                let mut prev_lsn: Option<Lsn> = None;
                let mut index = 0usize;
                for entry in entries {
                    let lsn = entry.lsn();
                    if prev_lsn == Some(lsn) {
                        index += 1;
                    } else {
                        index = 0;
                        prev_lsn = Some(lsn);
                    }
                    Self::write_change_log_entry_key(lsn, index, &mut key);
                    Self::encode_into(entry, &mut encoded)?;
                    table
                        .insert(key.as_str(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    pub fn load_ddl_log(&self) -> Result<Vec<(Lsn, DdlChange)>> {
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let table = match read_txn.open_table(DDL_LOG_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                Err(err) => return Err(Self::storage_error(err)),
            };

            let mut entries = Vec::new();
            for entry in table.iter().map_err(Self::storage_error)? {
                let (key, value) = entry.map_err(Self::storage_error)?;
                let key = key.value();
                let lsn = key
                    .split_once(':')
                    .map(|(lsn, _)| lsn)
                    .unwrap_or(key)
                    .parse::<u64>()
                    .map_err(|err| Error::Other(format!("invalid ddl log key: {err}")))?;
                entries.push((Lsn(lsn), Self::decode(value.value())?));
            }
            Ok(entries)
        })
    }

    pub fn load_trigger_audit_state(
        &self,
        ring_capacity: usize,
    ) -> Result<(Vec<TriggerAuditEntry>, u64)> {
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            if let Ok(config_table) = read_txn.open_table(CONFIG_TABLE) {
                let ring = config_table
                    .get(TRIGGER_AUDIT_RING_CONFIG_KEY)
                    .map_err(Self::storage_error)?
                    .map(|value| Self::decode::<Vec<TriggerAuditEntry>>(value.value()))
                    .transpose()?;
                let next_index = config_table
                    .get(TRIGGER_AUDIT_NEXT_INDEX_CONFIG_KEY)
                    .map_err(Self::storage_error)?
                    .map(|value| Self::decode::<u64>(value.value()))
                    .transpose()?;
                if let (Some(mut ring), Some(next_index)) = (ring, next_index) {
                    let overflow = ring.len().saturating_sub(ring_capacity);
                    if overflow > 0 {
                        ring.drain(0..overflow);
                    }
                    return Ok((ring, next_index));
                }
            }

            let table = match read_txn.open_table(TRIGGER_AUDIT_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok((Vec::new(), 0)),
                Err(err) => return Err(Self::storage_error(err)),
            };

            let mut ring = VecDeque::new();
            let mut next_index = 0;
            for entry in table.iter().map_err(Self::storage_error)? {
                let (key, value) = entry.map_err(Self::storage_error)?;
                if let Some(index) = key
                    .value()
                    .split(':')
                    .next()
                    .and_then(|raw| raw.parse::<u64>().ok())
                {
                    next_index = next_index.max(index.saturating_add(1));
                }
                ring.push_back(Self::decode(value.value())?);
                if ring.len() > ring_capacity {
                    ring.pop_front();
                }
            }
            Ok((ring.into_iter().collect(), next_index))
        })
    }

    /// Delete every durable audit row written before `cutoff_millis`,
    /// whatever table it came from — including rows a DROP TABLE orphaned.
    /// Returns the number removed. Rows written before stamps existed carry no
    /// stamp and are left alone rather than guessed at.
    pub fn prune_trigger_audit_history(&self, cutoff_millis: u64) -> Result<u64> {
        self.with_db(|db| {
            // Find the expired keys under a READ transaction first. The
            // maintenance loop calls this every tick, and on the overwhelmingly
            // common tick nothing has aged out — committing an empty write
            // transaction each time would be a durable write per tick forever
            // for no effect.
            let expired = {
                let read_txn = db.begin_read().map_err(Self::storage_error)?;
                match read_txn.open_table(TRIGGER_AUDIT_STAMPS_TABLE) {
                    Ok(stamps) => stamps
                        .iter()
                        .map_err(Self::storage_error)?
                        .filter_map(|entry| entry.ok())
                        .filter(|(_, stamped_at)| stamped_at.value() < cutoff_millis)
                        .map(|(key, _)| key.value().to_string())
                        .collect::<Vec<_>>(),
                    Err(redb::TableError::TableDoesNotExist(_)) => Vec::new(),
                    Err(err) => return Err(Self::storage_error(err)),
                }
            };
            if expired.is_empty() {
                return Ok(0);
            }

            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            let mut removed = 0u64;
            {
                let mut stamps = write_txn
                    .open_table(TRIGGER_AUDIT_STAMPS_TABLE)
                    .map_err(Self::storage_error)?;
                let mut audit_table = write_txn
                    .open_table(TRIGGER_AUDIT_TABLE)
                    .map_err(Self::storage_error)?;
                for key in expired {
                    audit_table
                        .remove(key.as_str())
                        .map_err(Self::storage_error)?;
                    stamps.remove(key.as_str()).map_err(Self::storage_error)?;
                    removed = removed.saturating_add(1);
                }
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(removed)
        })
    }

    pub fn load_trigger_audit_history(&self) -> Result<Vec<TriggerAuditEntry>> {
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let table = match read_txn.open_table(TRIGGER_AUDIT_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                Err(err) => return Err(Self::storage_error(err)),
            };

            let mut entries = Vec::new();
            for entry in table.iter().map_err(Self::storage_error)? {
                let (_, value) = entry.map_err(Self::storage_error)?;
                entries.push(Self::decode(value.value())?);
            }
            Ok(entries)
        })
    }

    pub fn load_commit_index(&self) -> Result<BTreeMap<Lsn, TxId>> {
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let table = match read_txn.open_table(COMMIT_INDEX_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(BTreeMap::new()),
                Err(err) => return Err(Self::storage_error(err)),
            };

            let mut index = BTreeMap::new();
            for entry in table.iter().map_err(Self::storage_error)? {
                let (lsn, tx) = entry.map_err(Self::storage_error)?;
                index.insert(Lsn(lsn.value()), TxId(tx.value()));
            }
            Ok(index)
        })
    }

    pub fn flush_commit_index_entries(&self, entries: &BTreeMap<Lsn, TxId>) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            {
                let mut table = write_txn
                    .open_table(COMMIT_INDEX_TABLE)
                    .map_err(Self::storage_error)?;
                for (lsn, tx) in entries {
                    table.insert(lsn.0, tx.0).map_err(Self::storage_error)?;
                }
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    pub fn rewrite_pruned_state(
        &self,
        table_rows: &HashMap<String, Vec<VersionedRow>>,
        vectors: &[VectorEntry],
        edges: &[AdjEntry],
    ) -> Result<()> {
        let table_meta = self.load_all_table_meta()?;
        let vector_quantization = Self::vector_quantization_map(&table_meta);
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;

            for (name, rows) in table_rows {
                let table_name = Self::rel_table_name(name);
                let table_def: TableDefinition<&[u8], &[u8]> =
                    TableDefinition::new(table_name.as_str());
                match write_txn.delete_table(table_def) {
                    Ok(_) | Err(redb::TableError::TableDoesNotExist(_)) => {}
                    Err(redb::TableError::TableTypeMismatch { .. }) => {}
                    Err(err) => return Err(Self::storage_error(err)),
                }
                let legacy_table_def: TableDefinition<u64, &[u8]> =
                    TableDefinition::new(table_name.as_str());
                match write_txn.delete_table(legacy_table_def) {
                    Ok(_) | Err(redb::TableError::TableDoesNotExist(_)) => {}
                    Err(redb::TableError::TableTypeMismatch { .. }) => {}
                    Err(err) => return Err(Self::storage_error(err)),
                }
                let mut redb_table = write_txn
                    .open_table(table_def)
                    .map_err(Self::storage_error)?;
                for row in rows {
                    let encoded = Self::encode_versioned_row(row, table_meta.get(name))?;
                    let key = Self::rel_row_key(row);
                    redb_table
                        .insert(key.as_slice(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
                Self::retain_sync_source_provenance_for_table_rows(&write_txn, name, rows)?;
            }

            match write_txn.delete_table(VECTORS_TABLE) {
                Ok(_) | Err(redb::TableError::TableDoesNotExist(_)) => {}
                Err(err) => return Err(Self::storage_error(err)),
            }
            {
                let mut table = write_txn
                    .open_table(VECTORS_TABLE)
                    .map_err(Self::storage_error)?;
                for entry in vectors {
                    let quantization = vector_quantization
                        .get(&entry.index)
                        .copied()
                        .unwrap_or_default();
                    let encoded = Self::encode_vector_entry(entry, quantization)?;
                    let key = Self::vector_key(entry);
                    table
                        .insert(key.as_slice(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }

            match write_txn.delete_table(GRAPH_FWD_TABLE) {
                Ok(_) | Err(redb::TableError::TableDoesNotExist(_)) => {}
                Err(err) => return Err(Self::storage_error(err)),
            }
            match write_txn.delete_table(GRAPH_REV_TABLE) {
                Ok(_) | Err(redb::TableError::TableDoesNotExist(_)) => {}
                Err(err) => return Err(Self::storage_error(err)),
            }
            {
                let mut fwd_table = write_txn
                    .open_table(GRAPH_FWD_TABLE)
                    .map_err(Self::storage_error)?;
                let mut rev_table = write_txn
                    .open_table(GRAPH_REV_TABLE)
                    .map_err(Self::storage_error)?;

                for entry in edges {
                    let encoded = Self::encode(entry)?;
                    let fwd_key = Self::graph_fwd_key(entry);
                    let rev_key = Self::graph_rev_key(entry);
                    fwd_table
                        .insert(fwd_key.as_slice(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                    rev_table
                        .insert(rev_key.as_slice(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }

            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    pub fn rewrite_commit_index(&self, entries: &BTreeMap<Lsn, TxId>) -> Result<()> {
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            let _ = write_txn.delete_table(COMMIT_INDEX_TABLE);
            {
                let mut table = write_txn
                    .open_table(COMMIT_INDEX_TABLE)
                    .map_err(Self::storage_error)?;
                for (lsn, tx) in entries {
                    table.insert(lsn.0, tx.0).map_err(Self::storage_error)?;
                }
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    /// Remove exactly the named relational row VERSIONS, in ONE redb write
    /// transaction, together with the change-log entries that referenced
    /// them and the sync-source-lsn tracking rows that lost their last live
    /// version. Nothing else in the file is read or rewritten: a table's
    /// surviving rows stay exactly where they are, and vectors, edges, and
    /// the commit index are untouched (see [`Self::prune_vectors_and_edges_scoped`]
    /// for those).
    ///
    /// `row_keys` are `(table, row_id, created_tx, lsn)` — the full
    /// relational key, so each is a point `remove` with no scan needed to
    /// find it (`rel_row_key`, computed from a value already in hand — the
    /// `VersionedRow` being pruned).
    ///
    /// `pruned_change_keys` are the `(table, row_id, lsn)` identities of the
    /// same pruned versions, used to find which change-log entries
    /// referenced them. This does NOT recompute each entry's on-disk
    /// `(lsn, index)` coordinate from an in-memory recount: the moment a
    /// scoped pass leaves holes (rather than the old wholesale rewrite's
    /// full re-densify every pass), an in-memory recount goes stale on the
    /// SECOND pass over an lsn group and can silently orphan a change-log
    /// entry (see `changes_since`'s replay, which reads the row version AT
    /// each logged LSN and substitutes the latest on a miss). Instead, for
    /// every LSN any pruned version's change-log entry names, this reads
    /// that LSN group's entries AS THEY ACTUALLY STAND ON DISK right now,
    /// drops the ones naming a pruned version, and rewrites ONLY that LSN
    /// group, densely renumbered from zero — exactly as a fresh sequence of
    /// commits would have written them, and correct regardless of how many
    /// prior scoped passes already touched that group. Cost is proportional
    /// to the number of DISTINCT lsns touched (each an O(entries-in-that-
    /// commit) read+rewrite), never to the whole change log.
    ///
    /// `orphaned_source_lsn_rows` are the `(table, row_id)` pairs that lost
    /// their last LIVE version this pass — looked up by key
    /// (`sync_row_source_lsn_key`) rather than by iterating the table, as
    /// `retain_sync_source_lsns_for_table_rows` does today.
    pub fn prune_versions_scoped(
        &self,
        row_keys: &[(String, RowId, TxId, Lsn)],
        pruned_change_keys: &[(String, RowId, Lsn)],
        orphaned_source_lsn_rows: &[(String, RowId)],
    ) -> Result<PruneScopedStats> {
        if row_keys.is_empty()
            && pruned_change_keys.is_empty()
            && orphaned_source_lsn_rows.is_empty()
        {
            return Ok(PruneScopedStats::default());
        }
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            let mut change_log_keys_removed = 0u64;

            // CHANGE-LOG-FIRST within this one transaction, for the same
            // reason the shipped wholesale path orders it first: an orphaned
            // change-log entry corrupts replay, while an orphaned row
            // version is inert. Merging both into ONE transaction (rather
            // than the two separate transactions the wholesale path used)
            // removes the interleaving window entirely — after a crash the
            // file has either both removals or neither.
            if !pruned_change_keys.is_empty() {
                let mut affected: BTreeMap<Lsn, HashSet<(String, RowId)>> = BTreeMap::new();
                for (table, row_id, lsn) in pruned_change_keys {
                    affected
                        .entry(*lsn)
                        .or_default()
                        .insert((table.clone(), *row_id));
                }
                match write_txn.open_table(CHANGE_LOG_TABLE) {
                    Ok(mut table) => {
                        let mut key_buf = String::with_capacity(Self::change_log_entry_key_len());
                        for (lsn, pruned_here) in &affected {
                            let prefix = format!("{:020}:", lsn.0);
                            let existing: Vec<(String, Vec<u8>)> = {
                                let mut entries = Vec::new();
                                for entry in table
                                    .range(prefix.as_str()..)
                                    .map_err(Self::storage_error)?
                                {
                                    let (key, value) = entry.map_err(Self::storage_error)?;
                                    let key = key.value();
                                    if !key.starts_with(prefix.as_str()) {
                                        break;
                                    }
                                    entries.push((key.to_string(), value.value().to_vec()));
                                }
                                entries
                            };
                            let mut survivors: Vec<Vec<u8>> = Vec::with_capacity(existing.len());
                            for (key, bytes) in existing {
                                let decoded: ChangeLogEntry = Self::decode(&bytes)?;
                                let referenced = match &decoded {
                                    ChangeLogEntry::RowInsert { table, row_id, .. }
                                    | ChangeLogEntry::RowDelete { table, row_id, .. } => {
                                        pruned_here.contains(&(table.clone(), *row_id))
                                    }
                                    _ => false,
                                };
                                table.remove(key.as_str()).map_err(Self::storage_error)?;
                                if referenced {
                                    change_log_keys_removed =
                                        change_log_keys_removed.saturating_add(1);
                                } else {
                                    survivors.push(bytes);
                                }
                            }
                            for (index, bytes) in survivors.into_iter().enumerate() {
                                Self::write_change_log_entry_key(*lsn, index, &mut key_buf);
                                table
                                    .insert(key_buf.as_str(), bytes.as_slice())
                                    .map_err(Self::storage_error)?;
                            }
                        }
                    }
                    Err(redb::TableError::TableDoesNotExist(_)) => {}
                    Err(err) => return Err(Self::storage_error(err)),
                }
            }

            let mut opened_rel_tables: HashMap<String, redb::Table<'_, &[u8], &[u8]>> =
                HashMap::new();
            for (table, row_id, created_tx, lsn) in row_keys {
                if !opened_rel_tables.contains_key(table) {
                    let table_name = Self::rel_table_name(table);
                    let table_def: TableDefinition<&[u8], &[u8]> =
                        TableDefinition::new(table_name.as_str());
                    match write_txn.open_table(table_def) {
                        Ok(redb_table) => {
                            opened_rel_tables.insert(table.clone(), redb_table);
                        }
                        Err(redb::TableError::TableDoesNotExist(_)) => continue,
                        Err(err) => return Err(Self::storage_error(err)),
                    }
                }
                if let Some(redb_table) = opened_rel_tables.get_mut(table) {
                    let key = Self::rel_row_key_from_parts(*row_id, *created_tx, *lsn);
                    redb_table
                        .remove(key.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }
            drop(opened_rel_tables);

            if !orphaned_source_lsn_rows.is_empty() {
                match (
                    write_txn.open_table(SYNC_ROW_SOURCE_LSN_TABLE),
                    write_txn.open_table(SYNC_ROW_SOURCE_KIND_TABLE),
                ) {
                    (Ok(mut lsn_table), Ok(mut kind_table)) => {
                        for (table_name, row_id) in orphaned_source_lsn_rows {
                            let key = Self::sync_row_source_lsn_key(table_name, *row_id);
                            lsn_table
                                .remove(key.as_slice())
                                .map_err(Self::storage_error)?;
                            kind_table
                                .remove(key.as_slice())
                                .map_err(Self::storage_error)?;
                        }
                    }
                    (Err(redb::TableError::TableDoesNotExist(_)), _) => {}
                    (_, Err(redb::TableError::TableDoesNotExist(_))) => {}
                    (Err(err), _) | (_, Err(err)) => return Err(Self::storage_error(err)),
                }
            }

            write_txn.commit().map_err(Self::storage_error)?;
            Ok(PruneScopedStats {
                row_keys_removed: row_keys.len() as u64,
                change_log_keys_removed,
                vector_keys_removed: 0,
                edge_keys_removed: 0,
            })
        })
    }

    /// Apply the durable half of one authoritative purge in exactly one Redb
    /// write transaction.  This is deliberately not composed from scoped
    /// pruning helpers: the permanent lifecycle record must become durable
    /// atomically with every selected copy's disappearance.
    pub(crate) fn commit_authoritative_purge(
        &self,
        projection: &AuthoritativePurgePersistenceProjection,
    ) -> Result<()> {
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;

            // A selected change-log occurrence is identified by its full
            // value inside its LSN group.  Read and re-densify only touched
            // groups so earlier scoped maintenance cannot leave an orphaned
            // or mis-indexed survivor.
            let mut selected_by_lsn = BTreeMap::<Lsn, Vec<ChangeLogEntry>>::new();
            for entry in &projection.change_log_entries {
                selected_by_lsn
                    .entry(entry.lsn())
                    .or_default()
                    .push(entry.clone());
            }
            if !selected_by_lsn.is_empty() {
                let mut table = write_txn
                    .open_table(CHANGE_LOG_TABLE)
                    .map_err(Self::storage_error)?;
                let mut key_buf = String::with_capacity(Self::change_log_entry_key_len());
                for (lsn, mut witnesses) in selected_by_lsn {
                    let prefix = format!("{:020}:", lsn.0);
                    let existing = {
                        let mut entries = Vec::new();
                        for entry in table
                            .range(prefix.as_str()..)
                            .map_err(Self::storage_error)?
                        {
                            let (key, value) = entry.map_err(Self::storage_error)?;
                            if !key.value().starts_with(prefix.as_str()) {
                                break;
                            }
                            entries.push((key.value().to_string(), value.value().to_vec()));
                        }
                        entries
                    };
                    let mut survivors = Vec::with_capacity(existing.len());
                    for (key, bytes) in existing {
                        let decoded: ChangeLogEntry = Self::decode(&bytes)?;
                        if let Some(position) = witnesses.iter().position(|wanted| *wanted == decoded)
                        {
                            witnesses.remove(position);
                        } else {
                            survivors.push(bytes);
                        }
                        table.remove(key.as_str()).map_err(Self::storage_error)?;
                    }
                    if !witnesses.is_empty() {
                        return Err(Error::SyncError(
                            "authoritative purge change-log witness is missing from its LSN group"
                                .to_string(),
                        ));
                    }
                    for (index, bytes) in survivors.into_iter().enumerate() {
                        Self::write_change_log_entry_key(lsn, index, &mut key_buf);
                        table
                            .insert(key_buf.as_str(), bytes.as_slice())
                            .map_err(Self::storage_error)?;
                    }
                }
            }

            let mut relational_tables: HashMap<String, redb::Table<'_, &[u8], &[u8]>> =
                HashMap::new();
            for (table, row_id, created_tx, lsn) in &projection.row_versions {
                if !relational_tables.contains_key(table) {
                    let name = Self::rel_table_name(table);
                    let definition: TableDefinition<&[u8], &[u8]> =
                        TableDefinition::new(name.as_str());
                    relational_tables.insert(
                        table.clone(),
                        write_txn.open_table(definition).map_err(Self::storage_error)?,
                    );
                }
                let key = Self::rel_row_key_from_parts(*row_id, *created_tx, *lsn);
                let removed = relational_tables
                    .get_mut(table)
                    .expect("opened relational table")
                    .remove(key.as_slice())
                    .map_err(Self::storage_error)?;
                if removed.is_none() {
                    return Err(Error::SyncError(
                        "authoritative purge selected relational version disappeared before durable commit"
                            .to_string(),
                    ));
                }
            }
            drop(relational_tables);

            if !projection.source_provenance.is_empty() {
                let mut lsns = write_txn
                    .open_table(SYNC_ROW_SOURCE_LSN_TABLE)
                    .map_err(Self::storage_error)?;
                let mut kinds = write_txn
                    .open_table(SYNC_ROW_SOURCE_KIND_TABLE)
                    .map_err(Self::storage_error)?;
                for (table, row_id, expected_lsn, expected_kind) in &projection.source_provenance {
                    let key = Self::sync_row_source_lsn_key(table, *row_id);
                    let lsn = lsns
                        .get(key.as_slice())
                        .map_err(Self::storage_error)?
                        .map(|value| value.value());
                    let kind = kinds
                        .get(key.as_slice())
                        .map_err(Self::storage_error)?
                        .map(|value| value.value());
                    if lsn != Some(expected_lsn.0) || kind != Some(*expected_kind) {
                        return Err(Error::SyncError(
                            "authoritative purge source-provenance witness changed before durable commit"
                                .to_string(),
                        ));
                    }
                    lsns.remove(key.as_slice()).map_err(Self::storage_error)?;
                    kinds.remove(key.as_slice()).map_err(Self::storage_error)?;
                }
            }

            if !projection.vectors.is_empty() {
                let mut vectors = write_txn.open_table(VECTORS_TABLE).map_err(Self::storage_error)?;
                for entry in &projection.vectors {
                    let key = Self::vector_key(entry);
                    let removed = vectors.remove(key.as_slice()).map_err(Self::storage_error)?;
                    if removed.is_none() {
                        return Err(Error::SyncError(
                            "authoritative purge selected vector occurrence disappeared before durable commit"
                                .to_string(),
                        ));
                    }
                }
            }

            if !projection.graph_entries.is_empty() {
                let mut forward = write_txn.open_table(GRAPH_FWD_TABLE).map_err(Self::storage_error)?;
                let mut reverse = write_txn.open_table(GRAPH_REV_TABLE).map_err(Self::storage_error)?;
                for entry in &projection.graph_entries {
                    let forward_key = Self::graph_fwd_key(entry);
                    let reverse_key = Self::graph_rev_key(entry);
                    let removed_forward = forward
                        .remove(forward_key.as_slice())
                        .map_err(Self::storage_error)?;
                    let removed_reverse = reverse
                        .remove(reverse_key.as_slice())
                        .map_err(Self::storage_error)?;
                    if removed_forward.is_none() || removed_reverse.is_none() {
                        return Err(Error::SyncError(
                            "authoritative purge selected graph occurrence disappeared before durable commit"
                                .to_string(),
                        ));
                    }
                }
            }

            for (sink, queue_id, expected_bytes) in &projection.sink_entries {
                let name = Self::sink_queue_table_name(sink);
                let definition: TableDefinition<u64, &[u8]> = TableDefinition::new(name.as_str());
                let mut queue = write_txn.open_table(definition).map_err(Self::storage_error)?;
                let bytes = queue
                    .get(*queue_id)
                    .map_err(Self::storage_error)?
                    .map(|value| value.value().to_vec());
                if bytes.as_deref() != Some(expected_bytes.as_slice()) {
                    return Err(Error::SyncError(
                        "authoritative purge selected sink occurrence disappeared before durable commit"
                            .to_string(),
                    ));
                }
                queue.remove(*queue_id).map_err(Self::storage_error)?;
            }

            {
                let mut config = write_txn.open_table(CONFIG_TABLE).map_err(Self::storage_error)?;
                for key in &projection.config_keys_removed {
                    config.remove(key.as_str()).map_err(Self::storage_error)?;
                }
                for (key, bytes) in &projection.lifecycle_records {
                    config
                        .insert(key.as_str(), bytes.as_slice())
                        .map_err(Self::storage_error)?;
                }
                for (key, bytes) in &projection.purge_delivery_items {
                    let previous = config
                        .insert(key.as_str(), bytes.as_slice())
                        .map_err(Self::storage_error)?;
                    if previous.is_some() {
                        return Err(Error::SyncError(format!(
                            "authoritative purge delivery journal collision at {key}"
                        )));
                    }
                }
            }

            apply_authoritative_purge_in_write(&write_txn, &projection.blob_purge)?;

            #[cfg(test)]
            if take_authoritative_purge_point_remove_persistence_failure_for_test() {
                return Err(Error::Other(
                    "authoritative purge point-remove persistence failure injected".to_string(),
                ));
            }
            write_txn.commit().map_err(Self::storage_error)
        })
    }

    /// Remove exactly the named vectors and edges, in ONE redb write
    /// transaction. Used by the retention pass when pruned rows carried
    /// vectors or graph nodes, and by version cleanup ONLY for the vector
    /// copies attached to released row versions — version cleanup never
    /// touches edges (edge identity is `(source, target, edge_type)` plus
    /// its own `created_tx`/`lsn`, self-owned and versioned by no relational
    /// row), so its caller always passes an empty `edges` slice.
    pub fn prune_vectors_and_edges_scoped(
        &self,
        vectors: &[VectorEntry],
        edges: &[AdjEntry],
    ) -> Result<PruneScopedStats> {
        if vectors.is_empty() && edges.is_empty() {
            return Ok(PruneScopedStats::default());
        }
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            if !vectors.is_empty() {
                match write_txn.open_table(VECTORS_TABLE) {
                    Ok(mut table) => {
                        for entry in vectors {
                            let key = Self::vector_key(entry);
                            table.remove(key.as_slice()).map_err(Self::storage_error)?;
                        }
                    }
                    Err(redb::TableError::TableDoesNotExist(_)) => {}
                    Err(err) => return Err(Self::storage_error(err)),
                }
            }
            if !edges.is_empty() {
                let fwd = write_txn.open_table(GRAPH_FWD_TABLE);
                let rev = write_txn.open_table(GRAPH_REV_TABLE);
                match (fwd, rev) {
                    (Ok(mut fwd), Ok(mut rev)) => {
                        for entry in edges {
                            let fwd_key = Self::graph_fwd_key(entry);
                            let rev_key = Self::graph_rev_key(entry);
                            fwd.remove(fwd_key.as_slice())
                                .map_err(Self::storage_error)?;
                            rev.remove(rev_key.as_slice())
                                .map_err(Self::storage_error)?;
                        }
                    }
                    (Err(redb::TableError::TableDoesNotExist(_)), _)
                    | (_, Err(redb::TableError::TableDoesNotExist(_))) => {}
                    (Err(err), _) | (_, Err(err)) => return Err(Self::storage_error(err)),
                }
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(PruneScopedStats {
                row_keys_removed: 0,
                change_log_keys_removed: 0,
                vector_keys_removed: vectors.len() as u64,
                edge_keys_removed: (edges.len() as u64).saturating_mul(2),
            })
        })
    }

    /// Remove exactly the named commit-index entries — the scoped
    /// counterpart of `rewrite_commit_index`, used when a version-cleanup
    /// pass only made a handful of LSNs' commit-index entries redundant
    /// (their change-log entries are all gone and nothing else names them).
    /// Point removes; nothing else in the index is read or rewritten.
    pub fn remove_commit_index_entries_scoped(&self, lsns: &[Lsn]) -> Result<u64> {
        if lsns.is_empty() {
            return Ok(0);
        }
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            let mut removed = 0u64;
            match write_txn.open_table(COMMIT_INDEX_TABLE) {
                Ok(mut table) => {
                    for lsn in lsns {
                        if table.remove(lsn.0).map_err(Self::storage_error)?.is_some() {
                            removed = removed.saturating_add(1);
                        }
                    }
                }
                Err(redb::TableError::TableDoesNotExist(_)) => {}
                Err(err) => return Err(Self::storage_error(err)),
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(removed)
        })
    }

    /// Compacts the store to its minimal on-disk size, THEN recycles the
    /// redb handle (closes it and reopens the same, now-minimal, file) —
    /// checkpoint export runs this on the finished artifact so
    /// `bytes_written` reflects content, not allocator slack from the
    /// batched copy.
    ///
    /// The recycle exists because redb retains in-process allocator/region
    /// bookkeeping sized to a database's historical peak allocation, and a
    /// file-level `compact()` does not reset it even though it fully
    /// normalizes the on-disk btree and file size — confirmed by direct
    /// measurement: two databases compacted down to a byte-identical
    /// on-disk btree, one carrying a much larger prior-churn history, still
    /// showed a large gap in per-transaction commit cost after compaction
    /// alone; closing and reopening the handle closed that gap. A
    /// long-running embedded consumer has no other opportunity to do this
    /// (it cannot process-restart), so the recycle happens HERE,
    /// transparently, under the SAME locks compaction itself already
    /// holds: no other caller can observe (or race) an intermediate closed
    /// state, because every access to the underlying handle — this
    /// function included — goes through `self.db`'s mutex.
    ///
    /// A deliberate working-headroom margin between the shrink and the
    /// recycle was tried and measured NOT to help: redb's own close-time
    /// bookkeeping (`ensure_allocator_state_table_and_trim` in `Drop for
    /// redb::Database`) regrows a maximally-shrunk file to roughly the
    /// same final size whether or not a margin was left beforehand, so the
    /// margin only added the cost of two extra write transactions for no
    /// measured benefit — removed. The steady-state cost story for a file
    /// carrying a large amount of PRE-cleanup history (never yet compacted
    /// before) is the still-open retrofit-scenario question the version-
    /// cleanup-scaling bench's A2 arm measures directly.
    ///
    /// Returns the recycle's own duration in microseconds, separate from
    /// compaction's own timing (which the caller already measures around
    /// the whole call), matching the existing convention of reporting every
    /// compaction-adjacent cost as its own explicit number.
    ///
    /// On a reopen failure, the file itself is untouched (compaction had
    /// already finished; nothing more is written to it here) and this
    /// `RedbPersistence` is left closed — [`Error::StoreHandleRecycleFailed`]
    /// names the failure; a fresh `RedbPersistence::open`/`Database::open`
    /// on the same path recovers all data once this instance's lock is
    /// released via `close()`.
    pub(crate) fn compact(&self) -> Result<u64> {
        let lock_guard = self.lock_companion_slot()?;
        if lock_guard.is_none() {
            return Err(Error::Other("database persistence is closed".to_string()));
        }
        let mut db_guard = self.lock_database()?;
        let db = db_guard
            .as_mut()
            .ok_or_else(|| Error::Other("database persistence is closed".to_string()))?;
        while db.compact().map_err(Self::storage_error)? {}

        let recycle_started = std::time::Instant::now();
        let old = db_guard.take();
        drop(old);
        if take_handle_recycle_reopen_fault_for_test() {
            return Err(Error::StoreHandleRecycleFailed {
                path: self.path.display().to_string(),
                reason: "injected reopen failure (test seam)".to_string(),
            });
        }
        let reopened = Self::open_hook_suppressed(|| redb_builder().open(&self.path));
        match reopened {
            Ok(Ok(fresh)) => {
                *db_guard = Some(fresh);
                Ok(recycle_started.elapsed().as_micros() as u64)
            }
            Ok(Err(err)) => Err(Error::StoreHandleRecycleFailed {
                path: self.path.display().to_string(),
                reason: err.to_string(),
            }),
            Err(_) => Err(Error::StoreHandleRecycleFailed {
                path: self.path.display().to_string(),
                reason: "reopen panicked; store may be truncated or corrupt".to_string(),
            }),
        }
    }

    /// Fraction of the on-disk file that is free/fragmented pages (dead space):
    /// `fragmented / (stored + metadata + fragmented)`. Used to decide whether a
    /// version-compaction pass should follow up with a full redb `compact()` to
    /// reclaim the freed pages. Reads stats via a short write transaction that is
    /// immediately aborted, so it never mutates the store.
    pub(crate) fn fragmentation_ratio(&self) -> Result<f64> {
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            let stats = write_txn.stats().map_err(Self::storage_error)?;
            let stored = stats.stored_bytes();
            let metadata = stats.metadata_bytes();
            let fragmented = stats.fragmented_bytes();
            write_txn.abort().map_err(Self::storage_error)?;
            let total = stored.saturating_add(metadata).saturating_add(fragmented);
            if total == 0 {
                Ok(0.0)
            } else {
                Ok(fragmented as f64 / total as f64)
            }
        })
    }

    // --- Checkpoint-export seams ------------------------------------------
    //
    // Raw dumps pair with the existing raw config writer so export can copy
    // dynamic-key state (tenant watermarks, per-sink queues, audit rings)
    // without decoding it. The append-batch writers stream per-category
    // batches into a fresh artifact; unlike the `rewrite_*` family they never
    // delete-then-rewrite a full slice.

    pub(crate) fn dump_config_raw(&self) -> Result<Vec<(String, Vec<u8>)>> {
        self.dump_str_keyed_table_raw(CONFIG_TABLE)
    }

    fn digest_canonical_relational_table(
        &self,
        read_txn: &redb::ReadTransaction,
        table_name: &str,
        hasher: &mut blake3::Hasher,
    ) -> Result<u64> {
        digest_field(hasher, table_name.as_bytes());
        let definition: TableDefinition<&[u8], &[u8]> = TableDefinition::new(table_name);
        let table = read_txn
            .open_table(definition)
            .map_err(Self::storage_error)?;
        let mut count = 0_u64;
        for entry in table.iter().map_err(Self::storage_error)? {
            let (key, value) = entry.map_err(Self::storage_error)?;
            digest_field(hasher, key.value());
            let row: PersistedVersionedRow = Self::decode(value.value())?;
            rmp_serde::encode::write(
                &mut *hasher,
                &(
                    row.row_id,
                    row.created_tx,
                    row.deleted_tx,
                    row.lsn,
                    row.created_at,
                ),
            )
            .map_err(|err| Error::Other(format!("failed to fingerprint relational row: {err}")))?;
            let mut columns = row.values.into_iter().collect::<Vec<_>>();
            columns.sort_by(|left, right| left.0.cmp(&right.0));
            rmp_serde::encode::write(&mut *hasher, &(columns.len() as u64)).map_err(|err| {
                Error::Other(format!("failed to fingerprint relational columns: {err}"))
            })?;
            for (column, value) in columns {
                digest_field(hasher, column.as_bytes());
                rmp_serde::encode::write(&mut *hasher, &value).map_err(|err| {
                    Error::Other(format!("failed to fingerprint relational value: {err}"))
                })?;
            }
            count = count.saturating_add(1);
        }
        Ok(count)
    }

    /// Fingerprint every Redb table an authenticated schema or data apply can
    /// mutate. Relational rows are decoded and hashed with column-sorted
    /// values because snapshot export re-encodes their `HashMap`; every other
    /// category remains an exact persisted key/value-byte hash. Dynamic
    /// relational and sink-queue table names are part of the digest, so
    /// creating or removing an empty table is still visible.
    pub(crate) fn raw_sync_apply_state_digest(&self) -> Result<RawSyncApplyStateDigest> {
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let mut hasher = blake3::Hasher::new();
            digest_field(&mut hasher, b"contextdb.raw-sync-apply-state.v2");

            digest_redb_table(&read_txn, META_TABLE, "meta", &mut hasher)?;
            digest_redb_table(
                &read_txn,
                FORMAT_METADATA_TABLE,
                "format-metadata",
                &mut hasher,
            )?;
            digest_redb_table(&read_txn, CONFIG_TABLE, "config", &mut hasher)?;
            digest_redb_table(&read_txn, CHANGE_LOG_TABLE, "change-log", &mut hasher)?;
            digest_redb_table(&read_txn, DDL_LOG_TABLE, "ddl-log", &mut hasher)?;
            digest_redb_table(&read_txn, COMMIT_INDEX_TABLE, "commit-index", &mut hasher)?;
            let sink_audit_entries =
                digest_redb_table(&read_txn, SINK_AUDIT_TABLE, "sink-audit", &mut hasher)?;
            let trigger_audit_entries =
                digest_redb_table(&read_txn, TRIGGER_AUDIT_TABLE, "trigger-audit", &mut hasher)?;
            digest_redb_table(
                &read_txn,
                TRIGGER_AUDIT_STAMPS_TABLE,
                "trigger-audit-stamps",
                &mut hasher,
            )?;
            digest_redb_table(&read_txn, GRAPH_FWD_TABLE, "graph-forward", &mut hasher)?;
            digest_redb_table(&read_txn, GRAPH_REV_TABLE, "graph-reverse", &mut hasher)?;
            digest_redb_table(&read_txn, VECTORS_TABLE, "vectors", &mut hasher)?;
            digest_redb_table(
                &read_txn,
                SYNC_ROW_SOURCE_LSN_TABLE,
                "sync-row-source-lsn",
                &mut hasher,
            )?;
            digest_redb_table(
                &read_txn,
                SYNC_ROW_SOURCE_KIND_TABLE,
                "sync-row-source-kind",
                &mut hasher,
            )?;

            let mut dynamic_names = read_txn
                .list_tables()
                .map_err(Self::storage_error)?
                .map(|handle| handle.name().to_string())
                .filter(|name| name.starts_with("rel_") || name.starts_with("__sink_queue_"))
                .collect::<Vec<_>>();
            dynamic_names.sort();
            let mut sink_queue_entries = 0_u64;
            for name in dynamic_names {
                if name.starts_with("rel_") {
                    self.digest_canonical_relational_table(&read_txn, &name, &mut hasher)?;
                } else {
                    let definition: TableDefinition<u64, &[u8]> =
                        TableDefinition::new(name.as_str());
                    sink_queue_entries = sink_queue_entries.saturating_add(digest_redb_table(
                        &read_txn,
                        definition,
                        &name,
                        &mut hasher,
                    )?);
                }
            }

            Ok(RawSyncApplyStateDigest {
                digest: hasher.finalize().to_hex().to_string(),
                trigger_audit_entries,
                sink_audit_entries,
                sink_queue_entries,
            })
        })
    }

    pub(crate) fn dump_trigger_audit_raw(&self) -> Result<Vec<(String, Vec<u8>)>> {
        self.dump_str_keyed_table_raw(TRIGGER_AUDIT_TABLE)
    }

    pub(crate) fn dump_trigger_audit_stamps_raw(&self) -> Result<Vec<(String, u64)>> {
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let table = match read_txn.open_table(TRIGGER_AUDIT_STAMPS_TABLE) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                Err(err) => return Err(Self::storage_error(err)),
            };
            let mut entries = Vec::new();
            for entry in table.iter().map_err(Self::storage_error)? {
                let (key, value) = entry.map_err(Self::storage_error)?;
                entries.push((key.value().to_string(), value.value()));
            }
            Ok(entries)
        })
    }

    pub(crate) fn dump_sink_audit_raw(&self) -> Result<Vec<(String, Vec<u8>)>> {
        self.dump_str_keyed_table_raw(SINK_AUDIT_TABLE)
    }

    fn dump_str_keyed_table_raw(
        &self,
        definition: TableDefinition<&str, &[u8]>,
    ) -> Result<Vec<(String, Vec<u8>)>> {
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let table = match read_txn.open_table(definition) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                Err(err) => return Err(Self::storage_error(err)),
            };
            let mut entries = Vec::new();
            for entry in table.iter().map_err(Self::storage_error)? {
                let (key, value) = entry.map_err(Self::storage_error)?;
                entries.push((key.value().to_string(), value.value().to_vec()));
            }
            Ok(entries)
        })
    }

    pub(crate) fn append_trigger_audit_raw(&self, entries: &[(String, Vec<u8>)]) -> Result<()> {
        self.append_str_keyed_table_raw(TRIGGER_AUDIT_TABLE, entries)
    }

    pub(crate) fn append_trigger_audit_stamps_raw(&self, entries: &[(String, u64)]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            {
                let mut table = write_txn
                    .open_table(TRIGGER_AUDIT_STAMPS_TABLE)
                    .map_err(Self::storage_error)?;
                for (key, value) in entries {
                    table
                        .insert(key.as_str(), *value)
                        .map_err(Self::storage_error)?;
                }
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    pub(crate) fn append_sink_audit_raw(&self, entries: &[(String, Vec<u8>)]) -> Result<()> {
        self.append_str_keyed_table_raw(SINK_AUDIT_TABLE, entries)
    }

    fn append_str_keyed_table_raw(
        &self,
        definition: TableDefinition<&str, &[u8]>,
        entries: &[(String, Vec<u8>)],
    ) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            {
                let mut table = write_txn
                    .open_table(definition)
                    .map_err(Self::storage_error)?;
                for (key, value) in entries {
                    table
                        .insert(key.as_str(), value.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    pub(crate) fn sink_queue_names(&self) -> Result<Vec<String>> {
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let mut names = Vec::new();
            for handle in read_txn.list_tables().map_err(Self::storage_error)? {
                if let Some(sink) = handle.name().strip_prefix("__sink_queue_") {
                    names.push(sink.to_string());
                }
            }
            Ok(names)
        })
    }

    pub(crate) fn dump_sink_queue_raw(&self, sink: &str) -> Result<Vec<(u64, Vec<u8>)>> {
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let table_name = Self::sink_queue_table_name(sink);
            let table_def: TableDefinition<u64, &[u8]> = TableDefinition::new(table_name.as_str());
            let table = match read_txn.open_table(table_def) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                Err(err) => return Err(Self::storage_error(err)),
            };
            let mut entries = Vec::new();
            for entry in table.iter().map_err(Self::storage_error)? {
                let (key, value) = entry.map_err(Self::storage_error)?;
                entries.push((key.value(), value.value().to_vec()));
            }
            Ok(entries)
        })
    }

    pub(crate) fn append_sink_queue_raw(
        &self,
        sink: &str,
        entries: &[(u64, Vec<u8>)],
    ) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            {
                let table_name = Self::sink_queue_table_name(sink);
                let table_def: TableDefinition<u64, &[u8]> =
                    TableDefinition::new(table_name.as_str());
                let mut table = write_txn
                    .open_table(table_def)
                    .map_err(Self::storage_error)?;
                for (id, value) in entries {
                    table
                        .insert(*id, value.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    pub(crate) fn append_table_rows_batch(
        &self,
        name: &str,
        meta: Option<&TableMeta>,
        rows: &[VersionedRow],
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            {
                let table_name = Self::rel_table_name(name);
                let table_def: TableDefinition<&[u8], &[u8]> =
                    TableDefinition::new(table_name.as_str());
                let mut redb_table = write_txn
                    .open_table(table_def)
                    .map_err(Self::storage_error)?;
                let mut encoded = Vec::new();
                for row in rows {
                    Self::encode_versioned_row_into(row, meta, &mut encoded)?;
                    let key = Self::rel_row_key(row);
                    redb_table
                        .insert(key.as_slice(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    pub(crate) fn append_sync_source_provenance_batch(
        &self,
        entries: &[(String, RowId, Lsn, u8)],
    ) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            {
                let mut lsns = write_txn
                    .open_table(SYNC_ROW_SOURCE_LSN_TABLE)
                    .map_err(Self::storage_error)?;
                let mut kinds = write_txn
                    .open_table(SYNC_ROW_SOURCE_KIND_TABLE)
                    .map_err(Self::storage_error)?;
                for (table, row_id, lsn, kind) in entries {
                    let key = Self::sync_row_source_lsn_key(table, *row_id);
                    lsns.insert(key.as_slice(), lsn.0)
                        .map_err(Self::storage_error)?;
                    kinds
                        .insert(key.as_slice(), *kind)
                        .map_err(Self::storage_error)?;
                }
            }
            write_txn.commit().map_err(Self::storage_error)
        })
    }

    pub(crate) fn append_graph_edges_batch(&self, edges: &[AdjEntry]) -> Result<()> {
        if edges.is_empty() {
            return Ok(());
        }
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            {
                let mut fwd_table = write_txn
                    .open_table(GRAPH_FWD_TABLE)
                    .map_err(Self::storage_error)?;
                let mut rev_table = write_txn
                    .open_table(GRAPH_REV_TABLE)
                    .map_err(Self::storage_error)?;
                let mut encoded = Vec::new();
                for entry in edges {
                    Self::encode_into(entry, &mut encoded)?;
                    let fwd_key = Self::graph_fwd_key(entry);
                    let rev_key = Self::graph_rev_key(entry);
                    fwd_table
                        .insert(fwd_key.as_slice(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                    rev_table
                        .insert(rev_key.as_slice(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    pub(crate) fn append_vector_entries_batch(
        &self,
        entries: &[VectorEntry],
        quantization: &HashMap<VectorIndexRef, VectorQuantization>,
    ) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            {
                let mut table = write_txn
                    .open_table(VECTORS_TABLE)
                    .map_err(Self::storage_error)?;
                for entry in entries {
                    let quantization = quantization.get(&entry.index).copied().unwrap_or_default();
                    let encoded = Self::encode_vector_entry(entry, quantization)?;
                    let key = Self::vector_key(entry);
                    table
                        .insert(key.as_slice(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    /// Each entry carries its per-LSN key index, tracked by the caller across
    /// batches so a multi-entry commit split over two batches never collides.
    pub(crate) fn append_change_log_entries_batch(
        &self,
        entries: &[(usize, ChangeLogEntry)],
    ) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            {
                let mut table = write_txn
                    .open_table(CHANGE_LOG_TABLE)
                    .map_err(Self::storage_error)?;
                let mut encoded = Vec::new();
                let mut key = String::with_capacity(Self::change_log_entry_key_len());
                for (index, entry) in entries {
                    Self::write_change_log_entry_key(entry.lsn(), *index, &mut key);
                    Self::encode_into(entry, &mut encoded)?;
                    table
                        .insert(key.as_str(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                }
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    pub(crate) fn append_ddl_log_entries(&self, entries: &[(Lsn, DdlChange)]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        self.with_db(|db| {
            let write_txn = db.begin_write().map_err(Self::storage_error)?;
            {
                let mut ddl_table = write_txn
                    .open_table(DDL_LOG_TABLE)
                    .map_err(Self::storage_error)?;
                // The log is LSN-ordered; key same-LSN runs exactly as the
                // commit-time writer does (plain key for a single change,
                // indexed keys for a multi-change commit).
                let mut start = 0usize;
                while start < entries.len() {
                    let lsn = entries[start].0;
                    let mut end = start + 1;
                    while end < entries.len() && entries[end].0 == lsn {
                        end += 1;
                    }
                    let count = end - start;
                    for (offset, (_, change)) in entries[start..end].iter().enumerate() {
                        let key = Self::ddl_log_key_for_index(lsn, offset, count);
                        let encoded = Self::encode(change)?;
                        ddl_table
                            .insert(key.as_str(), encoded.as_slice())
                            .map_err(Self::storage_error)?;
                    }
                    start = end;
                }
            }
            write_txn.commit().map_err(Self::storage_error)?;
            Ok(())
        })
    }

    fn load_graph_table(&self, definition: TableDefinition<&[u8], &[u8]>) -> Result<Vec<AdjEntry>> {
        self.with_db(|db| {
            let read_txn = db.begin_read().map_err(Self::storage_error)?;
            let table = match read_txn.open_table(definition) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                Err(err) => return Err(Self::storage_error(err)),
            };

            let mut entries = Vec::new();
            for entry in table.iter().map_err(Self::storage_error)? {
                let (_, value) = entry.map_err(Self::storage_error)?;
                entries.push(Self::decode(value.value())?);
            }
            Ok(entries)
        })
    }

    fn load_table_meta_in_read_txn(
        read_txn: &redb::ReadTransaction,
        name: &str,
    ) -> Result<Option<TableMeta>> {
        let meta_table = match read_txn.open_table(META_TABLE) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(err) => return Err(Self::storage_error(err)),
        };
        let key = Self::meta_key(name);
        meta_table
            .get(key.as_str())
            .map_err(Self::storage_error)?
            .map(|value| Self::decode_table_meta_versioned(value.value()).map(|(meta, _)| meta))
            .transpose()
    }

    pub(crate) fn vector_quantization_map(
        table_meta: &HashMap<String, TableMeta>,
    ) -> HashMap<VectorIndexRef, VectorQuantization> {
        let mut indexes = HashMap::new();
        for (table, meta) in table_meta {
            for column in &meta.columns {
                if matches!(column.column_type, ColumnType::Vector(_)) {
                    indexes.insert(
                        VectorIndexRef::new(table.clone(), column.name.clone()),
                        column.quantization,
                    );
                }
            }
        }
        indexes
    }

    fn write_set_visibility_tx(ws: &WriteSet) -> Option<TxId> {
        ws.relational_inserts
            .iter()
            .flat_map(|(_, row)| std::iter::once(row.created_tx).chain(row.deleted_tx))
            .chain(
                ws.relational_deletes
                    .iter()
                    .map(|(_, _, deleted_tx)| *deleted_tx),
            )
            .chain(
                ws.adj_inserts
                    .iter()
                    .flat_map(|entry| std::iter::once(entry.created_tx).chain(entry.deleted_tx)),
            )
            .chain(
                ws.adj_deletes
                    .iter()
                    .map(|(_, _, _, deleted_tx)| *deleted_tx),
            )
            .chain(
                ws.vector_inserts
                    .iter()
                    .flat_map(|entry| std::iter::once(entry.created_tx).chain(entry.deleted_tx)),
            )
            .chain(
                ws.vector_deletes
                    .iter()
                    .map(|(_, _, deleted_tx)| *deleted_tx),
            )
            .chain(ws.vector_moves.iter().map(|(_, _, _, tx)| *tx))
            .max()
    }

    fn column_quantization(meta: Option<&TableMeta>, column_name: &str) -> VectorQuantization {
        meta.and_then(|meta| {
            meta.columns
                .iter()
                .find(|column| {
                    column.name == column_name
                        && matches!(column.column_type, ColumnType::Vector(_))
                })
                .map(|column| column.quantization)
        })
        .unwrap_or_default()
    }

    fn encode_versioned_row(row: &VersionedRow, meta: Option<&TableMeta>) -> Result<Vec<u8>> {
        let mut encoded = Vec::new();
        Self::encode_versioned_row_into(row, meta, &mut encoded)?;
        Ok(encoded)
    }

    fn encode_versioned_row_into(
        row: &VersionedRow,
        meta: Option<&TableMeta>,
        encoded: &mut Vec<u8>,
    ) -> Result<()> {
        Self::encode_versioned_row_with_deleted_tx_into(row, row.deleted_tx, meta, encoded)
    }

    fn encode_versioned_row_with_deleted_tx_into(
        row: &VersionedRow,
        deleted_tx: Option<TxId>,
        meta: Option<&TableMeta>,
        encoded: &mut Vec<u8>,
    ) -> Result<()> {
        let values = row
            .values
            .iter()
            .map(|(column, value)| {
                let persisted = match value {
                    Value::Vector(vector) => {
                        let quantization = Self::column_quantization(meta, column);
                        if matches!(quantization, VectorQuantization::F32) {
                            PersistedValue::Vector(PersistedVector::from_f32(vector, quantization))
                        } else {
                            PersistedValue::Plain(Value::Null)
                        }
                    }
                    _ => PersistedValue::Plain(value.clone()),
                };
                (column.clone(), persisted)
            })
            .collect::<HashMap<_, _>>();
        Self::encode_into(
            &PersistedVersionedRow {
                row_id: row.row_id,
                values,
                created_tx: row.created_tx,
                deleted_tx,
                lsn: row.lsn,
                created_at: row.created_at,
            },
            encoded,
        )
    }

    fn decode_versioned_row(bytes: &[u8], _meta: Option<&TableMeta>) -> Result<VersionedRow> {
        let persisted: PersistedVersionedRow = Self::decode(bytes)?;
        let values = persisted
            .values
            .into_iter()
            .map(|(column, value)| {
                let value = match value {
                    PersistedValue::Plain(value) => value,
                    PersistedValue::Vector(vector) => Value::Vector(vector.to_f32()),
                };
                (column, value)
            })
            .collect::<HashMap<_, _>>();
        Ok(VersionedRow {
            row_id: persisted.row_id,
            values,
            created_tx: persisted.created_tx,
            deleted_tx: persisted.deleted_tx,
            lsn: persisted.lsn,
            created_at: persisted.created_at,
        })
    }

    fn encode_vector_entry(
        entry: &VectorEntry,
        quantization: VectorQuantization,
    ) -> Result<Vec<u8>> {
        Self::encode(&PersistedVectorEntry {
            index: entry.index.clone(),
            row_id: entry.row_id,
            vector: PersistedVector::from_f32(&entry.vector, quantization),
            created_tx: entry.created_tx,
            deleted_tx: entry.deleted_tx,
            lsn: entry.lsn,
        })
    }

    fn decode_vector_entry(bytes: &[u8]) -> Result<VectorEntry> {
        let persisted: PersistedVectorEntry = Self::decode(bytes)?;
        Ok(VectorEntry {
            index: persisted.index,
            row_id: persisted.row_id,
            vector: persisted.vector.to_f32(),
            created_tx: persisted.created_tx,
            deleted_tx: persisted.deleted_tx,
            lsn: persisted.lsn,
        })
    }

    fn rel_table_name(name: &str) -> String {
        format!("rel_{name}")
    }

    fn sink_queue_table_name(sink: &str) -> String {
        format!("__sink_queue_{sink}")
    }

    fn sink_audit_key(lsn: Lsn, index: usize, sink: &str) -> String {
        format!("{:020}:{index:06}:{sink}", lsn.0)
    }

    fn trigger_audit_key(index: u64, trigger: &str) -> String {
        format!("{index:020}:{trigger}")
    }

    fn rel_row_key(row: &VersionedRow) -> Vec<u8> {
        Self::rel_row_key_from_parts(row.row_id, row.created_tx, row.lsn)
    }

    /// The same key `rel_row_key` derives from a `VersionedRow`, built from
    /// the raw identity parts directly — for a scoped prune, which has the
    /// pruned version's `(row_id, created_tx, lsn)` in hand without needing
    /// the whole row.
    fn rel_row_key_from_parts(row_id: RowId, created_tx: TxId, lsn: Lsn) -> Vec<u8> {
        let mut key = Vec::with_capacity(24);
        key.extend_from_slice(&row_id.0.to_be_bytes());
        key.extend_from_slice(&created_tx.0.to_be_bytes());
        key.extend_from_slice(&lsn.0.to_be_bytes());
        key
    }

    fn sync_row_source_lsn_key(table: &str, row_id: RowId) -> Vec<u8> {
        let mut key = Vec::with_capacity(table.len() + 8);
        key.extend_from_slice(table.as_bytes());
        key.extend_from_slice(&row_id.0.to_be_bytes());
        key
    }

    fn decode_sync_row_source_lsn_key(key: &[u8]) -> Option<(String, RowId)> {
        let split = key.len().checked_sub(8)?;
        let (table, row_id) = key.split_at(split);
        let table = String::from_utf8(table.to_vec()).ok()?;
        let row_id = RowId(u64::from_be_bytes(row_id.try_into().ok()?));
        Some((table, row_id))
    }

    fn remove_sync_source_provenance_for_table(
        write_txn: &redb::WriteTransaction,
        table: &str,
    ) -> Result<()> {
        let mut source_lsn_table = write_txn
            .open_table(SYNC_ROW_SOURCE_LSN_TABLE)
            .map_err(Self::storage_error)?;
        let lsn_keys_to_remove =
            Self::sync_source_lsn_keys_for_table(&source_lsn_table, table, |_| true)?;
        let mut source_kind_table = write_txn
            .open_table(SYNC_ROW_SOURCE_KIND_TABLE)
            .map_err(Self::storage_error)?;
        let kind_keys_to_remove =
            Self::sync_source_kind_keys_for_table(&source_kind_table, table, |_| true)?;
        for key in lsn_keys_to_remove {
            source_lsn_table
                .remove(key.as_slice())
                .map_err(Self::storage_error)?;
        }
        for key in kind_keys_to_remove {
            source_kind_table
                .remove(key.as_slice())
                .map_err(Self::storage_error)?;
        }
        Ok(())
    }

    fn retain_sync_source_provenance_for_table_rows(
        write_txn: &redb::WriteTransaction,
        table: &str,
        rows: &[VersionedRow],
    ) -> Result<()> {
        let live_row_ids = rows
            .iter()
            .filter(|row| row.deleted_tx.is_none())
            .map(|row| row.row_id)
            .collect::<HashSet<_>>();
        let mut source_lsn_table = write_txn
            .open_table(SYNC_ROW_SOURCE_LSN_TABLE)
            .map_err(Self::storage_error)?;
        let lsn_keys_to_remove =
            Self::sync_source_lsn_keys_for_table(&source_lsn_table, table, |row_id| {
                !live_row_ids.contains(&row_id)
            })?;
        let mut source_kind_table = write_txn
            .open_table(SYNC_ROW_SOURCE_KIND_TABLE)
            .map_err(Self::storage_error)?;
        let kind_keys_to_remove =
            Self::sync_source_kind_keys_for_table(&source_kind_table, table, |row_id| {
                !live_row_ids.contains(&row_id)
            })?;
        for key in lsn_keys_to_remove {
            source_lsn_table
                .remove(key.as_slice())
                .map_err(Self::storage_error)?;
        }
        for key in kind_keys_to_remove {
            source_kind_table
                .remove(key.as_slice())
                .map_err(Self::storage_error)?;
        }
        Ok(())
    }

    fn sync_source_lsn_keys_for_table(
        source_lsn_table: &impl ReadableTable<&'static [u8], u64>,
        table: &str,
        should_remove: impl Fn(RowId) -> bool,
    ) -> Result<Vec<Vec<u8>>> {
        let mut keys = Vec::new();
        for entry in source_lsn_table.iter().map_err(Self::storage_error)? {
            let (key, _) = entry.map_err(Self::storage_error)?;
            if let Some((entry_table, row_id)) = Self::decode_sync_row_source_lsn_key(key.value())
                && entry_table == table
                && should_remove(row_id)
            {
                keys.push(key.value().to_vec());
            }
        }
        Ok(keys)
    }

    fn sync_source_kind_keys_for_table(
        source_kind_table: &impl ReadableTable<&'static [u8], u8>,
        table: &str,
        should_remove: impl Fn(RowId) -> bool,
    ) -> Result<Vec<Vec<u8>>> {
        let mut keys = Vec::new();
        for entry in source_kind_table.iter().map_err(Self::storage_error)? {
            let (key, _) = entry.map_err(Self::storage_error)?;
            if let Some((entry_table, row_id)) = Self::decode_sync_row_source_lsn_key(key.value())
                && entry_table == table
                && should_remove(row_id)
            {
                keys.push(key.value().to_vec());
            }
        }
        Ok(keys)
    }

    fn rel_row_key_range(row_id: RowId) -> ([u8; 8], [u8; 8]) {
        let lower = row_id.0.to_be_bytes();
        let upper = row_id.0.saturating_add(1).to_be_bytes();
        (lower, upper)
    }

    fn meta_key(name: &str) -> String {
        format!("table:{name}")
    }

    fn change_log_entry_key_len() -> usize {
        20 + 1 + 6
    }

    fn write_change_log_entry_key(lsn: Lsn, index: usize, key: &mut String) {
        key.clear();
        write!(key, "{:020}:{index:06}", lsn.0).expect("writing change-log key to String");
    }

    fn ddl_log_key(lsn: Lsn) -> String {
        format!("{:020}", lsn.0)
    }

    fn ddl_log_key_for_index(lsn: Lsn, index: usize, count: usize) -> String {
        if count <= 1 {
            Self::ddl_log_key(lsn)
        } else {
            format!("{:020}:{index:06}", lsn.0)
        }
    }

    fn graph_fwd_key(entry: &AdjEntry) -> Vec<u8> {
        let mut key = Vec::with_capacity(48 + entry.edge_type.len());
        key.extend_from_slice(entry.source.as_bytes());
        key.extend_from_slice(entry.target.as_bytes());
        key.extend_from_slice(entry.edge_type.as_bytes());
        key.push(0);
        key.extend_from_slice(&entry.created_tx.0.to_be_bytes());
        key.extend_from_slice(&entry.lsn.0.to_be_bytes());
        key
    }

    fn graph_rev_key(entry: &AdjEntry) -> Vec<u8> {
        let mut key = Vec::with_capacity(48 + entry.edge_type.len());
        key.extend_from_slice(entry.target.as_bytes());
        key.extend_from_slice(entry.source.as_bytes());
        key.extend_from_slice(entry.edge_type.as_bytes());
        key.push(0);
        key.extend_from_slice(&entry.created_tx.0.to_be_bytes());
        key.extend_from_slice(&entry.lsn.0.to_be_bytes());
        key
    }

    fn vector_key(entry: &VectorEntry) -> Vec<u8> {
        let mut key = Vec::with_capacity(entry.index.table.len() + entry.index.column.len() + 34);
        key.extend_from_slice(entry.index.table.as_bytes());
        key.push(0);
        key.extend_from_slice(entry.index.column.as_bytes());
        key.push(0);
        key.extend_from_slice(&entry.row_id.0.to_be_bytes());
        key.extend_from_slice(&entry.created_tx.0.to_be_bytes());
        key.extend_from_slice(&entry.lsn.0.to_be_bytes());
        key
    }

    fn encode<T: serde::Serialize>(value: &T) -> Result<Vec<u8>> {
        bincode::serde::encode_to_vec(value, bincode::config::standard())
            .map_err(|err| Error::Other(format!("bincode encode error: {err}")))
    }

    fn encode_into<T: serde::Serialize>(value: &T, encoded: &mut Vec<u8>) -> Result<()> {
        encoded.clear();
        bincode::serde::encode_into_std_write(value, encoded, bincode::config::standard())
            .map(|_| ())
            .map_err(|err| Error::Other(format!("bincode encode error: {err}")))
    }

    fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
        let (value, _) = bincode::serde::decode_from_slice(bytes, bincode::config::standard())
            .map_err(|err| Error::Other(format!("bincode decode error: {err}")))?;
        Ok(value)
    }

    fn decode_exact<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
        let (value, consumed) =
            bincode::serde::decode_from_slice(bytes, bincode::config::standard())
                .map_err(|err| Error::Other(format!("bincode decode error: {err}")))?;
        if consumed != bytes.len() {
            return Err(Error::Other(
                "bincode exact decode left trailing bytes".to_string(),
            ));
        }
        Ok(value)
    }

    /// Decode one `TableMeta` blob, trying the CURRENT layout first and
    /// falling back to the exact `v1.0.0` layout on the SPECIFIC decode
    /// failure that layout produces. Returns whether the legacy fallback
    /// fired, so callers can refuse (`Database::open`) or proceed
    /// (`contextdb migrate`) accordingly.
    ///
    /// bincode's struct-as-tuple encoding carries no field-count marker, so
    /// a `Vec<ColumnDef>`/`TableMeta` decoder that optimistically reads past
    /// its OWN declared trailing fields does not cleanly stop at "no more
    /// fields for me" — it keeps consuming bytes that actually belong to the
    /// NEXT `ColumnDef` (or the next `TableMeta` field), and only surfaces an
    /// error once one of those borrowed bytes fails to satisfy the type it's
    /// forced into (observed as `InvalidBooleanValue` when a borrowed length
    /// or variant-tag byte lands on a `bool` field). The current
    /// `TableMeta`/`ColumnDef` `Deserialize` impls' own trailing-field
    /// `unwrap_or_default()` therefore only protects a genuinely SHORTER
    /// CURRENT-shaped payload (e.g. one written before the most recent
    /// additive field) — never a payload from a release whose `ColumnDef`
    /// had fewer fields than today's, which is exactly the `v1.0.0` case
    /// this fallback exists for.
    fn decode_table_meta_versioned(bytes: &[u8]) -> Result<(TableMeta, bool)> {
        match Self::decode::<TableMeta>(bytes) {
            Ok(meta) => Ok((meta, false)),
            Err(_) => {
                let legacy: LegacyTableMetaV1 = Self::decode(bytes)?;
                Ok((legacy.into(), true))
            }
        }
    }

    pub(crate) fn storage_error(err: impl std::fmt::Display) -> Error {
        Error::Other(format!("storage error: {err}"))
    }

    fn storage_open_error(path: &Path, err: redb::DatabaseError) -> Error {
        match err {
            redb::DatabaseError::DatabaseAlreadyOpen => Self::held_store_error(path),
            other => Self::storage_error(other),
        }
    }

    /// An open refused because somebody else holds this store, told WHO.
    ///
    /// The two holders a caller can be told about need different actions --
    /// wait for hydration to finish, or go and stop a writer -- so naming the
    /// wrong one sends the caller after something that will never happen.
    ///
    /// Reaching this refusal at all means the advisory companion beside the
    /// store was there for the taking, which a live writer never allows: a
    /// writer holds it for as long as it holds the store, and an opener that
    /// meets one is refused by name long before here. So ordinarily the
    /// holder is a direct reader, and the observed count is floored at one --
    /// the refusal itself proves a reader is there even if the scan raced its
    /// own best-effort breadcrumb.
    ///
    /// The exception is a store whose companion carries no published writer
    /// run at all. That record is durable and is never removed in the ordinary
    /// course, so its absence beside a store somebody is holding means the
    /// advisory file was taken away from under its holder and this opener
    /// created the replacement it now holds. The holder that lost it is a
    /// WRITER, and saying "held by readers" there invents readers nobody can
    /// find and sends the caller to wait for hydration that is not happening.
    fn held_store_error(path: &Path) -> Error {
        let (observed_direct_readers, verified_readers) = live_reader_holders(path);
        if observed_direct_readers == 0
            && verified_readers.is_empty()
            && inspect_companion_record(path).is_err()
        {
            return Self::held_by_writer_error_from_record(path);
        }
        let detail = HeldByReadersDetail {
            observed_direct_readers: observed_direct_readers.max(1),
            verified_readers,
        };
        Error::ReadFailure(
            ReadFailure::held_by_readers(detail)
                .expect("held reader observation constructs a valid typed refusal"),
        )
    }

    /// A writer holds this store, named from the record it published beside
    /// the store. A store whose record cannot be read -- a companion this
    /// opener had to create for itself, because the advisory file was taken
    /// away -- still names the store and the holder's kind, which is the part
    /// the caller acts on.
    fn held_by_writer_error_from_record(path: &Path) -> Error {
        let process_id = inspect_companion_record(path)
            .ok()
            .map(|record| u64::from(record.fields.process_id));
        Error::ReadFailure(
            ReadFailure::new(
                ReadFailureKind::HeldByWriter,
                ReadFailureDetail::HeldByWriter(HeldByWriterDetail {
                    process_id,
                    store_path: path.display().to_string(),
                }),
            )
            .expect("held-by-writer carries the writer-contention detail"),
        )
    }

    /// The filesystem refused a writable open. That is never a statement
    /// about the store's contents, so it must never become a corruption
    /// verdict that tells the operator to run a repair. Live direct readers
    /// make it the typed reader refusal, carrying the observed count and the
    /// verified breadcrumbs; without one it stays an I/O refusal naming the
    /// exact operating-system reason.
    fn permission_refused_open_error(path: &Path, err: redb::DatabaseError) -> Error {
        let (observed_direct_readers, verified_readers) = live_reader_holders(path);
        if observed_direct_readers == 0 {
            return Self::storage_error(err);
        }
        let detail = HeldByReadersDetail {
            observed_direct_readers,
            verified_readers,
        };
        Error::ReadFailure(
            ReadFailure::held_by_readers(detail)
                .expect("held reader observation constructs a valid typed refusal"),
        )
    }

    pub(crate) fn with_db<T>(&self, f: impl FnOnce(&redb::Database) -> Result<T>) -> Result<T> {
        let lock_guard = self.lock_companion_slot()?;
        if lock_guard.is_none() {
            return Err(Error::Other("database persistence is closed".to_string()));
        }
        let db_guard = self.lock_database()?;
        let db = db_guard
            .as_ref()
            .ok_or_else(|| Error::Other("database persistence is closed".to_string()))?;
        f(db)
    }
}

impl Drop for RedbPersistence {
    fn drop(&mut self) {
        let db = self
            .db
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        drop(db);
        if let Some(file) = self
            .lock_file
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
        {
            drop(file);
        }
        if let Some(source) = self
            .migration_replacement_source
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
        {
            drop(source);
        }
    }
}

fn try_lock_exclusive(file: &File) -> Result<bool> {
    match fs2::FileExt::try_lock_exclusive(file) {
        Ok(()) => Ok(true),
        Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => Ok(false),
        Err(err) => Err(Error::Other(format!("companion lock error: {err}"))),
    }
}

fn unlock_file(file: &File) {
    let _ = fs2::FileExt::unlock(file);
}

#[cfg(test)]
mod reader_breadcrumb_location_proofs {
    use super::*;

    /// A runtime directory an operator supplied, owner-only, outside any
    /// platform default.
    fn supplied_root(directory: &Path, name: &str) -> PathBuf {
        let root = directory.join(name);
        std::fs::create_dir(&root).expect("create the operator-supplied runtime root");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
                .expect("secure the operator-supplied runtime root");
        }
        root
    }

    /// A supplied runtime root says where this deployment's owner CHANNEL is.
    /// It does not move where readers write themselves down, because the
    /// writer a reader blocks is started by somebody else -- often with no
    /// runtime flag at all -- and looks in the default per-user location.
    #[test]
    fn a_supplied_runtime_root_does_not_move_the_breadcrumb_location() {
        let directory = tempfile::TempDir::new().expect("task-scoped runtime directory");
        let root = supplied_root(directory.path(), "supplied-runtime");
        let store = directory.path().join("packaged.db");
        std::fs::write(&store, b"").expect("the store this deployment names exists");

        let Some(runtime) = reader_breadcrumb_runtime_directory() else {
            // This machine has no usable per-user runtime location, so there
            // is nowhere for a breadcrumb to go and nothing to compare.
            return;
        };
        let where_readers_go = reader_breadcrumb_directory(&store, runtime.root())
            .expect("the default per-user runtime location derives a breadcrumb directory");
        let where_the_channel_is = reader_breadcrumb_directory(&store, &root)
            .expect("a supplied root derives a directory too, and it is not this one");

        assert!(
            where_readers_go.starts_with(runtime.root()),
            "readers are written down in the default per-user runtime location"
        );
        assert_ne!(
            where_readers_go, where_the_channel_is,
            "a supplied root that moved the breadcrumb would give the two sides of one store a \
             flag to disagree on, and a writer would report a held store as held by nobody"
        );
    }

    /// The other half: a note somebody left inside a supplied runtime root is
    /// not what a writer reads, and the writer does not touch it either.
    #[test]
    fn a_writer_never_reads_the_breadcrumbs_of_a_supplied_runtime_root() {
        let directory = tempfile::TempDir::new().expect("task-scoped runtime directory");
        let root = supplied_root(directory.path(), "supplied-runtime");
        let store = directory.path().join("packaged.db");
        std::fs::write(&store, b"").expect("the store this deployment names exists");
        let breadcrumb = ReaderBreadcrumb {
            process_id: std::process::id(),
            process_start: contextdb_core::read_contract::ProcessStartIdentity(1),
            process_name: "packaged-reader".to_owned(),
        };
        let published = read_persistence_test_scaffold::create_unlocked_reader_breadcrumb_for_test(
            &store,
            &root,
            &breadcrumb,
        )
        .expect("publish a reader breadcrumb inside the supplied runtime root");
        assert!(published.exists(), "the breadcrumb was published there");

        let (observed, verified) = live_reader_holders(&store);
        assert_eq!(
            observed, 0,
            "a writer reads the default per-user runtime location, so a note inside a supplied \
             root is not one of this store's readers"
        );
        assert!(verified.is_empty());
        assert!(
            published.exists(),
            "the writer never went near the supplied root, so what an operator keeps there is \
             left exactly as it was"
        );
    }

    /// A real store nobody is holding, released at once.
    fn idle_store(directory: &Path, name: &str) -> PathBuf {
        let store = directory.join(name);
        drop(RedbPersistence::create(&store).expect("claim and release a real store"));
        store
    }

    /// The wait answers immediately when nobody holds the store. The verdict
    /// comes from the readers themselves, so a deployment's runtime directory
    /// neither supplies it nor withholds it.
    #[test]
    fn a_store_nobody_holds_is_released_at_once() {
        let directory = tempfile::TempDir::new().expect("task-scoped runtime directory");
        let root = supplied_root(directory.path(), "supplied-runtime");
        let store = idle_store(directory.path(), "packaged.db");
        let resolved = RuntimeDirectory::supplied(&root);
        let stop = OwnerReadCancellation::default();

        assert!(
            matches!(
                wait_for_reader_release(&store, Some(&resolved), &stop),
                ReaderReleaseWait::Released
            ),
            "no reader is holding this store, so there is nothing to wait for"
        );
    }

    /// A pathname that is not a store has no companion beside it, so there is
    /// nowhere for a reader of it to record a hold and nothing this wait can
    /// observe. A caller about to take it is told that, never `Released`.
    #[test]
    fn a_pathname_with_no_companion_is_unobservable_not_released() {
        let directory = tempfile::TempDir::new().expect("task-scoped runtime directory");
        let root = supplied_root(directory.path(), "supplied-runtime");
        let store = directory.path().join("not-a-store.db");
        std::fs::write(&store, b"").expect("plant a pathname that is not a store");
        let resolved = RuntimeDirectory::supplied(&root);
        let stop = OwnerReadCancellation::default();

        assert!(
            matches!(
                wait_for_reader_release(&store, Some(&resolved), &stop),
                ReaderReleaseWait::Unobservable(_)
            ),
            "a pathname with no coordination artifact beside it is not a store with no readers"
        );
    }

    /// The door a packaged consumer can actually reach: it holds the runtime
    /// ROOT its deployment named, not a directory this crate resolved, and it
    /// still reaches the wait below it and answers for the store.
    #[test]
    fn a_supplied_root_still_reaches_the_wait_below_it() {
        let directory = tempfile::TempDir::new().expect("task-scoped runtime directory");
        let root = supplied_root(directory.path(), "supplied-runtime");
        let store = idle_store(directory.path(), "packaged.db");
        let stop = OwnerReadCancellation::default();

        assert!(
            matches!(
                wait_for_reader_release_in_runtime_dir(&store, Some(&root), &stop),
                ReaderReleaseWait::Released
            ),
            "the root-taking door resolves what it was given and answers for the store"
        );
    }

    /// A root this process cannot read says nothing about this store's
    /// readers. Where the owner's channel lives is a different question, and a
    /// caller waiting on a store nobody is reading is answered from the store.
    #[test]
    fn a_root_that_cannot_be_read_does_not_decide_the_answer() {
        let directory = tempfile::TempDir::new().expect("task-scoped runtime directory");
        let store = idle_store(directory.path(), "packaged.db");
        let absent = directory.path().join("no-such-runtime-directory");
        let stop = OwnerReadCancellation::default();

        assert!(
            matches!(
                wait_for_reader_release_in_runtime_dir(&store, Some(&absent), &stop),
                ReaderReleaseWait::Released
            ),
            "nobody is holding this store, and an unreadable channel root is not a reason to \
             refuse to say so"
        );
    }

    /// Without a supplied root the new door is the old one: the platform's own
    /// runtime location, same answer.
    #[test]
    fn no_supplied_root_is_the_ordinary_wait() {
        let directory = tempfile::TempDir::new().expect("task-scoped runtime directory");
        let store = directory.path().join("packaged.db");
        std::fs::write(&store, b"").expect("the store this deployment names exists");
        let stop = OwnerReadCancellation::default();
        stop.cancel();

        assert!(
            matches!(
                wait_for_reader_release_in_runtime_dir(&store, None, &stop),
                ReaderReleaseWait::Stopped
            ),
            "the root-taking door carries the same stop contract as the one it delegates to"
        );
    }

    /// A caller that has already stopped is answered at once, with no wait.
    #[test]
    fn a_stopped_wait_answers_without_waiting() {
        let directory = tempfile::TempDir::new().expect("task-scoped runtime directory");
        let root = supplied_root(directory.path(), "supplied-runtime");
        let store = directory.path().join("packaged.db");
        std::fs::write(&store, b"").expect("the store this deployment names exists");
        let resolved = RuntimeDirectory::supplied(&root);
        let stop = OwnerReadCancellation::default();
        stop.cancel();

        assert!(
            matches!(
                wait_for_reader_release(&store, Some(&resolved), &stop),
                ReaderReleaseWait::Stopped
            ),
            "an interrupt is as prompt as a release"
        );
    }
}

#[cfg(test)]
mod recorded_owner_state_proofs {
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
        let directory = tempfile::TempDir::new().expect("task-scoped store directory");
        let store = directory.path().join("released.db");
        std::fs::write(&store, b"").expect("the store exists");
        let companion =
            CompanionGuard::acquire(&store, CompanionAdmission::NewStore).expect("claim the store");
        companion
            .publish_writer_run(&store, true)
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
            .publish_writer_run(&store, true)
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
            .publish_writer_run(&planted, true)
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
            .publish_writer_run(&store, true)
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
            .publish_writer_run(&store, true)
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
            .publish_writer_run(&store, false)
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
}
