//! Admission to the PHYSICAL store, decided on the data file itself.
//!
//! Every lock a writer and a direct reader coordinate through lives as an
//! open-file-description byte lock on the store's own data file. So every
//! name of the store -- the path it was opened by, a hard link, a renamed
//! path -- and a reader with no companion beside it all meet the same locks:
//! the kernel keys them by inode, not by pathname. Nothing is written; these
//! are advisory locks on bytes far past any reachable file length, and far
//! past the reader-presence ranges `reader_presence` scans.
//!
//! The bytes, all on the data file:
//!
//! - `CLAIM`: a writer holds it exclusive from its claim until it becomes
//!   quiescent or closes. Nobody else ever locks it; they ask about it.
//! - `READER_HOLD`: a starting reader holds it shared for exactly as long as it
//!   holds the data lock during admission. A writer takes it exclusive while
//!   acquiring the store.
//! - `GATE`: taken shared by a reader only inside the one request that also
//!   takes `READER_HOLD`, and given back at once. A writer holding it exclusive
//!   makes that request fail, so no reader can enter once the writer's gate is
//!   granted.
//! - `DATA`: the store's ownership lock. A writer holds it exclusive for its
//!   whole life (shared once quiescent); a reader holds it shared while it
//!   captures its snapshot.
//!
//! The data lock was a whole-file `flock`. It is one kind of lock with the
//! others now because on some kernels a whole-file `flock` and byte locks share
//! one lock list and would collide with each other.

use std::fs::File;
use std::io;
use std::os::fd::AsRawFd as _;

// The offsets below do not fit a 32-bit `off_t`; refuse to build on an ABI
// whose lock call would silently truncate them.
const _: () = assert!(std::mem::size_of::<libc::off_t>() == 8);

pub(crate) const ADMISSION_BASE: i64 = 1 << 62;
pub(crate) const CLAIM_OFFSET: i64 = ADMISSION_BASE;
pub(crate) const READER_HOLD_OFFSET: i64 = ADMISSION_BASE + 1;
pub(crate) const GATE_OFFSET: i64 = ADMISSION_BASE + 2;
pub(crate) const DATA_OFFSET: i64 = ADMISSION_BASE + 3;

// Reader presence occupies [1 << 60, 3 << 60); admission stays clear of it.
const _: () = assert!(ADMISSION_BASE >= 3 << 60);
const _: () = assert!(GATE_OFFSET == READER_HOLD_OFFSET + 1);

/// How a lock on the data file is asked for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ByteLock {
    Shared,
    Exclusive,
}

impl ByteLock {
    pub(super) fn lock_type(self) -> libc::c_short {
        match self {
            Self::Shared => libc::F_RDLCK as libc::c_short,
            Self::Exclusive => libc::F_WRLCK as libc::c_short,
        }
    }
}

fn request(start: i64, len: i64, lock_type: libc::c_short) -> libc::flock {
    // SAFETY: an all-zero `flock` is a valid value; every field the call reads
    // is set below.
    let mut lock: libc::flock = unsafe { std::mem::zeroed() };
    lock.l_type = lock_type;
    lock.l_whence = libc::SEEK_SET as libc::c_short;
    lock.l_start = start as libc::off_t;
    lock.l_len = len as libc::off_t;
    lock
}

/// Reopen the held inode, never its potentially replaced pathname. A dup
/// would alias OFD locks and could convert or release another operation's hold.
#[cfg(target_os = "linux")]
pub(super) fn reopen_independent(file: &File, writable: bool) -> io::Result<File> {
    use std::os::unix::fs::OpenOptionsExt as _;
    let opened = std::fs::OpenOptions::new()
        .read(true)
        .write(writable)
        .custom_flags(libc::O_CLOEXEC)
        .open(format!("/proc/self/fd/{}", file.as_raw_fd()))?;
    // SAFETY: both descriptors stay live and both stat buffers are writable.
    let (original, current) = unsafe {
        let mut original: libc::stat = std::mem::zeroed();
        let mut current: libc::stat = std::mem::zeroed();
        if libc::fstat(file.as_raw_fd(), &mut original) != 0
            || libc::fstat(opened.as_raw_fd(), &mut current) != 0
        {
            return Err(io::Error::last_os_error());
        }
        (original, current)
    };
    if original.st_dev != current.st_dev || original.st_ino != current.st_ino {
        return Err(io::Error::other(
            "reopened reader coordination inode changed",
        ));
    }
    Ok(opened)
}

/// Whether one non-blocking request was granted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Granted {
    Yes,
    HeldByAnother,
}

fn try_range(file: &File, start: i64, len: i64, mode: ByteLock) -> io::Result<Granted> {
    let mut lock = request(start, len, mode.lock_type());
    // SAFETY: `lock` is initialized and the descriptor stays owned by `file`.
    if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_OFD_SETLK, &mut lock) } != -1 {
        return Ok(Granted::Yes);
    }
    let error = io::Error::last_os_error();
    match error.raw_os_error() {
        Some(libc::EAGAIN) | Some(libc::EACCES) => Ok(Granted::HeldByAnother),
        _ => Err(error),
    }
}

/// One blocking request. The caller owns the signal discipline around it; a
/// request the kernel interrupts is returned as the interruption, never
/// repeated here.
pub(crate) fn block_on(file: &File, offset: i64, mode: ByteLock) -> io::Result<()> {
    let mut lock = request(offset, 1, mode.lock_type());
    // SAFETY: as in `try_range`.
    if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_OFD_SETLKW, &mut lock) } != -1 {
        return Ok(());
    }
    Err(io::Error::last_os_error())
}

/// Ask for one byte without blocking.
pub(crate) fn try_byte(file: &File, offset: i64, mode: ByteLock) -> io::Result<Granted> {
    try_range(file, offset, 1, mode)
}

/// Give back exactly one byte this descriptor holds. Unlocking never touches
/// any byte but the one named, so reader presence and the other admission
/// bytes this description holds are left exactly as they were.
pub(crate) fn release_byte(file: &File, offset: i64) -> io::Result<()> {
    let mut lock = request(offset, 1, libc::F_UNLCK as libc::c_short);
    // SAFETY: as in `try_range`.
    if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_OFD_SETLK, &mut lock) } != -1 {
        return Ok(());
    }
    Err(io::Error::last_os_error())
}

/// Whether some OTHER description holds `offset` in a way that conflicts with
/// `probe`. Holds nothing: the question is put to the kernel, not answered by
/// taking the lock, so asking can never make a writer's own request fail.
pub(crate) fn held_against(file: &File, offset: i64, probe: ByteLock) -> io::Result<bool> {
    let mut lock = request(offset, 1, probe.lock_type());
    // SAFETY: as in `try_range`.
    if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_OFD_GETLK, &mut lock) } == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok(lock.l_type != libc::F_UNLCK as libc::c_short)
}

/// Take the store's ownership lock without waiting.
pub(crate) fn try_data(file: &File, mode: ByteLock) -> io::Result<Granted> {
    let granted = try_byte(file, DATA_OFFSET, mode)?;
    #[cfg(any(test, feature = "test-seams"))]
    if granted == Granted::Yes && mode == ByteLock::Shared {
        count_for_test(file, |counts| counts.data_shared_grants += 1);
    }
    Ok(granted)
}

#[cfg(any(test, feature = "test-seams"))]
type CountsByStore = std::collections::HashMap<
    (u64, u64),
    super::read_persistence_test_scaffold::PhysicalAdmissionCounts,
>;

#[cfg(any(test, feature = "test-seams"))]
static COUNTS: std::sync::OnceLock<(std::sync::Mutex<CountsByStore>, std::sync::Condvar)> =
    std::sync::OnceLock::new();

#[cfg(any(test, feature = "test-seams"))]
fn counts() -> &'static (std::sync::Mutex<CountsByStore>, std::sync::Condvar) {
    COUNTS.get_or_init(Default::default)
}

#[cfg(any(test, feature = "test-seams"))]
fn count_for_test(
    file: &File,
    change: impl FnOnce(&mut super::read_persistence_test_scaffold::PhysicalAdmissionCounts),
) {
    use std::os::unix::fs::MetadataExt as _;
    let Ok(metadata) = file.metadata() else {
        return;
    };
    let (counts, changed) = counts();
    let mut counts = counts
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    change(counts.entry((metadata.dev(), metadata.ino())).or_default());
    changed.notify_all();
}

#[cfg(any(test, feature = "test-seams"))]
pub(crate) fn wait_for_counts_for_test(
    identity: (u64, u64),
    reached: impl Fn(&super::read_persistence_test_scaffold::PhysicalAdmissionCounts) -> bool,
    within: std::time::Duration,
) -> bool {
    let (counts, changed) = counts();
    let mut counts = counts
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let deadline = std::time::Instant::now() + within;
    loop {
        if reached(&counts.get(&identity).copied().unwrap_or_default()) {
            return true;
        }
        let left = deadline.saturating_duration_since(std::time::Instant::now());
        if left.is_zero() {
            return false;
        }
        counts = changed
            .wait_timeout(counts, left)
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .0;
    }
}

#[cfg(any(test, feature = "test-seams"))]
pub(crate) fn counts_for_test(
    identity: (u64, u64),
) -> super::read_persistence_test_scaffold::PhysicalAdmissionCounts {
    counts()
        .0
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(&identity)
        .copied()
        .unwrap_or_default()
}

/// Turn a held exclusive data lock into a shared one in one kernel request:
/// there is no instant at which the description holds neither.
pub(crate) fn downgrade_data(file: &File) -> io::Result<()> {
    match try_byte(file, DATA_OFFSET, ByteLock::Shared)? {
        Granted::Yes => Ok(()),
        // A description converting its own exclusive lock conflicts with
        // nobody; being refused means it did not hold what it was told it did.
        Granted::HeldByAnother => Err(io::Error::other(
            "the data lock could not be converted to shared",
        )),
    }
}

/// Give back the ownership lock and nothing else.
pub(crate) fn release_data(file: &File) -> io::Result<()> {
    release_byte(file, DATA_OFFSET)
}

/// Why a starting reader or cleaner was not admitted.
#[derive(Debug)]
pub(crate) enum ReaderGateRefusal {
    /// A writer has claimed this physical store, through whichever name.
    Claimed,
    /// A writer is acquiring the store: its gate (or reader hold) is held.
    GateHeld,
    /// The data lock is held exclusive: a writer owns the store.
    DataHeld,
    /// The descriptor is not the store the caller authenticated.
    IdentityChanged,
    Io(io::Error),
}

impl std::fmt::Display for ReaderGateRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Claimed => f.write_str("a writer has claimed this store"),
            Self::GateHeld => f.write_str("a writer is acquiring this store"),
            Self::DataHeld => f.write_str("the data descriptor is held by a writer"),
            Self::IdentityChanged => f.write_str("the data descriptor is a different store"),
            Self::Io(error) => write!(f, "store admission failed: {error}"),
        }
    }
}

/// A reader's admission: the reader hold and the shared data lock, on one
/// descriptor. Dropping it gives back the data lock, then the reader hold,
/// and nothing else the descriptor holds.
pub(crate) struct ReaderGateHold<'a> {
    file: &'a File,
    data: bool,
    reader_hold: bool,
}

impl ReaderGateHold<'_> {
    /// Give back the data lock now, keeping the reader hold.
    pub(crate) fn release_data(&mut self) -> io::Result<()> {
        if self.data {
            release_data(self.file)?;
            self.data = false;
        }
        Ok(())
    }

    /// Give back everything, data lock first.
    pub(crate) fn release(mut self) -> io::Result<()> {
        self.release_data()?;
        if self.reader_hold {
            release_byte(self.file, READER_HOLD_OFFSET)?;
            self.reader_hold = false;
        }
        Ok(())
    }

    /// Keep the shared data lock on the descriptor past this guard; the
    /// caller now owns giving it back. The reader hold is released here.
    pub(crate) fn keep_data_release_hold(mut self) -> io::Result<()> {
        self.data = false;
        if self.reader_hold {
            release_byte(self.file, READER_HOLD_OFFSET)?;
            self.reader_hold = false;
        }
        Ok(())
    }
}

impl Drop for ReaderGateHold<'_> {
    fn drop(&mut self) {
        if self.data {
            let _ = release_data(self.file);
        }
        if self.reader_hold {
            let _ = release_byte(self.file, READER_HOLD_OFFSET);
        }
    }
}

/// Admit one starting reader (or cleaner) to the physical store.
///
/// The one door every shared-data entrant comes through, whichever name it
/// opened the store by and whether or not a companion exists:
///
/// 1. it checks the descriptor is the inode the caller authenticated;
/// 2. it ASKS whether a writer has claimed the store, and holds nothing;
/// 3. one request takes the reader hold and the gate together, shared, and
///    fails if a writer holds either -- the gate's grant to a writer is the
///    instant after which no reader enters;
/// 4. it gives the gate straight back, keeping the reader hold;
/// 5. it takes the data lock shared without waiting.
///
/// Every refusal returns with nothing held.
pub(crate) fn admit_through_reader_gate(
    data: &File,
    physical_identity: (u64, u64),
) -> Result<ReaderGateHold<'_>, ReaderGateRefusal> {
    use std::os::unix::fs::MetadataExt as _;

    let metadata = data.metadata().map_err(ReaderGateRefusal::Io)?;
    if (metadata.dev(), metadata.ino()) != physical_identity {
        return Err(ReaderGateRefusal::IdentityChanged);
    }
    if held_against(data, CLAIM_OFFSET, ByteLock::Shared).map_err(ReaderGateRefusal::Io)? {
        return Err(ReaderGateRefusal::Claimed);
    }
    #[cfg(any(test, feature = "test-seams"))]
    super::read_persistence_test_scaffold::pause_reader_at_physical(
        physical_identity,
        super::read_persistence_test_scaffold::ReaderPausePoint::AfterPrecheck,
    );
    #[cfg(any(test, feature = "test-seams"))]
    count_for_test(data, |counts| counts.gate_requests += 1);
    match try_range(data, READER_HOLD_OFFSET, 2, ByteLock::Shared).map_err(ReaderGateRefusal::Io)? {
        Granted::Yes => {
            #[cfg(any(test, feature = "test-seams"))]
            count_for_test(data, |counts| counts.hold_shared_grants += 1);
        }
        Granted::HeldByAnother => {
            #[cfg(any(test, feature = "test-seams"))]
            count_for_test(data, |counts| counts.hold_range_refusals += 1);
            return Err(ReaderGateRefusal::GateHeld);
        }
    }
    let mut hold = ReaderGateHold {
        file: data,
        data: false,
        reader_hold: true,
    };
    if let Err(error) = release_byte(data, GATE_OFFSET) {
        // Give back both bytes of the range before refusing: the gate a writer
        // must be able to take, and (through `hold`) the reader hold.
        let _ = release_byte(data, GATE_OFFSET);
        return Err(ReaderGateRefusal::Io(error));
    }
    #[cfg(any(test, feature = "test-seams"))]
    super::read_persistence_test_scaffold::pause_reader_at_physical(
        physical_identity,
        super::read_persistence_test_scaffold::ReaderPausePoint::AfterGate,
    );
    match try_data(data, ByteLock::Shared).map_err(ReaderGateRefusal::Io)? {
        Granted::Yes => hold.data = true,
        // `hold` drops here and gives the reader hold back.
        Granted::HeldByAnother => return Err(ReaderGateRefusal::DataHeld),
    }
    Ok(hold)
}

/// How a writer's acquisition of the store ended without the store.
#[derive(Debug)]
pub(crate) enum WriterAcquireRefusal {
    /// Another writer holds this physical store, through whichever name.
    HeldByWriter,
    /// The caller's deadline had already passed with readers still admitted.
    DeadlineExpired,
    /// The caller cancelled.
    Cancelled,
    /// The caller asked for a cancellable or bounded wait this process cannot
    /// give it.
    CancellableWaitUnsupported,
    /// The helper ended without handing over the store.
    HelperFailed(String),
    /// The helper that was acquiring could not be confirmed dead: the caller
    /// must keep its description open, holding everything, in quarantine.
    #[cfg(all(
        target_os = "linux",
        any(target_arch = "x86_64", target_arch = "aarch64")
    ))]
    Quarantined(super::writer_wait_helper::Helper),
    Io(io::Error),
}

/// What the caller allows a writer's wait for starting readers.
pub(crate) struct WriterWait<'a> {
    pub(crate) stop: Option<&'a contextdb_core::read_contract::OwnerReadCancellation>,
    pub(crate) deadline: Option<(
        std::sync::Arc<dyn contextdb_core::read_contract::DeadlineClock>,
        u64,
    )>,
}

impl WriterWait<'_> {
    pub(crate) const UNTIL_HANDED_OVER: WriterWait<'static> = WriterWait {
        stop: None,
        deadline: None,
    };

    fn is_plain(&self) -> bool {
        self.stop.is_none() && self.deadline.is_none()
    }

    fn cancelled(&self) -> bool {
        self.stop.is_some_and(|stop| stop.is_cancelled())
    }

    fn expired(&self) -> bool {
        self.deadline
            .as_ref()
            .is_some_and(|(clock, deadline)| clock.now_ms() >= *deadline)
    }
}

/// The admission bytes one writer description holds while acquiring.
struct Acquiring<'a> {
    file: &'a File,
    claim: bool,
    gate: bool,
    reader_hold: bool,
    data: bool,
}

impl Acquiring<'_> {
    fn release_coordination(&mut self) {
        if self.gate {
            let _ = release_byte(self.file, GATE_OFFSET);
            self.gate = false;
        }
        if self.reader_hold {
            let _ = release_byte(self.file, READER_HOLD_OFFSET);
            self.reader_hold = false;
        }
    }
}

impl Drop for Acquiring<'_> {
    fn drop(&mut self) {
        self.release_coordination();
        if self.data {
            let _ = release_data(self.file);
        }
        if self.claim {
            let _ = release_byte(self.file, CLAIM_OFFSET);
        }
    }
}

/// Every catchable signal blocked on this thread for one scope, and the
/// thread's own mask put back on every exit. A blocking lock request a signal
/// handler cannot run during cannot be interrupted, so it is made once and
/// never repeated; the handler runs when the mask is restored.
struct SignalsBlocked {
    previous: libc::sigset_t,
}

impl SignalsBlocked {
    fn now() -> io::Result<Self> {
        // SAFETY: both sets are fully initialized by sigfillset/the call.
        unsafe {
            let mut all: libc::sigset_t = std::mem::zeroed();
            libc::sigfillset(&mut all);
            let mut previous: libc::sigset_t = std::mem::zeroed();
            let answered = libc::pthread_sigmask(libc::SIG_SETMASK, &all, &mut previous);
            if answered != 0 {
                return Err(io::Error::from_raw_os_error(answered));
            }
            Ok(Self { previous })
        }
    }
}

impl Drop for SignalsBlocked {
    fn drop(&mut self) {
        // SAFETY: `previous` was filled by pthread_sigmask above.
        unsafe {
            libc::pthread_sigmask(libc::SIG_SETMASK, &self.previous, std::ptr::null_mut());
        }
    }
}

/// Acquire the physical store for a writer on `file`, the read-write
/// descriptor it will keep: the claim, then the data lock, waiting once for
/// the readers already admitted when it has to.
///
/// On success the description holds the claim and the data lock exclusive;
/// the gate and reader hold it took on the way are given back. On every other
/// outcome it holds nothing it took here.
pub(crate) fn acquire_for_writer(
    file: &File,
    wait: &WriterWait<'_>,
    #[allow(unused_variables)] store_path: &std::path::Path,
) -> Result<(), WriterAcquireRefusal> {
    let mut held = Acquiring {
        file,
        claim: false,
        gate: false,
        reader_hold: false,
        data: false,
    };
    match try_byte(file, CLAIM_OFFSET, ByteLock::Exclusive).map_err(WriterAcquireRefusal::Io)? {
        Granted::Yes => held.claim = true,
        Granted::HeldByAnother => return Err(WriterAcquireRefusal::HeldByWriter),
    }
    // Always in the order gate, reader hold, data lock: a reader already
    // admitted (it holds the reader hold and may not yet have asked for the
    // data lock) is waited out, never overtaken. Each is asked for without
    // waiting first; an idle store costs three requests and no wait.
    held.gate = try_byte(file, GATE_OFFSET, ByteLock::Exclusive)
        .map_err(WriterAcquireRefusal::Io)?
        == Granted::Yes;
    if held.gate {
        held.reader_hold = try_byte(file, READER_HOLD_OFFSET, ByteLock::Exclusive)
            .map_err(WriterAcquireRefusal::Io)?
            == Granted::Yes;
    }
    if held.gate
        && held.reader_hold
        && try_byte(file, DATA_OFFSET, ByteLock::Exclusive).map_err(WriterAcquireRefusal::Io)?
            == Granted::Yes
    {
        held.data = true;
        held.release_coordination();
        held.claim = false;
        held.data = false;
        #[cfg(any(test, feature = "test-seams"))]
        note_acquired(store_path);
        return Ok(());
    }
    if wait.cancelled() {
        return Err(WriterAcquireRefusal::Cancelled);
    }
    if wait.expired() {
        return Err(WriterAcquireRefusal::DeadlineExpired);
    }
    if !wait.is_plain() {
        #[cfg(all(
            target_os = "linux",
            any(target_arch = "x86_64", target_arch = "aarch64")
        ))]
        {
            let mut missing = Vec::with_capacity(3);
            if !held.gate {
                missing.push(GATE_OFFSET);
            }
            if !held.reader_hold {
                missing.push(READER_HOLD_OFFSET);
            }
            missing.push(DATA_OFFSET);
            #[cfg(any(test, feature = "test-seams"))]
            count_for_test(file, |counts| {
                counts.writer_hold_waits += missing.len() as u64
            });
            use super::writer_wait_helper::{WaitEnded, wait_for_helper};
            match wait_for_helper(file, &missing, wait.stop, wait.deadline.clone(), store_path) {
                WaitEnded::Granted => {
                    held.gate = true;
                    held.reader_hold = true;
                    held.data = true;
                }
                WaitEnded::Refused(refusal) => {
                    // The helper may have been granted some bytes before it
                    // was ended; give back every one it was asked for.
                    held.gate = true;
                    held.reader_hold = true;
                    held.data = true;
                    return Err(refusal);
                }
                WaitEnded::Quarantine(helper) => {
                    // Nothing is released: the description keeps every byte
                    // the helper may still be granted, in quarantine.
                    held.claim = false;
                    held.gate = false;
                    held.reader_hold = false;
                    held.data = false;
                    return Err(WriterAcquireRefusal::Quarantined(helper));
                }
            }
            held.release_coordination();
            held.claim = false;
            held.data = false;
            #[cfg(any(test, feature = "test-seams"))]
            note_acquired(store_path);
            return Ok(());
        }
        #[cfg(not(all(
            target_os = "linux",
            any(target_arch = "x86_64", target_arch = "aarch64")
        )))]
        return Err(WriterAcquireRefusal::CancellableWaitUnsupported);
    }
    {
        let _blocked = SignalsBlocked::now().map_err(WriterAcquireRefusal::Io)?;
        if !held.gate {
            writer_takes(file, GATE_OFFSET)?;
            held.gate = true;
        }
        // The gate is held: no reader can enter from here. What is left is
        // waiting out the readers already inside.
        #[cfg(any(test, feature = "test-seams"))]
        super::read_persistence_test_scaffold::note_writer_admission_for_test(
            store_path,
            super::read_persistence_test_scaffold::WriterAdmissionEvent::Waiting,
        );
        if !held.reader_hold {
            writer_takes(file, READER_HOLD_OFFSET)?;
            held.reader_hold = true;
        }
        // Readers hold the data lock only inside the reader hold, so once the
        // hold is granted the data lock is normally free: asked for without
        // waiting first, and waited for only if a holder outside the gate
        // (a reader with no companion, a cleaner) still has it.
        writer_takes(file, DATA_OFFSET)?;
        held.data = true;
    }
    held.release_coordination();
    held.claim = false;
    held.data = false;
    #[cfg(any(test, feature = "test-seams"))]
    note_acquired(store_path);
    Ok(())
}

/// One exclusive byte for a writer: asked for without waiting, and only if
/// it is held, one blocking request, made once.
fn writer_takes(file: &File, offset: i64) -> Result<(), WriterAcquireRefusal> {
    if try_byte(file, offset, ByteLock::Exclusive).map_err(WriterAcquireRefusal::Io)?
        == Granted::Yes
    {
        return Ok(());
    }
    writer_blocks_on(file, offset)
}

/// One blocking exclusive request by a writer, made once.
fn writer_blocks_on(file: &File, offset: i64) -> Result<(), WriterAcquireRefusal> {
    #[cfg(any(test, feature = "test-seams"))]
    count_for_test(file, |counts| counts.writer_hold_waits += 1);
    block_on(file, offset, ByteLock::Exclusive).map_err(WriterAcquireRefusal::Io)
}

#[cfg(any(test, feature = "test-seams"))]
fn note_acquired(store_path: &std::path::Path) {
    super::read_persistence_test_scaffold::note_writer_admission_for_test(
        store_path,
        super::read_persistence_test_scaffold::WriterAdmissionEvent::Acquired,
    );
}

/// Give back what a writer holds on the physical store: the data lock and the
/// claim, and nothing else.
pub(crate) fn release_writer(file: &File) {
    let _ = release_data(file);
    let _ = release_byte(file, CLAIM_OFFSET);
}

/// The writer has stopped every write and reclamation: its data lock becomes
/// shared in one request, then its claim is given back so direct readers may
/// load the committed root during drain.
pub(crate) fn writer_becomes_quiescent(file: &File) -> io::Result<()> {
    downgrade_data(file)?;
    release_byte(file, CLAIM_OFFSET)
}

impl ReaderGateHold<'_> {
    /// Hand both held bytes to an owner that tracks and releases them itself;
    /// this guard then releases nothing.
    pub(crate) fn transfer_to_owner(mut self) {
        self.data = false;
        self.reader_hold = false;
    }
}
