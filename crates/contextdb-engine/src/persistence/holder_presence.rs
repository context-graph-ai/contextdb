//! Byte-free exclusive presence that proves a unit of background work still has a live holder.
//!
//! A process that owns work beside a store — work it carries on while its `Database` handle is
//! closed, such as computing embeddings between two short writes — holds an exclusive lock on one
//! byte of the store's empty `.holders` companion for as long as it owns that work. The byte is
//! chosen from the work's 16-byte token. These are Linux open-file-description locks, the same
//! contract as reader presence: the lock belongs to the guard's own descriptor, so closing a
//! `Database` never releases it, and the kernel releases it the moment the holder exits for any
//! reason, a kill or a crash included. A stopped or suspended holder keeps it. Holder locks never
//! touch the data file or the `.lock` companion, so they can neither hide nor impersonate a reader
//! or the writer.
//!
//! Presence reports liveness only. A consumer that publishes under a claim still fences that
//! publication on its own token inside the publishing transaction, because a holder can lose its
//! lock without exiting (a deleted companion, a network filesystem).
//!
//! Every presence a process holds beside one store shares a single open description of the
//! companion, each on its own byte, so holding many units of work costs one descriptor per store.
//! Asking and waiting always use a description of their own, which is what lets them see a lock
//! this same process holds.
//!
//! A consumer whose ownership outlives one call — a claim taken by one call and published by a
//! later one — hands the guard to [`HolderPresence::keep`] under a label of its own, asks for it
//! again with [`kept_token`] and lets it go with [`release_kept`], so it keeps no process state of
//! its own. A kept presence also ends when the process exits.

use std::path::Path;

use contextdb_core::{Error, Result, store_holders_path};

#[cfg(all(target_os = "linux", any(test, feature = "test-seams")))]
use std::collections::BTreeMap;
#[cfg(target_os = "linux")]
use std::collections::HashMap;
#[cfg(target_os = "linux")]
use std::fs::{File, OpenOptions};
#[cfg(target_os = "linux")]
use std::io;
#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;
#[cfg(target_os = "linux")]
use std::os::unix::fs::OpenOptionsExt;
#[cfg(target_os = "linux")]
use std::path::PathBuf;
#[cfg(target_os = "linux")]
use std::sync::{Arc, Mutex, Weak};

/// Offsets stay below 2^62, so every one-byte range fits `off_t` with room to spare.
#[cfg(target_os = "linux")]
const RANGE: u64 = 1 << 62;

/// Whether a token's holder is alive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Holder {
    /// A live process holds the token's presence.
    Present,
    /// No process holds it: its holder released it or exited.
    Absent,
}

/// The held presence of one token. Dropping it releases the lock.
#[derive(Debug)]
pub struct HolderPresence {
    #[cfg(target_os = "linux")]
    file: Arc<File>,
    #[cfg(target_os = "linux")]
    companion: PathBuf,
    #[cfg(target_os = "linux")]
    token: [u8; 16],
    #[cfg(target_os = "linux")]
    offset: i64,
}

/// The one description of each `.holders` companion this process holds presence through, shared
/// by every live [`HolderPresence`] beside that store and closed with the last of them.
#[cfg(target_os = "linux")]
static COMPANIONS: Mutex<Option<HashMap<PathBuf, Weak<File>>>> = Mutex::new(None);

/// Presences this process keeps after their guards are gone, by companion and the keeper's label.
#[cfg(target_os = "linux")]
static KEPT: Mutex<Option<HashMap<(PathBuf, [u8; 16]), HolderPresence>>> = Mutex::new(None);

/// Offsets each live presence holds, by companion, so a test can make this process lose them
/// without exiting.
#[cfg(all(target_os = "linux", any(test, feature = "test-seams")))]
static LOSABLE: Mutex<Option<HashMap<PathBuf, BTreeMap<i64, usize>>>> = Mutex::new(None);

#[cfg(target_os = "linux")]
fn shared_companion(path: &Path) -> io::Result<Arc<File>> {
    let mut companions = COMPANIONS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let companions = companions.get_or_insert_with(HashMap::new);
    if let Some(file) = companions.get(path).and_then(Weak::upgrade) {
        return Ok(file);
    }
    companions.retain(|_, file| file.strong_count() > 0);
    let file = Arc::new(
        OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .mode(0o600)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
            .open(path)?,
    );
    companions.insert(path.to_path_buf(), Arc::downgrade(&file));
    Ok(file)
}

#[cfg(target_os = "linux")]
impl Drop for HolderPresence {
    fn drop(&mut self) {
        #[cfg(any(test, feature = "test-seams"))]
        if !forget_losable(&self.companion, self.offset) {
            // A test already made this process lose the lock; the byte may belong to nobody now.
            return;
        }
        let _ = lock(
            &self.file,
            self.offset,
            libc::F_UNLCK as libc::c_short,
            libc::F_OFD_SETLK,
        );
    }
}

#[cfg(target_os = "linux")]
impl HolderPresence {
    /// Keep this presence held beside its store under `label` once the guard is gone, until
    /// [`release_kept`] or the process exits. Refuses, releasing this presence, when this process
    /// already keeps one under `label` beside that store.
    pub fn keep(self, label: [u8; 16]) -> Result<()> {
        let mut kept = KEPT
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let kept = kept.get_or_insert_with(HashMap::new);
        let key = (self.companion.clone(), label);
        if kept.contains_key(&key) {
            return Err(unavailable(
                "keeping background-work presence",
                &self.companion,
                "this process already keeps a presence under that label".to_string(),
            ));
        }
        kept.insert(key, self);
        Ok(())
    }
}

/// The token of the presence this process keeps under `label` beside `store_path`, if it keeps
/// one. Changes nothing.
#[cfg(target_os = "linux")]
pub fn kept_token(store_path: &Path, label: [u8; 16]) -> Option<[u8; 16]> {
    let kept = KEPT
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    kept.as_ref()?
        .get(&(store_holders_path(store_path), label))
        .map(|presence| presence.token)
}

/// Release the presence this process keeps under `label` beside `store_path`, returning its
/// token, or `None` when it keeps none there.
#[cfg(target_os = "linux")]
pub fn release_kept(store_path: &Path, label: [u8; 16]) -> Option<[u8; 16]> {
    let released = {
        let mut kept = KEPT
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        kept.as_mut()?
            .remove(&(store_holders_path(store_path), label))
    };
    released.map(|presence| presence.token)
}

/// Make this process lose every presence it holds beside `store_path`, kept ones included, as a
/// deleted companion or a lost network lock would, while the guards and kept entries stay where
/// they are. Returns how many it lost.
#[cfg(all(target_os = "linux", any(test, feature = "test-seams")))]
#[doc(hidden)]
pub fn lose_held_for_test(store_path: &Path) -> usize {
    let companion = store_holders_path(store_path);
    let offsets = {
        let mut losable = LOSABLE
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        losable
            .as_mut()
            .and_then(|losable| losable.remove(&companion))
            .unwrap_or_default()
    };
    let file = COMPANIONS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .as_ref()
        .and_then(|companions| companions.get(&companion))
        .and_then(Weak::upgrade);
    let Some(file) = file else {
        return 0;
    };
    let mut lost = 0;
    for (offset, holders) in offsets {
        let _ = lock(
            &file,
            offset,
            libc::F_UNLCK as libc::c_short,
            libc::F_OFD_SETLK,
        );
        lost += holders;
    }
    lost
}

#[cfg(all(target_os = "linux", any(test, feature = "test-seams")))]
fn remember_losable(companion: &Path, offset: i64) {
    let mut losable = LOSABLE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    *losable
        .get_or_insert_with(HashMap::new)
        .entry(companion.to_path_buf())
        .or_default()
        .entry(offset)
        .or_default() += 1;
}

/// Whether the lock at `offset` was still this process's to release; forgets it either way.
#[cfg(all(target_os = "linux", any(test, feature = "test-seams")))]
fn forget_losable(companion: &Path, offset: i64) -> bool {
    let mut losable = LOSABLE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let Some(offsets) = losable
        .as_mut()
        .and_then(|losable| losable.get_mut(companion))
    else {
        return false;
    };
    match offsets.get_mut(&offset) {
        Some(holders) if *holders > 1 => {
            *holders -= 1;
            true
        }
        Some(_) => {
            offsets.remove(&offset);
            true
        }
        None => false,
    }
}

/// Take the exclusive presence of `token` beside `store_path`, creating the empty companion if it
/// does not exist yet. Refuses with [`Error::HolderPresenceUnavailable`] when the filesystem
/// cannot lock or another process already holds this token. A token is held once: tokens are
/// minted fresh for every unit of work and never reused.
#[cfg(target_os = "linux")]
pub fn hold(store_path: &Path, token: [u8; 16]) -> Result<HolderPresence> {
    const OPERATION: &str = "taking background-work presence";
    let path = store_holders_path(store_path);
    #[cfg(any(test, feature = "test-seams"))]
    if let Some(raw_os_error) = INJECTED_HOLD_FAILURE.with(std::cell::Cell::take) {
        return Err(unavailable(
            OPERATION,
            &path,
            io::Error::from_raw_os_error(raw_os_error).to_string(),
        ));
    }
    let file = shared_companion(&path)
        .map_err(|error| unavailable(OPERATION, &path, error.to_string()))?;
    let offset = offset(token);
    match lock(
        &file,
        offset,
        libc::F_WRLCK as libc::c_short,
        libc::F_OFD_SETLK,
    ) {
        Ok(()) => {
            #[cfg(any(test, feature = "test-seams"))]
            remember_losable(&path, offset);
            Ok(HolderPresence {
                file,
                companion: path,
                token,
                offset,
            })
        }
        Err(error) if is_conflict(&error) => Err(unavailable(
            OPERATION,
            &path,
            "another process already holds this token's presence".to_string(),
        )),
        Err(error) => Err(unavailable(OPERATION, &path, error.to_string())),
    }
}

/// Whether a live process holds `token`'s presence. Changes no byte and never creates the
/// companion: a missing companion means there is no holder.
#[cfg(target_os = "linux")]
pub fn probe(store_path: &Path, token: [u8; 16]) -> Result<Holder> {
    const OPERATION: &str = "asking for background-work presence";
    let path = store_holders_path(store_path);
    let Some(file) = open_existing(&path, OPERATION)? else {
        return Ok(Holder::Absent);
    };
    let mut query = range(offset(token), libc::F_WRLCK as libc::c_short);
    // SAFETY: `query` is initialized and the descriptor stays owned by `file`.
    if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_OFD_GETLK, &mut query) } == -1 {
        return Err(unavailable(
            OPERATION,
            &path,
            io::Error::last_os_error().to_string(),
        ));
    }
    Ok(if query.l_type == libc::F_UNLCK as libc::c_short {
        Holder::Absent
    } else {
        Holder::Present
    })
}

/// Block in the kernel until `token`'s holder releases its presence or exits, and return at once
/// when no process holds it. Never polls and never gives up: a stopped holder keeps the caller
/// waiting until it resumes and releases.
#[cfg(target_os = "linux")]
pub fn wait_released(store_path: &Path, token: [u8; 16]) -> Result<()> {
    const OPERATION: &str = "waiting for background-work presence";
    let path = store_holders_path(store_path);
    let Some(file) = open_existing(&path, OPERATION)? else {
        return Ok(());
    };
    let offset = offset(token);
    loop {
        match lock(
            &file,
            offset,
            libc::F_RDLCK as libc::c_short,
            libc::F_OFD_SETLKW,
        ) {
            Ok(()) => {
                let _ = lock(
                    &file,
                    offset,
                    libc::F_UNLCK as libc::c_short,
                    libc::F_OFD_SETLK,
                );
                return Ok(());
            }
            Err(error) if error.raw_os_error() == Some(libc::EINTR) => continue,
            Err(error) => return Err(unavailable(OPERATION, &path, error.to_string())),
        }
    }
}

/// Make the next [`hold`] on this thread fail with `raw_os_error`, the way a filesystem that
/// cannot lock refuses it.
#[cfg(any(test, feature = "test-seams"))]
#[doc(hidden)]
pub fn fail_next_hold_for_test(raw_os_error: i32) {
    INJECTED_HOLD_FAILURE.with(|slot| slot.set(Some(raw_os_error)));
}

#[cfg(any(test, feature = "test-seams"))]
thread_local! {
    static INJECTED_HOLD_FAILURE: std::cell::Cell<Option<i32>> = const { std::cell::Cell::new(None) };
}

#[cfg(target_os = "linux")]
fn offset(token: [u8; 16]) -> i64 {
    let digest = blake3::hash(&token);
    let mut first = [0_u8; 8];
    first.copy_from_slice(&digest.as_bytes()[..8]);
    i64::try_from(u64::from_le_bytes(first) % RANGE).expect("offsets stay below 2^62")
}

#[cfg(target_os = "linux")]
fn range(offset: i64, kind: libc::c_short) -> libc::flock {
    libc::flock {
        l_type: kind,
        l_whence: libc::SEEK_SET as libc::c_short,
        l_start: offset,
        l_len: 1,
        l_pid: 0,
    }
}

#[cfg(target_os = "linux")]
fn lock(file: &File, offset: i64, kind: libc::c_short, command: libc::c_int) -> io::Result<()> {
    let mut request = range(offset, kind);
    // SAFETY: `request` is initialized and the descriptor stays owned by `file`.
    if unsafe { libc::fcntl(file.as_raw_fd(), command, &mut request) } == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(target_os = "linux")]
fn open_existing(path: &Path, operation: &str) -> Result<Option<File>> {
    match OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
        .open(path)
    {
        Ok(file) => Ok(Some(file)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(unavailable(operation, path, error.to_string())),
    }
}

#[cfg(target_os = "linux")]
fn is_conflict(error: &io::Error) -> bool {
    matches!(error.raw_os_error(), Some(libc::EAGAIN | libc::EACCES))
}

/// Take the exclusive presence of `token` beside `store_path`. This platform has no
/// open-file-description locks, so the request is refused and nothing is written.
#[cfg(not(target_os = "linux"))]
pub fn hold(store_path: &Path, _token: [u8; 16]) -> Result<HolderPresence> {
    Err(unsupported("taking background-work presence", store_path))
}

/// Whether a live process holds `token`'s presence. Refused on this platform.
#[cfg(not(target_os = "linux"))]
pub fn probe(store_path: &Path, _token: [u8; 16]) -> Result<Holder> {
    Err(unsupported(
        "asking for background-work presence",
        store_path,
    ))
}

/// Wait for `token`'s holder to release its presence. Refused on this platform.
#[cfg(not(target_os = "linux"))]
pub fn wait_released(store_path: &Path, _token: [u8; 16]) -> Result<()> {
    Err(unsupported(
        "waiting for background-work presence",
        store_path,
    ))
}

/// Keep a presence under a label. No presence exists on this platform to keep.
#[cfg(not(target_os = "linux"))]
impl HolderPresence {
    pub fn keep(self, _label: [u8; 16]) -> Result<()> {
        Err(unsupported(
            "keeping background-work presence",
            Path::new(""),
        ))
    }
}

/// The token kept under `label`. Nothing is kept on this platform.
#[cfg(not(target_os = "linux"))]
pub fn kept_token(_store_path: &Path, _label: [u8; 16]) -> Option<[u8; 16]> {
    None
}

/// Release the presence kept under `label`. Nothing is kept on this platform.
#[cfg(not(target_os = "linux"))]
pub fn release_kept(_store_path: &Path, _label: [u8; 16]) -> Option<[u8; 16]> {
    None
}

/// Lose every presence held beside a store. Nothing is held on this platform.
#[cfg(all(not(target_os = "linux"), any(test, feature = "test-seams")))]
#[doc(hidden)]
pub fn lose_held_for_test(_store_path: &Path) -> usize {
    0
}

#[cfg(not(target_os = "linux"))]
fn unsupported(operation: &str, store_path: &Path) -> Error {
    unavailable(
        operation,
        &store_holders_path(store_path),
        "holder presence needs Linux open-file-description locks".to_string(),
    )
}

fn unavailable(operation: &str, path: &Path, reason: String) -> Error {
    Error::HolderPresenceUnavailable {
        operation: operation.to_string(),
        path: path.display().to_string(),
        reason,
    }
}
