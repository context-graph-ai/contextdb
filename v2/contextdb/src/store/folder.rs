//! The files of a store folder around the pages of `data`: how the data file is read and
//! written, the writer's `lock`, and the readers' files in `readers/`.
//!
//! Every lock here is an operating-system lock on an open file, so it goes when its process
//! does, however the process ends. The `readers/` folder itself is the short lock: a reader
//! holds it, shared, for the moment it takes to find the newest commit and leave a file naming
//! it; the writer holds it, alone, while it looks through those files. So the writer never
//! sees a reader between its choice of a commit and the file that says so.
//!
//! Such a lock belongs to the open file, and a child process has a copy of every file its
//! parent had open when it was started, until it replaces itself with its program. So every
//! lock is let go by unlocking the file before it is closed: a copy a child still has then
//! holds nothing.

use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::ErrorKind::{PermissionDenied, ReadOnlyFilesystem};
use std::io::{self, Read as _, Write as _};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::SystemTime;

use crate::system::{self, Clock};
use crate::types::Error;

use super::disk::Stored;
use super::{Fixing, HeldFor, MapCover, Pins, StorageBackend, WrapStorage, disk, locked};
use crate::memory::Reservation;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, Weak};

/// How a writable open treats its data file.
#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub enum Create {
    #[default]
    IfMissing,
    Never,
    Fresh,
}

/// What a writable open of a store folder reads of the database's open options.
#[derive(Clone, Copy, Default)]
pub(crate) struct WriteOpenOptions<'a> {
    pub(crate) create: Create,
    pub(crate) clock: Option<&'a Arc<dyn Clock>>,
    pub(crate) deadline: Option<SystemTime>,
    pub(crate) cancel: Option<&'a AtomicBool>,
    pub(crate) storage: Option<&'a WrapStorage>,
}

/// The data file as the storage library reads and writes it: through the host's wrapper when
/// it gave one, plainly otherwise.
#[derive(Clone)]
pub(super) struct DataFile(Arc<dyn StorageBackend>, super::MapCover);
impl std::fmt::Debug for DataFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl DataFile {
    pub(super) fn over(
        file: File,
        storage: Option<&WrapStorage>,
        maps: &MapCover,
    ) -> io::Result<DataFile> {
        // What a folder reads as differs between systems, so nothing is asked of one.
        if !file.metadata()?.is_file() {
            return Err(io::Error::other("the store's data file is not a file"));
        }
        let backend: Box<dyn StorageBackend> = match storage {
            Some(wrap) => wrap(file)?,
            None => Box::new(Plain(file)),
        };
        Ok(DataFile(Arc::from(backend), maps.clone()))
    }

    fn sound(&self) -> io::Result<&dyn StorageBackend> {
        sound(&self.1).map(|()| &*self.0)
    }
}

pub(super) fn sound(maps: &MapCover) -> io::Result<()> {
    match maps.1.load(Ordering::Acquire) {
        true => Err(io::Error::other("a failed growth could not be cut back")),
        false => Ok(()),
    }
}

impl redb::StorageBackend for DataFile {
    fn len(&self) -> io::Result<u64> {
        self.sound()?.len()
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        self.sound()?.read(offset, out)
    }

    fn admit_growth(&self, len: u64) -> io::Result<()> {
        self.1
            .0
            .lock()
            .unwrap()
            .cover(super::maps(Some(len)))
            .map_err(io::Error::other)?;
        sound(&self.1)
    }

    /// A lengthening that fails, part way through writing zeros or otherwise, is cut back to the
    /// old length before the refusal is returned. A refused growth still leaves the page maps'
    /// memory set aside for the larger file (at most 768 KiB, used by the next growth) and the
    /// page cache emptied.
    fn set_len(&self, len: u64) -> io::Result<()> {
        let file = self.sound()?;
        let old = file.len()?;
        file.set_len(len).inspect_err(|_| {
            let cut = old < len && file.set_len(old).is_err();
            self.1.1.fetch_or(cut, Ordering::Release);
        })
    }

    fn sync_data(&self) -> io::Result<()> {
        self.sound()?.sync_data()
    }

    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.sound()?.write(offset, data)
    }

    fn close(&self) -> io::Result<()> {
        if Arc::strong_count(&self.0) == 1 {
            self.0.close()
        } else {
            Ok(())
        }
    }
}

/// The file as it is.
#[derive(Debug)]
struct Plain(File);

impl StorageBackend for Plain {
    fn len(&self) -> io::Result<u64> {
        Ok(self.0.metadata()?.len())
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        self.0.read_exact_at(out, offset)
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        redb::backends::set_file_len(&self.0, len)
    }

    fn sync_data(&self) -> io::Result<()> {
        self.0.sync_data()
    }

    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.0.write_all_at(data, offset)
    }
}

/// A file this process holds locked. Dropping it unlocks the file and then closes it.
pub(super) struct Locked(File);

impl Drop for Locked {
    fn drop(&mut self) {
        let _ = self.0.unlock();
    }
}

/// Opens a store file, keeping its contents when it already exists.
pub(super) fn kept(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
}

/// Waits once for ownership, then marks the writer until it passes the readers gate.
pub(super) fn take_lock(
    folder: &Path,
    options: &WriteOpenOptions,
    clock: &dyn Clock,
) -> io::Result<Result<(Locked, Locked), Error>> {
    let lock = kept(&folder.join("lock"))?;
    let mut held = Vec::new();
    let mut waiting = None;
    let readers = folder.join("readers");
    fs::create_dir_all(&readers)?;
    for file in [lock, File::open(&readers)?] {
        let try_lock = |_: usize| match file.try_lock() {
            Ok(()) => Ok(1),
            Err(TryLockError::WouldBlock) => Err(io::ErrorKind::WouldBlock.into()),
            Err(TryLockError::Error(error)) => Err(error),
        };
        let wait = options.deadline.map(|at| (at, clock));
        let waited = match (wait, options.cancel) {
            (None, None) => file.lock().map(|()| Ok(())),
            (wait, cancel) => system::wait_for(1, wait, cancel, try_lock),
        };
        if let Err(cancelled) = waited? {
            let path = folder.to_path_buf();
            if file.try_lock_shared().is_err() {
                return Ok(Err(if cancelled {
                    Error::OpenCancelled { path }
                } else {
                    Error::DatabaseLocked { path }
                }));
            }
            if file.try_lock().is_err() {
                return Ok(Err(Error::HeldByReaders { path }));
            }
        }
        held.push(Locked(file));
        if waiting.is_none() {
            let lock = held.last_mut().expect("ownership");
            lock.0.set_len(0)?;
            lock.0
                .write_all(std::process::id().to_string().as_bytes())?;
            let mark = Locked(kept(&folder.join("writer"))?);
            mark.0.lock_shared()?;
            waiting = Some(mark);
        }
    }
    look_through(&readers, system::now_millis(clock))?;
    held.pop(); // Release the reader gate after cleanup.
    let lock = held.pop().expect("ownership");
    Ok(Ok((lock, waiting.expect("writer admission mark"))))
}

/// Admission at reader open waits only for an owning writer to pass the readers gate.
pub(super) fn admit(folder: &Path, mark: &File) -> io::Result<()> {
    if fs::read_to_string(folder.join("lock"))?.trim().parse() == Ok(std::process::id()) {
        return Ok(());
    }
    mark.lock()?;
    mark.unlock()
}

/// One reader's file in `readers/`. It names the commit the reader reads, the lock held on it
/// says the reader is alive, and it is removed when the reader lets the commit go.
pub(super) struct Hold {
    pub(super) commit: u64,
    path: PathBuf,
    _locked: Locked,
}

impl Hold {
    pub(super) fn valid(&self) -> io::Result<bool> {
        Ok(self._locked.0.metadata()?.len() > 0)
    }

    /// Leaves a file naming `commit`. The caller holds the short lock.
    pub(super) fn leave(readers: &Path, commit: u64) -> io::Result<Hold> {
        // Two stores read in one process leave two files.
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let name = format!(
            "{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        );
        let path = readers.join(name);
        let file = kept(&path)?;
        file.lock()?;
        let mut file = Locked(file);
        file.0.write_all(commit.to_string().as_bytes())?;
        Ok(Hold {
            commit,
            path,
            _locked: file,
        })
    }

    /// Reads the body under the readers gate by this file's own descriptor and, given `deadline`,
    /// rewrites it. False when the writer let it go; `AuthoritativeErasure` when a purge did.
    pub(super) fn stamp(&self, deadline: Option<Option<i64>>) -> Stored<bool> {
        let gate = Locked(File::open(self.path.parent().expect("the readers folder"))?);
        gate.0.lock_shared()?;
        let (file, mut body, commit) = (&self._locked.0, [0; 48], self.commit);
        let len = file.read_at(&mut body, 0)?;
        match (&body[..len], deadline) {
            ([], _) => Err(Error::AuthoritativeErasure.into()),
            (b"!", _) => Ok(false),
            (_, None) => Ok(true),
            (_, Some(at)) => {
                let new = at.map_or(commit.to_string(), |at| format!("{commit} {at}"));
                file.write_all_at(new.as_bytes(), 0)?;
                file.set_len(new.len() as u64)?;
                Ok(true)
            }
        }
    }
}

impl Drop for Hold {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

/// The oldest commit that a living reader's file names, found under the short lock. A file
/// whose reader is gone is removed.
pub(super) fn oldest_held(readers: &Path, gate: &File, now: i64) -> io::Result<Option<u64>> {
    match gate.try_lock() {
        Ok(()) => {}
        Err(TryLockError::WouldBlock) => return Ok(Some(0)),
        Err(TryLockError::Error(error)) => return Err(error),
    }
    let oldest = look_through(readers, now);
    gate.unlock()?;
    oldest
}

/// A reader file's commit, 0 when unreadable, and an idle cursor's deadline.
fn parsed(body: &str) -> (u64, Option<i64>) {
    let (commit, at) = body.trim().split_once(' ').unwrap_or((body.trim(), ""));
    (commit.parse().unwrap_or(0), at.parse().ok())
}

/// Also lets go of an idle cursor, on which no read is in flight, past its deadline at `now`.
fn look_through(readers: &Path, now: i64) -> io::Result<Option<u64>> {
    let mut oldest: Option<u64> = None;
    for entry in fs::read_dir(readers)? {
        let path = entry?.path();
        let mut file = match OpenOptions::new().read(true).write(true).open(&path) {
            Ok(file) => file,
            // Its reader let go and removed it since the folder was listed.
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };
        match file.try_lock() {
            // Nobody holds it: its reader is gone, and may have removed it already.
            Ok(()) => {
                let _ = fs::remove_file(&path);
                file.unlock()?;
            }
            Err(TryLockError::WouldBlock) => {
                let mut named = String::new();
                file.read_to_string(&mut named)?;
                let (commit, deadline) = parsed(&named);
                // A `!` left by a writer that stopped before removing it holds nothing.
                if named == "!" || deadline.is_some_and(|at| now > at) {
                    file.write_all_at(b"!", 0)?;
                    file.set_len(1)?;
                    let _ = fs::remove_file(&path);
                    continue;
                }
                oldest = Some(oldest.map_or(commit, |oldest| oldest.min(commit)));
            }
            Err(TryLockError::Error(error)) => return Err(error),
        }
    }
    Ok(oldest)
}

/// Revocation leaves the live hold and its page protection in place; an empty file is revoked.
pub(super) fn cut_off(readers: &Path, before: u64) -> io::Result<()> {
    for entry in fs::read_dir(readers)? {
        let path = entry?.path();
        let file = match OpenOptions::new().read(true).write(true).open(&path) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e),
        };
        let mut named = String::new();
        (&file).read_to_string(&mut named)?;
        let (commit, deadline) = parsed(&named);
        // A cursor the writer let go stays an expiry; an idle one needs no page kept.
        if named != "!" && commit < before {
            file.set_len(0)?;
            let _ = deadline.map(|_| fs::remove_file(&path));
        }
    }
    Ok(())
}

pub(super) fn prepare_open(folder: &Path, create: Create) -> io::Result<bool> {
    let data = folder.join("data");
    let created = match fs::metadata(&data) {
        Ok(m) => m.len() == 0,
        Err(e) if e.kind() == io::ErrorKind::NotFound => true,
        Err(e) => return Err(e),
    };
    if create == Create::Fresh {
        fs::remove_file(data)?;
    }
    Ok(created || create == Create::Fresh)
}
/// Asks for `data` shared without waiting: a writer holds it alone from owning `lock` to its
/// close. Writers never race for it, so the probe's instant never refuses one.
pub(super) fn writer_running(folder: &Path) -> io::Result<bool> {
    let file = match File::open(folder.join("data")) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(e) => return Err(e),
    };
    match file.try_lock_shared() {
        Ok(()) => {
            file.unlock()?;
            Ok(false)
        }
        Err(TryLockError::WouldBlock) => Ok(true),
        Err(TryLockError::Error(e)) => Err(e),
    }
}

pub(super) fn writing_with_cache(
    folder: &Path,
    budget: &crate::memory::Budget,
    options: &WriteOpenOptions,
    bytes: u64,
) -> crate::Result<super::Store> {
    let mut cache = budget.reserve(bytes, super::CACHE_NEEDED)?;
    let maps = Arc::new((Mutex::new(cache.passing()), Default::default()));
    let held = HeldFor::writing(folder, options, budget.limit(), bytes, &maps, &mut cache)
        .map_err(|e| super::disk::failed(folder, "open", e))??;
    Ok(super::Store {
        held,
        maps,
        folder: folder.into(),
        budget: budget.clone(),
        _cache: cache,
        marks: Default::default(),
        surplus: Default::default(),
        space: super::space::Space::opened(folder),
    })
}

impl HeldFor {
    /// Takes writer ownership within the caller's deadline and prepares the data file.
    /// The cache follows the explicit limit, or the stored limit unless copying with zero cache.
    pub(super) fn writing(
        folder: &Path,
        options: &WriteOpenOptions,
        limit: Option<u64>,
        mut cache: u64,
        maps: &MapCover,
        held: &mut Reservation,
    ) -> Stored<crate::types::Result<HeldFor>> {
        if options.create != Create::IfMissing && !folder.join("data").try_exists()? {
            return Ok(Err(Error::StoreMissing {
                path: folder.into(),
            }));
        }
        fs::create_dir_all(folder)?;
        let clock = options.clock.cloned().unwrap_or_else(system::machine_clock);
        let storage = options.storage;
        let (lock, waiting) = match super::folder::take_lock(folder, options, &*clock)? {
            Ok(held) => held,
            Err(holder) => return Ok(Err(holder)),
        };
        drop(waiting);
        let created = prepare_open(folder, options.create)?;
        let backend = DataFile::over(super::folder::kept(&folder.join("data"))?, storage, maps)?;
        // Only a writer holds the page maps of its file.
        locked(&maps.0).cover(super::maps(Some(redb::StorageBackend::len(&backend)?)))?;
        let running = Locked(File::open(folder.join("data"))?);
        running.0.lock()?; // Waits at most for a probe's instant.
        if limit.is_none() && cache != 0 {
            cache = Self::remembered_cache(backend.clone())?;
        }
        held.grow(cache.saturating_sub(held.bytes()))?;
        let pins = Arc::new(Mutex::new(BTreeMap::new()));
        let data = HeldFor::data(folder, backend, cache, pins.clone(), clock)?;
        disk::prepare(&data)?;
        Ok(Ok(HeldFor::Writing {
            data: Arc::new(data),
            pins,
            _locks: [running, lock],
            created,
        }))
    }

    /// Opens the data file of a folder held for writing, with a page cache of `cache` bytes.
    pub(super) fn data(
        folder: &Path,
        backend: DataFile,
        cache: u64,
        pins: Pins,
        clock: Arc<dyn Clock>,
    ) -> Stored<redb::Database> {
        let readers = folder.join("readers");
        let gate = File::open(&readers)?;
        let data = redb::Builder::new()
            .set_incremental_recovery(true)
            .set_cache_size(super::pages(cache))
            .set_manual_compaction(true)
            // When the files cannot be looked through, every commit counts as held.
            .set_held_commits_callback(move || {
                let mut pins = locked(&pins);
                pins.retain(|_, pin| pin.strong_count() != 0);
                let local = pins.first_key_value().map(|(commit, _)| *commit);
                let now = system::now_millis(&*clock);
                let external = super::folder::oldest_held(&readers, &gate, now).unwrap_or(Some(0));
                local.into_iter().chain(external).min()
            })
            .create_with_backend(backend)?;
        Ok(data)
    }

    /// Opens the folder for reading with a page cache of `cache` bytes, or answers `None` when
    /// it has no data file.
    pub(super) fn reading(
        folder: &Path,
        storage: Option<&WrapStorage>,
        cache: u64,
        maps: &MapCover,
    ) -> Stored<Option<HeldFor>> {
        let file = match File::open(folder.join("data")) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let readers = folder.join("readers");
        let marked = fs::read_dir(&readers).and(fs::metadata(&readers))?;
        let marked = marked.permissions().readonly();
        let shared = match (!marked).then(|| kept(&folder.join("writer"))) {
            Some(Ok(mark)) => admit(folder, &mark).map(|()| None)?,
            Some(Err(e)) if !matches!(e.kind(), PermissionDenied | ReadOnlyFilesystem) => {
                return Err(e.into());
            }
            _ => {
                let lock = Locked(File::open(folder.join("lock"))?);
                match lock.0.try_lock_shared() {
                    Err(TryLockError::WouldBlock) => {
                        let path = folder.to_path_buf();
                        return Err(Error::DatabaseLocked { path }.into());
                    }
                    locked => locked.map_err(io::Error::from)?,
                }
                Some(lock)
            }
        };
        let fixing = Fixing {
            gate: File::open(&readers)?,
            readers,
            hold: Weak::new(),
            shared,
        };
        // The open checks the newest commit, so it holds every commit while it does, as a view
        // does; each view then holds the commit it reads.
        let pinned = fixing.shared.is_none().then(|| fixing.pin()).transpose()?;
        let data = redb::Builder::new()
            .set_incremental_recovery(true)
            .set_cache_size(super::pages(cache))
            .open_read_only_with_backend(DataFile::over(file, storage, maps)?)?;
        drop(pinned);
        Ok(Some(HeldFor::Reading {
            data: Arc::new(data),
            fixing: Mutex::new(fixing),
        }))
    }
}
