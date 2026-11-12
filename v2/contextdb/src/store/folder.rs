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
use std::io::{self, Read as _, Write as _};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use redb::StorageBackend;

use super::WrapStorage;

/// The data file as the storage library reads and writes it: through the host's wrapper when
/// it gave one, plainly otherwise.
#[derive(Debug)]
pub(super) struct DataFile(Box<dyn StorageBackend>);

impl DataFile {
    pub(super) fn over(file: File, storage: Option<&WrapStorage>) -> io::Result<DataFile> {
        // What a folder reads as differs between systems, so nothing is asked of one.
        if !file.metadata()?.is_file() {
            return Err(io::Error::other("the store's data file is not a file"));
        }
        Ok(DataFile(match storage {
            Some(wrap) => wrap(file)?,
            None => Box::new(Plain(file)),
        }))
    }
}

impl StorageBackend for DataFile {
    fn len(&self) -> io::Result<u64> {
        self.0.len()
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        self.0.read(offset, out)
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.0.set_len(len)
    }

    fn sync_data(&self) -> io::Result<()> {
        self.0.sync_data()
    }

    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.0.write(offset, data)
    }

    fn close(&self) -> io::Result<()> {
        self.0.close()
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
        self.0.set_len(len)
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

/// Takes the folder's `lock` for this process and writes the process id into it. When another
/// process holds it, answers at once with the id that process wrote.
pub(super) fn take_lock(folder: &Path) -> io::Result<Result<Locked, u32>> {
    let mut lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(folder.join("lock"))?;
    match lock.try_lock() {
        Ok(()) => {
            let mut lock = Locked(lock);
            lock.0.set_len(0)?;
            lock.0
                .write_all(std::process::id().to_string().as_bytes())?;
            Ok(Ok(lock))
        }
        Err(TryLockError::WouldBlock) => {
            let mut holder = String::new();
            lock.read_to_string(&mut holder)?;
            Ok(Err(holder.trim().parse().unwrap_or(0)))
        }
        Err(TryLockError::Error(error)) => Err(error),
    }
}

/// One reader's file in `readers/`. It names the commit the reader reads, the lock held on it
/// says the reader is alive, and it is removed when the reader lets the commit go.
pub(super) struct Hold {
    pub(super) commit: u64,
    path: PathBuf,
    _locked: Locked,
}

impl Hold {
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
        let file = File::create(&path)?;
        file.lock()?;
        let mut file = Locked(file);
        file.0.write_all(commit.to_string().as_bytes())?;
        Ok(Hold {
            commit,
            path,
            _locked: file,
        })
    }
}

impl Drop for Hold {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

/// The oldest commit that a living reader's file names, found under the short lock. A file
/// whose reader is gone is removed.
pub(super) fn oldest_held(readers: &Path, gate: &File) -> io::Result<Option<u64>> {
    gate.lock()?;
    let oldest = look_through(readers);
    gate.unlock()?;
    oldest
}

fn look_through(readers: &Path) -> io::Result<Option<u64>> {
    let mut oldest: Option<u64> = None;
    for entry in fs::read_dir(readers)? {
        let path = entry?.path();
        let mut file = match File::open(&path) {
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
                // A name that cannot be read holds every commit.
                let commit = named.trim().parse().unwrap_or(0);
                oldest = Some(oldest.map_or(commit, |oldest| oldest.min(commit)));
            }
            Err(TryLockError::Error(error)) => return Err(error),
        }
    }
    Ok(oldest)
}
