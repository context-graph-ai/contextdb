//! Durable scratch storage on disk.
//!
//! contextdb is a durable database and has no in-memory mode. Anything the
//! engine builds only to throw away -- the `:memory:` scratch database and the
//! private working images that schema changes use -- is an ordinary file-backed
//! store inside a scratch directory.
//!
//! A scratch directory holds an exclusive lock file while its owner lives and
//! is removed when the owner drops it. A process that is killed leaves its
//! directory behind with the lock free, so the next creation under the same
//! root sweeps every directory whose lock it can take.
//!
//! Scratch storage is never placed in the system temp dir, `/tmp` or
//! `/dev/shm`: those are often RAM-backed, and RAM behind a "durable" file is
//! memory no accountant charges.

use std::collections::HashMap;
use std::ffi::OsString;
use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use fs2::FileExt;

const LOCK_FILE: &str = ".scratch.lock";
const CREATE_ATTEMPTS: usize = 16;

/// A directory that is removed when this value drops. While it lives, its lock
/// file is held exclusively so no sweep can take it.
pub(crate) struct ScratchDirectory {
    path: PathBuf,
    lock: Option<File>,
}

impl ScratchDirectory {
    /// Create a fresh, uniquely named directory under `root` and lock it.
    /// Directories under `root` that a killed process abandoned are removed
    /// first.
    pub(crate) fn create_in(root: &Path) -> io::Result<Self> {
        std::fs::create_dir_all(root)?;
        sweep_abandoned(root);
        let mut last = io::Error::other("no scratch directory was created");
        for _ in 0..CREATE_ATTEMPTS {
            let path = root.join(uuid::Uuid::new_v4().simple().to_string());
            match std::fs::create_dir(&path) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                    last = error;
                    continue;
                }
                Err(error) => return Err(error),
            }
            // A concurrent sweep may take a directory between its creation and
            // its lock. `open_and_lock` proves it locked the live directory's
            // lock file, so a directory a sweep removed makes it fail and this
            // loop start over with a new name.
            match open_and_lock(&path) {
                Ok(lock) => {
                    return Ok(Self {
                        path,
                        lock: Some(lock),
                    });
                }
                Err(error) => last = error,
            }
        }
        Err(last)
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for ScratchDirectory {
    fn drop(&mut self) {
        // The lock stays held until the directory is gone, so a sweep cannot
        // race this removal.
        let _ = std::fs::remove_dir_all(&self.path);
        drop(self.lock.take());
    }
}

/// Open the lock file of `directory` and take its exclusive lock.
///
/// Opening and locking are two steps. Another process (or thread) can lock the
/// same file in between, remove the directory while it holds that lock, and
/// close it; the lock taken afterwards then succeeds on a file nobody can reach
/// any more. So after the lock is held, the locked descriptor must still be the
/// file the directory's lock path names. If it is not, the lock is dropped and
/// the caller starts over.
fn open_and_lock(directory: &Path) -> io::Result<File> {
    let lock_path = directory.join(LOCK_FILE);
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&lock_path)?;
    #[cfg(test)]
    test_support::run_between_open_and_lock_hook();
    file.try_lock_exclusive()?;
    if !locks_the_live_file(&file, &lock_path)? {
        // Dropping `file` releases the lock.
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            "the scratch directory was removed while its lock was taken",
        ));
    }
    Ok(file)
}

/// Whether `locked` is the file `path` names right now: the same device and
/// inode, so the directory still exists and holds this very lock file. A path
/// that names nothing is an error of its own kind (`NotFound`), which callers
/// treat the same way: the directory is gone.
#[cfg(unix)]
fn locks_the_live_file(locked: &File, path: &Path) -> io::Result<bool> {
    use std::os::unix::fs::MetadataExt;
    let held = locked.metadata()?;
    let named = std::fs::metadata(path)?;
    Ok(held.dev() == named.dev() && held.ino() == named.ino())
}

/// A file that is open cannot be unlinked on this platform, so a lock file
/// that still exists at its path is the file that was locked.
#[cfg(not(unix))]
fn locks_the_live_file(_locked: &File, path: &Path) -> io::Result<bool> {
    std::fs::metadata(path).map(|_| true)
}

/// Remove a scratch directory whose lock the caller holds. The lock file goes
/// last and the directory is removed without recursion: a creator that opens
/// the lock path after it is gone makes a new file there, which makes the
/// final removal fail and leaves that creator's directory alone.
fn remove_locked_directory(path: &Path) -> io::Result<()> {
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        if entry.file_name() == LOCK_FILE {
            continue;
        }
        let member = entry.path();
        if entry.file_type()?.is_dir() {
            std::fs::remove_dir_all(&member)?;
        } else {
            std::fs::remove_file(&member)?;
        }
    }
    match std::fs::remove_file(path.join(LOCK_FILE)) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    std::fs::remove_dir(path)
}
/// Remove every directory under `root` whose lock can be taken: its owner is
/// gone. Returns how many directories were removed.
pub(crate) fn sweep_abandoned(root: &Path) -> usize {
    let Ok(entries) = std::fs::read_dir(root) else {
        return 0;
    };
    let mut removed = 0;
    for entry in entries.flatten() {
        let path = entry.path();
        if !entry.file_type().is_ok_and(|kind| kind.is_dir()) {
            continue;
        }
        // Taking the lock proves no owner holds this directory. A creator that
        // has made the directory but not yet locked it loses the race cleanly:
        // either its own lock attempt fails, or it locks a file this sweep
        // removed and its live-file check fails. Both make it start over with
        // a new name.
        let Ok(lock) = open_and_lock(&path) else {
            continue;
        };
        if remove_locked_directory(&path).is_ok() {
            removed += 1;
        }
        drop(lock);
    }
    removed
}

/// The per-user application data directory of this platform, from the given
/// environment lookup. `None` when the platform names none.
pub(crate) fn user_data_dir_from(
    var: &dyn Fn(&str) -> Option<OsString>,
    home: Option<PathBuf>,
) -> Option<PathBuf> {
    let absolute = |name: &str| {
        var(name)
            .map(PathBuf::from)
            .filter(|path| path.is_absolute())
    };
    if cfg!(windows) {
        return absolute("LOCALAPPDATA").or_else(|| absolute("APPDATA"));
    }
    if cfg!(target_os = "macos") {
        return home.map(|home| home.join("Library").join("Application Support"));
    }
    absolute("XDG_DATA_HOME").or_else(|| home.map(|home| home.join(".local").join("share")))
}

fn home_dir() -> Option<PathBuf> {
    let from_env = std::env::var_os("HOME")
        .map(PathBuf::from)
        .filter(|path| path.is_absolute());
    #[cfg(unix)]
    {
        from_env.or_else(|| {
            nix::unistd::User::from_uid(nix::unistd::getuid())
                .ok()
                .flatten()
                .map(|user| user.dir)
                .filter(|path| path.is_absolute())
        })
    }
    #[cfg(not(unix))]
    {
        from_env
    }
}

/// `<user data dir>/contextdb/scratch`: where the `:memory:` scratch
/// databases live.
pub(crate) fn user_scratch_root() -> io::Result<PathBuf> {
    user_data_dir_from(&|name| std::env::var_os(name), home_dir())
        .map(|dir| dir.join("contextdb").join("scratch"))
        .ok_or_else(|| {
            io::Error::other(
                "cannot locate the user data directory for scratch storage \
                 (set XDG_DATA_HOME or HOME)",
            )
        })
}

/// Whether `path` lives on a RAM-backed filesystem. Scratch storage on such a
/// filesystem still works, but its bytes are memory no accountant charges.
pub(crate) fn is_ram_backed(path: &Path) -> bool {
    #[cfg(test)]
    if let Some(probe) = RAM_BACKED_PROBE.with(|slot| slot.get()) {
        return probe(path);
    }
    ram_backed_by_mount_table(path)
}

#[cfg(test)]
thread_local! {
    static RAM_BACKED_PROBE: std::cell::Cell<Option<fn(&Path) -> bool>> =
        const { std::cell::Cell::new(None) };
}

#[cfg(target_os = "linux")]
fn ram_backed_by_mount_table(path: &Path) -> bool {
    let Ok(canonical) = std::fs::canonicalize(path) else {
        return false;
    };
    let Ok(table) = std::fs::read_to_string("/proc/self/mountinfo") else {
        return false;
    };
    ram_backed_in_mount_table(&table, &canonical)
}

#[cfg(not(target_os = "linux"))]
fn ram_backed_by_mount_table(_path: &Path) -> bool {
    false
}

/// The filesystem type of the longest mount point that is a prefix of
/// `canonical`, read from `/proc/self/mountinfo` text.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn ram_backed_in_mount_table(table: &str, canonical: &Path) -> bool {
    let mut best: Option<(usize, bool)> = None;
    for line in table.lines() {
        let mut fields = line.split(' ');
        let Some(mount_point) = fields.nth(4) else {
            continue;
        };
        let mount_point = PathBuf::from(unescape_mount_field(mount_point));
        if !canonical.starts_with(&mount_point) {
            continue;
        }
        let depth = mount_point.components().count();
        let Some(fs_type) = fields.skip_while(|field| *field != "-").nth(1) else {
            continue;
        };
        let ram = matches!(fs_type, "tmpfs" | "ramfs");
        if best.is_none_or(|(best_depth, _)| depth >= best_depth) {
            best = Some((depth, ram));
        }
    }
    best.is_some_and(|(_, ram)| ram)
}

/// mountinfo writes a space, tab, newline or backslash in a path as a
/// three-digit octal escape.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn unescape_mount_field(field: &str) -> String {
    let bytes = field.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'\\'
            && index + 3 < bytes.len()
            && bytes[index + 1..index + 4]
                .iter()
                .all(|digit| (b'0'..=b'7').contains(digit))
        {
            let value = (bytes[index + 1] - b'0') * 64
                + (bytes[index + 2] - b'0') * 8
                + (bytes[index + 3] - b'0');
            out.push(value);
            index += 4;
        } else {
            out.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Log the single warning for a scratch directory on a RAM-backed filesystem.
/// The database still opens.
pub(crate) fn warn_if_ram_backed(directory: &Path) {
    if is_ram_backed(directory) {
        tracing::warn!(
            directory = %directory.display(),
            "contextdb scratch storage is on a RAM-backed filesystem; the bytes it holds are memory the memory limit does not charge"
        );
    }
}

fn owned_by_database() -> &'static Mutex<HashMap<PathBuf, ScratchDirectory>> {
    static OWNED: OnceLock<Mutex<HashMap<PathBuf, ScratchDirectory>>> = OnceLock::new();
    OWNED.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Hand a scratch directory to the database that lives in it, keyed by that
/// database's canonical file path. The directory is removed when the database
/// releases its file.
pub(crate) fn adopt(database_file: PathBuf, directory: ScratchDirectory) {
    owned_by_database()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .insert(database_file, directory);
}

/// The database at `database_file` has released its file: remove the scratch
/// directory it lived in, if it had one.
pub(crate) fn release(database_file: &Path) {
    let directory = owned_by_database()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .remove(database_file);
    drop(directory);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_dropped_scratch_directory_is_gone() {
        let root = tempfile::tempdir().unwrap();
        let directory = ScratchDirectory::create_in(root.path()).unwrap();
        let path = directory.path().to_path_buf();
        assert!(path.starts_with(root.path()));
        assert!(path.is_dir());
        std::fs::write(path.join("data"), b"x").unwrap();
        drop(directory);
        assert!(!path.exists());
    }

    #[test]
    fn a_sweep_removes_only_directories_whose_lock_is_free() {
        let root = tempfile::tempdir().unwrap();
        let live = ScratchDirectory::create_in(root.path()).unwrap();
        // A killed process leaves its directory and its lock file behind with
        // the lock released.
        let abandoned = root.path().join("abandoned");
        std::fs::create_dir(&abandoned).unwrap();
        std::fs::write(abandoned.join(LOCK_FILE), b"").unwrap();
        std::fs::write(abandoned.join("data"), b"x").unwrap();
        // A directory that never got its lock file is also abandoned.
        let unlocked = root.path().join("unlocked");
        std::fs::create_dir(&unlocked).unwrap();

        assert_eq!(sweep_abandoned(root.path()), 2);
        assert!(!abandoned.exists());
        assert!(!unlocked.exists());
        assert!(live.path().is_dir(), "a live owner's directory survives");
    }

    #[test]
    fn a_sweep_between_the_open_and_the_lock_makes_the_creator_start_over() {
        let root = tempfile::tempdir().unwrap();
        let swept = std::rc::Rc::new(std::cell::Cell::new(0));
        let hook_root = root.path().to_path_buf();
        let hook_swept = std::rc::Rc::clone(&swept);
        // The creator has opened its lock file and not yet locked it: a sweep
        // locks the same file, unlinks the directory and closes. The creator's
        // lock then succeeds on a file nobody can reach.
        test_support::on_next_open_before_lock(Box::new(move || {
            hook_swept.set(sweep_abandoned(&hook_root));
        }));
        let directory = ScratchDirectory::create_in(root.path()).unwrap();
        assert_eq!(
            swept.get(),
            1,
            "the sweep removed the creator's first directory"
        );
        assert!(
            directory.path().is_dir(),
            "the creator returned a directory that no longer exists: {}",
            directory.path().display()
        );
        assert!(directory.path().join(LOCK_FILE).is_file());
        assert!(
            open_and_lock(directory.path()).is_err(),
            "the returned directory's lock is held by its creator"
        );
        let entries = std::fs::read_dir(root.path()).unwrap().count();
        assert_eq!(entries, 1, "only the creator's live directory remains");
    }

    #[test]
    fn creating_a_scratch_directory_sweeps_what_a_killed_process_left() {
        let root = tempfile::tempdir().unwrap();
        let abandoned = root.path().join("abandoned");
        std::fs::create_dir(&abandoned).unwrap();
        std::fs::write(abandoned.join(LOCK_FILE), b"").unwrap();
        let fresh = ScratchDirectory::create_in(root.path()).unwrap();
        assert!(!abandoned.exists());
        assert!(fresh.path().is_dir());
    }

    #[test]
    fn data_directory_follows_the_platform_convention() {
        let home = Some(PathBuf::from("/home/user"));
        if cfg!(all(unix, not(target_os = "macos"))) {
            let with_xdg = |name: &str| (name == "XDG_DATA_HOME").then(|| "/xdg/data".into());
            assert_eq!(
                user_data_dir_from(&with_xdg, home.clone()),
                Some(PathBuf::from("/xdg/data"))
            );
            let relative = |name: &str| (name == "XDG_DATA_HOME").then(|| "relative".into());
            assert_eq!(
                user_data_dir_from(&relative, home.clone()),
                Some(PathBuf::from("/home/user/.local/share"))
            );
            assert_eq!(
                user_data_dir_from(&|_| None, home),
                Some(PathBuf::from("/home/user/.local/share"))
            );
            assert_eq!(user_data_dir_from(&|_| None, None), None);
        }
    }

    #[test]
    fn the_mount_table_names_ram_backed_filesystems() {
        let table = "\
22 1 259:3 / / rw,relatime - ext4 /dev/nvme0n1p3 rw
30 22 0:25 / /dev/shm rw,nosuid - tmpfs tmpfs rw
31 22 0:26 / /run rw,nosuid - tmpfs tmpfs rw
40 22 259:4 / /home rw,relatime - ext4 /dev/nvme1n1p3 rw
41 40 0:30 / /home/odd\\040name rw - tmpfs tmpfs rw
";
        assert!(ram_backed_in_mount_table(table, Path::new("/dev/shm/x")));
        assert!(ram_backed_in_mount_table(table, Path::new("/run/user/1")));
        assert!(!ram_backed_in_mount_table(
            table,
            Path::new("/home/me/.local/share")
        ));
        assert!(!ram_backed_in_mount_table(table, Path::new("/var/tmp")));
        assert!(ram_backed_in_mount_table(
            table,
            Path::new("/home/odd name/scratch")
        ));
    }

    #[test]
    fn a_ram_backed_directory_warns_once_and_a_disk_one_does_not() {
        fn ram(_: &Path) -> bool {
            true
        }
        fn disk(_: &Path) -> bool {
            false
        }
        let directory = tempfile::tempdir().unwrap();
        let ram_lines = test_support::with_ram_backed_probe(ram, || {
            test_support::captured_warnings(|| warn_if_ram_backed(directory.path()))
        });
        let disk_lines = test_support::with_ram_backed_probe(disk, || {
            test_support::captured_warnings(|| warn_if_ram_backed(directory.path()))
        });
        assert_eq!(ram_lines.len(), 1, "{ram_lines:?}");
        assert!(ram_lines[0].contains("RAM-backed"), "{ram_lines:?}");
        assert!(disk_lines.is_empty(), "{disk_lines:?}");
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use super::*;
    use std::cell::RefCell;

    thread_local! {
        static BETWEEN_OPEN_AND_LOCK: RefCell<Option<Box<dyn FnOnce()>>> =
            const { RefCell::new(None) };
    }

    /// Run `hook` once, on this thread, at the next `open_and_lock` between its
    /// open and its lock. The hook is taken out before it runs, so locks it
    /// takes itself (a sweep) do not run it again.
    pub(crate) fn on_next_open_before_lock(hook: Box<dyn FnOnce()>) {
        BETWEEN_OPEN_AND_LOCK.with(|slot| *slot.borrow_mut() = Some(hook));
    }

    pub(super) fn run_between_open_and_lock_hook() {
        let hook = BETWEEN_OPEN_AND_LOCK.with(|slot| slot.borrow_mut().take());
        if let Some(hook) = hook {
            hook();
        }
    }

    /// Run `body` on this thread and return the lines it logged at warn level.
    pub(crate) fn captured_warnings(body: impl FnOnce()) -> Vec<String> {
        use std::sync::Arc;
        #[derive(Clone)]
        struct Sink(Arc<Mutex<Vec<u8>>>);
        impl io::Write for Sink {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let sink = Sink(Arc::new(Mutex::new(Vec::new())));
        let writer = sink.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::WARN)
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .finish();
        tracing::subscriber::with_default(subscriber, body);
        let text = String::from_utf8(sink.0.lock().unwrap().clone()).unwrap();
        text.lines().map(str::to_owned).collect()
    }
    /// Run `body` on this thread with the RAM-backed check answering `probe`.
    pub(crate) fn with_ram_backed_probe<R>(
        probe: fn(&Path) -> bool,
        body: impl FnOnce() -> R,
    ) -> R {
        struct Reset;
        impl Drop for Reset {
            fn drop(&mut self) {
                RAM_BACKED_PROBE.with(|slot| slot.set(None));
            }
        }
        RAM_BACKED_PROBE.with(|slot| slot.set(Some(probe)));
        let _reset = Reset;
        body()
    }
}
