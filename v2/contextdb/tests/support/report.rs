use super::{StorageBackend, WrapStorage};
use std::{fmt, fs::File, io, os::unix::fs::FileExt, sync::Arc};

/// A store-wide maintenance count must be present for an admin handle.
pub fn count(value: Option<u64>) -> u64 {
    value.expect("an admin maintenance count")
}

// ---- the storage door ----

/// One call the engine makes on its data file, as a test's storage door sees it.
pub enum Call<'a> {
    /// These bytes were just read at this offset. Changing them changes what the engine is
    /// given, not the file.
    Read(u64, &'a mut [u8]),
    /// These bytes are about to be written at this offset.
    Write(u64, &'a [u8]),
    /// The file is about to be forced to disk.
    Sync,
    /// The file is about to change length.
    Resize(u64),
    /// The file is about to be closed, after the store's last call on it.
    Close,
}

/// A storage door that keeps the data file as it is and shows `watch` every read, write and
/// sync. A test counts the calls, stops at one, or changes what a read returned.
pub fn storage(watch: impl Fn(Call<'_>) + Send + Sync + 'static) -> WrapStorage {
    failing_storage(move |call| {
        watch(call);
        Ok(())
    })
}

/// A storage door like [`storage`] whose `watch` may also make a call fail. The error it
/// returns is what the engine gets from that read, write or sync; a write or a sync it fails
/// never reaches the file.
pub fn failing_storage(
    watch: impl Fn(Call<'_>) -> io::Result<()> + Send + Sync + 'static,
) -> WrapStorage {
    let watch: Arc<dyn Fn(Call<'_>) -> io::Result<()> + Send + Sync> = Arc::new(watch);
    Arc::new(move |file| {
        Ok(Box::new(Watched {
            file,
            watch: watch.clone(),
        }) as Box<dyn StorageBackend>)
    })
}

struct Watched {
    file: File,
    watch: Arc<dyn Fn(Call<'_>) -> io::Result<()> + Send + Sync>,
}

impl fmt::Debug for Watched {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "a watched {:?}", self.file)
    }
}

impl StorageBackend for Watched {
    fn len(&self) -> io::Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        self.file.read_exact_at(out, offset)?;
        (self.watch)(Call::Read(offset, out))
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        (self.watch)(Call::Resize(len))?;
        self.file.set_len(len)
    }

    fn sync_data(&self) -> io::Result<()> {
        (self.watch)(Call::Sync)?;
        self.file.sync_data()
    }

    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        (self.watch)(Call::Write(offset, data))?;
        self.file.write_all_at(data, offset)
    }

    fn close(&self) -> io::Result<()> {
        (self.watch)(Call::Close)
    }
}

/// A cold read identified by both file offset and payload, replayed as one sentinel refusal.
pub struct Fault {
    reads: std::sync::Mutex<Vec<(u64, Vec<u8>)>>,
    target: std::sync::Mutex<Option<(u64, Vec<u8>)>>,
    armed: std::sync::atomic::AtomicBool,
    pub hits: std::sync::atomic::AtomicU64,
    sentinel: &'static str,
}
impl Fault {
    pub fn new(sentinel: &'static str) -> Arc<Self> {
        Arc::new(Self {
            reads: Default::default(),
            target: Default::default(),
            armed: Default::default(),
            hits: Default::default(),
            sentinel,
        })
    }
    pub fn storage(self: &Arc<Self>) -> WrapStorage {
        let this = self.clone();
        let control = self.target.lock().unwrap().is_none();
        let watch = move |call: Call<'_>| {
            match call {
                Call::Read(offset, payload) => {
                    if this.armed.load(std::sync::atomic::Ordering::SeqCst) {
                        let target = this.target.lock().unwrap();
                        let (wanted, bytes) = target
                            .as_ref()
                            .expect("identify the cold read before arming");
                        if offset == *wanted {
                            assert_eq!(payload, bytes);
                            this.hits.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            return Err(io::Error::other(this.sentinel));
                        }
                    } else {
                        this.reads.lock().unwrap().push((offset, payload.to_vec()));
                    }
                }
                Call::Write(at, bytes) => {
                    assert!(at < u64::MAX && !bytes.is_empty());
                }
                Call::Resize(len) => assert!(len.is_multiple_of(4096), "unaligned resize to {len}"),
                Call::Sync | Call::Close => (),
            }
            Ok(())
        };
        if control {
            storage(move |call| watch(call).unwrap())
        } else {
            failing_storage(watch)
        }
    }
    pub fn options(
        self: &Arc<Self>,
        mut options: super::DatabaseOpenOptions,
    ) -> super::DatabaseOpenOptions {
        options.maintenance = super::Maintenance::Caller;
        options.storage = Some(self.storage());
        options
    }
    pub fn refuse_cycle(
        &self,
        db: super::Database,
        reopen: impl FnOnce() -> super::Database,
    ) -> super::Database {
        let before = db.committed_watermark().unwrap();
        self.arm(true);
        let result = db.run_maintenance_cycle();
        self.arm(false);
        assert!(
            matches!(&result, Err(super::Error::StoreIo { reason, .. }) if reason.contains(self.sentinel)),
            "{result:?}"
        );
        assert_eq!(self.hits.load(std::sync::atomic::Ordering::SeqCst), 1);
        // A failed read leaves the store open: later calls answer, and nothing was committed.
        assert_eq!(
            db.committed_watermark().unwrap(),
            before,
            "the store stays open"
        );
        db.close().unwrap();
        drop(db);
        let db = reopen();
        assert_eq!(db.committed_watermark().unwrap(), before);
        db
    }
    pub fn clear(&self) {
        self.reads.lock().unwrap().clear();
    }
    pub fn identify(&self, needle: &[u8]) {
        let target = self
            .reads
            .lock()
            .unwrap()
            .iter()
            .find(|(_, b)| b.windows(needle.len()).any(|v| v == needle))
            .cloned()
            .expect("a physical cold read contains the expected payload identity");
        *self.target.lock().unwrap() = Some(target);
    }
    pub fn arm(&self, armed: bool) {
        self.armed.store(armed, std::sync::atomic::Ordering::SeqCst);
    }
}
