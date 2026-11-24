//! The library's file double, taken in by path from the tests that use it, so there is one copy.
use super::StorageBackend;
use std::collections::BTreeSet;
use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Barrier, Mutex};

/// The data file the way contextdb's store hands it over: the plain file. The store keeps its
/// own lock on its folder, so this library's lock on the file is not what holds it for a writer.
///
/// `damaged` lists file offsets whose byte arrives with one bit flipped on every read. While
/// `header` holds bytes, they are what a read of the start of the file is answered with. The
/// file itself is never changed by either.
#[derive(Debug)]
pub struct PlainFile {
    pub file: File,
    pub damaged: Vec<u64>,
    pub header: ServedHeader,
    pub sent: Arc<AtomicU64>,
    pub reads: Arc<AtomicU64>,
    pub read_pages: Arc<AtomicU64>,
    pub synced: Arc<AtomicU64>,
    pub refuse_growth: Arc<AtomicU64>,
    pub resized: Arc<AtomicU64>,
    pub watch: Arc<Watch>,
    pub refuse_sync: Arc<AtomicBool>,
    pub closes: Arc<AtomicU64>,
}

/// While `gate` holds a barrier, the next read past the header waits on it twice: once to say it
/// is there, once to be let go. While `read` or `written` holds a list, each page read or
/// written is added to it. While `cuts` holds the file's path, each sync first saves beside it
/// what a power cut there would leave: what the sync before made safe, with the header since.
#[derive(Debug, Default)]
pub struct Watch {
    pub gate: Mutex<Option<Arc<Barrier>>>,
    pub read: Mutex<Option<BTreeSet<u64>>>,
    pub written: Mutex<Option<BTreeSet<u64>>>,
    pub cuts: Mutex<Option<(PathBuf, Vec<PathBuf>)>>,
}

/// Waits twice on the barrier `gate` holds, if it holds one, and disarms it.
pub fn pass(gate: &Mutex<Option<Arc<Barrier>>>) {
    let barrier = gate.lock().unwrap().take();
    if let Some(barrier) = barrier {
        barrier.wait();
        barrier.wait();
    }
}

/// Adds the pages `len` bytes at `offset` cover to `list`, if it is kept.
pub fn note(list: &Mutex<Option<BTreeSet<u64>>>, offset: u64, len: usize) {
    if let Some(pages) = list.lock().unwrap().as_mut() {
        pages.extend(offset / 4096..(offset + len as u64).div_ceil(4096));
    }
}

/// The start of the file as a reader is to be served it, or `None` for what the file holds.
pub type ServedHeader = Arc<Mutex<Option<Vec<u8>>>>;

impl PlainFile {
    pub fn open(path: &Path, damaged: Vec<u64>) -> PlainFile {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .unwrap();
        PlainFile::new(file, damaged, ServedHeader::default())
    }

    pub fn new(file: File, damaged: Vec<u64>, header: ServedHeader) -> PlainFile {
        PlainFile {
            file,
            damaged,
            header,
            sent: Arc::default(),
            reads: Arc::default(),
            read_pages: Arc::default(),
            synced: Arc::default(),
            refuse_growth: Arc::default(),
            resized: Arc::default(),
            watch: Arc::default(),
            refuse_sync: Arc::default(),
            closes: Arc::default(),
        }
    }

    /// The file as a reader opens it: for reading only.
    pub fn read_only(path: &Path) -> PlainFile {
        PlainFile::serving(path, ServedHeader::default())
    }

    /// The file for reading only, its start answered from `header` whenever that holds bytes.
    pub fn serving(path: &Path, header: ServedHeader) -> PlainFile {
        PlainFile::new(File::open(path).unwrap(), Vec::new(), header)
    }
}

impl StorageBackend for PlainFile {
    fn len(&self) -> io::Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        self.reads.fetch_add(1, Relaxed);
        if offset >= 4096 {
            pass(&self.watch.gate);
        }
        note(&self.watch.read, offset, out.len());
        let pages = (offset % 4096 + out.len() as u64).div_ceil(4096);
        self.read_pages.fetch_add(pages, Relaxed);
        self.file.read_exact_at(out, offset)?;
        for at in &self.damaged {
            if (offset..offset + out.len() as u64).contains(at) {
                out[(at - offset) as usize] ^= 0x01;
            }
        }
        if let Some(header) = self.header.lock().unwrap().as_deref() {
            let served = header.iter().enumerate().skip(offset as usize);
            for (at, byte) in served.take(out.len()) {
                out[at - offset as usize] = *byte;
            }
        }
        Ok(())
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.resized.fetch_add(1, Relaxed);
        if self.refuse_growth.load(Relaxed) > 0 && self.refuse_growth.fetch_sub(1, Relaxed) == 1 {
            return Err(io::Error::new(
                io::ErrorKind::StorageFull,
                "growth refused inside commit",
            ));
        }
        self.file.set_len(len)
    }

    fn sync_data(&self) -> io::Result<()> {
        self.synced.fetch_add(1, Relaxed);
        if self.refuse_sync.load(Relaxed) {
            return Err(io::Error::other("sync refused"));
        }
        let mut cuts = self.watch.cuts.lock().unwrap();
        if let Some((path, images)) = cuts.as_mut() {
            let image = path.with_extension(format!("cut{}", images.len()));
            std::fs::copy(path.with_extension("safe"), &image)?;
            let mut header = [0; 4096];
            self.file.read_exact_at(&mut header, 0)?;
            OpenOptions::new()
                .write(true)
                .open(&image)?
                .write_all_at(&header, 0)?;
            images.push(image);
        }
        self.file.sync_data()?;
        if let Some((path, _)) = cuts.as_ref() {
            std::fs::copy(path, path.with_extension("safe"))?;
        }
        Ok(())
    }

    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.sent.fetch_add(data.len() as u64, Relaxed);
        note(&self.watch.written, offset, data.len());
        self.file.write_all_at(data, offset)
    }

    fn close(&self) -> io::Result<()> {
        self.closes.fetch_add(1, Relaxed);
        Ok(())
    }
}
