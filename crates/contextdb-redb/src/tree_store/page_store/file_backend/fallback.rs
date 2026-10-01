use crate::{DatabaseError, Result, ScratchBackend, StorageBackend, scratch_backend_in_directory};
use std::fs::File;
use std::io;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// Stores a database as a file on-disk.
#[derive(Debug)]
pub struct FileBackend {
    file: Mutex<File>,
    scratch_directory: Option<PathBuf>,
}

impl FileBackend {
    /// Creates a new backend which stores data to the given file.
    pub fn new(file: File) -> Result<Self, DatabaseError> {
        Self::new_internal(file, false)
    }

    pub(crate) fn new_internal(file: File, _: bool) -> Result<Self, DatabaseError> {
        Ok(Self {
            file: Mutex::new(file),
            scratch_directory: None,
        })
    }

    pub(crate) fn new_with_path(file: File, path: &Path) -> Result<Self, DatabaseError> {
        let directory = path
            .canonicalize()?
            .parent()
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "database file has no directory",
                )
            })?
            .to_path_buf();
        let mut backend = Self::new(file)?;
        backend.scratch_directory = Some(directory);
        Ok(backend)
    }
}

impl StorageBackend for FileBackend {
    fn scratch_backend(&self) -> io::Result<Option<Box<dyn ScratchBackend>>> {
        let directory = self.scratch_directory.as_ref().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::Unsupported,
                "file-backed allocator scratch requires a path-based open on this platform",
            )
        })?;
        scratch_backend_in_directory(directory).map(Some)
    }

    fn len(&self) -> Result<u64, io::Error> {
        Ok(self.file.lock().unwrap().metadata()?.len())
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> Result<(), io::Error> {
        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(out)?;
        Ok(())
    }

    fn set_len(&self, len: u64) -> Result<(), io::Error> {
        self.file.lock().unwrap().set_len(len)
    }

    fn sync_data(&self) -> Result<(), io::Error> {
        self.file.lock().unwrap().sync_data()
    }

    fn write(&self, offset: u64, data: &[u8]) -> Result<(), io::Error> {
        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(data)
    }
}
