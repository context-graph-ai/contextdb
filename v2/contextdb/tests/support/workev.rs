//! The store the event, trigger and clock tests open: its data file is written as it is and never
//! forced to disk.
//!
//! These tests close every store before they open it again, so none of their checks rests on a
//! sync, while a sync is most of a debug commit's time and they commit once for each event,
//! firing or expiry they settle. The crash and durability tests keep the real file.

use contextdb::{Database, DatabaseOpenOptions, StorageBackend};
use std::{fs::File, io, os::unix::fs::FileExt, path::Path, sync::Arc};

/// Opens a store for writing as `support::open_with` does, through the unsynced file unless the
/// options name a storage door of their own.
pub fn open_with(folder: &Path, mut options: DatabaseOpenOptions) -> contextdb::Result<Database> {
    if options.storage.is_none() {
        options.storage = Some(Arc::new(|file| {
            Ok(Box::new(Unsynced(file)) as Box<dyn StorageBackend>)
        }));
    }
    crate::support::open_with(folder, options)
}

#[derive(Debug)]
struct Unsynced(File);

impl StorageBackend for Unsynced {
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
        Ok(())
    }

    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.0.write_all_at(data, offset)
    }

    fn close(&self) -> io::Result<()> {
        Ok(())
    }
}
