//! Fixed-window, non-durable backing for allocator bitmap words.
//!
//! The resident payload is `WINDOWS * WINDOW_BYTES` plus one fixed control
//! allocation; neither term depends on the file's region count. A window is
//! written to scratch before reuse. The caller supplies committed or virtual
//! base words when a scratch record is absent.

use crate::{ScratchBackend, StorageError};
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

pub(super) const WINDOW_BYTES: usize = 4096;
pub(super) const WINDOWS: usize = 32;
const RECORD_BYTES: u64 = (WINDOW_BYTES + 8) as u64;
const PAGES_PER_ARRAY: u64 = 32;
const HEIGHTS_PER_BITMAP: u64 = 4;
const ORDERS_PER_ENTITY: u64 = 21;

pub(super) fn bitmap_array_base(region: Option<u32>, order: u8) -> u64 {
    let entity = region.map_or(0, |region| u64::from(region) + 1);
    (entity * ORDERS_PER_ENTITY + u64::from(order)) * HEIGHTS_PER_BITMAP
}

type BaseLoader = dyn Fn(u64, u32, &mut [u8; WINDOW_BYTES]) -> io::Result<bool> + Send + Sync;

struct Window {
    identity: u64,
    bytes: [u8; WINDOW_BYTES],
    dirty: bool,
}

impl Window {
    fn empty() -> Self {
        Self {
            identity: 0,
            bytes: [0; WINDOW_BYTES],
            dirty: false,
        }
    }
}

struct Inner {
    scratch: Box<dyn ScratchBackend>,
    windows: Box<[Window]>,
    next_victim: usize,
    source: Option<Box<BaseLoader>>,
}

/// A scratch I/O fault is sticky: callers must refuse publication and skip
/// clean-header writes after this bit becomes set.
pub(super) struct AllocatorPager {
    inner: Mutex<Inner>,
    poisoned: AtomicBool,
    first_failure: Mutex<Option<(io::ErrorKind, String)>>,
}

impl AllocatorPager {
    pub(super) fn flush_and_detach_source(
        &self,
        virtual_source: Box<BaseLoader>,
    ) -> io::Result<()> {
        let mut inner = self.inner.lock().unwrap();
        let Inner {
            scratch,
            windows,
            source,
            ..
        } = &mut *inner;
        for window in windows.iter_mut() {
            if window.dirty {
                let offset = (window.identity - 1)
                    .checked_mul(RECORD_BYTES)
                    .ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidInput, "scratch offset overflow")
                    })?;
                scratch
                    .write(offset, &window.identity.to_le_bytes())
                    .inspect_err(|error| self.poison(error))?;
                scratch
                    .write(offset + 8, &window.bytes)
                    .inspect_err(|error| self.poison(error))?;
                window.dirty = false;
            }
        }
        // The durable tree may be reclaimed after this point. Keep the virtual
        // initializer for regions created later, while retaining spill records.
        *source = Some(virtual_source);
        Ok(())
    }
    pub(super) fn new(scratch: Box<dyn ScratchBackend>) -> Arc<Self> {
        let mut windows = Vec::with_capacity(WINDOWS);
        windows.resize_with(WINDOWS, Window::empty);
        Arc::new(Self {
            inner: Mutex::new(Inner {
                scratch,
                windows: windows.into_boxed_slice(),
                next_victim: 0,
                source: None,
            }),
            poisoned: AtomicBool::new(false),
            first_failure: Mutex::new(None),
        })
    }

    pub(super) fn resident_bytes() -> usize {
        WINDOWS * std::mem::size_of::<Window>() + std::mem::size_of::<Self>()
    }

    pub(super) fn is_poisoned(&self) -> bool {
        self.poisoned.load(Ordering::Acquire)
    }

    pub(super) fn storage_failure(&self) -> Option<StorageError> {
        if !self.is_poisoned() {
            return None;
        }
        Some(
            self.io_failure()
                .map_or(StorageError::PreviousIo, StorageError::Io),
        )
    }

    pub(super) fn io_failure(&self) -> Option<io::Error> {
        self.first_failure
            .lock()
            .unwrap()
            .as_ref()
            .map(|(kind, message)| io::Error::new(*kind, message.clone()))
    }

    pub(super) fn set_source(&self, source: Option<Box<BaseLoader>>) -> io::Result<()> {
        let mut inner = self.inner.lock().unwrap();
        inner
            .scratch
            .set_len(0)
            .inspect_err(|error| self.poison(error))?;
        inner.windows.iter_mut().for_each(|window| {
            window.identity = 0;
            window.dirty = false;
        });
        inner.source = source;
        Ok(())
    }

    pub(super) fn truncate_regions(&self, retained: u32) -> io::Result<()> {
        let mut inner = self.inner.lock().unwrap();
        let first_removed_array =
            (u64::from(retained) + 1) * ORDERS_PER_ENTITY * HEIGHTS_PER_BITMAP;
        let first_removed_key = first_removed_array * PAGES_PER_ARRAY + 1;
        for window in &mut inner.windows {
            if window.identity >= first_removed_key {
                window.identity = 0;
                window.dirty = false;
            }
        }
        let limit = (first_removed_key - 1) * RECORD_BYTES;
        if inner
            .scratch
            .len()
            .inspect_err(|error| self.poison(error))?
            > limit
        {
            inner
                .scratch
                .set_len(limit)
                .inspect_err(|error| self.poison(error))?;
        }
        Ok(())
    }

    pub(super) fn read_word(&self, array: u64, word: usize, len: u32, optimistic: bool) -> u64 {
        self.try_read_word(array, word, len, optimistic)
            .unwrap_or_else(|error| {
                self.poison(&error);
                u64::MAX
            })
    }

    pub(super) fn write_word(
        &self,
        array: u64,
        word: usize,
        len: u32,
        optimistic: bool,
        value: u64,
    ) {
        if let Err(error) = self.try_write_word(array, word, len, optimistic, value) {
            self.poison(&error);
        }
    }

    fn try_read_word(
        &self,
        array: u64,
        word: usize,
        len: u32,
        optimistic: bool,
    ) -> io::Result<u64> {
        let mut inner = self.inner.lock().unwrap();
        let slot = Self::find_window(&mut inner, array, word, len, optimistic)?;
        let start = word % (WINDOW_BYTES / 8) * 8;
        Ok(u64::from_le_bytes(
            inner.windows[slot].bytes[start..start + 8]
                .try_into()
                .unwrap(),
        ))
    }

    fn try_write_word(
        &self,
        array: u64,
        word: usize,
        len: u32,
        optimistic: bool,
        value: u64,
    ) -> io::Result<()> {
        let mut inner = self.inner.lock().unwrap();
        let slot = Self::find_window(&mut inner, array, word, len, optimistic)?;
        let start = word % (WINDOW_BYTES / 8) * 8;
        inner.windows[slot].bytes[start..start + 8].copy_from_slice(&value.to_le_bytes());
        inner.windows[slot].dirty = true;
        Ok(())
    }

    fn find_window(
        inner: &mut Inner,
        array: u64,
        word: usize,
        len: u32,
        optimistic: bool,
    ) -> io::Result<usize> {
        let page: u32 = (word / (WINDOW_BYTES / 8)).try_into().map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "allocator word index overflow")
        })?;
        if u64::from(page) >= PAGES_PER_ARRAY {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "allocator bitmap exceeds fixed scratch stride",
            ));
        }
        let identity = array
            .checked_mul(PAGES_PER_ARRAY)
            .and_then(|base| base.checked_add(u64::from(page)))
            .and_then(|key| key.checked_add(1))
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "allocator key overflow"))?;
        if let Some(index) = inner
            .windows
            .iter()
            .position(|window| window.identity == identity)
        {
            return Ok(index);
        }

        // Source-only words can be reloaded without spilling. Keep dirty
        // words in the same fixed owner while any clean window can be reused.
        let victim = (0..WINDOWS)
            .map(|offset| (inner.next_victim + offset) % WINDOWS)
            .find(|index| !inner.windows[*index].dirty)
            .unwrap_or(inner.next_victim);
        inner.next_victim = (victim + 1) % WINDOWS;
        let prior = &mut inner.windows[victim];
        if prior.dirty {
            let offset = (prior.identity - 1)
                .checked_mul(RECORD_BYTES)
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "scratch offset overflow")
                })?;
            inner.scratch.write(offset, &prior.identity.to_le_bytes())?;
            inner.scratch.write(offset + 8, &prior.bytes)?;
            prior.dirty = false;
        }

        let offset = (identity - 1).checked_mul(RECORD_BYTES).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "scratch offset overflow")
        })?;
        let stored = if inner.scratch.len()? >= offset + RECORD_BYTES {
            let mut saved_identity = [0u8; 8];
            inner.scratch.read(offset, &mut saved_identity)?;
            u64::from_le_bytes(saved_identity) == identity
        } else {
            false
        };
        if stored {
            inner.scratch.read(offset + 8, &mut prior.bytes)?;
        } else {
            let loaded = if let Some(source) = &inner.source {
                source(array, page, &mut prior.bytes)?
            } else {
                false
            };
            if !loaded {
                prior.bytes.fill(0xFF);
                if optimistic {
                    for (index, chunk) in prior.bytes.chunks_exact_mut(8).enumerate() {
                        let first =
                            (usize::try_from(page).unwrap() * WINDOW_BYTES / 8 + index) * 64;
                        let free_bits = (len as usize).saturating_sub(first).min(64);
                        let word = if free_bits == 64 {
                            0
                        } else {
                            u64::MAX << free_bits
                        };
                        chunk.copy_from_slice(&word.to_le_bytes());
                    }
                }
            }
        }
        prior.identity = identity;
        Ok(victim)
    }

    fn poison(&self, error: &io::Error) {
        let mut first = self.first_failure.lock().unwrap();
        if first.is_none() {
            *first = Some((error.kind(), error.to_string()));
        }
        self.poisoned.store(true, Ordering::Release);
    }
}
