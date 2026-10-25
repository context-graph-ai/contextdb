//! Independent rows of the existing page-lifetime contract. These exercise the
//! real cache/write/window handles and the existing allocator observation seam.
use super::*;
use crate::StorageBackend;
use crate::backends::InMemoryBackend;
use crate::tree_store::page_store::lru_cache::arena_index_tests::{FreeWatch, Watch};
use std::io;
use std::sync::Arc;

fn owner(page: &PageMemory) -> &PageAllocation {
    match page {
        PageMemory::Cached(page) | PageMemory::Charged(page) | PageMemory::Plain(page) => page,
        #[cfg(feature = "contextdb-internal")]
        PageMemory::Native(_) => panic!("expected the actual cache allocation"),
    }
}
fn linked(file: &PagedCachedFile, offset: u64, identity: &PageAllocation) -> bool {
    let stripe = file.read_cache
        [usize::try_from(offset % PagedCachedFile::lock_stripes()).expect("stripe fits usize")]
    .read()
    .unwrap();
    stripe
        .iter()
        .any(|(key, page)| key == offset && PageAllocation::ptr_eq(&page, identity))
}
fn file(cap: usize) -> PagedCachedFile {
    let backend = InMemoryBackend::new();
    backend.set_len(4096).unwrap();
    PagedCachedFile::new(Box::new(backend), 128, cap).unwrap()
}
struct Charge(Arc<AtomicUsize>, usize);
impl Drop for Charge {
    fn drop(&mut self) {
        self.0.fetch_sub(self.1, Ordering::SeqCst);
    }
}
fn admission(
    counter: &Arc<AtomicUsize>,
) -> Arc<dyn Fn(usize) -> io::Result<crate::ReadPageCharge> + Send + Sync> {
    let counter = counter.clone();
    Arc::new(move |bytes| {
        counter.fetch_add(bytes, Ordering::SeqCst);
        Ok(Box::new(Charge(counter.clone(), bytes)))
    })
}

pub(super) fn run() {
    displaced_and_linked_rows();
    charged_and_unwind_rows();
    retained_dirty_and_cow_row();
    source_funded_dirty_row();
    refusal_and_backend_failure_rows();
}

// Omitted invalidation, unlink-time refund and suppressed final refund are
// distinct mutants: exact identity, held occupancy, and final free catch each.
fn displaced_and_linked_rows() {
    let price = PageAllocation::layout(128).unwrap().0.size();
    for detach in [false, true] {
        let file = file(price);
        let page = file.read(0, 128, PageHint::Clean).unwrap();
        assert!(matches!(page, PageMemory::Cached(_)));
        let clone = page.clone();
        assert!(PageAllocation::ptr_eq(owner(&page), owner(&clone)));
        assert!(linked(&file, 0, owner(&page)));
        assert_eq!(PageAllocation::strong_count(owner(&page)), 3);
        if detach {
            file.invalidate_cache(0, 128);
            assert!(!linked(&file, 0, owner(&page)));
        }
        assert_eq!(file.read_cache_bytes.load(Ordering::SeqCst), price);
        // Full pinned share; misses cannot evict this owner or grow occupancy.
        for offset in [128, 256, 384] {
            let miss = file.read(offset, 128, PageHint::Clean).unwrap();
            assert!(!matches!(miss, PageMemory::Cached(_)));
            assert_eq!(file.read_cache_bytes.load(Ordering::SeqCst), price);
        }
        assert_eq!(&page[..], &[0; 128]);
        assert_eq!(&clone[..], &[0; 128]);
        let pointer = PageAllocation::as_ptr(owner(&page));
        let watch = FreeWatch::start(pointer, &file.read_cache_bytes);
        drop(page);
        assert_eq!(watch.cut(), [0; 4]);
        assert_eq!(file.read_cache_bytes.load(Ordering::SeqCst), price);
        if !detach {
            assert!(linked(&file, 0, owner(&clone)));
        }
        drop(clone);
        if detach {
            assert_eq!(watch.cut(), [1, price, price, price]);
            assert_eq!(file.read_cache_bytes.load(Ordering::SeqCst), 0);
        } else {
            assert_eq!(watch.cut(), [0; 4]);
            assert_eq!(file.read_cache_bytes.load(Ordering::SeqCst), price);
            let stripe = file.read_cache[0].read().unwrap();
            assert!(
                stripe
                    .iter()
                    .any(|(offset, page)| offset == 0 && PageAllocation::as_ptr(&page) == pointer)
            );
            drop(stripe);
            file.invalidate_cache(0, 128);
            assert_eq!(watch.cut(), [1, price, price, price]);
            assert_eq!(file.read_cache_bytes.load(Ordering::SeqCst), 0);
        }
        drop(watch);
        let replacement = file.read(0, 128, PageHint::Clean).unwrap();
        assert!(matches!(replacement, PageMemory::Cached(_)));
        assert_eq!(file.read_cache_bytes.load(Ordering::SeqCst), price);
    }
}

fn charged_and_unwind_rows() {
    let file = file(0);
    let paid = Arc::new(AtomicUsize::new(0));
    let price = PageAllocation::layout(128).unwrap().0.size();
    let quote = admitted_page_bytes(128);
    for unwind in [false, true] {
        let page = crate::with_read_page_charge(admission(&paid), || {
            file.read(0, 128, PageHint::Clean).unwrap()
        });
        assert!(matches!(page, PageMemory::Charged(_)));
        let clone = page.clone();
        let watch = FreeWatch::start(PageAllocation::as_ptr(owner(&page)), &paid);
        drop(page);
        assert_eq!(watch.cut(), [0; 4]);
        assert_eq!(paid.load(Ordering::SeqCst), quote);
        assert_eq!(&clone[..], &[0; 128]);
        if unwind {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                let _last_owner = clone;
                panic!("release the actual last page owner during unwind");
            }));
            assert!(result.is_err());
        } else {
            drop(clone);
        }
        assert_eq!(watch.cut(), [1, price, quote, quote]);
        assert_eq!(paid.load(Ordering::SeqCst), 0);
        assert_eq!(file.read_cache_bytes.load(Ordering::SeqCst), 0);
    }
    // The reject-only window is another real Charged owner, never an escaped
    // byte Arc. Its smaller Layout and genuine source invoice both survive.
    let window = crate::with_read_page_charge(admission(&paid), || {
        file.read_window(0, 128, 8..24, PageHint::Clean).unwrap()
    });
    assert!(matches!(window, PageMemory::Charged(_)));
    let clone = window.clone();
    let price = PageAllocation::layout(16).unwrap().0.size();
    let quote = admitted_page_bytes(16);
    let watch = FreeWatch::start(PageAllocation::as_ptr(owner(&window)), &paid);
    drop(window);
    assert_eq!(paid.load(Ordering::SeqCst), quote);
    assert_eq!(&clone[..], &[0; 16]);
    drop(clone);
    assert_eq!(watch.cut(), [1, price, quote, quote]);
    assert_eq!(paid.load(Ordering::SeqCst), 0);
}

fn retained_dirty_and_cow_row() {
    let price = cached_page_bytes(128);
    let file = file(8 * price);
    let old = file.read(0, 128, PageHint::Clean).unwrap();
    let old_clone = old.clone();
    let mut write = file.write(0, 128, false).unwrap();
    write.mem_mut().fill(0x31);
    assert_eq!(&old[..], &[0; 128]);
    assert_eq!(file.read_cache_bytes.load(Ordering::SeqCst), price);
    assert_eq!(file.write_buffer_bytes.load(Ordering::SeqCst), price);
    assert!(!PageAllocation::ptr_eq(owner(&old), &write.data));
    drop(write);
    let watch = FreeWatch::start(PageAllocation::as_ptr(owner(&old)), &file.read_cache_bytes);
    drop(old);
    assert_eq!(watch.cut(), [0; 4]);
    assert_eq!(&old_clone[..], &[0; 128]);
    drop(old_clone);
    assert_eq!(watch.cut(), [1, price, price, price]);
    assert_eq!(file.read_cache_bytes.load(Ordering::SeqCst), 0);
    drop(watch);
    let dirty = file.read(0, 128, PageHint::None).unwrap();
    assert!(matches!(dirty, PageMemory::Plain(_)));
    let dirty_clone = dirty.clone();
    let mut replacement = file.write(0, 128, false).unwrap();
    assert!(!PageAllocation::ptr_eq(owner(&dirty), &replacement.data));
    assert_eq!(replacement.mem(), &[0x31; 128]);
    replacement.mem_mut().fill(0x52);
    assert_eq!(&dirty[..], &[0x31; 128]);
    assert_eq!(file.write_buffer_bytes.load(Ordering::SeqCst), 2 * price);
    drop(replacement);
    let watch = FreeWatch::start(
        PageAllocation::as_ptr(owner(&dirty)),
        &file.write_buffer_bytes,
    );
    drop(dirty);
    assert_eq!(watch.cut(), [0; 4]);
    assert_eq!(&dirty_clone[..], &[0x31; 128]);
    drop(dirty_clone);
    assert_eq!(watch.cut(), [1, price, 2 * price, 2 * price]);
    assert_eq!(file.write_buffer_bytes.load(Ordering::SeqCst), price);
    drop(watch);
    let retained = file.read(0, 128, PageHint::None).unwrap();
    let retained_clone = retained.clone();
    let pointer = PageAllocation::as_ptr(owner(&retained));
    let watch = FreeWatch::start(pointer, &file.read_cache_bytes);
    file.flush().unwrap();
    assert_eq!(file.write_buffer_bytes.load(Ordering::SeqCst), 0);
    assert_eq!(file.read_cache_bytes.load(Ordering::SeqCst), price);
    assert!(linked(&file, 0, owner(&retained)));
    let committed = file.read(0, 128, PageHint::Clean).unwrap();
    assert!(PageAllocation::ptr_eq(owner(&retained), owner(&committed)));
    assert_eq!(&committed[..], &[0x52; 128]);
    drop(committed);
    file.invalidate_cache(0, 128);
    drop(retained);
    assert_eq!(watch.cut(), [0; 4]);
    assert_eq!(&retained_clone[..], &[0x52; 128]);
    drop(retained_clone);
    assert_eq!(watch.cut(), [1, price, price, price]);
    assert_eq!(file.read_cache_bytes.load(Ordering::SeqCst), 0);
}

fn source_funded_dirty_row() {
    let file = file(0);
    let paid = Arc::new(AtomicUsize::new(0));
    let mut write =
        crate::with_read_page_charge(admission(&paid), || file.write(0, 128, true).unwrap());
    write.mem_mut().fill(0x6b);
    drop(write);
    let dirty = file.read(0, 128, PageHint::None).unwrap();
    let clone = dirty.clone();
    let quote = admitted_page_bytes(128);
    assert_eq!(paid.load(Ordering::SeqCst), quote);
    let watch = FreeWatch::start(PageAllocation::as_ptr(owner(&dirty)), &paid);
    file.cancel_pending_write(0, 128);
    assert_eq!(
        file.write_buffer_bytes.load(Ordering::SeqCst),
        cached_page_bytes(128)
    );
    drop(dirty);
    assert_eq!(watch.cut(), [0; 4]);
    assert_eq!(&clone[..], &[0x6b; 128]);
    drop(clone);
    assert_eq!(watch.cut(), [1, cached_page_bytes(128), quote, quote]);
    assert_eq!(paid.load(Ordering::SeqCst), 0);
    assert_eq!(file.write_buffer_bytes.load(Ordering::SeqCst), 0);
}

#[derive(Debug)]
struct FaultBackend {
    reads: Arc<AtomicUsize>,
    panic: bool,
}
impl StorageBackend for FaultBackend {
    fn len(&self) -> io::Result<u64> {
        Ok(4096)
    }
    fn set_len(&self, _: u64) -> io::Result<()> {
        Ok(())
    }
    fn read(&self, _: u64, _: &mut [u8]) -> io::Result<()> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        assert!(!self.panic, "backend read unwind");
        Err(io::Error::from_raw_os_error(5))
    }
    fn write(&self, _: u64, _: &[u8]) -> io::Result<()> {
        Ok(())
    }
    fn sync_data(&self) -> io::Result<()> {
        Ok(())
    }
}
fn refusal_and_backend_failure_rows() {
    for panic in [false, true] {
        let reads = Arc::new(AtomicUsize::new(0));
        let file = PagedCachedFile::new(
            Box::new(FaultBackend {
                reads: reads.clone(),
                panic,
            }),
            128,
            cached_page_bytes(128),
        )
        .unwrap();
        let paid = Arc::new(AtomicUsize::new(0));
        // A scalar refusal precedes allocation and backend access, preserving
        // the exact cause and leaving the real handle unpoisoned.
        crate::read_admission::with_read_page_charge_storage(
            Arc::new(|_| Err(StorageError::ValueTooLarge(731))),
            || {
                let watch = Watch::start();
                let result = file.read(0, 128, PageHint::Clean);
                assert!(matches!(result, Err(StorageError::ValueTooLarge(731))));
                assert_eq!(watch.cut(), [0; 5]);
            },
        );
        assert_eq!(reads.load(Ordering::SeqCst), 0);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            crate::with_read_page_charge(admission(&paid), || file.read(0, 128, PageHint::Clean))
        }));
        if panic {
            assert!(result.is_err());
        } else {
            let error = result.unwrap().unwrap_err();
            assert!(matches!(error, StorageError::Io(ref io) if io.raw_os_error() == Some(5)));
            assert!(matches!(
                file.read(0, 128, PageHint::Clean),
                Err(StorageError::PreviousIo)
            ));
        }
        assert_eq!(reads.load(Ordering::SeqCst), 1);
        assert_eq!(paid.load(Ordering::SeqCst), 0);
        assert_eq!(file.read_cache_bytes.load(Ordering::SeqCst), 0);
        assert_eq!(file.write_buffer_bytes.load(Ordering::SeqCst), 0);
    }
}
