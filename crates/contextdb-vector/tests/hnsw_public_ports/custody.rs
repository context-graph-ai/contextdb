//! Physical allocation evidence for the native retained holder. Graph page
//! Arc/Weak-specific assertions have no native counterpart and remain listed.
use super::*;
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Mutex;
#[derive(Clone, Copy)]
struct Allocation {
    pointer: usize,
    size: usize,
    align: usize,
    freed: bool,
    held_before: usize,
    held_after: usize,
}
const EMPTY: Allocation = Allocation {
    pointer: 0,
    size: 0,
    align: 0,
    freed: false,
    held_before: 0,
    held_after: 0,
};
const CAPACITY: usize = 256;
static ALLOCATIONS: Mutex<[Allocation; CAPACITY]> = Mutex::new([EMPTY; CAPACITY]);
thread_local! {static RECORD:Cell<bool>=const{Cell::new(false)};static FUNDER:Cell<usize>=const{Cell::new(0)};}
struct Traced;
#[global_allocator]
static ALLOCATOR: Traced = Traced;
fn observe_alloc(ptr: *mut u8, layout: Layout) {
    if !ptr.is_null() && RECORD.try_with(Cell::get).unwrap_or(false) {
        let mut a = ALLOCATIONS.lock().unwrap();
        let row = a
            .iter_mut()
            .find(|a| a.pointer == 0)
            .expect("bounded independent allocation census");
        *row = Allocation {
            pointer: ptr as usize,
            size: layout.size(),
            align: layout.align(),
            ..EMPTY
        };
    }
}
fn held() -> usize {
    FUNDER
        .try_with(|f| {
            let ptr = f.get();
            if ptr == 0 {
                0
            } else {
                unsafe { (&*(ptr as *const AtomicUsize)).load(Ordering::SeqCst) }
            }
        })
        .unwrap_or(0)
}
unsafe impl GlobalAlloc for Traced {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(l) };
        observe_alloc(p, l);
        p
    }
    unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
        let p = unsafe { System.alloc_zeroed(l) };
        observe_alloc(p, l);
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        let before = held();
        unsafe { System.dealloc(p, l) };
        let after = held();
        let mut rows = ALLOCATIONS.lock().unwrap();
        for row in rows
            .iter_mut()
            .filter(|r| r.pointer == p as usize && !r.freed)
        {
            row.freed = true;
            row.held_before = before;
            row.held_after = after;
        }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        let result = unsafe { System.realloc(p, l, n) };
        if !result.is_null() {
            for row in ALLOCATIONS
                .lock()
                .unwrap()
                .iter_mut()
                .filter(|r| r.pointer == p as usize && !r.freed)
            {
                row.freed = true;
            }
            observe_alloc(result, Layout::from_size_align(n, l.align()).unwrap());
        }
        result
    }
}
#[test]
fn owned_page_rejection_and_shared_page_retirement_preserve_physical_custody() {
    let build = Payer::unlimited();
    let p = policy(128, 8);
    let index = built(&rows(8, 128), p, &build);
    let pages = index.into_storage();
    assert_eq!(build.used(), 0);
    let original = Payer::unlimited();
    let foreign = Payer::unlimited();
    let manifest = pages.manifest(0).unwrap().unwrap();
    let layouts =
        ivf::codebook_allocation_layout(manifest.dimensions, manifest.active_lists, false).unwrap();
    *ALLOCATIONS.lock().unwrap() = [EMPTY; CAPACITY];
    RECORD.with(|r| r.set(true));
    let book = ivf::RetainedCodebook::load(&original.clone().into(), &pages, manifest).unwrap();
    RECORD.with(|r| r.set(false));
    let allocations = *ALLOCATIONS.lock().unwrap();
    let mut witnessed = Vec::new();
    for layout in [layouts.control, layouts.values] {
        let allocation = allocations
            .iter()
            .find(|a| a.pointer != 0 && a.size == layout.size() && a.align == layout.align())
            .expect("real constructor request witnessed");
        assert!(!allocation.freed);
        witnessed.push(allocation.pointer);
    }
    assert_eq!(book.charged_bytes(), layouts.charged_bytes);
    assert_eq!(original.used(), layouts.charged_bytes);
    assert_eq!(foreign.used(), 0);
    FUNDER.with(|f| f.set(&original.live as *const AtomicUsize as usize));
    drop(book);
    FUNDER.with(|f| f.set(0));
    assert_eq!(original.used(), 0);
    assert_eq!(foreign.used(), 0);
    let allocations = *ALLOCATIONS.lock().unwrap();
    for ptr in witnessed {
        let row = allocations.iter().find(|a| a.pointer == ptr).unwrap();
        assert!(row.freed, "the native retained payload is physically freed");
        assert!(row.held_before >= layouts.charged_bytes);
        assert!(
            row.held_after >= layouts.charged_bytes,
            "physical free precedes the original funder's refund"
        );
    }
    let cap = layouts.charged_bytes - 1;
    original.limit.store(cap, Ordering::SeqCst);
    let before = original.used();
    assert!(ivf::RetainedCodebook::load(&original.clone().into(), &pages, manifest).is_err());
    assert_eq!(original.used(), before);
    assert_eq!(foreign.used(), 0);
}
