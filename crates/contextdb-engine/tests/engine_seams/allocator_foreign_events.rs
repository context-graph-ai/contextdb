//! A joined real global-allocator event must keep cross-thread qualification strict.
use contextdb_engine::read_work_probe::allocator_events::{FOREIGN_THREAD, Window, first_failure};
use std::sync::{Arc, Barrier};
#[test]
fn a_foreign_global_allocator_hook_records_its_actual_identity_and_layout() {
    let ready = Arc::new(Barrier::new(2));
    let go = Arc::new(Barrier::new(2));
    let child_ready = ready.clone();
    let child_go = go.clone();
    let child = std::thread::spawn(move || {
        child_ready.wait();
        child_go.wait();
        let layout = std::alloc::Layout::from_size_align(37, 8).unwrap();
        // SAFETY: this exact System-backed global allocation is non-null and
        // freed once with its original Layout, before the joined thread exits.
        unsafe {
            let pointer = std::alloc::alloc(layout);
            assert!(!pointer.is_null());
            std::ptr::write_volatile(pointer, 7);
            std::alloc::dealloc(pointer, layout);
        }
    });
    ready.wait();
    let window = Window::begin(&[]).unwrap();
    go.wait();
    child.join().unwrap();
    assert_eq!(window.finish().unwrap_err().reasons, FOREIGN_THREAD);
    let failure = first_failure().unwrap();
    assert_eq!(failure.reason, FOREIGN_THREAD);
    assert!(!failure.caller_is_window_thread);
    assert_ne!(failure.owner_token, 0);
    assert_ne!(failure.emitter_token, 0);
    assert_ne!(failure.owner_token, failure.emitter_token);
    assert_eq!(failure.operation, "before_alloc_or_zeroed");
    assert_eq!((failure.bytes, failure.align), (37, 8));
    eprintln!("joined_foreign_allocator_first_event={failure:?}");
}
