//! Test-recorder infrastructure, never engine data or a writer invoice.
use std::alloc::{GlobalAlloc, Layout, System, handle_alloc_error};
use std::cell::{BorrowMutError, Cell, Ref, RefCell, RefMut};

/// A tiny TLS owner. Only an explicit observation start may initialize it.
/// The separate borrow token preserves deferred recording during proof reads.
pub(crate) struct RecorderBuffer<T, const N: usize> {
    pointer: Cell<*mut T>,
    borrow: RefCell<()>,
}

impl<T, const N: usize> RecorderBuffer<T, N> {
    pub(crate) const fn new() -> Self {
        Self {
            pointer: Cell::new(std::ptr::null_mut()),
            borrow: RefCell::new(()),
        }
    }

    pub(crate) fn is_initialized(&self) -> bool {
        !self.pointer.get().is_null()
    }

    pub(crate) fn initialize(&self, element: impl Fn() -> T) {
        if self.is_initialized() {
            return;
        }
        assert!(N > 0 && std::mem::size_of::<T>() > 0);
        let _borrow = self.borrow.borrow_mut();
        let layout = Layout::array::<T>(N).expect("fixed recorder layout");
        // System bypasses the global measured allocator intentionally: that
        // allocator owns a Header which these infrastructure buffers lack.
        let pointer = unsafe { System.alloc(layout) }.cast::<T>();
        if pointer.is_null() {
            handle_alloc_error(layout);
        }
        struct Initializing<T> {
            pointer: *mut T,
            initialized: usize,
            layout: Layout,
        }
        impl<T> Drop for Initializing<T> {
            fn drop(&mut self) {
                unsafe {
                    std::ptr::drop_in_place(std::ptr::slice_from_raw_parts_mut(
                        self.pointer,
                        self.initialized,
                    ));
                    System.dealloc(self.pointer.cast(), self.layout);
                }
            }
        }
        let mut pending = Initializing {
            pointer,
            initialized: 0,
            layout,
        };
        // Publish only the fully initialized buffer. No giant temporary array.
        // Even a failed initializer retires every initialized element and the
        // exact System allocation, while the callback-visible pointer is null.
        for index in 0..N {
            unsafe {
                pointer.add(index).write(element());
            }
            pending.initialized += 1;
        }
        self.pointer.set(pointer);
        std::mem::forget(pending);
    }

    pub(crate) fn borrow(&self) -> Ref<'_, [T]> {
        let token = self.borrow.borrow();
        let pointer = self.pointer.get();
        assert!(
            !pointer.is_null(),
            "recorder must be armed outside callbacks"
        );
        // The TLS owner outlives the borrow token, which prevents a mutable
        // view until this view is released. Destruction nulls before freeing.
        Ref::map(token, |_| unsafe { std::slice::from_raw_parts(pointer, N) })
    }

    pub(crate) fn try_borrow_mut(&self) -> Result<RefMut<'_, [T]>, BorrowMutError> {
        let token = self.borrow.try_borrow_mut()?;
        let pointer = self.pointer.get();
        assert!(
            !pointer.is_null(),
            "recorder must be armed outside callbacks"
        );
        Ok(RefMut::map(token, |_| unsafe {
            std::slice::from_raw_parts_mut(pointer, N)
        }))
    }

    pub(crate) fn borrow_mut(&self) -> RefMut<'_, [T]> {
        self.try_borrow_mut().expect("exclusive recorder access")
    }
}

impl<T, const N: usize> Drop for RecorderBuffer<T, N> {
    fn drop(&mut self) {
        // LocalKey::try_with rejects this owner during destruction. Null it
        // first as well; System retirement cannot enter MeasuredAllocator.
        let pointer = self.pointer.replace(std::ptr::null_mut());
        if !pointer.is_null() {
            unsafe {
                std::ptr::drop_in_place(std::ptr::slice_from_raw_parts_mut(pointer, N));
                System.dealloc(pointer.cast(), Layout::array::<T>(N).unwrap());
            }
        }
    }
}

#[cfg(all(test, target_os = "linux"))]
#[test]
fn recorder_static_tls_fits_the_default_test_thread() {
    use std::io::{Read, Seek, SeekFrom};
    let mut elf = std::fs::File::open("/proc/self/exe").unwrap();
    let mut header = [0u8; 64];
    elf.read_exact(&mut header).unwrap();
    assert_eq!(&header[..4], b"\x7fELF");
    assert!(matches!(header[4], 1 | 2));
    assert!(matches!(header[5], 1 | 2));
    let little = header[5] == 1;
    let number = |bytes: &[u8]| -> u64 {
        if little {
            bytes.iter().rev().fold(0, |n, b| (n << 8) | u64::from(*b))
        } else {
            bytes.iter().fold(0, |n, b| (n << 8) | u64::from(*b))
        }
    };
    let wide = header[4] == 2;
    let (offset, stride, count) = if wide {
        (
            number(&header[32..40]),
            number(&header[54..56]),
            number(&header[56..58]),
        )
    } else {
        (
            number(&header[28..32]),
            number(&header[42..44]),
            number(&header[44..46]),
        )
    };
    assert!(
        count > 0 && count < 65535,
        "ordinary bounded program header table"
    );
    assert_eq!(stride, if wide { 56 } else { 32 });
    let table_end = offset
        .checked_add(stride.checked_mul(count).unwrap())
        .unwrap();
    assert!(table_end <= elf.metadata().unwrap().len());
    let mut tls = None;
    let mut program = [0u8; 56];
    for index in 0..count {
        elf.seek(SeekFrom::Start(offset + index * stride)).unwrap();
        elf.read_exact(&mut program[..stride as usize]).unwrap();
        if number(&program[..4]) == 7 {
            assert!(tls.is_none(), "one PT_TLS");
            tls = Some(if wide {
                number(&program[40..48])
            } else {
                number(&program[20..24])
            });
        }
    }
    let bytes = tls.expect("test binary has PT_TLS");
    assert!(
        bytes <= 204_800,
        "test instrumentation static TLS is {bytes} bytes; cap 204800"
    );
}

#[test]
fn recorder_partial_initialization_retires_elements_and_can_be_retried() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    static DROPPED: AtomicUsize = AtomicUsize::new(0);
    struct Element {
        _byte: u8,
    }
    impl Drop for Element {
        fn drop(&mut self) {
            DROPPED.fetch_add(1, Ordering::SeqCst);
        }
    }
    let buffer = RecorderBuffer::<Element, 8>::new();
    let initialized = Cell::new(0);
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            buffer.initialize(|| {
                let n = initialized.get();
                if n == 3 {
                    panic!("initializer failure");
                }
                initialized.set(n + 1);
                Element { _byte: 0 }
            });
        }))
        .is_err()
    );
    assert!(!buffer.is_initialized());
    assert_eq!(DROPPED.load(Ordering::SeqCst), 3);
    buffer.initialize(|| Element { _byte: 0 });
    assert!(buffer.is_initialized());
    assert_eq!(buffer.borrow().len(), 8);
    drop(buffer);
    assert_eq!(DROPPED.load(Ordering::SeqCst), 11);
}
