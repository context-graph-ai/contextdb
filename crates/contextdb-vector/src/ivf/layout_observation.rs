use super::{CodebookAllocationLayouts, WriterWorkspaceLayout};
use std::{cell::RefCell, sync::Arc};

#[derive(Clone, Copy, Debug)]
pub enum IvfConstructorAllocationForTest {
    Codebook(CodebookAllocationLayouts),
    WriterWorkspace(WriterWorkspaceLayout),
}
pub trait IvfConstructorObserverForTest: Send + Sync {
    fn admitted(&self, allocation: IvfConstructorAllocationForTest);
    fn constructed(&self, _allocation: IvfConstructorAllocationForTest) {}
    fn before_reserve(&self, _owner: usize, _bytes: usize, _operation: &'static str) {}
    fn debited(&self, _invoice: u64, _owner: usize, _bytes: usize, _operation: &'static str) {}
    /// The covered fields precede Invoice in their holder and have dropped.
    fn buffers_freed(&self, _invoice: u64, _owner: usize, _bytes: usize, _operation: &'static str) {
    }
    fn released(&self, _invoice: u64, _owner: usize, _bytes: usize, _operation: &'static str) {}
}
thread_local! {
    static OBSERVER: RefCell<Option<Arc<dyn IvfConstructorObserverForTest>>> = const { RefCell::new(None) };
}
pub fn with_ivf_constructor_observer_for_test<R>(
    observer: Arc<dyn IvfConstructorObserverForTest>,
    run: impl FnOnce() -> R,
) -> R {
    struct Restore(Option<Arc<dyn IvfConstructorObserverForTest>>);
    impl Drop for Restore {
        fn drop(&mut self) {
            OBSERVER.with(|slot| {
                slot.replace(self.0.take());
            });
        }
    }
    let _restore = Restore(OBSERVER.with(|slot| slot.replace(Some(observer))));
    run()
}
pub(super) fn admitted(allocation: IvfConstructorAllocationForTest) {
    let observer = OBSERVER.with(|slot| slot.borrow().clone());
    if let Some(observer) = observer {
        observer.admitted(allocation);
    }
}

pub(super) fn constructed(allocation: IvfConstructorAllocationForTest) {
    let observer = OBSERVER.with(|slot| slot.borrow().clone());
    if let Some(observer) = observer {
        observer.constructed(allocation);
    }
}
pub(super) struct ReservationObservation {
    observer: Option<Arc<dyn IvfConstructorObserverForTest>>,
    invoice: u64,
    owner: usize,
    bytes: usize,
    operation: &'static str,
}
static NEXT_INVOICE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
impl ReservationObservation {
    pub fn capture(
        owner: &crate::MemoryBudgetOwner,
        bytes: usize,
        operation: &'static str,
    ) -> contextdb_core::Result<Self> {
        let observer = OBSERVER.with(|slot| slot.borrow().clone());
        let invoice = if observer.is_some() {
            NEXT_INVOICE
                .fetch_update(
                    std::sync::atomic::Ordering::Relaxed,
                    std::sync::atomic::Ordering::Relaxed,
                    |n| n.checked_add(1),
                )
                .map_err(|_| {
                    contextdb_core::Error::Other(
                        "constructor observation identity exhausted".into(),
                    )
                })?
        } else {
            0
        };
        let (original, _, _) = owner.as_ref().original_read_payer_for_test();
        let original = if original == 0 {
            owner.as_ref().stable_funder_identity()
        } else {
            original
        };
        if let Some(observer) = &observer {
            observer.before_reserve(original, bytes, operation);
        }
        Ok(Self {
            observer,
            invoice,
            owner: original,
            bytes,
            operation,
        })
    }
    pub fn debited(&self) {
        if let Some(observer) = &self.observer {
            observer.debited(self.invoice, self.owner, self.bytes, self.operation);
        }
    }
    pub fn buffers_freed(&self) {
        if let Some(observer) = &self.observer {
            observer.buffers_freed(self.invoice, self.owner, self.bytes, self.operation);
        }
    }
    pub fn released(&self) {
        if let Some(observer) = &self.observer {
            observer.released(self.invoice, self.owner, self.bytes, self.operation);
        }
    }
}

/// Enter the real counts-free loader over the caller's actual admitted pages.
/// This observes its existing constructor; it supplies no manifest, page bytes,
/// allocation, invoice or alternate implementation of its own.
pub fn load_codebook_values_for_test(
    owner: &crate::MemoryBudgetOwner,
    pages: &impl super::storage::ReadPages,
    manifest: super::storage::Manifest,
) -> contextdb_core::Result<()> {
    let book = super::book::Codebook::load_values(owner, pages, manifest)?;
    drop(book);
    Ok(())
}
