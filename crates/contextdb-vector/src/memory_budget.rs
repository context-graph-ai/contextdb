use contextdb_core::Result;
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Opaque accounting capability consumed by vector storage. The database
/// engine owns the implementation and never returns that implementation to
/// callers; this crate can reserve and release bytes without exposing a
/// durable-limit mutation handle.
#[doc(hidden)]
pub trait MemoryBudget: Debug + Send + Sync {
    fn try_allocate_for(
        &self,
        bytes: usize,
        subsystem: &str,
        operation: &str,
        hint: &str,
    ) -> Result<()>;

    fn release(&self, bytes: usize);

    /// Bytes currently available under a finite shared budget. Unlimited or
    /// request-local budgets return `None`, so residency is retained.
    fn available_bytes(&self) -> Option<usize> {
        None
    }

    /// Move bytes already charged to the active caller into a retained store
    /// owner without touching the shared total. Budgets with no caller-bound
    /// transfer return `false`, and loaders reserve those bytes themselves.
    fn adopt_caller_reservation(&self, _bytes: usize) -> Result<bool> {
        Ok(false)
    }
}

#[derive(Debug)]
struct UnlimitedMemoryBudget;

impl MemoryBudget for UnlimitedMemoryBudget {
    fn try_allocate_for(
        &self,
        _bytes: usize,
        _subsystem: &str,
        _operation: &str,
        _hint: &str,
    ) -> Result<()> {
        Ok(())
    }

    fn release(&self, _bytes: usize) {}
}

pub(crate) fn unlimited_memory_budget() -> Arc<dyn MemoryBudget> {
    Arc::new(UnlimitedMemoryBudget)
}

/// One admitted vector workspace.  The counter and accountant reservation
/// move together and are both returned by this value's single `Drop`.
#[doc(hidden)]
pub struct VectorWorkspaceReservation {
    accountant: Arc<dyn MemoryBudget>,
    live_bytes: Arc<AtomicUsize>,
    bytes: usize,
}

impl VectorWorkspaceReservation {
    pub(crate) fn try_new(
        accountant: Arc<dyn MemoryBudget>,
        live_bytes: Arc<AtomicUsize>,
        bytes: usize,
        operation: &str,
        hint: &str,
    ) -> Result<Self> {
        accountant.try_allocate_for(bytes, "vector_index", operation, hint)?;
        live_bytes.fetch_add(bytes, Ordering::SeqCst);
        Ok(Self {
            accountant,
            live_bytes,
            bytes,
        })
    }

    /// Reconcile this reservation when a workspace changes ownership phase.
    /// Growth is admitted before the visible charge changes; shrinkage keeps
    /// the old charge until the released bytes no longer have an owner.
    #[doc(hidden)]
    pub fn resize(&mut self, bytes: usize, operation: &str, hint: &str) -> Result<()> {
        if bytes > self.bytes {
            let additional = bytes - self.bytes;
            self.accountant
                .try_allocate_for(additional, "vector_index", operation, hint)?;
            self.live_bytes.fetch_add(additional, Ordering::SeqCst);
        } else if bytes < self.bytes {
            let released = self.bytes - bytes;
            let previous = self.live_bytes.fetch_sub(released, Ordering::SeqCst);
            debug_assert!(previous >= released);
            self.accountant.release(released);
        }
        self.bytes = bytes;
        Ok(())
    }
}

impl Drop for VectorWorkspaceReservation {
    fn drop(&mut self) {
        let previous = self.live_bytes.fetch_sub(self.bytes, Ordering::SeqCst);
        debug_assert!(previous >= self.bytes);
        self.accountant.release(self.bytes);
        self.bytes = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Default)]
    struct CountingBudget(AtomicUsize);

    impl MemoryBudget for CountingBudget {
        fn try_allocate_for(
            &self,
            bytes: usize,
            _subsystem: &str,
            _operation: &str,
            _hint: &str,
        ) -> Result<()> {
            self.0.fetch_add(bytes, Ordering::SeqCst);
            Ok(())
        }

        fn release(&self, bytes: usize) {
            let previous = self.0.fetch_sub(bytes, Ordering::SeqCst);
            assert!(previous >= bytes);
        }
    }

    #[test]
    fn resizing_workspace_moves_one_charge_between_ownership_phases() {
        let budget = Arc::new(CountingBudget::default());
        let live = Arc::new(AtomicUsize::new(0));
        let mut reservation =
            VectorWorkspaceReservation::try_new(budget.clone(), live.clone(), 900, "build", "test")
                .unwrap();
        assert_eq!(budget.0.load(Ordering::SeqCst), 900);
        assert_eq!(live.load(Ordering::SeqCst), 900);

        reservation.resize(300, "persist", "test").unwrap();
        assert_eq!(budget.0.load(Ordering::SeqCst), 300);
        assert_eq!(live.load(Ordering::SeqCst), 300);

        reservation.resize(450, "persist", "test").unwrap();
        assert_eq!(budget.0.load(Ordering::SeqCst), 450);
        assert_eq!(live.load(Ordering::SeqCst), 450);

        drop(reservation);
        assert_eq!(budget.0.load(Ordering::SeqCst), 0);
        assert_eq!(live.load(Ordering::SeqCst), 0);
    }
}
