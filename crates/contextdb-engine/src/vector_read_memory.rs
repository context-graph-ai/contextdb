//! Vector copies use the requesting reader's existing budget capability.
use contextdb_core::{Result, read_memory::ReadMemoryBudget};
use contextdb_vector::{MemoryBudget, VectorCopyAdmission};
use std::sync::Arc;

#[derive(Debug)]
struct ReaderVectorBudget(Arc<dyn ReadMemoryBudget>);

impl MemoryBudget for ReaderVectorBudget {
    fn try_allocate_for(&self, bytes: usize, _: &str, _: &str, _: &str) -> Result<()> {
        self.0.try_reserve(
            bytes,
            "vector_copy",
            "materialize",
            "Reduce the selected copy or raise the executing reader's memory limit.",
        )
    }

    fn release(&self, bytes: usize) {
        self.0.release(bytes);
    }
}

/// Adapt the executing READ once, so copy admission and search share its capability.
pub(crate) fn budget(read_memory: Arc<dyn ReadMemoryBudget>) -> Arc<dyn MemoryBudget> {
    Arc::new(ReaderVectorBudget(read_memory))
}

pub fn admission(read_memory: Arc<dyn ReadMemoryBudget>) -> VectorCopyAdmission {
    VectorCopyAdmission::new(budget(read_memory))
}

pub trait ReadRequestBudgetProvider: std::fmt::Debug + Send + Sync {
    fn request_budget(&self) -> Result<Arc<dyn ReadMemoryBudget>>;
}
#[derive(Debug)]
pub struct DeclaredReadMemory {
    limit: u64,
    file_child_high_water: std::sync::atomic::AtomicU64,
    retained: std::sync::atomic::AtomicU64,
    live_child_control_bytes: std::sync::atomic::AtomicU64,
    live_acl_cache_bytes: std::sync::atomic::AtomicU64,
}
impl DeclaredReadMemory {
    pub fn from_reader_limit(limit: u64) -> Arc<Self> {
        Arc::new(Self {
            limit,
            file_child_high_water: std::sync::atomic::AtomicU64::new(0),
            retained: std::sync::atomic::AtomicU64::new(0),
            live_child_control_bytes: std::sync::atomic::AtomicU64::new(0),
            live_acl_cache_bytes: std::sync::atomic::AtomicU64::new(0),
        })
    }
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn retained_bytes_for_test(&self) -> u64 {
        self.retained.load(std::sync::atomic::Ordering::SeqCst)
    }
    /// Quiescent observation of actual admitted live child controls and parent bytes.
    pub(crate) fn standing_control_bytes(&self) -> (u64, u64) {
        (
            self.live_child_control_bytes
                .load(std::sync::atomic::Ordering::SeqCst),
            self.retained.load(std::sync::atomic::Ordering::SeqCst),
        )
    }

    /// Bytes every reader child currently holds against this parent.
    pub(crate) fn charged_bytes(&self) -> u64 {
        self.retained.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub(crate) fn acl_cache_bytes(&self) -> u64 {
        self.live_acl_cache_bytes
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    pub(crate) fn acl_cache_budget(self: &Arc<Self>) -> Result<Arc<dyn ReadMemoryBudget>> {
        let control_bytes =
            std::mem::size_of::<ReaderAclCacheBudget>() + 2 * std::mem::size_of::<usize>();
        self.try_reserve(
            control_bytes,
            "reader_acl_cache",
            "budget_owner",
            "Increase reader memory to cache ACL grants.",
        )?;
        self.live_acl_cache_bytes
            .fetch_add(control_bytes as u64, std::sync::atomic::Ordering::SeqCst);
        Ok(Arc::new(ReaderAclCacheBudget {
            parent: self.clone(),
            control_bytes,
        }))
    }

    /// Largest admitted live-byte figure recorded by an individual file
    /// reader child. Children update this before the native allocation and
    /// the session keeps the receipt after an operation child is dropped.
    #[cfg(feature = "test-seams")]
    pub(crate) fn file_child_high_water(&self) -> u64 {
        self.file_child_high_water
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    pub fn child(self: &Arc<Self>) -> Result<Arc<dyn ReadMemoryBudget>> {
        let control_bytes =
            std::mem::size_of::<DeclaredReadChild>() + 2 * std::mem::size_of::<usize>();
        self.try_reserve(
            control_bytes,
            "reader",
            "request_owner",
            "Reduce simultaneous requests or raise the declared reader memory limit.",
        )?;
        // The existing nonzero amount is the counted marker; no child exists
        // on refusal, and the native child layout and admission stay unchanged.
        debug_assert_ne!(control_bytes, 0);
        self.live_child_control_bytes
            .fetch_add(control_bytes as u64, std::sync::atomic::Ordering::SeqCst);
        let child = Arc::new(DeclaredReadChild {
            parent: self.clone(),
            retained: std::sync::atomic::AtomicU64::new(0),
            control_bytes,
        });
        #[cfg(feature = "test-seams")]
        register_child_observer(&child);
        Ok(child)
    }
}
#[derive(Debug)]
struct DeclaredReadChild {
    control_bytes: usize,
    parent: Arc<DeclaredReadMemory>,
    retained: std::sync::atomic::AtomicU64,
}
fn reader_memory_refusal(
    limit: u64,
    held: u64,
    requested: usize,
    subsystem: &'static str,
    operation: &'static str,
    hint: &'static str,
) -> contextdb_core::Error {
    contextdb_core::Error::MemoryBudgetExceeded {
        subsystem: subsystem.into(),
        operation: operation.into(),
        requested_bytes: requested,
        available_bytes: usize::try_from(limit.saturating_sub(held)).unwrap_or(usize::MAX),
        budget_limit_bytes: usize::try_from(limit).unwrap_or(usize::MAX),
        hint: hint.into(),
    }
}
#[cfg(feature = "test-seams")]
thread_local! {
    static READER_BUDGET_LEDGER: std::cell::RefCell<Option<Vec<crate::memory_accounting::BudgetLedgerEntry>>> = const { std::cell::RefCell::new(None) };
}

#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub fn watch_reader_budget_ledger_for_test() {
    READER_BUDGET_LEDGER.with(|ledger| *ledger.borrow_mut() = Some(Vec::new()));
}

#[cfg(feature = "test-seams")]
#[doc(hidden)]
pub fn take_reader_budget_ledger_for_test() -> Vec<crate::memory_accounting::BudgetLedgerEntry> {
    READER_BUDGET_LEDGER.with(|ledger| ledger.borrow_mut().take().unwrap_or_default())
}

impl ReadMemoryBudget for DeclaredReadMemory {
    fn try_reserve(
        &self,
        bytes: usize,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> Result<()> {
        use std::sync::atomic::Ordering;
        self.retained
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |held| {
                held.checked_add(bytes as u64)
                    .filter(|after| *after <= self.limit)
            })
            .map_err(|held| {
                reader_memory_refusal(self.limit, held, bytes, subsystem, operation, hint)
            })?;
        #[cfg(feature = "test-seams")]
        READER_BUDGET_LEDGER.with(|ledger| {
            if let Some(entries) = ledger.borrow_mut().as_mut() {
                entries.push(crate::memory_accounting::BudgetLedgerEntry {
                    what: format!("{subsystem}/{operation}"),
                    bytes,
                    used_after: usize::try_from(self.retained.load(Ordering::SeqCst))
                        .unwrap_or(usize::MAX),
                });
            }
        });
        Ok(())
    }
    fn release(&self, bytes: usize) {
        self.retained
            .fetch_sub(bytes as u64, std::sync::atomic::Ordering::SeqCst);
    }
}
impl ReadMemoryBudget for DeclaredReadChild {
    fn try_reserve(
        &self,
        bytes: usize,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> Result<()> {
        self.parent.try_reserve(bytes, subsystem, operation, hint)?;
        let held = self
            .retained
            .fetch_add(bytes as u64, std::sync::atomic::Ordering::SeqCst)
            + bytes as u64;
        self.parent
            .file_child_high_water
            .fetch_max(held, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
    fn release(&self, bytes: usize) {
        self.retained
            .fetch_sub(bytes as u64, std::sync::atomic::Ordering::SeqCst);
        self.parent.release(bytes);
    }
}
impl Drop for DeclaredReadChild {
    fn drop(&mut self) {
        #[cfg(feature = "test-seams")]
        child_observers()
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(&(self as *const Self as usize));
        if self.control_bytes != 0 {
            self.parent.live_child_control_bytes.fetch_sub(
                self.control_bytes as u64,
                std::sync::atomic::Ordering::SeqCst,
            );
            self.parent.release(self.control_bytes);
        }
    }
}
impl ReadRequestBudgetProvider for Arc<DeclaredReadMemory> {
    fn request_budget(&self) -> Result<Arc<dyn ReadMemoryBudget>> {
        self.child()
    }
}

#[cfg(feature = "test-seams")]
impl DeclaredReadMemory {
    /// Passive parent-byte observation for independent source-unit lifetimes.
    pub(crate) fn retained_bytes_for_source_unit_test(&self) -> u64 {
        self.retained.load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[derive(Debug)]
struct ReaderAclCacheBudget {
    parent: Arc<DeclaredReadMemory>,
    control_bytes: usize,
}
impl ReadMemoryBudget for ReaderAclCacheBudget {
    fn try_reserve(
        &self,
        bytes: usize,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> Result<()> {
        self.parent.try_reserve(bytes, subsystem, operation, hint)?;
        self.parent
            .live_acl_cache_bytes
            .fetch_add(bytes as u64, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
    fn release(&self, bytes: usize) {
        self.parent
            .live_acl_cache_bytes
            .fetch_sub(bytes as u64, std::sync::atomic::Ordering::SeqCst);
        self.parent.release(bytes);
    }
}
impl Drop for ReaderAclCacheBudget {
    fn drop(&mut self) {
        self.release(self.control_bytes);
    }
}

// Weak observation of the native owner, without extending its charge lifetime.
#[cfg(feature = "test-seams")]
#[derive(Clone)]
pub(crate) struct DeclaredReadChildObserver {
    child: std::sync::Weak<DeclaredReadChild>,
    parent: std::sync::Weak<DeclaredReadMemory>,
    owner_identity: usize,
    parent_identity: usize,
}
#[cfg(feature = "test-seams")]
impl DeclaredReadChildObserver {
    pub(crate) fn sample(&self) -> crate::read_work_probe::DeclaredReadChildFunding {
        use std::sync::atomic::Ordering;
        let child = self.child.upgrade();
        let parent = self.parent.upgrade();
        crate::read_work_probe::DeclaredReadChildFunding {
            owner_identity: self.owner_identity,
            alive: child.is_some(),
            charged_bytes: child.as_ref().map_or(0, |child| child.retained.load(Ordering::SeqCst)),
            parent_identity: self.parent_identity,
            parent_alive: parent.is_some(),
            parent_charged_bytes: parent.as_ref().map_or(0, |parent| parent.retained.load(Ordering::SeqCst)),
        }
    }
}
#[cfg(feature = "test-seams")]
fn child_observers() -> &'static std::sync::Mutex<std::collections::HashMap<usize, DeclaredReadChildObserver>> {
    static OBSERVERS: std::sync::OnceLock<std::sync::Mutex<std::collections::HashMap<usize, DeclaredReadChildObserver>>> = std::sync::OnceLock::new();
    OBSERVERS.get_or_init(Default::default)
}
#[cfg(feature = "test-seams")]
fn register_child_observer(child: &Arc<DeclaredReadChild>) {
    let owner_identity = Arc::as_ptr(child) as usize;
    let observer = DeclaredReadChildObserver {
        child: Arc::downgrade(child),
        parent: Arc::downgrade(&child.parent),
        owner_identity,
        parent_identity: Arc::as_ptr(&child.parent) as usize,
    };
    child_observers().lock().unwrap_or_else(|error| error.into_inner()).insert(owner_identity, observer);
}
#[cfg(feature = "test-seams")]
pub(crate) fn declared_child_observer(read_memory: &Arc<dyn ReadMemoryBudget>) -> Option<DeclaredReadChildObserver> {
    let identity = Arc::as_ptr(read_memory) as *const () as usize;
    child_observers().lock().unwrap_or_else(|error| error.into_inner()).get(&identity).cloned()
}
