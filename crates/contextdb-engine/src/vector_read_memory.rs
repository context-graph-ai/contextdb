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
    retained: std::sync::atomic::AtomicU64,
    live_child_control_bytes: std::sync::atomic::AtomicU64,
    live_acl_cache_bytes: std::sync::atomic::AtomicU64,
}
impl DeclaredReadMemory {
    pub fn from_reader_limit(limit: u64) -> Arc<Self> {
        Arc::new(Self {
            limit,
            retained: std::sync::atomic::AtomicU64::new(0),
            live_child_control_bytes: std::sync::atomic::AtomicU64::new(0),
            live_acl_cache_bytes: std::sync::atomic::AtomicU64::new(0),
        })
    }
    #[cfg(test)]
    pub(crate) fn retained_bytes_for_test(&self) -> u64 {
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
        Ok(Arc::new(DeclaredReadChild {
            parent: self.clone(),
            retained: std::sync::atomic::AtomicU64::new(0),
            control_bytes,
        }))
    }
}
#[derive(Debug)]
struct DeclaredReadChild {
    control_bytes: usize,
    parent: Arc<DeclaredReadMemory>,
    retained: std::sync::atomic::AtomicU64,
}
fn reader_memory_refusal(limit: u64, held: u64, requested: u64) -> contextdb_core::Error {
    use contextdb_core::read_contract::{
        OwnerLimitExceededDetail, ReadFailure, ReadFailureLimit, RequiredBytesSetting,
    };
    contextdb_core::Error::ReadFailure(ReadFailure::owner_limit_exceeded(
        OwnerLimitExceededDetail {
            limit: ReadFailureLimit::Memory,
            value: limit,
            required: Some(RequiredBytesSetting {
                required_bytes: held.saturating_add(requested),
                required_setting: "reader memory".to_string(),
            }),
            statement: None,
        },
    ))
}
impl ReadMemoryBudget for DeclaredReadMemory {
    fn try_reserve(
        &self,
        bytes: usize,
        _: &'static str,
        _: &'static str,
        _: &'static str,
    ) -> Result<()> {
        use std::sync::atomic::Ordering;
        self.retained
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |held| {
                held.checked_add(bytes as u64)
                    .filter(|after| *after <= self.limit)
            })
            .map_err(|held| reader_memory_refusal(self.limit, held, bytes as u64))?;
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
        self.retained
            .fetch_add(bytes as u64, std::sync::atomic::Ordering::SeqCst);
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
