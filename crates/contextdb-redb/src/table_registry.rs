//! The write transaction's set of open tables.
//!
//! It is the same hash table and the same randomly seeded hasher as
//! `std::collections::HashMap`, with an allocator a caller can fund: every
//! array the registry allocates or frees passes through [`RegistryAlloc`], so
//! its exact requested `Layout` is known before it is allocated (by a refused
//! fallible reserve) and accounted for when it is freed.

use allocator_api2::alloc::{AllocError, Allocator, Global, Layout};
use std::hash::RandomState;
use std::panic;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

pub(crate) type OpenTables =
    hashbrown::HashMap<Box<str>, &'static panic::Location<'static>, RandomState, RegistryAlloc>;

pub(crate) fn new_open_tables(funding: &Arc<RegistryFunding>) -> OpenTables {
    hashbrown::HashMap::with_hasher_in(RandomState::new(), RegistryAlloc(funding.clone()))
}

/// What a registry allocation request is checked against.
#[derive(Default)]
pub(crate) struct RegistryFunding {
    /// Set for a caller's whole pass: an allocation made outside a funded
    /// reserve while it is set is counted as an overdraft.
    monitoring: AtomicBool,
    /// Set only inside one funded reserve or probe.
    fallible: AtomicBool,
    /// Bytes deposited for the current funded reserve.
    credit: AtomicUsize,
    refused_size: AtomicUsize,
    refused_align: AtomicUsize,
    live: AtomicUsize,
    peak: AtomicUsize,
    overdraft: AtomicUsize,
    allocations: AtomicUsize,
    #[cfg(feature = "test-seams")]
    records: std::sync::Mutex<Option<Vec<RegistryAllocationRecord>>>,
}

/// One allocation or free of a registry array, in the order they happened.
#[cfg(feature = "test-seams")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegistryAllocationRecord {
    pub generation: usize,
    pub address: usize,
    pub size: usize,
    pub align: usize,
    pub allocated: bool,
}

impl RegistryFunding {
    fn note(&self, address: usize, layout: Layout, allocated: bool) {
        if allocated {
            let generation = self.allocations.fetch_add(1, Ordering::SeqCst);
            let live = self.live.fetch_add(layout.size(), Ordering::SeqCst) + layout.size();
            self.peak.fetch_max(live, Ordering::SeqCst);
            #[cfg(feature = "test-seams")]
            self.record(generation, address, layout, true);
            #[cfg(not(feature = "test-seams"))]
            let _ = (generation, address);
        } else {
            self.live.fetch_sub(layout.size(), Ordering::SeqCst);
            #[cfg(feature = "test-seams")]
            self.record(self.allocations.load(Ordering::SeqCst), address, layout, false);
            #[cfg(not(feature = "test-seams"))]
            let _ = address;
        }
    }

    #[cfg(feature = "test-seams")]
    fn record(&self, generation: usize, address: usize, layout: Layout, allocated: bool) {
        if let Some(records) = self.records.lock().unwrap().as_mut() {
            records.push(RegistryAllocationRecord {
                generation,
                address,
                size: layout.size(),
                align: layout.align(),
                allocated,
            });
        }
    }

    fn take_refused(&self) -> Option<Layout> {
        let size = self.refused_size.swap(0, Ordering::SeqCst);
        let align = self.refused_align.swap(0, Ordering::SeqCst);
        if align == 0 {
            return None;
        }
        Layout::from_size_align(size, align).ok()
    }
}

/// The registry's allocator. Outside a funded reserve it is the global
/// allocator, exactly as before.
#[derive(Clone)]
pub(crate) struct RegistryAlloc(Arc<RegistryFunding>);

unsafe impl Allocator for RegistryAlloc {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        let funding = &*self.0;
        if funding.fallible.load(Ordering::SeqCst) {
            let credit = funding.credit.load(Ordering::SeqCst);
            if layout.size() > credit {
                funding
                    .refused_size
                    .store(layout.size(), Ordering::SeqCst);
                funding
                    .refused_align
                    .store(layout.align(), Ordering::SeqCst);
                return Err(AllocError);
            }
            let block = Global.allocate(layout)?;
            funding
                .credit
                .store(credit - layout.size(), Ordering::SeqCst);
            funding.note(block.cast::<u8>().as_ptr() as usize, layout, true);
            return Ok(block);
        }
        if funding.monitoring.load(Ordering::SeqCst) {
            funding
                .overdraft
                .fetch_add(layout.size(), Ordering::SeqCst);
        }
        let block = Global.allocate(layout)?;
        funding.note(block.cast::<u8>().as_ptr() as usize, layout, true);
        Ok(block)
    }

    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        let address = ptr.as_ptr() as usize;
        // SAFETY: forwarded unchanged; every block came from `Global`.
        unsafe { Global.deallocate(ptr, layout) };
        self.0.note(address, layout, false);
    }
}

/// Sets the per-call funded state and restores it on every exit.
pub(crate) struct FundedCall<'a>(&'a RegistryFunding);
impl<'a> FundedCall<'a> {
    pub(crate) fn enter(funding: &'a RegistryFunding, credit: usize) -> Self {
        funding.take_refused();
        funding.credit.store(credit, Ordering::SeqCst);
        funding.fallible.store(true, Ordering::SeqCst);
        Self(funding)
    }

    /// The layout the last refused allocation asked for, if this call
    /// refused one.
    pub(crate) fn refused(&self) -> Option<Layout> {
        self.0.take_refused()
    }

    pub(crate) fn allocations(&self) -> usize {
        self.0.allocations.load(Ordering::SeqCst)
    }
}
impl Drop for FundedCall<'_> {
    fn drop(&mut self) {
        self.0.fallible.store(false, Ordering::SeqCst);
        self.0.credit.store(0, Ordering::SeqCst);
    }
}

/// Why the cost of a registry change could not be learned or paid.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegistryReserveError {
    /// The requested capacity overflows the hash table's layout arithmetic;
    /// nothing was asked of the allocator.
    CapacityOverflow,
    /// The allocator refused this exact layout: the credit did not cover it,
    /// or the global allocator failed. The registry is unchanged.
    AllocError(Layout),
    /// A prepared replacement cannot hold the open tables without growing.
    ReplacementTooSmall,
}

pub(crate) fn reserve_error(
    error: hashbrown::TryReserveError,
    call: &FundedCall<'_>,
) -> RegistryReserveError {
    match error {
        hashbrown::TryReserveError::CapacityOverflow => RegistryReserveError::CapacityOverflow,
        hashbrown::TryReserveError::AllocError { layout } => {
            RegistryReserveError::AllocError(call.refused().unwrap_or(layout))
        }
    }
}

/// An empty registry array reserved ahead of time, so that putting the open
/// tables back into a baseline-sized array later allocates nothing.
pub struct RegistryReplacement {
    pub(crate) map: OpenTables,
}
impl RegistryReplacement {
    /// The bytes this replacement's array holds.
    pub fn allocation_bytes(&self) -> usize {
        self.map.allocation_size()
    }
}

/// Counts allocations of the registry made outside a funded reserve for as
/// long as it is held, and restores the previous state when dropped.
pub struct RegistryMonitor {
    funding: Arc<RegistryFunding>,
    previous: bool,
    overdraft_at_start: usize,
}
impl RegistryMonitor {
    pub(crate) fn start(funding: &Arc<RegistryFunding>) -> Self {
        let previous = funding.monitoring.swap(true, Ordering::SeqCst);
        Self {
            funding: funding.clone(),
            previous,
            overdraft_at_start: funding.overdraft.load(Ordering::SeqCst),
        }
    }

    /// Bytes the registry allocated outside a funded reserve since this
    /// monitor started.
    pub fn overdraft_bytes(&self) -> usize {
        self.funding.overdraft.load(Ordering::SeqCst) - self.overdraft_at_start
    }
}
impl Drop for RegistryMonitor {
    fn drop(&mut self) {
        self.funding
            .monitoring
            .store(self.previous, Ordering::SeqCst);
    }
}

#[cfg(feature = "test-seams")]
impl RegistryFunding {
    pub(crate) fn start_recording(&self) {
        *self.records.lock().unwrap() = Some(Vec::new());
    }

    pub(crate) fn take_records(&self) -> Vec<RegistryAllocationRecord> {
        self.records.lock().unwrap().take().unwrap_or_default()
    }

    pub(crate) fn live_and_peak(&self) -> (usize, usize) {
        (
            self.live.load(Ordering::SeqCst),
            self.peak.load(Ordering::SeqCst),
        )
    }
}
