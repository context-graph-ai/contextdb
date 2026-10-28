//! Publication funding owns the original quota, never the execution kernel.
#![allow(
    clippy::result_large_err,
    reason = "Preserve inline budget refusals without allocating an error."
)]
use super::*;
use contextdb_core::read_memory::{NativeReadMemory, NativeRefusal, ReadMemoryOwner};
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug)]
struct FundingCounter {
    original: ReadMemoryOwner,
    limit: usize,
    held: AtomicUsize,
}
impl FundingCounter {
    fn admit(&self, bytes: usize) -> std::result::Result<(), usize> {
        self.held
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |held| {
                held.checked_add(bytes).filter(|after| *after <= self.limit)
            })
            .map(|_| ())
    }
    fn limit_error(&self) -> Error {
        Error::ReadFailure(ReadFailure::owner_limit_exceeded(
            OwnerLimitExceededDetail {
                limit: ReadFailureLimit::Memory,
                value: u64::try_from(self.limit).unwrap_or(u64::MAX),
                required: None,
                statement: None,
            },
        ))
    }
    fn subtract(&self, bytes: usize) {
        self.held
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |held| {
                held.checked_sub(bytes)
            })
            .expect("a live funding credit releases exactly once");
    }
}
impl ReadMemoryBudget for FundingCounter {
    #[cfg(feature = "test-seams")]
    fn funding_owner_handle_for_test(&self) -> Option<&ReadMemoryOwner> {
        Some(&self.original)
    }
    #[cfg(feature = "test-seams")]
    fn funding_owner_for_test(&self) -> Option<&Arc<dyn ReadMemoryBudget>> {
        self.original.funding_owner_for_test()
    }
    #[cfg(feature = "test-seams")]
    fn funding_accountant_identity_for_test(&self) -> Option<usize> {
        self.original.funding_accountant_identity_for_test()
    }
    fn try_reserve(
        &self,
        bytes: usize,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> Result<()> {
        self.admit(bytes).map_err(|_| self.limit_error())?;
        if let Err(error) = self.original.try_reserve(bytes, subsystem, operation, hint) {
            self.subtract(bytes);
            return Err(error);
        }
        Ok(())
    }
    fn native_reserve_supported(&self) -> bool {
        self.original.native_reserve_supported()
    }
    fn try_reserve_native(
        &self,
        bytes: usize,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> std::result::Result<(), NativeRefusal> {
        self.admit(bytes)
            .map_err(|held| NativeRefusal::MemoryBudgetExceeded {
                subsystem,
                operation,
                requested_bytes: bytes,
                available_bytes: self.limit.saturating_sub(held),
                budget_limit_bytes: self.limit,
                hint,
            })?;
        if let Err(error) = self
            .original
            .try_reserve_native(bytes, subsystem, operation, hint)
        {
            self.subtract(bytes);
            return Err(error);
        }
        Ok(())
    }
    fn release(&self, bytes: usize) {
        self.original.release(bytes);
        self.subtract(bytes);
    }
}

#[derive(Debug, Clone)]
pub(super) struct RequestFunding(NativeReadMemory<FundingCounter>);
impl RequestFunding {
    pub(super) fn new(original: ReadMemoryOwner, limit: u64) -> Result<Self> {
        let bytes = ReadMemoryOwner::native_layout::<FundingCounter, ReadCredit>().size();
        let payload = FundingCounter {
            original: original.clone(),
            limit: usize::try_from(limit).unwrap_or(usize::MAX),
            held: AtomicUsize::new(bytes),
        };
        if bytes > payload.limit {
            return Err(payload.limit_error());
        }
        // The cell's own invoice is outside the cell, paid directly by the
        // original. Seeding occupancy counts it once without an ownership cycle.
        let invoice = ReadCredit::try_new_owned(
            original,
            bytes,
            "bounded_read",
            "request_funding",
            "Release retained read controls or raise MEMORY_LIMIT.",
        )?;
        ReadMemoryOwner::try_native_typed(payload, invoice)
            .map(Self)
            .map_err(Error::NativeEngineOther)
    }
    pub(super) fn owner(&self) -> ReadMemoryOwner {
        self.0.owner()
    }
    pub(super) fn held(&self) -> usize {
        self.0.held.load(Ordering::SeqCst)
    }
}

#[derive(Debug)]
struct PublicationBudget {
    funding: RequestFunding,
    checkpoint: parking_lot::Mutex<ReadPublicationBudget>,
}
impl ReadMemoryBudget for PublicationBudget {
    #[cfg(feature = "test-seams")]
    fn funding_owner_handle_for_test(&self) -> Option<&ReadMemoryOwner> {
        Some(&self.funding.0.original)
    }
    #[cfg(feature = "test-seams")]
    fn funding_owner_for_test(&self) -> Option<&Arc<dyn ReadMemoryBudget>> {
        self.funding.0.original.funding_owner_for_test()
    }
    #[cfg(feature = "test-seams")]
    fn funding_accountant_identity_for_test(&self) -> Option<usize> {
        self.funding
            .0
            .original
            .funding_accountant_identity_for_test()
    }
    fn try_reserve(
        &self,
        bytes: usize,
        _: &'static str,
        _: &'static str,
        _: &'static str,
    ) -> Result<()> {
        let checkpoint = self.checkpoint.lock().clone();
        if checkpoint.cancellation.is_cancelled() {
            return Err(Error::ReadCancelled);
        }
        if checkpoint.elapsed() {
            return Err(Error::ReadFailure(checkpoint.exceeded_failure()));
        }
        self.funding.owner().try_reserve(
            bytes,
            "bounded_read",
            BoundedWorkSource::AccessControl.reservation_operation(),
            "Lower the statement's LIMIT or width, or raise MEMORY_LIMIT.",
        )
    }
    // Like the replaced SourceDecodeBudget, this checkpoint-bearing adapter
    // uses typed Legacy errors: NativeRefusal cannot represent cancellation.
    // The underlying shared quota still forwards original scalar admission.
    fn release(&self, bytes: usize) {
        self.funding.owner().release(bytes);
    }
}

#[derive(Debug, Clone)]
pub(crate) struct PublicationFunding(NativeReadMemory<PublicationBudget>);
impl PublicationFunding {
    pub(super) fn new(funding: RequestFunding, checkpoint: ReadPublicationBudget) -> Result<Self> {
        let bytes = ReadMemoryOwner::native_layout::<PublicationBudget, ReadCredit>().size();
        let invoice = ReadCredit::try_new_owned(
            funding.owner(),
            bytes,
            "bounded_read",
            BoundedWorkSource::AccessControl.reservation_operation(),
            "Lower the statement's LIMIT or width, or raise MEMORY_LIMIT.",
        )?;
        ReadMemoryOwner::try_native_typed(
            PublicationBudget {
                funding,
                checkpoint: parking_lot::Mutex::new(checkpoint),
            },
            invoice,
        )
        .map(Self)
        .map_err(Error::NativeEngineOther)
    }
    pub(in crate::executor) fn owner(&self) -> ReadMemoryOwner {
        self.0.owner()
    }
    pub(in crate::executor) fn refresh(&self, checkpoint: ReadPublicationBudget) {
        *self.0.checkpoint.lock() = checkpoint;
    }
}
