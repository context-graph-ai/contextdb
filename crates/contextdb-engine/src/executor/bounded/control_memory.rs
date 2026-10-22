//! Original fixed invoices outside the std Arc allocations they price.
use super::*;
use crate::local_transport::query_funding::Funded;
use crate::read_image_memory::{ImageMemory, ImageMemoryScope};
use contextdb_core::read_memory::ReadShared;

/// Field order matters on the final strong AND the final weak: std destroys
/// its allocation first, then this alias releases the shared original invoice.
/// Raw strong clones need a paired outer scope or their original invoice.
/// Weak aliases always carry the invoice; Deref is private to this module.
pub(in crate::executor) struct ControlArc<T> {
    value: Arc<T>,
    invoice: ReadShared<ReadCredit>,
}
pub(super) struct ControlWeak<T> {
    value: std::sync::Weak<T>,
    invoice: ReadShared<ReadCredit>,
}
impl<T> ControlArc<T> {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn arc_layout() -> Result<std::alloc::Layout> {
        // Conditional on the pinned compiler's std Arc implementation: its
        // repr(C) header has two atomic usize counts followed by T. This is
        // not a portable Arc ABI; qualification must compare the actual
        // allocator request, including padding for over-aligned payloads.
        std::alloc::Layout::new::<[std::sync::atomic::AtomicUsize; 2]>()
            .extend(std::alloc::Layout::new::<T>())
            .map(|(layout, _)| layout.pad_to_align())
            .map_err(|_| Error::Other("request control layout overflow".into()))
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn arc_bytes() -> Result<usize> {
        Self::arc_layout().map(|layout| layout.size())
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn control_bytes() -> Result<usize> {
        Self::arc_bytes()?
            .checked_add(ReadShared::<ReadCredit>::layout().size())
            .ok_or_else(|| Error::Other("request control layout overflow".into()))
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn control_bytes_for(_: &T) -> Result<usize> {
        Self::control_bytes()
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn admit(
        owner: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<ReadShared<ReadCredit>> {
        let owner = owner.into();

        Self::admit_owned(owner)
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn admit_owned(
        owner: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<ReadShared<ReadCredit>> {
        let owner = owner.into();

        let keeper = ReadCredit::try_new_owned(
            owner.clone(),
            ReadShared::<ReadCredit>::layout().size(),
            "bounded_read",
            "control_keeper",
            "Release retained read controls or raise MEMORY_LIMIT.",
        )?;
        let payload = ReadCredit::try_new_owned(
            owner,
            Self::arc_bytes()?,
            "bounded_read",
            "request_control",
            "Release retained read controls or raise MEMORY_LIMIT.",
        )?;
        #[cfg(feature = "test-seams")]
        crate::vector_read_memory::statement_child_proof::funding_next(
            payload.original_payer_for_test(),
        );
        ReadShared::try_from_prepaid(payload, keeper)
    }
    pub(super) fn from_prepaid(value: T, invoice: ReadShared<ReadCredit>) -> Self {
        #[cfg(feature = "test-seams")]
        crate::vector_read_memory::statement_child_proof::funding_next(
            invoice.original_payer_for_test(),
        );
        Self {
            value: Arc::new(value),
            invoice,
        }
    }
    pub(super) fn original_invoice(&self) -> ReadShared<ReadCredit> {
        self.invoice.clone()
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn try_new(
        owner: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        value: T,
    ) -> Result<Self> {
        let owner = owner.into();

        let invoice = Self::admit(owner)?;
        let value = Arc::new(value);
        Ok(Self { value, invoice })
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn try_new_cyclic(
        owner: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        make: impl FnOnce(&ControlWeak<T>) -> T,
    ) -> Result<Self> {
        let owner = owner.into();

        let invoice = Self::admit(owner)?;
        Ok(Self::cyclic_from_prepaid(invoice, make))
    }
    pub(super) fn cyclic_from_prepaid(
        invoice: ReadShared<ReadCredit>,
        make: impl FnOnce(&ControlWeak<T>) -> T,
    ) -> Self {
        // The local original keeper survives std's unpublished-Arc cleanup
        // on constructor unwind. Each escaping weak retains that same keeper.
        let value = Arc::new_cyclic(|weak| {
            make(&ControlWeak {
                value: weak.clone(),
                invoice: invoice.clone(),
            })
        });
        Self { value, invoice }
    }
    pub(super) fn downgrade(this: &Self) -> ControlWeak<T> {
        ControlWeak {
            value: Arc::downgrade(&this.value),
            invoice: this.invoice.clone(),
        }
    }
}
impl<T: std::fmt::Debug> std::fmt::Debug for ControlArc<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.value, f)
    }
}
impl<T> Clone for ControlArc<T> {
    fn clone(&self) -> Self {
        Self {
            value: self.value.clone(),
            invoice: self.invoice.clone(),
        }
    }
}
impl<T> Clone for ControlWeak<T> {
    fn clone(&self) -> Self {
        Self {
            value: self.value.clone(),
            invoice: self.invoice.clone(),
        }
    }
}
impl<T> ControlWeak<T> {
    pub(super) fn upgrade(&self) -> Option<ControlArc<T>> {
        self.value.upgrade().map(|value| ControlArc {
            value,
            invoice: self.invoice.clone(),
        })
    }
    pub(super) fn ptr_eq(left: &Self, right: &Self) -> bool {
        std::sync::Weak::ptr_eq(&left.value, &right.value)
    }
}
impl<T> std::ops::Deref for ControlArc<T> {
    type Target = Arc<T>;
    fn deref(&self) -> &Arc<T> {
        &self.value
    }
}

/// This private keeper outlives every scoped decoder and its accountant.
/// No default declaration or new read payer is acquired: payload admissions
/// still go to the exact executing reader. The accountant is codec bookkeeping.
pub(super) struct QueryVectorDecode {
    memory: Funded<crate::read_image_memory::ImageMemoryHandle>,
}
impl QueryVectorDecode {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn accountant_bytes() -> Result<usize> {
        crate::memory_accounting::AccountantHandle::reader_control_bytes()
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn fixed_bytes() -> Result<usize> {
        Self::accountant_bytes()?
            .checked_add(ImageMemory::admitted_control_bytes())
            .ok_or_else(|| Error::Other("request decode control overflow".into()))
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn from_prepaid(
        owner: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
        mut credit: ReadCredit,
    ) -> Result<Self> {
        let owner = owner.into();
        let accountant_credit = credit.try_split_off(Self::accountant_bytes()?)?;
        let accountant = crate::memory_accounting::AccountantHandle::reader_from_prepaid(
            None,
            None,
            accountant_credit,
        )?;
        let memory = ImageMemory::query_vector_per_call_prepaid(accountant, owner, credit)?;
        Ok(Self { memory })
    }
    pub(super) fn enter(&self) -> QueryDecodeScope<'_> {
        QueryDecodeScope {
            scope: Some(self.memory.value.enter()),
            decoder: self,
        }
    }
}
pub(super) struct QueryDecodeScope<'a> {
    scope: Option<ImageMemoryScope>,
    decoder: &'a QueryVectorDecode,
}
impl Drop for QueryDecodeScope<'_> {
    fn drop(&mut self) {
        // Clear TLS/JSON aliases before freeing the per-call child and invoice.
        drop(self.scope.take());
        self.decoder.memory.value.release_query_vector_call();
    }
}

/// The existing Native child, adapted only for the Arc-based kernel. This
/// forwarding allocation never acquires an owner, token, deadline or counter.
pub(super) struct NativeKernelBudget {
    owner: contextdb_core::read_memory::ReadMemoryOwner,
    invoice: ReadShared<ReadCredit>,
}
impl std::fmt::Debug for NativeKernelBudget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("NativeKernelBudget")
    }
}
impl ReadMemoryBudget for NativeKernelBudget {
    #[cfg(feature = "test-seams")]
    fn funding_owner_handle_for_test(
        &self,
    ) -> Option<&contextdb_core::read_memory::ReadMemoryOwner> {
        Some(&self.owner)
    }
    fn control_invoice(&self) -> Option<ReadShared<ReadCredit>> {
        Some(self.invoice.clone())
    }
    fn try_reserve(
        &self,
        bytes: usize,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> Result<()> {
        self.owner.try_reserve(bytes, subsystem, operation, hint)
    }
    fn try_reserve_native(
        &self,
        bytes: usize,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> std::result::Result<(), contextdb_core::read_memory::NativeRefusal> {
        self.owner
            .try_reserve_native(bytes, subsystem, operation, hint)
    }
    fn release(&self, bytes: usize) {
        self.owner.release(bytes)
    }
}
#[cfg(any(test, feature = "test-seams"))]
impl crate::vector_read_memory::NativeOriginalPinSource for NativeKernelBudget {
    fn original_native_owner(&self) -> contextdb_core::read_memory::ReadMemoryOwner {
        self.owner.clone()
    }
}
#[cfg(any(test, feature = "test-seams"))]
impl Drop for NativeKernelBudget {
    fn drop(&mut self) {
        // Remove the registry's weak tail before std frees this Arc, while
        // both original invoice aliases still exist. No invoice is retained
        // by the registry, and no registry/native-owner cycle is possible.
        crate::vector_read_memory::unregister_native_forwarder(self.owner.accounting_identity());
    }
}
impl NativeKernelBudget {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn try_from_operation(
        operation: &crate::owner_read::NativeReadRequest,
    ) -> Result<ControlArc<Self>> {
        Self::try_from_owner(operation.owner())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn try_from_child(
        child: crate::vector_read_memory::NativeReadChildReceipt,
    ) -> Result<ControlArc<Self>> {
        Self::try_from_owner(child.into_owner())
    }

    // Only the native request and sealed native-child doors above reach this
    // constructor; arbitrary legacy providers cannot acquire native provenance.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn try_from_owner(
        owner: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
    ) -> Result<ControlArc<Self>> {
        let owner = owner.into();

        let keeper = ReadCredit::try_new_owned(
            owner.clone(),
            ReadShared::<ReadCredit>::layout().size(),
            "bounded_read",
            "native_adapter_keeper",
            "Release retained read controls or raise MEMORY_LIMIT.",
        )?;
        let payload = ReadCredit::try_new_owned(
            owner.clone(),
            ControlArc::<Self>::arc_bytes()?,
            "bounded_read",
            "native_adapter",
            "Release retained read controls or raise MEMORY_LIMIT.",
        )?;
        #[cfg(any(test, feature = "test-seams"))]
        {
            #[cfg(feature = "test-seams")]
            let payers = (
                Some(keeper.original_payer_identity_for_test()),
                Some(payload.original_payer_identity_for_test()),
            );
            #[cfg(not(feature = "test-seams"))]
            let payers = (None, None);
            crate::vector_read_memory::statement_child_proof::native_control_credits(
                owner.accounting_identity(),
                keeper.bytes(),
                payload.bytes(),
                payers.0,
                payers.1,
                std::alloc::Layout::new::<Self>(),
            );
        }
        #[cfg(any(test, feature = "test-seams"))]
        let keeper_request_before =
            crate::vector_read_memory::statement_child_proof::constructor_ordinal();
        #[cfg(feature = "test-seams")]
        crate::vector_read_memory::statement_child_proof::funding_next(
            payload.original_payer_for_test(),
        );
        let invoice = ReadShared::try_from_prepaid(payload, keeper)?;
        #[cfg(all(test, feature = "test-seams"))]
        crate::database::ordinary_native_owner_tests::keeper_allocated(
            owner.accounting_identity(),
            &*invoice as *const ReadCredit as usize,
            invoice.original_payer_identity_for_test(),
            invoice.bytes(),
            ReadShared::<ReadCredit>::layout(),
        );
        #[cfg(any(test, feature = "test-seams"))]
        crate::vector_read_memory::statement_child_proof::native_control_allocated(
            owner.accounting_identity(),
            true,
            crate::vector_read_memory::statement_child_proof::control_allocation(
                &*invoice as *const ReadCredit as usize,
                ReadShared::<ReadCredit>::layout(),
                keeper_request_before,
            ),
            owner.accounting_identity(),
            invoice.bytes(),
            0,
            0,
        );
        #[cfg(any(test, feature = "test-seams"))]
        let adapter_request_before =
            crate::vector_read_memory::statement_child_proof::constructor_ordinal();
        // The first retained owner immediately pairs the original invoice;
        // no fallible action occurs between publication and that pairing.
        let control = ControlArc::from_prepaid(
            Self {
                owner,
                invoice: invoice.clone(),
            },
            invoice,
        );
        #[cfg(all(test, feature = "test-seams"))]
        crate::database::ordinary_native_owner_tests::forwarding_allocated(
            control.value.owner.accounting_identity(),
            Arc::as_ptr(&control.value) as usize,
            &*control.invoice as *const ReadCredit as usize,
            &*control.value.invoice as *const ReadCredit as usize,
            ControlArc::<Self>::arc_layout()?,
        );
        #[cfg(any(test, feature = "test-seams"))]
        crate::vector_read_memory::statement_child_proof::native_control_allocated(
            control.value.owner.accounting_identity(),
            false,
            crate::vector_read_memory::statement_child_proof::control_allocation(
                Arc::as_ptr(&control.value) as usize,
                ControlArc::<Self>::arc_layout()?,
                adapter_request_before,
            ),
            control.value.owner.accounting_identity(),
            control.invoice.bytes(),
            &*control.invoice as *const ReadCredit as usize,
            &*control.value.invoice as *const ReadCredit as usize,
        );
        #[cfg(any(test, feature = "test-seams"))]
        crate::vector_read_memory::register_native_forwarder(
            control.value.owner.accounting_identity(),
            &control.value,
        );
        Ok(control)
    }
}
