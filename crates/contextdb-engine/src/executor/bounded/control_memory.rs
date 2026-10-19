//! Original fixed invoices outside the std Arc allocations they price.
use super::*;
use contextdb_core::read_memory::ReadShared;
use crate::local_transport::query_funding::Funded;
use crate::read_image_memory::{ImageMemory, ImageMemoryScope};

/// Field order matters on the final strong AND the final weak: std destroys
/// its allocation first, then this alias releases the shared original invoice.
/// There is no raw Arc/Weak export; Deref is private to this execution module.
pub(super) struct ControlArc<T> {
    value: Arc<T>,
    invoice: ReadShared<ReadCredit>,
}
pub(super) struct ControlWeak<T> {
    value: std::sync::Weak<T>,
    invoice: ReadShared<ReadCredit>,
}
impl<T> ControlArc<T> {
    fn arc_bytes() -> Result<usize> {
        // std Arc's repr(C) header is two atomic usize counts, followed by T.
        // Include alignment padding, including over-aligned request payloads.
        std::alloc::Layout::new::<[std::sync::atomic::AtomicUsize; 2]>()
            .extend(std::alloc::Layout::new::<T>())
            .map(|(layout, _)| layout.pad_to_align().size())
            .map_err(|_| Error::Other("request control layout overflow".into()))
    }
    pub(super) fn control_bytes() -> Result<usize> {
        Self::arc_bytes()?.checked_add(ReadShared::<ReadCredit>::layout().size())
            .ok_or_else(|| Error::Other("request control layout overflow".into()))
    }
    pub(super) fn control_bytes_for(_: &T) -> Result<usize> { Self::control_bytes() }
    pub(super) fn admit(owner: Arc<dyn ReadMemoryBudget>) -> Result<ReadShared<ReadCredit>> {
        Self::admit_owned(contextdb_core::read_memory::ReadMemoryOwner::from_legacy(owner))
    }
    pub(super) fn admit_owned(owner: contextdb_core::read_memory::ReadMemoryOwner)
        -> Result<ReadShared<ReadCredit>> {
        let keeper = ReadCredit::try_new_owned(owner.clone(),
            ReadShared::<ReadCredit>::layout().size(), "bounded_read", "control_keeper",
            "Release retained read controls or raise MEMORY_LIMIT.")?;
        let payload = ReadCredit::try_new_owned(owner, Self::arc_bytes()?,
            "bounded_read", "request_control",
            "Release retained read controls or raise MEMORY_LIMIT.")?;
        ReadShared::try_from_prepaid(payload, keeper)
    }
    pub(super) fn from_prepaid(value: T, invoice: ReadShared<ReadCredit>) -> Self {
        Self { value: Arc::new(value), invoice }
    }
    pub(super) fn original_invoice(&self) -> ReadShared<ReadCredit> { self.invoice.clone() }
    pub(super) fn try_new(owner: Arc<dyn ReadMemoryBudget>, value: T) -> Result<Self> {
        let invoice = Self::admit(owner)?;
        let value = Arc::new(value);
        Ok(Self { value, invoice })
    }
    pub(super) fn try_new_cyclic(
        owner: Arc<dyn ReadMemoryBudget>,
        make: impl FnOnce(&ControlWeak<T>) -> T,
    ) -> Result<Self> {
        let invoice = Self::admit(owner)?;
        Ok(Self::cyclic_from_prepaid(invoice, make))
    }
    pub(super) fn cyclic_from_prepaid(invoice: ReadShared<ReadCredit>,
        make: impl FnOnce(&ControlWeak<T>) -> T) -> Self {
        // The local original keeper survives std's unpublished-Arc cleanup
        // on constructor unwind. Each escaping weak retains that same keeper.
        let value = Arc::new_cyclic(|weak| make(&ControlWeak {
            value: weak.clone(), invoice: invoice.clone(),
        }));
        Self { value, invoice }
    }
    pub(super) fn downgrade(this: &Self) -> ControlWeak<T> {
        ControlWeak { value: Arc::downgrade(&this.value), invoice: this.invoice.clone() }
    }
}
impl<T> Clone for ControlArc<T> {
    fn clone(&self) -> Self {
        Self { value: self.value.clone(), invoice: self.invoice.clone() }
    }
}
impl<T> Clone for ControlWeak<T> {
    fn clone(&self) -> Self {
        Self { value: self.value.clone(), invoice: self.invoice.clone() }
    }
}
impl<T> ControlWeak<T> {
    pub(super) fn upgrade(&self) -> Option<ControlArc<T>> {
        self.value.upgrade().map(|value| ControlArc { value, invoice: self.invoice.clone() })
    }
    pub(super) fn ptr_eq(left: &Self, right: &Self) -> bool {
        std::sync::Weak::ptr_eq(&left.value, &right.value)
    }
}
impl<T> std::ops::Deref for ControlArc<T> {
    type Target = Arc<T>;
    fn deref(&self) -> &Arc<T> { &self.value }
}

/// This private keeper outlives every scoped decoder and its accountant.
/// No default declaration or new read payer is acquired: payload admissions
/// still go to the exact executing reader. The accountant is codec bookkeeping.
pub(super) struct QueryVectorDecode {
    memory: Funded<Arc<ImageMemory>>,
    _accountant: Funded<Arc<crate::memory_accounting::MemoryAccountant>>,
}
impl QueryVectorDecode {
    fn accountant_bytes() -> usize {
         std::mem::size_of::<crate::memory_accounting::MemoryAccountant>()
            + std::mem::size_of::<std::sync::atomic::AtomicUsize>()
            + std::mem::size_of::<crate::memory_accounting::WriterCounter>()
            + 6 * std::mem::size_of::<usize>()
    }
    pub(super) fn fixed_bytes() -> Result<usize> {
        Self::accountant_bytes().checked_add(ImageMemory::admitted_control_bytes())
            .ok_or_else(|| Error::Other("request decode control overflow".into()))
    }
    pub(super) fn from_prepaid(owner: Arc<dyn ReadMemoryBudget>, mut credit: ReadCredit)
        -> Result<Self>
    {
        let accountant_credit = credit.try_split_off(Self::accountant_bytes())?;
        let accountant = Funded {
            value: Arc::new(crate::memory_accounting::MemoryAccountant::no_limit()),
            credit: Some(accountant_credit),
        };
        let memory = ImageMemory::query_vector_per_call_prepaid(
            accountant.value.clone().into(), owner, credit)?;
        Ok(Self { memory, _accountant: accountant })
    }
    pub(super) fn enter(&self) -> QueryDecodeScope<'_> {
        QueryDecodeScope { scope: Some(self.memory.value.enter()), decoder: self }
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
    fn control_invoice(&self) -> Option<ReadShared<ReadCredit>> { Some(self.invoice.clone()) }
    fn try_reserve(&self, bytes: usize, subsystem: &'static str,
        operation: &'static str, hint: &'static str) -> Result<()> {
        self.owner.try_reserve(bytes, subsystem, operation, hint)
    }
    fn try_reserve_native(&self, bytes: usize, subsystem: &'static str,
        operation: &'static str, hint: &'static str)
        -> std::result::Result<(), contextdb_core::read_memory::NativeRefusal> {
        self.owner.try_reserve_native(bytes, subsystem, operation, hint)
    }
    fn release(&self, bytes: usize) { self.owner.release(bytes) }
}
impl NativeKernelBudget {
    pub(super) fn try_from_operation(operation: &crate::owner_read::NativeReadRequest)
        -> Result<ControlArc<Self>> {
        let owner = operation.owner();
        let keeper = ReadCredit::try_new_owned(owner.clone(),
            ReadShared::<ReadCredit>::layout().size(), "bounded_read", "native_adapter_keeper",
            "Release retained read controls or raise MEMORY_LIMIT.")?;
        let payload = ReadCredit::try_new_owned(owner.clone(), ControlArc::<Self>::arc_bytes()?,
            "bounded_read", "native_adapter",
            "Release retained read controls or raise MEMORY_LIMIT.")?;
        let invoice = ReadShared::try_from_prepaid(payload, keeper)?;
        // The first retained owner immediately pairs the original invoice;
        // no fallible action occurs between publication and that pairing.
        Ok(ControlArc::from_prepaid(Self { owner, invoice: invoice.clone() }, invoice))
    }
}
