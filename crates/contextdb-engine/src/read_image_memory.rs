//! Admission precedes deserialization and every owned image copy. One lease
//! follows the image through sessions and cursors after persistence is released.
use crate::memory_accounting::MemoryAccountant;
use contextdb_core::{Error, Result};
use serde::de::{self, DeserializeSeed, Visitor};
use std::cell::RefCell;
use std::marker::PhantomData;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Debug)]
pub(crate) struct ImageMemory {
    pub(crate) accountant: crate::memory_accounting::AccountantHandle,
    subsystem: &'static str,
    operation: &'static str,
    hint: &'static str,
    reader_budget: Option<Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>>,
    _reader_control_invoice: Option<contextdb_core::read_memory::ReadShared<contextdb_core::read_memory::ReadCredit>>,
    held: AtomicUsize,
    control_bytes: usize,
    /// The pending refusal: an owned error, or (on an admitted reader) a
    /// refusal kept by value, with no owned error text, until it is taken.
    failure: parking_lot::Mutex<Option<Pending>>,
    /// Present on an owner whose every top-level decode call is charged to
    /// its own writer owner: the slot holds the latest call's owner until the
    /// next call begins or this owner drops.
    per_call: Option<parking_lot::Mutex<Option<Arc<ImageMemory>>>>,
    // Each original child invoice is outside the child Arc it prices.
    query_per_call: Option<parking_lot::Mutex<Option<
        crate::local_transport::query_funding::Funded<Arc<ImageMemory>>>>>,
    /// Decoded outputs a caller already admitted elsewhere -- an import's
    /// workspace for the vectors it keeps. An output admission under this
    /// owner, or a per-call owner it creates, adopts that credit instead of
    /// charging the same bytes a second time.
    prepaid_output: Option<Arc<PrepaidOutput>>,
    // Last custody field pins the original writer epoch through image refund.
    _writer_custody: Option<crate::memory_accounting::StoreAccountantHandle>,
}
#[derive(Debug)]
struct PrepaidOutput {
    remaining: AtomicUsize,
    adopted: AtomicUsize,
}

#[derive(Clone, Copy)]
struct CandidateDecodeCredit {
    reader: usize,
    remaining: usize,
    decoding: bool,
}
thread_local! {
    static CANDIDATE_DECODE: std::cell::Cell<Option<CandidateDecodeCredit>> =
        const { std::cell::Cell::new(None) };
}
struct CandidateDecodeRestore(Option<CandidateDecodeCredit>);
struct CandidateCodecRestore(bool);
impl Drop for CandidateCodecRestore {
    fn drop(&mut self) { CANDIDATE_DECODE.with(|slot| {
        if let Some(mut credit) = slot.get() { credit.decoding = self.0; slot.set(Some(credit)); }
    }); }
}
impl Drop for CandidateDecodeRestore {
    fn drop(&mut self) { CANDIDATE_DECODE.with(|slot| slot.set(self.0)); }
}

/// The caller has already admitted this window and retains its invoice through
/// destruction of the loaded body AND its scoring representation. Only the
/// vector codec, under the exact same reader adapter, can consume this bounded
/// part of the window. Metadata/proof decodes outside that codec do not adopt it.
pub(crate) fn with_candidate_decode_credit<T>(bytes: usize, body: impl FnOnce() -> T) -> T {
    let reader = current_reader_budget().map(|reader| Arc::as_ptr(&reader) as *const () as usize);
    let _restore = CandidateDecodeRestore(CANDIDATE_DECODE.with(|slot| slot.replace(
        reader.map(|reader| CandidateDecodeCredit { reader, remaining: bytes, decoding: false }))));
    body()
}
fn candidate_reader_matches(owner: &ImageMemory, credit: CandidateDecodeCredit) -> bool {
    owner.reader_budget.as_ref().is_some_and(|reader|
        Arc::as_ptr(reader) as *const () as usize == credit.reader)
}
fn adopt_candidate_decode(owner: &ImageMemory, bytes: usize) -> bool {
    CANDIDATE_DECODE.with(|slot| {
        let Some(mut credit) = slot.get() else { return false; };
        if !credit.decoding || !candidate_reader_matches(owner, credit) { return false; }
        let Some(remaining) = credit.remaining.checked_sub(bytes) else { return false; };
        credit.remaining = remaining;
        slot.set(Some(credit));
        true
    })
}

impl ImageMemory {
    /// Pin output provenance before constructing an escaping container.
    pub(crate) fn preparation_original(
        &self,
    ) -> Result<crate::memory_accounting::OwnedMemoryReservation> {
        self._writer_custody
            .as_ref()
            .ok_or_else(|| Error::Other("posting outputs require a pinned writer image".into()))?
            .preparation_original()
    }
    pub(crate) fn new(limit: Option<usize>) -> Arc<Self> {
        Arc::new(Self {
            accountant: Arc::new(match limit {
                Some(limit) => MemoryAccountant::with_budget(limit),
                None => MemoryAccountant::no_limit(),
            })
            .into(),
            subsystem: "direct_read",
            operation: "hydrate_owned_image",
            hint: "Raise MEMORY_LIMIT to admit the complete committed read image.",
            reader_budget: None,
            _reader_control_invoice: None,
            held: AtomicUsize::new(0),
            control_bytes: 0,
            failure: parking_lot::Mutex::new(None),
            per_call: None, query_per_call: None,
            prepaid_output: None,
            _writer_custody: None,
        })
    }
    /// Admission into an accountant that already exists -- a reading
    /// session's own -- so what is decoded under [`Self::scope`] is charged
    /// where the session's limit sees it, allocation by allocation, and given
    /// back when this charge is dropped.
    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn charging(
        accountant: impl Into<crate::memory_accounting::AccountantHandle>,
    ) -> Arc<Self> {
        Self::charging_for(
            accountant.into(),
            "direct_read",
            "hydrate_owned_image",
            "Raise MEMORY_LIMIT to admit the complete committed read image.",
        )
    }
    pub(crate) fn charging_for(
        accountant: crate::memory_accounting::AccountantHandle,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> Arc<Self> {
        let writer_custody =
            crate::memory_accounting::StoreAccountantHandle::retain_if_writer(&accountant);
        Arc::new(Self {
            accountant,
            subsystem,
            operation,
            hint,
            reader_budget: None,
            _reader_control_invoice: None,
            held: AtomicUsize::new(0),
            control_bytes: 0,
            failure: parking_lot::Mutex::new(None),
            per_call: None, query_per_call: None,
            prepaid_output: None,
            _writer_custody: writer_custody,
        })
    }
    /// A durable source window belongs to the requesting read when present.
    /// Internal writer operations continue to own their scratch directly.
    pub(crate) fn charging_reader(
        accountant: crate::memory_accounting::AccountantHandle,
        reader_budget: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Arc<Self> {
        Arc::new(Self {
            accountant,
            subsystem: "reader",
            operation: "materialize_answer",
            hint: "Reduce the statement's output or raise the declared reader memory limit.",
            _reader_control_invoice: reader_budget.control_invoice(),
            reader_budget: Some(reader_budget),
            held: AtomicUsize::new(0),
            control_bytes: 0,
            failure: parking_lot::Mutex::new(None),
            per_call: None, query_per_call: None,
            prepaid_output: None,
            _writer_custody: None,
        })
    }

    /// Explicit reader funding with the source operation's existing labels.
    /// The accountant is retained for image bridge identity only; funding
    /// always uses the required reader capability.
    pub(crate) fn charging_reader_for(
        accountant: crate::memory_accounting::AccountantHandle,
        reader_budget: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> Arc<Self> {
        Arc::new(Self {
            accountant,
            subsystem,
            operation,
            hint,
            _reader_control_invoice: reader_budget.control_invoice(),
            reader_budget: Some(reader_budget),
            held: AtomicUsize::new(0),
            control_bytes: 0,
            failure: parking_lot::Mutex::new(None),
            per_call: None, query_per_call: None,
            prepaid_output: None,
            _writer_custody: None,
        })
    }

    /// A request's reader owner for work that decodes stored values one call
    /// at a time and keeps none of them past the call -- a vector search
    /// scoring candidates whose bodies it drops after scoring. Each top-level
    /// decode call under it gets a fresh owner charged to the same declared
    /// reader, and the previous call's charge is returned when the next call
    /// begins or this owner drops, so a streamed scan holds one value's
    /// decode at a time instead of every value it has already freed.
    pub(crate) fn charging_reader_per_call(
        accountant: crate::memory_accounting::AccountantHandle,
        reader_budget: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Arc<Self> {
        let mut owner = Self::charging_reader(accountant, reader_budget);
        Arc::get_mut(&mut owner)
            .expect("new reader owner is exclusive")
            .per_call = Some(parking_lot::Mutex::new(None));
        owner
    }

    /// The executing kernel retains this original fixed invoice externally.
    /// The private slot owns the original per-call child invoice until its
    /// scope has cleared all TLS aliases and the actual child Arc is freed.
    pub(crate) fn query_vector_per_call_prepaid(
        accountant: crate::memory_accounting::AccountantHandle,
        reader_budget: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        credit: contextdb_core::read_memory::ReadCredit,
    ) -> Result<crate::local_transport::query_funding::Funded<Arc<Self>>> {
        if credit.bytes() != Self::admitted_control_bytes() {
            return Err(Error::Other("query decoder control layout differs from invoice".into()));
        }
        let value = Arc::new(Self {
            accountant, subsystem: "reader", operation: "materialize_answer",
            hint: "Reduce simultaneous query memory or raise the declared reader limit.",
            _reader_control_invoice: reader_budget.control_invoice(),
            reader_budget: Some(reader_budget), held: AtomicUsize::new(0),
            control_bytes: ADMITTED_TAG, failure: parking_lot::Mutex::new(None),
            per_call: None, query_per_call: Some(parking_lot::Mutex::new(None)),
            prepaid_output: None, _writer_custody: None,
        });
        Ok(crate::local_transport::query_funding::Funded { value, credit: Some(credit) })
    }
    pub(crate) fn release_query_vector_call(&self) {
        if let Some(slot) = &self.query_per_call {
            drop(slot.lock().take());
        }
    }

    /// Candidate codec calls retain the already-funded query owner in place.
    /// The loader's original window covers all adopted buffers until scoring
    /// destroys them; ordinary calls keep their existing per-call owners.
    pub(crate) fn with_vector_body_decode<T>(body: impl FnOnce() -> Result<T>) -> Result<T> {
        let Ok(owner) = active_owner() else {
            let _scope = Self::enter_decode_call()?;
            return body();
        };
        let candidate = CANDIDATE_DECODE.with(|slot| slot.get())
            .filter(|credit| candidate_reader_matches(&owner, *credit));
        if let Some(mut credit) = candidate {
            let _restore = CandidateCodecRestore(credit.decoding);
            credit.decoding = true;
            CANDIDATE_DECODE.with(|slot| slot.set(Some(credit)));
            // Keep the exact executing owner active, with no redundant child.
            let _scope = owner.enter();
            body()
        } else {
            let _scope = Self::enter_decode_call()?;
            body()
        }
    }

    /// Enter this owner for one top-level decode call. A per-call owner
    /// returns its previous call's charge and admits the call under a fresh
    /// owner with the same funder and labels; any other owner is entered
    /// as is.
    pub(crate) fn enter_call(self: &Arc<Self>) -> Result<ImageMemoryScope> {
        if CANDIDATE_DECODE.with(|slot| slot.get()).is_some_and(|credit|
            credit.decoding && candidate_reader_matches(self, credit)) {
            return Ok(self.enter());
        }
        if let Some(slot) = &self.query_per_call {
            let mut slot = slot.lock();
            drop(slot.take());
            let call = Self::query_reader_for(self.accountant.clone(),
                self.reader_budget.as_ref().expect("query decoder retains its reader").clone())?;
            // Enter before publication while the local keeper owns the original
            // credit. Assignment then moves that keeper without refund/readmit.
            let scope = call.value.enter();
            *slot = Some(call);
            return Ok(scope);
        }
        let Some(slot) = &self.per_call else {
            return Ok(self.enter());
        };
        let mut slot = slot.lock();
        // The previous call returned before this one began; its stored body
        // and codec growth are already dropped.
        drop(slot.take());
        let call = match &self.reader_budget {
            Some(reader) => Self::charging_reader_for(
                self.accountant.clone(),
                reader.clone(),
                self.subsystem,
                self.operation,
                self.hint,
            ),
            None => {
                let mut call = Self::operation_writer(self.accountant.clone())?;
                Arc::get_mut(&mut call)
                    .expect("new writer owner is exclusive")
                    .prepaid_output = self.prepaid_output.clone();
                call
            }
        };
        *slot = Some(call.clone());
        Ok(call.enter())
    }

    /// The admitted form of [`Self::charging_reader_for`]: the shared
    /// owner's own control is admitted to the reader before the owner is
    /// allocated, and the final holder releases it from Drop. A refusal is
    /// kept by value until the caller takes it, and a malformed input answers
    /// the bounded static class text [`ADMITTED_CODEC_ERROR`]. The control is
    /// refunded in Drop just before the shared allocation is freed: a fixed
    /// residue of `size_of::<ImageMemory>() + 2 * size_of::<usize>()` bytes
    /// for that instant, freed rather than retained.
    pub(crate) fn admitted_reader_for(
        accountant: crate::memory_accounting::AccountantHandle,
        reader_budget: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> Result<Arc<Self>> {
        let control = Self::admitted_control_bytes();
        reader_budget.try_reserve(control, subsystem, operation, hint)?;
        Ok(Arc::new(Self {
            accountant,
            subsystem,
            operation,
            hint,
            _reader_control_invoice: reader_budget.control_invoice(),
            reader_budget: Some(reader_budget),
            held: AtomicUsize::new(control),
            control_bytes: control | ADMITTED_TAG,
            failure: parking_lot::Mutex::new(None),
            per_call: None, query_per_call: None,
            prepaid_output: None,
            _writer_custody: None,
        }))
    }
    /// Private query supplier: the caller retains this original invoice
    /// OUTSIDE the Arc until every decoder/answer alias has been destroyed or
    /// handed to its real public consumer. No fixed credit lives in the Arc.
    pub(crate) fn query_reader_for(
        accountant: crate::memory_accounting::AccountantHandle,
        reader_budget: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<crate::local_transport::query_funding::Funded<Arc<Self>>> {
        crate::local_transport::query_funding::Funded::allocate(
            Some(reader_budget.clone()), Self::admitted_control_bytes(), || Arc::new(Self {
                accountant,
                subsystem: "reader", operation: "materialize_answer",
                hint: "Reduce simultaneous query memory or raise the declared reader limit.",
                _reader_control_invoice: reader_budget.control_invoice(),
            reader_budget: Some(reader_budget), held: AtomicUsize::new(0),
                control_bytes: ADMITTED_TAG,
                failure: parking_lot::Mutex::new(None), per_call: None, query_per_call: None,
                prepaid_output: None, _writer_custody: None,
            }),
        )
    }

    /// The shared allocation an admitted reader admits before it exists.
    pub(crate) fn admitted_control_bytes() -> usize {
        std::mem::size_of::<Self>() + 2 * std::mem::size_of::<usize>()
    }

    pub(crate) fn reader_budget(
        &self,
    ) -> Option<Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>> {
        self.reader_budget.clone()
    }

    /// A fallible operation bridge funded by the already declared read child.
    /// Its own Arc control is admitted before creating the scope owner.
    pub(crate) fn operation_reader(
        accountant: crate::memory_accounting::AccountantHandle,
        reader_budget: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<Arc<Self>> {
        let control = std::mem::size_of::<Self>() + 2 * std::mem::size_of::<usize>();
        reader_budget.try_reserve(
            control,
            "reader",
            "decode_owner",
            "Reduce simultaneous reads or raise the declared reader memory limit.",
        )?;
        Ok(Arc::new(Self {
            accountant,
            subsystem: "reader",
            operation: "materialize_answer",
            hint: "Reduce the statement's output or raise the declared reader memory limit.",
            _reader_control_invoice: reader_budget.control_invoice(),
            reader_budget: Some(reader_budget),
            held: AtomicUsize::new(control),
            control_bytes: control,
            failure: parking_lot::Mutex::new(None),
            per_call: None, query_per_call: None,
            prepaid_output: None,
            _writer_custody: None,
        }))
    }

    /// Actual write-side decoding borrows the existing writer accountant;
    /// the operation owner holds and releases only its own admitted payloads.
    pub(crate) fn operation_writer(
        accountant: crate::memory_accounting::AccountantHandle,
    ) -> Result<Arc<Self>> {
        let control = std::mem::size_of::<Self>() + 2 * std::mem::size_of::<usize>();
        // Fixed decode control is named open residue; growing payloads below
        // still use this writer's declared working-memory limit.
        crate::memory_accounting::admit_open_residue(control, "writer_decode_open_residue")?;
        let writer_custody =
            crate::memory_accounting::StoreAccountantHandle::retain_if_writer(&accountant);
        Ok(Arc::new(Self {
            accountant,
            subsystem: "writer_source",
            operation: "decode_stored_value",
            hint: "Raise MEMORY_LIMIT to decode this writer operation's stored values.",
            reader_budget: None,
            _reader_control_invoice: None,
            held: AtomicUsize::new(0),
            control_bytes: control | crate::memory_accounting::OPEN_RESIDUE_TAG,
            failure: parking_lot::Mutex::new(None),
            per_call: None, query_per_call: None,
            prepaid_output: None,
            _writer_custody: writer_custody,
        }))
    }

    /// A writer owner for a loop that decodes one stored value per call and
    /// whose retained results are already paid for by the caller's own
    /// workspace. Each top-level decode call under it gets a fresh writer
    /// owner, and the previous call's owner -- the stored body it decoded and
    /// dropped, its codec growth and its copy -- is returned when the next
    /// call begins. Its charges are therefore live for exactly one value at a
    /// time instead of accumulating across values that are already freed.
    pub(crate) fn operation_writer_per_call(
        accountant: crate::memory_accounting::AccountantHandle,
        prepaid_output_bytes: usize,
    ) -> Result<Arc<Self>> {
        let mut owner = Self::operation_writer(accountant)?;
        let owner_mut = Arc::get_mut(&mut owner).expect("new writer owner is exclusive");
        owner_mut.per_call = Some(parking_lot::Mutex::new(None));
        owner_mut.prepaid_output = Some(Arc::new(PrepaidOutput {
            remaining: AtomicUsize::new(prepaid_output_bytes),
            adopted: AtomicUsize::new(0),
        }));
        Ok(owner)
    }

    /// Prepaid output bytes this owner's per-call decodes adopted.
    pub(crate) fn adopted_output(&self) -> usize {
        self.prepaid_output
            .as_ref()
            .map_or(0, |prepaid| prepaid.adopted.load(Ordering::SeqCst))
    }

    /// Adopt `bytes` of a caller's prepaid output credit, if it has that much.
    fn adopt_prepaid_output(&self, bytes: usize) -> bool {
        self.prepaid_output.as_ref().is_some_and(|prepaid| {
            let adopted = prepaid
                .remaining
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(bytes)
                })
                .is_ok();
            if adopted {
                prepaid.adopted.fetch_add(bytes, Ordering::SeqCst);
            }
            adopted
        })
    }

    /// `subsystem/operation` this owner's admissions and refusals carry.
    pub(crate) fn labels(&self) -> (&'static str, &'static str) {
        (self.subsystem, self.operation)
    }

    /// Bytes held by this owner, including its current per-call owner.
    pub(crate) fn held_with_call(&self) -> usize {
        self.held().saturating_add(
            self.per_call
                .as_ref()
                .map_or(0, |slot| slot.lock().as_ref().map_or(0, |call| call.held())),
        ).saturating_add(self.query_per_call.as_ref()
            .map_or(0, |slot| slot.lock().as_ref().map_or(0, |call| call.value.held())))
    }

    pub(crate) fn is_per_call(&self) -> bool {
        self.per_call.is_some() || self.query_per_call.is_some()
    }

    /// A standalone call uses the engine's default open ceiling and keeps
    /// its accountant private, outside the registered Database writer domain.
    pub(crate) fn default_decode_accountant() -> crate::memory_accounting::AccountantHandle {
        let limit = crate::read_session::DatabaseOpenOptions::default().memory_limit;
        Arc::new(match limit {
            Some(bytes) => MemoryAccountant::with_budget(bytes),
            None => MemoryAccountant::no_limit(),
        })
        .into()
    }

    /// Reuse an operation's owner; otherwise admit a fresh call-local owner.
    pub(crate) fn enter_decode_call() -> Result<ImageMemoryScope> {
        if let Ok(owner) = active_owner() {
            return owner.enter_call();
        }
        #[cfg(any(test, feature = "test-seams"))]
        crate::vector_read_memory::note_unowned_decode_for_test();
        Self::enter_writer_operation(Self::default_decode_accountant())
    }

    pub(crate) fn enter_writer_operation(
        accountant: crate::memory_accounting::AccountantHandle,
    ) -> Result<ImageMemoryScope> {
        if let Ok(owner) = active_owner()
            && owner.funded_by_writer(&accountant)
        {
            return Ok(owner.enter());
        }
        Ok(Self::operation_writer(accountant)?.enter())
    }

    /// A boxed source window's control credit can survive its payload. Keep
    /// the original explicit reader funder, or a counted writer adapter, for
    /// that escaping credit instead of borrowing the payload's source epoch.
    pub(crate) fn source_box_budget(
        &self,
    ) -> Result<Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>> {
        match &self.reader_budget {
            Some(reader) => Ok(reader.clone()),
            None => self.accountant.read_budget(),
        }
    }

    pub(crate) fn reserve(&self, bytes: usize) -> Result<()> {
        match &self.reader_budget {
            Some(budget) => budget.try_reserve(bytes, self.subsystem, self.operation, self.hint)?,
            None => self.accountant.try_allocate_for(
                bytes,
                self.subsystem,
                self.operation,
                self.hint,
            )?,
        }
        self.held.fetch_add(bytes, Ordering::SeqCst);
        Ok(())
    }
    /// Observe this existing source-copy owner without allocating or cloning it.
    pub(crate) fn visit_retained_copy_credit(
        &self,
        writer: &MemoryAccountant,
        visit: &mut dyn FnMut(usize, usize, bool),
    ) {
        let bytes = self.held.load(Ordering::SeqCst);
        if bytes != 0 {
            if self.reader_budget.is_some() {
                visit(self as *const Self as usize, bytes, false);
            } else if std::ptr::eq(self.accountant.as_ref(), writer) {
                visit(self as *const Self as usize, bytes, true);
            }
        }
    }

    pub(crate) fn release(&self, bytes: usize) {
        self.held.fetch_sub(bytes, Ordering::SeqCst);
        match &self.reader_budget {
            Some(budget) => budget.release(bytes),
            None => self.accountant.release(bytes),
        }
    }
    /// The direct source's existing fixed reservation adopts a concrete
    /// control owner without refunding or admitting those bytes again.
    pub(crate) fn detach_direct_source_credit(
        &self,
        bytes: usize,
    ) -> Result<crate::memory_accounting::ControlCredit> {
        assert!(
            self.reader_budget.is_none(),
            "direct source owns its original accountant credit"
        );
        self.held
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |held| {
                held.checked_sub(bytes)
            })
            .map_err(|_| {
                Error::Other("direct source control exceeds its existing image credit".into())
            })?;
        Ok(crate::memory_accounting::ControlCredit::from_admitted(
            &self.accountant,
            bytes,
        ))
    }
    /// Detach only the caller's explicitly prepaid copy credit. No funding
    /// is returned or admitted while the consuming bridge changes ownership.
    pub(crate) fn vector_copy(self: &Arc<Self>, prepaid_bytes: usize) -> Result<ImageVectorCopy> {
        self.held
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |held| {
                held.checked_sub(prepaid_bytes)
            })
            .map_err(|_| Error::Other("vector copy exceeds its prepaid image credit".into()))?;
        let accountant: Arc<dyn contextdb_vector::MemoryBudget> =
            Arc::new(ImageVectorBudget(self.clone()));
        let charge = ImageVectorCharge {
            accountant: accountant.clone(),
            image: self.clone(),
            bytes: prepaid_bytes,
        };
        let owner =
            contextdb_vector::VectorCopyAdmission::from_prepaid(accountant.clone(), charge)?;
        Ok(ImageVectorCopy {
            owner,
            accountant,
            image: self.clone(),
        })
    }

    fn reserve_funding(&self, bytes: usize) -> Result<()> {
        match &self.reader_budget {
            Some(budget) => budget.try_reserve(bytes, self.subsystem, self.operation, self.hint),
            None => {
                self.accountant
                    .try_allocate_for(bytes, self.subsystem, self.operation, self.hint)
            }
        }
    }

    fn release_funding(&self, bytes: usize) {
        match &self.reader_budget {
            Some(budget) => budget.release(bytes),
            None => self.accountant.release(bytes),
        }
    }

    pub(crate) fn adopt_reader_credit(
        &self,
        budget: &Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        bytes: usize,
    ) -> Result<()> {
        if !self
            .reader_budget
            .as_ref()
            .is_some_and(|reader| Arc::ptr_eq(reader, budget))
        {
            return Err(Error::Other(
                "answer credit has a different reader budget".into()));
        }
        self.held
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |held| {
                held.checked_add(bytes)
            })
            .map_err(|_| Error::Other("answer credit overflow".into()))?;
        Ok(())
    }

    /// Decode under this already funded image owner. Any admission failure
    /// remains the shared typed refusal instead of becoming a codec error.
    pub(crate) fn decode_msgpack<T: de::DeserializeOwned>(
        self: &Arc<Self>,
        bytes: &[u8],
    ) -> Result<T> {
        let before = self.held();
        let result = self.scope(|| {
            let mut decoder = rmp_serde::Deserializer::from_read_ref(bytes);
            // Take this thread-local answer before the core guard restores
            // its enclosing scope's sideband.
            Seed(PhantomData::<T>).deserialize(&mut decoder)
        });
        if let Some(error) = self.take_failure() {
            drop(result);
            self.release(self.held() - before);
            return Err(error);
        }
        match result {
            Ok(value) => Ok(value),
            Err(error) => {
                // The decoder's error is alive under this owner's charges
                // (its text was bounded and admitted as it was built). The
                // returned error is rendered bounded before the decoder's is
                // dropped, and the charges are released only after that.
                let returned = if self.admitted() {
                    Error::SyncError(ADMITTED_CODEC_ERROR.into())
                } else {
                    Error::SyncError((bounded_text("MessagePack decode error: ", &error)).into())
                };
                drop(error);
                self.release(self.held() - before);
                Err(returned)
            }
        }
    }

    pub(crate) fn funded_by_writer(
        &self,
        accountant: &crate::memory_accounting::AccountantHandle,
    ) -> bool {
        self.reader_budget.is_none() && std::ptr::eq(self.accountant.as_ptr(), accountant.as_ptr())
    }

    /// Move prepaid payload funding into its next owner. The native image
    /// and Arc controls remain funded until this image itself drops.
    pub(crate) fn take_retained_credit(
        &self,
        bytes: usize,
    ) -> Result<contextdb_core::read_memory::ReadCredit> {
        let budget = self
            .reader_budget
            .as_ref()
            .ok_or_else(|| Error::Other("row move requires reader-owned credit".into()))?;
        self.held
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |held| {
                held.checked_sub(bytes)
                    .filter(|remaining| *remaining >= self.control_bytes & !ADMITTED_TAG)
            })
            .map_err(|_| Error::Other("row move exceeds its retained payload credit".into()))?;
        Ok(contextdb_core::read_memory::ReadCredit::from_prepaid(
            budget.clone(),
            bytes,
        ))
    }

    pub(crate) fn retained_payload_bytes(&self) -> usize {
        self.held().saturating_sub(
            if self.control_bytes & crate::memory_accounting::OPEN_RESIDUE_TAG != 0 {
                0
            } else {
                self.control_bytes & !ADMITTED_TAG
            },
        )
    }

    pub(crate) fn held(&self) -> usize {
        self.held.load(Ordering::SeqCst)
    }
    pub(crate) fn take_failure(&self) -> Option<Error> {
        match self.failure.lock().take()? {
            Pending::Owned(error) => Some(error),
            Pending::Refused(refused) => Some(Error::MemoryBudgetExceeded {
                subsystem: self.subsystem.into(),
                operation: self.operation.into(),
                requested_bytes: refused.requested,
                available_bytes: refused.available,
                budget_limit_bytes: refused.limit,
                hint: self.hint.into(),
            }),
        }
    }
    fn failed(&self) -> bool {
        self.failure.lock().is_some()
    }
    /// Whether this owner keeps refusals by value (an admitted reader).
    fn admitted(&self) -> bool {
        self.control_bytes & ADMITTED_TAG != 0
    }
    /// Keep `error` as this owner's pending refusal: by value on an admitted
    /// reader (the error is dropped here), owned otherwise.
    fn keep_refusal(&self, error: Error) {
        if self.admitted()
            && let Some(refused) = Refused::of(&error)
        {
            drop(error);
            *self.failure.lock() = Some(Pending::Refused(refused));
            return;
        }
        *self.failure.lock() = Some(Pending::Owned(error));
    }
    /// A separate row lifetime funded by this operation's existing capability.
    /// The parent remains live while a selected row is being replaced.
    /// A decoded row transferred directly into a detached write projection
    /// is a staged write copy. It keeps the same preallocation funder and
    /// receipt shape while naming that copy's admission at the payload boundary.
    pub(crate) fn staged_row_copy_owner(self: &Arc<Self>) -> Result<Arc<Self>> {
        let mut owner = self.row_owner()?;
        let memory = Arc::get_mut(&mut owner).expect("new row owner is exclusive");
        memory.subsystem = "sync";
        memory.operation = "stage_received_unit_row_image";
        memory.hint = "Raise MEMORY_LIMIT to stage this received row copy.";
        Ok(owner)
    }

    pub(crate) fn row_owner(self: &Arc<Self>) -> Result<Arc<Self>> {
        let control = std::mem::size_of::<Self>() + 2 * std::mem::size_of::<usize>();
        self.reserve_funding(control)?;
        Ok(Arc::new(Self {
            accountant: self.accountant.clone(),
            subsystem: self.subsystem,
            operation: self.operation,
            hint: self.hint,
            reader_budget: self.reader_budget.clone(),
            _reader_control_invoice: self._reader_control_invoice.clone(),
            held: AtomicUsize::new(control),
            control_bytes: control | (self.control_bytes & ADMITTED_TAG),
            failure: parking_lot::Mutex::new(None),
            per_call: None, query_per_call: None,
            prepaid_output: None,
            _writer_custody: self._writer_custody.clone(),
        }))
    }

    pub(crate) fn enter(self: &Arc<Self>) -> ImageMemoryScope {
        let previous = ACTIVE.with(|active| active.replace(Some(self.clone())));
        let budget: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget> = self.clone();
        let json = contextdb_core::types::JsonReadAdmissionGuard::enter(budget);
        ImageMemoryScope {
            previous,
            _json: json,
        }
    }

    pub(crate) fn scope<T>(self: &Arc<Self>, operation: impl FnOnce() -> T) -> T {
        let _scope = self.enter();
        operation()
    }
}
// The already captured image/session charge is itself the required read
// capability; callers do not reconstruct one from a vector-copy adapter.
impl contextdb_core::read_memory::ReadMemoryBudget for ImageMemory {
    fn try_reserve(
        &self,
        bytes: usize,
        _subsystem: &'static str,
        _operation: &'static str,
        _hint: &'static str,
    ) -> Result<()> {
        match self.reserve(bytes) {
            Ok(()) => Ok(()),
            Err(error) => {
                // Serde's fixed sentinel cannot carry a core Error. Keep the
                // original refusal in this same owner's established sideband.
                if self.admitted()
                    && let Some(refused) = Refused::of(&error)
                {
                    *self.failure.lock() = Some(Pending::Refused(refused));
                } else if let Error::MemoryBudgetExceeded {
                    requested_bytes,
                    available_bytes,
                    budget_limit_bytes,
                    ..
                } = &error
                {
                    *self.failure.lock() = Some(Pending::Owned(Error::MemoryBudgetExceeded {
                        subsystem: self.subsystem.into(),
                        operation: self.operation.into(),
                        requested_bytes: *requested_bytes,
                        available_bytes: *available_bytes,
                        budget_limit_bytes: *budget_limit_bytes,
                        hint: self.hint.into(),
                    }));
                } else if let Error::ReadFailure(failure) = &error {
                    use contextdb_core::read_contract::{ReadFailureDetail, ReadFailureLimit};
                    if let ReadFailureDetail::OwnerLimitExceeded(detail) = failure.detail()
                        && detail.limit == ReadFailureLimit::Memory
                    {
                        // A JSON producer preserves the existing typed native
                        // allocation refusal even when the funding capability
                        // expresses its limit in reader-contract vocabulary.
                        let limit = usize::try_from(detail.value).unwrap_or(usize::MAX);
                        let required = detail.required.as_ref().map_or(
                            limit.saturating_add(bytes),
                            |required| {
                                usize::try_from(required.required_bytes).unwrap_or(usize::MAX)
                            },
                        );
                        let held = required.saturating_sub(bytes);
                        *self.failure.lock() = Some(Pending::Owned(Error::MemoryBudgetExceeded {
                            subsystem: self.subsystem.into(),
                            operation: self.operation.into(),
                            requested_bytes: bytes,
                            available_bytes: limit.saturating_sub(held),
                            budget_limit_bytes: limit,
                            hint: self.hint.into(),
                        }));
                    } else {
                        *self.failure.lock() =
                            Some(Pending::Owned(Error::ReadFailure(failure.clone())));
                    }
                }
                Err(error)
            }
        }
    }
    fn release(&self, bytes: usize) {
        ImageMemory::release(self, bytes);
    }
}

/// The bounded static class text an admitted reader answers for malformed
/// input; the decoder's own message is never formatted.
pub(crate) const ADMITTED_CODEC_ERROR: &str = "MessagePack decode error: malformed input";

/// Marks an admitted reader in `ImageMemory::control_bytes` (beside the
/// open-residue tag), so the owner carries no separate flag field.
const ADMITTED_TAG: usize = 1 << (usize::BITS - 2);

/// An owner's pending refusal: an owned error, or one kept by value.
#[derive(Debug)]
enum Pending {
    Owned(Error),
    Refused(Refused),
}

/// A memory refusal kept by value until its owner's caller takes it.
#[derive(Clone, Copy, Debug)]
struct Refused {
    requested: usize,
    available: usize,
    limit: usize,
}
impl Refused {
    fn of(error: &Error) -> Option<Self> {
        if let Error::MemoryBudgetExceeded {
            requested_bytes,
            available_bytes,
            budget_limit_bytes,
            ..
        } = error
        {
            return Some(Self {
                requested: *requested_bytes,
                available: *available_bytes,
                limit: *budget_limit_bytes,
            });
        }
        if let Error::ReadFailure(failure) = error {
            use contextdb_core::read_contract::{ReadFailureDetail, ReadFailureLimit};
            if let ReadFailureDetail::OwnerLimitExceeded(detail) = failure.detail()
                && detail.limit == ReadFailureLimit::Memory
            {
                let limit = usize::try_from(detail.value).unwrap_or(usize::MAX);
                let required = detail.required.as_ref().map_or(limit, |required| {
                    usize::try_from(required.required_bytes).unwrap_or(usize::MAX)
                });
                let requested = required.saturating_sub(limit).max(1);
                return Some(Self {
                    requested,
                    available: limit.saturating_sub(required.saturating_sub(requested)),
                    limit,
                });
            }
        }
        None
    }
}

impl Drop for ImageMemory {
    fn drop(&mut self) {
        self.release(self.held.load(Ordering::SeqCst));
        if self.control_bytes & crate::memory_accounting::OPEN_RESIDUE_TAG != 0 {
            crate::memory_accounting::refund_open_residue(
                self.control_bytes & !(crate::memory_accounting::OPEN_RESIDUE_TAG | ADMITTED_TAG),
            );
        }
    }
}
#[derive(Debug)]
struct ImageVectorBudget(Arc<ImageMemory>);
impl contextdb_vector::MemoryBudget for ImageVectorBudget {
    fn try_allocate_for(&self, bytes: usize, _: &str, _: &str, _: &str) -> Result<()> {
        self.0.reserve_funding(bytes)
    }
    fn release(&self, bytes: usize) {
        self.0.release_funding(bytes);
    }
}

struct ImageVectorCharge {
    accountant: Arc<dyn contextdb_vector::MemoryBudget>,
    image: Arc<ImageMemory>,
    bytes: usize,
}
impl contextdb_vector::PrepaidVectorCopyCharge for ImageVectorCharge {
    fn accountant(&self) -> Arc<dyn contextdb_vector::MemoryBudget> {
        self.accountant.clone()
    }
    fn retained_bytes(&self) -> usize {
        self.bytes
    }
    fn transfer(mut self: Box<Self>) {
        self.bytes = 0;
    }
}
impl Drop for ImageVectorCharge {
    fn drop(&mut self) {
        self.image.release_funding(self.bytes);
    }
}

pub(crate) struct ImageVectorCopy {
    pub(crate) owner: contextdb_vector::VectorCopyAdmission,
    accountant: Arc<dyn contextdb_vector::MemoryBudget>,
    image: Arc<ImageMemory>,
}
impl ImageVectorCopy {
    pub(crate) fn accountant(&self) -> &Arc<dyn contextdb_vector::MemoryBudget> {
        &self.accountant
    }
    /// The returned answer's existing image owner adopts the complete charge
    /// once. The bridge is consumed, and no budget admission occurs here.
    pub(crate) fn into_image(self) {
        self.image
            .held
            .fetch_add(self.owner.retained_bytes(), Ordering::SeqCst);
        self.owner.transfer();
    }
}

/// The memory an answer holds against its reader's limit for as long as the
/// caller holds the answer: given back when its last copy is dropped. It is
/// bookkeeping, not content, so it never makes two answers unequal.
#[derive(Clone, Default)]
pub(crate) struct AnswerCharge {
    image: Option<Arc<ImageMemory>>,
    reader: Option<Arc<contextdb_core::read_memory::ReadCredit>>,
}

impl AnswerCharge {
    pub(crate) fn holding(charge: Arc<ImageMemory>) -> Self {
        Self {
            image: Some(charge),
            reader: None,
        }
    }

    /// Consume the answer's existing credit. The shared credit allocation is
    /// admitted before allocation and remains owned by every answer clone.
    pub(crate) fn holding_reader(
        mut credit: contextdb_core::read_memory::ReadCredit,
    ) -> Result<Self> {
        let control_bytes = std::mem::size_of::<contextdb_core::read_memory::ReadCredit>()
            + 2 * std::mem::size_of::<usize>();
        credit.try_grow_for(
            control_bytes,
            "bounded_read",
            "retain_answer_credit",
            "Reduce the statement's output or raise the declared reader memory limit.",
        )?;
        Ok(Self {
            image: None,
            reader: Some(Arc::new(credit)),
        })
    }

    pub(crate) fn held(&self) -> usize {
        self.image.as_ref().map_or(0, |charge| charge.held())
            + self.reader.as_ref().map_or(0, |charge| charge.bytes())
    }
}

impl std::fmt::Debug for AnswerCharge {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("AnswerCharge")
            .field(&self.held())
            .finish()
    }
}

impl PartialEq for AnswerCharge {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}

thread_local! {
    static ACTIVE: RefCell<Option<Arc<ImageMemory>>> = const { RefCell::new(None) };
    static NATIVE_DECODE: std::cell::Cell<Option<std::ptr::NonNull<NativeDecodeMemory>>> =
        const { std::cell::Cell::new(None) };
}

/// A codec stop signal whose message is empty, so building it allocates
/// nothing; the real cause stays in the decode accountant.
struct NoMessage;
impl std::fmt::Display for NoMessage {
    fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

/// The native delivery fetch's decode accountant. Its control block is a
/// `ReadShared` paid for before allocation, so its invoice settles only after
/// the block's last holder has freed it. Each decode admission is a native
/// reservation on the request owner; a refusal is kept by value with no owned
/// diagnostic. Bytes admitted for decoded values are returned when the
/// accountant drops, after the values it decoded (their owner drops them
/// first).
pub(crate) struct NativeDecodeMemory {
    owner: contextdb_core::read_memory::ReadMemoryOwner,
    held: AtomicUsize,
    refusal: parking_lot::Mutex<Option<contextdb_core::read_memory::NativeRefusal>>,
}

/// Why a native decode produced no value: a native admission refused, or the
/// input was not a valid encoding (a static class; the codec's own message is
/// dropped).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NativeDecodeFailure {
    Refused(contextdb_core::read_memory::NativeRefusal),
    Codec,
}

const NATIVE_DECODE_HINT: &str =
    "Reduce the sync response or raise the declared reader memory limit.";

impl NativeDecodeMemory {
    pub(crate) fn try_new(
        owner: contextdb_core::read_memory::ReadMemoryOwner,
    ) -> std::result::Result<
        contextdb_core::read_memory::ReadShared<Self>,
        contextdb_core::read_memory::NativeRefusal,
    > {
        use contextdb_core::read_memory::{ReadCredit, ReadShared};
        let invoice = ReadCredit::try_new_owned_native(
            owner.clone(),
            ReadShared::<Self>::layout().size(),
            "reader",
            "decode_sync_response",
            NATIVE_DECODE_HINT,
        )?;
        ReadShared::try_from_prepaid_native(
            Self {
                owner,
                held: AtomicUsize::new(0),
                refusal: parking_lot::Mutex::new(None),
            },
            invoice,
        )
    }
    pub(crate) fn held(&self) -> usize {
        self.held.load(Ordering::SeqCst)
    }
    fn admit(&self, bytes: usize) -> std::result::Result<(), ()> {
        use contextdb_core::read_memory::ReadMemoryBudget;
        let mut refusal = self.refusal.lock();
        if refusal.is_some() {
            return Err(());
        }
        match self.owner.try_reserve_native(
            bytes,
            "reader",
            "decode_sync_response",
            NATIVE_DECODE_HINT,
        ) {
            Ok(()) => {
                self.held.fetch_add(bytes, Ordering::SeqCst);
                Ok(())
            }
            Err(error) => {
                *refusal = Some(error);
                Err(())
            }
        }
    }
    fn release(&self, bytes: usize) {
        use contextdb_core::read_memory::ReadMemoryBudget;
        self.held.fetch_sub(bytes, Ordering::SeqCst);
        self.owner.release(bytes);
    }
}
impl Drop for NativeDecodeMemory {
    fn drop(&mut self) {
        use contextdb_core::read_memory::ReadMemoryBudget;
        self.owner.release(self.held.load(Ordering::SeqCst));
    }
}

/// Sets the native decode accountant for this thread while it is borrowed.
struct NativeDecodeScope {
    previous: Option<std::ptr::NonNull<NativeDecodeMemory>>,
}
impl NativeDecodeScope {
    fn enter(memory: &NativeDecodeMemory) -> Self {
        let previous =
            NATIVE_DECODE.with(|active| active.replace(Some(std::ptr::NonNull::from(memory))));
        Self { previous }
    }
}
impl Drop for NativeDecodeScope {
    fn drop(&mut self) {
        NATIVE_DECODE.with(|active| active.set(self.previous));
    }
}

/// Decode `bytes` with every allocation admitted natively on `memory` before
/// it is made. A refusal returns the `NativeRefusal` by value; malformed input
/// returns the static `Codec` class. Either way the bytes admitted for the
/// failed decode are returned before this returns.
pub(crate) fn decode_msgpack_native<T: de::DeserializeOwned>(
    memory: &NativeDecodeMemory,
    bytes: &[u8],
) -> std::result::Result<T, NativeDecodeFailure> {
    let before = memory.held();
    let result = {
        let _scope = NativeDecodeScope::enter(memory);
        let mut decoder = rmp_serde::Deserializer::from_read_ref(bytes);
        Seed(PhantomData::<T>).deserialize(&mut decoder)
    };
    if let Some(refusal) = memory.refusal.lock().take() {
        drop(result);
        memory.release(memory.held() - before);
        return Err(NativeDecodeFailure::Refused(refusal));
    }
    match result {
        Ok(value) => Ok(value),
        Err(error) => {
            drop(error);
            memory.release(memory.held() - before);
            Err(NativeDecodeFailure::Codec)
        }
    }
}

/// Inline restoration preserves nested owners and failure unwinding without a
/// heap-allocated context stack.
pub(crate) struct ImageMemoryScope {
    previous: Option<Arc<ImageMemory>>,
    _json: contextdb_core::types::JsonReadAdmissionGuard,
}
impl Drop for ImageMemoryScope {
    fn drop(&mut self) {
        ACTIVE.with(|active| {
            active.replace(self.previous.take());
        });
    }
}

pub(crate) fn active_owner() -> Result<Arc<ImageMemory>> {
    ACTIVE
        .with(|active| active.borrow().clone())
        .ok_or_else(|| Error::Other("engine decode requires an operation memory owner".into()))
}

pub(crate) fn is_missing_decode_owner(error: &Error) -> bool {
    matches!(error, Error::Other(reason) if matches!(reason.as_ref(),
        "engine decode requires an operation memory owner"
        | "writer decode requires its declared memory accountant"
        | "checkpoint source requires its installed accountant identity"))
}

pub(crate) fn is_memory_refusal(error: &Error) -> bool {
    match error {
        Error::MemoryBudgetExceeded { .. } => true,
        Error::ReadFailure(failure) => matches!(failure.detail(),
            contextdb_core::read_contract::ReadFailureDetail::OwnerLimitExceeded(detail)
                if detail.limit == contextdb_core::read_contract::ReadFailureLimit::Memory),
        _ => false,
    }
}

pub(crate) fn current_reader_budget()
-> Option<Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>> {
    ACTIVE.with(|active| {
        active
            .borrow()
            .as_ref()
            .and_then(|owner| owner.reader_budget())
    })
}

pub(crate) fn reserve(bytes: usize) -> Result<()> {
    active_owner()?.reserve(bytes)
}

/// Admit a decoded value the caller keeps. A caller that already admitted
/// its kept outputs (`operation_writer_per_call`) has that credit adopted;
/// otherwise the active owner is charged, as for any other admission.
pub(crate) fn reserve_output(bytes: usize) -> Result<()> {
    let owner = active_owner()?;
    if adopt_candidate_decode(&owner, bytes) || owner.adopt_prepaid_output(bytes) {
        return Ok(());
    }
    owner.reserve(bytes)
}

fn admit_output<E: de::Error>(bytes: usize) -> std::result::Result<(), E> {
    let owner = active_owner().map_err(E::custom)?;
    if owner.failed() {
        return Err(E::custom("read image admission refused"));
    }
    if adopt_candidate_decode(&owner, bytes) || owner.adopt_prepaid_output(bytes) {
        return Ok(());
    }
    admit(bytes)
}

// An exact map visitor admits its one concrete destination before allocation.
// Nested maps retain the ordinary accounting unless they explicitly opt in.
thread_local! {
    static EXACT_MAP: std::cell::Cell<Option<fn(usize) -> Result<usize>>> = const { std::cell::Cell::new(None) };
    static EXACT_MAP_STRINGS: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}
pub(crate) fn with_exact_map<K, V, T>(body: impl FnOnce() -> T) -> T {
    fn quote<K, V>(len: usize) -> Result<usize> {
        crate::memory_accounting::native_map_plan::<K, V>(len).map(|plan|
            plan.layout.size().saturating_add(if len == 0 { 0 } else { 2 * std::mem::size_of::<usize>() }))
    }
    struct Restore(Option<fn(usize) -> Result<usize>>, bool);
    impl Drop for Restore {
        fn drop(&mut self) {
            EXACT_MAP.with(|slot| slot.set(self.0));
            EXACT_MAP_STRINGS.with(|slot| slot.set(self.1));
        }
    }
    let _restore = Restore(EXACT_MAP.with(|slot| slot.replace(Some(quote::<K,V>))),
        EXACT_MAP_STRINGS.with(|slot| slot.replace(true)));
    body()
}
fn string_allocation_bytes(len: usize) -> usize {
    len.saturating_add(if len != 0 && EXACT_MAP_STRINGS.with(std::cell::Cell::get) {
        2 * std::mem::size_of::<usize>()
    } else { 0 })
}

thread_local! {
    /// The sequence `deserialize_exact_vec` is about to read, until the
    /// admitted wrapper (or the visitor itself) admits its exact length.
    static EXACT_SEQUENCE: std::cell::Cell<Option<ExactSequence>> =
        const { std::cell::Cell::new(None) };
}

#[derive(Clone, Copy)]
struct ExactSequence {
    element_bytes: usize,
    output: bool,
    admitted: bool,
}

/// Admit the exact length of the sequence `deserialize_exact_vec` opened,
/// before anything is allocated for it. Returns whether this is that sequence.
fn admit_exact_sequence<E: de::Error>(length: Option<usize>) -> std::result::Result<bool, E> {
    let Some(pending) = EXACT_SEQUENCE.with(std::cell::Cell::get) else {
        return Ok(false);
    };
    if pending.admitted {
        return Ok(false);
    }
    let length = length.ok_or_else(|| E::custom("exact sequence has no encoded length"))?;
    let bytes = length
        .checked_mul(pending.element_bytes)
        .ok_or_else(|| E::custom("exact sequence length overflows"))?;
    if pending.output {
        admit_output(bytes)?;
    } else {
        admit(bytes)?;
    }
    EXACT_SEQUENCE.with(|slot| {
        slot.set(Some(ExactSequence {
            admitted: true,
            ..pending
        }))
    });
    Ok(true)
}

/// Deserialize a vector into exactly its encoded slot count. Nested owned
/// payloads keep their separate ordinary decode admissions. Its bytes are admitted before the vector is allocated -- as a
/// kept output when `output`, so a caller's prepaid credit is adopted --
/// and nothing is charged per element. The generic sequence path, which
/// cannot know its element size before allocation, keeps its growth
/// allowance unchanged.
pub(crate) fn deserialize_exact_vec<'de, D, E>(
    deserializer: D,
    output: bool,
) -> std::result::Result<Vec<E>, D::Error>
where
    D: de::Deserializer<'de>,
    E: de::Deserialize<'de>,
{
    struct Restore(Option<ExactSequence>);
    impl Drop for Restore {
        fn drop(&mut self) {
            EXACT_SEQUENCE.with(|slot| slot.set(self.0));
        }
    }
    let _restore = Restore(EXACT_SEQUENCE.with(|slot| {
        slot.replace(Some(ExactSequence {
            element_bytes: std::mem::size_of::<E>(),
            output,
            admitted: false,
        }))
    }));
    struct ExactVec<E>(PhantomData<E>);
    impl<'de, E: de::Deserialize<'de>> Visitor<'de> for ExactVec<E> {
        type Value = Vec<E>;
        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("a length-prefixed sequence")
        }
        fn visit_seq<A: de::SeqAccess<'de>>(
            self,
            mut sequence: A,
        ) -> std::result::Result<Self::Value, A::Error> {
            // Read outside the admitted wrapper, the visitor admits itself.
            admit_exact_sequence::<A::Error>(sequence.size_hint())?;
            let length = sequence
                .size_hint()
                .ok_or_else(|| de::Error::custom("exact sequence has no encoded length"))?;
            let mut values = Vec::with_capacity(length);
            while let Some(value) = sequence.next_element()? {
                if values.len() == length {
                    return Err(de::Error::custom(
                        "exact sequence is longer than its length",
                    ));
                }
                values.push(value);
            }
            if values.len() != length {
                return Err(de::Error::custom(
                    "exact sequence is shorter than its length",
                ));
            }
            Ok(values)
        }
    }
    deserializer.deserialize_seq(ExactVec(PhantomData))
}
/// The longest detail a decoder error carries; longer text is cut at a char
/// boundary before any copy of it exists.
pub(crate) const DECODE_ERROR_DETAIL_MAX: usize = 16 * 1024;

/// Counts, or writes, at most `cap` bytes of formatted text, cut at a char
/// boundary: a counting pass and a writing pass agree byte for byte.
struct BoundedWriter<'a> {
    out: Option<&'a mut String>,
    len: usize,
    cap: usize,
    full: bool,
}
impl std::fmt::Write for BoundedWriter<'_> {
    fn write_str(&mut self, part: &str) -> std::fmt::Result {
        if self.full {
            return Ok(());
        }
        let room = self.cap - self.len;
        let mut end = part.len().min(room);
        if end < part.len() {
            while !part.is_char_boundary(end) {
                end -= 1;
            }
            self.full = true;
        }
        if let Some(out) = self.out.as_deref_mut() {
            out.push_str(&part[..end]);
        }
        self.len += end;
        Ok(())
    }
}
/// The bytes `prefix` + `detail` renders to, bounded at
/// `prefix.len() + DECODE_ERROR_DETAIL_MAX`.
fn bounded_len(prefix: &str, detail: &dyn std::fmt::Display) -> usize {
    use std::fmt::Write;
    let mut counter = BoundedWriter {
        out: None,
        len: 0,
        cap: prefix.len() + DECODE_ERROR_DETAIL_MAX,
        full: false,
    };
    let _ = write!(counter, "{prefix}{detail}");
    counter.len
}
/// `prefix` + `detail`, bounded, in a string allocated at exactly its length.
pub(crate) fn bounded_text(prefix: &str, detail: &dyn std::fmt::Display) -> String {
    use std::fmt::Write;
    let len = bounded_len(prefix, detail);
    let mut text = String::with_capacity(len);
    let mut writer = BoundedWriter {
        out: Some(&mut text),
        len: 0,
        cap: len,
        full: false,
    };
    let _ = write!(writer, "{prefix}{detail}");
    text
}

const ADMISSION_REFUSED: &str = "read image admission refused";

/// The error a decoder's visitors build inside an admitted decode: its text
/// is measured first, cut at `DECODE_ERROR_DETAIL_MAX`, admitted to the
/// active owner and only then allocated, and it releases that admission after
/// its text is freed. Converting it into the decoder's own error type admits
/// that copy before it exists; a refused admission keeps the owner's typed
/// refusal and carries only the fixed static text.
#[derive(Debug)]
struct BoundedDecodeError {
    text: Option<String>,
    charge: Option<(Arc<ImageMemory>, usize)>,
}
impl BoundedDecodeError {
    fn refused() -> Self {
        Self {
            text: None,
            charge: None,
        }
    }
    fn into_error<E: de::Error>(self) -> E {
        let Some(text) = self.text.as_deref() else {
            return E::custom(ADMISSION_REFUSED);
        };
        if let Ok(owner) = active_owner()
            && let Err(error) = owner.reserve(text.len())
        {
            owner.keep_refusal(error);
            return E::custom(ADMISSION_REFUSED);
        }
        E::custom(text)
    }
}
impl Drop for BoundedDecodeError {
    fn drop(&mut self) {
        self.text = None;
        if let Some((owner, bytes)) = self.charge.take() {
            owner.release(bytes);
        }
    }
}
impl std::fmt::Display for BoundedDecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.text.as_deref().unwrap_or(ADMISSION_REFUSED))
    }
}
impl std::error::Error for BoundedDecodeError {}
impl de::Error for BoundedDecodeError {
    fn custom<T: std::fmt::Display>(message: T) -> Self {
        let len = bounded_len("", &message);
        let Ok(owner) = active_owner() else {
            return Self {
                text: Some(bounded_text("", &message)),
                charge: None,
            };
        };
        if owner.failed() {
            return Self::refused();
        }
        if let Err(error) = owner.reserve(len) {
            owner.keep_refusal(error);
            return Self::refused();
        }
        Self {
            text: Some(bounded_text("", &message)),
            charge: Some((owner, len)),
        }
    }
}
/// A visitor's answer with its bounded error carried into the decoder's own.
fn bounded<T, E: de::Error>(
    answer: std::result::Result<T, BoundedDecodeError>,
) -> std::result::Result<T, E> {
    answer.map_err(BoundedDecodeError::into_error)
}

fn admit<E: de::Error>(bytes: usize) -> std::result::Result<(), E> {
    if let Some(native) = NATIVE_DECODE.with(std::cell::Cell::get) {
        // SAFETY: `NativeDecodeScope` sets this pointer only while it borrows
        // the live accountant, and restores the previous value before that
        // borrow ends.
        let native = unsafe { native.as_ref() };
        // A refusal is kept by value in the accountant; the codec only needs
        // to stop, and an empty message allocates nothing.
        return native.admit(bytes).map_err(|()| E::custom(NoMessage));
    }
    let owner = active_owner().map_err(E::custom)?;
    if owner.failed() {
        return Err(E::custom("read image admission refused"));
    }
    if let Err(error) = owner.reserve(bytes) {
        owner.keep_refusal(error);
        return Err(E::custom("read image admission refused"));
    }
    Ok(())
}
pub(crate) fn decode<T: de::DeserializeOwned>(bytes: &[u8]) -> Result<(T, usize)> {
    decode_with_control(bytes, std::mem::size_of::<T>()
        .checked_mul(4).and_then(|bytes| bytes.checked_add(128))
        .ok_or_else(|| Error::Other("decode control size overflow".into()))?)
}

/// Fixed-layout codecs with exact buffer visitors have no heap parser/control
/// object: their serde seed and bincode reader are stack values. Their one
/// destination, strings and nested payloads still admit before allocation.
pub(crate) fn decode_exact_buffers<T: de::DeserializeOwned>(bytes: &[u8]) -> Result<(T, usize)> {
    decode_with_control(bytes, 0)
}
fn decode_with_control<T: de::DeserializeOwned>(bytes: &[u8], control: usize) -> Result<(T, usize)> {
    let _decode_scope = ImageMemory::enter_decode_call()?;
    let owner = active_owner()?;
    owner.reserve(control)?;
    struct Control<'a>(&'a ImageMemory, usize);
    impl Drop for Control<'_> {
        fn drop(&mut self) {
            self.0.release(self.1);
        }
    }
    let _control = Control(&owner, control);
    let before = owner.held();
    let result = bincode::serde::seed_decode_from_slice(
        Seed(PhantomData::<T>),
        bytes,
        bincode::config::standard(),
    );
    if let Some(error) = owner.take_failure() {
        // Destroy a returned or partial payload before releasing its credit.
        drop(result);
        owner.release(owner.held() - before);
        return Err(error);
    }
    match result {
        Ok(value) => Ok(value),
        Err(error) => {
            let returned = Error::Other((bounded_text("bincode decode error: ", &error)).into());
            drop(error);
            owner.release(owner.held() - before);
            Err(returned)
        }
    }
}
/// One rendered metadata record uses the same admitted seed and JSON owner
/// as binary records. The native outer parser's scratch/lexical work is a
/// record-local conservative transient, paid before parser construction.
pub(crate) fn decode_json<T: de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    let _decode_scope = ImageMemory::enter_decode_call()?;
    let owner = active_owner()?;
    let bound = bytes
        .len()
        .checked_mul(4)
        .and_then(|n| n.checked_add(7232))
        .ok_or_else(|| Error::Other("rendered JSON admission size overflow".into()))?;
    let scratch = contextdb_core::read_memory::ReadCredit::try_new(
        owner.clone(),
        bound,
        "json_decode",
        "rendered_record",
        "Raise the reader memory limit to decode this record.",
    )?;
    let before = owner.held();
    let mut parser = serde_json::Deserializer::from_slice(bytes);
    let result = Seed(PhantomData::<T>).deserialize(&mut parser);
    let result = match result {
        Ok(value) => match parser.end() {
            Ok(()) => Ok(value),
            Err(error) => {
                drop(value);
                Err(error)
            }
        },
        Err(error) => Err(error),
    };
    drop(parser);
    if let Some(error) = owner.take_failure() {
        drop(result);
        owner.release(owner.held() - before);
        drop(scratch);
        return Err(error);
    }
    match result {
        Ok(value) => {
            drop(scratch);
            Ok(value)
        }
        Err(error) => {
            owner.release(owner.held() - before);
            drop(scratch);
            Err(Error::Other((format!("rendered JSON decode error: {error}")).into()))
        }
    }
}

/// Payload allocated by serde_json's ordinary deep clone. Cloned strings
/// and arrays allocate their lengths; map nodes use the approved unique-key
/// node bound. Source capacities remain separately funded by their owner.
pub(crate) fn json_clone_heap_bytes(value: &serde_json::Value) -> usize {
    match value {
        serde_json::Value::String(text) => text.len(),
        serde_json::Value::Array(values) => values.iter().fold(
            values
                .len()
                .saturating_mul(std::mem::size_of::<serde_json::Value>()),
            |bytes, child| bytes.saturating_add(json_clone_heap_bytes(child)),
        ),
        serde_json::Value::Object(values) => {
            let nodes = if values.is_empty() {
                0
            } else {
                1 + (values.len() - 1) / 5
            };
            values
                .iter()
                .fold(nodes.saturating_mul(728), |bytes, (key, child)| {
                    bytes
                        .saturating_add(key.len())
                        .saturating_add(json_clone_heap_bytes(child))
                })
        }
        _ => 0,
    }
}

struct Seed<S>(S);
impl<'de, S: DeserializeSeed<'de>> DeserializeSeed<'de> for Seed<S> {
    type Value = S::Value;
    fn deserialize<D: de::Deserializer<'de>>(
        self,
        d: D,
    ) -> std::result::Result<Self::Value, D::Error> {
        self.0.deserialize(Admitted(d))
    }
}
struct Admitted<D>(D);
struct V<VV> {
    inner: VV,
    heap: bool,
}
impl<'de, VV: Visitor<'de>> Visitor<'de> for V<VV> {
    type Value = VV::Value;
    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.expecting(f)
    }
    fn visit_bool<E: de::Error>(self, value: bool) -> std::result::Result<Self::Value, E> {
        bounded(self.inner.visit_bool::<BoundedDecodeError>(value))
    }
    fn visit_i8<E: de::Error>(self, value: i8) -> std::result::Result<Self::Value, E> {
        bounded(self.inner.visit_i8::<BoundedDecodeError>(value))
    }
    fn visit_i16<E: de::Error>(self, value: i16) -> std::result::Result<Self::Value, E> {
        bounded(self.inner.visit_i16::<BoundedDecodeError>(value))
    }
    fn visit_i32<E: de::Error>(self, value: i32) -> std::result::Result<Self::Value, E> {
        bounded(self.inner.visit_i32::<BoundedDecodeError>(value))
    }
    fn visit_i64<E: de::Error>(self, value: i64) -> std::result::Result<Self::Value, E> {
        bounded(self.inner.visit_i64::<BoundedDecodeError>(value))
    }
    fn visit_i128<E: de::Error>(self, value: i128) -> std::result::Result<Self::Value, E> {
        bounded(self.inner.visit_i128::<BoundedDecodeError>(value))
    }
    fn visit_u8<E: de::Error>(self, value: u8) -> std::result::Result<Self::Value, E> {
        bounded(self.inner.visit_u8::<BoundedDecodeError>(value))
    }
    fn visit_u16<E: de::Error>(self, value: u16) -> std::result::Result<Self::Value, E> {
        bounded(self.inner.visit_u16::<BoundedDecodeError>(value))
    }
    fn visit_u32<E: de::Error>(self, value: u32) -> std::result::Result<Self::Value, E> {
        bounded(self.inner.visit_u32::<BoundedDecodeError>(value))
    }
    fn visit_u64<E: de::Error>(self, value: u64) -> std::result::Result<Self::Value, E> {
        bounded(self.inner.visit_u64::<BoundedDecodeError>(value))
    }
    fn visit_u128<E: de::Error>(self, value: u128) -> std::result::Result<Self::Value, E> {
        bounded(self.inner.visit_u128::<BoundedDecodeError>(value))
    }
    fn visit_f32<E: de::Error>(self, value: f32) -> std::result::Result<Self::Value, E> {
        bounded(self.inner.visit_f32::<BoundedDecodeError>(value))
    }
    fn visit_f64<E: de::Error>(self, value: f64) -> std::result::Result<Self::Value, E> {
        bounded(self.inner.visit_f64::<BoundedDecodeError>(value))
    }
    fn visit_char<E: de::Error>(self, value: char) -> std::result::Result<Self::Value, E> {
        bounded(self.inner.visit_char::<BoundedDecodeError>(value))
    }
    fn visit_unit<E: de::Error>(self) -> std::result::Result<Self::Value, E> {
        bounded(self.inner.visit_unit::<BoundedDecodeError>())
    }
    fn visit_none<E: de::Error>(self) -> std::result::Result<Self::Value, E> {
        bounded(self.inner.visit_none::<BoundedDecodeError>())
    }
    fn visit_some<D: de::Deserializer<'de>>(
        self,
        d: D,
    ) -> std::result::Result<Self::Value, D::Error> {
        self.inner.visit_some(Admitted(d))
    }
    fn visit_newtype_struct<D: de::Deserializer<'de>>(
        self,
        d: D,
    ) -> std::result::Result<Self::Value, D::Error> {
        self.inner.visit_newtype_struct(Admitted(d))
    }
    fn visit_str<E: de::Error>(self, value: &str) -> std::result::Result<Self::Value, E> {
        admit(string_allocation_bytes(value.len()))?;
        bounded(self.inner.visit_str::<BoundedDecodeError>(value))
    }
    fn visit_borrowed_str<E: de::Error>(
        self,
        value: &'de str,
    ) -> std::result::Result<Self::Value, E> {
        admit(string_allocation_bytes(value.len()))?;
        bounded(self.inner.visit_borrowed_str::<BoundedDecodeError>(value))
    }
    fn visit_bytes<E: de::Error>(self, value: &[u8]) -> std::result::Result<Self::Value, E> {
        admit(value.len())?;
        bounded(self.inner.visit_bytes::<BoundedDecodeError>(value))
    }
    fn visit_borrowed_bytes<E: de::Error>(
        self,
        value: &'de [u8],
    ) -> std::result::Result<Self::Value, E> {
        admit(value.len())?;
        bounded(self.inner.visit_borrowed_bytes::<BoundedDecodeError>(value))
    }
    fn visit_seq<A: de::SeqAccess<'de>>(self, a: A) -> std::result::Result<Self::Value, A::Error> {
        let exact = self.heap && admit_exact_sequence::<A::Error>(a.size_hint())?;
        self.inner.visit_seq(Seq {
            inner: a,
            heap: self.heap && !exact,
            first: true,
        })
    }
    fn visit_map<A: de::MapAccess<'de>>(self, a: A) -> std::result::Result<Self::Value, A::Error> {
        let exact = if let Some(quote) = EXACT_MAP.with(std::cell::Cell::take) {
            let len = a.size_hint().ok_or_else(|| de::Error::custom("exact map has no encoded length"))?;
            let bytes = quote(len).map_err(de::Error::custom)?;
            admit(bytes)?;
            true
        } else { false };
        self.inner.visit_map(Map {
            inner: a, first_key: true, first_value: true, exact,
        })
    }
    fn visit_enum<A: de::EnumAccess<'de>>(
        self,
        a: A,
    ) -> std::result::Result<Self::Value, A::Error> {
        self.inner.visit_enum(Enum(a))
    }
}
struct Seq<A> {
    inner: A,
    heap: bool,
    first: bool,
}
impl<'de, A: de::SeqAccess<'de>> de::SeqAccess<'de> for Seq<A> {
    type Error = A::Error;
    fn next_element_seed<S: DeserializeSeed<'de>>(
        &mut self,
        seed: S,
    ) -> std::result::Result<Option<S::Value>, A::Error> {
        if self.heap && self.inner.size_hint() != Some(0) {
            // With size_hint hidden, Vec grows geometrically. Include its
            // minimum capacity and both allocations during a reallocation.
            let slots = if self.first { 12 } else { 4 };
            admit(std::mem::size_of::<S::Value>().saturating_mul(slots))?;
            self.first = false;
        }
        self.inner.next_element_seed(Seed(seed))
    }
    fn size_hint(&self) -> Option<usize> {
        if self.heap {
            Some(0)
        } else {
            self.inner.size_hint()
        }
    }
}
struct Map<A> {
    inner: A,
    first_key: bool,
    first_value: bool,
    exact: bool,
}
impl<'de, A: de::MapAccess<'de>> de::MapAccess<'de> for Map<A> {
    type Error = A::Error;
    fn next_key_seed<S: DeserializeSeed<'de>>(
        &mut self,
        seed: S,
    ) -> std::result::Result<Option<S::Value>, A::Error> {
        if !self.exact && self.inner.size_hint() != Some(0) {
            let slots = if self.first_key { 16 } else { 4 };
            admit(
                std::mem::size_of::<S::Value>()
                    .saturating_mul(slots)
                    .saturating_add(64),
            )?;
            self.first_key = false;
        }
        self.inner.next_key_seed(Seed(seed))
    }
    fn next_value_seed<S: DeserializeSeed<'de>>(
        &mut self,
        seed: S,
    ) -> std::result::Result<S::Value, A::Error> {
        if !self.exact {
            let slots = if self.first_value { 16 } else { 4 };
            admit(std::mem::size_of::<S::Value>().saturating_mul(slots).saturating_add(64))?;
        }
        self.first_value = false;
        self.inner.next_value_seed(Seed(seed))
    }
    fn size_hint(&self) -> Option<usize> {
        if self.exact { self.inner.size_hint() } else { Some(0) }
    }
}
struct Enum<A>(A);
impl<'de, A: de::EnumAccess<'de>> de::EnumAccess<'de> for Enum<A> {
    type Error = A::Error;
    type Variant = Variant<A::Variant>;
    fn variant_seed<S: DeserializeSeed<'de>>(
        self,
        seed: S,
    ) -> std::result::Result<(S::Value, Self::Variant), A::Error> {
        self.0
            .variant_seed(Seed(seed))
            .map(|(value, variant)| (value, Variant(variant)))
    }
}
struct Variant<A>(A);
impl<'de, A: de::VariantAccess<'de>> de::VariantAccess<'de> for Variant<A> {
    type Error = A::Error;
    fn unit_variant(self) -> std::result::Result<(), A::Error> {
        self.0.unit_variant()
    }
    fn newtype_variant_seed<S: DeserializeSeed<'de>>(
        self,
        seed: S,
    ) -> std::result::Result<S::Value, A::Error> {
        self.0.newtype_variant_seed(Seed(seed))
    }
    fn tuple_variant<VV: Visitor<'de>>(
        self,
        len: usize,
        visitor: VV,
    ) -> std::result::Result<VV::Value, A::Error> {
        self.0.tuple_variant(
            len,
            V {
                inner: visitor,
                heap: false,
            },
        )
    }
    fn struct_variant<VV: Visitor<'de>>(
        self,
        fields: &'static [&'static str],
        visitor: VV,
    ) -> std::result::Result<VV::Value, A::Error> {
        self.0.struct_variant(
            fields,
            V {
                inner: visitor,
                heap: false,
            },
        )
    }
}
impl<'de, D: de::Deserializer<'de>> de::Deserializer<'de> for Admitted<D> {
    type Error = D::Error;
    fn deserialize_any<VV: Visitor<'de>>(
        self,
        visitor: VV,
    ) -> std::result::Result<VV::Value, D::Error> {
        self.0.deserialize_any(V {
            inner: visitor,
            heap: false,
        })
    }
    fn deserialize_bool<VV: Visitor<'de>>(
        self,
        visitor: VV,
    ) -> std::result::Result<VV::Value, D::Error> {
        self.0.deserialize_bool(V {
            inner: visitor,
            heap: false,
        })
    }
    fn deserialize_i8<VV: Visitor<'de>>(
        self,
        visitor: VV,
    ) -> std::result::Result<VV::Value, D::Error> {
        self.0.deserialize_i8(V {
            inner: visitor,
            heap: false,
        })
    }
    fn deserialize_i16<VV: Visitor<'de>>(
        self,
        visitor: VV,
    ) -> std::result::Result<VV::Value, D::Error> {
        self.0.deserialize_i16(V {
            inner: visitor,
            heap: false,
        })
    }
    fn deserialize_i32<VV: Visitor<'de>>(
        self,
        visitor: VV,
    ) -> std::result::Result<VV::Value, D::Error> {
        self.0.deserialize_i32(V {
            inner: visitor,
            heap: false,
        })
    }
    fn deserialize_i64<VV: Visitor<'de>>(
        self,
        visitor: VV,
    ) -> std::result::Result<VV::Value, D::Error> {
        self.0.deserialize_i64(V {
            inner: visitor,
            heap: false,
        })
    }
    fn deserialize_i128<VV: Visitor<'de>>(
        self,
        visitor: VV,
    ) -> std::result::Result<VV::Value, D::Error> {
        self.0.deserialize_i128(V {
            inner: visitor,
            heap: false,
        })
    }
    fn deserialize_u8<VV: Visitor<'de>>(
        self,
        visitor: VV,
    ) -> std::result::Result<VV::Value, D::Error> {
        self.0.deserialize_u8(V {
            inner: visitor,
            heap: false,
        })
    }
    fn deserialize_u16<VV: Visitor<'de>>(
        self,
        visitor: VV,
    ) -> std::result::Result<VV::Value, D::Error> {
        self.0.deserialize_u16(V {
            inner: visitor,
            heap: false,
        })
    }
    fn deserialize_u32<VV: Visitor<'de>>(
        self,
        visitor: VV,
    ) -> std::result::Result<VV::Value, D::Error> {
        self.0.deserialize_u32(V {
            inner: visitor,
            heap: false,
        })
    }
    fn deserialize_u64<VV: Visitor<'de>>(
        self,
        visitor: VV,
    ) -> std::result::Result<VV::Value, D::Error> {
        self.0.deserialize_u64(V {
            inner: visitor,
            heap: false,
        })
    }
    fn deserialize_u128<VV: Visitor<'de>>(
        self,
        visitor: VV,
    ) -> std::result::Result<VV::Value, D::Error> {
        self.0.deserialize_u128(V {
            inner: visitor,
            heap: false,
        })
    }
    fn deserialize_f32<VV: Visitor<'de>>(
        self,
        visitor: VV,
    ) -> std::result::Result<VV::Value, D::Error> {
        self.0.deserialize_f32(V {
            inner: visitor,
            heap: false,
        })
    }
    fn deserialize_f64<VV: Visitor<'de>>(
        self,
        visitor: VV,
    ) -> std::result::Result<VV::Value, D::Error> {
        self.0.deserialize_f64(V {
            inner: visitor,
            heap: false,
        })
    }
    fn deserialize_char<VV: Visitor<'de>>(
        self,
        visitor: VV,
    ) -> std::result::Result<VV::Value, D::Error> {
        self.0.deserialize_char(V {
            inner: visitor,
            heap: false,
        })
    }
    fn deserialize_str<VV: Visitor<'de>>(
        self,
        visitor: VV,
    ) -> std::result::Result<VV::Value, D::Error> {
        self.0.deserialize_str(V {
            inner: visitor,
            heap: false,
        })
    }
    fn deserialize_string<VV: Visitor<'de>>(
        self,
        visitor: VV,
    ) -> std::result::Result<VV::Value, D::Error> {
        self.0.deserialize_str(V {
            inner: visitor,
            heap: false,
        })
    }
    fn deserialize_bytes<VV: Visitor<'de>>(
        self,
        visitor: VV,
    ) -> std::result::Result<VV::Value, D::Error> {
        self.0.deserialize_bytes(V {
            inner: visitor,
            heap: false,
        })
    }
    fn deserialize_byte_buf<VV: Visitor<'de>>(
        self,
        visitor: VV,
    ) -> std::result::Result<VV::Value, D::Error> {
        self.0.deserialize_bytes(V {
            inner: visitor,
            heap: false,
        })
    }
    fn deserialize_option<VV: Visitor<'de>>(
        self,
        visitor: VV,
    ) -> std::result::Result<VV::Value, D::Error> {
        self.0.deserialize_option(V {
            inner: visitor,
            heap: false,
        })
    }
    fn deserialize_unit<VV: Visitor<'de>>(
        self,
        visitor: VV,
    ) -> std::result::Result<VV::Value, D::Error> {
        self.0.deserialize_unit(V {
            inner: visitor,
            heap: false,
        })
    }
    fn deserialize_seq<VV: Visitor<'de>>(
        self,
        visitor: VV,
    ) -> std::result::Result<VV::Value, D::Error> {
        self.0.deserialize_seq(V {
            inner: visitor,
            heap: true,
        })
    }
    fn deserialize_map<VV: Visitor<'de>>(
        self,
        visitor: VV,
    ) -> std::result::Result<VV::Value, D::Error> {
        self.0.deserialize_map(V {
            inner: visitor,
            heap: true,
        })
    }
    fn deserialize_identifier<VV: Visitor<'de>>(
        self,
        visitor: VV,
    ) -> std::result::Result<VV::Value, D::Error> {
        self.0.deserialize_identifier(V {
            inner: visitor,
            heap: false,
        })
    }
    fn deserialize_ignored_any<VV: Visitor<'de>>(
        self,
        visitor: VV,
    ) -> std::result::Result<VV::Value, D::Error> {
        self.0.deserialize_ignored_any(V {
            inner: visitor,
            heap: false,
        })
    }
    fn deserialize_unit_struct<VV: Visitor<'de>>(
        self,
        name: &'static str,
        visitor: VV,
    ) -> std::result::Result<VV::Value, D::Error> {
        self.0.deserialize_unit_struct(
            name,
            V {
                inner: visitor,
                heap: false,
            },
        )
    }
    fn deserialize_newtype_struct<VV: Visitor<'de>>(
        self,
        name: &'static str,
        visitor: VV,
    ) -> std::result::Result<VV::Value, D::Error> {
        self.0.deserialize_newtype_struct(
            name,
            V {
                inner: visitor,
                heap: false,
            },
        )
    }
    fn deserialize_tuple<VV: Visitor<'de>>(
        self,
        len: usize,
        visitor: VV,
    ) -> std::result::Result<VV::Value, D::Error> {
        self.0.deserialize_tuple(
            len,
            V {
                inner: visitor,
                heap: false,
            },
        )
    }
    fn deserialize_tuple_struct<VV: Visitor<'de>>(
        self,
        name: &'static str,
        len: usize,
        visitor: VV,
    ) -> std::result::Result<VV::Value, D::Error> {
        self.0.deserialize_tuple_struct(
            name,
            len,
            V {
                inner: visitor,
                heap: false,
            },
        )
    }
    fn deserialize_struct<VV: Visitor<'de>>(
        self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: VV,
    ) -> std::result::Result<VV::Value, D::Error> {
        self.0.deserialize_struct(
            name,
            fields,
            V {
                inner: visitor,
                heap: false,
            },
        )
    }
    fn deserialize_enum<VV: Visitor<'de>>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: VV,
    ) -> std::result::Result<VV::Value, D::Error> {
        self.0.deserialize_enum(
            name,
            variants,
            V {
                inner: visitor,
                heap: false,
            },
        )
    }
    fn is_human_readable(&self) -> bool {
        self.0.is_human_readable()
    }
}

/// Private source admission distinguishes an actual copy from a selected,
/// already-funded row moved out of its decode owner. The callback must not
/// detach move credit until every fallible observation has succeeded.
pub(crate) enum RowReadAdmission<'a> {
    Copy(usize),
    Move(&'a ImageMemory),
}

impl RowReadAdmission<'_> {
    pub(crate) fn retain_into_image(self, owner: &ImageMemory) -> Result<()> {
        match self {
            Self::Copy(bytes) => owner.reserve(bytes),
            Self::Move(memory) => {
                let bytes = memory.retained_payload_bytes();
                let source_budget = memory
                    .reader_budget()
                    .ok_or_else(|| Error::Other("row move requires reader funding".into()))?;
                if owner
                    .reader_budget()
                    .is_some_and(|budget| Arc::ptr_eq(&budget, &source_budget))
                {
                    let credit = memory.take_retained_credit(bytes)?;
                    owner.adopt_reader_credit(&source_budget, bytes)?;
                    credit.transfer();
                } else {
                    owner.reserve(bytes)?;
                    drop(memory.take_retained_credit(bytes)?);
                }
                Ok(())
            }
        }
    }

    pub(crate) fn retain_into(
        self,
        credit: &mut contextdb_core::read_memory::ReadCredit,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> Result<()> {
        match self {
            Self::Copy(bytes) => credit.try_grow_for(bytes, subsystem, operation, hint),
            Self::Move(memory) => credit
                .try_merge_prepaid(memory.take_retained_credit(memory.retained_payload_bytes())?),
        }
    }
}

#[cfg(test)]
mod exact_sequence_tests {
    use super::*;
    use serde::Deserialize;

    #[derive(Deserialize)]
    struct Exact(#[serde(deserialize_with = "exact")] Vec<f32>);
    fn exact<'de, D: de::Deserializer<'de>>(d: D) -> std::result::Result<Vec<f32>, D::Error> {
        deserialize_exact_vec(d, false)
    }
    #[derive(Deserialize)]
    struct Kept(#[serde(deserialize_with = "kept")] Vec<f32>);
    fn kept<'de, D: de::Deserializer<'de>>(d: D) -> std::result::Result<Vec<f32>, D::Error> {
        deserialize_exact_vec(d, true)
    }
    #[derive(Deserialize)]
    struct Generic(Vec<f32>);

    fn encoded(values: &[f32]) -> Vec<u8> {
        bincode::serde::encode_to_vec(values, bincode::config::standard()).unwrap()
    }

    fn accountant(limit: usize) -> crate::memory_accounting::AccountantHandle {
        Arc::new(MemoryAccountant::with_budget(limit)).into()
    }

    #[test]
    fn an_exact_sequence_decodes_into_its_length_and_charges_exactly_it() {
        let values: Vec<f32> = (0..1536).map(|lane| lane as f32 * 0.5).collect();
        let owner = ImageMemory::operation_writer(accountant(1 << 20)).unwrap();
        let _scope = owner.enter();
        let (decoded, _) = decode::<Exact>(&encoded(&values)).unwrap();
        assert_eq!(decoded.0, values, "the stored format is unchanged");
        assert_eq!(decoded.0.capacity(), 1536, "no growth past its length");
        assert_eq!(owner.held(), 1536 * 4, "charged exactly its decoded bytes");
    }

    #[test]
    fn the_generic_sequence_keeps_its_growth_allowance() {
        let values: Vec<f32> = (0..1536).map(|lane| lane as f32).collect();
        let owner = ImageMemory::operation_writer(accountant(1 << 20)).unwrap();
        let _scope = owner.enter();
        let (decoded, _) = decode::<Generic>(&encoded(&values)).unwrap();
        assert_eq!(decoded.0, values);
        assert_eq!(owner.held(), 48 + 16 * 1535, "unchanged allowance");
    }

    #[test]
    fn a_kept_sequence_adopts_a_prepaid_credit_instead_of_charging_it() {
        let values: Vec<f32> = (0..1536).map(|lane| lane as f32).collect();
        let writer = accountant(1 << 20);
        let owner = ImageMemory::operation_writer_per_call(writer.clone(), 1536 * 4).unwrap();
        let _scope = owner.enter();
        let before = writer.usage().used;
        let (decoded, _) = decode::<Kept>(&encoded(&values)).unwrap();
        assert_eq!(decoded.0.capacity(), 1536);
        assert_eq!(owner.adopted_output(), 1536 * 4);
        // The call's own owner holds no payload: the kept vector was prepaid.
        assert_eq!(owner.held_with_call(), 0);
        assert_eq!(writer.usage().used, before);
        // A second kept vector finds no prepaid credit left and is charged.
        let (_second, _) = decode::<Kept>(&encoded(&values)).unwrap();
        assert_eq!(owner.adopted_output(), 1536 * 4);
        assert_eq!(writer.usage().used, before + 1536 * 4);
    }

    #[test]
    fn a_per_call_reader_holds_one_decode_at_a_time_and_returns_it_on_drop() {
        let values: Vec<f32> = (0..1536).map(|lane| lane as f32).collect();
        let declared = crate::vector_read_memory::DeclaredReadMemory::from_reader_limit(1 << 20);
        let reader = declared.child().unwrap();
        let owner = ImageMemory::charging_reader_per_call(accountant(1 << 20), reader.clone());
        let before = declared.retained_bytes_for_test();
        {
            let _scope = owner.enter();
            // A streamed scan: each body is decoded, scored and dropped.
            for _ in 0..64 {
                let (decoded, _) = decode::<Exact>(&encoded(&values)).unwrap();
                drop(decoded);
            }
            assert_eq!(owner.held(), 0, "the request owner itself holds nothing");
            assert_eq!(
                owner.held_with_call(),
                1536 * 4,
                "only the latest call's decode is still charged"
            );
            assert_eq!(declared.retained_bytes_for_test(), before + 1536 * 4);
        }
        drop(owner);
        assert_eq!(
            declared.retained_bytes_for_test(),
            before,
            "the last call's charge returns when the request owner drops"
        );
        drop(reader);
        assert_eq!(declared.retained_bytes_for_test(), 0);
    }

    fn length_prefix_u32(length: u32) -> Vec<u8> {
        let mut bytes = vec![252];
        bytes.extend_from_slice(&length.to_le_bytes());
        bytes
    }

    #[test]
    fn a_malformed_length_is_refused_before_anything_is_allocated() {
        let owner = ImageMemory::operation_writer(accountant(1 << 20)).unwrap();
        let _scope = owner.enter();
        // One billion f32 values claimed, none present: the 4 GB admission
        // is refused as the typed budget refusal, and nothing stays charged.
        let refused = decode::<Exact>(&length_prefix_u32(1_000_000_000))
            .err()
            .expect("refused");
        assert!(
            matches!(refused, Error::MemoryBudgetExceeded { requested_bytes, .. } if requested_bytes == 4_000_000_000),
            "{refused:?}"
        );
        assert_eq!(owner.held(), 0);
    }

    #[test]
    fn an_overflowing_length_is_refused_without_a_panic() {
        let owner = ImageMemory::operation_writer(accountant(1 << 20)).unwrap();
        let _scope = owner.enter();
        let mut bytes = vec![253];
        bytes.extend_from_slice(&u64::MAX.to_le_bytes());
        let refused = decode::<Exact>(&bytes).err().expect("refused");
        assert!(
            matches!(&refused, Error::Other(reason) if reason.contains("exact sequence length overflows")),
            "{refused:?}"
        );
        assert_eq!(owner.held(), 0);
    }

    #[test]
    fn a_truncated_sequence_is_refused_and_returns_its_charge() {
        let owner = ImageMemory::operation_writer(accountant(1 << 20)).unwrap();
        let _scope = owner.enter();
        let mut bytes = encoded(&[1.0, 2.0, 3.0, 4.0, 5.0]);
        bytes.truncate(bytes.len() - 8);
        assert!(decode::<Exact>(&bytes).is_err());
        assert_eq!(owner.held(), 0, "a failed decode returns its admission");
    }
}

#[cfg(test)]
mod admitted_reader_tests {
    use super::*;
    use contextdb_core::read_memory::ReadMemoryBudget;
    use std::sync::atomic::AtomicUsize;

    /// A reader budget that records what it holds.
    #[derive(Debug)]
    struct Counted {
        held: AtomicUsize,
        limit: usize,
    }
    impl ReadMemoryBudget for Counted {
        fn try_reserve(
            &self,
            bytes: usize,
            subsystem: &'static str,
            operation: &'static str,
            hint: &'static str,
        ) -> Result<()> {
            let held = self.held.load(Ordering::SeqCst);
            if held + bytes > self.limit {
                return Err(Error::MemoryBudgetExceeded {
                    subsystem: subsystem.into(),
                    operation: operation.into(),
                    requested_bytes: bytes,
                    available_bytes: self.limit - held,
                    budget_limit_bytes: self.limit,
                    hint: hint.into(),
                });
            }
            self.held.fetch_add(bytes, Ordering::SeqCst);
            Ok(())
        }
        fn release(&self, bytes: usize) {
            self.held.fetch_sub(bytes, Ordering::SeqCst);
        }
    }
    fn budget(limit: usize) -> Arc<Counted> {
        Arc::new(Counted {
            held: AtomicUsize::new(0),
            limit,
        })
    }
    fn reader(budget: &Arc<Counted>) -> Result<Arc<ImageMemory>> {
        ImageMemory::admitted_reader_for(
            ImageMemory::default_decode_accountant(),
            budget.clone(),
            "reader",
            "decode_custody_metadata",
            "Reduce selected custody metadata.",
        )
    }

    /// The control is admitted before the shared owner exists; a reader that
    /// cannot fund it is refused with nothing held.
    #[test]
    fn an_admitted_reader_is_refused_before_its_owner_exists() {
        let control = ImageMemory::admitted_control_bytes();
        let short = budget(control - 1);
        let refused = reader(&short);
        assert!(matches!(
            refused,
            Err(Error::MemoryBudgetExceeded { requested_bytes, .. }) if requested_bytes == control
        ));
        assert_eq!(short.held.load(Ordering::SeqCst), 0);
    }

    /// Every holder but the final one leaves the control charged; the final
    /// holder's drop returns the reader to exactly what it held before.
    #[test]
    fn the_final_holder_releases_the_admitted_control() {
        let control = ImageMemory::admitted_control_bytes();
        let funded = budget(1 << 20);
        let first = reader(&funded).unwrap();
        assert_eq!(funded.held.load(Ordering::SeqCst), control);
        assert_eq!(first.held(), control);
        let second = first.clone();
        let third = first.clone();
        drop(first);
        drop(third);
        assert_eq!(funded.held.load(Ordering::SeqCst), control);
        drop(second);
        assert_eq!(funded.held.load(Ordering::SeqCst), 0);
    }

    /// A refusal during a decode is kept by value and rendered once taken;
    /// malformed input answers the bounded static class text.
    #[test]
    fn an_admitted_reader_refuses_by_value_and_bounds_codec_text() {
        let control = ImageMemory::admitted_control_bytes();
        let funded = budget(control + 8);
        let memory = reader(&funded).unwrap();
        let long = rmp_serde::to_vec(&"x".repeat(4096)).unwrap();
        let refused = memory.decode_msgpack::<String>(&long);
        assert!(matches!(refused, Err(Error::MemoryBudgetExceeded { .. })));
        assert!(memory.failure.lock().is_none());
        assert_eq!(memory.held(), control);
        let malformed = memory.decode_msgpack::<String>(&[0xc1]);
        match malformed {
            Err(Error::SyncError(text)) => assert_eq!(text, ADMITTED_CODEC_ERROR),
            other => panic!("malformed input answered {other:?}"),
        }
        drop(memory);
        assert_eq!(funded.held.load(Ordering::SeqCst), 0);
    }
}

#[cfg(test)]
mod native_decode_tests {
    use super::*;
    use crate::vector_read_memory::DeclaredReadMemory;
    use contextdb_core::read_memory::{NativeRefusal, ReadShared};

    fn native_owner(
        parent: &Arc<DeclaredReadMemory>,
    ) -> contextdb_core::read_memory::ReadMemoryOwner {
        parent.native_child().expect("native owner").into_owner()
    }

    // The accountant's control charge follows its block's last holder: a
    // second holder keeps exactly the block charged after the first drops,
    // and the charge returns to zero only when the last holder frees it.
    #[test]
    fn the_native_decode_accountant_settles_after_its_last_holder_frees_it() {
        let parent = DeclaredReadMemory::from_reader_limit(1 << 20);
        let owner = native_owner(&parent);
        let baseline = parent.retained_bytes_for_test();
        let first = NativeDecodeMemory::try_new(owner.clone()).expect("accountant");
        let block = ReadShared::<NativeDecodeMemory>::layout().size() as u64;
        assert_eq!(parent.retained_bytes_for_test() - baseline, block);
        let second = first.clone();
        drop(first);
        assert_eq!(parent.retained_bytes_for_test() - baseline, block);
        let encoded = rmp_serde::to_vec(&vec!["decoded".to_string()]).unwrap();
        let value: Vec<String> = decode_msgpack_native(&second, &encoded).expect("decode");
        let held = second.held() as u64;
        assert!(held > 0, "decoded bytes are admitted on the request owner");
        assert_eq!(parent.retained_bytes_for_test() - baseline, block + held);
        drop(value);
        drop(second);
        assert_eq!(parent.retained_bytes_for_test(), baseline);
        drop(owner);
        assert_eq!(parent.retained_bytes_for_test(), 0);
    }

    // A refusal inside the decode is kept by value, with no owned diagnostic,
    // and every byte admitted for the failed decode is returned.
    #[test]
    fn a_refused_native_decode_returns_the_refusal_by_value_and_its_bytes() {
        let block = ReadShared::<NativeDecodeMemory>::layout().size() as u64;
        let probe = DeclaredReadMemory::from_reader_limit(1 << 20);
        let child = {
            let owner = native_owner(&probe);
            let control = probe.retained_bytes_for_test();
            drop(owner);
            control
        };
        let parent = DeclaredReadMemory::from_reader_limit(child + block + 64);
        let owner = native_owner(&parent);
        let memory = NativeDecodeMemory::try_new(owner.clone()).expect("accountant");
        let encoded = rmp_serde::to_vec(&vec!["x".repeat(4096)]).unwrap();
        let refused = decode_msgpack_native::<Vec<String>>(&memory, &encoded);
        assert!(
            matches!(
                refused,
                Err(NativeDecodeFailure::Refused(
                    NativeRefusal::MemoryBudgetExceeded {
                        subsystem: "reader",
                        operation: "decode_sync_response",
                        ..
                    }
                ))
            ),
            "{refused:?}"
        );
        assert_eq!(memory.held(), 0);
        assert_eq!(parent.retained_bytes_for_test(), child + block);
        drop(memory);
        drop(owner);
        assert_eq!(parent.retained_bytes_for_test(), 0);
    }

    // Malformed input answers the static codec class; nothing stays admitted.
    #[test]
    fn a_malformed_native_decode_answers_the_codec_class_and_retains_nothing() {
        let parent = DeclaredReadMemory::from_reader_limit(1 << 20);
        let owner = native_owner(&parent);
        let memory = NativeDecodeMemory::try_new(owner.clone()).expect("accountant");
        let failed = decode_msgpack_native::<Vec<String>>(&memory, &[0x92, 0xa3, b'a']);
        assert_eq!(failed, Err(NativeDecodeFailure::Codec));
        assert_eq!(memory.held(), 0);
        drop(memory);
        drop(owner);
        assert_eq!(parent.retained_bytes_for_test(), 0);
    }
}
