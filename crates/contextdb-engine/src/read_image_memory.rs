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
    pub(crate) accountant: Arc<MemoryAccountant>,
    subsystem: &'static str,
    operation: &'static str,
    hint: &'static str,
    reader_budget: Option<Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>>,
    held: AtomicUsize,
    failure: parking_lot::Mutex<Option<Error>>,
}
impl ImageMemory {
    pub(crate) fn new(limit: Option<usize>) -> Arc<Self> {
        Arc::new(Self {
            accountant: Arc::new(match limit {
                Some(limit) => MemoryAccountant::with_budget(limit),
                None => MemoryAccountant::no_limit(),
            }),
            subsystem: "direct_read",
            operation: "hydrate_owned_image",
            hint: "Raise MEMORY_LIMIT to admit the complete committed read image.",
            reader_budget: None,
            held: AtomicUsize::new(0),
            failure: parking_lot::Mutex::new(None),
        })
    }
    /// Admission into an accountant that already exists -- a reading
    /// session's own -- so what is decoded under [`Self::scope`] is charged
    /// where the session's limit sees it, allocation by allocation, and given
    /// back when this charge is dropped.
    pub(crate) fn charging(accountant: Arc<MemoryAccountant>) -> Arc<Self> {
        Self::charging_for(
            accountant,
            "direct_read",
            "hydrate_owned_image",
            "Raise MEMORY_LIMIT to admit the complete committed read image.",
        )
    }
    pub(crate) fn charging_for(
        accountant: Arc<MemoryAccountant>,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> Arc<Self> {
        Arc::new(Self {
            accountant,
            subsystem,
            operation,
            hint,
            reader_budget: None,
            held: AtomicUsize::new(0),
            failure: parking_lot::Mutex::new(None),
        })
    }
    /// A durable source window belongs to the requesting read when present.
    /// Internal writer operations continue to own their scratch directly.
    pub(crate) fn charging_reader(
        accountant: Arc<MemoryAccountant>,
        reader_budget: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Arc<Self> {
        Arc::new(Self {
            accountant,
            subsystem: "reader",
            operation: "materialize_answer",
            hint: "Reduce the statement's output or raise the declared reader memory limit.",
            reader_budget: Some(reader_budget),
            held: AtomicUsize::new(0),
            failure: parking_lot::Mutex::new(None),
        })
    }

    /// Explicit reader funding with the source operation's existing labels.
    /// The accountant is retained for image bridge identity only; funding
    /// always uses the required reader capability.
    pub(crate) fn charging_reader_for(
        accountant: Arc<MemoryAccountant>,
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
            reader_budget: Some(reader_budget),
            held: AtomicUsize::new(0),
            failure: parking_lot::Mutex::new(None),
        })
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
                "answer credit has a different reader budget".into(),
            ));
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
        let result = self.scope(|| {
            let mut decoder = rmp_serde::Deserializer::from_read_ref(bytes);
            Seed(PhantomData::<T>).deserialize(&mut decoder)
        });
        if let Some(error) = self.take_failure() {
            return Err(error);
        }
        result.map_err(|error| Error::SyncError(format!("MessagePack decode error: {error}")))
    }

    pub(crate) fn held(&self) -> usize {
        self.held.load(Ordering::SeqCst)
    }
    pub(crate) fn take_failure(&self) -> Option<Error> {
        self.failure.lock().take()
    }
    pub(crate) fn scope<T>(self: &Arc<Self>, operation: impl FnOnce() -> T) -> T {
        ACTIVE.with(|active| active.borrow_mut().push(self.clone()));
        struct Reset;
        impl Drop for Reset {
            fn drop(&mut self) {
                ACTIVE.with(|a| {
                    a.borrow_mut().pop();
                });
            }
        }
        let _reset = Reset;
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
        self.reserve(bytes)
    }
    fn release(&self, bytes: usize) {
        ImageMemory::release(self, bytes);
    }
}

impl Drop for ImageMemory {
    fn drop(&mut self) {
        self.release(self.held.load(Ordering::SeqCst));
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

thread_local! { static ACTIVE: RefCell<Vec<Arc<ImageMemory>>> = const { RefCell::new(Vec::new()) }; }
pub(crate) fn reserve(bytes: usize) -> Result<()> {
    ACTIVE.with(|active| match active.borrow().last() {
        Some(owner) => owner.reserve(bytes),
        None => Ok(()),
    })
}
fn admit<E: de::Error>(bytes: usize) -> std::result::Result<(), E> {
    ACTIVE.with(|active| {
        let active = active.borrow();
        if let Some(owner) = active.last() {
            if owner.failure.lock().is_some() {
                return Err(E::custom("read image admission refused"));
            }
            if let Err(error) = owner.reserve(bytes) {
                *owner.failure.lock() = Some(error);
                return Err(E::custom("read image admission refused"));
            }
        }
        Ok(())
    })
}
pub(crate) fn decode<T: de::DeserializeOwned>(bytes: &[u8]) -> Result<(T, usize)> {
    let active = ACTIVE.with(|a| a.borrow().last().cloned());
    let Some(owner) = active else {
        return bincode::serde::decode_from_slice(bytes, bincode::config::standard())
            .map_err(|error| Error::Other(format!("bincode decode error: {error}")));
    };
    // An outer Vec/Map can grow while the decoded record is being inserted.
    // Four slots cover that growth plus the record held by the decoder.
    owner.reserve(
        std::mem::size_of::<T>()
            .saturating_mul(4)
            .saturating_add(128),
    )?;
    let result = bincode::serde::seed_decode_from_slice(
        Seed(PhantomData::<T>),
        bytes,
        bincode::config::standard(),
    );
    if let Some(error) = owner.take_failure() {
        return Err(error);
    }
    result.map_err(|error| Error::Other(format!("bincode decode error: {error}")))
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
        self.inner.visit_bool(value)
    }
    fn visit_i8<E: de::Error>(self, value: i8) -> std::result::Result<Self::Value, E> {
        self.inner.visit_i8(value)
    }
    fn visit_i16<E: de::Error>(self, value: i16) -> std::result::Result<Self::Value, E> {
        self.inner.visit_i16(value)
    }
    fn visit_i32<E: de::Error>(self, value: i32) -> std::result::Result<Self::Value, E> {
        self.inner.visit_i32(value)
    }
    fn visit_i64<E: de::Error>(self, value: i64) -> std::result::Result<Self::Value, E> {
        self.inner.visit_i64(value)
    }
    fn visit_i128<E: de::Error>(self, value: i128) -> std::result::Result<Self::Value, E> {
        self.inner.visit_i128(value)
    }
    fn visit_u8<E: de::Error>(self, value: u8) -> std::result::Result<Self::Value, E> {
        self.inner.visit_u8(value)
    }
    fn visit_u16<E: de::Error>(self, value: u16) -> std::result::Result<Self::Value, E> {
        self.inner.visit_u16(value)
    }
    fn visit_u32<E: de::Error>(self, value: u32) -> std::result::Result<Self::Value, E> {
        self.inner.visit_u32(value)
    }
    fn visit_u64<E: de::Error>(self, value: u64) -> std::result::Result<Self::Value, E> {
        self.inner.visit_u64(value)
    }
    fn visit_u128<E: de::Error>(self, value: u128) -> std::result::Result<Self::Value, E> {
        self.inner.visit_u128(value)
    }
    fn visit_f32<E: de::Error>(self, value: f32) -> std::result::Result<Self::Value, E> {
        self.inner.visit_f32(value)
    }
    fn visit_f64<E: de::Error>(self, value: f64) -> std::result::Result<Self::Value, E> {
        self.inner.visit_f64(value)
    }
    fn visit_char<E: de::Error>(self, value: char) -> std::result::Result<Self::Value, E> {
        self.inner.visit_char(value)
    }
    fn visit_unit<E: de::Error>(self) -> std::result::Result<Self::Value, E> {
        self.inner.visit_unit()
    }
    fn visit_none<E: de::Error>(self) -> std::result::Result<Self::Value, E> {
        self.inner.visit_none()
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
        admit(value.len())?;
        self.inner.visit_str(value)
    }
    fn visit_borrowed_str<E: de::Error>(
        self,
        value: &'de str,
    ) -> std::result::Result<Self::Value, E> {
        admit(value.len())?;
        self.inner.visit_borrowed_str(value)
    }
    fn visit_bytes<E: de::Error>(self, value: &[u8]) -> std::result::Result<Self::Value, E> {
        admit(value.len())?;
        self.inner.visit_bytes(value)
    }
    fn visit_borrowed_bytes<E: de::Error>(
        self,
        value: &'de [u8],
    ) -> std::result::Result<Self::Value, E> {
        admit(value.len())?;
        self.inner.visit_borrowed_bytes(value)
    }
    fn visit_seq<A: de::SeqAccess<'de>>(self, a: A) -> std::result::Result<Self::Value, A::Error> {
        self.inner.visit_seq(Seq {
            inner: a,
            heap: self.heap,
            first: true,
        })
    }
    fn visit_map<A: de::MapAccess<'de>>(self, a: A) -> std::result::Result<Self::Value, A::Error> {
        self.inner.visit_map(Map {
            inner: a,
            first_key: true,
            first_value: true,
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
}
impl<'de, A: de::MapAccess<'de>> de::MapAccess<'de> for Map<A> {
    type Error = A::Error;
    fn next_key_seed<S: DeserializeSeed<'de>>(
        &mut self,
        seed: S,
    ) -> std::result::Result<Option<S::Value>, A::Error> {
        if self.inner.size_hint() != Some(0) {
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
        let slots = if self.first_value { 16 } else { 4 };
        admit(
            std::mem::size_of::<S::Value>()
                .saturating_mul(slots)
                .saturating_add(64),
        )?;
        self.first_value = false;
        self.inner.next_value_seed(Seed(seed))
    }
    fn size_hint(&self) -> Option<usize> {
        Some(0)
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
