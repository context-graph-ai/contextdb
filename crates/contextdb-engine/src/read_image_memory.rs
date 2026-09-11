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
            held: AtomicUsize::new(0),
            failure: parking_lot::Mutex::new(None),
        })
    }
    pub(crate) fn reserve(&self, bytes: usize) -> Result<()> {
        self.accountant.try_allocate_for(
            bytes,
            "direct_read",
            "hydrate_owned_image",
            "Raise MEMORY_LIMIT to admit the complete committed read image.",
        )?;
        self.held.fetch_add(bytes, Ordering::SeqCst);
        Ok(())
    }
    pub(crate) fn release(&self, bytes: usize) {
        self.held.fetch_sub(bytes, Ordering::SeqCst);
        self.accountant.release(bytes);
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
/// Source-page allocations may be evicted and reloaded during hydration.
/// Conservatively own every admission until all source handles are closed.
struct SourceMemory {
    image: Arc<ImageMemory>,
    held: AtomicUsize,
}
impl Drop for SourceMemory {
    fn drop(&mut self) {
        self.image.release(self.held.load(Ordering::SeqCst));
    }
}
pub(crate) fn with_source_memory<R>(image: Arc<ImageMemory>, operation: impl FnOnce() -> R) -> R {
    let source = Arc::new(SourceMemory {
        image,
        held: AtomicUsize::new(0),
    });
    redb::with_read_memory_admission(
        Arc::new(move |bytes| match source.image.reserve(bytes) {
            Ok(()) => {
                source.held.fetch_add(bytes, Ordering::SeqCst);
                Ok(())
            }
            Err(error) => {
                *source.image.failure.lock() = Some(error);
                Err(std::io::Error::from(std::io::ErrorKind::OutOfMemory))
            }
        }),
        operation,
    )
}

impl Drop for ImageMemory {
    fn drop(&mut self) {
        self.accountant.release(self.held.load(Ordering::SeqCst));
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

// Size canonical JSON workspace without materializing a document or encoded
// buffer. Text escaping and byte-to-JSON expansion are counted by serde itself.
pub(crate) fn canonical_workspace<T: serde::Serialize>(values: &[T]) -> Result<usize> {
    struct Count(usize);
    impl std::io::Write for Count {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 = self
                .0
                .checked_add(bytes.len())
                .ok_or_else(|| std::io::Error::other("canonical JSON length overflow"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut retained = values.len().saturating_mul(std::mem::size_of::<Vec<u8>>());
    let mut workspace = 0usize;
    for value in values {
        let mut encoded = Count(0);
        serde_json::to_writer(&mut encoded, value).map_err(|e| Error::Other(e.to_string()))?;
        let document = value
            .serialize(JsonMemory)
            .map_err(|e| Error::Other(e.to_string()))?;
        // Vec's geometric growth can hold its old and replacement buffer
        // simultaneously. The final buffers of earlier records remain owned.
        retained = retained.saturating_add(encoded.0.saturating_mul(2).max(128));
        workspace = workspace.max(document.saturating_add(encoded.0));
    }
    Ok(retained.saturating_add(workspace))
}

struct JsonMemory;
struct JsonCollection {
    bytes: usize,
    element_bytes: usize,
}
impl JsonCollection {
    fn element<T: ?Sized + serde::Serialize>(
        &mut self,
        value: &T,
    ) -> std::result::Result<(), serde_json::Error> {
        self.bytes = self
            .bytes
            .saturating_add(self.element_bytes)
            .saturating_add(value.serialize(JsonMemory)?);
        Ok(())
    }
}
impl serde::Serializer for JsonMemory {
    type Ok = usize;
    type Error = serde_json::Error;
    type SerializeSeq = JsonCollection;
    type SerializeTuple = JsonCollection;
    type SerializeTupleStruct = JsonCollection;
    type SerializeTupleVariant = JsonCollection;
    type SerializeMap = JsonCollection;
    type SerializeStruct = JsonCollection;
    type SerializeStructVariant = JsonCollection;
    fn serialize_bool(self, _: bool) -> std::result::Result<usize, Self::Error> {
        Ok(0)
    }
    fn serialize_i8(self, _: i8) -> std::result::Result<usize, Self::Error> {
        Ok(0)
    }
    fn serialize_i16(self, _: i16) -> std::result::Result<usize, Self::Error> {
        Ok(0)
    }
    fn serialize_i32(self, _: i32) -> std::result::Result<usize, Self::Error> {
        Ok(0)
    }
    fn serialize_i64(self, _: i64) -> std::result::Result<usize, Self::Error> {
        Ok(0)
    }
    fn serialize_i128(self, _: i128) -> std::result::Result<usize, Self::Error> {
        Ok(0)
    }
    fn serialize_u8(self, _: u8) -> std::result::Result<usize, Self::Error> {
        Ok(0)
    }
    fn serialize_u16(self, _: u16) -> std::result::Result<usize, Self::Error> {
        Ok(0)
    }
    fn serialize_u32(self, _: u32) -> std::result::Result<usize, Self::Error> {
        Ok(0)
    }
    fn serialize_u64(self, _: u64) -> std::result::Result<usize, Self::Error> {
        Ok(0)
    }
    fn serialize_u128(self, _: u128) -> std::result::Result<usize, Self::Error> {
        Ok(0)
    }
    fn serialize_f32(self, _: f32) -> std::result::Result<usize, Self::Error> {
        Ok(0)
    }
    fn serialize_f64(self, _: f64) -> std::result::Result<usize, Self::Error> {
        Ok(0)
    }
    fn serialize_char(self, value: char) -> std::result::Result<usize, Self::Error> {
        Ok(value.len_utf8())
    }
    fn serialize_str(self, value: &str) -> std::result::Result<usize, Self::Error> {
        Ok(value.len())
    }
    fn serialize_bytes(self, value: &[u8]) -> std::result::Result<usize, Self::Error> {
        Ok(value
            .len()
            .saturating_mul(std::mem::size_of::<serde_json::Value>()))
    }
    fn serialize_none(self) -> std::result::Result<usize, Self::Error> {
        Ok(0)
    }
    fn serialize_some<T: ?Sized + serde::Serialize>(
        self,
        value: &T,
    ) -> std::result::Result<usize, Self::Error> {
        value.serialize(self)
    }
    fn serialize_unit(self) -> std::result::Result<usize, Self::Error> {
        Ok(0)
    }
    fn serialize_unit_struct(self, _: &'static str) -> std::result::Result<usize, Self::Error> {
        Ok(0)
    }
    fn serialize_unit_variant(
        self,
        _: &'static str,
        _: u32,
        variant: &'static str,
    ) -> std::result::Result<usize, Self::Error> {
        Ok(variant.len())
    }
    fn serialize_newtype_struct<T: ?Sized + serde::Serialize>(
        self,
        _: &'static str,
        value: &T,
    ) -> std::result::Result<usize, Self::Error> {
        value.serialize(self)
    }
    fn serialize_newtype_variant<T: ?Sized + serde::Serialize>(
        self,
        _: &'static str,
        _: u32,
        variant: &'static str,
        value: &T,
    ) -> std::result::Result<usize, Self::Error> {
        Ok(1024usize
            .saturating_add(variant.len())
            .saturating_add(value.serialize(self)?))
    }
    fn serialize_seq(self, len: Option<usize>) -> std::result::Result<JsonCollection, Self::Error> {
        Ok(JsonCollection {
            bytes: len
                .unwrap_or(4)
                .saturating_mul(std::mem::size_of::<serde_json::Value>()),
            element_bytes: if len.is_some() {
                0
            } else {
                4 * std::mem::size_of::<serde_json::Value>()
            },
        })
    }
    fn serialize_tuple(self, len: usize) -> std::result::Result<JsonCollection, Self::Error> {
        self.serialize_seq(Some(len))
    }
    fn serialize_tuple_struct(
        self,
        _: &'static str,
        len: usize,
    ) -> std::result::Result<JsonCollection, Self::Error> {
        self.serialize_seq(Some(len))
    }
    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        variant: &'static str,
        len: usize,
    ) -> std::result::Result<JsonCollection, Self::Error> {
        let mut result = self.serialize_seq(Some(len))?;
        result.bytes = result
            .bytes
            .saturating_add(1024)
            .saturating_add(variant.len());
        Ok(result)
    }
    fn serialize_map(self, _: Option<usize>) -> std::result::Result<JsonCollection, Self::Error> {
        Ok(JsonCollection {
            bytes: 1024,
            element_bytes: 128,
        })
    }
    fn serialize_struct(
        self,
        _: &'static str,
        len: usize,
    ) -> std::result::Result<JsonCollection, Self::Error> {
        self.serialize_map(Some(len))
    }
    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        variant: &'static str,
        len: usize,
    ) -> std::result::Result<JsonCollection, Self::Error> {
        let mut result = self.serialize_map(Some(len))?;
        result.bytes = result
            .bytes
            .saturating_add(1024)
            .saturating_add(variant.len());
        Ok(result)
    }
    fn collect_str<T: ?Sized + std::fmt::Display>(
        self,
        value: &T,
    ) -> std::result::Result<usize, Self::Error> {
        struct Length(usize);
        impl std::fmt::Write for Length {
            fn write_str(&mut self, value: &str) -> std::fmt::Result {
                self.0 = self.0.saturating_add(value.len());
                Ok(())
            }
        }
        let mut length = Length(0);
        std::fmt::write(&mut length, format_args!("{value}")).map_err(serde::ser::Error::custom)?;
        Ok(length.0.saturating_mul(3).max(8))
    }
}
impl serde::ser::SerializeSeq for JsonCollection {
    type Ok = usize;
    type Error = serde_json::Error;
    fn serialize_element<T: ?Sized + serde::Serialize>(
        &mut self,
        value: &T,
    ) -> std::result::Result<(), Self::Error> {
        self.element(value)
    }
    fn end(self) -> std::result::Result<usize, Self::Error> {
        Ok(self.bytes)
    }
}
impl serde::ser::SerializeTuple for JsonCollection {
    type Ok = usize;
    type Error = serde_json::Error;
    fn serialize_element<T: ?Sized + serde::Serialize>(
        &mut self,
        value: &T,
    ) -> std::result::Result<(), Self::Error> {
        self.element(value)
    }
    fn end(self) -> std::result::Result<usize, Self::Error> {
        Ok(self.bytes)
    }
}
impl serde::ser::SerializeTupleStruct for JsonCollection {
    type Ok = usize;
    type Error = serde_json::Error;
    fn serialize_field<T: ?Sized + serde::Serialize>(
        &mut self,
        value: &T,
    ) -> std::result::Result<(), Self::Error> {
        self.element(value)
    }
    fn end(self) -> std::result::Result<usize, Self::Error> {
        Ok(self.bytes)
    }
}
impl serde::ser::SerializeTupleVariant for JsonCollection {
    type Ok = usize;
    type Error = serde_json::Error;
    fn serialize_field<T: ?Sized + serde::Serialize>(
        &mut self,
        value: &T,
    ) -> std::result::Result<(), Self::Error> {
        self.element(value)
    }
    fn end(self) -> std::result::Result<usize, Self::Error> {
        Ok(self.bytes)
    }
}
impl serde::ser::SerializeMap for JsonCollection {
    type Ok = usize;
    type Error = serde_json::Error;
    fn serialize_key<T: ?Sized + serde::Serialize>(
        &mut self,
        key: &T,
    ) -> std::result::Result<(), Self::Error> {
        // Numeric map keys are rendered as owned decimal strings.
        self.bytes = self
            .bytes
            .saturating_add(40)
            .saturating_add(key.serialize(JsonMemory)?);
        Ok(())
    }
    fn serialize_value<T: ?Sized + serde::Serialize>(
        &mut self,
        value: &T,
    ) -> std::result::Result<(), Self::Error> {
        self.element(value)
    }
    fn end(self) -> std::result::Result<usize, Self::Error> {
        Ok(self.bytes)
    }
}
impl serde::ser::SerializeStruct for JsonCollection {
    type Ok = usize;
    type Error = serde_json::Error;
    fn serialize_field<T: ?Sized + serde::Serialize>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> std::result::Result<(), Self::Error> {
        self.bytes = self.bytes.saturating_add(key.len());
        self.element(value)
    }
    fn end(self) -> std::result::Result<usize, Self::Error> {
        Ok(self.bytes)
    }
}
impl serde::ser::SerializeStructVariant for JsonCollection {
    type Ok = usize;
    type Error = serde_json::Error;
    fn serialize_field<T: ?Sized + serde::Serialize>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> std::result::Result<(), Self::Error> {
        self.bytes = self.bytes.saturating_add(key.len());
        self.element(value)
    }
    fn end(self) -> std::result::Result<usize, Self::Error> {
        Ok(self.bytes)
    }
}
