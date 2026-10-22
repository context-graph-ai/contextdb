//! Incremental admission for the pinned serde_json 1.0.149 string reader.
//! Serde remains the grammar, recursion-limit and number-conversion authority.
use crate::read_memory::{NativeRefusal, ReadCredit, ReadMemoryBudget, ReadMemoryOwner};
use serde::de::{DeserializeSeed, Error as _, MapAccess, SeqAccess, Visitor};
use serde_json::{Map, Number, Value};
use std::cell::RefCell;
use std::fmt;
use std::marker::PhantomData;
use std::rc::Rc;

const ADMISSION_ERROR: &str = "JSON allocation admission failed";
const MISSING_OWNER: &str = "JSON decode requires an operation memory owner";
const CAPACITY_ERROR: &str = "JSON allocation capacity differs from pinned growth";
const MAP_NODE: usize = 728;
// Owner-approved pinned lexical bound: at most six 1200-byte limb buffers,
// including native growth overlap. Separate from parser scratch and input.
const NUMBER_WORK_BYTES: usize = 7_200;

thread_local! {
    static CLOSED_HOOKS: RefCell<Option<ClosedJsonReadHooks>> = const { RefCell::new(None) };
    static CLOSED_FAILURE: RefCell<Option<ClosedState>> = const { RefCell::new(None) };
    static OWNER: RefCell<Option<ReadMemoryOwner>> = const { RefCell::new(None) };
    static NATIVE_OWNER: RefCell<Option<ReadMemoryOwner>> = const { RefCell::new(None) };
}

/// Internal, thread-affine bridge from the engine's row lease into core serde.
#[doc(hidden)]
pub struct JsonReadAdmissionGuard {
    previous: Option<crate::read_memory::ReadMemoryOwner>,
    previous_hooks: Option<ClosedJsonReadHooks>,
    previous_native: Option<ReadMemoryOwner>,
    _thread: PhantomData<Rc<()>>,
}
impl JsonReadAdmissionGuard {
    pub fn enter(owner: impl Into<crate::read_memory::ReadMemoryOwner>) -> Self {
        let owner: crate::read_memory::ReadMemoryOwner = owner.into();

        let previous = OWNER.with(|slot| slot.replace(Some(owner)));
        Self {
            previous,
            previous_native: NATIVE_OWNER.with(|slot| slot.replace(None)),
            previous_hooks: CLOSED_HOOKS.with(|slot| slot.replace(None)),
            _thread: PhantomData,
        }
    }
    /// Native companion; retains the exact original capability inline.
    #[doc(hidden)]
    pub fn enter_owned(owner: ReadMemoryOwner) -> Self {
        let mut guard = Self::enter_owned_empty();
        guard.previous_native = NATIVE_OWNER.with(|slot| slot.replace(Some(owner)));
        guard
    }
    fn enter_owned_empty() -> Self {
        Self {
            previous: OWNER.with(|slot| slot.replace(None)),
            previous_native: None,
            previous_hooks: CLOSED_HOOKS.with(|slot| slot.replace(None)),
            _thread: PhantomData,
        }
    }
    /// Select only a concrete engine root whose outer adapter owns these
    /// callbacks. This is refusal/diagnostic transport, not calibration proof.
    #[doc(hidden)]
    pub fn enter_closed(
        owner: impl Into<crate::read_memory::ReadMemoryOwner>,
        hooks: ClosedJsonReadHooks,
    ) -> Self {
        let owner: crate::read_memory::ReadMemoryOwner = owner.into();

        let guard = Self::enter(owner);
        CLOSED_HOOKS.with(|slot| {
            slot.replace(Some(hooks));
        });
        guard
    }
}
/// Private cross-crate callbacks into the existing operation ledger. Function
/// pointers cannot retain a caller or create an uncharged closure allocation.
#[derive(Clone, Copy)]
#[doc(hidden)]
pub struct ClosedJsonReadHooks {
    pub refused: fn(NativeRefusal),
    pub diagnostic: fn(serde_json::Error, Option<&'static str>) -> Result<(), serde_json::Error>,
}
impl ClosedJsonReadHooks {
    /// Maximum ONE native Message backing for this closed producer. Each
    /// explicit finite recipe passes &str to Error::custom: pinned std writes
    /// it once to an empty String (capacity = max(len, 8)); neither phrase has
    /// a coordinate suffix, so into_boxed_str cannot shrink/reallocate it.
    /// fix_position moves that backing; it never clones the Message.
    #[doc(hidden)]
    pub const fn native_message_bytes() -> usize {
        if CAPACITY_ERROR.len() > MISSING_OWNER.len() {
            CAPACITY_ERROR.len()
        } else {
            MISSING_OWNER.len()
        }
    }
}
pub(crate) fn has_closed_hooks() -> bool {
    CLOSED_HOOKS.with(|slot| slot.borrow().is_some())
}
pub(crate) fn capture_closed_json(error: ClosedJsonFailure) -> Result<(), serde_json::Error> {
    let hooks = CLOSED_HOOKS
        .with(|slot| *slot.borrow())
        .expect("closed JSON field remains within its producer");
    match error {
        ClosedJsonFailure::Refused(error) => {
            (hooks.refused)(error);
            Ok(())
        }
        ClosedJsonFailure::Native(error, finite) => (hooks.diagnostic)(error, finite),
    }
}
impl Drop for JsonReadAdmissionGuard {
    fn drop(&mut self) {
        OWNER.with(|slot| {
            slot.replace(self.previous.take());
        });
        CLOSED_HOOKS.with(|slot| {
            slot.replace(self.previous_hooks.take());
        });
        NATIVE_OWNER.with(|slot| {
            slot.replace(self.previous_native.take());
        });
    }
}

fn current_owner() -> Option<ReadMemoryOwner> {
    NATIVE_OWNER
        .with(|slot| slot.borrow().clone())
        .or_else(|| OWNER.with(|slot| slot.borrow().clone()))
}

// The outer engine serde adapter has already admitted this exact String.
// Free its native allocation before releasing its producer-owned credit.
pub(crate) fn release_serialized_input(input: String) {
    let capacity = input.capacity();
    drop(input);
    if let Some(owner) = current_owner() {
        owner.release(capacity);
    }
}

pub(crate) fn in_scope() -> bool {
    NATIVE_OWNER.with(|slot| slot.borrow().is_some()) || OWNER.with(|slot| slot.borrow().is_some())
}

/// Decode through the engine door; its caller must recover typed admission
/// failures from the installed budget's existing failure sideband.
#[doc(hidden)]
pub fn deserialize_json_in_read_scope(input: &str) -> serde_json::Result<Value> {
    let owner = current_owner().ok_or_else(|| serde_json::Error::custom(MISSING_OWNER))?;
    let mut state = BuildState::new(input, owner);
    let mut parser = serde_json::Deserializer::from_str(input);
    let result = ValueSeed { state: &mut state }.deserialize(&mut parser);
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
    // Native scratch must die before its credit. Partial values have already
    // been dropped on every error, before either payload credit is refunded.
    drop(parser);
    state
        .scratch
        .try_shrink(state.scratch.bytes())
        .map_err(|_| serde_json::Error::custom(ADMISSION_ERROR))?;
    match result {
        Ok(value) => {
            let retained = decoded_retained_heap_bytes(&value);
            if retained > state.payload.bytes() {
                drop(value);
                return Err(serde_json::Error::custom(CAPACITY_ERROR));
            }
            state
                .payload
                .try_shrink(state.payload.bytes() - retained)
                .map_err(|_| serde_json::Error::custom(ADMISSION_ERROR))?;
            // The same budget is the row's owner; the row lease now releases
            // this transferred retained charge at its existing last holder.
            state.payload.transfer();
            Ok(value)
        }
        Err(error) => Err(error),
    }
}

/// Private engine bridge. The selected engine caller prepays native workspace
/// before entry and retains it across this result and its outer conversion.
/// No arbitrary Deserialize visitor enters this path.
pub(crate) enum ClosedJsonFailure {
    Native(serde_json::Error, Option<&'static str>),
    Refused(NativeRefusal),
}
#[derive(Clone, Copy)]
struct ClosedState {
    refusal: Option<NativeRefusal>,
    finite: Option<&'static str>,
}
struct ClosedScope(Option<ClosedState>);
impl ClosedScope {
    fn enter() -> Self {
        Self(CLOSED_FAILURE.with(|slot| {
            slot.replace(Some(ClosedState {
                refusal: None,
                finite: None,
            }))
        }))
    }
    fn failure(&self) -> Option<NativeRefusal> {
        CLOSED_FAILURE.with(|slot| slot.borrow().as_ref().and_then(|state| state.refusal))
    }
    fn finite(&self) -> Option<&'static str> {
        CLOSED_FAILURE.with(|slot| slot.borrow().as_ref().and_then(|state| state.finite))
    }
}
impl Drop for ClosedScope {
    fn drop(&mut self) {
        CLOSED_FAILURE.with(|slot| {
            slot.replace(self.0.take());
        });
    }
}
fn latch_refusal(error: NativeRefusal) {
    CLOSED_FAILURE.with(|slot| {
        let mut slot = slot.borrow_mut();
        let failure = slot
            .as_mut()
            .expect("closed JSON refusal has an active scope");
        if failure.refusal.is_none() {
            failure.refusal = Some(error);
        }
    });
}
fn closed() -> bool {
    CLOSED_FAILURE.with(|slot| slot.borrow().is_some())
}
fn refused() -> bool {
    CLOSED_FAILURE.with(|slot| {
        slot.borrow()
            .as_ref()
            .is_some_and(|state| state.refusal.is_some())
    })
}

fn capacity_error<E: serde::de::Error>() -> E {
    CLOSED_FAILURE.with(|slot| {
        if let Some(state) = slot.borrow_mut().as_mut() {
            state.finite = Some(CAPACITY_ERROR);
        }
    });
    E::custom(CAPACITY_ERROR)
}

pub(crate) fn deserialize_json_closed(input: &str) -> Result<Value, ClosedJsonFailure> {
    let owner = current_owner().ok_or_else(|| {
        ClosedJsonFailure::Native(
            serde_json::Error::custom(MISSING_OWNER),
            Some(MISSING_OWNER),
        )
    })?;
    let scope = ClosedScope::enter();
    let mut state = BuildState::new(input, owner);
    let mut parser = serde_json::Deserializer::from_str(input);
    let result = ValueSeed { state: &mut state }.deserialize(Closed(&mut parser));
    // Container cleanup can construct a secondary native error even when a
    // visitor captured a refusal. The authentic scalar cause always wins.
    let result = if let Some(error) = scope.failure() {
        drop(result);
        Err(ClosedJsonFailure::Refused(error))
    } else {
        match result {
            Ok(value) => match parser.end() {
                Ok(()) => Ok(value),
                Err(error) => {
                    drop(value);
                    Err(ClosedJsonFailure::Native(error, scope.finite()))
                }
            },
            Err(ClosedError::Native(error)) => {
                Err(ClosedJsonFailure::Native(error, scope.finite()))
            }
            Err(ClosedError::Captured) => unreachable!("captured JSON refusal has a scalar cause"),
        }
    };
    drop(parser);
    // Neither a successful refund nor an accounting failure allocates text.
    // In particular, a cleanup failure must not replace the primary refusal.
    let scratch = state.scratch.bytes();
    if let Err(error) = state.scratch.try_shrink_native(scratch)
        && result.is_ok()
    {
        return Err(ClosedJsonFailure::Refused(error));
    }
    match result {
        Ok(value) => {
            let retained = decoded_retained_heap_bytes(&value);
            if retained > state.payload.bytes() {
                drop(value);
                // Preserve the genuine Data category and zero coordinates.
                // Its sole Message backing was prepaid by the selected caller.
                return Err(ClosedJsonFailure::Native(
                    capacity_error(),
                    Some(CAPACITY_ERROR),
                ));
            }
            if let Err(error) = state
                .payload
                .try_shrink_native(state.payload.bytes() - retained)
            {
                drop(value);
                return Err(ClosedJsonFailure::Refused(error));
            }
            state.payload.transfer();
            Ok(value)
        }
        Err(error) => Err(error),
    }
}

// Selected ValueSeed/KeySeed use only deserialize_any. Captured refusal travels
// as unit across the native visitor, and as Captured across seed/access edges.
// Ordinary native errors still cross as Err so fix_position retains its exact
// legacy message/category/coordinates. No generic visitor is certified here.
#[derive(Debug)]
enum ClosedError<E> {
    Native(E),
    Captured,
}
impl<E: fmt::Display> fmt::Display for ClosedError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Native(error) => fmt::Display::fmt(error, f),
            Self::Captured => f.write_str(ADMISSION_ERROR),
        }
    }
}
impl<E: serde::de::Error> std::error::Error for ClosedError<E> {}
impl<E: serde::de::Error> serde::de::Error for ClosedError<E> {
    fn custom<T: fmt::Display>(message: T) -> Self {
        if refused() {
            Self::Captured
        } else {
            Self::Native(E::custom(message))
        }
    }
}
fn closed_capture<T, E>(
    result: Result<T, ClosedError<E>>,
    output: &mut Option<T>,
) -> Result<(), E> {
    match result {
        Ok(value) => {
            *output = Some(value);
            Ok(())
        }
        Err(ClosedError::Native(error)) => Err(error),
        Err(ClosedError::Captured) => Ok(()),
    }
}
struct Closed<D>(D);
struct ClosedSeed<S>(S);
impl<'de, S: DeserializeSeed<'de>> DeserializeSeed<'de> for ClosedSeed<S> {
    type Value = Option<S::Value>;
    fn deserialize<D: serde::Deserializer<'de>>(self, de: D) -> Result<Self::Value, D::Error> {
        let mut output = None;
        closed_capture(self.0.deserialize(Closed(de)), &mut output)?;
        Ok(output)
    }
}
struct CaptureVisitor<'a, V, T> {
    inner: V,
    output: &'a mut Option<T>,
}
impl<'de, V: Visitor<'de>> Visitor<'de> for CaptureVisitor<'_, V, V::Value> {
    type Value = ();
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.expecting(f)
    }
    fn visit_bool<E: serde::de::Error>(self, value: bool) -> Result<(), E> {
        closed_capture(self.inner.visit_bool::<ClosedError<E>>(value), self.output)
    }
    fn visit_i64<E: serde::de::Error>(self, value: i64) -> Result<(), E> {
        closed_capture(self.inner.visit_i64::<ClosedError<E>>(value), self.output)
    }
    fn visit_u64<E: serde::de::Error>(self, value: u64) -> Result<(), E> {
        closed_capture(self.inner.visit_u64::<ClosedError<E>>(value), self.output)
    }
    fn visit_f64<E: serde::de::Error>(self, value: f64) -> Result<(), E> {
        closed_capture(self.inner.visit_f64::<ClosedError<E>>(value), self.output)
    }
    fn visit_unit<E: serde::de::Error>(self) -> Result<(), E> {
        closed_capture(self.inner.visit_unit::<ClosedError<E>>(), self.output)
    }
    fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<(), E> {
        closed_capture(self.inner.visit_str::<ClosedError<E>>(value), self.output)
    }
    fn visit_borrowed_str<E: serde::de::Error>(self, value: &'de str) -> Result<(), E> {
        closed_capture(
            self.inner.visit_borrowed_str::<ClosedError<E>>(value),
            self.output,
        )
    }
    fn visit_seq<A: SeqAccess<'de>>(self, access: A) -> Result<(), A::Error> {
        closed_capture(self.inner.visit_seq(ClosedAccess(access)), self.output)
    }
    fn visit_map<A: MapAccess<'de>>(self, access: A) -> Result<(), A::Error> {
        closed_capture(self.inner.visit_map(ClosedAccess(access)), self.output)
    }
}
struct ClosedAccess<A>(A);
impl<'de, A: SeqAccess<'de>> SeqAccess<'de> for ClosedAccess<A> {
    type Error = ClosedError<A::Error>;
    fn next_element_seed<S: DeserializeSeed<'de>>(
        &mut self,
        seed: S,
    ) -> Result<Option<S::Value>, Self::Error> {
        if refused() {
            return Err(ClosedError::Captured);
        }
        match self
            .0
            .next_element_seed(ClosedSeed(seed))
            .map_err(ClosedError::Native)?
        {
            Some(Some(value)) => Ok(Some(value)),
            None => Ok(None),
            Some(None) => Err(ClosedError::Captured),
        }
    }
    fn size_hint(&self) -> Option<usize> {
        self.0.size_hint()
    }
}
impl<'de, A: MapAccess<'de>> MapAccess<'de> for ClosedAccess<A> {
    type Error = ClosedError<A::Error>;
    fn next_key_seed<S: DeserializeSeed<'de>>(
        &mut self,
        seed: S,
    ) -> Result<Option<S::Value>, Self::Error> {
        if refused() {
            return Err(ClosedError::Captured);
        }
        match self
            .0
            .next_key_seed(ClosedSeed(seed))
            .map_err(ClosedError::Native)?
        {
            Some(Some(value)) => Ok(Some(value)),
            None => Ok(None),
            Some(None) => Err(ClosedError::Captured),
        }
    }
    fn next_value_seed<S: DeserializeSeed<'de>>(
        &mut self,
        seed: S,
    ) -> Result<S::Value, Self::Error> {
        if refused() {
            return Err(ClosedError::Captured);
        }
        self.0
            .next_value_seed(ClosedSeed(seed))
            .map_err(ClosedError::Native)?
            .ok_or(ClosedError::Captured)
    }
    fn size_hint(&self) -> Option<usize> {
        self.0.size_hint()
    }
}
impl<'de, D: serde::Deserializer<'de>> serde::Deserializer<'de> for Closed<D> {
    type Error = ClosedError<D::Error>;
    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        if refused() {
            return Err(ClosedError::Captured);
        }
        let mut output = None;
        self.0
            .deserialize_any(CaptureVisitor {
                inner: visitor,
                output: &mut output,
            })
            .map_err(ClosedError::Native)?;
        output.ok_or(ClosedError::Captured)
    }
    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char str string bytes
        byte_buf option unit unit_struct newtype_struct seq tuple tuple_struct
        map struct enum identifier ignored_any
    }
}

/// The approved retained BTreeMap node bound uses live unique members.
fn map_bytes(members: usize) -> usize {
    if members == 0 {
        0
    } else {
        (1 + (members - 1) / 5).saturating_mul(MAP_NODE)
    }
}
fn map_levels(members: usize) -> usize {
    if members == 0 {
        return 0;
    }
    let mut levels = 1;
    let mut minimum = 1usize;
    while let Some(next) = minimum
        .checked_add(1)
        .and_then(|n| n.checked_mul(6))
        .and_then(|n| n.checked_sub(1))
    {
        if next > members {
            break;
        }
        minimum = next;
        levels += 1;
    }
    levels
}

/// Exact payload capacities and the approved retained map-node bound.
/// Keep the shared estimator's recursive handling of host-created trees;
/// decoded-tree reconciliation uses the bounded inline traversal below.
#[doc(hidden)]
pub fn json_retained_heap_bytes(value: &Value) -> usize {
    match value {
        Value::String(text) => text.capacity(),
        Value::Array(items) => items
            .capacity()
            .saturating_mul(std::mem::size_of::<Value>())
            .saturating_add(items.iter().fold(0usize, |bytes, child| {
                bytes.saturating_add(json_retained_heap_bytes(child))
            })),
        Value::Object(members) => {
            members
                .iter()
                .fold(map_bytes(members.len()), |bytes, (key, child)| {
                    bytes
                        .saturating_add(key.capacity())
                        .saturating_add(json_retained_heap_bytes(child))
                })
        }
        _ => 0,
    }
}

fn decoded_retained_heap_bytes(root: &Value) -> usize {
    enum Frame<'a> {
        Array(std::slice::Iter<'a, Value>),
        Object(serde_json::map::Iter<'a>),
    }
    let mut frames: [Option<Frame<'_>>; 129] = std::array::from_fn(|_| None);
    let mut depth = 0usize;
    let mut next = Some(root);
    let mut bytes = 0usize;
    loop {
        if let Some(value) = next.take() {
            match value {
                Value::String(text) => bytes = bytes.saturating_add(text.capacity()),
                Value::Array(items) => {
                    bytes = bytes.saturating_add(
                        items
                            .capacity()
                            .saturating_mul(std::mem::size_of::<Value>()),
                    );
                    if depth == frames.len() {
                        return usize::MAX;
                    }
                    frames[depth] = Some(Frame::Array(items.iter()));
                    depth += 1;
                }
                Value::Object(members) => {
                    bytes = bytes.saturating_add(map_bytes(members.len()));
                    if depth == frames.len() {
                        return usize::MAX;
                    }
                    frames[depth] = Some(Frame::Object(members.iter()));
                    depth += 1;
                }
                _ => {}
            }
        }
        while depth != 0 {
            next = match frames[depth - 1]
                .as_mut()
                .expect("active JSON traversal frame")
            {
                Frame::Array(iter) => iter.next(),
                Frame::Object(iter) => iter.next().map(|(key, value)| {
                    bytes = bytes.saturating_add(key.capacity());
                    value
                }),
            };
            if next.is_some() {
                break;
            }
            depth -= 1;
            frames[depth] = None;
        }
        if next.is_none() {
            return bytes;
        }
    }
}

struct BuildState<'a> {
    shadow: ShadowCursor<'a>,
    payload: ReadCredit,
    scratch: ReadCredit,
    number: ReadCredit,
}
impl<'a> BuildState<'a> {
    fn new(input: &'a str, owner: impl Into<crate::read_memory::ReadMemoryOwner>) -> Self {
        let owner: crate::read_memory::ReadMemoryOwner = owner.into();

        Self {
            shadow: ShadowCursor::new(input),
            payload: ReadCredit::from_prepaid_owned(owner.clone(), 0),
            scratch: ReadCredit::from_prepaid_owned(owner.clone(), 0),
            number: ReadCredit::from_prepaid_owned(owner, 0),
        }
    }
    fn grow<E: serde::de::Error>(credit: &mut ReadCredit, bytes: usize) -> Result<(), E> {
        if bytes == 0 {
            return Ok(());
        }
        if closed() {
            credit
                .try_grow_for_native(
                    bytes,
                    "json_decode",
                    "construct_value",
                    "Reduce JSON payload or raise the operation memory limit.",
                )
                .map_err(|error| {
                    latch_refusal(error);
                    E::custom(ADMISSION_ERROR)
                })
        } else {
            credit
                .try_grow_for(
                    bytes,
                    "json_decode",
                    "construct_value",
                    "Reduce JSON payload or raise the operation memory limit.",
                )
                .map_err(|_| E::custom(ADMISSION_ERROR))
        }
    }
    fn shrink<E: serde::de::Error>(credit: &mut ReadCredit, bytes: usize) -> Result<(), E> {
        if closed() {
            credit.try_shrink_native(bytes).map_err(|error| {
                latch_refusal(error);
                E::custom(ADMISSION_ERROR)
            })
        } else {
            credit
                .try_shrink(bytes)
                .map_err(|_| E::custom(ADMISSION_ERROR))
        }
    }
    fn prepare<E: serde::de::Error>(&mut self) -> Result<(), E> {
        let prediction = self.shadow.token().ok_or_else(|| capacity_error::<E>())?;
        // Fund only token-local native requests, including realloc overlap.
        let additional = prediction
            .peak
            .checked_sub(self.scratch.bytes())
            .ok_or_else(|| capacity_error::<E>())?;
        Self::grow(&mut self.scratch, additional)?;
        if prediction.number {
            // Token-local upper bound includes lexical limb work, arbitrary
            // precision token copies and growth overlap. It is refunded at
            // the number callback before the next token is admitted.
            let bound = prediction
                .encoded_len
                .checked_mul(8)
                .and_then(|n| n.checked_add(NUMBER_WORK_BYTES))
                .ok_or_else(|| capacity_error::<E>())?;
            Self::grow(&mut self.number, bound)?;
        }
        Ok(())
    }
    fn callback<E: serde::de::Error>(&mut self) -> Result<(), E> {
        // The callback follows native growth. Retained scratch remains live
        // in the parser; only overlap is released before payload admission.
        let retained = self
            .shadow
            .scratch
            .iter()
            .map(|model| model.capacity)
            .max()
            .unwrap_or(0);
        let overlap = self.scratch.bytes().saturating_sub(retained);
        Self::shrink(&mut self.scratch, overlap)?;
        let number = self.number.bytes();
        Self::shrink(&mut self.number, number)
    }
    fn string<E: serde::de::Error>(&mut self, value: &str) -> Result<String, E> {
        // Release transient overlap before funding this retained copy.
        self.callback()?;
        Self::grow(&mut self.payload, value.len())?;
        let mut owned = String::with_capacity(value.len());
        if owned.capacity() != value.len() {
            drop(owned);
            return Err(capacity_error::<E>());
        }
        owned.push_str(value);
        Ok(owned)
    }
}

struct ValueSeed<'a, 'b> {
    state: &'a mut BuildState<'b>,
}
impl<'de> DeserializeSeed<'de> for ValueSeed<'_, '_> {
    type Value = Value;
    fn deserialize<D: serde::Deserializer<'de>>(self, de: D) -> Result<Value, D::Error> {
        self.state.prepare()?;
        de.deserialize_any(ValueVisitor { state: self.state })
    }
}
struct ValueVisitor<'a, 'b> {
    state: &'a mut BuildState<'b>,
}
impl<'de> Visitor<'de> for ValueVisitor<'_, '_> {
    type Value = Value;
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("any valid JSON value")
    }
    fn visit_bool<E: serde::de::Error>(self, value: bool) -> Result<Value, E> {
        self.state.callback()?;
        Ok(Value::Bool(value))
    }
    fn visit_i64<E: serde::de::Error>(self, value: i64) -> Result<Value, E> {
        self.state.callback()?;
        Ok(Value::Number(value.into()))
    }
    fn visit_u64<E: serde::de::Error>(self, value: u64) -> Result<Value, E> {
        self.state.callback()?;
        Ok(Value::Number(value.into()))
    }
    fn visit_f64<E: serde::de::Error>(self, value: f64) -> Result<Value, E> {
        self.state.callback()?;
        Ok(Number::from_f64(value).map_or(Value::Null, Value::Number))
    }
    fn visit_unit<E: serde::de::Error>(self) -> Result<Value, E> {
        self.state.callback()?;
        Ok(Value::Null)
    }
    fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Value, E> {
        self.state.string(value).map(Value::String)
    }
    fn visit_borrowed_str<E: serde::de::Error>(self, value: &'de str) -> Result<Value, E> {
        self.visit_str(value)
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut access: A) -> Result<Value, A::Error> {
        self.state.callback()?;
        let mut values = Vec::new();
        while let Some(value) = access.next_element_seed(ValueSeed { state: self.state })? {
            if values.len() == values.capacity() {
                let old = values.capacity();
                let new = if old == 0 {
                    4
                } else {
                    old.checked_mul(2).ok_or_else(capacity_error::<A::Error>)?
                };
                let bytes = new
                    .checked_mul(std::mem::size_of::<Value>())
                    .ok_or_else(capacity_error::<A::Error>)?;
                BuildState::grow(&mut self.state.payload, bytes)?;
                values.reserve_exact(new - values.len());
                if values.capacity() != new {
                    drop(value);
                    drop(values);
                    return Err(capacity_error::<A::Error>());
                }
                BuildState::shrink(&mut self.state.payload, old * std::mem::size_of::<Value>())?;
            }
            values.push(value);
        }
        Ok(Value::Array(values))
    }
    fn visit_map<A: MapAccess<'de>>(self, mut access: A) -> Result<Value, A::Error> {
        self.state.callback()?;
        let mut values = Map::new();
        while let Some(key) = access.next_key_seed(KeySeed { state: self.state })? {
            // Lookup precedes both value construction and node reservations.
            let duplicate = values.get_mut(&key);
            let value = access.next_value_seed(ValueSeed { state: self.state })?;
            if let Some(previous) = duplicate {
                let released = decoded_retained_heap_bytes(previous).saturating_add(key.capacity());
                let old = std::mem::replace(previous, value);
                drop(old);
                drop(key);
                BuildState::shrink(&mut self.state.payload, released)?;
            } else {
                let members = values.len();
                let next_members = members
                    .checked_add(1)
                    .ok_or_else(capacity_error::<A::Error>)?;
                let split = map_levels(members)
                    .checked_add(1)
                    .and_then(|levels| levels.checked_mul(MAP_NODE))
                    .ok_or_else(capacity_error::<A::Error>)?;
                BuildState::grow(&mut self.state.payload, split)?;
                values.insert(key, value);
                let retained_delta = map_bytes(next_members)
                    .checked_sub(map_bytes(members))
                    .ok_or_else(capacity_error::<A::Error>)?;
                let excess = split
                    .checked_sub(retained_delta)
                    .ok_or_else(capacity_error::<A::Error>)?;
                BuildState::shrink(&mut self.state.payload, excess)?;
            }
        }
        Ok(Value::Object(values))
    }
}
struct KeySeed<'a, 'b> {
    state: &'a mut BuildState<'b>,
}
impl<'de> DeserializeSeed<'de> for KeySeed<'_, '_> {
    type Value = String;
    fn deserialize<D: serde::Deserializer<'de>>(self, de: D) -> Result<String, D::Error> {
        self.state.prepare()?;
        de.deserialize_any(KeyVisitor { state: self.state })
    }
}
struct KeyVisitor<'a, 'b> {
    state: &'a mut BuildState<'b>,
}
impl<'de> Visitor<'de> for KeyVisitor<'_, '_> {
    type Value = String;
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a string")
    }
    fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<String, E> {
        self.state.string(value)
    }
    fn visit_borrowed_str<E: serde::de::Error>(self, value: &'de str) -> Result<String, E> {
        self.visit_str(value)
    }
}

#[derive(Clone, Copy, Default)]
struct ScratchModel {
    length: usize,
    capacity: usize,
    peak: usize,
}
impl ScratchModel {
    fn clear(&mut self) {
        self.length = 0;
        self.peak = self.capacity;
    }
    fn reserve(&mut self, additional: usize) -> Option<()> {
        let required = self.length.checked_add(additional)?;
        if required > self.capacity {
            let new = required.max(self.capacity.checked_mul(2)?).max(8);
            self.peak = self.peak.max(self.capacity.checked_add(new)?);
            self.capacity = new;
        }
        Some(())
    }
    fn append(&mut self, length: usize) -> Option<()> {
        self.reserve(length)?;
        self.length = self.length.checked_add(length)?;
        Some(())
    }
    fn codepoint(&mut self, codepoint: u32) -> Option<()> {
        if codepoint < 0x80 {
            return self.append(1);
        }
        self.reserve(4)?;
        self.length = self.length.checked_add(if codepoint < 0x800 {
            2
        } else if codepoint < 0x10000 {
            3
        } else {
            4
        })?;
        Some(())
    }
}
struct ScratchPrediction {
    peak: usize,
    encoded_len: usize,
    number: bool,
}
struct ShadowCursor<'a> {
    input: &'a [u8],
    offset: usize,
    // Both allowed feature histories: float_roundtrip may retain number
    // scratch; the default parser does not. Neither can grant refund authority
    // for the other history. This is a bound, with no feature probe.
    scratch: [ScratchModel; 2],
}
impl<'a> ShadowCursor<'a> {
    fn new(input: &'a str) -> Self {
        Self {
            input: input.as_bytes(),
            offset: 0,
            scratch: [ScratchModel::default(); 2],
        }
    }
    fn token(&mut self) -> Option<ScratchPrediction> {
        while matches!(
            self.input.get(self.offset),
            Some(b' ' | b'\n' | b'\r' | b'\t' | b',' | b':' | b']' | b'}')
        ) {
            self.offset += 1;
        }
        let token_start = self.offset;
        let number = matches!(self.input.get(self.offset), Some(b'-' | b'0'..=b'9'));
        let mut end = self.offset;
        for (index, model) in self.scratch.iter_mut().enumerate() {
            model.peak = model.capacity;
            end = match self.input.get(token_start) {
                Some(b'"') => string_requests(self.input, token_start, model)?,
                Some(b'[' | b'{') => token_start + 1,
                Some(b'n') => token_start.saturating_add(4).min(self.input.len()),
                Some(b't') => token_start.saturating_add(4).min(self.input.len()),
                Some(b'f') => token_start.saturating_add(5).min(self.input.len()),
                Some(_) if number => number_requests(self.input, token_start, model, index == 1)?,
                _ => token_start,
            };
        }
        self.offset = end;
        Some(ScratchPrediction {
            peak: self
                .scratch
                .iter()
                .map(|model| model.peak)
                .max()
                .unwrap_or(0),
            encoded_len: self.offset.saturating_sub(token_start),
            number,
        })
    }
}

// Only allocation requests are simulated. Invalid prefixes stop simulation at
// serde_json's own first failure, without constructing or validating a Value.
fn string_requests(input: &[u8], quote: usize, model: &mut ScratchModel) -> Option<usize> {
    model.clear();
    let mut offset = quote + 1;
    let mut span = offset;
    loop {
        let Some(&byte) = input.get(offset) else {
            return Some(input.len());
        };
        match byte {
            b'"' => {
                if model.length != 0 {
                    model.append(offset - span)?;
                }
                return Some(offset + 1);
            }
            0..=0x1f => return Some(offset + 1),
            b'\\' => {
                model.append(offset - span)?;
                offset += 1;
                let Some(&escape) = input.get(offset) else {
                    return Some(input.len());
                };
                offset += 1;
                match escape {
                    b'"' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't' => {
                        model.append(1)?;
                    }
                    b'u' => {
                        let Some(first) = hex_group(input, &mut offset) else {
                            return Some(offset);
                        };
                        if (0xdc00..=0xdfff).contains(&first) {
                            return Some(offset);
                        }
                        let codepoint = if (0xd800..=0xdbff).contains(&first) {
                            if input.get(offset) != Some(&b'\\') {
                                return Some((offset + 1).min(input.len()));
                            }
                            offset += 1;
                            if input.get(offset) != Some(&b'u') {
                                return Some((offset + 1).min(input.len()));
                            }
                            offset += 1;
                            let Some(second) = hex_group(input, &mut offset) else {
                                return Some(offset);
                            };
                            if !(0xdc00..=0xdfff).contains(&second) {
                                return Some(offset);
                            }
                            (((first as u32 - 0xd800) << 10) | (second as u32 - 0xdc00)) + 0x10000
                        } else {
                            first as u32
                        };
                        model.codepoint(codepoint)?;
                    }
                    _ => return Some(offset),
                }
                span = offset;
            }
            _ => offset += 1,
        }
    }
}
fn hex_group(input: &[u8], offset: &mut usize) -> Option<u16> {
    let end = offset.checked_add(4)?;
    let Some(group) = input.get(*offset..end) else {
        *offset = input.len();
        return None;
    };
    *offset = end;
    let mut value = 0u16;
    for &byte in group {
        let digit = match byte {
            b'0'..=b'9' => byte - b'0',
            b'a'..=b'f' => byte - b'a' + 10,
            b'A'..=b'F' => byte - b'A' + 10,
            _ => return None,
        };
        value = value * 16 + digit as u16;
    }
    Some(value)
}
fn decimal_digits(mut value: u64) -> usize {
    let mut length = 1;
    while value >= 10 {
        length += 1;
        value /= 10;
    }
    length
}
fn number_requests(
    input: &[u8],
    start: usize,
    model: &mut ScratchModel,
    float_on: bool,
) -> Option<usize> {
    let mut offset = start;
    if input.get(offset) == Some(&b'-') {
        offset += 1;
    }
    let Some(&first) = input.get(offset) else {
        return Some(offset);
    };
    offset += 1;
    let mut significand = match first {
        b'0' => 0u64,
        b'1'..=b'9' => (first - b'0') as u64,
        _ => return Some(offset),
    };
    if first == b'0' && matches!(input.get(offset), Some(b'0'..=b'9')) {
        return Some(offset);
    }
    let mut long = false;
    if first != b'0' {
        while let Some(&digit @ b'0'..=b'9') = input.get(offset) {
            match significand
                .checked_mul(10)
                .and_then(|n| n.checked_add((digit - b'0') as u64))
            {
                Some(next) => {
                    significand = next;
                    offset += 1;
                }
                None => {
                    if float_on {
                        model.clear();
                        model.append(decimal_digits(significand))?;
                    }
                    long = true;
                    break;
                }
            }
        }
        if long {
            while matches!(input.get(offset), Some(b'0'..=b'9')) {
                if float_on {
                    model.append(1)?;
                }
                offset += 1;
            }
        }
    }
    if input.get(offset) == Some(&b'.') {
        offset += 1;
        let fraction_start = offset;
        let mut digits = 0usize;
        while let Some(&digit @ b'0'..=b'9') = input.get(offset) {
            if long {
                if float_on {
                    model.append(1)?;
                }
                offset += 1;
                continue;
            }
            match significand
                .checked_mul(10)
                .and_then(|n| n.checked_add((digit - b'0') as u64))
            {
                Some(next) => {
                    significand = next;
                    digits += 1;
                    offset += 1;
                }
                None => {
                    if float_on {
                        model.clear();
                        let significant_digits = decimal_digits(significand);
                        // de.rs:829: Repeat::take exact-length extend, then the
                        // integer itoa slice, then each remaining fraction byte.
                        if let Some(zeros) = digits.checked_sub(significant_digits + 1) {
                            model.append(zeros + 1)?;
                        }
                        model.append(significant_digits)?;
                    }
                    long = true;
                }
            }
        }
        if offset == fraction_start {
            return Some(offset);
        }
    }
    if matches!(input.get(offset), Some(b'e' | b'E')) {
        offset += 1;
        if matches!(input.get(offset), Some(b'+' | b'-')) {
            offset += 1;
        }
        if input.get(offset).is_some() {
            offset += 1;
        }
        while matches!(input.get(offset), Some(b'0'..=b'9')) {
            offset += 1;
        }
    }
    Some(offset)
}

#[cfg(test)]
mod finite_recipe_provenance_rows {
    use super::*;
    use serde::de::Error as _;

    // A private constructor/provenance row, explicitly NOT a selected-root
    // reachability claim. The selected root always installs a memory owner.
    #[test]
    fn finite_native_data_recipes_preserve_their_real_constructor_provenance() {
        let scope = ClosedScope::enter();
        let produced: ClosedError<serde_json::Error> = capacity_error();
        let ClosedError::Native(produced) = produced else {
            panic!("finite capacity is native Data")
        };
        let control = serde_json::Error::custom(CAPACITY_ERROR);
        assert_eq!(scope.finite(), Some(CAPACITY_ERROR));
        assert_eq!(produced.classify(), control.classify());
        assert_eq!(
            (produced.line(), produced.column()),
            (control.line(), control.column())
        );
        assert_eq!(produced.to_string(), control.to_string());
        assert_eq!((produced.line(), produced.column()), (0, 0));
        assert!(produced.is_data());
        drop(produced);
        drop(control);
        drop(scope);

        struct Restore(Option<ReadMemoryOwner>);
        impl Drop for Restore {
            fn drop(&mut self) {
                OWNER.with(|slot| {
                    slot.replace(self.0.take());
                });
            }
        }
        let _restore = Restore(OWNER.with(|slot| slot.replace(None)));
        let failure = deserialize_json_closed("null")
            .err()
            .expect("no owner must refuse");
        let ClosedJsonFailure::Native(produced, finite) = failure else {
            panic!("missing owner retains native Data recipe")
        };
        let control = serde_json::Error::custom(MISSING_OWNER);
        assert_eq!(finite, Some(MISSING_OWNER));
        assert!(produced.is_data());
        assert_eq!(produced.to_string(), control.to_string());
        assert_eq!((produced.line(), produced.column()), (0, 0));
        assert_eq!(produced.classify(), control.classify());
    }

    #[test]
    fn captured_scalar_refusal_never_formats_a_custom_message() {
        struct Forbidden;
        impl fmt::Display for Forbidden {
            fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
                panic!("custom Display entered after actual scalar latch")
            }
        }
        let scope = ClosedScope::enter();
        let cause = NativeRefusal::MemoryBudgetExceeded {
            subsystem: "json_decode",
            operation: "construct_value",
            requested_bytes: 9,
            available_bytes: 8,
            budget_limit_bytes: 8,
            hint: "test scalar latch",
        };
        latch_refusal(cause);
        let error: ClosedError<serde_json::Error> = ClosedError::custom(Forbidden);
        assert!(matches!(error, ClosedError::Captured));
        assert_eq!(scope.failure(), Some(cause));
        assert_eq!(scope.finite(), None);
    }
}
