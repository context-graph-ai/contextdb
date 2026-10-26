use super::*;
use serde::de::{self, DeserializeSeed, Visitor};
use std::marker::PhantomData;
#[derive(Debug)]
struct Ledger {
    failure: Option<contextdb_core::read_memory::NativeRefusal>,
    diagnostic: Option<crate::funded_error::OriginalError>,
    messagepack: bool,
    payload_allocates: bool,
    json_diagnostic: Option<crate::funded_error::AdmittedJsonDiagnostic>,
    allocation: crate::memory_accounting::OwnedMemoryReservation,
}
// Serde's outer containers and core's nested JSON builder share this SAME
// original invoice. This is an operation-local adapter, never a registered
// reader/writer, counter lookup, or a second payer.
#[derive(Debug)]
struct DecodeBudget(parking_lot::Mutex<Ledger>);
impl contextdb_core::read_memory::ReadMemoryBudget for DecodeBudget {
    fn try_reserve(
        &self,
        bytes: usize,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> Result<()> {
        self.try_reserve_native(bytes, subsystem, operation, hint)
            .map_err(Error::NativeEngineOther)
    }
    fn native_reserve_supported(&self) -> bool {
        true
    }
    fn try_reserve_native(
        &self,
        bytes: usize,
        _: &'static str,
        _: &'static str,
        _: &'static str,
    ) -> std::result::Result<(), contextdb_core::read_memory::NativeRefusal> {
        let mut ledger = self.0.lock();
        if let Some(error) = ledger.failure {
            return Err(error);
        }
        if let Err(error) = crate::memory_accounting::grow_preparation_original_native(
            &mut ledger.allocation,
            bytes,
            "events",
            "decode_sink_queue_entry",
            "Reduce pending deliveries or raise MEMORY_LIMIT.",
        ) {
            #[cfg(test)]
            crate::persistence::content_identity::proof_allocation_oracle_tests::selected_json_producer_rows::refused(error);
            ledger.failure = Some(error);
            return Err(error);
        }
        Ok(())
    }
    fn release(&self, _bytes: usize) {
        // Core destroys parser scratch before releasing it, but a returned
        // JSON body still escapes that inner builder. Retain this complete
        // record's invoice until OriginalDecoded physically destroys it.
    }
}
thread_local! { static ACTIVE: std::cell::Cell<*const DecodeBudget> = const { std::cell::Cell::new(std::ptr::null()) }; }
struct Scope(*const DecodeBudget);
impl Drop for Scope {
    fn drop(&mut self) {
        ACTIVE.with(|slot| slot.set(self.0));
    }
}
fn admit<E: de::Error>(bytes: usize) -> std::result::Result<(), CodecError<E>> {
    use contextdb_core::read_memory::ReadMemoryBudget;
    ACTIVE.with(|slot| {
        let ptr = slot.get();
        assert!(!ptr.is_null(), "original decode requires its pinned seed");
        // The two selected streamed projections have no allocating
        // payload visitors, including Field and IgnoredAny. Their native
        // reader scratch is separately prepaid by the structural scan.
        {
            let ledger = unsafe { &*ptr }.0.lock();
            if ledger.failure.is_some() || ledger.diagnostic.is_some() {
                return Err(CodecError::Captured);
            }
            if !ledger.payload_allocates {
                return Ok(());
            }
        }
        // Scope owns this pointer until the synchronous decode completes.
        unsafe { &*ptr }
            .try_reserve_native(
                bytes,
                "events",
                "decode_sink_queue_entry",
                "Reduce pending deliveries or raise MEMORY_LIMIT.",
            )
            .map_err(|_| CodecError::Captured)
    })
}
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn decode<T: de::DeserializeOwned>(
    original: &crate::memory_accounting::OwnedMemoryReservation,
    bytes: &[u8],
) -> Result<OriginalDecoded<T>> {
    let diagnostic = crate::funded_error::OriginalError::prepare(
        original,
        "events",
        "decode_sink_queue_entry",
        "Reduce pending deliveries or raise MEMORY_LIMIT.",
    )
    .map_err(|failure| {
        native_storage_admission::original_storage_error(
            native_storage_admission::NativeStorageFailure::Refused(failure),
        )
    })?;
    decode_with_route(
        original,
        bytes,
        true,
        false,
        |error| error,
        |bytes| {
            crate::memory_accounting::reserve_preparation_from_original(
                original,
                bytes,
                "events",
                "decode_sink_queue_entry",
                "Reduce pending deliveries or raise MEMORY_LIMIT.",
            )
        },
        |refusal| {
            native_storage_admission::original_storage_error(
                native_storage_admission::NativeStorageFailure::Refused(refusal),
            )
        },
        |error| error.into_public_prepared(),
        move |error, ledger| diagnostic.decode(error, ledger).into_public_prepared(),
    )
}
// Concrete persisted-row root. Generic DeserializeOwned callers retain
// their unqualified legacy adapter, even when they happen to contain JSON.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn decode_row(
    original: &crate::memory_accounting::OwnedMemoryReservation,
    bytes: &[u8],
) -> Result<OriginalDecoded<PersistedVersionedRow>> {
    decode_closed_json_root(original, bytes)
}
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn decode_sink_entry(
    original: &crate::memory_accounting::OwnedMemoryReservation,
    bytes: &[u8],
) -> Result<OriginalDecoded<SinkQueueEntry>> {
    decode_closed_json_root(original, bytes)
}
// These are the only selected JSON-bearing roots. A foreign Deserialize
// implementation cannot opt itself in through the generic decoder.
trait ClosedJsonRoot: de::DeserializeOwned {}
impl ClosedJsonRoot for PersistedVersionedRow {}
impl ClosedJsonRoot for SinkQueueEntry {}
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn decode_closed_json_root<T: ClosedJsonRoot>(
    original: &crate::memory_accounting::OwnedMemoryReservation,
    bytes: &[u8],
) -> Result<OriginalDecoded<T>> {
    // Other targets retain their existing public behavior and honest
    // generic provenance. The native Layout below is target-specific;
    // compiler/dependency/profile/artifact identity is an acceptance gate.
    if !cfg!(all(
        target_arch = "x86_64",
        target_os = "linux",
        target_env = "gnu",
        target_pointer_width = "64"
    )) {
        return decode(original, bytes);
    }
    let diagnostic = crate::funded_error::OriginalError::prepare(
        original,
        "events",
        "decode_sink_queue_entry",
        "Reduce pending deliveries or raise MEMORY_LIMIT.",
    )
    .map_err(|error| {
        native_storage_admission::original_storage_error(
            native_storage_admission::NativeStorageFailure::Refused(error),
        )
    })?;
    decode_with_route(
        original,
        bytes,
        true,
        true,
        |error| error,
        |bytes| {
            crate::memory_accounting::reserve_preparation_from_original(
                original,
                bytes,
                "events",
                "decode_sink_queue_entry",
                "Reduce pending deliveries or raise MEMORY_LIMIT.",
            )
        },
        |error| {
            native_storage_admission::original_storage_error(
                native_storage_admission::NativeStorageFailure::Refused(error),
            )
        },
        |error| error.into_public_prepared(),
        move |error, ledger| diagnostic.decode(error, ledger).into_public_prepared(),
    )
}
fn capture_json_refusal(error: contextdb_core::read_memory::NativeRefusal) {
    #[cfg(test)]
    crate::persistence::content_identity::proof_allocation_oracle_tests::selected_json_producer_rows::json_refusal();
    ACTIVE.with(|slot| {
        let ptr = slot.get();
        assert!(!ptr.is_null(), "closed JSON stays inside the row producer");
        let mut ledger = unsafe { &*ptr }.0.lock();
        if ledger.diagnostic.is_none() {
            let first = ledger.failure.unwrap_or(error);
            ledger.failure = Some(first);
            let output = ledger
                .json_diagnostic
                .take()
                .expect("selected row prepared JSON refusal custody");
            ledger.diagnostic = Some(output.refuse(first));
        }
    });
}
fn capture_json_diagnostic(
    error: serde_json::Error,
    finite: Option<&'static str>,
) -> std::result::Result<(), serde_json::Error> {
    ACTIVE.with(|slot| {
        let ptr = slot.get();
        assert!(!ptr.is_null(), "closed JSON stays inside the row producer");
        let mut ledger = unsafe { &*ptr }.0.lock();
        if ledger.failure.is_some() || ledger.diagnostic.is_some() {
            return Ok(());
        }
        let output = ledger
            .json_diagnostic
            .take()
            .expect("selected row prepared JSON destination");
        let messagepack = ledger.messagepack;
        match output.finish(error, finite, messagepack) {
            Ok(error) => {
                ledger.diagnostic = Some(error);
                Ok(())
            }
            Err((error, output)) => {
                ledger.json_diagnostic = Some(output);
                Err(error)
            }
        }
    })
}
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn decode_config_compatible<T: de::DeserializeOwned>(
    original: &crate::memory_accounting::OwnedMemoryReservation,
    bytes: &[u8],
) -> Result<OriginalDecoded<T>> {
    let diagnostic = crate::funded_error::OriginalError::prepare(
        original,
        "events",
        "decode_sink_queue_entry",
        "Reduce pending deliveries or raise MEMORY_LIMIT.",
    )
    .map_err(|failure| {
        native_storage_admission::original_storage_error(
            native_storage_admission::NativeStorageFailure::Refused(failure),
        )
    })?;
    decode_with_route(
        original,
        bytes,
        false,
        false,
        |error| error,
        |bytes| {
            crate::memory_accounting::reserve_preparation_from_original(
                original,
                bytes,
                "events",
                "decode_sink_queue_entry",
                "Reduce pending deliveries or raise MEMORY_LIMIT.",
            )
        },
        |refusal| {
            native_storage_admission::original_storage_error(
                native_storage_admission::NativeStorageFailure::Refused(refusal),
            )
        },
        |error| error.into_public_prepared(),
        move |error, ledger| diagnostic.decode(error, ledger).into_public_prepared(),
    )
}
pub(super) fn decode_funded<T: de::DeserializeOwned>(
    original: &crate::memory_accounting::OwnedMemoryReservation,
    bytes: &[u8],
    exact: bool,
) -> crate::funded_error::OriginalResult<OriginalDecoded<T>> {
    let diagnostic = crate::funded_error::OriginalError::prepare_route(original)?;
    decode_with_route(
        original,
        bytes,
        exact,
        false,
        |error| diagnostic.error(error),
        |bytes| {
            crate::memory_accounting::reserve_preparation_original_native(
                original,
                bytes,
                "events",
                "decode_sink_queue_entry",
                "Reduce pending deliveries or raise MEMORY_LIMIT.",
            )
            .map_err(crate::funded_error::OriginalError::native)
        },
        crate::funded_error::OriginalError::native,
        |error| error,
        |error, ledger| diagnostic.decode(error, ledger),
    )
}
fn decode_with_route<T: de::DeserializeOwned, E>(
    _original: &crate::memory_accounting::OwnedMemoryReservation,
    bytes: &[u8],
    exact: bool,
    closed_json: bool,
    existing: impl Fn(Error) -> E,
    reserve: impl Fn(usize) -> std::result::Result<crate::memory_accounting::OwnedMemoryReservation, E>,
    native: impl FnOnce(contextdb_core::read_memory::NativeRefusal) -> E,
    captured: impl FnOnce(crate::funded_error::OriginalError) -> E,
    codec: impl FnOnce(
        bincode::error::DecodeError,
        crate::memory_accounting::OwnedMemoryReservation,
    ) -> E,
) -> std::result::Result<OriginalDecoded<T>, E> {
    #[cfg(test)]
    let _purge_producer = content_identity::proof_allocation_oracle_tests::PurgeProducerGuard::new::<
        T,
    >("original_event_decode::decode_with_route");
    let control = std::mem::size_of::<T>()
        .checked_mul(4)
        .and_then(|n| n.checked_add(128))
        .and_then(|n| n.checked_add(std::mem::size_of::<DecodeBudget>()))
        .and_then(|n| n.checked_add(2 * std::mem::size_of::<usize>()))
        .ok_or_else(|| existing(Error::Other("decode control size overflow".into())))?;
    let _control = reserve(control)?;
    let json_diagnostic = if closed_json {
        Some(
            crate::funded_error::AdmittedJsonDiagnostic::prepare(_original).map_err(|error| {
                existing(native_storage_admission::original_storage_error(
                    native_storage_admission::NativeStorageFailure::Refused(error),
                ))
            })?,
        )
    } else {
        None
    };
    let budget = Arc::new(DecodeBudget(parking_lot::Mutex::new(Ledger {
        allocation: reserve(0)?,
        failure: None,
        diagnostic: None,
        messagepack: false,
        payload_allocates: true,
        json_diagnostic,
    })));
    let old = ACTIVE.with(|slot| slot.replace(Arc::as_ptr(&budget)));
    let scope = Scope(old);
    let json = if closed_json {
        contextdb_core::types::JsonReadAdmissionGuard::enter_closed(
            budget.clone(),
            contextdb_core::types::ClosedJsonReadHooks {
                refused: capture_json_refusal,
                diagnostic: capture_json_diagnostic,
            },
        )
    } else {
        contextdb_core::types::JsonReadAdmissionGuard::enter(budget.clone())
    };
    let result = bincode::serde::seed_decode_from_slice(
        Seed(PhantomData::<T>),
        bytes,
        bincode::config::standard(),
    );
    drop(json);
    drop(scope);
    // The JSON guard restored its predecessor and released the only Arc
    // clone. Destroy the native control while _control still funds it,
    // then MOVE the very same invoice into the escaping typed carrier.
    let mut ledger = match Arc::try_unwrap(budget) {
        Ok(budget) => budget.0.into_inner(),
        Err(budget) => {
            drop(result);
            drop(budget);
            return Err(existing(Error::Other(
                "original JSON decode scope retained its owner".into(),
            )));
        }
    };
    if let Some(error) = ledger.diagnostic.take() {
        drop(result);
        return Err(captured(error));
    }
    if let Some(error) = ledger.failure.take() {
        drop(result);
        return Err(native(error));
    }
    let (value, consumed) = match result {
        Ok((Some(value), consumed)) => (value, consumed),
        Ok((None, _)) => unreachable!("captured codec error has a sideband owner"),
        Err(error) => return Err(codec(error, ledger.allocation)),
    };
    if exact && consumed != bytes.len() {
        drop(value);
        return Err(existing(Error::Other(
            "bincode exact decode left trailing bytes".into(),
        )));
    }
    Ok(OriginalDecoded {
        value,
        allocation: ledger.allocation,
    })
}
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn decode_blob<T: de::DeserializeOwned>(
    original: &crate::memory_accounting::OwnedMemoryReservation,
    bytes: &[u8],
) -> Result<OriginalDecoded<T>> {
    decode_blob_with(original, true, |seed| {
        seed.deserialize(&mut rmp_serde::Deserializer::from_read_ref(bytes))
    })
}
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn decode_blob_reader<T: StreamedBlobProjection, R: std::io::Read>(
    original: &crate::memory_accounting::OwnedMemoryReservation,
    reader: &mut R,
) -> Result<OriginalDecoded<T>> {
    decode_blob_with(original, false, |seed| {
        seed.deserialize(&mut rmp_serde::Deserializer::new(reader))
    })
}
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn decode_blob_with<T: de::DeserializeOwned>(
    original: &crate::memory_accounting::OwnedMemoryReservation,
    payload_allocates: bool,
    decode: impl FnOnce(
        Seed<PhantomData<T>>,
    ) -> std::result::Result<Option<T>, rmp_serde::decode::Error>,
) -> Result<OriginalDecoded<T>> {
    let diagnostic = crate::funded_error::OriginalError::prepare(
        original,
        "events",
        "decode_sink_queue_entry",
        "Reduce pending deliveries or raise MEMORY_LIMIT.",
    )
    .map_err(Error::NativeEngineOther)?;
    let control = std::mem::size_of::<T>()
        .checked_mul(4)
        .and_then(|n| n.checked_add(128))
        .and_then(|n| n.checked_add(std::mem::size_of::<DecodeBudget>()))
        .and_then(|n| n.checked_add(2 * std::mem::size_of::<usize>()))
        .ok_or_else(|| Error::Other("decode control size overflow".into()))?;
    let _control = crate::memory_accounting::reserve_preparation_from_original(
        original,
        control,
        "events",
        "decode_sink_queue_entry",
        "Reduce pending deliveries or raise MEMORY_LIMIT.",
    )?;
    let budget = Arc::new(DecodeBudget(parking_lot::Mutex::new(Ledger {
        allocation: crate::memory_accounting::reserve_preparation_from_original(
            original,
            0,
            "events",
            "decode_sink_queue_entry",
            "Reduce pending deliveries or raise MEMORY_LIMIT.",
        )?,
        failure: None,
        diagnostic: None,
        messagepack: true,
        payload_allocates,
        json_diagnostic: None,
    })));
    let old = ACTIVE.with(|slot| slot.replace(Arc::as_ptr(&budget)));
    let scope = Scope(old);
    let json = contextdb_core::types::JsonReadAdmissionGuard::enter(budget.clone());
    let result = decode(Seed(PhantomData::<T>));
    drop(json);
    drop(scope);
    // The JSON guard restored its predecessor and released the only Arc
    // clone. Destroy the native control while _control still funds it,
    // then MOVE the very same invoice into the escaping typed carrier.
    let mut ledger = match Arc::try_unwrap(budget) {
        Ok(budget) => budget.0.into_inner(),
        Err(budget) => {
            drop(result);
            drop(budget);
            return Err(Error::Other(
                "original JSON decode scope retained its owner".into(),
            ));
        }
    };
    if let Some(error) = ledger.diagnostic.take() {
        drop(result);
        return Err(error.into_public_prepared());
    }
    if let Some(error) = ledger.failure.take() {
        drop(result);
        return Err(native_storage_admission::original_storage_error(
            native_storage_admission::NativeStorageFailure::Refused(error),
        ));
    }
    let value = match result {
        Ok(Some(value)) => value,
        Ok(None) => unreachable!("captured codec error has a sideband owner"),
        Err(error) => {
            return Err(diagnostic
                .messagepack(error, ledger.allocation)
                .into_public_prepared());
        }
    };
    Ok(OriginalDecoded {
        value,
        allocation: ledger.allocation,
    })
}
/// A native codec transports only its own scalar/foreign errors. An
/// engine-owned diagnosis is moved through the existing operation ledger;
/// the native visitor returns unit so no allocating sentinel is needed.
#[derive(Debug)]
enum CodecError<E> {
    Native(E),
    Owned(crate::funded_error::OriginalError),
    Captured,
}
impl<E: std::fmt::Display> std::fmt::Display for CodecError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Native(error) => std::fmt::Display::fmt(error, f),
            Self::Owned(error) => std::fmt::Display::fmt(error, f),
            Self::Captured => f.write_str("codec diagnosis retained by producer"),
        }
    }
}
impl<E: de::Error> std::error::Error for CodecError<E> {}
impl<E: de::Error> de::Error for CodecError<E> {
    // An arbitrary Display is never counted, replayed or declared paid.
    fn custom<T: std::fmt::Display>(msg: T) -> Self {
        if terminal::<E>().is_err() {
            Self::Captured
        } else {
            Self::Native(E::custom(msg))
        }
    }
    fn missing_field(field: &'static str) -> Self {
        Self::field(field, false)
    }
    fn duplicate_field(field: &'static str) -> Self {
        Self::field(field, true)
    }
}
impl<E: de::Error> CodecError<E> {
    fn field(field: &'static str, duplicate: bool) -> Self {
        ACTIVE.with(|slot| {
            let ptr = slot.get();
            assert!(!ptr.is_null(), "closed diagnostic belongs to active decode");
            let mut ledger = unsafe { &*ptr }.0.lock();
            if ledger.failure.is_some() || ledger.diagnostic.is_some() {
                return Self::Captured;
            }
            // Only the marker-selected streamed roots have audited
            // visitor control flow. Generic roots may catch nested serde
            // errors; preserve those native errors and keep them unpaid.
            if ledger.payload_allocates {
                drop(ledger);
                return Self::Native(if duplicate {
                    E::duplicate_field(field)
                } else {
                    E::missing_field(field)
                });
            }
            match crate::funded_error::AdmittedDiagnostic::field(
                &ledger.allocation,
                field,
                duplicate,
                ledger.messagepack,
            ) {
                Ok(error) => Self::Owned(error),
                Err(error) => {
                    if ledger.failure.is_none() {
                        ledger.failure = Some(error);
                    }
                    Self::Captured
                }
            }
        })
    }
}
fn payload_allocates() -> bool {
    ACTIVE.with(|slot| {
        let ptr = slot.get();
        assert!(!ptr.is_null(), "sequence stays in its producer scope");
        unsafe { &*ptr }.0.lock().payload_allocates
    })
}
fn terminal<E>() -> std::result::Result<(), CodecError<E>> {
    ACTIVE.with(|slot| {
        let ptr = slot.get();
        assert!(
            !ptr.is_null(),
            "codec adapter remains in its producer scope"
        );
        let ledger = unsafe { &*ptr }.0.lock();
        if ledger.failure.is_some() || ledger.diagnostic.is_some() {
            Err(CodecError::Captured)
        } else {
            Ok(())
        }
    })
}
fn capture<T, E>(
    result: std::result::Result<T, CodecError<E>>,
    output: &mut Option<T>,
) -> std::result::Result<(), E> {
    match result {
        Ok(value) => {
            *output = Some(value);
            Ok(())
        }
        Err(CodecError::Native(error)) => Err(error),
        Err(CodecError::Owned(error)) => {
            ACTIVE.with(|slot| {
                let ptr = slot.get();
                assert!(!ptr.is_null(), "capture remains inside decode scope");
                let mut ledger = unsafe { &*ptr }.0.lock();
                if ledger.diagnostic.is_none() {
                    ledger.diagnostic = Some(error);
                }
            });
            Ok(())
        }
        Err(CodecError::Captured) => Ok(()),
    }
}
struct Seed<S>(S);
impl<'de, S: DeserializeSeed<'de>> DeserializeSeed<'de> for Seed<S> {
    type Value = Option<S::Value>;
    fn deserialize<D: de::Deserializer<'de>>(
        self,
        d: D,
    ) -> std::result::Result<Self::Value, D::Error> {
        let mut output = None;
        capture(self.0.deserialize(Admitted(d)), &mut output)?;
        Ok(output)
    }
}
struct Admitted<D>(D);
struct V<'a, VV, Out> {
    inner: VV,
    heap: bool,
    output: &'a mut Option<Out>,
}
impl<'de, VV: Visitor<'de>> Visitor<'de> for V<'_, VV, VV::Value> {
    type Value = ();
    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.expecting(f)
    }
    fn visit_bool<E: de::Error>(self, value: bool) -> std::result::Result<(), E> {
        capture(self.inner.visit_bool::<CodecError<E>>(value), self.output)
    }
    fn visit_i8<E: de::Error>(self, value: i8) -> std::result::Result<(), E> {
        capture(self.inner.visit_i8::<CodecError<E>>(value), self.output)
    }
    fn visit_i16<E: de::Error>(self, value: i16) -> std::result::Result<(), E> {
        capture(self.inner.visit_i16::<CodecError<E>>(value), self.output)
    }
    fn visit_i32<E: de::Error>(self, value: i32) -> std::result::Result<(), E> {
        capture(self.inner.visit_i32::<CodecError<E>>(value), self.output)
    }
    fn visit_i64<E: de::Error>(self, value: i64) -> std::result::Result<(), E> {
        capture(self.inner.visit_i64::<CodecError<E>>(value), self.output)
    }
    fn visit_i128<E: de::Error>(self, value: i128) -> std::result::Result<(), E> {
        capture(self.inner.visit_i128::<CodecError<E>>(value), self.output)
    }
    fn visit_u8<E: de::Error>(self, value: u8) -> std::result::Result<(), E> {
        capture(self.inner.visit_u8::<CodecError<E>>(value), self.output)
    }
    fn visit_u16<E: de::Error>(self, value: u16) -> std::result::Result<(), E> {
        capture(self.inner.visit_u16::<CodecError<E>>(value), self.output)
    }
    fn visit_u32<E: de::Error>(self, value: u32) -> std::result::Result<(), E> {
        capture(self.inner.visit_u32::<CodecError<E>>(value), self.output)
    }
    fn visit_u64<E: de::Error>(self, value: u64) -> std::result::Result<(), E> {
        capture(self.inner.visit_u64::<CodecError<E>>(value), self.output)
    }
    fn visit_u128<E: de::Error>(self, value: u128) -> std::result::Result<(), E> {
        capture(self.inner.visit_u128::<CodecError<E>>(value), self.output)
    }
    fn visit_f32<E: de::Error>(self, value: f32) -> std::result::Result<(), E> {
        capture(self.inner.visit_f32::<CodecError<E>>(value), self.output)
    }
    fn visit_f64<E: de::Error>(self, value: f64) -> std::result::Result<(), E> {
        capture(self.inner.visit_f64::<CodecError<E>>(value), self.output)
    }
    fn visit_char<E: de::Error>(self, value: char) -> std::result::Result<(), E> {
        capture(self.inner.visit_char::<CodecError<E>>(value), self.output)
    }
    fn visit_unit<E: de::Error>(self) -> std::result::Result<(), E> {
        capture(self.inner.visit_unit::<CodecError<E>>(), self.output)
    }
    fn visit_none<E: de::Error>(self) -> std::result::Result<(), E> {
        capture(self.inner.visit_none::<CodecError<E>>(), self.output)
    }
    fn visit_some<D: de::Deserializer<'de>>(self, d: D) -> std::result::Result<(), D::Error> {
        capture(self.inner.visit_some(Admitted(d)), self.output)
    }
    fn visit_newtype_struct<D: de::Deserializer<'de>>(
        self,
        d: D,
    ) -> std::result::Result<(), D::Error> {
        capture(self.inner.visit_newtype_struct(Admitted(d)), self.output)
    }
    fn visit_str<E: de::Error>(self, value: &str) -> std::result::Result<(), E> {
        let result =
            admit::<E>(value.len()).and_then(|()| self.inner.visit_str::<CodecError<E>>(value));
        capture(result, self.output)
    }
    fn visit_borrowed_str<E: de::Error>(self, value: &'de str) -> std::result::Result<(), E> {
        let result = admit::<E>(value.len())
            .and_then(|()| self.inner.visit_borrowed_str::<CodecError<E>>(value));
        capture(result, self.output)
    }
    fn visit_bytes<E: de::Error>(self, value: &[u8]) -> std::result::Result<(), E> {
        let result =
            admit::<E>(value.len()).and_then(|()| self.inner.visit_bytes::<CodecError<E>>(value));
        capture(result, self.output)
    }
    fn visit_borrowed_bytes<E: de::Error>(self, value: &'de [u8]) -> std::result::Result<(), E> {
        let result = admit::<E>(value.len())
            .and_then(|()| self.inner.visit_borrowed_bytes::<CodecError<E>>(value));
        capture(result, self.output)
    }
    fn visit_string<E: de::Error>(self, value: String) -> std::result::Result<(), E> {
        capture(self.inner.visit_string::<CodecError<E>>(value), self.output)
    }
    fn visit_byte_buf<E: de::Error>(self, value: Vec<u8>) -> std::result::Result<(), E> {
        capture(
            self.inner.visit_byte_buf::<CodecError<E>>(value),
            self.output,
        )
    }
    fn visit_seq<A: de::SeqAccess<'de>>(self, a: A) -> std::result::Result<(), A::Error> {
        capture(
            self.inner.visit_seq(Seq {
                inner: a,
                heap: self.heap,
                first: true,
            }),
            self.output,
        )
    }
    fn visit_map<A: de::MapAccess<'de>>(self, a: A) -> std::result::Result<(), A::Error> {
        capture(
            self.inner.visit_map(Map {
                inner: a,
                first_key: true,
                first_value: true,
            }),
            self.output,
        )
    }
    fn visit_enum<A: de::EnumAccess<'de>>(self, a: A) -> std::result::Result<(), A::Error> {
        capture(self.inner.visit_enum(Enum(a)), self.output)
    }
}
struct Seq<A> {
    inner: A,
    heap: bool,
    first: bool,
}
impl<'de, A: de::SeqAccess<'de>> de::SeqAccess<'de> for Seq<A> {
    type Error = CodecError<A::Error>;
    fn next_element_seed<S: DeserializeSeed<'de>>(
        &mut self,
        seed: S,
    ) -> std::result::Result<Option<S::Value>, Self::Error> {
        if self.heap && self.inner.size_hint() != Some(0) {
            let slots = if self.first { 12 } else { 4 };
            admit(std::mem::size_of::<S::Value>().saturating_mul(slots))?;
            self.first = false;
        }
        match self
            .inner
            .next_element_seed(Seed(seed))
            .map_err(CodecError::Native)?
        {
            Some(Some(value)) => Ok(Some(value)),
            None => Ok(None),
            Some(None) => Err(CodecError::Captured),
        }
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
    type Error = CodecError<A::Error>;
    fn next_key_seed<S: DeserializeSeed<'de>>(
        &mut self,
        seed: S,
    ) -> std::result::Result<Option<S::Value>, Self::Error> {
        if self.inner.size_hint() != Some(0) {
            let slots = if self.first_key { 16 } else { 4 };
            admit(
                std::mem::size_of::<S::Value>()
                    .saturating_mul(slots)
                    .saturating_add(128 + 11 * std::mem::size_of::<S::Value>()),
            )?;
            self.first_key = false;
        }
        match self
            .inner
            .next_key_seed(Seed(seed))
            .map_err(CodecError::Native)?
        {
            Some(Some(value)) => Ok(Some(value)),
            None => Ok(None),
            Some(None) => Err(CodecError::Captured),
        }
    }
    fn next_value_seed<S: DeserializeSeed<'de>>(
        &mut self,
        seed: S,
    ) -> std::result::Result<S::Value, Self::Error> {
        let slots = if self.first_value { 16 } else { 4 };
        admit(
            std::mem::size_of::<S::Value>()
                .saturating_mul(slots)
                .saturating_add(128 + 11 * std::mem::size_of::<S::Value>()),
        )?;
        self.first_value = false;
        self.inner
            .next_value_seed(Seed(seed))
            .map_err(CodecError::Native)?
            .ok_or(CodecError::Captured)
    }
    fn size_hint(&self) -> Option<usize> {
        Some(0)
    }
}
struct Enum<A>(A);
impl<'de, A: de::EnumAccess<'de>> de::EnumAccess<'de> for Enum<A> {
    type Error = CodecError<A::Error>;
    type Variant = Variant<A::Variant>;
    fn variant_seed<S: DeserializeSeed<'de>>(
        self,
        seed: S,
    ) -> std::result::Result<(S::Value, Self::Variant), Self::Error> {
        let (value, variant) = self
            .0
            .variant_seed(Seed(seed))
            .map_err(CodecError::Native)?;
        Ok((value.ok_or(CodecError::Captured)?, Variant(variant)))
    }
}
struct Variant<A>(A);
impl<'de, A: de::VariantAccess<'de>> de::VariantAccess<'de> for Variant<A> {
    type Error = CodecError<A::Error>;
    fn unit_variant(self) -> std::result::Result<(), Self::Error> {
        self.0.unit_variant().map_err(CodecError::Native)
    }
    fn newtype_variant_seed<S: DeserializeSeed<'de>>(
        self,
        seed: S,
    ) -> std::result::Result<S::Value, Self::Error> {
        self.0
            .newtype_variant_seed(Seed(seed))
            .map_err(CodecError::Native)?
            .ok_or(CodecError::Captured)
    }
    fn tuple_variant<VV: Visitor<'de>>(
        self,
        len: usize,
        visitor: VV,
    ) -> std::result::Result<VV::Value, Self::Error> {
        let mut output = None;
        self.0
            .tuple_variant(
                len,
                V {
                    inner: visitor,
                    heap: false,
                    output: &mut output,
                },
            )
            .map_err(CodecError::Native)?;
        output.ok_or(CodecError::Captured)
    }
    fn struct_variant<VV: Visitor<'de>>(
        self,
        fields: &'static [&'static str],
        visitor: VV,
    ) -> std::result::Result<VV::Value, Self::Error> {
        let mut output = None;
        self.0
            .struct_variant(
                fields,
                V {
                    inner: visitor,
                    heap: false,
                    output: &mut output,
                },
            )
            .map_err(CodecError::Native)?;
        output.ok_or(CodecError::Captured)
    }
}
impl<'de, D: de::Deserializer<'de>> de::Deserializer<'de> for Admitted<D> {
    type Error = CodecError<D::Error>;
    fn deserialize_any<VV: Visitor<'de>>(
        self,
        visitor: VV,
    ) -> std::result::Result<VV::Value, Self::Error> {
        terminal::<D::Error>()?;
        let mut output = None;
        self.0
            .deserialize_any(V {
                inner: visitor,
                heap: false,
                output: &mut output,
            })
            .map_err(CodecError::Native)?;
        output.ok_or(CodecError::Captured)
    }
    fn deserialize_bool<VV: Visitor<'de>>(
        self,
        visitor: VV,
    ) -> std::result::Result<VV::Value, Self::Error> {
        terminal::<D::Error>()?;
        let mut output = None;
        self.0
            .deserialize_bool(V {
                inner: visitor,
                heap: false,
                output: &mut output,
            })
            .map_err(CodecError::Native)?;
        output.ok_or(CodecError::Captured)
    }
    fn deserialize_i8<VV: Visitor<'de>>(
        self,
        visitor: VV,
    ) -> std::result::Result<VV::Value, Self::Error> {
        terminal::<D::Error>()?;
        let mut output = None;
        self.0
            .deserialize_i8(V {
                inner: visitor,
                heap: false,
                output: &mut output,
            })
            .map_err(CodecError::Native)?;
        output.ok_or(CodecError::Captured)
    }
    fn deserialize_i16<VV: Visitor<'de>>(
        self,
        visitor: VV,
    ) -> std::result::Result<VV::Value, Self::Error> {
        terminal::<D::Error>()?;
        let mut output = None;
        self.0
            .deserialize_i16(V {
                inner: visitor,
                heap: false,
                output: &mut output,
            })
            .map_err(CodecError::Native)?;
        output.ok_or(CodecError::Captured)
    }
    fn deserialize_i32<VV: Visitor<'de>>(
        self,
        visitor: VV,
    ) -> std::result::Result<VV::Value, Self::Error> {
        terminal::<D::Error>()?;
        let mut output = None;
        self.0
            .deserialize_i32(V {
                inner: visitor,
                heap: false,
                output: &mut output,
            })
            .map_err(CodecError::Native)?;
        output.ok_or(CodecError::Captured)
    }
    fn deserialize_i64<VV: Visitor<'de>>(
        self,
        visitor: VV,
    ) -> std::result::Result<VV::Value, Self::Error> {
        terminal::<D::Error>()?;
        let mut output = None;
        self.0
            .deserialize_i64(V {
                inner: visitor,
                heap: false,
                output: &mut output,
            })
            .map_err(CodecError::Native)?;
        output.ok_or(CodecError::Captured)
    }
    fn deserialize_i128<VV: Visitor<'de>>(
        self,
        visitor: VV,
    ) -> std::result::Result<VV::Value, Self::Error> {
        terminal::<D::Error>()?;
        let mut output = None;
        self.0
            .deserialize_i128(V {
                inner: visitor,
                heap: false,
                output: &mut output,
            })
            .map_err(CodecError::Native)?;
        output.ok_or(CodecError::Captured)
    }
    fn deserialize_u8<VV: Visitor<'de>>(
        self,
        visitor: VV,
    ) -> std::result::Result<VV::Value, Self::Error> {
        terminal::<D::Error>()?;
        let mut output = None;
        self.0
            .deserialize_u8(V {
                inner: visitor,
                heap: false,
                output: &mut output,
            })
            .map_err(CodecError::Native)?;
        output.ok_or(CodecError::Captured)
    }
    fn deserialize_u16<VV: Visitor<'de>>(
        self,
        visitor: VV,
    ) -> std::result::Result<VV::Value, Self::Error> {
        terminal::<D::Error>()?;
        let mut output = None;
        self.0
            .deserialize_u16(V {
                inner: visitor,
                heap: false,
                output: &mut output,
            })
            .map_err(CodecError::Native)?;
        output.ok_or(CodecError::Captured)
    }
    fn deserialize_u32<VV: Visitor<'de>>(
        self,
        visitor: VV,
    ) -> std::result::Result<VV::Value, Self::Error> {
        terminal::<D::Error>()?;
        let mut output = None;
        self.0
            .deserialize_u32(V {
                inner: visitor,
                heap: false,
                output: &mut output,
            })
            .map_err(CodecError::Native)?;
        output.ok_or(CodecError::Captured)
    }
    fn deserialize_u64<VV: Visitor<'de>>(
        self,
        visitor: VV,
    ) -> std::result::Result<VV::Value, Self::Error> {
        terminal::<D::Error>()?;
        let mut output = None;
        self.0
            .deserialize_u64(V {
                inner: visitor,
                heap: false,
                output: &mut output,
            })
            .map_err(CodecError::Native)?;
        output.ok_or(CodecError::Captured)
    }
    fn deserialize_u128<VV: Visitor<'de>>(
        self,
        visitor: VV,
    ) -> std::result::Result<VV::Value, Self::Error> {
        terminal::<D::Error>()?;
        let mut output = None;
        self.0
            .deserialize_u128(V {
                inner: visitor,
                heap: false,
                output: &mut output,
            })
            .map_err(CodecError::Native)?;
        output.ok_or(CodecError::Captured)
    }
    fn deserialize_f32<VV: Visitor<'de>>(
        self,
        visitor: VV,
    ) -> std::result::Result<VV::Value, Self::Error> {
        terminal::<D::Error>()?;
        let mut output = None;
        self.0
            .deserialize_f32(V {
                inner: visitor,
                heap: false,
                output: &mut output,
            })
            .map_err(CodecError::Native)?;
        output.ok_or(CodecError::Captured)
    }
    fn deserialize_f64<VV: Visitor<'de>>(
        self,
        visitor: VV,
    ) -> std::result::Result<VV::Value, Self::Error> {
        terminal::<D::Error>()?;
        let mut output = None;
        self.0
            .deserialize_f64(V {
                inner: visitor,
                heap: false,
                output: &mut output,
            })
            .map_err(CodecError::Native)?;
        output.ok_or(CodecError::Captured)
    }
    fn deserialize_char<VV: Visitor<'de>>(
        self,
        visitor: VV,
    ) -> std::result::Result<VV::Value, Self::Error> {
        terminal::<D::Error>()?;
        let mut output = None;
        self.0
            .deserialize_char(V {
                inner: visitor,
                heap: false,
                output: &mut output,
            })
            .map_err(CodecError::Native)?;
        output.ok_or(CodecError::Captured)
    }
    fn deserialize_str<VV: Visitor<'de>>(
        self,
        visitor: VV,
    ) -> std::result::Result<VV::Value, Self::Error> {
        terminal::<D::Error>()?;
        let mut output = None;
        self.0
            .deserialize_str(V {
                inner: visitor,
                heap: false,
                output: &mut output,
            })
            .map_err(CodecError::Native)?;
        output.ok_or(CodecError::Captured)
    }
    fn deserialize_string<VV: Visitor<'de>>(
        self,
        visitor: VV,
    ) -> std::result::Result<VV::Value, Self::Error> {
        terminal::<D::Error>()?;
        let mut output = None;
        self.0
            .deserialize_str(V {
                inner: visitor,
                heap: false,
                output: &mut output,
            })
            .map_err(CodecError::Native)?;
        output.ok_or(CodecError::Captured)
    }
    fn deserialize_bytes<VV: Visitor<'de>>(
        self,
        visitor: VV,
    ) -> std::result::Result<VV::Value, Self::Error> {
        terminal::<D::Error>()?;
        let mut output = None;
        self.0
            .deserialize_bytes(V {
                inner: visitor,
                heap: false,
                output: &mut output,
            })
            .map_err(CodecError::Native)?;
        output.ok_or(CodecError::Captured)
    }
    fn deserialize_byte_buf<VV: Visitor<'de>>(
        self,
        visitor: VV,
    ) -> std::result::Result<VV::Value, Self::Error> {
        terminal::<D::Error>()?;
        let mut output = None;
        self.0
            .deserialize_bytes(V {
                inner: visitor,
                heap: false,
                output: &mut output,
            })
            .map_err(CodecError::Native)?;
        output.ok_or(CodecError::Captured)
    }
    fn deserialize_option<VV: Visitor<'de>>(
        self,
        visitor: VV,
    ) -> std::result::Result<VV::Value, Self::Error> {
        terminal::<D::Error>()?;
        let mut output = None;
        self.0
            .deserialize_option(V {
                inner: visitor,
                heap: false,
                output: &mut output,
            })
            .map_err(CodecError::Native)?;
        output.ok_or(CodecError::Captured)
    }
    fn deserialize_unit<VV: Visitor<'de>>(
        self,
        visitor: VV,
    ) -> std::result::Result<VV::Value, Self::Error> {
        terminal::<D::Error>()?;
        let mut output = None;
        self.0
            .deserialize_unit(V {
                inner: visitor,
                heap: false,
                output: &mut output,
            })
            .map_err(CodecError::Native)?;
        output.ok_or(CodecError::Captured)
    }
    fn deserialize_seq<VV: Visitor<'de>>(
        self,
        visitor: VV,
    ) -> std::result::Result<VV::Value, Self::Error> {
        terminal::<D::Error>()?;
        let mut output = None;
        self.0
            .deserialize_seq(V {
                inner: visitor,
                heap: payload_allocates(),
                output: &mut output,
            })
            .map_err(CodecError::Native)?;
        output.ok_or(CodecError::Captured)
    }
    fn deserialize_map<VV: Visitor<'de>>(
        self,
        visitor: VV,
    ) -> std::result::Result<VV::Value, Self::Error> {
        terminal::<D::Error>()?;
        let mut output = None;
        self.0
            .deserialize_map(V {
                inner: visitor,
                heap: true,
                output: &mut output,
            })
            .map_err(CodecError::Native)?;
        output.ok_or(CodecError::Captured)
    }
    fn deserialize_identifier<VV: Visitor<'de>>(
        self,
        visitor: VV,
    ) -> std::result::Result<VV::Value, Self::Error> {
        terminal::<D::Error>()?;
        let mut output = None;
        self.0
            .deserialize_identifier(V {
                inner: visitor,
                heap: false,
                output: &mut output,
            })
            .map_err(CodecError::Native)?;
        output.ok_or(CodecError::Captured)
    }
    fn deserialize_ignored_any<VV: Visitor<'de>>(
        self,
        visitor: VV,
    ) -> std::result::Result<VV::Value, Self::Error> {
        terminal::<D::Error>()?;
        let mut output = None;
        self.0
            .deserialize_ignored_any(V {
                inner: visitor,
                heap: false,
                output: &mut output,
            })
            .map_err(CodecError::Native)?;
        output.ok_or(CodecError::Captured)
    }
    fn deserialize_unit_struct<VV: Visitor<'de>>(
        self,
        name: &'static str,
        visitor: VV,
    ) -> std::result::Result<VV::Value, Self::Error> {
        terminal::<D::Error>()?;
        let mut output = None;
        self.0
            .deserialize_unit_struct(
                name,
                V {
                    inner: visitor,
                    heap: false,
                    output: &mut output,
                },
            )
            .map_err(CodecError::Native)?;
        output.ok_or(CodecError::Captured)
    }
    fn deserialize_newtype_struct<VV: Visitor<'de>>(
        self,
        name: &'static str,
        visitor: VV,
    ) -> std::result::Result<VV::Value, Self::Error> {
        terminal::<D::Error>()?;
        let mut output = None;
        self.0
            .deserialize_newtype_struct(
                name,
                V {
                    inner: visitor,
                    heap: false,
                    output: &mut output,
                },
            )
            .map_err(CodecError::Native)?;
        output.ok_or(CodecError::Captured)
    }
    fn deserialize_tuple<VV: Visitor<'de>>(
        self,
        len: usize,
        visitor: VV,
    ) -> std::result::Result<VV::Value, Self::Error> {
        terminal::<D::Error>()?;
        let mut output = None;
        self.0
            .deserialize_tuple(
                len,
                V {
                    inner: visitor,
                    heap: false,
                    output: &mut output,
                },
            )
            .map_err(CodecError::Native)?;
        output.ok_or(CodecError::Captured)
    }
    fn deserialize_tuple_struct<VV: Visitor<'de>>(
        self,
        name: &'static str,
        len: usize,
        visitor: VV,
    ) -> std::result::Result<VV::Value, Self::Error> {
        terminal::<D::Error>()?;
        let mut output = None;
        self.0
            .deserialize_tuple_struct(
                name,
                len,
                V {
                    inner: visitor,
                    heap: false,
                    output: &mut output,
                },
            )
            .map_err(CodecError::Native)?;
        output.ok_or(CodecError::Captured)
    }
    fn deserialize_struct<VV: Visitor<'de>>(
        self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: VV,
    ) -> std::result::Result<VV::Value, Self::Error> {
        terminal::<D::Error>()?;
        let mut output = None;
        self.0
            .deserialize_struct(
                name,
                fields,
                V {
                    inner: visitor,
                    heap: false,
                    output: &mut output,
                },
            )
            .map_err(CodecError::Native)?;
        output.ok_or(CodecError::Captured)
    }
    fn deserialize_enum<VV: Visitor<'de>>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: VV,
    ) -> std::result::Result<VV::Value, Self::Error> {
        terminal::<D::Error>()?;
        let mut output = None;
        self.0
            .deserialize_enum(
                name,
                variants,
                V {
                    inner: visitor,
                    heap: false,
                    output: &mut output,
                },
            )
            .map_err(CodecError::Native)?;
        output.ok_or(CodecError::Captured)
    }
    fn is_human_readable(&self) -> bool {
        self.0.is_human_readable()
    }
}
