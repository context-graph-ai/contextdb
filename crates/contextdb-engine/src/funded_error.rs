//! Private diagnostic custody. No conversion from this type to public Error is
//! implicit: helper returns, callbacks and worker slots are internal consumers.
use crate::memory_accounting::OwnedMemoryReservation;
use contextdb_core::Error;
use contextdb_core::read_memory::NativeRefusal;
use std::alloc::{Layout, alloc, dealloc};
use std::fmt;
use std::mem::ManuallyDrop;
use std::ptr::NonNull;

const QUEUE_MISSING_PREFIX: &str = "pending sink action disappeared: ";
const QUEUE_UNRESOLVED_PREFIX: &str = "sink action has an unresolved attempt: ";

pub(crate) type OriginalResult<T> = std::result::Result<T, OriginalError>;

/// Only a scalar bootstrap diagnosis and pointers travel through private results.
/// Wide diagnostics live in a cell admitted before their producer is invoked.
#[derive(Debug)]
pub(crate) struct OriginalError {
    diagnostic: CompactDiagnostic,
    pub(crate) storage: Option<OriginalScopeCustody>,
}
#[derive(Debug)]
enum CompactDiagnostic {
    Native(NativeRefusal),
    Prepared(PreparedDiagnostic),
}
#[derive(Debug)]
struct DiagnosticPayload {
    // Physical diagnostic destruction precedes its source invoice and scopes.
    diagnostic: Diagnostic,
    _original: Option<OwnedMemoryReservation>,
    storage: Option<OriginalScopeCustody>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OriginalStoragePhase {
    Database,
    Queue,
    ColdSource,
    ReceivedOuter,
    ReceivedBegin,
}
#[derive(Debug)]
pub(crate) enum OriginalScopeCause {
    Refused(NativeRefusal),
    Unqualified(Error),
}
/// One strong owner of the actual producing scope. Its prepaid cell holds the
/// secondary diagnosis, unwind payload and preceding scope owner. Native
/// admission clones keep that same cell alive; there is no parallel registry.
#[repr(transparent)]
pub(crate) struct OriginalScopeCustody {
    pub(crate) scope: crate::persistence::native_storage_admission::StorageAdmissionScope,
}
// Scope compaction must survive future edits to the private carrier. Its
// optional owner occupies one pointer, including the no-scope refusal case.
const _: () =
    assert!(std::mem::size_of::<Option<OriginalScopeCustody>>() == std::mem::size_of::<usize>());
impl fmt::Debug for OriginalScopeCustody {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OriginalScopeCustody")
            .finish_non_exhaustive()
    }
}
#[derive(Debug)]
enum Diagnostic {
    // Explicit provenance gap. Moving this error NEVER retrospectively bills
    // its already allocated payload. Kept so an inherited diagnosis is not lost.
    Unqualified(Error),
    Borrowed(Error),
    Paid(Error),
    Decode(bincode::error::DecodeError),
    Encode(bincode::error::EncodeError),
    // A native error from measuring a concrete JSON Value remains unchanged
    // and explicitly unqualified: no private serde_json layout is asserted.
    JsonMeasure(serde_json::Error),
    PaidDecode(bincode::error::DecodeError),
    MessagePack(rmp_serde::decode::Error),
    PaidMessagePack(rmp_serde::decode::Error),
    Storage(redb::Error),
    ReaderCoordination {
        path: std::path::PathBuf,
        reason: String,
    },
    Disk {
        current_bytes: u64,
        limit_bytes: u64,
        source: redb::Error,
    },
}
impl OriginalError {
    /// Prepare complete primary-diagnostic storage at the actual producer,
    /// before invoking a codec or checking queue state.
    pub(crate) fn prepare(
        original: &OwnedMemoryReservation,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> std::result::Result<PreparedDiagnostic, NativeRefusal> {
        PreparedDiagnostic::allocate(original, subsystem, operation, hint)
    }
    pub(crate) fn prepare_route(
        original: &OwnedMemoryReservation,
    ) -> OriginalResult<PreparedDiagnosticRoute> {
        Self::prepare(
            original,
            "relational",
            "statement_overlay",
            "Reduce prepared event bodies or raise MEMORY_LIMIT.",
        )
        .map(|cell| PreparedDiagnosticRoute(std::cell::RefCell::new(Some(cell))))
        .map_err(Self::native)
    }
    pub(crate) fn native(refusal: NativeRefusal) -> Self {
        Self {
            storage: None,
            diagnostic: CompactDiagnostic::Native(refusal),
        }
    }
    /// Reserve and allocate before the queue checks, using their known ID.
    /// A batch may pass its largest actual ID to cover every checked member.
    pub(crate) fn prepare_queue(
        original: &OwnedMemoryReservation,
        largest_id: u64,
    ) -> std::result::Result<AdmittedDiagnostic, NativeRefusal> {
        let mut digits = 1;
        let mut remaining = largest_id;
        while remaining >= 10 {
            remaining /= 10;
            digits += 1;
        }
        let bytes = QUEUE_MISSING_PREFIX
            .len()
            .max(QUEUE_UNRESOLVED_PREFIX.len())
            + digits;
        AdmittedDiagnostic::allocate(
            original,
            bytes,
            "relational",
            "statement_overlay",
            "Reduce prepared event bodies or raise MEMORY_LIMIT.",
        )
    }
    /// Move the already admitted String and its actual invoice. No admission,
    /// counting pass, or allocation can follow the failed queue check.
    pub(crate) fn queue(mut output: AdmittedDiagnostic, id: u64, unresolved: bool) -> Self {
        use fmt::Write;
        let prefix = if unresolved {
            QUEUE_UNRESOLVED_PREFIX
        } else {
            QUEUE_MISSING_PREFIX
        };
        write!(&mut output, "{prefix}{id}")
            .expect("prepared queue output covers the checked source ID");
        output.into_other()
    }
    /// The public form owns the SAME prepared cell, text and actual invoices.
    /// It is only used for this engine-created queue diagnosis; plugin errors
    /// never enter this conversion.
    pub(crate) fn queue_public(output: AdmittedDiagnostic, id: u64, unresolved: bool) -> Error {
        Self::queue(output, id, unresolved).into_public_prepared()
    }
    /// Transfer only an already prepared producer. A helper return is not an
    /// ownership handoff: the public Error continues to own this same cell.
    pub(crate) fn into_public_prepared(self) -> Error {
        let Self {
            diagnostic,
            storage,
        } = self;
        let CompactDiagnostic::Prepared(mut owner) = diagnostic else {
            unreachable!("public diagnostic owns producer-prepared custody")
        };
        let payload = unsafe { owner.pointer.as_mut() }
            .payload
            .as_mut()
            .expect("public diagnostic initialized");
        assert!(payload.storage.is_none(), "one producing scope chain");
        payload.storage = storage;
        let pointer = owner.pointer.cast();
        std::mem::forget(owner);
        // SAFETY: the unique prepared owner has moved into this bridge. Its
        // logical Error and exact invoices stay in the same native cell.
        Error::FundedEngine(unsafe {
            contextdb_core::error::FundedEngineError::from_owned(
                pointer,
                borrow_public_diagnostic,
                display_public_diagnostic,
                release_public_diagnostic,
            )
        })
    }
    pub(crate) fn is_memory_refusal(&self) -> bool {
        match &self.diagnostic {
            CompactDiagnostic::Native(refusal) => {
                matches!(refusal, NativeRefusal::MemoryBudgetExceeded { .. })
            }
            CompactDiagnostic::Prepared(owner) => owner.payload().is_memory_refusal(),
        }
    }
    pub(crate) fn source_is_qualified(&self) -> bool {
        self.storage
            .as_ref()
            .is_none_or(|custody| custody.scope.error_source_is_qualified())
            && match &self.diagnostic {
                CompactDiagnostic::Native(_) => true,
                CompactDiagnostic::Prepared(owner) => owner.payload().source_is_qualified(),
            }
    }
}
impl DiagnosticPayload {
    pub(crate) fn is_memory_refusal(&self) -> bool {
        match &self.diagnostic {
            Diagnostic::Unqualified(error)
            | Diagnostic::Borrowed(error)
            | Diagnostic::Paid(error) => crate::read_image_memory::is_memory_refusal(error),
            Diagnostic::Storage(redb::Error::AdmissionRefused(error)) => error
                .downcast_ref::<Error>()
                .is_some_and(crate::read_image_memory::is_memory_refusal),
            Diagnostic::Decode(_)
            | Diagnostic::PaidDecode(_)
            | Diagnostic::MessagePack(_)
            | Diagnostic::PaidMessagePack(_)
            | Diagnostic::Encode(_)
            | Diagnostic::JsonMeasure(_)
            | Diagnostic::Storage(_)
            | Diagnostic::ReaderCoordination { .. }
            | Diagnostic::Disk { .. } => false,
        }
    }
    pub(crate) fn source_is_qualified(&self) -> bool {
        if self
            .storage
            .as_ref()
            .is_some_and(|custody| !custody.scope.error_source_is_qualified())
        {
            return false;
        }
        match &self.diagnostic {
            Diagnostic::Unqualified(_)
            | Diagnostic::JsonMeasure(_)
            | Diagnostic::ReaderCoordination { .. } => false,
            // These redb sources are entirely scalar or static at their real
            // constructors. A custom I/O payload, table name, corruption text
            // or admission Box is not certified by retaining a zero-byte use.
            Diagnostic::Storage(redb::Error::Native(error)) => qualified_native_io(error),
            Diagnostic::Storage(redb::Error::PreparedRead(redb::PreparedReadError::Native(
                error,
            )))
            | Diagnostic::Storage(redb::Error::PreparedRead(redb::PreparedReadError::Storage(
                redb::StorageError::Native(error),
            ))) => qualified_native_io(error),
            Diagnostic::Storage(error) => matches!(
                error,
                redb::Error::DatabaseAlreadyOpen
                    | redb::Error::InvalidSavepoint
                    | redb::Error::ImmediateDurabilityRequired
                    | redb::Error::RepairAborted
                    | redb::Error::PersistentSavepointModified
                    | redb::Error::PersistentSavepointExists
                    | redb::Error::EphemeralSavepointExists
                    | redb::Error::TransactionInProgress
                    | redb::Error::UpgradeRequired(_)
                    | redb::Error::ValueTooLarge(_)
                    | redb::Error::DatabaseClosed
                    | redb::Error::PreviousIo
                    | redb::Error::LockPoisoned(_)
            ),
            Diagnostic::PaidDecode(_) | Diagnostic::PaidMessagePack(_) => true,
            Diagnostic::MessagePack(error) => matches!(
                error,
                rmp_serde::decode::Error::TypeMismatch(_)
                    | rmp_serde::decode::Error::OutOfRange
                    | rmp_serde::decode::Error::LengthMismatch(_)
                    | rmp_serde::decode::Error::Utf8Error(_)
                    | rmp_serde::decode::Error::DepthLimitExceeded
            ),
            Diagnostic::Disk { source, .. } => matches!(
                source,
                redb::Error::Native(redb::NativeColdReadError::BackendIo(
                    redb::NativeIoCause::DiskGrowthRefused { .. }
                ))
            ),
            Diagnostic::Paid(_) | Diagnostic::Borrowed(_) => true,
            Diagnostic::Decode(error) => matches!(
                error,
                bincode::error::DecodeError::UnexpectedEnd { .. }
                    | bincode::error::DecodeError::LimitExceeded
                    | bincode::error::DecodeError::InvalidIntegerType { .. }
                    | bincode::error::DecodeError::NonZeroTypeIsZero { .. }
                    | bincode::error::DecodeError::UnexpectedVariant { .. }
                    | bincode::error::DecodeError::Utf8 { .. }
                    | bincode::error::DecodeError::InvalidCharEncoding(_)
                    | bincode::error::DecodeError::InvalidBooleanValue(_)
                    | bincode::error::DecodeError::ArrayLengthMismatch { .. }
                    | bincode::error::DecodeError::OutsideUsizeRange(_)
                    | bincode::error::DecodeError::EmptyEnum { .. }
                    | bincode::error::DecodeError::InvalidDuration { .. }
                    | bincode::error::DecodeError::InvalidSystemTime { .. }
                    | bincode::error::DecodeError::CStringNulError { .. }
                    | bincode::error::DecodeError::Other(_)
                    | bincode::error::DecodeError::Serde(
                        bincode::serde::DecodeError::AnyNotSupported
                            | bincode::serde::DecodeError::IdentifierNotSupported
                            | bincode::serde::DecodeError::IgnoredAnyNotSupported
                            | bincode::serde::DecodeError::CannotBorrowOwnedData
                    )
            ),
            // Non-exhaustive foreign enums: an unknown future source is a gap.
            Diagnostic::Encode(error) => matches!(
                error,
                bincode::error::EncodeError::UnexpectedEnd
                    | bincode::error::EncodeError::RefCellAlreadyBorrowed { .. }
                    | bincode::error::EncodeError::Other(_)
                    | bincode::error::EncodeError::InvalidPathCharacters
                    | bincode::error::EncodeError::LockFailed { .. }
                    | bincode::error::EncodeError::Serde(
                        bincode::serde::EncodeError::SequenceMustHaveLength
                    )
            ),
        }
    }
}
impl fmt::Display for OriginalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.diagnostic {
            CompactDiagnostic::Native(refusal) => fmt::Display::fmt(refusal, f),
            CompactDiagnostic::Prepared(owner) => fmt::Display::fmt(owner.payload(), f),
        }
    }
}
impl fmt::Display for DiagnosticPayload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.diagnostic {
            Diagnostic::Unqualified(error)
            | Diagnostic::Borrowed(error)
            | Diagnostic::Paid(error) => fmt::Display::fmt(error, f),
            Diagnostic::Decode(error) | Diagnostic::PaidDecode(error) => {
                write!(f, "bincode decode error: {error}")
            }
            Diagnostic::MessagePack(error) | Diagnostic::PaidMessagePack(error) => {
                write!(f, "decode blob repository record: {error}")
            }
            Diagnostic::Encode(error) => write!(f, "bincode encode error: {error}"),
            Diagnostic::JsonMeasure(error) => write!(f, "cannot measure JSON preparation: {error}"),
            Diagnostic::Storage(redb::Error::AdmissionRefused(error)) => {
                if let Some(error) = error.downcast_ref::<Error>() {
                    fmt::Display::fmt(error, f)
                } else {
                    write!(f, "storage admission refused: {error}")
                }
            }
            Diagnostic::Storage(redb::Error::Corrupted(reason)) => {
                write!(f, "corrupt store at storage: {reason}")
            }
            Diagnostic::Storage(error) => write!(f, "contextdb-engine-storage-io:{error}"),
            Diagnostic::ReaderCoordination { path, reason } => write!(
                f,
                "storage reclamation cannot safely coordinate readers through {}: {reason}",
                path.display()
            ),
            Diagnostic::Disk {
                current_bytes,
                limit_bytes,
                ..
            } => write!(
                f,
                "disk budget exceeded: allocate storage pages - file is {current_bytes} bytes, limit is {limit_bytes} bytes. Hint: Reduce retained file-backed data or raise DISK_LIMIT before writing more data."
            ),
        }
    }
}

/// Admission is separate from diagnostic construction: if it refuses, the
/// caller still owns its source diagnosis and must not substitute the refusal.
/// Exact bytes are allocated only after a real Original debit. No message cap.
#[derive(Debug)]
pub(crate) struct AdmittedDiagnostic {
    // Physical String deallocation precedes the producing invoice on all exits.
    text: String,
    original: OwnedMemoryReservation,
    custody: PreparedDiagnostic,
}
impl AdmittedDiagnostic {
    fn allocate(
        original: &OwnedMemoryReservation,
        bytes: usize,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> std::result::Result<Self, NativeRefusal> {
        let layout = std::alloc::Layout::array::<u8>(bytes)
            .map_err(|_| NativeRefusal::Other("source allocation size overflow"))?;
        let custody = OriginalError::prepare(original, subsystem, operation, hint)?;
        let original = crate::memory_accounting::reserve_preparation_original_native(
            original, bytes, subsystem, operation, hint,
        )?;
        if bytes == 0 {
            return Ok(Self {
                text: String::new(),
                original,
                custody,
            });
        }
        let raw = unsafe { std::alloc::alloc(layout) };
        let Some(pointer) = std::ptr::NonNull::new(raw) else {
            return Err(NativeRefusal::AllocatorFailed {
                site: "diagnostic text",
                layout,
            });
        };
        let text = unsafe { String::from_raw_parts(pointer.as_ptr(), 0, bytes) };
        Ok(Self {
            text,
            original,
            custody,
        })
    }
    /// The only accepted formatter is this module's closed field-name recipe.
    /// Pinned serde_core 1.0.228 de::Error uses these exact bytes. No Display from
    /// a consumer runs in either pass; field names are copied verbatim.
    pub(crate) fn field(
        original: &OwnedMemoryReservation,
        field: &'static str,
        duplicate: bool,
        messagepack: bool,
    ) -> std::result::Result<OriginalError, NativeRefusal> {
        use fmt::Write;
        let prefix = if duplicate {
            "duplicate field `"
        } else {
            "missing field `"
        };
        let bytes = prefix
            .len()
            .checked_add(field.len())
            .and_then(|n| n.checked_add(1))
            .ok_or(NativeRefusal::Other("codec diagnostic length overflow"))?;
        let mut output = Self::allocate(
            original,
            bytes,
            "events",
            "decode_sink_queue_entry",
            "Reduce pending deliveries or raise MEMORY_LIMIT.",
        )?;
        output
            .write_str(prefix)
            .expect("exact closed diagnostic capacity");
        output
            .write_str(field)
            .expect("exact closed diagnostic capacity");
        output
            .write_str("`")
            .expect("exact closed diagnostic capacity");
        let Self {
            text,
            original,
            custody,
        } = output;
        let diagnostic = if messagepack {
            Diagnostic::PaidMessagePack(rmp_serde::decode::Error::Syntax(text))
        } else {
            Diagnostic::PaidDecode(bincode::error::DecodeError::OtherString(text))
        };
        Ok(custody.finish(diagnostic, Some(original)))
    }
    pub(crate) fn into_other(self) -> OriginalError {
        let Self {
            text,
            original,
            custody,
        } = self;
        custody.finish(Diagnostic::Paid(Error::Other(text.into())), Some(original))
    }
}

/// Exact experimental dependency Layout from actual allocator requests, not
/// sizeof(Error) or a private ABI mirror. Acceptance MUST bind rustc 1.94.1,
/// x86_64-unknown-linux-gnu, the calibrated profile and default/std-only
/// serde_json 1.0.149 artifact. Any dependency/compiler/profile/feature drift
/// invalidates it; source edits require a fresh composed-flow proof.
const JSON_NATIVE_REQUEST_BYTES: usize = 40;
const JSON_NATIVE_REQUEST_ALIGNMENT: usize = 8;
// Primary + secondary end_seq/end_map + position-repair replacement. No
// prepared return token exists on this unit-visitor/scalar-refusal bridge.
const JSON_NATIVE_WORK_SLOTS: usize = 3;

#[derive(Debug)]
pub(crate) struct AdmittedJsonDiagnostic {
    // Destination/carrier destruction precedes native-invoice destruction.
    destination: AdmittedDiagnostic,
    native: OwnedMemoryReservation,
}
impl AdmittedJsonDiagnostic {
    pub(crate) fn prepare(
        original: &OwnedMemoryReservation,
    ) -> std::result::Result<Self, NativeRefusal> {
        let request =
            Layout::from_size_align(JSON_NATIVE_REQUEST_BYTES, JSON_NATIVE_REQUEST_ALIGNMENT)
                .expect("calibrated native JSON request");
        let bytes = request.size() * JSON_NATIVE_WORK_SLOTS
            + contextdb_core::types::ClosedJsonReadHooks::native_message_bytes();
        // This is a reservation for genuine native allocations made later by
        // serde_json, not a dummy buffer or another payer for parser/payload.
        let native = crate::memory_accounting::reserve_preparation_original_native(
            original,
            bytes,
            "events",
            "decode_sink_queue_entry",
            "Reduce pending deliveries or raise MEMORY_LIMIT.",
        )?;
        let phrase = "control character (\\u0000-\\u001F) found while parsing a string";
        let digits = usize::MAX.ilog10() as usize + 1;
        let bytes = phrase
            .len()
            .max(contextdb_core::types::ClosedJsonReadHooks::native_message_bytes())
            + " at line ".len()
            + digits
            + " column ".len()
            + digits;
        let destination = AdmittedDiagnostic::allocate(
            original,
            bytes,
            "events",
            "decode_sink_queue_entry",
            "Reduce pending deliveries or raise MEMORY_LIMIT.",
        )?;
        #[cfg(test)]
        crate::persistence::content_identity::proof_allocation_oracle_tests::selected_json_producer_rows::prepared(native.bytes(), destination.original.bytes());
        Ok(Self {
            destination,
            native,
        })
    }
    pub(crate) fn refuse(self, error: NativeRefusal) -> OriginalError {
        let Self {
            destination,
            native,
        } = self;
        let AdmittedDiagnostic {
            text,
            original,
            custody,
        } = destination;
        // Core already destroyed secondary native errors and parser scratch.
        // Move the scalar into its paid carrier; unused text refunds only
        // after physical free.
        // No native custom or outer text is constructed after denial.
        drop(text);
        drop(native);
        drop(original);
        let error = crate::persistence::native_storage_admission::original_storage_error(
            crate::persistence::native_storage_admission::NativeStorageFailure::Refused(error),
        );
        custody.finish(Diagnostic::Paid(error), None)
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn finish(
        mut self,
        error: serde_json::Error,
        finite: Option<&'static str>,
        messagepack: bool,
    ) -> std::result::Result<OriginalError, (serde_json::Error, Self)> {
        use fmt::Write;
        // The recipe originates at explicit sites in the closed Value/Key
        // producer, never from formatting or recognizing an arbitrary Display.
        // Data without that provenance and all I/O remain unqualified.
        #[cfg(test)]
        crate::persistence::content_identity::proof_allocation_oracle_tests::selected_json_producer_rows::diagnosis(&error, finite);
        let known = error.is_syntax() || error.is_eof() || (error.is_data() && finite.is_some());
        if !known || write!(&mut self.destination, "{error}").is_err() {
            return Err((error, self));
        }
        // Primary box, Message backing, cleanup/rebox work and destination
        // overlap are additive. Native physical free precedes its sole refund.
        #[cfg(test)]
        crate::persistence::content_identity::proof_allocation_oracle_tests::selected_json_producer_rows::rendered(self.destination.text.as_ptr() as usize);
        drop(error);
        #[cfg(test)]
        crate::persistence::content_identity::proof_allocation_oracle_tests::selected_json_producer_rows::native_destroyed();
        let Self {
            destination,
            native,
        } = self;
        drop(native);
        let AdmittedDiagnostic {
            text,
            original,
            custody,
        } = destination;
        let diagnostic = if messagepack {
            Diagnostic::PaidMessagePack(rmp_serde::decode::Error::Syntax(text))
        } else {
            Diagnostic::PaidDecode(bincode::error::DecodeError::OtherString(text))
        };
        Ok(custody.finish(diagnostic, Some(original)))
    }
}

/// This allocation is made before fallible producer work. Both the complete
/// primary payload and its actual source reservation move into it on failure.
/// No allocation, admission or registry lookup occurs during that transfer.
#[repr(transparent)]
pub(crate) struct PreparedDiagnostic {
    pointer: NonNull<DiagnosticBlock>,
}
/// Stack-only access to one existing prepaid cell. Every terminal failure
/// consumes it once; successful sequential checks reuse the same preparation.
pub(crate) struct PreparedDiagnosticRoute(std::cell::RefCell<Option<PreparedDiagnostic>>);
impl PreparedDiagnosticRoute {
    fn take(&self) -> PreparedDiagnostic {
        self.0
            .borrow_mut()
            .take()
            .expect("one escaping producer failure")
    }
    pub(crate) fn error(&self, error: Error) -> OriginalError {
        self.take().unqualified(error)
    }
    pub(crate) fn decode(
        &self,
        error: bincode::error::DecodeError,
        original: OwnedMemoryReservation,
    ) -> OriginalError {
        self.take().decode(error, original)
    }
    pub(crate) fn encode(
        &self,
        error: bincode::error::EncodeError,
        original: OwnedMemoryReservation,
    ) -> OriginalError {
        self.take().encode(error, original)
    }
    pub(crate) fn storage(
        &self,
        original: &OwnedMemoryReservation,
        error: impl Into<redb::Error>,
    ) -> OriginalError {
        crate::persistence::RedbPersistence::storage_error_funded(self.take(), original, error)
    }
}
struct DiagnosticBlock {
    payload: ManuallyDrop<Option<DiagnosticPayload>>,
    invoice: ManuallyDrop<DiagnosticInvoice>,
}
enum DiagnosticInvoice {
    Original {
        _owner: OwnedMemoryReservation,
    },
    Storage {
        _owner: crate::persistence::native_storage_admission::StorageAllocation,
    },
}
struct DiagnosticFree {
    pointer: NonNull<DiagnosticBlock>,
    invoice: Option<DiagnosticInvoice>,
}
impl Drop for DiagnosticFree {
    fn drop(&mut self) {
        unsafe {
            dealloc(
                self.pointer.as_ptr().cast(),
                Layout::new::<DiagnosticBlock>(),
            )
        };
        drop(self.invoice.take());
    }
}
// The owner is unique; only shared access to the Send + Sync payload escapes.
unsafe impl Send for PreparedDiagnostic {}
unsafe impl Sync for PreparedDiagnostic {}
impl fmt::Debug for PreparedDiagnostic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PreparedDiagnostic").finish_non_exhaustive()
    }
}
impl PreparedDiagnostic {
    fn allocate(
        original: &OwnedMemoryReservation,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> std::result::Result<Self, NativeRefusal> {
        let layout = Layout::new::<DiagnosticBlock>();
        let invoice = crate::memory_accounting::reserve_preparation_original_native(
            original,
            layout.size(),
            subsystem,
            operation,
            hint,
        )?;
        Self::allocate_paid(DiagnosticInvoice::Original { _owner: invoice })
    }
    pub(crate) fn allocate_scope(
        scope: &crate::persistence::native_storage_admission::StorageAdmissionScope,
    ) -> std::result::Result<Self, NativeRefusal> {
        let invoice = scope.reserve_diagnostic(Layout::new::<DiagnosticBlock>())?;
        Self::allocate_paid(DiagnosticInvoice::Storage { _owner: invoice })
    }
    fn allocate_paid(invoice: DiagnosticInvoice) -> std::result::Result<Self, NativeRefusal> {
        let layout = Layout::new::<DiagnosticBlock>();
        let Some(pointer) = NonNull::new(unsafe { alloc(layout) }.cast::<DiagnosticBlock>()) else {
            return Err(NativeRefusal::AllocatorFailed {
                site: "original diagnostic",
                layout,
            });
        };
        unsafe {
            pointer.as_ptr().write(DiagnosticBlock {
                payload: ManuallyDrop::new(None),
                invoice: ManuallyDrop::new(invoice),
            })
        };
        Ok(Self { pointer })
    }
    fn payload(&self) -> &DiagnosticPayload {
        unsafe { self.pointer.as_ref() }
            .payload
            .as_ref()
            .expect("prepared diagnostic initialized before escape")
    }
    fn finish(
        mut self,
        diagnostic: Diagnostic,
        original: Option<OwnedMemoryReservation>,
    ) -> OriginalError {
        unsafe { self.pointer.as_mut() }.payload = ManuallyDrop::new(Some(DiagnosticPayload {
            diagnostic,
            _original: original,
            storage: None,
        }));
        OriginalError {
            diagnostic: CompactDiagnostic::Prepared(self),
            storage: None,
        }
    }
    /// The opaque original moves unchanged into storage allocated BEFORE the
    /// callback. This pays only the carrier; foreign backing stays unqualified.
    pub(crate) fn unqualified(self, error: Error) -> OriginalError {
        let paid = matches!(&error, Error::FundedEngine(owner) if owner.source_is_qualified());
        let borrowed = matches!(&error, Error::Other(text) if text.static_text().is_some())
            || matches!(&error, Error::MemoryBudgetExceeded {
                subsystem, operation, hint, .. }
                if subsystem.static_text().is_some() && operation.static_text().is_some()
                    && hint.static_text().is_some())
            || matches!(&error, Error::ReadCancelled | Error::NativeEngineOther(_));
        self.finish(
            if paid {
                Diagnostic::Paid(error)
            } else if borrowed {
                Diagnostic::Borrowed(error)
            } else {
                Diagnostic::Unqualified(error)
            },
            None,
        )
    }
    pub(crate) fn storage(
        self,
        error: redb::Error,
        original: &OwnedMemoryReservation,
    ) -> OriginalError {
        self.finish(
            Diagnostic::Storage(error),
            Some(crate::memory_accounting::retain_preparation_original(
                original,
            )),
        )
    }
    pub(crate) fn reader_coordination(
        self,
        path: std::path::PathBuf,
        reason: String,
        original: &OwnedMemoryReservation,
    ) -> OriginalError {
        self.finish(
            Diagnostic::ReaderCoordination { path, reason },
            Some(crate::memory_accounting::retain_preparation_original(
                original,
            )),
        )
    }
    pub(crate) fn disk(
        self,
        current_bytes: u64,
        limit_bytes: u64,
        source: redb::Error,
        original: &OwnedMemoryReservation,
    ) -> OriginalError {
        self.finish(
            Diagnostic::Disk {
                current_bytes,
                limit_bytes,
                source,
            },
            Some(crate::memory_accounting::retain_preparation_original(
                original,
            )),
        )
    }
    pub(crate) fn decode(
        self,
        error: bincode::error::DecodeError,
        original: OwnedMemoryReservation,
    ) -> OriginalError {
        self.finish(Diagnostic::Decode(error), Some(original))
    }
    pub(crate) fn messagepack(
        self,
        error: rmp_serde::decode::Error,
        original: OwnedMemoryReservation,
    ) -> OriginalError {
        self.finish(Diagnostic::MessagePack(error), Some(original))
    }
    pub(crate) fn json_measure(self, error: serde_json::Error) -> OriginalError {
        self.finish(Diagnostic::JsonMeasure(error), None)
    }
    pub(crate) fn encode(
        self,
        error: bincode::error::EncodeError,
        original: OwnedMemoryReservation,
    ) -> OriginalError {
        self.finish(Diagnostic::Encode(error), Some(original))
    }
}
impl Drop for PreparedDiagnostic {
    fn drop(&mut self) {
        let block = unsafe { self.pointer.as_mut() };
        let invoice = unsafe { ManuallyDrop::take(&mut block.invoice) };
        let _free = DiagnosticFree {
            pointer: self.pointer,
            invoice: Some(invoice),
        };
        unsafe { ManuallyDrop::drop(&mut block.payload) };
    }
}
unsafe fn borrow_public_diagnostic(pointer: NonNull<()>) -> (Option<NonNull<Error>>, bool) {
    let block = unsafe { pointer.cast::<DiagnosticBlock>().as_ref() };
    let payload = block
        .payload
        .as_ref()
        .expect("public diagnostic initialized");
    let error = match &payload.diagnostic {
        Diagnostic::Paid(error) | Diagnostic::Borrowed(error) | Diagnostic::Unqualified(error) => {
            Some(NonNull::from(error))
        }
        _ => None,
    };
    (error, payload.source_is_qualified())
}
unsafe fn display_public_diagnostic(
    pointer: NonNull<()>,
    f: &mut fmt::Formatter<'_>,
) -> fmt::Result {
    let block = unsafe { pointer.cast::<DiagnosticBlock>().as_ref() };
    fmt::Display::fmt(
        block
            .payload
            .as_ref()
            .expect("public diagnostic initialized"),
        f,
    )
}
unsafe fn release_public_diagnostic(pointer: NonNull<()>) {
    drop(PreparedDiagnostic {
        pointer: pointer.cast(),
    });
}

impl fmt::Write for AdmittedDiagnostic {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        let next = self.text.len().checked_add(text.len()).ok_or(fmt::Error)?;
        if next > self.text.capacity() {
            return Err(fmt::Error);
        }
        self.text.push_str(text);
        Ok(())
    }
}

// Only the finite source-local backend recipes qualify, including through the
// original prepared-read wrappers. Opaque I/O never borrows this certificate.
fn qualified_native_io(error: &redb::NativeColdReadError) -> bool {
    matches!(
        error,
        redb::NativeColdReadError::BackendIo(
            redb::NativeIoCause::WriterOwnerClosed
                | redb::NativeIoCause::ProjectedGrowthPoisoned
                | redb::NativeIoCause::DiskGrowthRefused { .. }
        )
    )
}

/// Build the existing corruption class from borrowed operation data. The caller
/// supplies its actual payer; neither field allocates until admission succeeds.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(crate) fn corruption_display(
    owner: contextdb_core::Result<contextdb_core::read_memory::ReadMemoryOwner>,
    path: impl fmt::Display,
    reason: impl fmt::Display,
) -> Error {
    let result = (|| {
        let owner = owner?;
        let path = diagnostic_text(owner.clone(), path)?;
        let reason = diagnostic_text(owner, reason)?;
        Ok(Error::StoreCorrupted { path, reason })
    })();
    match result {
        Ok(error) | Err(error) => error,
    }
}

/// Re-label an already owned diagnosis without rendering or copying its text.
/// Only the borrowed runtime path needs new, independently retained storage.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(crate) fn corruption_text(
    owner: contextdb_core::Result<contextdb_core::read_memory::ReadMemoryOwner>,
    path: impl fmt::Display,
    reason: contextdb_core::error::DiagnosticText,
) -> Error {
    match owner.and_then(|owner| diagnostic_text(owner, path)) {
        Ok(path) => Error::StoreCorrupted { path, reason },
        Err(error) => error,
    }
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(crate) fn diagnostic_text(
    owner: contextdb_core::read_memory::ReadMemoryOwner,
    value: impl fmt::Display,
) -> contextdb_core::Result<contextdb_core::error::DiagnosticText> {
    contextdb_core::error::DiagnosticText::try_format(
        owner,
        value,
        "diagnostic",
        "prepare_diagnostic",
        "Raise the declared memory limit to report this operation.",
    )
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(crate) fn original_budget(
    original: &OwnedMemoryReservation,
) -> contextdb_core::Result<contextdb_core::read_memory::ReadMemoryOwner> {
    crate::memory_accounting::persistence_original_projection(original)?.read_budget()
}

/// Adapt a borrowed diagnostic at its caller's existing receiver boundary.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(crate) fn map_diagnostic(
    owner: contextdb_core::Result<contextdb_core::read_memory::ReadMemoryOwner>,
    value: impl fmt::Display,
    receive: impl FnOnce(contextdb_core::error::DiagnosticText) -> Error,
) -> Error {
    match owner.and_then(|owner| diagnostic_text(owner, value)) {
        Ok(text) => receive(text),
        Err(error) => error,
    }
}
