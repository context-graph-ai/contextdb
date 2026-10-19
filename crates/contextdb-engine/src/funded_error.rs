//! Private diagnostic custody. No conversion from this type to public Error is
//! implicit: helper returns, callbacks and worker slots are internal consumers.
use crate::memory_accounting::OwnedMemoryReservation;
use contextdb_core::read_memory::NativeRefusal;
use contextdb_core::Error;
use std::fmt;

const QUEUE_MISSING_PREFIX: &str = "pending sink action disappeared: ";
const QUEUE_UNRESOLVED_PREFIX: &str = "sink action has an unresolved attempt: ";

pub(crate) type OriginalResult<T> = std::result::Result<T, OriginalError>;

/// The diagnostic is physically destroyed before its producing reservation.
/// This object moves on the stack; it allocates no Box, Arc, registry or TLS cell.
#[derive(Debug)]
pub(crate) struct OriginalError {
    diagnostic: Diagnostic,
    storage: OriginalStorageCustody,
    original: Option<OwnedMemoryReservation>,
}
/// These five slots correspond to the actual funded storage call graph. The
/// database phase is entered only without an active scope; received begin can
/// nest inside received outer. Cold-source callbacks do not reenter that phase.
/// There is no recursive carrier, growth, allocation or diagnostic priority.
#[derive(Debug, Default)]
struct OriginalStorageCustody {
    database: Option<OriginalScopeCustody>,
    queue: Option<OriginalScopeCustody>,
    cold_source: Option<OriginalScopeCustody>,
    received_outer: Option<OriginalScopeCustody>,
    received_begin: Option<OriginalScopeCustody>,
}
#[derive(Clone, Copy)]
pub(crate) enum OriginalStoragePhase { Database, Queue, ColdSource, ReceivedOuter, ReceivedBegin }
#[derive(Debug)]
pub(crate) enum OriginalScopeCause { Refused(NativeRefusal), Unqualified(Error) }
/// The secondary diagnosis dies before its original scope owner. That owner's
/// final deallocator frees the scope cell before refunding its exact invoice.
pub(crate) struct OriginalScopeCustody {
    pub(crate) secondary: Option<OriginalScopeCause>,
    pub(crate) discard_unwind: Option<Box<dyn std::any::Any + Send>>,
    pub(crate) _scope: redb::NativeStorageAdmission,
}
impl fmt::Debug for OriginalScopeCustody {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OriginalScopeCustody").field("secondary", &self.secondary)
            .field("discard_unwind", &self.discard_unwind.is_some()).finish_non_exhaustive()
    }
}
#[derive(Debug)]
enum Diagnostic {
    // Explicit provenance gap. Moving this error NEVER retrospectively bills
    // its already allocated payload. Kept so an inherited diagnosis is not lost.
    Unqualified(Error),
    Borrowed(Error),
    Paid(Error),
    Native(NativeRefusal),
    Decode(bincode::error::DecodeError),
    Encode(bincode::error::EncodeError),
    Storage(redb::Error),
    ReaderCoordination { path: std::path::PathBuf, reason: String },
    Disk { current_bytes: u64, limit_bytes: u64 },
}
impl OriginalError {
    pub(crate) fn retain_storage_phase(mut self, phase: OriginalStoragePhase,
        custody: OriginalScopeCustody) -> Self {
        let slot = match phase {
            OriginalStoragePhase::Database => &mut self.storage.database,
            OriginalStoragePhase::Queue => &mut self.storage.queue,
            OriginalStoragePhase::ColdSource => &mut self.storage.cold_source,
            OriginalStoragePhase::ReceivedOuter => &mut self.storage.received_outer,
            OriginalStoragePhase::ReceivedBegin => &mut self.storage.received_begin,
        };
        assert!(slot.is_none(), "one entry of each funded storage phase");
        *slot = Some(custody);
        self
    }
    pub(crate) fn unqualified(error: Error) -> Self {
        let borrowed = matches!(&error,
            Error::Other(std::borrow::Cow::Borrowed(_))
            | Error::MemoryBudgetExceeded {
                subsystem: std::borrow::Cow::Borrowed(_), operation: std::borrow::Cow::Borrowed(_),
                hint: std::borrow::Cow::Borrowed(_), .. }
            | Error::ReadCancelled);
        Self { storage: OriginalStorageCustody::default(), diagnostic: if borrowed { Diagnostic::Borrowed(error) }
            else { Diagnostic::Unqualified(error) }, original: None }
    }
    pub(crate) fn native(refusal: NativeRefusal) -> Self {
        Self { storage: OriginalStorageCustody::default(), diagnostic: Diagnostic::Native(refusal), original: None }
    }
    pub(crate) fn decode(
        error: bincode::error::DecodeError,
        original: OwnedMemoryReservation,
    ) -> Self {
        // This is the decoder's actual ledger, moved without refund/readmit.
        // OtherString/Io source allocations are NOT certified by that ledger.
        Self { storage: OriginalStorageCustody::default(), diagnostic: Diagnostic::Decode(error), original: Some(original) }
            .materialize_other("events", "decode_sink_queue_entry", "Reduce pending deliveries or raise MEMORY_LIMIT.")
    }
    pub(crate) fn encode(
        error: bincode::error::EncodeError,
        original: OwnedMemoryReservation,
    ) -> Self {
        Self { storage: OriginalStorageCustody::default(), diagnostic: Diagnostic::Encode(error), original: Some(original) }
            .materialize_other("relational", "statement_overlay", "Reduce prepared event bodies or raise MEMORY_LIMIT.")
    }
    pub(crate) fn storage(error: redb::Error, original: &OwnedMemoryReservation) -> Self {
        Self { storage: OriginalStorageCustody::default(), diagnostic: Diagnostic::Storage(error),
            original: Some(crate::memory_accounting::retain_preparation_original(original)) }
    }
    pub(crate) fn reader_coordination(path: std::path::PathBuf, reason: String,
        original: &OwnedMemoryReservation) -> Self {
        Self { storage: OriginalStorageCustody::default(), diagnostic: Diagnostic::ReaderCoordination { path, reason },
            original: Some(crate::memory_accounting::retain_preparation_original(original)) }
    }
    pub(crate) fn disk(current_bytes: u64, limit_bytes: u64, original: &OwnedMemoryReservation) -> Self {
        Self { storage: OriginalStorageCustody::default(), diagnostic: Diagnostic::Disk { current_bytes, limit_bytes },
            original: Some(crate::memory_accounting::retain_preparation_original(original)) }
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
        let bytes = QUEUE_MISSING_PREFIX.len().max(QUEUE_UNRESOLVED_PREFIX.len()) + digits;
        AdmittedDiagnostic::allocate(original, bytes, "relational", "statement_overlay",
            "Reduce prepared event bodies or raise MEMORY_LIMIT.")
    }
    /// Move the already admitted String and its actual invoice. No admission,
    /// counting pass, or allocation can follow the failed queue check.
    pub(crate) fn queue(mut output: AdmittedDiagnostic, id: u64, unresolved: bool) -> Self {
        use fmt::Write;
        let prefix = if unresolved { QUEUE_UNRESOLVED_PREFIX } else { QUEUE_MISSING_PREFIX };
        write!(&mut output, "{prefix}{id}")
            .expect("prepared queue output covers the checked source ID");
        output.into_other()
    }
    // Only known allocation-free, stable sources may be formatted twice.
    // A failed admission keeps the SAME deferred diagnosis; it never returns
    // the new refusal in place of the producer's corruption/codec error.
    fn materialize_other(mut self, subsystem: &'static str, operation: &'static str,
        hint: &'static str) -> Self {
        use fmt::Write;
        if !self.source_is_qualified() { return self; }
        struct Count(usize);
        impl fmt::Write for Count {
            fn write_str(&mut self, text: &str) -> fmt::Result {
                self.0 = self.0.checked_add(text.len()).ok_or(fmt::Error)?;
                Ok(())
            }
        }
        let mut count = Count(0);
        if write!(&mut count, "{self}").is_err() { return self; }
        let Some(original) = self.original.as_ref() else { return self; };
        let Ok(mut text) = AdmittedDiagnostic::allocate(original, count.0, subsystem, operation, hint)
            else { return self; };
        if write!(&mut text, "{self}").is_err() || text.text.len() != count.0 {
            // Source remains owned and unchanged; scratch frees before refund.
            return self;
        }
        // Scratch's grant paid this exact String BEFORE allocation. It now
        // replaces an allocation-free source, not an already allocated Error.
        let mut paid = text.into_other();
        paid.storage = std::mem::take(&mut self.storage);
        drop(self);
        paid
    }
    pub(crate) fn is_memory_refusal(&self) -> bool {
        match &self.diagnostic {
            Diagnostic::Unqualified(error) | Diagnostic::Borrowed(error) | Diagnostic::Paid(error) =>
                crate::read_image_memory::is_memory_refusal(error),
            // Preserve the existing original_storage_error consumer class:
            // its generic Process fallback is Other, not a typed memory error.
            Diagnostic::Native(refusal) => matches!(refusal,
                NativeRefusal::MemoryBudgetExceeded { .. }),
            Diagnostic::Storage(redb::Error::AdmissionRefused(error)) => error.downcast_ref::<Error>()
                .is_some_and(crate::read_image_memory::is_memory_refusal),
            Diagnostic::Decode(_) | Diagnostic::Encode(_) | Diagnostic::Storage(_)
            | Diagnostic::ReaderCoordination { .. } | Diagnostic::Disk { .. } => false,
        }
    }
    /// A positive result certifies only that the SOURCE has no unadmitted
    /// owned allocation. Deferred codec display itself allocates no String.
    pub(crate) fn source_is_qualified(&self) -> bool {
        if [&self.storage.database, &self.storage.queue, &self.storage.cold_source,
            &self.storage.received_outer, &self.storage.received_begin].into_iter()
            .flatten().any(|c| c.discard_unwind.is_some()
                || matches!(&c.secondary, Some(OriginalScopeCause::Unqualified(_)))) {
            return false;
        }
        match &self.diagnostic {
            Diagnostic::Unqualified(_) | Diagnostic::Storage(_)
            | Diagnostic::ReaderCoordination { .. } => false,
            Diagnostic::Disk { .. } => true,
            Diagnostic::Paid(_) | Diagnostic::Borrowed(_) | Diagnostic::Native(_) => true,
            Diagnostic::Decode(error) => matches!(error,
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
                | bincode::error::DecodeError::Serde(bincode::serde::DecodeError::AnyNotSupported
                    | bincode::serde::DecodeError::IdentifierNotSupported
                    | bincode::serde::DecodeError::IgnoredAnyNotSupported
                    | bincode::serde::DecodeError::CannotBorrowOwnedData)),
            // Non-exhaustive foreign enums: an unknown future source is a gap.
            Diagnostic::Encode(error) => matches!(error,
                bincode::error::EncodeError::UnexpectedEnd
                | bincode::error::EncodeError::RefCellAlreadyBorrowed { .. }
                | bincode::error::EncodeError::Other(_)
                | bincode::error::EncodeError::InvalidPathCharacters
                | bincode::error::EncodeError::LockFailed { .. }
                | bincode::error::EncodeError::Serde(bincode::serde::EncodeError::SequenceMustHaveLength)),
        }
    }
}
impl From<Error> for OriginalError {
    fn from(error: Error) -> Self { Self::unqualified(error) }
}
impl fmt::Display for OriginalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.diagnostic {
            Diagnostic::Unqualified(error) | Diagnostic::Borrowed(error) | Diagnostic::Paid(error) => fmt::Display::fmt(error, f),
            Diagnostic::Native(error) => fmt::Display::fmt(error, f),
            Diagnostic::Decode(error) => write!(f, "bincode decode error: {error}"),
            Diagnostic::Encode(error) => write!(f, "bincode encode error: {error}"),
            Diagnostic::Storage(redb::Error::AdmissionRefused(error)) => {
                if let Some(error) = error.downcast_ref::<Error>() { fmt::Display::fmt(error, f) }
                else { write!(f, "storage admission refused: {error}") }
            }
            Diagnostic::Storage(redb::Error::Corrupted(reason)) =>
                write!(f, "corrupt store at storage: {reason}"),
            Diagnostic::Storage(error) => write!(f, "contextdb-engine-storage-io:{error}"),
            Diagnostic::ReaderCoordination { path, reason } =>
                write!(f, "storage reclamation cannot safely coordinate readers through {}: {reason}", path.display()),
            Diagnostic::Disk { current_bytes, limit_bytes } => write!(f,
                "disk budget exceeded: allocate storage pages - file is {current_bytes} bytes, limit is {limit_bytes} bytes. Hint: Reduce retained file-backed data or raise DISK_LIMIT before writing more data."),
        }
    }
}

/// Admission is separate from diagnostic construction: if it refuses, the
/// caller still owns its source diagnosis and must not substitute the refusal.
/// Exact bytes are allocated only after a real Original debit. No message cap.
pub(crate) struct AdmittedDiagnostic {
    // Physical String deallocation precedes the producing invoice on all exits.
    text: String,
    original: OwnedMemoryReservation,
}
impl AdmittedDiagnostic {
    fn allocate(original: &OwnedMemoryReservation, bytes: usize,
        subsystem: &'static str, operation: &'static str, hint: &'static str,
    ) -> std::result::Result<Self, NativeRefusal> {
        let layout = std::alloc::Layout::array::<u8>(bytes)
            .map_err(|_| NativeRefusal::Other("source allocation size overflow"))?;
        let original = crate::memory_accounting::reserve_preparation_original_native(
            original, bytes, subsystem, operation, hint,
        )?;
        if bytes == 0 { return Ok(Self { text: String::new(), original }); }
        let raw = unsafe { std::alloc::alloc(layout) };
        let Some(pointer) = std::ptr::NonNull::new(raw) else {
            std::alloc::handle_alloc_error(layout)
        };
        let text = unsafe { String::from_raw_parts(pointer.as_ptr(), 0, bytes) };
        Ok(Self { text, original })
    }
    pub(crate) fn into_other(self) -> OriginalError {
        let Self { text, original } = self;
        OriginalError { storage: OriginalStorageCustody::default(), diagnostic: Diagnostic::Paid(Error::Other(text.into())), original: Some(original) }
    }
}

impl fmt::Write for AdmittedDiagnostic {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        let next = self.text.len().checked_add(text.len()).ok_or(fmt::Error)?;
        if next > self.text.capacity() { return Err(fmt::Error); }
        self.text.push_str(text);
        Ok(())
    }
}
