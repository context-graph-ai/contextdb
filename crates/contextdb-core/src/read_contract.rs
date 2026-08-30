//! Vocabulary shared by the bounded direct-reader and local-owner routes.
//!
//! This module deliberately contains values and type contracts only. Route selection,
//! persistence, channel framing, deadline waiting, and query execution are performed by the
//! route and transport layers that consume this vocabulary.

use crate::Value;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

const MIB: u64 = 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadLimits {
    pub result_rows: u64,
    pub result_bytes: u64,
    pub work: u64,
    pub active_ms: u64,
    pub memory: u64,
    pub cursor_page_rows: u64,
    pub cursor_page_bytes: u64,
    pub cursor_idle_ms: u64,
    pub cursor_lifetime_ms: u64,
}

impl Default for ReadLimits {
    fn default() -> Self {
        Self {
            result_rows: 500,
            result_bytes: Self::SHIPPED_RESULT_BYTES,
            work: 50_000,
            active_ms: 5_000,
            memory: Self::SHIPPED_MEMORY,
            cursor_page_rows: 100,
            cursor_page_bytes: Self::SHIPPED_CURSOR_PAGE_BYTES,
            cursor_idle_ms: 300_000,
            cursor_lifetime_ms: 1_800_000,
        }
    }
}

impl ReadLimits {
    pub const SHIPPED_RESULT_BYTES: u64 = 4 * MIB;
    pub const SHIPPED_MEMORY: u64 = 16 * MIB;
    pub const SHIPPED_CURSOR_PAGE_BYTES: u64 = MIB;

    pub fn validate(&self) -> Result<(), ReadContractViolation> {
        for (value, field) in [
            (self.result_rows, ReadLimitField::ResultRows),
            (self.result_bytes, ReadLimitField::ResultBytes),
            (self.work, ReadLimitField::Work),
            (self.active_ms, ReadLimitField::ActiveMs),
            (self.memory, ReadLimitField::Memory),
            (self.cursor_page_rows, ReadLimitField::CursorPageRows),
            (self.cursor_page_bytes, ReadLimitField::CursorPageBytes),
            (self.cursor_idle_ms, ReadLimitField::CursorIdleMs),
            (self.cursor_lifetime_ms, ReadLimitField::CursorLifetimeMs),
        ] {
            if value == 0 {
                return Err(ReadContractViolation::ZeroLimit { field });
            }
        }

        if self.cursor_page_rows > self.result_rows {
            return Err(ReadContractViolation::CursorPageRowsExceedResultRows);
        }
        if self.cursor_page_bytes > self.result_bytes {
            return Err(ReadContractViolation::CursorPageBytesExceedResultBytes);
        }
        if self.cursor_idle_ms > self.cursor_lifetime_ms {
            return Err(ReadContractViolation::CursorIdleExceedsLifetime);
        }

        Ok(())
    }

    pub fn stricter_of(
        requested: Self,
        owner_maximum: Self,
    ) -> Result<Self, ReadContractViolation> {
        requested.validate()?;
        owner_maximum.validate()?;

        let effective = Self {
            result_rows: requested.result_rows.min(owner_maximum.result_rows),
            result_bytes: requested.result_bytes.min(owner_maximum.result_bytes),
            work: requested.work.min(owner_maximum.work),
            active_ms: requested.active_ms.min(owner_maximum.active_ms),
            memory: requested.memory.min(owner_maximum.memory),
            cursor_page_rows: requested
                .cursor_page_rows
                .min(owner_maximum.cursor_page_rows),
            cursor_page_bytes: requested
                .cursor_page_bytes
                .min(owner_maximum.cursor_page_bytes),
            cursor_idle_ms: requested.cursor_idle_ms.min(owner_maximum.cursor_idle_ms),
            cursor_lifetime_ms: requested
                .cursor_lifetime_ms
                .min(owner_maximum.cursor_lifetime_ms),
        };
        effective.validate()?;
        Ok(effective)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct OwnerReadLimits {
    pub limits: ReadLimits,
    pub concurrency: u64,
}

impl Default for OwnerReadLimits {
    fn default() -> Self {
        Self {
            limits: ReadLimits::default(),
            concurrency: 4,
        }
    }
}

impl OwnerReadLimits {
    pub fn validate(&self) -> Result<(), ReadContractViolation> {
        self.limits.validate()?;
        if self.concurrency == 0 {
            return Err(ReadContractViolation::ZeroLimit {
                field: ReadLimitField::Concurrency,
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadClientTimeouts {
    pub connect_ms: u64,
    pub routing_retry_ms: u64,
    pub response_ms: u64,
}

impl Default for ReadClientTimeouts {
    fn default() -> Self {
        Self {
            connect_ms: 1_000,
            routing_retry_ms: 1_000,
            response_ms: 11_000,
        }
    }
}

impl ReadClientTimeouts {
    pub fn validate(&self) -> Result<(), ReadContractViolation> {
        validate_timeout(self.connect_ms, ReadTimeoutField::ConnectMs)?;
        validate_timeout(self.routing_retry_ms, ReadTimeoutField::RoutingRetryMs)?;
        validate_timeout(self.response_ms, ReadTimeoutField::ResponseMs)
    }

    pub fn validate_at(&self, now_ms: u64) -> Result<(), ReadContractViolation> {
        self.validate()?;
        validate_timeout_at(now_ms, self.connect_ms, ReadTimeoutField::ConnectMs)?;
        validate_timeout_at(
            now_ms,
            self.routing_retry_ms,
            ReadTimeoutField::RoutingRetryMs,
        )?;
        validate_timeout_at(now_ms, self.response_ms, ReadTimeoutField::ResponseMs)
    }

    pub fn validate_with_owner(
        &self,
        owner: OwnerServiceTimeouts,
    ) -> Result<(), ReadContractViolation> {
        self.validate()?;
        owner.validate()?;
        if self.response_ms <= owner.request_ms {
            return Err(ReadContractViolation::ResponseMustExceedRequest);
        }
        Ok(())
    }

    pub fn validate_with_owner_at(
        &self,
        owner: OwnerServiceTimeouts,
        now_ms: u64,
    ) -> Result<(), ReadContractViolation> {
        self.validate_with_owner(owner)?;
        // The owner deadline is the inner service boundary. Validate it first
        // so a service configuration that cannot construct its own deadline
        // is reported even though the necessarily-longer response deadline
        // cannot fit either.
        owner.validate_at(now_ms)?;
        self.validate_at(now_ms)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct OwnerServiceTimeouts {
    pub request_ms: u64,
    pub shutdown_drain_ms: u64,
}

impl Default for OwnerServiceTimeouts {
    fn default() -> Self {
        Self {
            request_ms: 10_000,
            shutdown_drain_ms: 10_000,
        }
    }
}

impl OwnerServiceTimeouts {
    pub fn validate(&self) -> Result<(), ReadContractViolation> {
        validate_timeout(self.request_ms, ReadTimeoutField::RequestMs)?;
        validate_timeout(self.shutdown_drain_ms, ReadTimeoutField::ShutdownDrainMs)
    }

    pub fn validate_at(&self, now_ms: u64) -> Result<(), ReadContractViolation> {
        self.validate()?;
        validate_timeout_at(now_ms, self.request_ms, ReadTimeoutField::RequestMs)?;
        validate_timeout_at(
            now_ms,
            self.shutdown_drain_ms,
            ReadTimeoutField::ShutdownDrainMs,
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReadLimitField {
    ResultRows,
    ResultBytes,
    Work,
    ActiveMs,
    Memory,
    CursorPageRows,
    CursorPageBytes,
    CursorIdleMs,
    CursorLifetimeMs,
    Concurrency,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReadFailureLimit {
    ResultRows,
    ResultBytes,
    Work,
    ActiveMs,
    Memory,
    CursorPageBytes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReadTimeoutField {
    ConnectMs,
    RoutingRetryMs,
    ResponseMs,
    RequestMs,
    ShutdownDrainMs,
}

fn validate_timeout(value: u64, field: ReadTimeoutField) -> Result<(), ReadContractViolation> {
    if value == 0 {
        return Err(ReadContractViolation::ZeroTimeout { field });
    }
    if value == u64::MAX {
        return Err(ReadContractViolation::TimeoutOverflow { field });
    }
    Ok(())
}

fn validate_timeout_at(
    now_ms: u64,
    value: u64,
    field: ReadTimeoutField,
) -> Result<(), ReadContractViolation> {
    now_ms
        .checked_add(value)
        .map(|_| ())
        .ok_or(ReadContractViolation::TimeoutOverflow { field })
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ReadContractViolation {
    #[error("read contract validation is not implemented")]
    Unimplemented,
    #[error("{field:?} must be positive")]
    ZeroLimit { field: ReadLimitField },
    #[error("{field:?} must be positive")]
    ZeroTimeout { field: ReadTimeoutField },
    #[error("cursor page rows must not exceed result rows")]
    CursorPageRowsExceedResultRows,
    #[error("cursor page bytes must not exceed result bytes")]
    CursorPageBytesExceedResultBytes,
    #[error("cursor idle time must not exceed cursor lifetime")]
    CursorIdleExceedsLifetime,
    #[error("client response timeout must exceed owner request timeout")]
    ResponseMustExceedRequest,
    #[error("timeout value overflows deadline arithmetic")]
    TimeoutOverflow { field: ReadTimeoutField },
    #[error("owner serving state and reason are contradictory")]
    OwnerReadStatusReasonMismatch,
    #[error("cursor row {row} has {actual} values for {expected} columns")]
    CursorRowArityMismatch {
        row: usize,
        expected: usize,
        actual: usize,
    },
    #[error("metadata item {item} does not match the page vocabulary")]
    MetadataItemVocabularyMismatch { item: usize },
    #[error("a metadata page with more items must carry a continuation")]
    MissingMetadataContinuation,
    #[error("a final metadata page must not carry a continuation")]
    UnexpectedMetadataContinuation,
    #[error("a metadata page with more items must publish at least one complete item")]
    EmptyMetadataPageWithMore,
    #[error("held-by-readers detail must report at least one observed direct reader")]
    HeldByReadersObservedCountZero,
    #[error("held-by-readers detail lists more verified readers than were observed")]
    VerifiedReadersExceedObserved,
    #[error("held-by-readers detail repeats verified reader {reader}")]
    DuplicateVerifiedReader { reader: usize },
}

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct DatabaseIdentity(pub [u8; 16]);

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct WriterRunNumber(pub [u8; 16]);

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct LocalUserIdentity(pub u64);

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ChannelAddress(pub [u8; 32]);

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ProcessStartIdentity(pub u64);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReaderProcessIdentity {
    pub process_id: u32,
    pub process_start: ProcessStartIdentity,
}

/// The deterministic location key for a reader breadcrumb. This is separate from the
/// breadcrumb payload: the payload names the reader, while this key names the runtime file.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ReaderBreadcrumbLocation(pub ChannelAddress);

impl ReaderBreadcrumbLocation {
    pub const fn for_database_path_hash(database_path_hash: ChannelAddress) -> Self {
        Self(database_path_hash)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReaderBreadcrumb {
    pub process_id: u32,
    pub process_name: String,
    pub process_start: ProcessStartIdentity,
}

impl ReaderBreadcrumb {
    pub fn is_live_for(&self, observed: &ReaderProcessIdentity) -> bool {
        self.process_id == observed.process_id && self.process_start == observed.process_start
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReadRoute {
    File,
    Owner,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OwnerServingState {
    Serving,
    ServingDisabled,
    NotServing,
    NotApplicable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OwnerServingReason {
    DisabledByConfiguration,
    StartupFailure(String),
    PlatformUnsupported,
    ShutdownDraining,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OwnerReadStatus {
    pub state: OwnerServingState,
    pub reason: Option<OwnerServingReason>,
}

impl OwnerServingState {
    /// The stable word every surface prints for this state. Machine output and
    /// a refusal reason must not invent two vocabularies for one fact, so both
    /// take their word from here.
    pub const fn wire_word(self) -> &'static str {
        match self {
            Self::Serving => "serving",
            Self::ServingDisabled => "serving_disabled",
            Self::NotServing => "not_serving",
            Self::NotApplicable => "not_applicable",
        }
    }
}

impl OwnerServingReason {
    /// The stable word for why inspection is unavailable. A startup failure's
    /// operating-system detail travels separately, so this stays a word a
    /// script can branch on.
    pub const fn wire_word(&self) -> &'static str {
        match self {
            Self::DisabledByConfiguration => "disabled_by_configuration",
            Self::StartupFailure(_) => "startup_failure",
            Self::PlatformUnsupported => "platform_unsupported",
            Self::ShutdownDraining => "shutdown_draining",
        }
    }
}

impl OwnerReadStatus {
    /// One line naming why this store's owner cannot be asked, for a caller
    /// being REFUSED rather than reading a status document: the stable word
    /// first, so a script branches on it, then the operating-system detail the
    /// writer recorded when it had one.
    pub fn refusal_reason(&self) -> String {
        match &self.reason {
            Some(reason @ OwnerServingReason::StartupFailure(detail))
                if !detail.trim().is_empty() =>
            {
                format!("{}: {detail}", reason.wire_word())
            }
            Some(reason) => reason.wire_word().to_owned(),
            None => self.state.wire_word().to_owned(),
        }
    }

    pub fn validate(&self) -> Result<(), ReadContractViolation> {
        match (&self.state, &self.reason) {
            (OwnerServingState::Serving, None)
            | (
                OwnerServingState::ServingDisabled,
                Some(OwnerServingReason::DisabledByConfiguration),
            )
            | (OwnerServingState::NotServing, Some(OwnerServingReason::StartupFailure(_)))
            | (OwnerServingState::NotServing, Some(OwnerServingReason::ShutdownDraining))
            | (OwnerServingState::NotApplicable, None)
            | (OwnerServingState::NotApplicable, Some(OwnerServingReason::PlatformUnsupported)) => {
                Ok(())
            }
            _ => Err(ReadContractViolation::OwnerReadStatusReasonMismatch),
        }
    }
}

/// Monotonic deadline source for channel waits. This intentionally differs from
/// [`crate::Wallclock`], which stamps persisted data and has a thread-local test seam.
pub type DeadlineWait<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

pub trait DeadlineClock: Send + Sync + 'static {
    fn now_ms(&self) -> u64;

    fn wait_until(&self, deadline_ms: u64) -> DeadlineWait<'_>;
}

/// A callback registered to run the moment a read is cancelled.
type CancellationCallback = Arc<dyn Fn() + Send + Sync>;

struct CancellationToken {
    cancelled: AtomicBool,
    identity: u64,
    /// Who has asked to be told the moment this token is cancelled.
    ///
    /// A caller that is blocked waiting on something else -- a reply from
    /// another process, say -- cannot notice a flag being set. Watching it
    /// would mean waking up to look, over and over, at a rate nobody can
    /// choose well: too often and the waiting costs more than the read, too
    /// rarely and the caller keeps working long after it was told to stop. So
    /// the cancellation tells THEM, once, on the thread that cancelled.
    told_on_cancel: Mutex<Vec<(u64, CancellationCallback)>>,
}

impl std::fmt::Debug for CancellationToken {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CancellationToken")
            .field("cancelled", &self.cancelled)
            .field("identity", &self.identity)
            .finish_non_exhaustive()
    }
}

static NEXT_CANCELLATION_IDENTITY: AtomicU64 = AtomicU64::new(1);
static NEXT_CANCELLATION_LISTENER: AtomicU64 = AtomicU64::new(1);

/// Registration for being told when a read is cancelled, which stops being
/// told when it is dropped.
///
/// The registration is tied to one operation, not to the token: a token
/// outlives the request it interrupted, and a listener that outlived its own
/// request would act on a later cancellation about work it no longer has.
pub struct CancellationListener {
    token: Arc<CancellationToken>,
    listener: u64,
}

impl std::fmt::Debug for CancellationListener {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CancellationListener")
            .field("listener", &self.listener)
            .finish_non_exhaustive()
    }
}

impl Drop for CancellationListener {
    fn drop(&mut self) {
        let mut listeners = self
            .token
            .told_on_cancel
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        listeners.retain(|(identity, _)| *identity != self.listener);
    }
}

#[derive(Debug, Clone)]
pub struct OwnerReadCancellation {
    token: Arc<CancellationToken>,
}

impl Default for OwnerReadCancellation {
    fn default() -> Self {
        Self::new()
    }
}

impl OwnerReadCancellation {
    pub fn new() -> Self {
        Self {
            token: Arc::new(CancellationToken {
                cancelled: AtomicBool::new(false),
                identity: NEXT_CANCELLATION_IDENTITY.fetch_add(1, Ordering::SeqCst),
                told_on_cancel: Mutex::new(Vec::new()),
            }),
        }
    }

    pub fn cancel(&self) {
        self.token.cancelled.store(true, Ordering::SeqCst);
        // Taken out from under the lock and called with nothing held: a
        // listener is caller code, it may send, wait, or take locks of its
        // own, and it must not be able to hold up the next thing this token
        // has to tell somebody.
        let listeners: Vec<_> = {
            let listeners = self
                .token
                .told_on_cancel
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            listeners.iter().map(|(_, told)| Arc::clone(told)).collect()
        };
        for told in listeners {
            told();
        }
    }

    /// Ask to be told the moment this read is cancelled.
    ///
    /// A caller blocked on something that is not this token -- waiting for
    /// another process to reply, most of all -- cannot see a flag being set.
    /// This is how it hears instead. A token that has ALREADY been cancelled
    /// tells the new listener immediately, so a registration can never fall
    /// into the gap between the cancellation and itself. The registration
    /// ends when the returned value is dropped.
    pub fn tell_on_cancel(&self, told: impl Fn() + Send + Sync + 'static) -> CancellationListener {
        let told: Arc<dyn Fn() + Send + Sync> = Arc::new(told);
        let listener = NEXT_CANCELLATION_LISTENER.fetch_add(1, Ordering::SeqCst);
        {
            let mut listeners = self
                .token
                .told_on_cancel
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            listeners.push((listener, Arc::clone(&told)));
        }
        let registration = CancellationListener {
            token: Arc::clone(&self.token),
            listener,
        };
        if self.is_cancelled() {
            told();
        }
        registration
    }

    pub fn is_cancelled(&self) -> bool {
        self.token.cancelled.load(Ordering::SeqCst)
    }

    /// Which token this is.
    ///
    /// Clones of one token share an identity because they are one token; two
    /// separately minted tokens never do. That is what lets a caller show
    /// that the read it started is the read its own cancel would stop, rather
    /// than some other read that merely also finished. The number is minted
    /// with the token rather than read off where it happens to live, so a
    /// token that has been released cannot lend its name to the next one.
    pub fn identity(&self) -> u64 {
        self.token.identity
    }
}

pub trait OwnerRequestHandler: Send + Sync {
    fn handle(
        &self,
        namespace: &str,
        request: &[u8],
        cancel: &OwnerReadCancellation,
    ) -> crate::Result<Vec<u8>>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReadFailureKind {
    WriteRequiresFlag,
    HeldByWriter,
    HeldByReaders,
    OwnerNotRunning,
    OwnerNotServing,
    OwnerUserMismatch,
    OwnerMismatch,
    OwnerAtCapacity,
    OwnerLimitExceeded,
    OwnerTimeout,
    OwnerDisconnected,
    InvalidChannelData,
    LocalProtocolMismatch,
    CursorExpired,
    CursorNotFound,
    DirectReadRequiresWriter,
    StoreNotFound,
    InvalidContinuation,
    CursorAlreadyOpen,
    CursorTransactionActive,
    CursorInvalidStatement,
    // Wire discriminants are positional: new kinds append here so every
    // pre-existing kind keeps its checked-in bytes.
    OperationAlreadyCompleted,
    /// The inspection asked for is not one the live owner's channel answers,
    /// and the refusal names it rather than saying "not implemented" and
    /// leaving the caller to guess which question failed.
    OwnerRouteUnsupported,
    /// The session declared it reads as one principal and the writer serving
    /// this store was opened as another. Neither identity is inside the
    /// other, so there is no narrower view to serve: answering under the
    /// writer's identity would hand the reader every row that identity's
    /// grants open up, which is strictly more than it declared. The session is
    /// refused instead, and the reader is told which declaration was refused.
    DeclaredPrincipalRefused,
}

impl ReadFailureKind {
    pub const ALL: [Self; 24] = [
        Self::WriteRequiresFlag,
        Self::HeldByWriter,
        Self::HeldByReaders,
        Self::OwnerNotRunning,
        Self::OwnerNotServing,
        Self::OwnerUserMismatch,
        Self::OwnerMismatch,
        Self::OwnerAtCapacity,
        Self::OwnerLimitExceeded,
        Self::OwnerTimeout,
        Self::OwnerDisconnected,
        Self::InvalidChannelData,
        Self::LocalProtocolMismatch,
        Self::CursorExpired,
        Self::CursorNotFound,
        Self::DirectReadRequiresWriter,
        Self::StoreNotFound,
        Self::InvalidContinuation,
        Self::CursorAlreadyOpen,
        Self::CursorTransactionActive,
        Self::CursorInvalidStatement,
        Self::OperationAlreadyCompleted,
        Self::OwnerRouteUnsupported,
        Self::DeclaredPrincipalRefused,
    ];

    pub const fn class(self) -> ReadFailureClass {
        match self {
            Self::WriteRequiresFlag | Self::CursorAlreadyOpen | Self::CursorTransactionActive => {
                ReadFailureClass::Sql
            }
            Self::InvalidContinuation | Self::CursorInvalidStatement => ReadFailureClass::Usage,
            Self::HeldByWriter
            | Self::HeldByReaders
            | Self::OwnerNotRunning
            | Self::OwnerNotServing
            | Self::OwnerUserMismatch
            | Self::OwnerMismatch
            | Self::OwnerAtCapacity
            | Self::OwnerLimitExceeded
            | Self::OwnerTimeout
            | Self::OwnerDisconnected
            | Self::OperationAlreadyCompleted
            | Self::OwnerRouteUnsupported
            | Self::InvalidChannelData
            | Self::LocalProtocolMismatch
            | Self::CursorExpired
            | Self::CursorNotFound
            | Self::DirectReadRequiresWriter
            | Self::DeclaredPrincipalRefused
            | Self::StoreNotFound => ReadFailureClass::Io,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReadFailureClass {
    Sql,
    Io,
    Usage,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CursorExpiryKind {
    Idle,
    Lifetime,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequiredBytesSetting {
    pub required_bytes: u64,
    pub required_setting: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatementRemedy {
    pub statement: String,
    pub remedy_command: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OwnerLimitExceededDetail {
    pub limit: ReadFailureLimit,
    pub value: u64,
    pub required: Option<RequiredBytesSetting>,
    pub statement: Option<StatementRemedy>,
}

/// Concrete reader evidence attached to a writer refusal while direct readers
/// still hold the store's shared hydration lock.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeldByReadersDetail {
    pub observed_direct_readers: u64,
    pub verified_readers: Vec<ReaderBreadcrumb>,
}

impl HeldByReadersDetail {
    pub fn validate(&self) -> Result<(), ReadContractViolation> {
        if self.observed_direct_readers == 0 {
            return Err(ReadContractViolation::HeldByReadersObservedCountZero);
        }
        if u64::try_from(self.verified_readers.len()).unwrap_or(u64::MAX)
            > self.observed_direct_readers
        {
            return Err(ReadContractViolation::VerifiedReadersExceedObserved);
        }
        for (reader, breadcrumb) in self.verified_readers.iter().enumerate() {
            if self.verified_readers[..reader].iter().any(|earlier| {
                earlier.process_id == breadcrumb.process_id
                    && earlier.process_start == breadcrumb.process_start
            }) {
                return Err(ReadContractViolation::DuplicateVerifiedReader { reader });
            }
        }
        Ok(())
    }
}

/// Who holds a store that refused an opener because a writer already has it,
/// in the two parts a caller renders separately: the process to name and the
/// store it is about. The process number is absent when the holder has not
/// published a record about itself, which is a real state and never guessed at.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeldByWriterDetail {
    pub process_id: Option<u64>,
    pub store_path: String,
}

/// The inspection a refusal could not answer over the route it was asked on,
/// as the stable word a program branches on (`image_state`). Naming it is the
/// whole point of the refusal: a caller learns which question was refused, not
/// merely that something was.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OwnerRouteUnsupportedDetail {
    pub inspection: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReadFailureDetail {
    None,
    HeldByReaders(HeldByReadersDetail),
    OwnerLimitExceeded(OwnerLimitExceededDetail),
    CursorExpired { expiry: CursorExpiryKind },
    Reason { reason: String },
    // Wire discriminants are positional: new details append here so every
    // detail that was already encoded keeps its checked-in bytes.
    HeldByWriter(HeldByWriterDetail),
    OwnerRouteUnsupported(OwnerRouteUnsupportedDetail),
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ReadFailureConstructionError {
    #[error("read failure kind and specialized detail are contradictory")]
    KindDetailMismatch,
    #[error("held-by-readers detail is invalid: {0}")]
    InvalidHeldByReadersDetail(ReadContractViolation),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReadFailure {
    kind: ReadFailureKind,
    detail: ReadFailureDetail,
}

#[derive(Deserialize)]
struct ReadFailureWire {
    kind: ReadFailureKind,
    detail: ReadFailureDetail,
}

impl<'de> Deserialize<'de> for ReadFailure {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let wire = ReadFailureWire::deserialize(deserializer)?;
        Self::new(wire.kind, wire.detail).map_err(serde::de::Error::custom)
    }
}

impl ReadFailure {
    pub fn new(
        kind: ReadFailureKind,
        detail: ReadFailureDetail,
    ) -> Result<Self, ReadFailureConstructionError> {
        let owner_limit_kind = kind == ReadFailureKind::OwnerLimitExceeded;
        let owner_limit_detail = matches!(&detail, ReadFailureDetail::OwnerLimitExceeded(_));
        let cursor_expired_kind = kind == ReadFailureKind::CursorExpired;
        let cursor_expired_detail = matches!(&detail, ReadFailureDetail::CursorExpired { .. });
        let held_by_readers_kind = kind == ReadFailureKind::HeldByReaders;
        let held_by_readers_detail = matches!(&detail, ReadFailureDetail::HeldByReaders(_));
        // A route that refuses an inspection always knows which one it was
        // asked for, so this refusal owes its named detail in both directions:
        // arriving without one is the bare "not implemented" it exists to
        // replace, not a thinner answer.
        let owner_route_unsupported_kind = kind == ReadFailureKind::OwnerRouteUnsupported;
        let owner_route_unsupported_detail =
            matches!(&detail, ReadFailureDetail::OwnerRouteUnsupported(_));

        if owner_limit_kind != owner_limit_detail
            || cursor_expired_kind != cursor_expired_detail
            || held_by_readers_kind != held_by_readers_detail
            || owner_route_unsupported_kind != owner_route_unsupported_detail
        {
            return Err(ReadFailureConstructionError::KindDetailMismatch);
        }

        // The writer-contention detail belongs to writer contention and to
        // nothing else, but that refusal may still carry the ordinary detail
        // vocabulary, so the pairing is checked in one direction only and no
        // refusal already on the wire changes shape.
        if matches!(&detail, ReadFailureDetail::HeldByWriter(_))
            && kind != ReadFailureKind::HeldByWriter
        {
            return Err(ReadFailureConstructionError::KindDetailMismatch);
        }

        if let ReadFailureDetail::HeldByReaders(detail) = &detail {
            detail
                .validate()
                .map_err(ReadFailureConstructionError::InvalidHeldByReadersDetail)?;
        }

        Ok(Self { kind, detail })
    }

    pub const fn owner_limit_exceeded(detail: OwnerLimitExceededDetail) -> Self {
        Self {
            kind: ReadFailureKind::OwnerLimitExceeded,
            detail: ReadFailureDetail::OwnerLimitExceeded(detail),
        }
    }

    /// A refusal whose answer is that the continuation this request carries no
    /// longer describes where the read left off, with the words that say what
    /// moved underneath it.
    pub const fn invalid_continuation(reason: String) -> Self {
        Self {
            kind: ReadFailureKind::InvalidContinuation,
            detail: ReadFailureDetail::Reason { reason },
        }
    }

    /// A refusal whose answer is that this session already has a cursor open,
    /// with the words that say which read it stands in the way of.
    pub const fn cursor_already_open(reason: String) -> Self {
        Self {
            kind: ReadFailureKind::CursorAlreadyOpen,
            detail: ReadFailureDetail::Reason { reason },
        }
    }

    /// A refusal whose answer is that this session declared an identity the
    /// writer serving this store will not read as, with the words that name
    /// the two identities so the reader can re-declare or drop the
    /// declaration rather than guess.
    pub const fn declared_principal_refused(reason: String) -> Self {
        Self {
            kind: ReadFailureKind::DeclaredPrincipalRefused,
            detail: ReadFailureDetail::Reason { reason },
        }
    }

    /// A refusal whose answer is that the route this was asked on does not
    /// answer this inspection, carrying the stable word for the inspection so
    /// the caller learns which question was refused.
    pub const fn owner_route_unsupported(inspection: String) -> Self {
        Self {
            kind: ReadFailureKind::OwnerRouteUnsupported,
            detail: ReadFailureDetail::OwnerRouteUnsupported(OwnerRouteUnsupportedDetail {
                inspection,
            }),
        }
    }

    pub const fn cursor_expired(expiry: CursorExpiryKind) -> Self {
        Self {
            kind: ReadFailureKind::CursorExpired,
            detail: ReadFailureDetail::CursorExpired { expiry },
        }
    }

    pub fn held_by_readers(
        detail: HeldByReadersDetail,
    ) -> Result<Self, ReadFailureConstructionError> {
        Self::new(
            ReadFailureKind::HeldByReaders,
            ReadFailureDetail::HeldByReaders(detail),
        )
    }

    pub const fn kind(&self) -> ReadFailureKind {
        self.kind
    }

    pub const fn class(&self) -> ReadFailureClass {
        self.kind.class()
    }

    pub const fn detail(&self) -> &ReadFailureDetail {
        &self.detail
    }
}

impl ReadFailureKind {
    /// What happened, in the words a person reading a log line needs.  A
    /// refusal that carries a specialized detail says more than this; a
    /// refusal that carries none says exactly this.
    const fn stated(self) -> &'static str {
        match self {
            Self::WriteRequiresFlag => "this statement writes, so it needs a write session",
            Self::HeldByWriter => "another session holds this store for writing",
            Self::HeldByReaders => "direct readers are hydrating this store",
            Self::OwnerNotRunning => "the owner process for this store is not running",
            Self::OwnerNotServing => "the owner process for this store is not serving reads",
            Self::OwnerUserMismatch => "the owner process for this store belongs to another user",
            Self::OwnerMismatch => "the owner process answering here serves another store",
            Self::OwnerAtCapacity => {
                "the owner process is already serving as many reads as it admits"
            }
            Self::OwnerLimitExceeded => "the read went past what it is allowed",
            Self::OwnerTimeout => "the owner did not answer inside the time this read allows",
            Self::OwnerDisconnected => "the connection to the owner ended before the read finished",
            Self::OperationAlreadyCompleted => {
                "this operation already answered and its outcome was already delivered"
            }
            Self::InvalidChannelData => "the owner sent bytes this connection could not read",
            Self::LocalProtocolMismatch => {
                "this client and the owner speak different versions of the local read protocol"
            }
            Self::CursorExpired => "the cursor was closed before this request reached it",
            Self::CursorNotFound => "no open cursor answers to this identifier",
            // Never "somebody holds it" -- a store somebody holds refuses as
            // `HeldByWriter`. Every producer of THIS kind found something in
            // the file that only a writable open can settle: state a writable
            // hydration would sanitize or supplement, or an unclean close
            // whose recovery a read-only open will not perform. Both are the
            // same sentence to the caller, and neither depends on anyone
            // still being there.
            Self::DirectReadRequiresWriter => {
                "this store's file needs a writable open before it can be read"
            }
            Self::StoreNotFound => "there is no store at this path",
            Self::InvalidContinuation => {
                "this continuation no longer describes where the read left off"
            }
            Self::CursorAlreadyOpen => "this session already has a cursor open",
            Self::CursorTransactionActive => {
                "this session has a transaction in progress, so a cursor cannot open"
            }
            Self::CursorInvalidStatement => "this statement cannot be read through a cursor",
            Self::OwnerRouteUnsupported => {
                "this store's live owner does not answer this inspection over its channel"
            }
            Self::DeclaredPrincipalRefused => {
                "the writer serving this store reads as a different principal than this session \
                 declared"
            }
        }
    }
}

impl ReadFailureLimit {
    /// The sentence that names this ceiling and the number it stopped at.
    const fn crossing(self) -> &'static str {
        match self {
            Self::ResultRows => "the answer went past the rows this read is allowed",
            Self::ResultBytes => "the answer went past the bytes this read is allowed",
            Self::Work => "the read went past the work it is allowed",
            Self::ActiveMs => "the read went past the time it is allowed",
            Self::Memory => "the read went past the memory it is allowed",
            Self::CursorPageBytes => "a page went past the bytes a cursor page is allowed",
        }
    }

    /// The unit the number after the sentence is counted in.
    const fn unit(self) -> &'static str {
        match self {
            Self::ResultRows => "rows",
            Self::ResultBytes | Self::Memory | Self::CursorPageBytes => "bytes",
            Self::Work => "units of work",
            Self::ActiveMs => "milliseconds",
        }
    }
}

impl CursorExpiryKind {
    const fn stated(self) -> &'static str {
        match self {
            Self::Idle => "the cursor sat unread longer than it is allowed to and was closed",
            Self::Lifetime => "the cursor reached the end of the life it was opened for",
        }
    }
}

impl std::fmt::Display for ReadFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.detail {
            ReadFailureDetail::HeldByReaders(detail) => {
                if detail.verified_readers.is_empty() {
                    return write!(
                        formatter,
                        "{} direct readers are hydrating this store; retry in a moment",
                        detail.observed_direct_readers
                    );
                }

                write!(
                    formatter,
                    "{} direct readers are hydrating this store: ",
                    detail.observed_direct_readers
                )?;
                for (index, reader) in detail.verified_readers.iter().enumerate() {
                    if index != 0 {
                        formatter.write_str(", ")?;
                    }
                    write!(
                        formatter,
                        "{} (process {})",
                        reader.process_name, reader.process_id
                    )?;
                }
                formatter.write_str("; retry in a moment")
            }
            ReadFailureDetail::OwnerLimitExceeded(detail) => {
                write!(
                    formatter,
                    "{}: {} {}",
                    detail.limit.crossing(),
                    detail.value,
                    detail.limit.unit()
                )?;
                if let Some(required) = &detail.required {
                    write!(
                        formatter,
                        "; answering it needs {}",
                        required.required_setting
                    )?;
                }
                if let Some(statement) = &detail.statement {
                    write!(formatter, "; {}", statement.remedy_command)?;
                }
                Ok(())
            }
            ReadFailureDetail::HeldByWriter(detail) => match detail.process_id {
                Some(process_id) => write!(
                    formatter,
                    "{}: process {process_id} holds {}; wait for that process to exit or stop \
                     it, then retry",
                    self.kind.stated(),
                    detail.store_path
                ),
                None => write!(
                    formatter,
                    "{}: the process holding {} has not published its companion record; wait \
                     for it to exit or stop it, then retry",
                    self.kind.stated(),
                    detail.store_path
                ),
            },
            ReadFailureDetail::OwnerRouteUnsupported(detail) => write!(
                formatter,
                "{}: {}; a direct file session answers it once no writer holds the store",
                self.kind.stated(),
                detail.inspection.replace('_', " ")
            ),
            ReadFailureDetail::CursorExpired { expiry } => formatter.write_str(expiry.stated()),
            ReadFailureDetail::None => formatter.write_str(self.kind.stated()),
            ReadFailureDetail::Reason { reason } => {
                write!(formatter, "{}: {reason}", self.kind.stated())
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CursorPage {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
    pub has_more: bool,
}

impl CursorPage {
    pub fn validate(&self) -> Result<(), ReadContractViolation> {
        for (row, values) in self.rows.iter().enumerate() {
            if values.len() != self.columns.len() {
                return Err(ReadContractViolation::CursorRowArityMismatch {
                    row,
                    expected: self.columns.len(),
                    actual: values.len(),
                });
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetadataPageVocabulary {
    Tables,
    EventsStatus,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MetadataItem {
    Table(String),
    EventStatus(BTreeMap<String, Value>),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MetadataPage {
    pub vocabulary: MetadataPageVocabulary,
    pub items: Vec<MetadataItem>,
    pub has_more: bool,
    pub continuation: Option<String>,
}

impl MetadataPage {
    pub fn validate(&self) -> Result<(), ReadContractViolation> {
        for (item, value) in self.items.iter().enumerate() {
            let matches_vocabulary = matches!(
                (&self.vocabulary, value),
                (MetadataPageVocabulary::Tables, MetadataItem::Table(_))
                    | (
                        MetadataPageVocabulary::EventsStatus,
                        MetadataItem::EventStatus(_)
                    )
            );
            if !matches_vocabulary {
                return Err(ReadContractViolation::MetadataItemVocabularyMismatch { item });
            }
        }

        if self.has_more && self.items.is_empty() {
            return Err(ReadContractViolation::EmptyMetadataPageWithMore);
        }

        match (self.has_more, self.continuation.is_some()) {
            (true, false) => Err(ReadContractViolation::MissingMetadataContinuation),
            (false, true) => Err(ReadContractViolation::UnexpectedMetadataContinuation),
            _ => Ok(()),
        }
    }
}
