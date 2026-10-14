//! Vocabulary shared by the bounded direct-reader and local-owner routes.
//!
//! This module deliberately contains values and type contracts only. Route selection,
//! persistence, channel framing, deadline waiting, and query execution are performed by the
//! route and transport layers that consume this vocabulary.

use crate::Value;
use crate::read_memory::{
    NativeCancellationCallback, OwnedReadBox, ReadCredit, ReadMemoryOwner, ReadShared,
};
use serde::{Deserialize, Serialize};
use std::cell::UnsafeCell;
use std::collections::BTreeMap;
use std::future::Future;
use std::marker::PhantomPinned;
use std::pin::Pin;
use std::ptr::NonNull;
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
    /// Number of caller-held snapshot registrations prepaid by an open store.
    /// Further registrations use declared reader memory.
    pub const SHIPPED_SNAPSHOT_PIN_SLOTS: usize = 64;

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

    #[doc(hidden)]
    fn try_native_source(
        &self,
        _standing: &ReadMemoryOwner,
    ) -> crate::Result<Option<OwnedReadBox<dyn NativeDeadlineSource>>> {
        Ok(None)
    }
}

/// Synchronous parking on the driver caller's already-owned thread. A call
/// performs at most one kernel wait, returning on wake, interruption or timeout.
/// The lease retains the original paid wake control, never a host Waker.
#[doc(hidden)]
pub trait NativeDriverPark: crate::read_memory::NativeReadWake {
    fn park(&self) -> crate::Result<()>;
    fn park_for_ms(&self, ms: u64) -> crate::Result<()>;
    fn wake_lease(&self) -> crate::read_memory::NativeReadWakeLease;
}

/// A clock-authored native producer with no worker or independent time origin.
/// All time decisions use the binding's supplied clock; implementations only
/// park the caller for the remaining interval or register in that same clock.
#[doc(hidden)]
pub trait NativeDeadlineSource: Send + Sync {
    fn park_until(
        &self,
        now_ms: u64,
        deadline_ms: u64,
        park: &dyn NativeDriverPark,
    ) -> crate::Result<()>;
}

/// The supplied clock and its own admitted producer remain one time domain.
/// Source destruction precedes release of the clock alias; no worker exists.
/// This inline value must itself live in its consumer's admitted frame/root.
#[doc(hidden)]
pub struct NativeClockBinding {
    source: OwnedReadBox<dyn NativeDeadlineSource>,
    clock: Arc<dyn DeadlineClock>,
}
impl NativeClockBinding {
    #[doc(hidden)]
    pub fn try_select(
        clock: &Arc<dyn DeadlineClock>,
        standing: &ReadMemoryOwner,
    ) -> crate::Result<Self> {
        let source = clock
            .try_native_source(standing)?
            .ok_or(crate::Error::ReadSessionNotImplemented)?;
        if !source.is_funded_by(standing) {
            return Err(crate::Error::ReadSessionNotImplemented);
        }
        Ok(Self {
            source,
            clock: Arc::clone(clock),
        })
    }

    #[doc(hidden)]
    pub fn now_ms(&self) -> u64 {
        self.clock.now_ms()
    }

    #[doc(hidden)]
    pub fn park_until(&self, deadline_ms: u64, park: &dyn NativeDriverPark) -> crate::Result<()> {
        let now_ms = self.clock.now_ms();
        if now_ms >= deadline_ms {
            return Ok(());
        }
        self.source.park_until(now_ms, deadline_ms, park)
    }
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

/// The native cancellation capability is internal to the admitted read route.
/// It cannot be made from, converted into, or substituted for the public
/// Legacy cancellation token.
#[doc(hidden)]
pub struct NativeReadCancellation {
    token: ReadShared<NativeCancellationToken>,
}

impl Clone for NativeReadCancellation {
    fn clone(&self) -> Self {
        Self {
            token: self.token.clone(),
        }
    }
}

impl std::fmt::Debug for NativeReadCancellation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeReadCancellation")
            .field("identity", &self.identity())
            .finish_non_exhaustive()
    }
}

/// A native handler receives the authentic original cancellation capability
/// and owner. There is intentionally no Legacy adapter or default method.
#[doc(hidden)]
pub trait NativeOwnerRequestHandler: Send + Sync {
    fn handle_native(
        &self,
        namespace: &str,
        request: &[u8],
        cancel: &NativeReadCancellation,
        owner: ReadMemoryOwner,
    ) -> crate::Result<OwnedReadBox<[u8]>>;
}

static NEXT_NATIVE_CANCELLATION_IDENTITY: AtomicU64 = AtomicU64::new(1);

impl NativeReadCancellation {
    /// Inspect the original cancellation control's funder without conversion.
    #[doc(hidden)]
    pub fn is_funded_by(&self, owner: &ReadMemoryOwner) -> bool {
        self.token.owner.same_owner(owner)
    }

    #[doc(hidden)]
    pub fn try_new(owner: ReadMemoryOwner) -> crate::Result<Self> {
        let identity = NEXT_NATIVE_CANCELLATION_IDENTITY
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                current.checked_add(1)
            })
            .map_err(|_| crate::Error::NativeCancellationIdentityExhausted)?;
        let invoice = ReadCredit::try_new_owned(
            owner.clone(),
            ReadShared::<NativeCancellationToken>::layout().size(),
            "reader",
            "native cancellation token",
            "admit the original cancellation control before publishing it",
        )?;
        let token = ReadShared::try_from_prepaid(
            NativeCancellationToken {
                cancelled: AtomicBool::new(false),
                identity,
                owner,
                directory: Mutex::new(NativeListenerDirectory::default()),
            },
            invoice,
        )?;
        Ok(Self { token })
    }

    pub fn cancel(&self) {
        self.token.cancelled.store(true, Ordering::SeqCst);
        // Pin the stack record before it is linked. Its borrowed token keeps
        // the token body alive for the record's entire linked lifetime.
        let snapshot = NativeCancellationSnapshot::new(&self.token);
        let mut snapshot = std::pin::pin!(snapshot);
        snapshot.as_mut().link_and_capture();
        while let Some(callback) = snapshot.as_mut().take_next_callback() {
            // No callback or callback Drop runs under the directory mutex.
            callback.call();
        }
    }

    pub fn is_cancelled(&self) -> bool {
        self.token.cancelled.load(Ordering::SeqCst)
    }

    pub fn identity(&self) -> u64 {
        self.token.identity
    }

    pub fn try_tell_on_cancel<F>(&self, told: F) -> crate::Result<NativeCancellationListener>
    where
        F: Fn() + Send + Sync + 'static,
    {
        // Admit both escaping cells before allocating either one or changing
        // membership. Any refusal drops the original callback before credit.
        let callback_credit = ReadCredit::try_new_owned(
            self.token.owner.clone(),
            ReadShared::<F>::layout().size(),
            "reader",
            "native cancellation callback",
            "admit the callback before registration",
        )?;
        let node_credit = ReadCredit::try_new_owned(
            self.token.owner.clone(),
            ReadShared::<NativeListenerNode>::layout().size(),
            "reader",
            "native cancellation listener",
            "admit the listener node before registration",
        )?;
        let callback =
            ReadShared::try_from_prepaid(told, callback_credit)?.into_cancellation_callback();
        let node = ReadShared::try_from_prepaid(NativeListenerNode::new(callback), node_credit)?;
        {
            let mut directory = lock_directory(&self.token);
            directory.append(node.clone());
        }
        let listener = NativeCancellationListener {
            token: self.token.clone(),
            node,
        };
        // Linking precedes the SeqCst check, preserving the Legacy immediate
        // and registration/cancel race semantics (including duplicate calls).
        let callback = {
            let _directory = lock_directory(&self.token);
            listener.node.callback_clone()
        };
        if self.is_cancelled()
            && let Some(callback) = callback
        {
            callback.call();
        }
        Ok(listener)
    }
}

/// Inline registration guard; it retains the token and its node instead of a
/// finite listener identity.
#[doc(hidden)]
pub struct NativeCancellationListener {
    token: ReadShared<NativeCancellationToken>,
    node: ReadShared<NativeListenerNode>,
}

impl Drop for NativeCancellationListener {
    fn drop(&mut self) {
        retire_node(&self.token, &self.node);
    }
}

struct NativeCancellationToken {
    cancelled: AtomicBool,
    identity: u64,
    owner: ReadMemoryOwner,
    directory: Mutex<NativeListenerDirectory>,
}

impl Drop for NativeCancellationToken {
    fn drop(&mut self) {
        // Borrowed snapshots retain this token, so no linked stack record can
        // remain. Detach the strong chain iteratively; callback/node Drops are
        // outside the mutex and ReadShared frees before each original refund.
        let next = {
            let directory = self.directory.get_mut().unwrap_or_else(|p| p.into_inner());
            directory.tail = None;
            directory.newest = None;
            directory.head.take()
        };
        let mut drain = NativeTokenDrain { next };
        while drain.drain_one() {}
    }
}

struct NativeTokenDrain {
    next: Option<ReadShared<NativeListenerNode>>,
}

impl NativeTokenDrain {
    fn drain_one(&mut self) -> bool {
        let Some(node) = self.next.take() else {
            return false;
        };
        let (following, callback) = {
            // SAFETY: final token destruction is the sole remaining node
            // access path: listeners retain the token, and linked snapshots
            // borrow it. No shared ReadShared dereference grants &mut access;
            // this raw access is justified by that sole destruction state.
            let data = unsafe { &mut *node.data.get() };
            (data.next.take(), data.callback.take())
        };
        // Set the remainder first: if callback Drop panics, this stack guard
        // continues the iterative teardown during unwind. A second panic keeps
        // Rust's ordinary abort behavior rather than fabricating recovery.
        self.next = following;
        drop(callback);
        drop(node);
        true
    }
}

impl Drop for NativeTokenDrain {
    fn drop(&mut self) {
        while self.drain_one() {}
    }
}

#[derive(Default)]
struct NativeListenerDirectory {
    head: Option<ReadShared<NativeListenerNode>>,
    tail: Option<NonNull<NativeListenerNode>>,
    newest: Option<NonNull<NativeSnapshotRecord>>,
}

// SAFETY: the directory's non-owning node and stack-record pointers are read
// or changed only while NativeCancellationToken::directory holds its mutex.
// Node ownership stays in the forward ReadShared chain, and snapshots unlink
// their pinned records before their stack storage expires. The directory itself
// is never shared without that mutex.
unsafe impl Send for NativeListenerDirectory {}

impl NativeListenerDirectory {
    fn append(&mut self, node: ReadShared<NativeListenerNode>) {
        let node_ptr = NonNull::from(&*node);
        // SAFETY: directory mutex is held by every caller; tail is a node in
        // the strong chain and no raw node capability escapes that mutex.
        if let Some(tail) = self.tail {
            unsafe { (*tail.as_ref().data.get()).next = Some(node) };
        } else {
            self.head = Some(node);
        }
        self.tail = Some(node_ptr);
    }
}

struct NativeListenerNode {
    data: UnsafeCell<NativeListenerNodeData>,
}

struct NativeListenerNodeData {
    next: Option<ReadShared<NativeListenerNode>>,
    registered: bool,
    retired_through: Option<NonNull<NativeSnapshotRecord>>,
    callback: Option<NativeCancellationCallback>,
}

// SAFETY: directory locking is the sole access path to the UnsafeCell and no
// raw node capability is exposed outside this module.
unsafe impl Send for NativeListenerNode {}
unsafe impl Sync for NativeListenerNode {}

impl NativeListenerNode {
    fn new(callback: NativeCancellationCallback) -> Self {
        Self {
            data: UnsafeCell::new(NativeListenerNodeData {
                next: None,
                registered: true,
                retired_through: None,
                callback: Some(callback),
            }),
        }
    }

    fn callback_clone(&self) -> Option<NativeCancellationCallback> {
        // SAFETY: callers hold the directory mutex; callback cloning only
        // increments the same ReadShared ownership and does not invoke user code.
        unsafe { (&*self.data.get()).callback.clone() }
    }
}

struct NativeCancellationSnapshot<'a> {
    token: &'a NativeCancellationToken,
    record: UnsafeCell<NativeSnapshotRecord>,
    _pin: PhantomPinned,
}

struct NativeSnapshotRecord {
    newer: Option<NonNull<NativeSnapshotRecord>>,
    older: Option<NonNull<NativeSnapshotRecord>>,
    next: Option<NonNull<NativeListenerNode>>,
    last: Option<NonNull<NativeListenerNode>>,
    linked: bool,
}

impl<'a> NativeCancellationSnapshot<'a> {
    fn new(token: &'a NativeCancellationToken) -> Self {
        Self {
            token,
            record: UnsafeCell::new(NativeSnapshotRecord {
                newer: None,
                older: None,
                next: None,
                last: None,
                linked: false,
            }),
            _pin: PhantomPinned,
        }
    }

    fn link_and_capture(self: Pin<&mut Self>) {
        let mut directory = lock_directory(self.token);
        // SAFETY: this record was pinned by cancel before linking; every raw
        // snapshot pointer is read or written only while this mutex is held.
        unsafe {
            let record = &mut *self.record.get();
            let record_ptr = NonNull::from(&mut *record);
            record.older = directory.newest;
            if let Some(newest) = directory.newest {
                (*newest.as_ptr()).newer = Some(record_ptr);
            }
            record.next = directory.head.as_deref().map(NonNull::from);
            record.last = directory.tail;
            record.linked = true;
            directory.newest = Some(record_ptr);
        }
    }

    fn take_next_callback(self: Pin<&mut Self>) -> Option<NativeCancellationCallback> {
        let _directory = lock_directory(self.token);
        // SAFETY: the pinned record and all node pointer traversal occur under
        // the directory mutex. Advancing next before unlock makes the passed
        // node unavailable to later snapshot traversal/reclamation checks.
        unsafe {
            let record = &mut *self.record.get();
            loop {
                let node = record.next?;
                let data = &mut *node.as_ref().data.get();
                let is_last = record.last == Some(node);
                record.next = if is_last {
                    record.last = None;
                    None
                } else {
                    data.next.as_deref().map(NonNull::from)
                };
                if node_eligible(record, data)
                    && let Some(callback) = data.callback.clone()
                {
                    return Some(callback);
                }
            }
        }
    }
}

impl Drop for NativeCancellationSnapshot<'_> {
    fn drop(&mut self) {
        {
            let mut directory = lock_directory(self.token);
            // SAFETY: cutoff rewrite happens before unlink while each pointer
            // is protected by the directory mutex; no stack pointer survives.
            unsafe { unlink_snapshot(&mut directory, self.record.get()) };
        }
        reclaim_retired(self.token);
    }
}

fn lock_directory(
    token: &NativeCancellationToken,
) -> std::sync::MutexGuard<'_, NativeListenerDirectory> {
    token
        .directory
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn node_eligible(record: &NativeSnapshotRecord, data: &NativeListenerNodeData) -> bool {
    if data.registered {
        return true;
    }
    let mut cutoff = data.retired_through;
    while let Some(current) = cutoff {
        if std::ptr::eq(current.as_ptr(), record) {
            return true;
        }
        // SAFETY: record pointers are live only in the mutex-protected active
        // snapshot list, and this caller holds that mutex.
        cutoff = unsafe { current.as_ref().older };
    }
    false
}

fn retire_node(token: &NativeCancellationToken, node: &ReadShared<NativeListenerNode>) {
    {
        let directory = lock_directory(token);
        // SAFETY: raw node access is under the directory mutex. The cutoff is
        // the active newest record, never an address-derived ordering.
        unsafe {
            let data = &mut *node.data.get();
            if !data.registered {
                return;
            }
            data.registered = false;
            data.retired_through = directory.newest;
        }
    }
    reclaim_retired(token);
}

/// Returns whether an active snapshot can still reach this node. This must be
/// checked before either detaching the tombstone or retiring its callback.
unsafe fn snapshot_contains(
    record: NonNull<NativeSnapshotRecord>,
    needle: NonNull<NativeListenerNode>,
) -> bool {
    // SAFETY: callers hold the directory mutex; next/last delimit the live
    // interval and no physical node free occurs while any interval contains it.
    unsafe {
        let record = record.as_ref();
        let mut current = record.next;
        while let Some(node) = current {
            if node == needle {
                return true;
            }
            if Some(node) == record.last {
                break;
            }
            current = (*node.as_ref().data.get())
                .next
                .as_deref()
                .map(NonNull::from);
        }
        false
    }
}

unsafe fn any_snapshot_contains(
    directory: &NativeListenerDirectory,
    node: NonNull<NativeListenerNode>,
) -> bool {
    // SAFETY: all record links are protected by the directory mutex.
    unsafe {
        let mut record = directory.newest;
        while let Some(current) = record {
            if snapshot_contains(current, node) {
                return true;
            }
            record = current.as_ref().older;
        }
        false
    }
}

/// Returns whether an active snapshot can still invoke this retired callback.
/// An ineligible snapshot retains its cursor/node lifetime but cannot retain
/// the callback itself.
unsafe fn any_eligible_snapshot_contains(
    directory: &NativeListenerDirectory,
    node: NonNull<NativeListenerNode>,
    data: &NativeListenerNodeData,
) -> bool {
    // SAFETY: all record links and callback membership inputs are protected by
    // the directory mutex held by the caller.
    unsafe {
        let mut record = directory.newest;
        while let Some(current) = record {
            let snapshot = current.as_ref();
            if snapshot_contains(current, node) && node_eligible(snapshot, data) {
                return true;
            }
            record = snapshot.older;
        }
        false
    }
}

enum NativeReclaimed {
    Callback(NativeCancellationCallback),
    Node(ReadShared<NativeListenerNode>),
}

fn reclaim_retired(token: &NativeCancellationToken) {
    let mut guard = NativeReclaimGuard {
        token,
        complete: false,
    };
    loop {
        let reclaimed = {
            let mut directory = lock_directory(token);
            // SAFETY: every raw node/snapshot pointer below is reached and
            // modified only while the directory mutex is held. We take one
            // object then release the lock before any callback/node Drop.
            unsafe { take_one_reclaimable(&mut directory) }
        };
        match reclaimed {
            Some(NativeReclaimed::Callback(callback)) => drop(callback),
            Some(NativeReclaimed::Node(node)) => drop(node),
            None => {
                guard.complete = true;
                return;
            }
        }
    }
}

struct NativeReclaimGuard<'a> {
    token: &'a NativeCancellationToken,
    complete: bool,
}

impl Drop for NativeReclaimGuard<'_> {
    fn drop(&mut self) {
        if !self.complete {
            // A callback/node destructor panicked after its object had been
            // detached. Continue reclaiming the remaining tombstones during
            // unwind; a second panic preserves normal process abort behavior.
            reclaim_retired(self.token);
        }
    }
}

unsafe fn take_one_reclaimable(directory: &mut NativeListenerDirectory) -> Option<NativeReclaimed> {
    let mut link: *mut Option<ReadShared<NativeListenerNode>> = &mut directory.head;
    let mut previous = None;
    // SAFETY: caller holds the directory mutex. This pointer is either the
    // directory head or a node's next field, and never escapes the lock.
    unsafe {
        while let Some(node) = (*link).as_ref().cloned() {
            let node_ptr = NonNull::from(&*node);
            let data = &mut *node.data.get();
            if !data.registered {
                // A callback's lifetime is independent of the traversal
                // tombstone: an ineligible snapshot preserves its cursor but
                // cannot invoke this callback.
                if !any_eligible_snapshot_contains(directory, node_ptr, data)
                    && let Some(callback) = data.callback.take()
                {
                    return Some(NativeReclaimed::Callback(callback));
                }
                // Keep the tombstone until every active traversal has passed
                // it, including snapshots for which it is not eligible.
                if !any_snapshot_contains(directory, node_ptr) {
                    let detached = (*link).take().expect("linked node exists");
                    // SAFETY: the directory mutex excludes every other node
                    // mutation; detachment leaves this cell inaccessible from
                    // the chain before its ReadShared owner is dropped.
                    let next = (*detached.data.get()).next.take();
                    *link = next;
                    if directory.tail == Some(node_ptr) {
                        directory.tail = previous;
                    }
                    return Some(NativeReclaimed::Node(detached));
                }
            }
            previous = Some(node_ptr);
            link = &mut data.next;
        }
    }
    None
}

unsafe fn unlink_snapshot(
    directory: &mut NativeListenerDirectory,
    record_ptr: *mut NativeSnapshotRecord,
) {
    // SAFETY: caller holds the directory mutex and record_ptr belongs to the
    // still-pinned stack snapshot. Rewrite every cutoff before unlinking it.
    unsafe {
        let record = &mut *record_ptr;
        if !record.linked {
            return;
        }
        let record_non_null = NonNull::new_unchecked(record_ptr);
        let mut node = directory.head.as_deref().map(NonNull::from);
        while let Some(current) = node {
            let data = &mut *current.as_ref().data.get();
            if data.retired_through == Some(record_non_null) {
                data.retired_through = record.older;
            }
            node = data.next.as_deref().map(NonNull::from);
        }
        if let Some(older) = record.older {
            older.as_ptr().as_mut().expect("active snapshot").newer = record.newer;
        }
        if let Some(newer) = record.newer {
            newer.as_ptr().as_mut().expect("active snapshot").older = record.older;
        } else {
            directory.newest = record.older;
        }
        record.newer = None;
        record.older = None;
        record.next = None;
        record.last = None;
        record.linked = false;
    }
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
    /// Administrative custody inspection is unavailable through a constrained
    /// application-row handle because the answer is a whole metadata block.
    ConstrainedHandleInspectionRefused,
    /// A completed read cannot cross the authoritative erasure publication
    /// boundary for the snapshot it used.
    AuthoritativeErasure,
}

impl ReadFailureKind {
    pub const ALL: [Self; 26] = [
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
        Self::ConstrainedHandleInspectionRefused,
        Self::AuthoritativeErasure,
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
            | Self::ConstrainedHandleInspectionRefused
            | Self::AuthoritativeErasure
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
            Self::ConstrainedHandleInspectionRefused => {
                "this constrained handle cannot inspect the whole custody metadata block"
            }
            Self::AuthoritativeErasure => {
                "authoritative erasure invalidated this completed read before publication"
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
    /// Assemble a public terminal answer. Working credit belongs to the private engine page.
    #[doc(hidden)]
    pub fn from_parts(columns: Vec<String>, rows: Vec<Vec<Value>>, has_more: bool) -> Self {
        Self {
            columns,
            rows,
            has_more,
        }
    }
    /// Public terminal copies retain the HEAD payload (rulings 76/78).
    pub fn try_clone(
        &self,
        _owner: std::sync::Arc<dyn crate::read_memory::ReadMemoryBudget>,
    ) -> crate::Result<Self> {
        Ok(self.clone())
    }
    #[doc(hidden)]
    pub fn try_empty(
        columns: &[String],
        _owner: std::sync::Arc<dyn crate::read_memory::ReadMemoryBudget>,
    ) -> crate::Result<Self> {
        Ok(Self::from_parts(columns.to_vec(), Vec::new(), false))
    }

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

#[cfg(test)]
mod native_snapshot_tests {
    use super::*;
    use crate::read_memory::native_control_tests::{SERIAL, Watch};
    use std::sync::atomic::AtomicUsize;

    fn register_callback<F: Fn() + Send + Sync + 'static>(
        w: &Watch,
        token: &NativeReadCancellation,
        callback: F,
    ) -> NativeCancellationListener {
        let listener = w.allocating(|| token.try_tell_on_cancel(callback)).unwrap();
        w.assert_shared(&listener.node, &token.token.owner);
        let _directory = lock_directory(&token.token);
        let callback = listener.node.callback_clone().unwrap();
        w.assert_callback::<F>(&callback, &token.token.owner);
        drop(callback);
        drop(_directory);
        listener
    }
    fn register(
        w: &Watch,
        token: &NativeReadCancellation,
        count: &Arc<AtomicUsize>,
    ) -> NativeCancellationListener {
        let count = count.clone();
        register_callback(w, token, move || {
            count.fetch_add(1, Ordering::SeqCst);
        })
    }
    fn empty(token: &NativeReadCancellation) {
        let d = lock_directory(&token.token);
        assert!(
            d.newest.is_none(),
            "no pinned stack record survives completion or unwind"
        );
        assert!(d.head.is_none());
        assert!(d.tail.is_none());
    }
    fn run(w: &Watch, f: impl FnOnce(&NativeReadCancellation)) {
        let owner = w.native(false);
        let token = w
            .allocating(|| NativeReadCancellation::try_new(owner.clone()))
            .unwrap();
        assert!(token.token.owner.same_owner(&owner));
        w.assert_shared(&token.token, &owner);
        let clone = token.clone();
        assert_eq!(token.identity(), clone.identity());
        drop(clone);
        drop(owner);
        f(&token);
        drop(token);
        w.finish();
    }
    #[test]
    fn native_cancellation_snapshot_rows() {
        let _serial = SERIAL.lock().unwrap_or_else(|p| p.into_inner());
        {
            let w = Watch::new("captured-membership-tombstone-and-new-tail");
            run(&w, |token| {
                let first_count = Arc::new(AtomicUsize::new(0));
                let later_count = Arc::new(AtomicUsize::new(0));
                let new_count = Arc::new(AtomicUsize::new(0));
                let first = register(&w, token, &first_count);
                let later = register(&w, token, &later_count);
                let first_pointer = NonNull::from(&*first.node);
                let later_pointer = NonNull::from(&*later.node);
                token.token.cancelled.store(true, Ordering::SeqCst);
                // This outer stack storage is never moved while linked. Its
                // pinned reference ends before the stack storage is destroyed.
                {
                    let mut newer_storage = NativeCancellationSnapshot::new(&token.token);
                    let mut newer = unsafe { Pin::new_unchecked(&mut newer_storage) };
                    let before_retire = w.frees();
                    {
                        let older = NativeCancellationSnapshot::new(&token.token);
                        let mut older = std::pin::pin!(older);
                        older.as_mut().link_and_capture();
                        std::thread::scope(|s| {
                            s.spawn(move || drop(first)).join().unwrap();
                        });
                        assert_eq!(
                            w.frees(),
                            before_retire,
                            "eligible captured membership retains callback and node"
                        );
                        newer.as_mut().link_and_capture();
                        let new_tail = register(&w, token, &new_count);
                        assert_eq!(
                            new_count.load(Ordering::SeqCst),
                            1,
                            "cancelled token calls new registrations immediately"
                        );
                        {
                            let _d = lock_directory(&token.token);
                            // SAFETY: both pinned intervals and the mutex keep
                            // the retired node and stack pointers alive.
                            unsafe {
                                assert_eq!((*newer.record.get()).last, Some(later_pointer));
                                let data = &*first_pointer.as_ref().data.get();
                                assert!(!data.registered);
                                assert!(data.callback.is_some());
                                assert!(node_eligible(&*older.record.get(), data));
                                assert!(!node_eligible(&*newer.record.get(), data));
                            }
                        }
                        older.as_mut().take_next_callback().unwrap().call();
                        assert_eq!(first_count.load(Ordering::SeqCst), 1);
                        // Retiring an appended tail cannot extend either capture.
                        drop(new_tail);
                    }
                    {
                        let d = lock_directory(&token.token);
                        unsafe {
                            assert_eq!(d.newest, Some(NonNull::new_unchecked(newer.record.get())));
                            let data = &*first_pointer.as_ref().data.get();
                            assert!(!data.registered);
                            assert!(data.callback.is_none());
                            assert!(data.retired_through.is_none());
                            assert_eq!((*newer.record.get()).next, Some(first_pointer));
                        }
                    }
                    // The actual production traversal must skip this retained
                    // tombstone and reach the later eligible listener.
                    newer
                        .as_mut()
                        .take_next_callback()
                        .expect("ineligible tombstone must not stop later listener")
                        .call();
                    assert!(newer.as_mut().take_next_callback().is_none());
                    assert_eq!(first_count.load(Ordering::SeqCst), 1);
                    assert_eq!(later_count.load(Ordering::SeqCst), 1);
                    assert_eq!(new_count.load(Ordering::SeqCst), 1);
                    #[allow(
                        clippy::drop_non_drop,
                        reason = "the drop marks where the pinned test value goes out of scope"
                    )]
                    drop(newer);
                    // The linked value drops in place here, including unwind.
                }
                drop(later);
                empty(token);
            });
        }
        for operation in [
            "native cancellation token",
            "native cancellation callback",
            "native cancellation listener",
        ] {
            let w = Watch::new(operation);
            let owner = w.native(false);
            w.refuse(Some(operation));
            let before = w.cells();
            if operation == "native cancellation token" {
                assert!(
                    w.allocating(|| NativeReadCancellation::try_new(owner.clone()))
                        .is_err()
                );
                assert_eq!(w.cells(), before);
            } else {
                w.refuse(None);
                let token = w
                    .allocating(|| NativeReadCancellation::try_new(owner.clone()))
                    .unwrap();
                w.refuse(Some(operation));
                let before = w.cells();
                assert!(w.allocating(|| token.try_tell_on_cancel(|| {})).is_err());
                assert_eq!(
                    w.cells(),
                    before,
                    "both admissions precede either allocation"
                );
                empty(&token);
                drop(token);
            }
            w.refuse(None);
            drop(owner);
            w.finish();
        }
        {
            let w = Watch::new("token-null-allocation");
            let owner = w.native(false);
            w.null(ReadShared::<NativeCancellationToken>::layout());
            assert!(
                w.allocating(|| NativeReadCancellation::try_new(owner.clone()))
                    .is_err()
            );
            assert_eq!(w.cells(), 1);
            drop(owner);
            w.finish();
        }
        {
            let w = Watch::new("callback-null-allocation");
            run(&w, |token| {
                fn null_callback<F: Fn() + Send + Sync + 'static>(
                    w: &Watch,
                    token: &NativeReadCancellation,
                    f: F,
                ) {
                    w.null(ReadShared::<F>::layout());
                    let before = w.cells();
                    assert!(w.allocating(|| token.try_tell_on_cancel(f)).is_err());
                    assert_eq!(w.cells(), before);
                }
                null_callback(&w, token, || {});
                empty(token);
            });
        }
        {
            let w = Watch::new("listener-node-null-allocation");
            run(&w, |token| {
                w.null(ReadShared::<NativeListenerNode>::layout());
                assert!(w.allocating(|| token.try_tell_on_cancel(|| {})).is_err());
                empty(token);
            });
        }
        {
            let w = Watch::new("concurrent-register-unregister-cancel");
            run(&w, |token| {
                let count = Arc::new(AtomicUsize::new(0));
                let listener = register(&w, token, &count);
                let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(0);
                let (continue_tx, continue_rx) = std::sync::mpsc::sync_channel(0);
                let continue_rx = Mutex::new(continue_rx);
                let blocker = w
                    .allocating(|| {
                        token.try_tell_on_cancel(move || {
                            entered_tx.send(()).unwrap();
                            continue_rx.lock().unwrap().recv().unwrap();
                        })
                    })
                    .unwrap();
                std::thread::scope(|s| {
                    let clone = token.clone();
                    let cancel = s.spawn(move || clone.cancel());
                    entered_rx.recv().unwrap();
                    drop(listener);
                    let immediate = Arc::new(AtomicUsize::new(0));
                    let tail = register(&w, token, &immediate);
                    assert_eq!(immediate.load(Ordering::SeqCst), 1);
                    drop(tail);
                    continue_tx.send(()).unwrap();
                    cancel.join().unwrap();
                });
                assert_eq!(count.load(Ordering::SeqCst), 1);
                drop(blocker);
                empty(token);
            });
        }
        {
            let w = Watch::new("nested-reentrant-cancel");
            run(&w, |token| {
                let entered = Arc::new(AtomicBool::new(false));
                let count = Arc::new(AtomicUsize::new(0));
                let nested_token = token.clone();
                let entered_c = entered.clone();
                let first = w
                    .allocating(|| {
                        token.try_tell_on_cancel(move || {
                            if !entered_c.swap(true, Ordering::SeqCst) {
                                nested_token.cancel();
                            }
                        })
                    })
                    .unwrap();
                let later = register(&w, token, &count);
                token.cancel();
                assert!(entered.load(Ordering::SeqCst));
                assert_eq!(count.load(Ordering::SeqCst), 2);
                assert!(lock_directory(&token.token).newest.is_none());
                drop(first);
                drop(later);
                empty(token);
            });
        }
        {
            let w = Watch::new("callback-panic-stack-cutoff-restoration");
            run(&w, |token| {
                let listener = w
                    .allocating(|| token.try_tell_on_cancel(|| panic!("callback invocation panic")))
                    .unwrap();
                assert!(
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| token.cancel()))
                        .is_err()
                );
                assert!(lock_directory(&token.token).newest.is_none());
                drop(listener);
                empty(token);
                let count = Arc::new(AtomicUsize::new(0));
                let immediate = register(&w, token, &count);
                assert_eq!(count.load(Ordering::SeqCst), 1);
                token.cancel();
                assert_eq!(count.load(Ordering::SeqCst), 2);
                drop(immediate);
                empty(token);
            });
        }
        {
            struct PanicDrop;
            impl Drop for PanicDrop {
                fn drop(&mut self) {
                    panic!("callback destructor panic");
                }
            }
            let w = Watch::new("callback-drop-panic-reclamation");
            run(&w, |token| {
                let payload = PanicDrop;
                let first = w
                    .allocating(|| {
                        token.try_tell_on_cancel(move || {
                            std::hint::black_box(&payload);
                        })
                    })
                    .unwrap();
                let count = Arc::new(AtomicUsize::new(0));
                let later = register(&w, token, &count);
                // An older actual pinned interval retains both callbacks.
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let snapshot = NativeCancellationSnapshot::new(&token.token);
                    let mut snapshot = std::pin::pin!(snapshot);
                    snapshot.as_mut().link_and_capture();
                    drop(first);
                    drop(later);
                    // Leaving the scope unlinks the stack record then really
                    // drops the retired callback outside the mutex.
                }));
                assert!(result.is_err());
                assert_eq!(count.load(Ordering::SeqCst), 0);
                empty(token);
            });
        }
        for panic in [false, true] {
            let w = Watch::new(if panic {
                "snapshot-cutoff-panic-unlink"
            } else {
                "snapshot-cutoff-error-unlink"
            });
            run(&w, |token| {
                let count = Arc::new(AtomicUsize::new(0));
                let listener = register(&w, token, &count);
                {
                    let mut outer_storage = NativeCancellationSnapshot::new(&token.token);
                    let mut outer = unsafe { Pin::new_unchecked(&mut outer_storage) };
                    outer.as_mut().link_and_capture();
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                        || -> crate::Result<()> {
                            let inner = NativeCancellationSnapshot::new(&token.token);
                            let mut inner = std::pin::pin!(inner);
                            inner.as_mut().link_and_capture();
                            drop(listener);
                            if panic {
                                panic!("snapshot body panic");
                            }
                            Err(crate::Error::Other("snapshot body error".into()))
                        },
                    ));
                    assert_eq!(result.is_err(), panic);
                    if !panic {
                        assert!(result.unwrap().is_err());
                    }
                    {
                        let d = lock_directory(&token.token);
                        unsafe {
                            assert_eq!(d.newest, Some(NonNull::new_unchecked(outer.record.get())));
                            let node = d.head.as_ref().unwrap();
                            assert_eq!((*node.data.get()).retired_through, d.newest);
                            assert!((*outer.record.get()).newer.is_none());
                        }
                    }
                    outer.as_mut().take_next_callback().unwrap().call();
                    assert_eq!(count.load(Ordering::SeqCst), 1);
                    #[allow(
                        clippy::drop_non_drop,
                        reason = "the drop marks where the pinned test value goes out of scope"
                    )]
                    drop(outer);
                }
                empty(token);
            });
        }
    }
}
