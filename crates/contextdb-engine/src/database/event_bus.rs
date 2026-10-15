use super::*;
use std::cell::Cell;

const EVENT_TYPES_CONFIG_KEY: &str = "__event_bus_event_types";
const SINKS_CONFIG_KEY: &str = "__event_bus_sinks";
const ROUTES_CONFIG_KEY: &str = "__event_bus_routes";
pub(crate) const NEXT_ACTION_ID_CONFIG_KEY: &str = "__event_bus_next_action_id";
pub(crate) const UNCERTAIN_COUNT_CONFIG_PREFIX: &str = "__event_bus_uncertain_count.";
pub(crate) const UNCONFIRMED_COUNT_CONFIG_PREFIX: &str = "__event_bus_unconfirmed_count.";
pub(crate) const MAX_SINK_QUEUE_DEPTH: usize = 100_000;
const SINK_DISPATCH_BATCH: usize = 1024;
/// A durable sink queue keeps at most this many entries resident, and at most
/// `SINK_WINDOW_BYTES` of them by encoded width (one wider entry is resident
/// alone). The rest stays in the store and is read in windows as dispatch
/// reaches it, so opening a store and running its dispatcher hold the same
/// memory however long the queue is.
const SINK_WINDOW_ENTRIES: usize = SINK_DISPATCH_BATCH;
const SINK_WINDOW_BYTES: usize = 1024 * 1024;
// A durable marker written before calling an external sink. A pending marked
// entry has an unconfirmed effect even if the process died before the handler
// returned or its outcome could be recorded.
const UNCONFIRMED_ATTEMPT: u32 = 1 << 31;
const ATTEMPT_COUNT_MASK: u32 = !UNCONFIRMED_ATTEMPT;

type SinkCallback = Arc<dyn Fn(&SinkEvent) -> std::result::Result<(), SinkError> + Send + Sync>;

pub(crate) struct EventBusState {
    pub(super) definitions: Mutex<EventBusDefinitions>,
    callbacks: RwLock<HashMap<String, SinkRegistration>>,
    queues: Mutex<HashMap<String, SinkQueue>>,
    /// The writer accountant every bounded window's owner charges. Installed
    /// when a durable store's queues are loaded; an in-memory database leaves
    /// it unset and keeps every queue resident, as before.
    window_accountant: Mutex<Option<crate::memory_accounting::AccountantHandle>>,
    /// Identifies each prepared refill so an install can tell it is still
    /// the one its queue is waiting for.
    next_refill_token: AtomicU64,
    /// Set when a durable store's queues are loaded: every queue this writer
    /// creates from then on is a bounded window.
    durable_windows: AtomicBool,
    /// Entries a committed read image counted but left in the store. Always
    /// empty on a writable handle, which loads its queues.
    unloaded_queue_depths: Mutex<HashMap<String, u64>>,
    /// Stops new queue checkouts while an authoritative-purge snapshot is
    /// owned through its future durable commit and direct swap. Mutations take
    /// this state before queue locks; an in-flight user callback is the one
    /// deliberate reentrant exception so it can finish its own DB work.
    queue_mutation_state: Arc<Mutex<QueueMutationState>>,
    queue_mutation_waiters: Arc<Condvar>,
    staged_for_persistence: Mutex<HashMap<Lsn, Arc<Vec<PreparedSinkEvent>>>>,
    staged_ddl_for_persistence: Mutex<HashMap<Lsn, StagedEventBusDdlCommit>>,
    metrics: Mutex<HashMap<String, SinkMetricsState>>,
    runtimes: Mutex<HashMap<String, SinkRuntime>>,
    runtime_starts: Mutex<HashMap<String, u64>>,
    deferred_resource_cleanup: Mutex<Option<EventBusResourceCleanup>>,
    wait_lock: Mutex<()>,
    waiters: Condvar,
    next_queue_id: AtomicU64,
    #[cfg(any(test, feature = "test-seams"))]
    queue_capacity_for_test: std::sync::atomic::AtomicUsize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct EventBusDefinitions {
    event_types: BTreeMap<String, EventTypeDef>,
    sinks: BTreeMap<String, SinkDef>,
    routes: BTreeMap<String, RouteDef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct EventTypeDef {
    name: String,
    trigger: EventTrigger,
    table: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum EventTrigger {
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct SinkDef {
    name: String,
    sink_type: PersistedSinkType,
    url: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum PersistedSinkType {
    Webhook,
    Callback,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct RouteDef {
    name: String,
    event_type: String,
    sink: String,
    where_in: Option<PersistedRouteWhereIn>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PersistedRouteWhereIn {
    column: String,
    values: Vec<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct SinkQueueEntry {
    pub(crate) id: u64,
    pub(crate) event: SinkEvent,
    pub(crate) row_id: RowId,
    pub(crate) attempts: u32,
    gate: EventGateSnapshot,
}

/// One sink's queue as this writer holds it. An unbounded queue (in-memory
/// database, or a committed image) keeps every entry resident. A bounded
/// queue keeps a window of its durable table: `entries` holds ids in
/// ascending order, all at or below `resident_through`, decoded under the
/// window's own writer-funded owner, which holds each entry's actual decoded
/// heap (`credits`, aligned with `entries`) until the entry leaves the
/// window. Committed appends to a bounded queue stay in the store until a
/// window read reaches them.
#[derive(Debug, Clone, Default)]
pub(crate) struct SinkQueue {
    entries: VecDeque<SinkQueueEntry>,
    /// Decoded bytes each resident entry holds on `owner`.
    credits: VecDeque<usize>,
    owner: Option<Arc<crate::read_image_memory::ImageMemory>>,
    /// Durable entries; equals `entries.len()` while everything is resident.
    depth: u64,
    bounded: bool,
    /// The highest id the current sweep has read.
    resident_through: u64,
    /// The fixed upper id of the current sweep, captured when it started.
    sweep_upper: u64,
    /// Bumped by every published removal; a prepared refill installs only if
    /// no removal was published since it was planned.
    removal_generation: u64,
    /// The refill this queue is waiting for, if one is prepared.
    pending_refill: Option<u64>,
}

/// One bounded window read: entries in ascending id order with the decoded
/// bytes each holds on `owner`.
pub(crate) struct SinkWindow {
    pub(crate) entries: VecDeque<SinkQueueEntry>,
    pub(crate) credits: VecDeque<usize>,
    /// `None` for an empty window, which holds nothing.
    pub(crate) owner: Option<Arc<crate::read_image_memory::ImageMemory>>,
}

impl SinkQueue {
    fn unbounded(entries: VecDeque<SinkQueueEntry>) -> Self {
        Self {
            entries,
            ..Self::default()
        }
    }

    pub(crate) fn depth(&self) -> u64 {
        if self.bounded {
            self.depth
        } else {
            self.entries.len() as u64
        }
    }

    pub(crate) fn resident(&self) -> &VecDeque<SinkQueueEntry> {
        &self.entries
    }

    /// Decoded bytes the resident window holds.
    pub(crate) fn resident_bytes(&self) -> usize {
        self.credits.iter().sum()
    }

    fn all_resident(&self) -> bool {
        !self.bounded || self.depth == self.entries.len() as u64
    }

    fn contains(&self, id: u64) -> bool {
        self.entries.iter().any(|entry| entry.id == id)
    }

    /// Publish one committed append. A bounded queue counts it and leaves it
    /// in the store, where the next window read reaches it in id order: an
    /// entry is resident only as a window read decoded and funded it.
    fn publish_append(&mut self, entry: SinkQueueEntry) {
        if self.bounded {
            self.depth = self.depth.saturating_add(1);
        } else {
            self.entries.push_back(entry);
        }
    }

    /// Plan the next window read of a bounded queue with nothing visible
    /// resident: continue the current sweep up to its fixed upper id, or,
    /// once the sweep is exhausted, start a new sweep from the lowest id.
    fn plan_refill(&mut self, token: u64) -> SinkRefillPlan {
        self.pending_refill = Some(token);
        let wrap = self.resident_through >= self.sweep_upper;
        SinkRefillPlan {
            token,
            generation: self.removal_generation,
            after: (!wrap).then_some(self.resident_through),
            upper: (!wrap).then_some(self.sweep_upper),
        }
    }

    /// Install a window read for `plan`; the read's owner already holds its
    /// entries' decoded heap, and the predecessor's owner is released when it
    /// is replaced, so both were funded while both existed. Refused (`false`,
    /// nothing changed) when another plan replaced this one or any removal
    /// was published since it was planned, whatever range it read, a wrap
    /// included.
    fn install_refill(
        &mut self,
        plan: &SinkRefillPlan,
        window: SinkWindow,
        newest_id: u64,
        exhausted: bool,
    ) -> bool {
        if self.pending_refill != Some(plan.token) {
            return false;
        }
        self.pending_refill = None;
        if self.removal_generation != plan.generation {
            return false;
        }
        let sweep_upper = plan.upper.unwrap_or(newest_id);
        self.sweep_upper = sweep_upper;
        self.resident_through = if exhausted {
            sweep_upper
        } else {
            window.entries.back().map_or(sweep_upper, |entry| entry.id)
        };
        self.entries = window.entries;
        self.credits = window.credits;
        self.owner = window.owner;
        true
    }

    /// Remove these ids from the resident window without returning their
    /// credit: the caller returns it (`publish_removals`) or defers it to a
    /// publication. Returns the removed entries' credit.
    fn filter_removed(&mut self, ids: &HashSet<u64>) -> usize {
        self.removal_generation = self.removal_generation.wrapping_add(1);
        let mut write = 0;
        let mut released = 0usize;
        for read in 0..self.entries.len() {
            if ids.contains(&self.entries[read].id) {
                if self.bounded {
                    released = released.saturating_add(self.credits[read]);
                }
            } else {
                self.entries.swap(write, read);
                if self.bounded {
                    self.credits.swap(write, read);
                }
                write += 1;
            }
        }
        self.entries.truncate(write);
        if self.bounded {
            self.credits.truncate(write);
        }
        released
    }

    /// Publish committed removals of these ids, returning each removed
    /// resident entry's decoded credit to the window owner.
    fn publish_removals(&mut self, ids: &HashSet<u64>, durable_removed: u64) {
        // Compacted in place (the window's buffers are already funded); the
        // removed entries are destroyed before their credit is returned.
        let released = self.filter_removed(ids);
        if self.bounded {
            if let Some(owner) = &self.owner {
                owner.release(released);
            }
            self.depth = self.depth.saturating_sub(durable_removed);
        }
    }
}

/// Give `entry` its rewritten attempt count for `persist` and keep it only if
/// `persist` succeeds; the entry is borrowed, never copied.
fn with_rewritten_attempts<T>(
    entry: &mut SinkQueueEntry,
    attempts: u32,
    persist: impl FnOnce(&SinkQueueEntry) -> Result<T>,
) -> Result<T> {
    let previous = std::mem::replace(&mut entry.attempts, attempts);
    let result = persist(entry);
    if result.is_err() {
        entry.attempts = previous;
    }
    result
}

/// A charge reserved on a window owner for copies that live beside the
/// window (a dispatcher checkout, a purge replacement), returned when they
/// are gone.
pub(crate) struct SinkWindowCopyCharge {
    owner: Arc<crate::read_image_memory::ImageMemory>,
    bytes: usize,
}

impl SinkWindowCopyCharge {
    fn reserve(owner: &Arc<crate::read_image_memory::ImageMemory>, bytes: usize) -> Result<Self> {
        owner.reserve(bytes)?;
        Ok(Self {
            owner: owner.clone(),
            bytes,
        })
    }
}

impl Drop for SinkWindowCopyCharge {
    fn drop(&mut self) {
        self.owner.release(self.bytes);
    }
}

impl Serialize for SinkQueueEntry {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        #[derive(Serialize)]
        struct HumanEntry<'a> {
            id: u64,
            event: &'a SinkEvent,
            row_id: &'a RowId,
            attempts: u32,
            gate: &'a EventGateSnapshot,
        }
        #[derive(Serialize)]
        struct SavedEvent<'a> {
            event_type: &'a str,
            table: &'a str,
            row_values: &'a HashMap<String, Value>,
            severity: &'a str,
            at_lsn: Lsn,
        }
        #[derive(Serialize)]
        struct SavedEntry<'a> {
            id: u64,
            event: SavedEvent<'a>,
            row_id: &'a RowId,
            attempts: u32,
            gate: &'a EventGateSnapshot,
        }
        if serializer.is_human_readable() {
            HumanEntry {
                id: self.id,
                event: &self.event,
                row_id: &self.row_id,
                attempts: self.attempts,
                gate: &self.gate,
            }
            .serialize(serializer)
        } else {
            SavedEntry {
                id: self.id,
                event: SavedEvent {
                    event_type: &self.event.event_type,
                    table: &self.event.table,
                    row_values: &self.event.row_values,
                    severity: &self.event.severity,
                    at_lsn: self.event.at_lsn,
                },
                row_id: &self.row_id,
                attempts: self.attempts,
                gate: &self.gate,
            }
            .serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for SinkQueueEntry {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct HumanEntry {
            id: u64,
            event: SinkEvent,
            row_id: RowId,
            attempts: u32,
            #[serde(default)]
            gate: EventGateSnapshot,
        }
        #[derive(Deserialize)]
        struct SavedEvent {
            event_type: String,
            table: String,
            row_values: HashMap<String, Value>,
            severity: String,
            at_lsn: Lsn,
        }
        #[derive(Deserialize)]
        struct SavedEntry {
            id: u64,
            event: SavedEvent,
            row_id: RowId,
            attempts: u32,
            #[serde(default)]
            gate: EventGateSnapshot,
        }
        if deserializer.is_human_readable() {
            let saved = HumanEntry::deserialize(deserializer)?;
            let mut event = saved.event;
            event.action_id = saved.id;
            Ok(Self {
                id: saved.id,
                event,
                row_id: saved.row_id,
                attempts: saved.attempts,
                gate: saved.gate,
            })
        } else {
            let saved = SavedEntry::deserialize(deserializer)?;
            Ok(Self {
                id: saved.id,
                event: SinkEvent {
                    action_id: saved.id,
                    event_type: saved.event.event_type,
                    table: saved.event.table,
                    row_values: saved.event.row_values,
                    severity: saved.event.severity,
                    at_lsn: saved.event.at_lsn,
                },
                row_id: saved.row_id,
                attempts: saved.attempts,
                gate: saved.gate,
            })
        }
    }
}

#[derive(Default)]
struct QueueMutationState {
    checkout_paused: bool,
    mutation_blocked: bool,
    checked_out: HashSet<(String, u64)>,
    active_mutations: u64,
    dispatcher_writes_closed: bool,
}

thread_local! {
    static EVENT_BUS_CALLBACK_DEPTH: Cell<u32> = const { Cell::new(0) };
    static EVENT_BUS_CALLBACK_OWNER: Cell<usize> = const { Cell::new(0) };
}

struct EventBusCallbackGuard {
    previous_owner: usize,
}

impl EventBusCallbackGuard {
    fn enter(event_bus: &Arc<EventBusState>) -> Self {
        EVENT_BUS_CALLBACK_DEPTH.with(|depth| depth.set(depth.get().saturating_add(1)));
        let previous_owner =
            EVENT_BUS_CALLBACK_OWNER.with(|owner| owner.replace(Arc::as_ptr(event_bus) as usize));
        Self { previous_owner }
    }
}

impl Drop for EventBusCallbackGuard {
    fn drop(&mut self) {
        EVENT_BUS_CALLBACK_OWNER.with(|owner| owner.set(self.previous_owner));
        EVENT_BUS_CALLBACK_DEPTH.with(|depth| depth.set(depth.get().saturating_sub(1)));
    }
}

impl EventBusState {
    pub(super) fn callback_active_on_current_thread(&self) -> bool {
        EVENT_BUS_CALLBACK_OWNER.with(|owner| owner.get() == self as *const Self as usize)
    }
}

/// Phase one of authoritative purge: new dispatcher checkout is paused and
/// every callback already checked out has settled.  It deliberately leaves
/// ordinary queue mutations enabled so callback-owned database work can finish
/// before the purge takes the transaction commit mutex.
pub(crate) struct AuthoritativePurgeQueueDrainGuard {
    state: Arc<Mutex<QueueMutationState>>,
    waiters: Arc<Condvar>,
    promoted_to_mutation_freeze: bool,
}

impl Drop for AuthoritativePurgeQueueDrainGuard {
    fn drop(&mut self) {
        if self.promoted_to_mutation_freeze {
            return;
        }
        let mut state = self.state.lock();
        state.checkout_paused = false;
        state.mutation_blocked = false;
        self.waiters.notify_all();
    }
}

/// Phase two of authoritative purge. It can only be created by consuming a
/// drained checkout guard after the caller has entered the transaction commit
/// mutex. Dropping it reopens ordinary queue mutations if preparation aborts
/// before publication.
pub(crate) struct AuthoritativePurgeQueueMutationToken {
    state: Arc<Mutex<QueueMutationState>>,
    waiters: Arc<Condvar>,
}

impl Drop for AuthoritativePurgeQueueMutationToken {
    fn drop(&mut self) {
        let mut state = self.state.lock();
        state.checkout_paused = false;
        state.mutation_blocked = false;
        self.waiters.notify_all();
    }
}

impl AuthoritativePurgeQueueDrainGuard {
    /// Freeze mutations only after the future purge kernel owns the
    /// transaction commit mutex. Consuming the phase-one guard makes the lock
    /// order explicit: drain callback work first, then enter the commit path,
    /// then wait for durable/memory queue leases to settle.
    pub(crate) fn freeze_for_commit_locked_purge(mut self) -> AuthoritativePurgeQueueMutationToken {
        let mut state = self.state.lock();
        debug_assert!(state.checkout_paused);
        debug_assert!(!state.mutation_blocked);
        state.mutation_blocked = true;
        while state.active_mutations != 0 {
            self.waiters.wait(&mut state);
        }
        drop(state);
        self.promoted_to_mutation_freeze = true;
        AuthoritativePurgeQueueMutationToken {
            state: self.state.clone(),
            waiters: self.waiters.clone(),
        }
    }
}

/// A normal queue mutation lease. It covers the durable queue operation and
/// its corresponding memory publication; phase-two purge waits for all such
/// leases before copying the replacement map.
pub(crate) struct QueueMutationLease {
    state: Arc<Mutex<QueueMutationState>>,
    waiters: Arc<Condvar>,
}

impl Drop for QueueMutationLease {
    fn drop(&mut self) {
        let mut state = self.state.lock();
        state.active_mutations = state.active_mutations.saturating_sub(1);
        self.waiters.notify_all();
    }
}

impl EventBusState {
    /// The current callback may have initiated close, so its acknowledgement
    /// must be refused after the writer's final durable handoff. Every prior
    /// dispatcher mutation finishes before this method returns.
    fn close_dispatcher_queue_writes(&self) {
        let mut state = self.queue_mutation_state.lock();
        state.dispatcher_writes_closed = true;
        while state.active_mutations != 0 {
            self.queue_mutation_waiters.wait(&mut state);
        }
    }

    fn begin_dispatcher_queue_mutation(&self) -> Result<QueueMutationLease> {
        let mut state = self.queue_mutation_state.lock();
        while state.mutation_blocked && !state.dispatcher_writes_closed {
            self.queue_mutation_waiters.wait(&mut state);
        }
        if state.dispatcher_writes_closed {
            return Err(Error::EventBusQueueQuiescent);
        }
        state.active_mutations = state.active_mutations.saturating_add(1);
        drop(state);
        Ok(QueueMutationLease {
            state: self.queue_mutation_state.clone(),
            waiters: self.queue_mutation_waiters.clone(),
        })
    }

    fn queue_checkout_guard(&self) -> parking_lot::MutexGuard<'_, QueueMutationState> {
        let mut state = self.queue_mutation_state.lock();
        while state.checkout_paused {
            self.queue_mutation_waiters.wait(&mut state);
        }
        state
    }

    pub(crate) fn begin_queue_mutation(&self) -> QueueMutationLease {
        let mut state = self.queue_mutation_state.lock();
        while state.mutation_blocked {
            self.queue_mutation_waiters.wait(&mut state);
        }
        state.active_mutations = state.active_mutations.saturating_add(1);
        drop(state);
        QueueMutationLease {
            state: self.queue_mutation_state.clone(),
            waiters: self.queue_mutation_waiters.clone(),
        }
    }

    /// Phase one of the authoritative-purge queue protocol. Call this before
    /// taking the transaction commit mutex, then move the returned guard into
    /// `freeze_for_commit_locked_purge` from that mutex's active-prepare
    /// callback.
    pub(crate) fn begin_authoritative_purge_queue_drain(
        &self,
    ) -> Result<AuthoritativePurgeQueueDrainGuard> {
        if EVENT_BUS_CALLBACK_DEPTH.with(|depth| depth.get() != 0) {
            return Err(Error::SyncError(
                "authoritative purge cannot begin from an event-bus callback".to_string(),
            ));
        }
        let mut state = self.queue_mutation_state.lock();
        while state.checkout_paused || state.mutation_blocked {
            self.queue_mutation_waiters.wait(&mut state);
        }
        // No new dispatcher checkout, while normal durable queue mutations
        // remain allowed so callbacks and child DB work can settle.
        state.checkout_paused = true;
        while !state.checked_out.is_empty() {
            self.queue_mutation_waiters.wait(&mut state);
        }
        drop(state);
        Ok(AuthoritativePurgeQueueDrainGuard {
            state: self.queue_mutation_state.clone(),
            waiters: self.queue_mutation_waiters.clone(),
            promoted_to_mutation_freeze: false,
        })
    }

    fn finish_checked_out_entry(&self, sink: &str, entry_id: u64) {
        let mut state = self.queue_mutation_state.lock();
        state.checked_out.remove(&(sink.to_string(), entry_id));
        self.queue_mutation_waiters.notify_all();
    }

    /// Build the complete post-purge queue image before durability.  The
    /// payload, attempt count, and access gate remain in their original
    /// `SinkQueueEntry`; publication later swaps this opaque VecDeque map.
    pub(crate) fn prepare_authoritative_purge_queue_replacement(
        &self,
        queue_mutation_token: AuthoritativePurgeQueueMutationToken,
        durable_sink_names: BTreeSet<String>,
        selections: &BTreeMap<String, HashSet<RowId>>,
    ) -> Result<PreparedAuthoritativePurgeEventBusPublication> {
        let mut sink_names = durable_sink_names;
        sink_names.extend(self.queues.lock().keys().cloned());
        // A bounded queue's replacement filters only its resident window;
        // the durable removal count of each sink is recorded once the purge
        // has selected its durable entries, and applied to depth at the swap.
        // The replacement copies every resident window: fund the copies on
        // each window's owner before making them, until the swap destroys the
        // originals.
        // The copy includes each window's buffers, funded at their capacity.
        let current = self.queues.lock();
        let mut overlap = Vec::new();
        for queue in current.values() {
            if let Some(owner) = &queue.owner {
                let buffers = queue
                    .entries
                    .capacity()
                    .saturating_mul(std::mem::size_of::<SinkQueueEntry>())
                    .saturating_add(
                        queue
                            .credits
                            .capacity()
                            .saturating_mul(std::mem::size_of::<usize>()),
                    );
                overlap.push(SinkWindowCopyCharge::reserve(
                    owner,
                    queue.resident_bytes().saturating_add(buffers),
                )?);
            }
        }
        let mut queues = current.clone();
        drop(current);
        // Filtering the copy returns nothing to the shared window owner yet:
        // the originals still hold those bytes. The removed entries' credit
        // is returned only by a successful publication, so a preparation that
        // is dropped leaves every owner exactly as it was.
        let mut deferred_releases = Vec::new();
        for sink in &sink_names {
            let queue = queues
                .entry(sink.clone())
                .or_insert_with(|| self.fresh_queue());
            let removed = queue
                .entries
                .iter()
                .filter(|entry| {
                    selections
                        .get(&entry.event.table)
                        .is_some_and(|ids| ids.contains(&entry.row_id))
                })
                .map(|entry| entry.id)
                .collect::<HashSet<_>>();
            if !removed.is_empty() {
                let released = queue.filter_removed(&removed);
                if let Some(owner) = &queue.owner {
                    deferred_releases.push((owner.clone(), released));
                }
            }
        }
        Ok(PreparedAuthoritativePurgeEventBusPublication {
            queues,
            sink_names,
            durable_removed: BTreeMap::new(),
            deferred_releases,
            _overlap: overlap,
            _queue_mutation_token: queue_mutation_token,
        })
    }

    /// The later commit kernel may call only this direct swap after its
    /// durable transaction succeeds.  Preparation itself never calls it.
    pub(crate) fn publish_prepared_authoritative_purge_queue_replacement(
        &self,
        publication: PreparedAuthoritativePurgeEventBusPublication,
    ) {
        // `publication` owns the paused-checkout token acquired during preparation.
        // This direct swap therefore cannot discard an enqueue or an in-flight
        // worker acknowledgement that arrived after the snapshot.
        let mut replacement = publication.queues;
        for (sink, removed) in &publication.durable_removed {
            if let Some(queue) = replacement.get_mut(sink)
                && queue.bounded
            {
                queue.depth = queue.depth.saturating_sub(*removed);
                // A prepared refill may have read a purged entry.
                queue.removal_generation = queue.removal_generation.wrapping_add(1);
            }
        }
        let mut queues = self.queues.lock();
        let originals = std::mem::replace(&mut *queues, replacement);
        drop(queues);
        // The originals are destroyed before the purged entries' credit and
        // the copies' overlap charge are returned.
        drop(originals);
        for (owner, released) in publication.deferred_releases {
            owner.release(released);
        }
        drop(publication.durable_removed);
        drop(publication._overlap);
        self.waiters.notify_all();
    }
}

/// Opaque full queue replacement for authoritative purge.  Keeping the
/// VecDeque private prevents a later kernel from rebuilding entries after
/// durability and accidentally losing retry or gate state.
pub(crate) struct PreparedAuthoritativePurgeEventBusPublication {
    queues: HashMap<String, SinkQueue>,
    sink_names: BTreeSet<String>,
    /// Durable entries the purge removes from each sink, resident or not.
    durable_removed: BTreeMap<String, u64>,
    /// Credit of the purged resident entries, returned to their window owner
    /// only when this replacement is published.
    deferred_releases: Vec<(Arc<crate::read_image_memory::ImageMemory>, usize)>,
    /// Funding for the copied windows while the originals still live.
    _overlap: Vec<SinkWindowCopyCharge>,
    _queue_mutation_token: AuthoritativePurgeQueueMutationToken,
}

impl PreparedAuthoritativePurgeEventBusPublication {
    pub(crate) fn authoritative_purge_sink_names(&self) -> &BTreeSet<String> {
        &self.sink_names
    }

    /// Count one durable entry the purge's commit removes from `sink`.
    pub(crate) fn record_durable_sink_removal(&mut self, sink: &str) {
        *self.durable_removed.entry(sink.to_owned()).or_default() += 1;
    }
}

pub(crate) type PreparedSinkEvent = (String, SinkQueueEntry);

/// A received-schema stage must not reserve queue ids until the matching
/// Redb transaction has committed.  This carries the pure projection and the
/// counter value to publish afterwards.
pub(crate) struct PreparedReceivedSinkEvents {
    pub(crate) events: Vec<PreparedSinkEvent>,
    pub(crate) next_queue_id: u64,
}
type EncodedConfigValues = Vec<(&'static str, Vec<u8>)>;

#[derive(Debug, Clone)]
pub(crate) struct EventBusPersistenceCommit {
    pub(crate) config_values: Vec<(String, Vec<u8>)>,
    pub(crate) ddl: Vec<DdlChange>,
    pub(crate) generation_sidecars: Vec<super::DurableDdlGenerationSidecar>,
}

/// Event definitions and their encoded durable config, prepared before the
/// received-schema transaction commits.  The inner definitions remain
/// private: this is an engine staging handle, not a public schema API.
#[derive(Clone)]
pub(crate) struct PreparedEventBusPublication {
    definitions: EventBusDefinitions,
    pub(crate) config_values: Vec<(String, Vec<u8>)>,
}

#[derive(Debug, Clone)]
struct StagedEventBusDdlCommit {
    definitions: EventBusDefinitions,
    persistence: EventBusPersistenceCommit,
}

#[derive(Clone)]
struct SinkRegistration {
    access: AccessConstraints,
    callback: SinkCallback,
}

#[derive(Debug, Clone, Default)]
struct SinkMetricsState {
    delivered: u64,
    retried: u64,
    uncertain: u64,
    unconfirmed: u64,
    permanent_failures: u64,
    in_flight: u64,
    /// Queue entries pulled to the front and gate-evaluated by the dispatcher
    /// (delivered, permanently failed, or denied-and-rotated alike). Always-on,
    /// carried on this existing per-sink metrics mutex; see `SinkMetrics::examined`.
    examined: u64,
    window_refusals: u64,
    window_refused: bool,
    /// Window reads the dispatcher made, and idle pauses it took before one.
    window_reads: u64,
    window_idle_pauses: u64,
}

#[derive(Debug)]
struct SinkRuntime {
    shutdown: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum EventBusThreadShutdown {
    AllJoined,
    DeferredResourceCleanup,
}

impl EventBusThreadShutdown {
    pub(super) fn deferred_resource_cleanup(self) -> bool {
        matches!(self, Self::DeferredResourceCleanup)
    }
}

struct EventBusResourceCleanup {
    sink: String,
    persistence: Option<Arc<RedbPersistence>>,
    open_registry_path: Option<PathBuf>,
    close_completion: Option<(Arc<Mutex<CloseProgress>>, Arc<Condvar>, bool)>,
    /// A dropped owner left its writer open so the dispatcher could record
    /// the delivery it was making; the dispatcher makes the writer quiescent
    /// once that acknowledgement is durable, before it lets go.
    publish_quiescence: bool,
}

impl EventBusResourceCleanup {
    fn close(self) {
        if self.publish_quiescence
            && let Some(persistence) = &self.persistence
        {
            let published = persistence.make_writer_quiescent().and_then(|()| {
                persistence.record_owner_read_status(OwnerReadStatus {
                    state: OwnerServingState::NotServing,
                    reason: Some(OwnerServingReason::ShutdownDraining),
                })
            });
            if let Err(error) = published {
                tracing::warn!(
                    error = %error,
                    "could not publish quiescent owner status after the last delivery"
                );
            }
        }
        if let Some(persistence) = self.persistence {
            persistence.close();
        }
        if let Some(path) = self.open_registry_path {
            release_open_registry_path(&path);
        }
        if let Some((progress, changed, succeeded)) = self.close_completion {
            *progress.lock() = if succeeded {
                CloseProgress::Complete
            } else {
                CloseProgress::Idle
            };
            changed.notify_all();
        }
    }
}

#[derive(Debug, Clone)]
struct RowEventCandidate {
    trigger: EventTrigger,
    table: String,
    row_id: RowId,
    values: HashMap<String, Value>,
}

struct RowEventCandidateRef<'a> {
    trigger: EventTrigger,
    table: &'a str,
    row_id: RowId,
    values: &'a HashMap<String, Value>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct EventGateSnapshot {
    context_column: Option<String>,
    scope_read: Option<EventScopeReadGate>,
    acl: Option<EventAclGate>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct EventScopeReadGate {
    column: String,
    read_labels: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct EventAclGate {
    column: String,
    ref_table: String,
    ref_column: String,
    grant_gate: Box<EventGateSnapshot>,
    grant_rows: EventGrantRows,
}

/// Immutable copied grant values travel with every prepared/queued event.
/// Cloning a gate shares the payload and its charge rather than copying it.
#[derive(Debug, Clone)]
enum EventGrantRows {
    Legacy(Vec<HashMap<String, Value>>),
    Admitted(Arc<AdmittedEventGrants>),
}
impl Default for EventGrantRows {
    fn default() -> Self {
        Self::Legacy(Vec::new())
    }
}
#[derive(Debug)]
struct AdmittedEventGrants {
    rows: Vec<HashMap<String, Value>>,
    _memory: Option<crate::memory_accounting::OwnedMemoryReservation>,
}
impl EventGrantRows {
    fn new(
        rows: Vec<HashMap<String, Value>>,
        memory: Option<crate::memory_accounting::OwnedMemoryReservation>,
    ) -> Self {
        if rows.is_empty() {
            return Self::default();
        }
        Self::Admitted(Arc::new(AdmittedEventGrants {
            rows,
            _memory: memory,
        }))
    }
    fn rows(&self) -> &[HashMap<String, Value>] {
        match self {
            Self::Legacy(rows) => rows.as_slice(),
            Self::Admitted(owner) => owner.rows.as_slice(),
        }
    }
    fn iter(&self) -> std::slice::Iter<'_, HashMap<String, Value>> {
        self.rows().iter()
    }
}
impl Serialize for EventGrantRows {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        self.rows().serialize(serializer)
    }
}
impl<'de> Deserialize<'de> for EventGrantRows {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        Vec::<HashMap<String, Value>>::deserialize(deserializer).map(Self::Legacy)
    }
}

impl EventBusState {
    pub(super) fn new() -> Self {
        Self {
            definitions: Mutex::new(EventBusDefinitions::default()),
            callbacks: RwLock::new(HashMap::new()),
            queues: Mutex::new(HashMap::new()),
            window_accountant: Mutex::new(None),
            next_refill_token: AtomicU64::new(1),
            durable_windows: AtomicBool::new(false),
            unloaded_queue_depths: Mutex::new(HashMap::new()),
            queue_mutation_state: Arc::new(Mutex::new(QueueMutationState::default())),
            queue_mutation_waiters: Arc::new(Condvar::new()),
            staged_for_persistence: Mutex::new(HashMap::new()),
            staged_ddl_for_persistence: Mutex::new(HashMap::new()),
            metrics: Mutex::new(HashMap::new()),
            runtimes: Mutex::new(HashMap::new()),
            runtime_starts: Mutex::new(HashMap::new()),
            deferred_resource_cleanup: Mutex::new(None),
            wait_lock: Mutex::new(()),
            waiters: Condvar::new(),
            next_queue_id: AtomicU64::new(1),
            #[cfg(any(test, feature = "test-seams"))]
            queue_capacity_for_test: std::sync::atomic::AtomicUsize::new(MAX_SINK_QUEUE_DEPTH),
        }
    }

    /// Bytes the resident sink windows hold on the writer accountant.
    pub(crate) fn sink_window_charge(&self) -> usize {
        self.queues
            .lock()
            .values()
            .filter_map(|queue| queue.owner.as_ref())
            .map(|owner| owner.held())
            .sum()
    }

    /// A bounded window the writer budget could not hold: counted on the
    /// sink, which stays stalled until a later window read succeeds.
    fn record_window_refusal(&self, sink: &str, error: &Error) {
        tracing::warn!(sink, %error, "a sink window does not fit the writer memory limit");
        let mut metrics = self.metrics.lock();
        let state = metrics.entry(sink.to_owned()).or_default();
        state.window_refusals = state.window_refusals.saturating_add(1);
        state.window_refused = true;
    }

    fn clear_window_refusal(&self, sink: &str) {
        if let Some(state) = self.metrics.lock().get_mut(sink) {
            state.window_refused = false;
        }
    }

    /// An empty queue of the kind this writer keeps.
    fn fresh_queue(&self) -> SinkQueue {
        SinkQueue {
            bounded: self.durable_windows.load(Ordering::SeqCst),
            ..SinkQueue::default()
        }
    }

    fn allocate_queue_id(&self) -> u64 {
        self.next_queue_id.fetch_add(1, Ordering::SeqCst)
    }

    pub(crate) fn next_action_id(&self) -> u64 {
        self.next_queue_id.load(Ordering::SeqCst)
    }

    pub(crate) fn ensure_queue_capacity(&self, events: &[PreparedSinkEvent]) -> Result<()> {
        #[cfg(any(test, feature = "test-seams"))]
        let capacity = self.queue_capacity_for_test.load(Ordering::SeqCst);
        #[cfg(not(any(test, feature = "test-seams")))]
        let capacity = MAX_SINK_QUEUE_DEPTH;
        let queues = self.queues.lock();
        let mut additions = HashMap::<&str, usize>::new();
        for (sink, _) in events {
            let count = additions.entry(sink.as_str()).or_default();
            *count += 1;
            if queues
                .get(sink)
                .map_or(0, |queue| {
                    usize::try_from(queue.depth()).unwrap_or(usize::MAX)
                })
                .saturating_add(*count)
                > capacity
            {
                return Err(Error::Other(format!("sink queue is full: {sink}")));
            }
        }
        Ok(())
    }

    pub(crate) fn stage_sink_events_for_persistence(
        &self,
        lsn: Lsn,
        events: Vec<PreparedSinkEvent>,
    ) {
        if !events.is_empty() {
            self.staged_for_persistence
                .lock()
                .insert(lsn, Arc::new(events));
        }
    }

    pub(crate) fn staged_sink_events_for_persistence(
        &self,
        lsn: Lsn,
    ) -> Option<Arc<Vec<PreparedSinkEvent>>> {
        self.staged_for_persistence.lock().get(&lsn).cloned()
    }

    pub(crate) fn take_staged_sink_events_for_persistence(
        &self,
        lsn: Lsn,
    ) -> Option<Arc<Vec<PreparedSinkEvent>>> {
        self.staged_for_persistence.lock().remove(&lsn)
    }

    pub(crate) fn materialize_staged_sink_events(
        events: Arc<Vec<PreparedSinkEvent>>,
    ) -> Vec<PreparedSinkEvent> {
        Arc::try_unwrap(events).unwrap_or_else(|events| (*events).clone())
    }

    pub(crate) fn staged_event_bus_persistence_commit(
        &self,
        lsn: Lsn,
    ) -> Option<EventBusPersistenceCommit> {
        self.staged_ddl_for_persistence
            .lock()
            .get(&lsn)
            .map(|staged| staged.persistence.clone())
    }

    fn stage_event_bus_ddl_commit(
        &self,
        lsn: Lsn,
        definitions: EventBusDefinitions,
        persistence: EventBusPersistenceCommit,
    ) {
        self.staged_ddl_for_persistence.lock().insert(
            lsn,
            StagedEventBusDdlCommit {
                definitions,
                persistence,
            },
        );
    }
}

impl Database {
    /// Project every event-bus declaration against the caller's complete
    /// post-DDL table view, and encode all durable values before publication.
    /// Source-order coordination belongs to `ReceivedSchemaStage`; this helper
    /// deliberately does not append category-local DDL to the log.
    #[allow(dead_code)] // consumed when received-schema publication joins the staged Redb transaction
    pub(super) fn prepare_received_event_bus_publication(
        &self,
        ddl: &[DdlChange],
        projected_tables: &HashMap<String, TableMeta>,
    ) -> Result<PreparedEventBusPublication> {
        let mut definitions = self.received_event_bus_projection();
        for change in ddl {
            self.apply_received_event_bus_ddl_to_projection(
                &mut definitions,
                change,
                projected_tables,
            )?;
        }
        self.prepare_received_event_bus_publication_from_projection(definitions)
    }

    pub(super) fn received_event_bus_projection_bytes(&self) -> Result<usize> {
        let definitions = self.event_bus.definitions.lock();
        let encoded = bincode::serde::encode_into_std_write(
            &*definitions,
            &mut std::io::sink(),
            bincode::config::standard(),
        )
        .map_err(|error| Error::Other(format!("price received event definitions: {error}")))?;
        let entries =
            definitions.event_types.len() + definitions.sinks.len() + definitions.routes.len();
        let filter_entries = definitions.routes.values().fold(0usize, |entries, route| {
            entries.saturating_add(
                route
                    .where_in
                    .as_ref()
                    .map_or(0, |filter| filter.values.len()),
            )
        });
        Ok(encoded
            .saturating_add(entries.saturating_mul(512))
            .saturating_add(filter_entries.saturating_mul(32)))
    }

    pub(super) fn received_event_bus_projection(&self) -> EventBusDefinitions {
        self.event_bus.definitions.lock().clone()
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub(super) fn published_definition_names_for_test(
        &self,
    ) -> (Vec<String>, Vec<String>, Vec<String>) {
        let definitions = self.event_bus.definitions.lock();
        (
            definitions.event_types.keys().cloned().collect(),
            definitions.sinks.keys().cloned().collect(),
            definitions.routes.keys().cloned().collect(),
        )
    }

    /// Seed a detached received-schema adjudicator with the receiver's live
    /// declarations. Received DDL is replayed in source order on top of this
    /// projection, so separately committed event type, sink, and route
    /// declarations remain idempotent when a later mixed DDL/data unit arrives.
    pub(super) fn seed_received_event_bus_projection(&self, definitions: EventBusDefinitions) {
        *self.event_bus.definitions.lock() = definitions;
    }

    pub(super) fn apply_received_event_bus_ddl_to_projection(
        &self,
        definitions: &mut EventBusDefinitions,
        ddl: &DdlChange,
        projected_tables: &HashMap<String, TableMeta>,
    ) -> Result<()> {
        Self::apply_event_bus_ddl_to_definitions_with_table_lookup(definitions, ddl, &|table| {
            projected_tables.contains_key(table)
        })
    }

    pub(super) fn prepare_received_event_bus_publication_from_projection(
        &self,
        definitions: EventBusDefinitions,
    ) -> Result<PreparedEventBusPublication> {
        let config_values = Self::encoded_event_bus_config_values(&definitions)?
            .into_iter()
            .map(|(key, value)| (key.to_string(), value))
            .collect();
        Ok(PreparedEventBusPublication {
            definitions,
            config_values,
        })
    }

    /// Publish a received-schema event-bus replacement while the caller holds
    /// the mutation lease spanning its durable transaction.
    pub(super) fn publish_prepared_event_bus_publication_under_queue_mutation_lease(
        &self,
        publication: PreparedEventBusPublication,
        received_events: Vec<PreparedSinkEvent>,
    ) {
        let sink_names = publication
            .definitions
            .sinks
            .keys()
            .cloned()
            .collect::<HashSet<_>>();
        *self.event_bus.definitions.lock() = publication.definitions;
        self.event_bus
            .callbacks
            .write()
            .retain(|sink, _| sink_names.contains(sink));
        let mut queues = self.event_bus.queues.lock();
        queues.retain(|sink, _| sink_names.contains(sink));
        for sink in &sink_names {
            queues
                .entry(sink.clone())
                .or_insert_with(|| self.event_bus.fresh_queue());
        }
        for (sink, entry) in received_events {
            let queue = queues
                .entry(sink)
                .or_insert_with(|| self.event_bus.fresh_queue());
            assert!(
                queue.depth() < MAX_SINK_QUEUE_DEPTH as u64,
                "committed sink queue capacity"
            );
            queue.publish_append(entry);
        }
        drop(queues);
        self.event_bus.waiters.notify_all();
    }

    pub(super) fn load_event_bus_state_from_persistence(&self) -> Result<()> {
        let Some(source) = self.startup_state() else {
            return Ok(());
        };
        let definitions = EventBusDefinitions {
            event_types: source
                .config_value::<Vec<EventTypeDef>>(EVENT_TYPES_CONFIG_KEY)?
                .unwrap_or_default()
                .into_iter()
                .map(|event_type| (event_type.name.clone(), event_type))
                .collect(),
            sinks: source
                .config_value::<Vec<SinkDef>>(SINKS_CONFIG_KEY)?
                .unwrap_or_default()
                .into_iter()
                .map(|sink| (sink.name.clone(), sink))
                .collect(),
            routes: source
                .config_value::<Vec<RouteDef>>(ROUTES_CONFIG_KEY)?
                .unwrap_or_default()
                .into_iter()
                .map(|route| (route.name.clone(), route))
                .collect(),
        };
        let live_tables = self
            .relational_store
            .table_meta
            .read()
            .keys()
            .cloned()
            .collect::<HashSet<_>>();
        let definitions = sanitize_event_bus_definitions(definitions, &live_tables);
        let sink_names = definitions.sinks.keys().cloned().collect::<Vec<_>>();
        *self.event_bus.definitions.lock() = definitions;

        let _queue_mutation = self.event_bus.begin_queue_mutation();
        let mut queues = self.event_bus.queues.lock();
        queues.clear();
        let mut unloaded = self.event_bus.unloaded_queue_depths.lock();
        unloaded.clear();
        let durable_windows = matches!(source, StartupState::Durable(_));
        self.event_bus
            .durable_windows
            .store(durable_windows, Ordering::SeqCst);
        let window_accountant = durable_windows.then(|| {
            crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant)
        });
        *self.event_bus.window_accountant.lock() = window_accountant.clone();
        let mut max_id = source
            .config_value::<u64>(NEXT_ACTION_ID_CONFIG_KEY)?
            .unwrap_or(1)
            .saturating_sub(1);
        for sink in sink_names {
            let uncertain = source
                .config_value::<u64>(&format!("{UNCERTAIN_COUNT_CONFIG_PREFIX}{sink}"))?
                .unwrap_or(0);
            let unconfirmed = source
                .config_value::<u64>(&format!("{UNCONFIRMED_COUNT_CONFIG_PREFIX}{sink}"))?
                .unwrap_or(0);
            let mut metrics = self.event_bus.metrics.lock();
            let state = metrics.entry(sink.clone()).or_default();
            state.uncertain = uncertain;
            state.unconfirmed = unconfirmed;
            drop(metrics);
            if let Some(extent) = source.unloaded_sink_queue(&sink) {
                max_id = max_id.max(extent.newest_id);
                unloaded.insert(sink.clone(), extent.depth);
                queues.insert(sink, SinkQueue::default());
                continue;
            }
            if let StartupState::Durable(persistence) = &source {
                // Read one bounded window of the durable queue; the rest is
                // read as dispatch reaches it. A window the writer budget
                // cannot hold is left in the store and the refusal is recorded
                // on the sink; opening does not fail for it.
                let read = persistence.load_sink_queue_window::<SinkQueueEntry>(
                    &sink,
                    None,
                    None,
                    SINK_WINDOW_ENTRIES,
                    SINK_WINDOW_BYTES,
                    crate::memory_accounting::AccountantHandle::clone(
                        window_accountant
                            .as_ref()
                            .expect("a durable store has a window accountant"),
                    ),
                )?;
                max_id = max_id.max(read.newest_id);
                let mut queue = SinkQueue {
                    depth: read.depth,
                    bounded: true,
                    sweep_upper: read.newest_id,
                    ..SinkQueue::default()
                };
                if let Some(error) = &read.refusal {
                    self.event_bus.record_window_refusal(&sink, error);
                }
                if let Some(owner) = read.owner {
                    let mut entries = VecDeque::from(read.entries);
                    for entry in &mut entries {
                        entry.event.action_id = entry.id;
                    }
                    queue.resident_through = if read.exhausted {
                        read.newest_id
                    } else {
                        entries.back().map_or(0, |entry| entry.id)
                    };
                    queue.entries = entries;
                    queue.credits = VecDeque::from(read.credits);
                    queue.owner = Some(owner);
                }
                queues.insert(sink, queue);
                continue;
            }
            let mut queue = source.sink_queue::<SinkQueueEntry>(&sink)?;
            for entry in &mut queue {
                entry.event.action_id = entry.id;
            }
            max_id = max_id.max(queue.iter().map(|entry| entry.id).max().unwrap_or(0));
            queues.insert(sink, SinkQueue::unbounded(queue.into()));
        }
        drop(unloaded);
        self.event_bus
            .next_queue_id
            .store(max_id.saturating_add(1).max(1), Ordering::SeqCst);
        Ok(())
    }

    fn put_event_type_def(definitions: &mut EventBusDefinitions, def: EventTypeDef) -> Result<()> {
        if let Some(existing) = definitions.event_types.get(&def.name) {
            if existing == &def {
                return Ok(());
            }
            return Err(Error::Other(format!(
                "event type already exists with a different definition: {}",
                def.name
            )));
        }
        definitions.event_types.insert(def.name.clone(), def);
        Ok(())
    }

    fn put_sink_def(definitions: &mut EventBusDefinitions, def: SinkDef) -> Result<()> {
        if let Some(existing) = definitions.sinks.get(&def.name) {
            if existing == &def {
                return Ok(());
            }
            return Err(Error::Other(format!(
                "sink already exists with a different definition: {}",
                def.name
            )));
        }
        definitions.sinks.insert(def.name.clone(), def);
        Ok(())
    }

    fn put_route_def(definitions: &mut EventBusDefinitions, def: RouteDef) -> Result<String> {
        let route_table = definitions
            .event_types
            .get(&def.event_type)
            .map(|event_type| event_type.table.clone())
            .ok_or_else(|| Error::Other(format!("event type not found: {}", def.event_type)))?;
        if !definitions.sinks.contains_key(&def.sink) {
            return Err(Error::Other(format!("sink not found: {}", def.sink)));
        }
        if let Some(existing) = definitions.routes.get(&def.name) {
            if existing == &def {
                return Ok(route_table);
            }
            return Err(Error::Other(format!(
                "route already exists with a different definition: {}",
                def.name
            )));
        }
        definitions.routes.insert(def.name.clone(), def);
        Ok(route_table)
    }

    fn drop_route_def_from(definitions: &mut EventBusDefinitions, name: &str) -> Option<String> {
        let removed = definitions.routes.remove(name)?;
        definitions
            .event_types
            .get(&removed.event_type)
            .map(|event_type| event_type.table.clone())
            .or(Some(String::new()))
    }

    pub(super) fn validate_event_bus_ddl_batch_with_table_lookup<F>(
        &self,
        ddl: &[DdlChange],
        table_exists: F,
    ) -> Result<()>
    where
        F: Fn(&str) -> bool,
    {
        if ddl.is_empty() {
            return Ok(());
        }
        let mut projected = self.event_bus.definitions.lock().clone();
        for change in ddl {
            Self::apply_event_bus_ddl_to_definitions_with_table_lookup(
                &mut projected,
                change,
                &table_exists,
            )?;
        }
        Ok(())
    }

    pub(super) fn stage_event_bus_ddl_in_tx(&self, tx: TxId, ddl: DdlChange) -> Result<()> {
        if self.pending_local_schema_stages.lock().contains_key(&tx) {
            return Err(Error::Other(
                "event-bus DDL and transactional local table schema DDL cannot be combined yet; commit or roll back before changing the other schema surface"
                    .to_string(),
            ));
        }
        self.tx_mgr.cloned_write_set(tx)?;
        self.require_admin_event_bus_ddl()?;
        let _ = self.event_bus_definitions_with_ddl(Some(tx), std::slice::from_ref(&ddl))?;
        self.pending_event_bus_ddl
            .lock()
            .entry(tx)
            .or_default()
            .push(ddl);
        Ok(())
    }

    pub(super) fn take_pending_event_bus_ddl(&self, tx: TxId) -> Vec<DdlChange> {
        self.pending_event_bus_ddl
            .lock()
            .remove(&tx)
            .unwrap_or_default()
    }

    pub(super) fn apply_event_bus_ddl_from_user(
        &self,
        ddl: Vec<DdlChange>,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<()> {
        self.require_admin_event_bus_ddl()?;
        self.apply_event_bus_ddl_batch(ddl, read_memory.clone())
    }

    pub(super) fn stage_event_bus_ddl_for_commit(
        &self,
        lsn: Lsn,
        ddl: &[DdlChange],
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<u32> {
        if ddl.is_empty() {
            return Ok(0);
        }
        let mut projected = self.event_bus.definitions.lock().clone();
        for change in ddl {
            self.apply_event_bus_ddl_to_definitions(&mut projected, change)?;
        }
        if *self.event_bus.definitions.lock() == projected {
            return Ok(0);
        }
        let ddl_count = u32::try_from(ddl.len())
            .map_err(|_| Error::SyncError("event-bus DDL ordinal overflow".to_string()))?;
        let mut config_values = Self::encoded_event_bus_config_values(&projected)?
            .into_iter()
            .map(|(key, value)| (key.to_string(), value))
            .collect::<Vec<_>>();
        // PersistentCompositeStore writes event-bus DDL before trigger DDL in
        // this commit, so event-bus provenance owns the leading ordinals.
        let generation_sidecars = self.ddl_generation_sidecars(ddl, read_memory.clone())?;
        let provenance_values =
            self.ddl_generation_sidecar_values(lsn, ddl, 0, read_memory.clone())?;
        config_values.extend(provenance_values);
        let persistence = EventBusPersistenceCommit {
            config_values,
            ddl: ddl.to_vec(),
            generation_sidecars,
        };
        self.event_bus
            .stage_event_bus_ddl_commit(lsn, projected, persistence);
        Ok(ddl_count)
    }

    pub(super) fn publish_staged_event_bus_ddl_commit(&self, lsn: Lsn) {
        let _queue_mutation = self.event_bus.begin_queue_mutation();
        self.publish_staged_event_bus_ddl_commit_under_queue_mutation_lease(lsn);
    }

    /// Publish staged DDL while the matching durable commit still owns its
    /// mutation lease.
    pub(super) fn publish_staged_event_bus_ddl_commit_under_queue_mutation_lease(&self, lsn: Lsn) {
        let staged = self
            .event_bus
            .staged_ddl_for_persistence
            .lock()
            .remove(&lsn);
        if let Some(staged) = staged {
            self.publish_in_memory_ddl_generation_sidecars(
                lsn,
                0,
                &staged.persistence.generation_sidecars,
            );
            self.apply_event_bus_definitions_to_memory_under_queue_mutation_lease(
                staged.definitions,
            );
            let mut ddl_log = self.ddl_log.write();
            ddl_log.extend(
                staged
                    .persistence
                    .ddl
                    .into_iter()
                    .map(|change| (lsn, change)),
            );
        }
    }

    pub(super) fn discard_staged_event_bus_ddl_commit(&self, lsn: Lsn) {
        self.event_bus
            .staged_ddl_for_persistence
            .lock()
            .remove(&lsn);
    }

    pub(super) fn apply_event_bus_ddl_batch(
        &self,
        ddl: Vec<DdlChange>,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<()> {
        if ddl.is_empty() {
            return Ok(());
        }
        let projected = self.event_bus_definitions_with_ddl(None, &ddl)?;
        if *self.event_bus.definitions.lock() == projected {
            return Ok(());
        }
        self.commit_event_bus_definitions_and_log_batch(projected, ddl, read_memory.clone())
    }

    pub(super) fn event_bus_definitions_with_ddl(
        &self,
        tx: Option<TxId>,
        ddl: &[DdlChange],
    ) -> Result<EventBusDefinitions> {
        let mut projected = self.event_bus.definitions.lock().clone();
        let pending = tx
            .and_then(|tx| self.pending_event_bus_ddl.lock().get(&tx).cloned())
            .unwrap_or_default();
        for change in pending.iter().chain(ddl.iter()) {
            self.apply_event_bus_ddl_to_definitions(&mut projected, change)?;
        }
        Ok(projected)
    }

    fn apply_event_bus_ddl_to_definitions(
        &self,
        definitions: &mut EventBusDefinitions,
        ddl: &DdlChange,
    ) -> Result<()> {
        Self::apply_event_bus_ddl_to_definitions_with_table_lookup(definitions, ddl, &|table| {
            self.table_meta(table).is_some()
        })
    }

    fn apply_event_bus_ddl_to_definitions_with_table_lookup(
        definitions: &mut EventBusDefinitions,
        ddl: &DdlChange,
        table_exists: &impl Fn(&str) -> bool,
    ) -> Result<()> {
        match ddl {
            DdlChange::CreateEventType {
                name,
                trigger,
                table,
            } => {
                if !table_exists(table) {
                    return Err(Error::TableNotFound(table.clone()));
                }
                Self::put_event_type_def(
                    definitions,
                    EventTypeDef {
                        name: name.clone(),
                        trigger: EventTrigger::from_ddl_trigger(trigger)?,
                        table: table.clone(),
                    },
                )
            }
            DdlChange::CreateSink {
                name,
                sink_type,
                url,
            } => Self::put_sink_def(
                definitions,
                SinkDef {
                    name: name.clone(),
                    sink_type: PersistedSinkType::from_ddl_sink_type(sink_type)?,
                    url: url.clone(),
                },
            ),
            DdlChange::CreateRoute {
                name,
                event_type,
                sink,
                table,
                where_in,
            } => {
                let route_table = Self::put_route_def(
                    definitions,
                    RouteDef {
                        name: name.clone(),
                        event_type: event_type.clone(),
                        sink: sink.clone(),
                        where_in: where_in
                            .as_ref()
                            .map(|(column, values)| PersistedRouteWhereIn {
                                column: column.clone(),
                                values: values.clone(),
                            }),
                    },
                )?;
                if !table.is_empty() && *table != route_table {
                    return Err(Error::Other(format!(
                        "route table mismatch for {name}: ddl={table}, event_type={route_table}"
                    )));
                }
                Ok(())
            }
            DdlChange::DropRoute { name, .. } => {
                let _ = Self::drop_route_def_from(definitions, name);
                Ok(())
            }
            DdlChange::DropTable { name } => {
                let removed_event_types = definitions
                    .event_types
                    .values()
                    .filter(|event_type| event_type.table == *name)
                    .map(|event_type| event_type.name.clone())
                    .collect::<HashSet<_>>();
                definitions
                    .event_types
                    .retain(|event_type, _| !removed_event_types.contains(event_type));
                definitions
                    .routes
                    .retain(|_, route| !removed_event_types.contains(&route.event_type));
                Ok(())
            }
            _ => Ok(()),
        }
    }

    pub(super) fn event_bus_snapshot_ddl_for_tables(
        &self,
        live_tables: &HashSet<String>,
    ) -> Vec<DdlChange> {
        let definitions = self.event_bus.definitions.lock().clone();
        let live_event_types = definitions
            .event_types
            .iter()
            .filter(|(_, event_type)| live_tables.contains(&event_type.table))
            .map(|(name, _)| name.clone())
            .collect::<HashSet<_>>();
        let mut ddl = Vec::new();
        ddl.extend(
            definitions
                .event_types
                .values()
                .filter(|event_type| live_tables.contains(&event_type.table))
                .map(|event_type| DdlChange::CreateEventType {
                    name: event_type.name.clone(),
                    trigger: event_type.trigger.as_ddl_trigger().to_string(),
                    table: event_type.table.clone(),
                }),
        );
        ddl.extend(
            definitions
                .sinks
                .values()
                .map(|sink| DdlChange::CreateSink {
                    name: sink.name.clone(),
                    sink_type: sink.sink_type.as_ddl_sink_type().to_string(),
                    url: sink.url.clone(),
                }),
        );
        ddl.extend(
            definitions
                .routes
                .values()
                .filter(|route| live_event_types.contains(&route.event_type))
                .map(|route| DdlChange::CreateRoute {
                    name: route.name.clone(),
                    event_type: route.event_type.clone(),
                    sink: route.sink.clone(),
                    table: definitions
                        .event_types
                        .get(&route.event_type)
                        .map(|event_type| event_type.table.clone())
                        .unwrap_or_default(),
                    where_in: route
                        .where_in
                        .as_ref()
                        .map(|where_in| (where_in.column.clone(), where_in.values.clone())),
                }),
        );
        ddl
    }

    pub(super) fn event_bus_table_for_event_type_with_pending(
        &self,
        tx: Option<TxId>,
        event_type: &str,
    ) -> Option<String> {
        self.event_bus_definitions_with_ddl(tx, &[])
            .ok()?
            .event_types
            .get(event_type)
            .map(|event_type| event_type.table.clone())
    }

    pub(super) fn event_bus_table_for_route_with_pending(
        &self,
        tx: Option<TxId>,
        route_name: &str,
    ) -> Option<String> {
        let definitions = self.event_bus_definitions_with_ddl(tx, &[]).ok()?;
        definitions.routes.get(route_name).and_then(|route| {
            definitions
                .event_types
                .get(&route.event_type)
                .map(|event_type| event_type.table.clone())
        })
    }

    fn event_bus_definitions_without_table(
        mut next: EventBusDefinitions,
        table: &str,
    ) -> EventBusDefinitions {
        let removed_event_types = next
            .event_types
            .iter()
            .filter(|(_, event_type)| event_type.table == table)
            .map(|(name, _)| name.clone())
            .collect::<HashSet<_>>();
        if removed_event_types.is_empty() {
            return next;
        }
        next.event_types
            .retain(|name, _| !removed_event_types.contains(name));
        next.routes
            .retain(|_, route| !removed_event_types.contains(&route.event_type));
        next
    }

    pub(crate) fn event_bus_config_values_without_table(
        &self,
        table: &str,
    ) -> Result<Option<EncodedConfigValues>> {
        let current = self.event_bus.definitions.lock().clone();
        let next = Self::event_bus_definitions_without_table(current.clone(), table);
        if current == next {
            return Ok(None);
        }
        Ok(Some(Self::encoded_event_bus_config_values(&next)?))
    }

    pub(crate) fn apply_event_bus_definitions_without_table_to_memory(&self, table: &str) {
        let current = self.event_bus.definitions.lock().clone();
        let next = Self::event_bus_definitions_without_table(current.clone(), table);
        if current != next {
            self.apply_event_bus_definitions_to_memory(next);
        }
    }

    /// Register or replace the post-commit external delivery handler for a
    /// declared sink. The durable queue survives a crash after commit and
    /// retries the same `SinkEvent::action_id`. Receivers should deduplicate
    /// that identity. If an acknowledgement is lost, return
    /// `SinkError::OutcomeUncertain`; the queue stays pending and metrics expose
    /// the uncertainty. The dispatcher cannot promise exactly-once effects at
    /// an arbitrary endpoint.
    ///
    /// Sinks may be registered from owner or scoped handles. The handle's
    /// context and scope-label constraints, plus the resolved registration
    /// principal, are captured at registration time and gate every replayed and
    /// live delivery before the callback is invoked.
    pub fn register_sink<F>(
        &self,
        name: &str,
        principal: Option<Principal>,
        deliver: F,
    ) -> Result<()>
    where
        F: Fn(&SinkEvent) -> std::result::Result<(), SinkError> + Send + Sync + 'static,
    {
        let _operation = self.open_operation()?;
        let principal = match (&self.access.principal, principal) {
            (Some(handle_principal), Some(requested)) if handle_principal != &requested => {
                return Err(Error::Other(
                    "principal-scoped handles cannot register sinks for another principal"
                        .to_string(),
                ));
            }
            (Some(handle_principal), None) => Some(handle_principal.clone()),
            (_, requested) => requested,
        };
        let access = AccessConstraints {
            contexts: self.access.contexts.clone(),
            scope_labels: self.access.scope_labels.clone(),
            principal,
        };
        self.register_sink_with_access(name, access, deliver)
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __debug_register_sink_with_constraints_for_test<F>(
        &self,
        name: &str,
        contexts: Option<BTreeSet<ContextId>>,
        scope_labels: Option<BTreeSet<ScopeLabel>>,
        principal: Option<Principal>,
        deliver: F,
    ) -> Result<()>
    where
        F: Fn(&SinkEvent) -> std::result::Result<(), SinkError> + Send + Sync + 'static,
    {
        let _operation = self.open_operation()?;
        self.register_sink_with_access(
            name,
            AccessConstraints {
                contexts,
                scope_labels,
                principal,
            },
            deliver,
        )
    }

    fn register_sink_with_access<F>(
        &self,
        name: &str,
        access: AccessConstraints,
        deliver: F,
    ) -> Result<()>
    where
        F: Fn(&SinkEvent) -> std::result::Result<(), SinkError> + Send + Sync + 'static,
    {
        if !self.event_bus.definitions.lock().sinks.contains_key(name) {
            return Err(Error::Other(format!("sink not found: {name}")));
        }
        self.event_bus.callbacks.write().insert(
            name.to_string(),
            SinkRegistration {
                access,
                callback: Arc::new(deliver),
            },
        );
        self.ensure_sink_dispatcher_running(name);
        self.event_bus.waiters.notify_all();
        Ok(())
    }

    /// Current delivery counters and durable queue depth for a declared sink.
    pub fn sink_metrics(&self, sink: &str) -> SinkMetrics {
        let _operation = self.assert_open_operation();
        let queued = self
            .event_bus
            .queues
            .lock()
            .get(sink)
            .map(SinkQueue::depth)
            .unwrap_or(0)
            .saturating_add(
                self.event_bus
                    .unloaded_queue_depths
                    .lock()
                    .get(sink)
                    .copied()
                    .unwrap_or(0),
            );
        let metrics = self
            .event_bus
            .metrics
            .lock()
            .get(sink)
            .cloned()
            .unwrap_or_default();
        SinkMetrics {
            delivered: metrics.delivered,
            queued,
            retried: metrics.retried,
            uncertain: metrics.uncertain.saturating_add(metrics.unconfirmed),
            permanent_failures: metrics.permanent_failures,
            examined: metrics.examined,
            window_refusals: metrics.window_refusals,
            window_refused: metrics.window_refused,
        }
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub fn sink_metrics_for_test(&self, sink: &str) -> SinkMetrics {
        self.sink_metrics(sink)
    }

    /// Test-only view of pending identities before dispatcher registration.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn pending_sink_action_ids_for_test(&self, sink: &str) -> Vec<u64> {
        let _operation = self.assert_open_operation();
        let queues = self.event_bus.queues.lock();
        let Some(queue) = queues.get(sink) else {
            return Vec::new();
        };
        if !queue.all_resident()
            && let Some(persistence) = self.persistence.as_ref()
        {
            // Part of a bounded queue is only in the store: list it from there.
            drop(queues);
            let mut ids = Vec::new();
            persistence
                .visit_sink_queue(sink, |id, _| {
                    ids.push(id);
                    Ok(())
                })
                .expect("pending sink ids are readable");
            return ids;
        }
        queue.entries.iter().map(|entry| entry.id).collect()
    }

    /// Window reads the dispatcher made for one sink and the idle pauses it
    /// took before them, for pacing tests.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn sink_window_pacing_for_test(&self, sink: &str) -> (u64, u64) {
        let _operation = self.assert_open_operation();
        self.event_bus
            .metrics
            .lock()
            .get(sink)
            .map_or((0, 0), |state| {
                (state.window_reads, state.window_idle_pauses)
            })
    }

    /// Resident and durable depth of one sink's queue, for window tests.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn sink_queue_residency_for_test(&self, sink: &str) -> (usize, usize, u64) {
        let _operation = self.assert_open_operation();
        self.event_bus
            .queues
            .lock()
            .get(sink)
            .map_or((0, 0, 0), |queue| {
                (queue.entries.len(), queue.resident_bytes(), queue.depth())
            })
    }

    /// Bound a test queue without constructing the production maximum of 100,000 actions.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn set_sink_queue_capacity_for_test(&self, capacity: usize) {
        let _operation = self.assert_open_operation();
        self.event_bus
            .queue_capacity_for_test
            .store(capacity, Ordering::SeqCst);
    }

    /// Point-in-time view of every declared event type, sink (with its
    /// delivery metrics and whether a callback is currently registered), and
    /// route. Backs the CLI's `.events status` introspection and is a real
    /// public accessor, not a test seam.
    pub fn event_bus_status(&self) -> EventBusStatus {
        let _operation = self.assert_open_operation();
        let definitions = self.event_bus.definitions.lock().clone();
        let callbacks_registered = self.event_bus.callbacks.read();
        let event_types = definitions
            .event_types
            .values()
            .map(|event_type| EventTypeStatus {
                name: event_type.name.clone(),
                trigger: event_type.trigger.as_ddl_trigger().to_string(),
                table: event_type.table.clone(),
            })
            .collect();
        let sinks = definitions
            .sinks
            .values()
            .map(|sink| SinkStatus {
                name: sink.name.clone(),
                sink_type: sink.sink_type.as_ddl_sink_type().to_string(),
                callback_registered: callbacks_registered.contains_key(&sink.name),
                metrics: self.sink_metrics(&sink.name),
            })
            .collect();
        let routes = definitions
            .routes
            .values()
            .map(|route| RouteStatus {
                name: route.name.clone(),
                event_type: route.event_type.clone(),
                sink: route.sink.clone(),
            })
            .collect();
        EventBusStatus {
            event_types,
            sinks,
            routes,
        }
    }

    /// Ordered in-memory queue identity/payload view for received-schema race
    /// tests. The accessor is compiled only for tests; production dispatch
    /// continues to own and mutate the queue privately.
    #[cfg(test)]
    pub fn sink_queue_entries_for_test(&self, sink: &str) -> Vec<(u64, SinkEvent)> {
        let _operation = self.assert_open_operation();
        self.pending_sink_entries_for_test(sink)
            .into_iter()
            .map(|entry| (entry.id, entry.event))
            .collect()
    }

    /// Every pending entry of one sink in id order: the resident window's
    /// entries as held, and a bounded queue's entries that are only in the
    /// store, decoded from there.
    #[cfg(any(test, feature = "test-seams"))]
    pub(crate) fn pending_sink_entries_for_test(&self, sink: &str) -> Vec<SinkQueueEntry> {
        let (mut entries, needs_store) = match self.event_bus.queues.lock().get(sink) {
            Some(queue) => (
                queue.entries.iter().cloned().collect::<Vec<_>>(),
                !queue.all_resident(),
            ),
            None => return Vec::new(),
        };
        if needs_store && let Some(persistence) = self.persistence.as_ref() {
            let resident = entries.iter().map(|entry| entry.id).collect::<HashSet<_>>();
            let owner = crate::read_image_memory::ImageMemory::operation_writer(
                crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),
            )
            .expect("test decode owner");
            owner.scope(|| {
                persistence
                    .visit_sink_queue(sink, |id, bytes| {
                        if !resident.contains(&id) {
                            let mut entry =
                                RedbPersistence::decode_config_value::<SinkQueueEntry>(bytes)?;
                            entry.event.action_id = entry.id;
                            entries.push(entry);
                        }
                        Ok(())
                    })
                    .expect("pending sink entries are readable");
            });
            entries.sort_by_key(|entry| entry.id);
        }
        entries
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn runtimes_count_for_test(&self, sink: &str) -> usize {
        let _operation = self.assert_open_operation();
        self.event_bus
            .runtimes
            .lock()
            .get(sink)
            .map(|_| 1)
            .unwrap_or(0)
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn runtime_starts_count_for_test(&self, sink: &str) -> u64 {
        let _operation = self.assert_open_operation();
        self.event_bus
            .runtime_starts
            .lock()
            .get(sink)
            .copied()
            .unwrap_or(0)
    }

    pub(super) fn prepare_sink_events_for_write_set(
        &self,
        ws: &contextdb_tx::WriteSet,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<Vec<PreparedSinkEvent>> {
        self.prepare_sink_events_for_write_set_with_event_bus_ddl(ws, &[], read_memory)
    }

    pub(super) fn prepare_sink_events_for_write_set_with_event_bus_ddl(
        &self,
        ws: &contextdb_tx::WriteSet,
        event_bus_ddl: &[DdlChange],
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<Vec<PreparedSinkEvent>> {
        if ws.relational_inserts.is_empty() && ws.relational_deletes.is_empty() {
            return Ok(Vec::new());
        }
        let Some(lsn) = ws.commit_lsn else {
            return Ok(Vec::new());
        };
        let definitions = self.event_bus_definitions_with_ddl(None, event_bus_ddl)?;
        if definitions.event_types.is_empty() || definitions.routes.is_empty() {
            return Ok(Vec::new());
        }
        // Preparation observes the committed predecessor after all callbacks,
        // independently of a caller's retained transaction base.
        let published = self.tx_mgr.capture_published_read();
        let _current_source = self.enter_writer_read(
            super::writer_read::durable_root(published.handle)?,
            read_memory.clone(),
        );
        let mut static_gate_cache = HashMap::new();
        if ws.relational_deletes.is_empty() {
            let mut out = Vec::with_capacity(ws.relational_inserts.len());
            for (table, row) in &ws.relational_inserts {
                self.append_prepared_sink_events_for_candidate(
                    &mut out,
                    &definitions,
                    RowEventCandidateRef {
                        table,
                        trigger: EventTrigger::Insert,
                        row_id: row.row_id,
                        values: &row.values,
                    },
                    lsn,
                    ws,
                    &mut static_gate_cache,
                    read_memory.clone(),
                )?;
            }
            return Ok(out);
        }
        let (candidates, _candidate_memory) = self.row_event_candidates(ws, read_memory.clone())?;
        let mut out = Vec::new();
        for candidate in candidates {
            self.append_prepared_sink_events_for_candidate(
                &mut out,
                &definitions,
                RowEventCandidateRef {
                    table: &candidate.table,
                    trigger: candidate.trigger,
                    row_id: candidate.row_id,
                    values: &candidate.values,
                },
                lsn,
                ws,
                &mut static_gate_cache,
                read_memory.clone(),
            )?;
        }
        Ok(out)
    }

    /// Build sink work from the already-finalized received WriteSet and the
    /// projected definitions.  Unlike the normal commit path this never
    /// touches `next_queue_id`: abandoning a received stage must leave the
    /// live allocator exactly where it was.
    pub(super) fn prepare_received_sink_events_from_projection(
        &self,
        ws: &contextdb_tx::WriteSet,
        definitions: &EventBusDefinitions,
        projected_rows: &HashMap<String, Vec<VersionedRow>>,
        projected_tables: &HashMap<String, TableMeta>,
    ) -> PreparedReceivedSinkEvents {
        let mut next_queue_id = self.event_bus.next_queue_id.load(Ordering::SeqCst);
        let Some(lsn) = ws.commit_lsn else {
            return PreparedReceivedSinkEvents {
                events: Vec::new(),
                next_queue_id,
            };
        };
        if definitions.event_types.is_empty() || definitions.routes.is_empty() {
            return PreparedReceivedSinkEvents {
                events: Vec::new(),
                next_queue_id,
            };
        }

        let mut deleted = HashMap::<(String, RowId), VersionedRow>::new();
        for (table, row_id, _) in &ws.relational_deletes {
            if let Some(row) = projected_rows
                .get(table)
                .and_then(|rows| rows.iter().rev().find(|row| row.row_id == *row_id))
            {
                deleted.insert((table.clone(), *row_id), row.clone());
            }
        }
        let mut consumed_deletes = HashSet::new();
        let mut events = Vec::new();
        for (table, row) in &ws.relational_inserts {
            let same_row = (table.clone(), row.row_id);
            let same_identity = deleted.iter().find_map(|((old_table, old_id), old)| {
                (old_table == table
                    && old.values.contains_key("id")
                    && old.values.get("id") == row.values.get("id"))
                .then_some((old_table.clone(), *old_id))
            });
            let update_key = deleted
                .contains_key(&same_row)
                .then_some(same_row)
                .or(same_identity);
            let trigger = if let Some(key) = update_key {
                consumed_deletes.insert(key);
                EventTrigger::Update
            } else {
                EventTrigger::Insert
            };
            Self::append_received_sink_events_for_candidate(
                &mut events,
                definitions,
                RowEventCandidateRef {
                    table,
                    trigger,
                    row_id: row.row_id,
                    values: &row.values,
                },
                lsn,
                projected_tables,
                projected_rows,
                &mut next_queue_id,
            );
        }
        for ((table, row_id), row) in deleted {
            if !consumed_deletes.contains(&(table.clone(), row_id)) {
                Self::append_received_sink_events_for_candidate(
                    &mut events,
                    definitions,
                    RowEventCandidateRef {
                        table: &table,
                        trigger: EventTrigger::Delete,
                        row_id,
                        values: &row.values,
                    },
                    lsn,
                    projected_tables,
                    projected_rows,
                    &mut next_queue_id,
                );
            }
        }
        PreparedReceivedSinkEvents {
            events,
            next_queue_id,
        }
    }

    fn append_received_sink_events_for_candidate(
        out: &mut Vec<PreparedSinkEvent>,
        definitions: &EventBusDefinitions,
        candidate: RowEventCandidateRef<'_>,
        lsn: Lsn,
        projected_tables: &HashMap<String, TableMeta>,
        projected_rows: &HashMap<String, Vec<VersionedRow>>,
        next_queue_id: &mut u64,
    ) {
        for event_type in definitions.event_types.values() {
            if event_type.table != candidate.table || event_type.trigger != candidate.trigger {
                continue;
            }
            let severity = candidate
                .values
                .get("severity")
                .and_then(Value::as_text)
                .unwrap_or("")
                .to_string();
            let gate = event_gate_snapshot_from_projected_payload(
                projected_tables.get(candidate.table),
                candidate.values,
                projected_tables,
                projected_rows,
            );
            for route in definitions
                .routes
                .values()
                .filter(|route| route.event_type == event_type.name)
            {
                if !route_matches(route, candidate.values) {
                    continue;
                }
                let action_id = *next_queue_id;
                out.push((
                    route.sink.clone(),
                    SinkQueueEntry {
                        id: action_id,
                        event: SinkEvent {
                            action_id,
                            event_type: event_type.name.clone(),
                            table: candidate.table.to_string(),
                            severity: severity.clone(),
                            row_values: candidate.values.clone(),
                            at_lsn: lsn,
                        },
                        row_id: candidate.row_id,
                        attempts: 0,
                        gate: gate.clone(),
                    },
                ));
                *next_queue_id = next_queue_id.saturating_add(1);
            }
        }
    }

    fn append_prepared_sink_events_for_candidate(
        &self,
        out: &mut Vec<PreparedSinkEvent>,
        definitions: &EventBusDefinitions,
        candidate: RowEventCandidateRef<'_>,
        lsn: Lsn,
        ws: &contextdb_tx::WriteSet,
        static_gate_cache: &mut HashMap<String, EventGateSnapshot>,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<()> {
        for event_type in definitions.event_types.values() {
            if event_type.table != candidate.table || event_type.trigger != candidate.trigger {
                continue;
            }
            let severity = candidate
                .values
                .get("severity")
                .and_then(Value::as_text)
                .unwrap_or("")
                .to_string();
            let gate = self.event_gate_snapshot_cached(
                candidate.table,
                candidate.values,
                ws,
                static_gate_cache,
                read_memory.clone(),
            )?;
            for route in definitions
                .routes
                .values()
                .filter(|route| route.event_type == event_type.name)
            {
                if !route_matches(route, candidate.values) {
                    continue;
                }
                let action_id = self.event_bus.allocate_queue_id();
                let event = SinkEvent {
                    action_id,
                    event_type: event_type.name.clone(),
                    table: candidate.table.to_string(),
                    severity: severity.clone(),
                    row_values: candidate.values.clone(),
                    at_lsn: lsn,
                };
                out.push((
                    route.sink.clone(),
                    SinkQueueEntry {
                        id: action_id,
                        event,
                        row_id: candidate.row_id,
                        attempts: 0,
                        gate: gate.clone(),
                    },
                ));
            }
        }
        Ok(())
    }

    fn event_gate_snapshot_cached(
        &self,
        table: &str,
        values: &HashMap<String, Value>,
        ws: &contextdb_tx::WriteSet,
        static_gate_cache: &mut HashMap<String, EventGateSnapshot>,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<EventGateSnapshot> {
        if let Some(gate) = static_gate_cache.get(table) {
            return Ok(gate.clone());
        }
        let Some(meta) = self.table_meta(table) else {
            return Ok(EventGateSnapshot::default());
        };
        if meta.columns.iter().all(|column| column.acl_ref.is_none()) {
            let gate = event_gate_snapshot_from_meta(&meta);
            static_gate_cache.insert(table.to_string(), gate.clone());
            return Ok(gate);
        }
        self.event_gate_snapshot_with_meta(&meta, values, ws, read_memory)
    }

    fn event_gate_snapshot_with_meta(
        &self,
        meta: &TableMeta,
        values: &HashMap<String, Value>,
        ws: &contextdb_tx::WriteSet,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<EventGateSnapshot> {
        let snapshot = self.snapshot();
        let mut gate = event_gate_snapshot_from_meta(meta);
        let acl = meta
            .columns
            .iter()
            .find(|column| column.acl_ref.is_some())
            .map(|column| -> Result<EventAclGate> {
                let acl_ref = column
                    .acl_ref
                    .as_ref()
                    .expect("selected ACL column")
                    .clone();
                let grant_meta = self.table_meta(&acl_ref.ref_table);
                let grant_gate = grant_meta
                    .as_ref()
                    .map(event_gate_snapshot_from_meta)
                    .unwrap_or_default();
                let acl_id = match values.get(&column.name) {
                    Some(Value::Uuid(acl_id)) => Some(*acl_id),
                    _ => None,
                };
                let grant_rows = match acl_id {
                    Some(acl_id) => self.grant_rows_for_acl(
                        &acl_ref,
                        acl_id,
                        snapshot,
                        ws,
                        read_memory.clone(),
                    )?,
                    None => EventGrantRows::default(),
                };
                Ok(EventAclGate {
                    column: column.name.clone(),
                    ref_table: acl_ref.ref_table,
                    ref_column: acl_ref.ref_column,
                    grant_gate: Box::new(grant_gate),
                    grant_rows,
                })
            })
            .transpose()?;
        gate.acl = acl;
        Ok(gate)
    }

    fn grant_rows_for_acl(
        &self,
        acl_ref: &AclRef,
        acl_id: uuid::Uuid,
        snapshot: SnapshotId,
        ws: &contextdb_tx::WriteSet,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<EventGrantRows> {
        let deleted_row_ids = ws
            .relational_deletes
            .iter()
            .filter_map(|(table, row_id, _)| (table == &acl_ref.ref_table).then_some(*row_id))
            .collect::<HashSet<_>>();

        let mut grants = Vec::new();
        let mut memory =
            crate::memory_accounting::OwnedMemoryReservation::new(self.accountant.clone());
        let visited = self.visit_writer_rows::<Error>(
            &acl_ref.ref_table,
            |row| {
                if row.visible_at(snapshot)
                    && !deleted_row_ids.contains(&row.row_id)
                    && row.values.get(&acl_ref.ref_column) == Some(&Value::Uuid(acl_id))
                {
                    Self::admit_event_grant_copy(&mut memory, grants.is_empty(), row)?;
                    grants.push(row.values.clone());
                }
                Ok(())
            },
            read_memory,
        )?;
        if !visited {
            let tables = self.relational_store.tables.read();
            for row in tables.get(&acl_ref.ref_table).into_iter().flatten() {
                if row.visible_at(snapshot)
                    && !deleted_row_ids.contains(&row.row_id)
                    && row.values.get(&acl_ref.ref_column) == Some(&Value::Uuid(acl_id))
                {
                    Self::admit_event_grant_copy(&mut memory, grants.is_empty(), row)?;
                    grants.push(row.values.clone());
                }
            }
        }

        for (table, row) in &ws.relational_inserts {
            if table == &acl_ref.ref_table
                && row.deleted_tx.is_none()
                && row.values.get(&acl_ref.ref_column) == Some(&Value::Uuid(acl_id))
            {
                Self::admit_event_grant_copy(&mut memory, grants.is_empty(), row)?;
                grants.push(row.values.clone());
            }
        }
        Ok(EventGrantRows::new(grants, Some(memory)))
    }

    fn admit_event_grant_copy(
        memory: &mut crate::memory_accounting::OwnedMemoryReservation,
        first: bool,
        row: &VersionedRow,
    ) -> Result<()> {
        memory.try_grow_for(
            row.estimated_bytes()
                .saturating_add(128)
                .saturating_add(if first {
                    std::mem::size_of::<AdmittedEventGrants>().saturating_add(32)
                } else {
                    0
                }),
            "writer_source",
            "event_grants",
            "Raise MEMORY_LIMIT to prepare these sink grants.",
        )
    }

    pub(super) fn publish_prepared_sink_events_to_memory(&self, events: Vec<PreparedSinkEvent>) {
        if events.is_empty() {
            return;
        }
        let _queue_mutation = self.event_bus.begin_queue_mutation();
        self.publish_prepared_sink_events_to_memory_under_queue_mutation_lease(events);
    }

    /// Append prepared events while the caller retains the lease that spans
    /// their durable queue write and this memory publication.
    pub(super) fn publish_prepared_sink_events_to_memory_under_queue_mutation_lease(
        &self,
        events: Vec<PreparedSinkEvent>,
    ) {
        if events.is_empty() {
            return;
        }
        let mut queues = self.event_bus.queues.lock();
        for (sink, entry) in events {
            let queue = queues
                .entry(sink)
                .or_insert_with(|| self.event_bus.fresh_queue());
            assert!(
                queue.depth() < MAX_SINK_QUEUE_DEPTH as u64,
                "committed sink queue capacity"
            );
            queue.publish_append(entry);
        }
        drop(queues);
        self.event_bus.waiters.notify_all();
    }

    pub(super) fn advance_received_sink_event_id(&self, next_queue_id: u64) {
        self.event_bus
            .next_queue_id
            .fetch_max(next_queue_id, Ordering::SeqCst);
    }

    pub(super) fn stop_event_bus_threads(&self) -> EventBusThreadShutdown {
        self.stop_event_bus_threads_with(false)
    }

    /// Stop dispatchers for a dropped owner. When the drop runs inside a sink
    /// callback, nothing waits on the drop, so that dispatcher still records
    /// the delivery it is making and only then makes the writer quiescent. A
    /// callback-initiated `close` instead leaves its event queued for reopen:
    /// close must freeze writes before it returns.
    pub(super) fn stop_event_bus_threads_on_drop(&self) -> EventBusThreadShutdown {
        self.stop_event_bus_threads_with(true)
    }

    fn stop_event_bus_threads_with(&self, dispatcher_acknowledges: bool) -> EventBusThreadShutdown {
        if !self.resource_owner {
            return EventBusThreadShutdown::AllJoined;
        }
        let current_thread_id = thread::current().id();
        let runtimes = {
            let mut runtimes = self.event_bus.runtimes.lock();
            for runtime in runtimes.values() {
                runtime.shutdown.store(true, Ordering::SeqCst);
            }
            self.event_bus.waiters.notify_all();
            std::mem::take(&mut *runtimes)
        };
        let mut current_dispatcher = None;
        for (sink, mut runtime) in runtimes {
            let is_current_thread = runtime
                .handle
                .as_ref()
                .is_some_and(|handle| handle.thread().id() == current_thread_id);
            if is_current_thread {
                current_dispatcher = runtime.handle.take().map(|handle| (sink, handle));
                continue;
            }
            if let Some(handle) = runtime.handle.take() {
                let _ = handle.join();
            }
        }
        let dispatcher_acknowledges = dispatcher_acknowledges && current_dispatcher.is_some();
        if !dispatcher_acknowledges {
            self.event_bus.close_dispatcher_queue_writes();
        }
        if let Some((sink, handle)) = current_dispatcher {
            drop(handle);
            self.defer_current_dispatcher_resource_cleanup(sink, dispatcher_acknowledges);
            return EventBusThreadShutdown::DeferredResourceCleanup;
        }
        EventBusThreadShutdown::AllJoined
    }

    fn defer_current_dispatcher_resource_cleanup(&self, sink: String, publish_quiescence: bool) {
        // A callback-initiated close leaves its event queued for reopen; a
        // callback-initiated drop records the delivery first. Either way the
        // dispatcher owns the persistence resource until it exits.
        let cleanup = EventBusResourceCleanup {
            sink,
            persistence: self.persistence.clone(),
            open_registry_path: self.open_registry_path.lock().take(),
            close_completion: None,
            publish_quiescence,
        };
        let previous = self
            .event_bus
            .deferred_resource_cleanup
            .lock()
            .replace(cleanup);
        debug_assert!(
            previous.is_none(),
            "event bus resource cleanup was already deferred"
        );
    }

    pub(super) fn attach_deferred_close_completion(&self, succeeded: bool) -> bool {
        let mut cleanup = self.event_bus.deferred_resource_cleanup.lock();
        let Some(cleanup) = cleanup.as_mut() else {
            return false;
        };
        cleanup.close_completion = Some((
            Arc::clone(&self.close_progress),
            Arc::clone(&self.close_changed),
            succeeded,
        ));
        true
    }

    fn ensure_sink_dispatcher_running(&self, sink: &str) {
        let mut runtimes = self.event_bus.runtimes.lock();
        if runtimes.contains_key(sink) {
            return;
        }
        *self
            .event_bus
            .runtime_starts
            .lock()
            .entry(sink.to_string())
            .or_default() += 1;
        let shutdown = Arc::new(AtomicBool::new(false));
        let worker = EventBusWorker {
            sink: sink.to_string(),
            event_bus: self.event_bus.clone(),
            persistence: self.persistence.clone(),
            tx_mgr: self.tx_mgr.clone(),
        };
        let thread_shutdown = shutdown.clone();
        #[cfg(any(test, feature = "test-seams"))]
        crate::read_probe::note_event_delivery_start();
        #[cfg(any(test, feature = "test-seams"))]
        crate::read_probe::note_background_worker_start();
        let handle = thread::spawn(move || worker.dispatch_loop(thread_shutdown));
        runtimes.insert(
            sink.to_string(),
            SinkRuntime {
                shutdown,
                handle: Some(handle),
            },
        );
    }

    pub(super) fn require_admin_event_bus_ddl(&self) -> Result<()> {
        if self.has_access_constraints_for_query() {
            return Err(Error::Other(
                "event bus DDL requires an admin database handle".to_string(),
            ));
        }
        Ok(())
    }

    fn commit_event_bus_definitions_and_log_batch(
        &self,
        definitions: EventBusDefinitions,
        ddl: Vec<DdlChange>,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<()> {
        if *self.event_bus.definitions.lock() == definitions {
            return Ok(());
        }
        self.commit_event_bus_ddl_changes(ddl, read_memory.clone())
    }

    fn commit_event_bus_ddl_changes(
        &self,
        ddl: Vec<DdlChange>,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<()> {
        let mut projected = self.event_bus.definitions.lock().clone();
        for change in &ddl {
            self.apply_event_bus_ddl_to_definitions(&mut projected, change)?;
        }
        if *self.event_bus.definitions.lock() == projected {
            return Ok(());
        }
        self.allocate_ddl_lsn(|lsn| {
            let mut projected = self.event_bus.definitions.lock().clone();
            for change in &ddl {
                self.apply_event_bus_ddl_to_definitions(&mut projected, change)?;
            }
            if *self.event_bus.definitions.lock() == projected {
                return Ok(());
            }
            // `allocate_ddl_lsn` has the transaction commit mutex before
            // entering this closure. Keep this lease across the durable DDL
            // write and the matching in-memory queue-definition publication.
            let _queue_mutation = self.event_bus.begin_queue_mutation();
            if let Some(persistence) = &self.persistence {
                let mut config_values: Vec<(&str, Vec<u8>)> =
                    Self::encoded_event_bus_config_values(&projected)?
                        .into_iter()
                        .map(|(key, value)| (key as &str, value))
                        .collect();
                let provenance_values =
                    self.ddl_generation_sidecar_values(lsn, &ddl, 0, read_memory.clone())?;
                config_values.extend(
                    provenance_values
                        .iter()
                        .map(|(key, value)| (key.as_str(), value.clone())),
                );
                persistence.flush_encoded_config_values_and_append_ddl_log(
                    config_values,
                    lsn,
                    &ddl,
                )?;
            } else {
                self.record_ddl_generation_sidecars(lsn, &ddl, read_memory.clone())?;
            }
            self.apply_event_bus_definitions_to_memory_under_queue_mutation_lease(projected);
            self.ddl_log
                .write()
                .extend(ddl.into_iter().map(|change| (lsn, change)));
            Ok(())
        })
    }

    fn apply_event_bus_definitions_to_memory(&self, definitions: EventBusDefinitions) {
        let _queue_mutation = self.event_bus.begin_queue_mutation();
        self.apply_event_bus_definitions_to_memory_under_queue_mutation_lease(definitions);
    }

    /// Update queue definitions while the caller already holds the spanning
    /// mutation lease.
    fn apply_event_bus_definitions_to_memory_under_queue_mutation_lease(
        &self,
        definitions: EventBusDefinitions,
    ) {
        let sink_names = definitions.sinks.keys().cloned().collect::<Vec<_>>();
        *self.event_bus.definitions.lock() = definitions;
        let mut queues = self.event_bus.queues.lock();
        for sink in sink_names {
            queues
                .entry(sink)
                .or_insert_with(|| self.event_bus.fresh_queue());
        }
    }

    /// Encoded event-bus definitions for checkpoint export of in-memory
    /// sources, serialized exactly as file-backed DDL persistence would have.
    pub(super) fn export_event_bus_config_values(&self) -> Result<EncodedConfigValues> {
        let definitions = self.event_bus.definitions.lock().clone();
        Self::encoded_event_bus_config_values(&definitions)
    }

    fn encoded_event_bus_config_values(
        definitions: &EventBusDefinitions,
    ) -> Result<EncodedConfigValues> {
        let event_types = definitions
            .event_types
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let sinks = definitions.sinks.values().cloned().collect::<Vec<_>>();
        let routes = definitions.routes.values().cloned().collect::<Vec<_>>();
        Ok(vec![
            (
                EVENT_TYPES_CONFIG_KEY,
                RedbPersistence::encode_config_value(&event_types)?,
            ),
            (
                SINKS_CONFIG_KEY,
                RedbPersistence::encode_config_value(&sinks)?,
            ),
            (
                ROUTES_CONFIG_KEY,
                RedbPersistence::encode_config_value(&routes)?,
            ),
        ])
    }

    fn row_event_candidates(
        &self,
        ws: &contextdb_tx::WriteSet,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<(
        Vec<RowEventCandidate>,
        crate::memory_accounting::OwnedMemoryReservation,
    )> {
        let snapshot = self.snapshot();
        let delete_capacity = ws.relational_deletes.len();
        let candidate_capacity = ws.relational_inserts.len().saturating_add(delete_capacity);
        let mut memory = crate::memory_accounting::OwnedMemoryReservation::try_new_for(
            self.accountant.clone(),
            held::map::<(String, RowId), VersionedRow>(delete_capacity)
                .saturating_add(held::map::<(String, RowId), ()>(delete_capacity))
                .saturating_add(
                    candidate_capacity.saturating_mul(std::mem::size_of::<RowEventCandidate>()),
                )
                .saturating_add(held::ALLOCATION_BYTES),
            "writer_source",
            "event_preimages",
            "Raise MEMORY_LIMIT to prepare these sink events.",
        )?;
        let mut delete_preimages: HashMap<(String, RowId), VersionedRow> =
            HashMap::with_capacity(delete_capacity);
        let root = self.writer_read_root()?;
        let source = root
            .map(|root| {
                crate::persistence::writer_adapters::WriterRelationalSource::new_for_read(
                    root,
                    read_memory.clone(),
                )
            })
            .transpose()?;
        for (table, row_id, _) in &ws.relational_deletes {
            let row = if let Some(source) = &source {
                if let Some(row) =
                    contextdb_relational::source::visible_row(source, table, *row_id, snapshot)?
                {
                    memory.try_grow_for(
                        row.value()
                            .estimated_bytes()
                            .saturating_add(table.len())
                            .saturating_add(64),
                        "writer_source",
                        "event_preimages",
                        "Raise MEMORY_LIMIT to prepare these sink events.",
                    )?;
                    Some(row.value().clone())
                } else {
                    None
                }
            } else {
                self.relational_store.row_by_id(table, *row_id, snapshot)
            };
            if let Some(row) = row {
                delete_preimages.insert((table.clone(), *row_id), row);
            }
        }

        let mut consumed_deletes = HashSet::with_capacity(delete_capacity);
        let mut candidates = Vec::with_capacity(candidate_capacity);
        for (table, row) in &ws.relational_inserts {
            memory.try_grow_for(
                row.estimated_bytes()
                    .saturating_add(table.len().saturating_mul(4))
                    .saturating_add(128),
                "writer_source",
                "event_candidates",
                "Raise MEMORY_LIMIT to prepare these sink events.",
            )?;
            let same_row_key = (table.clone(), row.row_id);
            let same_id_key =
                delete_preimages
                    .iter()
                    .find_map(|((delete_table, delete_row_id), old)| {
                        (delete_table == table
                            && old.values.contains_key("id")
                            && old.values.get("id") == row.values.get("id"))
                        .then_some((delete_table.clone(), *delete_row_id))
                    });
            let update_key = if delete_preimages.contains_key(&same_row_key) {
                Some(same_row_key)
            } else {
                same_id_key
            };
            let trigger = if let Some(key) = update_key {
                consumed_deletes.insert(key);
                EventTrigger::Update
            } else {
                EventTrigger::Insert
            };
            candidates.push(RowEventCandidate {
                trigger,
                table: table.clone(),
                row_id: row.row_id,
                values: row.values.clone(),
            });
        }

        for ((table, row_id), row) in delete_preimages {
            if consumed_deletes.contains(&(table.clone(), row_id)) {
                continue;
            }
            candidates.push(RowEventCandidate {
                trigger: EventTrigger::Delete,
                table,
                row_id,
                values: row.values,
            });
        }
        Ok((candidates, memory))
    }
}

/// One planned read of a bounded queue's next window.
struct SinkRefillPlan {
    token: u64,
    generation: u64,
    /// Read ids after this one; `None` starts a new sweep at the lowest id.
    after: Option<u64>,
    /// The sweep's fixed upper id; `None` for a new sweep, which takes the
    /// newest id at the time of the read.
    upper: Option<u64>,
}

struct EventBusWorker {
    sink: String,
    event_bus: Arc<EventBusState>,
    persistence: Option<Arc<RedbPersistence>>,
    tx_mgr: DatabaseTransactionHost,
}

impl EventBusWorker {
    fn dispatch_loop(self, shutdown: Arc<AtomicBool>) {
        // Whether the last drive installed a window. A window that then shows
        // this registration nothing waits the idle pause before the next read,
        // so a queue it cannot see is not re-read back to back.
        let mut just_refilled = false;
        'dispatch: while !shutdown.load(Ordering::SeqCst) {
            let Some(registration) = self.event_bus.callbacks.read().get(&self.sink).cloned()
            else {
                self.wait(&shutdown, Duration::from_millis(100));
                continue;
            };
            // Checkout and checked-out registration share this state guard, so
            // a purge cannot pause between cloning an entry and recording it
            // as in-flight. The callback itself runs without the guard.
            let mut queue_mutation = self.event_bus.queue_checkout_guard();
            let mut refill = None;
            let mut checkout_charge = None;
            let entries = {
                let mut queues = self.event_bus.queues.lock();
                let Some(queue) = queues.get_mut(&self.sink) else {
                    drop(queues);
                    drop(queue_mutation);
                    self.wait(&shutdown, Duration::from_millis(250));
                    continue;
                };
                if queue.bounded {
                    // A bounded window: take the visible entries in id order,
                    // passing over (and counting) the ones this registration
                    // cannot see; they stay queued and the next sweep reaches
                    // them again.
                    let mut skipped = 0u64;
                    let mut chosen = Vec::new();
                    let mut chosen_bytes = 0usize;
                    for (position, entry) in queue.entries.iter().enumerate() {
                        if chosen.len() == SINK_DISPATCH_BATCH {
                            break;
                        }
                        if event_visible_for_access(&registration.access, entry) {
                            chosen.push(position);
                            chosen_bytes = chosen_bytes.saturating_add(queue.credits[position]);
                        } else {
                            skipped += 1;
                        }
                    }
                    // The checkout copies are funded on the window owner
                    // before they are made, and returned after the batch.
                    let mut picked = Vec::new();
                    if !chosen.is_empty()
                        && let Some(owner) = &queue.owner
                    {
                        match SinkWindowCopyCharge::reserve(
                            owner,
                            chosen_bytes.saturating_add(
                                chosen
                                    .len()
                                    .saturating_mul(std::mem::size_of::<SinkQueueEntry>()),
                            ),
                        ) {
                            Ok(charge) => {
                                self.event_bus.clear_window_refusal(&self.sink);
                                checkout_charge = Some(charge);
                                picked = chosen
                                    .iter()
                                    .map(|position| queue.entries[*position].clone())
                                    .collect();
                            }
                            Err(error) => {
                                drop(queues);
                                drop(queue_mutation);
                                self.event_bus.record_window_refusal(&self.sink, &error);
                                self.wait(&shutdown, Duration::from_millis(250));
                                continue;
                            }
                        }
                    }
                    if picked.is_empty()
                        && !queue.all_resident()
                        && queue.pending_refill.is_none()
                        && !queue_mutation
                            .checked_out
                            .iter()
                            .any(|(sink, _)| sink == &self.sink)
                    {
                        refill = Some(self.plan_refill(queue));
                    }
                    drop(queues);
                    self.record_examined(skipped);
                    picked
                } else {
                    let Some(front) = queue.entries.front().cloned() else {
                        drop(queues);
                        drop(queue_mutation);
                        self.wait(&shutdown, Duration::from_millis(250));
                        continue;
                    };
                    if !event_visible_for_access(&registration.access, &front) {
                        if let Some(entry) = queue.entries.pop_front() {
                            queue.entries.push_back(entry);
                        }
                        drop(queues);
                        self.record_examined(1);
                        self.event_bus.waiters.notify_all();
                        drop(queue_mutation);
                        self.wait(&shutdown, Duration::from_millis(10));
                        continue;
                    }
                    queue
                        .entries
                        .iter()
                        .take(SINK_DISPATCH_BATCH)
                        .take_while(|entry| event_visible_for_access(&registration.access, entry))
                        .cloned()
                        .collect::<Vec<_>>()
                }
            };
            if let Some(plan) = refill {
                drop(queue_mutation);
                if just_refilled {
                    // The window the last read installed showed this
                    // registration nothing: pause before reading the next.
                    self.note_window_pacing(0, 1);
                    self.wait(&shutdown, Duration::from_millis(250));
                }
                self.note_window_pacing(1, 0);
                just_refilled = self.refill_window(plan);
                if !just_refilled {
                    self.wait(&shutdown, Duration::from_millis(250));
                }
                continue;
            }
            just_refilled = false;
            let _checkout_charge = checkout_charge;
            for entry in &entries {
                queue_mutation
                    .checked_out
                    .insert((self.sink.clone(), entry.id));
            }
            drop(queue_mutation);

            if entries.is_empty() {
                self.wait(&shutdown, Duration::from_millis(250));
                continue;
            }

            let mut acked = Vec::with_capacity(entries.len());
            let mut delivered_count = 0u64;
            let mut permanent_failure_count = 0u64;
            let mut examined_count = 0u64;
            for (entry_position, entry) in entries.iter().enumerate() {
                if shutdown.load(Ordering::SeqCst) {
                    for pending in entries.iter().skip(entry_position) {
                        self.event_bus
                            .finish_checked_out_entry(&self.sink, pending.id);
                    }
                    break;
                }
                if entry.attempts & UNCONFIRMED_ATTEMPT != 0 {
                    if let Err(error) = self.transition_attempt(entry.id, false, true) {
                        tracing::warn!(sink = %self.sink, action_id = entry.id, %error,
                            "cannot durably reconcile an unconfirmed sink attempt");
                        for pending in entries.iter().skip(entry_position) {
                            self.event_bus
                                .finish_checked_out_entry(&self.sink, pending.id);
                        }
                        self.wait(&shutdown, Duration::from_millis(50));
                        continue 'dispatch;
                    }
                    self.event_bus
                        .metrics
                        .lock()
                        .entry(self.sink.clone())
                        .or_default()
                        .uncertain += 1;
                }
                let attempts = match self.transition_attempt(entry.id, true, false) {
                    Ok(attempts) => attempts,
                    Err(error) => {
                        tracing::warn!(sink = %self.sink, action_id = entry.id, %error,
                            "cannot durably record sink attempt before delivery");
                        for pending in entries.iter().skip(entry_position) {
                            self.event_bus
                                .finish_checked_out_entry(&self.sink, pending.id);
                        }
                        self.wait(&shutdown, Duration::from_millis(50));
                        continue 'dispatch;
                    }
                };
                examined_count = examined_count.saturating_add(1);
                self.add_in_flight(1);
                let callback = registration.callback.clone();
                // A window entry already carries its action id; only an
                // unbounded queue's entry may need a copy that names it.
                let named;
                let event = if entry.event.action_id == 0 {
                    let mut copy = entry.event.clone();
                    copy.action_id = entry.id;
                    named = copy;
                    &named
                } else {
                    &entry.event
                };
                let result = {
                    let _callback = EventBusCallbackGuard::enter(&self.event_bus);
                    catch_unwind(AssertUnwindSafe(|| callback(event)))
                };
                self.add_in_flight(-1);
                let result = match result {
                    Ok(outcome) => outcome,
                    Err(payload) => {
                        let panic_message = panic_payload_to_string(payload);
                        tracing::warn!(
                            sink = %self.sink,
                            action_id = event.action_id,
                            panic_message = %panic_message,
                            "sink callback panicked after delivery began"
                        );
                        Err(SinkError::OutcomeUncertain(panic_message))
                    }
                };
                let uncertain_reason = match &result {
                    Err(SinkError::OutcomeUncertain(reason)) => Some(reason.as_str()),
                    _ => None,
                };
                if let Some(reason) = uncertain_reason {
                    tracing::warn!(
                        sink = %self.sink,
                        action_id = event.action_id,
                        reason,
                        "sink delivery outcome uncertain; retrying with the same action identity"
                    );
                }

                match result {
                    Ok(()) => {
                        delivered_count = delivered_count.saturating_add(1);
                        acked.push(entry.id);
                    }
                    Err(SinkError::Transient(_) | SinkError::OutcomeUncertain(_)) => {
                        let _ = self.ack_front_entries(&acked);
                        let uncertain = uncertain_reason.is_some();
                        match self.transition_attempt(entry.id, false, uncertain) {
                            Ok(_) if uncertain => {
                                self.event_bus
                                    .metrics
                                    .lock()
                                    .entry(self.sink.clone())
                                    .or_default()
                                    .uncertain += 1;
                            }
                            Ok(_) => {}
                            Err(error) => {
                                tracing::warn!(sink = %self.sink, action_id = entry.id, %error,
                                    "sink attempt remains durably unconfirmed for recovery");
                            }
                        }
                        // Same combined flush as the post-batch path: examined
                        // must ride along or transient retries undercount it
                        // (the retried entry itself counts again on re-examine
                        // — the documented inflation full-sweep callers avoid).
                        self.record_batch_outcome(
                            delivered_count,
                            permanent_failure_count,
                            examined_count,
                            1,
                            0,
                        );
                        self.event_bus
                            .finish_checked_out_entry(&self.sink, entry.id);
                        let backoff = retry_backoff(attempts);
                        for pending in entries.iter().skip(entry_position + 1) {
                            self.event_bus
                                .finish_checked_out_entry(&self.sink, pending.id);
                        }
                        self.wait(&shutdown, backoff);
                        continue 'dispatch;
                    }
                    Err(SinkError::Permanent(_)) => {
                        permanent_failure_count = permanent_failure_count.saturating_add(1);
                        acked.push(entry.id);
                    }
                }
            }
            let _ = self.ack_front_entries(&acked);
            self.record_batch_outcome(
                delivered_count,
                permanent_failure_count,
                examined_count,
                0,
                0,
            );
        }
        self.finish_deferred_resource_cleanup();
    }

    fn plan_refill(&self, queue: &mut SinkQueue) -> SinkRefillPlan {
        queue.plan_refill(
            self.event_bus
                .next_refill_token
                .fetch_add(1, Ordering::SeqCst),
        )
    }

    /// Read the planned window outside every lock, then install it under the
    /// dispatcher lease if no removal was published and no other refill took
    /// its place meanwhile. Returns whether a window was installed.
    fn refill_window(&self, plan: SinkRefillPlan) -> bool {
        let clear = |event_bus: &EventBusState| {
            if let Some(queue) = event_bus.queues.lock().get_mut(&self.sink)
                && queue.pending_refill == Some(plan.token)
            {
                queue.pending_refill = None;
            }
        };
        let accountant = self
            .event_bus
            .window_accountant
            .lock()
            .as_ref()
            .map(crate::memory_accounting::AccountantHandle::clone);
        let (Some(persistence), Some(accountant)) = (self.persistence.as_ref(), accountant) else {
            clear(&self.event_bus);
            return false;
        };
        let read = match persistence.load_sink_queue_window::<SinkQueueEntry>(
            &self.sink,
            plan.after,
            plan.upper,
            SINK_WINDOW_ENTRIES,
            SINK_WINDOW_BYTES,
            accountant,
        ) {
            Ok(read) => read,
            Err(error) => {
                tracing::warn!(sink = %self.sink, %error, "cannot read the next window of a sink queue");
                clear(&self.event_bus);
                return false;
            }
        };
        if let Some(error) = &read.refusal {
            self.event_bus.record_window_refusal(&self.sink, error);
            clear(&self.event_bus);
            return false;
        }
        let Ok(_lease) = self.event_bus.begin_dispatcher_queue_mutation() else {
            clear(&self.event_bus);
            return false;
        };
        let mut queues = self.event_bus.queues.lock();
        let Some(queue) = queues.get_mut(&self.sink) else {
            return false;
        };
        let owner = read.owner;
        let mut entries = VecDeque::from(read.entries);
        for entry in &mut entries {
            entry.event.action_id = entry.id;
        }
        let window = SinkWindow {
            entries,
            credits: VecDeque::from(read.credits),
            owner,
        };
        let installed = queue.install_refill(&plan, window, read.newest_id, read.exhausted);
        drop(queues);
        if installed {
            self.event_bus.clear_window_refusal(&self.sink);
            self.event_bus.waiters.notify_all();
        }
        installed
    }

    fn note_window_pacing(&self, reads: u64, pauses: u64) {
        let mut metrics = self.event_bus.metrics.lock();
        let state = metrics.entry(self.sink.clone()).or_default();
        state.window_reads = state.window_reads.saturating_add(reads);
        state.window_idle_pauses = state.window_idle_pauses.saturating_add(pauses);
    }

    fn finish_deferred_resource_cleanup(&self) {
        let cleanup = {
            let mut deferred = self.event_bus.deferred_resource_cleanup.lock();
            if deferred
                .as_ref()
                .is_some_and(|cleanup| cleanup.sink == self.sink)
            {
                deferred.take()
            } else {
                None
            }
        };
        if let Some(cleanup) = cleanup {
            cleanup.close();
        }
    }

    fn wait(&self, shutdown: &AtomicBool, duration: Duration) {
        if shutdown.load(Ordering::SeqCst) {
            return;
        }
        let mut guard = self.event_bus.wait_lock.lock();
        self.event_bus.waiters.wait_for(&mut guard, duration);
    }

    fn add_in_flight(&self, delta: i64) {
        let mut metrics = self.event_bus.metrics.lock();
        let entry = metrics.entry(self.sink.clone()).or_default();
        if delta.is_positive() {
            entry.in_flight = entry.in_flight.saturating_add(delta as u64);
        } else {
            entry.in_flight = entry.in_flight.saturating_sub(delta.unsigned_abs());
        }
    }

    /// Bump the always-on dispatch-examination counter (see
    /// `SinkMetrics::examined`): one per denied-and-rotated entry, and once per
    /// entry in an admitted delivery batch. Rides the existing per-sink metrics
    /// mutex; no separate lock or atomic. For admitted batches the increment
    /// happens POST-batch (see `record_batch_outcome`), so `examined` reaching
    /// a target implies those entries' outcomes (`delivered` /
    /// `permanent_failures`) are already recorded — a test waiting on a full
    /// sweep can never observe examined-but-unsettled state.
    fn record_examined(&self, count: u64) {
        if count == 0 {
            return;
        }
        self.event_bus
            .metrics
            .lock()
            .entry(self.sink.clone())
            .or_default()
            .examined += count;
    }

    /// One post-batch metrics write: delivered + permanent failures + examined
    /// under a SINGLE lock acquisition, so the always-on examined counter adds
    /// zero extra lock traffic on the delivery path (the denied-rotation
    /// increment keeps its own acquisition, self-throttled by the dispatcher's
    /// 10 ms rotation wait).
    fn record_batch_outcome(
        &self,
        delivered: u64,
        permanent_failures: u64,
        examined: u64,
        retried: u64,
        uncertain: u64,
    ) {
        if delivered == 0
            && permanent_failures == 0
            && examined == 0
            && retried == 0
            && uncertain == 0
        {
            return;
        }
        let mut metrics = self.event_bus.metrics.lock();
        let state = metrics.entry(self.sink.clone()).or_default();
        state.delivered += delivered;
        state.permanent_failures += permanent_failures;
        state.examined += examined;
        state.retried += retried;
        state.uncertain += uncertain;
    }

    fn transition_attempt(&self, entry_id: u64, start: bool, uncertain: bool) -> Result<u32> {
        self.tx_mgr.with_maintenance_publication(|publication| {
            let _queue_mutation = self.event_bus.begin_dispatcher_queue_mutation()?;
            let mut queues = self.event_bus.queues.lock();
            let entry = queues
                .get_mut(&self.sink)
                .and_then(|queue| queue.entries.iter_mut().find(|entry| entry.id == entry_id))
                .ok_or_else(|| {
                    Error::Other(format!("pending sink action disappeared: {entry_id}"))
                })?;
            let attempts = entry.attempts & ATTEMPT_COUNT_MASK;
            let rewritten = if start {
                if entry.attempts & UNCONFIRMED_ATTEMPT != 0 {
                    return Err(Error::Other(format!(
                        "sink action has an unresolved attempt: {entry_id}"
                    )));
                }
                attempts.saturating_add(1).min(ATTEMPT_COUNT_MASK) | UNCONFIRMED_ATTEMPT
            } else {
                attempts
            };
            let was_unconfirmed = entry.attempts & UNCONFIRMED_ATTEMPT != 0;
            let unconfirmed_delta = if start {
                1
            } else if was_unconfirmed {
                -1
            } else {
                0
            };
            if self.persistence.is_some() {
                publication.reserve()?;
            }
            // The durable rewrite encodes the resident entry itself with its
            // new attempt count: no copy of the payload is made.
            with_rewritten_attempts(entry, rewritten, |updated| {
                persist_sink_queue_update(
                    &self.persistence,
                    &self.sink,
                    None,
                    Some(updated),
                    uncertain,
                    unconfirmed_delta,
                )
            })?;
            if self.persistence.is_some() {
                publication.complete();
            }
            if start {
                self.event_bus
                    .metrics
                    .lock()
                    .entry(self.sink.clone())
                    .or_default()
                    .unconfirmed += 1;
            } else if was_unconfirmed {
                let mut metrics = self.event_bus.metrics.lock();
                let state = metrics.entry(self.sink.clone()).or_default();
                state.unconfirmed = state.unconfirmed.saturating_sub(1);
            }
            Ok(attempts.max(1))
        })
    }

    fn ack_front_entries(&self, entry_ids: &[u64]) -> Result<()> {
        if entry_ids.is_empty() {
            return Ok(());
        }
        let result = self.tx_mgr.with_maintenance_publication(|publication| {
            let _queue_mutation = self.event_bus.begin_dispatcher_queue_mutation()?;
            let mut queues = self.event_bus.queues.lock();
            if let Some(queue) = queues.get_mut(&self.sink) {
                // Acknowledge by id: a checkout skips entries the registration
                // cannot see, so what it delivered need not be a front prefix.
                let resident = queue
                    .entries
                    .iter()
                    .map(|entry| entry.id)
                    .collect::<HashSet<_>>();
                let removable = entry_ids
                    .iter()
                    .copied()
                    .filter(|id| resident.contains(id))
                    .collect::<Vec<_>>();
                if !removable.is_empty() {
                    let removed = removable.iter().copied().collect::<HashSet<_>>();
                    let unconfirmed = queue
                        .entries
                        .iter()
                        .filter(|entry| {
                            removed.contains(&entry.id) && entry.attempts & UNCONFIRMED_ATTEMPT != 0
                        })
                        .count() as u64;
                    if self.persistence.is_some() {
                        publication.reserve()?;
                    }
                    persist_sink_queue_removals(
                        &self.persistence,
                        &self.sink,
                        &removable,
                        unconfirmed,
                    )?;
                    if self.persistence.is_some() {
                        publication.complete();
                    }
                    queue.publish_removals(&removed, removable.len() as u64);
                    let mut metrics = self.event_bus.metrics.lock();
                    let state = metrics.entry(self.sink.clone()).or_default();
                    state.unconfirmed = state.unconfirmed.saturating_sub(unconfirmed);
                }
            }
            Ok(())
        });
        for entry_id in entry_ids {
            self.event_bus
                .finish_checked_out_entry(&self.sink, *entry_id);
        }
        self.event_bus.waiters.notify_all();
        result
    }
}

impl EventTrigger {
    fn from_ddl_trigger(trigger: &str) -> Result<Self> {
        match trigger.to_ascii_uppercase().as_str() {
            "INSERT" => Ok(Self::Insert),
            "UPDATE" => Ok(Self::Update),
            "DELETE" => Ok(Self::Delete),
            _ => Err(Error::Other(format!(
                "invalid event type trigger: {trigger}"
            ))),
        }
    }

    fn as_ddl_trigger(self) -> &'static str {
        match self {
            Self::Insert => "INSERT",
            Self::Update => "UPDATE",
            Self::Delete => "DELETE",
        }
    }
}

impl PersistedSinkType {
    fn from_ddl_sink_type(sink_type: &str) -> Result<Self> {
        match sink_type.to_ascii_uppercase().as_str() {
            "WEBHOOK" => Ok(Self::Webhook),
            "CALLBACK" => Ok(Self::Callback),
            _ => Err(Error::Other(format!("invalid sink type: {sink_type}"))),
        }
    }

    fn as_ddl_sink_type(self) -> &'static str {
        match self {
            Self::Webhook => "WEBHOOK",
            Self::Callback => "CALLBACK",
        }
    }
}

fn route_matches(route: &RouteDef, values: &HashMap<String, Value>) -> bool {
    let Some(where_in) = route.where_in.as_ref() else {
        return true;
    };
    match values.get(&where_in.column) {
        Some(Value::Text(value)) => where_in.values.iter().any(|allowed| allowed == value),
        _ => false,
    }
}

fn sanitize_event_bus_definitions(
    mut definitions: EventBusDefinitions,
    live_tables: &HashSet<String>,
) -> EventBusDefinitions {
    definitions
        .event_types
        .retain(|_, event_type| live_tables.contains(&event_type.table));
    definitions.routes.retain(|_, route| {
        definitions.event_types.contains_key(&route.event_type)
            && definitions.sinks.contains_key(&route.sink)
    });
    definitions
}

fn event_visible_for_access(access: &AccessConstraints, entry: &SinkQueueEntry) -> bool {
    gate_allows_payload(access, &entry.gate, &entry.event.row_values)
}

fn event_gate_snapshot_from_meta(meta: &TableMeta) -> EventGateSnapshot {
    EventGateSnapshot {
        context_column: meta
            .columns
            .iter()
            .find(|column| column.context_id)
            .map(|column| column.name.clone()),
        scope_read: meta
            .columns
            .iter()
            .find_map(|column| match column.scope_label.as_ref() {
                Some(ScopeLabelKind::Split { read_labels, .. }) => Some(EventScopeReadGate {
                    column: column.name.clone(),
                    read_labels: read_labels.clone(),
                }),
                _ => None,
            }),
        acl: None,
    }
}

/// The received-schema stage owns a complete post-apply row projection, not
/// the live store that remains unchanged until durability.  Build the same
/// route-time gate used by ordinary commits from that projection so ACL grant
/// inserts, deletes, and updates in the received unit are captured inside the
/// queued event itself.
fn event_gate_snapshot_from_projected_payload(
    meta: Option<&TableMeta>,
    values: &HashMap<String, Value>,
    projected_tables: &HashMap<String, TableMeta>,
    projected_rows: &HashMap<String, Vec<VersionedRow>>,
) -> EventGateSnapshot {
    let Some(meta) = meta else {
        return EventGateSnapshot::default();
    };
    let mut gate = event_gate_snapshot_from_meta(meta);
    let acl = meta
        .columns
        .iter()
        .find(|column| column.acl_ref.is_some())
        .and_then(|column| {
            let acl_ref = column.acl_ref.as_ref()?.clone();
            let acl_id = match values.get(&column.name) {
                Some(Value::Uuid(acl_id)) => Some(*acl_id),
                _ => None,
            };
            let grant_gate = projected_tables
                .get(&acl_ref.ref_table)
                .map(event_gate_snapshot_from_meta)
                .unwrap_or_default();
            let grant_rows = projected_rows
                .get(&acl_ref.ref_table)
                .into_iter()
                .flatten()
                .filter(|row| {
                    row.deleted_tx.is_none()
                        && acl_id.is_some_and(|acl_id| {
                            row.values.get(&acl_ref.ref_column) == Some(&Value::Uuid(acl_id))
                        })
                })
                .map(|row| row.values.clone())
                .collect();
            Some(EventAclGate {
                column: column.name.clone(),
                ref_table: acl_ref.ref_table,
                ref_column: acl_ref.ref_column,
                grant_gate: Box::new(grant_gate),
                grant_rows: EventGrantRows::Legacy(grant_rows),
            })
        });
    gate.acl = acl;
    gate
}

fn gate_allows_payload(
    access: &AccessConstraints,
    gate: &EventGateSnapshot,
    values: &HashMap<String, Value>,
) -> bool {
    if !context_scope_allows_payload(access, gate, values) {
        return false;
    }

    let Some(acl) = gate.acl.as_ref() else {
        return true;
    };
    let Some(principal) = access.principal.as_ref() else {
        return access.contexts.is_none() && access.scope_labels.is_none();
    };
    if matches!(principal, Principal::System) {
        return false;
    }
    let Some(Value::Uuid(acl_id)) = values.get(&acl.column) else {
        return false;
    };
    acl.grant_rows.iter().any(|grant| {
        context_scope_allows_payload(access, &acl.grant_gate, grant)
            && principal_matches_grant(principal, grant)
            && grant.get(&acl.ref_column) == Some(&Value::Uuid(*acl_id))
    })
}

fn context_scope_allows_payload(
    access: &AccessConstraints,
    gate: &EventGateSnapshot,
    values: &HashMap<String, Value>,
) -> bool {
    if let (Some(contexts), Some(column)) = (access.contexts.as_ref(), gate.context_column.as_ref())
    {
        match values.get(column) {
            Some(Value::Uuid(context)) if contexts.contains(&ContextId::new(*context)) => {}
            _ => return false,
        }
    }

    if let (Some(labels), Some(scope)) = (access.scope_labels.as_ref(), gate.scope_read.as_ref()) {
        match values.get(&scope.column) {
            Some(Value::Text(label))
                if scope.read_labels.iter().any(|read| read == label)
                    && labels.contains(&ScopeLabel::new(label.clone())) => {}
            _ => return false,
        }
    }

    true
}

fn principal_matches_grant(principal: &Principal, grant: &HashMap<String, Value>) -> bool {
    let (kind, principal_id) = match principal {
        Principal::System => return false,
        Principal::Agent(id) => ("Agent", id.as_str()),
        Principal::Human(id) => ("Human", id.as_str()),
    };
    grant.get("principal_kind") == Some(&Value::Text(kind.to_string()))
        && grant.get("principal_id") == Some(&Value::Text(principal_id.to_string()))
}

fn persist_sink_queue_update(
    persistence: &Option<Arc<RedbPersistence>>,
    sink: &str,
    remove_id: Option<u64>,
    put: Option<&SinkQueueEntry>,
    uncertain: bool,
    unconfirmed_delta: i8,
) -> Result<()> {
    let Some(persistence) = persistence.as_ref() else {
        return Ok(());
    };
    persistence.update_sink_queue_entry(
        sink,
        remove_id,
        put.map(|entry| (entry.id, entry)),
        uncertain,
        unconfirmed_delta,
    )
}

fn persist_sink_queue_removals(
    persistence: &Option<Arc<RedbPersistence>>,
    sink: &str,
    remove_ids: &[u64],
    unconfirmed: u64,
) -> Result<()> {
    let Some(persistence) = persistence.as_ref() else {
        return Ok(());
    };
    persistence.remove_sink_queue_entries(sink, remove_ids, unconfirmed)
}

fn retry_backoff(attempts: u32) -> Duration {
    let shift = attempts.saturating_sub(1).min(6);
    Duration::from_millis(50u64.saturating_mul(1u64 << shift))
}

fn panic_payload_to_string(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "non-string panic payload".to_string()
    }
}

#[cfg(test)]
mod saved_sink_entry_encoding_tests {
    use super::*;

    #[test]
    fn saved_queue_bytes_keep_the_legacy_event_shape_and_recover_the_action_id() {
        #[derive(Serialize)]
        struct OldEvent<'a> {
            event_type: &'a str,
            table: &'a str,
            row_values: &'a HashMap<String, Value>,
            severity: &'a str,
            at_lsn: Lsn,
        }
        #[derive(Serialize)]
        struct OldEntry<'a> {
            id: u64,
            event: OldEvent<'a>,
            row_id: RowId,
            attempts: u32,
            gate: &'a EventGateSnapshot,
        }
        let entry = SinkQueueEntry {
            id: 17,
            event: SinkEvent {
                action_id: 17,
                event_type: "notice".into(),
                table: "notifications".into(),
                row_values: HashMap::new(),
                severity: "warning".into(),
                at_lsn: Lsn(9),
            },
            row_id: RowId(3),
            attempts: 1,
            gate: EventGateSnapshot::default(),
        };
        let old = OldEntry {
            id: entry.id,
            event: OldEvent {
                event_type: &entry.event.event_type,
                table: &entry.event.table,
                row_values: &entry.event.row_values,
                severity: &entry.event.severity,
                at_lsn: entry.event.at_lsn,
            },
            row_id: entry.row_id,
            attempts: entry.attempts,
            gate: &entry.gate,
        };
        let old_bytes = bincode::serde::encode_to_vec(&old, bincode::config::standard()).unwrap();
        assert_eq!(
            bincode::serde::encode_to_vec(&entry, bincode::config::standard()).unwrap(),
            old_bytes
        );
        let (decoded, _): (SinkQueueEntry, usize) =
            bincode::serde::decode_from_slice(&old_bytes, bincode::config::standard()).unwrap();
        assert_eq!(decoded.event.action_id, 17);
    }
}

#[cfg(test)]
mod quiescent_queue_tests {
    use super::*;

    #[test]
    fn dispatcher_queue_write_after_quiescence_has_a_typed_refusal() {
        let database = Database::open_memory();
        database.event_bus.close_dispatcher_queue_writes();
        assert!(matches!(
            database.event_bus.begin_dispatcher_queue_mutation(),
            Err(Error::EventBusQueueQuiescent)
        ));
    }
}

impl Database {
    /// Append the original next-action config body only after its actual writer
    /// has admitted the key and encoded value. The guarded splice retains that
    /// same reservation through publication and original source retirement.
    pub(super) fn stage_next_action_id_in_prepared_write_set(
        &self,
        guard: &mut contextdb_tx::manager::EngineGuard<'_>,
        next_action_id: u64,
    ) -> Result<()> {
        struct ByteCount(usize);
        impl std::io::Write for ByteCount {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0 = self
                    .0
                    .checked_add(bytes.len())
                    .ok_or_else(|| std::io::Error::other("next action config size overflow"))?;
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let mut count = ByteCount(0);
        bincode::serde::encode_into_std_write(
            next_action_id,
            &mut count,
            bincode::config::standard(),
        )
        .map_err(|error| Error::Other(format!("bincode encode error: {error}")))?;
        let bytes = NEXT_ACTION_ID_CONFIG_KEY
            .len()
            .checked_add(count.0)
            .ok_or_else(|| Error::Other("next action config size overflow".into()))?;
        let mut admission = self.reserve_owned_preparation(bytes)?;
        // Exact capacity prevents an encoder growth allocation outside the
        // original admission. Neither key nor value exists before admission.
        let mut encoded = Vec::with_capacity(count.0);
        bincode::serde::encode_into_std_write(
            next_action_id,
            &mut encoded,
            bincode::config::standard(),
        )
        .map_err(|error| Error::Other(format!("bincode encode error: {error}")))?;
        let body = contextdb_tx::staged_source::AuxBody::ConfigWrite((
            NEXT_ACTION_ID_CONFIG_KEY.to_owned(),
            encoded,
        ));
        let at = guard.read().config_writes.len();
        match &self.tx_mgr {
            DatabaseTransactionHost::Owned(handle) => handle.with_host(|host| {
                // The original body is moved immediately with its invoice;
                // there is no fallible operation while they are separate.
                let invoice = admission.into_source_aux_invoice_for(&host.funder)?;
                let batch = guard.prepare_owned_aux(body, invoice)?;
                let prepared = guard.prepare_aux_splice(at, &[], batch)?;
                guard.publish_tx(prepared);
                Ok(())
            }),
            DatabaseTransactionHost::Legacy(_) => Err(DatabaseTransactionHost::producer_gap(
                "stage_next_action_id_in_prepared_write_set",
            )),
        }
    }
}

/// Assembly helper for the approved immutable companion seed loan. Frozen
/// companion storage supplies its authentic stable original field outside
/// queue/stage/state guards; the borrowing private handle retains that physical
/// storage for the whole callback. The actual handle impl is composed only
/// after its frozen definition is available.
pub(crate) fn with_owned_persistence_original_seed<R>(
    original: &crate::memory_accounting::OwnedMemoryReservation,
    f: impl for<'o> FnOnce(&'o crate::memory_accounting::OwnedMemoryReservation) -> Result<R>,
) -> Result<R> {
    let _physical_origin = crate::memory_accounting::persistence_original_projection(original)?;
    f(original)
}

// Dormant original-funded event storage. Legacy assembly never constructs this
// companion; its public structs, methods, bodies and wire encoding stay intact.
use crate::memory_accounting::{
    copy_owned_native_string, copy_owned_native_value, new_owned_native_map, new_owned_native_ring,
    new_owned_native_set, new_owned_native_vec, reserve_preparation_from_original,
    validate_owned_original,
};
use contextdb_tx::manager::EngineTerminalIdentity;
use contextdb_tx::{OriginalAuxInvoice, OriginalCopyGrant, SourceFunder};

fn owned_event_overflow() -> Error {
    Error::Other("event preparation allocation size overflow".into())
}
fn owned_event_add(a: usize, b: usize) -> Result<usize> {
    a.checked_add(b).ok_or_else(owned_event_overflow)
}
fn owned_event_array<T>(count: usize) -> Result<usize> {
    std::alloc::Layout::array::<T>(count)
        .map(|layout| layout.size())
        .map_err(|_| owned_event_overflow())
}
fn reserve_owned_event_bytes(
    original: &OwnedMemoryReservation,
    bytes: usize,
) -> Result<OwnedMemoryReservation> {
    reserve_preparation_from_original(
        original,
        bytes,
        "relational",
        "statement_overlay",
        "Reduce prepared event bodies or raise MEMORY_LIMIT.",
    )
}

// Every native count has an external SourceCharge, carried by the existing
// OriginalAuxInvoice wrapper. Its separate Arc header is itself in that same
// pinned original invoice. No current SourceFunding is used for payloads.
pub(super) struct OwnedEventShared<T> {
    data: Option<Arc<T>>,
    credit: Option<Arc<OriginalAuxInvoice>>,
}
pub(super) struct OwnedEventSharedWeak<T> {
    data: Option<std::sync::Weak<T>>,
    credit: Option<Arc<OriginalAuxInvoice>>,
}
impl<T> OwnedEventShared<T> {
    pub(super) fn try_new(
        body: T,
        original: &OwnedMemoryReservation,
        funder: &SourceFunder,
    ) -> Result<Self> {
        let bytes = owned_event_add(
            contextdb_tx::staged_source::source_arc_layout::<T>().size(),
            contextdb_tx::staged_source::source_arc_layout::<OriginalAuxInvoice>().size(),
        )?;
        // A rejected stack body must die before its invoice.
        let mut reservation = reserve_owned_event_bytes(original, bytes)?;
        let invoice = match reservation.into_source_aux_invoice_for(funder) {
            Ok(invoice) => invoice,
            Err(error) => {
                drop(body);
                return Err(error);
            }
        };
        let credit = Arc::new(invoice);
        Ok(Self {
            data: Some(Arc::new(body)),
            credit: Some(credit),
        })
    }
    pub(super) fn get(&self) -> &T {
        self.data.as_deref().expect("live paid event body")
    }
    pub(super) fn downgrade(&self) -> OwnedEventSharedWeak<T> {
        let credit = self.credit.as_ref().expect("live event invoice").clone();
        let data = Arc::downgrade(self.data.as_ref().expect("live event body"));
        OwnedEventSharedWeak {
            data: Some(data),
            credit: Some(credit),
        }
    }
    pub(super) fn try_into_inner(mut self) -> std::result::Result<T, Self> {
        let data = self.data.take().expect("live event body");
        match Arc::try_unwrap(data) {
            Ok(body) => Ok(body),
            Err(data) => {
                self.data = Some(data);
                Err(self)
            }
        }
    }
}
impl<T> std::ops::Deref for OwnedEventShared<T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.get()
    }
}
impl<T> Clone for OwnedEventShared<T> {
    fn clone(&self) -> Self {
        let credit = self.credit.as_ref().expect("live event invoice").clone();
        let data = self.data.as_ref().expect("live event body").clone();
        Self {
            data: Some(data),
            credit: Some(credit),
        }
    }
}
impl<T> Drop for OwnedEventShared<T> {
    fn drop(&mut self) {
        drop(self.data.take());
        drop(self.credit.take().and_then(Arc::into_inner));
    }
}
impl<T> OwnedEventSharedWeak<T> {
    pub(super) fn upgrade(&self) -> Option<OwnedEventShared<T>> {
        let credit = self
            .credit
            .as_ref()
            .expect("live event weak invoice")
            .clone();
        let data = self.data.as_ref().expect("live event weak").upgrade()?;
        Some(OwnedEventShared {
            data: Some(data),
            credit: Some(credit),
        })
    }
}
impl<T> Clone for OwnedEventSharedWeak<T> {
    fn clone(&self) -> Self {
        let credit = self
            .credit
            .as_ref()
            .expect("live event weak invoice")
            .clone();
        let data = self.data.as_ref().expect("live event weak").clone();
        Self {
            data: Some(data),
            credit: Some(credit),
        }
    }
}
impl<T> Drop for OwnedEventSharedWeak<T> {
    fn drop(&mut self) {
        drop(self.data.take());
        drop(self.credit.take().and_then(Arc::into_inner));
    }
}

pub(super) struct OwnedEventValueCredits {
    keys: Vec<OwnedMemoryReservation>,
    values: Vec<OriginalCopyGrant>,
    map: OwnedMemoryReservation,
    key_array: OwnedMemoryReservation,
    value_array: OwnedMemoryReservation,
}
fn copy_owned_event_values(
    original: &OwnedMemoryReservation,
    source: &HashMap<String, Value>,
) -> Result<(HashMap<String, Value>, OwnedEventValueCredits)> {
    let (body, credit) = new_owned_native_vec(original, source.len())?;
    let keys = OwnedEventBody { body, credit };
    let (body, credit) = new_owned_native_vec(original, source.len())?;
    let values = OwnedEventBody { body, credit };
    let (body, map) = new_owned_native_map(original, source.len())?;
    let mut owner = OwnedEventBody {
        body,
        credit: OwnedEventValueCredits {
            keys: keys.body,
            key_array: keys.credit,
            values: values.body,
            value_array: values.credit,
            map,
        },
    };
    for (key, value) in source {
        let (body, credit) = copy_owned_native_string(original, key)?;
        let key = OwnedEventBody { body, credit };
        let (body, credit) = copy_owned_native_value(original, value)?;
        let value = OwnedEventBody { body, credit };
        owner.body.insert(key.body, value.body);
        owner.credit.keys.push(key.credit);
        owner.credit.values.push(value.credit);
    }
    Ok((owner.body, owner.credit))
}

pub(super) struct OwnedEventGateCredits {
    rows: Vec<OwnedEventValueCredits>,
    children: Vec<OwnedEventGateCredits>,
    row_array: OwnedMemoryReservation,
    child_array: OwnedMemoryReservation,
    metadata: OwnedMemoryReservation,
}
fn owned_gate_metadata_bytes(gate: &EventGateSnapshot) -> Result<usize> {
    let mut bytes = gate.context_column.as_ref().map_or(0, String::len);
    if let Some(scope) = &gate.scope_read {
        bytes = owned_event_add(bytes, scope.column.len())?;
        bytes = owned_event_add(bytes, owned_event_array::<String>(scope.read_labels.len())?)?;
        for label in &scope.read_labels {
            bytes = owned_event_add(bytes, label.len())?;
        }
    }
    if let Some(acl) = &gate.acl {
        bytes = owned_event_add(bytes, acl.column.len())?;
        bytes = owned_event_add(bytes, acl.ref_table.len())?;
        bytes = owned_event_add(bytes, acl.ref_column.len())?;
        bytes = owned_event_add(bytes, std::mem::size_of::<EventGateSnapshot>())?;
        bytes = owned_event_add(
            bytes,
            owned_event_array::<HashMap<String, Value>>(acl.grant_rows.rows().len())?,
        )?;
    }
    Ok(bytes)
}
fn copy_owned_prepaid_text(text: &str) -> Result<String> {
    let copy = text.to_owned();
    if copy.capacity() != text.len() {
        return Err(Error::Other("event text capacity mismatch".into()));
    }
    Ok(copy)
}
fn copy_owned_event_gate(
    original: &OwnedMemoryReservation,
    source: &EventGateSnapshot,
) -> Result<(EventGateSnapshot, OwnedEventGateCredits)> {
    let metadata = reserve_owned_event_bytes(original, owned_gate_metadata_bytes(source)?)?;
    let row_count = source
        .acl
        .as_ref()
        .map_or(0, |acl| acl.grant_rows.rows().len());
    let (body, credit) = new_owned_native_vec(original, row_count)?;
    let rows = OwnedEventBody { body, credit };
    let (body, credit) = new_owned_native_vec(original, usize::from(source.acl.is_some()))?;
    let children = OwnedEventBody { body, credit };
    let mut owner = OwnedEventBody {
        body: EventGateSnapshot::default(),
        credit: OwnedEventGateCredits {
            rows: rows.body,
            row_array: rows.credit,
            children: children.body,
            child_array: children.credit,
            metadata,
        },
    };
    owner.body.context_column = source
        .context_column
        .as_deref()
        .map(copy_owned_prepaid_text)
        .transpose()?;
    if let Some(scope) = &source.scope_read {
        let mut labels = Vec::with_capacity(scope.read_labels.len());
        if labels.capacity() != scope.read_labels.len() {
            return Err(Error::Other("event labels capacity mismatch".into()));
        }
        for label in &scope.read_labels {
            labels.push(copy_owned_prepaid_text(label)?);
        }
        owner.body.scope_read = Some(EventScopeReadGate {
            column: copy_owned_prepaid_text(&scope.column)?,
            read_labels: labels,
        });
    }
    if let Some(acl) = &source.acl {
        let (body, credit) = copy_owned_event_gate(original, &acl.grant_gate)?;
        let grant_gate = OwnedEventBody { body, credit };
        let mut grants = Vec::with_capacity(row_count);
        if grants.capacity() != row_count {
            return Err(Error::Other("event grants capacity mismatch".into()));
        }
        for row in acl.grant_rows.rows() {
            let (body, credit) = copy_owned_event_values(original, row)?;
            let copy = OwnedEventBody { body, credit };
            grants.push(copy.body);
            owner.credit.rows.push(copy.credit);
        }
        owner.body.acl = Some(EventAclGate {
            column: copy_owned_prepaid_text(&acl.column)?,
            ref_table: copy_owned_prepaid_text(&acl.ref_table)?,
            ref_column: copy_owned_prepaid_text(&acl.ref_column)?,
            grant_gate: Box::new(grant_gate.body),
            grant_rows: EventGrantRows::Legacy(grants),
        });
        owner.credit.children.push(grant_gate.credit);
    }
    Ok((owner.body, owner.credit))
}

pub(super) struct OwnedSinkEventCredit {
    row: OwnedEventValueCredits,
    gate: OwnedEventGateCredits,
    metadata: OwnedMemoryReservation,
}
pub(super) struct OwnedSinkQueueEntry {
    pub(super) entry: SinkQueueEntry,
    credit: OwnedSinkEventCredit,
}
fn copy_owned_sink_queue_entry(
    original: &OwnedMemoryReservation,
    source: &SinkQueueEntry,
) -> Result<OwnedSinkQueueEntry> {
    let bytes = owned_event_add(
        owned_event_add(source.event.event_type.len(), source.event.table.len())?,
        source.event.severity.len(),
    )?;
    let metadata = reserve_owned_event_bytes(original, bytes)?;
    let (body, credit) = copy_owned_event_values(original, &source.event.row_values)?;
    let row = OwnedEventBody { body, credit };
    let (body, credit) = copy_owned_event_gate(original, &source.gate)?;
    let gate = OwnedEventBody { body, credit };
    let event_type = copy_owned_prepaid_text(&source.event.event_type)?;
    let table = copy_owned_prepaid_text(&source.event.table)?;
    let severity = copy_owned_prepaid_text(&source.event.severity)?;
    Ok(OwnedSinkQueueEntry {
        entry: SinkQueueEntry {
            id: source.id,
            row_id: source.row_id,
            attempts: source.attempts,
            gate: gate.body,
            event: SinkEvent {
                action_id: source.event.action_id,
                event_type,
                table,
                row_values: row.body,
                severity,
                at_lsn: source.event.at_lsn,
            },
        },
        credit: OwnedSinkEventCredit {
            row: row.credit,
            gate: gate.credit,
            metadata,
        },
    })
}

pub(super) struct OwnedPreparedSinkEvents {
    events: Vec<PreparedSinkEvent>,
    credits: Vec<OwnedSinkEventCredit>,
    sink_keys: Vec<OwnedMemoryReservation>,
    event_array: OwnedMemoryReservation,
    credit_array: OwnedMemoryReservation,
    key_array: OwnedMemoryReservation,
    publication: Option<OwnedAppendCustody>,
}
impl OwnedPreparedSinkEvents {
    pub(super) fn is_empty(&self) -> bool {
        self.events.is_empty()
    }
    pub(super) fn try_copy(
        original: &OwnedMemoryReservation,
        source: &[PreparedSinkEvent],
    ) -> Result<Self> {
        let (body, credit) = new_owned_native_vec(original, source.len())?;
        let events = OwnedEventBody { body, credit };
        let (body, credit) = new_owned_native_vec(original, source.len())?;
        let credits = OwnedEventBody { body, credit };
        let (body, credit) = new_owned_native_vec(original, source.len())?;
        let sink_keys = OwnedEventBody { body, credit };
        let mut owner = Self {
            events: events.body,
            event_array: events.credit,
            credits: credits.body,
            credit_array: credits.credit,
            sink_keys: sink_keys.body,
            key_array: sink_keys.credit,
            publication: None,
        };
        for (sink, source) in source {
            let (body, credit) = copy_owned_native_string(original, sink)?;
            let key = OwnedEventBody { body, credit };
            let copied = copy_owned_sink_queue_entry(original, source)?;
            owner.events.push((key.body, copied.entry));
            owner.credits.push(copied.credit);
            owner.sink_keys.push(key.credit);
        }
        Ok(owner)
    }
    pub(super) fn empty(original: &OwnedMemoryReservation) -> Result<Self> {
        Self::try_copy(original, &[])
    }
}
// Field order is deliberate: payload arrays, row/gate invoices, array invoices,
// then escrow cancellation and mutation lease. No Clone of this aggregate.
pub(super) struct OwnedSinkQueue {
    entries: VecDeque<OwnedEventShared<OwnedSinkQueueEntry>>,
    name: OwnedEventShared<OwnedEventBody<String, OwnedMemoryReservation>>,
    buffer: OwnedMemoryReservation,
    key: Option<OwnedMemoryReservation>,
    pending: usize,
    reservations: usize,
    present: bool,
    retiring: usize,
    // Exact paid source pin of the one durable/memory publication in flight.
    mutation: Option<OwnedEventShared<OwnedSinkQueueEntry>>,
}
pub(super) struct OwnedQueueMap {
    values: HashMap<String, OwnedSinkQueue>,
    allocation: OwnedMemoryReservation,
}
pub(super) struct OwnedCommitEventStage {
    pub(super) identity: EngineTerminalIdentity,
    pub(super) events: OwnedPreparedSinkEvents,
    pub(super) ddl: Option<OwnedStagedEventBusDdlCommit>,
    pub(super) schema: Option<OwnedDefinitionPublication>,
}
pub(super) struct OwnedStagedEventBusDdlCommit {
    pub(super) persistence: EventBusPersistenceCommit,
    pub(super) persistence_credit: OwnedEncodedEventBusConfig,
    pub(super) publication: OwnedDefinitionPublication,
    pub(super) buffers: OwnedMemoryReservation,
    pub(super) ddl_array: OwnedMemoryReservation,
    pub(super) sidecar_array: OwnedMemoryReservation,
}
pub(super) struct OwnedStageMap {
    values: HashMap<Lsn, OwnedEventShared<OwnedCommitEventStage>>,
    allocation: OwnedMemoryReservation,
}
pub(crate) struct OwnedStageBorrow<'e> {
    pub(crate) events: &'e [PreparedSinkEvent],
    pub(crate) ddl: Option<&'e EventBusPersistenceCommit>,
    pub(crate) schema_config: Option<&'e [(String, Vec<u8>)]>,
}
pub(super) struct OwnedEventBusStorage {
    stages: Mutex<OwnedStageMap>,
    queues: Mutex<OwnedQueueMap>,
    queue_claim_waiters: Condvar,
    definition_credit: Mutex<Option<OwnedMemoryReservation>>,
    checkout_allocation: Mutex<Option<OwnedCheckoutAllocation>>,
    metric_allocation: Mutex<Option<OwnedMetricsAllocation>>,
    unloaded_allocation: Mutex<Option<OwnedMetricsAllocation>>,
    common: Arc<EventBusState>,
    funder: SourceFunder,
    pub(super) original: OwnedMemoryReservation,
}
pub(crate) struct OwnedEventBusHandle {
    storage: OwnedEventShared<OwnedEventBusStorage>,
}
pub(super) struct OwnedEventBusWeak {
    storage: OwnedEventSharedWeak<OwnedEventBusStorage>,
}
impl Clone for OwnedEventBusHandle {
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
        }
    }
}
impl Clone for OwnedEventBusWeak {
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
        }
    }
}
impl OwnedEventBusWeak {
    pub(super) fn upgrade(&self) -> Option<OwnedEventBusHandle> {
        self.storage
            .upgrade()
            .map(|storage| OwnedEventBusHandle { storage })
    }
    pub(super) fn matches(&self, handle: &OwnedEventBusHandle) -> bool {
        self.storage
            .data
            .as_ref()
            .expect("live event weak")
            .as_ptr()
            == Arc::as_ptr(handle.storage.data.as_ref().expect("live event body"))
    }
}
impl OwnedEventBusHandle {
    pub(super) fn try_new(
        common: Arc<EventBusState>,
        original: OwnedMemoryReservation,
        expected: &crate::memory_accounting::AccountantHandle,
        funder: &SourceFunder,
    ) -> Result<Self> {
        validate_owned_original(&original, expected, funder)?;
        // Exclusive assembly is required, rather than a shadow for Legacy.
        if !common.queue_mutation_state.lock().checked_out.is_empty()
            || common.queue_mutation_state.lock().active_mutations != 0
            || !common.queues.lock().is_empty()
            || !common.staged_for_persistence.lock().is_empty()
            || !common.staged_ddl_for_persistence.lock().is_empty()
            || *common.definitions.lock() != EventBusDefinitions::default()
            || !common.callbacks.read().is_empty()
            || !common.runtimes.lock().is_empty()
        {
            return Err(Error::Other(
                "owning event assembly requires empty Legacy bodies".into(),
            ));
        }
        let (stages, stage_memory) = new_owned_native_map(&original, 0)?;
        let (queues, queue_memory) = new_owned_native_map(&original, 0)?;
        let shared_bytes = owned_event_add(
            contextdb_tx::staged_source::source_arc_layout::<OwnedEventBusStorage>().size(),
            contextdb_tx::staged_source::source_arc_layout::<OriginalAuxInvoice>().size(),
        )?;
        let mut reservation = reserve_owned_event_bytes(&original, shared_bytes)?;
        let invoice = reservation.into_source_aux_invoice_for(funder)?;
        let credit = Arc::new(invoice);
        let body = OwnedEventBusStorage {
            stages: Mutex::new(OwnedStageMap {
                values: stages,
                allocation: stage_memory,
            }),
            queues: Mutex::new(OwnedQueueMap {
                values: queues,
                allocation: queue_memory,
            }),
            queue_claim_waiters: Condvar::new(),
            definition_credit: Mutex::new(None),
            checkout_allocation: Mutex::new(None),
            metric_allocation: Mutex::new(None),
            unloaded_allocation: Mutex::new(None),
            common,
            funder: funder.clone(),
            original,
        };
        Ok(Self {
            storage: OwnedEventShared {
                data: Some(Arc::new(body)),
                credit: Some(credit),
            },
        })
    }
    pub(super) fn data(&self) -> &OwnedEventBusStorage {
        self.storage.get()
    }
    pub(super) fn downgrade(&self) -> OwnedEventBusWeak {
        OwnedEventBusWeak {
            storage: self.storage.downgrade(),
        }
    }
    pub(super) fn owned_queue_depth(&self, sink: &str) -> u64 {
        let loaded = self
            .data()
            .queues
            .lock()
            .values
            .get(sink)
            .filter(|queue| queue.present)
            .map_or(0, |queue| {
                queue.entries.len().saturating_sub(queue.retiring) as u64
            });
        loaded.saturating_add(
            self.data()
                .common
                .unloaded_queue_depths
                .lock()
                .get(sink)
                .copied()
                .unwrap_or(0),
        )
    }
    pub(super) fn owned_sink_metrics(&self, sink: &str) -> SinkMetrics {
        let queued = self.owned_queue_depth(sink);
        let common = self.data().common.metrics.lock();
        let state = common.get(sink);
        SinkMetrics {
            queued,
            delivered: state.map_or(0, |s| s.delivered),
            retried: state.map_or(0, |s| s.retried),
            uncertain: state.map_or(0, |s| s.uncertain.saturating_add(s.unconfirmed)),
            permanent_failures: state.map_or(0, |s| s.permanent_failures),
            examined: state.map_or(0, |s| s.examined),
            window_refusals: state.map_or(0, |s| s.window_refusals),
            window_refused: state.is_some_and(|s| s.window_refused),
        }
    }
    pub(super) fn with_owned_queue_entries<R>(
        &self,
        sink: &str,
        f: impl for<'q> FnOnce(Option<&'q VecDeque<OwnedEventShared<OwnedSinkQueueEntry>>>) -> Result<R>,
    ) -> Result<R> {
        loop {
            let maximum = {
                let queues = self.data().queues.lock();
                queues
                    .values
                    .get(sink)
                    .filter(|q| q.present)
                    .map_or(0, |q| q.entries.len().saturating_sub(q.retiring))
            };
            let (body, credit) = new_owned_native_ring(&self.data().original, maximum)?;
            let mut snapshot: OwnedEventBody<VecDeque<OwnedEventShared<OwnedSinkQueueEntry>>, _> =
                OwnedEventBody { body, credit };
            let present;
            {
                let queues = self.data().queues.lock();
                let queue = queues.values.get(sink).filter(|q| q.present);
                if queue.is_some_and(|q| {
                    q.entries.len().saturating_sub(q.retiring) > snapshot.body.capacity()
                }) {
                    continue;
                }
                present = queue.is_some();
                if let Some(queue) = queue {
                    for entry in queue.entries.iter().skip(queue.retiring) {
                        snapshot.body.push_back(entry.clone());
                    }
                }
            }
            return f(present.then_some(&snapshot.body));
        }
    }
    pub(super) fn stage_owned_commit_events(
        &self,
        lsn: Lsn,
        mut stage: OwnedCommitEventStage,
    ) -> Result<OwnedEventStageRegistration> {
        self.merge_owned_commit_escrow(&mut stage)?;
        let identity = stage.identity;
        let append = match stage
            .events
            .publication
            .take()
            .ok_or_else(|| Error::Other("owning stage has no append lease".into()))?
        {
            OwnedAppendCustody::Ordinary(token) => {
                if !token.storage.matches(self) || token.state != OwnedEscrowState::Active {
                    return Err(Error::Other(
                        "owning stage requires its active original escrow".into(),
                    ));
                }
                OwnedDiscardAppendHandle {
                    control: OwnedEventShared::try_new(
                        OwnedDiscardAppendControl {
                            token: Mutex::new(Some(token)),
                        },
                        &self.data().original,
                        &self.data().funder,
                    )?,
                }
            }
            OwnedAppendCustody::Discard(handle) => handle,
        };
        stage.events.publication = Some(OwnedAppendCustody::Discard(append.clone()));
        let paid = OwnedEventShared::try_new(stage, &self.data().original, &self.data().funder)?;
        let control = reserve_owned_event_bytes(
            &self.data().original,
            std::mem::size_of::<OwnedEventStageRegistration>(),
        )?;
        let mut registration = OwnedEventStageRegistration {
            storage: self.clone(),
            append: Some(append),
            lsn,
            identity,
            installed: paid.clone(),
            armed: false,
            control,
        };
        loop {
            let (length, capacity, collision) = {
                let map = self.data().stages.lock();
                (
                    map.values.len(),
                    map.values.capacity(),
                    map.values.contains_key(&lsn),
                )
            };
            if collision {
                return Err(Error::Other("owning event stage collision".into()));
            }
            let required = owned_event_add(length, 1)?;
            // Both the successor and its partial/error owners are constructed
            // before the guard, so every retry/refusal/unwind retires unlocked.
            let mut successor = if required > capacity {
                let (values, allocation) = new_owned_native_map(&self.data().original, required)?;
                Some(OwnedStageMap { values, allocation })
            } else {
                None
            };
            let mut map = self.data().stages.lock();
            if map.values.contains_key(&lsn) {
                drop(map);
                return Err(Error::Other("owning event stage collision".into()));
            }
            if map.values.len() != length || map.values.capacity() != capacity {
                drop(map);
                continue;
            }
            if let Some(next) = &mut successor {
                for (key, value) in map.values.drain() {
                    next.values.insert(key, value);
                }
                std::mem::swap(&mut *map, next);
            }
            map.values.insert(lsn, paid);
            drop(map);
            // Nothing arbitrary may retire before exact installed-source cleanup
            // is armed. In particular the old map's original refund may unwind.
            registration.armed = true;
            drop(successor);
            return Ok(registration);
        }
    }

    pub(crate) fn with_owned_commit_events<R>(
        &self,
        lsn: Lsn,
        identity: EngineTerminalIdentity,
        f: impl for<'e> FnOnce(Option<OwnedStageBorrow<'e>>) -> Result<R>,
    ) -> Result<R> {
        // Pin an already-paid count, unlock the map, then lend synchronously.
        let pin = self
            .data()
            .stages
            .lock()
            .values
            .get(&lsn)
            .filter(|stage| stage.identity == identity)
            .cloned();
        f(pin.as_ref().map(|pin| {
            let stage = pin.get();
            OwnedStageBorrow {
                events: &stage.events.events,
                ddl: stage.ddl.as_ref().map(|ddl| &ddl.persistence),
                schema_config: stage
                    .schema
                    .as_ref()
                    .map(|schema| schema.config.values.as_slice()),
            }
        }))
    }
    pub(super) fn take_owned_commit_events(
        &self,
        lsn: Lsn,
        identity: EngineTerminalIdentity,
    ) -> Option<OwnedEventShared<OwnedCommitEventStage>> {
        let mut stages = self.data().stages.lock();
        if !stages
            .values
            .get(&lsn)
            .is_some_and(|stage| stage.get().identity == identity)
        {
            return None;
        }
        let taken = stages.values.remove(&lsn);
        drop(stages);
        taken
    }
}
pub(crate) struct OwnedEventStageRegistration {
    storage: OwnedEventBusHandle,
    append: Option<OwnedDiscardAppendHandle>,
    lsn: Lsn,
    identity: EngineTerminalIdentity,
    installed: OwnedEventShared<OwnedCommitEventStage>,
    armed: bool,
    control: OwnedMemoryReservation,
}
impl OwnedEventStageRegistration {
    pub(super) fn take(mut self) -> Option<OwnedEventShared<OwnedCommitEventStage>> {
        let stage = self.storage.take_owned_registered_stage(&self);
        self.armed = false;
        stage
    }
}
impl Drop for OwnedEventStageRegistration {
    fn drop(&mut self) {
        if self.armed {
            let retired = self.storage.take_owned_registered_stage(self);
            drop(retired);
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum OwnedEscrowState {
    Active,
    Suspended,
    Frozen,
    Published,
}
pub(super) struct OwnedQueueAppendSlot {
    sink: String,
    key: OwnedMemoryReservation,
    count: usize,
    retired: Option<(String, OwnedSinkQueue)>,
}
pub(super) struct OwnedSinkQueuePublication {
    entries:
        OwnedEventBody<Vec<Option<OwnedEventShared<OwnedSinkQueueEntry>>>, OwnedMemoryReservation>,
    slots: Vec<OwnedQueueAppendSlot>,
    workspace: OwnedMemoryReservation,
    storage: OwnedEventBusWeak,
    lease: Option<QueueMutationLease>,
    state: OwnedEscrowState,
}
pub(super) struct OwnedDiscardAppendControl {
    token: Mutex<Option<OwnedSinkQueuePublication>>,
}
pub(super) struct OwnedDiscardAppendHandle {
    control: OwnedEventShared<OwnedDiscardAppendControl>,
}
impl Clone for OwnedDiscardAppendHandle {
    fn clone(&self) -> Self {
        Self {
            control: self.control.clone(),
        }
    }
}
#[allow(
    clippy::large_enum_variant,
    reason = "boxing the ordinary publication would allocate outside its admission"
)]
pub(super) enum OwnedAppendCustody {
    Ordinary(OwnedSinkQueuePublication),
    Discard(OwnedDiscardAppendHandle),
}
impl OwnedAppendCustody {
    fn take_token(self) -> OwnedSinkQueuePublication {
        match self {
            Self::Ordinary(token) => token,
            Self::Discard(handle) => handle
                .control
                .get()
                .token
                .lock()
                .take()
                .expect("discard owns append token"),
        }
    }
}
fn cancel_owned_slots_locked(map: &mut OwnedQueueMap, slots: &mut [OwnedQueueAppendSlot]) {
    for slot in slots {
        let queue = map
            .values
            .get_mut(&slot.sink)
            .expect("active escrow sink exists");
        assert!(queue.pending >= slot.count, "own pending escrow exists");
        queue.pending -= slot.count;
        queue.reservations -= 1;
        if !queue.present && queue.pending == 0 && queue.reservations == 0 {
            slot.retired = map.values.remove_entry(&slot.sink);
        }
    }
}
impl Drop for OwnedSinkQueuePublication {
    fn drop(&mut self) {
        if self.state == OwnedEscrowState::Active
            && let Some(storage) = self.storage.upgrade()
        {
            let mut queues = storage.data().queues.lock();
            cancel_owned_slots_locked(&mut queues, &mut self.slots);
            drop(queues);
        }
        // Fields destroy retired bodies before their invoices; drop lease only
        // after cancellation releases the queue lock.
    }
}
pub(super) struct OwnedQueuePreparation {
    sink: String,
    name: Option<OwnedEventShared<OwnedEventBody<String, OwnedMemoryReservation>>>,
    required: usize,
    absent: bool,
    replace_ring: bool,
    key: Option<OwnedMemoryReservation>,
    count: usize,
    replacement: Option<(
        VecDeque<OwnedEventShared<OwnedSinkQueueEntry>>,
        OwnedMemoryReservation,
    )>,
    retired: Option<(
        VecDeque<OwnedEventShared<OwnedSinkQueueEntry>>,
        OwnedMemoryReservation,
    )>,
}
impl OwnedEventBusHandle {
    fn prepare_owned_queue_name(
        &self,
        name: &str,
    ) -> Result<OwnedEventShared<OwnedEventBody<String, OwnedMemoryReservation>>> {
        let (body, credit) = copy_owned_native_string(&self.data().original, name)?;
        OwnedEventShared::try_new(
            OwnedEventBody { body, credit },
            &self.data().original,
            &self.data().funder,
        )
    }
    fn queue_limit(&self) -> usize {
        #[cfg(any(test, feature = "test-seams"))]
        {
            self.data()
                .common
                .queue_capacity_for_test
                .load(Ordering::SeqCst)
        }
        #[cfg(not(any(test, feature = "test-seams")))]
        {
            MAX_SINK_QUEUE_DEPTH
        }
    }
    pub(super) fn reserve_owned_queue_publication(
        &self,
        events: &OwnedPreparedSinkEvents,
        lease: QueueMutationLease,
    ) -> Result<OwnedSinkQueuePublication> {
        // Pin actual original-funded copies before any queue guard. These are
        // also the checkout identity; publication performs no Arc admission.
        let (body, credit) = new_owned_native_vec(&self.data().original, events.events.len())?;
        let mut pins = OwnedEventBody { body, credit };
        for ((_, entry), credit) in events.events.iter().zip(&events.credits) {
            let copied = copy_owned_sink_queue_entry(&credit.metadata, entry)?;
            let pin = OwnedEventShared::try_new(copied, &credit.metadata, &self.data().funder)?;
            pins.body.push(Some(pin));
        }
        let mut token = self.reserve_owned_queue_names(&events.events, &[], lease)?;
        std::mem::swap(&mut token.entries, &mut pins);
        Ok(token)
    }
    fn reserve_owned_queue_names(
        &self,
        events: &[PreparedSinkEvent],
        names: &[String],
        lease: QueueMutationLease,
    ) -> Result<OwnedSinkQueuePublication> {
        if !Arc::ptr_eq(&lease.state, &self.data().common.queue_mutation_state) {
            return Err(Error::Other(
                "queue lease belongs to another event bus".into(),
            ));
        }
        let original = &self.data().original;
        // Arrays are sized for the worst-case distinct keys before creating any
        // owned lookup or successor. No unpriced additions HashMap.
        let maximum = owned_event_add(events.len(), names.len())?;
        let (slots, workspace) = new_owned_native_vec(original, maximum)?;
        let (body, credit) = new_owned_native_vec(original, 0)?;
        let mut token = OwnedSinkQueuePublication {
            entries: OwnedEventBody { body, credit },
            slots,
            workspace,
            storage: self.downgrade(),
            lease: Some(lease),
            state: OwnedEscrowState::Suspended,
        };
        for (sink, _) in events {
            if let Some(slot) = token.slots.iter_mut().find(|slot| slot.sink == *sink) {
                slot.count = owned_event_add(slot.count, 1)?;
            } else {
                let (sink, key) = copy_owned_native_string(original, sink)?;
                token.slots.push(OwnedQueueAppendSlot {
                    sink,
                    key,
                    count: 1,
                    retired: None,
                });
            }
        }
        for sink in names {
            if !token.slots.iter().any(|slot| slot.sink == *sink) {
                let (sink, key) = copy_owned_native_string(original, sink)?;
                token.slots.push(OwnedQueueAppendSlot {
                    sink,
                    key,
                    count: 0,
                    retired: None,
                });
            }
        }
        let (plans, plan_memory) = new_owned_native_vec(original, token.slots.len())?;
        let mut plans = OwnedEventBody {
            body: plans,
            credit: plan_memory,
        };
        for slot in &token.slots {
            let (sink, key) = copy_owned_native_string(original, &slot.sink)?;
            let (body, credit) = copy_owned_native_string(original, &slot.sink)?;
            let name = OwnedEventShared::try_new(
                OwnedEventBody { body, credit },
                original,
                &self.data().funder,
            )?;
            plans.body.push(OwnedQueuePreparation {
                sink,
                name: Some(name),
                key: Some(key),
                count: slot.count,
                required: 0,
                absent: false,
                replace_ring: false,
                replacement: None,
                retired: None,
            });
        }
        loop {
            // Inspect scalar requirements only. No funded constructor, copied
            // source, formatted refusal or partial owner lives inside this guard.
            let mut full = None;
            let required_map;
            let replace_map;
            {
                let queues = self.data().queues.lock();
                let mut absent = 0;
                for (index, plan) in plans.body.iter_mut().enumerate() {
                    let queue = queues.values.get(&plan.sink);
                    plan.absent = queue.is_none();
                    absent += usize::from(plan.absent);
                    let required = queue
                        .map_or(0, |q| q.entries.len())
                        .checked_add(queue.map_or(0, |q| q.pending))
                        .and_then(|n| n.checked_add(plan.count));
                    plan.required = required.unwrap_or(usize::MAX);
                    plan.replace_ring = queue.is_none()
                        || plan.required > queue.map_or(0, |q| q.entries.capacity());
                    if required.is_none() || plan.required > self.queue_limit() {
                        full = Some((index, required.is_none()));
                        break;
                    }
                }
                required_map = queues.values.len().checked_add(absent);
                replace_map = required_map.is_some_and(|n| n > queues.values.capacity());
            }
            if let Some((index, overflow)) = full {
                if overflow {
                    return Err(owned_event_overflow());
                }
                return Err(Error::Other(format!(
                    "sink queue is full: {}",
                    plans.body[index].sink
                )));
            }
            let required_map = required_map.ok_or_else(owned_event_overflow)?;
            // Constructors (including their capacity-refusal and unwind paths)
            // run unlocked; every partial successor stays in an outer owner.
            let mut successor = if replace_map {
                let (values, allocation) = new_owned_native_map(original, required_map)?;
                Some(OwnedQueueMap { values, allocation })
            } else {
                None
            };
            for plan in &mut plans.body {
                if plan.replace_ring {
                    plan.replacement = Some(new_owned_native_ring(original, plan.required)?);
                }
            }
            let mut queues = self.data().queues.lock();
            let valid = queues.values.len()
                + plans
                    .body
                    .iter()
                    .filter(|p| !queues.values.contains_key(&p.sink))
                    .count()
                <= successor
                    .as_ref()
                    .map_or(queues.values.capacity(), |q| q.values.capacity())
                && plans.body.iter().all(|p| {
                    let queue = queues.values.get(&p.sink);
                    queue
                        .map_or(0, |q| q.entries.len())
                        .checked_add(queue.map_or(0, |q| q.pending))
                        .and_then(|n| n.checked_add(p.count))
                        .is_some_and(|n| {
                            n <= self.queue_limit()
                                && match &p.replacement {
                                    Some((ring, _)) => n <= ring.capacity(),
                                    None => queue.is_some_and(|q| n <= q.entries.capacity()),
                                }
                        })
                });
            if !valid {
                drop(queues);
                for plan in &mut plans.body {
                    drop(plan.replacement.take());
                }
                drop(successor);
                continue;
            }
            // String hashing and paid-capacity moves only. Successor/plans were
            // declared before the guard, so an unwind unlocks before retirement.
            if let Some(successor) = &mut successor {
                for (key, value) in queues.values.drain() {
                    successor.values.insert(key, value);
                }
                std::mem::swap(&mut *queues, successor);
            }
            for plan in &mut plans.body {
                if let Some(queue) = queues.values.get_mut(&plan.sink) {
                    if let Some(replacement) = &mut plan.replacement
                        && replacement.0.capacity() > queue.entries.capacity()
                    {
                        replacement.0.append(&mut queue.entries);
                        std::mem::swap(&mut queue.entries, &mut replacement.0);
                        std::mem::swap(&mut queue.buffer, &mut replacement.1);
                    }
                    queue.pending += plan.count;
                    queue.reservations += 1;
                } else {
                    let vacant = queues.values.entry(std::mem::take(&mut plan.sink));
                    let (entries, buffer) = plan.replacement.take().expect("prepared new ring");
                    vacant.or_insert(OwnedSinkQueue {
                        entries,
                        buffer,
                        name: plan.name.take().expect("prepared queue name"),
                        key: plan.key.take(),
                        pending: plan.count,
                        reservations: 1,
                        present: false,
                        retiring: 0,
                        mutation: None,
                    });
                }
            }
            drop(queues);
            token.state = OwnedEscrowState::Active;
            drop(successor);
            break;
        }
        drop(plans);
        Ok(token)
    }
    pub(super) fn prepare_owned_sink_events(
        &self,
        source: &[PreparedSinkEvent],
    ) -> Result<OwnedPreparedSinkEvents> {
        let mut events = OwnedPreparedSinkEvents::try_copy(&self.data().original, source)?;
        // Empty changes still participate in the original mutation freeze.
        let lease = self.data().common.begin_queue_mutation();
        let token = self.reserve_owned_queue_publication(&events, lease)?;
        events.publication = Some(OwnedAppendCustody::Ordinary(token));
        Ok(events)
    }
    pub(super) fn publish_owned_sink_events_to_memory(&self, mut events: OwnedPreparedSinkEvents) {
        let mut token = events
            .publication
            .take()
            .expect("prepared append custody")
            .take_token();
        assert!(
            token.storage.matches(self),
            "append retains its actual event companion"
        );
        assert!(
            token.state == OwnedEscrowState::Active,
            "append transfer installed before thaw"
        );
        let mut queues = self.data().queues.lock();
        for ((sink, _), entry) in events.events.iter().zip(&mut token.entries.body) {
            let queue = queues
                .values
                .get_mut(sink)
                .expect("committed paid sink slot");
            assert!(queue.pending != 0 && queue.entries.len() < queue.entries.capacity());
            queue
                .entries
                .push_back(entry.take().expect("prepared append pin"));
            queue.pending -= 1;
            queue.present = true;
        }
        for slot in &token.slots {
            let queue = queues.values.get_mut(&slot.sink).expect("paid append slot");
            queue.reservations -= 1;
            if slot.count == 0 {
                queue.present = true;
            }
        }
        token.state = OwnedEscrowState::Published;
        drop(queues);
        drop(token);
        drop(events);
        self.data().common.waiters.notify_all();
    }
    pub(super) fn suspend_owned_append_for_discard(
        &self,
        events: &mut OwnedPreparedSinkEvents,
    ) -> Result<OwnedDiscardAppendHandle> {
        let token = events
            .publication
            .take()
            .ok_or_else(|| Error::Other("discard append has no custody".into()))?
            .take_token();
        if !token.storage.matches(self) || token.state != OwnedEscrowState::Active {
            return Err(Error::Other(
                "discard append is not active on its original companion".into(),
            ));
        }
        // Admit and build the actual shared transfer control BEFORE cancelling
        // this terminal's pending count or dropping its normal mutation lease.
        let control = OwnedEventShared::try_new(
            OwnedDiscardAppendControl {
                token: Mutex::new(Some(token)),
            },
            &self.data().original,
            &self.data().funder,
        )?;
        let handle = OwnedDiscardAppendHandle { control };
        let mut token = handle
            .control
            .get()
            .token
            .lock()
            .take()
            .expect("prepared transfer token");
        let mut queues = self.data().queues.lock();
        cancel_owned_slots_locked(&mut queues, &mut token.slots);
        token.state = OwnedEscrowState::Suspended;
        drop(queues);
        for slot in &mut token.slots {
            drop(slot.retired.take());
        }
        drop(token.lease.take());
        *handle.control.get().token.lock() = Some(token);
        events.publication = Some(OwnedAppendCustody::Discard(handle.clone()));
        Ok(handle)
    }
}
// A pair is always retained as fields, including partial preparation/error.
pub(super) struct OwnedEventBody<T, C> {
    body: T,
    credit: C,
}

// Error and unwind restore the SAME suspended escrow after queue guards leave.
struct OwnedSuspendedAppend {
    token: Option<OwnedSinkQueuePublication>,
    handle: Option<OwnedDiscardAppendHandle>,
}
impl Drop for OwnedSuspendedAppend {
    fn drop(&mut self) {
        if let (Some(handle), Some(token)) = (&self.handle, self.token.take()) {
            let previous = handle.control.get().token.lock().replace(token);
            assert!(previous.is_none(), "suspended escrow has one owner");
            drop(previous);
        }
    }
}
struct OwnedPurgeQueueSource {
    name: OwnedEventShared<OwnedEventBody<String, OwnedMemoryReservation>>,
    entries: std::ops::Range<usize>,
    present: bool,
}
// This field owner drops only after the publication's queues are physically
// destroyed, and before its original purge token reopens queue mutations.
struct OwnedPurgeAppendTransfer {
    handle: OwnedDiscardAppendHandle,
}
impl Drop for OwnedPurgeAppendTransfer {
    fn drop(&mut self) {
        let mut token = self.handle.control.get().token.lock();
        if let Some(append) = token.as_mut()
            && append.state == OwnedEscrowState::Frozen
        {
            append.state = OwnedEscrowState::Suspended;
        }
        // A published Active token keeps its transferred lease and identity.
    }
}
pub(super) struct OwnedPurgeQueuePublication {
    queues: OwnedQueueMap,
    sink_names: OwnedEventBody<Vec<String>, OwnedMemoryReservation>,
    name_keys: OwnedEventBody<Vec<OwnedMemoryReservation>, OwnedMemoryReservation>,
    transfer: Option<OwnedPurgeAppendTransfer>,
    transferred_lease: Option<QueueMutationLease>,
    token: AuthoritativePurgeQueueMutationToken,
}
impl OwnedPurgeQueuePublication {
    pub(super) fn with_sink_names<R>(&self, f: impl FnOnce(&[String]) -> R) -> R {
        f(&self.sink_names.body)
    }
}
impl OwnedEventBusHandle {
    pub(super) fn prepare_owned_purge_queue_replacement(
        &self,
        token: AuthoritativePurgeQueueMutationToken,
        durable_names: &BTreeSet<String>,
        selections: &BTreeMap<String, HashSet<RowId>>,
        own_append: Option<&OwnedPreparedSinkEvents>,
    ) -> Result<OwnedPurgeQueuePublication> {
        let original = &self.data().original;
        if !Arc::ptr_eq(&token.state, &self.data().common.queue_mutation_state) {
            return Err(Error::Other(
                "purge token belongs to another event bus".into(),
            ));
        }
        {
            let state = token.state.lock();
            // The ORIGINAL freeze wait, including empty received/definition
            // mutations, has finished before copying any live body.
            assert!(state.checkout_paused && state.mutation_blocked && state.active_mutations == 0);
        }
        let transfer = match own_append.and_then(|events| events.publication.as_ref()) {
            Some(OwnedAppendCustody::Discard(handle)) => Some(handle.clone()),
            Some(OwnedAppendCustody::Ordinary(_)) => {
                return Err(Error::Other(
                    "discard must suspend its own lease before freeze".into(),
                ));
            }
            None => None,
        };
        // Take control under its own guard and immediately unlock; never hold
        // that guard while acquiring queue/state locks.
        let mut suspended = OwnedSuspendedAppend {
            token: transfer.as_ref().map(|handle| {
                handle
                    .control
                    .get()
                    .token
                    .lock()
                    .take()
                    .expect("suspended discard escrow")
            }),
            handle: transfer.clone(),
        };
        if suspended
            .token
            .as_ref()
            .is_some_and(|append| append.state != OwnedEscrowState::Suspended)
        {
            return Err(Error::Other("discard escrow is not suspended".into()));
        }
        let (source_count, entry_count) = {
            let queues = self.data().queues.lock();
            assert!(
                queues
                    .values
                    .values()
                    .all(|queue| queue.pending == 0 && queue.mutation.is_none())
            );
            (
                queues.values.len(),
                queues
                    .values
                    .values()
                    .try_fold(0usize, |n, q| n.checked_add(q.entries.len())),
            )
        };
        let entry_count = entry_count.ok_or_else(owned_event_overflow)?;
        let (body, credit) = new_owned_native_vec(original, source_count)?;
        let mut sources: OwnedEventBody<Vec<OwnedPurgeQueueSource>, _> =
            OwnedEventBody { body, credit };
        let (body, credit) = new_owned_native_vec(original, entry_count)?;
        let mut pins = OwnedEventBody { body, credit };
        {
            let queues = self.data().queues.lock();
            // The original freeze makes these capacities immutable. Only paid
            // count clones run locked; deep copies and constructors run below.
            assert_eq!(queues.values.len(), source_count);
            for queue in queues.values.values() {
                let begin = pins.body.len();
                for entry in &queue.entries {
                    pins.body.push(entry.clone());
                }
                sources.body.push(OwnedPurgeQueueSource {
                    name: queue.name.clone(),
                    entries: begin..pins.body.len(),
                    present: queue.present,
                });
            }
        }
        let extra = suspended
            .token
            .as_ref()
            .map_or(0, |append| append.slots.len());
        let maximum = owned_event_add(owned_event_add(source_count, durable_names.len())?, extra)?;
        let (names, name_array) = new_owned_native_vec(original, maximum)?;
        let (keys, key_array) = new_owned_native_vec(original, maximum)?;
        let mut copied = OwnedEventNames {
            names,
            keys,
            name_array,
            key_array,
        };
        for name in durable_names
            .iter()
            .map(String::as_str)
            .chain(sources.body.iter().map(|q| q.name.body.as_str()))
            .chain(
                suspended
                    .token
                    .iter()
                    .flat_map(|append| append.slots.iter().map(|s| s.sink.as_str())),
            )
        {
            if !copied.names.iter().any(|existing| existing == name) {
                let (name, credit) = copy_owned_native_string(original, name)?;
                copied.names.push(name);
                copied.keys.push(credit);
            }
        }
        let (values, allocation) = new_owned_native_map(original, copied.names.len())?;
        let mut successor = OwnedQueueMap { values, allocation };
        for name in &copied.names {
            let source = sources.body.iter().find(|q| q.name.body == *name);
            let source_entries = source.map_or(&[][..], |q| &pins.body[q.entries.clone()]);
            let keep = |entry: &&OwnedEventShared<OwnedSinkQueueEntry>| {
                !selections
                    .get(&entry.entry.event.table)
                    .is_some_and(|ids| ids.contains(&entry.entry.row_id))
            };
            let survivors = source_entries.iter().filter(keep).count();
            let pending = suspended.token.as_ref().map_or(Ok(0), |append| {
                append
                    .slots
                    .iter()
                    .filter(|slot| slot.sink == *name)
                    .try_fold(0, |total, slot| owned_event_add(total, slot.count))
            })?;
            let required = owned_event_add(survivors, pending)?;
            if required > self.queue_limit() {
                return Err(Error::Other(format!("sink queue is full: {name}")));
            }
            let (body, credit) = copy_owned_native_string(original, name)?;
            let key = OwnedEventBody { body, credit };
            let (entries, buffer) = new_owned_native_ring(original, required)?;
            let mut queue = OwnedSinkQueue {
                entries,
                buffer,
                name: self.prepare_owned_queue_name(name)?,
                key: None,
                pending,
                reservations: suspended.token.as_ref().map_or(0, |append| {
                    append
                        .slots
                        .iter()
                        .filter(|slot| slot.sink == *name)
                        .count()
                }),
                present: source.is_some_and(|q| q.present) || durable_names.contains(name),
                retiring: 0,
                mutation: None,
            };
            for entry in source_entries.iter().filter(keep) {
                let copy = copy_owned_sink_queue_entry(&entry.get().credit.metadata, &entry.entry)?;
                let pin = OwnedEventShared::try_new(
                    copy,
                    &entry.get().credit.metadata,
                    &self.data().funder,
                )?;
                queue.entries.push_back(pin);
            }
            queue.key = Some(key.credit);
            successor.values.insert(key.body, queue);
        }
        let queues = successor;
        let sink_names = OwnedEventBody {
            body: copied.names,
            credit: copied.name_array,
        };
        let name_keys = OwnedEventBody {
            body: copied.keys,
            credit: copied.key_array,
        };
        // Prebuild the exact normal lease before durability, without counting
        // it active while the original freeze is still waiting/held.
        let transferred_lease = transfer.as_ref().map(|_| QueueMutationLease {
            state: token.state.clone(),
            waiters: token.waiters.clone(),
        });
        let transfer = transfer.map(|handle| OwnedPurgeAppendTransfer { handle });
        if let (Some(transfer), Some(mut append)) = (transfer.as_ref(), suspended.token.take()) {
            append.state = OwnedEscrowState::Frozen;
            *transfer.handle.control.get().token.lock() = Some(append);
        }
        Ok(OwnedPurgeQueuePublication {
            queues,
            sink_names,
            name_keys,
            transfer,
            transferred_lease,
            token,
        })
    }
    pub(super) fn publish_owned_purge_queue_replacement(
        &self,
        mut publication: OwnedPurgeQueuePublication,
    ) {
        {
            let mut queues = self.data().queues.lock();
            std::mem::swap(&mut *queues, &mut publication.queues);
            // The old paid map is now owned by publication, outside the guard.
        }
        if let Some(transfer) = &publication.transfer {
            let mut append = transfer
                .handle
                .control
                .get()
                .token
                .lock()
                .take()
                .expect("frozen append escrow");
            assert!(append.state == OwnedEscrowState::Frozen);
            // Queue guard was released. Install the SAME paid lease before
            // the ORIGINAL freeze token reopens mutations and checkouts.
            let mut state = publication.token.state.lock();
            assert!(state.active_mutations == 0 && state.mutation_blocked);
            state.active_mutations += 1;
            append.lease = publication.transferred_lease.take();
            append.state = OwnedEscrowState::Active;
            drop(state);
            *transfer.handle.control.get().token.lock() = Some(append);
        }
        // Retire old bodies before thaw. Publication field order retains the
        // freeze until the old map and transfer captures are physically gone.
        drop(publication);
        self.data().common.waiters.notify_all();
    }
}

// The sorted insertion node plan follows the existing original JSON producer's
// splitpoint plan. These concrete layouts require the same deployed-toolchain
// qualification as NativeMapPlan; this dormant source is not that proof.
#[repr(C)]
pub(super) struct OwnedEventBTreeLeaf<K, V> {
    parent: Option<std::ptr::NonNull<OwnedEventBTreeInternal<K, V>>>,
    parent_index: std::mem::MaybeUninit<u16>,
    len: u16,
    keys: [std::mem::MaybeUninit<K>; 11],
    values: [std::mem::MaybeUninit<V>; 11],
}
#[repr(C)]
pub(super) struct OwnedEventBTreeInternal<K, V> {
    data: OwnedEventBTreeLeaf<K, V>,
    edges: [std::mem::MaybeUninit<std::ptr::NonNull<OwnedEventBTreeLeaf<K, V>>>; 12],
}
fn owned_event_btree_bytes<K, V>(members: usize) -> Result<usize> {
    if members == 0 {
        return Ok(0);
    }
    let mut right = [0u8; usize::BITS as usize];
    let mut height = 0;
    let mut leaves = 1;
    let mut branches = 0;
    for _ in 0..members {
        let mut level = 0;
        loop {
            if right[level] < 11 {
                right[level] += 1;
                break;
            }
            right[level] = 5;
            if level == 0 {
                leaves = owned_event_add(leaves, 1)?;
            } else {
                branches = owned_event_add(branches, 1)?;
            }
            level += 1;
            if level > height {
                height = level;
                if height == right.len() {
                    return Err(owned_event_overflow());
                }
                right[height] = 1;
                branches = owned_event_add(branches, 1)?;
                break;
            }
        }
    }
    owned_event_add(
        owned_event_array::<OwnedEventBTreeLeaf<K, V>>(leaves)?,
        owned_event_array::<OwnedEventBTreeInternal<K, V>>(branches)?,
    )
}
pub(super) struct OwnedDefinitionsProjection {
    pub(super) definitions: EventBusDefinitions,
    allocation: OwnedMemoryReservation,
}
impl OwnedDefinitionsProjection {
    pub(super) fn try_copy_filtered(
        original: &OwnedMemoryReservation,
        source: &EventBusDefinitions,
        without_table: Option<&str>,
    ) -> Result<Self> {
        let keep_type = |def: &&EventTypeDef| without_table != Some(def.table.as_str());
        let keep_route = |def: &&RouteDef| {
            source
                .event_types
                .get(&def.event_type)
                .is_none_or(|event_type| without_table != Some(event_type.table.as_str()))
        };
        let type_count = source.event_types.values().filter(keep_type).count();
        let route_count = source.routes.values().filter(keep_route).count();
        let mut bytes = owned_event_add(
            owned_event_btree_bytes::<String, EventTypeDef>(type_count)?,
            owned_event_btree_bytes::<String, SinkDef>(source.sinks.len())?,
        )?;
        bytes = owned_event_add(
            bytes,
            owned_event_btree_bytes::<String, RouteDef>(route_count)?,
        )?;
        for (key, def) in source
            .event_types
            .iter()
            .filter(|(_, def)| without_table != Some(def.table.as_str()))
        {
            bytes = owned_event_add(
                bytes,
                owned_event_add(owned_event_add(key.len(), def.name.len())?, def.table.len())?,
            )?;
        }
        for (key, def) in &source.sinks {
            bytes = owned_event_add(
                bytes,
                owned_event_add(
                    owned_event_add(key.len(), def.name.len())?,
                    def.url.as_ref().map_or(0, String::len),
                )?,
            )?;
        }
        for (key, def) in source.routes.iter().filter(|(_, def)| keep_route(def)) {
            for text in [
                key.as_str(),
                def.name.as_str(),
                def.event_type.as_str(),
                def.sink.as_str(),
            ] {
                bytes = owned_event_add(bytes, text.len())?;
            }
            if let Some(filter) = &def.where_in {
                bytes = owned_event_add(bytes, filter.column.len())?;
                bytes = owned_event_add(bytes, owned_event_array::<String>(filter.values.len())?)?;
                for value in &filter.values {
                    bytes = owned_event_add(bytes, value.len())?;
                }
            }
        }
        let allocation = reserve_owned_event_bytes(original, bytes)?;
        let mut owner = Self {
            definitions: EventBusDefinitions::default(),
            allocation,
        };
        for (key, def) in source
            .event_types
            .iter()
            .filter(|(_, def)| without_table != Some(def.table.as_str()))
        {
            owner.definitions.event_types.insert(
                copy_owned_prepaid_text(key)?,
                EventTypeDef {
                    name: copy_owned_prepaid_text(&def.name)?,
                    trigger: def.trigger,
                    table: copy_owned_prepaid_text(&def.table)?,
                },
            );
        }
        for (key, def) in &source.sinks {
            owner.definitions.sinks.insert(
                copy_owned_prepaid_text(key)?,
                SinkDef {
                    name: copy_owned_prepaid_text(&def.name)?,
                    sink_type: def.sink_type,
                    url: def
                        .url
                        .as_deref()
                        .map(copy_owned_prepaid_text)
                        .transpose()?,
                },
            );
        }
        for (key, def) in source.routes.iter().filter(|(_, def)| keep_route(def)) {
            let where_in = if let Some(filter) = &def.where_in {
                let mut values = Vec::with_capacity(filter.values.len());
                if values.capacity() != filter.values.len() {
                    return Err(Error::Other("event route capacity mismatch".into()));
                }
                for value in &filter.values {
                    values.push(copy_owned_prepaid_text(value)?);
                }
                Some(PersistedRouteWhereIn {
                    column: copy_owned_prepaid_text(&filter.column)?,
                    values,
                })
            } else {
                None
            };
            owner.definitions.routes.insert(
                copy_owned_prepaid_text(key)?,
                RouteDef {
                    name: copy_owned_prepaid_text(&def.name)?,
                    event_type: copy_owned_prepaid_text(&def.event_type)?,
                    sink: copy_owned_prepaid_text(&def.sink)?,
                    where_in,
                },
            );
        }
        Ok(owner)
    }
}
pub(super) struct OwnedEncodedEventBusConfig {
    pub(super) values: Vec<(String, Vec<u8>)>,
    bodies: Vec<OwnedMemoryReservation>,
    values_memory: OwnedMemoryReservation,
    body_array: OwnedMemoryReservation,
}
pub(super) struct OwnedEventSequence<'a, T> {
    values: std::collections::btree_map::Values<'a, String, T>,
}
impl<T: Serialize> Serialize for OwnedEventSequence<'_, T> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        let mut sequence = serializer.serialize_seq(Some(self.values.len()))?;
        for value in self.values.clone() {
            sequence.serialize_element(value)?;
        }
        sequence.end()
    }
}
fn encode_owned_event_value<T: Serialize>(
    original: &OwnedMemoryReservation,
    value: &T,
) -> Result<OwnedEventBody<Vec<u8>, OwnedMemoryReservation>> {
    let mut count = bincode::enc::write::SizeWriter::default();
    bincode::serde::encode_into_writer(value, &mut count, bincode::config::standard())
        .map_err(|error| Error::Other(format!("bincode encode error: {error}")))?;
    let (body, credit) = new_owned_native_vec::<u8>(original, count.bytes_written)?;
    let mut owner = OwnedEventBody { body, credit };
    owner.body.resize(count.bytes_written, 0);
    let written =
        bincode::serde::encode_into_slice(value, &mut owner.body, bincode::config::standard())
            .map_err(|error| Error::Other(format!("bincode encode error: {error}")))?;
    if written != owner.body.len() {
        return Err(Error::Other("event encoding size changed".into()));
    }
    Ok(owner)
}
impl OwnedEncodedEventBusConfig {
    pub(super) fn try_new(
        original: &OwnedMemoryReservation,
        definitions: &EventBusDefinitions,
    ) -> Result<Self> {
        Self::try_new_capacity(original, definitions, 0)
    }
    fn try_empty(original: &OwnedMemoryReservation) -> Result<Self> {
        let (body, credit) = new_owned_native_vec(original, 0)?;
        let values = OwnedEventBody { body, credit };
        let (body, credit) = new_owned_native_vec(original, 0)?;
        Ok(Self {
            values: values.body,
            values_memory: values.credit,
            bodies: body,
            body_array: credit,
        })
    }
    fn try_new_capacity(
        original: &OwnedMemoryReservation,
        definitions: &EventBusDefinitions,
        extra: usize,
    ) -> Result<Self> {
        let (values, values_memory) = new_owned_native_vec(original, owned_event_add(3, extra)?)?;
        let values = OwnedEventBody {
            body: values,
            credit: values_memory,
        };
        let (bodies, body_array) = new_owned_native_vec(
            original,
            owned_event_add(6, extra.checked_mul(3).ok_or_else(owned_event_overflow)?)?,
        )?;
        let mut owner = Self {
            values: values.body,
            values_memory: values.credit,
            bodies,
            body_array,
        };
        owner.push(
            original,
            EVENT_TYPES_CONFIG_KEY,
            &OwnedEventSequence {
                values: definitions.event_types.values(),
            },
        )?;
        owner.push(
            original,
            SINKS_CONFIG_KEY,
            &OwnedEventSequence {
                values: definitions.sinks.values(),
            },
        )?;
        owner.push(
            original,
            ROUTES_CONFIG_KEY,
            &OwnedEventSequence {
                values: definitions.routes.values(),
            },
        )?;
        Ok(owner)
    }
    fn push<T: Serialize>(
        &mut self,
        original: &OwnedMemoryReservation,
        key: &str,
        value: &T,
    ) -> Result<()> {
        let (name, name_memory) = copy_owned_native_string(original, key)?;
        let name = OwnedEventBody {
            body: name,
            credit: name_memory,
        };
        let encoded = encode_owned_event_value(original, value)?;
        self.values.push((name.body, encoded.body));
        self.bodies.push(name.credit);
        self.bodies.push(encoded.credit);
        Ok(())
    }
}
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum OwnedDefinitionMode {
    Additive,
    Replace,
}
pub(super) struct OwnedDefinitionPublication {
    projection: OwnedDefinitionsProjection,
    pub(super) config: OwnedEncodedEventBusConfig,
    names: OwnedEventBody<Vec<String>, OwnedMemoryReservation>,
    keys: OwnedEventBody<Vec<OwnedMemoryReservation>, OwnedMemoryReservation>,
    slots: OwnedSinkQueuePublication,
    mode: OwnedDefinitionMode,
    // One paid move-out cell suffices for arbitrarily many hidden slots.
    // Publication never allocates a retirement vector or copies a map key.
    retirement: OwnedEventBody<Option<(String, OwnedSinkQueue)>, Option<OwnedMemoryReservation>>,
}
impl OwnedEventBusHandle {
    pub(super) fn prepare_owned_definition_publication(
        &self,
        projection: OwnedDefinitionsProjection,
        mode: OwnedDefinitionMode,
        lease: QueueMutationLease,
    ) -> Result<OwnedDefinitionPublication> {
        self.prepare_owned_definition_publication_with_extra_config(projection, mode, lease, 0)
    }
    fn prepare_owned_definition_publication_with_extra_config(
        &self,
        projection: OwnedDefinitionsProjection,
        mode: OwnedDefinitionMode,
        lease: QueueMutationLease,
        extra: usize,
    ) -> Result<OwnedDefinitionPublication> {
        if !Arc::ptr_eq(&lease.state, &self.data().common.queue_mutation_state) {
            return Err(Error::Other(
                "definition lease belongs to another event bus".into(),
            ));
        }
        let original = &self.data().original;
        let config =
            OwnedEncodedEventBusConfig::try_new_capacity(original, &projection.definitions, extra)?;
        let (names, memory) = new_owned_native_vec(original, projection.definitions.sinks.len())?;
        let names = OwnedEventBody {
            body: names,
            credit: memory,
        };
        let (keys, memory) = new_owned_native_vec(original, projection.definitions.sinks.len())?;
        let keys = OwnedEventBody {
            body: keys,
            credit: memory,
        };
        let mut copied = OwnedEventNames {
            names: names.body,
            keys: keys.body,
            name_array: names.credit,
            key_array: keys.credit,
        };
        for sink in projection.definitions.sinks.keys() {
            let (key, credit) = copy_owned_native_string(original, sink)?;
            copied.names.push(key);
            copied.keys.push(credit);
        }
        // Admit the move-out control before any publication; the removed key,
        // ring and entries keep their original invoices until physical drop.
        let retirement = OwnedEventBody {
            body: None,
            credit: if mode == OwnedDefinitionMode::Replace {
                Some(reserve_owned_event_bytes(
                    original,
                    std::mem::size_of::<Option<(String, OwnedSinkQueue)>>(),
                )?)
            } else {
                None
            },
        };
        self.prepare_owned_metrics_slots(&copied.names)?;
        let slots = self.reserve_owned_queue_names(&[], &copied.names, lease)?;
        Ok(OwnedDefinitionPublication {
            projection,
            config,
            names: OwnedEventBody {
                body: copied.names,
                credit: copied.name_array,
            },
            keys: OwnedEventBody {
                body: copied.keys,
                credit: copied.key_array,
            },
            slots,
            mode,
            retirement,
        })
    }
    pub(super) fn publish_owned_definition_publication(
        &self,
        publication: OwnedDefinitionPublication,
        events: OwnedPreparedSinkEvents,
    ) {
        // The event owner, even empty, retains a genuine normal lease while
        // definition-only publication releases its own reservation.
        self.publish_owned_definition_only(publication);
        self.publish_owned_sink_events_to_memory(events);
    }
    pub(super) fn publish_owned_definition_only(
        &self,
        mut publication: OwnedDefinitionPublication,
    ) {
        let old_definitions;
        let old_credit;
        {
            let mut definitions = self.data().common.definitions.lock();
            let mut credit = self.data().definition_credit.lock();
            old_definitions =
                std::mem::replace(&mut *definitions, publication.projection.definitions);
            old_credit = credit.replace(publication.projection.allocation);
        }
        // Physical old declarations precede their invoice, outside all guards.
        drop(old_definitions);
        drop(old_credit);
        if publication.mode == OwnedDefinitionMode::Replace {
            self.data()
                .common
                .callbacks
                .write()
                .retain(|sink, _| publication.names.body.iter().any(|name| name == sink));
        }
        let mut queues = self.data().queues.lock();
        while queues.values.values().any(|queue| queue.mutation.is_some()) {
            self.data().queue_claim_waiters.wait(&mut queues);
        }
        if publication.mode == OwnedDefinitionMode::Replace {
            for (sink, queue) in &mut queues.values {
                if !publication.names.body.iter().any(|name| name == sink) {
                    // Per-entry owners contain no cancellation token; capacity
                    // and key invoices remain with the actual paid ring/slot.
                    queue.retiring = queue.entries.len();
                    queue.present = false;
                }
            }
        }
        for name in &publication.names.body {
            queues
                .values
                .get_mut(name)
                .expect("paid definition sink slot")
                .present = true;
        }
        for slot in &publication.slots.slots {
            queues
                .values
                .get_mut(&slot.sink)
                .expect("paid definition reservation")
                .reservations -= 1;
        }
        publication.slots.state = OwnedEscrowState::Published;
        drop(queues);
        if publication.mode == OwnedDefinitionMode::Replace {
            // Check after this publication releases its reservations, too. A
            // competing terminal may retain P or a zero-count reservation;
            // its cancellation owns the later last-reservation retirement.
            // Hidden-slot removal has no observable queue contents. Recheck
            // eligibility under the mutex on every move; drop physical bodies
            // outside it, before their existing ring/key invoices.
            loop {
                // Whole paid queues retire intact when no terminal retains a
                // reservation. Otherwise retain key/ring and drain one entry
                // per critical section into a stack cell; no new buffer.
                let retired_entry;
                {
                    let mut queues = self.data().queues.lock();
                    publication.retirement.body = queues
                        .values
                        .extract_if(|_, queue| {
                            !queue.present && queue.pending == 0 && queue.reservations == 0
                        })
                        .next();
                    retired_entry = if publication.retirement.body.is_none() {
                        queues
                            .values
                            .values_mut()
                            .find(|q| q.retiring != 0)
                            .and_then(|q| {
                                q.retiring -= 1;
                                q.entries.pop_front()
                            })
                    } else {
                        None
                    };
                }
                let finished = publication.retirement.body.is_none() && retired_entry.is_none();
                drop(publication.retirement.body.take());
                drop(retired_entry);
                if finished {
                    break;
                }
            }
        }
        // The received lease remains held through both map change and append,
        // including when events is empty. Additive never prunes old queues.
        // Projection fields have moved into the live definitions. Drop each
        // remaining field in its original order before notifying waiters.
        drop(publication.config);
        drop(publication.names);
        drop(publication.keys);
        drop(publication.slots);
        drop(publication.retirement);
        self.data().common.waiters.notify_all();
    }
    pub(super) fn prepare_owned_event_bus_without_table(
        &self,
        table: &str,
    ) -> Result<Option<OwnedDefinitionPublication>> {
        let projection = {
            let definitions = self.data().common.definitions.lock();
            if !definitions
                .event_types
                .values()
                .any(|def| def.table == table)
            {
                return Ok(None);
            }
            OwnedDefinitionsProjection::try_copy_filtered(
                &self.data().original,
                &definitions,
                Some(table),
            )?
        };
        let lease = self.data().common.begin_queue_mutation();
        self.prepare_owned_definition_publication(projection, OwnedDefinitionMode::Additive, lease)
            .map(Some)
    }
    pub(super) fn export_owned_event_bus_config_values(
        &self,
    ) -> Result<OwnedEncodedEventBusConfig> {
        let definitions = self.data().common.definitions.lock();
        OwnedEncodedEventBusConfig::try_new(&self.data().original, &definitions)
    }
}
impl Drop for OwnedEventBusStorage {
    fn drop(&mut self) {
        // Exclusive Owned assembly: native bodies in common cannot survive the
        // companion's invoices even if a coordination Arc outlives it.
        let definitions = std::mem::take(&mut *self.common.definitions.lock());
        let credit = self.definition_credit.get_mut().take();
        drop(definitions);
        drop(credit);
        let checkouts = std::mem::take(&mut self.common.queue_mutation_state.lock().checked_out);
        let checkout_credit = self.checkout_allocation.get_mut().take();
        drop(checkouts);
        drop(checkout_credit);
        let metrics = std::mem::take(&mut *self.common.metrics.lock());
        let metric_credit = self.metric_allocation.get_mut().take();
        drop(metrics);
        drop(metric_credit);
        let unloaded = std::mem::take(&mut *self.common.unloaded_queue_depths.lock());
        let unloaded_credit = self.unloaded_allocation.get_mut().take();
        drop(unloaded);
        drop(unloaded_credit);
    }
}

fn owned_context_scope_allows(
    access: &AccessConstraints,
    gate: &EventGateSnapshot,
    values: &HashMap<String, Value>,
) -> bool {
    if let (Some(contexts), Some(column)) = (&access.contexts, &gate.context_column)
        && !matches!(values.get(column), Some(Value::Uuid(context)) if contexts.contains(&ContextId::new(*context)))
    {
        return false;
    }
    if let (Some(labels), Some(scope)) = (&access.scope_labels, &gate.scope_read)
        && !matches!(values.get(&scope.column), Some(Value::Text(label))
            if scope.read_labels.iter().any(|read| read == label) && labels.iter().any(|allowed| allowed.0 == *label))
    {
        return false;
    }
    true
}
fn owned_event_visible(access: &AccessConstraints, entry: &SinkQueueEntry) -> bool {
    if !owned_context_scope_allows(access, &entry.gate, &entry.event.row_values) {
        return false;
    }
    let Some(acl) = &entry.gate.acl else {
        return true;
    };
    let Some(principal) = &access.principal else {
        return access.contexts.is_none() && access.scope_labels.is_none();
    };
    let (kind, id) = match principal {
        Principal::System => return false,
        Principal::Agent(id) => ("Agent", id.as_str()),
        Principal::Human(id) => ("Human", id.as_str()),
    };
    let Some(Value::Uuid(acl_id)) = entry.event.row_values.get(&acl.column) else {
        return false;
    };
    acl.grant_rows.iter().any(|grant| {
        owned_context_scope_allows(access, &acl.grant_gate, grant)
            && matches!(grant.get("principal_kind"), Some(Value::Text(value)) if value == kind)
            && matches!(grant.get("principal_id"), Some(Value::Text(value)) if value == id)
            && grant.get(&acl.ref_column) == Some(&Value::Uuid(*acl_id))
    })
}
struct OwnedCheckoutKeyCredit {
    name: OwnedEventShared<OwnedEventBody<String, OwnedMemoryReservation>>,
    id: u64,
    memory: OwnedMemoryReservation,
}
struct OwnedCheckoutAddition {
    key: Option<(String, u64)>,
    credit: Option<OwnedCheckoutKeyCredit>,
}
pub(super) struct OwnedCheckoutAllocation {
    keys: Vec<OwnedCheckoutKeyCredit>,
    array: OwnedMemoryReservation,
    buckets: OwnedMemoryReservation,
}
pub(super) struct OwnedCheckoutLookup {
    name: OwnedEventShared<OwnedEventBody<String, OwnedMemoryReservation>>,
    source: OwnedEventShared<OwnedSinkQueueEntry>,
    key: (String, u64),
    memory: OwnedMemoryReservation,
    finished: Cell<bool>,
}
pub(super) struct OwnedSinkDispatchBatch {
    pub(super) entries: Vec<OwnedSinkQueueEntry>,
    pub(super) lookups: Vec<OwnedCheckoutLookup>,
    pub(super) acked: Vec<u64>,
    removable: Vec<u64>,
    retired: Vec<OwnedEventShared<OwnedSinkQueueEntry>>,
    entry_array: OwnedMemoryReservation,
    lookup_array: OwnedMemoryReservation,
    ack_array: OwnedMemoryReservation,
    removal_array: OwnedMemoryReservation,
    retired_array: OwnedMemoryReservation,
    storage: OwnedEventBusWeak,
    denied_rotation: bool,
    // Paid before checkout: explicit pre-delivery failures keep the already
    // delivered prefix registered, exactly as the original worker does.
    cleanup_from: OwnedEventBody<usize, OwnedMemoryReservation>,
}
impl Drop for OwnedSinkDispatchBatch {
    fn drop(&mut self) {
        if let Some(storage) = self.storage.upgrade() {
            for lookup in self.lookups.iter().skip(self.cleanup_from.body) {
                storage.finish_owned_checked_out(lookup);
            }
        }
    }
}
// This stack token and the queue's inline source-pin cell are paid from the
// companion original. No new map or provider operation runs under a guard.
struct OwnedQueueMutationClaim {
    storage: OwnedEventBusHandle,
    name: OwnedEventShared<OwnedEventBody<String, OwnedMemoryReservation>>,
    source: OwnedEventShared<OwnedSinkQueueEntry>,
    active: bool,
    control: OwnedMemoryReservation,
}
impl Drop for OwnedQueueMutationClaim {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let retired = {
            let mut queues = self.storage.data().queues.lock();
            queues.values.get_mut(&self.name.body).and_then(|queue| {
                let exact = queue.mutation.as_ref().is_some_and(|source| {
                    Arc::ptr_eq(
                        source.data.as_ref().expect("claimed source"),
                        self.source.data.as_ref().expect("claim generation"),
                    )
                });
                if exact { queue.mutation.take() } else { None }
            })
        };
        self.active = false;
        // Wake before any original body/control refund can unwind.
        self.storage.data().queue_claim_waiters.notify_all();
        drop(retired);
    }
}
// Only the caller retaining the authenticated source claim can mint this
// borrowed token. Marking it performs one stack Cell store: no allocation,
// provider, queue callback, source retirement or lock acquisition.
pub(crate) struct OwnedQueueCommitToken<'a> {
    committed: &'a Cell<bool>,
    _source: &'a OwnedQueueMutationClaim,
}
impl OwnedQueueMutationClaim {
    fn commit_token<'a>(&'a self, committed: &'a Cell<bool>) -> OwnedQueueCommitToken<'a> {
        assert!(self.active);
        OwnedQueueCommitToken {
            committed,
            _source: self,
        }
    }
}
impl OwnedQueueCommitToken<'_> {
    pub(crate) fn mark_success(&self) {
        self.committed.set(true);
    }
}

// The error itself precedes its original invoices in physical drop order.
// The existing maintenance Result borrows this admitted slot; no new public
// Error variant or changed worker diagnostic is needed. Lower error producer
// allocations are NOT certified by the slot's concrete control admission.
pub(crate) struct OwnedNativeQueueFailure {
    error: Option<Error>,
    control: Option<OwnedMemoryReservation>,
    diagnostic: Option<OwnedMemoryReservation>,
}
impl std::fmt::Display for OwnedNativeQueueFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(
            self.error
                .as_ref()
                .expect("native failure carries original error"),
            f,
        )
    }
}
impl OwnedNativeQueueFailure {
    fn prepare(original: &OwnedMemoryReservation) -> Self {
        let failure = match reserve_owned_event_bytes(original, std::mem::size_of::<Self>()) {
            Ok(control) => Self {
                error: None,
                control: Some(control),
                diagnostic: None,
            },
            // No successor exists on an original admission refusal. Preserve
            // that exact refusal rather than allocating a replacement error.
            Err(error) => {
                return Self {
                    error: Some(error),
                    control: None,
                    diagnostic: None,
                };
            }
        };
        #[cfg(any(test, feature = "test-seams"))]
        {
            let mut failure = failure;
            match reserve_owned_event_bytes(original, "injected currency cleanup failure".len()) {
                Ok(credit) => failure.diagnostic = Some(credit),
                Err(error) => failure.error = Some(error),
            }
            failure
        }
        #[cfg(not(any(test, feature = "test-seams")))]
        {
            failure
        }
    }
    pub(crate) fn capture(&mut self, result: Result<()>) {
        if let Err(error) = result {
            assert!(self.error.is_none(), "one original native error loan");
            self.error = Some(error);
        }
    }
    // The slot/invoices remain outside the maintenance guard, through this
    // original Result's error loan and the final transfer back into the slot.
    fn loan_result<T>(&mut self, value: T) -> Result<T> {
        match self.error.take() {
            Some(error) => Err(error),
            None => Ok(value),
        }
    }
    #[allow(
        clippy::result_large_err,
        reason = "the failure carries its own funded reservations; boxing it would allocate outside them"
    )]
    fn finish<T>(mut self, result: Result<T>) -> std::result::Result<T, Self> {
        match result {
            Err(error) => {
                self.capture(Err(error));
                Err(self)
            }
            Ok(value) if self.error.is_none() => Ok(value),
            Ok(_) => Err(self),
        }
    }
}

// Both completion guards independently observe the same caller-owned marker.
// Receipt is constructed last, so its unwind precedes memory publication.
// Setting settled first prevents a second callback during callback unwind.
struct OwnedQueuePublicationCompletion<'a, F: FnMut()> {
    publish: F,
    committed: &'a Cell<bool>,
    settled: bool,
    control: OwnedMemoryReservation,
}
impl<F: FnMut()> OwnedQueuePublicationCompletion<'_, F> {
    fn finish(&mut self) {
        if self.committed.get() && !std::mem::replace(&mut self.settled, true) {
            (self.publish)();
        }
    }
}
impl<F: FnMut()> Drop for OwnedQueuePublicationCompletion<'_, F> {
    fn drop(&mut self) {
        self.finish();
    }
}
fn prepare_owned_queue_completion<'a, F: FnMut()>(
    original: &OwnedMemoryReservation,
    committed: &'a Cell<bool>,
    publish: F,
) -> Result<OwnedQueuePublicationCompletion<'a, F>> {
    let control = reserve_owned_event_bytes(
        original,
        std::mem::size_of::<OwnedQueuePublicationCompletion<'a, F>>(),
    )?;
    Ok(OwnedQueuePublicationCompletion {
        publish,
        committed,
        settled: false,
        control,
    })
}
struct OwnedQueueReceiptCompletion<'a, F: FnMut()> {
    complete: F,
    committed: &'a Cell<bool>,
    settled: bool,
    control: OwnedMemoryReservation,
}
impl<F: FnMut()> OwnedQueueReceiptCompletion<'_, F> {
    fn finish(&mut self) {
        if self.committed.get() && !std::mem::replace(&mut self.settled, true) {
            (self.complete)();
        }
    }
}
impl<F: FnMut()> Drop for OwnedQueueReceiptCompletion<'_, F> {
    fn drop(&mut self) {
        self.finish();
    }
}
fn prepare_owned_queue_receipt<'a, F: FnMut()>(
    original: &OwnedMemoryReservation,
    committed: &'a Cell<bool>,
    complete: F,
) -> Result<OwnedQueueReceiptCompletion<'a, F>> {
    let control = reserve_owned_event_bytes(
        original,
        std::mem::size_of::<OwnedQueueReceiptCompletion<'a, F>>(),
    )?;
    Ok(OwnedQueueReceiptCompletion {
        complete,
        committed,
        settled: false,
        control,
    })
}
impl OwnedEventBusHandle {
    fn claim_owned_checked_out(
        &self,
        sink: &str,
        lookup: &OwnedCheckoutLookup,
    ) -> Result<OwnedQueueMutationClaim> {
        let control = reserve_owned_event_bytes(
            &self.data().original,
            std::mem::size_of::<OwnedQueueMutationClaim>(),
        )?;
        let mut claim = OwnedQueueMutationClaim {
            storage: self.clone(),
            name: lookup.name.clone(),
            source: lookup.source.clone(),
            active: false,
            control,
        };
        loop {
            let state = self.data().common.queue_mutation_state.lock();
            let mut queues = self.data().queues.lock();
            let current = !lookup.finished.get()
                && lookup.key.0 == sink
                && lookup.name.body == sink
                && state.checked_out.contains(&lookup.key)
                && queues.values.get(sink).is_some_and(|queue| {
                    queue.present
                        && queue.retiring == 0
                        && Arc::ptr_eq(
                            queue.name.data.as_ref().expect("queue lifecycle"),
                            lookup.name.data.as_ref().expect("checkout lifecycle"),
                        )
                        && queue.entries.iter().any(|source| {
                            Arc::ptr_eq(
                                source.data.as_ref().expect("queue source"),
                                lookup.source.data.as_ref().expect("checkout generation"),
                            )
                        })
                });
            if !current {
                drop(queues);
                drop(state);
                return Err(Error::Other(format!(
                    "pending sink action disappeared: {}",
                    lookup.key.1
                )));
            }
            if queues
                .values
                .get(sink)
                .expect("checked queue")
                .mutation
                .is_some()
            {
                drop(state);
                // Condvar atomically unlocks this queue guard; no missed wake.
                self.data().queue_claim_waiters.wait(&mut queues);
                drop(queues);
                continue;
            }
            queues.values.get_mut(sink).expect("checked queue").mutation =
                Some(claim.source.clone());
            drop(queues);
            drop(state);
            claim.active = true;
            return Ok(claim);
        }
    }
    fn empty_owned_dispatch_batch(&self, maximum: usize) -> Result<OwnedSinkDispatchBatch> {
        let original = &self.data().original;
        let cleanup_from = OwnedEventBody {
            body: 0,
            credit: reserve_owned_event_bytes(original, std::mem::size_of::<usize>())?,
        };
        let (body, credit) = new_owned_native_vec(original, maximum)?;
        let entries = OwnedEventBody { body, credit };
        let (body, credit) = new_owned_native_vec(original, maximum)?;
        let lookups = OwnedEventBody { body, credit };
        let (body, credit) = new_owned_native_vec(original, maximum)?;
        let acked = OwnedEventBody { body, credit };
        let (body, credit) = new_owned_native_vec(original, maximum)?;
        let removable = OwnedEventBody { body, credit };
        let (body, credit) = new_owned_native_vec(original, maximum)?;
        let retired = OwnedEventBody { body, credit };
        Ok(OwnedSinkDispatchBatch {
            entries: entries.body,
            entry_array: entries.credit,
            lookups: lookups.body,
            lookup_array: lookups.credit,
            acked: acked.body,
            ack_array: acked.credit,
            removable: removable.body,
            removal_array: removable.credit,
            retired: retired.body,
            retired_array: retired.credit,
            storage: self.downgrade(),
            denied_rotation: false,
            cleanup_from,
        })
    }
    pub(super) fn checkout_owned_sink_batch(
        &self,
        sink: &str,
        access: &AccessConstraints,
        maximum: usize,
    ) -> Result<OwnedSinkDispatchBatch> {
        let maximum = maximum.min(SINK_DISPATCH_BATCH);
        loop {
            let mut batch = self.empty_owned_dispatch_batch(maximum)?;
            let (body, credit) = new_owned_native_vec(&self.data().original, maximum)?;
            let mut pins: OwnedEventBody<
                Vec<(
                    OwnedEventShared<OwnedSinkQueueEntry>,
                    OwnedEventShared<OwnedEventBody<String, OwnedMemoryReservation>>,
                )>,
                _,
            > = OwnedEventBody { body, credit };
            {
                let state = self.data().common.queue_checkout_guard();
                let mut queues = self.data().queues.lock();
                let Some(queue) = queues
                    .values
                    .get_mut(sink)
                    .filter(|q| q.present && q.retiring == 0)
                else {
                    return Ok(batch);
                };
                let Some(front) = queue.entries.front() else {
                    return Ok(batch);
                };
                if !owned_event_visible(access, &front.entry) {
                    if queue.mutation.is_some() {
                        drop(queues);
                        drop(state);
                        let mut queues = self.data().queues.lock();
                        while queues
                            .values
                            .get(sink)
                            .is_some_and(|q| q.mutation.is_some())
                        {
                            self.data().queue_claim_waiters.wait(&mut queues);
                        }
                        drop(queues);
                        continue;
                    }
                    if let Some(entry) = queue.entries.pop_front() {
                        queue.entries.push_back(entry);
                    }
                    drop(queues);
                    drop(state);
                    self.record_owned_batch_outcome(sink, 0, 0, 1, 0, 0);
                    self.data().common.waiters.notify_all();
                    batch.denied_rotation = true;
                    return Ok(batch);
                }
                for entry in queue
                    .entries
                    .iter()
                    .take(maximum)
                    .take_while(|entry| owned_event_visible(access, &entry.entry))
                {
                    pins.body.push((entry.clone(), queue.name.clone()));
                }
            }
            // Every deep copy and key constructor sees a retained paid pin,
            // outside both guards. Helper-local unwind now retires unlocked.
            for (source, name) in pins.body.drain(..) {
                let copied =
                    copy_owned_sink_queue_entry(&source.get().credit.metadata, &source.entry)?;
                let (key, memory) = copy_owned_native_string(&self.data().original, sink)?;
                batch.entries.push(copied);
                batch.lookups.push(OwnedCheckoutLookup {
                    key: (key, source.entry.id),
                    memory,
                    source,
                    name,
                    finished: Cell::new(true),
                });
            }
            loop {
                let (members, credits) = {
                    let state = self.data().common.queue_checkout_guard();
                    let credits = self
                        .data()
                        .checkout_allocation
                        .lock()
                        .as_ref()
                        .map_or(0, |c| c.keys.len());
                    (state.checked_out.len(), credits)
                };
                let capacity = owned_event_add(members, batch.lookups.len())?;
                let (body, buckets) = new_owned_native_set(&self.data().original, capacity)?;
                let (keys, array) = new_owned_native_vec(
                    &self.data().original,
                    owned_event_add(credits, batch.lookups.len())?,
                )?;
                let mut copied = OwnedEventCopiedSet {
                    body,
                    keys,
                    array,
                    buckets,
                };
                let (body, credit) =
                    new_owned_native_vec(&self.data().original, batch.lookups.len())?;
                let mut additions: OwnedEventBody<Vec<OwnedCheckoutAddition>, _> =
                    OwnedEventBody { body, credit };
                for lookup in &batch.lookups {
                    let (name, credit) = copy_owned_native_string(&self.data().original, sink)?;
                    additions.body.push(OwnedCheckoutAddition {
                        key: Some((name, lookup.key.1)),
                        credit: Some(OwnedCheckoutKeyCredit {
                            name: lookup.name.clone(),
                            id: lookup.key.1,
                            memory: credit,
                        }),
                    });
                }
                let mut retirement = OwnedEventBody {
                    body: None,
                    credit: None,
                };
                let mut state = self.data().common.queue_checkout_guard();
                let queues = self.data().queues.lock();
                let unchanged = queues
                    .values
                    .get(sink)
                    .filter(|q| q.present && q.retiring == 0)
                    .is_some_and(|q| {
                        q.entries.len() >= batch.lookups.len()
                            && q.entries.iter().zip(&batch.lookups).all(|(entry, lookup)| {
                                Arc::ptr_eq(
                                    entry.data.as_ref().expect("live queue pin"),
                                    lookup.source.data.as_ref().expect("captured queue pin"),
                                )
                            })
                    });
                if !unchanged {
                    drop(queues);
                    drop(state);
                    break;
                }
                let mut credit_guard = self.data().checkout_allocation.lock();
                let current_credits = credit_guard.as_ref().map_or(0, |c| c.keys.len());
                if state.checked_out.len() + batch.lookups.len() > copied.body.capacity()
                    || current_credits + batch.lookups.len() > copied.keys.capacity()
                {
                    drop(credit_guard);
                    drop(queues);
                    drop(state);
                    continue;
                }
                // Move existing keys with their actual old invoices. No copied
                // set or key is destroyed under state/queue guards, even on unwind.
                for key in state.checked_out.drain() {
                    copied.body.insert(key);
                }
                for addition in &mut additions.body {
                    if !copied
                        .body
                        .contains(addition.key.as_ref().expect("prepared checkout key"))
                    {
                        copied
                            .body
                            .insert(addition.key.take().expect("prepared checkout key"));
                        copied
                            .keys
                            .push(addition.credit.take().expect("prepared key invoice"));
                    }
                }
                retirement.body = Some(std::mem::replace(&mut state.checked_out, copied.body));
                retirement.credit = credit_guard.take();
                if let Some(old) = &mut retirement.credit {
                    copied.keys.append(&mut old.keys);
                }
                *credit_guard = Some(OwnedCheckoutAllocation {
                    keys: std::mem::take(&mut copied.keys),
                    array: copied.array,
                    buckets: copied.buckets,
                });
                for lookup in &batch.lookups {
                    lookup.finished.set(false);
                }
                drop(credit_guard);
                drop(queues);
                drop(state);
                drop(retirement);
                return Ok(batch);
            }
            // A purge/replacement changed the snapshot before registration.
            // All partial owners retire unlocked; capture a new actual batch.
        }
    }
    pub(super) fn finish_owned_checked_out(&self, lookup: &OwnedCheckoutLookup) {
        if lookup.finished.replace(true) {
            return;
        }
        let mut retired = OwnedEventBody {
            body: None,
            credit: None,
        };
        {
            let mut state = self.data().common.queue_mutation_state.lock();
            retired.body = state.checked_out.take(&lookup.key);
            if retired.body.is_some() {
                let mut allocation = self.data().checkout_allocation.lock();
                if let Some(allocation) = &mut *allocation
                    && let Some(index) = allocation.keys.iter().position(|credit| {
                        credit.id == lookup.key.1 && credit.name.body == lookup.key.0
                    })
                {
                    retired.credit = Some(allocation.keys.swap_remove(index));
                }
            }
        }
        // Field order also covers unwinding: key bytes before their invoice.
        drop(retired);
        self.data().common.queue_mutation_waiters.notify_all();
    }
    fn record_owned_batch_outcome(
        &self,
        sink: &str,
        delivered: u64,
        failures: u64,
        examined: u64,
        retried: u64,
        uncertain: u64,
    ) {
        let mut metrics = self.data().common.metrics.lock();
        let state = metrics
            .get_mut(sink)
            .expect("owning sink metrics prepared before worker");
        state.delivered += delivered;
        state.permanent_failures += failures;
        state.examined += examined;
        state.retried += retried;
        state.uncertain += uncertain;
    }
    #[allow(
        clippy::result_large_err,
        reason = "the failure carries its own funded reservations; boxing it would allocate outside them"
    )]
    pub(super) fn transition_owned_sink_attempt<P: OwnedSinkQueuePersistence>(
        &self,
        persistence: &P,
        tx_mgr: &DatabaseTransactionHost,
        sink: &str,
        lookup: &mut OwnedCheckoutLookup,
        start: bool,
        uncertain: bool,
    ) -> std::result::Result<u32, OwnedNativeQueueFailure> {
        let mut failure = OwnedNativeQueueFailure::prepare(&self.data().original);
        if failure.error.is_some() {
            return Err(failure);
        }
        let result = tx_mgr.with_maintenance_publication(|publication| {
            let _lease = self.data().common.begin_dispatcher_queue_mutation()?;
            let entry_id = lookup.key.1;
            let _claim = self.claim_owned_checked_out(sink, lookup)?;
            // The lookup pins the ACTUAL checked-out entry, including its
            // original invoice. No new root/key lookup chooses a funding seed.
            let initially_current = {
                let state = self.data().common.queue_mutation_state.lock();
                let queues = self.data().queues.lock();
                !lookup.finished.get()
                    && state.checked_out.contains(&lookup.key)
                    && queues.values.get(sink).is_some_and(|q| {
                        q.entries.iter().skip(q.retiring).any(|entry| {
                            Arc::ptr_eq(
                                entry.data.as_ref().expect("live queue entry"),
                                lookup.source.data.as_ref().expect("checked-out source"),
                            )
                        })
                    })
            };
            if !initially_current {
                return Err(Error::Other(format!(
                    "pending sink action disappeared: {entry_id}"
                )));
            }
            let source = &lookup.source;
            let mut updated =
                copy_owned_sink_queue_entry(&source.get().credit.metadata, &source.entry)?;
            let attempts = updated.entry.attempts & ATTEMPT_COUNT_MASK;
            updated.entry.attempts = if start {
                if updated.entry.attempts & UNCONFIRMED_ATTEMPT != 0 {
                    return Err(Error::Other(format!(
                        "sink action has an unresolved attempt: {entry_id}"
                    )));
                }
                attempts.saturating_add(1).min(ATTEMPT_COUNT_MASK) | UNCONFIRMED_ATTEMPT
            } else {
                attempts
            };
            let was_unconfirmed = source.entry.attempts & UNCONFIRMED_ATTEMPT != 0;
            let delta = if start {
                1
            } else if was_unconfirmed {
                -1
            } else {
                0
            };
            let encoded = encode_owned_event_value(&source.get().credit.metadata, &updated.entry)?;
            let successor = OwnedEventShared::try_new(
                updated,
                &source.get().credit.metadata,
                &self.data().funder,
            )?;
            let prepared = persistence.prepare_update(
                &self.data().original,
                sink,
                entry_id,
                &encoded.body,
                uncertain,
                delta,
            )?;
            let current = {
                let state = self.data().common.queue_mutation_state.lock();
                let queues = self.data().queues.lock();
                !lookup.finished.get()
                    && state.checked_out.contains(&lookup.key)
                    && queues.values.get(sink).is_some_and(|q| {
                        q.entries.iter().skip(q.retiring).any(|entry| {
                            Arc::ptr_eq(
                                entry.data.as_ref().expect("live queue entry"),
                                source.data.as_ref().expect("checked-out source"),
                            )
                        })
                    })
            };
            if !current {
                return Err(Error::Other(format!(
                    "pending sink action disappeared: {entry_id}"
                )));
            }
            // The real claim keeps this pointer generation installed through
            // commit and completion. Pre-admit the unwind publication control.
            let source = &_claim.source;
            let mut successor = Some(successor);
            let committed = Cell::new(false);
            let mut completion =
                prepare_owned_queue_completion(&self.data().original, &committed, || {
                    let next = successor.take().expect("one attempt successor");
                    let retired;
                    {
                        let mut queues = self.data().queues.lock();
                        let entry = queues
                            .values
                            .get_mut(sink)
                            .and_then(|q| {
                                q.entries.iter_mut().find(|entry| {
                                    Arc::ptr_eq(
                                        entry.data.as_ref().expect("live queue entry"),
                                        source.data.as_ref().expect("claimed source"),
                                    )
                                })
                            })
                            .expect("claimed attempt source remains installed");
                        retired = std::mem::replace(entry, next.clone());
                    }
                    let old_lookup = std::mem::replace(&mut lookup.source, next);
                    {
                        let mut metrics = self.data().common.metrics.lock();
                        let state = metrics.get_mut(sink).expect("paid owning sink metrics");
                        if start {
                            state.unconfirmed += 1;
                        } else if was_unconfirmed {
                            state.unconfirmed = state.unconfirmed.saturating_sub(1);
                        }
                    }
                    // Durable and in-memory bodies already agree before any old
                    // original/control refund can panic.
                    drop(old_lookup);
                    drop(retired);
                })?;
            let durable = persistence.is_durable();
            if durable {
                publication.reserve()?;
            }
            let mut receipt =
                prepare_owned_queue_receipt(&self.data().original, &committed, || {
                    if durable {
                        publication.complete();
                    }
                })?;
            let token = _claim.commit_token(&committed);
            persistence.commit_update_at_boundary(prepared, &token, &mut failure);
            // A checkpoint diagnostic can report Err after mark. Settle both
            // original publications before lending that diagnostic to Result.
            receipt.finish();
            completion.finish();
            failure.loan_result(attempts.max(1))
        });
        failure.finish(result)
    }
    #[allow(
        clippy::result_large_err,
        reason = "the failure carries its own funded reservations; boxing it would allocate outside them"
    )]
    pub(super) fn ack_owned_front_entries<P: OwnedSinkQueuePersistence>(
        &self,
        persistence: &P,
        tx_mgr: &DatabaseTransactionHost,
        sink: &str,
        batch: &mut OwnedSinkDispatchBatch,
    ) -> std::result::Result<(), OwnedNativeQueueFailure> {
        if batch.acked.is_empty() {
            return Ok(());
        }
        let mut failure = OwnedNativeQueueFailure::prepare(&self.data().original);
        if failure.error.is_some() {
            return Err(failure);
        }
        let result = tx_mgr.with_maintenance_publication(|publication| {
            let _lease = self.data().common.begin_dispatcher_queue_mutation()?;
            batch.removable.clear();
            // Qualify the actual first source before preparing any durable ack.
            // An absent prefix retains the original no-op behavior; an equal
            // ID with a different source generation is an ordinary error.
            let first = batch.acked[0];
            let first_lookup = batch
                .lookups
                .iter()
                .find(|lookup| lookup.key.1 == first)
                .ok_or_else(|| Error::Other(format!("pending sink action disappeared: {first}")))?;
            let (front_id_matches, front_matches) = {
                let queues = self.data().queues.lock();
                queues
                    .values
                    .get(sink)
                    .filter(|q| q.retiring == 0)
                    .and_then(|q| q.entries.front())
                    .map_or((false, false), |entry| {
                        (
                            entry.entry.id == first,
                            entry.entry.id == first
                                && Arc::ptr_eq(
                                    entry.data.as_ref().expect("ack front source"),
                                    first_lookup
                                        .source
                                        .data
                                        .as_ref()
                                        .expect("ack checkout generation"),
                                ),
                        )
                    })
            };
            if !front_id_matches {
                return Ok(());
            }
            if !front_matches {
                return Err(Error::Other(format!(
                    "pending sink action disappeared: {first}"
                )));
            }
            let _claim = self.claim_owned_checked_out(sink, first_lookup)?;
            let unconfirmed;
            let mut stale = None;
            {
                let queues = self.data().queues.lock();
                if let Some(queue) = queues.values.get(sink).filter(|q| q.retiring == 0) {
                    for (id, entry) in batch.acked.iter().zip(&queue.entries) {
                        let exact = batch
                            .lookups
                            .iter()
                            .find(|lookup| lookup.key.1 == *id)
                            .is_some_and(|lookup| {
                                !lookup.finished.get()
                                    && Arc::ptr_eq(
                                        entry.data.as_ref().expect("ack source"),
                                        lookup.source.data.as_ref().expect("ack generation"),
                                    )
                            });
                        if *id != entry.entry.id {
                            break;
                        }
                        if !exact {
                            stale = Some(*id);
                            break;
                        }
                        batch.removable.push(*id);
                    }
                    unconfirmed = queue
                        .entries
                        .iter()
                        .take(batch.removable.len())
                        .filter(|entry| entry.entry.attempts & UNCONFIRMED_ATTEMPT != 0)
                        .count() as u64;
                } else {
                    unconfirmed = 0;
                }
            }
            if let Some(id) = stale {
                return Err(Error::Other(format!(
                    "pending sink action disappeared: {id}"
                )));
            }
            if batch.removable.is_empty() {
                return Ok(());
            }
            let prepared = persistence.prepare_removals(
                &self.data().original,
                sink,
                &batch.removable,
                unconfirmed,
            )?;
            let removable = &batch.removable;
            let retired = &mut batch.retired;
            let committed = Cell::new(false);
            let mut completion =
                prepare_owned_queue_completion(&self.data().original, &committed, || {
                    {
                        let mut queues = self.data().queues.lock();
                        let queue = queues.values.get_mut(sink).expect("claimed ack queue");
                        for id in removable {
                            assert!(
                                queue
                                    .entries
                                    .front()
                                    .is_some_and(|entry| entry.entry.id == *id),
                                "claimed ack prefix remains installed"
                            );
                            retired.push(queue.entries.pop_front().expect("ack prefix"));
                        }
                    }
                    let mut metrics = self.data().common.metrics.lock();
                    let state = metrics.get_mut(sink).expect("paid owning sink metrics");
                    state.unconfirmed = state.unconfirmed.saturating_sub(unconfirmed);
                })?;
            let durable = persistence.is_durable();
            if durable {
                publication.reserve()?;
            }
            let mut receipt =
                prepare_owned_queue_receipt(&self.data().original, &committed, || {
                    if durable {
                        publication.complete();
                    }
                })?;
            let token = _claim.commit_token(&committed);
            persistence.commit_removals_at_boundary(prepared, &token, &mut failure);
            receipt.finish();
            completion.finish();
            failure.loan_result(())
        });
        for lookup in &batch.lookups {
            if batch.acked.contains(&lookup.key.1) {
                self.finish_owned_checked_out(lookup);
            }
        }
        batch.retired.clear();
        self.data().common.waiters.notify_all();
        failure.finish(result)
    }
}
// Native persistence is a separate owned-path slice. This interface has no
// blanket Legacy implementation or fallback; the worker cannot encode naked
// keys or call the inherited allocating persistence helpers.
pub(crate) trait OwnedSinkQueuePersistence {
    type Update<'o>;
    type Removals<'o>;
    fn is_durable(&self) -> bool;
    fn prepare_update<'o>(
        &self,
        original: &'o OwnedMemoryReservation,
        sink: &'o str,
        entry_id: u64,
        encoded: &[u8],
        uncertain: bool,
        unconfirmed_delta: i8,
    ) -> Result<Self::Update<'o>>;
    fn commit_update(&self, prepared: Self::Update<'_>) -> Result<()>;
    fn commit_update_at_boundary(
        &self,
        prepared: Self::Update<'_>,
        completion: &OwnedQueueCommitToken<'_>,
        failure: &mut OwnedNativeQueueFailure,
    ) {
        assert!(
            !self.is_durable(),
            "durable native adapter requires its owning commit boundary"
        );
        let result = self.commit_update(prepared);
        if result.is_ok() {
            completion.mark_success();
        }
        failure.capture(result);
    }
    fn prepare_removals<'o>(
        &self,
        original: &'o OwnedMemoryReservation,
        sink: &'o str,
        ids: &'o [u64],
        unconfirmed: u64,
    ) -> Result<Self::Removals<'o>>;
    fn commit_removals(&self, prepared: Self::Removals<'_>) -> Result<()>;
    fn commit_removals_at_boundary(
        &self,
        prepared: Self::Removals<'_>,
        completion: &OwnedQueueCommitToken<'_>,
        failure: &mut OwnedNativeQueueFailure,
    ) {
        assert!(
            !self.is_durable(),
            "durable native adapter requires its owning commit boundary"
        );
        let result = self.commit_removals(prepared);
        if result.is_ok() {
            completion.mark_success();
        }
        failure.capture(result);
    }
}

pub(super) struct OwnedMetricsAllocation {
    keys: Vec<OwnedMemoryReservation>,
    array: OwnedMemoryReservation,
    buckets: OwnedMemoryReservation,
}
impl OwnedEventBusHandle {
    fn prepare_owned_metrics_slots(&self, names: &[String]) -> Result<()> {
        let original = &self.data().original;
        loop {
            // Count only. Overflow diagnostics are constructed after unlock.
            let counts = {
                let current = self.data().common.metrics.lock();
                let missing = names
                    .iter()
                    .filter(|name| !current.contains_key(*name))
                    .count();
                let bytes = current
                    .keys()
                    .try_fold(0usize, |n, key| n.checked_add(key.len()));
                (current.len(), missing, bytes)
            };
            if counts.1 == 0 {
                return Ok(());
            }
            let bytes = counts.2.ok_or_else(owned_event_overflow)?;
            let (body, credit) = new_owned_native_vec::<u8>(original, bytes)?;
            let mut text = OwnedEventBody { body, credit };
            let (body, credit) = new_owned_native_vec::<(std::ops::Range<usize>, SinkMetricsState)>(
                original, counts.0,
            )?;
            let mut captured = OwnedEventBody { body, credit };
            {
                let current = self.data().common.metrics.lock();
                let size = current
                    .keys()
                    .try_fold(0usize, |n, key| n.checked_add(key.len()));
                if current.len() > captured.body.capacity()
                    || size.is_none_or(|n| n > text.body.capacity())
                {
                    continue;
                }
                // Only prepaid byte copies and scalar state captures run here.
                for (key, state) in current.iter() {
                    let start = text.body.len();
                    text.body.extend_from_slice(key.as_bytes());
                    captured.body.push((start..text.body.len(), state.clone()));
                }
            }
            // A changed missing-name count can need more space than the initial
            // observation. Recompute from the captured, immutable key bytes.
            let source_key = |range: &std::ops::Range<usize>| {
                std::str::from_utf8(&text.body[range.clone()]).expect("captured UTF-8 metric key")
            };
            let missing = names
                .iter()
                .filter(|name| {
                    !captured
                        .body
                        .iter()
                        .any(|(range, _)| source_key(range) == name.as_str())
                })
                .count();
            let maximum = owned_event_add(captured.body.len(), missing)?;
            let (body, credit) = new_owned_native_map(original, maximum)?;
            let successor = OwnedEventBody { body, credit };
            let (body, credit) = new_owned_native_vec(original, maximum)?;
            let keys = OwnedEventBody { body, credit };
            let mut copied = OwnedEventCopiedMap {
                body: successor.body,
                keys: keys.body,
                array: keys.credit,
                buckets: successor.credit,
            };
            for (range, state) in &captured.body {
                let (key, memory) = copy_owned_native_string(original, source_key(range))?;
                copied.body.insert(key, state.clone());
                copied.keys.push(memory);
            }
            for name in names {
                if !copied.body.contains_key(name) {
                    let (key, memory) = copy_owned_native_string(original, name)?;
                    copied.body.insert(key, SinkMetricsState::default());
                    copied.keys.push(memory);
                }
            }
            let mut retired = OwnedEventBody {
                body: None,
                credit: None,
            };
            let mut current = self.data().common.metrics.lock();
            if current.len() != captured.body.len()
                || captured
                    .body
                    .iter()
                    .any(|(range, _)| !current.contains_key(source_key(range)))
            {
                drop(current);
                continue;
            }
            // Preserve counter increments made while the successor was built.
            for (key, state) in current.iter() {
                *copied.body.get_mut(key).expect("captured metric key") = state.clone();
            }
            retired.body = Some(std::mem::replace(&mut *current, copied.body));
            retired.credit = self
                .data()
                .metric_allocation
                .lock()
                .replace(OwnedMetricsAllocation {
                    keys: copied.keys,
                    array: copied.array,
                    buckets: copied.buckets,
                });
            drop(current);
            drop(retired);
            return Ok(());
        }
    }

    pub(super) fn copy_owned_sink_registration(
        &self,
        sink: &str,
    ) -> Result<Option<OwnedSinkRegistration>> {
        let callbacks = self.data().common.callbacks.read();
        let Some(registration) = callbacks.get(sink) else {
            return Ok(None);
        };
        let access = &registration.access;
        let mut bytes = owned_event_btree_bytes::<ContextId, ()>(
            access.contexts.as_ref().map_or(0, BTreeSet::len),
        )?;
        bytes = owned_event_add(
            bytes,
            owned_event_btree_bytes::<ScopeLabel, ()>(
                access.scope_labels.as_ref().map_or(0, BTreeSet::len),
            )?,
        )?;
        if let Some(labels) = &access.scope_labels {
            for label in labels {
                bytes = owned_event_add(bytes, label.0.len())?;
            }
        }
        if let Some(Principal::Agent(id) | Principal::Human(id)) = &access.principal {
            bytes = owned_event_add(bytes, id.len())?;
        }
        let allocation = reserve_owned_event_bytes(&self.data().original, bytes)?;
        let mut owner = OwnedSinkRegistration {
            registration: SinkRegistration {
                access: AccessConstraints::default(),
                callback: registration.callback.clone(),
            },
            allocation,
        };
        if let Some(contexts) = &access.contexts {
            let mut copied = BTreeSet::new();
            for context in contexts {
                copied.insert(context.clone());
            }
            owner.registration.access.contexts = Some(copied);
        }
        if let Some(labels) = &access.scope_labels {
            let mut copied = BTreeSet::new();
            for label in labels {
                copied.insert(ScopeLabel::new(copy_owned_prepaid_text(&label.0)?));
            }
            owner.registration.access.scope_labels = Some(copied);
        }
        owner.registration.access.principal = match &access.principal {
            None => None,
            Some(Principal::System) => Some(Principal::System),
            Some(Principal::Agent(id)) => Some(Principal::Agent(copy_owned_prepaid_text(id)?)),
            Some(Principal::Human(id)) => Some(Principal::Human(copy_owned_prepaid_text(id)?)),
        };
        Ok(Some(owner))
    }
}
pub(super) struct OwnedSinkRegistration {
    registration: SinkRegistration,
    allocation: OwnedMemoryReservation,
}
pub(super) struct OwnedEventBusWorker<P> {
    sink: OwnedEventBody<String, OwnedMemoryReservation>,
    events: OwnedEventBusHandle,
    persistence: P,
    tx_mgr: DatabaseTransactionHost,
    control: OwnedMemoryReservation,
}
impl<P: OwnedSinkQueuePersistence> OwnedEventBusWorker<P> {
    pub(super) fn try_new(
        events: OwnedEventBusHandle,
        sink: &str,
        persistence: P,
        tx_mgr: DatabaseTransactionHost,
        expected: &crate::memory_accounting::AccountantHandle,
    ) -> Result<Self> {
        match &tx_mgr {
            DatabaseTransactionHost::Owned(handle) => handle.with_host(|host| {
                validate_owned_original(&events.data().original, expected, &host.funder)
            })?,
            DatabaseTransactionHost::Legacy(_) => {
                return Err(Error::Other(
                    "owning worker requires its actual native host".into(),
                ));
            }
        }
        let (name, memory) = copy_owned_native_string(&events.data().original, sink)?;
        let sink = OwnedEventBody {
            body: name,
            credit: memory,
        };
        let control =
            reserve_owned_event_bytes(&events.data().original, std::mem::size_of::<Self>())?;
        Ok(Self {
            sink,
            events,
            persistence,
            tx_mgr,
            control,
        })
    }
    fn wait(&self, shutdown: &AtomicBool, duration: Duration) {
        if shutdown.load(Ordering::SeqCst) {
            return;
        }
        let mut guard = self.events.data().common.wait_lock.lock();
        self.events
            .data()
            .common
            .waiters
            .wait_for(&mut guard, duration);
    }
    fn in_flight(&self, increment: bool) {
        let mut metrics = self.events.data().common.metrics.lock();
        let state = metrics
            .get_mut(&self.sink.body)
            .expect("paid owning metrics");
        if increment {
            state.in_flight += 1;
        } else {
            state.in_flight = state.in_flight.saturating_sub(1);
        }
    }
    pub(super) fn dispatch_loop(self, shutdown: Arc<AtomicBool>) {
        'dispatch: while !shutdown.load(Ordering::SeqCst) {
            let registration = match self.events.copy_owned_sink_registration(&self.sink.body) {
                Ok(Some(registration)) => registration,
                Ok(None) => {
                    self.wait(&shutdown, Duration::from_millis(100));
                    continue;
                }
                Err(error) => {
                    tracing::warn!(%error, "cannot admit owning sink registration");
                    self.wait(&shutdown, Duration::from_millis(50));
                    continue;
                }
            };
            let mut batch = match self.events.checkout_owned_sink_batch(
                &self.sink.body,
                &registration.registration.access,
                SINK_DISPATCH_BATCH,
            ) {
                Ok(batch) => batch,
                Err(error) => {
                    tracing::warn!(%error, "cannot admit owning sink checkout");
                    self.wait(&shutdown, Duration::from_millis(50));
                    continue;
                }
            };
            if batch.entries.is_empty() {
                self.wait(
                    &shutdown,
                    Duration::from_millis(if batch.denied_rotation { 10 } else { 250 }),
                );
                continue;
            }
            let mut delivered = 0u64;
            let mut failed = 0u64;
            let mut examined = 0u64;
            for position in 0..batch.entries.len() {
                if shutdown.load(Ordering::SeqCst) {
                    // Original shutdown releases the undelivered suffix before
                    // the post-batch prefix ack. Finished lookups make the
                    // subsequent batch drop a no-op for that suffix.
                    for lookup in batch.lookups.iter().skip(position) {
                        self.events.finish_owned_checked_out(lookup);
                    }
                    break;
                }
                let entry = &batch.entries[position].entry;
                if entry.attempts & UNCONFIRMED_ATTEMPT != 0 {
                    if let Err(error) = self.events.transition_owned_sink_attempt(
                        &self.persistence,
                        &self.tx_mgr,
                        &self.sink.body,
                        &mut batch.lookups[position],
                        false,
                        true,
                    ) {
                        batch.cleanup_from.body = position;
                        tracing::warn!(%error, "cannot durably reconcile an unconfirmed sink attempt");
                        drop(batch);
                        self.wait(&shutdown, Duration::from_millis(50));
                        continue 'dispatch;
                    }
                    self.events
                        .record_owned_batch_outcome(&self.sink.body, 0, 0, 0, 0, 1);
                }
                let attempts = match self.events.transition_owned_sink_attempt(
                    &self.persistence,
                    &self.tx_mgr,
                    &self.sink.body,
                    &mut batch.lookups[position],
                    true,
                    false,
                ) {
                    Ok(attempts) => attempts,
                    Err(error) => {
                        batch.cleanup_from.body = position;
                        tracing::warn!(%error, "cannot durably record sink attempt before delivery");
                        drop(batch);
                        self.wait(&shutdown, Duration::from_millis(50));
                        continue 'dispatch;
                    }
                };
                examined = examined.saturating_add(1);
                self.in_flight(true);
                // Mutate only the already-paid batch copy. Callback borrows it;
                // there is no second naked event clone and no commit/queue lock.
                let entry = &mut batch.entries[position].entry;
                if entry.event.action_id == 0 {
                    entry.event.action_id = entry.id;
                }
                let result = {
                    let _callback = EventBusCallbackGuard::enter(&self.events.data().common);
                    catch_unwind(AssertUnwindSafe(|| {
                        (registration.registration.callback)(&entry.event)
                    }))
                };
                self.in_flight(false);
                let outcome = match result {
                    Ok(outcome) => OwnedEventBody {
                        body: outcome,
                        credit: None,
                    },
                    Err(payload) => {
                        let message = if let Some(message) = payload.downcast_ref::<&str>() {
                            *message
                        } else if let Some(message) = payload.downcast_ref::<String>() {
                            message.as_str()
                        } else {
                            "non-string panic payload"
                        };
                        let (body, credit) = match copy_owned_native_string(
                            &self.events.data().original,
                            message,
                        ) {
                            Ok(copy) => copy,
                            Err(error) => {
                                batch.cleanup_from.body = position;
                                tracing::warn!(%error, "sink panic remains durably unconfirmed for recovery");
                                drop(batch);
                                self.wait(&shutdown, Duration::from_millis(50));
                                continue 'dispatch;
                            }
                        };
                        OwnedEventBody {
                            body: Err(SinkError::OutcomeUncertain(body)),
                            credit: Some(credit),
                        }
                    }
                };
                let uncertain = matches!(&outcome.body, Err(SinkError::OutcomeUncertain(_)));
                match outcome.body {
                    Ok(()) => {
                        delivered = delivered.saturating_add(1);
                        batch.acked.push(entry.id);
                    }
                    Err(SinkError::Permanent(_)) => {
                        failed = failed.saturating_add(1);
                        batch.acked.push(entry.id);
                    }
                    Err(SinkError::Transient(_) | SinkError::OutcomeUncertain(_)) => {
                        let _ = self.events.ack_owned_front_entries(
                            &self.persistence,
                            &self.tx_mgr,
                            &self.sink.body,
                            &mut batch,
                        );
                        match self.events.transition_owned_sink_attempt(
                            &self.persistence,
                            &self.tx_mgr,
                            &self.sink.body,
                            &mut batch.lookups[position],
                            false,
                            uncertain,
                        ) {
                            Ok(_) if uncertain => self.events.record_owned_batch_outcome(
                                &self.sink.body,
                                0,
                                0,
                                0,
                                0,
                                1,
                            ),
                            Ok(_) => (),
                            Err(error) => {
                                tracing::warn!(%error, "sink attempt remains durably unconfirmed for recovery")
                            }
                        }
                        self.events.record_owned_batch_outcome(
                            &self.sink.body,
                            delivered,
                            failed,
                            examined,
                            1,
                            0,
                        );
                        drop(batch);
                        self.wait(&shutdown, retry_backoff(attempts));
                        continue 'dispatch;
                    }
                }
            }
            let _ = self.events.ack_owned_front_entries(
                &self.persistence,
                &self.tx_mgr,
                &self.sink.body,
                &mut batch,
            );
            self.events.record_owned_batch_outcome(
                &self.sink.body,
                delivered,
                failed,
                examined,
                0,
                0,
            );
        }
        let cleanup = {
            let mut deferred = self.events.data().common.deferred_resource_cleanup.lock();
            if deferred
                .as_ref()
                .is_some_and(|cleanup| cleanup.sink == self.sink.body)
            {
                deferred.take()
            } else {
                None
            }
        };
        if let Some(cleanup) = cleanup {
            cleanup.close();
        }
    }
}

impl OwnedEventBusHandle {
    pub(super) fn merge_owned_commit_escrow(
        &self,
        stage: &mut OwnedCommitEventStage,
    ) -> Result<()> {
        let additional = owned_event_add(
            stage
                .ddl
                .as_ref()
                .map_or(0, |ddl| ddl.publication.slots.slots.len()),
            stage
                .schema
                .as_ref()
                .map_or(0, |schema| schema.slots.slots.len()),
        )?;
        if additional == 0 {
            return Ok(());
        }
        let Some(OwnedAppendCustody::Ordinary(events)) = &mut stage.events.publication else {
            return Err(Error::Other(
                "combined stage must join ordinary escrow before discard".into(),
            ));
        };
        let maximum = owned_event_add(events.slots.len(), additional)?;
        let (body, credit) = new_owned_native_vec(&self.data().original, maximum)?;
        let mut successor = OwnedEventBody { body, credit };
        successor.body.append(&mut events.slots);
        for publication in stage
            .ddl
            .as_mut()
            .map(|ddl| &mut ddl.publication)
            .into_iter()
            .chain(stage.schema.as_mut())
        {
            assert!(publication.slots.state == OwnedEscrowState::Active);
            successor.body.append(&mut publication.slots.slots);
            publication.slots.state = OwnedEscrowState::Published;
            // The normal event lease remains active. No queue/control guard
            // crosses the release of another same-terminal lease.
            drop(publication.slots.lease.take());
        }
        let old_body = std::mem::replace(&mut events.slots, successor.body);
        let old_credit = std::mem::replace(&mut events.workspace, successor.credit);
        drop(old_body);
        drop(old_credit);
        Ok(())
    }
}

// The encoded source object must pin ONE durable root/read transaction for the
// entire call. These native-only dependency traits deliberately have no Legacy
// blanket adapter, decode-then-charge implementation, or per-pass root opener.
pub(super) trait OwnedEncodedEventSource {
    fn visit(
        &self,
        sink: &str,
        visit: impl for<'e> FnMut(u64, &'e [u8]) -> Result<()>,
    ) -> Result<()>;
    fn unloaded_extent(&self, sink: &str) -> Result<Option<(u64, u64)>>;
    fn counters(&self, sink: &str) -> Result<(u64, u64)>;
    fn next_action_id(&self) -> Result<u64>;
}
pub(super) trait OwnedEventEntryDecoder {
    fn decode(
        &self,
        original: &OwnedMemoryReservation,
        encoded: &[u8],
    ) -> Result<OwnedSinkQueueEntry>;
}
pub(super) struct OwnedEventStartupImage {
    queues: OwnedQueueMap,
    definitions: OwnedDefinitionsProjection,
    metrics: HashMap<String, SinkMetricsState>,
    metrics_credit: OwnedMetricsAllocation,
    unloaded: HashMap<String, u64>,
    unloaded_credit: OwnedMetricsAllocation,
    next_action_id: u64,
}
impl OwnedEventBusHandle {
    pub(super) fn prepare_owned_event_bus_image<
        S: OwnedEncodedEventSource,
        D: OwnedEventEntryDecoder,
    >(
        &self,
        source: &S,
        decoder: &D,
        definitions: OwnedDefinitionsProjection,
        writable: bool,
    ) -> Result<OwnedEventStartupImage> {
        let original = &self.data().original;
        let count = definitions.definitions.sinks.len();
        let (queues, allocation) = new_owned_native_map(original, count)?;
        let queues = OwnedQueueMap {
            values: queues,
            allocation,
        };
        let (body, credit) = new_owned_native_map(original, count)?;
        let metrics = OwnedEventBody { body, credit };
        let (body, credit) = new_owned_native_vec(original, count)?;
        let metric_keys = OwnedEventBody { body, credit };
        let (body, credit) = new_owned_native_map(original, if writable { 0 } else { count })?;
        let unloaded = OwnedEventBody { body, credit };
        let (body, credit) = new_owned_native_vec(original, if writable { 0 } else { count })?;
        let unloaded_keys = OwnedEventBody { body, credit };
        let next_action_id = source.next_action_id()?.max(1);
        let mut image = OwnedEventStartupImage {
            queues,
            definitions,
            metrics: metrics.body,
            metrics_credit: OwnedMetricsAllocation {
                keys: metric_keys.body,
                array: metric_keys.credit,
                buckets: metrics.credit,
            },
            unloaded: unloaded.body,
            unloaded_credit: OwnedMetricsAllocation {
                keys: unloaded_keys.body,
                array: unloaded_keys.credit,
                buckets: unloaded.credit,
            },
            next_action_id,
        };
        for name in image.definitions.definitions.sinks.keys() {
            let (uncertain, unconfirmed) = source.counters(name)?;
            let extent = source.unloaded_extent(name)?;
            if writable && extent.is_some() {
                return Err(Error::Other(
                    "writable owning queue cannot retain an unloaded extent".into(),
                ));
            }
            let mut entries_count = 0usize;
            if extent.is_none() {
                source.visit(name, |_, _| {
                    entries_count = owned_event_add(entries_count, 1)?;
                    Ok(())
                })?;
            }
            let (body, credit) = copy_owned_native_string(original, name)?;
            let key = OwnedEventBody { body, credit };
            let (entries, buffer) = new_owned_native_ring(original, entries_count)?;
            let mut queue = OwnedSinkQueue {
                entries,
                buffer,
                name: self.prepare_owned_queue_name(name)?,
                key: None,
                pending: 0,
                reservations: 0,
                present: true,
                retiring: 0,
                mutation: None,
            };
            if extent.is_none() {
                source.visit(name, |id, bytes| {
                    if queue.entries.len() == entries_count {
                        return Err(Error::Other("pinned event source count changed".into()));
                    }
                    let mut entry = decoder.decode(original, bytes)?;
                    if entry.entry.id != id {
                        return Err(Error::Other(
                            "sink queue key/content identity mismatch".into(),
                        ));
                    }
                    entry.entry.event.action_id = entry.entry.id;
                    image.next_action_id = image.next_action_id.max(id.saturating_add(1));
                    let pin = OwnedEventShared::try_new(entry, original, &self.data().funder)?;
                    queue.entries.push_back(pin);
                    Ok(())
                })?;
                if queue.entries.len() != entries_count {
                    return Err(Error::Other("pinned event source count changed".into()));
                }
            } else {
                let (key, credit) = copy_owned_native_string(original, name)?;
                let key = OwnedEventBody { body: key, credit };
                let Some((depth, newest_id)) = extent else {
                    unreachable!("read image extent")
                };
                image.next_action_id = image.next_action_id.max(newest_id.saturating_add(1));
                image.unloaded.insert(key.body, depth);
                image.unloaded_credit.keys.push(key.credit);
            }
            queue.key = Some(key.credit);
            image.queues.values.insert(key.body, queue);
            let (key, credit) = copy_owned_native_string(original, name)?;
            image.metrics.insert(
                key,
                SinkMetricsState {
                    uncertain,
                    unconfirmed,
                    ..SinkMetricsState::default()
                },
            );
            image.metrics_credit.keys.push(credit);
        }
        Ok(image)
    }
    pub(super) fn install_owned_event_bus_image(
        &self,
        mut image: OwnedEventStartupImage,
    ) -> Result<()> {
        // Startup only: no worker, checkout, stage or pending publication may
        // coexist with this wholesale initial-image installation.
        let state = self.data().common.queue_mutation_state.lock();
        if state.active_mutations != 0
            || !state.checked_out.is_empty()
            || !self.data().common.runtimes.lock().is_empty()
            || !self.data().stages.lock().values.is_empty()
        {
            return Err(Error::Other(
                "owning event load requires inactive assembly".into(),
            ));
        }
        let mut queues = self.data().queues.lock();
        if queues
            .values
            .values()
            .any(|queue| queue.pending != 0 || queue.reservations != 0 || queue.mutation.is_some())
        {
            return Err(Error::Other(
                "owning event load cannot replace escrow".into(),
            ));
        }
        std::mem::swap(&mut *queues, &mut image.queues);
        drop(queues);
        drop(state);
        let old_defs = std::mem::replace(
            &mut *self.data().common.definitions.lock(),
            image.definitions.definitions,
        );
        let old_defs_credit = self
            .data()
            .definition_credit
            .lock()
            .replace(image.definitions.allocation);
        let old_metrics = std::mem::replace(&mut *self.data().common.metrics.lock(), image.metrics);
        let old_metrics_credit = self
            .data()
            .metric_allocation
            .lock()
            .replace(image.metrics_credit);
        let old_unloaded = std::mem::replace(
            &mut *self.data().common.unloaded_queue_depths.lock(),
            image.unloaded,
        );
        let old_unloaded_credit = self
            .data()
            .unloaded_allocation
            .lock()
            .replace(image.unloaded_credit);
        self.data()
            .common
            .next_queue_id
            .store(image.next_action_id, Ordering::SeqCst);
        drop(old_defs);
        drop(old_defs_credit);
        drop(old_metrics);
        drop(old_metrics_credit);
        drop(old_unloaded);
        drop(old_unloaded_credit);
        drop(image.queues);
        Ok(())
    }
}

#[derive(Clone, Copy)]
pub(super) struct OwnedEventTypeView<'a> {
    key: &'a str,
    name: &'a str,
    trigger: EventTrigger,
    table: &'a str,
}
#[derive(Clone, Copy)]
pub(super) struct OwnedSinkView<'a> {
    key: &'a str,
    name: &'a str,
    sink_type: PersistedSinkType,
    url: Option<&'a str>,
}
#[derive(Clone, Copy)]
pub(super) struct OwnedRouteView<'a> {
    key: &'a str,
    name: &'a str,
    event_type: &'a str,
    sink: &'a str,
    where_in: Option<(&'a str, &'a [String])>,
}
impl OwnedEventBusHandle {
    pub(super) fn prepare_owned_definition_projection_with_ddl(
        &self,
        ddl: &[DdlChange],
        table_exists: impl Fn(&str) -> bool,
    ) -> Result<OwnedDefinitionsProjection> {
        let original = &self.data().original;
        // The inherited definitions are mutable, not an Arc-backed source.
        // Capture only counts, scalar tags, offsets, and prepaid bytes while
        // guarded. String/value-array/tree construction consumes that immutable
        // capture after unlock; no borrowed definition escapes the guard.
        type Span = std::ops::Range<usize>;
        type TypeCapture = (Span, Span, EventTrigger, Span);
        type SinkCapture = (Span, Span, PersistedSinkType, Option<Span>);
        type RouteCapture = (Span, Span, Span, Span, Option<(Span, Span)>);
        let snapshot = loop {
            let measure = |source: &EventBusDefinitions| {
                let mut bytes = Some(0usize);
                let mut values = Some(0usize);
                let mut add = |text: &str| {
                    bytes = bytes.and_then(|n| n.checked_add(text.len()));
                };
                for (key, def) in &source.event_types {
                    for text in [key, &def.name, &def.table] {
                        add(text);
                    }
                }
                for (key, def) in &source.sinks {
                    for text in [
                        key.as_str(),
                        def.name.as_str(),
                        def.url.as_deref().unwrap_or(""),
                    ] {
                        add(text);
                    }
                }
                for (key, def) in &source.routes {
                    for text in [key, &def.name, &def.event_type, &def.sink] {
                        add(text);
                    }
                    if let Some(filter) = &def.where_in {
                        add(&filter.column);
                        values = values.and_then(|n| n.checked_add(filter.values.len()));
                        for value in &filter.values {
                            add(value);
                        }
                    }
                }
                (
                    source.event_types.len(),
                    source.sinks.len(),
                    source.routes.len(),
                    bytes,
                    values,
                )
            };
            let counts = {
                let current = self.data().common.definitions.lock();
                measure(&current)
            };
            let bytes = counts.3.ok_or_else(owned_event_overflow)?;
            let value_count = counts.4.ok_or_else(owned_event_overflow)?;
            let (body, credit) = new_owned_native_vec::<u8>(original, bytes)?;
            let mut text = OwnedEventBody { body, credit };
            let (body, credit) = new_owned_native_vec::<TypeCapture>(original, counts.0)?;
            let mut type_source = OwnedEventBody { body, credit };
            let (body, credit) = new_owned_native_vec::<SinkCapture>(original, counts.1)?;
            let mut sink_source = OwnedEventBody { body, credit };
            let (body, credit) = new_owned_native_vec::<RouteCapture>(original, counts.2)?;
            let mut route_source = OwnedEventBody { body, credit };
            let (body, credit) = new_owned_native_vec::<Span>(original, value_count)?;
            let mut value_source = OwnedEventBody { body, credit };
            {
                let current = self.data().common.definitions.lock();
                let now = measure(&current);
                if now.0 > type_source.body.capacity()
                    || now.1 > sink_source.body.capacity()
                    || now.2 > route_source.body.capacity()
                    || now.3.is_none_or(|n| n > text.body.capacity())
                    || now.4.is_none_or(|n| n > value_source.body.capacity())
                {
                    continue;
                }
                let mut capture = |value: &str| {
                    let start = text.body.len();
                    text.body.extend_from_slice(value.as_bytes());
                    start..text.body.len()
                };
                for (key, def) in &current.event_types {
                    type_source.body.push((
                        capture(key),
                        capture(&def.name),
                        def.trigger,
                        capture(&def.table),
                    ));
                }
                for (key, def) in &current.sinks {
                    sink_source.body.push((
                        capture(key),
                        capture(&def.name),
                        def.sink_type,
                        def.url.as_deref().map(&mut capture),
                    ));
                }
                for (key, def) in &current.routes {
                    let key = capture(key);
                    let name = capture(&def.name);
                    let event_type = capture(&def.event_type);
                    let sink = capture(&def.sink);
                    let filter = def.where_in.as_ref().map(|filter| {
                        let column = capture(&filter.column);
                        let start = value_source.body.len();
                        for value in &filter.values {
                            value_source.body.push(capture(value));
                        }
                        (column, start..value_source.body.len())
                    });
                    route_source
                        .body
                        .push((key, name, event_type, sink, filter));
                }
            }
            let source_text = |span: &Span| {
                std::str::from_utf8(&text.body[span.clone()]).expect("captured UTF-8 definition")
            };
            let mut bytes = owned_event_add(
                text.body.len(),
                owned_event_array::<String>(value_source.body.len())?,
            )?;
            bytes = owned_event_add(
                bytes,
                owned_event_btree_bytes::<String, EventTypeDef>(type_source.body.len())?,
            )?;
            bytes = owned_event_add(
                bytes,
                owned_event_btree_bytes::<String, SinkDef>(sink_source.body.len())?,
            )?;
            bytes = owned_event_add(
                bytes,
                owned_event_btree_bytes::<String, RouteDef>(route_source.body.len())?,
            )?;
            let allocation = reserve_owned_event_bytes(original, bytes)?;
            let mut snapshot = OwnedDefinitionsProjection {
                definitions: EventBusDefinitions::default(),
                allocation,
            };
            for (key, name, trigger, table) in &type_source.body {
                snapshot.definitions.event_types.insert(
                    copy_owned_prepaid_text(source_text(key))?,
                    EventTypeDef {
                        name: copy_owned_prepaid_text(source_text(name))?,
                        trigger: *trigger,
                        table: copy_owned_prepaid_text(source_text(table))?,
                    },
                );
            }
            for (key, name, sink_type, url) in &sink_source.body {
                snapshot.definitions.sinks.insert(
                    copy_owned_prepaid_text(source_text(key))?,
                    SinkDef {
                        name: copy_owned_prepaid_text(source_text(name))?,
                        sink_type: *sink_type,
                        url: url
                            .as_ref()
                            .map(|span| copy_owned_prepaid_text(source_text(span)))
                            .transpose()?,
                    },
                );
            }
            for (key, name, event_type, sink, filter) in &route_source.body {
                let where_in = if let Some((column, span)) = filter {
                    let mut values = Vec::with_capacity(span.len());
                    if values.capacity() != span.len() {
                        return Err(Error::Other("event route capacity mismatch".into()));
                    }
                    for value in &value_source.body[span.clone()] {
                        values.push(copy_owned_prepaid_text(source_text(value))?);
                    }
                    Some(PersistedRouteWhereIn {
                        column: copy_owned_prepaid_text(source_text(column))?,
                        values,
                    })
                } else {
                    None
                };
                snapshot.definitions.routes.insert(
                    copy_owned_prepaid_text(source_text(key))?,
                    RouteDef {
                        name: copy_owned_prepaid_text(source_text(name))?,
                        event_type: copy_owned_prepaid_text(source_text(event_type))?,
                        sink: copy_owned_prepaid_text(source_text(sink))?,
                        where_in,
                    },
                );
            }
            break snapshot;
        };
        let current = &snapshot.definitions;
        let (body, credit) = new_owned_native_vec(
            original,
            owned_event_add(current.event_types.len(), ddl.len())?,
        )?;
        let mut types = OwnedEventBody { body, credit };
        let (body, credit) =
            new_owned_native_vec(original, owned_event_add(current.sinks.len(), ddl.len())?)?;
        let mut sinks = OwnedEventBody { body, credit };
        let (body, credit) =
            new_owned_native_vec(original, owned_event_add(current.routes.len(), ddl.len())?)?;
        let mut routes = OwnedEventBody { body, credit };
        for (key, def) in &current.event_types {
            types.body.push(OwnedEventTypeView {
                key,
                name: &def.name,
                trigger: def.trigger,
                table: &def.table,
            });
        }
        for (key, def) in &current.sinks {
            sinks.body.push(OwnedSinkView {
                key,
                name: &def.name,
                sink_type: def.sink_type,
                url: def.url.as_deref(),
            });
        }
        for (key, def) in &current.routes {
            routes.body.push(OwnedRouteView {
                key,
                name: &def.name,
                event_type: &def.event_type,
                sink: &def.sink,
                where_in: def
                    .where_in
                    .as_ref()
                    .map(|filter| (filter.column.as_str(), filter.values.as_slice())),
            });
        }
        for change in ddl {
            match change {
                DdlChange::CreateEventType {
                    name,
                    trigger,
                    table,
                } => {
                    if !table_exists(table) {
                        return Err(Error::TableNotFound(table.clone()));
                    }
                    let trigger = if trigger.eq_ignore_ascii_case("INSERT") {
                        EventTrigger::Insert
                    } else if trigger.eq_ignore_ascii_case("UPDATE") {
                        EventTrigger::Update
                    } else if trigger.eq_ignore_ascii_case("DELETE") {
                        EventTrigger::Delete
                    } else {
                        return Err(Error::Other(format!(
                            "invalid event type trigger: {trigger}"
                        )));
                    };
                    if let Some(existing) = types.body.iter().find(|def| def.key == name) {
                        if existing.name != name
                            || existing.trigger != trigger
                            || existing.table != table
                        {
                            return Err(Error::Other(format!(
                                "event type already exists with a different definition: {name}"
                            )));
                        }
                    } else {
                        types.body.push(OwnedEventTypeView {
                            key: name,
                            name,
                            trigger,
                            table,
                        });
                    }
                }
                DdlChange::CreateSink {
                    name,
                    sink_type,
                    url,
                } => {
                    let sink_type = if sink_type.eq_ignore_ascii_case("WEBHOOK") {
                        PersistedSinkType::Webhook
                    } else if sink_type.eq_ignore_ascii_case("CALLBACK") {
                        PersistedSinkType::Callback
                    } else {
                        return Err(Error::Other(format!("invalid sink type: {sink_type}")));
                    };
                    if let Some(existing) = sinks.body.iter().find(|def| def.key == name) {
                        if existing.name != name
                            || existing.sink_type != sink_type
                            || existing.url != url.as_deref()
                        {
                            return Err(Error::Other(format!(
                                "sink already exists with a different definition: {name}"
                            )));
                        }
                    } else {
                        sinks.body.push(OwnedSinkView {
                            key: name,
                            name,
                            sink_type,
                            url: url.as_deref(),
                        });
                    }
                }
                DdlChange::CreateRoute {
                    name,
                    event_type,
                    sink,
                    table,
                    where_in,
                } => {
                    let route_table = types
                        .body
                        .iter()
                        .find(|def| def.key == event_type)
                        .map(|def| def.table)
                        .ok_or_else(|| {
                            Error::Other(format!("event type not found: {event_type}"))
                        })?;
                    if !sinks.body.iter().any(|def| def.key == sink) {
                        return Err(Error::Other(format!("sink not found: {sink}")));
                    }
                    let filter = where_in
                        .as_ref()
                        .map(|(column, values)| (column.as_str(), values.as_slice()));
                    if let Some(existing) = routes.body.iter().find(|def| def.key == name) {
                        if existing.name != name
                            || existing.event_type != event_type
                            || existing.sink != sink
                            || existing.where_in != filter
                        {
                            return Err(Error::Other(format!(
                                "route already exists with a different definition: {name}"
                            )));
                        }
                    } else {
                        routes.body.push(OwnedRouteView {
                            key: name,
                            name,
                            event_type,
                            sink,
                            where_in: filter,
                        });
                    }
                    if !table.is_empty() && table != route_table {
                        return Err(Error::Other(format!(
                            "route table mismatch for {name}: ddl={table}, event_type={route_table}"
                        )));
                    }
                }
                DdlChange::DropRoute { name, .. } => routes.body.retain(|def| def.key != name),
                DdlChange::DropTable { name } => {
                    routes.body.retain(|route| {
                        !types
                            .body
                            .iter()
                            .any(|def| def.table == name && def.name == route.event_type)
                    });
                    types.body.retain(|def| def.table != name);
                }
                _ => (),
            }
        }
        // A borrowed overlay establishes exact final counts and nested sizes;
        // it never allocates String/DDL/definition bodies while validating.
        types.body.sort_unstable_by_key(|def| def.key);
        sinks.body.sort_unstable_by_key(|def| def.key);
        routes.body.sort_unstable_by_key(|def| def.key);
        let mut bytes = owned_event_add(
            owned_event_btree_bytes::<String, EventTypeDef>(types.body.len())?,
            owned_event_btree_bytes::<String, SinkDef>(sinks.body.len())?,
        )?;
        bytes = owned_event_add(
            bytes,
            owned_event_btree_bytes::<String, RouteDef>(routes.body.len())?,
        )?;
        for def in &types.body {
            for text in [def.key, def.name, def.table] {
                bytes = owned_event_add(bytes, text.len())?;
            }
        }
        for def in &sinks.body {
            for text in [def.key, def.name, def.url.unwrap_or("")] {
                bytes = owned_event_add(bytes, text.len())?;
            }
        }
        for def in &routes.body {
            for text in [def.key, def.name, def.event_type, def.sink] {
                bytes = owned_event_add(bytes, text.len())?;
            }
            if let Some((column, values)) = def.where_in {
                bytes = owned_event_add(bytes, column.len())?;
                bytes = owned_event_add(bytes, owned_event_array::<String>(values.len())?)?;
                for value in values {
                    bytes = owned_event_add(bytes, value.len())?;
                }
            }
        }
        let allocation = reserve_owned_event_bytes(original, bytes)?;
        let mut owner = OwnedDefinitionsProjection {
            definitions: EventBusDefinitions::default(),
            allocation,
        };
        for def in &types.body {
            owner.definitions.event_types.insert(
                copy_owned_prepaid_text(def.key)?,
                EventTypeDef {
                    name: copy_owned_prepaid_text(def.name)?,
                    trigger: def.trigger,
                    table: copy_owned_prepaid_text(def.table)?,
                },
            );
        }
        for def in &sinks.body {
            owner.definitions.sinks.insert(
                copy_owned_prepaid_text(def.key)?,
                SinkDef {
                    name: copy_owned_prepaid_text(def.name)?,
                    sink_type: def.sink_type,
                    url: def.url.map(copy_owned_prepaid_text).transpose()?,
                },
            );
        }
        for def in &routes.body {
            let where_in = if let Some((column, source)) = def.where_in {
                let mut values = Vec::with_capacity(source.len());
                if values.capacity() != source.len() {
                    return Err(Error::Other("event route capacity mismatch".into()));
                }
                for value in source {
                    values.push(copy_owned_prepaid_text(value)?);
                }
                Some(PersistedRouteWhereIn {
                    column: copy_owned_prepaid_text(column)?,
                    values,
                })
            } else {
                None
            };
            owner.definitions.routes.insert(
                copy_owned_prepaid_text(def.key)?,
                RouteDef {
                    name: copy_owned_prepaid_text(def.name)?,
                    event_type: copy_owned_prepaid_text(def.event_type)?,
                    sink: copy_owned_prepaid_text(def.sink)?,
                    where_in,
                },
            );
        }
        Ok(owner)
    }
}

pub(super) struct OwnedEventDdlInput {
    ddl: Vec<DdlChange>,
    bodies: OwnedMemoryReservation,
    array: OwnedMemoryReservation,
}
impl OwnedEventDdlInput {
    pub(super) fn try_copy(original: &OwnedMemoryReservation, ddl: &[DdlChange]) -> Result<Self> {
        let mut bytes = 0;
        for change in ddl {
            match change {
                DdlChange::CreateEventType {
                    name,
                    trigger,
                    table,
                } => {
                    for text in [name, trigger, table] {
                        bytes = owned_event_add(bytes, text.len())?;
                    }
                }
                DdlChange::CreateSink {
                    name,
                    sink_type,
                    url,
                } => {
                    bytes = owned_event_add(bytes, owned_event_add(name.len(), sink_type.len())?)?;
                    bytes = owned_event_add(bytes, url.as_ref().map_or(0, String::len))?;
                }
                DdlChange::CreateRoute {
                    name,
                    event_type,
                    sink,
                    table,
                    where_in,
                } => {
                    for text in [name, event_type, sink, table] {
                        bytes = owned_event_add(bytes, text.len())?;
                    }
                    if let Some((column, values)) = where_in {
                        bytes = owned_event_add(bytes, column.len())?;
                        bytes = owned_event_add(bytes, owned_event_array::<String>(values.len())?)?;
                        for text in values {
                            bytes = owned_event_add(bytes, text.len())?;
                        }
                    }
                }
                DdlChange::DropRoute { name, table } => {
                    bytes = owned_event_add(bytes, owned_event_add(name.len(), table.len())?)?;
                }
                DdlChange::DropTable { name } => bytes = owned_event_add(bytes, name.len())?,
                _ => return Err(Error::Other(
                    "owning event DDL requires the original producer for non-event declarations"
                        .into(),
                )),
            }
        }
        let bodies = reserve_owned_event_bytes(original, bytes)?;
        let (ddl_body, array) = new_owned_native_vec(original, ddl.len())?;
        let mut owner = Self {
            ddl: ddl_body,
            bodies,
            array,
        };
        for change in ddl {
            let copied = match change {
                DdlChange::CreateEventType {
                    name,
                    trigger,
                    table,
                } => DdlChange::CreateEventType {
                    name: copy_owned_prepaid_text(name)?,
                    trigger: copy_owned_prepaid_text(trigger)?,
                    table: copy_owned_prepaid_text(table)?,
                },
                DdlChange::CreateSink {
                    name,
                    sink_type,
                    url,
                } => DdlChange::CreateSink {
                    name: copy_owned_prepaid_text(name)?,
                    sink_type: copy_owned_prepaid_text(sink_type)?,
                    url: url.as_deref().map(copy_owned_prepaid_text).transpose()?,
                },
                DdlChange::CreateRoute {
                    name,
                    event_type,
                    sink,
                    table,
                    where_in,
                } => {
                    let where_in = if let Some((column, source)) = where_in {
                        let mut values = Vec::with_capacity(source.len());
                        if values.capacity() != source.len() {
                            return Err(Error::Other("event DDL values capacity mismatch".into()));
                        }
                        for value in source {
                            values.push(copy_owned_prepaid_text(value)?);
                        }
                        Some((copy_owned_prepaid_text(column)?, values))
                    } else {
                        None
                    };
                    DdlChange::CreateRoute {
                        name: copy_owned_prepaid_text(name)?,
                        event_type: copy_owned_prepaid_text(event_type)?,
                        sink: copy_owned_prepaid_text(sink)?,
                        table: copy_owned_prepaid_text(table)?,
                        where_in,
                    }
                }
                DdlChange::DropRoute { name, table } => DdlChange::DropRoute {
                    name: copy_owned_prepaid_text(name)?,
                    table: copy_owned_prepaid_text(table)?,
                },
                DdlChange::DropTable { name } => DdlChange::DropTable {
                    name: copy_owned_prepaid_text(name)?,
                },
                _ => unreachable!("category was validated before admission"),
            };
            owner.ddl.push(copied);
        }
        Ok(owner)
    }
}
fn owned_ddl_sidecar_key(
    original: &OwnedMemoryReservation,
    lsn: Lsn,
    ordinal: u32,
) -> Result<OwnedEventBody<String, OwnedMemoryReservation>> {
    use std::fmt::Write;
    let digits = if ordinal == 0 {
        1
    } else {
        ordinal.ilog10() as usize + 1
    };
    let size = owned_event_add("sync_ddl_generation.v1.".len() + 20 + 1, digits.max(6))?;
    let (body, credit) = new_owned_native_vec::<u8>(original, size)?;
    let mut owner = OwnedEventBody {
        body: String::from_utf8(body).expect("empty utf8 buffer"),
        credit,
    };
    write!(
        &mut owner.body,
        "sync_ddl_generation.v1.{:020}.{ordinal:06}",
        lsn.0
    )
    .map_err(|_| owned_event_overflow())?;
    if owner.body.len() != size || owner.body.capacity() != size {
        return Err(Error::Other("event sidecar key capacity mismatch".into()));
    }
    Ok(owner)
}
impl Database {
    pub(super) fn stage_owned_event_bus_ddl_for_commit(
        &self,
        events: &OwnedEventBusHandle,
        lsn: Lsn,
        ddl: &[DdlChange],
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<(u32, Option<OwnedStagedEventBusDdlCommit>)> {
        if ddl.is_empty() {
            return Ok((0, None));
        }
        let projection = events.prepare_owned_definition_projection_with_ddl(ddl, |table| {
            self.table_meta(table).is_some()
        })?;
        if projection.definitions == *events.data().common.definitions.lock() {
            return Ok((0, None));
        }
        let count = u32::try_from(ddl.len())
            .map_err(|_| Error::SyncError("event-bus DDL ordinal overflow".into()))?;
        match &self.tx_mgr {
            DatabaseTransactionHost::Owned(handle) => handle.with_host(|host| {
                validate_owned_original(
                    &events.data().original,
                    &self.finalization.accountant,
                    &host.funder,
                )
            })?,
            DatabaseTransactionHost::Legacy(_) => {
                return Err(Error::Other(
                    "owning event DDL requires its native database host".into(),
                ));
            }
        }
        let original = &events.data().original;
        let ddl = OwnedEventDdlInput::try_copy(original, ddl)?;
        let mut publication = events.prepare_owned_definition_publication_with_extra_config(
            projection,
            OwnedDefinitionMode::Additive,
            events.data().common.begin_queue_mutation(),
            ddl.ddl.len(),
        )?;
        let (body, credit) = new_owned_native_vec(original, ddl.ddl.len())?;
        let mut sidecars = OwnedEventBody { body, credit };
        for (ordinal, change) in ddl.ddl.iter().enumerate() {
            let table = Self::ddl_affected_table(change);
            let generation = table
                .map(|table| self.durable_lineage_table_generation(table, read_memory.clone()))
                .transpose()?;
            let table = match table {
                Some(table) => {
                    let (body, credit) = copy_owned_native_string(original, table)?;
                    publication.config.bodies.push(credit);
                    Some(body)
                }
                None => None,
            };
            let sidecar = super::DurableDdlGenerationSidecar {
                table,
                table_generation: generation,
            };
            let key = owned_ddl_sidecar_key(original, lsn, ordinal as u32)?;
            let encoded = encode_owned_event_value(original, &sidecar)?;
            publication.config.values.push((key.body, encoded.body));
            publication.config.bodies.push(key.credit);
            publication.config.bodies.push(encoded.credit);
            sidecars.body.push(sidecar);
        }
        let config_values = std::mem::take(&mut publication.config.values);
        let persistence = EventBusPersistenceCommit {
            config_values,
            ddl: ddl.ddl,
            generation_sidecars: sidecars.body,
        };
        let empty_config = OwnedEncodedEventBusConfig::try_empty(original)?;
        let persistence_credit = std::mem::replace(&mut publication.config, empty_config);
        Ok((
            count,
            Some(OwnedStagedEventBusDdlCommit {
                persistence,
                persistence_credit,
                publication,
                buffers: ddl.bodies,
                ddl_array: ddl.array,
                sidecar_array: sidecars.credit,
            }),
        ))
    }
    pub(super) fn commit_owned_event_bus_ddl_changes<P: OwnedEventDdlPersistence>(
        &self,
        events: &OwnedEventBusHandle,
        ddl: &[DdlChange],
        persistence: &P,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<()> {
        if !matches!(&self.tx_mgr, DatabaseTransactionHost::Owned(_)) {
            return Err(Error::Other(
                "owning event DDL requires its native database host".into(),
            ));
        }
        let initial = events.prepare_owned_definition_projection_with_ddl(ddl, |table| {
            self.table_meta(table).is_some()
        })?;
        if initial.definitions == *events.data().common.definitions.lock() {
            return Ok(());
        }
        drop(initial);
        // Same original LSN allocator and commit boundary as direct Legacy DDL.
        self.allocate_ddl_lsn(|lsn| {
            let (_, stage) =
                self.stage_owned_event_bus_ddl_for_commit(events, lsn, ddl, read_memory.clone())?;
            let Some(stage) = stage else {
                return Ok(());
            };
            let log = persistence.prepare_log(&events.data().original, lsn, &stage.persistence)?;
            let empty_events = events.prepare_owned_sink_events(&[])?;
            persistence.flush(lsn, &stage.persistence)?;
            let OwnedStagedEventBusDdlCommit {
                persistence: bodies,
                persistence_credit,
                publication,
                buffers,
                ddl_array,
                sidecar_array,
            } = stage;
            let bodies = OwnedEventDdlLogBodies {
                persistence: bodies,
                persistence_credit,
                buffers,
                ddl_array,
                sidecar_array,
            };
            events.publish_owned_definition_publication(publication, empty_events);
            // The native log adapter moves already-paid DDL/sidecars at the
            // original definitions-then-log position, with no new admission.
            persistence.publish_log(log, lsn, bodies);
            Ok(())
        })
    }
}
pub(super) struct OwnedEventDdlLogBodies {
    pub(super) persistence: EventBusPersistenceCommit,
    pub(super) persistence_credit: OwnedEncodedEventBusConfig,
    pub(super) buffers: OwnedMemoryReservation,
    pub(super) ddl_array: OwnedMemoryReservation,
    pub(super) sidecar_array: OwnedMemoryReservation,
}
pub(super) trait OwnedEventDdlPersistence {
    type Log;
    fn prepare_log(
        &self,
        original: &OwnedMemoryReservation,
        lsn: Lsn,
        commit: &EventBusPersistenceCommit,
    ) -> Result<Self::Log>;
    fn flush(&self, lsn: Lsn, commit: &EventBusPersistenceCommit) -> Result<()>;
    fn publish_log(&self, log: Self::Log, lsn: Lsn, bodies: OwnedEventDdlLogBodies);
}

// A restricted producer supplies the already-pinned committed predecessor and
// the original insertion/update/delete traversal, including ACL grant rows.
// No implementation here calls an allocating Legacy candidate/gate builder.
pub(super) trait OwnedEventCandidateSource {
    fn visit(
        &self,
        visit: impl for<'r> FnMut(
            OwnedRowEventCandidateRef<'r>,
            &'r OwnedPreparedEventGate,
        ) -> Result<()>,
    ) -> Result<()>;
}
impl OwnedPreparedSinkEvents {
    pub(super) fn with_capacity(
        original: &OwnedMemoryReservation,
        capacity: usize,
    ) -> Result<Self> {
        let (body, credit) = new_owned_native_vec(original, capacity)?;
        let events = OwnedEventBody { body, credit };
        let (body, credit) = new_owned_native_vec(original, capacity)?;
        let credits = OwnedEventBody { body, credit };
        let (body, credit) = new_owned_native_vec(original, capacity)?;
        let sink_keys = OwnedEventBody { body, credit };
        Ok(Self {
            events: events.body,
            event_array: events.credit,
            credits: credits.body,
            credit_array: credits.credit,
            sink_keys: sink_keys.body,
            key_array: sink_keys.credit,
            publication: None,
        })
    }
}
impl OwnedEventBusHandle {
    pub(super) fn prepare_owned_candidate_events<S: OwnedEventCandidateSource>(
        &self,
        source: &S,
        definitions: &EventBusDefinitions,
        lsn: Lsn,
        received_next: Option<u64>,
    ) -> Result<(OwnedPreparedSinkEvents, u64)> {
        let original = &self.data().original;
        let mut count = 0;
        source.visit(|candidate, _| {
            for event_type in definitions.event_types.values().filter(|event_type| {
                event_type.table == candidate.table && event_type.trigger == candidate.kind.legacy()
            }) {
                for route in definitions.routes.values().filter(|route| {
                    route.event_type == event_type.name && route_matches(route, candidate.values)
                }) {
                    let _ = route;
                    count = owned_event_add(count, 1)?;
                }
            }
            Ok(())
        })?;
        let mut out = OwnedPreparedSinkEvents::with_capacity(original, count)?;
        let mut next = received_next.unwrap_or_else(|| self.data().common.next_action_id());
        source.visit(|candidate, gate| {
            for event_type in definitions.event_types.values().filter(|event_type| {
                event_type.table == candidate.table && event_type.trigger == candidate.kind.legacy()
            }) {
                for route in definitions.routes.values().filter(|route| {
                    route.event_type == event_type.name && route_matches(route, candidate.values)
                }) {
                    if out.events.len() == count {
                        return Err(Error::Other("pinned event candidate source changed".into()));
                    }
                    let severity = candidate
                        .values
                        .get("severity")
                        .and_then(Value::as_text)
                        .unwrap_or("");
                    let metadata = reserve_owned_event_bytes(
                        original,
                        owned_event_add(
                            owned_event_add(event_type.name.len(), candidate.table.len())?,
                            severity.len(),
                        )?,
                    )?;
                    let (body, credit) = copy_owned_native_string(original, &route.sink)?;
                    let sink = OwnedEventBody { body, credit };
                    let (body, credit) = copy_owned_event_values(original, candidate.values)?;
                    let values = OwnedEventBody { body, credit };
                    let (body, credit) = copy_owned_event_gate(original, &gate.body)?;
                    let gate = OwnedEventBody { body, credit };
                    let name = copy_owned_prepaid_text(&event_type.name)?;
                    let table = copy_owned_prepaid_text(candidate.table)?;
                    let severity = copy_owned_prepaid_text(severity)?;
                    let action_id = if received_next.is_some() {
                        let id = next;
                        next = next.saturating_add(1);
                        id
                    } else {
                        self.data().common.allocate_queue_id()
                    };
                    out.events.push((
                        sink.body,
                        SinkQueueEntry {
                            id: action_id,
                            row_id: candidate.row_id,
                            attempts: 0,
                            gate: gate.body,
                            event: SinkEvent {
                                action_id,
                                event_type: name,
                                table,
                                row_values: values.body,
                                severity,
                                at_lsn: lsn,
                            },
                        },
                    ));
                    out.credits.push(OwnedSinkEventCredit {
                        row: values.credit,
                        gate: gate.credit,
                        metadata,
                    });
                    out.sink_keys.push(sink.credit);
                }
            }
            Ok(())
        })?;
        if out.events.len() != count {
            return Err(Error::Other("pinned event candidate source changed".into()));
        }
        let lease = self.data().common.begin_queue_mutation();
        out.publication = Some(OwnedAppendCustody::Ordinary(
            self.reserve_owned_queue_publication(&out, lease)?,
        ));
        Ok((out, next))
    }
}
pub(super) struct OwnedDeleteReleaseBytes {
    bytes: DeleteReleaseBytes,
    relational_array: OwnedMemoryReservation,
    vector_array: OwnedMemoryReservation,
}
impl OwnedDeleteReleaseBytes {
    pub(super) fn try_copy(
        original: &OwnedMemoryReservation,
        relational: &[usize],
        vectors: &[usize],
    ) -> Result<Self> {
        let (body, credit) =
            crate::memory_accounting::copy_owned_native_slice(original, relational)?;
        let relational = OwnedEventBody { body, credit };
        let (body, credit) = crate::memory_accounting::copy_owned_native_slice(original, vectors)?;
        Ok(Self {
            bytes: DeleteReleaseBytes {
                relational: relational.body,
                vectors: body,
            },
            relational_array: relational.credit,
            vector_array: credit,
        })
    }
}
impl Database {
    pub(super) fn settle_owned_delete_release_bytes(&self, bytes: OwnedDeleteReleaseBytes) {
        self.release_delete_allocations_from_bytes(&bytes.bytes);
        // Consuming custody makes successful settlement exactly once. Abort
        // simply destroys these arrays; it never calls this settlement door.
        drop(bytes);
    }
    pub(super) fn apply_owned_event_bus_ddl_from_user<P: OwnedEventDdlPersistence>(
        &self,
        events: &OwnedEventBusHandle,
        ddl: &[DdlChange],
        persistence: &P,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<()> {
        self.require_admin_event_bus_ddl()?;
        self.commit_owned_event_bus_ddl_changes(events, ddl, persistence, read_memory)
    }
}

#[derive(Clone, Copy)]
pub(super) enum OwnedRowEventKind {
    Insert,
    Update,
    Delete,
}
impl OwnedRowEventKind {
    fn legacy(self) -> EventTrigger {
        match self {
            Self::Insert => EventTrigger::Insert,
            Self::Update => EventTrigger::Update,
            Self::Delete => EventTrigger::Delete,
        }
    }
}
pub(super) struct OwnedRowEventCandidateRef<'a> {
    pub(super) kind: OwnedRowEventKind,
    pub(super) table: &'a str,
    pub(super) row_id: RowId,
    pub(super) values: &'a HashMap<String, Value>,
}
pub(super) struct OwnedPreparedEventGate {
    body: EventGateSnapshot,
    credit: OwnedEventGateCredits,
}
impl OwnedPreparedEventGate {
    fn try_from_meta(original: &OwnedMemoryReservation, meta: Option<&TableMeta>) -> Result<Self> {
        let context = meta
            .and_then(|meta| meta.columns.iter().find(|column| column.context_id))
            .map(|column| column.name.as_str());
        let scope = meta.and_then(|meta| {
            meta.columns
                .iter()
                .find_map(|column| match &column.scope_label {
                    Some(ScopeLabelKind::Split { read_labels, .. }) => {
                        Some((column.name.as_str(), read_labels.as_slice()))
                    }
                    _ => None,
                })
        });
        let mut bytes = context.map_or(0, str::len);
        if let Some((column, labels)) = scope {
            bytes = owned_event_add(bytes, column.len())?;
            bytes = owned_event_add(bytes, owned_event_array::<String>(labels.len())?)?;
            for label in labels {
                bytes = owned_event_add(bytes, label.len())?;
            }
        }
        let metadata = reserve_owned_event_bytes(original, bytes)?;
        let (body, credit) = new_owned_native_vec(original, 0)?;
        let rows = OwnedEventBody { body, credit };
        let (body, credit) = new_owned_native_vec(original, 0)?;
        let children = OwnedEventBody { body, credit };
        let mut owner = Self {
            body: EventGateSnapshot::default(),
            credit: OwnedEventGateCredits {
                rows: rows.body,
                row_array: rows.credit,
                children: children.body,
                child_array: children.credit,
                metadata,
            },
        };
        owner.body.context_column = context.map(copy_owned_prepaid_text).transpose()?;
        if let Some((column, source)) = scope {
            let mut labels = Vec::with_capacity(source.len());
            if labels.capacity() != source.len() {
                return Err(Error::Other("event scope labels capacity mismatch".into()));
            }
            for label in source {
                labels.push(copy_owned_prepaid_text(label)?);
            }
            owner.body.scope_read = Some(EventScopeReadGate {
                column: copy_owned_prepaid_text(column)?,
                read_labels: labels,
            });
        }
        Ok(owner)
    }
    pub(super) fn try_from_projection(
        original: &OwnedMemoryReservation,
        meta: Option<&TableMeta>,
        values: &HashMap<String, Value>,
        projected_tables: &HashMap<String, TableMeta>,
        projected_rows: &HashMap<String, Vec<VersionedRow>>,
    ) -> Result<Self> {
        let base = Self::try_from_meta(original, meta)?;
        let Some(column) =
            meta.and_then(|meta| meta.columns.iter().find(|column| column.acl_ref.is_some()))
        else {
            return Ok(base);
        };
        let acl = column.acl_ref.as_ref().expect("selected acl ref");
        let acl_id = match values.get(&column.name) {
            Some(Value::Uuid(id)) => Some(*id),
            _ => None,
        };
        let matches = |row: &&VersionedRow| {
            row.deleted_tx.is_none()
                && acl_id
                    .is_some_and(|id| row.values.get(&acl.ref_column) == Some(&Value::Uuid(id)))
        };
        let count = projected_rows
            .get(&acl.ref_table)
            .map_or(0, |rows| rows.iter().filter(matches).count());
        let metadata_bytes = owned_event_add(
            owned_event_add(
                owned_event_add(column.name.len(), acl.ref_table.len())?,
                acl.ref_column.len(),
            )?,
            owned_event_add(
                std::mem::size_of::<EventGateSnapshot>(),
                owned_event_array::<HashMap<String, Value>>(count)?,
            )?,
        )?;
        let metadata = reserve_owned_event_bytes(original, metadata_bytes)?;
        let (body, credit) = new_owned_native_vec(original, count)?;
        let rows = OwnedEventBody { body, credit };
        let (body, credit) = new_owned_native_vec(original, 2)?;
        let children = OwnedEventBody { body, credit };
        let mut owner = Self {
            body: base.body,
            credit: OwnedEventGateCredits {
                rows: rows.body,
                row_array: rows.credit,
                children: children.body,
                child_array: children.credit,
                metadata,
            },
        };
        owner.credit.children.push(base.credit);
        let grant_gate = Self::try_from_meta(original, projected_tables.get(&acl.ref_table))?;
        let mut grants = Vec::with_capacity(count);
        if grants.capacity() != count {
            return Err(Error::Other(
                "event projected grant capacity mismatch".into(),
            ));
        }
        if let Some(source) = projected_rows.get(&acl.ref_table) {
            for row in source.iter().filter(matches) {
                let (body, credit) = copy_owned_event_values(original, &row.values)?;
                let copy = OwnedEventBody { body, credit };
                grants.push(copy.body);
                owner.credit.rows.push(copy.credit);
            }
        }
        owner.body.acl = Some(EventAclGate {
            column: copy_owned_prepaid_text(&column.name)?,
            ref_table: copy_owned_prepaid_text(&acl.ref_table)?,
            ref_column: copy_owned_prepaid_text(&acl.ref_column)?,
            grant_gate: Box::new(grant_gate.body),
            grant_rows: EventGrantRows::Legacy(grants),
        });
        owner.credit.children.push(grant_gate.credit);
        Ok(owner)
    }
}
struct OwnedReceivedEventCandidates<'a> {
    deleted: HashMap<(&'a str, RowId), &'a VersionedRow>,
    deleted_memory: OwnedMemoryReservation,
    ws: &'a contextdb_tx::WriteSet,
    tables: &'a HashMap<String, TableMeta>,
    rows: &'a HashMap<String, Vec<VersionedRow>>,
    original: &'a OwnedMemoryReservation,
}
impl OwnedEventCandidateSource for OwnedReceivedEventCandidates<'_> {
    fn visit(
        &self,
        mut visit: impl for<'r> FnMut(
            OwnedRowEventCandidateRef<'r>,
            &'r OwnedPreparedEventGate,
        ) -> Result<()>,
    ) -> Result<()> {
        let (body, credit) = new_owned_native_set(self.original, self.deleted.len())?;
        let mut consumed = OwnedEventBody { body, credit };
        for (table, row) in &self.ws.relational_inserts {
            let same_row = (table.as_str(), row.row_id);
            let same_identity = self.deleted.iter().find_map(|((old_table, old_id), old)| {
                (*old_table == table.as_str()
                    && old.values.contains_key("id")
                    && old.values.get("id") == row.values.get("id"))
                .then_some((*old_table, *old_id))
            });
            let key = self
                .deleted
                .contains_key(&same_row)
                .then_some(same_row)
                .or(same_identity);
            let kind = if let Some(key) = key {
                consumed.body.insert(key);
                OwnedRowEventKind::Update
            } else {
                OwnedRowEventKind::Insert
            };
            let gate = OwnedPreparedEventGate::try_from_projection(
                self.original,
                self.tables.get(table),
                &row.values,
                self.tables,
                self.rows,
            )?;
            visit(
                OwnedRowEventCandidateRef {
                    kind,
                    table,
                    row_id: row.row_id,
                    values: &row.values,
                },
                &gate,
            )?;
        }
        for ((table, row_id), row) in &self.deleted {
            if !consumed.body.contains(&(*table, *row_id)) {
                let gate = OwnedPreparedEventGate::try_from_projection(
                    self.original,
                    self.tables.get(*table),
                    &row.values,
                    self.tables,
                    self.rows,
                )?;
                visit(
                    OwnedRowEventCandidateRef {
                        kind: OwnedRowEventKind::Delete,
                        table,
                        row_id: *row_id,
                        values: &row.values,
                    },
                    &gate,
                )?;
            }
        }
        Ok(())
    }
}
impl Database {
    pub(super) fn prepare_received_owned_sink_events_from_projection(
        &self,
        events: &OwnedEventBusHandle,
        host: &EngineDatabaseTransactions,
        ws: &contextdb_tx::WriteSet,
        definitions: &EventBusDefinitions,
        projected_rows: &HashMap<String, Vec<VersionedRow>>,
        projected_tables: &HashMap<String, TableMeta>,
    ) -> Result<(OwnedPreparedSinkEvents, u64)> {
        self.validate_owned_event_host(events, host)?;
        let next = events.data().common.next_action_id();
        let Some(lsn) = ws.commit_lsn else {
            return Ok((events.prepare_owned_sink_events(&[])?, next));
        };
        if definitions.event_types.is_empty() || definitions.routes.is_empty() {
            return Ok((events.prepare_owned_sink_events(&[])?, next));
        }
        let (deleted, deleted_memory) =
            new_owned_native_map(&events.data().original, ws.relational_deletes.len())?;
        let mut source = OwnedReceivedEventCandidates {
            deleted,
            deleted_memory,
            ws,
            tables: projected_tables,
            rows: projected_rows,
            original: &events.data().original,
        };
        for (table, row_id, _) in &ws.relational_deletes {
            if let Some(row) = projected_rows
                .get(table)
                .and_then(|rows| rows.iter().rev().find(|row| row.row_id == *row_id))
            {
                source.deleted.insert((table.as_str(), *row_id), row);
            }
        }
        events.prepare_owned_candidate_events(&source, definitions, lsn, Some(next))
    }
}

impl OwnedEventBusHandle {
    pub(super) fn suspend_owned_registered_append_for_discard(
        &self,
        registration: &OwnedEventStageRegistration,
    ) -> Result<OwnedDiscardAppendHandle> {
        let handle = registration.append.as_ref().ok_or_else(|| {
            Error::Other("registered stage has no append transfer control".into())
        })?;
        let mut token = handle
            .control
            .get()
            .token
            .lock()
            .take()
            .ok_or_else(|| Error::Other("registered append was already consumed".into()))?;
        if !token.storage.matches(self) || token.state != OwnedEscrowState::Active {
            *handle.control.get().token.lock() = Some(token);
            return Err(Error::Other(
                "registered discard requires its active original escrow".into(),
            ));
        }
        let mut queues = self.data().queues.lock();
        cancel_owned_slots_locked(&mut queues, &mut token.slots);
        token.state = OwnedEscrowState::Suspended;
        drop(queues);
        for slot in &mut token.slots {
            drop(slot.retired.take());
        }
        drop(token.lease.take());
        *handle.control.get().token.lock() = Some(token);
        // The actual paid control was installed before stage registration and
        // durability. The exact terminal registration is the source of this
        // capability, never a lookup in a replacement same-LSN slot.
        Ok(handle.clone())
    }
}
impl Database {
    pub(super) fn publish_owned_staged_event_bus_ddl_commit<P: OwnedEventDdlPersistence>(
        &self,
        events: &OwnedEventBusHandle,
        lsn: Lsn,
        stage: OwnedStagedEventBusDdlCommit,
        log: P::Log,
        persistence: &P,
    ) {
        let OwnedStagedEventBusDdlCommit {
            persistence: bodies,
            persistence_credit,
            publication,
            buffers,
            ddl_array,
            sidecar_array,
        } = stage;
        let bodies = OwnedEventDdlLogBodies {
            persistence: bodies,
            persistence_credit,
            buffers,
            ddl_array,
            sidecar_array,
        };
        events.publish_owned_definition_only(publication);
        persistence.publish_log(log, lsn, bodies);
    }
}

struct OwnedEventNames {
    names: Vec<String>,
    keys: Vec<OwnedMemoryReservation>,
    name_array: OwnedMemoryReservation,
    key_array: OwnedMemoryReservation,
}
struct OwnedEventCopiedMap<K, V> {
    body: HashMap<K, V>,
    keys: Vec<OwnedMemoryReservation>,
    array: OwnedMemoryReservation,
    buckets: OwnedMemoryReservation,
}
struct OwnedEventCopiedSet<K> {
    body: HashSet<K>,
    keys: Vec<OwnedCheckoutKeyCredit>,
    array: OwnedMemoryReservation,
    buckets: OwnedMemoryReservation,
}
impl OwnedEventBusHandle {
    pub(crate) fn with_owned_registered_stage_identity<R>(
        &self,
        registration: &OwnedEventStageRegistration,
        f: impl FnOnce(Lsn, EngineTerminalIdentity) -> Result<R>,
    ) -> Result<R> {
        let pin = self
            .pin_owned_registered_stage(registration)
            .ok_or_else(|| {
                Error::Other("native event apply registered stage identity mismatch".into())
            })?;
        // The supplied registration, never the looked-up stage, provides the
        // identity. The exact pin check only authenticates its installation.
        let result = f(registration.lsn, registration.identity);
        drop(pin);
        result
    }
    fn take_owned_registered_stage(
        &self,
        registration: &OwnedEventStageRegistration,
    ) -> Option<OwnedEventShared<OwnedCommitEventStage>> {
        let pin = self.pin_owned_registered_stage(registration)?;
        let mut stages = self.data().stages.lock();
        let matches = stages.values.get(&registration.lsn).is_some_and(|stage| {
            stage.identity == registration.identity
                && Arc::ptr_eq(
                    stage.data.as_ref().expect("installed stage"),
                    pin.data.as_ref().expect("registered stage"),
                )
        });
        let retired = if matches {
            stages.values.remove(&registration.lsn)
        } else {
            None
        };
        drop(stages);
        drop(pin);
        retired
    }
    pub(super) fn pin_owned_registered_stage(
        &self,
        registration: &OwnedEventStageRegistration,
    ) -> Option<OwnedEventShared<OwnedCommitEventStage>> {
        if !registration.armed
            || !Arc::ptr_eq(
                self.storage.data.as_ref().expect("live event companion"),
                registration
                    .storage
                    .storage
                    .data
                    .as_ref()
                    .expect("live registered event companion"),
            )
        {
            return None;
        }
        let stages = self.data().stages.lock();
        stages
            .values
            .get(&registration.lsn)
            .filter(|stage| {
                stage.get().identity == registration.identity
                    && Arc::ptr_eq(
                        stage.data.as_ref().expect("installed stage"),
                        registration
                            .installed
                            .data
                            .as_ref()
                            .expect("registered stage"),
                    )
            })
            .cloned()
    }
}
impl OwnedDefinitionsProjection {
    pub(super) fn with_definitions<R>(
        &self,
        f: impl for<'d> FnOnce(&'d EventBusDefinitions) -> R,
    ) -> R {
        f(&self.definitions)
    }
    pub(super) fn sanitize(&mut self, live_tables: &HashMap<String, TableMeta>) {
        self.definitions
            .event_types
            .retain(|_, def| live_tables.contains_key(&def.table));
        self.definitions.routes.retain(|_, route| {
            self.definitions.event_types.contains_key(&route.event_type)
                && self.definitions.sinks.contains_key(&route.sink)
        });
    }
}

struct OwnedOriginalEventPreimage {
    values: HashMap<String, Value>,
    credit: OriginalCopyGrant,
}
struct OwnedOrdinaryEventCandidates<'a> {
    deleted: HashMap<(&'a str, RowId), OwnedOriginalEventPreimage>,
    deleted_memory: OwnedMemoryReservation,
    database: &'a Database,
    ws: &'a contextdb_tx::WriteSet,
    original: &'a OwnedMemoryReservation,
    read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
}
impl OwnedEventCandidateSource for OwnedOrdinaryEventCandidates<'_> {
    fn visit(
        &self,
        mut visit: impl for<'r> FnMut(
            OwnedRowEventCandidateRef<'r>,
            &'r OwnedPreparedEventGate,
        ) -> Result<()>,
    ) -> Result<()> {
        let (body, credit) = new_owned_native_set(self.original, self.deleted.len())?;
        let mut consumed = OwnedEventBody { body, credit };
        for (table, row) in &self.ws.relational_inserts {
            let same_row = (table.as_str(), row.row_id);
            let same_identity = self.deleted.iter().find_map(|((old_table, old_id), old)| {
                (*old_table == table.as_str()
                    && old.values.contains_key("id")
                    && old.values.get("id") == row.values.get("id"))
                .then_some((*old_table, *old_id))
            });
            let key = self
                .deleted
                .contains_key(&same_row)
                .then_some(same_row)
                .or(same_identity);
            let kind = if let Some(key) = key {
                consumed.body.insert(key);
                OwnedRowEventKind::Update
            } else {
                OwnedRowEventKind::Insert
            };
            let gate = self.database.prepare_owned_event_gate(
                self.original,
                table,
                &row.values,
                self.ws,
                self.read_memory.clone(),
            )?;
            visit(
                OwnedRowEventCandidateRef {
                    kind,
                    table,
                    row_id: row.row_id,
                    values: &row.values,
                },
                &gate,
            )?;
        }
        for ((table, row_id), row) in &self.deleted {
            if !consumed.body.contains(&(*table, *row_id)) {
                let gate = self.database.prepare_owned_event_gate(
                    self.original,
                    table,
                    &row.values,
                    self.ws,
                    self.read_memory.clone(),
                )?;
                visit(
                    OwnedRowEventCandidateRef {
                        kind: OwnedRowEventKind::Delete,
                        table,
                        row_id: *row_id,
                        values: &row.values,
                    },
                    &gate,
                )?;
            }
        }
        Ok(())
    }
}
impl Database {
    fn with_owned_event_preimage<R>(
        &self,
        original: &OwnedMemoryReservation,
        table: &str,
        row_id: RowId,
        snapshot: SnapshotId,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        f: impl for<'r> FnOnce(Option<&'r VersionedRow>) -> Result<R>,
    ) -> Result<R> {
        if let Some(root) = self.writer_read_root()? {
            let source = crate::persistence::writer_adapters::WriterRelationalSource::new_for_read(
                root,
                read_memory,
            )?;
            let row = contextdb_relational::source::visible_row(&source, table, row_id, snapshot)?;
            return f(row.as_ref().map(|row| row.value()));
        }
        // This unchanged indexed observation helper creates one temporary
        // table String. Admit its exact source length BEFORE the helper; its
        // temporary dies inside the call, before this invoice is released.
        let lookup_memory = reserve_owned_event_bytes(original, table.len())?;
        let tables = self.relational_store.tables.read();
        let row = tables.get(table).and_then(|rows| {
            self.relational_store
                .visible_version_position(table, rows, row_id, snapshot)
                .and_then(|position| rows.get(position))
        });
        drop(lookup_memory);
        f(row)
    }
    fn visit_owned_event_grants(
        &self,
        acl: &AclRef,
        acl_id: uuid::Uuid,
        snapshot: SnapshotId,
        ws: &contextdb_tx::WriteSet,
        deleted: &HashSet<RowId>,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        mut visit: impl FnMut(&VersionedRow) -> Result<()>,
    ) -> Result<()> {
        let matches = |row: &VersionedRow| {
            row.visible_at(snapshot)
                && !deleted.contains(&row.row_id)
                && row.values.get(&acl.ref_column) == Some(&Value::Uuid(acl_id))
        };
        let visited = self.visit_writer_rows::<Error>(
            &acl.ref_table,
            |row| {
                if matches(row) {
                    visit(row)?;
                }
                Ok(())
            },
            read_memory,
        )?;
        if !visited {
            let tables = self.relational_store.tables.read();
            for row in tables.get(&acl.ref_table).into_iter().flatten() {
                if matches(row) {
                    visit(row)?;
                }
            }
        }
        for (table, row) in &ws.relational_inserts {
            if table == &acl.ref_table
                && row.deleted_tx.is_none()
                && row.values.get(&acl.ref_column) == Some(&Value::Uuid(acl_id))
            {
                visit(row)?;
            }
        }
        Ok(())
    }
    pub(super) fn prepare_owned_event_gate(
        &self,
        original: &OwnedMemoryReservation,
        table: &str,
        values: &HashMap<String, Value>,
        ws: &contextdb_tx::WriteSet,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<OwnedPreparedEventGate> {
        self.with_owned_event_meta_pair(table, |meta, grant_meta| {
            let Some(meta) = meta else {
                return OwnedPreparedEventGate::try_from_meta(original, None);
            };
            let base = OwnedPreparedEventGate::try_from_meta(original, Some(meta))?;
            let Some(column) = meta.columns.iter().find(|column| column.acl_ref.is_some()) else {
                return Ok(base);
            };
            let acl = column.acl_ref.as_ref().expect("selected acl ref");
            let acl_id = match values.get(&column.name) {
                Some(Value::Uuid(id)) => Some(*id),
                _ => None,
            };
            let (body, credit) = new_owned_native_set(original, ws.relational_deletes.len())?;
            let mut deleted = OwnedEventBody { body, credit };
            for (table, id, _) in &ws.relational_deletes {
                if table == &acl.ref_table {
                    deleted.body.insert(*id);
                }
            }
            let snapshot = self.snapshot();
            let mut count = 0;
            if let Some(id) = acl_id {
                self.visit_owned_event_grants(
                    acl,
                    id,
                    snapshot,
                    ws,
                    &deleted.body,
                    read_memory.clone(),
                    |_| {
                        count = owned_event_add(count, 1)?;
                        Ok(())
                    },
                )?;
            }
            let bytes = owned_event_add(
                owned_event_add(
                    owned_event_add(column.name.len(), acl.ref_table.len())?,
                    acl.ref_column.len(),
                )?,
                owned_event_add(
                    std::mem::size_of::<EventGateSnapshot>(),
                    owned_event_array::<HashMap<String, Value>>(count)?,
                )?,
            )?;
            let metadata = reserve_owned_event_bytes(original, bytes)?;
            let (body, credit) = new_owned_native_vec(original, count)?;
            let rows = OwnedEventBody { body, credit };
            let (body, credit) = new_owned_native_vec(original, 2)?;
            let children = OwnedEventBody { body, credit };
            let mut owner = OwnedPreparedEventGate {
                body: base.body,
                credit: OwnedEventGateCredits {
                    rows: rows.body,
                    row_array: rows.credit,
                    children: children.body,
                    child_array: children.credit,
                    metadata,
                },
            };
            owner.credit.children.push(base.credit);
            let grant_gate = OwnedPreparedEventGate::try_from_meta(original, grant_meta)?;
            let mut grants = Vec::with_capacity(count);
            if grants.capacity() != count {
                return Err(Error::Other("event grant capacity mismatch".into()));
            }
            if let Some(id) = acl_id {
                self.visit_owned_event_grants(
                    acl,
                    id,
                    snapshot,
                    ws,
                    &deleted.body,
                    read_memory.clone(),
                    |row| {
                        if grants.len() == count {
                            return Err(Error::Other("pinned event grant source changed".into()));
                        }
                        let (body, credit) = copy_owned_event_values(original, &row.values)?;
                        let copy = OwnedEventBody { body, credit };
                        grants.push(copy.body);
                        owner.credit.rows.push(copy.credit);
                        Ok(())
                    },
                )?;
            }
            if grants.len() != count {
                return Err(Error::Other("pinned event grant source changed".into()));
            }
            owner.body.acl = Some(EventAclGate {
                column: copy_owned_prepaid_text(&column.name)?,
                ref_table: copy_owned_prepaid_text(&acl.ref_table)?,
                ref_column: copy_owned_prepaid_text(&acl.ref_column)?,
                grant_gate: Box::new(grant_gate.body),
                grant_rows: EventGrantRows::Legacy(grants),
            });
            owner.credit.children.push(grant_gate.credit);
            Ok(owner)
        })
    }
    pub(super) fn prepare_owned_sink_events_in_prepared_write_set(
        &self,
        events: &OwnedEventBusHandle,
        host: &EngineDatabaseTransactions,
        guard: &contextdb_tx::manager::EngineGuard<'_>,
        event_bus_ddl: &[DdlChange],
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<OwnedPreparedSinkEvents> {
        self.validate_owned_event_host(events, host)?;
        guard.with_legacy(|ws| {
            let Some(lsn) = ws.commit_lsn else {
                return events.prepare_owned_sink_events(&[]);
            };
            if ws.relational_inserts.is_empty() && ws.relational_deletes.is_empty() {
                return events.prepare_owned_sink_events(&[]);
            }
            let definitions = events
                .prepare_owned_definition_projection_with_ddl(event_bus_ddl, |table| {
                    self.table_meta(table).is_some()
                })?;
            if definitions.definitions.event_types.is_empty()
                || definitions.definitions.routes.is_empty()
            {
                return events.prepare_owned_sink_events(&[]);
            }
            // Capture the SAME committed-predecessor cut as the original,
            // independently of the transaction's retained base read.
            let published = self.tx_mgr.capture_published_read();
            let _current_source = self.enter_writer_read(
                super::writer_read::durable_root(published.handle)?,
                read_memory.clone(),
            );
            let (deleted, deleted_memory) =
                new_owned_native_map(&events.data().original, ws.relational_deletes.len())?;
            let mut source = OwnedOrdinaryEventCandidates {
                deleted,
                deleted_memory,
                database: self,
                ws,
                original: &events.data().original,
                read_memory: read_memory.clone(),
            };
            let snapshot = self.snapshot();
            for (table, id, _) in &ws.relational_deletes {
                let preimage = self.with_owned_event_preimage(
                    &events.data().original,
                    table,
                    *id,
                    snapshot,
                    read_memory.clone(),
                    |row| {
                        row.map(|row| {
                            let (values, credit) =
                                crate::memory_accounting::copy_owned_event_row_values(
                                    &events.data().original,
                                    row,
                                )?;
                            Ok(OwnedOriginalEventPreimage { values, credit })
                        })
                        .transpose()
                    },
                )?;
                if let Some(preimage) = preimage {
                    source.deleted.insert((table.as_str(), *id), preimage);
                }
            }
            events
                .prepare_owned_candidate_events(&source, &definitions.definitions, lsn, None)
                .map(|(events, _)| events)
        })
    }
}

impl Database {
    fn with_owned_event_meta_pair<R>(
        &self,
        table: &str,
        f: impl for<'m> FnOnce(Option<&'m TableMeta>, Option<&'m TableMeta>) -> Result<R>,
    ) -> Result<R> {
        if let Some(root) = self.scoped_writer_root() {
            let meta = root.borrow_captured_table_meta(table);
            let grant_meta = meta
                .and_then(|meta| {
                    meta.columns
                        .iter()
                        .find_map(|column| column.acl_ref.as_ref())
                })
                .and_then(|acl| root.borrow_captured_table_meta(&acl.ref_table));
            f(meta, grant_meta)
        } else {
            // One catalog guard supplies BOTH borrowed metas. Never recursively
            // acquire a read guard behind a waiting catalog writer.
            let catalog = self.relational_store.table_meta.read();
            let meta = catalog.get(table);
            let grant_meta = meta
                .and_then(|meta| {
                    meta.columns
                        .iter()
                        .find_map(|column| column.acl_ref.as_ref())
                })
                .and_then(|acl| catalog.get(&acl.ref_table));
            f(meta, grant_meta)
        }
    }
}

impl Database {
    fn validate_owned_event_host(
        &self,
        events: &OwnedEventBusHandle,
        host: &EngineDatabaseTransactions,
    ) -> Result<()> {
        match &self.tx_mgr {
            DatabaseTransactionHost::Owned(handle) => handle.with_host(|actual| {
                if !std::ptr::eq(actual, host) {
                    return Err(Error::Other(
                        "owning events require the selected original transaction host".into(),
                    ));
                }
                validate_owned_original(
                    &events.data().original,
                    &self.finalization.accountant,
                    &actual.funder,
                )
            }),
            DatabaseTransactionHost::Legacy(_) => Err(Error::Other(
                "owning events require the selected native transaction host".into(),
            )),
        }
    }
}

impl OwnedEventBusHandle {
    /// The companion owns this immutable seed outside every mutable queue,
    /// stage and state guard. The shared storage cannot retire while self is
    /// lent, including callback failure and unwinding.
    pub(crate) fn with_owned_persistence_original<R>(
        &self,
        f: impl for<'o> FnOnce(&'o OwnedMemoryReservation) -> Result<R>,
    ) -> Result<R> {
        with_owned_persistence_original_seed(&self.storage.get().original, f)
    }
}

impl Database {
    pub(super) fn validate_owned_cron_store(
        &self,
        events: &OwnedEventBusHandle,
        host: &EngineDatabaseTransactions,
        store: &crate::persistent_store::OwnedPersistentCompositeStore,
    ) -> Result<()> {
        self.validate_owned_event_host(events, host)?;
        if !Arc::ptr_eq(&events.data().common, &self.event_bus) {
            return Err(Error::Other(
                "native cron requires this database's event companion".into(),
            ));
        }
        host.transactions.with_transactions(|lower| {
            let installed: &dyn contextdb_tx::WriteSetApplicator = lower.store().as_ref();
            if installed as *const dyn contextdb_tx::WriteSetApplicator as *const ()
                != store as *const _ as *const ()
            {
                return Err(Error::Other(
                    "native cron requires its host's installed store".into(),
                ));
            }
            Ok(())
        })
    }

    pub(super) fn prepare_owned_delete_release_bytes_in_prepared_write_set(
        &self,
        events: &OwnedEventBusHandle,
        host: &EngineDatabaseTransactions,
        guard: &contextdb_tx::manager::EngineGuard<'_>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<OwnedDeleteReleaseBytes> {
        self.validate_owned_event_host(events, host)?;
        events.with_owned_persistence_original(|original| {
            guard.with_legacy(|ws| {
                let mut relational_count = 0;
                let mut vector_count = 0;
                self.visit_owned_delete_release_bytes(
                    original,
                    ws,
                    vector_copy_owner,
                    read_memory.clone(),
                    |vector, _| {
                        let count = if vector {
                            &mut vector_count
                        } else {
                            &mut relational_count
                        };
                        *count = owned_event_add(*count, 1)?;
                        Ok(())
                    },
                )?;
                // Both arrays receive their original preparation invoices BEFORE allocation.
                let (body, credit) = new_owned_native_vec(original, relational_count)?;
                let relational = OwnedEventBody { body, credit };
                let (body, credit) = new_owned_native_vec(original, vector_count)?;
                let mut release = OwnedDeleteReleaseBytes {
                    bytes: DeleteReleaseBytes {
                        relational: relational.body,
                        vectors: body,
                    },
                    relational_array: relational.credit,
                    vector_array: credit,
                };
                self.visit_owned_delete_release_bytes(
                    original,
                    ws,
                    vector_copy_owner,
                    read_memory,
                    |vector, bytes| {
                        let (array, count) = if vector {
                            (&mut release.bytes.vectors, vector_count)
                        } else {
                            (&mut release.bytes.relational, relational_count)
                        };
                        if array.len() == count {
                            return Err(Error::Other(
                                "native delete release source changed".into(),
                            ));
                        }
                        array.push(bytes);
                        Ok(())
                    },
                )?;
                if release.bytes.relational.len() != relational_count
                    || release.bytes.vectors.len() != vector_count
                {
                    return Err(Error::Other("native delete release source changed".into()));
                }
                Ok(release)
            })
        })
    }

    fn visit_owned_delete_release_bytes(
        &self,
        original: &OwnedMemoryReservation,
        ws: &contextdb_tx::WriteSet,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        mut visit: impl FnMut(bool, usize) -> Result<()>,
    ) -> Result<()> {
        let snapshot = SnapshotId::from_raw_wire(u64::MAX);
        for (table, row_id, _) in &ws.relational_deletes {
            self.with_owned_event_meta_pair(table, |meta, _| {
                // The same resident indexed selection as row_by_id, borrowing
                // the row instead of making an unadmitted payload copy.
                let lookup_memory = reserve_owned_event_bytes(original, table.len())?;
                let tables = self.relational_store.tables.read();
                let row = tables.get(table).and_then(|rows| {
                    self.relational_store
                        .visible_version_position(table, rows, *row_id, snapshot)
                        .and_then(|position| rows.get(position))
                });
                drop(lookup_memory);
                if let Some(row) = row {
                    visit(
                        false,
                        retained_row_bytes_with_optional_meta(table, row, meta),
                    )?;
                }
                Ok(())
            })?;
        }
        // State-owned vector bodies retain their charge until physical removal.
        // Adjacency likewise remains charged; this commit settles neither.
        for (index, row_id, _) in &ws.vector_deletes {
            if self
                .vector_store
                .raw_entry_charge_owned_by_state(index, *row_id)
            {
                continue;
            }
            if let Some(vector) = self.find_vector_by_index_and_row(
                index,
                *row_id,
                vector_copy_owner,
                read_memory.clone(),
            )? {
                visit(
                    true,
                    self.vector_insert_accounted_bytes(
                        index,
                        vector.vector.len(),
                        vector_copy_owner,
                    )?,
                )?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "sink_window_tests.rs"]
mod sink_window_tests;
