use super::*;
use std::cell::Cell;

const EVENT_TYPES_CONFIG_KEY: &str = "__event_bus_event_types";
const SINKS_CONFIG_KEY: &str = "__event_bus_sinks";
const ROUTES_CONFIG_KEY: &str = "__event_bus_routes";
pub(crate) const MAX_SINK_QUEUE_DEPTH: usize = 100_000;
const SINK_DISPATCH_BATCH: usize = 1024;

type SinkCallback = Arc<dyn Fn(&SinkEvent) -> std::result::Result<(), SinkError> + Send + Sync>;

pub(crate) struct EventBusState {
    definitions: Mutex<EventBusDefinitions>,
    callbacks: RwLock<HashMap<String, SinkRegistration>>,
    queues: Mutex<HashMap<String, VecDeque<SinkQueueEntry>>>,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SinkQueueEntry {
    pub(crate) id: u64,
    pub(crate) event: SinkEvent,
    pub(crate) row_id: RowId,
    pub(crate) attempts: u32,
    #[serde(default)]
    gate: EventGateSnapshot,
}

#[derive(Default)]
struct QueueMutationState {
    checkout_paused: bool,
    mutation_blocked: bool,
    checked_out: HashSet<(String, u64)>,
    active_mutations: u64,
}

thread_local! {
    static EVENT_BUS_CALLBACK_DEPTH: Cell<u32> = const { Cell::new(0) };
}

struct EventBusCallbackGuard;

impl EventBusCallbackGuard {
    fn enter() -> Self {
        EVENT_BUS_CALLBACK_DEPTH.with(|depth| depth.set(depth.get().saturating_add(1)));
        Self
    }
}

impl Drop for EventBusCallbackGuard {
    fn drop(&mut self) {
        EVENT_BUS_CALLBACK_DEPTH.with(|depth| depth.set(depth.get().saturating_sub(1)));
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
        table: &str,
        row_ids: &HashSet<RowId>,
    ) -> PreparedAuthoritativePurgeEventBusPublication {
        let mut sink_names = durable_sink_names;
        sink_names.extend(self.queues.lock().keys().cloned());
        let mut queues = self.queues.lock().clone();
        for sink in &sink_names {
            let entries = queues.entry(sink.clone()).or_default();
            entries.retain(|entry| entry.event.table != table || !row_ids.contains(&entry.row_id));
        }
        PreparedAuthoritativePurgeEventBusPublication {
            queues,
            sink_names,
            _queue_mutation_token: queue_mutation_token,
        }
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
        *self.queues.lock() = publication.queues;
        self.waiters.notify_all();
    }

    #[cfg(feature = "test-seams")]
    pub(crate) fn authoritative_purge_memory_sink_queue_for_test(
        &self,
        sink: &str,
    ) -> BTreeMap<RowId, HashMap<String, Value>> {
        self.queues
            .lock()
            .get(sink)
            .into_iter()
            .flatten()
            .map(|entry| (entry.row_id, entry.event.row_values.clone()))
            .collect()
    }
}

/// Opaque full queue replacement for authoritative purge.  Keeping the
/// VecDeque private prevents a later kernel from rebuilding entries after
/// durability and accidentally losing retry or gate state.
pub(crate) struct PreparedAuthoritativePurgeEventBusPublication {
    queues: HashMap<String, VecDeque<SinkQueueEntry>>,
    sink_names: BTreeSet<String>,
    _queue_mutation_token: AuthoritativePurgeQueueMutationToken,
}

impl PreparedAuthoritativePurgeEventBusPublication {
    pub(crate) fn authoritative_purge_sink_names(&self) -> &BTreeSet<String> {
        &self.sink_names
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
    permanent_failures: u64,
    in_flight: u64,
    /// Queue entries pulled to the front and gate-evaluated by the dispatcher
    /// (delivered, permanently failed, or denied-and-rotated alike). Always-on,
    /// carried on this existing per-sink metrics mutex; see `SinkMetrics::examined`.
    examined: u64,
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
}

impl EventBusResourceCleanup {
    fn close(self) {
        if let Some(persistence) = self.persistence {
            persistence.close();
        }
        if let Some(path) = self.open_registry_path {
            release_open_registry_path(&path);
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
    grant_rows: Vec<HashMap<String, Value>>,
}

impl EventBusState {
    pub(super) fn new() -> Self {
        Self {
            definitions: Mutex::new(EventBusDefinitions::default()),
            callbacks: RwLock::new(HashMap::new()),
            queues: Mutex::new(HashMap::new()),
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
        }
    }

    fn allocate_queue_id(&self) -> u64 {
        self.next_queue_id.fetch_add(1, Ordering::SeqCst)
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
    #[allow(dead_code)] // consumed when Phase B joins the staged Redb transaction
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

    pub(super) fn received_event_bus_projection(&self) -> EventBusDefinitions {
        self.event_bus.definitions.lock().clone()
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
            queues.entry(sink.clone()).or_default();
        }
        for (sink, entry) in received_events {
            let queue = queues.entry(sink).or_default();
            if queue.len() == MAX_SINK_QUEUE_DEPTH {
                queue.pop_front();
            }
            queue.push_back(entry);
        }
        drop(queues);
        self.event_bus.waiters.notify_all();
    }

    pub(super) fn load_event_bus_state_from_persistence(&self) -> Result<()> {
        let Some(persistence) = self.persistence.as_ref() else {
            return Ok(());
        };
        let definitions = EventBusDefinitions {
            event_types: persistence
                .load_config_value::<Vec<EventTypeDef>>(EVENT_TYPES_CONFIG_KEY)?
                .unwrap_or_default()
                .into_iter()
                .map(|event_type| (event_type.name.clone(), event_type))
                .collect(),
            sinks: persistence
                .load_config_value::<Vec<SinkDef>>(SINKS_CONFIG_KEY)?
                .unwrap_or_default()
                .into_iter()
                .map(|sink| (sink.name.clone(), sink))
                .collect(),
            routes: persistence
                .load_config_value::<Vec<RouteDef>>(ROUTES_CONFIG_KEY)?
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
        let mut max_id = 0u64;
        for sink in sink_names {
            let queue = persistence.load_sink_queue::<SinkQueueEntry>(&sink)?;
            max_id = max_id.max(queue.iter().map(|entry| entry.id).max().unwrap_or(0));
            queues.insert(sink, queue.into());
        }
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

    pub(super) fn apply_event_bus_ddl_from_user(&self, ddl: Vec<DdlChange>) -> Result<()> {
        self.require_admin_event_bus_ddl()?;
        self.apply_event_bus_ddl_batch(ddl)
    }

    pub(super) fn stage_event_bus_ddl_for_commit(
        &self,
        lsn: Lsn,
        ddl: &[DdlChange],
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
        let generation_sidecars = self.ddl_generation_sidecars(ddl)?;
        let provenance_values = self.ddl_generation_sidecar_values(lsn, ddl, 0)?;
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

    pub(super) fn apply_event_bus_ddl_batch(&self, ddl: Vec<DdlChange>) -> Result<()> {
        if ddl.is_empty() {
            return Ok(());
        }
        let projected = self.event_bus_definitions_with_ddl(None, &ddl)?;
        if *self.event_bus.definitions.lock() == projected {
            return Ok(());
        }
        self.commit_event_bus_definitions_and_log_batch(projected, ddl)
    }

    fn event_bus_definitions_with_ddl(
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

    /// Register or replace the in-process callback for a declared sink.
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

    #[cfg(any(test, feature = "test-seams"))]
    pub fn sink_metrics_for_test(&self, sink: &str) -> SinkMetrics {
        let _operation = self.assert_open_operation();
        let queued = self
            .event_bus
            .queues
            .lock()
            .get(sink)
            .map(|queue| queue.len() as u64)
            .unwrap_or(0);
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
            permanent_failures: metrics.permanent_failures,
            examined: metrics.examined,
        }
    }

    /// Ordered in-memory queue identity/payload view for received-schema race
    /// tests. The accessor is compiled only for tests; production dispatch
    /// continues to own and mutate the queue privately.
    #[cfg(test)]
    pub fn sink_queue_entries_for_test(&self, sink: &str) -> Vec<(u64, SinkEvent)> {
        let _operation = self.assert_open_operation();
        self.event_bus
            .queues
            .lock()
            .get(sink)
            .map(|queue| {
                queue
                    .iter()
                    .map(|entry| (entry.id, entry.event.clone()))
                    .collect()
            })
            .unwrap_or_default()
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
    ) -> Result<Vec<PreparedSinkEvent>> {
        self.prepare_sink_events_for_write_set_with_event_bus_ddl(ws, &[])
    }

    pub(super) fn prepare_sink_events_for_write_set_with_event_bus_ddl(
        &self,
        ws: &contextdb_tx::WriteSet,
        event_bus_ddl: &[DdlChange],
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
                );
            }
            return Ok(out);
        }
        let candidates = self.row_event_candidates(ws)?;
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
            );
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

    pub(super) fn prepare_received_sink_events_for_publication(
        &self,
        ws: &contextdb_tx::WriteSet,
        publication: &PreparedEventBusPublication,
        projected_rows: &HashMap<String, Vec<VersionedRow>>,
        projected_tables: &HashMap<String, TableMeta>,
    ) -> PreparedReceivedSinkEvents {
        self.prepare_received_sink_events_from_projection(
            ws,
            &publication.definitions,
            projected_rows,
            projected_tables,
        )
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
                out.push((
                    route.sink.clone(),
                    SinkQueueEntry {
                        id: *next_queue_id,
                        event: SinkEvent {
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
            let gate = self.event_gate_snapshot_cached(
                candidate.table,
                candidate.values,
                ws,
                static_gate_cache,
            );
            for route in definitions
                .routes
                .values()
                .filter(|route| route.event_type == event_type.name)
            {
                if !route_matches(route, candidate.values) {
                    continue;
                }
                let event = SinkEvent {
                    event_type: event_type.name.clone(),
                    table: candidate.table.to_string(),
                    severity: severity.clone(),
                    row_values: candidate.values.clone(),
                    at_lsn: lsn,
                };
                out.push((
                    route.sink.clone(),
                    SinkQueueEntry {
                        id: self.event_bus.allocate_queue_id(),
                        event,
                        row_id: candidate.row_id,
                        attempts: 0,
                        gate: gate.clone(),
                    },
                ));
            }
        }
    }

    fn event_gate_snapshot_cached(
        &self,
        table: &str,
        values: &HashMap<String, Value>,
        ws: &contextdb_tx::WriteSet,
        static_gate_cache: &mut HashMap<String, EventGateSnapshot>,
    ) -> EventGateSnapshot {
        if let Some(gate) = static_gate_cache.get(table) {
            return gate.clone();
        }
        let Some(meta) = self.table_meta(table) else {
            return EventGateSnapshot::default();
        };
        if meta.columns.iter().all(|column| column.acl_ref.is_none()) {
            let gate = event_gate_snapshot_from_meta(&meta);
            static_gate_cache.insert(table.to_string(), gate.clone());
            return gate;
        }
        self.event_gate_snapshot_with_meta(&meta, values, ws)
    }

    fn event_gate_snapshot_with_meta(
        &self,
        meta: &TableMeta,
        values: &HashMap<String, Value>,
        ws: &contextdb_tx::WriteSet,
    ) -> EventGateSnapshot {
        let snapshot = self.snapshot();
        let mut gate = event_gate_snapshot_from_meta(meta);
        let acl = meta
            .columns
            .iter()
            .find(|column| column.acl_ref.is_some())
            .and_then(|column| {
                let acl_ref = column.acl_ref.as_ref()?.clone();
                let grant_meta = self.table_meta(&acl_ref.ref_table);
                let grant_gate = grant_meta
                    .as_ref()
                    .map(event_gate_snapshot_from_meta)
                    .unwrap_or_default();
                let acl_id = match values.get(&column.name) {
                    Some(Value::Uuid(acl_id)) => Some(*acl_id),
                    _ => None,
                };
                let grant_rows = acl_id
                    .map(|acl_id| self.grant_rows_for_acl(&acl_ref, acl_id, snapshot, ws))
                    .unwrap_or_default();
                Some(EventAclGate {
                    column: column.name.clone(),
                    ref_table: acl_ref.ref_table,
                    ref_column: acl_ref.ref_column,
                    grant_gate: Box::new(grant_gate),
                    grant_rows,
                })
            });
        gate.acl = acl;
        gate
    }

    fn grant_rows_for_acl(
        &self,
        acl_ref: &AclRef,
        acl_id: uuid::Uuid,
        snapshot: SnapshotId,
        ws: &contextdb_tx::WriteSet,
    ) -> Vec<HashMap<String, Value>> {
        let deleted_row_ids = ws
            .relational_deletes
            .iter()
            .filter_map(|(table, row_id, _)| (table == &acl_ref.ref_table).then_some(*row_id))
            .collect::<HashSet<_>>();

        let mut grants = self
            .relational_store
            .tables
            .read()
            .get(&acl_ref.ref_table)
            .map(|rows| {
                rows.iter()
                    .filter(|row| {
                        row.visible_at(snapshot)
                            && !deleted_row_ids.contains(&row.row_id)
                            && row.values.get(&acl_ref.ref_column) == Some(&Value::Uuid(acl_id))
                    })
                    .map(|row| row.values.clone())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        grants.extend(
            ws.relational_inserts
                .iter()
                .filter(|(table, row)| {
                    table == &acl_ref.ref_table
                        && row.deleted_tx.is_none()
                        && row.values.get(&acl_ref.ref_column) == Some(&Value::Uuid(acl_id))
                })
                .map(|(_, row)| row.values.clone()),
        );
        grants
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
            let queue = queues.entry(sink).or_default();
            if queue.len() == MAX_SINK_QUEUE_DEPTH {
                queue.pop_front();
            }
            queue.push_back(entry);
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
        if let Some((sink, handle)) = current_dispatcher {
            drop(handle);
            self.defer_current_dispatcher_resource_cleanup(sink);
            return EventBusThreadShutdown::DeferredResourceCleanup;
        }
        EventBusThreadShutdown::AllJoined
    }

    fn defer_current_dispatcher_resource_cleanup(&self, sink: String) {
        // The current dispatcher still has to ack the callback's event after
        // Database::drop returns, so it closes file resources at loop exit.
        let cleanup = EventBusResourceCleanup {
            sink,
            persistence: self.persistence.clone(),
            open_registry_path: self.open_registry_path.lock().take(),
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
        };
        let thread_shutdown = shutdown.clone();
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
    ) -> Result<()> {
        if *self.event_bus.definitions.lock() == definitions {
            return Ok(());
        }
        self.commit_event_bus_ddl_changes(ddl)
    }

    fn commit_event_bus_ddl_changes(&self, ddl: Vec<DdlChange>) -> Result<()> {
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
                let provenance_values = self.ddl_generation_sidecar_values(lsn, &ddl, 0)?;
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
                self.record_ddl_generation_sidecars(lsn, &ddl)?;
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
            queues.entry(sink).or_default();
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

    fn row_event_candidates(&self, ws: &contextdb_tx::WriteSet) -> Result<Vec<RowEventCandidate>> {
        let snapshot = self.snapshot();
        let mut delete_preimages: HashMap<(String, RowId), VersionedRow> = HashMap::new();
        for (table, row_id, _) in &ws.relational_deletes {
            if let Some(row) = self.relational_store.row_by_id(table, *row_id, snapshot) {
                delete_preimages.insert((table.clone(), *row_id), row);
            }
        }

        let mut consumed_deletes = HashSet::new();
        let mut candidates = Vec::new();
        for (table, row) in &ws.relational_inserts {
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
        Ok(candidates)
    }
}

struct EventBusWorker {
    sink: String,
    event_bus: Arc<EventBusState>,
    persistence: Option<Arc<RedbPersistence>>,
}

impl EventBusWorker {
    fn dispatch_loop(self, shutdown: Arc<AtomicBool>) {
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
            let entries = {
                let mut queues = self.event_bus.queues.lock();
                let Some(queue) = queues.get_mut(&self.sink) else {
                    drop(queues);
                    drop(queue_mutation);
                    self.wait(&shutdown, Duration::from_millis(250));
                    continue;
                };
                let Some(front) = queue.front().cloned() else {
                    drop(queues);
                    drop(queue_mutation);
                    self.wait(&shutdown, Duration::from_millis(250));
                    continue;
                };
                if !event_visible_for_access(&registration.access, &front) {
                    if let Some(entry) = queue.pop_front() {
                        queue.push_back(entry);
                    }
                    drop(queues);
                    self.record_examined(1);
                    self.event_bus.waiters.notify_all();
                    drop(queue_mutation);
                    self.wait(&shutdown, Duration::from_millis(10));
                    continue;
                }
                queue
                    .iter()
                    .take(SINK_DISPATCH_BATCH)
                    .take_while(|entry| event_visible_for_access(&registration.access, entry))
                    .cloned()
                    .collect::<Vec<_>>()
            };
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
            for (entry_position, entry) in entries.iter().cloned().enumerate() {
                if shutdown.load(Ordering::SeqCst) {
                    for pending in entries.iter().skip(entry_position) {
                        self.event_bus
                            .finish_checked_out_entry(&self.sink, pending.id);
                    }
                    break;
                }
                examined_count = examined_count.saturating_add(1);
                self.add_in_flight(1);
                let callback = registration.callback.clone();
                let result = {
                    let _callback = EventBusCallbackGuard::enter();
                    catch_unwind(AssertUnwindSafe(|| callback(&entry.event)))
                };
                self.add_in_flight(-1);

                match result {
                    Ok(Ok(())) => {
                        delivered_count = delivered_count.saturating_add(1);
                        acked.push(entry.id);
                    }
                    Ok(Err(SinkError::Transient(_))) => {
                        let _ = self.ack_front_entries(&acked);
                        // Same combined flush as the post-batch path: examined
                        // must ride along or transient retries undercount it
                        // (the retried entry itself counts again on re-examine
                        // — the documented inflation full-sweep callers avoid).
                        self.record_batch_outcome(
                            delivered_count,
                            permanent_failure_count,
                            examined_count,
                            1,
                        );
                        let attempts = self.bump_attempts(entry.id).unwrap_or(entry.attempts + 1);
                        let backoff = retry_backoff(attempts);
                        for pending in entries.iter().skip(entry_position + 1) {
                            self.event_bus
                                .finish_checked_out_entry(&self.sink, pending.id);
                        }
                        self.wait(&shutdown, backoff);
                        continue 'dispatch;
                    }
                    Ok(Err(SinkError::Permanent(_))) => {
                        permanent_failure_count = permanent_failure_count.saturating_add(1);
                        acked.push(entry.id);
                    }
                    Err(payload) => {
                        let panic_message = panic_payload_to_string(payload);
                        tracing::warn!(
                            sink = %self.sink,
                            entry_id = entry.id,
                            panic_message = %panic_message,
                            "sink callback panicked"
                        );
                        permanent_failure_count = permanent_failure_count.saturating_add(1);
                        acked.push(entry.id);
                    }
                }
            }
            let _ = self.ack_front_entries(&acked);
            self.record_batch_outcome(delivered_count, permanent_failure_count, examined_count, 0);
        }
        self.finish_deferred_resource_cleanup();
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
    ) {
        if delivered == 0 && permanent_failures == 0 && examined == 0 && retried == 0 {
            return;
        }
        let mut metrics = self.event_bus.metrics.lock();
        let state = metrics.entry(self.sink.clone()).or_default();
        state.delivered += delivered;
        state.permanent_failures += permanent_failures;
        state.examined += examined;
        state.retried += retried;
    }

    fn bump_attempts(&self, entry_id: u64) -> Result<u32> {
        let result = {
            let _queue_mutation = self.event_bus.begin_queue_mutation();
            (|| -> Result<Option<SinkQueueEntry>> {
                let mut queues = self.event_bus.queues.lock();
                let Some(queue) = queues.get_mut(&self.sink) else {
                    return Ok(None);
                };
                let Some(entry) = queue.front_mut().filter(|entry| entry.id == entry_id) else {
                    return Ok(None);
                };
                let mut updated = entry.clone();
                updated.attempts = updated.attempts.saturating_add(1);
                persist_sink_queue_update(&self.persistence, &self.sink, None, Some(&updated))?;
                entry.attempts = updated.attempts;
                Ok(Some(updated))
            })()
        };
        self.event_bus
            .finish_checked_out_entry(&self.sink, entry_id);
        result.map(|updated| updated.map(|entry| entry.attempts).unwrap_or(1))
    }

    fn ack_front_entries(&self, entry_ids: &[u64]) -> Result<()> {
        if entry_ids.is_empty() {
            return Ok(());
        }
        let result = {
            let _queue_mutation = self.event_bus.begin_queue_mutation();
            (|| -> Result<()> {
                let mut queues = self.event_bus.queues.lock();
                if let Some(queue) = queues.get_mut(&self.sink) {
                    let removable = entry_ids
                        .iter()
                        .copied()
                        .zip(queue.iter())
                        .take_while(|(id, entry)| *id == entry.id)
                        .map(|(id, _)| id)
                        .collect::<Vec<_>>();
                    if !removable.is_empty() {
                        persist_sink_queue_removals(&self.persistence, &self.sink, &removable)?;
                        for _ in 0..removable.len() {
                            queue.pop_front();
                        }
                    }
                }
                Ok(())
            })()
        };
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
                grant_rows,
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
) -> Result<()> {
    let Some(persistence) = persistence.as_ref() else {
        return Ok(());
    };
    persistence.update_sink_queue_entry(sink, remove_id, put.map(|entry| (entry.id, entry)))
}

fn persist_sink_queue_removals(
    persistence: &Option<Arc<RedbPersistence>>,
    sink: &str,
    remove_ids: &[u64],
) -> Result<()> {
    let Some(persistence) = persistence.as_ref() else {
        return Ok(());
    };
    persistence.remove_sink_queue_entries(sink, remove_ids)
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
