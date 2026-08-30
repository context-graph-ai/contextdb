use super::*;

const TRIGGER_DECLARATIONS_CONFIG_KEY: &str = "__trigger_declarations";
const TRIGGER_INCLUDING_SYNC_CONFIG_KEY: &str = "__trigger_including_sync";
pub(super) const TRIGGER_CASCADE_DEPTH_CAP: u32 = 16;
pub(crate) const TRIGGER_AUDIT_RING_CAPACITY: usize = 1024;

pub(super) type TriggerCallback =
    Arc<dyn Fn(&Database, &TriggerContext) -> Result<()> + Send + Sync + 'static>;
type TriggerRowKey = (String, RowId);
type TriggerRowKeySet = HashSet<TriggerRowKey>;

pub(crate) struct TriggerState {
    pub(super) declarations: Mutex<BTreeMap<String, TriggerDeclaration>>,
    pub(super) callbacks: RwLock<HashMap<String, TriggerCallback>>,
    pub(super) audit_ring: Mutex<VecDeque<TriggerAuditEntry>>,
    // `open_memory` has no redb history table, so it keeps its audit history
    // here. File-backed handles stream durable history from redb on demand and
    // keep only `audit_ring` resident.
    /// Each entry paired with when it was written, so the shipped audit
    /// retention can age them out on the same rule the durable table uses.
    pub(super) volatile_audit_history: Mutex<Vec<(Wallclock, TriggerAuditEntry)>>,
    staged_persistence_audits: Mutex<HashMap<Lsn, Vec<(u64, TriggerAuditEntry)>>>,
    staged_ddl_for_persistence: Mutex<HashMap<Lsn, StagedTriggerDdlCommit>>,
    callback_owner_threads: Mutex<HashMap<thread::ThreadId, usize>>,
    callback_active_txs: Mutex<HashMap<TxId, usize>>,
    active_trigger_names: Mutex<Vec<String>>,
    pub(super) wait_lock: Mutex<()>,
    pub(super) waiters: Condvar,
    pub(super) close_waiter_count: AtomicUsize,
    callback_active_count: AtomicUsize,
    pub(super) next_audit_index: AtomicU64,
    pub(super) ready: AtomicBool,
    pub(super) wait_observed_count: AtomicU64,
    pub(super) typed_err_observed_same_db_count: AtomicU64,
    pub(super) deadlock_guard_timeout_observed_count: AtomicU64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TriggerContention {
    None,
    SameDb,
}

#[derive(Debug, Clone)]
pub(crate) struct TriggerPersistenceCommit {
    pub(crate) config_values: Vec<(String, Vec<u8>)>,
    pub(crate) ddl: Vec<DdlChange>,
    pub(crate) generation_sidecars: Vec<super::DurableDdlGenerationSidecar>,
    pub(crate) start_ordinal: u32,
}

/// Trigger declarations and encoded config prepared before the received
/// schema transaction becomes durable.  Only this module may inspect the
/// declaration map; callers can persist `config_values` and later hand the
/// opaque value back for infallible publication.
#[derive(Clone)]
pub(crate) struct PreparedTriggerPublication {
    declarations: BTreeMap<String, TriggerDeclaration>,
    callbacks: HashMap<String, TriggerCallback>,
    active_trigger_names: Vec<String>,
    ready: bool,
    pub(crate) config_values: Vec<(String, Vec<u8>)>,
}

#[derive(Debug, Clone)]
struct StagedTriggerDdlCommit {
    declarations: BTreeMap<String, TriggerDeclaration>,
    persistence: TriggerPersistenceCommit,
}

#[derive(Debug, Clone)]
pub(super) struct PendingTriggerAudit {
    pub trigger_name: String,
    pub depth: u32,
    pub cascade_row_count: u32,
}

pub(super) struct TriggerDispatchFailure {
    pub error: Error,
    pub(super) active_guards: Vec<TriggerCallbackThreadGuard>,
}

pub(super) struct TriggerDispatchOutcome {
    pub(super) pending: Vec<PendingTriggerAudit>,
    pub(super) active_guards: Vec<TriggerCallbackThreadGuard>,
}

struct TriggerDispatchRun {
    processed: HashSet<(String, RowId, String)>,
    pending: Vec<PendingTriggerAudit>,
    active_guards: Vec<TriggerCallbackThreadGuard>,
}

struct TriggerDispatchSnapshot {
    triggered_tables: HashSet<String>,
    latest_insert_index: HashMap<TriggerRowKey, usize>,
    deleted_rows: TriggerRowKeySet,
}

struct TriggerFiring {
    event: TriggerEvent,
    row: VersionedRow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TriggerDispatchMode {
    Local,
    IncludingSync,
}

impl TriggerDispatchMode {
    fn includes(self, declaration: &TriggerDeclaration) -> bool {
        self == Self::Local || declaration.including_sync
    }
}

pub(super) struct TriggerCallbackThreadGuard {
    trigger: Arc<TriggerState>,
    owner: thread::ThreadId,
    tx: TxId,
    trigger_name: String,
}

fn global_trigger_callback_owner_threads() -> &'static Mutex<HashMap<thread::ThreadId, usize>> {
    static OWNERS: OnceLock<Mutex<HashMap<thread::ThreadId, usize>>> = OnceLock::new();
    OWNERS.get_or_init(|| Mutex::new(HashMap::new()))
}

pub(super) fn global_trigger_callback_active_count() -> &'static AtomicUsize {
    static ACTIVE: AtomicUsize = AtomicUsize::new(0);
    &ACTIVE
}

impl TriggerState {
    pub(super) fn new() -> Self {
        Self {
            declarations: Mutex::new(BTreeMap::new()),
            callbacks: RwLock::new(HashMap::new()),
            audit_ring: Mutex::new(VecDeque::new()),
            volatile_audit_history: Mutex::new(Vec::new()),
            staged_persistence_audits: Mutex::new(HashMap::new()),
            staged_ddl_for_persistence: Mutex::new(HashMap::new()),
            callback_owner_threads: Mutex::new(HashMap::new()),
            callback_active_txs: Mutex::new(HashMap::new()),
            active_trigger_names: Mutex::new(Vec::new()),
            wait_lock: Mutex::new(()),
            waiters: Condvar::new(),
            close_waiter_count: AtomicUsize::new(0),
            callback_active_count: AtomicUsize::new(0),
            next_audit_index: AtomicU64::new(0),
            ready: AtomicBool::new(true),
            wait_observed_count: AtomicU64::new(0),
            typed_err_observed_same_db_count: AtomicU64::new(0),
            deadlock_guard_timeout_observed_count: AtomicU64::new(0),
        }
    }

    pub(super) fn callback_active_on_other_thread(&self, _target_owner: thread::ThreadId) -> bool {
        // Local SQL/write-helper gates are scoped to this TriggerState. The
        // process-wide owner map is handled by explicit tx-control and DML
        // gates, so unrelated database instances can still run DDL while
        // another database has a trigger callback parked.
        if self.callback_active_count.load(Ordering::SeqCst) == 0 {
            return false;
        }
        let current = thread::current().id();
        let owners = self.callback_owner_threads.lock();
        !owners.is_empty() && !owners.contains_key(&current)
    }

    pub(super) fn callback_contention_for_tx_control(
        &self,
        target_owner: thread::ThreadId,
        include_owner_thread_global: bool,
    ) -> TriggerContention {
        // Trigger Class B contention is scoped to this TriggerState. A process
        // can embed multiple independent databases; a parked trigger on DB-X
        // must not make ordinary worker-thread writes on DB-Y fail just because
        // both handles live in the same process.
        let _ = (target_owner, include_owner_thread_global);
        if self.callback_active_count.load(Ordering::SeqCst) > 0 {
            let current_id = thread::current().id();
            let owners = self.callback_owner_threads.lock();
            if !owners.is_empty() && !owners.contains_key(&current_id) {
                return TriggerContention::SameDb;
            }
        }
        TriggerContention::None
    }

    pub(super) fn active_trigger_name_for_wait(&self) -> String {
        self.active_trigger_names
            .lock()
            .last()
            .cloned()
            .unwrap_or_else(|| "<unknown trigger>".to_string())
    }

    pub(super) fn callback_tx_active(&self, tx: TxId) -> bool {
        self.callback_active_txs.lock().contains_key(&tx)
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub(super) fn owner_thread_count(&self) -> usize {
        self.callback_owner_threads.lock().len()
    }

    fn enter_callback_thread_scope(
        trigger: &Arc<Self>,
        trigger_name: &str,
        tx: TxId,
    ) -> TriggerCallbackThreadGuard {
        let owner = thread::current().id();
        *global_trigger_callback_owner_threads()
            .lock()
            .entry(owner)
            .or_default() += 1;
        *trigger
            .callback_owner_threads
            .lock()
            .entry(owner)
            .or_default() += 1;
        *trigger.callback_active_txs.lock().entry(tx).or_default() += 1;
        trigger
            .active_trigger_names
            .lock()
            .push(trigger_name.to_string());
        global_callback_active_count().fetch_add(1, Ordering::SeqCst);
        global_trigger_callback_active_count().fetch_add(1, Ordering::SeqCst);
        trigger.callback_active_count.fetch_add(1, Ordering::SeqCst);
        TriggerCallbackThreadGuard {
            trigger: trigger.clone(),
            owner,
            tx,
            trigger_name: trigger_name.to_string(),
        }
    }

    pub(crate) fn stage_persistence_audits(&self, lsn: Lsn, entries: &[TriggerAuditEntry]) {
        if entries.is_empty() {
            return;
        }
        let indexed = entries
            .iter()
            .cloned()
            .map(|entry| {
                let index = self.next_audit_index.fetch_add(1, Ordering::SeqCst);
                (index, entry)
            })
            .collect::<Vec<_>>();
        self.staged_persistence_audits.lock().insert(lsn, indexed);
    }

    /// Reserve audit indexes without mutating the live counter.  Received
    /// schema work can still be abandoned after preparation, so its ids only
    /// become visible during the post-durability publication.
    pub(crate) fn prepare_received_persistence_audits(
        &self,
        entries: &[TriggerAuditEntry],
    ) -> (Vec<(u64, TriggerAuditEntry)>, u64) {
        let first = self.next_audit_index.load(Ordering::SeqCst);
        let indexed = entries
            .iter()
            .cloned()
            .enumerate()
            .map(|(offset, entry)| (first.saturating_add(offset as u64), entry))
            .collect::<Vec<_>>();
        (indexed, first.saturating_add(entries.len() as u64))
    }

    pub(crate) fn take_staged_persistence_audits(&self, lsn: Lsn) -> Vec<(u64, TriggerAuditEntry)> {
        self.staged_persistence_audits
            .lock()
            .remove(&lsn)
            .unwrap_or_default()
    }

    pub(crate) fn discard_staged_persistence_audits(&self, lsn: Lsn) {
        self.staged_persistence_audits.lock().remove(&lsn);
    }

    pub(crate) fn staged_trigger_persistence_commit(
        &self,
        lsn: Lsn,
    ) -> Option<TriggerPersistenceCommit> {
        self.staged_ddl_for_persistence
            .lock()
            .get(&lsn)
            .map(|staged| staged.persistence.clone())
    }

    fn stage_trigger_ddl_commit(
        &self,
        lsn: Lsn,
        declarations: BTreeMap<String, TriggerDeclaration>,
        persistence: TriggerPersistenceCommit,
    ) {
        self.staged_ddl_for_persistence.lock().insert(
            lsn,
            StagedTriggerDdlCommit {
                declarations,
                persistence,
            },
        );
    }
}

impl Drop for TriggerCallbackThreadGuard {
    fn drop(&mut self) {
        let _wait_guard = self.trigger.wait_lock.lock();
        let mut owners = self.trigger.callback_owner_threads.lock();
        match owners.get_mut(&self.owner) {
            Some(count) if *count > 1 => *count -= 1,
            Some(_) => {
                owners.remove(&self.owner);
            }
            None => {}
        }
        let mut global_owners = global_trigger_callback_owner_threads().lock();
        match global_owners.get_mut(&self.owner) {
            Some(count) if *count > 1 => *count -= 1,
            Some(_) => {
                global_owners.remove(&self.owner);
            }
            None => {}
        }
        let mut active_txs = self.trigger.callback_active_txs.lock();
        match active_txs.get_mut(&self.tx) {
            Some(count) if *count > 1 => *count -= 1,
            Some(_) => {
                active_txs.remove(&self.tx);
            }
            None => {}
        }
        drop(active_txs);
        self.trigger
            .callback_active_count
            .fetch_sub(1, Ordering::SeqCst);
        global_trigger_callback_active_count().fetch_sub(1, Ordering::SeqCst);
        global_callback_active_count().fetch_sub(1, Ordering::SeqCst);
        let mut names = self.trigger.active_trigger_names.lock();
        if let Some(pos) = names.iter().rposition(|name| name == &self.trigger_name) {
            names.remove(pos);
        }
        drop(names);
        self.trigger.waiters.notify_all();
    }
}

impl Database {
    pub(super) fn load_trigger_state_from_persistence(&self) -> Result<()> {
        let Some(source) = self.startup_state() else {
            return Ok(());
        };
        let mut declarations = source
            .config_value::<Vec<TriggerDeclaration>>(TRIGGER_DECLARATIONS_CONFIG_KEY)?
            .unwrap_or_default();
        let including_sync = source
            .config_value::<BTreeSet<String>>(TRIGGER_INCLUDING_SYNC_CONFIG_KEY)?
            .unwrap_or_default();
        for declaration in &mut declarations {
            declaration.including_sync = including_sync.contains(&declaration.name);
        }
        self.replace_trigger_declarations(declarations);

        let (ring_history, next_index) = source.trigger_audit_state(TRIGGER_AUDIT_RING_CAPACITY)?;
        {
            self.trigger.volatile_audit_history.lock().clear();
        }
        {
            let mut ring = self.trigger.audit_ring.lock();
            ring.clear();
            ring.extend(ring_history);
        }
        self.trigger
            .next_audit_index
            .store(next_index, Ordering::SeqCst);
        Ok(())
    }

    fn replace_trigger_declarations(&self, declarations: Vec<TriggerDeclaration>) {
        let mut map = BTreeMap::new();
        for declaration in declarations {
            map.insert(declaration.name.clone(), declaration);
        }
        let has_triggers = !map.is_empty();
        *self.trigger.declarations.lock() = map;
        self.trigger.ready.store(!has_triggers, Ordering::SeqCst);
    }

    pub(super) fn persisted_trigger_declarations(&self) -> Vec<TriggerDeclaration> {
        self.trigger.declarations.lock().values().cloned().collect()
    }

    /// Encoded trigger declarations for checkpoint export of in-memory
    /// sources, serialized exactly as file-backed DDL persistence would have.
    pub(super) fn export_trigger_config_values(&self) -> Result<Vec<(&'static str, Vec<u8>)>> {
        let declarations = self.trigger.declarations.lock().clone();
        Self::encoded_trigger_config_values(&declarations)
    }

    fn encoded_trigger_config_values(
        declarations: &BTreeMap<String, TriggerDeclaration>,
    ) -> Result<Vec<(&'static str, Vec<u8>)>> {
        let values = declarations.values().cloned().collect::<Vec<_>>();
        let including_sync = declarations
            .values()
            .filter(|declaration| declaration.including_sync)
            .map(|declaration| declaration.name.clone())
            .collect::<BTreeSet<_>>();
        Ok(vec![
            (
                TRIGGER_DECLARATIONS_CONFIG_KEY,
                RedbPersistence::encode_config_value(&values)?,
            ),
            (
                TRIGGER_INCLUDING_SYNC_CONFIG_KEY,
                RedbPersistence::encode_config_value(&including_sync)?,
            ),
        ])
    }

    pub(super) fn trigger_snapshot_ddl_for_tables(
        &self,
        live_tables: &HashSet<String>,
    ) -> Vec<DdlChange> {
        self.trigger
            .declarations
            .lock()
            .values()
            .filter(|declaration| live_tables.contains(&declaration.table))
            .map(|declaration| {
                let on_events = declaration
                    .on_events
                    .iter()
                    .map(|event| event.as_ddl_str().to_string())
                    .collect();
                if declaration.including_sync {
                    DdlChange::CreateTriggerIncludingSync {
                        name: declaration.name.clone(),
                        table: declaration.table.clone(),
                        on_events,
                    }
                } else {
                    DdlChange::CreateTrigger {
                        name: declaration.name.clone(),
                        table: declaration.table.clone(),
                        on_events,
                    }
                }
            })
            .collect()
    }

    pub(super) fn require_admin_trigger_ddl(&self, operation: &str) -> Result<()> {
        if TRIGGER_CALLBACK_ACTIVE.with(|active| active.get()) {
            return Err(Error::TriggerRequiresAdmin {
                operation: format!("{operation} from trigger callback"),
            });
        }
        if self.has_access_constraints_for_query() {
            return Err(Error::TriggerRequiresAdmin {
                operation: operation.to_string(),
            });
        }
        Ok(())
    }

    /// Validate and encode a received trigger projection using the projected
    /// table state supplied by the source-order schema planner.  This performs
    /// every fallible parse/definition check before durable publication.
    #[allow(dead_code)] // consumed when Phase B joins the staged Redb transaction
    pub(super) fn prepare_received_trigger_publication(
        &self,
        ddl: &[DdlChange],
        projected_tables: &HashMap<String, TableMeta>,
    ) -> Result<PreparedTriggerPublication> {
        let mut declarations = self.trigger.declarations.lock().clone();
        for change in ddl {
            self.apply_sync_trigger_ddl_to_projection(&mut declarations, change, projected_tables)?;
        }
        self.prepare_received_trigger_publication_from_projection(declarations)
    }

    pub(super) fn prepare_received_trigger_publication_from_projection(
        &self,
        declarations: BTreeMap<String, TriggerDeclaration>,
    ) -> Result<PreparedTriggerPublication> {
        let config_values = Self::encoded_trigger_config_values(&declarations)?
            .into_iter()
            .map(|(key, value)| (key.to_string(), value))
            .collect();
        let current_names = self
            .trigger
            .declarations
            .lock()
            .keys()
            .cloned()
            .collect::<HashSet<_>>();
        let projected_names = declarations.keys().cloned().collect::<HashSet<_>>();
        let mut callbacks = self.trigger.callbacks.read().clone();
        callbacks.retain(|name, _| projected_names.contains(name));
        let mut active_trigger_names = self.trigger.active_trigger_names.lock().clone();
        active_trigger_names.retain(|name| projected_names.contains(name));
        let adds_trigger = projected_names
            .iter()
            .any(|name| !current_names.contains(name));
        let ready = projected_names.is_empty()
            || (self.trigger.ready.load(Ordering::SeqCst) && !adds_trigger);
        Ok(PreparedTriggerPublication {
            declarations,
            callbacks,
            active_trigger_names,
            ready,
            config_values,
        })
    }

    /// Publish a trigger declaration map that was completely validated and
    /// encoded before Redb committed.  The post-durability path is deliberately
    /// infallible.
    pub(super) fn publish_prepared_trigger_publication(
        &self,
        publication: PreparedTriggerPublication,
    ) {
        *self.trigger.declarations.lock() = publication.declarations;
        *self.trigger.callbacks.write() = publication.callbacks;
        *self.trigger.active_trigger_names.lock() = publication.active_trigger_names;
        self.trigger
            .ready
            .store(publication.ready, Ordering::SeqCst);
    }

    pub(super) fn apply_trigger_ddl_from_user(&self, ddl: DdlChange) -> Result<()> {
        self.require_admin_trigger_ddl(match &ddl {
            DdlChange::CreateTrigger { .. } | DdlChange::CreateTriggerIncludingSync { .. } => {
                "CREATE TRIGGER"
            }
            DdlChange::DropTrigger { .. } => "DROP TRIGGER",
            _ => "trigger DDL",
        })?;
        self.apply_trigger_ddl_batch(vec![ddl])
    }

    pub(super) fn apply_sync_trigger_ddl_to_projection(
        &self,
        projected: &mut BTreeMap<String, TriggerDeclaration>,
        ddl: &DdlChange,
        projected_tables: &HashMap<String, TableMeta>,
    ) -> Result<()> {
        match ddl {
            change @ (DdlChange::CreateTrigger {
                name,
                table,
                on_events,
            }
            | DdlChange::CreateTriggerIncludingSync {
                name,
                table,
                on_events,
            }) => {
                if !projected_tables.contains_key(table) {
                    return Err(Error::TableNotFound(table.clone()));
                }
                let events = on_events
                    .iter()
                    .map(|event| TriggerEvent::from_ddl_str(event))
                    .collect::<Result<Vec<_>>>()?;
                let including_sync = matches!(change, DdlChange::CreateTriggerIncludingSync { .. });
                let incoming = TriggerDeclaration {
                    name: name.clone(),
                    table: table.clone(),
                    on_events: events,
                    including_sync,
                };
                if let Some(existing) = projected.get(name) {
                    if existing == &incoming {
                        return Ok(());
                    }
                    return Err(Error::Other(format!(
                        "trigger already exists with a different definition: {name}"
                    )));
                }
                projected.insert(name.clone(), incoming);
                Ok(())
            }
            DdlChange::DropTrigger { name } => {
                projected.remove(name);
                Ok(())
            }
            DdlChange::DropTable { name } => {
                let dropped_triggers = projected
                    .values()
                    .filter(|declaration| declaration.table == *name)
                    .map(|declaration| declaration.name.clone())
                    .collect::<Vec<_>>();
                for trigger in dropped_triggers {
                    projected.remove(&trigger);
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    pub(super) fn missing_callbacks_for_trigger_projection(
        &self,
        projected: &BTreeMap<String, TriggerDeclaration>,
    ) -> HashSet<String> {
        let callbacks = self.trigger.callbacks.read();
        projected
            .keys()
            .filter(|name| !callbacks.contains_key(*name))
            .cloned()
            .collect()
    }

    pub(super) fn sync_triggers_requiring_ready_gate(
        &self,
        final_projection: &BTreeMap<String, TriggerDeclaration>,
    ) -> HashSet<String> {
        let mut required = self.missing_callbacks_for_trigger_projection(final_projection);
        if !self.trigger.ready.load(Ordering::SeqCst) {
            required.extend(self.trigger.declarations.lock().keys().cloned());
            required.extend(final_projection.keys().cloned());
        }
        required
    }

    pub(super) fn sync_changeset_is_trigger_tombstone_only(&self, changes: &ChangeSet) -> bool {
        if changes.data_entry_count() != 0 || changes.ddl.is_empty() {
            return false;
        }

        let current = self.trigger.declarations.lock().clone();
        let mut projected = current.clone();
        for ddl in &changes.ddl {
            match ddl {
                DdlChange::DropTrigger { name } => {
                    projected.remove(name);
                }
                DdlChange::DropTable { name } => {
                    let dropped_triggers = projected
                        .values()
                        .filter(|declaration| declaration.table == *name)
                        .map(|declaration| declaration.name.clone())
                        .collect::<Vec<_>>();
                    for trigger in dropped_triggers {
                        projected.remove(&trigger);
                    }
                }
                _ => return false,
            }
        }

        projected.len() < current.len()
    }

    pub(super) fn preflight_sync_trigger_data_gate(
        &self,
        rows: &[RowChange],
        edges: &[EdgeChange],
        vectors: &[VectorChange],
        projected: &BTreeMap<String, TriggerDeclaration>,
        callback_required_triggers: &HashSet<String>,
    ) -> Result<()> {
        if self.sync_relay_mode_enabled() {
            return Ok(());
        }

        if projected.is_empty()
            || callback_required_triggers.is_empty()
            || (rows.is_empty() && edges.is_empty() && vectors.is_empty())
        {
            return Ok(());
        }

        let mut missing_callbacks_by_table: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for declaration in projected.values() {
            if !callback_required_triggers.contains(&declaration.name) {
                continue;
            }
            missing_callbacks_by_table
                .entry(declaration.table.clone())
                .or_default()
                .push(declaration.name.clone());
        }
        if missing_callbacks_by_table.is_empty() {
            return Ok(());
        }

        let missing = missing_callbacks_by_table
            .iter()
            .map(|(table, trigger_names)| format!("{table}: {}", trigger_names.join(", ")))
            .collect::<Vec<_>>()
            .join("; ");
        Err(Error::EngineNotInitialized {
            operation: format!(
                "apply_changes data batch before registered trigger callbacks for trigger-attached tables {missing}"
            ),
        })
    }

    fn trigger_ddl_has_transient_lifecycle(ddl: &[DdlChange]) -> bool {
        ddl.iter().enumerate().any(|(index, change)| {
            let (DdlChange::CreateTrigger { name, table, .. }
            | DdlChange::CreateTriggerIncludingSync { name, table, .. }) = change
            else {
                return false;
            };
            ddl.iter().skip(index + 1).any(|later| match later {
                DdlChange::DropTrigger { name: dropped } => dropped == name,
                DdlChange::DropTable {
                    name: dropped_table,
                } => dropped_table == table,
                _ => false,
            })
        })
    }

    fn apply_trigger_ddl_batch_projection(
        &self,
        ddl: &[DdlChange],
    ) -> Result<BTreeMap<String, TriggerDeclaration>> {
        let mut projected = self.trigger.declarations.lock().clone();
        for (index, change) in ddl.iter().enumerate() {
            match change {
                trigger_change @ (DdlChange::CreateTrigger {
                    name,
                    table,
                    on_events,
                }
                | DdlChange::CreateTriggerIncludingSync {
                    name,
                    table,
                    on_events,
                }) if self.table_meta(table).is_none() => {
                    let removed_later = ddl.iter().skip(index + 1).any(|later| match later {
                        DdlChange::DropTrigger { name: dropped } => dropped == name,
                        DdlChange::DropTable {
                            name: dropped_table,
                        } => dropped_table == table,
                        _ => false,
                    });
                    if !removed_later {
                        return Err(Error::TableNotFound(table.clone()));
                    }
                    let events = on_events
                        .iter()
                        .map(|event| TriggerEvent::from_ddl_str(event))
                        .collect::<Result<Vec<_>>>()?;
                    let including_sync =
                        matches!(trigger_change, DdlChange::CreateTriggerIncludingSync { .. });
                    let incoming = TriggerDeclaration {
                        name: name.clone(),
                        table: table.clone(),
                        on_events: events,
                        including_sync,
                    };
                    if let Some(existing) = projected.get(name) {
                        if existing != &incoming {
                            return Err(Error::Other(format!(
                                "trigger already exists with a different definition: {name}"
                            )));
                        }
                    } else {
                        projected.insert(name.clone(), incoming);
                    }
                }
                _ => self.apply_trigger_ddl_to_declarations(&mut projected, change)?,
            }
        }
        Ok(projected)
    }

    pub(super) fn apply_trigger_ddl_batch(&self, ddl: Vec<DdlChange>) -> Result<()> {
        if ddl.is_empty() {
            return Ok(());
        }
        self.allocate_ddl_lsn_maybe(|lsn| {
            let current = self.trigger.declarations.lock().clone();
            let projected = self.apply_trigger_ddl_batch_projection(&ddl)?;
            let transient_lifecycle = Self::trigger_ddl_has_transient_lifecycle(&ddl);
            if current == projected && !transient_lifecycle {
                return Ok(None);
            }
            if let Some(persistence) = &self.persistence {
                let mut config_values: Vec<(&str, Vec<u8>)> =
                    Self::encoded_trigger_config_values(&projected)?
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
            if current != projected {
                self.apply_trigger_declarations_to_memory(projected);
            }
            self.ddl_log
                .write()
                .extend(ddl.iter().cloned().map(|change| (lsn, change)));
            Ok(Some(()))
        })?;
        Ok(())
    }

    fn apply_trigger_declarations_to_memory(
        &self,
        declarations: BTreeMap<String, TriggerDeclaration>,
    ) {
        let was_ready = self.trigger.ready.load(Ordering::SeqCst);
        let current_names = self
            .trigger
            .declarations
            .lock()
            .keys()
            .cloned()
            .collect::<HashSet<_>>();
        let existing_names = declarations.keys().cloned().collect::<HashSet<_>>();
        self.trigger
            .callbacks
            .write()
            .retain(|name, _| existing_names.contains(name));
        let adds_trigger = declarations
            .keys()
            .any(|name| !current_names.contains(name));
        let has_triggers = !declarations.is_empty();
        *self.trigger.declarations.lock() = declarations;
        self.trigger.ready.store(
            !has_triggers || (was_ready && !adds_trigger),
            Ordering::SeqCst,
        );
    }

    pub(super) fn apply_trigger_ddl_to_declarations(
        &self,
        declarations: &mut BTreeMap<String, TriggerDeclaration>,
        ddl: &DdlChange,
    ) -> Result<()> {
        match ddl {
            change @ (DdlChange::CreateTrigger {
                name,
                table,
                on_events,
            }
            | DdlChange::CreateTriggerIncludingSync {
                name,
                table,
                on_events,
            }) => {
                if self.table_meta(table).is_none() {
                    return Err(Error::TableNotFound(table.clone()));
                }
                let events = on_events
                    .iter()
                    .map(|event| TriggerEvent::from_ddl_str(event))
                    .collect::<Result<Vec<_>>>()?;
                let including_sync = matches!(change, DdlChange::CreateTriggerIncludingSync { .. });
                if let Some(existing) = declarations.get(name) {
                    let incoming = TriggerDeclaration {
                        name: name.clone(),
                        table: table.clone(),
                        on_events: events,
                        including_sync,
                    };
                    if existing == &incoming {
                        return Ok(());
                    }
                    return Err(Error::Other(format!(
                        "trigger already exists with a different definition: {name}"
                    )));
                }
                declarations.insert(
                    name.clone(),
                    TriggerDeclaration {
                        name: name.clone(),
                        table: table.clone(),
                        on_events: events,
                        including_sync,
                    },
                );
                Ok(())
            }
            DdlChange::DropTrigger { name } => {
                declarations.remove(name);
                Ok(())
            }
            _ => Ok(()),
        }
    }

    pub(super) fn ensure_trigger_table_ready(&self, table: &str, operation: &str) -> Result<()> {
        if Self::sync_apply_trigger_gate_bypass_active()
            || self.trigger.ready.load(Ordering::SeqCst)
            || !self.table_has_trigger(table)
        {
            return Ok(());
        }
        // A booting handle unblocks per table once every trigger declared on
        // that table has a registered callback — no firing can be missed.
        // Sync apply still requires explicit `complete_initialization`.
        if self.table_triggers_have_callbacks(table) {
            return Ok(());
        }
        Err(Error::EngineNotInitialized {
            operation: format!("{operation} on trigger-attached table {table}"),
        })
    }

    fn table_triggers_have_callbacks(&self, table: &str) -> bool {
        let declarations = self.trigger.declarations.lock();
        let callbacks = self.trigger.callbacks.read();
        declarations
            .values()
            .filter(|declaration| declaration.table == table)
            .all(|declaration| callbacks.contains_key(&declaration.name))
    }

    pub(super) fn ensure_sync_apply_ready(&self) -> Result<()> {
        if self.trigger.ready.load(Ordering::SeqCst) {
            return Ok(());
        }
        Err(Error::EngineNotInitialized {
            operation: "apply_changes while trigger callbacks are not initialized".to_string(),
        })
    }

    fn table_has_trigger(&self, table: &str) -> bool {
        self.trigger
            .declarations
            .lock()
            .values()
            .any(|declaration| declaration.table == table)
    }

    fn triggers_for_table_event_from(
        &self,
        declarations: Option<&BTreeMap<String, TriggerDeclaration>>,
        table: &str,
        event: TriggerEvent,
    ) -> Vec<TriggerDeclaration> {
        let current;
        let values: Box<dyn Iterator<Item = &TriggerDeclaration> + '_> =
            if let Some(declarations) = declarations {
                Box::new(declarations.values())
            } else {
                current = self.trigger.declarations.lock();
                Box::new(current.values())
            };
        values
            .filter(|declaration| {
                declaration.table == table && declaration.on_events.contains(&event)
            })
            .cloned()
            .collect()
    }

    fn triggers_for_table_event_for_dispatch(
        &self,
        declarations: Option<&BTreeMap<String, TriggerDeclaration>>,
        table: &str,
        event: TriggerEvent,
        mode: TriggerDispatchMode,
    ) -> Vec<TriggerDeclaration> {
        self.triggers_for_table_event_from(declarations, table, event)
            .into_iter()
            .filter(|declaration| mode.includes(declaration))
            .collect()
    }

    fn trigger_declarations_from(
        &self,
        declarations: Option<&BTreeMap<String, TriggerDeclaration>>,
    ) -> Vec<TriggerDeclaration> {
        if let Some(declarations) = declarations {
            declarations.values().cloned().collect()
        } else {
            self.trigger.declarations.lock().values().cloned().collect()
        }
    }

    pub(super) fn dispatch_triggers_for_tx(
        &self,
        tx: TxId,
    ) -> std::result::Result<TriggerDispatchOutcome, Box<TriggerDispatchFailure>> {
        self.dispatch_triggers_for_tx_from(tx, None, TriggerDispatchMode::Local)
    }

    pub(super) fn dispatch_sync_triggers_for_tx(
        &self,
        tx: TxId,
        declarations: Option<&BTreeMap<String, TriggerDeclaration>>,
    ) -> std::result::Result<TriggerDispatchOutcome, Box<TriggerDispatchFailure>> {
        self.dispatch_triggers_for_tx_from(tx, declarations, TriggerDispatchMode::IncludingSync)
    }

    /// Keep the ordinary received-write commit path byte-for-byte quiet unless
    /// an opted-in declaration can actually observe one of its staged rows.
    /// In particular, running sync validation merely because an unrelated
    /// INCLUDING SYNC trigger exists can reject otherwise valid vector/schema
    /// arrivals.
    pub(super) fn has_matching_sync_trigger_for_tx(
        &self,
        tx: TxId,
        declarations: Option<&BTreeMap<String, TriggerDeclaration>>,
    ) -> Result<bool> {
        let tables = self.tx_mgr.with_write_set(tx, |ws| {
            ws.relational_inserts
                .iter()
                .map(|(table, _)| table.clone())
                .collect::<HashSet<_>>()
        })?;
        Ok(self
            .trigger_declarations_from(declarations)
            .iter()
            .any(|declaration| {
                declaration.including_sync
                    && tables.contains(&declaration.table)
                    && declaration
                        .on_events
                        .iter()
                        .any(|event| matches!(event, TriggerEvent::Insert | TriggerEvent::Update))
            }))
    }

    fn dispatch_triggers_for_tx_from(
        &self,
        tx: TxId,
        declarations: Option<&BTreeMap<String, TriggerDeclaration>>,
        mode: TriggerDispatchMode,
    ) -> std::result::Result<TriggerDispatchOutcome, Box<TriggerDispatchFailure>> {
        if self
            .trigger_declarations_from(declarations)
            .iter()
            .all(|declaration| !mode.includes(declaration))
        {
            return Ok(TriggerDispatchOutcome {
                pending: Vec::new(),
                active_guards: Vec::new(),
            });
        }
        let mut run = TriggerDispatchRun {
            processed: HashSet::new(),
            pending: Vec::new(),
            active_guards: Vec::new(),
        };
        let initial_len = self
            .tx_mgr
            .with_write_set(tx, |ws| ws.relational_inserts.len())
            .map_err(|error| {
                Box::new(TriggerDispatchFailure {
                    error,
                    active_guards: Vec::new(),
                })
            })?;
        if let Err(error) =
            self.dispatch_trigger_range(tx, 0, initial_len, 1, &mut run, declarations, mode)
        {
            let reason = error.to_string();
            if let Err(audit_error) =
                self.append_rolled_back_trigger_audits(&run.pending, tx, &reason)
            {
                return Err(Box::new(TriggerDispatchFailure {
                    error: audit_error,
                    active_guards: run.active_guards,
                }));
            }
            return Err(Box::new(TriggerDispatchFailure {
                error,
                active_guards: run.active_guards,
            }));
        }
        Ok(TriggerDispatchOutcome {
            pending: run.pending,
            active_guards: run.active_guards,
        })
    }

    pub(super) fn prepare_active_trigger_write_set_for_dispatch(
        &self,
        tx: TxId,
    ) -> Result<CommitValidationOutcome> {
        let conditional_update_guards = self.take_conditional_update_guards_for_tx(tx);
        let upsert_intents = self.pending_upsert_intents_for_tx(tx);
        self.tx_mgr.with_write_set_detached(tx, |ws| {
            ws.canonicalize_final_state();
            if ws.is_empty() {
                return Ok(CommitValidationOutcome::default());
            }
            self.rewrite_txid_placeholders(tx, ws)?;
            let snapshot = self.snapshot();
            let validation = self.revalidate_conditional_updates(
                ws,
                snapshot,
                &conditional_update_guards,
                None,
            )?;
            self.rewrite_commit_time_upserts_for_write_set(ws, snapshot, &upsert_intents)?;
            ws.canonicalize_final_state();
            Ok(validation)
        })?
    }

    pub(super) fn prepare_active_sync_trigger_write_set_for_dispatch(
        &self,
        tx: TxId,
    ) -> Result<CommitValidationOutcome> {
        self.tx_mgr.with_write_set_detached(tx, |ws| {
            ws.canonicalize_final_state();
            if ws.is_empty() {
                return Ok(CommitValidationOutcome::default());
            }
            self.commit_validate(tx, ws)
        })?
    }

    #[allow(clippy::too_many_arguments)]
    fn dispatch_trigger_range(
        &self,
        tx: TxId,
        start: usize,
        end: usize,
        depth: u32,
        run: &mut TriggerDispatchRun,
        declarations: Option<&BTreeMap<String, TriggerDeclaration>>,
        mode: TriggerDispatchMode,
    ) -> Result<()> {
        let mut snapshot = self.trigger_dispatch_snapshot(tx, declarations, mode)?;
        let mut index = start;
        while index < end {
            let maybe_row = self.tx_mgr.with_write_set(tx, |ws| {
                ws.relational_inserts
                    .get(index)
                    .map(|(table, row)| (table.clone(), row.clone()))
            })?;
            let Some((table, row)) = maybe_row else {
                break;
            };
            if !snapshot.triggered_tables.contains(&table) {
                index += 1;
                continue;
            }
            if snapshot
                .latest_insert_index
                .get(&(table.clone(), row.row_id))
                != Some(&index)
            {
                index += 1;
                continue;
            }
            let event = if snapshot.deleted_rows.contains(&(table.clone(), row.row_id))
                && self
                    .relational_store
                    .row_by_id(&table, row.row_id, SnapshotId::from_raw_wire(u64::MAX))
                    .is_some()
            {
                TriggerEvent::Update
            } else {
                TriggerEvent::Insert
            };
            for declaration in
                self.triggers_for_table_event_for_dispatch(declarations, &table, event, mode)
            {
                if !run
                    .processed
                    .insert((table.clone(), row.row_id, declaration.name.clone()))
                {
                    continue;
                }
                self.fire_trigger(
                    tx,
                    declaration,
                    TriggerFiring {
                        event,
                        row: row.clone(),
                    },
                    depth,
                    run,
                    &snapshot.triggered_tables,
                    declarations,
                    mode,
                )?;
                if index.saturating_add(1) < end {
                    snapshot = self.trigger_dispatch_snapshot(tx, declarations, mode)?;
                }
            }
            index += 1;
        }
        Ok(())
    }

    fn trigger_dispatch_snapshot(
        &self,
        tx: TxId,
        declarations: Option<&BTreeMap<String, TriggerDeclaration>>,
        mode: TriggerDispatchMode,
    ) -> Result<TriggerDispatchSnapshot> {
        let triggered_tables = self
            .trigger_declarations_from(declarations)
            .into_iter()
            .filter(|declaration| mode.includes(declaration))
            .map(|declaration| declaration.table.clone())
            .collect::<HashSet<_>>();
        let (latest_insert_index, deleted_rows) = self.tx_mgr.with_write_set(tx, |ws| {
            let latest_insert_index = ws
                .relational_inserts
                .iter()
                .enumerate()
                .map(|(index, (table, row))| ((table.clone(), row.row_id), index))
                .collect::<HashMap<_, _>>();
            let deleted_rows = ws
                .relational_deletes
                .iter()
                .map(|(table, row_id, _)| (table.clone(), *row_id))
                .collect::<TriggerRowKeySet>();
            (latest_insert_index, deleted_rows)
        })?;
        Ok(TriggerDispatchSnapshot {
            triggered_tables,
            latest_insert_index,
            deleted_rows,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn fire_trigger(
        &self,
        tx: TxId,
        declaration: TriggerDeclaration,
        firing: TriggerFiring,
        depth: u32,
        run: &mut TriggerDispatchRun,
        triggered_tables: &HashSet<String>,
        declarations: Option<&BTreeMap<String, TriggerDeclaration>>,
        mode: TriggerDispatchMode,
    ) -> Result<()> {
        if depth > TRIGGER_CASCADE_DEPTH_CAP {
            let entry = TriggerAuditEntry {
                trigger_name: declaration.name.clone(),
                firing_tx: tx,
                firing_lsn: Lsn(0),
                depth,
                cascade_row_count: 0,
                status: TriggerAuditStatus::DepthExceeded,
            };
            self.append_trigger_audit(entry)?;
            return Err(Error::TriggerCascadeDepthExceeded {
                trigger_name: declaration.name,
                depth,
            });
        }
        let callback = self
            .trigger
            .callbacks
            .read()
            .get(&declaration.name)
            .cloned()
            .ok_or_else(|| Error::TriggerCallbackMissing {
                trigger_name: declaration.name.clone(),
            })?;
        let before = self.write_set_counts(tx)?;
        let before_write_set = (mode == TriggerDispatchMode::IncludingSync)
            .then(|| self.tx_mgr.cloned_write_set(tx))
            .transpose()?;
        let ctx = TriggerContext {
            trigger_name: declaration.name.clone(),
            table: declaration.table.clone(),
            event: firing.event,
            tx,
            depth,
            row_values: firing.row.values.clone(),
        };
        let callback_thread =
            TriggerState::enter_callback_thread_scope(&self.trigger, &ctx.trigger_name, tx);
        run.active_guards.push(callback_thread);
        let callback_result = self
            .run_trigger_callback(tx, &ctx, callback)
            .map_err(|error| {
                let effect = if mode == TriggerDispatchMode::IncludingSync {
                    match &error {
                        Error::TriggerRequiresAdmin { .. } => Some("DDL"),
                        Error::CallbackReentry {
                            kind: CallbackKind::Trigger,
                        } => Some("DDL/control operation"),
                        _ => None,
                    }
                } else {
                    None
                };
                effect.map_or(error, |effect| Error::SyncTriggerEffectNotAllowed {
                    trigger_name: declaration.name.clone(),
                    effect: effect.to_owned(),
                })
            });
        if let Err(error) = callback_result {
            let entry = TriggerAuditEntry {
                trigger_name: declaration.name,
                firing_tx: tx,
                firing_lsn: Lsn(0),
                depth,
                cascade_row_count: 0,
                status: TriggerAuditStatus::RolledBack {
                    reason: error.to_string(),
                },
            };
            self.append_trigger_audit(entry)?;
            return Err(error);
        }
        if let Some(before_write_set) = before_write_set.as_ref() {
            // Classify raw callback effects before schema/vector validation
            // can obscure the dedicated effect error. Then normalize and
            // prove the normalized delta remains append-only. Derived TXID
            // placeholders are rewritten later, after final Tx reassignment.
            let validation = self
                .validate_sync_trigger_effects(tx, &declaration.name, before_write_set)
                .and_then(|_| self.prepare_active_sync_trigger_write_set_for_dispatch(tx))
                .and_then(|_| {
                    self.validate_sync_trigger_effects(tx, &declaration.name, before_write_set)
                });
            if let Err(error) = validation {
                let entry = TriggerAuditEntry {
                    trigger_name: declaration.name,
                    firing_tx: tx,
                    firing_lsn: Lsn(0),
                    depth,
                    cascade_row_count: 0,
                    status: TriggerAuditStatus::RolledBack {
                        reason: error.to_string(),
                    },
                };
                self.append_trigger_audit(entry)?;
                return Err(error);
            }
        }

        let after = self.write_set_counts(tx).unwrap_or(before);
        let nested_start = before.relational_inserts;
        let nested_end = after.relational_inserts;
        if nested_end <= nested_start
            || !self.trigger_range_has_triggered_table(
                tx,
                nested_start,
                nested_end,
                triggered_tables,
            )?
        {
            run.pending.push(PendingTriggerAudit {
                trigger_name: declaration.name,
                depth,
                cascade_row_count: before.delta_to(after),
            });
            return Ok(());
        }

        let preparation = if mode == TriggerDispatchMode::IncludingSync {
            self.prepare_active_sync_trigger_write_set_for_dispatch(tx)
        } else {
            self.prepare_active_trigger_write_set_for_dispatch(tx)
        };
        if let Err(error) = preparation {
            let entry = TriggerAuditEntry {
                trigger_name: declaration.name,
                firing_tx: tx,
                firing_lsn: Lsn(0),
                depth,
                cascade_row_count: 0,
                status: TriggerAuditStatus::RolledBack {
                    reason: error.to_string(),
                },
            };
            self.append_trigger_audit(entry)?;
            return Err(error);
        }
        let after = self.write_set_counts(tx).unwrap_or(before);
        let nested_start = before.relational_inserts;
        let nested_end = after.relational_inserts;
        run.pending.push(PendingTriggerAudit {
            trigger_name: declaration.name,
            depth,
            cascade_row_count: before.delta_to(after),
        });
        if nested_end > nested_start {
            self.dispatch_trigger_range(
                tx,
                nested_start,
                nested_end,
                depth.saturating_add(1),
                run,
                declarations,
                mode,
            )?;
        }
        Ok(())
    }

    fn validate_sync_trigger_effects(
        &self,
        tx: TxId,
        trigger_name: &str,
        before: &contextdb_tx::WriteSet,
    ) -> Result<()> {
        let after = self.tx_mgr.cloned_write_set(tx)?;
        let forbidden = if after.relational_inserts.len() < before.relational_inserts.len()
            || !after
                .relational_inserts
                .starts_with(&before.relational_inserts)
            || after.relational_deletes != before.relational_deletes
            || after.relational_delete_predicates != before.relational_delete_predicates
        {
            Some("relational update/delete")
        } else if after.relational_inserts[before.relational_inserts.len()..]
            .iter()
            .any(|(table, row)| {
                self.table_meta(table)
                    .and_then(|meta| natural_key_columns_for_meta(&meta))
                    .is_some_and(|columns| {
                        columns.iter().any(|column| {
                            matches!(row.values.get(column), Some(Value::TxId(value)) if *value == tx)
                        })
                    })
            })
        {
            Some("TXID identity placeholder")
        } else if after.adj_inserts != before.adj_inserts || after.adj_deletes != before.adj_deletes
        {
            Some("graph insert/delete")
        } else if after.vector_inserts != before.vector_inserts
            || after.vector_deletes != before.vector_deletes
            || after.vector_moves != before.vector_moves
        {
            Some("vector insert/delete/move")
        } else if after.config_max_u64_keys != before.config_max_u64_keys
            || after.config_writes.len() < before.config_writes.len()
            || !after.config_writes.starts_with(&before.config_writes)
            || after
                .config_writes
                .get(before.config_writes.len()..)
                .is_none_or(|writes| {
                    writes
                        .iter()
                        .any(|(key, _)| !key.starts_with("sync_creation_lineage.v1."))
                })
        {
            Some("non-owned configuration")
        } else {
            None
        };
        if let Some(effect) = forbidden {
            return Err(Error::SyncTriggerEffectNotAllowed {
                trigger_name: trigger_name.to_string(),
                effect: effect.to_owned(),
            });
        }
        Ok(())
    }

    fn trigger_range_has_triggered_table(
        &self,
        tx: TxId,
        start: usize,
        end: usize,
        triggered_tables: &HashSet<String>,
    ) -> Result<bool> {
        if triggered_tables.is_empty() || start >= end {
            return Ok(false);
        }
        let only_triggered_table = if triggered_tables.len() == 1 {
            triggered_tables.iter().next()
        } else {
            None
        };
        self.tx_mgr.with_write_set(tx, |ws| {
            let capped_end = end.min(ws.relational_inserts.len());
            ws.relational_inserts
                .get(start..capped_end)
                .is_some_and(|rows| match only_triggered_table {
                    Some(triggered_table) => rows.iter().any(|(table, _)| table == triggered_table),
                    None => rows
                        .iter()
                        .any(|(table, _)| triggered_tables.contains(table)),
                })
        })
    }

    fn run_trigger_callback(
        &self,
        tx: TxId,
        ctx: &TriggerContext,
        callback: TriggerCallback,
    ) -> Result<()> {
        let this_db = self.identity();
        let result = TRIGGER_CALLBACK_TX.with(|tx_slot| {
            let prior_tx = tx_slot.replace(Some(tx));
            let prior_db = TRIGGER_CALLBACK_DB.with(|db_slot| db_slot.replace(Some(this_db)));
            let gate_id = self.vector_schema_gate_id();
            let prior_gate = TRIGGER_CALLBACK_VECTOR_SCHEMA_GATE
                .with(|gate_slot| gate_slot.replace(Some(gate_id)));
            let prior_active = TRIGGER_CALLBACK_ACTIVE.with(|active| active.replace(true));
            let prior_name =
                TRIGGER_CALLBACK_NAME.with(|name| name.replace(Some(ctx.trigger_name.clone())));
            let prior_wallclock =
                TRIGGER_CALLBACK_WALLCLOCK.with(|slot| slot.replace(Some(current_wallclock())));
            #[cfg(feature = "test-seams")]
            crate::read_probe::note_trigger_callback_start();
            let result = catch_unwind(AssertUnwindSafe(|| callback(self, ctx)));
            let user_commit_reentry = self.take_user_commit_trigger_reentry();
            Self::clear_trigger_insert_state_machine_cache(this_db);
            TRIGGER_CALLBACK_WALLCLOCK.with(|slot| slot.set(prior_wallclock));
            TRIGGER_CALLBACK_ACTIVE.with(|active| active.set(prior_active));
            TRIGGER_CALLBACK_NAME.with(|name| name.replace(prior_name));
            TRIGGER_CALLBACK_VECTOR_SCHEMA_GATE.with(|gate_slot| gate_slot.set(prior_gate));
            TRIGGER_CALLBACK_DB.with(|db_slot| db_slot.set(prior_db));
            tx_slot.set(prior_tx);
            (result, user_commit_reentry)
        });
        match result {
            (Ok(Ok(())), true) => Err(Error::CallbackReentry {
                kind: CallbackKind::Trigger,
            }),
            (Ok(result), _) => result,
            (Err(payload), _) => Err(Error::TriggerCallbackFailed {
                trigger_name: ctx.trigger_name.clone(),
                reason: format!("panic: {}", panic_payload_to_string(payload)),
            }),
        }
    }

    pub(super) fn committed_trigger_audits_for_pending(
        &self,
        pending: &[PendingTriggerAudit],
        ws: &WriteSet,
        lsn: Lsn,
    ) -> Vec<TriggerAuditEntry> {
        let Some(firing_tx) = write_set_visibility_tx(ws) else {
            return Vec::new();
        };
        pending
            .iter()
            .map(|audit| TriggerAuditEntry {
                trigger_name: audit.trigger_name.clone(),
                firing_tx,
                firing_lsn: lsn,
                depth: audit.depth,
                cascade_row_count: audit.cascade_row_count,
                status: TriggerAuditStatus::Fired,
            })
            .collect()
    }

    pub(super) fn committed_sync_pull_trigger_audits_for_write_set(
        &self,
        ws: &WriteSet,
        lsn: Lsn,
        projected_declarations: Option<&BTreeMap<String, TriggerDeclaration>>,
    ) -> Result<Vec<TriggerAuditEntry>> {
        if ws.relational_inserts.is_empty() {
            return Ok(Vec::new());
        }
        let Some(firing_tx) = write_set_visibility_tx(ws) else {
            return Ok(Vec::new());
        };
        let inserted_tables = ws
            .relational_inserts
            .iter()
            .map(|(table, _)| table.clone())
            .collect::<HashSet<_>>();
        let candidate_trigger_tables = self
            .trigger_declarations_from(projected_declarations)
            .into_iter()
            .filter(|declaration| inserted_tables.contains(&declaration.table))
            .map(|declaration| declaration.table)
            .collect::<HashSet<_>>();
        if candidate_trigger_tables.is_empty() {
            return Ok(Vec::new());
        }
        let deleted_rows = self.sync_pull_deleted_rows_by_table(ws, &candidate_trigger_tables)?;
        let (paired_inserts, paired_deletes) = self.sync_pull_paired_update_rows(ws, &deleted_rows);
        let total_changes = logical_write_set_data_entry_count(ws, &paired_deletes);
        let mut firing_rows = HashSet::new();
        let mut audits = Vec::new();
        for (table, row) in &ws.relational_inserts {
            let event = if paired_inserts.contains(&(table.clone(), row.row_id)) {
                TriggerEvent::Update
            } else {
                TriggerEvent::Insert
            };
            let declarations =
                self.triggers_for_table_event_from(projected_declarations, table, event);
            if declarations.is_empty() {
                continue;
            }
            firing_rows.insert((table.clone(), row.row_id));
            for declaration in declarations
                .into_iter()
                .filter(|declaration| !declaration.including_sync)
            {
                audits.push(TriggerAuditEntry {
                    trigger_name: declaration.name,
                    firing_tx,
                    firing_lsn: lsn,
                    depth: 1,
                    cascade_row_count: 0,
                    status: TriggerAuditStatus::Fired,
                });
            }
        }
        let cascade_row_count = total_changes.saturating_sub(firing_rows.len()) as u32;
        for audit in &mut audits {
            audit.cascade_row_count = cascade_row_count;
        }
        Ok(audits)
    }

    pub(super) fn sync_pull_trigger_audit_projection(
        &self,
        ddl: &[DdlChange],
    ) -> Result<BTreeMap<String, TriggerDeclaration>> {
        let mut projected = self.trigger.declarations.lock().clone();
        for change in ddl {
            if let DdlChange::CreateTrigger {
                name,
                table,
                on_events,
            }
            | DdlChange::CreateTriggerIncludingSync {
                name,
                table,
                on_events,
            } = change
            {
                let events = on_events
                    .iter()
                    .map(|event| TriggerEvent::from_ddl_str(event))
                    .collect::<Result<Vec<_>>>()?;
                let including_sync = matches!(change, DdlChange::CreateTriggerIncludingSync { .. });
                projected.insert(
                    name.clone(),
                    TriggerDeclaration {
                        name: name.clone(),
                        table: table.clone(),
                        on_events: events,
                        including_sync,
                    },
                );
            }
        }
        Ok(projected)
    }

    fn sync_pull_deleted_rows_by_table(
        &self,
        ws: &WriteSet,
        candidate_trigger_tables: &HashSet<String>,
    ) -> Result<HashMap<String, Vec<VersionedRow>>> {
        let deleted_ids_by_table = ws.relational_deletes.iter().fold(
            HashMap::<String, HashSet<RowId>>::new(),
            |mut by_table, (table, row_id, _)| {
                if !candidate_trigger_tables.contains(table) {
                    return by_table;
                }
                by_table.entry(table.clone()).or_default().insert(*row_id);
                by_table
            },
        );
        if deleted_ids_by_table.is_empty() {
            return Ok(HashMap::new());
        }
        let snapshot = self.snapshot();
        let mut deleted_rows = HashMap::new();
        for (table, row_ids) in deleted_ids_by_table {
            let rows = self
                .relational
                .scan(&table, snapshot)?
                .into_iter()
                .filter(|row| row_ids.contains(&row.row_id))
                .collect::<Vec<_>>();
            if !rows.is_empty() {
                deleted_rows.insert(table, rows);
            }
        }
        Ok(deleted_rows)
    }

    fn sync_pull_paired_update_rows(
        &self,
        ws: &WriteSet,
        deleted_rows: &HashMap<String, Vec<VersionedRow>>,
    ) -> (TriggerRowKeySet, TriggerRowKeySet) {
        let mut paired_inserts = HashSet::new();
        let mut paired_deletes = HashSet::new();
        for (table, row) in &ws.relational_inserts {
            if let Some(deleted_row) = deleted_rows.get(table).and_then(|deleted_rows| {
                deleted_rows.iter().find(|deleted_row| {
                    !paired_deletes.contains(&(table.clone(), deleted_row.row_id))
                        && self.sync_pull_rows_share_identity(table, row, deleted_row)
                })
            }) {
                paired_inserts.insert((table.clone(), row.row_id));
                paired_deletes.insert((table.clone(), deleted_row.row_id));
            }
        }
        (paired_inserts, paired_deletes)
    }

    fn sync_pull_rows_share_identity(
        &self,
        table: &str,
        inserted: &VersionedRow,
        deleted: &VersionedRow,
    ) -> bool {
        if inserted.row_id == deleted.row_id {
            return true;
        }
        let mut identity_columns = Vec::new();
        if let Some(meta) = self.table_meta(table) {
            if let Some(natural_key_column) = meta.natural_key_column {
                identity_columns.push(vec![natural_key_column]);
            }
            identity_columns.extend(
                meta.columns
                    .iter()
                    .filter(|column| column.primary_key || column.unique)
                    .map(|column| vec![column.name.clone()]),
            );
            identity_columns.extend(meta.unique_constraints);
        }
        if identity_columns.is_empty()
            && inserted.values.contains_key("id")
            && deleted.values.contains_key("id")
        {
            identity_columns.push(vec!["id".to_string()]);
        }
        identity_columns.iter().any(|columns| {
            !columns.is_empty()
                && columns.iter().all(|column| {
                    inserted.values.contains_key(column)
                        && inserted.values.get(column) == deleted.values.get(column)
                })
        })
    }

    pub(super) fn stage_trigger_audits_for_persistence(
        &self,
        lsn: Lsn,
        entries: &[TriggerAuditEntry],
    ) {
        if self.persistence.is_some() {
            self.trigger.stage_persistence_audits(lsn, entries);
        }
    }

    pub(super) fn discard_staged_trigger_audits_for_persistence(&self, lsn: Lsn) {
        self.trigger.discard_staged_persistence_audits(lsn);
    }

    pub(super) fn stage_trigger_ddl_for_commit(
        &self,
        lsn: Lsn,
        ddl: &[DdlChange],
        start_ordinal: u32,
    ) -> Result<()> {
        if ddl.is_empty() {
            return Ok(());
        }
        let current = self.trigger.declarations.lock().clone();
        let projected = self.apply_trigger_ddl_batch_projection(ddl)?;
        let transient_lifecycle = Self::trigger_ddl_has_transient_lifecycle(ddl);
        if current == projected && !transient_lifecycle {
            return Ok(());
        }
        let mut config_values: Vec<(String, Vec<u8>)> =
            Self::encoded_trigger_config_values(&projected)?
                .into_iter()
                .map(|(key, value)| (key.to_string(), value))
                .collect();
        // The caller reserves the event-bus prefix because persistence
        // concatenates event-bus DDL before trigger DDL at this commit LSN.
        let generation_sidecars = self.ddl_generation_sidecars(ddl)?;
        let provenance_values = self.ddl_generation_sidecar_values(lsn, ddl, start_ordinal)?;
        config_values.extend(
            provenance_values
                .iter()
                .map(|(key, value)| (key.clone(), value.clone())),
        );
        let persistence = TriggerPersistenceCommit {
            config_values,
            ddl: ddl.to_vec(),
            generation_sidecars,
            start_ordinal,
        };
        self.trigger
            .stage_trigger_ddl_commit(lsn, projected, persistence);
        Ok(())
    }

    pub(super) fn staged_trigger_declarations_for_commit(
        &self,
        lsn: Lsn,
    ) -> Option<BTreeMap<String, TriggerDeclaration>> {
        self.trigger
            .staged_ddl_for_persistence
            .lock()
            .get(&lsn)
            .map(|staged| staged.declarations.clone())
    }

    pub(super) fn publish_staged_trigger_ddl_commit(&self, lsn: Lsn) {
        let staged = self.trigger.staged_ddl_for_persistence.lock().remove(&lsn);
        if let Some(staged) = staged {
            self.publish_in_memory_ddl_generation_sidecars(
                lsn,
                staged.persistence.start_ordinal,
                &staged.persistence.generation_sidecars,
            );
            self.apply_trigger_declarations_to_memory(staged.declarations);
            self.ddl_log.write().extend(
                staged
                    .persistence
                    .ddl
                    .into_iter()
                    .map(|change| (lsn, change)),
            );
        }
    }

    pub(super) fn discard_staged_trigger_ddl_commit(&self, lsn: Lsn) {
        self.trigger.staged_ddl_for_persistence.lock().remove(&lsn);
    }

    pub(crate) fn log_drop_table_ddl_and_remove_triggers(
        &self,
        table: &str,
        lsn: Lsn,
    ) -> Result<()> {
        self.log_drop_table_ddl_and_remove_triggers_with_prefix(table, lsn, &[])
    }

    pub(crate) fn log_drop_table_ddl_and_remove_triggers_with_prefix(
        &self,
        table: &str,
        lsn: Lsn,
        prefix_trigger_ddl: &[DdlChange],
    ) -> Result<()> {
        let current = self.trigger.declarations.lock().clone();
        let mut projected = self.apply_trigger_ddl_batch_projection(prefix_trigger_ddl)?;
        let implicit_trigger_drops = projected
            .values()
            .filter(|declaration| declaration.table == table)
            .map(|declaration| DdlChange::DropTrigger {
                name: declaration.name.clone(),
            })
            .collect::<Vec<_>>();

        for drop in &implicit_trigger_drops {
            self.apply_trigger_ddl_to_declarations(&mut projected, drop)?;
        }

        let mut ddl = prefix_trigger_ddl.to_vec();
        ddl.push(DdlChange::DropTable {
            name: table.to_string(),
        });
        ddl.extend(implicit_trigger_drops);
        let mut config_values = if current == projected {
            Vec::new()
        } else {
            Self::encoded_trigger_config_values(&projected)?
        };
        if let Some(event_bus_values) = self.event_bus_config_values_without_table(table)? {
            config_values.extend(event_bus_values);
        }
        let provenance_values = self.ddl_generation_sidecar_values(lsn, &ddl, 0)?;
        config_values.extend(
            provenance_values
                .iter()
                .map(|(key, value)| (key.as_str(), value.clone())),
        );
        let graph_edges = self.graph_edges_after_table_drop(table);
        let vectors = self.vector_entries_after_table_drop(table);
        if let Some(persistence) = &self.persistence {
            persistence.remove_table_rewrite_aux_with_config_values_and_ddl_log(
                table,
                config_values,
                lsn,
                &ddl,
                &graph_edges,
                &vectors,
            )?;
        } else {
            self.record_ddl_generation_sidecars(lsn, &ddl)?;
        }
        self.drop_table_aux_state(table);
        self.remove_rank_formulas_for_table(table);
        self.vector_store_deregister_table(table);
        self.relational_store().drop_table(table);
        if current != projected {
            self.apply_trigger_declarations_to_memory(projected);
        }
        self.apply_event_bus_definitions_without_table_to_memory(table);
        self.ddl_log
            .write()
            .extend(ddl.into_iter().map(|change| (lsn, change)));
        Ok(())
    }

    pub(super) fn append_trigger_audits_to_memory(&self, entries: Vec<TriggerAuditEntry>) {
        for entry in entries {
            self.append_trigger_audit_to_memory(entry);
        }
    }

    pub(super) fn publish_received_trigger_audits(
        &self,
        entries: Vec<(u64, TriggerAuditEntry)>,
        next_audit_index: u64,
    ) {
        for (_, entry) in entries {
            self.append_trigger_audit_to_memory(entry);
        }
        self.trigger
            .next_audit_index
            .fetch_max(next_audit_index, Ordering::SeqCst);
    }

    pub(super) fn append_rolled_back_trigger_audits(
        &self,
        pending: &[PendingTriggerAudit],
        tx: TxId,
        reason: &str,
    ) -> Result<()> {
        for audit in pending {
            let entry = TriggerAuditEntry {
                trigger_name: audit.trigger_name.clone(),
                firing_tx: tx,
                firing_lsn: Lsn(0),
                depth: audit.depth,
                cascade_row_count: 0,
                status: TriggerAuditStatus::RolledBack {
                    reason: reason.to_string(),
                },
            };
            self.append_trigger_audit(entry)?;
        }
        Ok(())
    }

    fn append_trigger_audit(&self, entry: TriggerAuditEntry) -> Result<()> {
        if let Some(persistence) = &self.persistence {
            let index = self.trigger.next_audit_index.fetch_add(1, Ordering::SeqCst);
            persistence.append_trigger_audit(index, &entry)?;
        } else {
            self.trigger.next_audit_index.fetch_add(1, Ordering::SeqCst);
        }
        self.append_trigger_audit_to_memory(entry);
        Ok(())
    }

    fn append_trigger_audit_to_memory(&self, entry: TriggerAuditEntry) {
        let volatile_entry = self.persistence.is_none().then(|| entry.clone());
        {
            let mut ring = self.trigger.audit_ring.lock();
            if ring.len() == TRIGGER_AUDIT_RING_CAPACITY {
                ring.pop_front();
            }
            ring.push_back(entry);
        }
        if let Some(entry) = volatile_entry {
            self.trigger
                .volatile_audit_history
                .lock()
                .push((Wallclock::now(), entry));
        }
    }

    pub(super) fn active_trigger_tx_for_this_handle(&self) -> Option<TxId> {
        let this_db = self.identity();
        if TRIGGER_CALLBACK_DB.with(|db| db.get()) == Some(this_db) {
            TRIGGER_CALLBACK_TX.with(|tx| tx.get())
        } else {
            None
        }
    }
}

impl TriggerEvent {
    fn from_ddl_str(event: &str) -> Result<Self> {
        match event.to_ascii_uppercase().as_str() {
            "INSERT" => Ok(Self::Insert),
            "UPDATE" => Ok(Self::Update),
            "DELETE" => Err(Error::TriggerEventUnsupported {
                event: "DELETE".to_string(),
            }),
            other => Err(Error::TriggerEventUnsupported {
                event: other.to_string(),
            }),
        }
    }

    fn as_ddl_str(self) -> &'static str {
        match self {
            Self::Insert => "INSERT",
            Self::Update => "UPDATE",
        }
    }
}

impl TriggerAuditStatus {
    pub(super) fn matches_filter(&self, filter: TriggerAuditStatusFilter) -> bool {
        matches!(
            (self, filter),
            (Self::Fired, TriggerAuditStatusFilter::Fired)
                | (
                    Self::RolledBack { .. },
                    TriggerAuditStatusFilter::RolledBack
                )
                | (Self::DepthExceeded, TriggerAuditStatusFilter::DepthExceeded)
        )
    }
}

impl WriteSetCounts {
    fn total(self) -> usize {
        self.relational_inserts
            + self.relational_deletes
            + self.adj_inserts
            + self.adj_deletes
            + self.vector_inserts
            + self.vector_deletes
            + self.vector_moves
    }

    fn delta_to(self, after: Self) -> u32 {
        after.total().saturating_sub(self.total()) as u32
    }
}

fn panic_payload_to_string(payload: Box<dyn std::any::Any + Send>) -> String {
    match payload.downcast::<String>() {
        Ok(value) => *value,
        Err(payload) => match payload.downcast::<&'static str>() {
            Ok(value) => (*value).to_string(),
            Err(_) => "non-string panic payload".to_string(),
        },
    }
}

fn write_set_visibility_tx(ws: &WriteSet) -> Option<TxId> {
    ws.relational_inserts
        .iter()
        .map(|(_, row)| row.created_tx)
        .chain(ws.relational_deletes.iter().map(|(_, _, tx)| *tx))
        .chain(ws.adj_inserts.iter().map(|entry| entry.created_tx))
        .chain(ws.adj_deletes.iter().map(|(_, _, _, tx)| *tx))
        .chain(ws.vector_inserts.iter().map(|entry| entry.created_tx))
        .chain(ws.vector_deletes.iter().map(|(_, _, tx)| *tx))
        .chain(ws.vector_moves.iter().map(|(_, _, _, tx)| *tx))
        .next()
}

fn logical_write_set_data_entry_count(
    ws: &WriteSet,
    paired_update_deletes: &HashSet<(String, RowId)>,
) -> usize {
    let relational_rows = ws
        .relational_inserts
        .len()
        .saturating_add(ws.relational_deletes.len())
        .saturating_sub(paired_update_deletes.len());
    relational_rows
        .saturating_add(ws.adj_inserts.len())
        .saturating_add(ws.adj_deletes.len())
        .saturating_add(ws.vector_inserts.len())
        .saturating_add(ws.vector_deletes.len())
        .saturating_add(ws.vector_moves.len())
}

#[cfg(test)]
mod sync_effect_tests {
    use super::*;

    #[test]
    fn vector_move_delta_is_a_typed_received_trigger_effect() {
        let db = Database::open_memory();
        let tx = db.begin().unwrap();
        let before = db.tx_mgr.cloned_write_set(tx).unwrap();
        db.tx_mgr
            .with_write_set(tx, |ws| {
                ws.vector_moves.push((
                    VectorIndexRef::new("items", "embedding"),
                    RowId(1),
                    RowId(2),
                    tx,
                ));
            })
            .unwrap();
        let error = db
            .validate_sync_trigger_effects(tx, "received_insert", &before)
            .unwrap_err();
        assert!(matches!(
            error,
            Error::SyncTriggerEffectNotAllowed {
                ref trigger_name,
                ref effect,
            } if trigger_name == "received_insert" && effect == "vector insert/delete/move"
        ));
        db.rollback(tx).unwrap();
    }
}
