#![allow(
    clippy::result_large_err,
    reason = "the error type is returned by value across the crate; boxing it would change every signature"
)]
use super::*;

const TRIGGER_DECLARATIONS_CONFIG_KEY: &str = "__trigger_declarations";
const TRIGGER_INCLUDING_SYNC_CONFIG_KEY: &str = "__trigger_including_sync";
pub(super) const TRIGGER_CASCADE_DEPTH_CAP: u32 = 16;
pub(crate) const TRIGGER_AUDIT_RING_CAPACITY: usize = 1024;

type CallbackFn = dyn Fn(&Database, &TriggerContext) -> Result<()> + Send + Sync + 'static;
pub(super) struct TriggerCallback {
    body: Arc<CallbackFn>,
    // This control is outside the Arc it prices and retires after that Arc.
    original: Option<contextdb_tx::SourceCharge>,
}
impl Clone for TriggerCallback {
    fn clone(&self) -> Self {
        let original = self.original.clone();
        let body = self.body.clone();
        Self { body, original }
    }
}
impl std::ops::Deref for TriggerCallback {
    type Target = CallbackFn;
    fn deref(&self) -> &Self::Target { self.body.as_ref() }
}
impl TriggerCallback {
    pub(super) fn legacy<F>(callback: F) -> Self
    where F: Fn(&Database, &TriggerContext) -> Result<()> + Send + Sync + 'static {
        Self { body: Arc::new(callback), original: None }
    }
}
type TriggerRowKey = (String, RowId);
type TriggerRowKeySet = HashSet<TriggerRowKey>;

pub(crate) struct TriggerState {
    pub(super) declarations: Mutex<BTreeMap<String, TriggerDeclaration>>,
    pub(super) callbacks: RwLock<HashMap<String, TriggerCallback>>,
    pub(super) audit_ring: Mutex<VecDeque<TriggerAuditEntry>>,
    native_audit_ring: Mutex<Option<OriginalTriggerAuditRing>>,
    native_audit_history: Mutex<Option<OriginalAuditHistoryLink>>,
    // `open_memory` has no redb history table, so it keeps its audit history
    // here. File-backed handles stream durable history from redb on demand and
    // keep only `audit_ring` resident.
    /// Each entry paired with when it was written, so the shipped audit
    /// retention can age them out on the same rule the durable table uses.
    pub(super) volatile_audit_history: Mutex<Vec<(Wallclock, TriggerAuditEntry)>>,
    native_staged_audits: Mutex<Option<OriginalStagedAuditLink>>,
    staged_persistence_audits: Mutex<HashMap<Lsn, Vec<(u64, TriggerAuditEntry)>>>,
    staged_ddl_for_persistence: Mutex<HashMap<Lsn, StagedTriggerDdlCommit>>,
    native_staged_ddl: Mutex<Option<event_bus::OwnedEventShared<OriginalTriggerDdlStage>>>,
    native_callbacks: Mutex<Option<OriginalCallbackLink>>,
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
    native_publication_memory: Mutex<Option<OwnedMemoryReservation>>,
    native_callback_map_memory: Mutex<Option<OwnedMemoryReservation>>,
    native_callback_map_bytes: std::sync::atomic::AtomicUsize,
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

pub(super) struct TriggerDispatchRun {
    initial_targets: Option<super::connected_replay::PrivateChangedRows>,
    initial_len: usize,
    processed: HashSet<(String, RowId, String)>,
    pub(super) pending: Vec<PendingTriggerAudit>,
    pub(super) active_guards: Vec<TriggerCallbackThreadGuard>,
    _private_key_credit: Option<contextdb_core::read_memory::ReadCredit>,
    _native_memory: Option<OwnedMemoryReservation>,
}
impl TriggerDispatchRun {
    pub(super) fn owned(database: &Database) -> Result<Self> {
        Ok(Self { initial_targets: None, initial_len: 0, processed: HashSet::new(), pending: Vec::new(),
            active_guards: Vec::new(), _private_key_credit: None, _native_memory: Some(database.reserve_owned_preparation(0)?) })
    }
    fn admit_slot<T>(original: &mut Option<OwnedMemoryReservation>, values: &mut Vec<T>, extra: usize) -> Result<()> {
        if let Some(original) = original.as_mut() {
            let target = if values.len() == values.capacity() { values.capacity().checked_mul(2).map(|n| n.max(4))
                .ok_or_else(EngineMetadataJournal::identity_error)? } else { values.capacity() };
            let controls = if target > values.capacity() { std::alloc::Layout::array::<T>(target)
                .map_err(|_| EngineMetadataJournal::identity_error())?.size() } else { 0 };
            crate::memory_accounting::grow_preparation_from_original(original, controls.checked_add(extra)
                .ok_or_else(EngineMetadataJournal::identity_error)?, "trigger", "dispatch_storage",
                "Raise MEMORY_LIMIT to retain this trigger dispatch.")?;
            if target > values.capacity() {
                values.reserve_exact(target - values.len());
                if values.capacity() != target { return Err(EngineMetadataJournal::identity_error()); }
            }
        }
        Ok(())
    }
    fn admit_guard(&mut self, _name: &str) -> Result<()> {
        Self::admit_slot(&mut self._native_memory, &mut self.active_guards, 0)
    }
    fn push_owned_pending(&mut self, name: &str, depth: u32, cascade_row_count: u32) -> Result<()> {
        Self::admit_slot(&mut self._native_memory, &mut self.pending, name.len())?;
        self.pending.push(PendingTriggerAudit { trigger_name: crate::memory_accounting::preparation_copy_string(name)?, depth, cascade_row_count });
        Ok(())
    }
    fn mark_firing(&mut self, table: &str, row: RowId, trigger: &str) -> Result<bool> {
        if self._native_memory.is_some() && self.processed.iter().any(|(name, id, registered)| name == table && *id == row && registered == trigger) {
            return Ok(false);
        }
        if let Some(original) = self._native_memory.as_mut() {
            let controls = if self.processed.len() == self.processed.capacity() {
                crate::memory_accounting::preparation_native_map_bytes::<(String, RowId, String), ()>(self.processed.len().saturating_add(1))?
            } else { 0 };
            crate::memory_accounting::grow_preparation_from_original(original,
                table.len().checked_add(trigger.len()).and_then(|n| n.checked_add(controls))
                    .ok_or_else(EngineMetadataJournal::identity_error)?,
                "trigger", "dispatch_processed_identity", "Raise MEMORY_LIMIT to dispatch these trigger identities.")?;
        }
        if let Some(credit) = self._private_key_credit.as_mut() {
            credit.try_grow_for(table.len().saturating_add(trigger.len()).saturating_add(512),
                "sync","connected_trigger_processed_key",
                "Raise the declared memory limit to retain callback dispatch keys.")?;
        }
        Ok(self.processed.insert((table.to_owned(),row,trigger.to_owned())))
    }
}

struct TriggerDispatchSnapshot {
    triggered_tables: HashSet<String>,
    latest_insert_index: HashMap<TriggerRowKey, usize>,
    deleted_rows: TriggerRowKeySet,
}

// The complete native callback identity snapshot retains the actual original
// admission through every callback and refresh. Fields free before the debit.
struct OwnedTriggerDispatchSnapshot {
    snapshot: TriggerDispatchSnapshot,
    _memory: OwnedMemoryReservation,
}
impl std::ops::Deref for OwnedTriggerDispatchSnapshot {
    type Target = TriggerDispatchSnapshot;
    fn deref(&self) -> &Self::Target { &self.snapshot }
}

struct TriggerFiring<'a> {
    event: TriggerEvent,
    row: &'a VersionedRow,
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

// Each link holds its Box control OUTSIDE the Box it prices. The next
// link moves with its exact debit; unlinking never refunds/readmits a body.
struct OriginalCallbackLink {
    node: Box<OriginalCallbackNode>,
    _memory: OwnedMemoryReservation,
}
struct OriginalCallbackNode {
    owner: thread::ThreadId,
    tx: TxId,
    name: String,
    visible_name: bool,
    next: Option<OriginalCallbackLink>,
}
impl OriginalCallbackLink {
    fn id(&self) -> usize { (&*self.node as *const OriginalCallbackNode) as usize }
}

pub(super) struct TriggerCallbackThreadGuard {
    native_id: Option<usize>,
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
            native_audit_ring: Mutex::new(None),
            native_audit_history: Mutex::new(None),
            volatile_audit_history: Mutex::new(Vec::new()),
            native_staged_audits: Mutex::new(None),
            staged_persistence_audits: Mutex::new(HashMap::new()),
            staged_ddl_for_persistence: Mutex::new(HashMap::new()),
            native_staged_ddl: Mutex::new(None),
            native_callbacks: Mutex::new(None),
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
            native_publication_memory: Mutex::new(None),
            native_callback_map_memory: Mutex::new(None),
            native_callback_map_bytes: std::sync::atomic::AtomicUsize::new(0),
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
        let native = self.native_callbacks.lock();
        let mut cursor = native.as_ref();
        let mut has_native = false;
        let mut current_native = false;
        while let Some(link) = cursor {
            has_native = true;
            current_native |= link.node.owner == current;
            cursor = link.node.next.as_ref();
        }
        (!owners.is_empty() || has_native) && !owners.contains_key(&current) && !current_native
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
            let native = self.native_callbacks.lock();
            let mut cursor = native.as_ref();
            let mut has_native = false;
            let mut current_native = false;
            while let Some(link) = cursor {
                has_native = true;
                current_native |= link.node.owner == current_id;
                cursor = link.node.next.as_ref();
            }
            if (!owners.is_empty() || has_native) && !owners.contains_key(&current_id) && !current_native {
                return TriggerContention::SameDb;
            }
        }
        TriggerContention::None
    }

    pub(super) fn active_trigger_name_for_wait(&self) -> String {
        let native = self.native_callbacks.lock();
        let mut cursor = native.as_ref();
        while let Some(link) = cursor {
            if link.node.visible_name { return link.node.name.clone(); }
            cursor = link.node.next.as_ref();
        }
        drop(native);
        self.active_trigger_names
            .lock()
            .last()
            .cloned()
            .unwrap_or_else(|| "<unknown trigger>".to_string())
    }

    pub(super) fn callback_tx_active(&self, tx: TxId) -> bool {
        if self.callback_active_txs.lock().contains_key(&tx) { return true; }
        let native = self.native_callbacks.lock();
        let mut cursor = native.as_ref();
        while let Some(link) = cursor {
            if link.node.tx == tx { return true; }
            cursor = link.node.next.as_ref();
        }
        false
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub(super) fn owner_thread_count(&self) -> usize {
        let owners = self.callback_owner_threads.lock();
        let native = self.native_callbacks.lock();
        let mut count = owners.len();
        let mut cursor = native.as_ref();
        while let Some(link) = cursor {
            let mut previous = native.as_ref();
            let mut already = owners.contains_key(&link.node.owner);
            while let Some(prior) = previous {
                if prior.id() == link.id() { break; }
                already |= prior.node.owner == link.node.owner;
                previous = prior.node.next.as_ref();
            }
            if !already { count += 1; }
            cursor = link.node.next.as_ref();
        }
        count
    }

    fn enter_callback_thread_scope_owned(trigger: &Arc<Self>, trigger_name: &str, tx: TxId,
        original: &OwnedMemoryReservation) -> Result<TriggerCallbackThreadGuard> {
        let bytes = std::alloc::Layout::new::<OriginalCallbackNode>().size()
            .checked_add(trigger_name.len()).ok_or_else(EngineMetadataJournal::identity_error)?;
        let memory = crate::memory_accounting::reserve_preparation_from_original(original, bytes,
            "trigger", "callback_registration", "Raise MEMORY_LIMIT to retain this callback registration.")?;
        let name = crate::memory_accounting::preparation_copy_string(trigger_name)?;
        let owner = thread::current().id();
        let mut link = OriginalCallbackLink { node: Box::new(OriginalCallbackNode {
            owner, tx, name, visible_name: true, next: None,
        }), _memory: memory };
        let id = link.id();
        let guard = TriggerCallbackThreadGuard { native_id: Some(id), trigger: trigger.clone(),
            owner, tx, trigger_name: String::new() };
        let mut head = trigger.native_callbacks.lock();
        link.node.next = head.take();
        *head = Some(link);
        // No fallible step follows registration. This guard unlinks the exact
        // node before settling any charge, including unwind during callbacks.
        global_callback_active_count().fetch_add(1, Ordering::SeqCst);
        global_trigger_callback_active_count().fetch_add(1, Ordering::SeqCst);
        trigger.callback_active_count.fetch_add(1, Ordering::SeqCst);
        drop(head);
        Ok(guard)
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
            native_id: None,
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

    pub(crate) fn take_staged_persistence_audits(&self, lsn: Lsn) -> PreparedPersistenceAudits {
        if let Some(batch) = self.take_original_staged_audits(lsn) { return batch; }
        PreparedPersistenceAudits { entries: self.staged_persistence_audits.lock().remove(&lsn).unwrap_or_default(), _memory: None }
    }

    pub(crate) fn discard_staged_persistence_audits(&self, lsn: Lsn) {
        let legacy = self.staged_persistence_audits.lock().remove(&lsn);
        let native = self.take_original_staged_audits(lsn);
        drop(legacy); drop(native);
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
        if let Some(id) = self.native_id {
            let removed = {
                let mut head = self.trigger.native_callbacks.lock();
                let mut slot = &mut *head;
                loop {
                    match slot.as_ref() {
                        Some(link) if link.id() == id => {
                            let mut removed = slot.take().expect("addressed original callback link");
                            *slot = removed.node.next.take();
                            break Some(removed);
                        }
                        Some(_) => { slot = &mut slot.as_mut().expect("live original callback link").node.next; }
                        None => break None,
                    }
                }
            };
            self.trigger.callback_active_count.fetch_sub(1, Ordering::SeqCst);
            global_trigger_callback_active_count().fetch_sub(1, Ordering::SeqCst);
            global_callback_active_count().fetch_sub(1, Ordering::SeqCst);
            drop(_wait_guard);
            // The removed Box and its name physically retire before the final
            // original fee in OriginalCallbackLink; no map capacity survives it.
            drop(removed);
            self.trigger.waiters.notify_all();
            return;
        }
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
    #[allow(dead_code)] // consumed when received-schema publication joins the staged Redb transaction
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

    pub(super) fn received_trigger_projection_bytes(&self) -> Result<usize> {
        let declarations = self.trigger.declarations.lock();
        let encoded = bincode::serde::encode_into_std_write(
            &*declarations,
            &mut std::io::sink(),
            bincode::config::standard(),
        )
        .map_err(|error| Error::Other((format!("price received triggers: {error}")).into()))?;
        let callbacks = self.trigger.callbacks.read();
        let callback_bytes = callbacks.keys().fold(0usize, |bytes, name| {
            bytes.saturating_add(name.len()).saturating_add(256)
        });
        let active = self.trigger.active_trigger_names.lock();
        let active_bytes = active.iter().fold(0usize, |bytes, name| {
            bytes.saturating_add(name.len()).saturating_add(128)
        });
        Ok(encoded
            .saturating_add(declarations.len().saturating_mul(512))
            .saturating_add(callback_bytes)
            .saturating_add(active_bytes))
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
        {
            let mut native = self.trigger.native_callbacks.lock();
            let mut cursor = native.as_mut();
            while let Some(link) = cursor {
                link.node.visible_name &= publication.declarations.contains_key(&link.node.name);
                cursor = link.node.next.as_mut();
            }
        }
        *self.trigger.declarations.lock() = publication.declarations;
        *self.trigger.callbacks.write() = publication.callbacks;
        *self.trigger.active_trigger_names.lock() = publication.active_trigger_names;
        self.trigger
            .ready
            .store(publication.ready, Ordering::SeqCst);
    }

    pub(super) fn apply_trigger_ddl_from_user(
        &self,
        ddl: DdlChange,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<()> {
        self.require_admin_trigger_ddl(match &ddl {
            DdlChange::CreateTrigger { .. } | DdlChange::CreateTriggerIncludingSync { .. } => {
                "CREATE TRIGGER"
            }
            DdlChange::DropTrigger { .. } => "DROP TRIGGER",
            _ => "trigger DDL",
        })?;
        self.apply_trigger_ddl_batch(vec![ddl], read_memory.clone())
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
                    return Err(Error::Other((format!(
                        "trigger already exists with a different definition: {name}"
                    )).into()));
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

    pub(super) fn trigger_ddl_has_transient_lifecycle(ddl: &[DdlChange]) -> bool {
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

    pub(super) fn apply_trigger_ddl_batch_projection(
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
                            return Err(Error::Other((format!(
                                "trigger already exists with a different definition: {name}"
                            )).into()));
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

    pub(super) fn apply_trigger_ddl_batch(
        &self,
        ddl: Vec<DdlChange>,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<()> {
        if ddl.is_empty() {
            return Ok(());
        }
        if matches!(self.tx_mgr, DatabaseTransactionHost::Owned(_)) {
            return self.apply_original_trigger_ddl_batch(&ddl, read_memory);
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
                    return Err(Error::Other((format!(
                        "trigger already exists with a different definition: {name}"
                    )).into()));
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
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> std::result::Result<TriggerDispatchOutcome, Box<TriggerDispatchFailure>> {
        self.dispatch_triggers_for_tx_from(
            tx,
            None,
            TriggerDispatchMode::Local,
            vector_copy_owner,
            read_memory.clone(),
        )
    }

    pub(super) fn dispatch_sync_triggers_for_tx(
        &self,
        tx: TxId,
        declarations: Option<&BTreeMap<String, TriggerDeclaration>>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> std::result::Result<TriggerDispatchOutcome, Box<TriggerDispatchFailure>> {
        self.dispatch_triggers_for_tx_from(
            tx,
            declarations,
            TriggerDispatchMode::IncludingSync,
            vector_copy_owner,
            read_memory.clone(),
        )
    }

    pub(super) fn dispatch_private_sync_trigger_rows_for_tx(
        &self, tx: TxId, targets: super::connected_replay::PrivateChangedRows,
        declarations: &BTreeMap<String, TriggerDeclaration>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> std::result::Result<(TriggerDispatchOutcome, super::connected_replay::PrivateChangedRows, usize), Box<TriggerDispatchFailure>> {
        let initial_len = self.tx_mgr.with_write_set(tx, |ws| ws.relational_inserts.len())
            .map_err(|error| Box::new(TriggerDispatchFailure { error, active_guards: Vec::new() }))?;
        let mut run = TriggerDispatchRun {
            initial_targets: Some(targets), initial_len,
            processed: HashSet::new(), pending: Vec::new(), active_guards: Vec::new(),
            _private_key_credit: self.private_replay_owner(tx).map(contextdb_core::read_memory::ReadCredit::new),
            _native_memory: None,
        };
        if let Err(error) = self.dispatch_trigger_range(
            tx, 0, initial_len, 1, &mut run, Some(declarations),
            TriggerDispatchMode::IncludingSync, vector_copy_owner, read_memory,
        ) {
            let _ = self.append_rolled_back_trigger_audits(&run.pending, tx, &error);
            return Err(Box::new(TriggerDispatchFailure { error, active_guards: run.active_guards }));
        }
        let targets = run.initial_targets.take().expect("private dispatch owns its target set");
        Ok((TriggerDispatchOutcome { pending: run.pending, active_guards: run.active_guards }, targets, initial_len))
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
        if self.private_callbacks_already_prepared(tx) { return Ok(false); }
        if matches!(self.tx_mgr, DatabaseTransactionHost::Owned(_)) {
            let check = |declarations: &BTreeMap<String, TriggerDeclaration>| {
                self.tx_mgr.with_read(tx, |ws| declarations.values().any(|declaration|
                    declaration.including_sync && declaration.on_events.iter().any(|event|
                        matches!(event, TriggerEvent::Insert | TriggerEvent::Update))
                        && ws.relational_inserts.iter().any(|(table, _)| table == &declaration.table)))
            };
            return match declarations { Some(declarations) => check(declarations), None => check(&self.trigger.declarations.lock()) };
        }
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

    /// Check the same final staged row and event that local dispatch will see.
    /// Ordinary commits with no matching trigger need only final validation.
    pub(super) fn has_matching_local_trigger_for_tx(
        &self,
        tx: TxId,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<bool> {
        if matches!(self.tx_mgr, DatabaseTransactionHost::Owned(_)) {
            let snapshot = self.trigger_dispatch_snapshot_owned(tx, None, TriggerDispatchMode::Local)?;
            for ((table, row_id), index) in &snapshot.latest_insert_index {
                if !snapshot.triggered_tables.contains(table) { continue; }
                let present = self.tx_mgr.with_read(tx, |ws| ws.relational_inserts.get(*index)
                    .is_some_and(|(name, row)| name == table && row.row_id == *row_id && row.deleted_tx.is_none()))?;
                if !present { continue; }
                let (insert, update) = {
                    let declarations = self.trigger.declarations.lock();
                    (declarations.values().any(|declaration| declaration.table == *table
                        && declaration.on_events.contains(&TriggerEvent::Insert)),
                     declarations.values().any(|declaration| declaration.table == *table
                        && declaration.on_events.contains(&TriggerEvent::Update)))
                };
                if !insert && !update { continue; }
                let is_update = snapshot.deleted_rows.iter().any(|(name, id)| name == table && id == row_id)
                    && self.committed_row_exists_at_current_frontier(table, *row_id, read_memory.clone())?;
                if if is_update { update } else { insert } { return Ok(true); }
            }
            return Ok(false);
        }
        let declarations = self.trigger_declarations_from(None);
        if declarations.is_empty() {
            return Ok(false);
        }
        let mut events_by_table = HashMap::new();
        for declaration in declarations {
            let events = events_by_table
                .entry(declaration.table)
                .or_insert((false, false));
            events.0 |= declaration.on_events.contains(&TriggerEvent::Insert);
            events.1 |= declaration.on_events.contains(&TriggerEvent::Update);
        }
        let snapshot = self.trigger_dispatch_snapshot(tx, None, TriggerDispatchMode::Local)?;
        let _order_memory = crate::memory_accounting::OwnedMemoryReservation::try_new_for(
            self.accountant.clone(),
            snapshot
                .latest_insert_index
                .len()
                .saturating_mul(std::mem::size_of::<(&TriggerRowKey, &usize)>())
                .saturating_add(held::ALLOCATION_BYTES),
            "writer_source",
            "trigger_identities",
            "Raise MEMORY_LIMIT to inspect these trigger identities.",
        )?;
        let mut latest = snapshot.latest_insert_index.iter().collect::<Vec<_>>();
        latest.sort_unstable_by_key(|(_, index)| **index);
        for ((table, row_id), index) in latest {
            if !snapshot.triggered_tables.contains(table)
                || !self.tx_mgr.with_write_set(tx, |ws| {
                    ws.relational_inserts
                        .get(*index)
                        .is_some_and(|(name, row)| name == table && row.row_id == *row_id)
                })?
            {
                continue;
            }
            let event = if snapshot.deleted_rows.contains(&(table.clone(), *row_id))
                && self.committed_row_exists_at_current_frontier(
                    table,
                    *row_id,
                    read_memory.clone(),
                )? {
                TriggerEvent::Update
            } else {
                TriggerEvent::Insert
            };
            if events_by_table
                .get(table)
                .is_some_and(|events| match event {
                    TriggerEvent::Insert => events.0,
                    TriggerEvent::Update => events.1,
                })
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn dispatch_triggers_for_tx_from(
        &self,
        tx: TxId,
        declarations: Option<&BTreeMap<String, TriggerDeclaration>>,
        mode: TriggerDispatchMode,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
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
            initial_targets: None,
            initial_len: 0,
            processed: HashSet::new(),
            pending: Vec::new(),
            active_guards: Vec::new(),
            _private_key_credit: self.private_replay_owner(tx).map(contextdb_core::read_memory::ReadCredit::new),
            _native_memory: None,
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
        if let Err(error) = self.dispatch_trigger_range(
            tx,
            0,
            initial_len,
            1,
            &mut run,
            declarations,
            mode,
            vector_copy_owner,
            read_memory.clone(),
        ) {
            let _ = self.append_rolled_back_trigger_audits(&run.pending, tx, &error);
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
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<CommitValidationOutcome> {
        (|| {
            let conditional_update_guards = self.take_conditional_update_guards_for_tx(tx);
            let upsert_intents = self.pending_upsert_intents_for_tx(tx);
            let published = self.tx_mgr.capture_published_read();
            let snapshot = published.snapshot;
            let _writer_read = self.enter_writer_read(
                super::writer_read::durable_root(published.handle)?,
                read_memory.clone(),
            );
            self.with_snapshot_override(snapshot, || {
                self.tx_mgr.with_write_set_detached(tx, |ws| {
                    ws.canonicalize_final_state();
                    if ws.is_empty() {
                        return Ok(CommitValidationOutcome::default());
                    }
                    self.rewrite_txid_placeholders(tx, ws)?;
                    let validation = self.revalidate_conditional_updates(
                        ws,
                        snapshot,
                        &conditional_update_guards,
                        None,
                        vector_copy_owner,
                        read_memory.clone(),
                    )?;
                    self.rewrite_commit_time_upserts_for_write_set(
                        ws,
                        snapshot,
                        &upsert_intents,
                        vector_copy_owner,
                        read_memory.clone(),
                    )?;
                    ws.canonicalize_final_state();
                    Ok(CommitValidationOutcome {
                        committed_tx: None,
                        conditional_noop_count: validation.conditional_noop_count,
                        conditional_conflict_count: validation.conditional_conflict_count,
                    })
                })?
            })
        })()
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
            Ok(CommitValidationOutcome::default())
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
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<()> {
        let mut private_snapshot_credit = self.private_replay_owner(tx)
            .map(contextdb_core::read_memory::ReadCredit::new);
        self.admit_private_trigger_snapshot(tx,declarations,private_snapshot_credit.as_mut())?;
        let mut snapshot = self.trigger_dispatch_snapshot(tx, declarations, mode)?;
        let mut index = start;
        while index < end {
            let mut private_row_credit = None;
            let maybe_row = self.tx_mgr.with_write_set(tx, |ws| -> Result<_> {
                ws.relational_inserts.get(index).map(|(table,row)| {
                    private_row_credit = self.private_trigger_row_copy_credit(tx,table,row)?;
                    let copied = if private_row_credit.is_some() {
                        crate::memory_accounting::preparation_copy_row(row)?
                    } else { row.clone() };
                    Ok((table.clone(),copied))
                }).transpose()
            })??;
            let Some((table, row)) = maybe_row else {
                break;
            };
            if run.initial_targets.as_ref().is_some_and(|targets|
                index < run.initial_len && !targets.contains(&table, row.row_id)) {
                index += 1;
                continue;
            }
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
            let prefix_update = run.initial_targets.as_ref().is_some_and(|targets|
                index < run.initial_len && targets.is_update(&table, row.row_id));
            let event = if prefix_update || (snapshot.deleted_rows.contains(&(table.clone(), row.row_id))
                && self.committed_row_exists_at_current_frontier(
                    &table,
                    row.row_id,
                    read_memory.clone(),
                )?) {
                TriggerEvent::Update
            } else {
                TriggerEvent::Insert
            };
            for declaration in
                self.triggers_for_table_event_for_dispatch(declarations, &table, event, mode)
            {
                if !run.mark_firing(&table,row.row_id,&declaration.name)? { continue; }
                self.fire_trigger(
                    tx,
                    declaration,
                    TriggerFiring {
                        event,
                        row: &row,
                    },
                    depth,
                    run,
                    &snapshot.triggered_tables,
                    declarations,
                    mode,
                    vector_copy_owner,
                    read_memory.clone(),
                )?;
                if index.saturating_add(1) < end {
                    self.admit_private_trigger_snapshot(tx,declarations,private_snapshot_credit.as_mut())?;
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
        firing: TriggerFiring<'_>,
        depth: u32,
        run: &mut TriggerDispatchRun,
        triggered_tables: &HashSet<String>,
        declarations: Option<&BTreeMap<String, TriggerDeclaration>>,
        mode: TriggerDispatchMode,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
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
        let private_baseline = if mode == TriggerDispatchMode::IncludingSync {
            self.capture_private_trigger_baseline(tx)?
        } else { None };
        let before_write_set = (mode == TriggerDispatchMode::IncludingSync && private_baseline.is_none())
            .then(|| self.tx_mgr.cloned_write_set(tx)).transpose()?;
        let before_write_set = private_baseline.as_ref().map(|baseline| &baseline.write_set)
            .or(before_write_set.as_ref());
        let _private_context_credit = self.private_callback_copy_credit(
            tx, &declaration.name, &declaration.table, &firing.row.values,
        )?;
        let ctx = TriggerContext {
            trigger_name: declaration.name.clone(),
            table: declaration.table.clone(),
            event: firing.event,
            tx,
            depth,
            row_values: firing.row.values.clone(),
        };
        self.admit_private_trigger_guard(tx,&ctx.trigger_name)?;
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
        if let Some(before_write_set) = before_write_set {
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
            self.admit_private_prefix_audit(tx, &declaration.name)?;
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
            self.prepare_active_trigger_write_set_for_dispatch(
                tx,
                vector_copy_owner,
                read_memory.clone(),
            )
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
        self.admit_private_prefix_audit(tx, &declaration.name)?;
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
                vector_copy_owner,
                read_memory.clone(),
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
        self.tx_mgr.with_write_set(tx, |after| {
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
        } else if (after.adj_inserts != before.adj_inserts || after.adj_deletes != before.adj_deletes)
            && !self.private_graph_capture_active(tx)?
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
        })?
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

        read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<Vec<TriggerAuditEntry>> {
        self.committed_sync_pull_trigger_audits_with_source_order(
            ws, lsn, projected_declarations, None, None, read_memory,
        )
    }

    pub(super) fn committed_sync_pull_trigger_audits_with_source_order(
        &self,
        ws: &WriteSet,
        lsn: Lsn,
        projected_declarations: Option<&BTreeMap<String, TriggerDeclaration>>,
        connected: Option<(&ChangeSet, &ConnectedCoordinates)>,
        additional: Option<&[super::connected_replay::PrivateSyntheticTriggerFiring]>,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<Vec<TriggerAuditEntry>> {
        if ws.relational_inserts.is_empty() && additional.is_none() {
            return Ok(Vec::new());
        }
        let Some(firing_tx) = write_set_visibility_tx(ws).or_else(|| additional.and_then(|firings| firings.first()).map(|firing| firing.tx)) else {
            return Ok(Vec::new());
        };
        let inserted_tables = ws
            .relational_inserts
            .iter()
            .map(|(table, _)| table.clone())
            .collect::<HashSet<_>>();
        let mut candidate_trigger_tables = self
            .trigger_declarations_from(projected_declarations)
            .into_iter()
            .filter(|declaration| inserted_tables.contains(&declaration.table))
            .map(|declaration| declaration.table)
            .collect::<HashSet<_>>();
        if connected.is_some() {
            candidate_trigger_tables.extend(self.trigger.declarations.lock().values()
                .filter(|declaration| inserted_tables.contains(&declaration.table))
                .map(|declaration| declaration.table.clone()));
        }
        if candidate_trigger_tables.is_empty() && additional.is_none() {
            return Ok(Vec::new());
        }
        let deleted_rows = self.sync_pull_deleted_rows_by_table(
            ws,
            &candidate_trigger_tables,
            read_memory.clone(),
        )?;
        let (paired_inserts, paired_deletes) = self.sync_pull_paired_update_rows(ws, &deleted_rows);
        let total_changes = logical_write_set_data_entry_count(ws, &paired_deletes);
        let mut firing_rows = HashSet::new();
        let mut audits = Vec::new();
        // One evolving prefix and one current group's transient declarations
        // replace the union of every trigger declared anywhere in a connected
        // acceptance. Ordinary single-source transactions retain their existing
        // audit semantics, including CREATE/DROP within that source group.
        let mut order_credit = contextdb_core::read_memory::ReadCredit::new(
            self.finalization.accountant.read_budget()?,
        );
        let mut source_projection = if let Some((changes, coordinates)) = connected {
            let declarations = self.trigger.declarations.lock();
            let declaration_bytes = declarations.values().fold(0usize, |bytes, declaration| {
                bytes.saturating_add(declaration.name.len())
                    .saturating_add(declaration.table.len())
                    .saturating_add(declaration.on_events.len().saturating_mul(32))
                    .saturating_add(512)
            });
            order_credit.try_grow_for(
                declaration_bytes.saturating_mul(3)
                    .saturating_add(changes.ddl.iter().map(DdlChange::estimated_bytes)
                        .fold(0usize, usize::saturating_add).saturating_mul(8))
                    .saturating_add(changes.ddl.len().saturating_mul(128))
                    .saturating_add(ws.relational_inserts.len().saturating_mul(64)),
                "sync", "connected_trigger_audit_prefix",
                "Raise MEMORY_LIMIT to prepare connected trigger audits.",
            )?;
            let prefix = declarations.clone();
            drop(declarations);
            let (catalog_credit, tables) = self.selected_received_catalog(changes, std::iter::empty())?;
            let mut ddl = changes.ddl.iter().zip(&changes.ddl_lsn)
                .map(|(change, position)| Ok((coordinates.local(*position)?, change)))
                .collect::<Result<Vec<_>>>()?;
            ddl.sort_by_key(|(position, _)| *position);
            Some((tables, prefix, BTreeMap::new(), ddl, 0usize, None, catalog_credit))
        } else {
            None
        };
        let mut rows = Vec::new();
        if connected.is_some() {
            rows.extend(ws.relational_inserts.iter());
            rows.sort_by_key(|(_, row)| row.lsn);
        }
        let rows = rows.into_iter().chain(
            ws.relational_inserts.iter().filter(|_| connected.is_none()),
        );
        for (table, row) in rows {
            let mut row_declarations = projected_declarations;
            if let Some((tables, prefix, current, ddl, next, current_position, _)) = source_projection.as_mut() {
                while *next < ddl.len() && ddl[*next].0 <= row.lsn {
                    let position = ddl[*next].0;
                    *current = prefix.clone();
                    *current_position = Some(position);
                    while *next < ddl.len() && ddl[*next].0 == position {
                        let change = ddl[*next].1;
                        Self::apply_sync_table_ddl_to_projected_meta(tables, change);
                        self.apply_sync_trigger_ddl_to_projection(prefix, change, tables)?;
                        if let DdlChange::CreateTrigger { name, .. }
                            | DdlChange::CreateTriggerIncludingSync { name, .. } = change {
                            // A same-group DROP/CREATE may reuse a trigger name.
                            // The validated last declaration keeps the ordinary
                            // one-source transaction's existing audit behavior.
                            current.remove(name);
                            self.apply_sync_trigger_ddl_to_projection(current, change, tables)?;
                        }
                        *next += 1;
                    }
                }
                row_declarations = Some(if *current_position == Some(row.lsn) { current } else { prefix });
            }
            let event = if paired_inserts.contains(&(table.clone(), row.row_id)) {
                TriggerEvent::Update
            } else {
                TriggerEvent::Insert
            };
            let declarations =
                self.triggers_for_table_event_from(row_declarations, table, event);
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
        if let Some(prefix) = additional {
            // Source phases already selected the actual accepted event against
            // its declarations before later DDL. Final-row reconstruction can
            // otherwise invent an INSERT for a later source UPDATE, or lose
            // an accepted firing whose table was subsequently dropped.
            // The established unit-wide cascade calculation stays unchanged.
            audits.clear();
            for firing in prefix {
                audits.push(TriggerAuditEntry { trigger_name: firing.trigger_name.clone(),firing_tx: firing.tx,
                    firing_lsn: firing.lsn,depth: 1,cascade_row_count,status: TriggerAuditStatus::Fired });
            }
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

        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
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
        let snapshot = self.snapshot_for_read();
        let mut deleted_rows = HashMap::new();
        for (table, row_ids) in deleted_ids_by_table {
            let mut row_ids = row_ids.into_iter().collect::<Vec<_>>();
            row_ids.sort_unstable();
            let mut rows = Vec::new();
            for row_id in row_ids {
                if let Some(row) =
                    self.committed_row_at(&table, row_id, snapshot, read_memory.clone())?
                {
                    rows.push(row);
                }
            }
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
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
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
        let generation_sidecars = self.ddl_generation_sidecars(ddl, read_memory.clone())?;
        let provenance_values =
            self.ddl_generation_sidecar_values(lsn, ddl, start_ordinal, read_memory.clone())?;
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
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<()> {
        self.log_drop_table_ddl_and_remove_triggers_with_prefix(
            table,
            lsn,
            &[],
            read_memory.clone(),
        )
    }

    pub(crate) fn log_drop_table_ddl_and_remove_triggers_with_prefix(
        &self,
        table: &str,
        lsn: Lsn,
        prefix_trigger_ddl: &[DdlChange],
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
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
        let provenance_values =
            self.ddl_generation_sidecar_values(lsn, &ddl, 0, read_memory.clone())?;
        config_values.extend(
            provenance_values
                .iter()
                .map(|(key, value)| (key.as_str(), value.clone())),
        );
        let graph_edges = self.graph_edges_after_table_drop(table, read_memory.clone())?;
        let row_graph_edges =
            self.row_graph_witnesses_for_table_drop(table, read_memory.clone())?;
        let history = if self.persistence.is_none() {
            Some(self.relational_store.prepare_schema_generation(std::iter::once(table), self.engine_sync_read_memory()?)?)
        } else { None };
        if let Some(persistence) = &self.persistence {
            persistence.remove_table_rewrite_aux_with_config_values_and_ddl_log(
                table,
                config_values,
                lsn,
                &ddl,
                &row_graph_edges,
            )?;
        } else {
            self.record_ddl_generation_sidecars(lsn, &ddl, read_memory.clone())?;
        }
        self.apply_graph_edges_after_table_drop(graph_edges);
        self.remove_rank_formulas_for_table(table);
        self.vector_store_deregister_table(table);
        if let Some(source)=crate::persistence::received_working_source::WorkingReadSource::current_for(self.tx_mgr.identity())? {
            source.project_schema(table,None,&ddl)?;
        }
        self.relational_store().drop_table_with_generation(table, history);
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
        error: &Error,
    ) -> Result<()> {
        // No trigger ran: do not format or copy the primary error, or enter
        // audit preparation, after the failed operation.
        if pending.is_empty() { return Ok(()); }
        // Pending-audit diagnostic/copy admission still needs the producing
        // formatter and publication custody; this does not claim it is funded.
        let reason = error.to_string();
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
        if matches!(self.tx_mgr, DatabaseTransactionHost::Owned(_)) {
            let publication = self.with_preparation_original(|original|
                self.prepare_original_audit_memory(original, std::slice::from_ref(&entry)))?;
            if let Some(persistence) = &self.persistence {
                self.tx_mgr.with_deferred_metadata_write(|| {
                    let index = self.trigger.next_audit_index.fetch_add(1, Ordering::SeqCst);
                    persistence.append_trigger_audit(index, &entry)
                })?;
            } else { self.trigger.next_audit_index.fetch_add(1, Ordering::SeqCst); }
            self.publish_original_audit_memory(publication);
            return Ok(());
        }
        if let Some(persistence) = &self.persistence {
            let publication = self.tx_mgr.reserve_deferred_metadata_publication()?;
            let index = self.trigger.next_audit_index.fetch_add(1, Ordering::SeqCst);
            persistence.append_trigger_audit(index, &entry)?;
            self.tx_mgr.owe_metadata_publication(publication);
        } else {
            self.trigger.next_audit_index.fetch_add(1, Ordering::SeqCst);
        }
        self.append_trigger_audit_to_memory(entry);
        Ok(())
    }

    pub(super) fn append_trigger_audit_to_memory(&self, entry: TriggerAuditEntry) {
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

// Scoped consumers keep exceptional temporary custody through callbacks and audits.
impl Database {
    pub(super) fn prepare_active_trigger_write_set_for_dispatch_owned(
        &self,
        scope: &DatabaseCommitScope<'_, '_, '_>,
        tx: TxId,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> PreparationResult<CommitValidationOutcome> {
        scope.prepare_temporary(
            tx,
            take_owned_conditions,
            |guard, peer, conditional_update_guards| {
                let published = self.tx_mgr.capture_published_read();
                let snapshot = published.snapshot;
                let _writer_read = self.enter_writer_read(
                    super::writer_read::durable_root(published.handle)?,
                    read_memory.clone(),
                );
                self.with_snapshot_override(snapshot, || {
                    let canonical = guard.prepare_canonicalize()?;
                    guard.publish(canonical);
                    if guard.read().is_empty() {
                        return Ok(CommitValidationOutcome::default());
                    }
                    self.rewrite_txid_placeholders_in_prepared_write_set(tx, guard)?;
                    // The validator checked each original body and invoice before detach.
                    // The moved buffer keeps both live while the producer borrows its bodies.
                    let conditions = conditional_update_guards.values.iter().map(|entry| {
                        entry
                            .body
                            .as_ref()
                            .expect("validated original conditional body")
                    });
                    let validation = self.revalidate_conditional_updates_in_prepared_write_set(
                        guard,
                        snapshot,
                        conditions,
                        None,
                        vector_copy_owner,
                        read_memory.clone(),
                    )?;
                    // Borrow only inside this HRTB callback; no intent reference escapes it.
                    let upserts = peer
                        .original
                        .node
                        .as_ref()
                        .and_then(|node| node.value.as_deref())
                        .into_iter()
                        .flat_map(|record| record.metadata.upsert_intents.values.iter())
                        .map(|entry| entry.body.as_ref().expect("validated original upsert body"));
                    self.rewrite_commit_time_upserts_in_prepared_write_set(
                        guard,
                        snapshot,
                        upserts,
                        vector_copy_owner,
                        read_memory.clone(),
                    )?;
                    let canonical = guard.prepare_canonicalize()?;
                    guard.publish(canonical);
                    Ok(CommitValidationOutcome {
                        committed_tx: None,
                        conditional_noop_count: validation.conditional_noop_count,
                        conditional_conflict_count: validation.conditional_conflict_count,
                    })
                })
            },
        )
    }

    pub(super) fn prepare_active_sync_trigger_write_set_for_dispatch_owned(
        &self,
        scope: &DatabaseCommitScope<'_, '_, '_>,
        tx: TxId,
    ) -> PreparationResult<CommitValidationOutcome> {
        scope.prepare_temporary(
            tx,
            |_| Ok(()),
            |guard, _peer, ()| {
                let canonical = guard.prepare_canonicalize()?;
                guard.publish(canonical);
                if guard.read().is_empty() {
                    return Ok(CommitValidationOutcome::default());
                }
                Ok(CommitValidationOutcome::default())
            },
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn dispatch_trigger_range_owned(
        &self,
        scope: &DatabaseCommitScope<'_, '_, '_>,
        tx: TxId,
        start: usize,
        end: usize,
        depth: u32,
        run: &mut TriggerDispatchRun,
        declarations: Option<&BTreeMap<String, TriggerDeclaration>>,
        mode: TriggerDispatchMode,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> PreparationResult<()> {
        let mut snapshot = self
            .trigger_dispatch_snapshot_owned(tx, declarations, mode)
            .map_err(TemporaryFailure::from_error)?;
        let mut index = start;
        while index < end {
            // Quote while borrowing, then release the active source before
            // admission. The second borrow either copies within that invoice
            // or requests a larger one; no caller-owned row body escapes a
            // guard without its original reservation.
            let _row_memory;
            let maybe_row;
            loop {
                let bytes = self.tx_mgr.with_read(tx, |ws| {
                    ws.relational_inserts.get(index).map_or(Ok(0), |(table, row)| {
                        crate::memory_accounting::preparation_row_copy_bytes(row)?
                            .checked_add(table.len())
                            .ok_or_else(EngineMetadataJournal::identity_error)
                    })
                }).map_err(TemporaryFailure::from_error)?
                    .map_err(TemporaryFailure::from_error)?;
                let memory = self.reserve_owned_preparation(bytes)
                    .map_err(TemporaryFailure::from_error)?;
                let copied = self.tx_mgr.with_read(tx, |ws| {
                    let Some((table, row)) = ws.relational_inserts.get(index) else {
                        return Ok(Some(None));
                    };
                    let needed = crate::memory_accounting::preparation_row_copy_bytes(row)?
                        .checked_add(table.len())
                        .ok_or_else(EngineMetadataJournal::identity_error)?;
                    if needed > bytes { return Ok(None); }
                    Ok(Some(Some((
                        crate::memory_accounting::preparation_copy_string(table)?,
                        crate::memory_accounting::preparation_copy_row(row)?,
                    ))))
                }).map_err(TemporaryFailure::from_error)?
                    .map_err(TemporaryFailure::from_error)?;
                if let Some(row) = copied {
                    // These locals are deliberately declared in this order:
                    // the callback's copied body dies before its admission.
                    _row_memory = memory;
                    maybe_row = row;
                    break;
                }
                drop(memory);
            }
            let Some((table, row)) = maybe_row else {
                break;
            };
            if run.initial_targets.as_ref().is_some_and(|targets|
                index < run.initial_len && !targets.contains(&table, row.row_id)) {
                index += 1;
                continue;
            }
            if !snapshot.triggered_tables.contains(&table) {
                index += 1;
                continue;
            }
            if snapshot.latest_insert_index.iter().find_map(|((name, id), index)|
                    (name == &table && *id == row.row_id).then_some(index))
                != Some(&index)
            {
                index += 1;
                continue;
            }
            let prefix_update = run.initial_targets.as_ref().is_some_and(|targets|
                index < run.initial_len && targets.is_update(&table, row.row_id));
            let event = if prefix_update || (snapshot.deleted_rows.iter().any(|(name, id)| name == &table && *id == row.row_id)
                && self
                    .committed_row_exists_at_current_frontier(
                        &table,
                        row.row_id,
                        read_memory.clone(),
                    )
                    .map_err(TemporaryFailure::from_error)?)
            {
                TriggerEvent::Update
            } else {
                TriggerEvent::Insert
            };
            let (_declaration_memory, selected) = self.owned_trigger_declarations_for_dispatch(
                declarations, &table, event, mode).map_err(TemporaryFailure::from_error)?;
            for declaration in selected {
                if !run.mark_firing(&table,row.row_id,&declaration.name)
                    .map_err(TemporaryFailure::from_error)? { continue; }
                self.fire_trigger_owned(
                    scope,
                    tx,
                    declaration,
                    TriggerFiring {
                        event,
                        row: &row,
                    },
                    depth,
                    run,
                    &snapshot.triggered_tables,
                    declarations,
                    mode,
                    vector_copy_owner,
                    read_memory.clone(),
                )?;
                if index.saturating_add(1) < end {
                    snapshot = self
                        .trigger_dispatch_snapshot_owned(tx, declarations, mode)
                        .map_err(TemporaryFailure::from_error)?;
                }
            }
            index += 1;
        }
        Ok(())
    }

    fn owned_trigger_declarations_for_dispatch(
        &self, declarations: Option<&BTreeMap<String, TriggerDeclaration>>, table: &str,
        event: TriggerEvent, mode: TriggerDispatchMode,
    ) -> Result<(OwnedMemoryReservation, Vec<TriggerDeclaration>)> {
        let selected = |declaration: &&TriggerDeclaration| declaration.table == table
            && declaration.on_events.contains(&event) && mode.includes(declaration);
        let quote = |declarations: &BTreeMap<String, TriggerDeclaration>| -> Result<(usize, usize)> {
            let count = declarations.values().filter(selected).count();
            let controls = std::alloc::Layout::array::<TriggerDeclaration>(count)
                .map_err(|_| EngineMetadataJournal::identity_error())?.size();
            let bytes = declarations.values().filter(selected).try_fold(controls, |bytes, declaration|
                bytes.checked_add(crate::persistence::content_identity::original_record_work_bytes(declaration)?)
                    .ok_or_else(EngineMetadataJournal::identity_error))?;
            Ok((count, bytes))
        };
        loop {
            let (count, bytes) = match declarations { Some(declarations) => quote(declarations)?,
                None => quote(&self.trigger.declarations.lock())? };
            let memory = self.reserve_owned_preparation(bytes)?;
            let copy = |declarations: &BTreeMap<String, TriggerDeclaration>| -> Result<Option<Vec<TriggerDeclaration>>> {
                let (current, needed) = quote(declarations)?;
                if current > count || needed > bytes { return Ok(None); }
                let mut copied = Vec::with_capacity(current);
                copied.extend(declarations.values().filter(selected).cloned());
                if copied.capacity() != current { return Err(EngineMetadataJournal::identity_error()); }
                Ok(Some(copied))
            };
            let copied = match declarations { Some(declarations) => copy(declarations)?,
                None => copy(&self.trigger.declarations.lock())? };
            if let Some(copied) = copied { return Ok((memory, copied)); }
        }
    }

    fn trigger_dispatch_snapshot_owned(
        &self,
        tx: TxId,
        declarations: Option<&BTreeMap<String, TriggerDeclaration>>,
        mode: TriggerDispatchMode,
    ) -> Result<OwnedTriggerDispatchSnapshot> {
        fn quote(
            declarations: &BTreeMap<String, TriggerDeclaration>,
            mode: TriggerDispatchMode,
            ws: &contextdb_tx::manager::EngineRead<'_>,
        ) -> Result<usize> {
            use crate::memory_accounting::preparation_native_map_bytes;
            let selected = declarations.values().filter(|declaration| mode.includes(declaration));
            let count = selected.clone().count();
            let mut bytes = preparation_native_map_bytes::<String, ()>(count)?
                .checked_add(preparation_native_map_bytes::<TriggerRowKey, usize>(ws.relational_inserts.len())?)
                .and_then(|bytes| bytes.checked_add(
                    preparation_native_map_bytes::<TriggerRowKey, ()>(ws.relational_deletes.len()).ok()?))
                .ok_or_else(EngineMetadataJournal::identity_error)?;
            for table in selected.map(|declaration| declaration.table.as_str())
                .chain(ws.relational_inserts.iter().map(|(table, _)| table.as_str()))
                .chain(ws.relational_deletes.iter().map(|(table, _, _)| table.as_str())) {
                bytes = bytes.checked_add(table.len()).ok_or_else(EngineMetadataJournal::identity_error)?;
            }
            Ok(bytes)
        }
        let with_declarations = |f: &mut dyn FnMut(&BTreeMap<String, TriggerDeclaration>) -> Result<usize>| {
            match declarations {
                Some(declarations) => f(declarations),
                None => f(&self.trigger.declarations.lock()),
            }
        };
        loop {
            // No declaration/active-tx guard is held across original admission.
            let bytes = with_declarations(&mut |declarations| {
                self.tx_mgr.with_read(tx, |ws| quote(declarations, mode, &ws))?
            })?;
            let memory = self.reserve_owned_preparation(bytes)?;
            let mut copied = None;
            with_declarations(&mut |declarations| {
                self.tx_mgr.with_read(tx, |ws| -> Result<usize> {
                    if quote(declarations, mode, &ws)? > bytes { return Ok(0); }
                    let mut triggered_tables = HashSet::with_capacity(declarations.values()
                        .filter(|declaration| mode.includes(declaration)).count());
                    for declaration in declarations.values().filter(|declaration| mode.includes(declaration)) {
                        triggered_tables.insert(crate::memory_accounting::preparation_copy_string(&declaration.table)?);
                    }
                    let mut latest_insert_index = HashMap::with_capacity(ws.relational_inserts.len());
                    for (index, (table, row)) in ws.relational_inserts.iter().enumerate() {
                        latest_insert_index.insert((crate::memory_accounting::preparation_copy_string(table)?, row.row_id), index);
                    }
                    let mut deleted_rows = HashSet::with_capacity(ws.relational_deletes.len());
                    for (table, row, _) in ws.relational_deletes {
                        deleted_rows.insert((crate::memory_accounting::preparation_copy_string(table)?, *row));
                    }
                    copied = Some(TriggerDispatchSnapshot { triggered_tables, latest_insert_index, deleted_rows });
                    Ok(bytes)
                })?
            })?;
            if let Some(snapshot) = copied {
                return Ok(OwnedTriggerDispatchSnapshot { snapshot, _memory: memory });
            }
            // A concurrent larger staged image needs another admitted quote.
            // The empty attempted image dies before this debit is retired.
            drop(memory);
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn fire_trigger_owned(
        &self,
        scope: &DatabaseCommitScope<'_, '_, '_>,
        tx: TxId,
        declaration: TriggerDeclaration,
        firing: TriggerFiring<'_>,
        depth: u32,
        run: &mut TriggerDispatchRun,
        triggered_tables: &HashSet<String>,
        declarations: Option<&BTreeMap<String, TriggerDeclaration>>,
        mode: TriggerDispatchMode,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> PreparationResult<()> {
        if depth > TRIGGER_CASCADE_DEPTH_CAP {
            let entry = TriggerAuditEntry {
                trigger_name: declaration.name.clone(),
                firing_tx: tx,
                firing_lsn: Lsn(0),
                depth,
                cascade_row_count: 0,
                status: TriggerAuditStatus::DepthExceeded,
            };
            self.append_trigger_audit(entry)
                .map_err(TemporaryFailure::from_error)?;
            return Err(TemporaryFailure::from_error(
                Error::TriggerCascadeDepthExceeded {
                    trigger_name: declaration.name,
                    depth,
                },
            ));
        }
        let callback = self
            .trigger
            .callbacks
            .read()
            .get(&declaration.name)
            .cloned()
            .ok_or_else(|| Error::TriggerCallbackMissing {
                trigger_name: declaration.name.clone(),
            })
            .map_err(TemporaryFailure::from_error)?;
        let before = self
            .write_set_counts(tx)
            .map_err(TemporaryFailure::from_error)?;
        let before_write_set = (mode == TriggerDispatchMode::IncludingSync)
            .then(|| {
                scope.prepare_temporary(tx, |_| Ok(()), |guard, _peer, ()| guard.capture_private())
            })
            .transpose()?;
        let _private_context_credit = self
            .private_callback_copy_credit(
                tx,
                &declaration.name,
                &declaration.table,
                &firing.row.values,
            )
            .map_err(TemporaryFailure::from_error)?;
        let _context_memory = self.reserve_owned_preparation(
            crate::memory_accounting::preparation_values_copy_bytes(&firing.row.values).map_err(TemporaryFailure::from_error)?
                .checked_add(declaration.name.len()).and_then(|n| n.checked_add(declaration.table.len()))
                .ok_or_else(EngineMetadataJournal::identity_error).map_err(TemporaryFailure::from_error)?).map_err(TemporaryFailure::from_error)?;
        let ctx = TriggerContext {
            trigger_name: crate::memory_accounting::preparation_copy_string(&declaration.name).map_err(TemporaryFailure::from_error)?,
            table: crate::memory_accounting::preparation_copy_string(&declaration.table).map_err(TemporaryFailure::from_error)?,
            event: firing.event,
            tx,
            depth,
            row_values: crate::memory_accounting::preparation_copy_values(&firing.row.values).map_err(TemporaryFailure::from_error)?,
        };
        run.admit_guard(&ctx.trigger_name).map_err(TemporaryFailure::from_error)?;
        let callback_thread = self.with_preparation_original(|original|
            TriggerState::enter_callback_thread_scope_owned(&self.trigger, &ctx.trigger_name, tx, original))
            .map_err(TemporaryFailure::from_error)?;
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
            let error = TemporaryFailure::from_error(error);
            let entry = TriggerAuditEntry {
                trigger_name: declaration.name,
                firing_tx: tx,
                firing_lsn: Lsn(0),
                depth,
                cascade_row_count: 0,
                status: TriggerAuditStatus::RolledBack {
                    reason: error.error().to_string(),
                },
            };
            if let Err(audit_error) = self.append_trigger_audit(entry) {
                return Err(error.map_error(|_| audit_error));
            }
            return Err(error);
        }
        if let Some(before_write_set) = before_write_set.as_ref() {
            // Classify raw callback effects before schema/vector validation
            // can obscure the dedicated effect error. Then normalize and
            // prove the normalized delta remains append-only. Derived TXID
            // placeholders are rewritten later, after final Tx reassignment.
            let validation = self
                .validate_sync_trigger_effects_owned(tx, &declaration.name, before_write_set)
                .map_err(TemporaryFailure::from_error)
                .and_then(|_| {
                    self.prepare_active_sync_trigger_write_set_for_dispatch_owned(scope, tx)
                })
                .and_then(|_| {
                    self.validate_sync_trigger_effects_owned(
                        tx,
                        &declaration.name,
                        before_write_set,
                    )
                    .map_err(TemporaryFailure::from_error)
                });
            if let Err(error) = validation {
                let entry = TriggerAuditEntry {
                    trigger_name: declaration.name,
                    firing_tx: tx,
                    firing_lsn: Lsn(0),
                    depth,
                    cascade_row_count: 0,
                    status: TriggerAuditStatus::RolledBack {
                        reason: error.error().to_string(),
                    },
                };
                if let Err(audit_error) = self.append_trigger_audit(entry) {
                    return Err(error.map_error(|_| audit_error));
                }
                return Err(error);
            }
        }

        let after = self.write_set_counts(tx).unwrap_or(before);
        let nested_start = before.relational_inserts;
        let nested_end = after.relational_inserts;
        if nested_end <= nested_start
            || !self
                .trigger_range_has_triggered_table_owned(
                    tx,
                    nested_start,
                    nested_end,
                    triggered_tables,
                )
                .map_err(TemporaryFailure::from_error)?
        {
            self.admit_private_prefix_audit(tx, &declaration.name)
                .map_err(TemporaryFailure::from_error)?;
            run.push_owned_pending(&declaration.name, depth, before.delta_to(after))
                .map_err(TemporaryFailure::from_error)?;
            return Ok(());
        }

        let preparation = if mode == TriggerDispatchMode::IncludingSync {
            self.prepare_active_sync_trigger_write_set_for_dispatch_owned(scope, tx)
        } else {
            self.prepare_active_trigger_write_set_for_dispatch_owned(
                scope,
                tx,
                vector_copy_owner,
                read_memory.clone(),
            )
        };
        if let Err(error) = preparation {
            let entry = TriggerAuditEntry {
                trigger_name: declaration.name,
                firing_tx: tx,
                firing_lsn: Lsn(0),
                depth,
                cascade_row_count: 0,
                status: TriggerAuditStatus::RolledBack {
                    reason: error.error().to_string(),
                },
            };
            if let Err(audit_error) = self.append_trigger_audit(entry) {
                return Err(error.map_error(|_| audit_error));
            }
            return Err(error);
        }
        let after = self.write_set_counts(tx).unwrap_or(before);
        let nested_start = before.relational_inserts;
        let nested_end = after.relational_inserts;
        self.admit_private_prefix_audit(tx, &declaration.name)
            .map_err(TemporaryFailure::from_error)?;
        run.push_owned_pending(&declaration.name, depth, before.delta_to(after))
            .map_err(TemporaryFailure::from_error)?;
        if nested_end > nested_start {
            self.dispatch_trigger_range_owned(
                scope,
                tx,
                nested_start,
                nested_end,
                depth.saturating_add(1),
                run,
                declarations,
                mode,
                vector_copy_owner,
                read_memory.clone(),
            )?;
        }
        Ok(())
    }

    fn validate_sync_trigger_effects_owned(
        &self,
        tx: TxId,
        trigger_name: &str,
        before: &contextdb_tx::manager::EngineCapturedWriteSet,
    ) -> Result<()> {
        before.with_legacy(|before| {
            self.tx_mgr.with_read(tx, |after| {
        let forbidden = if after.relational_inserts.len() < before.relational_inserts.len()
            || !after
                .relational_inserts
                .starts_with(&before.relational_inserts)
            || after.relational_deletes != &before.relational_deletes
            || after.relational_delete_predicates != &before.relational_delete_predicates
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
        } else if after.adj_inserts != &before.adj_inserts || after.adj_deletes != &before.adj_deletes
        {
            Some("graph insert/delete")
        } else if after.vector_inserts != &before.vector_inserts
            || after.vector_deletes != &before.vector_deletes
            || after.vector_moves != &before.vector_moves
        {
            Some("vector insert/delete/move")
        } else if after.config_max_u64_keys != &before.config_max_u64_keys
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
            })?
        })
    }

    fn trigger_range_has_triggered_table_owned(
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
        self.tx_mgr.with_read(tx, |ws| {
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
}

// The native table-drop route prepares this event component before its durable
// trigger/table/graph write, then publishes at the original event-definition
// position. The unchanged Legacy table-drop helper remains selected today.
pub(super) struct OwnedTableDropEventCompanion {
    definitions: Option<event_bus::OwnedDefinitionPublication>,
    events: event_bus::OwnedPreparedSinkEvents,
}
impl Database {
    pub(super) fn prepare_owned_table_drop_event_companion(
        &self,
        events: &event_bus::OwnedEventBusHandle,
        table: &str,
    ) -> Result<OwnedTableDropEventCompanion> {
        match &self.tx_mgr {
            DatabaseTransactionHost::Owned(handle) => handle.with_host(|host| {
                crate::memory_accounting::validate_owned_original(
                    &events.data().original,
                    &self.finalization.accountant,
                    &host.funder,
                )
            })?,
            DatabaseTransactionHost::Legacy(_) => {
                return Err(Error::Other(
                    "owning table drop requires its actual native database host".into()));
            }
        }
        let definitions = events.prepare_owned_event_bus_without_table(table)?;
        let prepared_events = events.prepare_owned_sink_events(&[])?;
        Ok(OwnedTableDropEventCompanion {
            definitions,
            events: prepared_events,
        })
    }
    pub(super) fn with_owned_table_drop_event_config<R>(
        &self,
        stage: &OwnedTableDropEventCompanion,
        flush: impl for<'e> FnOnce(Option<&'e [(String, Vec<u8>)]>) -> Result<R>,
    ) -> Result<R> {
        flush(
            stage
                .definitions
                .as_ref()
                .map(|definitions| definitions.config.values.as_slice()),
        )
    }
    pub(super) fn publish_owned_table_drop_event_companion(
        &self,
        events: &event_bus::OwnedEventBusHandle,
        stage: OwnedTableDropEventCompanion,
    ) {
        if let Some(definitions) = stage.definitions {
            events.publish_owned_definition_publication(definitions, stage.events);
        } else {
            events.publish_owned_sink_events_to_memory(stage.events);
        }
    }
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

impl Database {
    pub(super) fn retain_private_plain_trigger_firings(&self, tx: TxId,
        targets: &super::connected_replay::PrivateChangedRows,initial_len: usize,lsn: Lsn,
        declarations: &BTreeMap<String,TriggerDeclaration>,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>) -> Result<()> {
        self.tx_mgr.with_write_set(tx,|ws| -> Result<()> {
            for (position,(table,row)) in ws.relational_inserts.iter().enumerate() {
                if position < initial_len && !targets.contains(table,row.row_id) { continue; }
                if ws.relational_inserts[position + 1..].iter().any(|(later_table,later)|
                    later_table == table && later.row_id == row.row_id) { continue; }
                let event = if (position < initial_len && targets.is_update(table,row.row_id))
                    || (ws.relational_deletes.iter().any(|(name,id,_)| name == table && *id == row.row_id)
                        && self.committed_row_exists_at_current_frontier(table,row.row_id,read_memory.clone())?) {
                    TriggerEvent::Update
                } else { TriggerEvent::Insert };
                for declaration in declarations.values().filter(|declaration|
                    !declaration.including_sync && declaration.table == *table && declaration.on_events.contains(&event)) {
                    self.retain_private_synthetic_firing(tx,table,row.row_id,&declaration.name,lsn)?;
                }
            }
            Ok(())
        })?
    }
}

impl Database {
    /// The ordinary Native coordinator owns `run` through terminal cleanup;
    /// active callbacks never move its paid audit/name buffers out of that owner.
    pub(super) fn dispatch_triggers_for_tx_owned(
        &self, scope: &DatabaseCommitScope<'_, '_, '_>, tx: TxId,
        declarations: Option<&BTreeMap<String, TriggerDeclaration>>, including_sync: bool,
        run: &mut TriggerDispatchRun,
        vectors: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> PreparationResult<()> {
        let mode = if including_sync { TriggerDispatchMode::IncludingSync } else { TriggerDispatchMode::Local };
        let count = self.tx_mgr.with_read(tx, |ws| ws.relational_inserts.len()).map_err(TemporaryFailure::from_error)?;
        self.dispatch_trigger_range_owned(scope, tx, 0, count, 1, run, declarations, mode, vectors, read_memory)
    }
}


pub(super) struct OriginalOrdinaryAudits {
    pub(super) entries: Vec<TriggerAuditEntry>,
    _memory: OwnedMemoryReservation,
}
impl Database {
    pub(super) fn prepare_original_ordinary_audits(&self, original: &OwnedMemoryReservation,
        ws: &WriteSet, source: CommitSource, lsn: Lsn, pending: &[PendingTriggerAudit],
        projected: Option<&BTreeMap<String, TriggerDeclaration>>,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>) -> Result<OriginalOrdinaryAudits> {
        if let Some(projected) = projected {
            return self.prepare_original_ordinary_audits_from(original, ws, source, lsn, pending, projected, read_memory);
        }
        if let Some(staged) = self.trigger.pin_original_trigger_stage(lsn) {
            return self.prepare_original_ordinary_audits_from(original, ws, source, lsn, pending,
                &staged.publication.publication.declarations, read_memory);
        }
        let staged = self.trigger.staged_ddl_for_persistence.lock();
        if let Some(staged) = staged.get(&lsn) {
            return self.prepare_original_ordinary_audits_from(original, ws, source, lsn, pending, &staged.declarations, read_memory);
        }
        drop(staged);
        let declarations = self.trigger.declarations.lock();
        self.prepare_original_ordinary_audits_from(original, ws, source, lsn, pending, &declarations, read_memory)
    }

    fn prepare_original_ordinary_audits_from(&self, original: &OwnedMemoryReservation,
        ws: &WriteSet, source: CommitSource, lsn: Lsn, pending: &[PendingTriggerAudit],
        declarations: &BTreeMap<String, TriggerDeclaration>,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>) -> Result<OriginalOrdinaryAudits> {
        let frame = crate::persistence::OriginalPersistenceFrame::new(original)?;
        let Some(firing_tx) = write_set_visibility_tx(ws) else {
            return Ok(OriginalOrdinaryAudits { entries: Vec::new(), _memory: frame.into_reservation() });
        };
        let sync = source == CommitSource::SyncPull;
        let mut capacity = pending.len();
        for audit in pending { frame.reserve(audit.trigger_name.len())?; }
        if sync {
            for (table, _) in &ws.relational_inserts {
                for declaration in declarations.values().filter(|declaration| declaration.table == *table
                    && !declaration.including_sync && declaration.on_events.iter().any(|event|
                        matches!(event, TriggerEvent::Insert | TriggerEvent::Update))) {
                    capacity = capacity.checked_add(1).ok_or_else(EngineMetadataJournal::identity_error)?;
                    frame.reserve(declaration.name.len())?;
                }
            }
        }
        frame.array::<TriggerAuditEntry>(capacity)?;
        let mut entries = Vec::with_capacity(capacity);
        if entries.capacity() != capacity { return Err(EngineMetadataJournal::identity_error()); }
        if sync && !ws.relational_inserts.is_empty() {
            frame.array::<usize>(ws.relational_deletes.len())?;
            frame.array::<bool>(ws.relational_deletes.len())?;
            frame.array::<Option<usize>>(ws.relational_inserts.len())?;
            let mut deletes = Vec::with_capacity(ws.relational_deletes.len());
            deletes.extend(0..ws.relational_deletes.len());
            deletes.sort_unstable_by(|a, b| {
                let left = &ws.relational_deletes[*a]; let right = &ws.relational_deletes[*b];
                (&left.0, left.1).cmp(&(&right.0, right.1))
            });
            let mut used = vec![false; ws.relational_deletes.len()];
            let mut paired = vec![None; ws.relational_inserts.len()];
            if deletes.capacity() != ws.relational_deletes.len() || used.capacity() != ws.relational_deletes.len()
                || paired.capacity() != ws.relational_inserts.len() { return Err(EngineMetadataJournal::identity_error()); }
            let published = self.tx_mgr.capture_published_read();
            let snapshot = published.snapshot;
            let _writer_read = self.enter_writer_read(super::writer_read::durable_root(published.handle)?, read_memory.clone());
            for (position, (table, inserted)) in ws.relational_inserts.iter().enumerate() {
                if !declarations.values().any(|declaration| declaration.table == *table) { continue; }
                for (sorted, candidate) in deletes.iter().copied().enumerate() {
                    let (deleted_table, row_id, _) = &ws.relational_deletes[candidate];
                    if deleted_table != table || used[candidate] { continue; }
                    if sorted != 0 {
                        let previous = &ws.relational_deletes[deletes[sorted - 1]];
                        if previous.0 == *deleted_table && previous.1 == *row_id { continue; }
                    }
                    // The old gather sorted unique deleted row ids for each
                    // table. Preserve that order while retaining one body only.
                    let mut credit = contextdb_core::read_memory::ReadCredit::new(read_memory.clone());
                    let Some(deleted) = self.committed_row_at_admitted(table, *row_id, snapshot,
                        &mut credit, read_memory.clone())? else { continue; };
                    if self.sync_pull_rows_share_identity_borrowed(table, inserted, &deleted)? {
                        used[candidate] = true; paired[position] = Some(candidate); break;
                    }
                }
            }
            let total_changes = ws.relational_inserts.len().saturating_add(ws.relational_deletes.len())
                .saturating_sub(used.iter().filter(|used| **used).count())
                .saturating_add(ws.adj_inserts.len()).saturating_add(ws.adj_deletes.len())
                .saturating_add(ws.vector_inserts.len()).saturating_add(ws.vector_deletes.len())
                .saturating_add(ws.vector_moves.len());
            let mut firing_rows = 0usize;
            for (position, (table, _)) in ws.relational_inserts.iter().enumerate() {
                let event = if paired[position].is_some() { TriggerEvent::Update } else { TriggerEvent::Insert };
                let mut fired = false;
                for declaration in declarations.values().filter(|declaration|
                    declaration.table == *table && declaration.on_events.contains(&event)) {
                    fired = true;
                    if declaration.including_sync { continue; }
                    entries.push(TriggerAuditEntry { trigger_name: crate::memory_accounting::preparation_copy_string(&declaration.name)?,
                        firing_tx, firing_lsn: lsn, depth: 1, cascade_row_count: 0, status: TriggerAuditStatus::Fired });
                }
                if fired { firing_rows += 1; }
            }
            let cascade_row_count = total_changes.saturating_sub(firing_rows) as u32;
            for entry in &mut entries { entry.cascade_row_count = cascade_row_count; }
        }
        for audit in pending {
            entries.push(TriggerAuditEntry { trigger_name: crate::memory_accounting::preparation_copy_string(&audit.trigger_name)?,
                firing_tx, firing_lsn: lsn, depth: audit.depth, cascade_row_count: audit.cascade_row_count,
                status: TriggerAuditStatus::Fired });
        }
        Ok(OriginalOrdinaryAudits { entries, _memory: frame.into_reservation() })
    }

    fn sync_pull_rows_share_identity_borrowed(&self, table: &str,
        inserted: &VersionedRow, deleted: &VersionedRow) -> Result<bool> {
        if inserted.row_id == deleted.row_id { return Ok(true); }
        let single = |name: &str| inserted.values.get(name).is_some_and(|value| deleted.values.get(name) == Some(value));
        let identity = self.with_owned_delivery_meta_if_present(table, |meta| {
            let mut declared = false;
            if let Some(name) = &meta.natural_key_column { declared = true; if single(name) { return Ok((true, declared)); } }
            for column in &meta.columns {
                if column.primary_key || column.unique { declared = true; if single(&column.name) { return Ok((true, declared)); } }
            }
            for columns in &meta.unique_constraints {
                declared = true;
                if !columns.is_empty() && columns.iter().all(|name| single(name)) { return Ok((true, declared)); }
            }
            Ok((false, declared))
        })?.unwrap_or((false, false));
        Ok(identity.0 || (!identity.1 && single("id")))
    }
}


#[derive(Default)]
pub(crate) struct PreparedPersistenceAudits {
    entries: Vec<(u64, TriggerAuditEntry)>,
    _memory: Option<OwnedMemoryReservation>,
}
impl std::ops::Deref for PreparedPersistenceAudits {
    type Target = [(u64, TriggerAuditEntry)];
    fn deref(&self) -> &Self::Target { &self.entries }
}
struct OriginalStagedAuditLink {
    node: Box<OriginalStagedAuditNode>,
    _control: OwnedMemoryReservation,
}
struct OriginalStagedAuditNode {
    lsn: Lsn,
    batch: PreparedPersistenceAudits,
    next: Option<OriginalStagedAuditLink>,
}
fn audit_dynamic_bytes(entry: &TriggerAuditEntry) -> Result<usize> {
    entry.trigger_name.len().checked_add(match &entry.status {
        TriggerAuditStatus::RolledBack { reason } => reason.len(), _ => 0,
    }).ok_or_else(EngineMetadataJournal::identity_error)
}
fn copy_original_audit_entry(entry: &TriggerAuditEntry) -> Result<TriggerAuditEntry> {
    Ok(TriggerAuditEntry { trigger_name: crate::memory_accounting::preparation_copy_string(&entry.trigger_name)?,
        firing_tx: entry.firing_tx, firing_lsn: entry.firing_lsn, depth: entry.depth,
        cascade_row_count: entry.cascade_row_count, status: match &entry.status {
            TriggerAuditStatus::Fired => TriggerAuditStatus::Fired,
            TriggerAuditStatus::DepthExceeded => TriggerAuditStatus::DepthExceeded,
            TriggerAuditStatus::RolledBack { reason } => TriggerAuditStatus::RolledBack {
                reason: crate::memory_accounting::preparation_copy_string(reason)?,
            },
        } })
}
impl TriggerState {
    fn take_original_staged_audits(&self, lsn: Lsn) -> Option<PreparedPersistenceAudits> {
        let removed = {
            let mut head = self.native_staged_audits.lock();
            let mut slot = &mut *head;
            loop {
                match slot.as_ref() {
                    Some(link) if link.node.lsn == lsn => {
                        let mut removed = slot.take().expect("addressed original audit stage");
                        *slot = removed.node.next.take();
                        break Some(removed);
                    }
                    Some(_) => { slot = &mut slot.as_mut().expect("live original audit stage").node.next; }
                    None => break None,
                }
            }
        }?;
        let (control, batch) = {
            let OriginalStagedAuditLink { node, _control: control } = removed;
            let OriginalStagedAuditNode { batch, .. } = *node;
            // The partially moved Box's allocation deallocates at this block
            // exit while its exact control moves to the enclosing owner.
            (control, batch)
        };
        drop(control);
        Some(batch)
    }
    pub(super) fn stage_original_persistence_audits(&self, original: &OwnedMemoryReservation,
        lsn: Lsn, entries: &[TriggerAuditEntry]) -> Result<()> {
        if entries.is_empty() { return Ok(()); }
        let control = crate::memory_accounting::reserve_preparation_from_original(original,
            std::alloc::Layout::new::<OriginalStagedAuditNode>().size(), "trigger", "audit_stage_control",
            "Raise MEMORY_LIMIT to stage trigger audits.")?;
        let bytes = entries.iter().try_fold(crate::custody::preparation::preparation_array_bytes::<(u64, TriggerAuditEntry)>(entries.len())?,
            |bytes, entry| bytes.checked_add(audit_dynamic_bytes(entry)?).ok_or_else(EngineMetadataJournal::identity_error))?;
        let memory = crate::memory_accounting::reserve_preparation_from_original(original, bytes,
            "trigger", "audit_stage_entries", "Raise MEMORY_LIMIT to stage trigger audits.")?;
        let mut indexed = Vec::with_capacity(entries.len());
        if indexed.capacity() != entries.len() { return Err(EngineMetadataJournal::identity_error()); }
        for entry in entries { indexed.push((0, copy_original_audit_entry(entry)?)); }
        for (index, _) in &mut indexed { *index = self.next_audit_index.fetch_add(1, Ordering::SeqCst); }
        let batch = PreparedPersistenceAudits { entries: indexed, _memory: Some(memory) };
        let mut link = OriginalStagedAuditLink { node: Box::new(OriginalStagedAuditNode { lsn, batch, next: None }), _control: control };
        let previous = self.take_original_staged_audits(lsn);
        drop(previous);
        let mut head = self.native_staged_audits.lock();
        link.node.next = head.take();
        *head = Some(link);
        Ok(())
    }
}


struct OriginalTriggerAuditRing {
    entries: Option<OriginalAuditHistoryLink>,
    len: usize,
}
struct OriginalAuditHistoryLink {
    node: Box<OriginalAuditHistoryNode>,
    _memory: OwnedMemoryReservation,
}
struct OriginalAuditHistoryNode {
    stamped_at: Wallclock,
    entry: TriggerAuditEntry,
    next: Option<OriginalAuditHistoryLink>,
}
impl Drop for OriginalAuditHistoryLink {
    fn drop(&mut self) {
        let mut next = self.node.next.take();
        while let Some(mut link) = next {
            next = link.node.next.take();
            drop(link);
        }
    }
}
pub(super) struct OriginalAuditMemoryPublication {
    import: Option<OriginalTriggerAuditRing>,
    incoming: Option<OriginalAuditHistoryLink>,
    history: Option<OriginalAuditHistoryLink>,
}
impl Database {
    pub(super) fn prepare_original_audit_memory(&self, original: &OwnedMemoryReservation,
        entries: &[TriggerAuditEntry]) -> Result<OriginalAuditMemoryPublication> {
        self.prepare_original_audit_memory_iter(original, entries.iter())
    }
    pub(super) fn prepare_original_audit_memory_iter<'a>(&self, original: &OwnedMemoryReservation,
        entries: impl DoubleEndedIterator<Item = &'a TriggerAuditEntry> + Clone) -> Result<OriginalAuditMemoryPublication> {
        let incoming = Self::prepare_original_audit_chain(original, entries.clone())?;
        let history = if self.persistence.is_none() { Self::prepare_original_audit_chain(original, entries)? } else { None };
        let import = if self.trigger.native_audit_ring.lock().is_none() {
            let legacy = self.trigger.audit_ring.lock();
            let entries = Self::prepare_original_audit_chain(original, legacy.iter().skip(legacy.len().saturating_sub(TRIGGER_AUDIT_RING_CAPACITY)))?;
            Some(OriginalTriggerAuditRing { entries, len: legacy.len().min(TRIGGER_AUDIT_RING_CAPACITY) })
        } else { None };
        Ok(OriginalAuditMemoryPublication { import, incoming, history })
    }
    fn prepare_original_audit_chain<'a>(original: &OwnedMemoryReservation,
        entries: impl DoubleEndedIterator<Item = &'a TriggerAuditEntry>) -> Result<Option<OriginalAuditHistoryLink>> {
        let mut next = None;
        for entry in entries.rev() {
            let bytes = std::alloc::Layout::new::<OriginalAuditHistoryNode>().size()
                .checked_add(audit_dynamic_bytes(entry)?).ok_or_else(EngineMetadataJournal::identity_error)?;
            let memory = crate::memory_accounting::reserve_preparation_from_original(original, bytes,
                "trigger", "audit_memory_entry", "Raise MEMORY_LIMIT to retain trigger audits.")?;
            let entry = copy_original_audit_entry(entry)?;
            next = Some(OriginalAuditHistoryLink { node: Box::new(OriginalAuditHistoryNode {
                stamped_at: Wallclock(0), entry, next,
            }), _memory: memory });
        }
        Ok(next)
    }
    pub(super) fn publish_original_audit_memory(&self, mut publication: OriginalAuditMemoryPublication) {
        let mut incoming_count = 0usize;
        let mut cursor = publication.incoming.as_ref();
        while let Some(link) = cursor { incoming_count += 1; cursor = link.node.next.as_ref(); }
        let evicted = {
            let mut target = self.trigger.native_audit_ring.lock();
            let ring = target.get_or_insert_with(|| publication.import.take()
                .unwrap_or(OriginalTriggerAuditRing { entries: None, len: 0 }));
            let mut tail = &mut ring.entries;
            while let Some(link) = tail { tail = &mut link.node.next; }
            *tail = publication.incoming.take();
            ring.len = ring.len.saturating_add(incoming_count);
            let mut evicted = None;
            while ring.len > TRIGGER_AUDIT_RING_CAPACITY {
                let mut front = ring.entries.take().expect("nonempty original audit ring");
                ring.entries = front.node.next.take();
                front.node.next = evicted.take();
                evicted = Some(front);
                ring.len -= 1;
            }
            evicted
        };
        // Only already-funded links move during publication. Nested callback
        // commits appended after preparation stay in the current ring. Evicted
        // nodes and any unused Legacy import retire outside registry locks.
        drop(evicted);
        let legacy = std::mem::take(&mut *self.trigger.audit_ring.lock());
        drop(legacy); drop(publication.import.take());
        if publication.history.is_some() {
            let stamped_at = Wallclock::now();
            let mut cursor = publication.history.as_mut();
            while let Some(link) = cursor { link.node.stamped_at = stamped_at; cursor = link.node.next.as_mut(); }
            let mut head = self.trigger.native_audit_history.lock();
            let mut tail = &mut *head;
            while let Some(link) = tail { tail = &mut link.node.next; }
            *tail = publication.history.take();
        }
    }
    pub(super) fn original_audit_ring_snapshot(&self) -> Option<Vec<TriggerAuditEntry>> {
        let native = self.trigger.native_audit_ring.lock();
        let ring = native.as_ref()?;
        let mut entries = Vec::with_capacity(ring.len);
        let mut cursor = ring.entries.as_ref();
        while let Some(link) = cursor { entries.push(link.node.entry.clone()); cursor = link.node.next.as_ref(); }
        Some(entries)
    }
    pub(super) fn append_original_audit_history_to_read(&self,
        entries: &mut Vec<TriggerAuditEntry>, credit: &mut contextdb_core::read_memory::ReadCredit) -> Result<()> {
        let history = self.trigger.native_audit_history.lock();
        let mut cursor = history.as_ref();
        while let Some(link) = cursor {
            credit.try_grow_for(std::mem::size_of::<TriggerAuditEntry>().saturating_add(audit_dynamic_bytes(&link.node.entry)?).saturating_add(96),
                "reader", "trigger_audit_history", "Reduce the selected audit history or raise the declared read memory limit.")?;
            entries.push(copy_original_audit_entry(&link.node.entry)?);
            cursor = link.node.next.as_ref();
        }
        Ok(())
    }

}


pub(super) struct OriginalPreparedTriggerPublication {
    pub(super) publication: PreparedTriggerPublication,
    standing: OwnedMemoryReservation,
    config: OwnedMemoryReservation,
    callback_map_bytes: usize,
}
impl PreparedTriggerPublication {
    pub(super) fn empty_placeholder() -> Self {
        Self { declarations: BTreeMap::new(), callbacks: HashMap::new(), active_trigger_names: Vec::new(),
            ready: true, config_values: Vec::new() }
    }
}
impl Database {
    pub(super) fn register_original_trigger_callback<F>(
        &self, host: &EngineDatabaseTransactions, original: &OwnedMemoryReservation,
        name: &str, callback: F,
    ) -> Result<()>
    where F: Fn(&Database, &TriggerContext) -> Result<()> + Send + Sync + 'static {
        crate::memory_accounting::validate_owned_original(original, original.accountant(), &host.funder)?;
        let frame = crate::persistence::OriginalPersistenceFrame::new(original)?;
        let retired = {
            let declarations = self.trigger.declarations.lock();
            if !declarations.contains_key(name) {
                return Err(Error::TriggerNotDeclared { trigger_name: name.to_owned() });
            }
            let mut callbacks = self.trigger.callbacks.write();
            if callbacks.contains_key(name) {
                return Err(Error::TriggerAlreadyRegistered { trigger_name: name.to_owned() });
            }
            let count = callbacks.len().checked_add(1).ok_or_else(EngineMetadataJournal::identity_error)?;
            frame.map::<String, TriggerCallback>(count)?;
            frame.reserve(name.len())?;
            for key in callbacks.keys() { frame.reserve(key.len())?; }
            let mut next = HashMap::with_capacity(count);
            for (key, callback) in callbacks.iter() {
                next.insert(crate::memory_accounting::preparation_copy_string(key)?, callback.clone());
            }
            let key = crate::memory_accounting::preparation_copy_string(name)?;
            let new_bytes = crate::memory_accounting::preparation_native_map_bytes::<String, TriggerCallback>(count)?
                .checked_add(name.len()).and_then(|n| n.checked_add(callbacks.keys().map(String::len).sum::<usize>()))
                .ok_or_else(EngineMetadataJournal::identity_error)?;
            let control = contextdb_tx::SourceCharge::admit_funded(host.funder.clone(),
                contextdb_tx::staged_source::source_arc_layout::<F>().size())?;
            let mut publication_original = self.trigger.native_publication_memory.lock();
            let mut map_original = self.trigger.native_callback_map_memory.lock();
            let old_bytes = self.trigger.native_callback_map_bytes.load(Ordering::SeqCst);
            // Detach the old physical map's exact already-held debit; do not
            // refund it until that old allocation has been destroyed.
            let old_original = if map_original.is_some() { map_original.take() }
                else if let Some(publication) = publication_original.as_mut() {
                    Some(crate::memory_accounting::detach_preparation_original(publication, old_bytes)?)
                } else if old_bytes == 0 { None }
                else { return Err(EngineMetadataJournal::identity_error()); };
            // No fallible work follows moving the opaque host callback. Its
            // destructor can therefore never run under these guards on refusal.
            next.insert(key, TriggerCallback { body: Arc::new(callback), original: Some(control) });
            let old = std::mem::replace(&mut *callbacks, next);
            *map_original = Some(frame.into_reservation());
            self.trigger.native_callback_map_bytes.store(new_bytes, Ordering::SeqCst);
            RetiredOriginalCallbackMap { callbacks: old, _original: old_original }
        };
        drop(retired);
        Ok(())
    }

    pub(super) fn prepare_original_trigger_publication(&self, original: &OwnedMemoryReservation,
        source: &BTreeMap<String, TriggerDeclaration>) -> Result<OriginalPreparedTriggerPublication> {
        let standing = crate::persistence::OriginalPersistenceFrame::new(original)?;
        standing.tree::<String, TriggerDeclaration>(source.len())?;
        for (key, declaration) in source {
            standing.reserve(key.len().checked_add(declaration.name.len()).and_then(|n| n.checked_add(declaration.table.len()))
                .ok_or_else(EngineMetadataJournal::identity_error)?)?;
            standing.array::<TriggerEvent>(declaration.on_events.len())?;
        }
        let mut declarations = BTreeMap::new();
        for (key, declaration) in source {
            let mut on_events = Vec::with_capacity(declaration.on_events.len());
            if on_events.capacity() != declaration.on_events.len() { return Err(EngineMetadataJournal::identity_error()); }
            on_events.extend_from_slice(&declaration.on_events);
            declarations.insert(crate::memory_accounting::preparation_copy_string(key)?, TriggerDeclaration {
                name: crate::memory_accounting::preparation_copy_string(&declaration.name)?,
                table: crate::memory_accounting::preparation_copy_string(&declaration.table)?,
                on_events, including_sync: declaration.including_sync,
            });
        }
        let callbacks = {
            let source = self.trigger.callbacks.read();
            let count = source.keys().filter(|name| declarations.contains_key(*name)).count();
            standing.map::<String, TriggerCallback>(count)?;
            for name in source.keys().filter(|name| declarations.contains_key(*name)) { standing.reserve(name.len())?; }
            let mut callbacks = HashMap::with_capacity(count);
            for (name, callback) in source.iter().filter(|(name, _)| declarations.contains_key(*name)) {
                callbacks.insert(crate::memory_accounting::preparation_copy_string(name)?, callback.clone());
            }
            callbacks
        };
        let active_trigger_names = {
            let source = self.trigger.active_trigger_names.lock();
            let count = source.iter().filter(|name| declarations.contains_key(*name)).count();
            standing.array::<String>(count)?;
            for name in source.iter().filter(|name| declarations.contains_key(*name)) { standing.reserve(name.len())?; }
            let mut names = Vec::with_capacity(count);
            if names.capacity() != count { return Err(EngineMetadataJournal::identity_error()); }
            for name in source.iter().filter(|name| declarations.contains_key(*name)) {
                names.push(crate::memory_accounting::preparation_copy_string(name)?);
            }
            names
        };
        let adds_trigger = {
            let current = self.trigger.declarations.lock();
            declarations.keys().any(|name| !current.contains_key(name))
        };
        let ready = declarations.is_empty() || (self.trigger.ready.load(Ordering::SeqCst) && !adds_trigger);
        let config = crate::persistence::OriginalPersistenceFrame::new(original)?;
        config.array::<&TriggerDeclaration>(declarations.len())?;
        config.tree::<&str, ()>(declarations.values().filter(|declaration| declaration.including_sync).count())?;
        let values = declarations.values().collect::<Vec<_>>();
        let including_sync = declarations.values().filter(|declaration| declaration.including_sync)
            .map(|declaration| declaration.name.as_str()).collect::<BTreeSet<_>>();
        config.array::<(String, Vec<u8>)>(2)?;
        config.reserve(TRIGGER_DECLARATIONS_CONFIG_KEY.len().saturating_add(TRIGGER_INCLUDING_SYNC_CONFIG_KEY.len()))?;
        let mut config_values = Vec::with_capacity(2);
        if config_values.capacity() != 2 { return Err(EngineMetadataJournal::identity_error()); }
        config_values.push((crate::memory_accounting::preparation_copy_string(TRIGGER_DECLARATIONS_CONFIG_KEY)?, config.encode(&values)?));
        config_values.push((crate::memory_accounting::preparation_copy_string(TRIGGER_INCLUDING_SYNC_CONFIG_KEY)?, config.encode(&including_sync)?));
        drop(values); drop(including_sync);
        let callback_map_bytes = crate::memory_accounting::preparation_native_map_bytes::<String, TriggerCallback>(callbacks.len())?
            .checked_add(callbacks.keys().map(String::len).sum::<usize>())
            .ok_or_else(EngineMetadataJournal::identity_error)?;
        Ok(OriginalPreparedTriggerPublication { publication: PreparedTriggerPublication {
            declarations, callbacks, active_trigger_names, ready, config_values,
        }, standing: standing.into_reservation(), config: config.into_reservation(), callback_map_bytes })
    }
    pub(super) fn publish_original_trigger_publication(&self, publication: OriginalPreparedTriggerPublication) {
        let OriginalPreparedTriggerPublication { publication, standing, config, callback_map_bytes } = publication;
        {
            let mut native = self.trigger.native_callbacks.lock();
            let mut cursor = native.as_mut();
            while let Some(link) = cursor {
                link.node.visible_name &= publication.declarations.contains_key(&link.node.name);
                cursor = link.node.next.as_mut();
            }
        }
        let retired = {
            let mut declarations = self.trigger.declarations.lock();
            let mut callbacks = self.trigger.callbacks.write();
            let mut names = self.trigger.active_trigger_names.lock();
            let mut original = self.trigger.native_publication_memory.lock();
            let mut callback_original = self.trigger.native_callback_map_memory.lock();
            self.trigger.native_callback_map_bytes.store(callback_map_bytes, Ordering::SeqCst);
            RetiredOriginalTriggerState {
                declarations: std::mem::replace(&mut *declarations, publication.declarations),
                callbacks: std::mem::replace(&mut *callbacks, publication.callbacks),
                names: std::mem::replace(&mut *names, publication.active_trigger_names),
                _callback_original: callback_original.take(),
                _original: original.replace(standing),
            }
        };
        self.trigger.ready.store(publication.ready, Ordering::SeqCst);
        // Native originals are installed with their targets before any old
        // callback destructor can unwind. The retired carrier frees every old
        // map/name allocation before its final original, outside all locks.
        drop(retired);
        drop(publication.config_values);
        drop(config);
    }
}


pub(super) struct OriginalTriggerProjection {
    pub(super) declarations: BTreeMap<String, TriggerDeclaration>,
    _memory: OwnedMemoryReservation,
}
impl Database {
    pub(super) fn prepare_original_trigger_projection(&self, original: &OwnedMemoryReservation,
        ddl: &[DdlChange]) -> Result<OriginalTriggerProjection> {
        let frame = crate::persistence::OriginalPersistenceFrame::new(original)?;
        let mut projected = {
            let current = self.trigger.declarations.lock();
            frame.tree::<String, TriggerDeclaration>(current.len().checked_add(ddl.len())
                .ok_or_else(EngineMetadataJournal::identity_error)?)?;
            for (name, declaration) in current.iter() { frame.reserve(name.len())?; frame.record(declaration)?; }
            current.clone()
        };
        for (position, change) in ddl.iter().enumerate() {
            match change {
                change @ (DdlChange::CreateTrigger { name, table, on_events }
                    | DdlChange::CreateTriggerIncludingSync { name, table, on_events }) => {
                    let exists = self.with_owned_delivery_meta_if_present(table, |_| Ok(()))?.is_some();
                    if !exists && !ddl.iter().skip(position + 1).any(|later| match later {
                        DdlChange::DropTrigger { name: removed } => removed == name,
                        DdlChange::DropTable { name: removed } => removed == table,
                        _ => false,
                    }) { return Err(Error::TableNotFound(table.clone())); }
                    frame.reserve(name.len().checked_mul(2).and_then(|n| n.checked_add(table.len()))
                        .ok_or_else(EngineMetadataJournal::identity_error)?)?;
                    frame.array::<TriggerEvent>(on_events.len())?;
                    for event in on_events { frame.reserve(event.len())?; }
                    let events = on_events.iter().map(|event| TriggerEvent::from_ddl_str(event)).collect::<Result<Vec<_>>>()?;
                    let incoming = TriggerDeclaration { name: crate::memory_accounting::preparation_copy_string(name)?,
                        table: crate::memory_accounting::preparation_copy_string(table)?, on_events: events,
                        including_sync: matches!(change, DdlChange::CreateTriggerIncludingSync { .. }) };
                    if let Some(existing) = projected.get(name) {
                        if existing != &incoming { return Err(Error::Other((format!("trigger already exists with a different definition: {name}")).into())); }
                    } else { projected.insert(crate::memory_accounting::preparation_copy_string(name)?, incoming); }
                }
                DdlChange::DropTrigger { name } => { projected.remove(name); }
                // Keep the existing ordinary trigger-batch vocabulary. The
                // chronological schema projector owns table-drop cascades.
                _ => (),
            }
        }
        Ok(OriginalTriggerProjection { declarations: projected, _memory: frame.into_reservation() })
    }

    fn apply_original_trigger_ddl_batch(&self, ddl: &[DdlChange],
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>) -> Result<()> {
        self.with_preparation_original(|original| {
            self.allocate_ddl_lsn_maybe(|lsn| {
                let published = self.tx_mgr.capture_published_read();
                let _writer_read = self.enter_writer_read(super::writer_read::durable_root(published.handle)?, read_memory.clone());
                let projection = self.prepare_original_trigger_projection(original, ddl)?;
                if projection.declarations == *self.trigger.declarations.lock() && !Self::trigger_ddl_has_transient_lifecycle(ddl) {
                    return Ok(None);
                }
                let publication = self.prepare_original_trigger_publication(original, &projection.declarations)?;
                let frame = crate::persistence::OriginalPersistenceFrame::new(original)?;
                frame.array::<(String, Vec<u8>)>(ddl.len())?;
                for change in ddl {
                    frame.record(change)?;
                    if let Some(table) = Self::ddl_affected_table(change) {
                        self.with_owned_delivery_meta_if_present(table, |meta| frame.record(meta))?;
                    }
                }
                let provenance = self.ddl_generation_sidecar_values(lsn, ddl, 0, read_memory.clone())?;
                if let Some(persistence) = &self.persistence {
                    persistence.flush_original_encoded_config_and_ddl(original,
                        publication.publication.config_values.iter().chain(&provenance)
                            .map(|(key, bytes)| (key.as_str(), bytes.as_slice())), lsn, ddl)?;
                } else {
                    // Public memory constructors remain Legacy. Its future
                    // owning history/mirror selection needs standing custody.
                    self.record_ddl_generation_sidecars(lsn, ddl, read_memory.clone())?;
                }
                self.publish_original_trigger_publication(publication);
                if self.persistence.is_none() { self.ddl_log.write().extend(ddl.iter().cloned().map(|change| (lsn, change))); }
                Ok(Some(()))
            })?;
            Ok(())
        })
    }
}


pub(super) struct OriginalTriggerDdlStage {
    lsn: Lsn,
    identity: contextdb_tx::manager::EngineTerminalIdentity,
    publication: OriginalPreparedTriggerPublication,
    pub(crate) persistence: TriggerPersistenceCommit,
    next: Mutex<Option<event_bus::OwnedEventShared<OriginalTriggerDdlStage>>>,
    _memory: OwnedMemoryReservation,
}
pub(super) struct OriginalTriggerDdlRegistration {
    state: Arc<TriggerState>,
    installed: event_bus::OwnedEventShared<OriginalTriggerDdlStage>,
    armed: bool,
    _memory: OwnedMemoryReservation,
}
impl OriginalTriggerDdlRegistration {
    fn take(mut self) -> Option<event_bus::OwnedEventShared<OriginalTriggerDdlStage>> {
        let removed = self.state.take_original_trigger_stage(&self.installed);
        self.armed = false;
        removed
    }
}
impl Drop for OriginalTriggerDdlRegistration {
    fn drop(&mut self) {
        if self.armed { let retired = self.state.take_original_trigger_stage(&self.installed); drop(retired); }
    }
}
impl TriggerState {
    fn pin_original_trigger_stage(&self, lsn: Lsn) -> Option<event_bus::OwnedEventShared<OriginalTriggerDdlStage>> {
        let head = self.native_staged_ddl.lock();
        let mut cursor = head.clone();
        while let Some(stage) = cursor {
            if stage.lsn == lsn { return Some(stage); }
            cursor = stage.next.lock().clone();
        }
        None
    }
    fn take_original_trigger_stage(&self, expected: &event_bus::OwnedEventShared<OriginalTriggerDdlStage>)
        -> Option<event_bus::OwnedEventShared<OriginalTriggerDdlStage>> {
        let mut head = self.native_staged_ddl.lock();
        let mut previous: Option<event_bus::OwnedEventShared<OriginalTriggerDdlStage>> = None;
        let mut cursor = head.clone();
        while let Some(stage) = cursor {
            if std::ptr::eq(stage.get(), expected.get()) {
                let next = stage.next.lock().take();
                let removed = if let Some(previous) = previous { std::mem::replace(&mut *previous.next.lock(), next) }
                    else { std::mem::replace(&mut *head, next) };
                drop(head);
                return removed;
            }
            cursor = stage.next.lock().clone();
            previous = Some(stage);
        }
        None
    }
    pub(crate) fn original_trigger_persistence_commit(&self, lsn: Lsn,
        identity: contextdb_tx::manager::EngineTerminalIdentity) -> Result<Option<OriginalTriggerPersistencePin>> {
        let stage = self.pin_original_trigger_stage(lsn);
        if stage.as_ref().is_some_and(|stage| stage.identity != identity) {
            return Err(EngineMetadataJournal::identity_error());
        }
        Ok(stage.map(|stage| OriginalTriggerPersistencePin { stage }))
    }
}
pub(crate) struct OriginalTriggerPersistencePin {
    stage: event_bus::OwnedEventShared<OriginalTriggerDdlStage>,
}
impl OriginalTriggerPersistencePin {
    pub(crate) fn persistence(&self) -> &TriggerPersistenceCommit { &self.stage.persistence }
}
impl Database {
    pub(super) fn stage_original_trigger_ddl(&self, host: &EngineDatabaseTransactions,
        original: &OwnedMemoryReservation, identity: contextdb_tx::manager::EngineTerminalIdentity,
        lsn: Lsn, ddl: &[DdlChange], start_ordinal: u32,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>) -> Result<Option<OriginalTriggerDdlRegistration>> {
        if ddl.is_empty() { return Ok(None); }
        let projection = self.prepare_original_trigger_projection(original, ddl)?;
        if projection.declarations == *self.trigger.declarations.lock() && !Self::trigger_ddl_has_transient_lifecycle(ddl) { return Ok(None); }
        let publication = self.prepare_original_trigger_publication(original, &projection.declarations)?;
        let frame = crate::persistence::OriginalPersistenceFrame::new(original)?;
        frame.array::<(String, Vec<u8>)>(ddl.len())?;
        frame.array::<DdlChange>(ddl.len())?;
        frame.array::<DurableDdlGenerationSidecar>(ddl.len())?;
        for change in ddl {
            frame.record(change)?;
            if let Some(table) = Self::ddl_affected_table(change) {
                self.with_owned_delivery_meta_if_present(table, |meta| frame.record(meta))?;
            }
        }
        for (key, bytes) in &publication.publication.config_values { frame.reserve(key.len().saturating_add(bytes.len()))?; }
        let generation_sidecars = self.ddl_generation_sidecars(ddl, read_memory.clone())?;
        let provenance = self.ddl_generation_sidecar_values(lsn, ddl, start_ordinal, read_memory)?;
        let count = publication.publication.config_values.len().checked_add(provenance.len())
            .ok_or_else(EngineMetadataJournal::identity_error)?;
        frame.array::<(String, Vec<u8>)>(count)?;
        let mut config_values = Vec::with_capacity(count);
        if config_values.capacity() != count { return Err(EngineMetadataJournal::identity_error()); }
        config_values.extend(publication.publication.config_values.iter().cloned());
        config_values.extend(provenance);
        let persistence = TriggerPersistenceCommit { config_values, ddl: ddl.to_vec(), generation_sidecars, start_ordinal };
        let stage = event_bus::OwnedEventShared::try_new(OriginalTriggerDdlStage { lsn, identity,
            publication, persistence, next: Mutex::new(None), _memory: frame.into_reservation() }, original, &host.funder)?;
        let control = crate::memory_accounting::reserve_preparation_from_original(original,
            std::mem::size_of::<OriginalTriggerDdlRegistration>(), "trigger", "ddl_stage_registration",
            "Raise MEMORY_LIMIT to retain the trigger DDL stage.")?;
        let mut registration = OriginalTriggerDdlRegistration { state: self.trigger.clone(), installed: stage.clone(), armed: false, _memory: control };
        let mut head = self.trigger.native_staged_ddl.lock();
        let mut cursor = head.clone();
        while let Some(existing) = cursor {
            if existing.lsn == lsn { return Err(EngineMetadataJournal::identity_error()); }
            cursor = existing.next.lock().clone();
        }
        *stage.next.lock() = head.take();
        *head = Some(stage);
        registration.armed = true;
        Ok(Some(registration))
    }
    pub(super) fn publish_original_trigger_ddl(&self, registration: OriginalTriggerDdlRegistration) {
        let stage = registration.take().expect("committed original trigger stage retained");
        let stage = match stage.try_into_inner() {
            Ok(stage) => stage,
            Err(_) => panic!("committed trigger stage borrowed after immutable flush"),
        };
        let OriginalTriggerDdlStage { lsn, publication, persistence, next, _memory, .. } = stage;
        self.publish_in_memory_ddl_generation_sidecars(lsn, persistence.start_ordinal, &persistence.generation_sidecars);
        self.publish_original_trigger_publication(publication);
        if self.persistence.is_none() { self.ddl_log.write().extend(persistence.ddl.iter().cloned().map(|ddl| (lsn, ddl))); }
        drop(persistence); drop(next); drop(_memory);
    }
}

impl TriggerState {
    pub(crate) fn prune_original_audit_history(&self, cutoff: u64) -> u64 {
        let mut removed_count = 0;
        loop {
            let removed = {
                let mut head = self.native_audit_history.lock();
                let mut slot = &mut *head;
                loop {
                    match slot.as_ref() {
                        Some(link) if link.node.stamped_at.0 < cutoff => {
                            let mut removed = slot.take().expect("expired original audit history entry");
                            *slot = removed.node.next.take(); break Some(removed);
                        }
                        Some(_) => { slot = &mut slot.as_mut().expect("live original history entry").node.next; }
                        None => break None,
                    }
                }
            };
            if removed.is_none() { return removed_count; }
            drop(removed); removed_count += 1;
        }
    }
}


#[allow(dead_code)]
struct RetiredOriginalTriggerState {
    declarations: BTreeMap<String, TriggerDeclaration>,
    callbacks: HashMap<String, TriggerCallback>,
    names: Vec<String>,
    _callback_original: Option<OwnedMemoryReservation>,
    _original: Option<OwnedMemoryReservation>,
}

struct RetiredOriginalCallbackMap {
    callbacks: HashMap<String, TriggerCallback>,
    _original: Option<OwnedMemoryReservation>,
}
