use super::*;

const TRIGGER_DECLARATIONS_CONFIG_KEY: &str = "__trigger_declarations";
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
    pub(super) volatile_audit_history: Mutex<Vec<TriggerAuditEntry>>,
    staged_persistence_audits: Mutex<HashMap<Lsn, Vec<(u64, TriggerAuditEntry)>>>,
    staged_ddl_for_persistence: Mutex<HashMap<Lsn, StagedTriggerDdlCommit>>,
    callback_owner_threads: Mutex<HashMap<thread::ThreadId, usize>>,
    pub(super) next_audit_index: AtomicU64,
    pub(super) ready: AtomicBool,
}

#[derive(Debug, Clone)]
pub(crate) struct TriggerPersistenceCommit {
    pub(crate) config_values: Vec<(&'static str, Vec<u8>)>,
    pub(crate) ddl: Vec<DdlChange>,
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

#[derive(Debug)]
pub(super) struct TriggerDispatchFailure {
    pub error: Error,
}

struct TriggerDispatchRun {
    processed: HashSet<(String, RowId, String)>,
    pending: Vec<PendingTriggerAudit>,
}

struct TriggerFiring {
    event: TriggerEvent,
    row: VersionedRow,
}

struct TriggerCallbackThreadGuard<'a> {
    trigger: &'a TriggerState,
    owner: thread::ThreadId,
}

fn global_trigger_callback_owner_threads() -> &'static Mutex<HashMap<thread::ThreadId, usize>> {
    static OWNERS: OnceLock<Mutex<HashMap<thread::ThreadId, usize>>> = OnceLock::new();
    OWNERS.get_or_init(|| Mutex::new(HashMap::new()))
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
            next_audit_index: AtomicU64::new(0),
            ready: AtomicBool::new(true),
        }
    }

    pub(super) fn callback_active_on_other_thread(&self, target_owner: thread::ThreadId) -> bool {
        let current = thread::current().id();
        let owners = self.callback_owner_threads.lock();
        if !owners.is_empty() && !owners.contains_key(&current) {
            return true;
        }
        drop(owners);
        let global_owners = global_trigger_callback_owner_threads().lock();
        !global_owners.is_empty()
            && !global_owners.contains_key(&current)
            && current != target_owner
    }

    fn enter_callback_thread_scope(&self) -> TriggerCallbackThreadGuard<'_> {
        let owner = thread::current().id();
        *global_trigger_callback_owner_threads()
            .lock()
            .entry(owner)
            .or_default() += 1;
        *self.callback_owner_threads.lock().entry(owner).or_default() += 1;
        TriggerCallbackThreadGuard {
            trigger: self,
            owner,
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

impl Drop for TriggerCallbackThreadGuard<'_> {
    fn drop(&mut self) {
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
    }
}

impl Database {
    pub(super) fn load_trigger_state_from_persistence(&self) -> Result<()> {
        let Some(persistence) = self.persistence.as_ref() else {
            return Ok(());
        };
        let declarations = persistence
            .load_config_value::<Vec<TriggerDeclaration>>(TRIGGER_DECLARATIONS_CONFIG_KEY)?
            .unwrap_or_default();
        self.replace_trigger_declarations(declarations);

        let (ring_history, next_index) =
            persistence.load_trigger_audit_state(TRIGGER_AUDIT_RING_CAPACITY)?;
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

    fn encoded_trigger_config_values(
        declarations: &BTreeMap<String, TriggerDeclaration>,
    ) -> Result<Vec<(&'static str, Vec<u8>)>> {
        let values = declarations.values().cloned().collect::<Vec<_>>();
        Ok(vec![(
            TRIGGER_DECLARATIONS_CONFIG_KEY,
            RedbPersistence::encode_config_value(&values)?,
        )])
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
            .map(|declaration| DdlChange::CreateTrigger {
                name: declaration.name.clone(),
                table: declaration.table.clone(),
                on_events: declaration
                    .on_events
                    .iter()
                    .map(|event| event.as_ddl_str().to_string())
                    .collect(),
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

    pub(super) fn apply_trigger_ddl_from_user(&self, ddl: DdlChange) -> Result<()> {
        self.require_admin_trigger_ddl(match &ddl {
            DdlChange::CreateTrigger { .. } => "CREATE TRIGGER",
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
            DdlChange::CreateTrigger {
                name,
                table,
                on_events,
            } => {
                if !projected_tables.contains_key(table) {
                    return Err(Error::TableNotFound(table.clone()));
                }
                let events = on_events
                    .iter()
                    .map(|event| TriggerEvent::from_ddl_str(event))
                    .collect::<Result<Vec<_>>>()?;
                let incoming = TriggerDeclaration {
                    name: name.clone(),
                    table: table.clone(),
                    on_events: events,
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
            let DdlChange::CreateTrigger { name, table, .. } = change else {
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
                DdlChange::CreateTrigger {
                    name,
                    table,
                    on_events,
                } if self.table_meta(table).is_none() => {
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
                    let incoming = TriggerDeclaration {
                        name: name.clone(),
                        table: table.clone(),
                        on_events: events,
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
                persistence.flush_encoded_config_values_and_append_ddl_log(
                    Self::encoded_trigger_config_values(&projected)?,
                    lsn,
                    &ddl,
                )?;
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
            DdlChange::CreateTrigger {
                name,
                table,
                on_events,
            } => {
                if self.table_meta(table).is_none() {
                    return Err(Error::TableNotFound(table.clone()));
                }
                let events = on_events
                    .iter()
                    .map(|event| TriggerEvent::from_ddl_str(event))
                    .collect::<Result<Vec<_>>>()?;
                if let Some(existing) = declarations.get(name) {
                    let incoming = TriggerDeclaration {
                        name: name.clone(),
                        table: table.clone(),
                        on_events: events,
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
        Err(Error::EngineNotInitialized {
            operation: format!("{operation} on trigger-attached table {table}"),
        })
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

    fn triggers_for_table_event(
        &self,
        table: &str,
        event: TriggerEvent,
    ) -> Vec<TriggerDeclaration> {
        self.triggers_for_table_event_from(None, table, event)
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

    pub(super) fn dispatch_triggers_for_tx(
        &self,
        tx: TxId,
    ) -> std::result::Result<Vec<PendingTriggerAudit>, TriggerDispatchFailure> {
        if self.trigger.declarations.lock().is_empty() {
            return Ok(Vec::new());
        }
        let mut run = TriggerDispatchRun {
            processed: HashSet::new(),
            pending: Vec::new(),
        };
        let initial_len = self
            .tx_mgr
            .with_write_set(tx, |ws| ws.relational_inserts.len())
            .map_err(|error| TriggerDispatchFailure { error })?;
        if let Err(error) = self.dispatch_trigger_range(tx, 0, initial_len, 1, &mut run) {
            let reason = error.to_string();
            if let Err(audit_error) =
                self.append_rolled_back_trigger_audits(&run.pending, tx, &reason)
            {
                return Err(TriggerDispatchFailure { error: audit_error });
            }
            return Err(TriggerDispatchFailure { error });
        }
        Ok(run.pending)
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
            let conditional_noop_count =
                self.revalidate_conditional_updates(ws, snapshot, &conditional_update_guards)?;
            self.rewrite_commit_time_upserts_for_write_set(ws, snapshot, &upsert_intents)?;
            ws.canonicalize_final_state();
            Ok(CommitValidationOutcome {
                conditional_noop_count,
            })
        })?
    }

    fn dispatch_trigger_range(
        &self,
        tx: TxId,
        start: usize,
        end: usize,
        depth: u32,
        run: &mut TriggerDispatchRun,
    ) -> Result<()> {
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
            if !self.latest_staged_trigger_row(tx, index, &table, row.row_id)? {
                index += 1;
                continue;
            }
            let event = self.trigger_event_for_staged_row(tx, &table, row.row_id)?;
            for declaration in self.triggers_for_table_event(&table, event) {
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
                )?;
            }
            index += 1;
        }
        Ok(())
    }

    fn latest_staged_trigger_row(
        &self,
        tx: TxId,
        index: usize,
        table: &str,
        row_id: RowId,
    ) -> Result<bool> {
        self.tx_mgr.with_write_set(tx, |ws| {
            !ws.relational_inserts
                .iter()
                .enumerate()
                .skip(index.saturating_add(1))
                .any(|(_, (insert_table, row))| insert_table == table && row.row_id == row_id)
        })
    }

    fn trigger_event_for_staged_row(
        &self,
        tx: TxId,
        table: &str,
        row_id: RowId,
    ) -> Result<TriggerEvent> {
        let committed_row_exists = self
            .relational_store
            .row_by_id(table, row_id, SnapshotId::from_raw_wire(u64::MAX))
            .is_some();
        let updated = self.tx_mgr.with_write_set(tx, |ws| {
            committed_row_exists
                && ws
                    .relational_deletes
                    .iter()
                    .any(|(delete_table, deleted_row_id, _)| {
                        delete_table == table && *deleted_row_id == row_id
                    })
        })?;
        Ok(if updated {
            TriggerEvent::Update
        } else {
            TriggerEvent::Insert
        })
    }

    fn fire_trigger(
        &self,
        tx: TxId,
        declaration: TriggerDeclaration,
        firing: TriggerFiring,
        depth: u32,
        run: &mut TriggerDispatchRun,
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
        let ctx = TriggerContext {
            trigger_name: declaration.name.clone(),
            table: declaration.table.clone(),
            event: firing.event,
            tx,
            depth,
            row_values: firing.row.values.clone(),
        };
        let callback_result = self.run_trigger_callback(tx, &ctx, callback);
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

        if let Err(error) = self.prepare_active_trigger_write_set_for_dispatch(tx) {
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
            )?;
        }
        Ok(())
    }

    fn run_trigger_callback(
        &self,
        tx: TxId,
        ctx: &TriggerContext,
        callback: TriggerCallback,
    ) -> Result<()> {
        let _callback_thread = self.trigger.enter_callback_thread_scope();
        let this_db = self as *const Self as usize;
        let result = TRIGGER_CALLBACK_TX.with(|tx_slot| {
            let prior_tx = tx_slot.replace(Some(tx));
            let prior_db = TRIGGER_CALLBACK_DB.with(|db_slot| db_slot.replace(Some(this_db)));
            let prior_active = TRIGGER_CALLBACK_ACTIVE.with(|active| active.replace(true));
            let result = catch_unwind(AssertUnwindSafe(|| callback(self, ctx)));
            TRIGGER_CALLBACK_ACTIVE.with(|active| active.set(prior_active));
            TRIGGER_CALLBACK_DB.with(|db_slot| db_slot.set(prior_db));
            tx_slot.set(prior_tx);
            result
        });
        match result {
            Ok(result) => result,
            Err(payload) => Err(Error::TriggerCallbackFailed {
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
        let deleted_rows = self.sync_pull_deleted_rows_by_table(ws)?;
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
            for declaration in declarations {
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
            } = change
            {
                let events = on_events
                    .iter()
                    .map(|event| TriggerEvent::from_ddl_str(event))
                    .collect::<Result<Vec<_>>>()?;
                projected.insert(
                    name.clone(),
                    TriggerDeclaration {
                        name: name.clone(),
                        table: table.clone(),
                        on_events: events,
                    },
                );
            }
        }
        Ok(projected)
    }

    fn sync_pull_deleted_rows_by_table(
        &self,
        ws: &WriteSet,
    ) -> Result<HashMap<String, Vec<VersionedRow>>> {
        let deleted_ids_by_table = ws.relational_deletes.iter().fold(
            HashMap::<String, HashSet<RowId>>::new(),
            |mut by_table, (table, row_id, _)| {
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

    pub(super) fn stage_trigger_ddl_for_commit(&self, lsn: Lsn, ddl: &[DdlChange]) -> Result<()> {
        if ddl.is_empty() {
            return Ok(());
        }
        let current = self.trigger.declarations.lock().clone();
        let projected = self.apply_trigger_ddl_batch_projection(ddl)?;
        let transient_lifecycle = Self::trigger_ddl_has_transient_lifecycle(ddl);
        if current == projected && !transient_lifecycle {
            return Ok(());
        }
        let persistence = TriggerPersistenceCommit {
            config_values: Self::encoded_trigger_config_values(&projected)?,
            ddl: ddl.to_vec(),
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
        {
            let mut ring = self.trigger.audit_ring.lock();
            if ring.len() == TRIGGER_AUDIT_RING_CAPACITY {
                ring.pop_front();
            }
            ring.push_back(entry.clone());
        }
        {
            if self.persistence.is_none() {
                self.trigger.volatile_audit_history.lock().push(entry);
            }
        }
    }

    pub(super) fn trigger_active_on_this_handle(&self) -> bool {
        let this_db = self as *const Self as usize;
        TRIGGER_CALLBACK_ACTIVE.with(|active| active.get())
            && TRIGGER_CALLBACK_DB.with(|db| db.get()) == Some(this_db)
    }

    pub(super) fn active_trigger_tx_for_this_handle(&self) -> Option<TxId> {
        let this_db = self as *const Self as usize;
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
