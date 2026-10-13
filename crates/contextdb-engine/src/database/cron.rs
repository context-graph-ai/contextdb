use super::*;

const CRON_SCHEDULES_CONFIG_KEY: &str = "__cron_schedules";
const CRON_AUDIT_CONFIG_KEY: &str = "__cron_audit";
const MAX_CRON_AUDIT_DEPTH: usize = 10_000;

type CronCallback = Arc<dyn Fn(&Database) -> Result<()> + Send + Sync + 'static>;

pub(super) struct CronState {
    callbacks: RwLock<HashMap<String, CronCallback>>,
    schedules: Mutex<BTreeMap<String, CronSchedule>>,
    audit: Mutex<VecDeque<CronAuditEntry>>,
    runtime: Mutex<CronRuntime>,
    wait_lock: Mutex<()>,
    waiters: Condvar,
    dispatch_lock: Mutex<()>,
    running_schedules: Mutex<HashSet<String>>,
    callback_owner_threads: Mutex<HashMap<thread::ThreadId, usize>>,
    callback_active_count: AtomicUsize,
    pause_count: AtomicU64,
    schedule_count: AtomicU64,
}

#[derive(Debug)]
struct CronRuntime {
    shutdown: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CronSchedule {
    name: String,
    every_text: String,
    every_ms: u64,
    callback: String,
    policy: CronMissedPolicy,
    next_fire_at_ms: u64,
    last_fire_at_ms: Option<u64>,
    /// Count of successful fires. Additive field (defaults to 0 on
    /// deserialization of schedules persisted before this counter existed).
    #[serde(default)]
    fire_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum CronMissedPolicy {
    SkipAndAudit,
    CatchUp { within_ms: u64 },
    FailLoud,
}

#[derive(Debug)]
struct DueDecision {
    fire_count: u64,
    next_fire_at_ms: u64,
    missed_skipped: u64,
    caught_up: Option<u32>,
    fail_loud: Option<Error>,
}

#[derive(Debug)]
struct ReservedCronRun {
    schedule: CronSchedule,
    decision: DueDecision,
}

struct CronCallbackThreadGuard<'a> {
    cron: &'a CronState,
    owner: thread::ThreadId,
}

impl CronState {
    pub(super) fn new() -> Self {
        Self {
            callbacks: RwLock::new(HashMap::new()),
            schedules: Mutex::new(BTreeMap::new()),
            audit: Mutex::new(VecDeque::new()),
            runtime: Mutex::new(CronRuntime {
                shutdown: Arc::new(AtomicBool::new(false)),
                handle: None,
            }),
            wait_lock: Mutex::new(()),
            waiters: Condvar::new(),
            dispatch_lock: Mutex::new(()),
            running_schedules: Mutex::new(HashSet::new()),
            callback_owner_threads: Mutex::new(HashMap::new()),
            callback_active_count: AtomicUsize::new(0),
            pause_count: AtomicU64::new(0),
            schedule_count: AtomicU64::new(0),
        }
    }

    pub(super) fn callback_active_on_other_thread(&self) -> bool {
        if self.callback_active_count.load(Ordering::SeqCst) == 0 {
            return false;
        }
        let current = thread::current().id();
        let owners = self.callback_owner_threads.lock();
        !owners.is_empty() && !owners.contains_key(&current)
    }

    pub(super) fn has_schedules(&self) -> bool {
        self.schedule_count.load(Ordering::SeqCst) > 0
    }

    fn enter_callback_thread_scope(&self) -> CronCallbackThreadGuard<'_> {
        let owner = thread::current().id();
        *self.callback_owner_threads.lock().entry(owner).or_default() += 1;
        global_callback_active_count().fetch_add(1, Ordering::SeqCst);
        self.callback_active_count.fetch_add(1, Ordering::SeqCst);
        CronCallbackThreadGuard { cron: self, owner }
    }

    pub(super) fn resume_tickler(&self) {
        let _ = self
            .pause_count
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |count| {
                Some(count.saturating_sub(1))
            });
        self.waiters.notify_all();
    }
}

impl Drop for CronCallbackThreadGuard<'_> {
    fn drop(&mut self) {
        let mut owners = self.cron.callback_owner_threads.lock();
        match owners.get_mut(&self.owner) {
            Some(count) if *count > 1 => *count -= 1,
            Some(_) => {
                owners.remove(&self.owner);
            }
            None => {}
        }
        self.cron
            .callback_active_count
            .fetch_sub(1, Ordering::SeqCst);
        global_callback_active_count().fetch_sub(1, Ordering::SeqCst);
    }
}

impl Database {
    pub(super) fn load_cron_state_from_persistence(&self) -> Result<()> {
        let Some(source) = self.startup_state() else {
            return Ok(());
        };
        if let Some(schedules) =
            source.config_value::<Vec<CronSchedule>>(CRON_SCHEDULES_CONFIG_KEY)?
        {
            let mut stored = self.cron.schedules.lock();
            stored.clear();
            stored.extend(
                schedules
                    .into_iter()
                    .map(|schedule| (schedule.name.clone(), schedule)),
            );
            self.cron
                .schedule_count
                .store(stored.len() as u64, Ordering::SeqCst);
        }
        if let Some(entries) = source.config_value::<Vec<CronAuditEntry>>(CRON_AUDIT_CONFIG_KEY)? {
            let mut audit = self.cron.audit.lock();
            audit.clear();
            let start = entries.len().saturating_sub(MAX_CRON_AUDIT_DEPTH);
            audit.extend(entries.into_iter().skip(start));
        }
        Ok(())
    }

    pub(super) fn start_cron_tickler_if_schedules_present(&self) {
        if self.access_is_admin() && !self.cron.schedules.lock().is_empty() {
            self.ensure_cron_tickler_running();
        }
    }

    pub(super) fn create_cron_schedule(
        &self,
        name: &str,
        every: &str,
        callback: &str,
        missed_tick_policy: Option<&str>,
        catch_up_within_seconds: Option<u32>,
    ) -> Result<()> {
        let _operation = self.open_operation()?;
        self.require_admin_cron_ddl()?;
        let every_ms = parse_cron_interval_ms(every)?;
        let policy = parse_cron_policy(missed_tick_policy, catch_up_within_seconds)?;
        let schedule = CronSchedule {
            name: name.to_string(),
            every_text: every.to_string(),
            every_ms,
            callback: callback.to_string(),
            policy,
            next_fire_at_ms: now_millis().saturating_add(every_ms),
            last_fire_at_ms: None,
            fire_count: 0,
        };
        {
            let _dispatch = self.cron.dispatch_lock.lock();
            let previous = self
                .cron
                .schedules
                .lock()
                .insert(name.to_string(), schedule);
            if let Err(err) = self.persist_cron_schedules() {
                let mut schedules = self.cron.schedules.lock();
                match previous {
                    Some(previous) => {
                        schedules.insert(name.to_string(), previous);
                    }
                    None => {
                        schedules.remove(name);
                    }
                }
                return Err(err);
            }
            let schedule_count = self.cron.schedules.lock().len();
            self.cron
                .schedule_count
                .store(schedule_count as u64, Ordering::SeqCst);
        }
        self.ensure_cron_tickler_running();
        self.cron.waiters.notify_all();
        Ok(())
    }

    pub(super) fn drop_cron_schedule(&self, name: &str) -> Result<()> {
        let _operation = self.open_operation()?;
        self.require_admin_cron_ddl()?;
        {
            let _dispatch = self.cron.dispatch_lock.lock();
            let removed = self.cron.schedules.lock().remove(name);
            if let Err(err) = self.persist_cron_schedules() {
                if let Some(removed) = removed {
                    self.cron.schedules.lock().insert(name.to_string(), removed);
                }
                return Err(err);
            }
            let schedule_count = self.cron.schedules.lock().len();
            self.cron
                .schedule_count
                .store(schedule_count as u64, Ordering::SeqCst);
        }
        self.cron.waiters.notify_all();
        Ok(())
    }

    /// Registers a host callback for a cron schedule.
    ///
    /// Cron callbacks use the same Class A reentry rules as trigger callbacks:
    /// tx-control from inside the callback returns [`Error::CallbackReentry`],
    /// and writes are allowed only through the supplied tx-bound handle. Cron
    /// same-DB Class B contention returns [`Error::CallbackActiveCrossThread`]
    /// immediately; trigger same-DB Class B uses the wait-and-proceed contract
    /// documented on [`Error::CallbackActiveCrossThread`].
    pub fn register_cron_callback<F>(&self, name: &str, callback: F) -> Result<()>
    where
        F: Fn(&Database) -> Result<()> + Send + Sync + 'static,
    {
        let _operation = self.open_operation()?;
        self.require_admin_cron_ddl()?;
        self.cron
            .callbacks
            .write()
            .insert(name.to_string(), Arc::new(callback));
        if !self.cron.schedules.lock().is_empty() {
            self.ensure_cron_tickler_running();
        }
        self.cron.waiters.notify_all();
        Ok(())
    }

    /// Point-in-time status of every declared schedule: what it's wired to,
    /// whether that callback name has ever been registered, when it last
    /// fired, and how many times it has fired successfully. Backs the CLI's
    /// `.events status` introspection and is a real public accessor, not a
    /// test seam.
    pub fn cron_status(&self) -> Vec<CronScheduleStatus> {
        let _operation = self.assert_open_operation();
        let callbacks = self.cron.callbacks.read();
        self.cron
            .schedules
            .lock()
            .values()
            .map(|schedule| CronScheduleStatus {
                name: schedule.name.clone(),
                every_text: schedule.every_text.clone(),
                callback: schedule.callback.clone(),
                callback_registered: callbacks.contains_key(&schedule.callback),
                next_fire_at_ms: schedule.next_fire_at_ms,
                last_fire_at_ms: schedule.last_fire_at_ms,
                fire_count: schedule.fire_count,
            })
            .collect()
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub fn cron_run_due_now_for_test(
        &self,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<u64> {
        let _operation = self.open_operation()?;
        // Bounded at 1s: comfortably covers the short-but-not-instant
        // intervals a real test schedule uses (e.g. "EVERY '200
        // MILLISECONDS'") without ever hanging -- a schedule further out
        // than that returns unfired rather than sleeping the test through
        // it.
        self.wait_for_imminent_cron_due_for_test(Duration::from_millis(1_000));
        self.dispatch_due_cron_schedules(true, vector_copy_owner, read_memory.clone())
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub fn pause_cron_tickler_for_test(&self) -> CronPauseGuard {
        let _operation = self.assert_open_operation();
        self.cron.pause_count.fetch_add(1, Ordering::SeqCst);
        self.cron.waiters.notify_all();
        CronPauseGuard {
            cron: self.cron.clone(),
        }
    }

    /// Test-only: whether an autonomous cron tickler thread is currently
    /// running. No shipped surface reads this; temporal-absence negatives
    /// ("no tickler started" / "no further fires possible") assert on this
    /// state instead of sleeping through a wall-clock window.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn cron_tickler_running_for_test(&self) -> bool {
        self.cron.runtime.lock().handle.is_some()
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn __trigger_progress_worker_handle_for_test(&self) -> Database {
        let _operation = self.assert_open_operation();
        self.worker_handle_for_background()
    }

    #[cfg(any(test, feature = "test-seams"))]
    pub fn cron_audit_log_for_test(&self) -> Vec<CronAuditEntry> {
        let _operation = self.assert_open_operation();
        self.cron.audit.lock().iter().cloned().collect()
    }

    fn require_admin_cron_ddl(&self) -> Result<()> {
        if self.has_access_constraints_for_query() {
            return Err(Error::Other(
                "DDL requires an admin database handle".to_string(),
            ));
        }
        Ok(())
    }

    pub(super) fn stop_cron_tickler(&self) {
        if !self.resource_owner {
            return;
        }
        let handle = {
            let mut runtime = self.cron.runtime.lock();
            runtime.shutdown.store(true, Ordering::SeqCst);
            self.cron.waiters.notify_all();
            let handle = runtime.handle.take();
            runtime.shutdown = Arc::new(AtomicBool::new(false));
            handle
        };
        if let Some(handle) = handle {
            let _ = handle.join();
        }
    }

    fn ensure_cron_tickler_running(&self) {
        if !self.resource_owner {
            return;
        }
        if self.resource_closed.load(Ordering::SeqCst) {
            return;
        }
        if self.cron.pause_count.load(Ordering::SeqCst) > 0 {
            return;
        }
        self.start_cron_tickler_runtime();
    }

    fn start_cron_tickler_runtime(&self) {
        let mut runtime = self.cron.runtime.lock();
        if runtime.handle.is_some() {
            return;
        }
        runtime.shutdown.store(false, Ordering::SeqCst);
        let shutdown = runtime.shutdown.clone();
        let db = self.worker_handle_for_background();
        #[cfg(feature = "test-seams")]
        crate::read_probe::note_cron_worker_start();
        #[cfg(feature = "test-seams")]
        crate::read_probe::note_background_worker_start();
        let handle = thread::spawn(move || db.cron_tickler_loop(shutdown));
        runtime.handle = Some(handle);
    }

    fn cron_tickler_loop(self, shutdown: Arc<AtomicBool>) {
        while !shutdown.load(Ordering::SeqCst) && !self.resource_closed.load(Ordering::SeqCst) {
            if self.cron.pause_count.load(Ordering::SeqCst) == 0
                && !shutdown.load(Ordering::SeqCst)
                && !self.resource_closed.load(Ordering::SeqCst)
            {
                let _ = (|| -> Result<()> {
                    let read_memory = self.declared_reader_memory()?;
                    let mut vector_copy_owner =
                        crate::vector_read_memory::admission(read_memory.clone());
                    self.dispatch_due_cron_schedules(false, &mut vector_copy_owner, read_memory)?;
                    Ok(())
                })();
            }
            if shutdown.load(Ordering::SeqCst) || self.resource_closed.load(Ordering::SeqCst) {
                break;
            }
            let wait = if self.cron.pause_count.load(Ordering::SeqCst) > 0 {
                Duration::from_millis(50)
            } else {
                self.duration_until_next_cron_fire()
                    .unwrap_or_else(|| Duration::from_millis(500))
                    .clamp(Duration::from_millis(10), Duration::from_millis(500))
            };
            let mut guard = self.cron.wait_lock.lock();
            self.cron.waiters.wait_for(&mut guard, wait);
        }
    }

    fn duration_until_next_cron_fire(&self) -> Option<Duration> {
        let now = now_millis();
        self.cron
            .schedules
            .lock()
            .values()
            .map(|schedule| Duration::from_millis(schedule.next_fire_at_ms.saturating_sub(now)))
            .min()
    }

    #[cfg(any(test, feature = "test-seams"))]
    fn wait_for_imminent_cron_due_for_test(&self, max_wait: Duration) {
        let deadline = Instant::now() + max_wait;
        loop {
            let Some(wait) = self.duration_until_next_cron_fire() else {
                return;
            };
            if wait.is_zero() {
                return;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() || wait > remaining {
                return;
            }
            thread::sleep(wait.min(Duration::from_millis(10)));
        }
    }

    fn dispatch_due_cron_schedules(
        &self,
        manual: bool,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<u64> {
        let reserved_runs = self.reserve_due_cron_runs()?;
        let mut successful_fires = 0u64;
        let mut fail_loud_error = None;
        let mut dispatch_error = None;

        for run in reserved_runs {
            let schedule_name = run.schedule.name.clone();
            let result = self.dispatch_reserved_cron_run(
                run,
                manual,
                &mut successful_fires,
                &mut fail_loud_error,
                vector_copy_owner,
                read_memory.clone(),
            );
            self.cron.running_schedules.lock().remove(&schedule_name);
            self.cron.waiters.notify_all();
            if let Err(err) = result
                && dispatch_error.is_none()
            {
                dispatch_error = Some(err);
            }
        }

        if let Some(err) = dispatch_error {
            return Err(err);
        }

        if let Some(err) = fail_loud_error {
            Err(err)
        } else {
            Ok(successful_fires)
        }
    }

    fn reserve_due_cron_runs(&self) -> Result<Vec<ReservedCronRun>> {
        let _dispatch = self.cron.dispatch_lock.lock();
        let now = now_millis();
        let mut reserved = Vec::new();
        {
            let mut schedules = self.cron.schedules.lock();
            let mut running = self.cron.running_schedules.lock();
            for schedule in schedules.values_mut() {
                if schedule.next_fire_at_ms > now || running.contains(&schedule.name) {
                    continue;
                }
                let snapshot = schedule.clone();
                let decision = compute_due_decision(schedule, now);
                schedule.next_fire_at_ms = decision.next_fire_at_ms;
                if decision.fire_count > 0 {
                    schedule.last_fire_at_ms = Some(now);
                }
                running.insert(snapshot.name.clone());
                reserved.push(ReservedCronRun {
                    schedule: snapshot,
                    decision,
                });
            }
        }
        if !reserved.is_empty()
            && let Err(err) = self.persist_cron_schedules()
        {
            {
                let mut schedules = self.cron.schedules.lock();
                for run in &reserved {
                    if let Some(current) = schedules.get_mut(&run.schedule.name) {
                        current.next_fire_at_ms = run.schedule.next_fire_at_ms;
                        current.last_fire_at_ms = run.schedule.last_fire_at_ms;
                    }
                }
            }
            let mut running = self.cron.running_schedules.lock();
            for run in &reserved {
                running.remove(&run.schedule.name);
            }
            return Err(err);
        }
        Ok(reserved)
    }

    fn dispatch_reserved_cron_run(
        &self,
        run: ReservedCronRun,
        manual: bool,
        successful_fires: &mut u64,
        fail_loud_error: &mut Option<Error>,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<()> {
        let schedule = run.schedule;
        let decision = run.decision;
        for _ in 0..decision.missed_skipped {
            self.append_cron_audit(
                &schedule.name,
                CronAuditKind::MissedSkipped,
                self.current_lsn(),
            )?;
        }
        if let Some(ticks) = decision.caught_up {
            self.append_cron_audit(
                &schedule.name,
                CronAuditKind::MissedCaughtUp { ticks },
                self.current_lsn(),
            )?;
        }
        if let Some(err) = decision.fail_loud {
            self.append_cron_audit(
                &schedule.name,
                CronAuditKind::Failed(err.to_string()),
                self.current_lsn(),
            )?;
            if manual && fail_loud_error.is_none() {
                *fail_loud_error = Some(err);
            }
            return Ok(());
        }

        let callback = self.cron.callbacks.read().get(&schedule.callback).cloned();
        let Some(callback) = callback else {
            self.append_cron_audit(
                &schedule.name,
                CronAuditKind::Failed(format!("no callback registered: {}", schedule.callback)),
                self.current_lsn(),
            )?;
            return Ok(());
        };

        for _ in 0..decision.fire_count {
            match self.run_cron_callback_transaction(
                callback.clone(),
                vector_copy_owner,
                read_memory.clone(),
            ) {
                Ok(lsn) => {
                    *successful_fires += 1;
                    if let Some(current) = self.cron.schedules.lock().get_mut(&schedule.name) {
                        current.fire_count = current.fire_count.saturating_add(1);
                    }
                    self.append_cron_audit(&schedule.name, CronAuditKind::Fired, lsn)?;
                }
                Err(err) => {
                    self.append_cron_audit(
                        &schedule.name,
                        CronAuditKind::Failed(err.to_string()),
                        self.current_lsn(),
                    )?;
                }
            }
        }
        Ok(())
    }

    fn run_cron_callback_transaction(
        &self,
        callback: CronCallback,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<Lsn> {
        (|| {
            let vector_copy_owner = std::cell::RefCell::new(vector_copy_owner);

            let _callback_thread = self.cron.enter_callback_thread_scope();
            let tx = self.begin_for_internal_write()?;
            let mut pending_sink_events = Vec::new();
            let pending_trigger_audits = std::cell::RefCell::new(Vec::new());
            let pending_trigger_active_guards =
                std::cell::RefCell::new(Vec::<TriggerCallbackThreadGuard>::new());
            let mut committed_trigger_audit_entries = Vec::new();
            // Cron callbacks are arbitrary user code and can stage vector writes
            // while the tx manager's commit mutex is held. Acquire current vector
            // schema refs before entering that mutex so DDL keeps the normal
            // schema-gate -> commit-mutex lock order.
            let mut vector_schema_guard = Some(self.vector_schema_read_many(
                self.vector_store_schema_refs(&mut vector_copy_owner.borrow_mut())?,
            ));
            let delete_release_bytes = std::cell::RefCell::new(DeleteReleaseBytes::default());
            // Armed just before the durable apply; see `PublicationInterruptionWatch`
            // in `database.rs`. A scheduled job's commit is durable the moment the
            // manager applies it, and its sink events, trigger audits and commit
            // event reach memory only afterwards -- so an unwind in between leaves
            // this handle behind its own durable image, exactly as it does on the
            // local and received doors, and it must stop serving until reopened.
            let publication_watch =
                std::cell::RefCell::new(None::<super::PublicationInterruptionWatch>);
            let result = self.tx_mgr.commit_with_reserved_lsn_callback_mut(
                tx,
                read_memory.clone(),
                |reserved_lsn| {
                    let callback_result = CRON_LSN_OVERRIDE.with(|slot| {
                        let prior_lsn = slot.replace(Some(reserved_lsn));
                        let prior_tx = CRON_CALLBACK_TX.with(|tx_slot| tx_slot.replace(Some(tx)));
                        let this_db = self.identity();
                        let prior_db =
                            CRON_CALLBACK_DB.with(|db_slot| db_slot.replace(Some(this_db)));
                        let gate_id = self.vector_schema_gate_id();
                        let prior_gate = CRON_CALLBACK_VECTOR_SCHEMA_GATE
                            .with(|gate_slot| gate_slot.replace(Some(gate_id)));
                        let prior_active = CRON_CALLBACK_ACTIVE.with(|active| active.replace(true));
                        let mut result = catch_unwind(AssertUnwindSafe(|| callback(self)));
                        if let Ok(Ok(())) = &result
                            && let Err(error) = self.prepare_active_trigger_write_set_for_dispatch(
                                tx,
                                &mut vector_copy_owner.borrow_mut(),
                                read_memory.clone(),
                            )
                        {
                            result = Ok(Err(error));
                        }
                        if let Ok(Ok(())) = &result {
                            match self.dispatch_triggers_for_tx(
                                tx,
                                &mut vector_copy_owner.borrow_mut(),
                                read_memory.clone(),
                            ) {
                                Ok(outcome) => {
                                    *pending_trigger_audits.borrow_mut() = outcome.pending;
                                    pending_trigger_active_guards
                                        .borrow_mut()
                                        .extend(outcome.active_guards);
                                }
                                Err(failure) => {
                                    let failure = *failure;
                                    pending_trigger_active_guards
                                        .borrow_mut()
                                        .extend(failure.active_guards);
                                    result = Ok(Err(failure.error));
                                }
                            }
                        }
                        if let Ok(Ok(())) = &result
                            && let Err(error) = self.prepare_active_trigger_write_set_for_dispatch(
                                tx,
                                &mut vector_copy_owner.borrow_mut(),
                                read_memory.clone(),
                            )
                        {
                            result = Ok(Err(error));
                        }
                        CRON_CALLBACK_ACTIVE.with(|active| active.set(prior_active));
                        CRON_CALLBACK_DB.with(|db_slot| db_slot.set(prior_db));
                        CRON_CALLBACK_VECTOR_SCHEMA_GATE
                            .with(|gate_slot| gate_slot.set(prior_gate));
                        CRON_CALLBACK_TX.with(|tx_slot| tx_slot.set(prior_tx));
                        slot.replace(prior_lsn);
                        result
                    });
                    match callback_result {
                        Ok(result) => result,
                        Err(payload) => Err(Error::Other(format!(
                            "cron callback panicked: {}",
                            panic_payload_to_string(payload)
                        ))),
                    }
                },
                |ws| {
                    if !ws.is_empty() {
                        debug_assert!(
                            !Self::write_set_touches_vector_schema(ws)
                                || vector_schema_guard.is_some()
                        );
                        self.rewrite_txid_placeholders(tx, ws)?;
                        let _validation = self.commit_validate(
                            tx,
                            ws,
                            &mut vector_copy_owner.borrow_mut(),
                            read_memory.clone(),
                        )?;
                        self.plugin.pre_commit(ws, CommitSource::AutoCommit)?;
                        let prepared_sink_events =
                            self.prepare_sink_events_for_write_set(ws, read_memory.clone())?;
                        if !prepared_sink_events.is_empty() {
                            self.event_bus
                                .ensure_queue_capacity(&prepared_sink_events)?;
                        }
                        if self.persistence.is_some() && !prepared_sink_events.is_empty() {
                            ws.config_writes.push((
                                super::event_bus::NEXT_ACTION_ID_CONFIG_KEY.to_string(),
                                RedbPersistence::encode_config_value(
                                    &self.event_bus.next_action_id(),
                                )?,
                            ));
                        }
                        if self.persistence.is_some()
                            && let Some(lsn) = ws.commit_lsn
                        {
                            self.event_bus
                                .stage_sink_events_for_persistence(lsn, prepared_sink_events);
                        } else {
                            pending_sink_events = prepared_sink_events;
                        }
                        if let Some(lsn) = ws.commit_lsn {
                            let pending = pending_trigger_audits.borrow();
                            committed_trigger_audit_entries =
                                self.committed_trigger_audits_for_pending(&pending, ws, lsn);
                            self.stage_trigger_audits_for_persistence(
                                lsn,
                                &committed_trigger_audit_entries,
                            );
                        }
                        *delete_release_bytes.borrow_mut() = self
                            .delete_release_bytes_for_write_set(
                                ws,
                                &mut vector_copy_owner.borrow_mut(),
                                read_memory.clone(),
                            )?;
                    }
                    *publication_watch.borrow_mut() = Some(
                        super::PublicationInterruptionWatch::arm(&self.publication_interrupted),
                    );
                    Ok(())
                },
            );

            match result {
                Ok((lsn, ws)) => {
                    self.pending_commit_metadata.lock().remove(&tx);
                    if !ws.is_empty() {
                        #[cfg(test)]
                        super::take_local_commit_stage_fault_for_test(
                            super::LocalCommitStageFaultPoint::AfterScheduledJobDurableCommit,
                        );
                        self.release_delete_allocations_from_bytes(&delete_release_bytes.borrow());
                        drop(vector_schema_guard.take());
                        self.plugin.post_commit(&ws, CommitSource::AutoCommit);
                        let sink_events_to_publish = if self.persistence.is_some() {
                            ws.commit_lsn
                                .and_then(|lsn| {
                                    self.event_bus.take_staged_sink_events_for_persistence(lsn)
                                })
                                .map(EventBusState::materialize_staged_sink_events)
                                .unwrap_or_default()
                        } else {
                            pending_sink_events
                        };
                        self.publish_prepared_sink_events_to_memory(sink_events_to_publish);
                        self.append_trigger_audits_to_memory(committed_trigger_audit_entries);
                        self.publish_commit_event_if_subscribers(
                            &ws,
                            CommitSource::AutoCommit,
                            lsn,
                        );
                    }
                    if let Some(watch) = publication_watch.borrow_mut().take() {
                        watch.completed();
                    }
                    Ok(lsn)
                }
                Err(failure) => {
                    if let Some(watch) = publication_watch.borrow_mut().take() {
                        watch.completed();
                    }
                    // Owned no-host failure leaves source and journal installed.
                    // Its paired rollback below validates them before cleanup.
                    if matches!(&self.tx_mgr, super::DatabaseTransactionHost::Legacy(_)) {
                        self.pending_commit_metadata.lock().remove(&tx);
                    }
                    if let Some(lsn) = failure.write_set.as_ref().and_then(|ws| ws.commit_lsn) {
                        let _ = self.event_bus.take_staged_sink_events_for_persistence(lsn);
                        self.discard_staged_trigger_audits_for_persistence(lsn);
                    }
                    if let Some(ws) = failure.write_set {
                        self.release_insert_allocations(&ws, &mut vector_copy_owner.borrow_mut())?;
                    } else {
                        let _ = self.rollback_without_callback_tx_control(tx);
                    }
                    let pending = pending_trigger_audits.borrow();
                    self.append_rolled_back_trigger_audits(
                        &pending,
                        tx,
                        &failure.error.to_string(),
                    )?;
                    Err(failure.error)
                }
            }
        })()
    }

    fn append_cron_audit(
        &self,
        schedule_name: &str,
        kind: CronAuditKind,
        at_lsn: Lsn,
    ) -> Result<()> {
        {
            let mut audit = self.cron.audit.lock();
            if audit.len() == MAX_CRON_AUDIT_DEPTH {
                audit.pop_front();
            }
            audit.push_back(CronAuditEntry {
                schedule_name: schedule_name.to_string(),
                kind,
                at_lsn,
            });
        }
        self.persist_cron_audit()
    }

    fn persist_cron_schedules(&self) -> Result<()> {
        let Some(persistence) = self.persistence.as_ref() else {
            return Ok(());
        };
        let schedules = self
            .cron
            .schedules
            .lock()
            .values()
            .cloned()
            .collect::<Vec<_>>();
        self.publish_direct_metadata(|| {
            persistence.flush_config_value(CRON_SCHEDULES_CONFIG_KEY, &schedules)
        })
    }

    /// Encoded cron-schedule config for checkpoint export. DDL-declared
    /// schedules are database state even on in-memory sources, so export
    /// serializes them exactly as file-backed DDL persistence would have.
    pub(super) fn export_cron_config_values(&self) -> Result<Vec<(&'static str, Vec<u8>)>> {
        let schedules = self
            .cron
            .schedules
            .lock()
            .values()
            .cloned()
            .collect::<Vec<_>>();
        Ok(vec![(
            CRON_SCHEDULES_CONFIG_KEY,
            RedbPersistence::encode_config_value(&schedules)?,
        )])
    }

    fn persist_cron_audit(&self) -> Result<()> {
        let Some(persistence) = self.persistence.as_ref() else {
            return Ok(());
        };
        let audit = self.cron.audit.lock().iter().cloned().collect::<Vec<_>>();
        self.publish_direct_metadata(|| {
            persistence.flush_config_value(CRON_AUDIT_CONFIG_KEY, &audit)
        })
    }

    fn worker_handle_for_background(&self) -> Database {
        {
            let mut storage = std::mem::MaybeUninit::<super::Database>::uninit();
            let mut construction = super::DatabaseConstruction {
                source: storage.as_mut_ptr(),
                initialized: [false; 137],
            };
            // Every initializer is evaluated in its original order. A field is
            // marked only after the write; an unwinding initializer never makes
            // uninitialized storage eligible for cleanup.
            unsafe {
                {
                    std::ptr::addr_of_mut!((*construction.source).id)
                        .write(super::DatabaseId::next());
                    construction.initialized[0] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(0);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).declared_read_memory)
                        .write(self.declared_read_memory.clone());
                    construction.initialized[54] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(54);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).tx_mgr)
                        .write(self.tx_mgr.clone());
                    construction.initialized[1] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(1);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).relational_store)
                        .write(self.relational_store.clone());
                    construction.initialized[2] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(2);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).vector_serving_cache_bytes)
                        .write(self.vector_serving_cache_bytes.clone());
                    construction.initialized[101] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(101);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).graph_store)
                        .write(self.graph_store.clone());
                    construction.initialized[3] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(3);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).vector_store)
                        .write(self.vector_store.clone());
                    construction.initialized[4] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(4);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).vector_schema_gates)
                        .write(self.vector_schema_gates.clone());
                    construction.initialized[5] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(5);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).change_log)
                        .write(self.change_log.clone());
                    construction.initialized[6] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(6);
                }
                #[cfg(any(test, feature = "test-seams"))]
                {
                    std::ptr::addr_of_mut!((*construction.source).forced_change_log_gap_through)
                        .write(self.forced_change_log_gap_through.clone());
                    construction.initialized[7] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(7);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).change_log_table_index)
                        .write(self.change_log_table_index.clone());
                    construction.initialized[8] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(8);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).change_log_lsn_refcounts)
                        .write(self.change_log_lsn_refcounts.clone());
                    construction.initialized[9] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(9);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).ddl_log)
                        .write(self.ddl_log.clone());
                    construction.initialized[10] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(10);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).sync_tombstone_arrivals)
                        .write(self.sync_tombstone_arrivals.clone());
                    construction.initialized[11] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(11);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).sync_graph_arrivals)
                        .write(self.sync_graph_arrivals.clone());
                    construction.initialized[12] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(12);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).terminal_refusal_markers)
                        .write(self.terminal_refusal_markers.clone());
                    construction.initialized[13] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(13);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).terminal_refusal_scans)
                        .write(self.terminal_refusal_scans.clone());
                    construction.initialized[14] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(14);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).accepted_sync_row_authors)
                        .write(self.accepted_sync_row_authors.clone());
                    construction.initialized[15] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(15);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).sync_push_apply_locks)
                        .write(self.sync_push_apply_locks.clone());
                    construction.initialized[70] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(70);
                }
                #[cfg(feature = "sync-orchestration")]
                {
                    std::ptr::addr_of_mut!((*construction.source).sync_client_push_locks)
                        .write(self.sync_client_push_locks.clone());
                    construction.initialized[71] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(71);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).received_schema_stages)
                        .write(self.received_schema_stages.clone());
                    construction.initialized[16] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(16);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).local_erasure_stages)
                        .write(self.local_erasure_stages.clone());
                    construction.initialized[17] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(17);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).received_arriving_table_meta)
                        .write(self.received_arriving_table_meta.clone());
                    construction.initialized[18] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(18);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).pending_local_schema_stages)
                        .write(self.pending_local_schema_stages.clone());
                    construction.initialized[19] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(19);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).local_schema_stages)
                        .write(self.local_schema_stages.clone());
                    construction.initialized[20] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(20);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).capture_detached_sync_write_set)
                        .write(self.capture_detached_sync_write_set.clone());
                    construction.initialized[21] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(21);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).detached_sync_write_set)
                        .write(self.detached_sync_write_set.clone());
                    construction.initialized[22] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(22);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).received_staging_access)
                        .write(self.received_staging_access.clone());
                    construction.initialized[23] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(23);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).received_vector_image_pending)
                        .write(self.received_vector_image_pending.clone());
                    construction.initialized[24] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(24);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).persistence)
                        .write(self.persistence.clone());
                    construction.initialized[25] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(25);
                }
                #[cfg(any(test, feature = "test-seams"))]
                {
                    std::ptr::addr_of_mut!((*construction.source).commit_source_reference_charge)
                        .write(self.commit_source_reference_charge);
                    construction.initialized[26] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(26);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).committed_image_startup)
                        .write(self.committed_image_startup.clone());
                    construction.initialized[27] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(27);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).image_history)
                        .write(self.image_history.clone());
                    construction.initialized[28] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(28);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).retained_relational_source)
                        .write(self.retained_relational_source.clone());
                    construction.initialized[29] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(29);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).read_image_memory)
                        .write(self.read_image_memory.clone());
                    construction.initialized[134] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(134);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).blob_repository)
                        .write(self.blob_repository.clone());
                    construction.initialized[30] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(30);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).open_registry_path)
                        .write(Mutex::new(None));
                    construction.initialized[31] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(31);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).operation_gate)
                        .write(self.operation_gate.clone());
                    construction.initialized[32] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(32);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).read_publication_gate)
                        .write(self.read_publication_gate.clone());
                    construction.initialized[33] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(33);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).schema_publication_gate)
                        .write(self.schema_publication_gate.clone());
                    construction.initialized[34] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(34);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).apply_phase_pause)
                        .write(self.apply_phase_pause.clone());
                    construction.initialized[35] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(35);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).sync_apply_pre_commit_pause)
                        .write(self.sync_apply_pre_commit_pause.clone());
                    construction.initialized[36] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(36);
                }
                {
                    std::ptr::addr_of_mut!((*construction.source).retained_graph_source)
                        .write(self.retained_graph_source.clone());
                    construction.initialized[136] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(136);
                }
                #[cfg(any(test, feature = "test-seams"))]
                {
                    std::ptr::addr_of_mut!((*construction.source).received_schema_pre_commit_pause)
                        .write(self.received_schema_pre_commit_pause.clone());
                    construction.initialized[37] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(37);
                }
                #[cfg(any(test, feature = "test-seams"))]
                {
                    std::ptr::addr_of_mut!(
                        (*construction.source).received_schema_pre_publish_pause
                    )
                    .write(self.received_schema_pre_publish_pause.clone());
                    construction.initialized[38] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(38);
                }
                #[cfg(any(test, feature = "test-seams"))]
                {
                    std::ptr::addr_of_mut!((*construction.source).received_schema_apply_observer)
                        .write(self.received_schema_apply_observer.clone());
                    construction.initialized[39] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(39);
                }
                #[cfg(any(test, feature = "test-seams"))]
                {
                    std::ptr::addr_of_mut!((*construction.source).local_schema_pre_commit_pause)
                        .write(self.local_schema_pre_commit_pause.clone());
                    construction.initialized[40] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(40);
                }
                #[cfg(any(test, feature = "test-seams"))]
                {
                    std::ptr::addr_of_mut!((*construction.source).local_schema_pre_publish_pause)
                        .write(self.local_schema_pre_publish_pause.clone());
                    construction.initialized[41] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(41);
                }
                #[cfg(any(test, feature = "test-seams"))]
                {
                    std::ptr::addr_of_mut!(
                        (*construction.source).local_schema_between_publication_pause
                    )
                    .write(self.local_schema_between_publication_pause.clone());
                    construction.initialized[42] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(42);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).export_after_capture_pause)
                        .write(self.export_after_capture_pause.clone());
                    construction.initialized[43] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(43);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).relational).write(
                        self.tx_mgr
                            .relational_executor(self.relational_store.clone())
                            .with_staged_posting_admission(Arc::<
                                super::writer_read::WriterStagedPostingAdmission,
                            >::clone(
                                &self.finalization.staged_posting_admission,
                            )),
                    );
                    construction.initialized[44] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(44);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).graph)
                        .write(self.tx_mgr.graph_executor(self.graph_store.clone()));
                    construction.initialized[45] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(45);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).vector).write(
                        self.tx_mgr.vector_executor(
                            self.vector_store.clone(),
                            Arc::new(OnceLock::new()),
                            Arc::clone(&self.finalization.vector_accountant),
                        ),
                    );
                    construction.initialized[46] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(46);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).image_store_file_bytes)
                        .write(None);
                    construction.initialized[47] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(47);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).session_tx)
                        .write(Arc::new(Mutex::new(None)));
                    construction.initialized[48] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(48);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).origin_session_tx).write(None);
                    construction.initialized[49] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(49);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).instance_id)
                        .write(self.instance_id);
                    construction.initialized[50] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(50);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).owner_thread)
                        .write(self.owner_thread);
                    construction.initialized[51] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(51);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).plugin)
                        .write(self.plugin.clone());
                    construction.initialized[52] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(52);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).owner_read_config)
                        .write(self.owner_read_config.clone());
                    construction.initialized[53] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(53);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).owner_read_service).write(None);
                    construction.initialized[55] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(55);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).owner_read_startup_failure)
                        .write(None);
                    construction.initialized[56] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(56);
                }
                #[cfg(any(test, feature = "test-seams"))]
                {
                    std::ptr::addr_of_mut!((*construction.source).route_observer).write(None);
                    construction.initialized[57] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(57);
                }
                #[cfg(any(test, feature = "test-seams"))]
                {
                    std::ptr::addr_of_mut!((*construction.source).kernel_observer).write(None);
                    construction.initialized[58] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(58);
                }
                #[cfg(any(test, feature = "test-seams"))]
                {
                    std::ptr::addr_of_mut!(
                        (*construction.source).snapshot_free_retirement_pass_count
                    )
                    .write(self.snapshot_free_retirement_pass_count.clone());
                    construction.initialized[59] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(59);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).access)
                        .write(AccessConstraints::default());
                    construction.initialized[60] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(60);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).accountant)
                        .write(self.accountant.clone());
                    construction.initialized[61] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(61);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).conflict_policies)
                        .write(RwLock::new(self.conflict_policies.read().clone()));
                    construction.initialized[62] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(62);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).subscriptions)
                        .write(self.subscriptions.clone());
                    construction.initialized[63] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(63);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).pruning_runtime)
                        .write(Mutex::new(PruningRuntime::new()));
                    construction.initialized[64] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(64);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).pruning_guard)
                        .write(self.pruning_guard.clone());
                    construction.initialized[65] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(65);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).retention_sync_peer)
                        .write(self.retention_sync_peer.clone());
                    construction.initialized[66] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(66);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).in_memory_destination_reuploads)
                        .write(self.in_memory_destination_reuploads.clone());
                    construction.initialized[67] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(67);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).lineage_state_lock)
                        .write(self.lineage_state_lock.clone());
                    construction.initialized[68] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(68);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).received_schema_manifest_lock)
                        .write(self.received_schema_manifest_lock.clone());
                    construction.initialized[69] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(69);
                }

                {
                    std::ptr::addr_of_mut!(
                        (*construction.source).in_memory_received_schema_manifests
                    )
                    .write(self.in_memory_received_schema_manifests.clone());
                    construction.initialized[72] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(72);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).in_memory_table_generations)
                        .write(self.in_memory_table_generations.clone());
                    construction.initialized[73] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(73);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).in_memory_ddl_generations)
                        .write(self.in_memory_ddl_generations.clone());
                    construction.initialized[74] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(74);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).received_ddl_arrivals)
                        .write(self.received_ddl_arrivals.clone());
                    construction.initialized[75] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(75);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).superseded_ddl_occurrences)
                        .write(self.superseded_ddl_occurrences.clone());
                    construction.initialized[76] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(76);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).cron).write(self.cron.clone());
                    construction.initialized[77] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(77);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).event_bus)
                        .write(self.event_bus.clone());
                    construction.initialized[78] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(78);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).trigger)
                        .write(self.trigger.clone());
                    construction.initialized[79] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(79);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).sync_relay_mode)
                        .write(self.sync_relay_mode.clone());
                    construction.initialized[81] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(81);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).in_memory_sync_progress)
                        .write(self.in_memory_sync_progress.clone());
                    construction.initialized[82] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(82);
                }

                {
                    std::ptr::addr_of_mut!(
                        (*construction.source).in_memory_applied_push_watermarks
                    )
                    .write(self.in_memory_applied_push_watermarks.clone());
                    construction.initialized[83] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(83);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).sync_incarnations)
                        .write(self.sync_incarnations.clone());
                    construction.initialized[84] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(84);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).custody_runtime)
                        .write(self.custody_runtime.clone());
                    construction.initialized[85] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(85);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).custody_cache)
                        .write(self.custody_cache.clone());
                    construction.initialized[86] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(86);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).custody_metadata)
                        .write(self.custody_metadata.clone());
                    construction.initialized[87] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(87);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).pending_event_bus_ddl)
                        .write(Mutex::new(HashMap::new()));
                    construction.initialized[88] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(88);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).pending_commit_metadata)
                        .write(Mutex::new(HashMap::new()));
                    construction.initialized[89] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(89);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).limit_update_lock)
                        .write(self.limit_update_lock.clone());
                    construction.initialized[90] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(90);
                }
                #[cfg(any(test, feature = "test-seams"))]
                {
                    std::ptr::addr_of_mut!(
                        (*construction.source).maintenance_poll_interval_update_pause
                    )
                    .write(self.maintenance_poll_interval_update_pause.clone());
                    construction.initialized[91] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(91);
                }
                #[cfg(any(test, feature = "test-seams"))]
                {
                    std::ptr::addr_of_mut!(
                        (*construction.source).maintenance_poll_interval_update_probe
                    )
                    .write(self.maintenance_poll_interval_update_probe.clone());
                    construction.initialized[92] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(92);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).disk_limit)
                        .write(Arc::clone(&self.disk_limit));
                    construction.initialized[93] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(93);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).disk_limit_startup_ceiling)
                        .write(Arc::clone(&self.disk_limit_startup_ceiling));
                    construction.initialized[94] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(94);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).trigger_audit_retention_secs)
                        .write(self.trigger_audit_retention_secs.clone());
                    construction.initialized[95] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(95);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).snapshot_registry)
                        .write(self.snapshot_registry.clone());
                    construction.initialized[96] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(96);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).vector_partition_snapshot_stages)
                        .write(self.vector_partition_snapshot_stages.clone());
                    construction.initialized[97] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(97);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).retention_deferred_edge_nodes)
                        .write(self.retention_deferred_edge_nodes.clone());
                    construction.initialized[98] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(98);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).maintenance_caller_driven)
                        .write(self.maintenance_caller_driven.clone());
                    construction.initialized[99] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(99);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).maintenance_poll_interval_ms)
                        .write(self.maintenance_poll_interval_ms.clone());
                    construction.initialized[100] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(100);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).last_maintenance_cycle_at)
                        .write(self.last_maintenance_cycle_at.clone());
                    construction.initialized[102] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(102);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).caller_driven_backlog_warned_at)
                        .write(self.caller_driven_backlog_warned_at.clone());
                    construction.initialized[103] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(103);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).last_auto_compact_at)
                        .write(self.last_auto_compact_at.clone());
                    construction.initialized[104] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(104);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).auto_compact_min_interval)
                        .write(self.auto_compact_min_interval.clone());
                    construction.initialized[105] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(105);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).sync_watermark)
                        .write(self.sync_watermark.clone());
                    construction.initialized[106] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(106);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).closed)
                        .write(AtomicBool::new(false));
                    construction.initialized[107] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(107);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).close_progress)
                        .write(Arc::new(Mutex::new(CloseProgress::Idle)));
                    construction.initialized[108] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(108);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).close_changed)
                        .write(Arc::new(Condvar::new()));
                    construction.initialized[109] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(109);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).close_error)
                        .write(Mutex::new(None));
                    construction.initialized[110] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(110);
                }
                #[cfg(any(test, feature = "test-seams"))]
                {
                    std::ptr::addr_of_mut!((*construction.source).close_follower_observer)
                        .write(Mutex::new(None));
                    construction.initialized[111] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(111);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).resource_closed)
                        .write(self.resource_closed.clone());
                    construction.initialized[112] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(112);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).publication_interrupted)
                        .write(self.publication_interrupted.clone());
                    construction.initialized[113] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(113);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).rows_examined)
                        .write(AtomicU64::new(0));
                    construction.initialized[114] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(114);
                }
                #[cfg(any(test, feature = "test-seams"))]
                {
                    std::ptr::addr_of_mut!((*construction.source).candidate_rows_materialized)
                        .write(self.candidate_rows_materialized.clone());
                    construction.initialized[115] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(115);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).last_vector_search_used_hnsw)
                        .write(Arc::clone(&self.last_vector_search_used_hnsw));
                    construction.initialized[116] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(116);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).last_vector_search_trace)
                        .write(Arc::clone(&self.last_vector_search_trace));
                    construction.initialized[117] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(117);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).statement_cache)
                        .write(RwLock::new(HashMap::new()));
                    construction.initialized[118] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(118);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).rank_formula_cache)
                        .write(Arc::clone(&self.rank_formula_cache));
                    construction.initialized[119] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(119);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).acl_grant_cache)
                        .write(RwLock::new(super::gate::AdmittedAclGrantCache::new()));
                    construction.initialized[120] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(120);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).rank_policy_eval_count)
                        .write(AtomicU64::new(0));
                    construction.initialized[121] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(121);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).rank_policy_formula_parse_count)
                        .write(AtomicU64::new(0));
                    construction.initialized[122] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(122);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).fk_indexed_tuple_probes)
                        .write(AtomicU64::new(0));
                    construction.initialized[123] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(123);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).fk_full_scan_fallbacks)
                        .write(AtomicU64::new(0));
                    construction.initialized[124] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(124);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).commit_rows_validated)
                        .write(AtomicU64::new(0));
                    construction.initialized[125] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(125);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).commit_indexed_probes)
                        .write(AtomicU64::new(0));
                    construction.initialized[126] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(126);
                }

                {
                    std::ptr::addr_of_mut!(
                        (*construction.source).commit_staged_vs_staged_comparisons
                    )
                    .write(AtomicU64::new(0));
                    construction.initialized[127] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(127);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).commit_scan_rows_touched)
                        .write(AtomicU64::new(0));
                    construction.initialized[128] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(128);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).commit_index_maintenance_visits)
                        .write(AtomicU64::new(0));
                    construction.initialized[129] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(129);
                }
                #[cfg(any(test, feature = "test-seams"))]
                {
                    std::ptr::addr_of_mut!((*construction.source).commit_stage_wall_nanos)
                        .write(std::array::from_fn(|_| AtomicU64::new(0)));
                    construction.initialized[130] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(130);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).corrupt_joined_values)
                        .write(RwLock::new(self.corrupt_joined_values.read().clone()));
                    construction.initialized[131] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(131);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).resource_owner).write(false);
                    construction.initialized[132] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(132);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).handle_role)
                        .write(super::HandleRole::Internal);
                    construction.initialized[133] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(133);
                }

                {
                    std::ptr::addr_of_mut!(
                        (*construction.source).trigger_deadlock_timeout_override
                    )
                    .write(Mutex::new(*self.trigger_deadlock_timeout_override.lock()));
                    construction.initialized[80] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(80);
                }

                {
                    std::ptr::addr_of_mut!((*construction.source).finalization)
                        .write(Arc::clone(&self.finalization));
                    construction.initialized[135] = true;
                    #[cfg(all(test, feature = "test-seams"))]
                    super::r1_database_field_for_test(135);
                }
            }
            construction.initialized.fill(false);
            unsafe { storage.assume_init() }
        }
    }
}

fn parse_cron_interval_ms(input: &str) -> Result<u64> {
    let mut parts = input.split_whitespace();
    let amount = parts
        .next()
        .ok_or_else(|| Error::PlanError("cron interval is missing amount".to_string()))?
        .parse::<u64>()
        .map_err(|err| Error::PlanError(format!("invalid cron interval amount: {err}")))?;
    let unit = parts
        .next()
        .ok_or_else(|| Error::PlanError("cron interval is missing unit".to_string()))?
        .to_ascii_lowercase();
    if parts.next().is_some() {
        return Err(Error::PlanError(format!(
            "unsupported cron interval: {input}"
        )));
    }
    let multiplier = match unit.as_str() {
        "millisecond" | "milliseconds" => 1,
        "second" | "seconds" => 1_000,
        "minute" | "minutes" => 60_000,
        "hour" | "hours" => 3_600_000,
        other => {
            return Err(Error::PlanError(format!(
                "unsupported cron interval unit: {other}"
            )));
        }
    };
    amount
        .checked_mul(multiplier)
        .filter(|ms| *ms > 0)
        .ok_or_else(|| Error::PlanError("cron interval must be positive".to_string()))
}

fn parse_cron_policy(
    policy: Option<&str>,
    within_seconds: Option<u32>,
) -> Result<CronMissedPolicy> {
    match policy
        .unwrap_or("skip-and-audit")
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "skip-and-audit" => Ok(CronMissedPolicy::SkipAndAudit),
        "catch-up" => Ok(CronMissedPolicy::CatchUp {
            within_ms: u64::from(within_seconds.unwrap_or(0)).saturating_mul(1_000),
        }),
        "fail-loud" => Ok(CronMissedPolicy::FailLoud),
        other => Err(Error::PlanError(format!(
            "unsupported missed tick policy: {other}"
        ))),
    }
}

fn compute_due_decision(schedule: &CronSchedule, now: u64) -> DueDecision {
    if now < schedule.next_fire_at_ms {
        return DueDecision {
            fire_count: 0,
            next_fire_at_ms: schedule.next_fire_at_ms,
            missed_skipped: 0,
            caught_up: None,
            fail_loud: None,
        };
    }
    let ticks_due = ((now - schedule.next_fire_at_ms) / schedule.every_ms) + 1;
    let next_fire_at_ms = schedule
        .next_fire_at_ms
        .saturating_add(ticks_due.saturating_mul(schedule.every_ms));
    match schedule.policy {
        CronMissedPolicy::SkipAndAudit => DueDecision {
            fire_count: 1,
            next_fire_at_ms,
            missed_skipped: ticks_due.saturating_sub(1),
            caught_up: None,
            fail_loud: None,
        },
        CronMissedPolicy::CatchUp { within_ms } => {
            let cutoff = now.saturating_sub(within_ms);
            let fire_count = (0..ticks_due)
                .map(|offset| schedule.next_fire_at_ms + offset * schedule.every_ms)
                .filter(|scheduled_at| *scheduled_at >= cutoff)
                .count() as u64;
            DueDecision {
                fire_count,
                next_fire_at_ms,
                missed_skipped: 0,
                caught_up: (fire_count > 1).then_some(fire_count.min(u64::from(u32::MAX)) as u32),
                fail_loud: None,
            }
        }
        CronMissedPolicy::FailLoud if ticks_due > 1 => {
            let ticks = ticks_due.min(u64::from(u32::MAX)) as u32;
            DueDecision {
                fire_count: 0,
                next_fire_at_ms,
                missed_skipped: 0,
                caught_up: None,
                fail_loud: Some(Error::MissedTicksExceeded {
                    ticks,
                    policy: "fail-loud".to_string(),
                }),
            }
        }
        CronMissedPolicy::FailLoud => DueDecision {
            fire_count: 1,
            next_fire_at_ms,
            missed_skipped: 0,
            caught_up: None,
            fail_loud: None,
        },
    }
}

fn now_millis() -> u64 {
    contextdb_core::Wallclock::now().0
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
mod tests {
    use super::*;

    /// Every hand-built `Database` (the cron worker handle here, and the two
    /// initializers in `database.rs`) must write each declared field exactly
    /// once and under exactly the cfg gate the field is declared with. A field
    /// that exists in a build but is skipped by an initializer is left
    /// uninitialized behind `assume_init`, which is undefined behaviour: in
    /// plain `cargo test` the cron worker handle used to segfault this way.
    /// This check reads the sources so the mismatch fails here, by name,
    /// instead of crashing whichever test first drops the handle.
    fn assert_every_hand_built_database_writes_each_field_under_its_declared_gate() {
        fn gate_before(lines: &[&str], index: usize) -> Option<String> {
            index
                .checked_sub(1)
                .map(|previous| lines[previous].trim())
                .filter(|previous| previous.starts_with("#[cfg("))
                .map(str::to_string)
        }
        fn declared_fields(source: &str) -> Vec<(String, Option<String>)> {
            let lines: Vec<&str> = source.lines().collect();
            let start = lines
                .iter()
                .position(|line| *line == "pub struct Database {")
                .expect("the Database declaration");
            let mut fields = Vec::new();
            for (offset, line) in lines[start + 1..].iter().enumerate() {
                if *line == "}" {
                    break;
                }
                let trimmed = line.trim();
                if trimmed.starts_with("//") || trimmed.starts_with('#') {
                    continue;
                }
                let declaration = trimmed
                    .strip_prefix("pub(crate) ")
                    .or_else(|| trimmed.strip_prefix("pub(super) "))
                    .or_else(|| trimmed.strip_prefix("pub "))
                    .unwrap_or(trimmed);
                let Some((name, rest)) = declaration.split_once(':') else {
                    continue;
                };
                if !rest.starts_with(':')
                    && !name.is_empty()
                    && name
                        .chars()
                        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
                {
                    let mut index = start + 1 + offset;
                    let mut gate = None;
                    while index > start + 1 {
                        let previous = lines[index - 1].trim();
                        if previous.starts_with("#[cfg(") {
                            gate = Some(previous.to_string());
                            break;
                        }
                        if !(previous.starts_with("#[") || previous.starts_with("//")) {
                            break;
                        }
                        index -= 1;
                    }
                    fields.push((name.to_string(), gate));
                }
            }
            fields
        }
        fn written_fields(source: &str, anchor: &str) -> Vec<Vec<(String, Option<String>)>> {
            const MARK: &str = "(*construction.source).";
            let lines: Vec<&str> = source.lines().collect();
            let mut sites = Vec::new();
            for start in (0..lines.len()).filter(|index| lines[*index].trim() == anchor) {
                let mut writes = Vec::new();
                for index in start..lines.len() {
                    if lines[index].contains("assume_init") {
                        break;
                    }
                    let Some(at) = lines[index].find(MARK) else {
                        continue;
                    };
                    let name: String = lines[index][at + MARK.len()..]
                        .chars()
                        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                        .collect();
                    let opener = (start..=index)
                        .rev()
                        .find(|candidate| lines[*candidate].trim() == "{")
                        .expect("each field write sits in its own block");
                    writes.push((name, gate_before(&lines, opener)));
                }
                sites.push(writes);
            }
            sites
        }
        let database = include_str!("../database.rs");
        let declared = declared_fields(database);
        assert!(declared.len() > 100, "parsed the Database declaration");
        let mut sites = written_fields(include_str!("cron.rs"), "let mut construction = super::DatabaseConstruction {");
        sites.extend(written_fields(database, "let mut construction = DatabaseConstruction {"));
        assert_eq!(sites.len(), 3, "the three hand-built Database initializers");
        for (site, writes) in sites.iter().enumerate() {
            let mut mismatches = Vec::new();
            for (field, gate) in &declared {
                let found: Vec<_> = writes.iter().filter(|(name, _)| name == field).collect();
                match found.as_slice() {
                    [(_, written)] if written == gate => {}
                    [] => mismatches.push(format!("{field}: never written")),
                    [(_, written)] => mismatches.push(format!(
                        "{field}: declared under {gate:?} but written under {written:?}"
                    )),
                    _ => mismatches.push(format!("{field}: written {} times", found.len())),
                }
            }
            for (name, _) in writes {
                if !declared.iter().any(|(field, _)| field == name) {
                    mismatches.push(format!("{name}: written but not declared"));
                }
            }
            assert!(
                mismatches.is_empty(),
                "hand-built Database initializer #{site} disagrees with the declaration:\n{}",
                mismatches.join("\n")
            );
        }
    }

    #[test]
    fn paused_manual_cron_guard_drop_does_not_restart_tickler() -> Result<()> {
        // This test's background handle is one of the hand-built initializers;
        // a skipped field there is undefined behaviour, so check them first.
        assert_every_hand_built_database_writes_each_field_under_its_declared_gate();
        let db = Database::open_memory();
        let first_pause = db.pause_cron_tickler_for_test();
        let second_pause = db.pause_cron_tickler_for_test();

        db.create_cron_schedule("paused_manual", "1 HOUR", "cb", None, None)?;
        db.register_cron_callback("cb", |_| Ok(()))?;

        assert_eq!(db.cron.pause_count.load(Ordering::SeqCst), 2);
        assert!(db.cron.runtime.lock().handle.is_none());

        drop(first_pause);

        assert_eq!(db.cron.pause_count.load(Ordering::SeqCst), 1);
        assert!(db.cron.runtime.lock().handle.is_none());

        drop(second_pause);

        assert_eq!(db.cron.pause_count.load(Ordering::SeqCst), 0);
        assert!(db.cron.runtime.lock().handle.is_none());

        db.start_cron_tickler_if_schedules_present();
        assert!(db.cron.runtime.lock().handle.is_some());

        db.close()
    }

    /// THE STOP-SERVING RULE ON A SCHEDULED JOB'S COMMIT.
    ///
    /// A scheduled job commits through the reserved-LSN door and publishes its
    /// sink events, trigger audits and commit event to memory only after that
    /// commit is durable. It did not arm the publication-interruption watch,
    /// so a panic in that window left the handle serving a memory image behind
    /// its own file. Now it stops serving, and a reopen recovers the complete
    /// committed state -- the row the job wrote included.
    #[test]
    fn a_scheduled_job_whose_publication_is_interrupted_stops_serving_until_reopened() {
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("cron-publication-interrupted.redb");
        let params = HashMap::new();
        let db = Database::open(&path).unwrap();
        db.execute(
            "CREATE TABLE cron_notes (id INTEGER PRIMARY KEY, body TEXT) SYNC OFF",
            &params,
        )
        .unwrap();
        db.execute("INSERT INTO cron_notes VALUES (1, 'one')", &params)
            .unwrap();

        let callback: CronCallback = Arc::new(|db: &Database| {
            db.execute(
                "INSERT INTO cron_notes VALUES (2, 'scheduled')",
                &HashMap::new(),
            )
            .map(|_| ())
        });
        LOCAL_COMMIT_STAGE_FAULT.with(|fault| {
            fault.set(Some(
                LocalCommitStageFaultPoint::AfterScheduledJobDurableCommit,
            ))
        });
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let fixture_operation_reader_1 = db
                .declared_reader_memory()
                .expect("admit the actual fixture reader");
            let mut fixture_operation_copy_1 =
                contextdb_engine::vector_read_memory::admission(fixture_operation_reader_1.clone());

            let _ = db.run_cron_callback_transaction(
                callback,
                &mut fixture_operation_copy_1,
                fixture_operation_reader_1.clone(),
            );
        }));
        let fired = LOCAL_COMMIT_STAGE_FAULT.with(|fault| fault.get().is_none());
        LOCAL_COMMIT_STAGE_FAULT.with(|fault| fault.set(None));
        assert!(
            fired,
            "the scheduled job never reached its own publication boundary"
        );
        assert!(
            outcome.is_err(),
            "the injected unwind must unwind through the scheduled job's publication"
        );
        for (what, result) in [
            (
                "a read",
                db.execute("SELECT id FROM cron_notes", &params).map(|_| ()),
            ),
            (
                "a write",
                db.execute("INSERT INTO cron_notes VALUES (9001, 'after')", &params)
                    .map(|_| ()),
            ),
            ("a new transaction", db.begin().map(|_| ())),
        ] {
            assert!(
                matches!(result, Err(Error::PublicationInterrupted)),
                "{what} on a handle whose scheduled job's publication was interrupted after the \
                 durable commit must be refused until a reopen, got {result:?}"
            );
        }
        drop(db);

        let reopened = Database::open(&path).unwrap();
        assert_eq!(
            reopened
                .execute("SELECT id FROM cron_notes ORDER BY id", &params)
                .unwrap()
                .rows
                .len(),
            2,
            "a reopen recovers the complete committed state, the scheduled row included"
        );
        reopened.close().unwrap();
    }
}

// Reserved scoped commit retains the original pair until terminal cleanup.
impl Database {
    fn run_cron_callback_transaction_owned(
        &self,
        host: &EngineDatabaseTransactions,
        store: &crate::persistent_store::OwnedPersistentCompositeStore,
        callback: CronCallback,
        vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
        read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<Lsn> {
        let events = &store.events;
        self.validate_owned_cron_store(events, host, store)?;
        store.with_owned_terminal_apply_scope(|| {
            let vector_copy_owner = std::cell::RefCell::new(vector_copy_owner);

            let _callback_thread = self.cron.enter_callback_thread_scope();
            let tx = self.begin_for_internal_write()?;
            let mut event_registration = None::<event_bus::OwnedEventStageRegistration>;
            let original_identity = std::cell::Cell::new(None);
            let pending_trigger_audits = std::cell::RefCell::new(Vec::new());
            let pending_trigger_active_guards =
                std::cell::RefCell::new(Vec::<TriggerCallbackThreadGuard>::new());
            let mut committed_trigger_audit_entries = Vec::new();
            // Cron callbacks are arbitrary user code and can stage vector writes
            // while the tx manager's commit mutex is held. Acquire current vector
            // schema refs before entering that mutex so DDL keeps the normal
            // schema-gate -> commit-mutex lock order.
            let mut vector_schema_guard = Some(self.vector_schema_read_many(
                self.vector_store_schema_refs(&mut vector_copy_owner.borrow_mut())?,
            ));
            let delete_release_bytes =
                std::cell::RefCell::new(None::<event_bus::OwnedDeleteReleaseBytes>);
            // Armed just before the durable apply; see `PublicationInterruptionWatch`
            // in `database.rs`. A scheduled job's commit is durable the moment the
            // manager applies it, and its sink events, trigger audits and commit
            // event reach memory only afterwards -- so an unwind in between leaves
            // this handle behind its own durable image, exactly as it does on the
            // local and received doors, and it must stop serving until reopened.
            let publication_watch =
                std::cell::RefCell::new(None::<super::PublicationInterruptionWatch>);
            let result = host.commit_reserved_terminal_scoped(
                tx,
                read_memory.clone(),
                |scope, reserved_lsn| {
                    let callback_result = CRON_LSN_OVERRIDE.with(|slot| {
                        let prior_lsn = slot.replace(Some(reserved_lsn));
                        let prior_tx = CRON_CALLBACK_TX.with(|tx_slot| tx_slot.replace(Some(tx)));
                        let this_db = self.identity();
                        let prior_db =
                            CRON_CALLBACK_DB.with(|db_slot| db_slot.replace(Some(this_db)));
                        let gate_id = self.vector_schema_gate_id();
                        let prior_gate = CRON_CALLBACK_VECTOR_SCHEMA_GATE
                            .with(|gate_slot| gate_slot.replace(Some(gate_id)));
                        let prior_active = CRON_CALLBACK_ACTIVE.with(|active| active.replace(true));
                        // Keep the original external CoreResult catch boundary. Only
                        // its outcome is lifted; temporary preparation remains outside it.
                        let mut result = catch_unwind(AssertUnwindSafe(|| callback(self)))
                            .map(|result| result.map_err(TemporaryFailure::from_error));
                        if let Ok(Ok(())) = &result {
                            if let Err(error) = self
                                .prepare_active_trigger_write_set_for_dispatch_owned(
                                    scope,
                                    tx,
                                    &mut vector_copy_owner.borrow_mut(),
                                    read_memory.clone(),
                                )
                            {
                                result = Ok(Err(error));
                            }
                        }
                        if let Ok(Ok(())) = &result {
                            match self.dispatch_triggers_for_tx_owned(
                                scope,
                                tx,
                                &mut vector_copy_owner.borrow_mut(),
                                read_memory.clone(),
                            ) {
                                Ok(outcome) => {
                                    *pending_trigger_audits.borrow_mut() = outcome.pending;
                                    pending_trigger_active_guards
                                        .borrow_mut()
                                        .extend(outcome.active_guards);
                                }
                                Err((error, active_guards)) => {
                                    pending_trigger_active_guards
                                        .borrow_mut()
                                        .extend(active_guards);
                                    result = Ok(Err(error));
                                }
                            }
                        }
                        if let Ok(Ok(())) = &result {
                            if let Err(error) = self
                                .prepare_active_trigger_write_set_for_dispatch_owned(
                                    scope,
                                    tx,
                                    &mut vector_copy_owner.borrow_mut(),
                                    read_memory.clone(),
                                )
                            {
                                result = Ok(Err(error));
                            }
                        }
                        CRON_CALLBACK_ACTIVE.with(|active| active.set(prior_active));
                        CRON_CALLBACK_DB.with(|db_slot| db_slot.set(prior_db));
                        CRON_CALLBACK_VECTOR_SCHEMA_GATE
                            .with(|gate_slot| gate_slot.set(prior_gate));
                        CRON_CALLBACK_TX.with(|tx_slot| tx_slot.set(prior_tx));
                        slot.replace(prior_lsn);
                        result
                    });
                    match callback_result {
                        // The paired temporary door supplies the authentic identity
                        // even when no metadata node exists. It restores the SAME
                        // original source before the reserved host detaches it.
                        Ok(Ok(())) => scope.prepare_temporary(
                            tx,
                            |peer| Ok(peer.identity),
                            |_, _, identity| {
                                original_identity.set(Some(identity));
                                Ok(())
                            },
                        ),
                        Ok(Err(error)) => Err(error),
                        Err(payload) => Err(TemporaryFailure::from_error(Error::Other(format!(
                            "cron callback panicked: {}",
                            panic_payload_to_string(payload)
                        )))),
                    }
                },
                |guard, peer| {
                    if !guard.read().is_empty() {
                        debug_assert!(
                            guard.with_legacy(|ws| !Self::write_set_touches_vector_schema(ws)
                                || vector_schema_guard.is_some())
                        );
                        self.rewrite_txid_placeholders_in_prepared_write_set(tx, guard)?;
                        let _validation = self.commit_validate_in_prepared_write_set(
                            tx,
                            guard,
                            peer,
                            &mut vector_copy_owner.borrow_mut(),
                            read_memory.clone(),
                        )?;
                        guard.with_legacy(|ws| {
                            self.plugin.pre_commit(ws, CommitSource::AutoCommit)
                        })?;
                        let prepared_sink_events = self
                            .prepare_owned_sink_events_in_prepared_write_set(
                                events,
                                host,
                                guard,
                                &[],
                                read_memory.clone(),
                            )?;
                        if !prepared_sink_events.is_empty() {
                            self.stage_next_action_id_in_prepared_write_set(
                                guard,
                                self.event_bus.next_action_id(),
                            )?;
                        }
                        let commit_lsn = *guard.read().commit_lsn;
                        let lsn = commit_lsn.ok_or_else(|| {
                            Error::Other("native cron preparation requires its reserved LSN".into())
                        })?;
                        let identity = original_identity.get().ok_or_else(|| {
                            Error::Other(
                                "native cron preparation requires its original terminal identity"
                                    .into(),
                            )
                        })?;
                        let registration = events.stage_owned_commit_events(
                            lsn,
                            event_bus::OwnedCommitEventStage {
                                identity,
                                events: prepared_sink_events,
                                ddl: None,
                                schema: None,
                            },
                        )?;
                        store.bind_owned_registered_event_apply(&registration)?;
                        event_registration = Some(registration);
                        if let Some(lsn) = commit_lsn {
                            let pending = pending_trigger_audits.borrow();
                            committed_trigger_audit_entries = guard.with_legacy(|ws| {
                                self.committed_trigger_audits_for_pending(&pending, ws, lsn)
                            });
                            self.stage_trigger_audits_for_persistence(
                                lsn,
                                &committed_trigger_audit_entries,
                            );
                        }
                        *delete_release_bytes.borrow_mut() = Some(
                            self.prepare_owned_delete_release_bytes_in_prepared_write_set(
                                events,
                                host,
                                guard,
                                &mut vector_copy_owner.borrow_mut(),
                                read_memory.clone(),
                            )?,
                        );
                    }
                    *publication_watch.borrow_mut() = Some(
                        super::PublicationInterruptionWatch::arm(&self.publication_interrupted),
                    );
                    Ok(())
                },
            );

            match result {
                Ok((lsn, terminal)) => {
                    // Permanent detach already moved the original journal into terminal.
                    // Observe the immutable original source while both owners remain held.
                    terminal.with_legacy(|ws| {
                        if !ws.is_empty() {
                            #[cfg(test)]
                            super::take_local_commit_stage_fault_for_test(
                                super::LocalCommitStageFaultPoint::AfterScheduledJobDurableCommit,
                            );
                            assert_eq!(
                                terminal.identity(),
                                original_identity
                                    .get()
                                    .expect("original native cron identity"),
                                "native cron publishes the same original terminal",
                            );
                            self.settle_owned_delete_release_bytes(
                                delete_release_bytes
                                    .borrow_mut()
                                    .take()
                                    .expect("prepared native cron delete release"),
                            );
                            drop(vector_schema_guard.take());
                            self.plugin.post_commit(ws, CommitSource::AutoCommit);
                            let staged = event_registration
                                .take()
                                .expect("prepared native cron event registration")
                                .take()
                                .expect("same registered native cron events");
                            let stage = staged.try_into_inner().unwrap_or_else(|_| {
                                panic!("native cron event stage still borrowed after durable apply")
                            });
                            events.publish_owned_sink_events_to_memory(stage.events);
                            self.append_trigger_audits_to_memory(committed_trigger_audit_entries);
                            self.publish_commit_event_if_subscribers(
                                ws,
                                CommitSource::AutoCommit,
                                lsn,
                            );
                        }
                    });
                    if let Some(watch) = publication_watch.borrow_mut().take() {
                        watch.completed();
                    }
                    drop(terminal);
                    Ok(lsn)
                }
                Err(failure) => {
                    if let Some(watch) = publication_watch.borrow_mut().take() {
                        watch.completed();
                    }
                    let (error, pair) = failure.into_parts();
                    // A retained original must not take a newly installed same-key
                    // source or journal. Only None reaches qualified paired rollback.
                    if let Some(lsn) = pair.as_ref().and_then(|pair| pair.commit_lsn()) {
                        self.discard_staged_trigger_audits_for_persistence(lsn);
                    }
                    drop(event_registration.take());
                    drop(delete_release_bytes.borrow_mut().take());
                    if pair.is_none() {
                        let _ = host.rollback_terminal(tx).map(|terminal| {
                            self.remove_original_terminal_stages(terminal.identity().original_tx);
                            drop(terminal);
                        });
                    }
                    let pending = pending_trigger_audits.borrow();
                    self.append_rolled_back_trigger_audits(&pending, tx, &error.to_string())?;
                    drop(pair);
                    Err(error)
                }
            }
        })?
    }
}
