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
    pause_count: AtomicU64,
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
            pause_count: AtomicU64::new(0),
        }
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

impl Database {
    pub(super) fn load_cron_state_from_persistence(&self) -> Result<()> {
        let Some(persistence) = self.persistence.as_ref() else {
            return Ok(());
        };
        if let Some(schedules) =
            persistence.load_config_value::<Vec<CronSchedule>>(CRON_SCHEDULES_CONFIG_KEY)?
        {
            let mut stored = self.cron.schedules.lock();
            stored.clear();
            stored.extend(
                schedules
                    .into_iter()
                    .map(|schedule| (schedule.name.clone(), schedule)),
            );
        }
        if let Some(entries) =
            persistence.load_config_value::<Vec<CronAuditEntry>>(CRON_AUDIT_CONFIG_KEY)?
        {
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
        }
        self.cron.waiters.notify_all();
        Ok(())
    }

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

    pub fn cron_run_due_now_for_test(&self) -> Result<u64> {
        let _operation = self.open_operation()?;
        self.dispatch_due_cron_schedules(true)
    }

    pub fn pause_cron_tickler_for_test(&self) -> CronPauseGuard {
        let _operation = self.assert_open_operation();
        self.cron.pause_count.fetch_add(1, Ordering::SeqCst);
        self.cron.waiters.notify_all();
        CronPauseGuard {
            cron: self.cron.clone(),
        }
    }

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
        let mut runtime = self.cron.runtime.lock();
        if runtime.handle.is_some() {
            return;
        }
        runtime.shutdown.store(false, Ordering::SeqCst);
        let shutdown = runtime.shutdown.clone();
        let db = self.worker_handle_for_background();
        let handle = thread::spawn(move || db.cron_tickler_loop(shutdown));
        runtime.handle = Some(handle);
    }

    fn cron_tickler_loop(self, shutdown: Arc<AtomicBool>) {
        while !shutdown.load(Ordering::SeqCst) {
            if self.cron.pause_count.load(Ordering::SeqCst) == 0 {
                let _ = self.dispatch_due_cron_schedules(false);
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

    fn dispatch_due_cron_schedules(&self, manual: bool) -> Result<u64> {
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
            match self.run_cron_callback_transaction(callback.clone()) {
                Ok(lsn) => {
                    *successful_fires += 1;
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

    fn run_cron_callback_transaction(&self, callback: CronCallback) -> Result<Lsn> {
        let tx = self.begin();
        let result = self.tx_mgr.commit_with_reserved_lsn_callback(
            tx,
            |reserved_lsn| {
                let prior_session = {
                    let mut session = self.session_tx.lock();
                    let prior = *session;
                    *session = Some(tx);
                    prior
                };
                let callback_result = CRON_LSN_OVERRIDE.with(|slot| {
                    let prior_lsn = slot.replace(Some(reserved_lsn));
                    let prior_tx = CRON_CALLBACK_TX.with(|tx_slot| tx_slot.replace(Some(tx)));
                    let this_db = self as *const Self as usize;
                    let prior_db = CRON_CALLBACK_DB.with(|db_slot| db_slot.replace(Some(this_db)));
                    let prior_active = CRON_CALLBACK_ACTIVE.with(|active| active.replace(true));
                    let result = catch_unwind(AssertUnwindSafe(|| callback(self)));
                    CRON_CALLBACK_ACTIVE.with(|active| active.set(prior_active));
                    CRON_CALLBACK_DB.with(|db_slot| db_slot.set(prior_db));
                    CRON_CALLBACK_TX.with(|tx_slot| tx_slot.set(prior_tx));
                    slot.replace(prior_lsn);
                    result
                });
                *self.session_tx.lock() = prior_session;
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
                    self.validate_foreign_keys_in_write_set(ws)?;
                    self.plugin.pre_commit(ws, CommitSource::AutoCommit)?;
                }
                Ok(())
            },
        );

        match result {
            Ok((lsn, ws)) => {
                if !ws.is_empty() {
                    self.release_delete_allocations(&ws);
                    self.plugin.post_commit(&ws, CommitSource::AutoCommit);
                    self.publish_commit_event_if_subscribers(&ws, CommitSource::AutoCommit, lsn);
                }
                Ok(lsn)
            }
            Err(failure) => {
                if let Some(ws) = failure.write_set {
                    self.release_insert_allocations(&ws);
                } else {
                    let _ = self.rollback(tx);
                }
                Err(failure.error)
            }
        }
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
        persistence.flush_config_value(CRON_SCHEDULES_CONFIG_KEY, &schedules)
    }

    fn persist_cron_audit(&self) -> Result<()> {
        let Some(persistence) = self.persistence.as_ref() else {
            return Ok(());
        };
        let audit = self.cron.audit.lock().iter().cloned().collect::<Vec<_>>();
        persistence.flush_config_value(CRON_AUDIT_CONFIG_KEY, &audit)
    }

    fn worker_handle_for_background(&self) -> Database {
        Database {
            tx_mgr: self.tx_mgr.clone(),
            relational_store: self.relational_store.clone(),
            graph_store: self.graph_store.clone(),
            vector_store: self.vector_store.clone(),
            change_log: self.change_log.clone(),
            ddl_log: self.ddl_log.clone(),
            persistence: self.persistence.clone(),
            open_registry_path: Mutex::new(None),
            operation_gate: RwLock::new(()),
            apply_phase_pause: self.apply_phase_pause.clone(),
            relational: MemRelationalExecutor::new(
                self.relational_store.clone(),
                self.tx_mgr.clone(),
            ),
            graph: MemGraphExecutor::new(self.graph_store.clone(), self.tx_mgr.clone()),
            vector: MemVectorExecutor::new_with_accountant(
                self.vector_store.clone(),
                self.tx_mgr.clone(),
                Arc::new(OnceLock::new()),
                self.accountant.clone(),
            ),
            session_tx: Mutex::new(None),
            instance_id: self.instance_id,
            plugin: self.plugin.clone(),
            access: AccessConstraints::default(),
            accountant: self.accountant.clone(),
            conflict_policies: RwLock::new(self.conflict_policies.read().clone()),
            subscriptions: self.subscriptions.clone(),
            pruning_runtime: Mutex::new(PruningRuntime::new()),
            pruning_guard: self.pruning_guard.clone(),
            cron: self.cron.clone(),
            disk_limit: AtomicU64::new(self.disk_limit.load(Ordering::SeqCst)),
            disk_limit_startup_ceiling: AtomicU64::new(
                self.disk_limit_startup_ceiling.load(Ordering::SeqCst),
            ),
            sync_watermark: self.sync_watermark.clone(),
            closed: AtomicBool::new(false),
            rows_examined: AtomicU64::new(0),
            statement_cache: RwLock::new(HashMap::new()),
            rank_formula_cache: RwLock::new(self.rank_formula_cache.read().clone()),
            acl_grant_cache: RwLock::new(HashMap::new()),
            rank_policy_eval_count: AtomicU64::new(0),
            rank_policy_formula_parse_count: AtomicU64::new(0),
            corrupt_joined_values: RwLock::new(self.corrupt_joined_values.read().clone()),
            resource_owner: false,
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
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
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
