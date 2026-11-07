#![expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
use super::*;

#[cfg(any(test, feature = "test-seams"))]
thread_local! {
    static DRIVE_ITERATIONS: Cell<Option<usize>> = const { Cell::new(None) };
    static DRIVE_IDENTITY: Cell<Option<u64>> = const { Cell::new(None) };
}
pub(super) fn driving() -> bool {
    DRIVE_ITERATIONS.with(|value| value.get().is_some())
}
pub(super) fn batch_size() -> usize {
    if driving() { 1 } else { SINK_DISPATCH_BATCH }
}
pub(super) fn permits(id: u64) -> bool {
    DRIVE_IDENTITY.with(|value| value.get().is_none_or(|selected| selected == id))
}
struct DriveThread;
impl DriveThread {
    fn enter(identity: u64) -> Self {
        DRIVE_ITERATIONS.with(|value| {
            assert!(value.get().is_none());
            value.set(Some(0));
        });
        DRIVE_IDENTITY.with(|value| value.set(Some(identity)));
        Self
    }
}
impl Drop for DriveThread {
    fn drop(&mut self) {
        DRIVE_ITERATIONS.with(|value| value.set(None));
        DRIVE_IDENTITY.with(|value| value.set(None));
    }
}

#[derive(Default)]
struct DispatchState {
    paused: bool,
    active_iterations: usize,
}

#[derive(Default)]
pub(super) struct SinkDispatchControlForTest {
    state: Mutex<DispatchState>,
    pub(super) changed: Condvar,
    #[cfg(test)]
    pub(super) acknowledgment_pause: Mutex<Option<Arc<ApplyPhasePause>>>,
}

#[cfg(test)]
#[path = "sink_dispatch_drive/tests.rs"]
mod tests;

pub(super) struct DispatchIteration {
    control: Option<Arc<SinkDispatchControlForTest>>,
}

impl Drop for DispatchIteration {
    fn drop(&mut self) {
        if let Some(control) = &self.control {
            control.state.lock().active_iterations -= 1;
            control.changed.notify_all();
        }
    }
}

impl SinkDispatchControlForTest {
    pub(super) fn notify_shutdown(&self) {
        let _state = self.state.lock();
        self.changed.notify_all();
    }

    pub(super) fn begin_iteration(
        self: &Arc<Self>,
        shutdown: &AtomicBool,
    ) -> Option<DispatchIteration> {
        if let Some(iterations) = DRIVE_ITERATIONS.with(Cell::get) {
            if iterations != 0 {
                return None;
            }
            DRIVE_ITERATIONS.with(|value| value.set(Some(1)));
            return Some(DispatchIteration { control: None });
        }
        let mut state = self.state.lock();
        while state.paused && !shutdown.load(Ordering::SeqCst) {
            self.changed.wait(&mut state);
        }
        if shutdown.load(Ordering::SeqCst) {
            return None;
        }
        state.active_iterations += 1;
        Some(DispatchIteration {
            control: Some(Arc::clone(self)),
        })
    }
}

#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SinkDispatchIdentity {
    pub sink: String,
    pub action_id: u64,
    pub at_lsn: Lsn,
}

#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct SinkDispatchPrefix {
    generation: uuid::Uuid,
    committed_lsn: Lsn,
    entries: Vec<SinkDispatchIdentity>,
    error: Option<String>,
}

impl SinkDispatchPrefix {
    #[doc(hidden)]
    pub fn committed_lsn_for_test(&self) -> Lsn {
        self.committed_lsn
    }

    #[doc(hidden)]
    pub fn entries_for_test(&self) -> &[SinkDispatchIdentity] {
        &self.entries
    }
}

#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SinkDispatchRetryIdentity {
    pub identity: SinkDispatchIdentity,
    pub retry_at: Wallclock,
}

#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct SinkDispatchReceipt {
    pub committed_lsn: Lsn,
    pub processed_lsn: Lsn,
    pub committed_positions: Vec<SinkDispatchIdentity>,
    pub processed_positions: Vec<SinkDispatchIdentity>,
    pub pending_retries: Vec<SinkDispatchRetryIdentity>,
    pub blocked_positions: Vec<SinkDispatchIdentity>,
}

impl SinkDispatchReceipt {
    #[doc(hidden)]
    pub fn is_complete_for_test(&self) -> bool {
        self.pending_retries.is_empty() && self.blocked_positions.is_empty()
    }
}

#[derive(Default)]
struct DriveProgress {
    retries: HashMap<SinkDispatchIdentity, (Wallclock, SinkDispatchPrefix)>,
    settled: HashSet<SinkDispatchIdentity>,
}

#[doc(hidden)]
pub struct SinkDispatchDriveForTest<'a> {
    database: &'a Database,
    generation: uuid::Uuid,
    progress: Mutex<DriveProgress>,
}

impl Drop for SinkDispatchDriveForTest<'_> {
    fn drop(&mut self) {
        let control = &self.database.event_bus.dispatch_drive_for_test;
        control.state.lock().paused = false;
        control.changed.notify_all();
        self.database.event_bus.waiters.notify_all();
    }
}

#[cfg(any(test, feature = "test-seams"))]
impl Database {
    #[doc(hidden)]
    pub fn sink_dispatch_drive_for_test(&self) -> Result<SinkDispatchDriveForTest<'_>> {
        if self.event_bus.callback_active_on_current_thread() {
            return Err(Error::Other(
                "sink dispatch cannot quiesce inside its callback".into(),
            ));
        }
        let control = &self.event_bus.dispatch_drive_for_test;
        let mut state = control.state.lock();
        while state.paused {
            control.changed.wait(&mut state);
        }
        state.paused = true;
        while state.active_iterations != 0 {
            control.changed.wait(&mut state);
        }
        Ok(SinkDispatchDriveForTest {
            database: self,
            generation: uuid::Uuid::new_v4(),
            progress: Mutex::default(),
        })
    }
}

impl SinkDispatchDriveForTest<'_> {
    #[doc(hidden)]
    pub fn committed_prefix_for_test(&self) -> SinkDispatchPrefix {
        let bus = &self.database.event_bus;
        let mut mutations = bus.queue_mutation_state.lock();
        while mutations.active_mutations != 0 {
            bus.queue_mutation_waiters.wait(&mut mutations);
        }
        // Queue publication leases span durability through memory publication.
        // Holding their predicate here prevents a new event commit from passing
        // that boundary until its prefix membership has been captured.
        let committed_lsn = self.database.tx_mgr.current_lsn();
        let captured = self.pending_entries();
        let (entries, error) = match captured {
            Ok(entries) => (
                entries
                    .into_iter()
                    .filter(|(_, entry)| entry.event.at_lsn <= committed_lsn)
                    .map(|(sink, entry)| SinkDispatchIdentity {
                        sink,
                        action_id: entry.id,
                        at_lsn: entry.event.at_lsn,
                    })
                    .collect(),
                None,
            ),
            Err(error) => (Vec::new(), Some(error.to_string())),
        };
        SinkDispatchPrefix {
            generation: self.generation,
            committed_lsn,
            entries,
            error,
        }
    }

    fn pending_entries(&self) -> Result<Vec<(String, SinkQueueEntry)>> {
        let mut entries = Vec::new();
        let reader = self.database.ordinary_read_child()?;
        let _decode = crate::read_image_memory::ImageMemory::operation_reader(
            self.database.accountant.clone(),
            reader.into_owner(),
        )?
        .enter();
        if let Some(persistence) = &self.database.persistence {
            let sinks = self
                .database
                .event_bus
                .definitions
                .lock()
                .sinks
                .keys()
                .cloned()
                .collect::<Vec<_>>();
            for sink in sinks {
                entries.extend(
                    persistence
                        .load_sink_queue::<SinkQueueEntry>(&sink)?
                        .into_iter()
                        .map(|entry| (sink.clone(), entry)),
                );
            }
        } else if let DatabaseTransactionHost::Owned(handle) = &self.database.tx_mgr {
            handle.with_host(|host| {
                for (sink, queue) in &host.events.data().queues.lock().values {
                    entries.extend(
                        queue
                            .entries
                            .iter()
                            .map(|entry| (sink.clone(), entry.entry.clone())),
                    );
                }
            });
        } else {
            for (sink, queue) in self.database.event_bus.queues.lock().iter() {
                entries.extend(
                    queue
                        .entries
                        .iter()
                        .map(|entry| (sink.clone(), entry.clone())),
                );
            }
        }
        entries.sort_by(|a, b| (&a.0, a.1.id).cmp(&(&b.0, b.1.id)));
        Ok(entries)
    }

    fn drive_iteration(&self, sink: &str, identity: u64) -> Result<()> {
        let persistence = self.database.persistence.as_ref().ok_or_else(|| {
            Error::SinkRegistrationRequiresOpenedDatabase {
                sink: sink.to_owned(),
            }
        })?;
        let _drive_thread = DriveThread::enter(identity);
        let shutdown = Arc::new(AtomicBool::new(false));
        if let DatabaseTransactionHost::Owned(handle) = &self.database.tx_mgr {
            let worker = handle.with_host(|host| {
                OwnedEventBusWorker::try_new(
                    host.events.clone(),
                    sink,
                    persistence.clone(),
                    self.database.tx_mgr.clone(),
                    &self.database.finalization.accountant,
                )
            })?;
            worker.dispatch_loop(shutdown);
        } else {
            EventBusWorker {
                sink: sink.to_owned(),
                event_bus: self.database.event_bus.clone(),
                persistence: persistence.clone(),
                tx_mgr: self.database.tx_mgr.clone(),
            }
            .dispatch_loop(shutdown);
        }
        Ok(())
    }

    #[doc(hidden)]
    pub fn drive_through_for_test(
        &self,
        prefix: SinkDispatchPrefix,
    ) -> Result<SinkDispatchReceipt> {
        self.drive(prefix, None)
    }

    #[doc(hidden)]
    pub fn drive_retry_for_test(&self, retry: SinkDispatchIdentity) -> Result<SinkDispatchReceipt> {
        let (due, prefix) = self
            .progress
            .lock()
            .retries
            .get(&retry)
            .cloned()
            .ok_or_else(|| Error::Other("sink action has no pending retry on this drive".into()))?;
        if Wallclock::now().0 < due.0 {
            return self.drive(prefix, None);
        }
        self.drive(prefix, Some(retry))
    }

    fn drive(
        &self,
        prefix: SinkDispatchPrefix,
        granted_retry: Option<SinkDispatchIdentity>,
    ) -> Result<SinkDispatchReceipt> {
        if prefix.generation != self.generation {
            return Err(Error::Other(
                "sink prefix belongs to another drive generation".into(),
            ));
        }
        if let Some(error) = &prefix.error {
            return Err(Error::Other(error.clone().into()));
        }
        let mut progress = self.progress.lock();
        let mut processed = Vec::new();
        let mut blocked = Vec::new();
        let mut blocked_sinks = HashSet::new();
        for identity in &prefix.entries {
            if progress.settled.contains(identity) {
                processed.push(identity.clone());
                continue;
            }
            if blocked_sinks.contains(&identity.sink)
                || (progress.retries.contains_key(identity)
                    && granted_retry.as_ref() != Some(identity))
            {
                blocked.push(identity.clone());
                blocked_sinks.insert(identity.sink.clone());
                continue;
            }
            let current = self.pending_entries()?;
            let Some((_, entry)) = current
                .iter()
                .find(|(sink, entry)| sink == &identity.sink && entry.id == identity.action_id)
            else {
                processed.push(identity.clone());
                progress.retries.remove(identity);
                progress.settled.insert(identity.clone());
                continue;
            };
            let visible = self
                .database
                .event_bus
                .callbacks
                .read()
                .get(&identity.sink)
                .is_some_and(|registration| event_visible_for_access(&registration.access, entry));
            if !visible {
                blocked.push(identity.clone());
                blocked_sinks.insert(identity.sink.clone());
                continue;
            }
            let before_attempts = entry.attempts;
            // A cold queue needs refill iterations before its one real attempt.
            // The finite captured population bounds the number of window advances.
            for _ in 0..=current.len().saturating_add(1) {
                self.drive_iteration(&identity.sink, identity.action_id)?;
                let after = self.pending_entries()?;
                let remaining = after
                    .iter()
                    .find(|(sink, entry)| sink == &identity.sink && entry.id == identity.action_id);
                match remaining {
                    None => {
                        processed.push(identity.clone());
                        progress.retries.remove(identity);
                        progress.settled.insert(identity.clone());
                        break;
                    }
                    Some((_, entry)) if entry.attempts != before_attempts => {
                        let delay = retry_backoff(entry.attempts & ATTEMPT_COUNT_MASK);
                        let delay_ms = u64::try_from(delay.as_millis()).unwrap_or(u64::MAX);
                        progress.retries.insert(
                            identity.clone(),
                            (
                                Wallclock::from_raw_wire(
                                    Wallclock::now().0.saturating_add(delay_ms),
                                ),
                                prefix.clone(),
                            ),
                        );
                        blocked_sinks.insert(identity.sink.clone());
                        break;
                    }
                    _ => {}
                }
            }
            if !progress.settled.contains(identity) && !progress.retries.contains_key(identity) {
                blocked.push(identity.clone());
                blocked_sinks.insert(identity.sink.clone());
            }
        }
        let pending_retries = prefix
            .entries
            .iter()
            .filter_map(|identity| {
                progress
                    .retries
                    .get(identity)
                    .map(|(due, _)| SinkDispatchRetryIdentity {
                        identity: identity.clone(),
                        retry_at: *due,
                    })
            })
            .collect::<Vec<_>>();
        let blocked_floor = blocked
            .iter()
            .map(|identity| identity.at_lsn)
            .chain(pending_retries.iter().map(|retry| retry.identity.at_lsn))
            .min();
        let processed_lsn =
            blocked_floor.map_or(prefix.committed_lsn, |lsn| Lsn(lsn.0.saturating_sub(1)));
        Ok(SinkDispatchReceipt {
            committed_lsn: prefix.committed_lsn,
            processed_lsn,
            committed_positions: prefix.entries,
            processed_positions: processed,
            pending_retries,
            blocked_positions: blocked,
        })
    }
}
