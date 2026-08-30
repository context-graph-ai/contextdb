use super::*;
use crate::database::CursorIdleWindow;
use crate::memory_accounting::OwnedMemoryReservation;
use crate::plugin::QueryOutcome;
use crate::read_contract::CanonicalQueryResult;
use bincode::enc::write::Writer;
use contextdb_core::read_contract::{
    CursorExpiryKind, OwnerLimitExceededDetail, ReadFailureDetail, ReadFailureKind,
    ReadFailureLimit, RequiredBytesSetting, StatementRemedy,
};
use contextdb_planner::{ProjectColumn, SortKey};
use contextdb_relational::mem::{BoundedOrderedRowCursor, BoundedPhysicalCursor};
use serde::Serialize;
use std::cell::Cell;
use std::collections::{HashSet, VecDeque};
use std::fmt::Write as _;
use std::ops::Bound;
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

struct RequestState {
    started_ms: u64,
    cancellation: OwnerReadCancellation,
    telemetry: BoundedExecutionTelemetry,
    touches: BTreeMap<BoundedSourceTouch, u64>,
    memory: OwnedMemoryReservation,
    // What the answer being assembled holds right now, and how much work had
    // been done when its progress was last reported. These describe the
    // in-flight answer a caller is waiting on, which is exactly what the
    // finished telemetry cannot say: `encoded_bytes` is only true once the
    // read has stopped.
    rows_so_far: u64,
    bytes_so_far: u64,
    reported_work: u64,
    reported_rows: u64,
    // Sources release through infallible callbacks so a charge still travels
    // its release path while an error unwinds. A release can only fail by
    // underflowing the live charge, so that fault is parked here and raised
    // by the next boundary check instead of being dropped.
    release_fault: Option<BoundedExecutionError>,
}

/// Whether the read this context governs can hand an operator back its own
/// unfinished state and be asked again.
#[derive(Clone, Copy)]
enum ReadResumption {
    /// A one-shot read. The answer is assembled once, so an operator that
    /// cannot finish inside the ceilings the caller declared is refused with
    /// the typed refusal that names the ceiling.
    OneShot,
    /// A cursor. Each fetch is its own work budget over the one snapshot the
    /// cursor was opened on, so an operator that has spent this fetch's
    /// budget keeps what it has and finishes on a later fetch.
    AcrossFetches,
}

struct RequestContext {
    limits: ReadLimits,
    /// How this read ends when an operator runs out of budget mid-answer:
    /// refused, or paused for the next fetch.
    resumption: ReadResumption,
    /// The store-side reservation a door that declared NO ceilings holds for a
    /// traversal's frontier, grown as the drain resolves starts. `None` for a
    /// read that named its own ceilings: that read's frontier is governed by
    /// the ceiling it named, and it holds nothing the store accounts for.
    store_frontier: Mutex<Option<OwnedMemoryReservation>>,
    /// The transaction this read is running inside, decided ONCE where the
    /// read is entered. A caller holding an explicit transaction handle is not
    /// the session's transaction, so asking the store for "the active one"
    /// answers `None` and the read silently drops to committed state.
    transaction: Option<TxId>,
    clock: Arc<dyn DeadlineClock>,
    state: Mutex<RequestState>,
    #[cfg(feature = "test-seams")]
    probe: Mutex<Option<Arc<dyn BoundedExecutionProbe>>>,
}

impl RequestContext {
    fn new(
        db: &dyn ReadExecutionTarget,
        limits: ReadLimits,
        resumption: ReadResumption,
        clock: Arc<dyn DeadlineClock>,
        cancellation: OwnerReadCancellation,
        transaction: Option<TxId>,
        #[cfg(feature = "test-seams")] probe: Option<Arc<dyn BoundedExecutionProbe>>,
    ) -> std::result::Result<Arc<Self>, BoundedExecutionError> {
        Self::check_effective_limits(&limits)?;
        let started_ms = clock.now_ms();
        Ok(Arc::new(Self {
            limits,
            resumption,
            store_frontier: Mutex::new(None),
            transaction,
            clock,
            state: Mutex::new(RequestState {
                started_ms,
                cancellation,
                telemetry: BoundedExecutionTelemetry::default(),
                touches: BTreeMap::new(),
                memory: OwnedMemoryReservation::new(db.bounded_read_accountant()),
                rows_so_far: 0,
                bytes_so_far: 0,
                reported_work: 0,
                reported_rows: 0,
                release_fault: None,
            }),
            #[cfg(feature = "test-seams")]
            probe: Mutex::new(probe),
        }))
    }

    /// Check the limits this read will actually enforce.
    ///
    /// The kernel is handed *effective* limits — a set already narrowed field
    /// by field where the request was admitted. `ReadLimits::validate` also
    /// asserts relations between fields (a cursor page must fit inside the
    /// whole result, an idle window inside a lifetime); those describe a
    /// coherent limit *declaration*, and re-asserting them here refuses a read
    /// that merely asked for fewer result rows than the standing page ceiling
    /// — a stricter read, not an invalid one, and one that opens no cursor at
    /// all on the ordinary path. What the kernel needs from a limit is that it
    /// can admit something: a zero ceiling can never produce a row, a byte, or
    /// a unit of work.
    ///
    /// A limit the kernel cannot honour is reported as the same typed refusal
    /// every other ceiling produces, so a caller reads one vocabulary rather
    /// than an opaque engine string. The cursor-only ceilings carry their own
    /// typed refusals where the cursor path binds them (`default_page_rows`
    /// for a page that can hold no row, the expiry checks for a lifetime that
    /// has already elapsed).
    fn check_effective_limits(
        limits: &ReadLimits,
    ) -> std::result::Result<(), BoundedExecutionError> {
        for (value, limit) in [
            (limits.result_rows, ReadFailureLimit::ResultRows),
            (limits.result_bytes, ReadFailureLimit::ResultBytes),
            (limits.work, ReadFailureLimit::Work),
            (limits.active_ms, ReadFailureLimit::ActiveMs),
            (limits.memory, ReadFailureLimit::Memory),
            (limits.cursor_page_bytes, ReadFailureLimit::CursorPageBytes),
        ] {
            if value == 0 {
                return Err(Self::limit_failure(limit, value));
            }
        }
        Ok(())
    }

    fn state(&self) -> MutexGuard<'_, RequestState> {
        match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn begin_fetch(
        &self,
        cancellation: OwnerReadCancellation,
        #[cfg(feature = "test-seams")] probe: Option<Arc<dyn BoundedExecutionProbe>>,
    ) -> std::result::Result<(), BoundedExecutionError> {
        let mut state = self.state();
        let held = u64::try_from(state.memory.bytes())
            .map_err(|_| Self::limit_failure(ReadFailureLimit::Memory, self.limits.memory))?;
        state.started_ms = self.clock.now_ms();
        state.cancellation = cancellation;
        state.telemetry = BoundedExecutionTelemetry {
            peak_temporary_bytes: held,
            ..BoundedExecutionTelemetry::default()
        };
        state.touches.clear();
        // A page is its own read as far as a waiting caller is concerned: it
        // starts with no rows, no bytes, and nothing reported.
        state.rows_so_far = 0;
        state.bytes_so_far = 0;
        state.reported_work = 0;
        state.reported_rows = 0;
        #[cfg(feature = "test-seams")]
        {
            let mut current = match self.probe.lock() {
                Ok(current) => current,
                Err(poisoned) => poisoned.into_inner(),
            };
            *current = probe;
        }
        Ok(())
    }

    #[cfg(feature = "test-seams")]
    fn probe(&self) -> Option<Arc<dyn BoundedExecutionProbe>> {
        match self.probe.lock() {
            Ok(probe) => probe.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        }
    }

    /// Record what the answer being assembled holds right now.
    ///
    /// The caller waiting on this read wants to know how much of its answer
    /// exists, and the only place that is known is where a row has just been
    /// added and the answer re-measured.
    fn note_result_shape(&self, rows: usize, bytes: u64) {
        {
            let mut state = self.state();
            state.rows_so_far = u64::try_from(rows).unwrap_or(u64::MAX);
            state.bytes_so_far = bytes;
        }
        // Assembling the answer is the half of a read that examines nothing,
        // so it has to speak for itself: a read that sorted first arrives
        // here with every item already examined and not one row of the answer
        // reported.
        self.report_progress();
    }

    /// Tell whoever asked what this read has done so far, if it has done
    /// enough since the last report to have something new to say.
    ///
    /// The observer is called with no lock held: it is caller code, it may do
    /// anything, and it must not be able to deadlock the read that is
    /// reporting to it.
    fn report_progress(&self) {
        self.publish_progress(ProgressReport::WhileRunning);
    }

    /// Account for what this read did, now that it is finished.
    ///
    /// Exactly one total per completed read or published page, always. This is
    /// not a liveness report and does not become one: it goes to the
    /// accounting seam, which nothing on the local channel carries and whose
    /// default does nothing. The intervals stay exactly the promise they were
    /// -- a read that finishes inside one still tells a watching caller
    /// nothing -- while a caller adding up what reads did gets every read's
    /// number, including the read that ended on an interval boundary and the
    /// read that examined nothing.
    fn flush_final_progress(&self) {
        self.publish_progress(ProgressReport::AtCompletion);
    }

    fn publish_progress(&self, moment: ProgressReport) {
        let Some(observer) = crate::read_progress::observer_for_this_read() else {
            return;
        };
        let Some(progress) = self.progress_to_publish(moment) else {
            return;
        };
        // Liveness and accounting travel separately: a caller watching this
        // read run hears only what the intervals below admit, and the totals a
        // finished read owes go to the accounting seam whatever those
        // intervals happened to say.
        match moment {
            ProgressReport::WhileRunning => observer.progress(progress),
            ProgressReport::AtCompletion => observer.completed(progress),
        }
    }

    /// What to publish for this moment, or nothing when a LIVENESS report is
    /// not yet due.
    ///
    /// A completion total is always due. It is a statement of what one read
    /// cost, not news about a read in flight, so nothing about the liveness
    /// intervals decides whether it is made -- a read that ended exactly on an
    /// interval boundary, and a read that examined nothing at all, each owe
    /// their caller the same one total as any other. It neither reads nor
    /// moves the liveness bookkeeping for the same reason: a total is not a
    /// report the caller has now been told, and treating it as one would
    /// silence the next real report.
    fn progress_to_publish(
        &self,
        moment: ProgressReport,
    ) -> Option<crate::read_progress::ReadProgress> {
        let mut state = self.state();
        let work = state.telemetry.work_units;
        let rows = state.rows_so_far;
        if let ProgressReport::WhileRunning = moment {
            let due = work
                >= state
                    .reported_work
                    .saturating_add(crate::read_progress::WORK_REPORTING_INTERVAL)
                || rows
                    >= state
                        .reported_rows
                        .saturating_add(crate::read_progress::ROW_REPORTING_INTERVAL);
            if !due {
                return None;
            }
            state.reported_work = work;
            state.reported_rows = rows;
        }
        let active_ms = self.clock.now_ms().saturating_sub(state.started_ms);
        Some(crate::read_progress::ReadProgress {
            phase: crate::read_progress::ReadPhase::Executing,
            rows: state.rows_so_far,
            bytes: state.bytes_so_far,
            loaded_bytes: 0,
            total_bytes: None,
            work,
            active_ms,
        })
    }

    fn limit_failure(limit: ReadFailureLimit, value: u64) -> BoundedExecutionError {
        BoundedExecutionError::Refused(ReadFailure::owner_limit_exceeded(
            OwnerLimitExceededDetail {
                limit,
                value,
                required: None,
                statement: None,
            },
        ))
    }

    /// Whatever must stop the request before it does more work: a parked
    /// release fault first, because it means the live charge no longer
    /// describes what the request holds, then cancellation.
    fn pending_failure(&self) -> Option<BoundedExecutionError> {
        let fault = {
            let mut state = self.state();
            state.release_fault.take()
        };
        if fault.is_some() {
            return fault;
        }
        self.cancellation()
    }

    /// Give bytes back from a path that cannot report an error. The first
    /// fault is kept; later ones would describe the same broken accounting.
    fn release_parked(&self, bytes: usize) {
        let Err(fault) = self.release(bytes) else {
            return;
        };
        let mut state = self.state();
        if state.release_fault.is_none() {
            state.release_fault = Some(fault);
        }
    }

    /// The terminal answer for a caller that withdrew its read while the
    /// snapshot registration was parked behind an in-flight removal pass.
    fn withdrawn_failure(&self) -> BoundedExecutionError {
        self.pending_failure()
            .unwrap_or(BoundedExecutionError::Cancelled)
    }

    fn cancellation(&self) -> Option<BoundedExecutionError> {
        let mut state = self.state();
        if !state.cancellation.is_cancelled() {
            return None;
        }
        let first_observation = !state.telemetry.cancellation_observed;
        state.telemetry.cancellation_observed = true;
        let completed = state.telemetry.work_units;
        drop(state);
        #[cfg(not(feature = "test-seams"))]
        let _ = first_observation;
        #[cfg(feature = "test-seams")]
        if first_observation && let Some(probe) = self.probe() {
            probe.cancellation_observed(completed);
        }
        Some(BoundedExecutionError::Cancelled)
    }

    fn elapsed_since(&self, started_ms: u64) -> std::result::Result<u64, BoundedExecutionError> {
        self.clock.now_ms().checked_sub(started_ms).ok_or_else(|| {
            Error::Other("bounded deadline clock moved backwards".to_string()).into()
        })
    }

    fn check_final_boundary(&self) -> std::result::Result<(), BoundedExecutionError> {
        if let Some(cancelled) = self.pending_failure() {
            return Err(cancelled);
        }
        let started_ms = self.state().started_ms;
        if self.elapsed_since(started_ms)? > self.limits.active_ms {
            return Err(Self::limit_failure(
                ReadFailureLimit::ActiveMs,
                self.limits.active_ms,
            ));
        }
        Ok(())
    }

    /// Count one thing the read looked at, without charging for looking.
    ///
    /// The work a source does over an item is charged where it is done; this
    /// records that the item itself was read, once, so what the trace publishes
    /// as examined counts items rather than steps taken over them.
    fn note_touch(&self, touch: BoundedSourceTouch) {
        let mut state = self.state();
        let counted = state.touches.entry(touch).or_default();
        *counted = counted.saturating_add(1);
    }

    fn charge(
        &self,
        source: BoundedWorkSource,
        touch: BoundedSourceTouch,
    ) -> std::result::Result<(), BoundedExecutionError> {
        if let Some(cancelled) = self.pending_failure() {
            return Err(cancelled);
        }
        let (completed_work, completed_source, started_ms) = {
            let state = self.state();
            if state.telemetry.work_units >= self.limits.work {
                return Err(Self::limit_failure(
                    ReadFailureLimit::Work,
                    self.limits.work,
                ));
            }
            let elapsed = self.elapsed_since(state.started_ms)?;
            if elapsed >= self.limits.active_ms {
                return Err(Self::limit_failure(
                    ReadFailureLimit::ActiveMs,
                    self.limits.active_ms,
                ));
            }
            (
                state.telemetry.work_units,
                state.touches.get(&touch).copied().map_or(0, |count| count),
                state.started_ms,
            )
        };

        #[cfg(feature = "test-seams")]
        if let Some(probe) = self.probe() {
            probe.before_work(source, completed_work);
        }
        if self.elapsed_since(started_ms)? > self.limits.active_ms {
            return Err(Self::limit_failure(
                ReadFailureLimit::ActiveMs,
                self.limits.active_ms,
            ));
        }
        #[cfg(feature = "test-seams")]
        if let Some(probe) = self.probe() {
            probe.before_source_touch(touch, completed_source);
        }
        if let Some(cancelled) = self.pending_failure() {
            return Err(cancelled);
        }

        let mut state = self.state();
        state.telemetry.work_units = state
            .telemetry
            .work_units
            .checked_add(1)
            .ok_or_else(|| Self::limit_failure(ReadFailureLimit::Work, self.limits.work))?;
        {
            let source_work = state.telemetry.source_work.entry(source).or_default();
            *source_work = source_work
                .checked_add(1)
                .ok_or_else(|| Self::limit_failure(ReadFailureLimit::Work, self.limits.work))?;
        }
        {
            let source_touches = state.touches.entry(touch).or_default();
            *source_touches = source_touches
                .checked_add(1)
                .ok_or_else(|| Self::limit_failure(ReadFailureLimit::Work, self.limits.work))?;
        }
        #[cfg(feature = "test-seams")]
        {
            crate::read_probe::note_bounded_source_touch();
            crate::read_probe::note_session_source_touch();
        }
        drop(state);
        // Reported from inside the loop, so a caller keeps hearing from a read
        // that is being cancelled right up to the moment it returns.
        self.report_progress();
        Ok(())
    }

    fn charge_operator(
        &self,
        source: BoundedWorkSource,
    ) -> std::result::Result<(), BoundedExecutionError> {
        if let Some(cancelled) = self.pending_failure() {
            return Err(cancelled);
        }
        let (completed_work, started_ms) = {
            let state = self.state();
            if state.telemetry.work_units >= self.limits.work {
                return Err(Self::limit_failure(
                    ReadFailureLimit::Work,
                    self.limits.work,
                ));
            }
            let elapsed = self.elapsed_since(state.started_ms)?;
            if elapsed >= self.limits.active_ms {
                return Err(Self::limit_failure(
                    ReadFailureLimit::ActiveMs,
                    self.limits.active_ms,
                ));
            }
            (state.telemetry.work_units, state.started_ms)
        };
        #[cfg(feature = "test-seams")]
        if let Some(probe) = self.probe() {
            probe.before_work(source, completed_work);
        }
        if self.elapsed_since(started_ms)? > self.limits.active_ms {
            return Err(Self::limit_failure(
                ReadFailureLimit::ActiveMs,
                self.limits.active_ms,
            ));
        }
        if let Some(cancelled) = self.pending_failure() {
            return Err(cancelled);
        }
        let mut state = self.state();
        state.telemetry.work_units = state
            .telemetry
            .work_units
            .checked_add(1)
            .ok_or_else(|| Self::limit_failure(ReadFailureLimit::Work, self.limits.work))?;
        let source_work = state.telemetry.source_work.entry(source).or_default();
        *source_work = source_work
            .checked_add(1)
            .ok_or_else(|| Self::limit_failure(ReadFailureLimit::Work, self.limits.work))?;
        Ok(())
    }

    fn reserve(
        &self,
        source: BoundedWorkSource,
        requested: usize,
    ) -> std::result::Result<(), BoundedExecutionError> {
        if requested == 0 {
            return Ok(());
        }
        self.check_final_boundary()?;
        let requested_u64 = u64::try_from(requested)
            .map_err(|_| Self::limit_failure(ReadFailureLimit::Memory, self.limits.memory))?;
        let held = {
            let state = self.state();
            let held = u64::try_from(state.memory.bytes())
                .map_err(|_| Self::limit_failure(ReadFailureLimit::Memory, self.limits.memory))?;
            if held
                .checked_add(requested_u64)
                .is_none_or(|after| after > self.limits.memory)
            {
                return Err(Self::limit_failure(
                    ReadFailureLimit::Memory,
                    self.limits.memory,
                ));
            }
            held
        };
        #[cfg(feature = "test-seams")]
        if let Some(probe) = self.probe() {
            probe.before_temporary_reservation(source, requested_u64, held);
        }
        let held_after = {
            let mut state = self.state();
            // Named, so a store-budget refusal tells the operator WHICH part of
            // the statement wanted the memory rather than only how much.
            if let Err(error) = state.memory.try_grow_for(
                requested,
                "bounded_read",
                source.reservation_operation(),
                "Lower the statement's LIMIT or width, or raise MEMORY_LIMIT.",
            ) {
                // The read's own ceiling was checked just above and let this
                // through, so what refused is the STORE's standing budget.
                // Which vocabulary that is reported in depends on what the
                // caller declared:
                //
                // A read that DECLARED ceilings is answered in the refusal
                // vocabulary it declared them in -- a crossed ceiling, carrying
                // the ceiling, whether the memory was denied by its own limit
                // or by the database's budget. That is the standing contract
                // (`bounded_memory_refusal_typing.rs`), and it is what lets a
                // caller branch on one shape.
                //
                // A read that declared NONE has no ceiling to name. Reporting
                // one anyway names `u64::MAX` as the limit that stopped it,
                // which is not a number anyone can act on, so that caller gets
                // the store's own typed answer, which names the work its budget
                // stopped.
                if self.declares_no_ceilings() {
                    return Err(BoundedExecutionError::from(error));
                }
                return Err(Self::limit_failure(
                    ReadFailureLimit::Memory,
                    self.limits.memory,
                ));
            }
            let held_after = u64::try_from(state.memory.bytes())
                .map_err(|_| Self::limit_failure(ReadFailureLimit::Memory, self.limits.memory))?;
            state.telemetry.peak_temporary_bytes =
                state.telemetry.peak_temporary_bytes.max(held_after);
            state
                .telemetry
                .source_peak_temporary_bytes
                .entry(source)
                .and_modify(|peak| *peak = (*peak).max(held_after))
                .or_insert(held_after);
            held_after
        };
        #[cfg(feature = "test-seams")]
        if let Some(probe) = self.probe() {
            probe.after_temporary_reservation(source, requested_u64, held_after);
        }
        Ok(())
    }

    fn release(&self, bytes: usize) -> std::result::Result<(), BoundedExecutionError> {
        self.state()
            .memory
            .try_shrink(bytes)
            .map_err(BoundedExecutionError::from)
    }

    fn set_encoded_bytes(&self, bytes: u64) {
        self.state().telemetry.encoded_bytes = bytes;
    }

    fn can_charge_more_work(&self) -> bool {
        self.state().telemetry.work_units < self.limits.work
    }

    /// Whether an operator should stop here and finish on a later fetch.
    ///
    /// A cursor's work ceiling is charged per FETCH, so an operator that has
    /// spent this fetch's budget has not failed -- it has reached the end of
    /// what this page may pay for, and hands back what it has so far. A
    /// one-shot read has no later fetch to finish in, so it never stops here
    /// and meets the work ceiling as the typed refusal it always was.
    fn fetch_budget_spent(&self) -> bool {
        matches!(self.resumption, ReadResumption::AcrossFetches) && !self.can_charge_more_work()
    }

    /// Whether this refusal is this fetch's budget running out rather than a
    /// read that cannot be answered.
    ///
    /// A source charges as it walks, so it can reach the ceiling in the
    /// middle of one pull -- a filter reads several rows to hand back one.
    /// For a read that resumes, that refusal means "not in this page", not
    /// "no answer"; the operator that can carry its state forward absorbs it.
    fn refusal_pauses_this_fetch(&self, error: &BoundedExecutionError) -> bool {
        if !matches!(self.resumption, ReadResumption::AcrossFetches) {
            return false;
        }
        let BoundedExecutionError::Refused(failure) = error else {
            return false;
        };
        matches!(
            failure.detail(),
            ReadFailureDetail::OwnerLimitExceeded(detail)
                if detail.limit == ReadFailureLimit::Work
        )
    }

    fn held_bytes(&self) -> usize {
        self.state().memory.bytes()
    }

    fn attribute_live_memory(&self, source: BoundedWorkSource) {
        let mut state = self.state();
        let held = match u64::try_from(state.memory.bytes()) {
            Ok(held) => held,
            Err(_) => return,
        };
        state
            .telemetry
            .source_peak_temporary_bytes
            .entry(source)
            .and_modify(|peak| *peak = (*peak).max(held))
            .or_insert(held);
    }

    /// What this read looked at to answer, in the executor's own terms: rows
    /// it read, index entries it walked, adjacency entries it inspected.
    fn rows_examined(&self) -> std::result::Result<u64, BoundedExecutionError> {
        let state = self.state();
        [
            BoundedSourceTouch::TableRow,
            BoundedSourceTouch::IndexEntry,
            BoundedSourceTouch::AdjacencyEntry,
        ]
        .into_iter()
        .try_fold(0u64, |sum, touch| {
            sum.checked_add(state.touches.get(&touch).copied().unwrap_or(0))
        })
        .ok_or_else(|| Error::Other("bounded rows-examined counter overflow".to_string()).into())
    }

    /// Let this read hold the store's frontier reservation. Called only by a
    /// door that declared no ceilings; every other read leaves it unset and
    /// holds nothing the store accounts for.
    fn enable_store_frontier(&self, accountant: Arc<crate::memory_accounting::MemoryAccountant>) {
        *self
            .store_frontier
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) =
            Some(OwnedMemoryReservation::new(accountant));
    }

    /// Charge the store for one more resolved start's worth of frontier.
    ///
    /// Grown as the drain resolves starts, never estimated ahead of them: a
    /// traversal that resolves NO start pays nothing and cannot be refused for
    /// a frontier it never built, and one that resolves many pays for all of
    /// them, which is what the established door charges after resolving its
    /// own.
    fn charge_store_frontier(
        &self,
        bytes: usize,
    ) -> std::result::Result<(), BoundedExecutionError> {
        let mut held = self
            .store_frontier
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(reservation) = held.as_mut() else {
            return Ok(());
        };
        reservation
            .try_grow_for(
                bytes,
                "bfs_frontier",
                "graph_bfs",
                "Reduce traversal depth/fan-out or raise MEMORY_LIMIT before running BFS.",
            )
            .map_err(BoundedExecutionError::from)
    }

    /// Whether this read named any ceiling of its own. A read that named none
    /// is governed by what the store declares instead.
    fn declares_no_ceilings(&self) -> bool {
        self.limits.memory == u64::MAX
    }

    fn transaction(&self) -> Option<TxId> {
        self.transaction
    }

    fn telemetry(&self) -> BoundedExecutionTelemetry {
        self.state().telemetry.clone()
    }
}

/// Holds bytes a bounded source reserved on the caller's behalf for exactly
/// as long as the caller works with the value they cover. The source reports
/// the precise amount it reserved and never releases it itself, so every exit
/// from the owning block — including an error unwind — has to give it back.
/// Releasing from `Drop` is what makes that true of paths that never named
/// the reservation.
/// Why a report is being made, which decides both what makes one due and
/// which seam it goes to.
#[derive(Debug, Clone, Copy)]
enum ProgressReport {
    /// Liveness, mid-read: due once an interval's worth of new work exists,
    /// and told to whoever is watching this read run.
    WhileRunning,
    /// Accounting, at the end: due if anything at all is still untold, and
    /// told to the in-process seam rather than to the liveness surface.
    AtCompletion,
}

struct ReservedBytes<'a> {
    context: &'a Arc<RequestContext>,
    bytes: usize,
}

impl<'a> ReservedBytes<'a> {
    fn new(context: &'a Arc<RequestContext>, bytes: usize) -> Self {
        Self { context, bytes }
    }

    /// Give the bytes back where an accounting underflow can still be
    /// reported as the request's own failure.
    fn release(mut self) -> std::result::Result<(), BoundedExecutionError> {
        let bytes = std::mem::take(&mut self.bytes);
        self.context.release(bytes)
    }
}

/// A materialized row whose retained bytes stay charged to the request only
/// while this guard holds it.  Every way out of the block that pulled the row
/// — a refusal, a cancellation, an accounting fault — gives those bytes back;
/// handing the row on to the state that carries the charge from there is the
/// one exit that keeps them.
struct RetainedRow<'a> {
    context: &'a Arc<RequestContext>,
    row: PulledRow,
    charged: bool,
}

impl<'a> RetainedRow<'a> {
    fn new(context: &'a Arc<RequestContext>, row: PulledRow) -> Self {
        Self {
            context,
            row,
            charged: true,
        }
    }

    /// Give the bytes back where an accounting underflow can still be reported
    /// as the request's own failure.
    fn release(mut self) -> std::result::Result<(), BoundedExecutionError> {
        self.charged = false;
        let bytes = std::mem::take(&mut self.row.retained_bytes);
        self.context.release(bytes)
    }

    /// Hand the row to the state that carries its charge from here on.
    fn into_row(mut self) -> PulledRow {
        self.charged = false;
        std::mem::replace(
            &mut self.row,
            PulledRow {
                values: Vec::new(),
                retained_bytes: 0,
            },
        )
    }
}

impl std::ops::Deref for RetainedRow<'_> {
    type Target = PulledRow;

    fn deref(&self) -> &Self::Target {
        &self.row
    }
}

impl std::ops::DerefMut for RetainedRow<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.row
    }
}

impl Drop for RetainedRow<'_> {
    fn drop(&mut self) {
        if !self.charged {
            return;
        }
        let bytes = std::mem::take(&mut self.row.retained_bytes);
        if bytes != 0 {
            self.context.release_parked(bytes);
        }
    }
}

impl Drop for ReservedBytes<'_> {
    fn drop(&mut self) {
        let bytes = std::mem::take(&mut self.bytes);
        if bytes != 0 {
            self.context.release_parked(bytes);
        }
    }
}

// Ordinary execution still reaches these compatibility observers. The bounded
// path uses the fallible callbacks on the database helpers directly, so legacy
// callers retain their existing behavior and acquire no request budget.
pub(super) fn observe_access_control_row() {}

pub(super) fn observe_rank_candidate() {}

struct PullNode {
    columns: Vec<String>,
    trace: QueryTrace,
    kind: PullKind,
}

struct PulledRow {
    values: Vec<Value>,
    retained_bytes: usize,
}

impl PulledRow {
    fn release(
        self,
        context: &Arc<RequestContext>,
    ) -> std::result::Result<(), BoundedExecutionError> {
        context.release(self.retained_bytes)
    }
}

#[allow(clippy::large_enum_variant)]
enum PullKind {
    Scan(ScanState),
    Graph(GraphState),
    Vector(VectorState),
    Project(ProjectState),
    Sort(SortState),
    Limit(LimitState),
    Filter(FilterState),
    Distinct(DistinctState),
    Join(JoinState),
    Static(StaticState),
    Union(UnionState),
}

struct ScanState {
    table: String,
    meta: TableMeta,
    schema_columns: Vec<String>,
    filter: Option<Expr>,
    params: Arc<HashMap<String, Value>>,
    snapshot: SnapshotId,
    mode: ScanMode,
    cursor_key_bytes: usize,
    residual_pick: Option<IndexPick>,
    /// Whether the committed source offers EVERY committed row of the table.
    /// A physical walk does. An index run offers only the rows its predicate
    /// names, so it cannot answer "did this identity have a committed
    /// original" merely by having passed it -- the store is asked instead.
    committed_source_offers_every_row: bool,
    pending_row: Option<(VersionedRow, usize, BoundedWorkSource)>,
    /// Whether the row in hand came from the committed source. An ordered
    /// read hands committed and staged rows back interleaved, so which one is
    /// in hand is recorded rather than inferred from how far the walk has got.
    pending_from_committed: bool,
    /// The quantized vector columns THIS answer names, so the scan fills
    /// those and no others. A column nobody asked for is not read out of the
    /// store and not charged: a search that orders by distance and projects
    /// only an id scores from the index and never touches a row's vector.
    supplement_columns: Vec<String>,
    /// The transaction's own rows folded into an ordered walk where their
    /// ordering value puts them. Present only for a read that PROMISED an
    /// order and has a transaction open; every other read hands the staged
    /// rows over after the committed ones, which is all an unordered answer
    /// asks for.
    ordered_merge: Option<OrderedMerge>,
    /// What the reading handle's own open transaction adds to and hides from
    /// this table. Absent when no transaction is open, which is every read of
    /// a committed file and every read by a handle that is not mid-write.
    transaction: Option<ScanTransactionOverlay>,
}

/// An ordered walk's view of the rows its own transaction staged.
///
/// An index run is the committed rows in order and nothing else. Reading the
/// whole table and sorting it answers the same question, but it turns a
/// five-row answer over a declared index into a walk of every row the table
/// holds the moment the reader has a transaction open -- so a work ceiling
/// sized on committed state refuses the identical query with a write in
/// flight. The transaction's own rows are few, already materialized, and
/// already charged, so they are put in this read's order once and folded into
/// the run as it walks.
struct OrderedMerge {
    /// The column the run is ordered on, and the direction it is walked in.
    column: String,
    direction: contextdb_core::SortDirection,
    /// The staged rows in this read's order. `None` until they have been read.
    staged: Option<VecDeque<(VersionedRow, usize)>>,
    /// A committed row already pulled and then held back, because a staged
    /// row's ordering value came first. It is the next row out.
    deferred: Option<(VersionedRow, usize, BoundedWorkSource)>,
    /// What the two above are holding, so a paused read is charged for them.
    held_bytes: usize,
}

/// Where one row sits in an ordered walk, in the same total order the index
/// itself is kept in.
fn ordered_merge_key(
    row: &VersionedRow,
    column: &str,
    direction: contextdb_core::SortDirection,
) -> DirectedValue {
    let value = row.values.get(column).cloned().unwrap_or(Value::Null);
    match direction {
        contextdb_core::SortDirection::Asc => DirectedValue::Asc(TotalOrdAsc(value)),
        contextdb_core::SortDirection::Desc => DirectedValue::Desc(TotalOrdDesc(value)),
    }
}

/// The open transaction this scan is reading inside, and how far through its
/// staged rows the scan has got.
struct ScanTransactionOverlay {
    tx: TxId,
    overlay: crate::executor::TransactionTableOverlay,
    /// Identities of staged rows that also exist as committed rows this
    /// snapshot can see. A staged row whose committed original the
    /// transaction ALSO deleted is still published -- delete-then-reinsert
    /// leaves the reinserted row -- which is what this set is for.
    restaged_over_committed: std::collections::HashSet<RowId>,
    /// How many of the staged rows have been published.
    published: usize,
    /// Set once the committed source is exhausted and the staged rows start.
    committed_exhausted: bool,
}

impl ScanTransactionOverlay {
    /// Whether the committed row in hand is hidden by this transaction: it
    /// staged the row's deletion, it staged a deletion matching the row's
    /// values, or it restaged the row and its own version is published
    /// instead.
    fn hides_committed(&mut self, table: &str, row: &VersionedRow) -> bool {
        if self.overlay.staged_identities.contains(&row.row_id) {
            // Recorded whether or not the row is also hidden: what makes a
            // staged row publishable is that its original exists here at all.
            let _ = self.restaged_over_committed.insert(row.row_id);
            return true;
        }
        if self.overlay.deleted.contains(&row.row_id) {
            return true;
        }
        contextdb_tx::row_matches_delete_predicates(&self.overlay.delete_predicates, table, row)
    }
}

#[allow(clippy::large_enum_variant)]
enum ScanMode {
    Physical {
        cursor: BoundedPhysicalCursor,
    },
    Index {
        pick: IndexPick,
        column: String,
        direction: contextdb_core::SortDirection,
        cursor: BoundedOrderedRowCursor,
    },
    Ordered {
        column: String,
        direction: contextdb_core::SortDirection,
        cursor: BoundedOrderedRowCursor,
    },
    /// An index built for exact lookup answers whole keys rather than runs,
    /// so the source asks it the keys the predicate names instead of walking
    /// an order it does not keep.
    Exact {
        index: String,
        cursor: contextdb_relational::mem::BoundedExactIndexCursor,
    },
    Empty,
}

struct GraphState {
    start_alias: String,
    steps: Vec<GraphStepPlan>,
    filter: Option<Expr>,
    params: Arc<HashMap<String, Value>>,
    snapshot: SnapshotId,
    starts: VecDeque<uuid::Uuid>,
    start_candidates: Option<Box<PullNode>>,
    edge_source: Option<crate::database::gate::BoundedGraphEdgeCursor>,
    single_step_unpinned: bool,
    /// A single hop pinned only where it ENDS is walked backwards from that
    /// end: the pinned node is the one adjacency list worth opening, and the
    /// hop's own direction is reversed to reach the nodes that arrive at it.
    /// The bindings are then put back the way the statement wrote them.
    probes_backwards_from_target: bool,
    /// The standing visited ceiling this traversal keeps. `None` for a read
    /// that declared its own budget -- that budget replaces the ceiling, which
    /// is the whole point of a bounded read. `Some` for a read that declared
    /// none, which keeps the ceiling it always had.
    visited_cap: Option<usize>,
    /// What one resolved start costs the STORE's frontier budget, charged as
    /// each start is resolved rather than estimated ahead of them. Zero for a
    /// read that declared its own ceilings.
    frontier_unit_bytes: usize,
    /// Whether this traversal is the single hop that the other door answers
    /// with an adjacency probe. A probe reports one row per NEIGHBOUR reached,
    /// so the nodes already reported out of the adjacency being walked are
    /// remembered here for as long as that one adjacency is being walked.
    single_step_probe: bool,
    probe_seen: Vec<uuid::Uuid>,
    probe_seen_container_bytes: usize,
    seen_starts: Vec<uuid::Uuid>,
    seen_start_container_bytes: usize,
    seen_edges: Vec<(uuid::Uuid, uuid::Uuid)>,
    seen_edge_container_bytes: usize,
    /// The paths this traversal has already answered or already continued,
    /// keyed the way the other door keys them: by the nodes the pattern's
    /// target aliases are bound to, never by where the walk began. Two starts
    /// that arrive at the same nodes are ONE answer, and the continuation out
    /// of those nodes is walked once.
    seen_paths: Vec<Vec<uuid::Uuid>>,
    seen_path_container_bytes: usize,
    frontier: Vec<GraphPathState>,
    frontier_container_bytes: usize,
    row_memory: AmortizedRowMemory,
    active: Option<GraphActiveState>,
}

/// Memory this traversal has been admitted for and not yet handed to a row.
///
/// A traversal reports one row after another, each needing a small allocation
/// of its own. Asking the request to admit every one of them separately makes
/// the number of admissions grow with the rows the traversal reports rather
/// than with the memory it holds. The traversal is admitted for a block, hands
/// that block out row by row, and asks again — for a larger block — only once
/// the block is spent, so every byte is still admitted before it is taken and
/// the admissions grow with the logarithm of the rows.
#[derive(Default)]
struct AmortizedRowMemory {
    available: usize,
    block: usize,
}

/// The largest block a traversal is admitted for in one step. Past this size a
/// larger block would hold more memory ahead of the rows than it saves in
/// admissions.
const AMORTIZED_ROW_MEMORY_BLOCK_CEILING: usize = 8 * 1024;

impl AmortizedRowMemory {
    fn take(
        &mut self,
        context: &Arc<RequestContext>,
        bytes: usize,
    ) -> std::result::Result<(), BoundedExecutionError> {
        if self.available < bytes {
            let shortfall = bytes - self.available;
            let block = self
                .block
                .max(shortfall)
                .min(AMORTIZED_ROW_MEMORY_BLOCK_CEILING.max(shortfall));
            context.reserve(BoundedWorkSource::GraphTraversal, block)?;
            self.available = checked_size_add(self.available, block, "graph row memory")?;
            self.block = block
                .saturating_mul(2)
                .min(AMORTIZED_ROW_MEMORY_BLOCK_CEILING);
        }
        self.available -= bytes;
        Ok(())
    }

    /// Return bytes a row did not need. They stay admitted and are handed to
    /// the next row rather than released and re-admitted.
    fn give_back(&mut self, bytes: usize) -> std::result::Result<(), BoundedExecutionError> {
        self.available = checked_size_add(self.available, bytes, "graph row memory")?;
        Ok(())
    }

    fn release(
        &mut self,
        context: &Arc<RequestContext>,
    ) -> std::result::Result<(), BoundedExecutionError> {
        let held = std::mem::take(&mut self.available);
        self.block = 0;
        context.release(held)
    }
}

struct GraphPathState {
    bindings: Vec<uuid::Uuid>,
    node: uuid::Uuid,
    total_depth: u32,
    step_index: usize,
    retained_bytes: usize,
}

struct GraphActiveState {
    path: GraphPathState,
    cursor: contextdb_graph::mem::BoundedBfsCursor,
}

enum VectorPreparation {
    Candidates(Option<Box<PullNode>>),
    Search,
    Materialize,
    Output,
    Exhausted,
}

enum VectorCandidateSource {
    Brute(contextdb_vector::mem::BoundedBruteForceCursor),
}

struct RankedVectorRow {
    row_id: RowId,
    rank: f32,
    vector_score: f32,
    row: PulledRow,
}

struct VectorState {
    table: String,
    index: VectorIndexRef,
    query: Vec<f32>,
    _query_bytes: usize,
    k: usize,
    candidate_k: usize,
    sort_key: Option<String>,
    #[allow(dead_code)]
    params: Arc<HashMap<String, Value>>,
    snapshot: SnapshotId,
    _schema: crate::database::VectorSchemaReadGuard,
    preparation: VectorPreparation,
    candidate_filter: bool,
    candidate_ids: Vec<u64>,
    candidate_bytes: usize,
    source: Option<VectorCandidateSource>,
    scores: Vec<(RowId, f32)>,
    score_bytes: usize,
    /// The table's column names, in order, so a row read by identity is
    /// projected exactly as the source used to project it.
    schema_columns: Vec<String>,
    /// The quantized vector columns THIS answer names. A search fetches and
    /// projects its own top-k rows, so it fills them itself -- otherwise a
    /// column the caller asked for comes back empty from a search while the
    /// same column read through a scan comes back whole.
    supplement_columns: Vec<String>,
    /// How many of the scored rows have been materialised.
    materialized: usize,
    ranked_rows: Vec<RankedVectorRow>,
    ranked_container_bytes: usize,
    output: VecDeque<PulledRow>,
    output_container_bytes: usize,
    used_hnsw: bool,
    /// What this handle's own open transaction changes about the vectors this
    /// search can see. `None` whenever no transaction is open or it has
    /// touched nothing about this index -- which is every read of a committed
    /// file and every read by a handle that is not mid-write.
    vector_overlay: Option<super::TransactionVectorOverlay>,
    overlay_bytes: usize,
    /// The store-wide reservation this search holds while it reads the
    /// transaction's own vectors, released when the search is done with them.
    /// The request's own ceiling is charged separately and governs this
    /// request; this one is what the STORE's limit governs.
    _overlay_reservation: Option<Box<dyn Send + Sync>>,
}

struct ProjectState {
    input: Box<PullNode>,
    columns: Vec<ProjectColumn>,
    params: Arc<HashMap<String, Value>>,
    aggregate_done: bool,
    aggregate: bool,
    pending: Option<PulledRow>,
    /// One running answer per projected aggregate, kept between cursor
    /// fetches so an aggregate too large for one page's work budget finishes
    /// on a later one.
    aggregates: Option<Vec<AggregateState>>,
    aggregate_state_bytes: usize,
}

struct SortState {
    input: Box<PullNode>,
    keys: Vec<SortKey>,
    params: Arc<HashMap<String, Value>>,
    rows: Vec<SortEntry>,
    prepared: bool,
    pending: Option<PulledRow>,
    /// What the buffered rows are holding, kept as a RUNNING total rather than
    /// summed on demand. A sort is asked what it holds twice a page, and a
    /// walk of the buffer would make that question cost what the buffer holds
    /// -- so answering it would grow with the answer still to be delivered,
    /// which is the shape paging exists to avoid. Moved at the two places the
    /// bytes themselves move: where a row is admitted into the buffer, and
    /// where it leaves.
    buffered_bytes: usize,
}

struct SortEntry {
    keys: Vec<Value>,
    key_bytes: usize,
    row: PulledRow,
}

struct LimitState {
    input: Box<PullNode>,
    count: u64,
    emitted: u64,
}

struct FilterState {
    input: Box<PullNode>,
    predicate: Expr,
    params: Arc<HashMap<String, Value>>,
    pending: Option<PulledRow>,
}

struct DistinctState {
    input: Box<PullNode>,
    seen: HashSet<Vec<u8>>,
    seen_bytes: usize,
    pending: Option<PulledRow>,
}

struct JoinState {
    left: Box<PullNode>,
    right: Box<PullNode>,
    condition: Expr,
    condition_columns: Vec<String>,
    right_column_count: usize,
    join_type: contextdb_planner::JoinType,
    params: Arc<HashMap<String, Value>>,
    right_rows: Vec<PulledRow>,
    right_pending: Option<PulledRow>,
    right_prepared: bool,
    left_row: Option<PulledRow>,
    right_position: usize,
    matched: bool,
}

struct StaticState {
    rows: VecDeque<PulledRow>,
}

struct UnionState {
    inputs: Vec<PullNode>,
    input: usize,
    all: bool,
    seen: HashSet<Vec<u8>>,
    seen_bytes: usize,
    pending: Option<PulledRow>,
}

impl PullNode {
    fn next(
        &mut self,
        db: &dyn ReadExecutionTarget,
        context: &Arc<RequestContext>,
    ) -> std::result::Result<Option<PulledRow>, BoundedExecutionError> {
        match &mut self.kind {
            PullKind::Scan(state) => state.next(db, context),
            PullKind::Graph(state) => state.next(db, context),
            PullKind::Vector(state) => {
                let next = state.next(db, context);
                if state.used_hnsw {
                    // Runs on every row, so it has to say the same thing the
                    // second time it runs as the first. A catch-all that
                    // rewrote the label to the bare operator dropped the
                    // candidate source on the row after the one that earned
                    // the upgrade, which is how a read that scanned a table
                    // before searching it came back describing no scan at
                    // all. A label already upgraded is left exactly as it is.
                    self.trace.physical_plan = match self.trace.physical_plan {
                        "IndexScan -> VectorSearch" => "IndexScan -> HNSWSearch",
                        "Scan -> VectorSearch" => "Scan -> HNSWSearch",
                        "VectorSearch" => "HNSWSearch",
                        already_named => already_named,
                    };
                }
                next
            }
            PullKind::Project(state) => state.next(db, context),
            PullKind::Sort(state) => state.next(db, context),
            PullKind::Limit(state) => state.next(db, context),
            PullKind::Filter(state) => state.next(db, context),
            PullKind::Distinct(state) => state.next(db, context),
            PullKind::Join(state) => state.next(db, context),
            PullKind::Static(state) => state.next(),
            PullKind::Union(state) => state.next(db, context),
        }
    }

    /// The plan word this node publishes, asked once the read has run.
    ///
    /// A wrapper node copies its input's description when it is BUILT, so a
    /// label the run itself decides -- an approximate vector index the source
    /// only discovers it can use once it touches the data -- never reaches a
    /// copy taken before the first row. Asking the node that owns the label,
    /// at the moment the answer is published, is what makes the two doors
    /// describe the same run. Each arm delegates to the same input the node
    /// copied its trace from when it was built.
    ///
    /// A sort is the one wrapper that has a word of its own: a source whose
    /// label already implies an order keeps it, and any other source is
    /// described as the sort it really became -- the same rule, from the same
    /// function, the executor applies.
    fn published_plan(&self) -> &'static str {
        match &self.kind {
            PullKind::Scan(_) | PullKind::Graph(_) | PullKind::Vector(_) | PullKind::Static(_) => {
                self.trace.physical_plan
            }
            PullKind::Project(state) => state.input.published_plan(),
            PullKind::Limit(state) => state.input.published_plan(),
            PullKind::Filter(state) => state.input.published_plan(),
            PullKind::Distinct(state) => state.input.published_plan(),
            PullKind::Join(state) => state.left.published_plan(),
            PullKind::Union(state) => state
                .inputs
                .first()
                .map_or(self.trace.physical_plan, PullNode::published_plan),
            PullKind::Sort(state) => {
                let input = state.input.published_plan();
                if super::trace_label_survives_sort(input) {
                    input
                } else {
                    "Sort"
                }
            }
        }
    }

    /// `Some` is an exact answer. `None` means filters/deduplication may need
    /// an admitted one-row lookahead before exhaustion can be published.
    fn has_more_hint(&self) -> Option<bool> {
        match &self.kind {
            PullKind::Scan(state) => state.has_more_hint(),
            PullKind::Graph(state) => state.has_more_hint(),
            PullKind::Vector(state) => state.has_more_hint(),
            PullKind::Project(state) => {
                if state.aggregate {
                    Some(!state.aggregate_done)
                } else {
                    state
                        .pending
                        .as_ref()
                        .map(|_| true)
                        .or_else(|| state.input.has_more_hint())
                }
            }
            PullKind::Sort(state) if state.prepared => Some(!state.rows.is_empty()),
            PullKind::Sort(state) if state.pending.is_some() || !state.rows.is_empty() => {
                Some(true)
            }
            PullKind::Sort(state) => match state.input.has_more_hint() {
                Some(false) => Some(false),
                _ => None,
            },
            PullKind::Limit(state) => {
                if state.emitted >= state.count {
                    Some(false)
                } else {
                    state.input.has_more_hint()
                }
            }
            PullKind::Filter(state) => {
                if state.pending.is_some() {
                    None
                } else {
                    match state.input.has_more_hint() {
                        Some(false) => Some(false),
                        _ => None,
                    }
                }
            }
            PullKind::Distinct(state) => {
                if state.pending.is_some() {
                    None
                } else {
                    match state.input.has_more_hint() {
                        Some(false) => Some(false),
                        _ => None,
                    }
                }
            }
            PullKind::Join(state) => {
                if state.left_row.is_some() {
                    None
                } else {
                    match state.left.has_more_hint() {
                        Some(false) if state.right_prepared => Some(false),
                        _ => None,
                    }
                }
            }
            PullKind::Static(state) => Some(!state.rows.is_empty()),
            PullKind::Union(state) => {
                if state.pending.is_some() {
                    None
                } else if state.input >= state.inputs.len() {
                    Some(false)
                } else if state.all {
                    let mut any_unknown = false;
                    for input in &state.inputs[state.input..] {
                        match input.has_more_hint() {
                            Some(true) => return Some(true),
                            Some(false) => {}
                            None => any_unknown = true,
                        }
                    }
                    (!any_unknown).then_some(false)
                } else {
                    None
                }
            }
        }
    }
}

fn bounded_size_error(operation: &str) -> BoundedExecutionError {
    Error::Other(format!(
        "bounded {operation} memory size exceeds the native address space"
    ))
    .into()
}

fn checked_size_add(
    left: usize,
    right: usize,
    operation: &str,
) -> std::result::Result<usize, BoundedExecutionError> {
    left.checked_add(right)
        .ok_or_else(|| bounded_size_error(operation))
}

fn checked_size_mul(
    left: usize,
    right: usize,
    operation: &str,
) -> std::result::Result<usize, BoundedExecutionError> {
    left.checked_mul(right)
        .ok_or_else(|| bounded_size_error(operation))
}

fn decimal_digits_u64(mut value: u64) -> usize {
    let mut digits = 1usize;
    while value >= 10 {
        value /= 10;
        digits += 1;
    }
    digits
}

fn reconcile_new_reservation(
    context: &Arc<RequestContext>,
    source: BoundedWorkSource,
    planned: usize,
    actual: usize,
) -> std::result::Result<(), BoundedExecutionError> {
    if actual > planned {
        if let Err(error) = context.reserve(source, actual - planned) {
            context.release(planned)?;
            return Err(error);
        }
        Ok(())
    } else if planned > actual {
        context.release(planned - actual)
    } else {
        Ok(())
    }
}

fn strings_capacity_bytes(
    values: &Vec<String>,
    operation: &str,
) -> std::result::Result<usize, BoundedExecutionError> {
    values.iter().try_fold(
        checked_size_mul(values.capacity(), std::mem::size_of::<String>(), operation)?,
        |bytes, value| checked_size_add(bytes, value.capacity(), operation),
    )
}

fn clone_strings_with_reservation(
    source: &[String],
    context: &Arc<RequestContext>,
    operation: &str,
) -> std::result::Result<(Vec<String>, usize), BoundedExecutionError> {
    let planned = source.iter().try_fold(
        checked_size_mul(source.len(), std::mem::size_of::<String>(), operation)?,
        |bytes, value| checked_size_add(bytes, value.len(), operation),
    )?;
    context.reserve(BoundedWorkSource::TableScan, planned)?;
    let mut values = Vec::new();
    if values.try_reserve_exact(source.len()).is_err() {
        context.release(planned)?;
        return Err(Error::Other(format!("bounded {operation} allocation failed")).into());
    }
    for source_value in source {
        let mut value = String::new();
        if value.try_reserve_exact(source_value.len()).is_err() {
            context.release(planned)?;
            return Err(Error::Other(format!("bounded {operation} allocation failed")).into());
        }
        value.push_str(source_value);
        values.push(value);
    }
    let actual = strings_capacity_bytes(&values, operation)?;
    reconcile_new_reservation(context, BoundedWorkSource::TableScan, planned, actual)?;
    Ok((values, actual))
}

fn values_capacity_allocation_bytes(
    values: &Vec<Value>,
) -> std::result::Result<usize, BoundedExecutionError> {
    let mut bytes = checked_size_mul(
        values.capacity(),
        std::mem::size_of::<Value>(),
        "result row values",
    )?;
    for value in values {
        bytes = checked_size_add(bytes, value_capacity_bytes(value)?, "result row value")?;
    }
    Ok(bytes)
}

fn values_retained_bytes(values: &Vec<Value>) -> std::result::Result<usize, BoundedExecutionError> {
    checked_size_add(
        std::mem::size_of::<Vec<Value>>(),
        values_capacity_allocation_bytes(values)?,
        "result row",
    )
}

fn value_refs_retained_bytes<'a>(
    values: impl IntoIterator<Item = &'a Value>,
    count: usize,
) -> std::result::Result<usize, BoundedExecutionError> {
    let mut bytes = checked_size_add(
        std::mem::size_of::<Vec<Value>>(),
        checked_size_mul(count, std::mem::size_of::<Value>(), "projected values")?,
        "projected row",
    )?;
    for value in values {
        bytes = checked_size_add(bytes, value_capacity_bytes(value)?, "projected value")?;
    }
    Ok(bytes)
}

fn params_retained_bytes(
    params: &HashMap<String, Value>,
) -> std::result::Result<usize, BoundedExecutionError> {
    let mut bytes = checked_size_add(
        std::mem::size_of::<HashMap<String, Value>>(),
        checked_size_mul(
            params.capacity(),
            std::mem::size_of::<(String, Value)>(),
            "parameter capacity",
        )?,
        "parameters",
    )?;
    for (name, value) in params {
        bytes = checked_size_add(bytes, name.capacity(), "parameter name")?;
        bytes = checked_size_add(bytes, value_capacity_bytes(value)?, "parameter value")?;
    }
    Ok(bytes)
}

fn optional_string_capacity(value: &Option<String>) -> usize {
    value.as_ref().map_or(0, String::capacity)
}

fn literal_capacity_bytes(literal: &Literal) -> std::result::Result<usize, BoundedExecutionError> {
    match literal {
        Literal::Text(value) => Ok(value.capacity()),
        Literal::Vector(values) => checked_size_mul(
            values.capacity(),
            std::mem::size_of::<f32>(),
            "literal vector capacity",
        ),
        Literal::Null | Literal::Bool(_) | Literal::Integer(_) | Literal::Real(_) => Ok(0),
    }
}

fn expr_capacity_bytes(expr: &Expr) -> std::result::Result<usize, BoundedExecutionError> {
    let boxed_expr = std::mem::size_of::<Expr>();
    match expr {
        Expr::Column(column) => checked_size_add(
            optional_string_capacity(&column.table),
            column.column.capacity(),
            "expression column",
        ),
        Expr::Literal(literal) => literal_capacity_bytes(literal),
        Expr::Parameter(parameter) => Ok(parameter.capacity()),
        Expr::BinaryOp { left, right, .. } | Expr::CosineDistance { left, right } => {
            let children = checked_size_add(
                expr_capacity_bytes(left)?,
                expr_capacity_bytes(right)?,
                "expression children",
            )?;
            checked_size_add(
                checked_size_mul(2, boxed_expr, "expression boxes")?,
                children,
                "expression boxes",
            )
        }
        Expr::UnaryOp { operand, .. } | Expr::IsNull { expr: operand, .. } => {
            checked_size_add(boxed_expr, expr_capacity_bytes(operand)?, "expression box")
        }
        Expr::FunctionCall { name, args } => {
            let mut bytes = checked_size_add(
                name.capacity(),
                checked_size_mul(args.capacity(), boxed_expr, "function arguments")?,
                "function expression",
            )?;
            for argument in args {
                bytes =
                    checked_size_add(bytes, expr_capacity_bytes(argument)?, "function expression")?;
            }
            Ok(bytes)
        }
        Expr::RowVectorSource { table, column, key } => {
            let strings =
                checked_size_add(table.capacity(), column.capacity(), "ROW_VECTOR expression")?;
            checked_size_add(
                strings,
                checked_size_add(boxed_expr, expr_capacity_bytes(key)?, "ROW_VECTOR key")?,
                "ROW_VECTOR expression",
            )
        }
        Expr::InList { expr, list, .. } => {
            let mut bytes =
                checked_size_add(boxed_expr, expr_capacity_bytes(expr)?, "IN expression")?;
            bytes = checked_size_add(
                bytes,
                checked_size_mul(list.capacity(), boxed_expr, "IN-list capacity")?,
                "IN expression",
            )?;
            for item in list {
                bytes = checked_size_add(bytes, expr_capacity_bytes(item)?, "IN expression")?;
            }
            Ok(bytes)
        }
        Expr::Like { expr, pattern, .. } => {
            let children = checked_size_add(
                expr_capacity_bytes(expr)?,
                expr_capacity_bytes(pattern)?,
                "LIKE expression",
            )?;
            checked_size_add(
                checked_size_mul(2, boxed_expr, "LIKE expression boxes")?,
                children,
                "LIKE expression",
            )
        }
        Expr::InSubquery { expr, subquery, .. } => {
            let left = checked_size_add(
                boxed_expr,
                expr_capacity_bytes(expr)?,
                "subquery expression",
            )?;
            checked_size_add(
                left,
                checked_size_add(
                    std::mem::size_of::<contextdb_parser::ast::SelectBody>(),
                    select_body_capacity_bytes(subquery)?,
                    "subquery body",
                )?,
                "subquery expression",
            )
        }
    }
}

fn select_body_capacity_bytes(
    body: &contextdb_parser::ast::SelectBody,
) -> std::result::Result<usize, BoundedExecutionError> {
    use contextdb_parser::ast::{FromItem, SelectColumn};

    let mut bytes = checked_size_mul(
        body.columns.capacity(),
        std::mem::size_of::<SelectColumn>(),
        "subquery columns",
    )?;
    for column in &body.columns {
        bytes = checked_size_add(bytes, expr_capacity_bytes(&column.expr)?, "subquery")?;
        bytes = checked_size_add(bytes, optional_string_capacity(&column.alias), "subquery")?;
    }
    bytes = checked_size_add(
        bytes,
        checked_size_mul(
            body.from.capacity(),
            std::mem::size_of::<FromItem>(),
            "subquery sources",
        )?,
        "subquery",
    )?;
    for source in &body.from {
        match source {
            FromItem::Table { name, alias } => {
                bytes = checked_size_add(bytes, name.capacity(), "subquery source")?;
                bytes =
                    checked_size_add(bytes, optional_string_capacity(alias), "subquery source")?;
            }
            FromItem::GraphTable {
                graph_name,
                match_clause,
                columns,
            } => {
                bytes = checked_size_add(bytes, graph_name.capacity(), "subquery graph")?;
                bytes = checked_size_add(
                    bytes,
                    match_clause_capacity_bytes(match_clause)?,
                    "subquery graph",
                )?;
                bytes = checked_size_add(
                    bytes,
                    checked_size_mul(
                        columns.capacity(),
                        std::mem::size_of::<contextdb_parser::ast::GraphTableColumn>(),
                        "subquery graph columns",
                    )?,
                    "subquery graph",
                )?;
                for column in columns {
                    bytes = checked_size_add(
                        bytes,
                        expr_capacity_bytes(&column.expr)?,
                        "subquery graph column",
                    )?;
                    bytes =
                        checked_size_add(bytes, column.alias.capacity(), "subquery graph column")?;
                }
            }
        }
    }
    bytes = checked_size_add(
        bytes,
        checked_size_mul(
            body.joins.capacity(),
            std::mem::size_of::<contextdb_parser::ast::JoinClause>(),
            "subquery joins",
        )?,
        "subquery",
    )?;
    for join in &body.joins {
        bytes = checked_size_add(bytes, join.table.capacity(), "subquery join")?;
        bytes = checked_size_add(
            bytes,
            optional_string_capacity(&join.alias),
            "subquery join",
        )?;
        bytes = checked_size_add(bytes, expr_capacity_bytes(&join.on)?, "subquery join")?;
    }
    if let Some(filter) = body.where_clause.as_ref() {
        bytes = checked_size_add(bytes, expr_capacity_bytes(filter)?, "subquery filter")?;
    }
    bytes = checked_size_add(
        bytes,
        checked_size_mul(
            body.order_by.capacity(),
            std::mem::size_of::<contextdb_parser::ast::OrderByItem>(),
            "subquery ordering",
        )?,
        "subquery",
    )?;
    for ordering in &body.order_by {
        bytes = checked_size_add(
            bytes,
            expr_capacity_bytes(&ordering.expr)?,
            "subquery ordering",
        )?;
    }
    checked_size_add(bytes, optional_string_capacity(&body.use_rank), "subquery")
}

fn match_clause_capacity_bytes(
    clause: &contextdb_parser::ast::MatchClause,
) -> std::result::Result<usize, BoundedExecutionError> {
    let mut bytes = optional_string_capacity(&clause.graph_name);
    bytes = checked_size_add(
        bytes,
        node_pattern_capacity_bytes(&clause.pattern.start)?,
        "MATCH clause",
    )?;
    bytes = checked_size_add(
        bytes,
        checked_size_mul(
            clause.pattern.edges.capacity(),
            std::mem::size_of::<contextdb_parser::ast::EdgeStep>(),
            "MATCH edges",
        )?,
        "MATCH clause",
    )?;
    for edge in &clause.pattern.edges {
        bytes = checked_size_add(
            bytes,
            optional_string_capacity(&edge.edge_type),
            "MATCH edge",
        )?;
        bytes = checked_size_add(bytes, optional_string_capacity(&edge.alias), "MATCH edge")?;
        bytes = checked_size_add(
            bytes,
            node_pattern_capacity_bytes(&edge.target)?,
            "MATCH edge",
        )?;
    }
    if let Some(filter) = clause.where_clause.as_ref() {
        bytes = checked_size_add(bytes, expr_capacity_bytes(filter)?, "MATCH filter")?;
    }
    bytes = checked_size_add(
        bytes,
        checked_size_mul(
            clause.return_cols.capacity(),
            std::mem::size_of::<contextdb_parser::ast::ReturnCol>(),
            "MATCH return columns",
        )?,
        "MATCH clause",
    )?;
    for column in &clause.return_cols {
        bytes = checked_size_add(bytes, expr_capacity_bytes(&column.expr)?, "MATCH return")?;
        bytes = checked_size_add(
            bytes,
            optional_string_capacity(&column.alias),
            "MATCH return",
        )?;
    }
    Ok(bytes)
}

fn node_pattern_capacity_bytes(
    node: &contextdb_parser::ast::NodePattern,
) -> std::result::Result<usize, BoundedExecutionError> {
    let mut bytes = checked_size_add(
        node.alias.capacity(),
        optional_string_capacity(&node.label),
        "MATCH node",
    )?;
    bytes = checked_size_add(
        bytes,
        checked_size_mul(
            node.properties.capacity(),
            std::mem::size_of::<(String, Expr)>(),
            "MATCH properties",
        )?,
        "MATCH node",
    )?;
    for (name, value) in &node.properties {
        bytes = checked_size_add(bytes, name.capacity(), "MATCH property")?;
        bytes = checked_size_add(bytes, expr_capacity_bytes(value)?, "MATCH property")?;
    }
    Ok(bytes)
}

fn ctes_capacity_bytes(ctes: &Vec<Cte>) -> std::result::Result<usize, BoundedExecutionError> {
    let mut bytes = checked_size_mul(ctes.capacity(), std::mem::size_of::<Cte>(), "CTE plans")?;
    for cte in ctes {
        match cte {
            Cte::SqlCte { name, query } => {
                bytes = checked_size_add(bytes, name.capacity(), "CTE plan")?;
                bytes = checked_size_add(bytes, select_body_capacity_bytes(query)?, "CTE plan")?;
            }
            Cte::MatchCte { name, match_clause } => {
                bytes = checked_size_add(bytes, name.capacity(), "CTE plan")?;
                bytes = checked_size_add(
                    bytes,
                    match_clause_capacity_bytes(match_clause)?,
                    "CTE plan",
                )?;
            }
        }
    }
    Ok(bytes)
}

fn statement_capacity_bytes(
    statement: &Statement,
) -> std::result::Result<usize, BoundedExecutionError> {
    match statement {
        Statement::Select(select) => checked_size_add(
            ctes_capacity_bytes(&select.ctes)?,
            select_body_capacity_bytes(&select.body)?,
            "parsed SELECT",
        ),
        _ => Ok(0),
    }
}

fn physical_plan_capacity_bytes(
    plan: &PhysicalPlan,
) -> std::result::Result<usize, BoundedExecutionError> {
    let boxed_plan = std::mem::size_of::<PhysicalPlan>();
    match plan {
        PhysicalPlan::Scan {
            table,
            alias,
            filter,
        } => {
            let mut bytes = checked_size_add(
                table.capacity(),
                optional_string_capacity(alias),
                "scan plan",
            )?;
            if let Some(filter) = filter.as_ref() {
                bytes = checked_size_add(bytes, expr_capacity_bytes(filter)?, "scan plan")?;
            }
            Ok(bytes)
        }
        PhysicalPlan::IndexScan {
            table,
            index,
            range,
        } => {
            let mut bytes = checked_size_add(table.capacity(), index.capacity(), "index plan")?;
            bytes = checked_size_add(
                bytes,
                bound_value_capacity_bytes(&range.lower)?,
                "index plan",
            )?;
            bytes = checked_size_add(
                bytes,
                bound_value_capacity_bytes(&range.upper)?,
                "index plan",
            )?;
            if let Some(value) = range.equality.as_ref() {
                bytes = checked_size_add(bytes, value_capacity_bytes(value)?, "index plan")?;
            }
            Ok(bytes)
        }
        PhysicalPlan::GraphBfs {
            start_alias,
            start_expr,
            start_candidates,
            filter_ctes,
            steps,
            filter,
        } => {
            let mut bytes = checked_size_add(
                start_alias.capacity(),
                expr_capacity_bytes(start_expr)?,
                "graph plan",
            )?;
            if let Some(candidates) = start_candidates.as_deref() {
                bytes = checked_size_add(
                    bytes,
                    checked_size_add(
                        boxed_plan,
                        physical_plan_capacity_bytes(candidates)?,
                        "graph candidate plan",
                    )?,
                    "graph plan",
                )?;
            }
            bytes = checked_size_add(bytes, ctes_capacity_bytes(filter_ctes)?, "graph plan")?;
            bytes = checked_size_add(bytes, graph_steps_capacity_bytes(steps)?, "graph plan")?;
            if let Some(filter) = filter.as_ref() {
                bytes = checked_size_add(bytes, expr_capacity_bytes(filter)?, "graph plan")?;
            }
            Ok(bytes)
        }
        PhysicalPlan::VectorSearch {
            table,
            column,
            query_expr,
            candidates,
            sort_key,
            ..
        }
        | PhysicalPlan::HnswSearch {
            table,
            column,
            query_expr,
            candidates,
            sort_key,
            ..
        } => {
            let mut bytes = checked_size_add(table.capacity(), column.capacity(), "vector plan")?;
            bytes = checked_size_add(bytes, expr_capacity_bytes(query_expr)?, "vector plan")?;
            bytes = checked_size_add(bytes, optional_string_capacity(sort_key), "vector plan")?;
            if let Some(candidates) = candidates.as_deref() {
                bytes = checked_size_add(
                    bytes,
                    checked_size_add(
                        boxed_plan,
                        physical_plan_capacity_bytes(candidates)?,
                        "vector candidate plan",
                    )?,
                    "vector plan",
                )?;
            }
            Ok(bytes)
        }
        PhysicalPlan::Filter { input, predicate } => checked_size_add(
            checked_size_add(
                boxed_plan,
                physical_plan_capacity_bytes(input)?,
                "filter input plan",
            )?,
            expr_capacity_bytes(predicate)?,
            "filter plan",
        ),
        PhysicalPlan::Project { input, columns } => checked_size_add(
            checked_size_add(
                boxed_plan,
                physical_plan_capacity_bytes(input)?,
                "projection input plan",
            )?,
            project_columns_capacity_bytes(columns)?,
            "projection plan",
        ),
        PhysicalPlan::Distinct { input } | PhysicalPlan::Limit { input, .. } => checked_size_add(
            boxed_plan,
            physical_plan_capacity_bytes(input)?,
            "unary plan",
        ),
        PhysicalPlan::Join {
            left,
            right,
            condition,
            left_alias,
            right_alias,
            ..
        } => {
            let mut bytes = checked_size_add(
                checked_size_add(boxed_plan, physical_plan_capacity_bytes(left)?, "join plan")?,
                checked_size_add(
                    boxed_plan,
                    physical_plan_capacity_bytes(right)?,
                    "join plan",
                )?,
                "join plan",
            )?;
            bytes = checked_size_add(bytes, expr_capacity_bytes(condition)?, "join plan")?;
            bytes = checked_size_add(bytes, optional_string_capacity(left_alias), "join plan")?;
            checked_size_add(bytes, optional_string_capacity(right_alias), "join plan")
        }
        PhysicalPlan::Sort { input, keys } => checked_size_add(
            checked_size_add(
                boxed_plan,
                physical_plan_capacity_bytes(input)?,
                "sort input plan",
            )?,
            sort_keys_capacity_bytes(keys)?,
            "sort plan",
        ),
        PhysicalPlan::MaterializeCte { name, input } => checked_size_add(
            name.capacity(),
            checked_size_add(
                boxed_plan,
                physical_plan_capacity_bytes(input)?,
                "CTE input plan",
            )?,
            "CTE plan",
        ),
        PhysicalPlan::CteRef { name } => Ok(name.capacity()),
        PhysicalPlan::Union { inputs, .. } | PhysicalPlan::Pipeline(inputs) => {
            let mut bytes = checked_size_mul(inputs.capacity(), boxed_plan, "multi-input plan")?;
            for input in inputs {
                bytes = checked_size_add(
                    bytes,
                    physical_plan_capacity_bytes(input)?,
                    "multi-input plan",
                )?;
            }
            Ok(bytes)
        }
        _ => Ok(0),
    }
}

fn string_vec_capacity_bytes(
    values: &Vec<String>,
    operation: &str,
) -> std::result::Result<usize, BoundedExecutionError> {
    strings_capacity_bytes(values, operation)
}

fn table_meta_capacity_bytes(
    meta: &TableMeta,
) -> std::result::Result<usize, BoundedExecutionError> {
    use contextdb_core::{PropagationRule, ScopeLabelKind};

    let mut bytes = checked_size_mul(
        meta.columns.capacity(),
        std::mem::size_of::<contextdb_core::ColumnDef>(),
        "table metadata columns",
    )?;
    for column in &meta.columns {
        bytes = checked_size_add(bytes, column.name.capacity(), "table metadata column")?;
        bytes = checked_size_add(
            bytes,
            optional_string_capacity(&column.default),
            "table metadata column",
        )?;
        if let Some(reference) = column.references.as_ref() {
            bytes = checked_size_add(bytes, reference.table.capacity(), "foreign key")?;
            bytes = checked_size_add(bytes, reference.column.capacity(), "foreign key")?;
        }
        if let Some(rank) = column.rank_policy.as_ref() {
            for value in [
                &rank.joined_table,
                &rank.joined_column,
                &rank.anchor_column,
                &rank.sort_key,
                &rank.formula,
                &rank.protected_index,
            ] {
                bytes = checked_size_add(bytes, value.capacity(), "rank policy")?;
            }
        }
        if let Some(scope) = column.scope_label.as_ref() {
            match scope {
                ScopeLabelKind::Simple { write_labels } => {
                    bytes = checked_size_add(
                        bytes,
                        string_vec_capacity_bytes(write_labels, "scope labels")?,
                        "scope labels",
                    )?;
                }
                ScopeLabelKind::Split {
                    read_labels,
                    write_labels,
                } => {
                    bytes = checked_size_add(
                        bytes,
                        string_vec_capacity_bytes(read_labels, "scope labels")?,
                        "scope labels",
                    )?;
                    bytes = checked_size_add(
                        bytes,
                        string_vec_capacity_bytes(write_labels, "scope labels")?,
                        "scope labels",
                    )?;
                }
            }
        }
        if let Some(acl) = column.acl_ref.as_ref() {
            bytes = checked_size_add(bytes, acl.ref_table.capacity(), "ACL reference")?;
            bytes = checked_size_add(bytes, acl.ref_column.capacity(), "ACL reference")?;
        }
    }
    if let Some(machine) = meta.state_machine.as_ref() {
        bytes = checked_size_add(bytes, machine.column.capacity(), "state machine")?;
        bytes = checked_size_add(
            bytes,
            checked_size_mul(
                machine.transitions.capacity(),
                std::mem::size_of::<(String, Vec<String>)>(),
                "state-machine transitions",
            )?,
            "state machine",
        )?;
        for (state, targets) in &machine.transitions {
            bytes = checked_size_add(bytes, state.capacity(), "state machine")?;
            bytes = checked_size_add(
                bytes,
                string_vec_capacity_bytes(targets, "state-machine targets")?,
                "state machine",
            )?;
        }
    }
    bytes = checked_size_add(
        bytes,
        string_vec_capacity_bytes(&meta.dag_edge_types, "DAG edge types")?,
        "table metadata",
    )?;
    bytes = checked_size_add(
        bytes,
        checked_size_mul(
            meta.unique_constraints.capacity(),
            std::mem::size_of::<Vec<String>>(),
            "unique constraints",
        )?,
        "table metadata",
    )?;
    for constraint in &meta.unique_constraints {
        bytes = checked_size_add(
            bytes,
            string_vec_capacity_bytes(constraint, "unique constraint")?,
            "unique constraint",
        )?;
    }
    bytes = checked_size_add(
        bytes,
        optional_string_capacity(&meta.natural_key_column),
        "table metadata",
    )?;
    bytes = checked_size_add(
        bytes,
        checked_size_mul(
            meta.propagation_rules.capacity(),
            std::mem::size_of::<PropagationRule>(),
            "propagation rules",
        )?,
        "table metadata",
    )?;
    for rule in &meta.propagation_rules {
        match rule {
            PropagationRule::ForeignKey {
                fk_column,
                referenced_table,
                referenced_column,
                trigger_state,
                target_state,
                ..
            } => {
                for value in [
                    fk_column,
                    referenced_table,
                    referenced_column,
                    trigger_state,
                    target_state,
                ] {
                    bytes = checked_size_add(bytes, value.capacity(), "propagation rule")?;
                }
            }
            PropagationRule::Edge {
                edge_type,
                trigger_state,
                target_state,
                ..
            } => {
                for value in [edge_type, trigger_state, target_state] {
                    bytes = checked_size_add(bytes, value.capacity(), "propagation rule")?;
                }
            }
            PropagationRule::VectorExclusion { trigger_state } => {
                bytes = checked_size_add(bytes, trigger_state.capacity(), "propagation rule")?;
            }
        }
    }
    bytes = checked_size_add(
        bytes,
        optional_string_capacity(&meta.expires_column),
        "table metadata",
    )?;
    bytes = checked_size_add(
        bytes,
        checked_size_mul(
            meta.indexes.capacity(),
            std::mem::size_of::<contextdb_core::IndexDecl>(),
            "table indexes",
        )?,
        "table metadata",
    )?;
    for index in &meta.indexes {
        bytes = checked_size_add(bytes, index.name.capacity(), "table index")?;
        bytes = checked_size_add(
            bytes,
            checked_size_mul(
                index.columns.capacity(),
                std::mem::size_of::<(String, contextdb_core::SortDirection)>(),
                "table index columns",
            )?,
            "table index",
        )?;
        for (column, _) in &index.columns {
            bytes = checked_size_add(bytes, column.capacity(), "table index column")?;
        }
    }
    bytes = checked_size_add(
        bytes,
        checked_size_mul(
            meta.composite_foreign_keys.capacity(),
            std::mem::size_of::<contextdb_core::CompositeForeignKey>(),
            "composite foreign keys",
        )?,
        "table metadata",
    )?;
    for foreign_key in &meta.composite_foreign_keys {
        bytes = checked_size_add(
            bytes,
            string_vec_capacity_bytes(&foreign_key.child_columns, "foreign-key columns")?,
            "composite foreign key",
        )?;
        bytes = checked_size_add(
            bytes,
            foreign_key.parent_table.capacity(),
            "composite foreign key",
        )?;
        bytes = checked_size_add(
            bytes,
            string_vec_capacity_bytes(&foreign_key.parent_columns, "foreign-key columns")?,
            "composite foreign key",
        )?;
    }
    bytes = checked_size_add(
        bytes,
        string_vec_capacity_bytes(&meta.primary_key_columns, "primary-key columns")?,
        "table metadata",
    )?;
    Ok(bytes)
}

/// What the ONE copy of a pulled row this read is holding really costs.
///
/// The store charges before it clones, using the retained-representation
/// estimate -- it has to, because a charge that arrives after the allocation
/// guarantees nothing. That estimate covers the worst any consumer retains
/// (a graph edge keeps a property map in both adjacency directions), and a
/// read is not that consumer: it holds one copy. So once the row is in hand
/// the reservation is trued up to this, which is measured off the row itself.
fn pulled_row_retained_bytes(
    row: &VersionedRow,
) -> std::result::Result<usize, BoundedExecutionError> {
    let mut bytes = checked_size_add(
        std::mem::size_of::<VersionedRow>(),
        checked_size_mul(
            row.values.capacity(),
            std::mem::size_of::<(String, Value)>(),
            "pulled row entries",
        )?,
        "pulled row",
    )?;
    for (column, value) in &row.values {
        bytes = checked_size_add(bytes, column.capacity(), "pulled row column")?;
        bytes = checked_size_add(bytes, value_capacity_bytes(value)?, "pulled row value")?;
    }
    Ok(bytes)
}

fn value_capacity_bytes(value: &Value) -> std::result::Result<usize, BoundedExecutionError> {
    match value {
        Value::Text(value) => Ok(value.capacity()),
        Value::Vector(values) => checked_size_mul(
            values.capacity(),
            std::mem::size_of::<f32>(),
            "value vector capacity",
        ),
        Value::Json(value) => json_capacity_bytes(value),
        Value::Null
        | Value::Bool(_)
        | Value::Int64(_)
        | Value::Float64(_)
        | Value::Uuid(_)
        | Value::Timestamp(_)
        | Value::TxId(_) => Ok(0),
    }
}

fn json_capacity_bytes(
    value: &serde_json::Value,
) -> std::result::Result<usize, BoundedExecutionError> {
    match value {
        serde_json::Value::String(value) => Ok(value.capacity()),
        serde_json::Value::Array(values) => {
            let mut bytes = checked_size_mul(
                values.capacity(),
                std::mem::size_of::<serde_json::Value>(),
                "JSON array capacity",
            )?;
            for value in values {
                bytes = checked_size_add(bytes, json_capacity_bytes(value)?, "JSON array")?;
            }
            Ok(bytes)
        }
        serde_json::Value::Object(values) => {
            let mut bytes = checked_size_mul(
                values.len(),
                std::mem::size_of::<(String, serde_json::Value)>(),
                "JSON object entries",
            )?;
            for (key, value) in values {
                bytes = checked_size_add(bytes, key.capacity(), "JSON object key")?;
                bytes = checked_size_add(bytes, json_capacity_bytes(value)?, "JSON object")?;
            }
            Ok(bytes)
        }
        serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::Number(_) => {
            Ok(0)
        }
    }
}

fn bound_value_capacity_bytes(
    bound: &std::ops::Bound<Value>,
) -> std::result::Result<usize, BoundedExecutionError> {
    match bound {
        std::ops::Bound::Included(value) | std::ops::Bound::Excluded(value) => {
            value_capacity_bytes(value)
        }
        std::ops::Bound::Unbounded => Ok(0),
    }
}

fn index_pick_capacity_bytes(
    pick: &IndexPick,
) -> std::result::Result<usize, BoundedExecutionError> {
    let mut bytes = pick.name.capacity();
    bytes = checked_size_add(
        bytes,
        checked_size_mul(
            pick.columns.capacity(),
            std::mem::size_of::<(String, contextdb_core::SortDirection)>(),
            "index-pick columns",
        )?,
        "index pick",
    )?;
    for (column, _) in &pick.columns {
        bytes = checked_size_add(bytes, column.capacity(), "index-pick column")?;
    }
    bytes = checked_size_add(bytes, pick.pushed_column.capacity(), "index pick")?;
    bytes = checked_size_add(
        bytes,
        string_vec_capacity_bytes(&pick.pushed_columns, "pushed index columns")?,
        "index pick",
    )?;
    bytes = checked_size_add(
        bytes,
        checked_size_mul(
            pick.suffix_values.capacity(),
            std::mem::size_of::<Value>(),
            "index suffix values",
        )?,
        "index pick",
    )?;
    for value in &pick.suffix_values {
        bytes = checked_size_add(bytes, value_capacity_bytes(value)?, "index suffix value")?;
    }
    match &pick.shape {
        IndexPredicateShape::Equality(value) | IndexPredicateShape::NotEqual(value) => {
            checked_size_add(bytes, value_capacity_bytes(value)?, "index predicate")
        }
        IndexPredicateShape::Range { lower, upper } => {
            bytes = checked_size_add(bytes, bound_value_capacity_bytes(lower)?, "index predicate")?;
            checked_size_add(bytes, bound_value_capacity_bytes(upper)?, "index predicate")
        }
        IndexPredicateShape::InList(values) => {
            bytes = checked_size_add(
                bytes,
                checked_size_mul(
                    values.capacity(),
                    std::mem::size_of::<Value>(),
                    "index IN values",
                )?,
                "index predicate",
            )?;
            for value in values {
                bytes = checked_size_add(bytes, value_capacity_bytes(value)?, "index predicate")?;
            }
            Ok(bytes)
        }
        IndexPredicateShape::IsNull | IndexPredicateShape::IsNotNull => Ok(bytes),
    }
}

fn query_trace_capacity_bytes(
    trace: &QueryTrace,
) -> std::result::Result<usize, BoundedExecutionError> {
    let mut bytes = trace.index_used.as_ref().map_or(0, String::capacity);
    if trace.predicates_pushed.spilled() {
        bytes = checked_size_add(
            bytes,
            checked_size_mul(
                trace.predicates_pushed.capacity(),
                std::mem::size_of::<std::borrow::Cow<'static, str>>(),
                "trace predicates",
            )?,
            "query trace",
        )?;
    }
    for predicate in &trace.predicates_pushed {
        if let std::borrow::Cow::Owned(predicate) = predicate {
            bytes = checked_size_add(bytes, predicate.capacity(), "trace predicate")?;
        }
    }
    if trace.indexes_considered.spilled() {
        bytes = checked_size_add(
            bytes,
            checked_size_mul(
                trace.indexes_considered.capacity(),
                std::mem::size_of::<crate::database::IndexCandidate>(),
                "trace index candidates",
            )?,
            "query trace",
        )?;
    }
    for candidate in &trace.indexes_considered {
        bytes = checked_size_add(bytes, candidate.name.capacity(), "trace index candidate")?;
        if let std::borrow::Cow::Owned(reason) = &candidate.rejected_reason {
            bytes = checked_size_add(bytes, reason.capacity(), "trace rejection")?;
        }
    }
    if let Some(index) = trace.query_vector_source.as_ref() {
        bytes = checked_size_add(bytes, index.table.capacity(), "trace vector source")?;
        bytes = checked_size_add(bytes, index.column.capacity(), "trace vector source")?;
    }
    Ok(bytes)
}

fn graph_steps_capacity_bytes(
    steps: &Vec<GraphStepPlan>,
) -> std::result::Result<usize, BoundedExecutionError> {
    let mut bytes = checked_size_mul(
        steps.capacity(),
        std::mem::size_of::<GraphStepPlan>(),
        "graph steps",
    )?;
    for step in steps {
        bytes = checked_size_add(
            bytes,
            string_vec_capacity_bytes(&step.edge_types, "graph edge types")?,
            "graph step",
        )?;
        bytes = checked_size_add(bytes, step.target_alias.capacity(), "graph step")?;
    }
    Ok(bytes)
}

fn project_columns_capacity_bytes(
    columns: &Vec<ProjectColumn>,
) -> std::result::Result<usize, BoundedExecutionError> {
    let mut bytes = checked_size_mul(
        columns.capacity(),
        std::mem::size_of::<ProjectColumn>(),
        "projection columns",
    )?;
    for column in columns {
        bytes = checked_size_add(bytes, expr_capacity_bytes(&column.expr)?, "projection")?;
        bytes = checked_size_add(bytes, optional_string_capacity(&column.alias), "projection")?;
    }
    Ok(bytes)
}

fn sort_keys_capacity_bytes(
    keys: &Vec<SortKey>,
) -> std::result::Result<usize, BoundedExecutionError> {
    let mut bytes = checked_size_mul(keys.capacity(), std::mem::size_of::<SortKey>(), "sort keys")?;
    for key in keys {
        bytes = checked_size_add(bytes, expr_capacity_bytes(&key.expr)?, "sort key")?;
    }
    Ok(bytes)
}

fn boxed_pull_bytes(input: &PullNode) -> std::result::Result<usize, BoundedExecutionError> {
    checked_size_add(
        std::mem::size_of::<PullNode>(),
        pull_continuation_bytes(input)?,
        "boxed pull continuation",
    )
}

/// What a paused read is holding, part by part.
///
/// The settlement compares ONE number against the reader's ceiling, so a term
/// that is missing from it is memory the reader holds and is never asked to
/// pay for -- and from outside, a total that is too small looks exactly like a
/// read that is genuinely small. Naming the parts is what lets a caller say
/// which one is missing instead of only that the sum is wrong.
///
/// `total` is the production figure the cursor is settled to, taken from the
/// same walk the settlement uses, so a named part can never disagree with it:
/// `named` lists the parts a caller can ask about by name and `unnamed` is
/// whatever is left, so the parts always add up to the total.
#[cfg(feature = "test-seams")]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ContinuationBytes {
    pub total: usize,
    pub named: Vec<(&'static str, usize)>,
    pub unnamed: usize,
}

#[cfg(feature = "test-seams")]
impl ContinuationBytes {
    /// What the part by this name holds, or zero when this read holds no such
    /// part. A caller asking about a part a read does not have is asking a
    /// fair question and gets a number, not a panic.
    pub fn part(&self, name: &str) -> usize {
        self.named
            .iter()
            .find(|(part, _)| *part == name)
            .map_or(0, |(_, bytes)| *bytes)
    }
}

/// The name of the term every index run a list predicate still owes is
/// counted under.
#[cfg(feature = "test-seams")]
pub const PENDING_INDEX_RUNS: &str = "pending index runs";

#[cfg(feature = "test-seams")]
fn continuation_bytes_breakdown(
    node: &PullNode,
) -> std::result::Result<ContinuationBytes, BoundedExecutionError> {
    let mut named: Vec<(&'static str, usize)> = Vec::new();
    let mut pending_runs = 0_usize;
    let mut index_picks = 0_usize;
    let mut table_shapes = 0_usize;
    let mut filters = 0_usize;
    collect_continuation_parts(
        node,
        &mut pending_runs,
        &mut index_picks,
        &mut table_shapes,
        &mut filters,
    )?;
    named.push((PENDING_INDEX_RUNS, pending_runs));
    named.push(("index picks", index_picks));
    named.push(("table shapes", table_shapes));
    named.push(("row filters", filters));
    let total = pull_continuation_bytes(node)?;
    let accounted: usize = named.iter().map(|(_, bytes)| *bytes).sum();
    Ok(ContinuationBytes {
        total,
        named,
        unnamed: total.saturating_sub(accounted),
    })
}

/// Walk every source in the tree and add up the parts by name. The scan arm
/// reads the same fields the settlement reads, so a part named here is the
/// part the settlement counted.
#[cfg(feature = "test-seams")]
fn collect_continuation_parts(
    node: &PullNode,
    pending_runs: &mut usize,
    index_picks: &mut usize,
    table_shapes: &mut usize,
    filters: &mut usize,
) -> std::result::Result<(), BoundedExecutionError> {
    if let PullKind::Scan(state) = &node.kind {
        *table_shapes = table_shapes.saturating_add(table_meta_capacity_bytes(&state.meta)?);
        if let Some(filter) = state.filter.as_ref() {
            *filters = filters.saturating_add(expr_capacity_bytes(filter)?);
        }
        match &state.mode {
            ScanMode::Index { pick, cursor, .. } => {
                *index_picks = index_picks.saturating_add(index_pick_capacity_bytes(pick)?);
                *pending_runs = pending_runs.saturating_add(cursor.pending_run_bytes());
            }
            ScanMode::Ordered { cursor, .. } => {
                *pending_runs = pending_runs.saturating_add(cursor.pending_run_bytes());
            }
            ScanMode::Physical { .. } | ScanMode::Exact { .. } | ScanMode::Empty => {}
        }
        if let Some(pick) = state.residual_pick.as_ref() {
            *index_picks = index_picks.saturating_add(index_pick_capacity_bytes(pick)?);
        }
    }
    for input in pull_node_inputs(&node.kind) {
        collect_continuation_parts(input, pending_runs, index_picks, table_shapes, filters)?;
    }
    Ok(())
}

/// Every node this one reads from. One place says what a source's children
/// are, so a walk over the tree cannot miss a branch the settlement counts.
#[cfg(feature = "test-seams")]
fn pull_node_inputs(kind: &PullKind) -> Vec<&PullNode> {
    match kind {
        PullKind::Graph(state) => state.start_candidates.as_deref().into_iter().collect(),
        PullKind::Vector(state) => match &state.preparation {
            VectorPreparation::Candidates(Some(candidates)) => vec![candidates.as_ref()],
            _ => Vec::new(),
        },
        PullKind::Project(state) => vec![state.input.as_ref()],
        PullKind::Sort(state) => vec![state.input.as_ref()],
        PullKind::Limit(state) => vec![state.input.as_ref()],
        PullKind::Filter(state) => vec![state.input.as_ref()],
        PullKind::Distinct(state) => vec![state.input.as_ref()],
        PullKind::Join(state) => vec![state.left.as_ref(), state.right.as_ref()],
        PullKind::Union(state) => state.inputs.iter().collect(),
        PullKind::Scan(_) | PullKind::Static(_) => Vec::new(),
    }
}

fn pull_continuation_bytes(node: &PullNode) -> std::result::Result<usize, BoundedExecutionError> {
    let mut bytes = strings_capacity_bytes(&node.columns, "pull output columns")?;
    bytes = checked_size_add(
        bytes,
        query_trace_capacity_bytes(&node.trace)?,
        "pull continuation",
    )?;
    let state_bytes = match &node.kind {
        PullKind::Scan(state) => {
            let mut state_bytes = checked_size_add(
                state.table.capacity(),
                table_meta_capacity_bytes(&state.meta)?,
                "scan continuation",
            )?;
            state_bytes = checked_size_add(
                state_bytes,
                strings_capacity_bytes(&state.schema_columns, "scan schema columns")?,
                "scan continuation",
            )?;
            if let Some(filter) = state.filter.as_ref() {
                state_bytes =
                    checked_size_add(state_bytes, expr_capacity_bytes(filter)?, "scan filter")?;
            }
            let mode_bytes = match &state.mode {
                ScanMode::Physical { .. } | ScanMode::Empty => 0,
                // The probe keeps the keys it still owes plus the postings of
                // the one key it is reading, both already charged as they were
                // taken.
                ScanMode::Exact { index, cursor } => checked_size_add(
                    index.capacity(),
                    cursor.retained_bytes(),
                    "exact index continuation",
                )?,
                ScanMode::Index {
                    pick,
                    column,
                    cursor,
                    ..
                } => {
                    let mut mode_bytes = checked_size_add(
                        index_pick_capacity_bytes(pick)?,
                        column.capacity(),
                        "index scan continuation",
                    )?;
                    if let Some(index) = cursor.index_name() {
                        mode_bytes =
                            checked_size_add(mode_bytes, index.len(), "index cursor continuation")?;
                    }
                    checked_size_add(
                        mode_bytes,
                        cursor.pending_run_bytes(),
                        "index cursor continuation",
                    )?
                }
                ScanMode::Ordered { column, cursor, .. } => {
                    let mut mode_bytes = column.capacity();
                    if let Some(index) = cursor.index_name() {
                        mode_bytes = checked_size_add(
                            mode_bytes,
                            index.len(),
                            "ordered cursor continuation",
                        )?;
                    }
                    checked_size_add(
                        mode_bytes,
                        cursor.pending_run_bytes(),
                        "ordered cursor continuation",
                    )?
                }
            };
            state_bytes = checked_size_add(state_bytes, mode_bytes, "scan continuation")?;
            if let Some(merge) = state.ordered_merge.as_ref() {
                let mut fold_bytes =
                    checked_size_add(merge.held_bytes, merge.column.capacity(), "ordered fold")?;
                if let Some((_, deferred_bytes, _)) = merge.deferred.as_ref() {
                    fold_bytes = checked_size_add(fold_bytes, *deferred_bytes, "ordered fold")?;
                }
                state_bytes = checked_size_add(state_bytes, fold_bytes, "scan continuation")?;
            }
            if let Some(pick) = state.residual_pick.as_ref() {
                state_bytes = checked_size_add(
                    state_bytes,
                    index_pick_capacity_bytes(pick)?,
                    "scan residual",
                )?;
            }
            state_bytes
        }
        PullKind::Graph(state) => {
            let mut state_bytes = checked_size_add(
                state.start_alias.capacity(),
                graph_steps_capacity_bytes(&state.steps)?,
                "graph continuation",
            )?;
            if let Some(filter) = state.filter.as_ref() {
                state_bytes =
                    checked_size_add(state_bytes, expr_capacity_bytes(filter)?, "graph filter")?;
            }
            state_bytes = checked_size_add(
                state_bytes,
                checked_size_mul(
                    state.starts.capacity(),
                    std::mem::size_of::<uuid::Uuid>(),
                    "graph starts",
                )?,
                "graph continuation",
            )?;
            if let Some(candidates) = state.start_candidates.as_deref() {
                state_bytes = checked_size_add(
                    state_bytes,
                    boxed_pull_bytes(candidates)?,
                    "graph candidates",
                )?;
            }
            // The parts of a traversal that GROW as it reads. Everything above
            // is fixed the moment the walk is planned; these are not, and they
            // are what the walk hands back when it finally runs dry. Leaving
            // them out let a settlement computed from this figure give those
            // bytes away while the traversal was still holding them, and the
            // walk's own hand-back at exhaustion then asked for more than the
            // request had left. Every one is a figure the traversal already
            // keeps, so asking costs the same however deep the walk has got.
            for growing in [
                state.seen_start_container_bytes,
                state.seen_edge_container_bytes,
                state.seen_path_container_bytes,
                state.probe_seen_container_bytes,
                state.frontier_container_bytes,
                state.row_memory.available,
                state
                    .edge_source
                    .as_ref()
                    .map_or(0, |source| source.retained_bytes()),
            ] {
                state_bytes = checked_size_add(state_bytes, growing, "graph continuation")?;
            }
            if let Some(active) = state.active.as_ref() {
                state_bytes = checked_size_add(
                    state_bytes,
                    active.cursor.retained_bytes(),
                    "graph traversal continuation",
                )?;
                state_bytes = checked_size_add(
                    state_bytes,
                    active.path.retained_bytes,
                    "graph traversal path",
                )?;
            }
            state_bytes
        }
        PullKind::Vector(state) => {
            let mut state_bytes = checked_size_add(
                state.table.capacity(),
                checked_size_add(
                    state.index.table.capacity(),
                    state.index.column.capacity(),
                    "vector index",
                )?,
                "vector continuation",
            )?;
            state_bytes = checked_size_add(
                state_bytes,
                optional_string_capacity(&state.sort_key),
                "vector continuation",
            )?;
            if let VectorPreparation::Candidates(Some(candidates)) = &state.preparation {
                state_bytes = checked_size_add(
                    state_bytes,
                    boxed_pull_bytes(candidates)?,
                    "vector candidates",
                )?;
            }
            // Held for as long as the search is deciding, so a settlement
            // computed from this figure cannot give it away underneath the
            // search that is still reading it.
            state_bytes =
                checked_size_add(state_bytes, state.overlay_bytes, "vector continuation")?;
            state_bytes
        }
        PullKind::Project(state) => {
            let mut state_bytes = checked_size_add(
                boxed_pull_bytes(&state.input)?,
                project_columns_capacity_bytes(&state.columns)?,
                "projection continuation",
            )?;
            // An aggregate a cursor opened keeps its running answer -- and
            // the input row it has pulled but not folded in -- between
            // fetches. A settlement that cannot see them gives them away, and
            // the fetch that finally releases them under-releases by exactly
            // that much.
            state_bytes = checked_size_add(
                state_bytes,
                state.aggregate_state_bytes,
                "projection continuation",
            )?;
            if let Some(pending) = state.pending.as_ref() {
                state_bytes = checked_size_add(
                    state_bytes,
                    pending.retained_bytes,
                    "projection continuation",
                )?;
            }
            state_bytes
        }
        PullKind::Sort(state) => {
            let mut state_bytes = checked_size_add(
                boxed_pull_bytes(&state.input)?,
                sort_keys_capacity_bytes(&state.keys)?,
                "sort continuation",
            )?;
            // A sort answers nothing until it has read everything, so between
            // its first row and its last it is holding the whole answer: the
            // key bytes it reserved for each waiting row, and the row's own
            // bytes charged by the input. Read as the running total the sort
            // keeps, never walked -- this question is asked twice a page, and
            // a walk would make answering it cost what is still buffered, so
            // the bookkeeping would grow with the answer still to be
            // delivered. Counted for the same reason a traversal's frontier
            // is: what a settlement cannot see, it gives away.
            state_bytes =
                checked_size_add(state_bytes, state.buffered_bytes, "sort buffered rows")?;
            if let Some(pending) = state.pending.as_ref() {
                state_bytes =
                    checked_size_add(state_bytes, pending.retained_bytes, "sort pending row")?;
            }
            state_bytes
        }
        PullKind::Limit(state) => boxed_pull_bytes(&state.input)?,
        PullKind::Filter(state) => checked_size_add(
            boxed_pull_bytes(&state.input)?,
            expr_capacity_bytes(&state.predicate)?,
            "filter continuation",
        )?,
        PullKind::Distinct(state) => boxed_pull_bytes(&state.input)?,
        PullKind::Join(state) => {
            let mut state_bytes = checked_size_add(
                boxed_pull_bytes(&state.left)?,
                boxed_pull_bytes(&state.right)?,
                "join inputs",
            )?;
            state_bytes = checked_size_add(
                state_bytes,
                expr_capacity_bytes(&state.condition)?,
                "join condition",
            )?;
            checked_size_add(
                state_bytes,
                strings_capacity_bytes(&state.condition_columns, "join columns")?,
                "join continuation",
            )?
        }
        PullKind::Static(state) => {
            let mut state_bytes = checked_size_mul(
                state.rows.capacity(),
                std::mem::size_of::<PulledRow>(),
                "static rows",
            )?;
            for row in &state.rows {
                state_bytes = checked_size_add(
                    state_bytes,
                    values_retained_bytes(&row.values)?,
                    "static row",
                )?;
            }
            state_bytes
        }
        PullKind::Union(state) => {
            let mut state_bytes = checked_size_mul(
                state.inputs.capacity(),
                std::mem::size_of::<PullNode>(),
                "UNION inputs",
            )?;
            for input in &state.inputs {
                state_bytes = checked_size_add(
                    state_bytes,
                    pull_continuation_bytes(input)?,
                    "UNION continuation",
                )?;
            }
            state_bytes
        }
    };
    bytes = checked_size_add(bytes, state_bytes, "pull continuation")?;
    Ok(bytes)
}

fn expr_nodes(expr: &Expr) -> std::result::Result<usize, BoundedExecutionError> {
    let children = match expr {
        Expr::BinaryOp { left, right, .. } | Expr::CosineDistance { left, right } => {
            checked_size_add(expr_nodes(left)?, expr_nodes(right)?, "expression nodes")?
        }
        Expr::UnaryOp { operand, .. } | Expr::IsNull { expr: operand, .. } => expr_nodes(operand)?,
        Expr::FunctionCall { args, .. } => args.iter().try_fold(0usize, |total, arg| {
            checked_size_add(total, expr_nodes(arg)?, "expression nodes")
        })?,
        Expr::InList { expr, list, .. } => {
            let list_nodes = list.iter().try_fold(0usize, |total, item| {
                checked_size_add(total, expr_nodes(item)?, "expression nodes")
            })?;
            checked_size_add(expr_nodes(expr)?, list_nodes, "expression nodes")?
        }
        Expr::Like { expr, pattern, .. } => {
            checked_size_add(expr_nodes(expr)?, expr_nodes(pattern)?, "expression nodes")?
        }
        Expr::InSubquery { .. } => 0,
        Expr::RowVectorSource { key, .. } => expr_nodes(key)?,
        Expr::Column(_) | Expr::Literal(_) | Expr::Parameter(_) => 0,
    };
    checked_size_add(children, 1, "expression nodes")
}

/// The parameter names an expression reads.
fn collect_expr_parameters(expr: &Expr, into: &mut std::collections::BTreeSet<String>) {
    match expr {
        Expr::Parameter(name) => {
            into.insert(name.clone());
        }
        Expr::BinaryOp { left, right, .. } | Expr::CosineDistance { left, right } => {
            collect_expr_parameters(left, into);
            collect_expr_parameters(right, into);
        }
        Expr::FunctionCall { args, .. } => {
            for arg in args {
                collect_expr_parameters(arg, into);
            }
        }
        Expr::UnaryOp { operand, .. } => collect_expr_parameters(operand, into),
        Expr::IsNull { expr, .. } => collect_expr_parameters(expr, into),
        Expr::Like { expr, .. } => collect_expr_parameters(expr, into),
        Expr::InList { expr, list, .. } => {
            collect_expr_parameters(expr, into);
            for item in list {
                collect_expr_parameters(item, into);
            }
        }
        Expr::InSubquery { expr, .. } => collect_expr_parameters(expr, into),
        _ => {}
    }
}

/// What the named parameters cost, container included.
fn referenced_params_retained_bytes(
    params: &HashMap<String, Value>,
    referenced: &std::collections::BTreeSet<String>,
) -> std::result::Result<usize, BoundedExecutionError> {
    let mut bytes = std::mem::size_of::<HashMap<String, Value>>();
    for name in referenced {
        let Some(value) = params.get(name) else {
            continue;
        };
        bytes = checked_size_add(
            bytes,
            checked_size_add(
                std::mem::size_of::<(String, Value)>(),
                checked_size_add(name.capacity(), value_capacity_bytes(value)?, "parameter")?,
                "parameter",
            )?,
            "parameters",
        )?;
    }
    Ok(bytes)
}

fn expression_temporary_bytes<'a>(
    row: &Vec<Value>,
    params: &HashMap<String, Value>,
    expressions: impl IntoIterator<Item = &'a Expr>,
) -> std::result::Result<usize, BoundedExecutionError> {
    let mut referenced = std::collections::BTreeSet::new();
    let mut nodes = 0usize;
    for expression in expressions {
        collect_expr_parameters(expression, &mut referenced);
        nodes = checked_size_add(nodes, expr_nodes(expression)?, "expression evaluation")?;
    }
    let referenced = &referenced;
    let row_bytes = values_retained_bytes(row)?;
    // Only the parameters these expressions actually READ. A statement's
    // parameter map is charged where it is held; charging the whole of it
    // again for every row an expression is evaluated over makes a projection
    // of one small column cost whatever the largest parameter is -- a
    // 128-dimension query vector, say, doubled, per row, for a projection
    // that never mentions it.
    let parameter_bytes = referenced_params_retained_bytes(params, referenced)?;
    let working_values = checked_size_mul(
        checked_size_add(row_bytes, parameter_bytes, "expression inputs")?,
        2,
        "expression inputs",
    )?;
    checked_size_add(
        working_values,
        checked_size_mul(
            nodes,
            std::mem::size_of::<Value>(),
            "expression temporaries",
        )?,
        "expression evaluation",
    )
}

struct CheckedCountingWriter<'a> {
    bytes: &'a Cell<usize>,
}

impl Writer for CheckedCountingWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::result::Result<(), bincode::error::EncodeError> {
        let next =
            self.bytes
                .get()
                .checked_add(bytes.len())
                .ok_or(bincode::error::EncodeError::Other(
                    "bounded canonical size overflow",
                ))?;
        self.bytes.set(next);
        Ok(())
    }
}

fn serialized_size<T: Serialize + ?Sized>(
    value: &T,
) -> std::result::Result<usize, BoundedExecutionError> {
    let bytes = Cell::new(0usize);
    bincode::serde::encode_into_writer(
        value,
        CheckedCountingWriter { bytes: &bytes },
        bincode::config::standard(),
    )
    .map_err(|_| Error::Other("canonical read encoding failed".to_string()))?;
    Ok(bytes.get())
}

fn encoded_row_key(
    row: &[Value],
    size: usize,
) -> std::result::Result<Vec<u8>, BoundedExecutionError> {
    let mut key = vec![0u8; size];
    let written =
        bincode::serde::encode_into_slice(row, key.as_mut_slice(), bincode::config::standard())
            .map_err(|_| Error::Other("bounded row-key encoding failed".to_string()))?;
    if written != size {
        return Err(
            Error::Other("bounded row-key size changed during encoding".to_string()).into(),
        );
    }
    Ok(key)
}

#[derive(Debug, Clone, Copy)]
enum BorrowedScalar<'a> {
    Null,
    Bool(bool),
    Int64(i64),
    Float64(f64),
    Text(&'a str),
    Uuid(&'a uuid::Uuid),
    Timestamp(i64),
    Json(&'a serde_json::Value),
    Vector(&'a [f32]),
    TxId(TxId),
}

impl<'a> BorrowedScalar<'a> {
    fn from_value(value: &'a Value) -> Self {
        match value {
            Value::Null => Self::Null,
            Value::Bool(value) => Self::Bool(*value),
            Value::Int64(value) => Self::Int64(*value),
            Value::Float64(value) => Self::Float64(*value),
            Value::Text(value) => Self::Text(value),
            Value::Uuid(value) => Self::Uuid(value),
            Value::Timestamp(value) => Self::Timestamp(*value),
            Value::Json(value) => Self::Json(value),
            Value::Vector(value) => Self::Vector(value),
            Value::TxId(value) => Self::TxId(*value),
        }
    }

    fn from_literal(literal: &'a Literal) -> Self {
        match literal {
            Literal::Null => Self::Null,
            Literal::Bool(value) => Self::Bool(*value),
            Literal::Integer(value) => Self::Int64(*value),
            Literal::Real(value) => Self::Float64(*value),
            Literal::Text(value) => Self::Text(value),
            Literal::Vector(value) => Self::Vector(value),
        }
    }

    fn is_null(self) -> bool {
        matches!(self, Self::Null)
    }

    fn into_owned(self) -> Value {
        match self {
            Self::Null => Value::Null,
            Self::Bool(value) => Value::Bool(value),
            Self::Int64(value) => Value::Int64(value),
            Self::Float64(value) => Value::Float64(value),
            Self::Text(value) => Value::Text(value.to_owned()),
            Self::Uuid(value) => Value::Uuid(*value),
            Self::Timestamp(value) => Value::Timestamp(value),
            Self::Json(value) => Value::Json(value.clone()),
            Self::Vector(value) => Value::Vector(value.to_vec()),
            Self::TxId(value) => Value::TxId(value),
        }
    }
}

fn borrowed_scalar_truth(value: BorrowedScalar<'_>) -> bool {
    matches!(value, BorrowedScalar::Bool(true))
}

fn borrowed_scalar_exact_eq(left: BorrowedScalar<'_>, right: BorrowedScalar<'_>) -> bool {
    match (left, right) {
        (BorrowedScalar::Null, BorrowedScalar::Null) => true,
        (BorrowedScalar::Bool(left), BorrowedScalar::Bool(right)) => left == right,
        (BorrowedScalar::Int64(left), BorrowedScalar::Int64(right)) => left == right,
        (BorrowedScalar::Float64(left), BorrowedScalar::Float64(right)) => left == right,
        (BorrowedScalar::Text(left), BorrowedScalar::Text(right)) => left == right,
        (BorrowedScalar::Uuid(left), BorrowedScalar::Uuid(right)) => left == right,
        (BorrowedScalar::Timestamp(left), BorrowedScalar::Timestamp(right)) => left == right,
        (BorrowedScalar::Json(left), BorrowedScalar::Json(right)) => left == right,
        (BorrowedScalar::Vector(left), BorrowedScalar::Vector(right)) => left == right,
        (BorrowedScalar::TxId(left), BorrowedScalar::TxId(right)) => left == right,
        _ => false,
    }
}

fn borrowed_scalar_cmp(
    left: BorrowedScalar<'_>,
    right: BorrowedScalar<'_>,
) -> Option<std::cmp::Ordering> {
    use std::cmp::Ordering;

    match (left, right) {
        (BorrowedScalar::Int64(left), BorrowedScalar::Int64(right)) => Some(left.cmp(&right)),
        (BorrowedScalar::Float64(left), BorrowedScalar::Float64(right)) => {
            Some(left.total_cmp(&right))
        }
        (BorrowedScalar::Text(left), BorrowedScalar::Text(right)) => Some(left.cmp(right)),
        (BorrowedScalar::Timestamp(left), BorrowedScalar::Timestamp(right)) => {
            Some(left.cmp(&right))
        }
        (BorrowedScalar::Int64(left), BorrowedScalar::Float64(right)) => {
            Some((left as f64).total_cmp(&right))
        }
        (BorrowedScalar::Float64(left), BorrowedScalar::Int64(right)) => {
            Some(left.total_cmp(&(right as f64)))
        }
        (BorrowedScalar::Timestamp(left), BorrowedScalar::Int64(right)) => Some(left.cmp(&right)),
        (BorrowedScalar::Int64(left), BorrowedScalar::Timestamp(right)) => Some(left.cmp(&right)),
        (BorrowedScalar::Bool(left), BorrowedScalar::Bool(right)) => Some(left.cmp(&right)),
        (BorrowedScalar::Uuid(left), BorrowedScalar::Uuid(right)) => Some(left.cmp(right)),
        (BorrowedScalar::Uuid(left), BorrowedScalar::Text(right)) => right
            .parse::<uuid::Uuid>()
            .ok()
            .map(|right| left.cmp(&right)),
        (BorrowedScalar::Text(left), BorrowedScalar::Uuid(right)) => {
            left.parse::<uuid::Uuid>().ok().map(|left| left.cmp(right))
        }
        (BorrowedScalar::TxId(left), BorrowedScalar::TxId(right)) => Some(left.0.cmp(&right.0)),
        (BorrowedScalar::TxId(left), BorrowedScalar::Int64(right)) => {
            if right < 0 {
                Some(Ordering::Greater)
            } else {
                Some(left.0.cmp(&(right as u64)))
            }
        }
        (BorrowedScalar::Int64(left), BorrowedScalar::TxId(right)) => {
            if left < 0 {
                Some(Ordering::Less)
            } else {
                Some((left as u64).cmp(&right.0))
            }
        }
        (BorrowedScalar::TxId(_), BorrowedScalar::Timestamp(_))
        | (BorrowedScalar::Timestamp(_), BorrowedScalar::TxId(_))
        | (BorrowedScalar::Null, _)
        | (_, BorrowedScalar::Null) => None,
        _ => None,
    }
}

fn borrowed_scalar_sql_eq(left: BorrowedScalar<'_>, right: BorrowedScalar<'_>) -> bool {
    borrowed_scalar_cmp(left, right) == Some(std::cmp::Ordering::Equal)
        || borrowed_scalar_exact_eq(left, right)
}

fn bounded_eval_arithmetic<'a>(
    name: &str,
    left: BorrowedScalar<'a>,
    right: BorrowedScalar<'a>,
) -> Result<BorrowedScalar<'a>> {
    if left.is_null() || right.is_null() {
        return Ok(BorrowedScalar::Null);
    }
    match (left, right) {
        (BorrowedScalar::Int64(left), BorrowedScalar::Int64(right)) => match name {
            "__add" => left
                .checked_add(right)
                .map(BorrowedScalar::Int64)
                .ok_or_else(|| Error::PlanError("integer out of range".to_string())),
            "__sub" => left
                .checked_sub(right)
                .map(BorrowedScalar::Int64)
                .ok_or_else(|| Error::PlanError("integer out of range".to_string())),
            "__mul" => left
                .checked_mul(right)
                .map(BorrowedScalar::Int64)
                .ok_or_else(|| Error::PlanError("integer out of range".to_string())),
            "__div" => {
                if right == 0 {
                    Err(Error::PlanError("division by zero".to_string()))
                } else {
                    left.checked_div(right)
                        .map(BorrowedScalar::Int64)
                        .ok_or_else(|| {
                            Error::PlanError(format!("integer overflow: {left} / {right}"))
                        })
                }
            }
            _ => Err(Error::PlanError(format!("unknown function: {name}"))),
        },
        (BorrowedScalar::Float64(left), BorrowedScalar::Float64(right)) => {
            bounded_eval_float_arithmetic(name, left, right)
        }
        (BorrowedScalar::Int64(left), BorrowedScalar::Float64(right)) => {
            bounded_eval_float_arithmetic(name, left as f64, right)
        }
        (BorrowedScalar::Float64(left), BorrowedScalar::Int64(right)) => {
            bounded_eval_float_arithmetic(name, left, right as f64)
        }
        _ => Err(Error::PlanError(format!(
            "function {name} expects numeric arguments"
        ))),
    }
}

fn bounded_eval_float_arithmetic<'a>(
    name: &str,
    left: f64,
    right: f64,
) -> Result<BorrowedScalar<'a>> {
    match name {
        "__add" => Ok(BorrowedScalar::Float64(left + right)),
        "__sub" => Ok(BorrowedScalar::Float64(left - right)),
        "__mul" => Ok(BorrowedScalar::Float64(left * right)),
        "__div" if right == 0.0 => Err(Error::PlanError("division by zero".to_string())),
        "__div" => Ok(BorrowedScalar::Float64(left / right)),
        _ => Err(Error::PlanError(format!("unknown function: {name}"))),
    }
}

fn bounded_eval_function<'a>(
    name: &str,
    args: &'a [Expr],
    mut evaluate: impl FnMut(&'a Expr) -> Result<BorrowedScalar<'a>>,
) -> Result<BorrowedScalar<'a>> {
    let mut first = None;
    let mut second = None;
    let mut coalesced = None;
    for (position, argument) in args.iter().enumerate() {
        let value = evaluate(argument)?;
        if position == 0 {
            first = Some(value);
        } else if position == 1 {
            second = Some(value);
        }
        if coalesced.is_none() && !value.is_null() {
            coalesced = Some(value);
        }
    }

    if ["__add", "__sub", "__mul", "__div"]
        .iter()
        .any(|candidate| name.eq_ignore_ascii_case(candidate))
    {
        if args.len() != 2 {
            return Err(Error::PlanError(format!(
                "function {name} expects 2 arguments"
            )));
        }
        let (Some(left), Some(right)) = (first, second) else {
            return Err(Error::PlanError(format!(
                "function {name} expects 2 arguments"
            )));
        };
        return bounded_eval_arithmetic(name, left, right);
    }
    if name.eq_ignore_ascii_case("coalesce") {
        return Ok(coalesced.unwrap_or(BorrowedScalar::Null));
    }
    if name.eq_ignore_ascii_case("now") {
        // The name is already identified here. Dispatching through the shared
        // evaluator would first normalize it into a fresh String that no
        // request budget admitted, so the clock is read directly.
        return Ok(BorrowedScalar::Timestamp(now_timestamp_seconds()?));
    }
    Err(Error::PlanError(format!("unknown function: {name}")))
}

fn bounded_resolve_scalar<'a>(
    expr: &'a Expr,
    params: &'a HashMap<String, Value>,
) -> Result<BorrowedScalar<'a>> {
    match expr {
        Expr::Literal(literal) => Ok(BorrowedScalar::from_literal(literal)),
        Expr::Parameter(name) => params
            .get(name)
            .map(BorrowedScalar::from_value)
            .ok_or_else(|| Error::NotFound(format!("missing parameter: {name}"))),
        Expr::Column(column) => Ok(BorrowedScalar::Text(&column.column)),
        Expr::UnaryOp { op, operand } => match op {
            UnaryOp::Neg => match bounded_resolve_scalar(operand, params)? {
                BorrowedScalar::Int64(value) => value
                    .checked_neg()
                    .map(BorrowedScalar::Int64)
                    .ok_or_else(|| Error::PlanError("integer out of range".to_string())),
                BorrowedScalar::Float64(value) => Ok(BorrowedScalar::Float64(-value)),
                _ => Err(Error::PlanError(
                    "cannot negate non-numeric value".to_string(),
                )),
            },
            UnaryOp::Not => Err(Error::PlanError(
                "boolean NOT requires row context".to_string(),
            )),
        },
        Expr::FunctionCall { name, args } => bounded_eval_function(name, args, |argument| {
            bounded_resolve_scalar(argument, params)
        }),
        Expr::CosineDistance { right, .. } => bounded_resolve_scalar(right, params),
        _ => Err(Error::PlanError("unsupported expression".to_string())),
    }
}

fn bounded_eval_scalar_in_row<'a>(
    row: &'a VersionedRow,
    expr: &'a Expr,
    params: &'a HashMap<String, Value>,
) -> Result<BorrowedScalar<'a>> {
    match expr {
        Expr::Column(column) => {
            if column.column == "row_id" {
                Ok(BorrowedScalar::Int64(row.row_id.0 as i64))
            } else {
                Ok(row
                    .values
                    .get(&column.column)
                    .map(BorrowedScalar::from_value)
                    .unwrap_or(BorrowedScalar::Null))
            }
        }
        Expr::BinaryOp { left, op, right } => {
            let left = bounded_eval_scalar_in_row(row, left, params)?;
            let right = bounded_eval_scalar_in_row(row, right, params)?;
            let value = match op {
                BinOp::Eq => {
                    !left.is_null() && !right.is_null() && borrowed_scalar_sql_eq(left, right)
                }
                BinOp::Neq => {
                    !left.is_null() && !right.is_null() && !borrowed_scalar_sql_eq(left, right)
                }
                BinOp::Lt => borrowed_scalar_cmp(left, right) == Some(std::cmp::Ordering::Less),
                BinOp::Lte => matches!(
                    borrowed_scalar_cmp(left, right),
                    Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
                ),
                BinOp::Gt => borrowed_scalar_cmp(left, right) == Some(std::cmp::Ordering::Greater),
                BinOp::Gte => matches!(
                    borrowed_scalar_cmp(left, right),
                    Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
                ),
                BinOp::And => borrowed_scalar_truth(left) && borrowed_scalar_truth(right),
                BinOp::Or => borrowed_scalar_truth(left) || borrowed_scalar_truth(right),
            };
            Ok(BorrowedScalar::Bool(value))
        }
        Expr::UnaryOp { op, operand } => {
            let value = bounded_eval_scalar_in_row(row, operand, params)?;
            match op {
                UnaryOp::Not => Ok(BorrowedScalar::Bool(!borrowed_scalar_truth(value))),
                UnaryOp::Neg => match value {
                    BorrowedScalar::Int64(value) => value
                        .checked_neg()
                        .map(BorrowedScalar::Int64)
                        .ok_or_else(|| Error::PlanError("integer out of range".to_string())),
                    BorrowedScalar::Float64(value) => Ok(BorrowedScalar::Float64(-value)),
                    _ => Err(Error::PlanError(
                        "cannot negate non-numeric value".to_string(),
                    )),
                },
            }
        }
        Expr::FunctionCall { name, args } => bounded_eval_function(name, args, |argument| {
            bounded_eval_scalar_in_row(row, argument, params)
        }),
        Expr::IsNull { expr, negated } => {
            let is_null = bounded_eval_scalar_in_row(row, expr, params)?.is_null();
            Ok(BorrowedScalar::Bool(if *negated {
                !is_null
            } else {
                is_null
            }))
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let needle = bounded_eval_scalar_in_row(row, expr, params)?;
            let mut matched = false;
            for item in list {
                if matched {
                    break;
                }
                let candidate = bounded_eval_scalar_in_row(row, item, params)?;
                matched = borrowed_scalar_cmp(needle, candidate) == Some(std::cmp::Ordering::Equal)
                    || (!needle.is_null()
                        && !candidate.is_null()
                        && borrowed_scalar_exact_eq(needle, candidate));
            }
            Ok(BorrowedScalar::Bool(if *negated {
                !matched
            } else {
                matched
            }))
        }
        Expr::Like {
            expr,
            pattern,
            negated,
        } => {
            let value = bounded_eval_scalar_in_row(row, expr, params)?;
            let pattern = bounded_eval_scalar_in_row(row, pattern, params)?;
            let matched = match (value, pattern) {
                (BorrowedScalar::Text(value), BorrowedScalar::Text(pattern)) => {
                    bounded_like_matches(value, pattern)
                }
                _ => false,
            };
            Ok(BorrowedScalar::Bool(if *negated {
                !matched
            } else {
                matched
            }))
        }
        _ => bounded_resolve_scalar(expr, params),
    }
}

fn next_utf8_char(value: &str, position: usize) -> Option<(char, usize)> {
    let suffix = value.get(position..)?;
    let character = suffix.chars().next()?;
    Some((character, position + character.len_utf8()))
}

fn bounded_like_matches(value: &str, pattern: &str) -> bool {
    let (mut value_position, mut pattern_position) = (0usize, 0usize);
    let mut star_pattern_position = None;
    let mut star_value_position = 0usize;

    while value_position < value.len() {
        let Some((value_character, next_value_position)) = next_utf8_char(value, value_position)
        else {
            return false;
        };
        if let Some((pattern_character, next_pattern_position)) =
            next_utf8_char(pattern, pattern_position)
        {
            if pattern_character == '_' || pattern_character == value_character {
                value_position = next_value_position;
                pattern_position = next_pattern_position;
                continue;
            }
            if pattern_character == '%' {
                star_pattern_position = Some(next_pattern_position);
                star_value_position = value_position;
                pattern_position = next_pattern_position;
                continue;
            }
        }
        let Some(after_star) = star_pattern_position else {
            return false;
        };
        let Some((_, next_star_value_position)) = next_utf8_char(value, star_value_position) else {
            return false;
        };
        star_value_position = next_star_value_position;
        value_position = next_star_value_position;
        pattern_position = after_star;
    }

    while let Some((pattern_character, next_pattern_position)) =
        next_utf8_char(pattern, pattern_position)
    {
        if pattern_character != '%' {
            return false;
        }
        pattern_position = next_pattern_position;
    }
    pattern_position == pattern.len()
}

fn bounded_eval_bool_expr(
    row: &VersionedRow,
    expr: &Expr,
    params: &HashMap<String, Value>,
) -> Result<Option<bool>> {
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            BinOp::Eq | BinOp::Neq | BinOp::Lt | BinOp::Lte | BinOp::Gt | BinOp::Gte => {
                let left = bounded_eval_scalar_in_row(row, left, params)?;
                let right = bounded_eval_scalar_in_row(row, right, params)?;
                if left.is_null() || right.is_null() {
                    return Ok(None);
                }
                let result = match op {
                    BinOp::Eq => borrowed_scalar_sql_eq(left, right),
                    BinOp::Neq => !borrowed_scalar_sql_eq(left, right),
                    BinOp::Lt => borrowed_scalar_cmp(left, right) == Some(std::cmp::Ordering::Less),
                    BinOp::Lte => matches!(
                        borrowed_scalar_cmp(left, right),
                        Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
                    ),
                    BinOp::Gt => {
                        borrowed_scalar_cmp(left, right) == Some(std::cmp::Ordering::Greater)
                    }
                    BinOp::Gte => matches!(
                        borrowed_scalar_cmp(left, right),
                        Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
                    ),
                    BinOp::And | BinOp::Or => {
                        return Err(Error::Other(
                            "bounded boolean evaluator reached a non-comparison operator"
                                .to_string(),
                        ));
                    }
                };
                Ok(Some(result))
            }
            BinOp::And => {
                let left = bounded_eval_bool_expr(row, left, params)?;
                if left == Some(false) {
                    return Ok(Some(false));
                }
                let right = bounded_eval_bool_expr(row, right, params)?;
                Ok(match (left, right) {
                    (Some(true), Some(true)) => Some(true),
                    (Some(true), other) => other,
                    (None, Some(false)) => Some(false),
                    (None, Some(true)) | (None, None) => None,
                    (Some(false), _) => Some(false),
                })
            }
            BinOp::Or => {
                let left = bounded_eval_bool_expr(row, left, params)?;
                if left == Some(true) {
                    return Ok(Some(true));
                }
                let right = bounded_eval_bool_expr(row, right, params)?;
                Ok(match (left, right) {
                    (Some(false), Some(false)) => Some(false),
                    (Some(false), other) => other,
                    (None, Some(true)) => Some(true),
                    (None, Some(false)) | (None, None) => None,
                    (Some(true), _) => Some(true),
                })
            }
        },
        Expr::UnaryOp {
            op: UnaryOp::Not,
            operand,
        } => Ok(bounded_eval_bool_expr(row, operand, params)?.map(|value| !value)),
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let needle = bounded_eval_scalar_in_row(row, expr, params)?;
            if needle.is_null() {
                return Ok(None);
            }
            let mut matched = false;
            for item in list {
                if matched {
                    break;
                }
                let candidate = bounded_eval_scalar_in_row(row, item, params)?;
                matched = borrowed_scalar_cmp(needle, candidate) == Some(std::cmp::Ordering::Equal)
                    || (!candidate.is_null() && borrowed_scalar_exact_eq(needle, candidate));
            }
            Ok(Some(if *negated { !matched } else { matched }))
        }
        Expr::InSubquery { .. } => Err(Error::PlanError(
            "IN (subquery) must be resolved before execution".to_string(),
        )),
        Expr::Like {
            expr,
            pattern,
            negated,
        } => {
            let left = bounded_eval_scalar_in_row(row, expr, params)?;
            let right = bounded_eval_scalar_in_row(row, pattern, params)?;
            let matched = match (left, right) {
                (BorrowedScalar::Text(value), BorrowedScalar::Text(pattern)) => {
                    bounded_like_matches(value, pattern)
                }
                _ => false,
            };
            Ok(Some(if *negated { !matched } else { matched }))
        }
        Expr::IsNull { expr, negated } => {
            let is_null = bounded_eval_scalar_in_row(row, expr, params)?.is_null();
            Ok(Some(if *negated { !is_null } else { is_null }))
        }
        Expr::Literal(Literal::Bool(value)) => Ok(Some(*value)),
        Expr::Column(_)
        | Expr::Parameter(_)
        | Expr::Literal(Literal::Null)
        | Expr::FunctionCall { .. } => match bounded_eval_scalar_in_row(row, expr, params)? {
            BorrowedScalar::Bool(value) => Ok(Some(value)),
            BorrowedScalar::Null => Ok(None),
            other => Err(Error::PlanError(format!(
                "WHERE expression must be boolean, got {other:?}: {expr:?}"
            ))),
        },
        _ => Err(Error::PlanError(format!(
            "unsupported WHERE expression: {expr:?}"
        ))),
    }
}

fn bounded_row_matches(
    row: &VersionedRow,
    expr: &Expr,
    params: &HashMap<String, Value>,
) -> Result<bool> {
    Ok(bounded_eval_bool_expr(row, expr, params)?.unwrap_or(false))
}

impl ScanState {
    /// The next row this handle's own open transaction staged, once the
    /// committed rows are done.
    ///
    /// A staged row is charged exactly as a committed one is: the touch
    /// before it is read, the bytes before they are copied. A row the
    /// transaction staged and then deleted is dropped -- unless it had a
    /// committed original, which is what a delete followed by a reinsert
    /// leaves behind.
    fn next_staged_row(
        &mut self,
        db: &dyn ReadExecutionTarget,
        context: &Arc<RequestContext>,
    ) -> std::result::Result<Option<(VersionedRow, usize)>, BoundedExecutionError> {
        let Some(state) = self.transaction.as_mut() else {
            return Ok(None);
        };
        state.committed_exhausted = true;
        loop {
            let Some(position) = state.overlay.staged_positions.get(state.published).copied()
            else {
                let held = std::mem::take(&mut state.overlay.bytes);
                context.release(held)?;
                return Ok(None);
            };
            state.published = state.published.saturating_add(1);
            let reserved = Cell::new(0_usize);
            let staged = db.transaction_staged_row(
                state.tx,
                position,
                &mut || context.charge(BoundedWorkSource::TableScan, BoundedSourceTouch::TableRow),
                &mut |bytes| {
                    context.reserve(BoundedWorkSource::TableScan, bytes)?;
                    reserved.set(bytes);
                    Ok(())
                },
            )?;
            let Some(row) = staged else {
                continue;
            };
            // Same trade as a committed row: charged before the clone against
            // the retained estimate, trued up to the copy this read holds.
            let held = pulled_row_retained_bytes(&row)?;
            reconcile_new_reservation(context, BoundedWorkSource::TableScan, reserved.get(), held)?;
            reserved.set(held);
            if state.overlay.deleted.contains(&row.row_id)
                && !state.restaged_over_committed.contains(&row.row_id)
            {
                // The committed source does not always offer every row of the
                // table: an index run offers only the rows its predicate
                // names, so a delete-then-reinsert whose COMMITTED version the
                // predicate excludes never passes through the walk, and
                // inferring "no committed original" from that would drop a row
                // the transaction can plainly see it inserted. Ask the store.
                let mut had_committed = false;
                if !self.committed_source_offers_every_row {
                    let probe = Cell::new(0_usize);
                    let existing = db.bounded_row_by_identity(
                        None,
                        &self.table,
                        row.row_id,
                        self.snapshot,
                        &mut || {
                            context
                                .charge(BoundedWorkSource::TableScan, BoundedSourceTouch::TableRow)
                        },
                        &mut |bytes| {
                            context.reserve(BoundedWorkSource::TableScan, bytes)?;
                            probe.set(bytes);
                            Ok(())
                        },
                    )?;
                    // The probe asks only whether the original is there; the
                    // copy it made is handed straight back.
                    context.release(probe.get())?;
                    had_committed = existing.is_some();
                }
                if !had_committed {
                    context.release(reserved.get())?;
                    continue;
                }
            }
            return Ok(Some((row, reserved.get())));
        }
    }

    /// Read the transaction's staged rows once and put them in this read's
    /// order, so the walk below can fold them into the committed run. Each is
    /// charged exactly as it is on the unordered route -- this only changes
    /// WHEN they are read, not what they cost.
    fn materialize_ordered_staged(
        &mut self,
        db: &dyn ReadExecutionTarget,
        context: &Arc<RequestContext>,
    ) -> std::result::Result<(), BoundedExecutionError> {
        match self.ordered_merge.as_ref() {
            Some(merge) if merge.staged.is_none() => {}
            _ => return Ok(()),
        }
        let mut rows = Vec::new();
        while let Some(staged) = self.next_staged_row(db, context)? {
            rows.push(staged);
        }
        let Some(merge) = self.ordered_merge.as_mut() else {
            return Ok(());
        };
        let column = merge.column.clone();
        let direction = merge.direction;
        rows.sort_by(|left, right| {
            ordered_merge_key(&left.0, &column, direction)
                .cmp(&ordered_merge_key(&right.0, &column, direction))
        });
        merge.held_bytes = rows.iter().map(|(_, bytes)| *bytes).sum();
        merge.staged = Some(rows.into());
        Ok(())
    }

    /// Hand back a staged row ahead of the committed row in hand, when its
    /// ordering value comes first. The committed row is held, not dropped:
    /// it is the next row out.
    fn fold_in_earlier_staged_row(&mut self) {
        let Some(merge) = self.ordered_merge.as_mut() else {
            return;
        };
        if merge.deferred.is_some() {
            return;
        }
        let Some(committed) = self.pending_row.as_ref() else {
            return;
        };
        let comes_first = match merge.staged.as_ref().and_then(VecDeque::front) {
            Some((head, _)) => {
                ordered_merge_key(head, &merge.column, merge.direction)
                    < ordered_merge_key(&committed.0, &merge.column, merge.direction)
            }
            None => false,
        };
        if !comes_first {
            return;
        }
        let Some((row, bytes)) = merge.staged.as_mut().and_then(VecDeque::pop_front) else {
            return;
        };
        merge.held_bytes = merge.held_bytes.saturating_sub(bytes);
        merge.deferred = self.pending_row.take();
        self.pending_row = Some((row, bytes, BoundedWorkSource::TableScan));
        self.pending_from_committed = false;
    }

    /// The next staged row of an ordered walk, once the committed run is done.
    fn next_ordered_staged_row(&mut self) -> Option<(VersionedRow, usize)> {
        let merge = self.ordered_merge.as_mut()?;
        let (row, bytes) = merge.staged.as_mut().and_then(VecDeque::pop_front)?;
        merge.held_bytes = merge.held_bytes.saturating_sub(bytes);
        Some((row, bytes))
    }

    fn next(
        &mut self,
        db: &dyn ReadExecutionTarget,
        context: &Arc<RequestContext>,
    ) -> std::result::Result<Option<PulledRow>, BoundedExecutionError> {
        self.materialize_ordered_staged(db, context)?;
        loop {
            if self.pending_row.is_none()
                && let Some(merge) = self.ordered_merge.as_mut()
                && let Some(held) = merge.deferred.take()
            {
                self.pending_row = Some(held);
                self.pending_from_committed = true;
            }
            if self.pending_row.is_none() {
                let reserved = Cell::new(0usize);
                let (row, source) = match &mut self.mode {
                    ScanMode::Physical { cursor } => {
                        let pulled = db.bounded_physical_table_row_next(
                            self.table.as_str(),
                            cursor,
                            &mut || {
                                context.charge(
                                    BoundedWorkSource::TableScan,
                                    BoundedSourceTouch::TableRow,
                                )
                            },
                            &mut |bytes| {
                                context.reserve(BoundedWorkSource::TableScan, bytes)?;
                                reserved.set(bytes);
                                Ok(())
                            },
                        )?;
                        (pulled, BoundedWorkSource::TableScan)
                    }
                    ScanMode::Ordered {
                        column,
                        direction,
                        cursor,
                    } => {
                        let old_key_bytes = cursor.retained_key_bytes();
                        let old_key_generation = cursor.key_generation();
                        let pulled = db.bounded_ordered_table_row_next(
                            &self.table,
                            column,
                            *direction,
                            self.snapshot,
                            cursor,
                            &mut || {
                                context.charge(
                                    BoundedWorkSource::IndexRange,
                                    BoundedSourceTouch::IndexEntry,
                                )
                            },
                            &mut |bytes| {
                                context.reserve(BoundedWorkSource::IndexRange, bytes)?;
                                reserved.set(bytes);
                                Ok(())
                            },
                        )?;
                        if cursor.key_generation() != old_key_generation {
                            context.release(old_key_bytes)?;
                            let row_bytes = reserved
                                .get()
                                .checked_sub(cursor.retained_key_bytes())
                                .ok_or_else(|| {
                                    Error::Other(
                                        "bounded ordered row charge omitted its cursor key"
                                            .to_string(),
                                    )
                                })?;
                            reserved.set(row_bytes);
                            self.cursor_key_bytes = cursor.retained_key_bytes();
                        }
                        (pulled, BoundedWorkSource::IndexRange)
                    }
                    ScanMode::Index {
                        column,
                        direction,
                        cursor,
                        ..
                    } => {
                        let old_key_bytes = cursor.retained_key_bytes();
                        let old_key_generation = cursor.key_generation();
                        let pulled = db.bounded_ordered_table_row_next(
                            &self.table,
                            column,
                            *direction,
                            self.snapshot,
                            cursor,
                            &mut || {
                                context.charge(
                                    BoundedWorkSource::IndexRange,
                                    BoundedSourceTouch::IndexEntry,
                                )
                            },
                            &mut |bytes| {
                                context.reserve(BoundedWorkSource::IndexRange, bytes)?;
                                reserved.set(bytes);
                                Ok(())
                            },
                        )?;
                        if cursor.key_generation() != old_key_generation {
                            context.release(old_key_bytes)?;
                            let row_bytes = reserved
                                .get()
                                .checked_sub(cursor.retained_key_bytes())
                                .ok_or_else(|| {
                                    Error::Other(
                                        "bounded index row charge omitted its cursor key"
                                            .to_string(),
                                    )
                                })?;
                            reserved.set(row_bytes);
                            self.cursor_key_bytes = cursor.retained_key_bytes();
                        }
                        (pulled, BoundedWorkSource::IndexRange)
                    }
                    ScanMode::Exact { index, cursor } => {
                        let pulled = db.bounded_exact_index_row_next(
                            &self.table,
                            index,
                            self.snapshot,
                            cursor,
                            &mut || {
                                context.charge(
                                    BoundedWorkSource::IndexRange,
                                    BoundedSourceTouch::IndexEntry,
                                )
                            },
                            &mut |bytes| context.reserve(BoundedWorkSource::IndexRange, bytes),
                            &mut |bytes| context.release(bytes),
                            &mut |bytes| {
                                context.reserve(BoundedWorkSource::IndexRange, bytes)?;
                                reserved.set(bytes);
                                Ok(())
                            },
                        )?;
                        self.cursor_key_bytes = cursor.retained_bytes();
                        (pulled, BoundedWorkSource::IndexRange)
                    }
                    ScanMode::Empty => return Ok(None),
                };
                // The source charged its retained-representation estimate
                // before it cloned. This read holds exactly one copy of what
                // came back, so the reservation becomes what that copy costs.
                let row = match row {
                    Some(row) => {
                        let held = pulled_row_retained_bytes(&row)?;
                        reconcile_new_reservation(context, source, reserved.get(), held)?;
                        reserved.set(held);
                        Some(row)
                    }
                    None => None,
                };
                let Some(row) = row else {
                    let source_exhausted = match &self.mode {
                        ScanMode::Physical { cursor } => cursor.is_exhausted(),
                        ScanMode::Index { cursor, .. } => cursor.is_exhausted(),
                        ScanMode::Ordered { cursor, .. } => cursor.is_exhausted(),
                        ScanMode::Exact { cursor, .. } => cursor.is_exhausted(),
                        ScanMode::Empty => true,
                    };
                    if source_exhausted {
                        // The committed rows are done. If this handle has a
                        // transaction open, the rows it staged are part of
                        // its own answer and come next, each charged the same
                        // way a committed row is. An ordered walk has already
                        // read them and hands back whatever the fold has left.
                        if let Some((row, bytes)) = self.next_ordered_staged_row() {
                            self.pending_row = Some((row, bytes, source));
                            self.pending_from_committed = false;
                            continue;
                        }
                        if self.ordered_merge.is_none()
                            && let Some(staged) = self.next_staged_row(db, context)?
                        {
                            self.pending_row = Some((staged.0, staged.1, source));
                            self.pending_from_committed = false;
                            continue;
                        }
                        context.release(self.cursor_key_bytes)?;
                        self.cursor_key_bytes = 0;
                        return Ok(None);
                    }
                    continue;
                };
                self.pending_row = Some((row, reserved.get(), source));
                self.pending_from_committed = true;
            }
            // A staged row whose ordering value comes before the committed row
            // in hand is handed back first; the committed row waits its turn.
            if self.pending_from_committed {
                self.fold_in_earlier_staged_row();
            }
            if !self.supplement_columns.is_empty() {
                let (row_identity, row_lsn) = self
                    .pending_row
                    .as_ref()
                    .map(|(row, _, _)| (row.row_id, row.lsn))
                    .ok_or_else(|| Error::Other("bounded scan lost its row".to_string()))?;
                let filled: Vec<(String, Value)> = self
                    .supplement_columns
                    .iter()
                    .filter(|column| {
                        self.pending_row.as_ref().is_some_and(|(row, _, _)| {
                            matches!(row.values.get(*column), Some(Value::Null) | None)
                        })
                    })
                    .filter_map(|column| {
                        db.row_vector_for_column(
                            &self.table,
                            column,
                            row_identity,
                            row_lsn,
                            self.snapshot,
                        )
                        .map(|vector| (column.clone(), Value::Vector(vector)))
                    })
                    .collect();
                if let Some((row, _, _)) = self.pending_row.as_mut() {
                    for (column, value) in filled {
                        row.values.insert(column, value);
                    }
                }
            }

            let (source_bytes, source) = self
                .pending_row
                .as_ref()
                .map(|(_, bytes, source)| (*bytes, *source))
                .ok_or_else(|| Error::Other("bounded scan lost its pending row".to_string()))?;
            let row = &self
                .pending_row
                .as_ref()
                .ok_or_else(|| Error::Other("bounded scan lost its pending row".to_string()))?
                .0;
            // A row this handle's own transaction staged is not committed and
            // never will be at this snapshot, so the committed-visibility rule
            // is not the one that decides it -- the transaction it belongs to
            // is. Only a row that came from the committed source is judged
            // here.
            let from_committed = if self.ordered_merge.is_some() {
                self.pending_from_committed
            } else {
                self.transaction
                    .as_ref()
                    .is_none_or(|state| !state.committed_exhausted)
            };
            if from_committed && !row.visible_at(self.snapshot) {
                let (_, bytes, _) = self.pending_row.take().ok_or_else(|| {
                    Error::Other("bounded scan lost an invisible row".to_string())
                })?;
                context.release(bytes)?;
                continue;
            }
            if from_committed
                && let Some(state) = self.transaction.as_mut()
                && state.hides_committed(&self.table, row)
            {
                let (_, bytes, _) = self.pending_row.take().ok_or_else(|| {
                    Error::Other("bounded scan lost a row its transaction hides".to_string())
                })?;
                context.release(bytes)?;
                continue;
            }

            let index_pick = match &self.mode {
                ScanMode::Index { pick, .. } => Some(pick),
                _ => self.residual_pick.as_ref(),
            };
            if let Some(pick) = index_pick {
                let key = contextdb_relational::index_key_for_row(&pick.columns, &row.values);
                if !index_key_matches_pick(&key, pick) {
                    let (_, bytes, _) = self.pending_row.take().ok_or_else(|| {
                        Error::Other("bounded index scan lost a rejected row".to_string())
                    })?;
                    context.release(bytes)?;
                    continue;
                }
            }

            // A read that PINS the row it asks about is refused when the
            // reader is not entitled to it, with the reason the caller
            // branches on: answering "no such row" about a row that exists
            // tells the caller the store does not hold what it holds. A read
            // that merely scans is asking what it MAY see, and hiding is the
            // honest answer -- so the two are told apart by the executor's own
            // line, asked here rather than drawn again.
            let names_one_row = match (&self.mode, index_pick) {
                // An exact-lookup source asked a whole key: it is already
                // reading the row that key names.
                (ScanMode::Exact { index, .. }, _) => db.read_index_names_one_row(
                    &self.table,
                    index,
                    self.filter.as_ref(),
                    &self.params,
                ),
                (_, Some(pick)) => db.read_pick_names_one_row(
                    &self.table,
                    pick,
                    self.filter.as_ref(),
                    &self.params,
                ),
                _ => false,
            };
            let mut charge_access = |_candidate_bytes: usize| {
                context.charge(
                    BoundedWorkSource::AccessControl,
                    BoundedSourceTouch::AccessRow,
                )?;
                context.attribute_live_memory(BoundedWorkSource::AccessControl);
                Ok::<(), BoundedExecutionError>(())
            };
            let mut refused = None;
            let mut hidden = false;
            if names_one_row {
                refused = db.bounded_read_denial_for_row(
                    // The request's transaction, not this scan's row
                    // overlay: a transaction that staged only a GRANT row has
                    // no overlay on the table being scanned, and its own grant
                    // still has to admit its own rows.
                    context.transaction(),
                    &self.table,
                    &self.meta,
                    row,
                    self.snapshot,
                    &mut charge_access,
                )?;
            } else {
                hidden = !db.bounded_read_allowed_for_row(
                    // The request's transaction, not this scan's row
                    // overlay: a transaction that staged only a GRANT row has
                    // no overlay on the table being scanned, and its own grant
                    // still has to admit its own rows.
                    context.transaction(),
                    &self.table,
                    &self.meta,
                    row,
                    self.snapshot,
                    &mut charge_access,
                )?;
            }
            if hidden || refused.is_some() {
                let (_, bytes, _) = self.pending_row.take().ok_or_else(|| {
                    Error::Other("bounded scan lost a row the gate withheld".to_string())
                })?;
                context.release(bytes)?;
                if let Some(denial) = refused {
                    return Err(BoundedExecutionError::from(denial));
                }
                continue;
            }

            if let Some(filter) = self.filter.as_ref() {
                context.check_final_boundary()?;
                let matched = bounded_row_matches(row, filter, &self.params);
                context.check_final_boundary()?;
                if !matched? {
                    let (_, bytes, _) = self.pending_row.take().ok_or_else(|| {
                        Error::Other("bounded scan lost a filtered row".to_string())
                    })?;
                    context.release(bytes)?;
                    continue;
                }
            }

            let row_id = i64::try_from(row.row_id.0).map_err(|_| {
                Error::Other("bounded row identifier exceeds SQL INTEGER".to_string())
            })?;
            let row_id_value = Value::Int64(row_id);
            let output_capacity = self
                .schema_columns
                .len()
                .checked_add(1)
                .ok_or_else(|| bounded_size_error("scan projection"))?;
            let projected_values = self
                .schema_columns
                .iter()
                .map(|column| row.values.get(column).map_or(&Value::Null, |value| value));
            let planned_output_bytes = value_refs_retained_bytes(
                std::iter::once(&row_id_value).chain(projected_values),
                output_capacity,
            )?;
            context.reserve(source, planned_output_bytes)?;
            let mut values = Vec::new();
            if values.try_reserve_exact(output_capacity).is_err() {
                context.release(planned_output_bytes)?;
                return Err(
                    Error::Other("bounded scan projection allocation failed".to_string()).into(),
                );
            }
            values.push(row_id_value);
            values.extend(
                self.schema_columns
                    .iter()
                    .map(|column| match row.values.get(column) {
                        Some(value) => value.clone(),
                        None => Value::Null,
                    }),
            );
            let output_bytes = values_retained_bytes(&values)?;
            reconcile_new_reservation(context, source, planned_output_bytes, output_bytes)?;
            let (_, retained_source_bytes, _) = self
                .pending_row
                .take()
                .ok_or_else(|| Error::Other("bounded scan lost its projected row".to_string()))?;
            if source_bytes != retained_source_bytes {
                return Err(Error::Other(
                    "bounded scan source charge changed before release".to_string(),
                )
                .into());
            }
            context.release(retained_source_bytes)?;
            if let ScanMode::Physical { cursor } = &mut self.mode {
                // The emitted anchor is what a later pull re-anchors against
                // after maintenance compacts the vector; an emitted row is
                // visible at this scan's registered snapshot, so maintenance
                // never removes it while the continuation is suspended.
                cursor.note_emitted();
            }
            return Ok(Some(PulledRow {
                values,
                retained_bytes: output_bytes,
            }));
        }
    }

    fn has_more_hint(&self) -> Option<bool> {
        if self.pending_row.is_some() {
            return None;
        }
        // Rows the fold is still holding are part of this answer, whether or
        // not the committed run has anything left.
        if self.ordered_merge.as_ref().is_some_and(|merge| {
            merge.deferred.is_some()
                || merge
                    .staged
                    .as_ref()
                    .is_some_and(|staged| !staged.is_empty())
        }) {
            return None;
        }
        match &self.mode {
            ScanMode::Physical { cursor } if cursor.is_exhausted() => Some(false),
            ScanMode::Index { cursor, .. } if cursor.is_exhausted() => Some(false),
            ScanMode::Ordered { cursor, .. } if cursor.is_exhausted() => Some(false),
            ScanMode::Exact { cursor, .. } if cursor.is_exhausted() => Some(false),
            ScanMode::Empty => Some(false),
            ScanMode::Physical { .. }
            | ScanMode::Index { .. }
            | ScanMode::Ordered { .. }
            | ScanMode::Exact { .. } => None,
        }
    }
}

impl GraphState {
    fn next_unpinned_edge(
        &mut self,
        db: &dyn ReadExecutionTarget,
        context: &Arc<RequestContext>,
    ) -> std::result::Result<Option<(uuid::Uuid, uuid::Uuid)>, BoundedExecutionError> {
        loop {
            let Some(source) = self.edge_source.as_mut() else {
                return Ok(None);
            };
            let first_step = self.steps.first().ok_or_else(|| {
                Error::Other("bounded graph plan has no traversal step".to_string())
            })?;
            let edge_types =
                (!first_step.edge_types.is_empty()).then_some(first_step.edge_types.as_slice());
            let edge = db.bounded_graph_edge_next(
                source,
                edge_types,
                context.transaction(),
                &mut || {
                    context.charge(
                        BoundedWorkSource::GraphTraversal,
                        BoundedSourceTouch::GraphEdge,
                    )
                },
                &mut || {
                    context.charge(
                        BoundedWorkSource::AccessControl,
                        BoundedSourceTouch::AccessRow,
                    )?;
                    context.attribute_live_memory(BoundedWorkSource::AccessControl);
                    Ok(())
                },
                &mut |bytes| context.reserve(BoundedWorkSource::GraphTraversal, bytes),
                &mut |bytes| context.release(bytes),
                &mut || context.note_touch(BoundedSourceTouch::AdjacencyEntry),
            )?;
            let Some(edge) = edge else {
                let source = self.edge_source.take().ok_or_else(|| {
                    Error::Other("bounded graph lost its exhausted edge source".to_string())
                })?;
                context.release(source.retained_bytes())?;
                return Ok(None);
            };
            if self.single_step_unpinned {
                match self.seen_edges.binary_search(&edge) {
                    Ok(_) => continue,
                    Err(position) => {
                        let added = reserve_vector_slot(
                            context,
                            BoundedWorkSource::GraphTraversal,
                            &mut self.seen_edges,
                            "graph edge deduplication",
                        )?;
                        self.seen_edge_container_bytes = self
                            .seen_edge_container_bytes
                            .checked_add(added)
                            .ok_or_else(|| bounded_size_error("graph edge deduplication"))?;
                        self.seen_edges.insert(position, edge);
                        return Ok(Some(edge));
                    }
                }
            }
            let start = edge.0;
            match self.seen_starts.binary_search(&start) {
                Ok(_) => continue,
                Err(position) => {
                    let added = reserve_vector_slot(
                        context,
                        BoundedWorkSource::GraphTraversal,
                        &mut self.seen_starts,
                        "graph start deduplication",
                    )?;
                    self.seen_start_container_bytes = self
                        .seen_start_container_bytes
                        .checked_add(added)
                        .ok_or_else(|| bounded_size_error("graph start deduplication"))?;
                    self.seen_starts.insert(position, start);
                    return Ok(Some(edge));
                }
            }
        }
    }

    fn next_start(
        &mut self,
        db: &dyn ReadExecutionTarget,
        context: &Arc<RequestContext>,
    ) -> std::result::Result<Option<uuid::Uuid>, BoundedExecutionError> {
        let start = self.resolve_next_start(db, context)?;
        if start.is_some() {
            // One more resolved start, charged to the store as it is resolved.
            context.charge_store_frontier(self.frontier_unit_bytes)?;
        }
        Ok(start)
    }

    fn resolve_next_start(
        &mut self,
        db: &dyn ReadExecutionTarget,
        context: &Arc<RequestContext>,
    ) -> std::result::Result<Option<uuid::Uuid>, BoundedExecutionError> {
        if let Some(start) = self.starts.pop_front() {
            return Ok(Some(start));
        }
        if let Some(candidates) = self.start_candidates.as_mut() {
            loop {
                let Some(row) = candidates.next(db, context)? else {
                    self.start_candidates = None;
                    break;
                };
                let start = row.values.iter().find_map(|value| match value {
                    Value::Uuid(id) => Some(Ok(*id)),
                    Value::Text(text) => Some(uuid::Uuid::parse_str(text).map_err(|_| {
                        Error::Other(format!(
                            "bounded graph start candidate is not a UUID: {text}"
                        ))
                    })),
                    _ => None,
                });
                row.release(context)?;
                if let Some(start) = start {
                    return start.map(Some).map_err(BoundedExecutionError::from);
                }
            }
        }
        Ok(self.next_unpinned_edge(db, context)?.map(|edge| edge.0))
    }

    fn next_unpinned_single_step(
        &mut self,
        db: &dyn ReadExecutionTarget,
        context: &Arc<RequestContext>,
    ) -> std::result::Result<Option<PulledRow>, BoundedExecutionError> {
        loop {
            let Some((start, target)) = self.next_unpinned_edge(db, context)? else {
                context.release(self.seen_edge_container_bytes)?;
                self.seen_edge_container_bytes = 0;
                self.seen_edges = Vec::new();
                self.row_memory.release(context)?;
                return Ok(None);
            };
            let binding_capacity = 2usize;
            let planned_binding_bytes = checked_size_add(
                std::mem::size_of::<HashMap<String, uuid::Uuid>>(),
                checked_size_add(
                    checked_size_mul(
                        binding_capacity,
                        std::mem::size_of::<(String, uuid::Uuid)>(),
                        "graph bindings",
                    )?,
                    checked_size_add(
                        self.start_alias.len(),
                        self.steps[0].target_alias.len(),
                        "graph binding aliases",
                    )?,
                    "graph bindings",
                )?,
                "graph bindings",
            )?;
            self.row_memory.take(context, planned_binding_bytes)?;
            let mut bindings = HashMap::new();
            if bindings.try_reserve(binding_capacity).is_err() {
                self.row_memory.give_back(planned_binding_bytes)?;
                return Err(
                    Error::Other("bounded graph binding allocation failed".to_string()).into(),
                );
            }
            bindings.insert(self.start_alias.clone(), start);
            bindings.insert(self.steps[0].target_alias.clone(), target);
            let actual_binding_bytes = checked_size_add(
                std::mem::size_of::<HashMap<String, uuid::Uuid>>(),
                checked_size_add(
                    checked_size_mul(
                        bindings.capacity(),
                        std::mem::size_of::<(String, uuid::Uuid)>(),
                        "graph bindings",
                    )?,
                    bindings.keys().try_fold(0usize, |bytes, alias| {
                        checked_size_add(bytes, alias.capacity(), "graph binding aliases")
                    })?,
                    "graph bindings",
                )?,
                "graph bindings",
            )?;
            if actual_binding_bytes > planned_binding_bytes {
                if let Err(error) = self
                    .row_memory
                    .take(context, actual_binding_bytes - planned_binding_bytes)
                {
                    self.row_memory.give_back(planned_binding_bytes)?;
                    return Err(error);
                }
            } else {
                self.row_memory
                    .give_back(planned_binding_bytes - actual_binding_bytes)?;
            }
            let matches = self
                .filter
                .as_ref()
                .map(|filter| {
                    db.graph_filter_matches_bindings(
                        filter,
                        &self.params,
                        // The reader's own transaction: a WHERE on node
                        // metadata asks what this handle can see, and what it
                        // staged is part of that. Everything below this call
                        // already takes the transaction and already composes
                        // the overlay; only this door was never handing it
                        // over.
                        context.transaction(),
                        self.snapshot,
                        &bindings,
                    )
                })
                .transpose();
            let matches = match matches {
                Ok(Some(matches)) => matches,
                Ok(None) => true,
                Err(error) => {
                    self.row_memory.give_back(actual_binding_bytes)?;
                    return Err(error.into());
                }
            };
            self.row_memory.give_back(actual_binding_bytes)?;
            if !matches {
                continue;
            }
            let planned_values = 4usize;
            let planned_output_bytes = checked_size_add(
                std::mem::size_of::<Vec<Value>>(),
                checked_size_mul(
                    planned_values,
                    std::mem::size_of::<Value>(),
                    "graph projection",
                )?,
                "graph projection",
            )?;
            self.row_memory.take(context, planned_output_bytes)?;
            let mut projected = Vec::new();
            if projected.try_reserve_exact(planned_values).is_err() {
                self.row_memory.give_back(planned_output_bytes)?;
                return Err(
                    Error::Other("bounded graph projection allocation failed".to_string()).into(),
                );
            }
            projected.push(Value::Uuid(start));
            projected.push(Value::Uuid(target));
            projected.push(Value::Uuid(target));
            projected.push(Value::Int64(1));
            let output_bytes = values_retained_bytes(&projected)?;
            if output_bytes > planned_output_bytes {
                if let Err(error) = self
                    .row_memory
                    .take(context, output_bytes - planned_output_bytes)
                {
                    self.row_memory.give_back(planned_output_bytes)?;
                    return Err(error);
                }
            } else {
                self.row_memory
                    .give_back(planned_output_bytes - output_bytes)?;
            }
            return Ok(Some(PulledRow {
                values: projected,
                retained_bytes: output_bytes,
            }));
        }
    }

    fn initial_path(
        &self,
        start: uuid::Uuid,
        context: &Arc<RequestContext>,
    ) -> std::result::Result<GraphPathState, BoundedExecutionError> {
        let capacity = self
            .steps
            .len()
            .checked_add(1)
            .ok_or_else(|| bounded_size_error("graph path bindings"))?;
        let planned_bytes = checked_size_mul(
            capacity,
            std::mem::size_of::<uuid::Uuid>(),
            "graph path bindings",
        )?;
        context.reserve(BoundedWorkSource::GraphTraversal, planned_bytes)?;
        let mut bindings = Vec::new();
        if bindings.try_reserve_exact(capacity).is_err() {
            context.release(planned_bytes)?;
            return Err(Error::Other("bounded graph path allocation failed".to_string()).into());
        }
        bindings.push(start);
        let retained_bytes = checked_size_mul(
            bindings.capacity(),
            std::mem::size_of::<uuid::Uuid>(),
            "graph path bindings",
        )?;
        reconcile_new_reservation(
            context,
            BoundedWorkSource::GraphTraversal,
            planned_bytes,
            retained_bytes,
        )?;
        Ok(GraphPathState {
            bindings,
            node: start,
            total_depth: 0,
            step_index: 0,
            retained_bytes,
        })
    }

    fn open_active(
        &mut self,
        db: &dyn ReadExecutionTarget,
        context: &Arc<RequestContext>,
        path: GraphPathState,
    ) -> std::result::Result<(), BoundedExecutionError> {
        let step = self.steps.get(path.step_index).ok_or_else(|| {
            Error::Other("bounded graph continuation has no traversal step".to_string())
        })?;
        let edge_types = (!step.edge_types.is_empty()).then_some(step.edge_types.as_slice());
        let direction = if self.probes_backwards_from_target {
            super::reverse_graph_probe_direction(step.direction)
        } else {
            step.direction
        };
        let cursor = db.bounded_graph_bfs_cursor(
            context.transaction(),
            path.node,
            edge_types,
            direction,
            step.min_depth,
            step.max_depth,
            self.snapshot,
            &mut || {
                context.charge(
                    BoundedWorkSource::AccessControl,
                    BoundedSourceTouch::AccessRow,
                )?;
                context.attribute_live_memory(BoundedWorkSource::AccessControl);
                Ok(())
            },
            &mut |bytes| context.reserve(BoundedWorkSource::GraphTraversal, bytes),
            &mut |bytes| context.release_parked(bytes),
        )?;
        let Some(cursor) = cursor else {
            context.release(path.retained_bytes)?;
            return Ok(());
        };
        self.active = Some(GraphActiveState { path, cursor });
        Ok(())
    }

    /// Forget the neighbours a finished probe reported, and give back what
    /// remembering them was charged for.
    fn release_probe_seen(
        &mut self,
        context: &Arc<RequestContext>,
    ) -> std::result::Result<(), BoundedExecutionError> {
        let held = std::mem::take(&mut self.probe_seen_container_bytes);
        self.probe_seen = Vec::new();
        context.release(held)
    }

    fn next(
        &mut self,
        db: &dyn ReadExecutionTarget,
        context: &Arc<RequestContext>,
    ) -> std::result::Result<Option<PulledRow>, BoundedExecutionError> {
        if self.single_step_unpinned {
            return self.next_unpinned_single_step(db, context);
        }
        loop {
            if self.active.is_none() {
                let path = if let Some(path) = self.frontier.pop() {
                    path
                } else if let Some(start) = self.next_start(db, context)? {
                    self.initial_path(start, context)?
                } else {
                    context.release(self.frontier_container_bytes)?;
                    self.frontier_container_bytes = 0;
                    self.frontier = Vec::new();
                    context.release(self.seen_start_container_bytes)?;
                    self.seen_start_container_bytes = 0;
                    self.seen_starts = Vec::new();
                    context.release(self.seen_path_container_bytes)?;
                    self.seen_path_container_bytes = 0;
                    self.seen_paths = Vec::new();
                    self.release_probe_seen(context)?;
                    return Ok(None);
                };
                self.open_active(db, context, path)?;
                if self.active.is_none() {
                    continue;
                }
            }

            let active = self.active.as_mut().ok_or_else(|| {
                Error::Other("bounded graph lost its active continuation".to_string())
            })?;
            let next = db.bounded_graph_bfs_next(
                &mut active.cursor,
                &mut || {
                    context.charge(
                        BoundedWorkSource::GraphTraversal,
                        BoundedSourceTouch::GraphEdge,
                    )
                },
                // A path clone is traversal work, not an edge touch, so it is
                // charged without a source touch and `rows_examined` keeps its
                // meaning.
                &mut || context.charge_operator(BoundedWorkSource::GraphTraversal),
                &mut || {
                    context.charge(
                        BoundedWorkSource::AccessControl,
                        BoundedSourceTouch::AccessRow,
                    )?;
                    context.attribute_live_memory(BoundedWorkSource::AccessControl);
                    Ok(())
                },
                &mut |bytes| context.reserve(BoundedWorkSource::GraphTraversal, bytes),
                &mut |bytes| context.release_parked(bytes),
                // Stepping over one staged edge is a source touch like reading
                // one committed adjacency entry, and the one name copied out of
                // the transaction is reserved on the same channel that releases
                // it.
                &mut || {
                    context.charge(
                        BoundedWorkSource::GraphTraversal,
                        BoundedSourceTouch::GraphEdge,
                    )
                },
                &mut |bytes| context.reserve(BoundedWorkSource::GraphTraversal, bytes),
                &mut |bytes| context.release_parked(bytes),
                &mut || context.note_touch(BoundedSourceTouch::AdjacencyEntry),
                self.visited_cap,
                context.transaction(),
            );
            // A continuation that is abandoned keeps its charge until the
            // caller gives it back, so a refusal or cancellation from the
            // traversal has to dispose of the cursor before it propagates.
            let next = match next {
                Ok(next) => next,
                Err(error) => {
                    if let Some(active) = self.active.take() {
                        context.release_parked(active.cursor.into_retained_bytes());
                        context.release_parked(active.path.retained_bytes);
                    }
                    return Err(error);
                }
            };
            let Some(node) = next else {
                let active = self.active.take().ok_or_else(|| {
                    Error::Other("bounded graph lost its exhausted continuation".to_string())
                })?;
                context.release(active.cursor.into_retained_bytes())?;
                context.release(active.path.retained_bytes)?;
                // The adjacency this probe was reporting out of is finished,
                // so what it reported is no longer worth remembering: the next
                // start's own probe answers about its own neighbours.
                self.release_probe_seen(context)?;
                continue;
            };
            // The traversal reserved these bytes for this one projected row
            // and released nothing; every exit below returns them.
            let node_reservation = ReservedBytes::new(context, node.reserved_bytes);
            // A single hop answers with one row per NEIGHBOUR, not one row per
            // edge that arrives at it: a node reachable both ways round an
            // undirected hop is one answer, not two. That is the rule the
            // other door applies to the same statement, in
            // `graph_adjacency_probe_counted`, and it is applied here over the
            // one adjacency being walked so the two doors publish the same
            // rows. The entry was still read, so it is still examined.
            if self.single_step_probe {
                match self.probe_seen.binary_search(&node.node.id) {
                    Ok(_) => {
                        node_reservation.release()?;
                        continue;
                    }
                    Err(position) => {
                        let added = match reserve_vector_slot(
                            context,
                            BoundedWorkSource::GraphTraversal,
                            &mut self.probe_seen,
                            "graph probe deduplication",
                        ) {
                            Ok(added) => added,
                            Err(error) => {
                                node_reservation.release()?;
                                return Err(error);
                            }
                        };
                        let Some(total) = self.probe_seen_container_bytes.checked_add(added) else {
                            node_reservation.release()?;
                            return Err(bounded_size_error("graph probe deduplication"));
                        };
                        self.probe_seen_container_bytes = total;
                        self.probe_seen.insert(position, node.node.id);
                    }
                }
            }
            // Two starts that arrive at the same nodes are ONE answer, and the
            // walk out of those nodes is made once: the rule the other door
            // applies between steps in `dedupe_graph_frontier`, keyed on what
            // the pattern's target aliases are bound to and never on where the
            // walk began. Without it the same decision is reported once per
            // route that reaches it, so a statement asking which decisions a
            // detection reaches gets one row per detection rather than one row
            // per decision.
            let key = {
                let active = self.active.as_ref().ok_or_else(|| {
                    Error::Other("bounded graph lost its emitted path".to_string())
                })?;
                let key_len = active
                    .path
                    .step_index
                    .checked_add(1)
                    .ok_or_else(|| bounded_size_error("graph path deduplication"))?;
                let key_bytes = match checked_size_mul(
                    key_len,
                    std::mem::size_of::<uuid::Uuid>(),
                    "graph path deduplication",
                ) {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        node_reservation.release()?;
                        return Err(error);
                    }
                };
                if let Err(error) = context.reserve(BoundedWorkSource::GraphTraversal, key_bytes) {
                    node_reservation.release()?;
                    return Err(error);
                }
                let mut key: Vec<uuid::Uuid> = Vec::new();
                if key.try_reserve_exact(key_len).is_err() {
                    context.release(key_bytes)?;
                    node_reservation.release()?;
                    return Err(Error::Other(
                        "bounded graph path deduplication allocation failed".to_string(),
                    )
                    .into());
                }
                key.extend(active.path.bindings.iter().skip(1).copied());
                key.push(node.node.id);
                (key, key_bytes)
            };
            let (key, key_bytes) = key;
            match self.seen_paths.binary_search(&key) {
                Ok(_) => {
                    context.release(key_bytes)?;
                    node_reservation.release()?;
                    continue;
                }
                Err(position) => {
                    let added = match reserve_vector_slot(
                        context,
                        BoundedWorkSource::GraphTraversal,
                        &mut self.seen_paths,
                        "graph path deduplication",
                    ) {
                        Ok(added) => added,
                        Err(error) => {
                            context.release(key_bytes)?;
                            node_reservation.release()?;
                            return Err(error);
                        }
                    };
                    let total = self
                        .seen_path_container_bytes
                        .checked_add(added)
                        .and_then(|bytes| bytes.checked_add(key_bytes));
                    let Some(total) = total else {
                        node_reservation.release()?;
                        return Err(bounded_size_error("graph path deduplication"));
                    };
                    self.seen_path_container_bytes = total;
                    self.seen_paths.insert(position, key);
                }
            }
            let active = self
                .active
                .as_ref()
                .ok_or_else(|| Error::Other("bounded graph lost its emitted path".to_string()))?;
            let step_index = active.path.step_index;
            let total_depth = active
                .path
                .total_depth
                .checked_add(node.node.depth)
                .ok_or_else(|| Error::Other("bounded graph total depth overflow".to_string()))?;
            let final_step = step_index
                .checked_add(1)
                .ok_or_else(|| Error::Other("bounded graph step position overflow".to_string()))?
                == self.steps.len();
            if !final_step {
                let planned_bytes = active.path.retained_bytes;
                context.reserve(BoundedWorkSource::GraphTraversal, planned_bytes)?;
                let mut bindings = Vec::new();
                if bindings
                    .try_reserve_exact(active.path.bindings.capacity())
                    .is_err()
                {
                    context.release(planned_bytes)?;
                    return Err(Error::Other(
                        "bounded graph continuation allocation failed".to_string(),
                    )
                    .into());
                }
                bindings.extend(active.path.bindings.iter().copied());
                bindings.push(node.node.id);
                let retained_bytes = checked_size_mul(
                    bindings.capacity(),
                    std::mem::size_of::<uuid::Uuid>(),
                    "graph path bindings",
                )?;
                reconcile_new_reservation(
                    context,
                    BoundedWorkSource::GraphTraversal,
                    planned_bytes,
                    retained_bytes,
                )?;
                let path = GraphPathState {
                    bindings,
                    node: node.node.id,
                    total_depth,
                    step_index: step_index.checked_add(1).ok_or_else(|| {
                        Error::Other("bounded graph step position overflow".to_string())
                    })?,
                    retained_bytes,
                };
                let added = reserve_vector_slot(
                    context,
                    BoundedWorkSource::GraphTraversal,
                    &mut self.frontier,
                    "graph continuation stack",
                );
                let added = match added {
                    Ok(added) => added,
                    Err(error) => {
                        context.release(path.retained_bytes)?;
                        return Err(error);
                    }
                };
                self.frontier_container_bytes = self
                    .frontier_container_bytes
                    .checked_add(added)
                    .ok_or_else(|| bounded_size_error("graph continuation stack"))?;
                self.frontier.push(path);
                continue;
            }

            let binding_capacity = self
                .steps
                .len()
                .checked_add(1)
                .ok_or_else(|| bounded_size_error("graph bindings"))?;
            let planned_binding_bytes = checked_size_add(
                std::mem::size_of::<HashMap<String, uuid::Uuid>>(),
                checked_size_mul(
                    binding_capacity,
                    std::mem::size_of::<(String, uuid::Uuid)>(),
                    "graph bindings",
                )?,
                "graph bindings",
            )?;
            let alias_bytes =
                self.steps
                    .iter()
                    .try_fold(self.start_alias.len(), |bytes, step| {
                        checked_size_add(bytes, step.target_alias.len(), "graph binding aliases")
                    })?;
            let planned_binding_bytes =
                checked_size_add(planned_binding_bytes, alias_bytes, "graph bindings")?;
            context.reserve(BoundedWorkSource::GraphTraversal, planned_binding_bytes)?;
            let mut bindings = HashMap::new();
            if bindings.try_reserve(binding_capacity).is_err() {
                context.release(planned_binding_bytes)?;
                return Err(
                    Error::Other("bounded graph binding allocation failed".to_string()).into(),
                );
            }
            let start = active.path.bindings.first().copied().ok_or_else(|| {
                Error::Other("bounded graph path has no start binding".to_string())
            })?;
            if self.probes_backwards_from_target {
                // Walked backwards out of the pinned end, so what the walk
                // calls its start is the hop's TARGET and what it reached is
                // the hop's start. Named here the way the statement named
                // them, so the answer reads as the statement was written.
                let target_alias = self
                    .steps
                    .first()
                    .map(|step| step.target_alias.clone())
                    .ok_or_else(|| {
                        Error::Other("bounded graph backwards probe has no step".to_string())
                    })?;
                bindings.insert(self.start_alias.clone(), node.node.id);
                bindings.insert(target_alias, start);
            } else {
                bindings.insert(self.start_alias.clone(), start);
            }
            if !self.probes_backwards_from_target {
                for (step, id) in self.steps.iter().zip(
                    active
                        .path
                        .bindings
                        .iter()
                        .skip(1)
                        .copied()
                        .chain(std::iter::once(node.node.id)),
                ) {
                    bindings.insert(step.target_alias.clone(), id);
                }
            }
            let actual_binding_bytes = bindings.keys().try_fold(
                checked_size_add(
                    std::mem::size_of::<HashMap<String, uuid::Uuid>>(),
                    checked_size_mul(
                        bindings.capacity(),
                        std::mem::size_of::<(String, uuid::Uuid)>(),
                        "graph bindings",
                    )?,
                    "graph bindings",
                )?,
                |bytes, alias| checked_size_add(bytes, alias.capacity(), "graph binding aliases"),
            )?;
            reconcile_new_reservation(
                context,
                BoundedWorkSource::GraphTraversal,
                planned_binding_bytes,
                actual_binding_bytes,
            )?;
            let matches = self
                .filter
                .as_ref()
                .map(|filter| {
                    db.graph_filter_matches_bindings(
                        filter,
                        &self.params,
                        // The reader's own transaction: a WHERE on node
                        // metadata asks what this handle can see, and what it
                        // staged is part of that. Everything below this call
                        // already takes the transaction and already composes
                        // the overlay; only this door was never handing it
                        // over.
                        context.transaction(),
                        self.snapshot,
                        &bindings,
                    )
                })
                .transpose();
            let matches = match matches {
                Ok(Some(matches)) => matches,
                Ok(None) => true,
                Err(error) => {
                    context.release(actual_binding_bytes)?;
                    return Err(error.into());
                }
            };
            context.release(actual_binding_bytes)?;
            if !matches {
                continue;
            }
            let projected_capacity = binding_capacity
                .checked_add(2)
                .ok_or_else(|| bounded_size_error("graph projection"))?;
            let planned_output_bytes = checked_size_add(
                std::mem::size_of::<Vec<Value>>(),
                checked_size_mul(
                    projected_capacity,
                    std::mem::size_of::<Value>(),
                    "graph projection",
                )?,
                "graph projection",
            )?;
            context.reserve(BoundedWorkSource::GraphTraversal, planned_output_bytes)?;
            let mut projected = Vec::new();
            if projected.try_reserve_exact(projected_capacity).is_err() {
                context.release(planned_output_bytes)?;
                return Err(
                    Error::Other("bounded graph projection allocation failed".to_string()).into(),
                );
            }
            if self.probes_backwards_from_target {
                // The walk went out of the hop's pinned END, so its own idea
                // of start and reached is the statement's idea of them
                // reversed. The answer is published the way the statement was
                // written: the node the walk reached is where the hop starts,
                // and the pinned node is where it ends.
                projected.push(Value::Uuid(node.node.id));
                projected.extend(active.path.bindings.iter().copied().map(Value::Uuid));
            } else {
                projected.extend(active.path.bindings.iter().copied().map(Value::Uuid));
                projected.push(Value::Uuid(node.node.id));
            }
            projected.push(Value::Uuid(node.node.id));
            projected.push(Value::Int64(i64::from(total_depth)));
            let output_bytes = values_retained_bytes(&projected)?;
            reconcile_new_reservation(
                context,
                BoundedWorkSource::GraphTraversal,
                planned_output_bytes,
                output_bytes,
            )?;
            node_reservation.release()?;
            return Ok(Some(PulledRow {
                values: projected,
                retained_bytes: output_bytes,
            }));
        }
    }

    fn has_more_hint(&self) -> Option<bool> {
        if self.active.is_some()
            || !self.frontier.is_empty()
            || !self.starts.is_empty()
            || self.start_candidates.is_some()
            || self.edge_source.is_some()
        {
            None
        } else {
            Some(false)
        }
    }
}

fn bounded_graph_static_starts(
    filter: &Expr,
    params: &HashMap<String, Value>,
    start_alias: &str,
) -> std::result::Result<Option<Vec<uuid::Uuid>>, BoundedExecutionError> {
    match filter {
        Expr::BinaryOp {
            left,
            op: BinOp::Eq,
            right,
        } if is_graph_start_column_ref(left, start_alias, "id")
            || is_graph_start_column_ref(right, start_alias, "id") =>
        {
            let value = if is_graph_start_column_ref(left, start_alias, "id") {
                right
            } else {
                left
            };
            Ok(
                resolve_graph_static_uuid_expr(value, params, "graph start identifier in filter")?
                    .map(|id| vec![id]),
            )
        }
        Expr::InList {
            expr,
            list,
            negated: false,
        } if is_graph_start_column_ref(expr, start_alias, "id") => {
            let mut ids = Vec::with_capacity(list.len());
            for item in list {
                let Some(id) = resolve_graph_static_uuid_expr(
                    item,
                    params,
                    "graph start identifier in filter",
                )?
                else {
                    return Ok(None);
                };
                ids.push(id);
            }
            Ok(Some(ids))
        }
        Expr::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            let left = bounded_graph_static_starts(left, params, start_alias)?;
            let right = bounded_graph_static_starts(right, params, start_alias)?;
            Ok(match (left, right) {
                (Some(left), Some(right)) => {
                    let right = right.into_iter().collect::<BTreeSet<_>>();
                    Some(left.into_iter().filter(|id| right.contains(id)).collect())
                }
                (Some(ids), None) | (None, Some(ids)) => Some(ids),
                (None, None) => None,
            })
        }
        Expr::BinaryOp {
            left,
            op: BinOp::Or,
            right,
        } => {
            let left = bounded_graph_static_starts(left, params, start_alias)?;
            let right = bounded_graph_static_starts(right, params, start_alias)?;
            Ok(match (left, right) {
                (Some(left), Some(right)) => Some(
                    left.into_iter()
                        .chain(right)
                        .collect::<BTreeSet<_>>()
                        .into_iter()
                        .collect(),
                ),
                _ => None,
            })
        }
        _ => Ok(None),
    }
}

#[cfg(feature = "test-seams")]
struct BoundedHnswProbeObserver {
    probe: Arc<dyn BoundedExecutionProbe>,
}

#[cfg(feature = "test-seams")]
impl contextdb_vector::hnsw::HnswCandidateObserver for BoundedHnswProbeObserver {
    fn before_candidate_distance(&self, event: contextdb_vector::hnsw::HnswCandidateDistanceEvent) {
        self.probe.before_hnsw_candidate_distance(event);
    }
}

/// The capacity an accumulating collection starts at before doubling. Small
/// enough that a read accumulating a handful of items pays for a handful.
const AMORTIZED_GROWTH_FLOOR: usize = 4;

fn reserve_vector_slot<T>(
    context: &Arc<RequestContext>,
    source: BoundedWorkSource,
    values: &mut Vec<T>,
    operation: &str,
) -> std::result::Result<usize, BoundedExecutionError> {
    if values.len() < values.capacity() {
        return Ok(0);
    }
    let previous_capacity = values.capacity();
    // Growing by one slot at a time reallocates and copies everything already
    // accumulated on every append, so a read that examines many items costs
    // far more than the declared work ceiling counts. Asking for the larger
    // capacity before growing into it keeps the charge ahead of the memory
    // and keeps the ceiling describing what the read does.
    let target = previous_capacity
        .saturating_mul(2)
        .max(AMORTIZED_GROWTH_FLOOR);
    let additional = target.saturating_sub(previous_capacity).max(1);
    let bytes = checked_size_mul(additional, std::mem::size_of::<T>(), operation)?;
    context.reserve(source, bytes)?;
    if values.try_reserve_exact(additional).is_err() {
        context.release(bytes)?;
        return Err(Error::Other(format!("bounded {operation} allocation failed")).into());
    }
    let actual = match checked_size_mul(values.capacity(), std::mem::size_of::<T>(), operation) {
        Ok(actual) => actual,
        Err(error) => {
            context.release(bytes)?;
            return Err(error);
        }
    };
    let prior = match checked_size_mul(previous_capacity, std::mem::size_of::<T>(), operation) {
        Ok(prior) => prior,
        Err(error) => {
            context.release(bytes)?;
            return Err(error);
        }
    };
    let delta = match actual.checked_sub(prior) {
        Some(delta) => delta,
        None => {
            context.release(bytes)?;
            return Err(
                Error::Other(format!("bounded {operation} capacity moved backwards")).into(),
            );
        }
    };
    reconcile_new_reservation(context, source, bytes, delta)?;
    Ok(delta)
}

/// Which column of a candidate set names the rows it picked.
///
/// `row_id` is the direct name and is preferred wherever the plan carries it.
/// A candidate set that came through named queries carries the row's own `id`
/// instead, which names the same row one lookup away.
enum VectorCandidateIdentity {
    RowId(usize),
    NaturalId(usize),
}

fn vector_candidate_identity(columns: &[String]) -> Option<VectorCandidateIdentity> {
    if let Some(position) = columns
        .iter()
        .position(|column| column == "row_id" || column.rsplit('.').next() == Some("row_id"))
    {
        return Some(VectorCandidateIdentity::RowId(position));
    }
    columns
        .iter()
        .position(|column| column == "id" || column.rsplit('.').next() == Some("id"))
        .map(VectorCandidateIdentity::NaturalId)
}

impl VectorState {
    /// The row one scored candidate names, projected the way the source used
    /// to project it: the row id first, then the table's columns in order.
    ///
    /// `None` where the row is gone or this reader may not have it -- the same
    /// two outcomes the scan produced, decided per published row, so what the
    /// search admitted and what the reader is entitled to stay separate
    /// questions.
    fn materialize_scored_row(
        &mut self,
        db: &dyn ReadExecutionTarget,
        context: &Arc<RequestContext>,
        row_id: RowId,
    ) -> std::result::Result<Option<PulledRow>, BoundedExecutionError> {
        let mut retained = 0usize;
        let row = db.bounded_row_by_identity(
            context.transaction(),
            &self.table,
            row_id,
            self.snapshot,
            // Publishing a row the search already scored is charged as the
            // work it is, and it is NOT a source touch: the candidate source
            // already counted the rows this statement examined, and counting
            // the answer again would make a filtered search report more rows
            // read than the filter admitted -- one more per row returned. The
            // other door resolves the same row through the version index
            // (`RelationalStore::row_by_id`) and counts nothing, and
            // `rows_examined` is one figure both doors owe the same answer to.
            &mut || context.charge_operator(BoundedWorkSource::TableScan),
            &mut |bytes| {
                retained = bytes;
                context.reserve(BoundedWorkSource::TableScan, bytes)
            },
        )?;
        let Some(row) = row else {
            return Ok(None);
        };
        let Some(meta) = db.table_meta(&self.table) else {
            context.release(retained)?;
            return Ok(None);
        };
        // The gate still decides every published row, exactly as it did when
        // these rows arrived from a scan: hiding what this reader may not see
        // rather than refusing the statement, because a search over a
        // collection is a set read.
        let allowed = db.bounded_read_allowed_for_row(
            context.transaction(),
            &self.table,
            &meta,
            &row,
            self.snapshot,
            &mut |_bytes| {
                context.charge(
                    BoundedWorkSource::AccessControl,
                    BoundedSourceTouch::AccessRow,
                )?;
                context.attribute_live_memory(BoundedWorkSource::AccessControl);
                Ok(())
            },
        )?;
        if !allowed {
            context.release(retained)?;
            return Ok(None);
        }
        let mut row = row;
        for column in &self.supplement_columns {
            if !matches!(row.values.get(column), Some(Value::Null) | None) {
                continue;
            }
            if let Some(vector) =
                db.row_vector_for_column(&self.table, column, row.row_id, row.lsn, self.snapshot)
            {
                row.values.insert(column.clone(), Value::Vector(vector));
            }
        }
        let row = &row;
        let row_id_value =
            Value::Int64(i64::try_from(row.row_id.0).map_err(|_| {
                Error::Other("bounded row identifier exceeds SQL INTEGER".to_string())
            })?);
        let capacity = self
            .schema_columns
            .len()
            .checked_add(1)
            .ok_or_else(|| bounded_size_error("vector row projection"))?;
        let planned = value_refs_retained_bytes(
            std::iter::once(&row_id_value).chain(
                self.schema_columns
                    .iter()
                    .map(|column| row.values.get(column).unwrap_or(&Value::Null)),
            ),
            capacity,
        )?;
        context.reserve(BoundedWorkSource::TableScan, planned)?;
        let mut values = Vec::new();
        if values.try_reserve_exact(capacity).is_err() {
            context.release(planned)?;
            context.release(retained)?;
            return Err(Error::Other("bounded vector row allocation failed".to_string()).into());
        }
        values.push(row_id_value);
        values.extend(
            self.schema_columns
                .iter()
                .map(|column| row.values.get(column).cloned().unwrap_or(Value::Null)),
        );
        let actual = values_retained_bytes(&values)?;
        reconcile_new_reservation(context, BoundedWorkSource::TableScan, planned, actual)?;
        // The source copy has been projected and is no longer held.
        context.release(retained)?;
        Ok(Some(PulledRow {
            values,
            retained_bytes: actual,
        }))
    }

    /// Which identity a committed entry is published under, if at all, once
    /// this transaction's own vector work is taken into account.
    ///
    /// A move is a re-key rather than a re-read: the score the source just
    /// produced is the score of that vector wherever it is filed.
    fn overlay_publishes(&self, row_id: RowId) -> Option<RowId> {
        let Some(overlay) = self.vector_overlay.as_ref() else {
            return Some(row_id);
        };
        // A row this transaction restaged is published from what it staged,
        // not from the committed entry that version replaced.
        if overlay.removed.contains(&row_id) || overlay.staged_identities.contains(&row_id) {
            return None;
        }
        let published = overlay.moved.get(&row_id).copied().unwrap_or(row_id);
        if overlay.removed.contains(&published) || overlay.staged_identities.contains(&published) {
            return None;
        }
        self.candidate_admits(published).then_some(published)
    }

    /// Let the source yield the committed entries that this transaction has
    /// moved INTO the candidate set.
    ///
    /// The source filters and truncates by the identity an entry is filed
    /// under, which for a moved vector is the identity the statement never
    /// asked about. Adding those source identities to the list the source sees
    /// is what keeps a moved row reachable; it cannot widen the answer,
    /// because what is published is still judged on the identity it is
    /// published under.
    fn admit_move_sources(
        &mut self,
        context: &Arc<RequestContext>,
    ) -> std::result::Result<(), BoundedExecutionError> {
        if !self.candidate_filter {
            return Ok(());
        }
        let Some(overlay) = self.vector_overlay.as_ref() else {
            return Ok(());
        };
        let mut sources: Vec<u64> = overlay
            .moved
            .iter()
            .filter(|(from_row_id, to_row_id)| {
                self.candidate_ids.binary_search(&to_row_id.0).is_ok()
                    && self.candidate_ids.binary_search(&from_row_id.0).is_err()
            })
            .map(|(from_row_id, _)| from_row_id.0)
            .collect();
        if sources.is_empty() {
            return Ok(());
        }
        sources.sort_unstable();
        sources.dedup();
        for source in sources {
            let added = reserve_vector_slot(
                context,
                BoundedWorkSource::VectorCandidates,
                &mut self.candidate_ids,
                "vector candidate ids",
            )?;
            self.candidate_bytes = self
                .candidate_bytes
                .checked_add(added)
                .ok_or_else(|| bounded_size_error("vector candidate ids"))?;
            self.candidate_ids.push(source);
        }
        self.candidate_ids.sort_unstable();
        self.candidate_ids.dedup();
        Ok(())
    }

    /// Whether the candidate set this statement named still admits a row.
    fn candidate_admits(&self, row_id: RowId) -> bool {
        if !self.candidate_filter || self.vector_overlay.is_none() {
            return true;
        }
        self.candidate_ids.binary_search(&row_id.0).is_ok()
    }

    /// Keep one scored candidate, admitted before the slot that holds it.
    fn keep_score(
        &mut self,
        context: &Arc<RequestContext>,
        row_id: RowId,
        score: f32,
    ) -> std::result::Result<(), BoundedExecutionError> {
        let added = reserve_vector_slot(
            context,
            BoundedWorkSource::VectorCandidates,
            &mut self.scores,
            "brute-force result scores",
        )?;
        self.score_bytes = self
            .score_bytes
            .checked_add(added)
            .ok_or_else(|| bounded_size_error("brute-force result scores"))?;
        self.scores.push((row_id, score));
        Ok(())
    }

    /// Score the vectors this transaction staged, after the committed entries
    /// are done.
    ///
    /// One touch is charged for each staged vector before it is read and its
    /// payload before it is copied, so a vector this handle staged costs what
    /// a committed entry of the same width costs; the copy is handed back as
    /// soon as the score is taken, because a score is all that is kept.
    fn score_staged_vectors(
        &mut self,
        db: &dyn ReadExecutionTarget,
        context: &Arc<RequestContext>,
    ) -> std::result::Result<(), BoundedExecutionError> {
        if self.vector_overlay.is_none() {
            return Ok(());
        }
        let Some(tx) = context.transaction() else {
            return Ok(());
        };
        let mut staged = 0usize;
        loop {
            let Some(position) = self
                .vector_overlay
                .as_ref()
                .and_then(|overlay| overlay.staged_positions.get(staged).copied())
            else {
                return Ok(());
            };
            staged = staged.saturating_add(1);
            let mut retained = 0usize;
            let entry = db.transaction_staged_vector(
                tx,
                position,
                &mut || {
                    context.charge(
                        BoundedWorkSource::VectorCandidates,
                        BoundedSourceTouch::BruteForceVectorCandidate,
                    )
                },
                &mut |bytes| {
                    retained = bytes;
                    context.reserve(BoundedWorkSource::VectorCandidates, bytes)
                },
            )?;
            let Some((row_id, vector)) = entry else {
                continue;
            };
            let scored = self
                .candidate_admits(row_id)
                .then(|| contextdb_vector::cosine_similarity(&vector, &self.query));
            drop(vector);
            context.release(retained)?;
            if let Some(score) = scored {
                self.keep_score(context, row_id, score)?;
            }
        }
    }

    fn next(
        &mut self,
        db: &dyn ReadExecutionTarget,
        context: &Arc<RequestContext>,
    ) -> std::result::Result<Option<PulledRow>, BoundedExecutionError> {
        loop {
            match self.preparation {
                VectorPreparation::Candidates(_) => {
                    let candidate = match &mut self.preparation {
                        VectorPreparation::Candidates(candidate) => candidate,
                        _ => {
                            return Err(Error::Other(
                                "bounded vector candidate state changed unexpectedly".to_string(),
                            )
                            .into());
                        }
                    };
                    let Some(root) = candidate.as_mut() else {
                        self.preparation = VectorPreparation::Search;
                        continue;
                    };
                    // How this candidate set names the rows it picked. A plan
                    // that reads the table directly projects the table's own
                    // `row_id`. A candidate set written as named queries -- a
                    // CTE projecting `id`, joined and filtered -- has no
                    // `row_id` to project, and the identity it does carry is
                    // the row's own `id`. The executor reads that second shape
                    // too (`executor.rs`, the `id_idx` arm of the candidate
                    // bitmap), so refusing it here leaves the caller with no
                    // smaller question to fall back on: the narrowing is the
                    // whole question.
                    let identity = vector_candidate_identity(&root.columns).ok_or_else(|| {
                        Error::Other(
                            "bounded vector candidate plan must name its rows by row_id or id"
                                .to_string(),
                        )
                    })?;
                    match root.next(db, context)? {
                        Some(row) => {
                            let resolved = match identity {
                                VectorCandidateIdentity::RowId(position) => {
                                    match row.values.get(position) {
                                        Some(Value::Int64(row_id)) => {
                                            Some(u64::try_from(*row_id).map_err(|_| {
                                                Error::Other(
                                                    "bounded vector candidate row_id is negative"
                                                        .to_string(),
                                                )
                                            })?)
                                        }
                                        _ => None,
                                    }
                                }
                                VectorCandidateIdentity::NaturalId(position) => {
                                    match row.values.get(position) {
                                        Some(key @ Value::Uuid(_)) => {
                                            // One index entry, charged before it
                                            // is read, to turn the row's own id
                                            // into the identity the vector index
                                            // keys by. Resolved one candidate at
                                            // a time inside the caller's own
                                            // transaction -- the eager helper
                                            // builds a map of the whole table,
                                            // which is exactly the unbounded read
                                            // a ceiling exists to prevent.
                                            context.charge(
                                                BoundedWorkSource::VectorCandidates,
                                                BoundedSourceTouch::IndexEntry,
                                            )?;
                                            db.row_id_for_natural_key_in_tx(
                                                context.transaction(),
                                                &self.table,
                                                "id",
                                                key,
                                                self.snapshot,
                                            )?
                                            .map(|row_id| row_id.0)
                                        }
                                        _ => None,
                                    }
                                }
                            };
                            let Some(row_id) = resolved else {
                                row.release(context)?;
                                continue;
                            };
                            let added = reserve_vector_slot(
                                context,
                                BoundedWorkSource::VectorCandidates,
                                &mut self.candidate_ids,
                                "vector candidate ids",
                            )?;
                            self.candidate_bytes = self
                                .candidate_bytes
                                .checked_add(added)
                                .ok_or_else(|| bounded_size_error("vector candidate ids"))?;
                            self.candidate_ids.push(row_id);
                            row.release(context)?;
                            continue;
                        }
                        None => {
                            self.candidate_ids.sort_unstable();
                            self.candidate_ids.dedup();
                            self.preparation = VectorPreparation::Search;
                            continue;
                        }
                    }
                }
                VectorPreparation::Search => {
                    // A move re-keys a score AFTER the source has yielded it,
                    // so a row whose moved-to identity the statement asked for
                    // has to survive the source's own filtering under the
                    // identity it is still filed under. The list the source
                    // gets therefore carries those source identities as well;
                    // what is finally published is judged against the
                    // statement's own list, on the identity it is published
                    // under.
                    self.admit_move_sources(context)?;
                    let candidates = self
                        .candidate_filter
                        .then_some(self.candidate_ids.as_slice());
                    let hnsw_search = || {
                        db.bounded_hnsw_vector_search(
                            &self.index,
                            &self.query,
                            self.candidate_k,
                            candidates,
                            self.snapshot,
                            &mut || context.charge_operator(BoundedWorkSource::VectorCandidates),
                            &mut || context.charge_operator(BoundedWorkSource::VectorCandidates),
                            &mut |bytes| {
                                context.reserve(BoundedWorkSource::VectorCandidates, bytes)
                            },
                            &mut |bytes| context.release_parked(bytes),
                        )
                    };
                    // A graph describes committed rows and nothing else, so
                    // while this transaction has vectors of its own in play
                    // there is no graph that can answer for them. The
                    // established door makes the same call -- it reports this
                    // case as not having used the index -- so both doors fall
                    // to the same exhaustive scoring rather than one of them
                    // answering confidently from an index that cannot see half
                    // the question.
                    let hnsw = if self.vector_overlay.is_some() {
                        None
                    } else {
                        #[cfg(feature = "test-seams")]
                        let searched = if let Some(probe) = context.probe() {
                            contextdb_vector::hnsw::with_hnsw_candidate_observer(
                                Arc::new(BoundedHnswProbeObserver { probe }),
                                hnsw_search,
                            )
                        } else {
                            hnsw_search()
                        }?;
                        #[cfg(not(feature = "test-seams"))]
                        let searched = hnsw_search()?;
                        // No graph yet is not the same as no graph to be had.
                        // A store whose index has grown past the point where a
                        // graph is worth building has one built the first time
                        // a search asks for it, and until this door asked, only
                        // the other one ever did -- so a store read only
                        // through here scored every row of every search
                        // forever, and reported that it had. The store decides
                        // and the store builds; this asks once and looks again.
                        match searched {
                            Some(searched) => Some(searched),
                            None if db.bounded_ensure_hnsw_built(&self.index, self.snapshot) => {
                                #[cfg(feature = "test-seams")]
                                let rebuilt = if let Some(probe) = context.probe() {
                                    contextdb_vector::hnsw::with_hnsw_candidate_observer(
                                        Arc::new(BoundedHnswProbeObserver { probe }),
                                        hnsw_search,
                                    )
                                } else {
                                    hnsw_search()
                                }?;
                                #[cfg(not(feature = "test-seams"))]
                                let rebuilt = hnsw_search()?;
                                rebuilt
                            }
                            None => None,
                        }
                    };
                    if let Some((rows, retained_bytes)) = hnsw {
                        self.used_hnsw = true;
                        self.score_bytes = retained_bytes;
                        self.scores = rows;
                        self.preparation = VectorPreparation::Materialize;
                        continue;
                    }
                    self.scores = Vec::new();
                    self.score_bytes = 0;
                    let cursor = db.bounded_brute_force_vector_cursor(
                        self.index.clone(),
                        &self.query,
                        self.candidate_k,
                        candidates,
                        self.snapshot,
                        &mut || context.charge_operator(BoundedWorkSource::VectorCandidates),
                        &mut |bytes| context.reserve(BoundedWorkSource::VectorCandidates, bytes),
                        &mut |bytes| context.release_parked(bytes),
                    )?;
                    self.source = Some(VectorCandidateSource::Brute(cursor));
                    self.preparation = VectorPreparation::Materialize;
                    continue;
                }
                VectorPreparation::Materialize if self.source.is_some() => {
                    let step = match self.source.as_mut() {
                        Some(VectorCandidateSource::Brute(cursor)) => db
                            .bounded_brute_force_vector_step(
                                cursor,
                                &mut || {
                                    context.charge(
                                        BoundedWorkSource::VectorCandidates,
                                        BoundedSourceTouch::BruteForceVectorCandidate,
                                    )
                                },
                                &mut || {
                                    context.charge_operator(BoundedWorkSource::VectorCandidates)
                                },
                                &mut |bytes| {
                                    context.reserve(BoundedWorkSource::VectorCandidates, bytes)
                                },
                                &mut |bytes| context.release_parked(bytes),
                            ),
                        None => {
                            return Err(Error::Other(
                                "bounded vector lost its candidate source".to_string(),
                            )
                            .into());
                        }
                    };
                    // An abandoned continuation keeps its charge, so a refusal
                    // or cancellation from the step disposes of it first.
                    let step = match step {
                        Ok(step) => step,
                        Err(error) => {
                            if let Some(VectorCandidateSource::Brute(cursor)) = self.source.take() {
                                context.release_parked(cursor.into_retained_bytes());
                            }
                            return Err(error);
                        }
                    };
                    match step {
                        contextdb_vector::mem::BoundedVectorStep::Pending => continue,
                        contextdb_vector::mem::BoundedVectorStep::Row(row_id, score) => {
                            let Some(row_id) = self.overlay_publishes(row_id) else {
                                continue;
                            };
                            self.keep_score(context, row_id, score)?;
                            continue;
                        }
                        contextdb_vector::mem::BoundedVectorStep::Exhausted => {
                            let Some(VectorCandidateSource::Brute(cursor)) = self.source.take()
                            else {
                                return Err(Error::Other(
                                    "bounded vector lost its exhausted brute-force cursor"
                                        .to_string(),
                                )
                                .into());
                            };
                            context.release(cursor.into_retained_bytes())?;
                            // The committed entries are done. The vectors this
                            // transaction staged are part of its own answer and
                            // come next, each charged the way a committed entry
                            // is.
                            self.score_staged_vectors(db, context)?;
                            continue;
                        }
                    }
                }
                VectorPreparation::Materialize => {
                    // The search already knows WHICH rows it wants, so it
                    // asks for those rows. Walking the table to find them
                    // costs what the table has ever been written -- every
                    // superseded version of every row, inspected and thrown
                    // away, for each row published -- so a store that has been
                    // updated for a while answers the same query more and more
                    // slowly for no reason visible in its data.
                    let Some((row_id, vector_score)) = self.scores.get(self.materialized).copied()
                    else {
                        self.finish_materialization(context)?;
                        self.preparation = VectorPreparation::Output;
                        continue;
                    };
                    self.materialized = self.materialized.checked_add(1).ok_or_else(|| {
                        Error::Other("bounded vector materialization overflow".to_string())
                    })?;
                    let Some(row) = self.materialize_scored_row(db, context, row_id)? else {
                        continue;
                    };
                    // From here the row's retained bytes are held by a guard,
                    // so no exit out of this block leaves them charged.
                    let mut row = RetainedRow::new(context, row);
                    let rank = if let Some(sort_key) = self.sort_key.as_deref() {
                        let schema_columns = self.schema_columns.clone();
                        let anchor_capacity = schema_columns.len();
                        let mut anchor_bytes = checked_size_add(
                            std::mem::size_of::<HashMap<String, Value>>(),
                            checked_size_mul(
                                anchor_capacity,
                                std::mem::size_of::<(String, Value)>(),
                                "rank anchor map",
                            )?,
                            "rank anchor map",
                        )?;
                        for (column, value) in schema_columns.iter().zip(row.values.iter().skip(1))
                        {
                            anchor_bytes = checked_size_add(
                                anchor_bytes,
                                checked_size_add(
                                    column.len(),
                                    value_capacity_bytes(value)?,
                                    "rank anchor",
                                )?,
                                "rank anchor",
                            )?;
                        }
                        context.reserve(BoundedWorkSource::RankCandidates, anchor_bytes)?;
                        let mut anchor = HashMap::new();
                        if anchor.try_reserve(anchor_capacity).is_err() {
                            context.release(anchor_bytes)?;
                            row.release()?;
                            return Err(Error::Other(
                                "bounded rank-anchor allocation failed".to_string(),
                            )
                            .into());
                        }
                        anchor.extend(
                            schema_columns
                                .iter()
                                .cloned()
                                .zip(row.values.iter().skip(1).cloned()),
                        );
                        let actual_anchor_bytes = anchor.iter().try_fold(
                            checked_size_add(
                                std::mem::size_of::<HashMap<String, Value>>(),
                                checked_size_mul(
                                    anchor.capacity(),
                                    std::mem::size_of::<(String, Value)>(),
                                    "rank anchor map",
                                )?,
                                "rank anchor map",
                            )?,
                            |bytes, (column, value)| {
                                checked_size_add(
                                    bytes,
                                    checked_size_add(
                                        column.capacity(),
                                        value_capacity_bytes(value)?,
                                        "rank anchor",
                                    )?,
                                    "rank anchor",
                                )
                            },
                        )?;
                        if let Err(error) = reconcile_new_reservation(
                            context,
                            BoundedWorkSource::RankCandidates,
                            anchor_bytes,
                            actual_anchor_bytes,
                        ) {
                            row.release()?;
                            return Err(error);
                        }
                        let ranked = db.bounded_rank_candidate(
                            &self.index,
                            sort_key,
                            row_id,
                            &anchor,
                            vector_score,
                            self.snapshot,
                            context.transaction(),
                            &mut || {
                                context.charge(
                                    BoundedWorkSource::RankCandidates,
                                    BoundedSourceTouch::RankCandidate,
                                )?;
                                context.attribute_live_memory(BoundedWorkSource::RankCandidates);
                                Ok(())
                            },
                            &mut |_bytes| {
                                context.charge(
                                    BoundedWorkSource::AccessControl,
                                    BoundedSourceTouch::AccessRow,
                                )?;
                                context.attribute_live_memory(BoundedWorkSource::AccessControl);
                                Ok(())
                            },
                            &mut |bytes| context.reserve(BoundedWorkSource::RankCandidates, bytes),
                            &mut |bytes| context.release(bytes),
                        );
                        context.release(actual_anchor_bytes)?;
                        let (rank, joined_bytes) = ranked?;
                        context.release(joined_bytes)?;
                        let Some(rank) = rank else {
                            row.release()?;
                            continue;
                        };
                        rank
                    } else {
                        vector_score
                    };
                    let score_value = Value::Float64(rank as f64);
                    let old_capacity = row.values.capacity();
                    let planned_slot_growth = if row.values.len() == old_capacity {
                        std::mem::size_of::<Value>()
                    } else {
                        0
                    };
                    let reserve_bytes = checked_size_add(
                        planned_slot_growth,
                        value_capacity_bytes(&score_value)?,
                        "vector score",
                    )?;
                    context.reserve(BoundedWorkSource::VectorCandidates, reserve_bytes)?;
                    if row.values.try_reserve_exact(1).is_err() {
                        context.release(reserve_bytes)?;
                        row.release()?;
                        return Err(Error::Other(
                            "bounded vector output allocation failed".to_string(),
                        )
                        .into());
                    }
                    let new_capacity = row.values.capacity();
                    let slot_growth = checked_size_mul(
                        new_capacity
                            .checked_sub(old_capacity)
                            .ok_or_else(|| bounded_size_error("vector output capacity"))?,
                        std::mem::size_of::<Value>(),
                        "vector output capacity",
                    )?;
                    let actual_growth = checked_size_add(
                        slot_growth,
                        value_capacity_bytes(&score_value)?,
                        "vector score",
                    )?;
                    if actual_growth > reserve_bytes {
                        context.reserve(
                            BoundedWorkSource::VectorCandidates,
                            actual_growth - reserve_bytes,
                        )?;
                    } else if reserve_bytes > actual_growth {
                        context.release(reserve_bytes - actual_growth)?;
                    }
                    row.values.push(score_value);
                    row.retained_bytes = row
                        .retained_bytes
                        .checked_add(actual_growth)
                        .ok_or_else(|| bounded_size_error("vector output row"))?;
                    let added = reserve_vector_slot(
                        context,
                        BoundedWorkSource::RankCandidates,
                        &mut self.ranked_rows,
                        "ranked vector rows",
                    )?;
                    self.ranked_container_bytes = self
                        .ranked_container_bytes
                        .checked_add(added)
                        .ok_or_else(|| bounded_size_error("ranked vector rows"))?;
                    self.ranked_rows.push(RankedVectorRow {
                        row_id,
                        rank,
                        vector_score,
                        row: row.into_row(),
                    });
                    continue;
                }
                VectorPreparation::Output => {
                    if let Some(row) = self.output.pop_front() {
                        return Ok(Some(row));
                    }
                    context.release(self.output_container_bytes)?;
                    self.output_container_bytes = 0;
                    self.preparation = VectorPreparation::Exhausted;
                    continue;
                }
                VectorPreparation::Exhausted => return Ok(None),
            }
        }
    }

    fn finish_materialization(
        &mut self,
        context: &Arc<RequestContext>,
    ) -> std::result::Result<(), BoundedExecutionError> {
        if self.sort_key.is_some() {
            self.ranked_rows.sort_by(|left, right| {
                bounded_rank_float_desc(left.rank, right.rank)
                    .then_with(|| bounded_rank_float_desc(left.vector_score, right.vector_score))
                    .then_with(|| right.row_id.cmp(&left.row_id))
            });
        } else {
            self.ranked_rows.sort_by(|left, right| {
                right
                    .vector_score
                    .total_cmp(&left.vector_score)
                    .then_with(|| left.row_id.cmp(&right.row_id))
            });
        }
        while self.ranked_rows.len() > self.k {
            let removed = self.ranked_rows.pop().ok_or_else(|| {
                Error::Other("bounded vector truncation lost its tail row".to_string())
            })?;
            removed.row.release(context)?;
        }
        let planned_output_bytes = checked_size_mul(
            self.ranked_rows.len(),
            std::mem::size_of::<PulledRow>(),
            "vector output queue",
        )?;
        context.reserve(BoundedWorkSource::VectorCandidates, planned_output_bytes)?;
        let mut output = VecDeque::new();
        if output.try_reserve_exact(self.ranked_rows.len()).is_err() {
            context.release(planned_output_bytes)?;
            return Err(
                Error::Other("bounded vector output-queue allocation failed".to_string()).into(),
            );
        }
        let output_bytes = checked_size_mul(
            output.capacity(),
            std::mem::size_of::<PulledRow>(),
            "vector output queue",
        )?;
        reconcile_new_reservation(
            context,
            BoundedWorkSource::VectorCandidates,
            planned_output_bytes,
            output_bytes,
        )?;
        for ranked in self.ranked_rows.drain(..) {
            output.push_back(ranked.row);
        }
        self.ranked_rows = Vec::new();
        context.release(self.ranked_container_bytes)?;
        self.ranked_container_bytes = 0;
        self.output = output;
        self.output_container_bytes = output_bytes;
        context.release(self.score_bytes)?;
        self.score_bytes = 0;
        self.scores = Vec::new();
        context.release(self.candidate_bytes)?;
        self.candidate_bytes = 0;
        self.candidate_ids = Vec::new();
        // The transaction's own vector work has been read and applied; what
        // remembering it cost goes back with the rest of the search state.
        self.vector_overlay = None;
        context.release(self.overlay_bytes)?;
        self.overlay_bytes = 0;
        // The store gets its share back at the same moment the request does.
        self._overlay_reservation = None;
        Ok(())
    }

    fn has_more_hint(&self) -> Option<bool> {
        match self.preparation {
            VectorPreparation::Output if !self.output.is_empty() => Some(true),
            VectorPreparation::Exhausted => Some(false),
            _ => None,
        }
    }
}

fn bounded_rank_float_desc(left: f32, right: f32) -> Ordering {
    match (left.is_nan(), right.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (false, false) => right.total_cmp(&left),
    }
}

impl ProjectState {
    fn next(
        &mut self,
        db: &dyn ReadExecutionTarget,
        context: &Arc<RequestContext>,
    ) -> std::result::Result<Option<PulledRow>, BoundedExecutionError> {
        if self.aggregate {
            if self.aggregate_done {
                return Ok(None);
            }
            if self.aggregates.is_none() {
                let state_bytes = checked_size_add(
                    std::mem::size_of::<Vec<AggregateState>>(),
                    checked_size_mul(
                        self.columns.len(),
                        std::mem::size_of::<AggregateState>(),
                        "aggregate accumulators",
                    )?,
                    "aggregate accumulators",
                )?;
                context.reserve(BoundedWorkSource::TableScan, state_bytes)?;
                let mut running = Vec::new();
                if running.try_reserve_exact(self.columns.len()).is_err() {
                    context.release(state_bytes)?;
                    return Err(Error::Other(
                        "bounded aggregate accumulator allocation failed".to_string(),
                    )
                    .into());
                }
                for column in &self.columns {
                    let Some(aggregate) = column_aggregate(column) else {
                        context.release(state_bytes)?;
                        return Err(mixed_aggregate_error().into());
                    };
                    running.push(AggregateState::new(aggregate));
                }
                self.aggregates = Some(running);
                self.aggregate_state_bytes = state_bytes;
            }

            loop {
                // This fetch has spent the work it was given. The running
                // answer and the row already pulled stay exactly where they
                // are, and the page goes back with no row on it -- a cursor
                // finishes the aggregate on the next fetch rather than being
                // refused for an input larger than one page may pay for.
                if context.fetch_budget_spent() {
                    return Ok(None);
                }
                if self.pending.is_none() {
                    match self.input.next(db, context) {
                        Ok(pulled) => self.pending = pulled,
                        // The source reached the ceiling part-way through
                        // this pull -- a filter walks several rows to find
                        // one. Same answer as above: the fetch stops here,
                        // charged for exactly what it examined, and the next
                        // one carries on from the row the source has not
                        // touched yet.
                        Err(error) if context.refusal_pauses_this_fetch(&error) => {
                            return Ok(None);
                        }
                        Err(error) => return Err(error),
                    }
                }
                let Some(row) = self.pending.as_ref() else {
                    let running = self.aggregates.as_ref().ok_or_else(|| {
                        Error::Other("bounded aggregate lost its accumulators".to_string())
                    })?;
                    let planned_output_bytes = checked_size_add(
                        std::mem::size_of::<Vec<Value>>(),
                        checked_size_mul(
                            running.len(),
                            std::mem::size_of::<Value>(),
                            "aggregate output",
                        )?,
                        "aggregate output",
                    )?;
                    context.reserve(BoundedWorkSource::TableScan, planned_output_bytes)?;
                    let mut output = Vec::new();
                    if output.try_reserve_exact(running.len()).is_err() {
                        context.release(planned_output_bytes)?;
                        return Err(Error::Other(
                            "bounded aggregate output allocation failed".to_string(),
                        )
                        .into());
                    }
                    let mut output_bytes = match values_retained_bytes(&output) {
                        Ok(bytes) => bytes,
                        Err(error) => {
                            context.release(planned_output_bytes)?;
                            return Err(error);
                        }
                    };
                    reconcile_new_reservation(
                        context,
                        BoundedWorkSource::TableScan,
                        planned_output_bytes,
                        output_bytes,
                    )?;
                    for state in running {
                        match state.finish() {
                            Ok(value) => output.push(value),
                            Err(error) => {
                                context.release(output_bytes)?;
                                return Err(error.into());
                            }
                        }
                    }
                    let final_output_bytes = match values_retained_bytes(&output) {
                        Ok(bytes) => bytes,
                        Err(error) => {
                            context.release(output_bytes)?;
                            return Err(error);
                        }
                    };
                    reconcile_new_reservation(
                        context,
                        BoundedWorkSource::TableScan,
                        output_bytes,
                        final_output_bytes,
                    )?;
                    output_bytes = final_output_bytes;
                    self.aggregates = None;
                    context.release(self.aggregate_state_bytes)?;
                    self.aggregate_state_bytes = 0;
                    self.aggregate_done = true;
                    return Ok(Some(PulledRow {
                        values: output,
                        retained_bytes: output_bytes,
                    }));
                };

                // Folding a row the source has already been charged for is
                // not a second look at it: `work` is what a read EXAMINES, so
                // billing the fold too would make a declared ceiling of a
                // thousand admit five hundred rows. The boundary the charge
                // used to carry -- cancellation and the active-time ceiling --
                // is still checked here, on every row.
                context.check_final_boundary()?;
                let arguments = self.columns.iter().filter_map(|column| {
                    let Expr::FunctionCall { args, .. } = &column.expr else {
                        return None;
                    };
                    match aggregate_input(args) {
                        AggregateInput::Value(argument) => Some(argument),
                        AggregateInput::Row | AggregateInput::Nothing => None,
                    }
                });
                let temporary_bytes =
                    expression_temporary_bytes(&row.values, &self.params, arguments)?;
                context.reserve(BoundedWorkSource::TableScan, temporary_bytes)?;
                let folded = (|| {
                    let running = self.aggregates.as_mut().ok_or_else(|| {
                        Error::Other("bounded aggregate lost its accumulators".to_string())
                    })?;
                    for (column, state) in self.columns.iter().zip(running.iter_mut()) {
                        let Expr::FunctionCall { args, .. } = &column.expr else {
                            return Err(mixed_aggregate_error());
                        };
                        match aggregate_input(args) {
                            AggregateInput::Row => state.admit(AggregateItem::Row)?,
                            AggregateInput::Nothing => state.admit(AggregateItem::Nothing)?,
                            AggregateInput::Value(argument) => {
                                let value = eval_query_result_expr(
                                    argument,
                                    &row.values,
                                    &self.input.columns,
                                    &self.params,
                                )?;
                                state.admit(AggregateItem::Value(&value))?;
                            }
                        }
                    }
                    Ok(())
                })();
                context.release(temporary_bytes)?;
                match folded {
                    Ok(()) => {}
                    Err(error) => {
                        let row = self.pending.take().ok_or_else(|| {
                            Error::Other("bounded aggregate lost its input row".to_string())
                        })?;
                        row.release(context)?;
                        return Err(error.into());
                    }
                }
                let row = self.pending.take().ok_or_else(|| {
                    Error::Other("bounded aggregate lost its input row".to_string())
                })?;
                row.release(context)?;
            }
        }

        if self.pending.is_none() {
            self.pending = self.input.next(db, context)?;
        }
        let Some(row) = self.pending.as_ref() else {
            return Ok(None);
        };
        let temporary_bytes = expression_temporary_bytes(
            &row.values,
            &self.params,
            self.columns.iter().map(|column| &column.expr),
        )?;
        context.reserve(BoundedWorkSource::TableScan, temporary_bytes)?;
        let output = self
            .columns
            .iter()
            .map(|column| {
                eval_project_expr(&column.expr, &row.values, &self.input.columns, &self.params)
            })
            .collect::<Result<Vec<_>>>();
        let output = match output {
            Ok(output) => output,
            Err(error) => {
                context.release(temporary_bytes)?;
                let row = self.pending.take().ok_or_else(|| {
                    Error::Other("bounded projection lost its input row".to_string())
                })?;
                row.release(context)?;
                return Err(error.into());
            }
        };
        // The headroom above was reserved FOR this output; the output is what
        // it turned into, not something held beside it. Reserving the output
        // on top and only then releasing the headroom charged the read for
        // both at once -- a row's worth of double-count at the moment a
        // projection is at its widest, which is exactly when a memory ceiling
        // is decided.
        let output_bytes = values_retained_bytes(&output)?;
        reconcile_new_reservation(
            context,
            BoundedWorkSource::TableScan,
            temporary_bytes,
            output_bytes,
        )?;
        let input = self
            .pending
            .take()
            .ok_or_else(|| Error::Other("bounded projection lost its input row".to_string()))?;
        input.release(context)?;
        Ok(Some(PulledRow {
            values: output,
            retained_bytes: output_bytes,
        }))
    }
}

impl SortState {
    fn next(
        &mut self,
        db: &dyn ReadExecutionTarget,
        context: &Arc<RequestContext>,
    ) -> std::result::Result<Option<PulledRow>, BoundedExecutionError> {
        if !self.prepared {
            loop {
                if self.pending.is_none() {
                    self.pending = self.input.next(db, context)?;
                }
                let Some(row) = self.pending.as_ref() else {
                    let sort_scratch_bytes = checked_size_mul(
                        self.rows.len(),
                        std::mem::size_of::<SortEntry>(),
                        "sort scratch",
                    )?;
                    context.reserve(BoundedWorkSource::UnindexedSort, sort_scratch_bytes)?;
                    self.rows.sort_by(|left, right| {
                        for (index, key) in self.keys.iter().enumerate() {
                            let ordering = compare_sort_values(
                                &left.keys[index],
                                &right.keys[index],
                                key.direction,
                            );
                            if ordering != Ordering::Equal {
                                return ordering;
                            }
                        }
                        Ordering::Equal
                    });
                    self.rows.reverse();
                    context.release(sort_scratch_bytes)?;
                    self.prepared = true;
                    break;
                };
                context.charge(
                    BoundedWorkSource::UnindexedSort,
                    BoundedSourceTouch::SortCandidate,
                )?;
                let temporary_bytes = expression_temporary_bytes(
                    &row.values,
                    &self.params,
                    self.keys.iter().map(|key| &key.expr),
                )?;
                context.reserve(BoundedWorkSource::UnindexedSort, temporary_bytes)?;
                let values = self
                    .keys
                    .iter()
                    .map(|key| {
                        eval_query_result_expr(
                            &key.expr,
                            &row.values,
                            &self.input.columns,
                            &self.params,
                        )
                    })
                    .collect::<Result<Vec<_>>>();
                let values = match values {
                    Ok(values) => values,
                    Err(error) => {
                        context.release(temporary_bytes)?;
                        let row = self.pending.take().ok_or_else(|| {
                            Error::Other("bounded sort lost its candidate".to_string())
                        })?;
                        row.release(context)?;
                        return Err(error.into());
                    }
                };
                let key_bytes = checked_size_add(
                    values_retained_bytes(&values)?,
                    std::mem::size_of::<SortEntry>(),
                    "sort candidate",
                )?;
                if let Err(error) = context.reserve(BoundedWorkSource::UnindexedSort, key_bytes) {
                    context.release(temporary_bytes)?;
                    return Err(error);
                }
                context.release(temporary_bytes)?;
                let row = self.pending.take().ok_or_else(|| {
                    Error::Other("bounded sort lost its admitted candidate".to_string())
                })?;
                self.buffered_bytes = self
                    .buffered_bytes
                    .checked_add(key_bytes)
                    .and_then(|held| held.checked_add(row.retained_bytes))
                    .ok_or_else(|| bounded_size_error("sort buffer"))?;
                self.rows.push(SortEntry {
                    keys: values,
                    key_bytes,
                    row,
                });
            }
        }
        if self.rows.is_empty() {
            return Ok(None);
        }
        // Handing back a row buffered by an earlier page is work this read is
        // doing now. Charging it is what lets a resumed page be stopped by its
        // work ceiling and interrupted by its caller, exactly like every other
        // operator that emits from a buffer it filled earlier.
        context.charge_operator(BoundedWorkSource::UnindexedSort)?;
        let Some(entry) = self.rows.pop() else {
            return Ok(None);
        };
        self.buffered_bytes = self
            .buffered_bytes
            .saturating_sub(entry.key_bytes)
            .saturating_sub(entry.row.retained_bytes);
        context.release(entry.key_bytes)?;
        Ok(Some(entry.row))
    }
}

impl LimitState {
    fn next(
        &mut self,
        db: &dyn ReadExecutionTarget,
        context: &Arc<RequestContext>,
    ) -> std::result::Result<Option<PulledRow>, BoundedExecutionError> {
        if self.emitted >= self.count {
            return Ok(None);
        }
        let row = self.input.next(db, context)?;
        if row.is_some() {
            self.emitted = self
                .emitted
                .checked_add(1)
                .ok_or_else(|| Error::Other("bounded LIMIT counter overflow".to_string()))?;
        }
        Ok(row)
    }
}

impl FilterState {
    fn next(
        &mut self,
        db: &dyn ReadExecutionTarget,
        context: &Arc<RequestContext>,
    ) -> std::result::Result<Option<PulledRow>, BoundedExecutionError> {
        loop {
            if self.pending.is_none() {
                self.pending = self.input.next(db, context)?;
            }
            let Some(row) = self.pending.as_ref() else {
                return Ok(None);
            };
            let temporary_bytes = expression_temporary_bytes(
                &row.values,
                &self.params,
                std::iter::once(&self.predicate),
            )?;
            context.reserve(BoundedWorkSource::TableScan, temporary_bytes)?;
            let matches = query_result_row_matches(
                &row.values,
                &self.input.columns,
                &self.predicate,
                &self.params,
            );
            context.release(temporary_bytes)?;
            let matches = match matches {
                Ok(matches) => matches,
                Err(error) => {
                    let row = self.pending.take().ok_or_else(|| {
                        Error::Other("bounded filter lost its input row".to_string())
                    })?;
                    row.release(context)?;
                    return Err(error.into());
                }
            };
            if matches {
                return self.pending.take().map(Some).ok_or_else(|| {
                    Error::Other("bounded filter lost its match".to_string()).into()
                });
            }
            let row = self
                .pending
                .take()
                .ok_or_else(|| Error::Other("bounded filter lost its rejected row".to_string()))?;
            row.release(context)?;
        }
    }
}

impl DistinctState {
    fn next(
        &mut self,
        db: &dyn ReadExecutionTarget,
        context: &Arc<RequestContext>,
    ) -> std::result::Result<Option<PulledRow>, BoundedExecutionError> {
        loop {
            if self.pending.is_none() {
                self.pending = self.input.next(db, context)?;
            }
            let Some(row) = self.pending.as_ref() else {
                context.release(self.seen_bytes)?;
                self.seen_bytes = 0;
                self.seen.clear();
                return Ok(None);
            };
            context.charge_operator(BoundedWorkSource::UnindexedSort)?;
            let encoded_bytes = serialized_size(&row.values)?;
            let retained = checked_size_add(
                encoded_bytes,
                checked_size_add(
                    std::mem::size_of::<Vec<u8>>(),
                    checked_size_mul(2, std::mem::size_of::<usize>(), "DISTINCT key")?,
                    "DISTINCT key",
                )?,
                "DISTINCT key",
            )?;
            context.reserve(BoundedWorkSource::UnindexedSort, retained)?;
            let key = match encoded_row_key(&row.values, encoded_bytes) {
                Ok(key) => key,
                Err(error) => {
                    context.release(retained)?;
                    let row = self.pending.take().ok_or_else(|| {
                        Error::Other("bounded DISTINCT lost its input row".to_string())
                    })?;
                    row.release(context)?;
                    return Err(error);
                }
            };
            if self.seen.contains(&key) {
                context.release(retained)?;
                let row = self.pending.take().ok_or_else(|| {
                    Error::Other("bounded DISTINCT lost its duplicate".to_string())
                })?;
                row.release(context)?;
                continue;
            }
            self.seen.insert(key);
            self.seen_bytes = self
                .seen_bytes
                .checked_add(retained)
                .ok_or_else(|| bounded_size_error("DISTINCT state"))?;
            return self
                .pending
                .take()
                .map(Some)
                .ok_or_else(|| Error::Other("bounded DISTINCT lost its row".to_string()).into());
        }
    }
}

impl JoinState {
    fn next(
        &mut self,
        db: &dyn ReadExecutionTarget,
        context: &Arc<RequestContext>,
    ) -> std::result::Result<Option<PulledRow>, BoundedExecutionError> {
        if !self.right_prepared {
            loop {
                if self.right_pending.is_none() {
                    self.right_pending = self.right.next(db, context)?;
                }
                let Some(_) = self.right_pending.as_ref() else {
                    self.right_prepared = true;
                    break;
                };
                let entry_bytes = std::mem::size_of::<PulledRow>();
                context.reserve(BoundedWorkSource::UnindexedSort, entry_bytes)?;
                let mut row = self
                    .right_pending
                    .take()
                    .ok_or_else(|| Error::Other("bounded join lost its right input".to_string()))?;
                row.retained_bytes = row
                    .retained_bytes
                    .checked_add(entry_bytes)
                    .ok_or_else(|| bounded_size_error("join right buffer"))?;
                self.right_rows.push(row);
            }
        }

        loop {
            if self.left_row.is_none() {
                let Some(row) = self.left.next(db, context)? else {
                    for row in self.right_rows.drain(..) {
                        row.release(context)?;
                    }
                    return Ok(None);
                };
                self.left_row = Some(row);
                self.right_position = 0;
                self.matched = false;
            }

            while self.right_position < self.right_rows.len() {
                context.charge_operator(BoundedWorkSource::UnindexedSort)?;
                let right = self.right_rows.get(self.right_position).ok_or_else(|| {
                    Error::Other("bounded join lost its admitted right pair".to_string())
                })?;
                let left = self
                    .left_row
                    .as_ref()
                    .ok_or_else(|| Error::Other("bounded join lost its left input".to_string()))?;
                let output_bytes = value_refs_retained_bytes(
                    left.values.iter().chain(right.values.iter()),
                    left.values
                        .len()
                        .checked_add(right.values.len())
                        .ok_or_else(|| bounded_size_error("join output"))?,
                )?;
                context.reserve(BoundedWorkSource::UnindexedSort, output_bytes)?;
                let combined = concatenate_rows(&left.values, &right.values);
                let temporary_bytes = expression_temporary_bytes(
                    &combined,
                    &self.params,
                    std::iter::once(&self.condition),
                )?;
                if let Err(error) =
                    context.reserve(BoundedWorkSource::UnindexedSort, temporary_bytes)
                {
                    context.release(output_bytes)?;
                    return Err(error);
                }
                let matches = query_result_row_matches(
                    &combined,
                    &self.condition_columns,
                    &self.condition,
                    &self.params,
                );
                context.release(temporary_bytes)?;
                let matches = match matches {
                    Ok(matches) => matches,
                    Err(error) => {
                        context.release(output_bytes)?;
                        return Err(error.into());
                    }
                };
                self.right_position = self
                    .right_position
                    .checked_add(1)
                    .ok_or_else(|| Error::Other("bounded join position overflow".to_string()))?;
                if matches {
                    self.matched = true;
                    return Ok(Some(PulledRow {
                        values: combined,
                        retained_bytes: output_bytes,
                    }));
                }
                context.release(output_bytes)?;
            }

            if !self.matched && matches!(self.join_type, contextdb_planner::JoinType::Left) {
                let left = self.left_row.as_mut().ok_or_else(|| {
                    Error::Other("bounded join lost its unmatched row".to_string())
                })?;
                let old_capacity = left.values.capacity();
                let required_len = left
                    .values
                    .len()
                    .checked_add(self.right_column_count)
                    .ok_or_else(|| bounded_size_error("left join NULL extension"))?;
                let planned_slots = if required_len > old_capacity {
                    required_len
                        .checked_sub(old_capacity)
                        .ok_or_else(|| bounded_size_error("left join NULL extension"))?
                } else {
                    0
                };
                let planned_growth = checked_size_mul(
                    planned_slots,
                    std::mem::size_of::<Value>(),
                    "left join NULL extension",
                )?;
                context.reserve(BoundedWorkSource::UnindexedSort, planned_growth)?;
                if left
                    .values
                    .try_reserve_exact(self.right_column_count)
                    .is_err()
                {
                    context.release(planned_growth)?;
                    return Err(Error::Other(
                        "bounded left join NULL extension allocation failed".to_string(),
                    )
                    .into());
                }
                let actual_slots = match left.values.capacity().checked_sub(old_capacity) {
                    Some(slots) => slots,
                    None => {
                        context.release(planned_growth)?;
                        return Err(Error::Other(
                            "bounded left join capacity moved backwards".to_string(),
                        )
                        .into());
                    }
                };
                let actual_growth = match checked_size_mul(
                    actual_slots,
                    std::mem::size_of::<Value>(),
                    "left join NULL extension",
                ) {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        context.release(planned_growth)?;
                        return Err(error);
                    }
                };
                let retained_bytes = match left.retained_bytes.checked_add(actual_growth) {
                    Some(bytes) => bytes,
                    None => {
                        context.release(planned_growth)?;
                        return Err(bounded_size_error("left join output"));
                    }
                };
                reconcile_new_reservation(
                    context,
                    BoundedWorkSource::UnindexedSort,
                    planned_growth,
                    actual_growth,
                )?;
                left.values
                    .extend(std::iter::repeat_n(Value::Null, self.right_column_count));
                left.retained_bytes = retained_bytes;
                return Ok(self.left_row.take());
            }
            let left = self
                .left_row
                .take()
                .ok_or_else(|| Error::Other("bounded join lost its completed row".to_string()))?;
            left.release(context)?;
        }
    }
}

impl UnionState {
    fn next(
        &mut self,
        db: &dyn ReadExecutionTarget,
        context: &Arc<RequestContext>,
    ) -> std::result::Result<Option<PulledRow>, BoundedExecutionError> {
        loop {
            if self.pending.is_none() {
                let Some(input) = self.inputs.get_mut(self.input) else {
                    context.release(self.seen_bytes)?;
                    self.seen_bytes = 0;
                    self.seen.clear();
                    return Ok(None);
                };
                self.pending = input.next(db, context)?;
                if self.pending.is_none() {
                    self.input = self.input.checked_add(1).ok_or_else(|| {
                        Error::Other("bounded UNION input position overflow".to_string())
                    })?;
                    continue;
                }
            }
            context.charge_operator(BoundedWorkSource::UnindexedSort)?;
            if self.all {
                return self
                    .pending
                    .take()
                    .map(Some)
                    .ok_or_else(|| Error::Other("bounded UNION lost its row".to_string()).into());
            }
            let row = self
                .pending
                .as_ref()
                .ok_or_else(|| Error::Other("bounded UNION lost its pending row".to_string()))?;
            let encoded_bytes = serialized_size(&row.values)?;
            let retained = checked_size_add(
                encoded_bytes,
                checked_size_add(
                    std::mem::size_of::<Vec<u8>>(),
                    checked_size_mul(2, std::mem::size_of::<usize>(), "UNION key")?,
                    "UNION key",
                )?,
                "UNION key",
            )?;
            context.reserve(BoundedWorkSource::UnindexedSort, retained)?;
            let key = match encoded_row_key(&row.values, encoded_bytes) {
                Ok(key) => key,
                Err(error) => {
                    context.release(retained)?;
                    let row = self.pending.take().ok_or_else(|| {
                        Error::Other("bounded UNION lost its input row".to_string())
                    })?;
                    row.release(context)?;
                    return Err(error);
                }
            };
            if self.seen.contains(&key) {
                context.release(retained)?;
                let row = self
                    .pending
                    .take()
                    .ok_or_else(|| Error::Other("bounded UNION lost its duplicate".to_string()))?;
                row.release(context)?;
                continue;
            }
            self.seen.insert(key);
            self.seen_bytes = self
                .seen_bytes
                .checked_add(retained)
                .ok_or_else(|| bounded_size_error("UNION state"))?;
            return self
                .pending
                .take()
                .map(Some)
                .ok_or_else(|| Error::Other("bounded UNION lost its row".to_string()).into());
        }
    }
}

impl StaticState {
    fn next(&mut self) -> std::result::Result<Option<PulledRow>, BoundedExecutionError> {
        Ok(self.rows.pop_front())
    }
}

fn ensure_supported_plan(plan: &PhysicalPlan) -> std::result::Result<(), BoundedExecutionError> {
    match plan {
        PhysicalPlan::Scan { .. } => Ok(()),
        PhysicalPlan::Project { input, .. }
        | PhysicalPlan::Limit { input, .. }
        | PhysicalPlan::Sort { input, .. }
        | PhysicalPlan::Filter { input, .. }
        | PhysicalPlan::Distinct { input }
        | PhysicalPlan::MaterializeCte { input, .. } => ensure_supported_plan(input),
        PhysicalPlan::Join { left, right, .. } => {
            ensure_supported_plan(left)?;
            ensure_supported_plan(right)
        }
        PhysicalPlan::Union { inputs, .. } => {
            for input in inputs {
                ensure_supported_plan(input)?;
            }
            Ok(())
        }
        PhysicalPlan::GraphBfs {
            start_candidates, ..
        } => start_candidates
            .as_deref()
            .map_or(Ok(()), ensure_supported_plan),
        PhysicalPlan::VectorSearch { candidates, .. }
        | PhysicalPlan::HnswSearch { candidates, .. } => {
            candidates.as_deref().map_or(Ok(()), ensure_supported_plan)
        }
        PhysicalPlan::IndexScan { .. } | PhysicalPlan::CteRef { .. } => Ok(()),
        // A store answering questions about itself: no rows read, nothing
        // changed, and classified a READ, so a reading session runs it.
        PhysicalPlan::ShowMemoryLimit
        | PhysicalPlan::ShowDiskLimit
        | PhysicalPlan::ShowSyncConflictPolicy
        | PhysicalPlan::ShowVectorIndexes => Ok(()),
        PhysicalPlan::Pipeline(plans) => {
            for plan in plans {
                ensure_supported_plan(plan)?;
            }
            Ok(())
        }
        _ => Err(
            Error::PlanError("bounded execution accepts read-only SELECT plans".to_string()).into(),
        ),
    }
}

/// Resolve `IN (subquery)` for a bounded read by reading the inner answer the
/// bounded way.
///
/// The inner query is a read like any other: it touches sources, it retains
/// rows, and a caller who set a ceiling meant it to cover the whole statement,
/// not just the outer half. Handing the inner half to the eager executor reads
/// it with no ceiling at all, keeps the whole result in memory uncharged, and
/// cannot notice a cancellation until it is over -- so a one-row answer over a
/// million-row subquery escapes both limits it was given.
///
/// Everything except the row reading is the executor's: the same correlation
/// refusal, the same planner, the same literal conversion, the same walk.
fn bounded_resolve_in_subqueries(
    db: &dyn ReadExecutionTarget,
    expr: &Expr,
    params: &Arc<HashMap<String, Value>>,
    snapshot: SnapshotId,
    ctes: &[Cte],
    context: &Arc<RequestContext>,
) -> std::result::Result<Expr, BoundedExecutionError> {
    let Some(database) = db.as_database_for_subquery_plan() else {
        // Only the in-process target can plan a subquery; any other target
        // never carries one, so there is nothing to resolve.
        return Ok(expr.clone());
    };
    let mut drain = |plan: &PhysicalPlan,
                     select: &Expr|
     -> std::result::Result<Vec<Value>, BoundedExecutionError> {
        let mut node = build_kernel(db, plan, Arc::clone(params), snapshot, context)?;
        {
            let named: std::collections::BTreeSet<String> = node.columns.iter().cloned().collect();
            declare_supplemented_columns(db, &mut node, &named);
        }
        let mut values = Vec::new();
        // Reserved before the value exists and released on every exit,
        // including the unwind, so a refusal mid-drain leaves the accountant
        // where it started.
        let mut retained = 0usize;
        let outcome = (|| -> std::result::Result<(), BoundedExecutionError> {
            while let Some(row) = pull(db, context, &mut node)? {
                let projected =
                    super::eval_project_expr(select, &row.values, &node.columns, params)?;
                // What the list really costs to hold: the payload, the slot it
                // sits in while it is a value, and the slot it sits in again
                // once it becomes a literal. Charging the payload alone leaves
                // a scalar id looking free, and a million free ids is how the
                // ceiling gets passed without ever being consulted.
                let bytes = planned_value_clone_bytes(&projected)?
                    .checked_add(std::mem::size_of::<Value>())
                    .and_then(|bytes| bytes.checked_add(std::mem::size_of::<Expr>()))
                    .ok_or_else(|| bounded_size_error("bounded subquery literal"))?;
                context.reserve(BoundedWorkSource::TableScan, bytes)?;
                retained = retained.saturating_add(bytes);
                values.push(projected);
                context.release(row.retained_bytes)?;
            }
            Ok(())
        })();
        if outcome.is_err() {
            let _ = context.release(retained);
        }
        outcome?;
        Ok(values)
    };
    super::resolve_in_subqueries_with_drain(database, expr, params.as_ref(), ctes, &mut drain)
}

/// Resolve a traversal's start against the store the bounded way.
///
/// Finding a start by something other than its id reads rows -- a whole node
/// table when no index covers the predicate. Reading them through the eager
/// helper puts an unbounded scan and an unbounded candidate set inside a read
/// that was given a ceiling, and the ceiling only learns about it afterwards.
///
/// Only the row reading moves here. Which filter names a start, which tables
/// can describe one, which predicates were pushed and the per-candidate
/// confirmation all stay in the executor's own resolver.
fn bounded_resolve_graph_start_nodes(
    db: &dyn ReadExecutionTarget,
    filter: &Expr,
    params: &Arc<HashMap<String, Value>>,
    snapshot: SnapshotId,
    start_alias: &str,
    context: &Arc<RequestContext>,
) -> std::result::Result<Option<super::GraphStartResolution>, BoundedExecutionError> {
    let Some(database) = db.as_database_for_subquery_plan() else {
        return Ok(None);
    };
    let tx = context.transaction();
    let mut read = |table: &str,
                    pick: Option<&IndexPick>|
     -> std::result::Result<(Vec<uuid::Uuid>, u64), BoundedExecutionError> {
        // The index the executor's own resolver picked for this table, read
        // the bounded way. Ignoring it walks every row of a node table to find
        // a start the index names in one run, charges the reader for all of
        // them, and reports a figure the other door never reports.
        let mut node = build_scan(
            db,
            table,
            None,
            Arc::clone(params),
            snapshot,
            None,
            pick.cloned(),
            context,
        )?;
        let id_column = node.columns.iter().position(|column| column == "id");
        let mut ids = Vec::new();
        let mut examined = 0u64;
        let mut retained = 0usize;
        let outcome = (|| -> std::result::Result<(), BoundedExecutionError> {
            while let Some(row) = pull(db, context, &mut node)? {
                examined = examined.saturating_add(1);
                if let Some(position) = id_column
                    && let Some(Value::Uuid(id)) = row.values.get(position)
                {
                    // Reserved before the id is kept, so a candidate set that
                    // outgrows the ceiling is refused while it is growing.
                    let bytes = std::mem::size_of::<uuid::Uuid>();
                    context.reserve(BoundedWorkSource::TableScan, bytes)?;
                    retained = retained.saturating_add(bytes);
                    ids.push(*id);
                }
                context.release(row.retained_bytes)?;
            }
            Ok(())
        })();
        if outcome.is_err() {
            let _ = context.release(retained);
        }
        outcome?;
        Ok((ids, examined))
    };
    super::resolve_graph_start_nodes_with_reader(
        database,
        filter,
        params.as_ref(),
        tx,
        snapshot,
        start_alias,
        &mut read,
    )
    .map(Some)
}

fn build_kernel(
    db: &dyn ReadExecutionTarget,
    plan: &PhysicalPlan,
    params: Arc<HashMap<String, Value>>,
    snapshot: SnapshotId,
    context: &Arc<RequestContext>,
) -> std::result::Result<PullNode, BoundedExecutionError> {
    build_kernel_with_ctes(db, plan, params, snapshot, &HashMap::new(), context)
}

fn build_kernel_with_ctes<'plan>(
    db: &dyn ReadExecutionTarget,
    plan: &'plan PhysicalPlan,
    params: Arc<HashMap<String, Value>>,
    snapshot: SnapshotId,
    ctes: &HashMap<&'plan str, &'plan PhysicalPlan>,
    context: &Arc<RequestContext>,
) -> std::result::Result<PullNode, BoundedExecutionError> {
    match plan {
        PhysicalPlan::Scan { table, filter, .. } => build_scan(
            db,
            table,
            filter.as_ref(),
            params,
            snapshot,
            None,
            None,
            context,
        ),
        // Read-classified statements about the store itself. They walk no
        // source, so the whole answer arrives at once -- and every row of it
        // is charged on the way in, so a store with a great many vector
        // indexes is bounded by the same ceilings as a store with a great
        // many rows.
        PhysicalPlan::ShowMemoryLimit
        | PhysicalPlan::ShowDiskLimit
        | PhysicalPlan::ShowSyncConflictPolicy
        | PhysicalPlan::ShowVectorIndexes => build_store_state(db, plan, context),
        PhysicalPlan::Project { input, columns } => {
            let input =
                build_kernel_with_ctes(db, input, Arc::clone(&params), snapshot, ctes, context)?;
            let trace = input.trace.clone();
            let aggregate = projection_aggregates(columns);
            if aggregate
                && columns
                    .iter()
                    .any(|column| column_aggregate(column).is_none())
            {
                return Err(mixed_aggregate_error().into());
            }
            Ok(PullNode {
                columns: project_output_columns(columns),
                trace,
                kind: PullKind::Project(ProjectState {
                    input: Box::new(input),
                    columns: columns.clone(),
                    params,
                    aggregate_done: false,
                    aggregate,
                    pending: None,
                    aggregates: None,
                    aggregate_state_bytes: 0,
                }),
            })
        }
        PhysicalPlan::Limit { input, count } => {
            let input = build_kernel_with_ctes(db, input, params, snapshot, ctes, context)?;
            Ok(PullNode {
                columns: input.columns.clone(),
                trace: input.trace.clone(),
                kind: PullKind::Limit(LimitState {
                    input: Box::new(input),
                    count: *count,
                    emitted: 0,
                }),
            })
        }
        PhysicalPlan::Sort { input, keys } => {
            // An ordered index run is the committed rows in order and nothing
            // else, so with a transaction open the rows it staged have to be
            // put where they belong. The scan does that as it walks, which is
            // why the run below is taken with a transaction open too; the
            // shortcut after it has no such fold, so it still stands down.
            // Asked once, so the two places below cannot disagree about
            // whether a transaction is open.
            let inside_transaction = context.transaction().is_some();
            if let Some((table, filter, column, direction, index, reverse)) =
                ordered_scan(db, input, keys)
            {
                return build_scan(
                    db,
                    table,
                    filter,
                    params,
                    snapshot,
                    Some((column, direction, index, reverse)),
                    None,
                    context,
                );
            }
            let built =
                build_kernel_with_ctes(db, input, Arc::clone(&params), snapshot, ctes, context)?;
            // The scan may already be reading one run of an index, and that
            // run may already be in the order asked for -- in which case there
            // is nothing to sort. Whether it is, is the executor's rule again.
            if !inside_transaction
                && built.trace.physical_plan == "IndexScan"
                && let Some(index) = built.trace.index_used.as_deref()
                && crate::executor::sort_keys_match_index_prefix(db, input, index, keys)
            {
                let mut built = built;
                built.trace.sort_elided = true;
                return Ok(built);
            }
            let input = built;
            Ok(PullNode {
                columns: input.columns.clone(),
                trace: input.trace.clone(),
                kind: PullKind::Sort(SortState {
                    input: Box::new(input),
                    keys: keys.clone(),
                    params,
                    rows: Vec::new(),
                    prepared: false,
                    pending: None,
                    buffered_bytes: 0,
                }),
            })
        }
        PhysicalPlan::Filter { input, predicate } => {
            let input =
                build_kernel_with_ctes(db, input, Arc::clone(&params), snapshot, ctes, context)?;
            Ok(PullNode {
                columns: input.columns.clone(),
                trace: input.trace.clone(),
                kind: PullKind::Filter(FilterState {
                    input: Box::new(input),
                    predicate: predicate.clone(),
                    params,
                    pending: None,
                }),
            })
        }
        PhysicalPlan::Distinct { input } | PhysicalPlan::MaterializeCte { input, .. } => {
            let input = build_kernel_with_ctes(db, input, params, snapshot, ctes, context)?;
            if matches!(plan, PhysicalPlan::MaterializeCte { .. }) {
                return Ok(input);
            }
            Ok(PullNode {
                columns: input.columns.clone(),
                trace: input.trace.clone(),
                kind: PullKind::Distinct(DistinctState {
                    input: Box::new(input),
                    seen: HashSet::new(),
                    seen_bytes: 0,
                    pending: None,
                }),
            })
        }
        PhysicalPlan::Union { inputs, all } => {
            let mut built = Vec::with_capacity(inputs.len());
            for input in inputs {
                built.push(build_kernel_with_ctes(
                    db,
                    input,
                    Arc::clone(&params),
                    snapshot,
                    ctes,
                    context,
                )?);
            }
            let columns = built
                .first()
                .map_or_else(Vec::new, |input| input.columns.clone());
            let trace = built
                .first()
                .map_or_else(QueryTrace::scan, |input| input.trace.clone());
            Ok(PullNode {
                columns,
                trace,
                kind: PullKind::Union(UnionState {
                    inputs: built,
                    input: 0,
                    all: *all,
                    seen: HashSet::new(),
                    seen_bytes: 0,
                    pending: None,
                }),
            })
        }
        PhysicalPlan::Join {
            left,
            right,
            condition,
            join_type,
            left_alias,
            right_alias,
        } => {
            let right_prefix = right_alias
                .clone()
                .unwrap_or_else(|| right_table_name(right));
            let left =
                build_kernel_with_ctes(db, left, Arc::clone(&params), snapshot, ctes, context)?;
            let right =
                build_kernel_with_ctes(db, right, Arc::clone(&params), snapshot, ctes, context)?;
            let right_column_count = right.columns.len();
            let right_duplicate_names = duplicate_column_names(&left.columns, &right.columns);
            let mut condition_columns = left.columns.clone();
            condition_columns.extend(right.columns.iter().map(|column| {
                if right_duplicate_names.contains(column) {
                    format!("{right_prefix}.{column}")
                } else {
                    column.clone()
                }
            }));
            let output_columns = qualify_join_columns(
                &condition_columns,
                &left.columns,
                &right.columns,
                left_alias,
                &right_prefix,
            );
            let trace = left.trace.clone();
            Ok(PullNode {
                columns: output_columns,
                trace,
                kind: PullKind::Join(JoinState {
                    left: Box::new(left),
                    right: Box::new(right),
                    condition: condition.clone(),
                    condition_columns,
                    right_column_count,
                    join_type: *join_type,
                    params,
                    right_rows: Vec::new(),
                    right_pending: None,
                    right_prepared: false,
                    left_row: None,
                    right_position: 0,
                    matched: false,
                }),
            })
        }
        PhysicalPlan::GraphBfs {
            start_alias,
            start_expr,
            start_candidates,
            filter_ctes,
            steps,
            filter,
            ..
        } => {
            let Some(step) = steps.first() else {
                return Err(
                    Error::Other("bounded graph plan has no traversal step".to_string()).into(),
                );
            };
            let resolved_filter = filter
                .as_ref()
                .map(|filter| {
                    bounded_resolve_in_subqueries(
                        db,
                        filter,
                        &params,
                        snapshot,
                        filter_ctes,
                        context,
                    )
                })
                .transpose()?;
            let mut predicates_pushed: smallvec::SmallVec<[std::borrow::Cow<'static, str>; 4]> =
                smallvec::SmallVec::new();
            let mut candidate_root = None;
            let mut unpinned_start = false;
            let starts = match resolve_uuid(start_expr, &params) {
                Ok(start) => {
                    predicates_pushed.push(std::borrow::Cow::Owned(format!("{start_alias}.id")));
                    VecDeque::from([start])
                }
                Err(Error::PlanError(_))
                    if matches!(
                        start_expr,
                        Expr::Column(contextdb_parser::ast::ColumnRef { table: None, .. })
                    ) =>
                {
                    if let Some(candidates) = start_candidates.as_deref() {
                        predicates_pushed
                            .push(std::borrow::Cow::Owned(format!("{start_alias}.id")));
                        candidate_root = Some(Box::new(build_kernel_with_ctes(
                            db,
                            candidates,
                            Arc::clone(&params),
                            snapshot,
                            ctes,
                            context,
                        )?));
                        VecDeque::new()
                    } else if let Some(filter) = resolved_filter.as_ref() {
                        match bounded_graph_static_starts(filter, &params, start_alias)? {
                            Some(starts) => {
                                // A traversal that PINS its start by id is
                                // asking about a node it names, so a reader
                                // who may not see that node is refused rather
                                // than handed an empty answer -- being told
                                // "no edges" about a node that has them says
                                // the store does not hold what it holds. The
                                // executor draws this line for the same
                                // statement; the kernel asks it the same way.
                                if let Some(database) = db.as_database_for_subquery_plan() {
                                    database.bounded_assert_graph_anchor_nodes_readable(
                                        context.transaction(),
                                        &starts,
                                        snapshot,
                                        |_bytes| {
                                            context.charge(
                                                BoundedWorkSource::AccessControl,
                                                BoundedSourceTouch::AccessRow,
                                            )?;
                                            context.attribute_live_memory(
                                                BoundedWorkSource::AccessControl,
                                            );
                                            Ok::<(), BoundedExecutionError>(())
                                        },
                                    )?;
                                }
                                predicates_pushed
                                    .push(std::borrow::Cow::Owned(format!("{start_alias}.id")));
                                starts.into()
                            }
                            None => {
                                // A start named by something other than its id
                                // is still a PINNED start -- the executor
                                // resolves it against the store and then
                                // probes. Calling it unpinned walks every edge
                                // in the store to answer a two-entry probe, and
                                // tells the operator it did. Resolved here by
                                // the executor's own resolver, in the caller's
                                // own transaction.
                                let resolution = bounded_resolve_graph_start_nodes(
                                    db,
                                    filter,
                                    &params,
                                    snapshot,
                                    start_alias,
                                    context,
                                )?;
                                if resolution.as_ref().is_some_and(|r| r.pinned) {
                                    // Every row the resolution read was charged
                                    // as it was read, inside the bounded source
                                    // that read it, so `rows_examined` already
                                    // holds them. Charging again here -- which
                                    // is what this did while the resolution was
                                    // eager -- pays for work after it is done,
                                    // which is not a ceiling.
                                    let resolution = resolution.expect("pinned resolution");
                                    predicates_pushed.extend(resolution.predicates_pushed);
                                    VecDeque::from(resolution.ids)
                                } else {
                                    unpinned_start = true;
                                    VecDeque::new()
                                }
                            }
                        }
                    } else {
                        unpinned_start = true;
                        VecDeque::new()
                    }
                }
                Err(error) => return Err(error.into()),
            };
            let single_step = steps.len() == 1 && step.min_depth == 1 && step.max_depth == 1;
            // Both ends of a single hop can be pinned, and when they are, both
            // reached the source: the start picks the adjacency list and the
            // target decides which of its entries is even looked at. Reporting
            // only the start tells an operator the read filtered a whole
            // neighbourhood in the query when it filtered it at the source.
            // Resolved from the same residual the executor resolves.
            let start_id_predicate = format!("{start_alias}.id");
            let target_residual = if single_step {
                resolved_filter
                    .as_ref()
                    .map(|filter| {
                        super::resolve_graph_target_id_residual(filter, &params, &step.target_alias)
                    })
                    .transpose()?
                    .flatten()
            } else {
                None
            };
            if target_residual.is_some()
                && predicates_pushed
                    .iter()
                    .any(|predicate| predicate.as_ref() == start_id_predicate)
            {
                predicates_pushed
                    .push(std::borrow::Cow::Owned(format!("{}.id", step.target_alias)));
            }
            // A hop pinned only where it ENDS names one node, and that node's
            // adjacency is the only list worth opening -- reached by reversing
            // the hop. Scanning every edge of the type to find the ones that
            // arrive there reads the whole graph to answer a question about
            // one node, and reports itself as a full edge scan while doing it.
            let mut starts = starts;
            let backwards_target = match target_residual {
                Some(target) if unpinned_start => {
                    predicates_pushed
                        .push(std::borrow::Cow::Owned(format!("{}.id", step.target_alias)));
                    unpinned_start = false;
                    starts = VecDeque::from([target]);
                    Some(super::reverse_graph_probe_direction(step.direction))
                }
                _ => None,
            };
            let edge_source = if unpinned_start {
                Some(db.bounded_graph_edge_cursor(
                    step.direction,
                    snapshot,
                    &mut |bytes| context.reserve(BoundedWorkSource::GraphTraversal, bytes),
                    &mut |bytes| context.release(bytes),
                )?)
            } else {
                None
            };
            let trace = graph_query_trace(
                match backwards_target {
                    Some(direction) => super::GraphTraceShape::AdjacencyProbe {
                        index: super::graph_adjacency_index_label(direction),
                    },
                    None => graph_trace_shape(single_step, unpinned_start, Some(step.direction)),
                },
                predicates_pushed,
            );
            let mut columns = Vec::with_capacity(
                steps
                    .len()
                    .checked_add(3)
                    .ok_or_else(|| bounded_size_error("graph output columns"))?,
            );
            columns.push(format!("{start_alias}.id"));
            columns.extend(steps.iter().map(|step| format!("{}.id", step.target_alias)));
            columns.push("id".to_string());
            columns.push("depth".to_string());
            Ok(PullNode {
                columns,
                trace,
                kind: PullKind::Graph(GraphState {
                    start_alias: start_alias.clone(),
                    steps: steps.clone(),
                    filter: resolved_filter,
                    params,
                    snapshot,
                    starts,
                    start_candidates: candidate_root,
                    edge_source,
                    visited_cap: db.bounded_legacy_visited_cap(context.declares_no_ceilings()),
                    frontier_unit_bytes: crate::executor::estimate_bfs_working_bytes(
                        &[()],
                        steps.as_slice(),
                    ),
                    single_step_unpinned: single_step && unpinned_start,
                    probes_backwards_from_target: backwards_target.is_some(),
                    single_step_probe: single_step,
                    probe_seen: Vec::new(),
                    probe_seen_container_bytes: 0,
                    seen_starts: Vec::new(),
                    seen_start_container_bytes: 0,
                    seen_edges: Vec::new(),
                    seen_edge_container_bytes: 0,
                    seen_paths: Vec::new(),
                    seen_path_container_bytes: 0,
                    frontier: Vec::new(),
                    frontier_container_bytes: 0,
                    row_memory: AmortizedRowMemory::default(),
                    active: None,
                }),
            })
        }
        PhysicalPlan::VectorSearch {
            table,
            column,
            query_expr,
            k,
            candidates,
            sort_key,
        }
        | PhysicalPlan::HnswSearch {
            table,
            column,
            query_expr,
            k,
            candidates,
            sort_key,
        } => build_vector_kernel(
            db,
            plan,
            table,
            column,
            query_expr,
            *k,
            candidates.as_deref(),
            sort_key.as_deref(),
            params,
            snapshot,
            ctes,
            context,
        ),
        PhysicalPlan::Pipeline(plans) => {
            let mut visible_ctes = ctes.clone();
            let mut last = None;
            for pipeline_plan in plans {
                if let PhysicalPlan::MaterializeCte { name, input } = pipeline_plan {
                    visible_ctes.insert(name.as_str(), input.as_ref());
                } else {
                    last = Some(pipeline_plan);
                }
            }
            let Some(last) = last else {
                return Ok(empty_node());
            };
            build_kernel_with_ctes(db, last, params, snapshot, &visible_ctes, context)
        }
        PhysicalPlan::IndexScan {
            table,
            index,
            range,
        } => build_direct_index_scan(db, table, index, range, params, snapshot, context),
        PhysicalPlan::CteRef { name } => {
            let input = ctes.get(name.as_str()).copied().ok_or_else(|| {
                Error::Other(format!(
                    "bounded CTE reference `{name}` has no materialized input"
                ))
            })?;
            build_kernel_with_ctes(db, input, params, snapshot, ctes, context)
        }
        _ => Err(
            Error::PlanError("bounded execution accepts read-only SELECT plans".to_string()).into(),
        ),
    }
}

#[allow(clippy::too_many_arguments)]
fn build_vector_kernel<'plan>(
    db: &dyn ReadExecutionTarget,
    plan: &'plan PhysicalPlan,
    table: &str,
    column: &str,
    query_expr: &Expr,
    requested_k: u64,
    candidates: Option<&'plan PhysicalPlan>,
    sort_key: Option<&str>,
    params: Arc<HashMap<String, Value>>,
    snapshot: SnapshotId,
    ctes: &HashMap<&'plan str, &'plan PhysicalPlan>,
    context: &Arc<RequestContext>,
) -> std::result::Result<PullNode, BoundedExecutionError> {
    db.assert_table_read_allowed(table)?;
    let index = VectorIndexRef::new(table.to_string(), column.to_string());
    let mut schema_indexes = vec![index.clone()];
    if let Some(source) = row_vector_source_ref(query_expr) {
        schema_indexes.push(source);
    }
    let schema = db.bounded_vector_schema_read_many(schema_indexes);
    db.assert_vector_index_exists_under_schema_read(&index)?;
    let (query, query_vector_source, query_bytes) =
        bounded_query_vector(db, query_expr, &params, snapshot, context)?;
    let k = usize::try_from(requested_k).map_err(|_| {
        Error::Other("bounded vector LIMIT exceeds the native address space".to_string())
    })?;
    let candidate_k = db.bounded_vector_candidate_k(&index, k, sort_key)?;
    let access_filter = db.bounded_read_requires_candidate_filter(table)?;
    let unrestricted = candidates
        .is_some_and(|candidate| is_unrestricted_scan_for_table(candidate, table))
        && !access_filter;
    let candidate_plan = if unrestricted { None } else { candidates };
    let candidate_root = candidate_plan
        .map(|candidate| {
            build_kernel_with_ctes(db, candidate, Arc::clone(&params), snapshot, ctes, context)
        })
        .transpose()?;
    // A description describes the plan, not the shortcut through it. The
    // candidate source above is skipped only when it is an unrestricted scan
    // of the whole table -- the vector index already covers every row, so
    // reading them first would be work for nothing -- but the statement still
    // named that scan, and the source it names is what an operator is asking
    // about. So the elided scan is described exactly as a scan is described,
    // and the label composes through the same function either way.
    let candidate_trace = match candidate_root.as_ref() {
        Some(root) => Some(root.trace.clone()),
        None if unrestricted => Some(QueryTrace::scan()),
        None => None,
    };
    // What this handle's own transaction has done to these vectors, decided
    // once here rather than asked per entry. Charged before it is held, like
    // the row overlay a scan takes.
    let vector_overlay = match context.transaction() {
        Some(tx) => db
            .transaction_vector_overlay(tx, &index)?
            .into_option_if_not_empty(),
        None => None,
    };
    let overlay_bytes = vector_overlay.as_ref().map_or(0, |overlay| overlay.bytes);
    if overlay_bytes > 0 {
        context.reserve(BoundedWorkSource::VectorCandidates, overlay_bytes)?;
    }
    // Taken before a single vector is read, and named for the work it belongs
    // to: a store whose limit stops this tells the operator WHICH work it
    // stopped, rather than reporting the read's own ceiling for a limit that
    // is not the read's.
    let overlay_reservation = match vector_overlay.as_ref() {
        Some(overlay) => {
            match db.bounded_vector_overlay_reservation(&index, overlay.searchable_entry_count) {
                Ok(reservation) => reservation,
                Err(error) => {
                    context.release(overlay_bytes)?;
                    return Err(error.into());
                }
            }
        }
        None => None,
    };
    // With an overlay in play the source must hand up everything this
    // transaction can see, because the overlay decides what survives AFTER the
    // source has scored it: stopping at the statement's LIMIT drops a row the
    // transaction removed and never lets a row it staged compete. The
    // established door widens its own fetch for exactly this case.
    let candidate_k = match vector_overlay.as_ref() {
        Some(overlay) => candidate_k.max(overlay.searchable_entry_count),
        None => candidate_k,
    };
    let meta = db
        .table_meta(table)
        .ok_or_else(|| Error::TableNotFound(table.to_string()))?;
    let schema_columns: Vec<String> = meta
        .columns
        .iter()
        .map(|column| column.name.clone())
        .collect();
    let mut columns = vec!["row_id".to_string()];
    columns.extend(schema_columns.iter().cloned());
    columns.push("score".to_string());
    let operator = if matches!(plan, PhysicalPlan::HnswSearch { .. }) {
        "HNSWSearch"
    } else {
        "VectorSearch"
    };
    Ok(PullNode {
        columns,
        trace: vector_search_trace_with_source(operator, candidate_trace, query_vector_source),
        kind: PullKind::Vector(VectorState {
            supplement_columns: Vec::new(),
            table: table.to_string(),
            index,
            query,
            _query_bytes: query_bytes,
            k,
            candidate_k,
            sort_key: sort_key.map(str::to_string),
            params,
            snapshot,
            _schema: schema,
            preparation: VectorPreparation::Candidates(candidate_root.map(Box::new)),
            candidate_filter: candidate_plan.is_some(),
            candidate_ids: Vec::new(),
            candidate_bytes: 0,
            source: None,
            scores: Vec::new(),
            score_bytes: 0,
            schema_columns,
            materialized: 0,
            ranked_rows: Vec::new(),
            ranked_container_bytes: 0,
            output: VecDeque::new(),
            output_container_bytes: 0,
            used_hnsw: false,
            vector_overlay,
            overlay_bytes,
            _overlay_reservation: overlay_reservation,
        }),
    })
}

fn reserve_query_vector_clone(
    source: &[f32],
    context: &Arc<RequestContext>,
) -> std::result::Result<(Vec<f32>, usize), BoundedExecutionError> {
    let planned = checked_size_mul(source.len(), std::mem::size_of::<f32>(), "query vector")?;
    context.reserve(BoundedWorkSource::VectorCandidates, planned)?;
    let mut vector = Vec::new();
    if vector.try_reserve_exact(source.len()).is_err() {
        context.release(planned)?;
        return Err(Error::Other("bounded query-vector allocation failed".to_string()).into());
    }
    vector.extend_from_slice(source);
    let actual = match checked_size_mul(
        vector.capacity(),
        std::mem::size_of::<f32>(),
        "query vector",
    ) {
        Ok(actual) => actual,
        Err(error) => {
            context.release(planned)?;
            return Err(error);
        }
    };
    reconcile_new_reservation(
        context,
        BoundedWorkSource::VectorCandidates,
        planned,
        actual,
    )?;
    Ok((vector, actual))
}

fn bounded_text_query_vector(
    text: &str,
    context: &Arc<RequestContext>,
) -> std::result::Result<(Vec<f32>, usize), BoundedExecutionError> {
    let trimmed = text.trim();
    let inner = trimmed
        .strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
        .ok_or_else(|| Error::Other(format!("invalid VECTOR literal '{text}'")))?;
    let component_count = if inner.trim().is_empty() {
        0
    } else {
        inner.split(',').count()
    };
    let planned = checked_size_mul(component_count, std::mem::size_of::<f32>(), "query vector")?;
    context.reserve(BoundedWorkSource::VectorCandidates, planned)?;
    let mut vector = Vec::new();
    if vector.try_reserve_exact(component_count).is_err() {
        context.release(planned)?;
        return Err(Error::Other("bounded query-vector allocation failed".to_string()).into());
    }
    if component_count != 0 {
        for part in inner.split(',') {
            let value = match part.trim().parse::<f32>() {
                Ok(value) => value,
                Err(error) => {
                    context.release(planned)?;
                    return Err(Error::Other(format!(
                        "invalid VECTOR component '{}': {error}",
                        part.trim()
                    ))
                    .into());
                }
            };
            vector.push(value);
        }
    }
    let actual = checked_size_mul(
        vector.capacity(),
        std::mem::size_of::<f32>(),
        "query vector",
    )?;
    reconcile_new_reservation(
        context,
        BoundedWorkSource::VectorCandidates,
        planned,
        actual,
    )?;
    Ok((vector, actual))
}

fn bounded_query_vector_from_value(
    value: &Value,
    params: &HashMap<String, Value>,
    context: &Arc<RequestContext>,
) -> std::result::Result<(Vec<f32>, usize), BoundedExecutionError> {
    bounded_query_vector_from_scalar(BorrowedScalar::from_value(value), params, context)
}

fn bounded_query_vector_from_scalar(
    value: BorrowedScalar<'_>,
    params: &HashMap<String, Value>,
    context: &Arc<RequestContext>,
) -> std::result::Result<(Vec<f32>, usize), BoundedExecutionError> {
    match value {
        BorrowedScalar::Vector(vector) => reserve_query_vector_clone(vector, context),
        BorrowedScalar::Text(text) if text.trim_start().starts_with('[') => {
            bounded_text_query_vector(text, context)
        }
        BorrowedScalar::Text(name) => match params.get(name) {
            Some(Value::Vector(vector)) => reserve_query_vector_clone(vector, context),
            _ => Err(Error::PlanError("vector parameter missing".to_string()).into()),
        },
        _ => Err(Error::PlanError("invalid vector query expression".to_string()).into()),
    }
}

fn bounded_query_vector_from_text(
    text: &str,
    params: &HashMap<String, Value>,
    context: &Arc<RequestContext>,
) -> std::result::Result<(Vec<f32>, usize), BoundedExecutionError> {
    if text.trim_start().starts_with('[') {
        return bounded_text_query_vector(text, context);
    }
    match params.get(text) {
        Some(Value::Vector(vector)) => reserve_query_vector_clone(vector, context),
        _ => Err(Error::PlanError("vector parameter missing".to_string()).into()),
    }
}

fn bounded_query_vector(
    db: &dyn ReadExecutionTarget,
    expr: &Expr,
    params: &HashMap<String, Value>,
    snapshot: SnapshotId,
    context: &Arc<RequestContext>,
) -> std::result::Result<(Vec<f32>, Option<VectorIndexRef>, usize), BoundedExecutionError> {
    match expr {
        Expr::RowVectorSource { table, column, key } => {
            let source_index = VectorIndexRef::new(table.clone(), column.clone());
            let meta = db
                .table_meta(table)
                .ok_or_else(|| Error::TableNotFound(table.clone()))?;
            db.assert_table_read_allowed(table)?;
            if !meta.columns.iter().any(|candidate| {
                candidate.name == *column && matches!(candidate.column_type, ColumnType::Vector(_))
            }) {
                return Err(Error::UnknownVectorIndex {
                    index: source_index.clone(),
                }
                .into());
            }
            db.assert_vector_index_exists_under_schema_read(&source_index)?;

            let raw_key = bounded_resolve_scalar(key, params)?;
            if raw_key.is_null() {
                return Err(Error::PlanError("ROW_VECTOR key cannot be NULL".to_string()).into());
            }
            if matches!(raw_key, BorrowedScalar::Vector(_)) {
                return Err(
                    Error::PlanError("ROW_VECTOR key cannot be a vector".to_string()).into(),
                );
            }
            let key_column = db.natural_key_column_for_table(table)?;
            let key = db.coerce_into_column(
                table,
                &key_column,
                raw_key.into_owned(),
                None,
                None,
            )
            .map_err(|error| {
                Error::PlanError(format!(
                    "ROW_VECTOR argument 3 key cannot be coerced to `{table}`.`{key_column}` natural key: {error}"
                ))
            })?;
            if matches!(key, Value::Null) {
                return Err(Error::PlanError("ROW_VECTOR key cannot be NULL".to_string()).into());
            }
            let key_label = row_vector_key_label(&key);
            // The row the key names is resolved inside the caller's own
            // transaction, exactly as the executor resolves it, so a row this
            // session staged is the row this read is about.
            let tx = context.transaction();
            let row_id = db
                .row_id_for_natural_key_in_tx(tx, table, &key_column, &key, snapshot)?
                .ok_or_else(|| Error::PersistedRowVectorRowMissing {
                    index: source_index.clone(),
                    key: key_label.clone(),
                })?;
            let resolved = db.with_bounded_row_vector(
                &source_index,
                row_id,
                snapshot,
                tx,
                &mut |_candidate_bytes| {
                    context.charge(
                        BoundedWorkSource::AccessControl,
                        BoundedSourceTouch::AccessRow,
                    )?;
                    context.attribute_live_memory(BoundedWorkSource::AccessControl);
                    Ok::<(), BoundedExecutionError>(())
                },
                &mut |vector| {
                    db.validate_vector_under_schema_read(&source_index, vector.len())?;
                    reserve_query_vector_clone(vector, context)
                },
            )?;
            let (vector, retained_bytes) =
                resolved.ok_or_else(|| Error::PersistedRowVectorCellNull {
                    index: source_index.clone(),
                    key: key_label,
                })?;
            Ok((vector, Some(source_index), retained_bytes))
        }
        Expr::Literal(Literal::Vector(vector)) => {
            let (vector, bytes) = reserve_query_vector_clone(vector, context)?;
            Ok((vector, None, bytes))
        }
        Expr::Literal(Literal::Text(text)) => {
            let (vector, bytes) = bounded_query_vector_from_text(text, params, context)?;
            Ok((vector, None, bytes))
        }
        Expr::Parameter(name) => {
            let value = params
                .get(name)
                .ok_or_else(|| Error::NotFound(format!("missing parameter: {name}")))?;
            let (vector, bytes) = bounded_query_vector_from_value(value, params, context)?;
            Ok((vector, None, bytes))
        }
        Expr::Column(column) => {
            let value = params
                .get(&column.column)
                .ok_or_else(|| Error::PlanError("vector parameter missing".to_string()))?;
            let (vector, bytes) = bounded_query_vector_from_value(value, params, context)?;
            Ok((vector, None, bytes))
        }
        Expr::CosineDistance { right, .. } => {
            bounded_query_vector(db, right, params, snapshot, context)
        }
        _ => {
            let resolved = bounded_resolve_scalar(expr, params)?;
            let (vector, bytes) = bounded_query_vector_from_scalar(resolved, params, context)?;
            Ok((vector, None, bytes))
        }
    }
}

fn empty_node() -> PullNode {
    PullNode {
        columns: Vec::new(),
        trace: QueryTrace::scan(),
        kind: PullKind::Static(StaticState {
            rows: VecDeque::new(),
        }),
    }
}

/// Capture what an open transaction changes about this table, and charge what
/// holding it costs.
///
/// An index and an ordered run describe the committed rows only -- neither
/// knows a staged row exists -- so a scan that has to see a transaction reads
/// the table itself and re-applies the predicate per row. That is slower and
/// it is the only way the answer can be right; it applies to the writing
/// session's own reads and to nothing else.
fn scan_transaction_overlay(
    db: &dyn ReadExecutionTarget,
    table: &str,
    context: &Arc<RequestContext>,
) -> std::result::Result<Option<ScanTransactionOverlay>, BoundedExecutionError> {
    let Some(tx) = context.transaction() else {
        return Ok(None);
    };
    let overlay = db.transaction_table_overlay(tx, table)?;
    if overlay.is_empty() {
        return Ok(None);
    }
    context.reserve(BoundedWorkSource::TableScan, overlay.bytes)?;
    Ok(Some(ScanTransactionOverlay {
        tx,
        overlay,
        restaged_over_committed: std::collections::HashSet::new(),
        published: 0,
        committed_exhausted: false,
    }))
}

#[allow(clippy::too_many_arguments)]
fn build_scan(
    db: &dyn ReadExecutionTarget,
    table: &str,
    filter: Option<&Expr>,
    params: Arc<HashMap<String, Value>>,
    snapshot: SnapshotId,
    ordered: Option<(String, contextdb_core::SortDirection, String, bool)>,
    // The index the CALLER already chose for this table. A caller that has
    // asked the planner which index answers its predicate hands the answer in
    // rather than making the scan ask again -- and a scan that is given no
    // filter has nothing to ask with, so without this it would walk the whole
    // table where the executor reads one run of an index.
    chosen_pick: Option<IndexPick>,
    context: &Arc<RequestContext>,
) -> std::result::Result<PullNode, BoundedExecutionError> {
    // The name `dual` answers as a one-row, zero-column constant source only
    // while no table carries it. An operator who creates a table called `dual`
    // gets that table: its rows, its columns, and its vector index.
    if table == "dual" && db.table_meta(table).is_none() {
        return Ok(PullNode {
            columns: Vec::new(),
            trace: QueryTrace::scan(),
            kind: PullKind::Static(StaticState {
                rows: VecDeque::from([PulledRow {
                    values: Vec::new(),
                    retained_bytes: 0,
                }]),
            }),
        });
    }
    db.assert_table_read_allowed(table)?;
    let meta = db
        .table_meta(table)
        .ok_or_else(|| Error::TableNotFound(table.to_string()))?;
    let schema_columns = meta
        .columns
        .iter()
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    // A filter written as another query is resolved before the scan plans
    // anything, by the executor's own resolver and inside the caller's own
    // transaction -- the same call the executor makes. Left unresolved, the
    // inner query reaches the evaluator intact and the read refuses a
    // statement the store answers perfectly well through its other door.
    let resolved_filter = filter
        .map(|filter| bounded_resolve_in_subqueries(db, filter, &params, snapshot, &[], context))
        .transpose()?;
    let mut considered = smallvec::SmallVec::new();
    // The index choice is made from the filter AS WRITTEN. Resolving an inner
    // query first turns `a IN (SELECT ...)` into a list of literals, and a
    // shape the writer never wrote then looks indexable -- so the two doors
    // disagree about whether the statement uses an index at all.
    let filter_analysis =
        filter.map(|filter| analyze_filter_for_index(filter, &meta.indexes, &params));
    // A predicate that can match nothing -- contradictory equalities on the
    // same column, a NULL or NaN bound where a value is required, a literal
    // that cannot even be read as the column's type -- answers zero rows
    // having read none. The executor decides this before it touches the store
    // (`executor.rs:6912`); a read that instead drops the unusable pick walks
    // the whole table, charges the reader for every row of it, and for NaN
    // hands back a row the other door never returns.
    let coerced_filter_pick = filter_analysis
        .as_ref()
        .and_then(|analysis| analysis.pick.clone())
        .map(|pick| db.coerce_pick_shape_to_column_type(table, &pick));
    let filter_matches_nothing = match coerced_filter_pick.as_ref() {
        Some(Ok(pick)) => crate::executor::pick_matches_nothing(pick),
        Some(Err(_)) => true,
        None => false,
    };
    let filter_pick = match coerced_filter_pick {
        Some(Ok(pick)) => Some(pick),
        Some(Err(_)) | None => None,
    };
    let transaction = scan_transaction_overlay(db, table, context)?;
    let mut ordered_pick = None;
    let mut exact_pick: Option<IndexPick> = None;
    let mut empty_pick: Option<IndexPick> = None;
    let mode = if filter_matches_nothing {
        considered.extend(
            filter_analysis
                .as_ref()
                .map(|analysis| analysis.considered.clone())
                .unwrap_or_default(),
        );
        // The index was still chosen and still answered -- with nothing. The
        // statement reports the index it used, exactly as it does when the
        // same index returns rows.
        empty_pick = filter_pick.clone();
        ScanMode::Empty
    } else if let Some((column, direction, index, reverse)) = ordered {
        // The index that serves the order often serves the predicate too.
        // Walking it end to end and rejecting rows afterwards reads the whole
        // index where the executor reads the run the predicate names, so the
        // same statement reports a different examined count through this door.
        let pick = filter_pick.clone().filter(|pick| pick.name == index);
        let cursor = BoundedOrderedRowCursor::for_index(index, reverse);
        let cursor = match pick.as_ref() {
            Some(pick) => {
                considered.extend(
                    filter_analysis
                        .as_ref()
                        .map(|analysis| analysis.considered.clone())
                        .unwrap_or_default(),
                );
                // The run is expressed in the INDEX's physical order, which a
                // descending leading column reverses -- so the bounds are read
                // off the index's own declared direction, never off the
                // direction the ORDER BY asked for.
                let index_direction = pick
                    .columns
                    .first()
                    .map(|(_, index_direction)| *index_direction)
                    .unwrap_or(direction);
                let (run_start, run_end) = declared_index_run(pick, index_direction);
                cursor.seeking_run(run_start, run_end)
            }
            None => cursor,
        };
        ordered_pick = pick;
        ScanMode::Ordered {
            column,
            direction,
            cursor,
        }
    } else if let Some(pick) = match chosen_pick {
        Some(pick) => Some(db.coerce_pick_shape_to_column_type(table, &pick)?),
        None => match filter_analysis.as_ref() {
            Some(analysis) => {
                considered.extend(analysis.considered.clone());
                analysis
                    .pick
                    .clone()
                    .map(|pick| db.coerce_pick_shape_to_column_type(table, &pick))
                    .transpose()?
            }
            None => None,
        },
    } {
        let pick = Some(pick);
        let declaration = pick.as_ref().and_then(|pick| {
            meta.indexes
                .iter()
                .find(|index| index.name == pick.name)
                .cloned()
        });
        if let (Some(pick), Some(declaration)) = (pick.as_ref(), declaration.as_ref())
            && let Some(keys) = crate::executor::exact_probe_keys(declaration.columns.len(), pick)
        {
            // A predicate that names whole keys is answered by asking the
            // index for those keys -- one lookup each, whatever kind of index
            // it is. Collapsing a list of keys into the span between its
            // least and greatest member instead reads every key in between
            // and rejects them afterwards, which reports far more rows
            // examined for the same answer.
            exact_pick = Some(pick.clone());
            ScanMode::Exact {
                index: pick.name.clone(),
                cursor: contextdb_relational::mem::BoundedExactIndexCursor::for_keys(keys),
            }
        } else if let Some(pick) = pick.filter(|pick| {
            meta.indexes.iter().any(|index| {
                index.name == pick.name && index.kind == contextdb_core::IndexKind::UserDeclared
            })
        }) {
            let (column, direction) = pick.columns.first().cloned().ok_or_else(|| {
                Error::Other(format!("bounded index {} has no leading column", pick.name))
            })?;
            // Admitted before the runs are built, so a list too long for the
            // ceiling is refused rather than allocated and then discovered.
            // What the cursor ends up holding is charged by the continuation
            // this kernel is reconciled to, so the admission is handed back
            // as soon as the runs exist and is never counted twice.
            let planned_runs = planned_pending_run_bytes(&pick)?;
            context.reserve(BoundedWorkSource::TableScan, planned_runs)?;
            let cursor = BoundedOrderedRowCursor::for_index(pick.name.clone(), false)
                .seeking_runs(declared_index_runs(&pick, direction));
            context.release(planned_runs)?;
            ScanMode::Index {
                cursor,
                pick,
                column,
                direction,
            }
        } else {
            ScanMode::Physical {
                cursor: db.bounded_physical_table_cursor(table)?,
            }
        }
    } else {
        ScanMode::Physical {
            cursor: db.bounded_physical_table_cursor(table)?,
        }
    };
    let offers_every_committed_row = matches!(mode, ScanMode::Physical { .. });
    // An ordered walk with a transaction open folds the transaction's own
    // rows into the run rather than giving the run up for a sort of the
    // whole table.
    let ordered_merge = match (&mode, transaction.is_some()) {
        (
            ScanMode::Ordered {
                column, direction, ..
            },
            true,
        ) => Some(OrderedMerge {
            column: column.clone(),
            direction: *direction,
            staged: None,
            deferred: None,
            held_bytes: 0,
        }),
        _ => None,
    };
    let mut columns = vec!["row_id".to_string()];
    columns.extend(schema_columns.iter().cloned());
    Ok(PullNode {
        columns,
        trace: match &mode {
            ScanMode::Ordered { cursor, .. } => QueryTrace {
                physical_plan: "IndexScan",
                index_used: cursor.index_name().map(str::to_string),
                sort_elided: true,
                predicates_pushed: ordered_pick
                    .as_ref()
                    .map(|pick| {
                        pick.pushed_columns
                            .iter()
                            .cloned()
                            .map(std::borrow::Cow::Owned)
                            .collect()
                    })
                    .unwrap_or_default(),
                indexes_considered: considered.clone(),
                ..Default::default()
            },
            ScanMode::Index { pick, .. } => QueryTrace {
                physical_plan: "IndexScan",
                index_used: Some(pick.name.clone()),
                predicates_pushed: pick
                    .pushed_columns
                    .iter()
                    .cloned()
                    .map(std::borrow::Cow::Owned)
                    .collect(),
                indexes_considered: considered,
                ..Default::default()
            },
            ScanMode::Exact { index, .. } => QueryTrace {
                physical_plan: "IndexScan",
                index_used: Some(index.clone()),
                predicates_pushed: exact_pick
                    .as_ref()
                    .map(|pick| {
                        pick.pushed_columns
                            .iter()
                            .cloned()
                            .map(std::borrow::Cow::Owned)
                            .collect()
                    })
                    .unwrap_or_default(),
                indexes_considered: considered,
                ..Default::default()
            },
            ScanMode::Empty if empty_pick.is_some() => {
                let pick = empty_pick.as_ref().expect("empty scan pick");
                QueryTrace {
                    physical_plan: "IndexScan",
                    index_used: Some(pick.name.clone()),
                    predicates_pushed: pick
                        .pushed_columns
                        .iter()
                        .cloned()
                        .map(std::borrow::Cow::Owned)
                        .collect(),
                    indexes_considered: considered,
                    ..Default::default()
                }
            }
            ScanMode::Physical { .. } | ScanMode::Empty => QueryTrace {
                indexes_considered: considered,
                ..QueryTrace::scan()
            },
        },
        kind: PullKind::Scan(ScanState {
            table: table.to_string(),
            meta,
            schema_columns,
            filter: resolved_filter,
            params,
            snapshot,
            mode,
            cursor_key_bytes: 0,
            residual_pick: ordered_pick,
            committed_source_offers_every_row: offers_every_committed_row,
            pending_row: None,
            supplement_columns: Vec::new(),
            pending_from_committed: true,
            ordered_merge,
            transaction,
        }),
    })
}

/// Tell every source in this kernel which quantized vector columns the answer
/// names, so a scan fills those and leaves the rest in the store.
///
/// Demand is not only the answer's column list: a predicate, a join key or an
/// ordering expression evaluated against the ROW needs the value too, and a
/// column that reaches such an expression as `Null` answers wrong. Each node
/// therefore adds what its own expressions name before handing the set down.
/// A vector-distance ordering is not among them -- that comparison is scored
/// from the index, never from the row.
fn declare_supplemented_columns(
    db: &dyn ReadExecutionTarget,
    node: &mut PullNode,
    wanted: &std::collections::BTreeSet<String>,
) {
    let mut wanted = wanted.clone();
    match &node.kind {
        PullKind::Project(state) => {
            for column in &state.columns {
                collect_expr_columns(&column.expr, &mut wanted);
            }
        }
        PullKind::Filter(state) => collect_expr_columns(&state.predicate, &mut wanted),
        PullKind::Sort(state) => {
            for key in &state.keys {
                collect_expr_columns(&key.expr, &mut wanted);
            }
        }
        PullKind::Join(state) => collect_expr_columns(&state.condition, &mut wanted),
        _ => {}
    }
    if let PullKind::Scan(state) = &mut node.kind {
        if let Some(filter) = state.filter.as_ref() {
            collect_expr_columns(filter, &mut wanted);
        }
        state.supplement_columns = db
            .quantized_vector_columns(&state.table)
            .into_iter()
            .filter(|column| wanted.contains(column))
            .collect();
        return;
    }
    // A search fetches and projects its own rows, so it carries the same
    // demand the scan does.
    if let PullKind::Vector(state) = &mut node.kind {
        state.supplement_columns = db
            .quantized_vector_columns(&state.table)
            .into_iter()
            .filter(|column| wanted.contains(column))
            .collect();
    }
    for input in pull_node_inputs_mut(&mut node.kind) {
        declare_supplemented_columns(db, input, &wanted);
    }
}

/// Every column name an expression reads from a row. The vector-distance
/// operator is skipped: it is answered by the index, not by the row's value.
fn collect_expr_columns(expr: &Expr, into: &mut std::collections::BTreeSet<String>) {
    match expr {
        Expr::Column(reference) => {
            into.insert(reference.column.clone());
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_expr_columns(left, into);
            collect_expr_columns(right, into);
        }
        // Answered by the index, never by the row's own value, so a column
        // that appears only here is not demand.
        Expr::CosineDistance { .. } => {}
        Expr::FunctionCall { args, .. } => {
            for arg in args {
                collect_expr_columns(arg, into);
            }
        }
        Expr::UnaryOp { operand, .. } => collect_expr_columns(operand, into),
        Expr::IsNull { expr, .. } => collect_expr_columns(expr, into),
        Expr::Like { expr, .. } => collect_expr_columns(expr, into),
        Expr::InList { expr, .. } => collect_expr_columns(expr, into),
        Expr::InSubquery { expr, .. } => collect_expr_columns(expr, into),
        _ => {}
    }
}

/// The nodes this one reads from, borrowed for change.
fn pull_node_inputs_mut(kind: &mut PullKind) -> Vec<&mut PullNode> {
    match kind {
        PullKind::Graph(state) => state.start_candidates.as_deref_mut().into_iter().collect(),
        PullKind::Vector(state) => match &mut state.preparation {
            VectorPreparation::Candidates(Some(candidates)) => vec![candidates.as_mut()],
            _ => Vec::new(),
        },
        PullKind::Project(state) => vec![state.input.as_mut()],
        PullKind::Sort(state) => vec![state.input.as_mut()],
        PullKind::Limit(state) => vec![state.input.as_mut()],
        PullKind::Filter(state) => vec![state.input.as_mut()],
        PullKind::Distinct(state) => vec![state.input.as_mut()],
        PullKind::Join(state) => vec![state.left.as_mut(), state.right.as_mut()],
        PullKind::Union(state) => state.inputs.iter_mut().collect(),
        PullKind::Scan(_) | PullKind::Static(_) => Vec::new(),
    }
}

/// One complete answer about the store itself, admitted row by row.
fn build_store_state(
    db: &dyn ReadExecutionTarget,
    plan: &PhysicalPlan,
    context: &Arc<RequestContext>,
) -> std::result::Result<PullNode, BoundedExecutionError> {
    let answered = db.store_state_answer(plan)?;
    let mut rows = VecDeque::new();
    for values in answered.rows {
        context.charge(BoundedWorkSource::TableScan, BoundedSourceTouch::TableRow)?;
        let retained_bytes = value_refs_retained_bytes(values.iter(), values.len())?;
        context.reserve(BoundedWorkSource::TableScan, retained_bytes)?;
        rows.push_back(PulledRow {
            values,
            retained_bytes,
        });
    }
    Ok(PullNode {
        columns: answered.columns,
        trace: answered.trace,
        kind: PullKind::Static(StaticState { rows }),
    })
}

fn build_direct_index_scan(
    db: &dyn ReadExecutionTarget,
    table: &str,
    index: &str,
    range: &contextdb_planner::ScanRange,
    params: Arc<HashMap<String, Value>>,
    snapshot: SnapshotId,
    context: &Arc<RequestContext>,
) -> std::result::Result<PullNode, BoundedExecutionError> {
    db.assert_table_read_allowed(table)?;
    let meta = db
        .table_meta(table)
        .ok_or_else(|| Error::TableNotFound(table.to_string()))?;
    let declaration = meta
        .indexes
        .iter()
        .find(|candidate| candidate.name == index)
        .ok_or_else(|| Error::IndexNotFound {
            table: table.to_string(),
            index: index.to_string(),
        })?;
    let (leading_column, direction) = declaration
        .columns
        .first()
        .cloned()
        .ok_or_else(|| Error::Other(format!("bounded index {index} has no leading column")))?;
    let shape = if let Some(value) = range.equality.as_ref() {
        IndexPredicateShape::Equality(value.clone())
    } else {
        IndexPredicateShape::Range {
            lower: range.lower.clone(),
            upper: range.upper.clone(),
        }
    };
    let pick = db.coerce_pick_shape_to_column_type(
        table,
        &IndexPick {
            name: index.to_string(),
            columns: declaration.columns.clone(),
            shape,
            pushed_column: leading_column.clone(),
            pushed_columns: vec![leading_column.clone()],
            suffix_values: Vec::new(),
            prefix_empty: false,
        },
    )?;
    let schema_columns = meta
        .columns
        .iter()
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    let transaction = scan_transaction_overlay(db, table, context)?;
    // A declared index is walked as the run the predicate names, with an open
    // transaction as much as without one: the run answers for the committed
    // rows, and the same predicate is applied per row as the staged ones
    // merge in, so an open transaction no longer costs the reader a walk of
    // the whole table for a query the trace calls an index scan. An index the
    // engine synthesized is not a run this scan can seek, so its predicate
    // stays a residual the physical walk applies to every row it sees.
    let declared = declaration.kind == contextdb_core::IndexKind::UserDeclared;
    let residual_pick = (!declared).then(|| pick.clone());
    let mode = if declared {
        let (run_start, run_end) = declared_index_run(&pick, direction);
        ScanMode::Index {
            pick,
            column: leading_column,
            direction,
            cursor: BoundedOrderedRowCursor::for_index(index.to_string(), false)
                .seeking_run(run_start, run_end),
        }
    } else {
        ScanMode::Physical {
            cursor: db.bounded_physical_table_cursor(table)?,
        }
    };
    let offers_every_committed_row = matches!(mode, ScanMode::Physical { .. });
    let mut columns = vec!["row_id".to_string()];
    columns.extend(schema_columns.iter().cloned());
    Ok(PullNode {
        columns,
        trace: QueryTrace {
            physical_plan: "IndexScan",
            index_used: Some(index.to_string()),
            ..Default::default()
        },
        kind: PullKind::Scan(ScanState {
            table: table.to_string(),
            meta,
            schema_columns,
            filter: None,
            params,
            snapshot,
            mode,
            cursor_key_bytes: 0,
            residual_pick,
            committed_source_offers_every_row: offers_every_committed_row,
            pending_row: None,
            supplement_columns: Vec::new(),
            pending_from_committed: true,
            ordered_merge: None,
            transaction,
        }),
    })
}

#[allow(clippy::type_complexity)]
fn ordered_scan<'a>(
    db: &dyn ReadExecutionTarget,
    input: &'a PhysicalPlan,
    keys: &'a [SortKey],
) -> Option<(
    &'a str,
    Option<&'a Expr>,
    String,
    contextdb_core::SortDirection,
    String,
    bool,
)> {
    let PhysicalPlan::Scan { table, filter, .. } = input else {
        return None;
    };
    // Which index answers an ORDER BY without sorting is the executor's own
    // decision, asked here rather than decided again. Deciding it twice is how
    // one door sorts where the other does not.
    let decl = crate::executor::sort_elision_index_decl(db, input, keys)?;
    let (column, index_direction) = decl.columns.first()?.clone();
    // The rule above matched EVERY sort key against the index's own columns in
    // order, so walking the index answers all of them; the leading key is the
    // one the walk is named by. Insisting on a single key here refused an
    // ORDER BY over a composite index that the other door answers from the
    // index alone.
    let key = keys.first()?;
    let direction = match key.direction {
        SortDirection::Asc => contextdb_core::SortDirection::Asc,
        SortDirection::Desc => contextdb_core::SortDirection::Desc,
        SortDirection::CosineDistance => return None,
    };
    // The rule above already matched every key's direction against the index,
    // so the run is walked forward; a reversed walk would mean the two doors
    // disagreed about the order.
    let _ = index_direction;
    Some((
        table.as_str(),
        filter.as_ref(),
        column,
        direction,
        decl.name.clone(),
        false,
    ))
}

/// Where in index order the run this predicate names begins and ends, stated
/// on the leading indexed component. A shape that names no run — an exclusion,
/// or a prefix already known to be empty — declares none, and the source walks
/// as it would with no predicate at all.
/// The runs of index keys a predicate names, in the order it named them.
///
/// A predicate naming a LIST of values names one run per listed value, which
/// is how the other door reads it: `execute_index_scan` probes each listed
/// value's prefix in turn (`executor.rs`≈:7071) rather than reading from the
/// least listed value to the greatest and rejecting everything in between.
/// Reading the span costs every key between two listed values -- six rows
/// asked for, nine read, for a list of two on a table holding five values.
/// Every other predicate shape names exactly one run.
/// What the runs a list predicate leaves in the cursor will hold, worked out
/// from the predicate itself so the memory is admitted BEFORE it is built. A
/// predicate naming a list of values names one run per value; the walk takes
/// the first straight away and keeps the rest, and each kept run copies its
/// value into both of its edges.
fn planned_pending_run_bytes(
    pick: &IndexPick,
) -> std::result::Result<usize, BoundedExecutionError> {
    let IndexPredicateShape::InList(values) = &pick.shape else {
        return Ok(0);
    };
    if pick.prefix_empty || values.len() < 2 {
        return Ok(0);
    }
    let mut bytes = checked_size_mul(
        values.len() - 1,
        std::mem::size_of::<(Bound<DirectedValue>, Bound<DirectedValue>)>(),
        "index run bounds",
    )?;
    for value in values.iter().skip(1) {
        let edges = checked_size_mul(value.estimated_bytes(), 2, "index run bound")?;
        bytes = checked_size_add(bytes, edges, "index run bounds")?;
    }
    Ok(bytes)
}

fn declared_index_runs(
    pick: &IndexPick,
    direction: contextdb_core::SortDirection,
) -> Vec<(Bound<DirectedValue>, Bound<DirectedValue>)> {
    let directed = |value: &Value| match direction {
        contextdb_core::SortDirection::Asc => DirectedValue::Asc(TotalOrdAsc(value.clone())),
        contextdb_core::SortDirection::Desc => DirectedValue::Desc(TotalOrdDesc(value.clone())),
    };
    match &pick.shape {
        IndexPredicateShape::InList(values) if !pick.prefix_empty && !values.is_empty() => values
            .iter()
            .map(|value| {
                (
                    Bound::Included(directed(value)),
                    Bound::Included(directed(value)),
                )
            })
            .collect(),
        _ => vec![declared_index_run(pick, direction)],
    }
}

fn declared_index_run(
    pick: &IndexPick,
    direction: contextdb_core::SortDirection,
) -> (Bound<DirectedValue>, Bound<DirectedValue>) {
    let directed = |value: &Value| match direction {
        contextdb_core::SortDirection::Asc => DirectedValue::Asc(TotalOrdAsc(value.clone())),
        contextdb_core::SortDirection::Desc => DirectedValue::Desc(TotalOrdDesc(value.clone())),
    };
    let ordered = |lower: &Bound<Value>, upper: &Bound<Value>| {
        let low = match lower {
            Bound::Included(value) => Bound::Included(directed(value)),
            Bound::Excluded(value) => Bound::Excluded(directed(value)),
            Bound::Unbounded => Bound::Unbounded,
        };
        let high = match upper {
            Bound::Included(value) => Bound::Included(directed(value)),
            Bound::Excluded(value) => Bound::Excluded(directed(value)),
            Bound::Unbounded => Bound::Unbounded,
        };
        // A descending component stores the largest value first, so the run
        // the predicate names starts at the value order's upper edge.
        match direction {
            contextdb_core::SortDirection::Asc => (low, high),
            contextdb_core::SortDirection::Desc => (high, low),
        }
    };
    if pick.prefix_empty {
        return (Bound::Unbounded, Bound::Unbounded);
    }
    match &pick.shape {
        IndexPredicateShape::Equality(value) => (
            Bound::Included(directed(value)),
            Bound::Included(directed(value)),
        ),
        IndexPredicateShape::IsNull => (
            Bound::Included(directed(&Value::Null)),
            Bound::Included(directed(&Value::Null)),
        ),
        IndexPredicateShape::Range { lower, upper } => ordered(lower, upper),
        IndexPredicateShape::InList(values) => {
            let mut edges = values.iter().map(directed).collect::<Vec<_>>();
            edges.sort();
            match (edges.first(), edges.last()) {
                (Some(first), Some(last)) => (
                    Bound::Included(first.clone()),
                    Bound::Included(last.clone()),
                ),
                _ => (Bound::Unbounded, Bound::Unbounded),
            }
        }
        IndexPredicateShape::NotEqual(_) | IndexPredicateShape::IsNotNull => {
            (Bound::Unbounded, Bound::Unbounded)
        }
    }
}

fn index_key_matches_pick(key: &IndexKey, pick: &IndexPick) -> bool {
    if pick.prefix_empty || key.is_empty() {
        return false;
    }
    let mut values = key.iter().map(|value| match value {
        DirectedValue::Asc(TotalOrdAsc(value)) | DirectedValue::Desc(TotalOrdDesc(value)) => value,
    });
    let Some(value) = values.next() else {
        return false;
    };
    for expected in &pick.suffix_values {
        let Some(actual) = values.next() else {
            return false;
        };
        if !values_equal(expected, actual) {
            return false;
        }
    }
    match &pick.shape {
        IndexPredicateShape::Equality(expected) => values_equal(value, expected),
        IndexPredicateShape::NotEqual(expected) => !values_equal(value, expected),
        IndexPredicateShape::Range { lower, upper } => {
            bound_allows_lower(value, lower) && bound_allows_upper(value, upper)
        }
        IndexPredicateShape::InList(expected) => expected
            .iter()
            .any(|candidate| values_equal(value, candidate)),
        IndexPredicateShape::IsNull => value == &Value::Null,
        IndexPredicateShape::IsNotNull => value != &Value::Null,
    }
}

fn values_equal(left: &Value, right: &Value) -> bool {
    left == right || compare_values(left, right) == Some(Ordering::Equal)
}

fn bound_allows_lower(value: &Value, bound: &std::ops::Bound<Value>) -> bool {
    match bound {
        std::ops::Bound::Included(lower) => matches!(
            compare_values(value, lower),
            Some(Ordering::Equal | Ordering::Greater)
        ),
        std::ops::Bound::Excluded(lower) => compare_values(value, lower) == Some(Ordering::Greater),
        std::ops::Bound::Unbounded => true,
    }
}

fn bound_allows_upper(value: &Value, bound: &std::ops::Bound<Value>) -> bool {
    match bound {
        std::ops::Bound::Included(upper) => matches!(
            compare_values(value, upper),
            Some(Ordering::Equal | Ordering::Less)
        ),
        std::ops::Bound::Excluded(upper) => compare_values(value, upper) == Some(Ordering::Less),
        std::ops::Bound::Unbounded => true,
    }
}

fn pull(
    db: &dyn ReadExecutionTarget,
    context: &Arc<RequestContext>,
    root: &mut PullNode,
) -> std::result::Result<Option<PulledRow>, BoundedExecutionError> {
    root.next(db, context)
}

fn planned_value_clone_bytes(value: &Value) -> std::result::Result<usize, BoundedExecutionError> {
    match value {
        Value::Text(value) => Ok(value.len()),
        Value::Vector(values) => checked_size_mul(
            values.len(),
            std::mem::size_of::<f32>(),
            "canonical vector clone",
        ),
        Value::Json(value) => planned_json_clone_bytes(value),
        Value::Null
        | Value::Bool(_)
        | Value::Int64(_)
        | Value::Float64(_)
        | Value::Uuid(_)
        | Value::Timestamp(_)
        | Value::TxId(_) => Ok(0),
    }
}

fn planned_json_clone_bytes(
    value: &serde_json::Value,
) -> std::result::Result<usize, BoundedExecutionError> {
    match value {
        serde_json::Value::String(value) => Ok(value.len()),
        serde_json::Value::Array(values) => {
            let mut bytes = checked_size_mul(
                values.len(),
                std::mem::size_of::<serde_json::Value>(),
                "canonical JSON clone",
            )?;
            for value in values {
                bytes = checked_size_add(
                    bytes,
                    planned_json_clone_bytes(value)?,
                    "canonical JSON clone",
                )?;
            }
            Ok(bytes)
        }
        serde_json::Value::Object(values) => {
            let mut bytes = checked_size_mul(
                values.len(),
                std::mem::size_of::<(String, serde_json::Value)>(),
                "canonical JSON clone",
            )?;
            for (key, value) in values {
                bytes = checked_size_add(bytes, key.len(), "canonical JSON clone")?;
                bytes = checked_size_add(
                    bytes,
                    planned_json_clone_bytes(value)?,
                    "canonical JSON clone",
                )?;
            }
            Ok(bytes)
        }
        serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::Number(_) => {
            Ok(0)
        }
    }
}

#[allow(dead_code)]
fn planned_values_clone_bytes(
    values: &[Value],
) -> std::result::Result<usize, BoundedExecutionError> {
    let mut bytes = checked_size_mul(
        values.len(),
        std::mem::size_of::<Value>(),
        "canonical row clone",
    )?;
    for value in values {
        bytes = checked_size_add(
            bytes,
            planned_value_clone_bytes(value)?,
            "canonical row clone",
        )?;
    }
    Ok(bytes)
}

#[allow(dead_code)]
fn planned_query_clone_bytes(
    result: &QueryResult,
) -> std::result::Result<usize, BoundedExecutionError> {
    let mut bytes = std::mem::size_of::<CanonicalQueryResult>();
    bytes = checked_size_add(
        bytes,
        checked_size_mul(
            result.columns.len(),
            std::mem::size_of::<String>(),
            "canonical columns",
        )?,
        "canonical columns",
    )?;
    for column in &result.columns {
        bytes = checked_size_add(bytes, column.len(), "canonical column")?;
    }
    bytes = checked_size_add(
        bytes,
        checked_size_mul(
            result.rows.len(),
            std::mem::size_of::<Vec<Value>>(),
            "canonical rows",
        )?,
        "canonical rows",
    )?;
    for row in &result.rows {
        bytes = checked_size_add(bytes, planned_values_clone_bytes(row)?, "canonical row")?;
    }
    bytes = checked_size_add(bytes, result.trace.physical_plan.len(), "canonical trace")?;
    if let Some(index) = result.trace.index_used.as_ref() {
        bytes = checked_size_add(bytes, index.len(), "canonical trace index")?;
    }
    bytes = checked_size_add(
        bytes,
        checked_size_mul(
            result.trace.predicates_pushed.len(),
            std::mem::size_of::<String>(),
            "canonical trace predicates",
        )?,
        "canonical trace",
    )?;
    for predicate in &result.trace.predicates_pushed {
        bytes = checked_size_add(bytes, predicate.len(), "canonical trace predicate")?;
    }
    bytes = checked_size_add(
        bytes,
        checked_size_mul(
            result.trace.indexes_considered.len(),
            std::mem::size_of::<crate::read_contract::CanonicalIndexCandidate>(),
            "canonical considered indexes",
        )?,
        "canonical trace",
    )?;
    for index in &result.trace.indexes_considered {
        bytes = checked_size_add(bytes, index.name.len(), "canonical considered index")?;
        bytes = checked_size_add(
            bytes,
            index.rejected_reason.len(),
            "canonical considered-index reason",
        )?;
    }
    if let Some(source) = result.trace.query_vector_source.as_ref() {
        bytes = checked_size_add(bytes, source.table.len(), "canonical vector source")?;
        bytes = checked_size_add(bytes, source.column.len(), "canonical vector source")?;
    }
    if let Some(cascade) = result.cascade.as_ref() {
        bytes = checked_size_add(
            bytes,
            checked_size_mul(
                cascade.dropped_indexes.len(),
                std::mem::size_of::<String>(),
                "canonical cascade",
            )?,
            "canonical cascade",
        )?;
        for index in &cascade.dropped_indexes {
            bytes = checked_size_add(bytes, index.len(), "canonical cascade")?;
        }
    }
    Ok(bytes)
}

#[allow(dead_code)]
fn canonical_query_capacity_bytes(
    result: &CanonicalQueryResult,
) -> std::result::Result<usize, BoundedExecutionError> {
    let mut bytes = checked_size_add(
        std::mem::size_of::<CanonicalQueryResult>(),
        strings_capacity_bytes(&result.columns, "canonical columns")?,
        "canonical query",
    )?;
    bytes = checked_size_add(
        bytes,
        checked_size_mul(
            result.rows.capacity(),
            std::mem::size_of::<Vec<Value>>(),
            "canonical rows",
        )?,
        "canonical query",
    )?;
    for row in &result.rows {
        bytes = checked_size_add(
            bytes,
            values_capacity_allocation_bytes(row)?,
            "canonical row",
        )?;
    }
    bytes = checked_size_add(
        bytes,
        result.trace.physical_plan.capacity(),
        "canonical trace",
    )?;
    bytes = checked_size_add(
        bytes,
        optional_string_capacity(&result.trace.index_used),
        "canonical trace",
    )?;
    bytes = checked_size_add(
        bytes,
        strings_capacity_bytes(
            &result.trace.predicates_pushed,
            "canonical trace predicates",
        )?,
        "canonical trace",
    )?;
    bytes = checked_size_add(
        bytes,
        checked_size_mul(
            result.trace.indexes_considered.capacity(),
            std::mem::size_of::<crate::read_contract::CanonicalIndexCandidate>(),
            "canonical considered indexes",
        )?,
        "canonical trace",
    )?;
    for candidate in &result.trace.indexes_considered {
        bytes = checked_size_add(bytes, candidate.name.capacity(), "canonical trace index")?;
        bytes = checked_size_add(
            bytes,
            candidate.rejected_reason.capacity(),
            "canonical trace rejection",
        )?;
    }
    if let Some(source) = result.trace.query_vector_source.as_ref() {
        bytes = checked_size_add(bytes, source.table.capacity(), "canonical vector source")?;
        bytes = checked_size_add(bytes, source.column.capacity(), "canonical vector source")?;
    }
    if let Some(cascade) = result.cascade.as_ref() {
        bytes = checked_size_add(
            bytes,
            strings_capacity_bytes(&cascade.dropped_indexes, "canonical cascade")?,
            "canonical cascade",
        )?;
    }
    Ok(bytes)
}

fn encoded_query_size(
    context: &Arc<RequestContext>,
    result: &QueryResult,
) -> std::result::Result<u64, BoundedExecutionError> {
    for row in &result.rows {
        if row.len() != result.columns.len() {
            return Err(Error::Other("canonical query row arity is invalid".to_string()).into());
        }
    }
    // Measured, not copied. The encoder writes into a counter that keeps
    // nothing, so a copy of the whole result would be a second live copy of
    // every row it holds -- remade after each row appended -- taken purely to
    // be looked at. Borrowed, this costs the rows nothing and the number is
    // the same number, because a borrowed slice and a `Vec` write identically.
    let measured = serialized_size(&crate::read_contract::CanonicalQueryResultView::from(
        result,
    ))?;
    u64::try_from(measured).map_err(|_| {
        RequestContext::limit_failure(ReadFailureLimit::ResultBytes, context.limits.result_bytes)
    })
}

fn encoded_page_size(
    context: &Arc<RequestContext>,
    page: &CursorPage,
) -> std::result::Result<u64, BoundedExecutionError> {
    page.validate()
        .map_err(|error| Error::Other(error.to_string()))?;
    let measured = serialized_size(page)?;
    u64::try_from(measured).map_err(|_| {
        RequestContext::limit_failure(
            ReadFailureLimit::CursorPageBytes,
            context.limits.cursor_page_bytes,
        )
    })
}

#[allow(dead_code)]
fn strip_internal_row_id(columns: &mut Vec<String>, rows: &mut [Vec<Value>]) {
    if let Some(position) = columns.iter().position(|column| column == "row_id") {
        columns.remove(position);
        for row in rows {
            if position < row.len() {
                row.remove(position);
            }
        }
    }
}

fn bounded_error_text(error: &BoundedExecutionError) -> String {
    match error {
        BoundedExecutionError::Engine(error) => error.to_string(),
        BoundedExecutionError::Refused(failure) => format!("{failure:?}"),
        BoundedExecutionError::Cancelled => "bounded read cancelled".to_string(),
        BoundedExecutionError::Unimplemented => {
            "bounded read implementation is unavailable".to_string()
        }
    }
}

fn deadline_clock_duration(clock: &dyn DeadlineClock, started_ms: u64) -> Duration {
    let elapsed_ms = clock.now_ms().saturating_sub(started_ms);
    Duration::from_millis(elapsed_ms)
}

fn bounded_query_outcome(
    result: &std::result::Result<BoundedExecutionResult, BoundedExecutionError>,
) -> QueryOutcome {
    match result {
        Ok(result) => QueryOutcome::Success {
            row_count: result.result.rows.len(),
        },
        Err(error) => QueryOutcome::Error {
            error: bounded_error_text(error),
        },
    }
}

/// The bytes two same-length result containers occupy at these capacities.
fn result_container_bytes(
    rows: usize,
    charges: usize,
) -> std::result::Result<usize, BoundedExecutionError> {
    checked_size_add(
        checked_size_mul(
            rows,
            std::mem::size_of::<Vec<Value>>(),
            "bounded result rows",
        )?,
        checked_size_mul(
            charges,
            std::mem::size_of::<usize>(),
            "bounded result row charges",
        )?,
        "bounded result containers",
    )
}

/// Make room for one more result row, paying for the room first.
///
/// The charge follows the answer as it is assembled rather than the ceiling
/// the caller permitted, so a read costs what it does. Growth is doubled so
/// the charge is re-stated a logarithmic number of times over a long result
/// rather than once per row.
fn grow_result_containers(
    context: &Arc<RequestContext>,
    rows: &mut Vec<Vec<Value>>,
    charges: &mut Vec<usize>,
    held_bytes: &mut usize,
) -> std::result::Result<(), BoundedExecutionError> {
    if rows.len() < rows.capacity() && charges.len() < charges.capacity() {
        return Ok(());
    }
    let target = rows
        .capacity()
        .max(charges.capacity())
        .max(4)
        .saturating_mul(2);
    let planned = result_container_bytes(target, target)?;
    let growth = planned.saturating_sub(*held_bytes);
    context.reserve(BoundedWorkSource::TableScan, growth)?;
    if rows
        .try_reserve_exact(target.saturating_sub(rows.len()))
        .is_err()
    {
        context.release(growth)?;
        return Err(Error::Other("bounded result-row allocation failed".to_string()).into());
    }
    if charges
        .try_reserve_exact(target.saturating_sub(charges.len()))
        .is_err()
    {
        context.release(growth)?;
        return Err(Error::Other("bounded result-charge allocation failed".to_string()).into());
    }
    let actual = result_container_bytes(rows.capacity(), charges.capacity())?;
    match actual.cmp(&planned) {
        std::cmp::Ordering::Greater => {
            context.reserve(BoundedWorkSource::TableScan, actual - planned)?;
        }
        std::cmp::Ordering::Less => context.release(planned - actual)?,
        std::cmp::Ordering::Equal => {}
    }
    *held_bytes = actual;
    Ok(())
}

/// Ceilings that refuse nothing.
///
/// The established embedding door promises an uncapped answer, so the read it
/// runs is not given limits to stop it. What it shares with a bounded read is
/// the KERNEL: one statement is planned once and drained once, whichever door
/// asked for it, so the two can never answer differently or count differently.
fn uncapped_ceilings() -> ReadLimits {
    ReadLimits {
        result_rows: u64::MAX,
        result_bytes: u64::MAX,
        work: u64::MAX,
        active_ms: u64::MAX,
        memory: u64::MAX,
        cursor_page_rows: u64::MAX,
        cursor_page_bytes: u64::MAX,
        cursor_idle_ms: u64::MAX,
        cursor_lifetime_ms: u64::MAX,
    }
}

/// Whether the shared kernel can answer this plan at all.
///
/// The kernel reads; a plan that writes, or that names a shape it has no
/// source for, goes to the executor as it always did. Asked before anything
/// is built, so a plan the kernel cannot take costs nothing to refuse.
pub(crate) fn kernel_can_answer(plan: &PhysicalPlan) -> bool {
    ensure_supported_plan(plan).is_ok()
}

/// Drain a plan the CALLER already chose, with no ceilings.
///
/// This is the established embedding door's half of the shared kernel: the
/// statement was planned by the caller, so nothing is planned again here, and
/// the answer is drained once from the same sources a bounded read draws from.
/// One planning, one drain -- which is what stops a public read from being
/// answered twice, once for the rows and once for the trace.
pub(crate) fn drain_chosen_plan(
    db: &dyn ReadExecutionTarget,
    plan: &PhysicalPlan,
    params: &HashMap<String, Value>,
    tx: Option<TxId>,
    snapshot: SnapshotId,
    clock: Arc<dyn DeadlineClock>,
    #[cfg(feature = "test-seams")] probe: Option<Arc<dyn BoundedExecutionProbe>>,
) -> std::result::Result<QueryResult, BoundedExecutionError> {
    let context = RequestContext::new(
        db,
        uncapped_ceilings(),
        ReadResumption::OneShot,
        clock,
        OwnerReadCancellation::new(),
        tx,
        #[cfg(feature = "test-seams")]
        probe,
    )?;
    // This door declared no ceilings, so a traversal's frontier is the store's
    // to govern: the read holds that reservation and grows it as each start is
    // resolved, never pricing a frontier before it exists.
    context.enable_store_frontier(db.bounded_read_accountant());
    let params = Arc::new(params.clone());
    let mut root = build_kernel(db, plan, Arc::clone(&params), snapshot, &context)?;
    // The answer's own column list is the demand every source below is filled
    // against; each node adds what its expressions name on the way down.
    {
        let named: std::collections::BTreeSet<String> = root.columns.iter().cloned().collect();
        declare_supplemented_columns(db, &mut root, &named);
    }
    #[cfg(feature = "test-seams")]
    crate::database::Database::observe_pull_kernel_entered_for_test();
    let mut result = QueryResult {
        columns: root.columns.clone(),
        rows: Vec::new(),
        rows_affected: 0,
        trace: root.trace.clone(),
        cascade: None,
    };
    let internal_row_id = result.columns.iter().position(|column| column == "row_id");
    if let Some(position) = internal_row_id {
        result.columns.remove(position);
    }
    while let Some(mut row) = pull(db, &context, &mut root)? {
        if let Some(position) = internal_row_id
            && position < row.values.len()
        {
            row.values.remove(position);
        }
        result.rows.push(row.values);
        context.release(row.retained_bytes)?;
    }
    #[cfg(feature = "test-seams")]
    crate::database::Database::observe_pull_kernel_drained_for_test();
    result.trace.physical_plan = root.published_plan();
    result.trace.rows_examined = context.rows_examined()?;
    Ok(result)
}

pub(super) fn execute(
    db: &dyn ReadExecutionTarget,
    sql: &str,
    params: &HashMap<String, Value>,
    limits: ReadLimits,
    clock: Arc<dyn DeadlineClock>,
    cancellation: OwnerReadCancellation,
    #[cfg(feature = "test-seams")] probe: Option<Arc<dyn BoundedExecutionProbe>>,
) -> std::result::Result<BoundedExecutionResult, BoundedExecutionError> {
    let statement = contextdb_parser::parse(sql)?;
    db.plugin().on_query(sql)?;
    let plugin_started_ms = clock.now_ms();
    let plugin_clock = Arc::clone(&clock);
    let withdrawn = cancellation.clone();
    let execution = (|| {
        let context = RequestContext::new(
            db,
            limits,
            ReadResumption::OneShot,
            clock,
            cancellation,
            db.active_read_transaction(),
            #[cfg(feature = "test-seams")]
            probe,
        )?;
        let Some((snapshot, _snapshot_registration)) =
            db.bounded_read_snapshot_registration(&withdrawn)?
        else {
            return Err(context.withdrawn_failure());
        };
        let statement_bytes = statement_capacity_bytes(&statement)?;
        context.reserve(BoundedWorkSource::TableScan, statement_bytes)?;
        // The resolved statement and the physical plan are built out of this
        // statement, so the statement's own size is what the request is
        // admitted for before either exists; the reservation is trued up to
        // what they hold as soon as they do. A request whose ceiling cannot
        // cover the shape it asked for is refused before the engine builds
        // that shape, not after.
        let planned_prepared_bytes = checked_size_mul(statement_bytes, 2, "bounded read plan")?;
        context.reserve(BoundedWorkSource::TableScan, planned_prepared_bytes)?;
        let (resolved, plan) = db.prepare_bounded_read_plan(&statement, params)?;
        let resolved_bytes = statement_capacity_bytes(&resolved)?;
        ensure_supported_plan(&plan)?;
        let plan_bytes = physical_plan_capacity_bytes(&plan)?;
        reconcile_new_reservation(
            &context,
            BoundedWorkSource::TableScan,
            planned_prepared_bytes,
            checked_size_add(resolved_bytes, plan_bytes, "bounded read plan")?,
        )?;
        drop(statement);
        context.release(statement_bytes)?;
        drop(resolved);
        context.release(resolved_bytes)?;

        let planned_params_bytes = params_retained_bytes(params)?;
        context.reserve(BoundedWorkSource::TableScan, planned_params_bytes)?;
        let params = Arc::new(params.clone());
        let params_bytes = params_retained_bytes(&params)?;
        reconcile_new_reservation(
            &context,
            BoundedWorkSource::TableScan,
            planned_params_bytes,
            params_bytes,
        )?;
        // The kernel is built out of the plan and holds a copy of every table
        // shape it reads, so the plan's own size is the floor the request is
        // admitted for before the kernel is materialized; the reservation is
        // trued up to what the kernel holds once it exists.
        context.reserve(BoundedWorkSource::TableScan, plan_bytes)?;
        let mut root = build_kernel(db, &plan, Arc::clone(&params), snapshot, &context)?;
        // The answer's own column list is the demand every source below is filled
        // against; each node adds what its expressions name on the way down.
        {
            let named: std::collections::BTreeSet<String> = root.columns.iter().cloned().collect();
            declare_supplemented_columns(db, &mut root, &named);
        }
        let root_bytes = pull_continuation_bytes(&root)?;
        reconcile_new_reservation(
            &context,
            BoundedWorkSource::TableScan,
            plan_bytes,
            root_bytes,
        )?;
        drop(plan);
        context.release(plan_bytes)?;

        let (result_columns, result_column_bytes) =
            clone_strings_with_reservation(&root.columns, &context, "bounded result columns")?;
        let planned_result_trace_bytes = query_trace_capacity_bytes(&root.trace)?;
        context.reserve(BoundedWorkSource::TableScan, planned_result_trace_bytes)?;
        let result_trace = root.trace.clone();
        let result_trace_bytes = query_trace_capacity_bytes(&result_trace)?;
        reconcile_new_reservation(
            &context,
            BoundedWorkSource::TableScan,
            planned_result_trace_bytes,
            result_trace_bytes,
        )?;
        let mut result = QueryResult {
            columns: result_columns,
            rows: Vec::new(),
            rows_affected: 0,
            trace: result_trace,
            cascade: None,
        };
        let internal_row_id = result.columns.iter().position(|column| column == "row_id");
        if let Some(position) = internal_row_id {
            result.columns.remove(position);
        }
        let retained_result_column_bytes =
            strings_capacity_bytes(&result.columns, "bounded result columns")?;
        reconcile_new_reservation(
            &context,
            BoundedWorkSource::TableScan,
            result_column_bytes,
            retained_result_column_bytes,
        )?;
        // The read is charged for the answer it is building, not for the
        // ceiling the caller allowed it. Sizing these containers to
        // `result_rows` up front would refuse a caller who only said "do not
        // truncate my answer" -- a generous row ceiling would be billed as
        // memory before a single row existed, and a one-row answer would
        // never come back. So the containers start empty and are charged
        // again each time they actually have to grow; a growth the request
        // cannot pay for is still refused before the allocation happens.
        let mut held_container_bytes = 0usize;
        let mut retained_rows = Vec::<usize>::new();

        loop {
            let result_rows = u64::try_from(result.rows.len()).map_err(|_| {
                RequestContext::limit_failure(ReadFailureLimit::ResultRows, limits.result_rows)
            })?;
            if result_rows >= limits.result_rows {
                match root.has_more_hint() {
                    Some(false) => break,
                    Some(true) => {
                        return Err(RequestContext::limit_failure(
                            ReadFailureLimit::ResultRows,
                            limits.result_rows,
                        ));
                    }
                    None => {
                        if pull(db, &context, &mut root)?.is_some() {
                            return Err(RequestContext::limit_failure(
                                ReadFailureLimit::ResultRows,
                                limits.result_rows,
                            ));
                        }
                        break;
                    }
                }
            }

            let Some(mut row) = pull(db, &context, &mut root)? else {
                break;
            };
            if let Some(position) = internal_row_id
                && position < row.values.len()
            {
                row.values.remove(position);
            }
            grow_result_containers(
                &context,
                &mut result.rows,
                &mut retained_rows,
                &mut held_container_bytes,
            )?;
            retained_rows.push(row.retained_bytes);
            result.rows.push(row.values);
            result.trace.physical_plan = root.published_plan();
            result.trace.rows_examined = context.rows_examined()?;
            let encoded = encoded_query_size(&context, &result)?;
            if encoded > limits.result_bytes {
                return Err(RequestContext::limit_failure(
                    ReadFailureLimit::ResultBytes,
                    limits.result_bytes,
                ));
            }
            context.note_result_shape(result.rows.len(), encoded);
        }

        result.trace.physical_plan = root.published_plan();
        result.trace.rows_examined = context.rows_examined()?;
        let encoded = encoded_query_size(&context, &result)?;
        if encoded > limits.result_bytes {
            return Err(RequestContext::limit_failure(
                ReadFailureLimit::ResultBytes,
                limits.result_bytes,
            ));
        }
        context.set_encoded_bytes(encoded);
        context.check_final_boundary()?;
        for bytes in retained_rows {
            context.release(bytes)?;
        }
        context.release(held_container_bytes)?;
        context.check_final_boundary()?;
        // Every read says what it did at least once, however short it was.
        context.flush_final_progress();
        Ok(BoundedExecutionResult {
            result,
            telemetry: context.telemetry(),
        })
    })();
    let execution = execution.map_err(|error| work_refusal_naming_the_statement(sql, error));
    let plugin_outcome = bounded_query_outcome(&execution);
    db.plugin().post_query(
        sql,
        deadline_clock_duration(plugin_clock.as_ref(), plugin_started_ms),
        &plugin_outcome,
    );
    execution
}

pub(super) struct CursorState {
    database: Arc<dyn ReadExecutionTarget>,
    _snapshot: SnapshotId,
    _snapshot_registration: Box<dyn Send + Sync>,
    /// The idle window this cursor's registration is judged by. Stamped
    /// beside `last_used_ms` on every page handed back, so removal reads the
    /// same "still entitled to another page" answer `check_expiry` gives.
    idle_window: Arc<CursorIdleWindow>,
    context: Arc<RequestContext>,
    root: PullNode,
    replay: VecDeque<PulledRow>,
    visible_columns: Vec<String>,
    internal_row_id: Option<usize>,
    sql: String,
    created_ms: u64,
    last_used_ms: u64,
    plugin_started_ms: u64,
    plugin_finished: bool,
    published_rows: usize,
    /// The page scaffolding a live cursor holds between fetches.
    ///
    /// A cursor costs its scaffolding plus whatever rows it is still holding.
    /// The scaffolding is the flat part -- a page's columns, its row capacity,
    /// and the replay container are the same size for equally-shaped pages, so
    /// the previous page's share is given back as this page's is taken, and a
    /// build that is interrupted keeps it. `held_page_base` is that share.
    ///
    /// The rows are NOT flat, and pretending they were is what made the quoted
    /// cost untrue: a row that did not fit a page, or that a cancelled build
    /// handed back, stays alive in the replay until some later page publishes
    /// it, so it is charged for exactly as long as this cursor owns it. Every
    /// terminal path settles the request to scaffolding plus the rows still in
    /// the replay, which returns the rows that left with the answer and keeps
    /// the ones that did not.
    held_page_base: usize,
}

/// What a page build must leave unchanged, measured before it changes anything.
#[derive(Clone, Copy)]
struct CursorPageAnchor {
    /// The request's standing reservations plus the page scaffolding it holds
    /// right now -- everything except the two parts that move.
    fixed_bytes: usize,
    /// The scaffolding this build is about to give back.
    released_base: usize,
}

impl CursorState {
    fn check_expiry(&self) -> std::result::Result<(), BoundedExecutionError> {
        let now = self.context.clock.now_ms();
        let lifetime = now
            .checked_sub(self.created_ms)
            .ok_or_else(|| Error::Other("bounded cursor clock moved backwards".to_string()))?;
        if lifetime > self.context.limits.cursor_lifetime_ms {
            return Err(BoundedExecutionError::Refused(ReadFailure::cursor_expired(
                CursorExpiryKind::Lifetime,
            )));
        }
        let idle = now
            .checked_sub(self.last_used_ms)
            .ok_or_else(|| Error::Other("bounded cursor clock moved backwards".to_string()))?;
        if idle > self.context.limits.cursor_idle_ms {
            return Err(BoundedExecutionError::Refused(ReadFailure::cursor_expired(
                CursorExpiryKind::Idle,
            )));
        }
        Ok(())
    }

    fn check_lifetime(&self) -> std::result::Result<(), BoundedExecutionError> {
        let now = self.context.clock.now_ms();
        let lifetime = now
            .checked_sub(self.created_ms)
            .ok_or_else(|| Error::Other("bounded cursor clock moved backwards".to_string()))?;
        if lifetime > self.context.limits.cursor_lifetime_ms {
            return Err(BoundedExecutionError::Refused(ReadFailure::cursor_expired(
                CursorExpiryKind::Lifetime,
            )));
        }
        Ok(())
    }

    fn finish_plugin_success(&mut self) {
        if self.plugin_finished {
            return;
        }
        self.database.plugin().post_query(
            &self.sql,
            deadline_clock_duration(self.context.clock.as_ref(), self.plugin_started_ms),
            &QueryOutcome::Success {
                row_count: self.published_rows,
            },
        );
        self.plugin_finished = true;
    }

    fn finish_plugin_error(&mut self, error: &BoundedExecutionError) {
        if self.plugin_finished {
            return;
        }
        self.database.plugin().post_query(
            &self.sql,
            deadline_clock_duration(self.context.clock.as_ref(), self.plugin_started_ms),
            &QueryOutcome::Error {
                error: bounded_error_text(error),
            },
        );
        self.plugin_finished = true;
    }

    fn page_bytes_refusal(
        &self,
        required: u64,
    ) -> std::result::Result<BoundedExecutionError, BoundedExecutionError> {
        const SETTING_PREFIX: &str = "effective cursor_page_bytes >= ";
        const REMEDY_COMMAND: &str = "select fewer columns";
        let required_setting_len = checked_size_add(
            SETTING_PREFIX.len(),
            decimal_digits_u64(required),
            "cursor-page remedy",
        )?;
        let planned_bytes = checked_size_add(
            checked_size_add(required_setting_len, self.sql.len(), "cursor-page remedy")?,
            REMEDY_COMMAND.len(),
            "cursor-page remedy",
        )?;
        self.context
            .reserve(BoundedWorkSource::TableScan, planned_bytes)?;
        let mut required_setting = String::new();
        let mut statement = String::new();
        let mut remedy_command = String::new();
        if required_setting
            .try_reserve_exact(required_setting_len)
            .is_err()
            || statement.try_reserve_exact(self.sql.len()).is_err()
            || remedy_command
                .try_reserve_exact(REMEDY_COMMAND.len())
                .is_err()
        {
            self.context.release(planned_bytes)?;
            return Err(
                Error::Other("bounded cursor-page remedy allocation failed".to_string()).into(),
            );
        }
        required_setting.push_str(SETTING_PREFIX);
        if write!(&mut required_setting, "{required}").is_err() {
            self.context.release(planned_bytes)?;
            return Err(
                Error::Other("bounded cursor-page remedy formatting failed".to_string()).into(),
            );
        }
        statement.push_str(&self.sql);
        remedy_command.push_str(REMEDY_COMMAND);
        let actual_bytes = match (|| {
            checked_size_add(
                checked_size_add(
                    required_setting.capacity(),
                    statement.capacity(),
                    "cursor-page remedy",
                )?,
                remedy_command.capacity(),
                "cursor-page remedy",
            )
        })() {
            Ok(bytes) => bytes,
            Err(error) => {
                self.context.release(planned_bytes)?;
                return Err(error);
            }
        };
        reconcile_new_reservation(
            &self.context,
            BoundedWorkSource::TableScan,
            planned_bytes,
            actual_bytes,
        )?;
        let refusal = BoundedExecutionError::Refused(ReadFailure::owner_limit_exceeded(
            OwnerLimitExceededDetail {
                limit: ReadFailureLimit::CursorPageBytes,
                value: self.context.limits.cursor_page_bytes,
                required: Some(RequiredBytesSetting {
                    required_bytes: required,
                    required_setting,
                }),
                statement: Some(StatementRemedy {
                    statement,
                    remedy_command,
                }),
            },
        ));
        self.context.release(actual_bytes)?;
        Ok(refusal)
    }

    fn reserve_replay_slot(&mut self) -> std::result::Result<(), BoundedExecutionError> {
        if self.replay.len() < self.replay.capacity() {
            return Ok(());
        }
        Err(Error::Other(
            "bounded cursor replay exceeded the capacity admitted for this page".to_string(),
        )
        .into())
    }

    fn replay_push_front(
        &mut self,
        row: PulledRow,
    ) -> std::result::Result<(), BoundedExecutionError> {
        self.reserve_replay_slot()?;
        self.replay.push_front(row);
        Ok(())
    }

    fn replay_push_back(
        &mut self,
        row: PulledRow,
    ) -> std::result::Result<(), BoundedExecutionError> {
        self.reserve_replay_slot()?;
        self.replay.push_back(row);
        Ok(())
    }

    fn restore_page_rows(
        &mut self,
        page: &mut CursorPage,
        retained: &mut Vec<usize>,
    ) -> std::result::Result<(), BoundedExecutionError> {
        while !page.rows.is_empty() && !retained.is_empty() {
            if self.replay.len() >= self.replay.capacity() {
                return Err(Error::Other(
                    "bounded cursor replay capacity cannot restore its cancelled page".to_string(),
                )
                .into());
            }
            let values = page.rows.pop().ok_or_else(|| {
                Error::Other("bounded cursor lost a cancelled page row".to_string())
            })?;
            let retained_bytes = retained.pop().ok_or_else(|| {
                Error::Other("bounded cursor lost a cancelled page charge".to_string())
            })?;
            self.replay.push_front(PulledRow {
                values,
                retained_bytes,
            });
        }
        if !page.rows.is_empty() || !retained.is_empty() {
            return Err(
                Error::Other("bounded cursor page rows and charges diverged".to_string()).into(),
            );
        }
        Ok(())
    }

    fn pull_visible_row(
        &mut self,
    ) -> std::result::Result<Option<PulledRow>, BoundedExecutionError> {
        if let Some(row) = self.replay.pop_front() {
            return Ok(Some(row));
        }
        let Some(mut row) = pull(self.database.as_ref(), &self.context, &mut self.root)? else {
            return Ok(None);
        };
        if let Some(position) = self.internal_row_id
            && position < row.values.len()
        {
            row.values.remove(position);
        }
        Ok(Some(row))
    }

    fn has_more_after_page(&mut self) -> std::result::Result<bool, BoundedExecutionError> {
        if !self.replay.is_empty() {
            return Ok(true);
        }
        match self.root.has_more_hint() {
            Some(has_more) => Ok(has_more),
            None if self.context.can_charge_more_work() => {
                let Some(row) = self.pull_visible_row()? else {
                    return Ok(false);
                };
                self.replay_push_back(row)?;
                Ok(true)
            }
            None => Ok(true),
        }
    }

    /// Settle this cursor to what it is genuinely still holding.
    ///
    /// A page build moves the charge in every direction at once. The cursor
    /// exchanges one page's scaffolding for the next. The rows that leave with
    /// the answer stop being its cost -- but WHO gave them back differs by
    /// source: a sorted read hands its rows over already un-charged, because
    /// the sort released them as it drained, while a graph walk's rows are
    /// still charged when they are published. And the SOURCE ITSELF grows as
    /// it reads: a traversal's frontier, visited set and adjacency cursor all
    /// expand deeper into a walk, through this same request.
    ///
    /// So neither "release the rows that left" nor "come home to the figure
    /// you started from" is right on its own -- the first double-releases a
    /// source that already gave its rows back, the second hands back a growing
    /// source's own bytes and leaves it to over-release when it finally runs
    /// dry. What holds for every source is this: the parts that do not move
    /// -- the request's standing reservations and this page's scaffolding --
    /// are anchored, and the parts that do move are measured where they live,
    /// the continuation through `pull_continuation_bytes` and the deferred
    /// rows through the replay itself.
    /// What this cursor is holding right now, part by part -- the same walk
    /// the settlement above is computed from.
    #[cfg(feature = "test-seams")]
    pub(super) fn continuation_bytes(
        &self,
    ) -> std::result::Result<ContinuationBytes, BoundedExecutionError> {
        continuation_bytes_breakdown(&self.root)
    }

    fn settle_retained_charge(
        &mut self,
        anchor: CursorPageAnchor,
        page_base_bytes: usize,
    ) -> std::result::Result<(), BoundedExecutionError> {
        let continuation_now = pull_continuation_bytes(&self.root)?;
        let carried_now = self.carried_replay_bytes()?;
        let fixed = anchor
            .fixed_bytes
            .saturating_sub(anchor.released_base)
            .saturating_add(page_base_bytes);
        let target = checked_size_add(
            checked_size_add(fixed, continuation_now, "cursor retained charge")?,
            carried_now,
            "cursor retained charge",
        )?;
        let held = self.context.held_bytes();
        match held.cmp(&target) {
            std::cmp::Ordering::Greater => self.context.release(held - target),
            std::cmp::Ordering::Less => self
                .context
                .reserve(BoundedWorkSource::TableScan, target - held),
            std::cmp::Ordering::Equal => Ok(()),
        }
    }

    /// Measure the parts of this cursor's charge that a page build must leave
    /// where it found them, before the build touches any of them.
    fn page_anchor(&self) -> std::result::Result<CursorPageAnchor, BoundedExecutionError> {
        let continuation = pull_continuation_bytes(&self.root)?;
        let carried = self.carried_replay_bytes()?;
        Ok(CursorPageAnchor {
            fixed_bytes: self
                .context
                .held_bytes()
                .saturating_sub(continuation)
                .saturating_sub(carried),
            released_base: self.held_page_base,
        })
    }

    fn carried_replay_bytes(&self) -> std::result::Result<usize, BoundedExecutionError> {
        self.replay.iter().try_fold(0usize, |sum, row| {
            checked_size_add(sum, row.retained_bytes, "cursor replay rows")
        })
    }

    fn fetch_page(
        &mut self,
        rows: NonZeroUsize,
    ) -> std::result::Result<BoundedCursorFetch, BoundedExecutionError> {
        let row_limit = u64::try_from(rows.get()).map_err(|_| {
            RequestContext::limit_failure(
                ReadFailureLimit::ResultRows,
                self.context.limits.result_rows,
            )
        })?;
        if row_limit > self.context.limits.result_rows {
            return Err(RequestContext::limit_failure(
                ReadFailureLimit::ResultRows,
                self.context.limits.result_rows,
            ));
        }
        let row_capacity = rows.get();
        // What this cursor holds before it touches anything. A fetch that is
        // INTERRUPTED publishes nothing, so nothing about the cursor may have
        // changed when it returns -- every row it had assembled goes back into
        // the replay and every byte it took for the attempt goes back here.
        // The figure is taken before the page's own scaffolding is exchanged,
        // so "as it was" means exactly that.
        let anchor = self.page_anchor()?;
        // This page's scaffolding replaces the last page's rather than adding
        // to it: a cursor that is handing back equally-shaped pages costs the
        // same while it builds one as it does while it holds one.
        let released_base = anchor.released_base;
        let _ = std::mem::take(&mut self.held_page_base);
        if released_base > 0 {
            self.context.release(released_base)?;
        }
        let (page_columns, page_column_bytes) = clone_strings_with_reservation(
            &self.visible_columns,
            &self.context,
            "cursor-page columns",
        )?;
        let planned_row_state_bytes = checked_size_add(
            checked_size_mul(
                row_capacity,
                std::mem::size_of::<Vec<Value>>(),
                "cursor-page row capacity",
            )?,
            checked_size_mul(
                row_capacity,
                std::mem::size_of::<usize>(),
                "cursor-page retained capacity",
            )?,
            "cursor-page row state",
        )?;
        self.context
            .reserve(BoundedWorkSource::TableScan, planned_row_state_bytes)?;
        let mut page_rows = Vec::new();
        if page_rows.try_reserve_exact(row_capacity).is_err() {
            self.context.release(planned_row_state_bytes)?;
            self.context.release(page_column_bytes)?;
            return Err(
                Error::Other("bounded cursor-page row allocation failed".to_string()).into(),
            );
        }
        let mut retained = Vec::<usize>::new();
        if retained.try_reserve_exact(row_capacity).is_err() {
            self.context.release(planned_row_state_bytes)?;
            self.context.release(page_column_bytes)?;
            return Err(
                Error::Other("bounded cursor-page charge allocation failed".to_string()).into(),
            );
        }
        let actual_row_state_bytes = checked_size_add(
            checked_size_mul(
                page_rows.capacity(),
                std::mem::size_of::<Vec<Value>>(),
                "cursor-page row capacity",
            )?,
            checked_size_mul(
                retained.capacity(),
                std::mem::size_of::<usize>(),
                "cursor-page retained capacity",
            )?,
            "cursor-page row state",
        )?;
        reconcile_new_reservation(
            &self.context,
            BoundedWorkSource::TableScan,
            planned_row_state_bytes,
            actual_row_state_bytes,
        )?;
        // The replay is this page's scaffolding too: it holds what this page
        // pulled but did not publish, so it is shaped by this page's row limit
        // and by whatever an interrupted earlier page handed back, never by
        // the largest result the limits would ever admit. Equally-shaped pages
        // therefore take back exactly what the previous page released.
        let replay_capacity = row_capacity
            .checked_add(1)
            .ok_or_else(|| {
                RequestContext::limit_failure(
                    ReadFailureLimit::ResultRows,
                    self.context.limits.result_rows,
                )
            })?
            .max(self.replay.len());
        let planned_replay_bytes = checked_size_mul(
            replay_capacity,
            std::mem::size_of::<PulledRow>(),
            "cursor replay capacity",
        )?;
        self.context
            .reserve(BoundedWorkSource::TableScan, planned_replay_bytes)?;
        if self.replay.capacity() < replay_capacity {
            let additional = replay_capacity.saturating_sub(self.replay.len());
            if self.replay.try_reserve_exact(additional).is_err() {
                self.context.release(planned_replay_bytes)?;
                self.context.release(actual_row_state_bytes)?;
                self.context.release(page_column_bytes)?;
                return Err(
                    Error::Other("bounded cursor replay allocation failed".to_string()).into(),
                );
            }
        } else if self.replay.capacity() > replay_capacity {
            self.replay.shrink_to(replay_capacity);
        }
        let replay_bytes = checked_size_mul(
            self.replay.capacity(),
            std::mem::size_of::<PulledRow>(),
            "cursor replay capacity",
        )?;
        reconcile_new_reservation(
            &self.context,
            BoundedWorkSource::TableScan,
            planned_replay_bytes,
            replay_bytes,
        )?;
        let page_base_bytes = checked_size_add(
            checked_size_add(
                page_column_bytes,
                actual_row_state_bytes,
                "cursor-page state",
            )?,
            replay_bytes,
            "cursor-page state",
        )?;
        let mut page = CursorPage {
            columns: page_columns,
            rows: page_rows,
            has_more: true,
        };
        let assembled = (|| {
            while u64::try_from(page.rows.len()).map_err(|_| {
                RequestContext::limit_failure(
                    ReadFailureLimit::ResultRows,
                    self.context.limits.result_rows,
                )
            })? < row_limit
            {
                let Some(row) = self.pull_visible_row()? else {
                    // Out of rows for THIS page is not always out of input:
                    // an aggregate that has spent this fetch's work budget
                    // keeps its partial answer and says it still has more,
                    // so the page goes back empty rather than carrying a
                    // total that has not finished adding up.
                    page.has_more = matches!(self.root.has_more_hint(), Some(true));
                    break;
                };
                retained.push(row.retained_bytes);
                page.rows.push(row.values);
                page.has_more =
                    !self.replay.is_empty() || !matches!(self.root.has_more_hint(), Some(false));
                let encoded = encoded_page_size(&self.context, &page)?;
                self.context.note_result_shape(page.rows.len(), encoded);
                if encoded > self.context.limits.cursor_page_bytes {
                    let values = page.rows.pop().ok_or_else(|| {
                        Error::Other("bounded cursor lost its byte-stopped row".to_string())
                    })?;
                    let retained_bytes = retained.pop().ok_or_else(|| {
                        Error::Other("bounded cursor lost its byte-stopped charge".to_string())
                    })?;
                    self.replay_push_front(PulledRow {
                        values,
                        retained_bytes,
                    })?;
                    if page.rows.is_empty() {
                        return Err(self.page_bytes_refusal(encoded)?);
                    }
                    page.has_more = true;
                    break;
                }
                if !page.has_more {
                    break;
                }
            }
            if u64::try_from(page.rows.len()).map_err(|_| {
                RequestContext::limit_failure(
                    ReadFailureLimit::ResultRows,
                    self.context.limits.result_rows,
                )
            })? == row_limit
            {
                page.has_more = self.has_more_after_page()?;
            }
            let encoded = encoded_page_size(&self.context, &page)?;
            if encoded > self.context.limits.cursor_page_bytes {
                return Err(self.page_bytes_refusal(encoded)?);
            }
            self.context.set_encoded_bytes(encoded);
            Ok(encoded)
        })();
        let encoded = match assembled {
            Ok(encoded) => encoded,
            Err(BoundedExecutionError::Cancelled) => {
                // Interrupted while the page was still being assembled. The
                // cursor lives on, still holds a page's worth of scaffolding,
                // and has just taken every assembled row back into the replay,
                // so all of those rows stay charged.
                self.restore_page_rows(&mut page, &mut retained)?;
                self.held_page_base = page_base_bytes;
                self.settle_retained_charge(anchor, page_base_bytes)?;
                return Err(BoundedExecutionError::Cancelled);
            }
            Err(error) => return Err(error),
        };
        let publication_boundary = (|| {
            self.check_lifetime()?;
            self.context.check_final_boundary()?;
            let telemetry = self.context.telemetry();
            if telemetry.encoded_bytes != encoded {
                return Err(Error::Other(
                    "bounded cursor encoded-byte telemetry changed before publication".to_string(),
                )
                .into());
            }
            self.check_lifetime()?;
            self.context.check_final_boundary()?;
            Ok(telemetry)
        })();
        let telemetry = match publication_boundary {
            Ok(telemetry) => telemetry,
            Err(BoundedExecutionError::Cancelled) => {
                // Interrupted at the publication boundary. Nothing was
                // published, every assembled row is back in the replay, and
                // the cursor comes home to exactly what it held before this
                // fetch touched anything.
                self.restore_page_rows(&mut page, &mut retained)?;
                self.held_page_base = page_base_bytes;
                self.settle_retained_charge(anchor, page_base_bytes)?;
                return Err(BoundedExecutionError::Cancelled);
            }
            Err(error) => return Err(error),
        };
        let published = page.rows.len();
        // The page's rows leave with the answer, so they stop being this
        // cursor's cost; the scaffolding stays, to be given back and retaken by
        // the next page, and the rows the replay still owns stay charged.
        retained.clear();
        self.held_page_base = page_base_bytes;
        self.settle_retained_charge(anchor, page_base_bytes)?;
        self.published_rows = self
            .published_rows
            .checked_add(published)
            .ok_or_else(|| Error::Other("bounded cursor row counter overflow".to_string()))?;
        self.last_used_ms = self.context.clock.now_ms();
        self.idle_window.touch(self.last_used_ms);
        // The page is published, so this fetch is over: whatever it examined
        // and assembled is told now, even if it never crossed an interval.
        self.context.flush_final_progress();
        Ok(BoundedCursorFetch { page, telemetry })
    }
}

impl Drop for CursorState {
    fn drop(&mut self) {
        if self.plugin_finished {
            return;
        }
        self.database.plugin().post_query(
            &self.sql,
            deadline_clock_duration(self.context.clock.as_ref(), self.plugin_started_ms),
            &QueryOutcome::Error {
                error: "bounded cursor dropped before completion".to_string(),
            },
        );
        self.plugin_finished = true;
    }
}

fn default_page_rows(
    limits: ReadLimits,
) -> std::result::Result<NonZeroUsize, BoundedExecutionError> {
    let rows = usize::try_from(limits.cursor_page_rows).map_err(|_| {
        RequestContext::limit_failure(ReadFailureLimit::ResultRows, limits.result_rows)
    })?;
    NonZeroUsize::new(rows).ok_or_else(|| {
        RequestContext::limit_failure(ReadFailureLimit::ResultRows, limits.result_rows)
    })
}

pub(super) fn open_cursor(
    db: Arc<dyn ReadExecutionTarget>,
    sql: &str,
    params: &HashMap<String, Value>,
    limits: ReadLimits,
    clock: Arc<dyn DeadlineClock>,
    cancellation: OwnerReadCancellation,
    #[cfg(feature = "test-seams")] probe: Option<Arc<dyn BoundedExecutionProbe>>,
) -> std::result::Result<BoundedCursorOpen, BoundedExecutionError> {
    let statement = contextdb_parser::parse(sql)?;
    db.plugin().on_query(sql)?;
    let plugin_started_ms = clock.now_ms();
    let withdrawn = cancellation.clone();
    let prepared = (|| {
        let context = RequestContext::new(
            db.as_ref(),
            limits,
            ReadResumption::AcrossFetches,
            Arc::clone(&clock),
            cancellation,
            db.active_read_transaction(),
            #[cfg(feature = "test-seams")]
            probe,
        )?;
        let Some((snapshot, idle_window, snapshot_registration)) = db
            .bounded_cursor_snapshot_registration(
                Arc::clone(&clock),
                limits.cursor_idle_ms,
                &withdrawn,
            )?
        else {
            return Err(context.withdrawn_failure());
        };
        let statement_bytes = statement_capacity_bytes(&statement)?;
        context.reserve(BoundedWorkSource::TableScan, statement_bytes)?;
        // The resolved statement and the physical plan are built out of this
        // statement, so the statement's own size is what the request is
        // admitted for before either exists; the reservation is trued up to
        // what they hold as soon as they do. A request whose ceiling cannot
        // cover the shape it asked for is refused before the engine builds
        // that shape, not after.
        let planned_prepared_bytes = checked_size_mul(statement_bytes, 2, "bounded read plan")?;
        context.reserve(BoundedWorkSource::TableScan, planned_prepared_bytes)?;
        let (resolved, plan) = db.prepare_bounded_read_plan(&statement, params)?;
        let resolved_bytes = statement_capacity_bytes(&resolved)?;
        ensure_supported_plan(&plan)?;
        let plan_bytes = physical_plan_capacity_bytes(&plan)?;
        reconcile_new_reservation(
            &context,
            BoundedWorkSource::TableScan,
            planned_prepared_bytes,
            checked_size_add(resolved_bytes, plan_bytes, "bounded read plan")?,
        )?;
        drop(statement);
        context.release(statement_bytes)?;
        drop(resolved);
        context.release(resolved_bytes)?;
        context.reserve(
            BoundedWorkSource::TableScan,
            std::mem::size_of::<BoundedCursorExecutionState>(),
        )?;
        let planned_params_bytes = params_retained_bytes(params)?;
        context.reserve(BoundedWorkSource::TableScan, planned_params_bytes)?;
        let params = Arc::new(params.clone());
        let params_bytes = params_retained_bytes(&params)?;
        reconcile_new_reservation(
            &context,
            BoundedWorkSource::TableScan,
            planned_params_bytes,
            params_bytes,
        )?;
        // The kernel is built out of the plan and holds a copy of every table
        // shape it reads, so the plan's own size is the floor the request is
        // admitted for before the kernel is materialized; the reservation is
        // trued up to what the kernel holds once it exists.
        context.reserve(BoundedWorkSource::TableScan, plan_bytes)?;
        let mut root = build_kernel(db.as_ref(), &plan, params, snapshot, &context)?;
        // Every door that builds a kernel declares the answer's demand, or the
        // sources below it fill nothing and a column the answer names comes
        // back empty on that door alone.
        {
            let named: std::collections::BTreeSet<String> = root.columns.iter().cloned().collect();
            declare_supplemented_columns(db.as_ref(), &mut root, &named);
        }
        let root_bytes = pull_continuation_bytes(&root)?;
        reconcile_new_reservation(
            &context,
            BoundedWorkSource::TableScan,
            plan_bytes,
            root_bytes,
        )?;
        drop(plan);
        context.release(plan_bytes)?;
        let internal_row_id = root.columns.iter().position(|column| column == "row_id");
        let (mut visible_columns, visible_column_bytes) =
            clone_strings_with_reservation(&root.columns, &context, "cursor columns")?;
        if let Some(position) = internal_row_id {
            visible_columns.remove(position);
        }
        let retained_visible_column_bytes =
            strings_capacity_bytes(&visible_columns, "cursor columns")?;
        reconcile_new_reservation(
            &context,
            BoundedWorkSource::TableScan,
            visible_column_bytes,
            retained_visible_column_bytes,
        )?;
        // The replay carries rows a page did not publish into the next page,
        // so it is shaped by the page being built, not by the largest result
        // the limits would ever allow. It is taken and given back inside
        // `fetch_page` alongside the rest of the page's scaffolding; a cursor
        // that has not built a page yet holds no replay at all.
        let replay = VecDeque::new();
        let planned_sql_bytes = sql.len();
        context.reserve(BoundedWorkSource::TableScan, planned_sql_bytes)?;
        let mut retained_sql = String::new();
        if retained_sql.try_reserve_exact(sql.len()).is_err() {
            context.release(planned_sql_bytes)?;
            return Err(Error::Other("bounded cursor SQL allocation failed".to_string()).into());
        }
        retained_sql.push_str(sql);
        let sql_bytes = retained_sql.capacity();
        reconcile_new_reservation(
            &context,
            BoundedWorkSource::TableScan,
            planned_sql_bytes,
            sql_bytes,
        )?;
        let now = clock.now_ms();
        Ok(CursorState {
            database: Arc::clone(&db),
            _snapshot: snapshot,
            _snapshot_registration: snapshot_registration,
            idle_window,
            context,
            root,
            replay,
            visible_columns,
            internal_row_id,
            sql: retained_sql,
            created_ms: now,
            last_used_ms: now,
            plugin_started_ms,
            plugin_finished: false,
            published_rows: 0,
            held_page_base: 0,
        })
    })();
    let mut state = match prepared {
        Ok(state) => state,
        Err(error) => {
            let error = work_refusal_naming_the_statement(sql, error);
            db.plugin().post_query(
                sql,
                deadline_clock_duration(clock.as_ref(), plugin_started_ms),
                &QueryOutcome::Error {
                    error: bounded_error_text(&error),
                },
            );
            return Err(error);
        }
    };
    let first_page_rows = match default_page_rows(limits) {
        Ok(rows) => rows,
        Err(error) => {
            state.finish_plugin_error(&error);
            return Err(error);
        }
    };
    let first = match state.fetch_page(first_page_rows) {
        Ok(first) => first,
        Err(error) => {
            let error = work_refusal_naming_the_statement(sql, error);
            state.finish_plugin_error(&error);
            return Err(error);
        }
    };
    let lifecycle = if first.page.has_more {
        BoundedCursorLifecycle::Open(state)
    } else {
        let columns = first.page.columns.clone();
        state.finish_plugin_success();
        BoundedCursorLifecycle::Exhausted(columns)
    };
    Ok(BoundedCursorOpen {
        cursor: BoundedCursorHandle {
            _state: Arc::new(BoundedCursorExecutionState {
                lifecycle: Mutex::new(lifecycle),
            }),
        },
        first_page: first.page,
        telemetry: first.telemetry,
    })
}

/// A work refusal names the statement it stopped and the one move that
/// answers it inside the same ceiling.
///
/// A caller told only "the work ceiling is one thousand" learns nothing: the
/// number is theirs already. What they can act on is which statement reached
/// it and that the same statement, opened as a cursor, answers inside the
/// ceiling they declared. Attached where the statement text is known; a
/// refusal that already names one is left exactly as it is.
fn work_refusal_naming_the_statement(
    sql: &str,
    error: BoundedExecutionError,
) -> BoundedExecutionError {
    const REMEDY_COMMAND: &str =
        "open this statement as a cursor and fetch until it reports no more";
    let BoundedExecutionError::Refused(failure) = &error else {
        return error;
    };
    let ReadFailureDetail::OwnerLimitExceeded(detail) = failure.detail() else {
        return error;
    };
    if detail.limit != ReadFailureLimit::Work || detail.statement.is_some() {
        return error;
    }
    BoundedExecutionError::Refused(ReadFailure::owner_limit_exceeded(
        OwnerLimitExceededDetail {
            limit: detail.limit,
            value: detail.value,
            required: detail.required.clone(),
            statement: Some(StatementRemedy {
                statement: sql.to_string(),
                remedy_command: REMEDY_COMMAND.to_string(),
            }),
        },
    ))
}

fn cursor_not_found() -> BoundedExecutionError {
    match ReadFailure::new(ReadFailureKind::CursorNotFound, ReadFailureDetail::None) {
        Ok(failure) => BoundedExecutionError::Refused(failure),
        Err(error) => BoundedExecutionError::Engine(Error::Other(error.to_string())),
    }
}

/// Whether this cursor is still one a caller can ask anything of.
///
/// A drained cursor counts: it answers one empty page and closes clean. A
/// RELEASED one does not -- the read behind it is over, whether a close ended
/// it, a refusal did, or it expired.
pub(super) fn cursor_is_live(cursor: &BoundedCursorHandle) -> bool {
    !matches!(
        *cursor_lifecycle(cursor),
        BoundedCursorLifecycle::Released(_)
    )
}

/// Whether this cursor still holds the suspended read it was opened with.
///
/// A refusal the kernel answered without touching the continuation leaves the
/// cursor open, and the layers above ask here rather than assuming every
/// refusal ended the read.
pub(super) fn cursor_retains_continuation(cursor: &BoundedCursorHandle) -> bool {
    matches!(*cursor_lifecycle(cursor), BoundedCursorLifecycle::Open(_))
}

fn cursor_lifecycle(cursor: &BoundedCursorHandle) -> MutexGuard<'_, BoundedCursorLifecycle> {
    match cursor._state.lifecycle.lock() {
        Ok(lifecycle) => lifecycle,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn exhausted_fetch(
    columns: Vec<String>,
) -> std::result::Result<BoundedCursorFetch, BoundedExecutionError> {
    let page = CursorPage {
        columns,
        rows: Vec::new(),
        has_more: false,
    };
    let encoded = serialized_size(&page)?;
    let encoded = u64::try_from(encoded)
        .map_err(|_| Error::Other("empty cursor page exceeds u64".to_string()))?;
    Ok(BoundedCursorFetch {
        page,
        telemetry: BoundedExecutionTelemetry {
            encoded_bytes: encoded,
            ..BoundedExecutionTelemetry::default()
        },
    })
}

pub(super) fn fetch_cursor(
    cursor: &mut BoundedCursorHandle,
    rows: Option<NonZeroUsize>,
    cancellation: OwnerReadCancellation,
    #[cfg(feature = "test-seams")] probe: Option<Arc<dyn BoundedExecutionProbe>>,
) -> std::result::Result<BoundedCursorFetch, BoundedExecutionError> {
    let mut lifecycle = cursor_lifecycle(cursor);
    let current = std::mem::replace(
        &mut *lifecycle,
        BoundedCursorLifecycle::Released(BoundedCursorTerminalReason::ExplicitClose),
    );
    let mut state = match current {
        BoundedCursorLifecycle::Released(reason) => {
            *lifecycle = BoundedCursorLifecycle::Released(reason);
            return Err(cursor_not_found());
        }
        BoundedCursorLifecycle::Exhausted(columns) => {
            let fetch = exhausted_fetch(columns.clone());
            *lifecycle = BoundedCursorLifecycle::Exhausted(columns);
            return fetch;
        }
        BoundedCursorLifecycle::Open(state) => state,
    };
    if let Err(error) = state.check_expiry() {
        state.finish_plugin_error(&error);
        *lifecycle = BoundedCursorLifecycle::Released(BoundedCursorTerminalReason::Expired);
        return Err(error);
    }
    if let Err(error) = state.context.begin_fetch(
        cancellation,
        #[cfg(feature = "test-seams")]
        probe,
    ) {
        state.finish_plugin_error(&error);
        *lifecycle = BoundedCursorLifecycle::Released(BoundedCursorTerminalReason::Refused);
        return Err(error);
    }
    let rows = match rows {
        Some(rows) => rows,
        None => match default_page_rows(state.context.limits) {
            Ok(rows) => rows,
            Err(error) => {
                state.finish_plugin_error(&error);
                *lifecycle = BoundedCursorLifecycle::Released(BoundedCursorTerminalReason::Refused);
                return Err(error);
            }
        },
    };
    // An explicit count above the effective row ceiling refuses this REQUEST,
    // not the read. Nothing has been pulled, no continuation position has been
    // consumed, and no charge has been taken, so the cursor stays exactly where
    // the caller left it -- which is what makes the refusal's own escape (ask
    // for a smaller page, or close and reopen) executable rather than advice
    // about a cursor that is already gone.
    if u64::try_from(rows.get()).unwrap_or(u64::MAX) > state.context.limits.result_rows {
        let error = RequestContext::limit_failure(
            ReadFailureLimit::ResultRows,
            state.context.limits.result_rows,
        );
        *lifecycle = BoundedCursorLifecycle::Open(state);
        return Err(error);
    }
    let outcome = state.fetch_page(rows);
    match outcome {
        Ok(fetch) => {
            if fetch.page.has_more {
                *lifecycle = BoundedCursorLifecycle::Open(state);
            } else {
                let columns = fetch.page.columns.clone();
                state.finish_plugin_success();
                *lifecycle = BoundedCursorLifecycle::Exhausted(columns);
            }
            Ok(fetch)
        }
        Err(BoundedExecutionError::Cancelled) => {
            *lifecycle = BoundedCursorLifecycle::Open(state);
            Err(BoundedExecutionError::Cancelled)
        }
        Err(error) => {
            let error = work_refusal_naming_the_statement(&state.sql, error);
            state.finish_plugin_error(&error);
            *lifecycle = BoundedCursorLifecycle::Released(BoundedCursorTerminalReason::Refused);
            Err(error)
        }
    }
}

pub(super) fn close_cursor(
    cursor: &mut BoundedCursorHandle,
) -> std::result::Result<(), BoundedExecutionError> {
    let mut lifecycle = cursor_lifecycle(cursor);
    let current = std::mem::replace(
        &mut *lifecycle,
        BoundedCursorLifecycle::Released(BoundedCursorTerminalReason::ExplicitClose),
    );
    match current {
        BoundedCursorLifecycle::Open(mut state) => {
            state.finish_plugin_success();
            *lifecycle =
                BoundedCursorLifecycle::Released(BoundedCursorTerminalReason::ExplicitClose);
        }
        BoundedCursorLifecycle::Exhausted(columns) => {
            *lifecycle = BoundedCursorLifecycle::Exhausted(columns);
        }
        BoundedCursorLifecycle::Released(reason) => {
            *lifecycle = BoundedCursorLifecycle::Released(reason);
        }
    }
    Ok(())
}
