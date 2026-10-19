//! One protected resident history view and admitted per-occurrence copies.
use super::*;
use contextdb_core::read_memory::{ReadCredit, ReadMemoryBudget};

/// The registration protects superseded occurrences as well as the visible
/// version. Its slot is admitted before the registry can allocate it.
pub(super) struct ResidentHistoryGuard {
    pub(super) through: Lsn,
    pub(super) change_end: Option<(Lsn, usize)>,
    pub(super) ddl_end: Option<(Lsn, usize)>,
    pub(super) pending: Option<contextdb_relational::membership::MembershipImage>,
    _registration: SnapshotRegistration,
    _slot: SnapshotPinSlot,
}

impl Database {
    /// Pause a resident pull immediately before it attempts the commit gate.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn pause_before_pull_history_capture_for_test(&self) -> ApplyPhasePauseGuard {
        let inner = self.snapshot_registry.pull_capture_test_pause.clone();
        let generation = inner.arm();
        ApplyPhasePauseGuard { inner, generation }
    }

    /// Make the next indexed row body decoded on this thread report a wrong
    /// LSN after its physical key has passed the source's ordinary validation.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn arm_exact_row_lsn_fault_for_test(&self) {
        crate::persistence::writer_source::arm_exact_row_lsn_fault_for_test();
    }

    /// Observe the canonical registry, without installing another hold.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn active_pull_history_registrations_for_test(&self) -> usize {
        self.snapshot_registry
            .state
            .lock()
            .active
            .values()
            .filter(|entry| matches!(entry.class, SnapshotRegistrationClass::PullHistory { .. }))
            .count()
    }

    /// Inspect a selected page while all of its copy admissions remain alive.
    /// The callback also keeps the vector copy owner alive; returning a bare
    /// ChangeSet here would release its admissions before its allocations.
    #[doc(hidden)]
    #[cfg(any(test, feature = "test-seams"))]
    pub fn with_checked_changes_page_for_test<T>(
        &self,
        since: Lsn,
        page_bytes: usize,
        read_memory: Arc<dyn ReadMemoryBudget>,
        inspect: impl FnOnce(&ChangeSet, Option<Lsn>) -> T,
    ) -> Result<T> {
        let mut vector_copy = crate::vector_read_memory::admission(read_memory.clone());
        let mut credit = ReadCredit::new(read_memory.clone());
        let through = std::cell::Cell::new(None);
        let purge_items = std::cell::RefCell::new(Vec::new());
        let (changes, arrivals, provenance) = self.checked_changes_since_with_arrivals(
            since,
            &mut vector_copy,
            read_memory,
            &mut credit,
            Some(PullPageRequest {
                bytes: page_bytes,
                through: &through,
                purge_items: &purge_items,
            }),
        )?;
        let result = inspect(&changes, through.get());
        drop((changes, arrivals, provenance, purge_items));
        drop((credit, vector_copy));
        Ok(result)
    }

    pub(super) fn capture_resident_pull_history(
        &self,
        memory: Arc<dyn ReadMemoryBudget>,
    ) -> Result<ResidentHistoryGuard> {
        let slot = self.snapshot_registry.acquire_pin_slot(memory)?;
        #[cfg(any(test, feature = "test-seams"))]
        self.snapshot_registry.pull_capture_test_pause.maybe_pause();
        self.with_commit_lock(|| {
            // Removal passes finish inside this gate. Acquiring it establishes
            // their completion; waiting here would deadlock publication.
            let published = self.tx_mgr.capture_published_read();
            let through = published.lsn;
            let snapshot = published.snapshot;
            let change_end = captured_end(&self.change_log.read(), through, ChangeLogEntry::lsn);
            let ddl_end = captured_end(&self.ddl_log.read(), through, |(lsn, _)| *lsn);
            let pending_source = self.snapshot_registry.pending_delete_source.lock().clone();
            let pending = pending_source.map(|source| source.lock().image.clone());
            let state = self.snapshot_registry.state.lock();
            debug_assert!(state.active_pass.is_none());
            let registration = self.snapshot_registry.admit(
                state,
                snapshot,
                SnapshotRegistrationClass::PullHistory { through },
                None,
            );
            Ok(ResidentHistoryGuard {
                through,
                change_end,
                ddl_end,
                pending,
                _registration: registration,
                _slot: slot,
            })
        })
    }
}

/// The position is relative to a protected equal-LSN group, so removing an
/// earlier unprotected prefix cannot change which occurrence comes next.
pub(super) struct ResidentHistoryCursor<T> {
    entries: Arc<RwLock<Vec<T>>>,
    since: Lsn,
    through: Lsn,
    end: Option<(Lsn, usize)>,
    position: Option<(Lsn, usize)>,
    lsn: fn(&T) -> Lsn,
    copy_bytes: fn(&T) -> usize,
    memory: Arc<dyn ReadMemoryBudget>,
}

impl<T: Clone> ResidentHistoryCursor<T> {
    pub(super) fn new(
        entries: Arc<RwLock<Vec<T>>>,
        since: Lsn,
        through: Lsn,
        end: Option<(Lsn, usize)>,
        lsn: fn(&T) -> Lsn,
        copy_bytes: fn(&T) -> usize,
        memory: Arc<dyn ReadMemoryBudget>,
    ) -> Self {
        Self {
            entries,
            since,
            through,
            end,
            position: None,
            lsn,
            copy_bytes,
            memory,
        }
    }

    pub(super) fn next(&mut self) -> Result<Option<(T, ReadCredit)>> {
        let Some((last_lsn, last_ordinal)) = self.end else {
            return Ok(None);
        };
        let entries = self.entries.read();
        let position = match self.position {
            Some((lsn, consumed)) => entries
                .partition_point(|entry| (self.lsn)(entry) < lsn)
                .checked_add(consumed)
                .ok_or_else(|| Error::Other("resident history ordinal overflow".into()))?,
            None => entries.partition_point(|entry| (self.lsn)(entry) <= self.since),
        };
        let Some(entry) = entries
            .get(position)
            .filter(|entry| (self.lsn)(entry) <= self.through)
        else {
            return Ok(None);
        };
        let lsn = (self.lsn)(entry);
        let ordinal = match self.position {
            Some((previous, consumed)) if previous == lsn => consumed,
            _ => 0,
        };
        if lsn > last_lsn || (lsn == last_lsn && ordinal >= last_ordinal) {
            return Ok(None);
        }
        let credit = ReadCredit::try_new(
            self.memory.clone(),
            (self.copy_bytes)(entry),
            "reader",
            "pull_source_window",
            "Raise the declared memory limit to admit this history occurrence.",
        )?;
        let value = entry.clone();
        self.position = Some((lsn, ordinal + 1));
        Ok(Some((value, credit)))
    }
}

/// A constant-size endpoint in the protected group coordinate system.
fn captured_end<T>(entries: &[T], through: Lsn, lsn: impl Fn(&T) -> Lsn) -> Option<(Lsn, usize)> {
    let end = entries.partition_point(|entry| lsn(entry) <= through);
    let last = lsn(entries.get(end.checked_sub(1)?)?);
    let start = entries.partition_point(|entry| lsn(entry) < last);
    Some((last, end - start))
}
