//! The one finite maintenance cycle: advance each listed build once, expire due retained rows,
//! then seal partial tails
//! for caller or timer cycles. Every step uses the database's one commit function; no lock is
//! held between steps or while build arithmetic runs. Worker ownership lives in `database`
//! and thread creation and waiting live in `system`.

mod expire;
mod verify;
pub(crate) type Expiry = std::collections::BTreeMap<crate::store::IndexId, crate::store::Key>;

pub(crate) use verify::verify;
pub use verify::{ContentSize, VerifyReport};

use crate::database::{Open, locked};
use crate::store::{Read, VectorNumber};
use crate::types::{Error, Result};
use crate::vector;

/// Who drives a store's maintenance cycles.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Maintenance {
    /// The engine runs one worker for the lifetime of the writing handles.
    /// It polls at persisted `MAINTENANCE_POLL_INTERVAL` (60,000 ms by default)
    /// and wakes for due vector builds. Timer/caller cycles also seal partial tails;
    /// a vector wake does not. Read-only sessions start no worker; close joins it
    /// without requesting a final cycle. Due rows remain readable until expiry runs.
    #[default]
    Engine,
    /// The host calls [`crate::Database::run_maintenance_cycle`].
    Caller,
}

/// The most vectors one cycle seals.
pub(crate) const SEAL_ROWS: usize = 4096;

/// What one cycle of maintenance did, as [`crate::Database::run_maintenance_cycle`] returns it.
/// Every field is `None` on a narrowed caller, not a zero or a completion signal.
#[derive(Debug, Clone, Copy, PartialEq, Default, serde::Serialize)]
#[non_exhaustive]
pub struct MaintenanceReport {
    /// Whether maintenance work remains, including reader-blocked compaction. `None` is
    /// withheld; yield when `compaction_waiting_on` names a reader. Memory-stalled work waits.
    pub work_remaining: Option<bool>,
    /// What the cycle did for vector search.
    pub vector: VectorMaintenanceReport,
    /// Events delivered in this cycle: `Some(0)` on unnarrowed handles in this version.
    /// No external event-delivery worker is implemented.
    pub delivered: Option<u64>,
    /// Rows removed by expiry.
    pub expired_rows: Option<u64>,
    /// Versions removed from tables declared `HISTORY CURRENT ONLY`.
    pub versions_removed: Option<u64>,
    /// Pages moved by the cycle, at most 256; withheld on narrowed handles.
    pub compacted_pages: Option<u64>,
    /// Work deferred to a later cycle.
    pub deferred: Option<u64>,
    /// `Readers` when a reader still holds moved pages; withheld on narrowed handles.
    pub compaction_waiting_on: Option<WaitingOn>,
    /// Free pages over all pages, measured before a compaction step ran; `None` when none ran.
    pub compaction_free_ratio: Option<f64>,
    /// Bytes of pages in use holding no content, as a share; the shell fills it once per report.
    pub page_slack_ratio: Option<f64>,
    /// Old pages still to classify after a kill; zero when known, withheld when narrowed.
    pub recovery_pages_remaining: Option<u64>,
}

/// Why page reclamation waits; no reason is reported until a cycle observes it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub enum WaitingOn {
    /// A reader still holds the pages moved.
    Readers,
}

/// The part of a [`MaintenanceReport`] about vector search.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize)]
#[non_exhaustive]
pub struct VectorMaintenanceReport {
    /// How many vectors this cycle moved from pending lists into blocks, added to how many
    /// it packed into the blocks of an IVF being built.
    pub maintenance_vectors_done: Option<u64>,
    /// How many vectors still wait after this cycle, over every column of the store: those on
    /// a pending list, those a build under way has not packed, and those of partitions whose
    /// IVF is due and not begun. Those of a stalled build are not counted: no further
    /// cycle packs them until it has the memory.
    pub maintenance_vectors_remaining: Option<u64>,
    /// How many partitions have an IVF due or being built after this cycle.
    pub remaining_partitions: Option<u64>,
    /// How many of those had their last step refused for want of memory. A further cycle does
    /// nothing for them until the memory limit is raised or memory is let go, and with no
    /// limit declared there are none. These counts describe vector work only; use
    /// [`MaintenanceReport::work_remaining`] to decide whether to run another cycle.
    /// `None` means withheld, never completed.
    pub stalled_partitions: Option<u64>,
}

impl MaintenanceReport {
    pub(crate) fn withheld(self) -> Self {
        Self::default()
    }
}

/// The single finite cycle, with no commit lock held between its bounded steps.
pub(crate) fn cycle(
    open: &std::sync::Arc<Open>,
    partial: bool,
) -> Result<(MaintenanceReport, bool)> {
    let recovery = if open.store.recovery_state()?.0.is_some() {
        let held = open.committing.take(open)?;
        let result = open.store.recovery_step(4096);
        drop(held);
        Some(result?)
    } else {
        None
    };
    let begun = open.cancels(0);
    let stopped = || open.cancels(0) != begun;
    let mut delivery = MaintenanceReport::default();
    let (mut changed, more_delivery) = crate::events::deliver(open, &mut delivery)?;
    changed |= crate::events::run_schedules(open)?;
    let (view, catalog) = open.newest()?;
    let mut listed = open.budget.reserve(0, "maintenance")?;
    let totals = locked(&open.live).clone();
    let mut work = vector::unindexed_work(&view, &catalog, &mut listed, &totals)?;
    drop((view, catalog));
    let (mut done, mut remaining, mut partitions, mut stalled) = (0, 0, 0, 0);
    if recovery.is_none() {
        for item in work.iter().take_while(|_| !stopped()) {
            let (view, catalog) = open.newest()?;
            let vectors = locked(&open.live).live(item.vector);
            let step = vector::prepare(item, &view, &catalog, &open.budget, vectors);
            drop((view, catalog));
            let held = open.turn()?;
            let mut staged = open.begin()?;
            let step = step.and_then(|step| match step {
                Some(step) => {
                    locked(&open.work).rows_read += step.rows_read;
                    locked(&open.work).vectors_compared += step.compared;
                    let vectors = locked(&open.live).live(item.vector);
                    vector::stage_step(step, &mut staged.changes, vectors)
                }
                None => Ok(0),
            });
            match step {
                Ok(rows) => done += rows,
                Err(Error::MemoryBudgetExceeded {
                    requested_bytes, ..
                }) => {
                    staged = open.begin()?;
                    vector::stage_refusal(
                        item,
                        requested_bytes,
                        &mut staged.changes,
                        &staged.catalog,
                        &mut open.budget.reserve(0, "maintenance")?,
                    )?;
                }
                Err(other) => return Err(other),
            }
            changed |= !staged.changes.is_empty();
            open.commit(held, staged)?;
        }
    }
    let (expired, deferred, mut more_expiry) = expire::run(open)?;
    let (removed, more_versions) = expire::versions(open)?;
    more_expiry |= more_versions;
    changed |= expired > 0 || removed > 0;
    let (compacted, waiting, more_compaction, ratio) = if recovery.is_none() && !stopped() {
        if partial {
            let held = open.turn()?;
            let mut staged = open.begin()?;
            let sealed = vector::seal(
                &mut staged.changes,
                &staged.catalog,
                &open.budget,
                SEAL_ROWS,
            )?;
            done += sealed;
            changed |= !staged.changes.is_empty();
            open.commit(held, staged)?;
        }
        let (compacted, waiting, more_compaction, ratio) = compact(open, false)?;
        changed |= compacted > 0 || more_compaction;
        (compacted, waiting, more_compaction, ratio)
    } else {
        (0, None, false, None)
    };
    let (view, catalog) = open.newest()?;
    if expired > 0 {
        drop(work);
        listed.shrink(listed.bytes());
        work = vector::unindexed_work(&view, &catalog, &mut listed, &locked(&open.live))?;
    }
    remaining += view.vector_number(VectorNumber::Pending)?;
    for item in &work {
        let (vectors, waiting, blocked) = vector::progress(
            item,
            &view,
            &catalog,
            &mut listed,
            locked(&open.live).live(item.vector),
        )?;
        remaining += vectors;
        partitions += waiting;
        stalled += blocked;
    }
    let runnable =
        partitions > stalled || (partial && remaining > 0) || more_expiry || more_delivery;
    let report = MaintenanceReport {
        vector: VectorMaintenanceReport {
            maintenance_vectors_done: Some(done),
            maintenance_vectors_remaining: Some(remaining),
            remaining_partitions: Some(partitions),
            stalled_partitions: Some(stalled),
        },
        delivered: Some(delivery.delivered.unwrap_or(0)),
        expired_rows: Some(expired),
        versions_removed: Some(removed),
        compacted_pages: Some(compacted),
        recovery_pages_remaining: Some(recovery.unwrap_or(0)),
        compaction_waiting_on: waiting,
        compaction_free_ratio: ratio,
        deferred: Some(deferred + delivery.deferred.unwrap_or(0)),
        ..Default::default()
    };
    *locked(&open.last_cycle) = Some(report);
    open.signal.progress();
    // Recovery leaves vector work and compaction for the following cycle, even
    // when this step classified the final page; delivery and expiry still run.
    Ok((
        report,
        recovery.is_some() || changed && (runnable || more_compaction),
    ))
}

pub(crate) fn compact(
    open: &Open,
    forced: bool,
) -> Result<(u64, Option<WaitingOn>, bool, Option<f64>)> {
    let held = open.turn()?;
    let mut workspace = open.budget.reserve(256 * 256, "transaction")?;
    let settings = crate::settings::load(&open.newest()?.0)?;
    let ratio = |at: usize| f64::from_bits(settings[at].unwrap_or(crate::settings::SETTINGS[at].2));
    let due = forced || {
        let waited = settings[4].unwrap_or(crate::settings::SETTINGS[4].2);
        let (mut last, now) = (locked(&open.last_sweep), open.clock.now());
        let due = last.is_none_or(|at| {
            now.duration_since(at)
                .is_ok_and(|d| d.as_millis() as u64 >= waited)
        });
        *last = if due { Some(now) } else { *last };
        due
    };
    let space = || -> Result<_> {
        let (from, _) = open.store.compaction_position(false)?;
        let mut allowance = 256;
        let (free, total, pending, churn, _) = open.store.compaction_space(&mut allowance)?;
        let reclaimable = free + u64::from(from.is_none()) * pending;
        let (_, bytes, ceiling) = open.store.recovery_state()?;
        let active = forced
            || bytes > ceiling
            || reclaimable as f64 >= total as f64 * ratio(if from.is_some() { 3 } else { 2 });
        Ok((from, allowance, free, total, pending, churn, active))
    };
    let admitted = || -> Result<bool> {
        let (from, _, free, _, pending, churn, active) = space()?;
        Ok(pending > 0 || due && (active || from.is_some()) && free > churn)
    };
    if !admitted()? || workspace.grow(redb::RELEASE_PAGES as u64 * 8).is_err() {
        return Ok((0, None, false, None));
    }
    let before = open.store.compaction_backlog(None)?;
    drop(held);
    if drain(open)? {
        return Ok((0, Some(WaitingOn::Readers), false, None));
    }
    let held = open.turn()?;
    let (from, mut allowance, free, total, pending, churn, active) = space()?;
    let measured = (active && free > churn && total > 0).then(|| free as f64 / total as f64);
    allowance *= usize::from(active && free > churn);
    let allowance = (active || pending > 0).then_some(allowance);
    let mut staged = open.begin()?;
    let step = staged.changes.compact_pages(from, allowance, false);
    open.commit(held, staged)?;
    let outcome = step.load(std::sync::atomic::Ordering::Relaxed);
    let moved = outcome.saturating_sub(1);
    drain(open)?;
    let _held = open.committing.take(open)?;
    let after = open.store.compaction_backlog(None)?;
    let (next, moved_this_sweep) = open.store.compaction_position(moved > 0)?;
    let progressed = moved > 0
        || after.0 < before.0
        || after.1 < before.1
        || (next.is_some() && outcome > 0)
        || (next.is_none() && moved_this_sweep);
    let (_, _, _, readers) = open.store.compaction_state()?;
    let waiting = readers.then_some(WaitingOn::Readers);
    // The final drain can create a movable system page after an empty scan.
    // Report admission from that committed state, just as the next cycle does.
    let more = progressed && admitted()? && !readers;
    Ok((moved, waiting, more, measured))
}

fn drain(open: &Open) -> Result<bool> {
    let through = open.store.compaction_backlog(None)?.2;
    loop {
        let held = open.turn()?;
        let (from, _) = open.store.compaction_position(false)?;
        let before = open.store.compaction_backlog(Some(through))?;
        let pending = before.0 > 0;
        if !pending && !open.store.has_free_tail()? {
            return Ok(false);
        }
        let mut staged = open.begin()?;
        staged.changes.compact_pages(from, Some(0), true);
        open.commit(held, staged)?;
        let after = open.store.compaction_backlog(Some(through))?;
        if after.0 == 0 || (after.0 >= before.0 && after.1 >= before.1) {
            return Ok(after.0 > 0);
        }
    }
}

/// A complete record copy returned by [`crate::Database::export_snapshot`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub struct ExportReport {
    /// The source commit held throughout the copy.
    pub snapshot_tx: crate::TxId,
    /// Physical records copied, including slices and metadata, rather than logical rows.
    pub records: u64,
    /// Length of the completed destination data file, including physical page overhead.
    pub bytes_written: u64,
}

/// Validates and copies the same pinned moment through the store's bounded writer.
pub(crate) fn export(
    view: &crate::store::View,
    dest: &std::path::Path,
    budget: &crate::memory::Budget,
) -> Result<ExportReport> {
    let budget = budget.statement();
    let catalog = crate::schema::Catalog::load(view, &budget)?;
    verify(view, &catalog, &budget)?;
    std::fs::create_dir(dest).map_err(|error| Error::StoreIo {
        operation: "export",
        path: dest.to_path_buf(),
        reason: error.to_string(),
    })?;
    let (records, bytes_written) = crate::store::Store::copy_view(view, dest, &budget)?;
    Ok(ExportReport {
        snapshot_tx: view.commit(),
        records,
        bytes_written,
    })
}
