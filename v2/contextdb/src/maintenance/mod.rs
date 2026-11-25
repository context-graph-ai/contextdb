//! The one finite maintenance cycle: advance each listed build once, remove what is due in
//! batches, then seal partial tails for caller or timer cycles and step the tidy-up. Every step uses the database's one commit function; no lock is
//! held between steps or while build arithmetic runs. Worker ownership lives in `database`
//! and thread creation and waiting live in `system`.

mod expire;
mod remove;
pub(crate) use remove::any_owed;
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
    /// Versions removed that their table no longer keeps or whose row was removed.
    pub versions_removed: Option<u64>,
    /// Records removed in the cycle's batches: rows with their entries, versions, index entries,
    /// vector records and slices of large values, at most 1,024 a batch beside one row's own.
    pub removed_records: Option<u64>,
    /// Whether an owed range still holds records after the cycle's last batch.
    pub owed: Option<bool>,
    /// Whether a batch moved an owed range on, if only past newer versions to its stored place.
    pub removal_advanced: Option<bool>,
    /// True when the removal loop or the tidy-up was refused memory in this cycle.
    pub memory_refused: Option<bool>,
    /// True when the cycle's tidy-up step was refused room; the pass keeps its last place.
    pub compaction_disk_refused: Option<bool>,
    /// Pages moved by the cycle's tidy-up step: at most the allowance of 256 pages read and
    /// written, except that a step's first movable block moves whatever its size.
    pub compacted_pages: Option<u64>,
    /// Pages the tidy-up step read to find what to move.
    pub compaction_pages_read: Option<u64>,
    /// Pages the tidy-up step wrote: the copies and the pages above them.
    pub compaction_pages_written: Option<u64>,
    /// Bytes the data file lost from the start of the cycle (or `compact_now`) to its end; 0
    /// when it did not shrink.
    pub reclaimed_bytes: Option<u64>,
    /// Whether the tidy-up moved pages or its place, or was refused with a smaller move owed.
    pub compaction_advanced: Option<bool>,
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
    /// Whether the rebuild of the free-page record took a step in this cycle.
    pub recovery_advanced: Option<bool>,
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

/// The single finite cycle, with no commit lock held between its bounded steps. Its report is
/// kept as far as it went however it ends, and every end wakes the waiters.
pub(crate) fn cycle(
    open: &std::sync::Arc<Open>,
    partial: bool,
) -> Result<(MaintenanceReport, bool)> {
    let mut report = MaintenanceReport::default();
    let length = || open.store.recovery_state().ok().map(|(_, bytes, _)| bytes);
    let before = length();
    let result = stages(open, partial, &mut report);
    report.reclaimed_bytes = before.zip(length()).map(|(was, is)| was.saturating_sub(is));
    if let Ok(again) = result {
        report.work_remaining = Some(again || report.compaction_waiting_on.is_some());
    }
    *locked(&open.last_cycle) = Some(report);
    open.signal.progress();
    result.map(|again| (report, again))
}

/// One step of the free-page recovery a crash left owed, recorded in `report`: the pages left
/// to it, or `None` when none was owed. Until it ends no tidy-up step runs.
pub(crate) fn recover(open: &Open, report: &mut MaintenanceReport) -> Result<Option<u64>> {
    let recovery = if open.store.recovery_state()?.0.is_some() {
        let held = open.committing.take(open)?;
        let result = open.store.recovery_step(4096);
        drop(held);
        Some(result?)
    } else {
        None
    };
    report.recovery_pages_remaining = Some(recovery.unwrap_or(0));
    report.recovery_advanced = Some(recovery.is_some());
    Ok(recovery)
}

fn stages(
    open: &std::sync::Arc<Open>,
    partial: bool,
    report: &mut MaintenanceReport,
) -> Result<bool> {
    let recovery = recover(open, report)?;
    // A cancel returns the interruption from the stage it reaches.
    let begun = open.cancels(0);
    let check = || match open.cancels(0) == begun {
        true => Ok(()),
        false => Err(Error::QueryCanceled {
            statement_timeout: false,
        }),
    };
    let mut delivery = MaintenanceReport::default();
    let (mut changed, more_delivery) = crate::events::deliver(open, &mut delivery)?;
    report.delivered = Some(delivery.delivered.unwrap_or(0));
    changed |= crate::events::run_schedules(open)?;
    // Rows removed in this cycle are gone before its vector steps, which build for what stays.
    let more_removal = remove::run(open, report, &check)?;
    changed |= report.removed_records.unwrap_or(0) > 0 || report.removal_advanced == Some(true);
    changed |= expire::audit(open)? > 0;
    let (view, catalog) = open.newest()?;
    let mut listed = open.budget.reserve(0, "maintenance")?;
    let totals = locked(&open.live).clone();
    let work = vector::unindexed_work(&view, &catalog, &mut listed, &totals)?;
    drop((view, catalog));
    let (mut done, mut remaining, mut partitions, mut stalled) = (0, 0, 0, 0);
    // Partitions whose step was refused for disk room, each stamped in a writer turn at the
    // committed state the refused attempt began from, and not if a commit came between; the
    // cycle goes on to the others.
    let mut full = Vec::new();
    let mut stamp = |base, work: &vector::Work| {
        let _turn = open.turn()?;
        let (view, catalog) = open.newest()?;
        let work = (work.vector, work.number, Some(&work.tuple));
        let at = match view.commit() == base {
            true => vector::stamp(&view, &catalog, work, &mut listed)?,
            false => None,
        };
        Ok::<_, Error>(at.map(|at| (work.0, work.1, at)))
    };
    if recovery.is_none() {
        for item in work.iter() {
            check()?;
            let (view, catalog) = open.newest()?;
            let vectors = locked(&open.live).live(item.vector);
            let step = vector::prepare(item, &view, &catalog, &open.budget, vectors);
            drop((view, catalog));
            let held = open.turn()?;
            let mut staged = open.begin()?;
            let step = step.and_then(|step| match step {
                Some(step) => {
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
            let (staging, base) = (!staged.changes.is_empty(), staged.changes.first().commit());
            match open.commit(held, staged) {
                Err(Error::DiskLimitExceeded { .. }) => full.extend(stamp(base, item)?),
                result => changed |= result.map(|_| staging)?,
            }
        }
    }
    check()?;
    let mut more_compaction = false;
    if recovery.is_none() {
        // A seal refused for room is tried again for its first partition alone, which is the
        // one refused if that is too; the others are left pending.
        if partial {
            let mut alone = false;
            loop {
                let held = open.turn()?;
                let mut staged = open.begin()?;
                let (budget, rows) = (&open.budget, SEAL_ROWS);
                let (sealed, first) = match alone {
                    true => {
                        vector::sealing(&mut staged.changes, &staged.catalog, budget, rows, true)?
                    }
                    false => (
                        vector::seal(&mut staged.changes, &staged.catalog, budget, rows)?,
                        None,
                    ),
                };
                let staging = !staged.changes.is_empty();
                let base = staged.changes.first().commit();
                match open.commit(held, staged) {
                    Err(Error::DiskLimitExceeded { .. }) if !alone => {
                        alone = true;
                        continue;
                    }
                    Err(Error::DiskLimitExceeded { .. }) => full.extend(
                        first
                            .map(|first| stamp(base, &first))
                            .transpose()?
                            .flatten(),
                    ),
                    result => {
                        result?;
                        done += sealed;
                        changed |= staging;
                    }
                }
                break;
            }
        }
        stalls(open, full, &mut listed)?;
        check()?;
        more_compaction = compact(open, false, report)?;
        changed |= more_compaction;
    }
    let (view, catalog) = open.newest()?;
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
        partitions > stalled || (partial && remaining > 0) || more_removal || more_delivery;
    report.vector = VectorMaintenanceReport {
        maintenance_vectors_done: Some(done),
        maintenance_vectors_remaining: Some(remaining),
        remaining_partitions: Some(partitions),
        stalled_partitions: Some(stalled),
    };
    let deferred = report.deferred.unwrap_or(0);
    report.deferred = Some(deferred + delivery.deferred.unwrap_or(0));
    // Recovery leaves vector work and compaction for the following cycle, even
    // when this step classified the final page; delivery and removal still run.
    Ok(recovery.is_some() || changed && (runnable || more_compaction))
}

/// Keeps in the folder's `space` file the partitions refused for room, each with the stamp of
/// its committed state when it was refused, and drops those whose state has changed since.
fn stalls(
    open: &Open,
    full: Vec<(crate::store::VectorId, u32, u64)>,
    held: &mut crate::memory::Reservation,
) -> Result<()> {
    let kept = open.store.space().full;
    if kept.is_empty() && full.is_empty() {
        return Ok(());
    }
    let (view, catalog) = open.newest()?;
    let mut stamped = Vec::new();
    let again = |v, n| full.iter().any(|w| w.0 == v && w.1 == n);
    for (vector, number, at) in kept {
        let now = vector::stamp(&view, &catalog, (vector, number, None), held)?;
        if now == Some(at) && !again(vector, number) {
            stamped.push((vector, number, at));
        }
    }
    stamped.extend(full);
    open.store.remember(|space| space.full = stamped);
    Ok(())
}

/// One step of the tidy-up when one is due, recorded in `report`; true while a pass goes on.
/// A pass starts at the end of a window of `MIN_INTERVAL` when the fewest free pages a commit
/// of the window left itself are at least `START_RATIO` of the file, and runs a step a cycle
/// until free pages fall below `STOP_RATIO`, which ends it; `asked`, or a file at its disk limit,
/// takes a step whatever the share. After a pass that moved nothing, none starts again until a
/// commit releases a page that can open a lower place, as the store records across a reopen.
/// A step refused for room that had chosen a page owes the next poll's attempt the next page
/// alone, kept in the folder's `space` file under the release mark; one that had chosen none is
/// not tried again unasked while the mark stands.
pub(crate) fn compact(open: &Open, asked: bool, report: &mut MaintenanceReport) -> Result<bool> {
    let (view, _) = open.newest()?;
    let settings = crate::settings::load(&view)?;
    let ratio = |at: usize| f64::from_bits(settings[at].unwrap_or(crate::settings::SETTINGS[at].2));
    let interval = settings[4].unwrap_or(crate::settings::SETTINGS[4].2);
    drop(view);
    (report.compacted_pages, report.compaction_pages_read) = (Some(0), Some(0));
    report.compaction_pages_written = Some(0);
    let now = open.clock.now();
    let mut window = locked(&open.last_sweep);
    let held = open.turn()?;
    let (free, total, freed, rested) = open.store.compaction_state()?;
    // A file with no page of room left starts a pass at once, as one over its limit does.
    let (_, bytes, limit) = open.store.recovery_state()?;
    let forced = asked || limit.saturating_sub(bytes) < 4096;
    // With no disk limit a refused step met only the tidy-up's own rule that it never lengthens
    // the file: nothing is left to move, which is no disk refusal and owes no smaller move.
    let unlimited = limit == u64::MAX;
    let refused = |report: &mut MaintenanceReport, readers: bool| match readers {
        true => report.compaction_waiting_on = Some(WaitingOn::Readers),
        false if unlimited => {}
        false => report.compaction_disk_refused = Some(true),
    };
    let mark = open.store.compaction_mark()?;
    let (at, k, terminal) = open.store.space().retry;
    let (k, terminal) = if at == mark {
        (k, terminal)
    } else {
        (0, false)
    };
    if !asked && (terminal || forced && rested) {
        if terminal {
            refused(report, open.store.readers_hold());
        }
        return Ok(false);
    }
    // A live retry at the same mark goes on with the pass it was admitted to.
    let running = !forced && (k > 0 || open.store.compaction_position()?.is_some());
    if !running && !forced {
        // The first window starts at the first look, from nothing.
        let due = window.map_or(interval == 0, |at| {
            now.duration_since(at)
                .is_ok_and(|d| d.as_millis() as u64 >= interval)
        });
        if !due {
            if window.is_none() {
                (*window, _) = (Some(now), open.store.surplus());
            }
            return Ok(false);
        }
        let surplus = open
            .store
            .surplus()
            .filter(|_| interval > 0)
            .unwrap_or(free + freed);
        *window = Some(now);
        if rested || (surplus as f64) < total as f64 * ratio(2) {
            return Ok(false);
        }
    }
    report.compaction_free_ratio = Some(free as f64 / total.max(1) as f64);
    let room = 256 * 256 + redb::RELEASE_PAGES as u64 * 8;
    let _room = open
        .budget
        .reserve(room, "transaction")
        .inspect_err(|_| report.memory_refused = Some(true))?;
    let stop = if running {
        (total as f64 * ratio(3)).ceil() as u64
    } else {
        0
    };
    let (step, selected, read) = open.store.compaction_step(stop, k);
    // The newest view is read again, so it holds no page the step let go.
    locked(&open.newest).view = None;
    drop(held);
    let step = match step {
        Err(Error::DiskLimitExceeded { .. }) => {
            let readers = open.store.readers_hold();
            refused(report, readers);
            let terminal = !selected || unlimited && !readers;
            open.store
                .remember(|space| space.retry = (mark, k + 1, terminal));
            (report.compaction_advanced, report.compaction_pages_read) =
                (Some(!terminal), Some(read));
            return Ok(false);
        }
        step => step?,
    };
    open.store
        .remember(|space| space.retry = Default::default());
    let readers = open.store.readers_hold();
    report.compaction_waiting_on = readers.then_some(WaitingOn::Readers);
    report.compacted_pages = Some(step.moved);
    report.compaction_pages_read = Some(step.read);
    report.compaction_pages_written = Some(step.written);
    report.compaction_advanced = Some(step.moved > 0 || !step.ended);
    if step.ended {
        (*window, _) = (Some(now), open.store.surplus());
    }
    Ok(!step.ended && !readers)
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
