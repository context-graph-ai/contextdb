//! The one finite maintenance cycle: advance each listed build once, remove what is due in
//! batches, then seal partial tails for caller or timer cycles and step the tidy-up. Every step uses the one commit function; no lock is
//! held between steps or while build arithmetic runs. Worker ownership lives in `database`
//! and thread creation and waiting live in `system`.

mod expire;
mod remove;
mod verify;

pub use crate::types::{
    ContentSize, Maintenance, MaintenanceReport, VectorMaintenanceReport, VerifyReport, WaitingOn,
};
pub(crate) use verify::verify;

use std::ops::Bound::{Excluded, Included};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex};

use crate::commit::{Open, running, with_bound};
use crate::events::{IN_SINK_CALL, identity, key, load_body};
use crate::query::Visible;
use crate::sql::ast::EventDeclaration;
use crate::store::{Read, VectorNumber};
use crate::system::locked;
use crate::types::{Error, Kind, Result, SinkError};
use crate::vector;

pub(crate) type Expiry = std::collections::BTreeMap<crate::store::IndexId, crate::store::Key>;

/// What maintenance keeps between the cycles of one open store.
#[derive(Default)]
pub(crate) struct State {
    /// When the tidy-up's window began, the commit and file length a pass that moved nothing
    /// ended at, and whether the pass under way has moved a page.
    pub(crate) last_sweep: Mutex<Option<std::time::SystemTime>>,
    /// The source of removal the next cycle starts at.
    pub(crate) removal_from: Mutex<usize>,
    pub(crate) expiry: Mutex<Expiry>,
    /// The number the trigger history's prune goes on from.
    pub(crate) audit_swept: Mutex<u64>,
}

/// The most vectors one cycle seals.
pub(crate) const SEAL_ROWS: usize = 4096;

/// The single finite cycle, with no commit lock held between its bounded steps. Its report is
/// kept as far as it went however it ends, and every end wakes the waiters.
pub(crate) fn cycle(
    open: &std::sync::Arc<Open>,
    state: &State,
    partial: bool,
) -> Result<(MaintenanceReport, bool)> {
    let mut report = MaintenanceReport::default();
    let length = || open.store.recovery_state().ok().map(|(_, bytes, _)| bytes);
    let before = length();
    let result = stages(open, state, partial, &mut report);
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
    state: &State,
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
    let (mut changed, more_delivery, delivered, held_back) = deliver(open)?;
    report.delivered = Some(delivered);
    changed |= run_schedules(open)?;
    // Rows removed in this cycle are gone before its vector steps, which build for what stays.
    let more_removal = remove::run(open, state, report, &check)?;
    changed |= report.removed_records.unwrap_or(0) > 0 || report.removal_advanced == Some(true);
    changed |= expire::audit(open, state)? > 0;
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
        more_compaction = compact(open, state, false, report)?;
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
    report.deferred = Some(deferred + held_back);
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
pub(crate) fn compact(
    open: &Open,
    state: &State,
    asked: bool,
    report: &mut MaintenanceReport,
) -> Result<bool> {
    let (view, _) = open.newest()?;
    let settings = crate::settings::load(|at| view.setting(at))?;
    let ratio = |at: usize| f64::from_bits(settings[at].unwrap_or(crate::settings::SETTINGS[at].2));
    let interval = settings[4].unwrap_or(crate::settings::SETTINGS[4].2);
    drop(view);
    (report.compacted_pages, report.compaction_pages_read) = (Some(0), Some(0));
    report.compaction_pages_written = Some(0);
    let now = open.clock.now();
    let mut window = locked(&state.last_sweep);
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

type Change = (u64, Option<(u64, u8)>);
type Tally = (String, bool, bool, bool, bool);
fn settle(
    open: &Open,
    tally: &mut (u64, u64),
    previous: Option<(Change, Tally)>,
    mark: Option<Change>,
) -> Result<bool> {
    if previous.is_none() && mark.is_none() {
        return Ok(false);
    }
    let guard = open.turn()?;
    let mut staged = open.begin()?;
    for (action, change) in previous.as_ref().map(|p| p.0).into_iter().chain(mark) {
        staged
            .changes
            .put_delivery(Some(action), None, |view, _, encoded| {
                let at = key(action);
                let mut replacement = None;
                view.deliveries(
                    (Included(at.as_slice()), Included(at.as_slice())),
                    &mut |_, bytes| {
                        replacement = Some(None);
                        if let Some((attempts, flag)) = change {
                            encoded.grow(bytes.len() as u64)?;
                            let mut out = bytes.to_vec();
                            out[16..24].copy_from_slice(&attempts.to_be_bytes());
                            out[24] = flag;
                            replacement = Some(Some(out));
                        }
                        Ok(false)
                    },
                )?;
                Ok(replacement)
            })?;
    }
    open.commit(guard, staged)?;
    let Some((_, (sink, success, terminal, retried, fresh))) = previous else {
        return Ok(false);
    };
    let mut counters = locked(&open.deliveries.counters);
    let metrics = &mut counters.get_mut(&sink).expect("examined sink").0;
    metrics.delivered += u64::from(success);
    metrics.window_refused &= !success;
    metrics.retried += u64::from(retried);
    metrics.uncertain += u64::from(fresh);
    metrics.permanent_failures += u64::from(terminal);
    tally.0 += u64::from(success);
    tally.1 += u64::from(!success && !terminal);
    Ok(success || terminal)
}
/// One finite pass, with no view, registry lock or commit lock held during host code.
/// Answers whether it progressed, whether more is due, and the actions it delivered and
/// deferred.
pub(crate) fn deliver(open: &Open) -> Result<(bool, bool, u64, u64)> {
    let Ok(_dispatch) = open.deliveries.active.try_lock() else {
        return Ok((false, false, 0, 0));
    };
    let (mut carried, mut tally) = (None, (0, 0));
    let done = pass(open, &mut tally, &mut carried);
    let last = settle(open, &mut tally, carried, None)?;
    done.map(|(progressed, more)| (progressed | last, more, tally.0, tally.1))
}
fn pass(
    open: &Open,
    tally: &mut (u64, u64),
    carried: &mut Option<(Change, Tally)>,
) -> Result<(bool, bool)> {
    let upper = key(open.newest()?.0.last_action()?);
    let (mut cursor, mut progressed) = (key(0), false);
    for _ in 0..256 {
        let (view, catalog) = open.newest()?;
        let mut found = None;
        view.deliveries(
            (Excluded(cursor.as_slice()), Included(upper.as_slice())),
            &mut |k, bytes| {
                let mut held = open.budget.reserve(0, "delivery attempt")?;
                let mut delivery = identity(&view, k, bytes, &mut held)?;
                let (sink, action) = (&delivery.sink, delivery.event.action_id);
                open.deliveries.examine(&open.budget, sink, action)?;
                let registration = locked(&open.deliveries.callbacks)
                    .get(&delivery.sink)
                    .cloned();
                let table = catalog
                    .table(&delivery.event.table)
                    .filter(|t| t.id == delivery.table);
                let loaded = if table.is_some() && registration.is_some() {
                    load_body(&view, bytes, &mut delivery, &mut held)
                } else {
                    Ok(())
                };
                let admitted = match (table, &registration, loaded) {
                    (_, _, Err(Error::MemoryBudgetExceeded { .. })) => {
                        let mut counters = locked(&open.deliveries.counters);
                        let window = &mut counters.get_mut(&delivery.sink).expect("examined").0;
                        (window.window_refusals, window.window_refused) =
                            (window.window_refusals + 1, true);
                        false
                    }
                    (_, _, Err(error)) => return Err(error),
                    (None, _, _) => true,
                    (Some(t), Some(r), _) => match Visible::new(t, &catalog, &r.0, &mut held) {
                        Ok(mut filter) => filter.sees(&view, |at| {
                            delivery
                                .event
                                .row_values
                                .get(&t.meta.columns[at].name)
                                .or_else(|| t.missing.get(at))
                        })?,
                        Err(Error::PrincipalRequired { .. }) => false,
                        Err(error) => return Err(error),
                    },
                    _ => false,
                };
                if !admitted {
                    tally.1 += 1;
                    return Ok(true);
                }
                found = Some((delivery, registration.filter(|_| table.is_some()), held));
                Ok(false)
            },
        )?;
        let Some((delivery, registration, held)) = found else {
            return Ok((progressed, false));
        };
        cursor = key(delivery.event.action_id);
        drop((view, catalog));
        let (action, attempts) = (delivery.event.action_id, delivery.attempts);
        let mark = (delivery.flag != 2).then_some((action, Some((attempts, 2))));
        progressed |= settle(open, tally, carried.take(), mark)?;
        IN_SINK_CALL.set(true);
        let outcome = registration.map(|registration| {
            catch_unwind(AssertUnwindSafe(|| (registration.1)(&delivery.event))).unwrap_or_else(
                |_| Err(SinkError::OutcomeUncertain("host callback panicked".into())),
            )
        });
        IN_SINK_CALL.set(false);
        let success = matches!(outcome, Some(Ok(())));
        let terminal = matches!(outcome, None | Some(Err(SinkError::Permanent(_))));
        let fresh = matches!(outcome, Some(Err(SinkError::OutcomeUncertain(_))));
        let stored = (!success && !terminal)
            .then_some((attempts + 1, u8::from(delivery.flag != 0 || fresh)));
        let tally = (
            delivery.sink.clone(),
            success,
            terminal,
            attempts > 0,
            fresh || delivery.flag == 2,
        );
        *carried = Some(((action, stored), tally));
        drop((delivery, held, outcome));
    }
    Ok((progressed, cursor < upper))
}

/// Step two of the same cycle; the commit lock serializes both timer and caller drives.
pub(crate) fn run_schedules(open: &Arc<Open>) -> Result<bool> {
    use crate::types::MissedTickPolicy::*;
    let Ok(_running) = open.triggers.running.try_lock() else {
        return Ok(false);
    };
    let now = crate::system::now_millis(&*open.clock).max(0) as u64;
    let (_, catalog) = open.newest()?;
    let mut changed = false;
    let mut failure = None;
    for name in catalog.schedules.keys() {
        loop {
            let guard = open.turn()?;
            let mut staged = open.transaction_stage()?;
            let Some(&due) = staged.catalog.schedules.get(name) else {
                break;
            };
            if due > now {
                break;
            }
            let mut held = open
                .budget
                .reserve((name.len() * 2 + 160) as u64, "schedule dispatch")?;
            let Some((EventDeclaration::Schedule(schedule), _)) =
                staged.catalog.event(&(Kind::Schedule, name.clone()))
            else {
                break;
            };
            held.grow(schedule.callback.len() as u64)?;
            let every = schedule.every_ms;
            let catching = schedule.missed_tick_policy == CatchUp;
            let ticks = (now - due) / every + 1;
            let mut first = 0;
            if catching {
                let window = u64::from(schedule.catch_up_within_seconds.unwrap_or(0)) * 1000;
                let cutoff = now.saturating_sub(window);
                first = cutoff.saturating_sub(due).div_ceil(every).min(ticks);
            }
            let advance = (if catching { first + 1 } else { ticks }).min(ticks);
            let next = due.saturating_add(every.saturating_mul(advance));
            let missed = schedule.missed_tick_policy == FailLoud && ticks > 1;
            let callback = locked(&open.triggers.cron)
                .get(&schedule.callback)
                .cloned()
                .ok_or_else(|| {
                    Error::PlanError(format!("no callback registered: {}", schedule.callback))
                });
            let callback_missing = callback.is_err() && !missed && first != ticks;
            let mut catalog = staged.catalog.next(&staged.budget)?;
            catalog.advance_schedule(name, next, &mut staged.changes)?;
            staged.catalog = Arc::new(catalog);
            let (staged, result) = with_bound(open, staged, |_, host| {
                running(open, Some(name.clone()), true);
                if missed {
                    return Err(Error::MissedTicksExceeded {
                        ticks: ticks.min(u64::from(u32::MAX)) as u32,
                        policy: "fail-loud".into(),
                    });
                }
                if first == ticks {
                    return Ok(());
                }
                let callback = callback?;
                catch_unwind(AssertUnwindSafe(|| (callback.0)(host)))
                    .unwrap_or_else(|_| Err(Error::PlanError("schedule callback panicked".into())))
            });
            let result = result.and_then(|()| open.commit(guard, staged));
            // A missing callback is this schedule's failure alone: the cycle goes on.
            let mut failed = locked(&open.deliveries.failed);
            match &result {
                Err(error @ Error::PlanError(_)) if callback_missing => {
                    failed.insert(name.clone(), error.to_string())
                }
                _ => failed.remove(name),
            };
            drop(failed);
            if let Err(error) = result {
                let guard = open.turn()?;
                let mut settlement = open.begin()?;
                if settlement.catalog.schedules.get(name) == Some(&due) {
                    let mut catalog = settlement.catalog.next(&settlement.budget)?;
                    catalog.advance_schedule(name, next, &mut settlement.changes)?;
                    settlement.catalog = Arc::new(catalog);
                    open.commit(guard, settlement)?;
                }
                if !callback_missing {
                    failure.get_or_insert(error);
                }
            }
            changed = true;
        }
    }
    failure.map_or(Ok(changed), Err)
}
