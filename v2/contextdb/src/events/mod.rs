//! Transactional callbacks prepare related rows before the one commit's final validation.
//! Routed actions commit with those rows and are delivered afterwards by maintenance.

pub(crate) mod delivery;
pub(crate) use delivery::{Deliveries, deliver, erase, erase_gone, prepare, validate};

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex};
use std::thread::ThreadId;

use crate::database::{Database, Open, Staged, locked};
use crate::memory::{Budget, Reservation};
use crate::schema::Catalog;
use crate::sql::ast::EventDeclaration;
use crate::store::{ChangeSet, Key, Read, StagedValues, TableId};
use crate::types::{Error, Kind, Result, TriggerContext, TriggerEvent, TxId, heap_bytes};

pub(crate) const CASCADE_DEPTH_CAP: u32 = 16;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TriggerAuditEntry {
    pub trigger_name: String,
    pub firing_tx: TxId,
    /// How deep in the cascade the firing ran: 1 for a statement's own rows.
    pub depth: u32,
    /// The rows its callback wrote, its own cascade's not counted.
    pub cascade_row_count: u64,
    pub status: TriggerAuditStatus,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TriggerAuditStatus {
    #[default]
    Fired,
    RolledBack {
        reason: String,
    },
    DepthExceeded,
}
/// A write's firings: for each its name's place, depth and cascade-row count; each name once;
/// the write's transaction; and one status for them all, holding a refusal's text once.
pub(crate) type Firings = (Vec<(u32, u32, u32)>, Vec<String>, TxId, TriggerAuditStatus);
/// A running write's firings, held with it under one charge of its own.
pub(crate) type AuditQueue = (Firings, Reservation);
type Callback = dyn Fn(&Database, &TriggerContext) -> Result<()> + Send + Sync;
type CronCallback = dyn Fn(&Database) -> Result<()> + Send + Sync;
type CronEntry = Arc<(Box<CronCallback>, Reservation)>;
type Registered = BTreeMap<String, (Arc<Callback>, Reservation)>;
/// A callback on the stack: the thread it runs on, its name, and whether it is a schedule.
type ActiveCallback = (ThreadId, String, bool);
type CallbackTable = (Registered, Option<ActiveCallback>);

/// Keeps every firing of the write, its entry and a name new to the write reserved before
/// they are made.
pub(crate) fn audit_fired(
    ((entries, names, firing, _), held): &mut AuditQueue,
    name: &str,
    tx: TxId,
    depth: u32,
) -> Result<()> {
    let at = match names.iter().position(|known| known == name) {
        Some(at) => at,
        None => {
            held.room(names, usize::MAX)?;
            held.grow(name.len() as u64)?;
            names.push(name.into());
            names.len() - 1
        }
    };
    held.room(entries, usize::MAX)?;
    entries.push((at as u32, depth, 0));
    *firing = tx;
    Ok(())
}

/// Marks a refused write's firings rolled back with the refusal's text, kept once and charged
/// before it is made; the firing past the depth cap is stored `DepthExceeded`.
pub(crate) fn rolled_back(((_, _, _, status), held): &mut AuditQueue, error: &Error) -> Result<()> {
    let mut length = crate::store::record::Length(0);
    let _ = std::io::Write::write_fmt(&mut length, format_args!("{error}"));
    held.grow(length.0 as u64)?;
    let mut reason = String::with_capacity(length.0);
    let _ = std::fmt::Write::write_fmt(&mut reason, format_args!("{error}"));
    *status = TriggerAuditStatus::RolledBack { reason };
    Ok(())
}

/// The stamp of a stored firing the retention judged expired: no later period returns it.
pub(crate) const EXPIRED: i64 = i64::MIN;

/// Whether a firing stamped `stamp` is inside the retention `period` at `now`, in milliseconds.
pub(crate) fn retained(stamp: i64, now: i64, period: u64) -> bool {
    stamp != EXPIRED && i128::from(now) - i128::from(stamp) <= i128::from(period)
}

pub(crate) struct Triggers {
    callbacks: Mutex<CallbackTable>,
    cron: Mutex<BTreeMap<String, CronEntry>>,
    running: Mutex<()>,
    budget: Budget,
}

impl Triggers {
    pub(crate) fn new(budget: &Budget) -> Self {
        Self {
            callbacks: Mutex::new((BTreeMap::new(), None)),
            cron: Mutex::new(BTreeMap::new()),
            running: Mutex::new(()),
            budget: budget.clone(),
        }
    }

    pub(crate) fn register_cron<F>(&self, name: &str, callback: F) -> Result<()>
    where
        F: Fn(&Database) -> Result<()> + Send + Sync + 'static,
    {
        let held = self.budget.reserve(
            (name.len() + size_of::<F>() + 160) as u64,
            "schedule registration",
        )?;
        locked(&self.cron).insert(name.into(), Arc::new((Box::new(callback), held)));
        Ok(())
    }

    pub(crate) fn cron_names(&self) -> BTreeSet<String> {
        locked(&self.cron).keys().cloned().collect()
    }

    pub(crate) fn register<F>(&self, catalog: &Catalog, name: &str, callback: F) -> Result<()>
    where
        F: Fn(&Database, &TriggerContext) -> Result<()> + Send + Sync + 'static,
    {
        if !catalog.event_values().any(|(event, _)| {
            matches!(event,
            EventDeclaration::Trigger(trigger) if trigger.name == name)
        }) {
            return Err(Error::TriggerNotDeclared {
                trigger_name: name.into(),
            });
        }
        let mut registered = locked(&self.callbacks);
        let callbacks = &mut registered.0;
        if callbacks.contains_key(name) {
            return Err(Error::TriggerAlreadyRegistered {
                trigger_name: name.into(),
            });
        }
        let held = self.budget.reserve(
            (name.len() + size_of::<F>() + 160) as u64,
            "trigger callback registry",
        )?;
        callbacks.insert(name.into(), (Arc::new(callback), held));
        Ok(())
    }

    pub(crate) fn retiring<'a>(&'a self, changes: &ChangeSet) -> Result<impl FnOnce() + use<'a>> {
        let gone = self.registered(|n| changes.erased_schema(&Kind::Trigger.key(n)))?;
        Ok(move || locked(&self.callbacks).0.retain(|n, _| !gone.contains(n)))
    }

    pub(crate) fn registered(
        &self,
        filter: impl Fn(&String) -> bool,
    ) -> Result<crate::Owned<Vec<String>>> {
        let registered = locked(&self.callbacks);
        let names = registered.0.keys().filter(|n| filter(n));
        let bytes = names
            .clone()
            .map(|n| (size_of::<String>() + n.len()) as u64)
            .sum();
        let held = self.budget.reserve(bytes, "callback names answer")?;
        let mut answer = Vec::with_capacity(names.clone().count());
        answer.extend(names.cloned());
        Ok(crate::Owned::charged(answer, held))
    }

    pub(crate) fn firing_here(&self) -> bool {
        matches!(&locked(&self.callbacks).1, Some((on, ..)) if *on == std::thread::current().id())
    }

    pub(crate) fn running_elsewhere(&self) -> Option<(String, bool)> {
        let here = std::thread::current().id();
        let (_, on) = &*locked(&self.callbacks);
        on.clone().filter(|on| on.0 != here).map(|on| (on.1, on.2))
    }
    pub(crate) fn complete_initialization(&self, catalog: &Catalog) -> Result<()> {
        for (event, _) in catalog.event_values() {
            if let EventDeclaration::Trigger(trigger) = event {
                self.callback(&trigger.name)?;
            }
        }
        Ok(())
    }
    /// Refuses a write to `table` while one of its triggers has no callback.
    pub(crate) fn ready(this: Option<&Self>, catalog: &Catalog, table: &str) -> Result<()> {
        for (event, _) in catalog.event_values() {
            if let EventDeclaration::Trigger(t) = event
                && t.table == table
                && let Some(Err(e)) = this.map(|this| this.callback(&t.name))
            {
                let operation = e.to_string();
                return Err(Error::EngineNotInitialized { operation });
            }
        }
        Ok(())
    }

    fn callback(&self, name: &str) -> Result<Arc<Callback>> {
        locked(&self.callbacks)
            .0
            .get(name)
            .map(|(callback, _)| callback.clone())
            .ok_or_else(|| Error::TriggerCallbackMissing {
                trigger_name: name.into(),
            })
    }
}

/// Owned access to the same stage, available only on its dispatch thread while firing.
/// There is no writer/maintenance-worker ownership in a bound handle.
pub(crate) struct Bound {
    owner: ThreadId,
    state: Mutex<Option<(Arc<Open>, Staged)>>,
}

impl Bound {
    pub(crate) fn with_staged<T>(
        &self,
        run: impl FnOnce(&Arc<Open>, &mut Staged) -> Result<T>,
    ) -> Result<T> {
        if std::thread::current().id() != self.owner {
            return Err(Error::CallbackActive {
                operation: "a statement on another thread".into(),
            });
        }
        let mut state = locked(&self.state);
        let (open, staged) = state.as_mut().ok_or_else(|| Error::CallbackActive {
            operation: "a statement after callback dispatch".into(),
        })?;
        run(open, staged)
    }
}

/// Invalidates escaped handles on success, refusal and unwinding, releasing the open store.
struct Firing<'a>(Arc<Bound>, &'a Open);
impl Drop for Firing<'_> {
    fn drop(&mut self) {
        locked(&self.0.state).take();
        running(self.1, None, false);
    }
}

struct Dispatch {
    /// The initial rows' firings, fired before any a callback's writes give (`pending`).
    initial: VecDeque<(TriggerContext, Reservation)>,
    pending: VecDeque<(TriggerContext, Reservation)>,
    seen: BTreeSet<(String, TableId, Key)>,
    held: Reservation,
    /// The event the rows of a set written through fire, each read from its note.
    through: Option<TriggerEvent>,
}

/// Dispatches each trigger/table/key once; only new staging calls are examined after a callback.
/// The caller still owns the commit lock and performs the sole durable commit afterwards.
/// Written through, the statement's rows are read one at a time from their notes, as it left them.
pub(crate) fn fire(open: &Arc<Open>, staged: Staged) -> (Staged, Result<()>) {
    if staged.changes.is_empty() || staged.catalog.events.is_empty() {
        return (staged, Ok(()));
    }
    let budget = staged.budget.clone();
    let begun = open.cancels(0);
    let (changes, catalog) = (&staged.changes, &staged.catalog);
    let fires = |(table, _): (TableId, u64)| catalog.on(table, Kind::Trigger).next().is_some();
    let through = changes
        .operation
        .filter(|_| changes.streaming() && changes.changed_tables().any(fires));
    with_bound(open, staged, |bound, host| {
        let mut dispatch = Dispatch {
            initial: VecDeque::new(),
            pending: VecDeque::new(),
            seen: BTreeSet::new(),
            held: budget.reserve(0, "trigger dispatch identities")?,
            through,
        };
        bound.with_staged(|_, s| dispatch.enqueue(s, s.changes.rows_changed_since(0), 1))?;
        let mut after = vec![b'N'];
        loop {
            bound.with_staged(|_, staged| dispatch.noted(staged, &mut after))?;
            let next = dispatch.initial.pop_front();
            let Some((context, _held)) = next.or_else(|| dispatch.pending.pop_front()) else {
                break;
            };
            let (name, tx, depth) = (&context.trigger_name, context.tx, context.depth);
            bound.with_staged(|_, staged| audit_fired(&mut staged.audit, name, tx, depth))?;
            if context.depth > CASCADE_DEPTH_CAP {
                return Err(Error::TriggerCascadeDepthExceeded {
                    trigger_name: context.trigger_name,
                    depth: context.depth,
                });
            }
            let callback = open.triggers.callback(&context.trigger_name)?;
            running(open, Some(context.trigger_name.clone()), false);
            let mark = bound.with_staged(|_, staged| Ok(staged.changes.mark()))?;
            catch_unwind(AssertUnwindSafe(|| callback(host, &context))).or_else(|payload| {
                let text = payload.downcast_ref::<&str>().copied();
                let text = text.or_else(|| payload.downcast_ref::<String>().map(String::as_str));
                let text = text.unwrap_or("non-string panic payload");
                // The engine's copy is charged to the write's firings, before it is made.
                let bytes = "panic: ".len() + text.len();
                bound.with_staged(|_, staged| staged.audit.1.grow(bytes as u64))?;
                Err(Error::TriggerCallbackFailed {
                    trigger_name: context.trigger_name,
                    reason: format!("panic: {text}"),
                })
            })??;
            (open.cancels(0) == begun)
                .then_some(())
                .ok_or(Error::QueryCanceled {
                    statement_timeout: false,
                })?;
            bound.with_staged(|_, staged| {
                let written = staged.changes.rows_written_since(mark);
                let last = staged.audit.0.0.last_mut().expect("this firing's entry");
                last.2 = u32::try_from(written).unwrap_or(u32::MAX);
                let rows = staged.changes.rows_changed_since(mark);
                dispatch.enqueue(staged, rows, context.depth + 1)
            })?;
        }
        Ok(())
    })
}

fn running(open: &Open, name: Option<String>, schedule: bool) {
    let _turn = locked(&open.committing.0);
    let on = std::thread::current().id();
    locked(&open.triggers.callbacks).1 = name.map(|name| (on, name, schedule));
    open.committing.1.notify_all();
}

fn with_bound(
    open: &Arc<Open>,
    staged: Staged,
    run: impl FnOnce(&Arc<Bound>, &Database) -> Result<()>,
) -> (Staged, Result<()>) {
    let reader = staged.reader.clone();
    let active = Firing(
        Arc::new(Bound {
            owner: std::thread::current().id(),
            state: Mutex::new(Some((open.clone(), staged))),
        }),
        open,
    );
    let bound = &active.0;
    let host = Database::bound(bound.clone(), reader);
    let result = run(bound, &host);
    let (_, staged) = locked(&bound.state)
        .take()
        .expect("dispatch owns an active stage");
    (staged, result)
}

impl Dispatch {
    /// Queues the triggers `rows` fire; a row read from its note fires once and is not remembered.
    fn enqueue<'s>(
        &mut self,
        staged: &'s Staged,
        rows: impl Iterator<Item = (TableId, &'s Key, StagedValues<'s>, TriggerEvent)>,
        depth: u32,
    ) -> Result<()> {
        let noted = depth == 1 && self.through.is_some();
        for (table_id, key, (values, left), event) in rows {
            let Some(table) = staged.catalog.by_id(table_id) else {
                continue;
            };
            // A key with a note and a row in the view the statement left fired from its note.
            let changes = &staged.changes;
            if !noted
                && self.through == Some(event)
                && changes.noted(b'N', table_id, key, &staged.budget)?
                && changes.committed().contains(table_id, key)?
            {
                continue;
            }
            for (declaration, _) in staged.catalog.on(table_id, Kind::Trigger) {
                let EventDeclaration::Trigger(trigger) = declaration else {
                    continue;
                };
                if trigger.event != event {
                    continue;
                }
                // Charge the identity, temporary lookup key and queue slot before allocating.
                if !noted {
                    let bytes = (trigger.name.len() + key.as_bytes().len() + 512) as u64;
                    self.held.grow(bytes)?;
                    if !self
                        .seen
                        .insert((trigger.name.clone(), table_id, key.clone()))
                    {
                        self.held.shrink(bytes);
                        continue;
                    }
                }
                let mut row_held = staged.budget.reserve(
                    (trigger.name.len()
                        + trigger.table.len()
                        + table
                            .meta
                            .columns
                            .iter()
                            .map(|c| c.name.len() + 128)
                            .sum::<usize>()) as u64
                        + values.iter().map(heap_bytes).sum::<u64>(),
                    "trigger row context",
                )?;
                let mut row_values = HashMap::with_capacity(table.meta.columns.len());
                for (at, column) in table.meta.columns.iter().enumerate() {
                    let value = if let Some((_, beside)) = left.iter().find(|(slot, _)| *slot == at)
                    {
                        staged.changes.load(beside, &mut row_held)?
                    } else {
                        values[at].clone()
                    };
                    row_values.insert(column.name.clone(), value);
                }
                let queue = match depth {
                    1 => &mut self.initial,
                    _ => &mut self.pending,
                };
                self.held.room(queue, usize::MAX)?;
                queue.push_back((
                    TriggerContext {
                        trigger_name: trigger.name.clone(),
                        table: trigger.table.clone(),
                        event,
                        tx: staged.number,
                        depth,
                        row_values,
                    },
                    row_held,
                ));
            }
        }
        Ok(())
    }

    /// Queues the firings of the next noted row whose table fires, while none initial wait.
    fn noted(&mut self, staged: &Staged, after: &mut Vec<u8>) -> Result<()> {
        let (Some(event), false) = (self.through, after.is_empty()) else {
            return Ok(());
        };
        let (changes, budget) = (&staged.changes, &staged.budget);
        let fires = |id| staged.catalog.on(id, Kind::Trigger).next().is_some();
        let mut cover = budget.passing("trigger row")?;
        while self.initial.is_empty() && !after.is_empty() {
            let view = changes.committed();
            match changes.next_written(view, after, &fires, (budget, &mut cover))? {
                Some((id, key, Some(row))) => {
                    let (values, left) = crate::write::split(row.cells);
                    let row = (id, &key, (&values[..], &left[..]), event);
                    self.enqueue(staged, std::iter::once(row), 1)?;
                }
                Some(_) => {}
                None => after.clear(),
            }
        }
        Ok(())
    }
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
            if let Err(error) = result {
                let guard = open.turn()?;
                let mut settlement = open.begin()?;
                if settlement.catalog.schedules.get(name) == Some(&due) {
                    let mut catalog = settlement.catalog.next(&settlement.budget)?;
                    catalog.advance_schedule(name, next, &mut settlement.changes)?;
                    settlement.catalog = Arc::new(catalog);
                    open.commit(guard, settlement)?;
                }
                failure.get_or_insert(error);
            }
            changed = true;
        }
    }
    failure.map_or(Ok(changed), Err)
}
