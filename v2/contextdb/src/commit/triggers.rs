//! Host callbacks: the registered triggers and schedules, and the firing of a write's
//! triggers inside its own transaction.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex};
use std::thread::ThreadId;

use super::{Open, Staged};
use crate::events::{CASCADE_DEPTH_CAP, audit_fired};
use crate::memory::{Budget, Reservation};
use crate::query::Reader;
use crate::schema::Catalog;
use crate::sql::ast::EventDeclaration;
use crate::store::{ChangeSet, Key, Read, StagedValues, TableId};
use crate::system::locked;
use crate::types::{Error, Kind, Result, TriggerContext, TriggerEvent, heap_bytes};

type Callback = dyn Fn(&BoundHost, &TriggerContext) -> Result<()> + Send + Sync;
type CronCallback = dyn Fn(&BoundHost) -> Result<()> + Send + Sync;
type CronEntry = Arc<(Box<CronCallback>, Reservation)>;
type Registered = BTreeMap<String, (Arc<Callback>, Reservation)>;
/// A callback on the stack: the thread it runs on, its name, and whether it is a schedule.
type ActiveCallback = (ThreadId, String, bool);
type CallbackTable = (Registered, Option<ActiveCallback>);

pub(crate) struct Triggers {
    callbacks: Mutex<CallbackTable>,
    pub(crate) cron: Mutex<BTreeMap<String, CronEntry>>,
    pub(crate) running: Mutex<()>,
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
        F: Fn(&BoundHost) -> Result<()> + Send + Sync + 'static,
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
        F: Fn(&BoundHost, &TriggerContext) -> Result<()> + Send + Sync + 'static,
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
    pub(crate) fn ready(&self, catalog: &Catalog, table: &str) -> Result<()> {
        for (event, _) in catalog.event_values() {
            if let EventDeclaration::Trigger(t) = event
                && t.table == table
                && let Err(e) = self.callback(&t.name)
            {
                let operation = e.to_string();
                return Err(Error::EngineNotInitialized { operation });
            }
        }
        Ok(())
    }

    pub(crate) fn callback(&self, name: &str) -> Result<Arc<Callback>> {
        locked(&self.callbacks)
            .0
            .get(name)
            .map(|(callback, _)| callback.clone())
            .ok_or_else(|| Error::TriggerCallbackMissing {
                trigger_name: name.into(),
            })
    }
}

/// What a callback is handed: the firing stage and the reader that submitted the write. The
/// handle the host sees is made from it.
pub(crate) struct BoundHost {
    pub(crate) bound: Arc<Bound>,
    pub(crate) reader: Reader,
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

pub(crate) fn running(open: &Open, name: Option<String>, schedule: bool) {
    let _turn = locked(&open.committing.0);
    let on = std::thread::current().id();
    locked(&open.triggers.callbacks).1 = name.map(|name| (on, name, schedule));
    open.committing.1.notify_all();
}

pub(crate) fn with_bound(
    open: &Arc<Open>,
    staged: Staged,
    run: impl FnOnce(&Arc<Bound>, &BoundHost) -> Result<()>,
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
    let host = BoundHost {
        bound: bound.clone(),
        reader,
    };
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
                    let (values, left) = crate::store::split_cells(row.cells);
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
