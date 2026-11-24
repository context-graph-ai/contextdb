//! Durable routed records and the one post-commit dispatcher.
use super::*;
use crate::database::ScheduleStatus;
use crate::database::{EventBusStatus, EventTypeStatus, RouteStatus, SinkMetrics, SinkStatus};
use crate::maintenance::MaintenanceReport;
use crate::query::{Reader, Visible};
use crate::store::{
    Cell, ChangeSet, DELIVERY, View, delivery_parts, put_text, put_value, read_value, value_len,
};
use crate::types::{EventTypeTrigger, Kind, Principal, SinkError, SinkEvent, TxId, Value};
use std::borrow::Cow;
use std::collections::BTreeSet;
use std::ops::Bound::{Excluded, Included};

type Handler = dyn Fn(&SinkEvent) -> std::result::Result<(), SinkError> + Send + Sync;
type SinkEntry = Arc<(Reader, Box<Handler>, Reservation)>;
/// Each sink's counters, with the count of its uncertain pending deliveries beside them.
type Metrics = BTreeMap<String, (SinkMetrics, u64)>;
#[derive(Default)]
pub(crate) struct Deliveries {
    callbacks: Mutex<BTreeMap<String, SinkEntry>>,
    pub(crate) active: Mutex<()>,
    counters: Mutex<BTreeMap<String, (SinkMetrics, Reservation, u64)>>,
}
impl Deliveries {
    pub(crate) fn register<F>(
        &self,
        open: &Open,
        name: &str,
        reader: &Reader,
        principal: Option<Principal>,
        deliver: F,
    ) -> Result<()>
    where
        F: Fn(&SinkEvent) -> std::result::Result<(), SinkError> + Send + Sync + 'static,
    {
        let (_, catalog) = open.newest()?;
        if !catalog.events.contains_key(&(Kind::Sink, name.to_owned())) {
            return Err(Error::SinkNotDeclared {
                sink_name: name.into(),
            });
        }
        if matches!((&reader.principal, &principal), (Some(a), Some(b)) if **a != *b) {
            return Err(Error::SinkPrincipalMismatch);
        }
        let principal = reader.principal.clone().or_else(|| principal.map(Arc::new));
        let bytes = name.len() + size_of::<F>() + 256;
        let held = open.budget.reserve(bytes as u64, "sink registration")?;
        let mut reader = reader.clone();
        reader.principal = principal;
        locked(&self.callbacks).insert(name.into(), Arc::new((reader, Box::new(deliver), held)));
        open.signal.wake();
        Ok(())
    }
}
pub(crate) struct Delivery {
    row_key: Key,
    table: TableId,
    sink: String,
    event: SinkEvent,
    attempts: u64,
    flag: u8,
}
fn key(action: u64) -> [u8; 9] {
    let mut key = [0; 9];
    key[0] = DELIVERY;
    key[1..].copy_from_slice(&action.to_be_bytes());
    key
}
pub(crate) fn prepare(changes: &mut ChangeSet, catalog: &Catalog, budget: &Budget) -> Result<bool> {
    let mut held = budget.reserve(0, "delivery preparation")?;
    let mut routes = Vec::new();
    for (id, _) in changes.changed_tables() {
        let table = catalog.by_id(id);
        for (route, _) in catalog.on(id, Kind::Route) {
            let (Some(table), EventDeclaration::Route(route)) = (table, route) else {
                continue;
            };
            let key = (Kind::EventType, route.event_type.clone());
            if let Some((EventDeclaration::EventType(event), _)) = catalog.event(&key) {
                held.grow(64)?;
                routes.push((table, event, route));
            }
        }
    }
    routes.sort_by_key(|(_, event, route)| (&event.name, &route.name));
    let mut changed = Vec::new();
    for (table, key, values, inserted) in changes.rows_changed() {
        if routes.iter().any(|(t, _, _)| t.id == table) {
            held.grow(key.as_bytes().len() as u64 + 64)?;
            changed.push((table, key.clone(), values.is_some(), inserted));
        }
    }
    let mut queued = false;
    for (id, row_key, present, inserted) in changed {
        if !present && inserted {
            continue;
        }
        let kind = if !present {
            EventTypeTrigger::Delete
        } else if inserted {
            EventTypeTrigger::Insert
        } else {
            EventTypeTrigger::Update
        };
        for (table, event, route) in routes
            .iter()
            .filter(|(t, e, _)| t.id == id && e.event == kind)
        {
            let commit = changes.commit_number();
            let row = Some((id, &row_key));
            let mut cover = held.passing();
            queued |= changes.put_delivery(None, row, |view, staged, encoded| {
                let base = if present {
                    None
                } else {
                    view.row(id, &row_key, &mut cover)?
                };
                let cell = |at: usize, loaded: &mut Reservation| -> Result<Cow<'_, Value>> {
                    if let Some((values, left)) = staged {
                        return match left.iter().find(|(slot, _)| *slot == at) {
                            Some((_, beside)) => view.load(beside, loaded).map(Cow::Owned),
                            None => Ok(Cow::Borrowed(values.get(at).unwrap_or(&table.missing[at]))),
                        };
                    }
                    match base.as_ref().expect("a deleted row exists").cells.get(at) {
                        Some(Cell::Here(value)) => Ok(Cow::Borrowed(value)),
                        Some(Cell::Beside(beside)) => view.load(beside, loaded).map(Cow::Owned),
                        None => Ok(Cow::Borrowed(&table.missing[at])),
                    }
                };
                let mut loaded = budget.reserve(0, "delivery cell")?;
                if let Some(predicate) = &route.where_in {
                    let at = table.column(&predicate.column)?;
                    let value = cell(at, &mut loaded)?;
                    if !matches!(value.as_ref(), Value::Text(s) if predicate.values.contains(s)) {
                        return Ok(None);
                    }
                }
                loaded.shrink(loaded.bytes());
                let texts = [&route.sink, &event.name, &table.name];
                let mut size = 30
                    + row_key.as_bytes().len()
                    + texts.iter().map(|text| 9 + text.len()).sum::<usize>();
                for (at, column) in table.meta.columns.iter().enumerate() {
                    size += 9 + column.name.len() + value_len(cell(at, &mut loaded)?.as_ref());
                    loaded.shrink(loaded.bytes());
                }
                encoded.grow(size as u64)?;
                let mut out = Vec::with_capacity(size);
                out.extend([0; 4]);
                out.extend(id.0.to_be_bytes());
                out.extend(commit.0.to_be_bytes());
                out.extend([0; 9]);
                out.extend((row_key.as_bytes().len() as u32).to_le_bytes());
                out.extend(row_key.as_bytes());
                for text in texts {
                    put_text(&mut out, text);
                }
                let identity = (out.len() - 4) as u32;
                out[..4].copy_from_slice(&identity.to_le_bytes());
                out.push(0);
                for (at, column) in table.meta.columns.iter().enumerate() {
                    put_text(&mut out, &column.name);
                    let value = cell(at, &mut loaded)?;
                    put_value(&mut out, &value);
                    drop(value);
                    loaded.shrink(loaded.bytes());
                }
                Ok(Some(Some(out)))
            })?;
        }
    }
    Ok(queued)
}
fn identity(view: &View, key: &[u8], bytes: &[u8], held: &mut Reservation) -> Result<Delivery> {
    if key.len() != 9 || key[0] != DELIVERY || key[1..] == [0; 8] {
        return Err(view.damaged("an invalid delivery key"));
    }
    let (mut bytes, _, _) =
        delivery_parts(bytes).map_err(|_| view.damaged("an invalid delivery record"))?;
    held.grow(1024)?;
    let bad = || view.damaged("an invalid delivery record");
    let (front, rest) = bytes.split_first_chunk::<21>().ok_or_else(bad)?;
    let table = TableId(u32::from_be_bytes(front[..4].try_into().unwrap()));
    let at_lsn = TxId(u64::from_be_bytes(front[4..12].try_into().unwrap()));
    let attempts = u64::from_be_bytes(front[12..20].try_into().unwrap());
    if front[20] > 2 || at_lsn.0 == 0 {
        return Err(bad());
    }
    let flag = front[20];
    let (length, rest) = rest.split_first_chunk::<4>().ok_or_else(bad)?;
    let (row_key, rest) = rest
        .split_at_checked(u32::from_le_bytes(*length) as usize)
        .ok_or_else(bad)?;
    if row_key.is_empty() {
        return Err(bad());
    }
    held.grow(row_key.len() as u64)?;
    let row_key = Key::from_bytes(row_key);
    bytes = rest;
    let text = |bytes: &mut &[u8], held: &mut Reservation| match read_value(bytes, held)? {
        Some(Value::Text(s)) => Ok(s),
        _ => Err(bad()),
    };
    let sink = text(&mut bytes, held)?;
    let event_type = text(&mut bytes, held)?;
    let table_name = text(&mut bytes, held)?;
    if !bytes.is_empty() {
        return Err(bad());
    }
    Ok(Delivery {
        row_key,
        table,
        sink,
        event: SinkEvent {
            action_id: u64::from_be_bytes(key[1..].try_into().unwrap()),
            event_type,
            table: table_name,
            row_values: Default::default(),
            severity: String::new(),
            at_lsn,
        },
        attempts,
        flag,
    })
}
fn load_body(
    view: &View,
    bytes: &[u8],
    delivery: &mut Delivery,
    held: &mut Reservation,
) -> Result<()> {
    let body = view.delivery_body(bytes, held)?;
    let copied = u64::from(matches!(body, Cow::Owned(_))) * body.len() as u64;
    let mut bytes = body.as_ref();
    let bad = || view.damaged("an invalid delivery body");
    let event = &mut delivery.event;
    while !bytes.is_empty() {
        held.grow(128)?;
        let name = match read_value(&mut bytes, held)? {
            Some(Value::Text(name)) => name,
            _ => return Err(bad()),
        };
        let value = read_value(&mut bytes, held)?.ok_or_else(bad)?;
        if event.row_values.insert(name, value).is_some() {
            return Err(bad());
        }
    }
    event.severity = if let Some(Value::Text(text)) = event.row_values.get("severity") {
        held.grow(text.len() as u64)?;
        text.clone()
    } else {
        String::new()
    };
    drop(body);
    held.shrink(copied);
    Ok(())
}

pub(crate) fn validate(
    view: &View,
    key: &[u8],
    bytes: &[u8],
    held: &mut Reservation,
) -> Result<Delivery> {
    let mut delivery = identity(view, key, bytes, held)?;
    load_body(view, bytes, &mut delivery, held)?;
    Ok(delivery)
}
thread_local! {
    pub(crate) static IN_SINK_CALL: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}
type Change = (u64, Option<(u64, u8)>);
type Tally = (String, bool, bool, bool, bool);
fn settle(
    open: &Open,
    report: &mut MaintenanceReport,
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
    *report.delivered.get_or_insert(0) += u64::from(success);
    *report.deferred.get_or_insert(0) += u64::from(!success && !terminal);
    Ok(success || terminal)
}
/// One finite pass, with no view, registry lock or commit lock held during host code.
pub(crate) fn deliver(open: &Open, report: &mut MaintenanceReport) -> Result<(bool, bool)> {
    let Ok(_dispatch) = open.deliveries.active.try_lock() else {
        return Ok((false, false));
    };
    let mut carried = None;
    let done = pass(open, report, &mut carried);
    let last = settle(open, report, carried, None)?;
    done.map(|(progressed, more)| (progressed | last, more))
}
fn pass(
    open: &Open,
    report: &mut MaintenanceReport,
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
                open.deliveries.examine(open, sink, action)?;
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
                    *report.deferred.get_or_insert(0) += 1;
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
        progressed |= settle(open, report, carried.take(), mark)?;
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

/// PURGE stages no deliveries of its own; only prior removals overlay this walk.
pub(crate) fn erase(changes: &mut ChangeSet, budget: &Budget) -> Result<()> {
    let _held = budget.reserve(256 * 8, "purge deliveries")?;
    let mut cursor = key(0);
    loop {
        let mut actions = [0; 256];
        let mut visited = 0;
        let from = cursor;
        changes.base().deliveries(
            (
                Excluded(from.as_slice()),
                Included(key(u64::MAX).as_slice()),
            ),
            &mut |k, bytes| {
                let mut decoded = budget.reserve(0, "purge delivery")?;
                let d = identity(changes.base(), k, bytes, &mut decoded)?;
                cursor.copy_from_slice(k);
                if changes.noted(d.table, &d.row_key, budget)? {
                    actions[visited] = d.event.action_id;
                }
                visited += 1;
                Ok(visited < actions.len())
            },
        )?;
        for action in actions[..visited].iter().filter(|action| **action != 0) {
            changes.put_delivery(Some(*action), None, |_, _, _| Ok(Some(None)))?;
        }
        if visited < actions.len() {
            break;
        }
    }
    Ok(())
}

impl Deliveries {
    fn examine(&self, open: &Open, sink: &str, action: u64) -> Result<()> {
        let mut counters = locked(&self.counters);
        if !counters.contains_key(sink) {
            let held = open
                .budget
                .reserve((sink.len() + 256) as u64, "sink counters")?;
            counters.insert(sink.into(), (SinkMetrics::default(), held, 0));
        }
        let (metrics, _, seen) = counters.get_mut(sink).expect("counter inserted");
        metrics.examined += u64::from(action > *seen);
        *seen = (*seen).max(action);
        Ok(())
    }
    pub(crate) fn metrics(&self, open: &Open, view: &View, sink: &str) -> Result<SinkMetrics> {
        let (mut metrics, _held) = self.collected(&open.budget, view, true)?;
        Ok(metrics.remove(sink).unwrap_or_default().0)
    }
    fn collected(
        &self,
        budget: &Budget,
        view: &View,
        live: bool,
    ) -> Result<(Metrics, Reservation)> {
        let mut held = budget.reserve(0, "sink metrics")?;
        let mut metrics = BTreeMap::new();
        for (name, (counters, ..)) in locked(&self.counters).iter().filter(|_| live) {
            held.grow((name.len() + 256) as u64)?;
            metrics.insert(name.clone(), (counters.clone(), 0));
        }
        view.deliveries(
            (
                Included(key(0).as_slice()),
                Included(key(u64::MAX).as_slice()),
            ),
            &mut |key, bytes| {
                let mut loaded = budget.reserve(0, "pending delivery metrics")?;
                let delivery = identity(view, key, bytes, &mut loaded)?;
                if !metrics.contains_key(&delivery.sink) {
                    held.grow((delivery.sink.len() + 256) as u64)?;
                }
                let (counters, uncertain) = metrics.entry(delivery.sink).or_default();
                counters.queued += 1;
                *uncertain += u64::from(delivery.flag != 0);
                Ok(true)
            },
        )?;
        for (counters, uncertain) in metrics.values_mut() {
            counters.uncertain = counters.uncertain.max(*uncertain);
        }
        Ok((metrics, held))
    }
    pub(crate) fn status(
        &self,
        view: &View,
        catalog: &Catalog,
        budget: &Budget,
        live: bool,
        cron: &BTreeSet<String>,
    ) -> Result<crate::Owned<EventBusStatus>> {
        let (mut metrics, _metrics) = self.collected(budget, view, live)?;
        let held = catalog.answering(budget, 1)?;
        let callbacks = locked(&self.callbacks);
        let mut status = EventBusStatus {
            live,
            event_types: Vec::new(),
            sinks: Vec::new(),
            routes: Vec::new(),
            schedules: Vec::new(),
        };
        for (event, _) in catalog.event_values() {
            match event {
                EventDeclaration::EventType(event) => status.event_types.push(EventTypeStatus {
                    name: event.name.clone(),
                    trigger: format!("{:?}", event.event).to_uppercase(),
                    table: event.table.clone(),
                }),
                EventDeclaration::Sink(sink) => status.sinks.push(SinkStatus {
                    name: sink.name.clone(),
                    sink_type: match sink.sink_type {
                        crate::types::SinkType::Callback => "callback",
                        crate::types::SinkType::Webhook => "webhook",
                    }
                    .into(),
                    callback_registered: live && callbacks.contains_key(&sink.name),
                    metrics: metrics.remove(&sink.name).unwrap_or_default().0,
                }),
                EventDeclaration::Route(route) => status.routes.push(RouteStatus {
                    name: route.name.clone(),
                    event_type: route.event_type.clone(),
                    sink: route.sink.clone(),
                }),
                EventDeclaration::Schedule(d) => status.schedules.push(ScheduleStatus {
                    name: d.name.clone(),
                    callback_registered: live && cron.contains(&d.callback),
                }),
                _ => {}
            }
        }
        Ok(crate::Owned::charged(status, held))
    }
}
