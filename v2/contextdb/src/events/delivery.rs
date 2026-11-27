//! Durable routed records and the one post-commit dispatcher.
use super::*;
use crate::query::Reader;
use crate::store::{
    Cell, ChangeSet, DELIVERY, View, delivery_identity, put_text, put_value, read_value, value_len,
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
    pub(crate) callbacks: Mutex<BTreeMap<String, SinkEntry>>,
    pub(crate) active: Mutex<()>,
    pub(crate) counters: Mutex<BTreeMap<String, (SinkMetrics, Reservation, u64)>>,
    /// Each schedule's last failure that ended no cycle, until it next runs.
    pub(crate) failed: Mutex<BTreeMap<String, String>>,
}
impl Deliveries {
    pub(crate) fn register<F>(
        &self,
        (catalog, budget): (&Catalog, &Budget),
        name: &str,
        reader: &Reader,
        principal: Option<Principal>,
        deliver: F,
    ) -> Result<()>
    where
        F: Fn(&SinkEvent) -> std::result::Result<(), SinkError> + Send + Sync + 'static,
    {
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
        let held = budget.reserve(bytes as u64, "sink registration")?;
        let mut reader = reader.clone();
        reader.principal = principal;
        locked(&self.callbacks).insert(name.into(), Arc::new((reader, Box::new(deliver), held)));
        Ok(())
    }
}
pub(crate) struct Delivery {
    row_key: Key,
    pub(crate) table: TableId,
    pub(crate) sink: String,
    pub(crate) event: SinkEvent,
    pub(crate) attempts: u64,
    pub(crate) flag: u8,
}
pub(crate) fn key(action: u64) -> [u8; 9] {
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
    let routed = |id: TableId| routes.iter().any(|(t, _, _)| t.id == id);
    let mut changed = Vec::new();
    for (table, key, values, inserted) in changes.rows_changed() {
        if routed(table) {
            held.grow(key.as_bytes().len() as u64 + 64)?;
            changed.push((table, key.clone(), values.is_some(), inserted));
        }
    }
    // Written through, a row is found by its note and read back: new when the set began
    // without it, and read as the set began when it is gone.
    let (first, mut after) = (changes.first().clone(), vec![b'N']);
    let (mut changed, mut queued) = (changed.into_iter(), false);
    let mut read = budget.passing("routed row")?;
    loop {
        let (id, row_key, present, inserted) = match changed.next() {
            Some(row) => row,
            None if !routes.is_empty() && changes.streaming() => {
                let cover = (budget, &mut read);
                let next = changes.next_written(&*changes, &mut after, &routed, cover)?;
                let Some((id, key, row)) = next else {
                    break;
                };
                let inserted = !first.contains(id, &key)?;
                (id, key, row.is_some(), inserted)
            }
            None => break,
        };
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
                let from = if present { view } else { &first };
                let base = match staged {
                    Some(_) => None,
                    None => from.row(id, &row_key, &mut cover)?,
                };
                let cell = |at: usize, loaded: &mut Reservation| -> Result<Cow<'_, Value>> {
                    if let Some((values, left)) = staged {
                        return match left.iter().find(|(slot, _)| *slot == at) {
                            Some((_, beside)) => view.load(beside, loaded).map(Cow::Owned),
                            None => Ok(Cow::Borrowed(values.get(at).unwrap_or(&table.missing[at]))),
                        };
                    }
                    match base.as_ref().expect("a row read back").cells.get(at) {
                        Some(Cell::Here(value)) => Ok(Cow::Borrowed(value)),
                        Some(Cell::Beside(beside)) => from.load(beside, loaded).map(Cow::Owned),
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
pub(crate) fn identity(
    view: &View,
    key: &[u8],
    bytes: &[u8],
    held: &mut Reservation,
) -> Result<Delivery> {
    if key.len() != 9 || key[0] != DELIVERY || key[1..] == [0; 8] {
        return Err(view.damaged("an invalid delivery key"));
    }
    let mut bytes =
        delivery_identity(bytes).ok_or_else(|| view.damaged("an invalid delivery record"))?;
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
pub(crate) fn load_body(
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

/// What an erase does with the key and row of a delivery it takes.
pub(crate) type Taken<'a> = dyn FnMut(&mut ChangeSet, &Key, &[Value]) -> Result<()> + 'a;

/// Stages the removal of each queued delivery of a `table` row that is gone from the
/// statement's first view, keeps no version there, and whose stored row `passes`, handing
/// `each` its key and row: one walk of the queue, loading a body only for a row that is gone
/// and, when the condition reaches only named keys (`reach`, in key order), one of them.
pub(crate) fn erase_gone(
    slot: &std::cell::RefCell<&mut ChangeSet>,
    (table, reach): (&crate::schema::Table, Option<&[Key]>),
    budget: &Budget,
    passes: &mut dyn FnMut(&[Value]) -> Result<bool>,
    each: &mut Taken,
) -> Result<()> {
    let first = slot.borrow().first().clone();
    let mut cover = budget.passing("purge delivery row")?;
    let (from, upper) = (key(0), key(u64::MAX));
    let bounds = (Excluded(from.as_slice()), Included(upper.as_slice()));
    first.deliveries(bounds, &mut |k, bytes| {
        let mut decoded = budget.reserve(0, "purge delivery")?;
        let mut d = identity(&first, k, bytes, &mut decoded)?;
        if d.table != table.id
            || reach.is_some_and(|keys| keys.binary_search(&d.row_key).is_err())
            || first.row(table.id, &d.row_key, &mut cover)?.is_some()
            || first
                .versions(table.id, &d.row_key, &mut cover)?
                .next()
                .is_some()
        {
            return Ok(true);
        }
        load_body(&first, bytes, &mut d, &mut decoded)?;
        let columns = table.meta.columns.iter().zip(&table.missing);
        let row: Vec<_> = columns
            .map(|(c, missing)| {
                d.event
                    .row_values
                    .remove(&c.name)
                    .unwrap_or(missing.clone())
            })
            .collect();
        if passes(&row)? {
            let changes = &mut **slot.borrow_mut();
            changes.put_delivery(Some(d.event.action_id), None, |_, _, _| Ok(Some(None)))?;
            each(changes, &d.row_key, &row)?;
        }
        Ok(true)
    })
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
                if changes.noted(b'N', d.table, &d.row_key, budget)? {
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
    pub(crate) fn examine(&self, budget: &Budget, sink: &str, action: u64) -> Result<()> {
        let mut counters = locked(&self.counters);
        if !counters.contains_key(sink) {
            let held = budget.reserve((sink.len() + 256) as u64, "sink counters")?;
            counters.insert(sink.into(), (SinkMetrics::default(), held, 0));
        }
        let (metrics, _, seen) = counters.get_mut(sink).expect("counter inserted");
        metrics.examined += u64::from(action > *seen);
        *seen = (*seen).max(action);
        Ok(())
    }
    pub(crate) fn metrics(&self, budget: &Budget, view: &View, sink: &str) -> Result<SinkMetrics> {
        let (mut metrics, _held) = self.collected(budget, view, true)?;
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
                    last_error: locked(&self.failed).get(&d.name).cloned(),
                }),
                _ => {}
            }
        }
        Ok(crate::Owned::charged(status, held))
    }
}
