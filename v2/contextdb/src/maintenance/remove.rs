//! The one removal loop: rows past their table's `RETAIN` bound, deleted as a statement deletes
//! them, and the records owed ranges hold, a batch of at most [`BATCH`] records at a time. A
//! cycle runs at most eight batches, taking the sources in turn, and looks for a cancel between
//! them.
use std::ops::Bound::{Included, Unbounded};

use super::{MaintenanceReport, State};
use crate::commit::Open;
use crate::schema::Table;
use crate::store::{BATCH, IndexId, Key, Read, Reclaim};
use crate::system::locked;
use crate::types::{Error, Result, RetainUnit, Value};
use crate::{query, stage, system};

fn bound(table: &Table, id: IndexId, now: i64) -> Result<Option<Key>> {
    let value = if table.index_ids.last() == Some(&id) {
        let retain = table.meta.retain.as_ref().unwrap();
        let scale = match retain.unit {
            RetainUnit::Seconds => 1,
            RetainUnit::Minutes => 60,
            RetainUnit::Hours => 3600,
            RetainUnit::Days => 86400,
        };
        let now = (now / 1000).clamp(0, i64::from(u32::MAX)) as u64;
        now.checked_sub(retain.count.saturating_mul(scale))
            .filter(|&n| n >= u64::from(table.rules.retain_since.unwrap()))
            .map(|n| Value::Int64(n as i64))
    } else {
        Some(Value::Timestamp(now.clamp(-1, i64::MAX - 1)))
    };
    value.map(|v| Key::from_values(&[v], &[])).transpose()
}

fn add(field: &mut Option<u64>, n: u64) {
    *field = Some(field.unwrap_or(0) + n);
}

/// Runs the cycle's batches into `report`; true when more is due or owed.
pub(super) fn run(
    open: &Open,
    state: &State,
    report: &mut MaintenanceReport,
    check: &dyn Fn() -> Result<()>,
) -> Result<bool> {
    let now = system::now_millis(&*open.clock);
    let (_, catalog) = open.newest()?;
    let tables = catalog.tables();
    locked(&state.expiry).retain(|id, _| tables.iter().any(|t| t.index_ids.contains(id)));
    let mut units = Vec::new();
    for table in tables.into_iter().filter(|t| t.meta.retain.is_some()) {
        let ids = table.index_ids.iter().enumerate().filter(|(at, _)| {
            *at == table.meta.indexes.len() || table.meta.indexes[*at].name.starts_with("__retain_")
        });
        units.extend(ids.map(|(_, id)| Some((table.name.clone(), *id))));
    }
    units.push(None);
    drop(catalog);
    for field in [&mut report.expired_rows, &mut report.versions_removed] {
        add(field, 0);
    }
    add(&mut report.removed_records, 0);
    // The cycle starts at the source after the last one the cycle before it served.
    let mut at = *locked(&state.removal_from) % units.len();
    let (mut busy, mut batches) = (vec![true; units.len()], 0);
    while batches < 8 && busy.contains(&true) {
        if busy[at] {
            let batch = match &units[at] {
                Some((name, id)) => expire(open, state, name, *id, now, report),
                None => owed(open, report),
            };
            // A refusal is this source's alone; the cycle goes on.
            let (committed, more) = match batch {
                Err(Error::MemoryBudgetExceeded { .. }) => {
                    report.memory_refused = Some(true);
                    (false, false)
                }
                Err(Error::DiskLimitExceeded { .. }) => (false, false),
                other => other?,
            };
            (busy[at], batches) = (more, batches + usize::from(committed));
            if committed {
                *locked(&state.removal_from) = at + 1;
                check()?;
            }
        }
        at = (at + 1) % units.len();
    }
    // A row a removed one held is free for the next walk.
    let freed = report.expired_rows > Some(0) && report.deferred > Some(0);
    Ok(busy.contains(&true) || freed)
}

/// One batch of rows past `id`'s bound, from where the last batch of the walk stopped: rows until
/// their records reach [`BATCH`], and at least one. Rows a reference holds are passed by until
/// the walk begins again.
fn expire(
    open: &Open,
    state: &State,
    name: &str,
    id: IndexId,
    now: i64,
    report: &mut MaintenanceReport,
) -> Result<(bool, bool)> {
    let held = open.turn()?;
    let mut staged = open.begin()?;
    let catalog = staged.catalog.clone();
    let Some(table) = catalog.table(name).filter(|t| t.index_ids.contains(&id)) else {
        return Ok((false, false));
    };
    let Some(to) = bound(table, id, now)? else {
        return Ok((false, false));
    };
    let cost = 1 + table.index_ids.len() as u64 + table.vectors.len() as u64;
    let after = locked(&state.expiry).get(&id).cloned();
    let mut room = open.budget.reserve(0, "expiry keys")?;
    let (mut keys, mut place, mut more) = (Vec::new(), None, false);
    let (start, to) = query::resumed((Unbounded, Included(&to)), after.as_ref(), false);
    for entry in staged.changes.index(id, start, to, false)? {
        if keys.len() as u64 * cost >= BATCH {
            more = true;
            break;
        }
        let (entry, key, _) = entry?;
        room.grow(key.as_bytes().len() as u64 + 64)?;
        place = Some(query::index_place(&entry, &key, &mut room)?.0);
        room.room(&mut keys, 1024)?;
        keys.push(key);
    }
    match place.filter(|_| more) {
        Some(place) => locked(&state.expiry).insert(id, place),
        None => locked(&state.expiry).remove(&id),
    };
    if keys.is_empty() {
        return Ok((false, false));
    }
    keys.sort();
    let (done, deferred) = stage::expire_keys(
        name,
        &keys,
        &staged.catalog,
        &staged.budget,
        &mut staged.changes,
        &mut staged.partitions,
    )?;
    open.commit(held, staged)?;
    add(&mut report.expired_rows, done);
    add(&mut report.removed_records, done * cost);
    add(&mut report.deferred, deferred);
    Ok((true, more))
}

/// One batch of the records owed ranges hold, from the first mark.
fn owed(open: &Open, report: &mut MaintenanceReport) -> Result<(bool, bool)> {
    let held = open.turn()?;
    let mut staged = open.begin()?;
    if !open.newest()?.0.any_owed()? {
        report.owed = Some(false);
        return Ok((false, false));
    }
    let asked = std::sync::Arc::new(std::sync::Mutex::new(Reclaim::default()));
    staged.changes.reclaim = Some(asked.clone());
    open.commit(held, staged)?;
    let asked = std::mem::take(&mut *locked(&asked));
    add(&mut report.removed_records, asked.removed);
    add(&mut report.versions_removed, asked.versions);
    report.removal_advanced = Some(report.removal_advanced == Some(true) || asked.advanced);
    let more = open.newest()?.0.any_owed()?;
    report.owed = Some(more);
    Ok((true, more))
}
