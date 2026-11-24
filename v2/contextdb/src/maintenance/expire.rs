//! Due index ranges, removed through DELETE in bounded commits.
use crate::database::{Open, locked};
use crate::schema::Table;
use crate::store::{Dropped, IndexId, Key, Read, VERSION_KEYS};
use crate::types::{History, Result, RetainUnit, Value};
use crate::{query, system, write};
use std::ops::Bound::{Excluded, Included, Unbounded};

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

pub(super) fn run(open: &Open) -> Result<(u64, u64, bool)> {
    let now = system::now_millis(&*open.clock);
    let (_, catalog) = open.newest()?;
    let mut previous = locked(&open.expiry);
    let tables = catalog.tables();
    previous.retain(|id, _| tables.iter().any(|t| t.index_ids.contains(id)));
    let (mut backlog, mut more) = (8192, false);
    let (mut expired, mut skipped) = (0, 0);
    for init in tables.into_iter().filter(|t| t.meta.retain.is_some()) {
        let ids = init.index_ids.iter().enumerate().filter(|(at, _)| {
            *at == init.meta.indexes.len() || init.meta.indexes[*at].name.starts_with("__retain_")
        });
        for (_, &id) in ids {
            let was = previous.get(&id).cloned();
            let mut upper = None;
            for old in [false, true]
                .into_iter()
                .filter(|old| *old || was.is_some())
            {
                let mut after = None;
                loop {
                    let held = open.turn()?;
                    let mut staged = open.begin()?;
                    let Some(table) = staged
                        .catalog
                        .table(&init.name)
                        .filter(|t| t.index_ids.contains(&id))
                    else {
                        break;
                    };
                    let Some(now_bound) = bound(table, id, now)? else {
                        break;
                    };
                    upper = Some(now_bound.clone());
                    let start = if old {
                        Unbounded
                    } else {
                        Excluded(was.as_ref().unwrap())
                    };
                    let to = if old {
                        was.as_ref().map_or(&now_bound, |w| w.min(&now_bound))
                    } else {
                        &now_bound
                    };
                    let mut room = open.budget.reserve(0, "expiry keys")?;
                    let mut keys = Vec::new();
                    let place = after.as_ref().map(|(key, _)| key);
                    let (start, to) = query::resumed((start, Included(to)), place, false);
                    for entry in staged.changes.index(id, start, to, false)? {
                        let (entry, key, _) = entry?;
                        if old && backlog == 0 {
                            more = true;
                            break;
                        }
                        room.grow(key.as_bytes().len() as u64 + 64)?;
                        after = Some(query::index_place(&entry, &key, &mut room)?);
                        room.room(&mut keys, 1024)?;
                        keys.push(key);
                        if keys.len() == if old { backlog.min(1024) } else { 1024 } {
                            break;
                        }
                    }
                    if keys.is_empty() {
                        break;
                    }
                    keys.sort();
                    let (done, deferred) = write::expire_keys(&init.name, &keys, &mut staged)?;
                    open.commit(held, staged)?;
                    expired += done;
                    skipped += deferred;
                    if old {
                        backlog -= done as usize;
                    }
                }
            }
            if let Some(upper) = upper {
                previous.insert(id, was.map_or(upper.clone(), |w| w.max(upper)));
            }
        }
    }
    Ok((expired, skipped, more || (expired > 0 && skipped > 0)))
}

/// Removes the versions of `HISTORY CURRENT ONLY` tables, returning how many and whether more.
pub(super) fn versions(open: &Open) -> Result<(u64, bool)> {
    let (_, catalog) = open.newest()?;
    let (mut removed, mut more) = (0, false);
    let current = |t: &&Table| t.meta.history == Some(History::CurrentOnly);
    for table in catalog.tables().into_iter().filter(current) {
        let held = open.turn()?;
        let mut staged = open.begin()?;
        let (mut last, mut keys) = (None, 0);
        for key in staged.changes.base().versioned(table.id)? {
            let key = Some(key?);
            if key != last {
                if keys == VERSION_KEYS {
                    more = true;
                    break;
                }
                (last, keys) = (key, keys + 1);
            }
            removed += 1;
        }
        if keys > 0 {
            staged.changes.stage_drop(Dropped::Versions(table.id))?;
        }
        open.commit(held, staged)?;
    }
    Ok((removed, more))
}
