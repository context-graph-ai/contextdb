//! Due index ranges, removed through DELETE in bounded commits.
use crate::database::{Open, locked};
use crate::events::{EXPIRED, retained};
use crate::schema::Table;
use crate::store::{AUDIT, Dropped, IndexId, Key, LAST_AUDIT, Read, VERSION_KEYS};
use crate::types::{History, Result, RetainUnit, Value};
use crate::{query, system, write};
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::sync::atomic::Ordering::Relaxed;

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

/// Judges up to 1,024 stored trigger firings by their own stamps, going on from where the last
/// cycle stopped, and from the oldest again once it reaches the newest, so an entry behind a
/// newer stamp is judged too. An expired one is removed or, while among the newest 1,024 the
/// log shows, kept with its stamp marked expired, so no later period or clock returns it to the
/// history. Returns how many it judged expired.
pub(super) fn audit(open: &Open) -> Result<u64> {
    let now = system::now_millis(&*open.clock);
    let period = open.audit_retention.load(Relaxed);
    let held = open.turn()?;
    let mut staged = open.begin()?;
    let (view, _) = open.newest()?;
    let head = view.moment.number(LAST_AUDIT)?;
    let mut swept = locked(&open.audit_swept);
    let from = [&[AUDIT][..], &swept.to_be_bytes()].concat();
    let (mut judged, mut expired) = (0, 0);
    view.deliveries(
        (Included(&from), Excluded(&[AUDIT + 1])),
        &mut |key, value| {
            let number = key
                .get(1..)
                .and_then(|n| n.try_into().ok())
                .map(u64::from_be_bytes);
            let stamp = value.first_chunk().map(|stamp| i64::from_be_bytes(*stamp));
            let bad = || view.damaged("an invalid audit record");
            let (number, stamp) = number.zip(stamp).ok_or_else(bad)?;
            let logged = number + 1024 >= head;
            let kept = retained(stamp, now, period) || logged && stamp == EXPIRED;
            if !kept {
                staged.changes.expire_audit(key, logged.then_some(value))?;
                expired += 1;
            }
            (judged, *swept) = (judged + 1, number + 1);
            Ok(judged < 1024)
        },
    )?;
    if judged < 1024 {
        *swept = 0;
    }
    if expired > 0 {
        open.commit(held, staged)?;
    }
    Ok(expired)
}
