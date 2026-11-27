//! Transactional callbacks prepare related rows before the one commit's final validation.
//! Routed actions commit with those rows and are delivered afterwards by maintenance.

mod delivery;
pub(crate) use delivery::{
    Deliveries, IN_SINK_CALL, erase, erase_gone, identity, key, load_body, prepare, validate,
};

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use crate::memory::{Budget, Reservation};
use crate::schema::Catalog;
use crate::sql::ast::EventDeclaration;
use crate::store::{AUDIT, ChangeSet, Key, LAST_AUDIT, Length, Read, TableId};
use crate::system::locked;
use crate::types::{Error, Result, TxId};

pub(crate) const CASCADE_DEPTH_CAP: u32 = 16;

/// Delivery counters for this open, with queue depth read from the committed store.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct SinkMetrics {
    pub delivered: u64,
    pub queued: u64,
    pub retried: u64,
    pub uncertain: u64,
    pub permanent_failures: u64,
    pub examined: u64,
    pub window_refusals: u64,
    pub window_refused: bool,
}
#[derive(Debug, Clone, serde::Serialize)]
pub struct EventTypeStatus {
    pub name: String,
    pub trigger: String,
    pub table: String,
}
#[derive(Debug, Clone, serde::Serialize)]
pub struct SinkStatus {
    pub name: String,
    pub sink_type: String,
    pub callback_registered: bool,
    pub metrics: SinkMetrics,
}
#[derive(Debug, Clone, serde::Serialize)]
pub struct RouteStatus {
    pub name: String,
    pub event_type: String,
    pub sink: String,
}
#[derive(Debug, Clone, serde::Serialize)]
pub struct ScheduleStatus {
    pub name: String,
    pub callback_registered: bool,
    /// The last failure of a tick that ended no cycle: a callback not registered.
    pub last_error: Option<String>,
}
/// One committed snapshot of declared events, sinks and routes.
#[derive(Debug, Clone, serde::Serialize)]
pub struct EventBusStatus {
    /// False for readers: handler registration and live delivery counters are unavailable.
    pub live: bool,
    pub event_types: Vec<EventTypeStatus>,
    pub sinks: Vec<SinkStatus>,
    pub routes: Vec<RouteStatus>,
    pub schedules: Vec<ScheduleStatus>,
}

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
    let mut length = crate::store::Length(0);
    let _ = std::io::Write::write_fmt(&mut length, format_args!("{error}"));
    held.grow(length.0 as u64)?;
    let mut reason = String::with_capacity(length.0);
    let _ = std::fmt::Write::write_fmt(&mut reason, format_args!("{error}"));
    *status = TriggerAuditStatus::RolledBack { reason };
    Ok(())
}

/// The stamp of a stored firing the retention judged expired: no later period returns it.
pub(crate) const EXPIRED: i64 = i64::MIN;

/// Stores each firing under the next number of one sequence, stamped `now`.
/// Each is the entry's fields in order, the firing past the depth cap `DepthExceeded`.
pub(crate) fn write_audit(
    changes: &mut ChangeSet,
    (firings, _): &AuditQueue,
    now: i64,
) -> Result<()> {
    let (entries, names, tx, status) = firings;
    if entries.is_empty() {
        return Ok(());
    }
    let head = changes.base().moment.number(LAST_AUDIT)?;
    for (number, &(name, depth, rows)) in (head..).zip(entries) {
        let deep = TriggerAuditStatus::DepthExceeded;
        let status = if depth > CASCADE_DEPTH_CAP {
            &deep
        } else {
            status
        };
        let entry = (&names[name as usize], tx, depth, u64::from(rows), status);
        let key = [&[AUDIT][..], &number.to_be_bytes()].concat();
        let mut length = Length(8);
        serde_json::to_writer(&mut length, &entry).expect("an audit entry encodes");
        // Charged before the copy is made.
        changes.put_audit_record(
            key,
            Some((length.0, |value: &mut Vec<u8>| {
                value.extend(now.to_be_bytes());
                serde_json::to_writer(value, &entry).expect("an audit entry encodes");
            })),
        )?;
    }
    changes.put_last_audit(head + entries.len() as u64)
}

/// Stages a stored firing as read from `key`, judged expired: removed, or kept `kept`.
pub(crate) fn expire_audit(changes: &mut ChangeSet, key: &[u8], kept: Option<&[u8]>) -> Result<()> {
    let value = kept.map(|kept| {
        (kept.len(), |value: &mut Vec<u8>| {
            value.extend(EXPIRED.to_be_bytes());
            value.extend(&kept[8..]);
        })
    });
    changes.put_audit_record(key.into(), value)
}

/// Whether a firing stamped `stamp` is inside the retention `period` at `now`, in milliseconds.
pub(crate) fn retained(stamp: i64, now: i64, period: u64) -> bool {
    stamp != EXPIRED && i128::from(now) - i128::from(stamp) <= i128::from(period)
}
