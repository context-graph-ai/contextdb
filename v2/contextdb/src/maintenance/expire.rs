//! Stored trigger firings judged against the audit retention period, a bounded batch a cycle.
use crate::database::{Open, locked};
use crate::events::{EXPIRED, retained};
use crate::store::{AUDIT, LAST_AUDIT, Read};
use crate::system;
use crate::types::Result;
use std::ops::Bound::{Excluded, Included};
use std::sync::atomic::Ordering::Relaxed;

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
