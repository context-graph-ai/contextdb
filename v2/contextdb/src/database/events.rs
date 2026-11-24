//! The carried host callback API and its private handle into the committing transaction.

use super::{Database, QueryResult, admin, locked};
use crate::events::{Bound, CASCADE_DEPTH_CAP, retained};
use crate::query::Reader;
use crate::sql::Effect;
use crate::store::{AUDIT, LAST_AUDIT, Read};
use crate::types::{Error, Principal, Result, SinkError, SinkEvent, TriggerContext, Value};
use std::collections::HashMap;
use std::ops::Bound::{Excluded, Included};
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, Mutex};
use std::time::Duration;

impl Database {
    /// Registers one declared trigger's host callback. It prepares rows in the firing
    /// transaction, before final validation; it must not perform external notifications.
    /// The supplied handle keeps the submitting reader's permissions and works only during
    /// callback dispatch, on that thread. Its SQL statements use the same transaction. A write
    /// on another thread waits for it, and past the open's `callback_wait` gets `CallbackActive`.
    pub fn register_transactional_trigger_callback<F>(&self, name: &str, callback: F) -> Result<()>
    where
        F: Fn(&Database, &TriggerContext) -> Result<()> + Send + Sync + 'static,
    {
        admin(&self.reader, Some("trigger callbacks"))?;
        let open = self.opened()?;
        let _held = open.turn()?;
        let (_, catalog) = open.newest()?;
        open.triggers.register(&catalog, name, callback)
    }

    /// Registers or replaces a declared sink's external handler. Every attempt uses the
    /// registering handle's visibility; None inherits its principal. Delivery runs after commit.
    pub fn register_sink<F>(
        &self,
        name: &str,
        principal: Option<Principal>,
        deliver: F,
    ) -> Result<()>
    where
        F: Fn(&SinkEvent) -> std::result::Result<(), SinkError> + Send + Sync + 'static,
    {
        let open = self.opened()?;
        open.deliveries
            .register(&open, name, &self.reader, principal, deliver)
    }

    /// Refuses initialization if any declared trigger lacks its host callback.
    pub fn complete_initialization(&self) -> Result<()> {
        admin(&self.reader, Some("trigger callbacks"))?;
        let open = self.opened()?;
        let _held = open.turn()?;
        let (_, catalog) = open.newest()?;
        open.triggers.complete_initialization(&catalog)
    }

    /// Lists registered callback names in order; refuses a closed or callback handle.
    pub fn registered_trigger_callbacks(&self) -> Result<crate::Owned<Vec<String>>> {
        self.opened()?.triggers.registered(|_| true)
    }

    /// The newest 1,024 firings, oldest first, whatever their age. A refused write's firings
    /// are stored `RolledBack` before its refusal returns, under a `firing_tx` no commit
    /// carries. Requires an unnarrowed handle.
    pub fn trigger_audit_log(&self) -> Result<crate::Owned<Vec<crate::TriggerAuditEntry>>> {
        admin(&self.reader, Some("trigger_audit_log"))?;
        let open = self.opened()?;
        let (view, _) = open.newest()?;
        let head = view.moment.number(LAST_AUDIT)?;
        audit(&open, &view, head.saturating_sub(1024), &|_, _| true)
    }

    /// The stored firings `keep` admits, oldest first, each inside the retention period by its
    /// stamp. One the retention judged expired never returns, whatever the period or the clock
    /// later. Requires an unnarrowed handle.
    pub fn trigger_audit_history(
        &self,
        keep: impl Fn(&crate::TriggerAuditEntry) -> bool,
    ) -> Result<crate::Owned<Vec<crate::TriggerAuditEntry>>> {
        admin(&self.reader, Some("trigger_audit_history"))?;
        let open = self.opened()?;
        let (view, _) = open.newest()?;
        let now = crate::system::now_millis(&*open.clock);
        let period = open.audit_retention.load(Relaxed);
        audit(&open, &view, 0, &|stamp, e| {
            retained(stamp, now, period) && keep(e)
        })
    }

    /// Sets how long a firing stays in the history: seven days on every open, never stored.
    /// Requires an unnarrowed handle.
    pub fn set_trigger_audit_retention(&self, period: Duration) -> Result<()> {
        admin(&self.reader, Some("set_trigger_audit_retention"))?;
        let millis = u64::try_from(period.as_millis()).unwrap_or(u64::MAX);
        self.opened()?.audit_retention.store(millis, Relaxed);
        Ok(())
    }

    /// How long a firing stays in the history. Requires an unnarrowed handle.
    pub fn trigger_audit_retention(&self) -> Result<Duration> {
        admin(&self.reader, Some("trigger_audit_retention"))?;
        let millis = self.opened()?.audit_retention.load(Relaxed);
        Ok(Duration::from_millis(millis))
    }

    /// The carried maximum callback depth. Initial firings start at one.
    ///
    /// # Panics
    ///
    /// Panics on a closed handle, a callback handle or the firing thread during dispatch.
    pub fn trigger_cascade_depth_cap(&self) -> u32 {
        self.opened().unwrap_or_else(|error| panic!("{error}"));
        CASCADE_DEPTH_CAP
    }

    pub(crate) fn bound(bound: Arc<Bound>, reader: Reader) -> Self {
        Self {
            open: Mutex::new(None),
            reader,
            bound: Some(bound),
        }
    }

    pub(super) fn unbound(&self) -> Result<()> {
        if self.bound.is_some()
            || locked(&self.open).as_ref().is_some_and(|writer| {
                writer.open.triggers.firing_here() || super::observers::active(&writer.open)
            })
        {
            return Err(Error::CallbackReentry {
                operation: "a database control operation".into(),
            });
        }
        Ok(())
    }

    pub(super) fn execute_bound(
        &self,
        bound: &Bound,
        sql: &str,
        params: &HashMap<String, Value>,
    ) -> Result<QueryResult> {
        bound.with_staged(|open, staged| {
            let (plan, parsing, form) = open.plans.planned(sql, &open.budget)?;
            let refused = plan
                .admin_only()
                .filter(|n| n.starts_with("SET ") || *n == "DROP TRIGGER");
            if let Some(operation) = refused {
                return Err(Error::CallbackReentry {
                    operation: operation.into(),
                });
            }
            open.observed(sql, || {
                if matches!(plan.effect(), Effect::Declares) {
                    open.host(|plugin| plugin.on_ddl(sql))?;
                }
                let mark = staged.changes.mark();
                let generation = open.cancelled.load(std::sync::atomic::Ordering::Relaxed);
                let result = open.run(
                    plan,
                    (sql, parsing, form),
                    params,
                    staged,
                    &self.reader,
                    generation,
                );
                if result.is_err() {
                    staged.changes.rewind(mark);
                    staged.partitions.forget();
                }
                result
            })
        })
    }
}

/// The stored firings from number `from` that `keep` admits by stamp and entry, charged.
fn audit(
    open: &super::Open,
    view: &crate::store::View,
    from: u64,
    keep: &dyn Fn(i64, &crate::TriggerAuditEntry) -> bool,
) -> Result<crate::Owned<Vec<crate::TriggerAuditEntry>>> {
    let mut held = open.budget.reserve(0, "trigger audit answer")?;
    let mut answer = Vec::new();
    let from = [&[AUDIT][..], &from.to_be_bytes()].concat();
    let span = (Included(&from[..]), Excluded(&[AUDIT + 1][..]));
    view.deliveries(span, &mut |_, value| {
        let bad = || view.damaged("an invalid audit record");
        held.grow(value.len() as u64)?;
        let stamp = value.first_chunk().map(|s| i64::from_be_bytes(*s));
        let entry = serde_json::from_slice(&value[8.min(value.len())..]).ok();
        let (stamp, entry) = stamp.zip(entry).ok_or_else(bad)?;
        match keep(stamp, &entry) {
            true => held
                .room(&mut answer, usize::MAX)
                .map(|()| answer.push(entry))?,
            false => held.shrink(value.len() as u64),
        }
        Ok(true)
    })?;
    Ok(crate::Owned::charged(answer, held))
}
