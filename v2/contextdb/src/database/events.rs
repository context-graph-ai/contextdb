//! The carried host callback API and its private handle into the committing transaction.

use super::{Database, QueryResult, admin, locked};
use crate::events::{Bound, CASCADE_DEPTH_CAP};
use crate::query::Reader;
use crate::sql::Effect;
use crate::store::{AUDIT, LAST_AUDIT, Read};
use crate::types::{Error, Principal, Result, SinkError, SinkEvent, TriggerContext, Value};
use std::collections::HashMap;
use std::ops::Bound::{Excluded, Included};
use std::sync::{Arc, Mutex};

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

    /// The newest 1,024 firings, oldest first. A refused write's firings are `RolledBack`, listed
    /// from memory until a later write stores them (a handle closed first loses them), and its
    /// `firing_tx` is a number no commit carries. Requires an unnarrowed handle.
    pub fn trigger_audit_log(&self) -> Result<crate::Owned<Vec<crate::TriggerAuditEntry>>> {
        admin(&self.reader, Some("trigger_audit_log"))?;
        let open = self.opened()?;
        let (view, _) = open.newest()?;
        let mut held = open.budget.reserve(0, "trigger audit answer")?;
        let head = view.moment.number(LAST_AUDIT)?;
        let mut answer = Vec::new();
        let span = (Included(&[AUDIT][..]), Excluded(&[AUDIT + 1][..]));
        view.deliveries(span, &mut |_, value| {
            held.grow((size_of::<crate::TriggerAuditEntry>() + value.len()) as u64)?;
            let entry = serde_json::from_slice(value);
            answer.push(entry.map_err(|_| view.damaged("an invalid audit record"))?);
            Ok(true)
        })?;
        let kept = answer.len().max(1);
        answer.rotate_left(head as usize % kept);
        let audit = locked(&open.triggers.audit);
        held.grow((audit.1.len() * size_of::<crate::TriggerAuditEntry>()) as u64)?;
        answer.extend(audit.1.iter().cloned());
        Ok(crate::Owned::charged(answer, held))
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
