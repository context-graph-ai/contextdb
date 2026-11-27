//! The planned statement: what `schema`, `write` and `query` each take.

/// A declaration or a write is planned as it was written: `schema` and `write` judge every
/// clause of it.
pub use super::ast::{
    CreateIndex as CreateIndexPlan, CreateTable as CreateTablePlan, Insert as InsertPlan,
};
/// A SELECT, a select item, a sort key and a table or graph pattern of FROM are planned as
/// they were written too.
pub use super::ast::{
    EdgeDirection, EdgeStep, FromItem, GraphTable, NodePattern, OrderByItem as SortKey,
    Select as SelectPlan, SelectColumn as ProjectColumn,
};
use std::sync::{Arc, Mutex};

use super::Plan;
use super::ast::{Expr, Statement};
use crate::memory::{Budget, Reservation};
use crate::system::locked;
use crate::types::Result;

/// The reports `SHOW` names: each one's statement, whether only an unnarrowed handle may run
/// it, and whether it takes options. `query` fills each by its place here.
pub(crate) const REPORTS: [(&str, bool, bool); 5] = [
    ("SHOW VECTOR_INDEXES", true, false),
    ("SHOW VECTOR_PARTITIONS", true, true),
    ("SHOW CONTENT_SIZE", true, false),
    ("SHOW STATEMENT_STATISTICS", true, true),
    ("SHOW READ_CACHE_SHARE", false, false),
];

/// What a planned statement does to the store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Effect {
    /// Declares, changes or drops a table or an index.
    Declares,
    /// Adds, changes or removes rows.
    WritesRows,
    /// Changes one declared setting in its own commit.
    Sets(usize, u64),
    /// Changes nothing that is stored.
    Reads,
}

impl Statement {
    /// Which of these this statement is. A new kind of plan is given its answer here, in the
    /// one place every part asks.
    pub(crate) fn effect(&self) -> Effect {
        match self {
            Statement::CreateTable(_)
            | Statement::CreateIndex(_)
            | Statement::AlterTable(_)
            | Statement::DropTable(_)
            | Statement::DropIndex(_)
            | Statement::DeclareEvent(_)
            | Statement::DropEvent { .. } => Effect::Declares,
            Statement::Insert(_)
            | Statement::Delete(_)
            | Statement::Update(_)
            | Statement::Purge(_) => Effect::WritesRows,
            Statement::SetSetting(at, n) => Effect::Sets(*at, *n),
            Statement::Select(_)
            | Statement::ShowSetting(_)
            | Statement::Explain(_)
            | Statement::ShowReport(_)
            | Statement::ResetStatements => Effect::Reads,
        }
    }

    /// The statement's name when only a handle that declared no narrowing may run it: what it
    /// declares, sets or reads holds for every Context, label and principal at once.
    pub(crate) fn admin_only(&self) -> Option<&'static str> {
        Some(match self {
            Statement::DeclareEvent(event) => event.identity().0.words().1,
            Statement::DropEvent { kind, .. } => kind.words().2,
            Statement::CreateTable(_) => "CREATE TABLE",
            Statement::CreateIndex(_) => "CREATE INDEX",
            Statement::AlterTable(_) => "ALTER TABLE",
            Statement::DropTable(_) => "DROP TABLE",
            Statement::DropIndex(_) => "DROP INDEX",
            Statement::SetSetting(at, _) => crate::settings::SETTINGS[*at].0,
            Statement::ResetStatements => "RESET STATEMENT_STATISTICS",
            Statement::ShowReport(show) => {
                let (name, admin, _) = REPORTS[show.report];
                return admin.then_some(name);
            }
            _ => return None,
        })
    }

    /// The table an INSERT, UPDATE or DELETE names and every expression it holds: the VALUES,
    /// then the assignments of `ON CONFLICT`; the SET values, then WHERE; WHERE. `None` for
    /// any other statement.
    pub(crate) fn written(&self) -> Option<(&String, Vec<&Expr>)> {
        Some(match self {
            Statement::Insert(insert) => {
                let conflict = insert.on_conflict.iter().flat_map(|on| &on.assignments);
                let values = insert.values.iter().flatten();
                let written = values.chain(conflict.map(|(_, value)| value));
                (&insert.table, written.collect())
            }
            Statement::Update(update) => {
                let set = update.assignments.iter().map(|(_, value)| value);
                (&update.table, set.chain(&update.where_clause).collect())
            }
            Statement::Delete(delete) => (&delete.table, delete.where_clause.iter().collect()),
            _ => return None,
        })
    }
}

/// Exact-text, FIFO retention of unbound plans within the room reserved at open.
pub(crate) const HELD_PER_TEXT_BYTE: u64 = 128;
type Kept = (Vec<(Box<str>, Plan, Option<Arc<str>>)>, u64);
/// A statement planned, its parse's charge, and its form when the record of statements keeps it.
pub(crate) type Planned = (Plan, Reservation, Option<Arc<str>>);
pub(crate) struct KeptPlans(pub(crate) Mutex<Kept>, Reservation);
impl KeptPlans {
    pub(crate) fn new(budget: &Budget, limit: Option<u64>) -> Result<Self> {
        let cap = crate::memory::account(limit, 0, 0)[2];
        let held = budget.reserve(cap, "statement plan cache needed to open the store")?;
        Ok(Self(Mutex::new((Vec::with_capacity(1024), 0)), held))
    }

    pub(crate) fn planned(&self, text: &str, budget: &Budget) -> Result<Planned> {
        let mut guard = locked(&self.0);
        let (kept, used) = &mut *guard;
        if let Some((_, plan, form)) = kept.iter().find(|(key, ..)| &**key == text) {
            let held = super::charge(text, budget)?;
            return Ok((plan.clone(), held, form.clone()));
        }
        let (statement, held, form) = super::parse(text, budget)?;
        let plan = super::plan(statement)?;
        let kind = matches!(plan, Plan::Select(_) | Plan::Insert(_) | Plan::Update(_));
        let form = (kind || matches!(plan, Plan::Delete(_))).then(|| Arc::from(form));
        let bytes = text.len() as u64 * HELD_PER_TEXT_BYTE;
        let slots = 1024 * size_of::<(Box<str>, Plan, Option<Arc<str>>)>() as u64;
        if !(plan.effect() == Effect::WritesRows || matches!(plan, Plan::Select(_)))
            || bytes + slots > self.1.bytes()
        {
            return Ok((plan, held, form));
        }
        while kept.len() == 1024 || *used + bytes + slots > self.1.bytes() {
            *used -= kept.remove(0).0.len() as u64 * HELD_PER_TEXT_BYTE;
        }
        *used += bytes;
        kept.push((text.into(), plan.clone(), form.clone()));
        Ok((plan, held, form))
    }
}
