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
use super::ast::{Expr, Statement};

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
                let (name, _, admin, _, _) = crate::query::REPORTS[show.report];
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
