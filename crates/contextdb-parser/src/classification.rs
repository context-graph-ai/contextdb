use crate::Statement;

/// Whether a parsed SQL statement needs a writable store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatementEffect {
    Read,
    Write,
}

/// Classify a typed statement before any storage path is selected.
///
/// This match intentionally has no catch-all arm. Adding a parser statement
/// variant must classify it explicitly.
pub fn statement_effect(statement: &Statement) -> StatementEffect {
    match statement {
        // These statements inspect an already-open store without changing its
        // durable or session state.
        Statement::Select(_)
        | Statement::ShowMemoryLimit
        | Statement::ShowDiskLimit
        | Statement::ShowSyncConflictPolicy
        | Statement::ShowVectorIndexes => StatementEffect::Read,

        // This match intentionally stays exhaustive. A future parser variant
        // must make its store effect explicit at the same time it is added.
        Statement::CreateTable(_)
        | Statement::AlterTable(_)
        | Statement::DropTable(_)
        | Statement::CreateIndex(_)
        | Statement::DropIndex(_)
        | Statement::Insert(_)
        | Statement::Purge(_)
        | Statement::Delete(_)
        | Statement::Update(_)
        | Statement::Begin
        | Statement::Commit
        | Statement::Rollback
        | Statement::SetMemoryLimit(_)
        | Statement::SetDiskLimit(_)
        | Statement::CreateSchedule { .. }
        | Statement::DropSchedule { .. }
        | Statement::CreateTrigger { .. }
        | Statement::DropTrigger { .. }
        | Statement::CreateEventType { .. }
        | Statement::CreateSink { .. }
        | Statement::CreateRoute { .. }
        | Statement::DropRoute { .. } => StatementEffect::Write,
    }
}
