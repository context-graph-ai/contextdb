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
        | Statement::ShowMaintenancePollInterval
        | Statement::ShowSyncConflictPolicy
        | Statement::ShowVectorIndexes
        | Statement::ShowTenantTablePolicy { .. }
        | Statement::ShowSyncBindings
        | Statement::ShowDeliveryOutcomes(_)
        | Statement::ShowVectorPartitions { .. } => StatementEffect::Read,

        // This match intentionally stays exhaustive. A future parser variant
        // must make its store effect explicit at the same time it is added.
        Statement::CreateTable(_)
        | Statement::AlterTable(_)
        | Statement::DropTable(_)
        | Statement::CreateIndex(_)
        | Statement::DropIndex(_)
        | Statement::Insert(_)
        | Statement::Purge(_)
        | Statement::Discard(_)
        | Statement::DeclareTenantTablePolicy(_)
        | Statement::Delete(_)
        | Statement::Update(_)
        | Statement::Begin
        | Statement::Commit
        | Statement::Rollback
        | Statement::SetMemoryLimit(_)
        | Statement::SetDiskLimit(_)
        | Statement::SetMaintenancePollInterval(_)
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

/// Whether a SELECT contains a nearest-neighbour ORDER BY, including nested queries.
pub fn select_contains_vector_similarity(select: &crate::SelectStatement) -> bool {
    select_body_contains_vector_similarity(&select.body)
        || select.ctes.iter().any(|cte| match cte {
            crate::Cte::SqlCte { query, .. } => select_body_contains_vector_similarity(query),
            crate::Cte::MatchCte { .. } => false,
        })
}

fn select_body_contains_vector_similarity(body: &crate::SelectBody) -> bool {
    body.order_by
        .iter()
        .any(|item| matches!(item.direction, crate::SortDirection::CosineDistance))
        || body
            .columns
            .iter()
            .any(|column| expr_contains_vector_subquery(&column.expr))
        || body
            .where_clause
            .as_ref()
            .is_some_and(expr_contains_vector_subquery)
        || body
            .joins
            .iter()
            .any(|join| expr_contains_vector_subquery(&join.on))
        || body
            .order_by
            .iter()
            .any(|item| expr_contains_vector_subquery(&item.expr))
}

fn expr_contains_vector_subquery(expr: &crate::Expr) -> bool {
    use crate::Expr;

    match expr {
        Expr::InSubquery { expr, subquery, .. } => {
            expr_contains_vector_subquery(expr) || select_body_contains_vector_similarity(subquery)
        }
        Expr::BinaryOp { left, right, .. } | Expr::CosineDistance { left, right } => {
            expr_contains_vector_subquery(left) || expr_contains_vector_subquery(right)
        }
        Expr::UnaryOp { operand, .. } | Expr::IsNull { expr: operand, .. } => {
            expr_contains_vector_subquery(operand)
        }
        Expr::FunctionCall { args, .. } => args.iter().any(expr_contains_vector_subquery),
        Expr::InList { expr, list, .. } => {
            expr_contains_vector_subquery(expr) || list.iter().any(expr_contains_vector_subquery)
        }
        Expr::Like { expr, pattern, .. } => {
            expr_contains_vector_subquery(expr) || expr_contains_vector_subquery(pattern)
        }
        Expr::Column(_) | Expr::Literal(_) | Expr::Parameter(_) | Expr::RowVectorSource { .. } => {
            false
        }
    }
}
