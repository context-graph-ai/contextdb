//! Turns a parsed statement into the plan that `schema`, `write` and `query` take.
//!
//! The planner is a set of fixed rules over the statement alone. It knows no table, so it
//! chooses no index: a SELECT's plan names the table to read and the filter to apply, and
//! `query` decides how to reach the rows.

use super::ast::{ColumnRef, Expr, Select, SelectColumn, Statement};
use super::plan::*;
use crate::types::{Error, Result};

/// The functions an ORDER BY key may call: the ones `query` works out for every row. The four
/// `__` names are what the parser makes of `+`, `-`, `*` and `/`.
const SORTABLE_FUNCTIONS: [&str; 6] = ["__add", "__sub", "__mul", "__div", "coalesce", "now"];

pub(super) fn plan(statement: &Statement) -> Result<PhysicalPlan> {
    Ok(match statement {
        Statement::CreateTable(table) => PhysicalPlan::CreateTable(CreateTablePlan {
            name: table.name.clone(),
            columns: table.columns.clone(),
            unique_constraints: table.unique_constraints.clone(),
            if_not_exists: table.if_not_exists,
        }),
        Statement::CreateIndex(index) => PhysicalPlan::CreateIndex(CreateIndexPlan {
            name: index.name.clone(),
            table: index.table.clone(),
            columns: index.columns.clone(),
        }),
        Statement::Insert(insert) => PhysicalPlan::Insert(InsertPlan {
            table: insert.table.clone(),
            columns: insert.columns.clone(),
            values: insert.values.clone(),
        }),
        Statement::Delete(delete) => PhysicalPlan::Delete(DeletePlan {
            table: delete.table.clone(),
            where_clause: delete.where_clause.clone(),
        }),
        Statement::Update(update) => PhysicalPlan::Update(UpdatePlan {
            table: update.table.clone(),
            assignments: update.assignments.clone(),
            where_clause: update.where_clause.clone(),
        }),
        Statement::Select(select) => plan_select(select)?,
        Statement::SetMemoryLimit(limit) => PhysicalPlan::SetMemoryLimit(limit.clone()),
        Statement::ShowMemoryLimit => PhysicalPlan::ShowMemoryLimit,
    })
}

/// A scan with its filter, sorted, then projected unless the statement is `SELECT *`, then cut
/// to the limit.
fn plan_select(select: &Select) -> Result<PhysicalPlan> {
    let mut current = PhysicalPlan::Scan {
        table: select.table.clone(),
        filter: select.where_clause.clone(),
    };
    if !select.order_by.is_empty() {
        let mut keys = Vec::with_capacity(select.order_by.len());
        for item in &select.order_by {
            let expr = resolve_output_alias(&item.expr, &select.columns);
            // A key `query` cannot work out for every row is refused here: sorting by it would
            // treat every row as equal and hand the rows back unsorted.
            let sortable = match &expr {
                Expr::Column(_) => true,
                Expr::FunctionCall { name, .. } => SORTABLE_FUNCTIONS
                    .iter()
                    .any(|known| name.eq_ignore_ascii_case(known)),
                _ => false,
            };
            if !sortable {
                return Err(Error::OrderByExpressionNotSupported);
            }
            keys.push(SortKey {
                expr,
                direction: item.direction,
            });
        }
        current = PhysicalPlan::Sort {
            input: Box::new(current),
            keys,
        };
    }
    let star = matches!(
        select.columns.as_slice(),
        [SelectColumn { expr: Expr::Column(ColumnRef { table: None, column }), alias: None }]
            if column == "*"
    );
    if !star {
        current = PhysicalPlan::Project {
            input: Box::new(current),
            columns: select
                .columns
                .iter()
                .map(|column| ProjectColumn {
                    expr: column.expr.clone(),
                    alias: column.alias.clone(),
                })
                .collect(),
        };
    }
    if let Some(count) = select.limit {
        current = PhysicalPlan::Limit {
            input: Box::new(current),
            count,
        };
    }
    Ok(current)
}

/// Reads a bare ORDER BY name as a select-list alias first, as standard SQL does. The sort runs
/// below the projection, where an alias does not exist yet, so the aliased expression is sorted
/// by in its place. A qualified name is a table's column and is left alone.
fn resolve_output_alias(expr: &Expr, columns: &[SelectColumn]) -> Expr {
    let aliased = match expr {
        Expr::Column(ColumnRef {
            table: None,
            column,
        }) => columns
            .iter()
            .find(|select| select.alias.as_deref() == Some(column.as_str())),
        _ => None,
    };
    aliased.map_or_else(|| expr.clone(), |select| select.expr.clone())
}
