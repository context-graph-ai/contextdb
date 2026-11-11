//! Turns a parsed statement into the plan that `schema`, `write` and `query` take.
//!
//! The planner is a set of fixed rules over the statement alone. It knows no table, so it
//! chooses no index: a SELECT's plan names the table to read and the filter to apply, and
//! `query` decides how to reach the rows.

use super::ast::{ColumnRef, Expr, Literal, Select, SelectColumn, SortDirection, Statement};
use super::not_supported;
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
        Statement::Insert(insert) => {
            insert.values.iter().flatten().try_for_each(rows_stand)?;
            PhysicalPlan::Insert(InsertPlan {
                table: insert.table.clone(),
                columns: insert.columns.clone(),
                values: insert.values.clone(),
            })
        }
        Statement::Delete(delete) => {
            delete.where_clause.iter().try_for_each(in_a_write)?;
            PhysicalPlan::Delete(DeletePlan {
                table: delete.table.clone(),
                where_clause: delete.where_clause.clone(),
            })
        }
        Statement::Update(update) => {
            let values = update.assignments.iter().map(|(_, value)| value);
            let mut written = values.chain(&update.where_clause);
            written.try_for_each(in_a_write)?;
            PhysicalPlan::Update(UpdatePlan {
                table: update.table.clone(),
                assignments: update.assignments.clone(),
                where_clause: update.where_clause.clone(),
            })
        }
        Statement::Select(select) => plan_select(select)?,
        Statement::SetMemoryLimit(limit) => PhysicalPlan::SetMemoryLimit(limit.clone()),
        Statement::ShowMemoryLimit => PhysicalPlan::ShowMemoryLimit,
        Statement::ShowVectorIndexes => PhysicalPlan::ShowVectorIndexes,
        Statement::ShowVectorPartitions {
            column,
            limit,
            offset,
        } => PhysicalPlan::ShowVectorPartitions {
            column: column.clone(),
            limit: *limit,
            offset: *offset,
        },
    })
}

/// A scan with its filter, sorted, then projected unless the statement is `SELECT *`, then cut
/// to the limit. A similarity search stands where the scan, the sort and the limit would.
fn plan_select(select: &Select) -> Result<PhysicalPlan> {
    let written = select.columns.iter().map(|column| &column.expr);
    let written = written.chain(&select.where_clause);
    let written = written.chain(select.order_by.iter().map(|item| &item.expr));
    let written: Vec<&Expr> = written.collect();
    written.iter().copied().try_for_each(rows_stand)?;
    let keys: Vec<SortKey> = select
        .order_by
        .iter()
        .map(|item| SortKey {
            expr: resolve_output_alias(&item.expr, &select.columns),
            direction: item.direction,
        })
        .collect();
    // A distance among several ORDER BY items is a search of a later version.
    let distance = |key: &SortKey| matches!(key.expr, Expr::Distance { .. });
    if keys.len() > 1 && keys.iter().any(distance) {
        return Err(not_supported("ORDER BY <=> with a second sort key"));
    }
    // A similarity search: the one ORDER BY item is a distance.
    let mut search = None;
    if let [key] = keys.as_slice()
        && let Expr::Distance { left, right } = &key.expr
    {
        search = Some(plan_search(select, left, right, key)?);
    }
    let searching = search.is_some();
    let mut current = match search {
        Some(search) => search,
        None => {
            if select.use_vector.is_some() {
                return Err(Error::UseVectorRequiresVectorOrder);
            }
            for expr in written {
                distances(expr, &mut |_, _| {
                    Err(not_supported(
                        "<=> anywhere but as the one ORDER BY item of a SELECT with LIMIT, and in \
                         that SELECT's select list,",
                    ))
                })?;
            }
            // A key `query` cannot work out for every row is refused here: sorting by it would
            // treat every row as equal and hand the rows back unsorted.
            let sortable = |key: &SortKey| match &key.expr {
                Expr::Column(_) => true,
                Expr::FunctionCall { name, .. } => SORTABLE_FUNCTIONS
                    .iter()
                    .any(|known| name.eq_ignore_ascii_case(known)),
                _ => false,
            };
            if !keys.iter().all(sortable) {
                return Err(Error::OrderByExpressionNotSupported);
            }
            let scan = PhysicalPlan::Scan {
                table: select.table.clone(),
                filter: select.where_clause.clone(),
            };
            if keys.is_empty() {
                scan
            } else {
                PhysicalPlan::Sort {
                    input: Box::new(scan),
                    keys,
                }
            }
        }
    };
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
    if let (Some(count), false) = (select.limit, searching) {
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

/// The search a SELECT asks for when its one ORDER BY item is `left <=> right`: the rows nearest
/// `right` in the column `left`. Refuses the forms of a search, and of a distance beside it,
/// that a later version reads.
fn plan_search(select: &Select, left: &Expr, right: &Expr, key: &SortKey) -> Result<PhysicalPlan> {
    let Some(k) = select.limit else {
        return Err(not_supported("a vector search without LIMIT"));
    };
    if key.direction == SortDirection::Desc {
        return Err(not_supported("ORDER BY <=> DESC"));
    }
    let Some(column) = searched_column(select, left)? else {
        // No search: the statement is the sort it is written as, which `query` refuses by the
        // column's name once it has found the table.
        let (table, filter, keys) = (select.table.clone(), None, vec![key.clone()]);
        let input = Box::new(PhysicalPlan::Scan { table, filter });
        return Ok(PhysicalPlan::Sort { input, keys });
    };
    match right {
        Expr::Parameter(_) | Expr::Literal(Literal::Text(_) | Literal::Vector(_)) => {}
        // `+` and its like are calls too, under names no statement writes.
        Expr::FunctionCall { name, .. } if !name.starts_with("__") => {
            let what = format!("<=> with the function {name} on its right");
            return Err(not_supported(&what));
        }
        _ => {
            return Err(not_supported(
                "<=> with anything but a vector literal, its quoted text or a $name on its right",
            ));
        }
    }
    if let Some(filter) = &select.where_clause {
        distances(filter, &mut |_, _| Err(not_supported("<=> in WHERE")))?;
    }
    for item in &select.columns {
        distances(&item.expr, &mut |other: &Expr, vector: &Expr| {
            let Some(other) = searched_column(select, other)? else {
                return Ok(());
            };
            if other == column && same_vector(vector, right) {
                return Ok(());
            }
            Err(not_supported(&format!(
                "<=> on the column {other} in the select list, where it is not the distance the \
                 search is ordered by,"
            )))
        })?;
    }
    Ok(PhysicalPlan::VectorSearch {
        table: select.table.clone(),
        column: column.to_owned(),
        query: right.clone(),
        k,
        filter: select.where_clause.clone(),
        mode: select.use_vector,
    })
}

/// The column on the left of a distance: a column of the statement's table, by its name alone or
/// with the table's before it. `None` under the name of any other table: `query` says that it
/// is not found, as it does anywhere else in the statement, after it has found the table.
fn searched_column<'a>(select: &Select, left: &'a Expr) -> Result<Option<&'a str>> {
    match left {
        Expr::Column(ColumnRef { table, column })
            if table.as_ref().is_none_or(|table| *table == select.table) =>
        {
            Ok(Some(column.as_str()))
        }
        Expr::Column(_) => Ok(None),
        _ => Err(not_supported(
            "<=> with anything but a column of the table on its left",
        )),
    }
}

/// Whether two right sides of `<=>` are written the same: the same `$name`, the same quoted text
/// or the same vector literal.
fn same_vector(one: &Expr, other: &Expr) -> bool {
    match (one, other) {
        (Expr::Parameter(one), Expr::Parameter(other)) => one == other,
        (Expr::Literal(Literal::Text(one)), Expr::Literal(Literal::Text(other))) => one == other,
        (Expr::Literal(Literal::Vector(one)), Expr::Literal(Literal::Vector(other))) => {
            one == other
        }
        _ => false,
    }
}

/// The expressions directly inside `expr`.
fn inside(expr: &Expr) -> Vec<&Expr> {
    match expr {
        Expr::Column(_) | Expr::Literal(_) | Expr::Parameter(_) => Vec::new(),
        Expr::BinaryOp { left, right, .. } | Expr::Distance { left, right } => {
            vec![&**left, &**right]
        }
        Expr::UnaryOp { operand: inner, .. } | Expr::IsNull { expr: inner, .. } => vec![&**inner],
        Expr::FunctionCall { args: items, .. } | Expr::Row(items) => items.iter().collect(),
        Expr::InList { expr, list, .. } => std::iter::once(&**expr).chain(list).collect(),
    }
}

/// Hands `each` the left and right side of every distance written in `expr`, at any depth.
fn distances(expr: &Expr, each: &mut dyn FnMut(&Expr, &Expr) -> Result<()>) -> Result<()> {
    if let Expr::Distance { left, right } = expr {
        each(left, right)?;
    }
    inside(expr)
        .into_iter()
        .try_for_each(|inner| distances(inner, each))
}

/// What an expression of an UPDATE or DELETE must pass: [`rows_stand`], and no distance, which
/// only a search gives a row. Refused here, the statement is refused whatever its table holds.
fn in_a_write(expr: &Expr) -> Result<()> {
    rows_stand(expr)?;
    distances(expr, &mut |_, _| {
        Err(not_supported("<=> in an UPDATE or DELETE"))
    })
}

/// Refuses a row value that stands anywhere but on either side of `IN`, and one beside `IN`
/// that has another number of values than its partner.
fn rows_stand(expr: &Expr) -> Result<()> {
    let (left, list) = match expr {
        Expr::InList { expr, list, .. } => (&**expr, list),
        Expr::Row(_) => return Err(not_supported("a row value anywhere but beside IN")),
        _ => return inside(expr).into_iter().try_for_each(rows_stand),
    };
    let width = |side: &Expr| match side {
        Expr::Row(values) => values.len(),
        _ => 1,
    };
    for side in std::iter::once(left).chain(list) {
        if width(side) != width(left) {
            return Err(Error::PlanError(
                "a row value beside IN must have as many values as its partner".to_owned(),
            ));
        }
        // What a side holds is checked as any expression is: a row value holds no row value.
        match side {
            Expr::Row(values) => values.iter().try_for_each(rows_stand)?,
            _ => rows_stand(side)?,
        }
    }
    Ok(())
}
