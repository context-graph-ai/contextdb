use crate::database::Database;
use contextdb_core::{ColumnType, Error, Result, Value};
use contextdb_parser::ast::{Expr, Literal, UnaryOp};
use contextdb_planner::PhysicalPlan;
use std::collections::HashMap;

pub fn validate_dml(
    plan: &PhysicalPlan,
    db: &Database,
    params: &HashMap<String, Value>,
) -> Result<()> {
    match plan {
        PhysicalPlan::Update(p) => {
            // Error-ordering precedence on UPDATE (per I13):
            //   1. Table does not exist
            //   2. Column in SET list does not exist on the table
            //   3. Table-level ImmutableTable rejection
            //   4. Column-level ImmutableColumn rejection (first flagged column in SET-list order)
            //   5. STATE MACHINE / NOT NULL / coerce (handled later in executor)
            let metas = db.relational_store().table_meta.read();
            let table_meta = metas
                .get(&p.table)
                .ok_or_else(|| Error::TableNotFound(p.table.clone()))?;

            // (2) Unknown-column check on every SET target.
            for (col_name, _) in &p.assignments {
                if !table_meta.columns.iter().any(|c| c.name == *col_name) {
                    return Err(Error::Other(format!(
                        "column '{}' does not exist on table '{}'",
                        col_name, p.table
                    )));
                }
            }

            // (3) Table-level IMMUTABLE short-circuits column-level checks.
            if table_meta.immutable {
                return Err(Error::ImmutableTable(p.table.clone()));
            }

            // (4) Column-level IMMUTABLE: first flagged column in SET-list order wins.
            for (col_name, _) in &p.assignments {
                if let Some(col) = table_meta.columns.iter().find(|c| c.name == *col_name)
                    && col.immutable
                {
                    return Err(Error::ImmutableColumn {
                        table: p.table.clone(),
                        column: col_name.clone(),
                    });
                }
            }

            Ok(())
        }
        PhysicalPlan::Delete(p) => {
            let metas = db.relational_store().table_meta.read();
            if metas.get(&p.table).is_some_and(|meta| meta.immutable) {
                Err(Error::ImmutableTable(p.table.clone()))
            } else {
                Ok(())
            }
        }
        PhysicalPlan::Insert(p) => {
            let metas = db.relational_store().table_meta.read();
            let table_meta = metas
                .get(&p.table)
                .ok_or_else(|| Error::TableNotFound(p.table.clone()))?;

            // When no column list is provided, infer from table metadata.
            let columns: Vec<String> = if p.columns.is_empty() {
                table_meta.columns.iter().map(|c| c.name.clone()).collect()
            } else {
                p.columns.clone()
            };

            // Validate that all INSERT columns exist in the table schema
            for col_name in &columns {
                if !table_meta.columns.iter().any(|c| c.name == *col_name) {
                    return Err(Error::Other(format!(
                        "column '{}' does not exist in table '{}'",
                        col_name, p.table
                    )));
                }
            }

            for row in &p.values {
                for column in table_meta.columns.iter().filter(|column| !column.nullable) {
                    if matches!(column.column_type, ColumnType::TxId) {
                        continue;
                    }
                    if let Some(index) = columns.iter().position(|name| name == &column.name) {
                        let value = row
                            .get(index)
                            .ok_or_else(|| {
                                Error::PlanError("column/value count mismatch".to_string())
                            })
                            .and_then(|expr| {
                                resolve_required_expr(expr, params, &p.table, &column.name)
                            })?;
                        if value == Value::Null {
                            return Err(Error::Other(format!(
                                "NOT NULL constraint violated: {}.{}",
                                p.table, column.name
                            )));
                        }
                    } else if column.default.is_none() {
                        return Err(Error::Other(format!(
                            "NOT NULL constraint violated: {}.{}",
                            p.table, column.name
                        )));
                    }
                }
            }

            Ok(())
        }
        _ => Ok(()),
    }
}

/// Resolve the subset of INSERT expressions needed to check a required value
/// before staging the row. Expressions that need a row are deliberately
/// rejected here with the destination column, rather than being reported as
/// an unrelated unsupported-expression implementation detail.
fn resolve_required_expr(
    expr: &Expr,
    params: &HashMap<String, Value>,
    table: &str,
    column: &str,
) -> Result<Value> {
    match expr {
        Expr::Literal(l) => Ok(match l {
            Literal::Null => Value::Null,
            Literal::Bool(v) => Value::Bool(*v),
            Literal::Integer(v) => Value::Int64(*v),
            Literal::Real(v) => Value::Float64(*v),
            Literal::Text(v) => {
                if let Ok(id) = uuid::Uuid::parse_str(v) {
                    Value::Uuid(id)
                } else {
                    Value::Text(v.clone())
                }
            }
            Literal::Vector(v) => Value::Vector(v.clone()),
        }),
        Expr::Parameter(parameter) => params
            .get(parameter)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("missing parameter: {parameter}"))),
        Expr::UnaryOp {
            op: UnaryOp::Neg,
            operand,
        } => match resolve_required_expr(operand, params, table, column)? {
            Value::Int64(value) => value.checked_neg().map(Value::Int64).ok_or_else(|| {
                Error::Other(format!(
                    "cannot resolve required INSERT value for {table}.{column}: integer literal is out of range"
                ))
            }),
            Value::Float64(value) => Ok(Value::Float64(-value)),
            _ => Err(Error::Other(format!(
                "cannot resolve required INSERT value for {table}.{column}: unary - requires an INTEGER or REAL literal or parameter"
            ))),
        },
        _ => Err(Error::Other(format!(
            "cannot resolve required INSERT value for {table}.{column}: this expression requires row context; use a literal or bound parameter"
        ))),
    }
}
