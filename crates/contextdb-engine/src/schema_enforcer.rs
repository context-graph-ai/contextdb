use crate::database::Database;
use contextdb_core::{Error, Result, Value};
use contextdb_parser::ast::{Expr, Literal};
use contextdb_planner::PhysicalPlan;
use std::collections::{HashMap, HashSet};

pub fn validate_dml(
    plan: &PhysicalPlan,
    db: &Database,
    params: &HashMap<String, Value>,
) -> Result<()> {
    match plan {
        PhysicalPlan::Update(p) if db.relational_store().is_immutable(&p.table) => {
            Err(Error::ImmutableTable(p.table.clone()))
        }
        PhysicalPlan::Delete(p) if db.relational_store().is_immutable(&p.table) => {
            Err(Error::ImmutableTable(p.table.clone()))
        }
        PhysicalPlan::Insert(p) => {
            let table_meta = db
                .table_meta(&p.table)
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

            if p.on_conflict.is_none()
                && let Some(id_idx) = columns.iter().position(|c| c == "id")
            {
                let mut pending_ids = HashSet::new();
                for row in &p.values {
                    let id_value = row
                        .get(id_idx)
                        .map(|e| resolve_expr(e, params))
                        .transpose()?;
                    if let Some(v) = id_value {
                        if !pending_ids.insert(cache_key_for_value(&v)) {
                            return Err(Error::UniqueViolation {
                                table: p.table.clone(),
                                column: "id".to_string(),
                            });
                        }
                        let lookup = db.point_lookup(&p.table, "id", &v, db.snapshot())?;
                        if lookup.is_some() {
                            return Err(Error::UniqueViolation {
                                table: p.table.clone(),
                                column: "id".to_string(),
                            });
                        }
                    }
                }
            }

            for row in &p.values {
                for column in table_meta
                    .columns
                    .iter()
                    .filter(|column| !column.nullable && !column.primary_key)
                {
                    if let Some(index) = columns.iter().position(|name| name == &column.name) {
                        let value = row
                            .get(index)
                            .ok_or_else(|| {
                                Error::PlanError("column/value count mismatch".to_string())
                            })
                            .and_then(|expr| resolve_expr(expr, params))?;
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

            let unique_columns: Vec<_> = table_meta
                .columns
                .iter()
                .filter(|column| column.unique && !column.primary_key)
                .collect();
            if !unique_columns.is_empty() {
                let existing_rows = db.scan(&p.table, db.snapshot())?;
                for row in &p.values {
                    for column in &unique_columns {
                        let Some(index) = columns.iter().position(|name| name == &column.name)
                        else {
                            continue;
                        };
                        let value = row
                            .get(index)
                            .ok_or_else(|| {
                                Error::PlanError("column/value count mismatch".to_string())
                            })
                            .and_then(|expr| resolve_expr(expr, params))?;
                        if value == Value::Null {
                            continue;
                        }
                        if existing_rows
                            .iter()
                            .any(|existing| existing.values.get(&column.name) == Some(&value))
                        {
                            return Err(Error::UniqueViolation {
                                table: p.table.clone(),
                                column: column.name.clone(),
                            });
                        }
                    }
                }
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn resolve_expr(expr: &Expr, params: &HashMap<String, Value>) -> Result<Value> {
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
        Expr::Parameter(p) => params
            .get(p)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("missing parameter: {}", p))),
        Expr::Column(c) => Ok(Value::Text(c.column.clone())),
        _ => Err(Error::PlanError(
            "unsupported expression in schema enforcer".to_string(),
        )),
    }
}

fn cache_key_for_value(value: &Value) -> Vec<u8> {
    bincode::serde::encode_to_vec(value, bincode::config::standard())
        .expect("Value should serialize for uniqueness cache key generation")
}
