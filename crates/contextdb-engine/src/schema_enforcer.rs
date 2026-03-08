use crate::database::Database;
use contextdb_core::{Error, Result, Value};
use contextdb_parser::ast::{Expr, Literal};
use contextdb_planner::PhysicalPlan;
use std::collections::HashMap;

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
            if p.on_conflict.is_none()
                && let Some(id_idx) = p.columns.iter().position(|c| c == "id")
            {
                for row in &p.values {
                    let id_value = row
                        .get(id_idx)
                        .map(|e| resolve_expr(e, params))
                        .transpose()?;
                    if let Some(v) = id_value {
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
