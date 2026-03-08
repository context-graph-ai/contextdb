use crate::database::Database;
use contextdb_core::{Error, GraphExecutor, Result, Value, schema};
use contextdb_parser::ast::{BinOp, Expr, Literal};
use contextdb_planner::PhysicalPlan;
use std::collections::HashMap;

pub fn validate_dml(
    plan: &PhysicalPlan,
    db: &Database,
    params: &HashMap<String, Value>,
) -> Result<()> {
    match plan {
        PhysicalPlan::Update(p) if schema::is_immutable(&p.table) => {
            Err(Error::ImmutableTable(p.table.clone()))
        }
        PhysicalPlan::Delete(p) if schema::is_immutable(&p.table) => {
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
        PhysicalPlan::Update(p) if p.table == "intentions" => {
            let is_archive = p.assignments.iter().any(|(k, v)| {
                k == "status"
                    && matches!(resolve_expr(v, params), Ok(Value::Text(s)) if s == "archived")
            });

            if !is_archive {
                return Ok(());
            }

            let intention_id = extract_id_filter(p.where_clause.as_ref(), params)?;
            let Some(intention_id) = intention_id else {
                return Ok(());
            };

            let decisions = db.scan("decisions", db.snapshot())?;
            let active_decisions: std::collections::HashSet<_> = decisions
                .into_iter()
                .filter(|r| r.values.get("status") == Some(&Value::Text("active".to_string())))
                .filter_map(|r| r.values.get("id").and_then(Value::as_uuid).copied())
                .collect();

            let incoming = db.graph().neighbors(
                intention_id,
                Some(&["SERVES".to_string()]),
                contextdb_core::Direction::Incoming,
                db.snapshot(),
            )?;

            if incoming
                .into_iter()
                .any(|(source, _, _)| active_decisions.contains(&source))
            {
                return Err(Error::InvalidStateTransition(
                    "cannot archive intention with active decisions".to_string(),
                ));
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

fn extract_id_filter(
    where_clause: Option<&Expr>,
    params: &HashMap<String, Value>,
) -> Result<Option<uuid::Uuid>> {
    let Some(Expr::BinaryOp { left, op, right }) = where_clause else {
        return Ok(None);
    };

    if !matches!(op, BinOp::Eq) {
        return Ok(None);
    }

    if !matches!(&**left, Expr::Column(c) if c.column == "id") {
        return Ok(None);
    }

    match resolve_expr(right, params)? {
        Value::Uuid(u) => Ok(Some(u)),
        Value::Text(s) => Ok(uuid::Uuid::parse_str(&s).ok()),
        _ => Ok(None),
    }
}
