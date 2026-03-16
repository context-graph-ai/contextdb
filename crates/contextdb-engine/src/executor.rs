use crate::database::{Database, QueryResult};
use contextdb_core::*;
use contextdb_parser::ast::{BinOp, DataType, Expr, Literal};
use contextdb_planner::{DeletePlan, InsertPlan, PhysicalPlan, UpdatePlan};
use roaring::RoaringTreemap;
use std::collections::{BTreeSet, HashMap};

pub(crate) fn execute_plan(
    db: &Database,
    plan: &PhysicalPlan,
    params: &HashMap<String, Value>,
    tx: Option<TxId>,
) -> Result<QueryResult> {
    match plan {
        PhysicalPlan::CreateTable(p) => {
            let meta = TableMeta {
                columns: p
                    .columns
                    .iter()
                    .map(|c| contextdb_core::ColumnDef {
                        name: c.name.clone(),
                        column_type: map_column_type(&c.data_type),
                        nullable: c.nullable,
                        primary_key: c.primary_key,
                    })
                    .collect(),
                immutable: p.immutable,
                state_machine: p.state_machine.as_ref().map(|sm| StateMachineConstraint {
                    column: sm.column.clone(),
                    transitions: sm
                        .transitions
                        .iter()
                        .map(|(from, tos)| (from.clone(), tos.clone()))
                        .collect(),
                }),
                dag_edge_types: p.dag_edge_types.clone(),
                natural_key_column: None,
            };
            db.relational_store().create_table(&p.name, meta);
            if let Some(table_meta) = db.table_meta(&p.name) {
                let lsn = db.next_lsn_for_ddl();
                db.log_create_table_ddl(&p.name, &table_meta, lsn);
            }
            Ok(QueryResult::empty_with_affected(0))
        }
        PhysicalPlan::DropTable(name) => {
            db.relational_store().drop_table(name);
            let lsn = db.next_lsn_for_ddl();
            db.log_drop_table_ddl(name, lsn);
            Ok(QueryResult::empty_with_affected(0))
        }
        PhysicalPlan::Insert(p) => exec_insert(db, p, params, tx),
        PhysicalPlan::Delete(p) => exec_delete(db, p, params, tx),
        PhysicalPlan::Update(p) => exec_update(db, p, params, tx),
        PhysicalPlan::Scan { table, filter } => {
            let snapshot = db.snapshot();
            let rows = db.scan(table, snapshot)?;
            materialize_rows(rows, filter.as_ref(), params)
        }
        PhysicalPlan::GraphBfs {
            start_expr,
            edge_types,
            direction,
            min_depth,
            max_depth,
            ..
        } => {
            let start = resolve_uuid(start_expr, params)?;
            let edge_types_ref = if edge_types.is_empty() {
                None
            } else {
                Some(edge_types.as_slice())
            };
            let res = db.graph().bfs(
                start,
                edge_types_ref,
                *direction,
                *min_depth,
                *max_depth,
                db.snapshot(),
            )?;

            Ok(QueryResult {
                columns: vec!["id".to_string(), "depth".to_string()],
                rows: res
                    .nodes
                    .into_iter()
                    .map(|n| vec![Value::Uuid(n.id), Value::Int64(n.depth as i64)])
                    .collect(),
                rows_affected: 0,
            })
        }
        PhysicalPlan::VectorSearch {
            query_expr,
            k,
            candidates,
            ..
        } => {
            let query_vec = resolve_vector_from_expr(query_expr, params)?;
            let candidate_bitmap = if let Some(cands_plan) = candidates {
                let qr = execute_plan(db, cands_plan, params, tx)?;
                let mut bm = RoaringTreemap::new();
                for row in qr.rows {
                    if let Some(Value::Int64(id)) = row.first() {
                        bm.insert(*id as u64);
                    }
                }
                Some(bm)
            } else {
                None
            };

            let res = db.query_vector(
                &query_vec,
                *k as usize,
                candidate_bitmap.as_ref(),
                db.snapshot(),
            )?;
            Ok(QueryResult {
                columns: vec!["row_id".to_string(), "score".to_string()],
                rows: res
                    .into_iter()
                    .map(|(rid, score)| {
                        vec![Value::Int64(rid as i64), Value::Float64(score as f64)]
                    })
                    .collect(),
                rows_affected: 0,
            })
        }
        PhysicalPlan::MaterializeCte { input, .. } => execute_plan(db, input, params, tx),
        PhysicalPlan::Pipeline(plans) => {
            let mut last = QueryResult::empty();
            for p in plans {
                last = execute_plan(db, p, params, tx)?;
            }
            Ok(last)
        }
        _ => Err(Error::PlanError(
            "unsupported plan node in executor".to_string(),
        )),
    }
}

fn exec_insert(
    db: &Database,
    p: &InsertPlan,
    params: &HashMap<String, Value>,
    tx: Option<TxId>,
) -> Result<QueryResult> {
    let txid = tx.ok_or_else(|| Error::Other("missing tx for insert".to_string()))?;

    let mut rows_affected = 0;
    for row in &p.values {
        let mut values = HashMap::new();
        for (idx, expr) in row.iter().enumerate() {
            let col = p
                .columns
                .get(idx)
                .ok_or_else(|| Error::PlanError("column/value count mismatch".to_string()))?;
            let v = resolve_expr(expr, params)?;
            values.insert(col.clone(), coerce_uuid_if_needed(col, v));
        }

        let row_id = if let Some(on_conflict) = &p.on_conflict {
            db.upsert_row(txid, &p.table, &on_conflict.columns[0], values.clone())?;
            0
        } else {
            db.insert_row(txid, &p.table, values.clone())?
        };

        if p.table == "edges"
            && let (
                Some(Value::Uuid(source)),
                Some(Value::Uuid(target)),
                Some(Value::Text(edge_type)),
            ) = (
                values.get("source_id"),
                values.get("target_id"),
                values.get("edge_type"),
            )
        {
            db.insert_edge(txid, *source, *target, edge_type.clone(), HashMap::new())?;
        }

        if let Some(Value::Vector(v)) = values.get("embedding")
            && row_id != 0
        {
            db.insert_vector(txid, row_id, v.clone())?;
        }

        rows_affected += 1;
    }

    Ok(QueryResult::empty_with_affected(rows_affected))
}

fn exec_delete(
    db: &Database,
    p: &DeletePlan,
    params: &HashMap<String, Value>,
    tx: Option<TxId>,
) -> Result<QueryResult> {
    let txid = tx.ok_or_else(|| Error::Other("missing tx for delete".to_string()))?;
    let snapshot = db.snapshot();
    let rows = db.scan(&p.table, snapshot)?;
    let matched: Vec<_> = rows
        .into_iter()
        .filter(|r| {
            p.where_clause
                .as_ref()
                .is_none_or(|w| row_matches(r, w, params).unwrap_or(false))
        })
        .collect();

    for row in &matched {
        db.delete_row(txid, &p.table, row.row_id)?;
    }

    Ok(QueryResult::empty_with_affected(matched.len() as u64))
}

fn exec_update(
    db: &Database,
    p: &UpdatePlan,
    params: &HashMap<String, Value>,
    tx: Option<TxId>,
) -> Result<QueryResult> {
    let txid = tx.ok_or_else(|| Error::Other("missing tx for update".to_string()))?;
    let snapshot = db.snapshot();
    let rows = db.scan(&p.table, snapshot)?;
    let matched: Vec<_> = rows
        .into_iter()
        .filter(|r| {
            p.where_clause
                .as_ref()
                .is_none_or(|w| row_matches(r, w, params).unwrap_or(false))
        })
        .collect();

    for row in &matched {
        let mut values = row.values.clone();
        for (k, vexpr) in &p.assignments {
            values.insert(k.clone(), resolve_expr(vexpr, params)?);
        }
        db.delete_row(txid, &p.table, row.row_id)?;
        db.insert_row(txid, &p.table, values)?;
    }

    Ok(QueryResult::empty_with_affected(matched.len() as u64))
}

fn materialize_rows(
    rows: Vec<VersionedRow>,
    filter: Option<&Expr>,
    params: &HashMap<String, Value>,
) -> Result<QueryResult> {
    let mut keys = BTreeSet::new();
    let filtered: Vec<VersionedRow> = rows
        .into_iter()
        .filter(|r| filter.is_none_or(|f| row_matches(r, f, params).unwrap_or(false)))
        .collect();

    for r in &filtered {
        for k in r.values.keys() {
            keys.insert(k.clone());
        }
    }

    let mut columns = vec!["row_id".to_string()];
    columns.extend(keys.iter().cloned());

    let rows = filtered
        .into_iter()
        .map(|r| {
            let mut out = vec![Value::Int64(r.row_id as i64)];
            for k in &keys {
                out.push(r.values.get(k).cloned().unwrap_or(Value::Null));
            }
            out
        })
        .collect();

    Ok(QueryResult {
        columns,
        rows,
        rows_affected: 0,
    })
}

fn row_matches(row: &VersionedRow, expr: &Expr, params: &HashMap<String, Value>) -> Result<bool> {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            let lv = eval_expr_value(row, left, params)?;
            let rv = eval_expr_value(row, right, params)?;
            Ok(match op {
                BinOp::Eq => lv == rv,
                BinOp::Neq => lv != rv,
                _ => false,
            })
        }
        _ => Ok(true),
    }
}

fn eval_expr_value(
    row: &VersionedRow,
    expr: &Expr,
    params: &HashMap<String, Value>,
) -> Result<Value> {
    match expr {
        Expr::Column(c) => Ok(row.values.get(&c.column).cloned().unwrap_or(Value::Null)),
        _ => resolve_expr(expr, params),
    }
}

fn resolve_expr(expr: &Expr, params: &HashMap<String, Value>) -> Result<Value> {
    match expr {
        Expr::Literal(l) => Ok(match l {
            Literal::Null => Value::Null,
            Literal::Bool(v) => Value::Bool(*v),
            Literal::Integer(v) => Value::Int64(*v),
            Literal::Real(v) => Value::Float64(*v),
            Literal::Text(v) => Value::Text(v.clone()),
        }),
        Expr::Parameter(p) => params
            .get(p)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("missing parameter: {}", p))),
        Expr::Column(c) => Ok(Value::Text(c.column.clone())),
        Expr::CosineDistance { right, .. } => resolve_expr(right, params),
        _ => Err(Error::PlanError("unsupported expression".to_string())),
    }
}

fn resolve_uuid(expr: &Expr, params: &HashMap<String, Value>) -> Result<uuid::Uuid> {
    match resolve_expr(expr, params)? {
        Value::Uuid(u) => Ok(u),
        Value::Text(t) => uuid::Uuid::parse_str(&t)
            .map_err(|e| Error::PlanError(format!("invalid uuid '{}': {}", t, e))),
        _ => Err(Error::PlanError(
            "graph start node must be UUID".to_string(),
        )),
    }
}

fn resolve_vector_from_expr(expr: &Expr, params: &HashMap<String, Value>) -> Result<Vec<f32>> {
    match resolve_expr(expr, params)? {
        Value::Vector(v) => Ok(v),
        Value::Text(name) => match params.get(&name) {
            Some(Value::Vector(v)) => Ok(v.clone()),
            _ => Err(Error::PlanError("vector parameter missing".to_string())),
        },
        _ => Err(Error::PlanError(
            "invalid vector query expression".to_string(),
        )),
    }
}

fn coerce_uuid_if_needed(col: &str, v: Value) -> Value {
    if (col == "id" || col.ends_with("_id"))
        && let Value::Text(s) = &v
        && let Ok(u) = uuid::Uuid::parse_str(s)
    {
        return Value::Uuid(u);
    }
    v
}

fn map_column_type(dtype: &DataType) -> contextdb_core::ColumnType {
    match dtype {
        DataType::Uuid => contextdb_core::ColumnType::Uuid,
        DataType::Text => contextdb_core::ColumnType::Text,
        DataType::Integer => contextdb_core::ColumnType::Integer,
        DataType::Real => contextdb_core::ColumnType::Real,
        DataType::Boolean => contextdb_core::ColumnType::Boolean,
        DataType::Timestamp => contextdb_core::ColumnType::Text,
        DataType::Json => contextdb_core::ColumnType::Json,
        DataType::Vector(dim) => contextdb_core::ColumnType::Vector(*dim as usize),
    }
}
