use crate::database::{Database, QueryResult};
use crate::sync_types::ConflictPolicy;
use contextdb_core::*;
use contextdb_parser::ast::{
    AlterAction, BinOp, ColumnRef, Cte, DataType, Expr, Literal, SelectStatement, SortDirection,
    Statement, UnaryOp,
};
use contextdb_planner::{DeletePlan, InsertPlan, PhysicalPlan, UpdatePlan, plan};
use roaring::RoaringTreemap;
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap};
use std::time::{SystemTime, UNIX_EPOCH};

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
                        unique: c.unique,
                        default: c.default.as_ref().map(|expr| format!("{expr:?}")),
                        expires: false,
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
                propagation_rules: p.propagation_rules.clone(),
                default_ttl_seconds: None,
                sync_safe: false,
                expires_column: None,
            };
            db.relational_store().create_table(&p.name, meta);
            if let Some(table_meta) = db.table_meta(&p.name) {
                db.persist_table_meta(&p.name, &table_meta)?;
                let lsn = db.next_lsn_for_ddl();
                db.log_create_table_ddl(&p.name, &table_meta, lsn);
            }
            Ok(QueryResult::empty_with_affected(0))
        }
        PhysicalPlan::DropTable(name) => {
            db.relational_store().drop_table(name);
            db.remove_persisted_table(name)?;
            let lsn = db.next_lsn_for_ddl();
            db.log_drop_table_ddl(name, lsn);
            Ok(QueryResult::empty_with_affected(0))
        }
        PhysicalPlan::AlterTable(p) => {
            let store = db.relational_store();
            match &p.action {
                AlterAction::AddColumn(col) => {
                    if col.primary_key {
                        return Err(Error::Other(
                            "adding a primary key column via ALTER TABLE is not supported"
                                .to_string(),
                        ));
                    }
                    let core_col = contextdb_core::ColumnDef {
                        name: col.name.clone(),
                        column_type: map_column_type(&col.data_type),
                        nullable: col.nullable,
                        primary_key: col.primary_key,
                        unique: col.unique,
                        default: col.default.as_ref().map(|expr| format!("{expr:?}")),
                        expires: false,
                    };
                    store
                        .alter_table_add_column(&p.table, core_col)
                        .map_err(Error::Other)?;
                }
                AlterAction::DropColumn(name) => {
                    store
                        .alter_table_drop_column(&p.table, name)
                        .map_err(Error::Other)?;
                }
                AlterAction::RenameColumn { from, to } => {
                    store
                        .alter_table_rename_column(&p.table, from, to)
                        .map_err(Error::Other)?;
                }
                AlterAction::SetRetain { .. } => { /* stub: no-op */ }
                AlterAction::DropRetain => { /* stub: no-op */ }
                AlterAction::SetSyncConflictPolicy(policy) => {
                    let cp = parse_conflict_policy(policy)?;
                    db.set_table_conflict_policy(&p.table, cp);
                }
                AlterAction::DropSyncConflictPolicy => {
                    db.drop_table_conflict_policy(&p.table);
                }
            }
            if let Some(table_meta) = db.table_meta(&p.table) {
                db.persist_table_meta(&p.table, &table_meta)?;
                if !matches!(
                    p.action,
                    AlterAction::AddColumn(_)
                        | AlterAction::SetRetain { .. }
                        | AlterAction::DropRetain
                        | AlterAction::SetSyncConflictPolicy(_)
                        | AlterAction::DropSyncConflictPolicy
                ) {
                    db.persist_table_rows(&p.table)?;
                }
                let lsn = db.next_lsn_for_ddl();
                db.log_alter_table_ddl(&p.table, &table_meta, lsn);
            }
            Ok(QueryResult::empty_with_affected(0))
        }
        PhysicalPlan::Insert(p) => exec_insert(db, p, params, tx),
        PhysicalPlan::Delete(p) => exec_delete(db, p, params, tx),
        PhysicalPlan::Update(p) => exec_update(db, p, params, tx),
        PhysicalPlan::Scan { table, filter, .. } => {
            if table == "dual" {
                return Ok(QueryResult {
                    columns: vec![],
                    rows: vec![vec![]],
                    rows_affected: 0,
                });
            }
            let snapshot = db.snapshot();
            let rows = db.scan(table, snapshot)?;
            let schema_columns = db.table_meta(table).map(|meta| {
                meta.columns
                    .into_iter()
                    .map(|column| column.name)
                    .collect::<Vec<_>>()
            });
            let resolved_filter = filter
                .as_ref()
                .map(|expr| resolve_in_subqueries(db, expr, params, tx))
                .transpose()?;
            materialize_rows(
                rows,
                resolved_filter.as_ref(),
                params,
                schema_columns.as_deref(),
            )
        }
        PhysicalPlan::GraphBfs {
            start_expr,
            start_candidates,
            steps,
            filter,
        } => {
            let start_uuids = match resolve_uuid(start_expr, params) {
                Ok(start) => vec![start],
                Err(Error::PlanError(_))
                    if matches!(
                        start_expr,
                        Expr::Column(contextdb_parser::ast::ColumnRef { table: None, .. })
                    ) =>
                {
                    // Start node not directly specified — check if a subquery or filter can help
                    if let Some(candidate_plan) = start_candidates {
                        resolve_graph_start_nodes_from_plan(db, candidate_plan, params, tx)?
                    } else if let Some(filter_expr) = filter {
                        let resolved_filter = resolve_in_subqueries(db, filter_expr, params, tx)?;
                        resolve_graph_start_nodes_from_filter(db, &resolved_filter, params)?
                    } else {
                        vec![]
                    }
                }
                Err(err) => return Err(err),
            };
            if start_uuids.is_empty() {
                return Ok(QueryResult {
                    columns: vec!["id".to_string(), "depth".to_string()],
                    rows: vec![],
                    rows_affected: 0,
                });
            }
            let snapshot = db.snapshot();
            let mut frontier = start_uuids
                .into_iter()
                .map(|id| (id, 0_u32))
                .collect::<Vec<_>>();

            for step in steps {
                let edge_types_ref = if step.edge_types.is_empty() {
                    None
                } else {
                    Some(step.edge_types.as_slice())
                };
                let mut next = HashMap::<uuid::Uuid, u32>::new();

                for (start, base_depth) in &frontier {
                    let res = db.graph().bfs(
                        *start,
                        edge_types_ref,
                        step.direction,
                        step.min_depth,
                        step.max_depth,
                        snapshot,
                    )?;
                    for node in res.nodes {
                        let total_depth = base_depth.saturating_add(node.depth);
                        next.entry(node.id)
                            .and_modify(|depth| *depth = (*depth).min(total_depth))
                            .or_insert(total_depth);
                    }
                }

                frontier = next.into_iter().collect();
                if frontier.is_empty() {
                    break;
                }
            }

            Ok(QueryResult {
                columns: vec!["id".to_string(), "depth".to_string()],
                rows: frontier
                    .into_iter()
                    .map(|(id, depth)| vec![Value::Uuid(id), Value::Int64(depth as i64)])
                    .collect(),
                rows_affected: 0,
            })
        }
        PhysicalPlan::VectorSearch {
            table,
            query_expr,
            k,
            candidates,
            ..
        }
        | PhysicalPlan::HnswSearch {
            table,
            query_expr,
            k,
            candidates,
            ..
        } => {
            let query_vec = resolve_vector_from_expr(query_expr, params)?;
            let snapshot = db.snapshot();
            let all_rows = db.scan(table, snapshot)?;
            let candidate_bitmap = if let Some(cands_plan) = candidates {
                let qr = execute_plan(db, cands_plan, params, tx)?;
                let mut bm = RoaringTreemap::new();
                let row_id_idx = qr.columns.iter().position(|column| {
                    column == "row_id" || column.rsplit('.').next() == Some("row_id")
                });
                let id_idx = qr
                    .columns
                    .iter()
                    .position(|column| column == "id" || column.rsplit('.').next() == Some("id"));

                if let Some(idx) = row_id_idx {
                    for row in qr.rows {
                        if let Some(Value::Int64(id)) = row.get(idx) {
                            bm.insert(*id as u64);
                        }
                    }
                } else if let Some(idx) = id_idx {
                    let uuid_to_row_id: HashMap<uuid::Uuid, u64> = all_rows
                        .iter()
                        .filter_map(|row| match row.values.get("id") {
                            Some(Value::Uuid(uuid)) => Some((*uuid, row.row_id)),
                            _ => None,
                        })
                        .collect();
                    for row in qr.rows {
                        if let Some(Value::Uuid(uuid)) = row.get(idx)
                            && let Some(row_id) = uuid_to_row_id.get(uuid)
                        {
                            bm.insert(*row_id);
                        }
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

            // Re-materialize: look up actual rows by row_id so SELECT * returns user columns
            let schema_columns = db.table_meta(table).map(|meta| {
                meta.columns
                    .into_iter()
                    .map(|column| column.name)
                    .collect::<Vec<_>>()
            });
            let keys = if let Some(ref sc) = schema_columns {
                sc.clone()
            } else {
                let mut ks = BTreeSet::new();
                for r in &all_rows {
                    for k in r.values.keys() {
                        ks.insert(k.clone());
                    }
                }
                ks.into_iter().collect::<Vec<_>>()
            };

            let row_map: HashMap<u64, &VersionedRow> =
                all_rows.iter().map(|r| (r.row_id, r)).collect();

            let mut columns = vec!["row_id".to_string()];
            columns.extend(keys.iter().cloned());
            columns.push("score".to_string());

            let rows = res
                .into_iter()
                .filter_map(|(rid, score)| {
                    row_map.get(&rid).map(|row| {
                        let mut out = vec![Value::Int64(rid as i64)];
                        for k in &keys {
                            out.push(row.values.get(k).cloned().unwrap_or(Value::Null));
                        }
                        out.push(Value::Float64(score as f64));
                        out
                    })
                })
                .collect();

            Ok(QueryResult {
                columns,
                rows,
                rows_affected: 0,
            })
        }
        PhysicalPlan::MaterializeCte { input, .. } => execute_plan(db, input, params, tx),
        PhysicalPlan::Project { input, columns } => {
            let input_result = execute_plan(db, input, params, tx)?;
            let has_aggregate = columns.iter().any(|column| {
                matches!(
                    &column.expr,
                    Expr::FunctionCall { name, .. } if name.eq_ignore_ascii_case("count")
                )
            });
            if has_aggregate {
                if columns.iter().any(|column| {
                    !matches!(
                        &column.expr,
                        Expr::FunctionCall { name, .. } if name.eq_ignore_ascii_case("count")
                    )
                }) {
                    return Err(Error::PlanError(
                        "mixed aggregate and non-aggregate columns without GROUP BY".to_string(),
                    ));
                }

                let output_columns = columns
                    .iter()
                    .map(|column| {
                        column.alias.clone().unwrap_or_else(|| match &column.expr {
                            Expr::FunctionCall { name, .. } => name.clone(),
                            _ => "expr".to_string(),
                        })
                    })
                    .collect::<Vec<_>>();

                let aggregate_row = columns
                    .iter()
                    .map(|column| match &column.expr {
                        Expr::FunctionCall { name: _, args } => {
                            let count = if matches!(
                                args.as_slice(),
                                [Expr::Column(contextdb_parser::ast::ColumnRef { table: None, column })]
                                if column == "*"
                            ) {
                                input_result.rows.len() as i64
                            } else {
                                input_result
                                    .rows
                                    .iter()
                                    .filter_map(|row| {
                                        args.first().map(|arg| {
                                            eval_query_result_expr(
                                                arg,
                                                row,
                                                &input_result.columns,
                                                params,
                                            )
                                        })
                                    })
                                    .collect::<Result<Vec<_>>>()?
                                    .into_iter()
                                    .filter(|value| *value != Value::Null)
                                    .count() as i64
                            };
                            Ok(Value::Int64(count))
                        }
                        _ => Err(Error::PlanError(
                            "mixed aggregate and non-aggregate columns without GROUP BY"
                                .to_string(),
                        )),
                    })
                    .collect::<Result<Vec<_>>>()?;

                return Ok(QueryResult {
                    columns: output_columns,
                    rows: vec![aggregate_row],
                    rows_affected: 0,
                });
            }

            let output_columns = columns
                .iter()
                .map(|c| {
                    c.alias.clone().unwrap_or_else(|| match &c.expr {
                        Expr::Column(col) => col.column.clone(),
                        _ => "expr".to_string(),
                    })
                })
                .collect::<Vec<_>>();

            let mut output_rows = Vec::with_capacity(input_result.rows.len());
            for row in &input_result.rows {
                let mut projected = Vec::with_capacity(columns.len());
                for col in columns {
                    projected.push(eval_project_expr(
                        &col.expr,
                        row,
                        &input_result.columns,
                        params,
                    )?);
                }
                output_rows.push(projected);
            }

            Ok(QueryResult {
                columns: output_columns,
                rows: output_rows,
                rows_affected: 0,
            })
        }
        PhysicalPlan::Sort { input, keys } => {
            let mut input_result = execute_plan(db, input, params, tx)?;
            input_result.rows.sort_by(|left, right| {
                for key in keys {
                    let Expr::Column(column_ref) = &key.expr else {
                        return Ordering::Equal;
                    };
                    let Some(idx) = input_result
                        .columns
                        .iter()
                        .position(|name| name == &column_ref.column)
                    else {
                        return Ordering::Equal;
                    };
                    let left_value = left.get(idx).unwrap_or(&Value::Null);
                    let right_value = right.get(idx).unwrap_or(&Value::Null);
                    let ordering = compare_sort_values(left_value, right_value, key.direction);
                    if ordering != Ordering::Equal {
                        return ordering;
                    }
                }
                Ordering::Equal
            });
            Ok(input_result)
        }
        PhysicalPlan::Limit { input, count } => {
            let mut input_result = execute_plan(db, input, params, tx)?;
            input_result.rows.truncate(*count as usize);
            Ok(input_result)
        }
        PhysicalPlan::Filter { input, predicate } => {
            let mut input_result = execute_plan(db, input, params, tx)?;
            input_result.rows.retain(|row| {
                query_result_row_matches(row, &input_result.columns, predicate, params)
                    .unwrap_or(false)
            });
            Ok(input_result)
        }
        PhysicalPlan::Distinct { input } => {
            let input_result = execute_plan(db, input, params, tx)?;
            let mut seen = Vec::<Vec<Value>>::new();
            let rows = input_result
                .rows
                .into_iter()
                .filter(|row| {
                    if seen.contains(row) {
                        false
                    } else {
                        seen.push(row.clone());
                        true
                    }
                })
                .collect();
            Ok(QueryResult {
                columns: input_result.columns,
                rows,
                rows_affected: input_result.rows_affected,
            })
        }
        PhysicalPlan::Join {
            left,
            right,
            condition,
            join_type,
            left_alias,
            right_alias,
        } => {
            let left_result = execute_plan(db, left, params, tx)?;
            let right_result = execute_plan(db, right, params, tx)?;
            let right_duplicate_names =
                duplicate_column_names(&left_result.columns, &right_result.columns);
            let right_prefix = right_alias
                .clone()
                .unwrap_or_else(|| right_table_name(right));
            let right_columns = right_result
                .columns
                .iter()
                .map(|column| {
                    if right_duplicate_names.contains(column) {
                        format!("{right_prefix}.{column}")
                    } else {
                        column.clone()
                    }
                })
                .collect::<Vec<_>>();

            let mut columns = left_result.columns.clone();
            columns.extend(right_columns);

            let mut rows = Vec::new();
            for left_row in &left_result.rows {
                let mut matched = false;
                for right_row in &right_result.rows {
                    let combined = concatenate_rows(left_row, right_row);
                    if query_result_row_matches(&combined, &columns, condition, params)? {
                        matched = true;
                        rows.push(combined);
                    }
                }

                if !matched && matches!(join_type, contextdb_planner::JoinType::Left) {
                    let mut combined = left_row.clone();
                    combined.extend(std::iter::repeat_n(Value::Null, right_result.columns.len()));
                    rows.push(combined);
                }
            }

            let output_columns = qualify_join_columns(
                &columns,
                &left_result.columns,
                &right_result.columns,
                left_alias,
                &right_prefix,
            );

            Ok(QueryResult {
                columns: output_columns,
                rows,
                rows_affected: 0,
            })
        }
        PhysicalPlan::CreateIndex(_) => Ok(QueryResult::empty_with_affected(0)),
        PhysicalPlan::SetMemoryLimit(_val) => {
            // Stub: returns empty result without calling set_budget.
            Ok(QueryResult::empty())
        }
        PhysicalPlan::ShowMemoryLimit => {
            // Stub: returns hardcoded zeros.
            Ok(QueryResult {
                columns: vec![
                    "limit".to_string(),
                    "used".to_string(),
                    "available".to_string(),
                    "startup_ceiling".to_string(),
                ],
                rows: vec![vec![
                    Value::Text("none".to_string()),
                    Value::Int64(0),
                    Value::Text("none".to_string()),
                    Value::Text("none".to_string()),
                ]],
                rows_affected: 0,
            })
        }
        PhysicalPlan::SetSyncConflictPolicy(policy) => {
            let cp = parse_conflict_policy(policy)?;
            db.set_default_conflict_policy(cp);
            Ok(QueryResult::empty())
        }
        PhysicalPlan::ShowSyncConflictPolicy => {
            let policies = db.conflict_policies();
            let default_str = conflict_policy_to_string(policies.default);
            let mut rows = vec![vec![Value::Text(default_str)]];
            for (table, policy) in &policies.per_table {
                rows.push(vec![Value::Text(format!(
                    "{}={}",
                    table,
                    conflict_policy_to_string(*policy)
                ))]);
            }
            Ok(QueryResult {
                columns: vec!["policy".to_string()],
                rows,
                rows_affected: 0,
            })
        }
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

fn eval_project_expr(
    expr: &Expr,
    row: &[Value],
    input_columns: &[String],
    params: &HashMap<String, Value>,
) -> Result<Value> {
    match expr {
        Expr::Column(c) => lookup_query_result_column(row, input_columns, c),
        Expr::Literal(lit) => resolve_expr(&Expr::Literal(lit.clone()), params),
        Expr::Parameter(name) => params
            .get(name)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("missing parameter: {}", name))),
        Expr::BinaryOp { left, op, right } => {
            let left = eval_query_result_expr(left, row, input_columns, params)?;
            let right = eval_query_result_expr(right, row, input_columns, params)?;
            eval_binary_op(op, &left, &right)
        }
        Expr::UnaryOp { op, operand } => {
            let value = eval_query_result_expr(operand, row, input_columns, params)?;
            match op {
                UnaryOp::Not => Ok(Value::Bool(!value_to_bool(&value))),
                UnaryOp::Neg => match value {
                    Value::Int64(v) => Ok(Value::Int64(-v)),
                    Value::Float64(v) => Ok(Value::Float64(-v)),
                    _ => Err(Error::PlanError(
                        "cannot negate non-numeric value".to_string(),
                    )),
                },
            }
        }
        Expr::FunctionCall { name, args } => {
            let values = args
                .iter()
                .map(|arg| eval_query_result_expr(arg, row, input_columns, params))
                .collect::<Result<Vec<_>>>()?;
            eval_function(name, &values)
        }
        Expr::IsNull { expr, negated } => {
            let is_null = eval_query_result_expr(expr, row, input_columns, params)? == Value::Null;
            Ok(Value::Bool(if *negated { !is_null } else { is_null }))
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let needle = eval_query_result_expr(expr, row, input_columns, params)?;
            let matched = list.iter().try_fold(false, |found, item| {
                if found {
                    Ok(true)
                } else {
                    let candidate = eval_query_result_expr(item, row, input_columns, params)?;
                    Ok(
                        matches!(compare_values(&needle, &candidate), Some(Ordering::Equal))
                            || (needle != Value::Null
                                && candidate != Value::Null
                                && needle == candidate),
                    )
                }
            })?;
            Ok(Value::Bool(if *negated { !matched } else { matched }))
        }
        Expr::Like {
            expr,
            pattern,
            negated,
        } => {
            let matches = match (
                eval_query_result_expr(expr, row, input_columns, params)?,
                eval_query_result_expr(pattern, row, input_columns, params)?,
            ) {
                (Value::Text(value), Value::Text(pattern)) => like_matches(&value, &pattern),
                _ => false,
            };
            Ok(Value::Bool(if *negated { !matches } else { matches }))
        }
        _ => resolve_expr(expr, params),
    }
}

fn eval_query_result_expr(
    expr: &Expr,
    row: &[Value],
    input_columns: &[String],
    params: &HashMap<String, Value>,
) -> Result<Value> {
    match expr {
        Expr::Column(c) => lookup_query_result_column(row, input_columns, c),
        Expr::Literal(lit) => resolve_expr(&Expr::Literal(lit.clone()), params),
        Expr::Parameter(name) => params
            .get(name)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("missing parameter: {}", name))),
        Expr::FunctionCall { name, args } => {
            let values = args
                .iter()
                .map(|arg| eval_query_result_expr(arg, row, input_columns, params))
                .collect::<Result<Vec<_>>>()?;
            eval_function(name, &values)
        }
        _ => resolve_expr(expr, params),
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

        validate_vector_columns(db, &p.table, &values)?;

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

        if let Some(v) = vector_value_for_table(db, &p.table, &values)
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
    let resolved_where = p
        .where_clause
        .as_ref()
        .map(|expr| resolve_in_subqueries(db, expr, params, tx))
        .transpose()?;
    let matched: Vec<_> = rows
        .into_iter()
        .filter(|r| {
            resolved_where
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
    let resolved_where = p
        .where_clause
        .as_ref()
        .map(|expr| resolve_in_subqueries(db, expr, params, tx))
        .transpose()?;
    let matched: Vec<_> = rows
        .into_iter()
        .filter(|r| {
            resolved_where
                .as_ref()
                .is_none_or(|w| row_matches(r, w, params).unwrap_or(false))
        })
        .collect();

    for row in &matched {
        let mut values = row.values.clone();
        for (k, vexpr) in &p.assignments {
            values.insert(k.clone(), eval_assignment_expr(vexpr, &row.values, params)?);
        }

        let old_has_vector = db.has_live_vector(row.row_id, snapshot);
        validate_vector_columns(db, &p.table, &values)?;
        let new_vector = vector_value_for_table(db, &p.table, &values).cloned();

        db.delete_row(txid, &p.table, row.row_id)?;
        if old_has_vector {
            db.delete_vector(txid, row.row_id)?;
        }

        let new_row_id = db.insert_row(txid, &p.table, values)?;
        if let Some(vector) = new_vector {
            db.insert_vector(txid, new_row_id, vector)?;
        }
    }

    Ok(QueryResult::empty_with_affected(matched.len() as u64))
}

fn materialize_rows(
    rows: Vec<VersionedRow>,
    filter: Option<&Expr>,
    params: &HashMap<String, Value>,
    schema_columns: Option<&[String]>,
) -> Result<QueryResult> {
    let filtered: Vec<VersionedRow> = rows
        .into_iter()
        .filter(|r| filter.is_none_or(|f| row_matches(r, f, params).unwrap_or(false)))
        .collect();

    let keys = if let Some(schema_columns) = schema_columns {
        schema_columns.to_vec()
    } else {
        let mut keys = BTreeSet::new();
        for r in &filtered {
            for k in r.values.keys() {
                keys.insert(k.clone());
            }
        }
        keys.into_iter().collect::<Vec<_>>()
    };

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
    Ok(eval_bool_expr(row, expr, params)?.unwrap_or(false))
}

fn eval_expr_value(
    row: &VersionedRow,
    expr: &Expr,
    params: &HashMap<String, Value>,
) -> Result<Value> {
    match expr {
        Expr::Column(c) => {
            if c.column == "row_id" {
                Ok(Value::Int64(row.row_id as i64))
            } else {
                Ok(row.values.get(&c.column).cloned().unwrap_or(Value::Null))
            }
        }
        Expr::BinaryOp { left, op, right } => {
            let left = eval_expr_value(row, left, params)?;
            let right = eval_expr_value(row, right, params)?;
            eval_binary_op(op, &left, &right)
        }
        Expr::UnaryOp { op, operand } => {
            let value = eval_expr_value(row, operand, params)?;
            match op {
                UnaryOp::Not => Ok(Value::Bool(!value_to_bool(&value))),
                UnaryOp::Neg => match value {
                    Value::Int64(v) => Ok(Value::Int64(-v)),
                    Value::Float64(v) => Ok(Value::Float64(-v)),
                    _ => Err(Error::PlanError(
                        "cannot negate non-numeric value".to_string(),
                    )),
                },
            }
        }
        Expr::FunctionCall { name, args } => eval_function_in_row_context(row, name, args, params),
        Expr::IsNull { expr, negated } => {
            let is_null = eval_expr_value(row, expr, params)? == Value::Null;
            Ok(Value::Bool(if *negated { !is_null } else { is_null }))
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let needle = eval_expr_value(row, expr, params)?;
            let matched = list.iter().try_fold(false, |found, item| {
                if found {
                    Ok(true)
                } else {
                    let candidate = eval_expr_value(row, item, params)?;
                    Ok(
                        matches!(compare_values(&needle, &candidate), Some(Ordering::Equal))
                            || (needle != Value::Null
                                && candidate != Value::Null
                                && needle == candidate),
                    )
                }
            })?;
            Ok(Value::Bool(if *negated { !matched } else { matched }))
        }
        Expr::Like {
            expr,
            pattern,
            negated,
        } => {
            let matches = match (
                eval_expr_value(row, expr, params)?,
                eval_expr_value(row, pattern, params)?,
            ) {
                (Value::Text(value), Value::Text(pattern)) => like_matches(&value, &pattern),
                _ => false,
            };
            Ok(Value::Bool(if *negated { !matches } else { matches }))
        }
        _ => resolve_expr(expr, params),
    }
}

pub fn resolve_expr(expr: &Expr, params: &HashMap<String, Value>) -> Result<Value> {
    match expr {
        Expr::Literal(l) => Ok(match l {
            Literal::Null => Value::Null,
            Literal::Bool(v) => Value::Bool(*v),
            Literal::Integer(v) => Value::Int64(*v),
            Literal::Real(v) => Value::Float64(*v),
            Literal::Text(v) => Value::Text(v.clone()),
            Literal::Vector(v) => Value::Vector(v.clone()),
        }),
        Expr::Parameter(p) => params
            .get(p)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("missing parameter: {}", p))),
        Expr::Column(c) => Ok(Value::Text(c.column.clone())),
        Expr::UnaryOp { op, operand } => match op {
            UnaryOp::Neg => match resolve_expr(operand, params)? {
                Value::Int64(v) => Ok(Value::Int64(-v)),
                Value::Float64(v) => Ok(Value::Float64(-v)),
                _ => Err(Error::PlanError(
                    "cannot negate non-numeric value".to_string(),
                )),
            },
            UnaryOp::Not => Err(Error::PlanError(
                "boolean NOT requires row context".to_string(),
            )),
        },
        Expr::FunctionCall { name, args } => {
            let values = args
                .iter()
                .map(|arg| resolve_expr(arg, params))
                .collect::<Result<Vec<_>>>()?;
            eval_function(name, &values)
        }
        Expr::CosineDistance { right, .. } => resolve_expr(right, params),
        _ => Err(Error::PlanError("unsupported expression".to_string())),
    }
}

fn compare_values(a: &Value, b: &Value) -> Option<Ordering> {
    match (a, b) {
        (Value::Int64(left), Value::Int64(right)) => Some(left.cmp(right)),
        (Value::Float64(left), Value::Float64(right)) => Some(left.total_cmp(right)),
        (Value::Text(left), Value::Text(right)) => Some(left.cmp(right)),
        (Value::Timestamp(left), Value::Timestamp(right)) => Some(left.cmp(right)),
        (Value::Int64(left), Value::Float64(right)) => Some((*left as f64).total_cmp(right)),
        (Value::Float64(left), Value::Int64(right)) => Some(left.total_cmp(&(*right as f64))),
        (Value::Timestamp(left), Value::Int64(right)) => Some(left.cmp(right)),
        (Value::Int64(left), Value::Timestamp(right)) => Some(left.cmp(right)),
        (Value::Bool(left), Value::Bool(right)) => Some(left.cmp(right)),
        (Value::Uuid(left), Value::Uuid(right)) => Some(left.cmp(right)),
        (Value::Uuid(u), Value::Text(t)) => {
            if let Ok(parsed) = t.parse::<uuid::Uuid>() {
                Some(u.cmp(&parsed))
            } else {
                None
            }
        }
        (Value::Text(t), Value::Uuid(u)) => {
            if let Ok(parsed) = t.parse::<uuid::Uuid>() {
                Some(parsed.cmp(u))
            } else {
                None
            }
        }
        (Value::Null, _) | (_, Value::Null) => None,
        _ => None,
    }
}

fn eval_bool_expr(
    row: &VersionedRow,
    expr: &Expr,
    params: &HashMap<String, Value>,
) -> Result<Option<bool>> {
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            BinOp::Eq | BinOp::Neq | BinOp::Lt | BinOp::Lte | BinOp::Gt | BinOp::Gte => {
                let left = eval_expr_value(row, left, params)?;
                let right = eval_expr_value(row, right, params)?;
                if left == Value::Null || right == Value::Null {
                    return Ok(None);
                }

                let result = match op {
                    BinOp::Eq => {
                        compare_values(&left, &right) == Some(Ordering::Equal) || left == right
                    }
                    BinOp::Neq => {
                        !(compare_values(&left, &right) == Some(Ordering::Equal) || left == right)
                    }
                    BinOp::Lt => compare_values(&left, &right) == Some(Ordering::Less),
                    BinOp::Lte => matches!(
                        compare_values(&left, &right),
                        Some(Ordering::Less | Ordering::Equal)
                    ),
                    BinOp::Gt => compare_values(&left, &right) == Some(Ordering::Greater),
                    BinOp::Gte => matches!(
                        compare_values(&left, &right),
                        Some(Ordering::Greater | Ordering::Equal)
                    ),
                    BinOp::And | BinOp::Or => unreachable!(),
                };
                Ok(Some(result))
            }
            BinOp::And => {
                let left = eval_bool_expr(row, left, params)?;
                if left == Some(false) {
                    return Ok(Some(false));
                }
                let right = eval_bool_expr(row, right, params)?;
                Ok(match (left, right) {
                    (Some(true), Some(true)) => Some(true),
                    (Some(true), other) => other,
                    (None, Some(false)) => Some(false),
                    (None, Some(true)) | (None, None) => None,
                    (Some(false), _) => Some(false),
                })
            }
            BinOp::Or => {
                let left = eval_bool_expr(row, left, params)?;
                if left == Some(true) {
                    return Ok(Some(true));
                }
                let right = eval_bool_expr(row, right, params)?;
                Ok(match (left, right) {
                    (Some(false), Some(false)) => Some(false),
                    (Some(false), other) => other,
                    (None, Some(true)) => Some(true),
                    (None, Some(false)) | (None, None) => None,
                    (Some(true), _) => Some(true),
                })
            }
        },
        Expr::UnaryOp {
            op: UnaryOp::Not,
            operand,
        } => Ok(eval_bool_expr(row, operand, params)?.map(|value| !value)),
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let needle = eval_expr_value(row, expr, params)?;
            if needle == Value::Null {
                return Ok(None);
            }

            let matched = list.iter().try_fold(false, |found, item| {
                if found {
                    Ok(true)
                } else {
                    let candidate = eval_expr_value(row, item, params)?;
                    Ok(
                        matches!(compare_values(&needle, &candidate), Some(Ordering::Equal))
                            || (candidate != Value::Null && needle == candidate),
                    )
                }
            })?;
            Ok(Some(if *negated { !matched } else { matched }))
        }
        Expr::InSubquery { .. } => Err(Error::PlanError(
            "IN (subquery) must be resolved before execution".to_string(),
        )),
        Expr::Like {
            expr,
            pattern,
            negated,
        } => {
            let left = eval_expr_value(row, expr, params)?;
            let right = eval_expr_value(row, pattern, params)?;
            let matched = match (left, right) {
                (Value::Text(value), Value::Text(pattern)) => like_matches(&value, &pattern),
                _ => false,
            };
            Ok(Some(if *negated { !matched } else { matched }))
        }
        Expr::IsNull { expr, negated } => {
            let is_null = eval_expr_value(row, expr, params)? == Value::Null;
            Ok(Some(if *negated { !is_null } else { is_null }))
        }
        Expr::FunctionCall { .. } => match eval_expr_value(row, expr, params)? {
            Value::Bool(value) => Ok(Some(value)),
            Value::Null => Ok(None),
            _ => Err(Error::PlanError(format!(
                "unsupported WHERE expression: {:?}",
                expr
            ))),
        },
        _ => Err(Error::PlanError(format!(
            "unsupported WHERE expression: {:?}",
            expr
        ))),
    }
}

fn eval_binary_op(op: &BinOp, left: &Value, right: &Value) -> Result<Value> {
    let bool_value = match op {
        BinOp::Eq => {
            if left == &Value::Null || right == &Value::Null {
                false
            } else {
                compare_values(left, right) == Some(Ordering::Equal) || left == right
            }
        }
        BinOp::Neq => {
            if left == &Value::Null || right == &Value::Null {
                false
            } else {
                !(compare_values(left, right) == Some(Ordering::Equal) || left == right)
            }
        }
        BinOp::Lt => compare_values(left, right) == Some(Ordering::Less),
        BinOp::Lte => matches!(
            compare_values(left, right),
            Some(Ordering::Less | Ordering::Equal)
        ),
        BinOp::Gt => compare_values(left, right) == Some(Ordering::Greater),
        BinOp::Gte => matches!(
            compare_values(left, right),
            Some(Ordering::Greater | Ordering::Equal)
        ),
        BinOp::And => value_to_bool(left) && value_to_bool(right),
        BinOp::Or => value_to_bool(left) || value_to_bool(right),
    };
    Ok(Value::Bool(bool_value))
}

fn value_to_bool(value: &Value) -> bool {
    matches!(value, Value::Bool(true))
}

fn compare_sort_values(left: &Value, right: &Value, direction: SortDirection) -> Ordering {
    match (left, right) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => match direction {
            SortDirection::Asc => Ordering::Greater,
            SortDirection::Desc => Ordering::Less,
            SortDirection::CosineDistance => Ordering::Equal,
        },
        (_, Value::Null) => match direction {
            SortDirection::Asc => Ordering::Less,
            SortDirection::Desc => Ordering::Greater,
            SortDirection::CosineDistance => Ordering::Equal,
        },
        _ => {
            let ordering = compare_values(left, right).unwrap_or(Ordering::Equal);
            match direction {
                SortDirection::Asc => ordering,
                SortDirection::Desc => ordering.reverse(),
                SortDirection::CosineDistance => ordering,
            }
        }
    }
}

fn eval_assignment_expr(
    expr: &Expr,
    row_values: &HashMap<String, Value>,
    params: &HashMap<String, Value>,
) -> Result<Value> {
    match expr {
        Expr::Literal(lit) => literal_to_value(lit),
        Expr::Parameter(name) => params
            .get(name)
            .cloned()
            .ok_or_else(|| Error::Other(format!("unknown parameter: {}", name))),
        Expr::Column(col_ref) => row_values
            .get(&col_ref.column)
            .cloned()
            .ok_or_else(|| Error::Other(format!("column not found: {}", col_ref.column))),
        Expr::BinaryOp { left, op, right } => {
            let left = eval_assignment_expr(left, row_values, params)?;
            let right = eval_assignment_expr(right, row_values, params)?;
            eval_binary_op(op, &left, &right)
        }
        Expr::UnaryOp { op, operand } => match op {
            UnaryOp::Neg => match eval_assignment_expr(operand, row_values, params)? {
                Value::Int64(value) => Ok(Value::Int64(-value)),
                Value::Float64(value) => Ok(Value::Float64(-value)),
                _ => Err(Error::Other(format!(
                    "unsupported expression in UPDATE SET: {:?}",
                    expr
                ))),
            },
            UnaryOp::Not => Err(Error::Other(format!(
                "unsupported expression in UPDATE SET: {:?}",
                expr
            ))),
        },
        Expr::FunctionCall { name, args } => {
            let evaluated = args
                .iter()
                .map(|arg| eval_assignment_expr(arg, row_values, params))
                .collect::<Result<Vec<_>>>()?;
            eval_function(name, &evaluated)
        }
        _ => Err(Error::Other(format!(
            "unsupported expression in UPDATE SET: {:?}",
            expr
        ))),
    }
}

fn literal_to_value(lit: &Literal) -> Result<Value> {
    Ok(match lit {
        Literal::Null => Value::Null,
        Literal::Bool(v) => Value::Bool(*v),
        Literal::Integer(v) => Value::Int64(*v),
        Literal::Real(v) => Value::Float64(*v),
        Literal::Text(v) => Value::Text(v.clone()),
        Literal::Vector(v) => Value::Vector(v.clone()),
    })
}

fn eval_arithmetic(name: &str, args: &[Value]) -> Result<Value> {
    let [left, right] = args else {
        return Err(Error::PlanError(format!(
            "function {} expects 2 arguments",
            name
        )));
    };

    match (left, right) {
        (Value::Int64(left), Value::Int64(right)) => match name {
            "__add" => Ok(Value::Int64(left + right)),
            "__sub" => Ok(Value::Int64(left - right)),
            "__mul" => Ok(Value::Int64(left * right)),
            "__div" => Ok(Value::Int64(left / right)),
            _ => Err(Error::PlanError(format!("unknown function: {}", name))),
        },
        (Value::Float64(left), Value::Float64(right)) => match name {
            "__add" => Ok(Value::Float64(left + right)),
            "__sub" => Ok(Value::Float64(left - right)),
            "__mul" => Ok(Value::Float64(left * right)),
            "__div" => Ok(Value::Float64(left / right)),
            _ => Err(Error::PlanError(format!("unknown function: {}", name))),
        },
        (Value::Int64(left), Value::Float64(right)) => match name {
            "__add" => Ok(Value::Float64(*left as f64 + right)),
            "__sub" => Ok(Value::Float64(*left as f64 - right)),
            "__mul" => Ok(Value::Float64(*left as f64 * right)),
            "__div" => Ok(Value::Float64(*left as f64 / right)),
            _ => Err(Error::PlanError(format!("unknown function: {}", name))),
        },
        (Value::Float64(left), Value::Int64(right)) => match name {
            "__add" => Ok(Value::Float64(left + *right as f64)),
            "__sub" => Ok(Value::Float64(left - *right as f64)),
            "__mul" => Ok(Value::Float64(left * *right as f64)),
            "__div" => Ok(Value::Float64(left / *right as f64)),
            _ => Err(Error::PlanError(format!("unknown function: {}", name))),
        },
        _ => Err(Error::PlanError(format!(
            "function {} expects numeric arguments",
            name
        ))),
    }
}

fn eval_function_in_row_context(
    row: &VersionedRow,
    name: &str,
    args: &[Expr],
    params: &HashMap<String, Value>,
) -> Result<Value> {
    let values = args
        .iter()
        .map(|arg| eval_expr_value(row, arg, params))
        .collect::<Result<Vec<_>>>()?;
    eval_function(name, &values)
}

fn eval_function(name: &str, args: &[Value]) -> Result<Value> {
    match name.to_ascii_lowercase().as_str() {
        "__add" | "__sub" | "__mul" | "__div" => eval_arithmetic(name, args),
        "coalesce" => Ok(args
            .iter()
            .find(|value| **value != Value::Null)
            .cloned()
            .unwrap_or(Value::Null)),
        "now" => Ok(Value::Timestamp(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|err| Error::PlanError(err.to_string()))?
                .as_secs() as i64,
        )),
        _ => Err(Error::PlanError(format!("unknown function: {}", name))),
    }
}

fn like_matches(value: &str, pattern: &str) -> bool {
    let value_chars = value.chars().collect::<Vec<_>>();
    let pattern_chars = pattern.chars().collect::<Vec<_>>();
    let (mut vi, mut pi) = (0usize, 0usize);
    let (mut star_idx, mut match_idx) = (None, 0usize);

    while vi < value_chars.len() {
        if pi < pattern_chars.len()
            && (pattern_chars[pi] == '_' || pattern_chars[pi] == value_chars[vi])
        {
            vi += 1;
            pi += 1;
        } else if pi < pattern_chars.len() && pattern_chars[pi] == '%' {
            star_idx = Some(pi);
            match_idx = vi;
            pi += 1;
        } else if let Some(star) = star_idx {
            pi = star + 1;
            match_idx += 1;
            vi = match_idx;
        } else {
            return false;
        }
    }

    while pi < pattern_chars.len() && pattern_chars[pi] == '%' {
        pi += 1;
    }

    pi == pattern_chars.len()
}

fn resolve_in_subqueries(
    db: &Database,
    expr: &Expr,
    params: &HashMap<String, Value>,
    tx: Option<TxId>,
) -> Result<Expr> {
    resolve_in_subqueries_with_ctes(db, expr, params, tx, &[])
}

pub(crate) fn resolve_in_subqueries_with_ctes(
    db: &Database,
    expr: &Expr,
    params: &HashMap<String, Value>,
    tx: Option<TxId>,
    ctes: &[Cte],
) -> Result<Expr> {
    match expr {
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => {
            // Detect correlated subqueries: WHERE references to outer tables
            let mut subquery_tables: std::collections::HashSet<String> = subquery
                .from
                .iter()
                .filter_map(|item| match item {
                    contextdb_parser::ast::FromItem::Table { name, .. } => Some(name.clone()),
                    _ => None,
                })
                .collect();
            // CTE names are valid table references within the subquery
            for cte in ctes {
                match cte {
                    Cte::SqlCte { name, .. } | Cte::MatchCte { name, .. } => {
                        subquery_tables.insert(name.clone());
                    }
                }
            }
            if let Some(where_clause) = &subquery.where_clause
                && has_outer_table_ref(where_clause, &subquery_tables)
            {
                return Err(Error::Other(
                    "correlated subqueries are not supported".to_string(),
                ));
            }

            let query_plan = plan(&Statement::Select(SelectStatement {
                ctes: ctes.to_vec(),
                body: (**subquery).clone(),
            }))?;
            let result = execute_plan(db, &query_plan, params, tx)?;
            let select_expr = subquery
                .columns
                .first()
                .map(|column| column.expr.clone())
                .ok_or_else(|| Error::PlanError("subquery must select one column".to_string()))?;
            let list = result
                .rows
                .iter()
                .map(|row| eval_project_expr(&select_expr, row, &result.columns, params))
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .map(value_to_literal)
                .collect::<Result<Vec<_>>>()?;
            Ok(Expr::InList {
                expr: Box::new(resolve_in_subqueries_with_ctes(db, expr, params, tx, ctes)?),
                list,
                negated: *negated,
            })
        }
        Expr::BinaryOp { left, op, right } => Ok(Expr::BinaryOp {
            left: Box::new(resolve_in_subqueries_with_ctes(db, left, params, tx, ctes)?),
            op: *op,
            right: Box::new(resolve_in_subqueries_with_ctes(
                db, right, params, tx, ctes,
            )?),
        }),
        Expr::UnaryOp { op, operand } => Ok(Expr::UnaryOp {
            op: *op,
            operand: Box::new(resolve_in_subqueries_with_ctes(
                db, operand, params, tx, ctes,
            )?),
        }),
        Expr::InList {
            expr,
            list,
            negated,
        } => Ok(Expr::InList {
            expr: Box::new(resolve_in_subqueries_with_ctes(db, expr, params, tx, ctes)?),
            list: list
                .iter()
                .map(|item| resolve_in_subqueries_with_ctes(db, item, params, tx, ctes))
                .collect::<Result<Vec<_>>>()?,
            negated: *negated,
        }),
        Expr::Like {
            expr,
            pattern,
            negated,
        } => Ok(Expr::Like {
            expr: Box::new(resolve_in_subqueries_with_ctes(db, expr, params, tx, ctes)?),
            pattern: Box::new(resolve_in_subqueries_with_ctes(
                db, pattern, params, tx, ctes,
            )?),
            negated: *negated,
        }),
        Expr::IsNull { expr, negated } => Ok(Expr::IsNull {
            expr: Box::new(resolve_in_subqueries_with_ctes(db, expr, params, tx, ctes)?),
            negated: *negated,
        }),
        Expr::FunctionCall { name, args } => Ok(Expr::FunctionCall {
            name: name.clone(),
            args: args
                .iter()
                .map(|arg| resolve_in_subqueries_with_ctes(db, arg, params, tx, ctes))
                .collect::<Result<Vec<_>>>()?,
        }),
        _ => Ok(expr.clone()),
    }
}

fn has_outer_table_ref(expr: &Expr, subquery_tables: &std::collections::HashSet<String>) -> bool {
    match expr {
        Expr::Column(ColumnRef {
            table: Some(table), ..
        }) => !subquery_tables.contains(table),
        Expr::BinaryOp { left, right, .. } => {
            has_outer_table_ref(left, subquery_tables)
                || has_outer_table_ref(right, subquery_tables)
        }
        Expr::UnaryOp { operand, .. } => has_outer_table_ref(operand, subquery_tables),
        Expr::InList { expr, list, .. } => {
            has_outer_table_ref(expr, subquery_tables)
                || list
                    .iter()
                    .any(|item| has_outer_table_ref(item, subquery_tables))
        }
        Expr::IsNull { expr, .. } => has_outer_table_ref(expr, subquery_tables),
        Expr::Like { expr, pattern, .. } => {
            has_outer_table_ref(expr, subquery_tables)
                || has_outer_table_ref(pattern, subquery_tables)
        }
        Expr::FunctionCall { args, .. } => args
            .iter()
            .any(|arg| has_outer_table_ref(arg, subquery_tables)),
        _ => false,
    }
}

fn value_to_literal(value: Value) -> Result<Expr> {
    Ok(Expr::Literal(match value {
        Value::Null => Literal::Null,
        Value::Bool(v) => Literal::Bool(v),
        Value::Int64(v) => Literal::Integer(v),
        Value::Float64(v) => Literal::Real(v),
        Value::Text(v) => Literal::Text(v),
        Value::Uuid(v) => Literal::Text(v.to_string()),
        Value::Timestamp(v) => Literal::Integer(v),
        other => {
            return Err(Error::PlanError(format!(
                "unsupported subquery result value: {:?}",
                other
            )));
        }
    }))
}

fn query_result_row_matches(
    row: &[Value],
    columns: &[String],
    expr: &Expr,
    params: &HashMap<String, Value>,
) -> Result<bool> {
    Ok(eval_query_result_bool_expr(row, columns, expr, params)?.unwrap_or(false))
}

fn eval_query_result_bool_expr(
    row: &[Value],
    columns: &[String],
    expr: &Expr,
    params: &HashMap<String, Value>,
) -> Result<Option<bool>> {
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            BinOp::Eq | BinOp::Neq | BinOp::Lt | BinOp::Lte | BinOp::Gt | BinOp::Gte => {
                let left = eval_query_result_expr(left, row, columns, params)?;
                let right = eval_query_result_expr(right, row, columns, params)?;
                if left == Value::Null || right == Value::Null {
                    return Ok(None);
                }

                let result = match op {
                    BinOp::Eq => {
                        compare_values(&left, &right) == Some(Ordering::Equal) || left == right
                    }
                    BinOp::Neq => {
                        !(compare_values(&left, &right) == Some(Ordering::Equal) || left == right)
                    }
                    BinOp::Lt => compare_values(&left, &right) == Some(Ordering::Less),
                    BinOp::Lte => matches!(
                        compare_values(&left, &right),
                        Some(Ordering::Less | Ordering::Equal)
                    ),
                    BinOp::Gt => compare_values(&left, &right) == Some(Ordering::Greater),
                    BinOp::Gte => matches!(
                        compare_values(&left, &right),
                        Some(Ordering::Greater | Ordering::Equal)
                    ),
                    BinOp::And | BinOp::Or => unreachable!(),
                };
                Ok(Some(result))
            }
            BinOp::And => {
                let left = eval_query_result_bool_expr(row, columns, left, params)?;
                if left == Some(false) {
                    return Ok(Some(false));
                }
                let right = eval_query_result_bool_expr(row, columns, right, params)?;
                Ok(match (left, right) {
                    (Some(true), Some(true)) => Some(true),
                    (Some(true), other) => other,
                    (None, Some(false)) => Some(false),
                    (None, Some(true)) | (None, None) => None,
                    (Some(false), _) => Some(false),
                })
            }
            BinOp::Or => {
                let left = eval_query_result_bool_expr(row, columns, left, params)?;
                if left == Some(true) {
                    return Ok(Some(true));
                }
                let right = eval_query_result_bool_expr(row, columns, right, params)?;
                Ok(match (left, right) {
                    (Some(false), Some(false)) => Some(false),
                    (Some(false), other) => other,
                    (None, Some(true)) => Some(true),
                    (None, Some(false)) | (None, None) => None,
                    (Some(true), _) => Some(true),
                })
            }
        },
        Expr::UnaryOp {
            op: UnaryOp::Not,
            operand,
        } => Ok(eval_query_result_bool_expr(row, columns, operand, params)?.map(|value| !value)),
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let needle = eval_query_result_expr(expr, row, columns, params)?;
            if needle == Value::Null {
                return Ok(None);
            }

            let matched = list.iter().try_fold(false, |found, item| {
                if found {
                    Ok(true)
                } else {
                    let candidate = eval_query_result_expr(item, row, columns, params)?;
                    Ok(
                        matches!(compare_values(&needle, &candidate), Some(Ordering::Equal))
                            || (candidate != Value::Null && needle == candidate),
                    )
                }
            })?;
            Ok(Some(if *negated { !matched } else { matched }))
        }
        Expr::InSubquery { .. } => Err(Error::PlanError(
            "IN (subquery) must be resolved before execution".to_string(),
        )),
        Expr::Like {
            expr,
            pattern,
            negated,
        } => {
            let left = eval_query_result_expr(expr, row, columns, params)?;
            let right = eval_query_result_expr(pattern, row, columns, params)?;
            let matched = match (left, right) {
                (Value::Text(value), Value::Text(pattern)) => like_matches(&value, &pattern),
                _ => false,
            };
            Ok(Some(if *negated { !matched } else { matched }))
        }
        Expr::IsNull { expr, negated } => {
            let is_null = eval_query_result_expr(expr, row, columns, params)? == Value::Null;
            Ok(Some(if *negated { !is_null } else { is_null }))
        }
        Expr::FunctionCall { .. } => match eval_query_result_expr(expr, row, columns, params)? {
            Value::Bool(value) => Ok(Some(value)),
            Value::Null => Ok(None),
            _ => Err(Error::PlanError(format!(
                "unsupported WHERE expression: {:?}",
                expr
            ))),
        },
        _ => Err(Error::PlanError(format!(
            "unsupported WHERE expression: {:?}",
            expr
        ))),
    }
}

fn lookup_query_result_column(
    row: &[Value],
    input_columns: &[String],
    column_ref: &ColumnRef,
) -> Result<Value> {
    if let Some(table) = &column_ref.table {
        let qualified = format!("{table}.{}", column_ref.column);
        let idx = input_columns
            .iter()
            .position(|name| name == &qualified || name == &column_ref.column)
            .ok_or_else(|| Error::PlanError(format!("project column not found: {}", qualified)))?;
        return Ok(row.get(idx).cloned().unwrap_or(Value::Null));
    }

    let idx = input_columns
        .iter()
        .position(|name| {
            name == &column_ref.column
                || name.rsplit('.').next() == Some(column_ref.column.as_str())
        })
        .ok_or_else(|| {
            Error::PlanError(format!("project column not found: {}", column_ref.column))
        })?;
    Ok(row.get(idx).cloned().unwrap_or(Value::Null))
}

fn concatenate_rows(left: &[Value], right: &[Value]) -> Vec<Value> {
    let mut combined = Vec::with_capacity(left.len() + right.len());
    combined.extend_from_slice(left);
    combined.extend_from_slice(right);
    combined
}

fn duplicate_column_names(left: &[String], right: &[String]) -> BTreeSet<String> {
    let left_names = left
        .iter()
        .map(|column| column.rsplit('.').next().unwrap_or(column.as_str()))
        .collect::<BTreeSet<_>>();
    right
        .iter()
        .filter_map(|column| {
            let bare = column.rsplit('.').next().unwrap_or(column.as_str());
            left_names.contains(bare).then(|| bare.to_string())
        })
        .collect()
}

fn qualify_join_columns(
    columns: &[String],
    left_columns: &[String],
    right_columns: &[String],
    left_alias: &Option<String>,
    right_prefix: &str,
) -> Vec<String> {
    let left_prefix = left_alias.as_deref();
    columns
        .iter()
        .enumerate()
        .map(|(idx, column)| {
            if idx < left_columns.len() {
                if let Some(prefix) = left_prefix {
                    format!(
                        "{prefix}.{}",
                        left_columns[idx].rsplit('.').next().unwrap_or(column)
                    )
                } else {
                    left_columns[idx].clone()
                }
            } else {
                let right_idx = idx - left_columns.len();
                let bare = right_columns[right_idx]
                    .rsplit('.')
                    .next()
                    .unwrap_or(right_columns[right_idx].as_str());
                if column == bare {
                    format!("{right_prefix}.{bare}")
                } else {
                    column.clone()
                }
            }
        })
        .collect()
}

fn right_table_name(plan: &PhysicalPlan) -> String {
    match plan {
        PhysicalPlan::Scan { table, alias, .. } => alias.clone().unwrap_or_else(|| table.clone()),
        _ => "right".to_string(),
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

/// Resolve start nodes for a graph BFS from a WHERE filter like `a.name = 'entity-0'`.
/// Scans all relational tables for rows matching the filter condition.
fn resolve_graph_start_nodes_from_filter(
    db: &Database,
    filter: &Expr,
    params: &HashMap<String, Value>,
) -> Result<Vec<uuid::Uuid>> {
    if let Some(ids) = resolve_graph_start_ids_from_filter(filter, params)? {
        return Ok(ids);
    }

    // Extract column name and expected value from the filter (e.g., a.name = 'entity-0')
    let (col_name, expected_value) = match filter {
        Expr::BinaryOp {
            left,
            op: BinOp::Eq,
            right,
        } => {
            if let Some(col) = extract_column_name(left) {
                (col, resolve_expr(right, params)?)
            } else if let Some(col) = extract_column_name(right) {
                (col, resolve_expr(left, params)?)
            } else {
                return Ok(vec![]);
            }
        }
        _ => return Ok(vec![]),
    };

    let snapshot = db.snapshot();
    let mut uuids = Vec::new();
    for table_name in db.table_names() {
        let meta = match db.table_meta(&table_name) {
            Some(m) => m,
            None => continue,
        };
        // Only scan tables that have the referenced column and an id column
        let has_col = meta.columns.iter().any(|c| c.name == col_name);
        let has_id = meta.columns.iter().any(|c| c.name == "id");
        if !has_col || !has_id {
            continue;
        }
        let rows = db.scan_filter(&table_name, snapshot, &|row| {
            row.values.get(&col_name) == Some(&expected_value)
        })?;
        for row in rows {
            if let Some(Value::Uuid(id)) = row.values.get("id") {
                uuids.push(*id);
            }
        }
    }
    Ok(uuids)
}

fn resolve_graph_start_nodes_from_plan(
    db: &Database,
    plan: &PhysicalPlan,
    params: &HashMap<String, Value>,
    tx: Option<TxId>,
) -> Result<Vec<uuid::Uuid>> {
    let result = execute_plan(db, plan, params, tx)?;
    result
        .rows
        .into_iter()
        .filter_map(|row| row.into_iter().next())
        .map(|value| match value {
            Value::Uuid(id) => Ok(id),
            Value::Text(text) => uuid::Uuid::parse_str(&text)
                .map_err(|_| Error::PlanError(format!("invalid UUID in graph start plan: {text}"))),
            other => Err(Error::PlanError(format!(
                "invalid graph start identifier from plan: {other:?}"
            ))),
        })
        .collect()
}

fn resolve_graph_start_ids_from_filter(
    filter: &Expr,
    params: &HashMap<String, Value>,
) -> Result<Option<Vec<uuid::Uuid>>> {
    match filter {
        Expr::BinaryOp {
            left,
            op: BinOp::Eq,
            right,
        } if is_graph_id_ref(left) || is_graph_id_ref(right) => {
            let value = if is_graph_id_ref(left) {
                resolve_expr(right, params)?
            } else {
                resolve_expr(left, params)?
            };
            let id = match value {
                Value::Uuid(id) => id,
                Value::Text(text) => uuid::Uuid::parse_str(&text)
                    .map_err(|_| Error::PlanError(format!("invalid UUID in graph filter: {text}")))?,
                other => {
                    return Err(Error::PlanError(format!(
                        "invalid graph start identifier in filter: {other:?}"
                    )));
                }
            };
            Ok(Some(vec![id]))
        }
        Expr::InList { expr, list, .. } if is_graph_id_ref(expr) => {
            let ids = list
                .iter()
                .map(|item| resolve_expr(item, params))
                .map(|value| match value? {
                    Value::Uuid(id) => Ok(id),
                    Value::Text(text) => uuid::Uuid::parse_str(&text)
                        .map_err(|_| Error::PlanError(format!("invalid UUID in graph filter: {text}"))),
                    other => Err(Error::PlanError(format!(
                        "invalid graph start identifier in filter: {other:?}"
                    ))),
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Some(ids))
        }
        Expr::BinaryOp { left, right, .. } => {
            if let Some(ids) = resolve_graph_start_ids_from_filter(left, params)? {
                return Ok(Some(ids));
            }
            resolve_graph_start_ids_from_filter(right, params)
        }
        Expr::UnaryOp { operand, .. } => resolve_graph_start_ids_from_filter(operand, params),
        _ => Ok(None),
    }
}

fn is_graph_id_ref(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Column(contextdb_parser::ast::ColumnRef { column, .. }) if column == "id"
    )
}

/// Extract a bare column name from an Expr::Column, ignoring table alias.
fn extract_column_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Column(contextdb_parser::ast::ColumnRef { column, .. }) => Some(column.clone()),
        _ => None,
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

fn validate_vector_columns(
    db: &Database,
    table: &str,
    values: &HashMap<String, Value>,
) -> Result<()> {
    let Some(meta) = db.table_meta(table) else {
        return Ok(());
    };

    for column in &meta.columns {
        if let contextdb_core::ColumnType::Vector(expected) = column.column_type
            && let Some(Value::Vector(vector)) = values.get(&column.name)
        {
            let got = vector.len();
            if got != expected {
                return Err(Error::VectorDimensionMismatch { expected, got });
            }
        }
    }

    Ok(())
}

fn vector_value_for_table<'a>(
    db: &Database,
    table: &str,
    values: &'a HashMap<String, Value>,
) -> Option<&'a Vec<f32>> {
    let meta = db.table_meta(table)?;
    meta.columns.iter().find_map(|column| match column.column_type {
        contextdb_core::ColumnType::Vector(_) => match values.get(&column.name) {
            Some(Value::Vector(vector)) => Some(vector),
            _ => None,
        },
        _ => None,
    })
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

pub(crate) fn map_column_type(dtype: &DataType) -> contextdb_core::ColumnType {
    match dtype {
        DataType::Uuid => contextdb_core::ColumnType::Uuid,
        DataType::Text => contextdb_core::ColumnType::Text,
        DataType::Integer => contextdb_core::ColumnType::Integer,
        DataType::Real => contextdb_core::ColumnType::Real,
        DataType::Boolean => contextdb_core::ColumnType::Boolean,
        DataType::Timestamp => contextdb_core::ColumnType::Timestamp,
        DataType::Json => contextdb_core::ColumnType::Json,
        DataType::Vector(dim) => contextdb_core::ColumnType::Vector(*dim as usize),
    }
}

fn parse_conflict_policy(s: &str) -> Result<ConflictPolicy> {
    match s {
        "latest_wins" => Ok(ConflictPolicy::LatestWins),
        "server_wins" => Ok(ConflictPolicy::ServerWins),
        "edge_wins" => Ok(ConflictPolicy::EdgeWins),
        "insert_if_not_exists" => Ok(ConflictPolicy::InsertIfNotExists),
        _ => Err(Error::Other(format!("unknown conflict policy: {s}"))),
    }
}

fn conflict_policy_to_string(p: ConflictPolicy) -> String {
    match p {
        ConflictPolicy::LatestWins => "latest_wins".to_string(),
        ConflictPolicy::ServerWins => "server_wins".to_string(),
        ConflictPolicy::EdgeWins => "edge_wins".to_string(),
        ConflictPolicy::InsertIfNotExists => "insert_if_not_exists".to_string(),
    }
}
