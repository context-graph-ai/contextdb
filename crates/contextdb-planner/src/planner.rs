use crate::plan::*;
use contextdb_core::{Direction, Error, PropagationRule, Result};
use contextdb_parser::ast::{
    AstPropagationRule, BinOp, Cte, Expr, FromItem, MatchClause, SelectBody, SelectStatement,
    SortDirection, Statement,
};
use std::collections::HashMap;

const DEFAULT_MATCH_DEPTH: u32 = 5;
const ENGINE_MAX_BFS_DEPTH: u32 = 10;
const DEFAULT_PROPAGATION_MAX_DEPTH: u32 = 10;

pub fn plan(stmt: &Statement) -> Result<PhysicalPlan> {
    match stmt {
        Statement::CreateTable(ct) => Ok(PhysicalPlan::CreateTable(CreateTablePlan {
            name: ct.name.clone(),
            columns: ct.columns.clone(),
            immutable: ct.immutable,
            state_machine: ct.state_machine.clone(),
            dag_edge_types: ct.dag_edge_types.clone(),
            propagation_rules: extract_propagation_rules(ct)?,
        })),
        Statement::AlterTable(at) => Ok(PhysicalPlan::AlterTable(AlterTablePlan {
            table: at.table.clone(),
            action: at.action.clone(),
        })),
        Statement::DropTable(dt) => Ok(PhysicalPlan::DropTable(dt.name.clone())),
        Statement::CreateIndex(ci) => Ok(PhysicalPlan::CreateIndex(CreateIndexPlan {
            name: ci.name.clone(),
            table: ci.table.clone(),
            columns: ci.columns.clone(),
        })),
        Statement::Insert(i) => Ok(PhysicalPlan::Insert(InsertPlan {
            table: i.table.clone(),
            columns: i.columns.clone(),
            values: i.values.clone(),
            on_conflict: i.on_conflict.clone().map(Into::into),
        })),
        Statement::Delete(d) => Ok(PhysicalPlan::Delete(DeletePlan {
            table: d.table.clone(),
            where_clause: d.where_clause.clone(),
        })),
        Statement::Update(u) => Ok(PhysicalPlan::Update(UpdatePlan {
            table: u.table.clone(),
            assignments: u.assignments.clone(),
            where_clause: u.where_clause.clone(),
        })),
        Statement::Select(sel) => plan_select(sel),
        Statement::SetMemoryLimit(val) => Ok(PhysicalPlan::SetMemoryLimit(val.clone())),
        Statement::ShowMemoryLimit => Ok(PhysicalPlan::ShowMemoryLimit),
        Statement::SetSyncConflictPolicy(policy) => {
            Ok(PhysicalPlan::SetSyncConflictPolicy(policy.clone()))
        }
        Statement::ShowSyncConflictPolicy => Ok(PhysicalPlan::ShowSyncConflictPolicy),
        Statement::Begin | Statement::Commit | Statement::Rollback => {
            Ok(PhysicalPlan::Pipeline(vec![]))
        }
    }
}

fn extract_propagation_rules(
    ct: &contextdb_parser::ast::CreateTable,
) -> Result<Vec<PropagationRule>> {
    let mut rules = Vec::new();

    for column in &ct.columns {
        if let Some(fk) = &column.references {
            for rule in &fk.propagation_rules {
                if let AstPropagationRule::FkState {
                    trigger_state,
                    target_state,
                    max_depth,
                    abort_on_failure,
                } = rule
                {
                    rules.push(PropagationRule::ForeignKey {
                        fk_column: column.name.clone(),
                        referenced_table: fk.table.clone(),
                        referenced_column: fk.column.clone(),
                        trigger_state: trigger_state.clone(),
                        target_state: target_state.clone(),
                        max_depth: max_depth.unwrap_or(DEFAULT_PROPAGATION_MAX_DEPTH),
                        abort_on_failure: *abort_on_failure,
                    });
                }
            }
        }
    }

    for rule in &ct.propagation_rules {
        match rule {
            AstPropagationRule::EdgeState {
                edge_type,
                direction,
                trigger_state,
                target_state,
                max_depth,
                abort_on_failure,
            } => {
                let direction = match direction.to_ascii_uppercase().as_str() {
                    "OUTGOING" => Direction::Outgoing,
                    "INCOMING" => Direction::Incoming,
                    "BOTH" => Direction::Both,
                    other => {
                        return Err(Error::PlanError(format!(
                            "invalid edge direction in propagation rule: {}",
                            other
                        )));
                    }
                };
                rules.push(PropagationRule::Edge {
                    edge_type: edge_type.clone(),
                    direction,
                    trigger_state: trigger_state.clone(),
                    target_state: target_state.clone(),
                    max_depth: max_depth.unwrap_or(DEFAULT_PROPAGATION_MAX_DEPTH),
                    abort_on_failure: *abort_on_failure,
                });
            }
            AstPropagationRule::VectorExclusion { trigger_state } => {
                rules.push(PropagationRule::VectorExclusion {
                    trigger_state: trigger_state.clone(),
                });
            }
            AstPropagationRule::FkState { .. } => {}
        }
    }

    Ok(rules)
}

fn plan_select(sel: &SelectStatement) -> Result<PhysicalPlan> {
    let mut cte_env = HashMap::new();

    for cte in &sel.ctes {
        match cte {
            Cte::MatchCte { name, match_clause } => {
                cte_env.insert(name.clone(), graph_bfs_from_match(match_clause)?);
            }
            Cte::SqlCte { name, query } => {
                cte_env.insert(name.clone(), plan_select_body(query, &cte_env)?);
            }
        }
    }

    plan_select_body(&sel.body, &cte_env)
}

fn plan_select_body(
    body: &SelectBody,
    cte_env: &HashMap<String, PhysicalPlan>,
) -> Result<PhysicalPlan> {
    let graph_from = body
        .from
        .iter()
        .find(|f| matches!(f, FromItem::GraphTable { .. }));

    let mut current = if let Some(from_item) = graph_from {
        graph_plan_from_from_item(from_item)?
    } else {
        let from_item = body.from.iter().find_map(|item| match item {
            FromItem::Table { name, alias } => Some((name.clone(), alias.clone())),
            FromItem::GraphTable { .. } => None,
        });

        match from_item {
            Some((from_table, from_alias)) => {
                if let Some(cte_plan) = cte_env.get(&from_table) {
                    cte_plan.clone()
                } else {
                    PhysicalPlan::Scan {
                        table: from_table,
                        alias: from_alias.clone(),
                        filter: if body.joins.is_empty() {
                            body.where_clause.clone()
                        } else {
                            None
                        },
                    }
                }
            }
            None => PhysicalPlan::Scan {
                table: "dual".to_string(),
                alias: None,
                filter: None,
            },
        }
    };

    if !body.joins.is_empty() {
        let left_alias = body.from.iter().find_map(|item| match item {
            FromItem::Table { alias, name } => alias.clone().or_else(|| Some(name.clone())),
            FromItem::GraphTable { .. } => None,
        });

        for join in &body.joins {
            let right = if let Some(cte_plan) = cte_env.get(&join.table) {
                cte_plan.clone()
            } else {
                PhysicalPlan::Scan {
                    table: join.table.clone(),
                    alias: join.alias.clone(),
                    filter: None,
                }
            };

            current = PhysicalPlan::Join {
                left: Box::new(current),
                right: Box::new(right),
                condition: join.on.clone(),
                join_type: match join.join_type {
                    contextdb_parser::ast::JoinType::Inner => JoinType::Inner,
                    contextdb_parser::ast::JoinType::Left => JoinType::Left,
                },
                left_alias: left_alias.clone(),
                right_alias: join.alias.clone().or_else(|| Some(join.table.clone())),
            };
        }

        if let Some(where_clause) = &body.where_clause {
            current = PhysicalPlan::Filter {
                input: Box::new(current),
                predicate: where_clause.clone(),
            };
        }
    }

    let uses_vector_search = body
        .order_by
        .first()
        .is_some_and(|order| matches!(order.direction, SortDirection::CosineDistance));

    if let Some(order) = body.order_by.first()
        && matches!(order.direction, SortDirection::CosineDistance)
    {
        let k = body.limit.ok_or(Error::UnboundedVectorSearch)?;
        current = PhysicalPlan::VectorSearch {
            table: body
                .from
                .iter()
                .find_map(|item| match item {
                    FromItem::Table { name, .. } => Some(name.clone()),
                    FromItem::GraphTable { .. } => None,
                })
                .unwrap_or_else(|| "observations".to_string()),
            column: "embedding".to_string(),
            query_expr: order.expr.clone(),
            k,
            candidates: Some(Box::new(current)),
        };
    }

    if !body.order_by.is_empty() && !uses_vector_search {
        current = PhysicalPlan::Sort {
            input: Box::new(current),
            keys: body
                .order_by
                .iter()
                .map(|item| SortKey {
                    expr: item.expr.clone(),
                    direction: item.direction,
                })
                .collect(),
        };
    }

    let is_select_star = matches!(
        body.columns.as_slice(),
        [contextdb_parser::ast::SelectColumn {
            expr: Expr::Column(contextdb_parser::ast::ColumnRef { table: None, column }),
            alias: None
        }] if column == "*"
    );
    if !is_select_star {
        current = PhysicalPlan::Project {
            input: Box::new(current),
            columns: body
                .columns
                .iter()
                .map(|column| ProjectColumn {
                    expr: column.expr.clone(),
                    alias: column.alias.clone(),
                })
                .collect(),
        };
    }

    if body.distinct {
        current = PhysicalPlan::Distinct {
            input: Box::new(current),
        };
    }

    if let Some(limit) = body.limit
        && !uses_vector_search
    {
        current = PhysicalPlan::Limit {
            input: Box::new(current),
            count: limit,
        };
    }

    Ok(current)
}

fn graph_plan_from_from_item(from_item: &FromItem) -> Result<PhysicalPlan> {
    match from_item {
        FromItem::GraphTable {
            match_clause,
            columns,
            ..
        } => {
            let bfs = graph_bfs_from_match(match_clause)?;
            if columns.is_empty() {
                Ok(bfs)
            } else {
                Ok(PhysicalPlan::Project {
                    input: Box::new(bfs),
                    columns: columns
                        .iter()
                        .map(|c| ProjectColumn {
                            expr: c.expr.clone(),
                            alias: Some(c.alias.clone()),
                        })
                        .collect(),
                })
            }
        }
        FromItem::Table { name, .. } => Ok(PhysicalPlan::Scan {
            table: name.clone(),
            alias: None,
            filter: None,
        }),
    }
}

fn graph_bfs_from_match(match_clause: &contextdb_parser::ast::MatchClause) -> Result<PhysicalPlan> {
    let step = match_clause
        .pattern
        .edges
        .first()
        .ok_or_else(|| Error::PlanError("MATCH must include at least one edge".into()))?;
    let max_depth = if step.max_hops == 0 {
        DEFAULT_MATCH_DEPTH
    } else {
        step.max_hops
    };
    if max_depth > ENGINE_MAX_BFS_DEPTH {
        return Err(Error::BfsDepthExceeded(max_depth));
    }

    Ok(PhysicalPlan::GraphBfs {
        start_expr: extract_graph_start_expr(match_clause)?,
        edge_types: step.edge_type.clone().map(|t| vec![t]).unwrap_or_default(),
        direction: match step.direction {
            contextdb_parser::ast::EdgeDirection::Outgoing => Direction::Outgoing,
            contextdb_parser::ast::EdgeDirection::Incoming => Direction::Incoming,
            contextdb_parser::ast::EdgeDirection::Both => Direction::Both,
        },
        min_depth: step.min_hops.max(1),
        max_depth,
        filter: match_clause.where_clause.clone(),
    })
}

fn extract_graph_start_expr(match_clause: &MatchClause) -> Result<Expr> {
    let start_alias = &match_clause.pattern.start.alias;
    if let Some(where_clause) = &match_clause.where_clause
        && let Some(expr) = find_graph_start_expr(where_clause, start_alias)
    {
        return Ok(expr);
    }

    Ok(Expr::Column(contextdb_parser::ast::ColumnRef {
        table: None,
        column: start_alias.clone(),
    }))
}

fn find_graph_start_expr(expr: &Expr, start_alias: &str) -> Option<Expr> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinOp::Eq,
            right,
        } => {
            if is_graph_start_id_ref(left, start_alias) {
                Some((**right).clone())
            } else if is_graph_start_id_ref(right, start_alias) {
                Some((**left).clone())
            } else {
                None
            }
        }
        Expr::BinaryOp { left, right, .. } => find_graph_start_expr(left, start_alias)
            .or_else(|| find_graph_start_expr(right, start_alias)),
        Expr::UnaryOp { operand, .. } => find_graph_start_expr(operand, start_alias),
        _ => None,
    }
}

fn is_graph_start_id_ref(expr: &Expr, start_alias: &str) -> bool {
    matches!(
        expr,
        Expr::Column(contextdb_parser::ast::ColumnRef {
            table: Some(table),
            column
        }) if table == start_alias && column == "id"
    )
}
