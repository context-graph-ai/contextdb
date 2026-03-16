use crate::plan::*;
use contextdb_core::{Direction, Error, Result};
use contextdb_parser::ast::{Cte, Expr, FromItem, SelectStatement, SortDirection, Statement};

const DEFAULT_MATCH_DEPTH: u32 = 5;
const ENGINE_MAX_BFS_DEPTH: u32 = 10;

pub fn plan(stmt: &Statement) -> Result<PhysicalPlan> {
    match stmt {
        Statement::CreateTable(ct) => Ok(PhysicalPlan::CreateTable(CreateTablePlan {
            name: ct.name.clone(),
            columns: ct.columns.clone(),
            immutable: ct.immutable,
            state_machine: ct.state_machine.clone(),
            dag_edge_types: ct.dag_edge_types.clone(),
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
        Statement::Begin | Statement::Commit | Statement::Rollback => {
            Ok(PhysicalPlan::Pipeline(vec![]))
        }
    }
}

fn plan_select(sel: &SelectStatement) -> Result<PhysicalPlan> {
    let mut pipeline = Vec::new();

    for cte in &sel.ctes {
        match cte {
            Cte::MatchCte { name, match_clause } => {
                let step = match_clause.pattern.edges.first().ok_or_else(|| {
                    Error::PlanError("MATCH must include at least one edge".into())
                })?;
                let max_depth = if step.max_hops == 0 {
                    DEFAULT_MATCH_DEPTH
                } else {
                    step.max_hops
                };
                if max_depth > ENGINE_MAX_BFS_DEPTH {
                    return Err(Error::BfsDepthExceeded(max_depth));
                }

                let bfs = PhysicalPlan::GraphBfs {
                    start_expr: Expr::Column(contextdb_parser::ast::ColumnRef {
                        table: None,
                        column: match_clause.pattern.start.alias.clone(),
                    }),
                    edge_types: step.edge_type.clone().map(|t| vec![t]).unwrap_or_default(),
                    direction: match step.direction {
                        contextdb_parser::ast::EdgeDirection::Outgoing => Direction::Outgoing,
                        contextdb_parser::ast::EdgeDirection::Incoming => Direction::Incoming,
                        contextdb_parser::ast::EdgeDirection::Both => Direction::Both,
                    },
                    min_depth: step.min_hops.max(1),
                    max_depth,
                    filter: match_clause.where_clause.clone(),
                };

                pipeline.push(PhysicalPlan::MaterializeCte {
                    name: name.clone(),
                    input: Box::new(bfs),
                });
            }
            Cte::SqlCte { name, .. } => pipeline.push(PhysicalPlan::MaterializeCte {
                name: name.clone(),
                input: Box::new(PhysicalPlan::CteRef { name: name.clone() }),
            }),
        }
    }

    let from_table = sel
        .body
        .from
        .iter()
        .find_map(|item| match item {
            FromItem::Table { name, .. } => Some(name.clone()),
            FromItem::GraphTable { .. } => None,
        })
        .unwrap_or_else(|| "dual".to_string());

    let mut current = PhysicalPlan::Scan {
        table: from_table,
        filter: sel.body.where_clause.clone(),
    };

    if let Some(order) = sel.body.order_by.first()
        && matches!(order.direction, SortDirection::CosineDistance)
    {
        let k = sel.body.limit.ok_or(Error::UnboundedVectorSearch)?;
        current = PhysicalPlan::VectorSearch {
            table: sel
                .body
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

    if sel
        .body
        .where_clause
        .as_ref()
        .is_some_and(|w| matches!(w, Expr::InSubquery { .. }))
    {
        return Err(Error::SubqueryNotSupported);
    }

    if sel.ctes.iter().any(|c| matches!(c, Cte::MatchCte { .. }))
        && matches!(
            current,
            PhysicalPlan::VectorSearch { .. } | PhysicalPlan::Scan { .. }
        )
    {
        pipeline.push(current);
        return Ok(PhysicalPlan::Pipeline(pipeline));
    }

    if pipeline.is_empty() {
        Ok(current)
    } else {
        pipeline.push(current);
        Ok(PhysicalPlan::Pipeline(pipeline))
    }
}
