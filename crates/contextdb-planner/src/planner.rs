use crate::plan::*;
use contextdb_core::{Direction, Error, PropagationRule, Result};
use contextdb_parser::ast::{
    AstPropagationRule, Cte, Expr, FromItem, RowNumberWindow, SelectBody, SelectStatement,
    SortDirection, Statement, WindowBindingId,
};
use std::collections::{BTreeSet, HashMap};

const DEFAULT_MATCH_DEPTH: u32 = 5;
const ENGINE_MAX_BFS_DEPTH: u32 = 10;
const DEFAULT_PROPAGATION_MAX_DEPTH: u32 = 10;

pub fn plan(stmt: &Statement) -> Result<PhysicalPlan> {
    match stmt {
        Statement::CreateTable(ct) => Ok(PhysicalPlan::CreateTable(CreateTablePlan {
            name: ct.name.clone(),
            columns: ct.columns.clone(),
            unique_constraints: ct.unique_constraints.clone(),
            primary_key_columns: ct.primary_key_columns.clone(),
            composite_foreign_keys: ct.composite_foreign_keys.clone(),
            if_not_exists: ct.if_not_exists,
            immutable: ct.immutable,
            state_machine: ct.state_machine.clone(),
            dag_edge_types: ct.dag_edge_types.clone(),
            propagation_rules: extract_propagation_rules(ct)?,
            retain: ct.retain.clone(),
            sync_direction: ct.sync_direction,
            conflict_policy: ct.conflict_policy,
            history: ct.history,
            delivery_manifest_tables: ct.delivery_manifest_tables.clone(),
            edge_discard: ct.edge_discard,
            maintained_counts: ct
                .maintained_counts
                .as_ref()
                .map(map_maintained_counts_declaration)
                .transpose()?,
        })),
        Statement::AlterTable(at) => Ok(PhysicalPlan::AlterTable(AlterTablePlan {
            table: at.table.clone(),
            action: at.action.clone(),
        })),
        Statement::DropTable(dt) => Ok(PhysicalPlan::DropTable(dt.name.clone())),
        Statement::CreateIndex(ci) => {
            let mut columns = Vec::with_capacity(ci.columns.len());
            for (col, dir) in &ci.columns {
                columns.push((col.clone(), map_parser_to_core_sort_direction(*dir)?));
            }
            Ok(PhysicalPlan::CreateIndex(CreateIndexPlan {
                name: ci.name.clone(),
                table: ci.table.clone(),
                columns,
            }))
        }
        Statement::DropIndex(di) => Ok(PhysicalPlan::DropIndex(DropIndexPlan {
            name: di.name.clone(),
            table: di.table.clone(),
            if_exists: di.if_exists,
        })),
        Statement::Insert(i) => Ok(PhysicalPlan::Insert(InsertPlan {
            table: i.table.clone(),
            columns: i.columns.clone(),
            values: i.values.clone(),
            on_conflict: i.on_conflict.clone().map(Into::into),
        })),
        Statement::Purge(p) => Ok(PhysicalPlan::Purge(PurgePlan {
            selections: p
                .selections
                .iter()
                .map(|selection| ErasureSelectionPlan {
                    table: selection.table.clone(),
                    where_clause: selection.where_clause.clone(),
                })
                .collect(),
        })),
        Statement::Discard(p) => Ok(PhysicalPlan::Discard(DiscardPlan {
            selections: p
                .selections
                .iter()
                .map(|selection| ErasureSelectionPlan {
                    table: selection.table.clone(),
                    where_clause: selection.where_clause.clone(),
                })
                .collect(),
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
        Statement::SetDiskLimit(val) => Ok(PhysicalPlan::SetDiskLimit(val.clone())),
        Statement::ShowDiskLimit => Ok(PhysicalPlan::ShowDiskLimit),
        Statement::ShowContentSize => Ok(PhysicalPlan::ShowContentSize),
        Statement::SetMaintenancePollInterval(milliseconds) => {
            Ok(PhysicalPlan::SetMaintenancePollInterval(*milliseconds))
        }
        Statement::ShowMaintenancePollInterval => Ok(PhysicalPlan::ShowMaintenancePollInterval),
        Statement::SetVectorServingCache(bytes) => Ok(PhysicalPlan::SetVectorServingCache(*bytes)),
        Statement::ShowVectorServingCache => Ok(PhysicalPlan::ShowVectorServingCache),
        Statement::SetStorageCompaction(setting) => {
            Ok(PhysicalPlan::SetStorageCompaction(setting.clone()))
        }
        Statement::ShowStorageCompaction => Ok(PhysicalPlan::ShowStorageCompaction),
        Statement::ShowSyncConflictPolicy => Ok(PhysicalPlan::ShowSyncConflictPolicy),
        Statement::ShowVectorIndexes => Ok(PhysicalPlan::ShowVectorIndexes),
        Statement::DeclareTenantTablePolicy(declaration) => Ok(
            PhysicalPlan::DeclareTenantTablePolicy(DeclareTenantTablePolicyPlan {
                declaration: declaration.clone(),
            }),
        ),
        Statement::ShowTenantTablePolicy { table } => Ok(PhysicalPlan::ShowTenantTablePolicy {
            table: table.clone(),
        }),
        Statement::ShowSyncBindings => Ok(PhysicalPlan::ShowSyncBindings),
        Statement::ShowDeliveryOutcomes(query) => Ok(PhysicalPlan::ShowDeliveryOutcomes(
            ShowDeliveryOutcomesPlan {
                query: query.clone(),
            },
        )),
        Statement::ShowVectorPartitions {
            table,
            column,
            limit,
            offset,
        } => Ok(PhysicalPlan::ShowVectorPartitions {
            table: table.clone(),
            column: column.clone(),
            limit: *limit,
            offset: *offset,
        }),
        Statement::CreateSchedule { .. }
        | Statement::DropSchedule { .. }
        | Statement::CreateTrigger { .. }
        | Statement::DropTrigger { .. }
        | Statement::CreateEventType { .. }
        | Statement::CreateSink { .. }
        | Statement::CreateRoute { .. }
        | Statement::DropRoute { .. } => Ok(PhysicalPlan::Pipeline(vec![])),
        Statement::Begin | Statement::Commit | Statement::Rollback => {
            Ok(PhysicalPlan::Pipeline(vec![]))
        }
    }
}

/// Map one parsed `MAINTAIN COUNT` declaration onto the engine's own sort
/// direction. `DEFAULT` never reaches a CREATE (the parser refuses it there)
/// and clears the declaration on an ALTER, so it has no projected form.
pub fn map_maintained_counts_declaration(
    declared: &contextdb_parser::ast::MaintainedCountsDecl,
) -> Result<contextdb_core::MaintainedCounts> {
    use contextdb_parser::ast::MaintainedCountsDecl;
    Ok(match declared {
        MaintainedCountsDecl::None => contextdb_core::MaintainedCounts::None,
        MaintainedCountsDecl::AllPrefixes => contextdb_core::MaintainedCounts::AllPrefixes,
        MaintainedCountsDecl::Lists(lists) => {
            let mut mapped = Vec::with_capacity(lists.len());
            for columns in lists {
                let mut list = Vec::with_capacity(columns.len());
                for (column, direction) in columns {
                    list.push((
                        column.clone(),
                        map_parser_to_core_sort_direction(*direction)?,
                    ));
                }
                mapped.push(list);
            }
            contextdb_core::MaintainedCounts::Lists(mapped)
        }
        MaintainedCountsDecl::Default => {
            return Err(Error::PlanError(
                "MAINTAIN COUNT DEFAULT clears a declaration and has no projected form".to_string(),
            ));
        }
    })
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

/// `COUNT(*)` per group over one table. The parser admits only that shape;
/// whether the table maintains a count over these columns is decided by the
/// engine at runtime, against the declaration.
fn plan_grouped_count(
    body: &SelectBody,
    cte_env: &HashMap<String, PhysicalPlan>,
) -> Result<PhysicalPlan> {
    let refused = || Error::ParseError("GROUP BY is not supported".to_string());
    let [FromItem::Table { name, alias }] = body.from.as_slice() else {
        return Err(refused());
    };
    if cte_env.contains_key(name) || !body.joins.is_empty() {
        return Err(refused());
    }
    let group_by = body
        .group_by
        .iter()
        .map(|item| match item {
            Expr::Column(column) => Ok(column.clone()),
            _ => Err(refused()),
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(PhysicalPlan::GroupedCount {
        input: Box::new(PhysicalPlan::Scan {
            table: name.clone(),
            alias: alias.clone(),
            filter: body.where_clause.clone(),
        }),
        group_by,
        columns: body
            .columns
            .iter()
            .map(|column| ProjectColumn {
                expr: column.expr.clone(),
                alias: column.alias.clone(),
            })
            .collect(),
    })
}

fn plan_select(sel: &SelectStatement) -> Result<PhysicalPlan> {
    let mut cte_env = HashMap::new();
    let mut visible_ctes = Vec::new();

    for cte in &sel.ctes {
        match cte {
            Cte::MatchCte { name, match_clause } => {
                cte_env.insert(
                    name.clone(),
                    graph_bfs_from_match(match_clause, &visible_ctes)?,
                );
            }
            Cte::SqlCte { name, query } => {
                cte_env.insert(
                    name.clone(),
                    plan_select_body(query, &cte_env, &visible_ctes)?,
                );
            }
        }
        visible_ctes.push(cte.clone());
    }

    plan_select_body(&sel.body, &cte_env, &visible_ctes)
}

fn plan_select_body(
    body: &SelectBody,
    cte_env: &HashMap<String, PhysicalPlan>,
    visible_ctes: &[Cte],
) -> Result<PhysicalPlan> {
    if !body.group_by.is_empty() {
        return plan_grouped_count(body, cte_env);
    }
    // Work on this query scope only. CTE plans retain their own projection,
    // DISTINCT and LIMIT barriers, and their rank bindings have already been consumed.
    let mut lowered_body = body.clone();
    let body = &mut lowered_body;
    let mut window = None;
    if body.where_clause.as_ref().is_some_and(contains_window)
        || body.joins.iter().any(|join| contains_window(&join.on))
    {
        return Err(window_error("window in source predicate"));
    }
    for column in &mut body.columns {
        lower_window(&mut column.expr, &mut window)?;
    }
    for order in &mut body.order_by {
        lower_window(&mut order.expr, &mut window)?;
    }
    let graph_from = body
        .from
        .iter()
        .find(|f| matches!(f, FromItem::GraphTable { .. }));

    let mut current = if let Some(from_item) = graph_from {
        graph_plan_from_from_item(from_item, visible_ctes)?
    } else {
        let from_item = body.from.iter().find_map(|item| match item {
            FromItem::Table { name, alias } => Some((name.clone(), alias.clone())),
            FromItem::GraphTable { .. } => None,
        });

        match from_item {
            Some((from_table, from_alias)) => {
                if let Some(cte_plan) = cte_env.get(&from_table) {
                    let mut cte_plan = PhysicalPlan::MaterializeCte {
                        name: from_alias.clone().unwrap_or_else(|| from_table.clone()),
                        input: Box::new(cte_plan.clone()),
                    };
                    if body.joins.is_empty()
                        && let Some(where_clause) = &body.where_clause
                    {
                        cte_plan = PhysicalPlan::Filter {
                            input: Box::new(cte_plan),
                            predicate: where_clause.clone(),
                        };
                    }
                    cte_plan
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
                PhysicalPlan::MaterializeCte {
                    name: join.alias.clone().unwrap_or_else(|| join.table.clone()),
                    input: Box::new(cte_plan.clone()),
                }
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

    if let Some(window) = window {
        current = PhysicalPlan::WindowRank {
            input: Box::new(current),
            window,
            binding: WindowBindingId(0),
        };
    }

    let row_vector_order_count = body
        .order_by
        .iter()
        .filter(|order| expr_contains_row_vector_source(&order.expr))
        .count();
    if row_vector_order_count > 0 {
        let supported_row_vector_order = body.order_by.len() == 1
            && body.order_by.first().is_some_and(|order| {
                matches!(order.direction, SortDirection::CosineDistance)
                    && matches!(
                        &order.expr,
                        Expr::CosineDistance { right, .. }
                            if matches!(right.as_ref(), Expr::RowVectorSource { .. })
                    )
            });
        if !supported_row_vector_order {
            return Err(Error::PlanError(
                "ROW_VECTOR cosine distance must be the only ORDER BY item".to_string(),
            ));
        }
    }

    let uses_vector_search = body
        .order_by
        .first()
        .is_some_and(|order| matches!(order.direction, SortDirection::CosineDistance));

    if body.use_rank.is_some() && !uses_vector_search {
        return Err(Error::UseRankRequiresVectorOrder);
    }
    if body.use_vector.is_some() && !uses_vector_search {
        return Err(Error::UseVectorRequiresVectorOrder);
    }

    if let Some(order) = body.order_by.first()
        && matches!(order.direction, SortDirection::CosineDistance)
    {
        let k = body.limit.ok_or_else(|| {
            if body.use_rank.is_some() {
                Error::UseRankRequiresLimit
            } else {
                Error::UnboundedVectorSearch
            }
        })?;
        let vector_table = vector_base_table(&current)?.ok_or_else(|| {
            Error::PlanError("unable to resolve physical vector source table".to_string())
        })?;
        let (column, query_expr) = vector_search_parts(&order.expr)?;
        current = PhysicalPlan::VectorSearch {
            table: vector_table,
            column,
            query_expr,
            k,
            candidates: Some(Box::new(current)),
            sort_key: body.use_rank.clone(),
            search_mode: body.use_vector,
            materialized_columns: selected_row_columns(&body.columns),
        };
    }

    // Guarded by `!uses_vector_search` so the vector-similarity path
    // (`ORDER BY <col> <=> <query> [USE RANK ...]`) is untouched: it was
    // already turned into a VectorSearch/HnswSearch above and never reaches
    // these rules.
    if !body.order_by.is_empty() && !uses_vector_search {
        let keys = body
            .order_by
            .iter()
            .map(|item| {
                let expr = resolve_order_by_output_alias(&item.expr, &body.columns);
                // A plain column reference always evaluates; a function call
                // MAY (the engine's plan-time validator, `validate_sort_key`,
                // is the actual authority on which function names it
                // evaluates — e.g. `COALESCE` and the desugared `+`/`-`/`*`/
                // `/` arithmetic forms — and refuses anything else with the
                // same typed error). Anything else here cannot be evaluated
                // at all: refuse it rather than let the comparator treat
                // every row as equal and hand back a silently unsorted
                // result.
                if !matches!(
                    expr,
                    Expr::Column(_) | Expr::FunctionCall { .. } | Expr::WindowRank(_)
                ) {
                    return Err(Error::OrderByExpressionNotSupported);
                }
                Ok(SortKey {
                    expr,
                    direction: item.direction,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        current = PhysicalPlan::Sort {
            input: Box::new(current),
            keys,
        };
    }

    let is_select_star = selected_row_columns(&body.columns).is_none();
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

/// The row values an explicit SELECT list needs from its input. Vector search
/// uses this to avoid materialising a stored embedding merely because the
/// physical search node exposes the table's complete schema to its parent.
/// A wildcard deliberately returns `None`: every declared value is wanted.
fn selected_row_columns(columns: &[contextdb_parser::ast::SelectColumn]) -> Option<Vec<String>> {
    if matches!(
        columns,
        [contextdb_parser::ast::SelectColumn {
            expr: Expr::Column(contextdb_parser::ast::ColumnRef { table: None, column }),
            alias: None
        }] if column == "*"
    ) {
        return None;
    }

    let mut selected = BTreeSet::new();
    for column in columns {
        collect_row_columns(&column.expr, &mut selected);
    }
    Some(selected.into_iter().collect())
}

fn collect_row_columns(expr: &Expr, selected: &mut BTreeSet<String>) {
    match expr {
        Expr::Column(reference) => {
            selected.insert(reference.column.clone());
        }
        Expr::BinaryOp { left, right, .. } | Expr::CosineDistance { left, right } => {
            collect_row_columns(left, selected);
            collect_row_columns(right, selected);
        }
        Expr::UnaryOp { operand, .. } | Expr::IsNull { expr: operand, .. } => {
            collect_row_columns(operand, selected);
        }
        Expr::FunctionCall { args, .. } => {
            for argument in args {
                collect_row_columns(argument, selected);
            }
        }
        Expr::InList { expr, list, .. } => {
            collect_row_columns(expr, selected);
            for item in list {
                collect_row_columns(item, selected);
            }
        }
        Expr::Like { expr, pattern, .. } => {
            collect_row_columns(expr, selected);
            collect_row_columns(pattern, selected);
        }
        Expr::InSubquery { expr, .. } => collect_row_columns(expr, selected),
        Expr::Row(elements) => {
            for element in elements {
                collect_row_columns(element, selected);
            }
        }
        Expr::RowNumber(window) => {
            for column in &window.partition_by {
                selected.insert(column.column.clone());
            }
            for order in &window.order_by {
                selected.insert(order.column.column.clone());
            }
        }
        Expr::WindowRank(_)
        | Expr::RowVectorSource { .. }
        | Expr::Literal(_)
        | Expr::Parameter(_) => {}
    }
}

/// Resolve a bare `ORDER BY` name against the SELECT-list output names, as
/// standard SQL does before falling back to the input columns.
///
/// `Sort` is planned *below* `Project`, so a SELECT-list alias does not exist
/// in the sort input at all. Without this substitution `SELECT b AS x FROM t
/// ORDER BY x` looks exactly like a reference to a column that does not exist,
/// and the rows come back in storage order with no sort applied and no error.
/// A qualified name (`t.x`) names a table column, never an output alias, so it
/// is left alone.
fn resolve_order_by_output_alias(
    expr: &Expr,
    columns: &[contextdb_parser::ast::SelectColumn],
) -> Expr {
    let Expr::Column(contextdb_parser::ast::ColumnRef {
        table: None,
        column,
    }) = expr
    else {
        return expr.clone();
    };
    columns
        .iter()
        .find(|select| select.alias.as_deref() == Some(column.as_str()))
        .map_or_else(|| expr.clone(), |select| select.expr.clone())
}

fn vector_search_parts(expr: &Expr) -> Result<(String, Expr)> {
    match expr {
        Expr::CosineDistance { left, right } => match left.as_ref() {
            Expr::Column(column) => Ok((column.column.clone(), right.as_ref().clone())),
            _ => Err(Error::PlanError(
                "left side of vector distance must be a column".to_string(),
            )),
        },
        _ => Err(Error::PlanError(
            "vector search requires a cosine distance expression".to_string(),
        )),
    }
}

fn expr_contains_row_vector_source(expr: &Expr) -> bool {
    match expr {
        Expr::RowVectorSource { .. } => true,
        Expr::BinaryOp { left, right, .. } | Expr::CosineDistance { left, right } => {
            expr_contains_row_vector_source(left) || expr_contains_row_vector_source(right)
        }
        Expr::UnaryOp { operand, .. } | Expr::IsNull { expr: operand, .. } => {
            expr_contains_row_vector_source(operand)
        }
        Expr::FunctionCall { args, .. } => args.iter().any(expr_contains_row_vector_source),
        Expr::InList { expr, list, .. } => {
            expr_contains_row_vector_source(expr)
                || list.iter().any(expr_contains_row_vector_source)
        }
        Expr::Like { expr, pattern, .. } => {
            expr_contains_row_vector_source(expr) || expr_contains_row_vector_source(pattern)
        }
        Expr::InSubquery { expr, subquery, .. } => {
            expr_contains_row_vector_source(expr)
                || select_body_contains_row_vector_source(subquery)
        }
        Expr::Row(elements) => elements.iter().any(expr_contains_row_vector_source),
        Expr::RowNumber(_)
        | Expr::WindowRank(_)
        | Expr::Column(_)
        | Expr::Literal(_)
        | Expr::Parameter(_) => false,
    }
}

fn select_body_contains_row_vector_source(body: &SelectBody) -> bool {
    body.columns
        .iter()
        .any(|column| expr_contains_row_vector_source(&column.expr))
        || body
            .where_clause
            .as_ref()
            .is_some_and(expr_contains_row_vector_source)
        || body
            .order_by
            .iter()
            .any(|order| expr_contains_row_vector_source(&order.expr))
}

fn vector_base_table(plan: &PhysicalPlan) -> Result<Option<String>> {
    match plan {
        PhysicalPlan::Scan { table, .. } | PhysicalPlan::IndexScan { table, .. } => {
            Ok(Some(table.clone()))
        }
        PhysicalPlan::Filter { input, .. }
        | PhysicalPlan::Project { input, .. }
        | PhysicalPlan::Distinct { input }
        | PhysicalPlan::Sort { input, .. }
        | PhysicalPlan::Limit { input, .. }
        | PhysicalPlan::MaterializeCte { input, .. } => vector_base_table(input),
        PhysicalPlan::Join { left, right, .. } => {
            let left_table = vector_base_table(left)?;
            let right_table = vector_base_table(right)?;
            match (left_table, right_table) {
                (Some(left), Some(right)) if left == right => Ok(Some(left)),
                (Some(_), Some(_)) => Err(Error::PlanError(
                    "ambiguous physical vector source table in join".to_string(),
                )),
                (Some(table), None) | (None, Some(table)) => Ok(Some(table)),
                (None, None) => Ok(None),
            }
        }
        PhysicalPlan::Pipeline(plans) => {
            for plan in plans.iter().rev() {
                if let Some(table) = vector_base_table(plan)? {
                    return Ok(Some(table));
                }
            }
            Ok(None)
        }
        PhysicalPlan::GraphBfs { .. }
        | PhysicalPlan::CteRef { .. }
        | PhysicalPlan::Union { .. } => Ok(None),
        _ => Ok(None),
    }
}

fn graph_plan_from_from_item(from_item: &FromItem, visible_ctes: &[Cte]) -> Result<PhysicalPlan> {
    match from_item {
        FromItem::GraphTable {
            match_clause,
            columns,
            ..
        } => {
            let bfs = graph_bfs_from_match(match_clause, visible_ctes)?;
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

fn graph_bfs_from_match(
    match_clause: &contextdb_parser::ast::MatchClause,
    visible_ctes: &[Cte],
) -> Result<PhysicalPlan> {
    let steps = match_clause
        .pattern
        .edges
        .iter()
        .map(|step| {
            let max_depth = if step.max_hops == 0 {
                DEFAULT_MATCH_DEPTH
            } else {
                step.max_hops
            };
            if max_depth > ENGINE_MAX_BFS_DEPTH {
                return Err(Error::BfsDepthExceeded(max_depth));
            }

            Ok(GraphStepPlan {
                edge_types: step.edge_type.clone().map(|t| vec![t]).unwrap_or_default(),
                direction: match step.direction {
                    contextdb_parser::ast::EdgeDirection::Outgoing => Direction::Outgoing,
                    contextdb_parser::ast::EdgeDirection::Incoming => Direction::Incoming,
                    contextdb_parser::ast::EdgeDirection::Both => Direction::Both,
                },
                min_depth: step.min_hops.max(1),
                max_depth,
                target_alias: step.target.alias.clone(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    if steps.is_empty() {
        return Err(Error::PlanError(
            "MATCH must include at least one edge".into(),
        ));
    }

    Ok(PhysicalPlan::GraphBfs {
        start_alias: match_clause.pattern.start.alias.clone(),
        start_expr: Expr::Column(contextdb_parser::ast::ColumnRef {
            table: None,
            column: match_clause.pattern.start.alias.clone(),
        }),
        start_candidates: None,
        filter_ctes: visible_ctes.to_vec(),
        steps,
        filter: match_clause.where_clause.clone(),
    })
}

/// Stub bridge from parser-AST `SortDirection` (Asc/Desc/CosineDistance) to
/// core engine `SortDirection` (Asc/Desc). CosineDistance is not meaningful
/// for B-tree indexes; reject it as a parse-level error.
fn map_parser_to_core_sort_direction(
    dir: contextdb_parser::ast::SortDirection,
) -> Result<contextdb_core::SortDirection> {
    match dir {
        contextdb_parser::ast::SortDirection::Asc => Ok(contextdb_core::SortDirection::Asc),
        contextdb_parser::ast::SortDirection::Desc => Ok(contextdb_core::SortDirection::Desc),
        contextdb_parser::ast::SortDirection::CosineDistance => Err(Error::ParseError(
            "CosineDistance is not a valid CREATE INDEX direction".to_string(),
        )),
    }
}

fn window_error(form: &str) -> Error {
    Error::WindowFunctionNotSupported {
        form: form.to_string(),
    }
}

fn contains_window(expr: &Expr) -> bool {
    match expr {
        Expr::RowNumber(_) | Expr::WindowRank(_) => true,
        Expr::BinaryOp { left, right, .. } | Expr::CosineDistance { left, right } => {
            contains_window(left) || contains_window(right)
        }
        Expr::UnaryOp { operand, .. } | Expr::IsNull { expr: operand, .. } => {
            contains_window(operand)
        }
        Expr::FunctionCall { args, .. } | Expr::Row(args) => args.iter().any(contains_window),
        Expr::InList { expr, list, .. } => {
            contains_window(expr) || list.iter().any(contains_window)
        }
        Expr::Like { expr, pattern, .. } => contains_window(expr) || contains_window(pattern),
        // Nested SELECT has a separate window scope, not the parent's input.
        Expr::InSubquery { expr, .. } => contains_window(expr),
        Expr::RowVectorSource { key, .. } => contains_window(key),
        Expr::Column(_) | Expr::Literal(_) | Expr::Parameter(_) => false,
    }
}

/// A single typed binding serves repeated identical windows in this SELECT.
/// Specification equality uses parsed identifiers and defaulted directions.
fn lower_window(expr: &mut Expr, window: &mut Option<RowNumberWindow>) -> Result<()> {
    match expr {
        Expr::RowNumber(spec) => {
            if let Some(existing) = window.as_ref() {
                if existing != spec {
                    return Err(window_error("several distinct windows"));
                }
            } else {
                *window = Some(spec.clone());
            }
            *expr = Expr::WindowRank(WindowBindingId(0));
        }
        Expr::BinaryOp { left, right, .. } | Expr::CosineDistance { left, right } => {
            lower_window(left, window)?;
            lower_window(right, window)?;
        }
        Expr::UnaryOp { operand, .. } | Expr::IsNull { expr: operand, .. } => {
            lower_window(operand, window)?
        }
        Expr::FunctionCall { args, .. } | Expr::Row(args) => {
            for arg in args {
                lower_window(arg, window)?;
            }
        }
        Expr::InList { expr, list, .. } => {
            lower_window(expr, window)?;
            for item in list {
                lower_window(item, window)?;
            }
        }
        Expr::Like { expr, pattern, .. } => {
            lower_window(expr, window)?;
            lower_window(pattern, window)?;
        }
        Expr::InSubquery { expr, .. } => lower_window(expr, window)?,
        Expr::RowVectorSource { key, .. } => lower_window(key, window)?,
        Expr::WindowRank(_) | Expr::Column(_) | Expr::Literal(_) | Expr::Parameter(_) => {}
    }
    Ok(())
}
