use crate::database::{
    Database, InsertRowResult, QueryResult, QueryTrace, UpsertIntentDetails, rank_index_name,
};
use crate::rank_formula::RankFormula;
use crate::sync_types::ConflictPolicy;
use contextdb_core::*;
use contextdb_parser::ast::{
    AlterAction, BinOp, ColumnRef, Cte, DataType, Expr, Literal, SelectStatement,
    SetDiskLimitValue, SetMemoryLimitValue, SortDirection, Statement, UnaryOp,
};
use contextdb_planner::{
    DeletePlan, GraphStepPlan, InsertPlan, OnConflictPlan, PhysicalPlan, UpdatePlan, plan,
};
use roaring::RoaringTreemap;
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

pub(crate) fn execute_plan(
    db: &Database,
    plan: &PhysicalPlan,
    params: &HashMap<String, Value>,
    tx: Option<TxId>,
) -> Result<QueryResult> {
    match plan {
        PhysicalPlan::CreateTable(p) => {
            require_admin_for_create_table(db)?;
            db.check_disk_budget("CREATE TABLE")?;
            if p.name.eq_ignore_ascii_case("acl_grants")
                && p.columns.iter().any(|column| column.acl_ref.is_some())
            {
                return Err(Error::SchemaInvalid {
                    reason: "acl_grants cannot itself declare ACL-protected columns".to_string(),
                });
            }
            let expires_column = expires_column_name(&p.columns)?;
            // Auto-generate implicit indexes for PK / UNIQUE columns and
            // composite UNIQUE constraints, so constraint probes run at
            // O(log n) instead of O(n) per insert.
            let mut auto_indexes: Vec<contextdb_core::IndexDecl> = Vec::new();
            for c in &p.columns {
                if c.primary_key
                    && !matches!(
                        map_column_type(&c.data_type),
                        ColumnType::Json | ColumnType::Vector(_)
                    )
                {
                    auto_indexes.push(contextdb_core::IndexDecl {
                        name: format!("__pk_{}", c.name),
                        columns: vec![(c.name.clone(), contextdb_core::SortDirection::Asc)],
                        kind: contextdb_core::IndexKind::Auto,
                    });
                }
                if c.unique
                    && !c.primary_key
                    && !matches!(
                        map_column_type(&c.data_type),
                        ColumnType::Json | ColumnType::Vector(_)
                    )
                {
                    auto_indexes.push(contextdb_core::IndexDecl {
                        name: format!("__unique_{}", c.name),
                        columns: vec![(c.name.clone(), contextdb_core::SortDirection::Asc)],
                        kind: contextdb_core::IndexKind::Auto,
                    });
                }
            }
            for uc in &p.unique_constraints {
                // Only index composite UNIQUE constraints whose columns are
                // all B-tree indexable.
                let all_indexable = uc.iter().all(|col_name| {
                    p.columns
                        .iter()
                        .find(|c| c.name == *col_name)
                        .map(|c| {
                            !matches!(
                                map_column_type(&c.data_type),
                                ColumnType::Json | ColumnType::Vector(_)
                            )
                        })
                        .unwrap_or(false)
                });
                if !all_indexable || uc.is_empty() {
                    continue;
                }
                let name = format!("__unique_{}", uc.join("_"));
                let cols: Vec<(String, contextdb_core::SortDirection)> = uc
                    .iter()
                    .map(|c| (c.clone(), contextdb_core::SortDirection::Asc))
                    .collect();
                auto_indexes.push(contextdb_core::IndexDecl {
                    name,
                    columns: cols,
                    kind: contextdb_core::IndexKind::Auto,
                });
            }
            let mut resolved_policies = HashMap::<String, ResolvedRankPolicy>::new();
            for column in &p.columns {
                if let Some(resolved) =
                    validate_rank_policy_for_column(db, &p.name, column, &p.columns)?
                {
                    resolved_policies.insert(column.name.clone(), resolved);
                }
            }
            let meta = TableMeta {
                columns: p
                    .columns
                    .iter()
                    .map(|c| {
                        core_column_from_ast(
                            c,
                            resolved_policies
                                .get(&c.name)
                                .map(|resolved| resolved.policy.clone()),
                        )
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
                unique_constraints: p.unique_constraints.clone(),
                natural_key_column: None,
                propagation_rules: p.propagation_rules.clone(),
                default_ttl_seconds: p.retain.as_ref().map(|r| r.duration_seconds),
                sync_safe: p.retain.as_ref().is_some_and(|r| r.sync_safe),
                expires_column,
                // Auto-indexes land in `TableMeta.indexes` at CREATE TABLE
                // time with `kind == IndexKind::Auto`. The planner sees them
                // (so PK / UNIQUE point probes pick IndexScan); the user-
                // visible `.schema` render suppresses them by default but
                // keeps them in `EXPLAIN <query>` so agents can assert
                // routing programmatically.
                indexes: auto_indexes.clone(),
                composite_foreign_keys: Vec::new(),
            };
            let metadata_bytes = meta.estimated_bytes();
            db.accountant().try_allocate_for(
                metadata_bytes,
                "ddl",
                "create_table",
                "Reduce schema size or raise MEMORY_LIMIT before creating more tables.",
            )?;
            db.relational_store().create_table(&p.name, meta);
            for idx in &auto_indexes {
                db.relational_store()
                    .create_index_storage(&p.name, &idx.name, idx.columns.clone());
            }
            if let Some(table_meta) = db.table_meta(&p.name) {
                for column in &table_meta.columns {
                    db.register_vector_index_for_column(&p.name, column);
                }
            }
            for (column, resolved) in resolved_policies {
                db.register_rank_formula(&p.name, &column, resolved.formula);
            }
            if let Some(table_meta) = db.table_meta(&p.name) {
                db.persist_table_meta(&p.name, &table_meta)?;
                db.allocate_ddl_lsn(|lsn| db.log_create_table_ddl(&p.name, &table_meta, lsn))?;
            }
            db.clear_statement_cache();
            Ok(QueryResult::empty_with_affected(0))
        }
        PhysicalPlan::DropTable(name) => {
            require_admin_for_ddl(db)?;
            if let Some(block) = rank_policy_drop_table_blocker(db, name) {
                return Err(block);
            }
            let bytes_to_release = estimate_drop_table_bytes(db, name);
            db.allocate_ddl_lsn(|lsn| db.log_drop_table_ddl_and_remove_triggers(name, lsn))?;
            db.accountant().release(bytes_to_release);
            db.clear_statement_cache();
            Ok(QueryResult::empty_with_affected(0))
        }
        PhysicalPlan::AlterTable(p) => {
            require_admin_for_ddl(db)?;
            db.check_disk_budget("ALTER TABLE")?;
            let store = db.relational_store();
            let mut rewrite_vectors_after_alter = false;
            match &p.action {
                AlterAction::AddColumn(col) => {
                    if col.primary_key {
                        return Err(Error::Other(
                            "adding a primary key column via ALTER TABLE is not supported"
                                .to_string(),
                        ));
                    }
                    validate_expires_column(col)?;
                    // If the new column is flagged IMMUTABLE, refuse to add it if any
                    // existing propagation rule would write into a column of that name.
                    // This closes the DROP-then-ADD-as-flagged loophole (Gotcha 13).
                    if col.immutable
                        && let Some(existing_meta) = db.table_meta(&p.table)
                    {
                        let targets_col =
                            existing_meta
                                .propagation_rules
                                .iter()
                                .any(|rule| {
                                    match rule {
                                contextdb_core::table_meta::PropagationRule::ForeignKey {
                                    target_state,
                                    ..
                                } => *target_state == col.name,
                                contextdb_core::table_meta::PropagationRule::Edge {
                                    target_state,
                                    ..
                                } => *target_state == col.name,
                                contextdb_core::table_meta::PropagationRule::VectorExclusion {
                                    ..
                                } => false,
                            }
                                });
                        if targets_col {
                            return Err(Error::ImmutableColumn {
                                table: p.table.clone(),
                                column: col.name.clone(),
                            });
                        }
                    }
                    let mut all_columns = db
                        .table_meta(&p.table)
                        .map(|meta| {
                            meta.columns
                                .into_iter()
                                .map(ast_column_from_core)
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default();
                    all_columns.push(col.clone());
                    let resolved_policy =
                        validate_rank_policy_for_column(db, &p.table, col, &all_columns)?;
                    let core_col = core_column_from_ast(
                        col,
                        resolved_policy
                            .as_ref()
                            .map(|resolved| resolved.policy.clone()),
                    );
                    store
                        .alter_table_add_column(&p.table, core_col)
                        .map_err(Error::Other)?;
                    if let Some(table_meta) = db.table_meta(&p.table)
                        && let Some(column) = table_meta
                            .columns
                            .iter()
                            .find(|column| column.name == col.name)
                    {
                        db.register_vector_index_for_column(&p.table, column);
                    }
                    if col.expires {
                        let mut meta = store.table_meta.write();
                        let table_meta = meta.get_mut(&p.table).ok_or_else(|| {
                            Error::Other(format!("table '{}' not found", p.table))
                        })?;
                        table_meta.expires_column = Some(col.name.clone());
                    }
                    if let Some(resolved) = resolved_policy {
                        db.register_rank_formula(&p.table, &col.name, resolved.formula);
                    }
                }
                AlterAction::DropColumn {
                    column: name,
                    cascade,
                } => {
                    if let Some(block) = rank_policy_drop_column_blocker(db, &p.table, name) {
                        return Err(block);
                    }
                    let dropped_vector_column = db
                        .table_meta(&p.table)
                        .and_then(|meta| {
                            meta.columns.into_iter().find(|column| column.name == *name)
                        })
                        .is_some_and(|column| {
                            matches!(column.column_type, contextdb_core::ColumnType::Vector(_))
                        });
                    if let Some(existing_meta) = db.table_meta(&p.table)
                        && let Some(col) = existing_meta.columns.iter().find(|c| c.name == *name)
                        && col.immutable
                    {
                        return Err(Error::ImmutableColumn {
                            table: p.table.clone(),
                            column: name.clone(),
                        });
                    }
                    // PK check precedes index-dependency reporting: a column
                    // flagged PRIMARY KEY cannot be dropped (the auto-index
                    // would also show as a dependency, but the actionable
                    // error is that the PK cannot be removed).
                    if let Some(existing_meta) = db.table_meta(&p.table)
                        && let Some(col) = existing_meta.columns.iter().find(|c| c.name == *name)
                        && col.primary_key
                    {
                        return Err(Error::Other(format!(
                            "cannot drop primary key column {}.{}",
                            p.table, name
                        )));
                    }
                    // RESTRICT / CASCADE on indexed columns. Only user-declared
                    // indexes gate the RESTRICT path — auto-indexes dissolve
                    // naturally when their defining column leaves.
                    let dependent_user_indexes: Vec<String> = db
                        .table_meta(&p.table)
                        .map(|m| {
                            m.indexes
                                .iter()
                                .filter(|i| {
                                    i.kind == contextdb_core::IndexKind::UserDeclared
                                        && i.columns.iter().any(|(c, _)| c == name)
                                })
                                .map(|i| i.name.clone())
                                .collect()
                        })
                        .unwrap_or_default();
                    let dependent_indexes: Vec<String> = db
                        .table_meta(&p.table)
                        .map(|m| {
                            m.indexes
                                .iter()
                                .filter(|i| i.columns.iter().any(|(c, _)| c == name))
                                .map(|i| i.name.clone())
                                .collect()
                        })
                        .unwrap_or_default();
                    if !*cascade && !dependent_user_indexes.is_empty() {
                        return Err(Error::ColumnInIndex {
                            table: p.table.clone(),
                            column: name.clone(),
                            index: dependent_user_indexes[0].clone(),
                        });
                    }
                    store
                        .alter_table_drop_column(&p.table, name)
                        .map_err(Error::Other)?;
                    db.remove_rank_formula(&p.table, name);
                    db.deregister_vector_index(&p.table, name);
                    rewrite_vectors_after_alter = dropped_vector_column;
                    if *cascade {
                        // Remove IndexDecls referencing `name`, release storage.
                        {
                            let mut metas = store.table_meta.write();
                            if let Some(m) = metas.get_mut(&p.table) {
                                m.indexes
                                    .retain(|i| !i.columns.iter().any(|(c, _)| c == name));
                            }
                        }
                        for idx in &dependent_indexes {
                            store.drop_index_storage(&p.table, idx);
                            db.allocate_ddl_lsn(|lsn| db.log_drop_index_ddl(&p.table, idx, lsn))?;
                        }
                    }
                    let mut meta = store.table_meta.write();
                    if let Some(table_meta) = meta.get_mut(&p.table)
                        && table_meta.expires_column.as_deref() == Some(name.as_str())
                    {
                        table_meta.expires_column = None;
                    }
                    drop(meta);
                    if let Some(table_meta) = db.table_meta(&p.table) {
                        db.persist_table_meta(&p.table, &table_meta)?;
                        db.persist_table_rows(&p.table)?;
                        if rewrite_vectors_after_alter {
                            db.persist_vectors()?;
                        }
                        db.allocate_ddl_lsn(|lsn| {
                            db.log_alter_table_ddl(&p.table, &table_meta, lsn)
                        })?;
                    }
                    db.clear_statement_cache();
                    return Ok(QueryResult {
                        columns: vec![],
                        rows: vec![],
                        rows_affected: 0,
                        trace: crate::database::QueryTrace::scan(),
                        cascade: if *cascade {
                            Some(crate::database::CascadeReport {
                                dropped_indexes: dependent_indexes,
                            })
                        } else {
                            None
                        },
                    });
                }
                AlterAction::RenameColumn { from, to } => {
                    if let Some(block) = rank_policy_drop_column_blocker(db, &p.table, from) {
                        return Err(block);
                    }
                    let renamed_vector_column = db
                        .table_meta(&p.table)
                        .and_then(|meta| {
                            meta.columns.into_iter().find(|column| column.name == *from)
                        })
                        .is_some_and(|column| {
                            matches!(column.column_type, contextdb_core::ColumnType::Vector(_))
                        });
                    if let Some(existing_meta) = db.table_meta(&p.table)
                        && let Some(col) = existing_meta.columns.iter().find(|c| c.name == *from)
                        && col.immutable
                    {
                        return Err(Error::ImmutableColumn {
                            table: p.table.clone(),
                            column: from.clone(),
                        });
                    }
                    store
                        .alter_table_rename_column(&p.table, from, to)
                        .map_err(Error::Other)?;
                    if renamed_vector_column {
                        db.rename_vector_index(&p.table, from, to)?;
                        rewrite_vectors_after_alter = true;
                    }
                    let mut meta = store.table_meta.write();
                    if let Some(table_meta) = meta.get_mut(&p.table)
                        && table_meta.expires_column.as_deref() == Some(from.as_str())
                    {
                        table_meta.expires_column = Some(to.clone());
                    }
                }
                AlterAction::SetRetain {
                    duration_seconds,
                    sync_safe,
                } => {
                    let mut meta = store.table_meta.write();
                    let table_meta = meta
                        .get_mut(&p.table)
                        .ok_or_else(|| Error::Other(format!("table '{}' not found", p.table)))?;
                    if table_meta.immutable {
                        return Err(Error::Other(
                            "IMMUTABLE and RETAIN are mutually exclusive".to_string(),
                        ));
                    }
                    table_meta.default_ttl_seconds = Some(*duration_seconds);
                    table_meta.sync_safe = *sync_safe;
                }
                AlterAction::DropRetain => {
                    let mut meta = store.table_meta.write();
                    let table_meta = meta
                        .get_mut(&p.table)
                        .ok_or_else(|| Error::Other(format!("table '{}' not found", p.table)))?;
                    table_meta.default_ttl_seconds = None;
                    table_meta.sync_safe = false;
                }
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
                if rewrite_vectors_after_alter {
                    db.persist_vectors()?;
                }
                db.allocate_ddl_lsn(|lsn| db.log_alter_table_ddl(&p.table, &table_meta, lsn))?;
            }
            db.clear_statement_cache();
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
                    trace: crate::database::QueryTrace::scan(),
                    cascade: None,
                });
            }
            let snapshot = db.snapshot_for_read();
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

            // Try to route through an IndexScan if the filter is index-eligible.
            // Analyze the PRE-resolve filter so `a IN (SELECT …)` disqualifies
            // the outer IndexScan even after the subquery has been executed.
            let meta_for_indexes = db.table_meta(table);
            let indexes: Vec<contextdb_core::IndexDecl> = meta_for_indexes
                .as_ref()
                .map(|m| m.indexes.clone())
                .unwrap_or_default();
            let analysis = filter
                .as_ref()
                .filter(|_| !indexes.is_empty())
                .map(|f| analyze_filter_for_index(f, &indexes, params));

            if let Some(a) = analysis {
                if let Some(pick) = a.pick {
                    // IndexScan path. Fetch by BTree range; apply residual filter.
                    let (rows, examined) = execute_index_scan(db, table, &pick, snapshot, tx)?;
                    db.__bump_rows_examined(examined);
                    let mut result = materialize_rows(
                        rows,
                        resolved_filter.as_ref(),
                        params,
                        schema_columns.as_deref(),
                    )?;
                    let mut pushed: smallvec::SmallVec<[std::borrow::Cow<'static, str>; 4]> =
                        smallvec::SmallVec::new();
                    pushed.push(std::borrow::Cow::Owned(pick.pushed_column.clone()));
                    let considered: smallvec::SmallVec<[crate::database::IndexCandidate; 4]> = a
                        .considered
                        .iter()
                        .filter(|c| c.name != pick.name)
                        .cloned()
                        .collect();
                    result.trace = crate::database::QueryTrace {
                        physical_plan: "IndexScan",
                        index_used: Some(pick.name.clone()),
                        predicates_pushed: pushed,
                        indexes_considered: considered,
                        sort_elided: false,
                    };
                    return Ok(result);
                } else {
                    // Scan with rejection trace.
                    let rows = db.scan(table, snapshot)?;
                    db.__bump_rows_examined(rows.len() as u64);
                    let mut result = materialize_rows(
                        rows,
                        resolved_filter.as_ref(),
                        params,
                        schema_columns.as_deref(),
                    )?;
                    let considered: smallvec::SmallVec<[crate::database::IndexCandidate; 4]> =
                        a.considered.into_iter().collect();
                    result.trace = crate::database::QueryTrace {
                        physical_plan: "Scan",
                        index_used: None,
                        predicates_pushed: Default::default(),
                        indexes_considered: considered,
                        sort_elided: false,
                    };
                    return Ok(result);
                }
            }

            let rows = db.scan(table, snapshot)?;
            db.__bump_rows_examined(rows.len() as u64);
            let mut result = materialize_rows(
                rows,
                resolved_filter.as_ref(),
                params,
                schema_columns.as_deref(),
            )?;
            result.trace = crate::database::QueryTrace::scan();
            Ok(result)
        }
        PhysicalPlan::GraphBfs {
            start_alias,
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
                        let first_step = steps.first().ok_or_else(|| {
                            Error::PlanError("graph plan missing traversal step".to_string())
                        })?;
                        let edge_types_ref = if first_step.edge_types.is_empty() {
                            None
                        } else {
                            Some(first_step.edge_types.as_slice())
                        };
                        db.graph_start_nodes_for_match(
                            edge_types_ref,
                            first_step.direction,
                            db.snapshot(),
                        )?
                    }
                }
                Err(err) => return Err(err),
            };
            if start_uuids.is_empty() {
                return Ok(QueryResult {
                    columns: vec!["id".to_string(), "depth".to_string()],
                    rows: vec![],
                    rows_affected: 0,
                    trace: crate::database::QueryTrace::scan(),
                    cascade: None,
                });
            }
            let snapshot = db.snapshot();
            let mut frontier = start_uuids
                .into_iter()
                .map(|id| (HashMap::from([(start_alias.clone(), id)]), id, 0_u32))
                .collect::<Vec<_>>();
            let bfs_bytes = estimate_bfs_working_bytes(&frontier, steps);
            db.accountant().try_allocate_for(
                bfs_bytes,
                "bfs_frontier",
                "graph_bfs",
                "Reduce traversal depth/fan-out or raise MEMORY_LIMIT before running BFS.",
            )?;

            let result = (|| {
                for step in steps {
                    let edge_types_ref = if step.edge_types.is_empty() {
                        None
                    } else {
                        Some(step.edge_types.as_slice())
                    };
                    let mut next = Vec::new();

                    for (bindings, start, base_depth) in &frontier {
                        let res = if db.has_access_constraints_for_query() {
                            db.query_bfs_gated(
                                *start,
                                edge_types_ref,
                                step.direction,
                                step.min_depth,
                                step.max_depth,
                                snapshot,
                            )?
                        } else {
                            db.graph().bfs(
                                *start,
                                edge_types_ref,
                                step.direction,
                                step.min_depth,
                                step.max_depth,
                                snapshot,
                            )?
                        };
                        for node in res.nodes {
                            let total_depth = base_depth.saturating_add(node.depth);
                            let mut next_bindings = bindings.clone();
                            next_bindings.insert(step.target_alias.clone(), node.id);
                            next.push((next_bindings, node.id, total_depth));
                        }
                    }

                    frontier = dedupe_graph_frontier(next, steps);
                    if frontier.is_empty() {
                        break;
                    }
                }

                let mut columns =
                    steps
                        .iter()
                        .fold(vec![format!("{start_alias}.id")], |mut cols, step| {
                            cols.push(format!("{}.id", step.target_alias));
                            cols
                        });
                columns.push("id".to_string());
                columns.push("depth".to_string());

                Ok(QueryResult {
                    columns,
                    rows: project_graph_frontier_rows(frontier, start_alias, steps)?,
                    rows_affected: 0,
                    trace: crate::database::QueryTrace::scan(),
                    cascade: None,
                })
            })();
            db.accountant().release(bfs_bytes);

            result
        }
        PhysicalPlan::VectorSearch {
            table,
            column,
            query_expr,
            k,
            candidates,
            sort_key,
            ..
        }
        | PhysicalPlan::HnswSearch {
            table,
            column,
            query_expr,
            k,
            candidates,
            sort_key,
            ..
        } => {
            let query_vec = resolve_vector_from_expr(query_expr, params)?;
            let snapshot = db.snapshot();
            let mut candidate_trace = None;
            let candidate_bitmap = if let Some(cands_plan) = candidates {
                let qr = execute_plan(db, cands_plan, params, tx)?;
                candidate_trace = Some(qr.trace.clone());
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
                    let uuid_to_row_id = uuid_to_row_id_map(db, table, snapshot)?;
                    for row in qr.rows {
                        if let Some(Value::Uuid(uuid)) = row.get(idx)
                            && let Some(row_id) = uuid_to_row_id.get(uuid)
                        {
                            bm.insert(row_id.0);
                        }
                    }
                }
                Some(bm)
            } else {
                None
            };

            let vector_bytes = estimate_vector_search_bytes(query_vec.len(), *k as usize);
            db.accountant().try_allocate_for(
                vector_bytes,
                "vector_search",
                "search",
                "Reduce LIMIT/dimensionality or raise MEMORY_LIMIT before vector search.",
            )?;
            if let Some(sort_key) = sort_key {
                let mut semantic_query = crate::database::SemanticQuery::new(
                    table.clone(),
                    column.clone(),
                    query_vec,
                    *k as usize,
                );
                semantic_query.sort_key = Some(sort_key.clone());
                let res = db.semantic_search_with_candidates(semantic_query, candidate_bitmap);
                db.accountant().release(vector_bytes);
                let results = res?;
                let schema_columns = db.table_meta(table).map(|meta| {
                    meta.columns
                        .into_iter()
                        .map(|column| column.name)
                        .collect::<Vec<_>>()
                });
                let keys = schema_columns.unwrap_or_else(|| {
                    let mut ks = BTreeSet::new();
                    for result in &results {
                        for key in result.values.keys() {
                            ks.insert(key.clone());
                        }
                    }
                    ks.into_iter().collect()
                });
                let mut columns = vec!["row_id".to_string()];
                columns.extend(keys.iter().cloned());
                columns.push("score".to_string());
                let rows = results
                    .into_iter()
                    .map(|result| {
                        let mut out = vec![Value::Int64(result.row_id.0 as i64)];
                        for key in &keys {
                            out.push(result.values.get(key).cloned().unwrap_or(Value::Null));
                        }
                        out.push(Value::Float64(result.rank as f64));
                        out
                    })
                    .collect();
                return Ok(QueryResult {
                    columns,
                    rows,
                    rows_affected: 0,
                    trace: vector_search_trace("VectorSearch", candidate_trace),
                    cascade: None,
                });
            }
            let res = db.query_vector_strict(
                contextdb_core::VectorIndexRef::new(table.clone(), column.clone()),
                &query_vec,
                *k as usize,
                candidate_bitmap.as_ref(),
                db.snapshot(),
            );
            db.accountant().release(vector_bytes);
            let res = res?;

            // Re-materialize: look up actual rows by row_id so SELECT * returns user columns
            let result_row_ids = res.iter().map(|(rid, _)| *rid).collect::<Vec<_>>();
            let result_rows = rows_by_row_id(db, table, &result_row_ids, snapshot)?;
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
                for r in &result_rows {
                    for k in r.values.keys() {
                        ks.insert(k.clone());
                    }
                }
                ks.into_iter().collect::<Vec<_>>()
            };

            let row_map: HashMap<RowId, &VersionedRow> =
                result_rows.iter().map(|r| (r.row_id, r)).collect();

            let mut columns = vec!["row_id".to_string()];
            columns.extend(keys.iter().cloned());
            columns.push("score".to_string());

            let rows = res
                .into_iter()
                .filter_map(|(rid, score)| {
                    row_map.get(&rid).map(|row| {
                        let mut out = vec![Value::Int64(rid.0 as i64)];
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
                trace: vector_search_trace(
                    if db
                        .__debug_vector_hnsw_len(contextdb_core::VectorIndexRef::new(
                            table.clone(),
                            column.clone(),
                        ))
                        .is_some()
                    {
                        "HNSWSearch"
                    } else {
                        "VectorSearch"
                    },
                    candidate_trace,
                ),
                cascade: None,
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
                    trace: input_result.trace.clone(),
                    cascade: None,
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
                trace: input_result.trace.clone(),
                cascade: None,
            })
        }
        PhysicalPlan::Sort { input, keys } => {
            // Sort elision path A: input is a Scan and an index's direction
            // prefix matches `keys`. We rewrite the input into an IndexScan
            // and skip the re-sort.
            let elided = try_elide_sort(db, input, keys, params, tx)?;
            if let Some(mut result) = elided {
                result.trace.sort_elided = true;
                return Ok(result);
            }
            let mut input_result = execute_plan(db, input, params, tx)?;

            // Sort elision path B: the input already used an IndexScan whose
            // column list + directions prefix-match the ORDER BY keys. The
            // IndexScan already delivers rows in the requested order, so the
            // Sort is a no-op; skip it and mark `sort_elided`.
            if input_result.trace.physical_plan == "IndexScan"
                && let Some(idx_name) = &input_result.trace.index_used
                && sort_keys_match_index_prefix(db, input, idx_name, keys)
            {
                input_result.trace.sort_elided = true;
                return Ok(input_result);
            }
            input_result.rows.sort_by(|left, right| {
                for key in keys {
                    let Expr::Column(column_ref) = &key.expr else {
                        return Ordering::Equal;
                    };
                    let left_value =
                        match lookup_query_result_column(left, &input_result.columns, column_ref) {
                            Ok(value) => value,
                            Err(_) => return Ordering::Equal,
                        };
                    let right_value = match lookup_query_result_column(
                        right,
                        &input_result.columns,
                        column_ref,
                    ) {
                        Ok(value) => value,
                        Err(_) => return Ordering::Equal,
                    };
                    let ordering = compare_sort_values(&left_value, &right_value, key.direction);
                    if ordering != Ordering::Equal {
                        return ordering;
                    }
                }
                Ordering::Equal
            });
            // Preserve the child's physical_plan when it was an IndexScan:
            // the trace reports the data-source strategy, and Sort is
            // represented by `sort_elided = false` rather than overriding the
            // plan label. A plain `Scan` child gets relabeled to `Sort` to
            // match the plan's ORDER BY-without-index expectations.
            if input_result.trace.physical_plan != "IndexScan" {
                input_result.trace.physical_plan = "Sort";
            }
            input_result.trace.sort_elided = false;
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
            let mut seen = HashSet::<Vec<u8>>::new();
            let rows = input_result
                .rows
                .into_iter()
                .filter(|row| seen.insert(distinct_row_key(row)))
                .collect();
            Ok(QueryResult {
                columns: input_result.columns,
                rows,
                rows_affected: input_result.rows_affected,
                trace: input_result.trace,
                cascade: None,
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
                trace: crate::database::QueryTrace::scan(),
                cascade: None,
            })
        }
        PhysicalPlan::CreateIndex(p) => {
            require_admin_for_ddl(db)?;
            exec_create_index(db, p)
        }
        PhysicalPlan::DropIndex(p) => {
            require_admin_for_ddl(db)?;
            exec_drop_index(db, p)
        }
        PhysicalPlan::IndexScan {
            table,
            index,
            range: _,
        } => {
            // Stub: always return empty rows + an IndexScan trace marker. Impl
            // must walk the BTreeMap at the named index, apply visibility,
            // materialize rows, and populate the trace fully.
            let _ = (table, index);
            Ok(QueryResult {
                columns: vec![],
                rows: vec![],
                rows_affected: 0,
                trace: crate::database::QueryTrace {
                    physical_plan: "IndexScan",
                    index_used: None,
                    ..crate::database::QueryTrace::default()
                },
                cascade: None,
            })
        }
        PhysicalPlan::SetMemoryLimit(val) => {
            let limit = match val {
                SetMemoryLimitValue::Bytes(bytes) => Some(*bytes),
                SetMemoryLimitValue::None => None,
            };
            db.accountant().set_budget(limit)?;
            db.persist_memory_limit(limit)?;
            Ok(QueryResult::empty())
        }
        PhysicalPlan::ShowMemoryLimit => {
            let usage = db.accountant().usage();
            Ok(QueryResult {
                columns: vec![
                    "limit".to_string(),
                    "used".to_string(),
                    "available".to_string(),
                    "startup_ceiling".to_string(),
                ],
                rows: vec![vec![
                    usage
                        .limit
                        .map(|value| Value::Int64(value as i64))
                        .unwrap_or_else(|| Value::Text("none".to_string())),
                    Value::Int64(usage.used as i64),
                    usage
                        .available
                        .map(|value| Value::Int64(value as i64))
                        .unwrap_or_else(|| Value::Text("none".to_string())),
                    usage
                        .startup_ceiling
                        .map(|value| Value::Int64(value as i64))
                        .unwrap_or_else(|| Value::Text("none".to_string())),
                ]],
                rows_affected: 0,
                trace: crate::database::QueryTrace::scan(),
                cascade: None,
            })
        }
        PhysicalPlan::SetDiskLimit(val) => {
            let limit = match val {
                SetDiskLimitValue::Bytes(bytes) => Some(*bytes),
                SetDiskLimitValue::None => None,
            };
            db.set_disk_limit(limit)?;
            db.persist_disk_limit(limit)?;
            Ok(QueryResult::empty())
        }
        PhysicalPlan::ShowDiskLimit => {
            let limit = db.disk_limit();
            let used = db.disk_file_size();
            let startup_ceiling = db.disk_limit_startup_ceiling();
            Ok(QueryResult {
                columns: vec![
                    "limit".to_string(),
                    "used".to_string(),
                    "available".to_string(),
                    "startup_ceiling".to_string(),
                ],
                rows: vec![vec![
                    limit
                        .map(|value| Value::Int64(value as i64))
                        .unwrap_or_else(|| Value::Text("none".to_string())),
                    used.map(|value| Value::Int64(value as i64))
                        .unwrap_or(Value::Null),
                    match (limit, used) {
                        (Some(limit), Some(used)) => {
                            Value::Int64(limit.saturating_sub(used) as i64)
                        }
                        _ => Value::Null,
                    },
                    startup_ceiling
                        .map(|value| Value::Int64(value as i64))
                        .unwrap_or_else(|| Value::Text("none".to_string())),
                ]],
                rows_affected: 0,
                trace: crate::database::QueryTrace::scan(),
                cascade: None,
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
                trace: crate::database::QueryTrace::scan(),
                cascade: None,
            })
        }
        PhysicalPlan::ShowVectorIndexes => {
            let rows = db
                .vector_index_infos()
                .into_iter()
                .map(|info| {
                    vec![
                        Value::Text(info.index.table),
                        Value::Text(info.index.column),
                        Value::Int64(info.dimension as i64),
                        Value::Text(info.quantization.as_str().to_string()),
                        Value::Int64(info.vector_count as i64),
                        Value::Int64(info.bytes as i64),
                    ]
                })
                .collect();
            Ok(QueryResult {
                columns: vec![
                    "table".to_string(),
                    "column".to_string(),
                    "dimension".to_string(),
                    "quantization".to_string(),
                    "vector_count".to_string(),
                    "bytes".to_string(),
                ],
                rows,
                rows_affected: 0,
                trace: crate::database::QueryTrace::scan(),
                cascade: None,
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

fn require_admin_for_create_table(db: &Database) -> Result<()> {
    if db.has_context_or_principal_constraints() {
        return Err(Error::Other(
            "DDL requires an admin database handle".to_string(),
        ));
    }
    Ok(())
}

fn require_admin_for_ddl(db: &Database) -> Result<()> {
    if db.has_access_constraints_for_query() {
        return Err(Error::Other(
            "DDL requires an admin database handle".to_string(),
        ));
    }
    Ok(())
}

fn exec_insert(
    db: &Database,
    p: &InsertPlan,
    params: &HashMap<String, Value>,
    tx: Option<TxId>,
) -> Result<QueryResult> {
    db.check_disk_budget("INSERT")?;
    let txid = tx.ok_or_else(|| Error::Other("missing tx for insert".to_string()))?;

    let insert_meta = db
        .table_meta(&p.table)
        .ok_or_else(|| Error::TableNotFound(p.table.clone()))?;
    // When no column list is provided (INSERT INTO t VALUES (...)),
    // infer column names from table metadata in declaration order.
    let columns: Vec<String> = if p.columns.is_empty() {
        insert_meta.columns.iter().map(|c| c.name.clone()).collect()
    } else {
        p.columns.clone()
    };

    // Statement-scoped snapshot of the committed TxId watermark for TXID bound checks.
    let current_tx_max = Some(db.committed_watermark());
    let route_inserts_to_graph = p.table.eq_ignore_ascii_case("edges")
        || !insert_meta.dag_edge_types.is_empty()
        || has_edge_columns(&insert_meta);
    let vector_columns = vector_columns_for_meta(&insert_meta);
    let has_insert_completion = insert_meta.columns.iter().any(|column| {
        column.default.is_some()
            || (!column.nullable && matches!(column.column_type, ColumnType::TxId))
    });

    if !vector_columns.is_empty() {
        for row in &p.values {
            let mut values = HashMap::new();
            for (idx, expr) in row.iter().enumerate() {
                let col = columns
                    .get(idx)
                    .ok_or_else(|| Error::PlanError("column/value count mismatch".to_string()))?;
                let v = resolve_expr(expr, params)?;
                values.insert(
                    col.clone(),
                    coerce_insert_value_for_column_with_meta(
                        &p.table,
                        &insert_meta,
                        col,
                        v,
                        current_tx_max,
                        Some(txid),
                    )?,
                );
            }
            if has_insert_completion {
                apply_missing_column_defaults(db, &p.table, &mut values, Some(txid))?;
            }
            db.complete_insert_access_values(&p.table, &mut values)?;
            validate_vector_columns(db, &p.table, &values)?;
        }
    }

    let mut rows_affected = 0;
    for row in &p.values {
        let mut values = HashMap::new();
        for (idx, expr) in row.iter().enumerate() {
            let col = columns
                .get(idx)
                .ok_or_else(|| Error::PlanError("column/value count mismatch".to_string()))?;
            let v = resolve_expr(expr, params)?;
            values.insert(
                col.clone(),
                coerce_insert_value_for_column_with_meta(
                    &p.table,
                    &insert_meta,
                    col,
                    v,
                    current_tx_max,
                    Some(txid),
                )?,
            );
        }

        if has_insert_completion {
            apply_missing_column_defaults(db, &p.table, &mut values, Some(txid))?;
        }
        db.complete_insert_access_values(&p.table, &mut values)?;

        if !vector_columns.is_empty() {
            validate_vector_columns(db, &p.table, &values)?;
        }
        let row_bytes = estimate_row_bytes_for_meta(&values, &insert_meta, false);
        db.accountant().try_allocate_for(
            row_bytes,
            "insert",
            "row_insert",
            "Reduce row size or raise MEMORY_LIMIT before inserting more data.",
        )?;
        let checkpoint = db.write_set_checkpoint(txid)?;
        let mut vector_allocations = Vec::new();
        let graph_edge = if route_inserts_to_graph {
            match (
                values.get("source_id"),
                values.get("target_id"),
                values.get("edge_type"),
            ) {
                (
                    Some(Value::Uuid(source)),
                    Some(Value::Uuid(target)),
                    Some(Value::Text(edge_type)),
                ) => Some((*source, *target, edge_type.clone())),
                _ => None,
            }
        } else {
            None
        };
        let vector_values = vector_values_for_table(db, &p.table, &values);

        let row_id = if let Some(on_conflict) = &p.on_conflict {
            if on_conflict.columns.is_empty() {
                db.accountant().release(row_bytes);
                let _ = db.restore_write_set_checkpoint(txid, checkpoint);
                return Err(Error::Other(
                    "ON CONFLICT target must include at least one column".to_string(),
                ));
            }
            let conflict_values = match on_conflict
                .columns
                .iter()
                .map(|column| {
                    values.get(column).cloned().ok_or_else(|| {
                        Error::Other(format!("conflict column {column} not in values"))
                    })
                })
                .collect::<Result<Vec<_>>>()
            {
                Ok(values) => values,
                Err(err) => {
                    db.accountant().release(row_bytes);
                    let _ = db.restore_write_set_checkpoint(txid, checkpoint);
                    return Err(err);
                }
            };
            let existing = match db.conflict_lookup_in_tx(
                txid,
                &p.table,
                &on_conflict.columns,
                &conflict_values,
                db.snapshot(),
            ) {
                Ok(existing) => existing,
                Err(err) => {
                    db.accountant().release(row_bytes);
                    let _ = db.restore_write_set_checkpoint(txid, checkpoint);
                    return Err(err);
                }
            };
            let upsert_values = if let Some(existing_row) = existing.as_ref() {
                match apply_on_conflict_updates(
                    db,
                    &p.table,
                    values.clone(),
                    existing_row,
                    on_conflict,
                    params,
                    Some(txid),
                ) {
                    Ok(v) => v,
                    Err(err) => {
                        db.accountant().release(row_bytes);
                        let _ = db.restore_write_set_checkpoint(txid, checkpoint);
                        return Err(err);
                    }
                }
            } else {
                values.clone()
            };
            match existing {
                None => {
                    let intent_insert_values = upsert_values.clone();
                    match db.insert_row(txid, &p.table, upsert_values) {
                        Ok(row_id) => {
                            if let Err(err) = db.record_upsert_intent(
                                txid,
                                p.table.clone(),
                                row_id,
                                UpsertIntentDetails {
                                    insert_values: intent_insert_values,
                                    conflict_columns: on_conflict.columns.clone(),
                                    update_columns: on_conflict.update_columns.clone(),
                                    params: params.clone(),
                                },
                            ) {
                                db.accountant().release(row_bytes);
                                let _ = db.restore_write_set_checkpoint(txid, checkpoint);
                                return Err(err);
                            }
                            row_id
                        }
                        Err(err) => {
                            db.accountant().release(row_bytes);
                            let _ = db.restore_write_set_checkpoint(txid, checkpoint);
                            return Err(err);
                        }
                    }
                }
                Some(existing_row) => {
                    let changed = upsert_values
                        .iter()
                        .any(|(k, v)| existing_row.values.get(k) != Some(v));
                    if !changed {
                        db.accountant().release(row_bytes);
                        RowId(0)
                    } else {
                        if let Err(err) = validate_update_state_transition(
                            db,
                            &p.table,
                            &existing_row,
                            &upsert_values,
                        ) {
                            db.accountant().release(row_bytes);
                            let _ = db.restore_write_set_checkpoint(txid, checkpoint);
                            return Err(err);
                        }
                        if db.has_live_vector(existing_row.row_id, db.snapshot()) {
                            for index in vector_indexes_for_table(db, &p.table) {
                                if db
                                    .vector_store_live_entry_for_row(
                                        &index,
                                        existing_row.row_id,
                                        db.snapshot(),
                                    )
                                    .is_some()
                                    && let Err(err) =
                                        db.delete_vector(txid, index, existing_row.row_id)
                                {
                                    db.accountant().release(row_bytes);
                                    let _ = db.restore_write_set_checkpoint(txid, checkpoint);
                                    return Err(err);
                                }
                            }
                        }
                        if let Err(err) = db.delete_row(txid, &p.table, existing_row.row_id) {
                            db.accountant().release(row_bytes);
                            let _ = db.restore_write_set_checkpoint(txid, checkpoint);
                            return Err(err);
                        }
                        let row_uuid = upsert_values.get("id").and_then(Value::as_uuid).copied();
                        let new_state = db
                            .table_meta(&p.table)
                            .and_then(|meta| meta.state_machine)
                            .and_then(|sm| upsert_values.get(&sm.column))
                            .and_then(Value::as_text)
                            .map(std::borrow::ToOwned::to_owned);
                        let row_id = match db.insert_row_replacing(
                            txid,
                            &p.table,
                            upsert_values,
                            existing_row.row_id,
                        ) {
                            Ok(row_id) => row_id,
                            Err(err) => {
                                db.accountant().release(row_bytes);
                                let _ = db.restore_write_set_checkpoint(txid, checkpoint);
                                return Err(err);
                            }
                        };
                        if let (Some(uuid), Some(state)) = (row_uuid, new_state.as_deref())
                            && let Err(err) = db.propagate_state_change_if_needed(
                                txid,
                                &p.table,
                                Some(uuid),
                                Some(state),
                            )
                        {
                            db.accountant().release(row_bytes);
                            let _ = db.restore_write_set_checkpoint(txid, checkpoint);
                            return Err(err);
                        }
                        row_id
                    }
                }
            }
        } else {
            match db.insert_row_with_unique_noop(txid, &p.table, values) {
                Ok(InsertRowResult::Inserted(row_id)) => row_id,
                Ok(InsertRowResult::NoOp) => {
                    db.accountant().release(row_bytes);
                    continue;
                }
                Err(err) => {
                    db.accountant().release(row_bytes);
                    return Err(err);
                }
            }
        };

        if let Some((source, target, edge_type)) = graph_edge {
            match db.insert_edge(txid, source, target, edge_type, HashMap::new()) {
                Ok(true) => {}
                Ok(false) => {
                    let _ = db.restore_write_set_checkpoint(txid, checkpoint);
                    db.accountant().release(row_bytes);
                    continue;
                }
                Err(err) => {
                    let _ = db.restore_write_set_checkpoint(txid, checkpoint);
                    db.accountant().release(row_bytes);
                    return Err(err);
                }
            }
        }

        if row_id != RowId(0) {
            for (column, v) in &vector_values {
                let index = contextdb_core::VectorIndexRef::new(&p.table, column.clone());
                let vector_bytes = db.vector_insert_accounted_bytes(&index, v.len());
                if let Err(err) = db.insert_vector_strict(txid, index, row_id, v.clone()) {
                    let _ = db.restore_write_set_checkpoint(txid, checkpoint);
                    db.accountant().release(row_bytes);
                    release_accounted_bytes(db, &vector_allocations);
                    return Err(err);
                }
                vector_allocations.push(vector_bytes);
            }
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
    let rows = db.scan_in_tx_raw(txid, &p.table, snapshot)?;
    let rows = db.filter_rows_for_read(&p.table, rows, snapshot)?;
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
        db.assert_row_write_allowed(&p.table, row.row_id, &row.values, snapshot)?;
    }

    for row in &matched {
        for index in vector_indexes_for_table(db, &p.table) {
            if db
                .vector_store_live_entry_for_row(&index, row.row_id, snapshot)
                .is_some()
            {
                db.delete_vector(txid, index, row.row_id)?;
            }
        }
        db.delete_row(txid, &p.table, row.row_id)?;
    }

    Ok(QueryResult::empty_with_affected(matched.len() as u64))
}

fn collect_conditional_update_predicates(
    expr: &Expr,
    params: &HashMap<String, Value>,
) -> Result<Option<Vec<(String, Value)>>> {
    fn collect_into(
        expr: &Expr,
        params: &HashMap<String, Value>,
        out: &mut Vec<(String, Value)>,
    ) -> Result<bool> {
        match expr {
            Expr::BinaryOp {
                left,
                op: BinOp::And,
                right,
            } => Ok(collect_into(left, params, out)? && collect_into(right, params, out)?),
            Expr::BinaryOp {
                left,
                op: BinOp::Eq,
                right,
            } => {
                if let Expr::Column(column) = left.as_ref()
                    && matches!(right.as_ref(), Expr::Literal(_) | Expr::Parameter(_))
                {
                    out.push((column.column.clone(), resolve_expr(right, params)?));
                    return Ok(true);
                }
                if let Expr::Column(column) = right.as_ref()
                    && matches!(left.as_ref(), Expr::Literal(_) | Expr::Parameter(_))
                {
                    out.push((column.column.clone(), resolve_expr(left, params)?));
                    return Ok(true);
                }
                Ok(false)
            }
            _ => Ok(false),
        }
    }

    let mut predicates = Vec::new();
    if collect_into(expr, params, &mut predicates)?
        && predicates.iter().any(|(column, _)| column != "id")
    {
        Ok(Some(predicates))
    } else {
        Ok(None)
    }
}

fn exec_update(
    db: &Database,
    p: &UpdatePlan,
    params: &HashMap<String, Value>,
    tx: Option<TxId>,
) -> Result<QueryResult> {
    db.check_disk_budget("UPDATE")?;
    let txid = tx.ok_or_else(|| Error::Other("missing tx for update".to_string()))?;
    let snapshot = db.snapshot();
    let resolved_where = p
        .where_clause
        .as_ref()
        .map(|expr| resolve_in_subqueries(db, expr, params, tx))
        .transpose()?;
    // Use the same IndexScan candidate selection as SELECT when the UPDATE
    // predicate can narrow by an indexed first column. The residual WHERE is
    // still evaluated below, so this only reduces the candidate set.
    let rows = if let Some(where_clause) = resolved_where.as_ref() {
        let indexed_rows = db
            .table_meta(&p.table)
            .and_then(|meta| analyze_filter_for_index(where_clause, &meta.indexes, params).pick)
            .map(|pick| execute_index_scan(db, &p.table, &pick, snapshot, Some(txid)))
            .transpose()?;
        if let Some((rows, examined)) = indexed_rows {
            db.__bump_rows_examined(examined);
            rows
        } else {
            // Use in-tx scan so prior statements in a BEGIN/COMMIT block are
            // visible: the old row must not shadow a previously-updated row.
            let rows = db.scan_in_tx_raw(txid, &p.table, snapshot)?;
            db.filter_rows_for_read(&p.table, rows, snapshot)?
        }
    } else {
        let rows = db.scan_in_tx_raw(txid, &p.table, snapshot)?;
        db.filter_rows_for_read(&p.table, rows, snapshot)?
    };
    let current_tx_max = Some(db.committed_watermark());
    let conditional_predicates = resolved_where
        .as_ref()
        .map(|expr| collect_conditional_update_predicates(expr, params))
        .transpose()?
        .flatten()
        .map(|predicates| {
            predicates
                .into_iter()
                .map(|(column, value)| {
                    Ok((
                        column.clone(),
                        coerce_value_for_column(
                            db,
                            &p.table,
                            &column,
                            value,
                            current_tx_max,
                            Some(txid),
                        )?,
                    ))
                })
                .collect::<Result<Vec<_>>>()
        })
        .transpose()?;
    let matched: Vec<_> = rows
        .into_iter()
        .filter(|r| {
            resolved_where
                .as_ref()
                .is_none_or(|w| row_matches(r, w, params).unwrap_or(false))
        })
        .collect();

    struct PlannedUpdate {
        row: VersionedRow,
        values: HashMap<String, Value>,
        row_uuid: Option<uuid::Uuid>,
        new_state: Option<String>,
        assigned_vector_values: Vec<(String, Vec<f32>)>,
        assigned_vector_columns: HashSet<String>,
        conditional_predicates: Option<Vec<(String, Value)>>,
    }

    let mut planned = Vec::with_capacity(matched.len());
    for row in &matched {
        db.assert_row_write_allowed(&p.table, row.row_id, &row.values, snapshot)?;
        let mut values = row.values.clone();
        for (k, vexpr) in &p.assignments {
            let value = eval_assignment_expr(vexpr, &row.values, params)?;
            values.insert(
                k.clone(),
                coerce_value_for_column(db, &p.table, k, value, current_tx_max, Some(txid))?,
            );
        }
        let state_column_assigned = db
            .table_meta(&p.table)
            .and_then(|meta| meta.state_machine)
            .is_some_and(|sm| p.assignments.iter().any(|(column, _)| column == &sm.column));
        if state_column_assigned {
            validate_update_state_transition(db, &p.table, row, &values)?;
        }
        let row_uuid = values.get("id").and_then(Value::as_uuid).copied();
        let new_state = db
            .table_meta(&p.table)
            .as_ref()
            .and_then(|meta| meta.state_machine.as_ref())
            .and_then(|sm| values.get(&sm.column))
            .and_then(Value::as_text)
            .map(std::borrow::ToOwned::to_owned);

        validate_vector_columns(db, &p.table, &values)?;
        db.assert_row_write_allowed(&p.table, row.row_id, &values, snapshot)?;
        let assigned_vector_values: Vec<(String, Vec<f32>)> = p
            .assignments
            .iter()
            .filter_map(|(column, _)| match values.get(column) {
                Some(Value::Vector(vector)) => Some((column.clone(), vector.clone())),
                _ => None,
            })
            .collect();
        let assigned_vector_columns: HashSet<String> = assigned_vector_values
            .iter()
            .map(|(column, _)| column.clone())
            .collect();
        planned.push(PlannedUpdate {
            row: row.clone(),
            values,
            row_uuid,
            new_state,
            assigned_vector_values,
            assigned_vector_columns,
            conditional_predicates: conditional_predicates.clone(),
        });
    }

    for plan in planned {
        let row = plan.row;
        let values = plan.values;
        let row_uuid = plan.row_uuid;
        let new_state = plan.new_state;
        let assigned_vector_values = plan.assigned_vector_values;
        let assigned_vector_columns = plan.assigned_vector_columns;
        let conditional_predicates = plan.conditional_predicates;
        let new_row_bytes = estimate_table_row_bytes(db, &p.table, &values)?;
        db.accountant().try_allocate_for(
            new_row_bytes,
            "update",
            "row_replace",
            "Reduce row growth or raise MEMORY_LIMIT before updating this row.",
        )?;
        let checkpoint = db.write_set_checkpoint(txid)?;
        let before_counts = db.write_set_counts(txid)?;
        let mut vector_allocations = Vec::new();

        for column in &assigned_vector_columns {
            if let Err(err) = db.delete_vector(
                txid,
                contextdb_core::VectorIndexRef::new(&p.table, column.clone()),
                row.row_id,
            ) {
                db.accountant().release(new_row_bytes);
                let _ = db.restore_write_set_checkpoint(txid, checkpoint);
                return Err(err);
            }
        }
        if let Err(err) = db.delete_row(txid, &p.table, row.row_id) {
            db.accountant().release(new_row_bytes);
            return Err(err);
        }

        let new_row_id = match db.insert_row_replacing(txid, &p.table, values, row.row_id) {
            Ok(row_id) => row_id,
            Err(err) => {
                db.accountant().release(new_row_bytes);
                let _ = db.restore_write_set_checkpoint(txid, checkpoint);
                return Err(err);
            }
        };
        for index in vector_indexes_for_table(db, &p.table) {
            if assigned_vector_columns.contains(&index.column) {
                continue;
            }
            if let Err(err) = db.move_vector(txid, index, row.row_id, new_row_id) {
                db.accountant().release(new_row_bytes);
                let _ = db.restore_write_set_checkpoint(txid, checkpoint);
                return Err(err);
            }
        }
        for (column, vector) in assigned_vector_values {
            let index = contextdb_core::VectorIndexRef::new(&p.table, column);
            let vector_bytes = db.vector_insert_accounted_bytes(&index, vector.len());
            if let Err(err) = db.insert_vector_strict(txid, index, new_row_id, vector) {
                db.accountant().release(new_row_bytes);
                release_accounted_bytes(db, &vector_allocations);
                let _ = db.restore_write_set_checkpoint(txid, checkpoint);
                return Err(err);
            }
            vector_allocations.push(vector_bytes);
        }
        if let Err(err) =
            db.propagate_state_change_if_needed(txid, &p.table, row_uuid, new_state.as_deref())
        {
            db.accountant().release(new_row_bytes);
            release_accounted_bytes(db, &vector_allocations);
            let _ = db.restore_write_set_checkpoint(txid, checkpoint);
            return Err(err);
        }
        if let Some(predicates) = conditional_predicates {
            let after_counts = db.write_set_counts(txid)?;
            db.record_conditional_update_guard(
                txid,
                p.table.clone(),
                row.row_id,
                predicates,
                before_counts,
                after_counts,
            )?;
        }
    }

    Ok(QueryResult::empty_with_affected(matched.len() as u64))
}

fn exec_create_index(
    db: &Database,
    plan: &contextdb_planner::CreateIndexPlan,
) -> Result<QueryResult> {
    // Reserved-prefix guard: user-declared indexes must not collide with the
    // auto-index namespace used for PRIMARY KEY / UNIQUE backing indexes.
    for prefix in ["__pk_", "__unique_"] {
        if plan.name.starts_with(prefix) {
            return Err(Error::ReservedIndexName {
                table: plan.table.clone(),
                name: plan.name.clone(),
                prefix: prefix.to_string(),
            });
        }
    }

    // Error precedence (plan §Error precedence): TableNotFound > ColumnNotFound
    // > ColumnNotIndexable > DuplicateIndex. Check in that exact order so
    // "structural" bugs surface before "naming" bugs.
    let meta = db
        .table_meta(&plan.table)
        .ok_or_else(|| Error::TableNotFound(plan.table.clone()))?;

    // 2. Check every column exists.
    for (col_name, _) in &plan.columns {
        if !meta.columns.iter().any(|c| c.name == *col_name) {
            return Err(Error::ColumnNotFound {
                table: plan.table.clone(),
                column: col_name.clone(),
            });
        }
    }

    // 3. Check every column type is B-tree indexable.
    for (col_name, _) in &plan.columns {
        let col = meta
            .columns
            .iter()
            .find(|c| c.name == *col_name)
            .expect("column existence verified above");
        if matches!(col.column_type, ColumnType::Json | ColumnType::Vector(_)) {
            return Err(Error::ColumnNotIndexable {
                table: plan.table.clone(),
                column: col_name.clone(),
                column_type: col.column_type.clone(),
            });
        }
    }

    // 4. Duplicate-name check (last).
    if meta.indexes.iter().any(|i| i.name == plan.name) {
        return Err(Error::DuplicateIndex {
            table: plan.table.clone(),
            index: plan.name.clone(),
        });
    }

    // All validations passed. Persist the IndexDecl into TableMeta.indexes,
    // register the IndexStorage, rebuild it over existing rows, flush meta.
    {
        let store = db.relational_store();
        let mut metas = store.table_meta.write();
        let m = metas
            .get_mut(&plan.table)
            .ok_or_else(|| Error::TableNotFound(plan.table.clone()))?;
        m.indexes.push(contextdb_core::IndexDecl {
            name: plan.name.clone(),
            columns: plan.columns.clone(),
            kind: contextdb_core::IndexKind::UserDeclared,
        });
    }
    db.relational_store()
        .create_index_storage(&plan.table, &plan.name, plan.columns.clone());
    db.relational_store().rebuild_index(&plan.table, &plan.name);

    if let Some(table_meta) = db.table_meta(&plan.table) {
        db.persist_table_meta(&plan.table, &table_meta)?;
    }

    db.allocate_ddl_lsn(|lsn| {
        db.log_create_index_ddl(&plan.table, &plan.name, &plan.columns, lsn)
    })?;

    db.clear_statement_cache();
    Ok(QueryResult::empty_with_affected(0))
}

fn exec_drop_index(db: &Database, plan: &contextdb_planner::DropIndexPlan) -> Result<QueryResult> {
    let meta = db
        .table_meta(&plan.table)
        .ok_or_else(|| Error::TableNotFound(plan.table.clone()))?;
    let exists = meta.indexes.iter().any(|i| i.name == plan.name);
    if !exists {
        if plan.if_exists {
            return Ok(QueryResult::empty_with_affected(0));
        }
        return Err(Error::IndexNotFound {
            table: plan.table.clone(),
            index: plan.name.clone(),
        });
    }
    if let Some(block) = rank_policy_drop_index_blocker(db, &plan.table, &plan.name) {
        return Err(block);
    }
    {
        let store = db.relational_store();
        let mut metas = store.table_meta.write();
        if let Some(m) = metas.get_mut(&plan.table) {
            m.indexes.retain(|i| i.name != plan.name);
        }
    }
    db.relational_store()
        .drop_index_storage(&plan.table, &plan.name);
    if let Some(table_meta) = db.table_meta(&plan.table) {
        db.persist_table_meta(&plan.table, &table_meta)?;
    }
    db.allocate_ddl_lsn(|lsn| db.log_drop_index_ddl(&plan.table, &plan.name, lsn))?;
    db.clear_statement_cache();
    Ok(QueryResult::empty_with_affected(0))
}

fn estimate_table_row_bytes(
    db: &Database,
    table: &str,
    values: &HashMap<String, Value>,
) -> Result<usize> {
    let meta = db
        .table_meta(table)
        .ok_or_else(|| Error::TableNotFound(table.to_string()))?;
    Ok(estimate_row_bytes_for_meta(values, &meta, false))
}

// ========================= Index scan planning + execution =========================

/// Shape of a predicate on the first indexed column. Drives IndexScan
/// eligibility: equality narrows to a point, range to a range walk, IN-list
/// to multiple point lookups, IS NULL to the NULL partition.
#[derive(Debug, Clone)]
pub(crate) enum IndexPredicateShape {
    Equality(Value),
    NotEqual(Value),
    Range {
        lower: std::ops::Bound<Value>,
        upper: std::ops::Bound<Value>,
    },
    InList(Vec<Value>),
    IsNull,
    IsNotNull,
}

impl IndexPredicateShape {
    /// Selectivity tier — lower is more selective.
    fn selectivity_tier(&self) -> u8 {
        match self {
            IndexPredicateShape::Equality(_) | IndexPredicateShape::InList(_) => 0,
            IndexPredicateShape::Range { .. } | IndexPredicateShape::NotEqual(_) => 1,
            IndexPredicateShape::IsNull | IndexPredicateShape::IsNotNull => 2,
        }
    }
}

#[derive(Debug, Clone)]
struct IndexPick {
    name: String,
    columns: Vec<(String, contextdb_core::SortDirection)>,
    /// Shape on the FIRST indexed column. Only one shape drives the scan.
    shape: IndexPredicateShape,
    /// Pushed column name (engine column name) for trace.
    pushed_column: String,
}

/// Top-level decision: did we rewrite to IndexScan?
/// Carries index pick + the rejected candidates (for trace) + residual filter.
struct IndexAnalysis {
    pick: Option<IndexPick>,
    considered: Vec<crate::database::IndexCandidate>,
}

/// Coerce every literal value inside `pick.shape` to `pick.pushed_column`'s
/// declared type. B-tree walks use variant-exact comparisons by design, so a
/// SELECT `WHERE uuid_col = 'text-literal'` must arrive at the executor with
/// the text already converted to Uuid. Coercion failure propagates so callers
/// can fall back to zero-rows — matching the semantics a predicate-evaluating
/// scan would produce on an un-coercible literal.
fn coerce_pick_shape_to_column_type(
    db: &Database,
    table: &str,
    pick: &IndexPick,
) -> Result<IndexPick> {
    use std::ops::Bound;
    let col = &pick.pushed_column;
    let coerce = |v: Value| coerce_value_for_column(db, table, col, v, None, None);
    let new_shape = match &pick.shape {
        IndexPredicateShape::Equality(v) => IndexPredicateShape::Equality(coerce(v.clone())?),
        IndexPredicateShape::InList(vs) => IndexPredicateShape::InList(
            vs.iter()
                .cloned()
                .map(&coerce)
                .collect::<Result<Vec<_>>>()?,
        ),
        IndexPredicateShape::Range { lower, upper } => {
            let lower = match lower {
                Bound::Included(v) => Bound::Included(coerce(v.clone())?),
                Bound::Excluded(v) => Bound::Excluded(coerce(v.clone())?),
                Bound::Unbounded => Bound::Unbounded,
            };
            let upper = match upper {
                Bound::Included(v) => Bound::Included(coerce(v.clone())?),
                Bound::Excluded(v) => Bound::Excluded(coerce(v.clone())?),
                Bound::Unbounded => Bound::Unbounded,
            };
            IndexPredicateShape::Range { lower, upper }
        }
        IndexPredicateShape::NotEqual(v) => IndexPredicateShape::NotEqual(coerce(v.clone())?),
        IndexPredicateShape::IsNull => IndexPredicateShape::IsNull,
        IndexPredicateShape::IsNotNull => IndexPredicateShape::IsNotNull,
    };
    Ok(IndexPick {
        name: pick.name.clone(),
        columns: pick.columns.clone(),
        shape: new_shape,
        pushed_column: pick.pushed_column.clone(),
    })
}

/// Inspect `filter` looking for an eligible predicate on the first column of
/// any declared index. Returns the chosen pick + list of considered/rejected.
fn analyze_filter_for_index(
    filter: &Expr,
    indexes: &[contextdb_core::IndexDecl],
    params: &HashMap<String, Value>,
) -> IndexAnalysis {
    use std::borrow::Cow;
    let mut considered: Vec<crate::database::IndexCandidate> = Vec::new();

    // Find each conjunct (split on AND) and map to (column, shape).
    let conjuncts = split_conjuncts(filter);
    let mut conjunct_shapes: Vec<(String, IndexPredicateShape)> = Vec::new();
    for conjunct in &conjuncts {
        if let Some((col, shape)) = classify_index_predicate(conjunct, params) {
            conjunct_shapes.push((col, shape));
        }
    }

    // Annotate rejections on indexes that can't apply, for the trace.
    let mut candidates: Vec<(IndexPick, u8, usize)> = Vec::new();
    for (i_idx, decl) in indexes.iter().enumerate() {
        let first_col = match decl.columns.first() {
            Some((c, _)) => c.clone(),
            None => continue,
        };
        // Find the most-selective matching conjunct on the first column.
        let matching: Vec<&(String, IndexPredicateShape)> = conjunct_shapes
            .iter()
            .filter(|(c, _)| c == &first_col)
            .collect();
        if matching.is_empty() {
            // Check whether the filter mentions first_col in an un-usable way
            // (function call / arithmetic / col-to-col / subquery) to produce
            // a useful rejection reason.
            let reason = classify_rejection_reason(filter, &first_col);
            considered.push(crate::database::IndexCandidate {
                name: decl.name.clone(),
                rejected_reason: Cow::Borrowed(reason),
            });
            continue;
        }
        // Combine range conjuncts on the same column (BETWEEN = `>= X AND <= Y`).
        let shape = combine_shapes(matching.iter().map(|(_, s)| s.clone()).collect());
        let tier = shape.selectivity_tier();
        candidates.push((
            IndexPick {
                name: decl.name.clone(),
                columns: decl.columns.clone(),
                shape,
                pushed_column: first_col.clone(),
            },
            tier,
            i_idx,
        ));
    }

    // Selection: most-selective tier wins; tie-break by creation order (index i).
    let pick = candidates
        .into_iter()
        .min_by(|a, b| a.1.cmp(&b.1).then(a.2.cmp(&b.2)))
        .map(|(p, _, _)| p);

    IndexAnalysis { pick, considered }
}

/// Combine multiple index-shapes on the same column into the most-selective
/// composite form. Used by BETWEEN (which becomes `col >= X AND col <= Y`).
fn combine_shapes(mut shapes: Vec<IndexPredicateShape>) -> IndexPredicateShape {
    // Find the single best (most selective) shape.
    shapes.sort_by_key(|s| s.selectivity_tier());
    let head = shapes.remove(0);
    // If the head is a Range, try to merge subsequent Range conjuncts into it.
    if let IndexPredicateShape::Range {
        mut lower,
        mut upper,
    } = head.clone()
    {
        for s in shapes {
            if let IndexPredicateShape::Range { lower: l, upper: u } = s {
                // Merge lower: more restrictive is higher.
                lower = tighter_lower(&lower, &l);
                upper = tighter_upper(&upper, &u);
            }
        }
        return IndexPredicateShape::Range { lower, upper };
    }
    head
}

fn tighter_lower(a: &std::ops::Bound<Value>, b: &std::ops::Bound<Value>) -> std::ops::Bound<Value> {
    use std::ops::Bound;
    match (a, b) {
        (Bound::Unbounded, _) => b.clone(),
        (_, Bound::Unbounded) => a.clone(),
        (Bound::Included(va), Bound::Included(vb)) => {
            if compare_values(va, vb).is_some_and(|o| o == std::cmp::Ordering::Greater) {
                a.clone()
            } else {
                b.clone()
            }
        }
        (Bound::Excluded(va), Bound::Excluded(vb)) => {
            if compare_values(va, vb).is_some_and(|o| o == std::cmp::Ordering::Greater) {
                a.clone()
            } else {
                b.clone()
            }
        }
        (Bound::Included(va), Bound::Excluded(vb)) => {
            if compare_values(va, vb).is_some_and(|o| o == std::cmp::Ordering::Greater) {
                a.clone()
            } else {
                b.clone()
            }
        }
        (Bound::Excluded(va), Bound::Included(vb)) => {
            if compare_values(va, vb).is_some_and(|o| o == std::cmp::Ordering::Less) {
                b.clone()
            } else {
                a.clone()
            }
        }
    }
}

fn tighter_upper(a: &std::ops::Bound<Value>, b: &std::ops::Bound<Value>) -> std::ops::Bound<Value> {
    use std::ops::Bound;
    match (a, b) {
        (Bound::Unbounded, _) => b.clone(),
        (_, Bound::Unbounded) => a.clone(),
        (Bound::Included(va), Bound::Included(vb)) => {
            if compare_values(va, vb).is_some_and(|o| o == std::cmp::Ordering::Less) {
                a.clone()
            } else {
                b.clone()
            }
        }
        (Bound::Excluded(va), Bound::Excluded(vb)) => {
            if compare_values(va, vb).is_some_and(|o| o == std::cmp::Ordering::Less) {
                a.clone()
            } else {
                b.clone()
            }
        }
        (Bound::Included(va), Bound::Excluded(vb)) => {
            if compare_values(va, vb).is_some_and(|o| o == std::cmp::Ordering::Less) {
                a.clone()
            } else {
                b.clone()
            }
        }
        (Bound::Excluded(va), Bound::Included(vb)) => {
            if compare_values(va, vb).is_some_and(|o| o == std::cmp::Ordering::Greater) {
                b.clone()
            } else {
                a.clone()
            }
        }
    }
}

/// Split a boolean expression on top-level AND.
fn split_conjuncts(expr: &Expr) -> Vec<Expr> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            let mut out = split_conjuncts(left);
            out.extend(split_conjuncts(right));
            out
        }
        other => vec![other.clone()],
    }
}

/// Look at a predicate of the form `<col-ref> <op> <rhs>` where both sides are
/// simple. Return Some((column, shape)) if it's index-eligible, None otherwise.
fn classify_index_predicate(
    expr: &Expr,
    params: &HashMap<String, Value>,
) -> Option<(String, IndexPredicateShape)> {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            let col = extract_simple_col_ref(left)?;
            // Reject function / arithmetic / column-ref RHS / subquery.
            if !is_literal_or_param(right) {
                return None;
            }
            let rhs = resolve_simple_rhs(right, params)?;
            let shape = match op {
                BinOp::Eq => IndexPredicateShape::Equality(rhs),
                BinOp::Neq => IndexPredicateShape::NotEqual(rhs),
                BinOp::Lt => IndexPredicateShape::Range {
                    lower: std::ops::Bound::Unbounded,
                    upper: std::ops::Bound::Excluded(rhs),
                },
                BinOp::Lte => IndexPredicateShape::Range {
                    lower: std::ops::Bound::Unbounded,
                    upper: std::ops::Bound::Included(rhs),
                },
                BinOp::Gt => IndexPredicateShape::Range {
                    lower: std::ops::Bound::Excluded(rhs),
                    upper: std::ops::Bound::Unbounded,
                },
                BinOp::Gte => IndexPredicateShape::Range {
                    lower: std::ops::Bound::Included(rhs),
                    upper: std::ops::Bound::Unbounded,
                },
                _ => return None,
            };
            Some((col, shape))
        }
        Expr::InList {
            expr: e,
            list,
            negated: false,
        } => {
            let col = extract_simple_col_ref(e)?;
            let mut values = Vec::with_capacity(list.len());
            for v in list {
                if !is_literal_or_param(v) {
                    return None;
                }
                values.push(resolve_simple_rhs(v, params)?);
            }
            Some((col, IndexPredicateShape::InList(values)))
        }
        Expr::IsNull { expr: e, negated } => {
            let col = extract_simple_col_ref(e)?;
            Some((
                col,
                if *negated {
                    IndexPredicateShape::IsNotNull
                } else {
                    IndexPredicateShape::IsNull
                },
            ))
        }
        _ => None,
    }
}

/// Classify why `filter` rejected `column` for IndexScan. Returns a static
/// reason string matching the plan's trace-reason vocabulary.
fn classify_rejection_reason(filter: &Expr, column: &str) -> &'static str {
    // Walk the expression tree and find a predicate mentioning `column`; report
    // the first structural reason we detect.
    fn walk(expr: &Expr, column: &str) -> Option<&'static str> {
        match expr {
            Expr::BinaryOp {
                left,
                op: BinOp::And | BinOp::Or,
                right,
            } => walk(left, column).or_else(|| walk(right, column)),
            Expr::BinaryOp { left, op, right } => {
                // Detect arithmetic-on-column specifically (parser lowers
                // `a + 1` to FunctionCall { name: "__add", .. }).
                if expr_uses_arithmetic_on(left, column) || expr_uses_arithmetic_on(right, column) {
                    return Some("arithmetic in predicate");
                }
                // Generic function call (UPPER(col) etc.)
                if expr_uses_function_on(left, column) || expr_uses_function_on(right, column) {
                    return Some("function call in predicate");
                }
                // Column-ref RHS?
                if mentions_column_ref(left, column) || mentions_column_ref(right, column) {
                    let left_is_col = extract_simple_col_ref(left).as_deref() == Some(column);
                    let right_is_col_ref = matches!(right.as_ref(), Expr::Column(_));
                    if left_is_col && right_is_col_ref {
                        return Some("non-literal rhs");
                    }
                }
                let _ = op;
                None
            }
            Expr::Like { expr: e, .. } => {
                if mentions_column_ref(e, column) {
                    Some("LIKE is residual-only")
                } else {
                    None
                }
            }
            Expr::InSubquery { expr: e, .. } => {
                if mentions_column_ref(e, column) {
                    Some("non-literal rhs")
                } else {
                    None
                }
            }
            _ => None,
        }
    }
    walk(filter, column).unwrap_or("first column not in WHERE")
}

fn extract_simple_col_ref(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Column(r) => Some(r.column.clone()),
        _ => None,
    }
}

fn is_literal_or_param(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(_) | Expr::Parameter(_) => true,
        Expr::FunctionCall { name, args } => {
            // Arithmetic-of-literals (e.g., `0.0 / 0.0`, `1 + 2`) counts as
            // a const RHS for planning purposes; we evaluate at execute time.
            matches!(name.as_str(), "__add" | "__sub" | "__mul" | "__div")
                && args.iter().all(is_literal_or_param)
        }
        _ => false,
    }
}

fn resolve_simple_rhs(expr: &Expr, params: &HashMap<String, Value>) -> Option<Value> {
    match expr {
        Expr::Literal(lit) => Some(match lit {
            Literal::Null => Value::Null,
            Literal::Bool(b) => Value::Bool(*b),
            Literal::Integer(i) => Value::Int64(*i),
            Literal::Real(f) => Value::Float64(*f),
            Literal::Text(s) => Value::Text(s.clone()),
            Literal::Vector(_) => return None,
        }),
        Expr::Parameter(name) => params.get(name).cloned(),
        Expr::FunctionCall { name, args }
            if matches!(name.as_str(), "__add" | "__sub" | "__mul" | "__div") =>
        {
            if args.len() != 2 {
                return None;
            }
            let a = resolve_simple_rhs(&args[0], params)?;
            let b = resolve_simple_rhs(&args[1], params)?;
            match (a, b, name.as_str()) {
                (Value::Int64(x), Value::Int64(y), "__add") => {
                    Some(Value::Int64(x.wrapping_add(y)))
                }
                (Value::Int64(x), Value::Int64(y), "__sub") => {
                    Some(Value::Int64(x.wrapping_sub(y)))
                }
                (Value::Int64(x), Value::Int64(y), "__mul") => {
                    Some(Value::Int64(x.wrapping_mul(y)))
                }
                (Value::Int64(x), Value::Int64(y), "__div") if y != 0 => Some(Value::Int64(x / y)),
                (Value::Float64(x), Value::Float64(y), "__add") => Some(Value::Float64(x + y)),
                (Value::Float64(x), Value::Float64(y), "__sub") => Some(Value::Float64(x - y)),
                (Value::Float64(x), Value::Float64(y), "__mul") => Some(Value::Float64(x * y)),
                (Value::Float64(x), Value::Float64(y), "__div") => Some(Value::Float64(x / y)),
                _ => None,
            }
        }
        _ => None,
    }
}

fn expr_uses_function_on(expr: &Expr, column: &str) -> bool {
    match expr {
        Expr::FunctionCall { name, args } => {
            // Skip known arithmetic-lowering function-call names; those are
            // classified separately as "arithmetic in predicate".
            if matches!(name.as_str(), "__add" | "__sub" | "__mul" | "__div") {
                return false;
            }
            args.iter().any(|a| mentions_column_ref(a, column))
        }
        Expr::BinaryOp { left, right, .. } => {
            expr_uses_function_on(left, column) || expr_uses_function_on(right, column)
        }
        _ => false,
    }
}

fn expr_uses_arithmetic_on(expr: &Expr, column: &str) -> bool {
    // The parser lowers `a + 1`, `a - 1`, etc. into FunctionCall with reserved
    // names `__add` / `__sub` / `__mul` / `__div`. We detect that shape here.
    match expr {
        Expr::FunctionCall { name, args } => {
            matches!(name.as_str(), "__add" | "__sub" | "__mul" | "__div")
                && args.iter().any(|a| mentions_column_ref(a, column))
        }
        Expr::BinaryOp { left, right, .. } => {
            expr_uses_arithmetic_on(left, column) || expr_uses_arithmetic_on(right, column)
        }
        _ => false,
    }
}

fn mentions_column_ref(expr: &Expr, column: &str) -> bool {
    match expr {
        Expr::Column(r) => r.column == column,
        Expr::FunctionCall { args, .. } => args.iter().any(|a| mentions_column_ref(a, column)),
        Expr::BinaryOp { left, right, .. } => {
            mentions_column_ref(left, column) || mentions_column_ref(right, column)
        }
        Expr::UnaryOp { operand, .. } => mentions_column_ref(operand, column),
        Expr::IsNull { expr: e, .. } => mentions_column_ref(e, column),
        Expr::Like { expr: e, .. } => mentions_column_ref(e, column),
        Expr::InList { expr: e, .. } => mentions_column_ref(e, column),
        Expr::InSubquery { expr: e, .. } => mentions_column_ref(e, column),
        _ => false,
    }
}

/// Walk the index's B-tree per the picked shape, fetch matching rows by
/// row_id, apply residual filter, return VersionedRow list.
#[allow(clippy::too_many_arguments)]
fn execute_index_scan(
    db: &Database,
    table: &str,
    pick: &IndexPick,
    snapshot: contextdb_core::SnapshotId,
    tx: Option<TxId>,
) -> Result<(Vec<VersionedRow>, u64)> {
    use contextdb_core::{DirectedValue, SortDirection, TotalOrdAsc, TotalOrdDesc};
    use std::ops::Bound;

    // NaN equality short-circuit (I19): `col = NaN` or bound param NaN → empty.
    if let IndexPredicateShape::Equality(rhs) = &pick.shape
        && let Value::Float64(f) = rhs
        && f.is_nan()
    {
        return Ok((Vec::new(), 0));
    }
    // NULL equality short-circuit: `col = $p` with $p = NULL → empty (NULL
    // comparisons are UNKNOWN in SQL).
    if let IndexPredicateShape::Equality(Value::Null) = &pick.shape {
        return Ok((Vec::new(), 0));
    }

    // Coerce pick.shape's literal values to the pushed column's declared type.
    // A SELECT WHERE uuid_col = 'uuid-string' arrives here with Text(..) even
    // though the indexed column stores Uuid(..). B-tree walks use variant-exact
    // comparisons (value_total_cmp panics on mismatched variants by design),
    // so we must match the stored key-type before walking. Coercion failure
    // (e.g. Text that is not a valid UUID) is treated as zero rows matched —
    // same semantics a full-scan predicate would produce.
    let pick = match coerce_pick_shape_to_column_type(db, table, pick) {
        Ok(coerced) => coerced,
        Err(_) => return Ok((Vec::new(), 0)),
    };
    let pick = &pick;

    let indexes = db.relational_store().indexes.read();
    let storage = match indexes.get(&(table.to_string(), pick.name.clone())) {
        Some(s) => s,
        None => return Ok((Vec::new(), 0)),
    };

    // Build bound keys as single-column IndexKey prefixes. Composite indexes:
    // we walk the entire range for the first-col match and rely on residual
    // filter for subsequent columns.
    let first_dir = pick
        .columns
        .first()
        .map(|(_, d)| *d)
        .unwrap_or(SortDirection::Asc);

    let wrap = |v: Value| -> DirectedValue {
        match first_dir {
            SortDirection::Asc => DirectedValue::Asc(TotalOrdAsc(v)),
            SortDirection::Desc => DirectedValue::Desc(TotalOrdDesc(v)),
        }
    };

    // Collect matching postings then filter by MVCC visibility.
    let mut postings: Vec<contextdb_relational::IndexEntry> = Vec::new();
    let mut rows_examined: u64 = 0;

    let collect_range = |postings: &mut Vec<contextdb_relational::IndexEntry>,
                         examined: &mut u64,
                         lower: Bound<Vec<DirectedValue>>,
                         upper: Bound<Vec<DirectedValue>>| {
        for (_k, entries) in storage.tree.range((lower, upper)) {
            for e in entries {
                *examined += 1;
                if e.visible_at(snapshot) {
                    postings.push(e.clone());
                }
            }
        }
    };

    // For composite indexes, a first-column equality must walk ALL keys
    // whose first component equals the target (i.e. the prefix range). We
    // iterate the whole tree and filter by first-component match to cover
    // both single-column and composite shapes uniformly.
    let is_composite = pick.columns.len() > 1;

    match &pick.shape {
        IndexPredicateShape::Equality(v) => {
            if is_composite {
                // Walk every posting whose first component equals `v`.
                let want = wrap(v.clone());
                for (key, entries) in storage.tree.iter() {
                    if key.first() != Some(&want) {
                        continue;
                    }
                    for e in entries {
                        rows_examined += 1;
                        if e.visible_at(snapshot) {
                            postings.push(e.clone());
                        }
                    }
                }
            } else {
                let lower = vec![wrap(v.clone())];
                let upper = lower.clone();
                collect_range(
                    &mut postings,
                    &mut rows_examined,
                    Bound::Included(lower),
                    Bound::Included(upper),
                );
            }
        }
        IndexPredicateShape::InList(vs) => {
            for v in vs {
                if is_composite {
                    let want = wrap(v.clone());
                    for (key, entries) in storage.tree.iter() {
                        if key.first() != Some(&want) {
                            continue;
                        }
                        for e in entries {
                            rows_examined += 1;
                            if e.visible_at(snapshot) {
                                postings.push(e.clone());
                            }
                        }
                    }
                } else {
                    let k = vec![wrap(v.clone())];
                    collect_range(
                        &mut postings,
                        &mut rows_examined,
                        Bound::Included(k.clone()),
                        Bound::Included(k),
                    );
                }
            }
        }
        IndexPredicateShape::Range { lower, upper } => {
            if is_composite {
                // Composite + range on first column: walk entries whose first
                // component falls in the range.
                for (key, entries) in storage.tree.iter() {
                    let Some(first) = key.first() else { continue };
                    let in_lower = match lower {
                        Bound::Unbounded => true,
                        Bound::Included(v) => first >= &wrap(v.clone()),
                        Bound::Excluded(v) => first > &wrap(v.clone()),
                    };
                    let in_upper = match upper {
                        Bound::Unbounded => true,
                        Bound::Included(v) => first <= &wrap(v.clone()),
                        Bound::Excluded(v) => first < &wrap(v.clone()),
                    };
                    if !(in_lower && in_upper) {
                        continue;
                    }
                    for e in entries {
                        rows_examined += 1;
                        if e.visible_at(snapshot) {
                            postings.push(e.clone());
                        }
                    }
                }
            } else {
                let l = match lower {
                    Bound::Included(v) => Bound::Included(vec![wrap(v.clone())]),
                    Bound::Excluded(v) => Bound::Excluded(vec![wrap(v.clone())]),
                    Bound::Unbounded => Bound::Unbounded,
                };
                let u = match upper {
                    Bound::Included(v) => Bound::Included(vec![wrap(v.clone())]),
                    Bound::Excluded(v) => Bound::Excluded(vec![wrap(v.clone())]),
                    Bound::Unbounded => Bound::Unbounded,
                };
                collect_range(&mut postings, &mut rows_examined, l, u);
            }
        }
        IndexPredicateShape::NotEqual(v) => {
            // Full walk; skip exact key. For IndexScan-trace we still attribute
            // all postings touched to __rows_examined (trace counts postings).
            let except_key = vec![wrap(v.clone())];
            for (k, entries) in storage.tree.iter() {
                if *k == except_key {
                    continue;
                }
                for e in entries {
                    rows_examined += 1;
                    if e.visible_at(snapshot) {
                        postings.push(e.clone());
                    }
                }
            }
        }
        IndexPredicateShape::IsNull => {
            let k = vec![wrap(Value::Null)];
            collect_range(
                &mut postings,
                &mut rows_examined,
                Bound::Included(k.clone()),
                Bound::Included(k),
            );
        }
        IndexPredicateShape::IsNotNull => {
            // Everything except NULL partition.
            let null_key = vec![wrap(Value::Null)];
            for (k, entries) in storage.tree.iter() {
                if *k == null_key {
                    continue;
                }
                for e in entries {
                    rows_examined += 1;
                    if e.visible_at(snapshot) {
                        postings.push(e.clone());
                    }
                }
            }
        }
    }

    // Now fetch base rows by row_id while preserving index-order. The index
    // already enumerates postings in index sort order; rows[] preserve it.
    drop(indexes);
    let row_ids: Vec<RowId> = postings.iter().map(|p| p.row_id).collect();
    let mut out: Vec<VersionedRow> = Vec::with_capacity(row_ids.len());
    if !row_ids.is_empty() {
        let tables = db.relational_store().tables.read();
        if let Some(rows) = tables.get(table) {
            let visible_by_id: HashMap<RowId, &VersionedRow> = rows
                .iter()
                .filter(|row| row.visible_at(snapshot))
                .map(|row| (row.row_id, row))
                .collect();
            for rid in &row_ids {
                if let Some(r) = visible_by_id.get(rid) {
                    out.push((**r).clone());
                }
            }
        }
        drop(tables);
    }
    // Layer tx-scoped inserts / deletes on top, matching the semantics of
    // scan_with_tx.
    if let Some(tx_id) = tx {
        let overlay = db.index_scan_tx_overlay(tx_id, table, &pick.pushed_column, &pick.shape)?;
        let deleted_row_ids = overlay.deleted_row_ids;
        out.retain(|row| !deleted_row_ids.contains(&row.row_id));
        out.extend(overlay.matching_inserts);
    }
    out = db.filter_rows_for_read(table, out, snapshot)?;
    Ok((out, rows_examined))
}

pub(crate) fn range_includes(
    v: &Value,
    lower: &std::ops::Bound<Value>,
    upper: &std::ops::Bound<Value>,
) -> bool {
    use std::ops::Bound;
    let ok_lower = match lower {
        Bound::Unbounded => true,
        Bound::Included(b) => compare_values(v, b).is_some_and(|o| o != std::cmp::Ordering::Less),
        Bound::Excluded(b) => {
            compare_values(v, b).is_some_and(|o| o == std::cmp::Ordering::Greater)
        }
    };
    let ok_upper = match upper {
        Bound::Unbounded => true,
        Bound::Included(b) => {
            compare_values(v, b).is_some_and(|o| o != std::cmp::Ordering::Greater)
        }
        Bound::Excluded(b) => compare_values(v, b).is_some_and(|o| o == std::cmp::Ordering::Less),
    };
    ok_lower && ok_upper
}

/// Try to elide the `Sort` node when the child's Scan can be rewritten as an
/// IndexScan whose ordering matches `keys`. The common case with no WHERE
/// filter uses a full-range index walk. If the Scan has a WHERE filter that
/// does NOT match this specific index's first column, we refuse to elide
/// (the Scan arm will still pick the best-matching index for the filter and
/// the Sort arm's path-B check handles the elision).
fn try_elide_sort(
    db: &Database,
    input: &PhysicalPlan,
    keys: &[contextdb_planner::SortKey],
    params: &HashMap<String, Value>,
    tx: Option<TxId>,
) -> Result<Option<QueryResult>> {
    fn find_scan(plan: &PhysicalPlan) -> Option<(&String, &Option<String>, &Option<Expr>)> {
        match plan {
            PhysicalPlan::Scan {
                table,
                alias,
                filter,
            } => Some((table, alias, filter)),
            PhysicalPlan::Project { input, .. }
            | PhysicalPlan::Filter { input, .. }
            | PhysicalPlan::Distinct { input }
            | PhysicalPlan::Limit { input, .. } => find_scan(input),
            _ => None,
        }
    }
    let Some((table, _alias, filter)) = find_scan(input) else {
        return Ok(None);
    };
    // If the underlying Scan has a WHERE, route through the Scan executor
    // path so it gets the narrow range / correct rows_examined accounting.
    // Path B on the Sort arm will detect the IndexScan trace + matching
    // prefix and flip `sort_elided` on the result.
    if filter.is_some() {
        return Ok(None);
    }
    // Keys must all be simple column references.
    let key_cols: Option<Vec<(&str, &contextdb_parser::ast::SortDirection)>> = keys
        .iter()
        .map(|k| match &k.expr {
            Expr::Column(r) => Some((r.column.as_str(), &k.direction)),
            _ => None,
        })
        .collect();
    let Some(key_cols) = key_cols else {
        return Ok(None);
    };
    let meta = match db.table_meta(table) {
        Some(m) => m,
        None => return Ok(None),
    };
    let matching_index = meta.indexes.iter().find(|decl| {
        if decl.columns.len() < key_cols.len() {
            return false;
        }
        decl.columns
            .iter()
            .zip(key_cols.iter())
            .all(|((col, dir), (kcol, kdir))| col == kcol && core_dir_matches_ast(*dir, **kdir))
    });
    let Some(matching) = matching_index else {
        return Ok(None);
    };
    run_index_scan_with_order(db, table, matching, filter.as_ref(), params, tx)
}

/// Execute an IndexScan over `table` with `index`, applying the optional
/// residual filter. Constructs predicates_pushed / indexes_considered the
/// same way the Scan arm does.
fn run_index_scan_with_order(
    db: &Database,
    table: &str,
    decl: &contextdb_core::IndexDecl,
    filter: Option<&Expr>,
    params: &HashMap<String, Value>,
    tx: Option<TxId>,
) -> Result<Option<QueryResult>> {
    use std::borrow::Cow;
    let snapshot = db.snapshot_for_read();
    let schema_columns = db.table_meta(table).map(|meta| {
        meta.columns
            .into_iter()
            .map(|column| column.name)
            .collect::<Vec<_>>()
    });
    let resolved_filter = filter
        .map(|expr| resolve_in_subqueries(db, expr, params, tx))
        .transpose()?;

    // Pick: full-range walk for ORDER BY elision. Shape is "unbounded range"
    // so we walk every posting. Residual filter applies.
    let pick = IndexPick {
        name: decl.name.clone(),
        columns: decl.columns.clone(),
        shape: IndexPredicateShape::Range {
            lower: std::ops::Bound::Unbounded,
            upper: std::ops::Bound::Unbounded,
        },
        pushed_column: decl.columns[0].0.clone(),
    };
    let (rows, examined) = execute_index_scan(db, table, &pick, snapshot, tx)?;
    db.__bump_rows_examined(examined);
    let mut result = materialize_rows(
        rows,
        resolved_filter.as_ref(),
        params,
        schema_columns.as_deref(),
    )?;
    let mut pushed: smallvec::SmallVec<[Cow<'static, str>; 4]> = smallvec::SmallVec::new();
    pushed.push(Cow::Owned(decl.columns[0].0.clone()));
    result.trace = crate::database::QueryTrace {
        physical_plan: "IndexScan",
        index_used: Some(decl.name.clone()),
        predicates_pushed: pushed,
        indexes_considered: Default::default(),
        sort_elided: true,
    };
    Ok(Some(result))
}

fn sort_keys_match_index_prefix(
    db: &Database,
    input: &PhysicalPlan,
    index_name: &str,
    keys: &[contextdb_planner::SortKey],
) -> bool {
    fn find_scan_and_filter(plan: &PhysicalPlan) -> Option<(&String, &Option<Expr>)> {
        match plan {
            PhysicalPlan::Scan { table, filter, .. } => Some((table, filter)),
            PhysicalPlan::Project { input, .. }
            | PhysicalPlan::Filter { input, .. }
            | PhysicalPlan::Distinct { input }
            | PhysicalPlan::Limit { input, .. } => find_scan_and_filter(input),
            _ => None,
        }
    }
    let (table, filter) = match find_scan_and_filter(input) {
        Some(t) => t,
        None => return false,
    };
    let meta = match db.table_meta(table) {
        Some(m) => m,
        None => return false,
    };
    let decl = meta.indexes.iter().find(|i| i.name == index_name);
    let Some(decl) = decl else {
        return false;
    };
    // Shape guard: IndexScan with InList or NotEqual shape on the leading
    // indexed column walks fragmented posting-list ranges, so rows are
    // emitted per-value, not globally sorted. Refuse sort elision for those
    // shapes — the Sort node must run.
    if let Some(filter_expr) = filter.as_ref()
        && let Some(leading_col) = decl.columns.first().map(|(c, _)| c.as_str())
    {
        let conjuncts = split_conjuncts(filter_expr);
        let empty_params = HashMap::new();
        for conjunct in &conjuncts {
            if let Some((col, shape)) = classify_index_predicate(conjunct, &empty_params)
                && col == leading_col
                && matches!(
                    shape,
                    IndexPredicateShape::InList(_) | IndexPredicateShape::NotEqual(_)
                )
            {
                return false;
            }
        }
    }
    // Determine how many leading index columns the WHERE filter pins to a
    // single equality. Those columns are effectively "used up" by the
    // IndexScan's range; subsequent ORDER BY keys matching the remaining
    // index columns still elide the Sort.
    let pinned_prefix_len = count_equality_prefix(filter.as_ref(), &decl.columns);
    let remaining_index_cols = &decl.columns[pinned_prefix_len..];
    if remaining_index_cols.len() < keys.len() {
        return false;
    }
    remaining_index_cols
        .iter()
        .zip(keys.iter())
        .all(|((col, dir), k)| match &k.expr {
            Expr::Column(r) => r.column == *col && core_dir_matches_ast(*dir, k.direction),
            _ => false,
        })
}

fn count_equality_prefix(
    filter: Option<&Expr>,
    columns: &[(String, contextdb_core::SortDirection)],
) -> usize {
    let Some(filter) = filter else {
        return 0;
    };
    let conjuncts = split_conjuncts(filter);
    let mut pinned = 0usize;
    for (col, _) in columns {
        let has_eq = conjuncts.iter().any(|c| match c {
            Expr::BinaryOp {
                left,
                op: BinOp::Eq,
                right,
            } => {
                let left_is_col = matches!(left.as_ref(), Expr::Column(r) if r.column == *col);
                let right_is_simple =
                    matches!(right.as_ref(), Expr::Literal(_) | Expr::Parameter(_));
                left_is_col && right_is_simple
            }
            _ => false,
        });
        if has_eq {
            pinned += 1;
        } else {
            break;
        }
    }
    pinned
}

fn core_dir_matches_ast(
    core: contextdb_core::SortDirection,
    ast: contextdb_parser::ast::SortDirection,
) -> bool {
    matches!(
        (core, ast),
        (
            contextdb_core::SortDirection::Asc,
            contextdb_parser::ast::SortDirection::Asc
        ) | (
            contextdb_core::SortDirection::Desc,
            contextdb_parser::ast::SortDirection::Desc
        )
    )
}

// ========================= End of index scan planning =========================

fn validate_update_state_transition(
    db: &Database,
    table: &str,
    existing: &VersionedRow,
    next_values: &HashMap<String, Value>,
) -> Result<()> {
    let Some(meta) = db.table_meta(table) else {
        return Ok(());
    };
    let Some(state_machine) = meta.state_machine else {
        return Ok(());
    };

    let old_state = existing
        .values
        .get(&state_machine.column)
        .and_then(Value::as_text);
    let new_state = next_values
        .get(&state_machine.column)
        .and_then(Value::as_text);

    let (Some(old_state), Some(new_state)) = (old_state, new_state) else {
        return Ok(());
    };

    if db.relational_store().validate_state_transition(
        table,
        &state_machine.column,
        old_state,
        new_state,
    ) {
        return Ok(());
    }

    Err(Error::InvalidStateTransition(format!(
        "{old_state} -> {new_state}"
    )))
}

fn estimate_row_bytes_for_meta(
    values: &HashMap<String, Value>,
    meta: &TableMeta,
    include_vectors: bool,
) -> usize {
    let mut bytes = 96usize;
    for column in &meta.columns {
        let Some(value) = values.get(&column.name) else {
            continue;
        };
        if !include_vectors && matches!(column.column_type, ColumnType::Vector(_)) {
            continue;
        }
        bytes = bytes.saturating_add(32 + column.name.len() * 8 + value.estimated_bytes());
    }
    bytes
}

fn estimate_vector_search_bytes(dimension: usize, k: usize) -> usize {
    k.saturating_mul(3)
        .saturating_mul(dimension)
        .saturating_mul(std::mem::size_of::<f32>())
}

fn estimate_bfs_working_bytes<T>(
    frontier: &[T],
    steps: &[contextdb_planner::GraphStepPlan],
) -> usize {
    let max_hops = steps.iter().fold(0usize, |acc, step| {
        acc.saturating_add(step.max_depth as usize)
    });
    frontier
        .len()
        .saturating_mul(2048)
        .saturating_mul(max_hops.max(1))
}

fn dedupe_graph_frontier(
    frontier: Vec<(HashMap<String, uuid::Uuid>, uuid::Uuid, u32)>,
    steps: &[contextdb_planner::GraphStepPlan],
) -> Vec<(HashMap<String, uuid::Uuid>, uuid::Uuid, u32)> {
    let mut best =
        HashMap::<Vec<uuid::Uuid>, (HashMap<String, uuid::Uuid>, uuid::Uuid, u32)>::new();

    for (bindings, current_id, depth) in frontier {
        let mut key = Vec::with_capacity(steps.len());
        for step in steps {
            if let Some(id) = bindings.get(&step.target_alias) {
                key.push(*id);
            }
        }

        best.entry(key)
            .and_modify(|existing| {
                if depth < existing.2 {
                    *existing = (bindings.clone(), current_id, depth);
                }
            })
            .or_insert((bindings, current_id, depth));
    }

    best.into_values().collect()
}

pub(crate) fn estimate_drop_table_bytes(db: &Database, table: &str) -> usize {
    let meta = db.table_meta(table);
    let metadata_bytes = meta.as_ref().map(TableMeta::estimated_bytes).unwrap_or(0);
    let snapshot = db.snapshot();
    let rows = db.scan(table, snapshot).unwrap_or_default();
    let row_bytes = rows.iter().fold(0usize, |acc, row| {
        acc.saturating_add(meta.as_ref().map_or_else(
            || row.estimated_bytes(),
            |meta| estimate_row_bytes_for_meta(&row.values, meta, false),
        ))
    });
    let vector_bytes = rows
        .iter()
        .filter_map(|row| db.live_vector_entry(row.row_id, snapshot))
        .fold(0usize, |acc, entry| {
            acc.saturating_add(entry.estimated_bytes())
        });
    let edge_bytes = rows.iter().fold(0usize, |acc, row| {
        match (
            row.values.get("source_id").and_then(Value::as_uuid),
            row.values.get("target_id").and_then(Value::as_uuid),
            row.values.get("edge_type").and_then(Value::as_text),
        ) {
            (Some(_), Some(_), Some(edge_type)) => acc.saturating_add(
                96 + edge_type.len().saturating_mul(16) + estimate_row_value_bytes(&HashMap::new()),
            ),
            _ => acc,
        }
    });
    metadata_bytes
        .saturating_add(row_bytes)
        .saturating_add(vector_bytes)
        .saturating_add(edge_bytes)
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
            let mut out = vec![Value::Int64(r.row_id.0 as i64)];
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
        trace: crate::database::QueryTrace::scan(),
        cascade: None,
    })
}

fn rows_by_row_id(
    db: &Database,
    table: &str,
    row_ids: &[RowId],
    snapshot: SnapshotId,
) -> Result<Vec<VersionedRow>> {
    if row_ids.is_empty() {
        return Ok(Vec::new());
    }

    let wanted = row_ids.iter().copied().collect::<HashSet<_>>();
    let tables = db.relational_store().tables.read();
    let rows = tables
        .get(table)
        .ok_or_else(|| Error::TableNotFound(table.to_string()))?;
    let mut found = HashMap::with_capacity(wanted.len());
    for row in rows {
        if wanted.contains(&row.row_id) && row.visible_at(snapshot) {
            found.insert(row.row_id, row.clone());
            if found.len() == wanted.len() {
                break;
            }
        }
    }

    Ok(row_ids
        .iter()
        .filter_map(|row_id| found.remove(row_id))
        .collect())
}

fn uuid_to_row_id_map(
    db: &Database,
    table: &str,
    snapshot: SnapshotId,
) -> Result<HashMap<uuid::Uuid, RowId>> {
    let tables = db.relational_store().tables.read();
    let rows = tables
        .get(table)
        .ok_or_else(|| Error::TableNotFound(table.to_string()))?;
    Ok(rows
        .iter()
        .filter(|row| row.visible_at(snapshot))
        .filter_map(|row| match row.values.get("id") {
            Some(Value::Uuid(uuid)) => Some((*uuid, row.row_id)),
            _ => None,
        })
        .collect())
}

fn vector_search_trace(operator: &'static str, candidate_trace: Option<QueryTrace>) -> QueryTrace {
    let Some(mut trace) = candidate_trace else {
        return QueryTrace {
            physical_plan: operator,
            ..Default::default()
        };
    };

    trace.physical_plan = match (trace.physical_plan, operator) {
        ("IndexScan", "HNSWSearch") => "IndexScan -> HNSWSearch",
        ("IndexScan", _) => "IndexScan -> VectorSearch",
        ("Scan", "HNSWSearch") => "Scan -> HNSWSearch",
        ("Scan", _) => "Scan -> VectorSearch",
        (_, "HNSWSearch") => "HNSWSearch",
        _ => "VectorSearch",
    };
    trace.sort_elided = false;
    trace
}

pub(crate) fn row_matches(
    row: &VersionedRow,
    expr: &Expr,
    params: &HashMap<String, Value>,
) -> Result<bool> {
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
                Ok(Value::Int64(row.row_id.0 as i64))
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
        (Value::TxId(a), Value::TxId(b)) => Some(a.0.cmp(&b.0)),
        (Value::TxId(a), Value::Int64(b)) => {
            if *b < 0 {
                Some(Ordering::Greater)
            } else {
                Some(a.0.cmp(&(*b as u64)))
            }
        }
        (Value::Int64(a), Value::TxId(b)) => {
            if *a < 0 {
                Some(Ordering::Less)
            } else {
                Some((*a as u64).cmp(&b.0))
            }
        }
        (Value::TxId(_), Value::Timestamp(_)) | (Value::Timestamp(_), Value::TxId(_)) => None,
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

pub(crate) fn apply_on_conflict_updates(
    db: &Database,
    table: &str,
    mut insert_values: HashMap<String, Value>,
    existing_row: &VersionedRow,
    on_conflict: &OnConflictPlan,
    params: &HashMap<String, Value>,
    active_tx: Option<TxId>,
) -> Result<HashMap<String, Value>> {
    if on_conflict.update_columns.is_empty() {
        return Ok(insert_values);
    }

    if db.table_meta(table).is_some_and(|meta| meta.immutable) {
        return Err(Error::ImmutableTable(table.to_string()));
    }

    // Reject column-level IMMUTABLE updates at the ON CONFLICT DO UPDATE merge
    // point. First flagged column in update-list order wins. Rejection returns
    // Err here; the caller (exec_insert) is responsible for releasing any
    // allocator bytes and restoring the write-set checkpoint.
    if let Some(meta) = db.table_meta(table) {
        for (column, _) in &on_conflict.update_columns {
            if let Some(col_def) = meta.columns.iter().find(|c| c.name == *column)
                && col_def.immutable
            {
                return Err(Error::ImmutableColumn {
                    table: table.to_string(),
                    column: column.clone(),
                });
            }
        }
    }

    let current_tx_max = Some(db.committed_watermark());

    let mut merged = existing_row.values.clone();
    for (column, expr) in &on_conflict.update_columns {
        let value = eval_assignment_expr(expr, &existing_row.values, params)?;
        merged.insert(
            column.clone(),
            coerce_value_for_column(db, table, column, value, current_tx_max, active_tx)?,
        );
    }

    for (column, value) in insert_values.drain() {
        merged.entry(column).or_insert(value);
    }

    Ok(merged)
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
        // Prioritize qualified match (e.g., "e.id") over unqualified (e.g., "id")
        // to avoid picking the wrong table's column in JOINs.
        let idx = input_columns
            .iter()
            .position(|name| name == &qualified)
            .or_else(|| {
                input_columns
                    .iter()
                    .position(|name| name == &column_ref.column)
            })
            .ok_or_else(|| Error::PlanError(format!("project column not found: {}", qualified)))?;
        return Ok(row.get(idx).cloned().unwrap_or(Value::Null));
    }

    let matches = input_columns
        .iter()
        .enumerate()
        .filter_map(|(idx, name)| {
            (name == &column_ref.column
                || name.rsplit('.').next() == Some(column_ref.column.as_str()))
            .then_some(idx)
        })
        .collect::<Vec<_>>();

    match matches.as_slice() {
        [] => Err(Error::PlanError(format!(
            "project column not found: {}",
            column_ref.column
        ))),
        [idx] => Ok(row.get(*idx).cloned().unwrap_or(Value::Null)),
        _ => Err(Error::PlanError(format!(
            "ambiguous column reference: {}",
            column_ref.column
        ))),
    }
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

fn distinct_row_key(row: &[Value]) -> Vec<u8> {
    bincode::serde::encode_to_vec(row, bincode::config::standard())
        .expect("query rows should serialize for DISTINCT")
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
                Value::Text(text) => uuid::Uuid::parse_str(&text).map_err(|_| {
                    Error::PlanError(format!("invalid UUID in graph filter: {text}"))
                })?,
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
                    Value::Text(text) => uuid::Uuid::parse_str(&text).map_err(|_| {
                        Error::PlanError(format!("invalid UUID in graph filter: {text}"))
                    }),
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
        Value::Text(text) if text.trim_start().starts_with('[') => parse_text_vector_literal(&text),
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
                return Err(vector_dimension_error(table, &column.name, expected, got));
            }
        }
    }

    Ok(())
}

fn vector_columns_for_meta(meta: &TableMeta) -> Vec<String> {
    meta.columns
        .iter()
        .filter(|column| matches!(column.column_type, contextdb_core::ColumnType::Vector(_)))
        .map(|column| column.name.clone())
        .collect()
}

fn has_edge_columns(meta: &TableMeta) -> bool {
    ["source_id", "target_id", "edge_type"]
        .into_iter()
        .all(|name| meta.columns.iter().any(|column| column.name == name))
}

fn vector_values_for_table(
    db: &Database,
    table: &str,
    values: &HashMap<String, Value>,
) -> Vec<(String, Vec<f32>)> {
    db.table_meta(table)
        .map(|meta| {
            meta.columns
                .iter()
                .filter_map(|column| match column.column_type {
                    contextdb_core::ColumnType::Vector(_) => match values.get(&column.name) {
                        Some(Value::Vector(vector)) => Some((column.name.clone(), vector.clone())),
                        _ => None,
                    },
                    _ => None,
                })
                .collect()
        })
        .unwrap_or_default()
}

fn vector_indexes_for_table(db: &Database, table: &str) -> Vec<contextdb_core::VectorIndexRef> {
    db.table_meta(table)
        .map(|meta| {
            meta.columns
                .iter()
                .filter(|column| {
                    matches!(column.column_type, contextdb_core::ColumnType::Vector(_))
                })
                .map(|column| contextdb_core::VectorIndexRef::new(table, column.name.clone()))
                .collect()
        })
        .unwrap_or_default()
}

pub(crate) fn coerce_into_column(
    db: &Database,
    table: &str,
    col: &str,
    v: Value,
    current_tx_max: Option<TxId>,
    active_tx: Option<TxId>,
) -> Result<Value> {
    coerce_value_for_column(db, table, col, v, current_tx_max, active_tx)
}

pub(crate) fn coerce_into_column_with_meta(
    table: &str,
    meta: &TableMeta,
    col: &str,
    v: Value,
    current_tx_max: Option<TxId>,
    active_tx: Option<TxId>,
) -> Result<Value> {
    coerce_value_for_column_with_meta(table, meta, col, v, current_tx_max, active_tx)
}

fn coerce_value_for_column(
    db: &Database,
    table: &str,
    col_name: &str,
    v: Value,
    current_tx_max: Option<TxId>,
    active_tx: Option<TxId>,
) -> Result<Value> {
    let Some(meta) = db.table_meta(table) else {
        // Non-TxId variant: pass through with lenient id-name coercion.
        if let Value::TxId(_) = &v {
            return Err(Error::ColumnTypeMismatch {
                table: table.to_string(),
                column: col_name.to_string(),
                expected: "UNKNOWN",
                actual: "TxId",
            });
        }
        return Ok(coerce_uuid_if_needed(col_name, v));
    };
    coerce_value_for_column_with_meta(table, &meta, col_name, v, current_tx_max, active_tx)
}

fn coerce_value_for_column_with_meta(
    table: &str,
    meta: &TableMeta,
    col_name: &str,
    v: Value,
    current_tx_max: Option<TxId>,
    active_tx: Option<TxId>,
) -> Result<Value> {
    let Some(col) = meta.columns.iter().find(|c| c.name == col_name) else {
        if let Value::TxId(_) = &v {
            return Err(Error::ColumnTypeMismatch {
                table: table.to_string(),
                column: col_name.to_string(),
                expected: "UNKNOWN",
                actual: "TxId",
            });
        }
        return Ok(coerce_uuid_if_needed(col_name, v));
    };

    match col.column_type {
        contextdb_core::ColumnType::Uuid => match v {
            Value::TxId(_) => Err(Error::ColumnTypeMismatch {
                table: table.to_string(),
                column: col_name.to_string(),
                expected: "UUID",
                actual: "TxId",
            }),
            other => coerce_uuid_value(other),
        },
        contextdb_core::ColumnType::Timestamp => match v {
            Value::TxId(_) => Err(Error::ColumnTypeMismatch {
                table: table.to_string(),
                column: col_name.to_string(),
                expected: "TIMESTAMP",
                actual: "TxId",
            }),
            other => coerce_timestamp_value(other),
        },
        contextdb_core::ColumnType::Vector(dim) => match v {
            Value::TxId(_) => Err(Error::ColumnTypeMismatch {
                table: table.to_string(),
                column: col_name.to_string(),
                expected: format_vector_type(dim),
                actual: "TxId",
            }),
            other => coerce_vector_value(table, col_name, other, dim),
        },
        contextdb_core::ColumnType::Integer => match v {
            Value::TxId(_) => Err(Error::ColumnTypeMismatch {
                table: table.to_string(),
                column: col_name.to_string(),
                expected: "INTEGER",
                actual: "TxId",
            }),
            other => Ok(coerce_uuid_if_needed(col_name, other)),
        },
        contextdb_core::ColumnType::Real => match v {
            Value::TxId(_) => Err(Error::ColumnTypeMismatch {
                table: table.to_string(),
                column: col_name.to_string(),
                expected: "REAL",
                actual: "TxId",
            }),
            other => Ok(coerce_uuid_if_needed(col_name, other)),
        },
        contextdb_core::ColumnType::Text => match v {
            Value::TxId(_) => Err(Error::ColumnTypeMismatch {
                table: table.to_string(),
                column: col_name.to_string(),
                expected: "TEXT",
                actual: "TxId",
            }),
            other => Ok(coerce_uuid_if_needed(col_name, other)),
        },
        contextdb_core::ColumnType::Boolean => match v {
            Value::TxId(_) => Err(Error::ColumnTypeMismatch {
                table: table.to_string(),
                column: col_name.to_string(),
                expected: "BOOLEAN",
                actual: "TxId",
            }),
            other => Ok(coerce_uuid_if_needed(col_name, other)),
        },
        contextdb_core::ColumnType::Json => match v {
            Value::TxId(_) => Err(Error::ColumnTypeMismatch {
                table: table.to_string(),
                column: col_name.to_string(),
                expected: "JSON",
                actual: "TxId",
            }),
            other => Ok(coerce_uuid_if_needed(col_name, other)),
        },
        contextdb_core::ColumnType::TxId => {
            coerce_txid_value(table, col_name, v, col.nullable, current_tx_max, active_tx)
        }
    }
}

fn coerce_insert_value_for_column_with_meta(
    table: &str,
    meta: &TableMeta,
    col_name: &str,
    v: Value,
    current_tx_max: Option<TxId>,
    active_tx: Option<TxId>,
) -> Result<Value> {
    let should_auto_stamp_null = meta
        .columns
        .iter()
        .find(|column| column.name == col_name)
        .is_some_and(|column| {
            !column.nullable
                && matches!(column.column_type, contextdb_core::ColumnType::TxId)
                && matches!(&v, Value::Null)
        });
    if should_auto_stamp_null {
        let tx = active_tx.ok_or_else(|| Error::Other("missing active tx".to_string()))?;
        return Ok(Value::TxId(tx));
    }

    coerce_value_for_column_with_meta(table, meta, col_name, v, current_tx_max, active_tx)
}

fn format_vector_type(dim: usize) -> &'static str {
    // We need &'static str for the error variant. Fall back to a lookup for common dims.
    match dim {
        1 => "VECTOR(1)",
        2 => "VECTOR(2)",
        3 => "VECTOR(3)",
        4 => "VECTOR(4)",
        8 => "VECTOR(8)",
        16 => "VECTOR(16)",
        32 => "VECTOR(32)",
        64 => "VECTOR(64)",
        128 => "VECTOR(128)",
        256 => "VECTOR(256)",
        512 => "VECTOR(512)",
        768 => "VECTOR(768)",
        1024 => "VECTOR(1024)",
        1536 => "VECTOR(1536)",
        3072 => "VECTOR(3072)",
        _ => "VECTOR",
    }
}

fn coerce_txid_value(
    table: &str,
    col: &str,
    v: Value,
    nullable: bool,
    current_tx_max: Option<TxId>,
    active_tx: Option<TxId>,
) -> Result<Value> {
    match v {
        Value::Null => {
            if nullable {
                Ok(Value::Null)
            } else {
                Err(Error::ColumnNotNullable {
                    table: table.to_string(),
                    column: col.to_string(),
                })
            }
        }
        Value::TxId(tx) => {
            // Plan B7: `Value::TxId(n)` into a TXID column requires
            // `n <= max(committed_watermark, active_tx)`. The watermark is the
            // statement-scoped `current_tx_max` snapshot from
            // `TxManager::current_tx_max()`; `active_tx` is the in-flight
            // transaction that allocated the caller's TxId, which is permitted
            // as a self-reference. The error reports the watermark so callers
            // see what their edge has committed. Non-SQL callers pass `None`
            // for `current_tx_max` and skip the check.
            if let Some(max) = current_tx_max {
                let ceiling = max.0.max(active_tx.map(|t| t.0).unwrap_or(0));
                if tx.0 > ceiling {
                    return Err(Error::TxIdOutOfRange {
                        table: table.to_string(),
                        column: col.to_string(),
                        value: tx.0,
                        max: max.0,
                    });
                }
            }
            Ok(Value::TxId(tx))
        }
        other => Err(Error::ColumnTypeMismatch {
            table: table.to_string(),
            column: col.to_string(),
            expected: "TXID",
            actual: value_variant_name(&other),
        }),
    }
}

fn value_variant_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "Null",
        Value::Bool(_) => "Bool",
        Value::Int64(_) => "Int64",
        Value::Float64(_) => "Float64",
        Value::Text(_) => "Text",
        Value::Uuid(_) => "Uuid",
        Value::Timestamp(_) => "Timestamp",
        Value::Json(_) => "Json",
        Value::Vector(_) => "Vector",
        Value::TxId(_) => "TxId",
    }
}

fn coerce_uuid_value(v: Value) -> Result<Value> {
    match v {
        Value::Null => Ok(Value::Null),
        Value::Uuid(id) => Ok(Value::Uuid(id)),
        Value::Text(text) => uuid::Uuid::parse_str(&text)
            .map(Value::Uuid)
            .map_err(|err| Error::Other(format!("invalid UUID literal '{text}': {err}"))),
        other => Err(Error::Other(format!(
            "UUID column requires UUID or text literal, got {other:?}"
        ))),
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

fn coerce_timestamp_value(v: Value) -> Result<Value> {
    match v {
        Value::Null => Ok(Value::Null),
        Value::Text(text) if text.eq_ignore_ascii_case("infinity") => {
            Ok(Value::Timestamp(i64::MAX))
        }
        Value::Text(text) => {
            let parsed = OffsetDateTime::parse(&text, &Rfc3339).map_err(|err| {
                Error::Other(format!("invalid TIMESTAMP literal '{text}': {err}"))
            })?;
            Ok(Value::Timestamp(
                parsed.unix_timestamp_nanos() as i64 / 1_000_000,
            ))
        }
        other => Ok(other),
    }
}

fn coerce_vector_value(table: &str, column: &str, v: Value, expected_dim: usize) -> Result<Value> {
    let vector = match v {
        Value::Null => return Ok(Value::Null),
        Value::Vector(vector) => vector,
        Value::Text(text) => parse_text_vector_literal(&text)?,
        other => return Ok(other),
    };

    if vector.len() != expected_dim {
        return Err(vector_dimension_error(
            table,
            column,
            expected_dim,
            vector.len(),
        ));
    }

    Ok(Value::Vector(vector))
}

fn vector_dimension_error(table: &str, column: &str, expected: usize, got: usize) -> Error {
    Error::VectorIndexDimensionMismatch {
        index: contextdb_core::VectorIndexRef::new(table, column),
        expected,
        actual: got,
    }
}

fn parse_text_vector_literal(text: &str) -> Result<Vec<f32>> {
    let trimmed = text.trim();
    let inner = trimmed
        .strip_prefix('[')
        .and_then(|s| s.strip_suffix(']'))
        .ok_or_else(|| Error::Other(format!("invalid VECTOR literal '{text}'")))?;

    if inner.trim().is_empty() {
        return Ok(Vec::new());
    }

    inner
        .split(',')
        .map(|part| {
            part.trim().parse::<f32>().map_err(|err| {
                Error::Other(format!("invalid VECTOR component '{}': {err}", part.trim()))
            })
        })
        .collect()
}

fn apply_missing_column_defaults(
    db: &Database,
    table: &str,
    values: &mut HashMap<String, Value>,
    active_tx: Option<TxId>,
) -> Result<()> {
    let Some(meta) = db.table_meta(table) else {
        return Ok(());
    };

    let current_tx_max = Some(db.committed_watermark());

    for column in &meta.columns {
        if !column.nullable && matches!(column.column_type, ColumnType::TxId) {
            if matches!(values.get(&column.name), None | Some(Value::Null)) {
                let tx = active_tx.ok_or_else(|| Error::Other("missing active tx".to_string()))?;
                values.insert(column.name.clone(), Value::TxId(tx));
            }
            continue;
        }

        if values.contains_key(&column.name) {
            continue;
        }
        let Some(default) = &column.default else {
            continue;
        };
        let value = evaluate_stored_default_expr(default)?;
        values.insert(
            column.name.clone(),
            coerce_value_for_column(db, table, &column.name, value, current_tx_max, active_tx)?,
        );
    }

    Ok(())
}

fn evaluate_stored_default_expr(default: &str) -> Result<Value> {
    if default.eq_ignore_ascii_case("NOW()") {
        return eval_function("now", &[]);
    }
    if default.contains("FunctionCall") && default.contains("name: \"NOW\"") {
        return eval_function("now", &[]);
    }
    if default == "Literal(Null)" || default.eq_ignore_ascii_case("NULL") {
        return Ok(Value::Null);
    }
    if default.eq_ignore_ascii_case("TRUE") {
        return Ok(Value::Bool(true));
    }
    if default.eq_ignore_ascii_case("FALSE") {
        return Ok(Value::Bool(false));
    }
    if default.starts_with('\'') && default.ends_with('\'') && default.len() >= 2 {
        return Ok(Value::Text(
            default[1..default.len() - 1].replace("''", "'"),
        ));
    }
    if let Some(text) = default
        .strip_prefix("Literal(Text(\"")
        .and_then(|value| value.strip_suffix("\"))"))
    {
        return Ok(Value::Text(text.to_string()));
    }
    if let Some(value) = default
        .strip_prefix("Literal(Integer(")
        .and_then(|value| value.strip_suffix("))"))
    {
        let parsed = value.parse::<i64>().map_err(|err| {
            Error::Other(format!("invalid stored integer default '{value}': {err}"))
        })?;
        return Ok(Value::Int64(parsed));
    }
    if let Some(value) = default
        .strip_prefix("Literal(Real(")
        .and_then(|value| value.strip_suffix("))"))
    {
        let parsed = value
            .parse::<f64>()
            .map_err(|err| Error::Other(format!("invalid stored real default '{value}': {err}")))?;
        return Ok(Value::Float64(parsed));
    }
    if let Some(value) = default
        .strip_prefix("Literal(Bool(")
        .and_then(|value| value.strip_suffix("))"))
    {
        let parsed = value
            .parse::<bool>()
            .map_err(|err| Error::Other(format!("invalid stored bool default '{value}': {err}")))?;
        return Ok(Value::Bool(parsed));
    }

    Err(Error::Other(format!(
        "unsupported stored DEFAULT expression: {default}"
    )))
}

pub(crate) fn stored_default_expr(expr: &Expr) -> String {
    match expr {
        Expr::Literal(Literal::Null) => "NULL".to_string(),
        Expr::Literal(Literal::Bool(value)) => {
            if *value {
                "TRUE".to_string()
            } else {
                "FALSE".to_string()
            }
        }
        Expr::Literal(Literal::Integer(value)) => value.to_string(),
        Expr::Literal(Literal::Real(value)) => value.to_string(),
        Expr::Literal(Literal::Text(value)) => format!("'{}'", value.replace('\'', "''")),
        Expr::FunctionCall { name, args }
            if name.eq_ignore_ascii_case("NOW") && args.is_empty() =>
        {
            "NOW()".to_string()
        }
        _ => format!("{expr:?}"),
    }
}

fn validate_expires_column(col: &contextdb_parser::ast::ColumnDef) -> Result<()> {
    if col.expires && !matches!(col.data_type, DataType::Timestamp) {
        return Err(Error::Other(
            "EXPIRES is only valid on TIMESTAMP columns".to_string(),
        ));
    }
    Ok(())
}

fn expires_column_name(columns: &[contextdb_parser::ast::ColumnDef]) -> Result<Option<String>> {
    let mut expires_column = None;
    for col in columns {
        validate_expires_column(col)?;
        if col.expires {
            if expires_column.is_some() {
                return Err(Error::Other(
                    "only one EXPIRES column is supported per table".to_string(),
                ));
            }
            expires_column = Some(col.name.clone());
        }
    }
    Ok(expires_column)
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
        DataType::TxId => contextdb_core::ColumnType::TxId,
    }
}

pub(crate) fn map_rank_policy(
    policy: &contextdb_parser::ast::RankPolicyAst,
) -> contextdb_core::RankPolicy {
    contextdb_core::RankPolicy {
        joined_table: policy.joined_table.clone(),
        joined_column: policy.joined_column.clone(),
        anchor_column: String::new(),
        sort_key: policy.sort_key.clone(),
        formula: policy.formula.clone(),
        protected_index: String::new(),
    }
}

struct ResolvedRankPolicy {
    policy: contextdb_core::RankPolicy,
    formula: Arc<RankFormula>,
}

fn core_column_from_ast(
    col: &contextdb_parser::ast::ColumnDef,
    rank_policy: Option<contextdb_core::RankPolicy>,
) -> contextdb_core::ColumnDef {
    contextdb_core::ColumnDef {
        name: col.name.clone(),
        column_type: map_column_type(&col.data_type),
        nullable: col.nullable,
        primary_key: col.primary_key,
        unique: col.unique,
        default: col.default.as_ref().map(stored_default_expr),
        references: col
            .references
            .as_ref()
            .map(|reference| contextdb_core::ForeignKeyReference {
                table: reference.table.clone(),
                column: reference.column.clone(),
            }),
        expires: col.expires,
        immutable: col.immutable,
        quantization: map_vector_quantization(col.quantization),
        rank_policy,
        context_id: col.context_id,
        scope_label: col.scope_label.as_deref().map(|scope| match scope {
            contextdb_parser::ast::ScopeLabelConstraint::Simple { labels } => {
                contextdb_core::ScopeLabelKind::Simple {
                    write_labels: labels.clone(),
                }
            }
            contextdb_parser::ast::ScopeLabelConstraint::Split { read, write } => {
                contextdb_core::ScopeLabelKind::Split {
                    read_labels: read.clone(),
                    write_labels: write.clone(),
                }
            }
        }),
        acl_ref: col.acl_ref.as_ref().map(|acl| contextdb_core::AclRef {
            ref_table: acl.ref_table.clone(),
            ref_column: acl.ref_column.clone(),
        }),
    }
}

fn ast_column_from_core(col: contextdb_core::ColumnDef) -> contextdb_parser::ast::ColumnDef {
    contextdb_parser::ast::ColumnDef {
        name: col.name,
        data_type: match col.column_type {
            ColumnType::Uuid => DataType::Uuid,
            ColumnType::Text => DataType::Text,
            ColumnType::Integer => DataType::Integer,
            ColumnType::Real => DataType::Real,
            ColumnType::Boolean => DataType::Boolean,
            ColumnType::Timestamp => DataType::Timestamp,
            ColumnType::Json => DataType::Json,
            ColumnType::Vector(dim) => DataType::Vector(dim as u32),
            ColumnType::TxId => DataType::TxId,
        },
        nullable: col.nullable,
        primary_key: col.primary_key,
        unique: col.unique,
        default: None,
        references: None,
        expires: col.expires,
        immutable: col.immutable,
        quantization: match col.quantization {
            contextdb_core::VectorQuantization::F32 => {
                contextdb_parser::ast::VectorQuantization::F32
            }
            contextdb_core::VectorQuantization::SQ8 => {
                contextdb_parser::ast::VectorQuantization::SQ8
            }
            contextdb_core::VectorQuantization::SQ4 => {
                contextdb_parser::ast::VectorQuantization::SQ4
            }
        },
        rank_policy: None,
        context_id: col.context_id,
        scope_label: None,
        acl_ref: None,
    }
}

fn validate_rank_policy_for_column(
    db: &Database,
    table: &str,
    column: &contextdb_parser::ast::ColumnDef,
    all_columns: &[contextdb_parser::ast::ColumnDef],
) -> Result<Option<ResolvedRankPolicy>> {
    let Some(policy_ast) = column.rank_policy.as_deref() else {
        return Ok(None);
    };
    let index = rank_index_name(table, &column.name);
    if !matches!(column.data_type, DataType::Vector(_)) {
        return Err(Error::RankPolicyColumnType {
            index,
            column: column.name.clone(),
            expected: "VECTOR(N)".to_string(),
            actual: data_type_name(&column.data_type).to_string(),
        });
    }
    let joined_meta = db.table_meta(&policy_ast.joined_table).ok_or_else(|| {
        Error::RankPolicyJoinTableUnknown {
            index: index.clone(),
            table: policy_ast.joined_table.clone(),
        }
    })?;
    if !joined_meta
        .columns
        .iter()
        .any(|col| col.name == policy_ast.joined_column)
    {
        return Err(Error::RankPolicyJoinColumnUnknown {
            index: index.clone(),
            table: policy_ast.joined_table.clone(),
            column: policy_ast.joined_column.clone(),
        });
    }
    let protected_index = protected_rank_policy_index(&joined_meta, &policy_ast.joined_column)
        .ok_or_else(|| Error::RankPolicyJoinColumnUnindexed {
            index: index.clone(),
            joined_table: policy_ast.joined_table.clone(),
            column: policy_ast.joined_column.clone(),
        })?;
    let anchor_column =
        resolve_rank_policy_anchor_column(&index, policy_ast, all_columns, &joined_meta)?;
    let formula = Arc::new(RankFormula::compile_for_index(&index, &policy_ast.formula)?);
    validate_rank_formula_columns(
        &index,
        &column.name,
        all_columns,
        &joined_meta,
        formula.column_refs(),
    )?;
    Ok(Some(ResolvedRankPolicy {
        policy: contextdb_core::RankPolicy {
            joined_table: policy_ast.joined_table.clone(),
            joined_column: policy_ast.joined_column.clone(),
            anchor_column,
            sort_key: policy_ast.sort_key.clone(),
            formula: policy_ast.formula.clone(),
            protected_index,
        },
        formula,
    }))
}

fn protected_rank_policy_index(meta: &TableMeta, joined_column: &str) -> Option<String> {
    meta.indexes
        .iter()
        .filter(|index| index.kind == contextdb_core::IndexKind::UserDeclared)
        .chain(meta.indexes.iter())
        .find(|index| {
            index
                .columns
                .first()
                .is_some_and(|(column, _)| column == joined_column)
        })
        .map(|index| index.name.clone())
}

fn resolve_rank_policy_anchor_column(
    index: &str,
    policy: &contextdb_parser::ast::RankPolicyAst,
    anchor_columns: &[contextdb_parser::ast::ColumnDef],
    joined_meta: &TableMeta,
) -> Result<String> {
    let joined_column = joined_meta
        .columns
        .iter()
        .find(|col| col.name == policy.joined_column)
        .ok_or_else(|| Error::RankPolicyJoinColumnUnknown {
            index: index.to_string(),
            table: policy.joined_table.clone(),
            column: policy.joined_column.clone(),
        })?;

    let anchor_by_name = |name: &str| anchor_columns.iter().find(|col| col.name == name);
    let mut candidates = Vec::new();
    if joined_column.primary_key {
        let singular = singular_table_name(&policy.joined_table);
        for name in [
            format!("{singular}_id"),
            format!("{}_id", policy.joined_table),
        ] {
            if anchor_by_name(&name).is_some() && !candidates.contains(&name) {
                candidates.push(name);
            }
        }
    }
    if candidates.is_empty() && anchor_by_name(&policy.joined_column).is_some() {
        candidates.push(policy.joined_column.clone());
    }
    if candidates.is_empty()
        && let Some(primary_key) = anchor_columns.iter().find(|col| col.primary_key)
    {
        candidates.push(primary_key.name.clone());
    }
    if candidates.is_empty() && anchor_by_name("id").is_some() {
        candidates.push("id".to_string());
    }

    let anchor_column = match candidates.as_slice() {
        [single] => single.clone(),
        [] => {
            return Err(Error::RankPolicyColumnUnknown {
                index: index.to_string(),
                column: policy.joined_column.clone(),
            });
        }
        _ => {
            return Err(Error::RankPolicyColumnAmbiguous {
                index: index.to_string(),
                column: candidates.join(","),
            });
        }
    };
    let anchor_def =
        anchor_by_name(&anchor_column).ok_or_else(|| Error::RankPolicyColumnUnknown {
            index: index.to_string(),
            column: anchor_column.clone(),
        })?;
    let anchor_type = map_column_type(&anchor_def.data_type);
    if anchor_type != joined_column.column_type {
        return Err(Error::RankPolicyColumnType {
            index: index.to_string(),
            column: anchor_column,
            expected: column_type_name(&joined_column.column_type).to_string(),
            actual: data_type_name(&anchor_def.data_type).to_string(),
        });
    }
    Ok(anchor_column)
}

fn singular_table_name(table: &str) -> String {
    if let Some(stem) = table.strip_suffix("ies") {
        format!("{stem}y")
    } else if let Some(stem) = table.strip_suffix('s') {
        stem.to_string()
    } else {
        table.to_string()
    }
}

fn validate_rank_formula_columns(
    index: &str,
    anchor_vector_column: &str,
    anchor_columns: &[contextdb_parser::ast::ColumnDef],
    joined_meta: &TableMeta,
    refs: &[String],
) -> Result<()> {
    let vector_score_column_exists = anchor_columns.iter().any(|col| col.name == "vector_score")
        || joined_meta
            .columns
            .iter()
            .any(|col| col.name == "vector_score");
    for column in refs {
        if column == "vector_score" {
            if vector_score_column_exists {
                return Err(Error::RankPolicyColumnAmbiguous {
                    index: index.to_string(),
                    column: column.clone(),
                });
            }
            continue;
        }
        let anchor = anchor_columns.iter().find(|col| col.name == *column);
        let joined = joined_meta.columns.iter().find(|col| col.name == *column);
        if anchor.is_none() && joined.is_none() {
            return Err(Error::RankPolicyColumnUnknown {
                index: index.to_string(),
                column: column.clone(),
            });
        }
        if column == "id" && anchor.is_some() && joined.is_some() {
            return Err(Error::RankPolicyColumnAmbiguous {
                index: index.to_string(),
                column: column.clone(),
            });
        }
        if let Some(anchor) = anchor {
            validate_rank_formula_type(
                index,
                column,
                data_type_name(&anchor.data_type),
                &map_column_type(&anchor.data_type),
            )?;
        } else if let Some(joined) = joined {
            validate_rank_formula_type(
                index,
                column,
                column_type_name(&joined.column_type),
                &joined.column_type,
            )?;
        }
    }
    if refs.iter().any(|column| column == anchor_vector_column) {
        return Err(Error::RankPolicyColumnType {
            index: index.to_string(),
            column: anchor_vector_column.to_string(),
            expected: "number-or-bool".to_string(),
            actual: "VECTOR".to_string(),
        });
    }
    Ok(())
}

fn validate_rank_formula_type(
    index: &str,
    column: &str,
    actual_name: &str,
    column_type: &ColumnType,
) -> Result<()> {
    if matches!(
        column_type,
        ColumnType::Real | ColumnType::Integer | ColumnType::Boolean
    ) {
        return Ok(());
    }
    Err(Error::RankPolicyColumnType {
        index: index.to_string(),
        column: column.to_string(),
        expected: "number-or-bool".to_string(),
        actual: actual_name.to_string(),
    })
}

fn data_type_name(data_type: &DataType) -> &'static str {
    match data_type {
        DataType::Uuid => "UUID",
        DataType::Text => "TEXT",
        DataType::Integer => "INTEGER",
        DataType::Real => "REAL",
        DataType::Boolean => "BOOLEAN",
        DataType::Timestamp => "TIMESTAMP",
        DataType::Json => "JSON",
        DataType::Vector(_) => "VECTOR",
        DataType::TxId => "TXID",
    }
}

fn column_type_name(column_type: &ColumnType) -> &'static str {
    match column_type {
        ColumnType::Uuid => "UUID",
        ColumnType::Text => "TEXT",
        ColumnType::Integer => "INTEGER",
        ColumnType::Real => "REAL",
        ColumnType::Boolean => "BOOLEAN",
        ColumnType::Timestamp => "TIMESTAMP",
        ColumnType::Json => "JSON",
        ColumnType::Vector(_) => "VECTOR",
        ColumnType::TxId => "TXID",
    }
}

pub(crate) fn rank_policy_drop_table_blocker(db: &Database, table: &str) -> Option<Error> {
    for (policy_table, policy_column, policy) in all_rank_policies(db) {
        if policy.joined_table == table && policy_table != table {
            return Some(Error::DropBlockedByRankPolicy {
                table: table.into(),
                column: None,
                dropped_index: None,
                policy_table: policy_table.into_boxed_str(),
                policy_column: policy_column.into_boxed_str(),
                sort_key: policy.sort_key.into_boxed_str(),
            });
        }
    }
    None
}

pub(crate) fn rank_policy_drop_index_blocker(
    db: &Database,
    table: &str,
    index: &str,
) -> Option<Error> {
    for (policy_table, policy_column, policy) in all_rank_policies(db) {
        if policy.joined_table == table && policy.protected_index == index {
            return Some(Error::DropBlockedByRankPolicy {
                table: table.into(),
                column: None,
                dropped_index: Some(index.into()),
                policy_table: policy_table.into_boxed_str(),
                policy_column: policy_column.into_boxed_str(),
                sort_key: policy.sort_key.into_boxed_str(),
            });
        }
    }
    None
}

fn rank_policy_drop_column_blocker(db: &Database, table: &str, column: &str) -> Option<Error> {
    let metas = db
        .table_names()
        .into_iter()
        .filter_map(|name| db.table_meta(&name).map(|meta| (name, meta)))
        .collect::<HashMap<_, _>>();
    for (policy_table, meta) in &metas {
        for policy_col in &meta.columns {
            let Some(policy) = &policy_col.rank_policy else {
                continue;
            };
            if policy_table == table && policy_col.name == column {
                return Some(drop_column_rank_error(
                    table,
                    column,
                    policy_table,
                    &policy_col.name,
                    policy,
                ));
            }
            if policy.joined_table == table && policy.joined_column == column {
                return Some(drop_column_rank_error(
                    table,
                    column,
                    policy_table,
                    &policy_col.name,
                    policy,
                ));
            }
            if policy_table == table && policy.anchor_column == column {
                return Some(drop_column_rank_error(
                    table,
                    column,
                    policy_table,
                    &policy_col.name,
                    policy,
                ));
            }
            let Ok(formula) = RankFormula::compile_for_index(
                &rank_index_name(policy_table, &policy_col.name),
                &policy.formula,
            ) else {
                continue;
            };
            let joined_meta = metas.get(&policy.joined_table);
            for reference in formula.column_refs() {
                if reference == "vector_score" {
                    continue;
                }
                let anchor_has = meta.columns.iter().any(|col| col.name == *reference);
                let joined_has = joined_meta
                    .is_some_and(|joined| joined.columns.iter().any(|col| col.name == *reference));
                if anchor_has && policy_table == table && reference == column {
                    return Some(drop_column_rank_error(
                        table,
                        column,
                        policy_table,
                        &policy_col.name,
                        policy,
                    ));
                }
                if !anchor_has && joined_has && policy.joined_table == table && reference == column
                {
                    return Some(drop_column_rank_error(
                        table,
                        column,
                        policy_table,
                        &policy_col.name,
                        policy,
                    ));
                }
            }
        }
    }
    None
}

fn drop_column_rank_error(
    table: &str,
    column: &str,
    policy_table: &str,
    policy_column: &str,
    policy: &contextdb_core::RankPolicy,
) -> Error {
    Error::DropBlockedByRankPolicy {
        table: table.into(),
        column: Some(column.into()),
        dropped_index: None,
        policy_table: policy_table.into(),
        policy_column: policy_column.into(),
        sort_key: policy.sort_key.clone().into_boxed_str(),
    }
}

fn all_rank_policies(db: &Database) -> Vec<(String, String, contextdb_core::RankPolicy)> {
    db.table_names()
        .into_iter()
        .filter_map(|table| db.table_meta(&table).map(|meta| (table, meta)))
        .flat_map(|(table, meta)| {
            meta.columns.into_iter().filter_map(move |column| {
                column
                    .rank_policy
                    .map(|policy| (table.clone(), column.name, policy))
            })
        })
        .collect()
}

fn map_vector_quantization(
    quantization: contextdb_parser::ast::VectorQuantization,
) -> contextdb_core::VectorQuantization {
    match quantization {
        contextdb_parser::ast::VectorQuantization::F32 => contextdb_core::VectorQuantization::F32,
        contextdb_parser::ast::VectorQuantization::SQ8 => contextdb_core::VectorQuantization::SQ8,
        contextdb_parser::ast::VectorQuantization::SQ4 => contextdb_core::VectorQuantization::SQ4,
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

fn project_graph_frontier_rows(
    frontier: Vec<(HashMap<String, uuid::Uuid>, uuid::Uuid, u32)>,
    start_alias: &str,
    steps: &[GraphStepPlan],
) -> Result<Vec<Vec<Value>>> {
    frontier
        .into_iter()
        .map(|(bindings, id, depth)| {
            let mut row = Vec::with_capacity(steps.len() + 3);
            let start_id = bindings.get(start_alias).ok_or_else(|| {
                Error::PlanError(format!(
                    "graph frontier missing required start alias binding '{start_alias}'"
                ))
            })?;
            row.push(Value::Uuid(*start_id));
            for step in steps {
                let target_id = bindings.get(&step.target_alias).ok_or_else(|| {
                    Error::PlanError(format!(
                        "graph frontier missing required target alias binding '{}'",
                        step.target_alias
                    ))
                })?;
                row.push(Value::Uuid(*target_id));
            }
            row.push(Value::Uuid(id));
            row.push(Value::Int64(depth as i64));
            Ok(row)
        })
        .collect()
}

fn release_accounted_bytes(db: &Database, bytes: &[usize]) {
    for bytes in bytes {
        db.accountant().release(*bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use contextdb_planner::GraphStepPlan;
    use uuid::Uuid;

    #[test]
    fn graph_01_frontier_projection_requires_complete_bindings() {
        let steps = vec![GraphStepPlan {
            edge_types: vec!["EDGE".to_string()],
            direction: Direction::Outgoing,
            min_depth: 1,
            max_depth: 1,
            target_alias: "b".to_string(),
        }];

        let missing_start = vec![(HashMap::new(), Uuid::new_v4(), 0)];
        let missing_target = vec![(
            HashMap::from([("a".to_string(), Uuid::new_v4())]),
            Uuid::new_v4(),
            0,
        )];

        let start_result = project_graph_frontier_rows(missing_start, "a", &steps);
        assert!(
            matches!(start_result, Err(Error::PlanError(_))),
            "graph frontier projection should return a plan error on missing start alias binding, got {start_result:?}"
        );

        let target_result = project_graph_frontier_rows(missing_target, "a", &steps);
        assert!(
            matches!(target_result, Err(Error::PlanError(_))),
            "graph frontier projection should return a plan error on missing target alias binding, got {target_result:?}"
        );
    }
}
