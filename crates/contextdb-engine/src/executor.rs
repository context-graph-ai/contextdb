use crate::database::{
    Database, InsertRowResult, QueryResult, QueryTrace, UpdateReplacementContext,
    UpsertIntentDetails, rank_index_name,
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
use std::borrow::Cow;
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
            // The contradiction is refused before anything is created, so a
            // rejected declaration never leaves a half-made table behind.
            refuse_promise_with_no_delivery(
                &p.name,
                p.retain.as_ref().is_some_and(|retain| retain.sync_safe),
                p.sync_direction,
            )?;
            let expires_column = expires_column_name(&p.columns)?;
            // Auto-generate implicit indexes for PK / UNIQUE columns and
            // composite UNIQUE constraints, so constraint probes run at
            // O(log n) instead of O(n) per insert.
            let mut auto_indexes: Vec<contextdb_core::IndexDecl> = Vec::new();
            for c in &p.columns {
                if c.primary_key && exact_constraint_key_indexable(&map_column_type(&c.data_type)) {
                    auto_indexes.push(contextdb_core::IndexDecl {
                        name: format!("__pk_{}", c.name),
                        columns: vec![(c.name.clone(), contextdb_core::SortDirection::Asc)],
                        kind: contextdb_core::IndexKind::Auto,
                    });
                }
                if c.unique
                    && !c.primary_key
                    && exact_constraint_key_indexable(&map_column_type(&c.data_type))
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
                        .map(|c| exact_constraint_key_indexable(&map_column_type(&c.data_type)))
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
            let composite_foreign_keys = p
                .composite_foreign_keys
                .iter()
                .map(|fk| contextdb_core::CompositeForeignKey {
                    child_columns: fk.child_columns.clone(),
                    parent_table: fk.parent_table.clone(),
                    parent_columns: fk.parent_columns.clone(),
                })
                .collect::<Vec<_>>();

            let mut resolved_policies = HashMap::<String, ResolvedRankPolicy>::new();
            for column in &p.columns {
                if let Some(resolved) =
                    validate_rank_policy_for_column(db, &p.name, column, &p.columns)?
                {
                    resolved_policies.insert(column.name.clone(), resolved);
                }
            }
            let mut meta = TableMeta {
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
                composite_foreign_keys,
                sync_direction: p.sync_direction,
                retain_declared_unit: p.retain.as_ref().map(|retain| retain.declared_unit),
                primary_key_columns: p.primary_key_columns.clone(),
                conflict_policy: p.conflict_policy,
            };
            refuse_sync_safe_without_key(&p.name, &meta)?;
            let candidate_meta = meta.clone();
            validate_exact_constraint_keys_for_meta(&p.name, &meta)?;
            validate_single_column_foreign_keys_for_meta(&p.name, &meta, |parent| {
                if parent == p.name {
                    Some(candidate_meta.clone())
                } else {
                    db.table_meta(parent)
                }
            })?;
            validate_composite_foreign_keys_for_meta(&p.name, &meta, |parent| {
                if parent == p.name {
                    Some(candidate_meta.clone())
                } else {
                    db.table_meta(parent)
                }
            })?;
            meta.indexes = auto_indexes_for_table_meta(&meta);
            auto_indexes = meta.indexes.clone();
            let metadata_bytes = meta.estimated_bytes();
            db.accountant().try_allocate_for(
                metadata_bytes,
                "ddl",
                "create_table",
                "Reduce schema size or raise MEMORY_LIMIT before creating more tables.",
            )?;
            let has_vector_columns = meta
                .columns
                .iter()
                .any(|column| matches!(column.column_type, contextdb_core::ColumnType::Vector(_)));
            if has_vector_columns {
                let vector_schema_refs = meta
                    .columns
                    .iter()
                    .filter(|column| {
                        matches!(column.column_type, contextdb_core::ColumnType::Vector(_))
                    })
                    .map(|column| VectorIndexRef::new(&p.name, column.name.clone()));
                let _vector_schema = db.vector_schema_write_many(vector_schema_refs);
                db.allocate_ddl_lsn(|lsn| {
                    db.relational_store().create_table(&p.name, meta);
                    for idx in &auto_indexes {
                        db.relational_store().create_exact_index_storage(
                            &p.name,
                            &idx.name,
                            idx.columns.clone(),
                        );
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
                        db.log_create_table_ddl(&p.name, &table_meta, lsn)?;
                    }
                    Ok(())
                })?;
                db.clear_statement_cache();
                db.start_maintenance_if_eligible();
                return Ok(QueryResult::empty_with_affected(0));
            } else {
                db.allocate_ddl_lsn(|lsn| {
                    db.relational_store().create_table(&p.name, meta);
                    for idx in &auto_indexes {
                        db.relational_store().create_exact_index_storage(
                            &p.name,
                            &idx.name,
                            idx.columns.clone(),
                        );
                    }
                    if let Some(table_meta) = db.table_meta(&p.name) {
                        db.persist_table_meta(&p.name, &table_meta)?;
                        db.log_create_table_ddl(&p.name, &table_meta, lsn)?;
                    }
                    Ok(())
                })?;
            }
            db.clear_statement_cache();
            db.start_maintenance_if_eligible();
            Ok(QueryResult::empty_with_affected(0))
        }
        PhysicalPlan::DropTable(name) => {
            require_admin_for_ddl(db)?;
            if let Some(block) = rank_policy_drop_table_blocker(db, name) {
                return Err(block);
            }
            validate_projected_foreign_keys_after_drop_table(db, name)?;
            let bytes_to_release = estimate_drop_table_bytes(db, name);
            db.drain_vector_table_maintenance_for_ddl(name);
            let _vector_schema = db.vector_schema_write_table(name);
            db.allocate_ddl_lsn(|lsn| db.log_drop_table_ddl_and_remove_triggers(name, lsn))?;
            db.accountant().release(bytes_to_release);
            db.clear_statement_cache();
            Ok(QueryResult::empty_with_affected(0))
        }
        PhysicalPlan::AlterTable(p) => {
            require_admin_for_ddl(db)?;
            db.check_disk_budget("ALTER TABLE")?;
            let store = db.relational_store();
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
                    if let Some(existing_meta) = db.table_meta(&p.table) {
                        let mut candidate_meta = existing_meta;
                        candidate_meta.columns.push(core_col.clone());
                        let candidate_lookup = candidate_meta.clone();
                        validate_exact_constraint_keys_for_meta(&p.table, &candidate_meta)?;
                        validate_single_column_foreign_keys_for_meta(
                            &p.table,
                            &candidate_meta,
                            |parent| {
                                if parent == p.table {
                                    Some(candidate_lookup.clone())
                                } else {
                                    db.table_meta(parent)
                                }
                            },
                        )?;
                    }
                    if matches!(col.data_type, DataType::Vector(_)) {
                        let index = VectorIndexRef::new(&p.table, col.name.clone());
                        let _vector_schema = db.vector_schema_write(&index);
                        db.allocate_ddl_lsn(|lsn| {
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
                            refresh_auto_indexes_for_table(db, &p.table)?;
                            if let Some(table_meta) = db.table_meta(&p.table) {
                                db.persist_table_meta(&p.table, &table_meta)?;
                                db.log_alter_table_ddl(&p.table, &table_meta, lsn)?;
                            }
                            Ok(())
                        })?;
                        db.clear_statement_cache();
                        return Ok(QueryResult::empty_with_affected(0));
                    }
                    db.allocate_ddl_lsn(|lsn| {
                        store
                            .alter_table_add_column(&p.table, core_col)
                            .map_err(Error::Other)?;
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
                        refresh_auto_indexes_for_table(db, &p.table)?;
                        if let Some(table_meta) = db.table_meta(&p.table) {
                            db.persist_table_meta(&p.table, &table_meta)?;
                            db.log_alter_table_ddl(&p.table, &table_meta, lsn)?;
                        }
                        Ok(())
                    })?;
                    db.clear_statement_cache();
                    return Ok(QueryResult::empty_with_affected(0));
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
                    // A table-level composite PRIMARY KEY (col, col, ...) does not
                    // set the column-level primary_key flag above, so guard its key
                    // columns explicitly: a composite key column is as un-droppable
                    // as a single-column one. Dropping one would leave
                    // primary_key_columns naming an absent column and strand rows
                    // out of outgoing sync.
                    if let Some(existing_meta) = db.table_meta(&p.table)
                        && existing_meta.primary_key_columns.iter().any(|c| c == name)
                    {
                        return Err(Error::Other(format!(
                            "cannot drop primary key column {}.{}",
                            p.table, name
                        )));
                    }
                    validate_projected_foreign_keys_after_drop_column(db, &p.table, name)?;
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
                    if dropped_vector_column {
                        let index = VectorIndexRef::new(&p.table, name.clone());
                        db.drain_vector_index_maintenance_for_ddl(&index);
                        let _vector_schema = db.vector_schema_write(&index);
                        db.allocate_ddl_lsn(|lsn| {
                            store
                                .alter_table_drop_column(&p.table, name)
                                .map_err(Error::Other)?;
                            db.remove_rank_formula(&p.table, name);
                            if *cascade {
                                {
                                    let mut metas = store.table_meta.write();
                                    if let Some(m) = metas.get_mut(&p.table) {
                                        m.indexes
                                            .retain(|i| !i.columns.iter().any(|(c, _)| c == name));
                                    }
                                }
                                for idx in &dependent_indexes {
                                    store.drop_index_storage(&p.table, idx);
                                    if dependent_user_indexes.iter().any(|name| name == idx) {
                                        db.log_drop_index_ddl(&p.table, idx, lsn)?;
                                    }
                                }
                            }
                            let mut meta = store.table_meta.write();
                            if let Some(table_meta) = meta.get_mut(&p.table)
                                && table_meta.expires_column.as_deref() == Some(name.as_str())
                            {
                                table_meta.expires_column = None;
                            }
                            drop(meta);
                            db.deregister_vector_index(&p.table, name);
                            refresh_auto_indexes_for_table(db, &p.table)?;
                            if let Some(table_meta) = db.table_meta(&p.table) {
                                db.persist_table_meta_rows_vectors_and_log_alter_table_ddl(
                                    &p.table,
                                    &table_meta,
                                    lsn,
                                )?;
                            }
                            Ok(())
                        })?;
                    } else {
                        store
                            .alter_table_drop_column(&p.table, name)
                            .map_err(Error::Other)?;
                        db.remove_rank_formula(&p.table, name);
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
                                if dependent_user_indexes.iter().any(|name| name == idx) {
                                    db.allocate_ddl_lsn(|lsn| {
                                        db.log_drop_index_ddl(&p.table, idx, lsn)
                                    })?;
                                }
                            }
                        }
                        let mut meta = store.table_meta.write();
                        if let Some(table_meta) = meta.get_mut(&p.table)
                            && table_meta.expires_column.as_deref() == Some(name.as_str())
                        {
                            table_meta.expires_column = None;
                        }
                        drop(meta);
                        refresh_auto_indexes_for_table(db, &p.table)?;
                        if let Some(table_meta) = db.table_meta(&p.table) {
                            db.persist_table_meta(&p.table, &table_meta)?;
                            db.persist_table_rows(&p.table)?;
                            db.allocate_ddl_lsn(|lsn| {
                                db.log_alter_table_ddl(&p.table, &table_meta, lsn)
                            })?;
                        }
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
                    if let Some(existing_meta) = db.table_meta(&p.table)
                        && let Some(col) = existing_meta.columns.iter().find(|c| c.name == *from)
                        && col.primary_key
                    {
                        return Err(Error::Other(format!(
                            "cannot rename primary key column '{}'",
                            from
                        )));
                    }
                    // A table-level composite PRIMARY KEY (col, col, ...) does not
                    // set the column-level primary_key flag above, so guard its key
                    // columns explicitly: renaming one leaves primary_key_columns
                    // naming an absent column.
                    if let Some(existing_meta) = db.table_meta(&p.table)
                        && existing_meta.primary_key_columns.iter().any(|c| c == from)
                    {
                        return Err(Error::Other(format!(
                            "cannot rename primary key column '{}'",
                            from
                        )));
                    }
                    validate_projected_foreign_keys_after_rename_column(db, &p.table, from, to)?;
                    if renamed_vector_column
                        && db
                            .table_meta(&p.table)
                            .is_some_and(|meta| meta.columns.iter().any(|c| c.name == *to))
                    {
                        return Err(Error::Other(format!(
                            "column '{}' already exists in table '{}'",
                            to, p.table
                        )));
                    }
                    if renamed_vector_column {
                        let old_index = VectorIndexRef::new(&p.table, from.clone());
                        let new_index = VectorIndexRef::new(&p.table, to.clone());
                        db.drain_vector_index_maintenance_for_ddl(&old_index);
                        let _vector_schema =
                            db.vector_schema_write_many([old_index.clone(), new_index]);
                        db.allocate_ddl_lsn(|lsn| {
                            store
                                .alter_table_rename_column(&p.table, from, to)
                                .map_err(Error::Other)?;
                            let mut meta = store.table_meta.write();
                            if let Some(table_meta) = meta.get_mut(&p.table)
                                && table_meta.expires_column.as_deref() == Some(from.as_str())
                            {
                                table_meta.expires_column = Some(to.clone());
                            }
                            drop(meta);
                            db.rename_vector_index(&p.table, from, to)?;
                            refresh_auto_indexes_for_table(db, &p.table)?;
                            if let Some(table_meta) = db.table_meta(&p.table) {
                                db.persist_table_meta_rows_vectors_and_log_alter_table_ddl_with_vector_rename(
                                    &p.table,
                                    &table_meta,
                                    from,
                                    to,
                                    lsn,
                                )?;
                            }
                            Ok(())
                        })?;
                        db.clear_statement_cache();
                        return Ok(QueryResult::empty_with_affected(0));
                    } else {
                        store
                            .alter_table_rename_column(&p.table, from, to)
                            .map_err(Error::Other)?;
                    }
                    let mut meta = store.table_meta.write();
                    if let Some(table_meta) = meta.get_mut(&p.table)
                        && table_meta.expires_column.as_deref() == Some(from.as_str())
                    {
                        table_meta.expires_column = Some(to.clone());
                    }
                    drop(meta);
                    refresh_auto_indexes_for_table(db, &p.table)?;
                }
                AlterAction::SetRetain {
                    duration_seconds,
                    sync_safe,
                    declared_unit,
                } => {
                    // Refused before the write lock is taken, so a rejected
                    // ALTER applies no part of itself — not the new window,
                    // not the SYNC SAFE flag.
                    if *sync_safe {
                        let existing = db.table_meta(&p.table).ok_or_else(|| {
                            Error::Other(format!("table '{}' not found", p.table))
                        })?;
                        refuse_promise_with_no_delivery(
                            &p.table,
                            true,
                            Some(effective_sync_direction(&existing)),
                        )?;
                        refuse_sync_safe_without_key_for(&p.table, &existing)?;
                    }
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
                    table_meta.retain_declared_unit = Some(*declared_unit);
                }
                AlterAction::DropRetain => {
                    let mut meta = store.table_meta.write();
                    let table_meta = meta
                        .get_mut(&p.table)
                        .ok_or_else(|| Error::Other(format!("table '{}' not found", p.table)))?;
                    table_meta.default_ttl_seconds = None;
                    table_meta.sync_safe = false;
                    table_meta.retain_declared_unit = None;
                }
                AlterAction::SetSyncDirection(direction) => {
                    // Refused before the write lock, for the same reason: a
                    // rejected ALTER applies no part of itself.
                    let existing = db
                        .table_meta(&p.table)
                        .ok_or_else(|| Error::Other(format!("table '{}' not found", p.table)))?;
                    refuse_promise_with_no_delivery(
                        &p.table,
                        existing.sync_safe,
                        Some(*direction),
                    )?;
                    let mut meta = store.table_meta.write();
                    let table_meta = meta
                        .get_mut(&p.table)
                        .ok_or_else(|| Error::Other(format!("table '{}' not found", p.table)))?;
                    table_meta.sync_direction = Some(*direction);
                }
            }
            if let Some(table_meta) = db.table_meta(&p.table) {
                db.persist_table_meta(&p.table, &table_meta)?;
                if !matches!(
                    p.action,
                    AlterAction::AddColumn(_)
                        | AlterAction::SetRetain { .. }
                        | AlterAction::DropRetain
                ) {
                    db.persist_table_rows(&p.table)?;
                }
                db.allocate_ddl_lsn(|lsn| db.log_alter_table_ddl(&p.table, &table_meta, lsn))?;
            }
            db.clear_statement_cache();
            db.start_maintenance_if_eligible();
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
                    let (rows, examined) = execute_index_scan(
                        db,
                        table,
                        &pick,
                        snapshot,
                        tx,
                        IndexScanAccessMode::Select,
                        resolved_filter.as_ref(),
                        params,
                    )?;
                    db.__bump_rows_examined(examined);
                    let mut result = materialize_rows(
                        rows,
                        resolved_filter.as_ref(),
                        params,
                        schema_columns.as_deref(),
                    )?;
                    let mut pushed: smallvec::SmallVec<[std::borrow::Cow<'static, str>; 4]> =
                        smallvec::SmallVec::new();
                    pushed.extend(
                        pick.pushed_columns
                            .iter()
                            .cloned()
                            .map(std::borrow::Cow::Owned),
                    );
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
                        query_vector_source: None,
                    };
                    return Ok(result);
                } else {
                    // Scan with rejection trace.
                    let rows = scan_rows_for_select(db, table, snapshot, tx)?;
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
                        query_vector_source: None,
                    };
                    return Ok(result);
                }
            }

            let rows = scan_rows_for_select(db, table, snapshot, tx)?;
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
            filter_ctes,
            steps,
            filter,
        } => {
            // GRAPH_TABLE must use this captured read snapshot throughout this arm.
            // Calling db.snapshot() from here would leak live graph state into execute_at_snapshot.
            let snapshot = db.snapshot_for_read();
            let mut predicates_pushed: smallvec::SmallVec<[Cow<'static, str>; 4]> =
                smallvec::SmallVec::new();
            let mut unpinned_start = false;
            let first_step = steps
                .first()
                .ok_or_else(|| Error::PlanError("graph plan missing traversal step".to_string()))?;
            let single_step =
                steps.len() == 1 && first_step.min_depth == 1 && first_step.max_depth == 1;
            let resolved_filter = filter
                .as_ref()
                .map(|filter_expr| {
                    resolve_graph_filter_at_snapshot(
                        db,
                        filter_expr,
                        params,
                        tx,
                        snapshot,
                        filter_ctes,
                    )
                })
                .transpose()?;
            let start_uuids = match resolve_uuid(start_expr, params) {
                Ok(start) => {
                    let starts = vec![start];
                    db.assert_graph_anchor_nodes_readable_in_tx(tx, &starts, snapshot)?;
                    predicates_pushed.push(Cow::Owned(format!("{start_alias}.id")));
                    starts
                }
                Err(Error::PlanError(_))
                    if matches!(
                        start_expr,
                        Expr::Column(contextdb_parser::ast::ColumnRef { table: None, .. })
                    ) =>
                {
                    // Start node not directly specified — check if a subquery or filter can help
                    if let Some(candidate_plan) = start_candidates {
                        predicates_pushed.push(Cow::Owned(format!("{start_alias}.id")));
                        let starts = resolve_graph_start_nodes_from_plan(
                            db,
                            candidate_plan,
                            params,
                            tx,
                            snapshot,
                        )?;
                        db.assert_graph_anchor_nodes_readable_in_tx(tx, &starts, snapshot)?;
                        starts
                    } else if let Some(resolved_filter) = resolved_filter.as_ref() {
                        let resolution = resolve_graph_start_nodes_from_filter(
                            db,
                            resolved_filter,
                            params,
                            tx,
                            snapshot,
                            start_alias,
                        )?;
                        predicates_pushed.extend(resolution.predicates_pushed);
                        if resolution.pinned {
                            resolution.ids
                        } else {
                            unpinned_start = true;
                            let edge_types_ref = if first_step.edge_types.is_empty() {
                                None
                            } else {
                                Some(first_step.edge_types.as_slice())
                            };
                            if single_step {
                                Vec::new()
                            } else {
                                let (starts, examined) = db.graph_start_nodes_for_match_counted(
                                    tx,
                                    edge_types_ref,
                                    first_step.direction,
                                    snapshot,
                                )?;
                                db.__bump_rows_examined(examined);
                                starts
                            }
                        }
                    } else {
                        unpinned_start = true;
                        let edge_types_ref = if first_step.edge_types.is_empty() {
                            None
                        } else {
                            Some(first_step.edge_types.as_slice())
                        };
                        if single_step {
                            Vec::new()
                        } else {
                            let (starts, examined) = db.graph_start_nodes_for_match_counted(
                                tx,
                                edge_types_ref,
                                first_step.direction,
                                snapshot,
                            )?;
                            db.__bump_rows_examined(examined);
                            starts
                        }
                    }
                }
                Err(err) => return Err(err),
            };
            let target_residual = if single_step {
                resolved_filter
                    .as_ref()
                    .map(|expr| {
                        resolve_graph_target_id_residual(expr, params, &steps[0].target_alias)
                    })
                    .transpose()?
                    .flatten()
            } else {
                None
            };
            let target_probe_direction =
                if single_step && unpinned_start && target_residual.is_some() {
                    Some(reverse_graph_probe_direction(first_step.direction))
                } else {
                    None
                };
            let trace_shape = if let Some(direction) = target_probe_direction {
                GraphTraceShape::AdjacencyProbe {
                    index: graph_adjacency_index_label(direction),
                }
            } else {
                graph_trace_shape(
                    single_step,
                    unpinned_start,
                    steps.first().map(|step| step.direction),
                )
            };
            let start_id_predicate = format!("{start_alias}.id");
            if target_residual.is_some()
                && predicates_pushed
                    .iter()
                    .any(|predicate| predicate.as_ref() == start_id_predicate)
            {
                predicates_pushed.push(Cow::Owned(format!("{}.id", steps[0].target_alias)));
            }
            if let (Some(target_id), Some(direction)) = (target_residual, target_probe_direction) {
                predicates_pushed.push(Cow::Owned(format!("{}.id", first_step.target_alias)));
                let edge_types_ref = if first_step.edge_types.is_empty() {
                    None
                } else {
                    Some(first_step.edge_types.as_slice())
                };
                let (res, examined) = db.graph_adjacency_probe_counted(
                    tx,
                    target_id,
                    edge_types_ref,
                    direction,
                    snapshot,
                )?;
                db.__bump_rows_examined(examined);
                let mut frontier = Vec::with_capacity(res.nodes.len());
                for node in res.nodes {
                    frontier.push((
                        HashMap::from([
                            (start_alias.clone(), node.id),
                            (first_step.target_alias.clone(), target_id),
                        ]),
                        target_id,
                        1_u32,
                    ));
                }
                let frontier = filter_graph_frontier(
                    db,
                    dedupe_graph_frontier(frontier, steps),
                    resolved_filter.as_ref(),
                    params,
                    tx,
                    snapshot,
                )?;
                let bfs_bytes = estimate_bfs_working_bytes(&frontier, steps);
                db.accountant().try_allocate_for(
                    bfs_bytes,
                    "bfs_frontier",
                    "graph_bfs",
                    "Reduce traversal depth/fan-out or raise MEMORY_LIMIT before running BFS.",
                )?;
                let rows = project_graph_frontier_rows(frontier, start_alias, steps);
                db.accountant().release(bfs_bytes);
                let mut columns =
                    steps
                        .iter()
                        .fold(vec![format!("{start_alias}.id")], |mut cols, step| {
                            cols.push(format!("{}.id", step.target_alias));
                            cols
                        });
                columns.push("id".to_string());
                columns.push("depth".to_string());
                return Ok(QueryResult {
                    columns,
                    rows: rows?,
                    rows_affected: 0,
                    trace: graph_query_trace(trace_shape, predicates_pushed),
                    cascade: None,
                });
            }
            if single_step && unpinned_start {
                let edge_types_ref = if first_step.edge_types.is_empty() {
                    None
                } else {
                    Some(first_step.edge_types.as_slice())
                };
                let (mut edges, examined) = db.graph_edges_scan_counted(
                    tx,
                    edge_types_ref,
                    first_step.direction,
                    snapshot,
                )?;
                db.__bump_rows_examined(examined);
                edges.sort_unstable();
                let mut frontier = Vec::with_capacity(edges.len());
                for (start, target) in edges {
                    if let Some(target_id) = target_residual
                        && target != target_id
                    {
                        continue;
                    }
                    frontier.push((
                        HashMap::from([
                            (start_alias.clone(), start),
                            (first_step.target_alias.clone(), target),
                        ]),
                        target,
                        1_u32,
                    ));
                }
                let frontier = filter_graph_frontier(
                    db,
                    dedupe_graph_frontier(frontier, steps),
                    resolved_filter.as_ref(),
                    params,
                    tx,
                    snapshot,
                )?;
                let bfs_bytes = estimate_bfs_working_bytes(&frontier, steps);
                db.accountant().try_allocate_for(
                    bfs_bytes,
                    "bfs_frontier",
                    "graph_bfs",
                    "Reduce traversal depth/fan-out or raise MEMORY_LIMIT before running BFS.",
                )?;
                let rows = project_graph_frontier_rows(frontier, start_alias, steps);
                db.accountant().release(bfs_bytes);
                let mut columns =
                    steps
                        .iter()
                        .fold(vec![format!("{start_alias}.id")], |mut cols, step| {
                            cols.push(format!("{}.id", step.target_alias));
                            cols
                        });
                columns.push("id".to_string());
                columns.push("depth".to_string());
                return Ok(QueryResult {
                    columns,
                    rows: rows?,
                    rows_affected: 0,
                    trace: graph_query_trace(trace_shape, predicates_pushed),
                    cascade: None,
                });
            }
            if start_uuids.is_empty() {
                db.__bump_rows_examined(0);
                return Ok(QueryResult {
                    columns: vec!["id".to_string(), "depth".to_string()],
                    rows: vec![],
                    rows_affected: 0,
                    trace: graph_query_trace(trace_shape, predicates_pushed),
                    cascade: None,
                });
            }
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

            let result: Result<QueryResult> = (|| {
                for step in steps {
                    let edge_types_ref = if step.edge_types.is_empty() {
                        None
                    } else {
                        Some(step.edge_types.as_slice())
                    };
                    let mut next: Vec<(HashMap<String, uuid::Uuid>, uuid::Uuid, u32)> = Vec::new();

                    for (bindings, start, base_depth) in &frontier {
                        let (res, examined) = if single_step {
                            db.graph_adjacency_probe_counted(
                                tx,
                                *start,
                                edge_types_ref,
                                step.direction,
                                snapshot,
                            )?
                        } else {
                            db.graph_bfs_counted(
                                tx,
                                *start,
                                edge_types_ref,
                                step.direction,
                                step.min_depth..=step.max_depth,
                                snapshot,
                            )?
                        };
                        db.__bump_rows_examined(examined);
                        for node in res.nodes {
                            if let Some(target_id) = target_residual
                                && node.id != target_id
                            {
                                continue;
                            }
                            let total_depth = (*base_depth).saturating_add(node.depth);
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

                let frontier = filter_graph_frontier(
                    db,
                    frontier,
                    resolved_filter.as_ref(),
                    params,
                    tx,
                    snapshot,
                )?;

                Ok(QueryResult {
                    columns,
                    rows: project_graph_frontier_rows(frontier, start_alias, steps)?,
                    rows_affected: 0,
                    trace: graph_query_trace(trace_shape, predicates_pushed),
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
            let snapshot = db.snapshot_for_read();
            let index = contextdb_core::VectorIndexRef::new(table.clone(), column.clone());
            let mut candidate_trace = None;
            let unrestricted_scan_candidates = candidates
                .as_deref()
                .is_some_and(|plan| is_unrestricted_scan_for_table(plan, table));
            let candidate_bitmap = if unrestricted_scan_candidates {
                candidate_trace = Some(QueryTrace::scan());
                None
            } else if let Some(cands_plan) = candidates {
                let qr = db.with_snapshot_override(snapshot, || {
                    execute_plan(db, cands_plan, params, tx)
                })?;
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
                    let uuid_to_row_id = uuid_to_row_id_map(db, table, snapshot, tx)?;
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

            let mut vector_schema_refs = vec![index.clone()];
            if let Some(source) = row_vector_source_ref(query_expr) {
                vector_schema_refs.push(source);
            }
            let _vector_schema = db.vector_schema_read_many(vector_schema_refs);
            db.assert_vector_index_exists_under_schema_read(&index)?;
            let (query_vec, query_vector_source) =
                resolve_query_vector_from_expr(db, query_expr, params, tx, snapshot)?;
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
                let res = db.with_snapshot_override(snapshot, || {
                    db.semantic_search_with_candidates_under_schema_read_in_tx_with_strategy(
                        tx,
                        semantic_query,
                        candidate_bitmap,
                    )
                });
                db.accountant().release(vector_bytes);
                let (results, used_hnsw) = res?;
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
                    trace: vector_search_trace_with_source(
                        if used_hnsw {
                            "HNSWSearch"
                        } else {
                            "VectorSearch"
                        },
                        candidate_trace,
                        query_vector_source,
                    ),
                    cascade: None,
                });
            }
            let res = db.query_vector_strict_in_tx_with_strategy(
                tx,
                index.clone(),
                &query_vec,
                *k as usize,
                candidate_bitmap.as_ref(),
                snapshot,
            );
            db.accountant().release(vector_bytes);
            let (res, used_hnsw) = res?;

            // Re-materialize: look up actual rows by row_id so SELECT * returns user columns
            let result_row_ids = res.iter().map(|(rid, _)| *rid).collect::<Vec<_>>();
            let result_rows = rows_by_row_id(db, table, &result_row_ids, snapshot, tx)?;
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
                trace: vector_search_trace_with_source(
                    if used_hnsw {
                        "HNSWSearch"
                    } else {
                        "VectorSearch"
                    },
                    candidate_trace,
                    query_vector_source,
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
            // Preserve data-source trace labels through the post-read sort.
            // A plain `Scan` child gets relabeled to `Sort` to match the
            // plan's ORDER BY-without-index expectations.
            if !trace_label_survives_sort(input_result.trace.physical_plan) {
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
        PhysicalPlan::ShowSyncConflictPolicy => {
            // The per-table policy is DECLARED on each table's meta (via
            // `CREATE ... SYNC CONFLICT ...`), which is what the sync apply
            // resolves against. Read it from there so SHOW reports what the sync
            // path actually uses; the runtime layer only carries the deployment
            // default.
            let policies = db.conflict_policies();
            let default_str = conflict_policy_to_string(policies.default);
            let mut rows = vec![vec![Value::Text(default_str)]];
            let mut tables = db.table_names();
            tables.sort();
            for table in tables {
                if let Some(policy) = db.table_meta(&table).and_then(|meta| meta.conflict_policy) {
                    rows.push(vec![Value::Text(format!(
                        "{}={}",
                        table,
                        conflict_policy_to_string(policy)
                    ))]);
                }
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

    let mut insert_meta = db
        .table_meta(&p.table)
        .ok_or_else(|| Error::TableNotFound(p.table.clone()))?;
    let _vector_schema = if !vector_columns_for_meta(&insert_meta).is_empty() {
        let refs = vector_refs_for_meta(&p.table, &insert_meta);
        let guard = db.vector_schema_read_many(refs);
        insert_meta = db
            .table_meta(&p.table)
            .ok_or_else(|| Error::TableNotFound(p.table.clone()))?;
        Some(guard)
    } else {
        None
    };
    // When no column list is provided (INSERT INTO t VALUES (...)),
    // infer column names from table metadata in declaration order.
    let columns: Vec<String> = if p.columns.is_empty() {
        insert_meta.columns.iter().map(|c| c.name.clone()).collect()
    } else {
        p.columns.clone()
    };

    // Statement-scoped snapshot of the committed TxId watermark for TXID bound checks.
    let current_tx_max = Some(db.committed_watermark());
    let route_inserts_to_graph = has_edge_columns(&insert_meta);
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
                db.snapshot_for_read(),
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
                        if db.has_live_vector(existing_row.row_id, db.snapshot_for_read()) {
                            for index in vector_indexes_for_table(db, &p.table) {
                                if db
                                    .vector_store_live_entry_for_row(
                                        &index,
                                        existing_row.row_id,
                                        db.snapshot_for_read(),
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
    let _vector_schema = db.vector_schema_read_table(&p.table);
    let snapshot = db.snapshot_for_read();
    let rows = db.scan_in_tx_raw(txid, &p.table, snapshot)?;
    let rows = db.filter_rows_for_read(&p.table, rows, snapshot)?;
    let resolved_where = p
        .where_clause
        .as_ref()
        .map(|expr| resolve_in_subqueries(db, expr, params, tx))
        .transpose()?;
    let delete_predicates = resolved_where
        .as_ref()
        .map(|expr| collect_simple_equality_predicates(expr, params, false))
        .transpose()?
        .flatten();
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
    if !matched.is_empty()
        && let Some(predicates) = delete_predicates
    {
        db.record_relational_delete_predicate(txid, p.table.clone(), predicates)?;
    }

    Ok(QueryResult::empty_with_affected(matched.len() as u64))
}

fn collect_conditional_update_predicates(
    expr: &Expr,
    params: &HashMap<String, Value>,
) -> Result<Option<Vec<(String, Value)>>> {
    collect_simple_equality_predicates(expr, params, true)
}

fn collect_simple_equality_predicates(
    expr: &Expr,
    params: &HashMap<String, Value>,
    require_non_id: bool,
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
        && (!require_non_id || predicates.iter().any(|(column, _)| column != "id"))
    {
        Ok(Some(predicates))
    } else {
        Ok(None)
    }
}

fn unique_update_lookup_from_predicates(
    meta: &TableMeta,
    predicates: &[(String, Value)],
) -> Option<(Vec<String>, Vec<Value>)> {
    let predicate_value = |wanted: &str| {
        predicates
            .iter()
            .rev()
            .find_map(|(column, value)| (column == wanted).then_some(value))
    };

    for column in &meta.columns {
        if !(column.primary_key || column.unique) {
            continue;
        }
        if let Some(value) = predicate_value(&column.name) {
            return Some((vec![column.name.clone()], vec![value.clone()]));
        }
    }

    for columns in &meta.unique_constraints {
        if columns.is_empty() {
            continue;
        }
        let Some(values) = columns
            .iter()
            .map(|column| predicate_value(column).cloned())
            .collect::<Option<Vec<_>>>()
        else {
            continue;
        };
        return Some((columns.clone(), values));
    }

    None
}

struct TriggerUniqueUpdateLookup<'a> {
    table: &'a str,
    table_meta: Option<&'a TableMeta>,
    snapshot: SnapshotId,
    resolved_where: Option<&'a Expr>,
    conditional_predicates: Option<&'a [(String, Value)]>,
}

fn trigger_bound_unique_update_rows(
    db: &Database,
    txid: TxId,
    lookup: TriggerUniqueUpdateLookup<'_>,
) -> Result<Option<Vec<VersionedRow>>> {
    let (Some(meta), Some(_where_clause), Some(predicates)) = (
        lookup.table_meta,
        lookup.resolved_where,
        lookup.conditional_predicates,
    ) else {
        return Ok(None);
    };
    let Some((columns, values)) = unique_update_lookup_from_predicates(meta, predicates) else {
        return Ok(None);
    };
    let Some(row) =
        db.unique_row_lookup_in_tx(txid, lookup.table, &columns, &values, lookup.snapshot)?
    else {
        return Ok(Some(Vec::new()));
    };
    Ok(Some(vec![row]))
}

fn exec_update(
    db: &Database,
    p: &UpdatePlan,
    params: &HashMap<String, Value>,
    tx: Option<TxId>,
) -> Result<QueryResult> {
    db.check_disk_budget("UPDATE")?;
    let txid = tx.ok_or_else(|| Error::Other("missing tx for update".to_string()))?;
    let table_meta = db.table_meta(&p.table);
    let vector_indexes = table_meta
        .as_ref()
        .map(|meta| vector_refs_for_meta(&p.table, meta))
        .unwrap_or_default();
    let _vector_schema =
        (!vector_indexes.is_empty()).then(|| db.vector_schema_read_many(vector_indexes.clone()));
    let vector_columns = table_meta
        .as_ref()
        .map(|meta| {
            vector_columns_for_meta(meta)
                .into_iter()
                .collect::<HashSet<_>>()
        })
        .unwrap_or_default();
    let has_vector_columns = !vector_columns.is_empty();
    let snapshot = db.snapshot_for_read();
    let resolved_where = p
        .where_clause
        .as_ref()
        .map(|expr| resolve_in_subqueries(db, expr, params, tx))
        .transpose()?;
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
    let trigger_callback_bound = db.trigger_callback_tx_bound_matches(txid);
    let skip_trigger_access_checks =
        trigger_callback_bound && !db.has_access_constraints_for_query();
    let direct_rows = if trigger_callback_bound && !has_vector_columns {
        trigger_bound_unique_update_rows(
            db,
            txid,
            TriggerUniqueUpdateLookup {
                table: &p.table,
                table_meta: table_meta.as_ref(),
                snapshot,
                resolved_where: resolved_where.as_ref(),
                conditional_predicates: conditional_predicates.as_deref(),
            },
        )?
    } else {
        None
    };
    let direct_unique_update = direct_rows.is_some();
    let direct_unique_lookup_exhausts_where = direct_unique_update
        && table_meta
            .as_ref()
            .zip(conditional_predicates.as_ref())
            .and_then(|(meta, predicates)| {
                unique_update_lookup_from_predicates(meta, predicates)
                    .map(|(columns, _)| columns.len() == predicates.len())
            })
            .unwrap_or(false);
    let state_machine_column = table_meta
        .as_ref()
        .and_then(|meta| meta.state_machine.as_ref())
        .map(|sm| sm.column.clone());
    let state_column_assigned = state_machine_column
        .as_ref()
        .is_some_and(|column| p.assignments.iter().any(|(assigned, _)| assigned == column));
    // Use the same IndexScan candidate selection as SELECT when the UPDATE
    // predicate can narrow by an indexed first column. The residual WHERE is
    // still evaluated below, so this only reduces the candidate set.
    let rows = if let Some(rows) = direct_rows {
        rows
    } else if let Some(where_clause) = resolved_where.as_ref() {
        let indexed_rows = table_meta
            .as_ref()
            .and_then(|meta| analyze_filter_for_index(where_clause, &meta.indexes, params).pick)
            .map(|pick| {
                execute_index_scan(
                    db,
                    &p.table,
                    &pick,
                    snapshot,
                    Some(txid),
                    IndexScanAccessMode::Predicate,
                    resolved_where.as_ref(),
                    params,
                )
            })
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
    let matched: Vec<_> = if direct_unique_lookup_exhausts_where {
        rows
    } else {
        rows.into_iter()
            .filter(|r| {
                resolved_where
                    .as_ref()
                    .is_none_or(|w| row_matches(r, w, params).unwrap_or(false))
            })
            .collect()
    };

    if direct_unique_update && trigger_callback_bound && !has_vector_columns {
        let mut affected = 0_u64;
        for row in matched {
            if !skip_trigger_access_checks {
                db.assert_row_write_allowed(&p.table, row.row_id, &row.values, snapshot)?;
            }
            let mut values = row.values.clone();
            for (k, vexpr) in &p.assignments {
                let value = eval_assignment_expr(vexpr, &row.values, params)?;
                values.insert(
                    k.clone(),
                    coerce_value_for_column(db, &p.table, k, value, current_tx_max, Some(txid))?,
                );
            }
            if state_column_assigned {
                validate_update_state_transition(db, &p.table, &row, &values)?;
            }
            let row_uuid = values.get("id").and_then(Value::as_uuid).copied();
            let new_state = table_meta
                .as_ref()
                .and_then(|meta| meta.state_machine.as_ref())
                .and_then(|sm| values.get(&sm.column))
                .and_then(Value::as_text)
                .map(std::borrow::ToOwned::to_owned);
            if !skip_trigger_access_checks {
                db.assert_row_write_allowed(&p.table, row.row_id, &values, snapshot)?;
            }
            let new_row_bytes = estimate_table_row_bytes(db, &p.table, &values)?;
            db.accountant().try_allocate_for(
                new_row_bytes,
                "update",
                "row_replace",
                "Reduce row growth or raise MEMORY_LIMIT before updating this row.",
            )?;
            let propagation_possible = row_uuid.is_some()
                && new_state
                    .as_deref()
                    .is_some_and(|state| db.propagation_rules_can_react(&p.table, state));
            if !propagation_possible {
                let Some(meta) = table_meta.as_ref() else {
                    return Err(Error::TableNotFound(p.table.clone()));
                };
                let committed_row_exists = row.created_tx != txid
                    || db
                        .relational_store()
                        .row_by_id(&p.table, row.row_id, SnapshotId::from_raw_wire(u64::MAX))
                        .is_some();
                let (new_row_id, before_counts, after_counts) = match db
                    .replace_row_after_update_validation_counted(
                        txid,
                        &p.table,
                        row.row_id,
                        values,
                        UpdateReplacementContext {
                            meta,
                            committed_row_exists,
                            created_at: db.trigger_callback_wallclock(),
                        },
                    ) {
                    Ok(outcome) => outcome,
                    Err(err) => {
                        db.accountant().release(new_row_bytes);
                        return Err(err);
                    }
                };
                let _ = new_row_id;
                if let Some(predicates) = conditional_predicates.clone() {
                    db.record_conditional_update_guard(
                        txid,
                        p.table.clone(),
                        row.row_id,
                        predicates,
                        before_counts,
                        after_counts,
                        false,
                    )?;
                }
                affected = affected.saturating_add(1);
                continue;
            }
            let checkpoint = db.write_set_checkpoint(txid)?;
            let before_counts = db.write_set_counts(txid)?;
            let new_row_id = match db
                .replace_row_after_update_validation(txid, &p.table, row.row_id, values, snapshot)
            {
                Ok(row_id) => row_id,
                Err(err) => {
                    db.accountant().release(new_row_bytes);
                    let _ = db.restore_write_set_checkpoint(txid, checkpoint);
                    return Err(err);
                }
            };
            if let Err(err) =
                db.propagate_state_change_if_needed(txid, &p.table, row_uuid, new_state.as_deref())
            {
                db.accountant().release(new_row_bytes);
                let _ = db.restore_write_set_checkpoint(txid, checkpoint);
                return Err(err);
            }
            let _ = new_row_id;
            if let Some(predicates) = conditional_predicates.clone() {
                let after_counts = db.write_set_counts(txid)?;
                db.record_conditional_update_guard(
                    txid,
                    p.table.clone(),
                    row.row_id,
                    predicates,
                    before_counts,
                    after_counts,
                    false,
                )?;
            }
            affected = affected.saturating_add(1);
        }
        return Ok(QueryResult::empty_with_affected(affected));
    }

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
        if !skip_trigger_access_checks {
            db.assert_row_write_allowed(&p.table, row.row_id, &row.values, snapshot)?;
        }
        let mut values = row.values.clone();
        for (k, vexpr) in &p.assignments {
            let value = eval_assignment_expr(vexpr, &row.values, params)?;
            values.insert(
                k.clone(),
                coerce_value_for_column(db, &p.table, k, value, current_tx_max, Some(txid))?,
            );
        }
        if state_column_assigned {
            validate_update_state_transition(db, &p.table, row, &values)?;
        }
        let row_uuid = values.get("id").and_then(Value::as_uuid).copied();
        let new_state = table_meta
            .as_ref()
            .and_then(|meta| meta.state_machine.as_ref())
            .and_then(|sm| values.get(&sm.column))
            .and_then(Value::as_text)
            .map(std::borrow::ToOwned::to_owned);

        if has_vector_columns {
            validate_vector_columns(db, &p.table, &values)?;
        }
        if !skip_trigger_access_checks {
            db.assert_row_write_allowed(&p.table, row.row_id, &values, snapshot)?;
        }
        let assigned_vector_columns: HashSet<String> = if has_vector_columns {
            p.assignments
                .iter()
                .filter_map(|(column, _)| vector_columns.contains(column).then_some(column.clone()))
                .collect()
        } else {
            HashSet::new()
        };
        let assigned_vector_values: Vec<(String, Vec<f32>)> = if has_vector_columns {
            p.assignments
                .iter()
                .filter_map(|(column, _)| match values.get(column) {
                    Some(Value::Vector(vector)) if assigned_vector_columns.contains(column) => {
                        Some((column.clone(), vector.clone()))
                    }
                    _ => None,
                })
                .collect()
        } else {
            Vec::new()
        };
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

        let vector_free_trigger_update = trigger_callback_bound
            && assigned_vector_columns.is_empty()
            && vector_indexes.is_empty();
        let new_row_id = if vector_free_trigger_update {
            match db
                .replace_row_after_update_validation(txid, &p.table, row.row_id, values, snapshot)
            {
                Ok(row_id) => row_id,
                Err(err) => {
                    db.accountant().release(new_row_bytes);
                    let _ = db.restore_write_set_checkpoint(txid, checkpoint);
                    return Err(err);
                }
            }
        } else {
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
            for index in &vector_indexes {
                if assigned_vector_columns.contains(&index.column) {
                    continue;
                }
                if let Err(err) = db.move_vector(txid, index.clone(), row.row_id, new_row_id) {
                    db.accountant().release(new_row_bytes);
                    let _ = db.restore_write_set_checkpoint(txid, checkpoint);
                    return Err(err);
                }
            }
            new_row_id
        };
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
                false,
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
    if let Some(prefix) = reserved_index_prefix(&plan.name) {
        return Err(Error::ReservedIndexName {
            table: plan.table.clone(),
            name: plan.name.clone(),
            prefix: prefix.to_string(),
        });
    }

    // Error precedence: TableNotFound > ColumnNotFound
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
        if !btree_indexable(&col.column_type) {
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

    // All validations passed. Reserve the DDL LSN before publishing the
    // IndexDecl/storage so concurrent DML cannot appear before the index DDL
    // in changes_since().
    db.allocate_ddl_lsn(|lsn| {
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
    if let Some(prefix) = reserved_index_prefix(&plan.name) {
        return Err(Error::ReservedIndexName {
            table: plan.table.clone(),
            name: plan.name.clone(),
            prefix: prefix.to_string(),
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

pub(crate) fn reserved_index_prefix(name: &str) -> Option<&'static str> {
    ["__pk_", "__unique_", "__fk_", "__graph_edge_"]
        .into_iter()
        .find(|prefix| name.starts_with(prefix))
}

fn refresh_auto_indexes_for_table(db: &Database, table: &str) -> Result<()> {
    let Some(meta) = db.table_meta(table) else {
        return Err(Error::TableNotFound(table.to_string()));
    };
    db.replace_table_meta_and_refresh_auto_indexes(table, &meta.clone(), meta)?;
    Ok(())
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
    /// Full pushed prefix in index-column order.
    pushed_columns: Vec<String>,
    /// Equality values for pushed suffix columns, aligned with
    /// `pushed_columns[1..]`.
    suffix_values: Vec<Value>,
    /// The prefix is provably empty before touching the index tree
    /// (contradictory suffix constants, NULL/NaN suffix binds, or an
    /// incoercible leading equality/range value).
    prefix_empty: bool,
}

#[derive(Debug, Clone)]
struct IndexCandidatePlan {
    pick: IndexPick,
    match_count: usize,
    tier: u8,
    creation_index: usize,
}

fn auto_exact_index_supports_pick(
    decl: &contextdb_core::IndexDecl,
    shape: &IndexPredicateShape,
    pushed_columns: usize,
) -> bool {
    decl.kind != contextdb_core::IndexKind::Auto
        || (matches!(
            shape,
            IndexPredicateShape::Equality(_)
                | IndexPredicateShape::InList(_)
                | IndexPredicateShape::IsNull
        ) && pushed_columns == decl.columns.len())
}

/// Top-level decision: did we rewrite to IndexScan?
/// Carries index pick + the rejected candidates (for trace) + residual filter.
struct IndexAnalysis {
    pick: Option<IndexPick>,
    considered: Vec<crate::database::IndexCandidate>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IndexScanAccessMode {
    Select,
    Predicate,
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
    let coerce = |column: &str, v: Value| coerce_index_probe_value(db, table, column, v);
    let mut prefix_empty = pick.prefix_empty;
    let new_shape = match &pick.shape {
        IndexPredicateShape::Equality(v) => match coerce(col, v.clone()) {
            Some(v) => IndexPredicateShape::Equality(v),
            None => {
                prefix_empty = true;
                IndexPredicateShape::Equality(Value::Null)
            }
        },
        IndexPredicateShape::InList(vs) => {
            let coerced = vs.iter().cloned().filter_map(|v| coerce(col, v)).collect();
            IndexPredicateShape::InList(coerced)
        }
        IndexPredicateShape::Range { lower, upper } => {
            let lower = match lower {
                Bound::Included(v) => match coerce(col, v.clone()) {
                    Some(v) => Bound::Included(v),
                    None => {
                        prefix_empty = true;
                        Bound::Unbounded
                    }
                },
                Bound::Excluded(v) => match coerce(col, v.clone()) {
                    Some(v) => Bound::Excluded(v),
                    None => {
                        prefix_empty = true;
                        Bound::Unbounded
                    }
                },
                Bound::Unbounded => Bound::Unbounded,
            };
            let upper = match upper {
                Bound::Included(v) => match coerce(col, v.clone()) {
                    Some(v) => Bound::Included(v),
                    None => {
                        prefix_empty = true;
                        Bound::Unbounded
                    }
                },
                Bound::Excluded(v) => match coerce(col, v.clone()) {
                    Some(v) => Bound::Excluded(v),
                    None => {
                        prefix_empty = true;
                        Bound::Unbounded
                    }
                },
                Bound::Unbounded => Bound::Unbounded,
            };
            IndexPredicateShape::Range { lower, upper }
        }
        IndexPredicateShape::NotEqual(v) => match coerce(col, v.clone()) {
            Some(v) => IndexPredicateShape::NotEqual(v),
            None => {
                prefix_empty = true;
                IndexPredicateShape::NotEqual(Value::Null)
            }
        },
        IndexPredicateShape::IsNull => IndexPredicateShape::IsNull,
        IndexPredicateShape::IsNotNull => IndexPredicateShape::IsNotNull,
    };
    let mut suffix_values = Vec::with_capacity(pick.suffix_values.len());
    for ((column, _), value) in pick.columns.iter().skip(1).zip(pick.suffix_values.iter()) {
        match coerce(column, value.clone()) {
            Some(Value::Null) => {
                prefix_empty = true;
                suffix_values.push(Value::Null);
            }
            Some(Value::Float64(f)) if f.is_nan() => {
                prefix_empty = true;
                suffix_values.push(Value::Float64(f));
            }
            Some(value) => suffix_values.push(value),
            None => {
                prefix_empty = true;
                suffix_values.push(Value::Null);
            }
        }
    }
    Ok(IndexPick {
        name: pick.name.clone(),
        columns: pick.columns.clone(),
        shape: new_shape,
        pushed_column: pick.pushed_column.clone(),
        pushed_columns: pick.pushed_columns.clone(),
        suffix_values,
        prefix_empty,
    })
}

fn coerce_index_probe_value(
    db: &Database,
    table: &str,
    column: &str,
    value: Value,
) -> Option<Value> {
    let coerced = coerce_value_for_column(db, table, column, value, None, None).ok()?;
    let Some(meta) = db.table_meta(table) else {
        return Some(coerced);
    };
    let Some(column_def) = meta
        .columns
        .iter()
        .find(|candidate| candidate.name == column)
    else {
        return Some(coerced);
    };
    match (&column_def.column_type, coerced) {
        (_, Value::Null) => Some(Value::Null),
        (ColumnType::Integer, Value::Int64(v)) => Some(Value::Int64(v)),
        (ColumnType::Integer, Value::Float64(v)) => exact_i64_from_float(v).map(Value::Int64),
        (ColumnType::Real, Value::Float64(v)) => Some(Value::Float64(v)),
        (ColumnType::Real, Value::Int64(v)) => Some(Value::Float64(v as f64)),
        (ColumnType::Text, Value::Text(v)) => Some(Value::Text(v)),
        (ColumnType::Boolean, Value::Bool(v)) => Some(Value::Bool(v)),
        (ColumnType::Uuid, Value::Uuid(v)) => Some(Value::Uuid(v)),
        (ColumnType::Timestamp, Value::Timestamp(v)) => Some(Value::Timestamp(v)),
        (ColumnType::Timestamp, Value::Int64(v)) => Some(Value::Timestamp(v)),
        (ColumnType::TxId, Value::TxId(v)) => Some(Value::TxId(v)),
        _ => None,
    }
}

fn exact_i64_from_float(value: f64) -> Option<i64> {
    if !value.is_finite() || value.fract() != 0.0 {
        return None;
    }
    let candidate = value as i64;
    (compare_values(&Value::Int64(candidate), &Value::Float64(value)) == Some(Ordering::Equal))
        .then_some(candidate)
}

/// Inspect `filter` looking for an eligible predicate on the first column of
/// any declared index. Returns the chosen pick + list of considered/rejected.
fn analyze_filter_for_index(
    filter: &Expr,
    indexes: &[contextdb_core::IndexDecl],
    params: &HashMap<String, Value>,
) -> IndexAnalysis {
    use std::borrow::Cow;
    const EXACT_AUTO_REASON: &str = "auto index supports exact full-key probes only";
    const FEWER_COLUMNS_REASON: &str = "fewer predicate columns matched than chosen index";

    let mut considered: Vec<crate::database::IndexCandidate> = Vec::new();

    // Find each conjunct (split on AND) and map to (column, shape).
    let conjuncts = split_conjuncts(filter);
    let mut conjunct_shapes: Vec<(usize, String, IndexPredicateShape)> = Vec::new();
    for (order, conjunct) in conjuncts.iter().enumerate() {
        if let Some((col, shape)) = classify_index_predicate(conjunct, params) {
            conjunct_shapes.push((order, col, shape));
        }
    }

    // Annotate rejections on indexes that can't apply, for the trace.
    let mut candidates: Vec<IndexCandidatePlan> = Vec::new();
    let mut deferred_exact_auto_rejections: Vec<IndexCandidatePlan> = Vec::new();
    for (i_idx, decl) in indexes.iter().enumerate() {
        let first_col = match decl.columns.first() {
            Some((c, _)) => c.clone(),
            None => continue,
        };
        // Find the most-selective matching conjunct on the first column.
        let matching: Vec<&(usize, String, IndexPredicateShape)> = conjunct_shapes
            .iter()
            .filter(|(_, c, _)| c == &first_col)
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
        let shape = choose_driving_shape(&matching);
        let tier = shape.selectivity_tier();
        let mut pushed_columns = vec![first_col.clone()];
        let mut suffix_values = Vec::new();
        let mut prefix_empty = false;
        if matches!(
            shape,
            IndexPredicateShape::Equality(_)
                | IndexPredicateShape::InList(_)
                | IndexPredicateShape::IsNull
        ) {
            for (column, _) in decl.columns.iter().skip(1) {
                match suffix_equality_value(&conjunct_shapes, column) {
                    SuffixEquality::Absent => break,
                    SuffixEquality::Value(value) => {
                        pushed_columns.push(column.clone());
                        suffix_values.push(value);
                    }
                    SuffixEquality::Contradictory(value) => {
                        pushed_columns.push(column.clone());
                        suffix_values.push(value);
                        prefix_empty = true;
                        break;
                    }
                }
            }
        }
        let match_count = pushed_columns.len();
        let exact_auto_supported = auto_exact_index_supports_pick(decl, &shape, match_count);
        let plan = IndexCandidatePlan {
            pick: IndexPick {
                name: decl.name.clone(),
                columns: decl.columns.clone(),
                shape,
                pushed_column: first_col.clone(),
                pushed_columns,
                suffix_values,
                prefix_empty,
            },
            match_count,
            tier,
            creation_index: i_idx,
        };
        if !exact_auto_supported {
            deferred_exact_auto_rejections.push(plan);
            continue;
        }
        candidates.push(plan);
    }

    // Selection: deepest matched prefix wins; ties break by leading
    // selectivity tier, then declaration/creation order.
    let winner = candidates
        .iter()
        .min_by(|a, b| {
            b.match_count
                .cmp(&a.match_count)
                .then(a.tier.cmp(&b.tier))
                .then(a.creation_index.cmp(&b.creation_index))
        })
        .cloned();

    if let Some(winner) = &winner {
        for candidate in &candidates {
            if candidate.pick.name == winner.pick.name {
                continue;
            }
            let reason = if candidate.match_count < winner.match_count {
                "fewer predicate columns matched than chosen index"
            } else if candidate.tier > winner.tier {
                "lower selectivity than chosen index"
            } else {
                "tied with chosen index; lost by creation order"
            };
            considered.push(crate::database::IndexCandidate {
                name: candidate.pick.name.clone(),
                rejected_reason: Cow::Borrowed(reason),
            });
        }
        for candidate in deferred_exact_auto_rejections {
            let reason = if candidate.match_count < winner.match_count {
                FEWER_COLUMNS_REASON
            } else {
                EXACT_AUTO_REASON
            };
            considered.push(crate::database::IndexCandidate {
                name: candidate.pick.name,
                rejected_reason: Cow::Borrowed(reason),
            });
        }
    } else {
        for candidate in deferred_exact_auto_rejections {
            considered.push(crate::database::IndexCandidate {
                name: candidate.pick.name,
                rejected_reason: Cow::Borrowed(EXACT_AUTO_REASON),
            });
        }
    }

    IndexAnalysis {
        pick: winner.map(|winner| winner.pick),
        considered,
    }
}

fn choose_driving_shape(matching: &[&(usize, String, IndexPredicateShape)]) -> IndexPredicateShape {
    let Some((first_order, _, first_shape)) = matching
        .iter()
        .min_by_key(|(order, _, shape)| (shape.selectivity_tier(), *order))
        .copied()
    else {
        unreachable!("choose_driving_shape requires at least one match");
    };
    if matches!(first_shape, IndexPredicateShape::Range { .. }) {
        let mut range_shapes: Vec<IndexPredicateShape> = matching
            .iter()
            .filter_map(|(_, _, shape)| {
                if matches!(shape, IndexPredicateShape::Range { .. }) {
                    Some(shape.clone())
                } else {
                    None
                }
            })
            .collect();
        if !range_shapes.is_empty() {
            return combine_shapes(std::mem::take(&mut range_shapes));
        }
    }
    let _ = first_order;
    first_shape.clone()
}

enum SuffixEquality {
    Absent,
    Value(Value),
    Contradictory(Value),
}

fn suffix_equality_value(
    conjunct_shapes: &[(usize, String, IndexPredicateShape)],
    column: &str,
) -> SuffixEquality {
    let mut value: Option<Value> = None;
    for (_, col, shape) in conjunct_shapes {
        if col != column {
            continue;
        }
        let IndexPredicateShape::Equality(candidate) = shape else {
            return SuffixEquality::Absent;
        };
        match &value {
            None => value = Some(candidate.clone()),
            Some(existing) if values_constraint_equal(existing, candidate) => {}
            Some(existing) => return SuffixEquality::Contradictory(existing.clone()),
        }
    }
    value.map_or(SuffixEquality::Absent, SuffixEquality::Value)
}

fn values_constraint_equal(left: &Value, right: &Value) -> bool {
    left == right || compare_values(left, right).is_some_and(|ord| ord == Ordering::Equal)
}

fn is_anchor_shape_index_pick(
    db: &Database,
    table: &str,
    pick: &IndexPick,
    filter: &Expr,
    params: &HashMap<String, Value>,
) -> bool {
    if !matches!(pick.shape, IndexPredicateShape::Equality(_)) {
        return false;
    }
    let Some(meta) = db.table_meta(table) else {
        return false;
    };
    let pick_cols: Vec<&str> = pick.columns.iter().map(|(col, _)| col.as_str()).collect();
    let Some(anchor_cols) = unique_anchor_columns_for_pick(&meta, &pick_cols) else {
        return false;
    };
    filter_has_equality_for_columns(filter, params, &anchor_cols)
}

fn unique_anchor_columns_for_pick(meta: &TableMeta, pick_cols: &[&str]) -> Option<Vec<String>> {
    if pick_cols.len() == 1 {
        let column = meta
            .columns
            .iter()
            .find(|column| column.name == pick_cols[0])?;
        if column.primary_key || column.unique {
            return Some(vec![column.name.clone()]);
        }
    }
    for unique in &meta.unique_constraints {
        if unique.len() == pick_cols.len()
            && unique.iter().zip(pick_cols.iter()).all(|(a, b)| a == b)
        {
            return Some(unique.clone());
        }
    }
    None
}

fn filter_has_equality_for_columns(
    filter: &Expr,
    params: &HashMap<String, Value>,
    required_columns: &[String],
) -> bool {
    let mut equality_columns = HashSet::new();
    for conjunct in split_conjuncts(filter) {
        if let Some((column, IndexPredicateShape::Equality(_))) =
            classify_index_predicate(&conjunct, params)
        {
            equality_columns.insert(column);
        }
    }
    required_columns
        .iter()
        .all(|column| equality_columns.contains(column))
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
    access_mode: IndexScanAccessMode,
    residual_filter: Option<&Expr>,
    params: &HashMap<String, Value>,
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
    if pick.prefix_empty {
        return Ok((Vec::new(), 0));
    }

    let indexes = db.relational_store().indexes.read();
    let storage = match indexes
        .get(table)
        .and_then(|table_indexes| table_indexes.get(&pick.name))
    {
        Some(s) => s,
        None => return Ok((Vec::new(), 0)),
    };

    let first_dir = pick
        .columns
        .first()
        .map(|(_, d)| *d)
        .unwrap_or(SortDirection::Asc);

    let wrap_with_dir = |direction: SortDirection, v: Value| -> DirectedValue {
        match direction {
            SortDirection::Asc => DirectedValue::Asc(TotalOrdAsc(v)),
            SortDirection::Desc => DirectedValue::Desc(TotalOrdDesc(v)),
        }
    };
    let wrap = |v: Value| -> DirectedValue { wrap_with_dir(first_dir, v) };

    let suffix_prefix: Vec<DirectedValue> = pick
        .suffix_values
        .iter()
        .zip(pick.columns.iter().skip(1))
        .map(|(value, (_, direction))| wrap_with_dir(*direction, value.clone()))
        .collect();

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

    let is_composite = pick.columns.len() > 1;
    let collect_prefix = |postings: &mut Vec<contextdb_relational::IndexEntry>,
                          examined: &mut u64,
                          prefix: &[DirectedValue]| {
        for (key, entries) in storage
            .tree
            .range::<[DirectedValue], _>((Bound::Included(prefix), Bound::Unbounded))
        {
            if !key.starts_with(prefix) {
                break;
            }
            for e in entries {
                *examined += 1;
                if e.visible_at(snapshot) {
                    postings.push(e.clone());
                }
            }
        }
    };
    let make_probe_prefix = |leading: Value, probe_prefix: &mut Vec<DirectedValue>| {
        probe_prefix.clear();
        probe_prefix.push(wrap(leading));
        probe_prefix.extend(suffix_prefix.iter().cloned());
    };

    if storage.exact_only() {
        let exact_key_complete = 1 + suffix_prefix.len() == storage.columns.len();
        let collect_exact_key = |postings: &mut Vec<contextdb_relational::IndexEntry>,
                                 examined: &mut u64,
                                 key: &[DirectedValue]| {
            let key = key.to_vec();
            if let Some(entries) = storage.exact_postings(&key) {
                for e in entries {
                    *examined += 1;
                    if e.visible_at(snapshot) {
                        postings.push(e.clone());
                    }
                }
            }
        };
        match &pick.shape {
            IndexPredicateShape::Equality(v) if exact_key_complete => {
                let mut probe_prefix = Vec::with_capacity(1 + suffix_prefix.len());
                make_probe_prefix(v.clone(), &mut probe_prefix);
                collect_exact_key(&mut postings, &mut rows_examined, &probe_prefix);
            }
            IndexPredicateShape::InList(vs) if exact_key_complete => {
                let mut probe_prefix = Vec::with_capacity(1 + suffix_prefix.len());
                for v in vs {
                    make_probe_prefix(v.clone(), &mut probe_prefix);
                    collect_exact_key(&mut postings, &mut rows_examined, &probe_prefix);
                }
            }
            IndexPredicateShape::IsNull if exact_key_complete => {
                let mut probe_prefix = Vec::with_capacity(1 + suffix_prefix.len());
                make_probe_prefix(Value::Null, &mut probe_prefix);
                collect_exact_key(&mut postings, &mut rows_examined, &probe_prefix);
            }
            _ => {
                drop(indexes);
                let rows = scan_rows_for_select(db, table, snapshot, tx)?;
                let rows_examined = rows.len() as u64;
                return Ok((rows, rows_examined));
            }
        }
    } else {
        match &pick.shape {
            IndexPredicateShape::Equality(v) => {
                if is_composite {
                    let mut probe_prefix = Vec::with_capacity(1 + suffix_prefix.len());
                    make_probe_prefix(v.clone(), &mut probe_prefix);
                    collect_prefix(&mut postings, &mut rows_examined, &probe_prefix);
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
                let mut probe_prefix = Vec::with_capacity(1 + suffix_prefix.len());
                for v in vs {
                    if is_composite {
                        make_probe_prefix(v.clone(), &mut probe_prefix);
                        collect_prefix(&mut postings, &mut rows_examined, &probe_prefix);
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
                    let lower_key = match lower {
                        Bound::Included(v) => Bound::Included(vec![wrap(v.clone())]),
                        Bound::Excluded(v) => Bound::Excluded(vec![wrap(v.clone())]),
                        Bound::Unbounded => Bound::Unbounded,
                    };
                    // Composite + range on the leading column cannot push suffix
                    // equalities; walk the ordered leading range and stop once
                    // the first component is beyond the upper bound.
                    for (key, entries) in storage.tree.range((lower_key, Bound::Unbounded)) {
                        let Some(first) = key.first() else { continue };
                        let in_lower = match lower {
                            Bound::Unbounded => true,
                            Bound::Included(v) => first >= &wrap(v.clone()),
                            Bound::Excluded(v) => first > &wrap(v.clone()),
                        };
                        if !in_lower {
                            continue;
                        }
                        let in_upper = match upper {
                            Bound::Unbounded => true,
                            Bound::Included(v) => first <= &wrap(v.clone()),
                            Bound::Excluded(v) => first < &wrap(v.clone()),
                        };
                        if !in_upper {
                            break;
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
                if is_composite {
                    let mut probe_prefix = Vec::with_capacity(1 + suffix_prefix.len());
                    make_probe_prefix(Value::Null, &mut probe_prefix);
                    collect_prefix(&mut postings, &mut rows_examined, &probe_prefix);
                } else {
                    let k = vec![wrap(Value::Null)];
                    collect_range(
                        &mut postings,
                        &mut rows_examined,
                        Bound::Included(k.clone()),
                        Bound::Included(k),
                    );
                }
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
    }

    // Now fetch base rows by row_id while preserving index-order. The index
    // already enumerates postings in index sort order; rows[] preserve it.
    drop(indexes);
    let row_ids: Vec<RowId> = postings.iter().map(|p| p.row_id).collect();
    let mut out: Vec<VersionedRow> = Vec::with_capacity(row_ids.len());
    if !row_ids.is_empty() {
        for rid in &row_ids {
            if let Some(row) = db.relational_store().row_by_id(table, *rid, snapshot) {
                out.push(row);
            }
        }
    }
    // Layer tx-scoped inserts / deletes on top, matching the semantics of
    // scan_with_tx.
    if let Some(tx_id) = tx {
        let overlay = db.index_scan_tx_overlay(tx_id, table, &pick.pushed_column, &pick.shape)?;
        let deleted_row_ids = overlay.deleted_row_ids;
        out.retain(|row| !deleted_row_ids.contains(&row.row_id));
        out.extend(overlay.matching_inserts);
    }
    let anchor_shape = access_mode == IndexScanAccessMode::Select
        && residual_filter
            .map(|filter| is_anchor_shape_index_pick(db, table, pick, filter, params))
            .unwrap_or(false);
    if anchor_shape {
        if let Some(filter) = residual_filter {
            out.retain(|row| row_matches(row, filter, params).unwrap_or(false));
        }
        out = db.filter_rows_for_anchor_read_in_tx(tx, table, out, snapshot)?;
    } else {
        out = db.filter_rows_for_read_in_tx(tx, table, out, snapshot)?;
    }
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
        if decl.kind == contextdb_core::IndexKind::Auto {
            return false;
        }
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
        pushed_columns: vec![decl.columns[0].0.clone()],
        suffix_values: Vec::new(),
        prefix_empty: false,
    };
    let (rows, examined) = execute_index_scan(
        db,
        table,
        &pick,
        snapshot,
        tx,
        IndexScanAccessMode::Select,
        resolved_filter.as_ref(),
        params,
    )?;
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
        query_vector_source: None,
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
    if decl.kind == contextdb_core::IndexKind::Auto {
        return false;
    }
    // Shape guard: IndexScan with InList or NotEqual shape on the leading
    // indexed column walks fragmented posting-list ranges, so rows are
    // emitted per-value, not globally sorted. Refuse sort elision for those
    // shapes even when values are bound parameters.
    if let Some(filter_expr) = filter.as_ref()
        && let Some(leading_col) = decl.columns.first().map(|(c, _)| c.as_str())
        && leading_filter_has_fragmented_order_shape(filter_expr, leading_col)
    {
        return false;
    }
    // Determine how many leading index columns the WHERE filter pins to a
    // single equality. Those columns are effectively "used up" by the
    // IndexScan's range; subsequent ORDER BY keys matching the remaining
    // index columns still elide the Sort.
    let pinned_prefix_len = count_equality_prefix(filter.as_ref(), &decl.columns);
    if decl.columns.len() >= keys.len()
        && decl
            .columns
            .iter()
            .zip(keys.iter())
            .all(|((col, dir), k)| match &k.expr {
                Expr::Column(r) => r.column == *col && core_dir_matches_ast(*dir, k.direction),
                _ => false,
            })
    {
        return true;
    }
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

fn leading_filter_has_fragmented_order_shape(filter: &Expr, leading_col: &str) -> bool {
    split_conjuncts(filter)
        .iter()
        .any(|conjunct| match conjunct {
            Expr::InList {
                expr,
                negated: false,
                ..
            } => extract_simple_col_ref(expr).as_deref() == Some(leading_col),
            Expr::BinaryOp {
                left,
                op: BinOp::Neq,
                ..
            } => extract_simple_col_ref(left).as_deref() == Some(leading_col),
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
    let edge_bytes = if meta.as_ref().is_some_and(has_edge_columns) {
        rows.iter().fold(0usize, |acc, row| {
            match (
                row.values.get("source_id").and_then(Value::as_uuid),
                row.values.get("target_id").and_then(Value::as_uuid),
                row.values.get("edge_type").and_then(Value::as_text),
            ) {
                (Some(_), Some(_), Some(edge_type)) => acc.saturating_add(
                    96 + edge_type.len().saturating_mul(16)
                        + estimate_row_value_bytes(&HashMap::new()),
                ),
                _ => acc,
            }
        })
    } else {
        0
    };
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

fn scan_rows_for_select(
    db: &Database,
    table: &str,
    snapshot: SnapshotId,
    tx: Option<TxId>,
) -> Result<Vec<VersionedRow>> {
    if let Some(tx) = tx {
        let rows = db.scan_in_tx_raw(tx, table, snapshot)?;
        db.filter_rows_for_read_in_tx(Some(tx), table, rows, snapshot)
    } else {
        db.scan(table, snapshot)
    }
}

fn rows_by_row_id(
    db: &Database,
    table: &str,
    row_ids: &[RowId],
    snapshot: SnapshotId,
    tx: Option<TxId>,
) -> Result<Vec<VersionedRow>> {
    if row_ids.is_empty() {
        return Ok(Vec::new());
    }

    let mut rows = Vec::with_capacity(row_ids.len());
    for row_id in row_ids {
        if let Ok(row) = db.find_row_by_id_in_tx(tx, table, *row_id, snapshot) {
            rows.push(row);
        }
    }
    Ok(rows)
}

fn uuid_to_row_id_map(
    db: &Database,
    table: &str,
    snapshot: SnapshotId,
    tx: Option<TxId>,
) -> Result<HashMap<uuid::Uuid, RowId>> {
    let rows = scan_rows_for_select(db, table, snapshot, tx)?;
    Ok(rows
        .iter()
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

fn trace_label_survives_sort(physical_plan: &str) -> bool {
    matches!(
        physical_plan,
        "IndexScan" | "AdjacencyProbe" | "EdgesScan" | "GraphBfs"
    )
}

fn vector_search_trace_with_source(
    operator: &'static str,
    candidate_trace: Option<QueryTrace>,
    query_vector_source: Option<contextdb_core::VectorIndexRef>,
) -> QueryTrace {
    let mut trace = vector_search_trace(operator, candidate_trace);
    trace.query_vector_source = query_vector_source;
    trace
}

fn row_vector_source_ref(expr: &Expr) -> Option<contextdb_core::VectorIndexRef> {
    match expr {
        Expr::RowVectorSource { table, column, .. } => Some(contextdb_core::VectorIndexRef::new(
            table.clone(),
            column.clone(),
        )),
        _ => None,
    }
}

fn is_unrestricted_scan_for_table(plan: &PhysicalPlan, table: &str) -> bool {
    matches!(
        plan,
        PhysicalPlan::Scan {
            table: scan_table,
            filter: None,
            ..
        } if scan_table == table
    )
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

fn resolve_graph_filter_at_snapshot(
    db: &Database,
    expr: &Expr,
    params: &HashMap<String, Value>,
    tx: Option<TxId>,
    snapshot: SnapshotId,
    ctes: &[Cte],
) -> Result<Expr> {
    db.with_snapshot_override(snapshot, || {
        resolve_in_subqueries_with_ctes(db, expr, params, tx, ctes)
    })
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
                    contextdb_parser::ast::FromItem::Table { name, alias } => {
                        Some(alias.clone().unwrap_or_else(|| name.clone()))
                    }
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

#[derive(Clone, Copy)]
enum GraphTraceShape {
    AdjacencyProbe { index: &'static str },
    EdgesScan { rejected_index: &'static str },
    GraphBfs,
}

struct GraphStartResolution {
    ids: Vec<uuid::Uuid>,
    predicates_pushed: smallvec::SmallVec<[Cow<'static, str>; 4]>,
    pinned: bool,
}

type GraphFrontierRow = (HashMap<String, uuid::Uuid>, uuid::Uuid, u32);

fn graph_trace_shape(
    single_step: bool,
    unpinned_start: bool,
    direction: Option<Direction>,
) -> GraphTraceShape {
    if !single_step {
        return GraphTraceShape::GraphBfs;
    }
    let index = graph_adjacency_index_label(direction.unwrap_or(Direction::Outgoing));
    if unpinned_start {
        GraphTraceShape::EdgesScan {
            rejected_index: index,
        }
    } else {
        GraphTraceShape::AdjacencyProbe { index }
    }
}

fn graph_adjacency_index_label(direction: Direction) -> &'static str {
    match direction {
        Direction::Outgoing | Direction::Both => "forward_adj",
        Direction::Incoming => "reverse_adj",
    }
}

fn reverse_graph_probe_direction(direction: Direction) -> Direction {
    match direction {
        Direction::Outgoing => Direction::Incoming,
        Direction::Incoming => Direction::Outgoing,
        Direction::Both => Direction::Both,
    }
}

fn graph_query_trace(
    shape: GraphTraceShape,
    predicates_pushed: smallvec::SmallVec<[Cow<'static, str>; 4]>,
) -> QueryTrace {
    match shape {
        GraphTraceShape::AdjacencyProbe { index } => QueryTrace {
            physical_plan: "AdjacencyProbe",
            index_used: Some(index.to_string()),
            predicates_pushed,
            ..Default::default()
        },
        GraphTraceShape::EdgesScan { rejected_index } => {
            let mut indexes_considered: smallvec::SmallVec<[crate::database::IndexCandidate; 4]> =
                smallvec::SmallVec::new();
            indexes_considered.push(crate::database::IndexCandidate {
                name: rejected_index.to_string(),
                rejected_reason: Cow::Borrowed("no pinned vertex"),
            });
            QueryTrace {
                physical_plan: "EdgesScan",
                indexes_considered,
                ..Default::default()
            }
        }
        GraphTraceShape::GraphBfs => QueryTrace {
            physical_plan: "GraphBfs",
            predicates_pushed,
            ..Default::default()
        },
    }
}

/// Resolve start nodes for a graph traversal from a WHERE filter like
/// `a.name = 'entity-0'`. Uses a matching relational index when one exists and
/// otherwise reports the full row scan needed to resolve the start vertices.
fn resolve_graph_start_nodes_from_filter(
    db: &Database,
    filter: &Expr,
    params: &HashMap<String, Value>,
    tx: Option<TxId>,
    snapshot: contextdb_core::SnapshotId,
    start_alias: &str,
) -> Result<GraphStartResolution> {
    let mut predicates_pushed: smallvec::SmallVec<[Cow<'static, str>; 4]> =
        smallvec::SmallVec::new();

    let Some(start_filter) = graph_start_resolution_filter(filter, start_alias) else {
        return Ok(GraphStartResolution {
            ids: Vec::new(),
            predicates_pushed,
            pinned: false,
        });
    };
    if let Some(ids) =
        resolve_graph_start_ids_from_filter(db, &start_filter, params, tx, snapshot, start_alias)?
    {
        predicates_pushed.push(Cow::Owned(format!("{start_alias}.id")));
        return Ok(GraphStartResolution {
            ids,
            predicates_pushed,
            pinned: true,
        });
    }

    let start_columns = graph_start_columns(&start_filter, start_alias);
    if start_columns.is_empty() {
        return Ok(GraphStartResolution {
            ids: Vec::new(),
            predicates_pushed,
            pinned: false,
        });
    }
    if graph_start_filter_needs_unpinned_null_semantics(&start_filter, start_alias) {
        return Ok(GraphStartResolution {
            ids: Vec::new(),
            predicates_pushed,
            pinned: false,
        });
    }
    for column in &start_columns {
        predicates_pushed.push(Cow::Owned(format!("{start_alias}.{column}")));
    }

    let mut candidate_ids = BTreeSet::new();
    for table_name in db.table_names() {
        let meta = match db.table_meta(&table_name) {
            Some(m) => m,
            None => continue,
        };
        let has_start_col = meta.columns.iter().any(|c| start_columns.contains(&c.name));
        let has_id = has_exact_column_type(&meta, "id", &ColumnType::Uuid);
        if !has_start_col || !has_id {
            continue;
        }

        let analysis = analyze_filter_for_index(&start_filter, &meta.indexes, params);
        if let Some(pick) = analysis.pick.as_ref() {
            let (rows, examined) = execute_index_scan(
                db,
                &table_name,
                pick,
                snapshot,
                tx,
                IndexScanAccessMode::Select,
                None,
                params,
            )?;
            db.__bump_rows_examined(examined);
            for row in rows {
                if let Some(Value::Uuid(id)) = row.values.get("id") {
                    candidate_ids.insert(*id);
                }
            }
        } else {
            let rows = scan_rows_for_select(db, &table_name, snapshot, tx)?;
            db.__bump_rows_examined(rows.len() as u64);
            for row in rows {
                if let Some(Value::Uuid(id)) = row.values.get("id") {
                    candidate_ids.insert(*id);
                }
            }
        }
    }
    let mut ids = Vec::new();
    for id in candidate_ids {
        let bindings = HashMap::from([(start_alias.to_string(), id)]);
        if graph_filter_matches_bindings(db, &start_filter, params, tx, snapshot, &bindings)? {
            ids.push(id);
        }
    }
    db.assert_graph_anchor_nodes_readable_in_tx(tx, &ids, snapshot)?;
    Ok(GraphStartResolution {
        ids,
        predicates_pushed,
        pinned: true,
    })
}

fn resolve_graph_start_nodes_from_plan(
    db: &Database,
    plan: &PhysicalPlan,
    params: &HashMap<String, Value>,
    tx: Option<TxId>,
    snapshot: contextdb_core::SnapshotId,
) -> Result<Vec<uuid::Uuid>> {
    let result = db.with_snapshot_override(snapshot, || execute_plan(db, plan, params, tx))?;
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
    db: &Database,
    filter: &Expr,
    params: &HashMap<String, Value>,
    tx: Option<TxId>,
    snapshot: contextdb_core::SnapshotId,
    start_alias: &str,
) -> Result<Option<Vec<uuid::Uuid>>> {
    match filter {
        Expr::BinaryOp {
            left,
            op: BinOp::Eq,
            right,
        } if is_graph_start_column_ref(left, start_alias, "id")
            || is_graph_start_column_ref(right, start_alias, "id") =>
        {
            let id = if is_graph_start_column_ref(left, start_alias, "id") {
                resolve_graph_static_uuid_expr(right, params, "graph start identifier in filter")?
            } else {
                resolve_graph_static_uuid_expr(left, params, "graph start identifier in filter")?
            };
            let Some(id) = id else {
                return Ok(None);
            };
            let ids = vec![id];
            db.assert_graph_anchor_nodes_readable_in_tx(tx, &ids, snapshot)?;
            Ok(Some(ids))
        }
        Expr::InList {
            expr,
            list,
            negated: false,
        } if is_graph_start_column_ref(expr, start_alias, "id") => {
            let mut ids = Vec::with_capacity(list.len());
            for item in list {
                let Some(id) = resolve_graph_static_uuid_expr(
                    item,
                    params,
                    "graph start identifier in filter",
                )?
                else {
                    return Ok(None);
                };
                ids.push(id);
            }
            db.assert_graph_anchor_nodes_readable_in_tx(tx, &ids, snapshot)?;
            Ok(Some(ids))
        }
        Expr::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            let left_ids =
                resolve_graph_start_ids_from_filter(db, left, params, tx, snapshot, start_alias)?;
            let right_ids =
                resolve_graph_start_ids_from_filter(db, right, params, tx, snapshot, start_alias)?;
            match (left_ids, right_ids) {
                (Some(left_ids), Some(right_ids)) => {
                    let right_ids = right_ids.into_iter().collect::<BTreeSet<_>>();
                    let ids = left_ids
                        .into_iter()
                        .filter(|id| right_ids.contains(id))
                        .collect::<Vec<_>>();
                    db.assert_graph_anchor_nodes_readable_in_tx(tx, &ids, snapshot)?;
                    Ok(Some(ids))
                }
                (Some(_), None) | (None, Some(_)) | (None, None) => Ok(None),
            }
        }
        Expr::BinaryOp {
            left,
            op: BinOp::Or,
            right,
        } => {
            let left_ids =
                resolve_graph_start_ids_from_filter(db, left, params, tx, snapshot, start_alias)?;
            let right_ids =
                resolve_graph_start_ids_from_filter(db, right, params, tx, snapshot, start_alias)?;
            match (left_ids, right_ids) {
                (Some(left_ids), Some(right_ids)) => {
                    let ids = left_ids
                        .into_iter()
                        .chain(right_ids)
                        .collect::<BTreeSet<_>>()
                        .into_iter()
                        .collect::<Vec<_>>();
                    db.assert_graph_anchor_nodes_readable_in_tx(tx, &ids, snapshot)?;
                    Ok(Some(ids))
                }
                (Some(_), None) | (None, Some(_)) | (None, None) => Ok(None),
            }
        }
        Expr::UnaryOp { .. } => Ok(None),
        _ => Ok(None),
    }
}

fn resolve_graph_target_id_residual(
    filter: &Expr,
    params: &HashMap<String, Value>,
    target_alias: &str,
) -> Result<Option<uuid::Uuid>> {
    for conjunct in split_conjuncts(filter) {
        if let Expr::BinaryOp {
            left,
            op: BinOp::Eq,
            right,
        } = conjunct
        {
            if is_graph_alias_column_ref(&left, target_alias, "id") {
                return resolve_graph_static_uuid_expr(
                    &right,
                    params,
                    "graph target identifier in filter",
                );
            }
            if is_graph_alias_column_ref(&right, target_alias, "id") {
                return resolve_graph_static_uuid_expr(
                    &left,
                    params,
                    "graph target identifier in filter",
                );
            }
        }
    }
    Ok(None)
}

fn filter_graph_frontier(
    db: &Database,
    frontier: Vec<GraphFrontierRow>,
    filter: Option<&Expr>,
    params: &HashMap<String, Value>,
    tx: Option<TxId>,
    snapshot: contextdb_core::SnapshotId,
) -> Result<Vec<GraphFrontierRow>> {
    let Some(filter) = filter else {
        return Ok(frontier);
    };

    let mut filtered = Vec::with_capacity(frontier.len());
    let mut cache = HashMap::new();
    for (bindings, current, depth) in frontier {
        if graph_eval_bool_expr(db, filter, params, tx, snapshot, &bindings, &mut cache)?
            .unwrap_or(false)
        {
            filtered.push((bindings, current, depth));
        }
    }
    Ok(filtered)
}

fn graph_filter_matches_bindings(
    db: &Database,
    filter: &Expr,
    params: &HashMap<String, Value>,
    tx: Option<TxId>,
    snapshot: contextdb_core::SnapshotId,
    bindings: &HashMap<String, uuid::Uuid>,
) -> Result<bool> {
    let mut cache = HashMap::new();
    Ok(
        graph_eval_bool_expr(db, filter, params, tx, snapshot, bindings, &mut cache)?
            .unwrap_or(false),
    )
}

fn graph_eval_bool_expr(
    db: &Database,
    expr: &Expr,
    params: &HashMap<String, Value>,
    tx: Option<TxId>,
    snapshot: contextdb_core::SnapshotId,
    bindings: &HashMap<String, uuid::Uuid>,
    cache: &mut HashMap<(uuid::Uuid, String), Vec<Value>>,
) -> Result<Option<bool>> {
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            BinOp::Eq | BinOp::Neq | BinOp::Lt | BinOp::Lte | BinOp::Gt | BinOp::Gte => {
                let left = graph_eval_values(db, left, params, tx, snapshot, bindings, cache)?;
                let right = graph_eval_values(db, right, params, tx, snapshot, bindings, cache)?;
                graph_compare_any(&left, &right, *op)
            }
            BinOp::And => {
                let left = graph_eval_bool_expr(db, left, params, tx, snapshot, bindings, cache)?;
                if left == Some(false) {
                    return Ok(Some(false));
                }
                let right = graph_eval_bool_expr(db, right, params, tx, snapshot, bindings, cache)?;
                Ok(match (left, right) {
                    (Some(true), Some(true)) => Some(true),
                    (Some(true), other) => other,
                    (None, Some(false)) => Some(false),
                    (None, Some(true)) | (None, None) => None,
                    (Some(false), _) => Some(false),
                })
            }
            BinOp::Or => {
                let left = graph_eval_bool_expr(db, left, params, tx, snapshot, bindings, cache)?;
                if left == Some(true) {
                    return Ok(Some(true));
                }
                let right = graph_eval_bool_expr(db, right, params, tx, snapshot, bindings, cache)?;
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
        } => Ok(
            graph_eval_bool_expr(db, operand, params, tx, snapshot, bindings, cache)?
                .map(|value| !value),
        ),
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let needles = graph_eval_values(db, expr, params, tx, snapshot, bindings, cache)?;
            let mut candidates = Vec::new();
            for item in list {
                candidates.extend(graph_eval_values(
                    db, item, params, tx, snapshot, bindings, cache,
                )?);
            }
            let matched = needles.iter().any(|needle| {
                *needle != Value::Null
                    && candidates.iter().any(|candidate| {
                        *candidate != Value::Null
                            && (matches!(compare_values(needle, candidate), Some(Ordering::Equal))
                                || needle == candidate)
                    })
            });
            Ok(Some(if *negated { !matched } else { matched }))
        }
        Expr::Like {
            expr,
            pattern,
            negated,
        } => {
            let values = graph_eval_values(db, expr, params, tx, snapshot, bindings, cache)?;
            let patterns = graph_eval_values(db, pattern, params, tx, snapshot, bindings, cache)?;
            let matched = values.iter().any(|value| {
                patterns.iter().any(|pattern| match (value, pattern) {
                    (Value::Text(value), Value::Text(pattern)) => like_matches(value, pattern),
                    _ => false,
                })
            });
            Ok(Some(if *negated { !matched } else { matched }))
        }
        Expr::IsNull { expr, negated } => {
            let values = graph_eval_values(db, expr, params, tx, snapshot, bindings, cache)?;
            let is_null = values.iter().all(|value| *value == Value::Null);
            Ok(Some(if *negated { !is_null } else { is_null }))
        }
        Expr::FunctionCall { .. } => {
            match graph_eval_values(db, expr, params, tx, snapshot, bindings, cache)?
                .into_iter()
                .next()
                .unwrap_or(Value::Null)
            {
                Value::Bool(value) => Ok(Some(value)),
                Value::Null => Ok(None),
                _ => Err(Error::PlanError(format!(
                    "unsupported graph WHERE expression: {:?}",
                    expr
                ))),
            }
        }
        Expr::InSubquery { .. } => Err(Error::PlanError(
            "IN (subquery) must be resolved before graph execution".to_string(),
        )),
        _ => Err(Error::PlanError(format!(
            "unsupported graph WHERE expression: {:?}",
            expr
        ))),
    }
}

fn graph_eval_values(
    db: &Database,
    expr: &Expr,
    params: &HashMap<String, Value>,
    tx: Option<TxId>,
    snapshot: contextdb_core::SnapshotId,
    bindings: &HashMap<String, uuid::Uuid>,
    cache: &mut HashMap<(uuid::Uuid, String), Vec<Value>>,
) -> Result<Vec<Value>> {
    match expr {
        Expr::Literal(_) | Expr::Parameter(_) => Ok(vec![resolve_expr(expr, params)?]),
        Expr::Column(column) => graph_column_values(db, column, tx, snapshot, bindings, cache),
        Expr::BinaryOp { left, op, right } => {
            let left = graph_eval_values(db, left, params, tx, snapshot, bindings, cache)?;
            let right = graph_eval_values(db, right, params, tx, snapshot, bindings, cache)?;
            let mut values = Vec::new();
            for left in &left {
                for right in &right {
                    values.push(eval_binary_op(op, left, right)?);
                }
            }
            Ok(if values.is_empty() {
                vec![Value::Null]
            } else {
                values
            })
        }
        Expr::UnaryOp { op, operand } => {
            let values = graph_eval_values(db, operand, params, tx, snapshot, bindings, cache)?;
            values
                .into_iter()
                .map(|value| match op {
                    UnaryOp::Not => Ok(Value::Bool(!value_to_bool(&value))),
                    UnaryOp::Neg => match value {
                        Value::Int64(v) => Ok(Value::Int64(-v)),
                        Value::Float64(v) => Ok(Value::Float64(-v)),
                        _ => Err(Error::PlanError(
                            "cannot negate non-numeric graph value".to_string(),
                        )),
                    },
                })
                .collect()
        }
        Expr::FunctionCall { name, args } => {
            let mut values = Vec::with_capacity(args.len());
            for arg in args {
                let mut arg_values =
                    graph_eval_values(db, arg, params, tx, snapshot, bindings, cache)?;
                values.push(arg_values.pop().unwrap_or(Value::Null));
            }
            Ok(vec![eval_function(name, &values)?])
        }
        Expr::InList { .. } | Expr::Like { .. } | Expr::IsNull { .. } => Ok(vec![Value::Bool(
            graph_eval_bool_expr(db, expr, params, tx, snapshot, bindings, cache)?.unwrap_or(false),
        )]),
        Expr::InSubquery { .. } => Err(Error::PlanError(
            "IN (subquery) must be resolved before graph execution".to_string(),
        )),
        Expr::CosineDistance { .. } | Expr::RowVectorSource { .. } => Err(Error::PlanError(
            format!("unsupported graph WHERE value expression: {:?}", expr),
        )),
    }
}

fn graph_column_values(
    db: &Database,
    column: &ColumnRef,
    tx: Option<TxId>,
    snapshot: contextdb_core::SnapshotId,
    bindings: &HashMap<String, uuid::Uuid>,
    cache: &mut HashMap<(uuid::Uuid, String), Vec<Value>>,
) -> Result<Vec<Value>> {
    if let Some(alias) = column.table.as_ref() {
        let Some(node) = bindings.get(alias) else {
            return Ok(vec![Value::Null]);
        };
        return graph_node_column_values(db, *node, &column.column, tx, snapshot, cache);
    }

    if column.column == "id" {
        let values = bindings
            .values()
            .copied()
            .map(Value::Uuid)
            .collect::<Vec<_>>();
        return Ok(if values.is_empty() {
            vec![Value::Null]
        } else {
            values
        });
    }

    if bindings.len() == 1 {
        let node = *bindings.values().next().expect("length checked above");
        return graph_node_column_values(db, node, &column.column, tx, snapshot, cache);
    }

    Ok(vec![Value::Null])
}

fn graph_node_column_values(
    db: &Database,
    node: uuid::Uuid,
    column: &str,
    tx: Option<TxId>,
    snapshot: contextdb_core::SnapshotId,
    cache: &mut HashMap<(uuid::Uuid, String), Vec<Value>>,
) -> Result<Vec<Value>> {
    if column == "id" {
        return Ok(vec![Value::Uuid(node)]);
    }

    let key = (node, column.to_string());
    if let Some(values) = cache.get(&key) {
        return Ok(values.clone());
    }

    let values = db.readable_graph_node_column_values(tx, node, column, snapshot)?;
    let values = if values.is_empty() {
        vec![Value::Null]
    } else {
        values
    };
    cache.insert(key, values.clone());
    Ok(values)
}

fn graph_compare_any(left: &[Value], right: &[Value], op: BinOp) -> Result<Option<bool>> {
    let mut saw_unknown = false;
    for left in left {
        for right in right {
            if *left == Value::Null || *right == Value::Null {
                saw_unknown = true;
                continue;
            }
            let matched = match op {
                BinOp::Eq => compare_values(left, right) == Some(Ordering::Equal) || left == right,
                BinOp::Neq => {
                    !(compare_values(left, right) == Some(Ordering::Equal) || left == right)
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
                BinOp::And | BinOp::Or => unreachable!(),
            };
            if matched {
                return Ok(Some(true));
            }
        }
    }
    Ok(if saw_unknown { None } else { Some(false) })
}

fn graph_uuid_from_value(value: Value, context: &str) -> Result<uuid::Uuid> {
    match value {
        Value::Uuid(id) => Ok(id),
        Value::Text(text) => uuid::Uuid::parse_str(&text)
            .map_err(|_| Error::PlanError(format!("invalid UUID in {context}: {text}"))),
        other => Err(Error::PlanError(format!("invalid {context}: {other:?}"))),
    }
}

fn resolve_graph_static_uuid_expr(
    expr: &Expr,
    params: &HashMap<String, Value>,
    context: &str,
) -> Result<Option<uuid::Uuid>> {
    match expr {
        Expr::Literal(_) | Expr::Parameter(_) => {
            graph_uuid_from_value(resolve_expr(expr, params)?, context).map(Some)
        }
        _ => Ok(None),
    }
}

fn is_graph_alias_column_ref(expr: &Expr, alias: &str, column: &str) -> bool {
    matches!(
        expr,
        Expr::Column(contextdb_parser::ast::ColumnRef {
            table: Some(table),
            column: col,
        }) if table == alias && col == column
    )
}

fn is_graph_start_column_ref(expr: &Expr, alias: &str, column: &str) -> bool {
    matches!(
        expr,
        Expr::Column(contextdb_parser::ast::ColumnRef {
            table: Some(table),
            column: col,
        }) if table == alias && col == column
    ) || matches!(
        expr,
        Expr::Column(contextdb_parser::ast::ColumnRef {
            table: None,
            column: col,
        }) if col == column
    )
}

fn graph_start_resolution_filter(filter: &Expr, start_alias: &str) -> Option<Expr> {
    let conjuncts = split_conjuncts(filter)
        .into_iter()
        .filter(|conjunct| graph_expr_refs_only_start_alias(conjunct, start_alias))
        .collect::<Vec<_>>();
    combine_conjuncts(conjuncts)
}

fn graph_start_columns(expr: &Expr, start_alias: &str) -> BTreeSet<String> {
    fn walk(expr: &Expr, start_alias: &str, columns: &mut BTreeSet<String>) {
        match expr {
            Expr::Column(contextdb_parser::ast::ColumnRef {
                table: Some(table),
                column,
            }) if table == start_alias => {
                columns.insert(column.clone());
            }
            Expr::Column(contextdb_parser::ast::ColumnRef {
                table: None,
                column,
            }) if column == "id" => {
                columns.insert(column.clone());
            }
            Expr::BinaryOp { left, right, .. } | Expr::CosineDistance { left, right } => {
                walk(left, start_alias, columns);
                walk(right, start_alias, columns);
            }
            Expr::UnaryOp { operand, .. } | Expr::IsNull { expr: operand, .. } => {
                walk(operand, start_alias, columns);
            }
            Expr::FunctionCall { args, .. } => {
                for arg in args {
                    walk(arg, start_alias, columns);
                }
            }
            Expr::InList { expr, list, .. } => {
                walk(expr, start_alias, columns);
                for item in list {
                    walk(item, start_alias, columns);
                }
            }
            Expr::Like { expr, pattern, .. } => {
                walk(expr, start_alias, columns);
                walk(pattern, start_alias, columns);
            }
            Expr::InSubquery { expr, .. } => walk(expr, start_alias, columns),
            Expr::Column(_)
            | Expr::Literal(_)
            | Expr::Parameter(_)
            | Expr::RowVectorSource { .. } => {}
        }
    }

    let mut columns = BTreeSet::new();
    walk(expr, start_alias, &mut columns);
    columns
}

fn graph_start_filter_needs_unpinned_null_semantics(expr: &Expr, start_alias: &str) -> bool {
    fn walk(expr: &Expr, start_alias: &str, not_parity: bool) -> bool {
        match expr {
            Expr::IsNull { expr, negated } => {
                let matches_missing_metadata = !(*negated ^ not_parity);
                matches_missing_metadata
                    && graph_start_columns(expr, start_alias)
                        .into_iter()
                        .any(|column| column != "id")
            }
            Expr::UnaryOp {
                op: UnaryOp::Not,
                operand,
            } => walk(operand, start_alias, !not_parity),
            Expr::UnaryOp { operand, .. } => walk(operand, start_alias, not_parity),
            Expr::BinaryOp { left, right, .. } | Expr::CosineDistance { left, right } => {
                walk(left, start_alias, not_parity) || walk(right, start_alias, not_parity)
            }
            Expr::FunctionCall { args, .. } => {
                args.iter().any(|arg| walk(arg, start_alias, not_parity))
            }
            Expr::InList { expr, list, .. } => {
                walk(expr, start_alias, not_parity)
                    || list.iter().any(|item| walk(item, start_alias, not_parity))
            }
            Expr::Like { expr, pattern, .. } => {
                walk(expr, start_alias, not_parity) || walk(pattern, start_alias, not_parity)
            }
            Expr::InSubquery { expr, .. } => walk(expr, start_alias, not_parity),
            Expr::Column(_)
            | Expr::Literal(_)
            | Expr::Parameter(_)
            | Expr::RowVectorSource { .. } => false,
        }
    }

    walk(expr, start_alias, false)
}

fn graph_expr_refs_only_start_alias(expr: &Expr, start_alias: &str) -> bool {
    fn walk(expr: &Expr, start_alias: &str, saw_start: &mut bool, saw_other: &mut bool) {
        match expr {
            Expr::Column(contextdb_parser::ast::ColumnRef { table, .. }) => {
                match table.as_deref() {
                    Some(alias) if alias == start_alias => *saw_start = true,
                    None => {
                        if let Expr::Column(contextdb_parser::ast::ColumnRef { column, .. }) = expr
                            && column == "id"
                        {
                            *saw_start = true;
                        } else {
                            *saw_other = true;
                        }
                    }
                    _ => *saw_other = true,
                }
            }
            Expr::BinaryOp { left, right, .. } => {
                walk(left, start_alias, saw_start, saw_other);
                walk(right, start_alias, saw_start, saw_other);
            }
            Expr::UnaryOp { operand, .. } | Expr::IsNull { expr: operand, .. } => {
                walk(operand, start_alias, saw_start, saw_other);
            }
            Expr::FunctionCall { args, .. } => {
                for arg in args {
                    walk(arg, start_alias, saw_start, saw_other);
                }
            }
            Expr::InList { expr, list, .. } => {
                walk(expr, start_alias, saw_start, saw_other);
                for item in list {
                    walk(item, start_alias, saw_start, saw_other);
                }
            }
            Expr::Like { expr, pattern, .. } => {
                walk(expr, start_alias, saw_start, saw_other);
                walk(pattern, start_alias, saw_start, saw_other);
            }
            Expr::InSubquery { expr, .. } | Expr::CosineDistance { left: expr, .. } => {
                walk(expr, start_alias, saw_start, saw_other);
                *saw_other = true;
            }
            Expr::RowVectorSource { .. } => *saw_other = true,
            Expr::Literal(_) | Expr::Parameter(_) => {}
        }
    }

    let mut saw_start = false;
    let mut saw_other = false;
    walk(expr, start_alias, &mut saw_start, &mut saw_other);
    saw_start && !saw_other
}

fn combine_conjuncts(conjuncts: Vec<Expr>) -> Option<Expr> {
    let mut iter = conjuncts.into_iter();
    let first = iter.next()?;
    Some(iter.fold(first, |left, right| Expr::BinaryOp {
        left: Box::new(left),
        op: BinOp::And,
        right: Box::new(right),
    }))
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

fn resolve_query_vector_from_expr(
    db: &Database,
    expr: &Expr,
    params: &HashMap<String, Value>,
    tx: Option<TxId>,
    snapshot: SnapshotId,
) -> Result<(Vec<f32>, Option<contextdb_core::VectorIndexRef>)> {
    match expr {
        Expr::RowVectorSource { table, column, key } => {
            let index = contextdb_core::VectorIndexRef::new(table.clone(), column.clone());
            let vector = resolve_row_vector_source(db, &index, key, params, tx, snapshot)?;
            Ok((vector, Some(index)))
        }
        _ => Ok((resolve_vector_from_expr(expr, params)?, None)),
    }
}

fn resolve_row_vector_source(
    db: &Database,
    index: &contextdb_core::VectorIndexRef,
    key_expr: &Expr,
    params: &HashMap<String, Value>,
    tx: Option<TxId>,
    snapshot: SnapshotId,
) -> Result<Vec<f32>> {
    let meta = db
        .table_meta(&index.table)
        .ok_or_else(|| Error::TableNotFound(index.table.clone()))?;
    db.assert_table_read_allowed(&index.table)?;
    if !meta.columns.iter().any(|column| {
        column.name == index.column
            && matches!(column.column_type, contextdb_core::ColumnType::Vector(_))
    }) {
        return Err(Error::UnknownVectorIndex {
            index: index.clone(),
        });
    }
    db.assert_vector_index_exists_under_schema_read(index)?;

    let raw_key = resolve_expr(key_expr, params)?;
    if matches!(raw_key, Value::Null) {
        return Err(Error::PlanError(
            "ROW_VECTOR key cannot be NULL".to_string(),
        ));
    }
    if matches!(raw_key, Value::Vector(_)) {
        return Err(Error::PlanError(
            "ROW_VECTOR key cannot be a vector".to_string(),
        ));
    }

    let key_column = db.natural_key_column_for_table(&index.table)?;
    let key =
        coerce_into_column(db, &index.table, &key_column, raw_key, None, tx).map_err(|err| {
            Error::PlanError(format!(
                "ROW_VECTOR argument 3 key cannot be coerced to `{}`.`{}` natural key: {err}",
                index.table, key_column
            ))
        })?;
    if matches!(key, Value::Null) {
        return Err(Error::PlanError(
            "ROW_VECTOR key cannot be NULL".to_string(),
        ));
    }
    let key_label = row_vector_key_label(&key);
    let row_id = db
        .row_id_for_natural_key_in_tx(tx, &index.table, &key_column, &key, snapshot)?
        .ok_or_else(|| Error::PersistedRowVectorRowMissing {
            index: index.clone(),
            key: key_label.clone(),
        })?;
    db.assert_row_id_read_allowed_for_change(tx, &index.table, row_id, snapshot)?;
    let entry = db
        .vector_entry_for_row_in_tx(tx, index, row_id, snapshot)?
        .ok_or_else(|| Error::PersistedRowVectorCellNull {
            index: index.clone(),
            key: key_label,
        })?;
    db.validate_vector_under_schema_read(index, entry.vector.len())?;
    Ok(entry.vector)
}

fn row_vector_key_label(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Bool(value) => value.to_string(),
        Value::Int64(value) => value.to_string(),
        Value::Float64(value) => value.to_string(),
        Value::Text(value) => value.clone(),
        Value::Uuid(value) => value.to_string(),
        Value::Timestamp(value) => value.to_string(),
        Value::Json(value) => value.to_string(),
        Value::Vector(_) => "<vector>".to_string(),
        Value::TxId(value) => value.to_string(),
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

fn vector_refs_for_meta(table: &str, meta: &TableMeta) -> Vec<contextdb_core::VectorIndexRef> {
    meta.columns
        .iter()
        .filter(|column| matches!(column.column_type, contextdb_core::ColumnType::Vector(_)))
        .map(|column| contextdb_core::VectorIndexRef::new(table, column.name.clone()))
        .collect()
}

fn has_edge_columns(meta: &TableMeta) -> bool {
    [
        ("source_id", ColumnType::Uuid),
        ("target_id", ColumnType::Uuid),
        ("edge_type", ColumnType::Text),
    ]
    .into_iter()
    .all(|(name, column_type)| has_exact_column_type(meta, name, &column_type))
}

fn has_exact_column_type(meta: &TableMeta, name: &str, column_type: &ColumnType) -> bool {
    let mut columns = meta.columns.iter().filter(|column| column.name == name);
    matches!(columns.next(), Some(column) if &column.column_type == column_type)
        && columns.next().is_none()
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
            // A column DECLARED `TEXT` preserves its text — including a value that happens to parse
            // as a UUID. The id-name heuristic in `coerce_uuid_if_needed` is for untyped/UUID
            // columns; applying it to a declared-TEXT column would silently convert a TEXT id (e.g.
            // a `cg_skill_firing_trace.id` string) into a `Value::Uuid`, which then mismatches the
            // `(Text, Text)` probe arm and reads back as the wrong value type.
            other => Ok(other),
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

/// The direction a table actually has: the one it declared, or the engine
/// default when it declared none.
pub(crate) fn effective_sync_direction(meta: &TableMeta) -> SyncDirection {
    meta.sync_direction.unwrap_or(DEFAULT_SYNC_DIRECTION)
}

/// `SYNC SAFE` promises that a row is not expired locally until the
/// destination confirms receipt. A direction that never sends the table
/// anywhere makes that promise unkeepable — the rows would simply never
/// expire. So the contradiction is refused at the moment it is written, at
/// every door: CREATE, ALTER, and DDL arriving from another machine.
///
/// Plain retention with no delivery promise is untouched: a colocated
/// installation that keeps one copy declares `RETAIN … SYNC OFF` and is
/// entirely legal.
pub(crate) fn refuse_promise_with_no_delivery(
    table: &str,
    sync_safe: bool,
    direction: Option<SyncDirection>,
) -> Result<()> {
    if !sync_safe {
        return Ok(());
    }
    let direction = direction.unwrap_or(DEFAULT_SYNC_DIRECTION);
    if direction.delivers() {
        return Ok(());
    }
    Err(Error::SchemaInvalid {
        reason: format!(
            "table '{table}' declares SYNC SAFE, which promises that a row is not expired here \
             until the destination confirms receipt — but {} never delivers this table \
             anywhere, so the promise could never be kept and the rows would never expire. \
             Declare SYNC PUSH ONLY or SYNC TWO WAY, or drop SYNC SAFE and keep plain RETAIN.",
            direction.sql()
        ),
    })
}

/// `SYNC SAFE` promises delete-only-AFTER-DELIVERY, and delivery needs a key
/// the hub can identify the row by: `changes_since` builds a changeset row from
/// the table's natural key, so a table with none never gets its rows onto the
/// wire at all — while the push still reports success and advances the
/// watermark past their LSNs. The gate would then open on rows the hub never
/// received and retention would delete them. So the promise is refused wherever
/// it is declared on a table that cannot keep it. Plain `RETAIN` is untouched:
/// it prunes locally and promises no delivery.
pub(crate) fn refuse_sync_safe_without_key(table: &str, meta: &TableMeta) -> Result<()> {
    if !meta.sync_safe {
        return Ok(());
    }
    refuse_sync_safe_without_key_for(table, meta)
}

/// The key check itself, for callers that know `SYNC SAFE` is being declared.
pub(crate) fn refuse_sync_safe_without_key_for(table: &str, meta: &TableMeta) -> Result<()> {
    if crate::sync_types::natural_key_column_for_meta(meta).is_some() {
        return Ok(());
    }
    Err(Error::SchemaInvalid {
        reason: format!(
            "SYNC SAFE on table '{table}' requires a PRIMARY KEY: a row with no key never \
             enters a pushed changeset, so the delete-after-delivery gate would open on rows \
             the hub never received and retention would delete them undelivered. Declare a \
             PRIMARY KEY whose values are unique across every NODE that writes this table — \
             include an origin identifier, or use globally unique ids — since two edges \
             pushing the same key collide at the hub. Plain RETAIN without SYNC SAFE needs \
             no key."
        ),
    })
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

fn validate_projected_foreign_keys_after_drop_table(db: &Database, table: &str) -> Result<()> {
    let mut projected = db.relational_store().table_meta.read().clone();
    if projected.remove(table).is_some() {
        Database::validate_projected_foreign_key_schema(&projected)?;
    }
    Ok(())
}

fn validate_projected_foreign_keys_after_drop_column(
    db: &Database,
    table: &str,
    column: &str,
) -> Result<()> {
    let mut projected = db.relational_store().table_meta.read().clone();
    if let Some(meta) = projected.get_mut(table)
        && let Some(pos) = meta.columns.iter().position(|c| c.name == column)
    {
        meta.columns.remove(pos);
        if meta.expires_column.as_deref() == Some(column) {
            meta.expires_column = None;
        }
    }
    Database::validate_projected_foreign_key_schema(&projected)
}

fn validate_projected_foreign_keys_after_rename_column(
    db: &Database,
    table: &str,
    from: &str,
    to: &str,
) -> Result<()> {
    let mut projected = db.relational_store().table_meta.read().clone();
    if let Some(meta) = projected.get_mut(table) {
        if meta.columns.iter().any(|c| c.name == to) {
            return Ok(());
        }
        if let Some(column) = meta.columns.iter_mut().find(|c| c.name == from) {
            column.name = to.to_string();
            if meta.expires_column.as_deref() == Some(from) {
                meta.expires_column = Some(to.to_string());
            }
        }
    }
    Database::validate_projected_foreign_key_schema(&projected)
}

pub(crate) fn btree_indexable(column_type: &ColumnType) -> bool {
    !matches!(column_type, ColumnType::Json | ColumnType::Vector(_))
}

fn exact_constraint_key_indexable(column_type: &ColumnType) -> bool {
    !matches!(
        column_type,
        ColumnType::Real | ColumnType::Json | ColumnType::Vector(_)
    )
}

pub(crate) fn validate_exact_constraint_keys_for_meta(table: &str, meta: &TableMeta) -> Result<()> {
    for column in &meta.columns {
        if (column.primary_key || column.unique)
            && !exact_constraint_key_indexable(&column.column_type)
        {
            return Err(Error::ColumnNotIndexable {
                table: table.to_string(),
                column: column.name.clone(),
                column_type: column.column_type.clone(),
            });
        }
    }

    for unique_constraint in &meta.unique_constraints {
        if unique_constraint.is_empty() {
            return Err(Error::SchemaInvalid {
                reason: format!("UNIQUE constraint on {table} must include at least one column"),
            });
        }
        for column_name in unique_constraint {
            let Some(column) = meta
                .columns
                .iter()
                .find(|candidate| candidate.name == *column_name)
            else {
                return Err(Error::ColumnNotFound {
                    table: table.to_string(),
                    column: column_name.clone(),
                });
            };
            if !exact_constraint_key_indexable(&column.column_type) {
                return Err(Error::ColumnNotIndexable {
                    table: table.to_string(),
                    column: column_name.clone(),
                    column_type: column.column_type.clone(),
                });
            }
        }
    }

    // A multi-column PRIMARY KEY is the sync identity and needs a covering
    // index over exact-matchable columns; refuse a key column that cannot be
    // exact-indexed (REAL / JSON / VECTOR) at declaration rather than failing
    // the sync-apply probe later.
    for column_name in &meta.primary_key_columns {
        let Some(column) = meta
            .columns
            .iter()
            .find(|candidate| candidate.name == *column_name)
        else {
            return Err(Error::ColumnNotFound {
                table: table.to_string(),
                column: column_name.clone(),
            });
        };
        if !exact_constraint_key_indexable(&column.column_type) {
            return Err(Error::ColumnNotIndexable {
                table: table.to_string(),
                column: column_name.clone(),
                column_type: column.column_type.clone(),
            });
        }
    }

    Ok(())
}

pub(crate) fn auto_indexes_for_table_meta(meta: &TableMeta) -> Vec<contextdb_core::IndexDecl> {
    let mut meta_with_indexes = meta.clone();
    meta_with_indexes.indexes.clear();
    for column in &meta.columns {
        if column.primary_key && exact_constraint_key_indexable(&column.column_type) {
            let columns = vec![(column.name.clone(), contextdb_core::SortDirection::Asc)];
            if !auto_index_with_columns_exists(&meta_with_indexes, &columns) {
                let name =
                    unique_auto_index_name(&meta_with_indexes, format!("__pk_{}", column.name));
                meta_with_indexes.indexes.push(contextdb_core::IndexDecl {
                    name,
                    columns,
                    kind: contextdb_core::IndexKind::Auto,
                });
            }
        }
        if column.unique
            && !column.primary_key
            && exact_constraint_key_indexable(&column.column_type)
        {
            let columns = vec![(column.name.clone(), contextdb_core::SortDirection::Asc)];
            if !auto_index_with_columns_exists(&meta_with_indexes, &columns) {
                let name =
                    unique_auto_index_name(&meta_with_indexes, format!("__unique_{}", column.name));
                meta_with_indexes.indexes.push(contextdb_core::IndexDecl {
                    name,
                    columns,
                    kind: contextdb_core::IndexKind::Auto,
                });
            }
        }
    }
    for unique_constraint in &meta.unique_constraints {
        if unique_constraint.is_empty()
            || !unique_constraint.iter().all(|column_name| {
                meta.columns
                    .iter()
                    .find(|column| column.name == *column_name)
                    .is_some_and(|column| exact_constraint_key_indexable(&column.column_type))
            })
        {
            continue;
        }
        let columns = unique_constraint
            .iter()
            .map(|column| (column.clone(), contextdb_core::SortDirection::Asc))
            .collect::<Vec<_>>();
        if auto_index_with_columns_exists(&meta_with_indexes, &columns) {
            continue;
        }
        let name = unique_auto_index_name(
            &meta_with_indexes,
            format!("__unique_{}", unique_constraint.join("_")),
        );
        meta_with_indexes.indexes.push(contextdb_core::IndexDecl {
            name,
            columns,
            kind: contextdb_core::IndexKind::Auto,
        });
    }
    // The multi-column PRIMARY KEY is also the sync identity, and the exact
    // sync-key probe refuses to run without a covering index over
    // the whole key. Back it with an ordered auto-index over its columns, in
    // declared order, so an arriving row is matched by its full identity.
    if meta.primary_key_columns.len() >= 2
        && meta.primary_key_columns.iter().all(|column_name| {
            meta.columns
                .iter()
                .find(|column| column.name == *column_name)
                .is_some_and(|column| exact_constraint_key_indexable(&column.column_type))
        })
    {
        let columns = meta
            .primary_key_columns
            .iter()
            .map(|column| (column.clone(), contextdb_core::SortDirection::Asc))
            .collect::<Vec<_>>();
        if !auto_index_with_columns_exists(&meta_with_indexes, &columns) {
            let name = unique_auto_index_name(
                &meta_with_indexes,
                format!("__pk_{}", meta.primary_key_columns.join("_")),
            );
            meta_with_indexes.indexes.push(contextdb_core::IndexDecl {
                name,
                columns,
                kind: contextdb_core::IndexKind::Auto,
            });
        }
    }
    append_single_column_fk_auto_indexes(&mut meta_with_indexes);
    append_composite_fk_auto_indexes(&mut meta_with_indexes);
    append_graph_edge_auto_index(&mut meta_with_indexes);
    meta_with_indexes.indexes
}

fn auto_index_with_columns_exists(
    meta: &TableMeta,
    columns: &[(String, contextdb_core::SortDirection)],
) -> bool {
    meta.indexes
        .iter()
        .any(|index| index.kind == contextdb_core::IndexKind::Auto && index.columns == columns)
}

fn append_single_column_fk_auto_indexes(meta: &mut TableMeta) {
    for column in &meta.columns {
        let Some(reference) = &column.references else {
            continue;
        };
        if !exact_constraint_key_indexable(&column.column_type) {
            continue;
        }
        let columns = vec![(column.name.clone(), contextdb_core::SortDirection::Asc)];
        if meta.indexes.iter().any(|index| index.columns == columns) {
            continue;
        }
        let name = unique_auto_index_name(
            meta,
            single_column_fk_auto_index_name(&column.name, reference),
        );
        meta.indexes.push(contextdb_core::IndexDecl {
            name,
            columns,
            kind: contextdb_core::IndexKind::Auto,
        });
    }
}

pub(crate) fn append_composite_fk_auto_indexes(meta: &mut TableMeta) {
    for fk in &meta.composite_foreign_keys {
        if fk.child_columns.is_empty()
            || !fk.child_columns.iter().all(|column_name| {
                meta.columns
                    .iter()
                    .find(|column| column.name == *column_name)
                    .is_some_and(|column| exact_constraint_key_indexable(&column.column_type))
            })
        {
            continue;
        }
        let columns = fk
            .child_columns
            .iter()
            .map(|column| (column.clone(), contextdb_core::SortDirection::Asc))
            .collect::<Vec<_>>();
        if meta.indexes.iter().any(|index| index.columns == columns) {
            continue;
        }
        let name = unique_auto_index_name(meta, composite_fk_auto_index_name(fk));
        meta.indexes.push(contextdb_core::IndexDecl {
            name,
            columns,
            kind: contextdb_core::IndexKind::Auto,
        });
    }
}

fn append_graph_edge_auto_index(meta: &mut TableMeta) {
    let graph_edge_columns = [
        ("source_id", ColumnType::Uuid),
        ("target_id", ColumnType::Uuid),
        ("edge_type", ColumnType::Text),
    ];
    if !graph_edge_columns
        .iter()
        .all(|(column_name, column_type)| has_exact_column_type(meta, column_name, column_type))
    {
        return;
    }
    let name = "__graph_edge_source_target_type".to_string();
    if meta.indexes.iter().any(|index| index.name == name) {
        return;
    }
    meta.indexes.push(contextdb_core::IndexDecl {
        name,
        columns: graph_edge_columns
            .iter()
            .map(|(column, _)| ((*column).to_string(), contextdb_core::SortDirection::Asc))
            .collect(),
        kind: contextdb_core::IndexKind::Auto,
    });
}

fn single_column_fk_auto_index_name(
    column: &str,
    reference: &contextdb_core::ForeignKeyReference,
) -> String {
    encoded_auto_index_name("__fk", [column, &reference.table, &reference.column])
}

fn composite_fk_auto_index_name(fk: &contextdb_core::CompositeForeignKey) -> String {
    let mut parts = Vec::with_capacity(fk.child_columns.len() + fk.parent_columns.len() + 1);
    parts.extend(fk.child_columns.iter().map(String::as_str));
    parts.push(fk.parent_table.as_str());
    parts.extend(fk.parent_columns.iter().map(String::as_str));
    encoded_auto_index_name("__fk", parts)
}

fn encoded_auto_index_name<'a, I>(prefix: &str, parts: I) -> String
where
    I: IntoIterator<Item = &'a str>,
{
    let mut name = prefix.to_string();
    for part in parts {
        name.push('_');
        name.push_str(&part.len().to_string());
        name.push(':');
        name.push_str(part);
    }
    name
}

fn unique_auto_index_name(meta: &TableMeta, base: String) -> String {
    if meta.indexes.iter().all(|index| index.name != base) {
        return base;
    }

    for suffix in 1usize.. {
        let candidate = format!("{base}__{suffix}");
        if meta.indexes.iter().all(|index| index.name != candidate) {
            return candidate;
        }
    }

    unreachable!("unbounded suffix search must find a unique auto-index name")
}

pub(crate) fn validate_composite_foreign_keys_for_meta<F>(
    table: &str,
    meta: &TableMeta,
    mut parent_lookup: F,
) -> Result<()>
where
    F: FnMut(&str) -> Option<TableMeta>,
{
    let child_columns = meta
        .columns
        .iter()
        .map(|column| (column.name.as_str(), column))
        .collect::<HashMap<_, _>>();

    for fk in &meta.composite_foreign_keys {
        if fk.child_columns.is_empty()
            || fk.child_columns.len() != fk.parent_columns.len()
            || fk.parent_table.is_empty()
        {
            return Err(Error::SchemaInvalid {
                reason: format!(
                    "composite foreign key on {table} must have matching non-empty child and parent columns"
                ),
            });
        }
        reject_duplicate_columns(table, &fk.child_columns, "FOREIGN KEY child columns")?;
        reject_duplicate_columns(
            &fk.parent_table,
            &fk.parent_columns,
            "FOREIGN KEY parent columns",
        )?;

        for column in &fk.child_columns {
            let Some(column_meta) = child_columns.get(column.as_str()) else {
                return Err(Error::ColumnNotFound {
                    table: table.to_string(),
                    column: column.clone(),
                });
            };
            if !exact_constraint_key_indexable(&column_meta.column_type) {
                return Err(Error::ColumnNotIndexable {
                    table: table.to_string(),
                    column: column.clone(),
                    column_type: column_meta.column_type.clone(),
                });
            }
        }

        let Some(parent_meta) = parent_lookup(&fk.parent_table) else {
            return Err(Error::TableNotFound(fk.parent_table.clone()));
        };
        for column in &fk.parent_columns {
            let Some(parent_column) = parent_meta.columns.iter().find(|c| c.name == *column) else {
                return Err(Error::ColumnNotFound {
                    table: fk.parent_table.clone(),
                    column: column.clone(),
                });
            };
            if !exact_constraint_key_indexable(&parent_column.column_type) {
                return Err(Error::ColumnNotIndexable {
                    table: fk.parent_table.clone(),
                    column: column.clone(),
                    column_type: parent_column.column_type.clone(),
                });
            }
        }
        if !parent_tuple_is_key_covered(&parent_meta, &fk.parent_columns) {
            return Err(Error::SchemaInvalid {
                reason: format!(
                    "composite foreign key on {table} references {}({}) without an ordered PRIMARY KEY or UNIQUE constraint",
                    fk.parent_table,
                    fk.parent_columns.join(", ")
                ),
            });
        }
    }

    Ok(())
}

pub(crate) fn validate_single_column_foreign_keys_for_meta<F>(
    table: &str,
    meta: &TableMeta,
    mut parent_lookup: F,
) -> Result<()>
where
    F: FnMut(&str) -> Option<TableMeta>,
{
    for column in &meta.columns {
        let Some(reference) = &column.references else {
            continue;
        };
        if !exact_constraint_key_indexable(&column.column_type) {
            return Err(Error::ColumnNotIndexable {
                table: table.to_string(),
                column: column.name.clone(),
                column_type: column.column_type.clone(),
            });
        }

        let Some(parent_meta) = parent_lookup(&reference.table) else {
            return Err(Error::TableNotFound(reference.table.clone()));
        };
        let Some(parent_column) = parent_meta
            .columns
            .iter()
            .find(|candidate| candidate.name == reference.column)
        else {
            return Err(Error::ColumnNotFound {
                table: reference.table.clone(),
                column: reference.column.clone(),
            });
        };
        if !exact_constraint_key_indexable(&parent_column.column_type) {
            return Err(Error::ColumnNotIndexable {
                table: reference.table.clone(),
                column: reference.column.clone(),
                column_type: parent_column.column_type.clone(),
            });
        }

        if !parent_tuple_is_key_covered(&parent_meta, std::slice::from_ref(&reference.column)) {
            return Err(Error::SchemaInvalid {
                reason: format!(
                    "foreign key on {table}.{} references {}({}) without a PRIMARY KEY or UNIQUE constraint",
                    column.name, reference.table, reference.column
                ),
            });
        }
    }

    Ok(())
}

fn reject_duplicate_columns(table: &str, columns: &[String], label: &str) -> Result<()> {
    let mut seen = HashSet::new();
    for column in columns {
        if !seen.insert(column) {
            return Err(Error::SchemaInvalid {
                reason: format!("{label} on {table} contain duplicate column '{column}'"),
            });
        }
    }
    Ok(())
}

pub(crate) fn parent_tuple_is_key_covered(
    parent_meta: &TableMeta,
    parent_columns: &[String],
) -> bool {
    if parent_columns.len() == 1
        && parent_meta
            .columns
            .iter()
            .any(|column| column.name == parent_columns[0] && (column.primary_key || column.unique))
    {
        return true;
    }

    parent_meta
        .unique_constraints
        .iter()
        .any(|unique| unique == parent_columns)
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

    #[test]
    fn graph_02_filter_subqueries_resolve_at_graph_snapshot_without_outer_override() {
        let db = Database::open_memory();
        db.execute(
            "CREATE TABLE seeds (id UUID PRIMARY KEY, node_id UUID)",
            &HashMap::new(),
        )
        .unwrap();

        let pre_pin_seed = Uuid::from_u128(1);
        let post_pin_seed = Uuid::from_u128(2);
        db.execute(
            "INSERT INTO seeds (id, node_id) VALUES ($id, $node_id)",
            &HashMap::from([
                ("id".to_string(), Value::Uuid(Uuid::from_u128(10))),
                ("node_id".to_string(), Value::Uuid(pre_pin_seed)),
            ]),
        )
        .unwrap();
        let snapshot = db.snapshot();
        db.execute(
            "INSERT INTO seeds (id, node_id) VALUES ($id, $node_id)",
            &HashMap::from([
                ("id".to_string(), Value::Uuid(Uuid::from_u128(11))),
                ("node_id".to_string(), Value::Uuid(post_pin_seed)),
            ]),
        )
        .unwrap();

        let stmt =
            contextdb_parser::parse("SELECT id FROM nodes WHERE id IN (SELECT node_id FROM seeds)")
                .unwrap();
        let Statement::Select(select) = stmt else {
            panic!("expected SELECT");
        };
        let filter = select.body.where_clause.expect("expected WHERE filter");

        let resolved =
            resolve_graph_filter_at_snapshot(&db, &filter, &HashMap::new(), None, snapshot, &[])
                .unwrap();
        let Expr::InList { list, .. } = resolved else {
            panic!("expected resolved IN list");
        };
        let resolved_ids = list
            .into_iter()
            .map(|expr| match expr {
                Expr::Literal(Literal::Text(value)) => Uuid::parse_str(&value).unwrap(),
                other => panic!("expected UUID text literal, got {other:?}"),
            })
            .collect::<BTreeSet<_>>();

        assert_eq!(resolved_ids, BTreeSet::from([pre_pin_seed]));
        assert!(
            !resolved_ids.contains(&post_pin_seed),
            "post-pin seed leaked into graph filter subquery resolution"
        );
    }
}
