//! Changing and dropping what was declared: ALTER TABLE, DROP TABLE and DROP INDEX.
//!
//! An altered table is declared again from its own printed declaration with the change made
//! to it, so that an ALTER is refused whatever a CREATE TABLE is refused. The table keeps its
//! number, and every index, kept count and VECTOR column that is still declared keeps its own.
//! What is no longer declared is staged to leave the store in the same commit.

use super::rules::{self, Count, RankPolicy};
use super::{Catalog, Kind, RESERVED_PREFIXES, Table, VectorColumn, check, record, render};
use crate::memory::Budget;
use crate::sql::ast::{AlterAction, EventDeclaration::*, Statement, VectorClauses};
use crate::sql::plan::CreateTablePlan;
use crate::sql::{self, Plan};
use crate::store::{ChangeSet, CountId, Dropped, IndexId, Read, VectorNumber};
use crate::types::{ColumnDefault, Error, IndexDecl, KeptCounts, Result, Value};

/// What a declaration leaves: the catalog as it will be once the change set commits, and what
/// `database` still has to do and to answer.
pub(crate) struct Declared {
    pub catalog: Catalog,
    /// The indexes the declaration made on its table, which hold no entry yet.
    pub indexes: Vec<IndexId>,
    /// The kept counts the declaration made on its table, which hold no record yet.
    pub counts: Vec<CountId>,
    /// The names of the indexes a `DROP COLUMN ... CASCADE` dropped, in the order they were
    /// made.
    pub dropped: Vec<String>,
}

impl From<Catalog> for Declared {
    fn from(catalog: Catalog) -> Declared {
        Declared {
            catalog,
            indexes: Vec::new(),
            counts: Vec::new(),
            dropped: Vec::new(),
        }
    }
}

/// Applies an ALTER TABLE, a DROP TABLE or a DROP INDEX: checks it, stages the table's schema
/// record and the removal of everything the table no longer declares.
pub(super) fn declare(
    plan: &Plan,
    catalog: &Catalog,
    changes: &mut ChangeSet,
    now: i64,
    budget: &Budget,
) -> Result<Declared> {
    let mut declared = Declared::from(catalog.next(budget)?);
    let Some(name) = super::subject(plan) else {
        return Ok(declared);
    };
    let if_exists = matches!(plan, Plan::DropTable(d) if d.if_exists)
        || matches!(plan, Plan::DropIndex(d) if d.if_exists);
    let Some(old) = catalog.table(name) else {
        return match if_exists {
            true => Ok(declared),
            false => Err(Error::TableNotFound(name.clone())),
        };
    };
    let _working = budget.reserve(64 * u64::from(old.stored), "altering a declaration")?;
    let table = match plan {
        Plan::AlterTable(alter) => {
            let action = &alter.action;
            Some(altered(
                old,
                action,
                catalog,
                changes,
                now,
                &mut declared,
                budget,
            )?)
        }
        Plan::DropIndex(drop) => {
            let Some(index) = old.meta.indexes.iter().find(|i| made(i, &drop.name)) else {
                return match drop.if_exists {
                    true => Ok(declared),
                    false => Err(missing(name.clone(), &drop.name)),
                };
            };
            ranked(catalog, name, |_, policy| {
                let leads = index.columns.first();
                let joins = policy.policy.table == *name
                    && leads.is_some_and(|(column, _)| *column == policy.policy.column);
                Ok(joins.then(|| policy.policy.column.clone()))
            })?;
            let without = |_: &mut CreateTablePlan, made: &mut Vec<(IndexDecl, IndexId)>| {
                made.retain(|(index, _)| index.name != drop.name);
            };
            Some(again(old, catalog, budget, without)?)
        }
        _ => {
            if let Some(child) = catalog.children(old).find(|child| child.name != *name) {
                return Err(check::invalid(format!(
                    "table '{name}' is referenced by table '{}', so it was not dropped; drop \
                     that table first",
                    child.name
                )));
            }
            ranked(catalog, name, |reader, policy| {
                let joins = reader.name != *name && policy.policy.table == *name;
                Ok(joins.then(|| policy.policy.column.clone()))
            })?;
            None
        }
    };

    // Whatever the table no longer declares leaves the store with this commit.
    let new = table.as_ref();
    for id in &old.index_ids {
        if !new.is_some_and(|table| table.index_ids.contains(id)) {
            changes.stage_drop(Dropped::Index(*id))?;
        }
    }
    let counts = |table: &Table| -> Vec<CountId> {
        table.rules.counts.iter().map(|count| count.id).collect()
    };
    let kept = new.map(counts).unwrap_or_default();
    for id in counts(old) {
        if !kept.contains(&id) {
            changes.stage_drop(Dropped::Kept(id))?;
        }
    }
    for vector in &old.vectors {
        if !new.is_some_and(|table| table.vectors.iter().any(|kept| kept.id == vector.id)) {
            changes.stage_drop(Dropped::Vectors(vector.id))?;
        }
    }
    match table {
        Some(mut table) => {
            let made = table.index_ids.iter().copied();
            declared.indexes = made.filter(|id| !old.index_ids.contains(id)).collect();
            let before = counts(old);
            declared.counts = kept;
            declared.counts.retain(|id| !before.contains(id));
            changes.put_schema(&Kind::Table.key(name), record::encode(&mut table))?;
            let held = budget.reserve(table.heap, "a declaration")?;
            declared.catalog.put(table.shaped(), held, budget)?;
        }
        None => {
            changes.stage_drop(Dropped::Rows(old.id))?;
            changes.delete_schema(&Kind::Table.key(name))?;
            declared.catalog.take(name, budget)?;
            // Its triggers, its event types and the routes they feed go with it.
            for key in catalog.ids[&old.id].events.iter() {
                changes.delete_schema(&key.0.key(&key.1))?;
                declared.catalog.take_event(key, budget)?;
            }
        }
    }
    Ok(declared)
}

/// The table after one ALTER TABLE action. Stages what the action does beyond the table's own
/// schema record: the cell of a dropped column, and the record of another table whose
/// reference follows a renamed column.
fn altered(
    old: &Table,
    action: &AlterAction,
    catalog: &Catalog,
    changes: &mut ChangeSet,
    now: i64,
    declared: &mut Declared,
    budget: &Budget,
) -> Result<Table> {
    let name = &old.name;
    let position = |column: &str| rules::position(name, &old.meta, column);
    // A route fed by an event type of the table that filters on the column.
    let uses = |r: &crate::types::RouteDecl, col: &str| {
        r.where_in.as_ref().is_some_and(|f| f.column == col)
    };
    // The VECTOR column an `ALTER COLUMN ... SET` names.
    let vector = |column: &str| -> Result<&VectorColumn> {
        position(column)?;
        old.vector(column).ok_or_else(|| {
            check::invalid(format!(
                "column '{name}.{column}' is not a VECTOR column, and this clause is a VECTOR \
                 column's"
            ))
        })
    };
    // The table declared again with one of its options changed.
    let option =
        |set: &dyn Fn(&mut CreateTablePlan)| again(old, catalog, budget, |create, _| set(create));
    // The table declared again with a clause of one of its VECTOR columns changed.
    let clause = |column: &str, set: &dyn Fn(&mut VectorClauses)| {
        vector(column)?;
        option(&|create| {
            let columns = create.columns.iter_mut().filter(|c| c.name == column);
            columns.for_each(|c| set(&mut c.vector));
        })
    };
    match action {
        AlterAction::AddColumn(column) => {
            let refused = if column.primary_key {
                "PRIMARY KEY: a table's key is declared with the table"
            } else if !column.nullable && column.default.is_none() && changes.count(old.id)? > 0 {
                "NOT NULL with no DEFAULT on a table that has rows, which would have nothing to \
                 read in it: give it a DEFAULT"
            } else {
                ""
            };
            if !refused.is_empty() {
                return Err(check::invalid(format!(
                    "table '{name}': the added column '{}' declares {refused}",
                    column.name
                )));
            }
            let add = |create: &mut CreateTablePlan, _: &mut Vec<(IndexDecl, IndexId)>| {
                create.columns.push((**column).clone());
            };
            let mut table = again(old, catalog, budget, add)?;
            // What a row written before the column reads in it, worked out once, here.
            let added = table.meta.columns.last();
            let default = added.and_then(|c| Some((c, c.default.clone()?)));
            if let (Some(missing), Some((added, default))) = (table.missing.last_mut(), default) {
                *missing = match default {
                    ColumnDefault::Literal(value) => crate::types::typed(name, added, value)?,
                    ColumnDefault::Now => Value::Timestamp(now),
                };
            }
            Ok(table)
        }
        AlterAction::DropColumn { column, cascade } => {
            let at = position(column)?;
            held(old, column, at, catalog, budget)?;
            let mut routes = catalog.on(old.id, Kind::Route).map(|(e, _)| e);
            if let Some(Route(r)) = routes.find(|e| matches!(e, Route(r) if uses(r, column))) {
                return Err(check::invalid(format!(
                    "column '{name}.{column}' is named by route '{}', so it was not dropped",
                    r.name
                )));
            }
            // The column a rule of the table is declared by stays while the rule does.
            let lists = match &old.meta.kept_counts {
                Some(KeptCounts::Lists(lists)) => &lists[..],
                _ => &[],
            };
            let machine = old.meta.state_machine.iter();
            let machine = machine.map(|m| (&m.column, "STATE MACHINE"));
            let counts = lists.iter().flatten().map(|(c, _)| (c, "MAINTAIN COUNT"));
            if let Some((_, rule)) = machine.chain(counts).find(|(c, _)| *c == column) {
                return Err(check::invalid(format!(
                    "column '{name}.{column}' is named by the table's {rule}, so it was not dropped"
                )));
            }
            let parents = catalog.children(old).flat_map(|t| &t.rules.references);
            let mut parents = parents.filter(|reference| reference.parent == *name);
            if parents.any(|reference| reference.parent_columns.contains(column)) {
                return Err(check::invalid(format!(
                    "column '{name}.{column}' is referenced by another table, so it was not \
                     dropped; drop the column or the table that references it first"
                )));
            }
            // An index over the column stops the drop, unless the engine made it for this
            // column alone or for the three columns of a table of edges: those go with it.
            let names = |index: &IndexDecl| index.columns.iter().any(|(c, _)| c == column);
            let stops = |index: &&IndexDecl| {
                let own = engine(index) && index.columns.len() == 1;
                names(index) && !own && !index.name.starts_with("__graph_")
            };
            let mut stopping = old.meta.indexes.iter().filter(stops);
            if *cascade != Some(true)
                && let Some(index) = stopping.next()
            {
                return Err(Error::ColumnInIndex {
                    table: name.clone(),
                    column: column.clone(),
                    index: index.name.clone(),
                });
            }
            declared.dropped = stopping.map(|index| index.name.clone()).collect();
            let table = again(old, catalog, budget, |create, made| {
                create.columns.retain(|c| c.name != *column);
                create.unique_constraints.retain(|c| !c.contains(column));
                create.foreign_keys.retain(|(c, _)| !c.contains(column));
                made.retain(|(index, _)| !names(index));
            })?;
            // A row's cells are in the order of the table's columns.
            changes.stage_drop(Dropped::Cell(old.id, at))?;
            Ok(table)
        }
        AlterAction::RenameColumn { from, to } => {
            let at = position(from)?;
            held(old, from, at, catalog, budget)?;
            if old.meta.columns.iter().any(|column| column.name == *to) {
                return Err(check::invalid(format!(
                    "table '{name}' already has a column '{to}'; choose another name"
                )));
            }
            let others = catalog.children(old).filter(|t| t.name != *name);
            let routes = catalog.on(old.id, Kind::Route);
            let routes = routes.filter(|(e, _)| matches!(e, Route(r) if uses(r, from)));
            let related = others.map(|t| u64::from(t.stored)).sum::<u64>()
                + routes
                    .map(|(_, heap)| *heap + size_of::<crate::sql::ast::EventDeclaration>() as u64)
                    .sum::<u64>();
            let _related =
                budget.reserve(super::SCHEMA * related, "renaming dependent declarations")?;
            let rename = |c: &mut String| {
                let changed = c == from;
                if changed {
                    *c = to.clone();
                }
                changed
            };
            let mut table = old.clone();
            let meta = &mut table.meta;
            rename(&mut meta.columns[at].name);
            meta.unique_constraints.iter_mut().flatten().for_each(|c| {
                rename(c);
            });
            for index in &mut meta.indexes {
                let changed = index
                    .columns
                    .iter_mut()
                    .fold(false, |any, (c, _)| rename(c) | any);
                // The index behind a rule is named for the rule's columns.
                let behind = RESERVED_PREFIXES.iter().filter(|p| **p != "__graph_");
                if let Some(prefix) = behind
                    .into_iter()
                    .find(|p| changed && index.name.starts_with(**p))
                {
                    let columns: Vec<&str> = index.columns.iter().map(|(c, _)| &**c).collect();
                    index.name = format!("{prefix}{}", columns.join("_"));
                }
            }
            if let Some(machine) = &mut meta.state_machine {
                rename(&mut machine.column);
            }
            if let Some(KeptCounts::Lists(lists)) = &mut meta.kept_counts {
                lists.iter_mut().flatten().for_each(|(c, _)| {
                    rename(c);
                });
            }
            table.vectors.iter_mut().for_each(|v| {
                rename(&mut v.column);
            });
            // A reference to the column follows it, from this table and from any other.
            let follow = |child: &mut Table| {
                child
                    .rules
                    .references
                    .iter_mut()
                    .filter(|r| r.parent == *name)
                    .flat_map(|r| &mut r.parent_columns)
                    .fold(false, |any, c| rename(c) | any)
            };
            follow(&mut table);
            for other in catalog.children(old).filter(|t| t.name != *name) {
                let mut other = other.clone();
                if follow(&mut other) {
                    changes
                        .put_schema(&Kind::Table.key(&other.name), record::encode(&mut other))?;
                    let held = budget.reserve(other.heap, "a declaration")?;
                    declared.catalog.put(other, held, budget)?;
                }
            }
            for (route, _) in catalog.on(old.id, Kind::Route) {
                let Route(r) = route else { continue };
                let mut r = r.clone();
                if uses(&r, from) && rename(&mut r.where_in.as_mut().unwrap().column) {
                    let route = Route(r);
                    let (record, heap) = record::encode_event(&route);
                    changes.put_schema(&Kind::Route.key(route.identity().1), record)?;
                    let held = budget.reserve(heap, "a declaration")?;
                    declared.catalog.put_event((route, heap), held, budget)?;
                }
            }
            again(&table, catalog, budget, |_, _| {})
        }
        AlterAction::SetRetain(retain) => {
            option(&|create| create.options.retain = Some(retain.clone()))
        }
        AlterAction::DropRetain => option(&|create| create.options.retain = None),
        AlterAction::SetHistory(history) => {
            option(&|create| create.options.history = Some(*history))
        }
        AlterAction::SetSync(sync) => option(&|create| create.options.sync = Some(*sync)),
        AlterAction::SetConflict(conflict) => {
            option(&|create| create.options.conflict = Some(*conflict))
        }
        AlterAction::SetKeptCounts(kept) => {
            option(&|create| create.options.kept_counts.clone_from(kept))
        }
        AlterAction::SetMaxPartitions {
            column,
            max_partitions,
        } => {
            let vector = vector(column)?;
            let table = clause(column, &|v| v.max_partitions = Some(max_partitions.clone()))?;
            // The limit is never set below the partitions the column has.
            let has = changes.vector_number(VectorNumber::Partitions(vector.id))?;
            match table.vector(column).and_then(VectorColumn::limit) {
                Some(limit) if has > u64::from(limit) => Err(Error::VectorPartitionLimitExceeded {
                    index: old.vector_ref(vector),
                    max_partitions: limit,
                }),
                _ => Ok(table),
            }
        }
        AlterAction::SetSearchMode {
            column,
            search_mode,
        } => clause(column, &|v| v.search_mode = Some(*search_mode)),
        AlterAction::SetIndexingThreshold(c, to) => {
            clause(c, &|v| v.indexing_threshold.clone_from(to))
        }
        AlterAction::SetProbes(c, to) => clause(c, &|v| v.probes.clone_from(to)),
        AlterAction::SetCompaction(c, to) => clause(c, &|v| v.compaction.clone_from(to)),
    }
}

/// A DROP INDEX that names no table, resolved once against `catalog`: carried as the drop of the
/// one table's index of that name, and with no table when none has one under IF EXISTS.
pub(crate) fn named(plan: &mut Plan, catalog: &Catalog) -> Result<()> {
    if let Plan::DropIndex(drop @ sql::ast::DropIndex { table: None, .. }) = plan {
        let mut tables = catalog.tables();
        tables.retain(|t| t.meta.indexes.iter().any(|i| made(i, &drop.name)));
        drop.table = match tables[..] {
            [] if drop.if_exists => None,
            [] => return Err(missing(String::new(), &drop.name)),
            [table] => Some(table.name.clone()),
            _ => {
                let names: Vec<_> = tables.iter().map(|t| t.name.as_str()).collect();
                let (index, names) = (&drop.name, names.join(", "));
                let why = format!("index name \"{index}\" is ambiguous: tables {names}");
                return Err(Error::PlanError(format!("{why}; write ON <table>")));
            }
        };
    }
    Ok(())
}

/// Whether `index` is the user's index `name`, not one the engine made.
fn made(index: &IndexDecl, name: &str) -> bool {
    index.name == name && !engine(index)
}

fn missing(table: String, index: &str) -> Error {
    let index = index.to_owned();
    Error::IndexNotFound { table, index }
}

/// Whether the engine made the index for itself.
fn engine(index: &IndexDecl) -> bool {
    let mut reserved = RESERVED_PREFIXES.iter();
    reserved.any(|prefix| index.name.starts_with(prefix))
}

/// The table `old` declared again after `edit` has changed its CREATE TABLE and the indexes of
/// CREATE INDEX it has, each with its number. Whatever the new table still declares keeps the
/// number it had, and every column still there keeps what an older row reads in it.
fn again(
    old: &Table,
    catalog: &Catalog,
    budget: &Budget,
    edit: impl FnOnce(&mut CreateTablePlan, &mut Vec<(IndexDecl, IndexId)>),
) -> Result<Table> {
    // A kept count may be led by an index of CREATE INDEX, so the counts are settled after
    // those indexes are back.
    let mut bare = old.clone();
    bare.meta.kept_counts = None;
    bare.meta.indexes.retain(engine);
    let text = render::declaration(&bare);
    let text = text.trim_end().trim_end_matches(';');
    let (statement, ..) = sql::parse(text, budget)?;
    let Statement::CreateTable(mut create) = statement else {
        unreachable!("a table is printed back as a CREATE TABLE");
    };
    let indexes = old
        .meta
        .indexes
        .iter()
        .cloned()
        .zip(old.index_ids.iter().copied());
    let mut made: Vec<(IndexDecl, IndexId)> = indexes.filter(|(index, _)| !engine(index)).collect();
    create.options.kept_counts.clone_from(&old.meta.kept_counts);
    edit(&mut create, &mut made);
    let kept_counts = create.options.kept_counts.take();

    let mut table = check::new_table(&create, Some(catalog), budget)?;
    table.id = old.id;
    for (index, id) in table.meta.indexes.iter().zip(&mut table.index_ids) {
        if let Some(at) = old
            .meta
            .indexes
            .iter()
            .position(|had| had.name == index.name)
        {
            *id = old.index_ids[at];
        }
    }
    if table.meta.retain.is_some() && old.meta.retain.is_some() {
        table.rules.retain_since = old.rules.retain_since;
        let name = old
            .rules
            .expires_column
            .map(|at| &old.meta.columns[at].name);
        if name
            == table
                .rules
                .expires_column
                .map(|at| &table.meta.columns[at].name)
        {
            *table.index_ids.last_mut().unwrap() = *old.index_ids.last().unwrap();
        }
    }
    for (index, id) in made {
        table.index_ids.insert(table.meta.indexes.len(), id);
        table.meta.indexes.push(index);
    }
    for vector in &mut table.vectors {
        if let Some(had) = old.vector(&vector.column) {
            vector.id = had.id;
        }
    }
    let meta = &table.meta;
    let was = |column: &str| old.meta.columns.iter().position(|c| c.name == column);
    let missing = meta.columns.iter().map(|column| match was(&column.name) {
        Some(at) => old.missing[at].clone(),
        None => Value::Null,
    });
    table.missing = missing.collect();
    // A count over columns that are all still there is the count it was.
    let had = old.rules.counts.iter().filter_map(|count| {
        let columns = &old.meta.indexes[count.index].columns[..count.prefix];
        let index = table
            .meta
            .indexes
            .iter()
            .position(|i| i.json_path.is_empty() && i.columns.starts_with(columns))?;
        Some(Count {
            id: count.id,
            index,
            prefix: count.prefix,
        })
    });
    let had: Vec<Count> = had.collect();
    table.meta.kept_counts = kept_counts;
    table.rules.counts = rules::counts(&table.name, &table.meta, &had, catalog)?;
    Ok(table)
}

/// Refuses to drop or rename a column that something of its table is declared by: a VECTOR
/// column's PARTITION_KEY, IMMUTABLE, the primary key, the shape of a DAG table, a rank policy.
fn held(old: &Table, column: &str, at: usize, catalog: &Catalog, budget: &Budget) -> Result<()> {
    let (table, column) = (old.name.clone(), column.to_owned());
    let in_key = |vector: &&VectorColumn| vector.partition_key.contains(&column);
    if let Some(vector) = old.vectors.iter().find(in_key) {
        let index = vector.column.clone();
        return Err(Error::VectorPartitionKeyInUse {
            table,
            column,
            index,
        });
    }
    if old.rules.immutable_columns.contains(&at) {
        return Err(Error::ImmutableColumn { table, column });
    }
    let edge = ["source_id", "target_id", "edge_type"].contains(&&*column);
    let refused = if old.key.contains(&at) {
        "is in the PRIMARY KEY, which stays as the table declared it"
    } else if edge && !old.meta.dag.is_empty() {
        "is one of the three columns that make this DAG table a table of edges"
    } else {
        ""
    };
    if !refused.is_empty() {
        return Err(check::invalid(format!(
            "column '{table}.{column}' {refused}, so it is neither dropped nor renamed"
        )));
    }
    ranked(catalog, &table, |reader, policy| {
        let read = rules::formula_reads(&policy.policy.formula, budget)?;
        let read = read.contains(&column);
        let own = reader.id == old.id
            && (read || reader.meta.columns[policy.anchor].name == column)
            && policy.column != at;
        let joined = policy.policy.table == table && (read || policy.policy.column == column);
        Ok((own || joined).then(|| column.clone()))
    })
}

/// Refuses a drop or an ALTER of something of `table` that a rank policy reads: `reads` says,
/// for each policy of each table, the column of `table` it reads that the statement touches.
fn ranked(
    catalog: &Catalog,
    table: &str,
    reads: impl Fn(&Table, &RankPolicy) -> Result<Option<String>>,
) -> Result<()> {
    for r in catalog.tables() {
        for p in &r.rules.rank_policies {
            let Some(column) = reads(r, p)? else { continue };
            let col = &r.meta.columns[p.column].name;
            return Err(Error::DropBlockedByRankPolicy {
                table: table.to_owned(),
                column,
                policy: format!("{} on {}.{col}", p.policy.sort_key, r.name),
            });
        }
    }
    Ok(())
}
