//! Changing and dropping what was declared: ALTER TABLE, DROP TABLE and DROP INDEX.
//!
//! An altered table is declared again from its own printed declaration with the change made
//! to it, so that an ALTER is refused whatever a CREATE TABLE is refused. The table keeps its
//! number, and every index, kept count and VECTOR column that is still declared keeps its own.
//! What is no longer declared is staged to leave the store in the same commit.

use super::rules::{self, Count, RankPolicy};
use super::{Catalog, RESERVED_PREFIXES, Table, VectorColumn, check, record, render};
use crate::sql::ast::{AlterAction, Statement, VectorClauses};
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
) -> Result<Declared> {
    let (name, if_exists) = match plan {
        Plan::AlterTable(alter) => (&alter.table, false),
        Plan::DropTable(drop) => (&drop.name, drop.if_exists),
        Plan::DropIndex(drop) => (&drop.table, false),
        other => unreachable!("{other:?} alters and drops nothing"),
    };
    let (tables, left) = (catalog.tables.clone(), catalog.left.clone());
    let mut declared = Declared::from(Catalog { tables, left });
    let Some(old) = catalog.table(name) else {
        return match if_exists {
            true => Ok(declared),
            false => Err(Error::TableNotFound(name.clone())),
        };
    };
    declared.catalog.left.push(old.clone());
    let table = match plan {
        Plan::AlterTable(alter) => {
            let action = &alter.action;
            Some(altered(old, action, catalog, changes, now, &mut declared)?)
        }
        Plan::DropIndex(drop) => {
            let made = |index: &&IndexDecl| index.name == drop.name && !engine(index);
            let Some(index) = old.meta.indexes.iter().find(made) else {
                return match drop.if_exists {
                    true => Ok(declared),
                    false => Err(Error::IndexNotFound {
                        table: name.clone(),
                        index: drop.name.clone(),
                    }),
                };
            };
            ranked(catalog, name, |_, policy| {
                let leads = index.columns.first();
                let joins = policy.policy.table == *name
                    && leads.is_some_and(|(column, _)| *column == policy.policy.column);
                joins.then(|| policy.policy.column.clone())
            })?;
            let without = |_: &mut CreateTablePlan, made: &mut Vec<(IndexDecl, IndexId)>| {
                made.retain(|(index, _)| index.name != drop.name);
            };
            Some(again(old, catalog, without)?)
        }
        _ => {
            let mut others = catalog.tables.values().filter(|other| other.name != *name);
            let refers = |other: &&Table| other.rules.references.iter().any(|r| r.parent == *name);
            if let Some(child) = others.find(refers) {
                return Err(check::invalid(format!(
                    "table '{name}' is referenced by table '{}', so it was not dropped; drop \
                     that table first",
                    child.name
                )));
            }
            ranked(catalog, name, |reader, policy| {
                let joins = reader.name != *name && policy.policy.table == *name;
                joins.then(|| policy.policy.column.clone())
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
        Some(table) => {
            let made = table.index_ids.iter().copied();
            declared.indexes = made.filter(|id| !old.index_ids.contains(id)).collect();
            let before = counts(old);
            declared.counts = kept;
            declared.counts.retain(|id| !before.contains(id));
            changes.put_schema(&table.name, record::encode(&table))?;
            let tables = &mut declared.catalog.tables;
            tables.insert(table.name.clone(), table.shaped());
        }
        None => {
            changes.stage_drop(Dropped::Rows(old.id))?;
            changes.delete_schema(name)?;
            declared.catalog.tables.remove(name);
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
) -> Result<Table> {
    let name = &old.name;
    let position = |column: &str| rules::position(name, &old.meta, column);
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
    let option = |set: &dyn Fn(&mut CreateTablePlan)| again(old, catalog, |create, _| set(create));
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
            let mut table = again(old, catalog, add)?;
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
            held(old, column, at, catalog)?;
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
            let parents = catalog.tables.values().flat_map(|t| &t.rules.references);
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
            let table = again(old, catalog, |create, made| {
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
            held(old, from, at, catalog)?;
            if old.meta.columns.iter().any(|column| column.name == *to) {
                return Err(check::invalid(format!(
                    "table '{name}' already has a column '{to}'; choose another name"
                )));
            }
            let rename = |column: &mut String| {
                if *column == *from {
                    column.clone_from(to);
                }
            };
            let mut table = old.clone();
            let meta = &mut table.meta;
            rename(&mut meta.columns[at].name);
            meta.unique_constraints
                .iter_mut()
                .flatten()
                .for_each(rename);
            for index in &mut meta.indexes {
                index
                    .columns
                    .iter_mut()
                    .for_each(|(column, _)| rename(column));
                // The index behind a rule is named for the rule's columns.
                let behind = RESERVED_PREFIXES[..3].iter();
                if let Some(prefix) = behind.into_iter().find(|p| index.name.starts_with(**p)) {
                    let columns: Vec<&str> = index.columns.iter().map(|(c, _)| &**c).collect();
                    index.name = format!("{prefix}{}", columns.join("_"));
                }
            }
            if let Some(machine) = &mut meta.state_machine {
                rename(&mut machine.column);
            }
            if let Some(KeptCounts::Lists(lists)) = &mut meta.kept_counts {
                let listed = lists.iter_mut().flatten();
                listed.for_each(|(column, _)| rename(column));
            }
            table.vectors.iter_mut().for_each(|v| rename(&mut v.column));
            // A reference to the column follows it, from this table and from any other.
            let follow = |child: &mut Table| {
                let mut followed = false;
                let to_here = child.rules.references.iter_mut();
                let to_here = to_here.filter(|reference| reference.parent == *name);
                for column in to_here.flat_map(|reference| &mut reference.parent_columns) {
                    followed |= *column == *from;
                    rename(column);
                }
                followed
            };
            follow(&mut table);
            for other in declared.catalog.tables.values_mut() {
                if other.id != old.id && follow(other) {
                    changes.put_schema(&other.name, record::encode(other))?;
                }
            }
            again(&table, catalog, |_, _| {})
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
        AlterAction::SetAutoIndexAt(c, to) => clause(c, &|v| v.auto_index_at.clone_from(to)),
        AlterAction::SetConsolidation(c, to) => clause(c, &|v| v.consolidation.clone_from(to)),
    }
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
    edit: impl FnOnce(&mut CreateTablePlan, &mut Vec<(IndexDecl, IndexId)>),
) -> Result<Table> {
    // A kept count may be led by an index of CREATE INDEX, so the counts are settled after
    // those indexes are back.
    let mut bare = old.clone();
    bare.meta.kept_counts = None;
    bare.meta.indexes.retain(engine);
    let text = render::declaration(&bare);
    let text = text.trim_end().trim_end_matches(';');
    let Statement::CreateTable(mut create) = sql::parse(text)? else {
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

    let mut table = check::new_table(&create, catalog)?;
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
    for (index, id) in made {
        table.meta.indexes.push(index);
        table.index_ids.push(id);
    }
    for vector in &mut table.vectors {
        if let Some(had) = old.vector(&vector.column) {
            vector.id = had.id;
        }
    }
    let meta = &table.meta;
    let place = |column: &str| meta.columns.iter().position(|c| c.name == column);
    let was = |column: &str| old.meta.columns.iter().position(|c| c.name == column);
    let missing = meta.columns.iter().map(|column| match was(&column.name) {
        Some(at) => old.missing[at].clone(),
        None => Value::Null,
    });
    table.missing = missing.collect();
    // A count over columns that are all still there is the count it was.
    let had = old.rules.counts.iter().filter_map(|count| {
        let columns = count
            .columns
            .iter()
            .map(|(at, direction)| Some((place(&old.meta.columns[*at].name)?, *direction)));
        Some(Count {
            id: count.id,
            columns: columns.collect::<Option<_>>()?,
        })
    });
    let had: Vec<Count> = had.collect();
    table.meta.kept_counts = kept_counts;
    table.rules.counts = rules::counts(&table.name, &table.meta, &had, catalog)?;
    Ok(table)
}

/// Refuses to drop or rename a column that something of its table is declared by: a VECTOR
/// column's PARTITION_KEY, IMMUTABLE, the primary key, the shape of a DAG table, a rank policy.
fn held(old: &Table, column: &str, at: usize, catalog: &Catalog) -> Result<()> {
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
        let read = rules::formula_reads(&policy.policy.formula).unwrap_or_default();
        let read = read.contains(&column);
        let own = reader.id == old.id
            && (read || reader.meta.columns[policy.anchor].name == column)
            && policy.column != at;
        let joined = policy.policy.table == table && (read || policy.policy.column == column);
        (own || joined).then(|| column.clone())
    })
}

/// Refuses a drop or an ALTER of something of `table` that a rank policy reads: `reads` says,
/// for each policy of each table, the column of `table` it reads that the statement touches.
fn ranked(
    catalog: &Catalog,
    table: &str,
    reads: impl Fn(&Table, &RankPolicy) -> Option<String>,
) -> Result<()> {
    for reader in catalog.tables.values() {
        for policy in &reader.rules.rank_policies {
            if let Some(column) = reads(reader, policy) {
                return Err(Error::DropBlockedByRankPolicy {
                    table: table.to_owned(),
                    column,
                    policy: policy.policy.sort_key.clone(),
                });
            }
        }
    }
    Ok(())
}
