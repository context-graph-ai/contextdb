//! write: applies INSERT, UPDATE and DELETE to a transaction and checks every rule a table can
//! declare: column types, NOT NULL, the primary key and UNIQUE rules. It keeps each table's
//! index entries and row count in step with its rows.
//!
//! It never returns a query's result to a caller: it reads only to find the rows an UPDATE or
//! DELETE names and to check a rule.
//!
//! A statement goes row by row: a row is worked out, brought to its columns' types, held against
//! NOT NULL, its key and its unique rules, and staged with its index entries before the next row
//! is looked at, so a later row is checked against the earlier ones through the change set. The
//! caller undoes a refused statement by rewinding the change set.

#[cfg(test)]
mod tests;
mod values;

use std::collections::HashMap;
use std::ops::Bound;

use crate::memory::Budget;
use crate::query;
use crate::schema::{Catalog, Table};
use crate::sql::Plan;
use crate::sql::plan::{DeletePlan, InsertPlan, UpdatePlan};
use crate::store::{Cell, ChangeSet, IndexId, Key, Read, View};
use crate::types::{ColumnDefault, Error, Result, SortDirection, Value};
use crate::vector::Tally;

/// Applies one INSERT, UPDATE or DELETE to the change set and returns the number of rows: those
/// an INSERT stored, or those the WHERE of an UPDATE or DELETE matched.
///
/// Reads through the change set, so the statement sees what its transaction already wrote.
/// Finds the rows of an UPDATE or DELETE with `query::matching`, which hands them back under a
/// reservation that is held until this returns, and works out values with `query::eval`.
/// Checks every rule that can be checked now and stages each row together with its index
/// entries and count change. A refusal may leave part of the statement staged: the caller takes
/// a mark on the change set before the call and rewinds to it on refusal. `now` is the
/// statement's time in milliseconds: what `DEFAULT NOW()` stores and `NOW()` answers.
///
/// `partitions` is the transaction's tally of vector partitions. Every row staged in a table
/// that has a VECTOR column with a PARTITION_KEY is noted in it, and the statement's last act
/// is to settle it: a statement that would give a column one partition too many is refused
/// there, like any other refusal of this function.
///
/// # Panics
///
/// When the plan is not an INSERT, UPDATE or DELETE.
pub(crate) fn apply(
    plan: &Plan,
    params: &HashMap<String, Value>,
    catalog: &Catalog,
    changes: &mut ChangeSet,
    partitions: &mut Tally,
    budget: &Budget,
    now: i64,
) -> Result<u64> {
    match plan {
        Plan::Insert(plan) => insert(plan, params, catalog, changes, partitions, now),
        Plan::Update(plan) => update(plan, params, catalog, changes, partitions, budget, now),
        Plan::Delete(plan) => delete(plan, params, catalog, changes, partitions, budget, now),
        other => unreachable!("{other:?} writes no rows"),
    }
}

/// Stages an entry in a new index for every row the table already has.
///
/// # Panics
///
/// When the table does not list the index, or it is the index behind the primary key.
pub(crate) fn fill_index(
    table: &Table,
    index: IndexId,
    changes: &mut ChangeSet,
    budget: &Budget,
) -> Result<()> {
    // The rows are read and their entries staged a batch at a time, so that what is held here
    // does not grow with the table.
    const BATCH: usize = 1024;
    let rules = Rules::of(table)?;
    let mut indexes = rules.indexes.iter();
    let index = indexes
        .find(|known| known.id == index)
        .expect("a new index is one of its table's indexes that hold entries");
    let mut last: Option<Key> = None;
    loop {
        let after = last.as_ref().map_or(Bound::Unbounded, Bound::Excluded);
        let mut batch = Vec::with_capacity(BATCH);
        // What a batch loads of values kept beside their rows is given back with the batch.
        let mut loaded = budget.reserve(0, "CREATE INDEX")?;
        for found in changes.rows(table.id, after, Bound::Unbounded)?.take(BATCH) {
            let (key, row) = found?;
            let values = index.columns.iter().map(|&at| match &row.cells[at] {
                Cell::Here(value) => Ok(value.clone()),
                Cell::Beside(beside) => changes.load(beside, &mut loaded),
            });
            let values = values.collect::<Result<Vec<_>>>()?;
            batch.push((Key::from_values(&values, &index.directions)?, key));
        }
        let Some((_, key)) = batch.last() else {
            return Ok(());
        };
        last = Some(key.clone());
        for (entry, key) in batch {
            changes.put_index(index.id, entry, key)?;
        }
    }
}

/// Checks the staged rows against the moment the change set is now laid over.
///
/// The commit function calls this, under its lock, after rebasing a change set that another
/// commit overtook; `before` is the moment it was laid over until then. A key or unique value
/// that the other commit took is refused with `UniqueViolation`. A row this transaction changed
/// or deleted that is no longer as it found it is refused with `ConditionalUpdateConflict`,
/// which counts such rows.
///
/// `rows_changed` yields one staged row per key. A row whose key an UPDATE changed is two of
/// them: its old key as a delete, and its new key as a row marked new.
///
/// Costs two lookups per changed row, one per new row, and one per staged row and unique
/// column; the staged rows are walked once for each table of the catalog.
pub(crate) fn check_at_commit(changes: &ChangeSet, catalog: &Catalog, before: &View) -> Result<()> {
    let mut overtaken = 0;
    for table in catalog.tables() {
        let rules = Rules::of(table)?;
        let staged = changes.rows_changed();
        for (_, key, row, new_key) in staged.filter(|staged| staged.0 == table.id) {
            let there = changes.base().row(table.id, key)?;
            if new_key {
                if there.is_some() {
                    return Err(rules.violation(rules.key_columns()));
                }
            } else if before.row(table.id, key)? != there {
                overtaken += 1;
            }
            let Some(row) = row else { continue };
            for index in &rules.indexes {
                if let Some(columns) = index.rule(row)
                    && taken(changes, index.id, &index.entry(row)?, key)?
                {
                    return Err(rules.violation(columns.to_owned()));
                }
            }
        }
    }
    if overtaken > 0 {
        return Err(Error::ConditionalUpdateConflict { count: overtaken });
    }
    Ok(())
}

fn insert(
    plan: &InsertPlan,
    params: &HashMap<String, Value>,
    catalog: &Catalog,
    changes: &mut ChangeSet,
    partitions: &mut Tally,
    now: i64,
) -> Result<u64> {
    let table = find(catalog, &plan.table)?;
    let rules = Rules::of(table)?;
    let columns = &table.meta.columns;
    let named: Vec<usize> = if plan.columns.is_empty() {
        (0..columns.len()).collect()
    } else {
        let named = plan.columns.iter().map(|name| column(table, name));
        named.collect::<Result<_>>()?
    };
    // A column the statement leaves out starts at its default, or at NULL.
    let mut blank = vec![Value::Null; columns.len()];
    for (at, column) in columns.iter().enumerate() {
        if named.contains(&at) {
            continue;
        }
        match &column.default {
            Some(ColumnDefault::Literal(value)) => blank[at] = value.clone(),
            Some(ColumnDefault::Now) => blank[at] = Value::Timestamp(now),
            None => {}
        }
    }

    for given in &plan.values {
        if given.len() != named.len() {
            return Err(Error::PlanError(format!(
                "INSERT INTO {} gives {} values for {} columns",
                table.name,
                given.len(),
                named.len()
            )));
        }
        let mut row = blank.clone();
        for (&at, value) in named.iter().zip(given) {
            row[at] = query::eval(value, table, &[], params, now)?;
        }
        let row = row.into_iter().enumerate();
        let row = row.map(|(at, value)| rules.settle(at, value));
        rules.put(changes, partitions, None, row.collect::<Result<_>>()?)?;
    }
    changes.add_count(table.id, plan.values.len() as i64)?;
    rules.settle_partitions(partitions)?;
    Ok(plan.values.len() as u64)
}

fn update(
    plan: &UpdatePlan,
    params: &HashMap<String, Value>,
    catalog: &Catalog,
    changes: &mut ChangeSet,
    partitions: &mut Tally,
    budget: &Budget,
    now: i64,
) -> Result<u64> {
    let table = find(catalog, &plan.table)?;
    let rules = Rules::of(table)?;
    let set = plan.assignments.iter();
    let set: Vec<_> = set
        .map(|(name, value)| Ok((column(table, name)?, value)))
        .collect::<Result<_>>()?;
    let (found, _held) = query::matching(
        table,
        plan.where_clause.as_ref(),
        params,
        &*changes,
        budget,
        now,
    )?;
    for (key, old) in &found {
        // Every new value is worked out from the row as it was.
        let mut row = old.clone();
        for &(at, value) in &set {
            row[at] = rules.settle(at, query::eval(value, table, old, params, now)?)?;
        }
        // A row that already holds what it is set to is left alone: a statement that changes
        // nothing stages nothing.
        if row != *old {
            rules.put(changes, partitions, Some((key, old)), row)?;
        }
    }
    rules.settle_partitions(partitions)?;
    Ok(found.len() as u64)
}

fn delete(
    plan: &DeletePlan,
    params: &HashMap<String, Value>,
    catalog: &Catalog,
    changes: &mut ChangeSet,
    partitions: &mut Tally,
    budget: &Budget,
    now: i64,
) -> Result<u64> {
    let table = find(catalog, &plan.table)?;
    let rules = Rules::of(table)?;
    let (found, _held) = query::matching(
        table,
        plan.where_clause.as_ref(),
        params,
        &*changes,
        budget,
        now,
    )?;
    for (key, row) in &found {
        rules.note(partitions, changes, Some(key), None)?;
        for index in &rules.indexes {
            changes.delete_index(index.id, &index.entry(row)?, key)?;
        }
        changes.delete_row(table.id, key)?;
    }
    // A statement that finds no row stages nothing, not even a count change of zero.
    if !found.is_empty() {
        changes.add_count(table.id, -(found.len() as i64))?;
    }
    rules.settle_partitions(partitions)?;
    Ok(found.len() as u64)
}

fn find<'a>(catalog: &'a Catalog, table: &str) -> Result<&'a Table> {
    catalog
        .table(table)
        .ok_or_else(|| Error::TableNotFound(table.to_owned()))
}

/// Where a column a statement or an index names is in a row of the table.
fn column(table: &Table, name: &str) -> Result<usize> {
    let mut columns = table.meta.columns.iter();
    columns
        .position(|column| column.name == name)
        .ok_or_else(|| Error::ColumnNotFound {
            table: table.name.clone(),
            column: name.to_owned(),
        })
}

/// What a write needs to know about its table, worked out once for a statement.
struct Rules<'a> {
    table: &'a Table,
    /// Where the primary key's column is in a row: every column the table marks as its key.
    key: Vec<usize>,
    /// The table's indexes that hold entries, in the order the table lists them. The index
    /// behind the primary key (`__pk_...`) is left out: it holds none, a row is kept under its
    /// key.
    indexes: Vec<Index>,
    /// The table has a VECTOR column with a PARTITION_KEY: the rows a statement stages in it
    /// are noted in the transaction's tally of partitions.
    partitioned: bool,
}

/// One index of a table as a write uses it.
struct Index {
    id: IndexId,
    /// Where its columns are in a row, in index order.
    columns: Vec<usize>,
    /// The direction each column was declared with.
    directions: Vec<SortDirection>,
    /// The columns of the unique rule this index is behind, as a refusal names them. `schema`
    /// makes one index for each unique rule and names it `__unique_...`.
    unique: Option<String>,
}

impl<'a> Rules<'a> {
    fn of(table: &'a Table) -> Result<Rules<'a>> {
        let meta = &table.meta;
        let all = 0..meta.columns.len();
        let key = all.filter(|&at| meta.columns[at].primary_key).collect();
        let held = meta.indexes.iter().zip(&table.index_ids);
        let held = held.filter(|(index, _)| !index.name.starts_with("__pk_"));
        let indexes = held.map(|(index, &id)| {
            let names: Vec<&str> = index.columns.iter().map(|(name, _)| &**name).collect();
            let columns = names.iter().map(|name| column(table, name));
            Ok(Index {
                id,
                columns: columns.collect::<Result<_>>()?,
                directions: index.columns.iter().map(|(_, way)| *way).collect(),
                unique: index
                    .name
                    .starts_with("__unique_")
                    .then(|| names.join(", ")),
            })
        });
        let mut vectors = table.vectors.iter();
        Ok(Rules {
            table,
            key,
            indexes: indexes.collect::<Result<_>>()?,
            partitioned: vectors.any(|vector| !vector.partition_key.is_empty()),
        })
    }

    /// Notes in the tally a row the statement is about to stage: `was` is the key the
    /// transaction has had the row under until now, `None` for a new row, and `after` the
    /// values it is staged with, `None` for a removal.
    fn note(
        &self,
        partitions: &mut Tally,
        changes: &ChangeSet,
        was: Option<&Key>,
        after: Option<&[Value]>,
    ) -> Result<()> {
        if !self.partitioned {
            return Ok(());
        }
        let before = match was {
            Some(key) => changes.row(self.table.id, key)?,
            None => None,
        };
        partitions.note(self.table, before.as_ref(), after, changes)
    }

    /// The last act of a statement, once every row is staged and every other rule has passed:
    /// refuses a statement that gave a column one partition too many.
    fn settle_partitions(&self, partitions: &mut Tally) -> Result<()> {
        if self.partitioned {
            partitions.settle(self.table)?;
        }
        Ok(())
    }

    /// Brings a value written into a column to the column's type, and refuses NULL where the
    /// column needs a value: a NOT NULL column and the primary key.
    fn settle(&self, at: usize, value: Value) -> Result<Value> {
        let column = &self.table.meta.columns[at];
        let value = values::typed(&self.table.name, column, value)?;
        if matches!(value, Value::Null) && (!column.nullable || column.primary_key) {
            return Err(Error::ColumnNotNullable {
                table: self.table.name.clone(),
                column: column.name.clone(),
            });
        }
        Ok(value)
    }

    /// Stages a row with its entry in every index, once its key and its unique values are found
    /// free in what the change set shows.
    ///
    /// `was` is the key and the values the row had, for an UPDATE: what the row keeps from
    /// before is its own and is not looked up, an entry that stays as it was is left alone, and
    /// a row whose key changed leaves its old key. The row is noted in `partitions` first.
    fn put(
        &self,
        changes: &mut ChangeSet,
        partitions: &mut Tally,
        was: Option<(&Key, &[Value])>,
        row: Vec<Value>,
    ) -> Result<()> {
        self.note(partitions, changes, was.map(|(key, _)| key), Some(&row))?;
        // No directions: a row's key is encoded ascending.
        let key = Key::from_values(&pick(&self.key, &row), &[])?;
        // A new row, or a row whose key changed: the transaction must find no row at this key.
        let new_key = was.is_none_or(|(old, _)| *old != key);
        if new_key && changes.row(self.table.id, &key)?.is_some() {
            return Err(self.violation(self.key_columns()));
        }
        let own = was.map_or(&key, |(old_key, _)| old_key);
        for index in &self.indexes {
            let entry = index.entry(&row)?;
            let old = was.map(|(_, old)| index.entry(old)).transpose()?;
            let same = old.as_ref() == Some(&entry);
            if same && !new_key {
                continue;
            }
            if !same
                && let Some(columns) = index.rule(&row)
                && taken(&*changes, index.id, &entry, own)?
            {
                return Err(self.violation(columns.to_owned()));
            }
            if let Some(old) = &old {
                changes.delete_index(index.id, old, own)?;
            }
            changes.put_index(index.id, entry, key.clone())?;
        }
        if let Some((old_key, _)) = was
            && new_key
        {
            changes.delete_row(self.table.id, old_key)?;
        }
        changes.put_row(self.table.id, key, row, new_key)
    }

    /// The columns of the primary key, as a refusal names them.
    fn key_columns(&self) -> String {
        let columns = &self.table.meta.columns;
        let names: Vec<&str> = self.key.iter().map(|&at| &*columns[at].name).collect();
        names.join(", ")
    }

    fn violation(&self, column: String) -> Error {
        Error::UniqueViolation {
            table: self.table.name.clone(),
            column,
        }
    }
}

impl Index {
    /// The row's entry in this index.
    fn entry(&self, row: &[Value]) -> Result<Key> {
        Key::from_values(&pick(&self.columns, row), &self.directions)
    }

    /// The columns of the unique rule this index holds the row to. A NULL in any column of the
    /// rule takes the row out of it.
    fn rule(&self, row: &[Value]) -> Option<&str> {
        let columns = self.unique.as_deref()?;
        let whole = !self.columns.iter().any(|&at| row[at] == Value::Null);
        whole.then_some(columns)
    }
}

/// The values a row has in these columns, in this order.
fn pick(columns: &[usize], row: &[Value]) -> Vec<Value> {
    columns.iter().map(|&at| row[at].clone()).collect()
}

/// True when the index has this entry for a row other than `own`.
fn taken(read: &dyn Read, index: IndexId, entry: &Key, own: &Key) -> Result<bool> {
    for found in read.index(index, Bound::Included(entry), Bound::Included(entry))? {
        if found?.1 != *own {
            return Ok(true);
        }
    }
    Ok(false)
}
