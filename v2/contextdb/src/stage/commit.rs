//! The rules of a whole transaction, checked by the commit function over everything the
//! transaction staged: every reference, from the row that names a parent and from the parent
//! that is named, and, for a transaction another commit overtook, its keys, its unique values,
//! its edges, the rows it changed and the rows it guarded.

use std::collections::HashMap;
use std::ops::Bound;

use super::{Given, Rules, pick};
use crate::memory::{Budget, Reservation};
use crate::query::{self, Reader, equal};
use crate::schema::{Catalog, Reference, Table};
use crate::store::{Cell, ChangeSet, Key, Read, Row, RowGuard, StagedValues, TableId, View};
use crate::types::{Error, Result, Value, shown};

pub(crate) fn stage_retention(
    changes: &mut ChangeSet,
    catalog: &Catalog,
    now: u32,
    budget: &Budget,
) -> Result<()> {
    let mut held = budget.reserve(0, "retention entries")?;
    let mut cover = held.passing();
    let changed = changes
        .changed_tables()
        .filter_map(|(id, _)| catalog.by_id(id));
    let retained: Vec<&Table> = changed.filter(|t| t.meta.retain.is_some()).collect();
    for table in retained {
        changes.stamp_table(table.id, now);
        let (id, mut keys) = (*table.index_ids.last().unwrap(), Vec::new());
        changes.staged(table.id, &mut |key| {
            held.grow(key.as_bytes().len() as u64 + 32)?;
            keys.push(key.clone());
            Ok(true)
        })?;
        let entry = |r| table.retention_entry(&r);
        for key in keys {
            if let Some(old) = changes
                .base()
                .row(table.id, &key, &mut cover)?
                .and_then(entry)
            {
                changes.delete_index(id, &old.key, &key)?;
            }
            if let Some(new) = changes.row(table.id, &key, &mut cover)?.and_then(entry) {
                changes.put_index(id, new, key)?;
            }
        }
    }
    Ok(())
}

/// Checks everything a transaction staged against the moment its change set is laid over.
///
/// The commit function calls this, under its lock, for every commit. `before` is the moment
/// the change set was laid over until another commit overtook it, and `None` for a
/// transaction that none overtook.
///
/// For every commit, a reference is checked both ways over the complete set, so a parent may
/// be written after the row that names it: a staged row whose reference has no NULL must find
/// its parent row, and a parent row that was removed, or lost the values it was named by, must
/// be named by no row. Both are `ForeignKeyViolation`.
///
/// For an overtaken one, also: a key or unique value that the other commit took is refused
/// with `UniqueViolation`, and an edge that closes a cycle with the other commit's edges with
/// `CycleDetected`. A row this transaction changed or deleted that is no longer as it found
/// it, and a guarded row that is gone or no longer holds its guarded values, are counted and
/// refused together with `ConditionalUpdateConflict`. A transaction that none overtook met
/// each of these rules in its statements, over the moment it commits on.
///
/// `rows_changed` yields one staged row per key. A row whose key an UPDATE changed is two of
/// them: its old key as a delete, and its new key as a row marked new.
///
/// Costs one lookup of the parent per staged row and reference, and one row read and one
/// lookup in the child's index per changed or removed row and reference that names its table;
/// for an overtaken transaction two more lookups per changed row and per guard, one per new
/// row, one per staged row and unique column, and a walk per staged edge of a DAG type.
pub(crate) fn check_at_commit(
    changes: &ChangeSet,
    catalog: &Catalog,
    before: Option<&View>,
    budget: &Budget,
    visit: impl FnOnce(
        &mut dyn FnMut((TableId, &Key, Option<StagedValues<'_>>, bool)) -> Result<()>,
    ) -> Result<()>,
) -> Result<()> {
    let (params, reader) = (HashMap::new(), Reader::admin());
    let kept = query::Kept::new(budget.passing("expression evaluation")?);
    let given = Given::admin(&params, catalog, budget, &reader, &kept);
    let base = changes.first();
    let mut cover = budget.passing("commit row")?;
    // The table the staged rows now walked are of, with the references that name it as their
    // parent, each with its own table.
    let mut of: Option<(Rules, Vec<(&Table, _)>)> = None;
    visit(&mut |(id, key, row, new_key)| {
        if of.as_ref().is_none_or(|(rules, _)| rules.table.id != id) {
            of = None;
            if let Some(table) = catalog.by_id(id) {
                let name = &table.name;
                let children = catalog.children(table).flat_map(|child| {
                    let references = child.rules.references.iter();
                    let named = references.filter(move |reference| reference.parent == *name);
                    named.map(move |reference| (child, reference))
                });
                of = Some((Rules::of(table, &given)?, children.collect()));
            }
        }
        let Some((rules, children)) = &of else {
            return Ok(());
        };
        let table = rules.table;
        let named = !new_key && !children.is_empty();
        let there = match before.is_some() || named {
            true => base.row(id, key, &mut cover)?,
            false => None,
        };
        if let (Some(_), true, Some((row, _)), Some(_)) = (before, new_key, row, &there) {
            return Err(rules.violation(&table.key, row));
        }
        // The values this row was named by until now: when it no longer holds them, no row
        // may name them, unless another row of the table holds them now.
        if let (true, Some(was)) = (named, &there) {
            let mut loaded = budget.reserve(0, "query_decode")?;
            for &(child, reference) in children {
                let at = reference.parent_columns.iter();
                let at: Vec<usize> = at.map(|name| table.column(name)).collect::<Result<_>>()?;
                let old = at.iter().map(|&at| held(table, was, at, base, &mut loaded));
                let old: Vec<Value> = old.collect::<Result<_>>()?;
                let whole = !old.contains(&Value::Null);
                if whole
                    && !row.is_some_and(|(row, _)| pick(&at, row) == old)
                    && holds(
                        child,
                        reference.index,
                        &reference.columns,
                        &old,
                        changes,
                        &given,
                    )?
                    && !parent(table, &reference.parent_columns, &old, changes, &given)?
                {
                    return Err(refused(child, reference, true, &old));
                }
            }
        }
        // A large value the row left where it is reads as NULL here, which takes the row out
        // of a unique rule over it: the statement that staged the row changed no entry of that
        // rule, so the row holds what it held. A reference reads it, as a row written through
        // keeps every large value beside it.
        let Some((row, left)) = row else {
            return Ok(());
        };
        let mut loaded = budget.reserve(0, "query")?;
        for reference in &table.rules.references {
            let value = |&at: &usize| match left.iter().find(|(slot, _)| *slot == at) {
                Some((_, beside)) => changes.load(beside, &mut loaded),
                None => Ok(row[at].clone()),
            };
            let values: Vec<Value> = reference.columns.iter().map(value).collect::<Result<_>>()?;
            if values.contains(&Value::Null) {
                continue;
            }
            let found = match catalog.table(&reference.parent) {
                Some(to) => parent(to, &reference.parent_columns, &values, changes, &given)?,
                None => false,
            };
            if !found {
                return Err(refused(table, reference, false, &values));
            }
        }
        if before.is_none() {
            return Ok(());
        }
        rules.acyclic(changes, rules.edge(None, row)?)?;
        for index in &rules.indexes {
            if index.rule(row)
                && let Some(entry) = index.entry(row)?
                && rules.taken(changes, index, &entry, row, key)?
            {
                return Err(rules.violation(&index.columns, row));
            }
        }
        Ok(())
    })
}

/// Hands `each` every row a change set written through has written, as `check_at_commit`
/// visits it: by its note, read back as it now is, `None` for a row it removed. A row of a
/// table that names no parent and that no reference names is passed by.
pub(crate) fn written(
    changes: &ChangeSet,
    catalog: &Catalog,
    budget: &Budget,
    mut each: impl FnMut((TableId, &Key, Option<StagedValues<'_>>, bool)) -> Result<()>,
) -> Result<()> {
    let checked = |id| {
        let table = catalog.by_id(id);
        table
            .is_some_and(|t| !t.rules.references.is_empty() || catalog.children(t).next().is_some())
    };
    let (mut after, mut cover) = (vec![b'N'], budget.passing("commit row")?);
    while let Some((id, key, row)) =
        changes.next_written(changes, &mut after, &checked, (budget, &mut cover))?
    {
        let row = row.map(|row| super::split(row.cells));
        each((id, &key, row.as_ref().map(|(v, l)| (&v[..], &l[..])), false))?;
    }
    Ok(())
}

/// Two stored values are the same when they sort as equal.
fn same(a: &Value, b: &Value) -> bool {
    crate::query::sort_order(a, b).is_eq()
}

/// Rejects overtaken inputs before callbacks load their large cells.
pub(crate) fn check_provenance(changes: &ChangeSet, before: &View, budget: &Budget) -> Result<()> {
    let base = changes.base();
    let mut overtaken = 0;
    for (id, key, row, new_key) in changes.rows_changed() {
        if !(new_key && row.is_some() || before.same_row(base, id, key, budget, same)?) {
            overtaken += 1;
        }
    }
    for guard in changes.guards() {
        overtaken += u64::from(lost(guard, before, base, budget)?);
    }
    if overtaken > 0 {
        return Err(Error::ConditionalUpdateConflict { count: overtaken });
    }
    Ok(())
}

/// True when a guarded row is not as its guard found it: another commit removed it, or changed
/// a value the guard holds. `before` is the moment the guard was asked over and `now` the one
/// the commit is laid over. A row that is at both moments what it was, whatever its
/// transaction itself has done to it since, is as its guard found it.
fn lost(guard: &RowGuard, before: &View, now: &View, budget: &Budget) -> Result<bool> {
    let mut cover = budget.passing("guard row")?;
    if before.same_row(now, guard.table, &guard.key, budget, same)? {
        return Ok(false);
    }
    let Some(row) = now.row(guard.table, &guard.key, &mut cover)? else {
        return Ok(true);
    };
    let mut loaded = budget.reserve(0, "query_decode")?;
    for (cell, value) in &guard.columns {
        // A position the row has no cell at is a column added after the row was written: the
        // row reads there what it read when the guard was asked.
        let kept = match row.cells.get(*cell) {
            Some(Cell::Here(held)) => held == value,
            Some(Cell::Beside(beside)) => now.load(beside, &mut loaded)? == *value,
            None => true,
        };
        if !kept {
            return Ok(true);
        }
    }
    Ok(false)
}

/// The value a stored row holds in a column: its cell's, loaded when it is kept beside the
/// row, or what the table says a row written before the column was added reads as.
fn held(
    table: &Table,
    row: &Row,
    column: usize,
    read: &dyn Read,
    loaded: &mut Reservation,
) -> Result<Value> {
    match table.cell(row, column).into_owned() {
        Cell::Here(value) => Ok(value),
        Cell::Beside(beside) => read.load(&beside, loaded),
    }
}

/// True when `read` shows a row of `table` that holds `values` in the columns `named`: the
/// columns of its key or of one of its UNIQUE rules, in the rule's order.
pub(crate) fn parent(
    table: &Table,
    named: &[String],
    values: &[Value],
    read: &dyn Read,
    given: &Given,
) -> Result<bool> {
    // A reference names the columns of its rule in the rule's order.
    let rule = table.meta.indexes.iter().position(|index| {
        (index.name.starts_with("__pk_") || index.name.starts_with("__unique_"))
            && index.columns.iter().map(|(name, _)| name).eq(named)
    });
    let Some(rule) = rule else {
        return Ok(false);
    };
    let at = named.iter().map(|name| table.column(name));
    let at: Vec<usize> = at.collect::<Result<_>>()?;
    holds(table, rule, &at, values, read, given)
}

/// True when `read` shows a row of `table` that holds `values` in `columns`, the first columns
/// of the index at `index` of the table's indexes.
///
/// Costs one read of the first row or entry under those values. When one of them is too long
/// to be whole in an entry, the entry says only that such a row may be there: the rows are
/// then read through the table source and compared by their values.
fn holds(
    table: &Table,
    index: usize,
    columns: &[usize],
    values: &[Value],
    read: &dyn Read,
    given: &Given,
) -> Result<bool> {
    // The index behind the primary key holds no entries: a row is kept under its key.
    if table.meta.indexes[index].name.starts_with("__pk_") {
        let key = Key::from_values(values, &[])?;
        return read.contains(table.id, &key);
    }
    let entry = table.entry_shape(index).entry(values)?;
    let at = Bound::Included(&entry.key);
    let found = read.index(table.index_ids[index], at, at, false)?.next();
    if found.transpose()?.is_none() {
        return Ok(false);
    }
    if entry.cut.is_none() {
        return Ok(true);
    }
    let (filter, bound) = equal(table, columns, values);
    let asked = query::Asked {
        keys: None,
        table,
        filter: Some(&filter),
        assigned: &[],
        needed: &vec![false; table.meta.columns.len()],
        reader: given.reader,
        examined: None,
        ctx: query::EvalContext::new(&bound, given.now, given.kept),
    };
    let found = query::rows_for_write(&asked, None, given.catalog, read, given.budget)?;
    Ok(found.is_some())
}

/// The refusal of a reference of `table`, whose columns hold `key`.
fn refused(table: &Table, reference: &Reference, still: bool, key: &[Value]) -> Error {
    let columns = reference.columns.iter();
    let names = columns.map(|&at| &*table.meta.columns[at].name);
    Error::ForeignKeyViolation {
        table: table.name.clone(),
        columns: names.collect::<Vec<_>>().join(", "),
        parent: reference.parent.clone(),
        still_referenced: still,
        key: shown(key),
    }
}
