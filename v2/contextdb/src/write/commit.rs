//! The rules of a whole transaction, checked by the commit function over everything the
//! transaction staged: every reference, from the row that names a parent and from the parent
//! that is named, and, for a transaction another commit overtook, its keys, its unique values,
//! its edges, the rows it changed and the rows it guarded.

use std::collections::HashMap;
use std::ops::Bound;

use super::{Given, Rules, column, equal, pick};
use crate::memory::{Budget, Reservation};
use crate::query::{self, Reader};
use crate::schema::{Catalog, Reference, Table};
use crate::store::{Cell, ChangeSet, Key, Read, Row, RowGuard, View};
use crate::types::{Error, Result, TxId, Value, shown};

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
) -> Result<()> {
    let (params, reader) = (HashMap::new(), Reader::admin());
    let given = Given {
        params: &params,
        catalog,
        budget,
        reader: &reader,
        now: 0,
        number: TxId(0),
    };
    let (tables, base) = (catalog.tables(), changes.base());
    let mut overtaken = 0;
    // The table the staged rows now walked are of, with the references that name it as their
    // parent, each with its own table.
    let mut of: Option<(Rules, Vec<(&Table, _)>)> = None;
    for (id, key, row, new_key) in changes.rows_changed() {
        if of.as_ref().is_none_or(|(rules, _)| rules.table.id != id) {
            of = None;
            if let Some(table) = tables.iter().find(|table| table.id == id) {
                let name = &table.name;
                let children = tables.iter().flat_map(|child| {
                    let references = child.rules.references.iter();
                    let named = references.filter(move |reference| reference.parent == *name);
                    named.map(move |reference| (*child, reference))
                });
                of = Some((Rules::of(table, &given)?, children.collect()));
            }
        }
        let Some((rules, children)) = &of else {
            continue;
        };
        let table = rules.table;
        let named = !new_key && !children.is_empty();
        let there = match before.is_some() || named {
            true => base.row(id, key)?,
            false => None,
        };
        if let Some(before) = before {
            if let (true, Some((row, _))) = (new_key, row) {
                if there.is_some() {
                    return Err(rules.violation(rules.key_columns(), pick(&table.key, row)));
                }
            } else if before.row(id, key)? != there {
                overtaken += 1;
            }
        }
        // The values this row was named by until now: when it no longer holds them, no row
        // may name them, unless another row of the table holds them now.
        if let (true, Some(was)) = (named, &there) {
            let mut loaded = budget.reserve(0, "query")?;
            for &(child, reference) in children {
                let at = reference.parent_columns.iter();
                let at: Vec<usize> = at.map(|name| column(table, name)).collect::<Result<_>>()?;
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
        // of a rule over it: the statement that staged the row changed no entry of that rule
        // and no value of that reference, so the row holds what it held.
        let Some((row, _)) = row else { continue };
        for reference in &table.rules.references {
            let values = pick(&reference.columns, row);
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
            continue;
        }
        rules.acyclic(changes, rules.edge(None, row)?)?;
        for index in &rules.indexes {
            if let Some(columns) = index.rule(row)
                && rules.taken(changes, index, &index.entry(row)?, row, key)?
            {
                return Err(rules.violation(columns.to_owned(), pick(&index.columns, row)));
            }
        }
    }
    if let Some(before) = before {
        for guard in changes.guards() {
            overtaken += u64::from(lost(guard, before, base, budget)?);
        }
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
    let row = now.row(guard.table, &guard.key)?;
    if before.row(guard.table, &guard.key)? == row {
        return Ok(false);
    }
    let Some(row) = row else { return Ok(true) };
    let mut loaded = budget.reserve(0, "query")?;
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
fn parent(
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
    let at = named.iter().map(|name| column(table, name));
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
        let at = Bound::Included(&key);
        return Ok(read.rows(table.id, at, at)?.next().transpose()?.is_some());
    }
    let entry = table.entry_shape(index).entry(values)?;
    let at = Bound::Included(&entry.key);
    let found = read.index(table.index_ids[index], at, at)?.next();
    if found.transpose()?.is_none() {
        return Ok(false);
    }
    if entry.cut.is_none() {
        return Ok(true);
    }
    let (filter, bound) = equal(table, columns, values);
    let asked = query::Asked {
        table,
        filter: Some(&filter),
        assigned: &[],
        needed: &vec![false; table.meta.columns.len()],
        reader: given.reader,
        params: &bound,
        now: given.now,
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
