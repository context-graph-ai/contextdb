//! One standalone removal, choosing from its first view and writing bounded parts.
use crate::database::{Open, Staged};
use crate::memory::Budget;
use crate::query::{self, Visible};
use crate::schema::Table;
use crate::sql::{Expr, Plan, ast::Delete};
use crate::store::{Cell, ChangeSet, Key, PART_BYTES, Read, TableId};
use crate::types::{Error, Result, Value};
use crate::write::{Given, Rules};
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};

type Cached<'a> = (Rules<'a>, Option<Result<Visible<'a>>>, (bool, bool));

pub(crate) fn stage(
    open: &Open,
    selections: &[Delete],
    purge: bool,
    params: &HashMap<String, Value>,
    staged: &mut Staged,
    now: i64,
    examined: crate::query::Counted<'_>,
) -> Result<u64> {
    staged.changes.erasing = purge;
    let first = staged.changes.first().clone();
    let kept = crate::query::Kept::new(staged.budget.passing("purge expression")?);
    let given = Given {
        params,
        catalog: &staged.catalog,
        budget: &staged.budget,
        kept: &kept,
        reader: &staged.reader,
        now,
        number: staged.number,
        examined,
        triggers: Some(&open.triggers),
    };
    // The tables the removal reaches: those it names and, for a PURGE, their children.
    let mut tables = Vec::new();
    for selection in selections {
        crate::events::Triggers::ready(given.triggers, given.catalog, &selection.table)?;
        let named = given.catalog.table(&selection.table);
        tables.push(named.ok_or_else(|| Error::TableNotFound(selection.table.clone()))?);
    }
    let mut at = 0;
    while let Some(parent) = tables.get(at).copied() {
        for child in given.catalog.children(parent).filter(|_| purge) {
            if !tables.iter().any(|t| t.id == child.id) {
                tables.push(child);
            }
        }
        at += 1;
    }
    let mut held = given.budget.reserve(0, "removal predicate")?;
    let mut rules = BTreeMap::new();
    held.grow((tables.len() * size_of::<crate::memory::Reservation>()) as u64)?;
    let mut seeing = (0..tables.len())
        .map(|_| given.budget.reserve(0, "visibility"))
        .collect::<Result<Vec<_>>>()?;
    for (table, seeing) in tables.iter().zip(&mut seeing) {
        let named = |only_purge: bool| {
            given
                .catalog
                .children(table)
                .flat_map(|t| &t.rules.references)
                .any(|r| r.parent == table.name && (r.follows_parent_purge || !only_purge))
        };
        rules.insert(
            table.id,
            (
                Rules::of(table, &given)?,
                Some(Visible::new(table, given.catalog, given.reader, seeing)),
                (named(false), named(true)),
            ),
        );
    }
    let mut count = 0;
    let passes = &mut staged.passes;
    let mut rows = given.budget.passing("removal row")?;
    let mut walk = |table: &Table,
                    filter: Option<&Expr>,
                    params: &HashMap<String, Value>,
                    changes: &mut ChangeSet,
                    (rules, seen, (parent, follows)): &mut Cached| {
        let mut visible = seen.take().unwrap()?;
        let needed = rules.removal_columns();
        let asked = rules.asked(filter, params, &[], &needed, given.reader);
        rules.each(&asked, changes, |changes, key, cells| {
            if !purge || changes.row(table.id, &key, &mut rows)?.is_some() {
                changes.operation = None;
                rules.remove(changes, None, &mut visible, &key, cells)?;
                changes.add_count(table.id, -1)?;
                if purge || *parent {
                    changes.note(table.id, &key)?;
                }
                if purge && *follows {
                    changes.queue(table.id, &key)?;
                }
                count += 1;
                if changes.part_bytes() >= PART_BYTES {
                    open.passes(changes, given.catalog, given.budget, purge, passes)?;
                    changes.put_part(true)?;
                }
            }
            Ok(())
        })?;
        *seen = Some(Ok(visible));
        Ok(())
    };
    for (selection, &table) in selections.iter().zip(&tables) {
        let entry = rules.get_mut(&table.id).unwrap();
        let plan = Plan::Delete(selection.clone());
        let slot = RefCell::new(&mut staged.changes);
        let sets = query::SetContext {
            changes: &slot,
            budget: given.budget,
        };
        entry.0.values = query::written(&plan, &given, &first, &mut held, Some(sets))?;
        if !purge {
            entry.0.changeable(&plan)?;
        }
        walk(
            table,
            selection.where_clause.as_ref(),
            params,
            &mut staged.changes,
            entry,
        )?;
    }
    let mut cover = staged.budget.passing("purge row")?;
    let mut after = vec![b'Q'];
    while purge
        && let Some((id, key, mut loaded)) =
            staged.changes.next_note(b'Q', &mut after, given.budget)?
    {
        let parent = tables.iter().find(|t| t.id == id).expect("queued table");
        let row = first.row(id, &key, &mut cover)?.expect("queued parent");
        for child in given.catalog.children(parent) {
            for reference in child
                .rules
                .references
                .iter()
                .filter(|r| r.follows_parent_purge && r.parent == parent.name)
            {
                let values = reference
                    .parent_columns
                    .iter()
                    .map(|name| {
                        Ok(match &*parent.cell(&row, parent.column(name)?) {
                            Cell::Here(value) => value.clone(),
                            Cell::Beside(beside) => first.load(beside, &mut loaded)?,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                let (filter, bound) = query::equal(child, &reference.columns, &values);
                let entry = rules.get_mut(&child.id).unwrap();
                walk(child, Some(&filter), &bound, &mut staged.changes, entry)?;
            }
        }
    }
    staged.erasing = Some(purge);
    Ok(count)
}

/// Visits removed keys without retaining the journal or a storage guard.
pub(crate) fn noted(
    changes: &ChangeSet,
    budget: &Budget,
    each: &mut dyn FnMut(TableId, &Key) -> Result<()>,
) -> Result<()> {
    let mut after = vec![b'N'];
    while let Some((id, key, _held)) = changes.next_note(b'N', &mut after, budget)? {
        each(id, &key)?;
    }
    Ok(())
}
