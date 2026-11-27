//! One standalone removal, choosing from its first view and writing bounded parts.
use crate::commit::{Open, Staged};
use crate::memory::Reservation;
use crate::query::{self, Visible};
use crate::schema::{Catalog, Reference, Table};
use crate::sql::{Expr, Plan, ast::Delete};
use crate::stage::{self, Given, Rules};
use crate::store::{Cell, ChangeSet, Key, Read, View};
use crate::types::{Error, Result, Value};
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap};

type Cached<'a> = (Rules<'a>, Option<Result<Visible<'a>>>, bool);

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
    let passes = RefCell::new(&mut staged.passes);
    let part = |changes: &mut ChangeSet| {
        let passes = &mut passes.borrow_mut();
        open.part(changes, &staged.catalog, &staged.budget, passes)
    };
    let ready = |catalog: &Catalog, table: &str| open.triggers.ready(catalog, table);
    let given = Given {
        part: Some(&part),
        params,
        catalog: &staged.catalog,
        budget: &staged.budget,
        kept: &kept,
        reader: &staged.reader,
        now,
        number: staged.number,
        examined,
        ready: Some(&ready),
    };
    // Every table named is checked before anything is selected.
    let mut tables: Vec<&Table> = Vec::new();
    for selection in selections {
        if let Some(ready) = given.ready {
            ready(given.catalog, &selection.table)?;
        }
        let named = given.catalog.table(&selection.table);
        let named = named.ok_or_else(|| Error::TableNotFound(selection.table.clone()))?;
        let refused = match () {
            () if tables.iter().any(|t| t.id == named.id) => "is listed twice",
            () if purge && named.key.is_empty() => "has no declared erasure key",
            () => "",
        };
        if !refused.is_empty() {
            let reason = format!("table {} {refused}", selection.table);
            return Err(Error::SchemaInvalid { reason });
        }
        tables.push(named);
    }
    // A PURGE also reaches the children that follow the erase of a table it reaches.
    let mut at = 0;
    while let Some(parent) = tables.get(at).copied() {
        for (child, ..) in followers(given.catalog, parent).filter(|_| purge) {
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
        let children = given.catalog.children(table);
        let named = children
            .flat_map(|t| &t.rules.references)
            .any(|r| r.parent == table.name);
        let visible = Some(Visible::new(table, given.catalog, given.reader, seeing));
        rules.insert(table.id, (Rules::of(table, &given)?, visible, named));
    }
    let count = std::cell::Cell::new(0);
    let mut rows = given.budget.passing("removal row")?;
    // The rows a condition selects; under a PURGE each is taken as any key is.
    let mut walk = |table: &Table,
                    filter: Option<&Expr>,
                    params: &HashMap<String, Value>,
                    changes: &mut ChangeSet,
                    (rules, seen, parent): &mut Cached| {
        let mut visible = seen.take().unwrap()?;
        let needed = rules.removal_columns();
        let asked = rules.asked(filter, params, &[], &needed, given.reader);
        let mut named = vec![false; table.meta.columns.len()];
        followed(given.catalog, table, &mut named)?;
        rules.each(&asked, changes, |changes, key, cells| {
            if purge && changes.row(table.id, &key, &mut rows)?.is_none() {
                return Ok(());
            }
            let mut loading = given.budget.reserve(0, "removal row")?;
            let row = loaded(&first, &named, |at| cells[at].clone(), &mut loading)?;
            changes.operation = None;
            rules.remove(changes, None, &mut visible, &key, cells)?;
            changes.add_count(table.id, -1)?;
            match purge {
                true => drop(take(changes, &given, table, &key, &row, b'N')?),
                false if *parent => changes.note(b'N', table.id, &key)?,
                false => {}
            }
            count.set(count.get() + 1);
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
        entry.0.values = query::written(&plan, given.writing(), &first, &mut held, Some(sets))?;
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
    // Rounds until one takes nothing: the live children of each key taken, by an equality walk
    // on the values its reference names, then the keys with no row of each table owed a look.
    let mut owed: BTreeSet<usize> = (0..selections.len()).filter(|_| purge).collect();
    let mut after = vec![b'F'];
    loop {
        while let Some((id, named, _held)) =
            staged.changes.next_note(b'F', &mut after, given.budget)?
        {
            let at = tables
                .iter()
                .position(|t| t.id == id)
                .expect("follower table");
            // The note's first value is the reference's place, as `follower` made it.
            let values = named.values().expect("follower values");
            let Value::Int64(reference) = values[0] else {
                unreachable!("follower")
            };
            let r = &tables[at].rules.references[reference as usize];
            let (filter, bound) = query::equal(tables[at], &r.columns, &values[1..]);
            let entry = rules.get_mut(&id).unwrap();
            walk(
                tables[at],
                Some(&filter),
                &bound,
                &mut staged.changes,
                entry,
            )?;
            owed.insert(at);
        }
        let Some(at) = owed.pop_first() else {
            break;
        };
        let filter = selections.get(at).map(|s| s.where_clause.as_ref());
        let slot = RefCell::new(&mut staged.changes);
        let (rules, table) = (&rules[&tables[at].id].0, tables[at]);
        count.set(count.get() + rowless(&slot, &given, &first, table, rules, filter)?);
    }
    // A search code holds no value to test, so a reader who sees every row clears, in each
    // table reached, every one whose row is gone.
    for &table in tables.iter().filter(|t| purge && !t.vectors.is_empty()) {
        let mut seeing = given.budget.reserve(0, "visibility")?;
        let visible = Visible::new(table, given.catalog, given.reader, &mut seeing)?;
        let mut limited = vec![false; table.meta.columns.len()];
        visible.reads(&mut limited);
        if limited.contains(&true) {
            continue;
        }
        let changes = &mut staged.changes;
        crate::vector::left_behind(&first, table, &mut |key| match first
            .row(table.id, key, &mut rows)?
        {
            Some(_) => Ok(()),
            None => changes.note(b'V', table.id, key),
        })?;
    }
    staged.erasing = Some(purge);
    Ok(count.get())
}

/// Takes the keys of `table` with no row in the first view whose copy is selected: by `filter`
/// when the statement names the table, or by meeting the values a parent taken left for it.
/// A key is decided by its newest kept version; one with none by each queued delivery's copy,
/// which takes that delivery. Only the columns the visibility, the condition and the references
/// read are loaded. Returns how many keys it took.
fn rowless(
    slot: &RefCell<&mut ChangeSet>,
    given: &Given,
    first: &View,
    table: &Table,
    rules: &Rules,
    filter: Option<Option<&Expr>>,
) -> Result<u64> {
    let (budget, width) = (given.budget, table.meta.columns.len());
    let asked = rules.asked(filter.flatten(), given.params, &[], &[], given.reader);
    let sets = query::SetContext {
        changes: slot,
        budget,
    };
    let asked = query::Asked {
        ctx: asked.ctx.with_sets(Some(sets)),
        ..asked
    };
    let mut seeing = budget.reserve(0, "visibility")?;
    let mut visible = Visible::new(table, given.catalog, given.reader, &mut seeing)?;
    let mut wanted = vec![false; width];
    visible.reads(&mut wanted);
    if let Some(f) = filter.flatten() {
        query::reads(f, &crate::query::scope(table), given.params, &mut wanted)?;
    }
    for &at in table.rules.references.iter().flat_map(|r| &r.columns) {
        wanted[at] = true;
    }
    followed(given.catalog, table, &mut wanted)?;
    let follows = table
        .rules
        .references
        .iter()
        .any(|r| r.follows_parent_purge)
        && slot.borrow().has_note(b'M', table.id, budget)?;
    let mut passes = |row: &[Value]| {
        if visible.hidden(row, first)?.is_some() {
            return Ok(false);
        }
        let mut chosen = match filter {
            Some(Some(f)) => {
                *query::eval(f, &crate::query::scope(table), row, &asked.ctx)? == Value::Bool(true)
            }
            Some(None) => true,
            None => false,
        };
        let references = table.rules.references.iter().enumerate();
        for (at, r) in references.filter(|(_, r)| r.follows_parent_purge) {
            let values: Vec<_> = r.columns.iter().map(|&c| row[c].clone()).collect();
            let named = follower(at, &values)?;
            chosen = chosen
                || !values.contains(&Value::Null)
                    && slot.borrow().noted(b'M', table.id, &named, budget)?;
        }
        if chosen {
            visible.may_write(row, first)?;
        }
        Ok(chosen)
    };
    let mut held = budget.reserve(0, "purge versions")?;
    let named = match filter.flatten() {
        Some(_) if !follows => query::named_keys(&asked, given.catalog, first, &mut held)?,
        _ => None,
    };
    let keys: Box<dyn Iterator<Item = Result<Key>>> = match &named {
        Some(keys) => Box::new(keys.iter().cloned().map(Ok)),
        None => Box::new(first.versioned(table.id)?),
    };
    let (mut cover, mut last, mut taken) = (budget.passing("purge version")?, None, 0);
    for key in keys {
        let key = key?;
        if last.as_ref() == Some(&key) || first.row(table.id, &key, &mut cover)?.is_some() {
            continue;
        }
        let newest = first
            .versions(table.id, &key, &mut cover)?
            .next()
            .transpose()?;
        last = Some(key);
        let (Some((_, row)), Some(key)) = (newest, &last) else {
            continue;
        };
        let mut decoded = budget.reserve(row.bytes, "purge version")?;
        let values = loaded(
            first,
            &wanted,
            |at| table.cell(&row, at).into_owned(),
            &mut decoded,
        )?;
        if passes(&values)? {
            let changes = &mut **slot.borrow_mut();
            changes.operation = None;
            if take(changes, given, table, key, &values, b'N')? {
                changes.delete_row(table.id, key)?;
                taken += 1;
            }
            changes.part_full(given.part)?;
        }
    }
    crate::events::erase_gone(
        slot,
        (table, named.as_deref()),
        budget,
        &mut passes,
        &mut |changes, key, row| {
            taken += u64::from(take(changes, given, table, key, row, b'V')?);
            changes.part_full(given.part)
        },
    )?;
    Ok(taken)
}

/// Notes `key` of `table` taken, once (`kind` `N` for a key with a row or a kept version, `V`
/// for one with only queued deliveries), and leaves, for each reference by which a child
/// follows its erase, the values this copy of it names, once each whichever copy names them:
/// queued for the live children and noted for the copies of children with no row to meet.
/// NULL values follow nothing, nor values a row not taken holds now, in any context. False
/// when the key was taken before.
fn take(
    changes: &mut ChangeSet,
    given: &Given,
    table: &Table,
    key: &Key,
    row: &[Value],
    kind: u8,
) -> Result<bool> {
    let budget = given.budget;
    let first = !changes.noted(b'N', table.id, key, budget)?
        && !changes.noted(b'V', table.id, key, budget)?;
    if first {
        changes.note(kind, table.id, key)?;
    }
    let everyone = query::Reader::admin();
    let whole = Given::admin(given.params, given.catalog, budget, &everyone, given.kept);
    for (child, at, r) in followers(given.catalog, table) {
        let column = |name: &String| Ok(row[table.column(name)?].clone());
        let values = r
            .parent_columns
            .iter()
            .map(column)
            .collect::<Result<Vec<_>>>()?;
        let named = follower(at, &values)?;
        if values.contains(&Value::Null)
            || changes.noted(b'M', child.id, &named, budget)?
            || stage::parent(table, &r.parent_columns, &values, &*changes, &whole)?
        {
            continue;
        }
        changes.queue(b'F', child.id, &named)?;
        changes.note(b'M', child.id, &named)?;
    }
    Ok(first)
}

/// Each reference by which a child of `parent` follows its erase: the child, the reference's
/// place among the child's references, and the reference.
fn followers<'c>(
    catalog: &'c Catalog,
    parent: &'c Table,
) -> impl Iterator<Item = (&'c Table, usize, &'c Reference)> {
    catalog.children(parent).flat_map(move |child| {
        let references = child.rules.references.iter().enumerate();
        references
            .filter(move |(_, r)| r.follows_parent_purge && r.parent == parent.name)
            .map(move |(at, r)| (child, at, r))
    })
}

/// Marks the columns of `table` that the references following its erase name.
fn followed(catalog: &Catalog, table: &Table, wanted: &mut [bool]) -> Result<()> {
    for (.., r) in followers(catalog, table) {
        for name in &r.parent_columns {
            wanted[table.column(name)?] = true;
        }
    }
    Ok(())
}

/// The values a reference at `at` among a child's references names, as one key.
fn follower(at: usize, values: &[Value]) -> Result<Key> {
    Key::from_values(&[&[Value::Int64(at as i64)][..], values].concat(), &[])
}

/// The values of the `wanted` columns, each kept beside the row loaded; NULL for the rest.
fn loaded(
    first: &View,
    wanted: &[bool],
    cell: impl Fn(usize) -> Cell,
    held: &mut Reservation,
) -> Result<Vec<Value>> {
    held.grow((wanted.len() * size_of::<Value>()) as u64)?;
    let each = wanted.iter().enumerate().map(|(at, &wanted)| match wanted {
        false => Ok(Value::Null),
        true => match cell(at) {
            Cell::Here(value) => Ok(value),
            Cell::Beside(beside) => first.load(&beside, held),
        },
    });
    each.collect()
}
