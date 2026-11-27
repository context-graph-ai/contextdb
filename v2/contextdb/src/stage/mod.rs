//! stage: applies INSERT, UPDATE and DELETE to a transaction and checks every rule a table can
//! declare: column types, NOT NULL, the primary key and UNIQUE rules, how long a key may be,
//! IMMUTABLE, the state machine, references, a DAG, PROPAGATE, and what the handle's reader
//! may write. It keeps each table's index entries and row count
//! in step with its rows.
//!
//! It never returns a query's result to a caller: it reads only to find the rows an UPDATE or
//! DELETE names and to check a rule.
//!
//! A statement goes row by row: a row is worked out, brought to its columns' types, held against
//! NOT NULL, its key and its unique rules, and staged with its index entries before the next row
//! is looked at, so a later row is checked against the earlier ones through the change set. The
//! caller undoes a refused statement by rewinding the change set.
//!
//! The values that name a row or a group (a primary key, a kept count's values, a partition
//! key) are kept whole and refused past `KEY_LIMIT`. An index entry is made by `Table::entry`,
//! which cuts a long value, so no row is refused for one.

mod commit;
mod counts;
mod propagate;
mod rules;
#[cfg(test)]
mod tests;
pub(crate) use self::commit::{
    check_at_commit, check_provenance, parent, stage_retention, written,
};
pub(crate) use self::counts::fill_kept_count;

use std::cell::RefCell;
use std::collections::{BTreeSet, HashMap};
use std::ops::Bound;

use crate::memory::{Budget, Reservation};
use crate::query::{self, Reader, Visible, equal, most_bytes};
use crate::schema::{Catalog, Quantization, Table};
use crate::sql::plan::InsertPlan;
use crate::sql::{Expr, Plan};
pub(crate) use crate::store::split_cells as split;
use crate::store::{Beside, Cell, ChangeSet, Compact, Entry, IndexId, KEY_LIMIT, Key, Read, Row};
use crate::types::{
    self, ColumnDefault, ColumnType, Error, Result, SortDirection, TxId, Value, shown, typed,
};
use crate::vector::{Reduced, Tally};

/// What a statement that writes rows is run with.
pub(crate) struct Given<'a> {
    pub params: &'a HashMap<String, Value>,
    pub catalog: &'a Catalog,
    pub budget: &'a Budget,
    pub kept: &'a query::Kept,
    /// Who writes: a row this reader may not see is not matched, and a new or changed row it
    /// may not write is refused.
    pub reader: &'a Reader,
    /// The statement's time in milliseconds: what `DEFAULT NOW()` stores and `NOW()` answers.
    pub now: i64,
    /// The transaction's number: the highest a `TXID` column takes from the statement.
    pub number: TxId,
    pub examined: crate::query::Counted<'a>,
    /// Refuses a write to a table whose triggers are not all registered; with none, no check.
    pub ready: Option<Ready<'a>>,
    /// How a change set written through ends a part; with none, the statement stays in memory.
    pub part: Option<Part<'a>>,
}

/// Refuses a write to the table named while one of its triggers has no callback.
pub(crate) type Ready<'a> = &'a dyn Fn(&Catalog, &str) -> Result<()>;

/// Ends a part of a change set written through.
pub(crate) type Part<'a> = &'a dyn Fn(&mut ChangeSet) -> Result<()>;

/// How a declaration ends a part: no row is staged, so it has no pass to run.
fn put(changes: &mut ChangeSet) -> Result<()> {
    changes.put_part(true)
}

impl<'a> Given<'a> {
    /// What this write reads the SELECTs inside its statement with.
    pub(crate) fn writing(&self) -> query::Writing<'a> {
        query::Writing {
            params: self.params,
            catalog: self.catalog,
            budget: self.budget,
            kept: self.kept,
            reader: self.reader,
            now: self.now,
            examined: self.examined,
        }
    }

    pub(crate) fn admin(
        params: &'a HashMap<String, Value>,
        catalog: &'a Catalog,
        budget: &'a Budget,
        reader: &'a Reader,
        kept: &'a query::Kept,
    ) -> Self {
        Self {
            params,
            catalog,
            budget,
            kept,
            reader,
            now: 0,
            number: TxId(0),
            examined: None,
            ready: None,
            part: Some(&put),
        }
    }
}

/// Applies one INSERT, UPDATE or DELETE to the change set and returns the number of rows: those
/// an INSERT stored or, with `ON CONFLICT`, changed, or those the WHERE of an UPDATE or DELETE
/// matched.
///
/// Reads through the change set, so the statement sees what its transaction already wrote.
/// Finds the rows of an UPDATE or DELETE with `query::rows_for_write`, a batch at a time, and
/// stages each batch before it asks for the next; works out values with `query::eval`. Checks
/// every rule that can be checked now and stages each row together with its index entries and
/// count change. A refusal may leave part of the statement staged: the caller takes a mark on
/// the change set before the call and rewinds to it on refusal.
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
    given: &Given,
    changes: &mut ChangeSet,
    partitions: &mut Tally,
) -> Result<u64> {
    let (name, ..) = plan.written().expect("an INSERT, UPDATE or DELETE");
    let table = given.catalog.table(name);
    let table = table.ok_or_else(|| Error::TableNotFound(name.clone()))?;
    if let Some(ready) = given.ready {
        ready(given.catalog, name)?;
    }
    let mut rules = Rules::of(table, given)?;
    rules.changeable(plan)?;
    rules.from = changes.queued;
    // What the reader's filter and the statement's SELECTs hold is given back with it.
    let mut seeing = given.budget.reserve(0, "query")?;
    // Written through, a long list is kept in the set's own records, as a DELETE's is.
    rules.values = if rules.part(changes).is_some() {
        let first = changes.first().clone();
        let slot = RefCell::new(&mut *changes);
        let sets = query::SetContext {
            changes: &slot,
            budget: given.budget,
        };
        query::written(plan, given.writing(), &first, &mut seeing, Some(sets))?
    } else {
        query::written(plan, given.writing(), &*changes, &mut seeing, None)?
    };
    let mut visible = Visible::new(table, given.catalog, given.reader, &mut seeing)?;
    let rows = match plan {
        Plan::Insert(plan) => rules.insert(plan, changes, partitions, &mut visible)?,
        Plan::Update(plan) => {
            let (set, filter) = (table.set(&plan.assignments)?, plan.where_clause.as_ref());
            let params = given.params;
            rules.change(&set, filter, params, changes, partitions, &mut visible)?
        }
        Plan::Delete(plan) => {
            let filter = plan.where_clause.as_ref();
            rules
                .delete(filter, None, changes, partitions, &mut visible)?
                .0
        }
        other => unreachable!("{other:?} writes no rows"),
    };
    propagate::follow(&rules, changes, partitions)?;
    partitions.settle(given.catalog)?;
    Ok(rows)
}

/// Stages an entry in a new index for every row the table already has, a batch of rows at a
/// time, so that what is held here does not grow with the table. The primary key's index,
/// which holds no entries, is passed over.
pub(crate) fn fill_index(
    table: &Table,
    index: IndexId,
    catalog: &Catalog,
    changes: &mut ChangeSet,
    budget: &Budget,
) -> Result<()> {
    let (params, reader) = (HashMap::new(), Reader::admin());
    let kept = query::Kept::new(budget.passing("expression evaluation")?);
    let given = Given::admin(&params, catalog, budget, &reader, &kept);
    let rules = Rules::of(table, &given)?;
    let id = index;
    let index = rules.indexes.iter().find(|known| known.id == id);
    if index.is_none() && table.index_ids.get(table.meta.indexes.len()) != Some(&id) {
        return Ok(());
    }
    let mut needed = vec![false; table.meta.columns.len()];
    for &at in index.into_iter().flat_map(|index| &index.columns) {
        needed[at] = true;
    }
    let asked = rules.asked(None, &params, &[], &needed, &reader);
    let mut cover = budget.passing("index row")?;
    rules.each(&asked, changes, |changes, key, cells| {
        let row = split(cells).0;
        let entry = match index {
            Some(index) => index.entry(&row)?,
            None => table.retention_entry(&changes.row(table.id, &key, &mut cover)?.unwrap()),
        };
        if let (Some(index), Some(entry)) = (index, &entry)
            && index.unique
            && index.rule(&row)
            && rules.taken(changes, index, entry, &row, &key)?
        {
            return Err(rules.violation(&index.columns, &row));
        }
        entry.map_or(Ok(()), |entry| changes.put_index(id, entry, key))
    })
}

pub(crate) fn expire_keys(
    name: &str,
    keys: &[Key],
    catalog: &Catalog,
    budget: &Budget,
    changes: &mut ChangeSet,
    partitions: &mut Tally,
) -> Result<(u64, u64)> {
    let (params, reader) = (HashMap::new(), Reader::admin());
    let table = catalog.table(name).unwrap();
    let kept = query::Kept::new(budget.passing("expression evaluation")?);
    let given = Given::admin(&params, catalog, budget, &reader, &kept);
    let rules = Rules::of(table, &given)?;
    changes.erasing = true;
    let mut seeing = budget.reserve(0, "expiry")?;
    let mut visible = Visible::new(table, catalog, &reader, &mut seeing)?;
    rules.delete(None, Some((keys, true)), changes, partitions, &mut visible)
}

/// What a write needs to know about its table, worked out once for a statement.
pub(crate) struct Rules<'a> {
    table: &'a Table,
    given: &'a Given<'a>,
    /// The table's indexes that hold entries, in the order the table lists them. The index
    /// behind the primary key (`__pk_...`) is left out: it holds none, a row is kept under its
    /// key.
    indexes: Vec<Index<'a>>,
    /// For each VECTOR column with a PARTITION_KEY: where it is in a row, then where the
    /// columns of its key are. The rows a statement stages in such a table are noted in the
    /// transaction's tally of partitions.
    partitions: Vec<Vec<usize>>,
    /// Where the VECTOR columns that keep a vector in one byte a number are in a row.
    reduced: Vec<usize>,
    /// The number of the change set's last queued note when the statement began: the notes
    /// queued after it, its steps and its lifted keys, are the statement's own.
    from: u64,
    /// The values of the SELECTs inside the statement, read before it changed a row.
    pub(crate) values: query::Values,
}

/// One index of a table as a write uses it.
struct Index<'a> {
    id: IndexId,
    /// Where its columns are in a row, in index order.
    columns: Vec<usize>,
    /// Its table, and where it is among the table's indexes: how its entries are made.
    table: &'a Table,
    at: usize,
    /// Whether it is the index of a unique rule, which `schema` names `__unique_...`.
    unique: bool,
}

impl<'a> Rules<'a> {
    pub(crate) fn of(table: &'a Table, given: &'a Given<'a>) -> Result<Rules<'a>> {
        let held = table.meta.indexes.iter().enumerate().zip(&table.index_ids);
        let held = held.filter(|((_, index), _)| !index.name.starts_with("__pk_"));
        let indexes = held.map(|((at, index), &id)| {
            let columns = index.columns.iter().map(|(name, _)| table.column(name));
            Ok(Index {
                id,
                columns: columns.collect::<Result<_>>()?,
                table,
                at,
                unique: index.name.starts_with("__unique_"),
            })
        });
        let reduced = table.vectors.iter();
        let reduced = reduced.filter(|vector| vector.quantization == Quantization::Sq8);
        let vectors = table.vectors.iter();
        let vectors = vectors.filter(|vector| !vector.partition_key.is_empty());
        let partitions = vectors.map(|vector| {
            let names = std::iter::once(&vector.column).chain(&vector.partition_key);
            names.map(|name| table.column(name)).collect()
        });
        Ok(Rules {
            table,
            given,
            indexes: indexes.collect::<Result<_>>()?,
            partitions: partitions.collect::<Result<_>>()?,
            reduced: reduced
                .map(|vector| table.column(&vector.column))
                .collect::<Result<_>>()?,
            from: 0,
            values: Vec::new(),
        })
    }

    /// What a statement asks of the table source.
    pub(crate) fn asked<'b>(
        &'b self,
        filter: Option<&'b Expr>,
        params: &'b HashMap<String, Value>,
        assigned: &'b [usize],
        needed: &'b [bool],
        reader: &'b Reader,
    ) -> query::Asked<'b> {
        let given = self.given;
        let counts = given.reader.is_admin() || !reader.is_admin();
        query::Asked {
            table: self.table,
            keys: None,
            filter,
            assigned,
            needed,
            reader,
            examined: given.examined.filter(|_| counts),
            ctx: query::EvalContext::new(params, self.given.now, self.given.kept)
                .over(&self.values),
        }
    }

    /// How a change set written through ends a part of this statement.
    fn part(&self, changes: &ChangeSet) -> Option<Part<'a>> {
        self.given.part.filter(|_| changes.streaming())
    }

    /// Hands `each` every row that was asked for, a batch at a time: a batch is staged, and
    /// given back, before the next is asked for after its last key, so that one batch is held
    /// beside the change set and never the matched rows twice. Written through, the rows are
    /// read from the moment the set began at, ending a part whenever it is full.
    pub(crate) fn each(
        &self,
        asked: &query::Asked,
        changes: &mut ChangeSet,
        mut each: impl FnMut(&mut ChangeSet, Key, Vec<Cell>) -> Result<()>,
    ) -> Result<()> {
        if let Some(part) = self.part(changes) {
            let read = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
            let first = changes.first().counting(&read);
            let slot = RefCell::new(changes);
            let sets = query::SetContext {
                changes: &slot,
                budget: self.given.budget,
            };
            let asked = query::Asked {
                ctx: asked.ctx.with_sets(Some(sets)),
                ..*asked
            };
            let mut held = self.given.budget.reserve(0, "query_decode")?;
            let walked = query::walk_for_write(
                &asked,
                None,
                self.given.catalog,
                &first,
                &mut held,
                &mut |(key, cells, bytes), held| {
                    let mut changes = slot.borrow_mut();
                    each(&mut changes, key, cells)?;
                    changes.part_full(Some(part))?;
                    held.shrink(bytes);
                    Ok(true)
                },
            );
            let fetched = read.load(std::sync::atomic::Ordering::Relaxed);
            let changes = slot.borrow();
            changes.rows_read.set(changes.rows_read.get() + fetched);
            return walked;
        }
        self.paged(asked, changes, None, each)
    }

    /// Hands `each` the rows asked for as they stand, a batch at a time, ending a `part`
    /// between two batches whenever it is full. A propagation reads its followers so.
    fn paged(
        &self,
        asked: &query::Asked,
        changes: &mut ChangeSet,
        part: Option<Part<'_>>,
        mut each: impl FnMut(&mut ChangeSet, Key, Vec<Cell>) -> Result<()>,
    ) -> Result<()> {
        let (catalog, budget) = (self.given.catalog, self.given.budget);
        let mut after = None;
        while let Some((batch, _held)) =
            query::rows_for_write(asked, Some(&mut after), catalog, &*changes, budget)?
        {
            for (key, cells) in batch {
                each(changes, key, cells)?;
            }
            changes.part_full(part)?;
            if after.is_none() {
                break;
            }
        }
        Ok(())
    }

    fn insert(
        &self,
        plan: &InsertPlan,
        changes: &mut ChangeSet,
        partitions: &mut Tally,
        visible: &mut Visible,
    ) -> Result<u64> {
        let (table, given) = (self.table, self.given);
        let columns = &table.meta.columns;
        let (named, conflict) = table.insert(plan)?;
        // A column the statement leaves out starts at its default, or at NULL.
        let blank_bytes = table.heap + (columns.len() * size_of::<Value>()) as u64;
        let _blank = given.budget.reserve(blank_bytes, "write row")?;
        let mut cover = given.budget.reserve(0, "write row")?;
        let mut blank = vec![Value::Null; columns.len()];
        for (at, column) in columns.iter().enumerate() {
            if named.contains(&at) {
                continue;
            }
            match &column.default {
                Some(ColumnDefault::Literal(value)) => blank[at] = value.clone(),
                Some(ColumnDefault::Now) => blank[at] = Value::Timestamp(given.now),
                None => {}
            }
        }

        let (mut stored, scope) = (0, crate::query::scope(table));
        let ctx = query::EvalContext::new(given.params, given.now, given.kept).over(&self.values);
        for values in &plan.values {
            let most = values.iter().map(|v| most_bytes(v, &scope, &[], &ctx));
            cover.shrink(cover.bytes());
            cover.grow(blank_bytes + most.sum::<u64>())?;
            let mut row = blank.clone();
            for (&at, value) in named.iter().zip(values) {
                row[at] = query::eval(value, &scope, &[], &ctx)?.into_owned();
            }
            let row = row.into_iter().enumerate();
            let row = row.map(|(at, value)| self.settle(at, value, &mut cover));
            let row: Vec<Value> = row.collect::<Result<_>>()?;
            // A row that meets one already there in the named columns changes that row and is
            // not stored. A NULL meets nothing.
            if let Some((on, set)) = &conflict
                && !on.iter().any(|&at| row[at] == Value::Null)
            {
                let (filter, bound) = equal(table, on, &pick(on, &row));
                let filter = Some(&filter);
                if self.change(set, filter, &bound, changes, partitions, visible)? > 0 {
                    continue;
                }
            }
            changes.operation = Some(types::TriggerEvent::Insert);
            self.put(changes, partitions, visible, (None, None), row, Vec::new())?;
            stored += 1;
        }
        // A statement that stores no row stages no count change, not even one of zero.
        if stored > 0 {
            changes.add_count(table.id, stored)?;
        }
        Ok(plan.values.len() as u64)
    }

    /// Changes every row that passes `filter`, whose parameters are `params`, by the
    /// assignments of `set`, and returns how many rows passed.
    ///
    /// A large value is read only where the statement reads it: in an assignment, in the
    /// reader's filter, or with an index, a kept count or a partition key that an assigned
    /// column belongs to. Any other is handed back to the store as the cell it came as.
    fn change(
        &self,
        set: &[(usize, &Expr)],
        filter: Option<&Expr>,
        params: &HashMap<String, Value>,
        changes: &mut ChangeSet,
        partitions: &mut Tally,
        visible: &mut Visible,
    ) -> Result<u64> {
        changes.operation = Some(types::TriggerEvent::Update);
        let (table, given) = (self.table, self.given);
        let assigned: Vec<usize> = set.iter().map(|&(at, _)| at).collect();
        let mut needed = vec![false; table.meta.columns.len()];
        for &(_, value) in set {
            query::reads(
                value,
                &crate::query::scope(table),
                given.params,
                &mut needed,
            )?;
        }
        visible.reads(&mut needed);
        let state = self.state(&assigned, &mut needed)?;
        self.needs(&assigned, &mut needed);
        // A row whose key changes is a new row at its new key, and one whose key or unique
        // value is still held by a row this statement has yet to change is taken out and put
        // back last: either is written whole.
        let unique = self.indexes.iter().filter(|index| index.unique);
        let named = table
            .key
            .iter()
            .chain(unique.flat_map(|index| &index.columns));
        if named.into_iter().any(|at| assigned.contains(at)) {
            needed.fill(true);
        }
        let asked = self.asked(filter, params, &assigned, &needed, given.reader);
        let scope = crate::query::scope(table);
        // The keys this statement has moved rows to: a row met at one of them has been
        // changed already and is passed by.
        let mut reached = BTreeSet::new();
        let mut reserved = given.budget.reserve(0, "query")?;
        // The rows taken out, as they are to be: a rule is held to what the statement leaves,
        // so a row that meets one the statement has yet to change is no refusal yet.
        let mut lifted = Vec::new();
        let bytes = |row: &[Value]| {
            let held = row.iter().map(types::heap_bytes).sum::<u64>();
            held + size_of_val(row) as u64
        };
        // New values are worked out from the row as it was; the set is lent for memberships.
        let ctx = query::EvalContext::new(given.params, given.now, given.kept).over(&self.values);
        // The row a value is worked out for is held until the next: it is staged or lifted.
        let assign = |old: &[Value], cover: &mut Reservation, changes: &mut ChangeSet| {
            let slot = RefCell::new(changes);
            let sets = query::SetContext {
                changes: &slot,
                budget: given.budget,
            };
            let ctx = ctx.with_sets(Some(sets));
            let most = set.iter().map(|(_, v)| most_bytes(v, &scope, old, &ctx));
            cover.shrink(cover.bytes());
            cover.grow(bytes(old) + most.sum::<u64>())?;
            let mut row = old.to_vec();
            for &(at, value) in set {
                let value = query::eval(value, &scope, old, &ctx)?.into_owned();
                row[at] = self.settle(at, value, cover)?;
            }
            Ok::<_, Error>(row)
        };
        let mut cover = given.budget.reserve(0, "write row")?;
        let mut count = 0;
        self.each(&asked, changes, |changes, key, cells| {
            if reached.contains(&key) {
                return Ok(());
            }
            count += 1;
            let (old, mut left) = split(cells);
            // Checked before anything else, so a row this reader may not write is refused even
            // where the statement would leave it as it is.
            visible.may_write(&old, &*changes)?;
            let row = assign(&old, &mut cover, changes)?;
            self.transition(state, &old, &row)?;
            left.retain(|(at, _)| !assigned.contains(at));
            self.moved(&old, &row, &left, changes)?;
            let (was, mark) = (Some((&key, &*old)), changes.mark());
            match self.put(changes, partitions, visible, (was, None), row, left) {
                // Read from the moment the set began at, a moved row is never met again.
                Ok(Some(key)) if self.part(changes).is_none() => {
                    reserved.grow(key.as_bytes().len() as u64)?;
                    reached.insert(key);
                }
                Ok(_) => {}
                Err(Error::UniqueViolation { .. }) => {
                    changes.rewind(mark);
                    let streaming = self.part(changes).is_some();
                    let row = (!streaming).then(|| assign(&old, &mut cover, changes));
                    let cells = old.into_iter().map(Cell::Here).collect();
                    self.remove(changes, Some(partitions), visible, &key, cells)?;
                    // Written through, only the key is queued and the row worked out after.
                    let Some(row) = row.transpose()? else {
                        return changes.queue(b'L', table.id, &key);
                    };
                    reserved.grow(bytes(&row) + key.as_bytes().len() as u64)?;
                    reserved.room(&mut lifted, usize::MAX)?;
                    lifted.push((key, row));
                }
                Err(other) => return Err(other),
            }
            Ok(())
        })?;
        // Put back, a row is refused only by a row the statement leaves.
        // A row put back keeps its key: one under a hidden key is no new row.
        for (key, row) in lifted {
            reserved.shrink(bytes(&row) + key.as_bytes().len() as u64);
            let at = (None, Some(&key));
            self.put(changes, partitions, visible, at, row, Vec::new())?;
        }
        // Only the statement's own lifted keys: a callback's UPDATE finds none after its start.
        let first = changes.first().clone();
        let mut after = [&[b'L'][..], &self.from.to_be_bytes()].concat();
        let mut read = given.budget.passing("lifted row")?;
        while let Some((_, key, _held)) = changes.next_note(b'L', &mut after, given.budget)? {
            let row = first.row(table.id, &key, &mut read)?.expect("a lifted row");
            let width = 0..table.meta.columns.len();
            let (mut old, left) =
                split(width.map(|at| table.cell(&row, at).into_owned()).collect());
            for (at, beside) in left {
                old[at] = first.load(&beside, &mut cover)?;
            }
            let (row, at) = (assign(&old, &mut cover, changes)?, (None, Some(&key)));
            self.put(changes, partitions, visible, at, row, Vec::new())?;
            changes.part_full(self.part(changes))?;
        }
        Ok(count)
    }

    /// A write that changes a column reads every column of each index, count and partition
    /// group that column is in.
    fn needs(&self, changed: &[usize], needed: &mut [bool]) {
        let indexes = self.indexes.iter().map(|index| &index.columns);
        let groups = indexes.chain(&self.partitions);
        let touched = groups.filter(|group| group.iter().any(|at| changed.contains(at)));
        for &at in touched.flatten() {
            needed[at] = true;
        }
        for count in &self.table.rules.counts {
            let columns = self.table.count_columns(count);
            if columns.iter().any(|(at, _)| changed.contains(at)) {
                for &(at, _) in columns {
                    needed[at] = true;
                }
            }
        }
    }

    pub(crate) fn removal_columns(&self) -> Vec<bool> {
        let mut needed = vec![false; self.table.meta.columns.len()];
        let indexes = self.indexes.iter().map(|i| &i.columns);
        for &at in indexes.flatten() {
            needed[at] = true;
        }
        for &(at, _) in self
            .table
            .rules
            .counts
            .iter()
            .flat_map(|c| self.table.count_columns(c))
        {
            needed[at] = true;
        }
        needed
    }

    /// Removes every row that passes `filter` and returns how many did.
    fn delete(
        &self,
        filter: Option<&Expr>,
        keys: Option<(&[Key], bool)>,
        changes: &mut ChangeSet,
        partitions: &mut Tally,
        visible: &mut Visible,
    ) -> Result<(u64, u64)> {
        changes.operation = None;
        let (table, given) = (self.table, self.given);
        // A removed row's values are read only to find its index entries and its counts.
        let needed = self.removal_columns();
        let mut asked = self.asked(filter, given.params, &[], &needed, given.reader);
        asked.keys = keys.map(|(keys, _)| keys);
        let parent =
            keys.is_some_and(|(_, defer)| defer) && given.catalog.children(table).next().is_some();
        let (mut count, mut deferred) = (0_i64, 0);
        self.each(&asked, changes, |changes, key, cells| {
            let mark = changes.mark();
            self.remove(changes, Some(partitions), visible, &key, cells)?;
            if parent
                && let Err(err) =
                    check_at_commit(changes, given.catalog, None, given.budget, |each| {
                        each((table.id, &key, None, false))
                    })
            {
                let Error::ForeignKeyViolation { .. } = err else {
                    return Err(err);
                };
                changes.rewind(mark);
                deferred += 1;
            } else {
                count += 1;
            }
            Ok(())
        })?;
        // A statement that finds no row stages nothing, not even a count change of zero.
        if count > 0 {
            changes.add_count(table.id, -count)?;
        }
        Ok((count as u64, deferred))
    }

    /// Stages the removal of a row the reader may write, read with the columns of every index
    /// and kept count, of its entries, and of its place in each count.
    pub(crate) fn remove(
        &self,
        changes: &mut ChangeSet,
        partitions: Option<&mut Tally>,
        visible: &mut Visible,
        key: &Key,
        cells: Vec<Cell>,
    ) -> Result<()> {
        let before = Row {
            bytes: 0,
            cells,
            written: None,
        };
        if let Some(partitions) = partitions
            && !self.partitions.is_empty()
        {
            partitions.note(self.table, Some(&before), None, &*changes)?;
        }
        // The values move out of their cells, already counted, into a list of their own.
        let slots = size_of_val(before.cells.as_slice()) as u64;
        let _row = self.given.budget.reserve(slots, "deleted row")?;
        let (row, _) = split(before.cells);
        visible.may_write(
            &row,
            match self.part(changes) {
                Some(_) => changes.first(),
                None => &*changes,
            },
        )?;
        self.count(changes, Some(&row), None)?;
        for index in &self.indexes {
            if let Some(entry) = index.entry(&row)? {
                changes.delete_index(index.id, &entry.key, key)?;
            }
        }
        self.keep(changes)?;
        changes.delete_row(self.table.id, key)
    }

    /// The key of values that name a row or a group. It is kept whole, so one longer than
    /// `KEY_LIMIT` is refused, under the name `what` gives.
    fn identity(
        &self,
        what: impl FnOnce() -> String,
        values: &[Value],
        ways: &[SortDirection],
    ) -> Result<Key> {
        let bytes = crate::store::encoded_len(values);
        if bytes > KEY_LIMIT {
            return Err(Error::KeyTooLong {
                table: self.table.name.clone(),
                key: what(),
                bytes: bytes as u64,
                limit: KEY_LIMIT as u64,
            });
        }
        Key::from_values(values, ways)
    }

    /// Brings a value written into a column to the column's type, and refuses NULL where the
    /// column needs a value, a NOT NULL column and the primary key, and a transaction number
    /// above the transaction's own.
    fn settle(&self, at: usize, value: Value, cover: &mut Reservation) -> Result<Value> {
        let column = &self.table.meta.columns[at];
        // A text becomes a JSON tree only once the most the tree can take is counted, and the
        // tree stays counted with its row.
        if let (Value::Text(text), ColumnType::Json) = (&value, column.column_type) {
            cover.grow(types::json_bytes(text.as_bytes()))?;
        }
        let value = typed(&self.table.name, column, value)?;
        if matches!(value, Value::Null) && (!column.nullable || self.table.key.contains(&at)) {
            return Err(Error::ColumnNotNullable {
                table: self.table.name.clone(),
                column: column.name.clone(),
            });
        }
        let own = self.given.number;
        match value {
            Value::TxId(TxId(value)) if value > own.0 => {
                Err(Error::TxIdOutOfRange { value, max: own.0 })
            }
            value => Ok(value),
        }
    }

    /// Stages a row with its entry in every index, once the reader may write it (`change` has
    /// checked the row it was) and its key and its unique values are found free in what the
    /// change set shows. Returns the row's key when an UPDATE moved the row to it.
    ///
    /// `was` is the key and the values the row had, for an UPDATE, with NULL for each large
    /// value that was not read; `left` holds those of them the statement did not assign, which
    /// go back to the store as they came. What the row keeps from before is its own and is
    /// not looked up, an entry whose values stay is left alone, and a row whose key changed
    /// leaves its old key. A table with no primary key keeps a new row under the next row
    /// number, or under `kept`, the key of a row taken out to be put back.
    fn put(
        &self,
        changes: &mut ChangeSet,
        partitions: &mut Tally,
        visible: &mut Visible,
        (was, kept): (Option<(&Key, &[Value])>, Option<&Key>),
        mut row: Vec<Value>,
        left: Vec<(usize, Beside)>,
    ) -> Result<Option<Key>> {
        let table = self.table;
        // A vector in a column that keeps one byte a number is replaced by its reduced form,
        // whose scale and codes go to the store with the row; held until the row is staged.
        let mut compact = Vec::new();
        let mut held = self.given.budget.reserve(0, "query")?;
        for &at in &self.reduced {
            let Value::Vector(vector) = &row[at] else {
                continue;
            };
            held.grow(5 * vector.len() as u64)?;
            if let Some(reduced) = Reduced::of(vector) {
                row[at] = Value::Vector(reduced.numbers());
                let (scale, codes) = (reduced.scale, reduced.codes);
                held.room(&mut compact, usize::MAX)?;
                compact.push(Compact { at, scale, codes });
            }
        }
        visible.may_write(&row, &*changes)?;
        let edge = self.edge(was, &row)?;
        // True when the row is new or has another value in one of these columns.
        let differs = |columns: &[usize]| {
            was.is_none_or(|(_, old)| columns.iter().any(|&at| row[at] != old[at]))
        };
        let key = match was {
            Some((old, _)) if !differs(&table.key) => old.clone(),
            // The transaction's own number, which no other is given, and its count of such rows.
            _ if table.key.is_empty() => match kept {
                Some(kept) => kept.clone(),
                None => {
                    let made = [self.given.number.0, changes.next_row_number()];
                    Key::from_values(&made.map(|number| Value::Int64(number as i64)), &[])?
                }
            },
            // No directions: a row's key is encoded ascending.
            _ => self.identity(|| "primary key".to_owned(), &pick(&table.key, &row), &[])?,
        };
        // A new row, or a row whose key changed: the transaction must find no row at this key.
        let new_key = was.is_none_or(|(old, _)| *old != key);
        if new_key && changes.contains(table.id, &key)? {
            return Err(self.violation(&table.key, &row));
        }
        self.count(changes, was.map(|(_, old)| old), Some(&row))?;
        let own = was.map_or(&key, |(old, _)| old);
        for index in &self.indexes {
            let changed = differs(&index.columns);
            if !changed && !new_key {
                continue;
            }
            let entry = index.entry(&row)?;
            if changed
                && index.rule(&row)
                && let Some(entry) = &entry
                && self.taken(&*changes, index, entry, &row, own)?
            {
                return Err(self.violation(&index.columns, &row));
            }
            if let Some((_, old)) = was
                && let Some(old) = index.entry(old)?
            {
                changes.delete_index(index.id, &old.key, own)?;
            }
            if let Some(entry) = entry {
                changes.put_index(index.id, entry, key.clone())?;
            }
        }
        // Noted once nothing can refuse the row any more.
        if new_key || self.partitions.iter().any(|group| differs(group)) {
            for group in &self.partitions {
                if row[group[0]] != Value::Null {
                    let vector = || table.meta.columns[group[0]].name.clone();
                    self.identity(vector, &pick(&group[1..], &row), &[])?;
                }
            }
            // The row as it was read: a statement that reaches here read all of it that the
            // tally looks at.
            let before = was.map(|(_, old)| Row {
                bytes: 0,
                written: None,
                cells: old.iter().cloned().map(Cell::Here).collect(),
            });
            partitions.note(table, before.as_ref(), Some((&row, &left)), &*changes)?;
        }
        let moved = match was {
            Some((old, _)) if new_key => {
                changes.operation = None;
                changes.delete_row(table.id, old)?;
                changes.operation = Some(types::TriggerEvent::Update);
                Some(key.clone())
            }
            _ => None,
        };
        let mut cells: Vec<Cell> = row.into_iter().map(Cell::Here).collect();
        for (at, beside) in left {
            cells[at] = Cell::Beside(beside);
        }
        self.keep(changes)?;
        changes.put_row(table.id, key, cells, compact, new_key)?;
        self.acyclic(&*changes, edge)?;
        Ok(moved)
    }

    fn keep(&self, changes: &mut ChangeSet) -> Result<()> {
        let meta = &self.table.meta;
        if meta.immutable || meta.history == Some(types::History::CurrentOnly) {
            return Ok(());
        }
        changes.keep_versions(self.table.id, meta.retain.is_none())
    }

    /// True when a row other than `own` holds the values `row` has in the index's columns.
    ///
    /// An entry that holds a cut value says only that such a row may be there: the rows are
    /// then read through the table source and compared by their values.
    fn taken(
        &self,
        read: &dyn Read,
        index: &Index,
        entry: &Entry,
        row: &[Value],
        own: &Key,
    ) -> Result<bool> {
        let at = Bound::Included(&entry.key);
        let mut others = read
            .index(index.id, at, at, false)?
            .filter(|found| !matches!(found, Ok(found) if found.1 == *own));
        if others.next().transpose()?.is_none() {
            return Ok(false);
        }
        if entry.cut.is_none() {
            return Ok(true);
        }
        drop(others);
        let (filter, bound) = equal(self.table, &index.columns, &pick(&index.columns, row));
        let (needed, reader) = (vec![false; row.len()], Reader::admin());
        let asked = self.asked(Some(&filter), &bound, &[], &needed, &reader);
        let given = self.given;
        let found = query::rows_for_write(&asked, None, given.catalog, read, given.budget)?;
        Ok(found.is_some_and(|(rows, _)| rows.iter().any(|(key, _)| key != own)))
    }

    /// The refusal of a row whose values in `columns`, its key or a unique rule's, another row
    /// holds: the columns are named between commas.
    fn violation(&self, columns: &[usize], row: &[Value]) -> Error {
        let names: Vec<&str> = (columns.iter())
            .map(|&at| &*self.table.meta.columns[at].name)
            .collect();
        Error::UniqueViolation {
            table: self.table.name.clone(),
            column: names.join(", "),
            key: shown(&pick(columns, row)),
        }
    }
}

impl Index<'_> {
    /// The row's entry in this index, if it has one.
    fn entry(&self, row: &[Value]) -> Result<Option<Entry>> {
        self.table.entry(self.at, &pick(&self.columns, row))
    }

    /// Whether its unique rule holds the row: a NULL in any column of the rule takes it out.
    fn rule(&self, row: &[Value]) -> bool {
        self.unique && !self.columns.iter().any(|&at| row[at] == Value::Null)
    }
}

/// The values a row has in these columns, in this order.
fn pick(columns: &[usize], row: &[Value]) -> Vec<Value> {
    columns.iter().map(|&at| row[at].clone()).collect()
}
