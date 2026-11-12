//! write: applies INSERT, UPDATE and DELETE to a transaction and checks every rule a table can
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
//! key) are kept whole and refused past `KEY_LIMIT`. An index entry is made by
//! `EntryShape::entry`, which cuts a long value, so no row is refused for one.

mod commit;
mod counts;
mod propagate;
mod rules;
#[cfg(test)]
mod tests;
pub(crate) use self::commit::check_at_commit;
pub(crate) use self::counts::fill_kept_count;

use std::cell::RefCell;
use std::collections::{BTreeSet, HashMap};
use std::ops::Bound;

use crate::memory::Budget;
use crate::query::{self, Reader, Visible};
use crate::schema::{Catalog, Quantization, Table};
use crate::sql::ast::{BinOp, ColumnRef};
use crate::sql::plan::InsertPlan;
use crate::sql::{Expr, Plan};
use crate::store::{
    Beside, Cell, ChangeSet, Compact, Entry, EntryShape, IndexId, KEY_LIMIT, Key, Read, Row,
};
use crate::types::{
    self, ColumnDefault, ColumnType, Error, Result, SortDirection, TxId, Value, shown, typed,
};
use crate::vector::{Reduced, Tally};

/// What a statement that writes rows is run with.
pub(crate) struct Given<'a> {
    pub params: &'a HashMap<String, Value>,
    pub catalog: &'a Catalog,
    pub budget: &'a Budget,
    /// Who writes: a row this reader may not see is not matched, and a new or changed row it
    /// may not write is refused.
    pub reader: &'a Reader,
    /// The statement's time in milliseconds: what `DEFAULT NOW()` stores and `NOW()` answers.
    pub now: i64,
    /// The transaction's number: the highest a `TXID` column takes from the statement.
    pub number: TxId,
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
    let name = match plan {
        Plan::Insert(plan) => &plan.table,
        Plan::Update(plan) => &plan.table,
        Plan::Delete(plan) => &plan.table,
        other => unreachable!("{other:?} writes no rows"),
    };
    let table = given.catalog.table(name);
    let table = table.ok_or_else(|| Error::TableNotFound(name.clone()))?;
    let rules = Rules::of(table, given)?;
    rules.changeable(plan)?;
    // What the reader's filter holds is given back with the statement.
    let mut seeing = given.budget.reserve(0, "query")?;
    let mut visible = Visible::new(table, given.catalog, given.reader, &mut seeing)?;
    let rows = match plan {
        Plan::Insert(plan) => rules.insert(plan, changes, partitions, &mut visible)?,
        Plan::Update(plan) => {
            let (set, filter) = (rules.set(&plan.assignments)?, plan.where_clause.as_ref());
            let params = given.params;
            rules.change(&set, filter, params, changes, partitions, &mut visible)?
        }
        Plan::Delete(plan) => {
            let filter = plan.where_clause.as_ref();
            rules.delete(filter, changes, partitions, &mut visible)?
        }
        other => unreachable!("{other:?} writes no rows"),
    };
    propagate::follow(&rules, changes, partitions)?;
    partitions.settle(given.catalog)?;
    Ok(rows)
}

/// Stages an entry in a new index for every row the table already has, a batch of rows at a
/// time, so that what is held here does not grow with the table. An index that holds no
/// entries (the primary key's, or one over a JSON path) is passed over.
pub(crate) fn fill_index(
    table: &Table,
    index: IndexId,
    catalog: &Catalog,
    changes: &mut ChangeSet,
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
    let rules = Rules::of(table, &given)?;
    let Some(index) = rules.indexes.iter().find(|known| known.id == index) else {
        return Ok(());
    };
    let mut needed = vec![false; table.meta.columns.len()];
    for &at in &index.columns {
        needed[at] = true;
    }
    let asked = rules.asked(None, &params, &[], &needed, &reader);
    rules.each(&asked, changes, |changes, key, cells| {
        changes.put_index(index.id, index.entry(&split(cells).0)?, key)
    })
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
    given: &'a Given<'a>,
    /// The table's indexes that hold entries, in the order the table lists them. The index
    /// behind the primary key (`__pk_...`) is left out: it holds none, a row is kept under its
    /// key.
    indexes: Vec<Index<'a>>,
    /// The columns of each count the table keeps, and the direction of each.
    counts: Vec<(Vec<usize>, Vec<SortDirection>)>,
    /// For each VECTOR column with a PARTITION_KEY: where it is in a row, then where the
    /// columns of its key are. The rows a statement stages in such a table are noted in the
    /// transaction's tally of partitions.
    partitions: Vec<Vec<usize>>,
    /// Where the VECTOR columns that keep a vector in one byte a number are in a row.
    reduced: Vec<usize>,
    /// The rows that are to follow the rows whose state the statement has changed so far.
    followed: RefCell<Vec<propagate::Step>>,
}

/// One index of a table as a write uses it.
struct Index<'a> {
    id: IndexId,
    /// Where its columns are in a row, in index order.
    columns: Vec<usize>,
    /// How its entries are made.
    shape: &'a EntryShape,
    /// The columns of the unique rule this index is behind, as a refusal names them. `schema`
    /// makes one index for each unique rule and names it `__unique_...`.
    unique: Option<String>,
}

impl<'a> Rules<'a> {
    fn of(table: &'a Table, given: &'a Given<'a>) -> Result<Rules<'a>> {
        let held = table.meta.indexes.iter().enumerate().zip(&table.index_ids);
        let held = held.filter(|((_, index), _)| {
            !index.name.starts_with("__pk_") && index.json_path.is_empty()
        });
        let indexes = held.map(|((at, index), &id)| {
            let names: Vec<&str> = index.columns.iter().map(|(name, _)| &**name).collect();
            let columns = names.iter().map(|name| column(table, name));
            Ok(Index {
                id,
                columns: columns.collect::<Result<_>>()?,
                shape: table.entry_shape(at),
                unique: index
                    .name
                    .starts_with("__unique_")
                    .then(|| names.join(", ")),
            })
        });
        let counts = table.rules.counts.iter();
        let reduced = table.vectors.iter();
        let reduced = reduced.filter(|vector| vector.quantization == Quantization::Sq8);
        let vectors = table.vectors.iter();
        let vectors = vectors.filter(|vector| !vector.partition_key.is_empty());
        let partitions = vectors.map(|vector| {
            let names = std::iter::once(&vector.column).chain(&vector.partition_key);
            names.map(|name| column(table, name)).collect()
        });
        Ok(Rules {
            table,
            given,
            indexes: indexes.collect::<Result<_>>()?,
            counts: counts
                .map(|count| count.columns.iter().copied().unzip())
                .collect(),
            partitions: partitions.collect::<Result<_>>()?,
            reduced: reduced
                .map(|vector| column(table, &vector.column))
                .collect::<Result<_>>()?,
            followed: RefCell::default(),
        })
    }

    /// What a statement asks of the table source.
    fn asked<'b>(
        &'b self,
        filter: Option<&'b Expr>,
        params: &'b HashMap<String, Value>,
        assigned: &'b [usize],
        needed: &'b [bool],
        reader: &'b Reader,
    ) -> query::Asked<'b> {
        query::Asked {
            table: self.table,
            filter,
            assigned,
            needed,
            reader,
            params,
            now: self.given.now,
        }
    }

    /// Hands `each` every row that was asked for, a batch at a time: a batch is staged, and
    /// given back, before the next is asked for after its last key, so that one batch is held
    /// beside the change set and never the matched rows twice.
    fn each(
        &self,
        asked: &query::Asked,
        changes: &mut ChangeSet,
        mut each: impl FnMut(&mut ChangeSet, Key, Vec<Cell>) -> Result<()>,
    ) -> Result<()> {
        let (catalog, budget) = (self.given.catalog, self.given.budget);
        let mut after = None;
        while let Some((batch, _held)) =
            query::rows_for_write(asked, after.as_ref(), catalog, &*changes, budget)?
        {
            after = batch.last().map(|(key, _)| key.clone());
            for (key, cells) in batch {
                each(changes, key, cells)?;
            }
        }
        Ok(())
    }

    /// Where each assigned column is in a row, with its new value.
    fn set<'e>(&self, assignments: &'e [(String, Expr)]) -> Result<Vec<(usize, &'e Expr)>> {
        self.targets(assignments.iter().map(|(name, value)| (name, value)))
    }

    /// Where each column a statement writes is in a row, with what is written to it; one
    /// named twice is refused.
    fn targets<'n, T>(
        &self,
        named: impl Iterator<Item = (&'n String, T)>,
    ) -> Result<Vec<(usize, T)>> {
        let mut targets: Vec<(usize, T)> = Vec::new();
        for (name, with) in named {
            let at = column(self.table, name)?;
            if targets.iter().any(|(target, _)| *target == at) {
                let table = &self.table.name;
                let refusal = format!("{name} is named twice as a column {table} is written in");
                return Err(Error::PlanError(refusal));
            }
            targets.push((at, with));
        }
        Ok(targets)
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
        let named: Vec<usize> = if plan.columns.is_empty() {
            (0..columns.len()).collect()
        } else {
            let named = self.targets(plan.columns.iter().map(|name| (name, ())))?;
            named.into_iter().map(|(at, ())| at).collect()
        };
        // A column the statement leaves out starts at its default, or at NULL.
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
        // `ON CONFLICT` names the primary key or a whole UNIQUE rule, in any order.
        let conflict = match &plan.on_conflict {
            Some(conflict) => {
                let on = conflict.columns.iter().map(|name| column(table, name));
                let mut on: Vec<usize> = on.collect::<Result<_>>()?;
                on.sort_unstable();
                let is_on = |columns: &[usize]| {
                    let mut columns = columns.to_vec();
                    columns.sort_unstable();
                    columns == on
                };
                let mut unique = self.indexes.iter().filter(|index| index.unique.is_some());
                if !is_on(&table.key) && !unique.any(|index| is_on(&index.columns)) {
                    return Err(Error::PlanError(format!(
                        "ON CONFLICT ({}) names neither the primary key nor a UNIQUE rule of {}",
                        conflict.columns.join(", "),
                        table.name
                    )));
                }
                Some((on, self.set(&conflict.assignments)?))
            }
            None => None,
        };

        let (mut stored, scope) = (0, table.scope());
        for values in &plan.values {
            if values.len() != named.len() {
                return Err(Error::PlanError(format!(
                    "INSERT INTO {} gives {} values for {} columns",
                    table.name,
                    values.len(),
                    named.len()
                )));
            }
            let mut row = blank.clone();
            for (&at, value) in named.iter().zip(values) {
                let ctx = query::EvalContext::new(given.params, given.now);
                row[at] = query::eval(value, &scope, &[], &ctx)?;
            }
            let row = row.into_iter().enumerate();
            let row = row.map(|(at, value)| self.settle(at, value));
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
            self.put(changes, partitions, visible, None, row, Vec::new())?;
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
        let (table, given) = (self.table, self.given);
        let assigned: Vec<usize> = set.iter().map(|&(at, _)| at).collect();
        let mut needed = vec![false; table.meta.columns.len()];
        for &(_, value) in set {
            query::reads(value, table, given.params, &mut needed)?;
        }
        visible.reads(&mut needed);
        let state = self.state(&assigned, &mut needed)?;
        self.needs(&assigned, &mut needed);
        // A row whose key changes is a new row at its new key, and one whose key or unique
        // value is still held by a row this statement has yet to change is taken out and put
        // back last: either is written whole.
        let unique = self.indexes.iter().filter(|index| index.unique.is_some());
        let named = table
            .key
            .iter()
            .chain(unique.flat_map(|index| &index.columns));
        if named.into_iter().any(|at| assigned.contains(at)) {
            needed.fill(true);
        }
        let asked = self.asked(filter, params, &assigned, &needed, given.reader);
        let scope = table.scope();
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
        // Every new value is worked out from the row as it was.
        let assign = |old: &[Value]| -> Result<Vec<Value>> {
            let mut row = old.to_vec();
            for &(at, value) in set {
                let ctx = query::EvalContext::new(given.params, given.now);
                row[at] = self.settle(at, query::eval(value, &scope, old, &ctx)?)?;
            }
            Ok(row)
        };
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
            let row = assign(&old)?;
            self.transition(state, &old, &row)?;
            let unread = left.len();
            left.retain(|(at, _)| !assigned.contains(at));
            // A row that already holds what it is set to is left alone: a statement that
            // changes nothing stages nothing.
            if row == old && left.len() == unread {
                return Ok(());
            }
            self.moved(&old, &row, &left, &*changes)?;
            let (was, mark) = (Some((&key, &*old)), changes.mark());
            match self.put(changes, partitions, visible, was, row, left) {
                Ok(Some(key)) => {
                    reserved.grow(key.as_bytes().len() as u64)?;
                    reached.insert(key);
                }
                Ok(None) => {}
                Err(Error::UniqueViolation { .. }) => {
                    changes.rewind(mark);
                    let row = assign(&old)?;
                    reserved.grow(bytes(&row))?;
                    let cells = old.into_iter().map(Cell::Here).collect();
                    self.remove(changes, partitions, visible, &key, cells)?;
                    lifted.push(row);
                }
                Err(other) => return Err(other),
            }
            Ok(())
        })?;
        // Put back, a row is refused only by a row the statement leaves.
        for row in lifted {
            reserved.shrink(bytes(&row));
            self.put(changes, partitions, visible, None, row, Vec::new())?;
        }
        Ok(count)
    }

    /// A write that changes a column reads every column of each index, count and partition
    /// group that column is in.
    fn needs(&self, changed: &[usize], needed: &mut [bool]) {
        let indexes = self.indexes.iter().map(|index| &index.columns);
        let counts = self.counts.iter().map(|(columns, _)| columns);
        let groups = indexes.chain(counts).chain(&self.partitions);
        let touched = groups.filter(|group| group.iter().any(|at| changed.contains(at)));
        for &at in touched.flatten() {
            needed[at] = true;
        }
    }

    /// Removes every row that passes `filter` and returns how many did.
    fn delete(
        &self,
        filter: Option<&Expr>,
        changes: &mut ChangeSet,
        partitions: &mut Tally,
        visible: &mut Visible,
    ) -> Result<u64> {
        let (table, given) = (self.table, self.given);
        // A removed row's values are read only to find its index entries and its counts.
        let mut needed = vec![false; table.meta.columns.len()];
        let indexes = self.indexes.iter().map(|index| &index.columns);
        let counts = self.counts.iter().map(|(columns, _)| columns);
        for &at in indexes.chain(counts).flatten() {
            needed[at] = true;
        }
        let asked = self.asked(filter, given.params, &[], &needed, given.reader);
        let mut count = 0_i64;
        self.each(&asked, changes, |changes, key, cells| {
            count += 1;
            self.remove(changes, partitions, visible, &key, cells)
        })?;
        // A statement that finds no row stages nothing, not even a count change of zero.
        if count > 0 {
            changes.add_count(table.id, -count)?;
        }
        Ok(count as u64)
    }

    /// Stages the removal of a row the reader may write, read with the columns of every index
    /// and kept count, of its entries, and of its place in each count.
    fn remove(
        &self,
        changes: &mut ChangeSet,
        partitions: &mut Tally,
        visible: &mut Visible,
        key: &Key,
        cells: Vec<Cell>,
    ) -> Result<()> {
        let before = Row { cells };
        if !self.partitions.is_empty() {
            partitions.note(self.table, Some(&before), None, &*changes)?;
        }
        let (row, _) = split(before.cells);
        visible.may_write(&row, &*changes)?;
        self.count(changes, Some(&row), None)?;
        for index in &self.indexes {
            changes.delete_index(index.id, &index.entry(&row)?.key, key)?;
        }
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
        let key = Key::from_values(values, ways)?;
        let bytes = key.as_bytes().len();
        if bytes > KEY_LIMIT {
            return Err(Error::KeyTooLong {
                table: self.table.name.clone(),
                key: what(),
                bytes: bytes as u64,
                limit: KEY_LIMIT as u64,
            });
        }
        Ok(key)
    }

    /// Brings a value written into a column to the column's type, and refuses NULL where the
    /// column needs a value, a NOT NULL column and the primary key, and a transaction number
    /// above the transaction's own.
    fn settle(&self, at: usize, value: Value) -> Result<Value> {
        let column = &self.table.meta.columns[at];
        // A text becomes a JSON tree only once the most the tree can take is counted.
        let _tree = match (&value, column.column_type) {
            (Value::Text(text), ColumnType::Json) => Some(
                self.given
                    .budget
                    .reserve(types::json_bytes(text.as_bytes()), "query")?,
            ),
            _ => None,
        };
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
    /// number.
    fn put(
        &self,
        changes: &mut ChangeSet,
        partitions: &mut Tally,
        visible: &mut Visible,
        was: Option<(&Key, &[Value])>,
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
            _ if table.key.is_empty() => {
                let made = [self.given.number.0, changes.next_row_number()];
                Key::from_values(&made.map(|number| Value::Int64(number as i64)), &[])?
            }
            // No directions: a row's key is encoded ascending.
            _ => self.identity(|| "primary key".to_owned(), &pick(&table.key, &row), &[])?,
        };
        // A new row, or a row whose key changed: the transaction must find no row at this key.
        let new_key = was.is_none_or(|(old, _)| *old != key);
        if new_key && changes.row(table.id, &key)?.is_some() {
            return Err(self.violation(self.key_columns(), pick(&table.key, &row)));
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
                && let Some(columns) = index.rule(&row)
                && self.taken(&*changes, index, &entry, &row, own)?
            {
                return Err(self.violation(columns.to_owned(), pick(&index.columns, &row)));
            }
            if let Some((_, old)) = was {
                changes.delete_index(index.id, &index.entry(old)?.key, own)?;
            }
            changes.put_index(index.id, entry, key.clone())?;
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
                cells: old.iter().cloned().map(Cell::Here).collect(),
            });
            partitions.note(table, before.as_ref(), Some((&row, &left)), &*changes)?;
        }
        let moved = match was {
            Some((old, _)) if new_key => {
                changes.delete_row(table.id, old)?;
                Some(key.clone())
            }
            _ => None,
        };
        let mut cells: Vec<Cell> = row.into_iter().map(Cell::Here).collect();
        for (at, beside) in left {
            cells[at] = Cell::Beside(beside);
        }
        changes.put_row(table.id, key, cells, compact, new_key)?;
        self.acyclic(&*changes, edge)?;
        Ok(moved)
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
            .index(index.id, at, at)?
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

    /// The columns of the primary key, as a refusal names them.
    fn key_columns(&self) -> String {
        let columns = &self.table.meta.columns;
        let key = self.table.key.iter();
        let names: Vec<&str> = key.map(|&at| &*columns[at].name).collect();
        names.join(", ")
    }

    fn violation(&self, column: String, key: Vec<Value>) -> Error {
        Error::UniqueViolation {
            table: self.table.name.clone(),
            column,
            key: shown(&key),
        }
    }
}

impl Index<'_> {
    /// The row's entry in this index.
    fn entry(&self, row: &[Value]) -> Result<Entry> {
        self.shape.entry(&pick(&self.columns, row))
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

/// What passes exactly the rows that hold `values` in `columns`, side by side, and the
/// parameters it reads them from: each under its column's name.
fn equal(table: &Table, columns: &[usize], values: &[Value]) -> (Expr, HashMap<String, Value>) {
    let mut bound = HashMap::new();
    let each = columns.iter().zip(values).map(|(&at, value)| {
        let name = &table.meta.columns[at].name;
        bound.insert(name.clone(), value.clone());
        let column = ColumnRef {
            table: None,
            column: name.clone(),
        };
        Expr::BinaryOp {
            left: Box::new(Expr::Column(column)),
            op: BinOp::Eq,
            right: Box::new(Expr::Parameter(name.clone())),
        }
    });
    let filter = each.reduce(|left, right| Expr::BinaryOp {
        left: Box::new(left),
        op: BinOp::And,
        right: Box::new(right),
    });
    let filter = filter.expect("a key and a UNIQUE rule have a column");
    (filter, bound)
}

/// A row's values in column order, with NULL for each large value that was left beside the
/// row, and those cells with the columns they are of.
fn split(cells: Vec<Cell>) -> (Vec<Value>, Vec<(usize, Beside)>) {
    let mut left = Vec::new();
    let values = cells.into_iter().enumerate().map(|(at, cell)| match cell {
        Cell::Here(value) => value,
        Cell::Beside(beside) => {
            left.push((at, beside));
            Value::Null
        }
    });
    let values = values.collect();
    (values, left)
}
