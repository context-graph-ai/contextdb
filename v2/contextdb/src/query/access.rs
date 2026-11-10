//! Index choice: which rows of a table a statement has to read, and reading them.

use std::collections::HashMap;
use std::ops::Bound;

use super::expression::{column_index, eval};
use super::{heap_bytes, row_overhead};
use crate::memory::Reservation;
use crate::schema::Table;
use crate::sql::Expr;
use crate::sql::ast::{BinOp, SortDirection as Asked};
use crate::sql::plan::SortKey;
use crate::store::{Cell, IndexId, Key, Read, Row};
use crate::types::{ColumnType, Result, SortDirection, Value};

/// How the rows of one table are reached.
pub(super) enum Path {
    /// The one row with this primary key.
    Key(Key),
    /// The table between two keys, in key order.
    Rows(Bound<Key>, Bound<Key>),
    /// The rows the entries of an index between two entries point at, in entry order.
    Index(IndexId, Bound<Key>, Bound<Key>),
}

/// A part of a filter that every kept row must pass, and that compares a column with a value
/// known before any row is read.
struct Term {
    column: usize,
    op: BinOp,
    value: Value,
}

/// Something that keeps the rows of a table in an order: the table itself, under its primary
/// key, or one of its indexes.
struct Ordered {
    /// `None` for the table itself.
    index: Option<IndexId>,
    /// The columns it is ordered by, each with its direction.
    columns: Vec<(usize, SortDirection)>,
}

impl Ordered {
    /// The key these leading values make: the bytes every key with these values begins with.
    /// A row key is all ascending, which the store is told with no directions.
    fn key(&self, values: &[Value]) -> Result<Key> {
        let directions: Vec<SortDirection> = match self.index {
            Some(_) => self.columns[..values.len()].iter().map(|c| c.1).collect(),
            None => Vec::new(),
        };
        Key::from_values(values, &directions)
    }
}

/// The bound that ends a walk after the last key that begins with this one.
fn until_after(key: &Key) -> Bound<Key> {
    key.prefix_end().map_or(Bound::Unbounded, Bound::Excluded)
}

/// The bound that starts a walk after the last key that begins with this one.
fn starting_after(key: Key) -> Bound<Key> {
    let after = key.prefix_end();
    after.map_or(Bound::Excluded(key), Bound::Included)
}

/// The value as a key of a column of this type holds it, when there is one such value: only
/// then is looking the value up the same as comparing every row with it.
fn as_key(column: ColumnType, value: Value) -> Option<Value> {
    match (column, value) {
        (ColumnType::Integer, value @ Value::Int64(_))
        | (ColumnType::Real, value @ Value::Float64(_))
        | (ColumnType::Text, value @ Value::Text(_))
        | (ColumnType::Boolean, value @ Value::Bool(_))
        | (ColumnType::Uuid, value @ Value::Uuid(_))
        | (ColumnType::Timestamp, value @ Value::Timestamp(_)) => Some(value),
        (ColumnType::Real, Value::Int64(value)) => Some(Value::Float64(value as f64)),
        (ColumnType::Uuid, Value::Text(value)) => value.parse().ok().map(Value::Uuid),
        _ => None,
    }
}

/// One table as one statement reads it.
pub(super) struct Source<'a> {
    table: &'a Table,
    filter: Option<&'a Expr>,
    params: &'a HashMap<String, Value>,
    now: i64,
    read: &'a dyn Read,
    /// The columns the filter reads: taken from every row that is read.
    filtered: Vec<bool>,
    /// The other columns the statement reads: taken only from a row that passed the filter.
    rest: Vec<bool>,
}

impl<'a> Source<'a> {
    /// `filtered` marks the columns the filter reads and `wanted` every column the statement
    /// reads after it. A column in neither is never taken from a row, so a large value in it
    /// is never loaded.
    pub(super) fn new(
        table: &'a Table,
        filter: Option<&'a Expr>,
        params: &'a HashMap<String, Value>,
        now: i64,
        read: &'a dyn Read,
        filtered: Vec<bool>,
        wanted: &[bool],
    ) -> Source<'a> {
        let rest = wanted.iter().zip(&filtered).map(|(w, f)| *w && !*f);
        Source {
            table,
            filter,
            params,
            now,
            read,
            rest: rest.collect(),
            filtered,
        }
    }

    /// Picks the path that reads the fewest rows for the filter, and says whether its rows
    /// arrive in the order asked for, so that they need no sort.
    ///
    /// The table and each index are tried in turn. One is better the more of its leading
    /// columns the filter gives a value for, then if the filter gives a range of its next
    /// column, then if it keeps the order asked for; the table itself wins a tie. The filter is
    /// still applied to every row read, so a path only has to reach every row that can pass.
    ///
    /// Rows are kept under their primary key, so the key is answered from the table itself. Its
    /// own index, `__pk_<column>`, is declared and has no entries, and is never walked.
    pub(super) fn choose(&self, order: &[SortKey]) -> Result<(Path, bool)> {
        let mut terms = Vec::new();
        if let Some(filter) = self.filter {
            self.collect(filter, &mut terms)?;
        }
        let columns = &self.table.meta.columns;
        let column_named = |name: &String| columns.iter().position(|column| column.name == *name);
        let key = self.key_columns();
        // The order asked for, when it is plain columns.
        let asked: Option<Vec<(usize, SortDirection)>> = order
            .iter()
            .map(|key| {
                let Expr::Column(column) = &key.expr else {
                    return None;
                };
                let direction = match key.direction {
                    Asked::Asc => SortDirection::Asc,
                    Asked::Desc => SortDirection::Desc,
                };
                Some((column_index(self.table, column).ok()?, direction))
            })
            .collect();

        let the_table = Ordered {
            index: None,
            columns: key.iter().map(|at| (*at, SortDirection::Asc)).collect(),
        };
        let indexes = self.table.meta.indexes.iter().zip(&self.table.index_ids);
        let indexes = indexes
            .filter(|(index, _)| !index.name.starts_with("__pk_"))
            .map(|(index, id)| Ordered {
                index: Some(*id),
                columns: index
                    .columns
                    .iter()
                    .filter_map(|(name, direction)| Some((column_named(name)?, *direction)))
                    .collect(),
            });

        // Until something better is found: the whole table, in no order that was asked for.
        let mut best = (
            (0, false, false),
            Path::Rows(Bound::Unbounded, Bound::Unbounded),
        );
        for ordered in std::iter::once(the_table).chain(indexes) {
            let given: Vec<Value> = ordered
                .columns
                .iter()
                .map_while(|(column, _)| {
                    let equal = |term: &&Term| term.column == *column && term.op == BinOp::Eq;
                    Some(terms.iter().find(equal)?.value.clone())
                })
                .collect();
            let next = &ordered.columns[given.len()..];
            if ordered.index.is_none() && next.is_empty() {
                return Ok((Path::Key(ordered.key(&given)?), true));
            }
            let (mut from, mut to) = (Bound::Unbounded, Bound::Unbounded);
            if !given.is_empty() {
                let all = ordered.key(&given)?;
                to = until_after(&all);
                from = Bound::Included(all);
            }
            let mut ranged = false;
            if let Some((column, direction)) = next.first() {
                let with = |value: &Value| {
                    let mut values = given.clone();
                    values.push(value.clone());
                    ordered.key(&values)
                };
                let ranges = terms.iter().filter(|term| {
                    let range = matches!(term.op, BinOp::Gt | BinOp::Gte | BinOp::Lt | BinOp::Lte);
                    term.column == *column && range
                });
                for term in ranges {
                    if !ranged {
                        // No NULL is in a range. A key keeps NULL after every value going up
                        // and before every value going down, and the walk stops short of it.
                        let null = with(&Value::Null)?;
                        match direction {
                            SortDirection::Asc => to = Bound::Excluded(null),
                            SortDirection::Desc => from = starting_after(null),
                        }
                        ranged = true;
                    }
                    let key = with(&term.value)?;
                    // A descending column keeps its greater values first. When more columns
                    // follow, many keys begin with this one, and "after it" is after them all.
                    match (term.op, direction) {
                        (BinOp::Gt, SortDirection::Asc) | (BinOp::Lt, SortDirection::Desc) => {
                            from = starting_after(key);
                        }
                        (BinOp::Gte, SortDirection::Asc) | (BinOp::Lte, SortDirection::Desc) => {
                            from = Bound::Included(key);
                        }
                        (BinOp::Lt, SortDirection::Asc) | (BinOp::Gt, SortDirection::Desc) => {
                            to = Bound::Excluded(key);
                        }
                        // `<=` going up and `>=` going down.
                        _ => to = until_after(&key),
                    }
                }
            }
            // A key keeps its columns as ORDER BY sorts them, NULL included, so what follows
            // the columns the filter fixed is in order if it is what was asked for.
            let in_order = asked.as_ref().is_some_and(|asked| next.starts_with(asked));
            let score = (given.len(), ranged, in_order);
            if score > best.0 {
                let path = match ordered.index {
                    Some(index) => Path::Index(index, from, to),
                    None => Path::Rows(from, to),
                };
                best = (score, path);
            }
        }
        let ((_, _, in_order), path) = best;
        Ok((path, in_order))
    }

    /// Where the columns of the primary key sit in a row.
    pub(super) fn key_columns(&self) -> Vec<usize> {
        let columns = &self.table.meta.columns;
        (0..columns.len())
            .filter(|at| columns[*at].primary_key)
            .collect()
    }

    /// Gathers the terms of a filter: the comparisons joined by AND at its top.
    fn collect(&self, filter: &Expr, terms: &mut Vec<Term>) -> Result<()> {
        let Expr::BinaryOp { left, op, right } = filter else {
            return Ok(());
        };
        if *op == BinOp::And {
            self.collect(left, terms)?;
            return self.collect(right, terms);
        }
        let (column, value, op) = match (&**left, &**right) {
            (Expr::Column(column), value @ (Expr::Literal(_) | Expr::Parameter(_))) => {
                (column, value, *op)
            }
            // `5 < id` is `id > 5`.
            (value @ (Expr::Literal(_) | Expr::Parameter(_)), Expr::Column(column)) => {
                let turned = match op {
                    BinOp::Lt => BinOp::Gt,
                    BinOp::Lte => BinOp::Gte,
                    BinOp::Gt => BinOp::Lt,
                    BinOp::Gte => BinOp::Lte,
                    other => *other,
                };
                (column, value, turned)
            }
            _ => return Ok(()),
        };
        let column = column_index(self.table, column)?;
        let value = eval(value, self.table, &[], self.params, self.now)?;
        if let Some(value) = as_key(self.table.meta.columns[column].column_type, value) {
            terms.push(Term { column, op, value });
        }
        Ok(())
    }

    /// Reads the rows the path reaches and hands `each` every one that passes the filter: its
    /// key, its values, and the bytes of `reservation` it holds. Stops when `each` answers
    /// false. Returns how many stored rows were read.
    pub(super) fn walk(
        &self,
        path: &Path,
        reservation: &mut Reservation,
        mut each: impl FnMut(Key, Vec<Value>, u64, &mut Reservation) -> Result<bool>,
    ) -> Result<u64> {
        let mut examined = 0;
        let mut offer = |key: Key, row: Row| {
            examined += 1;
            let before = reservation.bytes();
            match self.values(row, reservation)? {
                Some(values) => {
                    let bytes = reservation.bytes() - before;
                    each(key, values, bytes, reservation)
                }
                None => {
                    reservation.shrink(reservation.bytes() - before);
                    Ok(true)
                }
            }
        };
        let table = self.table.id;
        match path {
            Path::Key(key) => {
                if let Some(row) = self.read.row(table, key)? {
                    offer(key.clone(), row)?;
                }
            }
            Path::Rows(from, to) => {
                for item in self.read.rows(table, from.as_ref(), to.as_ref())? {
                    let (key, row) = item?;
                    if !offer(key, row)? {
                        break;
                    }
                }
            }
            Path::Index(index, from, to) => {
                for entry in self.read.index(*index, from.as_ref(), to.as_ref())? {
                    let (_, key) = entry?;
                    if let Some(row) = self.read.row(table, &key)?
                        && !offer(key, row)?
                    {
                        break;
                    }
                }
            }
        }
        Ok(examined)
    }

    /// The values of a stored row if it passes the filter, charged to `reservation`. A column
    /// the statement does not read is left as NULL.
    fn values(&self, row: Row, reservation: &mut Reservation) -> Result<Option<Vec<Value>>> {
        let mut cells = row.cells;
        reservation.grow(row_overhead(cells.len()))?;
        let mut values = vec![Value::Null; cells.len()];
        self.take(&mut cells, &mut values, &self.filtered, reservation)?;
        if let Some(filter) = self.filter
            && eval(filter, self.table, &values, self.params, self.now)? != Value::Bool(true)
        {
            return Ok(None);
        }
        self.take(&mut cells, &mut values, &self.rest, reservation)?;
        Ok(Some(values))
    }

    /// Moves the marked cells into `values`, loading a value kept beside its row. The memory
    /// of the values is asked for before any is moved or loaded.
    fn take(
        &self,
        cells: &mut [Cell],
        values: &mut [Value],
        marked: &[bool],
        reservation: &mut Reservation,
    ) -> Result<()> {
        let here = cells.iter().zip(marked).map(|(cell, marked)| match cell {
            Cell::Here(here) if *marked => heap_bytes(here),
            _ => 0,
        });
        reservation.grow(here.sum())?;
        for ((cell, value), _) in cells
            .iter_mut()
            .zip(values)
            .zip(marked)
            .filter(|(_, marked)| **marked)
        {
            *value = match std::mem::replace(cell, Cell::Here(Value::Null)) {
                Cell::Here(here) => here,
                Cell::Beside(beside) => self.read.load(&beside, reservation)?,
            };
        }
        Ok(())
    }
}
