//! query: answers a SELECT from one table: which rows to read and through which index, the
//! filter, the select list, COUNT, SUM, MIN and MAX, ORDER BY and LIMIT, and a similarity
//! search, whose rows the vector part finds. It holds the one expression evaluator, and counts
//! exactly the rows it reads.
//!
//! It never changes anything.

mod access;
mod expression;
mod search;
#[cfg(test)]
mod tests;

use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;

pub(crate) use expression::eval;
pub(crate) use search::vector_from_text;

use crate::memory::{Budget, Reservation};
use crate::schema::{Catalog, Table};
use crate::sql::ast::SortDirection;
use crate::sql::plan::{ProjectColumn, SortKey};
use crate::sql::{Expr, Plan};
use crate::store::{Key, Read};
use crate::types::{ColumnType, Error, Result, Value};
use access::Source;
use expression::{
    Arithmetic, arithmetic, column_index, compare, most_bytes, reads, sort_order, unsupported,
};

/// What a statement returned: the column names and rows of a SELECT, or the number of rows a
/// write affected. Its rows stay counted against the memory limit until it is dropped.
pub struct QueryResult {
    columns: Vec<String>,
    rows: Vec<Vec<Value>>,
    rows_affected: u64,
    rows_examined: u64,
    /// Never read: held so that the memory the rows take stays counted against the limit, and
    /// dropping it with the result gives that memory back.
    _reservation: Option<Reservation>,
}

impl fmt::Debug for QueryResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QueryResult")
            .field("columns", &self.columns)
            .field("rows", &self.rows)
            .field("rows_affected", &self.rows_affected)
            .field("rows_examined", &self.rows_examined)
            .finish_non_exhaustive()
    }
}

impl QueryResult {
    /// The column names of a SELECT, in order; empty for a write.
    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    /// The rows of a SELECT, each a list of values in column order; empty for a write.
    pub fn rows(&self) -> &[Vec<Value>] {
        &self.rows
    }

    /// How many rows a write changed; zero for a SELECT.
    pub fn rows_affected(&self) -> u64 {
        self.rows_affected
    }

    /// How many stored rows the statement read to produce this result. An exact count, the same
    /// on every run over the same store: a whole-table `COUNT(*)` reads none, a lookup by key
    /// reads one. Speed is guarded in tests by this number, never by a time.
    pub fn rows_examined(&self) -> u64 {
        self.rows_examined
    }

    /// The result of a write that changed `rows` rows.
    pub(crate) fn affected(rows: u64) -> QueryResult {
        QueryResult {
            rows_affected: rows,
            ..QueryResult::from_rows(Vec::new(), Vec::new())
        }
    }

    /// The result of a statement its caller answers itself, such as `SHOW MEMORY_LIMIT`: these
    /// columns and rows, with no row affected and none examined.
    pub(crate) fn from_rows(columns: Vec<String>, rows: Vec<Vec<Value>>) -> QueryResult {
        QueryResult {
            columns,
            rows,
            rows_affected: 0,
            rows_examined: 0,
            _reservation: None,
        }
    }
}

/// What `SHOW MEMORY_LIMIT` answers for one budget, a writer's or a reading session's.
///
/// One row: `limit`, `used` and `available` in bytes; `startup_ceiling`, the limit the writer or
/// the session was opened under; and `ceiling_refresh_failures`, which is always 0 because this
/// engine reads no ceiling from the operating system. No limit is the text `none`, and a number
/// of bytes larger than an INTEGER column holds is shown as the largest it holds.
pub(crate) fn memory_limit_shown(budget: &Budget, opened_under: Option<u64>) -> QueryResult {
    let bytes = |bytes: Option<u64>| match bytes {
        Some(bytes) => Value::Int64(i64::try_from(bytes).unwrap_or(i64::MAX)),
        None => Value::Text("none".to_owned()),
    };
    let (limit, used) = (budget.limit(), budget.used());
    let columns = [
        "limit",
        "used",
        "available",
        "startup_ceiling",
        "ceiling_refresh_failures",
    ];
    let row = vec![
        bytes(limit),
        bytes(Some(used)),
        bytes(limit.map(|limit| limit.saturating_sub(used))),
        bytes(opened_under),
        Value::Int64(0),
    ];
    QueryResult::from_rows(columns.map(str::to_owned).to_vec(), vec![row])
}

/// Answers one planned SELECT from one reader.
///
/// Picks an index when the plan's filter can use one and scans otherwise; `COUNT(*)` of a whole
/// table is answered from the kept count. Loads a value kept beside its row only when a selected
/// or filtered column needs it. The result, and any sort or grouping on the way to it, grows
/// through reservations on `budget` and refuses with `MemoryBudgetExceeded` before it grows.
///
/// A similarity search, `ORDER BY column <=> vector LIMIT k`, is answered by the vector part
/// from the rows this part lets it see, and returns the nearest rows first.
pub(crate) fn run(
    plan: &Plan,
    params: &HashMap<String, Value>,
    catalog: &Catalog,
    read: &dyn Read,
    budget: &Budget,
    now: i64,
) -> Result<QueryResult> {
    if let Some(found) = search::run(plan, params, catalog, read, budget, now)? {
        return Ok(found);
    }
    let select = Select::of(plan)?;
    let table = catalog
        .table(select.table)
        .ok_or_else(|| Error::TableNotFound(select.table.to_owned()))?;

    // Everything the statement names is looked up before a row is read.
    let width = table.meta.columns.len();
    let (mut filtered, mut wanted) = (vec![false; width], vec![false; width]);
    if let Some(filter) = select.filter {
        reads(filter, table, params, &mut filtered)?;
    }
    for key in select.order {
        reads(&key.expr, table, params, &mut wanted)?;
    }
    let mut aggregates = Vec::new();
    match select.columns {
        None => wanted.fill(true),
        Some(columns) => {
            for column in columns {
                let Some(aggregate) = Aggregate::of(&column.expr)? else {
                    reads(&column.expr, table, params, &mut wanted)?;
                    continue;
                };
                if let Some(of) = aggregate.of {
                    reads(of, table, params, &mut wanted)?;
                    aggregate.check(table, of)?;
                }
                aggregates.push(aggregate);
            }
            if !aggregates.is_empty() && aggregates.len() < columns.len() {
                return Err(unsupported("a column beside an aggregate (GROUP BY)"));
            }
        }
    }
    let columns = names(table, select.columns, !aggregates.is_empty());

    let source = Source::new(table, select.filter, params, now, read, filtered, &wanted);
    let limit = select.limit.map_or(usize::MAX, |limit| {
        usize::try_from(limit).unwrap_or(usize::MAX)
    });
    let mut reservation = budget.reserve(0, "query")?;
    let mut rows = Vec::new();
    let mut examined = 0;

    if !aggregates.is_empty() {
        if select.filter.is_none() && aggregates.iter().all(|aggregate| aggregate.of.is_none()) {
            let count = read.count(table.id)? as i64;
            for aggregate in &mut aggregates {
                aggregate.seen = count;
            }
        } else {
            let (path, _) = source.choose(&[])?;
            examined = source.walk(&path, &mut reservation, |_, row, bytes, reservation| {
                for aggregate in &mut aggregates {
                    aggregate.add(table, &row, params, now, reservation)?;
                }
                reservation.shrink(bytes);
                Ok(true)
            })?;
        }
        if limit > 0 {
            // What a least or greatest value takes is already held.
            reservation.grow(row_overhead(aggregates.len()))?;
            rows.push(aggregates.into_iter().map(Aggregate::answer).collect());
        }
    } else if limit > 0 {
        // The select list worked out for one row.
        let moved = source.moved(select.columns)?;
        let output = |row: Vec<Value>, bytes: u64, reservation: &mut Reservation| {
            source.output(select.columns, &moved, row, bytes, None, reservation)
        };
        let (path, in_order) = source.choose(select.order)?;
        if in_order {
            examined = source.walk(&path, &mut reservation, |_, row, bytes, reservation| {
                rows.push(output(row, bytes, reservation)?);
                Ok(rows.len() < limit)
            })?;
        } else {
            let mut sorting: Vec<Sorting> = Vec::new();
            let mut key_bytes = 0;
            examined = source.walk(&path, &mut reservation, |_, row, bytes, reservation| {
                let order = select.order;
                let most = order
                    .iter()
                    .map(|key| most_bytes(&key.expr, table, &row, params));
                let held = row_overhead(order.len()) + most.sum::<u64>();
                reservation.grow(held + size_of::<Sorting>() as u64)?;
                key_bytes += held + size_of::<Sorting>() as u64;
                let keys = order
                    .iter()
                    .map(|key| eval(&key.expr, table, &row, params, now))
                    .collect::<Result<Vec<_>>>()?;
                sorting.push(Sorting { keys, row, bytes });
                Ok(true)
            })?;
            sorting.sort_by(|a, b| {
                let keys = a.keys.iter().zip(&b.keys).zip(select.order);
                keys.map(|((a, b), key)| match key.direction {
                    SortDirection::Desc => sort_order(a, b).reverse(),
                    _ => sort_order(a, b),
                })
                .find(|order| order.is_ne())
                .unwrap_or(Ordering::Equal)
            });
            for cut in sorting.drain(limit.min(sorting.len())..) {
                reservation.shrink(cut.bytes);
            }
            for kept in sorting {
                rows.push(output(kept.row, kept.bytes, &mut reservation)?);
            }
            reservation.shrink(key_bytes);
        }
    }

    Ok(QueryResult {
        columns,
        rows,
        rows_affected: 0,
        rows_examined: examined,
        _reservation: Some(reservation),
    })
}

/// The rows [`matching`] found, each with its key.
pub(crate) type Matched = Vec<(Key, Vec<Value>)>;

/// The rows of one table that pass a filter, with their keys: what an UPDATE or DELETE acts on.
/// Uses the same index choice and the same evaluator as [`run`]. The rows and their keys stay
/// counted against the memory limit for as long as the caller holds the reservation that comes
/// with them.
pub(crate) fn matching(
    table: &Table,
    filter: Option<&Expr>,
    params: &HashMap<String, Value>,
    read: &dyn Read,
    budget: &Budget,
    now: i64,
) -> Result<(Matched, Reservation)> {
    let width = table.meta.columns.len();
    let mut filtered = vec![false; width];
    if let Some(filter) = filter {
        reads(filter, table, params, &mut filtered)?;
    }
    let source = Source::new(
        table,
        filter,
        params,
        now,
        read,
        filtered,
        &vec![true; width],
    );
    let (path, _) = source.choose(&[])?;
    let mut reservation = budget.reserve(0, "query")?;
    let mut found = Vec::new();
    let key_columns = source.key_columns();
    source.walk(&path, &mut reservation, |key, row, _, reservation| {
        // A key is its row's key values once more, and is asked for as what they take.
        let values = key_columns.iter().map(|at| heap_bytes(&row[*at]));
        reservation.grow(row_overhead(key_columns.len()) + values.sum::<u64>())?;
        found.push((key, row));
        Ok(true)
    })?;
    Ok((found, reservation))
}

/// What a result calls its columns: the table's own names for `SELECT *` (`None`), otherwise
/// each item's `AS` name, a bare column's name, an aggregate's name, and `expr` for any other
/// expression.
fn names(table: &Table, columns: Option<&[ProjectColumn]>, aggregates: bool) -> Vec<String> {
    let Some(columns) = columns else {
        let declared = table.meta.columns.iter();
        return declared.map(|column| column.name.clone()).collect();
    };
    columns
        .iter()
        .map(|column| match (&column.alias, &column.expr) {
            (Some(alias), _) => alias.clone(),
            (None, Expr::Column(column)) => column.column.clone(),
            (None, Expr::FunctionCall { name, .. }) if aggregates => name.clone(),
            (None, _) => "expr".to_owned(),
        })
        .collect()
}

/// One planned SELECT, as the planner nests it: LIMIT over the select list over ORDER BY over
/// one table with its filter.
struct Select<'a> {
    table: &'a str,
    filter: Option<&'a Expr>,
    order: &'a [SortKey],
    /// `None` for `SELECT *`.
    columns: Option<&'a [ProjectColumn]>,
    limit: Option<u64>,
}

impl<'a> Select<'a> {
    /// Reads the shape out of a plan, refusing by name a plan this version does not answer.
    fn of(mut plan: &'a Plan) -> Result<Select<'a>> {
        let (mut limit, mut columns) = (None, None);
        let mut order: &[SortKey] = &[];
        if let Plan::Limit { input, count } = plan {
            limit = Some(*count);
            plan = &**input;
        }
        if let Plan::Project {
            input,
            columns: list,
        } = plan
        {
            columns = Some(list.as_slice());
            plan = &**input;
        }
        if let Plan::Sort { input, keys } = plan {
            order = keys;
            plan = &**input;
        }
        let Plan::Scan { table, filter } = plan else {
            return Err(unsupported("this statement"));
        };
        let (table, filter) = (table.as_str(), filter.as_ref());
        Ok(Select {
            table,
            filter,
            order,
            columns,
            limit,
        })
    }
}

/// A row waiting to be sorted: the values it sorts by, and the bytes the row holds of the
/// statement's reservation.
struct Sorting {
    keys: Vec<Value>,
    row: Vec<Value>,
    bytes: u64,
}

/// One aggregate of a select list, and its answer over the rows seen so far.
struct Aggregate<'a> {
    function: Function,
    /// What is counted, added or compared; `None` for `COUNT(*)`.
    of: Option<&'a Expr>,
    /// How many rows took part: every row for `COUNT(*)`, otherwise those where `of` is not NULL.
    seen: i64,
    /// The sum, the least or the greatest so far; NULL until a row takes part.
    value: Value,
    /// The bytes of the statement's reservation that `value` holds.
    held: u64,
}

#[derive(Clone, Copy, PartialEq)]
enum Function {
    Count,
    Sum,
    Min,
    Max,
}

impl<'a> Aggregate<'a> {
    /// The aggregate an item of a select list is, if it is one.
    fn of(expr: &'a Expr) -> Result<Option<Aggregate<'a>>> {
        let Expr::FunctionCall { name, args } = expr else {
            return Ok(None);
        };
        let functions = [
            ("COUNT", Function::Count),
            ("SUM", Function::Sum),
            ("MIN", Function::Min),
            ("MAX", Function::Max),
        ];
        let Some((_, function)) = functions
            .into_iter()
            .find(|(known, _)| name.eq_ignore_ascii_case(known))
        else {
            return Ok(None);
        };
        let [of] = args.as_slice() else {
            return Err(Error::PlanError(format!("{name} takes one argument")));
        };
        let every_row = function == Function::Count
            && matches!(of, Expr::Column(column) if column.column == "*");
        Ok(Some(Aggregate {
            function,
            of: (!every_row).then_some(of),
            seen: 0,
            value: Value::Null,
            held: 0,
        }))
    }

    /// Refuses a SUM over a column that does not hold numbers, whatever rows the table has.
    fn check(&self, table: &Table, of: &Expr) -> Result<()> {
        let Expr::Column(column) = of else {
            return Ok(());
        };
        let column = &table.meta.columns[column_index(table, column)?];
        if self.function == Function::Sum
            && !matches!(column.column_type, ColumnType::Integer | ColumnType::Real)
        {
            return Err(Error::PlanError(format!(
                "SUM needs numbers, and the column {} holds none",
                column.name
            )));
        }
        Ok(())
    }

    /// Takes one row into the answer. A NULL takes no part in any aggregate but `COUNT(*)`.
    /// The memory of the value is asked for before it is made, and given back unless the value
    /// is kept as the least or the greatest so far.
    fn add(
        &mut self,
        table: &Table,
        row: &[Value],
        params: &HashMap<String, Value>,
        now: i64,
        reservation: &mut Reservation,
    ) -> Result<()> {
        if let Some(of) = self.of {
            let most = most_bytes(of, table, row, params);
            reservation.grow(most)?;
            let value = eval(of, table, row, params, now)?;
            if value == Value::Null {
                reservation.shrink(most);
                return Ok(());
            }
            let first = self.value == Value::Null;
            let better = match self.function {
                Function::Count => None,
                Function::Sum => {
                    let so_far = if first { &Value::Int64(0) } else { &self.value };
                    self.value = arithmetic(Arithmetic::Add, so_far, &value)?;
                    None
                }
                Function::Min => Some(Ordering::Less),
                Function::Max => Some(Ordering::Greater),
            };
            if better.is_some_and(|better| first || compare(&value, &self.value) == Some(better)) {
                reservation.shrink(self.held);
                (self.value, self.held) = (value, most);
            } else {
                reservation.shrink(most);
            }
        }
        self.seen += 1;
        Ok(())
    }

    /// The answer over the rows taken: a count of nothing is zero, and every other aggregate of
    /// nothing is NULL.
    fn answer(self) -> Value {
        match self.function {
            Function::Count => Value::Int64(self.seen),
            _ => self.value,
        }
    }
}

/// The memory a row of this many values takes before any text, JSON or vector in it.
fn row_overhead(values: usize) -> u64 {
    (size_of::<Vec<Value>>() + values * size_of::<Value>()) as u64
}

/// The memory a value takes outside its own cell.
fn heap_bytes(value: &Value) -> u64 {
    fn json(value: &serde_json::Value) -> u64 {
        let cell = size_of::<serde_json::Value>();
        match value {
            serde_json::Value::String(text) => text.len() as u64,
            serde_json::Value::Array(items) => {
                let items = items.iter();
                items.map(|item| cell as u64 + json(item)).sum()
            }
            serde_json::Value::Object(members) => {
                let members = members.iter();
                members
                    .map(|(name, member)| (name.len() + cell) as u64 + json(member))
                    .sum()
            }
            _ => 0,
        }
    }
    match value {
        Value::Text(text) => text.len() as u64,
        Value::Vector(vector) => size_of_val(vector.as_slice()) as u64,
        Value::Json(value) => json(value),
        _ => 0,
    }
}
