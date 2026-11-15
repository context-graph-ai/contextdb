//! query: answers a SELECT from one table or from none: which rows to read and through which
//! index, the filter, the select list, GROUP BY with COUNT, SUM, MIN, MAX and AVG, HAVING,
//! ROW_NUMBER, DISTINCT, ORDER BY, LIMIT and OFFSET, and a similarity search, whose rows the
//! vector part finds. It holds the one expression evaluator, and counts exactly the rows it
//! reads.
//!
//! It never changes anything.

mod aggregate;
mod bind;
mod counts;
mod expression;
mod for_write;
mod graph;
mod inspect;
mod join;
mod plan;
mod rank;
mod search;
mod sort;
mod subquery;
mod table;
#[cfg(test)]
mod tests;
mod union;
mod visible;
mod window;

use std::cell::Cell;
use std::collections::HashMap;
use std::fmt;

pub(crate) use expression::{EvalContext, Scope, eval, reads};
pub(crate) use for_write::{Asked, rows_for_write};
pub(crate) use inspect::explain;
pub(crate) use table::equal;
pub(crate) use visible::{Reader, Visible};

use crate::memory::{Budget, Reservation};
use crate::schema::Catalog;
use crate::sql::{Plan, not_supported};
use crate::store::Read;
use crate::types::{Result, Value};
use plan::Given;

/// What a statement returned: the column names and rows of a SELECT, or the number of rows a
/// write affected. Its rows stay counted against the memory limit until it is dropped.
pub struct QueryResult {
    columns: Vec<String>,
    rows: Vec<Vec<Value>>,
    rows_affected: u64,
    rows_examined: u64,
    distance_evaluations: u64,
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
            .field("distance_evaluations", &self.distance_evaluations)
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

    /// How many distance evaluations a similarity search made to produce this result: the
    /// group centres it compared with the vector searched for, the sketches it estimated, the
    /// stored vectors it screened through their one-byte copies, and the rows whose distance
    /// it worked out. An exact count, the same on every run over the same store, and zero for
    /// any other statement. A search that used the index usually made fewer than the same
    /// search under `USE VECTOR EXACT`, and can make more: where it leaves few groups out, or
    /// goes over its sketches again because rows were turned away, the sketches it estimates
    /// come to more than the stored vectors EXACT screens.
    pub fn distance_evaluations(&self) -> u64 {
        self.distance_evaluations
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
            distance_evaluations: 0,
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

/// Answers one planned SELECT from one store, for one reader.
///
/// The statement is answered by the row sources `plan::select` puts one above the other. At
/// the bottom is what FROM names: the table source, which picks an index when the filter can
/// use one and scans otherwise, hands on only the rows `reader` may see, and loads a value kept
/// beside its row only when a selected or filtered column needs it. Above it the rows are
/// aggregated, or sorted when the path does not give the order and the select list worked
/// out; then DISTINCT, OFFSET and LIMIT. A `COUNT(*)` is answered with no row read when a kept
/// number or the entries of one index range give it. The result, and whatever a source keeps
/// on the way to it, grows through reservations on `budget` and refuses with
/// `MemoryBudgetExceeded` before it grows.
///
/// A similarity search, `ORDER BY column <=> vector LIMIT k`, is the table source of its
/// statement: the vector part finds the nearest of the rows this part lets it see.
pub(crate) fn run(
    plan: &Plan,
    params: &HashMap<String, Value>,
    catalog: &Catalog,
    read: &dyn Read,
    reader: &Reader,
    budget: &Budget,
    now: i64,
) -> Result<QueryResult> {
    if let Some(shown) = inspect::show(plan, catalog, read, budget)? {
        return Ok(shown);
    }
    if let Plan::Explain(plan) = plan {
        let lines = inspect::explain(plan, params, catalog, read, reader, budget, now)?;
        let rows = lines.lines().map(|line| vec![Value::Text(line.to_owned())]);
        return Ok(QueryResult::from_rows(
            vec!["QUERY PLAN".to_owned()],
            rows.collect(),
        ));
    }
    let Plan::Select(select) = plan else {
        return Err(not_supported("this statement"));
    };
    let given = Given {
        params,
        catalog,
        read,
        reader,
        budget,
        ctx: EvalContext::new(params, now),
        evaluations: &Cell::default(),
        named: None,
        bound: None,
    };
    // What the reader's filter holds is given back with the statement.
    let mut seeing = budget.reserve(0, "query")?;
    let (columns, source) = plan::select(select, &given, &mut seeing)?;
    let mut reservation = budget.reserve(0, "query")?;
    let mut rows = Vec::new();
    let examined = source.rows(&mut reservation, &mut |row, _, _| {
        rows.push(row);
        Ok(true)
    })?;
    Ok(QueryResult {
        columns,
        rows,
        rows_affected: 0,
        rows_examined: examined,
        distance_evaluations: given.evaluations.get(),
        _reservation: Some(reservation),
    })
}

/// The values of the SELECTs of one statement, each under the place its text begins.
pub(crate) type Values = Vec<(usize, Vec<Value>)>;

/// The values of the SELECTs inside the INSERT, UPDATE or DELETE `plan` that `write` is run
/// with, as [`run`] would read them from `read`, each read once before the write changes a row
/// and held in `held`.
pub(crate) fn written(
    plan: &Plan,
    write: &crate::write::Given,
    read: &dyn Read,
    held: &mut Reservation,
) -> Result<Values> {
    let given = Given {
        params: write.params,
        catalog: write.catalog,
        read,
        reader: write.reader,
        budget: write.budget,
        ctx: EvalContext::new(write.params, write.now),
        evaluations: &Cell::default(),
        named: None,
        bound: None,
    };
    subquery::written(plan, &given, held)
}

/// The memory a row of this many values takes before any text, JSON or vector in it.
fn row_overhead(values: usize) -> u64 {
    (size_of::<Vec<Value>>() + values * size_of::<Value>()) as u64
}
