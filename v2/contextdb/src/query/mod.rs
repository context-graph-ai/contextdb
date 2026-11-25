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
mod cursor;
pub(crate) use cursor::CursorRows;
mod expression;
mod filter;
mod for_write;
mod graph;
mod inspect;
pub(crate) use inspect::{Push, REPORTS};
mod join;
mod plan;
mod rank;
mod search;
mod sets;
mod sort;
pub(crate) use sets::SetContext;
mod subquery;
mod table;
mod union;
mod visible;
mod window;

use std::cell::Cell;
use std::collections::HashMap;
use std::fmt;

pub(crate) use expression::{EvalContext, Kept, Scope, eval, most_bytes, reads, sort_order};
pub(crate) use for_write::{Asked, named_keys, rows_for_write, walk_for_write};
pub(crate) use inspect::explain;
pub(crate) use subquery::tables_read;
pub(crate) use table::equal;
pub(crate) use table::{index_place, resumed};
pub(crate) use visible::{Reader, Visible};

use crate::memory::{Budget, Reservation};
use crate::read::{ReadLimits, ReadPhase, ReadProgress, ReadProgressObserver};
use crate::schema::Catalog;
use crate::sql::{Plan, not_supported};
use crate::store::Read;
use crate::system::{self, Clock};
use crate::types::{Error, Result, Value};
use plan::Given;
use std::sync::atomic::{AtomicU64, Ordering};

/// The statement's existing row and distance counters, plus session admission and progress.
fn stats(data: &redb::ReadOnlyDatabase) -> (u64, u64) {
    let stats = redb::ReadableDatabase::cache_stats(data);
    (stats.read_hits(), stats.read_misses())
}
pub(crate) struct Work<'a> {
    pub(crate) examined: Cell<u64>,
    pub(crate) evaluations: Cell<u64>,
    pages: Option<((u64, u64), std::sync::Arc<redb::ReadOnlyDatabase>)>,
    rows: Cell<u64>,
    /// The index the statement's path left unused, with why, for the record of statements.
    pub(crate) unused: Cell<Option<(String, &'static str)>>,
    pub(crate) statements: Option<&'a crate::database::Statements>,
    bytes: Cell<u64>,
    limits: &'a ReadLimits,
    clock: &'a dyn Clock,
    stop: &'a AtomicU64,
    generation: u64,
    started: i64,
    observer: Option<&'a dyn ReadProgressObserver>,
}
impl<'a> Work<'a> {
    pub(crate) fn new(
        limits: &'a ReadLimits,
        clock: &'a dyn Clock,
        stop: &'a AtomicU64,
        observer: Option<&'a dyn ReadProgressObserver>,
    ) -> Self {
        Self {
            examined: Cell::new(0),
            evaluations: Cell::new(0),
            pages: None,
            rows: Cell::new(0),
            unused: Cell::new(None),
            statements: None,
            bytes: Cell::new(0),
            limits,
            clock,
            stop,
            observer,
            generation: stop.load(Ordering::Relaxed),
            started: system::now_millis(clock),
        }
    }
    /// Counts a cancel that landed after `generation` and before this statement's own reads.
    pub(crate) fn since(mut self, generation: u64) -> Self {
        self.generation = generation;
        self
    }
    /// Counts the pages the reader's cache serves from here on.
    pub(crate) fn paged(mut self, data: Option<std::sync::Arc<redb::ReadOnlyDatabase>>) -> Self {
        self.pages = data.map(|data| (stats(&data), data));
        self
    }
    /// Pages found in the cache and pages read from the file since `paged`.
    pub(crate) fn cached(&self) -> (u64, u64) {
        self.pages.as_ref().map_or((0, 0), |((hits, reads), data)| {
            let now = stats(data);
            (now.0 - hits, now.1 - reads)
        })
    }
    fn progress(&self) -> ReadProgress {
        ReadProgress {
            phase: ReadPhase::Executing,
            rows: self.rows.get(),
            bytes: self.bytes.get(),
            work: self.examined.get().saturating_add(self.evaluations.get()),
            active_ms: system::now_millis(self.clock)
                .saturating_sub(self.started)
                .max(0) as u64,
        }
    }
    pub(crate) fn checkpoint(&self, report: bool) -> Result<()> {
        if report && let Some(observer) = self.observer {
            observer.progress(self.progress());
        }
        if self.stop.load(Ordering::Relaxed) != self.generation {
            return Err(Error::QueryCanceled {
                statement_timeout: false,
            });
        }
        if self.progress().active_ms > self.limits.active_ms {
            return Err(Error::QueryCanceled {
                statement_timeout: true,
            });
        }
        Ok(())
    }
    fn count(&self, before: u64) -> Result<()> {
        let total = self.examined.get().saturating_add(self.evaluations.get());
        limit("work", total, self.limits.work)?;
        // A cancel is observed on the next row. Progress is reported every 1024.
        self.checkpoint(total / 1024 != before / 1024)?;
        Ok(())
    }
    /// One more than the work the statement may still do: what a count reads at most, so that
    /// it is refused past the limit without reading further.
    pub(crate) fn left(&self) -> u64 {
        let total = self.examined.get().saturating_add(self.evaluations.get());
        self.limits.work.saturating_sub(total).saturating_add(1)
    }
    fn result_row(&self, bytes: u64) -> Result<()> {
        let rows = self.rows.get().saturating_add(1);
        let bytes = self.bytes.get().saturating_add(bytes);
        limit("result_rows", rows, self.limits.result_rows)?;
        limit("result_bytes", bytes, self.limits.result_bytes)?;
        self.rows.set(rows);
        self.bytes.set(bytes);
        if rows.is_multiple_of(1024) {
            self.checkpoint(true)?;
        }
        Ok(())
    }
    /// Rows given, rows examined and distance evaluations, as the record of statements counts.
    pub(crate) fn counts(&self) -> [u64; 3] {
        [self.rows.get(), self.examined.get(), self.evaluations.get()]
    }
    pub(crate) fn finish<T>(&self, result: Result<T>) -> Result<T> {
        let admitted = self.checkpoint(true);
        if let Some(observer) = self.observer {
            observer.completed(self.progress());
        }
        result.and_then(|value| {
            admitted?;
            self.checkpoint(false)?;
            Ok(value)
        })
    }
}
pub(crate) fn limit(field: &'static str, actual: u64, limit: u64) -> Result<()> {
    if actual > limit {
        return Err(Error::ReadLimitExceeded { field, limit });
    }
    Ok(())
}
/// Adds to the same public counter for unlimited writers and bounded sessions.
fn admit(counter: &Cell<u64>, amount: u64, work: Option<&Work<'_>>) -> Result<()> {
    let before = work.map_or(0, |w| w.examined.get().saturating_add(w.evaluations.get()));
    counter.set(counter.get().saturating_add(amount));
    work.map_or(Ok(()), |work| work.count(before))
}
pub(crate) fn result_row(
    row: &[Value],
    held: Option<&mut Reservation>,
    work: Option<&Work<'_>>,
) -> Result<()> {
    let bytes = row_overhead(row.len()) + row.iter().map(crate::types::heap_bytes).sum::<u64>();
    if let Some(held) = held {
        held.grow(bytes)?;
    }
    work.map_or(Ok(()), |work| work.result_row(bytes))
}
/// The existing visible-row attachment also carries optional session admission.
pub(crate) type Counted<'a> = Option<(&'a Cell<u64>, Option<&'a Work<'a>>)>;

/// What a statement returned: the column names and rows of a SELECT, or the number of rows a
/// write affected. Its rows stay counted against the memory limit until it is dropped.
pub struct QueryResult {
    report: bool,
    columns: Vec<String>,
    rows: Vec<Vec<Value>>,
    rows_affected: u64,
    rows_examined: u64,
    cache: (u64, u64),
    distance_evaluations: u64,
    peak_bytes: Option<u64>,
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
            .field("peak_bytes", &self.peak_bytes)
            .finish_non_exhaustive()
    }
}

impl QueryResult {
    /// Whether this report is rendered one field per line.
    pub fn is_report(&self) -> bool {
        self.report
    }
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

    /// How many visible stored rows the statement read, including reads for writes. The same
    /// on every run over the same store: a whole-table `COUNT(*)` reads none, a lookup by key
    /// reads one. Speed is guarded in tests by this number, never by a time.
    pub fn rows_examined(&self) -> u64 {
        self.rows_examined
    }

    /// The pages a read session found in its cache and the pages it read from the file, as
    /// Postgres counts hits and reads; the counts are per statement, not summed over the session.
    pub fn pages(&self) -> (u64, u64) {
        self.cache
    }

    /// How many distance evaluations a similarity search made to produce this result: the
    /// group centroids it compared with the vector searched for, the binary codes it estimated, the
    /// stored vectors it screened through their one-byte copies, and the rows whose distance
    /// it worked out. An exact count, the same on every run over the same store, and zero for
    /// any other statement. A narrowed reader counts only visible rows compared. A search that
    /// used the index usually made fewer than the same search under `USE VECTOR EXACT`, and can
    /// make more: where it leaves few groups out, or
    /// goes over its binary codes again because rows were turned away, the binary codes it estimates
    /// come to more than the stored vectors EXACT screens.
    pub fn distance_evaluations(&self) -> u64 {
        self.distance_evaluations
    }

    /// The highest charged bytes while this statement ran; withheld on a narrowed handle.
    /// Reads measure their statement reservations; writes measure the handle's total.
    pub fn peak_bytes(&self) -> Option<u64> {
        self.peak_bytes
    }

    pub(crate) fn measured(mut self, budget: &Budget, reader: &Reader) -> Self {
        self.peak_bytes = reader.is_admin().then(|| budget.peak_bytes());
        self
    }

    pub(crate) fn answer(columns: Vec<String>, rows: Vec<Vec<Value>>, held: Reservation) -> Self {
        QueryResult {
            columns,
            rows,
            _reservation: Some(held),
            ..Self::affected(0, 0)
        }
    }

    /// This result with the distances its statement compared.
    pub(crate) fn evaluated(self, distances: u64) -> QueryResult {
        QueryResult {
            distance_evaluations: distances,
            ..self
        }
    }

    /// The result of a write that changed `rows` rows.
    pub(crate) fn affected(rows: u64, examined: u64) -> QueryResult {
        QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
            rows_affected: rows,
            rows_examined: examined,
            cache: (0, 0),
            distance_evaluations: 0,
            peak_bytes: Some(0),
            report: false,
            _reservation: None,
        }
    }
}

/// What a read hands out once all its work is done, unless an erasure cut its view off first.
pub(crate) fn handed<T>(result: Result<T>, view: Option<&crate::store::View>) -> Result<T> {
    view.map_or(Ok(()), Read::valid)?;
    result
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
    (now, work, figures, (parsing, parse_bytes)): (
        i64,
        Option<&Work<'_>>,
        Option<&crate::database::Open>,
        (u64, u64),
    ),
) -> Result<QueryResult> {
    read.valid()?;
    let statement = budget.statement();
    statement.reset_peak(parsing);
    let budget = &statement;
    if let (Plan::ResetStatements, Some(statements)) = (plan, work.and_then(|w| w.statements)) {
        statements.reset();
        return Ok(QueryResult::affected(0, 0).measured(budget, reader));
    }
    if let Some(shown) = inspect::show(
        plan,
        catalog,
        read,
        (budget, work, figures, parsing),
        work.and_then(|work| work.limits.tables.as_ref()),
    )? {
        return Ok(shown.measured(budget, reader));
    }
    if let Plan::Explain(plan) = plan {
        let lines = inspect::explain(
            plan,
            params,
            catalog,
            read,
            reader,
            budget,
            (now, work, parse_bytes),
        )?;
        let mut held = budget.reserve(0, "materialize_answer")?;
        let mut rows = Vec::new();
        for line in lines.lines() {
            let row = vec![Value::Text(line.to_owned())];
            result_row(&row, Some(&mut held), work)?;
            held.room(&mut rows, usize::MAX)?;
            rows.push(row);
        }
        let result = QueryResult::answer(vec!["QUERY PLAN".to_owned()], rows, held);
        return Ok(result.measured(budget, reader));
    }
    let Plan::Select(select) = plan else {
        return Err(not_supported("this statement"));
    };
    let (rows_read, distances) = (Cell::default(), Cell::default());
    let examined = work.map_or(&rows_read, |work| &work.examined);
    let kept = Kept::new(budget.passing("expression evaluation")?);
    let given = Given {
        allowed: work.and_then(|work| work.limits.tables.as_ref()),
        params,
        catalog,
        read,
        reader,
        budget,
        ctx: EvalContext::new(params, now, &kept),
        evaluations: work.map_or(&distances, |work| &work.evaluations),
        examined: Some((examined, work)),
        named: None,
        bound: None,
    };
    // What the reader's filter holds is given back with the statement.
    let mut seeing = budget.reserve(0, "query")?;
    let (columns, source) = plan::select(select, &given, &mut seeing)?;
    let mut result = collect(columns, &*source, budget, work)?;
    result.rows_examined = examined.get();
    result.cache = work.map_or((0, 0), Work::cached);
    result.distance_evaluations = given.evaluations.get();
    Ok(result.measured(budget, reader))
}

fn columns_bytes(slots: usize, strings: impl Iterator<Item = usize>) -> u64 {
    (slots * size_of::<String>() + strings.sum::<usize>()) as u64
}

fn collect(
    columns: Vec<String>,
    source: &dyn plan::Source,
    budget: &Budget,
    work: Option<&Work<'_>>,
) -> Result<QueryResult> {
    let bytes = columns_bytes(columns.capacity(), columns.iter().map(String::capacity));
    let mut held = budget.reserve(bytes, "materialize_answer")?;
    let mut rows = Vec::new();
    source.rows(&mut held, &mut |row, bytes, held| {
        if let Some(work) = work {
            work.result_row(bytes)?;
        }
        held.room(&mut rows, usize::MAX)?;
        rows.push(row);
        Ok(true)
    })?;
    Ok(QueryResult::answer(columns, rows, held))
}

/// The values of the SELECTs of one statement, each under the place its text begins.
pub(crate) type Values = Vec<(usize, sets::List)>;

/// The values of the SELECTs inside the INSERT, UPDATE or DELETE `plan` that `write` is run
/// with, as [`run`] would read them from `read`, each read once before the write changes a row
/// and held in `held`.
pub(crate) fn written(
    plan: &Plan,
    write: &crate::write::Given,
    read: &dyn Read,
    held: &mut Reservation,
    sets: Option<SetContext<'_>>,
) -> Result<Values> {
    let distances = Cell::default();
    let evaluations = write
        .examined
        .and_then(|(_, work)| work)
        .map_or(&distances, |work| &work.evaluations);
    let given = Given {
        allowed: None,
        params: write.params,
        catalog: write.catalog,
        read,
        reader: write.reader,
        budget: write.budget,
        ctx: EvalContext::new(write.params, write.now, write.kept).with_sets(sets),
        evaluations,
        examined: write.examined,
        named: None,
        bound: None,
    };
    subquery::written(plan, &given, held)
}

/// The memory a row of this many values takes before any text, JSON or vector in it.
fn row_overhead(values: usize) -> u64 {
    (size_of::<Vec<Value>>() + values * size_of::<Value>()) as u64
}
