//! read: bounded sessions over the newest committed view, from a file or a live writer.
//! Sessions own their memory budget and never write to the store.

use std::collections::{BTreeSet, HashMap};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use crate::database::{KeptPlans, Open, Statements, admin, locked};
mod channel;
mod cursor;
pub(crate) use channel::Channel;
pub use channel::{OwnerReadCancellation, OwnerRequestHandler, request_owner};
pub use cursor::{Cursor, Snapshot};

use crate::memory::Budget;
use crate::query::{self, QueryResult};
use crate::schema::Catalog;
use crate::sql::{Effect, Plan};
use crate::store::{Cell, Key, Read, Store, View, WrapStorage};
use crate::system::{self, Clock};
use crate::types::{
    ContextId, Error, Principal, Result, ScopeLabel, TableMeta, TxId, Value, typed,
};

/// The ceilings of one reading session; equality is accepted. Result, work, active-time,
/// memory, cursor and table ceilings are enforced.
#[derive(Clone, Debug)]
pub struct ReadLimits {
    /// Most rows returned per statement; default 500.
    pub result_rows: u64,
    /// Most result bytes per statement; default 4 MiB.
    pub result_bytes: u64,
    /// The caller measures what it prints of `declaration`, `table_names` and `event_bus_status`.
    pub caller_checks_metadata: bool,
    /// Most row reads plus distance evaluations per statement; none unless declared.
    pub work: u64,
    /// Most active milliseconds per statement; default 5,000.
    pub active_ms: u64,
    /// Session memory in bytes; default 16 MiB. Exactly `u64::MAX` lifts the ceiling.
    /// Other finite limits size caches from the limit. The floor is the sum of four terms: the
    /// allowance, the page cache counted with its bookkeeping, the plan cache and the room of one
    /// check, 11,018,240 bytes with no cache share.
    pub memory: u64,
    /// Percent of `memory` given to the page cache, 0 to 100; absent keeps a quarter, 8 MiB at least.
    pub cache_share: Option<u64>,
    pub cursor_page_rows: u64,
    pub cursor_page_bytes: u64,
    pub cursor_idle_ms: u64,
    pub cursor_lifetime_ms: u64,
    pub tables: Option<BTreeSet<String>>,
}
impl Default for ReadLimits {
    fn default() -> Self {
        Self {
            result_rows: 500,
            result_bytes: 4 << 20,
            caller_checks_metadata: false,
            work: u64::MAX,
            active_ms: 5_000,
            memory: 16 << 20,
            cache_share: None,
            cursor_page_rows: 100,
            cursor_page_bytes: 1 << 20,
            cursor_idle_ms: 300_000,
            cursor_lifetime_ms: 1_800_000,
            tables: None,
        }
    }
}
/// The part of a read described by a progress report.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadPhase {
    Executing,
}
/// Progress reported inline; bytes are the existing assembled-row memory charge.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReadProgress {
    pub phase: ReadPhase,
    pub rows: u64,
    pub bytes: u64,
    /// Most row reads plus distance evaluations per statement; none unless declared.
    pub work: u64,
    /// Most active milliseconds per statement; default 5,000.
    pub active_ms: u64,
}
/// Receives progress before the read returns. Observer work is part of the read's cost.
pub trait ReadProgressObserver: Send + Sync {
    fn progress(&self, progress: ReadProgress);
    /// Receives final totals once, including reads shorter than one reporting interval.
    fn completed(&self, _totals: ReadProgress) {}
}
/// Options for a file reader. Start from the default and assign the fields to change.
#[derive(Clone, Default)]
#[non_exhaustive]
pub struct ReadSessionOptions {
    pub limits: ReadLimits,
    pub observer: Option<Arc<dyn ReadProgressObserver>>,
    /// Admitted Contexts; None is unconstrained, Some(empty) admits none.
    pub contexts: Option<BTreeSet<ContextId>>,
    /// Admitted scope labels; None is unconstrained, Some(empty) admits none.
    pub scope_labels: Option<BTreeSet<ScopeLabel>>,
    /// Principal whose grants this session uses; None declares no principal.
    pub principal: Option<Principal>,
    /// Clock for NOW() and active-time admission; None uses the machine clock. An idle cursor
    /// is let go by the writer's clock.
    pub clock: Option<Arc<dyn Clock>>,
    /// Wraps the opened data file before any store page is read.
    pub storage: Option<WrapStorage>,
}

/// A matching committed view and catalog. Writers publish it under their commit lock;
/// file readers acquire their view independently and reuse catalogs by schema commit.
#[derive(Clone)]
pub(crate) struct Core {
    pub(crate) commit: TxId,
    pub(crate) catalog: Arc<Catalog>,
    pub(crate) view: Option<View>,
}
impl Core {
    pub(crate) fn load(
        view: View,
        kept: &mut Option<(TxId, Arc<Catalog>)>,
        budget: &Budget,
    ) -> Result<Self> {
        let declared = view.schema_commit()?;
        let catalog = match kept {
            Some((at, catalog)) if *at == declared => catalog.clone(),
            _ => Arc::new(Catalog::load(&view, budget)?),
        };
        *kept = Some((declared, catalog.clone()));
        Ok(Self {
            commit: view.commit(),
            catalog,
            view: Some(view),
        })
    }
    pub(crate) fn newest(
        &mut self,
        store: &Store,
        budget: &Budget,
    ) -> Result<(View, Arc<Catalog>)> {
        if self.view.is_none() {
            self.view = Some(store.view_on(budget.standing(), false)?);
        }
        Ok((
            self.view.as_ref().expect("loaded view").clone(),
            self.catalog.clone(),
        ))
    }
    pub(crate) fn declaration(
        &self,
        table: &str,
        budget: &Budget,
    ) -> Result<crate::memory::Owned<String>> {
        self.catalog.declaration(table, budget)
    }
}

/// Older versions of one row, newest first, as [`ReadSession::row_versions`] lists them: the
/// columns `replaced_by_commit` and then the table's, and whether there are more.
#[non_exhaustive]
pub struct RowVersions {
    pub rows: QueryResult,
    pub more: bool,
}

pub(crate) fn row_versions(
    view: &View,
    catalog: &Catalog,
    reader: &query::Reader,
    budget: &Budget,
    (name, key, limit): (&str, &[Value], usize),
) -> Result<RowVersions> {
    let table = catalog
        .table(name)
        .ok_or_else(|| Error::TableNotFound(name.into()))?;
    if key.len() != table.key.len() || key.is_empty() {
        let n = table.key.len();
        let refusal = format!("{name}'s key takes {n} value(s), in key order");
        return Err(Error::PlanError(refusal));
    }
    let columns = table.key.iter().map(|&at| &table.meta.columns[at]);
    let key = columns
        .zip(key)
        .map(|(column, value)| typed(name, column, value.clone()));
    let key = Key::from_values(&key.collect::<Result<Vec<_>>>()?, &[])?;
    let mut held = budget.reserve(0, "versions")?;
    let mut seeing = budget.reserve(0, "visibility")?;
    let mut visible = query::Visible::new(table, catalog, reader, &mut seeing)?;
    let (mut cover, mut rows, mut more) = (budget.passing("versions")?, Vec::new(), false);
    for row in view.versions(table.id, &key, &mut cover)? {
        let ((commit, row), width) = (row?, table.meta.columns.len());
        held.grow(row.bytes + ((width + 1) * size_of::<Value>()) as u64)?;
        let values = (0..width).map(|at| match table.cell(&row, at).into_owned() {
            Cell::Here(value) => Ok(value),
            Cell::Beside(beside) => view.load(&beside, &mut held),
        });
        let mut values = values.collect::<Result<Vec<_>>>()?;
        if let Some(refusal) = visible.hidden(&values, view)? {
            return Err(refusal);
        }
        if rows.len() == limit {
            more = true;
            break;
        }
        values.insert(0, Value::TxId(commit));
        held.room(&mut rows, usize::MAX)?;
        rows.push(values);
    }
    let names = table.meta.columns.iter().map(|column| column.name.clone());
    let columns = std::iter::once("replaced_by_commit".into()).chain(names);
    let rows = QueryResult::answer(columns.collect(), rows, held);
    Ok(RowVersions { rows, more })
}

/// A bounded reader. Each statement acquires the newest commit and releases its view
/// before returning; retained results stay charged to this session's own memory budget.
pub struct ReadSession {
    plans: KeptPlans,
    /// Its own record when opened by path; the writer's when opened through it.
    statements: Arc<Statements>,
    store: Store,
    owner: Option<Weak<Open>>,
    budget: Budget,
    clock: Arc<dyn Clock>,
    catalog: Mutex<Option<(TxId, Arc<Catalog>)>>,
    reader: query::Reader,
    limits: ReadLimits,
    observer: Option<Arc<dyn ReadProgressObserver>>,
    stop: Arc<AtomicU64>,
    /// The one moment every read answers from, once `into_one_moment` has taken it.
    moment: Option<Core>,
}
impl ReadSession {
    /// Opens an existing store for bounded reading; never creates a store.
    ///
    /// Use `open_with_options` to declare contexts, labels and principal, or
    /// `Database::read_session` on a narrowed database handle.
    ///
    /// # Errors
    ///
    /// `StoreMissing` if absent; `StoreCorrupted` on damaged records; `StoreIo` on file
    /// failures; `MemoryBudgetExceeded` when the separate opening caches cannot fit.
    pub fn open(path: impl AsRef<Path>, limits: ReadLimits) -> Result<Self> {
        Self::open_with_options(
            path,
            ReadSessionOptions {
                limits,
                ..Default::default()
            },
        )
    }
    /// Opens a file reader with declared limits, clock and storage wrapper.
    /// Refuses a missing/damaged store or a memory cap below the opening reservation.
    ///
    /// Use `open_with_options` to declare contexts, labels and principal, or
    /// `Database::read_session` on a narrowed database handle.
    ///
    /// # Errors
    ///
    /// `StoreMissing` if absent; `StoreCorrupted` on damaged records; `StoreIo` on file
    /// failures; `MemoryBudgetExceeded` when the separate opening caches cannot fit.
    pub fn open_with_options(path: impl AsRef<Path>, options: ReadSessionOptions) -> Result<Self> {
        let limit = crate::settings::limit(options.limits.memory);
        let cache = crate::store::cache_under(limit, options.limits.cache_share);
        crate::memory::admit(limit, crate::memory::account(limit, cache, None))?;
        let budget = Budget::new(limit)?;
        query::limit("cache_share", options.limits.cache_share.unwrap_or(0), 100)?;
        let store = Store::open_for_reading(path.as_ref(), &budget, &options)?;
        Ok(Self {
            plans: KeptPlans::new(&budget, budget.limit())?,
            statements: Arc::new(Statements::new(&budget, None)),
            store,
            budget,
            limits: options.limits,
            clock: options.clock.unwrap_or_else(system::machine_clock),
            reader: query::Reader {
                contexts: options.contexts.map(Arc::new),
                scope_labels: options.scope_labels.map(Arc::new),
                principal: options.principal.map(Arc::new),
            },
            observer: options.observer,
            owner: None,
            catalog: Mutex::new(None),
            stop: Arc::new(AtomicU64::new(0)),
            moment: None,
        })
    }
    pub(crate) fn live(
        open: &Arc<Open>,
        path: &Path,
        options: ReadSessionOptions,
        reader: query::Reader,
    ) -> Result<Self> {
        let mut session = Self::open_with_options(path, options)?;
        session.reader = reader;
        session.owner = Some(Arc::downgrade(open));
        session.statements = open.statements.clone();
        Ok(session)
    }
    /// This session, answering every later row, declaration and version read from the newest
    /// commit now; `export_snapshot` still takes its own.
    pub fn into_one_moment(mut self) -> Result<Self> {
        self.moment = Some(self.newest()?);
        Ok(self)
    }
    /// Runs a read-only statement. A limit refusal or cancellation leaves this session usable.
    ///
    /// # Errors
    ///
    /// SQL parse/plan, name, visibility and vector refusals as for `Database::execute`;
    /// `ReadOnlySession` for a statement that is not read-only (execute only);
    /// `ReadLimitExceeded` for `work`, `result_rows` or `result_bytes`;
    /// `MemoryBudgetExceeded`; `QueryCanceled` for active time or cancellation;
    /// `StoreIo` or `StoreCorrupted` when the current view cannot be read.
    pub fn execute(&self, sql: &str, params: &HashMap<String, Value>) -> Result<QueryResult> {
        let _statistics = self.budget.pay_for_statistics();
        let mut work = query::Work::new(
            &self.limits,
            &*self.clock,
            &self.stop,
            self.observer.as_deref(),
        )
        .paged(self.store.pages());
        work.statements = Some(&self.statements);
        let (mut seen, mut form) = (None, None);
        let result = (|| {
            work.checkpoint(false)?;
            let (plan, mut parsing, formed) = self.plans.planned(sql, &self.budget)?;
            form = formed;
            let parse_bytes = parsing.bytes();
            parsing.grow(crate::sql::planning(sql))?;
            self.admit(&plan, None)?;
            let mut core = self.newest()?;
            let (view, catalog) = core.newest(&self.store, &self.budget)?;
            let view = &*seen.insert(view);
            self.admit(&plan, Some(&catalog))?;
            let now = system::now_millis(&*self.clock);
            query::run(
                &plan,
                params,
                &catalog,
                view,
                &self.reader,
                &self.budget,
                (now, Some(&work), None, (parsing.bytes(), parse_bytes)),
            )
        })();
        let result = work.finish(result);
        self.statements.ran(form.as_deref(), &work, 0);
        query::handed(result, seen.as_ref())
    }
    /// Copies the newest committed view through the existing purge-fenced exporter.
    pub fn export_snapshot(&self, dest: impl AsRef<Path>) -> Result<crate::ExportReport> {
        admin(&self.reader, Some("export_snapshot"))?;
        crate::maintenance::export(&self.store.view()?, dest.as_ref(), &self.budget)
    }
    /// Whether a writer currently holds this store, independent of reader admission.
    pub fn writer_running(&self) -> Result<bool> {
        self.store.writer_running()
    }
    /// The newest readable commit.
    pub fn committed_watermark(&self) -> Result<TxId> {
        Ok(self.newest()?.commit)
    }
    /// Stored event declarations and queue counts; the owner's live counters when it is running.
    pub fn event_bus_status(&self) -> Result<crate::Owned<crate::EventBusStatus>> {
        admin(&self.reader, Some("event_bus_status"))?;
        let (view, catalog) = self.newest()?.newest(&self.store, &self.budget)?;
        let all = match self.owner.as_ref().and_then(Weak::upgrade) {
            Some(open) => {
                let cron = open.triggers.cron_names();
                open.deliveries
                    .status(&view, &catalog, &self.budget, true, &cron)?
            }
            None => {
                let cron = std::collections::BTreeSet::new();
                crate::events::Deliveries::default().status(
                    &view,
                    &catalog,
                    &self.budget,
                    false,
                    &cron,
                )?
            }
        };
        self.fits(serde_json::to_string(&*all).map_or(0, |text| text.len()))?;
        Ok(all)
    }
    /// Opens a cursor at one commit; its first fetch returns the first page.
    pub fn open_cursor(&self, sql: &str, params: &HashMap<String, Value>) -> Result<Cursor> {
        let _statistics = self.budget.pay_for_statistics();
        Cursor::open(self, sql, params)
    }
    /// Cancels statements currently executing through this session; later statements can run.
    pub fn cancel(&self) {
        self.stop.fetch_add(1, Ordering::Relaxed);
    }
    /// Explains a statement under the same session limits as execute.
    ///
    /// # Errors
    ///
    /// SQL parse/plan, name, visibility and vector refusals as for `Database::execute`;
    /// `ReadOnlySession` for a statement that is not read-only (execute only);
    /// `ReadLimitExceeded` for `work`, `result_rows` or `result_bytes`;
    /// `MemoryBudgetExceeded`; `QueryCanceled` for active time or cancellation;
    /// `StoreIo` or `StoreCorrupted` when the current view cannot be read.
    pub fn explain(&self, sql: &str) -> Result<crate::Owned<String>> {
        let result = self.execute(&format!("EXPLAIN {sql}"), &HashMap::new())?;
        let bytes = result
            .rows()
            .iter()
            .flatten()
            .map(crate::types::heap_bytes)
            .sum::<u64>()
            + (result.rows().len() * (3 * size_of::<&str>() + 1)) as u64;
        let mut held = self.budget.reserve(bytes, "explanation")?;
        let text = result
            .rows()
            .iter()
            .filter_map(|row| match row.first() {
                Some(Value::Text(line)) => Some(line.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("\n");
        held.shrink(held.bytes().saturating_sub(text.capacity() as u64));
        Ok(crate::Owned::charged(text, held))
    }
    /// The newest table names, in name order.
    ///
    /// # Errors
    ///
    /// `StoreIo` or `StoreCorrupted` when loading the newest view/catalog;
    /// reader budget/limit refusals. `declaration` also returns `TableNotFound`.
    /// Metadata reads are charged and bounded under the session's reader contract.
    pub fn table_names(&self) -> Result<crate::Owned<Vec<String>>> {
        let all =
            (self.newest()?.catalog).table_names(&self.budget, self.limits.tables.as_ref())?;
        self.fits(all.iter().map(String::len).sum())?;
        Ok(all)
    }
    /// An answer past `result_bytes` is refused whole.
    fn fits(&self, bytes: usize) -> Result<()> {
        let bytes = bytes as u64 * u64::from(!self.limits.caller_checks_metadata);
        query::limit("result_bytes", bytes, self.limits.result_bytes)
    }
    /// The newest stored table metadata, including typed scope rules, within table admission.
    pub fn table_meta(&self, name: &str) -> Result<crate::Owned<TableMeta>> {
        self.check_tables(std::iter::once(name))?;
        self.newest()?.catalog.meta_answer(name, &self.budget)
    }
    /// The older versions of one row, newest first: each the row as it stood before a commit
    /// replaced or removed it, at most `n`, with `more` when there are others. `key` is
    /// the primary key's values in order. A version the reader may not see refuses the call
    /// with its row rule's refusal.
    pub fn row_versions(&self, table: &str, key: &[Value], n: usize) -> Result<RowVersions> {
        self.check_tables(std::iter::once(table))?;
        let (view, catalog) = self.newest()?.newest(&self.store, &self.budget)?;
        row_versions(&view, &catalog, &self.reader, &self.budget, (table, key, n))
    }
    /// The newest declaration, as executable SQL.
    ///
    /// # Errors
    ///
    /// `StoreIo` or `StoreCorrupted` when loading the newest view/catalog;
    /// reader budget/limit refusals. `declaration` also returns `TableNotFound`.
    /// Metadata reads are charged and bounded under the session's reader contract.
    pub fn declaration(&self, table: &str) -> Result<crate::memory::Owned<String>> {
        self.check_tables(std::iter::once(table))?;
        let ddl = self.newest()?.declaration(table, &self.budget)?;
        self.fits(ddl.len())?;
        Ok(ddl)
    }
    /// Sorted stored tables read by the statement, including declared rank joins.
    pub fn tables_read(&self, sql: &str) -> Result<crate::Owned<Vec<String>>> {
        let (plan, ..) = self.plans.planned(sql, &self.budget)?;
        let core = self.newest()?;
        let held = core.catalog.answering(&self.budget, 2)?;
        let read = query::tables_read(&plan, Some(&core.catalog), self.limits.tables.as_ref())?;
        Ok(crate::Owned::charged(read.into_iter().collect(), held))
    }
    fn admit(&self, plan: &Plan, catalog: Option<&Catalog>) -> Result<()> {
        if plan.effect() != Effect::Reads {
            return Err(Error::ReadOnlySession);
        }
        admin(&self.reader, plan.admin_only())?;
        let tables = query::tables_read(plan, catalog, self.limits.tables.as_ref())?;
        self.check_tables(tables.iter().map(String::as_str))
    }
    fn check_tables<'a>(&self, tables: impl IntoIterator<Item = &'a str>) -> Result<()> {
        if let Some(allowed) = &self.limits.tables
            && tables.into_iter().any(|table| !allowed.contains(table))
        {
            return Err(Error::ReadLimitExceeded {
                field: "tables",
                limit: allowed.len() as u64,
            });
        }
        Ok(())
    }
    fn newest(&self) -> Result<Core> {
        if let Some(core) = &self.moment {
            return Ok(core.clone());
        }
        if let Some(open) = self.owner.as_ref().and_then(Weak::upgrade) {
            let (view, catalog) = open.newest()?;
            return Ok(Core {
                commit: view.commit(),
                catalog,
                view: Some(view),
            });
        }
        let view = self.store.view_on(self.budget.standing(), false)?;
        if let Some(limit) = view.setting(6)? {
            self.statements.set_limit(limit);
        }
        Core::load(view, &mut locked(&self.catalog), &self.budget)
    }
}
