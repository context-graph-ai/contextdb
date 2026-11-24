//! database: the handle a store is opened for writing with, its transactions, and the one
//! commit function.
//!
//! It never has a second way to commit: a statement run on its own, a transaction, a change of
//! the memory limit and every step of a maintenance cycle each build a change set and hand it
//! to the same function.

mod commit;
mod events;
mod observers;
mod open;
mod plugin;
pub use observers::{CommitEvent, SubscriptionMetrics};
pub use plugin::*;
mod scoped;
mod statements;
pub(crate) use statements::Statements;
#[cfg(test)]
mod tests;

use std::cell::RefCell;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::time::{Duration, SystemTime};

use crate::maintenance::{self, Maintenance, MaintenanceReport};
use crate::memory::{Budget, Reservation};
use crate::query::{self, QueryResult};
use crate::read::{Core, ReadLimits, ReadProgressObserver, ReadSession, ReadSessionOptions};
use crate::read::{RowVersions, row_versions};
use crate::schema::{self, Catalog};
use crate::sql::ast::{BinOp, ColumnRef};
use crate::sql::{self, Effect, Expr, Plan};
use crate::store::{Cell, ChangeSet, Read, Store, View, WrapStorage};
use crate::system::{self, Clock};
use crate::types::{Error, Result, TableMeta, TxId, Value};
use crate::vector::{self, Tally};
use crate::write;

/// What a host may say when it opens a store for writing.
///
/// Start from `DatabaseOpenOptions::default()` and assign the fields that matter. The struct may
/// gain fields in a later version, so it cannot be written out field by field outside this
/// crate.
///
/// ```
/// let mut options = contextdb::DatabaseOpenOptions::default();
/// options.memory_limit = Some(64 << 20); // 64 MiB
/// ```
#[derive(Clone, Default)]
#[non_exhaustive]
pub struct DatabaseOpenOptions {
    /// The most memory, in bytes, that this open may hold for its page cache, its transactions
    /// and its results.
    ///
    /// `None`, the default, takes the limit the store remembers from the last
    /// `SET MEMORY_LIMIT` or [`Database::set_memory_limit`]. If never declared, it takes a
    /// quarter of known Linux machine/cgroup-v2 memory, or 2 GiB when unknown.
    /// Detection uses Linux procfs; macOS uses the 2 GiB fallback. Explicit NONE stays unlimited.
    /// A limit given here holds until the store is closed and leaves what the
    /// store remembers as it is.
    ///
    /// Every open sets aside 1 MiB, its page cache and its statement plan cache. The page cache
    /// is a quarter of a declared limit, at least 8 MiB; the plan cache is a thirty-second,
    /// between 1 and 8 MiB. With no declaration both take their minima and keep their opening
    /// size. A writer also holds 768 KiB for each started 4 GiB of its file. A limit below the
    /// sum, 11,272,192 bytes for a file under 4 GiB, is refused with
    /// [`crate::Error::MemoryBudgetExceeded`] and opens no store.
    ///
    /// A [`crate::ReadSession`] uses its own declared `ReadLimits::memory` ceiling.
    pub memory_limit: Option<usize>,
    /// This open's disk ceiling in bytes; absent uses the stored declaration without changing it.
    pub disk_limit: Option<u64>,
    pub create: Create,
    /// Host hooks; None uses CorePlugin. The same plug-in serves every scoped handle.
    pub plugin: Option<Arc<dyn DatabasePlugin>>,
    /// Where the time comes from for `NOW()` and `DEFAULT NOW()`. `None`, the default, is the
    /// machine's clock.
    pub clock: Option<Arc<dyn Clock>>,
    /// Stop waiting for ownership at this time on `clock`; `None` waits until free.
    /// A passed deadline still opens a free store, otherwise returns `DatabaseLocked`.
    pub deadline: Option<SystemTime>,
    /// Stop waiting when set, returning `OpenCancelled`. `None` never cancels.
    pub cancel: Option<Arc<AtomicBool>>,
    /// A wrapper around the store's data file: the store opens the file, hands it to this, and
    /// reads, writes and syncs it only through what this returns. `None`, the default, uses the
    /// file as it is.
    pub storage: Option<WrapStorage>,
    /// Who drives maintenance; the default is the engine worker.
    pub maintenance: Maintenance,
    /// How long a write on another thread waits for a running callback; `None` is 60 seconds.
    pub callback_wait: Option<std::time::Duration>,
    /// Directory for the optional writer request channel; None uses the store folder.
    pub channel_dir: Option<PathBuf>,
}

/// How a writable open treats its data file.
#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub enum Create {
    #[default]
    IfMissing,
    Never,
    Fresh,
}

/// Work performed since this store was opened; available only to an unnarrowed handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct WorkDone {
    /// Rows read, including maintenance's sealed or packed vectors.
    pub rows_read: u64,
    /// Full-precision vector distance evaluations, and maintenance's slot-to-centre ones.
    pub vectors_compared: u64,
}

/// One store folder, held for writing.
///
/// One handle holds a store for writing at a time. Another open waits until it closes,
/// the deadline passes or cancellation is set. Reopening on the holder's thread without
/// either option waits forever. Read sessions can read while a writer holds the store.
///
/// A handle can be shared between threads, and its calls block until they are done.
///
/// Dropping the handle lets go of the store as [`Database::close`] does, and ignores what the
/// store's last close found.
pub struct Database {
    /// The open store, `None` once [`Database::close`] has let go of it.
    open: Mutex<Option<Arc<Writer>>>,
    /// Who asks through this handle: every statement of it reads and writes as this reader.
    reader: query::Reader,
    bound: Option<Arc<crate::events::Bound>>,
}

/// One store held for writing. A call holds it for as long as the call runs, so the folder is
/// let go when the handle has let go and the last running call has returned.
pub(crate) struct Open {
    /// The first field, so that the close hook runs before the store is let go.
    closes: Closes,
    this: Weak<Open>,
    pub(crate) last_cycle: Mutex<Option<MaintenanceReport>>,
    pub(crate) last_sweep: Mutex<Option<std::time::SystemTime>>,
    plans: KeptPlans,
    pub(crate) statements: Arc<Statements>,
    plugin: Option<Arc<dyn DatabasePlugin>>,
    subscribers: observers::Subscribers,
    pub(crate) deliveries: crate::events::Deliveries,
    pub(crate) triggers: crate::events::Triggers,
    path: PathBuf,
    storage: Option<WrapStorage>,
    pub(crate) budget: Budget,
    pub(crate) live: Mutex<vector::Totals>,
    pub(crate) work: Mutex<WorkDone>,
    pub(crate) signal: Arc<system::Signal>,
    pub(crate) maintenance: Maintenance,
    pub(crate) clock: Arc<dyn Clock>,
    pub(crate) expiry: Mutex<maintenance::Expiry>,
    /// The trigger history's retention in milliseconds, and the number its prune goes on from.
    pub(crate) audit_retention: std::sync::atomic::AtomicU64,
    pub(crate) audit_swept: Mutex<u64>,
    /// The limit this open started under, as `SHOW MEMORY_LIMIT` reports it.
    started_under: Option<u64>,
    disk_started_under: Option<u64>,
    disk: Mutex<Option<u64>>,
    /// The commit lock: held from before a commit looks at the newest moment until it has
    /// replaced it.
    pub(crate) committing: commit::Turn,
    callback_wait: Duration,
    /// What every statement and every transaction begins from. Held to copy it, to replace it
    /// and to read it again after a read that failed, never while a commit goes to the disk.
    newest: Mutex<Core>,
    /// The highest number a transaction was given or a commit carries.
    numbered: Mutex<u64>,
    pub(crate) cancelled: std::sync::atomic::AtomicU64,
    /// The last field, so that the view above is dropped before the folder is let go.
    pub(crate) store: Store,
}

/// Handles own this value; the worker owns only a weak reference to its open store.
struct Writer {
    open: Arc<Open>,
    worker: Option<std::thread::JoinHandle<()>>,
    channel: Mutex<Option<crate::read::Channel>>,
    channel_dir: PathBuf,
    folder: PathBuf,
}

impl Drop for Writer {
    fn drop(&mut self) {
        self.channel
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        self.open.signal.stop();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

/// What one transaction has staged.
pub(crate) struct Staged {
    pub(crate) erasing: Option<bool>,
    pub(crate) passes: Option<vector::Committed>,
    pub(crate) budget: Budget,
    pub(crate) reader: query::Reader,
    pub(crate) changes: ChangeSet,
    /// The tables as the transaction sees them: those of the moment it began at and those it
    /// has declared since.
    pub(crate) catalog: Arc<Catalog>,
    /// The declarations it ran, in order, each with the time it read, for a commit that has
    /// to run them again.
    declared: Vec<(Plan, Box<str>, Reservation, i64)>,
    /// The vector partitions its statements have made and emptied, for the limit a statement
    /// is refused by. The commit function does not read it: it counts again.
    pub(crate) partitions: Tally,
    /// The transaction's number; zero for one that runs no statement of a host.
    pub(crate) number: TxId,
    /// Its firings, until it commits or is refused.
    pub(crate) audit: crate::events::AuditQueue,
    source: CommitSource,
    /// How much of `changes.rows_read` the handle's total work already has.
    pub(crate) counted: u64,
}

/// Exact-text, FIFO retention of unbound plans within the room reserved at open.
pub(crate) const HELD_PER_TEXT_BYTE: u64 = 128;
type Kept = (Vec<(Box<str>, Plan, Option<Arc<str>>)>, u64);
/// A statement planned, its parse's charge, and its form when the record of statements keeps it.
pub(crate) type Planned = (Plan, Reservation, Option<Arc<str>>);
pub(crate) struct KeptPlans(Mutex<Kept>, Reservation);
impl KeptPlans {
    pub(crate) fn new(budget: &Budget, limit: Option<u64>) -> Result<Self> {
        let cap = crate::memory::account(limit, 0, None)[2];
        let held = budget.reserve(cap, "statement plan cache needed to open the store")?;
        Ok(Self(Mutex::new((Vec::with_capacity(1024), 0)), held))
    }

    pub(crate) fn planned(&self, text: &str, budget: &Budget) -> Result<Planned> {
        let mut guard = locked(&self.0);
        let (kept, used) = &mut *guard;
        if let Some((_, plan, form)) = kept.iter().find(|(key, ..)| &**key == text) {
            let held = sql::charge(text, budget)?;
            return Ok((plan.clone(), held, form.clone()));
        }
        let (statement, held, form) = sql::parse(text, budget)?;
        let plan = sql::plan(statement)?;
        let kind = matches!(plan, Plan::Select(_) | Plan::Insert(_) | Plan::Update(_));
        let form = (kind || matches!(plan, Plan::Delete(_))).then(|| Arc::from(form));
        let bytes = text.len() as u64 * HELD_PER_TEXT_BYTE;
        let slots = 1024 * size_of::<(Box<str>, Plan, Option<Arc<str>>)>() as u64;
        if !(plan.effect() == Effect::WritesRows || matches!(plan, Plan::Select(_)))
            || bytes + slots > self.1.bytes()
        {
            return Ok((plan, held, form));
        }
        while kept.len() == 1024 || *used + bytes + slots > self.1.bytes() {
            *used -= kept.remove(0).0.len() as u64 * HELD_PER_TEXT_BYTE;
        }
        *used += bytes;
        kept.push((text.into(), plan.clone(), form.clone()));
        Ok((plan, held, form))
    }
}

/// Refuses `statement`, by its name, to a handle that declared a narrowing.
pub(crate) fn admin(reader: &query::Reader, statement: Option<&str>) -> Result<()> {
    let statement = statement.filter(|_| !reader.is_admin()).map(str::to_owned);
    statement.map_or(Ok(()), |statement| Err(Error::AdminRequired { statement }))
}

/// Takes a lock. None of this part's locks is held across a step that can leave what the lock
/// guards half changed, so a holder that panicked left it whole.
pub(crate) fn locked<T>(lock: &Mutex<T>) -> MutexGuard<'_, T> {
    lock.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Database {
    /// Opens the store in this folder for writing. A folder that is missing or empty becomes a
    /// new store, so this never returns [`crate::Error::StoreMissing`].
    ///
    /// After a clean close this takes a few milliseconds whatever the store's size. After a
    /// writer that was killed, the open first reads every page the store has in use.
    ///
    /// # Errors
    ///
    /// Waits until any existing writer closes, including one in this process.
    /// [`crate::Error::DatabaseLocked`] when the caller's open deadline passes while waiting,
    /// and [`crate::Error::OpenCancelled`] when the caller cancels that wait.
    /// [`crate::Error::StoreIo`] when
    /// the folder or a file in it cannot be created, read or written, and
    /// [`crate::Error::StoreCorrupted`] when stored data fails its check.
    /// [`crate::Error::MemoryBudgetExceeded`] when the memory limit is too small to open a
    /// store under.
    pub fn open(path: impl AsRef<Path>) -> Result<Database> {
        Database::open_with_options(path, DatabaseOpenOptions::default())
    }

    /// [`Database::open`] with options.
    ///
    /// The default waits without a deadline and starts a maintenance worker. For one
    /// attempt on contention, pass the current clock time as `deadline`; for host-driven
    /// maintenance use `Maintenance::Caller`. Reopening from another thread in this
    /// process waits too.
    ///
    /// # Errors
    ///
    /// The open errors listed on `Database::open`, including `StoreIo` if path
    /// canonicalization or maintenance-thread creation fails.
    pub fn open_with_options(
        path: impl AsRef<Path>,
        options: DatabaseOpenOptions,
    ) -> Result<Database> {
        // The opening option is the session's ceiling, and the only one.
        let ceiling = options.memory_limit.map(|l| l as u64);
        let file = std::fs::metadata(path.as_ref().join("data")).map_or(0, |data| data.len());
        let cache = crate::store::cache_under(ceiling, None);
        crate::memory::admit(ceiling, crate::memory::account(ceiling, cache, Some(file)))?;
        let mut cache_limit = ceiling;
        let budget = Budget::new(ceiling)?;
        let store = Store::open_for_writing(path.as_ref(), &budget, &options)?;
        let view = store.view_on(budget.standing(), false)?;
        let settings = crate::settings::load(&view)?;
        // With none named, the limit is the one the store remembers, or the default.
        if options.memory_limit.is_none() {
            let remembered = settings[0].map(crate::settings::limit);
            cache_limit = remembered.flatten();
            budget.set_limit(remembered.unwrap_or_else(|| Some(system::default_memory_limit())))?;
        }
        let disk_started_under = options
            .disk_limit
            .or(settings[1].and_then(crate::settings::limit));
        store.disk_limit(disk_started_under);
        let catalog = Arc::new(Catalog::load(&view, &budget)?);
        let live = vector::Totals::load(&view, &catalog)?;
        let newest = Core {
            commit: view.commit(),
            catalog,
            view: Some(view),
        };
        let plans = KeptPlans::new(&budget, cache_limit)?;
        if let Some(plugin) = &options.plugin {
            plugin.on_open()?;
        }
        let open = Arc::new_cyclic(|this| Open {
            closes: Closes(options.plugin.clone()),
            this: this.clone(),
            last_cycle: Mutex::new(None),
            last_sweep: Mutex::new(None),
            plans,
            statements: Arc::new(Statements::new(&budget, settings[6])),
            plugin: options.plugin,
            subscribers: Default::default(),
            triggers: crate::events::Triggers::new(&budget),
            deliveries: crate::events::Deliveries::default(),
            path: path.as_ref().into(),
            storage: options.storage.clone(),
            live: Mutex::new(live),
            work: Mutex::new(WorkDone::default()),
            signal: Arc::new(system::Signal::default()),
            maintenance: options.maintenance,
            numbered: Mutex::new(newest.commit.0),
            cancelled: Default::default(),
            started_under: ceiling,
            disk_started_under,
            disk: Mutex::new(disk_started_under),
            budget,
            clock: options.clock.unwrap_or_else(system::machine_clock),
            committing: commit::Turn(Mutex::new(false), Default::default()),
            callback_wait: options.callback_wait.unwrap_or(Duration::from_secs(60)),
            expiry: Mutex::new(Default::default()),
            audit_retention: (7 * 86_400_000).into(),
            audit_swept: Mutex::new(0),
            newest: Mutex::new(newest),
            store,
        });
        drop(open.turn()?);
        if open.store.recovery_state()?.0.is_some() {
            open.signal.wake();
        }
        let folder = std::fs::canonicalize(path.as_ref()).map_err(|error| Error::StoreIo {
            operation: "open",
            path: path.as_ref().into(),
            reason: crate::types::excerpt(error),
        })?;
        let worker = if options.maintenance == Maintenance::Engine {
            let open = open.clone();
            let poll_open = open.clone();
            Some(
                system::worker(
                    open.signal.clone(),
                    open.clock.clone(),
                    move || {
                        poll_open
                            .newest()
                            .and_then(|(view, _)| view.setting(7))
                            .ok()
                            .flatten()
                            .unwrap_or(crate::settings::SETTINGS[7].2)
                    },
                    move |partial| maintenance::cycle(&open, partial).map(|(_, again)| again),
                )
                .map_err(|error| Error::StoreIo {
                    operation: "open",
                    path: path.as_ref().into(),
                    reason: crate::types::excerpt(error),
                })?,
            )
        } else {
            None
        };
        let channel_dir = options.channel_dir.unwrap_or_else(|| folder.clone());
        Ok(Database {
            open: Mutex::new(Some(Arc::new(Writer {
                open,
                worker,
                folder,
                channel_dir,
                channel: Mutex::new(None),
            }))),
            reader: query::Reader::admin(),
            bound: None,
        })
    }

    /// Registers an opaque request handler on four joined listeners. Startup errors leave
    /// the writer usable. Handlers must return when their cancellation capability fires.
    pub fn serve_requests(
        &self,
        namespace: &str,
        handler: Arc<dyn crate::read::OwnerRequestHandler>,
    ) -> Result<()> {
        self.opened()?;
        admin(&self.reader, Some("serve_requests"))?;
        let writer = locked(&self.open)
            .as_ref()
            .cloned()
            .ok_or(Error::DatabaseClosed)?;
        let mut channel = locked(&writer.channel);
        if channel.is_some() {
            return Err(Error::PlanError(
                "a request handler is already registered".into(),
            ));
        }
        *channel = Some(crate::read::Channel::start(
            &writer.folder,
            &writer.channel_dir,
            namespace,
            handler,
            &writer.open.budget,
        )?);
        Ok(())
    }

    fn planned(&self, text: &str) -> Result<(Planned, Arc<Open>)> {
        let open = self.opened()?;
        Ok((open.plans.planned(text, &open.budget)?, open))
    }

    /// The open store, or the refusal of a handle that was closed.
    fn opened(&self) -> Result<Arc<Open>> {
        self.unbound()?;
        let open = locked(&self.open)
            .as_ref()
            .map(|writer| writer.open.clone())
            .ok_or(Error::DatabaseClosed)?;
        open.store.valid()?;
        Ok(open)
    }

    /// Opens a bounded reader with its own memory and the narrowing of this handle.
    ///
    /// Returns `CallbackReentry` on a callback handle or the firing thread during trigger
    /// dispatch, and `DatabaseClosed` after close.
    ///
    /// Inherits this handle's contexts, labels and principal. Opens the store files again
    /// and reserves separate page/plan caches; `limits.memory` must admit at least 11,018,240 bytes.
    ///
    /// # Errors
    ///
    /// `MemoryBudgetExceeded` for the opening reservations, `StoreMissing`, `StoreIo`
    /// or `StoreCorrupted` when files cannot be opened/read, plus the control refusals above.
    pub fn read_session(&self, limits: ReadLimits) -> Result<ReadSession> {
        self.session(limits, None)
    }
    /// Opens a bounded reader whose observer runs inline during each statement.
    ///
    /// Returns `CallbackReentry` on a callback handle or the firing thread during trigger
    /// dispatch, and `DatabaseClosed` after close.
    ///
    /// Inherits this handle's contexts, labels and principal. Opens the store files again
    /// and reserves separate page/plan caches; `limits.memory` must admit at least 11,018,240 bytes.
    ///
    /// # Errors
    ///
    /// `MemoryBudgetExceeded` for the opening reservations, `StoreMissing`, `StoreIo`
    /// or `StoreCorrupted` when files cannot be opened/read, plus the control refusals above.
    pub fn read_session_with_progress(
        &self,
        limits: ReadLimits,
        observer: Arc<dyn ReadProgressObserver>,
    ) -> Result<ReadSession> {
        self.session(limits, Some(observer))
    }
    fn session(
        &self,
        limits: ReadLimits,
        observer: Option<Arc<dyn ReadProgressObserver>>,
    ) -> Result<ReadSession> {
        let open = self.opened()?;
        let options = ReadSessionOptions {
            limits,
            observer,
            clock: Some(open.clock.clone()),
            storage: open.storage.clone(),
            contexts: None,
            scope_labels: None,
            principal: None,
        };
        ReadSession::live(&open, &open.path, options, self.reader.clone())
    }

    /// A table as it was declared, as of the newest commit: its columns, its UNIQUE rules, its
    /// indexes and the options `TableMeta` has fields for. A primary key column is not nullable, however the key was written.
    ///
    /// # Errors
    ///
    /// [`crate::Error::TableNotFound`] when the store has no table of this name, and
    /// [`crate::Error::DatabaseClosed`] on a handle that was closed.
    ///
    /// Returns `CallbackReentry` on a callback handle or the firing thread during trigger
    /// dispatch, and `DatabaseClosed` after close.
    ///
    /// Metadata reads use the reader budget and bounds, as statement reads do.
    pub fn table_meta(&self, name: &str) -> Result<crate::Owned<TableMeta>> {
        let open = self.opened()?;
        locked(&open.newest).catalog.meta_answer(name, &open.budget)
    }

    /// The older versions of one row, as [`crate::ReadSession::row_versions`] lists them.
    pub fn row_versions(&self, table: &str, key: &[Value], n: usize) -> Result<RowVersions> {
        let (open, reader) = (self.opened()?, &self.reader);
        let (view, catalog) = open.newest()?;
        row_versions(&view, &catalog, reader, &open.budget, (table, key, n))
    }

    /// Lists committed declarations without reading row or change history.
    ///
    /// Returns `CallbackReentry` on a callback handle or the firing thread during trigger
    /// dispatch, and `DatabaseClosed` after close.
    ///
    /// Metadata and audit reads use the reader budget and bounds.
    ///
    /// # Errors
    ///
    /// Reader budget/limit refusals, and the control refusals above.
    pub fn declared_events(&self) -> Result<crate::Owned<crate::types::DeclaredEvents>> {
        use sql::ast::EventDeclaration::*;
        let open = self.opened()?;
        let newest = locked(&open.newest);
        let held = newest.catalog.answering(&open.budget, 1)?;
        let mut answer = crate::types::DeclaredEvents::default();
        for (event, _) in newest.catalog.event_values() {
            match event {
                Trigger(d) => answer.triggers.push(d.clone()),
                EventType(d) => answer.event_types.push(d.clone()),
                Sink(d) => answer.sinks.push(d.clone()),
                Route(d) => answer.routes.push(d.clone()),
                Schedule(_) => {}
            }
        }
        Ok(crate::Owned::charged(answer, held))
    }
    /// Returns the normalized name and metadata of CREATE TABLE text without opening a store.
    pub fn declared_table(text: &str) -> Result<(String, TableMeta)> {
        let (plan, ..) = sql::parse(text, &Budget::new(Some(2 << 20))?)?;
        match plan {
            Plan::CreateTable(table) => schema::Catalog::declared_table(&table),
            _ => Err(Error::PlanError("expected CREATE TABLE text".into())),
        }
    }

    /// Runs one SQL statement in a transaction of its own and returns its result.
    ///
    /// A statement that changes something is committed before this returns: on `Ok` the change
    /// is on disk, on `Err` nothing of it is.
    ///
    /// Values are bound by name. `$name` in the statement takes the value kept under the key
    /// `name`, written without the `$`, in `params`; [`crate::params!`] writes the map. There
    /// is no positional form. A `$name` with no key in the map is refused with
    /// [`crate::Error::PlanError`], and a key the statement does not use is ignored.
    ///
    /// This may be called while a [`Transaction`] of the same handle is open, on the same
    /// thread too. The statement commits on its own, the open transaction does not see it, and
    /// the transaction's commit is then checked against it as [`Transaction::commit`] says.
    ///
    /// # Errors
    ///
    /// Whatever refuses the statement, each by its own kind of [`crate::Error`];
    /// [`crate::Error::StoreIo`] or [`crate::Error::StoreCorrupted`] when the store cannot be
    /// read or written; [`crate::Error::DatabaseClosed`] on a handle that was closed.
    ///
    /// Trigger dispatch precedes final constraint validation. It can return
    /// `TriggerCallbackMissing`, `TriggerCascadeDepthExceeded`, `TriggerCallbackFailed`
    /// or an error returned by the callback; `CREATE TRIGGER ... WHEN DELETE` returns
    /// `TriggerEventUnsupported`. Control use on the firing thread returns `CallbackReentry`;
    /// SQL through the active callback handle participates in its firing transaction.
    pub fn execute(&self, sql: &str, params: &HashMap<String, Value>) -> Result<QueryResult> {
        if let Some(bound) = &self.bound {
            return self.execute_bound(bound, sql, params);
        }
        let open = self.opened()?;
        let generation = open.cancelled.load(Relaxed);
        let mut seen = None;
        let result = open.observed(sql, || {
            let (plan, mut parse, form) = open.plans.planned(sql, &open.budget)?;
            if matches!(plan.effect(), Effect::Declares) {
                open.host(|plugin| plugin.on_ddl(sql))?;
            }
            if matches!(plan.effect(), Effect::Declares | Effect::WritesRows) {
                // Taken before the statement reads anything, so no other commit can overtake it.
                let purge = matches!(plan, Plan::Purge(_));
                if purge && crate::events::delivery::IN_SINK_CALL.get() {
                    let operation = "PURGE inside a sink".into();
                    return Err(Error::CallbackReentry { operation });
                }
                let _delivery = purge.then(|| locked(&open.deliveries.active));
                let held = open.writing_turn()?;
                let mut staged = open.transaction_stage()?;
                staged.reader = self.reader.clone();
                let budget = &staged.budget;
                staged.changes.set_number(staged.number);
                if matches!(plan, Plan::Delete(_) | Plan::Purge(_) | Plan::Update(_))
                    || matches!(plan.effect(), Effect::Declares)
                {
                    open.store.through(&mut staged.changes);
                }
                let selection = match &plan {
                    Plan::Purge(selections) => Some((selections.as_slice(), true)),
                    Plan::Delete(delete) => Some((std::slice::from_ref(delete), false)),
                    _ => None,
                };
                let result = if let Some((selections, purge)) = selection {
                    parse.grow(sql::planning(sql))?;
                    budget.reset_peak(open.budget.used().saturating_sub(budget.used()));
                    let now = system::now_millis(&*open.clock);
                    open.recorded(form.as_deref(), generation, |counted| {
                        let examined = (!purge).then_some((&counted.examined, Some(counted)));
                        let staged = &mut staged;
                        let result = crate::erase::stage(
                            &open, selections, purge, params, staged, now, examined,
                        )
                        .map(|count| QueryResult::affected(count, counted.examined.get()))
                        .map(|done| done.evaluated(counted.evaluations.get()));
                        staged.counted = staged.changes.rows_read.get();
                        (result, staged.counted)
                    })?
                } else {
                    open.run(
                        plan,
                        (sql, parse, form),
                        params,
                        &mut staged,
                        &self.reader,
                        generation,
                    )?
                };
                let budget = staged.budget.clone();
                open.commit(held, staged)?;
                Ok(result.measured(&budget, &self.reader))
            } else {
                // A statement that stages nothing has nothing to commit.
                let mut staged = open.begin()?;
                seen = Some(staged.changes.base().clone());
                open.run(
                    plan,
                    (sql, parse, form),
                    params,
                    &mut staged,
                    &self.reader,
                    generation,
                )
            }
        });
        query::handed(result, seen.as_ref())
    }

    /// Holds the newest committed view until the returned snapshot is dropped.
    pub fn snapshot(&self) -> Result<crate::Snapshot> {
        let open = self.opened()?;
        let budget = open.budget.statement();
        let store = Store::open_for_reading(
            &open.path,
            &budget,
            &ReadSessionOptions {
                storage: open.storage.clone(),
                ..Default::default()
            },
        )?;
        let (view, catalog) =
            Core::load(store.view()?, &mut None, &budget)?.newest(&store, &budget)?;
        Ok(crate::Snapshot {
            path: open.path.clone(),
            view,
            catalog,
            _store: store,
        })
    }
    /// Runs a read at a held moment, under this handle's narrowing and memory ceiling.
    pub fn execute_at_snapshot(
        &self,
        sql: &str,
        params: &HashMap<String, Value>,
        snapshot: &crate::Snapshot,
    ) -> Result<QueryResult> {
        let ((plan, mut parsing, form), open) = self.planned(sql)?;
        if open.path != snapshot.path {
            return Err(Error::SnapshotMismatch);
        }
        if plan.effect() != Effect::Reads {
            return Err(Error::ReadOnlySession);
        }
        admin(&self.reader, plan.admin_only())?;
        let parse_bytes = parsing.bytes();
        parsing.grow(sql::planning(sql))?;
        let now = system::now_millis(&*open.clock);
        let charge = (parsing.bytes(), parse_bytes);
        let changes = ChangeSet::new(snapshot.view.clone(), &open.budget)?;
        let (catalog, view, reader) = (&snapshot.catalog, &changes, &self.reader);
        let generation = open.cancelled.load(Relaxed);
        let result = open.recorded(form.as_deref(), generation, |counted| {
            let given = (now, Some(counted), Some(&*open), charge);
            let result = query::run(&plan, params, catalog, view, reader, &open.budget, given);
            (result, changes.rows_read.get())
        });
        query::handed(result, Some(&snapshot.view))
    }

    /// Registers or replaces a schedule's transactional callback by its TX name.
    /// Only the supplied handle may stage writes; failed callbacks consume their tick.
    pub fn register_cron_callback<F>(&self, name: &str, callback: F) -> Result<()>
    where
        F: Fn(&Database) -> Result<()> + Send + Sync + 'static,
    {
        admin(&self.reader, Some("schedule callbacks"))?;
        let open = self.opened()?;
        open.triggers.register_cron(name, callback)?;
        open.signal.wake();
        Ok(())
    }

    /// Begins a transaction over the newest commit. Nothing it does is visible to anyone else
    /// until it commits. Several transactions of one handle may be open at once.
    ///
    /// # Errors
    ///
    /// [`crate::Error::DatabaseClosed`] on a handle that was closed, and
    /// [`crate::Error::StoreIo`] or [`crate::Error::StoreCorrupted`] when the store cannot be
    /// read.
    ///
    /// Returns `CallbackReentry` on a callback handle or the firing thread during trigger
    /// dispatch, and `DatabaseClosed` after close.
    pub fn transaction(&self) -> Result<Transaction<'_>> {
        let open = self.opened()?;
        let mut staged = open.transaction_stage()?;
        staged.source = CommitSource::User;
        staged.reader = self.reader.clone();
        Ok(Transaction {
            staged: RefCell::new(staged),
            db: self,
        })
    }

    /// Declares the memory limit in bytes, or no limit for `None`.
    ///
    /// The limit takes hold at once and is committed, so the store remembers it for every
    /// later open whose options name no limit, exactly as `SET MEMORY_LIMIT` does. The page
    /// and plan caches keep the sizes this open gave them; the next open sizes them from the limit.
    ///
    /// # Errors
    ///
    /// [`crate::Error::MemoryBudgetExceeded`] when the limit is below the smallest this open
    /// can take: 1 MiB, its page and plan caches, and what open transactions and live results
    /// hold. That is never less than the open's floor, and is more for a store opened
    /// with no limit in force. The refusal names it; a limit below the caches of a store opened
    /// under a larger limit is given as [`DatabaseOpenOptions::memory_limit`] when the store is
    /// opened. [`crate::Error::StoreIo`] when the commit cannot be written. In both cases the
    /// limit stays what it was.
    /// [`crate::Error::DatabaseClosed`] on a handle that was closed.
    /// [`crate::Error::AdminRequired`] on a narrowed handle, as `SET MEMORY_LIMIT` is.
    ///
    /// Returns `CallbackReentry` on a callback handle or the firing thread during trigger
    /// dispatch, and `DatabaseClosed` after close.
    ///
    /// A damaged settings record can return `StoreCorrupted`; the prior limit remains in force.
    pub fn set_memory_limit(&self, limit: Option<usize>) -> Result<()> {
        admin(&self.reader, Some("SET MEMORY_LIMIT"))?;
        self.opened()?.set(0, limit.map_or(u64::MAX, |n| n as u64))
    }

    /// The memory limit in force, in bytes; `None` when there is none.
    ///
    /// Returns DatabaseClosed when the handle was closed.
    ///
    /// Returns `CallbackReentry` on a callback handle or the firing thread during trigger
    /// dispatch, and `DatabaseClosed` after close.
    pub fn memory_limit(&self) -> Result<Option<usize>> {
        Ok(self.opened()?.budget.limit().map(|limit| limit as usize))
    }

    /// Declares a disk ceiling after committing under the prior ceiling; growth refusals leave the handle usable.
    pub fn set_disk_limit(&self, limit: Option<u64>) -> Result<()> {
        admin(&self.reader, Some("SET DISK_LIMIT"))?;
        self.opened()?.set(1, limit.unwrap_or(u64::MAX))
    }
    /// The live disk ceiling, in bytes; None means unlimited. Refuses a closed or callback handle.
    pub fn disk_limit(&self) -> Result<Option<u64>> {
        Ok(*locked(&self.opened()?.disk))
    }

    /// The bytes counted against the memory limit at this moment: what every open sets aside,
    /// and what open transactions and results not yet dropped hold. `SHOW MEMORY_LIMIT` reports
    /// the same number as `used`.
    ///
    /// Returns DatabaseClosed when the handle was closed.
    ///
    /// Returns `CallbackReentry` on a callback handle or the firing thread during trigger
    /// dispatch, and `DatabaseClosed` after close.
    pub fn memory_used(&self) -> Result<usize> {
        Ok(self.opened()?.budget.used() as usize)
    }

    /// The names of the store's tables as of the newest commit, in name order. A new store has
    /// none.
    ///
    /// Returns DatabaseClosed when the handle was closed.
    ///
    /// Returns `CallbackReentry` on a callback handle or the firing thread during trigger
    /// dispatch, and `DatabaseClosed` after close.
    pub fn table_names(&self) -> Result<crate::Owned<Vec<String>>> {
        let open = self.opened()?;
        locked(&open.newest).catalog.table_names(&open.budget, None)
    }

    /// A table's declaration as SQL text, as of the newest commit: the `CREATE TABLE` statement
    /// that would declare the table as it stands, with every clause left at its default not
    /// written (except `MAX_PARTITIONS`, which is printed for every column with a `PARTITION_KEY`), then one `CREATE INDEX` statement for each index that was made with one. Each
    /// statement ends with `;` and a new line.
    ///
    /// # Errors
    ///
    /// [`crate::Error::TableNotFound`] when the store has no table of this name, and
    /// [`crate::Error::DatabaseClosed`] on a handle that was closed. The answer is not read
    /// from the disk. Metadata reads are charged and bounded by the reader budget.
    /// `CallbackReentry` is also returned during trigger callback dispatch.
    pub fn declaration(&self, table: &str) -> Result<crate::memory::Owned<String>> {
        let open = self.opened()?;
        locked(&open.newest).declaration(table, &open.budget)
    }

    /// The number of the newest commit on disk: 0 for a store nothing was ever committed to,
    /// and higher with every commit since, by one or by more: a commit carries its
    /// transaction's number ([`Transaction::id`]). A transaction that commits nothing does not
    /// move it, and no number is ever reported for work that did not commit.
    ///
    /// The position is for comparing two moments of one store. No call of this library takes
    /// one.
    ///
    /// Returns DatabaseClosed when the handle was closed.
    ///
    /// Returns `CallbackReentry` on a callback handle or the firing thread during trigger
    /// dispatch, and `DatabaseClosed` after close.
    ///
    /// The default engine worker can commit maintenance and advance this number without any
    /// host write; a refused write whose triggers fired advances it by the commit that stores
    /// their rolled-back entries, past numbers other writes may hold.
    pub fn committed_watermark(&self) -> Result<TxId> {
        Ok(locked(&self.opened()?.newest).commit)
    }

    /// What a statement would do as of the newest commit, without running it, as text of one
    /// `name: value` to a line. For a similarity search it names the mode asked for and the one
    /// in force, and for each partition the filter selects how it is searched: every vector of
    /// it, or the groups of vectors nearest the one searched for. Nothing is read but the
    /// store's small records, and nothing changes. A transaction has no such call.
    ///
    /// # Errors
    ///
    /// Whatever refuses the statement before it reads a row, by its own kind;
    /// [`crate::Error::StoreIo`] or [`crate::Error::StoreCorrupted`] when the store cannot be
    /// read; [`crate::Error::DatabaseClosed`] on a handle that was closed.
    ///
    /// Returns `CallbackReentry` on a callback handle or the firing thread during trigger
    /// dispatch, and `DatabaseClosed` after close.
    pub fn explain(&self, sql: &str) -> Result<crate::Owned<String>> {
        let ((plan, parsing, _), open) = self.planned(sql)?;
        let (view, catalog) = open.newest()?;
        let now = system::now_millis(&*open.clock);
        let (reader, budget, at) = (&self.reader, &open.budget, (now, None, parsing.bytes()));
        query::explain(&plan, &HashMap::new(), &catalog, &view, reader, budget, at)
    }

    /// Checks one committed store without changing it, returning content bytes by table.
    /// Damaged records or inconsistent indexes and counts return `StoreCorrupted`.
    ///
    /// Returns `CallbackReentry` on a callback handle or the firing thread during trigger
    /// dispatch, and `DatabaseClosed` after close.
    ///
    /// # Errors
    ///
    /// `AdminRequired` on a narrowed handle; `StoreCorrupted` on damaged content or
    /// inconsistent indexes/counts; `StoreIo` on storage failures; `MemoryBudgetExceeded`
    /// when verification cannot reserve its working memory, plus the control refusals above.
    pub fn verify(&self) -> Result<crate::Owned<crate::VerifyReport>> {
        admin(&self.reader, Some("verify"))?;
        let open = self.opened()?;
        let (view, catalog) = open.newest()?;
        maintenance::verify(&view, &catalog, &open.budget)
    }

    /// Exports one committed moment to a new store folder, retaining every record kind.
    /// The copy omits `identity.key`. Copy the whole folder with no writer open to keep it.
    /// Refuses an existing destination, damaged source data, or insufficient MEMORY_LIMIT.
    pub fn export_snapshot(&self, dest: impl AsRef<Path>) -> Result<crate::ExportReport> {
        admin(&self.reader, Some("export_snapshot"))?;
        let open = self.opened()?;
        let view = open.store.view()?;
        maintenance::export(&view, dest.as_ref(), &open.budget)
    }

    /// Runs one cycle of maintenance and returns what it did.
    ///
    /// A cycle first advances every partition with an IVF due or being built
    /// by one step, each step in a commit of its own: a build begins, moves its centroids
    /// once over a sample of at most 64 vectors a group, gives the vectors of whole blocks their
    /// groups in at most 4,194,304 comparisons, packs whole groups of at most 4,096 vectors
    /// together into new blocks, measures at most 4,194,304 vectors or binary codes against its
    /// questions, or puts the new IVF in force. A step that goes past its bound does so
    /// by one block, one group or one question. A store with nothing to do is left as it is,
    /// and no commit is made. Next it expires due retained rows in batches of up to
    /// 1,024 keys, with at most 8,192 older backlog removals per cycle, then seals at
    /// most 4,096 pending vectors into blocks. Newly sealed vectors are available
    /// to the next cycle's build step. Expiry permanently removes rows through DELETE's
    /// rules; `SYNC SAFE` does not fence unsynced rows in this version.
    ///
    /// Vector maintenance preserves search answers; row expiry can remove due rows.
    ///
    /// Under a memory limit with no room to seal everything, a cycle seals the partitions it has
    /// room for, at least one. A step of a build that has no room is not an error: the cycle
    /// records the bytes the step needed, reports the partition as stalled, and goes on.
    ///
    /// The engine worker runs cycles by default. With [`Maintenance::Caller`] the host
    /// calls this while `work_remaining` is `Some(true)`. Use an unnarrowed handle:
    /// every report field is `None` on narrowed handles even though the cycle maintains
    /// the whole store. Yield while `compaction_waiting_on` names a reader; the worker
    /// waits for its next poll. Memory-stalled vector builds wait for room.
    /// A step is worked out with no lock held, so other calls on the handle wait only for a
    /// step's commit, as they wait for any other commit, and never for its arithmetic; readers
    /// do not wait.
    ///
    /// # Errors
    ///
    /// [`crate::Error::MemoryBudgetExceeded`] when the memory limit has no room to seal the
    /// pending vectors of one partition, list build work, or expire rows. Sealing holds
    /// one block's bytes (256 KiB less 64) for its
    /// whole run, the keys of the pending vectors it collected (at most 4,096, each key's bytes
    /// plus 24), the partition's record, and one row with its vector at a time; then nothing
    /// was sealed. [`crate::Error::StoreIo`] or [`crate::Error::StoreCorrupted`] when the store
    /// cannot be read or written; [`crate::Error::DatabaseClosed`] on a handle that was closed.
    /// The commits made before the error stay, and every vector is still found by a search.
    ///
    /// Returns `CallbackReentry` on a callback handle or the firing thread during trigger
    /// dispatch, and `DatabaseClosed` after close.
    pub fn run_maintenance_cycle(&self) -> Result<MaintenanceReport> {
        let open = self.opened()?;
        maintenance::cycle(&open, true).map(|(mut report, more)| {
            if self.reader.is_admin() {
                report.work_remaining = Some(more || report.compaction_waiting_on.is_some());
                report
            } else {
                report.withheld()
            }
        })
    }

    /// One compaction step of at most 256 pages now; a reader holding moved pages is reported.
    pub fn compact_now(&self) -> Result<MaintenanceReport> {
        let open = self.opened()?;
        let (moved, waiting, more, ratio) = maintenance::compact(&open, true)?;
        let report = MaintenanceReport {
            compacted_pages: Some(moved),
            compaction_waiting_on: waiting,
            compaction_free_ratio: ratio,
            work_remaining: Some(more || waiting.is_some()),
            ..Default::default()
        };
        Ok(if self.reader.is_admin() {
            report
        } else {
            report.withheld()
        })
    }

    pub fn page_slack_ratio(&self) -> Option<f64> {
        let read = || self.opened().ok()?.newest().ok()?.0.page_slack().ok();
        self.reader.is_admin().then(read).flatten()
    }

    /// Cancels the statement or the maintenance cycle this handle is running; later ones run.
    pub fn cancel(&self) {
        let _ = match &self.bound {
            Some(bound) => bound.with_staged(|open, _| Ok(open.cancels(1))).ok(),
            None => locked(&self.open).as_ref().map(|w| w.open.cancels(1)),
        };
    }

    /// Running work totals for this open. A narrowed handle is refused by name.
    ///
    /// Returns `CallbackReentry` on a callback handle or the firing thread during trigger
    /// dispatch, and `DatabaseClosed` after close.
    ///
    /// # Errors
    ///
    /// `AdminRequired` on a narrowed handle, plus the control refusals above.
    pub fn work_done(&self) -> Result<WorkDone> {
        admin(&self.reader, Some("work_done"))?;
        let open = self.opened()?;
        let work = *locked(&open.work);
        Ok(work)
    }

    /// Lets go of the store, so that the next writer can open it. Every commit that returned is
    /// already on disk. The last holder's close runs the close hook, then finishes the rebuild
    /// of the free-page map an open after a kill left owed, commits the map, flushes and closes
    /// the file, and returns the drive's or the store's first failure, else the hook's. A full
    /// store leaves the map unsaved, and the next open rebuilds it. A handle that is not the
    /// last, because another scoped handle is open or a call is still running on another
    /// thread, closes nothing and returns `Ok`; the store is closed when the last lets go.
    /// Closing a handle that was closed changes nothing.
    ///
    /// Later fallible calls on this handle or its transactions return `DatabaseClosed`.
    /// The infallible `registered_trigger_callbacks` and `trigger_cascade_depth_cap` calls panic instead.
    /// Closing the last writing handle stops and joins maintenance and request-listener
    /// threads; it does not run a final maintenance cycle.
    ///
    /// # Errors
    ///
    /// `CallbackReentry` on a callback handle or the firing thread during dispatch.
    pub fn close(&self) -> Result<()> {
        self.unbound()?;
        let Some(writer) = locked(&self.open).take() else {
            return Ok(());
        };
        let open = writer.open.clone();
        drop(writer);
        Arc::into_inner(open).map_or(Ok(()), Open::close)
    }
}

/// A transaction: statements that commit together or not at all.
///
/// It reads the store as of the commit it began at, together with what it has written itself;
/// nobody else sees its changes until [`Transaction::commit`] returns. Dropping it without
/// committing rolls it back.
///
/// A transaction can be moved to another thread and cannot be shared between two: it is `Send`
/// and not `Sync`.
pub struct Transaction<'db> {
    staged: RefCell<Staged>,
    db: &'db Database,
}

impl Transaction<'_> {
    /// Runs one SQL statement inside the transaction. Values are bound by name, as
    /// [`Database::execute`] binds them.
    ///
    /// A refused statement changes nothing and leaves the transaction usable. A transaction
    /// either declares tables and indexes or writes rows: the other kind of statement is
    /// refused with [`crate::Error::MixedSchemaAndRowWrite`].
    ///
    /// # Errors
    ///
    /// Statement errors (including rule, memory and storage refusals), `DatabaseClosed`
    /// after close, and `CallbackReentry` during trigger dispatch on the firing thread.
    ///
    /// `SET MEMORY_LIMIT` is not part of the transaction. It takes hold at once, and a rollback
    /// does not undo it.
    pub fn execute(&self, sql: &str, params: &HashMap<String, Value>) -> Result<QueryResult> {
        let (open, mut reads) = (self.db.opened()?, false);
        let result = open.observed(sql, || {
            let (plan, parsing, form) = open.plans.planned(sql, &open.budget)?;
            reads = plan.effect() == Effect::Reads;
            if matches!(plan.effect(), Effect::Declares) {
                open.host(|plugin| plugin.on_ddl(sql))?;
            }
            let staged = &mut *self.staged.borrow_mut();
            let mark = staged.changes.mark();
            let generation = open.cancelled.load(Relaxed);
            let result = open.run(
                plan,
                (sql, parsing, form),
                params,
                staged,
                &self.db.reader,
                generation,
            );
            if result.is_err() {
                staged.changes.rewind(mark);
                staged.partitions.forget();
            }
            result
        });
        query::handed(result, reads.then_some(self.staged.borrow().changes.base()))
    }

    /// Commits and consumes this transaction. Success is on disk; refused growth writes nothing
    /// and leaves the store usable. Failed writes or syncs make the handle refuse every call;
    /// close and reopen to see the whole state before or after that commit.
    ///
    /// # Errors
    ///
    /// A transaction that was open while another commit landed is checked against that commit:
    ///
    /// - [`crate::Error::UniqueViolation`] when the other commit took a key, or a value of a
    ///   unique column, that this transaction inserted;
    /// - [`crate::Error::ConditionalUpdateConflict`] when the other commit changed or deleted a
    ///   row that this transaction changed or deleted, or a row it guarded no longer holds the
    ///   guarded values ([`Transaction::guard_row_conditions`]);
    /// - [`crate::Error::SchemaInvalid`] when a table or an index was declared meanwhile and
    ///   this transaction wrote rows;
    /// - for a transaction that declared tables or indexes, whatever would refuse its
    ///   statements if they were run now: for one, [`crate::Error::SchemaInvalid`] when a table
    ///   name it declared has been taken.
    ///
    /// A statement run with [`Database::execute`] is never overtaken and meets none of these.
    ///
    /// Also [`crate::Error::StoreIo`] when the commit cannot be written, and
    /// [`crate::Error::DatabaseClosed`] when the handle was closed.
    ///
    /// Final validation can also return `ForeignKeyViolation` (references are deferred),
    /// `CycleDetected`, `VectorPartitionLimitExceeded`, `MemoryBudgetExceeded` or
    /// `StoreCorrupted`. Trigger dispatch can return `TriggerCallbackMissing`,
    /// `TriggerCascadeDepthExceeded`, `TriggerCallbackFailed`, a callback's own error,
    /// or `CallbackReentry` or `CallbackActive`. A closed handle returns `DatabaseClosed`.
    pub fn commit(self) -> Result<()> {
        let open = self.db.opened()?;
        let held = open.writing_turn()?;
        open.commit(held, self.staged.into_inner())
    }

    /// The transaction's number, taken when it began: above every commit there was then and
    /// above the number of every transaction begun before it. It is the highest value a `TXID`
    /// column takes from this transaction; a higher one is refused with
    /// [`crate::Error::TxIdOutOfRange`]. The commit carries this number, or the next after the
    /// newest commit when another commit has taken this number or a higher one meanwhile.
    pub fn id(&self) -> TxId {
        self.staged.borrow().number
    }

    /// Answers whether the row that `key_column = key_value` names is shown to the handle and
    /// holds every `(column, value)` of `predicates` now, and if so asks the commit to stand
    /// only if the row still holds them then: otherwise the commit is refused with
    /// [`crate::Error::ConditionalUpdateConflict`]. Of several rows with that value it is the
    /// first one found. The guard writes nothing, so a guarded row is not a changed row.
    ///
    /// # Errors
    ///
    /// [`crate::Error::TableNotFound`] and [`crate::Error::ColumnNotFound`] for a name that is
    /// not declared, and whatever refuses a read of the table through this handle.
    ///
    /// Returns `CallbackReentry` on a callback handle or the firing thread during trigger
    /// dispatch, and `DatabaseClosed` after close.
    pub fn guard_row_conditions(
        &self,
        table: &str,
        key_column: &str,
        key_value: &Value,
        predicates: &[(String, Value)],
    ) -> Result<bool> {
        let open = self.db.opened()?;
        let staged = &mut *self.staged.borrow_mut();
        let catalog = staged.catalog.clone();
        let missing = || Error::TableNotFound(table.to_owned());
        let found = catalog.table(table).ok_or_else(missing)?;
        let columns = &found.meta.columns;
        let mut needed = vec![false; columns.len()];
        let mut held = Vec::new();
        for (name, value) in predicates {
            let at = columns.iter().position(|column| column.name == *name);
            let at = at.ok_or_else(|| Error::ColumnNotFound {
                table: table.to_owned(),
                column: name.clone(),
            })?;
            needed[at] = true;
            held.push((at, value.clone()));
        }
        let filter = Expr::BinaryOp {
            left: Box::new(Expr::Column(ColumnRef {
                table: None,
                column: key_column.to_owned(),
            })),
            op: BinOp::Eq,
            right: Box::new(Expr::Parameter("key".to_owned())),
        };
        let params = HashMap::from([("key".to_owned(), key_value.clone())]);
        let kept = query::Kept::new(open.budget.passing("expression evaluation")?);
        let asked = query::Asked {
            keys: None,
            table: found,
            filter: Some(&filter),
            assigned: &[],
            needed: &needed,
            reader: &self.db.reader,
            examined: None,
            ctx: query::EvalContext::new(&params, 0, &kept),
        };
        let rows = query::rows_for_write(&asked, None, &catalog, &staged.changes, &open.budget)?;
        locked(&open.work).rows_read += staged.changes.rows_read.get() - staged.counted;
        staged.counted = staged.changes.rows_read.get();
        let Some((key, cells)) = rows.and_then(|(rows, _)| rows.into_iter().next()) else {
            return Ok(false);
        };
        if held
            .iter()
            .any(|(at, value)| !matches!(&cells[*at], Cell::Here(holds) if holds == value))
        {
            return Ok(false);
        }
        staged.changes.guard_row(found.id, key, held)?;
        Ok(true)
    }

    /// Rolls back now. What the transaction staged is held in memory only, so this drops it,
    /// writes nothing and cannot fail. Dropping the transaction does the same.
    pub fn rollback(self) {}
}

/// The close hook of a store let go by being dropped, whose result is ignored.
struct Closes(Option<Arc<dyn DatabasePlugin>>);
impl Drop for Closes {
    fn drop(&mut self) {
        let hook = || self.0.take().map(|plugin| plugin.on_close());
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(hook));
    }
}

impl Open {
    /// The store's close by its last holder: the hook, then the storage library's last work,
    /// whose failure outranks the hook's.
    fn close(mut self) -> Result<()> {
        self.closes.0 = None;
        let hook = self.host(|plugin| plugin.on_close());
        let store = { self }.store;
        store.close().and(hook)
    }
}
