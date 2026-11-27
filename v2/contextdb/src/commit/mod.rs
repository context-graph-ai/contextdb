//! commit: one store held for writing, what a transaction stages on it, and the one commit
//! function every write goes through, with the triggers it fires on the way.

mod observers;
mod open;
mod plugin;
mod triggers;
mod turn;
pub(crate) use observers::active;
pub use observers::{CommitEvent, SubscriptionMetrics};
pub(crate) use triggers::{Bound, BoundHost, Triggers, fire, running, with_bound};

use std::cell::RefCell;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::time::{Duration, SystemTime};

use crate::memory::{Budget, Reservation};
use crate::query::{self, QueryResult, Statements, admin};
use crate::schema::{self, Catalog};
use crate::sql::plan::KeptPlans;
use crate::sql::{self, Effect, Plan};
use crate::stage;
use crate::store::{ChangeSet, Create, Read, Store, View, WrapStorage};
use crate::system::{self, Clock, locked};
use crate::types::{Error, Maintenance, MaintenanceReport, Result, TxId, Value};
use crate::vector::{self, Tally};
pub use plugin::{CommitSource, CorePlugin, DatabasePlugin, PluginHealth, QueryOutcome};

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
    /// `SET MEMORY_LIMIT` or [`crate::Database::set_memory_limit`]. If never declared, it takes a
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

/// The options a writable open of the store folder reads.
impl<'a> From<&'a DatabaseOpenOptions> for crate::store::WriteOpenOptions<'a> {
    fn from(options: &'a DatabaseOpenOptions) -> Self {
        Self {
            create: options.create,
            clock: options.clock.as_ref(),
            deadline: options.deadline,
            cancel: options.cancel.as_deref(),
            storage: options.storage.as_ref(),
        }
    }
}

/// Work performed since this store was opened; available only to an unnarrowed handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct WorkDone {
    /// Rows read, including maintenance's sealed or packed vectors.
    pub rows_read: u64,
    /// Full-precision vector distance evaluations, and maintenance's slot-to-centroid ones.
    pub vectors_compared: u64,
}

/// One store held for writing. A call holds it for as long as the call runs, so the folder is
/// let go when the handle has let go and the last running call has returned.
pub(crate) struct Open {
    /// The first field, so that the close hook runs before the store is let go.
    closes: Closes,
    this: Weak<Open>,
    pub(crate) last_cycle: Mutex<Option<MaintenanceReport>>,
    plans: KeptPlans,
    pub(crate) statements: Arc<Statements>,
    /// The content check `SHOW VERIFY` runs, given by the layer that owns it.
    verify: query::Verify,
    plugin: Option<Arc<dyn DatabasePlugin>>,
    subscribers: observers::Subscribers,
    pub(crate) deliveries: crate::events::Deliveries,
    pub(crate) triggers: Triggers,
    path: PathBuf,
    storage: Option<WrapStorage>,
    pub(crate) budget: Budget,
    pub(crate) live: Mutex<vector::Totals>,
    pub(crate) work: Mutex<WorkDone>,
    pub(crate) signal: Arc<system::Signal>,
    pub(crate) maintenance: Maintenance,
    pub(crate) clock: Arc<dyn Clock>,
    /// The trigger history's retention in milliseconds.
    pub(crate) audit_retention: std::sync::atomic::AtomicU64,
    /// The limit this open started under, as `SHOW MEMORY_LIMIT` reports it.
    started_under: Option<u64>,
    disk_started_under: Option<u64>,
    disk: Mutex<Option<u64>>,
    /// The commit lock: held from before a commit looks at the newest moment until it has
    /// replaced it.
    pub(crate) committing: turn::Turn,
    callback_wait: Duration,
    /// What every statement and every transaction begins from. Held to copy it, to replace it
    /// and to read it again after a read that failed, never while a commit goes to the disk.
    pub(crate) newest: Mutex<Core>,
    /// The highest number a transaction was given or a commit carries.
    numbered: Mutex<u64>,
    pub(crate) cancelled: std::sync::atomic::AtomicU64,
    /// The last field, so that the view above is dropped before the folder is let go.
    pub(crate) store: Store,
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
    pub(crate) source: CommitSource,
    /// How much of `changes.rows_read` the handle's total work already has.
    pub(crate) counted: u64,
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
    pub(crate) fn close(mut self) -> Result<()> {
        self.closes.0 = None;
        let hook = self.host(|plugin| plugin.on_close());
        let store = { self }.store;
        store.close().and(hook)
    }
}

impl Open {
    pub(crate) fn plans(&self) -> &KeptPlans {
        &self.plans
    }

    pub(crate) fn subscribers(&self) -> &Mutex<observers::Sent> {
        &self.subscribers.0
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn storage(&self) -> Option<&WrapStorage> {
        self.storage.as_ref()
    }

    pub(crate) fn disk(&self) -> &Mutex<Option<u64>> {
        &self.disk
    }

    /// Opens the store in `path` for writing under `options`, with `verify` as its content
    /// check; its close hook runs first on every later let-go.
    pub(crate) fn start(
        path: &Path,
        options: DatabaseOpenOptions,
        verify: query::Verify,
    ) -> Result<Arc<Open>> {
        // The opening option is the session's ceiling, and the only one.
        let ceiling = options.memory_limit.map(|l| l as u64);
        let file = std::fs::metadata(path.join("data")).map_or(0, |data| data.len());
        let cache = crate::store::cache_under(ceiling, None);
        crate::memory::admit(ceiling, crate::memory::account(ceiling, cache, Some(file)))?;
        let mut cache_limit = ceiling;
        let budget = Budget::new(ceiling)?;
        let store = Store::open_for_writing(path, &budget, &options)?;
        let view = store.view_on(budget.standing(), false)?;
        let settings = crate::settings::load(|at| view.setting(at))?;
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
            plans,
            statements: Arc::new(Statements::new(&budget, settings[6])),
            verify,
            plugin: options.plugin,
            subscribers: Default::default(),
            triggers: Triggers::new(&budget),
            deliveries: crate::events::Deliveries::default(),
            path: path.into(),
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
            committing: turn::Turn(Mutex::new(false), Default::default()),
            callback_wait: options.callback_wait.unwrap_or(Duration::from_secs(60)),
            audit_retention: (7 * 86_400_000).into(),
            newest: Mutex::new(newest),
            store,
        });
        drop(open.turn()?);
        if open.store.recovery_state()?.0.is_some() {
            open.signal.wake();
        }
        Ok(open)
    }
}
