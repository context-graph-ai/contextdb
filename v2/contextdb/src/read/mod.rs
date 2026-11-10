//! read: reading a store without holding it for writing, from the writer's process or any
//! other. A session opens the folder itself, and each statement reads the newest commit under
//! the session's own memory limit.
//!
//! It never writes to the store itself.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex, PoisonError};

use crate::memory::Budget;
use crate::query::{self, QueryResult};
use crate::schema::{self, Catalog};
use crate::sql::{self, Effect, Plan};
use crate::store::{Store, View, WrapStorage};
use crate::system::{self, Clock};
use crate::types::{Error, Result, TxId, Value};

/// What a reader may say when it opens a store.
///
/// Start from `ReadSessionOptions::default()` and assign the fields to change. The struct is
/// `#[non_exhaustive]`, so outside this crate it cannot be written out field by field.
#[derive(Clone, Default)]
#[non_exhaustive]
pub struct ReadSessionOptions {
    /// The most memory, in bytes, the session may use for its page cache and for the results
    /// it has returned and that are still alive. `None`, the default, is no ceiling: a reader
    /// does not take the limit the store remembers for its writer, so a reader that needs a
    /// cap sets it here. Every open sets aside 8 MiB for the page cache and 1 MiB for the
    /// small fixed needs of a statement, so a limit under 9 MiB is refused at open with
    /// [`crate::Error::MemoryBudgetExceeded`].
    pub memory_limit: Option<usize>,
    /// Where the session gets the time that `NOW()` in a statement answers with. `None`, the
    /// default, is the machine's clock.
    pub clock: Option<Arc<dyn Clock>>,
    /// A wrapper around the data file the session has opened; the session then reads the file
    /// only through what the wrapper returns. `None`, the default, reads the file as it is.
    pub storage: Option<WrapStorage>,
}

/// A store opened for reading.
///
/// A session is opened by the folder's path, with [`ReadSession::open`]; no call on
/// [`crate::Database`] returns one. It reads the folder itself and needs nothing from the
/// writer's process: it opens whether or not a writer is running, never makes a writer fail,
/// holds one up only for the instant a call takes to note which commit it reads, and keeps
/// answering when the writer stops, is killed or starts again. It changes nothing a writer
/// stored. While a call runs it keeps one small file under `readers/` in the
/// folder, which is how a writer knows the pages being read are still needed.
///
/// Each call sees the newest commit as of the moment it starts and keeps that one moment until
/// it returns. The next call starts from the newest commit again: no call holds one commit
/// across two statements.
///
/// Which to read with: the process that holds the store for writing reads through its
/// [`crate::Database`], where a transaction's statements also see what the transaction has
/// written. Every other process reads through a `ReadSession`, and so does code that must not
/// be able to write: a session refuses a statement that would change something with
/// [`crate::Error::ReadOnlySession`].
///
/// Calls block and may be made from many threads.
pub struct ReadSession {
    store: Store,
    budget: Budget,
    clock: Arc<dyn Clock>,
    /// The tables as last read, with the schema commit they were read at.
    catalog: Mutex<Option<(TxId, Arc<Catalog>)>>,
}

impl ReadSession {
    /// Opens the store in this folder for reading, with no memory limit and the machine's
    /// clock. Never creates anything.
    ///
    /// # Errors
    ///
    /// - [`crate::Error::StoreMissing`] when there is no store at the path.
    /// - [`crate::Error::StoreIo`] when the folder or its data file cannot be opened or read.
    /// - [`crate::Error::StoreCorrupted`] when the data file fails its check.
    pub fn open(path: impl AsRef<Path>) -> Result<ReadSession> {
        ReadSession::open_with_options(path, ReadSessionOptions::default())
    }

    /// [`ReadSession::open`] with options.
    ///
    /// # Errors
    ///
    /// Those of [`ReadSession::open`], and [`crate::Error::MemoryBudgetExceeded`] when
    /// [`ReadSessionOptions::memory_limit`] is under the 9 MiB every open sets aside.
    pub fn open_with_options(
        path: impl AsRef<Path>,
        options: ReadSessionOptions,
    ) -> Result<ReadSession> {
        let budget = Budget::new(options.memory_limit.map(|bytes| bytes as u64))?;
        let store = Store::open_for_reading(path.as_ref(), &budget, options.storage.as_ref())?;
        Ok(ReadSession {
            store,
            budget,
            clock: options.clock.unwrap_or_else(system::machine_clock),
            catalog: Mutex::new(None),
        })
    }

    /// Runs one statement that reads, `SELECT` or `SHOW MEMORY_LIMIT`, and returns its result.
    ///
    /// `params` gives a value for each `$name` in the statement; [`crate::params!`] writes the
    /// map.
    ///
    /// # Errors
    ///
    /// - [`crate::Error::ReadOnlySession`] when the statement would declare a table or an
    ///   index, write rows or change the memory limit. Nothing ran; run it on a
    ///   [`crate::Database`].
    /// - [`crate::Error::ParseError`] or [`crate::Error::PlanError`] when the text is not a
    ///   statement this version runs, in the words a [`crate::Database`] refuses it with.
    /// - [`crate::Error::TableNotFound`] when the newest commit has no table of that name.
    /// - [`crate::Error::MemoryBudgetExceeded`] when the result would pass the session's
    ///   memory limit. The session stays usable.
    /// - [`crate::Error::StoreIo`] when the folder or its data file cannot be read, and
    ///   [`crate::Error::StoreCorrupted`] when what was read fails its check.
    pub fn execute(&self, sql: &str, params: &HashMap<String, Value>) -> Result<QueryResult> {
        let plan = sql::plan(&sql::parse(sql)?)?;
        if plan.effect() != Effect::Reads {
            return Err(Error::ReadOnlySession);
        }
        if let Plan::ShowMemoryLimit = plan {
            // Nothing changes a session's limit, so the one it has is the one it opened under.
            let opened_under = self.budget.limit();
            return Ok(query::memory_limit_shown(&self.budget, opened_under));
        }
        let now = system::now_millis(&*self.clock);
        let (view, catalog) = self.newest()?;
        query::run(&plan, params, &catalog, &view, &self.budget, now)
    }

    /// The names of the store's tables as of the newest commit, in name order. A store with no
    /// table gives an empty list.
    ///
    /// # Errors
    ///
    /// This call reads the folder, so unlike [`crate::Database::table_names`] it can fail:
    /// [`crate::Error::StoreIo`] when the folder or its data file cannot be read, and
    /// [`crate::Error::StoreCorrupted`] when what was read fails its check.
    pub fn table_names(&self) -> Result<Vec<String>> {
        let (_, catalog) = self.newest()?;
        let tables = catalog.tables().into_iter();
        Ok(tables.map(|table| table.name.clone()).collect())
    }

    /// A table's declaration as SQL text, as of the newest commit: its `CREATE TABLE` statement,
    /// then one `CREATE INDEX` statement for each index made with `CREATE INDEX`, each ended
    /// by `;` and a new line. A clause left at its default is not written. It is the text
    /// [`crate::Database::declaration`] gives, and running it on an empty store declares the
    /// same table.
    ///
    /// # Errors
    ///
    /// - [`crate::Error::TableNotFound`] when the newest commit has no table of this name.
    /// - [`crate::Error::StoreIo`] when the folder or its data file cannot be read, and
    ///   [`crate::Error::StoreCorrupted`] when what was read fails its check.
    pub fn declaration(&self, table: &str) -> Result<String> {
        let (_, catalog) = self.newest()?;
        match catalog.table(table) {
            Some(table) => Ok(schema::render(table)),
            None => Err(Error::TableNotFound(table.to_owned())),
        }
    }

    /// The newest commit and the tables declared at it. The view holds that commit's pages for
    /// as long as the caller keeps it; the tables are read again only when a declaration has
    /// been committed since they were last read.
    fn newest(&self) -> Result<(View, Arc<Catalog>)> {
        let view = self.store.view()?;
        let declared_at = view.schema_commit()?;
        // The lock guards a finished catalog or none, so a holder that panicked left it whole.
        let mut kept = self.catalog.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some((commit, catalog)) = &*kept
            && *commit == declared_at
        {
            return Ok((view, catalog.clone()));
        }
        let catalog = Arc::new(Catalog::load(&view)?);
        *kept = Some((declared_at, catalog.clone()));
        Ok((view, catalog))
    }
}
