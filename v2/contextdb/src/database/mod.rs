//! database: the handle a store is opened for writing with, its transactions, and the one
//! commit function.
//!
//! It never has a second way to commit: a statement run on its own, a transaction and a change
//! of the memory limit each build a change set and hand it to the same function.

#[cfg(test)]
mod tests;

use std::cell::RefCell;
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use crate::memory::Budget;
use crate::query::{self, QueryResult};
use crate::schema::{self, Catalog};
use crate::sql::{self, Effect, Plan};
use crate::store::{ChangeSet, Read, Store, View, WrapStorage};
use crate::system::{self, Clock};
use crate::types::{Error, Result, TxId, Value};
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
    /// `SET MEMORY_LIMIT` or [`Database::set_memory_limit`], and means no limit when none was
    /// ever declared. A limit given here holds until the store is closed and leaves what the
    /// store remembers as it is.
    ///
    /// Every open sets 9 MiB aside out of the limit, 8 MiB of it for the store's page cache. A
    /// smaller limit is refused with [`crate::Error::MemoryBudgetExceeded`] and opens no store.
    ///
    /// A [`crate::ReadSession`] never takes the limit the store remembers: `None` in its own
    /// options means no limit at all, so a reader that needs a ceiling is given one there.
    pub memory_limit: Option<usize>,
    /// Where the time comes from for `NOW()` and `DEFAULT NOW()`. `None`, the default, is the
    /// machine's clock.
    pub clock: Option<Arc<dyn Clock>>,
    /// A wrapper around the store's data file: the store opens the file, hands it to this, and
    /// reads, writes and syncs it only through what this returns. `None`, the default, uses the
    /// file as it is.
    pub storage: Option<WrapStorage>,
}

/// One store folder, held for writing.
///
/// One handle holds a store for writing at a time. A second [`Database::open`] of the same
/// folder is refused with [`crate::Error::DatabaseLocked`], from another process and from this
/// one alike. Any number of [`crate::ReadSession`]s, in this process or others, read the store
/// meanwhile.
///
/// A handle can be shared between threads, and its calls block until they are done.
///
/// Dropping the handle lets go of the store as [`Database::close`] does. Every commit that
/// returned is already on disk, so neither writes anything.
pub struct Database {
    /// The open store, `None` once [`Database::close`] has let go of it.
    open: Mutex<Option<Arc<Open>>>,
}

/// One store held for writing. A call holds it for as long as the call runs, so the folder is
/// let go when the handle has let go and the last running call has returned.
struct Open {
    budget: Budget,
    clock: Arc<dyn Clock>,
    /// The limit this open started under, as `SHOW MEMORY_LIMIT` reports it.
    started_under: Option<u64>,
    /// The commit lock: held from before a commit looks at the newest moment until it has
    /// replaced it.
    committing: Mutex<()>,
    /// What every statement and every transaction begins from. Held to copy it, to replace it
    /// and to read it again after a read that failed, never while a commit goes to the disk.
    newest: Mutex<Newest>,
    /// The last field, so that the view above is dropped before the folder is let go.
    store: Store,
}

/// The newest commit.
struct Newest {
    commit: TxId,
    /// The tables declared as of it.
    catalog: Arc<Catalog>,
    /// A view of it. `None` when the read that follows a commit failed: the next call that
    /// needs a view reads again, so that nothing is answered from before that commit.
    view: Option<View>,
}

/// What one transaction has staged.
struct Staged {
    changes: ChangeSet,
    /// The tables as the transaction sees them: those of the moment it began at and those it
    /// has declared since.
    catalog: Arc<Catalog>,
    /// The declarations it ran, in order, for a commit that has to run them again.
    declared: Vec<Plan>,
}

fn planned(sql: &str) -> Result<Plan> {
    sql::plan(&sql::parse(sql)?)
}

/// Takes a lock. None of this part's locks is held across a step that can leave what the lock
/// guards half changed, so a holder that panicked left it whole.
fn locked<T>(lock: &Mutex<T>) -> MutexGuard<'_, T> {
    lock.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Open {
    /// A view of the newest commit and the tables declared as of it.
    fn newest(&self) -> Result<(View, Arc<Catalog>)> {
        let mut newest = locked(&self.newest);
        if let Some(view) = &newest.view {
            return Ok((view.clone(), newest.catalog.clone()));
        }
        // The tables are read from the view itself, so the two are of one commit even when
        // another commit lands in this instant.
        let view = self.store.view()?;
        let catalog = Arc::new(Catalog::load(&view)?);
        *newest = Newest {
            commit: view.commit(),
            catalog: catalog.clone(),
            view: Some(view.clone()),
        };
        Ok((view, catalog))
    }

    /// An empty transaction over the newest commit.
    fn begin(&self) -> Result<Staged> {
        let (view, catalog) = self.newest()?;
        Ok(Staged {
            changes: ChangeSet::new(view, &self.budget)?,
            catalog,
            declared: Vec::new(),
        })
    }

    /// Runs one planned statement inside a transaction. A refusal may leave part of the
    /// statement staged: the caller rewinds the change set or drops it.
    fn run(
        &self,
        plan: &Plan,
        params: &HashMap<String, Value>,
        staged: &mut Staged,
    ) -> Result<QueryResult> {
        // One time for the whole statement.
        let now = system::now_millis(&*self.clock);
        match plan.effect() {
            Effect::Declares => {
                if staged.declared.is_empty() && !staged.changes.is_empty() {
                    return Err(Error::MixedSchemaAndRowWrite);
                }
                let catalog = self.declare(plan, &staged.catalog, &mut staged.changes)?;
                staged.catalog = Arc::new(catalog);
                staged.declared.push(plan.clone());
                Ok(QueryResult::affected(0))
            }
            Effect::WritesRows => {
                if !staged.declared.is_empty() {
                    return Err(Error::MixedSchemaAndRowWrite);
                }
                let (catalog, changes) = (&staged.catalog, &mut staged.changes);
                write::apply(plan, params, catalog, changes, &self.budget, now)
                    .map(QueryResult::affected)
            }
            Effect::Reads => {
                if let Plan::ShowMemoryLimit = plan {
                    return Ok(query::memory_limit_shown(&self.budget, self.started_under));
                }
                let (catalog, changes) = (&staged.catalog, &staged.changes);
                query::run(plan, params, catalog, changes, &self.budget, now)
            }
            // A setting is not part of the transaction: it takes hold at once, in a commit of
            // its own, and a rollback does not undo it.
            Effect::Sets(limit) => self
                .set_memory_limit(limit)
                .map(|()| QueryResult::affected(0)),
        }
    }

    /// Stages one declaration and, for a new index, its entries for the rows already there.
    fn declare(&self, plan: &Plan, catalog: &Catalog, changes: &mut ChangeSet) -> Result<Catalog> {
        let declared = schema::declare(plan, catalog, changes)?;
        if let Plan::CreateIndex(create) = plan
            && let Some(table) = declared.table(&create.table)
            && let Some(&index) = table.index_ids.last()
        {
            write::fill_index(table, index, changes, &self.budget)?;
        }
        Ok(declared)
    }

    /// The one commit function: everything that changes the store is written here, one commit
    /// at a time. `_held` is the commit lock.
    ///
    /// A transaction that began before the newest commit was overtaken. One that declared runs
    /// its declarations again over the newest moment, because a table's and an index's number
    /// are counted on from the catalog and a new index is filled from the rows that are there;
    /// a refusal of that second run is the commit's refusal. One that wrote rows is laid over
    /// the newest moment and `write` checks its rows against it.
    fn commit(&self, _held: &MutexGuard<'_, ()>, staged: Staged) -> Result<()> {
        let Staged {
            mut changes,
            mut catalog,
            declared,
        } = staged;
        let (view, tables) = self.newest()?;
        if changes.commit() != view.commit() {
            if !declared.is_empty() {
                changes = ChangeSet::new(view, &self.budget)?;
                catalog = tables;
                for plan in &declared {
                    catalog = Arc::new(self.declare(plan, &catalog, &mut changes)?);
                }
            } else if !changes.is_empty() {
                let before = changes.rebase(view);
                // The rows were staged with the index entries of the tables as they were
                // declared then; an index declared since would be left without them.
                if before.schema_commit()? != changes.base().schema_commit()? {
                    return Err(Error::SchemaInvalid {
                        reason: "a table or an index was declared while this transaction was \
                                 open, so nothing of it was committed; run it again"
                            .to_owned(),
                    });
                }
                write::check_at_commit(&changes, &catalog, &before)?;
            }
        }
        if changes.is_empty() {
            return Ok(());
        }
        let commit = self.store.commit(changes)?;
        // The commit is on disk whatever the read after it says. When that read fails no view
        // is kept and the next call reads again, so none answers from before this commit.
        let view = self.store.view().ok();
        *locked(&self.newest) = Newest {
            commit,
            catalog,
            view,
        };
        Ok(())
    }

    /// The one path of `SET MEMORY_LIMIT` and [`Database::set_memory_limit`]: the limit takes
    /// hold in this open and is committed for the store to remember, or neither happens.
    fn set_memory_limit(&self, limit: Option<u64>) -> Result<()> {
        let held = locked(&self.committing);
        let mut staged = self.begin()?;
        staged.changes.put_memory_limit(limit)?;
        let before = self.budget.limit();
        // Refused here, before anything is written, when the limit is below what is in use.
        self.budget.set_limit(limit)?;
        let committed = self.commit(&held, staged);
        if committed.is_err() {
            // The store did not take the limit, so this open goes back to the one it had. That
            // is refused only when another thread has, in this instant, taken more than the old
            // limit allows; the new limit then stands until the store is closed.
            let _ = self.budget.set_limit(before);
        }
        committed
    }
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
    /// [`crate::Error::DatabaseLocked`], at once and without waiting, when another handle holds
    /// the store for writing, in another process or in this one. [`crate::Error::StoreIo`] when
    /// the folder or a file in it cannot be created, read or written, and
    /// [`crate::Error::StoreCorrupted`] when stored data fails its check.
    /// [`crate::Error::MemoryBudgetExceeded`] when the memory limit is too small to open a
    /// store under.
    pub fn open(path: impl AsRef<Path>) -> Result<Database> {
        Database::open_with_options(path, DatabaseOpenOptions::default())
    }

    /// [`Database::open`] with options.
    pub fn open_with_options(
        path: impl AsRef<Path>,
        options: DatabaseOpenOptions,
    ) -> Result<Database> {
        let budget = Budget::new(options.memory_limit.map(|limit| limit as u64))?;
        let store = Store::open_for_writing(path.as_ref(), &budget, options.storage.as_ref())?;
        let view = store.view()?;
        // A limit the options name is this open's ceiling. With none named, the ceiling is the
        // limit the store remembers.
        if options.memory_limit.is_none() {
            budget.set_limit(view.memory_limit()?)?;
        }
        let newest = Newest {
            commit: view.commit(),
            catalog: Arc::new(Catalog::load(&view)?),
            view: Some(view),
        };
        let open = Open {
            started_under: budget.limit(),
            budget,
            clock: options.clock.unwrap_or_else(system::machine_clock),
            committing: Mutex::new(()),
            newest: Mutex::new(newest),
            store,
        };
        Ok(Database {
            open: Mutex::new(Some(Arc::new(open))),
        })
    }

    /// The open store, or the refusal of a handle that was closed.
    fn opened(&self) -> Result<Arc<Open>> {
        locked(&self.open).clone().ok_or(Error::DatabaseClosed)
    }

    /// The open store, for the four calls that return a plain value and so have no way to
    /// refuse. On a closed handle they panic with the refusal's sentence.
    fn must_be_open(&self) -> Arc<Open> {
        self.opened().unwrap_or_else(|closed| panic!("{closed}"))
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
    pub fn execute(&self, sql: &str, params: &HashMap<String, Value>) -> Result<QueryResult> {
        let plan = planned(sql)?;
        let open = self.opened()?;
        if matches!(plan.effect(), Effect::Declares | Effect::WritesRows) {
            // Taken before the statement reads anything, so no other commit can overtake it.
            let held = locked(&open.committing);
            let mut staged = open.begin()?;
            let result = open.run(&plan, params, &mut staged)?;
            open.commit(&held, staged)?;
            Ok(result)
        } else {
            // A statement that stages nothing has nothing to commit.
            open.run(&plan, params, &mut open.begin()?)
        }
    }

    /// Begins a transaction over the newest commit. Nothing it does is visible to anyone else
    /// until it commits. Several transactions of one handle may be open at once.
    ///
    /// # Errors
    ///
    /// [`crate::Error::DatabaseClosed`] on a handle that was closed, and
    /// [`crate::Error::StoreIo`] or [`crate::Error::StoreCorrupted`] when the store cannot be
    /// read.
    pub fn transaction(&self) -> Result<Transaction<'_>> {
        Ok(Transaction {
            staged: RefCell::new(self.opened()?.begin()?),
            db: self,
        })
    }

    /// Declares the memory limit in bytes, or no limit for `None`.
    ///
    /// The limit takes hold at once and is committed, so the store remembers it for every
    /// later open whose options name no limit, exactly as `SET MEMORY_LIMIT` does.
    ///
    /// # Errors
    ///
    /// [`crate::Error::MemoryBudgetExceeded`] when the limit is below what is in use now, which
    /// is never less than the 9 MiB every open sets aside. [`crate::Error::StoreIo`] when the
    /// commit cannot be written. In both cases the limit stays what it was.
    /// [`crate::Error::DatabaseClosed`] on a handle that was closed.
    pub fn set_memory_limit(&self, limit: Option<usize>) -> Result<()> {
        self.opened()?
            .set_memory_limit(limit.map(|limit| limit as u64))
    }

    /// The memory limit in force, in bytes; `None` when there is none.
    ///
    /// # Panics
    ///
    /// On a handle that was closed, with the message "database handle is closed".
    pub fn memory_limit(&self) -> Option<usize> {
        let limit = self.must_be_open().budget.limit();
        limit.map(|limit| limit as usize)
    }

    /// The bytes counted against the memory limit at this moment: what every open sets aside,
    /// and what open transactions and results not yet dropped hold. `SHOW MEMORY_LIMIT` reports
    /// the same number as `used`.
    ///
    /// # Panics
    ///
    /// On a handle that was closed, with the message "database handle is closed".
    pub fn memory_used(&self) -> usize {
        self.must_be_open().budget.used() as usize
    }

    /// The names of the store's tables as of the newest commit, in name order. A new store has
    /// none.
    ///
    /// # Panics
    ///
    /// On a handle that was closed, with the message "database handle is closed".
    pub fn table_names(&self) -> Vec<String> {
        let open = self.must_be_open();
        let newest = locked(&open.newest);
        let tables = newest.catalog.tables().into_iter();
        tables.map(|table| table.name.clone()).collect()
    }

    /// A table's declaration as SQL text, as of the newest commit: the `CREATE TABLE` statement
    /// that would declare the table as it stands, with every clause left at its default not
    /// written, then one `CREATE INDEX` statement for each index that was made with one. Each
    /// statement ends with `;` and a new line.
    ///
    /// # Errors
    ///
    /// [`crate::Error::TableNotFound`] when the store has no table of this name, and
    /// [`crate::Error::DatabaseClosed`] on a handle that was closed. The answer is not read
    /// from the disk, so no other error is returned.
    pub fn declaration(&self, table: &str) -> Result<String> {
        let open = self.opened()?;
        let newest = locked(&open.newest);
        let found = newest.catalog.table(table).map(schema::render);
        found.ok_or_else(|| Error::TableNotFound(table.to_owned()))
    }

    /// The position of the newest commit on disk: 0 for a store nothing was ever committed to,
    /// and one more for every commit since. A transaction that commits nothing does not move
    /// it, and no position is ever reported for work that did not commit.
    ///
    /// The position is for comparing two moments of one store. No call of this library takes
    /// one.
    ///
    /// # Panics
    ///
    /// On a handle that was closed, with the message "database handle is closed".
    pub fn committed_watermark(&self) -> TxId {
        locked(&self.must_be_open().newest).commit
    }

    /// Lets go of the store, so that the next writer can open it. Every commit that returned is
    /// already on disk, and closing writes nothing. While a call is still running on another
    /// thread, the store is let go when that call returns. Closing a handle that was closed
    /// changes nothing.
    ///
    /// Every later call on this handle, and on a transaction that was begun from it, returns
    /// [`crate::Error::DatabaseClosed`]. Four calls return a plain value and have no way to say
    /// so; on a closed handle they panic: [`Database::memory_limit`],
    /// [`Database::memory_used`], [`Database::table_names`] and
    /// [`Database::committed_watermark`].
    pub fn close(&self) -> Result<()> {
        drop(locked(&self.open).take());
        Ok(())
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
    /// `SET MEMORY_LIMIT` is not part of the transaction. It takes hold at once, and a rollback
    /// does not undo it.
    pub fn execute(&self, sql: &str, params: &HashMap<String, Value>) -> Result<QueryResult> {
        let plan = planned(sql)?;
        let open = self.db.opened()?;
        let staged = &mut *self.staged.borrow_mut();
        let mark = staged.changes.mark();
        let result = open.run(&plan, params, staged);
        if result.is_err() {
            staged.changes.rewind(mark);
        }
        result
    }

    /// Commits. On `Ok(())` everything the transaction did is on disk. On `Err` none of it is,
    /// and the transaction is gone: to try again, begin a new one and run its statements again.
    ///
    /// # Errors
    ///
    /// A transaction that was open while another commit landed is checked against that commit:
    ///
    /// - [`crate::Error::UniqueViolation`] when the other commit took a key, or a value of a
    ///   unique column, that this transaction inserted;
    /// - [`crate::Error::ConditionalUpdateConflict`] when the other commit changed or deleted a
    ///   row that this transaction changed or deleted;
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
    pub fn commit(self) -> Result<()> {
        let open = self.db.opened()?;
        let held = locked(&open.committing);
        open.commit(&held, self.staged.into_inner())
    }

    /// Rolls back now. What the transaction staged is held in memory only, so this drops it,
    /// writes nothing and cannot fail. Dropping the transaction does the same.
    pub fn rollback(self) {}
}
