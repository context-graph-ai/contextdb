//! What an open store does for its handles.

use super::*;

impl Open {
    pub(crate) fn cancels(&self, add: u64) -> u64 {
        self.cancelled.fetch_add(add, Relaxed)
    }
    /// A view of the newest commit and the tables declared as of it.
    pub(crate) fn newest(&self) -> Result<(View, Arc<Catalog>)> {
        self.store.valid()?;
        locked(&self.newest).newest(&self.store, &self.budget)
    }

    /// An empty transaction over the newest commit.
    pub(crate) fn begin(&self) -> Result<Staged> {
        let (view, catalog) = self.newest()?;
        let budget = self.budget.statement();
        Ok(Staged {
            erasing: None,
            passes: None,
            changes: ChangeSet::new(view, &budget)?,
            catalog,
            declared: Vec::new(),
            partitions: Tally::new(&budget)?,
            audit: (Default::default(), budget.reserve(0, "trigger audit")?),
            budget,
            number: TxId(0),
            source: CommitSource::AutoCommit,
            counted: 0,
            reader: query::Reader::admin(),
        })
    }

    /// Starts every host-write transaction through the same number allocator.
    pub(super) fn writing_turn(&self) -> Result<commit::Held<'_>> {
        loop {
            let held = self.turn()?;
            let Some((current_bytes, budget_limit_bytes)) = self.store.recovery_wait()? else {
                return Ok(held);
            };
            if self.maintenance == Maintenance::Caller {
                return Err(Error::DiskLimitExceeded {
                    operation: "write",
                    current_bytes,
                    budget_limit_bytes,
                    hint: "free space is being found; run maintenance",
                });
            }
            let generation = self.signal.generation();
            drop(held);
            self.signal.wake();
            if !self.signal.wait_progress(generation) {
                return Err(Error::DatabaseClosed);
            }
        }
    }

    pub(crate) fn transaction_stage(&self) -> Result<Staged> {
        let mut staged = self.begin()?;
        staged.number = self.number();
        Ok(staged)
    }

    /// The next transaction's number: one past every number given out and every commit.
    pub(super) fn number(&self) -> TxId {
        let mut last = locked(&self.numbered);
        *last += 1;
        TxId(*last)
    }

    /// Runs one planned statement inside a transaction. A refusal may leave part of the
    /// statement staged: the caller rewinds the change set or drops it.
    pub(super) fn run(
        &self,
        plan: Plan,
        (text, parsing, form): (&str, Reservation, Option<Arc<str>>),
        params: &HashMap<String, Value>,
        staged: &mut Staged,
        reader: &query::Reader,
        generation: u64,
    ) -> Result<QueryResult> {
        self.recorded(form.as_deref(), generation, |counted| {
            let before = staged.changes.rows_read.get();
            let result = self.statement(plan, (text, parsing), params, staged, reader, counted);
            staged.changes.end_statement();
            let delta = staged.changes.rows_read.get() - before;
            staged.counted += delta;
            (result, delta)
        })
    }

    /// Runs a statement of the writer under no read limit, and adds its run and work to the record.
    pub(super) fn recorded(
        &self,
        form: Option<&str>,
        generation: u64,
        run: impl FnOnce(&query::Work) -> (Result<QueryResult>, u64),
    ) -> Result<QueryResult> {
        let limits = crate::read::ReadLimits {
            result_rows: u64::MAX,
            result_bytes: u64::MAX,
            work: u64::MAX,
            active_ms: u64::MAX,
            ..Default::default()
        };
        let mut counted =
            query::Work::new(&limits, &*self.clock, &self.cancelled, None).since(generation);
        counted.checkpoint(false)?;
        counted.statements = Some(&self.statements);
        let (result, rows_read) = run(&counted);
        let affected = result.as_ref().map_or(0, QueryResult::rows_affected);
        self.statements.ran(form, &counted, affected);
        let mut work = locked(&self.work);
        work.rows_read += rows_read;
        work.vectors_compared += counted.evaluations.get();
        result
    }

    fn statement(
        &self,
        mut plan: Plan,
        parsed: (&str, Reservation),
        params: &HashMap<String, Value>,
        staged: &mut Staged,
        reader: &query::Reader,
        counted: &query::Work<'_>,
    ) -> Result<QueryResult> {
        if matches!(plan, Plan::Purge(_)) {
            return Err(Error::PurgeRequiresStandaloneExecution);
        }
        let (text, mut parsing) = parsed;
        let parse_bytes = parsing.bytes();
        parsing.grow(sql::planning(text))?;
        // One time for the whole statement.
        let now = system::now_millis(&*self.clock);
        admin(reader, plan.admin_only())?;
        if !matches!(plan.effect(), Effect::Reads) {
            staged
                .budget
                .reset_peak(self.budget.used().saturating_sub(staged.budget.used()));
        }
        let result = match plan.effect() {
            Effect::Declares => {
                if staged.declared.is_empty() && !staged.changes.is_empty() {
                    return Err(Error::MixedSchemaAndRowWrite);
                }
                schema::named(&mut plan, &staged.catalog)?;
                let (catalog, dropped) = Self::declare(
                    &plan,
                    &staged.catalog,
                    &mut staged.changes,
                    &staged.budget,
                    now,
                )?;
                staged.catalog = Arc::new(catalog);
                staged
                    .declared
                    .push((plan.clone(), text.into(), parsing, now));
                if dropped.is_empty() {
                    return Ok(QueryResult::affected(0, 0).measured(&staged.budget, reader));
                }
                let mut held = staged.budget.reserve(0, "dropped indexes answer")?;
                let mut rows = Vec::new();
                for index in dropped {
                    let row = vec![Value::Text(index)];
                    query::result_row(&row, Some(&mut held), None)?;
                    held.room(&mut rows, usize::MAX)?;
                    rows.push(row);
                }
                let columns = vec!["dropped_indexes".to_owned()];
                Ok(QueryResult::answer(columns, rows, held))
            }
            Effect::WritesRows => {
                if !staged.declared.is_empty() {
                    return Err(Error::MixedSchemaAndRowWrite);
                }
                let (catalog, changes) = (&staged.catalog, &mut staged.changes);
                let partitions = &mut staged.partitions;
                let kept = query::Kept::new(staged.budget.passing("expression evaluation")?);
                let passes = std::cell::RefCell::new(&mut staged.passes);
                let part = |changes: &mut ChangeSet| {
                    self.part(changes, catalog, &staged.budget, &mut passes.borrow_mut())
                };
                // A callback's statement is staged in memory, over the rows its commit wrote.
                let part: write::Part = &part;
                let given = write::Given {
                    part: (!self.triggers.firing_here()).then_some(part),
                    params,
                    catalog,
                    budget: &staged.budget,
                    kept: &kept,
                    reader,
                    now,
                    number: staged.number,
                    examined: Some((&counted.examined, Some(counted))),
                    triggers: Some(&self.triggers),
                };
                write::apply(&plan, &given, changes, partitions)
                    .map(|rows| QueryResult::affected(rows, counted.examined.get()))
                    .map(|done| done.evaluated(counted.evaluations.get()))
            }
            Effect::Reads => {
                let (catalog, changes) = (&staged.catalog, &staged.changes);
                let charge = (parsing.bytes(), parse_bytes);
                query::run(
                    &plan,
                    params,
                    catalog,
                    changes,
                    reader,
                    &self.budget,
                    (now, Some(counted), Some(self), charge),
                )
            }
            // A setting is not part of the transaction: it takes hold at once, in a commit of
            // its own, and a rollback does not undo it.
            Effect::Sets(at, n) => {
                drop(plan);
                drop(parsing);
                return self
                    .set(at, n)
                    .map(|()| QueryResult::affected(0, 0).measured(&staged.budget, reader));
            }
        };
        result.map(|result| match plan.effect() {
            Effect::Reads => result,
            _ => result.measured(&staged.budget, reader),
        })
    }

    /// Stages one declaration, with the entries of each index and the records of each kept
    /// count it makes for the rows already there. Returns the tables as they will be, and the
    /// names of the indexes a `DROP COLUMN ... CASCADE` dropped.
    fn declare(
        plan: &Plan,
        catalog: &Catalog,
        changes: &mut ChangeSet,
        budget: &Budget,
        now: i64,
    ) -> Result<(Catalog, Vec<String>)> {
        let declared = schema::declare(plan, catalog, changes, now, budget)?;
        let mut catalog = declared.catalog;
        catalog.fit()?;
        // What a declaration made is on its own table, which is filled.
        if let Some(table) = schema::subject(plan).and_then(|name| catalog.table(name)) {
            write::fill_kept_count(&declared.counts, table, &catalog, changes, budget)?;
            let mut made = declared.indexes;
            if let Plan::CreateIndex(_) = plan {
                made.push(table.index_ids[table.meta.indexes.len() - 1]);
            }
            for index in made {
                write::fill_index(table, index, &catalog, changes, budget)?;
            }
        }
        Ok((catalog, declared.dropped))
    }

    /// The one commit function: everything that changes the store is written here, one commit
    /// at a time. `_held` is the commit lock.
    ///
    /// A transaction that began before the newest commit was overtaken. One that declared runs
    /// its declarations again over the newest moment, because a table's and an index's number
    /// are counted on from the catalog and a new index is filled from the rows that are there;
    /// a refusal of that second run is the commit's refusal. One that wrote rows is laid over
    /// the newest moment and `write` checks its rows against it. Every row guard it took is
    /// checked there too, whatever else it staged. Then the vector part stages
    /// what the rows mean for it, so a row and its place in a search commit together. The
    /// commit carries the transaction's number, or the next after the newest commit when that
    /// is higher.
    pub(super) fn rebase(&self, staged: &mut Staged) -> Result<Option<View>> {
        let (changes, catalog, declared, budget) = (
            &mut staged.changes,
            &mut staged.catalog,
            &staged.declared,
            &staged.budget,
        );
        let (view, tables) = self.newest()?;
        let mut overtaken = None;
        if changes.commit() != view.commit() {
            // What the declarations staged is taken back, to be staged again below. A rewind
            // leaves the row guards.
            if !declared.is_empty() {
                changes.rewind(0);
            }
            let before = changes.rebase(view);
            // The rows were staged with the index entries of the tables as they were declared
            // then, and a guard holds the cells of its row as they lay then; an index or a
            // column declared since would leave either wrong.
            if !(changes.is_empty() && changes.guards().is_empty())
                && before.schema_commit()? != changes.base().schema_commit()?
            {
                return Err(Error::SchemaInvalid {
                    reason: "a table or an index was declared while this transaction was \
                             open, so nothing of it was committed; run it again"
                        .to_owned(),
                });
            }
            *catalog = tables;
            for (plan, _text, _held, now) in declared {
                *catalog = Arc::new(Self::declare(plan, catalog, changes, budget, *now)?.0);
            }
            write::check_provenance(changes, &before, budget)?;
            overtaken = Some(before);
        }
        Ok(overtaken)
    }

    pub(crate) fn settings(&self) -> Result<(View, [Option<u64>; 3])> {
        let view = self.newest()?.0;
        let disk = *locked(&self.disk);
        Ok((view, [self.started_under, self.disk_started_under, disk]))
    }

    /// Every setter validates, stages and commits its row; a refused commit leaves live values intact.
    pub(super) fn set(&self, at: usize, n: u64) -> Result<()> {
        let held = self.turn()?;
        let mut staged = self.begin()?;
        crate::settings::check(at, n, &crate::settings::load(&staged.changes)?)?;
        let over = |c: u64| crate::settings::applied(0, n).is_none_or(|limit| limit > c);
        if let Some(ceiling) = self.started_under.filter(|c| at == 0 && over(*c)) {
            let refused =
                format!("SET MEMORY_LIMIT is above this session's ceiling of {ceiling} bytes");
            return Err(Error::PlanError(refused));
        }
        let before = self.budget.limit();
        if at == 0 {
            let limit = crate::settings::applied(0, n);
            self.store.count_cache_under(limit)?;
            self.budget.set_limit(limit)?;
        }
        let committed = (|| {
            staged.changes.put_setting(at, n)?;
            self.commit(held, staged)
        })();
        if at == 0 && committed.is_err() {
            self.budget.restore_limit(before);
        }
        committed?;
        if at == 6 {
            self.statements
                .set_limit(crate::settings::applied(6, n).unwrap_or(0));
        }
        self.signal.wake();
        Ok(())
    }
}
