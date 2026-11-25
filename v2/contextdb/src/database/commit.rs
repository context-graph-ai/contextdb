//! The one writer turn, commit and publication.
use super::*;
use crate::events::AuditQueue;

pub(crate) struct Turn(pub(crate) Mutex<bool>, pub(crate) std::sync::Condvar);
pub(crate) struct Held<'a>(&'a Turn);
impl Drop for Held<'_> {
    fn drop(&mut self) {
        *locked(&self.0.0) = false;
        self.0.1.notify_one();
    }
}
impl Turn {
    pub(crate) fn take<'a>(&'a self, open: &Open) -> Result<Held<'a>> {
        let (mut taken, mut first) = (locked(&self.0), None);
        while *taken {
            let Some((callback, schedule)) = open.triggers.running_elsewhere() else {
                first = None;
                taken = self.1.wait(taken).unwrap_or_else(PoisonError::into_inner);
                continue;
            };
            if schedule {
                let operation = format!("a write behind schedule {callback}");
                return Err(Error::CallbackReentry { operation });
            }
            let now = system::now_millis(&*open.clock);
            let waited = Duration::from_millis((now - *first.get_or_insert(now)).max(0) as u64);
            let Some(left) = open.callback_wait.checked_sub(waited) else {
                let waited = waited.as_millis();
                let operation = format!("a write behind callback {callback} for {waited} ms");
                return Err(Error::CallbackActive { operation });
            };
            (taken, _) = self
                .1
                .wait_timeout(taken, left)
                .unwrap_or_else(PoisonError::into_inner);
        }
        *taken = true;
        Ok(Held(self))
    }
}
impl Open {
    /// Makes a commit the newest; one with a catalog of its own drops its dropped columns' totals.
    pub(crate) fn adopt(&self, commit: TxId, catalog: Arc<Catalog>, totals: vector::Totals) {
        let mut last = locked(&self.numbered);
        *last = (*last).max(commit.0);
        drop(last);
        let declared = !Arc::ptr_eq(&locked(&self.newest).catalog, &catalog);
        locked(&self.live).merge(totals, declared.then_some(&*catalog));
        *locked(&self.newest) = Core {
            commit,
            catalog,
            view: None,
        };
    }

    pub(crate) fn publish(&self, _held: &Held<'_>) -> Result<TxId> {
        let (cutoff, catalog) = self.newest()?;
        if !cutoff.publication_owed()? {
            return Ok(cutoff.commit());
        }
        self.store.cut_off(Some(&cutoff))?;
        let mut empty = ChangeSet::new(cutoff, &self.budget)?;
        empty.mark_publication(false)?;
        let after = self.store.commit(&mut empty)?;
        self.adopt(after, catalog, vector::Totals::default());
        Ok(after)
    }

    pub(crate) fn turn(&self) -> Result<Held<'_>> {
        let held = self.committing.take(self)?;
        self.publish(&held)?;
        Ok(held)
    }

    pub(crate) fn commit(&self, held: Held<'_>, mut staged: Staged) -> Result<()> {
        let source = staged.source;
        // A set written through ends its part before its triggers read its notes, and again
        // after, so that the rows the callbacks wrote leave theirs before the rules are checked.
        let part = |staged: &mut Staged| {
            let (changes, passes) = (&mut staged.changes, &mut staged.passes);
            self.part(changes, &staged.catalog, &staged.budget, passes)
        };
        let overtaken = self
            .rebase(&mut staged)
            .and_then(|overtaken| part(&mut staged).map(|()| overtaken))
            .inspect_err(|error| self.failed(&staged.changes, source, error))?;
        let (mut staged, fired) =
            crate::events::fire(&self.this.upgrade().expect("active open store"), staged);
        if let Err(error) = fired.and_then(|()| part(&mut staged)) {
            self.failed(&staged.changes, source, &error);
            // The rest of the write goes first, its storage transaction and memory with it.
            let audit = { staged }.audit;
            return Err(self.rolled_back(&held, audit, error)?);
        }
        let Staged {
            audit,
            budget,
            mut changes,
            mut catalog,
            declared,
            number,
            erasing,
            mut passes,
            counted,
            ..
        } = staged;
        let before = (self.budget.limit(), *locked(&self.disk));
        let result = (|| {
            // A PURGE that matches no row still takes what is owed.
            let purge = erasing == Some(true);
            if changes.is_empty() && !(purge && crate::maintenance::any_owed(self)?) {
                return Ok(None);
            }
            self.host(|plugin| plugin.pre_commit(&changes, source))?;
            write::check_at_commit(&changes, &catalog, overtaken.as_ref(), &budget, |each| {
                if changes.streaming() {
                    write::written(&changes, &catalog, &budget, each)
                } else {
                    changes.rows_changed().try_for_each(each)
                }
            })?;
            let now =
                (system::now_millis(&*self.clock) / 1000).clamp(0, i64::from(u32::MAX)) as u32;
            if !declared.is_empty() {
                if Arc::get_mut(&mut catalog).is_none() {
                    catalog = Arc::new(catalog.next(&budget)?);
                }
                let own = Arc::get_mut(&mut catalog).expect("the transaction's own catalog");
                let plans = declared.iter().map(|(plan, ..)| plan);
                own.stamp_retention(plans, &mut changes, now, &budget)?;
            }
            changes.set_number(number);
            self.passes(&mut changes, &catalog, &budget, &mut passes)?;
            let vectors = passes.take().unwrap();
            let deliveries =
                erasing != Some(true) && crate::events::prepare(&mut changes, &catalog, &budget)?;
            if erasing == Some(true) {
                changes.purging = true;
                self.store.cut_off(None)?;
                vector::erase(&mut changes, &catalog, &budget)?;
                crate::events::erase(&mut changes, &budget)?;
                changes.mark_publication(true)?;
            }
            let mut event = self.summary(&mut changes, &catalog, source);
            if let Some(n) = changes.changed_setting(0) {
                self.budget.set_limit(crate::settings::applied(0, n))?;
            }
            // A lower disk limit holds for the commit that records it: setting it never grows the
            // file past it
            let lower = changes
                .changed_setting(1)
                .and_then(|n| crate::settings::applied(1, n));
            if let Some(n) = lower.filter(|n| before.1.is_none_or(|b| *n < b)) {
                self.store.disk_limit(Some(n));
            }
            // Only a declaration erases a trigger.
            let retire = declared.first().map(|_| self.triggers.retiring(&changes));
            let retire = retire.transpose()?;
            changes.put_audit(&audit, crate::system::now_millis(&*self.clock))?;
            let commit = self.store.commit(&mut changes)?;
            if let Some(n) = changes.changed_setting(1) {
                self.store.disk_limit(crate::settings::applied(1, n));
                *locked(&self.disk) = crate::settings::applied(1, n);
            }
            retire.into_iter().for_each(|retire| retire());
            self.adopt(commit, catalog, vectors.totals);
            if let Some(event) = &mut event {
                event.lsn = commit;
            }
            Ok(Some((event, vectors.wake || deliveries)))
        })();
        locked(&self.work).rows_read += changes.rows_read.get() - counted;
        let event = match result {
            Ok(event) => event,
            Err(error) => {
                self.budget.restore_limit(before.0);
                self.store.disk_limit(before.1);
                *locked(&self.disk) = before.1;
                self.failed(&changes, source, &error);
                drop((changes, passes, declared));
                return Err(self.rolled_back(&held, audit, error)?);
            }
        };
        let tail = if event.is_some() && erasing == Some(true) {
            self.publish(&held).map(|_| ())
        } else {
            Ok(())
        };
        drop(held);
        if let Some((event, wake)) = event {
            self.confirmed(&changes, source, event);
            if wake {
                self.signal.wake();
            }
        }
        tail
    }

    /// Stores a refused write's firings, rolled back, in a commit of their own under the turn
    /// it still holds, before its refusal returns; a refusal of that commit is the caller's.
    fn rolled_back(&self, _held: &Held<'_>, mut audit: AuditQueue, error: Error) -> Result<Error> {
        if !audit.0.0.is_empty() {
            crate::events::rolled_back(&mut audit, &error)?;
            let (view, catalog) = self.newest()?;
            let mut changes = ChangeSet::new(view, &self.budget)?;
            // Written through, a long write's firings leave in parts as the set fills.
            self.store.through(&mut changes);
            changes.set_number(self.number());
            changes.put_audit(&audit, crate::system::now_millis(&*self.clock))?;
            let after = self.store.commit(&mut changes)?;
            self.adopt(after, catalog, vector::Totals::default());
        }
        Ok(error)
    }

    /// Ends a part of a change set written through: the passes run over the rows it staged,
    /// then the rows are written, each leaving its note. A set with no row staged ends none.
    pub(crate) fn part(
        &self,
        changes: &mut ChangeSet,
        catalog: &Catalog,
        budget: &Budget,
        passes: &mut Option<vector::Committed>,
    ) -> Result<()> {
        if !changes.streaming() || changes.rows_changed().next().is_none() {
            return Ok(());
        }
        self.passes(changes, catalog, budget, passes)?;
        changes.put_part(true)
    }

    /// The same derived passes run at the end of each part and once at finish.
    pub(crate) fn passes(
        &self,
        changes: &mut ChangeSet,
        catalog: &Catalog,
        budget: &Budget,
        state: &mut Option<vector::Committed>,
    ) -> Result<()> {
        let now = (system::now_millis(&*self.clock) / 1000).clamp(0, i64::from(u32::MAX)) as u32;
        write::stage_retention(changes, catalog, now, budget)?;
        let live = locked(&self.live);
        // A later part of the transaction counts on from the totals of its earlier parts.
        let (earlier, wake) = state.take().map_or((None, false), |v| {
            let mut totals = live.clone();
            totals.merge(v.totals, None);
            (Some(totals), v.wake)
        });
        let mut vectors =
            vector::track_unindexed(changes, catalog, budget, earlier.as_ref().unwrap_or(&live))?;
        if let Some(mut earlier) = earlier {
            earlier.merge(std::mem::take(&mut vectors.totals), None);
            vectors.totals = earlier;
        }
        vectors.wake |= wake;
        *state = Some(vectors);
        Ok(())
    }
}
