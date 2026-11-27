//! Cursor lifetime and admission, sharing the session's query sources and work record.
use super::*;
use std::num::NonZeroUsize;
use std::path::PathBuf;

/// A committed view and its catalog, retained until this snapshot is dropped.
pub struct Snapshot {
    pub(crate) path: PathBuf,
    pub(crate) view: View,
    pub(crate) catalog: Arc<Catalog>,
    // Retains the independent reader backend and its original handle's cache charge.
    pub(crate) _store: Store,
}
impl Snapshot {
    /// The commit retained by this snapshot, even after later commits are published.
    pub fn id(&self) -> TxId {
        self.view.commit()
    }
}
/// One pinned read. Fetches move complete pages out of its charged query state.
pub struct Cursor {
    at: Option<query::CursorRows>,
    /// The session's record, the cursor's form, and its rows, rows examined and distance
    /// evaluations so far: a cursor is one call.
    counted: (Arc<Statements>, Option<Arc<str>>, [u64; 3]),
    owner: Option<Weak<Open>>,
    commit: TxId,
    budget: Budget,
    reader: query::Reader,
    limits: ReadLimits,
    clock: Arc<dyn Clock>,
    stop: Arc<AtomicU64>,
    pages: Option<crate::store::Pages>,
    observer: Option<Arc<dyn ReadProgressObserver>>,
    opened: i64,
    touched: i64,
    end: Option<Option<(&'static str, u64)>>,
}
impl Cursor {
    pub(super) fn open(
        session: &ReadSession,
        sql: &str,
        params: &HashMap<String, Value>,
    ) -> Result<Self> {
        let work = query::Work::new(
            &session.limits,
            &*session.clock,
            &session.stop,
            (session.observer.as_deref(), crate::maintenance::verify),
        );
        let now = system::now_millis(&*session.clock);
        let mut form = None;
        let result = (|| {
            work.checkpoint(false)?;
            let (plan, mut parsing, formed) = session.plans.planned(sql, &session.budget)?;
            form = formed;
            parsing.grow(crate::sql::planning(sql))?;
            session.admit(&plan, None)?;
            let (view, catalog) = Core::load(
                session.store.view_on(&session.budget, true)?,
                &mut locked(&session.catalog),
                &session.budget,
            )?
            .newest(&session.store, &session.budget)?;
            let commit = view.commit();
            Self::publication(&session.owner, commit)?;
            session.admit(&plan, Some(&catalog))?;
            let at = query::CursorRows::open(
                (plan, parsing),
                params,
                sql.len(),
                (
                    view,
                    catalog,
                    session.reader.clone(),
                    session.budget.clone(),
                    now,
                ),
            )?;
            Ok((at, commit))
        })();
        let result = work.finish(result);
        session.statements.ran(form.as_deref(), &work, 0);
        let (at, commit) = result?;
        let touched = system::now_millis(&*session.clock);
        let (due, _) = Self::deadline(&session.limits, now, touched);
        at.view.stamp(Some(Some(due)))?;
        Ok(Self {
            at: Some(at),
            counted: (session.statements.clone(), form, work.counts()),
            owner: session.owner.clone(),
            commit,
            budget: session.budget.statement(),
            reader: session.reader.clone(),
            limits: session.limits.clone(),
            clock: session.clock.clone(),
            stop: session.stop.clone(),
            pages: session.store.pages(),
            observer: session.observer.clone(),
            opened: now,
            touched,
            end: None,
        })
    }
    fn deadline(limits: &ReadLimits, opened: i64, touched: i64) -> (i64, (&'static str, u64)) {
        let at = |from: i64, field, limit: u64| {
            let due = i64::try_from(limit).ok().and_then(|l| from.checked_add(l));
            (due.unwrap_or(i64::MAX), (field, limit))
        };
        let idle = at(touched, "cursor_idle_ms", limits.cursor_idle_ms);
        idle.min(at(opened, "cursor_lifetime_ms", limits.cursor_lifetime_ms))
    }
    /// Whether scanning may continue; a full page can be followed by an empty page.
    pub fn has_more(&self) -> bool {
        self.at.is_some()
    }
    /// Fetches a page. An excessive row request preserves position; other refusals end it.
    pub fn fetch(&mut self, rows: Option<NonZeroUsize>) -> Result<QueryResult> {
        if let Some(Some((field, limit))) = self.end {
            return Err(Error::ReadLimitExceeded { field, limit });
        }
        let rows = rows.map_or(self.limits.cursor_page_rows, |rows| rows.get() as u64);
        query::limit("result_rows", rows, self.limits.result_rows)?;
        if self.end.is_some() {
            return Err(Error::CursorClosed);
        }
        if self.at.is_none() {
            Self::publication(&self.owner, self.commit)?;
            return Ok(QueryResult::affected(0, 0).measured(&self.budget, &self.reader));
        }
        let now = system::now_millis(&*self.clock);
        let work = query::Work::new(
            &self.limits,
            &*self.clock,
            &self.stop,
            (self.observer.as_deref(), crate::maintenance::verify),
        )
        .paged(self.pages.clone());
        let result = (|| {
            Self::publication(&self.owner, self.commit)?;
            query::limit(
                "cursor_idle_ms",
                now.saturating_sub(self.touched).max(0) as u64,
                self.limits.cursor_idle_ms,
            )?;
            query::limit(
                "cursor_lifetime_ms",
                now.saturating_sub(self.opened).max(0) as u64,
                self.limits.cursor_lifetime_ms,
            )?;
            if !self.at.as_ref().expect("cursor").view.stamp(Some(None))? {
                let (_, (field, limit)) = Self::deadline(&self.limits, self.opened, self.touched);
                return Err(Error::ReadLimitExceeded { field, limit });
            }
            work.checkpoint(false)?;
            self.at.as_mut().expect("cursor continuation").page(
                rows,
                self.limits.cursor_page_bytes,
                &work,
            )
        })();
        // An erasure is refused first; then the file holds the next deadline, or none at the end.
        let result = work.finish(result);
        self.touched = system::now_millis(&*self.clock);
        let (due, _) = Self::deadline(&self.limits, self.opened, self.touched);
        let at = self.at.as_ref().expect("cursor continuation");
        let next = (result.is_ok() && at.has_more()).then_some(Some(due));
        let result = at.view.stamp(next).and(result);
        let (statements, form, so_far) = &mut self.counted;
        let page = work.counts();
        *so_far = std::array::from_fn(|at| so_far[at] + page[at]);
        statements.add(form.as_deref(), false, page, *so_far, None);
        if result.is_err() || self.at.as_ref().is_some_and(|at| !at.has_more()) {
            self.at = None;
        }
        let lapsed = |field| matches!(field, "cursor_idle_ms" | "cursor_lifetime_ms");
        self.end = result.as_ref().err().map(|error| match *error {
            Error::ReadLimitExceeded { field, limit } if lapsed(field) => Some((field, limit)),
            _ => None,
        });
        result
    }
    fn publication(owner: &Option<Weak<Open>>, commit: TxId) -> Result<()> {
        if let Some(open) = owner.as_ref().and_then(Weak::upgrade) {
            let (view, _) = open.newest()?;
            if view.commit() > commit && view.publication_owed()? {
                return Err(Error::AuthoritativeErasure);
            }
        }
        Ok(())
    }
    /// Releases the held view and remaining answer. Repeated close is accepted.
    pub fn close(&mut self) -> Result<()> {
        self.at = None;
        self.end.get_or_insert(None);
        Ok(())
    }
}
