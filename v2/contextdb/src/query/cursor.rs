//! Paging owns a continuation through the same sources that answer an ordinary query.
use super::*;
use crate::store::View;
use std::sync::Arc;

pub(crate) struct CursorRows {
    plan: Plan,
    _parsing: Reservation,
    params: HashMap<String, Value>,
    catalog: Arc<Catalog>,
    pub(crate) view: View,
    reader: Reader,
    budget: Budget,
    now: i64,
    walk: plan::Walk,
    whole: Option<QueryResult>,
    pending: Option<(Vec<Value>, u64)>,
    held: Reservation,
}
impl CursorRows {
    pub(crate) fn open(
        (plan, parsing): (Plan, Reservation),
        params: &HashMap<String, Value>,
        _text_bytes: usize,
        (view, catalog, reader, budget, now): (View, Arc<Catalog>, Reader, Budget, i64),
    ) -> Result<Self> {
        let (left, offset) = match &plan {
            Plan::Select(select) => plan::limits(select, params)?,
            Plan::ShowSetting(_) | Plan::ShowReport(_) => (0, 0),
            _ => return Err(Error::CursorInvalidStatement),
        };
        let bytes = params
            .iter()
            .map(|(key, value)| {
                key.len() as u64
                    + size_of::<(String, Value)>() as u64
                    + crate::types::heap_bytes(value)
            })
            .sum::<u64>();
        let whole = if matches!(&plan, Plan::ShowSetting(at) if at == &(0..1)) {
            inspect::show(
                &plan,
                &catalog,
                &view,
                (&budget, None, None, parsing.bytes()),
                None,
                None,
            )?
        } else {
            None
        };
        let budget = budget.statement();
        let held = budget.reserve(bytes, "cursor")?;
        Ok(Self {
            plan,
            _parsing: parsing,
            params: params.clone(),
            catalog,
            view,
            reader,
            budget,
            now,
            walk: plan::Walk {
                left: Cell::new(left),
                offset: Cell::new(offset),
                ..Default::default()
            },
            whole,
            pending: None,
            held,
        })
    }
    pub(crate) fn has_more(&self) -> bool {
        self.pending.is_some()
            || self
                .whole
                .as_ref()
                .map_or(self.walk.left.get() > 0 && self.walk.more.get(), |whole| {
                    !whole.rows.is_empty()
                })
    }
    pub(crate) fn page(&mut self, rows: u64, bytes: u64, work: &Work<'_>) -> Result<QueryResult> {
        self.view.valid()?;
        if self.whole.is_none() && !matches!(self.plan, Plan::Select(_)) {
            self.whole = inspect::show(
                &self.plan,
                &self.catalog,
                &self.view,
                (&self.budget, None, None, self._parsing.bytes()),
                work.limits.tables.as_ref(),
                Some(work.verify),
            )?;
        }
        if let Some(whole) = &mut self.whole {
            let page = whole.take_page(rows, bytes, work)?;
            return Ok(page.measured(&self.budget, &self.reader));
        }
        let Plan::Select(select) = &self.plan else {
            unreachable!()
        };
        let kept = Kept::new(self.budget.passing("cursor expression")?);
        let given = Given {
            allowed: work.limits.tables.as_ref(),
            params: &self.params,
            catalog: &self.catalog,
            read: &self.view,
            reader: &self.reader,
            budget: &self.budget,
            ctx: EvalContext::new(&self.params, self.now, &kept),
            evaluations: &work.evaluations,
            examined: Some((&work.examined, Some(work))),
            named: None,
            bound: None,
        };
        let mut seeing = self.budget.reserve(0, "query")?;
        let (columns, source) = plan::select(select, &given, &mut seeing)?;
        if !source.streaming() {
            let mut whole = collect(columns, &*source, &self.budget, None)?;
            let page = whole.take_page(rows, bytes, work)?;
            self.whole = Some(whole);
            return Ok(page.measured(&self.budget, &self.reader));
        }
        let columns_bytes = columns_bytes(columns.capacity(), columns.iter().map(String::capacity));
        self.held.grow(columns_bytes)?;
        let mut page = Vec::new();
        let mut published = 0;
        let pending = self.pending.take();
        let mut take = |row: Vec<Value>, charge: u64, held: &mut Reservation| -> Result<bool> {
            if !page.is_empty() && published + charge > bytes {
                self.pending = Some((row, charge));
                return Ok(false);
            }
            limit("cursor_page_bytes", charge, bytes)?;
            limit("cursor_page_rows", page.len() as u64 + 1, rows)?;
            work.result_row(charge)?;
            published += charge;
            held.room(&mut page, usize::MAX)?;
            page.push(row);
            Ok((page.len() as u64) < rows)
        };
        let more = match pending {
            Some((row, charge)) => take(row, charge, &mut self.held)?,
            None => true,
        };
        if more {
            source.page(&self.walk, &mut self.held, &mut take)?;
        }
        let moved = published + columns_bytes + (page.capacity() * size_of::<Vec<Value>>()) as u64;
        let mut page = QueryResult::answer(columns, page, self.held.split(moved));
        page.rows_examined = work.examined.get();
        page.cache = work.cached();
        page.distance_evaluations = work.evaluations.get();
        Ok(page.measured(&self.budget, &self.reader))
    }
}

impl QueryResult {
    fn take_page(&mut self, rows: u64, bytes: u64, work: &Work<'_>) -> Result<Self> {
        let (mut count, mut published) = (0, 0);
        for row in self
            .rows
            .iter()
            .take(usize::try_from(rows).unwrap_or(usize::MAX))
        {
            let charge =
                row_overhead(row.len()) + row.iter().map(crate::types::heap_bytes).sum::<u64>();
            if count > 0 && published + charge > bytes {
                break;
            }
            limit("cursor_page_bytes", charge, bytes)?;
            work.result_row(charge)?;
            published += charge;
            count += 1;
        }
        if rows == 0 && !self.rows.is_empty() {
            limit("cursor_page_rows", 1, rows)?;
        }
        let copied = columns_bytes(self.columns.len(), self.columns.iter().map(String::len))
            + (count * size_of::<Vec<Value>>()) as u64;
        let held = self._reservation.as_mut().expect("a charged answer");
        // An answer held at no charge, as a fixed SHOW is, hands out its pages at no charge.
        let moved = u64::from(held.bytes() != 0) * (published + copied);
        held.grow(moved.saturating_sub(published))?;
        let rows = self.rows.drain(..count).collect();
        let mut page = Self::answer(self.columns.clone(), rows, held.split(moved));
        page.rows_examined = work.examined.get();
        page.distance_evaluations = work.evaluations.get();
        Ok(page)
    }
}
