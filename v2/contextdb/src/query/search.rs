//! A similarity search, one source of a statement's rows: the rows of a table that pass the
//! statement's filter and lie nearest a vector, or under `USE RANK` rank best among those the
//! search compares. This part settles what is searched for, which partitions the filter selects
//! and which rows may be seen, and works out each row's distance from its stored vector; the
//! vector part finds the rows.

use std::cell::{Cell, RefCell};

use super::aggregate::made;
use super::expression::{EvalContext, Scope as Names, eval, holds, sort_order};
use super::filter::{self, Columns, Fact, MOST_CASES};
use super::plan::{self, Each, Given, Source};
use super::rank::Rank;
use super::table;
use super::visible::{Reader, Visible};
use crate::memory::Reservation;
use crate::schema::{Table, VectorColumn};
use crate::sql::Expr;
use crate::sql::ast::{BinOp, ColumnRef, SortDirection};
use crate::sql::plan::{FromItem, SelectPlan, SortKey};
use crate::store::Key;
use crate::types::{
    ColumnType, ContextId, Error, Result, Value, VectorIndexRef, VectorSearchMode, heap_bytes,
};
use crate::vector::{self, Nearest, Planned, Route};

/// Whether a SELECT is a similarity search, and if so its table, its VECTOR column and the
/// vector it is near: it reads one stored table and nothing beside it, groups no rows, has no
/// DISTINCT and has a LIMIT, and its first sort key is `column <=> value` nearest first, where
/// the value reads no column of a row. Planning asks this; EXPLAIN takes what the plan made.
pub(super) fn asked(select: &SelectPlan) -> Option<(&String, &ColumnRef, &Expr)> {
    let [FromItem::Table { name, .. }] = select.from.as_slice() else {
        return None;
    };
    let Some(SortKey {
        expr: Expr::Distance { left, right },
        direction: SortDirection::Asc,
    }) = select.order_by.first()
    else {
        return None;
    };
    let Expr::Column(column) = &**left else {
        return None;
    };
    let alone = select.joins.is_empty() && select.union.is_empty() && select.group_by.is_empty();
    let limited = select.limit.is_some() || select.limit_name.is_some();
    let reads_row = holds(right, |expr| matches!(expr, Expr::Column(_)));
    let search = alone && limited && !select.distinct && !reads_row;
    search.then_some((name, column, &**right))
}

/// The bound a filter sets on the distance when it is the one sort key: of the parts at its top
/// level that compare the distance with a value, `<` or `<=` either way about, the tightest, as
/// the most distance and whether a row may have it (at one value, the strict one); and the filter
/// without those parts. A value not worked out to a finite number before any row leaves its part
/// a row condition.
pub(super) fn bound(select: &SelectPlan, ctx: &EvalContext) -> (Option<(f64, bool)>, Option<Expr>) {
    let searched = asked(select).filter(|_| select.order_by.len() == 1);
    let distance = |side: &Expr| match (side, searched) {
        (Expr::Distance { left, right }, Some((_, column, near))) => {
            matches!(&**left, Expr::Column(c) if c.column == column.column) && **right == *near
        }
        _ => false,
    };
    let (mut top, mut rest, mut bound) = (Vec::from_iter(&select.filter), Vec::new(), None);
    while let Some(expr) = top.pop() {
        let Expr::BinaryOp { left, op, right } = expr else {
            rest.push(expr);
            continue;
        };
        let value = match op {
            BinOp::And => {
                top.extend([&**right, &**left]);
                continue;
            }
            BinOp::Lt | BinOp::Lte if distance(left) => Some(right),
            BinOp::Gt | BinOp::Gte if distance(right) => Some(left),
            _ => None,
        };
        let most = value.and_then(|value| eval(value, &Names::default(), &[], ctx).ok());
        match most.and_then(|most| table::as_key(ColumnType::Real, most.into_owned())) {
            Some(Value::Float64(most)) if most.is_finite() => {
                let new = (most, matches!(op, BinOp::Lte | BinOp::Gte));
                bound = bound.filter(|old| *old < new).or(Some(new));
            }
            // NaN sorts above every number, so the bound holds for every row.
            Some(Value::Float64(most)) if most.is_nan() => {}
            _ => rest.push(expr),
        }
    }
    let and = |left, right| Expr::BinaryOp {
        left: Box::new(left),
        op: BinOp::And,
        right: Box::new(right),
    };
    (bound, rest.into_iter().cloned().reduce(and))
}

/// A similarity search as the source of a statement's rows: the `most` rows of the table nearest
/// the vector `near`, nearest first and rows at one distance in key order, or under `USE RANK`
/// the `most` the column's rank policy puts first among the rows the search compares. A row in
/// a state its table leaves out of every search (`EXCLUDE VECTOR`) is in no answer.
///
/// Each selected partition is read as [`decide`] says: directly through its countable path, or
/// in the vector part's one walk over the partitions the filter selects, which asks, row by
/// row, for the distance. A row it asks about is read for the filter, its vector, its state and what its rank
/// reads; a row of the answer once more, for what the statement reads, its distance last unless
/// it reads `*`. With further sort keys one row more is asked for: when it is at the distance of
/// the last, and the search was exact, every row the search may answer with is handed on, for the
/// sort above to choose among the rows at one distance. A [`bound`] on the distance turns a row
/// past it away and ends the vector part's search early.
pub(super) struct Search<'a> {
    pub(super) table: &'a Table,
    /// The name the statement gives the table.
    pub(super) qualifier: &'a str,
    /// The VECTOR column, and where it is in a row.
    pub(super) vector: &'a VectorColumn,
    pub(super) at: usize,
    /// The vector searched for; `None` for NULL, which is near no row.
    pub(super) near: Option<Vec<f32>>,
    /// Under `USE RANK`, where its policy is among the table's.
    pub(super) rank: Option<usize>,
    pub(super) select: &'a SelectPlan,
    /// The columns the filter reads, and those the statement reads of a row of the answer.
    pub(super) filtered: Vec<bool>,
    pub(super) wanted: Vec<bool>,
    pub(super) most: usize,
    pub(super) given: &'a Given<'a>,
}

impl Source for Search<'_> {
    fn rows(&self, reservation: &mut Reservation, each: &mut Each<'_>) -> Result<()> {
        let (table, vector, select, given) = (self.table, self.vector, self.select, self.given);
        let (ctx, at) = (&given.ctx, self.at);
        let (bound, rest) = bound(select, ctx);
        let filter = rest.as_ref();
        let evaluations = given.evaluations;
        // A NULL vector is near no row: no row has a distance from it.
        let Some(near) = &self.near else {
            return Ok(());
        };
        let reserve = || given.budget.reserve(0, "query");
        // The vector searched for is the plan's copy of the one bound or written, held as it runs.
        let _near = given
            .budget
            .reserve(size_of_val(near.as_slice()) as u64, "query")?;
        let (mut seeing, mut looking, mut kept) = (reserve()?, reserve()?, reserve()?);
        // A row is read for the filter, its vector, its state and what its rank reads.
        let (columns, states) = (&table.meta.columns, &table.rules.exclude_vector_states);
        let machine = &table.meta.state_machine;
        let state = machine.as_ref().map(|machine| &machine.column);
        let state = columns.iter().position(|c| Some(&c.name) == state);
        let its = (0..columns.len()).map(|c| c == at || Some(c) == state);
        let mut its: Vec<bool> = its.collect();
        let excluded = |row: &[Value]| matches!(state.map(|at| &row[at]), Some(Value::Text(s)) if states.contains(s));
        let measured = |row: &[Value]| match (&row[at], excluded(row)) {
            (Value::Vector(stored), false) => Some(vector::distance(stored, near)),
            _ => None,
        };
        let (rank, score) = (self.rank, [made(None, "vector_score").0]);
        let rank = rank.map(|at| Rank::of(table, at, &score, given, &mut looking, &mut its));
        let rank = rank.transpose()?;
        let visible = Visible::new(table, given.catalog, given.reader, &mut seeing)?;
        let asked = table::Source::new(table, filter, ctx, given.read, visible);
        let asked = asked.named(self.qualifier).counted(given.examined);
        let asked = asked.reading(self.filtered.clone(), &its);
        let mut path = asked.choose(&[], &[])?;
        // The find's own reads leave a hidden row out; the row the statement names is decided
        // once, below, by the whole WHERE.
        let names = std::mem::take(&mut path.one);
        let path = &path;
        // A row of the answer passed both filters when it was asked about: now only what is read.
        let (admin, mut shown) = (Reader::admin(), reserve()?);
        let shown = Visible::new(table, given.catalog, &admin, &mut shown)?;
        let shown = table::Source::new(table, None, ctx, given.read, shown).counted(given.examined);
        let shown = shown.reading(vec![false; columns.len()], &self.wanted);
        let mut answer = |key: &Key, by: &Value, reservation: &mut Reservation| {
            let Some((mut row, bytes)) = shown.row(key, reservation)? else {
                return Ok(true);
            };
            row.extend(plan::list(select).map(|_| by.clone()));
            each(row, bytes, reservation)
        };

        let further = select.order_by.len() > 1;
        let want = self.most.saturating_add(usize::from(further));
        let mut best = Vec::new();

        let scope = Scope::new(table, self.qualifier, vector, given);
        let owned = asked.screen_owned(scope.context_leads());
        let mode = select.use_vector.unwrap_or_else(|| vector.mode());
        if !owned && mode == VectorSearchMode::Indexed {
            return Err(Error::VectorIndexedNarrowed {
                index: table.vector_ref(vector),
            });
        }
        let work = given.examined.and_then(|(_, work)| work);
        let count = |amount| super::admit(evaluations, amount, work);
        let (Some(keys), on_rows) = scope.keys(filter)? else {
            return Ok(());
        };
        let matching = |tuple: &Key| scope.matches(tuple);
        let mut nearest = Nearest {
            count: Some(&count),
            table: table.id,
            vector: vector.id,
            column: vector,
            mode,
            scope: &keys,
            matching: Some(&matching),
            query: near,
            k: want,
            ranked: rank.is_some(),
            bound: bound.map(|(most, _)| 1.0 - most),
            filtered: on_rows || !states.is_empty(),
        };
        // Keys the statement names are read directly under AUTO and EXACT, as are the rows a
        // reader that does not own its partitions may see.
        let named =
            matches!(path.reach, table::Reach::Keys(_)) && mode != VectorSearchMode::Indexed;
        let walks = owned && !named;
        let mut searching = given.budget.reserve(0, "query")?;
        let mut planned = Vec::new();
        if walks {
            planned = vector::plan(&nearest, given.read, &mut searching)?;
        }
        let on = (table, &scope.key[..], filter.filter(|_| on_rows));
        let how = (mode, want, rank.is_some());
        let mut decided = decide(&asked, on, &planned, how)?;
        if let Some(refused) = refusal(mode, &planned, &decided, table.vector_ref(vector)) {
            return Err(refused);
        }
        // The row the statement names is asked the whole WHERE, its distance bound too, by its
        // own path: refused when it is hidden and passes. A handle that hides nothing skips it.
        if names && !asked.screen_owned(false) {
            let mut seeing = reserve()?;
            let visible = Visible::new(table, given.catalog, given.reader, &mut seeing)?;
            let whole = table::Source::new(table, select.filter.as_ref(), ctx, given.read, visible);
            let whole = whole.named(self.qualifier);
            let whole = whole.reading(self.filtered.clone(), &self.filtered);
            whole.walk(&whole.choose(&[], &[])?, &mut kept, |_, _, bytes, kept| {
                kept.shrink(bytes);
                Ok(false)
            })?;
        }
        // Rows tied at the LIMIT's cutoff are held only while a partition is walked through its
        // index: a walk of every row reads them again, streamed, when the lookahead pair ties.
        // Set once the partitions read directly are known, and unset when the walk turns direct.
        let ties = Cell::new(None);
        // Works a row's distance out from its stored vector and offers the row to the answer
        // by that distance, or by its rank; the vector part keeps the nearest itself.
        let offer = |key: &Key, row: &[Value], kept: &mut Reservation, best: &mut Vec<_>| {
            let Some(distance) = measured(row) else {
                return Ok(None);
            };
            super::admit(evaluations, 1, work)?;
            // A row past the bound goes to the vector part as infinitely far: the bound cut it.
            if bound.is_some_and(|(most, equal)| distance > most || distance == most && !equal) {
                return Ok(Some(f64::INFINITY));
            }
            let by = match &rank {
                None => Value::Float64(distance),
                Some(rank) => rank.order(row, distance, given, kept)?,
            };
            keep(best, (want, ties.get()), (by, key), kept)?;
            Ok::<_, Error>(Some(distance))
        };
        // The countable paths read directly: the partitions decided so, and the walked ones
        // when the rows the walk read reached at a doubling the entries their paths reach.
        let mut paths = Vec::new();
        if walks {
            loop {
                let mut direct = decided.iter().map(|decided| decided.direct);
                planned.retain(|_| !direct.next().unwrap_or(false));
                let indexed = planned.iter().any(|planned| planned.route != Route::Exact);
                ties.set((further && indexed).then_some(self.most));
                if ties.get().is_none() {
                    let_go(&mut best, want, &mut kept)?;
                }
                let (now, walked): (Vec<_>, Vec<_>) = decided.drain(..).partition(|d| d.direct);
                paths.extend(now.into_iter().filter_map(|decided| decided.path));
                let doubling = !matches!(how, (VectorSearchMode::Indexed, ..))
                    && !matches!(how, (VectorSearchMode::Exact, _, true))
                    && walked.iter().all(|decided| decided.path.is_some());
                let crossed = walked.iter().map(|decided| decided.crossing).sum::<u64>();
                let next = Cell::new(if doubling {
                    2 * crossed.max(1)
                } else {
                    u64::MAX
                });
                let (read, limit, doubled) =
                    (Cell::new(0_u64), Cell::new(u64::MAX), Cell::new(false));
                let mut distance_of = |key: &Key| -> Result<Option<f64>> {
                    read.set(read.get() + 1);
                    if read.get() == next.get() {
                        let mut reached = 0;
                        for path in walked.iter().filter_map(|decided| decided.path.as_ref()) {
                            reached += asked.entries(path, next.get().saturating_add(1), false)?;
                        }
                        if reached <= next.get() {
                            doubled.set(true);
                            limit.set(evaluations.get());
                            return Ok(None);
                        }
                        next.set(next.get().saturating_mul(2));
                    }
                    let Some((row, bytes)) = asked.row(key, &mut kept)? else {
                        return Ok(None);
                    };
                    let distance = offer(key, &row, &mut kept, &mut best)?;
                    kept.shrink(bytes);
                    Ok(distance)
                };
                let read = given.read;
                let found = vector::search(
                    &nearest,
                    read,
                    &mut searching,
                    &planned,
                    &mut distance_of,
                    (&limit, evaluations),
                );
                if !matches!(found, Err(Error::StoreCorrupted { .. }))
                    || nearest.mode != VectorSearchMode::Auto
                {
                    found?;
                    // At a doubling every walked partition is read directly: none is walked.
                    if doubled.get() {
                        ties.set(None);
                        let_go(&mut best, want, &mut kept)?;
                        planned.clear();
                        paths.extend(walked.into_iter().filter_map(|decided| decided.path));
                    }
                    break;
                }
                nearest.mode = VectorSearchMode::Exact;
                planned.clear();
                paths.clear();
                searching.shrink(searching.bytes());
                planned = vector::plan(&nearest, read, &mut searching)?;
                decided = decide(&asked, on, &planned, (VectorSearchMode::Exact, want, how.2))?;
            }
        }
        let statement = Some(path).filter(|_| !walks);
        for path in statement.into_iter().chain(&paths) {
            asked.walk(path, &mut kept, |key, row, bytes, kept| {
                offer(&key, &row, kept, &mut best)?;
                kept.shrink(bytes);
                Ok(true)
            })?;
        }
        // Every row was compared when no partition left is walked through its index: the
        // direct ones were taken out of `planned` as they were decided or at a doubling.
        let exact = !walks || planned.iter().all(|planned| planned.route == Route::Exact);
        let tied = matches!(best.get(..want), Some([.., (last, ..), (after, ..)]) if last == after);
        if further && tied && exact {
            asked.walk(path, &mut kept, |key, row, bytes, kept| {
                kept.shrink(bytes);
                measured(&row).map_or(Ok(true), |d| answer(&key, &Value::Float64(d), reservation))
            })?;
            return Ok(());
        }
        // The lookahead pair, or a pair after it, is answered only when it ties the LIMIT's
        // cutoff: one farther is past the LIMIT whatever the later ORDER BY keys say.
        let cut = self.most.checked_sub(1).and_then(|last| best.get(last));
        let within =
            |(by, _): &&(Value, Key)| cut.is_none_or(|(cut, _)| sort_order(cut, by).is_ge());
        for (by, key) in best.iter().take_while(within) {
            if !answer(key, by, reservation)? {
                break;
            }
        }
        Ok(())
    }
}

/// What one selected partition of a find does, the one decision execution and EXPLAIN read:
/// whether its filter on rows is served, its countable path, the entries that path reaches
/// counted no further than one past the crossing, the walk's expected cost in units, each
/// vector compared or row read one, and whether it is read directly through its path.
pub(super) struct Decided {
    pub(super) served: bool,
    pub(super) path: Option<table::Path>,
    pub(super) m: u64,
    pub(super) crossing: u64,
    pub(super) walk: u64,
    pub(super) direct: bool,
}

/// The decision for each of `planned`, from the statement's filter on rows `rest` with the
/// partition's tuple met last as equalities on the key columns at `key`. With no filter on
/// rows a partition is walked and decides nothing. A countable filter is direct while its path
/// reaches no more than the crossing; under EXACT with a rank at every size; under INDEXED
/// never.
pub(super) fn decide(
    asked: &table::Source,
    (table, key, rest): (&Table, &[usize], Option<&Expr>),
    planned: &[Planned],
    (mode, k, ranked): (VectorSearchMode, usize, bool),
) -> Result<Vec<Decided>> {
    let and = |left, right| Expr::BinaryOp {
        left: Box::new(left),
        op: BinOp::And,
        right: Box::new(right),
    };
    let decided = |p: &Planned| {
        let (k, groups) = (k as u64, u64::from(p.groups));
        let fixed = match p.route {
            // Each centroid and each slot of the groups its build's questions needed is a unit.
            Route::Indexed if groups > 1 => {
                groups + per(p.probes.into(), p.live, groups).min(p.live)
            }
            _ => p.live / 16,
        };
        let crossing = crossing(fixed, p.live, k);
        let Some(rest) = rest else {
            let (path, m, walk, direct) = (None, 0, 0, false);
            return Ok(Decided {
                served: true,
                path,
                m,
                crossing,
                walk,
                direct,
            });
        };
        let tuple = key
            .iter()
            .zip(p.tuple.values().unwrap_or_default())
            .map(|(at, value)| {
                let column = table.meta.columns[*at].name.clone();
                let column = Box::new(Expr::Column(ColumnRef {
                    table: None,
                    column,
                }));
                match value {
                    Value::Null => Expr::IsNull {
                        expr: column,
                        negated: false,
                    },
                    value => Expr::BinaryOp {
                        left: column,
                        op: BinOp::Eq,
                        right: Box::new(Expr::Literal(value)),
                    },
                }
            });
        let (served, path) = asked.partition(&tuple.fold(rest.clone(), and), key)?;
        let m = match &path {
            Some(path) => asked.entries(path, crossing.saturating_add(1), false)?,
            None => 0,
        };
        let walk = fixed.saturating_add(per(3 * u128::from(k), p.live, m.max(1)));
        let direct = path.is_some()
            && match mode {
                VectorSearchMode::Indexed => false,
                VectorSearchMode::Exact if ranked => true,
                _ => m <= crossing,
            };
        Ok(Decided {
            served,
            path,
            m,
            crossing,
            walk,
            direct,
        })
    };
    planned.iter().map(decided).collect()
}

/// `a * b / c`, worked in 128 bits and at most `u64::MAX`, so no LIMIT or size overflows it.
fn per(a: u128, b: u64, c: u64) -> u64 {
    u64::try_from(a * u128::from(b) / u128::from(c)).unwrap_or(u64::MAX)
}

/// The most entries a partition's path may reach and be read directly: the largest `m` with
/// `m * m <= fixed * m + 3 * k * live`, the walk's expected cost being its fixed part and three
/// rows read for each of `k` rows asked where one row in `live / m` passes. Compared without
/// dividing.
fn crossing(fixed: u64, live: u64, k: u64) -> u64 {
    let fits = |m: u64| {
        let m = u128::from(m);
        m * m <= u128::from(fixed) * m + 3 * u128::from(k) * u128::from(live)
    };
    let (f, c) = (fixed as f64, 3.0 * k as f64 * live as f64);
    let mut m = ((f + (f * f + 4.0 * c).sqrt()) / 2.0) as u64;
    while !fits(m) {
        m -= 1;
    }
    while fits(m + 1) {
        m += 1;
    }
    m
}

/// Whether a find under INDEXED is refused before anything is read, and by which kind: a
/// selected partition behind its writes, or one whose filter on rows nothing serves.
pub(super) fn refusal(
    mode: VectorSearchMode,
    planned: &[Planned],
    decided: &[Decided],
    index: VectorIndexRef,
) -> Option<Error> {
    let indexed = mode == VectorSearchMode::Indexed;
    if indexed && planned.iter().any(|planned| !planned.usable) {
        return Some(Error::VectorIndexedRouteUnavailable { index });
    }
    let unserved = indexed && decided.iter().any(|decided| !decided.served);
    unserved.then_some(Error::VectorIndexedFilterUnindexed { index })
}

/// Offers a pair to `best`, which holds in order the `most` least pairs offered: by value as
/// ORDER BY puts values going up, NULL last, then by key. With `ties` it holds besides every
/// pair offered whose value is that of the `ties`-th, the cutoff, so that a later sort on
/// other keys sees each of them as pgvector's does; only pairs beyond the cutoff are dropped.
/// A key `best` holds is not offered again: the same row comes with the same value, so it is
/// where the pair would go. What a pair holds is taken from `reservation` while it is held.
/// The room `best` grows to is taken as it grows and kept until `best` is dropped, since a
/// pair let go leaves that room allocated. Pairs held for ties are let go by [`let_go`] when
/// ties are no longer held.
pub(super) fn keep(
    best: &mut Vec<(Value, Key)>,
    (most, ties): (usize, Option<usize>),
    (value, key): (Value, &Key),
    reservation: &mut Reservation,
) -> Result<()> {
    let bytes = |key: &Key| key.as_bytes().len() as u64;
    let before = |(held, by): &(_, Key)| sort_order(held, &value).then(by.cmp(key)).is_lt();
    let beyond = |best: &[(Value, Key)], value: &Value| {
        let cutoff = ties.and_then(|ties| best.get(ties.checked_sub(1)?));
        cutoff.is_none_or(|(held, _)| sort_order(held, value).is_lt())
    };
    let trim = |best: &mut Vec<(Value, Key)>, reservation: &mut Reservation| {
        while best.len() > most
            && beyond(best, &best[best.len() - 1].0)
            && let Some((_, out)) = best.pop()
        {
            reservation.shrink(bytes(&out));
        }
    };
    let at = best.partition_point(before);
    if at >= most && beyond(best, &value) || best.get(at).is_some_and(|(_, held)| held == key) {
        return Ok(());
    }
    reservation.room(best, usize::MAX)?;
    reservation.grow(bytes(key))?;
    best.insert(at, (value, key.clone()));
    trim(best, reservation);
    Ok(())
}

/// Lets go, at once, of the pairs `best` holds past its `most` when ties stop being held, and of
/// the room they grew it to: a list just large enough for what is left, taken first, replaces
/// it, so that nothing read after is charged beside room no pair uses.
pub(super) fn let_go(
    best: &mut Vec<(Value, Key)>,
    most: usize,
    reservation: &mut Reservation,
) -> Result<()> {
    let past = most.min(best.len());
    for (_, out) in best.drain(past..) {
        reservation.shrink(out.as_bytes().len() as u64);
    }
    if best.capacity() > best.len() {
        let slot = size_of::<(Value, Key)>();
        reservation.grow((best.len() * slot) as u64)?;
        let mut fresh = Vec::with_capacity(best.len());
        fresh.append(best);
        let large = std::mem::replace(best, fresh).capacity();
        reservation.shrink((large * slot) as u64);
    }
    Ok(())
}

/// Reads a filter for the partitions it selects.
pub(super) struct Scope<'a> {
    pub(super) table: &'a Table,
    /// The name the statement gives the table, which its filter may qualify a column with.
    qualifier: &'a str,
    /// Where each PARTITION_KEY column is in a row, in declared order.
    pub(super) key: Vec<usize>,
    pub(super) ctx: EvalContext<'a>,
    reader: &'a Reader,
    /// What the keys of the partitions the filter names hold.
    held: RefCell<Reservation>,
    /// Each `$name` beside a key column that nothing is bound to, as it is met; its column is
    /// left open. A search refuses such a name before it asks; `explain` binds nothing, lists
    /// every partition and names the last.
    pub(super) unbound: RefCell<Vec<String>>,
    /// Past the most tuples, each case's values for each key column it lists, sorted.
    lists: RefCell<Vec<Listed>>,
}

/// One case's values for each key column it lists, sorted.
type Listed = Vec<(usize, Vec<Value>)>;

impl<'a> Scope<'a> {
    /// The scope of the partitions of `vector`, a VECTOR column of `table`.
    pub(super) fn new(
        table: &'a Table,
        qualifier: &'a str,
        vector: &VectorColumn,
        given: &Given<'a>,
    ) -> Self {
        let key = vector.partition_key.iter();
        let key = key.filter_map(|name| table.meta.columns.iter().position(|c| c.name == *name));
        Scope {
            table,
            qualifier,
            key: key.collect(),
            ctx: given.ctx,
            reader: given.reader,
            held: RefCell::new(given.budget.reserve(0, "partition tuples").unwrap()),
            unbound: RefCell::default(),
            lists: RefCell::default(),
        }
    }

    /// Whether a partition's key tuple can pass some case of the filter: one binary search for
    /// each listed key column of each case. Every tuple can below the most tuples.
    pub(super) fn matches(&self, tuple: &Key) -> bool {
        let (lists, values) = (self.lists.borrow(), tuple.values().unwrap_or_default());
        let holds = |(at, list): &(usize, Vec<Value>)| {
            let value = values.get(*at);
            value.is_some_and(|value| list.binary_search_by(|v| sort_order(v, value)).is_ok())
        };
        lists.is_empty() || lists.iter().any(|case| case.iter().all(holds))
    }

    fn context_leads(&self) -> bool {
        let context = self.table.rules.context_column;
        context.is_some() && context == self.key.first().copied()
    }

    /// The partitions to search, and whether `filter` does more than select them, so that some
    /// row of them may not pass it: a filter on rows, which fills `Nearest::filtered`.
    ///
    /// The partitions are, for each case of the filter, the key of the values it gives the
    /// leading key columns, which every partition with those values begins with; every one for
    /// no filter, more than 4,096 keys, or a case that gives the first key column no value or
    /// gives one through a `$name` with no value. It may select too much and never too little:
    /// the filter is still applied to every row. When the reader's Context is the leading key
    /// column they are narrowed first to its own Contexts, so that nothing of another is read
    /// or counted: `None` when none is left.
    pub(super) fn keys(&self, filter: Option<&Expr>) -> Result<(Option<Vec<Key>>, bool)> {
        let scope = Names::of(self.qualifier, &self.table.meta.columns);
        let reading = filter::read(
            filter,
            &Columns {
                scope: &scope,
                table: None,
            },
            &self.ctx,
        )?;
        if reading.cases.is_empty() {
            return Ok((None, false));
        }
        // A case searches the reader's Contexts, those it names among them when it names some:
        // none in every case leaves nothing to search, however the table is partitioned.
        let context = self.table.rules.context_column;
        if let (Some(own), Some(context)) = (&self.reader.contexts, context) {
            let mine = |v: &Value| matches!(v, Value::Uuid(id) if own.contains(&ContextId(*id)));
            let none = |case: &Vec<_>| match case.iter().find(|(at, _)| *at == context) {
                Some((_, Fact::Among { values, .. })) => !values.iter().any(mine),
                _ => own.is_empty(),
            };
            if reading.cases.iter().all(none) {
                return Ok((None, false));
            }
        }
        let open = reading
            .unbound
            .iter()
            .filter(|(at, _)| self.key.contains(at));
        self.unbound
            .borrow_mut()
            .extend(open.map(|(_, name)| name.clone()));
        let (mut tuples, mut every, mut on_rows) = (Vec::new(), false, false);
        for case in &reading.cases {
            let mut each = vec![Vec::new()];
            for at in &self.key {
                let Some((_, Fact::Among { values, .. })) = case.iter().find(|(c, _)| c == at)
                else {
                    break;
                };
                // Past the most keys a case names its partitions by the values before.
                if each.len() * values.len() > MOST_CASES {
                    break;
                }
                let more = each.iter().flat_map(|tuple: &Vec<Value>| {
                    values
                        .iter()
                        .map(|value| [tuple, std::slice::from_ref(value)].concat())
                });
                each = more.collect();
            }
            every |= each[0].is_empty();
            on_rows |= each[0].is_empty() || case.len() > each[0].len();
            tuples.extend(each);
            every |= tuples.len() > MOST_CASES;
            on_rows |= tuples.len() > MOST_CASES;
        }
        let every = every || !self.unbound.borrow().is_empty();
        let mut held = self.held.borrow_mut();
        for case in reading.cases.iter().filter(|_| every) {
            let mut lists = Vec::new();
            for (place, at) in self.key.iter().enumerate() {
                if let Some((_, Fact::Among { values, .. })) = case.iter().find(|(c, _)| c == at) {
                    let bytes = values
                        .iter()
                        .map(|v| size_of::<Value>() as u64 + heap_bytes(v));
                    held.grow(bytes.sum::<u64>() + 64)?;
                    let mut values = values.clone();
                    values.sort_by(sort_order);
                    lists.push((place, values));
                }
            }
            self.lists.borrow_mut().push(lists);
        }
        let on_rows = filter.is_some() && on_rows || reading.parts.iter().any(|p| p.kept.is_some());
        let mut keys = Vec::new();
        for leading in tuples.iter().filter(|_| !every) {
            held.grow(crate::store::encoded_len(leading) as u64)?;
            held.room(&mut keys, usize::MAX)?;
            keys.push(Key::from_values(leading, &[])?);
        }
        // A partition is named once, and not again under a key that begins as another does.
        keys.sort();
        keys.dedup_by(|later, kept| later.starts_with(kept));
        let Some(own) = self
            .reader
            .contexts
            .as_ref()
            .filter(|_| self.context_leads())
        else {
            return Ok((Some(keys), on_rows));
        };
        held.grow((own.len() * (17 + size_of::<Key>())) as u64)?;
        let own = (own.iter()).map(|context| Key::from_values(&[Value::Uuid(context.0)], &[]));
        let own = own.collect::<Result<Vec<_>>>()?;
        let named = if keys.is_empty() { &own } else { &keys };
        let mine = |key: &&Key| own.iter().any(|own| key.starts_with(own));
        let bytes = |k: &Key| k.as_bytes().len() + size_of::<Key>();
        held.grow(named.iter().filter(mine).map(bytes).sum::<usize>() as u64)?;
        let named: Vec<Key> = named.iter().filter(mine).cloned().collect();
        Ok(((!named.is_empty()).then_some(named), on_rows))
    }
}
