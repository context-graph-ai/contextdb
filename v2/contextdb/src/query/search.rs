//! A similarity search, one source of a statement's rows: the rows of a table that pass the
//! statement's filter and lie nearest a vector, or under `USE RANK` rank best among those the
//! search compares. This part settles what is searched for, which partitions the filter selects
//! and which rows may be seen, and works out each row's distance from its stored vector; the
//! vector part finds the rows.

use std::cell::RefCell;

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
use crate::types::{ColumnType, Error, Result, Value, VectorSearchMode};
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
/// When the filter's path reaches few rows (see [`small`]), they are compared exactly; otherwise
/// the vector part searches the partitions the filter selects and asks, row by row, for the
/// distance. A row it asks about is read for the filter, its vector, its state and what its rank
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
        let path = &asked.choose(&[], &[])?;
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

        let scope = Scope::new(table, vector, given);
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
        let mut nearest = Nearest {
            count: Some(&count),
            table: table.id,
            vector: vector.id,
            column: vector,
            mode,
            scope: &keys,
            query: near,
            k: want,
            filtered: on_rows,
            ranked: rank.is_some(),
            bound: bound.map(|(most, _)| 1.0 - most),
        };
        let mut searching = given.budget.reserve(0, "query")?;
        let mut planned = Vec::new();
        if owned {
            planned = vector::plan(&nearest, given.read, &mut searching)?;
        }
        // Under INDEXED a partition that is behind its writes refuses the search whole.
        let index = table.vector_ref(vector);
        let refused = Error::VectorIndexedRouteUnavailable { index };
        if nearest.mode == VectorSearchMode::Indexed && planned.iter().any(|p| !p.usable) {
            return Err(refused);
        }
        let confined = given.reader.contexts.is_none() || asked.confined(path, &keys);
        let few = !owned || confined && asked.few(path, small(&planned, want))?;
        let mut selected = reserve()?;
        let mut passing = (!few && confined)
            .then(|| asked.keys(path, &mut selected))
            .transpose()?
            .flatten();
        let exact = few || planned.iter().all(|planned| planned.route == Route::Exact);
        // Works a row's distance out from its stored vector and offers the row to the answer
        // by that distance, or by its rank; the vector part keeps the nearest itself.
        let mut offer = |key: &Key, row: &[Value], kept: &mut Reservation| -> Result<_> {
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
            keep(&mut best, want, (by, key), kept)?;
            Ok(Some(distance))
        };
        if !few {
            let mut distance_of = |key: &Key| -> Result<Option<f64>> {
                let Some((row, bytes)) = asked.row(key, &mut kept)? else {
                    return Ok(None);
                };
                let distance = offer(key, &row, &mut kept)?;
                kept.shrink(bytes);
                Ok(distance)
            };
            let grouped = |p: &Planned| p.route == Route::Indexed && p.groups > 1;
            let unit = if planned.iter().any(grouped) { 128 } else { 16 };
            let cap = passing
                .as_ref()
                .filter(|_| mode == VectorSearchMode::Auto)
                .map(|keys| {
                    evaluations
                        .get()
                        .saturating_add((keys.len() as u64).saturating_mul(unit))
                });
            let found = loop {
                let found = vector::search(
                    &nearest,
                    given.read,
                    &mut searching,
                    &mut planned,
                    &mut |key| {
                        if passing.as_mut().is_some_and(|keys| !keys.remove(key)) {
                            return Ok(None);
                        }
                        distance_of(key)
                    },
                    (cap, evaluations),
                );
                if matches!(found, Err(Error::StoreCorrupted { .. }))
                    && nearest.mode == VectorSearchMode::Auto
                {
                    if passing.take().is_some() {
                        selected.shrink(selected.bytes());
                        passing = asked.keys(path, &mut selected)?;
                    }
                    nearest.mode = VectorSearchMode::Exact;
                    planned.clear();
                    searching.shrink(searching.bytes());
                    planned = vector::plan(&nearest, given.read, &mut searching)?;
                } else {
                    break found?.ok_or(refused)?;
                }
            };
            if found.stopped {
                for key in passing.iter().flatten() {
                    distance_of(key)?;
                }
            }
        } else {
            asked.walk(path, &mut kept, |key, row, bytes, kept| {
                offer(&key, &row, kept)?;
                kept.shrink(bytes);
                Ok(true)
            })?;
        }
        let tied = matches!(&best[..], [.., (last, ..), (after, ..)] if last == after);
        if further && best.len() == want && tied && exact {
            asked.walk(path, &mut kept, |key, row, bytes, kept| {
                kept.shrink(bytes);
                measured(&row).map_or(Ok(true), |d| answer(&key, &Value::Float64(d), reservation))
            })?;
            return Ok(());
        }
        for (by, key) in best.iter().take(self.most) {
            if !answer(key, by, reservation)? {
                break;
            }
        }
        Ok(())
    }
}

/// The most rows a filter's path may reach and be compared one by one, and never fewer than the
/// rows `asked` for: a 16th of the live vectors, or for a partition routed through its groups the
/// rows of 16 groups where that is fewer, up to which reading rows by key beats an IVF walk,
/// whose cost grows at least as a group does.
pub(super) fn small(planned: &[Planned], asked: usize) -> u64 {
    let groups = |p: &Planned| u32::from(p.route == Route::Indexed) * p.groups;
    let rows = |p: &Planned| p.live / u64::from(groups(p) / 16).max(16);
    planned.iter().map(rows).sum::<u64>().max(asked as u64)
}

/// Offers a pair to `best`, which holds in order the `most` least pairs offered: by value as
/// ORDER BY puts values going up, NULL last, then by key. A key `best` holds is not offered
/// again. What a pair holds is taken from `reservation` while it is held.
fn keep(
    best: &mut Vec<(Value, Key)>,
    most: usize,
    (value, key): (Value, &Key),
    reservation: &mut Reservation,
) -> Result<()> {
    let bytes = |key: &Key| (size_of::<(Value, Key)>() + key.as_bytes().len()) as u64;
    let before = |(held, by): &(_, Key)| sort_order(held, &value).then(by.cmp(key)).is_lt();
    let at = best.partition_point(before);
    if at == most || best.iter().any(|(_, held)| held == key) {
        return Ok(());
    }
    reservation.grow(bytes(key))?;
    best.insert(at, (value, key.clone()));
    if best.len() > most
        && let Some((_, out)) = best.pop()
    {
        reservation.shrink(bytes(&out));
    }
    Ok(())
}

/// Reads a filter for the partitions it selects.
pub(super) struct Scope<'a> {
    pub(super) table: &'a Table,
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
}

impl<'a> Scope<'a> {
    /// The scope of the partitions of `vector`, a VECTOR column of `table`.
    pub(super) fn new(table: &'a Table, vector: &VectorColumn, given: &Given<'a>) -> Self {
        let key = vector.partition_key.iter();
        let key = key.filter_map(|name| table.meta.columns.iter().position(|c| c.name == *name));
        Scope {
            table,
            key: key.collect(),
            ctx: given.ctx,
            reader: given.reader,
            held: RefCell::new(given.budget.reserve(0, "partition tuples").unwrap()),
            unbound: RefCell::default(),
        }
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
        let scope = self.table.scope();
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
        let on_rows = filter.is_some() && on_rows || reading.parts.iter().any(|p| p.kept.is_some());
        let mut keys = Vec::new();
        let mut held = self.held.borrow_mut();
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
