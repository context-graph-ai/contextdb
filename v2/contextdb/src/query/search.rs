//! A similarity search, one source of a statement's rows: the rows of a table that pass the
//! statement's filter and lie nearest a vector, or under `USE RANK` rank best among those the
//! search compares. This part settles what is searched for, which partitions the filter selects
//! and which rows may be seen, and works out each row's distance from its stored vector; the
//! vector part finds the rows.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;

use super::aggregate::made;
use super::expression::{EvalContext, column_index, eval, holds, sort_order};
use super::plan::{self, Each, Given, Source};
use super::rank::Rank;
use super::table::{self, as_key};
use super::visible::{Reader, Visible};
use crate::memory::Reservation;
use crate::schema::{Table, VectorColumn};
use crate::sql::Expr;
use crate::sql::ast::{BinOp, ColumnRef, SortDirection};
use crate::sql::plan::{FromItem, SelectPlan, SortKey};
use crate::store::Key;
use crate::types::{Error, Result, Value, VectorSearchMode};
use crate::vector::{self, Nearest, Planned, Route};

/// The most key tuples a filter may name; past that the search reads every partition.
const MOST_TUPLES: usize = 4096;

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
/// sort above to choose among the rows at one distance.
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
    fn rows(&self, reservation: &mut Reservation, each: &mut Each<'_>) -> Result<u64> {
        let (table, vector, select, given) = (self.table, self.vector, self.select, self.given);
        let (ctx, filter, at) = (&given.ctx, select.filter.as_ref(), self.at);
        let evaluations = given.evaluations;
        // A NULL vector is near no row: no row has a distance from it.
        let Some(near) = &self.near else {
            return Ok(0);
        };
        let reserve = || given.budget.reserve(0, "query");
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
        let asked = asked.named(self.qualifier);
        let asked = asked.reading(self.filtered.clone(), &its);
        let path = &asked.choose(&[], &[])?;
        // A row of the answer passed both filters when it was asked about: now only what is read.
        let (admin, mut shown) = (Reader::admin(), reserve()?);
        let shown = Visible::new(table, given.catalog, &admin, &mut shown)?;
        let shown = table::Source::new(table, None, ctx, given.read, shown);
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
        let examined = Cell::new(0);
        let mut best = Vec::new();

        let scope = Scope::new(table, vector, given);
        let Some(keys) = scope.keys(filter)? else {
            return Ok(0);
        };
        // A narrowed handle is never refused by the state of an index it shares: INDEXED is AUTO.
        let mode = select.use_vector.unwrap_or_else(|| vector.mode());
        let auto = !given.reader.is_admin() && mode == VectorSearchMode::Indexed;
        let nearest = Nearest {
            table: table.id,
            vector: vector.id,
            column: vector,
            mode: if auto { VectorSearchMode::Auto } else { mode },
            scope: &keys,
            query: near,
            k: want,
            filtered: scope.on_rows(filter)?,
            ranked: rank.is_some(),
        };
        let mut searching = given.budget.reserve(0, "query")?;
        let mut planned = vector::plan(&nearest, given.read, &mut searching)?;
        // Under INDEXED a partition that is behind its writes refuses the search whole.
        let index = table.vector_ref(vector);
        let refused = Error::VectorIndexedRouteUnavailable { index };
        if nearest.mode == VectorSearchMode::Indexed && planned.iter().any(|p| !p.usable) {
            return Err(refused);
        }
        let few = asked.few(path, small(&planned, want))?;
        let exact = few || planned.iter().all(|planned| planned.route == Route::Exact);
        // Works a row's distance out from its stored vector and offers the row to the answer
        // by that distance, or by its rank; the vector part keeps the nearest itself.
        let mut offer = |key: &Key, row: &[Value], kept: &mut Reservation| -> Result<_> {
            let Some(distance) = measured(row) else {
                return Ok(None);
            };
            evaluations.set(evaluations.get() + 1);
            let by = match &rank {
                None if !few => return Ok(Some(distance)),
                None => Value::Float64(distance),
                Some(rank) => {
                    let (by, read) = rank.order(row, distance, given, kept)?;
                    examined.set(examined.get() + read);
                    by
                }
            };
            keep(&mut best, want, (by, key), kept)?;
            Ok(Some(distance))
        };
        if few {
            let read = asked.walk(path, &mut kept, |key, row, bytes, kept| {
                offer(&key, &row, kept)?;
                kept.shrink(bytes);
                Ok(true)
            })?;
            examined.set(examined.get() + read);
        } else {
            let mut distance_of = |key: &Key| -> Result<Option<f64>> {
                examined.set(examined.get() + 1);
                let Some((row, bytes)) = asked.row(key, &mut kept)? else {
                    return Ok(None);
                };
                let distance = offer(key, &row, &mut kept)?;
                kept.shrink(bytes);
                Ok(distance)
            };
            // `None` is a search under INDEXED whose filter on rows passes fewer rows than it
            // asks for.
            let (read, searched) = (given.read, &mut searching);
            let found = vector::search(&nearest, read, searched, &mut planned, &mut distance_of)?;
            let found = found.ok_or(refused)?;
            evaluations.set(evaluations.get() + found.evaluations);
            let hits = found.hits.into_iter().filter(|_| rank.is_none());
            best.extend(hits.map(|hit| (Value::Float64(hit.distance), hit.key)));
        }
        let mut examined = examined.get();
        let tied = matches!(&best[..], [.., (last, ..), (after, ..)] if last == after);
        if further && best.len() == want && tied && exact {
            let read = asked.walk(path, &mut kept, |key, row, bytes, kept| {
                kept.shrink(bytes);
                measured(&row).map_or(Ok(true), |d| answer(&key, &Value::Float64(d), reservation))
            })?;
            return Ok(examined + read);
        }
        for (by, key) in best.iter().take(self.most) {
            examined += 1;
            if !answer(key, by, reservation)? {
                break;
            }
        }
        Ok(examined)
    }
}

/// The most rows a filter's path may reach and be compared one by one, and never fewer than the
/// rows `asked` for: a row read by key costs what 16 slots of a block or 128 sketches cost to
/// screen, so a 128th of the live vectors searched through a grouping, a 16th of the others.
pub(super) fn small(planned: &[Planned], asked: usize) -> u64 {
    let grouped = |p: &Planned| p.route == Route::Indexed && p.groups > 1;
    let weight = |p: &Planned| if grouped(p) { p.live } else { p.live * 8 };
    (planned.iter().map(weight).sum::<u64>() / 128).max(asked as u64)
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

/// The values a filter fixes for the PARTITION_KEY columns, in their order; `None` for a
/// column it leaves open.
type Tuple = Vec<Option<Value>>;

/// Reads a filter for the partitions it selects.
pub(super) struct Scope<'a> {
    pub(super) table: &'a Table,
    /// Where each PARTITION_KEY column is in a row, in declared order.
    pub(super) key: Vec<usize>,
    pub(super) params: &'a HashMap<String, Value>,
    pub(super) now: i64,
    reader: &'a Reader,
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
            params: given.params,
            now: given.ctx.now(),
            reader: given.reader,
            unbound: RefCell::default(),
        }
    }

    /// True when `filter` does more than select partitions, so that some row of the
    /// partitions [`Scope::keys`] selects may not pass it: a filter on rows. False with no
    /// filter, and for a filter made only of the selections of key tuples `keys` understood.
    /// It fills `Nearest::filtered`: a search with a filter on rows uses the index as any
    /// other does, and only the refusal of INDEXED asks whether there is one.
    pub(super) fn on_rows(&self, filter: Option<&Expr>) -> Result<bool> {
        let Some(filter) = filter else {
            return Ok(false);
        };
        // Each tuple selects whole partitions when the columns it fixes are the leading ones.
        let leading = |tuple: &Tuple| {
            let fixed = tuple.iter().take_while(|value| value.is_some()).count();
            fixed > 0 && tuple[fixed..].iter().all(Option::is_none)
        };
        let tuples = self.said(filter)?;
        let selects = tuples.len() <= MOST_TUPLES && tuples.iter().all(leading);
        Ok(!selects || !self.only_selects(filter)?)
    }

    /// True when every part of `filter` is one [`Scope::said`] reads whole: `AND` of two
    /// sides that fix different key columns, `OR`, and `=` or `IN` whose every pair fixes a
    /// key column.
    fn only_selects(&self, filter: &Expr) -> Result<bool> {
        let whole = |columns: &Expr, values: &Expr| -> Result<bool> {
            let (columns, values) = (columns.sides(), values.sides());
            let fixed = self.tuple(columns, values)?.iter().flatten().count();
            Ok(fixed == columns.len() && fixed == values.len())
        };
        Ok(match filter {
            Expr::BinaryOp { left, op, right } if matches!(op, BinOp::And | BinOp::Or) => {
                let (one, other) = (self.said(left)?, self.said(right)?);
                let fixes =
                    |said: &[Tuple], at: usize| said.iter().any(|tuple| tuple[at].is_some());
                let clash = (0..self.key.len()).any(|at| fixes(&one, at) && fixes(&other, at));
                !(clash && *op == BinOp::And)
                    && self.only_selects(left)?
                    && self.only_selects(right)?
            }
            Expr::BinaryOp { left, op, right } if *op == BinOp::Eq => match **left {
                Expr::Column(_) => whole(left, right)?,
                _ => whole(right, left)?,
            },
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let mut every = !*negated;
                for item in list {
                    every &= whole(expr, item)?;
                }
                every
            }
            _ => false,
        })
    }

    /// The partitions to search: for each key tuple the filter names, the key of the leading
    /// key columns it fixes, which every partition with those values begins with; every one for
    /// no filter, more than 4,096 tuples, or a tuple that fixes no leading column or names one
    /// through a `$name` with no value. It may select too much and never too little: the whole
    /// filter is still applied to every row. When the reader's Context is the leading key
    /// column they are narrowed first to its own Contexts, so that nothing of another is read
    /// or counted: `None` when none is left.
    pub(super) fn keys(&self, filter: Option<&Expr>) -> Result<Option<Vec<Key>>> {
        let tuples = match (filter, self.key.is_empty()) {
            (Some(filter), false) => self.said(filter)?,
            _ => Vec::new(),
        };
        let every = tuples.len() > MOST_TUPLES || tuples.iter().any(|tuple| tuple[0].is_none());
        let every = every || !self.unbound.borrow().is_empty();
        let mut keys = Vec::new();
        for tuple in tuples.iter().filter(|_| !every) {
            let leading: Vec<Value> = tuple.iter().map_while(Clone::clone).collect();
            keys.push(Key::from_values(&leading, &[])?);
        }
        // A partition is named once, and not again under a key that begins as another does.
        keys.sort();
        keys.dedup_by(|later, kept| later.starts_with(kept));
        let leading = self.table.rules.context_column;
        let leading = leading.is_some() && leading == self.key.first().copied();
        let Some(own) = self.reader.contexts.as_ref().filter(|_| leading) else {
            return Ok(Some(keys));
        };
        let own = (own.iter()).map(|context| Key::from_values(&[Value::Uuid(context.0)], &[]));
        let own = own.collect::<Result<Vec<_>>>()?;
        let named = if keys.is_empty() { &own } else { &keys };
        let mine = |key: &&Key| own.iter().any(|own| key.starts_with(own));
        let named: Vec<Key> = named.iter().filter(mine).cloned().collect();
        Ok((!named.is_empty()).then_some(named))
    }

    /// What a filter says about the key columns: a row passes it only under one of these
    /// tuples. `AND` joins what its two sides say, `OR` gives the tuples of both, and
    /// `column = value`, `column IN (values)` and `(columns) IN (row values)` say which values;
    /// anything else leaves every column open.
    fn said(&self, filter: &Expr) -> Result<Vec<Tuple>> {
        let open = || vec![vec![None; self.key.len()]];
        Ok(match filter {
            Expr::BinaryOp { left, op, right } if *op == BinOp::And => {
                let (left, right) = (self.said(left)?, self.said(right)?);
                if left.len().saturating_mul(right.len()) > MOST_TUPLES {
                    return Ok(open());
                }
                let join = |left: &Tuple, right: &Tuple| -> Tuple {
                    let both = left.iter().zip(right);
                    both.map(|(left, right)| left.clone().or_else(|| right.clone()))
                        .collect()
                };
                let pairs = left.iter().flat_map(|l| right.iter().map(move |r| (l, r)));
                pairs.map(|(left, right)| join(left, right)).collect()
            }
            Expr::BinaryOp { left, op, right } if *op == BinOp::Or => {
                [self.said(left)?, self.said(right)?].concat()
            }
            Expr::BinaryOp { left, op, right } if *op == BinOp::Eq => {
                let (column, value) = match **left {
                    Expr::Column(_) => (left, right),
                    _ => (right, left),
                };
                vec![self.tuple(column.sides(), value.sides())?]
            }
            Expr::InList {
                expr,
                list,
                negated: false,
            } => {
                let tuples = list.iter();
                let tuples = tuples.map(|item| self.tuple(expr.sides(), item.sides()));
                tuples.collect::<Result<_>>()?
            }
            _ => open(),
        })
    }

    /// The tuple `columns = values` fixes, pair by pair. A pair counts when it is a key column
    /// beside a literal or a `$name` whose value a key of the column's type holds; any other
    /// pair leaves its column open.
    fn tuple(&self, columns: &[Expr], values: &[Expr]) -> Result<Tuple> {
        let mut tuple = vec![None; self.key.len()];
        for pair in columns.iter().zip(values) {
            let (Expr::Column(column), value @ (Expr::Literal(_) | Expr::Parameter(_))) = pair
            else {
                continue;
            };
            let at = column_index(self.table, column)?;
            let Some(place) = self.key.iter().position(|key| *key == at) else {
                continue;
            };
            match value {
                Expr::Parameter(name) if !self.params.contains_key(name) => {
                    self.unbound.borrow_mut().push(name.clone());
                }
                value => {
                    let ctx = EvalContext::new(self.params, self.now);
                    let value = eval(value, &self.table.scope(), &[], &ctx)?;
                    tuple[place] = as_key(self.table.meta.columns[at].column_type, value);
                }
            }
        }
        Ok(tuple)
    }
}
