//! The shape of a statement: the row sources that answer one SELECT, each above the one below.
//! At the bottom is what FROM names; above it the rows are aggregated, or sorted unless they
//! arrive in order and the select list worked out, rows that are the same are given once for
//! DISTINCT, and OFFSET and LIMIT leave rows out and stop the read. Every stage takes a source
//! of rows and the scope of its columns, never a table.

use std::cell::Cell;
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashMap};

use super::aggregate::{Grouped, Groups, spelled};
use super::expression::{EvalContext, Scope, eval, holds, most_bytes, numbers, reads, unbound};
use super::graph::Graph;
use super::search::{self, Search};
use super::sort::{Key, Sort};
use super::table::{self, Path};
use super::visible::{Reader, Visible};
use super::window::{Numbered, Window};
use super::{counts, join, row_overhead, subquery};
use crate::memory::{Budget, Reservation};
use crate::schema::{Catalog, Table};
use crate::sql::plan::{FromItem, ProjectColumn, SelectPlan, SortKey};
use crate::sql::{Expr, Plan, not_supported};
use crate::store::Read;
use crate::types::{ColumnDef, Error, Result, Value, VectorIndexRef, heap_bytes};

/// What takes the rows of a source: one row, and the bytes of the statement's reservation the
/// row holds, which are the taker's from then on to pass on or give back. Answers false when
/// it wants no more rows.
pub(super) type Each<'e> = dyn FnMut(Vec<Value>, u64, &mut Reservation) -> Result<bool> + 'e;

/// Something that gives rows, one at a time and in its own order. What its columns are is
/// the `Scope` that is made with it and handed to every stage that reads its rows.
pub(super) trait Source {
    /// Hands `each` every row until it answers false. What the source keeps on the way is
    /// asked of `reservation` before it is kept. Returns how many stored rows were read.
    fn rows(&self, reservation: &mut Reservation, each: &mut Each<'_>) -> Result<u64>;
}

/// What one statement is answered with, the same for every source of it.
pub(super) struct Given<'a> {
    pub(super) params: &'a HashMap<String, Value>,
    pub(super) catalog: &'a Catalog,
    pub(super) read: &'a dyn Read,
    pub(super) reader: &'a Reader,
    /// What a source asks for the memory of a reader's filter it makes for itself.
    pub(super) budget: &'a Budget,
    /// What every expression of the statement is worked out with beside its row.
    pub(super) ctx: EvalContext<'a>,
    /// The distances the statement's searches worked out, counted as they are.
    pub(super) evaluations: &'a Cell<u64>,
    /// The last WITH name in reach, which leads to the ones before it.
    pub(super) named: Option<&'a subquery::Named<'a>>,
    /// For the SELECT of a WITH name read in place: the WHERE of the statement that reads it,
    /// which says how much of each partition of a window it asks for.
    pub(super) bound: Option<&'a Expr>,
}

/// One SELECT ready to give its rows: what its columns are called, and the source of them.
/// A statement that reads more than one table of its own is bound here and put together
/// when its rows are asked for.
pub(super) fn select<'a>(
    select: &'a SelectPlan,
    given: &'a Given<'a>,
    seeing: &'a mut Reservation,
) -> Result<(Vec<String>, Box<dyn Source + 'a>)> {
    let (columns, source) = match subquery::Whole::of(select, given) {
        Some(whole) => (whole.bound()?, Box::new(whole) as Box<dyn Source + 'a>),
        None => one(select, given, seeing, None)?,
    };
    Ok((
        columns.into_iter().map(|column| column.name).collect(),
        source,
    ))
}

/// The SELECT itself, without its WITH names, the SELECTs a UNION puts after it and the
/// values of its subqueries: its columns, and the source of its rows over `from`, or over
/// the one table FROM names when `from` is `None`.
///
/// A form whose source is not built in this version is refused here by its name, before any
/// table is looked up; then every name is bound against the scope of what FROM names, so no
/// refusal depends on the rows. `seeing` is what the reader's filter of the table holds.
pub(super) fn one<'a>(
    select: &'a SelectPlan,
    given: &'a Given<'a>,
    seeing: &'a mut Reservation,
    from: Option<join::From<'a>>,
) -> Result<(Vec<ColumnDef>, Box<dyn Source + 'a>)> {
    let table = match (select.from.as_slice(), &from) {
        ([], _) | (_, Some(_)) => None,
        ([FromItem::Table { name, alias }], None) => {
            let table = given.catalog.table(name);
            let table = table.ok_or_else(|| Error::TableNotFound(name.clone()))?;
            Some((table, alias.as_ref().unwrap_or(name).as_str()))
        }
        ([FromItem::Graph(_)], None) => None,
        _ => return Err(not_supported("more than one FROM item")),
    };
    // A graph pattern is a source of its own, whose columns are what its COLUMNS gives.
    let (graph, mut scope) = match (select.from.as_slice(), &from, table) {
        (_, Some(from), _) => (None, from.scope.clone()),
        ([FromItem::Graph(pattern)], None, _) => {
            let (graph, scope) = Graph::new(pattern, given)?;
            (Some(graph), scope)
        }
        (_, None, Some((table, qualifier))) => (None, Scope::of(qualifier, &table.meta.columns)),
        (_, None, None) => (None, Scope::default()),
    };
    // A search names a VECTOR column of its table and a vector of its length, or NULL, and
    // under USE RANK a policy of that column: all settled before any row, whatever the LIMIT.
    let search = match (table, search::asked(select)) {
        (Some((table, _)), Some((_, column, near))) => {
            let (at, _) = scope.find(column)?;
            let Some(vector) = table.vector(&column.column) else {
                let (table, column) = (table.name.clone(), column.column.clone());
                let index = VectorIndexRef { table, column };
                return Err(Error::UnknownVectorIndex { index });
            };
            let (index, expected) = (table.vector_ref(vector), table.dimension(vector));
            // Explained, a `$name` with no value as the vector, taken as NULL, or beside a key
            // column is left open: neither changes what the search says.
            let explained = given.ctx.said.is_some();
            if explained {
                let keys = search::Scope::new(table, vector, given);
                keys.keys(select.filter.as_ref())?;
                scope.open = keys.unbound.take();
            }
            let near = match near {
                Expr::Parameter(name) if explained && !given.params.contains_key(name) => {
                    scope.open.push(name.clone());
                    None
                }
                near => numbers(eval(near, &table.scope(), &[], &given.ctx)?)?,
            };
            let actual = near.as_ref().map_or(expected, Vec::len);
            if actual != expected {
                return Err(Error::VectorIndexDimensionMismatch {
                    index,
                    expected,
                    actual,
                });
            }
            if let Some(position) = near.iter().flatten().position(|n| !n.is_finite()) {
                return Err(Error::InvalidVectorValue { index, position });
            }
            let mut declared = table.rules.rank_policies.iter();
            let rank = select.use_rank.as_ref().map(|sort_key| {
                let at = declared
                    .position(|ranked| ranked.column == at && ranked.policy.sort_key == *sort_key);
                let sort_key = sort_key.clone();
                at.ok_or(Error::RankPolicyNotFound { index, sort_key })
            });
            // A reader the table its rank joins or this one refuses is refused, whatever follows.
            let rank = rank.transpose()?;
            let joined = rank.map(|at| &table.rules.rank_policies[at].policy.table);
            let joined = joined.and_then(|name| given.catalog.table(name));
            for table in joined.into_iter().chain([table]) {
                let mut held = given.budget.reserve(0, "query")?;
                Visible::new(table, given.catalog, given.reader, &mut held)?;
            }
            Some((vector, at, near, rank))
        }
        _ => None,
    };
    let (params, ctx) = (given.params, &given.ctx);

    // Everything the statement names is looked up before a row is read.
    let width = scope.columns().count();
    let (mut filtered, mut wanted) = (vec![false; width], vec![false; width]);
    let filter = match &from {
        Some(from) => from.filter,
        None => select.filter.as_ref(),
    };
    if let Some(filter) = filter {
        reads(filter, &scope, params, &mut filtered)?;
    }
    let (limit, offset) = limits(select, params)?;
    let list = list(select);
    let stored = table.map(|(table, _)| table);
    let groups = Groups::of(select, &scope, stored, params, &mut wanted)?;
    let (mut window, mut carried) = (None, None);
    if groups.is_none() {
        window = Window::of(select)?;
        // A search's one sort key is the order its rows arrive in: no stage works it out. A
        // select item written as that key takes the true distance the search has for each row.
        let ordered = search.is_some() && window.is_none() && select.order_by.len() == 1;
        let own = |column: &ProjectColumn| column.expr == select.order_by[0].expr;
        let unranked = list.filter(|_| ordered && select.use_rank.is_none());
        carried = unranked.and_then(|list| list.iter().position(own));
        let order = select.order_by.iter().map(|key| &key.expr);
        let order = order.skip(usize::from(ordered));
        let listed = list.unwrap_or_default().iter().enumerate();
        let listed = listed.filter(|(at, _)| Some(*at) != carried);
        for expr in order.chain(listed.map(|(_, column)| &column.expr)) {
            reads(expr, &scope, params, &mut wanted)?;
        }
    }
    if list.is_none() && groups.is_none() {
        wanted.fill(true);
    }
    // A SELECT DISTINCT sorts only by what it lists, as Postgres has it.
    let listed = list.unwrap_or_default().iter();
    let listed: Vec<_> = listed.map(|item| spelled(&item.expr, &scope)).collect();
    let unlisted = |key: &SortKey| match list {
        Some(_) => !listed.contains(&spelled(&key.expr, &scope)),
        None => !matches!(key.expr, Expr::Column(_)),
    };
    if select.distinct && select.order_by.iter().any(unlisted) {
        let why = "for SELECT DISTINCT, ORDER BY expressions must appear in the select list";
        return Err(Error::PlanError(why.to_owned()));
    }
    let columns = subquery::columns(&scope, list, params)?;
    let search = search.filter(|_| groups.is_none() && window.is_none());
    if select.use_rank.is_some() && search.is_none() {
        return Err(Error::UseRankRequiresVectorOrder);
    }
    let order = select.order_by.iter().map(|key| &key.expr);
    let listed = list.unwrap_or_default().iter().map(|column| &column.expr);
    let mut written = order.chain(listed).chain(filter).chain(&select.having);
    if let (Some(Table { name, rules, .. }), None) = (stored, &search)
        && !rules.exclude_vector_states.is_empty()
        && written.any(|expr| holds(expr, |expr| matches!(expr, Expr::Distance { .. })))
    {
        return Err(Error::PlanError(format!(
            "{name} leaves rows in some states out of vector search (EXCLUDE VECTOR), so <=> is \
             answered there only as ORDER BY <column> <=> <vector> LIMIT <n>"
        )));
    }

    // A count of every row is answered with no row read when a kept number gives it.
    let counted = match (table, &groups) {
        (Some((table, _)), Some(groups)) if groups.counts_every_row() => {
            counts::count_without_rows(table, &scope, filter, given.reader, ctx, given.read)?
        }
        _ => None,
    };
    // A grouped count is answered from one record for each group when a kept count has them.
    let kept = match (table, &groups) {
        (Some((table, _)), Some(groups)) => {
            groups.kept(table, &scope, filter, given.reader, ctx, given.read)?
        }
        _ => None,
    };
    let cut = |input| -> Box<dyn Source + 'a> {
        Box::new(Cut {
            input,
            offset,
            limit,
        })
    };
    // Only the rows that can leave are kept, unless which rows leave is settled later.
    let most = match select.distinct {
        true => usize::MAX,
        false => limit.saturating_add(offset),
    };
    // Rows whose select list is worked out already: DISTINCT, then OFFSET and LIMIT.
    let leave = |input: Box<dyn Source + 'a>| match select.distinct {
        true => cut(Box::new(Distinct { input })),
        false => cut(input),
    };

    // What FROM names, and whether its rows arrive in the order asked for: the order of the
    // window when there is one, and none for rows that are grouped.
    let order = match groups {
        Some(_) => &[],
        None => select.order_by.as_slice(),
    };
    let reached = window.as_ref().map_or(order, Window::keys);
    let (mut rows, in_order): (Box<dyn Source + 'a>, bool) = match (table, search, graph, from) {
        (Some(_), ..) if counted.is_some() || kept.is_some() => (Box::new(One(Vec::new())), true),
        // The distance is the one sort key, or a rank orders the rows.
        (Some((table, qualifier)), Some((vector, at, near, rank)), ..) => {
            let search = Search {
                table,
                qualifier,
                vector,
                at,
                near,
                rank,
                select,
                filtered,
                wanted,
                most,
                given,
            };
            // The search of the SELECT explained says only itself, in place of all said before.
            let explained =
                matches!(ctx.explained, Some(Plan::Select(top)) if std::ptr::eq(top, select));
            if let Some(said) = ctx.said.filter(|_| explained) {
                said.take();
                said.replace(vec![super::inspect::searched(&search)?]);
            }
            (Box::new(search), select.order_by.len() == 1)
        }
        (Some((table, qualifier)), None, ..) => {
            let visible = Visible::new(table, given.catalog, given.reader, seeing)?;
            let rows = table::Source::new(table, filter, ctx, given.read, visible)
                .named(qualifier)
                .reading(filtered, &wanted);
            let path = rows.choose(reached, &[])?;
            rows.say(&[], reached, &[])?;
            let in_order = path.in_order;
            (Box::new(Rows { table: rows, path }), in_order)
        }
        // With no table: the matches of a pattern or the rows of a join or of a WITH name, in
        // no order, or the one row of no FROM, each through the statement's filter.
        (None, _, Some(graph), _) => (Box::new(graph), reached.is_empty()),
        (None, _, None, Some(from)) => (from.rows, false),
        (None, _, None, None) => (Box::new(One(Vec::new())), true),
    };
    if let (None, Some(filter)) = (table, filter) {
        rows = Box::new(Filter {
            input: rows,
            scope: scope.clone(),
            filter,
            ctx,
        });
    }
    if let Some(groups) = groups {
        let grouped = Grouped {
            input: rows,
            scope,
            groups,
            ctx,
            counted,
            kept,
            most,
        };
        return Ok((columns, leave(Box::new(grouped))));
    }
    if let Some(window) = window {
        let numbered = Numbered {
            input: rows,
            scope,
            window,
            ctx,
            in_order,
            most,
        };
        return Ok((columns, leave(Box::new(numbered))));
    }
    if !in_order {
        rows = Box::new(Sort {
            input: rows,
            scope: scope.clone(),
            order,
            ctx,
            most,
        });
    }
    let mut moved = moved(list, &scope);
    carried.into_iter().for_each(|at| moved[at] = Some(width));
    let listed = |input| -> Box<dyn Source + 'a> {
        Box::new(Project {
            input,
            scope,
            columns: list,
            moved,
            ctx,
        })
    };
    let rows = match select.distinct {
        true => cut(Box::new(Distinct {
            input: listed(rows),
        })),
        false => listed(cut(rows)),
    };
    Ok((columns, rows))
}

/// The select list of a SELECT unless it is `*`, which is `None`.
pub(super) fn list(select: &SelectPlan) -> Option<&[ProjectColumn]> {
    let star = matches!(
        select.columns.as_slice(),
        [ProjectColumn { expr: Expr::Column(column), alias: None }]
            if column.table.is_none() && column.column == "*"
    );
    (!star).then_some(select.columns.as_slice())
}

/// What a result calls its columns: the names of the scope's columns for `SELECT *` (`None`),
/// otherwise each item's `AS` name, or without one the name the expression gives it.
pub(super) fn names(scope: &Scope, columns: Option<&[ProjectColumn]>) -> Vec<String> {
    let Some(columns) = columns else {
        return scope.columns().map(|column| column.name.clone()).collect();
    };
    columns.iter().map(ProjectColumn::name).collect()
}

/// How many rows LIMIT lets through and how many OFFSET leaves out first: the number written,
/// or the value bound to the `$name` written in its place, which is refused unless it is a
/// whole number that is not negative. No LIMIT is every row, and no OFFSET is none left out.
pub(super) fn limits(
    select: &SelectPlan,
    params: &HashMap<String, Value>,
) -> Result<(usize, usize)> {
    let count = |written: Option<u64>, name: &Option<String>, word: &str| {
        let count = match name {
            None => written,
            Some(name) => match params.get(name).ok_or_else(|| unbound(name))? {
                Value::Int64(count) if *count >= 0 => Some(*count as u64),
                _ => {
                    return Err(Error::PlanError(format!(
                        "{word} ${name} needs a whole number that is not negative"
                    )));
                }
            },
        };
        Ok(count.map(|count| usize::try_from(count).unwrap_or(usize::MAX)))
    };
    let limit = count(select.limit, &select.limit_name, "LIMIT")?;
    let offset = count(select.offset, &select.offset_name, "OFFSET")?;
    Ok((limit.unwrap_or(usize::MAX), offset.unwrap_or(0)))
}

/// Hands on the rows a source kept, each with the bytes it holds and the bytes that were held
/// beside it, which are given back. Once `each` answers false the rest give back theirs too.
pub(super) fn hand_on(
    kept: impl Iterator<Item = (Vec<Value>, u64, u64)>,
    reservation: &mut Reservation,
    each: &mut Each<'_>,
) -> Result<()> {
    let mut more = true;
    for (row, bytes, beside) in kept {
        reservation.shrink(beside);
        match more {
            true => more = each(row, bytes, reservation)?,
            false => reservation.shrink(bytes),
        }
    }
    Ok(())
}

/// The rows of one table that a path reaches, in the path's order: those the reader may see
/// and the statement's filter passes. Holds the row it is handing on and nothing else.
pub(super) struct Rows<'a> {
    pub(super) table: table::Source<'a>,
    pub(super) path: Path,
}

impl Source for Rows<'_> {
    fn rows(&self, reservation: &mut Reservation, each: &mut Each<'_>) -> Result<u64> {
        let each = |_, row, bytes, reservation: &mut Reservation| each(row, bytes, reservation);
        self.table.walk(&self.path, reservation, each)
    }
}

/// One row that is known before any is read: the row of no columns that a SELECT without FROM
/// works its select list out over, and a count that needed no row. Its values are numbers.
struct One(Vec<Value>);

impl Source for One {
    fn rows(&self, reservation: &mut Reservation, each: &mut Each<'_>) -> Result<u64> {
        let bytes = row_overhead(self.0.len());
        reservation.grow(bytes)?;
        each(self.0.clone(), bytes, reservation)?;
        Ok(0)
    }
}

/// The rows of its input that a filter passes; the others give their bytes back. For rows
/// that are not the rows of one table, whose source applies the statement's filter itself.
pub(super) struct Filter<'a> {
    pub(super) input: Box<dyn Source + 'a>,
    pub(super) scope: Scope<'a>,
    pub(super) filter: &'a Expr,
    pub(super) ctx: &'a EvalContext<'a>,
}

impl Source for Filter<'_> {
    fn rows(&self, reservation: &mut Reservation, each: &mut Each<'_>) -> Result<u64> {
        self.input
            .rows(reservation, &mut |row, bytes, reservation| {
                if eval(self.filter, &self.scope, &row, self.ctx)? == Value::Bool(true) {
                    return each(row, bytes, reservation);
                }
                reservation.shrink(bytes);
                Ok(true)
            })
    }
}

/// For each item of a select list, the column whose value it moves out of a row of `scope`:
/// the column it names, when no other item of the list reads that column. Any other item
/// copies what it reads. Empty for `SELECT *`, which hands the row over as it is.
pub(super) fn moved(columns: Option<&[ProjectColumn]>, scope: &Scope) -> Vec<Option<usize>> {
    let columns = columns.unwrap_or_default();
    let mut readers = vec![0; scope.columns().count()];
    for column in columns {
        let mut read = vec![false; readers.len()];
        super::expression::mark(&column.expr, scope, &mut read);
        for (readers, read) in readers.iter_mut().zip(read) {
            *readers += usize::from(read);
        }
    }
    let alone = |column: &ProjectColumn| match &column.expr {
        Expr::Column(named) => scope.find(named).ok().map(|(at, _)| at),
        _ => None,
    };
    let moved = columns.iter().map(alone);
    moved.map(|at| at.filter(|at| readers[*at] == 1)).collect()
}

/// A select list over the rows of one scope: `moved` is [`moved`] of the list.
pub(super) struct List<'s, 'a> {
    /// `None` for `SELECT *`.
    pub(super) columns: Option<&'s [ProjectColumn]>,
    pub(super) moved: &'s [Option<usize>],
    pub(super) scope: &'s Scope<'a>,
    pub(super) ctx: &'s EvalContext<'a>,
}

impl List<'_, '_> {
    /// The select list worked out for one row that holds `bytes` of `reservation`, or the row
    /// itself for `SELECT *`. The memory of the values is asked for before they are made, and
    /// the row's bytes are given back once they are. A value that is moved is held once: what
    /// it takes passes from the row to the result and is not asked for again.
    pub(super) fn of(
        &self,
        mut row: Vec<Value>,
        bytes: u64,
        reservation: &mut Reservation,
    ) -> Result<Vec<Value>> {
        let Some(columns) = self.columns else {
            return Ok(row);
        };
        let most = columns
            .iter()
            .map(|column| most_bytes(&column.expr, self.scope, &row, self.ctx));
        let passed = self.moved.iter().flatten().map(|at| heap_bytes(&row[*at]));
        let passed = passed.sum::<u64>().min(bytes);
        reservation.grow(row_overhead(columns.len()) + most.sum::<u64>() - passed)?;
        let values = columns
            .iter()
            .zip(self.moved)
            .map(|(column, moved)| match moved {
                Some(at) => Ok(std::mem::replace(&mut row[*at], Value::Null)),
                None => eval(&column.expr, self.scope, &row, self.ctx),
            })
            .collect::<Result<Vec<_>>>()?;
        reservation.shrink(bytes - passed);
        Ok(values)
    }
}

/// The rows of `input`, whose columns are `scope`, in `order` when it says one, with the select
/// list `columns` worked out over each: what is written over the groups or a window.
pub(super) fn listed<'a>(
    mut input: Box<dyn Source + 'a>,
    scope: Scope<'a>,
    (order, columns): (&'a [SortKey], &'a [ProjectColumn]),
    ctx: &'a EvalContext<'a>,
    most: usize,
) -> Box<dyn Source + 'a> {
    if !order.is_empty() {
        let scope = scope.clone();
        input = Box::new(Sort {
            input,
            scope,
            order,
            ctx,
            most,
        });
    }
    let moved = vec![None; columns.len()];
    let columns = Some(columns);
    Box::new(Project {
        input,
        scope,
        columns,
        moved,
        ctx,
    })
}

/// The select list worked out for each row of its input, whose columns are `scope`.
struct Project<'a> {
    input: Box<dyn Source + 'a>,
    scope: Scope<'a>,
    /// `None` for `SELECT *`.
    columns: Option<&'a [ProjectColumn]>,
    moved: Vec<Option<usize>>,
    ctx: &'a EvalContext<'a>,
}

impl Source for Project<'_> {
    fn rows(&self, reservation: &mut Reservation, each: &mut Each<'_>) -> Result<u64> {
        let list = List {
            columns: self.columns,
            moved: &self.moved,
            scope: &self.scope,
            ctx: self.ctx,
        };
        self.input
            .rows(reservation, &mut |row, bytes, reservation| {
                let before = reservation.bytes();
                let row = list.of(row, bytes, reservation)?;
                // What the row held has become what the worked-out values hold.
                each(row, reservation.bytes() + bytes - before, reservation)
            })
    }
}

/// OFFSET and LIMIT: the first `offset` rows are given back, the next `limit` are handed on,
/// and then the input is asked for no more. Holds nothing.
pub(super) struct Cut<'s> {
    pub(super) input: Box<dyn Source + 's>,
    pub(super) offset: usize,
    pub(super) limit: usize,
}

impl Source for Cut<'_> {
    fn rows(&self, reservation: &mut Reservation, each: &mut Each<'_>) -> Result<u64> {
        if self.limit == 0 {
            return Ok(0);
        }
        let (mut skipped, mut left) = (0, 0);
        self.input
            .rows(reservation, &mut |row, bytes, reservation| {
                if skipped < self.offset {
                    skipped += 1;
                    reservation.shrink(bytes);
                    return Ok(true);
                }
                left += 1;
                Ok(each(row, bytes, reservation)? && left < self.limit)
            })
    }
}

/// DISTINCT: each different row of its input once, in the order the first of each arrived.
/// Reads every row of its input and holds one entry for each different row.
pub(super) struct Distinct<'s> {
    pub(super) input: Box<dyn Source + 's>,
}

impl Source for Distinct<'_> {
    fn rows(&self, reservation: &mut Reservation, each: &mut Each<'_>) -> Result<u64> {
        // Beside a row: its entry here, and its place when the rows are put back in the order
        // they arrived in.
        let beside = 2 * size_of::<(Key<'_, ()>, (usize, u64))>() as u64;
        let mut first = BTreeMap::new();
        let examined = self
            .input
            .rows(reservation, &mut |row, bytes, reservation| {
                let arrived = first.len();
                match first.entry(Key(row, &[], ())) {
                    Entry::Vacant(new) => {
                        reservation.grow(beside)?;
                        new.insert((arrived, bytes));
                    }
                    Entry::Occupied(_) => reservation.shrink(bytes),
                }
                Ok(true)
            })?;
        let mut kept: Vec<_> = first.into_iter().collect();
        kept.sort_unstable_by_key(|(_, (arrived, _))| *arrived);
        let kept = kept
            .into_iter()
            .map(|(Key(row, ..), (_, bytes))| (row, bytes, beside));
        hand_on(kept, reservation, each)?;
        Ok(examined)
    }
}
