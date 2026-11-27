//! A SELECT that reads more than one relation of its own: its WITH names, then the values of
//! the SELECTs written inside its expressions, each worked out once before its first row, and
//! then the statement itself over them. What such a statement borrows lives while its rows
//! are read, so it is bound once when it is planned and put together when it is run.

use std::collections::{BTreeSet, HashMap};

use super::bind::{bind, kind};
use super::expression::Scope;
use super::plan::{self, Each, Given, Source};
use super::{Values, Visible, aggregate, join, sets::List, union};
use crate::memory::Reservation;
use crate::schema::Catalog;
use crate::sql::ast::Select;
use crate::sql::plan::{FromItem, ProjectColumn, SelectPlan};
use crate::sql::{Expr, MAX_EXPR_NESTING, Plan, depth_error, inside};
use crate::types::{ColumnDef, ColumnType, Error, Result, Value, heap_bytes};

#[derive(Clone, Copy, PartialEq)]
pub(super) enum Purpose {
    Scalar,
    Membership,
    Held,
}

/// What a statement is put together with.
pub(super) struct Pass<'r> {
    /// The statement's reservation: whatever is kept is asked of it first.
    pub(super) reservation: &'r mut Reservation,
    /// False while the statement is only bound: no row is read.
    pub(super) run: bool,
}

/// What takes a statement once it is put together: its columns and the source of its rows.
pub(super) type Then<'t, R> = dyn FnMut(&[ColumnDef], &dyn Source, &mut Pass<'_>) -> Result<R> + 't;

/// A statement that is put together when its rows are asked for, `depth` statements deep.
pub(super) struct Whole<'a> {
    pub(super) select: &'a SelectPlan,
    pub(super) given: &'a Given<'a>,
    pub(super) depth: usize,
}

impl<'a> Whole<'a> {
    /// `select` as such a statement; `None` for one SELECT of one table or of none with no
    /// SELECT inside it, which needs nothing put together.
    pub(super) fn of(select: &'a SelectPlan, given: &'a Given<'a>) -> Option<Whole<'a>> {
        let alone = select.with.is_empty() && select.union.is_empty() && select.joins.is_empty();
        (!alone || !subqueries(select).is_empty()).then_some(Whole {
            select,
            given,
            depth: 0,
        })
    }

    /// Binds the statement, reading no row: its columns, or its refusal.
    pub(super) fn bound(&self) -> Result<Vec<ColumnDef>> {
        let mut nothing = self.given.budget.reserve(0, "query")?;
        let mut pass = Pass {
            reservation: &mut nothing,
            run: false,
        };
        let columns =
            &mut |columns: &[ColumnDef], _: &dyn Source, _: &mut Pass<'_>| Ok(columns.to_vec());
        frame(self.select, 0, self.depth, self.given, &mut pass, columns)
    }
}

impl Source for Whole<'_> {
    fn rows(&self, reservation: &mut Reservation, each: &mut Each<'_>) -> Result<()> {
        let mut pass = Pass {
            reservation,
            run: true,
        };
        frame(
            self.select,
            0,
            self.depth,
            self.given,
            &mut pass,
            &mut |_, source, pass| source.rows(pass.reservation, each),
        )
    }
}

/// A WITH name in reach of a statement, and through `before` the ones written before it.
pub(super) struct Named<'a> {
    name: &'a str,
    plan: &'a SelectPlan,
    pub(super) columns: Vec<ColumnDef>,
    /// Its rows and the bytes each holds, when the statement reads the name more than once.
    kept: Option<Vec<(Vec<Value>, u64)>>,
    before: Option<&'a Named<'a>>,
}

impl<'a> Named<'a> {
    /// The WITH name a FROM item names, if it is one.
    pub(super) fn find(given: &Given<'a>, name: &str) -> Option<&'a Named<'a>> {
        let mut next = given.named;
        while let Some(named) = next {
            if named.name == name {
                return Some(named);
            }
            next = named.before;
        }
        None
    }

    /// Hands on the rows of the name: the kept rows, each copied and asked for again, or the
    /// rows of its SELECT as they are read, `depth` statements deep, with the names written
    /// before it in reach and `bound`, the WHERE of the statement that reads nothing but this name.
    pub(super) fn rows(
        &self,
        given: &Given<'_>,
        bound: Option<&Expr>,
        depth: usize,
        reservation: &mut Reservation,
        each: &mut Each<'_>,
    ) -> Result<()> {
        let Some(kept) = &self.kept else {
            let given = Given {
                named: self.before,
                bound,
                ..*given
            };
            let whole = Whole {
                select: self.plan,
                given: &given,
                depth,
            };
            return whole.rows(reservation, each);
        };
        for (row, bytes) in kept {
            reservation.grow(*bytes)?;
            if !each(row.clone(), *bytes, reservation)? {
                break;
            }
        }
        Ok(())
    }
}

/// Puts `select` together from its WITH name at `at` on and hands it to `then`: each name in
/// written order, the SELECTs of its UNION, the values of its subqueries and its own source
/// over them; what names and values hold goes back when `then` is done. `depth` statements and
/// joins hold it; one more for it and each of its joins past the limit of nesting is refused.
pub(super) fn frame<R>(
    select: &SelectPlan,
    at: usize,
    depth: usize,
    given: &Given<'_>,
    pass: &mut Pass<'_>,
    then: &mut Then<'_, R>,
) -> Result<R> {
    let depth = depth + 1;
    if depth + select.joins.len() > MAX_EXPR_NESTING {
        return Err(depth_error(depth + select.joins.len()));
    }
    // The bound is for this SELECT alone, never for one inside it.
    let bound = given.bound;
    let given = &Given {
        bound: None,
        ..*given
    };
    let start = pass.reservation.bytes();
    if let Some((name, plan)) = select.with.get(at) {
        let mut columns = Whole {
            select: plan,
            given,
            depth,
        }
        .bound()?;
        if plan.column_aliases.len() > columns.len() {
            return Err(Error::PlanError(
                "derived table has more column aliases than columns".to_owned(),
            ));
        }
        for (column, alias) in columns.iter_mut().zip(&plan.column_aliases) {
            column.name = alias.clone();
        }
        let mut named = Named {
            name,
            plan,
            columns,
            kept: None,
            before: given.named,
        };
        // A name the rest of the statement reads once is read in place; read more, its rows
        // are read once and kept.
        if pass.run && reads(select, at + 1, name) > 1 {
            let mut rows = Vec::new();
            frame(plan, 0, depth, given, pass, &mut |_, source, pass| {
                source.rows(pass.reservation, &mut |row, bytes, reservation| {
                    reservation.room(&mut rows, usize::MAX)?;
                    rows.push((row, bytes));
                    Ok(true)
                })
            })?;
            named.kept = Some(rows);
        }
        let given = Given {
            named: Some(&named),
            bound,
            ..*given
        };
        let taken = pass.reservation.bytes() - start;
        let framed = frame(select, at + 1, depth, &given, pass, then);
        pass.reservation.shrink(taken);
        return framed;
    }
    if !select.union.is_empty() {
        return union::after(select, depth, given, pass, then);
    }

    let outer: Vec<_> = relations(select).collect();
    let values = values(subqueries(select), &outer, depth, given, pass)?;
    let taken = pass.reservation.bytes() - start;
    let given = Given {
        ctx: given.ctx.over(&values),
        ..*given
    };
    let mut seeing = given.budget.reserve(0, "query")?;
    let mut firsts = match bound {
        Some(bound) => join::firsts(select, bound, &given)?,
        None => None,
    };
    let first = match &mut firsts {
        Some(firsts) => join::first(firsts, &given)?,
        None => None,
    };
    let mut held = match first {
        Some(_) => None,
        None => join::held(select, &given)?,
    };
    let from = match (first, &mut held) {
        (Some(first), _) => Some(first),
        (None, Some(held)) => Some(join::from(select, &given, held, depth)?),
        (None, None) => None,
    };
    let (columns, source) = plan::one(select, &given, &mut seeing, from, pass.run)?;
    let framed = then(&columns, &*source, pass);
    pass.reservation.shrink(taken);
    framed
}

/// Works out each SELECT of `found`, written inside a statement whose relations FROM names
/// `outer`, `depth` statements deep: its values, under the place its text begins.
pub(super) fn values(
    found: Vec<(&Select, Purpose)>,
    outer: &[&String],
    depth: usize,
    given: &Given<'_>,
    pass: &mut Pass<'_>,
) -> Result<Values> {
    let mut values = Vec::new();
    for (query, purpose) in found {
        let single = &mut |columns: &[ColumnDef], source: &dyn Source, pass: &mut Pass<'_>| {
            if columns.len() != 1 {
                return Err(Error::PlanError(
                    "a SELECT inside an expression gives one column".to_owned(),
                ));
            }
            let _ = query.gives.0.set(columns[0].column_type);
            let mut found = List::Memory(Vec::new(), 0);
            if pass.run {
                // What the row holds stays held for its value.
                let rows = &mut |mut row: Vec<Value>, bytes, reservation: &mut Reservation| {
                    if purpose == Purpose::Scalar && !found.small()?.is_empty() {
                        return Err(Error::PlanError(
                            "a SELECT that stands for one value gives one row".to_owned(),
                        ));
                    }
                    let value = row.swap_remove(0);
                    reservation.shrink(bytes - heap_bytes(&value));
                    let sets = given.ctx.sets.filter(|_| purpose == Purpose::Membership);
                    found.push(value, query.at.0, sets, reservation)?;
                    Ok(true)
                };
                source.rows(pass.reservation, rows)?;
            }
            Ok(found)
        };
        let mut given = Given { ..*given };
        given.ctx.membership =
            (purpose == Purpose::Membership && given.ctx.sets.is_some()).then_some(query.at.0);
        let found = frame(query, 0, depth, &given, pass, single);
        pass.reservation.room(&mut values, usize::MAX)?;
        values.push((
            query.at.0,
            found.map_err(|refused| around(outer, query, refused))?,
        ));
    }
    Ok(values)
}

/// The values of the SELECTs inside the INSERT, UPDATE or DELETE `plan`, each worked out once
/// before the write changes a row (when it is explained, bound and no row read); then every
/// expression of it bound, those of VALUES with no column in reach.
pub(super) fn written(plan: &Plan, given: &Given, held: &mut Reservation) -> Result<Values> {
    let Some((name, exprs)) = plan.written() else {
        return Ok(Vec::new());
    };
    // A reader the written table refuses is refused before any SELECT of the statement.
    let table = given.catalog.table(name);
    let table = table.ok_or_else(|| Error::TableNotFound(name.clone()))?;
    let mut seeing = given
        .budget
        .reserve(table.heap + 32 * table.meta.columns.len() as u64, "query")?;
    Visible::new(table, given.catalog, given.reader, &mut seeing)?;
    let mut found = Vec::new();
    exprs.iter().for_each(|expr| gather(expr, &mut found));
    let mut pass = Pass {
        reservation: held,
        run: given.ctx.said.is_none(),
    };
    let found = values(found, &[name], 0, given, &mut pass)?;
    let plain = match plan {
        Plan::Insert(insert) => insert.values.iter().map(Vec::len).sum(),
        _ => 0,
    };
    let (none, scope) = (Scope::of(name, &[]), crate::query::scope(table));
    for (at, expr) in exprs.into_iter().enumerate() {
        bind(expr, if at < plain { &none } else { &scope }, given.params)?;
    }
    Ok(found)
}

/// Consumer-data tables read by a statement; visibility filters remain internal.
pub(crate) fn tables_read(
    plan: &Plan,
    catalog: Option<&Catalog>,
    allowed: Option<&BTreeSet<String>>,
) -> Result<BTreeSet<String>> {
    let mut found = BTreeSet::new();
    match plan {
        Plan::Select(select) => {
            select_tables(select, &BTreeSet::new(), catalog, allowed, &mut found)
        }
        Plan::Explain(inner) => return tables_read(inner, catalog, allowed),
        Plan::ShowReport(crate::sql::ast::Show {
            report: 1,
            column: Some((table, _)),
            ..
        }) => {
            found.insert(table.clone());
        }
        _ => {
            if let Some((table, exprs)) = plan.written() {
                found.insert(table.clone());
                let mut inner = Vec::new();
                for expr in exprs {
                    gather(expr, &mut inner);
                }
                for (select, _) in inner {
                    select_tables(select, &BTreeSet::new(), catalog, allowed, &mut found);
                }
            }
        }
    }
    if let Some(catalog) = catalog {
        for name in found
            .iter()
            .filter(|name| allowed.is_none_or(|names| names.contains(*name)))
        {
            catalog
                .table(name)
                .ok_or_else(|| Error::TableNotFound(name.clone()))?;
        }
    }
    Ok(found)
}

fn select_tables(
    select: &SelectPlan,
    scope: &BTreeSet<String>,
    catalog: Option<&Catalog>,
    allowed: Option<&BTreeSet<String>>,
    found: &mut BTreeSet<String>,
) {
    let mut scope = scope.clone();
    for (name, inner) in &select.with {
        select_tables(inner, &scope, catalog, allowed, found);
        scope.insert(name.clone());
    }
    for item in select
        .from
        .iter()
        .chain(select.joins.iter().map(|join| &join.item))
    {
        match item {
            FromItem::Table { name, .. } if !scope.contains(name) => {
                found.insert(name.clone());
            }
            FromItem::Graph(pattern) => {
                found.insert(pattern.edge_table.clone());
                // A node's column COLUMNS names is read from each table that can carry the node.
                let named = (pattern.columns.iter()).filter_map(|column| match &column.expr {
                    Expr::Column(c) if c.table.is_some() && c.column != "id" => Some(&c.column),
                    _ => None,
                });
                let tables = catalog.into_iter().flat_map(Catalog::tables);
                let carry = tables.filter(|t| allowed.is_none_or(|names| names.contains(&t.name)));
                let carry = carry.filter(|t| super::graph::node_id(t).is_some_and(|id| id.1));
                let carry = carry.filter(|t| named.clone().any(|name| t.column(name).is_ok()));
                found.extend(carry.map(|t| t.name.clone()));
            }
            _ => {}
        }
    }
    if let (Some(key), Some((name, column, _))) = (&select.use_rank, super::search::asked(select))
        && !scope.contains(name)
        && allowed.is_none_or(|names| names.contains(name))
        && let Some(table) = catalog.and_then(|catalog| catalog.table(name))
        && let Ok(at) = table.column(&column.column)
        && let Some(rank) = table
            .rules
            .rank_policies
            .iter()
            .find(|rank| rank.column == at && rank.policy.sort_key == *key)
    {
        found.insert(rank.policy.table.clone());
    }
    for inner in select
        .union
        .iter()
        .map(|(_, inner)| inner)
        .chain(subqueries(select).into_iter().map(|(inner, _)| inner))
    {
        select_tables(inner, &scope, catalog, allowed, found);
    }
}

/// The SELECTs written inside the expressions of `select` itself, each once, with whether it
/// stands for one value and not for the list after IN.
fn subqueries(select: &SelectPlan) -> Vec<(&Select, Purpose)> {
    let mut found = Vec::new();
    let columns = select.columns.iter().map(|column| &column.expr);
    let joins = select.joins.iter().filter_map(|join| join.on.as_ref());
    let order = select.order_by.iter().map(|key| &key.expr);
    let rest = (select.filter.iter().chain(&select.group_by)).chain(&select.having);
    let patterns = select
        .from
        .iter()
        .chain(select.joins.iter().map(|join| &join.item));
    let patterns = patterns
        .filter_map(|item| match item {
            FromItem::Graph(pattern) => Some(pattern),
            FromItem::Table { .. } => None,
        })
        .flat_map(|pattern| {
            pattern
                .filter
                .iter()
                .chain(pattern.columns.iter().map(|column| &column.expr))
        });
    for expr in columns.chain(joins).chain(rest).chain(order) {
        gather(expr, &mut found);
    }
    let held = found.len();
    patterns.for_each(|expr| gather(expr, &mut found));
    found[held..]
        .iter_mut()
        .filter(|(_, purpose)| *purpose == Purpose::Membership)
        .for_each(|(_, purpose)| *purpose = Purpose::Held);
    let mut seen = std::collections::HashSet::new();
    found.retain(|(query, _)| seen.insert(query.at.0));
    found
}

/// Adds the SELECTs written in `expr`, at any depth, to `found`.
pub(super) fn gather<'s>(expr: &'s Expr, found: &mut Vec<(&'s Select, Purpose)>) {
    match expr {
        Expr::Subquery(query) => found.push((query, Purpose::Scalar)),
        Expr::InSubquery { query, .. } => found.push((query, Purpose::Membership)),
        _ => {}
    }
    for inner in inside(expr) {
        gather(inner, found);
    }
}

/// How many FROM items and joins name `name` in the WITH SELECTs of `select` from `from` on, in
/// `select` itself, and in the SELECTs of its UNION and inside its expressions, at any depth.
fn reads(select: &SelectPlan, from: usize, name: &str) -> usize {
    let items = select
        .from
        .iter()
        .chain(select.joins.iter().map(|join| &join.item));
    let own =
        items.filter(|item| matches!(item, FromItem::Table { name: read, .. } if read == name));
    let with = select.with.iter().skip(from).map(|(_, select)| select);
    let inner = with.chain(select.union.iter().map(|(_, select)| select));
    let inner = inner.chain(subqueries(select).into_iter().map(|(select, _)| select));
    own.count() + inner.map(|select| reads(select, 0, name)).sum::<usize>()
}

/// The names FROM gives the relations of a SELECT.
fn relations(select: &SelectPlan) -> impl Iterator<Item = &String> {
    let items = select.from.iter();
    let items = items.chain(select.joins.iter().map(|join| &join.item));
    items.filter_map(|item| match item {
        FromItem::Table { name, alias } => Some(alias.as_ref().unwrap_or(name)),
        FromItem::Graph(_) => None,
    })
}

/// The refusal of a subquery `inner` of a statement whose relations are `outer`: a column it
/// could not find under a name that only the statement around it gives a relation is a read of
/// the outer row, which no version does, and is refused by that name.
fn around(outer: &[&String], inner: &SelectPlan, refused: Error) -> Error {
    match &refused {
        Error::ColumnNotFound { table, .. }
            if outer.contains(&table) && !relations(inner).any(|name| name == table) =>
        {
            Error::SubqueryNotSupported
        }
        _ => refused,
    }
}

/// The columns a SELECT gives a statement that reads it, for the select list `list` over
/// `scope`: each one's name, and its type, which is TEXT for a value whose type no row is
/// needed to know only because it is NULL.
pub(super) fn columns(
    scope: &Scope,
    list: Option<&[ProjectColumn]>,
    params: &HashMap<String, Value>,
) -> Result<Vec<ColumnDef>> {
    let Some(list) = list else {
        return Ok(scope.columns().cloned().collect());
    };
    let names = plan::names(scope, Some(list));
    let typed = names.into_iter().zip(list).map(|(name, column)| {
        // An aggregate is typed by what it answers: binding, which has passed, knows none.
        let mut expr = column.expr.clone();
        aggregate::typed(&mut expr);
        let column_type = kind(&expr, scope, params).ok().flatten();
        Ok(ColumnDef {
            name,
            column_type: column_type.unwrap_or(ColumnType::Text),
            nullable: true,
            primary_key: false,
            unique: false,
            default: None,
        })
    });
    typed.collect()
}
