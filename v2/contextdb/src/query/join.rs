//! FROM over more than one relation, or over a WITH name: the relations joined in the order
//! they are written. For each row of the left side the right table is read through the key
//! or index that the equalities of ON give values for; when none serves, the right side is
//! read once and its rows kept, asked for before each is kept.

use std::cell::Cell;
use std::cmp::Ordering;

use super::expression::{EvalContext, Scope, compare, eval, holds, mark, reads};
use super::plan::{self, Cut, Each, Given, Rows, Source};
use super::row_overhead;
use super::subquery::Named;
use super::table::{self, as_key};
use super::visible::Visible;
use crate::memory::Reservation;
use crate::schema::Table;
use crate::sql::ast::BinOp;
use crate::sql::plan::{FromItem, SelectPlan, SortKey};
use crate::sql::{Expr, not_supported};
use crate::types::heap_bytes;
use crate::types::{ColumnDef, ColumnType, Error, Result, Value};

/// What FROM gives a SELECT: the joined rows, their columns, and what of WHERE is still to be
/// applied to them.
pub(super) struct From<'a> {
    pub(super) rows: Box<dyn Source + 'a>,
    pub(super) scope: Scope<'a>,
    pub(super) filter: Option<&'a Expr>,
}

/// One relation of FROM under the name the statement gives it.
struct Relation<'c> {
    qualifier: &'c str,
    columns: &'c [ColumnDef],
    of: Of<'c>,
}

/// What a relation is: a stored table, or a WITH name of the statement.
#[derive(Clone, Copy)]
enum Of<'c> {
    Table(&'c Table),
    Named(&'c Named<'c>),
}

/// What the sources of a FROM borrow while its rows are read.
pub(super) struct Held<'c> {
    relations: Vec<Relation<'c>>,
    /// For each relation, the parts of WHERE that read it alone, which its table source
    /// applies before the join.
    filters: Vec<Option<Expr>>,
    /// For each relation, what the reader's filter of its table holds.
    seeing: Vec<Reservation>,
    /// Of each column of a joined row: whether a part of WHERE its table source applies reads
    /// it, and whether the statement reads it above the sources.
    read: (Vec<bool>, Vec<bool>),
    /// Whether a part of WHERE is left for the joined rows.
    rest: bool,
}

/// Looks up the relations of FROM, a missing table refused first, and binds WHERE against
/// them. `None` for a FROM `plan::one` answers alone: nothing, a pattern, or one stored table.
pub(super) fn held<'c>(select: &'c SelectPlan, given: &Given<'c>) -> Result<Option<Held<'c>>> {
    let items = select.from.iter();
    let (mut relations, mut seeing) = (Vec::new(), Vec::new());
    for item in items.chain(select.joins.iter().map(|join| &join.item)) {
        let FromItem::Table { name, alias } = item else {
            return match select.joins.is_empty() {
                true => Ok(None),
                false => Err(not_supported("GRAPH_TABLE beside a JOIN")),
            };
        };
        let qualifier = alias.as_ref().unwrap_or(name);
        let mut before = relations
            .iter()
            .map(|relation: &Relation| relation.qualifier);
        if !qualifier.is_empty() && before.any(|before| before == *qualifier) {
            let refused = format!("table name \"{qualifier}\" specified more than once");
            return Err(Error::PlanError(refused));
        }
        let of = match Named::find(given, name) {
            Some(named) => Of::Named(named),
            None => {
                let table = given.catalog.table(name);
                Of::Table(table.ok_or_else(|| Error::TableNotFound(name.clone()))?)
            }
        };
        // A relation's columns are listed, and their names copied, as the statement is planned.
        let (columns, heap) = match of {
            Of::Named(named) => (&named.columns, 0),
            Of::Table(table) => (&table.meta.columns, table.heap),
        };
        let bytes = |c: &ColumnDef| c.name.len() as u64 + 32;
        let names = columns.iter().map(bytes).sum::<u64>();
        seeing.push(given.budget.reserve(heap + names, "query")?);
        relations.push(Relation {
            qualifier,
            columns,
            of,
        });
    }
    if let []
    | [
        Relation {
            of: Of::Table(_), ..
        },
    ] = relations.as_slice()
    {
        return Ok(None);
    }

    let mut scope = Scope::default();
    scope.checked = true;
    let scope = relations.iter().fold(scope, |scope, relation| {
        scope.and(relation.qualifier, relation.columns)
    });
    let mut filters: Vec<Option<Expr>> = relations.iter().map(|_| None).collect();
    let (mut parts, mut rest) = (Vec::new(), false);
    let mut filtered = vec![false; scope.columns().count()];
    if let Some(filter) = &select.filter {
        joined(filter, BinOp::And, &mut parts);
    }
    for part in parts {
        let mut read = vec![false; scope.columns().count()];
        reads(part, &scope, given.params, &mut read)?;
        // The relation that holds every column the part reads, or the first that can take a part
        // that reads none. Its table source applies the part unless it is the right side of a
        // LEFT JOIN, whose unmatched rows WHERE sees as NULL.
        let stored = |at: usize| matches!(relations[at].of, Of::Table(_));
        let takes = |at: usize| stored(at) && (at == 0 || !select.joins[at - 1].left_outer);
        let (mut start, none) = (0, !read.contains(&true));
        let home = relations.iter().enumerate().position(|(at, relation)| {
            let here = start..start + relation.columns.len();
            start = here.end;
            let elsewhere = read
                .iter()
                .enumerate()
                .any(|(at, read)| *read && !here.contains(&at));
            !elsewhere && read[here].contains(&true) || none && takes(at)
        });
        match home.filter(|at| takes(*at)) {
            Some(at) => {
                (filtered.iter_mut().zip(&read)).for_each(|(filtered, read)| *filtered |= read);
                filters[at] = Some(match filters[at].take() {
                    None => part.clone(),
                    Some(so_far) => Expr::BinaryOp {
                        left: Box::new(so_far),
                        op: BinOp::And,
                        right: Box::new(part.clone()),
                    },
                });
            }
            _ => rest = true,
        }
    }
    Ok(Some(Held {
        seeing,
        read: (filtered, wanted(select, &scope)),
        relations,
        filters,
        rest,
    }))
}

/// The columns of `scope` a SELECT reads of its rows above their sources: every one for `*`,
/// else those its list, ON, WHERE, GROUP BY, HAVING and ORDER BY name.
fn wanted(select: &SelectPlan, scope: &Scope) -> Vec<bool> {
    let mut wanted = vec![plan::list(select).is_none(); scope.columns().count()];
    let mut mark = |expr: &Expr| mark(expr, scope, &mut wanted);
    select.columns.iter().for_each(|column| mark(&column.expr));
    select.order_by.iter().for_each(|key| mark(&key.expr));
    let on = select.joins.iter().filter_map(|join| join.on.as_ref());
    on.chain(&select.filter).for_each(&mut mark);
    select.group_by.iter().chain(&select.having).for_each(mark);
    wanted
}

/// The parts of a condition that `by`, AND or OR, joins at its top.
fn joined<'e>(condition: &'e Expr, by: BinOp, parts: &mut Vec<&'e Expr>) {
    match condition {
        Expr::BinaryOp { left, op, right } if *op == by => {
            joined(left, by, parts);
            joined(right, by, parts);
        }
        part => parts.push(part),
    }
}

/// The source of the relations looked up in `held`, joined in written order, each ON bound
/// against the relations written so far; a WITH name among them is read `depth` deep.
pub(super) fn from<'a, 'c: 'a>(
    select: &'a SelectPlan,
    given: &'a Given<'a>,
    held: &'a mut Held<'c>,
    depth: usize,
) -> Result<From<'a>> {
    let (relations, filters, rest) = (&held.relations, &held.filters, held.rest);
    let (mut scope, mut rows) = (Scope::default(), None::<Side<'a>>);
    scope.checked = true;
    let sides = relations.iter().zip(filters).zip(&mut held.seeing);
    for (at, ((relation, filter), seeing)) in sides.enumerate() {
        let start = scope.columns().count();
        let width = relation.columns.len();
        scope = scope.and(relation.qualifier, relation.columns);
        seeing.grow(((at + 1) * size_of::<(&str, &[ColumnDef])>()) as u64)?;
        let right = match relation.of {
            Of::Table(table) => {
                let (filtered, wanted) = (&held.read.0[start..], &held.read.1[start..]);
                let visible = Visible::new(table, given.catalog, given.reader, seeing)?;
                let table =
                    table::Source::new(table, filter.as_ref(), &given.ctx, given.read, visible);
                // What the statement reads of a joined row is settled above the join.
                let table = table.named(relation.qualifier).counted(given.examined);
                Side::Table(Box::new(table.reading(filtered[..width].to_vec(), wanted)))
            }
            // A statement that reads nothing but the name hands its WHERE to the name.
            Of::Named(named) => {
                let alone = relations.len() == 1;
                let bound = select.filter.as_ref().filter(|_| alone);
                Side::Named(named, given, bound, depth + select.joins.len())
            }
        };
        let Some(left) = rows else {
            if let Side::Table(table) = &right {
                table.say(&[], &[], &[])?;
            }
            rows = Some(right);
            continue;
        };
        let join = &select.joins[at - 1];
        // The columns of the right side that ON says equal a column of the left row.
        let mut pins = Vec::new();
        if let Some(on) = &join.on {
            reads(on, &scope, given.params, &mut vec![false; start + width])?;
            let mut parts = Vec::new();
            joined(on, BinOp::And, &mut parts);
            for part in parts {
                if let Expr::BinaryOp {
                    left,
                    op: BinOp::Eq,
                    right,
                } = part
                    && let (Expr::Column(left), Expr::Column(right)) = (&**left, &**right)
                {
                    let (left, right) = (scope.find(left)?.0, scope.find(right)?.0);
                    if left.max(right) >= start && left.min(right) < start {
                        pins.push((left.max(right) - start, left.min(right)));
                    }
                }
            }
        }
        if let Side::Table(table) = &right {
            table.say(&pins.iter().map(|pin| pin.0).collect::<Vec<_>>(), &[], &[])?;
        }
        // Keep the written scope, but let a WITH output pin a stored-first INNER join.
        let (left, right, pins, width, swapped): (Box<dyn Source>, _, _, _, _) = match (left, right)
        {
            (Side::Table(table), right @ Side::Named(..))
                if !join.left_outer && !pins.is_empty() =>
            {
                let pins = pins.into_iter().map(|(a, b)| (b, a)).collect();
                (Box::new(right), Side::Table(table), pins, start, true)
            }
            (left, right) => (Box::new(left), right, pins, width, false),
        };
        rows = Some(Side::Joined(Box::new(Join {
            left,
            right,
            on: join.on.as_ref(),
            outer: join.left_outer,
            scope: scope.clone(),
            width,
            pins,
            ctx: &given.ctx,
            swapped,
        })));
    }
    let filter = select.filter.as_ref().filter(|_| rest);
    let from = rows.map(|rows| From {
        rows: Box::new(rows),
        scope,
        filter,
    });
    from.ok_or_else(|| not_supported("a FROM that names no table"))
}

/// One relation as a join reads it: a stored table through its source, or a WITH name.
enum Side<'a> {
    Joined(Box<Join<'a>>),
    Table(Box<table::Source<'a>>),
    Named(&'a Named<'a>, &'a Given<'a>, Option<&'a Expr>, usize),
}

impl Source for Side<'_> {
    fn rows(&self, reservation: &mut Reservation, each: &mut Each<'_>) -> Result<()> {
        match self {
            Side::Joined(join) => join.rows(reservation, each),
            Side::Table(table) => {
                let path = table.choose(&[], &[])?;
                table.walk(&path, reservation, |_, row, bytes, reservation| {
                    each(row, bytes, reservation)
                })
            }
            Side::Named(named, given, bound, depth) => {
                named.rows(given, *bound, *depth, reservation, each)
            }
        }
    }
}

/// The rows of `left`, each put beside every row of `right` that ON holds for; for a LEFT JOIN
/// a left row that nothing matches is put beside NULLs. Holds the left row and the row it is
/// handing on, and the rows of the right side when no key or index of it serves ON.
struct Join<'a> {
    left: Box<dyn Source + 'a>,
    right: Side<'a>,
    /// `None` for CROSS JOIN.
    on: Option<&'a Expr>,
    outer: bool,
    /// The columns of both sides, the right side's last.
    scope: Scope<'a>,
    /// How many columns the right side has.
    width: usize,
    /// For each equality of ON between the two sides: the column of the right side, and where
    /// its partner stands in a left row.
    pins: Vec<(usize, usize)>,
    ctx: &'a EvalContext<'a>,
    swapped: bool,
}

impl Source for Join<'_> {
    fn rows(&self, reservation: &mut Reservation, each: &mut Each<'_>) -> Result<()> {
        // How many leading columns the right table's own filter gives its best path. A path
        // serves the join when a left row's values give it more, or its whole key; the right
        // side is held only when no path would.
        let served = match &self.right {
            Side::Table(table) if !self.pins.is_empty() => {
                let own = table.choose(&[], &[])?.pinned;
                let any: Vec<_> = (self.pins.iter()).map(|pin| (pin.0, None)).collect();
                let path = table.given(&any, &[], &[])?;
                path.serves(own).then_some((table, own))
            }
            _ => None,
        };
        let mut kept: Option<Vec<Vec<Value>>> = None;
        let mut held = 0;
        self.left
            .rows(reservation, &mut |left, took, reservation| {
                let (matched, mut more) = (Cell::new(false), true);
                let mut pair =
                    |right: &[Value], on: Option<&Expr>, reservation: &mut Reservation| {
                        let values = left.iter().chain(right);
                        let bytes = row_overhead(left.len() + right.len())
                            + values.map(heap_bytes).sum::<u64>();
                        reservation.grow(bytes)?;
                        let row = if self.swapped {
                            [right, left.as_slice()]
                        } else {
                            [left.as_slice(), right]
                        }
                        .concat();
                        if let Some(on) = on
                            && *eval(on, &self.scope, &row, self.ctx)? != Value::Bool(true)
                        {
                            reservation.shrink(bytes);
                            return Ok(true);
                        }
                        matched.set(true);
                        each(row, bytes, reservation)
                    };
                let given: Vec<(usize, Option<&Value>)> = (self.pins.iter())
                    .map(|&(column, at)| (column, Some(&left[at])))
                    .collect();
                // Nothing equals NULL: such a left row has no partner, and reads none.
                if given.iter().all(|(_, value)| *value != Some(&Value::Null)) {
                    if let Some((table, own)) = served {
                        let mut paired =
                            |right: Vec<Value>, bytes, reservation: &mut Reservation| {
                                more = pair(&right, self.on, reservation)?;
                                reservation.shrink(bytes);
                                Ok(more)
                            };
                        // A value no path takes reads the right side through, holding none of it.
                        let path = table.given(&given, &[], &[])?;
                        match path.serves(own) {
                            true => {
                                table.walk(&path, reservation, |_, right, bytes, reservation| {
                                    paired(right, bytes, reservation)
                                })?
                            }
                            false => self.right.rows(reservation, &mut paired)?,
                        }
                    } else {
                        if kept.is_none() {
                            let mut rows = Vec::new();
                            self.right
                                .rows(reservation, &mut |row, bytes, reservation| {
                                    reservation.room(&mut rows, usize::MAX)?;
                                    held += bytes;
                                    rows.push(row);
                                    Ok(true)
                                })?;
                            kept = Some(rows);
                        }
                        for right in kept.iter().flatten() {
                            more = pair(right, self.on, reservation)?;
                            if !more {
                                break;
                            }
                        }
                    }
                }
                if self.outer && !matched.get() {
                    more = pair(&vec![Value::Null; self.width], None, reservation)?;
                }
                reservation.shrink(took);
                Ok(more)
            })?;
        reservation.shrink(held);
        Ok(())
    }
}

/// What reading only the first rows of each partition of a window borrows.
pub(super) struct Firsts<'c> {
    table: &'c Table,
    qualifier: &'c str,
    /// The window's ORDER BY, and the columns the SELECT reads.
    order: &'c [SortKey],
    wanted: Vec<bool>,
    /// For each partition asked for: the SELECT's filter with the partition's value, how
    /// many of its rows are asked for, and what the reader's filter of its source holds.
    parts: Vec<(Expr, usize, Reservation)>,
}

/// For the SELECT of a WITH name that numbers the rows of one stored table in partitions of
/// one column: the partitions that `bound`, the WHERE of the statement reading the name, asks
/// for, when every row it can pass is in a partition it names by `=` and has a number it
/// bounds by `<=`. `None` for any other SELECT or bound: then every row is numbered.
pub(super) fn firsts<'c>(
    select: &'c SelectPlan,
    bound: &Expr,
    given: &Given<'c>,
) -> Result<Option<Firsts<'c>>> {
    let [FromItem::Table { name, alias }] = select.from.as_slice() else {
        return Ok(None);
    };
    let cut = select.limit.is_some() || select.offset.is_some();
    let cut = cut || select.limit_name.is_some() || select.offset_name.is_some();
    let more = cut || select.distinct || !select.group_by.is_empty() || select.having.is_some();
    let mut windows = select
        .columns
        .iter()
        .enumerate()
        .filter_map(|(at, column)| match &column.expr {
            Expr::RowNumber {
                partition_by,
                order_by,
            } => Some((at, partition_by.as_slice(), order_by.as_slice())),
            _ => None,
        });
    let (Some((numbered, [Expr::Column(partition)], order)), None, false, Some(table)) = (
        windows.next(),
        windows.next(),
        more || !select.joins.is_empty(),
        given
            .catalog
            .table(name)
            .filter(|_| Named::find(given, name).is_none()),
    ) else {
        return Ok(None);
    };
    // The names the reading statement knows the number and the partition's column by.
    let listed = select.columns.iter().position(
        |column| matches!(&column.expr, Expr::Column(named) if named.column == partition.column),
    );
    let Some(listed) = listed else {
        return Ok(None);
    };
    let [listed, number] = [listed, numbered].map(|at| match select.column_aliases.get(at) {
        Some(alias) => alias.clone(),
        None => select.columns[at].name(),
    });

    // `bound` is one or more alternatives joined by OR, each of parts joined by AND.
    let mut alternatives = Vec::new();
    joined(bound, BinOp::Or, &mut alternatives);
    let qualifier = alias.as_ref().unwrap_or(name);
    let kind = Scope::of(qualifier, &table.meta.columns).find(partition)?.1;
    let mut asked: Vec<(Value, &Expr, usize)> = Vec::new();
    for alternative in alternatives {
        let mut parts = Vec::new();
        joined(alternative, BinOp::And, &mut parts);
        let beside = |name: &str, wanted: BinOp| {
            parts.iter().find_map(|part| match part {
                Expr::BinaryOp { left, op, right } if *op == wanted => match &**left {
                    Expr::Column(named) if named.column == name => Some(&**right),
                    _ => None,
                },
                _ => None,
            })
        };
        let (
            Some(value @ (Expr::Literal(_) | Expr::Parameter(_))),
            Some(Expr::Literal(Value::Int64(most))),
        ) = (beside(&listed, BinOp::Eq), beside(&number, BinOp::Lte))
        else {
            return Ok(None);
        };
        let most = usize::try_from(*most).unwrap_or(0);
        // A partition is one value of its column's type, however it is written.
        let key = eval(value, &Scope::default(), &[], &given.ctx)?;
        given.ctx.kept.keep(heap_bytes(&key))?;
        let Some(key) = as_key(kind, key.into_owned()) else {
            return Ok(None);
        };
        let same =
            |(asked, ..): &(Value, &Expr, usize)| compare(asked, &key) == Some(Ordering::Equal);
        match asked.iter().position(same) {
            Some(at) => asked[at].2 = asked[at].2.max(most),
            None => asked.push((key, value, most)),
        }
    }
    // Use the general window path beyond 64 copies or for a filter with a SELECT.
    let inner = select.filter.as_ref();
    let selects = |e: &Expr| matches!(e, Expr::Subquery(_) | Expr::InSubquery { .. });
    if asked.len() > 64 || inner.is_some_and(|inner| holds(inner, selects)) {
        return Ok(None);
    }
    let named = partition.column.len() + partition.table.as_ref().map_or(0, String::len);
    let width = 32 * table.meta.columns.len() + 3 * size_of::<Expr>() + named;
    let parts = asked.into_iter().map(|(_, value, most)| {
        let copy = width as u64 + copied(value) + inner.map_or(0, copied);
        let held = given.budget.reserve(copy, "query")?;
        // The partition's value first: it is the one value the path reads the column at.
        let mut filter = Expr::BinaryOp {
            left: Box::new(Expr::Column(partition.clone())),
            op: BinOp::Eq,
            right: Box::new(value.clone()),
        };
        if let Some(inner) = &select.filter {
            filter = Expr::BinaryOp {
                left: Box::new(filter),
                op: BinOp::And,
                right: Box::new(inner.clone()),
            };
        }
        Ok((filter, most, held))
    });
    Ok(Some(Firsts {
        table,
        qualifier,
        order,
        wanted: wanted(select, &Scope::of(qualifier, &table.meta.columns)),
        parts: parts.collect::<Result<_>>()?,
    }))
}

/// What a copy of `expr` holds: its nodes, and the names and values written in them.
fn copied(expr: &Expr) -> u64 {
    let text = match expr {
        Expr::Literal(value) => heap_bytes(value),
        Expr::Parameter(name) | Expr::FunctionCall { name, .. } => name.len() as u64,
        Expr::Column(c) => (c.column.len() + c.table.as_ref().map_or(0, String::len)) as u64,
        _ => 0,
    };
    text + size_of::<Expr>() as u64 + crate::sql::inside(expr).map(copied).sum::<u64>()
}

/// The rows `firsts` asks for: of each partition, the first rows in the window's order, read
/// through a key or index that gives that order. `None` when a partition has none that does.
pub(super) fn first<'a, 'c: 'a>(
    firsts: &'a mut Firsts<'c>,
    given: &'a Given<'a>,
) -> Result<Option<From<'a>>> {
    let (table, qualifier, order) = (firsts.table, firsts.qualifier, firsts.order);
    let scope = Scope::of(qualifier, &table.meta.columns);
    let width = table.meta.columns.len();
    let mut parts: Vec<Box<dyn Source + 'a>> = Vec::new();
    for (filter, most, seeing) in &mut firsts.parts {
        let filter: &'a Expr = filter;
        let mut filtered = vec![false; width];
        reads(filter, &scope, given.params, &mut filtered)?;
        let visible = Visible::new(table, given.catalog, given.reader, seeing)?;
        let table = table::Source::new(table, Some(filter), &given.ctx, given.read, visible);
        let table = table.counted(given.examined);
        let table = table.named(qualifier).reading(filtered, &firsts.wanted);
        let path = table.choose(order, &[])?;
        if !path.in_order {
            return Ok(None);
        }
        table.say(&[], order, &[])?;
        parts.push(Box::new(Cut {
            input: Box::new(Rows { table, path }),
            offset: 0,
            limit: *most,
        }));
    }
    Ok(Some(From {
        rows: Box::new(Parts(parts, &[])),
        scope,
        filter: None,
    }))
}

/// The rows of one source after another's, whole numbers REALs in the REAL columns given.
pub(super) struct Parts<'a>(
    pub(super) Vec<Box<dyn Source + 'a>>,
    pub(super) &'a [ColumnDef],
);

impl Source for Parts<'_> {
    fn rows(&self, reservation: &mut Reservation, each: &mut Each<'_>) -> Result<()> {
        let mut more = true;
        for part in &self.0 {
            if !more {
                break;
            }
            part.rows(reservation, &mut |mut row, bytes, reservation| {
                for (value, column) in row.iter_mut().zip(self.1) {
                    if let (Value::Int64(whole), ColumnType::Real) = (&*value, column.column_type) {
                        *value = Value::Float64(*whole as f64);
                    }
                }
                more = each(row, bytes, reservation)?;
                Ok(more)
            })?;
        }
        Ok(())
    }
}
