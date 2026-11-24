//! GROUP BY, the aggregates and HAVING: one row for each set of grouping values, in the order
//! of those values, and for a statement of aggregates with no GROUP BY one row over every row
//! of its input.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::btree_map::Entry;
use std::ops::Bound;

use super::bind::{bind, kind};
use super::counts::{names, undeclared, used};
use super::expression::{
    Arithmetic, EvalContext, Scope, arithmetic, compare, eval, most_bytes, reads,
};
use super::filter::{self, Columns};
use super::plan::{Each, Filter, Source, hand_on, listed};
use super::row_overhead;
use super::sort::Key;
use super::visible::Reader;
use crate::memory::Reservation;
use crate::schema::Table;
use crate::sql::ast::ColumnRef;
use crate::sql::plan::{ProjectColumn, SelectPlan, SortKey};
use crate::sql::{AGGREGATES, Expr, inside_mut, not_supported};
use crate::store::{self, CountId, Read};
use crate::types::heap_bytes;
use crate::types::{ColumnDef, ColumnType, Error, Result, SortDirection, Value};

/// How a grouped statement is answered: what its rows are grouped by, the aggregates it
/// writes, and its HAVING, ORDER BY and select list written over a row of one group, which
/// holds the grouping values and then the answer of each aggregate.
pub(super) struct Groups {
    keys: Vec<Expr>,
    /// For each grouping value, whether its groups leave greatest first.
    descending: Vec<bool>,
    calls: Vec<Call>,
    /// The columns of a group's row. Each is found by its name; no type of theirs is read.
    columns: Vec<ColumnDef>,
    having: Option<Expr>,
    order: Vec<SortKey>,
    list: Vec<ProjectColumn>,
}

/// One aggregate as it was written.
struct Call {
    function: Function,
    /// What is counted, added or compared; `None` for `COUNT(*)`.
    of: Option<Expr>,
}

#[derive(Clone, Copy, PartialEq)]
enum Function {
    Count,
    Sum,
    Min,
    Max,
    Avg,
}

impl Function {
    /// The aggregate called `name`, from the one list of them the planner has.
    fn named(name: &str) -> Option<Function> {
        let mut names = AGGREGATES.iter();
        let at = names.position(|known| known.eq_ignore_ascii_case(name))?;
        let functions = [Self::Count, Self::Sum, Self::Min, Self::Max, Self::Avg];
        Some(functions[at])
    }
}

/// A column that a stage makes for the stages above it, and the expression that reads it.
pub(super) fn made(table: Option<&str>, name: &str) -> (ColumnDef, Expr) {
    let column = ColumnDef {
        name: name.to_owned(),
        column_type: ColumnType::Integer,
        nullable: true,
        primary_key: false,
        unique: false,
        default: None,
    };
    let read = Expr::Column(ColumnRef {
        table: table.map(str::to_owned),
        column: name.to_owned(),
    });
    (column, read)
}

impl Groups {
    /// The grouping of a SELECT that has GROUP BY, HAVING or an aggregate; `None` for any
    /// other. Everything it names is bound against `scope`, the columns of the rows it reads,
    /// and each column it reads is marked in `wanted`. An item that is neither a grouping
    /// expression nor inside an aggregate is refused here, whatever rows there are.
    ///
    /// Groups leave in the order of their grouping values, each going up unless a count list
    /// of `table` declares that column DESC, NULL last.
    pub(super) fn of(
        select: &SelectPlan,
        scope: &Scope,
        table: Option<&Table>,
        params: &HashMap<String, Value>,
        wanted: &mut [bool],
    ) -> Result<Option<Groups>> {
        let mut groups = Groups {
            keys: select.group_by.clone(),
            descending: Vec::new(),
            calls: Vec::new(),
            columns: Vec::new(),
            having: select.having.clone(),
            order: select.order_by.clone(),
            list: select.columns.clone(),
        };
        for key in &groups.keys {
            reads(key, scope, params, wanted)?;
            let at = match key {
                Expr::Column(column) => Some(scope.find(column)?.0),
                _ => None,
            };
            let mut declared = table.into_iter().flat_map(|table| {
                table
                    .rules
                    .counts
                    .iter()
                    .flat_map(move |c| table.count_columns(c))
            });
            let declared = declared.find(|(column, _)| Some(*column) == at);
            let descending = declared.is_some_and(|(_, way)| *way == SortDirection::Desc);
            groups.descending.push(descending);
        }
        let written: Vec<String> = groups.keys.iter().map(|key| spelled(key, scope)).collect();
        // What stands outside every group is refused only once the statement is known to be
        // grouped: an aggregate may be written after it.
        let mut outside = None;
        let mut over = |expr: &mut Expr| {
            let calls = &mut groups.calls;
            over_group(expr, &written, calls, (scope, params), wanted, &mut outside)
        };
        groups.having.iter_mut().try_for_each(&mut over)?;
        let order = groups.order.iter_mut().map(|key| &mut key.expr);
        let list = groups.list.iter_mut().map(|column| &mut column.expr);
        order.chain(list).try_for_each(&mut over)?;
        if groups.keys.is_empty() && groups.calls.is_empty() && groups.having.is_none() {
            return Ok(None);
        }
        outside.map_or(Ok(()), Err)?;
        // What is written over the groups is bound as it was written, each aggregate as a
        // value of the type of its answer: a comparison of types that do not compare is
        // refused here.
        let order = select.order_by.iter().map(|key| &key.expr);
        let list = select.columns.iter().map(|column| &column.expr);
        for expr in select.having.iter().chain(order).chain(list) {
            let mut expr = expr.clone();
            typed(&mut expr);
            bind(&expr, scope, params)?;
        }
        let width = groups.keys.len() + groups.calls.len();
        groups.columns = (0..width).map(|at| made(None, &at.to_string()).0).collect();
        Ok(Some(groups))
    }

    /// True when the answer is known from the number of rows alone: no GROUP BY, and every
    /// aggregate a `COUNT(*)`.
    pub(super) fn counts_every_row(&self) -> bool {
        let every_row = self.calls.iter().all(|call| call.of.is_none());
        self.keys.is_empty() && every_row && !self.calls.is_empty()
    }

    /// The kept count of `table` that holds one record for each group, when there is one and
    /// its records are the answer: every aggregate is `COUNT(*)`, the grouping expressions are
    /// the last columns of the count's list, and `filter` gives one value that is not NULL for
    /// each column before them and says nothing else.
    ///
    /// `None` when the rows have to be read: for a reader that is not shown every row, for a
    /// table the transaction has staged a row of, and for a list that declares a column DESC,
    /// whose records do not give their values back.
    pub(super) fn kept<'a>(
        &self,
        table: &Table,
        scope: &Scope,
        filter: Option<&Expr>,
        reader: &Reader,
        ctx: &EvalContext,
        read: &'a dyn Read,
    ) -> Result<Option<Kept<'a>>> {
        let counted = self.calls.iter().all(|call| call.of.is_none());
        if self.keys.is_empty() || !counted || !reader.is_admin() {
            return Ok(None);
        }
        let mut grouped = Vec::new();
        for key in &self.keys {
            let Expr::Column(column) = key else {
                return Ok(None);
            };
            grouped.push(scope.find(column)?.0);
        }
        let reading = filter::read(
            filter,
            &Columns {
                scope,
                table: Some(table),
            },
            ctx,
        )?;
        let Some(case) = filter::pins(&reading) else {
            return Ok(None);
        };
        let say = |line: &dyn Fn() -> String| ctx.said.map(|said| said.borrow_mut().push(line()));
        // The one value the filter gives a column, when it gives it nothing else.
        let given = |column: usize| match case.iter().find(|(at, _)| *at == column) {
            Some((_, Some(value))) => Some((*value).clone()),
            _ => None,
        };
        for count in &table.rules.counts {
            let columns = table.count_columns(count);
            let Some(given_for) = columns.len().checked_sub(grouped.len()) else {
                continue;
            };
            let (first, last) = columns.split_at(given_for);
            let values: Option<Vec<Value>> = first.iter().map(|column| given(column.0)).collect();
            let places = grouped
                .iter()
                .map(|at| last.iter().position(|column| column.0 == *at));
            let places: Option<Vec<usize>> = places.collect();
            let places = places.filter(|places| (0..places.len()).all(|at| places.contains(&at)));
            let ascending = columns.iter().all(|column| column.1 == SortDirection::Asc);
            let pinned = (case.iter()).all(|(at, _)| first.iter().any(|column| column.0 == *at));
            if let (Some(values), Some(places), true, true) = (values, places, ascending, pinned) {
                say(&|| used(table, columns));
                return Ok(Some(Kept {
                    read,
                    count: count.id,
                    given: store::Key::from_values(&values, &[])?,
                    places: places.into_iter().map(|at| given_for + at).collect(),
                }));
            }
        }
        let wanted = case.iter().map(|(at, _)| *at).chain(grouped);
        say(&|| undeclared(table, &names(table, wanted.clone())));
        Ok(None)
    }
}

/// The records of a kept count that are the groups of a statement.
pub(super) struct Kept<'a> {
    read: &'a dyn Read,
    count: CountId,
    /// What every record of these groups begins with: the values the filter gives.
    given: store::Key,
    /// For each grouping value, where it is among the values of a record.
    places: Vec<usize>,
}

/// How a written expression is told from another: a column by where it sits in a row, whatever
/// qualifier it was written with, and anything else by how it is written.
pub(super) fn spelled(expr: &Expr, scope: &Scope) -> String {
    let at = match expr {
        Expr::Column(column) => scope.find(column).ok(),
        _ => None,
    };
    at.map_or_else(|| format!("{expr:?}"), |(at, _)| format!("#{at}"))
}

/// Puts in place of each aggregate inside `expr` something of the type of its answer, so that
/// what is written over the groups binds as any expression does.
pub(super) fn typed(expr: &mut Expr) {
    let function = match expr {
        Expr::FunctionCall { name, .. } => Function::named(name),
        _ => None,
    };
    *expr = match (function, &mut *expr) {
        (Some(Function::Count), _) => Expr::Literal(Value::Int64(0)),
        (Some(Function::Avg), _) => Expr::Literal(Value::Float64(0.0)),
        (Some(_), Expr::FunctionCall { args, .. }) if args.len() == 1 => args.remove(0),
        (_, other) => return inside_mut(other).for_each(typed),
    };
}

/// Writes `expr` over a group's row: a grouping expression becomes the column of its value,
/// and an aggregate, which joins `calls`, the column of its answer. `AVG` is the sum of the
/// values as REALs over the count of those that are not NULL. The refusal of the first thing
/// that is neither, a column or a window, becomes `outside`.
fn over_group(
    expr: &mut Expr,
    keys: &[String],
    calls: &mut Vec<Call>,
    (scope, params): (&Scope, &HashMap<String, Value>),
    wanted: &mut [bool],
    outside: &mut Option<Error>,
) -> Result<()> {
    if let Some(at) = keys.iter().position(|key| *key == spelled(expr, scope)) {
        *expr = made(None, &at.to_string()).1;
        return Ok(());
    }
    let function = match expr {
        Expr::FunctionCall { name, .. } => Function::named(name),
        _ => None,
    };
    let (function, name, args) = match (function, &mut *expr) {
        (Some(function), Expr::FunctionCall { name, args }) => (function, name, args),
        (_, Expr::Column(column)) => {
            outside.get_or_insert_with(|| {
                Error::PlanError(format!(
                    "the column {} must be in GROUP BY or inside an aggregate",
                    column.column
                ))
            });
            return Ok(());
        }
        (_, Expr::RowNumber { .. }) => {
            outside.get_or_insert_with(|| {
                not_supported("ROW_NUMBER() OVER beside GROUP BY or an aggregate")
            });
            return Ok(());
        }
        (_, other) => {
            let mut inner = inside_mut(other);
            let given = (scope, params);
            return inner
                .try_for_each(|inner| over_group(inner, keys, calls, given, wanted, outside));
        }
    };
    let [of] = args.as_slice() else {
        return Err(Error::PlanError(format!("{name} takes one argument")));
    };
    let every_row =
        function == Function::Count && matches!(of, Expr::Column(column) if column.column == "*");
    if !every_row {
        reads(of, scope, params, wanted)?;
    }
    // A SUM over a column that does not hold numbers is refused whatever rows there are.
    if let (Function::Sum | Function::Avg, Expr::Column(column)) = (function, of)
        && !matches!(
            scope.find(column)?.1,
            ColumnType::Integer | ColumnType::Real
        )
    {
        return Err(Error::PlanError(format!(
            "{} needs numbers, and the column {} holds none",
            name.to_uppercase(),
            column.column
        )));
    }
    if let Function::Min | Function::Max = function
        && let Some(ColumnType::Vector(_)) = kind(of, scope, params)?
    {
        let refused = format!("function {}(vector) does not exist", name.to_lowercase());
        return Err(Error::PlanError(refused));
    }
    let of = (!every_row).then(|| of.clone());
    let mut call = |function, of| {
        calls.push(Call { function, of });
        made(None, &(keys.len() + calls.len() - 1).to_string()).1
    };
    *expr = match function {
        Function::Avg => {
            let operator = |name: &str, args| Expr::FunctionCall {
                name: name.to_owned(),
                args,
            };
            let real = |of| operator("__mul", vec![of, Expr::Literal(Value::Float64(1.0))]);
            let sum = call(Function::Sum, of.clone().map(real));
            operator("__div", vec![sum, call(Function::Count, of)])
        }
        function => call(function, of),
    };
    Ok(())
}

/// The rows of a grouped statement: its input, whose columns are `scope`, taken into one entry
/// for each group, then HAVING, ORDER BY and the select list over the groups. Reads every row
/// of its input and holds the row it is taking in and, for each group, its grouping values and
/// each aggregate's answer so far. With `counted`, the number of rows the input has, or `kept`,
/// the count records that are its groups, the input is not read.
pub(super) struct Grouped<'a> {
    pub(super) input: Box<dyn Source + 'a>,
    pub(super) scope: Scope<'a>,
    pub(super) groups: Groups,
    pub(super) ctx: &'a EvalContext<'a>,
    pub(super) counted: Option<u64>,
    pub(super) kept: Option<Kept<'a>>,
    /// How many rows can leave, as [`Sort`](super::sort::Sort) has it.
    pub(super) most: usize,
}

impl Source for Grouped<'_> {
    fn rows(&self, reservation: &mut Reservation, each: &mut Each<'_>) -> Result<()> {
        let groups = &self.groups;
        let (scope, ctx) = (Scope::of("", &groups.columns), self.ctx);
        let mut rows: Box<dyn Source + '_> = Box::new(Entries(self));
        if let Some(filter) = &groups.having {
            let scope = scope.clone();
            rows = Box::new(Filter {
                input: rows,
                scope,
                filter,
                ctx,
            });
        }
        let written = (groups.order.as_slice(), groups.list.as_slice());
        listed(rows, scope, written, ctx, self.most).rows(reservation, each)
    }
}

/// One row for each group of a grouped statement, in the order of the grouping values: those
/// values, then the answer of each aggregate.
struct Entries<'s, 'a>(&'s Grouped<'a>);

impl Source for Entries<'_, '_> {
    fn rows(&self, reservation: &mut Reservation, each: &mut Each<'_>) -> Result<()> {
        let Grouped {
            input,
            scope,
            groups,
            ctx,
            counted,
            kept,
            ..
        } = self.0;
        let fresh = |seen| -> Vec<Running> {
            let calls = groups.calls.iter();
            calls.map(|_| Running::seen(seen)).collect()
        };
        // What a group holds before any least or greatest value: its row and its entry here.
        let entry =
            row_overhead(groups.columns.len()) + crate::memory::node::<(Key<'_, ()>, Held)>();
        let mut held: BTreeMap<Key<'_, ()>, Held> = BTreeMap::new();
        reservation.grow(3 * crate::memory::node::<(Key<'_, ()>, Held)>())?;
        if let Some(kept) = kept {
            let damaged = || {
                kept.read
                    .damaged("a kept count's record does not hold its values")
            };
            let end = kept.given.prefix_end();
            let end = end.as_ref().map_or(Bound::Unbounded, Bound::Excluded);
            for record in kept
                .read
                .kept(kept.count, Bound::Included(&kept.given), end)?
            {
                let (key, rows) = record?;
                let values = key.values().ok_or_else(damaged)?;
                let places = kept.places.iter();
                let values: Option<Vec<Value>> =
                    places.map(|at| values.get(*at).cloned()).collect();
                let values = values.ok_or_else(damaged)?;
                let beside = entry + values.iter().map(heap_bytes).sum::<u64>();
                reservation.grow(beside)?;
                held.insert(
                    Key(values, &groups.descending, ()),
                    (fresh(rows as i64), beside),
                );
            }
        } else if counted.is_none() {
            input.rows(reservation, &mut |row, bytes, reservation| {
                let keys = groups.keys.iter();
                let most = keys.map(|key| most_bytes(key, scope, &row, ctx));
                let beside = entry + most.sum::<u64>();
                reservation.grow(beside)?;
                let keys = groups.keys.iter();
                let values = keys
                    .map(|key| eval(key, scope, &row, ctx).map(std::borrow::Cow::into_owned))
                    .collect::<Result<Vec<_>>>()?;
                let (running, _) = match held.entry(Key(values, &groups.descending, ())) {
                    Entry::Vacant(new) => new.insert((fresh(0), beside)),
                    Entry::Occupied(found) => {
                        reservation.shrink(beside);
                        found.into_mut()
                    }
                };
                for (call, running) in groups.calls.iter().zip(running) {
                    running.add(call, scope, &row, ctx, reservation)?;
                }
                reservation.shrink(bytes);
                Ok(true)
            })?;
        }
        // Without GROUP BY every row is in the one group, which is there with no row too.
        if groups.keys.is_empty() && held.is_empty() {
            reservation.grow(entry)?;
            let seen = counted.unwrap_or(0) as i64;
            held.insert(Key(Vec::new(), &[], ()), (fresh(seen), entry));
        }
        let rows = held
            .into_iter()
            .map(|(Key(mut row, _, ()), (running, beside))| {
                let values = running.iter().map(|running| running.held);
                let bytes = beside + values.sum::<u64>();
                row.extend(groups.calls.iter().zip(running).map(Running::answer));
                (row, bytes, 0)
            });
        hand_on(rows, reservation, each)?;
        Ok(())
    }
}

/// What is held for one group beside its grouping values: each aggregate's answer so far, and
/// the bytes of the statement's reservation that the group's entry holds.
type Held = (Vec<Running>, u64);

/// The answer of one aggregate over the rows of one group seen so far.
struct Running {
    /// How many rows took part: every row for `COUNT(*)`, otherwise those where the value is
    /// not NULL.
    seen: i64,
    /// The sum, the least or the greatest so far; NULL until a row takes part.
    value: Value,
    /// The bytes of the statement's reservation that `value` holds.
    held: u64,
}

impl Running {
    fn seen(seen: i64) -> Running {
        Running {
            seen,
            value: Value::Null,
            held: 0,
        }
    }

    /// Takes one row into the answer. A NULL takes no part in any aggregate but `COUNT(*)`.
    /// The memory of the value is asked for before it is made, and given back unless the value
    /// is kept as the least or the greatest so far.
    fn add(
        &mut self,
        call: &Call,
        scope: &Scope,
        row: &[Value],
        ctx: &EvalContext,
        reservation: &mut Reservation,
    ) -> Result<()> {
        if let Some(of) = &call.of {
            let most = most_bytes(of, scope, row, ctx);
            reservation.grow(most)?;
            let value = eval(of, scope, row, ctx)?.into_owned();
            if value == Value::Null {
                reservation.shrink(most);
                return Ok(());
            }
            let first = self.value == Value::Null;
            let better = match call.function {
                Function::Count | Function::Avg => None,
                Function::Sum => {
                    let so_far = if first { &Value::Int64(0) } else { &self.value };
                    self.value = arithmetic(Arithmetic::Add, so_far, &value)?;
                    None
                }
                Function::Min => Some(Ordering::Less),
                Function::Max => Some(Ordering::Greater),
            };
            if better.is_some_and(|better| first || compare(&value, &self.value) == Some(better)) {
                reservation.shrink(self.held);
                (self.value, self.held) = (value, most);
            } else {
                reservation.shrink(most);
            }
        }
        self.seen += 1;
        Ok(())
    }

    /// The answer over the rows taken: a count of nothing is zero, and every other aggregate of
    /// nothing is NULL.
    fn answer((call, running): (&Call, Running)) -> Value {
        match call.function {
            Function::Count => Value::Int64(running.seen),
            _ => running.value,
        }
    }
}
