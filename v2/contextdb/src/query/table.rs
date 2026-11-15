//! The one source of a table's rows: which rows a statement has to read, and reading them
//! through the reader's filter and the statement's own.

use std::cell::{OnceCell, RefCell};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::ops::Bound::{self, Unbounded};

use super::expression::{EvalContext, Scope, eval, sort_order};
use super::row_overhead;
use super::visible::Visible;
use crate::memory::Reservation;
use crate::schema::{Table, field_key};
use crate::sql::Expr;
use crate::sql::ast::{BinOp, ColumnRef, Literal, SortDirection as Asked};
use crate::sql::plan::SortKey;
use crate::store::{Cell, Entry, Key, Keys, Read, Row};
use crate::types::{ColumnType, Result, SortDirection, Value, heap_bytes};

/// The keys or entries between two bounds.
type Range = (Bound<Key>, Bound<Key>);

/// Where the rows of one table are reached.
enum Reach {
    /// The rows with these primary keys, in key order.
    Keys(Vec<Key>),
    /// The table between each pair of keys, in key order.
    Rows(Vec<Range>),
    /// The rows that the entries of the index at this place among the table's indexes point
    /// at, between each pair of entries, in entry order.
    Index(usize, Vec<Range>),
}

/// How the rows of one table are reached for one statement.
pub(super) struct Path {
    reach: Reach,
    /// True when the statement names one row whole, by its key or by every column of a UNIQUE
    /// rule: a reader that row is hidden from is refused by the rule that hid it.
    pub(super) one: bool,
    /// True when the rows arrive in the order asked for, so that they need no sort.
    pub(super) in_order: bool,
    /// How many leading columns of the key or index it reads are given one value each, or any
    /// values when they make the whole key, whose rows a list names exactly.
    pub(super) pinned: usize,
    /// The columns of that order. A run of entries that hold cut values is in the order of
    /// their digests, and is put in the order of the values themselves before it is handed on.
    order: Vec<(usize, SortDirection)>,
}

impl Path {
    /// True when a row's values give this path the whole key, or more than `own` columns.
    pub(super) fn serves(&self, own: usize) -> bool {
        matches!(self.reach, Reach::Keys(_)) || self.pinned > own
    }
}

/// A part of a filter that every kept row must pass, and that compares a column with values
/// known before any row is read: one value, or for `=` the values of an `IN` list.
pub(super) struct Term {
    /// The column; for the field an index over a JSON path keeps, the table's number of columns
    /// and the index's place: a column only that index has.
    pub(super) column: usize,
    pub(super) op: BinOp,
    pub(super) values: Vec<Value>,
    /// True for `column = value`, which names one value.
    one: bool,
}

/// A key or index tried for a path, with how many of its leading columns were given a value and
/// whether the next was given a range.
type Tried = (Ordered, usize, bool);

/// Something that keeps the rows of a table in an order: the table itself, under its primary
/// key, or one of its indexes.
struct Ordered {
    /// Where the index is among the table's indexes; `None` for the table itself.
    index: Option<usize>,
    /// True when no two rows have the same values in all of its columns.
    unique: bool,
    /// The columns it is ordered by, each with its direction.
    columns: Vec<(usize, SortDirection)>,
}

impl Ordered {
    /// What these leading values make: the bytes every key or entry with these values begins
    /// with. A row key is whole and all ascending; an entry is made as its index makes it.
    fn key(&self, table: &Table, values: &[Value]) -> Result<Entry> {
        match self.index {
            Some(at) => table.entry_shape(at).entry(values),
            None => Ok(Entry {
                key: Key::from_values(values, &[])?,
                cut: None,
            }),
        }
    }

    /// What the filter leaves of the keys that begin with `given`: what `given` makes, the
    /// range, and whether the filter gives a range of the next column.
    fn range(
        &self,
        table: &Table,
        given: &[Value],
        terms: &[Term],
    ) -> Result<(Entry, Range, bool)> {
        let all = self.key(table, given)?;
        let (mut from, mut to) = (Bound::Unbounded, Bound::Unbounded);
        if !given.is_empty() {
            to = until_after(&all.key);
            from = Bound::Included(all.key.clone());
        }
        let mut ranged = false;
        // Behind a cut value nothing says where the next column's values lie.
        let next = self.columns.get(given.len()).filter(|_| all.cut.is_none());
        if let Some((column, direction)) = next {
            // The key of a bound, and whether the bound is a cut value. A cut bound is cut back
            // to its run, the entries that share its kept bytes, and the filter leaves out the
            // rows of the run that are outside the range.
            let with = |value: &Value| -> Result<(Key, bool)> {
                let entry = self.key(table, &[given, std::slice::from_ref(value)].concat())?;
                Ok(match entry.cut {
                    Some(cut) => (Key::from_bytes(&entry.key.as_bytes()[..cut + 2]), true),
                    None => (entry.key, false),
                })
            };
            let ranges = terms.iter().filter(|term| {
                let range = matches!(term.op, BinOp::Gt | BinOp::Gte | BinOp::Lt | BinOp::Lte);
                term.column == *column && range
            });
            for term in ranges {
                if !ranged {
                    // No NULL is in a range. A key keeps NULL after every value going up
                    // and before every value going down, and the walk stops short of it.
                    let null = with(&Value::Null)?.0;
                    match direction {
                        SortDirection::Asc => to = Bound::Excluded(null),
                        SortDirection::Desc => from = starting_after(null),
                    }
                    ranged = true;
                }
                let (key, cut) = with(&term.values[0])?;
                // A descending column keeps its greater values first. When more columns
                // follow, many keys begin with this one, and "after it" is after them all.
                match (term.op, direction, cut) {
                    (BinOp::Gt, SortDirection::Asc, false)
                    | (BinOp::Lt, SortDirection::Desc, false) => from = starting_after(key),
                    (BinOp::Gt | BinOp::Gte, SortDirection::Asc, _)
                    | (BinOp::Lt | BinOp::Lte, SortDirection::Desc, _) => {
                        from = Bound::Included(key);
                    }
                    (BinOp::Lt, SortDirection::Asc, false)
                    | (BinOp::Gt, SortDirection::Desc, false) => to = Bound::Excluded(key),
                    // `<=` going up and `>=` going down, and the far end of a run.
                    _ => to = until_after(&key),
                }
            }
        }
        Ok((all, (from, to), ranged))
    }
}

/// The bound that ends a walk after the last key that begins with this one.
fn until_after(key: &Key) -> Bound<Key> {
    key.prefix_end().map_or(Bound::Unbounded, Bound::Excluded)
}

/// The bound that starts a walk after the last key that begins with this one.
fn starting_after(key: Key) -> Bound<Key> {
    let after = key.prefix_end();
    after.map_or(Bound::Excluded(key), Bound::Included)
}

/// The value as a key of a column of this type holds it, when there is one such value: only
/// then is looking the value up the same as comparing every row with it.
pub(super) fn as_key(column: ColumnType, value: Value) -> Option<Value> {
    match (column, value) {
        (ColumnType::Integer, value @ Value::Int64(_))
        | (ColumnType::Real, value @ Value::Float64(_))
        | (ColumnType::Text, value @ Value::Text(_))
        | (ColumnType::Boolean, value @ Value::Bool(_))
        | (ColumnType::Uuid, value @ Value::Uuid(_))
        | (ColumnType::Timestamp, value @ Value::Timestamp(_))
        | (ColumnType::TxId, value @ Value::TxId(_)) => Some(value),
        (ColumnType::Real, Value::Int64(value)) if (value as f64) as i128 == i128::from(value) => {
            Some(Value::Float64(value as f64))
        }
        (ColumnType::Uuid, Value::Text(value)) => value.parse().ok().map(Value::Uuid),
        _ => None,
    }
}

/// True for a value known before any row: a literal, a `$name`, and a SELECT when it is run.
pub(super) fn known(value: &Expr, ctx: &EvalContext) -> bool {
    let ran = ctx.said.is_none() && matches!(value, Expr::Subquery(_));
    ran || matches!(value, Expr::Literal(_) | Expr::Parameter(_))
}

/// Gathers the terms of a filter: the comparisons, `IS NULL` and `IN` lists joined by AND at
/// its top, its columns named as `scope` names the table's, and the values of a SELECT after
/// `IN`. Answers whether the filter is nothing but the terms gathered.
pub(super) fn collect(
    table: &Table,
    scope: &Scope,
    filter: &Expr,
    ctx: &EvalContext,
    terms: &mut Vec<Term>,
) -> Result<bool> {
    let worked = |value: &Expr| eval(value, scope, &[], ctx);
    let (column, op, mut values, one) = match filter {
        Expr::BinaryOp { left, op, right } if *op == BinOp::And => {
            let left = collect(table, scope, left, ctx, terms)?;
            return Ok(collect(table, scope, right, ctx, terms)? && left);
        }
        Expr::BinaryOp { left, op, right } => match (&**left, &**right, op) {
            (_, _, BinOp::Neq | BinOp::Or) => return Ok(false),
            // `doc -> 'k' -> 'j' = value` is a term of the index over that path, if there is one.
            (mut json @ Expr::Field { .. }, value, BinOp::Eq) if known(value, ctx) => {
                let mut path = Vec::new();
                while let Expr::Field {
                    json: of,
                    key,
                    as_text: false,
                } = json
                    && let Expr::Literal(Literal::Text(key)) = &**key
                {
                    path.insert(0, key);
                    json = of;
                }
                let Expr::Column(column) = json else {
                    return Ok(false);
                };
                let name = &table.meta.columns[scope.find(column)?.0].name;
                let place = table.meta.indexes.iter().position(|index| {
                    index.json_path.iter().eq(path.iter().copied()) && index.columns[0].0 == *name
                });
                let field = match worked(value)? {
                    Value::Text(text) => serde_json::from_str(&text).ok(),
                    Value::Json(field) => Some(field),
                    _ => None,
                };
                let (Some(place), Some(key)) = (place, field.as_ref().and_then(field_key)) else {
                    return Ok(false);
                };
                terms.push(Term {
                    column: table.meta.columns.len() + place,
                    op: BinOp::Eq,
                    values: vec![key],
                    one: true,
                });
                return Ok(true);
            }
            (column @ Expr::Column(_), value, op) if known(value, ctx) => {
                (column, *op, vec![worked(value)?], true)
            }
            // `5 < id` is `id > 5`.
            (value, column @ Expr::Column(_), op) if known(value, ctx) => {
                let turned = match op {
                    BinOp::Lt => BinOp::Gt,
                    BinOp::Lte => BinOp::Gte,
                    BinOp::Gt => BinOp::Lt,
                    BinOp::Gte => BinOp::Lte,
                    other => *other,
                };
                (column, turned, vec![worked(value)?], true)
            }
            _ => return Ok(false),
        },
        Expr::IsNull { expr, negated } => match &**expr {
            Expr::Column(column) if !negated => {
                terms.push(Term {
                    column: scope.find(column)?.0,
                    op: BinOp::Eq,
                    values: vec![Value::Null],
                    one: false,
                });
                return Ok(true);
            }
            _ => return Ok(false),
        },
        Expr::InList {
            expr,
            list,
            negated: false,
        } if list.iter().all(|value| known(value, ctx)) => {
            let values = list.iter().map(worked).collect::<Result<_>>()?;
            (&**expr, BinOp::Eq, values, false)
        }
        Expr::InSubquery {
            expr,
            query,
            negated: false,
        } if ctx.said.is_none() => (&**expr, BinOp::Eq, ctx.subquery(query)?.to_vec(), false),
        _ => return Ok(false),
    };
    let Expr::Column(column) = column else {
        return Ok(false);
    };
    let column = scope.find(column)?.0;
    // A NULL in a list equals nothing.
    values.retain(|value| one || *value != Value::Null);
    let kind = table.meta.columns[column].column_type;
    let Some(values) = values.into_iter().map(|key| as_key(kind, key)).collect() else {
        return Ok(false);
    };
    terms.push(Term {
        column,
        op,
        values,
        one,
    });
    Ok(true)
}

/// Why a key or index none of whose leading columns `filter` gives a value lost, when a part of
/// it reads `column` in a way no key or index serves.
fn residual(filter: &Expr, column: &str) -> Option<&'static str> {
    fn reads(expr: &Expr, column: &str) -> bool {
        let mut inner = crate::sql::inside(expr).into_iter();
        matches!(expr, Expr::Column(c) if c.column == column) || inner.any(|e| reads(e, column))
    }
    let known = |expr: &Expr| matches!(expr, Expr::Literal(_) | Expr::Parameter(_));
    match filter {
        Expr::BinaryOp { left, op, right } if *op == BinOp::And => {
            residual(left, column).or_else(|| residual(right, column))
        }
        _ if !reads(filter, column) => None,
        Expr::Like { .. } => Some("LIKE is residual-only"),
        Expr::BinaryOp { op: BinOp::Or, .. } => Some("OR gives no single range"),
        Expr::BinaryOp { left, right, .. } => match (&**left, &**right) {
            (Expr::FunctionCall { name, .. }, _) | (_, Expr::FunctionCall { name, .. }) => {
                Some(match name.starts_with("__") {
                    true => "arithmetic in predicate",
                    false => "function call in predicate",
                })
            }
            (side, other) if known(side) || known(other) => None,
            _ => Some("non-literal rhs"),
        },
        _ => None,
    }
}

/// A row a walk holds back: its key, its values, the cells not taken from it, and the bytes
/// its values hold.
type Held = (Key, Vec<Value>, Vec<Cell>, u64);

/// The bytes a held row takes beside its values.
fn held_bytes(key: &Key) -> u64 {
    (size_of::<Held>() + key.as_bytes().len()) as u64
}

/// One table as one statement reads it. Every row a statement reads or changes comes from
/// here, so every one has passed the reader's filter.
pub(super) struct Source<'a> {
    table: &'a Table,
    /// The table's columns under the name the statement gives the table.
    scope: Scope<'a>,
    filter: Option<&'a Expr>,
    /// The terms of the filter, gathered once and shared by every path tried.
    terms: OnceCell<Vec<Term>>,
    /// What the filter is worked out with beside a row; the caller's, so that it holds
    /// whatever the statement worked out before any row.
    ctx: &'a EvalContext<'a>,
    read: &'a dyn Read,
    /// The reader's filter, which remembers the grants it has looked up; `None` when it reads
    /// no column of a row, and so hides none.
    visible: Option<RefCell<Visible<'a>>>,
    /// The columns the reader's filter reads: taken from every row that is read.
    seen: Vec<bool>,
    /// The other columns the statement's filter reads: taken from every row the reader may see.
    filtered: Vec<bool>,
    /// The other columns the statement reads: taken only from a row that passed both filters.
    rest: Vec<bool>,
}

impl<'a> Source<'a> {
    /// The source of `table` for a statement with this filter, read through `visible`.
    pub(super) fn new(
        table: &'a Table,
        filter: Option<&'a Expr>,
        ctx: &'a EvalContext<'a>,
        read: &'a dyn Read,
        visible: Visible<'a>,
    ) -> Source<'a> {
        let width = table.meta.columns.len();
        let mut seen = vec![false; width];
        visible.reads(&mut seen);
        Source {
            table,
            scope: table.scope(),
            filter,
            terms: OnceCell::new(),
            ctx,
            read,
            visible: seen.contains(&true).then(|| RefCell::new(visible)),
            seen,
            filtered: vec![false; width],
            rest: vec![false; width],
        }
    }

    /// The source whose filter names the table's columns under `qualifier`, the name the
    /// statement gives the table, in place of the table's own.
    pub(super) fn named(mut self, qualifier: &'a str) -> Source<'a> {
        self.scope = Scope::of(qualifier, &self.table.meta.columns);
        self
    }

    /// `filtered` marks the columns the filter reads and `wanted` every column the statement
    /// reads after it. A column in neither is never taken from a row, so a large value in it
    /// is never loaded.
    pub(super) fn reading(mut self, filtered: Vec<bool>, wanted: &[bool]) -> Source<'a> {
        let columns = 0..filtered.len();
        let rest = columns.map(|at| wanted[at] && !filtered[at] && !self.seen[at]);
        self.rest = rest.collect();
        let filtered = filtered.iter().zip(&self.seen);
        self.filtered = filtered
            .map(|(filtered, seen)| *filtered && !*seen)
            .collect();
        self
    }

    /// Picks the path that reads the fewest rows for the filter, and says whether its rows
    /// arrive in the order asked for, so that they need no sort.
    ///
    /// The table and each index are tried in turn. One is better the more of its leading
    /// columns the filter gives a value for (`=`, `IS NULL`, and one `IN` list), then if the
    /// filter gives a range of its next column, then if it keeps the order asked for; the table
    /// itself wins a tie. The filter is still applied to every row read, so a path only has to
    /// reach every row that can pass. An index over a column of `assigned` is not tried: the
    /// statement moves its entries.
    ///
    /// Rows are kept under their primary key, so the key is answered from the table itself. Its
    /// own index, `__pk_<column>`, is declared and has no entries, and is never walked.
    pub(super) fn choose(&self, order: &[SortKey], assigned: &[usize]) -> Result<Path> {
        self.given(&[], order, assigned)
    }

    /// [`Source::choose`] for the rows whose column at each place of `given` equals the value
    /// beside it, as well as passing the filter: a join gives the values of its left row. A
    /// value that is no key of its column gives nothing to the path; `None` stands for the
    /// value a row will give, when the path is said before any row is read.
    pub(super) fn given(
        &self,
        given: &[(usize, Option<&Value>)],
        order: &[SortKey],
        assigned: &[usize],
    ) -> Result<Path> {
        Ok(self.scored(given, order, assigned)?.0)
    }

    /// The path [`Source::given`] picks, and each key or index tried: first the one the path
    /// reads, when one did better than the whole table.
    fn scored(
        &self,
        given: &[(usize, Option<&Value>)],
        order: &[SortKey],
        assigned: &[usize],
    ) -> Result<(Path, Vec<Tried>)> {
        let own = match self.terms.get() {
            Some(own) => own,
            None => {
                let mut own = Vec::new();
                if let Some(filter) = self.filter {
                    collect(self.table, &self.scope, filter, self.ctx, &mut own)?;
                }
                self.terms.get_or_init(|| own)
            }
        };
        let (columns, mut terms) = (&self.table.meta.columns, Vec::new());
        for &(column, value) in given {
            let value = value.map_or(Some(Value::Null), |value| {
                as_key(columns[column].column_type, value.clone())
            });
            terms.extend(value.map(|value| Term {
                column,
                op: BinOp::Eq,
                values: vec![value],
                one: false,
            }));
        }
        let column_named = |name: &String| columns.iter().position(|column| column.name == *name);
        // The order asked for, when it is plain columns.
        let asked: Option<Vec<(usize, SortDirection)>> = order
            .iter()
            .map(|key| {
                let Expr::Column(column) = &key.expr else {
                    return None;
                };
                let direction = match key.direction {
                    Asked::Asc => SortDirection::Asc,
                    Asked::Desc => SortDirection::Desc,
                };
                Some((self.scope.find(column).ok()?.0, direction))
            })
            .collect();

        let the_table = Ordered {
            index: None,
            unique: true,
            columns: (self.table.key.iter())
                .map(|at| (*at, SortDirection::Asc))
                .collect(),
        };
        let indexes = self.table.meta.indexes.iter().enumerate();
        let indexes = indexes
            .filter(|(_, index)| !index.name.starts_with("__pk_"))
            .filter(|(_, index)| {
                let mut columns = index.columns.iter().map(|(name, _)| column_named(name));
                !columns.any(|at| at.is_some_and(|at| assigned.contains(&at)))
            })
            .map(|(at, index)| Ordered {
                index: Some(at),
                unique: index.name.starts_with("__unique_"),
                columns: match index.json_path.is_empty() {
                    true => (index.columns.iter())
                        .filter_map(|(name, direction)| Some((column_named(name)?, *direction)))
                        .collect(),
                    false => vec![(columns.len() + at, SortDirection::Asc)],
                },
            });

        // Until something better is found: the whole table, in no order that was asked for.
        let mut best = (
            (false, 0, false, false),
            Path {
                reach: Reach::Rows(vec![(Bound::Unbounded, Bound::Unbounded)]),
                one: false,
                in_order: false,
                pinned: 0,
                order: Vec::new(),
            },
        );
        let mut tried = Vec::new();
        // A table that declares no key keeps its rows under numbers no statement names.
        let the_table = Some(the_table).filter(|_| !self.table.key.is_empty());
        for ordered in the_table.into_iter().chain(indexes) {
            // The leading columns given values. One list at most is taken, and none for a row's
            // values, so the ranges read are no more than the values the statement wrote.
            let mut listed = !given.is_empty();
            let pins: Vec<&Term> = (ordered.columns.iter())
                .map_while(|(column, _)| {
                    let equal = |term: &&Term| term.column == *column && term.op == BinOp::Eq;
                    let term = terms.iter().chain(own).find(equal)?;
                    let list = term.values.len() != 1;
                    let taken = !(list && listed);
                    listed |= list;
                    taken.then_some(term)
                })
                .collect();
            let mut ranges = Vec::new();
            let (mut ranged, mut cut) = (false, false);
            // A key for each value of the list taken, if one is, among the single values.
            let each = pins
                .iter()
                .map(|pin| pin.values.len())
                .find(|&len| len != 1);
            for at in 0..each.unwrap_or(1) {
                let one = |pin: &&Term| pin.values[at.min(pin.values.len() - 1)].clone();
                let tuple: Vec<Value> = pins.iter().map(one).collect();
                let (entry, range, is_ranged) = ordered.range(self.table, &tuple, own)?;
                cut |= entry.cut.is_some();
                ranged = is_ranged;
                ranges.push((entry.key, range));
            }
            // In key order, and a value written twice read once.
            ranges.sort_by(|a, b| a.0.cmp(&b.0));
            ranges.dedup_by(|a, b| a.0 == b.0);

            let next = &ordered.columns[pins.len()..];
            let one = ordered.unique && next.is_empty() && pins.iter().all(|pin| pin.one);
            let whole_key = ordered.index.is_none() && next.is_empty();
            // A key keeps its columns as ORDER BY sorts them, NULL included, so what follows
            // the columns the filter fixed is in order if it is what was asked for. Behind a
            // list each value starts the order again, and behind a cut value entries follow
            // their digests.
            let single = ranges.len() <= 1 && !cut;
            let asked = asked.as_ref().filter(|asked| {
                asked.is_empty() || single && (whole_key || next.starts_with(asked))
            });
            let in_order = asked.is_some() || single && whole_key;
            let order = asked.cloned().unwrap_or_default();
            let (keys, ranges): (Vec<Key>, Vec<Range>) = ranges.into_iter().unzip();
            let reach = match ordered.index {
                Some(index) => Reach::Index(index, ranges),
                None if whole_key => Reach::Keys(keys),
                None => Reach::Rows(ranges),
            };
            let path = Path {
                reach,
                one,
                in_order,
                pinned: pins
                    .iter()
                    .filter(|pin| whole_key || pin.values.len() == 1)
                    .count(),
                order,
            };
            // The whole key is better than any index.
            let score = (whole_key, pins.len(), ranged, in_order);
            let pinned = pins.len();
            if score > best.0 {
                best = (score, path);
                tried.insert(0, (ordered, pinned, ranged));
            } else {
                tried.push((ordered, pinned, ranged));
            }
        }
        Ok((best.1, tried))
    }

    /// Says, when the statement is explained, the path [`Source::given`] picks, with `joined`
    /// given by a row when that serves better than the filter alone: its key or index (`Scan`
    /// when no column is given a value or a range), the columns given, and why each other lost.
    pub(super) fn say(
        &self,
        joined: &[usize],
        order: &[SortKey],
        assigned: &[usize],
    ) -> Result<()> {
        let Some(said) = self.ctx.said else {
            return Ok(());
        };
        let row: Vec<_> = joined.iter().map(|&column| (column, None)).collect();
        let (path, mut tried) = self.scored(&row, order, assigned)?;
        if !path.serves(self.choose(order, assigned)?.pinned) {
            tried = self.scored(&[], order, assigned)?.1;
        }
        let (table, columns) = (self.table, &self.table.meta.columns);
        let indexes = &table.meta.indexes;
        let name = |index: Option<usize>| {
            let key = indexes.iter().find(|index| index.name.starts_with("__pk_"));
            index
                .map_or(key, |at| indexes.get(at))
                .map_or("", |index| &index.name)
        };
        // A column only an index over a JSON path has is named by the column it reads.
        let column = |at: usize| match columns.get(at) {
            Some(column) => column.name.clone(),
            None => table.meta.indexes[at - columns.len()].columns[0].0.clone(),
        };
        let chosen = tried.first().filter(|tried| tried.1 > 0 || tried.2);
        let mut lines = vec![match chosen {
            Some((by, ..)) => format!("IndexScan {{ index: {} }}", name(by.index)),
            None => "Scan".to_owned(),
        }];
        if let Some((by, pinned, ranged)) = chosen {
            let pushed = by.columns.iter().take(pinned + usize::from(*ranged));
            let pushed: Vec<String> = pushed.map(|(at, _)| column(*at)).collect();
            lines.push(format!("  predicates_pushed: [{}]", pushed.join(", ")));
        }
        let pinned = chosen.map_or(0, |chosen| chosen.1);
        let whole = chosen.is_some_and(|(by, n, _)| by.index.is_none() && by.columns.len() == *n);
        let ranged = chosen.is_some_and(|chosen| chosen.2);
        let considered: Vec<String> = (tried[usize::from(chosen.is_some())..].iter())
            .map(|(other, other_pinned, other_ranged)| {
                let first = column(other.columns[0].0);
                let reason = match *other_pinned {
                    0 => (self.filter.and_then(|filter| residual(filter, &first)))
                        .unwrap_or("first column not in WHERE"),
                    other_pinned if other_pinned < pinned => {
                        "fewer predicate columns matched than chosen index"
                    }
                    _ if whole || ranged && !other_ranged => "lower selectivity than chosen index",
                    _ => "tied with chosen index; lost by creation order",
                };
                format!("{}: {reason}", name(other.index))
            })
            .collect();
        if !considered.is_empty() {
            lines.push(format!("  indexes_considered: [{}]", considered.join(", ")));
        }
        said.borrow_mut().extend(lines);
        Ok(())
    }

    /// Whether the path reaches at most `most` rows, counted by keys or index entries alone and
    /// no further than one past `most`. The whole table is never few.
    pub(super) fn few(&self, path: &Path, most: u64) -> Result<bool> {
        let ranges = match &path.reach {
            Reach::Keys(keys) => return Ok(keys.len() as u64 <= most),
            Reach::Rows(ranges) if ranges[..] == [(Unbounded, Unbounded)] => return Ok(false),
            Reach::Rows(ranges) | Reach::Index(_, ranges) => ranges,
        };
        let of = match path.reach {
            Reach::Index(at, _) => Keys::Index(self.table.index_ids[at]),
            _ => Keys::Table(self.table.id),
        };
        let mut left = most.saturating_add(1);
        for (from, to) in ranges {
            let (from, to) = (from.as_ref(), to.as_ref());
            left -= self.read.counted(of, from, to, left as usize)?;
            if left == 0 {
                break;
            }
        }
        Ok(left > 0)
    }

    /// Reads the rows the path reaches and hands `each` every one the reader may see and the
    /// filter passes: its key, its values, and the bytes of `reservation` it holds. Stops when
    /// `each` answers false. Returns how many stored rows were read.
    pub(super) fn walk(
        &self,
        path: &Path,
        reservation: &mut Reservation,
        mut each: impl FnMut(Key, Vec<Value>, u64, &mut Reservation) -> Result<bool>,
    ) -> Result<u64> {
        let mut each = |(key, values, _, bytes): Held, reservation: &mut Reservation| {
            each(key, values, bytes, reservation)
        };
        self.cells(path, None, reservation, &mut each)
    }

    /// [`Source::walk`] from the row after the one with the key `after`, handing over with
    /// each row the cells that were not taken from it, in column order.
    pub(super) fn cells(
        &self,
        path: &Path,
        after: Option<&Key>,
        reservation: &mut Reservation,
        each: &mut dyn FnMut(Held, &mut Reservation) -> Result<bool>,
    ) -> Result<u64> {
        let table = self.table.id;
        let mut examined = 0;
        match &path.reach {
            Reach::Keys(keys) => {
                let keys = keys
                    .iter()
                    .filter(|key| after.is_none_or(|after| *key > after));
                for key in keys {
                    let Some(row) = self.read.row(table, key)? else {
                        continue;
                    };
                    examined += 1;
                    if let Some((values, cells, bytes)) = self.pass(row, path.one, reservation)?
                        && !each((key.clone(), values, cells, bytes), reservation)?
                    {
                        break;
                    }
                }
            }
            Reach::Rows(ranges) => {
                'ranges: for (from, to) in ranges {
                    // A bound stands for every key that begins with it, so the walk resumes
                    // after `after` only where that is later than where the range starts.
                    let resumed = after.filter(|after| match from {
                        Bound::Included(from) => *after >= from,
                        Bound::Excluded(from) => *after > from && !after.starts_with(from),
                        Bound::Unbounded => true,
                    });
                    let from = resumed.map_or(from.as_ref(), Bound::Excluded);
                    for item in self.read.rows(table, from, to.as_ref())? {
                        let (key, row) = item?;
                        examined += 1;
                        if let Some((values, cells, bytes)) =
                            self.pass(row, path.one, reservation)?
                            && !each((key, values, cells, bytes), reservation)?
                        {
                            break 'ranges;
                        }
                    }
                }
            }
            Reach::Index(at, ranges) => {
                let index = self.table.index_ids[*at];
                // An index is resumed by passing the entries up to the one of the row resumed
                // after. When that row is gone, so is every row matched before it.
                let mut passing = match after {
                    Some(after) => self.read.row(table, after)?.is_some(),
                    None => false,
                };
                // The run of cut entries held back, and the bytes its entries share.
                let mut run: Vec<Held> = Vec::new();
                let mut shared = Vec::new();
                'ranges: for (from, to) in ranges {
                    for entry in self.read.index(index, from.as_ref(), to.as_ref())? {
                        let (entry, key, cut) = entry?;
                        if passing {
                            passing = Some(&key) != after;
                            continue;
                        }
                        // Cut entries with the same kept bytes are a run; only an order that
                        // is used asks for the values of one.
                        let cut = cut.filter(|_| !path.order.is_empty());
                        let begins = match cut.map(|cut| entry.as_bytes().get(..cut + 2)) {
                            Some(None) => {
                                return Err(self
                                    .read
                                    .damaged("an index entry ends inside its cut"));
                            }
                            Some(begins) => begins,
                            None => None,
                        };
                        if !run.is_empty()
                            && begins != Some(shared.as_slice())
                            && !self.hand_on(&mut run, &path.order, reservation, each)?
                        {
                            break 'ranges;
                        }
                        let Some(row) = self.read.row(table, &key)? else {
                            return Err(self.read.damaged("an index entry names no row"));
                        };
                        examined += 1;
                        let Some((values, cells, bytes)) = self.pass(row, path.one, reservation)?
                        else {
                            continue;
                        };
                        match begins {
                            Some(begins) => {
                                reservation.grow(held_bytes(&key))?;
                                shared = begins.to_vec();
                                run.push((key, values, cells, bytes));
                            }
                            None => {
                                if !each((key, values, cells, bytes), reservation)? {
                                    break 'ranges;
                                }
                            }
                        }
                    }
                }
                self.hand_on(&mut run, &path.order, reservation, each)?;
            }
        }
        Ok(examined)
    }

    /// Hands on a run of rows in the order of their values in the columns asked for, each in
    /// its direction, and otherwise as their entries came. Answers false when `each` did.
    fn hand_on(
        &self,
        run: &mut Vec<Held>,
        order: &[(usize, SortDirection)],
        reservation: &mut Reservation,
        each: &mut dyn FnMut(Held, &mut Reservation) -> Result<bool>,
    ) -> Result<bool> {
        run.sort_by(|a, b| {
            let mut columns = order.iter().map(|(column, direction)| {
                let order = sort_order(&a.1[*column], &b.1[*column]);
                match direction {
                    SortDirection::Asc => order,
                    SortDirection::Desc => order.reverse(),
                }
            });
            columns
                .find(|order| order.is_ne())
                .unwrap_or(Ordering::Equal)
        });
        let mut more = true;
        for held in run.drain(..) {
            reservation.shrink(held_bytes(&held.0));
            match more {
                true => more = each(held, reservation)?,
                false => reservation.shrink(held.3),
            }
        }
        Ok(more)
    }

    /// The row with this key if it is there, the reader may see it and it passes the filter,
    /// as [`Source::walk`] hands a row over: its values, and the bytes of `reservation` it
    /// holds.
    pub(super) fn row(
        &self,
        key: &Key,
        reservation: &mut Reservation,
    ) -> Result<Option<(Vec<Value>, u64)>> {
        let Some(row) = self.read.row(self.table.id, key)? else {
            return Ok(None);
        };
        let passed = self.pass(row, false, reservation)?;
        Ok(passed.map(|(values, _, bytes)| (values, bytes)))
    }

    /// A stored row the reader may see and the filter passes: its values, the cells not taken
    /// from it, both in column order, and the bytes of `reservation` the values hold. A row
    /// that does not pass holds none. A column the statement does not read is left as NULL.
    ///
    /// The reader's filter is asked first, and nothing else of a row it hides is looked at.
    /// With `refuse`, such a row is the refusal of the rule that hid it.
    fn pass(
        &self,
        row: Row,
        refuse: bool,
        reservation: &mut Reservation,
    ) -> Result<Option<Passed>> {
        let before = reservation.bytes();
        let missing = &self.table.missing;
        reservation.grow(row_overhead(missing.len()))?;
        // A column added since the row was written has no cell and reads as the table says.
        let mut cells = row.cells;
        cells.extend(missing.iter().skip(cells.len()).cloned().map(Cell::Here));
        let mut values = vec![Value::Null; cells.len()];
        let mut hidden = None;
        if let Some(visible) = &self.visible {
            self.take(&mut cells, &mut values, &self.seen, reservation)?;
            hidden = visible.borrow_mut().hidden(&values, self.read)?;
        }
        let mut passes = hidden.is_none();
        if passes {
            self.take(&mut cells, &mut values, &self.filtered, reservation)?;
            if let Some(filter) = self.filter {
                passes = eval(filter, &self.scope, &values, self.ctx)? == Value::Bool(true);
            }
        }
        if !passes {
            reservation.shrink(reservation.bytes() - before);
            return hidden.filter(|_| refuse).map_or(Ok(None), Err);
        }
        self.take(&mut cells, &mut values, &self.rest, reservation)?;
        Ok(Some((values, cells, reservation.bytes() - before)))
    }

    /// Moves the marked cells into `values`, loading a value kept beside its row. The memory
    /// of the values is asked for before any is moved or loaded.
    fn take(
        &self,
        cells: &mut [Cell],
        values: &mut [Value],
        marked: &[bool],
        reservation: &mut Reservation,
    ) -> Result<()> {
        let here = cells.iter().zip(marked).map(|(cell, marked)| match cell {
            Cell::Here(here) if *marked => heap_bytes(here),
            _ => 0,
        });
        reservation.grow(here.sum())?;
        for ((cell, value), _) in cells
            .iter_mut()
            .zip(values)
            .zip(marked)
            .filter(|(_, marked)| **marked)
        {
            *value = match std::mem::replace(cell, Cell::Here(Value::Null)) {
                Cell::Here(here) => here,
                Cell::Beside(beside) => self.read.load(&beside, reservation)?,
            };
        }
        Ok(())
    }
}

/// What a row that passed hands over: its values, the cells not taken from it, and the bytes
/// its values hold.
type Passed = (Vec<Value>, Vec<Cell>, u64);

/// What passes exactly the rows that hold `values` in `columns`, side by side, and the
/// parameters it reads them from: each under its column's name.
pub(crate) fn equal(
    table: &Table,
    columns: &[usize],
    values: &[Value],
) -> (Expr, HashMap<String, Value>) {
    let mut bound = HashMap::new();
    let each = columns.iter().zip(values).map(|(&at, value)| {
        let name = &table.meta.columns[at].name;
        bound.insert(name.clone(), value.clone());
        let column = ColumnRef {
            table: None,
            column: name.clone(),
        };
        Expr::BinaryOp {
            left: Box::new(Expr::Column(column)),
            op: BinOp::Eq,
            right: Box::new(Expr::Parameter(name.clone())),
        }
    });
    let filter = each.reduce(|left, right| Expr::BinaryOp {
        left: Box::new(left),
        op: BinOp::And,
        right: Box::new(right),
    });
    let filter = filter.expect("a key and a UNIQUE rule have a column");
    (filter, bound)
}
