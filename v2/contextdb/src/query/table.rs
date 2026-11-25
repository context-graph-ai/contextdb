//! The one source of a table's rows: which rows a statement has to read, and reading them
//! through the reader's filter and the statement's own.

use std::cell::{OnceCell, RefCell};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::ops::Bound;
use std::slice;

use super::expression::{EvalContext, Scope, eval, sort_order};
use super::filter::{self, Case, Columns, Fact, MOST_CASES, Part, Reading};
use super::row_overhead;
use super::visible::Visible;
use crate::memory::{Passing, Reservation};
use crate::schema::Table;
use crate::sql::Expr;
use crate::sql::ast::{BinOp, ColumnRef, SortDirection as Asked};
use crate::sql::plan::SortKey;
use crate::store::{Cell, Entry, Key, Keys, Read, Row, Span, cells_end, span};
use crate::types::{ColumnType, Error, Result, SortDirection, Value, timestamp_millis};

/// The keys or entries between two bounds.
type Range = (Bound<Key>, Bound<Key>);
pub(super) type Place = (Key, Reservation);
/// Why an index lost when no part of the filter reads its first column.
const UNREAD: &str = "first column not in WHERE";

/// Where the rows of one table are reached.
#[derive(Clone)]
pub(super) enum Reach {
    /// The rows with these primary keys, in key order.
    Keys(Vec<Key>),
    /// The table between each pair of keys, in key order.
    Rows(Vec<Range>),
    /// The rows that the entries of the index at this place among the table's indexes point
    /// at, between each pair of entries, in entry order.
    Index(usize, Vec<Range>),
}

/// How the rows of one table are reached for one statement.
#[derive(Clone)]
pub(super) struct Path {
    pub(super) reach: Reach,
    /// True when the statement names one row whole, by its key or by every column of a UNIQUE
    /// rule: a row hidden from the reader that the whole filter passes is refused by its rule.
    pub(super) one: bool,
    /// True when the rows arrive in the order asked for, so that they need no sort.
    pub(super) in_order: bool,
    /// True when the walk starts from the far end of its reach.
    pub(super) back: bool,
    /// How many leading keys of `order` the walk gives; rows equal on them are a run.
    tied: usize,
    /// How many leading columns of the key or index it reads are given one value each, or any
    /// values when they make the whole key, whose rows a list names exactly.
    pub(super) pinned: usize,
    /// The columns of that order. A run of rows, and a run of entries that hold cut values (in
    /// the order of their digests), is put in the order of the values before it is handed on.
    order: Vec<(usize, SortDirection)>,
    /// The parts of the filter each row is still asked, in order: those the ranges of the path
    /// do not prove; `None` for every part, as a row reached any other way is asked.
    pub(super) checks: Option<Vec<usize>>,
    /// The whole order asked, columns the filter fixes included, when the path gives it.
    pub(super) asked: Vec<(usize, SortDirection)>,
}

impl Path {
    /// True when a row's values give this path the whole key, or more than `own` columns.
    pub(super) fn serves(&self, own: usize) -> bool {
        matches!(self.reach, Reach::Keys(_)) || self.pinned > own
    }
}

/// A key or index tried for a path, with how many of its leading columns were given a value and
/// whether the next was given a range, how many keys of the requested order it supplies, and
/// its rank: 2 when it names at most one row, 1 for the table's whole key by a list, else 0.
type Tried = (Ordered, usize, bool, usize, u8);

/// True when the path reads by what was tried: some of its columns given, or its order used.
fn walked((_, pinned, ranged, tied, _): &Tried) -> bool {
    *pinned > 0 || *ranged || *tied > 0
}

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
    /// with. A row key is whole and all ascending; an entry is made as its index makes it, and
    /// the row key it ends with as a row key.
    fn key(&self, table: &Table, values: &[Value]) -> Result<Entry> {
        match self.index {
            Some(at) => {
                let own = values.len().min(table.meta.indexes[at].columns.len());
                let mut entry = table.entry_shape(at).entry(&values[..own])?;
                entry.key = entry.key.joined(&Key::from_values(&values[own..], &[])?);
                Ok(entry)
            }
            None => Ok(Entry {
                key: Key::from_values(values, &[])?,
                cut: None,
            }),
        }
    }

    /// What `within`, a fact of the next column, leaves of the keys that begin with `given`:
    /// what `given` makes, the range, whether it is a range of the next column, and whether a
    /// bound was cut back to its run, so that the range holds rows outside it.
    fn range(
        &self,
        table: &Table,
        given: &[Value],
        within: Option<&Fact>,
    ) -> Result<(Entry, Range, bool, bool)> {
        let all = self.key(table, given)?;
        let (mut from, mut to) = (Bound::Unbounded, Bound::Unbounded);
        if !given.is_empty() {
            to = until_after(&all.key);
            from = Bound::Included(all.key.clone());
        }
        let (mut ranged, mut cut) = (false, false);
        // Behind a cut value nothing says where the next column's values lie.
        let next = self.columns.get(given.len()).filter(|_| all.cut.is_none());
        if let (Some((_, direction)), Some(Fact::Within(low, high))) = (next, within) {
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
            // No NULL is in a range. A key keeps NULL after every value going up and before
            // every value going down, and the walk stops short of it.
            let null = with(&Value::Null)?.0;
            match direction {
                SortDirection::Asc => to = Bound::Excluded(null),
                SortDirection::Desc => from = starting_after(null),
            }
            ranged = true;
            // A descending column keeps its greater values first. When more columns follow,
            // many keys begin with this one, and "after it" is after them all.
            for (bound, low) in [(low, true), (high, false)] {
                let (Bound::Included(value) | Bound::Excluded(value)) = bound else {
                    continue;
                };
                let (key, is_cut) = with(value)?;
                cut |= is_cut;
                let open = matches!(bound, Bound::Excluded(_)) && !is_cut;
                match (low == (*direction == SortDirection::Asc), open) {
                    (true, true) => from = starting_after(key),
                    (true, false) => from = Bound::Included(key),
                    (false, true) => to = Bound::Excluded(key),
                    (false, false) => to = until_after(&key),
                }
            }
        }
        Ok((all, (from, to), ranged, cut))
    }
}

/// Ranges in key order, those that overlap joined into one, so that no key is read twice.
fn merged(ranges: impl Iterator<Item = Range>) -> Vec<Range> {
    let spanned = ranges.map(|range| (span(range.0.as_ref(), range.1.as_ref()), range));
    let mut spanned: Vec<_> = spanned.collect();
    spanned.sort_by(|a, b| a.0.0.cmp(&b.0.0));
    let mut merged: Vec<(Span, Range)> = Vec::new();
    for (span, range) in spanned {
        match merged.last_mut() {
            Some((last, kept)) if span.0 <= last.1 => {
                if span.1 > last.1 {
                    (last.1, kept.1) = (span.1, range.1);
                }
            }
            _ => merged.push((span, range)),
        }
    }
    merged.into_iter().map(|(_, range)| range).collect()
}

/// The fact a joined row's value or else the case gives a column.
fn fixed<'c>(given: &'c [(usize, Fact)], case: &'c Case, column: usize) -> Option<&'c Fact> {
    let mut facts = given.iter().chain(case);
    facts.find(|(at, _)| *at == column).map(|(_, fact)| fact)
}

/// True when `expr` reads the column called `column`.
fn reads(expr: &Expr, column: &str) -> bool {
    let mut inner = crate::sql::inside(expr);
    matches!(expr, Expr::Column(c) if c.column == column) || inner.any(|e| reads(e, column))
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

/// A walk's bounds resumed past `after`, a complete stored key: the later of the start and that
/// place going forward, the earlier of the end and that place going `back`.
pub(crate) fn resumed<'a>(
    (from, to): (Bound<&'a Key>, Bound<&'a Key>),
    after: Option<&'a Key>,
    back: bool,
) -> (Bound<&'a Key>, Bound<&'a Key>) {
    let end = if back { to } else { from };
    let resumed = after.filter(|after| match end {
        Bound::Included(end) => after.starts_with(end) || (*after > end) != back,
        Bound::Excluded(end) => !after.starts_with(end) && (*after > end) != back,
        Bound::Unbounded => true,
    });
    let end = resumed.map_or(end, Bound::Excluded);
    if back { (from, end) } else { (end, to) }
}

/// The items in the order a walk takes them: as they are, or last first going `back`.
fn taken<T>(items: &[T], back: bool) -> Vec<&T> {
    let mut taken: Vec<&T> = items.iter().collect();
    if back {
        taken.reverse();
    }
    taken
}

/// How two rows compare in `order`: by their values in its columns, each in its direction.
pub(super) fn ordering(order: &[(usize, SortDirection)], a: &[Value], b: &[Value]) -> Ordering {
    let mut columns = order.iter().map(|(at, direction)| match direction {
        SortDirection::Asc => sort_order(&a[*at], &b[*at]),
        SortDirection::Desc => sort_order(&b[*at], &a[*at]),
    });
    columns
        .find(|order| order.is_ne())
        .unwrap_or(Ordering::Equal)
}

/// The stored index pair and its charge, taken before the pair is copied.
pub(crate) fn index_place(entry: &Key, row: &Key, held: &mut Reservation) -> Result<Place> {
    let bytes = (entry.as_bytes().len() + row.as_bytes().len()) as u64;
    held.grow(bytes)?;
    Ok((entry.joined(row), held.split(bytes)))
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
        (ColumnType::Timestamp, Value::Text(text)) => timestamp_millis(&text).map(Value::Timestamp),
        _ => None,
    }
}

/// A row a walk holds back: its key, its values, the cells not taken from it, and the bytes
/// its values hold.
pub(super) type Held = (Key, Vec<Value>, Vec<Cell>, u64);

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
    /// The filter, read once and shared by every path tried and every row asked.
    reading: OnceCell<Reading<'a>>,
    /// What the filter is worked out with beside a row; the caller's, so that it holds
    /// whatever the statement worked out before any row.
    ctx: &'a EvalContext<'a>,
    read: &'a dyn Read,
    cover: RefCell<Passing>,
    defaults: u64,
    examined: super::Counted<'a>,
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
            reading: OnceCell::new(),
            ctx,
            read,
            cover: RefCell::new(visible.cover.passing()),
            defaults: table.missing.iter().map(crate::types::heap_bytes).sum(),
            examined: None,
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

    /// Counts visible rows read by this source in the statement's counter.
    pub(super) fn counted(mut self, examined: super::Counted<'a>) -> Source<'a> {
        self.examined = examined;
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
    /// The table and each index are tried in turn. The first that names at most one row wins;
    /// then the table's whole key; then one is better the more of its leading columns the
    /// filter gives a value for (`=`, `IS NULL`, and one `IN` list), then if the filter gives a
    /// range of its next column, then if it keeps the order asked for; the table itself wins a
    /// tie, then the index declared first. The filter is still applied to every row read, so a
    /// path only has to reach every row that can pass. An index over a column of `assigned` is
    /// not tried: the statement moves its entries.
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
        let (path, tried, ..) = self.scored(self.facts()?, None, given, order, assigned)?;
        // The statement's record names an index EXPLAIN lists as considered whose first column a
        // kept part reads, with that part's reason, else one that gives the order not given.
        if let Some((_, Some(work))) = self.examined {
            let used = tried.first().is_some_and(walked);
            let (reading, other) = (self.facts()?, tried.iter().skip(usize::from(used)));
            let mut lost = other.clone().filter(|other| other.1 == 0);
            let lost = lost.find_map(|other| {
                let why = self.lost(reading, other.0.columns[0].0);
                (why != UNREAD).then_some((other, why))
            });
            let mut order = other.filter(|other| other.3 > 0 && !path.in_order);
            let note = lost.or(order.next().map(|other| (other, "gives the order asked")));
            let note = note.map(|(other, why)| (self.index_name(other.0.index).to_owned(), why));
            work.unused.set(work.unused.take().or(note));
        }
        Ok(path)
    }

    /// The name of the index at `index`, or of the table's own key.
    fn index_name(&self, index: Option<usize>) -> &str {
        let indexes = &self.table.meta.indexes;
        let key = indexes.iter().find(|index| index.name.starts_with("__pk_"));
        let index = index.map_or(key, |at| indexes.get(at));
        index.map_or("", |index| &index.name)
    }

    /// Why an index none of whose leading columns is given a value lost, its first column at
    /// `at`: the cases fix that column in some and not in others, or a part reads it in a way no
    /// index serves.
    fn lost(&self, reading: &Reading, at: usize) -> &'static str {
        let fixes = |case: &Case| case.iter().any(|(column, _)| *column == at);
        if reading.cases.iter().any(fixes) && !reading.cases.iter().all(fixes) {
            return "OR arms reach the table by different paths";
        }
        let (parts, name) = (&reading.parts, self.column_name(at));
        let parts = parts.iter().filter(|part| !part.columns.contains(&at));
        let mut read = parts.filter(|part| reads(part.expr, name));
        read.find_map(|part| part.kept).unwrap_or(UNREAD)
    }

    /// The column at `at`; one only an index over a JSON path has is named by the column it reads.
    fn column_name(&self, at: usize) -> &str {
        let (columns, indexes) = (&self.table.meta.columns, &self.table.meta.indexes);
        match columns.get(at) {
            Some(column) => &column.name,
            None => &indexes[at - columns.len()].columns[0].0,
        }
    }

    /// The statement's filter, read once.
    pub(super) fn facts(&self) -> Result<&Reading<'a>> {
        if let Some(reading) = self.reading.get() {
            return Ok(reading);
        }
        let columns = Columns {
            scope: &self.scope,
            table: Some(self.table),
        };
        let reading = filter::read(self.filter, &columns, self.ctx)?;
        Ok(self.reading.get_or_init(|| reading))
    }

    /// Whether the filter `filter`, the statement's own with a partition's tuple met last, is
    /// served for the partition whose key columns are at `key`, and its countable path: the
    /// best path tried whose every range holds each key column to one value by a whole key and
    /// serves every case. Its checks are the statement's own parts.
    pub(super) fn partition(&self, filter: &Expr, key: &[usize]) -> Result<(bool, Option<Path>)> {
        let columns = Columns {
            scope: &self.scope,
            table: Some(self.table),
        };
        let reading = filter::read(Some(filter), &columns, self.ctx)?;
        let (_, _, served, countable) = self.scored(&reading, Some(key), &[], &[], &[])?;
        let parts = self.facts()?.parts.len();
        // The find decides a row the statement names once, by its whole WHERE: a partition's
        // path leaves a hidden row out.
        let own = |mut path: Path| {
            path.one = false;
            path.checks
                .iter_mut()
                .for_each(|checks| checks.retain(|at| *at < parts));
            path
        };
        Ok((served, countable.map(own)))
    }

    /// The path [`Source::given`] picks for `reading`, and each key or index tried: first the
    /// one the path reads, when one did better than the whole table; with, for a partition's
    /// `key`, whether every case that needs a test on rows has a path that holds or ranges one of
    /// its columns outside `key`, and the best countable path tried (see [`Source::partition`]).
    fn scored(
        &self,
        reading: &Reading<'_>,
        key: Option<&[usize]>,
        given: &[(usize, Option<&Value>)],
        order: &[SortKey],
        assigned: &[usize],
    ) -> Result<(Path, Vec<Tried>, bool, Option<Path>)> {
        let (columns, partition) = (&self.table.meta.columns, key.is_some());
        let key = key.unwrap_or_default();
        let opaque = reading.parts.iter().any(|part| part.kept.is_some());
        let needs = |case: &Case| opaque || case.iter().any(|(at, _)| !key.contains(at));
        let own = |case: &Case, applied: &[usize]| {
            !needs(case)
                || case
                    .iter()
                    .any(|(at, _)| !key.contains(at) && applied.contains(at))
        };
        let mut served: Vec<bool> = reading.cases.iter().map(|case| !needs(case)).collect();
        let mut countable = None;
        // A joined row's values stand in for what the filter says of their columns.
        let given: Vec<(usize, Fact)> = (given.iter())
            .filter_map(|&(column, value)| {
                let value = value.map_or(Some(Value::Null), |value| {
                    as_key(columns[column].column_type, value.clone())
                })?;
                Some((
                    column,
                    Fact::Among {
                        values: vec![value],
                        one: false,
                    },
                ))
            })
            .collect();
        let column_named = |name: &String| columns.iter().position(|column| column.name == *name);
        // The order asked for, when it is plain columns.
        let mut asked: Option<Vec<(usize, SortDirection)>> = order
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
        let whole = asked.clone();
        // A column every case gives the same one value contributes no ordering within this read.
        let held = |column: usize| {
            let cases = reading.cases.iter();
            let mut values = cases.map(|case| match fixed(&given, case, column) {
                Some(Fact::Among { values, .. }) if values.len() == 1 => values.first(),
                _ => None,
            });
            let first = values.next().flatten();
            first.is_some() && values.all(|value| value == first)
        };
        if let Some(asked) = &mut asked {
            asked.retain(|(column, _)| !held(*column));
        }

        let by_key: Vec<_> = (self.table.key.iter().map(|at| (*at, SortDirection::Asc))).collect();
        let the_table = Ordered {
            index: None,
            unique: true,
            columns: by_key.clone(),
        };
        let indexes = self.table.meta.indexes.iter().enumerate();
        let indexes = indexes
            .filter(|(_, index)| !index.name.starts_with("__pk_"))
            .filter(|(_, index)| {
                let mut columns = index.columns.iter().map(|(name, _)| column_named(name));
                !columns.any(|at| at.is_some_and(|at| assigned.contains(&at)))
            })
            .map(|(at, index)| (at, index, index.name.starts_with("__unique_")))
            .map(|(at, index, unique)| Ordered {
                index: Some(at),
                unique,
                columns: match index.json_path.is_empty() {
                    // Each entry of an index that is not unique ends with its row's key.
                    true => (index.columns.iter())
                        .filter_map(|(name, direction)| Some((column_named(name)?, *direction)))
                        .chain(by_key.iter().copied().filter(|_| !unique))
                        .collect(),
                    false => vec![(columns.len() + at, SortDirection::Asc)],
                },
            });

        // Until something better is found: the whole table, in no order that was asked for.
        let mut best = (
            (0, 0, false, 0),
            Path {
                reach: Reach::Rows(vec![(Bound::Unbounded, Bound::Unbounded)]),
                one: false,
                in_order: asked.as_ref().is_some_and(Vec::is_empty),
                back: false,
                tied: 0,
                pinned: 0,
                order: Vec::new(),
                checks: None,
                asked: Vec::new(),
            },
        );
        // A filter no row can pass reads nothing, in a table with no key or index too.
        if reading.cases.is_empty() {
            best.1.reach = Reach::Keys(Vec::new());
        }
        let mut tried = Vec::new();
        // A table that declares no key keeps its rows under numbers no statement names.
        let the_table = Some(the_table).filter(|_| !self.table.key.is_empty());
        for ordered in the_table.into_iter().chain(indexes) {
            let width = ordered.columns.len();
            let (mut ranges, mut cut, mut one, mut sole) = (Vec::new(), false, true, true);
            // For each case: the leading columns it pins, how many of them one value each,
            // whether it gives a range of the next, and the columns whose facts are applied whole.
            let mut each = Vec::new();
            for case in &reading.cases {
                // A key for each value of the lists taken: past a first list of any length, as
                // many as multiply to at most the most cases, and none beside a joined row's
                // values, so that the keys read are no more than the values it gives.
                let (mut tuples, mut single) = (vec![Vec::new()], 0);
                for (column, _) in &ordered.columns {
                    let Some(Fact::Among { values, one: named }) = fixed(&given, case, *column)
                    else {
                        break;
                    };
                    let many = tuples.len() > 1 && tuples.len() * values.len() > MOST_CASES;
                    if many || !given.is_empty() && values.len() > 1 {
                        break;
                    }
                    tuples = (tuples.iter())
                        .flat_map(|t| values.iter().map(move |v| [t, slice::from_ref(v)].concat()))
                        .collect();
                    single += usize::from(values.len() == 1);
                    one &= *named;
                    sole &= *named || given.iter().any(|(at, _)| at == column);
                }
                let pins = tuples[0].len();
                let next = ordered
                    .columns
                    .get(pins)
                    .and_then(|(at, _)| fixed(&given, case, *at));
                let (mut ranged, mut whole) = (false, true);
                for tuple in &tuples {
                    let (entry, range, is_ranged, bound_cut) =
                        ordered.range(self.table, tuple, next)?;
                    whole &= entry.cut.is_none() && !bound_cut;
                    cut |= entry.cut.is_some();
                    ranged = is_ranged;
                    ranges.push((entry.key, range));
                }
                let applied = ordered.columns[..pins + usize::from(ranged)].iter();
                let applied: Vec<usize> = applied.map(|(at, _)| *at).filter(|_| whole).collect();
                each.push((pins, single, ranged, applied));
            }
            let pinned = each.iter().map(|case| case.0).min().unwrap_or(width);
            let ranged = each.iter().all(|case| case.0 > pinned || case.2);
            let whole_key = ordered.index.is_none() && pinned == width;
            // Every case names the same row by `=`: the same values as written, not one entry.
            let same = reading.cases.iter().skip(1).all(|case| {
                let (first, mut columns) = (&reading.cases[0], ordered.columns.iter());
                columns.all(|(at, _)| fixed(&given, case, *at) == fixed(&given, first, *at))
            });
            let one = one && same && ordered.unique && pinned == width;
            // One value for every column, by `=` or a joined row: at most one row.
            let sole = sole && same && ordered.unique && pinned == width;
            let lone = each.iter().map(|case| case.1).min().unwrap_or(width);
            // Ranges walked in turn, in key order, keep the order past the columns every case
            // holds to one value, when each case narrows the walk; a list of whole keys is not
            // walked back.
            let leading = (ordered.columns.iter()).take_while(|(at, _)| held(*at));
            let narrowed = each.iter().all(|case| case.0 > 0 || case.2) && leading.count() == lone;
            let single = !cut && (ranges.len() <= 1 || !whole_key && narrowed);
            let reach = match ordered.index {
                None if whole_key => {
                    let mut keys: Vec<Key> = ranges.into_iter().map(|(key, _)| key).collect();
                    keys.sort();
                    keys.dedup();
                    Reach::Keys(keys)
                }
                None => Reach::Rows(merged(ranges.into_iter().map(|(_, range)| range))),
                Some(at) => Reach::Index(at, merged(ranges.into_iter().map(|(_, range)| range))),
            };
            // A key keeps its columns as ORDER BY sorts them, NULL included, so what follows
            // the columns the filter fixed is in order if it is what was asked for; a row and
            // an index entry end with the row's key. Behind a list each value starts the order
            // again, and behind a cut value entries follow their digests.
            let next = [&ordered.columns[lone..], &by_key].concat();
            // How many leading keys of the order asked it gives, walked forward or back.
            let gives = |back: bool| {
                let pairs = asked.iter().flatten().zip(&next).filter(|_| single);
                let given = |((at, way), (column, by)): &(&(_, _), &(_, _))| {
                    at == column && (way == by) != back
                };
                pairs.take_while(given).count()
            };
            let (back, tied) = match &asked {
                Some(asked) if single && whole_key => (false, asked.len()),
                _ => (gives(true) > gives(false), gives(false).max(gives(true))),
            };
            let full = asked.as_ref().is_some_and(|asked| tied == asked.len());
            // The table's own key gives the order whole or not at all: its rows make no runs.
            let (back, tied) = if full || ordered.index.is_some() {
                (back, tied)
            } else {
                (false, 0)
            };
            let in_order = full || tied > 0 || single && whole_key;
            let order = (asked.as_ref())
                .filter(|_| in_order && ordered.index.is_some())
                .cloned()
                .unwrap_or_default();
            // A part is still asked of a row unless, in every case, each of its facts is applied
            // whole by the path's pins or range.
            let proven = |part: &Part| {
                let mut each = reading.cases.iter().zip(&each);
                part.kept.is_none()
                    && given.is_empty()
                    && each.all(|(case, (.., applied))| {
                        let mut on = case.iter().filter(|(at, _)| part.columns.contains(at));
                        on.all(|(at, _)| applied.contains(at))
                    })
            };
            let checks = reading
                .parts
                .iter()
                .enumerate()
                .filter(|(_, part)| !proven(part));
            let path = Path {
                reach,
                one,
                in_order,
                back,
                tied,
                pinned: match whole_key {
                    true => width,
                    false => lone,
                },
                order,
                checks: Some(checks.map(|(at, _)| at).collect()),
                asked: whole.clone().filter(|_| in_order).unwrap_or_default(),
            };
            // The first path to at most one row decides; then the table's whole key by a list is
            // better than any index.
            let rank = if sole { 2 } else { u8::from(whole_key) };
            let score = match sole {
                true => (2, 0, false, 0),
                false => (rank, pinned, ranged, tied),
            };
            let cases = reading.cases.iter().zip(&each).filter(|_| partition);
            let mut counts = partition;
            for ((case, (.., applied)), served) in cases.zip(&mut served) {
                *served |= own(case, applied);
                counts &= own(case, applied) && key.iter().all(|at| applied.contains(at));
            }
            if counts && countable.as_ref().is_none_or(|(at, _)| score > *at) {
                countable = Some((score, path.clone()));
            }
            if score > best.0 {
                best = (score, path);
                tried.insert(0, (ordered, pinned, ranged, tied, rank));
            } else {
                tried.push((ordered, pinned, ranged, tied, rank));
            }
        }
        let served = served.into_iter().all(|served| served);
        Ok((best.1, tried, served, countable.map(|(_, path)| path)))
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
        if self.facts()?.cases.is_empty() {
            said.borrow_mut().push("NoScan { filter: false }".into());
            return Ok(());
        }
        let row: Vec<_> = joined.iter().map(|&column| (column, None)).collect();
        let reading = self.facts()?;
        let (mut path, mut tried, ..) = self.scored(reading, None, &row, order, assigned)?;
        if !path.serves(self.choose(order, assigned)?.pinned) {
            (path, tried, ..) = self.scored(reading, None, &[], order, assigned)?;
        }
        let chosen = tried.first().filter(|tried| walked(tried));
        let back = if path.back { " Backward" } else { "" };
        let mut lines = vec![match chosen {
            Some((by, ..)) => format!("IndexScan{back} {{ index: {} }}", self.index_name(by.index)),
            None => "Scan".to_owned(),
        }];
        if let Some((by, pinned, ranged, ..)) = chosen {
            let pushed = by.columns.iter().take(pinned + usize::from(*ranged));
            let pushed: Vec<&str> = pushed.map(|(at, _)| self.column_name(*at)).collect();
            lines.push(format!("  predicates_pushed: [{}]", pushed.join(", ")));
        }
        let pinned = chosen.map_or(0, |chosen| chosen.1);
        let ranged = chosen.is_some_and(|chosen| chosen.2);
        let (tied, rank) = chosen.map_or((0, 0), |chosen| (chosen.3, chosen.4));
        let reading = self.facts()?;
        let considered: Vec<String> = (tried[usize::from(chosen.is_some())..].iter())
            .map(|(other, other_pinned, other_ranged, other_tied, n)| {
                let reason = match *other_pinned {
                    0 if !other_ranged => self.lost(reading, other.columns[0].0),
                    _ if *n == 2 => "tied with chosen index; lost by creation order",
                    other_pinned if other_pinned < pinned => {
                        "fewer predicate columns matched than chosen index"
                    }
                    _ if rank > *n || ranged && !other_ranged => {
                        "lower selectivity than chosen index"
                    }
                    _ if tied > *other_tied => "does not supply requested order",
                    _ => "tied with chosen index; lost by creation order",
                };
                format!("{}: {reason}", self.index_name(other.index))
            })
            .collect();
        if !considered.is_empty() {
            lines.push(format!("  indexes_considered: [{}]", considered.join(", ")));
        }
        said.borrow_mut().extend(lines);
        Ok(())
    }

    pub(super) fn screen_owned(&self, context_leads: bool) -> bool {
        self.seen.iter().enumerate().all(|(at, reads)| {
            !reads || (context_leads && Some(at) == self.table.rules.context_column)
        })
    }

    /// How many rows the path reaches, counted by keys or index entries alone and no further
    /// than `most`; a named key counts when its row is there. With `charged`, what is counted
    /// is work the statement examined, read no further than one past its work limit.
    pub(super) fn entries(&self, path: &Path, most: u64, charged: bool) -> Result<u64> {
        let of = match path.reach {
            Reach::Index(at, _) => Keys::Index(self.table.index_ids[at]),
            _ => Keys::Table(self.table.id),
        };
        let ranges: Vec<_> = match &path.reach {
            Reach::Keys(keys) => keys
                .iter()
                .map(|k| (Bound::Included(k), Bound::Included(k)))
                .collect(),
            Reach::Rows(ranges) | Reach::Index(_, ranges) => ranges
                .iter()
                .map(|(from, to)| (from.as_ref(), to.as_ref()))
                .collect(),
        };
        // What is counted is charged every 1,024 and for the rest at the end.
        let (mut counted, examined) = (0_u64, self.examined.filter(|_| charged));
        let most = examined
            .and_then(|(_, work)| work)
            .map_or(most, |work| most.min(work.left()));
        let charge =
            |n| examined.map_or(Ok(()), |(examined, work)| super::admit(examined, n, work));
        'ranges: for (from, to) in ranges {
            for found in self.read.counted(of, from, to)? {
                if counted == most {
                    break 'ranges;
                }
                found?;
                counted += 1;
                if counted.is_multiple_of(1024) {
                    charge(1024)?;
                }
            }
        }
        charge(counted % 1024)?;
        Ok(counted)
    }

    /// Reads the rows the path reaches and hands `each` every one the reader may see and the
    /// filter passes: its key, its values, and the bytes of `reservation` it holds. Stops when
    /// `each` answers false.
    pub(super) fn walk(
        &self,
        path: &Path,
        reservation: &mut Reservation,
        mut each: impl FnMut(Key, Vec<Value>, u64, &mut Reservation) -> Result<bool>,
    ) -> Result<()> {
        let mut each = |(key, values, _, bytes): Held, reservation: &mut Reservation| {
            each(key, values, bytes, reservation)
        };
        self.cells(path, None, reservation, &mut each).map(drop)
    }

    /// A full walk, or a walk resumed through `place`: a charged row key or joined index pair.
    /// A stop replaces that place; exhaustion clears it. Each row includes its untaken cells in
    /// column order. Answers, when a stop with a place falls inside a run of an index, the rows
    /// of the run not handed on, in their order and still charged; the place is the run's end.
    pub(super) fn cells(
        &self,
        path: &Path,
        mut place: Option<&mut Option<Place>>,
        reservation: &mut Reservation,
        each: &mut dyn FnMut(Held, &mut Reservation) -> Result<bool>,
    ) -> Result<Vec<Held>> {
        let previous = place.as_deref_mut().and_then(Option::take);
        let after = previous.as_ref().map(|(key, _)| key);
        let table = self.table.id;
        let mut cover = self.cover.borrow_mut();
        match &path.reach {
            Reach::Keys(keys) => {
                let keys = keys
                    .iter()
                    .filter(|key| after.is_none_or(|after| *key > after));
                for key in keys {
                    let Some(row) = self.read.row(table, key, &mut cover)? else {
                        continue;
                    };
                    if let Some((values, cells, bytes)) =
                        self.pass(row, path.one, path.checks.as_deref(), reservation)?
                        && !each((key.clone(), values, cells, bytes), reservation)?
                    {
                        if let Some(place) = place {
                            let bytes = key.as_bytes().len() as u64;
                            reservation.grow(bytes)?;
                            *place = Some((key.clone(), reservation.split(bytes)));
                        }
                        return Ok(Vec::new());
                    }
                }
            }
            Reach::Rows(ranges) => {
                let mut after = after.cloned();
                for (from, to) in taken(ranges, path.back) {
                    let range = (from.as_ref(), to.as_ref());
                    let (from, to) = resumed(range, after.as_ref(), path.back);
                    for item in self.read.rows(table, from, to, path.back, &mut cover)? {
                        let (key, row) = item?;
                        after = Some(key.clone());
                        if let Some((values, cells, bytes)) =
                            self.pass(row, path.one, path.checks.as_deref(), reservation)?
                            && !each((key, values, cells, bytes), reservation)?
                        {
                            if let Some(place) = place {
                                let bytes = after.as_ref().map_or(0, |k| k.as_bytes().len() as u64);
                                reservation.grow(bytes)?;
                                *place = after.map(|key| (key, reservation.split(bytes)));
                            }
                            return Ok(Vec::new());
                        }
                    }
                }
            }
            Reach::Index(at, ranges) => {
                let index = self.table.index_ids[*at];
                let columns = &self.table.meta.indexes[*at].columns;
                let turned = |at| matches!(columns.get(at), Some((_, SortDirection::Desc)));
                // The run held back, the bytes its entries share, and its last entry.
                let (mut run, mut shared, mut ends) = (Vec::new(), Vec::new(), None);
                let more = 'walk: {
                    for (from, to) in taken(ranges, path.back) {
                        let (from, to) = resumed((from.as_ref(), to.as_ref()), after, path.back);
                        for entry in self.read.index(index, from, to, path.back)? {
                            let (entry, key, cut) = entry?;
                            let last = (entry.clone(), key.clone());
                            // Rows equal on the keys the walk gives of its order are a run, told by
                            // their entries' bytes, and so are cut entries with the same kept bytes.
                            let runs = cut.is_some() || path.tied < path.order.len();
                            let begins = match runs && !path.order.is_empty() {
                                true => {
                                    let whole = [entry.as_bytes(), key.as_bytes()].concat();
                                    let end = cells_end(&whole, turned, path.pinned + path.tied);
                                    let end = end.or(cut.map(|cut| cut + 2));
                                    let Some(begins) = end.and_then(|end| whole.get(..end)) else {
                                        let damaged = "an index entry ends inside its cut";
                                        return Err(self.read.damaged(damaged));
                                    };
                                    Some(begins.to_vec())
                                }
                                false => None,
                            };
                            if !run.is_empty()
                                && begins.as_ref() != Some(&shared)
                                && !self.hand_on(&mut run, &path.order, reservation, each)?
                            {
                                break 'walk false;
                            }
                            let Some(row) = self.read.row(table, &key, &mut cover)? else {
                                return Err(self.read.damaged("an index entry names no row"));
                            };
                            let Some((values, cells, bytes)) =
                                self.pass(row, path.one, path.checks.as_deref(), reservation)?
                            else {
                                continue;
                            };
                            match begins {
                                Some(begins) => {
                                    reservation.grow(held_bytes(&key))?;
                                    (shared, ends) = (begins, Some(last));
                                    reservation.room(&mut run, usize::MAX)?;
                                    run.push((key, values, cells, bytes));
                                }
                                None => {
                                    if !each((key, values, cells, bytes), reservation)? {
                                        ends = Some(last);
                                        break 'walk false;
                                    }
                                }
                            }
                        }
                    }
                    self.hand_on(&mut run, &path.order, reservation, each)?
                };
                // The list's own room is given back; each row it holds stays charged as held.
                reservation.shrink((run.capacity() * size_of::<Held>()) as u64);
                if let (Some(place), Some((entry, key)), false) = (place, ends, more) {
                    *place = Some(index_place(&entry, &key, reservation)?);
                    run.reverse();
                    return Ok(run);
                }
                for held in run {
                    reservation.shrink(held.3 + held_bytes(&held.0));
                }
            }
        }
        Ok(Vec::new())
    }

    /// Hands on a run of rows in the order of their values in the columns asked for, each in
    /// its direction, and otherwise as their entries came. Answers false when `each` did; the
    /// rows not handed on are left in the run, last first and still charged.
    pub(super) fn hand_on(
        &self,
        run: &mut Vec<Held>,
        order: &[(usize, SortDirection)],
        reservation: &mut Reservation,
        each: &mut dyn FnMut(Held, &mut Reservation) -> Result<bool>,
    ) -> Result<bool> {
        run.sort_by(|a, b| ordering(order, &a.1, &b.1));
        // Taken from the end, so that the rows a stop leaves stay in the run, still charged.
        run.reverse();
        while let Some(held) = run.pop() {
            reservation.shrink(held_bytes(&held.0));
            if !each(held, reservation)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// The row with this key if it is there, the reader may see it and it passes the filter,
    /// as [`Source::walk`] hands a row over: its values, and the bytes of `reservation` it
    /// holds.
    pub(super) fn row(
        &self,
        key: &Key,
        reservation: &mut Reservation,
    ) -> Result<Option<(Vec<Value>, u64)>> {
        let mut cover = self.cover.borrow_mut();
        let Some(row) = self.read.row(self.table.id, key, &mut cover)? else {
            return Ok(None);
        };
        let passed = self.pass(row, false, None, reservation)?;
        Ok(passed.map(|(values, _, bytes)| (values, bytes)))
    }

    /// A stored row the reader may see and the filter passes: its values, the cells not taken
    /// from it, both in column order, and the bytes of `reservation` the values hold. A row
    /// that does not pass holds none. A column the statement does not read is left as NULL.
    ///
    /// The reader's filter is asked first, and nothing else of a row it hides is looked at but
    /// with `refuse`: the statement's filter is asked of it too, and a hidden row that passes is
    /// the refusal of the rule that hid it. The statement's filter is asked part by part, those
    /// of `checks` or else every one, as AND asks them: the first FALSE ends it.
    fn pass(
        &self,
        row: Row,
        refuse: bool,
        checks: Option<&[usize]>,
        reservation: &mut Reservation,
    ) -> Result<Option<Passed>> {
        let before = reservation.bytes();
        let missing = &self.table.missing;
        reservation.grow(row.bytes + self.defaults + row_overhead(missing.len()))?;
        // A column added since the row was written has no cell and reads as the table says.
        let mut cells = row.cells;
        cells.extend(missing.iter().skip(cells.len()).cloned().map(Cell::Here));
        let mut values = vec![Value::Null; cells.len()];
        let mut hidden = None;
        if let Some(visible) = &self.visible {
            self.take(&mut cells, &mut values, &self.seen, reservation)?;
            hidden = visible.borrow_mut().hidden(&values, self.read)?;
        }
        let mut passes = hidden.is_none() || refuse;
        if passes {
            if let (None, Some((examined, work))) = (&hidden, self.examined) {
                super::admit(examined, 1, work)?;
            }
            self.take(&mut cells, &mut values, &self.filtered, reservation)?;
            let parts = self.facts()?.parts.iter().enumerate();
            for (_, part) in parts.filter(|(at, _)| checks.is_none_or(|checks| checks.contains(at)))
            {
                // A hidden row whose conditions cannot be worked out is not selected.
                let value = eval(part.expr, &self.scope, &values, self.ctx);
                if hidden.is_some() && matches!(value, Err(Error::PlanError(_))) {
                    passes = false;
                    break;
                }
                match *value? {
                    Value::Bool(true) => {}
                    Value::Bool(false) => {
                        passes = false;
                        break;
                    }
                    _ => passes = false,
                }
            }
        }
        if !passes || hidden.is_some() {
            reservation.shrink(reservation.bytes() - before);
            return hidden.filter(|_| passes).map_or(Ok(None), Err);
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
