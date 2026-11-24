//! A graph pattern: the rows `GRAPH_TABLE(...)` gives over a table of edges.
//!
//! One step followed once is a single hop: one match for each edge. Anything longer is a walk,
//! breadth first: a node is reached once, at the shallowest depth, from the start that comes
//! first, and a start is reached again when a cycle within the range comes back to it. What the
//! reader may not see is taken out before anything is matched: an edge is followed only when
//! the reader may see its row and the nodes at both its ends. Every edge and every row that
//! carries a node is read through the one table source, with the reader's filter in it.

use std::cell::{Cell, RefCell};
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::iter::once;
use std::sync::LazyLock;

use super::bind::bind;
use super::expression::{EvalContext, Scope, eval, mark, reads};
use super::filter::{Columns, Fact, read};
use super::plan::{self, Each, Given};
use super::row_overhead;
use super::table::{Source, equal};
use super::visible::Visible;
use crate::memory::Reservation;
use crate::schema::Table;
use crate::sql::Expr;
use crate::sql::ast::ColumnRef;
use crate::sql::plan::{EdgeDirection, EdgeStep, GraphTable, NodePattern};
use crate::types::{ColumnDef, ColumnType, Error, IndexDecl, Result, Uuid, Value, heap_bytes};

/// The columns that hold the two ends of an edge: where it leaves, then where it arrives.
const ENDS: [&str; 2] = ["source_id", "target_id"];

/// The bytes a walk is counted for each node it remembers: the node's id, and beside it the
/// node's place among the nodes met and among those still to follow.
const NODE: u64 = crate::memory::node::<(Uuid, Uuid, u32)>();

/// What qualifies the columns of a pattern in a refusal: a pattern has no name of its own.
const PATTERN: &str = "GRAPH_TABLE";

/// What every match has: the id of its last node, and how many edges lead to that node from
/// the first. The first of the two is also what each node the pattern names has.
static REACHED: LazyLock<[ColumnDef; 2]> = LazyLock::new(|| {
    let column = |name: &str, column_type| ColumnDef {
        name: name.to_owned(),
        column_type,
        nullable: false,
        primary_key: false,
        unique: false,
        default: None,
    };
    [
        column("id", ColumnType::Uuid),
        column("depth", ColumnType::Integer),
    ]
});

/// What takes the matches of a pattern: the nodes of one match in the order the pattern names
/// them, and how many edges lead from the first to the last. Answers false when it wants no
/// more.
type Match<'e> = dyn FnMut(&[Uuid], u32, &mut Reservation) -> Result<bool> + 'e;

/// What takes the nodes one step reaches: the start, the node reached and its depth.
type Reached<'e> = dyn FnMut(Uuid, Uuid, u32, &mut Reservation) -> Result<bool> + 'e;

/// What takes the edges at a node: the near end of one, then its far end.
type Ends<'e> = dyn FnMut(Uuid, Uuid, &mut Reservation) -> Result<bool> + 'e;

/// A table searched for a node's property: the places of its `id` and of the property.
type Carried<'a> = (&'a Table, usize, usize);

/// What takes the rows of one read.
type Rows<'e> = dyn FnMut(&[Value], &mut Reservation) -> Result<bool> + 'e;

/// One pattern over one table of edges, for one reader.
pub(super) struct Graph<'a> {
    given: &'a Given<'a>,
    _held: Reservation,
    edges: &'a Table,
    /// Where the columns of `ENDS` are in a row of `edges`, and where its `edge_type` is.
    ends: [usize; 2],
    typed: usize,
    steps: &'a [EdgeStep],
    /// The nodes the pattern's filter names for its first node, in the order it names them.
    /// `None` when it names none: then every node is a start.
    starts: Option<Vec<Uuid>>,
    /// True when they are the one node an `=` names: one the reader may not see is then the
    /// refusal of the rule that hides it, where a hidden node of a list is left out.
    one: bool,
    /// A single hop whose filter names its second node and not its first is matched from the
    /// second: the edges at a node are read from there, the other way round.
    reversed: bool,
    /// The tables that can hide a node from the reader: those with an `id UUID` column whose
    /// rows the reader's filter is asked about.
    nodes: Vec<&'a Table>,
    /// The tables with an `id UUID` column whose rows need a grant, for a reader with no
    /// principal that can hold one, each without that rule: a row of one that carries a node
    /// and that the reader's other rules let through is `PrincipalRequired`.
    closed: Vec<Table>,
    /// Each node property COLUMNS names, read as `hidden` reads: its node among `named`, tables.
    props: Vec<(usize, Vec<Carried<'a>>)>,
    /// The columns of a match before COLUMNS: `REACHED`, then the id of each node the
    /// pattern names, which are the nodes at these places in a match.
    scope: Scope<'a>,
    named: Vec<usize>,
    filter: Option<&'a Expr>,
    /// For each item of COLUMNS, the column of `scope` it gives.
    picks: Vec<usize>,
    /// Whether the reader may see each node met so far. Empty when no table can hide one.
    seen: RefCell<BTreeMap<Uuid, bool>>,
    /// The matches the filter has passed: a node is handed on once one through it has.
    passed: Cell<u64>,
}

impl<'a> Graph<'a> {
    /// The pattern as one statement matches it, and the scope of the rows it gives. Refuses,
    /// before any row is read: a table that is not there or holds no edges, a step that cannot
    /// be followed, a reader the edge table cannot answer, a table that carries nodes with no
    /// key or index that begins with `id`, and a WHERE or a COLUMNS that reads anything but
    /// `id`, `depth` and the id of a node the pattern names.
    pub(super) fn new(
        pattern: &'a GraphTable,
        given: &'a Given<'a>,
    ) -> Result<(Graph<'a>, Scope<'a>)> {
        let (catalog, reader, budget) = (given.catalog, given.reader, given.budget);
        let name = &pattern.edge_table;
        let edges = catalog
            .table(name)
            .ok_or_else(|| Error::TableNotFound(name.clone()))?;
        let column = |table: &Table, name: &str, column_type: ColumnType| {
            let mut columns = table.meta.columns.iter();
            columns.position(|column| column.name == name && column.column_type == column_type)
        };
        let (Some(source), Some(target), Some(typed)) = (
            column(edges, ENDS[0], ColumnType::Uuid),
            column(edges, ENDS[1], ColumnType::Uuid),
            column(edges, "edge_type", ColumnType::Text),
        ) else {
            return Err(Error::PlanError(format!(
                "{name} holds no edges: a graph pattern needs a table with source_id UUID, \
                 target_id UUID and edge_type TEXT"
            )));
        };
        for step in &pattern.steps {
            if step.min == 0 || step.min > step.max {
                return Err(Error::PlanError(format!(
                    "a step of a graph pattern is followed at least once, and its lower bound \
                     is not above its upper bound: {{{},{}}} cannot be followed",
                    step.min, step.max
                )));
            }
        }
        let bytes = catalog.table_count() * (size_of::<&Table>() + size_of::<Visible>());
        let mut held = budget.reserve(bytes as u64, "graph planning")?;
        let mut seeing = budget.reserve(0, "query")?;
        let visible = Visible::new(edges, catalog, reader, &mut seeing)?;

        let (mut nodes, mut closed, mut searched) = (Vec::new(), Vec::new(), Vec::new());
        for table in catalog.tables() {
            let Some((id, keyed)) = node_id(table) else {
                continue;
            };
            let mut asked = vec![false; table.meta.columns.len()];
            let mut seeing = budget.reserve(0, "query")?;
            let needs_principal = match Visible::new(table, catalog, reader, &mut seeing) {
                Ok(visible) => {
                    visible.reads(&mut asked);
                    false
                }
                Err(Error::PrincipalRequired { .. }) => true,
                Err(other) => return Err(other),
            };
            if keyed && !needs_principal && listed(given.allowed, &table.name) {
                searched.push((table, id));
            }
            if !needs_principal && !asked.contains(&true) {
                continue;
            }
            if !keyed {
                if !listed(given.allowed, &table.name) {
                    let message = "a graph pattern needs a node id key or index";
                    return Err(Error::PlanError(message.to_owned()));
                }
                return Err(Error::PlanError(format!(
                    "a graph pattern asks {} which nodes this handle may see, and needs a key \
                     or an index that begins with its id",
                    table.name
                )));
            }
            if needs_principal {
                held.grow(table.heap)?;
                let mut open = table.clone();
                open.rules.acl = None;
                closed.push(open);
            } else {
                nodes.push(table);
            }
        }

        // What a match is: its last node and depth, then each node the pattern names.
        let all = std::iter::once(&pattern.start).chain(pattern.steps.iter().map(|step| &step.to));
        let aliases = all.map(|node| node.alias.as_deref());
        let named: Vec<(usize, &str)> = aliases
            .enumerate()
            .filter_map(|(at, alias)| Some((at, alias?)))
            .collect();
        let mut scope = Scope::of(PATTERN, &REACHED[..]);
        for (_, alias) in &named {
            scope = scope.and(alias, &REACHED[..1]);
        }
        let filter = pattern.filter.as_ref();
        if let Some(filter) = filter {
            bind(filter, &scope, given.params)?;
        }
        let (mut picks, mut given_as, mut props) = (Vec::new(), Vec::new(), Vec::new());
        for column in &pattern.columns {
            let Expr::Column(wanted) = &column.expr else {
                return Err(Error::PlanError(
                    "COLUMNS gives id, depth and the id of a node the pattern names, and works \
                     nothing out of them"
                        .to_owned(),
                ));
            };
            let (alias, name) = (wanted.table.as_deref(), &wanted.column);
            let shown = alias.map_or(name.clone(), |node| format!("{node}.{name}"));
            let missing = || Error::PlanError(format!("project column not found: {shown}"));
            let node = named.iter().position(|(_, a)| Some(*a) == alias);
            let carry: Vec<Carried> = (searched.iter())
                .filter_map(|&(table, id)| Some((table, id, table.column(name).ok()?)))
                .collect();
            let typed = |&(table, _, at): &Carried| table.meta.columns[at].column_type;
            let (at, column_type, nullable) = match (scope.find(wanted), node, carry.first()) {
                (Ok((at, column_type)), ..) => (at, column_type, false),
                (_, Some(node), Some(first)) => {
                    let column_type = typed(first);
                    if carry.iter().any(|carries| typed(carries) != column_type) {
                        let them = carry.iter().map(|c| format!("{} {}", c.0.name, typed(c)));
                        return Err(Error::PlanError(format!(
                            "COLUMNS {shown} is read from the node's row, and the tables that \
                             carry it give different types: {}; a join gives it",
                            them.collect::<Vec<_>>().join(", ")
                        )));
                    }
                    let at = REACHED.len() + named.len() + props.len();
                    props.push((node, carry));
                    (at, column_type, true)
                }
                _ => return Err(missing()),
            };
            given_as.push(ColumnDef {
                name: column.name(),
                column_type,
                nullable,
                ..REACHED[0].clone()
            });
            picks.push(at);
        }
        let gives = Scope::of(PATTERN, pattern.output.0.get_or_init(|| given_as));

        let hop = matches!(pattern.steps.as_slice(), [step] if step.max == 1);
        let names = |node: &NodePattern| match (filter, &node.alias) {
            (Some(filter), Some(alias)) => named_by(filter, alias, &scope, &given.ctx),
            _ => Ok(None),
        };
        let mut starts = names(&pattern.start)?;
        let mut reversed = false;
        if hop && starts.is_none() {
            starts = names(&pattern.steps[0].to)?;
            reversed = starts.is_some();
        }
        // Explained, the first step reads the edges at each start, or every edge.
        if let Some(said) = given.ctx.said {
            let read = Source::new(edges, None, &given.ctx, given.read, visible);
            let way = (pattern.steps[0].direction == EdgeDirection::Incoming) != reversed;
            let near = starts.as_ref().map(|_| [source, target][usize::from(way)]);
            read.say(near.as_slice(), &[], &[])?;
            let unpinned = format!("  {}: no pinned vertex", edges.name);
            said.borrow_mut()
                .extend(starts.is_none().then_some(unpinned));
        }
        let graph = Graph {
            given,
            _held: held,
            edges,
            ends: [source, target],
            typed,
            steps: &pattern.steps,
            one: starts.as_ref().is_some_and(|(_, one)| *one),
            starts: starts.map(|(starts, _)| starts),
            reversed,
            nodes,
            closed,
            props,
            scope,
            named: named.into_iter().map(|(at, _)| at).collect(),
            filter,
            picks,
            seen: RefCell::new(BTreeMap::new()),
            passed: Cell::new(0),
        };
        Ok((graph, gives))
    }

    /// Matches the steps from the one at `at` on, from `starts` or from every node; `path`
    /// holds the nodes matched before them, `depth` edges from the first. Answers false once
    /// `each` wants no more.
    fn walk(
        &self,
        at: usize,
        starts: Option<&[Uuid]>,
        path: &mut Vec<Uuid>,
        depth: u32,
        reservation: &mut Reservation,
        each: &mut Match<'_>,
    ) -> Result<bool> {
        let step = &self.steps[at];
        let reached: &mut Reached<'_> = &mut |start, far, deep, reservation| {
            path.truncate(at);
            path.extend([start, far]);
            match self.steps.len() - at {
                1 => each(path, depth + deep, reservation),
                _ => self.walk(at + 1, Some(&[far]), path, depth + deep, reservation, each),
            }
        };
        match starts {
            Some(starts) => self.from(step, starts, reservation, reached),
            None => self.every_node(step, reservation, reached),
        }
    }

    /// One step from `starts`, breadth first: hands `each` every node some start reaches within
    /// the step's range, once, at the least such depth and with that start, the starts taking
    /// turns in their order. Each start walks on its own, so a node one start reaches too soon
    /// is still reached from another. A start is reached when an edge comes back to it.
    /// Remembers every node met from each start, asked of `reservation` before it is remembered.
    fn from(
        &self,
        step: &EdgeStep,
        starts: &[Uuid],
        reservation: &mut Reservation,
        each: &mut Reached<'_>,
    ) -> Result<bool> {
        // Every node met from each start at each depth up to the lower bound, and every node handed
        // on. Met at the bound a level after the start met it, a node is not followed again.
        let (mut met, mut told) = (BTreeSet::new(), BTreeSet::new());
        let mut frontier = Vec::new();
        for start in starts {
            reservation.grow(NODE)?;
            met.insert((*start, *start, 0));
            reservation.room(&mut frontier, usize::MAX)?;
            frontier.push((*start, *start));
        }
        let mut more = true;
        for depth in 1..=step.max {
            let mut next = Vec::new();
            for (node, start) in frontier {
                let reached: &mut Ends<'_> = &mut |_, far, reservation| {
                    let key = (start, far, depth.min(step.min));
                    if met.contains(&key) {
                        return Ok(true);
                    }
                    reservation.grow(NODE)?;
                    met.insert(key);
                    if depth != step.min || !met.contains(&(start, far, step.min - 1)) {
                        reservation.room(&mut next, usize::MAX)?;
                        next.push((far, start));
                    }
                    match depth < step.min || told.contains(&far) {
                        true => Ok(true),
                        false => {
                            let passed = self.passed.get();
                            let more = each(start, far, depth, reservation)?;
                            if self.passed.get() > passed {
                                reservation.grow(NODE)?;
                                told.insert(far);
                            }
                            Ok(more)
                        }
                    }
                };
                more = more && self.edges(step, Some(node), reservation, reached)?;
            }
            if !more {
                break;
            }
            frontier = next;
        }
        reservation.shrink(NODE * (met.len() + told.len()) as u64);
        Ok(more)
    }

    /// One step from every node. Each node with an edge is a start, so whatever the step
    /// reaches is one edge from a start, and of the starts as near the smallest id wins: hands
    /// `each` the far end of every edge once, with the smallest near end. Reads each edge
    /// once and remembers each far end, asked of `reservation` before it is remembered. A step
    /// of more than one edge is walked from each node with an edge, in the order of their ids.
    fn every_node(
        &self,
        step: &EdgeStep,
        reservation: &mut Reservation,
        each: &mut Reached<'_>,
    ) -> Result<bool> {
        // A filter that reads the first node or a depth past one may refuse the nearest start.
        let mut read = vec![false; REACHED.len() + self.named.len()];
        (self.filter).inspect(|filter| mark(filter, &self.scope, &mut read));
        let first = self.named.first() == Some(&0) && read[REACHED.len()];
        if step.min > 1 || first || read[1] && step.max > 1 {
            let mut starts = BTreeSet::new();
            self.edges(step, None, reservation, &mut |near, _, reservation| {
                if !starts.contains(&near) {
                    reservation.grow(NODE * if starts.is_empty() { 4 } else { 1 })?;
                    starts.insert(near);
                }
                Ok(true)
            })?;
            let starts: Vec<Uuid> = starts.into_iter().collect();
            let more = self.from(step, &starts, reservation, each);
            reservation.shrink(NODE * starts.len() as u64);
            return more;
        }
        let mut nearest = BTreeMap::new();
        self.edges(step, None, reservation, &mut |near, far, reservation| {
            match nearest.entry(far) {
                Entry::Vacant(new) => {
                    reservation.grow(NODE)?;
                    new.insert(near);
                }
                Entry::Occupied(mut held) => {
                    if near < *held.get() {
                        held.insert(near);
                    }
                }
            }
            Ok(true)
        })?;
        let held = NODE * nearest.len() as u64;
        let mut more = true;
        for (far, near) in nearest {
            more = more && each(near, far, 1, reservation)?;
        }
        reservation.shrink(held);
        Ok(more)
    }

    /// Hands `each` the two ends of every edge of the step that the reader may follow, the
    /// near end first: the edges at `near`, read through the index that begins with that end,
    /// or with no node every edge of the table. An edge is followed when its row passes the
    /// reader's filter and the reader may see both its ends. An edge from a node to itself is
    /// one edge whichever way it is followed. Answers false once `each` wants no more.
    fn edges(
        &self,
        step: &EdgeStep,
        near: Option<Uuid>,
        reservation: &mut Reservation,
        each: &mut Ends<'_>,
    ) -> Result<bool> {
        // Which end is the near one: the first of `ENDS`, the second, or each in turn.
        let ways: &[usize] = match (step.direction, self.reversed) {
            (EdgeDirection::Both, _) => &[0, 1],
            (EdgeDirection::Outgoing, false) | (EdgeDirection::Incoming, true) => &[0],
            _ => &[1],
        };
        let typed = step.edge_type.clone().map(Value::Text);
        let mut give = |row: &[Value], way: usize, reservation: &mut Reservation| -> Result<bool> {
            let ends = (&row[self.ends[way]], &row[self.ends[1 - way]]);
            let (Value::Uuid(near), Value::Uuid(far)) = ends else {
                return Ok(true);
            };
            let seen =
                self.sees(*near, false, reservation)? && self.sees(*far, false, reservation)?;
            Ok(!seen || each(*near, *far, reservation)?)
        };
        let Some(node) = near else {
            let either: &mut Rows<'_> = &mut |row, reservation| {
                let own = row[self.ends[0]] == row[self.ends[1]];
                for way in ways.iter().take(if own { 1 } else { 2 }) {
                    if !give(row, *way, reservation)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            };
            let every = typed.map(|edge_type| equal(self.edges, &[self.typed], &[edge_type]));
            return self.read(self.edges, every.as_ref(), &self.ends, reservation, either);
        };
        for (turn, way) in ways.iter().enumerate() {
            let values: Vec<Value> = once(Value::Uuid(node)).chain(typed.clone()).collect();
            let at = [self.ends[*way], self.typed];
            let at = equal(self.edges, &at[..values.len()], &values);
            // The second way round, an edge from the node to itself has been followed.
            let row: &mut Rows<'_> = &mut |row, reservation| {
                let own = row[self.ends[0]] == row[self.ends[1]];
                Ok((turn == 1 && own) || give(row, *way, reservation)?)
            };
            if !self.read(self.edges, Some(&at), &self.ends, reservation, row)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Whether the reader may see a node, looked up when the node is first met and remembered
    /// from then on, asked of `reservation` before it is. A node that is hidden and `named` by
    /// the pattern's filter is the refusal of the rule that hides it.
    fn sees(&self, node: Uuid, named: bool, reservation: &mut Reservation) -> Result<bool> {
        if self.nodes.is_empty() && self.closed.is_empty() {
            return Ok(true);
        }
        if let Some(seen) = self.seen.borrow().get(&node) {
            return Ok(*seen);
        }
        reservation.grow(NODE)?;
        match self.hidden(node, reservation)? {
            Some(refusal) if named && disclosed(self.given.allowed, &refusal) => Err(refusal),
            hidden => {
                self.seen.borrow_mut().insert(node, hidden.is_none());
                Ok(hidden.is_none())
            }
        }
    }

    /// The refusal of the rule that hides a node from the reader: some table that can hide a
    /// node has a row with the node's id that the reader may not see. `None` for a node the
    /// reader may see, as one that no row carries is.
    fn hidden(&self, node: Uuid, reservation: &mut Reservation) -> Result<Option<Error>> {
        let open = self.nodes.iter().map(|table| (*table, false));
        for (table, closed) in open.chain(self.closed.iter().map(|table| (table, true))) {
            let id = table
                .meta
                .columns
                .iter()
                .position(|column| column.name == "id");
            let carries = equal(table, &[id.expect("a node's id")], &[Value::Uuid(node)]);
            let mut carried = false;
            let row: &mut Rows<'_> = &mut |_, _| {
                carried = true;
                Ok(true)
            };
            match self.read(table, Some(&carries), &[], reservation, row) {
                Err(
                    refusal @ (Error::ContextScopeViolation { .. }
                    | Error::ScopeLabelViolation { .. }
                    | Error::AclDenied { .. }),
                ) => {
                    if disclosed(self.given.allowed, &refusal) {
                        return Ok(Some(refusal));
                    }
                    continue;
                }
                read => read?,
            };
            if closed && carried {
                let table = table.name.clone();
                if listed(self.given.allowed, &table) {
                    return Err(Error::PrincipalRequired { table });
                }
            }
        }
        Ok(None)
    }

    /// Reads through the table source the rows of `table` that `filter` passes, with its
    /// parameters bound, and hands `each` every one the reader may see, with its `wanted`
    /// columns. With no column wanted the rows are those that carry one node, and one the
    /// reader may not see is the refusal of the rule that hides it. Answers false once `each`
    /// wants no more.
    fn read(
        &self,
        table: &Table,
        filter: Option<&(Expr, HashMap<String, Value>)>,
        wanted: &[usize],
        reservation: &mut Reservation,
        each: &mut Rows<'_>,
    ) -> Result<bool> {
        let none = HashMap::new();
        let (filter, params) =
            filter.map_or((None, &none), |(filter, bound)| (Some(filter), bound));
        let width = table.meta.columns.len();
        let (mut filtered, mut taken) = (vec![false; width], vec![false; width]);
        if let Some(filter) = filter {
            reads(filter, &table.scope(), params, &mut filtered)?;
        }
        for column in wanted {
            taken[*column] = true;
        }
        // What the reader's filter holds for this read is given back with it.
        let (given, ctx) = (self.given, EvalContext::new(params, 0, self.given.ctx.kept));
        let mut seeing = given.budget.reserve(0, "query")?;
        let visible = Visible::new(table, given.catalog, given.reader, &mut seeing)?;
        let count = listed(given.allowed, &table.name);
        let source = Source::new(table, filter, &ctx, given.read, visible)
            .counted(given.examined.filter(|_| count))
            .reading(filtered, &taken);
        let mut path = source.choose(&[], &[])?;
        path.one = wanted.is_empty();
        let mut more = true;
        source.walk(&path, reservation, |_, row, bytes, reservation| {
            more = each(&row, reservation)?;
            reservation.shrink(bytes);
            Ok(more)
        })?;
        Ok(more)
    }
}

/// Where `table` keeps a node's `id UUID`, and whether its key or an index begins with it.
pub(super) fn node_id(table: &Table) -> Option<(usize, bool)> {
    let columns = &table.meta.columns;
    let id = (columns.iter()).position(|c| c.name == "id" && c.column_type == ColumnType::Uuid)?;
    let leads = |index: &IndexDecl| {
        let first = index.columns.first();
        index.json_path.is_empty() && first.is_some_and(|(name, _)| name == "id")
    };
    let keyed = table.key.first() == Some(&id) || table.meta.indexes.iter().any(leads);
    Some((id, keyed))
}

fn listed(allowed: Option<&BTreeSet<String>>, table: &str) -> bool {
    allowed.is_none_or(|names| names.contains(table))
}

fn disclosed(allowed: Option<&BTreeSet<String>>, error: &Error) -> bool {
    match error {
        Error::ContextScopeViolation { table }
        | Error::ScopeLabelViolation { table }
        | Error::AclDenied { table, .. }
        | Error::PrincipalRequired { table } => listed(allowed, table),
        _ => true,
    }
}

impl plan::Source for Graph<'_> {
    /// A row for every match the pattern's filter passes: what COLUMNS names. The one start an
    /// `=` names, when the reader may not see it, is the refusal of the rule that hides it.
    fn rows(&self, reservation: &mut Reservation, each: &mut Each<'_>) -> Result<()> {
        let starts = self.starts.iter().flatten();
        for start in starts.clone() {
            reservation.grow(NODE)?;
            self.sees(*start, self.one, reservation)?;
        }
        let matched: &mut Match<'_> = &mut |path, depth, reservation| {
            let mut row = vec![
                Value::Uuid(path[path.len() - 1]),
                Value::Int64(depth.into()),
            ];
            row.extend(self.named.iter().map(|at| Value::Uuid(path[*at])));
            if let Some(filter) = self.filter
                && *eval(filter, &self.scope, &row, &self.given.ctx)? != Value::Bool(true)
            {
                return Ok(true);
            }
            self.passed.set(self.passed.get() + 1);
            for (node, tables) in &self.props {
                let (node, mut value) = (Value::Uuid(path[self.named[*node]]), Value::Null);
                for &(table, id, at) in tables {
                    let carries = equal(table, &[id], std::slice::from_ref(&node));
                    let found: &mut Rows<'_> = &mut |found, _| {
                        value = found[at].clone();
                        Ok(false)
                    };
                    if !self.read(table, Some(&carries), &[at], reservation, found)? {
                        break;
                    }
                }
                row.push(value);
            }
            row = self.picks.iter().map(|at| row[*at].clone()).collect();
            let bytes = row_overhead(row.len()) + row.iter().map(heap_bytes).sum::<u64>();
            reservation.grow(bytes)?;
            each(row, bytes, reservation)
        };
        match self.steps {
            [hop] if hop.max == 1 => {
                let give: &mut Ends<'_> = &mut |near, far, reservation| match self.reversed {
                    true => matched(&[far, near], 1, reservation),
                    false => matched(&[near, far], 1, reservation),
                };
                match &self.starts {
                    None => {
                        self.edges(hop, None, reservation, give)?;
                    }
                    Some(starts) => {
                        for start in starts {
                            if !self.edges(hop, Some(*start), reservation, give)? {
                                break;
                            }
                        }
                    }
                }
            }
            _ => {
                let starts = self.starts.as_deref();
                self.walk(0, starts, &mut Vec::new(), 0, reservation, matched)?;
            }
        }
        let seen = std::mem::take(&mut *self.seen.borrow_mut());
        reservation.shrink(NODE * (seen.len() + starts.count()) as u64);
        Ok(())
    }
}

/// The nodes a filter names for the node of a pattern called `alias`, each once, and whether
/// they are the one an `=` names: the values every case of the filter gives `alias.id`. `None`
/// when a case gives it none.
fn named_by(
    filter: &Expr,
    alias: &str,
    scope: &Scope,
    ctx: &EvalContext,
) -> Result<Option<(Vec<Uuid>, bool)>> {
    let id = ColumnRef {
        table: Some(alias.to_owned()),
        column: "id".to_owned(),
    };
    let at = scope.find(&id)?.0;
    let reading = read(Some(filter), &Columns { scope, table: None }, ctx)?;
    let (mut nodes, mut once) = (Vec::new(), BTreeSet::new());
    for case in &reading.cases {
        let Some((_, Fact::Among { values, .. })) = case.iter().find(|(column, _)| *column == at)
        else {
            return Ok(None);
        };
        let ids = values.iter().filter_map(|value| match value {
            Value::Uuid(node) => Some(*node),
            _ => None,
        });
        nodes.extend(ids.filter(|node| once.insert(*node)));
    }
    // Every case names the same node by `=`.
    let one = nodes.len() == 1
        && (reading.cases.iter()).all(|case| {
            case.iter().any(|(column, fact)| {
                *column == at && matches!(fact, Fact::Among { one: true, .. })
            })
        });
    Ok(Some((nodes, one)))
}
