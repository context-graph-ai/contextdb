//! A graph pattern: the rows `GRAPH_TABLE(...)` gives over a table of edges.
//!
//! One step followed once is a single hop: one match for each edge. Anything longer is a walk,
//! breadth first: a node is reached once, at the shallowest depth, from the start that comes
//! first, and a start is reached again when a cycle within the range comes back to it. What the
//! reader may not see is taken out before anything is matched: an edge is followed only when
//! the reader may see its row and the nodes at both its ends. Every edge and every row that
//! carries a node is read through the one table source, with the reader's filter in it.

use std::borrow::Cow;
use std::cell::{Cell, RefCell};
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::iter::once;
use std::sync::LazyLock;

use super::bind::bind;
use super::expression::{EvalContext, Scope, eval, reads};
use super::plan::{self, Each, Given};
use super::row_overhead;
use super::table::{Source, as_key, equal, known};
use super::visible::Visible;
use crate::memory::Reservation;
use crate::schema::Table;
use crate::sql::Expr;
use crate::sql::ast::{BinOp, ColumnRef};
use crate::sql::plan::{EdgeDirection, EdgeStep, GraphTable, NodePattern};
use crate::types::{ColumnDef, ColumnType, Error, IndexDecl, Result, Uuid, Value};

/// The columns that hold the two ends of an edge: where it leaves, then where it arrives.
const ENDS: [&str; 2] = ["source_id", "target_id"];

/// The bytes a walk is counted for each node it remembers: the node's id, and beside it the
/// node's place among the nodes met and among those still to follow.
const NODE: u64 = 16 + 24;

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

/// What takes the rows of one read.
type Rows<'e> = dyn FnMut(&[Value], &mut Reservation) -> Result<bool> + 'e;

/// One pattern over one table of edges, for one reader.
pub(super) struct Graph<'a> {
    given: &'a Given<'a>,
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
    /// The columns of a match before COLUMNS: `REACHED`, then the id of each node the
    /// pattern names, which are the nodes at these places in a match.
    scope: Scope<'a>,
    named: Vec<usize>,
    filter: Option<&'a Expr>,
    /// For each item of COLUMNS, the column of `scope` it gives.
    picks: Vec<usize>,
    /// Whether the reader may see each node met so far. Empty when no table can hide one.
    seen: RefCell<BTreeMap<Uuid, bool>>,
    /// The stored rows read so far.
    examined: Cell<u64>,
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
        let mut seeing = budget.reserve(0, "query")?;
        let visible = Visible::new(edges, catalog, reader, &mut seeing)?;

        let (mut nodes, mut closed) = (Vec::new(), Vec::new());
        for table in catalog.tables() {
            let Some(id) = column(table, "id", ColumnType::Uuid) else {
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
            if !needs_principal && !asked.contains(&true) {
                continue;
            }
            let leads = |index: &IndexDecl| {
                let first = index.columns.first();
                index.json_path.is_empty() && first.is_some_and(|(name, _)| name == "id")
            };
            if table.key.first() != Some(&id) && !table.meta.indexes.iter().any(leads) {
                return Err(Error::PlanError(format!(
                    "a graph pattern asks {} which nodes this handle may see, and needs a key \
                     or an index that begins with its id",
                    table.name
                )));
            }
            if needs_principal {
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
        let mut picks = Vec::new();
        let mut given_as = Vec::new();
        for column in &pattern.columns {
            let Expr::Column(named) = &column.expr else {
                return Err(Error::PlanError(
                    "COLUMNS gives id, depth and the id of a node the pattern names, and works \
                     nothing out of them"
                        .to_owned(),
                ));
            };
            let (at, column_type) = scope.find(named).map_err(|_| {
                let of = named.table.as_ref().map(|node| format!("{node}."));
                let (of, column) = (of.unwrap_or_default(), &named.column);
                Error::PlanError(format!("project column not found: {of}{column}"))
            })?;
            given_as.push(ColumnDef {
                name: column.name(),
                column_type,
                ..REACHED[0].clone()
            });
            picks.push(at);
        }
        let gives = Scope::of(PATTERN, pattern.output.0.get_or_init(|| given_as));

        let hop = matches!(pattern.steps.as_slice(), [step] if step.max == 1);
        let names = |node: &NodePattern| match (filter, &node.alias) {
            (Some(filter), Some(alias)) => named_by(filter, alias, &given.ctx),
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
            edges,
            ends: [source, target],
            typed,
            steps: &pattern.steps,
            one: starts.as_ref().is_some_and(|(_, one)| *one),
            starts: starts.map(|(starts, _)| starts),
            reversed,
            nodes,
            closed,
            scope,
            named: named.into_iter().map(|(at, _)| at).collect(),
            filter,
            picks,
            seen: RefCell::new(BTreeMap::new()),
            examined: Cell::new(0),
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
        // Every node met from each start, and whether it has been reached: a start is met
        // before it is; and every node handed on.
        let (mut met, mut told) = (BTreeMap::new(), BTreeSet::new());
        let mut frontier = Vec::new();
        for start in starts {
            reservation.grow(NODE)?;
            met.insert((*start, *start), false);
            frontier.push((*start, *start));
        }
        let mut more = true;
        for depth in 1..=step.max {
            let mut next = Vec::new();
            for (node, start) in frontier {
                let reached: &mut Ends<'_> = &mut |_, far, reservation| {
                    match met.get_mut(&(start, far)) {
                        Some(true) => return Ok(true),
                        // A start the walk has come back to: its edges are followed already.
                        Some(reached) => *reached = true,
                        None => {
                            reservation.grow(NODE)?;
                            met.insert((start, far), true);
                            next.push((far, start));
                        }
                    }
                    match depth < step.min || told.contains(&far) {
                        true => Ok(true),
                        false => {
                            reservation.grow(NODE)?;
                            told.insert(far);
                            each(start, far, depth, reservation)
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
        if step.min > 1 {
            let mut starts = BTreeSet::new();
            self.edges(step, None, reservation, &mut |near, _, reservation| {
                reservation.grow(NODE * u64::from(starts.insert(near)))?;
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
            Some(refusal) if named => Err(refusal),
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
                    // The row that hides the node was read.
                    self.examined.set(self.examined.get() + 1);
                    return Ok(Some(refusal));
                }
                read => read?,
            };
            if closed && carried {
                let table = table.name.clone();
                return Err(Error::PrincipalRequired { table });
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
        let (given, ctx) = (self.given, EvalContext::new(params, 0));
        let mut seeing = given.budget.reserve(0, "query")?;
        let visible = Visible::new(table, given.catalog, given.reader, &mut seeing)?;
        let source =
            Source::new(table, filter, &ctx, given.read, visible).reading(filtered, &taken);
        let mut path = source.choose(&[], &[])?;
        path.one = wanted.is_empty();
        let mut more = true;
        let examined = source.walk(&path, reservation, |_, row, bytes, reservation| {
            more = each(&row, reservation)?;
            reservation.shrink(bytes);
            Ok(more)
        })?;
        self.examined.set(self.examined.get() + examined);
        Ok(more)
    }
}

impl plan::Source for Graph<'_> {
    /// A row for every match the pattern's filter passes: what COLUMNS names. The one start an
    /// `=` names, when the reader may not see it, is the refusal of the rule that hides it.
    fn rows(&self, reservation: &mut Reservation, each: &mut Each<'_>) -> Result<u64> {
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
                && eval(filter, &self.scope, &row, &self.given.ctx)? != Value::Bool(true)
            {
                return Ok(true);
            }
            row = self.picks.iter().map(|at| row[*at].clone()).collect();
            let bytes = row_overhead(row.len());
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
        Ok(self.examined.take())
    }
}

/// The nodes a filter names for the node of a pattern called `alias`, each once and in the
/// order it names them, and whether they are the one an `=` names: the values of the first
/// `alias.id = value` or `alias.id IN (...)` among the terms joined by AND at its top, a value
/// or list a SELECT works out included, and of both sides of an OR that each name some. NULL
/// names no node. `None` when it names none that way.
fn named_by(filter: &Expr, alias: &str, ctx: &EvalContext) -> Result<Option<(Vec<Uuid>, bool)>> {
    let id = |expr: &Expr| {
        matches!(expr, Expr::Column(ColumnRef { table: Some(table), column })
            if table == alias && column == "id")
    };
    let worked = |value: &Expr| eval(value, &Scope::default(), &[], ctx);
    // A SELECT's values are read where they are held, never copied.
    let (values, one): (Cow<[Value]>, bool) = match filter {
        Expr::BinaryOp { left, op, right } => match op {
            BinOp::And => {
                let left = named_by(left, alias, ctx)?;
                return left.map_or_else(|| named_by(right, alias, ctx), |left| Ok(Some(left)));
            }
            BinOp::Or => match (named_by(left, alias, ctx)?, named_by(right, alias, ctx)?) {
                (Some((left, _)), Some((right, _))) => (
                    Cow::Owned(left.into_iter().chain(right).map(Value::Uuid).collect()),
                    false,
                ),
                _ => return Ok(None),
            },
            BinOp::Eq if id(left) && known(right, ctx) => (vec![worked(right)?].into(), true),
            BinOp::Eq if id(right) && known(left, ctx) => (vec![worked(left)?].into(), true),
            _ => return Ok(None),
        },
        Expr::InList {
            expr,
            list,
            negated: false,
        } if id(expr) && list.iter().all(|value| known(value, ctx)) => (
            Cow::Owned(list.iter().map(worked).collect::<Result<_>>()?),
            false,
        ),
        // The values of a SELECT after IN are worked out before the pattern is matched.
        Expr::InSubquery {
            expr,
            query,
            negated: false,
        } if id(expr) => (ctx.subquery(query)?.into(), false),
        _ => return Ok(None),
    };
    let (mut nodes, mut once) = (Vec::new(), BTreeSet::new());
    for value in values.iter().filter(|value| **value != Value::Null) {
        // A value that is no node's id names no node, and the filter is asked of every match.
        let Some(Value::Uuid(node)) = as_key(ColumnType::Uuid, value.clone()) else {
            return Ok(None);
        };
        nodes.extend(once.insert(node).then_some(node));
    }
    Ok(Some((nodes, one)))
}
