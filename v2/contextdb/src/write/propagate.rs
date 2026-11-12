//! Propagation: a row that changes its state is followed, inside the statement that changed
//! it, by the rows that declared they follow that change. Those are the rows of a table whose
//! reference says `ON STATE s PROPAGATE SET t`, and the rows of the row's own table that an
//! edge joins it to under `PROPAGATE ON EDGE`. A row that moved by following is followed in
//! turn.
//!
//! The rows a statement names are all written first, and what follows them is then walked
//! nearest first. So a row is reached over the fewest edges there are, and a rule's
//! `MAX DEPTH` counts the steps (edges, or references) between a row and a row the statement
//! itself changed.
//!
//! A follower is found whoever wrote, and written as the statement's reader: one that reader
//! may not write refuses the statement.

use std::collections::VecDeque;

use super::{Given, Rules, column, equal, split};
use crate::memory::Reservation;
use crate::query::{Reader, Visible};
use crate::schema::Table;
use crate::sql::ast::{EdgePropagation, Propagation};
use crate::store::{Beside, ChangeSet, Read};
use crate::types::{Error, Result, Value};
use crate::vector::Tally;

/// How many edges from a changed row a rule reaches when it names no `MAX DEPTH`.
const MAX_DEPTH: u32 = 10;

/// The rows that are to follow one row that changed its state.
pub(super) struct Step {
    /// The table whose rows follow.
    table: String,
    via: Via,
    /// What finds them: the values the reference's columns hold in them, or the id of the row
    /// that changed.
    values: Vec<Value>,
    /// The memory the step takes, given back when it has been followed.
    _held: Reservation,
}

/// The declaration a step follows, by its place among those of the step's table.
enum Via {
    Reference(usize),
    Edge(usize),
}

impl Rules<'_> {
    /// Notes the rows that follow a row of the table which an UPDATE takes from `old` to
    /// `row`: none unless the row changes its state. `left` holds the row's large values that
    /// were not read.
    pub(super) fn moved(
        &self,
        old: &[Value],
        row: &[Value],
        left: &[(usize, Beside)],
        read: &dyn Read,
    ) -> Result<()> {
        let table = self.table;
        let Some(machine) = &table.meta.state_machine else {
            return Ok(());
        };
        let at = column(table, &machine.column)?;
        let Value::Text(state) = &row[at] else {
            return Ok(());
        };
        if old[at] == row[at] {
            return Ok(());
        }
        let step = |follower: &Table, via: Via, columns: &[usize]| -> Result<()> {
            let bytes = size_of::<Step>() + columns.len() * size_of::<Value>();
            let mut held = self.given.budget.reserve(bytes as u64, "query")?;
            let mut values = Vec::new();
            for &at in columns {
                values.push(match left.iter().find(|unread| unread.0 == at) {
                    Some((_, beside)) => read.load(beside, &mut held)?,
                    None => {
                        held.grow(crate::types::heap_bytes(&row[at]))?;
                        row[at].clone()
                    }
                });
            }
            self.followed.borrow_mut().push(Step {
                table: follower.name.clone(),
                via,
                values,
                _held: held,
            });
            Ok(())
        };
        for child in self.given.catalog.tables() {
            for (place, reference) in child.rules.references.iter().enumerate() {
                let rule = reference.on_state.as_ref();
                if reference.parent == table.name && rule.is_some_and(|r| r.on_state == *state) {
                    let columns = reference.parent_columns.iter();
                    let columns = columns.map(|name| column(table, name));
                    step(
                        child,
                        Via::Reference(place),
                        &columns.collect::<Result<Vec<_>>>()?,
                    )?;
                }
            }
        }
        for (place, edge) in table.rules.propagate_edges.iter().enumerate() {
            if edge.rule.on_state == *state {
                step(table, Via::Edge(place), &[column(table, "id")?])?;
            }
        }
        Ok(())
    }
}

/// One statement's propagation: the steps not yet followed, nearest first.
struct Walk<'a> {
    /// What the statement is run with, read as a handle that is shown every row.
    given: &'a Given<'a>,
    /// Who writes the rows that follow.
    writer: &'a Reader,
    /// Each step with the number of edges between its rows and a row the statement changed.
    queue: VecDeque<(u32, Step)>,
    /// That number for the step being followed.
    depth: u32,
}

/// Follows every change of state the statement made to the rows of its table, and every change
/// made by following. Called once the statement's own rows are written, so the statement finds
/// its rows as they were before any of them was followed.
///
/// # Panics
///
/// When a step names a table that is not in the statement's catalog.
pub(super) fn follow(rules: &Rules, changes: &mut ChangeSet, partitions: &mut Tally) -> Result<()> {
    let first = rules.followed.take();
    if first.is_empty() {
        return Ok(());
    }
    let admin = Reader::admin();
    let given = Given {
        reader: &admin,
        ..*rules.given
    };
    let mut walk = Walk {
        given: &given,
        writer: rules.given.reader,
        queue: first.into_iter().map(|step| (1, step)).collect(),
        depth: 1,
    };
    while let Some((depth, step)) = walk.queue.pop_front() {
        walk.depth = depth;
        let table = given.catalog.table(&step.table);
        let table = table.expect("a step names a table of the statement's catalog");
        match step.via {
            Via::Reference(place) => {
                let reference = &table.rules.references[place];
                if let Some(rule) = &reference.on_state {
                    let columns = &reference.columns;
                    walk.shift(table, columns, &step.values, rule, changes, partitions)?;
                }
            }
            Via::Edge(place) => {
                let edge = &table.rules.propagate_edges[place];
                walk.along(table, edge, &step.values[0], changes, partitions)?;
            }
        }
    }
    Ok(())
}

impl Walk<'_> {
    /// True when the rule reaches the rows of the step being followed.
    fn reaches(&self, rule: &Propagation) -> bool {
        self.depth <= rule.max_depth.unwrap_or(MAX_DEPTH)
    }

    /// Moves the rows of `table` that an edge of the declared type joins to the row with this
    /// id, over every table of edges: OUTGOING follows an edge from its source to its target,
    /// INCOMING from its target to its source, and BOTH either way.
    fn along(
        &mut self,
        table: &Table,
        edge: &EdgePropagation,
        id: &Value,
        changes: &mut ChangeSet,
        partitions: &mut Tally,
    ) -> Result<()> {
        if !self.reaches(&edge.rule) {
            return Ok(());
        }
        let given = self.given;
        let key = [column(table, "id")?];
        for edges in given.catalog.tables() {
            let mut indexes = edges.meta.indexes.iter();
            if !indexes.any(|index| index.name.starts_with("__graph_edge_")) {
                continue;
            }
            let ends = [column(edges, "source_id")?, column(edges, "target_id")?];
            let kind = column(edges, "edge_type")?;
            let rules = Rules::of(edges, given)?;
            for (near, far) in [(0, 1), (1, 0)] {
                if edge.direction == ["INCOMING", "OUTGOING"][near] {
                    continue;
                }
                let values = [id.clone(), Value::Text(edge.edge_type.clone())];
                let (filter, bound) = equal(edges, &[ends[near], kind], &values);
                let needed = vec![false; edges.meta.columns.len()];
                let asked = rules.asked(Some(&filter), &bound, &[], &needed, given.reader);
                rules.each(&asked, changes, |changes, _, cells| {
                    let other = split(cells).0.swap_remove(ends[far]);
                    self.shift(table, &key, &[other], &edge.rule, changes, partitions)
                })?;
            }
        }
        Ok(())
    }

    /// Moves to the state `rule` sets every row of `table` that holds `values` in `columns`
    /// and whose state lists that state as a next one, and notes who follows each. A row
    /// already in that state is passed by. A row in any other state is left as it is, or,
    /// under `ABORT ON FAILURE`, refuses the statement; so is every row of a table whose rows
    /// make no move: one with no state machine, and one that is IMMUTABLE or whose state is.
    fn shift(
        &mut self,
        table: &Table,
        columns: &[usize],
        values: &[Value],
        rule: &Propagation,
        changes: &mut ChangeSet,
        partitions: &mut Tally,
    ) -> Result<()> {
        if !self.reaches(rule) || values.contains(&Value::Null) {
            return Ok(());
        }
        let machine = table.meta.state_machine.as_ref();
        let state = match machine {
            Some(machine) if !table.meta.immutable => Some(column(table, &machine.column)?),
            _ => None,
        };
        let state = state.filter(|at| !table.rules.immutable_columns.contains(at));
        if state.is_none() && !rule.abort_on_failure {
            return Ok(());
        }
        let given = self.given;
        let rules = Rules::of(table, given)?;
        let mut seeing = given.budget.reserve(0, "query")?;
        let mut visible = Visible::new(table, given.catalog, self.writer, &mut seeing)?;
        // What the write of a row reads when its state is all that changes.
        let mut needed = vec![false; table.meta.columns.len()];
        visible.reads(&mut needed);
        if let Some(state) = state {
            needed[state] = true;
            rules.needs(&[state], &mut needed);
        }
        let (filter, bound) = equal(table, columns, values);
        let assigned = state.as_slice();
        let asked = rules.asked(Some(&filter), &bound, assigned, &needed, given.reader);
        let to = Value::Text(rule.set.clone());
        rules.each(&asked, changes, |changes, key, cells| {
            let (old, left) = split(cells);
            if state.is_some_and(|at| old[at] == to) {
                return Ok(());
            }
            let leaves = |at: &usize| {
                let mut listed = machine.into_iter().flat_map(|machine| &machine.transitions);
                listed.any(|(from, next)| {
                    matches!(&old[*at], Value::Text(now) if now == from) && next.contains(&rule.set)
                })
            };
            let Some(at) = state.filter(leaves) else {
                if !rule.abort_on_failure {
                    return Ok(());
                }
                return Err(Error::PropagationAborted {
                    table: table.name.clone(),
                    reason: format!(
                        "a row that follows cannot be moved to {}: its table lists no such \
                         move from the state the row is in",
                        rule.set
                    ),
                });
            };
            let mut row = old.clone();
            row[at] = to.clone();
            rules.moved(&old, &row, &left, &*changes)?;
            let was = Some((&key, &*old));
            rules.put(changes, partitions, &mut visible, was, row, left)?;
            Ok(())
        })?;
        let next = self.depth + 1;
        let noted = rules.followed.take().into_iter();
        self.queue.extend(noted.map(|step| (next, step)));
        Ok(())
    }
}
