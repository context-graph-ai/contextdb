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
//!
//! No step is held: each is queued as a note, and the followers end parts as the statement does.

use super::{Given, Part, Rules, split};
use crate::query::{Kept, Reader, Visible, equal};
use crate::schema::Table;
use crate::sql::ast::{EdgePropagation, Propagation};
use crate::store::{Beside, ChangeSet, Key, Read, put_value, read_value};
use crate::types::{Error, Result, Value, heap_bytes};
use crate::vector::Tally;

/// How many edges from a changed row a rule reaches when it names no `MAX DEPTH`.
const MAX_DEPTH: u32 = 10;

/// The notes of the steps, under the follower table: reference (0) or edge (1), its place, then
/// the values that find the rows.
const STEP: u8 = b'P';

impl Rules<'_> {
    /// Notes the rows that follow a row of the table which an UPDATE takes from `old` to
    /// `row`: none unless the row changes its state. `left` holds the row's large values that
    /// were not read.
    pub(super) fn moved(
        &self,
        old: &[Value],
        row: &[Value],
        left: &[(usize, Beside)],
        changes: &mut ChangeSet,
    ) -> Result<()> {
        let table = self.table;
        let Some(machine) = &table.meta.state_machine else {
            return Ok(());
        };
        let at = table.column(&machine.column)?;
        let Value::Text(state) = &row[at] else {
            return Ok(());
        };
        if old[at] == row[at] {
            return Ok(());
        }
        let mut step = |follower: &Table, via: u8, place: usize, columns: &[usize]| -> Result<()> {
            let mut held = self.given.budget.reserve(0, "query_decode")?;
            let mut note = [&[via][..], &(place as u32).to_be_bytes()].concat();
            for &at in columns {
                let unread = left.iter().find(|unread| unread.0 == at);
                let loaded = unread.map(|(_, beside)| changes.load(beside, &mut held));
                let loaded = loaded.transpose()?;
                let value = loaded.as_ref().unwrap_or(&row[at]);
                held.grow(heap_bytes(value))?;
                put_value(&mut note, value);
            }
            changes.queue(STEP, follower.id, &Key::from_bytes(&note))
        };
        for child in self.given.catalog.children(table) {
            for (place, reference) in child.rules.references.iter().enumerate() {
                let rule = reference.on_state.as_ref();
                if reference.parent == table.name && rule.is_some_and(|r| r.on_state == *state) {
                    let columns = reference.parent_columns.iter();
                    let columns = columns.map(|name| table.column(name));
                    step(child, 0, place, &columns.collect::<Result<Vec<_>>>()?)?;
                }
            }
        }
        for (place, edge) in table.rules.propagate_edges.iter().enumerate() {
            if edge.rule.on_state == *state {
                step(table, 1, place, &[table.column("id")?])?;
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
    /// How the statement ends a part: between two batches of the rows that follow.
    part: Option<Part<'a>>,
    /// The number of edges between the step's rows and a row the statement changed.
    depth: u32,
}

/// Follows every change of state the statement made to the rows of its table, and every change
/// made by following: its steps, queued after `rules.from`, a level at a time, each level the
/// steps queued while the one before it was followed. Called once the statement's own rows are
/// written, so the statement finds its rows as they were before any of them was followed.
///
/// # Panics
///
/// When a step names a table that is not in the statement's catalog.
pub(super) fn follow(rules: &Rules, changes: &mut ChangeSet, partitions: &mut Tally) -> Result<()> {
    if changes.queued == rules.from {
        return Ok(());
    }
    changes.operation = Some(crate::types::TriggerEvent::Update);
    let admin = Reader::admin();
    let narrowed = !rules.given.reader.is_admin();
    // A follower is read as the rows stand, never from the moment the set began at.
    let given = Given {
        reader: &admin,
        examined: rules.given.examined.filter(|_| !narrowed),
        part: None,
        ..*rules.given
    };
    let mut walk = Walk {
        given: &given,
        writer: rules.given.reader,
        part: rules.part(changes),
        depth: 1,
    };
    // The last step of the level being followed: those queued after it are one step further.
    let mut after = [&[STEP][..], &rules.from.to_be_bytes()].concat();
    let mut last = changes.queued;
    while let Some((id, step, _held)) = changes.next_note(STEP, &mut after, given.budget)? {
        if u64::from_be_bytes(after[1..9].try_into().unwrap()) > last {
            (walk.depth, last) = (walk.depth + 1, changes.queued);
        }
        let table = given.catalog.by_id(id);
        let table = table.expect("a step names a table of the statement's catalog");
        let (via, mut rest) = step.as_bytes().split_at(5);
        let place = u32::from_be_bytes(via[1..].try_into().unwrap()) as usize;
        let (mut held, mut values) = (given.budget.reserve(0, "query")?, Vec::new());
        while !rest.is_empty() {
            values.push(read_value(&mut rest, &mut held)?.expect("a step's value"));
        }
        if via[0] == 0 {
            let reference = &table.rules.references[place];
            if let Some(rule) = &reference.on_state {
                let columns = &reference.columns;
                walk.shift(table, columns, &values, rule, changes, partitions)?;
            }
        } else {
            let edge = &table.rules.propagate_edges[place];
            walk.along(table, edge, &values[0], changes, partitions)?;
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
        // Each step's filter is kept for that step alone, so a long walk holds one at a time.
        let kept = Kept::new(self.given.budget.passing("expression evaluation")?);
        let given = &Given {
            kept: &kept,
            ..*self.given
        };
        let key = [table.column("id")?];
        for edges in given.catalog.edges() {
            let ends = [edges.column("source_id")?, edges.column("target_id")?];
            let kind = edges.column("edge_type")?;
            let rules = Rules::of(edges, given)?;
            for (near, far) in [(0, 1), (1, 0)] {
                if edge.direction == ["INCOMING", "OUTGOING"][near] {
                    continue;
                }
                let values = [id.clone(), Value::Text(edge.edge_type.clone())];
                let (filter, bound) = equal(edges, &[ends[near], kind], &values);
                let needed = vec![false; edges.meta.columns.len()];
                let asked = rules.asked(Some(&filter), &bound, &[], &needed, given.reader);
                rules.paged(&asked, changes, self.part, |changes, _, cells| {
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
    /// under `ABORT ON FAILURE`, refuses the statement; so is every row of a table with no
    /// state machine.
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
        let state = machine.map(|m| table.column(&m.column)).transpose()?;
        if state.is_none() && !rule.abort_on_failure {
            return Ok(());
        }
        let kept = Kept::new(self.given.budget.passing("expression evaluation")?);
        let given = &Given {
            kept: &kept,
            ..*self.given
        };
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
        let mut cover = given.budget.passing("propagated row")?;
        let to = Value::Text(rule.set.clone());
        rules.paged(&asked, changes, self.part, |changes, key, cells| {
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
            let bytes = old.iter().map(heap_bytes).sum::<u64>();
            cover.cover(bytes + size_of_val(old.as_slice()) as u64 + rule.set.len() as u64)?;
            let mut row = old.clone();
            row[at] = to.clone();
            rules.moved(&old, &row, &left, changes)?;
            let was = Some((&key, &*old));
            rules.put(changes, partitions, &mut visible, (was, None), row, left)?;
            Ok(())
        })?;
        Ok(())
    }
}
