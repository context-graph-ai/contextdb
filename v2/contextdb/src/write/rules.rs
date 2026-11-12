//! The rules a statement is held to as it writes a row: IMMUTABLE on a table and on a column,
//! the transitions of a STATE MACHINE, and the edge types of DAG, whose edges never close a
//! cycle. Each is checked here, once, for every way a row is written.

use std::collections::BTreeSet;

use super::{Rules, column, equal};
use crate::query::{self, Reader};
use crate::sql::{Expr, Plan};
use crate::store::{Cell, Key, Read};
use crate::types::{Error, Result, Uuid, Value};

/// What a node reached by a walk along edges is counted as: its id among the nodes reached and
/// in the list of those still to leave from.
const NODE_BYTES: u64 = 64;

/// An edge of a type its table lists under DAG: its source, its target and its type.
pub(super) type Edge = (Uuid, Uuid, String);

impl Rules<'_> {
    /// Refuses an UPDATE, a DELETE and an `ON CONFLICT DO UPDATE` on a table declared
    /// IMMUTABLE, and an assignment to a column declared IMMUTABLE. No row is read: the table
    /// says so, whatever it holds.
    pub(super) fn changeable(&self, plan: &Plan) -> Result<()> {
        let table = self.table;
        let assignments: &[(String, Expr)] = match plan {
            Plan::Update(plan) => &plan.assignments,
            Plan::Insert(plan) => match &plan.on_conflict {
                Some(conflict) => &conflict.assignments,
                None => return Ok(()),
            },
            _ => &[],
        };
        if table.meta.immutable {
            return Err(Error::ImmutableTable {
                table: table.name.clone(),
            });
        }
        for (name, _) in assignments {
            if table
                .rules
                .immutable_columns
                .contains(&column(table, name)?)
            {
                return Err(Error::ImmutableColumn {
                    table: table.name.clone(),
                    column: name.clone(),
                });
            }
        }
        Ok(())
    }

    /// Where the column of the table's STATE MACHINE is in a row, when the statement assigns
    /// it. The column is then marked as read: a move is judged from the state the row is in,
    /// however long that state is.
    pub(super) fn state(&self, assigned: &[usize], needed: &mut [bool]) -> Result<Option<usize>> {
        let Some(machine) = &self.table.meta.state_machine else {
            return Ok(None);
        };
        let at = column(self.table, &machine.column)?;
        let moves = assigned.contains(&at);
        needed[at] |= moves;
        Ok(moves.then_some(at))
    }

    /// Refuses a row whose state, at `state`, is set to one the declaration does not list after
    /// the state the row is in. A state does not follow itself unless it is listed so. A row
    /// that has no state, or is given none, makes no move.
    pub(super) fn transition(
        &self,
        state: Option<usize>,
        old: &[Value],
        new: &[Value],
    ) -> Result<()> {
        let (Some(at), Some(machine)) = (state, &self.table.meta.state_machine) else {
            return Ok(());
        };
        let (Value::Text(from), Value::Text(to)) = (&old[at], &new[at]) else {
            return Ok(());
        };
        let mut listed = machine.transitions.iter();
        if listed.any(|(state, next)| state == from && next.contains(to)) {
            return Ok(());
        }
        Err(Error::InvalidStateTransition {
            table: self.table.name.clone(),
            column: machine.column.clone(),
            from: from.clone(),
            to: to.clone(),
        })
    }

    /// The edge a row is, as its source, its target and its type, when its table lists that
    /// type under DAG. `was` is the row before an UPDATE: an edge the UPDATE leaves as it was
    /// is none to look at.
    pub(super) fn edge(
        &self,
        was: Option<(&Key, &[Value])>,
        row: &[Value],
    ) -> Result<Option<Edge>> {
        if self.table.meta.dag.is_empty() {
            return Ok(None);
        }
        let at = self.edge_columns()?;
        let (Value::Uuid(from), Value::Uuid(to), Value::Text(edge_type)) =
            (&row[at[0]], &row[at[1]], &row[at[2]])
        else {
            return Ok(None);
        };
        let same = was.is_some_and(|(_, old)| at.iter().all(|&at| old[at] == row[at]));
        let listed = self.table.meta.dag.contains(edge_type);
        Ok((listed && !same).then(|| (*from, *to, edge_type.clone())))
    }

    /// Where an edge's source, target and type are in a row.
    fn edge_columns(&self) -> Result<[usize; 3]> {
        let [source, target, kind] =
            ["source_id", "target_id", "edge_type"].map(|name| column(self.table, name));
        Ok([source?, target?, kind?])
    }

    /// Refuses an edge when the edges of its type that `read` shows lead from its target back
    /// to its source: it closes a cycle. Each type a table lists under DAG is held to this by
    /// itself.
    ///
    /// It is asked once the edge's row is staged, so `read` shows the row as its statement
    /// leaves it and nothing of the row it was: an UPDATE that takes a row out and puts it back
    /// is judged where it puts it back.
    ///
    /// Costs the edges of that type that can be reached from the target, read through the
    /// table source by the index that leads with an edge's source. The nodes reached are
    /// counted against the memory limit as they are met.
    pub(super) fn acyclic(&self, read: &dyn Read, edge: Option<Edge>) -> Result<()> {
        let Some((from, to, edge_type)) = edge else {
            return Ok(());
        };
        let (table, given) = (self.table, self.given);
        let at = self.edge_columns()?;
        // The edges of this type that leave a node: the node is bound anew for each one.
        let probe = [Value::Null, Value::Text(edge_type.clone())];
        let (filter, mut bound) = equal(table, &[at[0], at[2]], &probe);
        let source = table.meta.columns[at[0]].name.clone();
        let mut needed = vec![false; table.meta.columns.len()];
        needed[at[1]] = true;
        let reader = Reader::admin();
        let mut reserved = given.budget.reserve(0, "query")?;
        let (mut reached, mut leave) = (BTreeSet::new(), vec![to]);
        while let Some(node) = leave.pop() {
            if node == from {
                return Err(Error::CycleDetected {
                    table: table.name.clone(),
                    edge_type,
                });
            }
            bound.insert(source.clone(), Value::Uuid(node));
            let asked = self.asked(Some(&filter), &bound, &[], &needed, &reader);
            let mut after = None;
            while let Some((edges, _held)) =
                query::rows_for_write(&asked, after.as_ref(), given.catalog, read, given.budget)?
            {
                after = edges.last().map(|(key, _)| key.clone());
                for (_, cells) in &edges {
                    if let Cell::Here(Value::Uuid(next)) = cells[at[1]]
                        && reached.insert(next)
                    {
                        reserved.grow(NODE_BYTES)?;
                        leave.push(next);
                    }
                }
            }
        }
        Ok(())
    }
}
