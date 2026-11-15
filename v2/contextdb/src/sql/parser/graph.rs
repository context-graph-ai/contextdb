//! Reads `GRAPH_TABLE(...)`: the pattern a FROM or a JOIN matches over an edge table.

use super::select::column;
use super::{Pair, Rule, child, first_ident, has, ident, idents, number, where_clause};
use crate::sql::ast::{EdgeDirection, EdgeStep, GraphTable, NodePattern};
use crate::sql::not_supported;
use crate::types::{Error, Result};

/// The most hops one step of a pattern may ask for.
const MAX_HOPS: u32 = 10;

pub(super) fn graph_table(pair: Pair<'_>) -> Result<GraphTable> {
    let mut graph = GraphTable {
        edge_table: first_ident(pair.clone()),
        filter: where_clause(pair.clone())?,
        ..GraphTable::default()
    };
    for part in pair.into_inner() {
        match part.as_rule() {
            Rule::node_pattern => graph.start = node(part)?,
            Rule::edge_step => graph.steps.push(step(part)?),
            Rule::graph_columns => {
                graph.columns = part.into_inner().map(column).collect::<Result<_>>()?;
            }
            // The edge table and the WHERE clause, read above.
            _ => {}
        }
    }
    if graph.columns.is_empty() {
        return Err(not_supported("a graph pattern with no COLUMNS"));
    }
    Ok(graph)
}

/// The label or edge type written after the colon of a node or an edge.
fn label(pair: Pair<'_>) -> Option<String> {
    child(pair, Rule::label).map(|label| ident(&label.into_inner().next().expect("a label")))
}

/// The name written in a node's parentheses. A label there would select nothing: refused.
fn node(pair: Pair<'_>) -> Result<NodePattern> {
    if label(pair.clone()).is_some() {
        return Err(not_supported("a label on a node of a pattern"));
    }
    let alias = idents(pair).pop();
    Ok(NodePattern { alias })
}

/// One edge and the node it leads to. Refuses a step with no upper bound, and one whose upper
/// bound is above `MAX_HOPS`.
fn step(pair: Pair<'_>) -> Result<EdgeStep> {
    let direction = if has(&pair, Rule::incoming) {
        EdgeDirection::Incoming
    } else if has(&pair, Rule::outgoing) {
        EdgeDirection::Outgoing
    } else {
        EdgeDirection::Both
    };
    let edge_type = label(child(pair.clone(), Rule::edge).expect("an edge"));
    let to = node(child(pair.clone(), Rule::node_pattern).expect("a node"))?;
    let (min, max) = match child(pair, Rule::hops) {
        None => (1, 1),
        Some(hops) => {
            let mut bounds = hops.into_inner();
            let (Some(min), Some(max)) = (bounds.next(), bounds.next()) else {
                return Err(Error::UnboundedTraversal);
            };
            (number(&min, "hop count")?, number(&max, "hop count")?)
        }
    };
    if max > MAX_HOPS {
        return Err(Error::BfsDepthExceeded {
            depth: max,
            max: MAX_HOPS,
        });
    }
    Ok(EdgeStep {
        direction,
        edge_type,
        min,
        max,
        to,
    })
}
