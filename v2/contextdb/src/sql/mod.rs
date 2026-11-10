//! sql text: grammar, parser, planner.
//!
//! It never touches storage: it turns text into a statement and a statement into a plan, and
//! knows nothing of what any table holds.
//!
//! The fourth thing the parts meet at is defined here: [`Plan`], the planned statement that
//! `schema`, `write` and `query` each take.

pub(crate) mod ast;
mod parser;
pub(crate) mod plan;
mod planner;

pub(crate) use ast::{Expr, Statement};
pub(crate) use plan::{Effect, PhysicalPlan as Plan};

use crate::types::Result;

/// Reads one SQL statement. Refuses with `ParseError`, or with `PlanError` when the text begins
/// a statement or a clause that a later version of the library reads. Costs time in proportion
/// to the text.
pub(crate) fn parse(text: &str) -> Result<Statement> {
    parser::parse(text)
}

/// Plans a parsed statement. Refuses with `PlanError`. Looks at no table and no data.
pub(crate) fn plan(statement: &Statement) -> Result<Plan> {
    planner::plan(statement)
}
