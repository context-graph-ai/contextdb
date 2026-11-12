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

use crate::types::{Error, Result};

/// Reads one SQL statement. Refuses with `ParseError`, or with `PlanError` when the text begins
/// a statement or a clause that a later version of the library reads. Costs time in proportion
/// to the text.
pub(crate) fn parse(text: &str) -> Result<Statement> {
    parser::parse(text)
}

/// Reads the formula of a rank policy: numbers and `{column}`s joined by `+` and `*`, in
/// parentheses or in `coalesce(<formula>, <number>)`, as the tree any expression is. Refuses
/// with `ParseError`.
pub(crate) fn formula(text: &str) -> Result<Expr> {
    parser::formula(text)
}

/// Plans a parsed statement. Refuses with `PlanError`. Looks at no table and no data.
pub(crate) fn plan(statement: &Statement) -> Result<Plan> {
    planner::plan(statement)
}

/// The refusal of a statement, a clause or a form that a later version of the library reads,
/// by its name.
pub(crate) fn not_supported(what: &str) -> Error {
    Error::PlanError(format!(
        "{what} is not supported in this version of the library"
    ))
}

/// The refusal of a form of the earlier engine that no version of the library reads, by its
/// name, with what to write in its place.
pub(crate) fn not_carried(what: &str, instead: &str) -> Error {
    Error::PlanError(format!("{what} is not part of this library: {instead}"))
}
