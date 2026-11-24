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

pub(crate) use ast::{Expr, Statement, Statement as Plan};
pub use parser::read_session_answers;
pub(crate) use parser::{MAX_EXPR_NESTING, depth_error};
pub(crate) use plan::Effect;
pub(crate) use planner::{AGGREGATES, inside, inside_mut};

use crate::memory::{Budget, Reservation};
use crate::types::{Error, Result};

/// Reads one SQL statement. Refuses with `ParseError`, or with `PlanError` when the text begins
/// a statement or a clause that a later version of the library reads. Costs time in proportion
/// to the text. Gives the statement's form too: its text with each constant as `?`.
pub(crate) fn parse(text: &str, budget: &Budget) -> Result<(Statement, Reservation, String)> {
    let held = charge(text, budget)?;
    let (statement, form) = parser::parse(text)?;
    Ok((statement, held, form))
}

/// Admission shared by parser entry and cache hits.
pub(crate) fn charge(text: &str, budget: &Budget) -> Result<Reservation> {
    budget.reserve(parser::charge(text), "SQL parsing")
}

/// Planning makes one copy of the parsed statement, the one the plan cache keeps or gives.
pub(crate) fn planning(text: &str) -> u64 {
    parser::charge(text)
}

/// Reads the formula of a rank policy: numbers and `{column}`s joined by `+` and `*`, in
/// parentheses or in `coalesce(<formula>, <number>)`, as the tree any expression is. Refuses
/// with `ParseError`.
pub(crate) fn formula(text: &str, budget: &Budget) -> Result<(Expr, Reservation)> {
    let held = charge(text, budget)?;
    Ok((parser::formula(text)?, held))
}

/// Plans a parsed statement, which is its own plan once it passes. Refuses with `PlanError`.
/// Looks at no table and no data.
pub(crate) fn plan(statement: Statement) -> Result<Plan> {
    planner::stands(&statement)?;
    Ok(statement)
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

/// The first 63 bytes of an identifier, ending on a whole UTF-8 character.
pub(crate) fn identifier(text: &str) -> &str {
    let mut end = text.len().min(63);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}
