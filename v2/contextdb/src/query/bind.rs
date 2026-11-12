//! Binding: every name of an expression resolved and every type known before a row is read, so
//! that whether a statement is refused never depends on the rows.

use std::collections::HashMap;

use super::expression::{Scalar, Scope, refused, scalar, unbound};
use crate::sql::Expr;
use crate::sql::ast::{BinOp, Literal, UnaryOp};
use crate::types::{ColumnType, Error, Result, Value};

/// Refuses what no row could answer: a column no table in reach has, a `$name` nothing is bound
/// to, a function this version does not work out, and a comparison between two types that
/// neither compare nor convert.
pub(crate) fn bind(expr: &Expr, scope: &Scope, params: &HashMap<String, Value>) -> Result<()> {
    kind(expr, scope, params).map(|_| ())
}

/// The type of an expression's value, or `None` where it is not known before a row is read:
/// NULL, a row value, and arithmetic over either.
fn kind(expr: &Expr, scope: &Scope, params: &HashMap<String, Value>) -> Result<Option<ColumnType>> {
    let kind = |expr: &Expr| kind(expr, scope, params);
    // The types of one side of IN: those of a row value, or the one type of anything else.
    let side = |expr: &Expr| -> Result<Vec<_>> {
        match expr {
            Expr::Row(items) => items.iter().map(kind).collect(),
            one => Ok(vec![kind(one)?]),
        }
    };
    Ok(match expr {
        Expr::Column(column) => Some(scope.find(column)?.1),
        Expr::Literal(literal) => match literal {
            Literal::Null => None,
            Literal::Bool(_) => Some(ColumnType::Boolean),
            Literal::Integer(_) => Some(ColumnType::Integer),
            Literal::Real(_) => Some(ColumnType::Real),
            Literal::Text(_) => Some(ColumnType::Text),
            Literal::Vector(vector) => Some(ColumnType::Vector(vector.len())),
        },
        Expr::Parameter(name) => match params.get(name).ok_or_else(|| unbound(name))? {
            Value::Null => None,
            Value::Bool(_) => Some(ColumnType::Boolean),
            Value::Int64(_) => Some(ColumnType::Integer),
            Value::Float64(_) => Some(ColumnType::Real),
            Value::Text(_) => Some(ColumnType::Text),
            Value::Uuid(_) => Some(ColumnType::Uuid),
            Value::Timestamp(_) => Some(ColumnType::Timestamp),
            Value::Json(_) => Some(ColumnType::Json),
            Value::Vector(vector) => Some(ColumnType::Vector(vector.len())),
            Value::TxId(_) => Some(ColumnType::TxId),
        },
        Expr::BinaryOp { left, op, right } => {
            let (left, right) = (kind(left)?, kind(right)?);
            if !matches!(op, BinOp::And | BinOp::Or) {
                comparable(left, right)?;
            }
            Some(ColumnType::Boolean)
        }
        Expr::UnaryOp { op, operand } => match (op, kind(operand)?) {
            (UnaryOp::Not, _) => Some(ColumnType::Boolean),
            (UnaryOp::Neg, operand) => operand,
        },
        Expr::IsNull { expr, .. } => {
            kind(expr)?;
            Some(ColumnType::Boolean)
        }
        Expr::FunctionCall { name, args } => {
            let scalar = scalar(name).ok_or_else(|| refused(name))?;
            let args = args.iter().map(kind).collect::<Result<Vec<_>>>()?;
            match scalar {
                Scalar::Arithmetic(_) if args.contains(&Some(ColumnType::Real)) => {
                    Some(ColumnType::Real)
                }
                Scalar::Arithmetic(_) => {
                    let whole = args.iter().all(|arg| *arg == Some(ColumnType::Integer));
                    whole.then_some(ColumnType::Integer)
                }
                Scalar::Now => Some(ColumnType::Timestamp),
                Scalar::Coalesce => args.into_iter().flatten().next(),
            }
        }
        Expr::InList { expr, list, .. } => {
            let left = side(expr)?;
            for item in list {
                for (left, item) in left.iter().zip(side(item)?) {
                    comparable(*left, item)?;
                }
            }
            Some(ColumnType::Boolean)
        }
        Expr::Row(items) => {
            for item in items {
                kind(item)?;
            }
            None
        }
        Expr::Distance { left, right } => {
            kind(left)?;
            kind(right)?;
            Some(ColumnType::Real)
        }
    })
}

/// Whether values of the two types compare: the same type, or one that converts to the other
/// as a write converts it (quoted text to a UUID, TIMESTAMP, JSON or VECTOR, an INTEGER to a
/// REAL). A type that is not known compares with any.
fn comparable(left: Option<ColumnType>, right: Option<ColumnType>) -> Result<()> {
    use ColumnType::{Integer, Json, Real, Text, Timestamp, Uuid, Vector};
    let (Some(left), Some(right)) = (left, right) else {
        return Ok(());
    };
    let converts = |from, to| {
        matches!(
            (from, to),
            (Integer, Real) | (Text, Uuid | Timestamp | Json | Vector(_))
        )
    };
    let same = std::mem::discriminant(&left) == std::mem::discriminant(&right);
    if same || converts(left, right) || converts(right, left) {
        return Ok(());
    }
    Err(Error::PlanError(format!(
        "cannot compare {left} with {right}"
    )))
}
