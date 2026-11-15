//! Binding: every name of an expression resolved and every type known before a row is read, so
//! that whether a statement is refused never depends on the rows.

use std::collections::HashMap;

use super::expression::{Scalar, Scope, refused, scalar, unbound};
use crate::sql::ast::{BinOp, Literal, UnaryOp};
use crate::sql::{Expr, not_supported};
use crate::types::{ColumnType, Error, Result, Value};

/// Refuses what no row could answer: a column no table in reach has, a `$name` nothing is bound
/// to, a function this version does not work out, and a comparison between two types that
/// neither compare nor convert.
pub(crate) fn bind(expr: &Expr, scope: &Scope, params: &HashMap<String, Value>) -> Result<()> {
    kind(expr, scope, params).map(|_| ())
}

/// The type of an expression's value, or `None` where it is not known before a row is read:
/// NULL, a row value, and arithmetic over either.
pub(super) fn kind(
    expr: &Expr,
    scope: &Scope,
    params: &HashMap<String, Value>,
) -> Result<Option<ColumnType>> {
    let kind = |expr: &Expr| kind(expr, scope, params);
    // The types of one side of IN: those of a row value, or the one type of anything else.
    let side = |expr: &Expr| expr.sides().iter().map(kind).collect::<Result<Vec<_>>>();
    Ok(match expr {
        Expr::Column(column) => Some(scope.one(column)?.1),
        Expr::Literal(literal) => match literal {
            Literal::Null => None,
            Literal::Bool(_) => Some(ColumnType::Boolean),
            Literal::Integer(_) => Some(ColumnType::Integer),
            Literal::Real(_) => Some(ColumnType::Real),
            Literal::Text(_) => Some(ColumnType::Text),
            Literal::Vector(vector) => Some(ColumnType::Vector(vector.len())),
        },
        Expr::Parameter(name) if scope.open.contains(name) => None,
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
            let (left, right) = (side(left)?, side(right)?);
            let equality = matches!(op, BinOp::Eq | BinOp::Neq);
            if left.len() != right.len() {
                return Err(Error::PlanError(
                    "a row value needs as many values as the one it is compared with".to_owned(),
                ));
            }
            if !equality && left.len() > 1 {
                return Err(not_supported("< and > between row values"));
            }
            if !matches!(op, BinOp::And | BinOp::Or) {
                for (left, right) in left.into_iter().zip(right) {
                    comparable(left, right)?;
                    if !equality && [left, right].contains(&Some(ColumnType::Json)) {
                        return Err(not_supported("< and > between JSON values"));
                    }
                }
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
            comparable(kind(left)?, Some(ColumnType::Vector(0)))?;
            comparable(kind(right)?, Some(ColumnType::Vector(0)))?;
            Some(ColumnType::Real)
        }
        Expr::Like { expr, pattern, .. } => {
            comparable(kind(expr)?, Some(ColumnType::Text))?;
            comparable(kind(pattern)?, Some(ColumnType::Text))?;
            Some(ColumnType::Boolean)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            let middle = kind(expr)?;
            comparable(middle, kind(low)?)?;
            comparable(middle, kind(high)?)?;
            Some(ColumnType::Boolean)
        }
        Expr::Field { json, key, as_text } => {
            let of_json = matches!(kind(json)?, None | Some(ColumnType::Json));
            let key = kind(key)?;
            if !of_json || !matches!(key, None | Some(ColumnType::Text | ColumnType::Integer)) {
                return Err(Error::PlanError(
                    "-> and ->> take a JSON value, then a field's name or a place in an array"
                        .to_owned(),
                ));
            }
            Some(if *as_text {
                ColumnType::Text
            } else {
                ColumnType::Json
            })
        }
        // What a subquery gives is known once its own statement is bound.
        Expr::Subquery(query) => query.gives.0.get().copied(),
        Expr::InSubquery { expr, query, .. } => {
            comparable(kind(expr)?, query.gives.0.get().copied())?;
            Some(ColumnType::Boolean)
        }
        Expr::RowNumber {
            partition_by,
            order_by,
        } => {
            for expr in partition_by
                .iter()
                .chain(order_by.iter().map(|key| &key.expr))
            {
                kind(expr)?;
            }
            Some(ColumnType::Integer)
        }
    })
}

/// Whether values of the two types compare: the same type, or one that converts to the other
/// as a write converts it (quoted text to a UUID, TIMESTAMP, JSON or VECTOR, an INTEGER to a
/// REAL). A type that is not known compares with any.
pub(super) fn comparable(left: Option<ColumnType>, right: Option<ColumnType>) -> Result<()> {
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
