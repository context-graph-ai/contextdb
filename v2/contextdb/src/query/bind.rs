//! Binding: every name of an expression resolved and every type known before a row is read, so
//! that whether a statement is refused never depends on the rows.

use std::collections::HashMap;

use super::expression::{Scalar, Scope, refused, scalar, unbound};
use crate::sql::ast::{BinOp, UnaryOp};
use crate::sql::{Expr, not_supported};
use crate::types::ColumnType::{Json, Timestamp, Uuid, Vector};
use crate::types::{ColumnDef, ColumnType, Error, Result, Value};

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
    let constant = |expr: &Expr| match expr {
        Expr::Literal(value) => Some(value.clone()),
        Expr::Parameter(name) => params.get(name).cloned(),
        _ => None,
    };
    // Quoted or bound text is read as the UUID, TIMESTAMP, VECTOR or JSON value it is compared
    // with, the column `name` if it is one; JSON, and a VECTOR of no known length, with a row.
    let read = |text: &Expr, with: Option<ColumnType>, name: &Expr| {
        let (
            Some(value @ Value::Text(_)),
            Some(column_type @ (Uuid | Timestamp | Vector(_) | Json)),
        ) = (constant(text), with)
        else {
            return kind(text);
        };
        if !matches!(column_type, Json | Vector(0)) {
            let (table, name) = match name {
                Expr::Column(column) => (column.table.as_deref(), column.column.clone()),
                _ => (None, "a computed value".to_owned()),
            };
            let (nullable, primary_key, unique, default) = (true, false, false, None);
            let column = ColumnDef {
                name,
                column_type,
                nullable,
                primary_key,
                unique,
                default,
            };
            crate::types::typed(table.unwrap_or_default(), &column, value)?;
        }
        Ok(None)
    };
    // Two sides that compare, which are not JSON values when they are `ordered`.
    let pair = |left: &Expr, right: &Expr, ordered: bool| {
        let (left, right) = (
            read(left, kind(right)?, right)?,
            read(right, kind(left)?, left)?,
        );
        comparable(left, right)?;
        match ordered && [left, right].contains(&Some(Json)) {
            true => Err(not_supported("< and > between JSON values")),
            false => Ok(()),
        }
    };
    Ok(match expr {
        Expr::Column(column) => Some(scope.one(column)?.1),
        Expr::Parameter(name) if scope.open.contains(name) => None,
        Expr::Parameter(name) if !params.contains_key(name) => return Err(unbound(name)),
        Expr::Literal(_) | Expr::Parameter(_) => match constant(expr).unwrap_or(Value::Null) {
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
            let (width, equality) = (side(left)?.len(), matches!(op, BinOp::Eq | BinOp::Neq));
            if width != side(right)?.len() {
                return Err(Error::PlanError(
                    "a row value needs as many values as the one it is compared with".to_owned(),
                ));
            }
            if !equality && width > 1 {
                return Err(not_supported("< and > between row values"));
            }
            if !matches!(op, BinOp::And | BinOp::Or) {
                for (left, right) in left.sides().iter().zip(right.sides()) {
                    pair(left, right, !equality)?;
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
                Scalar::Coalesce => (args.into_iter().flatten())
                    .reduce(|kind, next| super::union::meet(kind, next).unwrap_or(kind)),
            }
        }
        Expr::InList { expr, list, .. } => {
            side(expr)?;
            for item in list {
                side(item)?;
                for (left, item) in expr.sides().iter().zip(item.sides()) {
                    pair(left, item, false)?;
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
            for side in [left, right] {
                comparable(read(side, Some(Vector(0)), side)?, Some(Vector(0)))?;
            }
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
            pair(expr, low, true)?;
            pair(expr, high, true)?;
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
            let gives = query.gives.0.get().copied();
            comparable(read(expr, gives, expr)?, gives)?;
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

/// Whether values of the two types compare: the same type, or an INTEGER and a REAL. Only quoted
/// or bound text converts, which `read` takes first. A type that is not known compares with any.
pub(super) fn comparable(left: Option<ColumnType>, right: Option<ColumnType>) -> Result<()> {
    match (left, right) {
        (Some(left), Some(right)) if super::union::meet(left, right).is_none() => Err(
            Error::PlanError(format!("cannot compare {left} with {right}")),
        ),
        _ => Ok(()),
    }
}
