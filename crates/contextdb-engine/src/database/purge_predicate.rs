//! The declared node-local selection travels on the ordinary purge plane.
use super::*;
use contextdb_parser::ast::{BinOp, ColumnRef, Expr, Literal, UnaryOp};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct NodeLocalPredicate {
    id: uuid::Uuid,
    predicate: Option<Predicate>,
    parameters: HashMap<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
enum Predicate {
    Column(Option<String>, String),
    Parameter(String),
    Literal(Scalar),
    Binary(u8, Box<Self>, Box<Self>),
    Unary(bool, Box<Self>),
    Function(String, Vec<Self>),
    InList(Box<Self>, Vec<Self>, bool),
    Like(Box<Self>, Box<Self>, bool),
    IsNull(Box<Self>, bool),
    CosineDistance(Box<Self>, Box<Self>),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
enum Scalar {
    Null,
    Bool(bool),
    Integer(i64),
    Real(f64),
    Text(String),
    Vector(Vec<f32>),
}

impl Predicate {
    fn capture(
        expr: &Expr,
        parameters: &HashMap<String, Value>,
        used: &mut HashMap<String, Value>,
    ) -> Result<Self> {
        let mut capture = |e: &Expr| Self::capture(e, parameters, used);
        Ok(match expr {
            Expr::Column(c) => Self::Column(c.table.clone(), c.column.clone()),
            Expr::Parameter(name) => {
                let value = parameters.get(name).ok_or_else(|| {
                    Error::PlanError("node-local purge parameter is missing".into())
                })?;
                used.insert(name.clone(), value.clone());
                Self::Parameter(name.clone())
            }
            Expr::Literal(value) => Self::Literal(match value {
                Literal::Null => Scalar::Null,
                Literal::Bool(v) => Scalar::Bool(*v),
                Literal::Integer(v) => Scalar::Integer(*v),
                Literal::Real(v) => Scalar::Real(*v),
                Literal::Text(v) => Scalar::Text(v.clone()),
                Literal::Vector(v) => Scalar::Vector(v.clone()),
            }),
            Expr::BinaryOp { left, op, right } => Self::Binary(
                match op {
                    BinOp::Eq => 0,
                    BinOp::Neq => 1,
                    BinOp::Lt => 2,
                    BinOp::Lte => 3,
                    BinOp::Gt => 4,
                    BinOp::Gte => 5,
                    BinOp::And => 6,
                    BinOp::Or => 7,
                },
                Box::new(capture(left)?),
                Box::new(capture(right)?),
            ),
            Expr::UnaryOp { op, operand } => {
                Self::Unary(*op == UnaryOp::Not, Box::new(capture(operand)?))
            }
            Expr::FunctionCall { name, args } => Self::Function(
                name.clone(),
                args.iter().map(capture).collect::<Result<_>>()?,
            ),
            Expr::InList {
                expr,
                list,
                negated,
            } => Self::InList(
                Box::new(capture(expr)?),
                list.iter().map(capture).collect::<Result<_>>()?,
                *negated,
            ),
            Expr::Like {
                expr,
                pattern,
                negated,
            } => Self::Like(
                Box::new(capture(expr)?),
                Box::new(capture(pattern)?),
                *negated,
            ),
            Expr::IsNull { expr, negated } => Self::IsNull(Box::new(capture(expr)?), *negated),
            Expr::CosineDistance { left, right } => {
                Self::CosineDistance(Box::new(capture(left)?), Box::new(capture(right)?))
            }
            // Neither subqueries nor another table's vector lookup is self-contained.
            Expr::InSubquery { .. } | Expr::RowVectorSource { .. } => {
                return Err(Error::SubqueryNotSupported);
            }
        })
    }

    fn expression(&self) -> Result<Expr> {
        Ok(match self {
            Self::Column(table, column) => Expr::Column(ColumnRef {
                table: table.clone(),
                column: column.clone(),
            }),
            Self::Parameter(name) => Expr::Parameter(name.clone()),
            Self::Literal(value) => Expr::Literal(match value {
                Scalar::Null => Literal::Null,
                Scalar::Bool(v) => Literal::Bool(*v),
                Scalar::Integer(v) => Literal::Integer(*v),
                Scalar::Real(v) => Literal::Real(*v),
                Scalar::Text(v) => Literal::Text(v.clone()),
                Scalar::Vector(v) => Literal::Vector(v.clone()),
            }),
            Self::Binary(op, left, right) => Expr::BinaryOp {
                op: *[
                    BinOp::Eq,
                    BinOp::Neq,
                    BinOp::Lt,
                    BinOp::Lte,
                    BinOp::Gt,
                    BinOp::Gte,
                    BinOp::And,
                    BinOp::Or,
                ]
                .get(*op as usize)
                .ok_or_else(crate::custody::canonical::invalid)?,
                left: Box::new(left.expression()?),
                right: Box::new(right.expression()?),
            },
            Self::Unary(not, operand) => Expr::UnaryOp {
                op: if *not { UnaryOp::Not } else { UnaryOp::Neg },
                operand: Box::new(operand.expression()?),
            },
            Self::Function(name, args) => Expr::FunctionCall {
                name: name.clone(),
                args: args.iter().map(Self::expression).collect::<Result<_>>()?,
            },
            Self::InList(expr, list, negated) => Expr::InList {
                expr: Box::new(expr.expression()?),
                list: list.iter().map(Self::expression).collect::<Result<_>>()?,
                negated: *negated,
            },
            Self::Like(expr, pattern, negated) => Expr::Like {
                expr: Box::new(expr.expression()?),
                pattern: Box::new(pattern.expression()?),
                negated: *negated,
            },
            Self::IsNull(expr, negated) => Expr::IsNull {
                expr: Box::new(expr.expression()?),
                negated: *negated,
            },
            Self::CosineDistance(left, right) => Expr::CosineDistance {
                left: Box::new(left.expression()?),
                right: Box::new(right.expression()?),
            },
        })
    }
}

impl NodeLocalPredicate {
    pub(crate) fn capture(
        expr: Option<&Expr>,
        parameters: &HashMap<String, Value>,
    ) -> Result<Vec<u8>> {
        let mut used = HashMap::new();
        let predicate = expr
            .map(|e| Predicate::capture(e, parameters, &mut used))
            .transpose()?;
        // This identity is retained with the instruction, never re-minted by a receiver.
        RedbPersistence::encode_config_value(&Self {
            id: uuid::Uuid::new_v4(),
            predicate,
            parameters: used,
        })
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        RedbPersistence::decode_config_value(bytes)
    }

    pub(crate) fn receipt_key(&self) -> String {
        format!("authoritative_purge_local_applied.v1.{}", self.id)
    }

    pub(super) fn select(
        &self,
        db: &Database,
        table: &str,
    ) -> Result<Vec<AuthoritativePurgeSelection>> {
        let Some(meta) = db.table_meta(table) else {
            return Ok(Vec::new());
        };
        if crate::executor::effective_sync_direction(&meta) != SyncDirection::None {
            return Err(Error::SyncError(
                "node-local purge instruction requires SYNC OFF".into(),
            ));
        }
        let predicate = self
            .predicate
            .as_ref()
            .map(Predicate::expression)
            .transpose()?;
        let rows = crate::executor::filter_rows_by_predicate(
            db.scan(table, db.snapshot_for_read())?,
            predicate.as_ref(),
            &self.parameters,
        )?;
        rows.into_iter()
            .map(|row| {
                let key = natural_key_from_row_values(&meta, &row.values)
                    .ok_or_else(|| Error::NotSyncEligible(table.into()))?;
                db.resolve_authoritative_purge_selection(table, &key)
            })
            .collect()
    }
}

#[derive(Default)]
pub(super) struct Instructions<'a> {
    pub(super) outgoing: &'a [(String, Vec<u8>)],
    pub(super) applied: &'a [String],
}
