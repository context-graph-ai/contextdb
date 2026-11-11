//! A statement as it was written: what `parse` returns and `plan` reads.

pub use crate::types::{SortDirection, VectorSearchMode};

#[derive(Debug, Clone)]
pub enum Statement {
    CreateTable(CreateTable),
    CreateIndex(CreateIndex),
    Insert(Insert),
    Delete(Delete),
    Update(Update),
    Select(Select),
    SetMemoryLimit(SetMemoryLimitValue),
    ShowMemoryLimit,
}

#[derive(Debug, Clone)]
pub enum SetMemoryLimitValue {
    Bytes(usize),
    None,
}

#[derive(Debug, Clone)]
pub struct Select {
    /// `SELECT *` is one column named `*`.
    pub columns: Vec<SelectColumn>,
    pub table: String,
    pub where_clause: Option<Expr>,
    /// The ORDER BY items. A similarity search has one, a distance ([`Expr::Distance`]) or the
    /// name the select list gives one.
    pub order_by: Vec<OrderByItem>,
    /// `USE VECTOR AUTO | EXACT | INDEXED`, when it was written.
    pub use_vector: Option<VectorSearchMode>,
    pub limit: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct SelectColumn {
    pub expr: Expr,
    pub alias: Option<String>,
}

#[derive(Debug, Clone)]
pub struct OrderByItem {
    pub expr: Expr,
    pub direction: SortDirection,
}

#[derive(Debug, Clone)]
pub enum Expr {
    Column(ColumnRef),
    Literal(Literal),
    Parameter(String),
    BinaryOp {
        left: Box<Expr>,
        op: BinOp,
        right: Box<Expr>,
    },
    UnaryOp {
        op: UnaryOp,
        operand: Box<Expr>,
    },
    /// A call by name. `a + b` arrives as the call `__add(a, b)`, and likewise `__sub`, `__mul`
    /// and `__div`. The `*` of `COUNT(*)` is a column named `*`.
    FunctionCall {
        name: String,
        args: Vec<Expr>,
    },
    IsNull {
        expr: Box<Expr>,
        negated: bool,
    },
    /// `left <=> right`: the cosine distance between a VECTOR column and a vector, a REAL that
    /// is lower for a closer row. It binds tighter than a comparison and looser than `+` and
    /// `-`, as in Postgres. This version works it out for the rows of a similarity search: the
    /// planner reads it as the one ORDER BY key of a SELECT with LIMIT and, written the same
    /// way, inside that statement's select list; anywhere else it is refused by name.
    Distance {
        left: Box<Expr>,
        right: Box<Expr>,
    },
    /// A row value, `(a, b)`: two or more expressions in parentheses. It is read wherever an
    /// expression is, and has a meaning only on either side of `IN`: the planner refuses it
    /// anywhere else.
    Row(Vec<Expr>),
    /// `expr [NOT] IN (item, ...)`. Beside a row value every item is a row value of the same
    /// length: the planner refuses any other list.
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnRef {
    pub table: Option<String>,
    pub column: String,
}

#[derive(Debug, Clone)]
pub enum Literal {
    Null,
    Bool(bool),
    Integer(i64),
    Real(f64),
    Text(String),
    Vector(Vec<f32>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinOp {
    Eq,
    Neq,
    Lt,
    Lte,
    Gt,
    Gte,
    And,
    Or,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Not,
    Neg,
}

#[derive(Debug, Clone)]
pub struct Insert {
    pub table: String,
    /// Empty when the statement names no columns.
    pub columns: Vec<String>,
    pub values: Vec<Vec<Expr>>,
}

#[derive(Debug, Clone)]
pub struct Delete {
    pub table: String,
    pub where_clause: Option<Expr>,
}

#[derive(Debug, Clone)]
pub struct Update {
    pub table: String,
    pub assignments: Vec<(String, Expr)>,
    pub where_clause: Option<Expr>,
}

#[derive(Debug, Clone)]
pub struct CreateTable {
    pub name: String,
    pub columns: Vec<ColumnDef>,
    /// The columns of each table-level `UNIQUE (...)`, in the order written.
    pub unique_constraints: Vec<Vec<String>>,
    pub if_not_exists: bool,
}

#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub primary_key: bool,
    pub unique: bool,
    pub default: Option<Expr>,
    /// The clauses after `VECTOR(n)`. Left as it starts on a column of any other type.
    pub vector: VectorClauses,
}

/// The clauses a VECTOR column may carry, as they were written.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VectorClauses {
    /// The columns of `PARTITION_KEY (...)`, in the order written; empty when it was not.
    pub partition_key: Vec<String>,
    /// The number after `MAX_PARTITIONS`, kept as written: `schema` judges it, so that zero, a
    /// negative number and one too large are refused by their own kind and not as a syntax
    /// error.
    pub max_partitions: Option<String>,
    /// `SEARCH_MODE AUTO | EXACT | INDEXED`, when it was written.
    pub search_mode: Option<VectorSearchMode>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataType {
    Uuid,
    Text,
    Integer,
    Real,
    Boolean,
    Timestamp,
    Json,
    Vector(u32),
}

#[derive(Debug, Clone)]
pub struct CreateIndex {
    pub name: String,
    pub table: String,
    pub columns: Vec<(String, SortDirection)>,
}
