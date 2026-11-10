//! A statement as it was written: what `parse` returns and `plan` reads.

pub use crate::types::SortDirection;

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
    pub order_by: Vec<OrderByItem>,
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
