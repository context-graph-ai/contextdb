#[derive(Debug, Clone)]
pub enum Statement {
    CreateTable(CreateTable),
    DropTable(DropTable),
    CreateIndex(CreateIndex),
    Insert(Insert),
    Delete(Delete),
    Update(Update),
    Select(SelectStatement),
    Begin,
    Commit,
    Rollback,
}

#[derive(Debug, Clone)]
pub struct SelectStatement {
    pub ctes: Vec<Cte>,
    pub body: SelectBody,
}

#[derive(Debug, Clone)]
pub enum Cte {
    SqlCte {
        name: String,
        query: SelectBody,
    },
    MatchCte {
        name: String,
        match_clause: MatchClause,
    },
}

#[derive(Debug, Clone)]
pub struct MatchClause {
    pub pattern: GraphPattern,
    pub where_clause: Option<Expr>,
    pub return_cols: Vec<ReturnCol>,
}

#[derive(Debug, Clone)]
pub struct ReturnCol {
    pub expr: Expr,
    pub alias: Option<String>,
}

#[derive(Debug, Clone)]
pub struct GraphPattern {
    pub start: NodePattern,
    pub edges: Vec<EdgeStep>,
}

#[derive(Debug, Clone)]
pub struct NodePattern {
    pub alias: String,
    pub label: Option<String>,
    pub properties: Vec<(String, Expr)>,
}

#[derive(Debug, Clone)]
pub struct EdgeStep {
    pub direction: EdgeDirection,
    pub edge_type: Option<String>,
    pub min_hops: u32,
    pub max_hops: u32,
    pub alias: Option<String>,
    pub target: NodePattern,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EdgeDirection {
    Outgoing,
    Incoming,
    Both,
}

#[derive(Debug, Clone)]
pub struct SelectBody {
    pub columns: Vec<SelectColumn>,
    pub from: Option<FromClause>,
    pub joins: Vec<JoinClause>,
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
pub struct FromClause {
    pub table: String,
    pub alias: Option<String>,
}

#[derive(Debug, Clone)]
pub struct JoinClause {
    pub join_type: JoinType,
    pub table: String,
    pub alias: Option<String>,
    pub on: Expr,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
}

#[derive(Debug, Clone)]
pub struct OrderByItem {
    pub expr: Expr,
    pub direction: SortDirection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDirection {
    Asc,
    Desc,
    CosineDistance,
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
    FunctionCall {
        name: String,
        args: Vec<Expr>,
    },
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },
    InSubquery {
        expr: Box<Expr>,
        subquery: Box<SelectBody>,
        negated: bool,
    },
    IsNull {
        expr: Box<Expr>,
        negated: bool,
    },
    CosineDistance {
        left: Box<Expr>,
        right: Box<Expr>,
    },
}

#[derive(Debug, Clone)]
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
    pub columns: Vec<String>,
    pub values: Vec<Vec<Expr>>,
    pub on_conflict: Option<OnConflict>,
}

#[derive(Debug, Clone)]
pub struct OnConflict {
    pub columns: Vec<String>,
    pub update_columns: Vec<(String, Expr)>,
}

#[derive(Debug, Clone)]
pub struct CreateTable {
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub if_not_exists: bool,
    pub immutable: bool,
    pub state_machine: Option<StateMachineDef>,
}

#[derive(Debug, Clone)]
pub struct StateMachineDef {
    pub column: String,
    pub transitions: Vec<(String, Vec<String>)>,
}

#[derive(Debug, Clone)]
pub struct DropTable {
    pub name: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone)]
pub struct CreateIndex {
    pub name: String,
    pub table: String,
    pub columns: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub primary_key: bool,
    pub unique: bool,
    pub default: Option<Expr>,
    pub references: Option<ForeignKey>,
}

#[derive(Debug, Clone)]
pub struct ForeignKey {
    pub table: String,
    pub column: String,
}

#[derive(Debug, Clone)]
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
