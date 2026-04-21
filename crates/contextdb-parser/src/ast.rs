#[derive(Debug, Clone)]
pub enum Statement {
    CreateTable(CreateTable),
    AlterTable(AlterTable),
    DropTable(DropTable),
    CreateIndex(CreateIndex),
    Insert(Insert),
    Delete(Delete),
    Update(Update),
    Select(SelectStatement),
    Begin,
    Commit,
    Rollback,
    SetMemoryLimit(SetMemoryLimitValue),
    ShowMemoryLimit,
    SetDiskLimit(SetDiskLimitValue),
    ShowDiskLimit,
    SetSyncConflictPolicy(String),
    ShowSyncConflictPolicy,
}

#[derive(Debug, Clone)]
pub enum SetMemoryLimitValue {
    Bytes(usize),
    None,
}

#[derive(Debug, Clone)]
pub enum SetDiskLimitValue {
    Bytes(u64),
    None,
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
    pub graph_name: Option<String>,
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
    pub distinct: bool,
    pub columns: Vec<SelectColumn>,
    pub from: Vec<FromItem>,
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
pub enum FromItem {
    Table {
        name: String,
        alias: Option<String>,
    },
    GraphTable {
        graph_name: String,
        match_clause: MatchClause,
        columns: Vec<GraphTableColumn>,
    },
}

#[derive(Debug, Clone)]
pub struct GraphTableColumn {
    pub expr: Expr,
    pub alias: String,
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
    Like {
        expr: Box<Expr>,
        pattern: Box<Expr>,
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
    pub unique_constraints: Vec<Vec<String>>,
    pub if_not_exists: bool,
    pub immutable: bool,
    pub state_machine: Option<StateMachineDef>,
    pub dag_edge_types: Vec<String>,
    pub propagation_rules: Vec<AstPropagationRule>,
    pub retain: Option<RetainOption>,
}

#[derive(Debug, Clone)]
pub struct RetainOption {
    pub duration_seconds: u64,
    pub sync_safe: bool,
}

#[derive(Debug, Clone)]
pub struct AlterTable {
    pub table: String,
    pub action: AlterAction,
}

#[derive(Debug, Clone)]
pub enum AlterAction {
    AddColumn(ColumnDef),
    DropColumn(String),
    RenameColumn {
        from: String,
        to: String,
    },
    SetRetain {
        duration_seconds: u64,
        sync_safe: bool,
    },
    DropRetain,
    SetSyncConflictPolicy(String),
    DropSyncConflictPolicy,
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
    pub expires: bool,
}

#[derive(Debug, Clone)]
pub struct ForeignKey {
    pub table: String,
    pub column: String,
    pub propagation_rules: Vec<AstPropagationRule>,
}

#[derive(Debug, Clone)]
pub enum AstPropagationRule {
    FkState {
        trigger_state: String,
        target_state: String,
        max_depth: Option<u32>,
        abort_on_failure: bool,
    },
    EdgeState {
        edge_type: String,
        direction: String,
        trigger_state: String,
        target_state: String,
        max_depth: Option<u32>,
        abort_on_failure: bool,
    },
    VectorExclusion {
        trigger_state: String,
    },
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
    TxId,
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
