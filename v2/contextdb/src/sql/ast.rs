//! A statement as it was written: what `parse` returns and `plan` reads.

pub use crate::types::{
    ColumnType as DataType, ConflictPolicy, History, KeptCounts, Retain, RetainUnit, SortDirection,
    StateMachine, SyncDirection, VectorSearchMode,
};

#[derive(Debug, Clone)]
pub enum Statement {
    CreateTable(CreateTable),
    CreateIndex(CreateIndex),
    AlterTable(AlterTable),
    DropTable(DropTable),
    DropIndex(DropIndex),
    Insert(Insert),
    Delete(Delete),
    Update(Update),
    Select(Select),
    SetMemoryLimit(SetMemoryLimitValue),
    ShowMemoryLimit,
    ShowVectorIndexes,
    ShowVectorPartitions {
        /// The table and column after `FOR`.
        column: Option<(String, String)>,
        limit: Option<u64>,
        /// Zero when OFFSET was not written.
        offset: u64,
    },
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
    pub on_conflict: Option<OnConflict>,
}

/// `ON CONFLICT (columns) DO UPDATE SET column = value, ...`.
#[derive(Debug, Clone)]
pub struct OnConflict {
    pub columns: Vec<String>,
    pub assignments: Vec<(String, Expr)>,
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
    /// The columns of a table-level `PRIMARY KEY (...)`; empty when it was not written.
    pub primary_key: Vec<String>,
    /// Each `FOREIGN KEY (...) REFERENCES ...`: its own columns, then what they refer to.
    pub foreign_keys: Vec<(Vec<String>, Reference)>,
    pub if_not_exists: bool,
    pub options: TableOptions,
}

/// The options written after a table's columns. Each is empty, false or `None` when it was
/// not written.
#[derive(Debug, Clone, Default)]
pub struct TableOptions {
    pub immutable: bool,
    pub state_machine: Option<StateMachine>,
    /// The edge types of `DAG(...)`.
    pub dag: Vec<String>,
    pub propagate_edges: Vec<EdgePropagation>,
    /// The state of each `PROPAGATE ON STATE s EXCLUDE VECTOR`.
    pub exclude_vector_states: Vec<String>,
    pub retain: Option<Retain>,
    pub history: Option<History>,
    pub sync: Option<SyncDirection>,
    pub conflict: Option<ConflictPolicy>,
    /// The tables of `DELIVERY MANIFEST OVER ...`.
    pub delivery_manifest: Vec<String>,
    pub edge_discard: Option<EdgeDiscard>,
    pub kept_counts: Option<KeptCounts>,
}

/// `REFERENCES table (column, ...)`, with its `ON STATE` rule when one was written.
#[derive(Debug, Clone)]
pub struct Reference {
    pub table: String,
    pub columns: Vec<String>,
    pub on_state: Option<Propagation>,
}

/// `ON STATE on_state PROPAGATE SET set [MAX DEPTH n] [ABORT ON FAILURE]`, and the same four
/// things of a `PROPAGATE ON EDGE`. A state keeps the letters it was written in.
#[derive(Debug, Clone)]
pub struct Propagation {
    pub on_state: String,
    pub set: String,
    pub max_depth: Option<u32>,
    pub abort_on_failure: bool,
}

/// `PROPAGATE ON EDGE edge_type direction STATE ... SET ...`. The edge type keeps the letters
/// it was written in; the direction is INCOMING, OUTGOING or BOTH, in capitals.
#[derive(Debug, Clone)]
pub struct EdgePropagation {
    pub edge_type: String,
    pub direction: String,
    pub rule: Propagation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EdgeDiscard {
    Never,
    AfterOutcome,
    Always,
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
    pub references: Option<Reference>,
    pub immutable: bool,
    pub context_id: bool,
    pub scope_labels: Option<ScopeLabels>,
    /// The table and column of `ACL REFERENCES table (column)`.
    pub acl: Option<(String, String)>,
    pub rank_policy: Option<RankPolicy>,
}

/// `SCOPE_LABEL (...)`, whose one list is for reading and writing alike and leaves `write`
/// empty, or `SCOPE_LABEL_READ (...) WRITE (...)`.
#[derive(Debug, Clone)]
pub struct ScopeLabels {
    pub read: Vec<String>,
    pub write: Option<Vec<String>>,
}

/// `RANK_POLICY (JOIN table ON column, FORMULA '...', SORT_KEY key)`.
#[derive(Debug, Clone)]
pub struct RankPolicy {
    pub table: String,
    pub column: String,
    pub formula: String,
    pub sort_key: String,
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
    /// The number after `AUTO_INDEX_AT`, kept as written for `schema` to judge.
    pub auto_index_at: Option<String>,
    /// `CONSOLIDATION NONE` or `CONSOLIDATION (...)`, when it was written.
    pub consolidation: Option<ConsolidationClause>,
    /// The text of `WITH (quantization = '...')`, kept as written for `schema` to judge.
    pub quantization: Option<String>,
}

/// `CONSOLIDATION` as it was written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConsolidationClause {
    /// `CONSOLIDATION NONE`.
    None,
    /// `CONSOLIDATION (CHANGE_PERCENT = n, TOMBSTONE_PERCENT = n)`: each number as written,
    /// `None` for a member that was not.
    Percent {
        change: Option<String>,
        tombstone: Option<String>,
    },
}

#[derive(Debug, Clone)]
pub struct CreateIndex {
    pub name: String,
    pub table: String,
    pub columns: Vec<(String, SortDirection)>,
    /// The keys of an index over a path into a JSON column, `((column -> 'key' -> ...))`, whose
    /// one column is that column; empty for an index over columns.
    pub json_path: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct AlterTable {
    pub table: String,
    pub action: AlterAction,
}

/// The one thing an ALTER TABLE changes, as it was written.
#[derive(Debug, Clone)]
pub enum AlterAction {
    AddColumn(Box<ColumnDef>),
    DropColumn {
        column: String,
        /// `Some(true)` for CASCADE, `Some(false)` for RESTRICT, `None` when neither was
        /// written, which means RESTRICT.
        cascade: Option<bool>,
    },
    RenameColumn {
        from: String,
        to: String,
    },
    SetRetain(Retain),
    DropRetain,
    SetHistory(History),
    SetSync(SyncDirection),
    SetConflict(ConflictPolicy),
    /// `SET MAINTAIN COUNT ...`; `None` is the word DEFAULT.
    SetKeptCounts(Option<KeptCounts>),
    /// `ALTER COLUMN column SET MAX_PARTITIONS n`, the number kept as written.
    SetMaxPartitions {
        column: String,
        max_partitions: String,
    },
    /// `ALTER COLUMN column SET SEARCH_MODE mode`.
    SetSearchMode {
        column: String,
        search_mode: VectorSearchMode,
    },
    /// `ALTER COLUMN column SET AUTO_INDEX_AT n`, the number kept as written; `None` is DEFAULT.
    SetAutoIndexAt(String, Option<String>),
    /// `ALTER COLUMN column SET CONSOLIDATION ...`; `None` is DEFAULT.
    SetConsolidation(String, Option<ConsolidationClause>),
}

#[derive(Debug, Clone)]
pub struct DropTable {
    pub name: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone)]
pub struct DropIndex {
    pub name: String,
    pub table: String,
    pub if_exists: bool,
}
