//! A statement as it was written: what `parse` returns and `plan` reads.

use std::sync::OnceLock;

pub use crate::types::{
    ColumnType as DataType, ConflictPolicy, History, KeptCounts, Retain, RetainUnit, SortDirection,
    StateMachine, SyncDirection, VectorSearchMode,
};

#[derive(Debug, Clone)]
pub enum Statement {
    CreateTable(CreateTable),
    DeclareEvent(EventDeclaration),
    DropEvent {
        kind: crate::types::Kind,
        name: String,
        if_exists: bool,
        table: Option<String>,
    },
    CreateIndex(CreateIndex),
    AlterTable(AlterTable),
    DropTable(DropTable),
    DropIndex(DropIndex),
    Insert(Insert),
    Delete(Delete),
    Purge(Vec<Delete>),
    Update(Update),
    Select(Select),
    SetSetting(usize, u64),
    ShowSetting(std::ops::Range<usize>),
    /// `EXPLAIN <statement>`: the statement's path, as rows, without running it.
    Explain(Box<Statement>),
    ShowReport(Show),
    /// `RESET STATEMENT_STATISTICS`: empties the record of statements.
    ResetStatements,
}

#[derive(Debug, Clone)]
pub struct Show {
    pub report: usize,
    pub column: Option<(String, String)>,
    pub limit: Option<u64>,
    pub offset: u64,
}

/// One SELECT as it was written. ORDER BY and what follows it are over the whole statement:
/// over every SELECT of `union` when there is one.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Select {
    /// The SELECTs named by WITH, in written order.
    pub with: Vec<(String, Select)>,
    /// Positional output names of a derived relation.
    pub column_aliases: Vec<String>,
    pub distinct: bool,
    /// `SELECT *` is one column named `*`.
    pub columns: Vec<SelectColumn>,
    /// Empty for a SELECT without FROM.
    pub from: Vec<FromItem>,
    pub joins: Vec<Join>,
    pub filter: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub having: Option<Expr>,
    /// The SELECTs after this one, in written order; true for UNION ALL.
    pub union: Vec<(bool, Select)>,
    /// A bare name the select list gives an item stands here as that item's expression.
    pub order_by: Vec<OrderByItem>,
    /// `USE VECTOR AUTO | EXACT | INDEXED`, when it was written.
    pub use_vector: Option<VectorSearchMode>,
    /// The key after `USE RANK`.
    pub use_rank: Option<String>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
    /// The `$name` written where the count of LIMIT stands, and of OFFSET.
    pub limit_name: Option<String>,
    pub offset_name: Option<String>,
    /// Where its text begins in the statement: what a SELECT inside an expression is found by.
    pub at: Note<usize>,
    /// The type of its one column, kept from the first time a SELECT inside an expression is
    /// framed.
    pub gives: Note<OnceLock<DataType>>,
}

/// What is kept on a statement but is no part of what it says: printed as nothing, so the same
/// expression written twice prints alike.
#[derive(Clone, Default, PartialEq)]
pub struct Note<T>(pub T);

impl<T> std::fmt::Debug for Note<T> {
    fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum FromItem {
    Table { name: String, alias: Option<String> },
    Graph(GraphTable),
}

/// `GRAPH_TABLE(...)`: the rows a pattern matches over an edge table.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct GraphTable {
    pub edge_table: String,
    pub start: NodePattern,
    pub steps: Vec<EdgeStep>,
    pub filter: Option<Expr>,
    pub columns: Vec<SelectColumn>,
    /// What COLUMNS calls each of its columns and the type of each, kept from the first time
    /// the pattern is bound: the statement's text settles both.
    pub output: Note<OnceLock<Vec<crate::types::ColumnDef>>>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct NodePattern {
    pub alias: Option<String>,
}

/// One edge of a pattern and the node it leads to, followed at least `min` and at most `max`
/// times; `max` is never above 10.
#[derive(Debug, Clone, PartialEq)]
pub struct EdgeStep {
    pub direction: EdgeDirection,
    pub edge_type: Option<String>,
    pub min: u32,
    pub max: u32,
    pub to: NodePattern,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EdgeDirection {
    Outgoing,
    Incoming,
    Both,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Join {
    /// LEFT JOIN: a row of the left side with no partner is kept.
    pub left_outer: bool,
    pub item: FromItem,
    /// `None` for CROSS JOIN.
    pub on: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectColumn {
    pub expr: Expr,
    pub alias: Option<String>,
}

impl SelectColumn {
    /// What a result calls the column: its alias; without one a column's own name, a function's
    /// name in lower case, `row_number`, the name a one-column SELECT gives, else `?column?`.
    pub fn name(&self) -> String {
        match (&self.alias, &self.expr) {
            (Some(alias), _) => alias.clone(),
            (None, Expr::Column(column)) => column.column.clone(),
            // The four operators arrive as calls whose names begin `__`.
            (None, Expr::FunctionCall { name, .. }) if !name.starts_with("__") => {
                name.to_lowercase()
            }
            (None, Expr::RowNumber { .. }) => "row_number".to_owned(),
            (None, Expr::Subquery(query)) => match query.columns.as_slice() {
                [item] if item.name() != "*" => item.name(),
                _ => "?column?".to_owned(),
            },
            _ => "?column?".to_owned(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderByItem {
    pub expr: Expr,
    pub direction: SortDirection,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Column(ColumnRef),
    Literal(crate::types::Value),
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
    /// `expr [NOT] IN (SELECT ...)`.
    InSubquery {
        expr: Box<Expr>,
        query: Box<Select>,
        negated: bool,
    },
    /// `(SELECT ...)` standing where a value does.
    Subquery(Box<Select>),
    /// `expr [NOT] LIKE pattern`.
    Like {
        expr: Box<Expr>,
        pattern: Box<Expr>,
        negated: bool,
    },
    /// `expr [NOT] BETWEEN low AND high`.
    Between {
        expr: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
        negated: bool,
    },
    /// `json -> key` and, with `as_text`, `json ->> key`. Both bind as `<=>` does.
    Field {
        json: Box<Expr>,
        key: Box<Expr>,
        as_text: bool,
    },
    /// `ROW_NUMBER() OVER ([PARTITION BY ...] ORDER BY ...)`. The parser refuses every other
    /// function before `OVER`, and a window with no ORDER BY.
    RowNumber {
        partition_by: Vec<Expr>,
        order_by: Vec<OrderByItem>,
    },
}

impl Expr {
    /// The expressions of one side of `=` or IN: those of a row value, or the one expression.
    pub(crate) fn sides(&self) -> &[Expr] {
        match self {
            Expr::Row(items) => items,
            one => std::slice::from_ref(one),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnRef {
    pub table: Option<String>,
    pub column: String,
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reference {
    pub follows_parent_purge: bool,
    pub table: String,
    pub columns: Vec<String>,
    pub on_state: Option<Propagation>,
}

/// `ON STATE on_state PROPAGATE SET set [MAX DEPTH n] [ABORT ON FAILURE]`, and the same four
/// things of a `PROPAGATE ON EDGE`. A state keeps the letters it was written in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Propagation {
    pub on_state: String,
    pub set: String,
    pub max_depth: Option<u32>,
    pub abort_on_failure: bool,
}

/// `PROPAGATE ON EDGE edge_type direction STATE ... SET ...`. The edge type keeps the letters
/// it was written in; the direction is INCOMING, OUTGOING or BOTH, in capitals.
#[derive(Debug, Clone, PartialEq, Eq)]
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
    pub expires: bool,
}

/// `SCOPE_LABEL (...)` stores its single write-only list in `read`, with `write = None`;
/// it hides no rows on reads. `SCOPE_LABEL_READ (...) WRITE (...)` stores separate read
/// and write allowlists in the corresponding fields.
#[derive(Debug, Clone, PartialEq, Eq)]
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
    /// The number after `INDEXING_THRESHOLD`, kept as written for `schema` to judge.
    pub indexing_threshold: Option<String>,
    /// The number after `PROBES`, kept as written for `schema` to judge.
    pub probes: Option<String>,
    /// `COMPACTION NONE` or `COMPACTION (...)`, when it was written.
    pub compaction: Option<CompactionClause>,
    /// The text of `WITH (quantization = '...')`, kept as written for `schema` to judge.
    pub quantization: Option<String>,
}

/// `COMPACTION` as it was written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompactionClause {
    /// `COMPACTION NONE`.
    None,
    /// `COMPACTION (CHANGE_PERCENT = n, TOMBSTONE_PERCENT = n)`: each number as written,
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
    /// `ALTER COLUMN column SET INDEXING_THRESHOLD n`, the number kept as written; `None` is DEFAULT.
    SetIndexingThreshold(String, Option<String>),
    /// `ALTER COLUMN column SET PROBES n`; `None` is DEFAULT.
    SetProbes(String, Option<String>),
    /// `ALTER COLUMN column SET COMPACTION ...`; `None` is DEFAULT.
    SetCompaction(String, Option<CompactionClause>),
}

#[derive(Debug, Clone)]
pub struct DropTable {
    pub name: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone)]
pub struct DropIndex {
    pub name: String,
    /// `None` when written without ON and, once resolved, when IF EXISTS found it on no table.
    pub table: Option<String>,
    pub if_exists: bool,
}

/// A declaration's kind byte also separates its namespace in the schema store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventDeclaration {
    Trigger(crate::types::TriggerDecl),
    EventType(crate::types::EventTypeDecl),
    Sink(crate::types::SinkDecl),
    Route(crate::types::RouteDecl),
    Schedule(crate::types::ScheduleDecl),
}
impl EventDeclaration {
    pub(crate) fn identity(&self) -> (crate::types::Kind, &str) {
        use crate::types::Kind;
        match self {
            Self::Trigger(d) => (Kind::Trigger, &d.name),
            Self::EventType(d) => (Kind::EventType, &d.name),
            Self::Sink(d) => (Kind::Sink, &d.name),
            Self::Route(d) => (Kind::Route, &d.name),
            Self::Schedule(d) => (Kind::Schedule, &d.name),
        }
    }
}
