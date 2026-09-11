use contextdb_core::{Direction, PropagationRule};
use contextdb_parser::ast::{
    AlterAction, ColumnDef, CompositeForeignKey, Cte, Expr, OnConflict, RetainOption,
    SetDiskLimitValue, SetMemoryLimitValue, SortDirection, StateMachineDef, VectorSearchMode,
};

#[derive(Debug, Clone)]
pub enum PhysicalPlan {
    CreateTable(CreateTablePlan),
    AlterTable(AlterTablePlan),
    DropTable(String),
    CreateIndex(CreateIndexPlan),
    DropIndex(DropIndexPlan),
    Insert(InsertPlan),
    Purge(PurgePlan),
    Discard(DiscardPlan),
    Delete(DeletePlan),
    Update(UpdatePlan),
    Scan {
        table: String,
        alias: Option<String>,
        filter: Option<Expr>,
    },
    IndexScan {
        table: String,
        index: String,
        range: ScanRange,
    },
    GraphBfs {
        start_alias: String,
        start_expr: Expr,
        start_candidates: Option<Box<PhysicalPlan>>,
        filter_ctes: Vec<Cte>,
        steps: Vec<GraphStepPlan>,
        filter: Option<Expr>,
    },
    VectorSearch {
        table: String,
        column: String,
        query_expr: Expr,
        k: u64,
        candidates: Option<Box<PhysicalPlan>>,
        sort_key: Option<String>,
        search_mode: Option<VectorSearchMode>,
        /// Columns whose row values the outer SELECT actually reads. `None`
        /// means `SELECT *`; an empty vector means the search answer needs no
        /// stored row value beyond its synthetic row id and score.
        materialized_columns: Option<Vec<String>>,
    },
    HnswSearch {
        table: String,
        column: String,
        query_expr: Expr,
        k: u64,
        candidates: Option<Box<PhysicalPlan>>,
        sort_key: Option<String>,
        search_mode: Option<VectorSearchMode>,
        materialized_columns: Option<Vec<String>>,
    },
    Filter {
        input: Box<PhysicalPlan>,
        predicate: Expr,
    },
    Project {
        input: Box<PhysicalPlan>,
        columns: Vec<ProjectColumn>,
    },
    Distinct {
        input: Box<PhysicalPlan>,
    },
    Join {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        condition: Expr,
        join_type: JoinType,
        left_alias: Option<String>,
        right_alias: Option<String>,
    },
    Sort {
        input: Box<PhysicalPlan>,
        keys: Vec<SortKey>,
    },
    Limit {
        input: Box<PhysicalPlan>,
        count: u64,
    },
    MaterializeCte {
        name: String,
        input: Box<PhysicalPlan>,
    },
    CteRef {
        name: String,
    },
    Union {
        inputs: Vec<PhysicalPlan>,
        all: bool,
    },
    Pipeline(Vec<PhysicalPlan>),
    SetMemoryLimit(SetMemoryLimitValue),
    ShowMemoryLimit,
    SetDiskLimit(SetDiskLimitValue),
    ShowDiskLimit,
    SetMaintenancePollInterval(u64),
    ShowMaintenancePollInterval,
    ShowSyncConflictPolicy,
    ShowVectorIndexes,
    DeclareTenantTablePolicy(DeclareTenantTablePolicyPlan),
    ShowTenantTablePolicy {
        table: Option<String>,
    },
    ShowSyncBindings,
    ShowDeliveryOutcomes(ShowDeliveryOutcomesPlan),
    ShowVectorPartitions {
        table: Option<String>,
        column: Option<String>,
        limit: Option<u64>,
        offset: Option<u64>,
    },
}

impl PhysicalPlan {
    /// The table-local residual predicate carried into a vector search.
    /// It is intentionally only a predicate source: after parameter binding
    /// the engine may ask the relational store for *unauthorised* candidates,
    /// then intersects those ids with its independent authorization result.
    pub fn vector_residual_filters(&self) -> Vec<&Expr> {
        match self {
            PhysicalPlan::Scan { filter, .. } => filter.iter().collect(),
            PhysicalPlan::Filter { input, predicate } => {
                let mut filters = input.vector_residual_filters();
                filters.push(predicate);
                filters
            }
            PhysicalPlan::Project { input, .. }
            | PhysicalPlan::Distinct { input }
            | PhysicalPlan::Sort { input, .. }
            | PhysicalPlan::Limit { input, .. }
            | PhysicalPlan::MaterializeCte { input, .. } => input.vector_residual_filters(),
            _ => Vec::new(),
        }
    }

    pub fn explain(&self) -> String {
        match self {
            PhysicalPlan::GraphBfs { steps, .. } => {
                format!(
                    "GraphBfs(steps={})",
                    steps
                        .iter()
                        .map(|step| format!(
                            "{}..{}:{:?}",
                            step.min_depth, step.max_depth, step.edge_types
                        ))
                        .collect::<Vec<_>>()
                        .join(" -> ")
                )
            }
            PhysicalPlan::VectorSearch {
                table,
                column,
                query_expr,
                k,
                ..
            } => {
                format!(
                    "VectorSearch(table={}, column={}, k={}{}{})",
                    table,
                    column,
                    k,
                    query_source_prefix(query_expr),
                    query_source_suffix(query_expr)
                )
            }
            PhysicalPlan::HnswSearch {
                table,
                column,
                query_expr,
                k,
                ..
            } => {
                format!(
                    "HNSWSearch(table={}, column={}, k={}{}{})",
                    table,
                    column,
                    k,
                    query_source_prefix(query_expr),
                    query_source_suffix(query_expr)
                )
            }
            PhysicalPlan::Scan { table, .. } => format!("Scan(table={})", table),
            PhysicalPlan::AlterTable(p) => format!("AlterTable(table={})", p.table),
            PhysicalPlan::Insert(p) => format!("Insert(table={})", p.table),
            PhysicalPlan::Purge(p) => format!("Purge(selections={})", p.selections.len()),
            PhysicalPlan::Discard(p) => format!("Discard(selections={})", p.selections.len()),
            PhysicalPlan::Delete(p) => format!("Delete(table={})", p.table),
            PhysicalPlan::Update(p) => format!("Update(table={})", p.table),
            PhysicalPlan::Pipeline(plans) => plans
                .iter()
                .map(Self::explain)
                .collect::<Vec<_>>()
                .join(" -> "),
            PhysicalPlan::Project { input, .. } => {
                format!("Project -> {}", input.explain())
            }
            _ => format!("{:?}", self),
        }
    }
}

fn query_source_prefix(expr: &Expr) -> &'static str {
    match expr {
        Expr::RowVectorSource { .. } => ", query_source=",
        _ => "",
    }
}

fn query_source_suffix(expr: &Expr) -> String {
    match expr {
        Expr::RowVectorSource { table, column, key } => {
            format!(
                "RowVectorSource(table={}, column={}, key={})",
                table,
                column,
                row_vector_key_for_explain(key)
            )
        }
        _ => String::new(),
    }
}

fn row_vector_key_for_explain(expr: &Expr) -> String {
    match expr {
        Expr::Literal(_) | Expr::Parameter(_) => "<redacted>".to_string(),
        _ => "<expr>".to_string(),
    }
}

#[derive(Debug, Clone)]
pub struct GraphStepPlan {
    pub edge_types: Vec<String>,
    pub direction: Direction,
    pub min_depth: u32,
    pub max_depth: u32,
    pub target_alias: String,
}

#[derive(Debug, Clone)]
pub struct CreateTablePlan {
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub unique_constraints: Vec<Vec<String>>,
    /// The ordered columns of a table-level `PRIMARY KEY (a, b, ...)`; empty
    /// for a single-column or absent primary key.
    pub primary_key_columns: Vec<String>,
    pub composite_foreign_keys: Vec<CompositeForeignKey>,
    /// `true` when the declaration wrote `IF NOT EXISTS`. This is not part of
    /// the table's shape -- it only decides what a collision on an existing
    /// name does: the bare spelling is refused, this one is a no-op. The
    /// executor needs it here for the same reason `DropIndexPlan` carries
    /// `if_exists`.
    pub if_not_exists: bool,
    pub immutable: bool,
    pub state_machine: Option<StateMachineDef>,
    pub dag_edge_types: Vec<String>,
    pub propagation_rules: Vec<PropagationRule>,
    pub retain: Option<RetainOption>,
    pub sync_direction: Option<contextdb_core::SyncDirection>,
    pub conflict_policy: Option<contextdb_core::ConflictPolicy>,
    pub history: Option<contextdb_core::HistoryPolicy>,
    pub delivery_manifest_tables: Option<Vec<String>>,
    pub edge_discard: Option<contextdb_core::EdgeDiscardMode>,
}

#[derive(Debug, Clone)]
pub struct AlterTablePlan {
    pub table: String,
    pub action: AlterAction,
}

#[derive(Debug, Clone)]
pub struct CreateIndexPlan {
    pub name: String,
    pub table: String,
    pub columns: Vec<(String, contextdb_core::SortDirection)>,
}

#[derive(Debug, Clone)]
pub struct DropIndexPlan {
    pub name: String,
    pub table: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone)]
pub struct InsertPlan {
    pub table: String,
    pub columns: Vec<String>,
    pub values: Vec<Vec<Expr>>,
    pub on_conflict: Option<OnConflictPlan>,
}

#[derive(Debug, Clone)]
pub struct OnConflictPlan {
    pub columns: Vec<String>,
    pub update_columns: Vec<(String, Expr)>,
}

#[derive(Debug, Clone)]
pub struct DeletePlan {
    pub table: String,
    pub where_clause: Option<Expr>,
}

#[derive(Debug, Clone)]
pub struct PurgePlan {
    pub selections: Vec<ErasureSelectionPlan>,
}

#[derive(Debug, Clone)]
pub struct DiscardPlan {
    pub selections: Vec<ErasureSelectionPlan>,
}

#[derive(Debug, Clone)]
pub struct ErasureSelectionPlan {
    pub table: String,
    pub where_clause: Option<Expr>,
}

#[derive(Debug, Clone)]
pub struct DeclareTenantTablePolicyPlan {
    pub declaration: contextdb_parser::ast::DeclareTenantTablePolicy,
}

#[derive(Debug, Clone)]
pub struct ShowDeliveryOutcomesPlan {
    pub query: contextdb_parser::ast::ShowDeliveryOutcomes,
}

#[derive(Debug, Clone)]
pub struct UpdatePlan {
    pub table: String,
    pub assignments: Vec<(String, Expr)>,
    pub where_clause: Option<Expr>,
}

#[derive(Debug, Clone)]
pub struct ProjectColumn {
    pub expr: Expr,
    pub alias: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SortKey {
    pub expr: Expr,
    pub direction: SortDirection,
}

#[derive(Debug, Clone, Copy)]
pub enum JoinType {
    Inner,
    Left,
}

#[derive(Debug, Clone)]
pub struct ScanRange {
    pub lower: std::ops::Bound<contextdb_core::Value>,
    pub upper: std::ops::Bound<contextdb_core::Value>,
    pub equality: Option<contextdb_core::Value>,
}

impl Default for ScanRange {
    fn default() -> Self {
        Self {
            lower: std::ops::Bound::Unbounded,
            upper: std::ops::Bound::Unbounded,
            equality: None,
        }
    }
}

impl From<OnConflict> for OnConflictPlan {
    fn from(value: OnConflict) -> Self {
        Self {
            columns: value.columns,
            update_columns: value.update_columns,
        }
    }
}
