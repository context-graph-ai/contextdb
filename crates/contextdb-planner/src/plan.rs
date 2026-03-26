use contextdb_core::{Direction, PropagationRule};
use contextdb_parser::ast::{
    AlterAction, ColumnDef, Expr, OnConflict, SetMemoryLimitValue, SortDirection, StateMachineDef,
};

#[derive(Debug, Clone)]
pub enum PhysicalPlan {
    CreateTable(CreateTablePlan),
    AlterTable(AlterTablePlan),
    DropTable(String),
    CreateIndex(CreateIndexPlan),
    Insert(InsertPlan),
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
        start_expr: Expr,
        edge_types: Vec<String>,
        direction: Direction,
        min_depth: u32,
        max_depth: u32,
        filter: Option<Expr>,
    },
    VectorSearch {
        table: String,
        column: String,
        query_expr: Expr,
        k: u64,
        candidates: Option<Box<PhysicalPlan>>,
    },
    HnswSearch {
        table: String,
        column: String,
        query_expr: Expr,
        k: u64,
        candidates: Option<Box<PhysicalPlan>>,
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
    SetSyncConflictPolicy(String),
    ShowSyncConflictPolicy,
}

impl PhysicalPlan {
    pub fn explain(&self) -> String {
        match self {
            PhysicalPlan::GraphBfs {
                min_depth,
                max_depth,
                edge_types,
                ..
            } => {
                format!(
                    "GraphBfs(depth={}..{}, types={:?})",
                    min_depth, max_depth, edge_types
                )
            }
            PhysicalPlan::VectorSearch {
                table, column, k, ..
            } => {
                format!("VectorSearch(table={}, column={}, k={})", table, column, k)
            }
            PhysicalPlan::HnswSearch {
                table, column, k, ..
            } => {
                format!("HNSWSearch(table={}, column={}, k={})", table, column, k)
            }
            PhysicalPlan::Scan { table, .. } => format!("Scan(table={})", table),
            PhysicalPlan::AlterTable(p) => format!("AlterTable(table={})", p.table),
            PhysicalPlan::Insert(p) => format!("Insert(table={})", p.table),
            PhysicalPlan::Delete(p) => format!("Delete(table={})", p.table),
            PhysicalPlan::Update(p) => format!("Update(table={})", p.table),
            PhysicalPlan::Pipeline(plans) => plans
                .iter()
                .map(Self::explain)
                .collect::<Vec<_>>()
                .join(" -> "),
            _ => format!("{:?}", self),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CreateTablePlan {
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub immutable: bool,
    pub state_machine: Option<StateMachineDef>,
    pub dag_edge_types: Vec<String>,
    pub propagation_rules: Vec<PropagationRule>,
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
    pub columns: Vec<String>,
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
pub struct ScanRange;

impl From<OnConflict> for OnConflictPlan {
    fn from(value: OnConflict) -> Self {
        Self {
            columns: value.columns,
            update_columns: value.update_columns,
        }
    }
}
