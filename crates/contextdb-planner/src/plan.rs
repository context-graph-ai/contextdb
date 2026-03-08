use contextdb_core::Direction;
use contextdb_parser::ast::{Expr, OnConflict};

#[derive(Debug, Clone)]
pub enum PhysicalPlan {
    CreateTable(CreateTablePlan),
    DropTable(String),
    CreateIndex(CreateIndexPlan),
    Insert(InsertPlan),
    Delete(DeletePlan),
    Update(UpdatePlan),
    Scan {
        table: String,
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
    Filter {
        input: Box<PhysicalPlan>,
        predicate: Expr,
    },
    Project {
        input: Box<PhysicalPlan>,
        columns: Vec<ProjectColumn>,
    },
    Join {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        condition: Expr,
        join_type: JoinType,
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
            PhysicalPlan::Scan { table, .. } => format!("Scan(table={})", table),
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
