//! The planned statement: what `schema`, `write` and `query` each take.

/// A declaration or a write is planned as it was written: `schema` and `write` judge every
/// clause of it.
pub use super::ast::{
    AlterTable as AlterTablePlan, CreateIndex as CreateIndexPlan, CreateTable as CreateTablePlan,
    Delete as DeletePlan, DropIndex as DropIndexPlan, DropTable as DropTablePlan,
    Insert as InsertPlan, Update as UpdatePlan,
};
use super::ast::{Expr, SetMemoryLimitValue, SortDirection, VectorSearchMode};

#[derive(Debug, Clone)]
pub enum PhysicalPlan {
    CreateTable(CreateTablePlan),
    CreateIndex(CreateIndexPlan),
    AlterTable(AlterTablePlan),
    DropTable(DropTablePlan),
    DropIndex(DropIndexPlan),
    Insert(InsertPlan),
    Delete(DeletePlan),
    Update(UpdatePlan),
    /// Every row of a table that passes the filter. `query` chooses how to reach them.
    Scan {
        table: String,
        filter: Option<Expr>,
    },
    Sort {
        input: Box<PhysicalPlan>,
        keys: Vec<SortKey>,
    },
    Project {
        input: Box<PhysicalPlan>,
        columns: Vec<ProjectColumn>,
    },
    Limit {
        input: Box<PhysicalPlan>,
        count: u64,
    },
    /// The `k` rows of a table that pass the filter and lie nearest `query` by cosine distance
    /// in a VECTOR column, nearest first. `query` decides which partitions the filter selects
    /// and the vector part finds the rows. It stands where `Sort` over `Scan` stands in other
    /// plans; its LIMIT is `k`, so no `Limit` wraps it. A `Project` over it may hold the
    /// statement's distance expression, `column <=> query`, any number of times: each stands
    /// for the distance the search found for the row.
    VectorSearch {
        table: String,
        /// The VECTOR column on the left of `<=>`.
        column: String,
        /// The right of `<=>`.
        query: Expr,
        k: u64,
        filter: Option<Expr>,
        /// What `USE VECTOR` asked for; `None` leaves it to the column's declaration.
        mode: Option<VectorSearchMode>,
    },
    SetMemoryLimit(SetMemoryLimitValue),
    ShowMemoryLimit,
    /// `SHOW VECTOR_INDEXES`: one row for each VECTOR column of the store.
    ShowVectorIndexes,
    /// `SHOW VECTOR_PARTITIONS [FOR table.column] [LIMIT count [OFFSET start]]`: one row for
    /// each partition, of the named column or of every VECTOR column.
    ShowVectorPartitions {
        /// The table and column after `FOR`.
        column: Option<(String, String)>,
        limit: Option<u64>,
        offset: u64,
    },
}

/// What a planned statement does to the store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Effect {
    /// Declares, changes or drops a table or an index.
    Declares,
    /// Adds, changes or removes rows.
    WritesRows,
    /// Changes a setting the store keeps. There is one: the memory limit, set to this many
    /// bytes, or to no limit for `None`.
    Sets(Option<u64>),
    /// Changes nothing that is stored.
    Reads,
}

impl PhysicalPlan {
    /// Which of these this statement is. A new kind of plan is given its answer here, in the
    /// one place every part asks.
    pub(crate) fn effect(&self) -> Effect {
        match self {
            PhysicalPlan::CreateTable(_)
            | PhysicalPlan::CreateIndex(_)
            | PhysicalPlan::AlterTable(_)
            | PhysicalPlan::DropTable(_)
            | PhysicalPlan::DropIndex(_) => Effect::Declares,
            PhysicalPlan::Insert(_) | PhysicalPlan::Delete(_) | PhysicalPlan::Update(_) => {
                Effect::WritesRows
            }
            PhysicalPlan::SetMemoryLimit(SetMemoryLimitValue::Bytes(bytes)) => {
                Effect::Sets(Some(*bytes as u64))
            }
            PhysicalPlan::SetMemoryLimit(SetMemoryLimitValue::None) => Effect::Sets(None),
            PhysicalPlan::Scan { .. }
            | PhysicalPlan::Sort { .. }
            | PhysicalPlan::Project { .. }
            | PhysicalPlan::Limit { .. }
            | PhysicalPlan::VectorSearch { .. }
            | PhysicalPlan::ShowMemoryLimit
            | PhysicalPlan::ShowVectorIndexes
            | PhysicalPlan::ShowVectorPartitions { .. } => Effect::Reads,
        }
    }

    /// The statement's name when only a handle that declared no narrowing may run it: what it
    /// declares, sets or reads holds for every Context, label and principal at once.
    pub(crate) fn admin_only(&self) -> Option<&'static str> {
        Some(match self {
            PhysicalPlan::CreateTable(_) => "CREATE TABLE",
            PhysicalPlan::CreateIndex(_) => "CREATE INDEX",
            PhysicalPlan::AlterTable(_) => "ALTER TABLE",
            PhysicalPlan::DropTable(_) => "DROP TABLE",
            PhysicalPlan::DropIndex(_) => "DROP INDEX",
            PhysicalPlan::SetMemoryLimit(_) => "SET MEMORY_LIMIT",
            PhysicalPlan::ShowVectorIndexes => "SHOW VECTOR_INDEXES",
            PhysicalPlan::ShowVectorPartitions { .. } => "SHOW VECTOR_PARTITIONS",
            _ => return None,
        })
    }
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
