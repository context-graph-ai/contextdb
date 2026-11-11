//! schema: table declarations: checking them, storing them, printing them back.
//!
//! It never changes a row.
//!
//! A table's declaration is kept as one schema record under the table's name: its number, its
//! columns and rules as they were declared, its indexes with their numbers, and each VECTOR
//! column's number and clauses.
//!
//! A table's indexes come in one order: the index behind the primary key (`__pk_<column>`),
//! the index behind each UNIQUE column in column order (`__unique_<column>`; the key column has
//! its key's index and no other), the index behind each UNIQUE rule of the table in declared
//! order (`__unique_<column>_<column>...`), then the indexes of CREATE INDEX in the order they
//! were made.

mod check;
mod record;
mod render;
#[cfg(test)]
mod tests;

use std::collections::BTreeMap;

use crate::sql::Plan;
use crate::store::{ChangeSet, IndexId, Read, TableId, VectorId, View};
use crate::types::{ColumnType, Error, Result, TableMeta, VectorIndexRef, VectorSearchMode};

/// What the name of an index the engine makes for itself begins with. An index of CREATE INDEX
/// may not take one of these.
const RESERVED_PREFIXES: [&str; 3] = ["__pk_", "__unique_", "__fk_"];

/// One declared table as the other parts use it.
#[derive(Debug, Clone)]
pub(crate) struct Table {
    pub id: TableId,
    pub name: String,
    pub meta: TableMeta,
    /// One id per entry of `meta.indexes`, in the same order.
    pub index_ids: Vec<IndexId>,
    /// One entry per VECTOR column, in column order.
    pub vectors: Vec<VectorColumn>,
}

/// How many partitions a VECTOR column with a PARTITION_KEY may have when its declaration
/// names no MAX_PARTITIONS.
const DEFAULT_MAX_PARTITIONS: u32 = 256;

/// What a table keeps about one of its VECTOR columns: its number in the store and the clauses
/// it was declared with. The column's dimension is in its type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VectorColumn {
    pub id: VectorId,
    /// The column's name.
    pub column: String,
    /// The columns of PARTITION_KEY, in declared order; empty when none was declared.
    pub partition_key: Vec<String>,
    /// MAX_PARTITIONS as declared; `None` when the declaration names none.
    pub max_partitions: Option<u32>,
    /// SEARCH_MODE as declared; `None` when the declaration names none.
    pub search_mode: Option<VectorSearchMode>,
    /// AUTO_INDEX_AT as declared; `None` when the declaration names none.
    pub auto_index_at: Option<u64>,
    /// CONSOLIDATION as declared; `None` when the declaration names none.
    pub consolidation: Option<Consolidation>,
    /// How the column keeps a vector: what `WITH (quantization = ...)` declared.
    pub quantization: Quantization,
}

/// What `CONSOLIDATION` declares: when the vectors written since a partition was grouped, and
/// the slots its rows have left behind, call for grouping it again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Consolidation {
    /// `CONSOLIDATION NONE`: never for those reasons.
    None,
    /// `CONSOLIDATION (CHANGE_PERCENT = n, TOMBSTONE_PERCENT = n)`; a member that was not
    /// written is `None` and takes its default.
    Percent {
        change: Option<u8>,
        tombstone: Option<u8>,
    },
}

/// How a VECTOR column keeps its vectors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum Quantization {
    /// Every number as it was written, in 32 bits.
    #[default]
    F32,
    /// One scale and one byte a number: a write keeps the nearest such vector.
    Sq8,
}

/// AUTO_INDEX_AT of a column that declares none: a comparison of every vector of this many
/// takes about a hundredth of a second on one core, and past it the groupings are worth using.
const DEFAULT_INDEX_AT: u64 = 100_000;
/// A partition is given its first grouping when it holds this share of AUTO_INDEX_AT: one in
/// a hundred, so that the groupings of a store spread over many partitions are in place before
/// AUTO begins to use them.
const GROUPED_SHARE: u64 = 100;
/// CHANGE_PERCENT and TOMBSTONE_PERCENT of a column that declares none.
const DEFAULT_CONSOLIDATION: (u8, u8) = (20, 10);

impl VectorColumn {
    /// The most partitions the column may have: what was declared, 256 when a PARTITION_KEY
    /// was declared with no MAX_PARTITIONS, and `None` for a column with no PARTITION_KEY,
    /// which has its one partition and no limit to meet.
    pub(crate) fn limit(&self) -> Option<u32> {
        let partitioned = !self.partition_key.is_empty();
        partitioned.then(|| self.max_partitions.unwrap_or(DEFAULT_MAX_PARTITIONS))
    }

    /// The mode a search of the column runs in when its statement has no USE VECTOR.
    pub(crate) fn mode(&self) -> VectorSearchMode {
        self.search_mode.unwrap_or_default()
    }

    /// AUTO_INDEX_AT in force: what was declared, else 100,000. The count of vectors in the
    /// selected partitions together from which AUTO uses their groupings.
    pub(crate) fn index_at(&self) -> u64 {
        self.auto_index_at.unwrap_or(DEFAULT_INDEX_AT)
    }

    /// How many slots a partition's tail holds when its first grouping is due: a hundredth of
    /// AUTO_INDEX_AT in force and one at least, so 1,000 where nothing is declared.
    pub(crate) fn grouped_from(&self) -> u64 {
        (self.index_at() / GROUPED_SHARE).max(1)
    }

    /// CHANGE_PERCENT and TOMBSTONE_PERCENT in force, 20 and 10 where none was declared;
    /// `None` under `CONSOLIDATION NONE`.
    pub(crate) fn consolidation(&self) -> Option<(u8, u8)> {
        let (change, tombstone) = DEFAULT_CONSOLIDATION;
        match self.consolidation {
            None => Some((change, tombstone)),
            Some(Consolidation::None) => None,
            Some(Consolidation::Percent {
                change: c,
                tombstone: t,
            }) => Some((c.unwrap_or(change), t.unwrap_or(tombstone))),
        }
    }
}

impl Table {
    /// What the table keeps about the VECTOR column of this name, `None` when it has no such
    /// column or the column is of another type.
    pub(crate) fn vector(&self, column: &str) -> Option<&VectorColumn> {
        self.vectors.iter().find(|vector| vector.column == column)
    }

    /// How many numbers a vector of this VECTOR column has.
    pub(crate) fn dimension(&self, column: &VectorColumn) -> usize {
        let columns = self.meta.columns.iter();
        let mut sizes = columns.filter_map(|declared| match declared.column_type {
            ColumnType::Vector(size) if declared.name == column.column => Some(size),
            _ => None,
        });
        sizes.next().unwrap_or_default()
    }

    /// The column as a refusal names it.
    pub(crate) fn vector_ref(&self, vector: &VectorColumn) -> VectorIndexRef {
        VectorIndexRef {
            table: self.name.clone(),
            column: vector.column.clone(),
        }
    }
}

/// Every table declared in a store at one moment.
pub(crate) struct Catalog {
    tables: BTreeMap<String, Table>,
}

impl Catalog {
    /// Reads the declarations the reader shows. Costs one small record per table; `database`
    /// keeps the result and loads again only when [`View::schema_commit`] has moved.
    pub(crate) fn load(view: &View) -> Result<Catalog> {
        let mut tables = BTreeMap::new();
        for bytes in view.schema_records()? {
            let table = record::decode(&bytes);
            // No declaration names, as a VECTOR column, a column that is not one of its table.
            let is_vector = |vector: &VectorColumn| {
                let column = table.meta.columns.iter().find(|c| c.name == vector.column);
                column.is_some_and(|c| matches!(c.column_type, ColumnType::Vector(_)))
            };
            if !table.vectors.iter().all(is_vector) {
                return Err(view.damaged("a schema record's VECTOR column is not one"));
            }
            tables.insert(table.name.clone(), table);
        }
        Ok(Catalog { tables })
    }

    /// A table by name, as SQL names it.
    pub(crate) fn table(&self, name: &str) -> Option<&Table> {
        self.tables.get(name)
    }

    /// Every table, in name order.
    pub(crate) fn tables(&self) -> Vec<&Table> {
        self.tables.values().collect()
    }

    /// The table that has the VECTOR column with this number, and what it keeps about it.
    pub(crate) fn vector(&self, id: VectorId) -> Option<(&Table, &VectorColumn)> {
        self.tables.values().find_map(|table| {
            let vector = table.vectors.iter().find(|vector| vector.id == id)?;
            Some((table, vector))
        })
    }
}

/// Applies one declaration (CREATE TABLE, CREATE INDEX, ALTER TABLE, DROP) to a change set.
///
/// Checks the declaration against the catalog, stages its schema record and returns the
/// catalog as it will be once the change set commits. Stages no row and no index entry; after
/// a CREATE INDEX on a table that has rows, `database` asks `write` to fill the index.
pub(crate) fn declare(plan: &Plan, catalog: &Catalog, changes: &mut ChangeSet) -> Result<Catalog> {
    let mut tables = catalog.tables.clone();
    let table = match plan {
        Plan::CreateTable(create) if !tables.contains_key(&create.name) => {
            check::new_table(create, catalog)?
        }
        // A taken name is settled before the rest of the declaration is looked at.
        Plan::CreateTable(create) if create.if_not_exists => return Ok(Catalog { tables }),
        Plan::CreateTable(create) => {
            return Err(Error::SchemaInvalid {
                reason: format!(
                    "table '{}' already exists: CREATE TABLE never redefines a table, so nothing \
                     was changed; CREATE TABLE IF NOT EXISTS leaves an existing table as it is",
                    create.name
                ),
            });
        }
        Plan::CreateIndex(create) => check::with_index(create, catalog)?,
        _ => return Err(crate::sql::not_supported("this declaration")),
    };
    changes.put_schema(&table.name, record::encode(&table))?;
    tables.insert(table.name.clone(), table);
    Ok(Catalog { tables })
}

/// The declaration as `.schema` prints it: text that, run again, declares the same table.
pub(crate) fn render(table: &Table) -> String {
    render::declaration(table)
}
