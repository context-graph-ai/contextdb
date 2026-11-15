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
//! order (`__unique_<column>_<column>...`), the index that serves the child side of each
//! reference no earlier index leads with (`__fk_<column>...`), the two indexes of a table of
//! edges (`__graph_edge_...`), then the indexes of CREATE INDEX in the order they were made.

mod alter;
mod check;
mod record;
mod render;
mod rules;
#[cfg(test)]
mod tests;

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::sync::Arc;

pub(crate) use self::alter::Declared;
pub(crate) use self::rules::{Reference, Rules};
use crate::query::Scope;
use crate::sql::plan::InsertPlan;
use crate::sql::{Expr, Plan};
use crate::store::{
    Cell, ChangeSet, Entry, EntryShape, IndexId, Read, Row, TableId, VectorId, View,
};
use crate::types::{
    ColumnType, Error, IndexDecl, Result, TableMeta, Value, VectorIndexRef, VectorSearchMode,
};

/// What the name of an index the engine makes for itself begins with. An index of CREATE INDEX
/// may not take one of these.
const RESERVED_PREFIXES: [&str; 4] = ["__pk_", "__unique_", "__fk_", "__graph_"];

/// What [`Table::insert`] gives.
pub(crate) type Inserted<'p> = (Vec<usize>, Option<(Vec<usize>, Vec<(usize, &'p Expr)>)>);

/// One declared table as the other parts use it. The rules a host reads are in `meta`; what
/// `schema` works out from the declaration beyond them is in `rules`.
#[derive(Clone)]
pub(crate) struct Table {
    pub id: TableId,
    pub name: String,
    pub meta: TableMeta,
    /// One id per entry of `meta.indexes`, in the same order.
    pub index_ids: Vec<IndexId>,
    /// One entry per VECTOR column, in column order.
    pub vectors: Vec<VectorColumn>,
    /// The primary key's columns in declared order; empty for a table that declares no key.
    pub key: Vec<usize>,
    /// The key was written as an element of the table, `PRIMARY KEY (...)`, not on its column.
    pub key_is_element: bool,
    /// For each column, what a row with no cell for it reads as.
    pub missing: Vec<Value>,
    pub rules: Rules,
    /// How the entries of each index are made, in the order of `meta.indexes`.
    shapes: Arc<[EntryShape]>,
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
    /// INDEXING_THRESHOLD as declared; `None` when the declaration names none.
    pub indexing_threshold: Option<u64>,
    /// COMPACTION as declared; `None` when the declaration names none.
    pub compaction: Option<Compaction>,
    /// How the column keeps a vector: what `WITH (quantization = ...)` declared.
    pub quantization: Quantization,
}

/// What `COMPACTION` declares: when the vectors written since a partition was grouped, and
/// the slots its rows have left behind, call for grouping it again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Compaction {
    /// `COMPACTION NONE`: never for those reasons.
    None,
    /// `COMPACTION (CHANGE_PERCENT = n, TOMBSTONE_PERCENT = n)`; a member that was not
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

/// INDEXING_THRESHOLD of a column that declares none: a comparison of every vector of this many
/// takes about a hundredth of a second on one core, and past it the groupings are worth using.
const DEFAULT_INDEX_AT: u64 = 100_000;
/// A partition is given its first grouping when it holds this share of INDEXING_THRESHOLD: one in
/// a hundred, so that the groupings of a store spread over many partitions are in place before
/// AUTO begins to use them.
const GROUPED_SHARE: u64 = 100;
/// CHANGE_PERCENT and TOMBSTONE_PERCENT of a column that declares none.
const DEFAULT_COMPACTION: (u8, u8) = (20, 10);

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

    /// INDEXING_THRESHOLD in force: what was declared, else 100,000. The count of vectors in the
    /// selected partitions together from which AUTO uses their groupings.
    pub(crate) fn index_at(&self) -> u64 {
        self.indexing_threshold.unwrap_or(DEFAULT_INDEX_AT)
    }

    /// How many slots a partition's tail holds when its first grouping is due: a hundredth of
    /// INDEXING_THRESHOLD in force and one at least, so 1,000 where nothing is declared.
    pub(crate) fn grouped_from(&self) -> u64 {
        (self.index_at() / GROUPED_SHARE).max(1)
    }

    /// CHANGE_PERCENT and TOMBSTONE_PERCENT in force, 20 and 10 where none was declared;
    /// `None` under `COMPACTION NONE`.
    pub(crate) fn compaction(&self) -> Option<(u8, u8)> {
        let (change, tombstone) = DEFAULT_COMPACTION;
        match self.compaction {
            None => Some((change, tombstone)),
            Some(Compaction::None) => None,
            Some(Compaction::Percent {
                change: c,
                tombstone: t,
            }) => Some((c.unwrap_or(change), t.unwrap_or(tombstone))),
        }
    }
}

impl Table {
    /// The table's columns as an expression reaches them: under the table's name, in row order.
    pub(crate) fn scope(&self) -> Scope<'_> {
        Scope::of(&self.name, &self.meta.columns)
    }

    /// Where the column a statement or an index names is in a row of the table.
    pub(crate) fn column(&self, name: &str) -> Result<usize> {
        rules::position(&self.name, &self.meta, name)
    }

    /// Where each column a statement writes is in a row; one named twice is refused.
    pub(crate) fn targets<'n>(&self, of: impl Iterator<Item = &'n String>) -> Result<Vec<usize>> {
        let mut targets = Vec::new();
        for name in of {
            let at = self.column(name)?;
            if targets.contains(&at) {
                let table = &self.name;
                let refusal = format!("{name} is named twice as a column {table} is written in");
                return Err(Error::PlanError(refusal));
            }
            targets.push(at);
        }
        Ok(targets)
    }

    /// Where each assigned column is in a row, with its new value.
    pub(crate) fn set<'e>(&self, set: &'e [(String, Expr)]) -> Result<Vec<(usize, &'e Expr)>> {
        let at = self.targets(set.iter().map(|(name, _)| name))?;
        let values = set.iter().map(|(_, value)| value);
        Ok(at.into_iter().zip(values).collect())
    }

    /// Checks an INSERT's columns, its `ON CONFLICT` and the length of each row of VALUES. Gives
    /// where each column is, and the columns of `ON CONFLICT` with where each assignment writes.
    pub(crate) fn insert<'p>(&self, plan: &'p InsertPlan) -> Result<Inserted<'p>> {
        let named = match plan.columns.is_empty() {
            true => (0..self.meta.columns.len()).collect(),
            false => self.targets(plan.columns.iter())?,
        };
        let conflict = match &plan.on_conflict {
            Some(conflict) => {
                let on = conflict.columns.iter().map(|name| self.column(name));
                let mut on: Vec<usize> = on.collect::<Result<_>>()?;
                on.sort_unstable();
                let is_on = |index: &IndexDecl| {
                    let at = index.columns.iter().map(|(name, _)| self.column(name).ok());
                    let mut at: Vec<_> = at.collect::<Option<_>>().unwrap_or_default();
                    at.sort_unstable();
                    rules::is_rule(index) && at == on
                };
                if !self.meta.indexes.iter().any(is_on) {
                    return Err(Error::PlanError(format!(
                        "ON CONFLICT ({}) names neither the primary key nor a UNIQUE rule of {}",
                        conflict.columns.join(", "),
                        self.name
                    )));
                }
                Some((on, self.set(&conflict.assignments)?))
            }
            None => None,
        };
        let (table, columns) = (&self.name, named.len());
        if let Some(given) = plan.values.iter().map(Vec::len).find(|&n| n != columns) {
            let refusal = format!("INSERT INTO {table} gives {given} values for {columns} columns");
            return Err(Error::PlanError(refusal));
        }
        Ok((named, conflict))
    }

    /// The cell a stored row holds for a column. A row written before the column was added has
    /// none, and reads as the table says such a row does.
    pub(crate) fn cell<'a>(&self, row: &'a Row, column: usize) -> Cow<'a, Cell> {
        match row.cells.get(column) {
            Some(cell) => Cow::Borrowed(cell),
            None => Cow::Owned(Cell::Here(self.missing[column].clone())),
        }
    }

    /// How the entries of the index at this place in `meta.indexes` are made.
    pub(crate) fn entry_shape(&self, index: usize) -> &EntryShape {
        &self.shapes[index]
    }

    /// The entry, in the index at this place in `meta.indexes`, of a row holding `values` in its
    /// columns. One over a JSON path has none for a row with no [`field_key`] at the path.
    pub(crate) fn entry(&self, index: usize, values: &[Value]) -> Result<Option<Entry>> {
        let path = &self.meta.indexes[index].json_path;
        let field = match (path.is_empty(), values) {
            (true, _) => return self.shapes[index].entry(values).map(Some),
            (false, [Value::Json(json)]) => path.iter().try_fold(json, |json, key| json.get(key)),
            (false, _) => None,
        };
        let key = field.and_then(field_key);
        key.map(|key| self.shapes[index].entry(&[key])).transpose()
    }

    /// The table with the shape of each of its indexes, made once here: every table of a
    /// catalog has been through this.
    fn shaped(mut self) -> Table {
        let shape = |index: &IndexDecl| {
            let columns = index.columns.iter();
            let directions: Vec<_> = columns.clone().map(|(_, direction)| *direction).collect();
            let of = |name: &String| self.meta.columns.iter().find(|c| c.name == *name);
            let types: Vec<ColumnType> = match index.json_path.is_empty() {
                true => columns
                    .filter_map(|(name, _)| Some(of(name)?.column_type))
                    .collect(),
                // An index over a JSON path is one TEXT column to its entries.
                false => vec![ColumnType::Text],
            };
            EntryShape::new(&types, &directions)
        };
        self.shapes = self.meta.indexes.iter().map(shape).collect();
        self
    }

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

/// What an index over a JSON path keeps for the field there: its text as JSON writes it, a
/// number as its REAL value (adding zero makes `-0` into `0`), so that fields `=` finds equal
/// keep one text; nothing for an object or an array.
pub(crate) fn field_key(field: &serde_json::Value) -> Option<Value> {
    Some(Value::Text(match field {
        serde_json::Value::Object(_) | serde_json::Value::Array(_) => return None,
        serde_json::Value::Number(number) => (number.as_f64()? + 0.0).to_string(),
        scalar => scalar.to_string(),
    }))
}

/// Every table declared in a store at one moment.
pub(crate) struct Catalog {
    tables: BTreeMap<String, Table>,
    /// The tables a transaction has altered or dropped, as they were before. What they held a
    /// number for may still be in the store under it until the commit, so no declaration of
    /// the same transaction is given one of their numbers.
    left: Vec<Table>,
}

impl Catalog {
    /// Reads the declarations the reader shows. Costs one small record per table; `database`
    /// keeps the result and loads again only when [`View::schema_commit`] has moved.
    pub(crate) fn load(view: &View) -> Result<Catalog> {
        let mut tables = BTreeMap::new();
        for bytes in view.schema_records()? {
            let table = record::decode(&bytes, view)?;
            // No declaration names, as a VECTOR column, a column that is not one of its table.
            let is_vector = |vector: &VectorColumn| {
                let column = table.meta.columns.iter().find(|c| c.name == vector.column);
                column.is_some_and(|c| matches!(c.column_type, ColumnType::Vector(_)))
            };
            if !table.vectors.iter().all(is_vector) {
                return Err(view.damaged("a schema record's VECTOR column is not one"));
            }
            tables.insert(table.name.clone(), table.shaped());
        }
        let left = Vec::new();
        Ok(Catalog { tables, left })
    }

    /// A table by name, as SQL names it.
    pub(crate) fn table(&self, name: &str) -> Option<&Table> {
        self.tables.get(name)
    }

    /// Every table that holds numbers: those declared, and those a transaction has altered or
    /// dropped, as they were. A new table, index, kept count or VECTOR column is numbered one
    /// past the highest of these.
    fn numbered(&self) -> impl Iterator<Item = &Table> {
        self.tables.values().chain(&self.left)
    }

    /// Every table, in name order.
    pub(crate) fn tables(&self) -> Vec<&Table> {
        self.tables.values().collect()
    }

    /// The name of every table, in name order.
    pub(crate) fn table_names(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }

    /// A table's declaration as `.schema` prints it, or `TableNotFound`.
    pub(crate) fn declaration(&self, name: &str) -> Result<String> {
        let table = self.table(name).map(render);
        table.ok_or_else(|| Error::TableNotFound(name.to_owned()))
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
/// a CREATE INDEX on a table that has rows, `database` asks `write` to fill the index. `now`
/// is the time of the statement, which a row written before an added column reads where the
/// column's default is `NOW()`.
pub(crate) fn declare(
    plan: &Plan,
    catalog: &Catalog,
    changes: &mut ChangeSet,
    now: i64,
) -> Result<Declared> {
    let (mut tables, left) = (catalog.tables.clone(), catalog.left.clone());
    let table = match plan {
        Plan::CreateTable(create) if !tables.contains_key(&create.name) => {
            check::new_table(create, catalog)?
        }
        // A taken name is settled before the rest of the declaration is looked at.
        Plan::CreateTable(create) if create.if_not_exists => {
            return Ok(Catalog { tables, left }.into());
        }
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
        _ => return alter::declare(plan, catalog, changes, now),
    };
    // The counts a CREATE INDEX made on its table hold no record yet.
    let old = catalog.table(&table.name);
    let before = |id| old.is_some_and(|old| old.rules.counts.iter().any(|count| count.id == id));
    let made = table.rules.counts.iter().map(|count| count.id);
    let counts = made.filter(|id| !before(*id)).collect();
    changes.put_schema(&table.name, record::encode(&table))?;
    tables.insert(table.name.clone(), table.shaped());
    let mut declared = Declared::from(Catalog { tables, left });
    declared.counts = counts;
    Ok(declared)
}

/// The declaration as `.schema` prints it: text that, run again, declares the same table.
fn render(table: &Table) -> String {
    render::declaration(table)
}
