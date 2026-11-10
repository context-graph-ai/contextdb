//! schema: table declarations: checking them, storing them, printing them back.
//!
//! It never changes a row.
//!
//! A table's declaration is kept as one schema record under the table's name: its number, its
//! columns and rules as they were declared, and its indexes with their numbers.
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
use crate::store::{ChangeSet, IndexId, TableId, View};
use crate::types::{Error, Result, TableMeta};

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
        _ => return Err(check::not_supported("this declaration")),
    };
    changes.put_schema(&table.name, record::encode(&table))?;
    tables.insert(table.name.clone(), table);
    Ok(Catalog { tables })
}

/// The declaration as `.schema` prints it: text that, run again, declares the same table.
pub(crate) fn render(table: &Table) -> String {
    render::declaration(table)
}
