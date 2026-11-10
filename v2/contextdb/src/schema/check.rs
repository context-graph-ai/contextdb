//! Checking a declaration: what CREATE TABLE and CREATE INDEX may say, and the table each gives.

use super::{Catalog, RESERVED_PREFIXES, Table};
use crate::sql::ast::{self, DataType, Expr, Literal, UnaryOp};
use crate::sql::plan::{CreateIndexPlan, CreateTablePlan};
use crate::store::{IndexId, TableId};
use crate::types::{
    ColumnDef, ColumnDefault, ColumnType, Error, IndexDecl, Result, SortDirection, TableMeta, Value,
};

/// The refusal of a statement or clause that arrives with a later version.
pub(super) fn not_supported(clause: &str) -> Error {
    Error::PlanError(format!(
        "{clause} is not supported in this version of the library"
    ))
}

fn invalid(reason: String) -> Error {
    Error::SchemaInvalid { reason }
}

/// The table a CREATE TABLE declares, numbered after the tables the catalog holds.
pub(super) fn new_table(create: &CreateTablePlan, catalog: &Catalog) -> Result<Table> {
    let table = &create.name;
    let mut columns: Vec<ColumnDef> = Vec::with_capacity(create.columns.len());
    for declared in &create.columns {
        if columns.iter().any(|earlier| earlier.name == declared.name) {
            return Err(invalid(format!(
                "table '{table}' declares the column '{}' twice",
                declared.name
            )));
        }
        columns.push(column(table, declared)?);
    }
    match columns.iter().filter(|column| column.primary_key).count() {
        0 => return Err(not_supported("a table without a PRIMARY KEY")),
        1 => {}
        _ => {
            return Err(invalid(format!(
                "table '{table}' declares more than one PRIMARY KEY"
            )));
        }
    }

    let mut meta = TableMeta {
        columns,
        unique_constraints: create.unique_constraints.clone(),
        indexes: Vec::new(),
    };
    // The index behind the key, then behind each UNIQUE column, then behind each UNIQUE rule.
    // The key's index already keeps the key column unique.
    let one = |column: &ColumnDef| vec![column.name.clone()];
    let key = meta.columns.iter().filter(|column| column.primary_key);
    let unique = meta.columns.iter();
    let unique = unique.filter(|column| column.unique && !column.primary_key);
    let rules = create.unique_constraints.iter().cloned();
    let behind = key
        .map(|column| ("__pk_", one(column)))
        .chain(unique.map(|column| ("__unique_", one(column))))
        .chain(rules.map(|rule| ("__unique_", rule)));
    for (prefix, rule) in behind {
        // Two rules can spell one name (`a_b`, and `a` with `b`); the later one takes a number.
        let plain = format!("{prefix}{}", rule.join("_"));
        let mut name = plain.clone();
        let mut copy = 1;
        while meta.indexes.iter().any(|other| other.name == name) {
            copy += 1;
            name = format!("{plain}_{copy}");
        }
        let columns = rule.into_iter().map(|column| (column, SortDirection::Asc));
        let index = IndexDecl {
            name,
            columns: columns.collect(),
        };
        check_columns(table, &meta, &index, true)?;
        meta.indexes.push(index);
    }

    let id = catalog.tables.values().map(|other| other.id.0 + 1).max();
    let index_ids = (next_index_id(catalog)..)
        .take(meta.indexes.len())
        .map(IndexId)
        .collect();
    Ok(Table {
        id: TableId(id.unwrap_or(0)),
        name: table.clone(),
        meta,
        index_ids,
    })
}

/// The table a CREATE INDEX names, with the new index as its last.
pub(super) fn with_index(create: &CreateIndexPlan, catalog: &Catalog) -> Result<Table> {
    let reserved = RESERVED_PREFIXES
        .into_iter()
        .find(|prefix| create.name.starts_with(*prefix));
    if let Some(prefix) = reserved {
        return Err(Error::ReservedIndexName {
            table: create.table.clone(),
            name: create.name.clone(),
            prefix: prefix.to_owned(),
        });
    }
    let mut table = catalog
        .table(&create.table)
        .ok_or_else(|| Error::TableNotFound(create.table.clone()))?
        .clone();
    let index = IndexDecl {
        name: create.name.clone(),
        columns: create.columns.clone(),
    };
    check_columns(&table.name, &table.meta, &index, false)?;
    if table
        .meta
        .indexes
        .iter()
        .any(|other| other.name == index.name)
    {
        return Err(Error::DuplicateIndex {
            table: table.name,
            index: index.name,
        });
    }
    table.meta.indexes.push(index);
    table.index_ids.push(IndexId(next_index_id(catalog)));
    Ok(table)
}

/// One past the highest index number in use. Nothing in this version removes an index or a
/// table, so counting on from the highest never gives a number twice.
fn next_index_id(catalog: &Catalog) -> u32 {
    let ids = catalog.tables.values().flat_map(|table| &table.index_ids);
    ids.map(|id| id.0 + 1).max().unwrap_or(0)
}

/// Refuses an index over a column the table does not have or whose type it cannot be over.
/// `rule` says the index is the one behind a key or a UNIQUE rule.
fn check_columns(table: &str, meta: &TableMeta, index: &IndexDecl, rule: bool) -> Result<()> {
    for (name, _) in &index.columns {
        let column = meta.columns.iter().find(|column| column.name == *name);
        let column = column.ok_or_else(|| Error::ColumnNotFound {
            table: table.to_owned(),
            column: name.clone(),
        })?;
        // JSON and VECTOR have no order to keep an index by. A REAL has one, and is still no
        // identity for a key or a UNIQUE rule: it has two zeros and a NaN.
        let refused = match column.column_type {
            ColumnType::Json | ColumnType::Vector(_) => true,
            ColumnType::Real => rule,
            _ => false,
        };
        if refused {
            return Err(Error::ColumnNotIndexable {
                table: table.to_owned(),
                column: name.clone(),
                column_type: column.column_type,
            });
        }
    }
    Ok(())
}

/// One column as the catalog keeps it.
fn column(table: &str, declared: &ast::ColumnDef) -> Result<ColumnDef> {
    let column_type = match declared.data_type {
        DataType::Integer => ColumnType::Integer,
        DataType::Real => ColumnType::Real,
        DataType::Text => ColumnType::Text,
        DataType::Boolean => ColumnType::Boolean,
        DataType::Uuid => ColumnType::Uuid,
        DataType::Timestamp => ColumnType::Timestamp,
        DataType::Json => ColumnType::Json,
        DataType::Vector(size) => ColumnType::Vector(size as usize),
    };
    let default = match &declared.default {
        Some(expr) => Some(default(expr).ok_or_else(|| {
            invalid(format!(
                "the DEFAULT of '{table}.{}' must be NULL, TRUE, FALSE, a number, quoted text \
                 or NOW()",
                declared.name
            ))
        })?),
        None => None,
    };
    Ok(ColumnDef {
        name: declared.name.clone(),
        column_type,
        nullable: declared.nullable,
        primary_key: declared.primary_key,
        unique: declared.unique,
        default,
    })
}

/// A DEFAULT as the catalog keeps it, in the type the author wrote. `None` for anything but
/// NULL, TRUE, FALSE, a number, quoted text or `NOW()`.
pub(super) fn default(expr: &Expr) -> Option<ColumnDefault> {
    let value = match expr {
        Expr::FunctionCall { name, args }
            if name.eq_ignore_ascii_case("NOW") && args.is_empty() =>
        {
            return Some(ColumnDefault::Now);
        }
        Expr::Literal(Literal::Null) => Value::Null,
        Expr::Literal(Literal::Bool(flag)) => Value::Bool(*flag),
        Expr::Literal(Literal::Integer(whole)) => Value::Int64(*whole),
        Expr::Literal(Literal::Real(real)) => Value::Float64(*real),
        Expr::Literal(Literal::Text(words)) => Value::Text(words.clone()),
        Expr::UnaryOp {
            op: UnaryOp::Neg,
            operand,
        } => match **operand {
            Expr::Literal(Literal::Integer(whole)) => Value::Int64(-whole),
            Expr::Literal(Literal::Real(real)) => Value::Float64(-real),
            _ => return None,
        },
        _ => return None,
    };
    Some(ColumnDefault::Literal(value))
}
