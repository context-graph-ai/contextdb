//! Checking a declaration: what CREATE TABLE and CREATE INDEX may say, and the table each gives.

use super::{Catalog, Consolidation, Quantization, RESERVED_PREFIXES, Table, VectorColumn, rules};
use crate::sql::ast::{self, ConsolidationClause, Expr, Literal};
use crate::sql::not_carried;
use crate::sql::plan::{CreateIndexPlan, CreateTablePlan};
use crate::store::{IndexId, TableId, VectorId};
use crate::types::{
    ColumnDef, ColumnDefault, ColumnType, Error, IndexDecl, Result, SortDirection, TableMeta,
    Value, VectorIndexRef, VectorPartitionDeclarationIssue as PartitionIssue,
    VectorPolicyDeclarationIssue as PolicyIssue,
};

pub(super) fn invalid(reason: String) -> Error {
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
    // One key to a table: written with one column, or as an element of the table.
    let key = columns.iter().filter(|column| column.primary_key);
    let mut key: Vec<String> = key.map(|column| column.name.clone()).collect();
    if key.len() + usize::from(!create.primary_key.is_empty()) > 1 {
        return Err(invalid(format!(
            "table '{table}' declares more than one PRIMARY KEY: write one, over one column or \
             several"
        )));
    }
    key.extend(create.primary_key.iter().cloned());
    for column in columns
        .iter_mut()
        .filter(|column| key.contains(&column.name))
    {
        column.primary_key = true;
    }

    let options = &create.options;
    let mut meta = TableMeta {
        columns,
        unique_constraints: create.unique_constraints.clone(),
        indexes: Vec::new(),
        immutable: options.immutable,
        state_machine: options.state_machine.clone(),
        dag: options.dag.clone(),
        retain: options.retain.clone(),
        history: options.history,
        sync: options.sync,
        conflict: options.conflict,
        kept_counts: options.kept_counts.clone(),
    };
    // The index behind the key, then behind each UNIQUE column, then behind each UNIQUE rule.
    // The key's index already keeps the key column unique.
    let one = |column: &ColumnDef| vec![column.name.clone()];
    let unique = meta.columns.iter();
    let unique = unique.filter(|column| column.unique && !column.primary_key);
    let rules = create.unique_constraints.iter().cloned();
    let behind: Vec<(&str, Vec<String>)> = (!key.is_empty())
        .then(|| ("__pk_", key.clone()))
        .into_iter()
        .chain(unique.map(|column| ("__unique_", one(column))))
        .chain(rules.map(|rule| ("__unique_", rule)))
        .collect();
    for (prefix, rule) in behind {
        let name = format!("{prefix}{}", rule.join("_"));
        engine_index(table, &mut meta, name, rule, true)?;
    }
    let rules = rules::of(create, &mut meta, catalog)?;
    let position = |name: &String| meta.columns.iter().position(|column| column.name == *name);
    let key: Vec<usize> = key.iter().filter_map(position).collect();

    // Each VECTOR column is numbered on from the highest number in the catalog, as indexes are.
    let numbered = catalog.numbered().flat_map(|other| &other.vectors);
    let next_vector = numbered.map(|vector| vector.id.0 + 1).max().unwrap_or(0);
    let mut vectors = Vec::new();
    for declared in &create.columns {
        if matches!(declared.data_type, ColumnType::Vector(_)) {
            let id = VectorId(next_vector + vectors.len() as u32);
            vectors.push(vector_column(table, declared, &meta.columns, id)?);
        }
    }

    let id = catalog.numbered().map(|other| other.id.0 + 1).max();
    let index_ids = (next_index_id(catalog)..)
        .take(meta.indexes.len())
        .map(IndexId)
        .collect();
    Ok(Table {
        id: TableId(id.unwrap_or(0)),
        name: table.clone(),
        index_ids,
        vectors,
        key,
        key_is_element: !create.primary_key.is_empty(),
        missing: vec![Value::Null; meta.columns.len()],
        meta,
        rules,
        shapes: Vec::new().into(),
    })
}

/// Gives the table an index the engine makes for itself, under `plain` or, when a rule before
/// it spelt the same name (`a_b`, and `a` with `b`), under that name and a number.
pub(super) fn engine_index(
    table: &str,
    meta: &mut TableMeta,
    plain: String,
    columns: Vec<String>,
    rule: bool,
) -> Result<()> {
    let mut name = plain.clone();
    let mut copy = 1;
    while meta.indexes.iter().any(|other| other.name == name) {
        copy += 1;
        name = format!("{plain}_{copy}");
    }
    let columns = columns
        .into_iter()
        .map(|column| (column, SortDirection::Asc));
    let index = IndexDecl {
        name,
        columns: columns.collect(),
        json_path: Vec::new(),
    };
    check_columns(table, meta, &index, rule)?;
    meta.indexes.push(index);
    Ok(())
}

/// What the table keeps about the VECTOR column `declared`, or the refusal of a clause of it
/// that cannot stand. `columns` is every column of the table.
fn vector_column(
    table: &str,
    declared: &ast::ColumnDef,
    columns: &[ColumnDef],
    id: VectorId,
) -> Result<VectorColumn> {
    let index = VectorIndexRef {
        table: table.to_owned(),
        column: declared.name.clone(),
    };
    let policy = |issue| Error::InvalidVectorPolicyDeclaration {
        index: index.clone(),
        issue,
    };
    if declared.data_type == ColumnType::Vector(0) {
        return Err(policy(PolicyIssue::DimensionsNotPositive));
    }
    let max_partitions = partitions(declared, columns).map_err(|issue| {
        Error::InvalidVectorPartitionDeclaration {
            index: index.clone(),
            issue,
        }
    })?;
    let written = declared.vector.quantization.as_deref();
    let quantization = match written.map(str::to_ascii_uppercase).as_deref() {
        None | Some("F32") => Quantization::F32,
        Some("SQ8") => Quantization::Sq8,
        Some("SQ4") => {
            let instead = "write 'SQ8' to store one byte a component, or leave the clause out to \
                           store each vector as given";
            return Err(not_carried("WITH (quantization = 'SQ4')", instead));
        }
        Some(_) => {
            let member = "quantization";
            return Err(policy(PolicyIssue::InvalidMember { member }));
        }
    };
    let auto_index_at = match &declared.vector.auto_index_at {
        None => None,
        Some(written) => match written.parse::<u64>() {
            Ok(at) if at > 0 => Some(at),
            Err(_) if !written.starts_with('-') => {
                return Err(policy(PolicyIssue::AutoIndexAtOutOfRange));
            }
            _ => return Err(policy(PolicyIssue::AutoIndexAtNotPositive)),
        },
    };
    let percent = |written: &Option<String>| match written {
        None => Ok(None),
        Some(written) => match written.parse::<u8>() {
            Ok(percent @ 1..=100) => Ok(Some(percent)),
            _ => Err(policy(PolicyIssue::ConsolidationPercentOutOfRange)),
        },
    };
    let consolidation = match &declared.vector.consolidation {
        None => None,
        Some(ConsolidationClause::None) => Some(Consolidation::None),
        Some(ConsolidationClause::Percent { change, tombstone }) => Some(Consolidation::Percent {
            change: percent(change)?,
            tombstone: percent(tombstone)?,
        }),
    };
    Ok(VectorColumn {
        id,
        column: declared.name.clone(),
        partition_key: declared.vector.partition_key.clone(),
        max_partitions,
        search_mode: declared.vector.search_mode,
        auto_index_at,
        consolidation,
        quantization,
    })
}

/// Checks the PARTITION_KEY and the MAX_PARTITIONS of a VECTOR column and gives the limit it
/// declares, `None` when it declares none.
fn partitions(
    declared: &ast::ColumnDef,
    columns: &[ColumnDef],
) -> std::result::Result<Option<u32>, PartitionIssue> {
    let key = &declared.vector.partition_key;
    for (at, name) in key.iter().enumerate() {
        let column = columns.iter().find(|column| column.name == *name);
        let issue = match column {
            _ if key[..at].contains(name) => PartitionIssue::DuplicatePartitionKeyColumn,
            _ if *name == declared.name => PartitionIssue::VectorColumnInPartitionKey,
            None => PartitionIssue::UnknownPartitionKeyColumn,
            // Two values of a key column are one partition or they are not: a REAL has two
            // zeros and a NaN, and JSON and VECTOR values have no order to be keyed by.
            Some(ColumnDef {
                column_type: ColumnType::Real | ColumnType::Json | ColumnType::Vector(_),
                ..
            }) => PartitionIssue::UnsupportedPartitionKeyColumnType,
            // The primary key column holds no NULL, written NOT NULL or not.
            Some(column) if column.nullable && !column.primary_key => {
                PartitionIssue::NullablePartitionKeyColumn
            }
            Some(_) => continue,
        };
        return Err(issue);
    }
    let Some(written) = &declared.vector.max_partitions else {
        return Ok(None);
    };
    if key.is_empty() {
        return Err(PartitionIssue::MaxPartitionsWithoutPartitionKey);
    }
    match written.parse::<u32>() {
        Ok(limit) if limit > 0 => Ok(Some(limit)),
        Ok(_) => Err(PartitionIssue::MaxPartitionsNotPositive),
        Err(_) if written.starts_with('-') => Err(PartitionIssue::MaxPartitionsNotPositive),
        Err(_) => Err(PartitionIssue::MaxPartitionsOutOfRange),
    }
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
        json_path: create.json_path.clone(),
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
    // A table that keeps a count for every beginning of every index keeps the new index's.
    table.rules.counts = rules::counts(&table.name, &table.meta, &table.rules.counts, catalog)?;
    Ok(table)
}

/// One past the highest index number in use.
fn next_index_id(catalog: &Catalog) -> u32 {
    let ids = catalog.numbered().flat_map(|table| &table.index_ids);
    ids.map(|id| id.0 + 1).max().unwrap_or(0)
}

/// Refuses an index over a column the table does not have or whose type it cannot be over.
/// `rule` says the index is the one behind a key or a UNIQUE rule.
fn check_columns(table: &str, meta: &TableMeta, index: &IndexDecl, rule: bool) -> Result<()> {
    if index.columns.len() > 32 {
        return Err(invalid(format!(
            "table '{table}': '{}' names {} columns, and a key, a UNIQUE rule or an index names \
             at most 32; name fewer",
            index.name,
            index.columns.len()
        )));
    }
    for (name, _) in &index.columns {
        let column = meta.columns.iter().find(|column| column.name == *name);
        let column = column.ok_or_else(|| Error::ColumnNotFound {
            table: table.to_owned(),
            column: name.clone(),
        })?;
        // JSON and VECTOR have no order to keep an index by; a path into a JSON column has,
        // and only a JSON column has a path. A REAL has an order, and is still no identity for
        // a key or a UNIQUE rule: it has two zeros and a NaN.
        let refused = match column.column_type {
            ColumnType::Json => index.json_path.is_empty(),
            ColumnType::Vector(_) => true,
            ColumnType::Real => rule,
            _ => !index.json_path.is_empty(),
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
    let column = ColumnDef {
        name: declared.name.clone(),
        column_type: declared.data_type,
        nullable: declared.nullable,
        primary_key: declared.primary_key,
        unique: declared.unique,
        default,
    };
    // A default that a row could not hold in the column is refused here, not at the first row.
    if let Some(ColumnDefault::Literal(value)) = &column.default {
        crate::types::typed(table, &column, value.clone())?;
    }
    Ok(column)
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
        _ => return None,
    };
    Some(ColumnDefault::Literal(value))
}
