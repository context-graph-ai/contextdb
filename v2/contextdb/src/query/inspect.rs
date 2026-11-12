//! Inspection of VECTOR columns: what `SHOW VECTOR_INDEXES` and `SHOW VECTOR_PARTITIONS` return
//! and what explaining a similarity search says. All three read the vector part's small records
//! and no block, no centre and no row, and none changes anything.

use std::cell::Cell;
use std::collections::HashMap;

use serde_json::json;

use super::search::Scope;
use super::{QueryResult, Reader};
use crate::memory::Budget;
use crate::schema::{Catalog, Consolidation, Quantization, Table, VectorColumn};
use crate::sql::{Plan, not_supported};
use crate::store::{Key, Read, VectorNumber};
use crate::types::{Error, Result, Value, VectorIndexRef, VectorSearchMode};
use crate::vector::{self, Inspected, Nearest, Route, State};

const INDEX_FIELDS: [&str; 31] = [
    "table",
    "column",
    "dimension",
    "quantization",
    "vector_count",
    "bytes",
    "partition_key_columns",
    "max_partitions",
    "live_partitions",
    "search_mode",
    "declared_auto_index_at",
    "effective_auto_index_at",
    "declared_consolidation_mode",
    "declared_consolidation_change_percent",
    "declared_consolidation_tombstone_percent",
    "effective_consolidation_mode",
    "effective_consolidation_change_percent",
    "effective_consolidation_tombstone_percent",
    "pending_inserts",
    "tombstones",
    "durable_vector_bytes",
    "durable_index_bytes",
    "indexed_available",
    "maintenance_state",
    "maintenance_vectors_total",
    "maintenance_vectors_done",
    "maintenance_vectors_remaining",
    "unavailable_partitions",
    "stalled_partitions",
    "broad_route",
    "recovery_action",
];

const PARTITION_FIELDS: [&str; 18] = [
    "table",
    "column",
    "partition_key",
    "live_rows",
    "pending_inserts",
    "tombstones",
    "durable_vector_bytes",
    "durable_index_bytes",
    "indexed_available",
    "availability_reason",
    "maintenance_state",
    "maintenance_reason",
    "maintenance_vectors_total",
    "maintenance_vectors_done",
    "maintenance_vectors_remaining",
    "lists",
    "probes",
    "recovery_action",
];

/// Answers `SHOW VECTOR_INDEXES` and `SHOW VECTOR_PARTITIONS`; `None` for any other plan.
///
/// `SHOW VECTOR_INDEXES` gives one row for each VECTOR column of the store, in the order of
/// table name and column; `SHOW VECTOR_PARTITIONS` one row for each partition, of the column
/// after `FOR` or of every column, in the order of table, column and key tuple, after `OFFSET`
/// rows and at most `LIMIT`.
pub(super) fn show(
    plan: &Plan,
    catalog: &Catalog,
    read: &dyn Read,
    budget: &Budget,
) -> Result<Option<QueryResult>> {
    // The partitions read are held until their rows are made.
    let held = &mut budget.reserve(0, "query")?;
    let (fields, rows) = match plan {
        Plan::ShowVectorIndexes => {
            let mut rows = Vec::new();
            for (table, column) in columns(catalog, None)? {
                let parts =
                    vector::partitions(read, column, table.dimension(column), 0, None, held)?;
                rows.push(index_row(table, column, &parts));
            }
            (&INDEX_FIELDS[..], rows)
        }
        Plan::ShowVectorPartitions {
            column,
            limit,
            offset,
        } => {
            let (mut skip, mut left, mut rows) = (*offset, *limit, Vec::new());
            for (table, column) in columns(catalog, column.as_ref())? {
                let n = table.dimension(column);
                let parts = vector::partitions(read, column, n, skip, left, held)?;
                // A column with no PARTITION_KEY always has its one partition.
                let has = read.vector_number(VectorNumber::Partitions(column.id))?;
                skip = skip.saturating_sub(has.max(u64::from(column.partition_key.is_empty())));
                left = left.map(|left| left - parts.len() as u64);
                let shown = parts.iter();
                rows.extend(shown.map(|part| partition_row(table, column, part)));
            }
            (&PARTITION_FIELDS[..], rows)
        }
        _ => return Ok(None),
    };
    let fields = fields.iter().map(|name| (*name).to_owned()).collect();
    Ok(Some(QueryResult::from_rows(fields, rows)))
}

/// The VECTOR column after `FOR`, or every VECTOR column in the order of table name and
/// column.
fn columns<'a>(
    catalog: &'a Catalog,
    named: Option<&(String, String)>,
) -> Result<Vec<(&'a Table, &'a VectorColumn)>> {
    if let Some((table, column)) = named {
        let table = catalog
            .table(table)
            .ok_or_else(|| Error::TableNotFound(table.clone()))?;
        let index = VectorIndexRef {
            table: table.name.clone(),
            column: column.clone(),
        };
        let column = table.vector(column);
        return Ok(vec![(
            table,
            column.ok_or(Error::UnknownVectorIndex { index })?,
        )]);
    }
    let mut found = Vec::new();
    for table in catalog.tables() {
        let mut vectors: Vec<_> = table.vectors.iter().map(|column| (table, column)).collect();
        vectors.sort_by(|left, right| left.1.column.cmp(&right.1.column));
        found.extend(vectors);
    }
    Ok(found)
}

fn whole(number: u64) -> Value {
    Value::Int64(i64::try_from(number).unwrap_or(i64::MAX))
}

fn text(word: &str) -> Value {
    Value::Text(word.to_owned())
}

fn mode_name(mode: VectorSearchMode) -> &'static str {
    match mode {
        VectorSearchMode::Auto => "AUTO",
        VectorSearchMode::Exact => "EXACT",
        VectorSearchMode::Indexed => "INDEXED",
    }
}

fn state_name(state: State) -> &'static str {
    match state {
        State::Idle => "idle",
        State::Pending => "pending",
        State::Building => "building",
        State::Stalled => "stalled",
    }
}

/// What brings a partition, or a column, out of this state.
fn recovery(state: State) -> &'static str {
    match state {
        State::Idle => "none",
        State::Pending | State::Building => "run a maintenance cycle",
        State::Stalled => "raise the memory limit",
    }
}

/// The names of the PARTITION_KEY columns as a JSON array.
fn key_columns(column: &VectorColumn) -> String {
    json!(column.partition_key).to_string()
}

/// A partition's key tuple as a JSON object of the key columns, in declared order, and their
/// values.
fn key_object(column: &VectorColumn, tuple: &Key) -> String {
    let values = tuple.values().unwrap_or_default();
    let members = column
        .partition_key
        .iter()
        .zip(&values)
        .map(|(name, value)| {
            let value = match value {
                Value::Bool(value) => json!(value),
                Value::Int64(value) | Value::Timestamp(value) => json!(value),
                Value::Float64(value) => json!(value),
                Value::Text(value) => json!(value),
                Value::Uuid(value) => json!(value.to_string()),
                _ => serde_json::Value::Null,
            };
            format!("{}:{value}", json!(name))
        });
    format!("{{{}}}", members.collect::<Vec<_>>().join(","))
}

/// The row of `SHOW VECTOR_INDEXES` for one column, from all its partitions.
fn index_row(table: &Table, column: &VectorColumn, parts: &[Inspected]) -> Vec<Value> {
    let sum = |of: fn(&Inspected) -> u64| parts.iter().map(of).sum::<u64>();
    let count = |of: fn(&Inspected) -> bool| parts.iter().filter(|part| of(part)).count() as u64;
    let optional = |number: Option<u64>| number.map_or(Value::Null, whole);
    let percent = |number: Option<u8>| optional(number.map(u64::from));
    let (declared, change, tombstone) = match column.consolidation {
        None => (Value::Null, None, None),
        Some(Consolidation::None) => (text("NONE"), None, None),
        Some(Consolidation::Percent { change, tombstone }) => (text("PERCENT"), change, tombstone),
    };
    let in_force = column.consolidation();
    let state = parts.iter().map(|part| part.state).max();
    let state = state.unwrap_or(State::Idle);
    let (slot_bytes, index_bytes) = (sum(|part| part.slot_bytes), sum(|part| part.index_bytes));
    let (total, done) = (sum(|part| part.total), sum(|part| part.done));
    let unavailable = count(|part| !part.usable);
    vec![
        text(&table.name),
        text(&column.column),
        whole(table.dimension(column) as u64),
        text(match column.quantization {
            Quantization::F32 => "F32",
            Quantization::Sq8 => "SQ8",
        }),
        whole(sum(|part| part.live)),
        whole(slot_bytes + index_bytes),
        text(&key_columns(column)),
        optional(column.limit().map(u64::from)),
        whole(count(|part| part.live > 0)),
        text(mode_name(column.mode())),
        optional(column.auto_index_at),
        whole(column.index_at()),
        declared,
        percent(change),
        percent(tombstone),
        text(in_force.map_or("NONE", |_| "PERCENT")),
        percent(in_force.map(|(change, _)| change)),
        percent(in_force.map(|(_, tombstone)| tombstone)),
        whole(sum(|part| part.pending)),
        whole(sum(|part| part.stale)),
        whole(slot_bytes),
        whole(index_bytes),
        Value::Bool(unavailable == 0),
        text(state_name(state)),
        whole(total),
        whole(done),
        whole(total - done),
        whole(unavailable),
        whole(count(|part| part.state == State::Stalled)),
        text("fan_out"),
        text(recovery(state)),
    ]
}

/// The row of `SHOW VECTOR_PARTITIONS` for one partition of a column.
fn partition_row(table: &Table, column: &VectorColumn, part: &Inspected) -> Vec<Value> {
    // Why the grouping is due or being built: the slots rows have left behind, when they
    // alone are enough; else what has changed since.
    let reason = match part.state {
        _ if part.damage.is_some() => "damaged",
        State::Idle => "none",
        State::Stalled => "memory_limit",
        _ if part.groups == 0 => "first_grouping",
        _ if part.left_behind => "tombstones",
        _ => "changes",
    };
    let grouping = |number: u32| match part.groups {
        0 => Value::Null,
        _ => whole(u64::from(number)),
    };
    vec![
        text(&table.name),
        text(&column.column),
        text(&key_object(column, &part.tuple)),
        whole(part.live),
        whole(part.pending),
        whole(part.stale),
        whole(part.slot_bytes),
        whole(part.index_bytes),
        Value::Bool(part.usable),
        text(if part.usable { "none" } else { "behind_writes" }),
        text(state_name(part.state)),
        text(reason),
        whole(part.total),
        whole(part.done),
        whole(part.total - part.done),
        grouping(part.groups),
        grouping(part.probes),
        text(recovery(part.state)),
    ]
}

/// What a statement would do, as text of one `name: value` to a line. For a similarity search:
/// the mode asked for and the one in force, the column's `AUTO_INDEX_AT`, how many partitions
/// the filter selects, and for each of them how it is searched; then what refuses the search,
/// or `none`. The search is not run. A partition named through a `$name` has no value here:
/// every partition is then listed, and the line of their count says so.
pub(crate) fn explain(
    plan: &Plan,
    catalog: &Catalog,
    read: &dyn Read,
    reader: &Reader,
    budget: &Budget,
    now: i64,
) -> Result<String> {
    let plan = match plan {
        Plan::Project { input, .. } => &**input,
        plan => plan,
    };
    let Plan::VectorSearch {
        table,
        column,
        k,
        filter,
        mode,
        ..
    } = plan
    else {
        return Err(not_supported("explaining this statement"));
    };
    let table = catalog
        .table(table)
        .ok_or_else(|| Error::TableNotFound(table.clone()))?;
    let index = VectorIndexRef {
        table: table.name.clone(),
        column: column.clone(),
    };
    let Some(vector) = table.vector(column) else {
        return Err(Error::UnknownVectorIndex { index });
    };
    let mode = mode.unwrap_or_else(|| vector.mode());
    let key = vector.partition_key.iter();
    let key = key.filter_map(|name| table.meta.columns.iter().position(|c| c.name == *name));
    let scope = Scope {
        table,
        key: key.collect(),
        params: &HashMap::new(),
        now,
        unbound: Cell::default(),
    };
    // A narrowed handle is told what the statement and the declaration say: no partition
    // record is read for it, as one names its Context's values and counts its vectors.
    if !reader.is_admin() {
        let lines = [
            format!("requested_mode: {}", mode_name(mode)),
            format!("effective_auto_index_at: {}", vector.index_at()),
            format!("partition_key_columns: {}", key_columns(vector)),
        ];
        return Ok(lines.join("\n"));
    }
    let mut keys = scope.keys(filter.as_ref())?;
    let every = scope.unbound.take();
    if every.is_some() {
        keys.clear();
    }
    // The list is let go with the text made from it.
    let mut reservation = budget.reserve(0, "query")?;
    let planned = vector::plan(
        &Nearest {
            table: table.id,
            vector: vector.id,
            column: vector,
            mode,
            scope: &keys,
            query: &[],
            k: usize::try_from(*k).unwrap_or(usize::MAX),
            // Whether rows are turned away is known only when the search runs.
            filtered: false,
        },
        read,
        &mut reservation,
    )?;

    let indexed = mode == VectorSearchMode::Indexed;
    let allowed: u64 = planned.iter().map(|partition| partition.live).sum();
    // AUTO over fewer vectors than the column's size is EXACT, as the search settles it.
    let resolved = match mode {
        VectorSearchMode::Auto if allowed < vector.index_at() => VectorSearchMode::Exact,
        mode => mode,
    };
    let mut lines = vec![
        format!("requested_mode: {}", mode_name(mode)),
        format!("resolved_mode: {}", mode_name(resolved)),
        format!("effective_auto_index_at: {}", vector.index_at()),
        format!("aggregate_allowed_vectors: {allowed}"),
        format!("partition_key_columns: {}", key_columns(vector)),
        match every {
            None => format!("partitions: {}", planned.len()),
            Some(name) => format!("partitions: every partition (${name} is not bound)"),
        },
    ];
    for partition in &planned {
        let route = match partition.route {
            Route::Exact => "exact",
            Route::Indexed => "indexed",
        };
        let grouping = |number: u32| match partition.groups {
            0 => "none".to_owned(),
            _ => number.to_string(),
        };
        lines.extend([
            format!("partition: {}", key_object(vector, &partition.tuple)),
            format!("  route: {route}"),
            format!("  lists: {}", grouping(partition.groups)),
            format!("  probes: {}", grouping(partition.probes)),
            format!("  live_rows: {}", partition.live),
            format!("  outside_grouping: {}", partition.tail + partition.pending),
        ]);
    }
    // A search under INDEXED with a partition that is not usable is refused whole.
    lines.push(
        if indexed && planned.iter().any(|partition| !partition.usable) {
            format!(
                "refusal: {}",
                Error::VectorIndexedRouteUnavailable { index }
            )
        } else {
            "refusal: none".to_owned()
        },
    );
    Ok(lines.join("\n"))
}
