//! One dispatcher for declared settings and table-driven reports.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeSet, HashMap};

use serde_json::json;

use super::expression::EvalContext;
use super::plan::{self, Given};
use super::search::{self, Scope, Search};
use super::{QueryResult, Reader, Visible};
use super::{filter, subquery, table};
use crate::memory::{Budget, Reservation};
use crate::schema::{Catalog, Compaction, Quantization, Table, VectorColumn};
use crate::sql::ast::Show;
use crate::sql::plan::REPORTS;
use crate::sql::{Expr, Plan, not_supported};
use crate::store::{Key, Read, VectorNumber};
use crate::types::{Error, Maintenance, Result, Value, VectorIndexRef, VectorSearchMode};
use crate::vector::{self, Inspected, Nearest, Route, State};

const INDEX_FIELDS: [&str; 32] = [
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
    "declared_indexing_threshold",
    "effective_indexing_threshold",
    "declared_probes",
    "declared_compaction_mode",
    "declared_compaction_change_percent",
    "declared_compaction_tombstone_percent",
    "effective_compaction_mode",
    "effective_compaction_change_percent",
    "effective_compaction_tombstone_percent",
    "pending_tuples",
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

const PART_FIELDS: [&str; 18] = [
    "table",
    "column",
    "partition_key",
    "live_rows",
    "pending_tuples",
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

const MEMORY_FIELDS: [&str; 5] = [
    "limit",
    "used",
    "available",
    "startup_ceiling",
    "ceiling_refresh_failures",
];
const SHARE_FIELDS: [&str; 5] = [
    "declared",
    "default_percent",
    "effective_percent",
    "effective_bytes",
    "budget_bytes",
];
const RUN_FIELDS: [&str; 9] = [
    "statement",
    "calls",
    "rows",
    "rows_examined",
    "distance_evaluations",
    "most_rows",
    "most_rows_examined",
    "most_distance_evaluations",
    "unused_index",
];
const SIZE_FIELDS: [&str; 8] = [
    "table",
    "rows",
    "large_values",
    "indexes",
    "vectors",
    "blocks",
    "versions",
    "total",
];

pub(crate) struct Source<'a> {
    catalog: &'a Catalog,
    statements: Option<&'a super::Statements>,
    read: &'a dyn Read,
    allowed: Option<&'a BTreeSet<String>>,
    budget: &'a Budget,
    held: &'a mut Reservation,
    wait: &'a str,
    limits: Option<&'a super::ReadLimits>,
    verify: Option<super::Verify>,
}
pub(crate) type Push<'a> = &'a mut dyn FnMut(Vec<Value>) -> Result<()>;
type Fill = fn(&Show, &mut Source<'_>, &mut dyn FnMut(Vec<Value>) -> Result<()>) -> Result<()>;
/// The fields and the filling of each report, in the order of [`REPORTS`].
const FILLS: [(&[&str], Fill); 5] = [
    (&INDEX_FIELDS, indexes),
    (&PART_FIELDS, parts),
    (&SIZE_FIELDS, content),
    (&RUN_FIELDS, runs),
    (&SHARE_FIELDS, share),
];

pub(super) fn show(
    plan: &Plan,
    catalog: &Catalog,
    read: &dyn Read,
    (budget, work, figures, parsing): (
        &Budget,
        Option<&super::Work<'_>>,
        Option<&super::Figures<'_>>,
        u64,
    ),
    allowed: Option<&BTreeSet<String>>,
    verify: Option<super::Verify>,
) -> Result<Option<QueryResult>> {
    // The partitions read are held until their rows are made.
    let held = &mut budget.reserve(0, "query")?;
    let mut assembled = budget.reserve(0, "materialize_answer")?;
    let setting = matches!(plan, Plan::ShowSetting(_));
    let mut push = |row: Vec<Value>, rows: &mut Vec<Vec<Value>>| -> Result<()> {
        super::result_row(&row, (!setting).then_some(&mut assembled), work)?;
        if setting {
            rows.push(row);
            Ok(())
        } else {
            assembled.room(rows, usize::MAX)?;
            rows.push(row);
            Ok(())
        }
    };
    let (fields, rows) = match plan {
        Plan::ShowSetting(selected) => {
            let current = figures.map(|figures| (figures.settings)()).transpose()?;
            let (read, figures) = match current.as_ref() {
                Some((view, figures)) => (view as &dyn Read, Some(*figures)),
                None => (read, None),
            };
            use crate::settings::{Kind, SETTINGS, limit};
            let (at, mut rows) = (selected.start, Vec::new());
            let stored = read.setting(at)?;
            let value = stored.unwrap_or(SETTINGS[at].2);
            let bytes = |n: Option<u64>| n.map_or(text("none"), whole);
            let fields = match SETTINGS[at].1 {
                Kind::Bytes => {
                    let handle = budget.handle();
                    let memory = at == 0;
                    let (limit, used, startup) = if memory {
                        (
                            handle.limit(),
                            handle.used().saturating_sub(parsing),
                            figures.map_or(handle.limit(), |f| f[0]),
                        )
                    } else {
                        let disk = figures.map_or(limit(value), |f| f[2]);
                        (
                            disk,
                            read.committed().file_bytes()?,
                            figures.map_or(disk, |f| f[1]),
                        )
                    };
                    let available = if !memory && limit.is_none() {
                        Value::Null
                    } else {
                        bytes(limit.map(|n| n.saturating_sub(used)))
                    };
                    // A session opened with no option has no starting ceiling
                    let startup = match startup {
                        None if memory => Value::Null,
                        startup => bytes(startup),
                    };
                    let mut row = vec![bytes(limit), whole(used), available, startup];
                    if memory {
                        row.push(whole(0));
                    }
                    push(row, &mut rows)?;
                    &MEMORY_FIELDS[..if memory { 5 } else { 4 }]
                }
                Kind::Ratio(_) => {
                    for i in selected.clone() {
                        let n = read.setting(i)?;
                        let value = n.unwrap_or(SETTINGS[i].2);
                        let label = SETTINGS[i].0.rsplit(' ').next().unwrap();
                        push(
                            vec![
                                text(&label.to_ascii_lowercase()),
                                if matches!(SETTINGS[i].1, Kind::Seconds) {
                                    whole(value)
                                } else {
                                    Value::Float64(f64::from_bits(value))
                                },
                                text(if n.is_some() { "declared" } else { "default" }),
                            ],
                            &mut rows,
                        )?;
                    }
                    &["setting", "value", "source"][..]
                }
                Kind::Seconds | Kind::Count | Kind::Many => {
                    let value = if matches!(SETTINGS[at].1, Kind::Count) && stored.is_none() {
                        text("block")
                    } else {
                        whole(value)
                    };
                    push(vec![value], &mut rows)?;
                    if matches!(SETTINGS[at].1, Kind::Seconds) {
                        &["milliseconds"][..]
                    } else {
                        &["limit"][..]
                    }
                }
            };
            (fields, rows)
        }

        Plan::ShowReport(show) => {
            let row = &REPORTS[show.report];
            if !row.2 && (show.column.is_some() || show.limit.is_some() || show.offset != 0) {
                return Err(Error::ParseError("this report accepts no options".into()));
            }
            let mut rows = Vec::new();
            let from = &mut Source {
                catalog,
                statements: work.and_then(|work| work.statements),
                read,
                allowed,
                budget,
                held,
                limits: work
                    .filter(|work| work.pages.is_some())
                    .map(|work| work.limits),
                verify,
                wait: match figures.map(|figures| figures.maintenance) {
                    Some(Maintenance::Engine) => "wait for the engine's cycle",
                    Some(_) => "run a maintenance cycle",
                    None => "wait for the engine's cycle, or run one if you drive them",
                },
            };
            let (fields, fill) = FILLS[show.report];
            fill(show, from, &mut |row| push(row, &mut rows))?;
            (fields, rows)
        }
        _ => return Ok(None),
    };
    let fields = fields.iter().map(|name| (*name).to_owned()).collect();
    let mut result = QueryResult::answer(fields, rows, assembled);
    result.report =
        matches!(plan, Plan::ShowReport(show) if REPORTS[show.report].0 != "SHOW CONTENT_SIZE");
    Ok(Some(result))
}

fn indexes(_: &Show, from: &mut Source<'_>, push: Push<'_>) -> Result<()> {
    let (read, held) = (from.read, &mut *from.held);
    for (table, column) in columns(from.catalog, None, from.allowed)? {
        let parts = vector::partitions(read, column, table.dimension(column), 0, None, held)?;
        push(index_row(table, column, &parts, from.wait))?;
    }
    Ok(())
}
fn parts(show: &Show, from: &mut Source<'_>, push: Push<'_>) -> Result<()> {
    let (read, held) = (from.read, &mut *from.held);
    let (mut skip, mut left) = (show.offset, show.limit);
    for (table, column) in columns(from.catalog, show.column.as_ref(), from.allowed)? {
        let parts = vector::partitions(read, column, table.dimension(column), skip, left, held)?;
        let has = read.vector_number(VectorNumber::Partitions(column.id))?;
        skip = skip.saturating_sub(has.max(u64::from(column.partition_key.is_empty())));
        left = left.map(|left| left - parts.len() as u64);
        for part in &parts {
            push(partition_row(table, column, part, from.wait))?;
        }
    }
    Ok(())
}
fn share(_: &Show, from: &mut Source<'_>, push: Push<'_>) -> Result<()> {
    let Some(l) = from.limits else {
        return push(vec![Value::Null; 5]);
    };
    let (share, limit) = (l.cache_share, Some(l.memory));
    let cache = |share| crate::store::cache_under(limit, share);
    let percent = |bytes: u64| bytes * 100 / l.memory.max(1);
    push(vec![
        share.map_or(Value::Null, whole),
        whole(percent(cache(None))),
        whole(percent(cache(share))),
        whole(cache(share)),
        whole(l.memory),
    ])
}
fn runs(show: &Show, from: &mut Source<'_>, push: Push<'_>) -> Result<()> {
    from.statements.map_or(Ok(()), |kept| kept.rows(show, push))
}
fn content(_: &Show, from: &mut Source<'_>, push: Push<'_>) -> Result<()> {
    // Every handle's statement and cursor page gives the check.
    let Some(verify) = from.verify else {
        return Ok(());
    };
    let view = from.read.committed();
    let report = verify(view, &Catalog::load(view, from.budget)?, from.budget)?;
    for t in &report.tables {
        let values = [
            t.rows,
            t.large_values,
            t.indexes,
            t.vectors,
            t.blocks,
            t.versions,
        ];
        push(
            [text(&t.table)]
                .into_iter()
                .chain(values.into_iter().chain([values.iter().sum()]).map(whole))
                .collect(),
        )?;
    }
    Ok(())
}

/// The VECTOR column after `FOR`, or every VECTOR column in the order of table name and
/// column.
fn columns<'a>(
    catalog: &'a Catalog,
    named: Option<&(String, String)>,
    allowed: Option<&BTreeSet<String>>,
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
    for table in catalog
        .tables()
        .into_iter()
        .filter(|table| allowed.is_none_or(|names| names.contains(&table.name)))
    {
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
        State::Stalled | State::Full => "stalled",
    }
}

/// What brings a partition, or a column, out of this state.
fn recovery(state: State, wait: &str) -> &str {
    match state {
        State::Idle => "none",
        State::Pending | State::Building => wait,
        State::Stalled => "raise the memory limit",
        State::Full => "raise the disk limit or free space",
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
fn index_row(table: &Table, column: &VectorColumn, parts: &[Inspected], wait: &str) -> Vec<Value> {
    let sum = |of: fn(&Inspected) -> u64| parts.iter().map(of).sum::<u64>();
    let count = |of: fn(&Inspected) -> bool| parts.iter().filter(|part| of(part)).count() as u64;
    let optional = |number: Option<u64>| number.map_or(Value::Null, whole);
    let percent = |number: Option<u8>| optional(number.map(u64::from));
    let (declared, change, tombstone) = match column.compaction {
        None => (Value::Null, None, None),
        Some(Compaction::None) => (text("NONE"), None, None),
        Some(Compaction::Percent { change, tombstone }) => (text("PERCENT"), change, tombstone),
    };
    let in_force = column.compaction();
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
        optional(column.indexing_threshold),
        whole(column.index_at()),
        optional(column.probes.map(u64::from)),
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
        whole(count(|part| part.state >= State::Stalled)),
        text("fan_out"),
        text(recovery(state, wait)),
    ]
}

/// The row of `SHOW VECTOR_PARTITIONS` for one partition of a column.
fn partition_row(table: &Table, column: &VectorColumn, part: &Inspected, wait: &str) -> Vec<Value> {
    // Why the IVF is due or being built: the slots rows have left behind, when they
    // alone are enough; else what has changed since.
    let reason = match part.state {
        _ if part.damage => "damaged",
        State::Idle => "none",
        State::Stalled => "memory_limit",
        State::Full => "disk_limit",
        _ if part.groups == 0 => "unindexed",
        _ if part.left_behind => "tombstones",
        _ => "changes",
    };
    let ivf = |number: u32| match part.groups {
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
        ivf(part.groups),
        ivf(part.probes),
        text(recovery(part.state, wait)),
    ]
}

/// What a statement would do, as text of one `name: value` to a line, made ready as running
/// it makes it with `params` and no row read. For a similarity search: the mode asked for and
/// the one in force, the column's `INDEXING_THRESHOLD`, how many partitions the filter selects, and
/// for each of them how it is searched; the path of the filter; then what refuses the search,
/// or `none`. A partition named through a `$name` with no value is not known: every partition
/// is then listed, and the line of their count says so. A narrowed handle is told no count.
/// For any other statement: the path each table is read by, and each count read from a kept
/// number.
pub(crate) fn explain(
    plan: &Plan,
    params: &HashMap<String, Value>,
    catalog: &Catalog,
    read: &dyn Read,
    reader: &Reader,
    budget: &Budget,
    (now, work, parsing): (i64, Option<&super::Work<'_>>, u64),
) -> Result<crate::Owned<String>> {
    let named = subquery::tables_read(plan, Some(catalog), None)?;
    let bytes: u64 = named
        .iter()
        .filter_map(|n| catalog.table(n))
        .map(|t| t.heap)
        .sum();
    let held = RefCell::new(budget.reserve(3 * (parsing + bytes), "explanation")?);
    let said = RefCell::new(Vec::new());
    let kept = super::Kept::new(budget.passing("expression evaluation")?);
    let mut ctx = EvalContext::new(params, now, &kept);
    (ctx.said, ctx.explained, ctx.held) = (Some(&said), Some(plan), Some(&held));
    let given = Given {
        allowed: work.and_then(|work| work.limits.tables.as_ref()),
        params,
        catalog,
        read,
        reader,
        budget,
        ctx,
        evaluations: &Cell::default(),
        examined: None,
        named: None,
        bound: None,
    };
    let mut seeing = budget.reserve(0, "query")?;
    match plan {
        Plan::Select(select) => {
            drop(plan::select(select, &given, &mut seeing)?);
        }
        Plan::Insert(insert) => {
            let table = catalog.table(&insert.table);
            let table = table.ok_or_else(|| Error::TableNotFound(insert.table.clone()))?;
            table.insert(insert)?;
            said.borrow_mut()
                .push(format!("Insert(table={})", table.name));
        }
        Plan::Delete(delete) => path(&given, &delete.table, delete.where_clause.as_ref(), &[])?,
        Plan::Update(update) => {
            let filter = update.where_clause.as_ref();
            path(&given, &update.table, filter, &update.assignments)?;
        }
        _ => {}
    }
    subquery::written(plan, &given, &mut seeing)?;
    let said = said.take();
    if said.is_empty() {
        return Err(not_supported("explaining this statement"));
    }
    let mut held = held.into_inner();
    let text = said.join("\n");
    drop(said);
    held.shrink(held.bytes().saturating_sub(text.capacity() as u64));
    Ok(crate::Owned::charged(text, held))
}

/// Says the path the rows of the table `name` that pass `filter` are read by, with no index
/// over a column `set` assigns walked.
fn path(given: &Given, name: &str, filter: Option<&Expr>, set: &[(String, Expr)]) -> Result<()> {
    let table = given.catalog.table(name);
    let table = table.ok_or_else(|| Error::TableNotFound(name.to_owned()))?;
    let set = table.targets(set.iter().map(|(name, _)| name))?;
    let mut seeing = given.budget.reserve(0, "query")?;
    let visible = Visible::new(table, given.catalog, given.reader, &mut seeing)?;
    let source = table::Source::new(table, filter, &given.ctx, given.read, visible);
    source.say(&[], &[], &set)
}

/// The explanation of a similarity search, as it is planned.
pub(super) fn searched(search: &Search) -> Result<String> {
    let (table, vector, select, given) = (search.table, search.vector, search.select, search.given);
    let further = usize::from(select.order_by.len() > 1);
    let k = search.most.saturating_add(further);
    let (mode, reader) = (&select.use_vector, given.reader);
    let (bound, filter) = search::bound(select, &given.ctx);
    let mode = mode.unwrap_or_else(|| vector.mode());
    let scope = Scope::new(table, search.qualifier, vector, given);
    // A narrowed handle is told nothing the state of the index decides: no partition is planned.
    let narrowed = !reader.is_admin();
    let mut lines = vec![
        format!("requested_mode: {}", mode_name(mode)),
        format!("effective_indexing_threshold: {}", vector.index_at()),
        format!("partition_key_columns: {}", key_columns(vector)),
    ];
    if let Some(n) = vector.probes {
        lines.push(format!("effective_probes: {n} (declared)"));
    }
    if let Some((most, equal)) = bound {
        let op = if equal { "<=" } else { "<" };
        lines.push(format!("distance_bound: {op} {most}"));
    }
    let (keys, on_rows) = scope.keys(filter.as_ref())?;
    let matching = |tuple: &Key| scope.matches(tuple);
    let every = scope.unbound.take().pop();
    // The list is let go with the text made from it; a reader with no partition plans none.
    let mut reservation = given.budget.reserve(0, "query")?;
    let planned = match &keys {
        Some(scope) if !narrowed => {
            let nearest = Nearest {
                count: None,
                table: table.id,
                vector: vector.id,
                column: vector,
                mode,
                scope,
                matching: Some(&matching),
                query: &[],
                k,
                ranked: false,
                bound: None,
                filtered: on_rows,
            };
            vector::plan(&nearest, given.read, &mut reservation)?
        }
        _ => Vec::new(),
    };

    let names = vector.partition_key.iter().map(String::len).sum::<usize>();
    let fixed = 128 * vector.partition_key.len() + 6 * (64 + size_of::<String>());
    let bytes = planned
        .iter()
        .map(|p| 3 * (6 * (p.tuple.as_bytes().len() + names) + fixed) as u64)
        .sum();
    let held = given.ctx.held.expect("explanation reservation");
    held.borrow_mut().grow(bytes)?;
    let allowed: u64 = planned.iter().map(|partition| partition.live).sum();
    // AUTO over fewer vectors than the column's size is EXACT, as the search settles it.
    let resolved = match mode {
        VectorSearchMode::Auto if allowed < vector.index_at() => VectorSearchMode::Exact,
        mode => mode,
    };
    // A narrowed handle is told no count of vectors or partitions.
    if !narrowed {
        lines.insert(1, format!("resolved_mode: {}", mode_name(resolved)));
        lines.insert(3, format!("aggregate_allowed_vectors: {allowed}"));
        lines.push(match &every {
            None => format!("partitions: {}", planned.len()),
            Some(name) => format!("partitions: every partition (${name} is not bound)"),
        });
    }
    let mut seeing = given.budget.reserve(0, "query")?;
    let visible = Visible::new(table, given.catalog, reader, &mut seeing)?;
    let source = table::Source::new(table, filter.as_ref(), &given.ctx, given.read, visible);
    let rest = filter.as_ref().filter(|_| on_rows);
    let how = (mode, k, search.rank.is_some());
    let decided = search::decide(&source, (table, &scope.key, rest), &planned, how)?;
    let refused = search::refusal(mode, &planned, &decided, table.vector_ref(vector));
    for (partition, decided) in planned.iter().zip(&decided) {
        let route = match partition.route {
            Route::Exact => "exact",
            Route::Indexed => "indexed",
        };
        let ivf = |number: u32| match partition.groups {
            0 => "none".to_owned(),
            _ => number.to_string(),
        };
        let key = key_object(vector, &partition.tuple);
        lines.extend([
            format!("partition: {key}"),
            format!("  route: {route}"),
            format!("  lists: {}", ivf(partition.groups)),
            format!("  probes: {}", ivf(partition.probes)),
            format!("  live_rows: {}", partition.live),
            format!("  unindexed_rows: {}", partition.tail + partition.pending),
        ]);
        // A refusal made when planning has no walk and no cost to print.
        if rest.is_none() || refused.is_some() {
            continue;
        }
        let way = if decided.direct { "direct" } else { "walk" };
        lines.push(format!("  filter_route: {way}"));
        if decided.path.is_some() {
            let (m, crossing) = (decided.m, decided.crossing);
            let m = if m > crossing {
                format!("more than {crossing}")
            } else {
                m.to_string()
            };
            lines.push(format!("  direct_cost: {m}"));
            lines.push(format!("  walk_cost: {}", decided.walk));
        }
    }
    // The path the filter is read by, as its table source says it, when every name has a value.
    if filter.is_some() && every.is_none() {
        source.say(&[], &[], &[])?;
        let said = given.ctx.said.map(|said| said.take()).unwrap_or_default();
        let mut said = said.iter().map(|line| line.trim_start());
        let path = said.next().filter(|path| *path != "Scan");
        lines.push(format!("filter_path: {}", path.unwrap_or("none")));
        lines.extend(said.map(str::to_owned));
        // For a filter of one case that is not countable for a selected partition: the index
        // that would be, by the partition's columns, the columns the case gives values, then
        // one it gives a range.
        let (columns, cases) = (&table.meta.columns, &source.facts()?.cases);
        let case = cases.iter().flatten().filter(|_| cases.len() == 1);
        let (values, ranges): (Vec<_>, Vec<_>) = case
            .filter(|(at, _)| *at < columns.len() && !scope.key.contains(at))
            .partition(|(_, fact)| matches!(fact, filter::Fact::Among { .. }));
        let wanted = values.iter().chain(ranges.first()).map(|(at, _)| at);
        let wanted = scope.key.iter().chain(wanted).map(|at| &columns[*at].name);
        let wanted: Vec<&str> = wanted.map(String::as_str).collect();
        let uncountable = rest.is_some() && decided.iter().any(|d| d.path.is_none());
        if (uncountable || planned.is_empty() && path.is_none()) && !wanted.is_empty() {
            let (name, list) = (&table.name, wanted.join(", "));
            let index = format!("{name}_{}_idx", wanted.join("_"));
            lines.push(format!("recovery: CREATE INDEX {index} ON {name} ({list})"));
        }
    }
    let refused = refused.map(|refused| refused.to_string());
    let refused = refused.as_deref().unwrap_or("none");
    lines.extend((!narrowed && keys.is_some()).then(|| format!("refusal: {refused}")));
    Ok(lines.join("\n"))
}
