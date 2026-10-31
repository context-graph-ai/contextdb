use super::*;

pub(super) fn strip_internal_row_id(mut qr: OwnedQueryResult) -> OwnedQueryResult {
    if let Some(pos) = qr.columns.iter().position(|c| c == "row_id") {
        qr.columns.remove(pos);
        for row in &mut qr.rows {
            if pos < row.len() {
                row.remove(pos);
            }
        }
    }
    qr
}

pub(super) fn current_write_set_counts(ws: &WriteSet) -> WriteSetCounts {
    WriteSetCounts {
        relational_inserts: ws.relational_inserts.len(),
        relational_deletes: ws.relational_deletes.len(),
        adj_inserts: ws.adj_inserts.len(),
        adj_deletes: ws.adj_deletes.len(),
        row_adj_inserts: ws.row_adj_inserts.len(),
        row_adj_deletes: ws.row_adj_deletes.len(),
        vector_inserts: ws.vector_inserts.len(),
        vector_deletes: ws.vector_deletes.len(),
        vector_moves: ws.vector_moves.len(),
    }
}

pub(super) fn write_set_touches_vector_search(ws: &WriteSet, index: &VectorIndexRef) -> bool {
    ws.vector_inserts.iter().any(|entry| entry.index == *index)
        || ws
            .vector_deletes
            .iter()
            .any(|(delete_index, _, _)| delete_index == index)
        || ws
            .vector_moves
            .iter()
            .any(|(move_index, _, _, _)| move_index == index)
        || ws
            .relational_deletes
            .iter()
            .any(|(delete_table, _, _)| delete_table == &index.table)
}

pub(super) fn estimate_active_tx_vector_overlay_bytes(
    entry_count: usize,
    dimension: usize,
) -> usize {
    let decoded_vector_bytes = dimension.saturating_mul(std::mem::size_of::<f32>());
    let per_entry = std::mem::size_of::<VectorEntry>()
        .saturating_add(std::mem::size_of::<(RowId, Vec<f32>)>())
        .saturating_add(decoded_vector_bytes)
        .saturating_add(64);
    entry_count.saturating_mul(per_entry)
}

// Row-stamp clock. Delegates to the canonical `Wallclock::now()` so the core
// test-clock seam covers every `created_at` stamp. Semantic delta vs the old
// inline read, reachable only with a pre-Unix-epoch system clock: this panics
// (`unwrap`) where the old read stamped 0 (`unwrap_or_default`) — deliberate,
// because a 0 stamp makes every TTL row instantly ancient and silently pruned.
pub(super) fn current_wallclock() -> Wallclock {
    Wallclock::now()
}

pub(super) fn remove_write_set_slice(
    ws: &mut WriteSet,
    before: WriteSetCounts,
    after: WriteSetCounts,
) {
    fn drain_range<T>(items: &mut Vec<T>, start: usize, end: usize) {
        if start >= items.len() {
            return;
        }
        let end = end.min(items.len());
        if start < end {
            items.drain(start..end);
        }
    }

    drain_range(
        &mut ws.relational_inserts,
        before.relational_inserts,
        after.relational_inserts,
    );
    drain_range(
        &mut ws.relational_deletes,
        before.relational_deletes,
        after.relational_deletes,
    );
    drain_range(&mut ws.adj_inserts, before.adj_inserts, after.adj_inserts);
    drain_range(&mut ws.adj_deletes, before.adj_deletes, after.adj_deletes);
    drain_range(
        &mut ws.row_adj_inserts,
        before.row_adj_inserts,
        after.row_adj_inserts,
    );
    drain_range(
        &mut ws.row_adj_deletes,
        before.row_adj_deletes,
        after.row_adj_deletes,
    );
    drain_range(
        &mut ws.vector_inserts,
        before.vector_inserts,
        after.vector_inserts,
    );
    drain_range(
        &mut ws.vector_deletes,
        before.vector_deletes,
        after.vector_deletes,
    );
    drain_range(
        &mut ws.vector_moves,
        before.vector_moves,
        after.vector_moves,
    );
}

pub(super) fn cached_table_meta(
    db: &Database,
    cache: &mut HashMap<String, Option<TableMeta>>,
    table: &str,
) -> Option<TableMeta> {
    cache
        .entry(table.to_string())
        .or_insert_with(|| db.table_meta(table))
        .clone()
}

pub(crate) fn rank_index_name(table: &str, column: &str) -> String {
    format!("{table}.{column}")
}

pub(super) fn bounded_rank_clone_size_error(operation: &str) -> Error {
    Error::Other(
        (format!("bounded {operation} memory size exceeds the native address space")).into(),
    )
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn bounded_rank_clone_checked_add(
    left: usize,
    right: usize,
    operation: &str,
) -> std::result::Result<usize, Error> {
    left.checked_add(right)
        .ok_or_else(|| bounded_rank_clone_size_error(operation))
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn bounded_rank_clone_checked_mul(
    left: usize,
    right: usize,
    operation: &str,
) -> std::result::Result<usize, Error> {
    left.checked_mul(right)
        .ok_or_else(|| bounded_rank_clone_size_error(operation))
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn bounded_rank_value_clone_planned_bytes(
    value: &Value,
) -> std::result::Result<usize, Error> {
    match value {
        Value::Text(value) => Ok(value.len()),
        Value::Vector(values) => bounded_rank_clone_checked_mul(
            values.len(),
            std::mem::size_of::<f32>(),
            "rank joined vector clone",
        ),
        Value::Json(_) => Ok(0),
        Value::Null
        | Value::Bool(_)
        | Value::Int64(_)
        | Value::Float64(_)
        | Value::Uuid(_)
        | Value::Timestamp(_)
        | Value::TxId(_) => Ok(0),
    }
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn bounded_rank_value_retained_bytes(
    value: &Value,
) -> std::result::Result<usize, Error> {
    match value {
        Value::Text(value) => Ok(value.capacity()),
        Value::Vector(values) => bounded_rank_clone_checked_mul(
            values.capacity(),
            std::mem::size_of::<f32>(),
            "rank joined vector clone",
        ),
        Value::Json(_) => Ok(0),
        Value::Null
        | Value::Bool(_)
        | Value::Int64(_)
        | Value::Float64(_)
        | Value::Uuid(_)
        | Value::Timestamp(_)
        | Value::TxId(_) => Ok(0),
    }
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn bounded_rank_row_clone_planned_bytes(
    row: &VersionedRow,
) -> std::result::Result<usize, Error> {
    let mut bytes = bounded_rank_clone_checked_add(
        std::mem::size_of::<VersionedRow>(),
        bounded_rank_clone_checked_mul(
            row.values.len(),
            std::mem::size_of::<(String, Value)>(),
            "rank joined row clone",
        )?,
        "rank joined row clone",
    )?;
    for (column, value) in &row.values {
        bytes = bounded_rank_clone_checked_add(bytes, column.len(), "rank joined row clone")?;
        bytes = bounded_rank_clone_checked_add(
            bytes,
            bounded_rank_value_clone_planned_bytes(value)?,
            "rank joined row clone",
        )?;
    }
    Ok(bytes)
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn bounded_rank_row_retained_bytes(
    row: &VersionedRow,
) -> std::result::Result<usize, Error> {
    let mut bytes = bounded_rank_clone_checked_add(
        std::mem::size_of::<VersionedRow>(),
        bounded_rank_clone_checked_mul(
            row.values.capacity(),
            std::mem::size_of::<(String, Value)>(),
            "rank joined row clone",
        )?,
        "rank joined row clone",
    )?;
    for (column, value) in &row.values {
        bytes = bounded_rank_clone_checked_add(bytes, column.capacity(), "rank joined row clone")?;
        bytes = bounded_rank_clone_checked_add(
            bytes,
            bounded_rank_value_retained_bytes(value)?,
            "rank joined row clone",
        )?;
    }
    Ok(bytes)
}

pub(super) fn rank_value_to_number(
    value: &Value,
    column: &str,
) -> std::result::Result<Option<f32>, FormulaEvalError> {
    match value {
        Value::Null => Ok(None),
        Value::Float64(value) => Ok(Some(*value as f32)),
        Value::Int64(value) => Ok(Some(*value as f32)),
        Value::Bool(value) => Ok(Some(if *value { 1.0 } else { 0.0 })),
        Value::Text(_) => Err(FormulaEvalError::UnsupportedType {
            column: column.to_string(),
            actual: "TEXT",
        }),
        Value::Json(_) => Err(FormulaEvalError::UnsupportedType {
            column: column.to_string(),
            actual: "JSON",
        }),
        Value::Uuid(_) => Err(FormulaEvalError::UnsupportedType {
            column: column.to_string(),
            actual: "UUID",
        }),
        Value::Vector(_) => Err(FormulaEvalError::UnsupportedType {
            column: column.to_string(),
            actual: "VECTOR",
        }),
        Value::Timestamp(_) => Err(FormulaEvalError::UnsupportedType {
            column: column.to_string(),
            actual: "TIMESTAMP",
        }),
        Value::TxId(_) => Err(FormulaEvalError::UnsupportedType {
            column: column.to_string(),
            actual: "TXID",
        }),
    }
}

pub(super) fn merged_rank_values(
    anchor: &VersionedRow,
    joined: Option<&VersionedRow>,
) -> HashMap<String, Value> {
    let mut values = anchor.values.clone();
    if let Some(joined) = joined {
        for (key, value) in &joined.values {
            values.entry(key.clone()).or_insert_with(|| value.clone());
        }
    }
    values
}

pub(super) fn values_equal_for_rank_join(left: &Value, right: &Value) -> bool {
    if matches!((left, right), (Value::Null, _) | (_, Value::Null)) {
        return false;
    }
    left == right
}

impl RankedSearchCandidate {
    pub(super) fn answer_order(&self, other: &Self) -> std::cmp::Ordering {
        rank_float_desc(self.rank, other.rank)
            .then_with(|| rank_float_desc(self.vector_score, other.vector_score))
            .then_with(|| self.row_id.cmp(&other.row_id))
    }
}

impl PartialEq for RankedSearchCandidate {
    fn eq(&self, other: &Self) -> bool {
        self.answer_order(other) == std::cmp::Ordering::Equal
    }
}

impl Eq for RankedSearchCandidate {}

impl PartialOrd for RankedSearchCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RankedSearchCandidate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.answer_order(other)
    }
}

pub(super) fn rank_float_desc(left: f32, right: f32) -> std::cmp::Ordering {
    match (left.is_nan(), right.is_nan()) {
        (true, true) => std::cmp::Ordering::Equal,
        (true, false) => std::cmp::Ordering::Greater,
        (false, true) => std::cmp::Ordering::Less,
        (false, false) => right.total_cmp(&left),
    }
}

pub(super) fn projected_point_lookup(
    cache: &HashMap<String, Vec<VersionedRow>>,
    table: &str,
    natural_key: &NaturalKey,
) -> Option<VersionedRow> {
    cache
        .get(table)
        .and_then(|rows| rows.iter().find(|r| natural_key.matches_values(&r.values)))
        .cloned()
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn sync_visible_point_lookup(
    db: &Database,
    cache: &HashMap<String, Vec<VersionedRow>>,
    table: &str,
    natural_key: &NaturalKey,
    snapshot: SnapshotId,
    skip_deleted: &HashSet<RowId>,

    read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
) -> Result<Option<VersionedRow>> {
    let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

    if let Some(projected) = projected_point_lookup(cache, table, natural_key) {
        return Ok(Some(projected));
    }
    if db.table_meta(table).is_none() {
        return Err(Error::TableNotFound(table.to_string()));
    }
    // Match the arriving row on its WHOLE identity — every key column — so two
    // rows that share only a leading key column stay distinct on apply.
    db.required_indexed_visible_row_by_columns(
        table,
        &natural_key.key_columns(),
        &natural_key.key_values(),
        snapshot,
        skip_deleted,
        read_memory.clone(),
    )
}

pub(super) fn record_cached_insert(
    cache: &mut HashMap<String, Vec<VersionedRow>>,
    table: &str,
    row: VersionedRow,
) {
    cache.entry(table.to_string()).or_default().push(row);
}

pub(super) fn upsert_cached_projection(
    cache: &mut HashMap<String, Vec<VersionedRow>>,
    table: &str,
    row_id: RowId,
    values: HashMap<String, Value>,
    lsn: Lsn,
) {
    let rows = cache.entry(table.to_string()).or_default();
    if let Some(row) = rows.iter_mut().find(|row| row.row_id == row_id) {
        row.values = values;
        row.lsn = lsn;
    } else {
        rows.push(VersionedRow {
            row_id,
            values,
            created_tx: TxId(0),
            deleted_tx: None,
            lsn,
            created_at: None,
        });
    }
}

/// Split served vectors into the groups a row in this apply can own, and a
/// count of the groups no row here can.
///
/// Owner pairing is positional: the n-th group carrying a given table, LSN,
/// and liveness belongs to the n-th row carrying the same. A group beyond
/// that count belongs to a row this apply does not carry, so it is reported
/// rather than paired with whatever row happens to sit at the cursor.
pub(super) fn partition_owned_vector_groups(
    vectors: Vec<VectorChange>,
    rows: &[RowChange],
) -> (Vec<VectorChange>, usize) {
    let mut unclaimed_rows: HashMap<(&str, Lsn, bool), usize> = HashMap::new();
    for row in rows {
        *unclaimed_rows
            .entry((row.table.as_str(), row.lsn, row.deleted))
            .or_insert(0) += 1;
    }
    let mut owned = Vec::with_capacity(vectors.len());
    let mut unowned_groups = 0usize;
    let mut cursor = 0usize;
    while cursor < vectors.len() {
        let end = vector_row_group_end(&vectors, cursor);
        let group = &vectors[cursor..end];
        let key = (
            group[0].index.table.as_str(),
            group[0].lsn,
            group[0].vector.is_empty(),
        );
        match unclaimed_rows.get_mut(&key) {
            Some(remaining) if *remaining > 0 => {
                *remaining -= 1;
                owned.extend_from_slice(group);
            }
            _ => unowned_groups += 1,
        }
        cursor = end;
    }
    (owned, unowned_groups)
}

pub(super) fn consume_vector_row_group(
    vectors: &[VectorChange],
    cursor: &mut usize,
    local_row_id: RowId,
    map: &mut HashMap<RowId, RowId>,
) {
    let Some(first) = vectors.get(*cursor) else {
        return;
    };
    let remote_row_id = first.row_id;
    let table = first.index.table.clone();
    let lsn = first.lsn;
    let deleted = first.vector.is_empty();
    while vectors.get(*cursor).is_some_and(|vector| {
        vector.row_id == remote_row_id
            && vector.index.table == table
            && vector.lsn == lsn
            && vector.vector.is_empty() == deleted
    }) {
        map.insert(remote_row_id, local_row_id);
        *cursor += 1;
    }
}

pub(super) fn consume_failed_vector_row_group(
    vectors: &[VectorChange],
    cursor: &mut usize,
    failed: &mut HashSet<(RowId, String, Lsn, bool)>,
) {
    let Some(first) = vectors.get(*cursor) else {
        return;
    };
    let remote_row_id = first.row_id;
    let table = first.index.table.clone();
    let lsn = first.lsn;
    let deleted = first.vector.is_empty();
    while vectors.get(*cursor).is_some_and(|vector| {
        vector.row_id == remote_row_id
            && vector.index.table == table
            && vector.lsn == lsn
            && vector.vector.is_empty() == deleted
    }) {
        failed.insert((remote_row_id, table.clone(), lsn, deleted));
        *cursor += 1;
    }
}

pub(super) fn next_vector_row_group_matches(
    vectors: &[VectorChange],
    cursor: usize,
    row: &RowChange,
) -> bool {
    vectors.get(cursor).is_some_and(|vector| {
        vector.index.table == row.table
            && vector.lsn == row.lsn
            && vector.vector.is_empty() == row.deleted
    })
}

/// Apply and suppression both pair a vector-bearing row with exactly one
/// contiguous remote-owner group. Same-LSN vectors for another remote owner
/// start the next group and must remain untouched.
pub(super) fn vector_row_group_end(vectors: &[VectorChange], cursor: usize) -> usize {
    let Some(first) = vectors.get(cursor) else {
        return cursor;
    };
    let remote_row_id = first.row_id;
    let table = &first.index.table;
    let lsn = first.lsn;
    let deleted = first.vector.is_empty();
    let mut end = cursor;
    while vectors.get(end).is_some_and(|vector| {
        vector.row_id == remote_row_id
            && vector.index.table.as_str() == table.as_str()
            && vector.lsn == lsn
            && vector.vector.is_empty() == deleted
    }) {
        end += 1;
    }
    end
}

// Access predicates remain residual even when SQL or access-derived tuple
// selection covers the partition key. Both passive and completed disclosure
// use the same candidate-route decision that admits execution.
pub(super) fn vector_search_candidate_residual(
    shape: &VectorExplainShape<'_>,
    route: &crate::executor::VectorCandidateRoute,
) -> VectorSearchResidual {
    let restricted = route.access_filter
        || (shape.restricted_candidates
            && !route
                .scope
                .as_ref()
                .is_some_and(|scope| scope.covers_filter));
    if !restricted {
        VectorSearchResidual::None
    } else if route.complete {
        VectorSearchResidual::Bounded
    } else {
        VectorSearchResidual::Unsupported
    }
}

pub(super) fn vector_search_shape_from_plan(plan: &PhysicalPlan) -> Option<VectorExplainShape<'_>> {
    match plan {
        PhysicalPlan::VectorSearch {
            table,
            column,
            query_expr,
            k,
            candidates,
            search_mode,
            query_policy,
            ..
        }
        | PhysicalPlan::HnswSearch {
            table,
            column,
            query_expr,
            k,
            candidates,
            search_mode,
            query_policy,
            ..
        } => Some(VectorExplainShape {
            index: VectorIndexRef::new(table.clone(), column.clone()),
            k: usize::try_from(*k).unwrap_or(usize::MAX),
            restricted_candidates: candidates
                .as_deref()
                .is_some_and(|candidate| !is_unrestricted_scan_for_table(candidate, table)),
            candidates: candidates.as_deref(),
            search_mode: *search_mode,
            query_policy: query_policy.as_ref(),
            query_expr,
        }),
        PhysicalPlan::Project { input, .. }
        | PhysicalPlan::Filter { input, .. }
        | PhysicalPlan::Distinct { input }
        | PhysicalPlan::Limit { input, .. }
        | PhysicalPlan::Sort { input, .. }
        | PhysicalPlan::MaterializeCte { input, .. } => vector_search_shape_from_plan(input),
        PhysicalPlan::Join { left, right, .. } => {
            vector_search_shape_from_plan(left).or_else(|| vector_search_shape_from_plan(right))
        }
        PhysicalPlan::Pipeline(plans) => plans.iter().find_map(vector_search_shape_from_plan),
        _ => None,
    }
}

pub(super) fn is_unrestricted_scan_for_table(plan: &PhysicalPlan, table: &str) -> bool {
    matches!(
        plan,
        PhysicalPlan::Scan {
            table: scan_table,
            filter: None,
            ..
        } if scan_table == table
    )
}

pub(super) fn vector_filter_index_recovery(
    candidates: Option<&PhysicalPlan>,
    table: &str,
) -> Option<String> {
    const GENERIC_RECOVERY: &str =
        "use EXACT or rewrite the filter with a supporting relational index";

    fn collect(expr: &Expr, table: &str, columns: &mut BTreeSet<String>) {
        match expr {
            Expr::Column(column) if column.table.as_deref().is_none_or(|named| named == table) => {
                columns.insert(column.column.clone());
            }
            Expr::BinaryOp { left, right, .. } | Expr::CosineDistance { left, right } => {
                collect(left, table, columns);
                collect(right, table, columns);
            }
            Expr::UnaryOp { operand, .. } | Expr::IsNull { expr: operand, .. } => {
                collect(operand, table, columns);
            }
            Expr::FunctionCall { args, .. } => {
                for arg in args {
                    collect(arg, table, columns);
                }
            }
            Expr::InList { expr, list, .. } => {
                collect(expr, table, columns);
                for item in list {
                    collect(item, table, columns);
                }
            }
            Expr::Like { expr, pattern, .. } => {
                collect(expr, table, columns);
                collect(pattern, table, columns);
            }
            Expr::RowVectorSource { key, .. } => collect(key, table, columns),
            Expr::InSubquery { expr, .. } => collect(expr, table, columns),
            Expr::Row(elements) => {
                for element in elements {
                    collect(element, table, columns);
                }
            }
            Expr::Literal(_)
            | Expr::Parameter(_)
            | Expr::Column(_)
            | Expr::RowNumber(_)
            | Expr::WindowRank(_) => {}
        }
    }

    let Some(candidates) = candidates else {
        return Some(GENERIC_RECOVERY.to_string());
    };
    let PhysicalPlan::Scan {
        table: candidate_table,
        filter: Some(filter),
        ..
    } = candidates
    else {
        return Some(GENERIC_RECOVERY.to_string());
    };
    if candidate_table != table {
        return Some(GENERIC_RECOVERY.to_string());
    }
    let mut columns = BTreeSet::new();
    collect(filter, table, &mut columns);
    if columns.is_empty() {
        return Some(GENERIC_RECOVERY.to_string());
    }
    let columns = columns.into_iter().collect::<Vec<_>>();
    Some(format!(
        "CREATE INDEX {table}_{}_idx ON {table} ({})",
        columns.join("_"),
        columns.join(", ")
    ))
}

pub(super) fn annotate_vector_search_strategy(mut output: String, strategy: &str) -> String {
    let needle = "VectorSearch(";
    let mut search_from = 0;
    while let Some(relative_pos) = output[search_from..].find(needle) {
        let operator_start = search_from + relative_pos;
        let open_paren = operator_start + needle.len() - 1;
        let mut depth = 0_u32;
        let mut insert_at = None;
        for (relative_idx, ch) in output[open_paren..].char_indices() {
            match ch {
                '(' => depth = depth.saturating_add(1),
                ')' => {
                    depth = depth.saturating_sub(1);
                    if depth == 0 {
                        insert_at = Some(open_paren + relative_idx);
                        break;
                    }
                }
                _ => {}
            }
        }
        let Some(insert_at) = insert_at else {
            break;
        };
        let annotation = format!(", strategy={strategy}");
        output.insert_str(insert_at, &annotation);
        search_from = insert_at + annotation.len();
    }
    output
}

impl StartupState<'_> {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn config_value<T: serde::de::DeserializeOwned>(
        &self,
        key: &str,
    ) -> Result<Option<T>> {
        match self {
            Self::Durable(persistence) => persistence.load_config_value(key),
            Self::Image(image) => match image.config_values.get(key) {
                Some(bytes) => Ok(Some(RedbPersistence::decode_config_value(bytes)?)),
                None if crate::persistence::read_image_open_config_key(key) => Ok(None),
                None => image.config_source.load_image_config_value(key),
            },
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn sink_queue<T: serde::de::DeserializeOwned>(&self, sink: &str) -> Result<Vec<T>> {
        match self {
            Self::Durable(persistence) => persistence.load_sink_queue(sink),
            Self::Image(image) => image
                .sink_queues
                .get(sink)
                .into_iter()
                .flatten()
                .map(|(_, bytes)| RedbPersistence::decode_config_value(bytes))
                .collect(),
        }
    }

    /// A queue this source counts without holding its payloads: a committed
    /// image leaves them in the store. `None` when the queue was loaded.
    pub(crate) fn unloaded_sink_queue(
        &self,
        sink: &str,
    ) -> Option<crate::persistence::SinkQueueExtent> {
        match self {
            Self::Durable(_) => None,
            Self::Image(image) if image.sink_queues.contains_key(sink) => None,
            Self::Image(image) => image.sink_queue_extents.get(sink).copied(),
        }
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn trigger_audit_state(
        &self,
        ring_capacity: usize,
    ) -> Result<(Vec<TriggerAuditEntry>, u64)> {
        let persistence = match self {
            Self::Durable(persistence) => {
                return persistence.load_trigger_audit_state(ring_capacity);
            }
            Self::Image(image) => image,
        };
        let ring = persistence
            .config_values
            .get(crate::persistence::TRIGGER_AUDIT_RING_CONFIG_KEY)
            .map(|bytes| RedbPersistence::decode_config_value::<Vec<TriggerAuditEntry>>(bytes))
            .transpose()?;
        let next_index = persistence
            .config_values
            .get(crate::persistence::TRIGGER_AUDIT_NEXT_INDEX_CONFIG_KEY)
            .map(|bytes| RedbPersistence::decode_config_value::<u64>(bytes))
            .transpose()?;
        if let (Some(mut ring), Some(next_index)) = (ring, next_index) {
            let overflow = ring.len().saturating_sub(ring_capacity);
            if overflow > 0 {
                ring.drain(0..overflow);
            }
            return Ok((ring, next_index));
        }

        let mut ring = std::collections::VecDeque::new();
        let mut next_index = 0;
        for (key, bytes) in &persistence.trigger_audit {
            if let Some(index) = key
                .split(':')
                .next()
                .and_then(|raw| raw.parse::<u64>().ok())
            {
                next_index = next_index.max(index.saturating_add(1));
            }
            ring.push_back(RedbPersistence::decode_config_value(bytes)?);
            if ring.len() > ring_capacity {
                ring.pop_front();
            }
        }
        Ok((ring.into_iter().collect(), next_index))
    }
}
impl PurgeReplay {
    pub(super) fn empty() -> Self {
        Self {
            cursor: None,
            root: None,
            owner: None,
            peeked: None,
        }
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn fill(&mut self) -> Result<()> {
        if self.peeked.is_none()
            && let Some(cursor) = self.cursor.as_mut()
        {
            // The ordered key supplies the frontier. Do not decode an
            // excluded item's potentially large lineage list just to cut.
            self.peeked = cursor.next(|key, _value| {
                let coordinate = std::str::from_utf8(key)
                    .ok()
                    .and_then(|key| key.strip_prefix(AUTHORITATIVE_PURGE_DELIVERY_PREFIX))
                    .and_then(|key| key.split_once('.'))
                    .filter(|(lsn, ordinal)| {
                        lsn.len() == 20
                            && ordinal.len() == 10
                            && lsn
                                .bytes()
                                .chain(ordinal.bytes())
                                .all(|byte| byte.is_ascii_digit())
                    })
                    .and_then(|(lsn, ordinal)| {
                        Some((Lsn(lsn.parse().ok()?), ordinal.parse::<u32>().ok()?))
                    })
                    .ok_or_else(|| Error::StoreCorrupted {
                        path: "authoritative purge delivery journal".into(),
                        reason: "invalid ordered purge coordinate".into(),
                    })?;
                Ok(coordinate)
            })?;
            #[cfg(any(test, feature = "test-seams"))]
            if self.peeked.is_some() {
                resident_history_test_seams::note(|work| work.pull_source_entries += 1);
            }
        }
        Ok(())
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn peek_lsn(&mut self) -> Result<Option<Lsn>> {
        self.fill()?;
        Ok(self.peeked.as_ref().map(|window| window.value.0))
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn next_item(
        &mut self,
    ) -> Result<
        Option<crate::persistence::writer_source::WriterWindow<AuthoritativePurgeDeliveryItem>>,
    > {
        self.fill()?;
        let Some(position) = self.peeked.take() else {
            return Ok(None);
        };
        let root = self
            .root
            .as_ref()
            .ok_or_else(|| Error::Other("purge cursor has no captured root".into()))?;
        let owner = self
            .owner
            .as_ref()
            .ok_or_else(|| Error::Other("purge cursor has no source owner".into()))?;
        let key = std::str::from_utf8(position.position()).map_err(|_| Error::StoreCorrupted {
            path: "authoritative purge delivery journal".into(),
            reason: "invalid ordered purge key".into(),
        })?;
        let item = root
            .config_point::<AuthoritativePurgeDeliveryItem>(owner.clone(), key)?
            .ok_or_else(|| Error::StoreCorrupted {
                path: "authoritative purge delivery journal".into(),
                reason: "selected purge occurrence has no body".into(),
            })?;
        if (item.value.frontier, item.value.ordinal) != position.value {
            return Err(Error::StoreCorrupted {
                path: "authoritative purge delivery journal".into(),
                reason: "ordered purge coordinate differs from its body".into(),
            });
        }
        Ok(Some(item))
    }
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn unbounded_checkpoint() -> Result<()> {
    Ok(())
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn unbounded_admission(_: usize) -> Result<()> {
    Ok(())
}

impl ChangeProjection<'static> {
    pub(crate) fn unbounded() -> Self {
        Self {
            checkpoint: &unbounded_checkpoint,
            admit: &unbounded_admission,
            release: &unbounded_admission,
            bounded: false,
            ddl: true,
            page: None,
        }
    }
}

/// What a change set occupies: its lists at their capacities and everything
/// their elements own.
pub(crate) fn change_set_held_bytes(changes: &ChangeSet) -> usize {
    fn list<T>(items: &[T], capacity: usize, element: impl Fn(&T) -> usize) -> usize {
        let owned = items
            .iter()
            .map(|item| element(item).saturating_sub(std::mem::size_of::<T>()))
            .sum::<usize>();
        if capacity == 0 {
            owned
        } else {
            capacity
                .saturating_mul(std::mem::size_of::<T>())
                .saturating_add(held::ALLOCATION_BYTES)
                .saturating_add(owned)
        }
    }
    list(
        &changes.rows,
        changes.rows.capacity(),
        RowChange::held_bytes,
    )
    .saturating_add(list(
        &changes.edges,
        changes.edges.capacity(),
        EdgeChange::held_bytes,
    ))
    .saturating_add(list(
        &changes.vectors,
        changes.vectors.capacity(),
        VectorChange::held_bytes,
    ))
    .saturating_add(list(&changes.ddl, changes.ddl.capacity(), |ddl| {
        std::mem::size_of::<DdlChange>().saturating_add(ddl.estimated_bytes())
    }))
    .saturating_add(
        changes
            .ddl_lsn
            .capacity()
            .saturating_mul(std::mem::size_of::<Lsn>()),
    )
}

/// Test-only: the next pending-deletion record this thread reads for a
/// committed reader's history answer reads as bytes that do not decode.
#[cfg(any(test, feature = "test-seams"))]
#[doc(hidden)]
pub fn fail_next_pending_deletion_read_for_test() {
    PENDING_DELETION_READ_FAULT.with(|fault| fault.set(true));
}

/// One copied change-log entry: the entry itself and the names it owns.
pub(super) fn change_log_entry_copy_bytes(entry: &ChangeLogEntry) -> usize {
    std::mem::size_of::<ChangeLogEntry>().saturating_add(match entry {
        ChangeLogEntry::RowInsert { table, .. } => held::text(table),
        ChangeLogEntry::RowDelete {
            table, natural_key, ..
        } => held::text(table).saturating_add(natural_key.held_bytes()),
        ChangeLogEntry::EdgeInsert { edge_type, .. }
        | ChangeLogEntry::EdgeDelete { edge_type, .. } => held::text(edge_type),
        ChangeLogEntry::VectorInsert { index, .. } | ChangeLogEntry::VectorDelete { index, .. } => {
            held::text(&index.table).saturating_add(held::text(&index.column))
        }
    })
}

/// One element of an answer's list while the list is built: what the
/// element owns, and its slot twice more -- a list grows by doubling, so its
/// buffer is at most twice its length, and the buffer a growth replaces
/// lives until the new one is filled.
pub(super) fn answer_list_entry_bytes<T>(element: usize) -> usize {
    element.saturating_add(std::mem::size_of::<T>().saturating_mul(2))
}

/// One key slot in a hash map with its bucket array's spare room: a map is
/// at most half empty after it grows, and the array it grew from lives until
/// its entries have moved.
pub(super) const fn hashed_slot_bytes<T>() -> usize {
    (std::mem::size_of::<T>() + 1) * 3
}

/// One entry of the owner-tracking maps a change extraction keeps per row
/// change-log record, beyond the names it copies.
pub(super) fn owner_tracking_slot_bytes() -> usize {
    hashed_slot_bytes::<((String, RowId), Lsn)>()
        .max(hashed_slot_bytes::<((Vec<u8>, Lsn), RowId)>())
}

/// One row's entry in the upsert-pairing and represented-row maps: its slot,
/// its copied table name and its natural key rendered as text.
pub(super) fn row_pairing_entry_bytes(row: &RowChange) -> usize {
    hashed_slot_bytes::<((String, String, Lsn), Lsn)>()
        .saturating_add(held::text(&row.table))
        .saturating_add(row.natural_key.held_bytes().saturating_mul(2))
        .saturating_add(64 + held::ALLOCATION_BYTES)
}

impl From<Error> for ImageHistoryError {
    fn from(error: Error) -> Self {
        Self::Engine(error)
    }
}

impl ImageHistoryError {
    pub(crate) fn store(
        owner: contextdb_core::read_memory::ReadMemoryOwner,
        path: impl std::fmt::Display,
        diagnosis: crate::persistence::LoadReadImageError,
    ) -> Self {
        // Preserve the original refusal before preparing any rendered detail.
        if let crate::persistence::LoadReadImageError::Memory(error) = diagnosis {
            return Self::Engine(error);
        }
        match crate::funded_error::diagnostic_text(owner.clone(), path) {
            Ok(path) => Self::Store {
                path,
                diagnosis,
                owner,
            },
            Err(error) => Self::Engine(error),
        }
    }

    pub(crate) fn into_engine_error(self) -> Error {
        use crate::persistence::LoadReadImageError as Load;
        match self {
            Self::Engine(error) => error,
            Self::Store {
                path,
                diagnosis: Load::Memory(error),
                ..
            } => {
                drop(path);
                error
            }
            Self::Store {
                path,
                diagnosis,
                owner,
            } => {
                let corrupted =
                    matches!(diagnosis, Load::Corrupt { .. } | Load::InvalidState { .. });
                if matches!(diagnosis, Load::RequiresWriter { .. }) {
                    struct Count(usize);
                    impl std::fmt::Write for Count {
                        fn write_str(&mut self, text: &str) -> std::fmt::Result {
                            self.0 = self.0.checked_add(text.len()).ok_or(std::fmt::Error)?;
                            Ok(())
                        }
                    }
                    let mut count = Count(0);
                    use std::fmt::Write;
                    if write!(&mut count, "{diagnosis}").is_err() {
                        return Error::Other("history diagnosis length overflow".into());
                    }
                    return match contextdb_core::read_contract::ReadFailure::try_funded_reason(
                        contextdb_core::read_contract::ReadFailureKind::DirectReadRequiresWriter,
                        owner,
                        count.0,
                        |text| {
                            write!(text, "{diagnosis}").expect("exact diagnosis capacity");
                        },
                    ) {
                        Ok(failure) => Error::ReadFailure(failure),
                        Err(error) => error,
                    };
                }
                match crate::funded_error::diagnostic_text(owner, diagnosis) {
                    Ok(reason) if corrupted => Error::StoreCorrupted { path, reason },
                    Ok(reason) => Error::Other(reason),
                    Err(error) => error,
                }
            }
        }
    }
}
impl Drop for LoadedHistoryBuffers {
    fn drop(&mut self) {
        drop(std::mem::take(&mut *self.ddl_log.write()));
        drop(std::mem::take(&mut *self.change_log.write()));
        drop(std::mem::take(&mut *self.table_index.write()));
        drop(std::mem::take(&mut *self.lsn_refcounts.write()));
        drop(self.history.loaded.lock().take());
    }
}

pub(crate) fn vector_index_layout_from_column(column: &ColumnDef) -> Option<VectorIndexLayout> {
    let ColumnType::Vector(dimension) = &column.column_type else {
        return None;
    };
    Some(
        VectorIndexLayout::new(
            *dimension,
            column.quantization,
            column.partition_key_columns.clone().unwrap_or_default(),
            column.effective_max_partitions(),
            column.search_mode,
        )
        .with_policy(
            column.auto_index_at,
            column.hnsw_m,
            column.hnsw_ef_construction,
            column.hnsw_ef_search,
            column.vector_policy_revision,
        )
        .with_consolidation_policy(
            column.consolidation_change_percent,
            column.consolidation_tombstone_percent,
        )
        .with_consolidation_disabled(column.consolidation_disabled)
        .with_flat_below(column.flat_below)
        .with_auto_expansion_ceiling(column.auto_expansion_ceiling)
        .with_search_radius_millionths(column.hnsw_search_radius_millionths),
    )
}

/// Two layouts store their vectors alike: the same dimension, quantization
/// and partition key. Any other difference is policy.
pub(super) fn same_vector_storage_shape(
    left: &VectorIndexLayout,
    right: &VectorIndexLayout,
) -> bool {
    left.dimension == right.dimension
        && left.quantization == right.quantization
        && left.partition_key_columns == right.partition_key_columns
}

// The prepared delta and generation encoding simultaneously retain these
// input images. Keep their copy charge with the enclosing purge workspace.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn admit_erasure_vector_input_copies(
    schemas: &[(VectorIndexRef, VectorIndexLayout)],
    entries: &[PartitionedVectorEntry],
    affected: &[VectorPartitionRef],
    owner: &mut contextdb_vector::VectorCopyAdmission,
) -> Result<()> {
    owner.reserve(
        schemas
            .len()
            .saturating_mul(std::mem::size_of::<(VectorIndexRef, VectorIndexLayout)>())
            .saturating_add(
                entries
                    .len()
                    .saturating_mul(std::mem::size_of::<PartitionedVectorEntry>()),
            )
            .saturating_add(
                affected
                    .len()
                    .saturating_mul(std::mem::size_of::<VectorPartitionRef>()),
            ),
    )?;
    for (index, layout) in schemas {
        owner.reserve(
            index
                .table
                .len()
                .saturating_add(index.column.len())
                .saturating_add(layout.estimated_bytes()),
        )?;
    }
    for entry in entries {
        owner.reserve(
            crate::memory_accounting::preparation_vector_copy_bytes(&entry.entry)?
                .saturating_add(entry.partition_key.estimated_bytes()),
        )?;
    }
    for partition in affected {
        owner.reserve(
            partition
                .index
                .table
                .len()
                .saturating_add(partition.index.column.len())
                .saturating_add(partition.partition_key.estimated_bytes()),
        )?;
    }
    Ok(())
}

pub(super) fn vector_layouts_from_meta(
    table_meta: &HashMap<String, TableMeta>,
) -> Vec<(VectorIndexRef, VectorIndexLayout)> {
    let mut layouts = table_meta
        .iter()
        .flat_map(|(table, meta)| {
            meta.columns.iter().filter_map(move |column| {
                vector_index_layout_from_column(column).map(|layout| {
                    (
                        VectorIndexRef::new(table.clone(), column.name.clone()),
                        layout,
                    )
                })
            })
        })
        .collect::<Vec<_>>();
    layouts.sort_by(|(left, _), (right, _)| {
        left.table
            .cmp(&right.table)
            .then(left.column.cmp(&right.column))
    });
    layouts
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn register_vector_layouts(
    vector: &VectorStore,
    table_meta: &HashMap<String, TableMeta>,
    declared_owner: impl Into<contextdb_vector::MemoryBudgetOwner>,
    vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
    _read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
) -> Result<()> {
    let declared_owner: contextdb_vector::MemoryBudgetOwner = declared_owner.into();

    let _read_memory: contextdb_core::read_memory::ReadMemoryOwner = _read_memory.into();

    for (index, layout) in vector_layouts_from_meta(table_meta) {
        vector.register_index_with_layout(
            index,
            layout,
            declared_owner.clone(),
            vector_copy_owner,
        )?;
    }
    Ok(())
}

// Proof-only recovery coverage retains this helper in test builds.
#[cfg(test)]
pub(super) fn supplement_loaded_vectors_from_table_rows(
    tables: &HashMap<String, Vec<VersionedRow>>,
    table_meta: &HashMap<String, TableMeta>,
    vectors: &mut Vec<VectorEntry>,
) -> bool {
    let mut seen_occurrences = vectors
        .iter()
        .map(|entry| {
            (
                entry.index.clone(),
                entry.row_id,
                entry.created_tx,
                entry.lsn,
            )
        })
        .collect::<HashSet<_>>();
    // Every (index, row_id) the persisted vector store has EVER recorded an
    // occurrence for, live or deleted. A deliberate deletion here (e.g.
    // `PROPAGATE ON STATE ... EXCLUDE VECTOR`, which tombstones the vector
    // entry but leaves the row's own vector column value untouched) means
    // the engine already made and recorded a decision about this row's
    // vector -- recovery below exists only for a row whose vector the engine
    // has NEVER recorded at all, never to reverse an
    // explicit deletion decision it already made and persisted.
    let mut known_owners = vectors
        .iter()
        .map(|entry| (entry.index.clone(), entry.row_id))
        .collect::<HashSet<_>>();
    let mut supplemented = false;
    for (table, meta) in table_meta {
        let Some(rows) = tables.get(table) else {
            continue;
        };
        for column in &meta.columns {
            let ColumnType::Vector(dimension) = column.column_type else {
                continue;
            };
            let index = VectorIndexRef::new(table.clone(), column.name.clone());
            for row in rows {
                let Some(Value::Vector(vector)) = row.values.get(&column.name) else {
                    continue;
                };
                if vector.len() != dimension {
                    continue;
                }
                // A relational body-only update gets a newer row version but
                // does not create another vector occurrence.  A previously
                // recorded owner -- live OR deleted -- is authoritative for
                // that row ID; only recover a live row when NO such owner
                // exists at all.  Historical and deleted row versions still
                // use their full version identity below.
                if row.deleted_tx.is_none() && known_owners.contains(&(index.clone(), row.row_id)) {
                    continue;
                }
                let occurrence = (index.clone(), row.row_id, row.created_tx, row.lsn);
                if seen_occurrences.insert(occurrence) {
                    vectors.push(VectorEntry {
                        index: index.clone(),
                        row_id: row.row_id,
                        vector: vector.clone(),
                        created_tx: row.created_tx,
                        deleted_tx: row.deleted_tx,
                        lsn: row.lsn,
                    });
                    known_owners.insert((index.clone(), row.row_id));
                    supplemented = true;
                }
            }
        }
    }
    supplemented
}

pub(super) fn remove_cached_row(
    cache: &mut HashMap<String, Vec<VersionedRow>>,
    table: &str,
    row_id: RowId,
) {
    if let Some(rows) = cache.get_mut(table) {
        rows.retain(|row| row.row_id != row_id);
    }
}

pub(super) fn query_outcome_from_result(result: &Result<OwnedQueryResult>) -> QueryOutcome {
    match result {
        Ok(query_result) => QueryOutcome::Success {
            row_count: if query_result.rows.is_empty() {
                query_result.rows_affected as usize
            } else {
                query_result.rows.len()
            },
        },
        Err(error) => QueryOutcome::Error {
            error: error.to_string(),
        },
    }
}

pub(super) fn maybe_prebuild_hnsw(vector_store: &VectorStore, accountant: &MemoryAccountant) {
    let _ = (vector_store, accountant);
}

/// What a row VERSION costs the store that KEEPS it, as opposed to what one
/// copy of it occupies. Three things, and a declared `MEMORY_LIMIT` is only
/// effective if the figure covers all three:
///
/// * the version the table's own vector keeps -- `ROW_VALUE_RETENTIONS`
///   copies of the values, which is one;
/// * the four side maps the relational store keeps beside every row
///   (`retained_row_side_map_bytes`), which are fixed per row and invisible in
///   the row's own bytes;
/// * one posting per DECLARED INDEX (`retained_index_posting_bytes_for_meta`),
///   each a whole clone of the columns that index keys on. This is what the
///   flat doubling used to stand in for, and it is why the same figure has to
///   be taken against the table's CURRENT schema at both ends: a CREATE INDEX
///   re-computes it for every existing row under the new schema
///   (`prepare_local_table_projection_memory`), so the tenth index is charged
///   for the postings it builds and a DROP INDEX gives them back.
///
/// An F32 vector column keeps its relational projection inside the row
/// values, so that projection is part of what the row version physically
/// holds: it is charged when the version is admitted and released when the
/// version is physically removed, never by any other owner (the vector store
/// owns only the raw body it keeps). `include_vectors` says whether that
/// projection is part of the figure. Every site that charges or releases a
/// whole version passes `true`; the one exception is an admission that
/// charges the projection separately under its own `vector_insert@` tag
/// (`Database::admit_retained_row_bytes`), so a budget refusal names the
/// vector column that caused it. The figure with `true` is exactly the sum
/// of the two parts, so the version's release is the same whichever door
/// admitted it.
pub(crate) fn retained_row_bytes_for_meta(
    table: &str,
    values: &HashMap<ColName, Value>,
    meta: &TableMeta,
    include_vectors: bool,
) -> usize {
    let non_vector = estimate_row_bytes_for_meta(values, meta, false)
        .saturating_mul(ROW_VALUE_RETENTIONS)
        .saturating_add(contextdb_core::retained_row_side_map_bytes(table))
        .saturating_add(contextdb_core::retained_index_posting_bytes_for_meta(
            values, meta,
        ));
    if include_vectors {
        non_vector.saturating_add(f32_projection_payload_bytes(values, meta))
    } else {
        non_vector
    }
}

/// Price schema declarations and only the filters this backend actually retains.
pub(crate) fn retained_table_metadata_bytes(store: &RelationalStore, meta: &TableMeta) -> usize {
    if store.uses_disk_backed_committed_bodies() {
        meta.metadata_bytes()
    } else {
        meta.estimated_bytes()
    }
}

/// What one stored row version costs the store, for a caller that has the
/// table name and the row but may not have the table's schema.
///
/// The ONE door for every release. A release that does not answer the same
/// figure admission charged strands bytes the store is no longer holding, and
/// an operator sees a limit refuse an import it has room for -- so every
/// release site asks this, and the schema-less case is answered by
/// [`fallback_retained_row_bytes`] rather than by each site inventing its own.
pub(crate) fn retained_row_bytes_with_optional_meta(
    table: &str,
    row: &VersionedRow,
    meta: Option<&TableMeta>,
) -> usize {
    match meta {
        Some(meta) => retained_row_bytes_for_meta(table, &row.values, meta, true),
        None => fallback_retained_row_bytes(table, row),
    }
}

/// What a row version costs when its table's schema is already gone.
///
/// Exactly what [`retained_row_bytes_for_meta`] answers for a table whose
/// columns are this row's own and which declares no index, read from the row
/// instead of from the schema: the version's header and value map, each value
/// once -- a vector column contributing its F32 projection, which is what the
/// version physically keeps -- and the four side maps the relational store
/// keeps beside every row. The third term, one posting per declared index, is
/// zero because a table with no schema has no declared index; its storages
/// went with it. `row_version_bytes_match_the_schema_less_fallback` in this
/// file holds the two to the same number.
pub(crate) fn fallback_retained_row_bytes(table: &str, row: &VersionedRow) -> usize {
    let values = row
        .values
        .iter()
        .fold(96usize, |bytes, (column, value)| match value {
            // A vector column contributes its F32 projection and nothing else,
            // which is what the schema-carrying figure counts for it: the column
            // is skipped by the row estimate and its payload added back by the
            // projection term.
            Value::Vector(vector) => {
                bytes.saturating_add(vector.len().saturating_mul(std::mem::size_of::<f32>()))
            }
            other => bytes.saturating_add(
                contextdb_core::estimate_value_key_bytes(column) + other.estimated_bytes(),
            ),
        });
    values.saturating_add(contextdb_core::retained_row_side_map_bytes(table))
}

/// The F32 projection bytes a row version holds, per vector column, in
/// declaration order. Only an F32 column keeps its vector inside the row;
/// a quantized column's relational slot is blank (`blank_quantized_vector_values`).
pub(super) fn f32_projection_payload_bytes_by_column<'a>(
    values: &'a HashMap<ColName, Value>,
    meta: &'a TableMeta,
) -> impl Iterator<Item = (&'a ColName, usize)> + 'a {
    meta.columns.iter().filter_map(move |column| {
        if !matches!(column.column_type, ColumnType::Vector(_))
            || column.quantization != VectorQuantization::F32
        {
            return None;
        }
        let Some(Value::Vector(vector)) = values.get(&column.name) else {
            return None;
        };
        Some((
            &column.name,
            vector.len().saturating_mul(std::mem::size_of::<f32>()),
        ))
    })
}

pub(super) fn f32_projection_payload_bytes(
    values: &HashMap<ColName, Value>,
    meta: &TableMeta,
) -> usize {
    f32_projection_payload_bytes_by_column(values, meta).fold(0usize, |bytes, (_, column_bytes)| {
        bytes.saturating_add(column_bytes)
    })
}

pub(super) fn estimate_row_bytes_for_meta(
    values: &HashMap<ColName, Value>,
    meta: &TableMeta,
    include_vectors: bool,
) -> usize {
    let mut bytes = 96usize;
    for column in &meta.columns {
        let Some(value) = values.get(&column.name) else {
            continue;
        };
        if !include_vectors && matches!(column.column_type, ColumnType::Vector(_)) {
            continue;
        }
        bytes =
            bytes.saturating_add(estimate_value_key_bytes(&column.name) + value.estimated_bytes());
    }
    bytes
}

pub(super) fn standalone_edge_retained_bytes(edge: &AdjEntry) -> usize {
    edge.estimated_bytes().saturating_add(
        std::mem::size_of::<Option<contextdb_tx::RowEdgeOwner>>()
            .saturating_mul(contextdb_core::ADJACENCY_RETENTIONS),
    )
}

pub(super) fn estimate_edge_bytes(
    source: NodeId,
    target: NodeId,
    edge_type: &str,
    properties: &HashMap<String, Value>,
) -> usize {
    // Price the borrowed body without cloning an unadmitted property map.
    let empty = AdjEntry {
        source,
        target,
        edge_type: String::new(),
        properties: HashMap::new(),
        created_tx: TxId(0),
        deleted_tx: None,
        lsn: Lsn(0),
    };
    standalone_edge_retained_bytes(&empty)
        .saturating_add(
            edge_type
                .len()
                .saturating_mul(2)
                .saturating_mul(contextdb_core::ADJACENCY_RETENTIONS),
        )
        .saturating_add(
            contextdb_core::estimate_row_value_bytes(properties)
                .saturating_sub(contextdb_core::estimate_row_value_bytes(&empty.properties))
                .saturating_mul(contextdb_core::ADJACENCY_RETENTIONS),
        )
}
impl DatabaseConstruction {
    pub(super) fn finish(&mut self) {
        for step in (0..135).chain([137, 136, 135]) {
            if !std::mem::replace(&mut self.initialized[step], false) {
                continue;
            }
            // Each marked field is consumed once. On a single unwind a live
            // guard resumes after the field already being destroyed.
            unsafe {
                match step {
                    0 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!((*self.source).id));
                    }
                    1 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!((*self.source).tx_mgr));
                    }
                    2 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).relational_store
                        ));
                    }
                    3 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!((*self.source).graph_store));
                    }
                    4 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).vector_store
                        ));
                    }
                    5 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).vector_schema_gates
                        ));
                    }
                    137 => {}
                    6 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!((*self.source).change_log));
                    }
                    7 => {
                        #[cfg(any(test, feature = "test-seams"))]
                        {
                            std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                                (*self.source).forced_change_log_gap_through
                            ));
                        }
                    }
                    8 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).change_log_table_index
                        ));
                    }
                    9 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).change_log_lsn_refcounts
                        ));
                    }
                    10 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!((*self.source).ddl_log));
                    }
                    11 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).sync_tombstone_arrivals
                        ));
                    }
                    12 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).sync_graph_arrivals
                        ));
                    }
                    13 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).terminal_refusal_markers
                        ));
                    }
                    14 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).terminal_refusal_scans
                        ));
                    }
                    15 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).accepted_sync_row_authors
                        ));
                    }
                    16 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).received_schema_stages
                        ));
                    }
                    17 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).local_erasure_stages
                        ));
                    }
                    18 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).received_arriving_table_meta
                        ));
                    }
                    19 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).pending_local_schema_stages
                        ));
                    }
                    20 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).local_schema_stages
                        ));
                    }
                    21 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).capture_detached_sync_write_set
                        ));
                    }
                    22 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).detached_sync_write_set
                        ));
                    }
                    23 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).received_staging_access
                        ));
                    }
                    24 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).received_vector_image_pending
                        ));
                    }
                    25 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!((*self.source).persistence));
                    }
                    26 => {
                        #[cfg(any(test, feature = "test-seams"))]
                        {
                            std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                                (*self.source).commit_source_reference_charge
                            ));
                        }
                    }
                    27 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).committed_image_startup
                        ));
                    }
                    28 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).image_history
                        ));
                    }
                    29 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).retained_relational_source
                        ));
                    }
                    30 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).blob_repository
                        ));
                    }
                    31 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).open_registry_path
                        ));
                    }
                    32 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).operation_gate
                        ));
                    }
                    33 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).read_publication_gate
                        ));
                    }
                    34 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).schema_publication_gate
                        ));
                    }
                    35 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).apply_phase_pause
                        ));
                    }
                    36 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).sync_apply_pre_commit_pause
                        ));
                    }
                    37 => {
                        #[cfg(any(test, feature = "test-seams"))]
                        {
                            std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                                (*self.source).received_schema_pre_commit_pause
                            ));
                        }
                    }
                    38 => {
                        #[cfg(any(test, feature = "test-seams"))]
                        {
                            std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                                (*self.source).received_schema_pre_publish_pause
                            ));
                        }
                    }
                    39 => {
                        #[cfg(any(test, feature = "test-seams"))]
                        {
                            std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                                (*self.source).received_schema_apply_observer
                            ));
                        }
                    }
                    40 => {
                        #[cfg(any(test, feature = "test-seams"))]
                        {
                            std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                                (*self.source).local_schema_pre_commit_pause
                            ));
                        }
                    }
                    41 => {
                        #[cfg(any(test, feature = "test-seams"))]
                        {
                            std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                                (*self.source).local_schema_pre_publish_pause
                            ));
                        }
                    }
                    42 => {
                        #[cfg(any(test, feature = "test-seams"))]
                        {
                            std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                                (*self.source).local_schema_between_publication_pause
                            ));
                        }
                    }
                    43 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).export_after_capture_pause
                        ));
                    }
                    44 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!((*self.source).relational));
                    }
                    45 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!((*self.source).graph));
                    }
                    46 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!((*self.source).vector));
                    }
                    47 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).image_store_file_bytes
                        ));
                    }
                    48 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!((*self.source).session_tx));
                    }
                    49 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).origin_session_tx
                        ));
                    }
                    50 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!((*self.source).instance_id));
                    }
                    51 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).owner_thread
                        ));
                    }
                    52 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!((*self.source).plugin));
                    }
                    53 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).owner_read_config
                        ));
                    }
                    54 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).declared_read_memory
                        ));
                    }
                    55 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).owner_read_service
                        ));
                    }
                    56 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).owner_read_startup_failure
                        ));
                    }
                    57 => {
                        #[cfg(any(test, feature = "test-seams"))]
                        {
                            std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                                (*self.source).route_observer
                            ));
                        }
                    }
                    58 => {
                        #[cfg(any(test, feature = "test-seams"))]
                        {
                            std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                                (*self.source).kernel_observer
                            ));
                        }
                    }
                    59 => {
                        #[cfg(any(test, feature = "test-seams"))]
                        {
                            std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                                (*self.source).snapshot_free_retirement_pass_count
                            ));
                        }
                    }
                    60 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!((*self.source).access));
                    }
                    61 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!((*self.source).accountant));
                    }
                    62 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).conflict_policies
                        ));
                    }
                    63 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).subscriptions
                        ));
                    }
                    64 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).pruning_runtime
                        ));
                    }
                    65 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).pruning_guard
                        ));
                    }
                    66 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).retention_sync_peer
                        ));
                    }
                    67 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).in_memory_destination_reuploads
                        ));
                    }
                    68 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).lineage_state_lock
                        ));
                    }
                    69 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).received_schema_manifest_lock
                        ));
                    }
                    70 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).sync_push_apply_locks
                        ));
                    }
                    71 => {
                        #[cfg(feature = "sync-orchestration")]
                        {
                            std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                                (*self.source).sync_client_push_locks
                            ));
                        }
                    }
                    72 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).in_memory_received_schema_manifests
                        ));
                    }
                    73 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).in_memory_table_generations
                        ));
                    }
                    74 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).in_memory_ddl_generations
                        ));
                    }
                    75 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).received_ddl_arrivals
                        ));
                    }
                    76 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).superseded_ddl_occurrences
                        ));
                    }
                    77 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!((*self.source).cron));
                    }
                    78 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!((*self.source).event_bus));
                    }
                    79 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!((*self.source).trigger));
                    }
                    80 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).trigger_deadlock_timeout_override
                        ));
                    }
                    81 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).sync_relay_mode
                        ));
                    }
                    82 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).in_memory_sync_progress
                        ));
                    }
                    83 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).in_memory_applied_push_watermarks
                        ));
                    }
                    84 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).sync_incarnations
                        ));
                    }
                    85 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).custody_runtime
                        ));
                    }
                    86 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).custody_cache
                        ));
                    }
                    87 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).custody_metadata
                        ));
                    }
                    88 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).pending_event_bus_ddl
                        ));
                    }
                    89 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).pending_commit_metadata
                        ));
                    }
                    90 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).limit_update_lock
                        ));
                    }
                    91 => {
                        #[cfg(any(test, feature = "test-seams"))]
                        {
                            std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                                (*self.source).maintenance_poll_interval_update_pause
                            ));
                        }
                    }
                    92 => {
                        #[cfg(any(test, feature = "test-seams"))]
                        {
                            std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                                (*self.source).maintenance_poll_interval_update_probe
                            ));
                        }
                    }
                    93 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!((*self.source).disk_limit));
                    }
                    94 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).disk_limit_startup_ceiling
                        ));
                    }
                    95 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).trigger_audit_retention_secs
                        ));
                    }
                    96 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).snapshot_registry
                        ));
                    }
                    97 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).vector_partition_snapshot_stages
                        ));
                    }
                    98 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).retention_deferred_edge_nodes
                        ));
                    }
                    99 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).maintenance_caller_driven
                        ));
                    }
                    100 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).maintenance_poll_interval_ms
                        ));
                    }
                    101 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).vector_serving_cache_bytes
                        ));
                    }
                    102 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).last_maintenance_cycle_at
                        ));
                    }
                    103 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).caller_driven_backlog_warned_at
                        ));
                    }
                    104 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).last_auto_compact_at
                        ));
                    }
                    105 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).storage_compaction_policy
                        ));
                    }
                    106 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).sync_watermark
                        ));
                    }
                    107 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!((*self.source).closed));
                    }
                    108 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).close_progress
                        ));
                    }
                    109 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).close_changed
                        ));
                    }
                    110 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!((*self.source).close_error));
                    }
                    111 => {
                        #[cfg(any(test, feature = "test-seams"))]
                        {
                            std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                                (*self.source).close_follower_observer
                            ));
                        }
                    }
                    112 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).resource_closed
                        ));
                    }
                    113 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).publication_interrupted
                        ));
                    }
                    114 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).rows_examined
                        ));
                    }
                    115 => {
                        #[cfg(any(test, feature = "test-seams"))]
                        {
                            std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                                (*self.source).candidate_rows_materialized
                            ));
                        }
                    }
                    117 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).last_vector_search_trace
                        ));
                    }
                    118 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).statement_cache
                        ));
                    }
                    119 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).rank_formula_cache
                        ));
                    }
                    120 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).acl_grant_cache
                        ));
                    }
                    121 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).rank_policy_eval_count
                        ));
                    }
                    122 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).rank_policy_formula_parse_count
                        ));
                    }
                    123 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).fk_indexed_tuple_probes
                        ));
                    }
                    124 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).fk_full_scan_fallbacks
                        ));
                    }
                    125 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).commit_rows_validated
                        ));
                    }
                    126 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).commit_indexed_probes
                        ));
                    }
                    127 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).commit_staged_vs_staged_comparisons
                        ));
                    }
                    128 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).commit_scan_rows_touched
                        ));
                    }
                    129 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).commit_index_maintenance_visits
                        ));
                    }
                    130 => {
                        #[cfg(any(test, feature = "test-seams"))]
                        {
                            std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                                (*self.source).commit_stage_wall_nanos
                            ));
                        }
                    }
                    131 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).corrupt_joined_values
                        ));
                    }
                    132 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).resource_owner
                        ));
                    }
                    133 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!((*self.source).handle_role));
                    }
                    134 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).read_image_memory
                        ));
                    }
                    135 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).finalization
                        ));
                    }
                    136 => {
                        std::ptr::drop_in_place(std::ptr::addr_of_mut!(
                            (*self.source).retained_graph_source
                        ));
                    }
                    _ => unreachable!(),
                }
            }
        }
    }
}
impl Drop for DatabaseConstruction {
    fn drop(&mut self) {
        self.finish();
    }
}
impl DatabaseCleanup {
    pub(super) fn finish(&mut self) {
        let source = unsafe { &mut *self.source };
        if !std::mem::replace(&mut self.holder_notified, true)
            && source.handle_role.holds_the_store()
            && source.finalization.note_holder_released()
            && let Some(mut release) = source.finalization.take_pending()
        {
            if self.handed_over_here {
                release.plugin = None;
            }
            release_store(release);
        }
        // Each take disarms its field before child destruction. Remaining
        // payload fields, image custody, then finalization use Rust drop glue.
        drop(source.received_staging_access.take());
        drop(source.persistence.take());
        drop(source.committed_image_startup.take());
        drop(source.image_history.take());
        drop(source.retained_relational_source.take());
        let _ = source.image_store_file_bytes.take();
        drop(source.origin_session_tx.take());
        drop(source.owner_read_service.take());
        drop(source.owner_read_startup_failure.take());
        #[cfg(any(test, feature = "test-seams"))]
        drop(source.route_observer.take());
        #[cfg(any(test, feature = "test-seams"))]
        drop(source.kernel_observer.take());
    }
}
impl Drop for DatabaseCleanup {
    fn drop(&mut self) {
        self.finish();
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        let mut cleanup = DatabaseCleanup {
            source: self,
            holder_notified: false,
            handed_over_here: false,
        };

        // Owner reads stop admitting first. Work already accepted keeps its
        // store hold and finishes under its own operation limits; the last
        // holder releases the channel and the shared resources.
        //
        // A drop does NOT wait for that work to finish. Waiting is what close
        // is for: a caller who calls close asked for the wait and is told when
        // the deadline could not be met, while a drop has no way to report
        // that and no business blocking on a deadline whose clock the program
        // may never advance again. Whoever is still inside finishes, and the
        // last one out pays the release.
        let already_closed = if self.resource_owner {
            let _operation_barrier = self.operation_gate.write();
            self.closed.swap(true, Ordering::SeqCst)
        } else {
            // Rust has exclusive ownership of this handle at destruction.
            // Its readers retain their own Arc, so retiring a non-owning view
            // cannot race an operation through that view and must not wait
            // for unrelated work using the store's shared operation barrier.
            self.closed.swap(true, Ordering::SeqCst)
        };
        // A handle closed by hand has already done the work below, but it is
        // still one of the holders this store is waiting on, so it still
        // reports itself gone at the end.
        let mut handed_over_here = false;
        if !already_closed {
            if self.resource_owner {
                self.pending_local_schema_stages.lock().clear();
                self.local_schema_stages.lock().clear();
                // A handle that is dropped without `close` stops holding the
                // durable writer's own retention just as surely as one that
                // is closed by hand, so it hands back the same figure. An
                // embedder that reuses one accountant across opens saw the
                // same change log charged once per open otherwise.
                self.finalization
                    .accountant
                    .release(self.durable_writer_retained_bytes());
            }
            self.stop_cron_tickler();
            let event_bus_shutdown = self.stop_event_bus_threads_on_drop();
            let runtime = self.pruning_runtime.get_mut();
            if let Some(handle) = runtime.request_shutdown_and_take_handle() {
                let _ = handle.join();
            }
            // A drop inside a sink callback leaves the writer to that
            // dispatcher, which records its delivery and then publishes.
            if !event_bus_shutdown.deferred_resource_cleanup()
                && let Err(error) = self.publish_quiescent_owner()
            {
                tracing::warn!(error = %error, "could not publish quiescent owner status during drop");
            }
            let _stopped = self.stop_owner_reads_without_waiting();
            if self.resource_owner {
                self.blob_repository.close();
                self.subscriptions.lock().subscribers.clear();
                if !event_bus_shutdown.deferred_resource_cleanup() {
                    // What this owner acquired is ready to be let go -- but not
                    // necessarily by this handle. A reader that is mid-request or
                    // holding a suspended cursor is still looking at the store,
                    // and a second opener must keep being told the store is taken
                    // until that reader is done. So the owner hands the store over
                    // and whichever holder is last pays it out below.
                    //
                    // A read still running is exactly that case: its own hold
                    // keeps the store until the request is finished.
                    self.finalization.defer(PendingStoreRelease {
                        #[cfg(feature = "test-seams")]
                        release_completion_for_test: Arc::clone(
                            &self.finalization.release_completion_for_test,
                        ),
                        persistence: self.persistence.clone(),
                        resource_closed: Arc::clone(&self.resource_closed),
                        open_registry_path: self.open_registry_path.lock().take(),
                        owner_read_service: self.owner_read_service.as_ref().map(Arc::downgrade),
                        owner_read_status: self
                            .owner_read_service
                            .as_ref()
                            .map(|service| service.status()),
                        plugin: Some(Arc::clone(&self.plugin)),
                    });
                    handed_over_here = true;
                }
            }
        }
        if already_closed && self.resource_owner && !self.resource_closed.load(Ordering::SeqCst) {
            // An explicit close crossed the writer barrier but its drain
            // deadline expired. The accepted work still owns the store; hand
            // final release to its last holder when this handle drops.
            self.blob_repository.close();
            self.subscriptions.lock().subscribers.clear();
            let _stopped = self.stop_owner_reads_without_waiting();
            self.finalization.defer(PendingStoreRelease {
                #[cfg(feature = "test-seams")]
                release_completion_for_test: Arc::clone(
                    &self.finalization.release_completion_for_test,
                ),
                persistence: self.persistence.clone(),
                resource_closed: Arc::clone(&self.resource_closed),
                open_registry_path: self.open_registry_path.lock().take(),
                owner_read_service: self.owner_read_service.as_ref().map(Arc::downgrade),
                owner_read_status: self
                    .owner_read_service
                    .as_ref()
                    .map(|service| service.status()),
                plugin: Some(Arc::clone(&self.plugin)),
            });
        }
        cleanup.handed_over_here = handed_over_here;
        #[cfg(test)]
        R1_DATABASE_DROP_PANIC_FOR_TEST.with(|flag| {
            if flag.replace(false) {
                panic!("CDB127 injected single database drop unwind");
            }
        });
        cleanup.finish();
    }
}

pub(super) fn tuple_values_from_map(
    values_by_column: &HashMap<String, Value>,
    columns: &[String],
) -> Option<Vec<Value>> {
    let mut values = Vec::with_capacity(columns.len());
    for column in columns {
        match values_by_column.get(column) {
            Some(Value::Null) | None => return None,
            Some(value) => values.push(value.clone()),
        }
    }
    Some(values)
}

pub(super) fn tuple_values_for_row(row: &VersionedRow, columns: &[String]) -> Option<Vec<Value>> {
    tuple_values_from_map(&row.values, columns)
}

/// SQL reads materialize a NULL for a nullable column that was added after a
/// row was stored. Row guards use the same visible value at both registration
/// and commit revalidation, so an old row can be claimed once and a competing
/// update still makes that claim stale.
pub(super) fn row_conditions_match(
    row: &VersionedRow,
    meta: &TableMeta,
    predicates: &[(String, Value)],
) -> bool {
    predicates
        .iter()
        .all(|(column, expected)| match row.values.get(column) {
            Some(actual) => actual == expected,
            None => {
                matches!(expected, Value::Null)
                    && meta
                        .columns
                        .iter()
                        .any(|definition| definition.name == *column && definition.nullable)
            }
        })
}
impl RetentionCandidates {
    pub(super) fn contains(&self, table: &str, row: &VersionedRow) -> bool {
        self.tables
            .get(table)
            .is_some_and(|rows| rows.contains(&(row.row_id, row.created_tx)))
    }
    pub(super) fn insert(&mut self, table: &str, row: &VersionedRow) -> bool {
        self.tables
            .entry(table.to_owned())
            .or_default()
            .insert((row.row_id, row.created_tx))
    }
    pub(super) fn is_empty(&self) -> bool {
        self.tables.values().all(HashSet::is_empty)
    }
    pub(super) fn contains_physical(&self, table: &str, key: &[u8; 24]) -> bool {
        let row_id = RowId(u64::from_be_bytes(key[..8].try_into().expect("row id")));
        let created_tx = TxId(u64::from_be_bytes(key[8..16].try_into().expect("tx")));
        self.tables
            .get(table)
            .is_some_and(|rows| rows.contains(&(row_id, created_tx)))
    }
}

impl SyncParentKeyProjectionOwner {
    pub(super) fn new(owner: OwnedMemoryReservation) -> Self {
        #[cfg(feature = "in-process-test-seams")]
        crate::writer_query_test_seams::note_sync_parent_key_projection(owner.bytes(), true);
        Self(owner)
    }
}

impl Drop for SyncParentKeyProjectionOwner {
    fn drop(&mut self) {
        #[cfg(feature = "in-process-test-seams")]
        crate::writer_query_test_seams::note_sync_parent_key_projection(self.0.bytes(), false);
    }
}

pub(super) fn staged_tuple_exists(
    ws: &WriteSet,
    table: &str,
    columns: &[String],
    values: &[Value],
) -> bool {
    ws.relational_inserts.iter().any(|(insert_table, row)| {
        insert_table == table
            && columns
                .iter()
                .zip(values.iter())
                .all(|(column, value)| row.values.get(column) == Some(value))
    })
}

#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn prune_expired_rows(
    ctx: &MaintenanceContext,
    sync_watermark: Lsn,
    now: Wallclock,
    publication: &mut contextdb_tx::manager::MaintenancePublication<'_, DynStore>,
    mut workspace: crate::memory_accounting::OwnedMemoryReservation,
    prepaid_carry_bytes: usize,
    prepaid_durable_carry_bytes: usize,
    read_memory: impl Into<contextdb_core::read_memory::ReadMemoryOwner>,
) -> Result<PruningReport> {
    let read_memory: contextdb_core::read_memory::ReadMemoryOwner = read_memory.into();

    let relational_store = &ctx.relational;
    let graph_store = &ctx.graph;
    let vector_store = &ctx.vector;
    let accountant = ctx.finalization.accountant.as_ref();
    // Retention cleanup uses the same caller child for persistence decoding
    // as its other maintenance reads; never fall back to a fresh declaration.
    let _decode_scope = crate::read_image_memory::ImageMemory::operation_reader(
        crate::memory_accounting::AccountantHandle::clone(&ctx.finalization.accountant),
        read_memory.clone(),
    )?
    .enter();
    let maintenance_read_memory = read_memory.clone();
    let mut vector_copy_owner = crate::vector_read_memory::admission(read_memory.clone());
    let persistence = &ctx.persistence;
    let change_log = &ctx.change_log;
    let change_log_table_index = &ctx.change_log_table_index;
    let change_log_lsn_refcounts = &ctx.change_log_lsn_refcounts;
    let tx_mgr = ctx.tx_mgr.as_ref();
    let metas = relational_store.table_meta.read();

    let mut carried_edge_nodes = {
        let carried = ctx.retention_deferred_edge_nodes.lock();
        let bytes = carried.nodes.len().saturating_mul(128);
        if prepaid_carry_bytes > bytes {
            workspace.try_shrink(prepaid_carry_bytes - bytes)?;
        }
        workspace.try_grow_for(
            bytes.saturating_sub(prepaid_carry_bytes),
            "retention",
            "prepare_prune",
            "Raise MEMORY_LIMIT to admit reader-deferred retention identities.",
        )?;
        carried.nodes.clone()
    };
    {
        carried_edge_nodes.extend(persistence.load_retention_deferred_edge_nodes_prepaid(
            &mut workspace,
            prepaid_durable_carry_bytes,
        )?);
    }

    let mut pruned_versions_by_table: HashMap<String, Vec<(RowId, TxId)>> = HashMap::new();
    let mut fully_pruned_row_ids = HashSet::new();
    let mut pruned_node_ids = HashSet::new();
    // Grouped by table -- see `compact_currency_versions_inner`'s identical
    // choice.
    let mut pruned_change_keys: HashMap<String, HashSet<(RowId, Lsn)>> = HashMap::new();
    // The full relational key of every pruned version, in hand from the row
    // already being walked -- no scan is needed later to find it (see
    // `RedbPersistence::prune_versions_scoped`).
    let mut row_keys: Vec<(String, RowId, TxId, Lsn)> = Vec::new();
    // A row whose complete retained lifetime is pruned here loses its one
    // sync-source-lsn tracking row too -- looked up by key rather than the
    // table-wide scan `retain_sync_source_lsns_for_table_rows` used to need.
    let mut orphaned_source_lsn_rows: Vec<(String, RowId)> = Vec::new();
    let mut released_row_bytes = 0usize;
    // The physical bytes this pass reclaims (`released_row_bytes`) and the
    // bytes it must hand back to the accountant are not the same set. A
    // version carrying `deleted_tx` already had its charge released at commit,
    // so returning it again here would report less memory than is live.
    let mut accounted_row_bytes = 0usize;
    let mut blocked = Vec::new();

    // Every registered reader owns the versions visible at its snapshot,
    // including ordinary statements that have registered before their first
    // source pull. The committed expiry already hides these rows from newer
    // snapshots, so continuous reads do not keep extending their lifetime.
    let watermark = tx_mgr.current_tx_max();
    let active_removal_pass = ctx
        .snapshot_registry
        .begin_retention_removal_pass(watermark);
    ctx.snapshot_registry.removal_pass_test_pause.maybe_pause();

    // The caller owns the commit lock until publication. Keep a borrowed
    // frontier for FK probes and complete-lifetime checks, never a payload copy.
    let frontier = MaintenanceFrontier::capture(
        relational_store,
        tx_mgr,
        crate::persistence::writer_source::SourceWindowOwner::writer(
            crate::memory_accounting::AccountantHandle::clone(&ctx.finalization.accountant),
        ),
    )?
    .retention_batch(&metas, "prune", &mut workspace)?;
    let mut retention_progress = frontier.retention_progress().to_vec();
    let durable_source = frontier.is_durable();
    // A stamp further ahead than the tolerance can never age out on this
    // holder's clock, so it would pin its row silently. Count it per table,
    // every cycle, whether or not anything prunes.
    let skew_horizon = now
        .0
        .saturating_add(RETENTION_CLOCK_SKEW_TOLERANCE.as_millis() as u64);
    let mut future_dated_rows = 0u64;
    let mut future_dated_tables: Vec<String> = Vec::new();
    // Counted at the one place a row is actually held back, below.
    let mut rows_deferred_for_readers = 0u64;
    for (table_name, meta) in metas.iter() {
        if meta.default_ttl_seconds.is_none() {
            continue;
        }
        let (table_future_rows, progress) =
            frontier.future_stamp_count(table_name, meta, skew_horizon)?;
        if let Some(progress) = progress {
            retention_progress.push(progress);
        }
        future_dated_rows = future_dated_rows.saturating_add(table_future_rows);
        if table_future_rows > 0 {
            workspace.try_grow_for(
                256usize.saturating_add(table_name.len().saturating_mul(2)),
                "retention",
                "prepare_prune",
                "Raise MEMORY_LIMIT to admit the retention reclamation workspace.",
            )?;
            future_dated_tables.push(table_name.clone());
        }
    }
    future_dated_tables.sort();
    let prune_candidates = retention_prune_candidates(
        &metas,
        &frontier,
        now,
        sync_watermark,
        &mut workspace,
        RetentionPreparation::Prune,
    )?;
    let protected_prune_candidates =
        fk_protected_prune_candidates(&metas, &frontier, &prune_candidates)?;
    retention_progress.extend(
        frontier.cohort_progress(|table, key| {
            protected_prune_candidates.contains_physical(table, key)
        })?,
    );
    for (table_name, meta) in metas.iter() {
        if meta.default_ttl_seconds.is_none() {
            continue;
        }

        frontier.visit(table_name, |row| {
            #[cfg(any(test, feature = "test-seams"))]
            resident_history_test_seams::note(|work| work.retention_keys_examined += 1);
            if !prune_candidates.contains(table_name, row) {
                return Ok(true);
            }
            // An expired row a registered reader can still see is deferred,
            // never removed mid-read: a live expired row is visible at every
            // registered snapshot at or after its creation, and physically
            // removing it would both hide data a pinned snapshot is entitled
            // to see and shift the vector positions a suspended continuation
            // re-anchors against.
            if active_removal_pass.protects_history(row.lsn)
                || any_registered_snapshot_sees(active_removal_pass.registered_snapshots(), row)
            {
                rows_deferred_for_readers = rows_deferred_for_readers.saturating_add(1);
                return Ok(true);
            }
            if let Some(reason) = prune_blocker_for_referenced_parent(
                table_name,
                row,
                &metas,
                &frontier,
                &prune_candidates,
                &protected_prune_candidates,
            )? {
                blocked.push(reason);
                return Ok(true);
            }

            workspace.try_grow_for(
                2048usize.saturating_add(table_name.len().saturating_mul(16)),
                "retention",
                "prepare_prune",
                "Raise MEMORY_LIMIT to admit the retention reclamation workspace.",
            )?;
            // Expiry stamps its current row before this physical pass. That
            // exact stamped version expires this endpoint even when an older
            // version is still owed to a reader. Keep
            // graph reclamation independent of whole-row-history cleanup;
            // the edge's own snapshot visibility below decides deferral.
            if (row.deleted_tx.is_none() || frontier.was_retention_expired(table_name, row)?)
                && let Some(Value::Uuid(id)) = row.values.get("id")
            {
                pruned_node_ids.insert(*id);
            }
            pruned_versions_by_table
                .entry(table_name.clone())
                .or_default()
                .push((row.row_id, row.created_tx));
            // The change-log entries this row produced go with it. Collected
            // from the rows actually being pruned, so no watermark logic is
            // needed here: `row_is_prunable` already refuses to prune an
            // unconfirmed SYNC SAFE row, which means a pruned row is a
            // confirmed row and an unconfirmed backlog's entries are never in
            // this set. One rule, enforced at one place.
            pruned_change_keys
                .entry(table_name.clone())
                .or_default()
                .insert((row.row_id, row.lsn));
            row_keys.push((table_name.clone(), row.row_id, row.created_tx, row.lsn));
            let version_bytes = retained_row_bytes_for_meta(table_name, &row.values, meta, true);
            released_row_bytes = released_row_bytes.saturating_add(version_bytes);
            if !frontier.is_durable()
                && (row.deleted_tx.is_none()
                    || relational_store.is_retention_expired(
                        table_name,
                        row.row_id,
                        row.created_tx,
                    ))
            {
                // A version that was never deleted still carries the charge it
                // took at insert, so pruning it here is its ONE release. A
                // version carrying `deleted_tx` was released at commit by
                // `release_delete_allocations_from_bytes`; releasing it again
                // would take the same bytes back twice. This is the rule the
                // vector and edge reclaim below keep their own complete-row
                // lifecycle test; accounting must not decide data ownership.
                accounted_row_bytes = accounted_row_bytes.saturating_add(version_bytes);
            }
            Ok(true)
        })?;
    }

    let pruned_version_count = pruned_versions_by_table
        .values()
        .map(|versions| versions.len() as u64)
        .sum::<u64>();
    // Node ids this pass must judge adjacency for: the ones whose live row
    // it just reclaimed, plus the ones an earlier pass reclaimed a row for
    // and had to leave adjacency behind because a registered reader could
    // still see it. A pass that prunes no row at all still has to revisit
    // that carry-over -- the reader it was waiting on may be gone.
    if pruned_version_count == 0 && carried_edge_nodes.is_empty() && retention_progress.is_empty() {
        return Ok(PruningReport {
            pruned_rows: 0,
            blocked_count: blocked.len() as u64,
            blocked,
            future_dated_rows,
            future_dated_tables,
            rows_deferred_for_readers,
            ..PruningReport::default()
        });
    }

    let pruned_versions_by_table_sets = pruned_versions_by_table
        .iter()
        .map(|(table, versions)| {
            (
                table.clone(),
                versions.iter().copied().collect::<HashSet<_>>(),
            )
        })
        .collect::<HashMap<_, _>>();

    // Row memory was released when a delete stamped `deleted_tx`, but the
    // row's durable vector membership, body and provenance remain until the
    // retention pass removes its final version. Select those owners from the
    // complete row lifetime, independently of the accounting condition above.
    // If even one version survives (including one owed to a pinned reader),
    // retain every row-scoped owner until a later pass can remove them together.
    for (table_name, pruned_versions) in &pruned_versions_by_table_sets {
        let candidate_row_ids = pruned_versions
            .iter()
            .map(|(row_id, _)| *row_id)
            .collect::<HashSet<_>>();
        for row_id in candidate_row_ids {
            let mut survives = false;
            frontier.visit_row(table_name, row_id, |row| {
                survives = !pruned_versions.contains(&(row.row_id, row.created_tx));
                Ok(!survives)
            })?;
            if survives {
                continue;
            }
            fully_pruned_row_ids.insert(row_id);
            orphaned_source_lsn_rows.push((table_name.clone(), row_id));
            frontier.visit_row(table_name, row_id, |row| {
                if let Some(Value::Uuid(id)) = row.values.get("id") {
                    pruned_node_ids.insert(*id);
                }
                Ok(true)
            })?;
        }
    }

    // Physical publication rebuilds the selected tables' position maps and
    // retained index postings. Admit their simultaneous old/new images now,
    // while the frontier is borrowed and before any durable removal. Count
    // only surviving keys and identity scaffolding, never unrelated payloads.
    for (table, versions) in &pruned_versions_by_table_sets {
        if frontier.is_durable() {
            continue;
        }
        let Some(meta) = metas.get(table) else {
            continue;
        };
        let mut rebuild_bytes = meta
            .indexes
            .iter()
            .fold(0usize, |bytes, index| {
                bytes.saturating_add(index.estimated_bytes().saturating_mul(2))
            })
            .saturating_add(meta.exact_probe_filter_bytes());
        frontier.visit(table, |row| {
            #[cfg(any(test, feature = "test-seams"))]
            resident_history_test_seams::note(|work| work.retention_keys_examined += 1);
            if versions.contains(&(row.row_id, row.created_tx)) {
                return Ok(true);
            }
            // Hash buckets, both position maps, per-version vectors, the
            // row-id set, sorting references/scratch and transient keys.
            rebuild_bytes = rebuild_bytes
                .saturating_add(1024)
                .saturating_add(
                    meta.columns
                        .iter()
                        .filter(|column| matches!(column.column_type, ColumnType::Vector(_)))
                        .count()
                        .saturating_mul(1024),
                )
                .saturating_add(table.len().saturating_mul(8))
                .saturating_add(
                    contextdb_core::retained_index_posting_bytes_for_meta(&row.values, meta)
                        .saturating_mul(4),
                );
            Ok(true)
        })?;
        workspace.try_grow_for(
            rebuild_bytes,
            "retention",
            "prepare_prune",
            "Raise MEMORY_LIMIT to rebuild the selected table's retained index identities.",
        )?;
    }

    drop(frontier);

    // The vectors and edges being removed, not the wholesale survivor set the
    // old full-rewrite path needed. A fully removed row contributes every
    // retained vector version, including tombstoned versions that no longer
    // own relational memory.
    let mut removed_vectors = Vec::new();
    if !fully_pruned_row_ids.is_empty() {
        for (table, meta) in metas.iter() {
            if !pruned_versions_by_table.contains_key(table) {
                continue;
            }
            for column in &meta.columns {
                if !matches!(column.column_type, ColumnType::Vector(_)) {
                    continue;
                }
                let index = VectorIndexRef::new(table, &column.name);
                vector_store.visit_directory_versions_for_rows(
                    &index,
                    &fully_pruned_row_ids,
                    |entry| {
                        workspace.try_grow_for(
                            512usize
                                .saturating_add(index.table.len().saturating_mul(4))
                                .saturating_add(index.column.len().saturating_mul(4)),
                            "retention",
                            "prepare_prune",
                            "Raise MEMORY_LIMIT to admit the retention vector identities.",
                        )?;
                        removed_vectors.push(contextdb_vector::VectorVersionIdentity {
                            index: index.clone(),
                            row_id: entry.row_id,
                            created_tx: entry.created_tx,
                            lsn: entry.lsn,
                        });
                        Ok(())
                    },
                    &mut vector_copy_owner,
                )?;
            }
        }
    }
    let removed_vector_indexes = removed_vectors
        .iter()
        .map(|entry| entry.index.clone())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let mut reclaimable_edge_nodes = pruned_node_ids;
    reclaimable_edge_nodes.extend(carried_edge_nodes.iter().copied());
    // An adjacency entry is judged by the EDGE's own visibility, never by
    // the node row that happens to sit at one of its ends: an edge committed
    // before a registered snapshot is owed to that reader whether or not the
    // id it points at still carries a row. An entry a registered snapshot
    // can still see is left in place and revisited on a later pass, exactly
    // as an expired row a reader can still see is.
    let edge_is_reclaimable = |entry: &AdjEntry| -> bool {
        (reclaimable_edge_nodes.contains(&entry.source)
            || reclaimable_edge_nodes.contains(&entry.target))
            && !active_removal_pass.protects_history(entry.lsn)
            && !any_registered_snapshot_sees_edge(active_removal_pass.registered_snapshots(), entry)
    };
    let graph_root = writer_read::durable_root(tx_mgr.capture_published_read().handle)?;
    // A carried endpoint id is only a retry key. It cannot retire a new owner
    // life that was created after that endpoint's expiry transaction.
    let is_expiry_tombstone = |entry: &AdjEntry,
                               owner: &contextdb_tx::RowEdgeOwner|
     -> Result<bool> {
        if entry.deleted_tx.is_none() {
            return Ok(false);
        }
        let image = crate::read_image_memory::ImageMemory::charging_reader(
            crate::memory_accounting::AccountantHandle::clone(&ctx.finalization.accountant),
            maintenance_read_memory.clone(),
        );
        image.scope(|| -> Result<bool> {
            let key = crate::persistence::row_graph::retirement_key(owner)?;
            if let Some(root) = &graph_root {
                let mut cursor = root.lineage_cursor_from(
                    crate::persistence::writer_source::SourceWindowOwner::Read(
                        maintenance_read_memory.clone(),
                    ),
                    &key,
                    &key,
                )?;
                Ok(cursor
                    .next(|stored_key, bytes| {
                        if stored_key != key.as_bytes() {
                            return Ok(false);
                        }
                        let record = crate::persistence::row_graph::decode_retirement(bytes)?;
                        if record.owned.owner != *owner || record.owned.entry != *entry {
                            return Err(Error::StoreCorrupted {
                                path: "row-derived adjacency".into(),
                                reason: "retention retry does not match the exact expiry tombstone"
                                    .into(),
                            });
                        }
                        Ok(true)
                    })?
                    .is_some_and(|window| window.value))
            } else {
                let carry = ctx.retention_deferred_edge_nodes.lock();
                let Some(record) = carry.retired.get(&key) else {
                    return Ok(false);
                };
                if record.owned.owner != *owner || record.owned.entry != *entry {
                    return Err(Error::StoreCorrupted {
                        path: "row-derived adjacency".into(),
                        reason: "memory retention retry does not match exact expiry tombstone"
                            .into(),
                    });
                }
                Ok(true)
            }
        })
    };
    let mut removed_edges = Vec::new();
    let mut removed_owned_edges = Vec::new();
    if !reclaimable_edge_nodes.is_empty() {
        writer_read::visit_graph_versions_for_nodes_with_owner(
            graph_store,
            graph_root.clone(),
            &reclaimable_edge_nodes,
            |entry, owner| {
                if let Some(owner) = owner {
                    if !is_expiry_tombstone(entry, owner)? {
                        return Ok(());
                    }
                }
                if !edge_is_reclaimable(entry) {
                    return Ok(());
                }
                workspace.try_grow_for(
                    1024usize.saturating_add(entry.edge_type.len().saturating_mul(4)),
                    "retention",
                    "prepare_prune",
                    "Raise MEMORY_LIMIT to admit the retention edge identities.",
                )?;
                // Persistence removes by identity; properties are not needed and
                // must never be cloned just to delete the edge that owns them.
                let selected = AdjEntry {
                    source: entry.source,
                    target: entry.target,
                    edge_type: entry.edge_type.clone(),
                    properties: HashMap::new(),
                    created_tx: entry.created_tx,
                    deleted_tx: entry.deleted_tx,
                    lsn: entry.lsn,
                };
                if let Some(owner) = owner {
                    workspace.try_grow_for(
                        owner.estimated_bytes(),
                        "retention",
                        "prepare_prune",
                        "Raise MEMORY_LIMIT to retain exact row-owned retirement coordinates.",
                    )?;
                    removed_owned_edges.push(contextdb_tx::OwnedAdjEntry {
                        owner: owner.clone(),
                        entry: selected,
                    });
                } else {
                    removed_edges.push(selected);
                }
                Ok(())
            },
            maintenance_read_memory.clone(),
        )?;
    }

    if !removed_edges.is_empty() {
        // Identity repair for a removed edge can point-read an older copy
        // still owed to a reader. Admit that body before the durable callback.
        let identities = removed_edges
            .iter()
            .map(|entry| (entry.source, entry.edge_type.as_str(), entry.target))
            .collect::<HashSet<_>>();
        writer_read::visit_graph_versions_for_nodes(
            graph_store,
            graph_root.clone(),
            &reclaimable_edge_nodes,
            |entry| {
                if edge_is_reclaimable(entry)
                    || !identities.contains(&(entry.source, entry.edge_type.as_str(), entry.target))
                {
                    return Ok(());
                }
                workspace.try_grow_for(
                    entry.estimated_bytes().saturating_mul(8),
                    "retention",
                    "prepare_prune",
                    "Raise MEMORY_LIMIT to repair retained edge identities.",
                )?;
                Ok(())
            },
            maintenance_read_memory.clone(),
        )?;
    }

    let mut deferred_edge_nodes = HashSet::new();
    let mut deferred_edge_memory = crate::memory_accounting::OwnedMemoryReservation::new(
        crate::memory_accounting::AccountantHandle::clone(&ctx.finalization.accountant),
    );
    {
        writer_read::visit_graph_versions_for_nodes_with_owner(
            graph_store,
            graph_root.clone(),
            &reclaimable_edge_nodes,
            |entry, owner| {
                if let Some(owner) = owner {
                    if !is_expiry_tombstone(entry, owner)? {
                        return Ok(());
                    }
                }
                if edge_is_reclaimable(entry) {
                    return Ok(());
                }
                for node in [entry.source, entry.target]
                    .into_iter()
                    .filter(|node| reclaimable_edge_nodes.contains(node))
                {
                    if deferred_edge_nodes.contains(&node) {
                        continue;
                    }
                    // The request holds the serialization/B-tree copies; the next
                    // published carry state retains its own hash-bucket charge.
                    workspace.try_grow_for(
                        128,
                        "retention",
                        "prepare_prune",
                        "Raise MEMORY_LIMIT to encode reader-deferred retention identities.",
                    )?;
                    deferred_edge_memory.try_grow_for(
                        128,
                        "retention",
                        "prepare_prune",
                        "Raise MEMORY_LIMIT to retain reader-deferred retention identities.",
                    )?;
                    deferred_edge_nodes.insert(node);
                }
                Ok(())
            },
            maintenance_read_memory.clone(),
        )?;
    }

    // Bounded to the tables THIS pass touches, never the whole change log
    // -- matching the cleanup path's own fix (`count_pruned_change_log_
    // entries`/`change_log_table_index`). Persist-then-mutate: nothing here
    // mutates `change_log` itself; a failed persisted removal below must
    // never leave memory ahead of what is durably on disk.
    let pruned_change_keys_vec: Vec<(String, RowId, Lsn)> = pruned_change_keys
        .iter()
        .flat_map(|(table, keys)| {
            keys.iter()
                .map(move |(row_id, lsn)| (table.clone(), *row_id, *lsn))
        })
        .collect();
    let (_pruned_change_log_entries, removed_lsn_counts) = {
        if let Some(root) = writer_read::durable_root(tx_mgr.capture_published_read().handle)? {
            RedbPersistence::pruned_change_counts_at_root(
                &root,
                crate::persistence::writer_source::SourceWindowOwner::writer(
                    crate::memory_accounting::AccountantHandle::clone(&ctx.finalization.accountant),
                ),
                &pruned_change_keys,
            )?
        } else {
            let table_index = change_log_table_index.read();
            count_pruned_change_log_entries(&table_index, &pruned_change_keys)
        }
    };

    let membership_deletes = orphaned_source_lsn_rows
        .iter()
        .map(|(table, row)| (table.clone(), *row, watermark))
        .collect::<Vec<_>>();
    let memberships = relational_store.prepare_memberships(
        &membership_deletes,
        &[],
        ctx.finalization.accountant.read_budget()?,
    )?;

    // Commit-index candidate-only removal, shared with the cleanup path —
    // see the function doc. Over-retention remains the deliberate safe
    // direction: a candidate not yet proven safe by `scoped_commit_index_
    // removal`'s own floor computation simply is not removed.
    let mut affected_lsns: HashSet<Lsn> = removed_lsn_counts.keys().copied().collect();
    affected_lsns
        .extend(relational_store.reclaimed_retention_expiry_lsns(&pruned_versions_by_table_sets));
    // A durable writer keeps each expiry identity only in its root. The
    // expiry commits of the versions pruned here are candidates too; the
    // root-side coverage check keeps any whose change log still has entries.
    if let Some(root) = writer_read::durable_root(tx_mgr.capture_published_read().handle)? {
        for (table_name, versions) in &pruned_versions_by_table_sets {
            for (row_id, created_tx) in versions {
                if let Some(identity) = root.config_point::<(String, RowId, TxId, Lsn)>(
                    crate::persistence::writer_source::SourceWindowOwner::Read(read_memory.clone()),
                    &crate::composite_store::retention_expiry_key(table_name, *row_id, *created_tx),
                )? {
                    affected_lsns.insert(identity.value.3);
                }
            }
        }
    }
    let commit_index_candidates = {
        let lsn_refcounts = change_log_lsn_refcounts.read();
        scoped_commit_index_candidates(tx_mgr, &lsn_refcounts, &removed_lsn_counts, &affected_lsns)?
    };

    // Capture candidate membership before the same durable transaction removes
    // it. The file directory has no second resident map to remove afterward.
    let durable_directory_removals =
        commit_index_candidates
            .iter()
            .try_fold(0u64, |count, lsn| {
                tx_mgr
                    .try_commit_index_contains(*lsn)
                    .map(|present| count + u64::from(present))
            })?;

    let schema_history = relational_store.prepare_schema_history_erasure(
        row_keys.iter().map(|(_, row, _, lsn)| (*row, *lsn)),
        ctx.finalization.accountant.read_budget()?,
    )?;
    let publication_watch;
    {
        publication.reserve()?;
        let watch = PublicationInterruptionWatch::arm(&ctx.publication_interrupted);
        // Point-removes in ONE redb write transaction: exactly the pruned
        // row versions, the change-log entries that referenced them
        // (change-log-first WITHIN that transaction, exactly as the cleanup
        // path's fix), and the sync-source-lsn rows orphaned by a row whose
        // whole lifetime just ended. Nothing else is read or rewritten.
        // The same commit removes vectors and adjacency and retains retry
        // identities for edges still owed to registered readers, including a
        // pass that only revisits adjacency after its row is already gone.
        let persisted = persistence.prune_retention_with_owned_scoped(
            &metas,
            &row_keys,
            &pruned_change_keys_vec,
            &orphaned_source_lsn_rows,
            Some((
                &removed_vectors,
                &removed_edges,
                &removed_owned_edges,
                &deferred_edge_nodes,
            )),
            &commit_index_candidates,
            &retention_progress,
        );
        if let Err(error) = persisted {
            watch.completed();
            return Err(error);
        }
        publication_watch = Some(watch);
    }

    // Only now, with every persisted step behind us, does memory follow —
    // scoped to a suffix of `change_log` (see
    // `remove_pruned_change_log_entries`), never the whole Vec.
    let released_log_bytes = if let Some(min_affected_lsn) = affected_lsns.iter().min().copied() {
        remove_pruned_change_log_entries(
            accountant,
            &mut change_log.write(),
            &pruned_change_keys,
            min_affected_lsn,
        )
    } else {
        0
    };
    remove_pruned_from_table_index(&mut change_log_table_index.write(), &pruned_change_keys);
    apply_removed_lsn_counts(&mut change_log_lsn_refcounts.write(), &removed_lsn_counts);
    // The REAL count: `tx_mgr`'s own in-memory removal, not the persisted
    // side effect's count (which reads 0 on an in-memory database even
    // though the in-memory index genuinely shrinks -- see
    // `scoped_commit_index_removal`'s own doc comment).
    let pruned_commit_index_entries = if commit_index_candidates.is_empty() {
        0
    } else {
        durable_directory_removals
    };

    relational_store.publish_schema_history_erasure(schema_history);
    for (table_name, versions) in &pruned_versions_by_table_sets {
        relational_store.remove_row_versions(table_name, versions);
    }

    relational_store.publish_memberships(memberships);

    for table_name in pruned_versions_by_table.keys() {
        if let Some(meta) = metas.get(table_name) {
            for index in &meta.indexes {
                relational_store.rebuild_index_preserving_current(table_name, &index.name);
            }
        }
    }

    let released_vector_bytes = if fully_pruned_row_ids.is_empty() {
        0
    } else {
        vector_store.prune_row_ids_in_indexes(
            &fully_pruned_row_ids,
            &removed_vector_indexes,
            &mut vector_copy_owner,
        )?
    };

    let mut released_edge_bytes = 0usize;
    // Every adjacency entry this pass removes is charged until it is removed,
    // whether it was stamped `deleted_tx` by an earlier commit or was still
    // live at expiry: a tombstoned entry is still whole and still in both
    // adjacency maps, so its bytes are still held. This pass is the one
    // release, and it happens where the entry actually goes away.
    let mut accounted_edge_bytes = 0usize;
    // Ids whose adjacency this pass had to leave behind, so the next pass
    // knows to come back to them. Rebuilt from what is actually still in
    // place, so an id whose entries are all gone stops being carried.
    {
        let mut forward = graph_store.forward_adj.write();
        for entries in forward.values_mut() {
            entries.retain(|entry| {
                if entry.row_owner.as_ref().map_or_else(
                    || edge_is_reclaimable(entry),
                    |owner| {
                        removed_owned_edges
                            .iter()
                            .any(|selected| selected.owner == *owner)
                    },
                ) {
                    released_edge_bytes =
                        released_edge_bytes.saturating_add(entry.estimated_bytes());
                    accounted_edge_bytes =
                        accounted_edge_bytes.saturating_add(entry.estimated_bytes());
                    false
                } else {
                    true
                }
            });
        }
        forward.retain(|_, entries| !entries.is_empty());
    }
    {
        let mut reverse = graph_store.reverse_adj.write();
        for entries in reverse.values_mut() {
            entries.retain(|entry| {
                !entry.row_owner.as_ref().map_or_else(
                    || edge_is_reclaimable(entry),
                    |owner| {
                        removed_owned_edges
                            .iter()
                            .any(|selected| selected.owner == *owner)
                    },
                )
            });
        }
        reverse.retain(|_, entries| !entries.is_empty());
    }
    // A deferred entry is still charged and still physically present; it is
    // released and handed back to the accountant on the pass that finally
    // removes it, which is the same one release every other entry gets.
    {
        let mut carry = ctx.retention_deferred_edge_nodes.lock();
        carry.nodes = deferred_edge_nodes;
        carry._memory = Some(deferred_edge_memory);
    }

    // Reclaimed physical bytes are reported in full below; only the versions
    // that still held a charge are returned to the accountant.
    accountant.release(
        accounted_row_bytes
            .saturating_add(released_vector_bytes)
            .saturating_add(accounted_edge_bytes),
    );

    let report = PruningReport {
        pruned_rows: pruned_version_count,
        blocked_count: blocked.len() as u64,
        blocked,
        reclaimed_bytes: (if durable_source {
            accounted_row_bytes.saturating_add(released_log_bytes)
        } else {
            released_row_bytes
        })
        .saturating_add(released_vector_bytes)
        .saturating_add(released_edge_bytes) as u64,
        future_dated_rows,
        future_dated_tables,
        pruned_commit_index_entries,
        rows_deferred_for_readers,
        ..PruningReport::default()
    };
    if let Some(watch) = publication_watch {
        publication.complete();
        watch.completed();
    }
    // The removal marker remains live through the physical-root swap.
    drop(active_removal_pass);
    Ok(report)
}

// Retention owns its catalog/key/reader-chain buffers until publication.

/// Select without mutating either durable or in-memory state. Currency cleanup
/// includes these candidates in the same transaction as their last references.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn scoped_commit_index_candidates(
    tx_mgr: &DatabaseTransactionHost,
    lsn_refcounts: &ChangeLogLsnRefcounts,
    removed_lsn_counts: &HashMap<Lsn, u64>,
    affected_lsns: &HashSet<Lsn>,
) -> Result<Vec<Lsn>> {
    if affected_lsns.is_empty() {
        return Ok(Vec::new());
    }
    if let Some(root) = writer_read::durable_root(tx_mgr.capture_published_read().handle)? {
        return RedbPersistence::commit_candidates_at_root(
            &root,
            |upper| tx_mgr.try_commit_index_floor(upper),
            removed_lsn_counts,
            affected_lsns,
        );
    }
    let still_covered = |lsn: Lsn| -> bool {
        lsn_refcounts.get(&lsn).copied().unwrap_or(0)
            > removed_lsn_counts.get(&lsn).copied().unwrap_or(0)
    };
    let floor_after = match lsn_refcounts.keys().next().copied() {
        Some(current_floor) if still_covered(current_floor) => Some(current_floor),
        Some(current_floor) => lsn_refcounts
            .range(current_floor..)
            .find(|(lsn, _)| still_covered(**lsn))
            .map(|(lsn, _)| *lsn),
        None => None,
    };
    let anchor_lsn = match floor_after {
        Some(floor) => tx_mgr
            .try_commit_index_floor(std::ops::Bound::Excluded(floor))?
            .map(|(lsn, _)| lsn),
        None => tx_mgr
            .try_commit_index_floor(std::ops::Bound::Unbounded)?
            .map(|(lsn, _)| lsn),
    };
    Ok(affected_lsns
        .iter()
        .filter(|lsn| !still_covered(**lsn) && Some(**lsn) != anchor_lsn)
        .copied()
        .collect())
}

/// Whether a change-log entry names one of the pruned `(table, row_id, lsn)`
/// versions, so it must be dropped in lockstep. Only row entries can reference a
/// relational version; edge/vector entries never do.
pub(super) fn change_entry_references_pruned_version(
    entry: &ChangeLogEntry,
    pruned_by_table: &HashMap<String, HashSet<(RowId, Lsn)>>,
) -> bool {
    match entry {
        ChangeLogEntry::RowInsert {
            table, row_id, lsn, ..
        }
        | ChangeLogEntry::RowDelete {
            table, row_id, lsn, ..
        } => pruned_by_table
            .get(table)
            .is_some_and(|set| set.contains(&(*row_id, *lsn))),
        _ => false,
    }
}

/// Build both change-log aux structures from a freshly loaded change log at
/// `Database::open` — a one-time O(log length) cost at startup (never
/// per-cycle), reusing the SAME per-entry logic
/// `CompositeStore::apply_exact_with_log_entries` uses at append, so the two
/// choke points can never disagree about which entries populate the index.
pub(super) fn build_change_log_aux_indexes(
    log: &[ChangeLogEntry],
) -> (ChangeLogTableIndex, ChangeLogLsnRefcounts) {
    let mut table_index = ChangeLogTableIndex::new();
    let mut lsn_refcounts = ChangeLogLsnRefcounts::new();
    record_change_log_entries(&mut table_index, &mut lsn_refcounts, log);
    (table_index, lsn_refcounts)
}

/// Build both change-log aux structures for a reader, admitting each of their
/// allocations to `memory` before it is made, so a reader whose room holds
/// the history but not its indexes is refused before the allocation that
/// would not fit, and what is built is charged at what it occupies: a table's
/// key is its name's own bytes, the table map is charged at its bucket
/// array each time that array grows, a table's list at its buffer each time
/// that buffer grows (the array or buffer a growth replaces given back once
/// it is gone), and every distinct LSN one tree entry.
///
/// A refusal drops what was built so far; `memory`'s owner gives its charge
/// back with it.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn build_admitted_change_log_aux_indexes(
    log: &[ChangeLogEntry],
    memory: &crate::read_image_memory::ImageMemory,
) -> Result<(ChangeLogTableIndex, ChangeLogLsnRefcounts)> {
    const LIST_ENTRY_BYTES: usize = std::mem::size_of::<(Lsn, RowId)>();
    const TABLE_SLOT_BYTES: usize = std::mem::size_of::<(TableName, Vec<(Lsn, RowId)>)>() + 1;
    let bucket_array_bytes =
        |buckets: usize| buckets.saturating_mul(TABLE_SLOT_BYTES) + 16 + held::ALLOCATION_BYTES;
    let list_bytes =
        |capacity: usize| capacity.saturating_mul(LIST_ENTRY_BYTES) + held::ALLOCATION_BYTES;
    let mut table_index = ChangeLogTableIndex::new();
    let mut lsn_refcounts = ChangeLogLsnRefcounts::new();
    let mut table_buckets = 0usize;
    for entry in log {
        let lsn = entry.lsn();
        match lsn_refcounts.get_mut(&lsn) {
            Some(count) => *count += 1,
            None => {
                memory.reserve(LSN_REFCOUNT_ENTRY_UPPER_BYTES)?;
                lsn_refcounts.insert(lsn, 1);
            }
        }
        let (ChangeLogEntry::RowInsert { table, row_id, lsn }
        | ChangeLogEntry::RowDelete {
            table, row_id, lsn, ..
        }) = entry
        else {
            continue;
        };
        if !table_index.contains_key(table) {
            if table_index.len() == table_index.capacity() {
                let wanted = table_index.capacity().saturating_mul(2).max(4);
                let buckets = wanted
                    .saturating_mul(8)
                    .div_ceil(7)
                    .next_power_of_two()
                    .max(4);
                memory.reserve(bucket_array_bytes(buckets))?;
                table_index.reserve(wanted - table_index.len());
                if table_buckets > 0 {
                    memory.release(bucket_array_bytes(table_buckets));
                }
                table_buckets = buckets;
            }
            memory.reserve(held::text(table))?;
            table_index.insert(table.clone(), Vec::new());
        }
        let list = table_index
            .get_mut(table)
            .expect("the table's list was just ensured");
        if list.len() == list.capacity() {
            let previous = list.capacity();
            let grown = previous.saturating_mul(2).max(4);
            memory.reserve(list_bytes(grown))?;
            list.reserve_exact(grown - list.len());
            if previous > 0 {
                memory.release(list_bytes(previous));
            }
        }
        list.push((*lsn, *row_id));
    }
    Ok((table_index, lsn_refcounts))
}

#[cfg(any(test, feature = "test-seams"))]
impl ChangeLogAuxIndexFixtureForTest {
    pub fn new(tables: usize, name_len: usize, rows_per_table: usize) -> Self {
        let mut log = Vec::with_capacity(tables.saturating_mul(rows_per_table));
        let mut lsn = 0u64;
        for row in 0..rows_per_table {
            for table in 0..tables {
                lsn += 1;
                let mut name = format!("t{table:08}_");
                let pad = name_len.saturating_sub(name.len());
                name.extend(std::iter::repeat_n('n', pad));
                log.push(ChangeLogEntry::RowInsert {
                    table: name,
                    row_id: RowId((table * rows_per_table + row) as u64 + 1),
                    lsn: Lsn(lsn),
                });
            }
        }
        Self {
            log,
            memory: crate::read_image_memory::ImageMemory::new(None)
                .expect("unlimited change-log fixture image owner"),
            built: None,
        }
    }

    /// Build the aux structures under admission and keep them; answers what
    /// they were charged.
    pub fn build(&mut self) -> usize {
        self.built = Some(
            build_admitted_change_log_aux_indexes(&self.log, &self.memory)
                .expect("an unlimited reader admits every aux index allocation"),
        );
        self.memory.held()
    }
}

pub(super) fn count_pruned_change_log_entries(
    change_log_table_index: &ChangeLogTableIndex,
    pruned_by_table: &HashMap<String, HashSet<(RowId, Lsn)>>,
) -> (u64, HashMap<Lsn, u64>) {
    let mut total = 0u64;
    let mut removed_lsn_counts: HashMap<Lsn, u64> = HashMap::new();
    for (table, pruned_set) in pruned_by_table {
        let Some(coords) = change_log_table_index.get(table) else {
            continue;
        };
        for (lsn, row_id) in coords {
            if pruned_set.contains(&(*row_id, *lsn)) {
                total += 1;
                *removed_lsn_counts.entry(*lsn).or_insert(0) += 1;
            }
        }
    }
    (total, removed_lsn_counts)
}

/// Physically remove every change-log entry naming a pruned version from
/// `change_log`'s Vec, starting the scan at the lowest LSN any pruned
/// version could possibly touch rather than at position zero. `change_log`
/// is APPENDED in strictly non-decreasing LSN order (one writer, one
/// commit lock, entries pushed in commit order — see
/// `CompositeStore::apply_exact_with_log_entries`), so a binary search
/// finds exactly where "could possibly match" begins, and everything
/// before it is provably untouched: never read, never moved. This is what
/// makes a scoped pass's physical removal track "how much of the log came
/// at or after what THIS pass is cleaning," not "how big the database's
/// unrelated history is" — an unrelated table's writes that happened
/// BEFORE the earliest pruned version, however many, cost nothing here.
/// Order-preserving (matches `Vec::retain`'s own guarantee), just scoped to
/// a suffix instead of the whole Vec.
pub(super) fn remove_pruned_change_log_entries(
    accountant: &MemoryAccountant,
    change_log: &mut Vec<ChangeLogEntry>,
    pruned_by_table: &HashMap<String, HashSet<(RowId, Lsn)>>,
    min_affected_lsn: Lsn,
) -> usize {
    // The log is charged per entry when it is appended, so a pass that
    // shortens it gives back exactly the entries it removed.
    let mut released = 0usize;
    let start = change_log.partition_point(|entry| entry.lsn() < min_affected_lsn);
    let mut write = start;
    for read in start..change_log.len() {
        if change_entry_references_pruned_version(&change_log[read], pruned_by_table) {
            released = released.saturating_add(
                crate::composite_store::change_log_entry_retained_bytes(&change_log[read]),
            );
            continue;
        }
        if write != read {
            change_log.swap(write, read);
        }
        write += 1;
    }
    change_log.truncate(write);
    accountant.release(released);
    released
}

/// Drop the pruned `(lsn, row_id)` coordinates from the affected tables'
/// own shadow — bounded to those tables' own entries, never the whole
/// index.
pub(super) fn remove_pruned_from_table_index(
    change_log_table_index: &mut ChangeLogTableIndex,
    pruned_by_table: &HashMap<String, HashSet<(RowId, Lsn)>>,
) {
    for (table, pruned_set) in pruned_by_table {
        if let Some(coords) = change_log_table_index.get_mut(table) {
            coords.retain(|(lsn, row_id)| !pruned_set.contains(&(*row_id, *lsn)));
        }
    }
}

/// Decrement the global LSN refcount by exactly what this pass removed —
/// bounded to `removed_lsn_counts` (the LSNs THIS pass touched), never a
/// scan of the whole map. A count reaching zero means the change log no
/// longer covers that LSN at all, so the key is dropped rather than left
/// at zero (an absent key IS "not covered," matching
/// `scoped_commit_index_removal`'s `still_covered` check).
pub(super) fn apply_removed_lsn_counts(
    lsn_refcounts: &mut ChangeLogLsnRefcounts,
    removed_lsn_counts: &HashMap<Lsn, u64>,
) {
    for (lsn, removed) in removed_lsn_counts {
        if let std::collections::btree_map::Entry::Occupied(mut slot) = lsn_refcounts.entry(*lsn) {
            let remaining = slot.get().saturating_sub(*removed);
            if remaining == 0 {
                slot.remove();
            } else {
                *slot.get_mut() = remaining;
            }
        }
    }
}

// Native allocation plan for a fresh TableMeta clone. Inline fields live in
// the selected vector's already-priced slots; only nested backing is counted.
// Vec/String clones allocate their initialized length. HashMap::clone keeps
// its native bucket capacity, including spare buckets in a sparse source.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn cleanup_declaration_copy_bytes(meta: &TableMeta) -> Result<usize> {
    struct Plan(usize);
    impl Plan {
        #[expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )]
        fn add(&mut self, bytes: usize) -> Result<()> {
            self.0 = self
                .0
                .checked_add(bytes)
                .ok_or_else(EngineMetadataJournal::identity_error)?;
            Ok(())
        }
        #[expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )]
        fn array<T>(&mut self, values: &[T]) -> Result<()> {
            self.add(
                std::alloc::Layout::array::<T>(values.len())
                    .map_err(|_| EngineMetadataJournal::identity_error())?
                    .size(),
            )
        }
        #[expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )]
        fn text(&mut self, value: &str) -> Result<()> {
            self.add(value.len())
        }
        #[expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )]
        fn optional_text(&mut self, value: &Option<String>) -> Result<()> {
            if let Some(value) = value {
                self.text(value)?;
            }
            Ok(())
        }
        #[expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )]
        fn names(&mut self, values: &[String]) -> Result<()> {
            self.array(values)?;
            for value in values {
                self.text(value)?;
            }
            Ok(())
        }
        #[expect(
            clippy::result_large_err,
            reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
        )]
        fn ordered(&mut self, values: &[(String, SortDirection)]) -> Result<()> {
            self.array(values)?;
            for (value, _) in values {
                self.text(value)?;
            }
            Ok(())
        }
    }
    let mut plan = Plan(0);
    plan.array(&meta.columns)?;
    for column in &meta.columns {
        plan.text(&column.name)?;
        plan.optional_text(&column.default)?;
        if let Some(reference) = &column.references {
            plan.text(&reference.table)?;
            plan.text(&reference.column)?;
        }
        if let Some(rank) = &column.rank_policy {
            for name in [
                &rank.joined_table,
                &rank.joined_column,
                &rank.anchor_column,
                &rank.sort_key,
                &rank.formula,
                &rank.protected_index,
            ] {
                plan.text(name)?;
            }
        }
        if let Some(scope) = &column.scope_label {
            match scope {
                contextdb_core::ScopeLabelKind::Simple { write_labels } => {
                    plan.names(write_labels)?
                }
                contextdb_core::ScopeLabelKind::Split {
                    read_labels,
                    write_labels,
                } => {
                    plan.names(read_labels)?;
                    plan.names(write_labels)?;
                }
            }
        }
        if let Some(reference) = &column.acl_ref {
            plan.text(&reference.ref_table)?;
            plan.text(&reference.ref_column)?;
        }
        if let Some(names) = &column.partition_key_columns {
            plan.names(names)?;
        }
    }
    if let Some(machine) = &meta.state_machine {
        plan.text(&machine.column)?;
        plan.add(
            crate::memory_accounting::native_map_plan::<String, Vec<String>>(
                machine.transitions.capacity(),
            )?
            .layout
            .size(),
        )?;
        for (state, next) in &machine.transitions {
            plan.text(state)?;
            plan.names(next)?;
        }
    }
    plan.names(&meta.dag_edge_types)?;
    plan.array(&meta.unique_constraints)?;
    for names in &meta.unique_constraints {
        plan.names(names)?;
    }
    plan.optional_text(&meta.natural_key_column)?;
    plan.array(&meta.propagation_rules)?;
    for rule in &meta.propagation_rules {
        match rule {
            contextdb_core::PropagationRule::ForeignKey {
                fk_column,
                referenced_table,
                referenced_column,
                trigger_state,
                target_state,
                ..
            } => {
                for name in [
                    fk_column,
                    referenced_table,
                    referenced_column,
                    trigger_state,
                    target_state,
                ] {
                    plan.text(name)?;
                }
            }
            contextdb_core::PropagationRule::Edge {
                edge_type,
                trigger_state,
                target_state,
                ..
            } => {
                for name in [edge_type, trigger_state, target_state] {
                    plan.text(name)?;
                }
            }
            contextdb_core::PropagationRule::VectorExclusion { trigger_state } => {
                plan.text(trigger_state)?
            }
        }
    }
    plan.optional_text(&meta.expires_column)?;
    plan.array(&meta.indexes)?;
    for index in &meta.indexes {
        plan.text(&index.name)?;
        plan.ordered(&index.columns)?;
    }
    plan.array(&meta.composite_foreign_keys)?;
    for key in &meta.composite_foreign_keys {
        plan.names(&key.child_columns)?;
        plan.text(&key.parent_table)?;
        plan.names(&key.parent_columns)?;
    }
    plan.names(&meta.primary_key_columns)?;
    if let Some(names) = &meta.delivery_manifest_tables {
        plan.names(names)?;
    }
    if let Some(contextdb_core::MaintainedCounts::Lists(lists)) = &meta.maintained_counts {
        plan.array(lists)?;
        for columns in lists {
            plan.ordered(columns)?;
        }
    }
    Ok(plan.0)
}
