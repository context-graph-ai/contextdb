//! `COUNT(*)` per group, answered from the counts a table declares the engine
//! keeps current (`MAINTAIN COUNT`).
//!
//! The one grouped form is a count over a column list the table maintains: the
//! WHERE clause pins that list's leading columns and GROUP BY names the rest.
//! A served answer reads one stored count per group under the pinned prefix,
//! so its work is the number of groups and never the number of rows they
//! count. Where this read cannot use those records -- a transaction that has
//! staged rows of the table, rows filtered per reader, a committed image, pins
//! that name no single exact prefix -- the identical answer is counted from the
//! rows instead, and the trace says the declared count was not served. A
//! grouped count over columns the table does not maintain is refused.

use super::*;
use crate::maintained_counts::MaintainedCountDisclosure;

/// The grouped statement's columns, resolved against its table.
struct GroupedShape {
    /// Distinct columns the WHERE clause pins, in written order.
    pinned: Vec<String>,
    /// Distinct GROUP BY columns, in written order.
    grouped: Vec<String>,
}

impl GroupedShape {
    fn resolve(
        table: &str,
        alias: Option<&str>,
        meta: &TableMeta,
        filter: Option<&Expr>,
        group_by: &[ColumnRef],
        columns: &[ProjectColumn],
    ) -> std::result::Result<Self, BoundedExecutionError> {
        let column = |reference: &ColumnRef| -> std::result::Result<String, BoundedExecutionError> {
            let qualified_elsewhere = reference
                .table
                .as_deref()
                .is_some_and(|qualifier| qualifier != table && Some(qualifier) != alias);
            if qualified_elsewhere
                || !meta
                    .columns
                    .iter()
                    .any(|declared| declared.name == reference.column)
            {
                return Err(Error::ColumnNotFound {
                    table: table.to_string(),
                    column: reference.column.clone(),
                }
                .into());
            }
            Ok(reference.column.clone())
        };
        let mut grouped = Vec::with_capacity(group_by.len());
        for reference in group_by {
            let name = column(reference)?;
            if !grouped.contains(&name) {
                grouped.push(name);
            }
        }
        for projected in columns {
            match &projected.expr {
                Expr::Column(reference) => {
                    if !grouped.contains(&column(reference)?) {
                        return Err(group_by_refusal().into());
                    }
                }
                Expr::FunctionCall { name, args }
                    if name.eq_ignore_ascii_case("count")
                        && matches!(aggregate_input(args), AggregateInput::Row) => {}
                _ => return Err(group_by_refusal().into()),
            }
        }
        let mut pinned = Vec::new();
        for conjunct in filter.map(split_conjuncts).unwrap_or_default() {
            let reference = match &conjunct {
                Expr::BinaryOp {
                    left,
                    op: BinOp::Eq,
                    right,
                } if matches!(right.as_ref(), Expr::Literal(_) | Expr::Parameter(_)) => {
                    match left.as_ref() {
                        Expr::Column(reference) => reference,
                        _ => return Err(group_by_refusal().into()),
                    }
                }
                Expr::IsNull {
                    expr,
                    negated: false,
                } => match expr.as_ref() {
                    Expr::Column(reference) => reference,
                    _ => return Err(group_by_refusal().into()),
                },
                _ => return Err(group_by_refusal().into()),
            };
            let name = column(reference)?;
            if !pinned.contains(&name) {
                pinned.push(name);
            }
        }
        Ok(Self { pinned, grouped })
    }

    /// Every column the count is keyed by: the pinned ones and the grouped
    /// ones, each once.
    fn covered(&self) -> BTreeSet<&str> {
        self.pinned
            .iter()
            .chain(&self.grouped)
            .map(String::as_str)
            .collect()
    }

    /// Whether a count over `list` answers this statement: it covers exactly
    /// these columns and leads with exactly the pinned ones.
    fn answered_by(&self, list: &[(String, contextdb_core::SortDirection)]) -> bool {
        let covered = self.covered();
        list.len() == covered.len()
            && list
                .iter()
                .all(|(column, _)| covered.contains(column.as_str()))
            && list[..self.pinned.len()]
                .iter()
                .all(|(column, _)| self.pinned.contains(column))
    }

    fn describe(&self) -> String {
        let grouped = format!("COUNT(*) grouped by ({})", self.grouped.join(", "));
        if self.pinned.is_empty() {
            grouped
        } else {
            format!("{grouped} with ({}) pinned", self.pinned.join(", "))
        }
    }
}

fn group_by_refusal() -> Error {
    Error::ParseError("GROUP BY is not supported".to_string())
}

/// The refusal for a grouped count over columns the table maintains no count
/// for, naming the declaration that would serve it where one can.
fn unmaintained_refusal(table: &str, meta: &TableMeta, shape: &GroupedShape) -> Error {
    let width = shape.covered().len();
    let candidate = crate::maintained_counts::count_bearing_index_declarations(meta)
        .into_iter()
        .find_map(|index| {
            (index.columns.len() >= width && shape.answered_by(&index.columns[..width]))
                .then(|| index.columns[..width].to_vec())
        });
    let reason = match candidate {
        Some(list)
            if crate::persistence::relational_index_prefix_is_constraint_bounded(meta, &list) =>
        {
            format!(
                "({}) is bounded at 0 or 1 by a primary key or unique constraint, so no count over \
                 it is maintained",
                contextdb_core::render_maintained_count_columns(&list)
            )
        }
        Some(list) => {
            let spelled = contextdb_core::render_maintained_count_columns(&list);
            let advised = MaintainedCountDisclosure::NotDeclared {
                table: table.to_string(),
                columns: list,
                already_declared: crate::maintained_counts::declared_lists(meta),
            }
            .advised_statement()
            .unwrap_or_default();
            format!("the table maintains no count over ({spelled}); {advised} declares one")
        }
        None => {
            let mut covered = shape.pinned.clone();
            covered.extend(
                shape
                    .grouped
                    .iter()
                    .filter(|column| !shape.pinned.contains(column))
                    .cloned(),
            );
            format!(
                "no index leads with ({}), so no count over them can be maintained",
                covered.join(", ")
            )
        }
    };
    Error::PlanError(format!(
        "GROUP BY is not supported for {} on '{table}': {reason}",
        shape.describe()
    ))
}

/// The one exact leading prefix the pins name, in the list's own directions,
/// or `None` when they name none: a pin that matches nothing, a value that
/// cannot be the column's type, an unbound parameter, two pins on one column.
fn pinned_prefix(
    db: &dyn ReadExecutionTarget,
    table: &str,
    list: &[(String, contextdb_core::SortDirection)],
    pinned: usize,
    filter: Option<&Expr>,
    params: &HashMap<String, Value>,
) -> std::result::Result<Option<IndexKey>, BoundedExecutionError> {
    if pinned == 0 {
        return Ok(Some(Vec::new()));
    }
    let mut shapes: Vec<Option<IndexPredicateShape>> = vec![None; pinned];
    for conjunct in filter.map(split_conjuncts).unwrap_or_default() {
        let Some((column, shape)) = classify_index_predicate(&conjunct, params) else {
            return Ok(None);
        };
        let Some(position) = list[..pinned].iter().position(|(name, _)| *name == column) else {
            return Ok(None);
        };
        if shapes[position].replace(shape).is_some() {
            return Ok(None);
        }
    }
    let Some(mut shapes) = shapes.into_iter().collect::<Option<Vec<_>>>() else {
        return Ok(None);
    };
    let suffix_shapes = shapes.split_off(1);
    let pick = IndexPick {
        name: String::new(),
        columns: list.to_vec(),
        shape: shapes.pop().expect("a pinned prefix has a first column"),
        pushed_column: list[0].0.clone(),
        pushed_columns: list[..pinned]
            .iter()
            .map(|(column, _)| column.clone())
            .collect(),
        suffix_shapes,
        trailing_range: None,
        prefix_empty: false,
        probe_values_normalized: false,
        tuple_prefix: None,
    };
    let Ok(pick) = db.coerce_pick_shape_to_column_type(table, &pick) else {
        return Ok(None);
    };
    if crate::executor::pick_matches_nothing(&pick) {
        return Ok(None);
    }
    let mut runs = crate::executor::expanded_index_runs(&pick)?;
    if runs.len() != 1 {
        return Ok(None);
    }
    let (Bound::Included(prefix), _) = runs.pop().expect("one exact index run") else {
        return Ok(None);
    };
    Ok((prefix.len() == pinned).then_some(prefix))
}

/// Whether this read may answer from stored counts at all. They count every
/// committed row, so a reader whose rows are filtered per reader, or a
/// transaction that has staged rows of this table, needs the rows themselves.
fn stored_counts_answer(
    db: &dyn ReadExecutionTarget,
    table: &str,
    context: &Arc<RequestContext>,
) -> std::result::Result<bool, BoundedExecutionError> {
    if db.bounded_read_requires_candidate_filter(table)? {
        return Ok(false);
    }
    Ok(match context.transaction() {
        None => true,
        Some(tx) => db
            .transaction_table_overlay(
                tx,
                table,
                context.source_read_memory(BoundedWorkSource::TableScan),
            )?
            .is_empty(),
    })
}

fn key_bytes(key: &IndexKey) -> std::result::Result<usize, BoundedExecutionError> {
    let mut bytes = checked_size_mul(
        key.capacity(),
        std::mem::size_of::<DirectedValue>(),
        "grouped count key",
    )?;
    for part in key {
        let (DirectedValue::Asc(TotalOrdAsc(value)) | DirectedValue::Desc(TotalOrdDesc(value))) =
            part;
        bytes = checked_size_add(bytes, value_capacity_bytes(value)?, "grouped count key")?;
    }
    Ok(bytes)
}

fn key_value(part: &DirectedValue) -> &Value {
    let (DirectedValue::Asc(TotalOrdAsc(value)) | DirectedValue::Desc(TotalOrdDesc(value))) = part;
    value
}

fn output_columns(columns: &[ProjectColumn]) -> Vec<String> {
    columns
        .iter()
        .map(|column| {
            column.alias.clone().unwrap_or_else(|| match &column.expr {
                Expr::Column(reference) => reference.column.clone(),
                Expr::FunctionCall { name, .. } => name.clone(),
                _ => "expr".to_string(),
            })
        })
        .collect()
}

/// One output row per group, in the declared key order, each charged to the
/// request before its values are cloned and carried by the row from there.
fn group_rows(
    groups: impl ExactSizeIterator<Item = (IndexKey, u64)>,
    list: &[(String, contextdb_core::SortDirection)],
    columns: &[ProjectColumn],
    held: &mut ReservedBytes<'_>,
    context: &Arc<RequestContext>,
) -> std::result::Result<VecDeque<PulledRow>, BoundedExecutionError> {
    let positions = columns
        .iter()
        .map(|column| match &column.expr {
            Expr::Column(reference) => list
                .iter()
                .position(|(name, _)| *name == reference.column)
                .map(Some)
                .ok_or_else(|| BoundedExecutionError::from(group_by_refusal())),
            _ => Ok(None),
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let container = checked_size_mul(
        groups.len(),
        std::mem::size_of::<PulledRow>(),
        "grouped count rows",
    )?;
    context.reserve(BoundedWorkSource::IndexRange, container)?;
    held.bytes = checked_size_add(held.bytes, container, "grouped count rows")?;
    let mut rows = VecDeque::new();
    if rows.try_reserve_exact(groups.len()).is_err() {
        return Err(Error::Other("bounded grouped count allocation failed".to_string()).into());
    }
    for (key, count) in groups {
        let count = i64::try_from(count)
            .map_err(|_| Error::PlanError("integer out of range".to_string()))?;
        let mut retained = checked_size_add(
            std::mem::size_of::<Vec<Value>>(),
            checked_size_mul(
                columns.len(),
                std::mem::size_of::<Value>(),
                "grouped count row",
            )?,
            "grouped count row",
        )?;
        for position in positions.iter().flatten() {
            retained = checked_size_add(
                retained,
                value_capacity_bytes(key_value(&key[*position]))?,
                "grouped count row",
            )?;
        }
        context.reserve(BoundedWorkSource::IndexRange, retained)?;
        held.bytes = checked_size_add(held.bytes, retained, "grouped count row")?;
        let values = positions
            .iter()
            .map(|position| match position {
                Some(position) => key_value(&key[*position]).clone(),
                None => Value::Int64(count),
            })
            .collect::<Vec<_>>();
        // The row carries this charge from here; the drain releases it.
        held.bytes -= retained;
        rows.push_back(PulledRow {
            window: None,
            values,
            retained_bytes: retained,
            group_binding: None,
        });
    }
    // The container is the static node's own continuation from here.
    held.bytes -= container;
    context.release(container)?;
    Ok(rows)
}

/// Serve the grouped count from the stored counts, or `None` when this read
/// cannot reach them.
#[allow(clippy::too_many_arguments)]
fn served_groups(
    database: &crate::database::Database,
    table: &str,
    meta: &TableMeta,
    list: &[(String, contextdb_core::SortDirection)],
    prefix: &IndexKey,
    columns: &[ProjectColumn],
    snapshot: SnapshotId,
    context: &Arc<RequestContext>,
) -> std::result::Result<Option<Box<PullNode>>, BoundedExecutionError> {
    let mut held = ReservedBytes::new(context, 0);
    let mut groups: Vec<(IndexKey, u64)> = Vec::new();
    let served = database.maintained_count_groups(
        table,
        list,
        prefix,
        snapshot,
        context.source_read_memory(BoundedWorkSource::IndexRange),
        &mut || {
            context.charge(
                BoundedWorkSource::IndexRange,
                BoundedSourceTouch::IndexEntry,
            )
        },
        &mut |key, count| {
            let slot = reserve_vector_slot(
                context,
                BoundedWorkSource::IndexRange,
                &mut groups,
                "grouped counts",
            )?;
            held.bytes = checked_size_add(held.bytes, slot, "grouped counts")?;
            let bytes = key_bytes(&key)?;
            context.reserve(BoundedWorkSource::IndexRange, bytes)?;
            held.bytes = checked_size_add(held.bytes, bytes, "grouped counts")?;
            groups.push((key, count));
            Ok(())
        },
    )?;
    if !served {
        held.release()?;
        return Ok(None);
    }
    groups.sort_by(|left, right| left.0.cmp(&right.0));
    let rows = group_rows(groups.into_iter(), list, columns, &mut held, context)?;
    held.release()?;
    Ok(Some(Box::new(PullNode {
        columns: output_columns(columns),
        trace: QueryTrace {
            physical_plan: "IndexScan",
            index_used: crate::maintained_counts::indexes_leading_with(meta, list)
                .into_iter()
                .next(),
            predicates_pushed: list[..prefix.len()]
                .iter()
                .map(|(column, _)| std::borrow::Cow::Owned(column.clone()))
                .collect(),
            maintained_count: Some(Box::new(MaintainedCountDisclosure::Used {
                table: table.to_string(),
                columns: list.to_vec(),
            })),
            ..Default::default()
        },
        kind: PullKind::Static(StaticState { rows }),
    })))
}

/// Count the same groups from the rows the ordinary scan admits, charged per
/// group, and say on the trace that the declared count was not served.
#[allow(clippy::too_many_arguments)]
fn counted_groups(
    db: &dyn ReadExecutionTarget,
    table: &str,
    filter: Option<&Expr>,
    list: &[(String, contextdb_core::SortDirection)],
    columns: &[ProjectColumn],
    params: Arc<HashMap<String, Value>>,
    snapshot: SnapshotId,
    context: &Arc<RequestContext>,
) -> std::result::Result<Box<PullNode>, BoundedExecutionError> {
    let mut source = build_scan(db, table, filter, params, snapshot, None, None, context)?;
    let positions = list
        .iter()
        .map(|(name, _)| {
            source
                .columns
                .iter()
                .position(|column| column == name)
                .ok_or_else(|| {
                    BoundedExecutionError::from(Error::ColumnNotFound {
                        table: table.to_string(),
                        column: name.clone(),
                    })
                })
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let mut held = ReservedBytes::new(context, 0);
    let mut groups: BTreeMap<IndexKey, u64> = BTreeMap::new();
    // What one map entry holds besides its key's own values.
    let entry_bytes = checked_size_mul(
        2,
        std::mem::size_of::<(IndexKey, u64)>(),
        "grouped count entry",
    )?;
    while let Some(row) = pull(db, context, &mut source)? {
        let row = RetainedRow::new(context, row);
        let mut planned = checked_size_add(
            entry_bytes,
            checked_size_mul(
                list.len(),
                std::mem::size_of::<DirectedValue>(),
                "grouped count key",
            )?,
            "grouped count key",
        )?;
        for position in &positions {
            let value = row.values.get(*position).ok_or_else(|| {
                BoundedExecutionError::from(Error::Other(
                    "grouped count row lacks a key column".to_string(),
                ))
            })?;
            planned = checked_size_add(planned, value_capacity_bytes(value)?, "grouped count key")?;
        }
        context.reserve(BoundedWorkSource::IndexRange, planned)?;
        held.bytes = checked_size_add(held.bytes, planned, "grouped count key")?;
        let key: IndexKey = positions
            .iter()
            .zip(list)
            .map(|(position, (_, direction))| {
                let value = row.values[*position].clone();
                match direction {
                    contextdb_core::SortDirection::Asc => DirectedValue::Asc(TotalOrdAsc(value)),
                    contextdb_core::SortDirection::Desc => DirectedValue::Desc(TotalOrdDesc(value)),
                }
            })
            .collect();
        match groups.get_mut(&key) {
            Some(count) => {
                *count = count
                    .checked_add(1)
                    .ok_or_else(|| Error::PlanError("integer out of range".to_string()))?;
                drop(key);
                held.bytes -= planned;
                context.release(planned)?;
            }
            None => {
                groups.insert(key, 1);
            }
        }
        row.release()?;
    }
    let mut trace = source.trace.clone();
    drop(source);
    trace.maintained_count = Some(Box::new(MaintainedCountDisclosure::DeclaredNotServed {
        table: table.to_string(),
        columns: list.to_vec(),
    }));
    let rows = group_rows(groups.into_iter(), list, columns, &mut held, context)?;
    held.release()?;
    Ok(Box::new(PullNode {
        columns: output_columns(columns),
        trace,
        kind: PullKind::Static(StaticState { rows }),
    }))
}

/// Answer a grouped `COUNT(*)` at construction: served from stored counts,
/// counted exactly from the rows, or refused.
#[allow(clippy::too_many_arguments)]
pub(super) fn build_grouped_count(
    db: &dyn ReadExecutionTarget,
    input: &PhysicalPlan,
    group_by: &[ColumnRef],
    columns: &[ProjectColumn],
    params: Arc<HashMap<String, Value>>,
    snapshot: SnapshotId,
    context: &Arc<RequestContext>,
) -> std::result::Result<Box<PullNode>, BoundedExecutionError> {
    let PhysicalPlan::Scan {
        table,
        alias,
        filter,
    } = input
    else {
        return Err(group_by_refusal().into());
    };
    db.assert_table_read_allowed(table)?;
    let meta = db
        .table_meta(table)
        .ok_or_else(|| Error::TableNotFound(table.clone()))?;
    let shape = GroupedShape::resolve(
        table,
        alias.as_deref(),
        &meta,
        filter.as_ref(),
        group_by,
        columns,
    )?;
    let Some(list) = crate::maintained_counts::every_maintained_prefix(&meta)
        .into_iter()
        .find(|list| shape.answered_by(list))
    else {
        return Err(unmaintained_refusal(table, &meta, &shape).into());
    };
    let prefix = pinned_prefix(
        db,
        table,
        &list,
        shape.pinned.len(),
        filter.as_ref(),
        &params,
    )?;
    if let Some(prefix) = prefix.as_ref()
        && stored_counts_answer(db, table, context)?
        && let Some(database) = db.as_database_for_subquery_plan()
        && let Some(node) = served_groups(
            database, table, &meta, &list, prefix, columns, snapshot, context,
        )?
    {
        return Ok(node);
    }
    counted_groups(
        db,
        table,
        filter.as_ref(),
        &list,
        columns,
        params,
        snapshot,
        context,
    )
}

/// What a planned GROUP BY column list holds.
pub(super) fn group_by_capacity_bytes(
    group_by: &Vec<ColumnRef>,
) -> std::result::Result<usize, BoundedExecutionError> {
    let mut bytes = checked_size_mul(
        group_by.capacity(),
        std::mem::size_of::<ColumnRef>(),
        "grouped columns",
    )?;
    for column in group_by {
        bytes = checked_size_add(
            bytes,
            optional_string_capacity(&column.table),
            "grouped column",
        )?;
        bytes = checked_size_add(bytes, column.column.capacity(), "grouped column")?;
    }
    Ok(bytes)
}
