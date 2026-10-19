//! Receiver-private coordinates. Each position is a separately addressed
//! metadata record, committed with the one acceptance END. Neither source
//! progress nor immutable author coordinates are receiver coordinates.
use super::*;
use contextdb_core::read_memory::ReadCredit;
use contextdb_relational::membership::MembershipImage;
use contextdb_relational::store::IndexEntry;
use std::ops::Bound;

pub(crate) const MEMBERSHIP_PREFIX: &str = "__contextdb.connected_position.v1.";

pub(crate) fn membership_key(lsn: Lsn) -> String {
    format!("{MEMBERSHIP_PREFIX}{:020}", lsn.0)
}

pub(super) struct ConnectedCoordinates {
    groups: BTreeMap<Lsn, Lsn>,
    start: Lsn,
    end: Lsn,
    pub(super) _credit: Arc<ReadCredit>,
}

impl ConnectedCoordinates {
    pub(super) fn prepare(
        changes: &ChangeSet,
        start: Lsn,
        end: Lsn,
        owner: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<Self> {
        let mut credit = ReadCredit::new(owner);
        let mut groups = BTreeMap::new();
        for source in changes
            .rows
            .iter()
            .map(|row| row.lsn)
            .chain(changes.edges.iter().map(|edge| edge.lsn))
            .chain(changes.vectors.iter().map(|vector| vector.lsn))
            .chain(changes.ddl_lsn.iter().copied())
        {
            if !groups.contains_key(&source) {
                credit.try_grow_for(
                    2048,
                    "sync",
                    "connected_coordinates",
                    "Raise the declared memory limit to receive this unit.",
                )?;
                groups.insert(source, Lsn(0));
            }
        }
        if groups.len() as u64 != end.0 - start.0 + 1 {
            return Err(Error::SyncError(
                "private interval does not cover the source groups".into(),
            ));
        }
        for (offset, local) in groups.values_mut().enumerate() {
            *local = Lsn(start.0 + offset as u64);
        }
        Ok(Self {
            _credit: Arc::new(credit),
            groups,
            start,
            end,
        })
    }

    pub(super) fn local(&self, source: Lsn) -> Result<Lsn> {
        self.groups.get(&source).copied().ok_or_else(|| {
            Error::SyncError("received member has no reserved subgroup coordinate".into())
        })
    }

    pub(super) fn stage_membership(&self, writes: &mut Vec<(String, Vec<u8>)>) -> Result<()> {
        for local in self.groups.values() {
            writes.push((
                membership_key(*local),
                RedbPersistence::encode_config_value(&(self.start.0, self.end.0))?,
            ));
        }
        Ok(())
    }
}

fn membership_position(lsn: Lsn) -> Vec<contextdb_core::DirectedValue> {
    vec![contextdb_core::DirectedValue::Asc(
        contextdb_core::TotalOrdAsc(Value::TxId(TxId(lsn.0))),
    )]
}

impl Database {
    pub(super) fn connected_membership_at(
        &self,
        lsn: Lsn,
        read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<Option<(Lsn, Lsn)>> {
        let _point_credit = ReadCredit::try_new(
            read_memory.clone(),
            256,
            "sync",
            "connected_coordinate_key",
            "Raise the declared reader memory limit to inspect connected history.",
        )?;
        let (_, root) = self.capture_writer_statement()?;
        if let Some(root) = root {
            let record = root.config_point::<(u64, u64)>(
                crate::persistence::writer_source::SourceWindowOwner::Read(read_memory),
                &membership_key(lsn),
            )?;
            return record
                .as_ref()
                .map(|record| {
                    let (start, end) = record.value;
                    if start > lsn.0 || end < lsn.0 || end > root.lsn.0 {
                        return Err(Error::StoreCorrupted {
                            path: membership_key(lsn),
                            reason: "connected membership exceeds its committed interval".into(),
                        });
                    }
                    Ok((Lsn(start), Lsn(end)))
                })
                .transpose();
        }
        if let Some(history) = self.image_history.as_ref() {
            let record = history
                .source
                .image_connected_membership_point(&membership_key(lsn))?;
            return record
                .map(|(start, end)| {
                    if start > lsn.0 || end < lsn.0 || end > self.current_lsn().0 {
                        return Err(Error::StoreCorrupted {
                            path: membership_key(lsn),
                            reason: "invalid retained connected interval".into(),
                        });
                    }
                    Ok((Lsn(start), Lsn(end)))
                })
                .transpose();
        }
        let _credit = ReadCredit::try_new(
            read_memory,
            128,
            "sync",
            "connected_membership_point",
            "Raise the declared reader memory limit to inspect connected history.",
        )?;
        let key = membership_position(lsn);
        let state = self.lineage_state_lock.lock();
        let selected = state
            .connected_membership
            .next(None, Bound::Included(&key), false);
        Ok(selected
            .filter(|(position, _)| *position == key.as_slice())
            .map(|(_, entry)| (Lsn(entry.row_id.0), Lsn(entry.created_tx.0))))
    }

    pub(crate) fn connected_history_end(
        &self,
        lsn: Lsn,
        owner: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<Lsn> {
        Ok(self
            .connected_membership_at(lsn, owner)?
            .map_or(lsn, |(_, end)| end))
    }

    pub(super) fn connected_history_since(
        &self,
        lsn: Lsn,
        owner: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<Lsn> {
        Ok(self
            .connected_membership_at(lsn, owner)?
            .map_or(lsn, |(start, end)| {
                if lsn < end {
                    Lsn(start.0.saturating_sub(1))
                } else {
                    lsn
                }
            }))
    }

    pub(super) fn prepare_connected_membership_delta(
        &self,
        writes: &[(String, Vec<u8>)],
    ) -> Result<Option<MembershipImage>> {
        let _scratch = ReadCredit::try_new(
            self.finalization.accountant.read_budget()?,
            128,
            "sync",
            "connected_membership_prepare",
            "Raise MEMORY_LIMIT to prepare connected history.",
        )?;
        let mut image = None;
        for (key, value) in writes {
            let Some(position) = key.strip_prefix(MEMBERSHIP_PREFIX) else {
                continue;
            };
            let position = position.parse::<u64>().map_err(|_| Error::StoreCorrupted {
                path: key.clone(),
                reason: "invalid connected position".into(),
            })?;
            let (start, end): (u64, u64) = RedbPersistence::decode_config_value(value)?;
            if start > position || position > end {
                return Err(Error::StoreCorrupted {
                    path: key.clone(),
                    reason: "invalid connected interval".into(),
                });
            }
            if image.is_none() {
                let mut root = self.lineage_state_lock.lock().connected_membership.clone();
                root.admit(self.finalization.accountant.read_budget()?)?;
                image = Some(root);
            }
            image.as_mut().expect("prepared root").insert(
                &membership_position(Lsn(position)),
                IndexEntry {
                    row_id: RowId(start),
                    created_tx: TxId(end),
                    deleted_tx: None,
                },
            )?;
        }
        Ok(image)
    }
}

/// Pending source members and their immutable input images are distinct
/// owners. DDL transforms only the pending projection. The input image is
/// retained for historical export and is never decoded using a later shape.
pub(super) struct ConnectedOverlay {
    pub(super) projected: ChangeSet,
    pub(super) originals: Vec<(RowChange, Arc<AuthoredRowShape>, Option<RowChange>, Option<std::ops::Range<usize>>)>,
    source_vector_group_present: Vec<bool>,
    pub(super) _credit: Arc<ReadCredit>,
}

impl Database {
    pub(super) fn connected_source_positions(
        &self,
        changes: &ChangeSet,
        owner: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<(ReadCredit, BTreeSet<Lsn>)> {
        let mut credit = ReadCredit::new(owner);
        let mut positions = BTreeSet::new();
        for lsn in changes
            .rows
            .iter()
            .map(|row| row.lsn)
            .chain(changes.edges.iter().map(|edge| edge.lsn))
            .chain(changes.vectors.iter().map(|vector| vector.lsn))
            .chain(changes.ddl_lsn.iter().copied())
        {
            if !positions.contains(&lsn) {
                credit.try_grow_for(
                    128,
                    "sync",
                    "connected_source_groups",
                    "Raise the declared reader memory limit to receive this unit.",
                )?;
                positions.insert(lsn);
            }
        }
        Ok((credit, positions))
    }

    pub(super) fn prepare_connected_overlay(
        &self,
        changes: &ChangeSet,
        owner: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<ConnectedOverlay> {
        self.prepare_connected_overlay_with_vectors(changes,owner,false)
    }

    pub(super) fn prepare_connected_overlay_with_vectors(
        &self,changes: &ChangeSet,owner: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        source_vectors: bool,
    ) -> Result<ConnectedOverlay> {
        let (mut credit, positions) = self.connected_source_positions(changes, owner.clone())?;
        let (_catalog_credit, mut tables) =
            self.selected_received_catalog(changes, std::iter::empty())?;
        let mut bytes = changes
            .ddl
            .iter()
            .map(DdlChange::estimated_bytes)
            .sum::<usize>();
        for row in &changes.rows {
            bytes = bytes
                .saturating_add(row.held_bytes().saturating_mul(8))
                // Exact accepted-occurrence marker, encoding and write-set overlap.
                .saturating_add(2048);
        }
        bytes = bytes
            .saturating_add(
                changes
                    .edges
                    .iter()
                    .map(|edge| edge.held_bytes().saturating_mul(4))
                    .sum::<usize>(),
            )
            .saturating_add(
                changes
                    .vectors
                    .iter()
                    .map(|vector| vector.held_bytes().saturating_mul(4))
                    .sum::<usize>(),
            );
        credit.try_grow_for(
            bytes.saturating_add(4096),
            "sync",
            "connected_pending_overlay",
            "Raise the declared reader memory limit to receive this unit.",
        )?;
        let mut projected = ChangeSet {
            ddl: changes.ddl.clone(),
            ddl_lsn: changes.ddl_lsn.clone(),
            ..ChangeSet::default()
        };
        let mut originals: Vec<(RowChange, Arc<AuthoredRowShape>, Option<RowChange>, Option<std::ops::Range<usize>>)> = Vec::new();
        let mut authored_shapes = HashMap::<String, Arc<AuthoredRowShape>>::new();
        let vector_controls = if source_vectors { changes.vectors.len().saturating_mul(2048) } else { 0 };
        credit.try_grow_for(vector_controls,"sync","connected_authored_vector_ranges",
            "Raise the declared memory limit to retain exact source vector occurrences.")?;
        let mut source_vector_group_present = Vec::new();
        if source_vectors {
            credit.try_grow_for(changes.rows.len().saturating_add(2 * std::mem::size_of::<Vec<bool>>()),
                "sync","connected_vector_group_presence","Raise the declared memory limit to retain source vector ownership.")?;
            source_vector_group_present.reserve_exact(changes.rows.len());
        }
        let mut vector_groups = BTreeMap::<(&str,Lsn,bool),std::collections::VecDeque<std::ops::Range<usize>>>::new();
        let mut vector_cursor = 0;
        while source_vectors && vector_cursor < changes.vectors.len() {
            let end = vector_row_group_end(&changes.vectors,vector_cursor);
            let first = &changes.vectors[vector_cursor];
            vector_groups.entry((first.index.table.as_str(),first.lsn,first.vector.is_empty()))
                .or_default().push_back(vector_cursor..end);
            vector_cursor = end;
        }
        for position in positions {
            for (ddl, _) in changes
                .ddl
                .iter()
                .zip(&changes.ddl_lsn)
                .filter(|(_, lsn)| **lsn == position)
            {
                if let Some(table) = Self::ddl_affected_table(ddl) {
                    authored_shapes.remove(table);
                }
                Self::validate_sync_table_ddl_against_projected_meta(&tables, ddl)?;
                let previous = Self::ddl_affected_table(ddl)
                    .and_then(|table| tables.get(table))
                    .cloned();
                Self::apply_sync_table_ddl_to_projected_meta(&mut tables, ddl);
                match ddl {
                    DdlChange::DropTable { name } => {
                        if previous.as_ref().is_some_and(has_graph_edge_table_shape) {
                            projected.edges.retain(|edge| {
                                !originals
                                    .iter()
                                    .filter_map(|(_, _, pending, _)| pending.as_ref())
                                    .filter(|row| row.table == *name && !row.deleted)
                                    .any(|row| {
                                        row.values.get("source_id").and_then(Value::as_uuid)
                                            == Some(&edge.source)
                                            && row.values.get("target_id").and_then(Value::as_uuid)
                                                == Some(&edge.target)
                                            && row.values.get("edge_type").and_then(Value::as_text)
                                                == Some(edge.edge_type.as_str())
                                    })
                            });
                        }
                        for (original, _, pending, _) in &mut originals {
                            if original.table == *name {
                                *pending = None;
                            }
                        }
                        projected
                            .vectors
                            .retain(|vector| vector.index.table != *name);
                    }
                    DdlChange::AlterTable {
                        name, constraints, ..
                    } => {
                        let Some(meta) = tables.get(name) else {
                            continue;
                        };
                        let mut renames = sync_column_renames_from_constraints(constraints);
                        if let Some(rename) = sync_vector_rename_from_constraints(constraints) {
                            if !renames.contains(&rename) {
                                renames.push(rename);
                            }
                        }
                        let old_vector_shapes = previous
                            .as_ref()
                            .map(Self::sync_vector_columns)
                            .unwrap_or_default();
                        let new_vector_shapes = Self::sync_vector_columns(meta);
                        for (_, _, pending, _) in &mut originals {
                            let Some(row) = pending.as_mut().filter(|row| row.table == *name)
                            else {
                                continue;
                            };
                            for (from, to) in &renames {
                                if let Some(value) = row.values.remove(from) {
                                    row.values.insert(to.clone(), value);
                                }
                            }
                            row.values.retain(|column, _| {
                                meta.columns.iter().any(|declared| declared.name == *column)
                            });
                            for column in &meta.columns {
                                let original_column = renames
                                    .iter()
                                    .find(|(_, to)| *to == column.name)
                                    .map_or(column.name.as_str(), |(from, _)| from.as_str());
                                if old_vector_shapes
                                    .get(original_column)
                                    .zip(new_vector_shapes.get(&column.name))
                                    .is_some_and(|(old, new)| old != new)
                                {
                                    row.values.insert(column.name.clone(), Value::Null);
                                }
                                if !row.values.contains_key(&column.name) {
                                    let value = column
                                        .default
                                        .as_deref()
                                        .map(crate::executor::evaluate_stored_default_expr)
                                        .transpose()?
                                        .unwrap_or(Value::Null);
                                    row.values.insert(column.name.clone(), value);
                                }
                            }
                            if let Some(key) = natural_key_from_row_values(meta, &row.values) {
                                row.natural_key = key;
                            }
                        }
                        for vector in &mut projected.vectors {
                            if vector.index.table == *name {
                                if let Some((_, to)) = renames
                                    .iter()
                                    .find(|(from, _)| *from == vector.index.column)
                                {
                                    vector.index.column = to.clone();
                                }
                            }
                        }
                        projected.vectors.retain(|vector| vector.index.table != *name || meta.columns.iter().any(|column| {
                            let original_column = renames.iter().find(|(_,to)|*to == vector.index.column)
                                .map_or(vector.index.column.as_str(),|(from,_)|from.as_str());
                            column.name == vector.index.column
                                && old_vector_shapes.get(original_column).zip(new_vector_shapes.get(&column.name)).is_none_or(|(old,new)|old == new)
                                && matches!(column.column_type,ColumnType::Vector(dimension) if dimension == vector.vector.len() || vector.vector.is_empty())
                        }));
                    }
                    _ => {}
                }
            }
            for row in changes.rows.iter().filter(|row| row.lsn == position) {
                self.preflight_sync_rows_against_projected_schema(
                    std::slice::from_ref(row),
                    &tables,
                    owner.clone(),
                )?;
                let meta = tables
                    .get(&row.table)
                    .ok_or_else(|| Error::TableNotFound(row.table.clone()))?;
                let shape = match authored_shapes.get(&row.table) {
                    Some(shape) => shape.clone(),
                    None => {
                        // One immutable declaration per table/source prefix,
                        // admitted before either its Arc or cache key exists.
                        credit.try_grow_for(
                            row.table.len().saturating_add(1024),
                            "sync", "connected_authored_shape",
                            "Raise the declared reader memory limit to retain authored history.",
                        )?;
                        let shape = AuthoredRowShape::prepare(meta, owner.clone())?;
                        authored_shapes.insert(row.table.clone(), shape.clone());
                        shape
                    }
                };
                if source_vectors {
                    // Keep key presence even after its last range is consumed.
                    // No range can mean declared NULL only if that source
                    // table/group had no body at all, rather than an exhausted
                    // same-position queue belonging to another row.
                    source_vector_group_present.push(vector_groups.contains_key(&(row.table.as_str(),row.lsn,row.deleted)));
                }
                let vector_group = vector_groups.get_mut(&(row.table.as_str(),row.lsn,row.deleted))
                    .and_then(std::collections::VecDeque::pop_front);
                originals.push((row.clone(), shape, Some(row.clone()), vector_group));
            }
            for vector in changes
                .vectors
                .iter()
                .filter(|vector| vector.lsn == position)
            {
                self.preflight_sync_vectors_against_projected_schema(
                    std::slice::from_ref(vector),
                    &tables,
                )?;
                projected.vectors.push(vector.clone());
            }
            projected.edges.extend(
                changes
                    .edges
                    .iter()
                    .filter(|edge| edge.lsn == position)
                    .cloned(),
            );
        }
        Self::validate_projected_foreign_key_schema(&tables)?;
        projected
            .rows
            .extend(originals.iter().filter_map(|(_, _, row, _)| row.clone()));
        Ok(ConnectedOverlay {
            _credit: Arc::new(credit),
            projected,
            originals,
            source_vector_group_present,
        })
    }

    pub(super) fn preflight_connected_overlay(
        &self,
        changes: &ChangeSet,
        policies: &ConflictPolicies,
        arrivals: &HashMap<Lsn, Option<Lsn>>,
        adoption: SyncAdoption,
        deletes: &[AuthenticatedFreshCreatorDelete],
        owner: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<()> {
        let overlay = self.prepare_connected_overlay(changes, owner.clone())?;
        let (_catalog_credit, mut tables) =
            self.selected_received_catalog(changes, std::iter::empty())?;
        for ddl in &changes.ddl {
            Self::apply_sync_table_ddl_to_projected_meta(&mut tables, ddl);
        }
        self.preflight_sync_foreign_keys_against_lsn_prefix(
            &overlay.projected.rows,
            &tables,
            self.snapshot(),
            &mut HashMap::new(),
            &mut HashMap::new(),
            policies,
            arrivals,
            adoption,
            deletes,
            owner.clone(),
        )?;
        let dag = self.projected_sync_dag_edge_types(&changes.ddl);
        self.preflight_sync_edge_cycles(&overlay.projected, &dag, owner)
    }
}

impl ConnectedOverlay {
    pub(super) fn admit_source_vector_archives(&mut self,source: &ChangeSet) -> Result<()> {
        let bytes = source.vectors.iter().map(|vector| vector.vector.len().saturating_mul(4)
            .saturating_add(vector.index.column.len()).saturating_add(256)).fold(0usize,usize::saturating_add);
        let credit = Arc::get_mut(&mut self._credit)
            .ok_or_else(|| Error::Other("source vector archive credit was shared before admission".into()))?;
        credit.try_grow_for(bytes,"sync","connected_authored_vector_bodies",
            "Raise the declared memory limit to retain accepted source vectors.")
    }

    pub(super) fn source_vectors_complete(&self,source: &ChangeSet) -> bool {
        self.originals.iter().enumerate().all(|(position,(row,shape,_,group))| row.deleted || shape.meta.columns.iter()
            .filter(|column| matches!(column.column_type,ColumnType::Vector(_)))
            .all(|column| {
                let body = group.as_ref().is_some_and(|group| source.vectors[group.clone()].iter()
                    .any(|vector| vector.index.column == column.name && !vector.vector.is_empty()));
                let explicit_null = column.nullable && matches!(row.values.get(&column.name),Some(Value::Null));
                body || (explicit_null && (group.is_some()
                    || self.source_vector_group_present.get(position) == Some(&false)))
            }))
    }

    /// The latest accepted mutation of each receiver-owned row, never the
    /// last competing wire member. The overlay credit funds this scalar map.
    pub(super) fn accepted_row_positions(
        &self,
        finalized: &WriteSet,
        coordinates: &ConnectedCoordinates,
        source_order: bool,
        owner: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<BTreeMap<(String, RowId), Lsn>> {
        let mut positions = BTreeMap::new();
        for (original, _, projected, _) in &self.originals {
            let Some(projected) = projected else { continue; };
            let pending = if source_order { original } else { projected };
            let key = Database::detached_accepted_row_key(pending, owner.clone())?;
            let accepted = finalized
                .config_writes
                .iter()
                .rev()
                .find(|(candidate, _)| candidate == &key)
                .map(|(_, value)| RedbPersistence::decode_config_value::<Option<RowId>>(value))
                .transpose()?
                .flatten();
            if let Some(row_id) = accepted {
                positions.insert(
                    (pending.table.clone(), row_id),
                    coordinates.local(pending.lsn)?,
                );
            }
        }
        Ok(positions)
    }

    /// Canonicalizing the working write set may erase an insert followed by
    /// a delete. The accepted source history must still carry both members.
    pub(super) fn accepted_row_history(
        &self,
        finalized: &WriteSet,
        coordinates: &ConnectedCoordinates,
        source_order: bool,
        owner: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<Vec<ChangeLogEntry>> {
        let mut history = Vec::new();
        for (original, _, projected, _) in &self.originals {
            let pending = if source_order { original } else {
                let Some(projected) = projected else { continue; };
                projected
            };
            let key = Database::detached_accepted_row_key(pending, owner.clone())?;
            let accepted = finalized
                .config_writes
                .iter()
                .rev()
                .find(|(candidate, _)| candidate == &key)
                .map(|(_, value)| RedbPersistence::decode_config_value::<Option<RowId>>(value))
                .transpose()?
                .flatten();
            let Some(row_id) = accepted else {
                continue;
            };
            let lsn = coordinates.local(pending.lsn)?;
            history.push(if pending.deleted {
                ChangeLogEntry::RowDelete {
                    table: pending.table.clone(),
                    row_id,
                    natural_key: pending.natural_key.clone(),
                    lsn,
                }
            } else {
                ChangeLogEntry::RowInsert {
                    table: pending.table.clone(),
                    row_id,
                    lsn,
                }
            });
        }
        Ok(history)
    }

    pub(super) fn authored_rows(
        &self,
        finalized: &WriteSet,
        coordinates: &ConnectedCoordinates,
        tx: TxId,
        source_order: bool,
        source: &ChangeSet,
        owner: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<Vec<(String, Arc<AuthoredRowShape>, VersionedRow)>> {
        let mut rows = Vec::new();
        for (original, meta, pending, vector_group) in &self.originals {
            if original.deleted {
                continue;
            }
            let pending = if source_order { original } else {
                let Some(pending) = pending else { continue; };
                pending
            };
            // Acceptance belongs to this exact transport occurrence. A final
            // same-key insert or standing lineage is not evidence that an
            // earlier competing member won. The private adjudicator records
            // the selected receiver RowId at the moment that member wins.
            let key = Database::detached_accepted_row_key(pending, owner.clone())?;
            let row_id = finalized
                .config_writes
                .iter()
                .rev()
                .find(|(candidate, _)| candidate == &key)
                .map(|(_, value)| RedbPersistence::decode_config_value::<Option<RowId>>(value))
                .transpose()?
                .flatten();
            let Some(row_id) = row_id else {
                continue;
            };
            let mut row = VersionedRow {
                row_id,
                values: original.values.clone(),
                created_tx: tx,
                deleted_tx: None,
                lsn: coordinates.local(original.lsn)?,
                created_at: original.created_at,
            };
            if source_order && let Some(group) = vector_group {
                for vector in &source.vectors[group.clone()] {
                    let column = meta.meta.columns.iter().find(|column| column.name == vector.index.column)
                        .ok_or_else(|| Error::UnknownVectorIndex { index: vector.index.clone() })?;
                    // A live nullable NULL has an empty vector lifecycle
                    // companion. Preserve the accepted SQL NULL in authored
                    // history; it is not a zero-dimensional vector body.
                    if vector.vector.is_empty()
                        && matches!(row.values.get(&vector.index.column),Some(Value::Null)) {
                        continue;
                    }
                    let _scratch = ReadCredit::try_new(owner.clone(),vector.vector.len().saturating_mul(8).saturating_add(512),
                        "sync","connected_authored_vector_quantization",
                        "Raise the declared memory limit to preserve this accepted vector occurrence.")?;
                    let value = Value::Vector(contextdb_vector::stored_vector_value(&vector.vector,column.quantization));
                    if let Some(slot) = row.values.get_mut(&vector.index.column) { *slot = value; }
                    else { row.values.insert(vector.index.column.clone(),value); }
                }
            }
            rows.push((original.table.clone(), meta.clone(), row));
        }
        Ok(rows)
    }
}

impl Database {
    /// Group ordinary source transactions separately, and connected positions
    /// by their one durable acceptance. The caller's output admission owns the
    /// grouping allocations until encoding/publication finishes.
    pub(crate) fn connected_delivery_groups(
        &self,
        changes: ChangeSet,
        output: &mut contextdb_vector::VectorCopyAdmission,
        owner: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<Vec<(ChangeSet, bool)>> {
        let count = changes
            .rows
            .len()
            .saturating_add(changes.edges.len())
            .saturating_add(changes.vectors.len())
            .saturating_add(changes.ddl.len());
        output.reserve(count.saturating_mul(768).saturating_add(256))?;
        let mut positions = BTreeMap::<Lsn, (Lsn, bool)>::new();
        for source in changes
            .rows
            .iter()
            .map(|row| row.lsn)
            .chain(changes.edges.iter().map(|edge| edge.lsn))
            .chain(changes.vectors.iter().map(|vector| vector.lsn))
            .chain(changes.ddl_lsn.iter().copied())
        {
            if !positions.contains_key(&source) {
                let membership = self.connected_membership_at(source, owner.clone())?;
                positions.insert(
                    source,
                    membership.map_or((source, false), |(_, end)| (end, true)),
                );
            }
        }
        let mut groups = BTreeMap::<Lsn, (ChangeSet, bool)>::new();
        for row in changes.rows {
            let (end, connected) = positions[&row.lsn];
            let group = groups.entry(end).or_default();
            group.0.rows.push(row);
            group.1 |= connected;
        }
        for edge in changes.edges {
            let (end, connected) = positions[&edge.lsn];
            let group = groups.entry(end).or_default();
            group.0.edges.push(edge);
            group.1 |= connected;
        }
        for vector in changes.vectors {
            let (end, connected) = positions[&vector.lsn];
            let group = groups.entry(end).or_default();
            group.0.vectors.push(vector);
            group.1 |= connected;
        }
        for (ddl, lsn) in changes.ddl.into_iter().zip(changes.ddl_lsn) {
            let (end, connected) = positions[&lsn];
            let group = groups.entry(end).or_default();
            group.0.ddl.push(ddl);
            group.0.ddl_lsn.push(lsn);
            group.1 |= connected;
        }
        Ok(groups.into_values().collect())
    }

    pub(crate) fn connected_delivery_ceiling(
        &self,
        lsn: Lsn,
        owner: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<Lsn> {
        Ok(self
            .connected_membership_at(lsn, owner)?
            .map_or(lsn, |(start, end)| {
                if lsn < end {
                    Lsn(start.0.saturating_sub(1))
                } else {
                    end
                }
            }))
    }
}

impl Database {
    pub(crate) fn expand_connected_positions(
        &self,
        positions: &mut HashSet<Lsn>,
        output: &mut contextdb_vector::VectorCopyAdmission,
        memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<()> {
        output.reserve(positions.len().saturating_mul(32).saturating_add(128))?;
        let original = positions.iter().copied().collect::<Vec<_>>();
        for position in original {
            if let Some((start, end)) = self.connected_membership_at(position, memory.clone())? {
                let length = usize::try_from(end.0 - start.0 + 1).map_err(|_| {
                    Error::SyncError("connected interval exceeds the process address space".into())
                })?;
                output.reserve(length.saturating_mul(128))?;
                positions.extend((start.0..=end.0).map(Lsn));
            }
        }
        Ok(())
    }
}

pub(super) enum PrivateApplyPhases {
    One(Option<ChangeSet>),
    Ordered { groups: std::vec::IntoIter<ChangeSet>, _credit: ReadCredit },
}
impl PrivateApplyPhases {
    pub(super) fn prepare(changes: ChangeSet, coordinates: Option<&ConnectedCoordinates>,
        owner: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>) -> Result<Self> {
        let Some(coordinates) = coordinates else { return Ok(Self::One(Some(changes))); };
        let controls = changes.rows.len().saturating_mul(std::mem::size_of::<RowChange>())
            .saturating_add(changes.edges.len().saturating_mul(std::mem::size_of::<EdgeChange>()))
            .saturating_add(changes.vectors.len().saturating_mul(std::mem::size_of::<VectorChange>()))
            .saturating_add(changes.ddl.len().saturating_mul(std::mem::size_of::<DdlChange>()))
            .saturating_add(changes.ddl_lsn.len().saturating_mul(std::mem::size_of::<Lsn>()))
            .saturating_mul(4)
            .saturating_add(coordinates.groups.len().saturating_mul(4096));
        let credit = ReadCredit::try_new(owner, controls, "sync", "connected_source_phase_controls",
            "Raise the declared memory limit to prepare this unit's source groups.")?;
        // This existing splitter moves bodies; it allocates only the admitted
        // group/tree/vector controls, never a second copy of row/vector data.
        Ok(Self::Ordered { groups: changes.split_by_data_lsn().into_iter(), _credit: credit })
    }
}
impl Iterator for PrivateApplyPhases {
    type Item = ChangeSet;
    fn next(&mut self) -> Option<ChangeSet> {
        match self { Self::One(group) => group.take(), Self::Ordered { groups, .. } => groups.next() }
    }
}

/// Only private received replay uses these records. Payloads precede their
/// exact reader/writer-adapter credit through every capture and error exit.
pub(super) struct PrivateChangedRows {
    // Bit one means an actual insertion; bit two means this phase replaced
    // an earlier pending row. A delete without an insert is not a target.
    rows: HashMap<String, HashMap<RowId, PrivateRowEffect>>,
    _credit: ReadCredit,
}
struct PrivateRowEffect { kind: u8, before: Option<VersionedRow> }
impl PrivateChangedRows {
    fn new(owner: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>) -> Self {
        Self { rows: HashMap::new(), _credit: ReadCredit::new(owner) }
    }
    fn insert(&mut self, table: &str, row: RowId) -> Result<()> { self.insert_kind(table, row, 1) }
    fn insert_kind(&mut self, table: &str, row: RowId, kind: u8) -> Result<()> {
        if let Some(existing) = self.rows.get_mut(table).and_then(|rows| rows.get_mut(&row)) {
            existing.kind |= kind;
            return Ok(());
        }
        let first = !self.rows.contains_key(table);
        self._credit.try_grow_for(1024usize.saturating_add(if first { table.len().saturating_add(2048) } else { 0 }),
            "sync", "connected_changed_occurrence", "Raise the declared memory limit to receive this unit.")?;
        if let Some(rows) = self.rows.get_mut(table) { rows.insert(row, PrivateRowEffect { kind, before: None }); }
        else {
            let mut rows = HashMap::new(); rows.insert(row, PrivateRowEffect { kind, before: None });
            self.rows.insert(table.to_owned(), rows);
        }
        Ok(())
    }
    fn capture_before(&mut self, table: &str, row: &VersionedRow) -> Result<()> {
        self.insert_kind(table, row.row_id, 2)?;
        if self.before(table, row.row_id).is_some() { return Ok(()); }
        self._credit.try_grow_for(crate::memory_accounting::preparation_row_copy_bytes(row)?,
            "sync", "connected_previous_row", "Raise the declared memory limit to retain a source-phase predecessor.")?;
        let copied = crate::memory_accounting::preparation_copy_row(row)?;
        self.rows.get_mut(table).expect("admitted table").get_mut(&row.row_id)
            .expect("admitted row").before = Some(copied);
        Ok(())
    }
    pub(super) fn before(&self, table: &str, row: RowId) -> Option<&VersionedRow> {
        self.rows.get(table)?.get(&row)?.before.as_ref()
    }
    pub(super) fn predecessors(&self) -> impl Iterator<Item = (&str, &VersionedRow)> {
        self.rows.iter().flat_map(|(table, rows)| rows.values()
            .filter_map(move |effect| effect.before.as_ref().map(|row| (table.as_str(), row))))
    }
    pub(super) fn contains(&self, table: &str, row: RowId) -> bool {
        self.rows.get(table).and_then(|rows| rows.get(&row)).is_some_and(|effect| effect.kind & 1 != 0)
    }
    pub(super) fn is_update(&self, table: &str, row: RowId) -> bool {
        self.rows.get(table).and_then(|rows| rows.get(&row)).is_some_and(|effect| effect.kind & 2 != 0)
    }
}
#[derive(Clone, Copy)]
pub(super) struct PrivateReplayContext<'a> {
    pub(super) coordinates: Option<&'a ConnectedCoordinates>,
    pub(super) authoritative_generations: Option<&'a [ReceivedSchemaOrderEntry]>,
    pub(super) receiver_tx: TxId,
    pub(super) end: Lsn,
}
pub(super) struct PrivateSyntheticTriggerFiring {
    pub(super) trigger_name: String,
    pub(super) table: String,
    pub(super) row_id: RowId,
    pub(super) lsn: Lsn,
    pub(super) tx: TxId,
}
pub(super) struct PrivatePrefixEffects {
    pub(super) audits: Vec<(Lsn, trigger::PendingTriggerAudit)>,
    pub(super) synthetic_firings: Vec<PrivateSyntheticTriggerFiring>,
    sink_events: Vec<event_bus::PreparedSinkEvent>,
    sink_cursor: Option<(u64, u64)>,
    source_creation_keys: HashSet<String>,
    pub(super) derived_rows: Vec<(String, Arc<AuthoredRowShape>, VersionedRow)>,
    pub(super) derived_history: Vec<ChangeLogEntry>,
    derived_positions: HashMap<String, HashMap<RowId, Lsn>>,
    vector_alter_credits: Vec<ReadCredit>,
    graph_statement_owner: Option<crate::memory_accounting::OwnedMemoryReservation>,
    _credit: ReadCredit,
    // Existing staged row debits move here with their owning capture.
    captured_rows_invoice:Option<ReadCredit>,
}
impl PrivatePrefixEffects {
    pub(super) fn for_refusal_metadata(configs: &[(String, Vec<u8>)], result: &ApplyResult,
        owner: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>) -> Result<Self> {
        let mut effects = Self::new(owner);
        let bytes = configs.iter().map(|(key, value)| key.len().saturating_add(value.len())
            .saturating_mul(8).saturating_add(512)).sum::<usize>()
            .saturating_add(crate::persistence::content_identity::original_record_work_bytes(result)?);
        effects._credit.try_grow_for(bytes, "sync", "connected_refusal_metadata",
            "Raise the declared memory limit to retain this unit's definitive outcome.")?;
        Ok(effects)
    }

    /// Only final surviving callback rows need a position in the current image.
    /// Earlier callback bodies/history remain independently retained below.
    pub(super) fn add_derived_positions(&mut self, finalized: &WriteSet,
        positions: &mut BTreeMap<(String, RowId), Lsn>) -> Result<()> {
        for (table,row) in &finalized.relational_inserts {
            if let Some(lsn) = self.derived_positions.get(table).and_then(|rows| rows.get(&row.row_id)).copied() {
                self._credit.try_grow_for(table.len().saturating_add(512),"sync","connected_derived_position",
                    "Raise the declared memory limit to retain callback history positions.")?;
                positions.entry((table.clone(),row.row_id)).or_insert(lsn);
            }
        }
        Ok(())
    }

    pub(super) fn synthetic_firings_for_commit(&mut self) -> Result<&[PrivateSyntheticTriggerFiring]> {
        self._credit.try_grow_for(self.synthetic_firings.iter().fold(0usize,|bytes,firing| bytes
            .saturating_add(firing.trigger_name.len()).saturating_add(std::mem::size_of::<TriggerAuditEntry>()
                .saturating_mul(2)).saturating_add(512)),"sync","connected_plain_committed_audits",
            "Raise the declared memory limit to retain source trigger audits.")?;
        Ok(&self.synthetic_firings)
    }

    pub(super) fn take_prepared_sinks(&mut self, current: u64) -> Result<Option<event_bus::PreparedReceivedSinkEvents>> {
        let Some((base, next_queue_id)) = self.sink_cursor else { return Ok(None); };
        if current != base {
            return Err(Error::Other("received sink cursor changed during private replay".into()));
        }
        Ok(Some(event_bus::PreparedReceivedSinkEvents { events: std::mem::take(&mut self.sink_events), next_queue_id }))
    }
    pub(super) fn committed_audits(&mut self, tx: TxId) -> Result<Vec<TriggerAuditEntry>> {
        let bytes = self.audits.iter().map(|(_, pending)| pending.trigger_name.len()
            .saturating_add(std::mem::size_of::<TriggerAuditEntry>().saturating_mul(4))
            .saturating_add(512)).sum::<usize>();
        self._credit.try_grow_for(bytes, "sync", "connected_committed_audits",
            "Raise the declared memory limit to retain this unit's audits.")?;
        Ok(self.audits.iter().map(|(lsn, pending)| TriggerAuditEntry {
            trigger_name: pending.trigger_name.clone(), firing_tx: tx, firing_lsn: *lsn,
            depth: pending.depth, cascade_row_count: pending.cascade_row_count,
            status: TriggerAuditStatus::Fired,
        }).collect())
    }

    pub(super) fn take_captured_rows_invoice(&mut self,invoice:Option<ReadCredit>) {
        if invoice.is_none() {return;}
        debug_assert!(self.captured_rows_invoice.is_none(),"one captured row invoice");
        self.captured_rows_invoice=invoice;
    }
    fn new(owner: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>) -> Self {
        Self { audits: Vec::new(), synthetic_firings: Vec::new(), sink_events: Vec::new(), sink_cursor: None, source_creation_keys: HashSet::new(), derived_rows: Vec::new(), derived_history: Vec::new(), derived_positions: HashMap::new(), vector_alter_credits: Vec::new(), graph_statement_owner: None, _credit: ReadCredit::new(owner), captured_rows_invoice:None }
    }
}
#[derive(Default)]
pub(super) struct DetachedSyncCapture {
    pub(super) write_set: WriteSet,
    pub(super) refused: bool,
    pub(super) prefix: Option<PrivatePrefixEffects>,
    pub(super) rows_invoice:Option<ReadCredit>,
}
struct PrivatePhaseState {
    host: usize,
    tx: TxId,
    changed: PrivateChangedRows,
}
struct PrivateUnitState {
    host: usize,
    tx: TxId,
    canonical_tx: TxId,
    end: Lsn,
    callbacks_prepared: bool,
    graph_origin: Option<crate::persistence::received_working_graph::WorkingGraphOrigin>,
    callback_config_cursor: usize,
    derived_shapes: HashMap<String, Arc<AuthoredRowShape>>,
    owner: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    effects: PrivatePrefixEffects,
}
thread_local! {
    static PRIVATE_PHASE: std::cell::RefCell<Option<PrivatePhaseState>> = const { std::cell::RefCell::new(None) };
    static PRIVATE_UNIT: std::cell::RefCell<Option<PrivateUnitState>> = const { std::cell::RefCell::new(None) };
}
pub(super) struct PrivatePhaseScope<'a> {
    db: &'a Database,
    tx: TxId,
    previous: Option<PrivatePhaseState>,
    active: bool,
    _same_thread: std::marker::PhantomData<std::rc::Rc<()>>,
}
impl<'a> PrivatePhaseScope<'a> {
    pub(super) fn enter(db: &'a Database, tx: TxId,
        owner: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>) -> Self {
        let previous = PRIVATE_PHASE.with(|slot| slot.replace(Some(PrivatePhaseState {
            host: db.tx_mgr.identity(), tx, changed: PrivateChangedRows::new(owner),
        })));
        Self { db, tx, previous, active: true, _same_thread: std::marker::PhantomData }
    }
    pub(super) fn finish(mut self) -> Result<PrivateChangedRows> {
        let current = PRIVATE_PHASE.with(|slot| slot.replace(self.previous.take()));
        self.active = false;
        current.filter(|state| state.host == self.db.tx_mgr.identity() && state.tx == self.tx)
            .map(|state| state.changed).ok_or_else(|| Error::Other("private source phase lost its owner".into()))
    }
}
impl Drop for PrivatePhaseScope<'_> {
    fn drop(&mut self) {
        if self.active { PRIVATE_PHASE.with(|slot| { slot.replace(self.previous.take()); }); }
    }
}
pub(super) struct PrivateUnitScope<'a> {
    db: &'a Database,
    tx: TxId,
    previous: Option<PrivateUnitState>,
    active: bool,
    _same_thread: std::marker::PhantomData<std::rc::Rc<()>>,
}
impl<'a> PrivateUnitScope<'a> {
    pub(super) fn enter(db: &'a Database, tx: TxId, canonical_tx: TxId, end: Lsn,
        owner: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>) -> Result<Self> {
        if !db.capture_detached_sync_write_set.load(Ordering::SeqCst) {
            return Err(Error::Other("source phases require private captured replay".into()));
        }
        let previous = PRIVATE_UNIT.with(|slot| slot.replace(Some(PrivateUnitState {
            host: db.tx_mgr.identity(), tx, canonical_tx, end, callbacks_prepared: false, graph_origin: None, callback_config_cursor: 0, derived_shapes: HashMap::new(),
            effects: PrivatePrefixEffects::new(owner.clone()), owner,
        })));
        Ok(Self { db, tx, previous, active: true, _same_thread: std::marker::PhantomData })
    }
    pub(super) fn finish(mut self) -> Result<PrivatePrefixEffects> {
        let current = PRIVATE_UNIT.with(|slot| slot.replace(self.previous.take()));
        self.active = false;
        current.filter(|state| state.host == self.db.tx_mgr.identity() && state.tx == self.tx)
            .map(|state| state.effects).ok_or_else(|| Error::Other("private source unit lost its owner".into()))
    }
}
impl Drop for PrivateUnitScope<'_> {
    fn drop(&mut self) {
        if !self.active { return; }
        let current = PRIVATE_UNIT.with(|slot| slot.replace(self.previous.take()));
        if let Some(state) = current.filter(|state|
            state.host == self.db.tx_mgr.identity() && state.tx == self.tx) {
            // On an early ? or unwind the pending write set can still own
            // callback/ALTER keys funded by this unit. Destroy it while that
            // exact credit is live. Use the existing operation owner, without
            // creating a reader child or entering public callback tx control.
            if let Ok(ws) = self.db.tx_mgr.rollback_write_set(self.tx) {
                self.db.pending_event_bus_ddl.lock().remove(&self.tx);
                self.db.pending_local_schema_stages.lock().remove(&self.tx);
                self.db.pending_commit_metadata.lock().remove(&self.tx);
                let mut vector_owner = crate::vector_read_memory::admission(state.owner.clone());
                let _ = self.db.dispose_private_abandoned_write_set(ws,&mut vector_owner);
            }
            // A private commit may already have captured an owning write set
            // before a later failure. Its payload precedes this credit in the
            // capture on ordinary drop and on the enclosing database unwind.
            let mut captured = self.db.detached_sync_write_set.lock();
            if let Some(captured) = captured.as_mut() { captured.prefix = Some(state.effects); }
            else {
                // A private commit can have published into its unpublished
                // memory store before a later hook fails. Keep this exact
                // credit through that store's destruction even without WS.
                let mut owner = self.db.finalization.non_vector_memory.lock();
                if let Some(owner) = owner.as_mut() {
                    debug_assert!(owner.aborted_private_effects.is_none(),"one source replay owns each private database");
                    owner.aborted_private_effects = Some(state.effects);
                }
            }
        }
    }
}
impl Database {
    pub(super) fn note_private_source_row_change(&self, tx: TxId, table: &str, row: RowId) -> Result<()> {
        PRIVATE_PHASE.with(|slot| {
            let mut state = slot.borrow_mut();
            match state.as_mut().filter(|state| state.host == self.tx_mgr.identity() && state.tx == tx) {
                Some(state) => state.changed.insert(table, row),
                None => Ok(()),
            }
        })
    }
    pub(super) fn note_private_pending_row_replacement(&self, tx: TxId, table: &str, row: RowId) -> Result<()> {
        PRIVATE_PHASE.with(|slot| {
            let mut state = slot.borrow_mut();
            let Some(state) = state.as_mut().filter(|state| state.host == self.tx_mgr.identity() && state.tx == tx)
                else { return Ok(()); };
            // An insert and its later replacement/delete inside this same
            // source transaction still has no predecessor at phase start.
            if state.changed.contains(table, row) && state.changed.before(table, row).is_none() {
                return Ok(());
            }
            let pending = self.tx_mgr.with_write_set(tx, |ws| -> Result<bool> {
                if let Some((_, pending)) = ws.relational_inserts.iter().rev()
                    .find(|(name, pending)| name == table && pending.row_id == row) {
                    state.changed.capture_before(table, pending)?;
                    Ok(true)
                } else { Ok(false) }
            })??;
            if !pending {
                let snapshot = self.snapshot_for_read();
                if self.relational_store.uses_disk_backed_committed_bodies() {
                    let owner=self.private_replay_owner(tx).ok_or_else(||Error::Other(
                        "paged private predecessor has no preparation owner".into()))?;
                    if let Some(previous)=self.committed_row_at(table,row,snapshot,owner)? {
                        state.changed.capture_before(table,&previous)?;
                    }
                }else{
                    let tables = self.relational_store.tables.read();
                    if let Some(previous) = tables.get(table).and_then(|rows| rows.iter().rev()
                        .find(|previous| previous.row_id == row && previous.visible_at(snapshot))) {
                        state.changed.capture_before(table, previous)?;
                    }
                }
            }
            Ok(())
        })
    }
    pub(super) fn private_callback_copy_credit(&self, tx: TxId, name: &str, table: &str,
        values: &HashMap<String, Value>) -> Result<Option<ReadCredit>> {
        let owner = PRIVATE_UNIT.with(|slot| slot.borrow().as_ref().filter(|state|
            state.host == self.tx_mgr.identity() && state.tx == tx).map(|state| state.owner.clone()));
        owner.map(|owner| {
            let bytes = crate::memory_accounting::preparation_values_copy_bytes(values)?
                .saturating_add(name.len()).saturating_add(table.len())
                .saturating_add(std::mem::size_of::<TriggerContext>()).saturating_add(128);
            ReadCredit::try_new(owner, bytes, "sync", "connected_callback_context",
                "Raise the declared memory limit to prepare this callback context.")
        }).transpose()
    }
    pub(super) fn admit_private_prefix_audit(&self, tx: TxId, name: &str) -> Result<()> {
        PRIVATE_UNIT.with(|slot| {
            let mut state = slot.borrow_mut();
            if let Some(state) = state.as_mut().filter(|state| state.host == self.tx_mgr.identity() && state.tx == tx) {
                state.effects._credit.try_grow_for(name.len().saturating_mul(4).saturating_add(1024),
                    "sync", "connected_prefix_audits", "Raise the declared memory limit to retain this unit's audits.")?;
            }
            Ok(())
        })
    }
    pub(super) fn retain_private_prefix_audits(&self, tx: TxId, lsn: Lsn,
        pending: Vec<trigger::PendingTriggerAudit>) -> Result<()> {
        PRIVATE_UNIT.with(|slot| {
            let mut state = slot.borrow_mut();
            let state = state.as_mut().filter(|state| state.host == self.tx_mgr.identity() && state.tx == tx)
                .ok_or_else(|| Error::Other("private prefix audit lost its unit".into()))?;
            state.effects._credit.try_grow_for(pending.iter().fold(0usize,|bytes,audit|
                bytes.saturating_add(audit.trigger_name.len()).saturating_add(
                    std::mem::size_of::<(Lsn,trigger::PendingTriggerAudit)>().saturating_mul(2)).saturating_add(512)),
                "sync","connected_prefix_audit_retention","Raise the declared memory limit to retain source callback audits.")?;
            state.effects.audits.extend(pending.into_iter().map(|audit| (lsn, audit)));
            Ok(())
        })
    }
    pub(super) fn admit_private_prefix_sink_event(&self, tx: TxId, bytes: usize) -> Result<()> {
        PRIVATE_UNIT.with(|slot| {
            let mut state = slot.borrow_mut();
            let state = state.as_mut().filter(|state| state.host == self.tx_mgr.identity() && state.tx == tx)
                .ok_or_else(|| Error::Other("private sink event lost its unit".into()))?;
            state.effects._credit.try_grow_for(bytes, "sync", "connected_prefix_sink_event",
                "Raise the declared memory limit to retain this unit's sink events.")
        })
    }
    pub(super) fn retain_private_prefix_sink_events(&self, tx: TxId, base: u64, next: u64,
        events: Vec<event_bus::PreparedSinkEvent>) -> Result<()> {
        PRIVATE_UNIT.with(|slot| {
            let mut state = slot.borrow_mut();
            let state = state.as_mut().filter(|state| state.host == self.tx_mgr.identity() && state.tx == tx)
                .ok_or_else(|| Error::Other("private sink event capture lost its unit".into()))?;
            let cursor = state.effects.sink_cursor.get_or_insert((base, base));
            if cursor.1 != base { return Err(Error::Other("private sink cursor is discontinuous".into())); }
            cursor.1 = next;
            state.effects.sink_events.extend(events);
            Ok(())
        })
    }
    pub(super) fn private_prefix_effects_prepared(&self, ws: &WriteSet) -> bool {
        ws.config_writes.iter().any(|(key, _)| key == "__contextdb.detached_capture_owner.v1")
            && PRIVATE_UNIT.with(|slot| slot.borrow().as_ref().is_some_and(|state|
                state.host == self.tx_mgr.identity() && state.callbacks_prepared))
    }
    pub(super) fn prepare_private_callbacks_for_commit(&self, tx: TxId) -> Result<()> {
        PRIVATE_UNIT.with(|slot| {
            let mut state = slot.borrow_mut();
            let state = state.as_mut().filter(|state| state.host == self.tx_mgr.identity() && state.tx == tx)
                .ok_or_else(|| Error::Other("private callback preparation lost its unit".into()))?;
            state.callbacks_prepared = true;
            Ok(())
        })
    }
    pub(super) fn retain_private_final_audits(&self, tx: TxId,
        pending: Vec<trigger::PendingTriggerAudit>) -> Result<()> {
        PRIVATE_UNIT.with(|slot| {
            let mut state = slot.borrow_mut();
            if let Some(state) = state.as_mut().filter(|state|
                state.host == self.tx_mgr.identity() && state.tx == tx && !state.callbacks_prepared) {
                state.effects._credit.try_grow_for(pending.iter().fold(0usize,|bytes,audit|
                    bytes.saturating_add(audit.trigger_name.len()).saturating_add(
                        std::mem::size_of::<(Lsn,trigger::PendingTriggerAudit)>().saturating_mul(2)).saturating_add(512)),
                    "sync","connected_final_audit_retention","Raise the declared memory limit to retain source callback audits.")?;
                state.effects.audits.extend(pending.into_iter().map(|audit| (state.end, audit)));
            }
            Ok(())
        })
    }
    pub(super) fn private_callbacks_already_prepared(&self, tx: TxId) -> bool {
        PRIVATE_UNIT.with(|slot| slot.borrow().as_ref().is_some_and(|state|
            state.host == self.tx_mgr.identity() && state.tx == tx && state.callbacks_prepared))
    }
    pub(super) fn private_replay_owner(&self, tx: TxId) -> Option<Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>> {
        PRIVATE_UNIT.with(|slot| slot.borrow().as_ref().filter(|state|
            state.host == self.tx_mgr.identity() && state.tx == tx).map(|state| state.owner.clone()))
    }
    pub(super) fn private_receiver_transaction(&self, tx: TxId) -> Option<TxId> {
        PRIVATE_UNIT.with(|slot| slot.borrow().as_ref().filter(|state|
            state.host == self.tx_mgr.identity() && state.tx == tx).map(|state| state.canonical_tx))
    }
}

impl Database {
    /// Retire graph inserts owned by pending graph-table rows before DROP
    /// destroys the row identities used by the existing graph projection.
    pub(super) fn drop_private_pending_graph_rows(&self, tx: TxId, table: &str) -> Result<()> {
        if !self.table_meta(table).is_some_and(|meta| has_graph_edge_table_shape(&meta)) {
            return Ok(());
        }
        // The current private graph can also contain an older committed edge
        // with this pending row's identity. Preserve that identity before the
        // pending row is retired: the later committed-row DROP cursor cannot
        // recover it from the write set after drop_private_pending_relational_rows.
        // Each pass retains one independently admitted name, never an identity
        // set or adjacency image. The same working loan supplies the exact identity seek.
        if let Some(source) = crate::persistence::received_working_source::WorkingReadSource::current_for(
            self.tx_mgr.identity())? {
            source.with_original(|original| {
                let mut position = 0usize;
                loop {
                    let candidate = self.tx_mgr.with_ordinary_read(tx, |ws| {
                        ws.relational_inserts.iter().enumerate().skip(position)
                            .find_map(|(index, (name, row))| {
                                if name != table { return None; }
                                let from = *row.values.get("source_id")?.as_uuid()?;
                                let to = *row.values.get("target_id")?.as_uuid()?;
                                let kind = row.values.get("edge_type")?.as_text()?;
                                Some((index, row.row_id, from, to, kind.len()))
                            })
                    })?;
                    let Some((index, row_id, from, to, length)) = candidate else { break; };
                    // Admission happens outside the transaction-state guard;
                    // the exact source is rechecked before allocating the copy.
                    let identity_owner = crate::memory_accounting::reserve_preparation_from_original(
                        original, length, "sync", "private_graph_drop_identity",
                        "Raise MEMORY_LIMIT to prepare graph table removal.")?;
                    let kind = self.tx_mgr.with_ordinary_read(tx, |ws| -> Result<String> {
                        let (name, row) = ws.relational_inserts.get(index).ok_or_else(||
                            Error::Other("private graph DROP row changed during admission".into()))?;
                        let kind = row.values.get("edge_type").and_then(Value::as_text)
                            .ok_or_else(|| Error::Other("private graph DROP identity changed during admission".into()))?;
                        if name != table || row.row_id != row_id || kind.len() != length
                            || row.values.get("source_id").and_then(Value::as_uuid) != Some(&from)
                            || row.values.get("target_id").and_then(Value::as_uuid) != Some(&to) {
                            return Err(Error::Other("private graph DROP identity changed during admission".into()));
                        }
                        crate::memory_accounting::preparation_copy_string(kind)
                    })??;
                    source.remove_private_graph_identity(from, &kind, to)?;
                    drop(kind);
                    drop(identity_owner);
                    position = index + 1;
                }
                Ok(())
            })?;
        }
        // Retire the pending graph bodies only after the private source has
        // consumed their row identities. Committed rows are handled separately
        // by graph_edges_after_table_drop before its schema removal.
        loop {
            let retired = self.tx_mgr.with_write_set(tx, |ws| -> Result<_> {
                if ws.ordered_staged_rows.checkpoint_active() {
                    return Err(Error::Other("private graph DROP retained a statement checkpoint".into()));
                }
                let position = ws.adj_inserts.iter().position(|edge|
                    ws.relational_inserts.iter().any(|(name, row)| name == table
                        && row.values.get("source_id").and_then(Value::as_uuid) == Some(&edge.source)
                        && row.values.get("target_id").and_then(Value::as_uuid) == Some(&edge.target)
                        && row.values.get("edge_type").and_then(Value::as_text) == Some(edge.edge_type.as_str())));
                Ok(position.map(|position| ws.adj_inserts.remove(position)))
            })??;
            let Some(edge) = retired else { break; };
            let bytes = edge.estimated_bytes();
            drop(edge);
            self.finalization.accountant.release(bytes);
        }
        Ok(())
    }

    /// Retire pending row bodies through their recorded owners before DROP
    /// destroys the declaration needed by legacy fallback pricing. No row
    /// mutation policy is consulted: this is the private schema projection.
    pub(super) fn drop_private_pending_relational_rows(&self, tx: TxId, table: &str) -> Result<()> {
        self.tx_mgr.with_write_set(tx, |ws| -> Result<()> {
            if ws.ordered_staged_rows.checkpoint_active() {
                return Err(Error::Other("private schema replay retained a statement checkpoint".into()));
            }
            while let Some(position) = ws.relational_inserts.iter().position(|(name, _)| name == table) {
                let row_id = ws.relational_inserts[position].1.row_id;
                let bytes = self.staged_row_release_bytes(ws, table,
                    &ws.relational_inserts[position].1, self.table_meta(table));
                ws.ordered_staged_rows.remove_statement_row(table, row_id)?;
                ws.ordered_staged_rows.remove(table, row_id);
                let retired = ws.relational_inserts.remove(position);
                ws.forget_relational_insert_admitted_bytes(table, row_id);
                drop(retired);
                self.finalization.accountant.release(bytes);
            }
            ws.relational_deletes.retain(|(name, _, _)| name != table);
            ws.relational_delete_predicates.retain(|predicate| predicate.table != table);
            ws.relational_insert_source_lsns.remove(table);
            ws.relational_insert_source_kinds.remove(table);
            ws.sync_source_provenance_marks.retain(|(name,_,_,_)| name != table);
            ws.rebind_ordered_staged_rows();
            Ok(())
        })?
    }

    /// Transform one private pending body at a time after an ALTER, including
    /// callback-derived rows. The old admission stays live until its body is
    /// destroyed; the replacement and its postings own independent invoices.
    pub(super) fn alter_private_pending_relational_rows(&self, tx: TxId,
        ddl: &DdlChange, previous: Option<&TableMeta>, owner: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>) -> Result<()> {
        let DdlChange::AlterTable { name, constraints, .. } = ddl else { return Ok(()); };
        let mut shape_credit = ReadCredit::new(owner.clone());
        let meta = {
            let tables = self.relational_store.table_meta.read();
            let Some(meta) = tables.get(name) else { return Ok(()); };
            shape_credit.try_grow_for(crate::persistence::content_identity::original_record_work_bytes(meta)?
                .saturating_add(ddl.estimated_bytes().saturating_mul(8)).saturating_add(4096),
                "sync", "connected_pending_shape", "Raise the declared memory limit to transform pending rows.")?;
            meta.clone()
        };
        let mut renames = sync_column_renames_from_constraints(constraints);
        if let Some((from,to)) = sync_vector_rename_from_constraints(constraints)
            && let Some(old) = previous.and_then(|meta| meta.columns.iter().find(|column| column.name == from))
            && let Some(new) = meta.columns.iter().find(|column| column.name == to)
            && old.column_type == new.column_type && old.quantization == new.quantization
            && matches!(old.column_type,ColumnType::Vector(_)) {
            renames.push((from,to));
        }
        self.tx_mgr.with_write_set(tx, |ws| -> Result<()> {
            if ws.ordered_staged_rows.checkpoint_active() {
                return Err(Error::Other("private schema replay retained a statement checkpoint".into()));
            }
            for position in 0..ws.relational_inserts.len() {
                if ws.relational_inserts[position].0 != *name { continue; }
                let mut copy_credit = ReadCredit::new(owner.clone());
                copy_credit.try_grow_for(crate::memory_accounting::preparation_row_copy_bytes(
                    &ws.relational_inserts[position].1)?
                    .saturating_add(crate::persistence::content_identity::original_record_work_bytes(&meta)?)
                    .saturating_add(ddl.estimated_bytes().saturating_mul(8)).saturating_add(4096),
                    "sync", "connected_pending_row_transform", "Raise the declared memory limit to transform pending rows.")?;
                let mut row = crate::memory_accounting::preparation_copy_row(&ws.relational_inserts[position].1)?;
                for (from, to) in &renames {
                    if !meta.columns.iter().any(|column| column.name == *from)
                        && meta.columns.iter().any(|column| column.name == *to)
                        && let Some(value) = row.values.remove(from) {
                        row.values.insert(to.clone(), value);
                    }
                }
                if let Some(previous) = previous {
                    // The ordinary ALTER door clears the old row projection
                    // when a vector's dimension/quantization lifetime changes.
                    for old in &previous.columns {
                        if matches!(old.column_type,ColumnType::Vector(_))
                            && meta.columns.iter().find(|new| new.name == old.name).is_some_and(|new|
                                old.column_type != new.column_type || old.quantization != new.quantization) {
                            row.values.remove(&old.name);
                        }
                    }
                }
                row.values.retain(|column, _| meta.columns.iter().any(|declared| declared.name == *column));
                for column in &meta.columns {
                    if !row.values.contains_key(&column.name) {
                        let value = column.default.as_deref()
                            .map(crate::executor::evaluate_stored_default_expr).transpose()?.unwrap_or(Value::Null);
                        row.values.insert(column.name.clone(), value);
                    }
                }
                let old_bytes = self.staged_row_release_bytes(ws, name,
                    &ws.relational_inserts[position].1, Some(meta.clone()));
                let admission = self.admit_retained_row_owned(name, &row.values, &meta,
                    "sync", "connected_pending_row", "Raise the declared memory limit to transform pending rows.")?;
                self.relational.stage_ordered_row_from_descriptor(ws, name, &row, position, &meta)?;
                let retired = std::mem::replace(&mut ws.relational_inserts[position].1, row);
                ws.set_relational_insert_admitted_bytes(name, retired.row_id, admission.bytes());
                admission.transfer_to_staged();
                drop(retired);
                self.finalization.accountant.release(old_bytes);
            }
            Ok(())
        })?
    }
}

impl Database {
    /// Each intermediate declaration must be handled by the prefix kernel.
    /// Pending vector lifetimes move through its funded ALTER/DROP transforms.
    /// A graph-backed table can accompany table-only schema replay when no
    /// source operation touches it. Graph lifetimes still use the established
    /// replacement kernel; mixed data units remain outside this opening.
    pub(super) fn private_source_order_supported(changes: &ChangeSet,
        live: &HashMap<String, TableMeta>, projected: &HashMap<String, TableMeta>,
        owner: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>) -> Result<bool> {
        if !changes.edges.is_empty() { return Ok(false); }
        let graph_bearing = |meta: &TableMeta|
            has_graph_edge_table_shape(meta) || !meta.dag_edge_types.is_empty();
        if live.values().chain(projected.values()).any(graph_bearing) {
            // Only unrelated table-schema units are newly admitted. Rows can
            // fire callbacks and vectors can carry row ownership; neither is
            // evidence of an untouched graph, even when explicit edges is empty.
            if !changes.rows.is_empty() || !changes.vectors.is_empty()
                || changes.ddl.iter().any(|ddl| !matches!(ddl,
                    DdlChange::CreateTable { .. } | DdlChange::AlterTable { .. }
                    | DdlChange::DropTable { .. })) { return Ok(false); }
            for ddl in &changes.ddl {
                if let Some(table) = Self::ddl_affected_table(ddl)
                    && live.get(table).into_iter().chain(projected.get(table)).any(graph_bearing) {
                    return Ok(false);
                }
            }
        }
        let mut catalog_credit = ReadCredit::new(owner);
        catalog_credit.try_grow_for(live.iter().fold(0usize,|bytes,(table,meta)| bytes
            .saturating_add(meta.estimated_bytes().saturating_mul(4))
            .saturating_add(table.len()).saturating_add(512)),
            "sync","connected_prefix_catalog","Raise the declared memory limit to inspect intermediate declarations.")?;
        let mut tables = live.clone();
        for ddl in &changes.ddl {
            catalog_credit.try_grow_for(ddl.estimated_bytes().saturating_mul(8).saturating_add(4096),
                "sync","connected_intermediate_shape","Raise the declared memory limit to inspect intermediate declarations.")?;
            // Inspect both sides of each source step. DROP or an ALTER that
            // removes edge shape must not hide the preceding graph lifetime;
            // a repeated final declaration must not hide an intermediate one.
            if let Some(meta) = Self::ddl_affected_table(ddl).and_then(|table| tables.get(table)) {
                if graph_bearing(meta) { return Ok(false); }
            }
            Self::apply_sync_table_ddl_to_projected_meta(&mut tables,ddl);
            if let Some(meta) = Self::ddl_affected_table(ddl).and_then(|table| tables.get(table)) {
                if graph_bearing(meta) { return Ok(false); }
            }
        }
        Ok(true)
    }
}

pub(super) struct PrivateTriggerBaseline {
    pub(super) write_set: WriteSet,
    _credit: ReadCredit,
}
impl Database {
    pub(super) fn private_trigger_row_copy_credit(&self, tx: TxId, table: &str,
        row: &VersionedRow) -> Result<Option<ReadCredit>> {
        self.private_replay_owner(tx).map(|owner| ReadCredit::try_new(owner,
            crate::memory_accounting::preparation_row_copy_bytes(row)?.saturating_add(table.len()).saturating_add(256),
            "sync", "connected_trigger_row_copy", "Raise the declared memory limit to dispatch this source row.")).transpose()
    }
    pub(super) fn capture_private_trigger_baseline(&self, tx: TxId) -> Result<Option<PrivateTriggerBaseline>> {
        let Some(owner) = self.private_replay_owner(tx) else { return Ok(None); };
        self.tx_mgr.with_write_set(tx, |ws| -> Result<Option<PrivateTriggerBaseline>> {
            let mut bytes = 4096usize;
            for (table,row) in &ws.relational_inserts {
                bytes = bytes.saturating_add(crate::memory_accounting::preparation_row_copy_bytes(row)?)
                    .saturating_add(table.len()).saturating_add(256);
            }
            for (table,_,_) in &ws.relational_deletes { bytes = bytes.saturating_add(table.len()).saturating_add(128); }
            for predicate in &ws.relational_delete_predicates {
                bytes = bytes.saturating_add(predicate.table.len()).saturating_add(256);
                for (column,value) in &predicate.predicates {
                    bytes = bytes.saturating_add(column.len()).saturating_add(value.estimated_bytes().saturating_mul(8)).saturating_add(128);
                }
            }
            for edge in &ws.adj_inserts { bytes = bytes.saturating_add(edge.estimated_bytes().saturating_mul(4)).saturating_add(128); }
            for (_,kind,_,_) in &ws.adj_deletes { bytes = bytes.saturating_add(kind.len()).saturating_add(128); }
            for vector in &ws.vector_inserts {
                bytes = bytes.saturating_add(vector.vector.len().saturating_mul(4))
                    .saturating_add(vector.index.table.len()).saturating_add(vector.index.column.len()).saturating_add(256);
            }
            for (index,_,_) in &ws.vector_deletes { bytes = bytes.saturating_add(index.table.len()).saturating_add(index.column.len()).saturating_add(128); }
            for (index,_,_,_) in &ws.vector_moves { bytes = bytes.saturating_add(index.table.len()).saturating_add(index.column.len()).saturating_add(128); }
            for (key,value) in &ws.config_writes { bytes = bytes.saturating_add(key.len()).saturating_add(value.len()).saturating_add(128); }
            for key in &ws.config_max_u64_keys { bytes = bytes.saturating_add(key.len()).saturating_add(64); }
            let credit = ReadCredit::try_new(owner,bytes,"sync","connected_trigger_effect_baseline",
                "Raise the declared memory limit to validate this callback's append-only effects.")?;
            // Only fields the exact append-only validator compares. In
            // particular no declared-index tree or posting owner is cloned.
            let mut before = WriteSet::default();
            before.relational_inserts = ws.relational_inserts.iter().map(|(table,row)|
                Ok((table.clone(),crate::memory_accounting::preparation_copy_row(row)?)))
                .collect::<Result<Vec<_>>>()?;
            before.relational_deletes = ws.relational_deletes.clone();
            before.relational_delete_predicates = ws.relational_delete_predicates.clone();
            before.adj_inserts = ws.adj_inserts.clone(); before.adj_deletes = ws.adj_deletes.clone();
            before.vector_inserts = ws.vector_inserts.clone(); before.vector_deletes = ws.vector_deletes.clone();
            before.vector_moves = ws.vector_moves.clone(); before.config_writes = ws.config_writes.clone();
            before.config_max_u64_keys = ws.config_max_u64_keys.clone();
            Ok(Some(PrivateTriggerBaseline { write_set: before, _credit: credit }))
        })?
    }
}

impl Database {
    pub(super) fn admit_private_trigger_snapshot(&self, tx: TxId,
        declarations: Option<&BTreeMap<String,TriggerDeclaration>>, credit: Option<&mut ReadCredit>) -> Result<()> {
        let Some(credit) = credit else { return Ok(()); };
        let declaration_bytes = |declarations: &BTreeMap<String,TriggerDeclaration>|
            declarations.values().fold(0usize,|bytes,declaration| bytes.saturating_add(declaration.name.len())
                .saturating_add(declaration.table.len()).saturating_add(declaration.on_events.len().saturating_mul(32))
                .saturating_add(512));
        let declarations = declarations.map_or_else(|| declaration_bytes(&self.trigger.declarations.lock()),declaration_bytes);
        let bytes = self.tx_mgr.with_write_set(tx,|ws| {
            ws.relational_inserts.iter().map(|(table,_)| table.len().saturating_add(256))
                .chain(ws.relational_deletes.iter().map(|(table,_,_)| table.len().saturating_add(256)))
                .fold(declarations,usize::saturating_add).saturating_mul(4).saturating_add(4096)
        })?;
        if bytes > credit.bytes() {
            credit.try_grow_for(bytes-credit.bytes(),"sync","connected_trigger_snapshot_controls",
                "Raise the declared memory limit to inspect this callback's pending rows.")?;
        }
        Ok(())
    }
}

impl Database {
    pub(super) fn admit_private_trigger_guard(&self, tx: TxId, name: &str) -> Result<()> {
        PRIVATE_UNIT.with(|slot| {
            let mut state = slot.borrow_mut();
            if let Some(state) = state.as_mut().filter(|state| state.host == self.tx_mgr.identity() && state.tx == tx) {
                state.effects._credit.try_grow_for(name.len().saturating_mul(4).saturating_add(1536),
                    "sync","connected_trigger_guard","Raise the declared memory limit to retain callback guards.")?;
            }
            Ok(())
        })
    }
}

impl Database {
    /// DROP destroys the old layout, so settle pending vectors while their
    /// admission-time quantization still exists. No body is refunded live.
    pub(super) fn drop_private_pending_vectors(&self, tx: TxId, table: &str,
        vector_owner: &mut contextdb_vector::VectorCopyAdmission) -> Result<()> {
        loop {
            let retired = self.tx_mgr.with_write_set(tx,|ws| -> Result<_> {
                if ws.ordered_staged_rows.checkpoint_active() {
                    return Err(Error::Other("private vector DROP retained a statement checkpoint".into()));
                }
                let Some(position) = ws.vector_inserts.iter().position(|entry| entry.index.table == table)
                    else { return Ok(None); };
                let entry = &ws.vector_inserts[position];
                let bytes = self.vector_insert_accounted_bytes(&entry.index,entry.vector.len(),vector_owner)?;
                ws.ordered_staged_rows.remove_vector_capability(&entry.index,entry.row_id);
                Ok(Some((ws.vector_inserts.remove(position),bytes)))
            })??;
            let Some((entry,bytes)) = retired else { break; };
            drop(entry);
            self.finalization.accountant.release(bytes);
        }
        self.tx_mgr.with_write_set(tx,|ws| {
            ws.vector_deletes.retain(|(index,_,_)| index.table != table);
            ws.vector_moves.retain(|(index,_,_,_)| index.table != table);
        })?;
        // A recreated column is a fresh schema lifetime. Its next insertion
        // must record that new epoch, rather than inherit the dropped one.
        if let Some(metadata) = self.pending_commit_metadata.lock().get_mut(&tx) {
            metadata.vector_schema_epochs.retain(|index,_| index.table != table);
            metadata.upsert_intents.retain(|intent| intent.table != table);
            metadata.conditional_update_guards.retain(|guard| guard.table != table);
            metadata.sync_lineage_guards.retain(|guard| guard.table != table);
        }
        Ok(())
    }
}

impl Database {
    pub(super) fn private_callback_start(&self, tx: TxId) -> Result<usize> {
        PRIVATE_UNIT.with(|slot| {
            let mut state = slot.borrow_mut();
            let state = state.as_mut().filter(|state| state.host == self.tx_mgr.identity() && state.tx == tx)
                .ok_or_else(|| Error::Other("private callback start lost its unit".into()))?;
            self.tx_mgr.with_write_set(tx,|ws| -> Result<usize> {
                let source = ws.config_writes.get(state.callback_config_cursor..)
                    .ok_or_else(|| Error::Other("private callback configuration prefix was truncated".into()))?;
                for (key,_) in source {
                    if key.starts_with("sync_creation_lineage.v1.") && !state.effects.source_creation_keys.contains(key) {
                        state.effects._credit.try_grow_for(key.len().saturating_add(512),"sync","connected_source_creation_key",
                            "Raise the declared memory limit to retain source creation provenance.")?;
                        state.effects.source_creation_keys.insert(key.clone());
                    }
                }
                state.callback_config_cursor = ws.config_writes.len();
                Ok(state.callback_config_cursor)
            })?
        })
    }
    pub(super) fn private_callback_end(&self, tx: TxId, initial_rows: usize, materialized_edge: bool, phase_lsn: Lsn) -> Result<()> {
        PRIVATE_UNIT.with(|slot| {
            let mut state = slot.borrow_mut();
            let state = state.as_mut().filter(|state| state.host == self.tx_mgr.identity() && state.tx == tx)
                .ok_or_else(|| Error::Other("private callback end lost its unit".into()))?;
            self.tx_mgr.with_write_set(tx,|ws| -> Result<()> {
                if materialized_edge {
                    for index in initial_rows..ws.relational_inserts.len() {
                        let (table,row_id) = {
                            let (table,row) = &ws.relational_inserts[index];
                            state.effects._credit.try_grow_for(table.len().saturating_mul(2).saturating_add(1024),
                                "sync","connected_materialized_row_provenance",
                                "Raise the declared memory limit to retain materialized callback provenance.")?;
                            (table.clone(),row.row_id)
                        };
                        ws.set_relational_insert_source_lsn(table,row_id,SYNC_SOURCE_LSN_OWN_COMMIT);
                    }
                } else {
                    // Hub callbacks author new rows. Preserve their actual body
                    // and declaration here, before a later source ALTER/DROP
                    // transforms/removes the pending current projection.
                    for (table,row) in &ws.relational_inserts[initial_rows..] {
                        let bytes = crate::memory_accounting::preparation_row_copy_bytes(row)?
                            .saturating_add(table.len().saturating_mul(4)).saturating_add(4096);
                        state.effects._credit.try_grow_for(bytes,"sync","connected_derived_authored_row",
                            "Raise the declared memory limit to retain callback authored history.")?;
                        let shape = if let Some(shape) = state.derived_shapes.get(table) { shape.clone() }
                        else {
                            let metadata = self.relational_store.table_meta.read();
                            let meta = metadata.get(table).ok_or_else(||
                                Error::Other("private callback authored a row without its declaration".into()))?;
                            let shape = AuthoredRowShape::prepare(meta,state.owner.clone())?;
                            state.derived_shapes.insert(table.clone(),shape.clone());
                            shape
                        };
                        let mut body = crate::memory_accounting::preparation_copy_row(row)?;
                        // Quantized staged rows deliberately carry NULL instead
                        // of a second float payload. Preserve the callback's
                        // actual vector under its current declaration BEFORE
                        // a later ALTER/DROP retires the staged vector owner.
                        for vector in ws.vector_inserts.iter().filter(|vector|
                            vector.index.table == *table && vector.row_id == row.row_id) {
                            let column = shape.meta.columns.iter().find(|column|
                                column.name == vector.index.column).ok_or_else(||
                                    Error::UnknownVectorIndex { index: vector.index.clone() })?;
                            if vector.vector.is_empty()
                                && matches!(body.values.get(&vector.index.column), Some(Value::Null)) {
                                continue;
                            }
                            state.effects._credit.try_grow_for(vector.vector.len().saturating_mul(8)
                                .saturating_add(vector.index.column.len()).saturating_add(512),
                                "sync", "connected_derived_authored_vector",
                                "Raise the declared memory limit to retain callback authored vectors.")?;
                            let value = Value::Vector(contextdb_vector::stored_vector_value(
                                &vector.vector, column.quantization));
                            if let Some(slot) = body.values.get_mut(&vector.index.column) { *slot = value; }
                            else { body.values.insert(vector.index.column.clone(), value); }
                        }
                        body.created_tx = state.canonical_tx;
                        body.deleted_tx = None;
                        body.lsn = phase_lsn;
                        state.effects.derived_history.push(ChangeLogEntry::RowInsert {
                            table: table.clone(), row_id: body.row_id, lsn: phase_lsn,
                        });
                        state.effects.derived_positions.entry(table.clone()).or_default().insert(body.row_id,phase_lsn);
                        state.effects.derived_rows.push((table.clone(),shape,body));
                    }
                }
                state.callback_config_cursor = ws.config_writes.len();
                Ok(())
            })?
        })
    }
    pub(super) fn invalidate_private_derived_shape(&self, tx: TxId, ddl: &DdlChange) {
        if let Some(table) = Self::ddl_affected_table(ddl) {
            PRIVATE_UNIT.with(|slot| {
                if let Some(state) = slot.borrow_mut().as_mut().filter(|state|
                    state.host == self.tx_mgr.identity() && state.tx == tx) {
                    state.derived_shapes.remove(table);
                }
            });
        }
    }
    pub(super) fn private_source_creation_key(&self, tx: TxId, key: &str) -> bool {
        PRIVATE_UNIT.with(|slot| slot.borrow().as_ref().is_some_and(|state|
            state.host == self.tx_mgr.identity() && state.tx == tx && state.effects.source_creation_keys.contains(key)))
    }
    pub(super) fn private_acceptance_end_for_write_set(&self, ws: &WriteSet) -> Option<Lsn> {
        if !ws.config_writes.iter().any(|(key,_)| key == "__contextdb.detached_capture_owner.v1") { return None; }
        PRIVATE_UNIT.with(|slot| slot.borrow().as_ref().filter(|state| state.host == self.tx_mgr.identity()).map(|state| state.end))
    }
}

/// Payload before its exact old-layout invoice on every failure and unwind.
struct PrivatePendingVector {
    entry: Option<VectorEntry>,
    invoice: StagedCopyCharge,
}
pub(super) struct PrivatePendingVectorAlter {
    pub(super) previous: TableMeta,
    table: String,
    renamed: Option<(String,String)>,
    entries: Vec<PrivatePendingVector>,
    deletes: Vec<(VectorIndexRef,RowId,TxId)>,
    moves: Vec<(VectorIndexRef,RowId,RowId,TxId,Option<Arc<contextdb_tx::ordered_staged::StagedVectorCapability>>)>,
    _credit: ReadCredit,
}
impl PrivatePendingVectorAlter {
    fn target(&self, index: &VectorIndexRef, meta: &TableMeta) -> Option<VectorIndexRef> {
        let column = self.renamed.as_ref().filter(|(from,_)| *from == index.column)
            .map(|(_,to)| to.as_str()).unwrap_or(index.column.as_str());
        let old = self.previous.columns.iter().find(|old| old.name == index.column)?;
        let new = meta.columns.iter().find(|new| new.name == column)?;
        (matches!(old.column_type,ColumnType::Vector(_)) && old.column_type == new.column_type
            && old.quantization == new.quantization).then(|| VectorIndexRef::new(&self.table,column))
    }
}
impl Database {
    pub(super) fn detach_private_pending_vectors_for_alter(&self, tx: TxId, ddl: &DdlChange,
        owner: &mut contextdb_vector::VectorCopyAdmission,
        memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>) -> Result<Option<PrivatePendingVectorAlter>> {
        let DdlChange::AlterTable { name,columns,constraints,foreign_keys,composite_foreign_keys,composite_unique } = ddl
            else { return Ok(None); };
        let mut credit = ReadCredit::new(memory);
        let previous = {
            let tables = self.relational_store.table_meta.read();
            let Some(meta) = tables.get(name) else { return Ok(None); };
            credit.try_grow_for(meta.estimated_bytes().saturating_mul(4)
                .saturating_add(ddl.estimated_bytes().saturating_mul(8)).saturating_add(4096),
                "sync","connected_pending_vector_shape","Raise the declared memory limit to transform pending vectors.")?;
            meta.clone()
        };
        let incoming = rough_sync_table_meta(columns,constraints,foreign_keys,composite_foreign_keys,composite_unique);
        let renamed = Self::sync_marked_vector_rename(&previous,&incoming,constraints);
        let mut plan = PrivatePendingVectorAlter { previous,table: name.clone(),renamed,
            entries: Vec::new(),deletes: Vec::new(),moves: Vec::new(),_credit: credit };
        self.tx_mgr.with_write_set(tx,|ws| -> Result<()> {
            if ws.ordered_staged_rows.checkpoint_active() {
                return Err(Error::Other("private vector ALTER retained a statement checkpoint".into()));
            }
            let entries = ws.vector_inserts.iter().filter(|entry| entry.index.table == *name).count();
            let deletes = ws.vector_deletes.iter().filter(|(index,_,_)| index.table == *name).count();
            let moves = ws.vector_moves.iter().filter(|(index,_,_,_)| index.table == *name).count();
            plan._credit.try_grow_for(entries.saturating_mul(std::mem::size_of::<PrivatePendingVector>())
                .saturating_add(deletes.saturating_mul(std::mem::size_of::<(VectorIndexRef,RowId,TxId)>()))
                .saturating_add(moves.saturating_mul(std::mem::size_of::<(VectorIndexRef,RowId,RowId,TxId,Option<Arc<contextdb_tx::ordered_staged::StagedVectorCapability>>)>()))
                .saturating_mul(2),"sync","connected_pending_vector_directory",
                "Raise the declared memory limit to transform pending vectors.")?;
            plan.entries.reserve_exact(entries); plan.deletes.reserve_exact(deletes); plan.moves.reserve_exact(moves);
            let mut position = 0;
            while position < ws.vector_inserts.len() {
                if ws.vector_inserts[position].index.table != *name { position += 1; continue; }
                let entry = &ws.vector_inserts[position];
                let bytes = self.vector_insert_accounted_bytes(&entry.index,entry.vector.len(),owner)?;
                ws.ordered_staged_rows.remove_vector_capability(&entry.index,entry.row_id);
                let entry = ws.vector_inserts.remove(position);
                plan.entries.push(PrivatePendingVector { entry: Some(entry), invoice: StagedCopyCharge::take_admitted(
                    &self.finalization.accountant,Arc::clone(&self.finalization.vector_accountant),bytes) });
            }
            let mut position = 0;
            while position < ws.vector_deletes.len() {
                if ws.vector_deletes[position].0.table == *name { plan.deletes.push(ws.vector_deletes.remove(position)); }
                else { position += 1; }
            }
            let mut position = 0;
            while position < ws.vector_moves.len() {
                if ws.vector_moves[position].0.table == *name {
                    let (index,from,to,_) = &ws.vector_moves[position];
                    let capability = ws.ordered_staged_rows.vector_capability(index,*to);
                    ws.ordered_staged_rows.remove_vector_capability(index,*from);
                    ws.ordered_staged_rows.remove_vector_capability(index,*to);
                    let (index,from,to,created) = ws.vector_moves.remove(position);
                    plan.moves.push((index,from,to,created,capability));
                } else { position += 1; }
            }
            Ok(())
        })??;
        if let Some(metadata) = self.pending_commit_metadata.lock().get_mut(&tx) {
            metadata.vector_schema_epochs.retain(|index,_| index.table != *name);
        }
        Ok(Some(plan))
    }
    pub(super) fn restore_private_pending_vectors_after_alter(&self, tx: TxId, mut plan: PrivatePendingVectorAlter) -> Result<()> {
        // The directory/old-shape credit now follows the output write set;
        // failure after restoring only some entries cannot refund live keys.
        self.retain_private_vector_alter_credit(tx,&mut plan._credit)?;
        let meta = self.relational_store.table_meta.read();
        let meta = meta.get(&plan.table).ok_or_else(|| Error::TableNotFound(plan.table.clone()))?;
        for mut held in std::mem::take(&mut plan.entries) {
            let old = held.entry.as_ref().expect("detached pending vector owns its body");
            self.admit_private_vector_alter_output(tx,plan.table.len().saturating_add(meta.estimated_bytes()).saturating_add(1024))?;
            let Some(index) = plan.target(&old.index,meta) else { drop(held); continue; };
            self.record_vector_schema_epoch(tx,&index)?;
            let entry = held.entry.as_mut().expect("detached pending vector owns its body");
            entry.index = index;
            self.tx_mgr.with_write_set(tx,|ws| -> Result<()> {
                let entry = held.entry.as_ref().expect("detached pending vector owns its body");
                let prepared = ws.ordered_staged_rows.prepare_vector_capability(&entry.index,entry.row_id,&entry.vector,|bytes| {
                    Ok(crate::memory_accounting::OwnedMemoryReservation::try_new_for(
                        crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),bytes,"vector","staged_immutable_owner",
                        "Reduce staged vectors or raise MEMORY_LIMIT.")?.into_admitted_arc()?)
                })?;
                let entry = held.entry.take().expect("detached pending vector owns its body");
                let index = entry.index.clone();
                ws.vector_inserts.push(entry);
                held.invoice.transfer();
                ws.ordered_staged_rows.publish_vector_capability(&index,prepared);
                Ok(())
            })??;
        }
        // These carriers have no raw-body invoice; keep their exact row/tx
        // fields and translate only surviving schema identities.
        for (index,row,deleted) in std::mem::take(&mut plan.deletes) {
            self.admit_private_vector_alter_output(tx,meta.estimated_bytes().saturating_add(1024))?;
            if let Some(index) = plan.target(&index,meta) {
                self.record_vector_schema_epoch(tx,&index)?;
                self.tx_mgr.with_write_set(tx,|ws| ws.vector_deletes.push((index,row,deleted)))?;
            }
        }
        for (index,from,to,created,capability) in std::mem::take(&mut plan.moves) {
            self.admit_private_vector_alter_output(tx,meta.estimated_bytes().saturating_add(1024))?;
            if let Some(index) = plan.target(&index,meta) {
                self.record_vector_schema_epoch(tx,&index)?;
                self.tx_mgr.with_write_set(tx,|ws| -> Result<()> {
                    let prepared = capability.as_ref().map(|capability|
                        ws.ordered_staged_rows.prepare_vector_capability(&index,to,capability.vector(),|bytes| {
                            Ok(crate::memory_accounting::OwnedMemoryReservation::try_new_for(
                                crate::memory_accounting::AccountantHandle::clone(&self.finalization.accountant),bytes,"vector","staged_immutable_owner",
                                "Reduce staged vectors or raise MEMORY_LIMIT.")?.into_admitted_arc()?)
                        })).transpose()?;
                    ws.vector_moves.push((index.clone(),from,to,created));
                    if let Some(prepared) = prepared { ws.ordered_staged_rows.publish_vector_capability(&index,prepared); }
                    Ok(())
                })??;
            }
        }
        Ok(())
    }
}

impl Database {
    fn retain_private_vector_alter_credit(&self, tx: TxId, credit: &mut ReadCredit) -> Result<()> {
        PRIVATE_UNIT.with(|slot| {
            let mut state = slot.borrow_mut();
            let state = state.as_mut().filter(|state| state.host == self.tx_mgr.identity() && state.tx == tx)
                .ok_or_else(|| Error::Other("private vector ALTER lost its unit owner".into()))?;
            state.effects._credit.try_grow_for(std::mem::size_of::<ReadCredit>().saturating_mul(2).saturating_add(256),
                "sync","connected_vector_alter_credit_control","Raise the declared memory limit to retain vector ALTER custody.")?;
            let original = std::mem::replace(credit,ReadCredit::new(state.owner.clone()));
            state.effects.vector_alter_credits.push(original);
            Ok(())
        })
    }
    fn admit_private_vector_alter_output(&self, tx: TxId, bytes: usize) -> Result<()> {
        PRIVATE_UNIT.with(|slot| {
            let mut state = slot.borrow_mut();
            let state = state.as_mut().filter(|state| state.host == self.tx_mgr.identity() && state.tx == tx)
                .ok_or_else(|| Error::Other("private vector ALTER output lost its unit owner".into()))?;
            state.effects._credit.try_grow_for(bytes,"sync","connected_vector_alter_output",
                "Raise the declared memory limit to retain vector ALTER output.")
        })
    }
}

impl Database {
    pub(super) fn retain_private_synthetic_firing(&self, tx: TxId, table: &str, row: RowId,
        trigger_name: &str, lsn: Lsn) -> Result<()> {
        PRIVATE_UNIT.with(|slot| {
            let mut state = slot.borrow_mut();
            let state = state.as_mut().filter(|state| state.host == self.tx_mgr.identity() && state.tx == tx)
                .ok_or_else(|| Error::Other("private trigger firing lost its unit owner".into()))?;
            state.effects._credit.try_grow_for(table.len().saturating_add(trigger_name.len())
                .saturating_add(std::mem::size_of::<PrivateSyntheticTriggerFiring>().saturating_mul(2)).saturating_add(512),
                "sync","connected_plain_trigger_firing","Raise the declared memory limit to retain source trigger firings.")?;
            state.effects.synthetic_firings.push(PrivateSyntheticTriggerFiring {
                trigger_name: trigger_name.to_owned(),table: table.to_owned(),row_id: row,lsn,tx: state.canonical_tx,
            });
            Ok(())
        })
    }
}

// One pending identity is copied under the authentic Original before graph
// canonicalization can replace it. This is an operation window, not history.
pub(super) struct PrivateGraphMutation {
    source: crate::persistence::received_working_source::WorkingReadSource,
    from: NodeId,
    to: NodeId,
    kind: String,
    before: Option<Vec<u8>>,
    before_bytes: usize,
    deleted: bool,
    origin: crate::persistence::received_working_graph::WorkingGraphOrigin,
    _owner: crate::memory_accounting::OwnedMemoryReservation,
}
impl Database {
    pub(super) fn set_private_graph_phase(&self, tx: TxId,
        origin: crate::persistence::received_working_graph::WorkingGraphOrigin) -> Result<()> {
        PRIVATE_UNIT.with(|slot| {
            let mut state = slot.borrow_mut();
            let Some(state) = state.as_mut().filter(|state| state.host == self.tx_mgr.identity() && state.tx == tx)
                else { return Ok::<(), Error>(()); };
            state.graph_origin = Some(origin);
            Ok::<(), Error>(())
        })?;
        if let Some(source) = crate::persistence::received_working_source::WorkingReadSource::current_for(self.tx_mgr.identity())? {
            source.begin_private_graph_capture()?;
            let mut declarations = source.current_private_graph_declarations()?;
            self.publish_private_graph_declarations(&source, &mut declarations)?;
        }
        Ok(())
    }
    pub(super) fn prepare_private_graph_mutation(&self, tx: TxId, from: NodeId,
        kind: &str, to: NodeId) -> Result<Option<PrivateGraphMutation>> {
        let origin = PRIVATE_UNIT.with(|slot| slot.borrow().as_ref().filter(|state|
            state.host == self.tx_mgr.identity() && state.tx == tx).and_then(|state| state.graph_origin));
        let Some(origin) = origin else { return Ok(None); };
        let Some(source) = crate::persistence::received_working_source::WorkingReadSource::current_for(self.tx_mgr.identity())?
            else { return Ok(None); };
        source.with_original(|original| {
            let frame = crate::persistence::OriginalPersistenceFrame::new(original)?;
            let kind = frame.copy_text(kind)?;
            let (before, before_bytes, deleted) = self.tx_mgr.with_write_set(tx, |ws| -> Result<_> {
                let entry = ws.adj_inserts.iter().rev().find(|edge|
                    edge.source == from && edge.target == to && edge.edge_type == kind);
                let before_bytes = entry.map_or(0, AdjEntry::estimated_bytes);
                let before = entry.map(|edge| frame.encode(edge)).transpose()?;
                let deleted = ws.adj_deletes.iter().any(|(s, k, t, _)| *s == from && *t == to && k == &kind);
                Ok((before, before_bytes, deleted))
            })??;
            Ok(Some(PrivateGraphMutation { source: source.clone(), from, to, kind, before,
                before_bytes, deleted, origin, _owner: frame.into_reservation() }))
        })
    }
    pub(super) fn finish_private_graph_mutation(&self, tx: TxId,
        mutation: Option<PrivateGraphMutation>) -> Result<()> {
        let Some(mutation) = mutation else { return Ok(()); };
        mutation.source.with_original(|original| {
            let frame = crate::persistence::OriginalPersistenceFrame::new(original)?;
            let (after, deleted) = self.tx_mgr.with_write_set(tx, |ws| -> Result<_> {
                let after = ws.adj_inserts.iter().rev().find(|edge| edge.source == mutation.from
                    && edge.target == mutation.to && edge.edge_type == mutation.kind)
                    .map(|edge| frame.encode(edge)).transpose()?;
                let deleted = ws.adj_deletes.iter().any(|(s, k, t, _)| *s == mutation.from
                    && *t == mutation.to && k == &mutation.kind);
                Ok((after, deleted))
            })??;
            mutation.source.select_private_graph_type(&mutation.kind)?;
            if after == mutation.before && deleted == mutation.deleted { return Ok(()); }
            let after = after.as_ref().map(|body| frame.decode::<AdjEntry>(body)).transpose()?;
            mutation.source.retain_private_graph_mutation(mutation.origin, mutation.from, &mutation.kind,
                mutation.to, after.as_ref(), tx)
        })
    }
}

/// Pending graph originals survive a refused statement even when the graph
/// executor replaced an entry in place or removed it before returning.
pub(super) struct PrivateGraphStatementCheckpoint {
    source: crate::persistence::received_working_source::WorkingReadSource,
    inserts: Vec<AdjEntry>,
    deletes: Vec<(NodeId, String, NodeId, TxId)>,
    marker: crate::persistence::received_working_graph::WorkingGraphCheckpoint,
    _owner: Option<crate::memory_accounting::OwnedMemoryReservation>,
    active: bool,
}
impl Drop for PrivateGraphStatementCheckpoint {
    fn drop(&mut self) {
        if self.active { let _ = self.source.settle_private_graph_checkpoint(self.marker, false); }
    }
}
impl Database {
    pub(super) fn checkpoint_private_graph_statement(&self, tx: TxId) -> Result<Option<PrivateGraphStatementCheckpoint>> {
        let Some(source) = crate::persistence::received_working_source::WorkingReadSource::current_for(self.tx_mgr.identity())?
            else { return Ok(None); };
        if !source.private_graph_capturing()? { return Ok(None); }
        source.with_original(|original| {
            let frame = crate::persistence::OriginalPersistenceFrame::new(original)?;
            let (inserts, deletes) = self.tx_mgr.with_write_set(tx, |ws| -> Result<_> {
                let inserts = frame.encode(&ws.adj_inserts)?;
                let inserts = frame.decode::<Vec<AdjEntry>>(&inserts)?;
                let deletes = frame.encode(&ws.adj_deletes)?;
                let deletes = frame.decode::<Vec<(NodeId, String, NodeId, TxId)>>(&deletes)?;
                Ok((inserts, deletes))
            })??;
            let marker = source.checkpoint_private_graph()?;
            Ok(Some(PrivateGraphStatementCheckpoint { source: source.clone(), inserts, deletes,
                marker, _owner: Some(frame.into_reservation()), active: true }))
        })
    }
    pub(super) fn restore_private_graph_statement(&self, tx: TxId,
        checkpoint: &mut PrivateGraphStatementCheckpoint) -> Result<()> {
        let source = checkpoint.source.clone();
        source.with_original(|_original| {
            let (current_bytes, old_bytes) = self.tx_mgr.with_write_set(tx, |ws| (
                ws.adj_inserts.iter().map(AdjEntry::estimated_bytes).sum::<usize>(),
                checkpoint.inserts.iter().map(AdjEntry::estimated_bytes).sum::<usize>()))?;
            let added = old_bytes.saturating_sub(current_bytes);
            if added != 0 {
                self.finalization.accountant.try_allocate_for(added,
                    "graph", "restore_private_graph_statement", "Raise MEMORY_LIMIT to restore this statement.")?;
            }
            let restored = (|| -> Result<()> {
                source.settle_private_graph_checkpoint(checkpoint.marker, true)?;
                let retired_owner = self.replace_private_graph_statement_original(tx, &mut checkpoint._owner)?;
                self.tx_mgr.with_write_set(tx, |ws| {
                    std::mem::swap(&mut ws.adj_inserts, &mut checkpoint.inserts);
                    std::mem::swap(&mut ws.adj_deletes, &mut checkpoint.deletes);
                })?;
                checkpoint.inserts.clear(); checkpoint.deletes.clear();
                drop(retired_owner);
                if current_bytes > old_bytes { self.finalization.accountant.release(current_bytes - old_bytes); }
                checkpoint.active = false;
                Ok(())
            })();
            if restored.is_err() && added != 0 { self.finalization.accountant.release(added); }
            restored
        })
    }
}

impl Database {
    pub(super) fn private_graph_capture_active(&self, tx: TxId) -> Result<bool> {
        if self.private_replay_owner(tx).is_none() { return Ok(false); }
        match crate::persistence::received_working_source::WorkingReadSource::current_for(self.tx_mgr.identity())? {
            Some(source) => source.private_graph_capturing(), None => Ok(false),
        }
    }
    fn replace_private_graph_statement_original(&self, tx: TxId,
        owner: &mut Option<crate::memory_accounting::OwnedMemoryReservation>)
        -> Result<Option<crate::memory_accounting::OwnedMemoryReservation>> {
        PRIVATE_UNIT.with(|slot| {
            let mut state = slot.borrow_mut();
            let state = state.as_mut().filter(|state| state.host == self.tx_mgr.identity() && state.tx == tx)
                .ok_or_else(|| Error::Other("private graph original lost its unit".into()))?;
            if owner.is_none() { return Err(Error::Other("graph statement lost its original invoice".into())); }
            Ok(state.effects.graph_statement_owner.replace(owner.take().expect("validated statement original")))
        })
    }
}

pub(super) struct PrivateGraphSchemaTransition {
    table: String,
    previous: Option<TableMeta>,
    _owner: crate::memory_accounting::OwnedMemoryReservation,
}
impl Database {
    pub(super) fn prepare_private_graph_schema_transition(&self, tx: TxId, ddl: &DdlChange)
        -> Result<Option<PrivateGraphSchemaTransition>> {
        if !self.private_graph_capture_active(tx)? { return Ok(None); }
        let Some(table) = Self::ddl_affected_table(ddl) else { return Ok(None); };
        let source = crate::persistence::received_working_source::WorkingReadSource::current_for(self.tx_mgr.identity())?
            .ok_or_else(|| Error::Other("private graph transition lost its current source".into()))?;
        source.with_original(|original| {
            let frame = crate::persistence::OriginalPersistenceFrame::new(original)?;
            let bytes = self.relational_store.table_meta.read().get(table)
                .map_or(0, |meta| meta.estimated_bytes().saturating_mul(16).saturating_add(4096));
            frame.reserve(bytes)?;
            let table = frame.copy_text(table)?;
            // The detached schema gate excludes mutations between sizing and
            // copying; no schema lock crosses an Original admission/callback.
            let previous = self.relational_store.table_meta.read().get(&table).cloned();
            Ok(Some(PrivateGraphSchemaTransition { table, previous, _owner: frame.into_reservation() }))
        })
    }
    pub(super) fn finish_private_graph_schema_transition(&self, tx: TxId,
        transition: Option<PrivateGraphSchemaTransition>) -> Result<()> {
        let Some(transition) = transition else { return Ok(()); };
        let source = crate::persistence::received_working_source::WorkingReadSource::current_for(self.tx_mgr.identity())?
            .ok_or_else(|| Error::Other("private graph transition lost its source".into()))?;
        source.with_original(|original| {
            let frame = crate::persistence::OriginalPersistenceFrame::new(original)?;
            let bytes = self.relational_store.table_meta.read().get(&transition.table)
                .map_or(0, |meta| meta.estimated_bytes().saturating_mul(16).saturating_add(4096));
            frame.reserve(bytes)?;
            let current = self.relational_store.table_meta.read().get(&transition.table).cloned();
            let previous = transition.previous.as_ref();
            for kind in previous.into_iter().flat_map(|meta| meta.dag_edge_types.iter())
                .chain(current.iter().flat_map(|meta| meta.dag_edge_types.iter())) {
                if previous.is_some_and(|meta| meta.dag_edge_types.contains(kind))
                    != current.as_ref().is_some_and(|meta| meta.dag_edge_types.contains(kind)) {
                    source.select_private_graph_type(kind)?;
                }
            }
            // Identity-preserving ALTER carries native and pending links. DROP
            // already consumed both old row populations before schema removal.
            // Identity/shape-changing ALTER is deliberately still behind the
            // unchanged source-prefix guard pending its owner product decision.
            let mut declarations = source.current_private_graph_declarations()?;
            self.publish_private_graph_declarations(&source, &mut declarations)
        })
    }
}

pub(super) struct PrivatePendingGraphProperties {
    pub(super) values: HashMap<String, Value>,
    pub(super) changed: bool,
    _owner: crate::memory_accounting::OwnedMemoryReservation,
}
impl PrivateGraphMutation {
    pub(super) fn pending_bytes(&self) -> usize { self.before_bytes }
    pub(super) fn merged_pending_properties(&self, incoming: &HashMap<String, Value>)
        -> Result<Option<PrivatePendingGraphProperties>> {
        let Some(before) = &self.before else { return Ok(None); };
        self.source.with_original(|original| {
            let frame = crate::persistence::OriginalPersistenceFrame::new(original)?;
            let edge: AdjEntry = frame.decode(before)?;
            let changed = incoming.iter().any(|(name, value)| edge.properties.get(name) != Some(value));
            // Admit the merge control before growing the copied pending map.
            frame.reserve(edge.estimated_bytes().saturating_mul(8)
                .saturating_add(incoming.len().saturating_mul(256)).saturating_add(4096))?;
            let encoded = frame.encode(incoming)?;
            let copied: HashMap<String, Value> = frame.decode(&encoded)?;
            let mut values = edge.properties;
            values.extend(copied);
            Ok(Some(PrivatePendingGraphProperties { values, changed, _owner: frame.into_reservation() }))
        })
    }
}

impl Database {
    fn publish_private_graph_declarations(&self,
        source: &crate::persistence::received_working_source::WorkingReadSource,
        declarations: &mut crate::persistence::received_working_source::OriginalGraphDeclarations) -> Result<()> {
        let prepared = MemGraphExecutor::<DynStore>::prepare_dag_edge_types(std::mem::take(&mut declarations.types));
        let retired_owner = source.replace_private_graph_declarations_original(&mut declarations.owner)?;
        self.graph.publish_prepared_dag_edge_types(prepared);
        drop(retired_owner);
        Ok(())
    }
}

pub(super) struct PrivateGraphTargets {
    pub(super) targets: Vec<NodeId>,
    _owner: crate::memory_accounting::OwnedMemoryReservation,
}
impl Database {
    pub(super) fn private_graph_read_tx(&self, snapshot: SnapshotId) -> Result<Option<TxId>> {
        let tx = PRIVATE_UNIT.with(|slot| slot.borrow().as_ref().filter(|state|
            state.host == self.tx_mgr.identity()).map(|state| state.tx));
        match tx {
            Some(tx) if snapshot == self.snapshot_for_read() && self.private_graph_capture_active(tx)? => Ok(Some(tx)),
            _ => Ok(None),
        }
    }
    pub(super) fn private_graph_read_hides(&self, tx: TxId, edge: &AdjEntry) -> Result<bool> {
        self.tx_mgr.with_write_set(tx, |ws| {
            ws.adj_deletes.iter().any(|(source, kind, target, _)| *source == edge.source
                && *target == edge.target && kind == &edge.edge_type)
                || ws.adj_inserts.iter().any(|pending| pending.source == edge.source
                    && pending.target == edge.target && pending.edge_type == edge.edge_type)
        })
    }
    pub(super) fn private_graph_targets(&self, tx: TxId, source: NodeId, kind: &str) -> Result<PrivateGraphTargets> {
        let working = crate::persistence::received_working_source::WorkingReadSource::current_for(self.tx_mgr.identity())?
            .ok_or_else(|| Error::Other("private graph count lost its source".into()))?;
        working.with_original(|original| {
            let frame = crate::persistence::OriginalPersistenceFrame::new(original)?;
            let count = self.tx_mgr.with_write_set(tx, |ws| ws.adj_inserts.iter()
                .filter(|edge| edge.source == source && edge.edge_type == kind).count())?;
            frame.array::<NodeId>(count)?;
            let targets = self.tx_mgr.with_write_set(tx, |ws| ws.adj_inserts.iter()
                .filter(|edge| edge.source == source && edge.edge_type == kind).map(|edge| edge.target).collect())?;
            Ok(PrivateGraphTargets { targets, _owner: frame.into_reservation() })
        })
    }
    pub(super) fn private_graph_properties(&self, tx: TxId, source: NodeId, kind: &str, target: NodeId,
        reader: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>) -> Result<Option<Option<HashMap<String, Value>>>> {
        let Some(capture) = self.prepare_private_graph_mutation(tx, source, kind, target)? else { return Ok(None); };
        if let Some(before) = &capture.before {
            capture.source.with_original(|original| {
                let frame = crate::persistence::OriginalPersistenceFrame::new(original)?;
                let edge: AdjEntry = frame.decode(before)?;
                let mut output = ReadCredit::new(reader);
                output.try_grow_for(edge.estimated_bytes().saturating_mul(4), "reader", "private_graph_properties",
                    "Raise the declared reader memory limit to return edge properties.")?;
                self.retain_writer_output(output);
                Ok(Some(Some(edge.properties)))
            })
        } else if capture.deleted { Ok(Some(None)) } else { Ok(None) }
    }
}
