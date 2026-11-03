//! Immutable authored bodies retired by schema projection. The pool is keyed
//! by exact local occurrence, so replacing a projection cannot replace history.
use super::*;

pub(super) const BODIES: TableDefinition<&str, &[u8]> =
    TableDefinition::new("__authored_row_bodies");

/// Archived lives of a table that follows its parent's purge, keyed from the
/// parent: `(parent table, parent key values, child table, child generation,
/// row key)`. A purge of the parent finds them from the parent alone, never
/// from the child's current declaration, which may since have changed or gone.
pub(crate) const FOLLOWER_POSTINGS: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("__authored_follower_postings");
/// The same postings keyed `(child table, row key, forward key)`, so erasing
/// an archived life removes its postings without reading any declaration.
pub(crate) const FOLLOWER_POSTINGS_BY_ROW: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("__authored_follower_postings_by_row");

fn segment(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&u32::try_from(bytes.len()).unwrap_or(u32::MAX).to_be_bytes());
    out.extend_from_slice(bytes);
}

/// The prefix every archived follower of one parent row shares.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(crate) fn follower_parent_prefix(
    parent_table: &str,
    parent_values: &[Value],
) -> Result<Vec<u8>> {
    let encoded = RedbPersistence::encode_config_value(&parent_values)?;
    let mut prefix = Vec::with_capacity(parent_table.len() + encoded.len() + 8);
    segment(&mut prefix, parent_table.as_bytes());
    segment(&mut prefix, &encoded);
    Ok(prefix)
}

fn follower_row_prefix(child_table: &str, row_key: &[u8]) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(child_table.len() + row_key.len() + 4);
    segment(&mut prefix, child_table.as_bytes());
    prefix.extend_from_slice(row_key);
    prefix
}

/// What one posting under a parent prefix names: the archived child table,
/// that table's generation when it was archived, and the row's key.
pub(crate) fn decode_follower_posting(
    key: &[u8],
    prefix_len: usize,
) -> Option<(String, u64, [u8; 24])> {
    let rest = key.get(prefix_len..)?;
    let length = usize::try_from(u32::from_be_bytes(rest.get(..4)?.try_into().ok()?)).ok()?;
    let table = std::str::from_utf8(rest.get(4..4 + length)?)
        .ok()?
        .to_string();
    let tail = rest.get(4 + length..)?;
    if tail.len() != 8 + 24 {
        return None;
    }
    let generation = u64::from_be_bytes(tail[..8].try_into().ok()?);
    Some((table, generation, tail[8..].try_into().ok()?))
}

pub(super) fn body_key(table: &str, key: &[u8]) -> String {
    use std::fmt::Write;
    let mut result = String::with_capacity(table.len() * 2 + 1 + 48);
    for byte in table.as_bytes() {
        write!(&mut result, "{byte:02x}").expect("string write");
    }
    result.push(':');
    for byte in key {
        write!(&mut result, "{byte:02x}").expect("string write");
    }
    result
}

impl RedbPersistence {
    /// A policy declaration does not replace an authored body. Inspect only
    /// schema metadata, including intermediate declarations in source order,
    /// before deciding whether the outgoing population needs archival.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn retain_received_authored_rows_in_write(
        &self,
        write: &redb::WriteTransaction,
        table: &str,
        stage: &ReceivedSchemaPersistenceProjection,
    ) -> Result<()> {
        let Some((mut previous, _credit)) = self.schema_projection_metadata_point(table)? else {
            return Ok(());
        };
        let same_body_shape = |before: &TableMeta, after: &TableMeta| {
            before
                .columns
                .iter()
                .map(|column| {
                    (
                        &column.name,
                        &column.column_type,
                        column.quantization,
                        &column.partition_key_columns,
                    )
                })
                .eq(after.columns.iter().map(|column| {
                    (
                        &column.name,
                        &column.column_type,
                        column.quantization,
                        &column.partition_key_columns,
                    )
                }))
        };
        for (_, ddl) in &stage.ddl_log {
            match ddl {
                DdlChange::DropTable { name } if name == table => {
                    return self.retain_authored_rows_in_write(write, table);
                }
                DdlChange::AlterTable {
                    name,
                    columns,
                    constraints,
                    foreign_keys,
                    composite_foreign_keys,
                    composite_unique,
                } if name == table => {
                    let incoming = crate::database::rough_sync_table_meta(
                        columns,
                        constraints,
                        foreign_keys,
                        composite_foreign_keys,
                        composite_unique,
                    );
                    let mut next = previous.clone();
                    crate::database::Database::apply_sync_alter_to_projected_table_meta(
                        table,
                        &mut next,
                        incoming,
                        constraints,
                    );
                    if !same_body_shape(&previous, &next) {
                        return self.retain_authored_rows_in_write(write, table);
                    }
                    previous = next;
                }
                _ => {}
            }
        }
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn retain_authored_rows_in_write(
        &self,
        write: &redb::WriteTransaction,
        logical: &str,
    ) -> Result<()> {
        let name = Self::rel_table_name(logical);
        let definition = TableDefinition::<&[u8], &[u8]>::new(&name);
        // The schema write already owns this addressed relation; opening an
        // empty newly declared relation is part of that same unpublished write.
        let source = write.open_table(definition).map_err(Self::storage_error)?;
        let mut destination = write.open_table(BODIES).map_err(Self::storage_error)?;
        let mut metadata_credit = self.proof_admission();
        // Some replacement doors have already written the arriving declaration
        // into this unpublished transaction. The committed source declaration
        // still owns the outgoing physical rows and their vector placements.
        let meta = self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            let metadata = read.open_table(META_TABLE).map_err(Self::storage_error)?;
            metadata
                .get(Self::meta_key(logical).as_str())
                .map_err(Self::storage_error)?
                .map(|value| {
                    metadata_credit
                        .admit(value.value().len().saturating_mul(64).saturating_add(1024))?;
                    Self::decode_persisted_schema(value.value()).map(|(meta, _, _)| meta)
                })
                .transpose()
        })?;
        // A table that follows its parent's purge leaves, for each archived
        // row, a posting under the parent it referenced, keyed by the parent.
        if let Some(meta) = meta.as_ref() {
            metadata_credit.admit(crate::database::purge_followers::declared_purge_follower_bytes(
                logical, meta,
            ))?;
        }
        let followers = meta
            .as_ref()
            .map(|meta| crate::database::purge_followers::declared_purge_followers(logical, meta))
            .unwrap_or_default();
        let archived_generation = if followers.is_empty() {
            0
        } else {
            let key = crate::database::Database::durable_lineage_table_generation_key(logical);
            write
                .open_table(CONFIG_TABLE)
                .map_err(Self::storage_error)?
                .get(key.as_str())
                .map_err(Self::storage_error)?
                .map(|value| Self::decode_config_value::<u64>(value.value()))
                .transpose()?
                .unwrap_or(0)
        };
        let mut postings = if followers.is_empty() {
            None
        } else {
            Some((
                write
                    .open_table(FOLLOWER_POSTINGS)
                    .map_err(Self::storage_error)?,
                write
                    .open_table(FOLLOWER_POSTINGS_BY_ROW)
                    .map_err(Self::storage_error)?,
            ))
        };
        // Stored rows are self-describing. Borrow one encoded value at a time;
        // neither decoded history nor a table-sized key list is materialized.
        for entry in source.iter().map_err(Self::storage_error)? {
            let (key, value) = entry.map_err(Self::storage_error)?;
            let raw: [u8; 24] = key.value().try_into().map_err(|_| {
                crate::funded_error::corruption_text(
                    self.require_decode_original()
                        .and_then(|original| original.read_budget()),
                    logical,
                    "invalid authored row key".into(),
                )
            })?;
            let mut admission = self.proof_admission();
            admission.admit(logical.len().saturating_mul(2).saturating_add(128))?;
            let addressed = body_key(logical, &raw);
            // A row projected by an earlier schema occurrence keeps its first
            // authored image, never the latest projection of that image.
            let present = destination
                .get(addressed.as_str())
                .map_err(Self::storage_error)?
                .is_some();
            // A received row's body was archived when it arrived, so `present`
            // says nothing about its postings: every outgoing life gets them.
            if let Some((forward, by_row)) = postings.as_mut() {
                admission.admit(value.value().len().saturating_mul(8).saturating_add(256))?;
                let row = Self::decode_versioned_row(value.value(), None)?;
                // The archived life's own key and its encoding.
                admission.admit(
                    value
                        .value()
                        .len()
                        .saturating_mul(2)
                        .saturating_add(logical.len())
                        .saturating_add(128),
                )?;
                // The archived life's own key, recorded now: after a drop or a
                // redeclaration the current schema cannot be asked for it.
                let Some(child_key) = meta.as_ref().and_then(|meta| {
                    crate::sync_types::natural_key_from_row_values(meta, &row.values)
                }) else {
                    continue;
                };
                let child_key = Self::encode_config_value(&child_key)?;
                for follower in &followers {
                    // Count what this posting pair will own before any of it
                    // exists. A NULL reference names no parent and follows none.
                    let mut referenced = Some(0_usize);
                    for column in &follower.child_columns {
                        referenced = match (referenced, row.values.get(column)) {
                            (Some(bytes), Some(value)) if *value != Value::Null => {
                                Some(bytes.saturating_add(value.estimated_bytes()))
                            }
                            _ => None,
                        };
                    }
                    let Some(referenced) = referenced else {
                        continue;
                    };
                    let key_bytes = follower
                        .parent_table
                        .len()
                        .saturating_add(logical.len())
                        .saturating_add(raw.len())
                        .saturating_add(referenced.saturating_mul(4))
                        .saturating_add(96);
                    admission.admit(
                        key_bytes
                            .saturating_mul(3)
                            .saturating_add(
                                follower
                                    .child_columns
                                    .len()
                                    .saturating_mul(std::mem::size_of::<Value>()),
                            )
                            .saturating_add(referenced)
                            .saturating_add(64),
                    )?;
                    let Some(values) = follower
                        .child_columns
                        .iter()
                        .map(|column| row.values.get(column).cloned())
                        .collect::<Option<Vec<_>>>()
                    else {
                        continue;
                    };
                    let mut key = follower_parent_prefix(&follower.parent_table, &values)?;
                    segment(&mut key, logical.as_bytes());
                    key.extend_from_slice(&archived_generation.to_be_bytes());
                    key.extend_from_slice(&raw);
                    let mut reverse = follower_row_prefix(logical, &raw);
                    reverse.extend_from_slice(&key);
                    forward
                        .insert(key.as_slice(), child_key.as_slice())
                        .map_err(Self::storage_error)?;
                    by_row
                        .insert(reverse.as_slice(), &[][..])
                        .map_err(Self::storage_error)?;
                }
            }
            if !present {
                // Quantized relational columns are placeholders. Retire their
                // exact canonical placement now, while that column lifetime
                // still exists, instead of consulting a recreated live index
                // when this occurrence is served later.
                if let Some(meta) = meta.as_ref().filter(|meta| {
                    meta.columns.iter().any(|column| {
                        matches!(column.column_type, ColumnType::Vector(_))
                            && column.quantization != VectorQuantization::F32
                    })
                }) {
                    let vector_bytes = meta
                        .columns
                        .iter()
                        .map(|column| match column.column_type {
                            ColumnType::Vector(dimension) => dimension.saturating_mul(4),
                            _ => 0,
                        })
                        .sum::<usize>();
                    admission.admit(
                        value
                            .value()
                            .len()
                            .saturating_mul(8)
                            .saturating_add(vector_bytes.saturating_mul(8))
                            .saturating_add(meta.estimated_bytes().saturating_mul(4)),
                    )?;
                    let mut row = Self::decode_versioned_row(value.value(), None)?;
                    self.fill_authored_quantized_columns_in_write(write, logical, meta, &mut row)?;
                    // This archive owns the already stored approximation. The
                    // f32 encoding preserves those values without requantizing.
                    let encoded = Self::encode_versioned_row(&row, None)?;
                    destination
                        .insert(addressed.as_str(), encoded.as_slice())
                        .map_err(Self::storage_error)?;
                } else {
                    destination
                        .insert(addressed.as_str(), value.value())
                        .map_err(Self::storage_error)?;
                }
            }
        }
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn fill_authored_quantized_columns_in_write(
        &self,
        _write: &redb::WriteTransaction,
        table: &str,
        meta: &TableMeta,
        row: &mut VersionedRow,
    ) -> Result<()> {
        for column in &meta.columns {
            let ColumnType::Vector(dimension) = column.column_type else {
                continue;
            };
            if column.quantization == VectorQuantization::F32
                || !matches!(row.values.get(&column.name), Some(Value::Null) | None)
            {
                continue;
            }
            let index = VectorIndexRef::new(table, &column.name);
            let partition = vector_partition_key_for_row(&index, meta, column, row)?;
            let mut prefix =
                Self::vector_partition_membership_row_prefix(&index, &partition, row.row_id);
            prefix.extend_from_slice(&row.created_tx.0.to_be_bytes());
            prefix.extend_from_slice(&row.lsn.0.to_be_bytes());
            let membership = self.with_db(|db| {
                // DROP may already have removed the unpublished placements.
                // Read both placement and body from the still-committed source
                // generation, which the enclosing writer excludes replacing.
                let read = db.begin_read().map_err(Self::storage_error)?;
                let members = read
                    .open_table(VECTOR_PARTITION_MEMBERSHIP_TABLE)
                    .map_err(Self::storage_error)?;
                let mut entries = members
                    .range(prefix.as_slice()..)
                    .map_err(Self::storage_error)?;
                let first = entries.next().transpose().map_err(Self::storage_error)?;
                let membership = first
                    .filter(|(key, _)| key.value().starts_with(&prefix))
                    .map(|(key, bytes)| {
                        let record: VectorPartitionMembershipRecord = self
                            .decode_vector_partition_record(
                                VectorPartitionRecordKind::Membership,
                                bytes.value(),
                            )?;
                        if Self::vector_partition_membership_key(&record) != key.value()
                            || record.index != index
                            || record.partition_key != partition
                            || record.row_id != row.row_id
                            || record.row_created_tx != row.created_tx
                            || record.row_lsn != row.lsn
                        {
                            return Err(crate::funded_error::corruption_text(
                                self.require_decode_original()
                                    .and_then(|original| original.read_budget()),
                                table,
                                "authored vector placement names another occurrence".into(),
                            ));
                        }
                        Ok(record)
                    })
                    .transpose()?;
                if entries
                    .next()
                    .transpose()
                    .map_err(Self::storage_error)?
                    .is_some_and(|(key, _)| key.value().starts_with(&prefix))
                {
                    return Err(crate::funded_error::corruption_text(
                        self.require_decode_original()
                            .and_then(|original| original.read_budget()),
                        table,
                        "authored vector occurrence has duplicate placements".into(),
                    ));
                }
                Ok(membership)
            })?;
            let Some(membership) = membership else {
                continue;
            };
            let mut bodies = self.load_vector_entries_for_memberships(
                &[membership],
                dimension,
                Some(column.quantization),
            )?;
            let body = bodies.pop().ok_or_else(|| {
                crate::funded_error::corruption_text(
                    self.require_decode_original()
                        .and_then(|original| original.read_budget()),
                    table,
                    "authored vector placement lost its canonical body".into(),
                )
            })?;
            row.values
                .insert(column.name.clone(), Value::Vector(body.vector));
        }
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn erase_authored_row_in_write(
        &self,
        write: &redb::WriteTransaction,
        logical: &str,
        key: &[u8],
    ) -> Result<bool> {
        if write
            .normal_table_root(BODIES.name())
            .map_err(Self::storage_error)?
            .is_none()
            && Self::witnessed_row_count_in_write(write, BODIES.name())?.is_none()
        {
            return Ok(false);
        }
        let mut admission = self.proof_admission();
        admission.admit(logical.len().saturating_mul(2).saturating_add(128))?;
        let row_key = key;
        let key = body_key(logical, key);
        let removed = write
            .open_table(BODIES)
            .map_err(Self::storage_error)?
            .remove(key.as_str())
            .map_err(Self::storage_error)?
            .is_some();
        if removed {
            self.erase_follower_postings_in_write(write, logical, row_key)?;
        }
        Ok(removed)
    }
}

impl RedbPersistence {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn retain_pending_authored_rows_in_write_original(
        &self,
        original: &OriginalPersistenceFrame<'_>,
        write: &redb::WriteTransaction,
        rows: &[(String, Arc<crate::database::AuthoredRowShape>, VersionedRow)],
    ) -> Result<()> {
        let mut destination = write.open_table(BODIES).map_err(Self::storage_error)?;
        for (table, _, row) in rows {
            let window = OriginalPersistenceFrame::new(original.original)?;
            window.reserve(table.len().saturating_mul(2).saturating_add(128))?;
            let key = body_key(table, &Self::rel_row_key(row));
            if destination
                .get(key.as_str())
                .map_err(Self::storage_error)?
                .is_some()
            {
                continue;
            }
            let encoded = Self::encode_versioned_row_original(&window, row, None)?;
            destination
                .insert(key.as_str(), encoded.as_slice())
                .map_err(Self::storage_error)?;
        }
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(super) fn retain_pending_authored_rows_in_write(
        &self,
        write: &redb::WriteTransaction,
        rows: &[(String, Arc<crate::database::AuthoredRowShape>, VersionedRow)],
    ) -> Result<()> {
        let mut destination = write.open_table(BODIES).map_err(Self::storage_error)?;
        for (table, _, row) in rows {
            let mut scratch = self.proof_admission();
            scratch.admit(
                row.estimated_bytes()
                    .saturating_mul(8)
                    .saturating_add(table.len() * 2 + 256),
            )?;
            let key = body_key(table, &Self::rel_row_key(row));
            if destination
                .get(key.as_str())
                .map_err(Self::storage_error)?
                .is_some()
            {
                continue;
            }
            let encoded = Self::encode_versioned_row(row, None)?;
            destination
                .insert(key.as_str(), encoded.as_slice())
                .map_err(Self::storage_error)?;
        }
        Ok(())
    }
}

impl RedbPersistence {
    /// Remove every follower posting an erased archived row left, found from
    /// the row itself. A row of a table that never followed has none.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn erase_follower_postings_in_write(
        &self,
        write: &redb::WriteTransaction,
        logical: &str,
        row_key: &[u8],
    ) -> Result<()> {
        if write
            .normal_table_root(FOLLOWER_POSTINGS_BY_ROW.name())
            .map_err(Self::storage_error)?
            .is_none()
        {
            return Ok(());
        }
        let prefix = follower_row_prefix(logical, row_key);
        let mut by_row = write
            .open_table(FOLLOWER_POSTINGS_BY_ROW)
            .map_err(Self::storage_error)?;
        let mut forward = write
            .open_table(FOLLOWER_POSTINGS)
            .map_err(Self::storage_error)?;
        loop {
            let mut admission = self.proof_admission();
            let next = {
                let mut range = by_row
                    .range(prefix.as_slice()..)
                    .map_err(Self::storage_error)?;
                match range.next() {
                    Some(entry) => {
                        let (key, _) = entry.map_err(Self::storage_error)?;
                        let key = key.value();
                        if !key.starts_with(&prefix) {
                            None
                        } else {
                            admission.admit(key.len().saturating_mul(2).saturating_add(64))?;
                            Some(key.to_vec())
                        }
                    }
                    None => None,
                }
            };
            let Some(reverse) = next else {
                return Ok(());
            };
            forward
                .remove(&reverse[prefix.len()..])
                .map_err(Self::storage_error)?;
            by_row
                .remove(reverse.as_slice())
                .map_err(Self::storage_error)?;
        }
    }
}

fn decode_hex(text: &str) -> Option<Vec<u8>> {
    if !text.len().is_multiple_of(2) {
        return None;
    }
    (0..text.len())
        .step_by(2)
        .map(|at| u8::from_str_radix(text.get(at..at + 2)?, 16).ok())
        .collect()
}

/// The `(forward, reverse, archived child key)` follower postings an archived
/// body left, read by its body key in one captured root. Empty for a table
/// that never followed.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
pub(super) fn follower_postings_of_body(
    read: &redb::ReadTransaction,
    body_key: &str,
) -> Result<Vec<(Vec<u8>, Vec<u8>, Vec<u8>)>> {
    let corrupt = || Error::StoreCorrupted {
        path: "authored history".into(),
        reason: "invalid occurrence key".into(),
    };
    let (table, row) = body_key.split_once(':').ok_or_else(corrupt)?;
    let table = String::from_utf8(decode_hex(table).ok_or_else(corrupt)?).map_err(|_| corrupt())?;
    let row = decode_hex(row).ok_or_else(corrupt)?;
    let by_row = match read.open_table(FOLLOWER_POSTINGS_BY_ROW) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
        Err(error) => return Err(RedbPersistence::storage_error(error)),
    };
    let forward = read
        .open_table(FOLLOWER_POSTINGS)
        .map_err(RedbPersistence::storage_error)?;
    let prefix = follower_row_prefix(&table, &row);
    let mut postings = Vec::new();
    for entry in by_row
        .range(prefix.as_slice()..)
        .map_err(RedbPersistence::storage_error)?
    {
        let (key, _) = entry.map_err(RedbPersistence::storage_error)?;
        let key = key.value();
        if !key.starts_with(&prefix) {
            break;
        }
        let child_key = forward
            .get(&key[prefix.len()..])
            .map_err(RedbPersistence::storage_error)?
            .ok_or_else(corrupt)?;
        crate::read_image_memory::reserve(
            key.len()
                .saturating_mul(2)
                .saturating_add(child_key.value().len()),
        )?;
        postings.push((
            key[prefix.len()..].to_vec(),
            key.to_vec(),
            child_key.value().to_vec(),
        ));
    }
    Ok(postings)
}

/// One archived life of a following table, found from its parent.
pub(crate) struct ArchivedFollower {
    pub(crate) child_table: String,
    pub(crate) table_generation: u64,
    pub(crate) row_id: RowId,
    pub(crate) natural_key: crate::sync_types::NaturalKey,
}

impl WriterSnapshotHandle {
    /// Every archived life whose following foreign key named this parent row,
    /// whatever the child table's current declaration is. Read in the
    /// handle's captured root, one posting window charged at a time.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn archived_purge_followers(
        &self,
        parent_table: &str,
        parent_values: &[Value],
        credit: &mut contextdb_core::read_memory::ReadCredit,
    ) -> Result<Vec<ArchivedFollower>> {
        let read = self.owner_read()?;
        let forward = match read.open_table(FOLLOWER_POSTINGS) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
            Err(error) => return Err(RedbPersistence::storage_error(error)),
        };
        credit.try_grow_for(
            parent_table
                .len()
                .saturating_add(
                    parent_values
                        .iter()
                        .map(Value::estimated_bytes)
                        .fold(0, usize::saturating_add)
                        .saturating_mul(4),
                )
                .saturating_add(64),
            "writer_source",
            "purge_follower_keys",
            "Raise MEMORY_LIMIT to find what follows this purge.",
        )?;
        let prefix = follower_parent_prefix(parent_table, parent_values)?;
        let corrupt = || Error::StoreCorrupted {
            path: "authored follower postings".into(),
            reason: "invalid follower posting".into(),
        };
        let mut found = Vec::new();
        for entry in forward
            .range(prefix.as_slice()..)
            .map_err(RedbPersistence::storage_error)?
        {
            let (key, value) = entry.map_err(RedbPersistence::storage_error)?;
            let key = key.value();
            if !key.starts_with(&prefix) {
                break;
            }
            credit.try_grow_for(
                key.len()
                    .saturating_add(value.value().len().saturating_mul(4))
                    .saturating_add(std::mem::size_of::<ArchivedFollower>()),
                "writer_source",
                "purge_follower_discovery",
                "Raise MEMORY_LIMIT to find what follows this purge.",
            )?;
            let (child_table, table_generation, row_key) =
                decode_follower_posting(key, prefix.len()).ok_or_else(corrupt)?;
            let row_id = RowId(u64::from_be_bytes(
                row_key[..8].try_into().map_err(|_| corrupt())?,
            ));
            let natural_key = RedbPersistence::decode_config_value(value.value())?;
            if found.len() == found.capacity() {
                let grow = found.capacity().max(4);
                credit.try_grow_for(
                    found
                        .capacity()
                        .saturating_add(grow)
                        .saturating_mul(std::mem::size_of::<ArchivedFollower>()),
                    "writer_source",
                    "purge_follower_discovery",
                    "Raise MEMORY_LIMIT to find what follows this purge.",
                )?;
                found.reserve_exact(grow);
            }
            found.push(ArchivedFollower {
                child_table,
                table_generation,
                row_id,
                natural_key,
            });
        }
        Ok(found)
    }
}
