//! Immutable authored bodies retired by schema projection. The pool is keyed
//! by exact local occurrence, so replacing a projection cannot replace history.
use super::*;

pub(super) const BODIES: TableDefinition<&str, &[u8]> =
    TableDefinition::new("__authored_row_bodies");

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
            metadata.get(Self::meta_key(logical).as_str()).map_err(Self::storage_error)?
                .map(|value| {
                    metadata_credit.admit(value.value().len().saturating_mul(64).saturating_add(1024))?;
                    Self::decode_persisted_schema(value.value()).map(|(meta, _, _)| meta)
                }).transpose()
        })?;
        // Stored rows are self-describing. Borrow one encoded value at a time;
        // neither decoded history nor a table-sized key list is materialized.
        for entry in source.iter().map_err(Self::storage_error)? {
            let (key, value) = entry.map_err(Self::storage_error)?;
            let raw: [u8; 24] = key.value().try_into().map_err(|_| Error::StoreCorrupted {
                path: logical.into(),
                reason: "invalid authored row key".into(),
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
            if !present {
                // Quantized relational columns are placeholders. Retire their
                // exact canonical placement now, while that column lifetime
                // still exists, instead of consulting a recreated live index
                // when this occurrence is served later.
                if let Some(meta) = meta.as_ref().filter(|meta| meta.columns.iter().any(|column|
                    matches!(column.column_type, ColumnType::Vector(_))
                        && column.quantization != VectorQuantization::F32)) {
                    let vector_bytes = meta.columns.iter().map(|column| match column.column_type {
                        ColumnType::Vector(dimension) => dimension.saturating_mul(4), _ => 0,
                    }).sum::<usize>();
                    admission.admit(value.value().len().saturating_mul(8)
                        .saturating_add(vector_bytes.saturating_mul(8)).saturating_add(meta.estimated_bytes().saturating_mul(4)))?;
                    let mut row = Self::decode_versioned_row(value.value(), None)?;
                    self.fill_authored_quantized_columns_in_write(write, logical, meta, &mut row)?;
                    // This archive owns the already stored approximation. The
                    // f32 encoding preserves those values without requantizing.
                    let encoded = Self::encode_versioned_row(&row, None)?;
                    destination.insert(addressed.as_str(), encoded.as_slice()).map_err(Self::storage_error)?;
                } else {
                    destination.insert(addressed.as_str(), value.value()).map_err(Self::storage_error)?;
                }
            }
        }
        Ok(())
    }

    fn fill_authored_quantized_columns_in_write(
        &self, _write: &redb::WriteTransaction, table: &str, meta: &TableMeta, row: &mut VersionedRow,
    ) -> Result<()> {
        for column in &meta.columns {
            let ColumnType::Vector(dimension) = column.column_type else { continue; };
            if column.quantization == VectorQuantization::F32
                || !matches!(row.values.get(&column.name), Some(Value::Null) | None) { continue; }
            let index = VectorIndexRef::new(table, &column.name);
            let partition = vector_partition_key_for_row(&index, meta, column, row)?;
            let mut prefix = Self::vector_partition_membership_row_prefix(&index, &partition, row.row_id);
            prefix.extend_from_slice(&row.created_tx.0.to_be_bytes());
            prefix.extend_from_slice(&row.lsn.0.to_be_bytes());
            let membership = self.with_db(|db| {
                // DROP may already have removed the unpublished placements.
                // Read both placement and body from the still-committed source
                // generation, which the enclosing writer excludes replacing.
                let read = db.begin_read().map_err(Self::storage_error)?;
                let members = read.open_table(VECTOR_PARTITION_MEMBERSHIP_TABLE).map_err(Self::storage_error)?;
                let mut entries = members.range(prefix.as_slice()..).map_err(Self::storage_error)?;
                let first = entries.next().transpose().map_err(Self::storage_error)?;
                let membership = first.filter(|(key, _)| key.value().starts_with(&prefix))
                .map(|(key, bytes)| {
                    let record: VectorPartitionMembershipRecord =
                        self.decode_vector_partition_record(VectorPartitionRecordKind::Membership, bytes.value())?;
                    if Self::vector_partition_membership_key(&record) != key.value()
                        || record.index != index || record.partition_key != partition
                        || record.row_id != row.row_id || record.row_created_tx != row.created_tx
                        || record.row_lsn != row.lsn {
                        return Err(Error::StoreCorrupted { path: table.into(), reason: "authored vector placement names another occurrence".into() });
                    }
                    Ok(record)
                }).transpose()?;
                if entries.next().transpose().map_err(Self::storage_error)?
                    .is_some_and(|(key, _)| key.value().starts_with(&prefix)) {
                    return Err(Error::StoreCorrupted { path: table.into(), reason: "authored vector occurrence has duplicate placements".into() });
                }
                Ok(membership)
            })?;
            let Some(membership) = membership else { continue; };
            let mut bodies = self.load_vector_entries_for_memberships(&[membership], dimension, Some(column.quantization))?;
            let body = bodies.pop().ok_or_else(|| Error::StoreCorrupted {
                path: table.into(), reason: "authored vector placement lost its canonical body".into(),
            })?;
            row.values.insert(column.name.clone(), Value::Vector(body.vector));
        }
        Ok(())
    }

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
        let key = body_key(logical, key);
        let removed = write
            .open_table(BODIES)
            .map_err(Self::storage_error)?
            .remove(key.as_str())
            .map_err(Self::storage_error)?
            .is_some();
        Ok(removed)
    }
}

impl RedbPersistence {
    pub(super) fn retain_pending_authored_rows_in_write_original(
        &self, original: &OriginalPersistenceFrame<'_>, write: &redb::WriteTransaction,
        rows: &[(String, Arc<crate::database::AuthoredRowShape>, VersionedRow)],
    ) -> Result<()> {
        let mut destination = write.open_table(BODIES).map_err(Self::storage_error)?;
        for (table, _, row) in rows {
            let window = OriginalPersistenceFrame::new(original.original)?;
            window.reserve(table.len().saturating_mul(2).saturating_add(128))?;
            let key = body_key(table, &Self::rel_row_key(row));
            if destination.get(key.as_str()).map_err(Self::storage_error)?.is_some() { continue; }
            let encoded = Self::encode_versioned_row_original(&window, row, None)?;
            destination.insert(key.as_str(), encoded.as_slice()).map_err(Self::storage_error)?;
        }
        Ok(())
    }

    pub(super) fn retain_pending_authored_rows_in_write(&self, write: &redb::WriteTransaction,
        rows: &[(String,Arc<crate::database::AuthoredRowShape>,VersionedRow)]) -> Result<()> {
        let mut destination = write.open_table(BODIES).map_err(Self::storage_error)?;
        for (table,_,row) in rows {
            let mut scratch = self.proof_admission();
            scratch.admit(row.estimated_bytes().saturating_mul(8).saturating_add(table.len()*2+256))?;
            let key = body_key(table,&Self::rel_row_key(row));
            if destination.get(key.as_str()).map_err(Self::storage_error)?.is_some() { continue; }
            let encoded = Self::encode_versioned_row(row,None)?;
            destination.insert(key.as_str(),encoded.as_slice()).map_err(Self::storage_error)?;
        }
        Ok(())
    }
}
