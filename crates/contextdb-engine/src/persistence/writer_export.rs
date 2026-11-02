//! Checkpoint history copies retain one admitted occurrence at the captured root.
use super::writer_source::SourceWindowOwner;
use super::*;
use std::ops::Bound;

impl WriterSnapshotHandle {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn copy_config_into(
        &self,
        owner: SourceWindowOwner,
        artifact: &RedbPersistence,
        watermark: TxId,
    ) -> Result<()> {
        let mut cursor = self.config_cursor(owner.clone(), "")?;
        while let Some(window) = cursor.next(|key, bytes| {
            crate::read_image_memory::reserve(
                bytes
                    .len()
                    .saturating_mul(2)
                    .saturating_add(key.len())
                    .saturating_add(256),
            )?;
            let key = std::str::from_utf8(key).map_err(|_| Error::StoreCorrupted {
                path: "config".into(),
                reason: "configuration key is not UTF-8".into(),
            })?;
            if key.starts_with(super::row_graph::RETIRED_PREFIX) {
                // Decode and nested point checks stay inside this original
                // source window's admission; nothing borrows a new decoder.
                let record = super::row_graph::decode_retirement(bytes)?;
                if super::row_graph::retirement_key(&record.owned.owner)? != key {
                    return Err(Error::StoreCorrupted {
                        path: "row-derived adjacency".into(),
                        reason: "export retirement key does not match its exact owner".into(),
                    });
                }
                if record.owned.owner.created_tx > watermark {
                    return Ok(None);
                }
                crate::read_image_memory::reserve(
                    crate::database::Database::DURABLE_LINEAGE_TABLE_GENERATION_PREFIX
                        .len()
                        .saturating_add(record.owned.owner.table.len().saturating_mul(4)),
                )?;
                let generation_key =
                    crate::database::Database::durable_lineage_table_generation_key(
                        &record.owned.owner.table,
                    );
                let generation = self.config_point::<u64>(owner.clone(), &generation_key)?;
                if !generation.as_ref().is_some_and(|value| {
                    value.value != 0 && value.value == record.owned.owner.table_generation
                }) {
                    return Err(Error::StoreCorrupted {
                        path: "row-derived adjacency".into(),
                        reason:
                            "export retirement disagrees with the authoritative table generation"
                                .into(),
                    });
                }
                let mut row_key = [0_u8; 24];
                row_key[..8].copy_from_slice(&record.owned.owner.row_id.0.to_be_bytes());
                row_key[8..16].copy_from_slice(&record.owned.owner.created_tx.0.to_be_bytes());
                row_key[16..].copy_from_slice(&record.owned.owner.lsn.0.to_be_bytes());
                let mut rows =
                    self.exact_row(owner.clone(), &record.owned.owner.table, &row_key)?;
                if rows.next_row()?.is_none() {
                    return Err(Error::StoreCorrupted {
                        path: "row-derived adjacency".into(),
                        reason: "export retirement has no exact owning SQL row".into(),
                    });
                }
                if !RedbPersistence::row_retirement_in_read(
                    self.owner_read()?.as_ref(),
                    &record.owned,
                )? {
                    return Err(Error::StoreCorrupted {
                        path: "row-derived adjacency".into(),
                        reason: "export retirement has no authenticated projection".into(),
                    });
                }
                if record
                    .owned
                    .entry
                    .deleted_tx
                    .is_some_and(|tx| tx > watermark)
                {
                    return Ok(None);
                }
            }
            Ok(Some(bytes.to_vec()))
        })? {
            if let Some(bytes) = &window.value {
                let key =
                    std::str::from_utf8(window.position()).map_err(|_| Error::StoreCorrupted {
                        path: "config".into(),
                        reason: "configuration key is not UTF-8".into(),
                    })?;
                artifact.append_encoded_config_occurrence(key, bytes)?;
            }
        }
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn copy_authored_into(
        &self,
        owner: SourceWindowOwner,
        artifact: &RedbPersistence,
        through: Lsn,
    ) -> Result<()> {
        let read = self.owner_read()?;
        let witnessed = RedbPersistence::witnessed_row_count_in_read(
            &read,
            super::authored_history::BODIES.name(),
        )?;
        if read
            .normal_table_root(super::authored_history::BODIES.name())
            .map_err(RedbPersistence::storage_error)?
            .is_none()
            && witnessed.is_none()
        {
            return Ok(());
        }
        RedbPersistence::verify_history_root(&read, super::authored_history::BODIES.name())?;
        let mut cursor = self.cursor(
            owner,
            super::authored_history::BODIES.name(),
            writer_source::WriterCursorFamily::Text,
            Bound::Unbounded,
            Bound::Unbounded,
            false,
        )?;
        while let Some(window) = cursor.next(|_, bytes| {
            let row = RedbPersistence::decode_versioned_row(bytes, None)?;
            crate::read_image_memory::reserve(bytes.len())?;
            Ok((row.lsn <= through).then(|| bytes.to_vec()))
        })? {
            if let Some(bytes) = &window.value {
                let key =
                    std::str::from_utf8(window.position()).map_err(|_| Error::StoreCorrupted {
                        path: "authored history".into(),
                        reason: "invalid occurrence key".into(),
                    })?;
                artifact.append_encoded_authored_occurrence(key, bytes)?;
                // The parent-keyed postings of a following table travel with
                // its archived row, or a restored store could not find it.
                let postings = super::authored_history::follower_postings_of_body(&read, key)?;
                if !postings.is_empty() {
                    artifact.append_follower_postings(&postings)?;
                }
            }
        }
        Ok(())
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn copy_ddl_into(
        &self,
        owner: SourceWindowOwner,
        artifact: &RedbPersistence,
        through: Lsn,
    ) -> Result<()> {
        let _keys = self.adapter_control(&owner, 64)?;
        // Include every ordinal at the watermark, preserving the physical
        // occurrence key even when a same-LSN run spans several windows.
        let upper = format!("{:020};", through.0);
        let mut cursor = self.cursor(
            owner,
            "ddl_log",
            writer_source::WriterCursorFamily::Text,
            Bound::Unbounded,
            Bound::Excluded(upper.as_bytes()),
            false,
        )?;
        while let Some(window) = cursor.next(|key, bytes| {
            // Artifact encoding and its content-identity mutation coexist
            // with this decoded occurrence. The window owns that scratch
            // through the destination transaction, including error unwind.
            crate::read_image_memory::reserve(
                bytes
                    .len()
                    .saturating_mul(64)
                    .saturating_add(key.len().saturating_mul(8))
                    .saturating_add(4096),
            )?;
            let key = std::str::from_utf8(key)
                .map_err(|_| Error::Other("DDL occurrence key is not UTF-8".into()))?;
            let lsn = key
                .split(':')
                .next()
                .unwrap_or(key)
                .parse::<u64>()
                .map_err(|_| Error::Other("invalid DDL occurrence LSN".into()))?;
            let (change, consumed) = crate::read_image_memory::decode::<DdlChange>(
                &crate::read_image_memory::ImageMemory::enter_decode_call()?,
                bytes,
            )?;
            if consumed != bytes.len() {
                return Err(Error::Other("DDL occurrence has trailing bytes".into()));
            }
            Ok((Lsn(lsn), change))
        })? {
            let key = std::str::from_utf8(window.position())
                .map_err(|_| Error::Other("DDL occurrence key is not UTF-8".into()))?;
            artifact.append_ddl_occurrence(key, window.value.0, &window.value.1)?;
        }
        Ok(())
    }
}

impl RedbPersistence {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn append_authored_row(
        &self,
        table: &str,
        meta: &TableMeta,
        row: &VersionedRow,
    ) -> Result<()> {
        self.prepare_proof_owner()?;
        let _decode = self.writer_decode_scope()?;
        let mut paid = self.proof_admission();
        paid.admit(
            row.estimated_bytes()
                .saturating_mul(4)
                .saturating_add(table.len().saturating_mul(2))
                .saturating_add(256),
        )?;
        let key = super::authored_history::body_key(table, &Self::rel_row_key(row));
        let encoded = Self::encode_versioned_row(row, Some(meta))?;
        self.append_encoded_authored_occurrence(&key, &encoded)
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn append_follower_postings(&self, postings: &[(Vec<u8>, Vec<u8>, Vec<u8>)]) -> Result<()> {
        self.prepare_proof_owner()?;
        let mut paid = self.proof_admission();
        paid.admit(
            postings
                .iter()
                .map(|(forward, reverse, value)| forward.len() + reverse.len() + value.len())
                .sum::<usize>()
                .saturating_mul(2)
                .saturating_add(256),
        )?;
        self.with_db(|db| {
            let write = Self::begin_history_write(db)?;
            {
                let mut forward = write
                    .open_table(super::authored_history::FOLLOWER_POSTINGS)
                    .map_err(Self::storage_error)?;
                let mut by_row = write
                    .open_table(super::authored_history::FOLLOWER_POSTINGS_BY_ROW)
                    .map_err(Self::storage_error)?;
                for (key, reverse, value) in postings {
                    forward
                        .insert(key.as_slice(), value.as_slice())
                        .map_err(Self::storage_error)?;
                    by_row
                        .insert(reverse.as_slice(), &[][..])
                        .map_err(Self::storage_error)?;
                }
            }
            write.commit().map_err(Self::storage_error)
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn append_encoded_authored_occurrence(&self, key: &str, encoded: &[u8]) -> Result<()> {
        self.prepare_proof_owner()?;
        let _decode = self.writer_decode_scope()?;
        let mut paid = self.proof_admission();
        paid.admit(
            key.len()
                .saturating_add(encoded.len().saturating_mul(8))
                .saturating_add(256),
        )?;
        let row = Self::decode_versioned_row(encoded, None)?;
        self.with_db(|db| {
            let write = Self::begin_history_write(db)?;
            write
                .open_table(super::authored_history::BODIES)
                .map_err(Self::storage_error)?
                .insert(key, encoded)
                .map_err(Self::storage_error)?;
            Self::raise_noncanonical_writer_floors_in_write(
                &write,
                row.created_tx.max(row.deleted_tx.unwrap_or(TxId(0))),
                row.lsn,
                row.row_id,
            )?;
            Self::write_writer_recovery_bounds_in_write(&write, None)?;
            write.commit().map_err(Self::storage_error)
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn append_encoded_config_occurrence(&self, key: &str, encoded: &[u8]) -> Result<()> {
        self.prepare_proof_owner()?;
        let mut admission = self.proof_admission();
        let group_plan_bytes = custody_groups::plan_bytes(key, encoded)?;
        admission.admit(
            encoded
                .len()
                .saturating_mul(64)
                .saturating_add(key.len().saturating_mul(8))
                .saturating_add(4096)
                .saturating_add(group_plan_bytes),
        )?;
        let group_plan = custody_groups::GroupPlan::single_prepaid(key, encoded)?;
        self.with_db(|db| {
            let write = Self::begin_history_write(db)?;
            Self::raise_config_identity_floors_in_write(&write, key, encoded)?;
            Self::update_policy_hold_release_index(&write, key, true)?;
            write
                .open_table(CONFIG_TABLE)
                .map_err(Self::storage_error)?
                .insert(key, encoded)
                .map_err(Self::storage_error)?;
            let mutations = content_identity::config_mutations(key, Some(encoded));
            Self::apply_content_identity_with_manifests_and_groups_in_write(
                &write,
                &mutations,
                &[],
                &mut admission,
                Some(&group_plan),
            )?;
            Self::write_writer_recovery_bounds_in_write(&write, None)?;
            write.commit().map_err(Self::storage_error)
        })
    }

    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn append_ddl_occurrence(&self, key: &str, lsn: Lsn, change: &DdlChange) -> Result<()> {
        self.prepare_proof_owner()?;
        let mut admission = self.proof_admission();
        struct Counter(usize);
        impl std::io::Write for Counter {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0 = self
                    .0
                    .checked_add(bytes.len())
                    .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::OutOfMemory))?;
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let mut length = Counter(0);
        bincode::serde::encode_into_std_write(change, &mut length, bincode::config::standard())
            .map_err(|error| {
                Error::Other((format!("declaration encoding failed: {error}")).into())
            })?;
        admission.admit(
            length
                .0
                .saturating_mul(64)
                .saturating_add(key.len().saturating_mul(8))
                .saturating_add(4096),
        )?;
        self.with_db(|db| {
            let write = Self::begin_history_write(db)?;
            let encoded = Self::encode(change)?;
            write
                .open_table(DDL_LOG_TABLE)
                .map_err(Self::storage_error)?
                .insert(key, encoded.as_slice())
                .map_err(Self::storage_error)?;
            self.append_ddl_summary_in_write(&write, key, change)?;
            let mutation = content_identity::encoded_history_mutation(key, change)?;
            Self::apply_content_identity_mutations_in_write(&write, &[mutation], &mut admission)?;
            Self::raise_noncanonical_writer_floors_in_write(&write, TxId(0), lsn, RowId(0))?;
            Self::write_writer_recovery_bounds_in_write(&write, None)?;
            write.commit().map_err(Self::storage_error)
        })
    }
}
