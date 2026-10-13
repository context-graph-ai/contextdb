//! Checkpoint history copies retain one admitted occurrence at the captured root.
use super::writer_source::SourceWindowOwner;
use super::*;
use std::ops::Bound;

impl WriterSnapshot {
    pub(crate) fn copy_config_into(
        self: &Arc<Self>,
        owner: SourceWindowOwner,
        artifact: &RedbPersistence,
    ) -> Result<()> {
        let mut cursor = self.config_cursor(owner, "")?;
        while let Some(window) = cursor.next(|key, bytes| {
            crate::read_image_memory::reserve(
                bytes
                    .len()
                    .saturating_mul(2)
                    .saturating_add(key.len())
                    .saturating_add(256),
            )?;
            Ok(bytes.to_vec())
        })? {
            let key =
                std::str::from_utf8(window.position()).map_err(|_| Error::StoreCorrupted {
                    path: "config".into(),
                    reason: "configuration key is not UTF-8".into(),
                })?;
            artifact.append_encoded_config_occurrence(key, &window.value)?;
        }
        Ok(())
    }

    pub(crate) fn copy_ddl_into(
        self: &Arc<Self>,
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
            let (change, consumed) = crate::read_image_memory::decode::<DdlChange>(bytes)?;
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
            .map_err(|error| Error::Other(format!("declaration encoding failed: {error}")))?;
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
            Self::append_ddl_summary_in_write(&write, key, change)?;
            let mutation = content_identity::encoded_history_mutation(key, change)?;
            Self::apply_content_identity_mutations_in_write(&write, &[mutation], &mut admission)?;
            Self::raise_noncanonical_writer_floors_in_write(&write, TxId(0), lsn, RowId(0))?;
            Self::write_writer_recovery_bounds_in_write(&write, None)?;
            write.commit().map_err(Self::storage_error)
        })
    }
}
