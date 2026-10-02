//! Checkpoint history copies retain one admitted occurrence at the captured root.
use super::*;
use std::ops::Bound;

impl WriterSnapshot {
    pub(crate) fn copy_ddl_into(
        self: &Arc<Self>,
        artifact: &RedbPersistence,
        through: Lsn,
    ) -> Result<()> {
        let _keys = self.adapter_control(64)?;
        // Include every ordinal at the watermark, preserving the physical
        // occurrence key even when a same-LSN run spans several windows.
        let upper = format!("{:020};", through.0);
        let mut cursor = self.cursor(
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
    fn append_ddl_occurrence(&self, key: &str, lsn: Lsn, change: &DdlChange) -> Result<()> {
        self.with_db(|db| {
            let write = db.begin_write().map_err(Self::storage_error)?;
            let encoded = Self::encode(change)?;
            write
                .open_table(DDL_LOG_TABLE)
                .map_err(Self::storage_error)?
                .insert(key, encoded.as_slice())
                .map_err(Self::storage_error)?;
            let mutation = content_identity::encoded_history_mutation(key, change)?;
            Self::apply_content_identity_mutations_in_write(
                &write,
                &[mutation],
                &mut self.proof_admission(),
            )?;
            Self::raise_noncanonical_writer_floors_in_write(&write, TxId(0), lsn, RowId(0))?;
            Self::write_writer_recovery_bounds_in_write(&write, None)?;
            write.commit().map_err(Self::storage_error)
        })
    }
}
