//! Receipt writes share the actual outer native transaction and scope binding.
use super::super::qualification::{self, Record, TableName};
use super::*;
impl WriteScope<'_, '_> {
    pub(in crate::persistence::vector_ivf) fn read_qualification(
        &self,
        generation: u64,
        kind: u8,
        ordinal: u64,
    ) -> Result<Option<Record>> {
        self.check_cancel()?;
        let family = *self
            .record
            .borrow()
            .family(generation)
            .ok_or_else(corrupt)?;
        qualification::read(
            &NativeIo::Borrowed(self.session.write),
            &self.key,
            family,
            kind,
            ordinal,
        )
    }
    /// A cold unpublished calibration restarts after a bounded physical clear.
    pub(in crate::persistence::vector_ivf) fn clear_calibration_records(
        &mut self,
        generation: u64,
        limit: usize,
    ) -> Result<bool> {
        let mut family = *self
            .record
            .borrow()
            .family(generation)
            .ok_or_else(corrupt)?;
        let mut remaining = limit;
        self.reclaim_qualification(&mut family, &mut remaining)?;
        self.record
            .borrow_mut()
            .families
            .iter_mut()
            .flatten()
            .find(|f| f.generation == generation)
            .ok_or_else(corrupt)?
            .qualification_root = family.qualification_root;
        self.begin()?.commit()?;
        Ok(family.qualification_root.is_none())
    }
    pub(in crate::persistence::vector_ivf) fn write_qualification(
        &self,
        generation: u64,
        kind: u8,
        ordinal: u64,
        record: Record,
    ) -> Result<()> {
        self.check_cancel()?;
        let family = *self
            .record
            .borrow()
            .family(generation)
            .ok_or_else(corrupt)?;
        let name = TableName::new(family.storage)?;
        let _invoice = Invoice::reserve(
            &self.session.owner,
            std::mem::size_of::<([u8; qualification::RECORD_BYTES], Record)>(),
            "clustered_qualification_record",
        )?;
        let key = qualification::key(kind, ordinal);
        let mut bytes = [0; qualification::RECORD_BYTES];
        qualification::encode(&self.key, generation, &key, record, &mut bytes)?;
        let native = NativeIo::Borrowed(self.session.write);
        native.put(name.as_str()?, &key, &bytes)?;
        let root = native.fingerprint(name.as_str()?)?.ok_or_else(corrupt)?;
        let mut state = self.record.borrow_mut();
        state
            .families
            .iter_mut()
            .flatten()
            .find(|f| f.generation == generation)
            .ok_or_else(corrupt)?
            .qualification_root = Some(root);
        Ok(())
    }
    pub(super) fn reclaim_qualification(
        &self,
        family: &mut Family,
        remaining: &mut usize,
    ) -> Result<()> {
        use redb::{ReadableTable, ReadableTableMetadata};
        if family.qualification_root.is_none() {
            return Ok(());
        }
        let name = TableName::new(family.storage)?;
        let definition = redb::TableDefinition::<&[u8], &[u8]>::new(name.as_str()?);
        let mut table = self
            .session
            .write
            .open_table(definition)
            .map_err(RedbPersistence::storage_error)?;
        while *remaining != 0 {
            self.check_cancel()?;
            let key = {
                let mut cursor = table.iter().map_err(RedbPersistence::storage_error)?;
                cursor
                    .next()
                    .map(|row| {
                        row.map_err(RedbPersistence::storage_error)
                            .and_then(|(k, _)| {
                                <[u8; 9]>::try_from(k.value()).map_err(|_| corrupt())
                            })
                    })
                    .transpose()?
            };
            let Some(key) = key else {
                break;
            };
            table
                .remove(key.as_slice())
                .map_err(RedbPersistence::storage_error)?;
            *remaining -= 1;
        }
        let empty = table.is_empty().map_err(RedbPersistence::storage_error)?;
        drop(table);
        family.qualification_root = if empty {
            self.session
                .write
                .delete_table(definition)
                .map_err(RedbPersistence::storage_error)?;
            None
        } else {
            Some(
                NativeIo::Borrowed(self.session.write)
                    .fingerprint(name.as_str()?)?
                    .ok_or_else(corrupt)?,
            )
        };
        Ok(())
    }
}
